/*
** Copyright (c) 1999, 2000 D. Richard Hipp
**
** This program is free software; you can redistribute it and/or
** modify it under the terms of the GNU General Public
** License as published by the Free Software Foundation; either
** version 2 of the License, or (at your option) any later version.
**
** This program is distributed in the hope that it will be useful,
** but WITHOUT ANY WARRANTY; without even the implied warranty of
** MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
** General Public License for more details.
** 
** You should have received a copy of the GNU General Public
** License along with this library; if not, write to the
** Free Software Foundation, Inc., 59 Temple Place - Suite 330,
** Boston, MA  02111-1307, USA.
**
** Author contact information:
**   drh@hwaci.com
**   http://www.hwaci.com/drh/
**
*************************************************************************
** This file contains C code routines that are called by the parser
** when syntax rules are reduced.  The routines in this file handle
** the following kinds of syntax:
**
**     CREATE TABLE
**     DROP TABLE
**     CREATE INDEX
**     DROP INDEX
**     creating expressions and ID lists
**     COPY
**     VACUUM
**
** $Id: build.c,v 1.30 2001/09/13 14:46:10 drh Exp $
*/
#include "sqliteInt.h"

/*
** This routine is called after a single SQL statement has been
** parsed and we want to execute the VDBE code to implement 
** that statement.  Prior action routines should have already
** constructed VDBE code to do the work of the SQL statement.
** This routine just has to execute the VDBE code.
**
** Note that if an error occurred, it might be the case that
** no VDBE code was generated.
*/
void sqliteExec(Parse *pParse){
  int rc = SQLITE_OK;
  sqlite *db = pParse->db;
  if( sqlite_malloc_failed ) return;
  if( pParse->pVdbe ){
    if( pParse->explain ){
      rc = sqliteVdbeList(pParse->pVdbe, pParse->xCallback, pParse->pArg, 
                          &pParse->zErrMsg);
    }else{
      FILE *trace = (db->flags & SQLITE_VdbeTrace)!=0 ? stderr : 0;
      sqliteVdbeTrace(pParse->pVdbe, trace);
      rc = sqliteVdbeExec(pParse->pVdbe, pParse->xCallback, pParse->pArg, 
                          &pParse->zErrMsg, db->pBusyArg,
                          db->xBusyCallback);
    }
    sqliteVdbeDelete(pParse->pVdbe);
    pParse->pVdbe = 0;
    pParse->colNamesSet = 0;
    pParse->rc = rc;
  }
}

/*
** Construct a new expression node and return a pointer to it.
*/
Expr *sqliteExpr(int op, Expr *pLeft, Expr *pRight, Token *pToken){
  Expr *pNew;
  pNew = sqliteMalloc( sizeof(Expr) );
  if( pNew==0 ) return 0;
  pNew->op = op;
  pNew->pLeft = pLeft;
  pNew->pRight = pRight;
  if( pToken ){
    pNew->token = *pToken;
  }else{
    pNew->token.z = "";
    pNew->token.n = 0;
  }
  if( pLeft && pRight ){
    sqliteExprSpan(pNew, &pLeft->span, &pRight->span);
  }else{
    pNew->span = pNew->token;
  }
  return pNew;
}

/*
** Set the Expr.token field of the given expression to span all
** text between the two given tokens.
*/
void sqliteExprSpan(Expr *pExpr, Token *pLeft, Token *pRight){
  if( pExpr ){
    pExpr->span.z = pLeft->z;
    pExpr->span.n = pRight->n + (int)pRight->z - (int)pLeft->z;
  }
}

/*
** Construct a new expression node for a function with multiple
** arguments.
*/
Expr *sqliteExprFunction(ExprList *pList, Token *pToken){
  Expr *pNew;
  pNew = sqliteMalloc( sizeof(Expr) );
  if( pNew==0 ) return 0;
  pNew->op = TK_FUNCTION;
  pNew->pList = pList;
  if( pToken ){
    pNew->token = *pToken;
  }else{
    pNew->token.z = "";
    pNew->token.n = 0;
  }
  return pNew;
}

/*
** Recursively delete an expression tree.
*/
void sqliteExprDelete(Expr *p){
  if( p==0 ) return;
  if( p->pLeft ) sqliteExprDelete(p->pLeft);
  if( p->pRight ) sqliteExprDelete(p->pRight);
  if( p->pList ) sqliteExprListDelete(p->pList);
  if( p->pSelect ) sqliteSelectDelete(p->pSelect);
  sqliteFree(p);
}

/*
** Locate the in-memory structure that describes the
** format of a particular database table given the name
** of that table.  Return NULL if not found.
*/
Table *sqliteFindTable(sqlite *db, char *zName){
  Table *pTable;
  int h;

  h = sqliteHashNoCase(zName, 0) % N_HASH;
  for(pTable=db->apTblHash[h]; pTable; pTable=pTable->pHash){
    if( sqliteStrICmp(pTable->zName, zName)==0 ) return pTable;
  }
  return 0;
}

/*
** Locate the in-memory structure that describes the
** format of a particular index given the name
** of that index.  Return NULL if not found.
*/
Index *sqliteFindIndex(sqlite *db, char *zName){
  Index *p;
  int h;

  h = sqliteHashNoCase(zName, 0) % N_HASH;
  for(p=db->apIdxHash[h]; p; p=p->pHash){
    if( sqliteStrICmp(p->zName, zName)==0 ) return p;
  }
  return 0;
}

/*
** Remove the given index from the index hash table, and free
** its memory structures.
**
** The index is removed from the database hash table if db!=NULL.
** But it is not unlinked from the Table that is being indexed.  
** Unlinking from the Table must be done by the calling function.
*/
static void sqliteDeleteIndex(sqlite *db, Index *pIndex){
  int h;
  if( pIndex->zName && db ){
    h = sqliteHashNoCase(pIndex->zName, 0) % N_HASH;
    if( db->apIdxHash[h]==pIndex ){
      db->apIdxHash[h] = pIndex->pHash;
    }else{
      Index *p;
      for(p=db->apIdxHash[h]; p && p->pHash!=pIndex; p=p->pHash){}
      if( p && p->pHash==pIndex ){
        p->pHash = pIndex->pHash;
      }
    }
  }
  sqliteFree(pIndex);
}

/*
** Unlink the given  index from its table, then remove
** the index from the index hash table, and free its memory
** structures.
*/
static void sqliteUnlinkAndDeleteIndex(sqlite *db, Index *pIndex){
  if( pIndex->pTable->pIndex==pIndex ){
    pIndex->pTable->pIndex = pIndex->pNext;
  }else{
    Index *p;
    for(p=pIndex->pTable->pIndex; p && p->pNext!=pIndex; p=p->pNext){}
    if( p && p->pNext==pIndex ){
      p->pNext = pIndex->pNext;
    }
  }
  sqliteDeleteIndex(db, pIndex);
}

/*
** Remove the memory data structures associated with the given
** Table.  No changes are made to disk by this routine.
**
** This routine just deletes the data structure.  It does not unlink
** the table data structure from the hash table.  But it does destroy
** memory structures of the indices associated with the table.
**
** Indices associated with the table are unlinked from the "db"
** data structure if db!=NULL.  If db==NULL, indices attached to
** the table are deleted, but it is assumed they have already been
** unlinked.
*/
void sqliteDeleteTable(sqlite *db, Table *pTable){
  int i;
  Index *pIndex, *pNext;
  if( pTable==0 ) return;
  for(i=0; i<pTable->nCol; i++){
    sqliteFree(pTable->aCol[i].zName);
    sqliteFree(pTable->aCol[i].zDflt);
  }
  for(pIndex = pTable->pIndex; pIndex; pIndex=pNext){
    pNext = pIndex->pNext;
    sqliteDeleteIndex(db, pIndex);
  }
  sqliteFree(pTable->zName);
  sqliteFree(pTable->aCol);
  sqliteFree(pTable);
}

/*
** Check all Tables and Indexes in the internal hash table and commit
** any additions or deletions to those hash tables.
**
** When executing CREATE TABLE and CREATE INDEX statements, the Table
** and Index structures are created and added to the hash tables, but
** the "isCommit" field is not set.  This routine sets those fields.
** When executing DROP TABLE and DROP INDEX, the "isDelete" fields of
** Table and Index structures is set but the structures are not unlinked
** from the hash tables nor deallocated.  This routine handles that
** deallocation. 
**
** See also: sqliteRollbackInternalChanges()
*/
void sqliteCommitInternalChanges(sqlite *db){
  int i;
  if( (db->flags & SQLITE_InternChanges)==0 ) return;
  for(i=0; i<N_HASH; i++){
    Table *pTable, *pNext;
    for(pTable = db->apTblHash[i]; pTable; pTable=pNext){
      pNext = pTable->pHash;
      if( pTable->isDelete ){
        sqliteDeleteTable(db, pTable);
      }else if( pTable->isCommit==0 ){
        pTable->isCommit = 1;
      }
    }
  }
  for(i=0; i<N_HASH; i++){
    Index *pIndex, *pNext;
    for(pIndex = db->apIdxHash[i]; pIndex; pIndex=pNext){
      pNext = pIndex->pHash;
      if( pIndex->isDelete ){
        sqliteUnlinkAndDeleteIndex(db, pIndex);
      }else if( pIndex->isCommit==0 ){
        pIndex->isCommit = 1;
      }
    }
  }
  db->flags &= ~SQLITE_InternChanges;
}

/*
** This routine runs when one or more CREATE TABLE, CREATE INDEX,
** DROP TABLE, or DROP INDEX statements get rolled back.  The
** additions or deletions of Table and Index structures in the
** internal hash tables are undone.
**
** See also: sqliteCommitInternalChanges()
*/
void sqliteRollbackInternalChanges(sqlite *db){
  int i;
  if( (db->flags & SQLITE_InternChanges)==0 ) return;
  for(i=0; i<N_HASH; i++){
    Table *pTable, *pNext;
    for(pTable = db->apTblHash[i]; pTable; pTable=pNext){
      pNext = pTable->pHash;
      if( !pTable->isCommit ){
        sqliteDeleteTable(db, pTable);
      }else if( pTable->isDelete ){
        pTable->isDelete = 0;
      }
    }
  }
  for(i=0; i<N_HASH; i++){
    Index *pIndex, *pNext;
    for(pIndex = db->apIdxHash[i]; pIndex; pIndex=pNext){
      pNext = pIndex->pHash;
      if( !pIndex->isCommit ){
        sqliteUnlinkAndDeleteIndex(db, pIndex);
      }else if( pIndex->isDelete ){
        pIndex->isDelete = 0;
      }
    }
  }
  db->flags &= ~SQLITE_InternChanges;
}

/*
** Construct the name of a user table or index from a token.
**
** Space to hold the name is obtained from sqliteMalloc() and must
** be freed by the calling function.
*/
char *sqliteTableNameFromToken(Token *pName){
  char *zName = sqliteStrNDup(pName->z, pName->n);
  sqliteDequote(zName);
  return zName;
}

/*
** Begin constructing a new table representation in memory.  This is
** the first of several action routines that get called in response
** to a CREATE TABLE statement.  In particular, this routine is called
** after seeing tokens "CREATE" and "TABLE" and the table name.  The
** pStart token is the CREATE and pName is the table name.
**
** The new table is constructed in files of the pParse structure.  As
** more of the CREATE TABLE statement is parsed, additional action
** routines are called to build up more of the table.
*/
void sqliteStartTable(Parse *pParse, Token *pStart, Token *pName){
  Table *pTable;
  char *zName;
  sqlite *db = pParse->db;

  pParse->sFirstToken = *pStart;
  zName = sqliteTableNameFromToken(pName);
  if( zName==0 ) return;
  pTable = sqliteFindTable(db, zName);
  if( pTable!=0 ){
    sqliteSetNString(&pParse->zErrMsg, "table ", 0, pName->z, pName->n,
        " already exists", 0, 0);
    sqliteFree(zName);
    pParse->nErr++;
    return;
  }
  if( sqliteFindIndex(db, zName) ){
    sqliteSetString(&pParse->zErrMsg, "there is already an index named ", 
       zName, 0);
    sqliteFree(zName);
    pParse->nErr++;
    return;
  }
  pTable = sqliteMalloc( sizeof(Table) );
  if( pTable==0 ) return;
  pTable->zName = zName;
  pTable->pHash = 0;
  pTable->nCol = 0;
  pTable->aCol = 0;
  pTable->pIndex = 0;
  if( pParse->pNewTable ) sqliteDeleteTable(db, pParse->pNewTable);
  pParse->pNewTable = pTable;
  if( !pParse->initFlag && (db->flags & SQLITE_InTrans)==0 ){
    Vdbe *v = sqliteGetVdbe(pParse);
    if( v ){
      sqliteVdbeAddOp(v, OP_Transaction, 0, 0, 0, 0);
    }
  }
}

/*
** Add a new column to the table currently being constructed.
**
** The parser calls this routine once for each column declaration
** in a CREATE TABLE statement.  sqliteStartTable() gets called
** first to get things going.  Then this routine is called for each
** column.
*/
void sqliteAddColumn(Parse *pParse, Token *pName){
  Table *p;
  char **pz;
  if( (p = pParse->pNewTable)==0 ) return;
  if( (p->nCol & 0x7)==0 ){
    p->aCol = sqliteRealloc( p->aCol, (p->nCol+8)*sizeof(p->aCol[0]));
    if( p->aCol==0 ){
      p->nCol = 0;
      return;
    }
  }
  memset(&p->aCol[p->nCol], 0, sizeof(p->aCol[0]));
  pz = &p->aCol[p->nCol++].zName;
  sqliteSetNString(pz, pName->z, pName->n, 0);
  sqliteDequote(*pz);
}

/*
** The given token is the default value for the last column added to
** the table currently under construction.  If "minusFlag" is true, it
** means the value token was preceded by a minus sign.
**
** This routine is called by the parser while in the middle of
** parsing a CREATE TABLE statement.
*/
void sqliteAddDefaultValue(Parse *pParse, Token *pVal, int minusFlag){
  Table *p;
  int i;
  char **pz;
  if( (p = pParse->pNewTable)==0 ) return;
  i = p->nCol-1;
  pz = &p->aCol[i].zDflt;
  if( minusFlag ){
    sqliteSetNString(pz, "-", 1, pVal->z, pVal->n, 0);
  }else{
    sqliteSetNString(pz, pVal->z, pVal->n, 0);
  }
  sqliteDequote(*pz);
}

/*
** This routine is called to report the final ")" that terminates
** a CREATE TABLE statement.
**
** The table structure is added to the internal hash tables.  
**
** An entry for the table is made in the master table on disk,
** unless initFlag==1.  When initFlag==1, it means we are reading
** the master table because we just connected to the database, so 
** the entry for this table already exists in the master table.
** We do not want to create it again.
*/
void sqliteEndTable(Parse *pParse, Token *pEnd){
  Table *p;
  int h;
  sqlite *db = pParse->db;

  if( pEnd==0 || pParse->nErr || sqlite_malloc_failed ) return;
  p = pParse->pNewTable;
  if( p==0 ) return;

  /* Add the table to the in-memory representation of the database
  */
  if( pParse->explain==0 ){
    h = sqliteHashNoCase(p->zName, 0) % N_HASH;
    p->pHash = db->apTblHash[h];
    db->apTblHash[h] = p;
    pParse->pNewTable = 0;
    db->nTable++;
    db->flags |= SQLITE_InternChanges;
  }

  /* If not initializing, then create the table on disk.
  */
  if( !pParse->initFlag ){
    static VdbeOp addTable[] = {
      { OP_Open,        0, 2, 0},
      { OP_NewRecno,    0, 0, 0},
      { OP_String,      0, 0, "table"     },
      { OP_String,      0, 0, 0},            /* 3 */
      { OP_CreateTable, 0, 0, 0},
      { OP_String,      0, 0, 0},            /* 5 */
      { OP_String,      0, 0, 0},            /* 6 */
      { OP_MakeRecord,  4, 0, 0},
      { OP_Put,         0, 0, 0},
    };
    int n, base;
    Vdbe *v;

    v = sqliteGetVdbe(pParse);
    if( v==0 ) return;
    n = (int)pEnd->z - (int)pParse->sFirstToken.z + 1;
    base = sqliteVdbeAddOpList(v, ArraySize(addTable), addTable);
    sqliteVdbeChangeP3(v, base+3, p->zName, 0);
    sqliteVdbeTableRootAddr(v, &p->tnum);
    sqliteVdbeChangeP3(v, base+5, p->zName, 0);
    sqliteVdbeChangeP3(v, base+6, pParse->sFirstToken.z, n);
    sqliteVdbeAddOp(v, OP_Close, 0, 0, 0, 0);
    if( (db->flags & SQLITE_InTrans)==0 ){
      sqliteVdbeAddOp(v, OP_Commit, 0, 0, 0, 0);
    }
  }
}

/*
** Given a token, look up a table with that name.  If not found, leave
** an error for the parser to find and return NULL.
*/
Table *sqliteTableFromToken(Parse *pParse, Token *pTok){
  char *zName;
  Table *pTab;
  zName = sqliteTableNameFromToken(pTok);
  if( zName==0 ) return 0;
  pTab = sqliteFindTable(pParse->db, zName);
  sqliteFree(zName);
  if( pTab==0 ){
    sqliteSetNString(&pParse->zErrMsg, "no such table: ", 0, 
        pTok->z, pTok->n, 0);
    pParse->nErr++;
  }
  return pTab;
}

/*
** This routine is called to do the work of a DROP TABLE statement.
** pName is the name of the table to be dropped.
*/
void sqliteDropTable(Parse *pParse, Token *pName){
  Table *pTable;
  Vdbe *v;
  int base;

  if( pParse->nErr || sqlite_malloc_failed ) return;
  pTable = sqliteTableFromToken(pParse, pName);
  if( pTable==0 ) return;
  if( pTable->readOnly ){
    sqliteSetString(&pParse->zErrMsg, "table ", pTable->zName, 
       " may not be dropped", 0);
    pParse->nErr++;
    return;
  }

  /* Generate code to remove the table from the master table
  ** on disk.
  */
  v = sqliteGetVdbe(pParse);
  if( v ){
    static VdbeOp dropTable[] = {
      { OP_Open,       0, 2,        0},
      { OP_String,     0, 0,        0}, /* 1 */
      { OP_Next,       0, ADDR(9),  0}, /* 2 */
      { OP_Dup,        0, 0,        0},
      { OP_Column,     0, 3,        0},
      { OP_Ne,         0, ADDR(2),  0},
      { OP_Recno,      0, 0,        0},
      { OP_Delete,     0, 0,        0},
      { OP_Goto,       0, ADDR(2),  0},
      { OP_Destroy,    0, 0,        0}, /* 9 */
      { OP_Close,      0, 0,        0},
    };
    Index *pIdx;
    if( (pParse->db->flags & SQLITE_InTrans)==0 ){
      sqliteVdbeAddOp(v, OP_Transaction, 0, 0, 0, 0);
    }
    base = sqliteVdbeAddOpList(v, ArraySize(dropTable), dropTable);
    sqliteVdbeChangeP1(v, base+9, pTable->tnum);
    for(pIdx=pTable->pIndex; pIdx; pIdx=pIdx->pNext){
      sqliteVdbeAddOp(v, OP_Destroy, pIdx->tnum, 0, 0, 0);
    }
    if( (pParse->db->flags & SQLITE_InTrans)==0 ){
      sqliteVdbeAddOp(v, OP_Commit, 0, 0, 0, 0);
    }
  }

  /* Mark the in-memory Table structure as being deleted.  The actually
  ** deletion occurs inside of sqliteCommitInternalChanges().
  **
  ** Exception: if the SQL statement began with the EXPLAIN keyword,
  ** then no changes should be made.
  */
  if( !pParse->explain ){
    pTable->isDelete = 1;
    pParse->db->flags |= SQLITE_InternChanges;
  }
}

/*
** Create a new index for an SQL table.  pIndex is the name of the index 
** and pTable is the name of the table that is to be indexed.  Both will 
** be NULL for a primary key.  In that case, use pParse->pNewTable as the 
** table to be indexed.
**
** pList is a list of columns to be indexed.  pList will be NULL if the
** most recently added column of the table is labeled as the primary key.
*/
void sqliteCreateIndex(
  Parse *pParse,   /* All information about this parse */
  Token *pName,    /* Name of the index.  May be NULL */
  Token *pTable,   /* Name of the table to index.  Use pParse->pNewTable if 0 */
  IdList *pList,   /* A list of columns to be indexed */
  Token *pStart,   /* The CREATE token that begins a CREATE TABLE statement */
  Token *pEnd      /* The ")" that closes the CREATE INDEX statement */
){
  Table *pTab;     /* Table to be indexed */
  Index *pIndex;   /* The index to be created */
  char *zName = 0;
  int i, j, h;
  Token nullId;    /* Fake token for an empty ID list */
  sqlite *db = pParse->db;

  if( pParse->nErr || sqlite_malloc_failed ) goto exit_create_index;

  /*
  ** Find the table that is to be indexed.  Return early if not found.
  */
  if( pTable!=0 ){
    pTab =  sqliteTableFromToken(pParse, pTable);
  }else{
    pTab =  pParse->pNewTable;
  }
  if( pTab==0 || pParse->nErr ) goto exit_create_index;
  if( pTab->readOnly ){
    sqliteSetString(&pParse->zErrMsg, "table ", pTab->zName, 
      " may not have new indices added", 0);
    pParse->nErr++;
    goto exit_create_index;
  }

  /*
  ** Find the name of the index.  Make sure there is not already another
  ** index or table with the same name.
  */
  if( pName ){
    zName = sqliteTableNameFromToken(pName);
  }else{
    zName = 0;
    sqliteSetString(&zName, pTab->zName, "__primary_key", 0);
  }
  if( zName==0 ) goto exit_create_index;
  if( sqliteFindIndex(db, zName) ){
    sqliteSetString(&pParse->zErrMsg, "index ", zName, 
       " already exists", 0);
    pParse->nErr++;
    goto exit_create_index;
  }
  if( sqliteFindTable(db, zName) ){
    sqliteSetString(&pParse->zErrMsg, "there is already a table named ",
       zName, 0);
    pParse->nErr++;
    goto exit_create_index;
  }

  /* If pList==0, it means this routine was called to make a primary
  ** key out of the last column added to the table under construction.
  ** So create a fake list to simulate this.
  */
  if( pList==0 ){
    nullId.z = pTab->aCol[pTab->nCol-1].zName;
    nullId.n = strlen(nullId.z);
    pList = sqliteIdListAppend(0, &nullId);
    if( pList==0 ) goto exit_create_index;
  }

  /* 
  ** Allocate the index structure. 
  */
  pIndex = sqliteMalloc( sizeof(Index) + strlen(zName) + 1 +
                        sizeof(int)*pList->nId );
  if( pIndex==0 ) goto exit_create_index;
  pIndex->aiColumn = (int*)&pIndex[1];
  pIndex->zName = (char*)&pIndex->aiColumn[pList->nId];
  strcpy(pIndex->zName, zName);
  pIndex->pTable = pTab;
  pIndex->nColumn = pList->nId;

  /* Scan the names of the columns of the table to be indexed and
  ** load the column indices into the Index structure.  Report an error
  ** if any column is not found.
  */
  for(i=0; i<pList->nId; i++){
    for(j=0; j<pTab->nCol; j++){
      if( sqliteStrICmp(pList->a[i].zName, pTab->aCol[j].zName)==0 ) break;
    }
    if( j>=pTab->nCol ){
      sqliteSetString(&pParse->zErrMsg, "table ", pTab->zName, 
        " has no column named ", pList->a[i].zName, 0);
      pParse->nErr++;
      sqliteFree(pIndex);
      goto exit_create_index;
    }
    pIndex->aiColumn[i] = j;
  }

  /* Link the new Index structure to its table and to the other
  ** in-memory database structures.
  */
  if( pParse->explain==0 ){
    h = sqliteHashNoCase(pIndex->zName, 0) % N_HASH;
    pIndex->pHash = db->apIdxHash[h];
    db->apIdxHash[h] = pIndex;
    pIndex->pNext = pTab->pIndex;
    pTab->pIndex = pIndex;
    db->flags |= SQLITE_InternChanges;
  }

  /* If the initFlag is 0 then create the index on disk.  This
  ** involves writing the index into the master table and filling in the
  ** index with the current table contents.
  **
  ** The initFlag is 0 when the user first enters a CREATE INDEX 
  ** command.  The initFlag is 1 when a database is opened and 
  ** CREATE INDEX statements are read out of the master table.  In
  ** the latter case the index already exists on disk, which is why
  ** we don't want to recreate it.
  */
  if( pParse->initFlag==0 ){
    static VdbeOp addTable[] = {
      { OP_Open,        2, 2, 0},
      { OP_NewRecno,    2, 0, 0},
      { OP_String,      0, 0, "index"},
      { OP_String,      0, 0, 0},  /* 3 */
      { OP_CreateIndex, 0, 0, 0},
      { OP_String,      0, 0, 0},  /* 5 */
      { OP_String,      0, 0, 0},  /* 6 */
      { OP_MakeRecord,  5, 0, 0},
      { OP_Put,         2, 0, 0},
      { OP_Close,       2, 0, 0},
    };
    int n;
    Vdbe *v = pParse->pVdbe;
    int lbl1, lbl2;
    int i;

    v = sqliteGetVdbe(pParse);
    if( v==0 ) goto exit_create_index;
    if( pTable!=0 && (db->flags & SQLITE_InTrans)==0 ){
      sqliteVdbeAddOp(v, OP_Transaction, 0, 0, 0, 0);
    }
    sqliteVdbeAddOp(v, OP_Open, 0, pTab->tnum, pTab->zName, 0);
    sqliteVdbeAddOp(v, OP_Open, 1, pIndex->tnum, pIndex->zName, 0);
    if( pStart && pEnd ){
      int base;
      n = (int)pEnd->z - (int)pStart->z + 1;
      base = sqliteVdbeAddOpList(v, ArraySize(addTable), addTable);
      sqliteVdbeChangeP3(v, base+3, pIndex->zName, 0);
      sqliteVdbeIndexRootAddr(v, &pIndex->tnum);
      sqliteVdbeChangeP3(v, base+5, pTab->zName, 0);
      sqliteVdbeChangeP3(v, base+6, pStart->z, n);
    }
    lbl1 = sqliteVdbeMakeLabel(v);
    lbl2 = sqliteVdbeMakeLabel(v);
    sqliteVdbeAddOp(v, OP_Next, 0, lbl2, 0, lbl1);
    sqliteVdbeAddOp(v, OP_Recno, 0, 0, 0, 0);
    for(i=0; i<pIndex->nColumn; i++){
      sqliteVdbeAddOp(v, OP_Column, 0, pIndex->aiColumn[i], 0, 0);
    }
    sqliteVdbeAddOp(v, OP_MakeIdxKey, pIndex->nColumn, 0, 0, 0);
    sqliteVdbeAddOp(v, OP_PutIdx, 1, 0, 0, 0);
    sqliteVdbeAddOp(v, OP_Goto, 0, lbl1, 0, 0);
    sqliteVdbeAddOp(v, OP_Noop, 0, 0, 0, lbl2);
    sqliteVdbeAddOp(v, OP_Close, 1, 0, 0, 0);
    sqliteVdbeAddOp(v, OP_Close, 0, 0, 0, 0);
    if( pTable!=0 && (db->flags & SQLITE_InTrans)==0 ){
      sqliteVdbeAddOp(v, OP_Commit, 0, 0, 0, 0);
    }
  }

  /* Reclaim memory on an EXPLAIN call.
  */
  if( pParse->explain ){
    sqliteFree(pIndex);
  }

  /* Clean up before exiting */
exit_create_index:
  sqliteIdListDelete(pList);
  sqliteFree(zName);
  return;
}

/*
** This routine will drop an existing named index.
*/
void sqliteDropIndex(Parse *pParse, Token *pName){
  Index *pIndex;
  char *zName;
  Vdbe *v;
  sqlite *db = pParse->db;

  if( pParse->nErr || sqlite_malloc_failed ) return;
  zName = sqliteTableNameFromToken(pName);
  if( zName==0 ) return;
  pIndex = sqliteFindIndex(db, zName);
  sqliteFree(zName);
  if( pIndex==0 ){
    sqliteSetNString(&pParse->zErrMsg, "no such index: ", 0, 
        pName->z, pName->n, 0);
    pParse->nErr++;
    return;
  }

  /* Generate code to remove the index and from the master table */
  v = sqliteGetVdbe(pParse);
  if( v ){
    static VdbeOp dropIndex[] = {
      { OP_Open,       0, 2,       0},
      { OP_String,     0, 0,       0}, /* 1 */
      { OP_Next,       0, ADDR(8), 0}, /* 2 */
      { OP_Dup,        0, 0,       0},
      { OP_Column,     0, 1,       0},
      { OP_Ne,         0, ADDR(2), 0},
      { OP_Recno,      0, 0,       0},
      { OP_Delete,     0, 0,       0},
      { OP_Destroy,    0, 0,       0}, /* 8 */
      { OP_Close,      0, 0,       0},
    };
    int base;

    if( (db->flags & SQLITE_InTrans)==0 ){
      sqliteVdbeAddOp(v, OP_Transaction, 0, 0, 0, 0);
    }
    base = sqliteVdbeAddOpList(v, ArraySize(dropIndex), dropIndex);
    sqliteVdbeChangeP1(v, base+8, pIndex->tnum);
    if( (db->flags & SQLITE_InTrans)==0 ){
      sqliteVdbeAddOp(v, OP_Commit, 0, 0, 0, 0);
    }
  }

  /* Mark the internal Index structure for deletion by the
  ** sqliteCommitInternalChanges routine.
  */
  if( !pParse->explain ){
    pIndex->isDelete = 1;
    db->flags |= SQLITE_InternChanges;
  }
}

/*
** Add a new element to the end of an expression list.  If pList is
** initially NULL, then create a new expression list.
*/
ExprList *sqliteExprListAppend(ExprList *pList, Expr *pExpr, Token *pName){
  int i;
  if( pList==0 ){
    pList = sqliteMalloc( sizeof(ExprList) );
    if( pList==0 ) return 0;
  }
  if( (pList->nExpr & 7)==0 ){
    int n = pList->nExpr + 8;
    pList->a = sqliteRealloc(pList->a, n*sizeof(pList->a[0]));
    if( pList->a==0 ){
      pList->nExpr = 0;
      return pList;
    }
  }
  i = pList->nExpr++;
  pList->a[i].pExpr = pExpr;
  pList->a[i].zName = 0;
  if( pName ){
    sqliteSetNString(&pList->a[i].zName, pName->z, pName->n, 0);
    sqliteDequote(pList->a[i].zName);
  }
  return pList;
}

/*
** Delete an entire expression list.
*/
void sqliteExprListDelete(ExprList *pList){
  int i;
  if( pList==0 ) return;
  for(i=0; i<pList->nExpr; i++){
    sqliteExprDelete(pList->a[i].pExpr);
    sqliteFree(pList->a[i].zName);
  }
  sqliteFree(pList->a);
  sqliteFree(pList);
}

/*
** Append a new element to the given IdList.  Create a new IdList if
** need be.
**
** A new IdList is returned, or NULL if malloc() fails.
*/
IdList *sqliteIdListAppend(IdList *pList, Token *pToken){
  if( pList==0 ){
    pList = sqliteMalloc( sizeof(IdList) );
    if( pList==0 ) return 0;
  }
  if( (pList->nId & 7)==0 ){
    pList->a = sqliteRealloc(pList->a, (pList->nId+8)*sizeof(pList->a[0]) );
    if( pList->a==0 ){
      pList->nId = 0;
      sqliteIdListDelete(pList);
      return 0;
    }
  }
  memset(&pList->a[pList->nId], 0, sizeof(pList->a[0]));
  if( pToken ){
    char **pz = &pList->a[pList->nId].zName;
    sqliteSetNString(pz, pToken->z, pToken->n, 0);
    if( *pz==0 ){
      sqliteIdListDelete(pList);
      return 0;
    }else{
      sqliteDequote(*pz);
    }
  }
  pList->nId++;
  return pList;
}

/*
** Add an alias to the last identifier on the given identifier list.
*/
void sqliteIdListAddAlias(IdList *pList, Token *pToken){
  if( pList && pList->nId>0 ){
    int i = pList->nId - 1;
    sqliteSetNString(&pList->a[i].zAlias, pToken->z, pToken->n, 0);
    sqliteDequote(pList->a[i].zAlias);
  }
}

/*
** Delete an entire IdList
*/
void sqliteIdListDelete(IdList *pList){
  int i;
  if( pList==0 ) return;
  for(i=0; i<pList->nId; i++){
    sqliteFree(pList->a[i].zName);
    sqliteFree(pList->a[i].zAlias);
    if( pList->a[i].pSelect ){
      sqliteFree(pList->a[i].zName);
      sqliteSelectDelete(pList->a[i].pSelect);
      sqliteDeleteTable(0, pList->a[i].pTab);
    }
  }
  sqliteFree(pList->a);
  sqliteFree(pList);
}


/*
** The COPY command is for compatibility with PostgreSQL and specificially
** for the ability to read the output of pg_dump.  The format is as
** follows:
**
**    COPY table FROM file [USING DELIMITERS string]
**
** "table" is an existing table name.  We will read lines of code from
** file to fill this table with data.  File might be "stdin".  The optional
** delimiter string identifies the field separators.  The default is a tab.
*/
void sqliteCopy(
  Parse *pParse,       /* The parser context */
  Token *pTableName,   /* The name of the table into which we will insert */
  Token *pFilename,    /* The file from which to obtain information */
  Token *pDelimiter    /* Use this as the field delimiter */
){
  Table *pTab;
  char *zTab;
  int i, j;
  Vdbe *v;
  int addr, end;
  Index *pIdx;
  sqlite *db = pParse->db;

  zTab = sqliteTableNameFromToken(pTableName);
  if( sqlite_malloc_failed || zTab==0 ) goto copy_cleanup;
  pTab = sqliteFindTable(db, zTab);
  sqliteFree(zTab);
  if( pTab==0 ){
    sqliteSetNString(&pParse->zErrMsg, "no such table: ", 0, 
        pTableName->z, pTableName->n, 0);
    pParse->nErr++;
    goto copy_cleanup;
  }
  if( pTab->readOnly ){
    sqliteSetString(&pParse->zErrMsg, "table ", pTab->zName,
        " may not be modified", 0);
    pParse->nErr++;
    goto copy_cleanup;
  }
  v = sqliteGetVdbe(pParse);
  if( v ){
    if( (db->flags & SQLITE_InTrans)==0 ){
      sqliteVdbeAddOp(v, OP_Transaction, 0, 0, 0, 0);
    }
    addr = sqliteVdbeAddOp(v, OP_FileOpen, 0, 0, 0, 0);
    sqliteVdbeChangeP3(v, addr, pFilename->z, pFilename->n);
    sqliteVdbeDequoteP3(v, addr);
    sqliteVdbeAddOp(v, OP_Open, 0, pTab->tnum, pTab->zName, 0);
    for(i=1, pIdx=pTab->pIndex; pIdx; pIdx=pIdx->pNext, i++){
      sqliteVdbeAddOp(v, OP_Open, i, pIdx->tnum, pIdx->zName, 0);
    }
    end = sqliteVdbeMakeLabel(v);
    addr = sqliteVdbeAddOp(v, OP_FileRead, pTab->nCol, end, 0, 0);
    if( pDelimiter ){
      sqliteVdbeChangeP3(v, addr, pDelimiter->z, pDelimiter->n);
      sqliteVdbeDequoteP3(v, addr);
    }else{
      sqliteVdbeChangeP3(v, addr, "\t", 1);
    }
    sqliteVdbeAddOp(v, OP_NewRecno, 0, 0, 0, 0);
    if( pTab->pIndex ){
      sqliteVdbeAddOp(v, OP_Dup, 0, 0, 0, 0);
    }
    for(i=0; i<pTab->nCol; i++){
      sqliteVdbeAddOp(v, OP_FileColumn, i, 0, 0, 0);
    }
    sqliteVdbeAddOp(v, OP_MakeRecord, pTab->nCol, 0, 0, 0);
    sqliteVdbeAddOp(v, OP_Put, 0, 0, 0, 0);
    for(i=1, pIdx=pTab->pIndex; pIdx; pIdx=pIdx->pNext, i++){
      if( pIdx->pNext ){
        sqliteVdbeAddOp(v, OP_Dup, 0, 0, 0, 0);
      }
      for(j=0; j<pIdx->nColumn; j++){
        sqliteVdbeAddOp(v, OP_FileColumn, pIdx->aiColumn[j], 0, 0, 0);
      }
      sqliteVdbeAddOp(v, OP_MakeIdxKey, pIdx->nColumn, 0, 0, 0);
      sqliteVdbeAddOp(v, OP_PutIdx, i, 0, 0, 0);
    }
    sqliteVdbeAddOp(v, OP_Goto, 0, addr, 0, 0);
    sqliteVdbeAddOp(v, OP_Noop, 0, 0, 0, end);
    if( (db->flags & SQLITE_InTrans)==0 ){
      sqliteVdbeAddOp(v, OP_Commit, 0, 0, 0, 0);
    }
  }
  
copy_cleanup:
  return;
}

/*
** The non-standard VACUUM command is used to clean up the database,
** collapse free space, etc.  It is modelled after the VACUUM command
** in PostgreSQL.
*/
void sqliteVacuum(Parse *pParse, Token *pTableName){
  char *zName;
  Vdbe *v;
  sqlite *db = pParse->db;

  if( pParse->nErr || sqlite_malloc_failed ) return;
  if( pTableName ){
    zName = sqliteTableNameFromToken(pTableName);
  }else{
    zName = 0;
  }
  if( zName && sqliteFindIndex(db, zName)==0
    && sqliteFindTable(db, zName)==0 ){
    sqliteSetString(&pParse->zErrMsg, "no such table or index: ", zName, 0);
    pParse->nErr++;
    goto vacuum_cleanup;
  }
  v = sqliteGetVdbe(pParse);
  if( v==0 ) goto vacuum_cleanup;
  if( (db->flags & SQLITE_InTrans)==0 ){
    sqliteVdbeAddOp(v, OP_Transaction, 0, 0, 0, 0);
  }
  if( zName ){
    sqliteVdbeAddOp(v, OP_Reorganize, 0, 0, zName, 0);
  }else{
    int h;
    Table *pTab;
    Index *pIdx;
    for(h=0; h<N_HASH; h++){
      for(pTab=db->apTblHash[h]; pTab; pTab=pTab->pHash){
        sqliteVdbeAddOp(v, OP_Reorganize, 0, 0, pTab->zName, 0);
        for(pIdx=pTab->pIndex; pIdx; pIdx=pIdx->pNext){
          sqliteVdbeAddOp(v, OP_Reorganize, 0, 0, pIdx->zName, 0);
        }
      }
    }
  }
  if( (db->flags & SQLITE_InTrans)==0 ){
    sqliteVdbeAddOp(v, OP_Commit, 0, 0, 0, 0);
  }

vacuum_cleanup:
  sqliteFree(zName);
  return;
}

/*
** Begin a transaction
*/
void sqliteBeginTransaction(Parse *pParse){
  sqlite *db;
  Vdbe *v;

  if( pParse==0 || (db=pParse->db)==0 || db->pBe==0 ) return;
  if( pParse->nErr || sqlite_malloc_failed ) return;
  if( db->flags & SQLITE_InTrans ) return;
  v = sqliteGetVdbe(pParse);
  if( v ){
    sqliteVdbeAddOp(v, OP_Transaction, 1, 0, 0, 0);
  }
  db->flags |= SQLITE_InTrans;
}

/*
** Commit a transaction
*/
void sqliteCommitTransaction(Parse *pParse){
  sqlite *db;
  Vdbe *v;

  if( pParse==0 || (db=pParse->db)==0 || db->pBe==0 ) return;
  if( pParse->nErr || sqlite_malloc_failed ) return;
  if( (db->flags & SQLITE_InTrans)==0 ) return;
  v = sqliteGetVdbe(pParse);
  if( v ){
    sqliteVdbeAddOp(v, OP_Commit, 0, 0, 0, 0);
  }
  db->flags &= ~SQLITE_InTrans;
}

/*
** Rollback a transaction
*/
void sqliteRollbackTransaction(Parse *pParse){
  sqlite *db;
  Vdbe *v;

  if( pParse==0 || (db=pParse->db)==0 || db->pBe==0 ) return;
  if( pParse->nErr || sqlite_malloc_failed ) return;
  if( (db->flags & SQLITE_InTrans)==0 ) return;
  v = sqliteGetVdbe(pParse);
  if( v ){
    sqliteVdbeAddOp(v, OP_Rollback, 0, 0, 0, 0);
  }
  db->flags &= ~SQLITE_InTrans;
}

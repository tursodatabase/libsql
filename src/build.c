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
** $Id: build.c,v 1.23 2000/08/03 15:09:20 drh Exp $
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
  if( pParse->pVdbe ){
    if( pParse->explain ){
      sqliteVdbeList(pParse->pVdbe, pParse->xCallback, pParse->pArg, 
                     &pParse->zErrMsg);
    }else{
      FILE *trace = (pParse->db->flags & SQLITE_VdbeTrace)!=0 ? stderr : 0;
      sqliteVdbeTrace(pParse->pVdbe, trace);
      sqliteVdbeExec(pParse->pVdbe, pParse->xCallback, pParse->pArg, 
                     &pParse->zErrMsg, pParse->db->pBusyArg,
                     pParse->db->xBusyCallback);
    }
    sqliteVdbeDelete(pParse->pVdbe);
    pParse->pVdbe = 0;
    pParse->colNamesSet = 0;
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
  pExpr->span.z = pLeft->z;
  pExpr->span.n = pRight->n + (int)pRight->z - (int)pLeft->z;
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
** The index is removed from the database hash table, but it is
** not unlinked from the Table that is being indexed.  Unlinking
** from the Table must be done by the calling function.
*/
static void sqliteDeleteIndex(sqlite *db, Index *pIndex){
  int h;
  if( pIndex->zName ){
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
** Remove the memory data structures associated with the given
** Table.  No changes are made to disk by this routine.
**
** This routine just deletes the data structure.  It does not unlink
** the table data structure from the hash table.  But does it destroy
** memory structures of the indices associated with the table.
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
** to a CREATE TABLE statement.
*/
void sqliteStartTable(Parse *pParse, Token *pStart, Token *pName){
  Table *pTable;
  char *zName;

  pParse->sFirstToken = *pStart;
  zName = sqliteTableNameFromToken(pName);
  pTable = sqliteFindTable(pParse->db, zName);
  if( pTable!=0 ){
    sqliteSetNString(&pParse->zErrMsg, "table ", 0, pName->z, pName->n,
        " already exists", 0, 0);
    sqliteFree(zName);
    pParse->nErr++;
    return;
  }
  if( sqliteFindIndex(pParse->db, zName) ){
    sqliteSetString(&pParse->zErrMsg, "there is already an index named ", 
       zName, 0);
    sqliteFree(zName);
    pParse->nErr++;
    return;
  }
  pTable = sqliteMalloc( sizeof(Table) );
  if( pTable==0 ){
    sqliteSetString(&pParse->zErrMsg, "out of memory", 0);
    pParse->nErr++;
    return;
  }
  pTable->zName = zName;
  pTable->pHash = 0;
  pTable->nCol = 0;
  pTable->aCol = 0;
  pTable->pIndex = 0;
  if( pParse->pNewTable ) sqliteDeleteTable(pParse->db, pParse->pNewTable);
  pParse->pNewTable = pTable;
}

/*
** Add a new column to the table currently being constructed.
*/
void sqliteAddColumn(Parse *pParse, Token *pName){
  Table *p;
  char **pz;
  if( (p = pParse->pNewTable)==0 ) return;
  if( (p->nCol & 0x7)==0 ){
    p->aCol = sqliteRealloc( p->aCol, (p->nCol+8)*sizeof(p->aCol[0]));
  }
  if( p->aCol==0 ){
    p->nCol = 0;
    return;
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
  int addMeta;       /* True to insert a meta records into the file */

  if( pParse->nErr ) return;
  p = pParse->pNewTable;
  addMeta =  p!=0 && pParse->db->nTable==1;

  /* Add the table to the in-memory representation of the database
  */
  if( p!=0 && pParse->explain==0 ){
    h = sqliteHashNoCase(p->zName, 0) % N_HASH;
    p->pHash = pParse->db->apTblHash[h];
    pParse->db->apTblHash[h] = p;
    pParse->pNewTable = 0;
    pParse->db->nTable++;
  }

  /* If not initializing, then create the table on disk.
  */
  if( !pParse->initFlag ){
    static VdbeOp addTable[] = {
      { OP_Open,        0, 1, MASTER_NAME },
      { OP_New,         0, 0, 0},
      { OP_String,      0, 0, "table"     },
      { OP_String,      0, 0, 0},            /* 3 */
      { OP_String,      0, 0, 0},            /* 4 */
      { OP_String,      0, 0, 0},            /* 5 */
      { OP_MakeRecord,  4, 0, 0},
      { OP_Put,         0, 0, 0},
    };
    static VdbeOp addVersion[] = {
      { OP_New,         0, 0, 0},
      { OP_String,      0, 0, "meta"            },
      { OP_String,      0, 0, ""                },
      { OP_String,      0, 0, ""                },
      { OP_String,      0, 0, "file format 2"   },
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
    sqliteVdbeChangeP3(v, base+4, p->zName, 0);
    sqliteVdbeChangeP3(v, base+5, pParse->sFirstToken.z, n);
    if( addMeta ){
      sqliteVdbeAddOpList(v, ArraySize(addVersion), addVersion);
    }
    sqliteVdbeAddOp(v, OP_Close, 0, 0, 0, 0);
  }
}

/*
** Given a token, look up a table with that name.  If not found, leave
** an error for the parser to find and return NULL.
*/
Table *sqliteTableFromToken(Parse *pParse, Token *pTok){
  char *zName = sqliteTableNameFromToken(pTok);
  Table *pTab = sqliteFindTable(pParse->db, zName);
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
*/
void sqliteDropTable(Parse *pParse, Token *pName){
  Table *pTable;
  int h;
  Vdbe *v;
  int base;

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
      { OP_Open,       0, 1,        MASTER_NAME },
      { OP_ListOpen,   0, 0,        0},
      { OP_String,     0, 0,        0}, /* 2 */
      { OP_Next,       0, ADDR(10), 0}, /* 3 */
      { OP_Dup,        0, 0,        0},
      { OP_Field,      0, 2,        0},
      { OP_Ne,         0, ADDR(3),  0},
      { OP_Key,        0, 0,        0},
      { OP_ListWrite,  0, 0,        0},
      { OP_Goto,       0, ADDR(3),  0},
      { OP_ListRewind, 0, 0,        0}, /* 10 */
      { OP_ListRead,   0, ADDR(14), 0}, /* 11 */
      { OP_Delete,     0, 0,        0},
      { OP_Goto,       0, ADDR(11), 0},
      { OP_Destroy,    0, 0,        0}, /* 14 */
      { OP_Close,      0, 0,        0},
    };
    Index *pIdx;
    base = sqliteVdbeAddOpList(v, ArraySize(dropTable), dropTable);
    sqliteVdbeChangeP3(v, base+2, pTable->zName, 0);
    sqliteVdbeChangeP3(v, base+14, pTable->zName, 0);
    for(pIdx=pTable->pIndex; pIdx; pIdx=pIdx->pNext){
      sqliteVdbeAddOp(v, OP_Destroy, 0, 0, pIdx->zName, 0);
    }
  }

  /* Remove the in-memory table structure and free its memory.
  **
  ** Exception: if the SQL statement began with the EXPLAIN keyword,
  ** then no changes are made.
  */
  if( !pParse->explain ){
    h = sqliteHashNoCase(pTable->zName, 0) % N_HASH;
    if( pParse->db->apTblHash[h]==pTable ){
      pParse->db->apTblHash[h] = pTable->pHash;
    }else{
      Table *p;
      for(p=pParse->db->apTblHash[h]; p && p->pHash!=pTable; p=p->pHash){}
      if( p && p->pHash==pTable ){
        p->pHash = pTable->pHash;
      }
    }
    pParse->db->nTable--;
    sqliteDeleteTable(pParse->db, pTable);
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
  if( sqliteFindIndex(pParse->db, zName) ){
    sqliteSetString(&pParse->zErrMsg, "index ", zName, 
       " already exists", 0);
    pParse->nErr++;
    goto exit_create_index;
  }
  if( sqliteFindTable(pParse->db, zName) ){
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
  if( pIndex==0 ){
    sqliteSetString(&pParse->zErrMsg, "out of memory", 0);
    pParse->nErr++;
    goto exit_create_index;
  }
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
    pIndex->pHash = pParse->db->apIdxHash[h];
    pParse->db->apIdxHash[h] = pIndex;
    pIndex->pNext = pTab->pIndex;
    pTab->pIndex = pIndex;
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
      { OP_Open,        2, 1, MASTER_NAME},
      { OP_New,         2, 0, 0},
      { OP_String,      0, 0, "index"},
      { OP_String,      0, 0, 0},  /* 3 */
      { OP_String,      0, 0, 0},  /* 4 */
      { OP_String,      0, 0, 0},  /* 5 */
      { OP_MakeRecord,  4, 0, 0},
      { OP_Put,         2, 0, 0},
      { OP_Close,       2, 0, 0},
    };
    int n;
    Vdbe *v = pParse->pVdbe;
    int lbl1, lbl2;
    int i;

    v = sqliteGetVdbe(pParse);
    if( v==0 ) goto exit_create_index;
    sqliteVdbeAddOp(v, OP_Open, 0, 0, pTab->zName, 0);
    sqliteVdbeAddOp(v, OP_Open, 1, 1, pIndex->zName, 0);
    if( pStart && pEnd ){
      int base;
      n = (int)pEnd->z - (int)pStart->z + 1;
      base = sqliteVdbeAddOpList(v, ArraySize(addTable), addTable);
      sqliteVdbeChangeP3(v, base+3, pIndex->zName, 0);
      sqliteVdbeChangeP3(v, base+4, pTab->zName, 0);
      sqliteVdbeChangeP3(v, base+5, pStart->z, n);
    }
    lbl1 = sqliteVdbeMakeLabel(v);
    lbl2 = sqliteVdbeMakeLabel(v);
    sqliteVdbeAddOp(v, OP_Next, 0, lbl2, 0, lbl1);
    sqliteVdbeAddOp(v, OP_Key, 0, 0, 0, 0);
    for(i=0; i<pIndex->nColumn; i++){
      sqliteVdbeAddOp(v, OP_Field, 0, pIndex->aiColumn[i], 0, 0);
    }
    sqliteVdbeAddOp(v, OP_MakeKey, pIndex->nColumn, 0, 0, 0);
    sqliteVdbeAddOp(v, OP_PutIdx, 1, 0, 0, 0);
    sqliteVdbeAddOp(v, OP_Goto, 0, lbl1, 0, 0);
    sqliteVdbeAddOp(v, OP_Noop, 0, 0, 0, lbl2);
    sqliteVdbeAddOp(v, OP_Close, 1, 0, 0, 0);
    sqliteVdbeAddOp(v, OP_Close, 0, 0, 0, 0);
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

  zName = sqliteTableNameFromToken(pName);
  pIndex = sqliteFindIndex(pParse->db, zName);
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
      { OP_Open,       0, 1,       MASTER_NAME},
      { OP_ListOpen,   0, 0,       0},
      { OP_String,     0, 0,       0}, /* 2 */
      { OP_Next,       0, ADDR(9), 0}, /* 3 */
      { OP_Dup,        0, 0,       0},
      { OP_Field,      0, 1,       0},
      { OP_Ne,         0, ADDR(3), 0},
      { OP_Key,        0, 0,       0},
      { OP_Delete,     0, 0,       0},
      { OP_Destroy,    0, 0,       0}, /* 9 */
      { OP_Close,      0, 0,       0},
    };
    int base;

    base = sqliteVdbeAddOpList(v, ArraySize(dropIndex), dropIndex);
    sqliteVdbeChangeP3(v, base+2, pIndex->zName, 0);
    sqliteVdbeChangeP3(v, base+9, pIndex->zName, 0);
  }

  /* Remove the index structure and free its memory.  Except if the
  ** EXPLAIN keyword is present, no changes are made.
  */
  if( !pParse->explain ){
    if( pIndex->pTable->pIndex==pIndex ){
      pIndex->pTable->pIndex = pIndex->pNext;
    }else{
      Index *p;
      for(p=pIndex->pTable->pIndex; p && p->pNext!=pIndex; p=p->pNext){}
      if( p && p->pNext==pIndex ){
        p->pNext = pIndex->pNext;
      }
    }
    sqliteDeleteIndex(pParse->db, pIndex);
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
  }
  if( pList==0 ) return 0;
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
      return pList;
    }
  }
  memset(&pList->a[pList->nId], 0, sizeof(pList->a[0]));
  if( pToken ){
    sqliteSetNString(&pList->a[pList->nId].zName, pToken->z, pToken->n, 0);
    sqliteDequote(pList->a[pList->nId].zName);
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

  zTab = sqliteTableNameFromToken(pTableName);
  pTab = sqliteFindTable(pParse->db, zTab);
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
    addr = sqliteVdbeAddOp(v, OP_FileOpen, 0, 0, 0, 0);
    sqliteVdbeChangeP3(v, addr, pFilename->z, pFilename->n);
    sqliteVdbeDequoteP3(v, addr);
    sqliteVdbeAddOp(v, OP_Open, 0, 1, pTab->zName, 0);
    for(i=1, pIdx=pTab->pIndex; pIdx; pIdx=pIdx->pNext, i++){
      sqliteVdbeAddOp(v, OP_Open, i, 1, pIdx->zName, 0);
    }
    end = sqliteVdbeMakeLabel(v);
    addr = sqliteVdbeAddOp(v, OP_FileRead, pTab->nCol, end, 0, 0);
    if( pDelimiter ){
      sqliteVdbeChangeP3(v, addr, pDelimiter->z, pDelimiter->n);
      sqliteVdbeDequoteP3(v, addr);
    }else{
      sqliteVdbeChangeP3(v, addr, "\t", 1);
    }
    sqliteVdbeAddOp(v, OP_New, 0, 0, 0, 0);
    if( pTab->pIndex ){
      sqliteVdbeAddOp(v, OP_Dup, 0, 0, 0, 0);
    }
    for(i=0; i<pTab->nCol; i++){
      sqliteVdbeAddOp(v, OP_FileField, i, 0, 0, 0);
    }
    sqliteVdbeAddOp(v, OP_MakeRecord, pTab->nCol, 0, 0, 0);
    sqliteVdbeAddOp(v, OP_Put, 0, 0, 0, 0);
    for(i=1, pIdx=pTab->pIndex; pIdx; pIdx=pIdx->pNext, i++){
      if( pIdx->pNext ){
        sqliteVdbeAddOp(v, OP_Dup, 0, 0, 0, 0);
      }
      for(j=0; j<pIdx->nColumn; j++){
        sqliteVdbeAddOp(v, OP_FileField, pIdx->aiColumn[j], 0, 0, 0);
      }
      sqliteVdbeAddOp(v, OP_MakeKey, pIdx->nColumn, 0, 0, 0);
      sqliteVdbeAddOp(v, OP_PutIdx, i, 0, 0, 0);
    }
    sqliteVdbeAddOp(v, OP_Goto, 0, addr, 0, 0);
    sqliteVdbeAddOp(v, OP_Noop, 0, 0, 0, end);
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

  if( pTableName ){
    zName = sqliteTableNameFromToken(pTableName);
  }else{
    zName = 0;
  }
  if( zName && sqliteFindIndex(pParse->db, zName)==0
    && sqliteFindTable(pParse->db, zName)==0 ){
    sqliteSetString(&pParse->zErrMsg, "no such table or index: ", zName, 0);
    pParse->nErr++;
    goto vacuum_cleanup;
  }
  v = sqliteGetVdbe(pParse);
  if( v==0 ) goto vacuum_cleanup;
  if( zName ){
    sqliteVdbeAddOp(v, OP_Reorganize, 0, 0, zName, 0);
  }else{
    int h;
    Table *pTab;
    Index *pIdx;
    for(h=0; h<N_HASH; h++){
      for(pTab=pParse->db->apTblHash[h]; pTab; pTab=pTab->pHash){
        sqliteVdbeAddOp(v, OP_Reorganize, 0, 0, pTab->zName, 0);
        for(pIdx=pTab->pIndex; pIdx; pIdx=pIdx->pNext){
          sqliteVdbeAddOp(v, OP_Reorganize, 0, 0, pIdx->zName, 0);
        }
      }
    }
  }

vacuum_cleanup:
  sqliteFree(zName);
  return;
}

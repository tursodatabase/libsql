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
** when syntax rules are reduced.
**
** $Id: build.c,v 1.1 2000/05/29 14:26:01 drh Exp $
*/
#include "sqliteInt.h"

/*
** This routine is called after a single SQL statement has been
** parsed and we want to execute the code to implement 
** the statement.  Prior action routines should have already
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
                     &pParse->zErrMsg);
    }
    sqliteVdbeDelete(pParse->pVdbe);
    pParse->pVdbe = 0;
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
  return pNew;
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
** format of a particular index table given the name
** of that table.  Return NULL if not found.
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
** not unlinked from the table that is being indexed.  Unlinking
** from the table must be done by the calling function.
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
** table.  No changes are made to disk by this routine.
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
    if( pTable->azCol[i] ) sqliteFree(pTable->azCol[i]);
  }
  for(pIndex = pTable->pIndex; pIndex; pIndex=pNext){
    pNext = pIndex->pNext;
    sqliteDeleteIndex(db, pIndex);
  }
  sqliteFree(pTable->azCol);
  sqliteFree(pTable);
}

/*
** Construct the name of a user table from a token.
**
** Space to hold the name is obtained from sqliteMalloc() and must
** be freed by the calling function.
*/
static char *sqliteTableNameFromToken(Token *pName){
  char *zName = 0;
  sqliteSetNString(&zName, pName->z, pName->n, 0);
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
    sqliteSetNString(&pParse->zErrMsg, "table \"", 0, pName->z, pName->n,
        "\" already exists", 0, 0);
    sqliteFree(zName);
    pParse->nErr++;
    return;
  }
  if( sqliteFindIndex(pParse->db, zName) ){
    sqliteSetString(&pParse->zErrMsg, "there is already an index named \"", 
       zName, "\"", 0);
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
  pTable->azCol = 0;
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
    p->azCol = sqliteRealloc( p->azCol, p->nCol+8);
  }
  if( p->azCol==0 ){
    p->nCol = 0;
    return;
  }
  pz = &p->azCol[p->nCol++];
  *pz = 0;
  sqliteSetNString(pz, pName->z, pName->n, 0);
}

/*
** This routine is called to report the final ")" that terminates
** a CREATE TABLE statement.
**
** The table structure is added to the internal hash tables.  
**
** An entry for the table is made in the master table, unless 
** initFlag==1.  When initFlag==1, it means we are reading the
** master table because we just connected to the database, so 
** the entry for this table already exists in the master table.
** We do not want to create it again.
*/
void sqliteEndTable(Parse *pParse, Token *pEnd){
  Table *p;
  int h;

  if( pParse->nErr ) return;

  /* Add the table to the in-memory representation of the database
  */
  if( (p = pParse->pNewTable)!=0 && pParse->explain==0 ){
    h = sqliteHashNoCase(p->zName, 0) % N_HASH;
    p->pHash = pParse->db->apTblHash[h];
    pParse->db->apTblHash[h] = p;
    pParse->pNewTable = 0;
  }

  /* If not initializing, then create the table on disk.
  */
  if( !pParse->initFlag ){
    static VdbeOp addTable[] = {
      { OP_Open,        0, 0, MASTER_NAME },
      { OP_New,         0, 0, 0},
      { OP_String,      0, 0, "table"     },
      { OP_String,      0, 0, 0},            /* 2 */
      { OP_String,      0, 0, 0},            /* 3 */
      { OP_String,      0, 0, 0},            /* 4 */
      { OP_MakeRecord,  4, 0, 0},
      { OP_Put,         0, 0, 0},
      { OP_Close,       0, 0, 0},
    };
    int n, base;
    Vdbe *v = pParse->pVdbe;

    if( v==0 ){
      v = pParse->pVdbe = sqliteVdbeCreate(pParse->db->pBe);
    }
    if( v==0 ) return;
    n = (int)pEnd->z - (int)pParse->sFirstToken.z + 1;
    base = sqliteVdbeAddOpList(v, ArraySize(addTable), addTable);
    sqliteVdbeChangeP3(v, base+2, p->zName, 0);
    sqliteVdbeChangeP3(v, base+3, p->zName, 0);
    sqliteVdbeChangeP3(v, base+4, pParse->sFirstToken.z, n);
  }
}

/*
** Given a token, look up a table with that name.  If not found, leave
** an error for the parser to find and return NULL.
*/
static Table *sqliteTableFromToken(Parse *pParse, Token *pTok){
  char *zName = sqliteTableNameFromToken(pTok);
  Table *pTab = sqliteFindTable(pParse->db, zName);
  sqliteFree(zName);
  if( pTab==0 ){
    sqliteSetNString(&pParse->zErrMsg, "no such table: \"", 0, 
        pTok->z, pTok->n, "\"", 1, 0);
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
    sqliteSetString(&pParse->zErrMsg, "table \"", pTable->zName, 
       "\" may not be dropped", 0);
    pParse->nErr++;
    return;
  }

  /* Generate code to remove the table and its reference in sys_master */
  v = pParse->pVdbe;
  if( v==0 ){
    v = pParse->pVdbe = sqliteVdbeCreate(pParse->db->pBe);
  }
  if( v ){
    static VdbeOp dropTable[] = {
      { OP_Open,       0, 0,        MASTER_NAME },
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

  /* Remove the table structure and free its memory.
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
    sqliteDeleteTable(pParse->db, pTable);
  }
}

/*
** Create a new index for an SQL table.  pIndex is the name of the index 
** and pTable is the name of the table that is to be indexed.  Both will 
** be NULL for a primary key.  In that case, use pParse->pNewTable as the 
** table to be indexed.
**
** pList is a list of fields to be indexed.  pList will be NULL if the
** most recently added field of the table is labeled as the primary key.
*/
void sqliteCreateIndex(
  Parse *pParse,   /* All information about this parse */
  Token *pName,    /* Name of the index.  May be NULL */
  Token *pTable,   /* Name of the table to index.  Use pParse->pNewTable if 0 */
  IdList *pList,   /* A list of fields to be indexed */
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
    sqliteSetString(&pParse->zErrMsg, "table \"", pTab->zName, 
      "\" may not have new indices added", 0);
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
    sqliteSetString(&pParse->zErrMsg, "index \"", zName, 
       "\" already exists", 0);
    pParse->nErr++;
    goto exit_create_index;
  }
  if( sqliteFindTable(pParse->db, zName) ){
    sqliteSetString(&pParse->zErrMsg, "there is already a table named \"",
       zName, "\"", 0);
    pParse->nErr++;
    goto exit_create_index;
  }

  /* If pList==0, it means this routine was called to make a primary
  ** key out of the last field added to the table under construction.
  ** So create a fake list to simulate this.
  */
  if( pList==0 ){
    nullId.z = pTab->azCol[pTab->nCol-1];
    nullId.n = strlen(nullId.z);
    pList = sqliteIdListAppend(0, &nullId);
    if( pList==0 ) goto exit_create_index;
  }

  /* 
  ** Allocate the index structure. 
  */
  pIndex = sqliteMalloc( sizeof(Index) + strlen(zName) + 
                        sizeof(int)*pList->nId );
  if( pIndex==0 ){
    sqliteSetString(&pParse->zErrMsg, "out of memory", 0);
    pParse->nErr++;
    goto exit_create_index;
  }
  pIndex->aiField = (int*)&pIndex[1];
  pIndex->zName = (char*)&pIndex->aiField[pList->nId];
  strcpy(pIndex->zName, zName);
  pIndex->pTable = pTab;
  pIndex->nField = pList->nId;

  /* Scan the names of the fields of the table to be indexed and
  ** load the field indices into the Index structure.  Report an error
  ** if any field is not found.
  */
  for(i=0; i<pList->nId; i++){
    for(j=0; j<pTab->nCol; j++){
      if( sqliteStrICmp(pList->a[i].zName, pTab->azCol[j])==0 ) break;
    }
    if( j>=pTab->nCol ){
      sqliteSetString(&pParse->zErrMsg, "table being indexed has no field "
        "named \"", pList->a[i].zName, "\"", 0);
      pParse->nErr++;
      sqliteFree(pIndex);
      goto exit_create_index;
    }
    pIndex->aiField[i] = j;
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
      { OP_Open,        0, 0, MASTER_NAME},
      { OP_New,         0, 0, 0},
      { OP_String,      0, 0, "index"},
      { OP_String,      0, 0, 0},  /* 2 */
      { OP_String,      0, 0, 0},  /* 3 */
      { OP_String,      0, 0, 0},  /* 4 */
      { OP_MakeRecord,  4, 0, 0},
      { OP_Put,         0, 0, 0},
      { OP_Close,       0, 0, 0},
    };
    int n;
    Vdbe *v = pParse->pVdbe;
    int lbl1, lbl2;
    int i;

    if( v==0 ){
      v = pParse->pVdbe = sqliteVdbeCreate(pParse->db->pBe);
    }
    if( v==0 ) goto exit_create_index;
    if( pStart && pEnd ){
      int base;
      n = (int)pEnd->z - (int)pStart->z + 1;
      base = sqliteVdbeAddOpList(v, ArraySize(addTable), addTable);
      sqliteVdbeChangeP3(v, base+2, pIndex->zName, 0);
      sqliteVdbeChangeP3(v, base+3, pTab->zName, 0);
      sqliteVdbeChangeP3(v, base+4, pStart->z, n);
    }
    sqliteVdbeAddOp(v, OP_Open, 0, 0, pTab->zName, 0);
    sqliteVdbeAddOp(v, OP_Open, 1, 0, pIndex->zName, 0);
    lbl1 = sqliteVdbeMakeLabel(v);
    lbl2 = sqliteVdbeMakeLabel(v);
    sqliteVdbeAddOp(v, OP_Next, 0, lbl2, 0, lbl1);
    sqliteVdbeAddOp(v, OP_Key, 0, 0, 0, 0);
    for(i=0; i<pIndex->nField; i++){
      sqliteVdbeAddOp(v, OP_Field, 0, pIndex->aiField[i], 0, 0);
    }
    sqliteVdbeAddOp(v, OP_MakeKey, pIndex->nField, 0, 0, 0);
    sqliteVdbeAddOp(v, OP_PutIdx, 1, 0, 0, 0);
    sqliteVdbeAddOp(v, OP_Goto, 0, lbl1, 0, 0);
    sqliteVdbeAddOp(v, OP_Noop, 0, 0, 0, lbl2);
    sqliteVdbeAddOp(v, OP_Close, 0, 0, 0, 0);
    sqliteVdbeAddOp(v, OP_Close, 1, 0, 0, 0);
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
    sqliteSetNString(&pParse->zErrMsg, "no such index: \"", 0, 
        pName->z, pName->n, "\"", 1, 0);
    pParse->nErr++;
    return;
  }

  /* Generate code to remove the index and from the master table */
  v = pParse->pVdbe = sqliteVdbeCreate(pParse->db->pBe);
  if( v ){
    static VdbeOp dropIndex[] = {
      { OP_Open,       0, 0,       MASTER_NAME},
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
** This routine is call to handle SQL of the following form:
**
**    insert into TABLE (IDLIST) values(EXPRLIST)
**
** The parameters are the table name and the expression list.
*/
void sqliteInsert(
  Parse *pParse,        /* Parser context */
  Token *pTableName,    /* Name of table into which we are inserting */
  ExprList *pList,      /* List of values to be inserted */
  IdList *pField        /* Field name corresponding to pList.  Might be NULL */
){
  Table *pTab;
  char *zTab;
  int i, j;
  Vdbe *v;

  zTab = sqliteTableNameFromToken(pTableName);
  pTab = sqliteFindTable(pParse->db, zTab);
  sqliteFree(zTab);
  if( pTab==0 ){
    sqliteSetNString(&pParse->zErrMsg, "no such table: \"", 0, 
        pTableName->z, pTableName->n, "\"", 1, 0);
    pParse->nErr++;
    goto insert_cleanup;
  }
  if( pTab->readOnly ){
    sqliteSetString(&pParse->zErrMsg, "table \"", pTab->zName,
        "\" may not be modified", 0);
    pParse->nErr++;
    goto insert_cleanup;
  }
  if( pField==0 && pList->nExpr!=pTab->nCol ){
    char zNum1[30];
    char zNum2[30];
    sprintf(zNum1,"%d", pList->nExpr);
    sprintf(zNum2,"%d", pTab->nCol);
    sqliteSetString(&pParse->zErrMsg, "table ", pTab->zName,
       " has ", zNum2, " columns but only ",
       zNum1, " values were supplied", 0);
    pParse->nErr++;
    goto insert_cleanup;
  }
  if( pField!=0 && pList->nExpr!=pField->nId ){
    char zNum1[30];
    char zNum2[30];
    sprintf(zNum1,"%d", pList->nExpr);
    sprintf(zNum2,"%d", pTab->nCol);
    sqliteSetString(&pParse->zErrMsg, zNum1, " values for ",
       zNum2, " columns", 0);
    pParse->nErr++;
    goto insert_cleanup;
  }
  if( pField ){
    for(i=0; i<pField->nId; i++){
      pField->a[i].idx = -1;
    }
    for(i=0; i<pField->nId; i++){
      for(j=0; j<pTab->nCol; j++){
        if( sqliteStrICmp(pField->a[i].zName, pTab->azCol[j])==0 ){
          pField->a[i].idx = j;
          break;
        }
      }
      if( j>=pTab->nCol ){
        sqliteSetString(&pParse->zErrMsg, "table ", pTab->zName,
           " has no column named ", pField->a[i].zName, 0);
        pParse->nErr++;
        goto insert_cleanup;
      }
    }
  }
  v = pParse->pVdbe = sqliteVdbeCreate(pParse->db->pBe);
  if( v ){
    Index *pIdx;
    sqliteVdbeAddOp(v, OP_Open, 0, 0, pTab->zName, 0);
    sqliteVdbeAddOp(v, OP_New, 0, 0, 0, 0);
    if( pTab->pIndex ){
      sqliteVdbeAddOp(v, OP_Dup, 0, 0, 0, 0);
    }
    for(i=0; i<pTab->nCol; i++){
      if( pField==0 ){
        j = i;
      }else{
        for(j=0; j<pField->nId; j++){
          if( pField->a[j].idx==i ) break;
        }
      }
      if( pField && j>=pField->nId ){
        sqliteVdbeAddOp(v, OP_String, 0, 0, "", 0);
      }else{
        sqliteExprCode(pParse, pList->a[j].pExpr);
      }
    }
    sqliteVdbeAddOp(v, OP_MakeRecord, pTab->nCol, 0, 0, 0);
    sqliteVdbeAddOp(v, OP_Put, 0, 0, 0, 0);
    sqliteVdbeAddOp(v, OP_Close, 0, 0, 0, 0);
    for(pIdx=pTab->pIndex; pIdx; pIdx=pIdx->pNext){
      if( pIdx->pNext ){
        sqliteVdbeAddOp(v, OP_Dup, 0, 0, 0, 0);
      }
      sqliteVdbeAddOp(v, OP_Open, 0, 0, pIdx->zName, 0);
      for(i=0; i<pIdx->nField; i++){
        int idx = pIdx->aiField[i];
        if( pField==0 ){
          j = idx;
        }else{
          for(j=0; j<pField->nId; j++){
            if( pField->a[j].idx==idx ) break;
          }
        }
        if( pField && j>=pField->nId ){
          sqliteVdbeAddOp(v, OP_String, 0, 0, "", 0);
        }else{
          sqliteExprCode(pParse, pList->a[j].pExpr);
        }
      }
      sqliteVdbeAddOp(v, OP_MakeKey, pIdx->nField, 0, 0, 0);
      sqliteVdbeAddOp(v, OP_PutIdx, 0, 0, 0, 0);
      sqliteVdbeAddOp(v, OP_Close, 0, 0, 0, 0);
    }
  }

insert_cleanup:
  sqliteExprListDelete(pList);
  sqliteIdListDelete(pField);
}

/*
** This routine walks an expression tree and resolves references to
** table fields.  Nodes of the form ID.ID or ID resolve into an
** index to the table in the table list and a field offset.  The opcode
** for such nodes is changed to TK_FIELD.  The iTable value is changed
** to the index of the referenced table in pTabList, and the iField value
** is changed to the index of the field of the referenced table.
**
** Unknown fields or tables provoke an error.  The function returns
** the number of errors seen and leaves an error message on pParse->zErrMsg.
*/
int sqliteExprResolveIds(Parse *pParse, IdList *pTabList, Expr *pExpr){
  if( pExpr==0 ) return 0;
  switch( pExpr->op ){
    /* A lone identifier */
    case TK_ID: {
      int cnt = 0;   /* Number of matches */
      int i;         /* Loop counter */
      char *z = pExpr->token.z;
      int n = pExpr->token.n;
      for(i=0; i<pTabList->nId; i++){
        int j;
        Table *pTab = pTabList->a[i].pTab;
        if( pTab==0 ) continue;
        for(j=0; j<pTab->nCol; j++){
          if( sqliteStrNICmp(pTab->azCol[j], z, n)==0 ){
            cnt++;
            pExpr->iTable = i;
            pExpr->iField = j;
          }
        }
      }
      if( cnt==0 ){
        sqliteSetNString(&pParse->zErrMsg, "unknown field name: \"", -1,  
          pExpr->token.z, pExpr->token.n, "\"", -1, 0);
        pParse->nErr++;
        return 1;
      }else if( cnt>1 ){
        sqliteSetNString(&pParse->zErrMsg, "ambiguous field name: \"", -1,  
          pExpr->token.z, pExpr->token.n, "\"", -1, 0);
        pParse->nErr++;
        return 1;
      }
      pExpr->op = TK_FIELD;
      break; 
    }
  
    /* A table name and field name:  ID.ID */
    case TK_DOT: {
      int cnt = 0;   /* Number of matches */
      int i;         /* Loop counter */
      Expr *pLeft, *pRight;    /* Left and right subbranches of the expr */
      int n;                   /* Length of an identifier */
      char *z;                 /* Text of an identifier */

      pLeft = pExpr->pLeft;
      pRight = pExpr->pRight;
      assert( pLeft && pLeft->op==TK_ID );
      assert( pRight && pRight->op==TK_ID );
      n = pRight->token.n;
      z = pRight->token.z;      
      for(i=0; i<pTabList->nId; i++){
        int j;
        char *zTab;
        Table *pTab = pTabList->a[i].pTab;
        if( pTab==0 ) continue;
        if( pTabList->a[i].zAlias ){
          zTab = pTabList->a[i].zAlias;
        }else{
          zTab = pTab->zName;
        }
        if( sqliteStrNICmp(zTab, pLeft->token.z, pLeft->token.n)!=0 ) continue;
        for(j=0; j<pTab->nCol; j++){
          if( sqliteStrNICmp(pTab->azCol[j], z, n)==0 ){
            cnt++;
            pExpr->iTable = i;
            pExpr->iField = j;
          }
        }
      }
      if( cnt==0 ){
        sqliteSetNString(&pParse->zErrMsg, "unknown field name: \"", -1,  
          pLeft->token.z, pLeft->token.n, ".", 1, z, n, "\"", 1, 0);
        pParse->nErr++;
        return 1;
      }else if( cnt>1 ){
        sqliteSetNString(&pParse->zErrMsg, "ambiguous field name: \"", -1,  
          pExpr->token.z, pExpr->token.n, ".", 1, z, n, "\"", 1, 0);
        pParse->nErr++;
        return 1;
      }
      sqliteExprDelete(pLeft);
      pExpr->pLeft = 0;
      sqliteExprDelete(pRight);
      pExpr->pRight = 0;
      pExpr->op = TK_FIELD;
      break;
    }

    /* For all else, just recursively walk the tree */
    default: {
      if( pExpr->pLeft 
            && sqliteExprResolveIds(pParse, pTabList, pExpr->pLeft) ){
        return 1;
      }
      if( pExpr->pRight 
            && sqliteExprResolveIds(pParse, pTabList, pExpr->pRight) ){
        return 1;
      }
      if( pExpr->pList ){
        int i;
        ExprList *pList = pExpr->pList;
        for(i=0; i<pList->nExpr; i++){
          if( sqliteExprResolveIds(pParse, pTabList, pList->a[i].pExpr) ){
            return 1;
          }
        }
      }
    }
  }
  return 0;
}

/*
** Process a SELECT statement.
*/
void sqliteSelect(
  Parse *pParse,         /* The parser context */
  ExprList *pEList,      /* List of fields to extract.  NULL means "*" */
  IdList *pTabList,      /* List of tables to select from */
  Expr *pWhere,          /* The WHERE clause.  May be NULL */
  ExprList *pOrderBy     /* The ORDER BY clause.  May be NULL */
){
  int i, j;
  WhereInfo *pWInfo;
  Vdbe *v;

  if( pParse->nErr>0 ) goto select_cleanup;

  /* Look up every table in the table list.
  */
  for(i=0; i<pTabList->nId; i++){
    pTabList->a[i].pTab = sqliteFindTable(pParse->db, pTabList->a[i].zName);
    if( pTabList->a[i].pTab==0 ){
      sqliteSetString(&pParse->zErrMsg, "unknown table \"", 
         pTabList->a[i].zName, "\"", 0);
      pParse->nErr++;
      goto select_cleanup;
    }
  }

  /* If the list of fields to retrieve is "*" then replace it with
  ** a list of all fields from all tables.
  */
  if( pEList==0 ){
    for(i=0; i<pTabList->nId; i++){
      Table *pTab = pTabList->a[i].pTab;
      for(j=0; j<pTab->nCol; j++){
        Expr *pExpr = sqliteExpr(TK_FIELD, 0, 0, 0);
        pExpr->iTable = i;
        pExpr->iField = j;
        pEList = sqliteExprListAppend(pEList, pExpr, 0);
      }
    }
  }

  /* Resolve the field names in all the expressions.
  */
  for(i=0; i<pEList->nExpr; i++){
    if( sqliteExprResolveIds(pParse, pTabList, pEList->a[i].pExpr) ){
      goto select_cleanup;
    }
  }
  if( pWhere && sqliteExprResolveIds(pParse, pTabList, pWhere) ){
    goto select_cleanup;
  }
  if( pOrderBy ){
    for(i=0; i<pOrderBy->nExpr; i++){
      if( sqliteExprResolveIds(pParse, pTabList, pOrderBy->a[i].pExpr) ){
        goto select_cleanup;
      }
    }
  }

  /* Begin generating code.
  */
  v = pParse->pVdbe;
  if( v==0 ){
    v = pParse->pVdbe = sqliteVdbeCreate(pParse->db->pBe);
  }
  if( v==0 ) goto select_cleanup;
  if( pOrderBy ){
    sqliteVdbeAddOp(v, OP_SortOpen, 0, 0, 0, 0);
  }


  /* Identify column names
  */
  sqliteVdbeAddOp(v, OP_ColumnCount, pEList->nExpr, 0, 0, 0);
  for(i=0; i<pEList->nExpr; i++){
    Expr *p;
    if( pEList->a[i].zName ){
      char *zName = pEList->a[i].zName;
      int addr = sqliteVdbeAddOp(v, OP_ColumnName, i, 0, zName, 0);
      if( zName[0]=='\'' || zName[0]=='"' ){
        sqliteVdbeDequoteP3(v, addr);
      }
      continue;
    }
    p = pEList->a[i].pExpr;
    if( p->op!=TK_FIELD ){
      char zName[30];
      sprintf(zName, "field%d", i+1);
      sqliteVdbeAddOp(v, OP_ColumnName, i, 0, zName, 0);
    }else{
      if( pTabList->nId>1 ){
        char *zName = 0;
        Table *pTab = pTabList->a[p->iTable].pTab;
        sqliteSetString(&zName, pTab->zName, ".", 
               pTab->azCol[p->iField], 0);
        sqliteVdbeAddOp(v, OP_ColumnName, i, 0, zName, 0);
        sqliteFree(zName);
      }else{
        Table *pTab = pTabList->a[0].pTab;
        sqliteVdbeAddOp(v, OP_ColumnName, i, 0, pTab->azCol[p->iField], 0);
      }
    }
  }

  /* Begin the database scan
  */  
  pWInfo = sqliteWhereBegin(pParse, pTabList, pWhere, 0);
  if( pWInfo==0 ) goto select_cleanup;

  /* Pull the requested fields.
  */
  for(i=0; i<pEList->nExpr; i++){
    sqliteExprCode(pParse, pEList->a[i].pExpr);
  }
  
  /* If there is no ORDER BY clause, then we can invoke the callback
  ** right away.  If there is an ORDER BY, then we need to put the
  ** data into an appropriate sorter record.
  */
  if( pOrderBy==0 ){
    sqliteVdbeAddOp(v, OP_Callback, pEList->nExpr, 0, 0, 0);
  }else{
    char *zSortOrder;
    sqliteVdbeAddOp(v, OP_SortMakeRec, pEList->nExpr, 0, 0, 0);
    zSortOrder = sqliteMalloc( pOrderBy->nExpr + 1 );
    if( zSortOrder==0 ) goto select_cleanup;
    for(i=0; i<pOrderBy->nExpr; i++){
      zSortOrder[i] = pOrderBy->a[i].idx ? '-' : '+';
      sqliteExprCode(pParse, pOrderBy->a[i].pExpr);
    }
    zSortOrder[pOrderBy->nExpr] = 0;
    sqliteVdbeAddOp(v, OP_SortMakeKey, pOrderBy->nExpr, 0, zSortOrder, 0);
    sqliteVdbeAddOp(v, OP_SortPut, 0, 0, 0, 0);
  }

  /* End the database scan loop.
  */
  sqliteWhereEnd(pWInfo);

  /* If there is an ORDER BY clause, then we need to sort the results
  ** and send them to the callback one by one.
  */
  if( pOrderBy ){
    int end = sqliteVdbeMakeLabel(v);
    int addr;
    sqliteVdbeAddOp(v, OP_Sort, 0, 0, 0, 0);
    addr = sqliteVdbeAddOp(v, OP_SortNext, 0, end, 0, 0);
    sqliteVdbeAddOp(v, OP_SortCallback, pEList->nExpr, 0, 0, 0);
    sqliteVdbeAddOp(v, OP_Goto, 0, addr, 0, 0);
    sqliteVdbeAddOp(v, OP_Noop, 0, 0, 0, end);
  }

  /* Always execute the following code before exiting, in order to
  ** release resources.
  */
select_cleanup:
  sqliteExprListDelete(pEList);
  sqliteIdListDelete(pTabList);
  sqliteExprDelete(pWhere);
  sqliteExprListDelete(pOrderBy);
  return;
}

/*
** Process a DELETE FROM statement.
*/
void sqliteDeleteFrom(
  Parse *pParse,         /* The parser context */
  Token *pTableName,     /* The table from which we should delete things */
  Expr *pWhere           /* The WHERE clause.  May be null */
){
  Vdbe *v;               /* The virtual database engine */
  Table *pTab;           /* The table from which records will be deleted */
  IdList *pTabList;      /* An ID list holding pTab and nothing else */
  int end, addr;         /* A couple addresses of generated code */
  int i;                 /* Loop counter */
  WhereInfo *pWInfo;     /* Information about the WHERE clause */
  Index *pIdx;           /* For looping over indices of the table */

  /* Locate the table which we want to update.  This table has to be
  ** put in an IdList structure because some of the subroutines will
  ** will be calling are designed to work with multiple tables and expect
  ** an IdList* parameter instead of just a Table* parameger.
  */
  pTabList = sqliteIdListAppend(0, pTableName);
  for(i=0; i<pTabList->nId; i++){
    pTabList->a[i].pTab = sqliteFindTable(pParse->db, pTabList->a[i].zName);
    if( pTabList->a[i].pTab==0 ){
      sqliteSetString(&pParse->zErrMsg, "unknown table \"", 
         pTabList->a[i].zName, "\"", 0);
      pParse->nErr++;
      goto delete_from_cleanup;
    }
    if( pTabList->a[i].pTab->readOnly ){
      sqliteSetString(&pParse->zErrMsg, "table \"", pTabList->a[i].zName,
        "\" may not be modified", 0);
      pParse->nErr++;
      goto delete_from_cleanup;
    }
  }
  pTab = pTabList->a[0].pTab;

  /* Resolve the field names in all the expressions.
  */
  if( pWhere && sqliteExprResolveIds(pParse, pTabList, pWhere) ){
    goto delete_from_cleanup;
  }

  /* Begin generating code.
  */
  v = pParse->pVdbe;
  if( v==0 ){
    v = pParse->pVdbe = sqliteVdbeCreate(pParse->db->pBe);
  }
  if( v==0 ) goto delete_from_cleanup;

  /* Begin the database scan
  */
  sqliteVdbeAddOp(v, OP_ListOpen, 0, 0, 0, 0);
  pWInfo = sqliteWhereBegin(pParse, pTabList, pWhere, 1);
  if( pWInfo==0 ) goto delete_from_cleanup;

  /* Remember the index of every item to be deleted.
  */
  sqliteVdbeAddOp(v, OP_ListWrite, 0, 0, 0, 0);

  /* End the database scan loop.
  */
  sqliteWhereEnd(pWInfo);

  /* Delete every item identified in the list.
  */
  sqliteVdbeAddOp(v, OP_ListRewind, 0, 0, 0, 0);
  for(i=1, pIdx=pTab->pIndex; pIdx; i++, pIdx=pIdx->pNext){
    sqliteVdbeAddOp(v, OP_Open, i, 0, pIdx->zName, 0);
  }
  end = sqliteVdbeMakeLabel(v);
  addr = sqliteVdbeAddOp(v, OP_ListRead, 0, end, 0, 0);
  if( pTab->pIndex ){
    sqliteVdbeAddOp(v, OP_Dup, 0, 0, 0, 0);
    sqliteVdbeAddOp(v, OP_Fetch, 0, 0, 0, 0);
    for(i=1, pIdx=pTab->pIndex; pIdx; i++, pIdx=pIdx->pNext){
      int j;
      sqliteVdbeAddOp(v, OP_Dup, 0, 0, 0, 0);
      for(j=0; j<pIdx->nField; j++){
        sqliteVdbeAddOp(v, OP_Field, 0, pIdx->aiField[j], 0, 0);
      }
      sqliteVdbeAddOp(v, OP_MakeKey, pIdx->nField, 0, 0, 0);
      sqliteVdbeAddOp(v, OP_DeleteIdx, i, 0, 0, 0);
    }
  }
  sqliteVdbeAddOp(v, OP_Delete, 0, 0, 0, 0);
  sqliteVdbeAddOp(v, OP_Goto, 0, addr, 0, 0);
  sqliteVdbeAddOp(v, OP_ListClose, 0, 0, 0, end);

delete_from_cleanup:
  sqliteIdListDelete(pTabList);
  sqliteExprDelete(pWhere);
  return;
}

/*
** Process an UPDATE statement.
*/
void sqliteUpdate(
  Parse *pParse,         /* The parser context */
  Token *pTableName,     /* The table in which we should change things */
  ExprList *pChanges,    /* Things to be changed */
  Expr *pWhere           /* The WHERE clause.  May be null */
){
  int i, j;              /* Loop counters */
  Table *pTab;           /* The table to be updated */
  IdList *pTabList = 0;  /* List containing only pTab */
  int end, addr;         /* A couple of addresses in the generated code */
  WhereInfo *pWInfo;     /* Information about the WHERE clause */
  Vdbe *v;               /* The virtual database engine */
  Index *pIdx;           /* For looping over indices */
  int nIdx;              /* Number of indices that need updating */
  Index **apIdx = 0;     /* An array of indices that need updating too */
  int *aXRef = 0;        /* aXRef[i] is the index in pChanges->a[] of the
                         ** an expression for the i-th field of the table.
                         ** aXRef[i]==-1 if the i-th field is not changed. */

  /* Locate the table which we want to update.  This table has to be
  ** put in an IdList structure because some of the subroutines will
  ** will be calling are designed to work with multiple tables and expect
  ** an IdList* parameter instead of just a Table* parameger.
  */
  pTabList = sqliteIdListAppend(0, pTableName);
  for(i=0; i<pTabList->nId; i++){
    pTabList->a[i].pTab = sqliteFindTable(pParse->db, pTabList->a[i].zName);
    if( pTabList->a[i].pTab==0 ){
      sqliteSetString(&pParse->zErrMsg, "unknown table \"", 
         pTabList->a[i].zName, "\"", 0);
      pParse->nErr++;
      goto update_cleanup;
    }
    if( pTabList->a[i].pTab->readOnly ){
      sqliteSetString(&pParse->zErrMsg, "table \"", pTabList->a[i].zName,
        "\" may not be modified", 0);
      pParse->nErr++;
      goto update_cleanup;
    }
  }
  pTab = pTabList->a[0].pTab;
  aXRef = sqliteMalloc( sizeof(int) * pTab->nCol );
  if( aXRef==0 ) goto update_cleanup;
  for(i=0; i<pTab->nCol; i++) aXRef[i] = -1;

  /* Resolve the field names in all the expressions in both the
  ** WHERE clause and in the new values.  Also find the field index
  ** for each field to be updated in the pChanges array.
  */
  if( pWhere && sqliteExprResolveIds(pParse, pTabList, pWhere) ){
    goto update_cleanup;
  }
  for(i=0; i<pChanges->nExpr; i++){
    if( sqliteExprResolveIds(pParse, pTabList, pChanges->a[i].pExpr) ){
      goto update_cleanup;
    }
    for(j=0; j<pTab->nCol; j++){
      if( strcmp(pTab->azCol[j], pChanges->a[i].zName)==0 ){
        pChanges->a[i].idx = j;
        aXRef[j] = i;
        break;
      }
    }
    if( j>=pTab->nCol ){
      sqliteSetString(&pParse->zErrMsg, "no such field: \"", 
         pChanges->a[i].zName, "\"", 0);
      pParse->nErr++;
      goto update_cleanup;
    }
  }

  /* Allocate memory for the array apIdx[] and fill it pointers to every
  ** index that needs to be updated.  Indices only need updating if their
  ** key includes one of the fields named in pChanges.
  */
  for(nIdx=0, pIdx=pTab->pIndex; pIdx; pIdx=pIdx->pNext){
    for(i=0; i<pIdx->nField; i++){
      if( aXRef[pIdx->aiField[i]]>=0 ) break;
    }
    if( i<pIdx->nField ) nIdx++;
  }
  apIdx = sqliteMalloc( sizeof(Index*) * nIdx );
  if( apIdx==0 ) goto update_cleanup;
  for(nIdx=0, pIdx=pTab->pIndex; pIdx; pIdx=pIdx->pNext){
    for(i=0; i<pIdx->nField; i++){
      if( aXRef[pIdx->aiField[i]]>=0 ) break;
    }
    if( i<pIdx->nField ) apIdx[nIdx++] = pIdx;
  }

  /* Begin generating code.
  */
  v = pParse->pVdbe;
  if( v==0 ){
    v = pParse->pVdbe = sqliteVdbeCreate(pParse->db->pBe);
  }
  if( v==0 ) goto update_cleanup;

  /* Begin the database scan
  */
  sqliteVdbeAddOp(v, OP_ListOpen, 0, 0, 0, 0);
  pWInfo = sqliteWhereBegin(pParse, pTabList, pWhere, 1);
  if( pWInfo==0 ) goto update_cleanup;

  /* Remember the index of every item to be updated.
  */
  sqliteVdbeAddOp(v, OP_ListWrite, 0, 0, 0, 0);

  /* End the database scan loop.
  */
  sqliteWhereEnd(pWInfo);

  /* Rewind the list of records that need to be updated and
  ** open every index that needs updating.
  */
  sqliteVdbeAddOp(v, OP_ListRewind, 0, 0, 0, 0);
  for(i=0; i<nIdx; i++){
    sqliteVdbeAddOp(v, OP_Open, i+1, 0, apIdx[i]->zName, 0);
  }

  /* Loop over every record that needs updating.  We have to load
  ** the old data for each record to be updated because some fields
  ** might not change and we will need to copy the old value, therefore.
  ** Also, the old data is needed to delete the old index entires.
  */
  end = sqliteVdbeMakeLabel(v);
  addr = sqliteVdbeAddOp(v, OP_ListRead, 0, end, 0, 0);
  sqliteVdbeAddOp(v, OP_Dup, 0, 0, 0, 0);
  sqliteVdbeAddOp(v, OP_Fetch, 0, 0, 0, 0);

  /* Delete the old indices for the current record.
  */
  for(i=0; i<nIdx; i++){
    sqliteVdbeAddOp(v, OP_Dup, 0, 0, 0, 0);
    pIdx = apIdx[i];
    for(j=0; j<pIdx->nField; j++){
      sqliteVdbeAddOp(v, OP_Field, 0, pIdx->aiField[j], 0, 0);
    }
    sqliteVdbeAddOp(v, OP_MakeKey, pIdx->nField, 0, 0, 0);
    sqliteVdbeAddOp(v, OP_DeleteIdx, i+1, 0, 0, 0);
  }

  /* Compute a completely new data for this record.  
  */
  for(i=0; i<pTab->nCol; i++){
    j = aXRef[i];
    if( j<0 ){
      sqliteVdbeAddOp(v, OP_Field, 0, i, 0, 0);
    }else{
      sqliteExprCode(pParse, pChanges->a[j].pExpr);
    }
  }

  /* Insert new index entries that correspond to the new data
  */
  for(i=0; i<nIdx; i++){
    sqliteVdbeAddOp(v, OP_Dup, pTab->nCol, 0, 0, 0); /* The KEY */
    pIdx = apIdx[i];
    for(j=0; j<pIdx->nField; j++){
      sqliteVdbeAddOp(v, OP_Dup, j+pTab->nCol-pIdx->aiField[j], 0, 0, 0);
    }
    sqliteVdbeAddOp(v, OP_MakeKey, pIdx->nField, 0, 0, 0);
    sqliteVdbeAddOp(v, OP_PutIdx, i+1, 0, 0, 0);
  }

  /* Write the new data back into the database.
  */
  sqliteVdbeAddOp(v, OP_MakeRecord, pTab->nCol, 0, 0, 0);
  sqliteVdbeAddOp(v, OP_Put, 0, 0, 0, 0);

  /* Repeat the above with the next record to be updated, until
  ** all record selected by the WHERE clause have been updated.
  */
  sqliteVdbeAddOp(v, OP_Goto, 0, addr, 0, 0);
  sqliteVdbeAddOp(v, OP_ListClose, 0, 0, 0, end);

update_cleanup:
  sqliteFree(apIdx);
  sqliteFree(aXRef);
  sqliteIdListDelete(pTabList);
  sqliteExprListDelete(pChanges);
  sqliteExprDelete(pWhere);
  return;
}

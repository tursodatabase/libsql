/*
** 2001 September 15
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
*************************************************************************
** This file contains C code routines that are called by the SQLite parser
** when syntax rules are reduced.  The routines in this file handle the
** following kinds of SQL syntax:
**
**     CREATE TABLE
**     DROP TABLE
**     CREATE INDEX
**     DROP INDEX
**     creating ID lists
**     BEGIN TRANSACTION
**     COMMIT
**     ROLLBACK
**     PRAGMA
**
** $Id: build.c,v 1.194 2004/05/28 11:37:27 danielk1977 Exp $
*/
#include "sqliteInt.h"
#include <ctype.h>

/*
** This routine is called when a new SQL statement is beginning to
** be parsed.  Check to see if the schema for the database needs
** to be read from the SQLITE_MASTER and SQLITE_TEMP_MASTER tables.
** If it does, then read it.
*/
void sqlite3BeginParse(Parse *pParse, int explainFlag){
  sqlite *db = pParse->db;
  int i;
  pParse->explain = explainFlag;
  if((db->flags & SQLITE_Initialized)==0 && db->init.busy==0 ){
    int rc = sqlite3Init(db, &pParse->zErrMsg);
    if( rc!=SQLITE_OK ){
      pParse->rc = rc;
      pParse->nErr++;
    }
  }
  for(i=0; i<db->nDb; i++){
    DbClearProperty(db, i, DB_Locked);
    if( !db->aDb[i].inTrans ){
      DbClearProperty(db, i, DB_Cookie);
    }
  }
  pParse->nVar = 0;
}

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
void sqlite3Exec(Parse *pParse){
  sqlite *db = pParse->db;
  Vdbe *v = pParse->pVdbe;

  if( v==0 && (v = sqlite3GetVdbe(pParse))!=0 ){
    sqlite3VdbeAddOp(v, OP_Halt, 0, 0);
  }
  if( sqlite3_malloc_failed ) return;
  if( v && pParse->nErr==0 ){
    FILE *trace = (db->flags & SQLITE_VdbeTrace)!=0 ? stdout : 0;
    sqlite3VdbeTrace(v, trace);
    sqlite3VdbeMakeReady(v, pParse->nVar, pParse->explain);
    pParse->rc = pParse->nErr ? SQLITE_ERROR : SQLITE_DONE;
    pParse->colNamesSet = 0;
  }else if( pParse->rc==SQLITE_OK ){
    pParse->rc = SQLITE_ERROR;
  }
  pParse->nTab = 0;
  pParse->nMem = 0;
  pParse->nSet = 0;
  pParse->nAgg = 0;
  pParse->nVar = 0;
}

/*
** Locate the in-memory structure that describes 
** a particular database table given the name
** of that table and (optionally) the name of the database
** containing the table.  Return NULL if not found.
**
** If zDatabase is 0, all databases are searched for the
** table and the first matching table is returned.  (No checking
** for duplicate table names is done.)  The search order is
** TEMP first, then MAIN, then any auxiliary databases added
** using the ATTACH command.
**
** See also sqlite3LocateTable().
*/
Table *sqlite3FindTable(sqlite *db, const char *zName, const char *zDatabase){
  Table *p = 0;
  int i;
  for(i=0; i<db->nDb; i++){
    int j = (i<2) ? i^1 : i;   /* Search TEMP before MAIN */
    if( zDatabase!=0 && sqlite3StrICmp(zDatabase, db->aDb[j].zName) ) continue;
    p = sqlite3HashFind(&db->aDb[j].tblHash, zName, strlen(zName)+1);
    if( p ) break;
  }
  return p;
}

/*
** Locate the in-memory structure that describes 
** a particular database table given the name
** of that table and (optionally) the name of the database
** containing the table.  Return NULL if not found.
** Also leave an error message in pParse->zErrMsg.
**
** The difference between this routine and sqlite3FindTable()
** is that this routine leaves an error message in pParse->zErrMsg
** where sqlite3FindTable() does not.
*/
Table *sqlite3LocateTable(Parse *pParse, const char *zName, const char *zDbase){
  Table *p;

  p = sqlite3FindTable(pParse->db, zName, zDbase);
  if( p==0 ){
    if( zDbase ){
      sqlite3ErrorMsg(pParse, "no such table: %s.%s", zDbase, zName);
    }else if( sqlite3FindTable(pParse->db, zName, 0)!=0 ){
      sqlite3ErrorMsg(pParse, "table \"%s\" is not in database \"%s\"",
         zName, zDbase);
    }else{
      sqlite3ErrorMsg(pParse, "no such table: %s", zName);
    }
  }
  return p;
}

/*
** Locate the in-memory structure that describes 
** a particular index given the name of that index
** and the name of the database that contains the index.
** Return NULL if not found.
**
** If zDatabase is 0, all databases are searched for the
** table and the first matching index is returned.  (No checking
** for duplicate index names is done.)  The search order is
** TEMP first, then MAIN, then any auxiliary databases added
** using the ATTACH command.
*/
Index *sqlite3FindIndex(sqlite *db, const char *zName, const char *zDb){
  Index *p = 0;
  int i;
  for(i=0; i<db->nDb; i++){
    int j = (i<2) ? i^1 : i;  /* Search TEMP before MAIN */
    if( zDb && sqlite3StrICmp(zDb, db->aDb[j].zName) ) continue;
    p = sqlite3HashFind(&db->aDb[j].idxHash, zName, strlen(zName)+1);
    if( p ) break;
  }
  return p;
}

/*
** Remove the given index from the index hash table, and free
** its memory structures.
**
** The index is removed from the database hash tables but
** it is not unlinked from the Table that it indexes.
** Unlinking from the Table must be done by the calling function.
*/
static void sqliteDeleteIndex(sqlite *db, Index *p){
  Index *pOld;

  assert( db!=0 && p->zName!=0 );
  pOld = sqlite3HashInsert(&db->aDb[p->iDb].idxHash, p->zName,
                          strlen(p->zName)+1, 0);
  if( pOld!=0 && pOld!=p ){
    sqlite3HashInsert(&db->aDb[p->iDb].idxHash, pOld->zName,
                     strlen(pOld->zName)+1, pOld);
  }
  if( p->zColAff ){
    sqliteFree(p->zColAff);
  }
  sqliteFree(p);
}

/*
** Unlink the given index from its table, then remove
** the index from the index hash table and free its memory
** structures.
*/
void sqlite3UnlinkAndDeleteIndex(sqlite *db, Index *pIndex){
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
** Erase all schema information from the in-memory hash tables of
** database connection.  This routine is called to reclaim memory
** before the connection closes.  It is also called during a rollback
** if there were schema changes during the transaction.
**
** If iDb<=0 then reset the internal schema tables for all database
** files.  If iDb>=2 then reset the internal schema for only the
** single file indicated.
*/
void sqlite3ResetInternalSchema(sqlite *db, int iDb){
  HashElem *pElem;
  Hash temp1;
  Hash temp2;
  int i, j;

  assert( iDb>=0 && iDb<db->nDb );
  db->flags &= ~SQLITE_Initialized;
  for(i=iDb; i<db->nDb; i++){
    Db *pDb = &db->aDb[i];
    temp1 = pDb->tblHash;
    temp2 = pDb->trigHash;
    sqlite3HashInit(&pDb->trigHash, SQLITE_HASH_STRING, 0);
    sqlite3HashClear(&pDb->aFKey);
    sqlite3HashClear(&pDb->idxHash);
    for(pElem=sqliteHashFirst(&temp2); pElem; pElem=sqliteHashNext(pElem)){
      Trigger *pTrigger = sqliteHashData(pElem);
      sqlite3DeleteTrigger(pTrigger);
    }
    sqlite3HashClear(&temp2);
    sqlite3HashInit(&pDb->tblHash, SQLITE_HASH_STRING, 0);
    for(pElem=sqliteHashFirst(&temp1); pElem; pElem=sqliteHashNext(pElem)){
      Table *pTab = sqliteHashData(pElem);
      sqlite3DeleteTable(db, pTab);
    }
    sqlite3HashClear(&temp1);
    DbClearProperty(db, i, DB_SchemaLoaded);
    if( iDb>0 ) return;
  }
  assert( iDb==0 );
  db->flags &= ~SQLITE_InternChanges;

  /* If one or more of the auxiliary database files has been closed,
  ** then remove then from the auxiliary database list.  We take the
  ** opportunity to do this here since we have just deleted all of the
  ** schema hash tables and therefore do not have to make any changes
  ** to any of those tables.
  */
  for(i=0; i<db->nDb; i++){
    struct Db *pDb = &db->aDb[i];
    if( pDb->pBt==0 ){
      if( pDb->pAux && pDb->xFreeAux ) pDb->xFreeAux(pDb->pAux);
      pDb->pAux = 0;
    }
  }
  for(i=j=2; i<db->nDb; i++){
    struct Db *pDb = &db->aDb[i];
    if( pDb->pBt==0 ){
      sqliteFree(pDb->zName);
      pDb->zName = 0;
      continue;
    }
    if( j<i ){
      db->aDb[j] = db->aDb[i];
    }
    j++;
  }
  memset(&db->aDb[j], 0, (db->nDb-j)*sizeof(db->aDb[j]));
  db->nDb = j;
  if( db->nDb<=2 && db->aDb!=db->aDbStatic ){
    memcpy(db->aDbStatic, db->aDb, 2*sizeof(db->aDb[0]));
    sqliteFree(db->aDb);
    db->aDb = db->aDbStatic;
  }
}

/*
** This routine is called whenever a rollback occurs.  If there were
** schema changes during the transaction, then we have to reset the
** internal hash tables and reload them from disk.
*/
void sqlite3RollbackInternalChanges(sqlite *db){
  if( db->flags & SQLITE_InternChanges ){
    sqlite3ResetInternalSchema(db, 0);
  }
}

/*
** This routine is called when a commit occurs.
*/
void sqlite3CommitInternalChanges(sqlite *db){
  db->aDb[0].schema_cookie = db->next_cookie;
  db->flags &= ~SQLITE_InternChanges;
}

/*
** Remove the memory data structures associated with the given
** Table.  No changes are made to disk by this routine.
**
** This routine just deletes the data structure.  It does not unlink
** the table data structure from the hash table.  Nor does it remove
** foreign keys from the sqlite.aFKey hash table.  But it does destroy
** memory structures of the indices and foreign keys associated with 
** the table.
**
** Indices associated with the table are unlinked from the "db"
** data structure if db!=NULL.  If db==NULL, indices attached to
** the table are deleted, but it is assumed they have already been
** unlinked.
*/
void sqlite3DeleteTable(sqlite *db, Table *pTable){
  int i;
  Index *pIndex, *pNext;
  FKey *pFKey, *pNextFKey;

  if( pTable==0 ) return;

  /* Delete all indices associated with this table
  */
  for(pIndex = pTable->pIndex; pIndex; pIndex=pNext){
    pNext = pIndex->pNext;
    assert( pIndex->iDb==pTable->iDb || (pTable->iDb==0 && pIndex->iDb==1) );
    sqliteDeleteIndex(db, pIndex);
  }

  /* Delete all foreign keys associated with this table.  The keys
  ** should have already been unlinked from the db->aFKey hash table 
  */
  for(pFKey=pTable->pFKey; pFKey; pFKey=pNextFKey){
    pNextFKey = pFKey->pNextFrom;
    assert( pTable->iDb<db->nDb );
    assert( sqlite3HashFind(&db->aDb[pTable->iDb].aFKey,
                           pFKey->zTo, strlen(pFKey->zTo)+1)!=pFKey );
    sqliteFree(pFKey);
  }

  /* Delete the Table structure itself.
  */
  for(i=0; i<pTable->nCol; i++){
    sqliteFree(pTable->aCol[i].zName);
    sqliteFree(pTable->aCol[i].zDflt);
    sqliteFree(pTable->aCol[i].zType);
  }
  sqliteFree(pTable->zName);
  sqliteFree(pTable->aCol);
  if( pTable->zColAff ){
    sqliteFree(pTable->zColAff);
  }
  sqlite3SelectDelete(pTable->pSelect);
  sqliteFree(pTable);
}

/*
** Unlink the given table from the hash tables and the delete the
** table structure with all its indices and foreign keys.
*/
static void sqliteUnlinkAndDeleteTable(sqlite *db, Table *p){
  Table *pOld;
  FKey *pF1, *pF2;
  int i = p->iDb;
  assert( db!=0 );
  pOld = sqlite3HashInsert(&db->aDb[i].tblHash, p->zName, strlen(p->zName)+1, 0);
  assert( pOld==0 || pOld==p );
  for(pF1=p->pFKey; pF1; pF1=pF1->pNextFrom){
    int nTo = strlen(pF1->zTo) + 1;
    pF2 = sqlite3HashFind(&db->aDb[i].aFKey, pF1->zTo, nTo);
    if( pF2==pF1 ){
      sqlite3HashInsert(&db->aDb[i].aFKey, pF1->zTo, nTo, pF1->pNextTo);
    }else{
      while( pF2 && pF2->pNextTo!=pF1 ){ pF2=pF2->pNextTo; }
      if( pF2 ){
        pF2->pNextTo = pF1->pNextTo;
      }
    }
  }
  sqlite3DeleteTable(db, p);
}

/*
** Construct the name of a user table or index from a token.
**
** Space to hold the name is obtained from sqliteMalloc() and must
** be freed by the calling function.
*/
char *sqlite3TableNameFromToken(Token *pName){
  char *zName = sqliteStrNDup(pName->z, pName->n);
  sqlite3Dequote(zName);
  return zName;
}

/*
** Open the sqlite_master table stored in database number iDb for
** writing. The table is opened using cursor 0.
*/
void sqlite3OpenMasterTable(Vdbe *v, int iDb){
  sqlite3VdbeAddOp(v, OP_Integer, iDb, 0);
  sqlite3VdbeAddOp(v, OP_OpenWrite, 0, MASTER_ROOT);
  sqlite3VdbeAddOp(v, OP_SetNumColumns, 0, 5); /* sqlite_master has 5 columns */
}

/*
** The token *pName contains the name of a database (either "main" or
** "temp" or the name of an attached db). This routine returns the
** index of the named database in db->aDb[], or -1 if the named db 
** does not exist.
*/
int findDb(sqlite3 *db, Token *pName){
  int i;
  for(i=0; i<db->nDb; i++){
    if( pName->n==strlen(db->aDb[i].zName) && 
        0==sqlite3StrNICmp(db->aDb[i].zName, pName->z, pName->n) ){
      return i;
    }
  }
  return -1;
}

static int resolveSchemaName(
  Parse *pParse, 
  Token *pName1, 
  Token *pName2, 
  Token **pUnqual
){
  int iDb;
  sqlite3 *db = pParse->db;

  if( pName2 && pName2->n>0 ){
    assert( !db->init.busy );
    *pUnqual = pName2;
    iDb = findDb(db, pName1);
    if( iDb<0 ){
      sqlite3ErrorMsg(pParse, "unknown database %T", pName1);
      pParse->nErr++;
      return -1;
    }
  }else{
    assert( db->init.iDb==0 || db->init.busy );
    iDb = db->init.iDb;
    *pUnqual = pName1;
  }
  return iDb;
}

/*
** Begin constructing a new table representation in memory.  This is
** the first of several action routines that get called in response
** to a CREATE TABLE statement.  In particular, this routine is called
** after seeing tokens "CREATE" and "TABLE" and the table name.  The
** pStart token is the CREATE and pName is the table name.  The isTemp
** flag is true if the table should be stored in the auxiliary database
** file instead of in the main database file.  This is normally the case
** when the "TEMP" or "TEMPORARY" keyword occurs in between
** CREATE and TABLE.
**
** The new table record is initialized and put in pParse->pNewTable.
** As more of the CREATE TABLE statement is parsed, additional action
** routines will be called to add more information to this record.
** At the end of the CREATE TABLE statement, the sqlite3EndTable() routine
** is called to complete the construction of the new table record.
*/
void sqlite3StartTable(
  Parse *pParse,   /* Parser context */
  Token *pStart,   /* The "CREATE" token */
  Token *pName1,   /* First part of the name of the table or view */
  Token *pName2,   /* Second part of the name of the table or view */
  int isTemp,      /* True if this is a TEMP table */
  int isView       /* True if this is a VIEW */
){
  Table *pTable;
  Index *pIdx;
  char *zName;
  sqlite *db = pParse->db;
  Vdbe *v;
  int iDb;         /* Database number to create the table in */
  Token *pName;    /* Unqualified name of the table to create */

  /* The table or view name to create is passed to this routine via tokens
  ** pName1 and pName2. If the table name was fully qualified, for example:
  **
  ** CREATE TABLE xxx.yyy (...);
  ** 
  ** Then pName1 is set to "xxx" and pName2 "yyy". On the other hand if
  ** the table name is not fully qualified, i.e.:
  **
  ** CREATE TABLE yyy(...);
  **
  ** Then pName1 is set to "yyy" and pName2 is "".
  **
  ** The call below sets the pName pointer to point at the token (pName1 or
  ** pName2) that stores the unqualified table name. The variable iDb is
  ** set to the index of the database that the table or view is to be
  ** created in.
  */
  iDb = resolveSchemaName(pParse, pName1, pName2, &pName);
  if( iDb<0 ) return;
  if( isTemp && iDb>1 ){
    /* If creating a temp table, the name may not be qualified */
    sqlite3ErrorMsg(pParse, "temporary table name must be unqualified");
    pParse->nErr++;
    return;
  }
  if( isTemp ) iDb = 1;

  pParse->sNameToken = *pName;
  zName = sqlite3TableNameFromToken(pName);
  if( zName==0 ) return;
  if( db->init.iDb==1 ) isTemp = 1;
#ifndef SQLITE_OMIT_AUTHORIZATION
  assert( (isTemp & 1)==isTemp );
  {
    int code;
    char *zDb = db->aDb[iDb].zName;
    if( sqlite3AuthCheck(pParse, SQLITE_INSERT, SCHEMA_TABLE(isTemp), 0, zDb) ){
      sqliteFree(zName);
      return;
    }
    if( isView ){
      if( isTemp ){
        code = SQLITE_CREATE_TEMP_VIEW;
      }else{
        code = SQLITE_CREATE_VIEW;
      }
    }else{
      if( isTemp ){
        code = SQLITE_CREATE_TEMP_TABLE;
      }else{
        code = SQLITE_CREATE_TABLE;
      }
    }
    if( sqlite3AuthCheck(pParse, code, zName, 0, zDb) ){
      sqliteFree(zName);
      return;
    }
  }
#endif

  /* Before trying to create a temporary table, make sure the Btree for
  ** holding temporary tables is open.
  */
  if( isTemp && db->aDb[1].pBt==0 && !pParse->explain ){
    int rc = sqlite3BtreeFactory(db, 0, 0, MAX_PAGES, &db->aDb[1].pBt);
    if( rc!=SQLITE_OK ){
      sqlite3ErrorMsg(pParse, "unable to open a temporary database "
        "file for storing temporary tables");
      pParse->nErr++;
      return;
    }
    if( db->flags & SQLITE_InTrans ){
      rc = sqlite3BtreeBeginTrans(db->aDb[1].pBt);
      if( rc!=SQLITE_OK ){
        sqlite3ErrorMsg(pParse, "unable to get a write lock on "
          "the temporary database file");
        return;
      }
    }
  }

  /* Make sure the new table name does not collide with an existing
  ** index or table name.  Issue an error message if it does.
  **
  ** If we are re-reading the sqlite_master table because of a schema
  ** change and a new permanent table is found whose name collides with
  ** an existing temporary table, that is not an error.
  */
  pTable = sqlite3FindTable(db, zName, 0);
  if( pTable!=0 && (pTable->iDb==iDb || !db->init.busy) ){
    sqlite3ErrorMsg(pParse, "table %T already exists", pName);
    sqliteFree(zName);
    return;
  }
  if( (pIdx = sqlite3FindIndex(db, zName, 0))!=0 &&
          (pIdx->iDb==0 || !db->init.busy) ){
    sqlite3ErrorMsg(pParse, "there is already an index named %s", zName);
    sqliteFree(zName);
    return;
  }
  pTable = sqliteMalloc( sizeof(Table) );
  if( pTable==0 ){
    sqliteFree(zName);
    return;
  }
  pTable->zName = zName;
  pTable->nCol = 0;
  pTable->aCol = 0;
  pTable->iPKey = -1;
  pTable->pIndex = 0;
  pTable->iDb = iDb;
  if( pParse->pNewTable ) sqlite3DeleteTable(db, pParse->pNewTable);
  pParse->pNewTable = pTable;

  /* Begin generating the code that will insert the table record into
  ** the SQLITE_MASTER table.  Note in particular that we must go ahead
  ** and allocate the record number for the table entry now.  Before any
  ** PRIMARY KEY or UNIQUE keywords are parsed.  Those keywords will cause
  ** indices to be created and the table record must come before the 
  ** indices.  Hence, the record number for the table must be allocated
  ** now.
  */
  if( !db->init.busy && (v = sqlite3GetVdbe(pParse))!=0 ){
    sqlite3BeginWriteOperation(pParse, 0, iDb);
    if( !isTemp ){
      /* Every time a new table is created the file-format
      ** and encoding meta-values are set in the database, in
      ** case this is the first table created.
      */
      sqlite3VdbeAddOp(v, OP_Integer, db->file_format, 0);
      sqlite3VdbeAddOp(v, OP_SetCookie, iDb, 1);
      sqlite3VdbeAddOp(v, OP_Integer, db->enc, 0);
      sqlite3VdbeAddOp(v, OP_SetCookie, iDb, 4);
    }
    sqlite3OpenMasterTable(v, iDb);
    sqlite3VdbeAddOp(v, OP_NewRecno, 0, 0);
    sqlite3VdbeAddOp(v, OP_Dup, 0, 0);
    sqlite3VdbeAddOp(v, OP_String, 0, 0);
    sqlite3VdbeAddOp(v, OP_PutIntKey, 0, 0);
  }
}

/*
** Add a new column to the table currently being constructed.
**
** The parser calls this routine once for each column declaration
** in a CREATE TABLE statement.  sqlite3StartTable() gets called
** first to get things going.  Then this routine is called for each
** column.
*/
void sqlite3AddColumn(Parse *pParse, Token *pName){
  Table *p;
  int i;
  char *z = 0;
  Column *pCol;
  if( (p = pParse->pNewTable)==0 ) return;
  sqlite3SetNString(&z, pName->z, pName->n, 0);
  if( z==0 ) return;
  sqlite3Dequote(z);
  for(i=0; i<p->nCol; i++){
    if( sqlite3StrICmp(z, p->aCol[i].zName)==0 ){
      sqlite3ErrorMsg(pParse, "duplicate column name: %s", z);
      sqliteFree(z);
      return;
    }
  }
  if( (p->nCol & 0x7)==0 ){
    Column *aNew;
    aNew = sqliteRealloc( p->aCol, (p->nCol+8)*sizeof(p->aCol[0]));
    if( aNew==0 ) return;
    p->aCol = aNew;
  }
  pCol = &p->aCol[p->nCol];
  memset(pCol, 0, sizeof(p->aCol[0]));
  pCol->zName = z;
 
  /* If there is no type specified, columns have the default affinity
  ** 'NUMERIC'. If there is a type specified, then sqlite3AddColumnType()
  ** will be called next to set pCol->affinity correctly.
  */
  pCol->affinity = SQLITE_AFF_NUMERIC;
  pCol->pColl = pParse->db->pDfltColl;
  p->nCol++;
}

/*
** This routine is called by the parser while in the middle of
** parsing a CREATE TABLE statement.  A "NOT NULL" constraint has
** been seen on a column.  This routine sets the notNull flag on
** the column currently under construction.
*/
void sqlite3AddNotNull(Parse *pParse, int onError){
  Table *p;
  int i;
  if( (p = pParse->pNewTable)==0 ) return;
  i = p->nCol-1;
  if( i>=0 ) p->aCol[i].notNull = onError;
}

/*
** This routine is called by the parser while in the middle of
** parsing a CREATE TABLE statement.  The pFirst token is the first
** token in the sequence of tokens that describe the type of the
** column currently under construction.   pLast is the last token
** in the sequence.  Use this information to construct a string
** that contains the typename of the column and store that string
** in zType.
*/ 
void sqlite3AddColumnType(Parse *pParse, Token *pFirst, Token *pLast){
  Table *p;
  int i, j;
  int n;
  char *z, **pz;
  Column *pCol;
  if( (p = pParse->pNewTable)==0 ) return;
  i = p->nCol-1;
  if( i<0 ) return;
  pCol = &p->aCol[i];
  pz = &pCol->zType;
  n = pLast->n + Addr(pLast->z) - Addr(pFirst->z);
  sqlite3SetNString(pz, pFirst->z, n, 0);
  z = *pz;
  if( z==0 ) return;
  for(i=j=0; z[i]; i++){
    int c = z[i];
    if( isspace(c) ) continue;
    z[j++] = c;
  }
  z[j] = 0;
//  pCol->sortOrder = sqlite3CollateType(z, n);
  pCol->affinity = sqlite3AffinityType(z, n);
}

/*
** The given token is the default value for the last column added to
** the table currently under construction.  If "minusFlag" is true, it
** means the value token was preceded by a minus sign.
**
** This routine is called by the parser while in the middle of
** parsing a CREATE TABLE statement.
*/
void sqlite3AddDefaultValue(Parse *pParse, Token *pVal, int minusFlag){
  Table *p;
  int i;
  char **pz;
  if( (p = pParse->pNewTable)==0 ) return;
  i = p->nCol-1;
  if( i<0 ) return;
  pz = &p->aCol[i].zDflt;
  if( minusFlag ){
    sqlite3SetNString(pz, "-", 1, pVal->z, pVal->n, 0);
  }else{
    sqlite3SetNString(pz, pVal->z, pVal->n, 0);
  }
  sqlite3Dequote(*pz);
}

/*
** Designate the PRIMARY KEY for the table.  pList is a list of names 
** of columns that form the primary key.  If pList is NULL, then the
** most recently added column of the table is the primary key.
**
** A table can have at most one primary key.  If the table already has
** a primary key (and this is the second primary key) then create an
** error.
**
** If the PRIMARY KEY is on a single column whose datatype is INTEGER,
** then we will try to use that column as the row id.  (Exception:
** For backwards compatibility with older databases, do not do this
** if the file format version number is less than 1.)  Set the Table.iPKey
** field of the table under construction to be the index of the
** INTEGER PRIMARY KEY column.  Table.iPKey is set to -1 if there is
** no INTEGER PRIMARY KEY.
**
** If the key is not an INTEGER PRIMARY KEY, then create a unique
** index for the key.  No index is created for INTEGER PRIMARY KEYs.
*/
void sqlite3AddPrimaryKey(Parse *pParse, IdList *pList, int onError){
  Table *pTab = pParse->pNewTable;
  char *zType = 0;
  int iCol = -1, i;
  if( pTab==0 ) goto primary_key_exit;
  if( pTab->hasPrimKey ){
    sqlite3ErrorMsg(pParse, 
      "table \"%s\" has more than one primary key", pTab->zName);
    goto primary_key_exit;
  }
  pTab->hasPrimKey = 1;
  if( pList==0 ){
    iCol = pTab->nCol - 1;
    pTab->aCol[iCol].isPrimKey = 1;
  }else{
    for(i=0; i<pList->nId; i++){
      for(iCol=0; iCol<pTab->nCol; iCol++){
        if( sqlite3StrICmp(pList->a[i].zName, pTab->aCol[iCol].zName)==0 ){
          break;
        }
      }
      if( iCol<pTab->nCol ) pTab->aCol[iCol].isPrimKey = 1;
    }
    if( pList->nId>1 ) iCol = -1;
  }
  if( iCol>=0 && iCol<pTab->nCol ){
    zType = pTab->aCol[iCol].zType;
  }
  if( zType && sqlite3StrICmp(zType, "INTEGER")==0 ){
    pTab->iPKey = iCol;
    pTab->keyConf = onError;
  }else{
    sqlite3CreateIndex(pParse, 0, 0, 0, pList, onError, 0, 0);
    pList = 0;
  }

primary_key_exit:
  sqlite3IdListDelete(pList);
  return;
}

/*
** Return a pointer to CollSeq given the name of a collating sequence.
** If the collating sequence did not previously exist, create it but
** assign it an NULL comparison function.
*/
CollSeq *sqlite3CollateType(Parse *pParse, const char *zType, int nType){
  CollSeq *pColl;
  sqlite *db = pParse->db;

  pColl = sqlite3HashFind(&db->aCollSeq, zType, nType);
  if( pColl==0 ){
    sqlite3ChangeCollatingFunction(db, zType, nType, 0, 0);
    pColl = sqlite3HashFind(&db->aCollSeq, zType, nType);
  }
  return pColl;
}

/*
** Set the collation function of the most recently parsed table column
** to the CollSeq given.
*/
void sqlite3AddCollateType(Parse *pParse, const char *zType, int nType){
  Table *p;
  CollSeq *pColl;
  sqlite *db = pParse->db;

  if( (p = pParse->pNewTable)==0 ) return;
  pColl = sqlite3HashFind(&db->aCollSeq, zType, nType);
  if( pColl==0 ){
    pColl = sqlite3ChangeCollatingFunction(db, zType, nType, 0, 0);
  }
  if( pColl ){
    p->aCol[p->nCol-1].pColl = pColl;
  }
}

/*
** Create or modify a collating sequence entry in the sqlite.aCollSeq
** table.
**
** Once an entry is added to the sqlite.aCollSeq table, it can never
** be removed, though is comparison function or user data can be changed.
**
** Return a pointer to the collating function that was created or modified.
*/
CollSeq *sqlite3ChangeCollatingFunction(
  sqlite *db,             /* Database into which to insert the collation */
  const char *zName,      /* Name of the collation */
  int nName,              /* Number of characters in zName */
  void *pUser,            /* First argument to xCmp */
  int (*xCmp)(void*,int,const void*,int,const void*) /* Comparison function */
){
  CollSeq *pColl;

  pColl = sqlite3HashFind(&db->aCollSeq, zName, nName);
  if( pColl==0 ){
    pColl = sqliteMallocRaw( sizeof(*pColl) + nName + 1 );
    if( pColl==0 ){
      return 0;
    }
    pColl->zName = (char*)&pColl[1];
    memcpy(pColl->zName, zName, nName+1);
    sqlite3HashInsert(&db->aCollSeq, pColl->zName, nName, pColl);
  }
  pColl->pUser = pUser;
  pColl->xCmp = xCmp;
  return pColl;
}

/*
** Scan the column type name zType (length nType) and return the
** associated affinity type.
*/
char sqlite3AffinityType(const char *zType, int nType){
  int n, i;
  struct {
    const char *zSub;  /* Keywords substring to search for */
    int nSub;          /* length of zSub */
    char affinity;     /* Affinity to return if it matches */
  } substrings[] = {
    {"INT",  3, SQLITE_AFF_INTEGER},
    {"CHAR", 4, SQLITE_AFF_TEXT},
    {"CLOB", 4, SQLITE_AFF_TEXT},
    {"TEXT", 4, SQLITE_AFF_TEXT},
    {"BLOB", 4, SQLITE_AFF_NONE},
  };

  for(i=0; i<sizeof(substrings)/sizeof(substrings[0]); i++){
    int c1 = substrings[i].zSub[0];
    int c2 = tolower(c1);
    int limit = nType - substrings[i].nSub;
    const char *z = substrings[i].zSub;
    for(n=0; n<=limit; n++){
      int c = zType[n];
      if( (c==c1 || c==c2)
             && 0==sqlite3StrNICmp(&zType[n], z, substrings[i].nSub) ){
        return substrings[i].affinity;
      }
    }
  }
  return SQLITE_AFF_NUMERIC;
}

/*
** Come up with a new random value for the schema cookie.  Make sure
** the new value is different from the old.
**
** The schema cookie is used to determine when the schema for the
** database changes.  After each schema change, the cookie value
** changes.  When a process first reads the schema it records the
** cookie.  Thereafter, whenever it goes to access the database,
** it checks the cookie to make sure the schema has not changed
** since it was last read.
**
** This plan is not completely bullet-proof.  It is possible for
** the schema to change multiple times and for the cookie to be
** set back to prior value.  But schema changes are infrequent
** and the probability of hitting the same cookie value is only
** 1 chance in 2^32.  So we're safe enough.
*/
void sqlite3ChangeCookie(sqlite *db, Vdbe *v, int iDb){
  if( db->next_cookie==db->aDb[0].schema_cookie ){
    unsigned char r;
    sqlite3Randomness(1, &r);
    db->next_cookie = db->aDb[0].schema_cookie + r + 1;
    db->flags |= SQLITE_InternChanges;
    sqlite3VdbeAddOp(v, OP_Integer, db->next_cookie, 0);
    sqlite3VdbeAddOp(v, OP_SetCookie, iDb, 0);
  }
}

/*
** Measure the number of characters needed to output the given
** identifier.  The number returned includes any quotes used
** but does not include the null terminator.
*/
static int identLength(const char *z){
  int n;
  int needQuote = 0;
  for(n=0; *z; n++, z++){
    if( *z=='\'' ){ n++; needQuote=1; }
  }
  return n + needQuote*2;
}

/*
** Write an identifier onto the end of the given string.  Add
** quote characters as needed.
*/
static void identPut(char *z, int *pIdx, char *zIdent){
  int i, j, needQuote;
  i = *pIdx;
  for(j=0; zIdent[j]; j++){
    if( !isalnum(zIdent[j]) && zIdent[j]!='_' ) break;
  }
  needQuote =  zIdent[j]!=0 || isdigit(zIdent[0])
                  || sqlite3KeywordCode(zIdent, j)!=TK_ID;
  if( needQuote ) z[i++] = '\'';
  for(j=0; zIdent[j]; j++){
    z[i++] = zIdent[j];
    if( zIdent[j]=='\'' ) z[i++] = '\'';
  }
  if( needQuote ) z[i++] = '\'';
  z[i] = 0;
  *pIdx = i;
}

/*
** Generate a CREATE TABLE statement appropriate for the given
** table.  Memory to hold the text of the statement is obtained
** from sqliteMalloc() and must be freed by the calling function.
*/
static char *createTableStmt(Table *p){
  int i, k, n;
  char *zStmt;
  char *zSep, *zSep2, *zEnd;
  n = 0;
  for(i=0; i<p->nCol; i++){
    n += identLength(p->aCol[i].zName);
  }
  n += identLength(p->zName);
  if( n<40 ){
    zSep = "";
    zSep2 = ",";
    zEnd = ")";
  }else{
    zSep = "\n  ";
    zSep2 = ",\n  ";
    zEnd = "\n)";
  }
  n += 35 + 6*p->nCol;
  zStmt = sqliteMallocRaw( n );
  if( zStmt==0 ) return 0;
  strcpy(zStmt, p->iDb==1 ? "CREATE TEMP TABLE " : "CREATE TABLE ");
  k = strlen(zStmt);
  identPut(zStmt, &k, p->zName);
  zStmt[k++] = '(';
  for(i=0; i<p->nCol; i++){
    strcpy(&zStmt[k], zSep);
    k += strlen(&zStmt[k]);
    zSep = zSep2;
    identPut(zStmt, &k, p->aCol[i].zName);
  }
  strcpy(&zStmt[k], zEnd);
  return zStmt;
}

/*
** This routine is called to report the final ")" that terminates
** a CREATE TABLE statement.
**
** The table structure that other action routines have been building
** is added to the internal hash tables, assuming no errors have
** occurred.
**
** An entry for the table is made in the master table on disk, unless
** this is a temporary table or db->init.busy==1.  When db->init.busy==1
** it means we are reading the sqlite_master table because we just
** connected to the database or because the sqlite_master table has
** recently changes, so the entry for this table already exists in
** the sqlite_master table.  We do not want to create it again.
**
** If the pSelect argument is not NULL, it means that this routine
** was called to create a table generated from a 
** "CREATE TABLE ... AS SELECT ..." statement.  The column names of
** the new table will match the result set of the SELECT.
*/
void sqlite3EndTable(Parse *pParse, Token *pEnd, Select *pSelect){
  Table *p;
  sqlite *db = pParse->db;

  if( (pEnd==0 && pSelect==0) || pParse->nErr || sqlite3_malloc_failed ) return;
  p = pParse->pNewTable;
  if( p==0 ) return;

  /* If the table is generated from a SELECT, then construct the
  ** list of columns and the text of the table.
  */
  if( pSelect ){
    Table *pSelTab = sqlite3ResultSetOfSelect(pParse, 0, pSelect);
    if( pSelTab==0 ) return;
    assert( p->aCol==0 );
    p->nCol = pSelTab->nCol;
    p->aCol = pSelTab->aCol;
    pSelTab->nCol = 0;
    pSelTab->aCol = 0;
    sqlite3DeleteTable(0, pSelTab);
  }

  /* If the db->init.busy is 1 it means we are reading the SQL off the
  ** "sqlite_master" or "sqlite_temp_master" table on the disk.
  ** So do not write to the disk again.  Extract the root page number
  ** for the table from the db->init.newTnum field.  (The page number
  ** should have been put there by the sqliteOpenCb routine.)
  */
  if( db->init.busy ){
    p->tnum = db->init.newTnum;
  }

  /* If not initializing, then create a record for the new table
  ** in the SQLITE_MASTER table of the database.  The record number
  ** for the new table entry should already be on the stack.
  **
  ** If this is a TEMPORARY table, write the entry into the auxiliary
  ** file instead of into the main database file.
  */
  if( !db->init.busy ){
    int n;
    Vdbe *v;

    v = sqlite3GetVdbe(pParse);
    if( v==0 ) return;
    if( p->pSelect==0 ){
      /* A regular table */
      sqlite3VdbeOp3(v, OP_CreateTable, 0, p->iDb, (char*)&p->tnum, P3_POINTER);
    }else{
      /* A view */
      sqlite3VdbeAddOp(v, OP_Integer, 0, 0);
    }
    p->tnum = 0;
    sqlite3VdbeAddOp(v, OP_Pull, 1, 0);
    sqlite3VdbeOp3(v, OP_String, 0, 0, p->pSelect==0?"table":"view", P3_STATIC);
    sqlite3VdbeOp3(v, OP_String, 0, 0, p->zName, 0);
    sqlite3VdbeOp3(v, OP_String, 0, 0, p->zName, 0);
    sqlite3VdbeAddOp(v, OP_Dup, 4, 0);
    if( pSelect ){
      char *z = createTableStmt(p);
      n = z ? strlen(z) : 0;
      sqlite3VdbeAddOp(v, OP_String, 0, 0);
      sqlite3VdbeChangeP3(v, -1, z, n);
      sqliteFree(z);
    }else{
      if( p->pSelect ){
        sqlite3VdbeOp3(v, OP_String, 0, 0, "CREATE VIEW ", P3_STATIC);
      }else{
        sqlite3VdbeOp3(v, OP_String, 0, 0, "CREATE TABLE ", P3_STATIC);
      }
      assert( pEnd!=0 );
      n = Addr(pEnd->z) - Addr(pParse->sNameToken.z) + 1;
      sqlite3VdbeAddOp(v, OP_String, 0, 0);
      sqlite3VdbeChangeP3(v, -1, pParse->sNameToken.z, n);
      sqlite3VdbeAddOp(v, OP_Concat, 2, 0);
    }
    sqlite3VdbeOp3(v, OP_MakeRecord, 5, 0, "tttit", P3_STATIC);
    sqlite3VdbeAddOp(v, OP_PutIntKey, 0, 0);
    if( !p->iDb ){
      sqlite3ChangeCookie(db, v, p->iDb);
    }
    sqlite3VdbeAddOp(v, OP_Close, 0, 0);
    if( pSelect ){
      sqlite3VdbeAddOp(v, OP_Integer, p->iDb, 0);
      sqlite3VdbeAddOp(v, OP_OpenWrite, 1, 0);
      pParse->nTab = 2;
      sqlite3Select(pParse, pSelect, SRT_Table, 1, 0, 0, 0, 0);
    }
    sqlite3EndWriteOperation(pParse);
  }

  /* Add the table to the in-memory representation of the database.
  */
  if( pParse->explain==0 && pParse->nErr==0 ){
    Table *pOld;
    FKey *pFKey;
    pOld = sqlite3HashInsert(&db->aDb[p->iDb].tblHash, 
                            p->zName, strlen(p->zName)+1, p);
    if( pOld ){
      assert( p==pOld );  /* Malloc must have failed inside HashInsert() */
      return;
    }
    for(pFKey=p->pFKey; pFKey; pFKey=pFKey->pNextFrom){
      int nTo = strlen(pFKey->zTo) + 1;
      pFKey->pNextTo = sqlite3HashFind(&db->aDb[p->iDb].aFKey, pFKey->zTo, nTo);
      sqlite3HashInsert(&db->aDb[p->iDb].aFKey, pFKey->zTo, nTo, pFKey);
    }
    pParse->pNewTable = 0;
    db->nTable++;
    db->flags |= SQLITE_InternChanges;
  }
}

/*
** The parser calls this routine in order to create a new VIEW
*/
void sqlite3CreateView(
  Parse *pParse,     /* The parsing context */
  Token *pBegin,     /* The CREATE token that begins the statement */
  Token *pName,      /* The token that holds the name of the view */
  Select *pSelect,   /* A SELECT statement that will become the new view */
  int isTemp         /* TRUE for a TEMPORARY view */
){
  Table *p;
  int n;
  const char *z;
  Token sEnd;
  DbFixer sFix;

  sqlite3StartTable(pParse, pBegin, pName, 0, isTemp, 1);
  p = pParse->pNewTable;
  if( p==0 || pParse->nErr ){
    sqlite3SelectDelete(pSelect);
    return;
  }
  if( sqlite3FixInit(&sFix, pParse, p->iDb, "view", pName)
    && sqlite3FixSelect(&sFix, pSelect)
  ){
    sqlite3SelectDelete(pSelect);
    return;
  }

  /* Make a copy of the entire SELECT statement that defines the view.
  ** This will force all the Expr.token.z values to be dynamically
  ** allocated rather than point to the input string - which means that
  ** they will persist after the current sqlite3_exec() call returns.
  */
  p->pSelect = sqlite3SelectDup(pSelect);
  sqlite3SelectDelete(pSelect);
  if( !pParse->db->init.busy ){
    sqlite3ViewGetColumnNames(pParse, p);
  }

  /* Locate the end of the CREATE VIEW statement.  Make sEnd point to
  ** the end.
  */
  sEnd = pParse->sLastToken;
  if( sEnd.z[0]!=0 && sEnd.z[0]!=';' ){
    sEnd.z += sEnd.n;
  }
  sEnd.n = 0;
  n = ((int)sEnd.z) - (int)pBegin->z;
  z = pBegin->z;
  while( n>0 && (z[n-1]==';' || isspace(z[n-1])) ){ n--; }
  sEnd.z = &z[n-1];
  sEnd.n = 1;

  /* Use sqlite3EndTable() to add the view to the SQLITE_MASTER table */
  sqlite3EndTable(pParse, &sEnd, 0);
  return;
}

/*
** The Table structure pTable is really a VIEW.  Fill in the names of
** the columns of the view in the pTable structure.  Return the number
** of errors.  If an error is seen leave an error message in pParse->zErrMsg.
*/
int sqlite3ViewGetColumnNames(Parse *pParse, Table *pTable){
  ExprList *pEList;
  Select *pSel;
  Table *pSelTab;
  int nErr = 0;

  assert( pTable );

  /* A positive nCol means the columns names for this view are
  ** already known.
  */
  if( pTable->nCol>0 ) return 0;

  /* A negative nCol is a special marker meaning that we are currently
  ** trying to compute the column names.  If we enter this routine with
  ** a negative nCol, it means two or more views form a loop, like this:
  **
  **     CREATE VIEW one AS SELECT * FROM two;
  **     CREATE VIEW two AS SELECT * FROM one;
  **
  ** Actually, this error is caught previously and so the following test
  ** should always fail.  But we will leave it in place just to be safe.
  */
  if( pTable->nCol<0 ){
    sqlite3ErrorMsg(pParse, "view %s is circularly defined", pTable->zName);
    return 1;
  }

  /* If we get this far, it means we need to compute the table names.
  */
  assert( pTable->pSelect ); /* If nCol==0, then pTable must be a VIEW */
  pSel = pTable->pSelect;

  /* Note that the call to sqlite3ResultSetOfSelect() will expand any
  ** "*" elements in this list.  But we will need to restore the list
  ** back to its original configuration afterwards, so we save a copy of
  ** the original in pEList.
  */
  pEList = pSel->pEList;
  pSel->pEList = sqlite3ExprListDup(pEList);
  if( pSel->pEList==0 ){
    pSel->pEList = pEList;
    return 1;  /* Malloc failed */
  }
  pTable->nCol = -1;
  pSelTab = sqlite3ResultSetOfSelect(pParse, 0, pSel);
  if( pSelTab ){
    assert( pTable->aCol==0 );
    pTable->nCol = pSelTab->nCol;
    pTable->aCol = pSelTab->aCol;
    pSelTab->nCol = 0;
    pSelTab->aCol = 0;
    sqlite3DeleteTable(0, pSelTab);
    DbSetProperty(pParse->db, pTable->iDb, DB_UnresetViews);
  }else{
    pTable->nCol = 0;
    nErr++;
  }
  sqlite3SelectUnbind(pSel);
  sqlite3ExprListDelete(pSel->pEList);
  pSel->pEList = pEList;
  return nErr;  
}

/*
** Clear the column names from the VIEW pTable.
**
** This routine is called whenever any other table or view is modified.
** The view passed into this routine might depend directly or indirectly
** on the modified or deleted table so we need to clear the old column
** names so that they will be recomputed.
*/
static void sqliteViewResetColumnNames(Table *pTable){
  int i;
  Column *pCol;
  assert( pTable!=0 && pTable->pSelect!=0 );
  for(i=0, pCol=pTable->aCol; i<pTable->nCol; i++, pCol++){
    sqliteFree(pCol->zName);
    sqliteFree(pCol->zDflt);
    sqliteFree(pCol->zType);
  }
  sqliteFree(pTable->aCol);
  pTable->aCol = 0;
  pTable->nCol = 0;
}

/*
** Clear the column names from every VIEW in database idx.
*/
static void sqliteViewResetAll(sqlite *db, int idx){
  HashElem *i;
  if( !DbHasProperty(db, idx, DB_UnresetViews) ) return;
  for(i=sqliteHashFirst(&db->aDb[idx].tblHash); i; i=sqliteHashNext(i)){
    Table *pTab = sqliteHashData(i);
    if( pTab->pSelect ){
      sqliteViewResetColumnNames(pTab);
    }
  }
  DbClearProperty(db, idx, DB_UnresetViews);
}

/*
** Given a token, look up a table with that name.  If not found, leave
** an error for the parser to find and return NULL.
*/
Table *sqlite3TableFromToken(Parse *pParse, Token *pTok){
  char *zName;
  Table *pTab;
  zName = sqlite3TableNameFromToken(pTok);
  if( zName==0 ) return 0;
  pTab = sqlite3FindTable(pParse->db, zName, 0);
  sqliteFree(zName);
  if( pTab==0 ){
    sqlite3ErrorMsg(pParse, "no such table: %T", pTok);
  }
  return pTab;
}

/*
** This routine is called to do the work of a DROP TABLE statement.
** pName is the name of the table to be dropped.
*/
void sqlite3DropTable(Parse *pParse, Token *pName, int isView){
  Table *pTable;
  Vdbe *v;
  int base;
  sqlite *db = pParse->db;
  int iDb;

  if( pParse->nErr || sqlite3_malloc_failed ) return;
  pTable = sqlite3TableFromToken(pParse, pName);
  if( pTable==0 ) return;
  iDb = pTable->iDb;
  assert( iDb>=0 && iDb<db->nDb );
#ifndef SQLITE_OMIT_AUTHORIZATION
  {
    int code;
    const char *zTab = SCHEMA_TABLE(pTable->iDb);
    const char *zDb = db->aDb[pTable->iDb].zName;
    if( sqlite3AuthCheck(pParse, SQLITE_DELETE, zTab, 0, zDb)){
      return;
    }
    if( isView ){
      if( iDb==1 ){
        code = SQLITE_DROP_TEMP_VIEW;
      }else{
        code = SQLITE_DROP_VIEW;
      }
    }else{
      if( iDb==1 ){
        code = SQLITE_DROP_TEMP_TABLE;
      }else{
        code = SQLITE_DROP_TABLE;
      }
    }
    if( sqlite3AuthCheck(pParse, code, pTable->zName, 0, zDb) ){
      return;
    }
    if( sqlite3AuthCheck(pParse, SQLITE_DELETE, pTable->zName, 0, zDb) ){
      return;
    }
  }
#endif
  if( pTable->readOnly ){
    sqlite3ErrorMsg(pParse, "table %s may not be dropped", pTable->zName);
    pParse->nErr++;
    return;
  }
  if( isView && pTable->pSelect==0 ){
    sqlite3ErrorMsg(pParse, "use DROP TABLE to delete table %s", pTable->zName);
    return;
  }
  if( !isView && pTable->pSelect ){
    sqlite3ErrorMsg(pParse, "use DROP VIEW to delete view %s", pTable->zName);
    return;
  }

  /* Generate code to remove the table from the master table
  ** on disk.
  */
  v = sqlite3GetVdbe(pParse);
  if( v ){
    static VdbeOpList dropTable[] = {
      { OP_Rewind,     0, ADDR(10), 0},
      { OP_String,     0, 0,        0}, /* 1 */
      { OP_MemStore,   1, 1,        0},
      { OP_MemLoad,    1, 0,        0}, /* 3 */
      { OP_Column,     0, 2,        0},
      { OP_Ne,         0, ADDR(9),  0},
      { OP_Delete,     0, 0,        0},
      { OP_Rewind,     0, ADDR(10), 0},
      { OP_Goto,       0, ADDR(3),  0},
      { OP_Next,       0, ADDR(3),  0}, /* 9 */
    };
    Index *pIdx;
    Trigger *pTrigger;
    sqlite3BeginWriteOperation(pParse, 0, pTable->iDb);

    /* Drop all triggers associated with the table being dropped */
    pTrigger = pTable->pTrigger;
    while( pTrigger ){
      assert( pTrigger->iDb==pTable->iDb || pTrigger->iDb==1 );
      sqlite3DropTriggerPtr(pParse, pTrigger, 1);
      if( pParse->explain ){
        pTrigger = pTrigger->pNext;
      }else{
        pTrigger = pTable->pTrigger;
      }
    }

    /* Drop all SQLITE_MASTER entries that refer to the table */
    sqlite3OpenMasterTable(v, pTable->iDb);
    base = sqlite3VdbeAddOpList(v, ArraySize(dropTable), dropTable);
    sqlite3VdbeChangeP3(v, base+1, pTable->zName, 0);

    /* Drop all SQLITE_TEMP_MASTER entries that refer to the table */
    if( pTable->iDb!=1 ){
      sqlite3OpenMasterTable(v, 1);
      base = sqlite3VdbeAddOpList(v, ArraySize(dropTable), dropTable);
      sqlite3VdbeChangeP3(v, base+1, pTable->zName, 0);
    }

    if( pTable->iDb!=1 ){  /* Temp database has no schema cookie */
      sqlite3ChangeCookie(db, v, pTable->iDb);
    }
    sqlite3VdbeAddOp(v, OP_Close, 0, 0);
    if( !isView ){
      sqlite3VdbeAddOp(v, OP_Destroy, pTable->tnum, pTable->iDb);
      for(pIdx=pTable->pIndex; pIdx; pIdx=pIdx->pNext){
        sqlite3VdbeAddOp(v, OP_Destroy, pIdx->tnum, pIdx->iDb);
      }
    }
    sqlite3EndWriteOperation(pParse);
  }

  /* Delete the in-memory description of the table.
  **
  ** Exception: if the SQL statement began with the EXPLAIN keyword,
  ** then no changes should be made.
  */
  if( !pParse->explain ){
    sqliteUnlinkAndDeleteTable(db, pTable);
    db->flags |= SQLITE_InternChanges;
  }
  sqliteViewResetAll(db, iDb);
}

/*
** This routine is called to create a new foreign key on the table
** currently under construction.  pFromCol determines which columns
** in the current table point to the foreign key.  If pFromCol==0 then
** connect the key to the last column inserted.  pTo is the name of
** the table referred to.  pToCol is a list of tables in the other
** pTo table that the foreign key points to.  flags contains all
** information about the conflict resolution algorithms specified
** in the ON DELETE, ON UPDATE and ON INSERT clauses.
**
** An FKey structure is created and added to the table currently
** under construction in the pParse->pNewTable field.  The new FKey
** is not linked into db->aFKey at this point - that does not happen
** until sqlite3EndTable().
**
** The foreign key is set for IMMEDIATE processing.  A subsequent call
** to sqlite3DeferForeignKey() might change this to DEFERRED.
*/
void sqlite3CreateForeignKey(
  Parse *pParse,       /* Parsing context */
  IdList *pFromCol,    /* Columns in this table that point to other table */
  Token *pTo,          /* Name of the other table */
  IdList *pToCol,      /* Columns in the other table */
  int flags            /* Conflict resolution algorithms. */
){
  Table *p = pParse->pNewTable;
  int nByte;
  int i;
  int nCol;
  char *z;
  FKey *pFKey = 0;

  assert( pTo!=0 );
  if( p==0 || pParse->nErr ) goto fk_end;
  if( pFromCol==0 ){
    int iCol = p->nCol-1;
    if( iCol<0 ) goto fk_end;
    if( pToCol && pToCol->nId!=1 ){
      sqlite3ErrorMsg(pParse, "foreign key on %s"
         " should reference only one column of table %T",
         p->aCol[iCol].zName, pTo);
      goto fk_end;
    }
    nCol = 1;
  }else if( pToCol && pToCol->nId!=pFromCol->nId ){
    sqlite3ErrorMsg(pParse,
        "number of columns in foreign key does not match the number of "
        "columns in the referenced table");
    goto fk_end;
  }else{
    nCol = pFromCol->nId;
  }
  nByte = sizeof(*pFKey) + nCol*sizeof(pFKey->aCol[0]) + pTo->n + 1;
  if( pToCol ){
    for(i=0; i<pToCol->nId; i++){
      nByte += strlen(pToCol->a[i].zName) + 1;
    }
  }
  pFKey = sqliteMalloc( nByte );
  if( pFKey==0 ) goto fk_end;
  pFKey->pFrom = p;
  pFKey->pNextFrom = p->pFKey;
  z = (char*)&pFKey[1];
  pFKey->aCol = (struct sColMap*)z;
  z += sizeof(struct sColMap)*nCol;
  pFKey->zTo = z;
  memcpy(z, pTo->z, pTo->n);
  z[pTo->n] = 0;
  z += pTo->n+1;
  pFKey->pNextTo = 0;
  pFKey->nCol = nCol;
  if( pFromCol==0 ){
    pFKey->aCol[0].iFrom = p->nCol-1;
  }else{
    for(i=0; i<nCol; i++){
      int j;
      for(j=0; j<p->nCol; j++){
        if( sqlite3StrICmp(p->aCol[j].zName, pFromCol->a[i].zName)==0 ){
          pFKey->aCol[i].iFrom = j;
          break;
        }
      }
      if( j>=p->nCol ){
        sqlite3ErrorMsg(pParse, 
          "unknown column \"%s\" in foreign key definition", 
          pFromCol->a[i].zName);
        goto fk_end;
      }
    }
  }
  if( pToCol ){
    for(i=0; i<nCol; i++){
      int n = strlen(pToCol->a[i].zName);
      pFKey->aCol[i].zCol = z;
      memcpy(z, pToCol->a[i].zName, n);
      z[n] = 0;
      z += n+1;
    }
  }
  pFKey->isDeferred = 0;
  pFKey->deleteConf = flags & 0xff;
  pFKey->updateConf = (flags >> 8 ) & 0xff;
  pFKey->insertConf = (flags >> 16 ) & 0xff;

  /* Link the foreign key to the table as the last step.
  */
  p->pFKey = pFKey;
  pFKey = 0;

fk_end:
  sqliteFree(pFKey);
  sqlite3IdListDelete(pFromCol);
  sqlite3IdListDelete(pToCol);
}

/*
** This routine is called when an INITIALLY IMMEDIATE or INITIALLY DEFERRED
** clause is seen as part of a foreign key definition.  The isDeferred
** parameter is 1 for INITIALLY DEFERRED and 0 for INITIALLY IMMEDIATE.
** The behavior of the most recently created foreign key is adjusted
** accordingly.
*/
void sqlite3DeferForeignKey(Parse *pParse, int isDeferred){
  Table *pTab;
  FKey *pFKey;
  if( (pTab = pParse->pNewTable)==0 || (pFKey = pTab->pFKey)==0 ) return;
  pFKey->isDeferred = isDeferred;
}

/*
** Create a new index for an SQL table.  pIndex is the name of the index 
** and pTable is the name of the table that is to be indexed.  Both will 
** be NULL for a primary key or an index that is created to satisfy a
** UNIQUE constraint.  If pTable and pIndex are NULL, use pParse->pNewTable
** as the table to be indexed.  pParse->pNewTable is a table that is
** currently being constructed by a CREATE TABLE statement.
**
** pList is a list of columns to be indexed.  pList will be NULL if this
** is a primary key or unique-constraint on the most recent column added
** to the table currently under construction.  
*/
void sqlite3CreateIndex(
  Parse *pParse,   /* All information about this parse */
  Token *pName1,   /* First part of index name. May be NULL */
  Token *pName2,   /* Second part of index name. May be NULL */
  Token *pTblName, /* Name of the table to index. Use pParse->pNewTable if 0 */
  IdList *pList,   /* A list of columns to be indexed */
  int onError,     /* OE_Abort, OE_Ignore, OE_Replace, or OE_None */
  Token *pStart,   /* The CREATE token that begins a CREATE TABLE statement */
  Token *pEnd      /* The ")" that closes the CREATE INDEX statement */
){
  Table *pTab = 0; /* Table to be indexed */
  Index *pIndex;   /* The index to be created */
  char *zName = 0;
  int i, j;
  Token nullId;    /* Fake token for an empty ID list */
  DbFixer sFix;    /* For assigning database names to pTable */
  int isTemp;      /* True for a temporary index */
  sqlite *db = pParse->db;

  int iDb;          /* Index of the database that is being written */
  Token *pName = 0; /* Unqualified name of the index to create */

/*
  if( pParse->nErr || sqlite3_malloc_failed ) goto exit_create_index;
  if( db->init.busy 
     && sqlite3FixInit(&sFix, pParse, db->init.iDb, "index", pName)
     && sqlite3FixSrcList(&sFix, pTable)
  ){
    goto exit_create_index;
  }
*/

  /*
  ** Find the table that is to be indexed.  Return early if not found.
  */
  if( pTblName!=0 ){
    char *zTblName;

    /* Use the two-part index name to determine the database 
    ** to search for the table. If no database name is specified, 
    ** iDb is set to 0. In this case search both the temp and main
    ** databases for the named table.
    */
    assert( pName1 && pName2 );
    iDb = resolveSchemaName(pParse, pName1, pName2, &pName);
    if( iDb<0 ) goto exit_create_index;

    /* Now search for the table in the database iDb. If iDb is
    ** zero, then search both the "main" and "temp" databases.
    */
    zTblName = sqlite3TableNameFromToken(pTblName);
    if( !zTblName ){
      pParse->nErr++;
      pParse->rc = SQLITE_NOMEM;
      goto exit_create_index;
    }
    assert( pName1!=0 );
    if( iDb==0 ){
      pTab = sqlite3FindTable(db, zTblName, "temp");
    }
    if( !pTab ){
      pTab = sqlite3LocateTable(pParse, zTblName, db->aDb[iDb].zName);
    }
    sqliteFree( zTblName );
    if( !pTab ) goto exit_create_index;
    iDb = pTab->iDb;
  }else{
    assert( pName==0 );
    pTab =  pParse->pNewTable;
    iDb = pTab->iDb;
  }

  if( pTab==0 || pParse->nErr ) goto exit_create_index;
  if( pTab->readOnly ){
    sqlite3ErrorMsg(pParse, "table %s may not be indexed", pTab->zName);
    goto exit_create_index;
  }
/*
  if( pTab->iDb>=2 && db->init.busy==0 ){
    sqlite3ErrorMsg(pParse, "table %s may not have indices added", pTab->zName);
    goto exit_create_index;
  }
*/
  if( pTab->pSelect ){
    sqlite3ErrorMsg(pParse, "views may not be indexed");
    goto exit_create_index;
  }
  isTemp = pTab->iDb==1;

  /*
  ** Find the name of the index.  Make sure there is not already another
  ** index or table with the same name.  
  **
  ** Exception:  If we are reading the names of permanent indices from the
  ** sqlite_master table (because some other process changed the schema) and
  ** one of the index names collides with the name of a temporary table or
  ** index, then we will continue to process this index.
  **
  ** If pName==0 it means that we are
  ** dealing with a primary key or UNIQUE constraint.  We have to invent our
  ** own name.
  */
  if( pName && !db->init.busy ){
    Index *pISameName;    /* Another index with the same name */
    Table *pTSameName;    /* A table with same name as the index */
    zName = sqliteStrNDup(pName->z, pName->n);
    if( zName==0 ) goto exit_create_index;
    if( (pISameName = sqlite3FindIndex(db, zName, 0))!=0 ){
      sqlite3ErrorMsg(pParse, "index %s already exists", zName);
      goto exit_create_index;
    }
    if( (pTSameName = sqlite3FindTable(db, zName, 0))!=0 ){
      sqlite3ErrorMsg(pParse, "there is already a table named %s", zName);
      goto exit_create_index;
    }
  }else if( pName==0 ){
    char zBuf[30];
    int n;
    Index *pLoop;
    for(pLoop=pTab->pIndex, n=1; pLoop; pLoop=pLoop->pNext, n++){}
    sprintf(zBuf,"%d)",n);
    zName = 0;
    sqlite3SetString(&zName, "(", pTab->zName, " autoindex ", zBuf, (char*)0);
    if( zName==0 ) goto exit_create_index;
  }else{
    zName = sqliteStrNDup(pName->z, pName->n);
  }

  /* Check for authorization to create an index.
  */
#ifndef SQLITE_OMIT_AUTHORIZATION
  {
    const char *zDb = db->aDb[pTab->iDb].zName;
    if( sqlite3AuthCheck(pParse, SQLITE_INSERT, SCHEMA_TABLE(isTemp), 0, zDb) ){
      goto exit_create_index;
    }
    i = SQLITE_CREATE_INDEX;
    if( isTemp ) i = SQLITE_CREATE_TEMP_INDEX;
    if( sqlite3AuthCheck(pParse, i, zName, pTab->zName, zDb) ){
      goto exit_create_index;
    }
  }
#endif

  /* If pList==0, it means this routine was called to make a primary
  ** key out of the last column added to the table under construction.
  ** So create a fake list to simulate this.
  */
  if( pList==0 ){
    nullId.z = pTab->aCol[pTab->nCol-1].zName;
    nullId.n = strlen(nullId.z);
    pList = sqlite3IdListAppend(0, &nullId);
    if( pList==0 ) goto exit_create_index;
  }

  /* 
  ** Allocate the index structure. 
  */
  pIndex = sqliteMalloc( sizeof(Index) + strlen(zName) + 1 +
                        (sizeof(int) + sizeof(CollSeq*))*pList->nId );
  if( pIndex==0 ) goto exit_create_index;
  pIndex->aiColumn = (int*)&pIndex->keyInfo.aColl[pList->nId];
  pIndex->zName = (char*)&pIndex->aiColumn[pList->nId];
  strcpy(pIndex->zName, zName);
  pIndex->pTable = pTab;
  pIndex->nColumn = pList->nId;
  pIndex->onError = onError;
  pIndex->autoIndex = pName==0;
  pIndex->iDb = iDb;

  /* Scan the names of the columns of the table to be indexed and
  ** load the column indices into the Index structure.  Report an error
  ** if any column is not found.
  */
  for(i=0; i<pList->nId; i++){
    for(j=0; j<pTab->nCol; j++){
      if( sqlite3StrICmp(pList->a[i].zName, pTab->aCol[j].zName)==0 ) break;
    }
    if( j>=pTab->nCol ){
      sqlite3ErrorMsg(pParse, "table %s has no column named %s",
        pTab->zName, pList->a[i].zName);
      sqliteFree(pIndex);
      goto exit_create_index;
    }
    pIndex->aiColumn[i] = j;
    pIndex->keyInfo.aColl[i] = pTab->aCol[j].pColl;
  }
  pIndex->keyInfo.nField = pList->nId;

  /* Link the new Index structure to its table and to the other
  ** in-memory database structures. 
  */
  if( !pParse->explain ){
    Index *p;
    p = sqlite3HashInsert(&db->aDb[pIndex->iDb].idxHash, 
                         pIndex->zName, strlen(pIndex->zName)+1, pIndex);
    if( p ){
      assert( p==pIndex );  /* Malloc must have failed */
      sqliteFree(pIndex);
      goto exit_create_index;
    }
    db->flags |= SQLITE_InternChanges;
  }

  /* When adding an index to the list of indices for a table, make
  ** sure all indices labeled OE_Replace come after all those labeled
  ** OE_Ignore.  This is necessary for the correct operation of UPDATE
  ** and INSERT.
  */
  if( onError!=OE_Replace || pTab->pIndex==0
       || pTab->pIndex->onError==OE_Replace){
    pIndex->pNext = pTab->pIndex;
    pTab->pIndex = pIndex;
  }else{
    Index *pOther = pTab->pIndex;
    while( pOther->pNext && pOther->pNext->onError!=OE_Replace ){
      pOther = pOther->pNext;
    }
    pIndex->pNext = pOther->pNext;
    pOther->pNext = pIndex;
  }

  /* If the db->init.busy is 1 it means we are reading the SQL off the
  ** "sqlite_master" table on the disk.  So do not write to the disk
  ** again.  Extract the table number from the db->init.newTnum field.
  */
  if( db->init.busy && pTblName!=0 ){
    pIndex->tnum = db->init.newTnum;
  }

  /* If the db->init.busy is 0 then create the index on disk.  This
  ** involves writing the index into the master table and filling in the
  ** index with the current table contents.
  **
  ** The db->init.busy is 0 when the user first enters a CREATE INDEX 
  ** command.  db->init.busy is 1 when a database is opened and 
  ** CREATE INDEX statements are read out of the master table.  In
  ** the latter case the index already exists on disk, which is why
  ** we don't want to recreate it.
  **
  ** If pTblName==0 it means this index is generated as a primary key
  ** or UNIQUE constraint of a CREATE TABLE statement.  Since the table
  ** has just been created, it contains no data and the index initialization
  ** step can be skipped.
  */
  else if( db->init.busy==0 ){
    int n;
    Vdbe *v;
    int lbl1, lbl2;
    int i;

    v = sqlite3GetVdbe(pParse);
    if( v==0 ) goto exit_create_index;
    if( pTblName!=0 ){
      sqlite3BeginWriteOperation(pParse, 0, iDb);
      sqlite3OpenMasterTable(v, iDb);
    }
    sqlite3VdbeAddOp(v, OP_NewRecno, 0, 0);
    sqlite3VdbeOp3(v, OP_String, 0, 0, "index", P3_STATIC);
    sqlite3VdbeOp3(v, OP_String, 0, 0, pIndex->zName, 0);
    sqlite3VdbeOp3(v, OP_String, 0, 0, pTab->zName, 0);
    sqlite3VdbeOp3(v, OP_CreateIndex, 0, iDb,(char*)&pIndex->tnum,P3_POINTER);
    pIndex->tnum = 0;
    if( pTblName ){
      sqlite3VdbeCode(v,
          OP_Dup,       0,      0,
          OP_Integer,   iDb,    0,
      0);
      sqlite3VdbeOp3(v, OP_OpenWrite, 1, 0,
                     (char*)&pIndex->keyInfo, P3_KEYINFO);
    }
    sqlite3VdbeAddOp(v, OP_String, 0, 0);
    if( pStart && pEnd ){
      sqlite3VdbeChangeP3(v, -1, "CREATE INDEX ", n);
      sqlite3VdbeAddOp(v, OP_String, 0, 0);
      n = Addr(pEnd->z) - Addr(pName->z) + 1;
      sqlite3VdbeChangeP3(v, -1, pName->z, n);
      sqlite3VdbeAddOp(v, OP_Concat, 2, 0);
    }
    sqlite3VdbeOp3(v, OP_MakeRecord, 5, 0, "tttit", P3_STATIC);
    sqlite3VdbeAddOp(v, OP_PutIntKey, 0, 0);
    if( pTblName ){
      sqlite3VdbeAddOp(v, OP_Integer, pTab->iDb, 0);
      sqlite3VdbeAddOp(v, OP_OpenRead, 2, pTab->tnum);
      /* VdbeComment((v, "%s", pTab->zName)); */
      sqlite3VdbeAddOp(v, OP_SetNumColumns, 2, pTab->nCol);
      lbl2 = sqlite3VdbeMakeLabel(v);
      sqlite3VdbeAddOp(v, OP_Rewind, 2, lbl2);
      lbl1 = sqlite3VdbeAddOp(v, OP_Recno, 2, 0);
      for(i=0; i<pIndex->nColumn; i++){
        int iCol = pIndex->aiColumn[i];
        if( pTab->iPKey==iCol ){
          sqlite3VdbeAddOp(v, OP_Dup, i, 0);
        }else{
          sqlite3VdbeAddOp(v, OP_Column, 2, iCol);
        }
      }
      sqlite3VdbeAddOp(v, OP_MakeIdxKey, pIndex->nColumn, 0);
      sqlite3IndexAffinityStr(v, pIndex);
      sqlite3VdbeOp3(v, OP_IdxPut, 1, pIndex->onError!=OE_None,
                      "indexed columns are not unique", P3_STATIC);
      sqlite3VdbeAddOp(v, OP_Next, 2, lbl1);
      sqlite3VdbeResolveLabel(v, lbl2);
      sqlite3VdbeAddOp(v, OP_Close, 2, 0);
      sqlite3VdbeAddOp(v, OP_Close, 1, 0);
    }
    if( pTblName!=0 ){
      if( !isTemp ){
        sqlite3ChangeCookie(db, v, iDb);
      }
      sqlite3VdbeAddOp(v, OP_Close, 0, 0);
      sqlite3EndWriteOperation(pParse);
    }
  }

  /* Clean up before exiting */
exit_create_index:
  sqlite3IdListDelete(pList);
  /* sqlite3SrcListDelete(pTable); */
  sqliteFree(zName);
  return;
}

/*
** This routine will drop an existing named index.  This routine
** implements the DROP INDEX statement.
*/
void sqlite3DropIndex(Parse *pParse, SrcList *pName){
  Index *pIndex;
  Vdbe *v;
  sqlite *db = pParse->db;

  if( pParse->nErr || sqlite3_malloc_failed ) return;
  assert( pName->nSrc==1 );
  pIndex = sqlite3FindIndex(db, pName->a[0].zName, pName->a[0].zDatabase);
  if( pIndex==0 ){
    sqlite3ErrorMsg(pParse, "no such index: %S", pName, 0);
    goto exit_drop_index;
  }
  if( pIndex->autoIndex ){
    sqlite3ErrorMsg(pParse, "index associated with UNIQUE "
      "or PRIMARY KEY constraint cannot be dropped", 0);
    goto exit_drop_index;
  }
  if( pIndex->iDb>1 ){
    sqlite3ErrorMsg(pParse, "cannot alter schema of attached "
       "databases", 0);
    goto exit_drop_index;
  }
#ifndef SQLITE_OMIT_AUTHORIZATION
  {
    int code = SQLITE_DROP_INDEX;
    Table *pTab = pIndex->pTable;
    const char *zDb = db->aDb[pIndex->iDb].zName;
    const char *zTab = SCHEMA_TABLE(pIndex->iDb);
    if( sqlite3AuthCheck(pParse, SQLITE_DELETE, zTab, 0, zDb) ){
      goto exit_drop_index;
    }
    if( pIndex->iDb ) code = SQLITE_DROP_TEMP_INDEX;
    if( sqlite3AuthCheck(pParse, code, pIndex->zName, pTab->zName, zDb) ){
      goto exit_drop_index;
    }
  }
#endif

  /* Generate code to remove the index and from the master table */
  v = sqlite3GetVdbe(pParse);
  if( v ){
    static VdbeOpList dropIndex[] = {
      { OP_Rewind,     0, ADDR(9), 0}, 
      { OP_String,     0, 0,       0}, /* 1 */
      { OP_MemStore,   1, 1,       0},
      { OP_MemLoad,    1, 0,       0}, /* 3 */
      { OP_Column,     0, 1,       0},
      { OP_Eq,         0, ADDR(8), 0},
      { OP_Next,       0, ADDR(3), 0},
      { OP_Goto,       0, ADDR(9), 0},
      { OP_Delete,     0, 0,       0}, /* 8 */
    };
    int base;

    sqlite3BeginWriteOperation(pParse, 0, pIndex->iDb);
    sqlite3OpenMasterTable(v, pIndex->iDb);
    base = sqlite3VdbeAddOpList(v, ArraySize(dropIndex), dropIndex);
    sqlite3VdbeChangeP3(v, base+1, pIndex->zName, 0);
    if( pIndex->iDb!=1 ){
      sqlite3ChangeCookie(db, v, pIndex->iDb);
    }
    sqlite3VdbeAddOp(v, OP_Close, 0, 0);
    sqlite3VdbeAddOp(v, OP_Destroy, pIndex->tnum, pIndex->iDb);
    sqlite3EndWriteOperation(pParse);
  }

  /* Delete the in-memory description of this index.
  */
  if( !pParse->explain ){
    sqlite3UnlinkAndDeleteIndex(db, pIndex);
    db->flags |= SQLITE_InternChanges;
  }

exit_drop_index:
  sqlite3SrcListDelete(pName);
}

/*
** Append a new element to the given IdList.  Create a new IdList if
** need be.
**
** A new IdList is returned, or NULL if malloc() fails.
*/
IdList *sqlite3IdListAppend(IdList *pList, Token *pToken){
  if( pList==0 ){
    pList = sqliteMalloc( sizeof(IdList) );
    if( pList==0 ) return 0;
    pList->nAlloc = 0;
  }
  if( pList->nId>=pList->nAlloc ){
    struct IdList_item *a;
    pList->nAlloc = pList->nAlloc*2 + 5;
    a = sqliteRealloc(pList->a, pList->nAlloc*sizeof(pList->a[0]) );
    if( a==0 ){
      sqlite3IdListDelete(pList);
      return 0;
    }
    pList->a = a;
  }
  memset(&pList->a[pList->nId], 0, sizeof(pList->a[0]));
  if( pToken ){
    char **pz = &pList->a[pList->nId].zName;
    sqlite3SetNString(pz, pToken->z, pToken->n, 0);
    if( *pz==0 ){
      sqlite3IdListDelete(pList);
      return 0;
    }else{
      sqlite3Dequote(*pz);
    }
  }
  pList->nId++;
  return pList;
}

/*
** Append a new table name to the given SrcList.  Create a new SrcList if
** need be.  A new entry is created in the SrcList even if pToken is NULL.
**
** A new SrcList is returned, or NULL if malloc() fails.
**
** If pDatabase is not null, it means that the table has an optional
** database name prefix.  Like this:  "database.table".  The pDatabase
** points to the table name and the pTable points to the database name.
** The SrcList.a[].zName field is filled with the table name which might
** come from pTable (if pDatabase is NULL) or from pDatabase.  
** SrcList.a[].zDatabase is filled with the database name from pTable,
** or with NULL if no database is specified.
**
** In other words, if call like this:
**
**         sqlite3SrcListAppend(A,B,0);
**
** Then B is a table name and the database name is unspecified.  If called
** like this:
**
**         sqlite3SrcListAppend(A,B,C);
**
** Then C is the table name and B is the database name.
*/
SrcList *sqlite3SrcListAppend(SrcList *pList, Token *pTable, Token *pDatabase){
  if( pList==0 ){
    pList = sqliteMalloc( sizeof(SrcList) );
    if( pList==0 ) return 0;
    pList->nAlloc = 1;
  }
  if( pList->nSrc>=pList->nAlloc ){
    SrcList *pNew;
    pList->nAlloc *= 2;
    pNew = sqliteRealloc(pList,
               sizeof(*pList) + (pList->nAlloc-1)*sizeof(pList->a[0]) );
    if( pNew==0 ){
      sqlite3SrcListDelete(pList);
      return 0;
    }
    pList = pNew;
  }
  memset(&pList->a[pList->nSrc], 0, sizeof(pList->a[0]));
  if( pDatabase && pDatabase->z==0 ){
    pDatabase = 0;
  }
  if( pDatabase && pTable ){
    Token *pTemp = pDatabase;
    pDatabase = pTable;
    pTable = pTemp;
  }
  if( pTable ){
    char **pz = &pList->a[pList->nSrc].zName;
    sqlite3SetNString(pz, pTable->z, pTable->n, 0);
    if( *pz==0 ){
      sqlite3SrcListDelete(pList);
      return 0;
    }else{
      sqlite3Dequote(*pz);
    }
  }
  if( pDatabase ){
    char **pz = &pList->a[pList->nSrc].zDatabase;
    sqlite3SetNString(pz, pDatabase->z, pDatabase->n, 0);
    if( *pz==0 ){
      sqlite3SrcListDelete(pList);
      return 0;
    }else{
      sqlite3Dequote(*pz);
    }
  }
  pList->a[pList->nSrc].iCursor = -1;
  pList->nSrc++;
  return pList;
}

/*
** Assign cursors to all tables in a SrcList
*/
void sqlite3SrcListAssignCursors(Parse *pParse, SrcList *pList){
  int i;
  for(i=0; i<pList->nSrc; i++){
    if( pList->a[i].iCursor<0 ){
      pList->a[i].iCursor = pParse->nTab++;
    }
  }
}

/*
** Add an alias to the last identifier on the given identifier list.
*/
void sqlite3SrcListAddAlias(SrcList *pList, Token *pToken){
  if( pList && pList->nSrc>0 ){
    int i = pList->nSrc - 1;
    sqlite3SetNString(&pList->a[i].zAlias, pToken->z, pToken->n, 0);
    sqlite3Dequote(pList->a[i].zAlias);
  }
}

/*
** Delete an IdList.
*/
void sqlite3IdListDelete(IdList *pList){
  int i;
  if( pList==0 ) return;
  for(i=0; i<pList->nId; i++){
    sqliteFree(pList->a[i].zName);
  }
  sqliteFree(pList->a);
  sqliteFree(pList);
}

/*
** Return the index in pList of the identifier named zId.  Return -1
** if not found.
*/
int sqlite3IdListIndex(IdList *pList, const char *zName){
  int i;
  if( pList==0 ) return -1;
  for(i=0; i<pList->nId; i++){
    if( sqlite3StrICmp(pList->a[i].zName, zName)==0 ) return i;
  }
  return -1;
}

/*
** Delete an entire SrcList including all its substructure.
*/
void sqlite3SrcListDelete(SrcList *pList){
  int i;
  if( pList==0 ) return;
  for(i=0; i<pList->nSrc; i++){
    sqliteFree(pList->a[i].zDatabase);
    sqliteFree(pList->a[i].zName);
    sqliteFree(pList->a[i].zAlias);
    if( pList->a[i].pTab && pList->a[i].pTab->isTransient ){
      sqlite3DeleteTable(0, pList->a[i].pTab);
    }
    sqlite3SelectDelete(pList->a[i].pSelect);
    sqlite3ExprDelete(pList->a[i].pOn);
    sqlite3IdListDelete(pList->a[i].pUsing);
  }
  sqliteFree(pList);
}

/*
** Begin a transaction
*/
void sqlite3BeginTransaction(Parse *pParse, int onError){
  sqlite *db;

  if( pParse==0 || (db=pParse->db)==0 || db->aDb[0].pBt==0 ) return;
  if( pParse->nErr || sqlite3_malloc_failed ) return;
  if( sqlite3AuthCheck(pParse, SQLITE_TRANSACTION, "BEGIN", 0, 0) ) return;
  if( db->flags & SQLITE_InTrans ){
    sqlite3ErrorMsg(pParse, "cannot start a transaction within a transaction");
    return;
  }
  sqlite3BeginWriteOperation(pParse, 0, 0);
  if( !pParse->explain ){
    db->flags |= SQLITE_InTrans;
    db->onError = onError;
  }
}

/*
** Commit a transaction
*/
void sqlite3CommitTransaction(Parse *pParse){
  sqlite *db;

  if( pParse==0 || (db=pParse->db)==0 || db->aDb[0].pBt==0 ) return;
  if( pParse->nErr || sqlite3_malloc_failed ) return;
  if( sqlite3AuthCheck(pParse, SQLITE_TRANSACTION, "COMMIT", 0, 0) ) return;
  if( (db->flags & SQLITE_InTrans)==0 ){
    sqlite3ErrorMsg(pParse, "cannot commit - no transaction is active");
    return;
  }
  if( !pParse->explain ){
    db->flags &= ~SQLITE_InTrans;
  }
  sqlite3EndWriteOperation(pParse);
  if( !pParse->explain ){
    db->onError = OE_Default;
  }
}

/*
** Rollback a transaction
*/
void sqlite3RollbackTransaction(Parse *pParse){
  sqlite *db;
  Vdbe *v;

  if( pParse==0 || (db=pParse->db)==0 || db->aDb[0].pBt==0 ) return;
  if( pParse->nErr || sqlite3_malloc_failed ) return;
  if( sqlite3AuthCheck(pParse, SQLITE_TRANSACTION, "ROLLBACK", 0, 0) ) return;
  if( (db->flags & SQLITE_InTrans)==0 ){
    sqlite3ErrorMsg(pParse, "cannot rollback - no transaction is active");
    return; 
  }
  v = sqlite3GetVdbe(pParse);
  if( v ){
    sqlite3VdbeAddOp(v, OP_Rollback, 0, 0);
  }
  if( !pParse->explain ){
    db->flags &= ~SQLITE_InTrans;
    db->onError = OE_Default;
  }
}

/*
** Generate VDBE code that will verify the schema cookie for all
** named database files.
*/
void sqlite3CodeVerifySchema(Parse *pParse, int iDb){
  sqlite *db = pParse->db;
  Vdbe *v = sqlite3GetVdbe(pParse);
  assert( iDb>=0 && iDb<db->nDb );
  assert( db->aDb[iDb].pBt!=0 );
  if( iDb!=1 && !DbHasProperty(db, iDb, DB_Cookie) ){
    sqlite3VdbeAddOp(v, OP_VerifyCookie, iDb, db->aDb[iDb].schema_cookie);
    DbSetProperty(db, iDb, DB_Cookie);
  }
}

/*
** Generate VDBE code that prepares for doing an operation that
** might change the database.
**
** This routine starts a new transaction if we are not already within
** a transaction.  If we are already within a transaction, then a checkpoint
** is set if the setStatement parameter is true.  A checkpoint should
** be set for operations that might fail (due to a constraint) part of
** the way through and which will need to undo some writes without having to
** rollback the whole transaction.  For operations where all constraints
** can be checked before any changes are made to the database, it is never
** necessary to undo a write and the checkpoint should not be set.
**
** Only database iDb and the temp database are made writable by this call.
** If iDb==0, then the main and temp databases are made writable.   If
** iDb==1 then only the temp database is made writable.  If iDb>1 then the
** specified auxiliary database and the temp database are made writable.
*/
void sqlite3BeginWriteOperation(Parse *pParse, int setStatement, int iDb){
  Vdbe *v;
  sqlite *db = pParse->db;
  if( DbHasProperty(db, iDb, DB_Locked) ) return;
  v = sqlite3GetVdbe(pParse);
  if( v==0 ) return;
  if( !db->aDb[iDb].inTrans ){
    sqlite3VdbeAddOp(v, OP_Transaction, iDb, 0);
    DbSetProperty(db, iDb, DB_Locked);
    sqlite3CodeVerifySchema(pParse, iDb);
    if( iDb!=1 ){
      sqlite3BeginWriteOperation(pParse, setStatement, 1);
    }
  }else if( setStatement ){
    sqlite3VdbeAddOp(v, OP_Statement, iDb, 0);
    DbSetProperty(db, iDb, DB_Locked);
  }
}

/*
** Generate code that concludes an operation that may have changed
** the database.  If a statement transaction was started, then emit
** an OP_Commit that will cause the changes to be committed to disk.
**
** Note that checkpoints are automatically committed at the end of
** a statement.  Note also that there can be multiple calls to 
** sqlite3BeginWriteOperation() but there should only be a single
** call to sqlite3EndWriteOperation() at the conclusion of the statement.
*/
void sqlite3EndWriteOperation(Parse *pParse){
  Vdbe *v;
  sqlite *db = pParse->db;
  if( pParse->trigStack ) return; /* if this is in a trigger */
  v = sqlite3GetVdbe(pParse);
  if( v==0 ) return;
  if( db->flags & SQLITE_InTrans ){
    /* A BEGIN has executed.  Do not commit until we see an explicit
    ** COMMIT statement. */
  }else{
    sqlite3VdbeAddOp(v, OP_Commit, 0, 0);
  }
}

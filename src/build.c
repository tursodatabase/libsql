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
**     creating expressions and ID lists
**     COPY
**     VACUUM
**     BEGIN TRANSACTION
**     COMMIT
**     ROLLBACK
**     PRAGMA
**
** $Id: build.c,v 1.62 2002/01/09 03:20:00 drh Exp $
*/
#include "sqliteInt.h"
#include <ctype.h>

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
  if( pParse->pVdbe && pParse->nErr==0 ){
    if( pParse->explain ){
      rc = sqliteVdbeList(pParse->pVdbe, pParse->xCallback, pParse->pArg, 
                          &pParse->zErrMsg);
    }else{
      FILE *trace = (db->flags & SQLITE_VdbeTrace)!=0 ? stdout : 0;
      sqliteVdbeTrace(pParse->pVdbe, trace);
      rc = sqliteVdbeExec(pParse->pVdbe, pParse->xCallback, pParse->pArg, 
                          &pParse->zErrMsg, db->pBusyArg,
                          db->xBusyCallback);
      if( rc ) pParse->nErr++;
    }
    sqliteVdbeDelete(pParse->pVdbe);
    pParse->pVdbe = 0;
    pParse->colNamesSet = 0;
    pParse->rc = rc;
    pParse->schemaVerified = 0;
  }
}

/*
** Construct a new expression node and return a pointer to it.  Memory
** for this node is obtained from sqliteMalloc().  The calling function
** is responsible for making sure the node eventually gets freed.
*/
Expr *sqliteExpr(int op, Expr *pLeft, Expr *pRight, Token *pToken){
  Expr *pNew;
  pNew = sqliteMalloc( sizeof(Expr) );
  if( pNew==0 ){
    sqliteExprDelete(pLeft);
    sqliteExprDelete(pRight);
    return 0;
  }
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
    pExpr->span.n = pRight->n + Addr(pRight->z) - Addr(pLeft->z);
  }
}

/*
** Construct a new expression node for a function with multiple
** arguments.
*/
Expr *sqliteExprFunction(ExprList *pList, Token *pToken){
  Expr *pNew;
  pNew = sqliteMalloc( sizeof(Expr) );
  if( pNew==0 ){
    sqliteExprListDelete(pList);
    return 0;
  }
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
** Locate the in-memory structure that describes 
** a particular database table given the name
** of that table.  Return NULL if not found.
*/
Table *sqliteFindTable(sqlite *db, char *zName){
  Table *p = sqliteHashFind(&db->tblHash, zName, strlen(zName)+1);
  return p;
}

/*
** Locate the in-memory structure that describes 
** a particular index given the name of that index.
** Return NULL if not found.
*/
Index *sqliteFindIndex(sqlite *db, char *zName){
  Index *p = sqliteHashFind(&db->idxHash, zName, strlen(zName)+1);
  return p;
}

/*
** Remove the given index from the index hash table, and free
** its memory structures.
**
** The index is removed from the database hash tables if db!=NULL.
** But the index is not unlinked from the Table that it indexes.
** Unlinking from the Table must be done by the calling function.
*/
static void sqliteDeleteIndex(sqlite *db, Index *p){
  if( p->zName && db ){
    Index *pOld;
    pOld = sqliteHashInsert(&db->idxHash, p->zName, strlen(p->zName)+1, 0);
    assert( pOld==0 || pOld==p );
    sqliteHashInsert(&db->idxDrop, p, 0, 0);
  }
  sqliteFree(p);
}

/*
** Unlink the given index from its table, then remove
** the index from the index hash table and free its memory
** structures.
*/
void sqliteUnlinkAndDeleteIndex(sqlite *db, Index *pIndex){
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
** Move the given index to the pending DROP INDEX queue if it has
** been committed.  If this index was never committed, then just
** delete it.
**
** Indices on the pending drop queue are deleted when a COMMIT is
** executed.  If a ROLLBACK occurs, the indices are moved back into
** the main index hash table.
*/
void sqlitePendingDropIndex(sqlite *db, Index *p){
  if( !p->isCommit ){
    sqliteUnlinkAndDeleteIndex(db, p);
  }else{
    Index *pOld;
    pOld = sqliteHashInsert(&db->idxHash, p->zName, strlen(p->zName)+1, 0);
    assert( pOld==p );
    sqliteHashInsert(&db->idxDrop, p, 0, p);
    p->isDropped = 1;
  }
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
    sqliteFree(pTable->aCol[i].zType);
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
** Unlink the given table from the hash tables and the delete the
** table structure with all its indices.
*/
static void sqliteUnlinkAndDeleteTable(sqlite *db, Table *p){
  if( p->zName && db ){
    Table *pOld;
    pOld = sqliteHashInsert(&db->tblHash, p->zName, strlen(p->zName)+1, 0);
    assert( pOld==0 || pOld==p );
    sqliteHashInsert(&db->tblDrop, p, 0, 0);
  }
  sqliteDeleteTable(db, p);
}

/*
** Move the given table to the pending DROP TABLE queue if it has
** been committed.  If this table was never committed, then just
** delete it.  Do the same for all its indices.
**
** Table on the drop queue are not actually deleted until a COMMIT
** statement is executed.  If a ROLLBACK occurs instead of a COMMIT,
** then the tables on the drop queue are moved back into the main
** hash table.
*/
void sqlitePendingDropTable(sqlite *db, Table *pTbl){
  if( !pTbl->isCommit ){
    sqliteUnlinkAndDeleteTable(db, pTbl);
  }else{
    Table *pOld;
    Index *pIndex, *pNext;
    pOld = sqliteHashInsert(&db->tblHash, pTbl->zName, strlen(pTbl->zName)+1,0);
    assert( pOld==pTbl );
    sqliteHashInsert(&db->tblDrop, pTbl, 0, pTbl);
    for(pIndex = pTbl->pIndex; pIndex; pIndex=pNext){
      pNext = pIndex->pNext;
      sqlitePendingDropIndex(db, pIndex);
    }
  }
}

/*
** Check all Tables and Indexes in the internal hash table and commit
** any additions or deletions to those hash tables.
**
** When executing CREATE TABLE and CREATE INDEX statements, the Table
** and Index structures are created and added to the hash tables, but
** the "isCommit" field is not set.  This routine sets those fields.
** When executing DROP TABLE and DROP INDEX, the table or index structures
** are moved out of tblHash and idxHash into tblDrop and idxDrop.  This
** routine deletes the structure in tblDrop and idxDrop.
**
** See also: sqliteRollbackInternalChanges()
*/
void sqliteCommitInternalChanges(sqlite *db){
  HashElem *pElem;
  if( (db->flags & SQLITE_InternChanges)==0 ) return;
  db->schema_cookie = db->next_cookie;
  for(pElem=sqliteHashFirst(&db->tblHash); pElem; pElem=sqliteHashNext(pElem)){
    Table *pTable = sqliteHashData(pElem);
    pTable->isCommit = 1;
  }
  for(pElem=sqliteHashFirst(&db->tblDrop); pElem; pElem=sqliteHashNext(pElem)){
    Table *pTable = sqliteHashData(pElem);
    sqliteDeleteTable(db, pTable);
  }
  sqliteHashClear(&db->tblDrop);
  for(pElem=sqliteHashFirst(&db->idxHash); pElem; pElem=sqliteHashNext(pElem)){
    Index *pIndex = sqliteHashData(pElem);
    pIndex->isCommit = 1;
  }
  while( (pElem=sqliteHashFirst(&db->idxDrop))!=0 ){
    Index *pIndex = sqliteHashData(pElem);
    sqliteUnlinkAndDeleteIndex(db, pIndex);
  }
  sqliteHashClear(&db->idxDrop);
  db->flags &= ~SQLITE_InternChanges;
}

/*
** This routine runs when one or more CREATE TABLE, CREATE INDEX,
** DROP TABLE, or DROP INDEX statements gets rolled back.  The
** additions or deletions of Table and Index structures in the
** internal hash tables are undone.
**
** See also: sqliteCommitInternalChanges()
*/
void sqliteRollbackInternalChanges(sqlite *db){
  Hash toDelete;
  HashElem *pElem;
  if( (db->flags & SQLITE_InternChanges)==0 ) return;
  sqliteHashInit(&toDelete, SQLITE_HASH_POINTER, 0);
  db->next_cookie = db->schema_cookie;
  for(pElem=sqliteHashFirst(&db->tblHash); pElem; pElem=sqliteHashNext(pElem)){
    Table *pTable = sqliteHashData(pElem);
    if( !pTable->isCommit ){
      sqliteHashInsert(&toDelete, pTable, 0, pTable);
    }
  }
  for(pElem=sqliteHashFirst(&toDelete); pElem; pElem=sqliteHashNext(pElem)){
    Table *pTable = sqliteHashData(pElem);
    sqliteUnlinkAndDeleteTable(db, pTable);
  }
  sqliteHashClear(&toDelete);
  for(pElem=sqliteHashFirst(&db->tblDrop); pElem; pElem=sqliteHashNext(pElem)){
    Table *pOld, *p = sqliteHashData(pElem);
    assert( p->isCommit );
    pOld = sqliteHashInsert(&db->tblHash, p->zName, strlen(p->zName)+1, p);
    assert( pOld==0 || pOld==p );
  }
  sqliteHashClear(&db->tblDrop);
  for(pElem=sqliteHashFirst(&db->idxHash); pElem; pElem=sqliteHashNext(pElem)){
    Index *pIndex = sqliteHashData(pElem);
    if( !pIndex->isCommit ){
      sqliteHashInsert(&toDelete, pIndex, 0, pIndex);
    }
  }
  for(pElem=sqliteHashFirst(&toDelete); pElem; pElem=sqliteHashNext(pElem)){
    Index *pIndex = sqliteHashData(pElem);
    sqliteUnlinkAndDeleteIndex(db, pIndex);
  }
  sqliteHashClear(&toDelete);
  for(pElem=sqliteHashFirst(&db->idxDrop); pElem; pElem=sqliteHashNext(pElem)){
    Index *pOld, *p = sqliteHashData(pElem);
    assert( p->isCommit );
    p->isDropped = 0;
    pOld = sqliteHashInsert(&db->idxHash, p->zName, strlen(p->zName)+1, p);
    assert( pOld==0 || pOld==p );
  }
  sqliteHashClear(&db->idxDrop);
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
** pStart token is the CREATE and pName is the table name.  The isTemp
** flag is true if the "TEMP" or "TEMPORARY" keyword occurs in between
** CREATE and TABLE.
**
** The new table record is initialized and put in pParse->pNewTable.
** As more of the CREATE TABLE statement is parsed, additional action
** routines will be called to add more information to this record.
** At the end of the CREATE TABLE statement, the sqliteEndTable() routine
** is called to complete the construction of the new table record.
*/
void sqliteStartTable(Parse *pParse, Token *pStart, Token *pName, int isTemp){
  Table *pTable;
  Index *pIdx;
  char *zName;
  sqlite *db = pParse->db;
  Vdbe *v;

  pParse->sFirstToken = *pStart;
  zName = sqliteTableNameFromToken(pName);
  if( zName==0 ) return;

  /* Before trying to create a temporary table, make sure the Btree for
  ** holding temporary tables is open.
  */
  if( isTemp && db->pBeTemp==0 ){
    int rc = sqliteBtreeOpen(0, 0, MAX_PAGES, &db->pBeTemp);
    if( rc!=SQLITE_OK ){
      sqliteSetNString(&pParse->zErrMsg, "unable to open a temporary database "
        "file for storing temporary tables", 0);
      pParse->nErr++;
      return;
    }
    if( db->flags & SQLITE_InTrans ){
      rc = sqliteBtreeBeginTrans(db->pBeTemp);
      if( rc!=SQLITE_OK ){
        sqliteSetNString(&pParse->zErrMsg, "unable to get a write lock on "
          "the temporary datbase file", 0);
        pParse->nErr++;
        return;
      }
    }
  }

  /* Make sure the new table name does not collide with an existing
  ** index or table name.  Issue an error message if it does.
  **
  ** If we are re-reading the sqlite_master table because of a schema
  ** change and a new permanent table is found whose name collides with
  ** an existing temporary table, then ignore the new permanent table.
  ** We will continue parsing, but the pParse->nameClash flag will be set
  ** so we will know to discard the table record once parsing has finished.
  */
  pTable = sqliteFindTable(db, zName);
  if( pTable!=0 ){
    if( pTable->isTemp && pParse->initFlag ){
      pParse->nameClash = 1;
    }else{
      sqliteSetNString(&pParse->zErrMsg, "table ", 0, pName->z, pName->n,
          " already exists", 0, 0);
      sqliteFree(zName);
      pParse->nErr++;
      return;
    }
  }else{
    pParse->nameClash = 0;
  }
  if( (pIdx = sqliteFindIndex(db, zName))!=0 &&
          (!pIdx->pTable->isTemp || !pParse->initFlag) ){
    sqliteSetString(&pParse->zErrMsg, "there is already an index named ", 
       zName, 0);
    sqliteFree(zName);
    pParse->nErr++;
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
  pTable->isTemp = isTemp;
  if( pParse->pNewTable ) sqliteDeleteTable(db, pParse->pNewTable);
  pParse->pNewTable = pTable;
  if( !pParse->initFlag && (v = sqliteGetVdbe(pParse))!=0 ){
    if( (db->flags & SQLITE_InTrans)==0 ){
      sqliteVdbeAddOp(v, OP_Transaction, 0, 0);
      sqliteVdbeAddOp(v, OP_VerifyCookie, db->schema_cookie, 0);
      pParse->schemaVerified = 1;
    }
    if( !isTemp ){
      sqliteVdbeAddOp(v, OP_SetCookie, db->file_format, 1);
      sqliteVdbeAddOp(v, OP_OpenWrite, 0, 2);
      sqliteVdbeChangeP3(v, -1, MASTER_NAME, P3_STATIC);
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
    Column *aNew;
    aNew = sqliteRealloc( p->aCol, (p->nCol+8)*sizeof(p->aCol[0]));
    if( aNew==0 ) return;
    p->aCol = aNew;
  }
  memset(&p->aCol[p->nCol], 0, sizeof(p->aCol[0]));
  pz = &p->aCol[p->nCol++].zName;
  sqliteSetNString(pz, pName->z, pName->n, 0);
  sqliteDequote(*pz);
}

/*
** This routine is called by the parser while in the middle of
** parsing a CREATE TABLE statement.  A "NOT NULL" constraint has
** been seen on a column.  This routine sets the notNull flag on
** the column currently under construction.
*/
void sqliteAddNotNull(Parse *pParse){
  Table *p;
  int i;
  if( (p = pParse->pNewTable)==0 ) return;
  i = p->nCol-1;
  if( i>=0 ) p->aCol[i].notNull = 1;
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
void sqliteAddColumnType(Parse *pParse, Token *pFirst, Token *pLast){
  Table *p;
  int i, j;
  int n;
  char *z, **pz;
  if( (p = pParse->pNewTable)==0 ) return;
  i = p->nCol-1;
  if( i<0 ) return;
  pz = &p->aCol[i].zType;
  n = pLast->n + Addr(pLast->z) - Addr(pFirst->z);
  sqliteSetNString(pz, pFirst->z, n, 0);
  z = *pz;
  if( z==0 ) return;
  for(i=j=0; z[i]; i++){
    int c = z[i];
    if( isspace(c) ) continue;
    z[j++] = c;
  }
  z[j] = 0;
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
  if( i<0 ) return;
  pz = &p->aCol[i].zDflt;
  if( minusFlag ){
    sqliteSetNString(pz, "-", 1, pVal->z, pVal->n, 0);
  }else{
    sqliteSetNString(pz, pVal->z, pVal->n, 0);
  }
  sqliteDequote(*pz);
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
void sqliteAddPrimaryKey(Parse *pParse, IdList *pList){
  Table *pTab = pParse->pNewTable;
  char *zType = 0;
  int iCol = -1;
  if( pTab==0 ) return;
  if( pTab->hasPrimKey ){
    sqliteSetString(&pParse->zErrMsg, "table \"", pTab->zName, 
        "\" has more than one primary key", 0);
    pParse->nErr++;
    return;
  }
  pTab->hasPrimKey = 1;
  if( pList==0 ){
    iCol = pTab->nCol - 1;
  }else if( pList->nId==1 ){
    for(iCol=0; iCol<pTab->nCol; iCol++){
      if( sqliteStrICmp(pList->a[0].zName, pTab->aCol[iCol].zName)==0 ) break;
    }
  }
  if( iCol>=0 && iCol<pTab->nCol ){
    zType = pTab->aCol[iCol].zType;
  }
  if( pParse->db->file_format>=1 && 
           zType && sqliteStrICmp(zType, "INTEGER")==0 ){
    pTab->iPKey = iCol;
  }else{
    sqliteCreateIndex(pParse, 0, 0, pList, 1, 0, 0);
  }
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
static void changeCookie(sqlite *db){
  if( db->next_cookie==db->schema_cookie ){
    db->next_cookie = db->schema_cookie + sqliteRandomByte() + 1;
    db->flags |= SQLITE_InternChanges;
  }
}

/*
** This routine is called to report the final ")" that terminates
** a CREATE TABLE statement.
**
** The table structure that other action routines have been building
** is added to the internal hash tables, assuming no errors have
** occurred.
**
** An entry for the table is made in the master table on disk,
** unless this is a temporary table or initFlag==1.  When initFlag==1,
** it means we are reading the sqlite_master table because we just
** connected to the database or because the sqlite_master table has
** recently changes, so the entry for this table already exists in
** the sqlite_master table.  We do not want to create it again.
*/
void sqliteEndTable(Parse *pParse, Token *pEnd){
  Table *p;
  sqlite *db = pParse->db;

  if( pEnd==0 || pParse->nErr || sqlite_malloc_failed ) return;
  p = pParse->pNewTable;
  if( p==0 ) return;

  /* Add the table to the in-memory representation of the database.
  */
  assert( pParse->nameClash==0 || pParse->initFlag==1 );
  if( pParse->explain==0 && pParse->nameClash==0 ){
    Table *pOld;
    pOld = sqliteHashInsert(&db->tblHash, p->zName, strlen(p->zName)+1, p);
    if( pOld ){
      assert( p==pOld );  /* Malloc must have failed inside HashInsert() */
      return;
    }
    pParse->pNewTable = 0;
    db->nTable++;
    db->flags |= SQLITE_InternChanges;
  }

  /* If the initFlag is 1 it means we are reading the SQL off the
  ** "sqlite_master" table on the disk.  So do not write to the disk
  ** again.  Extract the root page number for the table from the 
  ** pParse->newTnum field.  (The page number should have been put
  ** there by the sqliteOpenCb routine.)
  */
  if( pParse->initFlag ){
    p->tnum = pParse->newTnum;
  }

  /* If not initializing, then create a record for the new table
  ** in the SQLITE_MASTER table of the database.
  **
  ** If this is a TEMPORARY table, then just create the table.  Do not
  ** make an entry in SQLITE_MASTER.
  */
  if( !pParse->initFlag ){
    int n, addr;
    Vdbe *v;

    v = sqliteGetVdbe(pParse);
    if( v==0 ) return;
    n = Addr(pEnd->z) - Addr(pParse->sFirstToken.z) + 1;
    if( !p->isTemp ){
      sqliteVdbeAddOp(v, OP_NewRecno, 0, 0);
      sqliteVdbeAddOp(v, OP_String, 0, 0);
      sqliteVdbeChangeP3(v, -1, "table", P3_STATIC);
      sqliteVdbeAddOp(v, OP_String, 0, 0);
      sqliteVdbeChangeP3(v, -1, p->zName, P3_STATIC);
      sqliteVdbeAddOp(v, OP_String, 0, 0);
      sqliteVdbeChangeP3(v, -1, p->zName, P3_STATIC);
    }
    addr = sqliteVdbeAddOp(v, OP_CreateTable, 0, p->isTemp);
    sqliteVdbeChangeP3(v, addr, (char *)&p->tnum, P3_POINTER);
    p->tnum = 0;
    if( !p->isTemp ){
      addr = sqliteVdbeAddOp(v, OP_String, 0, 0);
      sqliteVdbeChangeP3(v, addr, pParse->sFirstToken.z, n);
      sqliteVdbeAddOp(v, OP_MakeRecord, 5, 0);
      sqliteVdbeAddOp(v, OP_Put, 0, 0);
      changeCookie(db);
      sqliteVdbeAddOp(v, OP_SetCookie, db->next_cookie, 0);
      sqliteVdbeAddOp(v, OP_Close, 0, 0);
    }
    if( (db->flags & SQLITE_InTrans)==0 ){
      sqliteVdbeAddOp(v, OP_Commit, 0, 0);
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
  sqlite *db = pParse->db;

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
      { OP_OpenWrite,  0, 2,        MASTER_NAME},
      { OP_Rewind,     0, ADDR(9),  0},
      { OP_String,     0, 0,        0}, /* 2 */
      { OP_MemStore,   1, 1,        0},
      { OP_MemLoad,    1, 0,        0}, /* 4 */
      { OP_Column,     0, 2,        0},
      { OP_Ne,         0, ADDR(8),  0},
      { OP_Delete,     0, 0,        0},
      { OP_Next,       0, ADDR(4),  0}, /* 8 */
      { OP_SetCookie,  0, 0,        0}, /* 9 */
      { OP_Close,      0, 0,        0},
    };
    Index *pIdx;
    if( (db->flags & SQLITE_InTrans)==0 ){
      sqliteVdbeAddOp(v, OP_Transaction, 0, 0);
      sqliteVdbeAddOp(v, OP_VerifyCookie, db->schema_cookie, 0);
      pParse->schemaVerified = 1;
    }
    if( !pTable->isTemp ){
      base = sqliteVdbeAddOpList(v, ArraySize(dropTable), dropTable);
      sqliteVdbeChangeP3(v, base+2, pTable->zName, P3_STATIC);
      changeCookie(db);
      sqliteVdbeChangeP1(v, base+9, db->next_cookie);
    }
    sqliteVdbeAddOp(v, OP_Destroy, pTable->tnum, pTable->isTemp);
    for(pIdx=pTable->pIndex; pIdx; pIdx=pIdx->pNext){
      sqliteVdbeAddOp(v, OP_Destroy, pIdx->tnum, pTable->isTemp);
    }
    if( (db->flags & SQLITE_InTrans)==0 ){
      sqliteVdbeAddOp(v, OP_Commit, 0, 0);
    }
  }

  /* Move the table (and all its indices) to the pending DROP queue.
  ** Or, if the table was never committed, just delete it.  If the table
  ** has been committed and is placed on the pending DROP queue, then the
  ** delete will occur when sqliteCommitInternalChanges() executes.
  **
  ** Exception: if the SQL statement began with the EXPLAIN keyword,
  ** then no changes should be made.
  */
  if( !pParse->explain ){
    sqlitePendingDropTable(db, pTable);
    db->flags |= SQLITE_InternChanges;
  }
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
void sqliteCreateIndex(
  Parse *pParse,   /* All information about this parse */
  Token *pName,    /* Name of the index.  May be NULL */
  Token *pTable,   /* Name of the table to index.  Use pParse->pNewTable if 0 */
  IdList *pList,   /* A list of columns to be indexed */
  int isUnique,    /* True if all entries in this index must be unique */
  Token *pStart,   /* The CREATE token that begins a CREATE TABLE statement */
  Token *pEnd      /* The ")" that closes the CREATE INDEX statement */
){
  Table *pTab;     /* Table to be indexed */
  Index *pIndex;   /* The index to be created */
  char *zName = 0;
  int i, j;
  Token nullId;             /* Fake token for an empty ID list */
  sqlite *db = pParse->db;
  int hideName = 0;         /* Do not put table name in the hash table */

  if( pParse->nErr || sqlite_malloc_failed ) goto exit_create_index;

  /*
  ** Find the table that is to be indexed.  Return early if not found.
  */
  if( pTable!=0 ){
    assert( pName!=0 );
    pTab =  sqliteTableFromToken(pParse, pTable);
  }else{
    assert( pName==0 );
    pTab =  pParse->pNewTable;
  }
  if( pTab==0 || pParse->nErr ) goto exit_create_index;
  if( pTab->readOnly ){
    sqliteSetString(&pParse->zErrMsg, "table ", pTab->zName, 
      " may not have new indices added", 0);
    pParse->nErr++;
    goto exit_create_index;
  }

  /* If this index is created while re-reading the schema from sqlite_master
  ** but the table associated with this index is a temporary table, it can
  ** only mean that the table that this index is really associated with is
  ** one whose name is hidden behind a temporary table with the same name.
  ** Since its table has been suppressed, we need to also suppress the
  ** index.
  */
  if( pParse->initFlag && pTab->isTemp ){
    goto exit_create_index;
  }

  /*
  ** Find the name of the index.  Make sure there is not already another
  ** index or table with the same name.  
  **
  ** Exception:  If we are reading the names of permanent indices from the
  ** sqlite_master table (because some other process changed the schema) and
  ** one of the index names collides with the name of a temporary table or
  ** index, then we will continue to process this index, but we will not
  ** store its name in the hash table.  Set the hideName flag to accomplish
  ** this.
  **
  ** If pName==0 it means that we are
  ** dealing with a primary key or UNIQUE constraint.  We have to invent our
  ** own name.
  */
  if( pName ){
    Index *pISameName;    /* Another index with the same name */
    Table *pTSameName;    /* A table with same name as the index */
    zName = sqliteTableNameFromToken(pName);
    if( zName==0 ) goto exit_create_index;
    if( (pISameName = sqliteFindIndex(db, zName))!=0 ){
      if( pISameName->pTable->isTemp && pParse->initFlag ){
        hideName = 1;
      }else{
        sqliteSetString(&pParse->zErrMsg, "index ", zName, 
           " already exists", 0);
        pParse->nErr++;
        goto exit_create_index;
      }
    }
    if( (pTSameName = sqliteFindTable(db, zName))!=0 ){
      if( pTSameName->isTemp && pParse->initFlag ){
        hideName = 1;
      }else{
        sqliteSetString(&pParse->zErrMsg, "there is already a table named ",
           zName, 0);
        pParse->nErr++;
        goto exit_create_index;
      }
    }
  }else{
    char zBuf[30];
    int n;
    Index *pLoop;
    for(pLoop=pTab->pIndex, n=1; pLoop; pLoop=pLoop->pNext, n++){}
    sprintf(zBuf,"%d)",n);
    zName = 0;
    sqliteSetString(&zName, "(", pTab->zName, " autoindex ", zBuf, 0);
    if( zName==0 ) goto exit_create_index;
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
  pIndex->isUnique = isUnique;

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
  if( !pParse->explain && !hideName ){
    Index *p;
    p = sqliteHashInsert(&db->idxHash, pIndex->zName, strlen(zName)+1, pIndex);
    if( p ){
      assert( p==pIndex );  /* Malloc must have failed */
      sqliteFree(pIndex);
      goto exit_create_index;
    }
    db->flags |= SQLITE_InternChanges;
  }
  pIndex->pNext = pTab->pIndex;
  pTab->pIndex = pIndex;

  /* If the initFlag is 1 it means we are reading the SQL off the
  ** "sqlite_master" table on the disk.  So do not write to the disk
  ** again.  Extract the table number from the pParse->newTnum field.
  */
  if( pParse->initFlag && pTable!=0 ){
    pIndex->tnum = pParse->newTnum;
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
  **
  ** If pTable==0 it means this index is generated as a primary key
  ** or UNIQUE constraint of a CREATE TABLE statement.  Since the table
  ** has just been created, it contains no data and the index initialization
  ** step can be skipped.
  */
  else if( pParse->initFlag==0 ){
    int n;
    Vdbe *v;
    int lbl1, lbl2;
    int i;
    int addr;
    int isTemp = pTab->isTemp;

    v = sqliteGetVdbe(pParse);
    if( v==0 ) goto exit_create_index;
    if( pTable!=0 ){
      if( (db->flags & SQLITE_InTrans)==0 ){
        sqliteVdbeAddOp(v, OP_Transaction, 0, 0);
        sqliteVdbeAddOp(v, OP_VerifyCookie, db->schema_cookie, 0);
        pParse->schemaVerified = 1;
      }
      if( !isTemp ){
        sqliteVdbeAddOp(v, OP_OpenWrite, 0, 2);
        sqliteVdbeChangeP3(v, -1, MASTER_NAME, P3_STATIC);
      }
    }
    if( !isTemp ){
      sqliteVdbeAddOp(v, OP_NewRecno, 0, 0);
      sqliteVdbeAddOp(v, OP_String, 0, 0);
      sqliteVdbeChangeP3(v, -1, "index", P3_STATIC);
      sqliteVdbeAddOp(v, OP_String, 0, 0);
      sqliteVdbeChangeP3(v, -1, pIndex->zName, P3_STATIC);
      sqliteVdbeAddOp(v, OP_String, 0, 0);
      sqliteVdbeChangeP3(v, -1, pTab->zName, P3_STATIC);
    }
    addr = sqliteVdbeAddOp(v, OP_CreateIndex, 0, isTemp);
    sqliteVdbeChangeP3(v, addr, (char*)&pIndex->tnum, P3_POINTER);
    pIndex->tnum = 0;
    if( pTable ){
      if( isTemp ){
        sqliteVdbeAddOp(v, OP_OpenWrAux, 1, 0);
      }else{
        sqliteVdbeAddOp(v, OP_Dup, 0, 0);
        sqliteVdbeAddOp(v, OP_OpenWrite, 1, 0);
      }
    }
    if( !isTemp ){
      addr = sqliteVdbeAddOp(v, OP_String, 0, 0);
      if( pStart && pEnd ){
        n = Addr(pEnd->z) - Addr(pStart->z) + 1;
        sqliteVdbeChangeP3(v, addr, pStart->z, n);
      }
      sqliteVdbeAddOp(v, OP_MakeRecord, 5, 0);
      sqliteVdbeAddOp(v, OP_Put, 0, 0);
    }
    if( pTable ){
      sqliteVdbeAddOp(v, isTemp ? OP_OpenAux : OP_Open, 2, pTab->tnum);
      sqliteVdbeChangeP3(v, -1, pTab->zName, P3_STATIC);
      lbl2 = sqliteVdbeMakeLabel(v);
      sqliteVdbeAddOp(v, OP_Rewind, 2, lbl2);
      lbl1 = sqliteVdbeAddOp(v, OP_Recno, 2, 0);
      for(i=0; i<pIndex->nColumn; i++){
        sqliteVdbeAddOp(v, OP_Column, 2, pIndex->aiColumn[i]);
      }
      sqliteVdbeAddOp(v, OP_MakeIdxKey, pIndex->nColumn, 0);
      sqliteVdbeAddOp(v, OP_IdxPut, 1, pIndex->isUnique);
      sqliteVdbeAddOp(v, OP_Next, 2, lbl1);
      sqliteVdbeResolveLabel(v, lbl2);
      sqliteVdbeAddOp(v, OP_Close, 2, 0);
      sqliteVdbeAddOp(v, OP_Close, 1, 0);
    }
    if( pTable!=0 ){
      if( !isTemp ){
        changeCookie(db);
        sqliteVdbeAddOp(v, OP_SetCookie, db->next_cookie, 0);
        sqliteVdbeAddOp(v, OP_Close, 0, 0);
      }
      if( (db->flags & SQLITE_InTrans)==0 ){
        sqliteVdbeAddOp(v, OP_Commit, 0, 0);
      }
    }
  }

  /* Clean up before exiting */
exit_create_index:
  sqliteIdListDelete(pList);
  sqliteFree(zName);
  return;
}

/*
** This routine will drop an existing named index.  This routine
** implements the DROP INDEX statement.
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
      { OP_OpenWrite,  0, 2,       MASTER_NAME},
      { OP_Rewind,     0, ADDR(10),0}, 
      { OP_String,     0, 0,       0}, /* 2 */
      { OP_MemStore,   1, 1,       0},
      { OP_MemLoad,    1, 0,       0}, /* 4 */
      { OP_Column,     0, 1,       0},
      { OP_Eq,         0, ADDR(9), 0},
      { OP_Next,       0, ADDR(4), 0},
      { OP_Goto,       0, ADDR(10),0},
      { OP_Delete,     0, 0,       0}, /* 9 */
      { OP_SetCookie,  0, 0,       0}, /* 10 */
      { OP_Close,      0, 0,       0},
    };
    int base;
    Table *pTab = pIndex->pTable;

    if( (db->flags & SQLITE_InTrans)==0 ){
      sqliteVdbeAddOp(v, OP_Transaction, 0, 0);
      sqliteVdbeAddOp(v, OP_VerifyCookie, db->schema_cookie, 0);
      pParse->schemaVerified = 1;
    }
    if( !pTab->isTemp ){
      base = sqliteVdbeAddOpList(v, ArraySize(dropIndex), dropIndex);
      sqliteVdbeChangeP3(v, base+2, pIndex->zName, P3_STATIC);
      changeCookie(db);
      sqliteVdbeChangeP1(v, base+10, db->next_cookie);
    }
    sqliteVdbeAddOp(v, OP_Destroy, pIndex->tnum, pTab->isTemp);
    if( (db->flags & SQLITE_InTrans)==0 ){
      sqliteVdbeAddOp(v, OP_Commit, 0, 0);
    }
  }

  /* Move the index onto the pending DROP queue.  Or, if the index was
  ** never committed, just delete it.  Indices on the pending DROP queue
  ** get deleted by sqliteCommitInternalChanges() when the user executes
  ** a COMMIT.  Or if a rollback occurs, the elements of the DROP queue
  ** are moved back into the main hash table.
  */
  if( !pParse->explain ){
    sqlitePendingDropIndex(db, pIndex);
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
    if( pList==0 ){
      sqliteExprDelete(pExpr);
      return 0;
    }
  }
  if( (pList->nExpr & 7)==0 ){
    int n = pList->nExpr + 8;
    struct ExprList_item *a;
    a = sqliteRealloc(pList->a, n*sizeof(pList->a[0]));
    if( a==0 ){
      sqliteExprDelete(pExpr);
      return pList;
    }
    pList->a = a;
  }
  if( pExpr || pName ){
    i = pList->nExpr++;
    pList->a[i].pExpr = pExpr;
    pList->a[i].zName = 0;
    if( pName ){
      sqliteSetNString(&pList->a[i].zName, pName->z, pName->n, 0);
      sqliteDequote(pList->a[i].zName);
    }
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
    struct IdList_item *a;
    a = sqliteRealloc(pList->a, (pList->nId+8)*sizeof(pList->a[0]) );
    if( a==0 ){
      sqliteIdListDelete(pList);
      return 0;
    }
    pList->a = a;
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
    int openOp;
    if( (db->flags & SQLITE_InTrans)==0 ){
      sqliteVdbeAddOp(v, OP_Transaction, 0, 0);
      sqliteVdbeAddOp(v, OP_VerifyCookie, db->schema_cookie, 0);
      pParse->schemaVerified = 1;
    }
    addr = sqliteVdbeAddOp(v, OP_FileOpen, 0, 0);
    sqliteVdbeChangeP3(v, addr, pFilename->z, pFilename->n);
    sqliteVdbeDequoteP3(v, addr);
    openOp = pTab->isTemp ? OP_OpenWrAux : OP_OpenWrite;
    sqliteVdbeAddOp(v, openOp, 0, pTab->tnum);
    sqliteVdbeChangeP3(v, -1, pTab->zName, P3_STATIC);
    for(i=1, pIdx=pTab->pIndex; pIdx; pIdx=pIdx->pNext, i++){
      sqliteVdbeAddOp(v, openOp, i, pIdx->tnum);
      sqliteVdbeChangeP3(v, -1, pIdx->zName, P3_STATIC);
    }
    end = sqliteVdbeMakeLabel(v);
    addr = sqliteVdbeAddOp(v, OP_FileRead, pTab->nCol, end);
    if( pDelimiter ){
      sqliteVdbeChangeP3(v, addr, pDelimiter->z, pDelimiter->n);
      sqliteVdbeDequoteP3(v, addr);
    }else{
      sqliteVdbeChangeP3(v, addr, "\t", 1);
    }
    if( pTab->iPKey>=0 ){
      sqliteVdbeAddOp(v, OP_FileColumn, pTab->iPKey, 0);
      sqliteVdbeAddOp(v, OP_MustBeInt, 0, 0);
    }else{
      sqliteVdbeAddOp(v, OP_NewRecno, 0, 0);
    }
    if( pTab->pIndex ){
      sqliteVdbeAddOp(v, OP_Dup, 0, 0);
    }
    for(i=0; i<pTab->nCol; i++){
      if( i==pTab->iPKey ){
        /* The integer primary key column is filled with NULL since its
        ** value is always pulled from the record number */
        sqliteVdbeAddOp(v, OP_String, 0, 0);
      }else{
        sqliteVdbeAddOp(v, OP_FileColumn, i, 0);
      }
    }
    sqliteVdbeAddOp(v, OP_MakeRecord, pTab->nCol, 0);
    sqliteVdbeAddOp(v, OP_Put, 0, 0);
    for(i=1, pIdx=pTab->pIndex; pIdx; pIdx=pIdx->pNext, i++){
      if( pIdx->pNext ){
        sqliteVdbeAddOp(v, OP_Dup, 0, 0);
      }
      for(j=0; j<pIdx->nColumn; j++){
        sqliteVdbeAddOp(v, OP_FileColumn, pIdx->aiColumn[j], 0);
      }
      sqliteVdbeAddOp(v, OP_MakeIdxKey, pIdx->nColumn, 0);
      sqliteVdbeAddOp(v, OP_IdxPut, i, pIdx->isUnique);
    }
    sqliteVdbeAddOp(v, OP_Goto, 0, addr);
    sqliteVdbeResolveLabel(v, end);
    sqliteVdbeAddOp(v, OP_Noop, 0, 0);
    if( (db->flags & SQLITE_InTrans)==0 ){
      sqliteVdbeAddOp(v, OP_Commit, 0, 0);
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
    sqliteVdbeAddOp(v, OP_Transaction, 0, 0);
    sqliteVdbeAddOp(v, OP_VerifyCookie, db->schema_cookie, 0);
    pParse->schemaVerified = 1;
  }
  if( zName ){
    sqliteVdbeAddOp(v, OP_Reorganize, 0, 0);
    sqliteVdbeChangeP3(v, -1, zName, strlen(zName));
  }else{
    Table *pTab;
    Index *pIdx;
    HashElem *pE;
    for(pE=sqliteHashFirst(&db->tblHash); pE; pE=sqliteHashNext(pE)){
      pTab = sqliteHashData(pE);
      sqliteVdbeAddOp(v, OP_Reorganize, 0, 0);
      sqliteVdbeChangeP3(v, -1, pTab->zName, P3_STATIC);
      for(pIdx=pTab->pIndex; pIdx; pIdx=pIdx->pNext){
        sqliteVdbeAddOp(v, OP_Reorganize, 0, 0);
        sqliteVdbeChangeP3(v, -1, pIdx->zName, P3_STATIC);
      }
    }
  }
  if( (db->flags & SQLITE_InTrans)==0 ){
    sqliteVdbeAddOp(v, OP_Commit, 0, 0);
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
    sqliteVdbeAddOp(v, OP_Transaction, 1, 0);
    sqliteVdbeAddOp(v, OP_VerifyCookie, db->schema_cookie, 0);
    pParse->schemaVerified = 1;
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
    sqliteVdbeAddOp(v, OP_Commit, 0, 0);
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
    sqliteVdbeAddOp(v, OP_Rollback, 0, 0);
  }
  db->flags &= ~SQLITE_InTrans;
}

/*
** Interpret the given string as a boolean value.
*/
static int getBoolean(char *z){
  static char *azTrue[] = { "yes", "on", "true" };
  int i;
  if( z[0]==0 ) return 0;
  if( isdigit(z[0]) || (z[0]=='-' && isdigit(z[1])) ){
    return atoi(z);
  }
  for(i=0; i<sizeof(azTrue)/sizeof(azTrue[0]); i++){
    if( sqliteStrICmp(z,azTrue[i])==0 ) return 1;
  }
  return 0;
}

/*
** Process a pragma statement.  
**
** Pragmas are of this form:
**
**      PRAGMA id = value
**
** The identifier might also be a string.  The value is a string, and
** identifier, or a number.  If minusFlag is true, then the value is
** a number that was preceded by a minus sign.
*/
void sqlitePragma(Parse *pParse, Token *pLeft, Token *pRight, int minusFlag){
  char *zLeft = 0;
  char *zRight = 0;
  sqlite *db = pParse->db;

  zLeft = sqliteStrNDup(pLeft->z, pLeft->n);
  sqliteDequote(zLeft);
  if( minusFlag ){
    zRight = 0;
    sqliteSetNString(&zRight, "-", 1, pRight->z, pRight->n, 0);
  }else{
    zRight = sqliteStrNDup(pRight->z, pRight->n);
    sqliteDequote(zRight);
  }
 
  if( sqliteStrICmp(zLeft,"cache_size")==0 ){
    int size = atoi(zRight);
    sqliteBtreeSetCacheSize(db->pBe, size);
  }else

  if( sqliteStrICmp(zLeft, "vdbe_trace")==0 ){
    if( getBoolean(zRight) ){
      db->flags |= SQLITE_VdbeTrace;
    }else{
      db->flags &= ~SQLITE_VdbeTrace;
    }
  }else

  if( sqliteStrICmp(zLeft, "full_column_names")==0 ){
    if( getBoolean(zRight) ){
      db->flags |= SQLITE_FullColNames;
    }else{
      db->flags &= ~SQLITE_FullColNames;
    }
  }else

  if( sqliteStrICmp(zLeft, "result_set_details")==0 ){
    if( getBoolean(zRight) ){
      db->flags |= SQLITE_ResultDetails;
    }else{
      db->flags &= ~SQLITE_ResultDetails;
    }
  }else

  if( sqliteStrICmp(zLeft, "count_changes")==0 ){
    if( getBoolean(zRight) ){
      db->flags |= SQLITE_CountRows;
    }else{
      db->flags &= ~SQLITE_CountRows;
    }
  }else

  if( sqliteStrICmp(zLeft, "empty_result_callbacks")==0 ){
    if( getBoolean(zRight) ){
      db->flags |= SQLITE_NullCallback;
    }else{
      db->flags &= ~SQLITE_NullCallback;
    }
  }else

  if( sqliteStrICmp(zLeft, "table_info")==0 ){
    Table *pTab;
    Vdbe *v;
    pTab = sqliteFindTable(db, zRight);
    if( pTab ) v = sqliteGetVdbe(pParse);
    if( pTab && v ){
      static VdbeOp tableInfoPreface[] = {
        { OP_ColumnCount, 5, 0,       0},
        { OP_ColumnName,  0, 0,       "cid"},
        { OP_ColumnName,  1, 0,       "name"},
        { OP_ColumnName,  2, 0,       "type"},
        { OP_ColumnName,  3, 0,       "notnull"},
        { OP_ColumnName,  4, 0,       "dflt_value"},
      };
      int i;
      sqliteVdbeAddOpList(v, ArraySize(tableInfoPreface), tableInfoPreface);
      for(i=0; i<pTab->nCol; i++){
        sqliteVdbeAddOp(v, OP_Integer, i, 0);
        sqliteVdbeAddOp(v, OP_String, 0, 0);
        sqliteVdbeChangeP3(v, -1, pTab->aCol[i].zName, P3_STATIC);
        sqliteVdbeAddOp(v, OP_String, 0, 0);
        sqliteVdbeChangeP3(v, -1, 
           pTab->aCol[i].zType ? pTab->aCol[i].zType : "text", P3_STATIC);
        sqliteVdbeAddOp(v, OP_Integer, pTab->aCol[i].notNull, 0);
        sqliteVdbeAddOp(v, OP_String, 0, 0);
        sqliteVdbeChangeP3(v, -1, pTab->aCol[i].zDflt, P3_STATIC);
        sqliteVdbeAddOp(v, OP_Callback, 5, 0);
      }
    }
  }else

  if( sqliteStrICmp(zLeft, "index_info")==0 ){
    Index *pIdx;
    Table *pTab;
    Vdbe *v;
    pIdx = sqliteFindIndex(db, zRight);
    if( pIdx ) v = sqliteGetVdbe(pParse);
    if( pIdx && v ){
      static VdbeOp tableInfoPreface[] = {
        { OP_ColumnCount, 3, 0,       0},
        { OP_ColumnName,  0, 0,       "seqno"},
        { OP_ColumnName,  1, 0,       "cid"},
        { OP_ColumnName,  2, 0,       "name"},
      };
      int i;
      pTab = pIdx->pTable;
      sqliteVdbeAddOpList(v, ArraySize(tableInfoPreface), tableInfoPreface);
      for(i=0; i<pIdx->nColumn; i++){
        int cnum = pIdx->aiColumn[i];
        sqliteVdbeAddOp(v, OP_Integer, i, 0);
        sqliteVdbeAddOp(v, OP_Integer, cnum, 0);
        sqliteVdbeAddOp(v, OP_String, 0, 0);
        sqliteVdbeChangeP3(v, -1, pTab->aCol[cnum].zName, P3_STATIC);
        sqliteVdbeAddOp(v, OP_Callback, 3, 0);
      }
    }
  }else

  if( sqliteStrICmp(zLeft, "index_list")==0 ){
    Index *pIdx;
    Table *pTab;
    Vdbe *v;
    pTab = sqliteFindTable(db, zRight);
    if( pTab ){
      v = sqliteGetVdbe(pParse);
      pIdx = pTab->pIndex;
    }
    if( pTab && pIdx && v ){
      int i = 0; 
      static VdbeOp indexListPreface[] = {
        { OP_ColumnCount, 3, 0,       0},
        { OP_ColumnName,  0, 0,       "seq"},
        { OP_ColumnName,  1, 0,       "name"},
        { OP_ColumnName,  2, 0,       "unique"},
      };

      sqliteVdbeAddOpList(v, ArraySize(indexListPreface), indexListPreface);
      while(pIdx){
        sqliteVdbeAddOp(v, OP_Integer, i, 0);
        sqliteVdbeAddOp(v, OP_String, 0, 0);
        sqliteVdbeChangeP3(v, -1, pIdx->zName, P3_STATIC);
        sqliteVdbeAddOp(v, OP_Integer, pIdx->isUnique, 0);
        sqliteVdbeAddOp(v, OP_Callback, 3, 0);
	++i;
	pIdx = pIdx->pNext;
      }
    }
  }else

#ifndef NDEBUG
  if( sqliteStrICmp(zLeft, "parser_trace")==0 ){
    extern void sqliteParserTrace(FILE*, char *);
    if( getBoolean(zRight) ){
      sqliteParserTrace(stdout, "parser: ");
    }else{
      sqliteParserTrace(0, 0);
    }
  }else
#endif

  {}
  sqliteFree(zLeft);
  sqliteFree(zRight);
}

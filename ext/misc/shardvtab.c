/*
** 2019-04-26
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
*************************************************************************
**
** This file implements a virtual-table that can be used to access a
** sharded table implemented as the UNION ALL of various separate tables.
*/
#if !defined(SQLITEINT_H)
#include "sqlite3ext.h"
#endif
SQLITE_EXTENSION_INIT1
#include <string.h>
#include <assert.h>
#include <math.h>

/* shardvtab_vtab is a subclass of sqlite3_vtab which is
** underlying representation of the virtual table
*/
typedef struct shardvtab_vtab shardvtab_vtab;
struct shardvtab_vtab {
  sqlite3_vtab base;  /* Base class - must be first */
  sqlite3 *db;        /* The database connection */
  char *zView;        /* Name of view that implements the shard */
  int nCol;           /* Number of columns in the view */
  char **azCol;       /* Names of the columns, individually malloced */
};

/* shardvtab_cursor is a subclass of sqlite3_vtab_cursor which will
** serve as the underlying representation of a cursor that scans
** over rows of the result
*/
typedef struct shardvtab_cursor shardvtab_cursor;
struct shardvtab_cursor {
  sqlite3_vtab_cursor base;  /* Base class - must be first */
  sqlite3_stmt *pStmt;       /* Prepared statement to access the shard */
  int rcLastStep;            /* Last return from sqlite3_step() */
};

/*
** The shardvtabConnect() method is invoked to create a new
** shard virtual table.
**
** Think of this routine as the constructor for shardvtab_vtab objects.
**
** All this routine needs to do is:
**
**    (1) Allocate the shardvtab_vtab object and initialize all fields.
**
**    (2) Tell SQLite (via the sqlite3_declare_vtab() interface) what the
**        result set of queries against the virtual table will look like.
*/
static int shardvtabConnect(
  sqlite3 *db,
  void *pAux,
  int argc, const char *const*argv,
  sqlite3_vtab **ppVtab,
  char **pzErr
){
  shardvtab_vtab *pNew;
  int rc;
  char *zSql;
  sqlite3_str *pSchema;
  sqlite3_stmt *pStmt = 0;
  const char *zView = 0;
  char **azCol = 0;
  int nCol = 0;
  char cSep;
  int i;

  if( argc!=4 || argv[0]==0 ){
    *pzErr = sqlite3_mprintf("one argument requires: the name of a view");
    return SQLITE_ERROR;
  }
  zView = argv[3];
  zSql = sqlite3_mprintf("SELECT * FROM \"%w\"", zView);
  if( zSql==0 ){
    return SQLITE_NOMEM;
  }
  rc = sqlite3_prepare_v2(db, zSql, -1, &pStmt, 0);
  sqlite3_free(zSql);
  if( rc ){
    *pzErr = sqlite3_mprintf("not a valid view: \"%w\"", zView);
    return SQLITE_NOMEM;
  }
  pSchema = sqlite3_str_new(db);
  if( pSchema==0 ){
    sqlite3_finalize(pStmt);
    return SQLITE_NOMEM;
  }
  sqlite3_str_appendall(pSchema, "CREATE TABLE x");
  cSep = '(';
  for(i=0; i<sqlite3_column_count(pStmt); i++){
    const char *zName = sqlite3_column_name(pStmt,i);
    char **azNew = sqlite3_realloc64(azCol, sizeof(azCol[0])*(i+1));
    if( azNew==0 ){
      rc = SQLITE_NOMEM;
      goto shardvtab_connect_error;
    }
    sqlite3_str_appendf(pSchema, "%c\"%w\"", cSep, zName);
    cSep = ',';
    azCol = azNew;
    azCol[nCol] = sqlite3_mprintf("%s", zName);
    if( azCol[nCol]==0 ){
      rc = SQLITE_NOMEM;
      goto shardvtab_connect_error;
    }
    nCol++;
  }
  sqlite3_str_appendall(pSchema, ")");
  sqlite3_finalize(pStmt);
  pStmt = 0;
  zSql = sqlite3_str_finish(pSchema);
  pSchema = 0;
  if( zSql==0 ){
    rc = SQLITE_NOMEM;
    goto shardvtab_connect_error;
  }
  rc = sqlite3_declare_vtab(db, zSql);
  sqlite3_free(zSql);
  if( rc!=SQLITE_OK ){
    goto shardvtab_connect_error;
  }else{
    size_t n = strlen(zView) + 1;
    pNew = sqlite3_malloc64( sizeof(*pNew) + n );
    *ppVtab = (sqlite3_vtab*)pNew;
    if( pNew==0 ){
      rc = SQLITE_NOMEM;
      goto shardvtab_connect_error;
    }
    memset(pNew, 0, sizeof(*pNew));
    pNew->db = db;
    pNew->zView = (char*)&pNew[1];
    memcpy(pNew->zView, zView, n);
    pNew->nCol = nCol;
    pNew->azCol = azCol;
  }
  return SQLITE_OK;

shardvtab_connect_error:
  sqlite3_finalize(pStmt);
  for(i=0; i<nCol; i++) sqlite3_free(azCol[i]);
  sqlite3_free(azCol);
  sqlite3_free(sqlite3_str_finish(pSchema));
  return rc;
}

/*
** This method is the destructor for shardvtab_vtab objects.
*/
static int shardvtabDisconnect(sqlite3_vtab *pVtab){
  int i;
  shardvtab_vtab *p = (shardvtab_vtab*)pVtab;
  for(i=0; i<p->nCol; i++) sqlite3_free(p->azCol[i]);
  sqlite3_free(p->azCol);
  sqlite3_free(p);
  return SQLITE_OK;
}

/*
** Constructor for a new shardvtab_cursor object.
*/
static int shardvtabOpen(sqlite3_vtab *p, sqlite3_vtab_cursor **ppCursor){
  shardvtab_cursor *pCur;
  pCur = sqlite3_malloc( sizeof(*pCur) );
  if( pCur==0 ) return SQLITE_NOMEM;
  memset(pCur, 0, sizeof(*pCur));
  *ppCursor = &pCur->base;
  return SQLITE_OK;
}

/*
** Destructor for a shardvtab_cursor.
*/
static int shardvtabClose(sqlite3_vtab_cursor *cur){
  shardvtab_cursor *pCur = (shardvtab_cursor*)cur;
  sqlite3_finalize(pCur->pStmt);
  sqlite3_free(pCur);
  return SQLITE_OK;
}


/*
** Advance a shardvtab_cursor to its next row of output.
*/
static int shardvtabNext(sqlite3_vtab_cursor *cur){
  shardvtab_cursor *pCur = (shardvtab_cursor*)cur;
  int rc;
  rc = pCur->rcLastStep = sqlite3_step(pCur->pStmt);
  if( rc==SQLITE_ROW || rc==SQLITE_DONE ) return SQLITE_OK;
  return rc;
}

/*
** Return values of columns for the row at which the shardvtab_cursor
** is currently pointing.
*/
static int shardvtabColumn(
  sqlite3_vtab_cursor *cur,   /* The cursor */
  sqlite3_context *ctx,       /* First argument to sqlite3_result_...() */
  int i                       /* Which column to return */
){
  shardvtab_cursor *pCur = (shardvtab_cursor*)cur;
  sqlite3_result_value(ctx, sqlite3_column_value(pCur->pStmt, i));
  return SQLITE_OK;
}

/*
** Return the rowid for the current row.  In this implementation, the
** rowid is the same as the output value.
*/
static int shardvtabRowid(sqlite3_vtab_cursor *cur, sqlite_int64 *pRowid){
  *pRowid = 0;
  return SQLITE_OK;
}

/*
** Return TRUE if the cursor has been moved off of the last
** row of output.
*/
static int shardvtabEof(sqlite3_vtab_cursor *cur){
  shardvtab_cursor *pCur = (shardvtab_cursor*)cur;
  return pCur->rcLastStep!=SQLITE_ROW;
}

/*
** This method is called to "rewind" the shardvtab_cursor object back
** to the first row of output.  This method is always called at least
** once prior to any call to shardvtabColumn() or shardvtabRowid() or 
** shardvtabEof().
*/
static int shardvtabFilter(
  sqlite3_vtab_cursor *pVtabCursor, 
  int idxNum, const char *idxStr,
  int argc, sqlite3_value **argv
){
  shardvtab_cursor *pCur = (shardvtab_cursor *)pVtabCursor;
  shardvtab_vtab *pTab = (shardvtab_vtab *)pVtabCursor->pVtab;
  int rc;
  sqlite3_finalize(pCur->pStmt);
  pCur->pStmt = 0;
  rc = sqlite3_prepare_v2(pTab->db, idxStr, -1, &pCur->pStmt, 0);
  if( rc==SQLITE_OK ){
    int i;
    for(i=0; i<argc; i++){
      sqlite3_bind_value(pCur->pStmt, i+1, argv[i]);
    }
  }else{
    sqlite3_finalize(pCur->pStmt);
    pCur->pStmt = 0;
  }
  pCur->rcLastStep = rc;
  return rc;
}

/*
** SQLite will invoke this method one or more times while planning a query
** that uses the virtual table.  This routine needs to create
** a query plan for each invocation and compute an estimated cost for that
** plan.
*/
static int shardvtabBestIndex(
  sqlite3_vtab *tab,
  sqlite3_index_info *p
){
  shardvtab_vtab *pTab = (shardvtab_vtab*)tab;
  int i;
  int n;
  sqlite3_stmt *pStmt;
  int rc;
  sqlite3_str *pSql;
  char *zSep = "WHERE";
  char *zSql;
  pSql = sqlite3_str_new(pTab->db);
  if( pSql==0 ) return SQLITE_NOMEM;
  sqlite3_str_appendf(pSql, "SELECT * FROM \"%w\"", pTab->zView);
  for(i=n=0; i<p->nConstraint; i++){
    const char *zOp;
    int iCol;
    if( p->aConstraint[i].usable==0 ) continue;
    iCol = p->aConstraint[i].iColumn;
    if( iCol<0 ) continue;
    zOp = 0;
    switch( p->aConstraint[i].op ){
      case SQLITE_INDEX_CONSTRAINT_EQ:     zOp = "==";     break;
      case SQLITE_INDEX_CONSTRAINT_GT:     zOp = ">";      break;
      case SQLITE_INDEX_CONSTRAINT_LE:     zOp = "<=";     break;
      case SQLITE_INDEX_CONSTRAINT_LT:     zOp = "<";      break;
      case SQLITE_INDEX_CONSTRAINT_GE:     zOp = ">=";     break;
      case SQLITE_INDEX_CONSTRAINT_MATCH:  zOp = "MATCH";  break;
      case SQLITE_INDEX_CONSTRAINT_LIKE:   zOp = "LIKE";   break;
      case SQLITE_INDEX_CONSTRAINT_GLOB:   zOp = "GLOB";   break;
      case SQLITE_INDEX_CONSTRAINT_REGEXP: zOp = "REGEXP"; break;
      case SQLITE_INDEX_CONSTRAINT_NE:     zOp = "<>";     break;
      case SQLITE_INDEX_CONSTRAINT_IS:     zOp = "IS";     break;
    }
    if( zOp ){
      n++;
      p->aConstraintUsage[i].argvIndex = n;
      sqlite3_str_appendf(pSql, " %s (\"%w\" %s ?%d)", 
                          zSep, pTab->azCol[iCol], zOp, n);
      zSep = "AND";
    }
  }
  zSql = sqlite3_str_finish(pSql);
  if( zSql==0 ){
    return SQLITE_NOMEM;
  }
  rc = sqlite3_prepare_v2(pTab->db, zSql, -1, &pStmt, 0);
  if( rc==SQLITE_OK ){
    int x = sqlite3_stmt_status(pStmt, SQLITE_STMTSTATUS_EST_COST, 0);
    p->estimatedCost = pow(2.0, 0.1*x);
    p->estimatedRows =
            sqlite3_stmt_status(pStmt, SQLITE_STMTSTATUS_EST_ROWS, 0);
    p->idxStr = zSql;
    p->needToFreeIdxStr = 1;
  }else{
    sqlite3_free(zSql);
  }
  sqlite3_finalize(pStmt);
  return rc;
}

/*
** This following structure defines all the methods for the 
** virtual table.
*/
static sqlite3_module shardvtabModule = {
  /* iVersion    */ 0,
  /* xCreate     */ shardvtabConnect,
  /* xConnect    */ shardvtabConnect,
  /* xBestIndex  */ shardvtabBestIndex,
  /* xDisconnect */ shardvtabDisconnect,
  /* xDestroy    */ shardvtabDisconnect,
  /* xOpen       */ shardvtabOpen,
  /* xClose      */ shardvtabClose,
  /* xFilter     */ shardvtabFilter,
  /* xNext       */ shardvtabNext,
  /* xEof        */ shardvtabEof,
  /* xColumn     */ shardvtabColumn,
  /* xRowid      */ shardvtabRowid,
  /* xUpdate     */ 0,
  /* xBegin      */ 0,
  /* xSync       */ 0,
  /* xCommit     */ 0,
  /* xRollback   */ 0,
  /* xFindMethod */ 0,
  /* xRename     */ 0,
  /* xSavepoint  */ 0,
  /* xRelease    */ 0,
  /* xRollbackTo */ 0,
  /* xShadowName */ 0
};


#ifdef _WIN32
__declspec(dllexport)
#endif
int sqlite3_shardvtab_init(
  sqlite3 *db, 
  char **pzErrMsg, 
  const sqlite3_api_routines *pApi
){
  int rc = SQLITE_OK;
  SQLITE_EXTENSION_INIT2(pApi);
  rc = sqlite3_create_module(db, "shardvtab", &shardvtabModule, 0);
  return rc;
}

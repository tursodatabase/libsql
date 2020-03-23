/*
** 2020-03-23
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
** This file implements virtual-tables for examining the bytecode content
** of a prepared statement.
*/
#ifdef SQLITE_ENABLE_BYTECODE_VTAB
#include "sqliteInt.h"
#include "vdbeInt.h"

/* An instance of the bytecode() table-valued function.
*/
typedef struct bytecodevtab_vtab bytecodevtab_vtab;
struct bytecodevtab_vtab {
  sqlite3_vtab base;     /* Base class - must be first */
  sqlite3_stmt *pStmt;   /* The statement whose bytecode is to be displayed */
};

/* A cursor for scanning through the bytecode
*/
typedef struct bytecodevtab_cursor bytecodevtab_cursor;
struct bytecodevtab_cursor {
  sqlite3_vtab_cursor base;  /* Base class - must be first */
  sqlite3_int64 iRowid;      /* The rowid */
};

/*
** Create a new bytecode() table-valued function.
*/
static int bytecodevtabConnect(
  sqlite3 *db,
  void *pAux,
  int argc, const char *const*argv,
  sqlite3_vtab **ppVtab,
  char **pzErr
){
  bytecodevtab_vtab *pNew;
  int rc;

  rc = sqlite3_declare_vtab(db,
         "CREATE TABLE x("
           "addr INT,"
           "opcode TEXT,"
           "p1 INT,"
           "p2 INT,"
           "p3 INT,"
           "p4 TEXT,"
           "p5 INT,"
           "comment TEXT,"
           "subprog TEXT," 
           "stmt HIDDEN"
         ");"
       );
  if( rc==SQLITE_OK ){
    pNew = sqlite3_malloc( sizeof(*pNew) );
    *ppVtab = (sqlite3_vtab*)pNew;
    if( pNew==0 ) return SQLITE_NOMEM;
    memset(pNew, 0, sizeof(*pNew));
  }
  return rc;
}

/*
** This method is the destructor for bytecodevtab_vtab objects.
*/
static int bytecodevtabDisconnect(sqlite3_vtab *pVtab){
  bytecodevtab_vtab *p = (bytecodevtab_vtab*)pVtab;
  sqlite3_free(p);
  return SQLITE_OK;
}

/*
** Constructor for a new bytecodevtab_cursor object.
*/
static int bytecodevtabOpen(sqlite3_vtab *p, sqlite3_vtab_cursor **ppCursor){
  bytecodevtab_cursor *pCur;
  pCur = sqlite3_malloc( sizeof(*pCur) );
  if( pCur==0 ) return SQLITE_NOMEM;
  memset(pCur, 0, sizeof(*pCur));
  *ppCursor = &pCur->base;
  return SQLITE_OK;
}

/*
** Destructor for a bytecodevtab_cursor.
*/
static int bytecodevtabClose(sqlite3_vtab_cursor *cur){
  bytecodevtab_cursor *pCur = (bytecodevtab_cursor*)cur;
  sqlite3_free(pCur);
  return SQLITE_OK;
}


/*
** Advance a bytecodevtab_cursor to its next row of output.
*/
static int bytecodevtabNext(sqlite3_vtab_cursor *cur){
  bytecodevtab_cursor *pCur = (bytecodevtab_cursor*)cur;
  pCur->iRowid++;
  return SQLITE_OK;
}

/*
** Return values of columns for the row at which the bytecodevtab_cursor
** is currently pointing.
*/
static int bytecodevtabColumn(
  sqlite3_vtab_cursor *cur,   /* The cursor */
  sqlite3_context *ctx,       /* First argument to sqlite3_result_...() */
  int i                       /* Which column to return */
){
  bytecodevtab_cursor *pCur = (bytecodevtab_cursor*)cur;
#if 0
  switch( i ){
    case TEMPLATEVTAB_A:
      sqlite3_result_int(ctx, 1000 + pCur->iRowid);
      break;
    default:
      assert( i==TEMPLATEVTAB_B );
      sqlite3_result_int(ctx, 2000 + pCur->iRowid);
      break;
  }
#endif
  return SQLITE_OK;
}

/*
** Return the rowid for the current row.  In this implementation, the
** rowid is the same as the output value.
*/
static int bytecodevtabRowid(sqlite3_vtab_cursor *cur, sqlite_int64 *pRowid){
  bytecodevtab_cursor *pCur = (bytecodevtab_cursor*)cur;
  *pRowid = pCur->iRowid;
  return SQLITE_OK;
}

/*
** Return TRUE if the cursor has been moved off of the last
** row of output.
*/
static int bytecodevtabEof(sqlite3_vtab_cursor *cur){
  bytecodevtab_cursor *pCur = (bytecodevtab_cursor*)cur;
  return pCur->iRowid>=10;
}

/*
** This method is called to "rewind" the bytecodevtab_cursor object back
** to the first row of output.  This method is always called at least
** once prior to any call to bytecodevtabColumn() or bytecodevtabRowid() or 
** bytecodevtabEof().
*/
static int bytecodevtabFilter(
  sqlite3_vtab_cursor *pVtabCursor, 
  int idxNum, const char *idxStr,
  int argc, sqlite3_value **argv
){
  bytecodevtab_cursor *pCur = (bytecodevtab_cursor *)pVtabCursor;
  pCur->iRowid = 1;
  return SQLITE_OK;
}

/*
** SQLite will invoke this method one or more times while planning a query
** that uses the virtual table.  This routine needs to create
** a query plan for each invocation and compute an estimated cost for that
** plan.
*/
static int bytecodevtabBestIndex(
  sqlite3_vtab *tab,
  sqlite3_index_info *pIdxInfo
){
  pIdxInfo->estimatedCost = (double)10;
  pIdxInfo->estimatedRows = 10;
  return SQLITE_OK;
}

/*
** This following structure defines all the methods for the 
** virtual table.
*/
static sqlite3_module bytecodevtabModule = {
  /* iVersion    */ 0,
  /* xCreate     */ 0,
  /* xConnect    */ bytecodevtabConnect,
  /* xBestIndex  */ bytecodevtabBestIndex,
  /* xDisconnect */ bytecodevtabDisconnect,
  /* xDestroy    */ 0,
  /* xOpen       */ bytecodevtabOpen,
  /* xClose      */ bytecodevtabClose,
  /* xFilter     */ bytecodevtabFilter,
  /* xNext       */ bytecodevtabNext,
  /* xEof        */ bytecodevtabEof,
  /* xColumn     */ bytecodevtabColumn,
  /* xRowid      */ bytecodevtabRowid,
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


int sqlite3VdbeBytecodeVtabInit(sqlite3 *db){
  int rc;
  rc = sqlite3_create_module(db, "bytecodevtab", &bytecodevtabModule, 0);
  return rc;
}
#endif /* SQLITE_ENABLE_BYTECODE_VTAB */

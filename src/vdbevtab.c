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
typedef struct bytecodevtab bytecodevtab;
struct bytecodevtab {
  sqlite3_vtab base;     /* Base class - must be first */
  sqlite3 *db;           /* Database connection */
};

/* A cursor for scanning through the bytecode
*/
typedef struct bytecodevtab_cursor bytecodevtab_cursor;
struct bytecodevtab_cursor {
  sqlite3_vtab_cursor base;  /* Base class - must be first */
  sqlite3_stmt *pStmt;       /* The statement whose bytecode is displayed */
  int iRowid;                /* The rowid of the output table */
  int iAddr;                 /* Address */
  int needFinalize;          /* Cursors owns pStmt and must finalize it */
  Op *aOp;                   /* Operand array */
  char *zP4;                 /* Rendered P4 value */
  Mem sub;                   /* Subprograms */
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
  bytecodevtab *pNew;
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
    pNew->db = db;
  }
  return rc;
}

/*
** This method is the destructor for bytecodevtab objects.
*/
static int bytecodevtabDisconnect(sqlite3_vtab *pVtab){
  bytecodevtab *p = (bytecodevtab*)pVtab;
  sqlite3_free(p);
  return SQLITE_OK;
}

/*
** Constructor for a new bytecodevtab_cursor object.
*/
static int bytecodevtabOpen(sqlite3_vtab *p, sqlite3_vtab_cursor **ppCursor){
  bytecodevtab *pVTab = (bytecodevtab*)p;
  bytecodevtab_cursor *pCur;
  pCur = sqlite3_malloc( sizeof(*pCur) );
  if( pCur==0 ) return SQLITE_NOMEM;
  memset(pCur, 0, sizeof(*pCur));
  sqlite3VdbeMemInit(&pCur->sub, pVTab->db, 1);
  *ppCursor = &pCur->base;
  return SQLITE_OK;
}

/*
** Clear all internal content from a bytecodevtab cursor.
*/
static void bytecodevtabCursorClear(bytecodevtab_cursor *pCur){
  sqlite3_free(pCur->zP4);
  pCur->zP4 = 0;
  sqlite3VdbeMemSetNull(&pCur->sub);
  if( pCur->needFinalize ){
    sqlite3_finalize(pCur->pStmt);
  }
  pCur->pStmt = 0;
  pCur->needFinalize = 0;
}

/*
** Destructor for a bytecodevtab_cursor.
*/
static int bytecodevtabClose(sqlite3_vtab_cursor *cur){
  bytecodevtab_cursor *pCur = (bytecodevtab_cursor*)cur;
  bytecodevtabCursorClear(pCur);
  sqlite3_free(pCur);
  return SQLITE_OK;
}


/*
** Advance a bytecodevtab_cursor to its next row of output.
*/
static int bytecodevtabNext(sqlite3_vtab_cursor *cur){
  bytecodevtab_cursor *pCur = (bytecodevtab_cursor*)cur;
  int rc;
  if( pCur->zP4 ){
    sqlite3_free(pCur->zP4);
    pCur->zP4 = 0;
  }
  rc = sqlite3VdbeNextOpcode((Vdbe*)pCur->pStmt, &pCur->sub, 0,
                             &pCur->iRowid, &pCur->iAddr, &pCur->aOp);
  if( rc!=SQLITE_OK ){
    sqlite3VdbeMemSetNull(&pCur->sub);
    pCur->aOp = 0;
  }
  return SQLITE_OK;
}

/*
** Return TRUE if the cursor has been moved off of the last
** row of output.
*/
static int bytecodevtabEof(sqlite3_vtab_cursor *cur){
  bytecodevtab_cursor *pCur = (bytecodevtab_cursor*)cur;
  return pCur->aOp==0;
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
  bytecodevtab *pVTab;
  Op *pOp = pCur->aOp + pCur->iAddr;
  switch( i ){
    case 0:
      sqlite3_result_int(ctx, pCur->iAddr);
      break;
    case 1:
      sqlite3_result_text(ctx, (char*)sqlite3OpcodeName(pOp->opcode),
                          -1, SQLITE_STATIC);
      break;
    case 2:
      sqlite3_result_int(ctx, pOp->p1);
      break;
    case 3:
      sqlite3_result_int(ctx, pOp->p2);
      break;
    case 4:
      sqlite3_result_int(ctx, pOp->p3);
      break;
    case 5:
    case 7:
      pVTab = (bytecodevtab*)cur->pVtab;
      if( pCur->zP4==0 ){
        pCur->zP4 = sqlite3VdbeDisplayP4(pVTab->db, pOp);
      }
      if( i==5 ){
        sqlite3_result_text(ctx, pCur->zP4, -1, SQLITE_STATIC);
      }else{
        char *zCom = sqlite3VdbeDisplayComment(pVTab->db, pOp, pCur->zP4);
        sqlite3_result_text(ctx, zCom, -1, sqlite3_free);
      }
      break;
    case 6:
      sqlite3_result_int(ctx, pOp->p5);
      break;
  }
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
** Initialize a cursor to use a new
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
  bytecodevtab *pVTab = (bytecodevtab *)pVtabCursor->pVtab;
  int rc = SQLITE_OK;

  bytecodevtabCursorClear(pCur);
  pCur->iRowid = 0;
  pCur->iAddr = 0;
  assert( argc==1 );
  if( sqlite3_value_type(argv[0])==SQLITE_TEXT ){
    const char *zSql = (const char*)sqlite3_value_text(argv[0]);
    if( zSql==0 ){
      rc = SQLITE_NOMEM;
    }else{
      rc = sqlite3_prepare_v2(pVTab->db, zSql, -1, &pCur->pStmt, 0);
      pCur->needFinalize = 1;
    }
  }else{
    pCur->pStmt = (sqlite3_stmt*)sqlite3_value_pointer(argv[0],"stmt-pointer");
  }
  if( pCur->pStmt==0 ){
    pVTab->base.zErrMsg = sqlite3_mprintf(
       "argument to bytecode() is not a valid SQL statement"
    );
    rc = SQLITE_ERROR;
  }else{
    bytecodevtabNext(pVtabCursor);
  }
  return rc;
}

/*
** We must have a single stmt=? constraint that will be passed through
** into the xFilter method.  If there is no valid stmt=? constraint,
** then return an SQLITE_CONSTRAINT error.
*/
static int bytecodevtabBestIndex(
  sqlite3_vtab *tab,
  sqlite3_index_info *pIdxInfo
){
  int i;
  int rc = SQLITE_CONSTRAINT;
  pIdxInfo->estimatedCost = (double)100;
  pIdxInfo->estimatedRows = 100;
  for(i=0; i<pIdxInfo->nConstraint; i++){
    if( pIdxInfo->aConstraint[i].usable==0 ) continue;
    if( pIdxInfo->aConstraint[i].op!=SQLITE_INDEX_CONSTRAINT_EQ ) continue;
    if( pIdxInfo->aConstraint[i].iColumn!=9 ) continue;
    rc = SQLITE_OK;
    pIdxInfo->aConstraintUsage[i].omit = 1;
    pIdxInfo->aConstraintUsage[i].argvIndex = 1;
    break;
  }
  return rc;
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
  rc = sqlite3_create_module(db, "bytecode", &bytecodevtabModule, 0);
  return rc;
}
#endif /* SQLITE_ENABLE_BYTECODE_VTAB */

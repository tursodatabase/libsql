/*
** 2020-09-02
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
** This file contains the implementation of the "shmlock" eponymous 
** virtual table module. The schema of which is effectively:
**
**   CREATE TABLE shmlock(
**       connid TEXT, 
**       lock TEXT, 
**       locktype TEXT, 
**       mxFrame INTEGER,
**       dbname HIDDEN
**   );
**
** This virtual table may be used to query for the locks held on the main or
** any attached database that is in wal mode. If the database is not in
** wal mode, zero rows are returned. Otherwise, one row is returned for
** each lock held on the database by a connection in either the current 
** process, as well as - to the extent possible - one row for each lock held 
** by another process. On unix, "to the extent possible" means that if
** two or more external processes hold a SHARED wal-mode lock on the same
** locking-slot, only one of them is reported on.
**
** To query for locks on the main database, either of:
**
**     SELECT * FROM shmlock;
**     SELECT * FROM shmlock('main');
**
** To query for locks on the attached database named 'aux':
**
**     SELECT * FROM shmlock('aux');
**
** The non-HIDDEN columns of each row may be interpreted as follows:
**
** connid:
**   String identifying the connection. For a local connection lock, this is
**   either the string representation of an internal pointer value, or else a
**   string configured using the file-control SQLITE_FCNTL_SHMLOCK_NAME.
**   For a lock from an external process, the id takes the form "pid.123",
**   where 123 is the process-id of the lock holder.
**
** lock:
**   A text value representing the particular wal mode lock held. One of
**   "WRITE", "CHECKPOINT", "RECOVER" or "READ(n)", where n is between 0 and
**   4, inclusive.
**
** locktype:
**   'R' for a shared (reader) lock, or 'W' for an exclusive (writer) lock.
**
** mxframe:
**   When the lock is a shared lock held on a READ(n) slot, the integer value
**   of the corresponding read-mark slot in shared-memory.
**
** This module currently only works with the "unix" VFS.
**
*/
#if !defined(SQLITEINT_H)
#include "sqlite3ext.h"
#endif
SQLITE_EXTENSION_INIT1
#include <string.h>
#include <assert.h>

/* shmlock_vtab is a subclass of sqlite3_vtab which is
** underlying representation of the virtual table
*/
typedef struct shmlock_vtab shmlock_vtab;
struct shmlock_vtab {
  sqlite3_vtab base;  /* Base class - must be first */
  sqlite3 *db;
};

/* shmlock_cursor is a subclass of sqlite3_vtab_cursor which will
** serve as the underlying representation of a cursor that scans
** over rows of the result
*/
typedef struct shmlock_cursor shmlock_cursor;
struct shmlock_cursor {
  sqlite3_vtab_cursor base;  /* Base class - must be first */
  char *zFcntl;
  int iFcntl;
  char *aSpace;              /* Space to use for dequoted strings */

  const char *azCol[4];
  sqlite3_int64 iRowid;
};

/*
** Create a new shmlock_vtab object.
*/
static int shmlockConnect(
  sqlite3 *db,
  void *pAux,
  int argc, const char *const*argv,
  sqlite3_vtab **ppVtab,
  char **pzErr
){
  shmlock_vtab *pNew;
  int rc;

  rc = sqlite3_declare_vtab(db,
    "CREATE TABLE shmlock("
        "connid TEXT, "
        "lock TEXT, "
        "locktype TEXT, "
        "mxFrame INTEGER,"
        "dbname HIDDEN"
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
** This method is the destructor for shmlock_vtab objects.
*/
static int shmlockDisconnect(sqlite3_vtab *pVtab){
  shmlock_vtab *p = (shmlock_vtab*)pVtab;
  sqlite3_free(p);
  return SQLITE_OK;
}

/*
** Constructor for a new shmlock_cursor object.
*/
static int shmlockOpen(sqlite3_vtab *p, sqlite3_vtab_cursor **ppCursor){
  shmlock_cursor *pCur;
  pCur = sqlite3_malloc( sizeof(*pCur) );
  if( pCur==0 ) return SQLITE_NOMEM;
  memset(pCur, 0, sizeof(*pCur));
  *ppCursor = &pCur->base;
  return SQLITE_OK;
}

/*
** Destructor for a shmlock_cursor.
*/
static int shmlockClose(sqlite3_vtab_cursor *cur){
  shmlock_cursor *pCur = (shmlock_cursor*)cur;
  sqlite3_free(pCur->zFcntl);
  sqlite3_free(pCur->aSpace);
  sqlite3_free(pCur);
  return SQLITE_OK;
}


/*
** Advance a shmlock_cursor to its next row of output.
*/
static int shmlockNext(sqlite3_vtab_cursor *cur){
  shmlock_cursor *pCur = (shmlock_cursor*)cur;
  int ii = pCur->iFcntl;
  const char *z = pCur->zFcntl;
  char *a = pCur->aSpace;
  int iCol;

  memset(pCur->azCol, 0, sizeof(char*)*4);
  if( z[ii]=='\0' ) return SQLITE_OK;;
  for(iCol=0; iCol<4; iCol++){
    if( z[ii]!='\'' ) return SQLITE_ERROR;

    pCur->azCol[iCol] = a;
    ii++;
    while( z[ii] ){
      if( z[ii]=='\'' ){
        ii++;
        if( z[ii]!='\'' ) break;
      }
      *a++ = z[ii++];
    }
    *a++ = '\0';
    while( z[ii]==' ' ) ii++;
    if( z[ii]=='\0' ) break;
    if( z[ii]=='\n' ){
      ii++;
      break;
    }
  }

  pCur->iFcntl = ii;
  pCur->iRowid++;
  return SQLITE_OK;
}

/*
** Return values of columns for the row at which the shmlock_cursor
** is currently pointing.
*/
static int shmlockColumn(
  sqlite3_vtab_cursor *cur,   /* The cursor */
  sqlite3_context *ctx,       /* First argument to sqlite3_result_...() */
  int i                       /* Which column to return */
){
  shmlock_cursor *pCur = (shmlock_cursor*)cur;
  if( i<=3 && pCur->azCol[i] ){
    sqlite3_result_text(ctx, pCur->azCol[i], -1, SQLITE_TRANSIENT);
  }
  return SQLITE_OK;
}

/*
** Return the rowid for the current row.  In this implementation, the
** rowid is the same as the output value.
*/
static int shmlockRowid(sqlite3_vtab_cursor *cur, sqlite_int64 *pRowid){
  shmlock_cursor *pCur = (shmlock_cursor*)cur;
  *pRowid = pCur->iRowid;
  return SQLITE_OK;
}

/*
** Return TRUE if the cursor has been moved off of the last
** row of output.
*/
static int shmlockEof(sqlite3_vtab_cursor *cur){
  shmlock_cursor *pCur = (shmlock_cursor*)cur;
  return pCur->azCol[1]==0;
}

/*
** This method is called to "rewind" the shmlock_cursor object back
** to the first row of output.  This method is always called at least
** once prior to any call to shmlockColumn() or shmlockRowid() or 
** shmlockEof().
*/
static int shmlockFilter(
  sqlite3_vtab_cursor *pVtabCursor, 
  int idxNum, const char *idxStr,
  int argc, sqlite3_value **argv
){
  int rc;
  shmlock_cursor *pCur = (shmlock_cursor *)pVtabCursor;
  shmlock_vtab *pTab = (shmlock_vtab*)pVtabCursor->pVtab;
  const char *zDb = "main";

  if( idxNum==1 ){
    assert( argc==1 );
    zDb = (const char*)sqlite3_value_text(argv[0]);
  }
  sqlite3_free(pCur->aSpace);
  pCur->zFcntl = 0;
  pCur->aSpace = 0;
  rc = sqlite3_file_control(
      pTab->db, zDb, SQLITE_FCNTL_SHMLOCK_GET, (void*)&pCur->zFcntl
  );
  pCur->iRowid = 1;
  pCur->iFcntl = 0;
  if( rc==SQLITE_NOTFOUND ){
    rc = SQLITE_OK;
  }
  if( pCur->zFcntl ){
    int n = strlen(pCur->zFcntl);
    pCur->aSpace = sqlite3_malloc(n);
    if( pCur->aSpace==0 ){
      rc = SQLITE_NOMEM;
    }else{
      rc = shmlockNext(pVtabCursor);
    }
  }
  return rc;
}

/*
** SQLite will invoke this method one or more times while planning a query
** that uses the virtual table.  This routine needs to create
** a query plan for each invocation and compute an estimated cost for that
** plan.
*/
static int shmlockBestIndex(
  sqlite3_vtab *tab,
  sqlite3_index_info *pIdxInfo
){
  int ii;

  /* Search for a dbname=? constraint. If one is found, set idxNum=1 and
  ** pass the ? as the only argument to xFilter. Otherwise, leave idxNum=0
  ** and pass no arguments to xFilter.  */
  for(ii=0; ii<pIdxInfo->nConstraint; ii++){
    struct sqlite3_index_constraint *p = &pIdxInfo->aConstraint[ii];
    if( p->usable && p->op==SQLITE_INDEX_CONSTRAINT_EQ && p->iColumn==4 ){
      pIdxInfo->aConstraintUsage[ii].argvIndex = 1;
      pIdxInfo->aConstraintUsage[ii].omit = 1;
      pIdxInfo->idxNum = 1;
      break;
    }
  }

  pIdxInfo->estimatedCost = (double)10;
  pIdxInfo->estimatedRows = 10;
  return SQLITE_OK;
}

/*
** This following structure defines all the methods for the 
** virtual table.
*/
static sqlite3_module shmlockModule = {
  /* iVersion    */ 0,
  /* xCreate     */ 0,
  /* xConnect    */ shmlockConnect,
  /* xBestIndex  */ shmlockBestIndex,
  /* xDisconnect */ shmlockDisconnect,
  /* xDestroy    */ 0,
  /* xOpen       */ shmlockOpen,
  /* xClose      */ shmlockClose,
  /* xFilter     */ shmlockFilter,
  /* xNext       */ shmlockNext,
  /* xEof        */ shmlockEof,
  /* xColumn     */ shmlockColumn,
  /* xRowid      */ shmlockRowid,
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
int sqlite3_shmlockvtab_init(
  sqlite3 *db, 
  char **pzErrMsg, 
  const sqlite3_api_routines *pApi
){
  int rc = SQLITE_OK;
  SQLITE_EXTENSION_INIT2(pApi);
  rc = sqlite3_create_module(db, "shmlock", &shmlockModule, 0);
  return rc;
}

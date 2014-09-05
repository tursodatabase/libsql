/*
** 2014 August 30
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
*************************************************************************
*/

#include <assert.h>
#include <string.h>
#include <stdio.h>
#include <unistd.h>

#include "sqlite3.h"
#include "sqlite3ota.h"


/*
** The ota_state table is used to save the state of a partially applied
** update so that it can be resumed later. The table contains at most a
** single row:
**
**   "tbl"       -> Table currently being written (target database names).
**
**   "idx"       -> Index currently being written (target database names).
**                  Or, if the main table is being written, a NULL value.
**
**   "row"       -> Number of rows for this object already processed
**
**   "progress"  -> total number of key/value b-tree operations performed
**                  so far as part of this ota update.
*/
#define OTA_CREATE_STATE "CREATE TABLE IF NOT EXISTS ota.ota_state"        \
                             "(tbl, idx, row, progress)"

typedef struct OtaState OtaState;
typedef struct OtaObjIter OtaObjIter;
typedef unsigned char u8;

/*
** A structure to store values read from the ota_state table in memory.
*/
struct OtaState {
  char *zTbl;
  char *zIdx;
  int nRow;
};

/*
** An iterator of this type is used to iterate through all objects in
** the target database that require updating. For each such table, the
** iterator visits, in order:
**
**     * the table itself, 
**     * each index of the table (zero or more points to visit), and
**     * a special "cleanup table" point.
*/
struct OtaObjIter {
  sqlite3_stmt *pTblIter;         /* Iterate through tables */
  sqlite3_stmt *pIdxIter;         /* Index iterator */
  int nTblCol;                    /* Size of azTblCol[] array */
  char **azTblCol;                /* Array of quoted column names */
  u8 *abTblPk;                    /* Array of flags - true for PK columns */

  /* Output variables. zTbl==0 implies EOF. */
  int bCleanup;                   /* True in "cleanup" state */
  const char *zTbl;               /* Name of target db table */
  const char *zIdx;               /* Name of target db index (or null) */
  int iVisit;                     /* Number of points visited, incl. current */

  /* Statements created by otaObjIterPrepareAll() */
  int nCol;                       /* Number of columns in current object */
  sqlite3_stmt *pSelect;          /* Source data */
  sqlite3_stmt *pInsert;          /* Statement for INSERT operations */
};

/*
** OTA handle.
*/
struct sqlite3ota {
  sqlite3 *db;                    /* "main" -> target db, "ota" -> ota db */
  char *zTarget;                  /* Path to target db */
  int rc;                         /* Value returned by last ota_step() call */
  char *zErrmsg;                  /* Error message if rc!=SQLITE_OK */
  int nStep;                      /* Rows processed for current object */
  OtaObjIter objiter;
};

/*
** Prepare the SQL statement in buffer zSql against database handle db.
** If successful, set *ppStmt to point to the new statement and return
** SQLITE_OK. 
**
** Otherwise, if an error does occur, set *ppStmt to NULL and return
** an SQLite error code. Additionally, set output variable *pzErrmsg to
** point to a buffer containing an error message. It is the responsibility
** of the caller to (eventually) free this buffer using sqlite3_free().
*/
static int prepareAndCollectError(
  sqlite3 *db, 
  sqlite3_stmt **ppStmt,
  char **pzErrmsg,
  const char *zSql
){
  int rc = sqlite3_prepare_v2(db, zSql, -1, ppStmt, 0);
  if( rc!=SQLITE_OK ){
    *pzErrmsg = sqlite3_mprintf("%s", sqlite3_errmsg(db));
    *ppStmt = 0;
  }
  return rc;
}

/*
** Reset the SQL statement passed as the first argument. Return a copy
** of the value returned by sqlite3_reset().
**
** If an error has occurred, then set *pzErrmsg to point to a buffer
** containing an error message. It is the responsibility of the caller
** to eventually free this buffer using sqlite3_free().
*/
static int resetAndCollectError(sqlite3_stmt *pStmt, char **pzErrmsg){
  int rc = sqlite3_reset(pStmt);
  if( rc!=SQLITE_OK ){
    *pzErrmsg = sqlite3_mprintf("%s", sqlite3_errmsg(sqlite3_db_handle(pStmt)));
  }
  return rc;
}

/*
** Unless it is NULL, argument zSql points to a buffer allocated using
** sqlite3_malloc containing an SQL statement. This function prepares the SQL
** statement against database db and frees the buffer. If statement 
** compilation is successful, *ppStmt is set to point to the new statement 
** handle and SQLITE_OK is returned. 
**
** Otherwise, if an error occurs, *ppStmt is set to NULL and an error code
** returned. In this case, *pzErrmsg may also be set to point to an error
** message. It is the responsibility of the caller to free this error message
** buffer using sqlite3_free().
**
** If argument zSql is NULL, this function assumes that an OOM has occurred.
** In this case SQLITE_NOMEM is returned and *ppStmt set to NULL.
*/
static int prepareFreeAndCollectError(
  sqlite3 *db, 
  sqlite3_stmt **ppStmt,
  char **pzErrmsg,
  char *zSql
){
  int rc;
  assert( *pzErrmsg==0 );
  if( zSql==0 ){
    rc = SQLITE_NOMEM;
    *ppStmt = 0;
  }else{
    rc = prepareAndCollectError(db, ppStmt, pzErrmsg, zSql);
    sqlite3_free(zSql);
  }
  return rc;
}

/*
** Free the OtaObjIter.azTblCol[] and OtaObjIter.abTblPk[] arrays allocated
** by an earlier call to otaObjIterGetCols().
*/
static void otaObjIterFreeCols(OtaObjIter *pIter){
  int i;
  for(i=0; i<pIter->nTblCol; i++){
    sqlite3_free(pIter->azTblCol[i]);
  }
  sqlite3_free(pIter->azTblCol);
  sqlite3_free(pIter->abTblPk);
  pIter->azTblCol = 0;
  pIter->abTblPk = 0;
  pIter->nTblCol = 0;
}

/*
** Clean up any resources allocated as part of the iterator object passed
** as the only argument.
*/
static void otaObjIterFinalize(OtaObjIter *pIter){
  sqlite3_finalize(pIter->pTblIter);
  sqlite3_finalize(pIter->pIdxIter);
  sqlite3_finalize(pIter->pSelect);
  sqlite3_finalize(pIter->pInsert);
  otaObjIterFreeCols(pIter);
  memset(pIter, 0, sizeof(OtaObjIter));
}

/*
** Advance the iterator to the next position.
**
** If no error occurs, SQLITE_OK is returned and the iterator is left 
** pointing to the next entry. Otherwise, an error code and message is 
** left in the OTA handle passed as the first argument. A copy of the 
** error code is returned.
*/
static int otaObjIterNext(sqlite3ota *p, OtaObjIter *pIter){
  int rc = p->rc;
  if( rc==SQLITE_OK ){

    /* Free any SQLite statements used while processing the previous object */ 
    sqlite3_finalize(pIter->pSelect);
    sqlite3_finalize(pIter->pInsert);
    pIter->pSelect = 0;
    pIter->pInsert = 0;
    pIter->nCol = 0;

    if( pIter->bCleanup ){
      otaObjIterFreeCols(pIter);
      pIter->bCleanup = 0;
      rc = sqlite3_step(pIter->pTblIter);
      if( rc!=SQLITE_ROW ){
        rc = sqlite3_reset(pIter->pTblIter);
        pIter->zTbl = 0;
      }else{
        pIter->zTbl = (const char*)sqlite3_column_text(pIter->pTblIter, 0);
        rc = SQLITE_OK;
      }
    }else{
      if( pIter->zIdx==0 ){
        sqlite3_bind_text(pIter->pIdxIter, 1, pIter->zTbl, -1, SQLITE_STATIC);
      }
      rc = sqlite3_step(pIter->pIdxIter);
      if( rc!=SQLITE_ROW ){
        rc = sqlite3_reset(pIter->pIdxIter);
        pIter->bCleanup = 1;
        pIter->zIdx = 0;
      }else{
        pIter->zIdx = (const char*)sqlite3_column_text(pIter->pIdxIter, 0);
        rc = SQLITE_OK;
      }
    }
  }

  if( rc!=SQLITE_OK ){
    otaObjIterFinalize(pIter);
    p->rc = rc;
  }
  pIter->iVisit++;
  return rc;
}

/*
** Initialize the iterator structure passed as the second argument.
**
** If no error occurs, SQLITE_OK is returned and the iterator is left 
** pointing to the first entry. Otherwise, an error code and message is 
** left in the OTA handle passed as the first argument. A copy of the 
** error code is returned.
*/
static int otaObjIterFirst(sqlite3ota *p, OtaObjIter *pIter){
  int rc;
  memset(pIter, 0, sizeof(OtaObjIter));

  rc = prepareAndCollectError(p->db, &pIter->pTblIter, &p->zErrmsg, 
      "SELECT substr(name, 6) FROM ota.sqlite_master "
      "WHERE type='table' AND name LIKE 'data_%'"
  );

  if( rc==SQLITE_OK ){
    rc = prepareAndCollectError(p->db, &pIter->pIdxIter, &p->zErrmsg,
        "SELECT name FROM main.sqlite_master "
        "WHERE type='index' AND tbl_name = ?"
    );
  }

  pIter->bCleanup = 1;
  p->rc = rc;
  return otaObjIterNext(p, pIter);
}

/*
** Allocate a buffer and populate it with the double-quoted version of the
** string in the argument buffer, suitable for use as an SQL identifier. 
** For example:
**
**      [quick "brown" fox]    ->    ["quick ""brown"" fox"]
**
** Assuming the allocation is successful, a pointer to the new buffer is 
** returned. It is the responsibility of the caller to free it using 
** sqlite3_free() at some point in the future. Or, if the allocation fails,
** a NULL pointer is returned.
*/
static char *otaQuoteName(const char *zName){
  int nName = strlen(zName);
  char *zRet = sqlite3_malloc(nName * 2 + 2 + 1);
  if( zRet ){
    int i;
    char *p = zRet;
    *p++ = '"';
    for(i=0; i<nName; i++){
      if( zName[i]=='"' ) *p++ = '"';
      *p++ = zName[i];
    }
    *p++ = '"';
    *p++ = '\0';
  }
  return zRet;
}

/*
** If they are not already populated, populate the pIter->azTblCol[],
** pIter->abTblPk[] and pIter->nTblCol variables according to the table 
** that the iterator currently points to.
**
** Return SQLITE_OK if successful, or an SQLite error code otherwise. If
** an error does occur, an error code and error message are also left in 
** the OTA handle.
*/
static int otaObjIterGetCols(sqlite3ota *p, OtaObjIter *pIter){
  if( pIter->azTblCol==0 ){
    sqlite3_stmt *pStmt;
    char *zSql;
    int nCol = 0;
    int bSeenPk = 0;
    int rc2;                      /* sqlite3_finalize() return value */

    zSql = sqlite3_mprintf("PRAGMA main.table_info(%Q)", pIter->zTbl);
    p->rc = prepareFreeAndCollectError(p->db, &pStmt, &p->zErrmsg, zSql);
    while( p->rc==SQLITE_OK && SQLITE_ROW==sqlite3_step(pStmt) ){
      if( (nCol % 8)==0 ){
        int nByte = sizeof(char*) * (nCol+8);
        char **azNew = (char**)sqlite3_realloc(pIter->azTblCol, nByte);
        u8 *abNew = (u8*)sqlite3_realloc(pIter->azTblCol, nCol+8);

        if( azNew ) pIter->azTblCol = azNew;
        if( abNew ) pIter->abTblPk = abNew;
        if( azNew==0 || abNew==0 ) p->rc = SQLITE_NOMEM;
      }

      if( p->rc==SQLITE_OK ){
        const char *zName = (const char*)sqlite3_column_text(pStmt, 1);
        pIter->abTblPk[nCol] = sqlite3_column_int(pStmt, 5);
        if( pIter->abTblPk[nCol] ) bSeenPk = 1;
        pIter->azTblCol[nCol] = otaQuoteName(zName);
        if( pIter->azTblCol[nCol]==0 ) p->rc = SQLITE_NOMEM;
        nCol++;
      }
    }
    pIter->nTblCol = nCol;
    rc2 = sqlite3_finalize(pStmt);
    if( p->rc==SQLITE_OK ) p->rc = rc2;

    if( p->rc==SQLITE_OK && bSeenPk==0 ){
      p->zErrmsg = sqlite3_mprintf("table %s has no PRIMARY KEY", pIter->zTbl);
      p->rc = SQLITE_ERROR;
    }
  }

  return p->rc;
}

static char *otaObjIterGetCollist(
  sqlite3ota *p, 
  OtaObjIter *pIter, 
  int nCol, 
  int *aiCol
){
  char *zList = 0;
  if( p->rc==SQLITE_OK ){
    const char *zSep = "";
    int i;
    for(i=0; i<nCol; i++){
      int iCol = aiCol ? aiCol[i] : i;
      zList = sqlite3_mprintf("%z%s%s", zList, zSep, pIter->azTblCol[iCol]);
      zSep = ", ";
      if( zList==0 ){
        p->rc = SQLITE_NOMEM;
        break;
      }
    }
  }
  return zList;
}

static char *otaObjIterGetBindlist(sqlite3ota *p, int nBind){
  char *zRet = 0;
  if( p->rc==SQLITE_OK ){
    int nByte = nBind*2 + 1;
    zRet = sqlite3_malloc(nByte);
    if( zRet==0 ){
      p->rc = SQLITE_NOMEM;
    }else{
      int i;
      for(i=0; i<nBind; i++){
        zRet[i*2] = '?';
        zRet[i*2+1] = (i+1==nBind) ? '\0' : ',';
      }
    }
  }
  return zRet;
}

/*
** Ensure that the SQLite statement handles required to update the 
** target database object currently indicated by the iterator passed 
** as the second argument are available.
*/
static int otaObjIterPrepareAll(
  sqlite3ota *p, 
  OtaObjIter *pIter,
  int nOffset                     /* Add "LIMIT -1 OFFSET $nOffset" to SELECT */
){
  assert( pIter->bCleanup==0 );
  if( pIter->pSelect==0 && otaObjIterGetCols(p, pIter)==SQLITE_OK ){
    char *zCollist = 0;           /* List of indexed columns */
    char **pz = &p->zErrmsg;
    const char *zIdx = pIter->zIdx;
    char *zLimit = 0;

    if( nOffset ){
      zLimit = sqlite3_mprintf(" LIMIT -1 OFFSET %d", nOffset);
      if( !zLimit ) p->rc = SQLITE_NOMEM;
    }

    if( zIdx ){
      int *aiCol;                 /* Column map */

      /* Create the index writer */
      if( p->rc==SQLITE_OK ){
        p->rc = sqlite3_index_writer(
            p->db, 0, zIdx, &pIter->pInsert, &aiCol, &pIter->nCol
        );
      }

      /* Create the SELECT statement to read keys in sorted order */
      zCollist = otaObjIterGetCollist(p, pIter, pIter->nCol, aiCol);
      if( p->rc==SQLITE_OK ){
        p->rc = prepareFreeAndCollectError(p->db, &pIter->pSelect, pz,
            sqlite3_mprintf(
              "SELECT %s FROM ota.'data_%q' ORDER BY %s%s",
              zCollist, pIter->zTbl, zCollist, zLimit
            )
        );
      }
    }else{
      char *zBindings = otaObjIterGetBindlist(p, pIter->nTblCol);
      zCollist = otaObjIterGetCollist(p, pIter, pIter->nTblCol, 0);
      pIter->nCol = pIter->nTblCol;

      /* Create the SELECT statement to read keys from data_xxx */
      if( p->rc==SQLITE_OK ){
        p->rc = prepareFreeAndCollectError(p->db, &pIter->pSelect, pz,
            sqlite3_mprintf(
              "SELECT %s FROM ota.'data_%q'%s", 
              zCollist, pIter->zTbl, zLimit)
        );
      }

      /* Create the INSERT statement to write to the target PK b-tree */
      if( p->rc==SQLITE_OK ){
        p->rc = prepareFreeAndCollectError(p->db, &pIter->pInsert, pz,
            sqlite3_mprintf(
              "INSERT INTO main.%Q(%s) VALUES(%s)", 
              pIter->zTbl, zCollist, zBindings
            )
        );
      }
      sqlite3_free(zBindings);
    }
    sqlite3_free(zCollist);
    sqlite3_free(zLimit);
  }
  
  return p->rc;
}

/*
** This function does the work for an sqlite3ota_step() call.
**
** The object-iterator (p->objiter) currently points to a valid object,
** and the input cursor (p->objiter.pSelect) currently points to a valid
** input row. Perform whatever processing is required and return.
**
** If no  error occurs, SQLITE_OK is returned. Otherwise, an error code
** and message is left in the OTA handle and a copy of the error code
** returned.
*/
static int otaStep(sqlite3ota *p){
  OtaObjIter *pIter = &p->objiter;
  int i;

  for(i=0; i<pIter->nCol; i++){
    sqlite3_value *pVal = sqlite3_column_value(pIter->pSelect, i);
    sqlite3_bind_value(pIter->pInsert, i+1, pVal);
  }

  sqlite3_step(pIter->pInsert);
  p->rc = resetAndCollectError(pIter->pInsert, &p->zErrmsg);
  return p->rc;
}

/*
** Step the OTA object.
*/
int sqlite3ota_step(sqlite3ota *p){
  if( p ){
    OtaObjIter *pIter = &p->objiter;
    while( p && p->rc==SQLITE_OK && pIter->zTbl ){

      if( pIter->bCleanup ){
        /* this is where cleanup of the ota_xxx table will happen... */
      }else{
        otaObjIterPrepareAll(p, pIter, 0);
        
        /* Advance to the next row to process. */
        if( p->rc==SQLITE_OK ){
          int rc = sqlite3_step(pIter->pSelect);
          if( rc==SQLITE_ROW ){
            p->nStep++;
            return otaStep(p);
          }
          p->rc = sqlite3_reset(pIter->pSelect);
          p->nStep = 0;
        }
      }

      otaObjIterNext(p, pIter);
    }

    if( p->rc==SQLITE_OK && pIter->zTbl==0 ){
      p->rc = SQLITE_DONE;
    }
  }
  return p->rc;
}

/*
** Argument zFmt is a sqlite3_mprintf() style format string. The trailing
** arguments are the usual subsitution values. This function performs
** the printf() style substitutions and executes the result as an SQL
** statement on the OTA handles database.
**
** If an error occurs, an error code and error message is stored in the
** OTA handle. If an error has already occurred when this function is
** called, it is a no-op.
*/
static int otaMPrintfExec(sqlite3ota *p, const char *zFmt, ...){
  va_list ap;
  va_start(ap, zFmt);
  if( p->rc==SQLITE_OK ){
    char *zSql = sqlite3_vmprintf(zFmt, ap);
    if( zSql==0 ){
      p->rc = SQLITE_NOMEM;
    }else{
      p->rc = sqlite3_exec(p->db, zSql, 0, 0, &p->zErrmsg);
      sqlite3_free(zSql);
    }
  }
  va_end(ap);
  return p->rc;
}

static void otaSaveTransactionState(sqlite3ota *p){
  otaMPrintfExec(p, 
    "INSERT OR REPLACE INTO ota.ota_state(rowid, tbl, idx, row, progress)"
    "VALUES(1, %Q, %Q, %d, NULL)",
    p->objiter.zTbl, p->objiter.zIdx, p->nStep
  );
}

/*
** Allocate an OtaState object and load the contents of the ota_state 
** table into it. Return a pointer to the new object. It is the 
** responsibility of the caller to eventually free the object using
** sqlite3_free().
**
** If an error occurs, leave an error code and message in the ota handle
** and return NULL.
*/
static OtaState *otaLoadState(sqlite3ota *p){
  const char *zSelect = "SELECT tbl, idx, row, progress FROM ota.ota_state";
  OtaState *pRet = 0;
  sqlite3_stmt *pStmt;
  int rc;

  assert( p->rc==SQLITE_OK );
  rc = prepareAndCollectError(p->db, &pStmt, &p->zErrmsg, zSelect);
  if( rc==SQLITE_OK ){
    if( sqlite3_step(pStmt)==SQLITE_ROW ){
      const char *zIdx = (const char*)sqlite3_column_text(pStmt, 1);
      const char *zTbl = (const char*)sqlite3_column_text(pStmt, 0);
      int nIdx = zIdx ? (strlen(zIdx) + 1) : 0;
      int nTbl = strlen(zTbl) + 1;
      int nByte = sizeof(OtaState) + nTbl + nIdx;

      pRet = (OtaState*)sqlite3_malloc(nByte);
      if( pRet ){
        pRet->zTbl = (char*)&pRet[1];
        memcpy(pRet->zTbl, sqlite3_column_text(pStmt, 0), nTbl);
        if( zIdx ){
          pRet->zIdx = &pRet->zTbl[nTbl];
          memcpy(pRet->zIdx, zIdx, nIdx);
        }else{
          pRet->zIdx = 0;
        }
        pRet->nRow = sqlite3_column_int(pStmt, 2);
      }
    }else{
      pRet = (OtaState*)sqlite3_malloc(sizeof(OtaState));
      if( pRet ){
        memset(pRet, 0, sizeof(*pRet));
      }
    }
    rc = sqlite3_finalize(pStmt);
    if( rc==SQLITE_OK && pRet==0 ) rc = SQLITE_NOMEM;
    if( rc!=SQLITE_OK ){
      sqlite3_free(pRet);
      pRet = 0;
    }
  }

  p->rc = rc;
  return pRet;
}

static int otaStrCompare(const char *z1, const char *z2){
  if( z1==0 && z2==0 ) return 0;
  if( z1==0 || z2==0 ) return 1;
  return (sqlite3_stricmp(z1, z2)!=0);
}

static void otaLoadTransactionState(sqlite3ota *p, OtaState *pState){
  assert( p->rc==SQLITE_OK );
  if( pState->zTbl ){
    OtaObjIter *pIter = &p->objiter;
    int rc;

    while( rc==SQLITE_OK && pIter->zTbl && (pIter->bCleanup 
       || otaStrCompare(pIter->zTbl, pState->zTbl) 
       || otaStrCompare(pIter->zIdx, pState->zIdx)
    )){
      rc = otaObjIterNext(p, &p->objiter);
    }

    if( rc==SQLITE_OK && !p->objiter.zTbl ){
      rc = SQLITE_ERROR;
      p->zErrmsg = sqlite3_mprintf("ota_state mismatch error");
    }

    if( rc==SQLITE_OK ){
      p->nStep = pState->nRow;
      rc = otaObjIterPrepareAll(p, &p->objiter, p->nStep);
    }

    p->rc = rc;
  }
}

/*
** Move the "*-oal" file corresponding to the target database to the
** "*-wal" location. If an error occurs, leave an error code and error 
** message in the ota handle.
*/
static void otaMoveOalFile(sqlite3ota *p){
  char *zWal = sqlite3_mprintf("%s-wal", p->zTarget);
  char *zOal = sqlite3_mprintf("%s-oal", p->zTarget);

  assert( p->rc==SQLITE_DONE && p->zErrmsg==0 );
  if( zWal==0 || zOal==0 ){
    p->rc = SQLITE_NOMEM;
  }else{
    rename(zOal, zWal);
  }

  sqlite3_free(zWal);
  sqlite3_free(zOal);
}

/*
** If there is a "*-oal" file in the file-system corresponding to the
** target database in the file-system, delete it. If an error occurs,
** leave an error code and error message in the ota handle.
*/
static void otaDeleteOalFile(sqlite3ota *p){
  char *zOal = sqlite3_mprintf("%s-oal", p->zTarget);
  assert( p->rc==SQLITE_OK && p->zErrmsg==0 );
  unlink(zOal);
  sqlite3_free(zOal);
}

/*
** Open and return a new OTA handle. 
*/
sqlite3ota *sqlite3ota_open(const char *zTarget, const char *zOta){
  sqlite3ota *p;
  int nTarget = strlen(zTarget);

  p = (sqlite3ota*)sqlite3_malloc(sizeof(sqlite3ota)+nTarget+1);
  if( p ){
    OtaState *pState = 0;

    /* Open the target database */
    memset(p, 0, sizeof(sqlite3ota));
    p->zTarget = (char*)&p[1];
    memcpy(p->zTarget, zTarget, nTarget+1);
    p->rc = sqlite3_open(zTarget, &p->db);
    if( p->rc ){
      p->zErrmsg = sqlite3_mprintf("%s", sqlite3_errmsg(p->db));
    }
    otaMPrintfExec(p, "ATTACH %Q AS ota", zOta);

    /* If it has not already been created, create the ota_state table */
    if( p->rc==SQLITE_OK ){
      p->rc = sqlite3_exec(p->db, OTA_CREATE_STATE, 0, 0, &p->zErrmsg);
    }

    if( p->rc==SQLITE_OK ){
      pState = otaLoadState(p);
      if( pState && pState->zTbl==0 ){
        otaDeleteOalFile(p);
      }
    }

    if( p->rc==SQLITE_OK ){
      const char *zScript =
        "PRAGMA journal_mode=off;"
        "PRAGMA pager_ota_mode=1;"
        "PRAGMA ota_mode=1;"
        "BEGIN IMMEDIATE;"
      ;
      p->rc = sqlite3_exec(p->db, zScript, 0, 0, &p->zErrmsg);
    }

    /* Point the object iterator at the first object */
    if( p->rc==SQLITE_OK ){
      p->rc = otaObjIterFirst(p, &p->objiter);
    }

    if( p->rc==SQLITE_OK ){
      otaLoadTransactionState(p, pState);
    }

    sqlite3_free(pState);
  }

  return p;
}

/*
** Close the OTA handle.
*/
int sqlite3ota_close(sqlite3ota *p, char **pzErrmsg){
  int rc;
  if( p ){

    /* If the update has not been fully applied, save the state in 
    ** the ota db. If successful, this call also commits the open 
    ** transaction on the ota db. */
    assert( p->rc!=SQLITE_ROW );
    if( p->rc==SQLITE_OK ){
      assert( p->zErrmsg==0 );
      otaSaveTransactionState(p);
    }

    /* Close all open statement handles. */
    otaObjIterFinalize(&p->objiter);

    /* Commit the transaction to the *-oal file. */
    if( p->rc==SQLITE_OK || p->rc==SQLITE_DONE ){
      rc = sqlite3_exec(p->db, "COMMIT", 0, 0, &p->zErrmsg);
      if( rc!=SQLITE_OK ) p->rc = rc;
    }

    /* Close the open database handles */
    sqlite3_close(p->db);

    /* If the OTA has been completely applied and no error occurred, move
    ** the *-oal file to *-wal. */
    if( p->rc==SQLITE_DONE ){
      otaMoveOalFile(p);
    }

    rc = p->rc;
    *pzErrmsg = p->zErrmsg;
    sqlite3_free(p);
  }else{
    rc = SQLITE_NOMEM;
    *pzErrmsg = 0;
  }
  return rc;
}


/**************************************************************************/

#ifdef SQLITE_TEST 

#include <tcl.h>

/* From main.c (apparently...) */
extern const char *sqlite3ErrName(int);

static int test_sqlite3ota_cmd(
  ClientData clientData,
  Tcl_Interp *interp,
  int objc,
  Tcl_Obj *CONST objv[]
){
  int ret = TCL_OK;
  sqlite3ota *pOta = (sqlite3ota*)clientData;
  const char *azMethod[] = { "step", "close", 0 };
  int iMethod;

  if( objc!=2 ){
    Tcl_WrongNumArgs(interp, 1, objv, "METHOD");
    return TCL_ERROR;
  }
  if( Tcl_GetIndexFromObj(interp, objv[1], azMethod, "method", 0, &iMethod) ){
    return TCL_ERROR;
  }

  switch( iMethod ){
    case 0: /* step */ {
      int rc = sqlite3ota_step(pOta);
      Tcl_SetObjResult(interp, Tcl_NewStringObj(sqlite3ErrName(rc), -1));
      break;
    }

    case 1: /* close */ {
      char *zErrmsg = 0;
      int rc;
      Tcl_DeleteCommand(interp, Tcl_GetString(objv[0]));
      rc = sqlite3ota_close(pOta, &zErrmsg);
      if( rc==SQLITE_OK || rc==SQLITE_DONE ){
        Tcl_SetObjResult(interp, Tcl_NewStringObj(sqlite3ErrName(rc), -1));
        assert( zErrmsg==0 );
      }else{
        Tcl_SetObjResult(interp, Tcl_NewStringObj(sqlite3ErrName(rc), -1));
        if( zErrmsg ){
          Tcl_AppendResult(interp, " - ", zErrmsg, 0);
          sqlite3_free(zErrmsg);
        }
        ret = TCL_ERROR;
      }
      break;
    }

    default: /* seems unlikely */
      assert( !"cannot happen" );
      break;
  }

  return ret;
}

/*
** Tclcmd: sqlite3ota CMD <target-db> <ota-db>
*/
static int test_sqlite3ota(
  ClientData clientData,
  Tcl_Interp *interp,
  int objc,
  Tcl_Obj *CONST objv[]
){
  sqlite3ota *pOta = 0;
  const char *zCmd;
  const char *zTarget;
  const char *zOta;

  if( objc!=4 ){
    Tcl_WrongNumArgs(interp, 1, objv, "NAME TARGET-DB OTA-DB");
    return TCL_ERROR;
  }
  zCmd = Tcl_GetString(objv[1]);
  zTarget = Tcl_GetString(objv[2]);
  zOta = Tcl_GetString(objv[3]);

  pOta = sqlite3ota_open(zTarget, zOta);
  Tcl_CreateObjCommand(interp, zCmd, test_sqlite3ota_cmd, (ClientData)pOta, 0);
  Tcl_SetObjResult(interp, objv[1]);
  return TCL_OK;
}

int SqliteOta_Init(Tcl_Interp *interp){ 
  Tcl_CreateObjCommand(interp, "sqlite3ota", test_sqlite3ota, 0, 0);
  return TCL_OK;
}

#endif                  /* ifdef SQLITE_TEST */




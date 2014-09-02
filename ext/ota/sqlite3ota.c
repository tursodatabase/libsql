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

#include "sqlite3.h"
#include "sqlite3ota.h"


/*
** The ota_state table is used to save the state of a partially applied
** update so that it can be resumed later. The table contains at most a
** single row:
**
**   "wal_state" -> Blob to use with sqlite3_transaction_restore().
**
**   "tbl"       -> Table currently being written (target database names).
**
**   "idx"       -> Index currently being written (target database names).
**                  Or, if the main table is being written, a NULL value.
**
**   "row"       -> Last rowid processed from ota database table (i.e. data_%).
**
**   "progress"  -> total number of key/value b-tree operations performed
**                  so far as part of this ota update.
*/
#define OTA_CREATE_STATE "CREATE TABLE IF NOT EXISTS ota_state"        \
                             "(wal_state, tbl, idx, row, progress)"

typedef struct OtaTblIter OtaTblIter;
typedef struct OtaIdxIter OtaIdxIter;

/*
** Iterator used to iterate through all data tables in the OTA. As follows:
**
**   OtaTblIter iter;
**   for(rc=tblIterFirst(db, &iter); 
**       rc==SQLITE_OK && iter.zTarget; 
**       rc=tblIterNext(&iter)
**   ){
**   }
*/
struct OtaTblIter {
  sqlite3_stmt *pTblIter;         /* Iterate through tables */
  int iEntry;                     /* Index of current entry (from 1) */

  /* Output varibles. zTarget==0 implies EOF. */
  const char *zTarget;            /* Name of target table */
  const char *zSource;            /* Name of source table */

  /* Useful things populated by a call to tblIterPrepareAll() */
  int nCol;                       /* Number of columns in this table */
  char **azCol;                   /* Array of quoted column names */
  sqlite3_stmt *pSelect;          /* PK b-tree SELECT statement */
  sqlite3_stmt *pInsert;          /* PK b-tree INSERT statement */
};

/*
** API is:
**
**     idxIterFirst()
**     idxIterNext()
**     idxIterFinalize()
**     idxIterPrepareAll()
*/
struct OtaIdxIter {
  sqlite3_stmt *pIdxIter;         /* Iterate through indexes */
  int iEntry;                     /* Index of current entry (from 1) */

  /* Output varibles. zTarget==0 implies EOF. */
  const char *zIndex;             /* Name of index */

  int nCol;                       /* Number of columns in index */
  int *aiCol;                     /* Array of column indexes */
  sqlite3_stmt *pWriter;          /* Index writer */
  sqlite3_stmt *pSelect;          /* Select to read values in index order */
};


struct sqlite3ota {
  sqlite3 *dbDest;                /* Target db */
  sqlite3 *dbOta;                 /* Ota db */

  int rc;                         /* Value returned by last ota_step() call */
  char *zErrmsg;                  /* Error message if rc!=SQLITE_OK */

  OtaTblIter tbliter;             /* Used to iterate through tables */
  OtaIdxIter idxiter;             /* Used to iterate through indexes */
};

static int prepareAndCollectError(
  sqlite3 *db, 
  const char *zSql, 
  sqlite3_stmt **ppStmt,
  char **pzErrmsg
){
  int rc = sqlite3_prepare_v2(db, zSql, -1, ppStmt, 0);
  if( rc!=SQLITE_OK ){
    *pzErrmsg = sqlite3_mprintf("%s", sqlite3_errmsg(db));
    *ppStmt = 0;
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
  char *zSql, 
  sqlite3_stmt **ppStmt,
  char **pzErrmsg
){
  int rc;
  assert( *pzErrmsg==0 );
  if( zSql==0 ){
    rc = SQLITE_NOMEM;
    *ppStmt = 0;
  }else{
    rc = prepareAndCollectError(db, zSql, ppStmt, pzErrmsg);
    sqlite3_free(zSql);
  }
  return rc;
}

static char *quoteSqlName(const char *zName){
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

static int tblIterPrepareAll(sqlite3ota *p){
  OtaTblIter *pIter = &p->tbliter;
  int rc = SQLITE_OK;
  char *zCol = 0;
  char *zBindings = 0;
  char *zSql;
  sqlite3_stmt *pPragma = 0;
  int i;
  int bSeenPk = 0;                /* Set to true once PK column seen */

  /* Allocate and populate the azCol[] array */
  zSql = sqlite3_mprintf("PRAGMA main.table_info(%Q)", pIter->zTarget);
  rc = prepareFreeAndCollectError(p->dbDest, zSql, &pPragma, &p->zErrmsg);
  pIter->nCol = 0;
  if( rc==SQLITE_OK ){
    while( SQLITE_ROW==sqlite3_step(pPragma) ){
      const char *zName = (const char*)sqlite3_column_text(pPragma, 1);
      if( (pIter->nCol % 4)==0 ){
        int nByte = sizeof(char*) * (pIter->nCol+4);
        char **azNew = (char**)sqlite3_realloc(pIter->azCol, nByte);
        if( azNew==0 ){
          rc = SQLITE_NOMEM;
          break;
        }
        pIter->azCol = azNew;
      }
      pIter->azCol[pIter->nCol] = quoteSqlName(zName);
      if( pIter->azCol[pIter->nCol]==0 ){
        rc = SQLITE_NOMEM;
        break;
      }
      pIter->nCol++;
      if( sqlite3_column_int(pPragma, 5) ) bSeenPk = 1;
    }
    if( rc==SQLITE_OK ){
      rc = sqlite3_finalize(pPragma);
    }else{
      sqlite3_finalize(pPragma);
    }
  }

  /* If the table has no PRIMARY KEY, throw an exception. */
  if( bSeenPk==0 ){
    p->zErrmsg = sqlite3_mprintf("table %s has no PRIMARY KEY", pIter->zTarget);
    rc = SQLITE_ERROR;
  }

  /* Populate the zCol variable */
  for(i=0; rc==SQLITE_OK && i<pIter->nCol; i++){
    zCol = sqlite3_mprintf("%z%s%s", zCol, (i==0?"":", "), pIter->azCol[i]);
    if( zCol==0 ){
      rc = SQLITE_NOMEM;
    }
  }

  /* Allocate and populate zBindings */
  if( rc==SQLITE_OK ){
    zBindings = (char*)sqlite3_malloc(pIter->nCol * 2);
    if( zBindings==0 ){
      rc = SQLITE_NOMEM;
    }else{
      int i;
      for(i=0; i<pIter->nCol; i++){
        zBindings[i*2] = '?';
        zBindings[i*2+1] = ',';
      }
      zBindings[pIter->nCol*2-1] = '\0';
    }
  }

  /* Create OtaTblIter.pSelect */
  if( rc==SQLITE_OK ){
    zSql = sqlite3_mprintf("SELECT rowid, %s FROM %Q", zCol, pIter->zSource);
    rc = prepareFreeAndCollectError(p->dbOta,zSql,&pIter->pSelect, &p->zErrmsg);
  }

  /* Create OtaTblIter.pInsert */
  if( rc==SQLITE_OK ){
    zSql = sqlite3_mprintf("INSERT INTO %Q(%s) VALUES(%s)", 
        pIter->zTarget, zCol, zBindings
    );
    rc = prepareFreeAndCollectError(p->dbDest,zSql,&pIter->pInsert,&p->zErrmsg);
  }

  sqlite3_free(zCol);
  sqlite3_free(zBindings);
  return rc;
}

static void tblIterFreeAll(OtaTblIter *pIter){
  int i;

  sqlite3_finalize(pIter->pSelect);
  sqlite3_finalize(pIter->pInsert);
  for(i=0; i<pIter->nCol; i++) sqlite3_free(pIter->azCol[i]);
  sqlite3_free(pIter->azCol);
  pIter->azCol = 0;
  pIter->pSelect = 0;
  pIter->pInsert = 0;
  pIter->nCol = 0;
}

static int tblIterNext(OtaTblIter *pIter){
  int rc;

  tblIterFreeAll(pIter);
  assert( pIter->pTblIter );
  rc = sqlite3_step(pIter->pTblIter);
  if( rc==SQLITE_ROW ){
    pIter->zSource = (const char*)sqlite3_column_text(pIter->pTblIter, 0);
    pIter->zTarget = &pIter->zSource[5]; assert( 5==strlen("data_") );
    pIter->iEntry++;
  }else{
    pIter->zSource = 0;
    pIter->zTarget = 0;
  }

  if( rc==SQLITE_ROW || rc==SQLITE_DONE ) rc = SQLITE_OK;
  return rc;
}

static int tblIterFirst(sqlite3 *db, OtaTblIter *pIter){
  int rc;                         /* return code */
  memset(pIter, 0, sizeof(OtaTblIter));
  rc = sqlite3_prepare_v2(db, 
      "SELECT name FROM sqlite_master "
      "WHERE type='table' AND name LIKE 'data_%'", -1, &pIter->pTblIter, 0
  );
  if( rc==SQLITE_OK ){
    rc = tblIterNext(pIter);
  }
  return rc;
}


static void tblIterFinalize(OtaTblIter *pIter){
  tblIterFreeAll(pIter);
  sqlite3_finalize(pIter->pTblIter);
  memset(pIter, 0, sizeof(OtaTblIter));
}

static void idxIterFreeAll(OtaIdxIter *pIter){
  sqlite3_finalize(pIter->pWriter);
  sqlite3_finalize(pIter->pSelect);
  pIter->pWriter = 0;
  pIter->pSelect = 0;
  pIter->aiCol = 0;
  pIter->nCol = 0;
}

static int idxIterPrepareAll(sqlite3ota *p){
  int rc;
  int i;                          /* Iterator variable */
  char *zSql = 0;
  char *zCols = 0;                /* Columns list */
  OtaIdxIter *pIter = &p->idxiter;

  /* Prepare the writer statement to write (insert) entries into the index. */
  rc = sqlite3_index_writer(
      p->dbDest, 0, pIter->zIndex, &pIter->pWriter, &pIter->aiCol, &pIter->nCol
  );

  /* Prepare a SELECT statement to read values from the source table in 
  ** the same order as they are stored in the current index. The statement 
  ** is:
  **
  **     SELECT rowid, <cols> FROM data_<tbl> ORDER BY <cols>
  */
  for(i=0; rc==SQLITE_OK && i<pIter->nCol; i++){
    const char *zQuoted = p->tbliter.azCol[ pIter->aiCol[i] ];
    zCols = sqlite3_mprintf("%z%s%s", zCols, zCols?", ":"", zQuoted);
    if( !zCols ){
      rc = SQLITE_NOMEM;
    }
  }
  if( rc==SQLITE_OK ){
    const char *zFmt = "SELECT rowid, %s FROM %Q ORDER BY %s";
    zSql = sqlite3_mprintf(zFmt, zCols, p->tbliter.zSource, zCols);
    if( zSql ){
      sqlite3_stmt **pp = &p->idxiter.pSelect;
      rc = prepareFreeAndCollectError(p->dbOta, zSql, pp, &p->zErrmsg);
    }else{
      rc = SQLITE_NOMEM;
    }
  }

  sqlite3_free(zCols);
  return rc;
}

static int idxIterNext(OtaIdxIter *pIter){
  int rc;

  idxIterFreeAll(pIter);
  assert( pIter->pIdxIter );
  rc = sqlite3_step(pIter->pIdxIter);
  if( rc==SQLITE_ROW ){
    pIter->zIndex = (const char*)sqlite3_column_text(pIter->pIdxIter, 0);
    pIter->iEntry++;
  }else{
    pIter->zIndex = 0;
    rc = sqlite3_finalize(pIter->pIdxIter);
    pIter->pIdxIter = 0;
  }

  if( rc==SQLITE_ROW ) rc = SQLITE_OK;
  return rc;
}

static int idxIterFirst(sqlite3 *db, const char *zTable, OtaIdxIter *pIter){
  int rc;                         /* return code */
  memset(pIter, 0, sizeof(OtaIdxIter));
  rc = sqlite3_prepare_v2(db, 
      "SELECT name FROM sqlite_master "
      "WHERE type='index' AND tbl_name = ?", -1, &pIter->pIdxIter, 0
  );
  if( rc==SQLITE_OK ){
    rc = sqlite3_bind_text(pIter->pIdxIter, 1, zTable, -1, SQLITE_TRANSIENT);
  }
  if( rc==SQLITE_OK ){
    rc = idxIterNext(pIter);
  }
  return rc;
}

static void idxIterFinalize(OtaIdxIter *pIter){
  idxIterFreeAll(pIter);
  sqlite3_finalize(pIter->pIdxIter);
  memset(pIter, 0, sizeof(OtaIdxIter));
}

/*
** Call sqlite3_reset() on the SQL statement passed as the second argument.
** If it returns anything other than SQLITE_OK, store the error code and
** error message in the OTA handle.
*/
static void otaResetStatement(sqlite3ota *p, sqlite3_stmt *pStmt){
  assert( p->rc==SQLITE_OK );
  assert( p->zErrmsg==0 );
  p->rc = sqlite3_reset(pStmt);
  if( p->rc!=SQLITE_OK ){
    sqlite3 *db = sqlite3_db_handle(pStmt);
    p->zErrmsg = sqlite3_mprintf("%s", sqlite3_errmsg(db));
  }
}

/* 
** Check that all SQL statements required to process the current 
** table and index have been prepared. If not, prepare them. If
** an error occurs, store the error code and message in the OTA
** handle before returning.
*/
static int otaPrepareAll(sqlite3ota *p){
  assert( p->rc==SQLITE_OK );
  assert( p->zErrmsg==0 );
  assert( p->tbliter.zTarget );

  if( p->tbliter.pSelect==0 ){
    p->rc = tblIterPrepareAll(p);
  }
  if( p->rc==SQLITE_OK && p->idxiter.zIndex && 0==p->idxiter.pSelect ){
    p->rc = idxIterPrepareAll(p);
  }
  return p->rc;
}

int sqlite3ota_step(sqlite3ota *p){
  if( p ){
    while( p && p->rc==SQLITE_OK && p->tbliter.zTarget ){
      sqlite3_stmt *pSelect;
      int i;

      otaPrepareAll(p);
      pSelect = (p->idxiter.zIndex ? p->idxiter.pSelect : p->tbliter.pSelect);

      /* Advance to the next input row. */
      if( p->rc==SQLITE_OK ){
        int rc = sqlite3_step(pSelect);
        if( rc!=SQLITE_ROW ){
          otaResetStatement(p, pSelect);

          /* Go to the next index. */
          if( p->rc==SQLITE_OK ){
            if( p->idxiter.zIndex ){
              p->rc = idxIterNext(&p->idxiter);
            }else{
              p->rc = idxIterFirst(p->dbDest, p->tbliter.zTarget, &p->idxiter);
            }
          }

          /* If there is no next index, go to the next table. */
          if( p->rc==SQLITE_OK && p->idxiter.zIndex==0 ){
            p->rc = tblIterNext(&p->tbliter);
          }
          continue;
        }
      }

      /* Update the target database PK table according to the row that 
      ** tbliter.pSelect currently points to. 
      **
      ** todo: For now, we assume all rows are INSERT commands - this will 
      ** change.  */
      if( p->rc==SQLITE_OK ){
        sqlite3_stmt *pInsert;
        int nCol;
        if( p->idxiter.zIndex ){
          pInsert = p->idxiter.pWriter;
          nCol = p->idxiter.nCol;
        }else{
          pInsert = p->tbliter.pInsert;
          nCol = p->tbliter.nCol;
        }

        for(i=0; i<nCol; i++){
          sqlite3_value *pVal = sqlite3_column_value(pSelect, i+1);
          sqlite3_bind_value(pInsert, i+1, pVal);
        }

        sqlite3_step(pInsert);
        otaResetStatement(p, pInsert);
      }
      
      break;
    }

    if( p->rc==SQLITE_OK && p->tbliter.zTarget==0 ) p->rc = SQLITE_DONE;
  }

  return (p ? p->rc : SQLITE_NOMEM);
}

static void otaOpenDatabase(sqlite3ota *p, sqlite3 **pDb, const char *zFile){
  if( p->rc==SQLITE_OK ){
    p->rc = sqlite3_open(zFile, pDb);
    if( p->rc ){
      const char *zErr = sqlite3_errmsg(*pDb);
      p->zErrmsg = sqlite3_mprintf("sqlite3_open(): %s", zErr);
    }
  }
}

static void otaSaveTransactionState(sqlite3ota *p){
  sqlite3_stmt *pStmt = 0;
  void *pWalState = 0;
  int nWalState = 0;
  int rc;

  const char *zInsert = 
    "INSERT INTO ota_state(wal_state, tbl, idx, row, progress)"
    "VALUES(:wal_state, :tbl, :idx, :row, :progress)";

  rc = sqlite3_transaction_save(p->dbDest, &pWalState, &nWalState);
  if( rc==SQLITE_OK ){
    rc = sqlite3_exec(p->dbOta, "DELETE FROM ota_state", 0, 0, 0);
  }
  if( rc==SQLITE_OK ){
    rc = prepareAndCollectError(p->dbOta, zInsert, &pStmt, &p->zErrmsg);
  }
  if( rc==SQLITE_OK ){
    sqlite3_stmt *pSelect;
    pSelect = (p->idxiter.zIndex ? p->idxiter.pSelect : p->tbliter.pSelect);
    sqlite3_bind_blob(pStmt, 1, pWalState, nWalState, SQLITE_STATIC);
    sqlite3_bind_text(pStmt, 2, p->tbliter.zTarget, -1, SQLITE_STATIC);
    if( p->idxiter.zIndex ){
      sqlite3_bind_text(pStmt, 3, p->idxiter.zIndex, -1, SQLITE_STATIC);
    }
    sqlite3_bind_int64(pStmt, 4, sqlite3_column_int64(pSelect, 0));
    sqlite3_step(pStmt);
    rc = sqlite3_finalize(pStmt);
    if( rc==SQLITE_OK ){
      rc = sqlite3_exec(p->dbOta, "COMMIT", 0, 0, 0);
    }
    if( rc!=SQLITE_OK ){
      p->zErrmsg = sqlite3_mprintf("%s", sqlite3_errmsg(p->dbOta));
    }
  }
  sqlite3_free(pWalState);
  assert( p->rc==SQLITE_OK );
  p->rc = rc;
}

static void otaLoadTransactionState(sqlite3ota *p){
  sqlite3_stmt *pStmt = 0;
  int rc;

  const char *zSelect = 
    "SELECT wal_state, tbl, idx, row, progress FROM ota_state";

  rc = prepareAndCollectError(p->dbOta, zSelect, &pStmt, &p->zErrmsg);
  if( rc==SQLITE_OK ){
    if( SQLITE_ROW==sqlite3_step(pStmt) ){
      const void *pWalState = 0;
      int nWalState = 0;
      const char *zTbl;
      const char *zIdx;
      sqlite3_int64 iRowid;

      pWalState = sqlite3_column_blob(pStmt, 0);
      nWalState = sqlite3_column_bytes(pStmt, 0);
      zTbl = (const char*)sqlite3_column_text(pStmt, 1);
      zIdx = (const char*)sqlite3_column_text(pStmt, 2);
      iRowid = sqlite3_column_int64(pStmt, 3);
      rc = sqlite3_transaction_restore(p->dbDest, pWalState, nWalState);

      while( rc==SQLITE_OK 
          && p->tbliter.zTarget 
          && sqlite3_stricmp(p->tbliter.zTarget, zTbl) 
      ){
        rc = tblIterNext(&p->tbliter);
      }
      if( rc==SQLITE_OK && !p->tbliter.zTarget ){
        rc = SQLITE_ERROR;
        p->zErrmsg = sqlite3_mprintf("ota_state mismatch error");
      }

      if( rc==SQLITE_OK && zIdx ){
        rc = idxIterFirst(p->dbDest, p->tbliter.zTarget, &p->idxiter);
        while( rc==SQLITE_OK 
            && p->idxiter.zIndex 
            && sqlite3_stricmp(p->idxiter.zIndex, zIdx) 
        ){
          rc = idxIterNext(&p->idxiter);
        }
        if( rc==SQLITE_OK && !p->idxiter.zIndex ){
          rc = SQLITE_ERROR;
          p->zErrmsg = sqlite3_mprintf("ota_state mismatch error");
        }
      }

      if( rc==SQLITE_OK ){
        rc = otaPrepareAll(p);
      }

      if( rc==SQLITE_OK ){
        sqlite3_stmt *pSelect;
        pSelect = (p->idxiter.zIndex ? p->idxiter.pSelect : p->tbliter.pSelect);
        while( sqlite3_column_int64(pSelect, 0)!=iRowid ){
          rc = sqlite3_step(pSelect);
          if( rc!=SQLITE_ROW ) break;
        }
        if( rc==SQLITE_ROW ){
          rc = SQLITE_OK;
        }else{
          rc = SQLITE_ERROR;
          p->zErrmsg = sqlite3_mprintf("ota_state mismatch error");
        }
      }
    }
    if( rc==SQLITE_OK ){
      rc = sqlite3_finalize(pStmt);
    }else{
      sqlite3_finalize(pStmt);
    }
  }
  p->rc = rc;
}


/*
** Open and return a new OTA handle. 
*/
sqlite3ota *sqlite3ota_open(const char *zTarget, const char *zOta){
  sqlite3ota *p;

  p = (sqlite3ota*)sqlite3_malloc(sizeof(sqlite3ota));
  if( p ){

    /* Open the target database */
    memset(p, 0, sizeof(sqlite3ota));
    otaOpenDatabase(p, &p->dbDest, zTarget);
    otaOpenDatabase(p, &p->dbOta, zOta);

    /* If it has not already been created, create the ota_state table */
    if( p->rc==SQLITE_OK ){
      p->rc = sqlite3_exec(p->dbOta, OTA_CREATE_STATE, 0, 0, &p->zErrmsg);
    }

    if( p->rc==SQLITE_OK ){
      const char *zScript = 
        "PRAGMA ota_mode=1;"
        "PRAGMA journal_mode=wal;"
        "BEGIN IMMEDIATE;"
      ;
      p->rc = sqlite3_exec(p->dbDest, zScript, 0, 0, &p->zErrmsg);
    }

    if( p->rc==SQLITE_OK ){
      const char *zScript = "BEGIN IMMEDIATE";
      p->rc = sqlite3_exec(p->dbOta, zScript, 0, 0, &p->zErrmsg);
    }

    /* Point the table iterator at the first table */
    if( p->rc==SQLITE_OK ){
      p->rc = tblIterFirst(p->dbOta, &p->tbliter);
    }

    if( p->rc==SQLITE_OK ){
      otaLoadTransactionState(p);
    }
  }

  return p;
}

static void otaCloseHandle(sqlite3 *db){
  int rc = sqlite3_close(db);
  assert( rc==SQLITE_OK );
}

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
    tblIterFinalize(&p->tbliter);
    idxIterFinalize(&p->idxiter);

    /* If the ota update has been fully applied, commit the transaction
    ** on the target database. */
    if( p->rc==SQLITE_DONE ){
      rc = sqlite3_exec(p->dbDest, "COMMIT", 0, 0, &p->zErrmsg);
      if( rc!=SQLITE_OK ) p->rc = rc;
    }

    rc = p->rc;
    *pzErrmsg = p->zErrmsg;
    otaCloseHandle(p->dbDest);
    otaCloseHandle(p->dbOta);
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




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

#if !defined(SQLITE_CORE) || defined(SQLITE_ENABLE_OTA)
#include "sqlite3ota.h"


/*
** The ota_state table is used to save the state of a partially applied
** update so that it can be resumed later. The table consists of integer
** keys mapped to values as follows:
**
** OTA_STATE_STAGE:
**   May be set to integer values 1, 2 or 3. As follows:
**       0: Nothing at all has been done.
**       1: the *-ota file is currently under construction.
**       2: the *-ota file has been constructed, but not yet moved 
**          to the *-wal path.
**       3: the checkpoint is underway.
**
** OTA_STATE_TBL:
**   Only valid if STAGE==1. The target database name of the table 
**   currently being written.
**
** OTA_STATE_IDX:
**   Only valid if STAGE==1. The target database name of the index 
**   currently being written, or NULL if the main table is currently being
**   updated.
**
** OTA_STATE_ROW:
**   Only valid if STAGE==1. Number of rows already processed for the current
**   table/index.
**
** OTA_STATE_PROGRESS:
**   Total number of sqlite3ota_step() calls made so far as part of this
**   ota update.
**
** OTA_STATE_CKPT:
**   Valid if STAGE==3. The blob to pass to sqlite3ckpt_start() to resume
**   the incremental checkpoint.
**
*/
#define OTA_STATE_STAGE       1
#define OTA_STATE_TBL         2
#define OTA_STATE_IDX         3
#define OTA_STATE_ROW         4
#define OTA_STATE_PROGRESS    5
#define OTA_STATE_CKPT        6

#define OTA_STAGE_OAL         1
#define OTA_STAGE_COPY        2
#define OTA_STAGE_CKPT        3
#define OTA_STAGE_DONE        4


#define OTA_CREATE_STATE "CREATE TABLE IF NOT EXISTS ota.ota_state"        \
                             "(k INTEGER PRIMARY KEY, v)"

typedef struct OtaState OtaState;
typedef struct OtaObjIter OtaObjIter;

/*
** A structure to store values read from the ota_state table in memory.
*/
struct OtaState {
  int eStage;
  char *zTbl;
  char *zIdx;
  unsigned char *pCkptState;
  int nCkptState;
  int nRow;
  sqlite3_int64 nProgress;
};

/*
** An iterator of this type is used to iterate through all objects in
** the target database that require updating. For each such table, the
** iterator visits, in order:
**
**     * the table itself, 
**     * each index of the table (zero or more points to visit), and
**     * a special "cleanup table" state.
*/
struct OtaObjIter {
  sqlite3_stmt *pTblIter;         /* Iterate through tables */
  sqlite3_stmt *pIdxIter;         /* Index iterator */
  int nTblCol;                    /* Size of azTblCol[] array */
  char **azTblCol;                /* Array of unquoted column names */
  char **azTblType;               /* Array of column types */
  int *aiTblOrder;                /* Order of columns in target table */
  unsigned char *abTblPk;         /* Array of flags - true for PK columns */
  int eType;

  /* Output variables. zTbl==0 implies EOF. */
  int bCleanup;                   /* True in "cleanup" state */
  const char *zTbl;               /* Name of target db table */
  const char *zIdx;               /* Name of target db index (or null) */
  int tnum;                       /* Root page of index (not table) */
  int bUnique;                    /* Current index is unique */
  int iVisit;                     /* Number of points visited, incl. current */

  /* Statements created by otaObjIterPrepareAll() */
  int nCol;                       /* Number of columns in current object */
  sqlite3_stmt *pSelect;          /* Source data */
  sqlite3_stmt *pInsert;          /* Statement for INSERT operations */
  sqlite3_stmt *pDelete;          /* Statement for DELETE ops */

  /* Last UPDATE used (for PK b-tree updates only), or NULL. */
  char *zMask;                    /* Copy of update mask used with pUpdate */
  sqlite3_stmt *pUpdate;          /* Last update statement (or NULL) */
};

/*
** Values for OtaObjIter.eType
**
**     1: Table has an implicit rowid.
**     2: Table has an explicit IPK column.
**     3: Table has an external PK index.
**     4: Table is WITHOUT ROWID.
**     5: Table is a virtual table.
*/
#define OTA_PK_NONE           1
#define OTA_PK_IPK            2
#define OTA_PK_EXTERNAL       3
#define OTA_PK_WITHOUT_ROWID  4
#define OTA_PK_VTAB           5

/*
** OTA handle.
*/
struct sqlite3ota {
  int eStage;                     /* Value of OTA_STATE_STAGE field */
  sqlite3 *db;                    /* "main" -> target db, "ota" -> ota db */
  char *zTarget;                  /* Path to target db */
  char *zOta;                     /* Path to ota db */
  int rc;                         /* Value returned by last ota_step() call */
  char *zErrmsg;                  /* Error message if rc!=SQLITE_OK */
  int nStep;                      /* Rows processed for current object */
  int nProgress;                  /* Rows processed for all objects */
  OtaObjIter objiter;             /* Iterator for skipping through tbl/idx */
  sqlite3_ckpt *pCkpt;            /* Incr-checkpoint handle */
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
** by an earlier call to otaObjIterCacheTableInfo().
*/
static void otaObjIterFreeCols(OtaObjIter *pIter){
  int i;
  for(i=0; i<pIter->nTblCol; i++){
    sqlite3_free(pIter->azTblCol[i]);
    sqlite3_free(pIter->azTblType[i]);
  }
  sqlite3_free(pIter->azTblCol);
  pIter->azTblCol = 0;
  pIter->azTblType = 0;
  pIter->aiTblOrder = 0;
  pIter->abTblPk = 0;
  pIter->nTblCol = 0;
  sqlite3_free(pIter->zMask);
  pIter->zMask = 0;
  pIter->eType = 0;               /* Invalid value */
}

/*
** Finalize all statements and free all allocations that are specific to
** the current object (table/index pair).
*/
static void otaObjIterClearStatements(OtaObjIter *pIter){
  sqlite3_finalize(pIter->pSelect);
  sqlite3_finalize(pIter->pInsert);
  sqlite3_finalize(pIter->pDelete);
  sqlite3_finalize(pIter->pUpdate);
  pIter->pSelect = 0;
  pIter->pInsert = 0;
  pIter->pDelete = 0;
  pIter->pUpdate = 0;
  pIter->nCol = 0;
}

/*
** Clean up any resources allocated as part of the iterator object passed
** as the only argument.
*/
static void otaObjIterFinalize(OtaObjIter *pIter){
  otaObjIterClearStatements(pIter);
  sqlite3_finalize(pIter->pTblIter);
  sqlite3_finalize(pIter->pIdxIter);
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
    otaObjIterClearStatements(pIter);
    if( pIter->zIdx==0 ){
      rc = sqlite3_exec(p->db, 
          "DROP TRIGGER IF EXISTS temp.ota_insert_tr;"
          "DROP TRIGGER IF EXISTS temp.ota_update1_tr;"
          "DROP TRIGGER IF EXISTS temp.ota_update2_tr;"
          "DROP TRIGGER IF EXISTS temp.ota_delete_tr;"
          , 0, 0, &p->zErrmsg
      );
    }

    if( rc==SQLITE_OK ){
      if( pIter->bCleanup ){
        otaObjIterFreeCols(pIter);
        pIter->bCleanup = 0;
        rc = sqlite3_step(pIter->pTblIter);
        if( rc!=SQLITE_ROW ){
          rc = sqlite3_reset(pIter->pTblIter);
          pIter->zTbl = 0;
        }else{
          pIter->zTbl = (const char*)sqlite3_column_text(pIter->pTblIter, 0);
          pIter->tnum = sqlite3_column_int(pIter->pTblIter, 1);
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
          pIter->tnum = sqlite3_column_int(pIter->pIdxIter, 1);
          pIter->bUnique = sqlite3_column_int(pIter->pIdxIter, 2);
          rc = SQLITE_OK;
        }
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
      "SELECT substr(a.name, 6), b.rootpage FROM ota.sqlite_master AS a "
      "LEFT JOIN main.sqlite_master AS b ON "
      "(substr(a.name, 6)==b.name) "
      "WHERE a.type='table' AND a.name LIKE 'data_%'"
  );

  if( rc==SQLITE_OK ){
    rc = prepareAndCollectError(p->db, &pIter->pIdxIter, &p->zErrmsg,
        "SELECT name, rootpage, sql IS NULL OR substr(8, 6)=='UNIQUE' "
        "  FROM main.sqlite_master "
        "  WHERE type='index' AND tbl_name = ?"
    );
  }

  pIter->bCleanup = 1;
  p->rc = rc;
  return otaObjIterNext(p, pIter);
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

/*
** Allocate and zero the pIter->azTblCol[] and abTblPk[] arrays so that
** there is room for at least nCol elements. If an OOM occurs, store an
** error code in the OTA handle passed as the first argument.
*/
static void otaAllocateIterArrays(sqlite3ota *p, OtaObjIter *pIter, int nCol){
  int nByte = (sizeof(char*) * 2 + sizeof(int) + sizeof(unsigned char)) * nCol;
  char **azNew;

  assert( p->rc==SQLITE_OK );
  azNew = (char**)sqlite3_malloc(nByte);
  if( azNew ){
    memset(azNew, 0, nByte);
    pIter->azTblCol = azNew;
    pIter->azTblType = &azNew[nCol];
    pIter->aiTblOrder = (int*)&pIter->azTblType[nCol];
    pIter->abTblPk = (unsigned char*)&pIter->aiTblOrder[nCol];
  }else{
    p->rc = SQLITE_NOMEM;
  }
}

static char *otaStrndup(const char *zStr, int nStr, int *pRc){
  char *zRet = 0;
  assert( *pRc==SQLITE_OK );

  if( zStr ){
    int nCopy = nStr;
    if( nCopy<0 ) nCopy = strlen(zStr) + 1;
    zRet = (char*)sqlite3_malloc(nCopy);
    if( zRet ){
      memcpy(zRet, zStr, nCopy);
    }else{
      *pRc = SQLITE_NOMEM;
    }
  }

  return zRet;
}


/*
** If they are not already populated, populate the pIter->azTblCol[],
** pIter->abTblPk[], pIter->nTblCol and pIter->bRowid variables according to
** the table (not index) that the iterator currently points to.
**
** Return SQLITE_OK if successful, or an SQLite error code otherwise. If
** an error does occur, an error code and error message are also left in 
** the OTA handle.
*/
static int otaObjIterCacheTableInfo(sqlite3ota *p, OtaObjIter *pIter){
  if( pIter->azTblCol==0 ){
    sqlite3_stmt *pStmt = 0;
    int nCol = 0;
    int i;                        /* for() loop iterator variable */
    int rc2;                      /* sqlite3_finalize() return value */
    int bOtaRowid = 0;            /* If input table has column "ota_rowid" */
    int iOrder = 0;

    /* Figure out the type of table this step will deal with. */
    assert( pIter->eType==0 );
    sqlite3_test_control(
        SQLITE_TESTCTRL_TBLTYPE, p->db, "main", pIter->zTbl, &pIter->eType
    );
    assert( pIter->eType==OTA_PK_NONE || pIter->eType==OTA_PK_IPK 
         || pIter->eType==OTA_PK_EXTERNAL || pIter->eType==OTA_PK_WITHOUT_ROWID
         || pIter->eType==OTA_PK_VTAB
    );

    /* Populate the azTblCol[] and nTblCol variables based on the columns
    ** of the input table. Ignore any input table columns that begin with
    ** "ota_".  */
    p->rc = prepareFreeAndCollectError(p->db, &pStmt, &p->zErrmsg, 
        sqlite3_mprintf("SELECT * FROM 'data_%q'", pIter->zTbl)
    );
    if( p->rc==SQLITE_OK ){
      nCol = sqlite3_column_count(pStmt);
      otaAllocateIterArrays(p, pIter, nCol);
    }
    for(i=0; p->rc==SQLITE_OK && i<nCol; i++){
      const char *zName = (const char*)sqlite3_column_name(pStmt, i);
      if( sqlite3_strnicmp("ota_", zName, 4) ){
        char *zCopy = otaStrndup(zName, -1, &p->rc);
        pIter->azTblCol[pIter->nTblCol++] = zCopy;
      }
      else if( 0==sqlite3_stricmp("ota_rowid", zName) ){
        bOtaRowid = 1;
      }
    }
    sqlite3_finalize(pStmt);
    pStmt = 0;

    if( p->rc==SQLITE_OK
     && bOtaRowid!=(pIter->eType==OTA_PK_VTAB || pIter->eType==OTA_PK_NONE)
    ){
      p->rc = SQLITE_ERROR;
      p->zErrmsg = sqlite3_mprintf(
          "table data_%q %s ota_rowid column", pIter->zTbl,
          (bOtaRowid ? "may not have" : "requires")
      );
    }

    /* Check that all non-HIDDEN columns in the destination table are also
    ** present in the input table. Populate the abTblPk[], azTblType[] and
    ** aiTblOrder[] arrays at the same time.  */
    if( p->rc==SQLITE_OK ){
      p->rc = prepareFreeAndCollectError(p->db, &pStmt, &p->zErrmsg, 
          sqlite3_mprintf("PRAGMA main.table_info(%Q)", pIter->zTbl)
      );
    }
    while( p->rc==SQLITE_OK && SQLITE_ROW==sqlite3_step(pStmt) ){
      const char *zName = (const char*)sqlite3_column_text(pStmt, 1);
      for(i=0; i<pIter->nTblCol; i++){
        if( 0==strcmp(zName, pIter->azTblCol[i]) ) break;
      }
      if( i==pIter->nTblCol ){
        p->rc = SQLITE_ERROR;
        p->zErrmsg = sqlite3_mprintf("column missing from data_%q: %s",
            pIter->zTbl, zName
        );
      }else{
        int iPk = sqlite3_column_int(pStmt, 5);
        const char *zType = (const char*)sqlite3_column_text(pStmt, 2);
        pIter->aiTblOrder[i] = iOrder++;
        pIter->azTblType[i] = otaStrndup(zType, -1, &p->rc);
        pIter->abTblPk[i] = (iPk!=0);
      }
    }
    while( iOrder<pIter->nTblCol ){
      for(i=0; i<pIter->nTblCol; i++){
        if( pIter->aiTblOrder[i]==0 ){
          pIter->aiTblOrder[i] = iOrder++;
          continue;
        }
      }
    }

    /* Check that there were no extra columns in the data_xxx table that
    ** are not present in the target table. If there are, an error. */
#if 0
    assert( iOrder<=pIter->nTblCol );
    if( p->rc==SQLITE_OK && iOrder!=pIter->nTblCol ){
      p->rc = SQLITE_ERROR;
      p->zErrmsg = sqlite3_mprintf("data_%q has %d columns, expected %d",
          pIter->zTbl, iOrder, pIter->nTblCol
      );
    }
#endif

    rc2 = sqlite3_finalize(pStmt);
    if( p->rc==SQLITE_OK ) p->rc = rc2;
  }

  return p->rc;
}

/*
** This is a wrapper around "sqlite3_mprintf(zFmt, ...)". If an OOM occurs,
** an error code is stored in the OTA handle passed as the first argument.
*/
static char *otaMPrintfAndCollectError(sqlite3ota *p, const char *zFmt, ...){
  char *zSql = 0;
  va_list ap;
  va_start(ap, zFmt);
  if( p->rc==SQLITE_OK ){
    zSql = sqlite3_vmprintf(zFmt, ap);
    if( zSql==0 ) p->rc = SQLITE_NOMEM;
  }
  va_end(ap);
  return zSql;
}

/*
** This function constructs and returns a pointer to a nul-terminated 
** string containing some SQL clause or list based on one or more of the 
** column names currently stored in the pIter->azTblCol[] array.
*/
static char *otaObjIterGetCollist(
  sqlite3ota *p,                  /* OTA object */
  OtaObjIter *pIter               /* Object iterator for column names */
){
  char *zList = 0;
  const char *zSep = "";
  int i;
  for(i=0; i<pIter->nTblCol; i++){
    const char *z = pIter->azTblCol[i];
    zList = otaMPrintfAndCollectError(p, "%z%s\"%w\"", zList, zSep, z);
    zSep = ", ";
  }
  return zList;
}

/*
** This function is used to create a SELECT list (the list of SQL 
** expressions that follows a SELECT keyword) for a SELECT statement 
** used to read from an ota_xxx table while updating the index object
** currently indicated by the iterator object passed as the second 
** argument. A "PRAGMA index_xinfo = <idxname>" statement is used to
** obtain the required information.
**
** If the index is of the following form:
**
**   CREATE INDEX i1 ON t1(c, b COLLATE nocase);
**
** and "t1" is a table with an explicit INTEGER PRIMARY KEY column 
** "ipk", the returned string is:
**
**   "`c` COLLATE 'BINARY', `b` COLLATE 'NOCASE', `ipk` COLLATE 'BINARY'"
**
** As well as the returned string, three other malloc'd strings are 
** returned via output parameters. As follows:
**
**   pzImposterCols: ...
**   pzImposterPk: ...
**   pzWhere: ...
*/
static char *otaObjIterGetIndexCols(
  sqlite3ota *p,                  /* OTA object */
  OtaObjIter *pIter,              /* Object iterator for column names */
  char **pzImposterCols,          /* OUT: Columns for imposter table */
  char **pzImposterPk,            /* OUT: Imposter PK clause */
  char **pzWhere,                 /* OUT: WHERE clause */
  int *pnBind                     /* OUT: Total number of columns */
){
  int rc = p->rc;                 /* Error code */
  int rc2;                        /* sqlite3_finalize() return code */
  char *zRet = 0;                 /* String to return */
  char *zImpCols = 0;             /* String to return via *pzImposterCols */
  char *zImpPK = 0;               /* String to return via *pzImposterPK */
  char *zWhere = 0;               /* String to return via *pzWhere */
  int nBind = 0;                  /* Value to return via *pnBind */
  const char *zCom = "";          /* Set to ", " later on */
  const char *zAnd = "";          /* Set to " AND " later on */
  sqlite3_stmt *pXInfo = 0;       /* PRAGMA index_xinfo = ? */

  if( rc==SQLITE_OK ){
    assert( p->zErrmsg==0 );
    rc = prepareFreeAndCollectError(p->db, &pXInfo, &p->zErrmsg,
        sqlite3_mprintf("PRAGMA main.index_xinfo = %Q", pIter->zIdx)
    );
  }

  while( rc==SQLITE_OK && SQLITE_ROW==sqlite3_step(pXInfo) ){
    const char *zCollate = (const char*)sqlite3_column_text(pXInfo, 4);
    int iCid = sqlite3_column_int(pXInfo, 1);
    const char *zCol;
    const char *zType;

    if( iCid<0 ){
      /* An integer primary key. If the table has an explicit IPK, use
      ** its name. Otherwise, use "ota_rowid".  */
      if( pIter->eType==OTA_PK_IPK ){
        int i;
        for(i=0; i<pIter->nTblCol && pIter->abTblPk[i]==0; i++);
        assert( i<pIter->nTblCol );
        zCol = pIter->azTblCol[i];
      }else{
        zCol = "ota_rowid";
      }
      zType = "INTEGER";
    }else{
      zCol = pIter->azTblCol[iCid];
      zType = pIter->azTblType[iCid];
    }

    zRet = sqlite3_mprintf("%z%s\"%w\" COLLATE %Q", zRet, zCom, zCol, zCollate);
    if( pIter->bUnique==0 || sqlite3_column_int(pXInfo, 5) ){
      zImpPK = sqlite3_mprintf("%z%sc%d", zImpPK, zCom, nBind);
    }
    zImpCols = sqlite3_mprintf(
        "%z%sc%d %s COLLATE %Q", zImpCols, zCom, nBind, zType, zCollate
    );
    zWhere = sqlite3_mprintf("%z%sc%d IS ?", zWhere, zAnd, nBind);
    if( zRet==0 || zImpPK==0 || zImpCols==0 || zWhere==0 ) rc = SQLITE_NOMEM;
    zCom = ", ";
    zAnd = " AND ";
    nBind++;
  }

  rc2 = sqlite3_finalize(pXInfo);
  if( rc==SQLITE_OK ) rc = rc2;

  if( rc!=SQLITE_OK ){
    sqlite3_free(zRet);
    sqlite3_free(zImpCols);
    sqlite3_free(zImpPK);
    sqlite3_free(zWhere);
    zRet = 0;
    zImpCols = 0;
    zImpPK = 0;
    zWhere = 0;
    p->rc = rc;
  }

  *pzImposterCols = zImpCols;
  *pzImposterPk = zImpPK;
  *pzWhere = zWhere;
  *pnBind = nBind;
  return zRet;
}

/*
** Assuming the current table columns are "a", "b" and "c", and the zObj
** paramter is passed "old", return a string of the form:
**
**     "old.a, old.b, old.b"
**
** With the column names escaped.
**
** For tables with implicit rowids - OTA_PK_EXTERNAL and OTA_PK_NONE, append
** the text ", old._rowid_" to the returned value.
*/
static char *otaObjIterGetOldlist(
  sqlite3ota *p, 
  OtaObjIter *pIter,
  const char *zObj
){
  char *zList = 0;
  if( p->rc==SQLITE_OK ){
    const char *zS = "";
    int i;
    for(i=0; i<pIter->nTblCol; i++){
      const char *zCol = pIter->azTblCol[i];
      zList = sqlite3_mprintf("%z%s%s.\"%w\"", zList, zS, zObj, zCol);
      zS = ", ";
      if( zList==0 ){
        p->rc = SQLITE_NOMEM;
        break;
      }
    }

    /* For a table with implicit rowids, append "old._rowid_" to the list. */
    if( pIter->eType==OTA_PK_EXTERNAL || pIter->eType==OTA_PK_NONE ){
      zList = sqlite3_mprintf("%z, %s._rowid_", zList, zObj);
    }
  }
  return zList;
}

/*
** Return an expression that can be used in a WHERE clause to match the
** primary key of the current table. For example, if the table is:
**
**   CREATE TABLE t1(a, b, c, PRIMARY KEY(b, c));
**
** Return the string:
**
**   "b = ?1 AND c = ?2"
*/
static char *otaObjIterGetWhere(
  sqlite3ota *p, 
  OtaObjIter *pIter
){
  char *zList = 0;
  if( pIter->eType==OTA_PK_VTAB || pIter->eType==OTA_PK_NONE ){
    zList = otaMPrintfAndCollectError(p, "_rowid_ = ?%d", pIter->nTblCol+1);
  }else{
    const char *zSep = "";
    int i;
    for(i=0; i<pIter->nTblCol; i++){
      if( pIter->abTblPk[i] ){
        const char *zCol = pIter->azTblCol[i];
        zList = otaMPrintfAndCollectError(
            p, "%z%s\"%w\"=?%d", zList, zSep, zCol, i+1
            );
        zSep = " AND ";
      }
    }
  }
  return zList;
}

/*
** The SELECT statement iterating through the keys for the current object
** (p->objiter.pSelect) currently points to a valid row. However, there
** is something wrong with the ota_control value in the ota_control value
** stored in the (p->nCol+1)'th column. Set the error code and error message
** of the OTA handle to something reflecting this.
*/
static void otaBadControlError(sqlite3ota *p){
  p->rc = SQLITE_ERROR;
  p->zErrmsg = sqlite3_mprintf("Invalid ota_control value");
}


static char *otaObjIterGetSetlist(
  sqlite3ota *p,
  OtaObjIter *pIter,
  const char *zMask
){
  char *zList = 0;
  if( p->rc==SQLITE_OK ){
    int i;

    if( strlen(zMask)!=pIter->nTblCol ){
      otaBadControlError(p);
    }else{
      const char *zSep = "";
      for(i=0; i<pIter->nTblCol; i++){
        char c = zMask[i];
        if( c=='x' ){
          zList = otaMPrintfAndCollectError(p, "%z%s\"%w\"=?%d", 
              zList, zSep, pIter->azTblCol[i], i+1
          );
          zSep = ", ";
        }
        if( c=='d' ){
          zList = otaMPrintfAndCollectError(p, 
              "%z%s\"%w\"=ota_delta(\"%w\", ?%d)", 
              zList, zSep, pIter->azTblCol[i], pIter->azTblCol[i], i+1
          );
          zSep = ", ";
        }
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
** If an error has already occurred when this function is called, it 
** immediately returns zero (without doing any work). Or, if an error
** occurs during the execution of this function, it sets the error code
** in the sqlite3ota object indicated by the first argument and returns
** zero.
**
** The iterator passed as the second argument is guaranteed to point to
** a table (not an index) when this function is called. This function
** attempts to create any imposter tables required to write to the main
** table b-tree of the table before returning. Non-zero is returned if
** imposter tables are created, or zero otherwise.
**
** The required imposter tables depend on the type of table that the
** iterator currently points to.
**
**   OTA_PK_NONE, OTA_PK_IPK, OTA_PK_WITHOUT_ROWID:
**     A single imposter table is required. With the same schema as
**     the actual target table (less any UNIQUE constraints). More
**     precisely, the "same schema" means the same columns, types, collation
**     sequences and primary key declaration.
**
**   OTA_PK_VTAB:
**     No imposters required. 
**
**   OTA_PK_EXTERNAL:
**     Two imposters are required (TODO!!)
*/
static void otaCreateImposterTable(sqlite3ota *p, OtaObjIter *pIter){
  if( p->rc==SQLITE_OK && pIter->eType!=OTA_PK_VTAB ){
    int tnum = pIter->tnum;
    const char *zComma = "";
    char *zSql = 0;
    int iCol;
    sqlite3_test_control(SQLITE_TESTCTRL_IMPOSTER, p->db, "main", 0, 1);

    for(iCol=0; p->rc==SQLITE_OK && iCol<pIter->nTblCol; iCol++){
      int iDataCol = pIter->aiTblOrder[iCol];
      const char *zCol = pIter->azTblCol[iDataCol];
      const char *zColl = 0;
      p->rc = sqlite3_table_column_metadata(
          p->db, "main", pIter->zTbl, zCol, 0, &zColl, 0, 0, 0
      );
      zSql = otaMPrintfAndCollectError(p, "%z%s\"%w\" %s COLLATE %s", 
          zSql, zComma, zCol, pIter->azTblType[iDataCol], zColl
      );
      zComma = ", ";
    }

    if( pIter->eType==OTA_PK_IPK || pIter->eType==OTA_PK_WITHOUT_ROWID ){
      zSql = otaMPrintfAndCollectError(p, "%z, PRIMARY KEY(", zSql);
      zComma = "";
      for(iCol=0; iCol<pIter->nTblCol; iCol++){
        if( pIter->abTblPk[iCol] ){
          zSql = otaMPrintfAndCollectError(p, "%z%s\"%w\"", 
              zSql, zComma, pIter->azTblCol[iCol]
          );
          zComma = ", ";
        }
      }
      zSql = otaMPrintfAndCollectError(p, "%z)", zSql);
    }

    zSql = otaMPrintfAndCollectError(p, "CREATE TABLE ota_imposter(%z)%s", 
        zSql, (pIter->eType==OTA_PK_WITHOUT_ROWID ? " WITHOUT ROWID" : "")
    );
    if( p->rc==SQLITE_OK ){
      sqlite3_test_control(SQLITE_TESTCTRL_IMPOSTER, p->db, "main", 1, tnum);
      p->rc = sqlite3_exec(p->db, zSql, 0, 0, &p->zErrmsg);
      sqlite3_test_control(SQLITE_TESTCTRL_IMPOSTER, p->db, "main", 0, 0);
    }
    sqlite3_free(zSql);
  }
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
  if( pIter->pSelect==0 && otaObjIterCacheTableInfo(p, pIter)==SQLITE_OK ){
    const int tnum = pIter->tnum;
    char *zCollist = 0;           /* List of indexed columns */
    char **pz = &p->zErrmsg;
    const char *zIdx = pIter->zIdx;
    char *zLimit = 0;

    if( nOffset ){
      zLimit = sqlite3_mprintf(" LIMIT -1 OFFSET %d", nOffset);
      if( !zLimit ) p->rc = SQLITE_NOMEM;
    }

    if( zIdx ){
      char *zImposterCols = 0;    /* Columns for imposter table */
      char *zImposterPK = 0;      /* Primary key declaration for imposter */
      char *zWhere = 0;           /* WHERE clause on PK columns */
      char *zBind = 0;
      int nBind = 0;

      assert( pIter->eType!=OTA_PK_VTAB );
      zCollist = otaObjIterGetIndexCols(
          p, pIter, &zImposterCols, &zImposterPK, &zWhere, &nBind
      );
      zBind = otaObjIterGetBindlist(p, nBind);

      /* Create the imposter table used to write to this index. */
      sqlite3_test_control(SQLITE_TESTCTRL_IMPOSTER, p->db, "main", 0, 1);
      sqlite3_test_control(SQLITE_TESTCTRL_IMPOSTER, p->db, "main", 1, tnum);
      otaMPrintfExec(p, 
          "CREATE TABLE ota_imposter( %s, PRIMARY KEY( %s ) ) WITHOUT ROWID",
          zImposterCols, zImposterPK
      );
      sqlite3_test_control(SQLITE_TESTCTRL_IMPOSTER, p->db, "main", 0, 0);

      /* Create the statement to insert index entries */
      pIter->nCol = nBind;
      if( p->rc==SQLITE_OK ){
        p->rc = prepareFreeAndCollectError(p->db, &pIter->pInsert, &p->zErrmsg,
          sqlite3_mprintf("INSERT INTO ota_imposter VALUES(%s)", zBind)
        );
      }

      /* And to delete index entries */
      if( p->rc==SQLITE_OK ){
        p->rc = prepareFreeAndCollectError(p->db, &pIter->pDelete, &p->zErrmsg,
          sqlite3_mprintf("DELETE FROM ota_imposter WHERE %s", zWhere)
        );
      }

      /* Create the SELECT statement to read keys in sorted order */
      if( p->rc==SQLITE_OK ){
        char *zSql;
        if( pIter->eType==OTA_PK_EXTERNAL || pIter->eType==OTA_PK_NONE ){
          zSql = sqlite3_mprintf(
              "SELECT %s, ota_control FROM ota.'ota_tmp_%q' ORDER BY %s%s",
              zCollist, pIter->zTbl,
              zCollist, zLimit
          );
        }else{
          zSql = sqlite3_mprintf(
              "SELECT %s, ota_control FROM ota.'data_%q' "
              "WHERE typeof(ota_control)='integer' AND ota_control!=1 "
              "UNION ALL "
              "SELECT %s, ota_control FROM ota.'ota_tmp_%q' "
              "ORDER BY %s%s",
              zCollist, pIter->zTbl, 
              zCollist, pIter->zTbl, 
              zCollist, zLimit
          );
        }
        p->rc = prepareFreeAndCollectError(p->db, &pIter->pSelect, pz, zSql);
      }

      sqlite3_free(zImposterCols);
      sqlite3_free(zImposterPK);
      sqlite3_free(zWhere);
      sqlite3_free(zBind);
    }else{
      int bOtaRowid = (pIter->eType==OTA_PK_VTAB || pIter->eType==OTA_PK_NONE);
      const char *zTbl = pIter->zTbl;       /* Table this step applies to */
      const char *zWrite;                   /* Imposter table name */

      char *zBindings = otaObjIterGetBindlist(p, pIter->nTblCol + bOtaRowid);
      char *zWhere = otaObjIterGetWhere(p, pIter);
      char *zOldlist = otaObjIterGetOldlist(p, pIter, "old");
      char *zNewlist = otaObjIterGetOldlist(p, pIter, "new");

      zCollist = otaObjIterGetCollist(p, pIter);
      pIter->nCol = pIter->nTblCol;

      /* Create the SELECT statement to read keys from data_xxx */
      if( p->rc==SQLITE_OK ){
        p->rc = prepareFreeAndCollectError(p->db, &pIter->pSelect, pz,
            sqlite3_mprintf(
              "SELECT %s, ota_control%s FROM ota.'data_%q'%s", 
              zCollist, (bOtaRowid ? ", ota_rowid" : ""), zTbl, zLimit
            )
        );
      }

      /* Create the imposter table or tables (if required). */
      otaCreateImposterTable(p, pIter);
      zWrite = (pIter->eType==OTA_PK_VTAB ? zTbl : "ota_imposter");

      /* Create the INSERT statement to write to the target PK b-tree */
      if( p->rc==SQLITE_OK ){
        p->rc = prepareFreeAndCollectError(p->db, &pIter->pInsert, pz,
            sqlite3_mprintf(
              "INSERT INTO main.%Q(%s%s) VALUES(%s)", 
              zWrite, zCollist, (bOtaRowid ? ", _rowid_" : ""), zBindings
            )
        );
      }

      /* Create the DELETE statement to write to the target PK b-tree */
      if( p->rc==SQLITE_OK ){
        p->rc = prepareFreeAndCollectError(p->db, &pIter->pDelete, pz,
            sqlite3_mprintf(
              "DELETE FROM main.%Q WHERE %s", zWrite, zWhere
            )
        );
      }

      if( pIter->eType!=OTA_PK_VTAB ){
        const char *zOtaRowid = "";
        if( pIter->eType==OTA_PK_EXTERNAL || pIter->eType==OTA_PK_NONE ){
          zOtaRowid = ", ota_rowid";
        }

        /* Create the ota_tmp_xxx table and the triggers to populate it. */
        otaMPrintfExec(p, 
            "PRAGMA ota_mode = 1;"
            "CREATE TABLE IF NOT EXISTS ota.'ota_tmp_%q' AS "
            "SELECT *%s FROM ota.'data_%q' WHERE 0;"

            "CREATE TEMP TRIGGER ota_delete_tr BEFORE DELETE ON ota_imposter "
            "BEGIN "
            "  INSERT INTO 'ota_tmp_%q'(ota_control, %s%s) VALUES(2, %s);"
            "END;"

            "CREATE TEMP TRIGGER ota_update1_tr BEFORE UPDATE ON ota_imposter "
            "BEGIN "
            "  INSERT INTO 'ota_tmp_%q'(ota_control, %s%s) VALUES(2, %s);"
            "END;"

            "CREATE TEMP TRIGGER ota_update2_tr AFTER UPDATE ON ota_imposter "
            "BEGIN "
            "  INSERT INTO 'ota_tmp_%q'(ota_control, %s%s) VALUES(3, %s);"
            "END;"
            , zTbl, (pIter->eType==OTA_PK_EXTERNAL ? ", 0 AS ota_rowid" : "")
            , zTbl, 
            zTbl, zCollist, zOtaRowid, zOldlist,
            zTbl, zCollist, zOtaRowid, zOldlist,
            zTbl, zCollist, zOtaRowid, zNewlist
        );
        if( pIter->eType==OTA_PK_EXTERNAL || pIter->eType==OTA_PK_NONE ){
          otaMPrintfExec(p, 
              "CREATE TEMP TRIGGER ota_insert_tr AFTER INSERT ON ota_imposter "
              "BEGIN "
              "  INSERT INTO 'ota_tmp_%q'(ota_control, %s, ota_rowid)"
              "  VALUES(0, %s);"
              "END;"
              , zTbl, zCollist, zNewlist
          );
        }
      }else if( p->rc==SQLITE_OK ){
        p->rc = sqlite3_exec(p->db, "PRAGMA ota_mode = 0", 0, 0, &p->zErrmsg);
      }

      /* Allocate space required for the zMask field. */
      if( p->rc==SQLITE_OK ){
        int nMask = pIter->nTblCol+1;
        pIter->zMask = (char*)sqlite3_malloc(nMask);
        if( pIter->zMask==0 ){
          p->rc = SQLITE_NOMEM;
        }else{
          memset(pIter->zMask, 0, nMask);
        }
      }

      sqlite3_free(zWhere);
      sqlite3_free(zOldlist);
      sqlite3_free(zNewlist);
      sqlite3_free(zBindings);
    }
    sqlite3_free(zCollist);
    sqlite3_free(zLimit);
  }
  
  return p->rc;
}

#define OTA_INSERT     1
#define OTA_DELETE     2
#define OTA_IDX_DELETE 3
#define OTA_IDX_INSERT 4
#define OTA_UPDATE     5

static int otaGetUpdateStmt(
  sqlite3ota *p, 
  OtaObjIter *pIter, 
  const char *zMask,
  sqlite3_stmt **ppStmt
){
  if( pIter->pUpdate && strcmp(zMask, pIter->zMask)==0 ){
    *ppStmt = pIter->pUpdate;
  }else{
    char *zWhere = otaObjIterGetWhere(p, pIter);
    char *zSet = otaObjIterGetSetlist(p, pIter, zMask);
    char *zUpdate = 0;
    sqlite3_finalize(pIter->pUpdate);
    pIter->pUpdate = 0;
    if( p->rc==SQLITE_OK ){
      zUpdate = sqlite3_mprintf("UPDATE \"%w\" SET %s WHERE %s", 
          (pIter->eType==OTA_PK_VTAB ? pIter->zTbl : "ota_imposter"),
          zSet, zWhere
      );
      p->rc = prepareFreeAndCollectError(
          p->db, &pIter->pUpdate, &p->zErrmsg, zUpdate
      );
      *ppStmt = pIter->pUpdate;
    }
    if( p->rc==SQLITE_OK ){
      memcpy(pIter->zMask, zMask, pIter->nTblCol);
    }
    sqlite3_free(zWhere);
    sqlite3_free(zSet);
  }
  return p->rc;
}

/*
** Open the database handle and attach the OTA database as "ota". If an
** error occurs, leave an error code and message in the OTA handle.
*/
static void otaOpenDatabase(sqlite3ota *p){
  assert( p->rc==SQLITE_OK );
  sqlite3_close(p->db);
  p->db = 0;

  p->rc = sqlite3_open(p->zTarget, &p->db);
  if( p->rc ){
    p->zErrmsg = sqlite3_mprintf("%s", sqlite3_errmsg(p->db));
  }
  otaMPrintfExec(p, "ATTACH %Q AS ota", p->zOta);
}

/*
** This routine is a copy of the sqlite3FileSuffix3() routine from the core.
** It is a no-op unless SQLITE_ENABLE_8_3_NAMES is defined.
**
** If SQLITE_ENABLE_8_3_NAMES is set at compile-time and if the database
** filename in zBaseFilename is a URI with the "8_3_names=1" parameter and
** if filename in z[] has a suffix (a.k.a. "extension") that is longer than
** three characters, then shorten the suffix on z[] to be the last three
** characters of the original suffix.
**
** If SQLITE_ENABLE_8_3_NAMES is set to 2 at compile-time, then always
** do the suffix shortening regardless of URI parameter.
**
** Examples:
**
**     test.db-journal    =>   test.nal
**     test.db-wal        =>   test.wal
**     test.db-shm        =>   test.shm
**     test.db-mj7f3319fa =>   test.9fa
*/
static void otaFileSuffix3(const char *zBase, char *z){
#ifdef SQLITE_ENABLE_8_3_NAMES
#if SQLITE_ENABLE_8_3_NAMES<2
  if( sqlite3_uri_boolean(zBase, "8_3_names", 0) )
#endif
  {
    int i, sz;
    sz = sqlite3Strlen30(z);
    for(i=sz-1; i>0 && z[i]!='/' && z[i]!='.'; i--){}
    if( z[i]=='.' && ALWAYS(sz>i+4) ) memmove(&z[i+1], &z[sz-3], 4);
  }
#endif
}

/*
** The OTA handle is currently in OTA_STAGE_OAL state, with a SHARED lock
** on the database file. This proc moves the *-oal file to the *-wal path,
** then reopens the database file (this time in vanilla, non-oal, WAL mode).
** If an error occurs, leave an error code and error message in the ota 
** handle.
*/
static void otaMoveOalFile(sqlite3ota *p){
  const char *zBase = sqlite3_db_filename(p->db, "main");

  char *zWal = sqlite3_mprintf("%s-wal", zBase);
  char *zOal = sqlite3_mprintf("%s-oal", zBase);

  assert( p->eStage==OTA_STAGE_OAL );
  assert( p->rc==SQLITE_OK && p->zErrmsg==0 );
  if( zWal==0 || zOal==0 ){
    p->rc = SQLITE_NOMEM;
  }else{
    /* Move the *-oal file to *-wal. At this point connection p->db is
    ** holding a SHARED lock on the target database file (because it is
    ** in WAL mode). So no other connection may be writing the db.  */
    otaFileSuffix3(zBase, zWal);
    otaFileSuffix3(zBase, zOal);
    rename(zOal, zWal);

    /* Re-open the databases. */
    otaObjIterFinalize(&p->objiter);
    otaOpenDatabase(p);
    p->eStage = OTA_STAGE_CKPT;
  }

  sqlite3_free(zWal);
  sqlite3_free(zOal);
}

/*
** The SELECT statement iterating through the keys for the current object
** (p->objiter.pSelect) currently points to a valid row. This function
** determines the type of operation requested by this row and returns
** one of the following values to indicate the result:
**
**     * OTA_INSERT
**     * OTA_DELETE
**     * OTA_IDX_DELETE
**     * OTA_UPDATE
**
** If OTA_UPDATE is returned, then output variable *pzMask is set to
** point to the text value indicating the columns to update.
**
** If the ota_control field contains an invalid value, an error code and
** message are left in the OTA handle and zero returned.
*/
static int otaStepType(sqlite3ota *p, const char **pzMask){
  int iCol = p->objiter.nCol;     /* Index of ota_control column */
  int res = 0;                    /* Return value */

  switch( sqlite3_column_type(p->objiter.pSelect, iCol) ){
    case SQLITE_INTEGER: {
      int iVal = sqlite3_column_int(p->objiter.pSelect, iCol);
      if( iVal==0 ){
        res = OTA_INSERT;
      }else if( iVal==1 ){
        res = OTA_DELETE;
      }else if( iVal==2 ){
        res = OTA_IDX_DELETE;
      }else if( iVal==3 ){
        res = OTA_IDX_INSERT;
      }
      break;
    }

    case SQLITE_TEXT:
      *pzMask = (const char*)sqlite3_column_text(p->objiter.pSelect, iCol);
      res = OTA_UPDATE;
      break;

    default:
      break;
  }

  if( res==0 ){
    otaBadControlError(p);
  }
  return res;
}

#ifdef SQLITE_DEBUG
static void assertColumnName(sqlite3_stmt *pStmt, int iCol, const char *zName){
  const char *zCol = sqlite3_column_name(pStmt, iCol);
  assert( 0==sqlite3_stricmp(zName, zCol) );
}
#else
# define assertColumnName(x,y,z)
#endif

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
  const char *zMask = 0;
  int i;
  int eType = otaStepType(p, &zMask);

  if( eType ){
    assert( eType!=OTA_UPDATE || pIter->zIdx==0 );

    if( pIter->zIdx==0 && eType==OTA_IDX_DELETE ){
      otaBadControlError(p);
    }
    else if( 
        eType==OTA_INSERT 
     || eType==OTA_DELETE
     || eType==OTA_IDX_DELETE 
     || eType==OTA_IDX_INSERT
    ){
      sqlite3_value *pVal;
      sqlite3_stmt *pWriter;

      assert( eType!=OTA_UPDATE );
      assert( eType!=OTA_DELETE || pIter->zIdx==0 );

      if( eType==OTA_IDX_DELETE || eType==OTA_DELETE ){
        pWriter = pIter->pDelete;
      }else{
        pWriter = pIter->pInsert;
      }

      for(i=0; i<pIter->nCol; i++){
        /* If this is an INSERT into a table b-tree and the table has an
        ** explicit INTEGER PRIMARY KEY, check that this is not an attempt
        ** to write a NULL into the IPK column. That is not permitted.  */
        if( eType==OTA_INSERT 
         && pIter->zIdx==0 && pIter->eType==OTA_PK_IPK && pIter->abTblPk[i] 
         && sqlite3_column_type(pIter->pSelect, i)==SQLITE_NULL
        ){
          p->rc = SQLITE_MISMATCH;
          p->zErrmsg = sqlite3_mprintf("datatype mismatch");
          goto step_out;
        }

        if( eType==SQLITE_DELETE && pIter->zIdx==0 && pIter->abTblPk[i]==0 ){
          continue;
        }

        pVal = sqlite3_column_value(pIter->pSelect, i);
        sqlite3_bind_value(pWriter, i+1, pVal);
      }
      if( pIter->zIdx==0
       && (pIter->eType==OTA_PK_VTAB || pIter->eType==OTA_PK_NONE) 
      ){
        /* For a virtual table, or a table with no primary key, the 
        ** SELECT statement is:
        **
        **   SELECT <cols>, ota_control, ota_rowid FROM ....
        **
        ** Hence column_value(pIter->nCol+1).
        */
        assertColumnName(pIter->pSelect, pIter->nCol+1, "ota_rowid");
        pVal = sqlite3_column_value(pIter->pSelect, pIter->nCol+1);
        sqlite3_bind_value(pWriter, pIter->nCol+1, pVal);
      }
      sqlite3_step(pWriter);
      p->rc = resetAndCollectError(pWriter, &p->zErrmsg);
    }else if( eType==OTA_UPDATE ){
      sqlite3_value *pVal;
      sqlite3_stmt *pUpdate = 0;
      otaGetUpdateStmt(p, pIter, zMask, &pUpdate);
      if( pUpdate ){
        for(i=0; p->rc==SQLITE_OK && i<pIter->nCol; i++){
          pVal = sqlite3_column_value(pIter->pSelect, i);
          sqlite3_bind_value(pUpdate, i+1, pVal);
        }
        if( pIter->eType==OTA_PK_VTAB || pIter->eType==OTA_PK_NONE ){
          /* Bind the ota_rowid value to column _rowid_ */
          assertColumnName(pIter->pSelect, pIter->nCol+1, "ota_rowid");
          pVal = sqlite3_column_value(pIter->pSelect, pIter->nCol+1);
          sqlite3_bind_value(pUpdate, pIter->nCol+1, pVal);
        }
        sqlite3_step(pUpdate);
        p->rc = resetAndCollectError(pUpdate, &p->zErrmsg);
      }
    }else{
      /* no-op */
      assert( eType==OTA_DELETE && pIter->zIdx );
    }
  }

 step_out:
  return p->rc;
}

/*
** Increment the schema cookie of the main database opened by p->db.
*/
static void otaIncrSchemaCookie(sqlite3ota *p){
  int iCookie = 1000000;
  sqlite3_stmt *pStmt;

  assert( p->rc==SQLITE_OK && p->zErrmsg==0 );
  p->rc = prepareAndCollectError(p->db, &pStmt, &p->zErrmsg, 
      "PRAGMA schema_version"
  );
  if( p->rc==SQLITE_OK ){
    if( SQLITE_ROW==sqlite3_step(pStmt) ){
      iCookie = sqlite3_column_int(pStmt, 0);
    }
    p->rc = sqlite3_finalize(pStmt);
  }
  if( p->rc==SQLITE_OK ){
    otaMPrintfExec(p, "PRAGMA schema_version = %d", iCookie+1);
  }
}

/*
** Step the OTA object.
*/
int sqlite3ota_step(sqlite3ota *p){
  if( p ){
    switch( p->eStage ){
      case OTA_STAGE_OAL: {
        OtaObjIter *pIter = &p->objiter;
        while( p && p->rc==SQLITE_OK && pIter->zTbl ){

          if( pIter->bCleanup ){
            /* Clean up the ota_tmp_xxx table for the previous table. It 
            ** cannot be dropped as there are currently active SQL statements.
            ** But the contents can be deleted.  */
            if( pIter->eType!=OTA_PK_VTAB ){
              otaMPrintfExec(p, "DELETE FROM ota.'ota_tmp_%q'", pIter->zTbl);
            }
          }else{
            otaObjIterPrepareAll(p, pIter, 0);

            /* Advance to the next row to process. */
            if( p->rc==SQLITE_OK ){
              int rc = sqlite3_step(pIter->pSelect);
              if( rc==SQLITE_ROW ){
                p->nProgress++;
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
          p->nProgress++;
          otaIncrSchemaCookie(p);
          if( p->rc==SQLITE_OK ){
            p->rc = sqlite3_exec(p->db, "COMMIT", 0, 0, &p->zErrmsg);
          }
          if( p->rc==SQLITE_OK ){
            otaMoveOalFile(p);
          }
        }
        break;
      }

      case OTA_STAGE_CKPT: {

        if( p->rc==SQLITE_OK && p->pCkpt==0 ){
          p->rc = sqlite3_ckpt_open(p->db, 0, 0, &p->pCkpt);
        }
        if( p->rc==SQLITE_OK ){
          if( p->pCkpt==0 ){
            p->eStage = OTA_STAGE_DONE;
            p->rc = SQLITE_DONE;
          }else if( SQLITE_OK!=sqlite3_ckpt_step(p->pCkpt) ){
            p->rc = sqlite3_ckpt_close(p->pCkpt, 0, 0);
            p->pCkpt = 0;
            if( p->rc==SQLITE_OK ){
              p->eStage = OTA_STAGE_DONE;
              p->rc = SQLITE_DONE;
            }
          }
          p->nProgress++;
        }

        break;
      }

      default:
        break;
    }
    return p->rc;
  }else{
    return SQLITE_NOMEM;
  }
}

static void otaSaveTransactionState(sqlite3ota *p){
  sqlite3_stmt *pInsert;
  int rc;

  assert( (p->rc==SQLITE_OK || p->rc==SQLITE_DONE) && p->zErrmsg==0 );
  rc = prepareFreeAndCollectError(p->db, &pInsert, &p->zErrmsg, 
      sqlite3_mprintf(
        "INSERT OR REPLACE INTO ota.ota_state(k, v) VALUES "
        "(%d, %d), "
        "(%d, %Q), "
        "(%d, %Q), "
        "(%d, %d), "
        "(%d, %lld), "
        "(%d, ?) ",
        OTA_STATE_STAGE, p->eStage,
        OTA_STATE_TBL, p->objiter.zTbl, 
        OTA_STATE_IDX, p->objiter.zIdx, 
        OTA_STATE_ROW, p->nStep, 
        OTA_STATE_PROGRESS, p->nProgress,
        OTA_STATE_CKPT
      )
  );
  assert( pInsert==0 || rc==SQLITE_OK );
  if( rc==SQLITE_OK ){
    if( p->pCkpt ){
      unsigned char *pCkptState = 0;
      int nCkptState = 0;
      rc = sqlite3_ckpt_close(p->pCkpt, &pCkptState, &nCkptState);
      p->pCkpt = 0;
      sqlite3_bind_blob(pInsert, 1, pCkptState, nCkptState, SQLITE_TRANSIENT);
      sqlite3_free(pCkptState);
    }
  }
  if( rc==SQLITE_OK ){
    sqlite3_step(pInsert);
    rc = sqlite3_finalize(pInsert);
  }else{
    sqlite3_finalize(pInsert);
  }

  if( rc!=SQLITE_OK ){
    p->rc = rc;
  }
}

static void otaFreeState(OtaState *p){
  if( p ){
    sqlite3_free(p->zTbl);
    sqlite3_free(p->zIdx);
    sqlite3_free(p->pCkptState);
    sqlite3_free(p);
  }
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
  const char *zSelect = "SELECT k, v FROM ota.ota_state";
  OtaState *pRet = 0;
  sqlite3_stmt *pStmt;
  int rc;
  int rc2;

  assert( p->rc==SQLITE_OK );
  pRet = (OtaState*)sqlite3_malloc(sizeof(OtaState));
  if( pRet==0 ){
    rc = SQLITE_NOMEM;
  }else{
    memset(pRet, 0, sizeof(OtaState));
    rc = prepareAndCollectError(p->db, &pStmt, &p->zErrmsg, zSelect);
  }

  while( rc==SQLITE_OK && SQLITE_ROW==sqlite3_step(pStmt) ){
    switch( sqlite3_column_int(pStmt, 0) ){
      case OTA_STATE_STAGE:
        pRet->eStage = sqlite3_column_int(pStmt, 1);
        if( pRet->eStage!=OTA_STAGE_OAL
         && pRet->eStage!=OTA_STAGE_COPY
         && pRet->eStage!=OTA_STAGE_CKPT
        ){
          p->rc = SQLITE_CORRUPT;
        }
        break;

      case OTA_STATE_TBL:
        pRet->zTbl = otaStrndup((char*)sqlite3_column_text(pStmt, 1), -1, &rc);
        break;

      case OTA_STATE_IDX:
        pRet->zIdx = otaStrndup((char*)sqlite3_column_text(pStmt, 1), -1, &rc);
        break;

      case OTA_STATE_ROW:
        pRet->nRow = sqlite3_column_int(pStmt, 1);
        break;

      case OTA_STATE_PROGRESS:
        pRet->nProgress = sqlite3_column_int64(pStmt, 1);
        break;

      case OTA_STATE_CKPT:
        pRet->nCkptState = sqlite3_column_bytes(pStmt, 1);
        pRet->pCkptState = (unsigned char*)otaStrndup(
            (char*)sqlite3_column_blob(pStmt, 1), pRet->nCkptState, &rc
        );
        break;

      default:
        rc = SQLITE_CORRUPT;
        break;
    }
  }
  rc2 = sqlite3_finalize(pStmt);
  if( rc==SQLITE_OK ) rc = rc2;

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
    int rc = SQLITE_OK;

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
  int nOta = strlen(zOta);

  p = (sqlite3ota*)sqlite3_malloc(sizeof(sqlite3ota)+nTarget+1+nOta+1);
  if( p ){
    OtaState *pState = 0;

    /* Open the target database */
    memset(p, 0, sizeof(sqlite3ota));
    p->zTarget = (char*)&p[1];
    memcpy(p->zTarget, zTarget, nTarget+1);
    p->zOta = &p->zTarget[nTarget+1];
    memcpy(p->zOta, zOta, nOta+1);
    otaOpenDatabase(p);

    /* If it has not already been created, create the ota_state table */
    if( p->rc==SQLITE_OK ){
      p->rc = sqlite3_exec(p->db, OTA_CREATE_STATE, 0, 0, &p->zErrmsg);
    }

    if( p->rc==SQLITE_OK ){
      pState = otaLoadState(p);
      assert( pState || p->rc!=SQLITE_OK );
      if( p->rc==SQLITE_OK ){
        if( pState->eStage==0 ){ 
          otaDeleteOalFile(p);
          p->eStage = 1;
        }else{
          p->eStage = pState->eStage;
        }
        p->nProgress = pState->nProgress;
      }
    }
    assert( p->rc!=SQLITE_OK || p->eStage!=0 );

    if( p->rc==SQLITE_OK ){
      if( p->eStage==OTA_STAGE_OAL ){
        const char *zScript =
          "PRAGMA journal_mode=off;"
          "PRAGMA pager_ota_mode=1;"
          "PRAGMA ota_mode=1;"
          "BEGIN IMMEDIATE;"
        ;
        p->rc = sqlite3_exec(p->db, zScript, 0, 0, &p->zErrmsg);
  
        /* Point the object iterator at the first object */
        if( p->rc==SQLITE_OK ){
          p->rc = otaObjIterFirst(p, &p->objiter);
        }
  
        if( p->rc==SQLITE_OK ){
          otaLoadTransactionState(p, pState);
        }
      }else if( p->eStage==OTA_STAGE_CKPT ){
        p->rc = sqlite3_ckpt_open(
            p->db, pState->pCkptState, pState->nCkptState, &p->pCkpt
        );
        if( p->rc==SQLITE_MISMATCH || (p->rc==SQLITE_OK && p->pCkpt==0) ){
          p->eStage = OTA_STAGE_DONE;
          p->rc = SQLITE_DONE;
        }
      }else if( p->eStage==OTA_STAGE_DONE ){
        p->rc = SQLITE_DONE;
      }
    }

    otaFreeState(pState);
  }

  return p;
}

/*
** Return the database handle used by pOta.
*/
sqlite3 *sqlite3ota_db(sqlite3ota *pOta){
  return (pOta ? pOta->db : 0);
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
    if( p->rc==SQLITE_OK || p->rc==SQLITE_DONE ){
      assert( p->zErrmsg==0 );
      otaSaveTransactionState(p);
    }

    /* Close any open statement handles. */
    otaObjIterFinalize(&p->objiter);

    /* Commit the transaction to the *-oal file. */
    if( p->rc==SQLITE_OK && p->eStage==OTA_STAGE_OAL ){
      p->rc = sqlite3_exec(p->db, "COMMIT", 0, 0, &p->zErrmsg);
    }

    if( p->rc==SQLITE_OK && p->eStage==OTA_STAGE_CKPT ){
      p->rc = sqlite3_exec(p->db, "PRAGMA pager_ota_mode=2", 0, 0, &p->zErrmsg);
    }

    /* Close the open database handle */
    if( p->pCkpt ) sqlite3_ckpt_close(p->pCkpt, 0, 0);
    sqlite3_close(p->db);

    rc = p->rc;
    *pzErrmsg = p->zErrmsg;
    sqlite3_free(p);
  }else{
    rc = SQLITE_NOMEM;
    *pzErrmsg = 0;
  }
  return rc;
}

/*
** Return the total number of key-value operations (inserts, deletes or 
** updates) that have been performed on the target database since the
** current OTA update was started.
*/
sqlite3_int64 sqlite3ota_progress(sqlite3ota *pOta){
  return pOta->nProgress;
}


/**************************************************************************/

#ifdef SQLITE_TEST 

#include <tcl.h>

/* From main.c (apparently...) */
extern const char *sqlite3ErrName(int);

void test_ota_delta(sqlite3_context *pCtx, int nArg, sqlite3_value **apVal){
  Tcl_Interp *interp = (Tcl_Interp*)sqlite3_user_data(pCtx);
  Tcl_Obj *pScript;
  int i;

  pScript = Tcl_NewObj();
  Tcl_IncrRefCount(pScript);
  Tcl_ListObjAppendElement(0, pScript, Tcl_NewStringObj("ota_delta", -1));
  for(i=0; i<nArg; i++){
    sqlite3_value *pIn = apVal[i];
    const char *z = (const char*)sqlite3_value_text(pIn);
    Tcl_ListObjAppendElement(0, pScript, Tcl_NewStringObj(z, -1));
  }

  if( TCL_OK==Tcl_EvalObjEx(interp, pScript, TCL_GLOBAL_ONLY) ){
    const char *z = Tcl_GetStringResult(interp);
    sqlite3_result_text(pCtx, z, -1, SQLITE_TRANSIENT);
  }else{
    Tcl_BackgroundError(interp);
  }

  Tcl_DecrRefCount(pScript);
}


static int test_sqlite3ota_cmd(
  ClientData clientData,
  Tcl_Interp *interp,
  int objc,
  Tcl_Obj *CONST objv[]
){
  int ret = TCL_OK;
  sqlite3ota *pOta = (sqlite3ota*)clientData;
  const char *azMethod[] = { "step", "close", "create_ota_delta", 0 };
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

    case 2: /* create_ota_delta */ {
      sqlite3 *db = sqlite3ota_db(pOta);
      int rc = sqlite3_create_function(
          db, "ota_delta", -1, SQLITE_UTF8, (void*)interp, test_ota_delta, 0, 0
      );
      Tcl_SetObjResult(interp, Tcl_NewStringObj(sqlite3ErrName(rc), -1));
      sqlite3_exec(db, "PRAGMA vdbe_trace = 1", 0, 0, 0);
      ret = (rc==SQLITE_OK ? TCL_OK : TCL_ERROR);
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
#else   /* !SQLITE_CORE || SQLITE_ENABLE_OTA */
# ifdef SQLITE_TEST
#include <tcl.h>
int SqliteOta_Init(Tcl_Interp *interp){ return TCL_OK; }
# endif
#endif 




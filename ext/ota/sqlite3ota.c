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
** Swap two objects of type TYPE.
*/
#if !defined(SQLITE_AMALGAMATION)
# define SWAP(TYPE,A,B) {TYPE t=A; A=B; B=t;}
#endif

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
** OTA_STATE_COOKIE:
**   Valid if STAGE==1. The current change-counter cookie value in the 
**   target db file.
*/
#define OTA_STATE_STAGE       1
#define OTA_STATE_TBL         2
#define OTA_STATE_IDX         3
#define OTA_STATE_ROW         4
#define OTA_STATE_PROGRESS    5
#define OTA_STATE_CKPT        6
#define OTA_STATE_COOKIE      7

#define OTA_STAGE_OAL         1
#define OTA_STAGE_MOVE        2
#define OTA_STAGE_CAPTURE     3
#define OTA_STAGE_CKPT        4
#define OTA_STAGE_DONE        5


#define OTA_CREATE_STATE "CREATE TABLE IF NOT EXISTS ota.ota_state"        \
                             "(k INTEGER PRIMARY KEY, v)"

typedef struct OtaState OtaState;
typedef struct OtaObjIter OtaObjIter;
typedef struct ota_vfs ota_vfs;
typedef struct ota_file ota_file;

#if !defined(SQLITE_AMALGAMATION)
typedef unsigned int u32;
typedef unsigned char u8;
typedef sqlite3_int64 i64;
#endif

/*
** These values must match the values defined in wal.c for the equivalent
** locks. These are not magic numbers as they are part of the SQLite file
** format.
*/
#define WAL_LOCK_WRITE  0
#define WAL_LOCK_CKPT   1
#define WAL_LOCK_READ0  3

/*
** A structure to store values read from the ota_state table in memory.
*/
struct OtaState {
  int eStage;
  char *zTbl;
  char *zIdx;
  i64 iWalCksum;
  int nRow;
  i64 nProgress;
  u32 iCookie;
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
  char **azTblCol;                /* Array of unquoted target column names */
  char **azTblType;               /* Array of target column types */
  int *aiSrcOrder;                /* src table col -> target table col */
  u8 *abTblPk;                    /* Array of flags, set on target PK columns */
  u8 *abNotNull;                  /* Array of flags, set on NOT NULL columns */
  int eType;                      /* Table type - an OTA_PK_XXX value */

  /* Output variables. zTbl==0 implies EOF. */
  int bCleanup;                   /* True in "cleanup" state */
  const char *zTbl;               /* Name of target db table */
  const char *zIdx;               /* Name of target db index (or null) */
  int iTnum;                      /* Root page of current object */
  int iPkTnum;                    /* If eType==EXTERNAL, root of PK index */
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
**     0: Table does not exist (error)
**     1: Table has an implicit rowid.
**     2: Table has an explicit IPK column.
**     3: Table has an external PK index.
**     4: Table is WITHOUT ROWID.
**     5: Table is a virtual table.
*/
#define OTA_PK_NOTABLE        0
#define OTA_PK_NONE           1
#define OTA_PK_IPK            2
#define OTA_PK_EXTERNAL       3
#define OTA_PK_WITHOUT_ROWID  4
#define OTA_PK_VTAB           5


typedef struct OtaFrame OtaFrame;
struct OtaFrame {
  u32 iDbPage;
  u32 iWalFrame;
};

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
  const char *zVfsName;           /* Name of automatically created ota vfs */
  ota_file *pTargetFd;            /* File handle open on target db */

  /* The following state variables are used as part of the incremental
  ** checkpoint stage (eStage==OTA_STAGE_CKPT). See function otaSetupCkpt()
  ** for details.  */
  u32 iMaxFrame;                  /* Largest iWalFrame value in aFrame[] */
  u32 mLock;
  int nFrame;                     /* Entries in aFrame[] array */
  int nFrameAlloc;                /* Allocated size of aFrame[] array */
  OtaFrame *aFrame;
  int pgsz;
  u8 *aBuf;
  i64 iWalCksum;
};

struct ota_vfs {
  sqlite3_vfs base;               /* ota VFS shim methods */
  sqlite3_vfs *pRealVfs;          /* Underlying VFS */
  sqlite3_mutex *mutex;           /* Mutex to protect pMain */
  ota_file *pMain;                /* Linked list of main db files */
};

struct ota_file {
  sqlite3_file base;              /* sqlite3_file methods */
  sqlite3_file *pReal;            /* Underlying file handle */
  ota_vfs *pOtaVfs;               /* Pointer to the ota_vfs object */
  sqlite3ota *pOta;               /* Pointer to ota object (ota target only) */

  int openFlags;                  /* Flags this file was opened with */
  u32 iCookie;                    /* Cookie value for main db files */
  u8 iWriteVer;                   /* "write-version" value for main db files */

  int nShm;                       /* Number of entries in apShm[] array */
  char **apShm;                   /* Array of mmap'd *-shm regions */
  char *zDel;                     /* Delete this when closing file */

  const char *zWal;               /* Wal filename for this main db file */
  ota_file *pWalFd;               /* Wal file descriptor for this main db */
  ota_file *pMainNext;            /* Next MAIN_DB file */
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
  pIter->aiSrcOrder = 0;
  pIter->abTblPk = 0;
  pIter->abNotNull = 0;
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
          pIter->iTnum = sqlite3_column_int(pIter->pTblIter, 1);
          rc = pIter->zTbl ? SQLITE_OK : SQLITE_NOMEM;
        }
      }else{
        if( pIter->zIdx==0 ){
          sqlite3_stmt *pIdx = pIter->pIdxIter;
          rc = sqlite3_bind_text(pIdx, 1, pIter->zTbl, -1, SQLITE_STATIC);
        }
        if( rc==SQLITE_OK ){
          rc = sqlite3_step(pIter->pIdxIter);
          if( rc!=SQLITE_ROW ){
            rc = sqlite3_reset(pIter->pIdxIter);
            pIter->bCleanup = 1;
            pIter->zIdx = 0;
          }else{
            pIter->zIdx = (const char*)sqlite3_column_text(pIter->pIdxIter, 0);
            pIter->iTnum = sqlite3_column_int(pIter->pIdxIter, 1);
            pIter->bUnique = sqlite3_column_int(pIter->pIdxIter, 2);
            rc = pIter->zIdx ? SQLITE_OK : SQLITE_NOMEM;
          }
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

static void *otaMalloc(sqlite3ota *p, int nByte){
  void *pRet = 0;
  if( p->rc==SQLITE_OK ){
    pRet = sqlite3_malloc(nByte);
    if( pRet==0 ){
      p->rc = SQLITE_NOMEM;
    }else{
      memset(pRet, 0, nByte);
    }
  }
  return pRet;
}


/*
** Allocate and zero the pIter->azTblCol[] and abTblPk[] arrays so that
** there is room for at least nCol elements. If an OOM occurs, store an
** error code in the OTA handle passed as the first argument.
*/
static void otaAllocateIterArrays(sqlite3ota *p, OtaObjIter *pIter, int nCol){
  int nByte = (2*sizeof(char*) + sizeof(int) + 2*sizeof(u8)) * nCol;
  char **azNew;

  azNew = (char**)otaMalloc(p, nByte);
  if( azNew ){
    pIter->azTblCol = azNew;
    pIter->azTblType = &azNew[nCol];
    pIter->aiSrcOrder = (int*)&pIter->azTblType[nCol];
    pIter->abTblPk = (u8*)&pIter->aiSrcOrder[nCol];
    pIter->abNotNull = (u8*)&pIter->abTblPk[nCol];
  }
}

static char *otaStrndup(const char *zStr, int *pRc){
  char *zRet = 0;

  assert( *pRc==SQLITE_OK );
  if( zStr ){
    int nCopy = strlen(zStr) + 1;
    zRet = (char*)sqlite3_malloc(nCopy);
    if( zRet ){
      memcpy(zRet, zStr, nCopy);
    }else{
      *pRc = SQLITE_NOMEM;
    }
  }

  return zRet;
}

/* Determine the type of a table.
**
**   peType is of type (int*), a pointer to an output parameter of type
**   (int). This call sets the output parameter as follows, depending
**   on the type of the table specified by parameters dbName and zTbl.
**
**     OTA_PK_NOTABLE:       No such table.
**     OTA_PK_NONE:          Table has an implicit rowid.
**     OTA_PK_IPK:           Table has an explicit IPK column.
**     OTA_PK_EXTERNAL:      Table has an external PK index.
**     OTA_PK_WITHOUT_ROWID: Table is WITHOUT ROWID.
**     OTA_PK_VTAB:          Table is a virtual table.
**
**   Argument *piPk is also of type (int*), and also points to an output
**   parameter. Unless the table has an external primary key index 
**   (i.e. unless *peType is set to 3), then *piPk is set to zero. Or,
**   if the table does have an external primary key index, then *piPk
**   is set to the root page number of the primary key index before
**   returning.
**
** ALGORITHM:
**
**   if( no entry exists in sqlite_master ){
**     return OTA_PK_NOTABLE
**   }else if( sql for the entry starts with "CREATE VIRTUAL" ){
**     return OTA_PK_VTAB
**   }else if( "PRAGMA index_list()" for the table contains a "pk" index ){
**     if( the index that is the pk exists in sqlite_master ){
**       *piPK = rootpage of that index.
**       return OTA_PK_EXTERNAL
**     }else{
**       return OTA_PK_WITHOUT_ROWID
**     }
**   }else if( "PRAGMA table_info()" lists one or more "pk" columns ){
**     return OTA_PK_IPK
**   }else{
**     return OTA_PK_NONE
**   }
*/
static void otaTableType(
  sqlite3ota *p,
  const char *zTab,
  int *peType,
  int *piPk
){
  /*
  ** 0) SELECT count(*) FROM sqlite_master where name=%Q AND IsVirtual(%Q)
  ** 1) PRAGMA index_list = ?
  ** 2) SELECT count(*) FROM sqlite_master where name=%Q 
  ** 3) PRAGMA table_info = ?
  */
  sqlite3_stmt *aStmt[4] = {0, 0, 0, 0};

  *peType = OTA_PK_NOTABLE;
  *piPk = 0;

  assert( p->rc==SQLITE_OK );
  p->rc = prepareFreeAndCollectError(p->db, &aStmt[0], &p->zErrmsg, 
    sqlite3_mprintf(
          "SELECT (sql LIKE 'create virtual%%')"
          "  FROM sqlite_master"
          " WHERE name=%Q", zTab
  ));
  if( p->rc!=SQLITE_OK || sqlite3_step(aStmt[0])!=SQLITE_ROW ){
    /* Either an error, or no such table. */
    goto otaTableType_end;
  }
  if( sqlite3_column_int(aStmt[0], 0) ){
    *peType = OTA_PK_VTAB;                     /* virtual table */
    goto otaTableType_end;
  }

  p->rc = prepareFreeAndCollectError(p->db, &aStmt[1], &p->zErrmsg, 
    sqlite3_mprintf("PRAGMA index_list=%Q",zTab)
  );
  if( p->rc ) goto otaTableType_end;
  while( sqlite3_step(aStmt[1])==SQLITE_ROW ){
    const u8 *zOrig = sqlite3_column_text(aStmt[1], 3);
    const u8 *zIdx = sqlite3_column_text(aStmt[1], 1);
    if( zOrig && zIdx && zOrig[0]=='p' ){
      p->rc = prepareFreeAndCollectError(p->db, &aStmt[2], &p->zErrmsg, 
          sqlite3_mprintf(
            "SELECT rootpage FROM sqlite_master WHERE name = %Q", zIdx
      ));
      if( p->rc==SQLITE_OK ){
        if( sqlite3_step(aStmt[2])==SQLITE_ROW ){
          *piPk = sqlite3_column_int(aStmt[2], 0);
          *peType = OTA_PK_EXTERNAL;
        }else{
          *peType = OTA_PK_WITHOUT_ROWID;
        }
      }
      goto otaTableType_end;
    }
  }

  p->rc = prepareFreeAndCollectError(p->db, &aStmt[3], &p->zErrmsg, 
    sqlite3_mprintf("PRAGMA table_info=%Q",zTab)
  );
  if( p->rc==SQLITE_OK ){
    while( sqlite3_step(aStmt[3])==SQLITE_ROW ){
      if( sqlite3_column_int(aStmt[3],5)>0 ){
        *peType = OTA_PK_IPK;                /* explicit IPK column */
        goto otaTableType_end;
      }
    }
    *peType = OTA_PK_NONE;
  }

otaTableType_end: {
    int i;
    for(i=0; i<sizeof(aStmt)/sizeof(aStmt[0]); i++){
      int rc2 = sqlite3_finalize(aStmt[i]);
      if( p->rc==SQLITE_OK ) p->rc = rc2;
    }
  }
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
    otaTableType(p, pIter->zTbl, &pIter->eType, &pIter->iPkTnum);
    if( p->rc ) return p->rc;

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
        char *zCopy = otaStrndup(zName, &p->rc);
        pIter->aiSrcOrder[pIter->nTblCol] = pIter->nTblCol;
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
      if( zName==0 ) break;  /* An OOM - finalize() below returns S_NOMEM */
      for(i=iOrder; i<pIter->nTblCol; i++){
        if( 0==strcmp(zName, pIter->azTblCol[i]) ) break;
      }
      if( i==pIter->nTblCol ){
        p->rc = SQLITE_ERROR;
        p->zErrmsg = sqlite3_mprintf("column missing from data_%q: %s",
            pIter->zTbl, zName
        );
      }else{
        int iPk = sqlite3_column_int(pStmt, 5);
        int bNotNull = sqlite3_column_int(pStmt, 3);
        const char *zType = (const char*)sqlite3_column_text(pStmt, 2);

        if( i!=iOrder ){
          SWAP(int, pIter->aiSrcOrder[i], pIter->aiSrcOrder[iOrder]);
          SWAP(char*, pIter->azTblCol[i], pIter->azTblCol[iOrder]);
        }

        pIter->azTblType[iOrder] = otaStrndup(zType, &p->rc);
        pIter->abTblPk[iOrder] = (iPk!=0);
        pIter->abNotNull[iOrder] = (u8)bNotNull || (iPk!=0);
        iOrder++;
      }
    }

    rc2 = sqlite3_finalize(pStmt);
    if( p->rc==SQLITE_OK ) p->rc = rc2;
  }

  return p->rc;
}

/*
** This is a wrapper around "sqlite3_mprintf(zFmt, ...)". If an OOM occurs,
** an error code is stored in the OTA handle passed as the first argument.
**
** If an error has already occurred (p->rc is already set to something other
** than SQLITE_OK), then this function returns NULL without modifying the
** stored error code. In this case it still calls sqlite3_free() on any 
** printf() parameters associated with %z conversions.
*/
static char *otaMPrintf(sqlite3ota *p, const char *zFmt, ...){
  char *zSql = 0;
  va_list ap;
  va_start(ap, zFmt);
  zSql = sqlite3_vmprintf(zFmt, ap);
  if( p->rc==SQLITE_OK ){
    if( zSql==0 ) p->rc = SQLITE_NOMEM;
  }else{
    sqlite3_free(zSql);
    zSql = 0;
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
    zList = otaMPrintf(p, "%z%s\"%w\"", zList, zSep, z);
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
    int iCid = sqlite3_column_int(pXInfo, 1);
    int bDesc = sqlite3_column_int(pXInfo, 3);
    const char *zCollate = (const char*)sqlite3_column_text(pXInfo, 4);
    const char *zCol;
    const char *zType;

    if( iCid<0 ){
      /* An integer primary key. If the table has an explicit IPK, use
      ** its name. Otherwise, use "ota_rowid".  */
      if( pIter->eType==OTA_PK_IPK ){
        int i;
        for(i=0; pIter->abTblPk[i]==0; i++);
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
      const char *zOrder = (bDesc ? " DESC" : "");
      zImpPK = sqlite3_mprintf("%z%s\"ota_imp_%d%w\"%s", 
          zImpPK, zCom, nBind, zCol, zOrder
      );
    }
    zImpCols = sqlite3_mprintf("%z%s\"ota_imp_%d%w\" %s COLLATE %Q", 
        zImpCols, zCom, nBind, zCol, zType, zCollate
    );
    zWhere = sqlite3_mprintf(
        "%z%s\"ota_imp_%d%w\" IS ?", zWhere, zAnd, nBind, zCol
    );
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
      zList = otaMPrintf(p, "%z, %s._rowid_", zList, zObj);
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
    zList = otaMPrintf(p, "_rowid_ = ?%d", pIter->nTblCol+1);
  }else if( pIter->eType==OTA_PK_EXTERNAL ){
    const char *zSep = "";
    int i;
    for(i=0; i<pIter->nTblCol; i++){
      if( pIter->abTblPk[i] ){
        zList = otaMPrintf(p, "%z%sc%d=?%d", zList, zSep, i, i+1);
        zSep = " AND ";
      }
    }
    zList = otaMPrintf(p, 
        "_rowid_ = (SELECT id FROM ota_imposter2 WHERE %z)", zList
    );

  }else{
    const char *zSep = "";
    int i;
    for(i=0; i<pIter->nTblCol; i++){
      if( pIter->abTblPk[i] ){
        const char *zCol = pIter->azTblCol[i];
        zList = otaMPrintf(p, "%z%s\"%w\"=?%d", zList, zSep, zCol, i+1);
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
  p->zErrmsg = sqlite3_mprintf("invalid ota_control value");
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
        char c = zMask[pIter->aiSrcOrder[i]];
        if( c=='x' ){
          zList = otaMPrintf(p, "%z%s\"%w\"=?%d", 
              zList, zSep, pIter->azTblCol[i], i+1
          );
          zSep = ", ";
        }
        if( c=='d' ){
          zList = otaMPrintf(p, "%z%s\"%w\"=ota_delta(\"%w\", ?%d)", 
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
  int nByte = nBind*2 + 1;

  zRet = (char*)otaMalloc(p, nByte);
  if( zRet ){
    int i;
    for(i=0; i<nBind; i++){
      zRet[i*2] = '?';
      zRet[i*2+1] = (i+1==nBind) ? '\0' : ',';
    }
  }
  return zRet;
}

/*
** The iterator currently points to a table (not index) of type 
** OTA_PK_WITHOUT_ROWID. This function creates the PRIMARY KEY 
** declaration for the corresponding imposter table. For example,
** if the iterator points to a table created as:
**
**   CREATE TABLE t1(a, b, c, PRIMARY KEY(b, a DESC)) WITHOUT ROWID
**
** this function returns:
**
**   PRIMARY KEY("b", "a" DESC)
*/
static char *otaWithoutRowidPK(sqlite3ota *p, OtaObjIter *pIter){
  char *z = 0;
  assert( pIter->zIdx==0 );
  if( p->rc==SQLITE_OK ){
    const char *zSep = "PRIMARY KEY(";
    sqlite3_stmt *pXList = 0;     /* PRAGMA index_list = (pIter->zTbl) */
    sqlite3_stmt *pXInfo = 0;     /* PRAGMA index_xinfo = <pk-index> */
    int rc;                       /* sqlite3_finalize() return code */

   
    p->rc = prepareFreeAndCollectError(p->db, &pXList, &p->zErrmsg,
        sqlite3_mprintf("PRAGMA main.index_list = %Q", pIter->zTbl)
    );
    while( p->rc==SQLITE_OK && SQLITE_ROW==sqlite3_step(pXList) ){
      const char *zOrig = (const char*)sqlite3_column_text(pXList,3);
      if( zOrig && strcmp(zOrig, "pk")==0 ){
        const char *zIdx = (const char*)sqlite3_column_text(pXList,1);
        if( zIdx ){
          p->rc = prepareFreeAndCollectError(p->db, &pXInfo, &p->zErrmsg,
              sqlite3_mprintf("PRAGMA main.index_xinfo = %Q", zIdx)
          );
        }
        break;
      }
    }
    rc = sqlite3_finalize(pXList);
    if( p->rc==SQLITE_OK ) p->rc = rc;

    while( p->rc==SQLITE_OK && SQLITE_ROW==sqlite3_step(pXInfo) ){
      if( sqlite3_column_int(pXInfo, 5) ){
        /* int iCid = sqlite3_column_int(pXInfo, 0); */
        const char *zCol = (const char*)sqlite3_column_text(pXInfo, 2);
        const char *zDesc = sqlite3_column_int(pXInfo, 3) ? " DESC" : "";
        z = otaMPrintf(p, "%z%s\"%w\"%s", z, zSep, zCol, zDesc);
        zSep = ", ";
      }
    }
    z = otaMPrintf(p, "%z)", z);
    rc = sqlite3_finalize(pXInfo);
    if( p->rc==SQLITE_OK ) p->rc = rc;
  }
  return z;
}

static void otaFinalize(sqlite3ota *p, sqlite3_stmt *pStmt){
  int rc = sqlite3_finalize(pStmt);
  if( p->rc==SQLITE_OK ) p->rc = rc;
}

/*
** This function creates the second imposter table used when writing to
** a table b-tree where the table has an external primary key. If the
** iterator passed as the second argument does not currently point to
** a table (not index) with an external primary key, this function is a
** no-op. 
**
** Assuming the iterator does point to a table with an external PK, this
** function creates a WITHOUT ROWID imposter table named "ota_imposter2"
** used to access that PK index. For example, if the target table is
** declared as follows:
**
**   CREATE TABLE t1(a, b TEXT, c REAL, PRIMARY KEY(b, c));
**
** then the imposter table schema is:
**
**   CREATE TABLE ota_imposter2(c1 TEXT, c2 REAL, id INTEGER) WITHOUT ROWID;
**
*/
static void otaCreateImposterTable2(sqlite3ota *p, OtaObjIter *pIter){
  if( p->rc==SQLITE_OK && pIter->eType==OTA_PK_EXTERNAL ){
    int tnum = pIter->iPkTnum;    /* Root page of PK index */
    sqlite3_stmt *pQuery = 0;     /* SELECT name ... WHERE rootpage = $tnum */
    const char *zIdx = 0;         /* Name of PK index */
    sqlite3_stmt *pXInfo = 0;     /* PRAGMA main.index_xinfo = $zIdx */
    int rc;

    const char *zComma = "";

    char *zCols = 0;              /* Used to build up list of table cols */
    char *zPk = 0;                /* Used to build up table PK declaration */
    char *zSql = 0;               /* CREATE TABLE statement */

    /* Figure out the name of the primary key index for the current table.
    ** This is needed for the argument to "PRAGMA index_xinfo". Set
    ** zIdx to point to a nul-terminated string containing this name. */
    p->rc = prepareAndCollectError(p->db, &pQuery, &p->zErrmsg, 
        "SELECT name FROM sqlite_master WHERE rootpage = ?"
    );
    if( p->rc==SQLITE_OK ){
      sqlite3_bind_int(pQuery, 1, tnum);
      if( SQLITE_ROW==sqlite3_step(pQuery) ){
        zIdx = (const char*)sqlite3_column_text(pQuery, 0);
      }
    }
    if( zIdx ){
      p->rc = prepareFreeAndCollectError(p->db, &pXInfo, &p->zErrmsg,
          sqlite3_mprintf("PRAGMA main.index_xinfo = %Q", zIdx)
      );
    }
    otaFinalize(p, pQuery);

    while( p->rc==SQLITE_OK && SQLITE_ROW==sqlite3_step(pXInfo) ){
      int bKey = sqlite3_column_int(pXInfo, 5);
      if( bKey ){
        int iCid = sqlite3_column_int(pXInfo, 1);
        int bDesc = sqlite3_column_int(pXInfo, 3);
        const char *zCollate = (const char*)sqlite3_column_text(pXInfo, 4);
        zCols = otaMPrintf(p, "%z%sc%d %s COLLATE %s", zCols, zComma, 
            iCid, pIter->azTblType[iCid], zCollate
        );
        zPk = otaMPrintf(p, "%z%sc%d%s", zPk, zComma, iCid, bDesc?" DESC":"");
        zComma = ", ";
      }
    }
    zCols = otaMPrintf(p, "%z, id INTEGER", zCols);
    rc = sqlite3_finalize(pXInfo);
    if( p->rc==SQLITE_OK ) p->rc = rc;

    zSql = otaMPrintf(p, 
        "CREATE TABLE ota_imposter2(%z, PRIMARY KEY(%z)) WITHOUT ROWID", 
        zCols, zPk
    );
    assert( (zSql==0)==(p->rc!=SQLITE_OK) );
    if( zSql ){
      sqlite3_test_control(SQLITE_TESTCTRL_IMPOSTER, p->db, "main", 1, tnum);
      p->rc = sqlite3_exec(p->db, zSql, 0, 0, &p->zErrmsg);
      sqlite3_test_control(SQLITE_TESTCTRL_IMPOSTER, p->db, "main", 0, 0);
    }
    sqlite3_free(zSql);
  }
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
**     Two imposters are required. The first has the same schema as the
**     target database table, with no PRIMARY KEY or UNIQUE clauses. The
**     second is used to access the PK b-tree index on disk.
*/
static void otaCreateImposterTable(sqlite3ota *p, OtaObjIter *pIter){
  if( p->rc==SQLITE_OK && pIter->eType!=OTA_PK_VTAB ){
    int tnum = pIter->iTnum;
    const char *zComma = "";
    char *zSql = 0;
    int iCol;
    sqlite3_test_control(SQLITE_TESTCTRL_IMPOSTER, p->db, "main", 0, 1);

    for(iCol=0; p->rc==SQLITE_OK && iCol<pIter->nTblCol; iCol++){
      const char *zPk = "";
      const char *zCol = pIter->azTblCol[iCol];
      const char *zColl = 0;

      p->rc = sqlite3_table_column_metadata(
          p->db, "main", pIter->zTbl, zCol, 0, &zColl, 0, 0, 0
      );

      if( pIter->eType==OTA_PK_IPK && pIter->abTblPk[iCol] ){
        /* If the target table column is an "INTEGER PRIMARY KEY", add
        ** "PRIMARY KEY" to the imposter table column declaration. */
        zPk = "PRIMARY KEY ";
      }
      zSql = otaMPrintf(p, "%z%s\"%w\" %s %sCOLLATE %s%s", 
          zSql, zComma, zCol, pIter->azTblType[iCol], zPk, zColl,
          (pIter->abNotNull[iCol] ? " NOT NULL" : "")
      );
      zComma = ", ";
    }

    if( pIter->eType==OTA_PK_WITHOUT_ROWID ){
      char *zPk = otaWithoutRowidPK(p, pIter);
      if( zPk ){
        zSql = otaMPrintf(p, "%z, %z", zSql, zPk);
      }
    }

    zSql = otaMPrintf(p, "CREATE TABLE \"ota_imp_%w\"(%z)%s", 
        pIter->zTbl, zSql, 
        (pIter->eType==OTA_PK_WITHOUT_ROWID ? " WITHOUT ROWID" : "")
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
    const int tnum = pIter->iTnum;
    char *zCollist = 0;           /* List of indexed columns */
    char **pz = &p->zErrmsg;
    const char *zIdx = pIter->zIdx;
    char *zLimit = 0;

    if( nOffset ){
      zLimit = sqlite3_mprintf(" LIMIT -1 OFFSET %d", nOffset);
      if( !zLimit ) p->rc = SQLITE_NOMEM;
    }

    if( zIdx ){
      const char *zTbl = pIter->zTbl;
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
          "CREATE TABLE \"ota_imp_%w\"( %s, PRIMARY KEY( %s ) ) WITHOUT ROWID",
          zTbl, zImposterCols, zImposterPK
      );
      sqlite3_test_control(SQLITE_TESTCTRL_IMPOSTER, p->db, "main", 0, 0);

      /* Create the statement to insert index entries */
      pIter->nCol = nBind;
      if( p->rc==SQLITE_OK ){
        p->rc = prepareFreeAndCollectError(p->db, &pIter->pInsert, &p->zErrmsg,
          sqlite3_mprintf("INSERT INTO \"ota_imp_%w\" VALUES(%s)", zTbl, zBind)
        );
      }

      /* And to delete index entries */
      if( p->rc==SQLITE_OK ){
        p->rc = prepareFreeAndCollectError(p->db, &pIter->pDelete, &p->zErrmsg,
          sqlite3_mprintf("DELETE FROM \"ota_imp_%w\" WHERE %s", zTbl, zWhere)
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
      otaCreateImposterTable2(p, pIter);
      zWrite = (pIter->eType==OTA_PK_VTAB ? "" : "ota_imp_");

      /* Create the INSERT statement to write to the target PK b-tree */
      if( p->rc==SQLITE_OK ){
        p->rc = prepareFreeAndCollectError(p->db, &pIter->pInsert, pz,
            sqlite3_mprintf(
              "INSERT INTO main.\"%s%w\"(%s%s) VALUES(%s)", 
              zWrite, zTbl, zCollist, (bOtaRowid ? ", _rowid_" : ""), zBindings
            )
        );
      }

      /* Create the DELETE statement to write to the target PK b-tree */
      if( p->rc==SQLITE_OK ){
        p->rc = prepareFreeAndCollectError(p->db, &pIter->pDelete, pz,
            sqlite3_mprintf(
              "DELETE FROM main.\"%s%w\" WHERE %s", zWrite, zTbl, zWhere
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
            "CREATE TABLE IF NOT EXISTS ota.'ota_tmp_%q' AS "
            "SELECT *%s FROM ota.'data_%q' WHERE 0;"

            "CREATE TEMP TRIGGER ota_delete_tr BEFORE DELETE ON \"%s%w\" "
            "BEGIN "
            "  INSERT INTO 'ota_tmp_%q'(ota_control, %s%s) VALUES(2, %s);"
            "END;"

            "CREATE TEMP TRIGGER ota_update1_tr BEFORE UPDATE ON \"%s%w\" "
            "BEGIN "
            "  INSERT INTO 'ota_tmp_%q'(ota_control, %s%s) VALUES(2, %s);"
            "END;"

            "CREATE TEMP TRIGGER ota_update2_tr AFTER UPDATE ON \"%s%w\" "
            "BEGIN "
            "  INSERT INTO 'ota_tmp_%q'(ota_control, %s%s) VALUES(3, %s);"
            "END;"
            , zTbl, (pIter->eType==OTA_PK_EXTERNAL ? ", 0 AS ota_rowid" : "")
            , zTbl, 
            zWrite, zTbl, zTbl, zCollist, zOtaRowid, zOldlist,
            zWrite, zTbl, zTbl, zCollist, zOtaRowid, zOldlist,
            zWrite, zTbl, zTbl, zCollist, zOtaRowid, zNewlist
        );
        if( pIter->eType==OTA_PK_EXTERNAL || pIter->eType==OTA_PK_NONE ){
          otaMPrintfExec(p, 
              "CREATE TEMP TRIGGER ota_insert_tr AFTER INSERT ON \"%s%w\" "
              "BEGIN "
              "  INSERT INTO 'ota_tmp_%q'(ota_control, %s, ota_rowid)"
              "  VALUES(0, %s);"
              "END;",
              zWrite, zTbl, zTbl, zCollist, zNewlist
          );
        }
      }

      /* Allocate space required for the zMask field. */
      pIter->zMask = (char*)otaMalloc(p, pIter->nTblCol+1);

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
      const char *zPrefix = "";

      if( pIter->eType!=OTA_PK_VTAB ) zPrefix = "ota_imp_";
      zUpdate = sqlite3_mprintf("UPDATE \"%s%w\" SET %s WHERE %s", 
          zPrefix, pIter->zTbl, zSet, zWhere
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
  int flags = SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE;
  assert( p->rc==SQLITE_OK );
  assert( p->db==0 );

  p->eStage = 0;
  p->rc = sqlite3_open_v2(p->zTarget, &p->db, flags, p->zVfsName);
  if( p->rc ){
    p->zErrmsg = sqlite3_mprintf("%s", sqlite3_errmsg(p->db));
  }else{
    p->rc = sqlite3_file_control(p->db, "main", SQLITE_FCNTL_OTA, (void*)p);
    if( p->rc==SQLITE_OK ){
      otaMPrintfExec(p, "ATTACH %Q AS ota", p->zOta);
    }

    /* Mark the database file just opened as an OTA target database. If 
    ** this call returns SQLITE_NOTFOUND, then the OTA vfs is not in use.
    ** This is an error.  */
    if( p->rc==SQLITE_OK ){
      p->rc = sqlite3_file_control(p->db, "main", SQLITE_FCNTL_OTA, (void*)p);
    }

    if( p->rc==SQLITE_NOTFOUND ){
      p->rc = SQLITE_ERROR;
      p->zErrmsg = sqlite3_mprintf("ota vfs not found");
    }
  }
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
** Return the current wal-index header checksum for the target database 
** as a 64-bit integer.
**
** The checksum is store in the first page of xShmMap memory as an 8-byte 
** blob starting at byte offset 40.
*/
static i64 otaShmChecksum(sqlite3ota *p){
  i64 iRet;
  if( p->rc==SQLITE_OK ){
    sqlite3_file *pDb = p->pTargetFd->pReal;
    u32 volatile *ptr;
    p->rc = pDb->pMethods->xShmMap(pDb, 0, 32*1024, 0, (void volatile**)&ptr);
    if( p->rc==SQLITE_OK ){
      iRet = ((i64)ptr[10] << 32) + ptr[11];
    }
  }
  return iRet;
}

static void otaSetupCheckpoint(sqlite3ota *p, OtaState *pState){

  if( pState==0 ){
    p->eStage = 0;
    if( p->rc==SQLITE_OK ){
      p->rc = sqlite3_exec(p->db, "SELECT * FROM sqlite_master", 0, 0, 0);
    }
  }

  if( p->rc==SQLITE_OK ){
    int rc2;
    p->eStage = OTA_STAGE_CAPTURE;
    rc2 = sqlite3_exec(p->db, "PRAGMA main.wal_checkpoint=restart", 0, 0, 0);
    if( rc2!=SQLITE_INTERNAL ) p->rc = rc2;
  }

  if( p->rc==SQLITE_OK ){
    p->eStage = OTA_STAGE_CKPT;
    p->nStep = 0;
    p->aBuf = otaMalloc(p, p->pgsz);
    p->iWalCksum = otaShmChecksum(p);
  }

  if( p->rc==SQLITE_OK && pState && pState->iWalCksum!=p->iWalCksum ){
    p->rc = SQLITE_DONE;
    p->eStage = OTA_STAGE_DONE;
  }
}

static int otaCaptureWalRead(sqlite3ota *pOta, i64 iOff, int iAmt){
  const u32 mReq = (1<<WAL_LOCK_WRITE)|(1<<WAL_LOCK_CKPT)|(1<<WAL_LOCK_READ0);
  u32 iFrame;

  if( pOta->mLock!=mReq ){
    pOta->rc = SQLITE_BUSY;
    return SQLITE_INTERNAL;
  }

  pOta->pgsz = iAmt;
  if( pOta->nFrame==pOta->nFrameAlloc ){
    int nNew = (pOta->nFrameAlloc ? pOta->nFrameAlloc : 64) * 2;
    OtaFrame *aNew;
    aNew = (OtaFrame*)sqlite3_realloc(pOta->aFrame, nNew * sizeof(OtaFrame));
    if( aNew==0 ) return SQLITE_NOMEM;
    pOta->aFrame = aNew;
    pOta->nFrameAlloc = nNew;
  }

  iFrame = (u32)((iOff-32) / (i64)(iAmt+24)) + 1;
  if( pOta->iMaxFrame<iFrame ) pOta->iMaxFrame = iFrame;
  pOta->aFrame[pOta->nFrame].iWalFrame = iFrame;
  pOta->aFrame[pOta->nFrame].iDbPage = 0;
  pOta->nFrame++;
  return SQLITE_OK;
}

static int otaCaptureDbWrite(sqlite3ota *pOta, i64 iOff){
  pOta->aFrame[pOta->nFrame-1].iDbPage = (u32)(iOff / pOta->pgsz) + 1;
  return SQLITE_OK;
}

static void otaCheckpointFrame(sqlite3ota *p, OtaFrame *pFrame){
  sqlite3_file *pWal = p->pTargetFd->pWalFd->pReal;
  sqlite3_file *pDb = p->pTargetFd->pReal;
  i64 iOff;

  assert( p->rc==SQLITE_OK );
  iOff = (i64)(pFrame->iWalFrame-1) * (p->pgsz + 24) + 32 + 24;
  p->rc = pWal->pMethods->xRead(pWal, p->aBuf, p->pgsz, iOff);
  if( p->rc ) return;

  iOff = (i64)(pFrame->iDbPage-1) * p->pgsz;
  p->rc = pDb->pMethods->xWrite(pDb, p->aBuf, p->pgsz, iOff);
}


/*
** Take an EXCLUSIVE lock on the database file.
*/
static void otaLockDatabase(sqlite3ota *p){
  sqlite3_file *pReal = p->pTargetFd->pReal;
  assert( p->rc==SQLITE_OK );
  p->rc = pReal->pMethods->xLock(pReal, SQLITE_LOCK_SHARED);
  if( p->rc==SQLITE_OK ){
    p->rc = pReal->pMethods->xLock(pReal, SQLITE_LOCK_EXCLUSIVE);
  }
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

  assert( p->eStage==OTA_STAGE_MOVE );
  assert( p->rc==SQLITE_OK && p->zErrmsg==0 );
  if( zWal==0 || zOal==0 ){
    p->rc = SQLITE_NOMEM;
  }else{
    /* Move the *-oal file to *-wal. At this point connection p->db is
    ** holding a SHARED lock on the target database file (because it is
    ** in WAL mode). So no other connection may be writing the db. 
    **
    ** In order to ensure that there are no database readers, an EXCLUSIVE
    ** lock is obtained here before the *-oal is moved to *-wal.
    */
    otaLockDatabase(p);
    if( p->rc==SQLITE_OK ){
      otaFileSuffix3(zBase, zWal);
      otaFileSuffix3(zBase, zOal);
      rename(zOal, zWal);

      /* Re-open the databases. */
      otaObjIterFinalize(&p->objiter);
      sqlite3_close(p->db);
      p->db = 0;
      otaOpenDatabase(p);
      otaSetupCheckpoint(p, 0);
    }
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

    case SQLITE_TEXT: {
      const unsigned char *z = sqlite3_column_text(p->objiter.pSelect, iCol);
      if( z==0 ){
        p->rc = SQLITE_NOMEM;
      }else{
        *pzMask = (const char*)z;
      }
      res = OTA_UPDATE;

      break;
    }

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

        if( eType==OTA_DELETE && pIter->abTblPk[i]==0 ){
          continue;
        }

        pVal = sqlite3_column_value(pIter->pSelect, i);
        p->rc = sqlite3_bind_value(pWriter, i+1, pVal);
        if( p->rc ) goto step_out;
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
        p->rc = sqlite3_bind_value(pWriter, pIter->nCol+1, pVal);
      }
      if( p->rc==SQLITE_OK ){
        sqlite3_step(pWriter);
        p->rc = resetAndCollectError(pWriter, &p->zErrmsg);
      }
    }else{
      sqlite3_value *pVal;
      sqlite3_stmt *pUpdate = 0;
      assert( eType==OTA_UPDATE );
      otaGetUpdateStmt(p, pIter, zMask, &pUpdate);
      if( pUpdate ){
        for(i=0; p->rc==SQLITE_OK && i<pIter->nCol; i++){
          char c = zMask[pIter->aiSrcOrder[i]];
          pVal = sqlite3_column_value(pIter->pSelect, i);
          if( pIter->abTblPk[i] || c=='x' || c=='d' ){
            p->rc = sqlite3_bind_value(pUpdate, i+1, pVal);
          }
        }
        if( p->rc==SQLITE_OK 
         && (pIter->eType==OTA_PK_VTAB || pIter->eType==OTA_PK_NONE) 
        ){
          /* Bind the ota_rowid value to column _rowid_ */
          assertColumnName(pIter->pSelect, pIter->nCol+1, "ota_rowid");
          pVal = sqlite3_column_value(pIter->pSelect, pIter->nCol+1);
          p->rc = sqlite3_bind_value(pUpdate, pIter->nCol+1, pVal);
        }
        if( p->rc==SQLITE_OK ){
          sqlite3_step(pUpdate);
          p->rc = resetAndCollectError(pUpdate, &p->zErrmsg);
        }
      }
    }
  }

 step_out:
  return p->rc;
}

/*
** Increment the schema cookie of the main database opened by p->db.
*/
static void otaIncrSchemaCookie(sqlite3ota *p){
  if( p->rc==SQLITE_OK ){
    int iCookie = 1000000;
    sqlite3_stmt *pStmt;

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
}

static void otaSaveState(sqlite3ota *p, int eStage){
  if( p->rc==SQLITE_OK || p->rc==SQLITE_DONE ){
    sqlite3_stmt *pInsert = 0;
    int rc;

    assert( p->zErrmsg==0 );
    rc = prepareFreeAndCollectError(p->db, &pInsert, &p->zErrmsg, 
        sqlite3_mprintf(
          "INSERT OR REPLACE INTO ota.ota_state(k, v) VALUES "
          "(%d, %d), "
          "(%d, %Q), "
          "(%d, %Q), "
          "(%d, %d), "
          "(%d, %lld), "
          "(%d, %lld), "
          "(%d, %lld) ",
          OTA_STATE_STAGE, eStage,
          OTA_STATE_TBL, p->objiter.zTbl, 
          OTA_STATE_IDX, p->objiter.zIdx, 
          OTA_STATE_ROW, p->nStep, 
          OTA_STATE_PROGRESS, p->nProgress,
          OTA_STATE_CKPT, p->iWalCksum,
          OTA_STATE_COOKIE, (i64)p->pTargetFd->iCookie
          )
        );
    assert( pInsert==0 || rc==SQLITE_OK );

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
}


/*
** Step the OTA object.
*/
int sqlite3ota_step(sqlite3ota *p){
  if( p ){
    switch( p->eStage ){
      case OTA_STAGE_OAL: {
        OtaObjIter *pIter = &p->objiter;
        while( p->rc==SQLITE_OK && pIter->zTbl ){

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

        if( p->rc==SQLITE_OK ){
          assert( pIter->zTbl==0 );
          otaSaveState(p, OTA_STAGE_MOVE);
          otaIncrSchemaCookie(p);
          if( p->rc==SQLITE_OK ){
            p->rc = sqlite3_exec(p->db, "COMMIT", 0, 0, &p->zErrmsg);
          }
          p->eStage = OTA_STAGE_MOVE;
        }
        break;
      }

      case OTA_STAGE_MOVE: {
        if( p->rc==SQLITE_OK ){
          otaMoveOalFile(p);
          p->nProgress++;
        }
        break;
      }

      case OTA_STAGE_CKPT: {
        if( p->rc==SQLITE_OK ){
          if( p->nStep>=p->nFrame ){
            sqlite3_file *pDb = p->pTargetFd->pReal;
  
            /* Sync the db file */
            p->rc = pDb->pMethods->xSync(pDb, SQLITE_SYNC_NORMAL);
  
            /* Update nBackfill */
            if( p->rc==SQLITE_OK ){
              void volatile *ptr;
              p->rc = pDb->pMethods->xShmMap(pDb, 0, 32*1024, 0, &ptr);
              if( p->rc==SQLITE_OK ){
                ((u32*)ptr)[12] = p->iMaxFrame;
              }
            }
  
            if( p->rc==SQLITE_OK ){
              p->eStage = OTA_STAGE_DONE;
              p->rc = SQLITE_DONE;
            }
          }else{
            OtaFrame *pFrame = &p->aFrame[p->nStep];
            otaCheckpointFrame(p, pFrame);
            p->nStep++;
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

static void otaFreeState(OtaState *p){
  if( p ){
    sqlite3_free(p->zTbl);
    sqlite3_free(p->zIdx);
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
  sqlite3_stmt *pStmt = 0;
  int rc;
  int rc2;

  pRet = (OtaState*)otaMalloc(p, sizeof(OtaState));
  if( pRet==0 ) return 0;

  rc = prepareAndCollectError(p->db, &pStmt, &p->zErrmsg, zSelect);
  while( rc==SQLITE_OK && SQLITE_ROW==sqlite3_step(pStmt) ){
    switch( sqlite3_column_int(pStmt, 0) ){
      case OTA_STATE_STAGE:
        pRet->eStage = sqlite3_column_int(pStmt, 1);
        if( pRet->eStage!=OTA_STAGE_OAL
         && pRet->eStage!=OTA_STAGE_MOVE
         && pRet->eStage!=OTA_STAGE_CKPT
        ){
          p->rc = SQLITE_CORRUPT;
        }
        break;

      case OTA_STATE_TBL:
        pRet->zTbl = otaStrndup((char*)sqlite3_column_text(pStmt, 1), &rc);
        break;

      case OTA_STATE_IDX:
        pRet->zIdx = otaStrndup((char*)sqlite3_column_text(pStmt, 1), &rc);
        break;

      case OTA_STATE_ROW:
        pRet->nRow = sqlite3_column_int(pStmt, 1);
        break;

      case OTA_STATE_PROGRESS:
        pRet->nProgress = sqlite3_column_int64(pStmt, 1);
        break;

      case OTA_STATE_CKPT:
        pRet->iWalCksum = sqlite3_column_int64(pStmt, 1);
        break;

      case OTA_STATE_COOKIE:
        pRet->iCookie = (u32)sqlite3_column_int64(pStmt, 1);
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
       || otaStrCompare(pIter->zIdx, pState->zIdx)
       || otaStrCompare(pIter->zTbl, pState->zTbl) 
    )){
      rc = otaObjIterNext(p, pIter);
    }

    if( rc==SQLITE_OK && !pIter->zTbl ){
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

static void otaCreateVfs(sqlite3ota *p){
  int rnd;
  char zRnd[64];

  assert( p->rc==SQLITE_OK );
  sqlite3_randomness(sizeof(int), (void*)&rnd);
  sprintf(zRnd, "ota_vfs_%d", rnd);
  p->rc = sqlite3ota_create_vfs(zRnd, 0);
  if( p->rc==SQLITE_OK ){
    sqlite3_vfs *pVfs = sqlite3_vfs_find(zRnd);
    assert( pVfs );
    p->zVfsName = pVfs->zName;
  }
}

static void otaDeleteVfs(sqlite3ota *p){
  if( p->zVfsName ){
    sqlite3ota_destroy_vfs(p->zVfsName);
    p->zVfsName = 0;
  }
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

    /* Create the custom VFS. */
    memset(p, 0, sizeof(sqlite3ota));
    otaCreateVfs(p);

    /* Open the target database */
    if( p->rc==SQLITE_OK ){
      p->zTarget = (char*)&p[1];
      memcpy(p->zTarget, zTarget, nTarget+1);
      p->zOta = &p->zTarget[nTarget+1];
      memcpy(p->zOta, zOta, nOta+1);
      otaOpenDatabase(p);
    }

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
          p->eStage = OTA_STAGE_OAL;
        }else{
          p->eStage = pState->eStage;
        }
        p->nProgress = pState->nProgress;
      }
    }
    assert( p->rc!=SQLITE_OK || p->eStage!=0 );

    if( p->rc==SQLITE_OK 
     && (p->eStage==OTA_STAGE_OAL || p->eStage==OTA_STAGE_MOVE)
    ){   
      /* Check that this is not a wal mode database. If it is, it cannot 
      ** be updated.  */
      if( p->pTargetFd->pWalFd ){
        p->rc = SQLITE_ERROR;
        p->zErrmsg = sqlite3_mprintf("cannot update wal mode database");
      }

      /* At this point (pTargetFd->iCookie) contains the value of the
      ** change-counter cookie (the thing that gets incremented when a 
      ** transaction is committed in rollback mode) currently stored on 
      ** page 1 of the database file. */
      else if( pState->eStage!=0 && p->pTargetFd->iCookie!=pState->iCookie ){
        p->rc = SQLITE_BUSY;
        p->zErrmsg = sqlite3_mprintf("database modified during ota update");
      }
    }

    if( p->rc==SQLITE_OK ){
      if( p->eStage==OTA_STAGE_OAL ){

        /* Open the transaction */
        p->rc = sqlite3_exec(p->db, "BEGIN IMMEDIATE", 0, 0, &p->zErrmsg);
  
        /* Point the object iterator at the first object */
        if( p->rc==SQLITE_OK ){
          p->rc = otaObjIterFirst(p, &p->objiter);
        }
  
        if( p->rc==SQLITE_OK ){
          otaLoadTransactionState(p, pState);
        }
      }else if( p->eStage==OTA_STAGE_MOVE ){
        /* no-op */
      }else if( p->eStage==OTA_STAGE_CKPT ){
        otaSetupCheckpoint(p, pState);
        p->nStep = pState->nRow;
      }else if( p->eStage==OTA_STAGE_DONE ){
        p->rc = SQLITE_DONE;
      }else{
        p->rc = SQLITE_CORRUPT;
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
** If the error code currently stored in the OTA handle is SQLITE_CONSTRAINT,
** then edit any error message string so as to remove all occurrences of
** the pattern "ota_imp_[0-9]*".
*/
static void otaEditErrmsg(sqlite3ota *p){
  if( p->rc==SQLITE_CONSTRAINT && p->zErrmsg ){
    int i;
    int nErrmsg = strlen(p->zErrmsg);
    for(i=0; i<(nErrmsg-8); i++){
      if( memcmp(&p->zErrmsg[i], "ota_imp_", 8)==0 ){
        int nDel = 8;
        while( p->zErrmsg[i+nDel]>='0' && p->zErrmsg[i+nDel]<='9' ) nDel++;
        memmove(&p->zErrmsg[i], &p->zErrmsg[i+nDel], nErrmsg + 1 - i - nDel);
        nErrmsg -= nDel;
      }
    }
  }
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
    otaSaveState(p, p->eStage);

    /* Close any open statement handles. */
    otaObjIterFinalize(&p->objiter);

    /* Commit the transaction to the *-oal file. */
    if( p->rc==SQLITE_OK && p->eStage==OTA_STAGE_OAL ){
      p->rc = sqlite3_exec(p->db, "COMMIT", 0, 0, &p->zErrmsg);
    }

    /* Close the open database handle and VFS object. */
    sqlite3_close(p->db);
    otaDeleteVfs(p);
    sqlite3_free(p->aBuf);
    sqlite3_free(p->aFrame);

    otaEditErrmsg(p);
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

/**************************************************************************
** Beginning of OTA VFS shim methods. The VFS shim modifies the behaviour
** of a standard VFS in the following ways:
**
**   1. Whenever the first page of a main database file is read or 
**      written, the value of the change-counter cookie is stored in
**      ota_file.iCookie. Similarly, the value of the "write-version"
**      database header field is stored in ota_file.iWriteVer. This ensures
**      that the values are always trustworthy within an open transaction.
**
**   2. When the ota handle is in OTA_STAGE_OAL or OTA_STAGE_CKPT state, all
**      EXCLUSIVE lock attempts on the target database fail. This prevents
**      sqlite3_close() from running an automatic checkpoint. Until the
**      ota handle reaches OTA_STAGE_DONE - at that point the automatic
**      checkpoint may be required to delete the *-wal file.
**
**   3. In OTA_STAGE_OAL, the *-shm file is stored in memory. All xShmLock()
**      calls are noops. This is just an optimization.
**
**   4. In OTA_STAGE_OAL mode, when SQLite calls xAccess() to check if a
**      *-wal file associated with the target database exists, the following
**      special handling applies:
**
**        a) if the *-wal file does exist, return SQLITE_CANTOPEN. An OTA
**           target database may not be in wal mode already.
**
**        b) if the *-wal file does not exist, set the output parameter to
**           non-zero (to tell SQLite that it does exist) anyway.
**
**   5. In OTA_STAGE_OAL mode, if SQLite tries to open a *-wal file 
**      associated with a target database, open the corresponding *-oal file
**      instead.
*/

/*
** Close an ota file.
*/
static int otaVfsClose(sqlite3_file *pFile){
  ota_file *p = (ota_file*)pFile;
  int rc;
  int i;

  /* Free the contents of the apShm[] array. And the array itself. */
  for(i=0; i<p->nShm; i++){
    sqlite3_free(p->apShm[i]);
  }
  sqlite3_free(p->apShm);
  p->apShm = 0;
  sqlite3_free(p->zDel);

  if( p->openFlags & SQLITE_OPEN_MAIN_DB ){
    ota_file **pp;
    sqlite3_mutex_enter(p->pOtaVfs->mutex);
    for(pp=&p->pOtaVfs->pMain; *pp!=p; pp=&((*pp)->pMainNext));
    *pp = p->pMainNext;
    sqlite3_mutex_leave(p->pOtaVfs->mutex);
    p->pReal->pMethods->xShmUnmap(p->pReal, 0);
  }

  /* Close the underlying file handle */
  rc = p->pReal->pMethods->xClose(p->pReal);
  return rc;
}


/*
** Read and return an unsigned 32-bit big-endian integer from the buffer 
** passed as the only argument.
*/
static u32 otaGetU32(u8 *aBuf){
  return ((u32)aBuf[0] << 24)
       + ((u32)aBuf[1] << 16)
       + ((u32)aBuf[2] <<  8)
       + ((u32)aBuf[3]);
}

/*
** Read data from an otaVfs-file.
*/
static int otaVfsRead(
  sqlite3_file *pFile, 
  void *zBuf, 
  int iAmt, 
  sqlite_int64 iOfst
){
  ota_file *p = (ota_file*)pFile;
  int rc;

  if( p->pOta && p->pOta->eStage==OTA_STAGE_CAPTURE ){
    assert( p->openFlags & SQLITE_OPEN_WAL );
    rc = otaCaptureWalRead(p->pOta, iOfst, iAmt);
  }else{
    rc = p->pReal->pMethods->xRead(p->pReal, zBuf, iAmt, iOfst);
    if( rc==SQLITE_OK && iOfst==0 && (p->openFlags & SQLITE_OPEN_MAIN_DB) ){
      /* These look like magic numbers. But they are stable, as they are part
       ** of the definition of the SQLite file format, which may not change. */
      u8 *pBuf = (u8*)zBuf;
      p->iCookie = otaGetU32(&pBuf[24]);
      p->iWriteVer = pBuf[19];
    }
  }
  return rc;
}

/*
** Write data to an otaVfs-file.
*/
static int otaVfsWrite(
  sqlite3_file *pFile, 
  const void *zBuf, 
  int iAmt, 
  sqlite_int64 iOfst
){
  ota_file *p = (ota_file*)pFile;
  int rc;
  if( p->pOta && p->pOta->eStage==OTA_STAGE_CAPTURE ){
    assert( p->openFlags & SQLITE_OPEN_MAIN_DB );
    rc = otaCaptureDbWrite(p->pOta, iOfst);
  }else{
    rc = p->pReal->pMethods->xWrite(p->pReal, zBuf, iAmt, iOfst);
    if( rc==SQLITE_OK && iOfst==0 && (p->openFlags & SQLITE_OPEN_MAIN_DB) ){
      /* These look like magic numbers. But they are stable, as they are part
      ** of the definition of the SQLite file format, which may not change. */
      u8 *pBuf = (u8*)zBuf;
      p->iCookie = otaGetU32(&pBuf[24]);
      p->iWriteVer = pBuf[19];
    }
  }
  return rc;
}

/*
** Truncate an otaVfs-file.
*/
static int otaVfsTruncate(sqlite3_file *pFile, sqlite_int64 size){
  ota_file *p = (ota_file*)pFile;
  return p->pReal->pMethods->xTruncate(p->pReal, size);
}

/*
** Sync an otaVfs-file.
*/
static int otaVfsSync(sqlite3_file *pFile, int flags){
  ota_file *p = (ota_file *)pFile;
  if( p->pOta && p->pOta->eStage==OTA_STAGE_CAPTURE ){
    if( p->openFlags & SQLITE_OPEN_MAIN_DB ){
      return SQLITE_INTERNAL;
    }
    return SQLITE_OK;
  }
  return p->pReal->pMethods->xSync(p->pReal, flags);
}

/*
** Return the current file-size of an otaVfs-file.
*/
static int otaVfsFileSize(sqlite3_file *pFile, sqlite_int64 *pSize){
  ota_file *p = (ota_file *)pFile;
  return p->pReal->pMethods->xFileSize(p->pReal, pSize);
}

/*
** Lock an otaVfs-file.
*/
static int otaVfsLock(sqlite3_file *pFile, int eLock){
  ota_file *p = (ota_file*)pFile;
  sqlite3ota *pOta = p->pOta;
  int rc = SQLITE_OK;

  assert( p->openFlags & (SQLITE_OPEN_MAIN_DB|SQLITE_OPEN_TEMP_DB) );
  if( pOta && eLock==SQLITE_LOCK_EXCLUSIVE && pOta->eStage!=OTA_STAGE_DONE ){
    /* Do not allow EXCLUSIVE locks. Preventing SQLite from taking this 
    ** prevents it from checkpointing the database from sqlite3_close(). */
    rc = SQLITE_BUSY;
  }else{
    rc = p->pReal->pMethods->xLock(p->pReal, eLock);
  }

  return rc;
}

/*
** Unlock an otaVfs-file.
*/
static int otaVfsUnlock(sqlite3_file *pFile, int eLock){
  ota_file *p = (ota_file *)pFile;
  return p->pReal->pMethods->xUnlock(p->pReal, eLock);
}

/*
** Check if another file-handle holds a RESERVED lock on an otaVfs-file.
*/
static int otaVfsCheckReservedLock(sqlite3_file *pFile, int *pResOut){
  ota_file *p = (ota_file *)pFile;
  return p->pReal->pMethods->xCheckReservedLock(p->pReal, pResOut);
}

/*
** File control method. For custom operations on an otaVfs-file.
*/
static int otaVfsFileControl(sqlite3_file *pFile, int op, void *pArg){
  ota_file *p = (ota_file *)pFile;
  int (*xControl)(sqlite3_file*,int,void*) = p->pReal->pMethods->xFileControl;

  assert( p->openFlags & (SQLITE_OPEN_MAIN_DB|SQLITE_OPEN_TEMP_DB) );
  if( op==SQLITE_FCNTL_OTA ){
    int rc;
    sqlite3ota *pOta = (sqlite3ota*)pArg;

    /* First try to find another OTA vfs lower down in the vfs stack. If
    ** one is found, this vfs will operate in pass-through mode. The lower
    ** level vfs will do the special OTA handling.  */
    rc = xControl(p->pReal, op, pArg);

    if( rc==SQLITE_NOTFOUND ){
      /* Now search for a zipvfs instance lower down in the VFS stack. If
      ** one is found, this is an error.  */
      void *dummy = 0;
      rc = xControl(p->pReal, SQLITE_FCNTL_ZIPVFS_PAGER, &dummy);
      if( rc==SQLITE_OK ){
        rc = SQLITE_ERROR;
        pOta->zErrmsg = sqlite3_mprintf("ota/zipvfs setup error");
      }else if( rc==SQLITE_NOTFOUND ){
        pOta->pTargetFd = p;
        p->pOta = pOta;
        if( p->pWalFd ) p->pWalFd->pOta = pOta;
        rc = SQLITE_OK;
      }
    }
    return rc;
  }
  return xControl(p->pReal, op, pArg);
}

/*
** Return the sector-size in bytes for an otaVfs-file.
*/
static int otaVfsSectorSize(sqlite3_file *pFile){
  ota_file *p = (ota_file *)pFile;
  return p->pReal->pMethods->xSectorSize(p->pReal);
}

/*
** Return the device characteristic flags supported by an otaVfs-file.
*/
static int otaVfsDeviceCharacteristics(sqlite3_file *pFile){
  ota_file *p = (ota_file *)pFile;
  return p->pReal->pMethods->xDeviceCharacteristics(p->pReal);
}

/*
** Shared-memory methods are all pass-thrus.
*/
static int otaVfsShmLock(sqlite3_file *pFile, int ofst, int n, int flags){
  ota_file *p = (ota_file*)pFile;
  sqlite3ota *pOta = p->pOta;
  int rc = SQLITE_OK;

#ifdef SQLITE_AMALGAMATION
    assert( WAL_CKPT_LOCK==1 );
#endif

  assert( p->openFlags & (SQLITE_OPEN_MAIN_DB|SQLITE_OPEN_TEMP_DB) );
  if( pOta && (pOta->eStage==OTA_STAGE_OAL || pOta->eStage==OTA_STAGE_MOVE) ){
    /* Magic number 1 is the WAL_CKPT_LOCK lock. Preventing SQLite from
    ** taking this lock also prevents any checkpoints from occurring. 
    ** todo: really, it's not clear why this might occur, as 
    ** wal_autocheckpoint ought to be turned off.  */
    if( ofst==WAL_LOCK_CKPT && n==1 ) rc = SQLITE_BUSY;
  }else{
    int bCapture = 0;
    if( n==1 && (flags & SQLITE_SHM_EXCLUSIVE)
     && pOta && pOta->eStage==OTA_STAGE_CAPTURE
     && (ofst==WAL_LOCK_WRITE || ofst==WAL_LOCK_CKPT || ofst==WAL_LOCK_READ0)
    ){
      bCapture = 1;
    }

    if( bCapture==0 || 0==(flags & SQLITE_SHM_UNLOCK) ){
      rc = p->pReal->pMethods->xShmLock(p->pReal, ofst, n, flags);
      if( bCapture && rc==SQLITE_OK ){
        pOta->mLock |= (1 << ofst);
      }
    }
  }

  return rc;
}

static int otaVfsShmMap(
  sqlite3_file *pFile, 
  int iRegion, 
  int szRegion, 
  int isWrite, 
  void volatile **pp
){
  ota_file *p = (ota_file*)pFile;
  int rc = SQLITE_OK;
  int eStage = (p->pOta ? p->pOta->eStage : 0);

  /* If not in OTA_STAGE_OAL, allow this call to pass through. Or, if this
  ** ota is in the OTA_STAGE_OAL state, use heap memory for *-shm space 
  ** instead of a file on disk.  */
  assert( p->openFlags & (SQLITE_OPEN_MAIN_DB|SQLITE_OPEN_TEMP_DB) );
  if( eStage==OTA_STAGE_OAL || eStage==OTA_STAGE_MOVE ){
    if( iRegion<=p->nShm ){
      int nByte = (iRegion+1) * sizeof(char*);
      char **apNew = (char**)sqlite3_realloc(p->apShm, nByte);
      if( apNew==0 ){
        rc = SQLITE_NOMEM;
      }else{
        memset(&apNew[p->nShm], 0, sizeof(char*) * (1 + iRegion - p->nShm));
        p->apShm = apNew;
        p->nShm = iRegion+1;
      }
    }

    if( rc==SQLITE_OK && p->apShm[iRegion]==0 ){
      char *pNew = (char*)sqlite3_malloc(szRegion);
      if( pNew==0 ){
        rc = SQLITE_NOMEM;
      }else{
        memset(pNew, 0, szRegion);
        p->apShm[iRegion] = pNew;
      }
    }

    if( rc==SQLITE_OK ){
      *pp = p->apShm[iRegion];
    }else{
      *pp = 0;
    }
  }else{
    assert( p->apShm==0 );
    rc = p->pReal->pMethods->xShmMap(p->pReal, iRegion, szRegion, isWrite, pp);
  }

  return rc;
}

/*
** Memory barrier.
*/
static void otaVfsShmBarrier(sqlite3_file *pFile){
  ota_file *p = (ota_file *)pFile;
  p->pReal->pMethods->xShmBarrier(p->pReal);
}

static int otaVfsShmUnmap(sqlite3_file *pFile, int delFlag){
  ota_file *p = (ota_file*)pFile;
  int rc = SQLITE_OK;
  int eStage = (p->pOta ? p->pOta->eStage : 0);

  assert( p->openFlags & (SQLITE_OPEN_MAIN_DB|SQLITE_OPEN_TEMP_DB) );
  if( eStage==OTA_STAGE_OAL || eStage==OTA_STAGE_MOVE ){
    /* no-op */
  }else{
    rc = p->pReal->pMethods->xShmUnmap(p->pReal, delFlag);
  }
  return rc;
}

static ota_file *otaFindMaindb(ota_vfs *pOtaVfs, const char *zWal){
  ota_file *pDb;
  sqlite3_mutex_enter(pOtaVfs->mutex);
  for(pDb=pOtaVfs->pMain; pDb && pDb->zWal!=zWal; pDb=pDb->pMainNext);
  sqlite3_mutex_leave(pOtaVfs->mutex);
  return pDb;
}

/*
** Open an ota file handle.
*/
static int otaVfsOpen(
  sqlite3_vfs *pVfs,
  const char *zName,
  sqlite3_file *pFile,
  int flags,
  int *pOutFlags
){
  static sqlite3_io_methods otavfs_io_methods = {
    2,                            /* iVersion */
    otaVfsClose,                  /* xClose */
    otaVfsRead,                   /* xRead */
    otaVfsWrite,                  /* xWrite */
    otaVfsTruncate,               /* xTruncate */
    otaVfsSync,                   /* xSync */
    otaVfsFileSize,               /* xFileSize */
    otaVfsLock,                   /* xLock */
    otaVfsUnlock,                 /* xUnlock */
    otaVfsCheckReservedLock,      /* xCheckReservedLock */
    otaVfsFileControl,            /* xFileControl */
    otaVfsSectorSize,             /* xSectorSize */
    otaVfsDeviceCharacteristics,  /* xDeviceCharacteristics */
    otaVfsShmMap,                 /* xShmMap */
    otaVfsShmLock,                /* xShmLock */
    otaVfsShmBarrier,             /* xShmBarrier */
    otaVfsShmUnmap                /* xShmUnmap */
  };
  ota_vfs *pOtaVfs = (ota_vfs*)pVfs;
  sqlite3_vfs *pRealVfs = pOtaVfs->pRealVfs;
  ota_file *pFd = (ota_file *)pFile;
  int rc = SQLITE_OK;
  const char *zOpen = zName;

  memset(pFd, 0, sizeof(ota_file));
  pFd->pReal = (sqlite3_file*)&pFd[1];
  pFd->pOtaVfs = pOtaVfs;
  pFd->openFlags = flags;
  if( zName ){
    if( flags & SQLITE_OPEN_MAIN_DB ){
      /* A main database has just been opened. The following block sets
      ** (pFd->zWal) to point to a buffer owned by SQLite that contains
      ** the name of the *-wal file this db connection will use. SQLite
      ** happens to pass a pointer to this buffer when using xAccess()
      ** or xOpen() to operate on the *-wal file.  */
      int n = strlen(zName);
      const char *z = &zName[n];
      if( flags & SQLITE_OPEN_URI ){
        int odd = 0;
        while( 1 ){
          if( z[0]==0 ){
            odd = 1 - odd;
            if( odd && z[1]==0 ) break;
          }
          z++;
        }
        z += 2;
      }else{
        while( *z==0 ) z++;
      }
      z += (n + 8 + 1);
      pFd->zWal = z;
    }
    else if( flags & SQLITE_OPEN_WAL ){
      ota_file *pDb = otaFindMaindb(pOtaVfs, zName);
      if( pDb ){
        if( pDb->pOta && pDb->pOta->eStage==OTA_STAGE_OAL ){
          char *zCopy = otaStrndup(zName, &rc);
          if( zCopy ){
            int nCopy = strlen(zCopy);
            zCopy[nCopy-3] = 'o';
            zOpen = (const char*)(pFd->zDel = zCopy);
          }
          pFd->pOta = pDb->pOta;
        }
        pDb->pWalFd = pFd;
      }
    }
  }

  if( rc==SQLITE_OK ){
    rc = pRealVfs->xOpen(pRealVfs, zOpen, pFd->pReal, flags, pOutFlags);
  }
  if( pFd->pReal->pMethods ){
    /* The xOpen() operation has succeeded. Set the sqlite3_file.pMethods
    ** pointer and, if the file is a main database file, link it into the
    ** mutex protected linked list of all such files.  */
    pFile->pMethods = &otavfs_io_methods;
    if( flags & SQLITE_OPEN_MAIN_DB ){
      sqlite3_mutex_enter(pOtaVfs->mutex);
      pFd->pMainNext = pOtaVfs->pMain;
      pOtaVfs->pMain = pFd;
      sqlite3_mutex_leave(pOtaVfs->mutex);
    }
  }

  return rc;
}

/*
** Delete the file located at zPath.
*/
static int otaVfsDelete(sqlite3_vfs *pVfs, const char *zPath, int dirSync){
  sqlite3_vfs *pRealVfs = ((ota_vfs*)pVfs)->pRealVfs;
  return pRealVfs->xDelete(pRealVfs, zPath, dirSync);
}

/*
** Test for access permissions. Return true if the requested permission
** is available, or false otherwise.
*/
static int otaVfsAccess(
  sqlite3_vfs *pVfs, 
  const char *zPath, 
  int flags, 
  int *pResOut
){
  ota_vfs *pOtaVfs = (ota_vfs*)pVfs;
  sqlite3_vfs *pRealVfs = pOtaVfs->pRealVfs;
  int rc;

  rc = pRealVfs->xAccess(pRealVfs, zPath, flags, pResOut);

  /* If this call is to check if a *-wal file associated with an OTA target
  ** database connection exists, and the OTA update is in OTA_STAGE_OAL,
  ** the following special handling is activated:
  **
  **   a) if the *-wal file does exist, return SQLITE_CANTOPEN. This
  **      ensures that the OTA extension never tries to update a database
  **      in wal mode, even if the first page of the database file has
  **      been damaged. 
  **
  **   b) if the *-wal file does not exist, claim that it does anyway,
  **      causing SQLite to call xOpen() to open it. This call will also
  **      be intercepted (see the otaVfsOpen() function) and the *-oal
  **      file opened instead.
  */
  if( rc==SQLITE_OK && flags==SQLITE_ACCESS_EXISTS ){
    ota_file *pDb = otaFindMaindb(pOtaVfs, zPath);
    if( pDb && pDb->pOta && pDb->pOta->eStage==OTA_STAGE_OAL ){
      if( *pResOut ){
        rc = SQLITE_CANTOPEN;
      }else{
        *pResOut = 1;
      }
    }
  }

  return rc;
}

/*
** Populate buffer zOut with the full canonical pathname corresponding
** to the pathname in zPath. zOut is guaranteed to point to a buffer
** of at least (DEVSYM_MAX_PATHNAME+1) bytes.
*/
static int otaVfsFullPathname(
  sqlite3_vfs *pVfs, 
  const char *zPath, 
  int nOut, 
  char *zOut
){
  sqlite3_vfs *pRealVfs = ((ota_vfs*)pVfs)->pRealVfs;
  return pRealVfs->xFullPathname(pRealVfs, zPath, nOut, zOut);
}

#ifndef SQLITE_OMIT_LOAD_EXTENSION
/*
** Open the dynamic library located at zPath and return a handle.
*/
static void *otaVfsDlOpen(sqlite3_vfs *pVfs, const char *zPath){
  sqlite3_vfs *pRealVfs = ((ota_vfs*)pVfs)->pRealVfs;
  return pRealVfs->xDlOpen(pRealVfs, zPath);
}

/*
** Populate the buffer zErrMsg (size nByte bytes) with a human readable
** utf-8 string describing the most recent error encountered associated 
** with dynamic libraries.
*/
static void otaVfsDlError(sqlite3_vfs *pVfs, int nByte, char *zErrMsg){
  sqlite3_vfs *pRealVfs = ((ota_vfs*)pVfs)->pRealVfs;
  pRealVfs->xDlError(pRealVfs, nByte, zErrMsg);
}

/*
** Return a pointer to the symbol zSymbol in the dynamic library pHandle.
*/
static void (*otaVfsDlSym(
  sqlite3_vfs *pVfs, 
  void *pArg, 
  const char *zSym
))(void){
  sqlite3_vfs *pRealVfs = ((ota_vfs*)pVfs)->pRealVfs;
  return pRealVfs->xDlSym(pRealVfs, pArg, zSym);
}

/*
** Close the dynamic library handle pHandle.
*/
static void otaVfsDlClose(sqlite3_vfs *pVfs, void *pHandle){
  sqlite3_vfs *pRealVfs = ((ota_vfs*)pVfs)->pRealVfs;
  return pRealVfs->xDlClose(pRealVfs, pHandle);
}
#endif /* SQLITE_OMIT_LOAD_EXTENSION */

/*
** Populate the buffer pointed to by zBufOut with nByte bytes of 
** random data.
*/
static int otaVfsRandomness(sqlite3_vfs *pVfs, int nByte, char *zBufOut){
  sqlite3_vfs *pRealVfs = ((ota_vfs*)pVfs)->pRealVfs;
  return pRealVfs->xRandomness(pRealVfs, nByte, zBufOut);
}

/*
** Sleep for nMicro microseconds. Return the number of microseconds 
** actually slept.
*/
static int otaVfsSleep(sqlite3_vfs *pVfs, int nMicro){
  sqlite3_vfs *pRealVfs = ((ota_vfs*)pVfs)->pRealVfs;
  return pRealVfs->xSleep(pRealVfs, nMicro);
}

/*
** Return the current time as a Julian Day number in *pTimeOut.
*/
static int otaVfsCurrentTime(sqlite3_vfs *pVfs, double *pTimeOut){
  sqlite3_vfs *pRealVfs = ((ota_vfs*)pVfs)->pRealVfs;
  return pRealVfs->xCurrentTime(pRealVfs, pTimeOut);
}

static int otaVfsGetLastError(sqlite3_vfs *pVfs, int a, char *b){
  return 0;
}

void sqlite3ota_destroy_vfs(const char *zName){
  sqlite3_vfs *pVfs = sqlite3_vfs_find(zName);
  if( pVfs && pVfs->xOpen==otaVfsOpen ){
    sqlite3_vfs_unregister(pVfs);
    sqlite3_free(pVfs);
  }
}

int sqlite3ota_create_vfs(const char *zName, const char *zParent){

  /* Template for VFS */
  static sqlite3_vfs vfs_template = {
    1,                            /* iVersion */
    0,                            /* szOsFile */
    0,                            /* mxPathname */
    0,                            /* pNext */
    0,                            /* zName */
    0,                            /* pAppData */
    otaVfsOpen,                   /* xOpen */
    otaVfsDelete,                 /* xDelete */
    otaVfsAccess,                 /* xAccess */
    otaVfsFullPathname,           /* xFullPathname */

    otaVfsDlOpen,                 /* xDlOpen */
    otaVfsDlError,                /* xDlError */
    otaVfsDlSym,                  /* xDlSym */
    otaVfsDlClose,                /* xDlClose */

    otaVfsRandomness,             /* xRandomness */
    otaVfsSleep,                  /* xSleep */
    otaVfsCurrentTime,            /* xCurrentTime */
    otaVfsGetLastError,           /* xGetLastError */
    0,                            /* xCurrentTimeInt64 (version 2) */
    0, 0, 0                       /* Unimplemented version 3 methods */
  };

  ota_vfs *pNew = 0;              /* Newly allocated VFS */
  int nName;
  int rc = SQLITE_OK;

  int nByte;
  nName = strlen(zName);
  nByte = sizeof(ota_vfs) + nName + 1;
  pNew = (ota_vfs*)sqlite3_malloc(nByte);
  if( pNew==0 ){
    rc = SQLITE_NOMEM;
  }else{
    sqlite3_vfs *pParent;           /* Parent VFS */
    memset(pNew, 0, nByte);
    pParent = sqlite3_vfs_find(zParent);
    if( pParent==0 ){
      rc = SQLITE_NOTFOUND;
    }else{
      char *zSpace;
      memcpy(&pNew->base, &vfs_template, sizeof(sqlite3_vfs));
      pNew->base.mxPathname = pParent->mxPathname;
      pNew->base.szOsFile = sizeof(ota_file) + pParent->szOsFile;
      pNew->pRealVfs = pParent;

      pNew->base.zName = (const char*)(zSpace = (char*)&pNew[1]);
      memcpy(zSpace, zName, nName);

      /* Register the new VFS (not as the default) */
      rc = sqlite3_vfs_register(&pNew->base, 0);
    }
  }

  if( rc!=SQLITE_OK ){
    sqlite3_free(pNew);
  }
  return rc;
}


/**************************************************************************/

#endif /* !defined(SQLITE_CORE) || defined(SQLITE_ENABLE_OTA) */

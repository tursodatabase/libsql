/*
** 2016-05-07
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


#include <sqlite3.h>
#include <stdlib.h>
#include <stddef.h>
#include "tt3_core.c"


typedef struct Config Config;
struct Config {
  int nIPT;                       /* --inserts-per-transaction */
  int nThread;                    /* --threads */
  int nSecond;                    /* --seconds */
  int bMutex;                     /* --mutex */
  int nAutoCkpt;                  /* --autockpt */
  int bRm;                        /* --rm */

  pthread_cond_t cond;
  pthread_mutex_t mutex;
  int nCondWait;                  /* Number of threads waiting on hCond */
  sqlite3_vfs *pVfs;
};

typedef struct WalHookCtx WalHookCtx;
struct WalHookCtx {
  Config *pConfig;
  Sqlite *pDb;
  Error *pErr;
};

typedef struct VfsWrapperFd VfsWrapperFd;
struct VfsWrapperFd {
  sqlite3_file base;              /* Base class */
  int bWriter;                    /* True if holding shm WRITER lock */
  Config *pConfig;
  sqlite3_file *pFd;              /* Underlying file descriptor */
};

/* Methods of the wrapper VFS */
static int vfsWrapOpen(sqlite3_vfs*, const char*, sqlite3_file*, int, int*);
static int vfsWrapDelete(sqlite3_vfs*, const char*, int);
static int vfsWrapAccess(sqlite3_vfs*, const char*, int, int*);
static int vfsWrapFullPathname(sqlite3_vfs*, const char *, int, char*);
static void *vfsWrapDlOpen(sqlite3_vfs*, const char*);
static void vfsWrapDlError(sqlite3_vfs*, int, char*);
static void (*vfsWrapDlSym(sqlite3_vfs*,void*, const char*))(void);
static void vfsWrapDlClose(sqlite3_vfs*, void*);
static int vfsWrapRandomness(sqlite3_vfs*, int, char*);
static int vfsWrapSleep(sqlite3_vfs*, int);
static int vfsWrapCurrentTime(sqlite3_vfs*, double*);
static int vfsWrapGetLastError(sqlite3_vfs*, int, char*);
static int vfsWrapCurrentTimeInt64(sqlite3_vfs*, sqlite3_int64*);
static int vfsWrapSetSystemCall(sqlite3_vfs*, const char*, sqlite3_syscall_ptr);
static sqlite3_syscall_ptr vfsWrapGetSystemCall(sqlite3_vfs*, const char*);
static const char *vfsWrapNextSystemCall(sqlite3_vfs*, const char*);

/* Methods of wrapper sqlite3_io_methods object (see vfsWrapOpen()) */
static int vfsWrapClose(sqlite3_file*);
static int vfsWrapRead(sqlite3_file*, void*, int iAmt, sqlite3_int64 iOfst);
static int vfsWrapWrite(sqlite3_file*, const void*, int iAmt, sqlite3_int64);
static int vfsWrapTruncate(sqlite3_file*, sqlite3_int64 size);
static int vfsWrapSync(sqlite3_file*, int flags);
static int vfsWrapFileSize(sqlite3_file*, sqlite3_int64 *pSize);
static int vfsWrapLock(sqlite3_file*, int);
static int vfsWrapUnlock(sqlite3_file*, int);
static int vfsWrapCheckReservedLock(sqlite3_file*, int *pResOut);
static int vfsWrapFileControl(sqlite3_file*, int op, void *pArg);
static int vfsWrapSectorSize(sqlite3_file*);
static int vfsWrapDeviceCharacteristics(sqlite3_file*);
static int vfsWrapShmMap(sqlite3_file*, int iPg, int, int, void volatile**);
static int vfsWrapShmLock(sqlite3_file*, int offset, int n, int flags);
static void vfsWrapShmBarrier(sqlite3_file*);
static int vfsWrapShmUnmap(sqlite3_file*, int deleteFlag);
static int vfsWrapFetch(sqlite3_file*, sqlite3_int64 iOfst, int iAmt, void **);
static int vfsWrapUnfetch(sqlite3_file*, sqlite3_int64 iOfst, void *p);

static int vfsWrapOpen(
  sqlite3_vfs *pVfs, 
  const char *zName, 
  sqlite3_file *pFd, 
  int flags, 
  int *fout
){
  static sqlite3_io_methods methods = {
    3,
    vfsWrapClose, vfsWrapRead, vfsWrapWrite,
    vfsWrapTruncate, vfsWrapSync, vfsWrapFileSize,
    vfsWrapLock, vfsWrapUnlock, vfsWrapCheckReservedLock,
    vfsWrapFileControl, vfsWrapSectorSize, vfsWrapDeviceCharacteristics,
    vfsWrapShmMap, vfsWrapShmLock, vfsWrapShmBarrier,
    vfsWrapShmUnmap, vfsWrapFetch, vfsWrapUnfetch
  };

  Config *pConfig = (Config*)pVfs->pAppData;
  VfsWrapperFd *pWrapper = (VfsWrapperFd*)pFd;
  int rc;

  pWrapper->pFd = (sqlite3_file*)&pWrapper[1];
  pWrapper->pConfig = pConfig;
  rc = pConfig->pVfs->xOpen(pConfig->pVfs, zName, pWrapper->pFd, flags, fout);
  if( rc==SQLITE_OK ){
    pWrapper->base.pMethods = &methods;
  }
  return rc;
}

static int vfsWrapDelete(sqlite3_vfs *pVfs, const char *a, int b){
  Config *pConfig = (Config*)pVfs->pAppData;
  return pConfig->pVfs->xDelete(pConfig->pVfs, a, b);
}
static int vfsWrapAccess(sqlite3_vfs *pVfs, const char *a, int b, int *c){
  Config *pConfig = (Config*)pVfs->pAppData;
  return pConfig->pVfs->xAccess(pConfig->pVfs, a, b, c);
}
static int vfsWrapFullPathname(sqlite3_vfs *pVfs, const char *a, int b, char*c){
  Config *pConfig = (Config*)pVfs->pAppData;
  return pConfig->pVfs->xFullPathname(pConfig->pVfs, a, b, c);
}
static void *vfsWrapDlOpen(sqlite3_vfs *pVfs, const char *a){
  Config *pConfig = (Config*)pVfs->pAppData;
  return pConfig->pVfs->xDlOpen(pConfig->pVfs, a);
}
static void vfsWrapDlError(sqlite3_vfs *pVfs, int a, char *b){
  Config *pConfig = (Config*)pVfs->pAppData;
  return pConfig->pVfs->xDlError(pConfig->pVfs, a, b);
}
static void (*vfsWrapDlSym(sqlite3_vfs *pVfs, void *a, const char *b))(void){
  Config *pConfig = (Config*)pVfs->pAppData;
  return pConfig->pVfs->xDlSym(pConfig->pVfs, a, b);
}
static void vfsWrapDlClose(sqlite3_vfs *pVfs, void *a){
  Config *pConfig = (Config*)pVfs->pAppData;
  return pConfig->pVfs->xDlClose(pConfig->pVfs, a);
}
static int vfsWrapRandomness(sqlite3_vfs *pVfs, int a, char *b){
  Config *pConfig = (Config*)pVfs->pAppData;
  return pConfig->pVfs->xRandomness(pConfig->pVfs, a, b);
}
static int vfsWrapSleep(sqlite3_vfs *pVfs, int a){
  Config *pConfig = (Config*)pVfs->pAppData;
  return pConfig->pVfs->xSleep(pConfig->pVfs, a);
}
static int vfsWrapCurrentTime(sqlite3_vfs *pVfs, double *a){
  Config *pConfig = (Config*)pVfs->pAppData;
  return pConfig->pVfs->xCurrentTime(pConfig->pVfs, a);
}
static int vfsWrapGetLastError(sqlite3_vfs *pVfs, int a, char *b){
  Config *pConfig = (Config*)pVfs->pAppData;
  return pConfig->pVfs->xGetLastError(pConfig->pVfs, a, b);
}
static int vfsWrapCurrentTimeInt64(sqlite3_vfs *pVfs, sqlite3_int64 *a){
  Config *pConfig = (Config*)pVfs->pAppData;
  return pConfig->pVfs->xCurrentTimeInt64(pConfig->pVfs, a);
}
static int vfsWrapSetSystemCall(
  sqlite3_vfs *pVfs, 
  const char *a, 
  sqlite3_syscall_ptr b
){
  Config *pConfig = (Config*)pVfs->pAppData;
  return pConfig->pVfs->xSetSystemCall(pConfig->pVfs, a, b);
}
static sqlite3_syscall_ptr vfsWrapGetSystemCall(
  sqlite3_vfs *pVfs, 
  const char *a
){
  Config *pConfig = (Config*)pVfs->pAppData;
  return pConfig->pVfs->xGetSystemCall(pConfig->pVfs, a);
}
static const char *vfsWrapNextSystemCall(sqlite3_vfs *pVfs, const char *a){
  Config *pConfig = (Config*)pVfs->pAppData;
  return pConfig->pVfs->xNextSystemCall(pConfig->pVfs, a);
}

static int vfsWrapClose(sqlite3_file *pFd){
  VfsWrapperFd *pWrapper = (VfsWrapperFd*)pFd;
  pWrapper->pFd->pMethods->xClose(pWrapper->pFd);
  pWrapper->pFd = 0;
  return SQLITE_OK;
}
static int vfsWrapRead(sqlite3_file *pFd, void *a, int b, sqlite3_int64 c){
  VfsWrapperFd *pWrapper = (VfsWrapperFd*)pFd;
  return pWrapper->pFd->pMethods->xRead(pWrapper->pFd, a, b, c);
}
static int vfsWrapWrite(
  sqlite3_file *pFd, 
  const void *a, int b, 
  sqlite3_int64 c
){
  VfsWrapperFd *pWrapper = (VfsWrapperFd*)pFd;
  return pWrapper->pFd->pMethods->xWrite(pWrapper->pFd, a, b, c);
}
static int vfsWrapTruncate(sqlite3_file *pFd, sqlite3_int64 a){
  VfsWrapperFd *pWrapper = (VfsWrapperFd*)pFd;
  return pWrapper->pFd->pMethods->xTruncate(pWrapper->pFd, a);
}
static int vfsWrapSync(sqlite3_file *pFd, int a){
  VfsWrapperFd *pWrapper = (VfsWrapperFd*)pFd;
  return pWrapper->pFd->pMethods->xSync(pWrapper->pFd, a);
}
static int vfsWrapFileSize(sqlite3_file *pFd, sqlite3_int64 *a){
  VfsWrapperFd *pWrapper = (VfsWrapperFd*)pFd;
  return pWrapper->pFd->pMethods->xFileSize(pWrapper->pFd, a);
}
static int vfsWrapLock(sqlite3_file *pFd, int a){
  VfsWrapperFd *pWrapper = (VfsWrapperFd*)pFd;
  return pWrapper->pFd->pMethods->xLock(pWrapper->pFd, a);
}
static int vfsWrapUnlock(sqlite3_file *pFd, int a){
  VfsWrapperFd *pWrapper = (VfsWrapperFd*)pFd;
  return pWrapper->pFd->pMethods->xUnlock(pWrapper->pFd, a);
}
static int vfsWrapCheckReservedLock(sqlite3_file *pFd, int *a){
  VfsWrapperFd *pWrapper = (VfsWrapperFd*)pFd;
  return pWrapper->pFd->pMethods->xCheckReservedLock(pWrapper->pFd, a);
}
static int vfsWrapFileControl(sqlite3_file *pFd, int a, void *b){
  VfsWrapperFd *pWrapper = (VfsWrapperFd*)pFd;
  return pWrapper->pFd->pMethods->xFileControl(pWrapper->pFd, a, b);
}
static int vfsWrapSectorSize(sqlite3_file *pFd){
  VfsWrapperFd *pWrapper = (VfsWrapperFd*)pFd;
  return pWrapper->pFd->pMethods->xSectorSize(pWrapper->pFd);
}
static int vfsWrapDeviceCharacteristics(sqlite3_file *pFd){
  VfsWrapperFd *pWrapper = (VfsWrapperFd*)pFd;
  return pWrapper->pFd->pMethods->xDeviceCharacteristics(pWrapper->pFd);
}
static int vfsWrapShmMap(
  sqlite3_file *pFd, 
  int a, int b, int c, 
  void volatile **d
){
  VfsWrapperFd *pWrapper = (VfsWrapperFd*)pFd;
  return pWrapper->pFd->pMethods->xShmMap(pWrapper->pFd, a, b, c, d);
}
static int vfsWrapShmLock(sqlite3_file *pFd, int offset, int n, int flags){
  VfsWrapperFd *pWrapper = (VfsWrapperFd*)pFd;
  Config *pConfig = pWrapper->pConfig;
  int bMutex = 0;
  int rc;

  if(  (offset==0 && n==1)
    && (flags & SQLITE_SHM_LOCK) && (flags & SQLITE_SHM_EXCLUSIVE)
  ){
    pthread_mutex_lock(&pConfig->mutex);
    pWrapper->bWriter = 1;
    bMutex = 1;
  }

  if( offset==0 && (flags & SQLITE_SHM_UNLOCK) && pWrapper->bWriter ){
    pthread_mutex_unlock(&pConfig->mutex);
    pWrapper->bWriter = 0;
  }

  rc = pWrapper->pFd->pMethods->xShmLock(pWrapper->pFd, offset, n, flags);

  if( rc!=SQLITE_OK && bMutex ){
    pthread_mutex_unlock(&pConfig->mutex);
    pWrapper->bWriter = 0;
  }

  return rc;
}
static void vfsWrapShmBarrier(sqlite3_file *pFd){
  VfsWrapperFd *pWrapper = (VfsWrapperFd*)pFd;
  return pWrapper->pFd->pMethods->xShmBarrier(pWrapper->pFd);
}
static int vfsWrapShmUnmap(sqlite3_file *pFd, int a){
  VfsWrapperFd *pWrapper = (VfsWrapperFd*)pFd;
  return pWrapper->pFd->pMethods->xShmUnmap(pWrapper->pFd, a);
}
static int vfsWrapFetch(sqlite3_file *pFd, sqlite3_int64 a, int b, void **c){
  VfsWrapperFd *pWrapper = (VfsWrapperFd*)pFd;
  return pWrapper->pFd->pMethods->xFetch(pWrapper->pFd, a, b, c);
}
static int vfsWrapUnfetch(sqlite3_file *pFd, sqlite3_int64 a, void *b){
  VfsWrapperFd *pWrapper = (VfsWrapperFd*)pFd;
  return pWrapper->pFd->pMethods->xUnfetch(pWrapper->pFd, a, b);
}

static void create_vfs(Config *pConfig){
  static sqlite3_vfs vfs = {
    3, 0, 0, 0, "wrapper", 0,
    vfsWrapOpen, vfsWrapDelete, vfsWrapAccess,
    vfsWrapFullPathname, vfsWrapDlOpen, vfsWrapDlError,
    vfsWrapDlSym, vfsWrapDlClose, vfsWrapRandomness,
    vfsWrapSleep, vfsWrapCurrentTime, vfsWrapGetLastError,
    vfsWrapCurrentTimeInt64, vfsWrapSetSystemCall, vfsWrapGetSystemCall,
    vfsWrapNextSystemCall
  };
  sqlite3_vfs *pVfs;

  pVfs = sqlite3_vfs_find(0);
  vfs.mxPathname = pVfs->mxPathname;
  vfs.szOsFile = pVfs->szOsFile + sizeof(VfsWrapperFd);
  vfs.pAppData = (void*)pConfig;
  pConfig->pVfs = pVfs;

  sqlite3_vfs_register(&vfs, 1);
}


/*
** Wal hook used by connections in thread_main().
*/
static int thread_wal_hook(
  void *pArg,                     /* Pointer to Config object */
  sqlite3 *db,
  const char *zDb, 
  int nFrame
){
  WalHookCtx *pCtx = (WalHookCtx*)pArg;
  Config *pConfig = pCtx->pConfig;

  if( nFrame>=pConfig->nAutoCkpt ){
    pthread_mutex_lock(&pConfig->mutex);
    if( pConfig->nCondWait>=0 ){
      pConfig->nCondWait++;
      if( pConfig->nCondWait==pConfig->nThread ){
        execsql(pCtx->pErr, pCtx->pDb, "PRAGMA wal_checkpoint");
        pthread_cond_broadcast(&pConfig->cond);
      }else{
        pthread_cond_wait(&pConfig->cond, &pConfig->mutex);
      }
      pConfig->nCondWait--;
    }
    pthread_mutex_unlock(&pConfig->mutex);
  }

  return SQLITE_OK;
}


static char *thread_main(int iTid, void *pArg){
  WalHookCtx ctx;
  Config *pConfig = (Config*)pArg;
  Error err = {0};                /* Error code and message */
  Sqlite db = {0};                /* SQLite database connection */
  int nAttempt = 0;               /* Attempted transactions */
  int nCommit = 0;                /* Successful transactions */
  int j;

  opendb(&err, &db, "xyz.db", 0);
  sqlite3_busy_handler(db.db, 0, 0);
  sql_script_printf(&err, &db, 
      "PRAGMA wal_autocheckpoint = %d;"
      "PRAGMA synchronous = 0;", pConfig->nAutoCkpt
  );

  ctx.pConfig = pConfig;
  ctx.pErr = &err;
  ctx.pDb = &db;
  sqlite3_wal_hook(db.db, thread_wal_hook, (void*)&ctx);

  while( !timetostop(&err) ){
    execsql(&err, &db, "BEGIN CONCURRENT");
    for(j=0; j<pConfig->nIPT; j++){
      execsql(&err, &db, 
          "INSERT INTO t1 VALUES"
          "(randomblob(10), randomblob(20), randomblob(30), randomblob(200))"
      );
    }
    execsql(&err, &db, "COMMIT");
    nAttempt++;
    if( err.rc==SQLITE_OK ){
      nCommit++;
    }else{
      clear_error(&err, SQLITE_BUSY);
      execsql(&err, &db, "ROLLBACK");
    }
  }

  closedb(&err, &db);

  pthread_mutex_lock(&pConfig->mutex);
  pConfig->nCondWait = -1;
  pthread_cond_broadcast(&pConfig->cond);
  pthread_mutex_unlock(&pConfig->mutex);

  return sqlite3_mprintf("%d/%d successful commits", nCommit, nAttempt);
}

int main(int argc, const char **argv){
  Error err = {0};                /* Error code and message */
  Sqlite db = {0};                /* SQLite database connection */
  Threadset threads = {0};        /* Test threads */
  Config conf = {5, 3, 5};
  int i;

  CmdlineArg apArg[] = {
    { "--seconds", CMDLINE_INT,  offsetof(Config, nSecond) },
    { "--inserts", CMDLINE_INT,  offsetof(Config, nIPT) },
    { "--threads", CMDLINE_INT,  offsetof(Config, nThread) },
    { "--mutex",   CMDLINE_BOOL, offsetof(Config, bMutex) },
    { "--rm",      CMDLINE_BOOL, offsetof(Config, bRm) },
    { "--autockpt",CMDLINE_INT,  offsetof(Config, nAutoCkpt) },
    { 0, 0, 0 }
  };

  cmdline_process(apArg, argc, argv, (void*)&conf);
  if( err.rc==SQLITE_OK ){
    char *z = cmdline_construct(apArg, (void*)&conf);
    printf("With: %s\n", z);
    sqlite3_free(z);
  }

  /* Create the special VFS - "wrapper". And the mutex and condition 
  ** variable. */
  create_vfs(&conf);
  pthread_mutex_init(&conf.mutex, 0);
  pthread_cond_init(&conf.cond, 0);

  /* Ensure the schema has been created */
  opendb(&err, &db, "xyz.db", conf.bRm);
  sql_script(&err, &db,
      "PRAGMA journal_mode = wal;"
      "CREATE TABLE IF NOT EXISTS t1(a PRIMARY KEY, b, c, d) WITHOUT ROWID;"
      "CREATE INDEX IF NOT EXISTS t1b ON t1(b);"
      "CREATE INDEX IF NOT EXISTS t1c ON t1(c);"
  );

  setstoptime(&err, conf.nSecond*1000);
  if( conf.nThread==1 ){
    char *z = thread_main(0, (void*)&conf);
    printf("Thread 0 says: %s\n", (z==0 ? "..." : z));
    fflush(stdout);
  }else{
    for(i=0; i<conf.nThread; i++){
      launch_thread(&err, &threads, thread_main, (void*)&conf);
    }
    join_all_threads(&err, &threads);
  }

  if( err.rc==SQLITE_OK ){
    printf("Database is %dK\n", (int)(filesize(&err, "xyz.db") / 1024));
  }
  if( err.rc==SQLITE_OK ){
    printf("Wal file is %dK\n", (int)(filesize(&err, "xyz.db-wal") / 1024));
  }

  closedb(&err, &db);
  print_and_free_err(&err);
  return 0;
}

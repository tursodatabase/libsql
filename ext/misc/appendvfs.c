/*
** 2017-10-20
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
******************************************************************************
**
** This file implements a VFS shim that allows an SQLite database to be
** appended onto the end of some other file, such as an executable.
**
** A special record must appear at the end of the file that identifies the
** file as an appended database and provides an offset to page 1.  For
** best performance page 1 should be located at a disk page boundary, though
** that is not required.
**
** An appended database is considered immutable.  It is read-only and no
** locks are ever taken.
**
** If the file being opened is not an appended database, then this shim is
** a pass-through into the default underlying VFS.
**/
#include <sqlite3ext.h>
SQLITE_EXTENSION_INIT1
#include <string.h>
#include <assert.h>

/* The append mark at the end of the database is:
**
**     Start-Of-SQLite3-NNNNNNNN
**     123456789 123456789 12345
**
** The NNNNNNNN represents a 64-bit big-endian unsigned integer which is
** the offset to page 1.
*/
#define APND_MARK_PREFIX     "Start-Of-SQLite3-"
#define APND_MARK_PREFIX_SZ  17
#define APND_MARK_SIZE       25

/*
** Forward declaration of objects used by this utility
*/
typedef struct sqlite3_vfs ApndVfs;
typedef struct ApndFile ApndFile;

/* Access to a lower-level VFS that (might) implement dynamic loading,
** access to randomness, etc.
*/
#define ORIGVFS(p)  ((sqlite3_vfs*)((p)->pAppData))
#define ORIGFILE(p) ((sqlite3_file*)(((ApndFile*)(p))+1))

/* An open file */
struct ApndFile {
  sqlite3_file base;              /* IO methods */
  sqlite3_int64 iPgOne;           /* File offset to page 1 */
};

/*
** Methods for ApndFile
*/
static int apndClose(sqlite3_file*);
static int apndRead(sqlite3_file*, void*, int iAmt, sqlite3_int64 iOfst);
static int apndWrite(sqlite3_file*,const void*,int iAmt, sqlite3_int64 iOfst);
static int apndTruncate(sqlite3_file*, sqlite3_int64 size);
static int apndSync(sqlite3_file*, int flags);
static int apndFileSize(sqlite3_file*, sqlite3_int64 *pSize);
static int apndLock(sqlite3_file*, int);
static int apndUnlock(sqlite3_file*, int);
static int apndCheckReservedLock(sqlite3_file*, int *pResOut);
static int apndFileControl(sqlite3_file*, int op, void *pArg);
static int apndSectorSize(sqlite3_file*);
static int apndDeviceCharacteristics(sqlite3_file*);
static int apndShmMap(sqlite3_file*, int iPg, int pgsz, int, void volatile**);
static int apndShmLock(sqlite3_file*, int offset, int n, int flags);
static void apndShmBarrier(sqlite3_file*);
static int apndShmUnmap(sqlite3_file*, int deleteFlag);
static int apndFetch(sqlite3_file*, sqlite3_int64 iOfst, int iAmt, void **pp);
static int apndUnfetch(sqlite3_file*, sqlite3_int64 iOfst, void *p);

/*
** Methods for ApndVfs
*/
static int apndOpen(sqlite3_vfs*, const char *, sqlite3_file*, int , int *);
static int apndDelete(sqlite3_vfs*, const char *zName, int syncDir);
static int apndAccess(sqlite3_vfs*, const char *zName, int flags, int *);
static int apndFullPathname(sqlite3_vfs*, const char *zName, int, char *zOut);
static void *apndDlOpen(sqlite3_vfs*, const char *zFilename);
static void apndDlError(sqlite3_vfs*, int nByte, char *zErrMsg);
static void (*apndDlSym(sqlite3_vfs *pVfs, void *p, const char*zSym))(void);
static void apndDlClose(sqlite3_vfs*, void*);
static int apndRandomness(sqlite3_vfs*, int nByte, char *zOut);
static int apndSleep(sqlite3_vfs*, int microseconds);
static int apndCurrentTime(sqlite3_vfs*, double*);
static int apndGetLastError(sqlite3_vfs*, int, char *);
static int apndCurrentTimeInt64(sqlite3_vfs*, sqlite3_int64*);
static int apndSetSystemCall(sqlite3_vfs*, const char*,sqlite3_syscall_ptr);
static sqlite3_syscall_ptr apndGetSystemCall(sqlite3_vfs*, const char *z);
static const char *apndNextSystemCall(sqlite3_vfs*, const char *zName);

static sqlite3_vfs apnd_vfs = {
  3,                            /* iVersion (set when registered) */
  0,                            /* szOsFile (set when registered) */
  1024,                         /* mxPathname */
  0,                            /* pNext */
  "apndvfs",                    /* zName */
  0,                            /* pAppData (set when registered) */ 
  apndOpen,                     /* xOpen */
  apndDelete,                   /* xDelete */
  apndAccess,                   /* xAccess */
  apndFullPathname,             /* xFullPathname */
  apndDlOpen,                   /* xDlOpen */
  apndDlError,                  /* xDlError */
  apndDlSym,                    /* xDlSym */
  apndDlClose,                  /* xDlClose */
  apndRandomness,               /* xRandomness */
  apndSleep,                    /* xSleep */
  apndCurrentTime,              /* xCurrentTime */
  apndGetLastError,             /* xGetLastError */
  apndCurrentTimeInt64,         /* xCurrentTimeInt64 */
  apndSetSystemCall,            /* xSetSystemCall */
  apndGetSystemCall,            /* xGetSystemCall */
  apndNextSystemCall            /* xNextSystemCall */
};

static const sqlite3_io_methods apnd_io_methods = {
  3,                              /* iVersion */
  apndClose,                      /* xClose */
  apndRead,                       /* xRead */
  apndWrite,                      /* xWrite */
  apndTruncate,                   /* xTruncate */
  apndSync,                       /* xSync */
  apndFileSize,                   /* xFileSize */
  apndLock,                       /* xLock */
  apndUnlock,                     /* xUnlock */
  apndCheckReservedLock,          /* xCheckReservedLock */
  apndFileControl,                /* xFileControl */
  apndSectorSize,                 /* xSectorSize */
  apndDeviceCharacteristics,      /* xDeviceCharacteristics */
  apndShmMap,                     /* xShmMap */
  apndShmLock,                    /* xShmLock */
  apndShmBarrier,                 /* xShmBarrier */
  apndShmUnmap,                   /* xShmUnmap */
  apndFetch,                      /* xFetch */
  apndUnfetch                     /* xUnfetch */
};



/*
** Close an apnd-file.
*/
static int apndClose(sqlite3_file *pFile){
  pFile = ORIGFILE(pFile);
  return pFile->pMethods->xClose(pFile);
}

/*
** Read data from an apnd-file.
*/
static int apndRead(
  sqlite3_file *pFile, 
  void *zBuf, 
  int iAmt, 
  sqlite_int64 iOfst
){
  ApndFile *p = (ApndFile *)pFile;
  pFile = ORIGFILE(pFile);
  return pFile->pMethods->xRead(pFile, zBuf, iAmt, iOfst+p->iPgOne);
}

/*
** Write data to an apnd-file.
*/
static int apndWrite(
  sqlite3_file *pFile,
  const void *z,
  int iAmt,
  sqlite_int64 iOfst
){
  return SQLITE_READONLY;
}

/*
** Truncate an apnd-file.
*/
static int apndTruncate(sqlite3_file *pFile, sqlite_int64 size){
  return SQLITE_READONLY;
}

/*
** Sync an apnd-file.
*/
static int apndSync(sqlite3_file *pFile, int flags){
  return SQLITE_READONLY;
}

/*
** Return the current file-size of an apnd-file.
*/
static int apndFileSize(sqlite3_file *pFile, sqlite_int64 *pSize){
  ApndFile *p = (ApndFile*)pFile;
  int rc;
  pFile = ORIGFILE(p);
  rc = pFile->pMethods->xFileSize(pFile, pSize);
  if( rc==SQLITE_OK && p->iPgOne ){
    *pSize -= p->iPgOne + APND_MARK_SIZE;
  }
  return rc;
}

/*
** Lock an apnd-file.
*/
static int apndLock(sqlite3_file *pFile, int eLock){
  return SQLITE_READONLY;
}

/*
** Unlock an apnd-file.
*/
static int apndUnlock(sqlite3_file *pFile, int eLock){
  return SQLITE_OK;
}

/*
** Check if another file-handle holds a RESERVED lock on an apnd-file.
*/
static int apndCheckReservedLock(sqlite3_file *pFile, int *pResOut){
  *pResOut = 0;
  return SQLITE_OK;
}

/*
** File control method. For custom operations on an apnd-file.
*/
static int apndFileControl(sqlite3_file *pFile, int op, void *pArg){
  ApndFile *p = (ApndFile *)pFile;
  int rc;
  pFile = ORIGFILE(pFile);
  rc = pFile->pMethods->xFileControl(pFile, op, pArg);
  if( rc==SQLITE_OK && op==SQLITE_FCNTL_VFSNAME ){
    *(char**)pArg = sqlite3_mprintf("apnd(%lld)/%z", p->iPgOne, *(char**)pArg);
  }
  return rc;
}

/*
** Return the sector-size in bytes for an apnd-file.
*/
static int apndSectorSize(sqlite3_file *pFile){
  pFile = ORIGFILE(pFile);
  return pFile->pMethods->xSectorSize(pFile);
}

/*
** Return the device characteristic flags supported by an apnd-file.
*/
static int apndDeviceCharacteristics(sqlite3_file *pFile){
  pFile = ORIGFILE(pFile);
  return SQLITE_IOCAP_IMMUTABLE |
           pFile->pMethods->xDeviceCharacteristics(pFile);
}

/* Create a shared memory file mapping */
static int apndShmMap(
  sqlite3_file *pFile,
  int iPg,
  int pgsz,
  int bExtend,
  void volatile **pp
){
  return SQLITE_READONLY;
}

/* Perform locking on a shared-memory segment */
static int apndShmLock(sqlite3_file *pFile, int offset, int n, int flags){
  return SQLITE_READONLY;
}

/* Memory barrier operation on shared memory */
static void apndShmBarrier(sqlite3_file *pFile){
  return;
}

/* Unmap a shared memory segment */
static int apndShmUnmap(sqlite3_file *pFile, int deleteFlag){
  return SQLITE_OK;
}

/* Fetch a page of a memory-mapped file */
static int apndFetch(
  sqlite3_file *pFile,
  sqlite3_int64 iOfst,
  int iAmt,
  void **pp
){
  ApndFile *p = (ApndFile *)pFile;
  pFile = ORIGFILE(pFile);
  return pFile->pMethods->xFetch(pFile, iOfst+p->iPgOne, iAmt, pp);
}

/* Release a memory-mapped page */
static int apndUnfetch(sqlite3_file *pFile, sqlite3_int64 iOfst, void *pPage){
  ApndFile *p = (ApndFile *)pFile;
  pFile = ORIGFILE(pFile);
  return pFile->pMethods->xUnfetch(pFile, iOfst+p->iPgOne, pPage);
}

/*
** Open an apnd file handle.
*/
static int apndOpen(
  sqlite3_vfs *pVfs,
  const char *zName,
  sqlite3_file *pFile,
  int flags,
  int *pOutFlags
){
  ApndFile *p;
  int rc;
  sqlite3_int64 sz;
  pVfs = ORIGVFS(pVfs);
  if( (flags & SQLITE_OPEN_MAIN_DB)==0 ){
    return pVfs->xOpen(pVfs, zName, pFile, flags, pOutFlags);
  }
  p = (ApndFile*)pFile;
  memset(p, 0, sizeof(*p));
  p->base.pMethods = &apnd_io_methods;
  pFile = ORIGFILE(pFile);
  flags &= ~(SQLITE_OPEN_CREATE|SQLITE_OPEN_READWRITE);
  flags |= SQLITE_OPEN_READONLY;
  rc = pVfs->xOpen(pVfs, zName, pFile, flags, pOutFlags);
  if( rc ) return rc;
  rc = pFile->pMethods->xFileSize(pFile, &sz);
  if( rc==SQLITE_OK && sz>512 ){
    unsigned char a[APND_MARK_SIZE];
    rc = pFile->pMethods->xRead(pFile, a, APND_MARK_SIZE, sz-APND_MARK_SIZE);
    if( rc==SQLITE_OK
     && memcmp(a, APND_MARK_PREFIX, APND_MARK_PREFIX_SZ)==0
    ){
      p->iPgOne =
         ((sqlite3_uint64)a[APND_MARK_PREFIX_SZ]<<56) +
         ((sqlite3_uint64)a[APND_MARK_PREFIX_SZ+1]<<48) +
         ((sqlite3_uint64)a[APND_MARK_PREFIX_SZ+2]<<40) +
         ((sqlite3_uint64)a[APND_MARK_PREFIX_SZ+3]<<32) +
         ((sqlite3_uint64)a[APND_MARK_PREFIX_SZ+4]<<24) +
         ((sqlite3_uint64)a[APND_MARK_PREFIX_SZ+5]<<16) +
         ((sqlite3_uint64)a[APND_MARK_PREFIX_SZ+6]<<8) +
         ((sqlite3_uint64)a[APND_MARK_PREFIX_SZ+7]);
    }
  }
  return SQLITE_OK;
}

/*
** All other VFS methods are pass-thrus.
*/
static int apndDelete(sqlite3_vfs *pVfs, const char *zPath, int dirSync){
  return ORIGVFS(pVfs)->xDelete(ORIGVFS(pVfs), zPath, dirSync);
}
static int apndAccess(
  sqlite3_vfs *pVfs, 
  const char *zPath, 
  int flags, 
  int *pResOut
){
  return ORIGVFS(pVfs)->xAccess(ORIGVFS(pVfs), zPath, flags, pResOut);
}
static int apndFullPathname(
  sqlite3_vfs *pVfs, 
  const char *zPath, 
  int nOut, 
  char *zOut
){
  return ORIGVFS(pVfs)->xFullPathname(ORIGVFS(pVfs),zPath,nOut,zOut);
}
static void *apndDlOpen(sqlite3_vfs *pVfs, const char *zPath){
  return ORIGVFS(pVfs)->xDlOpen(ORIGVFS(pVfs), zPath);
}
static void apndDlError(sqlite3_vfs *pVfs, int nByte, char *zErrMsg){
  ORIGVFS(pVfs)->xDlError(ORIGVFS(pVfs), nByte, zErrMsg);
}
static void (*apndDlSym(sqlite3_vfs *pVfs, void *p, const char *zSym))(void){
  return ORIGVFS(pVfs)->xDlSym(ORIGVFS(pVfs), p, zSym);
}
static void apndDlClose(sqlite3_vfs *pVfs, void *pHandle){
  ORIGVFS(pVfs)->xDlClose(ORIGVFS(pVfs), pHandle);
}
static int apndRandomness(sqlite3_vfs *pVfs, int nByte, char *zBufOut){
  return ORIGVFS(pVfs)->xRandomness(ORIGVFS(pVfs), nByte, zBufOut);
}
static int apndSleep(sqlite3_vfs *pVfs, int nMicro){
  return ORIGVFS(pVfs)->xSleep(ORIGVFS(pVfs), nMicro);
}
static int apndCurrentTime(sqlite3_vfs *pVfs, double *pTimeOut){
  return ORIGVFS(pVfs)->xCurrentTime(ORIGVFS(pVfs), pTimeOut);
}
static int apndGetLastError(sqlite3_vfs *pVfs, int a, char *b){
  return ORIGVFS(pVfs)->xGetLastError(ORIGVFS(pVfs), a, b);
}
static int apndCurrentTimeInt64(sqlite3_vfs *pVfs, sqlite3_int64 *p){
  return ORIGVFS(pVfs)->xCurrentTimeInt64(ORIGVFS(pVfs), p);
}
static int apndSetSystemCall(
  sqlite3_vfs *pVfs,
  const char *zName,
  sqlite3_syscall_ptr pCall
){
  return ORIGVFS(pVfs)->xSetSystemCall(ORIGVFS(pVfs),zName,pCall);
}
static sqlite3_syscall_ptr apndGetSystemCall(
  sqlite3_vfs *pVfs,
  const char *zName
){
  return ORIGVFS(pVfs)->xGetSystemCall(ORIGVFS(pVfs),zName);
}
static const char *apndNextSystemCall(sqlite3_vfs *pVfs, const char *zName){
  return ORIGVFS(pVfs)->xNextSystemCall(ORIGVFS(pVfs), zName);
}

  
#ifdef _WIN32
__declspec(dllexport)
#endif
/* 
** This routine is called when the extension is loaded.
** Register the new VFS.
*/
int sqlite3_appendvfs_init(
  sqlite3 *db, 
  char **pzErrMsg, 
  const sqlite3_api_routines *pApi
){
  int rc = SQLITE_OK;
  sqlite3_vfs *pOrig;
  SQLITE_EXTENSION_INIT2(pApi);
  pOrig = sqlite3_vfs_find(0);
  apnd_vfs.iVersion = pOrig->iVersion;
  apnd_vfs.pAppData = pOrig;
  apnd_vfs.szOsFile = sizeof(ApndFile);
  rc = sqlite3_vfs_register(&apnd_vfs, 1);
#ifdef APPENDVFS_TEST
  if( rc==SQLITE_OK ){
    rc = sqlite3_auto_extension((void(*)(void))apndvfsRegister);
  }
#endif
  if( rc==SQLITE_OK ) rc = SQLITE_OK_LOAD_PERMANENTLY;
  return rc;
}

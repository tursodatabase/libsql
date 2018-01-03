/*
** 2016-09-07
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
** This is an in-memory VFS implementation.  The application supplies
** a chunk of memory to hold the database file.
**
** USAGE:
**
**    sqlite3_open_v2("whatever", &db, SQLITE_OPEN_READWRITE, "memdb");
**    void *sqlite3_memdb_ptr(db, "main", &sz);
**    int sqlite3_memdb_config(db, "main", pMem, szData, szMem, mFlags);
**
** Flags:
**
**    SQLITE_MEMDB_FREEONCLOSE        Free pMem when closing the connection
**    SQLITE_MEMDB_RESIZEABLE         Use sqlite3_realloc64() to resize pMem
*/
#ifdef SQLITE_ENABLE_MEMDB
#include "sqliteInt.h"

/*
** Forward declaration of objects used by this utility
*/
typedef struct sqlite3_vfs MemVfs;
typedef struct MemFile MemFile;

/* Access to a lower-level VFS that (might) implement dynamic loading,
** access to randomness, etc.
*/
#define ORIGVFS(p) ((sqlite3_vfs*)((p)->pAppData))

/* An open file */
struct MemFile {
  sqlite3_file base;              /* IO methods */
  sqlite3_int64 sz;               /* Size of the file */
  sqlite3_int64 szMax;            /* Space allocated to aData */
  unsigned char *aData;           /* content of the file */
  int nMmap;                      /* Number of memory mapped pages */
  unsigned mFlags;                /* Flags */
  int eLock;                      /* Most recent lock against this file */
};

/*
** Methods for MemFile
*/
static int memdbClose(sqlite3_file*);
static int memdbRead(sqlite3_file*, void*, int iAmt, sqlite3_int64 iOfst);
static int memdbWrite(sqlite3_file*,const void*,int iAmt, sqlite3_int64 iOfst);
static int memdbTruncate(sqlite3_file*, sqlite3_int64 size);
static int memdbSync(sqlite3_file*, int flags);
static int memdbFileSize(sqlite3_file*, sqlite3_int64 *pSize);
static int memdbLock(sqlite3_file*, int);
static int memdbCheckReservedLock(sqlite3_file*, int *pResOut);
static int memdbFileControl(sqlite3_file*, int op, void *pArg);
static int memdbSectorSize(sqlite3_file*);
static int memdbDeviceCharacteristics(sqlite3_file*);
static int memdbShmMap(sqlite3_file*, int iPg, int pgsz, int, void volatile**);
static int memdbShmLock(sqlite3_file*, int offset, int n, int flags);
static void memdbShmBarrier(sqlite3_file*);
static int memdbShmUnmap(sqlite3_file*, int deleteFlag);
static int memdbFetch(sqlite3_file*, sqlite3_int64 iOfst, int iAmt, void **pp);
static int memdbUnfetch(sqlite3_file*, sqlite3_int64 iOfst, void *p);

/*
** Methods for MemVfs
*/
static int memdbOpen(sqlite3_vfs*, const char *, sqlite3_file*, int , int *);
static int memdbDelete(sqlite3_vfs*, const char *zName, int syncDir);
static int memdbAccess(sqlite3_vfs*, const char *zName, int flags, int *);
static int memdbFullPathname(sqlite3_vfs*, const char *zName, int, char *zOut);
static void *memdbDlOpen(sqlite3_vfs*, const char *zFilename);
static void memdbDlError(sqlite3_vfs*, int nByte, char *zErrMsg);
static void (*memdbDlSym(sqlite3_vfs *pVfs, void *p, const char*zSym))(void);
static void memdbDlClose(sqlite3_vfs*, void*);
static int memdbRandomness(sqlite3_vfs*, int nByte, char *zOut);
static int memdbSleep(sqlite3_vfs*, int microseconds);
static int memdbCurrentTime(sqlite3_vfs*, double*);
static int memdbGetLastError(sqlite3_vfs*, int, char *);
static int memdbCurrentTimeInt64(sqlite3_vfs*, sqlite3_int64*);

static sqlite3_vfs memdb_vfs = {
  2,                           /* iVersion */
  0,                           /* szOsFile (set when registered) */
  1024,                        /* mxPathname */
  0,                           /* pNext */
  "memdb",                     /* zName */
  0,                           /* pAppData (set when registered) */ 
  memdbOpen,                   /* xOpen */
  memdbDelete,                 /* xDelete */
  memdbAccess,                 /* xAccess */
  memdbFullPathname,           /* xFullPathname */
  memdbDlOpen,                 /* xDlOpen */
  memdbDlError,                /* xDlError */
  memdbDlSym,                  /* xDlSym */
  memdbDlClose,                /* xDlClose */
  memdbRandomness,             /* xRandomness */
  memdbSleep,                  /* xSleep */
  memdbCurrentTime,            /* xCurrentTime */
  memdbGetLastError,           /* xGetLastError */
  memdbCurrentTimeInt64        /* xCurrentTimeInt64 */
};

static const sqlite3_io_methods memdb_io_methods = {
  3,                              /* iVersion */
  memdbClose,                      /* xClose */
  memdbRead,                       /* xRead */
  memdbWrite,                      /* xWrite */
  memdbTruncate,                   /* xTruncate */
  memdbSync,                       /* xSync */
  memdbFileSize,                   /* xFileSize */
  memdbLock,                       /* xLock */
  memdbLock,                       /* xUnlock - same as xLock in this case */ 
  memdbCheckReservedLock,          /* xCheckReservedLock */
  memdbFileControl,                /* xFileControl */
  memdbSectorSize,                 /* xSectorSize */
  memdbDeviceCharacteristics,      /* xDeviceCharacteristics */
  memdbShmMap,                     /* xShmMap */
  memdbShmLock,                    /* xShmLock */
  memdbShmBarrier,                 /* xShmBarrier */
  memdbShmUnmap,                   /* xShmUnmap */
  memdbFetch,                      /* xFetch */
  memdbUnfetch                     /* xUnfetch */
};



/*
** Close an memdb-file.
**
** The pData pointer is owned by the application, so there is nothing
** to free.
*/
static int memdbClose(sqlite3_file *pFile){
  MemFile *p = (MemFile *)pFile;
  if( p->mFlags & SQLITE_MEMDB_FREEONCLOSE ) sqlite3_free(p->aData);
  return SQLITE_OK;
}

/*
** Read data from an memdb-file.
*/
static int memdbRead(
  sqlite3_file *pFile, 
  void *zBuf, 
  int iAmt, 
  sqlite_int64 iOfst
){
  MemFile *p = (MemFile *)pFile;
  if( iOfst+iAmt>p->sz ){
    memset(zBuf, 0, iAmt);
    return SQLITE_IOERR_SHORT_READ;
  }
  memcpy(zBuf, p->aData+iOfst, iAmt);
  return SQLITE_OK;
}

/*
** Try to enlarge the memory allocation to hold at least sz bytes
*/
static int memdbEnlarge(MemFile *p, sqlite3_int64 newSz){
  unsigned char *pNew;
  if( (p->mFlags & SQLITE_MEMDB_RESIZEABLE)==0 ) return SQLITE_FULL;
  if( p->nMmap>0 ) return SQLITE_FULL;
  pNew = sqlite3_realloc64(p->aData, newSz);
  if( pNew==0 ) return SQLITE_FULL;
  p->aData = pNew;
  p->szMax = newSz;
  return SQLITE_OK;
}

/*
** Write data to an memdb-file.
*/
static int memdbWrite(
  sqlite3_file *pFile,
  const void *z,
  int iAmt,
  sqlite_int64 iOfst
){
  MemFile *p = (MemFile *)pFile;
  if( iOfst+iAmt>p->sz ){
    if( iOfst+iAmt>p->szMax && memdbEnlarge(p, (iOfst+iAmt)*2) ){
      return SQLITE_FULL;
    }
    if( iOfst>p->sz ) memset(p->aData+p->sz, 0, iOfst-p->sz);
    p->sz = iOfst+iAmt;
  }
  memcpy(p->aData+iOfst, z, iAmt);
  return SQLITE_OK;
}

/*
** Truncate an memdb-file.
*/
static int memdbTruncate(sqlite3_file *pFile, sqlite_int64 size){
  MemFile *p = (MemFile *)pFile;
  if( size>p->sz ){
    if( size>p->szMax && memdbEnlarge(p, size) ) return SQLITE_FULL;
    memset(p->aData+p->sz, 0, size-p->sz);
  }
  p->sz = size; 
  return SQLITE_OK;
}

/*
** Sync an memdb-file.
*/
static int memdbSync(sqlite3_file *pFile, int flags){
  return SQLITE_OK;
}

/*
** Return the current file-size of an memdb-file.
*/
static int memdbFileSize(sqlite3_file *pFile, sqlite_int64 *pSize){
  MemFile *p = (MemFile *)pFile;
  *pSize = p->sz;
  return SQLITE_OK;
}

/*
** Lock an memdb-file.
*/
static int memdbLock(sqlite3_file *pFile, int eLock){
  MemFile *p = (MemFile *)pFile;
  p->eLock = eLock;
  return SQLITE_OK;
}

/*
** Check if another file-handle holds a RESERVED lock on an memdb-file.
*/
static int memdbCheckReservedLock(sqlite3_file *pFile, int *pResOut){
  *pResOut = 0;
  return SQLITE_OK;
}

/*
** File control method. For custom operations on an memdb-file.
*/
static int memdbFileControl(sqlite3_file *pFile, int op, void *pArg){
  MemFile *p = (MemFile *)pFile;
  int rc = SQLITE_NOTFOUND;
  if( op==SQLITE_FCNTL_VFSNAME ){
    *(char**)pArg = sqlite3_mprintf("memdb(%p,%lld)", p->aData, p->sz);
    rc = SQLITE_OK;
  }
  return rc;
}

/*
** Return the sector-size in bytes for an memdb-file.
*/
static int memdbSectorSize(sqlite3_file *pFile){
  return 1024;
}

/*
** Return the device characteristic flags supported by an memdb-file.
*/
static int memdbDeviceCharacteristics(sqlite3_file *pFile){
  return SQLITE_IOCAP_ATOMIC | 
         SQLITE_IOCAP_POWERSAFE_OVERWRITE |
         SQLITE_IOCAP_SAFE_APPEND |
         SQLITE_IOCAP_SEQUENTIAL;
}

/* Create a shared memory file mapping */
static int memdbShmMap(
  sqlite3_file *pFile,
  int iPg,
  int pgsz,
  int bExtend,
  void volatile **pp
){
  return SQLITE_IOERR_SHMMAP;
}

/* Perform locking on a shared-memory segment */
static int memdbShmLock(sqlite3_file *pFile, int offset, int n, int flags){
  return SQLITE_IOERR_SHMLOCK;
}

/* Memory barrier operation on shared memory */
static void memdbShmBarrier(sqlite3_file *pFile){
  return;
}

/* Unmap a shared memory segment */
static int memdbShmUnmap(sqlite3_file *pFile, int deleteFlag){
  return SQLITE_OK;
}

/* Fetch a page of a memory-mapped file */
static int memdbFetch(
  sqlite3_file *pFile,
  sqlite3_int64 iOfst,
  int iAmt,
  void **pp
){
  MemFile *p = (MemFile *)pFile;
  p->nMmap++;
  *pp = (void*)(p->aData + iOfst);
  return SQLITE_OK;
}

/* Release a memory-mapped page */
static int memdbUnfetch(sqlite3_file *pFile, sqlite3_int64 iOfst, void *pPage){
  MemFile *p = (MemFile *)pFile;
  p->nMmap--;
  return SQLITE_OK;
}

/*
** Open an mem file handle.
*/
static int memdbOpen(
  sqlite3_vfs *pVfs,
  const char *zName,
  sqlite3_file *pFile,
  int flags,
  int *pOutFlags
){
  MemFile *p = (MemFile*)pFile;
  memset(p, 0, sizeof(*p));
  if( (flags & SQLITE_OPEN_MAIN_DB)==0 ) return SQLITE_CANTOPEN;
  p->mFlags = SQLITE_MEMDB_RESIZEABLE | SQLITE_MEMDB_FREEONCLOSE;
  *pOutFlags = flags | SQLITE_OPEN_MEMORY;
  p->base.pMethods = &memdb_io_methods;
  return SQLITE_OK;
}

/*
** Delete the file located at zPath. If the dirSync argument is true,
** ensure the file-system modifications are synced to disk before
** returning.
*/
static int memdbDelete(sqlite3_vfs *pVfs, const char *zPath, int dirSync){
  return SQLITE_IOERR_DELETE;
}

/*
** Test for access permissions. Return true if the requested permission
** is available, or false otherwise.
*/
static int memdbAccess(
  sqlite3_vfs *pVfs, 
  const char *zPath, 
  int flags, 
  int *pResOut
){
  *pResOut = 0;
  return SQLITE_OK;
}

/*
** Populate buffer zOut with the full canonical pathname corresponding
** to the pathname in zPath. zOut is guaranteed to point to a buffer
** of at least (INST_MAX_PATHNAME+1) bytes.
*/
static int memdbFullPathname(
  sqlite3_vfs *pVfs, 
  const char *zPath, 
  int nOut, 
  char *zOut
){
  sqlite3_snprintf(nOut, zOut, "%s", zPath);
  return SQLITE_OK;
}

/*
** Open the dynamic library located at zPath and return a handle.
*/
static void *memdbDlOpen(sqlite3_vfs *pVfs, const char *zPath){
  return ORIGVFS(pVfs)->xDlOpen(ORIGVFS(pVfs), zPath);
}

/*
** Populate the buffer zErrMsg (size nByte bytes) with a human readable
** utf-8 string describing the most recent error encountered associated 
** with dynamic libraries.
*/
static void memdbDlError(sqlite3_vfs *pVfs, int nByte, char *zErrMsg){
  ORIGVFS(pVfs)->xDlError(ORIGVFS(pVfs), nByte, zErrMsg);
}

/*
** Return a pointer to the symbol zSymbol in the dynamic library pHandle.
*/
static void (*memdbDlSym(sqlite3_vfs *pVfs, void *p, const char *zSym))(void){
  return ORIGVFS(pVfs)->xDlSym(ORIGVFS(pVfs), p, zSym);
}

/*
** Close the dynamic library handle pHandle.
*/
static void memdbDlClose(sqlite3_vfs *pVfs, void *pHandle){
  ORIGVFS(pVfs)->xDlClose(ORIGVFS(pVfs), pHandle);
}

/*
** Populate the buffer pointed to by zBufOut with nByte bytes of 
** random data.
*/
static int memdbRandomness(sqlite3_vfs *pVfs, int nByte, char *zBufOut){
  return ORIGVFS(pVfs)->xRandomness(ORIGVFS(pVfs), nByte, zBufOut);
}

/*
** Sleep for nMicro microseconds. Return the number of microseconds 
** actually slept.
*/
static int memdbSleep(sqlite3_vfs *pVfs, int nMicro){
  return ORIGVFS(pVfs)->xSleep(ORIGVFS(pVfs), nMicro);
}

/*
** Return the current time as a Julian Day number in *pTimeOut.
*/
static int memdbCurrentTime(sqlite3_vfs *pVfs, double *pTimeOut){
  return ORIGVFS(pVfs)->xCurrentTime(ORIGVFS(pVfs), pTimeOut);
}

static int memdbGetLastError(sqlite3_vfs *pVfs, int a, char *b){
  return ORIGVFS(pVfs)->xGetLastError(ORIGVFS(pVfs), a, b);
}
static int memdbCurrentTimeInt64(sqlite3_vfs *pVfs, sqlite3_int64 *p){
  return ORIGVFS(pVfs)->xCurrentTimeInt64(ORIGVFS(pVfs), p);
}

/*
** Translate a database connection pointer and schema name into a
** MemFile pointer.
*/
static MemFile *memdbFromDbSchema(sqlite3 *db, const char *zSchema){
  MemFile *p = 0;
  int rc = sqlite3_file_control(db, zSchema, SQLITE_FCNTL_FILE_POINTER, &p);
  if( rc ) return 0;
  if( p->base.pMethods!=&memdb_io_methods ) return 0;
  return p;
}

/*
** Return a pointer to the memory used to hold the database.
** Return NULL if the arguments do not describe a memdb database.
*/
void *sqlite3_memdb_ptr(sqlite3 *db, const char *zSchema, sqlite3_int64 *pSz){
  MemFile *p = memdbFromDbSchema(db, zSchema);
  if( p==0 ){
    *pSz = 0;
    return 0;
  }
  *pSz = p->sz;
  return p->aData;
}

/*
** Reconfigure a memdb database.
*/
int sqlite3_memdb_config(
  sqlite3 *db,
  const char *zSchema,
  void *aData,
  sqlite3_int64 sz,
  sqlite3_int64 szMax,
  unsigned int mFlags
){
  MemFile *p = memdbFromDbSchema(db, zSchema);
  int rc;
  if( p==0 ){
    rc = SQLITE_ERROR;
  }else if( p->eLock!=SQLITE_LOCK_NONE || p->nMmap>0 ){
    rc = SQLITE_BUSY;
  }else{
    if( p->mFlags & SQLITE_MEMDB_FREEONCLOSE ) sqlite3_free(p->aData);
    p->aData = aData;
    p->sz = sz;
    p->szMax = szMax;
    p->mFlags = mFlags;
    rc = SQLITE_OK;
  }
  if( rc!=SQLITE_OK && (mFlags & SQLITE_MEMDB_FREEONCLOSE)!=0 ){
    sqlite3_free(aData);
  }
  return SQLITE_OK;
}

/* 
** This routine is called when the extension is loaded.
** Register the new VFS.
*/
int sqlite3MemdbInit(void){
  memdb_vfs.pAppData = sqlite3_vfs_find(0);
  memdb_vfs.szOsFile = sizeof(MemFile);
  return sqlite3_vfs_register(&memdb_vfs, 0);
}
#endif /* SQLITE_ENABLE_MEMDB */

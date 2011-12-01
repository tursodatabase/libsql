/*
** 2005 November 29
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
** This file contains OS interface code that is common to all
** architectures.
*/
#define _SQLITE_OS_C_ 1
#include "sqliteInt.h"
#undef _SQLITE_OS_C_

/*
** The default SQLite sqlite3_vfs implementations do not allocate
** memory (actually, os_unix.c allocates a small amount of memory
** from within OsOpen()), but some third-party implementations may.
** So we test the effects of a malloc() failing and the sqlite3OsXXX()
** function returning SQLITE_IOERR_NOMEM using the DO_OS_MALLOC_TEST macro.
**
** The following functions are instrumented for malloc() failure 
** testing:
**
**     sqlite3OsRead()
**     sqlite3OsWrite()
**     sqlite3OsSync()
**     sqlite3OsFileSize()
**     sqlite3OsLock()
**     sqlite3OsCheckReservedLock()
**     sqlite3OsFileControl()
**     sqlite3OsShmMap()
**     sqlite3OsOpen()
**     sqlite3OsDelete()
**     sqlite3OsAccess()
**     sqlite3OsFullPathname()
**
*/
#if defined(SQLITE_TEST)
int sqlite3_memdebug_vfs_oom_test = 1;
  #define DO_OS_MALLOC_TEST(x)                                       \
  if (sqlite3_memdebug_vfs_oom_test && (!x || !sqlite3IsMemJournal(x))) {  \
    void *pTstAlloc = sqlite3Malloc(10);                             \
    if (!pTstAlloc) return SQLITE_IOERR_NOMEM;                       \
    sqlite3_free(pTstAlloc);                                         \
  }
#else
  #define DO_OS_MALLOC_TEST(x)
#endif

/*
** The following routines are convenience wrappers around methods
** of the sqlite3_file object.  This is mostly just syntactic sugar. All
** of this would be completely automatic if SQLite were coded using
** C++ instead of plain old C.
*/
int sqlite3OsClose(sqlite3_file *pId){
  int rc = SQLITE_OK;
  if( pId->pMethods ){
    rc = pId->pMethods->xClose(pId);
    pId->pMethods = 0;
  }
  return rc;
}
int sqlite3OsRead(sqlite3_file *id, void *pBuf, int amt, i64 offset){
  DO_OS_MALLOC_TEST(id);
  return id->pMethods->xRead(id, pBuf, amt, offset);
}
int sqlite3OsWrite(sqlite3_file *id, const void *pBuf, int amt, i64 offset){
  DO_OS_MALLOC_TEST(id);
  return id->pMethods->xWrite(id, pBuf, amt, offset);
}
int sqlite3OsTruncate(sqlite3_file *id, i64 size){
  return id->pMethods->xTruncate(id, size);
}
int sqlite3OsSync(sqlite3_file *id, int flags){
  DO_OS_MALLOC_TEST(id);
  return id->pMethods->xSync(id, flags);
}
int sqlite3OsFileSize(sqlite3_file *id, i64 *pSize){
  DO_OS_MALLOC_TEST(id);
  return id->pMethods->xFileSize(id, pSize);
}
int sqlite3OsLock(sqlite3_file *id, int lockType){
  DO_OS_MALLOC_TEST(id);
  return id->pMethods->xLock(id, lockType);
}
int sqlite3OsUnlock(sqlite3_file *id, int lockType){
  return id->pMethods->xUnlock(id, lockType);
}
int sqlite3OsCheckReservedLock(sqlite3_file *id, int *pResOut){
  DO_OS_MALLOC_TEST(id);
  return id->pMethods->xCheckReservedLock(id, pResOut);
}
int sqlite3OsFileControl(sqlite3_file *id, int op, void *pArg){
  DO_OS_MALLOC_TEST(id);
  return id->pMethods->xFileControl(id, op, pArg);
}
int sqlite3OsSectorSize(sqlite3_file *id){
  int (*xSectorSize)(sqlite3_file*) = id->pMethods->xSectorSize;
  return (xSectorSize ? xSectorSize(id) : SQLITE_DEFAULT_SECTOR_SIZE);
}
int sqlite3OsDeviceCharacteristics(sqlite3_file *id){
  return id->pMethods->xDeviceCharacteristics(id);
}
int sqlite3OsShmLock(sqlite3_file *id, int offset, int n, int flags){
  return id->pMethods->xShmLock(id, offset, n, flags);
}
void sqlite3OsShmBarrier(sqlite3_file *id){
  id->pMethods->xShmBarrier(id);
}
int sqlite3OsShmUnmap(sqlite3_file *id, int deleteFlag){
  return id->pMethods->xShmUnmap(id, deleteFlag);
}
int sqlite3OsShmMap(
  sqlite3_file *id,               /* Database file handle */
  int iPage,
  int pgsz,
  int bExtend,                    /* True to extend file if necessary */
  void volatile **pp              /* OUT: Pointer to mapping */
){
  DO_OS_MALLOC_TEST(id);
  return id->pMethods->xShmMap(id, iPage, pgsz, bExtend, pp);
}

/*
** The next group of routines are convenience wrappers around the
** VFS methods.
*/
int sqlite3OsOpen(
  sqlite3_vfs *pVfs, 
  const char *zPath, 
  sqlite3_file *pFile, 
  int flags, 
  int *pFlagsOut
){
  int rc;
  DO_OS_MALLOC_TEST(0);
  /* 0x87f3f is a mask of SQLITE_OPEN_ flags that are valid to be passed
  ** down into the VFS layer.  Some SQLITE_OPEN_ flags (for example,
  ** SQLITE_OPEN_FULLMUTEX or SQLITE_OPEN_SHAREDCACHE) are blocked before
  ** reaching the VFS. */
  rc = pVfs->xOpen(pVfs, zPath, pFile, flags & 0x87f7f, pFlagsOut);
  assert( rc==SQLITE_OK || pFile->pMethods==0 );
  return rc;
}
int sqlite3OsDelete(sqlite3_vfs *pVfs, const char *zPath, int dirSync){
  DO_OS_MALLOC_TEST(0);
  return pVfs->xDelete(pVfs, zPath, dirSync);
}
int sqlite3OsAccess(
  sqlite3_vfs *pVfs, 
  const char *zPath, 
  int flags, 
  int *pResOut
){
  DO_OS_MALLOC_TEST(0);
  return pVfs->xAccess(pVfs, zPath, flags, pResOut);
}
int sqlite3OsFullPathname(
  sqlite3_vfs *pVfs, 
  const char *zPath, 
  int nPathOut, 
  char *zPathOut
){
  DO_OS_MALLOC_TEST(0);
  zPathOut[0] = 0;
  return pVfs->xFullPathname(pVfs, zPath, nPathOut, zPathOut);
}
#ifndef SQLITE_OMIT_LOAD_EXTENSION
void *sqlite3OsDlOpen(sqlite3_vfs *pVfs, const char *zPath){
  return pVfs->xDlOpen(pVfs, zPath);
}
void sqlite3OsDlError(sqlite3_vfs *pVfs, int nByte, char *zBufOut){
  pVfs->xDlError(pVfs, nByte, zBufOut);
}
void (*sqlite3OsDlSym(sqlite3_vfs *pVfs, void *pHdle, const char *zSym))(void){
  return pVfs->xDlSym(pVfs, pHdle, zSym);
}
void sqlite3OsDlClose(sqlite3_vfs *pVfs, void *pHandle){
  pVfs->xDlClose(pVfs, pHandle);
}
#endif /* SQLITE_OMIT_LOAD_EXTENSION */
int sqlite3OsRandomness(sqlite3_vfs *pVfs, int nByte, char *zBufOut){
  return pVfs->xRandomness(pVfs, nByte, zBufOut);
}
int sqlite3OsSleep(sqlite3_vfs *pVfs, int nMicro){
  return pVfs->xSleep(pVfs, nMicro);
}
int sqlite3OsCurrentTimeInt64(sqlite3_vfs *pVfs, sqlite3_int64 *pTimeOut){
  int rc;
  /* IMPLEMENTATION-OF: R-49045-42493 SQLite will use the xCurrentTimeInt64()
  ** method to get the current date and time if that method is available
  ** (if iVersion is 2 or greater and the function pointer is not NULL) and
  ** will fall back to xCurrentTime() if xCurrentTimeInt64() is
  ** unavailable.
  */
  if( pVfs->iVersion>=2 && pVfs->xCurrentTimeInt64 ){
    rc = pVfs->xCurrentTimeInt64(pVfs, pTimeOut);
  }else{
    double r;
    rc = pVfs->xCurrentTime(pVfs, &r);
    *pTimeOut = (sqlite3_int64)(r*86400000.0);
  }
  return rc;
}

int sqlite3OsOpenMalloc(
  sqlite3_vfs *pVfs, 
  const char *zFile, 
  sqlite3_file **ppFile, 
  int flags,
  int *pOutFlags
){
  int rc = SQLITE_NOMEM;
  sqlite3_file *pFile;
  pFile = (sqlite3_file *)sqlite3MallocZero(pVfs->szOsFile);
  if( pFile ){
    rc = sqlite3OsOpen(pVfs, zFile, pFile, flags, pOutFlags);
    if( rc!=SQLITE_OK ){
      sqlite3_free(pFile);
    }else{
      *ppFile = pFile;
    }
  }
  return rc;
}
int sqlite3OsCloseFree(sqlite3_file *pFile){
  int rc = SQLITE_OK;
  assert( pFile );
  rc = sqlite3OsClose(pFile);
  sqlite3_free(pFile);
  return rc;
}

/*
** This function is a wrapper around the OS specific implementation of
** sqlite3_os_init(). The purpose of the wrapper is to provide the
** ability to simulate a malloc failure, so that the handling of an
** error in sqlite3_os_init() by the upper layers can be tested.
*/
int sqlite3OsInit(void){
  void *p = sqlite3_malloc(10);
  if( p==0 ) return SQLITE_NOMEM;
  sqlite3_free(p);
  return sqlite3_os_init();
}

/*
** The list of all registered VFS implementations.
*/
static sqlite3_vfs * SQLITE_WSD vfsList = 0;
#define vfsList GLOBAL(sqlite3_vfs *, vfsList)

/*
** Locate a VFS by name.  If no name is given, simply return the
** first VFS on the list.
*/
sqlite3_vfs *sqlite3_vfs_find(const char *zVfs){
  sqlite3_vfs *pVfs = 0;
#if SQLITE_THREADSAFE
  sqlite3_mutex *mutex;
#endif
#ifndef SQLITE_OMIT_AUTOINIT
  int rc = sqlite3_initialize();
  if( rc ) return 0;
#endif
#if SQLITE_THREADSAFE
  mutex = sqlite3MutexAlloc(SQLITE_MUTEX_STATIC_MASTER);
#endif
  sqlite3_mutex_enter(mutex);
  for(pVfs = vfsList; pVfs; pVfs=pVfs->pNext){
    if( zVfs==0 ) break;
    if( strcmp(zVfs, pVfs->zName)==0 ) break;
  }
  sqlite3_mutex_leave(mutex);
  return pVfs;
}

/*
** Unlink a VFS from the linked list
*/
static void vfsUnlink(sqlite3_vfs *pVfs){
  assert( sqlite3_mutex_held(sqlite3MutexAlloc(SQLITE_MUTEX_STATIC_MASTER)) );
  if( pVfs==0 ){
    /* No-op */
  }else if( vfsList==pVfs ){
    vfsList = pVfs->pNext;
  }else if( vfsList ){
    sqlite3_vfs *p = vfsList;
    while( p->pNext && p->pNext!=pVfs ){
      p = p->pNext;
    }
    if( p->pNext==pVfs ){
      p->pNext = pVfs->pNext;
    }
  }
}

/*
** Register a VFS with the system.  It is harmless to register the same
** VFS multiple times.  The new VFS becomes the default if makeDflt is
** true.
*/
int sqlite3_vfs_register(sqlite3_vfs *pVfs, int makeDflt){
  MUTEX_LOGIC(sqlite3_mutex *mutex;)
#ifndef SQLITE_OMIT_AUTOINIT
  int rc = sqlite3_initialize();
  if( rc ) return rc;
#endif
  MUTEX_LOGIC( mutex = sqlite3MutexAlloc(SQLITE_MUTEX_STATIC_MASTER); )
  sqlite3_mutex_enter(mutex);
  vfsUnlink(pVfs);
  if( makeDflt || vfsList==0 ){
    pVfs->pNext = vfsList;
    vfsList = pVfs;
  }else{
    pVfs->pNext = vfsList->pNext;
    vfsList->pNext = pVfs;
  }
  assert(vfsList);
  sqlite3_mutex_leave(mutex);
  return SQLITE_OK;
}

/*
** Unregister a VFS so that it is no longer accessible.
*/
int sqlite3_vfs_unregister(sqlite3_vfs *pVfs){
#if SQLITE_THREADSAFE
  sqlite3_mutex *mutex = sqlite3MutexAlloc(SQLITE_MUTEX_STATIC_MASTER);
#endif
  sqlite3_mutex_enter(mutex);
  vfsUnlink(pVfs);
  sqlite3_mutex_leave(mutex);
  return SQLITE_OK;
}

#ifndef SQLITE_OMIT_VFS_STDIO
/*****************************************************************************
** The remainder of this file contains a simplified stdio-like interface
** to the VFS layer.
*/

/*
** An instance of the following object records the state of an
** open file.  This object is opaque to all users - the internal
** structure is only visible to the functions below.
*/
struct sqlite3_FILE {
  char *zFilename;        /* Full pathname of the open file */
  sqlite3_int64 iOfst;    /* Current offset into the file */
  sqlite3_vfs *pVfs;      /* The VFS used for this file */
  u8 alwaysAppend;        /* Always append if true */
  sqlite3_file sFile;     /* Open file.  MUST BE LAST */
};

/*
** This is a helper routine used to translate a URI into a full pathname
** and a pointer to the appropriate VFS.
*/
static int getFilename(const char *zURI, sqlite3_vfs **ppVfs, char **pzName){
  int rc;
  char *zOpen = 0;
  char *zFullname = 0;
  unsigned int flags;
  char *zErrmsg = 0;
  sqlite3_vfs *pVfs = 0;

  rc = sqlite3ParseUri(0, zURI, &flags, &pVfs, &zOpen, &zErrmsg);
  sqlite3_free(zErrmsg);
  if( rc ) goto getFilename_error;
  zFullname = sqlite3_malloc( pVfs->mxPathname+1 );
  if( zFullname==0 ){ rc = SQLITE_NOMEM;  goto getFilename_error; }
  rc = pVfs->xFullPathname(pVfs, zOpen, pVfs->mxPathname, zFullname);
  if( rc ) goto getFilename_error;
  sqlite3_free(zOpen);
  zOpen = 0;
  *pzName = sqlite3_realloc(zFullname, sqlite3Strlen30(zFullname)+1);
  if( *pzName==0 ) goto getFilename_error;
  zFullname = 0;
  *ppVfs = pVfs;
  return SQLITE_OK;

getFilename_error:
  sqlite3_free(zOpen);
  sqlite3_free(zFullname);
  *pzName = 0;
  *ppVfs = 0;
  return rc;
}

/*
** Open a file for stdio-like reading and writing.  The file is identified
** by the URI in the first parameter.  The access mode can be "r", "r+",
** "w", "w+", "a", or "a+" with the usual meanings.
**
** On success, a pointer to a new sqlite3_FILE object is returned.  On
** failure, NULL is returned.  Unfortunately, there is no way to recover
** detailed error information after a failure.
*/
sqlite3_FILE *sqlite3_fopen(const char *zURI, const char *zMode){
  char *zFile = 0;
  sqlite3_vfs *pVfs = 0;
  int rc;
  int openFlags;
  int doTruncate = 0;
  int seekEnd = 0;
  int alwaysAppend = 0;
  int nToAlloc;
  sqlite3_FILE *p;

  if( zMode[0]==0 ) return 0;
  if( zMode[0]=='r' ){
    if( zMode[1]=='+' ){
      openFlags = SQLITE_OPEN_READWRITE;
    }else{
      openFlags = SQLITE_OPEN_READONLY;
    }
  }else if( zMode[0]=='w' ){
    openFlags = SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE;
    doTruncate = 1;
  }else if( zMode[0]=='a' ){
    openFlags = SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE;
    if( zMode[1]=='+' ){
      alwaysAppend = 1;
    }else{
      seekEnd = 1;
    }
  }else{
    return 0;
  }
  rc = getFilename(zURI, &pVfs, &zFile);
  if( rc ) return 0;
  nToAlloc = sizeof(*p) + ROUND8(pVfs->szOsFile);
  p = sqlite3_malloc( nToAlloc );
  if( p==0 ){
    sqlite3_free(zFile);
    return 0;
  }
  memset(p, 0, nToAlloc);
  p->zFilename = zFile;
  rc = pVfs->xOpen(pVfs, zFile, &p->sFile, openFlags, &openFlags);
  if( rc!=SQLITE_OK ){
    sqlite3_free(zFile);
    sqlite3_free(p);
    return 0;
  }
  p->pVfs = pVfs;
  p->alwaysAppend = alwaysAppend;
  if( seekEnd ) sqlite3_fseek(p, 0, SQLITE_SEEK_END);
  if( doTruncate ) sqlite3_ftruncate(p, 0);
  return p;
}

/*
** Close a file perviously opened by sqlite3_fopen().
*/
int sqlite3_fclose(sqlite3_FILE *p){
  p->sFile.pMethods->xClose(&p->sFile);
  sqlite3_free(p);
  return SQLITE_OK;
}

/*
** Read iAmt bytes from the file p into pBuf.  
**
** Return 0 on success or an error code if the full amount could
** not be read.
*/
int sqlite3_fread(
  void *pBuf,            /* Write content read into this buffer */
  sqlite3_int64 iAmt,    /* Number of bytes to read */
  sqlite3_FILE *p        /* Read from this file */
){
  int rc = p->sFile.pMethods->xRead(&p->sFile, pBuf, iAmt, p->iOfst);
  if( rc==SQLITE_OK ){
    p->iOfst += iAmt;
  }
  return rc;
}

/*
** Write iAmt bytes from buffer pBuf into the file p.
**
** Return 0 on success or an error code if anything goes wrong.
*/
int sqlite3_fwrite(
  const void *pBuf,      /* Take content to be written from this buffer */
  sqlite3_int64 iAmt,    /* Number of bytes to write */
  sqlite3_FILE *p        /* Write into this file */
){
  int rc;

  if( p->alwaysAppend ) sqlite3_fseek(p, 0, SQLITE_SEEK_END);
  rc = p->sFile.pMethods->xWrite(&p->sFile, pBuf, iAmt, p->iOfst);
  if( rc==SQLITE_OK ){
    p->iOfst += iAmt;
  }
  return rc;
}

/*
** Truncate an open file to newSize bytes.
*/
int sqlite3_ftruncate(sqlite3_FILE *p, sqlite3_int64 newSize){
  int rc;
  rc = p->sFile.pMethods->xTruncate(&p->sFile, newSize);
  return rc;
}

/*
** Return the current position of the file pointer.
*/
sqlite3_int64 sqlite3_ftell(sqlite3_FILE *p){
  return p->iOfst;
}

/*
** Move the file pointer to a new position in the file.
*/
int sqlite3_fseek(sqlite3_FILE *p, sqlite3_int64 ofst, int whence){
  int rc = SQLITE_OK;
  if( whence==SQLITE_SEEK_SET ){
    p->iOfst = ofst;
  }else if( whence==SQLITE_SEEK_CUR ){
    p->iOfst += ofst;
  }else{
    sqlite3_int64 iCur = 0;
    rc = p->sFile.pMethods->xFileSize(&p->sFile, &iCur);
    if( rc==SQLITE_OK ){
      p->iOfst = iCur + ofst;
    }
  }
  return rc;
}

/*
** Rewind the file pointer to the beginning of the file.
*/
int sqlite3_rewind(sqlite3_FILE *p){
  p->iOfst = 0;
  return SQLITE_OK;
}

/*
** Flush the content of OS cache buffers to disk.  (fsync())
*/
int sqlite3_fflush(sqlite3_FILE *p){
  return p->sFile.pMethods->xSync(&p->sFile, SQLITE_SYNC_NORMAL);
}

/*
** Delete the file identified by the URI in the first parameter
*/
int sqlite3_remove(const char *zURI){
  sqlite3_vfs *pVfs = 0;
  char *zFilename = 0;
  int rc;

  rc = getFilename(zURI, &pVfs, &zFilename);
  if( rc==SQLITE_OK ){
    rc = pVfs->xDelete(pVfs, zFilename, 0);
  }
  sqlite3_free(zFilename);
  return rc;
}

#endif /* SQLITE_OMIT_VFS_STDIO */

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
#include "os.h"
#undef _SQLITE_OS_C_

/*
** The following routines are convenience wrappers around methods
** of the sqlite3_file object.  This is mostly just syntactic sugar. All
** of this would be completely automatic if SQLite were coded using
** C++ instead of plain old C.
*/
int sqlite3OsClose(sqlite3_file *pId){
  if( !pId->pMethods ) return SQLITE_OK;
  return pId->pMethods->xClose(pId);
}
int sqlite3OsRead(sqlite3_file *id, void *pBuf, int amt, i64 offset){
  return id->pMethods->xRead(id, pBuf, amt, offset);
}
int sqlite3OsWrite(sqlite3_file *id, const void *pBuf, int amt, i64 offset){
  return id->pMethods->xWrite(id, pBuf, amt, offset);
}
int sqlite3OsTruncate(sqlite3_file *id, i64 size){
  return id->pMethods->xTruncate(id, size);
}
int sqlite3OsSync(sqlite3_file *id, int flags){
  return id->pMethods->xSync(id, flags);
}
int sqlite3OsFileSize(sqlite3_file *id, i64 *pSize){
  return id->pMethods->xFileSize(id, pSize);
}
int sqlite3OsLock(sqlite3_file *id, int lockType){
  return id->pMethods->xLock(id, lockType);
}
int sqlite3OsUnlock(sqlite3_file *id, int lockType){
  return id->pMethods->xUnlock(id, lockType);
}
int sqlite3OsBreakLock(sqlite3_file *id){
  return id->pMethods->xBreakLock(id);
}
int sqlite3OsCheckReservedLock(sqlite3_file *id){
  return id->pMethods->xCheckReservedLock(id);
}
int sqlite3OsSectorSize(sqlite3_file *id){
  int (*xSectorSize)(sqlite3_file*) = id->pMethods->xSectorSize;
  return xSectorSize ? xSectorSize(id) : SQLITE_DEFAULT_SECTOR_SIZE;
}
int sqlite3OsDeviceCharacteristics(sqlite3_file *id){
  return id->pMethods->xDeviceCharacteristics(id);
}

#if defined(SQLITE_TEST) || defined(SQLITE_DEBUG)
  /* These methods are currently only used for testing and debugging. */
  int sqlite3OsFileHandle(sqlite3_file *id){
    /* return id->pMethods->xFileHandle(id); */
    return 0;
  }
  int sqlite3OsLockState(sqlite3_file *id){
    return id->pMethods->xLockState(id);
  }
#endif

int sqlite3OsOpen(
  sqlite3_vfs *pVfs, 
  const char *zPath, 
  sqlite3_file *pFile, 
  int flags, 
  int *pFlagsOut
){
  return pVfs->xOpen(pVfs->pAppData, zPath, pFile, flags, pFlagsOut);
}
int sqlite3OsDelete(sqlite3_vfs *pVfs, const char *zPath){
  return pVfs->xDelete(pVfs->pAppData, zPath);
}
int sqlite3OsAccess(sqlite3_vfs *pVfs, const char *zPath, int flags){
  return pVfs->xAccess(pVfs->pAppData, zPath, flags);
}
int sqlite3OsGetTempName(sqlite3_vfs *pVfs, char *zBufOut){
  return pVfs->xGetTempName(pVfs->pAppData, zBufOut);
}
int sqlite3OsFullPathname(sqlite3_vfs *pVfs, const char *zPath, char *zPathOut){
  return pVfs->xFullPathname(pVfs->pAppData, zPath, zPathOut);
}
void *sqlite3OsDlOpen(sqlite3_vfs *pVfs, const char *zPath){
  return pVfs->xDlOpen(pVfs->pAppData, zPath);
}
void sqlite3OsDlError(sqlite3_vfs *pVfs, int nByte, char *zBufOut){
  pVfs->xDlError(pVfs->pAppData, nByte, zBufOut);
}
void *sqlite3OsDlSym(sqlite3_vfs *pVfs, void *pHandle, const char *zSymbol){
  return pVfs->xDlSym(pHandle, zSymbol);
}
void sqlite3OsDlClose(sqlite3_vfs *pVfs, void *pHandle){
  pVfs->xDlClose(pHandle);
}
int sqlite3OsRandomness(sqlite3_vfs *pVfs, int nByte, char *zBufOut){
  return pVfs->xRandomness(pVfs->pAppData, nByte, zBufOut);
}
int sqlite3OsSleep(sqlite3_vfs *pVfs, int nMicro){
  return pVfs->xSleep(pVfs->pAppData, nMicro);
}
int sqlite3OsCurrentTime(sqlite3_vfs *pVfs, double *pTimeOut){
  return pVfs->xCurrentTime(pVfs->pAppData, pTimeOut);
}

int sqlite3OsOpenMalloc(
  sqlite3_vfs *pVfs, 
  const char *zFile, 
  sqlite3_file **ppFile, 
  int flags
){
  int rc = SQLITE_NOMEM;
  sqlite3_file *pFile;
  pFile = (sqlite3_file *)sqlite3_malloc(pVfs->szOsFile);
  if( pFile ){
    rc = sqlite3OsOpen(pVfs, zFile, pFile, flags, 0);
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
  if( pFile ){
    rc = sqlite3OsClose(pFile);
    sqlite3_free(pFile);
  }
  return rc;
}

/* 
** Default vfs implementation. Defined by the various os_X.c implementations.
*/
extern sqlite3_vfs sqlite3DefaultVfs;

sqlite3_vfs *sqlite3_find_vfs(const char *zVfs){
  return &sqlite3DefaultVfs;
}


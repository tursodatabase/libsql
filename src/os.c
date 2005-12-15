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
#include "sqliteInt.h"
#include "os.h"

/*
** The following routines are convenience wrappers around methods
** of the OsFile object.  This is mostly just syntactic sugar.  All
** of this would be completely automatic if SQLite were coded using
** C++ instead of plain old C.
*/
int sqlite3OsClose(OsFile **pId){
  OsFile *id;
  if( pId!=0 && (id = *pId)!=0 ){
    return id->pMethod->xClose(pId);
  }else{
    return SQLITE_OK;
  }
}
int sqlite3OsOpenDirectory(OsFile *id, const char *zName){
  return id->pMethod->xOpenDirectory(id, zName);
}
int sqlite3OsRead(OsFile *id, void *pBuf, int amt){
  return id->pMethod->xRead(id, pBuf, amt);
}
int sqlite3OsWrite(OsFile *id, const void *pBuf, int amt){
  return id->pMethod->xWrite(id, pBuf, amt);
}
int sqlite3OsSeek(OsFile *id, i64 offset){
  return id->pMethod->xSeek(id, offset);
}
int sqlite3OsTruncate(OsFile *id, i64 size){
  return id->pMethod->xTruncate(id, size);
}
int sqlite3OsSync(OsFile *id, int fullsync){
  return id->pMethod->xSync(id, fullsync);
}
void sqlite3OsSetFullSync(OsFile *id, int value){
  id->pMethod->xSetFullSync(id, value);
}
int sqlite3OsFileHandle(OsFile *id){
  return id->pMethod->xFileHandle(id);
}
int sqlite3OsFileSize(OsFile *id, i64 *pSize){
  return id->pMethod->xFileSize(id, pSize);
}
int sqlite3OsLock(OsFile *id, int lockType){
  return id->pMethod->xLock(id, lockType);
}
int sqlite3OsUnlock(OsFile *id, int lockType){
  return id->pMethod->xUnlock(id, lockType);
}
int sqlite3OsLockState(OsFile *id){
  return id->pMethod->xLockState(id);
}
int sqlite3OsCheckReservedLock(OsFile *id){
  return id->pMethod->xCheckReservedLock(id);
}

static void**getOsRoutinePtr(int eRoutine){
  switch( eRoutine ){
    case SQLITE_OS_ROUTINE_OPENREADWRITE:
      return (void **)(&sqlite3Os.xOpenReadWrite);
    case SQLITE_OS_ROUTINE_OPENREADONLY:
      return (void **)(&sqlite3Os.xOpenReadOnly);
    case SQLITE_OS_ROUTINE_OPENEXCLUSIVE:
      return (void **)(&sqlite3Os.xOpenExclusive);
    case SQLITE_OS_ROUTINE_DELETE:
      return (void **)(&sqlite3Os.xDelete);
    case SQLITE_OS_ROUTINE_FILEEXISTS:
      return (void **)(&sqlite3Os.xFileExists);
    case SQLITE_OS_ROUTINE_SYNCDIRECTORY:
      return (void **)(&sqlite3Os.xSyncDirectory);
    default:
      assert(!"Illegal eRoutine value");
  }
  return 0;
}

void *sqlite3_os_routine_get(int eRoutine){
  return *getOsRoutinePtr(eRoutine);
}

void *sqlite3_os_routine_set(int eRoutine, void *pRoutine){
  void **ppRet = getOsRoutinePtr(eRoutine);
  void *pRet = *ppRet;
  *ppRet = pRoutine;
  return pRet;
}

void sqlite3_os_enter_mutex(){
  sqlite3Os.xEnterMutex();
}
void sqlite3_os_leave_mutex(){
  sqlite3Os.xLeaveMutex();
}


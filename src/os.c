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
int sqlite3OsClose(sqlite3_file **pId){
  int rc = SQLITE_OK;
  sqlite3_file *id;
  if( pId!=0 && (id = *pId)!=0 ){
    rc = id->pMethods->xClose(id);
    if( rc==SQLITE_OK ){
      *pId = 0;
    }
  }
  return rc;
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
int sqlite3OsSync(sqlite3_file *id, int fullsync){
  return id->pMethods->xSync(id, fullsync);
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
    /* return id->pMethods->xLockState(id); */
    return 0;
  }
#endif

#ifdef SQLITE_ENABLE_REDEF_IO
/*
** A function to return a pointer to the virtual function table.
** This routine really does not accomplish very much since the
** virtual function table is a global variable and anybody who
** can call this function can just as easily access the variable
** for themselves.  Nevertheless, we include this routine for
** backwards compatibility with an earlier redefinable I/O
** interface design.
*/
struct sqlite3OsVtbl *sqlite3_os_switch(void){
  return &sqlite3Os;
}
#endif

/*
** 2001 September 15
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
*************************************************************************
**
** Memory allocation functions used throughout sqlite.
**
** $Id: malloc.c,v 1.16 2008/06/14 16:56:22 drh Exp $
*/
#include "sqliteInt.h"
#include <stdarg.h>
#include <ctype.h>

/*
** This routine runs when the memory allocator sees that the
** total memory allocation is about to exceed the soft heap
** limit.
*/
static void softHeapLimitEnforcer(
  void *NotUsed, 
  sqlite3_int64 inUse,
  int allocSize
){
  sqlite3_release_memory(allocSize);
}

/*
** Set the soft heap-size limit for the current thread. Passing a
** zero or negative value indicates no limit.
*/
void sqlite3_soft_heap_limit(int n){
  sqlite3_uint64 iLimit;
  int overage;
  if( n<0 ){
    iLimit = 0;
  }else{
    iLimit = n;
  }
  if( iLimit>0 ){
    sqlite3_memory_alarm(softHeapLimitEnforcer, 0, iLimit);
  }else{
    sqlite3_memory_alarm(0, 0, 0);
  }
  overage = sqlite3_memory_used() - n;
  if( overage>0 ){
    sqlite3_release_memory(overage);
  }
}

/*
** Release memory held by SQLite instances created by the current thread.
*/
int sqlite3_release_memory(int n){
#ifdef SQLITE_ENABLE_MEMORY_MANAGEMENT
  int nRet = sqlite3VdbeReleaseMemory(n);
  nRet += sqlite3PagerReleaseMemory(n-nRet);
  return nRet;
#else
  return SQLITE_OK;
#endif
}

/*
** State information local to the memory allocation subsystem.
*/
static struct {
  sqlite3_mutex *mutex;         /* Mutex to serialize access */

  /*
  ** The alarm callback and its arguments.  The mem0.mutex lock will
  ** be held while the callback is running.  Recursive calls into
  ** the memory subsystem are allowed, but no new callbacks will be
  ** issued.  The alarmBusy variable is set to prevent recursive
  ** callbacks.
  */
  sqlite3_int64 alarmThreshold;
  void (*alarmCallback)(void*, sqlite3_int64,int);
  void *alarmArg;
  int alarmBusy;

  /*
  ** Performance statistics
  */
  sqlite3_int64 nowUsed;  /* Main memory currently in use */
  sqlite3_int64 mxUsed;   /* Highwater mark for nowUsed */
  int mxReq;              /* maximum request size for main or page-cache mem */
} mem0;

/*
** Initialize the memory allocation subsystem.
*/
int sqlite3MallocInit(void){
  if( sqlite3Config.m.xMalloc==0 ){
    sqlite3MemSetDefault();
  }
  memset(&mem0, 0, sizeof(mem0));
  if( sqlite3Config.bMemstat && sqlite3Config.bCoreMutex ){
    mem0.mutex = sqlite3_mutex_alloc(SQLITE_MUTEX_STATIC_MEM);
  }
  return sqlite3Config.m.xInit(sqlite3Config.m.pAppData);
}

/*
** Deinitialize the memory allocation subsystem.
*/
void sqlite3MallocEnd(void){
   sqlite3Config.m.xShutdown(sqlite3Config.m.pAppData);
}

/*
** Return the amount of memory currently checked out.
*/
sqlite3_int64 sqlite3_memory_used(void){
  sqlite3_int64 n;
  sqlite3_mutex_enter(mem0.mutex);
  n = mem0.nowUsed;
  sqlite3_mutex_leave(mem0.mutex);  
  return n;
}

/*
** Return the maximum amount of memory that has ever been
** checked out since either the beginning of this process
** or since the most recent reset.
*/
sqlite3_int64 sqlite3_memory_highwater(int resetFlag){
  sqlite3_int64 n;
  sqlite3_mutex_enter(mem0.mutex);
  n = mem0.mxUsed;
  if( resetFlag ){
    mem0.mxUsed = mem0.nowUsed;
  }
  sqlite3_mutex_leave(mem0.mutex);  
  return n;
}

/*
** Change the alarm callback
*/
int sqlite3_memory_alarm(
  void(*xCallback)(void *pArg, sqlite3_int64 used,int N),
  void *pArg,
  sqlite3_int64 iThreshold
){
  sqlite3_mutex_enter(mem0.mutex);
  mem0.alarmCallback = xCallback;
  mem0.alarmArg = pArg;
  mem0.alarmThreshold = iThreshold;
  sqlite3_mutex_leave(mem0.mutex);
  return SQLITE_OK;
}

/*
** Trigger the alarm 
*/
static void sqlite3MallocAlarm(int nByte){
  void (*xCallback)(void*,sqlite3_int64,int);
  sqlite3_int64 nowUsed;
  void *pArg;
  if( mem0.alarmCallback==0 || mem0.alarmBusy  ) return;
  mem0.alarmBusy = 1;
  xCallback = mem0.alarmCallback;
  nowUsed = mem0.nowUsed;
  pArg = mem0.alarmArg;
  sqlite3_mutex_leave(mem0.mutex);
  xCallback(pArg, nowUsed, nByte);
  sqlite3_mutex_enter(mem0.mutex);
  mem0.alarmBusy = 0;
}


/*
** Allocate memory.  This routine is like sqlite3_malloc() except that it
** assumes the memory subsystem has already been initialized.
*/
void *sqlite3Malloc(int n){
  void *p;
  int nFull;
  if( n<=0 ){
    return 0;
  }else if( sqlite3Config.bMemstat ){
    nFull = sqlite3Config.m.xRoundup(n);
    sqlite3_mutex_enter(mem0.mutex);
    if( n>mem0.mxReq ) mem0.mxReq = n;
    if( mem0.alarmCallback!=0 && mem0.nowUsed+nFull>=mem0.alarmThreshold ){
      sqlite3MallocAlarm(nFull);
    }
    if( sqlite3FaultStep(SQLITE_FAULTINJECTOR_MALLOC) ){
      p = 0;
    }else{
      p = sqlite3Config.m.xMalloc(nFull);
      if( p==0 ){
        sqlite3MallocAlarm(nFull);
        p = malloc(nFull);
      }
    }
    if( p ){
      mem0.nowUsed += nFull;
      if( mem0.nowUsed>mem0.mxUsed ){
        mem0.mxUsed = mem0.nowUsed;
      }
    }
    sqlite3_mutex_leave(mem0.mutex);
  }else{
    p = sqlite3Config.m.xMalloc(n);
  }
  return p;
}

/*
** This version of the memory allocation is for use by the application.
** First make sure the memory subsystem is initialized, then do the
** allocation.
*/
void *sqlite3_malloc(int n){
#ifndef SQLITE_OMIT_AUTOINIT
  if( sqlite3_initialize() ) return 0;
#endif
  return sqlite3Malloc(n);
}

/*
** Return the size of a memory allocation previously obtained from
** sqlite3Malloc() or sqlite3_malloc().
*/
int sqlite3MallocSize(void *p){
  return sqlite3Config.m.xSize(p);
}

/*
** Free memory previously obtained from sqlite3Malloc().
*/
void sqlite3_free(void *p){
  if( p==0 ) return;
  if( sqlite3Config.bMemstat ){
    sqlite3_mutex_enter(mem0.mutex);
    mem0.nowUsed -= sqlite3MallocSize(p);
    sqlite3Config.m.xFree(p);
    sqlite3_mutex_leave(mem0.mutex);
  }else{
    sqlite3Config.m.xFree(p);
  }
}

/*
** Change the size of an existing memory allocation
*/
void *sqlite3Realloc(void *pOld, int nBytes){
  int nOld, nNew;
  void *pNew;
  if( pOld==0 ){
    return sqlite3Malloc(nBytes);
  }
  if( nBytes<=0 ){
    sqlite3_free(pOld);
    return 0;
  }
  nOld = sqlite3MallocSize(pOld);
  if( sqlite3Config.bMemstat ){
    sqlite3_mutex_enter(mem0.mutex);
    if( nBytes>mem0.mxReq ) mem0.mxReq = nBytes;
    nNew = sqlite3Config.m.xRoundup(nBytes);
    if( nOld==nNew ){
      pNew = pOld;
    }else{
      if( mem0.nowUsed+nNew-nOld>=mem0.alarmThreshold ){
        sqlite3MallocAlarm(nNew-nOld);
      }
      if( sqlite3FaultStep(SQLITE_FAULTINJECTOR_MALLOC) ){
        pNew = 0;
      }else{
        pNew = sqlite3Config.m.xRealloc(pOld, nNew);
        if( pNew==0 ){
          sqlite3MallocAlarm(nBytes);
          pNew = sqlite3Config.m.xRealloc(pOld, nNew);
        }
      }
      if( pNew ){
        mem0.nowUsed += nNew-nOld;
        if( mem0.nowUsed>mem0.mxUsed ){
          mem0.mxUsed = mem0.nowUsed;
        }
      }
    }
    sqlite3_mutex_leave(mem0.mutex);
  }else{
    pNew = sqlite3Config.m.xRealloc(pOld, nBytes);
  }
  return pNew;
}

/*
** The public interface to sqlite3Realloc.  Make sure that the memory
** subsystem is initialized prior to invoking sqliteRealloc.
*/
void *sqlite3_realloc(void *pOld, int n){
#ifndef SQLITE_OMIT_AUTOINIT
  if( sqlite3_initialize() ) return 0;
#endif
  return sqlite3Realloc(pOld, n);
}


/*
** Allocate and zero memory.
*/ 
void *sqlite3MallocZero(int n){
  void *p = sqlite3Malloc(n);
  if( p ){
    memset(p, 0, n);
  }
  return p;
}

/*
** Allocate and zero memory.  If the allocation fails, make
** the mallocFailed flag in the connection pointer.
*/
void *sqlite3DbMallocZero(sqlite3 *db, int n){
  void *p = sqlite3DbMallocRaw(db, n);
  if( p ){
    memset(p, 0, n);
  }
  return p;
}

/*
** Allocate and zero memory.  If the allocation fails, make
** the mallocFailed flag in the connection pointer.
*/
void *sqlite3DbMallocRaw(sqlite3 *db, int n){
  void *p = 0;
  if( !db || db->mallocFailed==0 ){
    p = sqlite3Malloc(n);
    if( !p && db ){
      db->mallocFailed = 1;
    }
  }
  return p;
}

/*
** Resize the block of memory pointed to by p to n bytes. If the
** resize fails, set the mallocFailed flag inthe connection object.
*/
void *sqlite3DbRealloc(sqlite3 *db, void *p, int n){
  void *pNew = 0;
  if( db->mallocFailed==0 ){
    pNew = sqlite3_realloc(p, n);
    if( !pNew ){
      db->mallocFailed = 1;
    }
  }
  return pNew;
}

/*
** Attempt to reallocate p.  If the reallocation fails, then free p
** and set the mallocFailed flag in the database connection.
*/
void *sqlite3DbReallocOrFree(sqlite3 *db, void *p, int n){
  void *pNew;
  pNew = sqlite3DbRealloc(db, p, n);
  if( !pNew ){
    sqlite3_free(p);
  }
  return pNew;
}

/*
** Make a copy of a string in memory obtained from sqliteMalloc(). These 
** functions call sqlite3MallocRaw() directly instead of sqliteMalloc(). This
** is because when memory debugging is turned on, these two functions are 
** called via macros that record the current file and line number in the
** ThreadData structure.
*/
char *sqlite3StrDup(const char *z){
  char *zNew;
  int n;
  if( z==0 ) return 0;
  n = strlen(z)+1;
  zNew = sqlite3_malloc(n);
  if( zNew ) memcpy(zNew, z, n);
  return zNew;
}
char *sqlite3StrNDup(const char *z, int n){
  char *zNew;
  if( z==0 ) return 0;
  zNew = sqlite3_malloc(n+1);
  if( zNew ){
    memcpy(zNew, z, n);
    zNew[n] = 0;
  }
  return zNew;
}

char *sqlite3DbStrDup(sqlite3 *db, const char *z){
  char *zNew = sqlite3StrDup(z);
  if( z && !zNew ){
    db->mallocFailed = 1;
  }
  return zNew;
}
char *sqlite3DbStrNDup(sqlite3 *db, const char *z, int n){
  char *zNew = sqlite3StrNDup(z, n);
  if( z && !zNew ){
    db->mallocFailed = 1;
  }
  return zNew;
}

/*
** Create a string from the 2nd and subsequent arguments (up to the
** first NULL argument), store the string in memory obtained from
** sqliteMalloc() and make the pointer indicated by the 1st argument
** point to that string.  The 1st argument must either be NULL or 
** point to memory obtained from sqliteMalloc().
*/
void sqlite3SetString(char **pz, ...){
  va_list ap;
  int nByte;
  const char *z;
  char *zResult;

  assert( pz!=0 );
  nByte = 1;
  va_start(ap, pz);
  while( (z = va_arg(ap, const char*))!=0 ){
    nByte += strlen(z);
  }
  va_end(ap);
  sqlite3_free(*pz);
  *pz = zResult = sqlite3_malloc(nByte);
  if( zResult==0 ){
    return;
  }
  *zResult = 0;
  va_start(ap, pz);
  while( (z = va_arg(ap, const char*))!=0 ){
    int n = strlen(z);
    memcpy(zResult, z, n);
    zResult += n;
  }
  zResult[0] = 0;
  va_end(ap);
}


/*
** This function must be called before exiting any API function (i.e. 
** returning control to the user) that has called sqlite3_malloc or
** sqlite3_realloc.
**
** The returned value is normally a copy of the second argument to this
** function. However, if a malloc() failure has occured since the previous
** invocation SQLITE_NOMEM is returned instead. 
**
** If the first argument, db, is not NULL and a malloc() error has occured,
** then the connection error-code (the value returned by sqlite3_errcode())
** is set to SQLITE_NOMEM.
*/
int sqlite3ApiExit(sqlite3* db, int rc){
  /* If the db handle is not NULL, then we must hold the connection handle
  ** mutex here. Otherwise the read (and possible write) of db->mallocFailed 
  ** is unsafe, as is the call to sqlite3Error().
  */
  assert( !db || sqlite3_mutex_held(db->mutex) );
  if( db && db->mallocFailed ){
    sqlite3Error(db, SQLITE_NOMEM, 0);
    db->mallocFailed = 0;
    rc = SQLITE_NOMEM;
  }
  return rc & (db ? db->errMask : 0xff);
}

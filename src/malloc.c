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
** $Id: malloc.c,v 1.28 2008/07/14 12:38:21 drh Exp $
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
** Set the soft heap-size limit for the library. Passing a zero or 
** negative value indicates no limit.
*/
void sqlite3_soft_heap_limit(int n){
  sqlite3_uint64 iLimit;
  int overage;
  if( n<0 ){
    iLimit = 0;
  }else{
    iLimit = n;
  }
  sqlite3_initialize();
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
** Attempt to release up to n bytes of non-essential memory currently
** held by SQLite. An example of non-essential memory is memory used to
** cache database pages that are not currently in use.
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
  ** Pointers to the end of sqlite3Config.pScratch and
  ** sqlite3Config.pPage to a block of memory that records
  ** which pages are available.
  */
  u32 *aScratchFree;
  u32 *aPageFree;

  /* Number of free pages for scratch and page-cache memory */
  u32 nScratchFree;
  u32 nPageFree;
} mem0;

/*
** Initialize the memory allocation subsystem.
*/
int sqlite3MallocInit(void){
  if( sqlite3Config.m.xMalloc==0 ){
    sqlite3MemSetDefault();
  }
  memset(&mem0, 0, sizeof(mem0));
  if( sqlite3Config.bCoreMutex ){
    mem0.mutex = sqlite3MutexAlloc(SQLITE_MUTEX_STATIC_MEM);
  }
  if( sqlite3Config.pScratch && sqlite3Config.szScratch>=3000
      && sqlite3Config.nScratch>0 ){
    int i;
    mem0.aScratchFree = (u32*)&((char*)sqlite3Config.pScratch)
                  [sqlite3Config.szScratch*sqlite3Config.nScratch];
    for(i=0; i<sqlite3Config.nScratch; i++){ mem0.aScratchFree[i] = i; }
    mem0.nScratchFree = sqlite3Config.nScratch;
  }else{
    sqlite3Config.pScratch = 0;
    sqlite3Config.szScratch = 0;
  }
  if( sqlite3Config.pPage && sqlite3Config.szPage>=512
      && sqlite3Config.nPage>0 ){
    int i;
    mem0.aPageFree = (u32*)&((char*)sqlite3Config.pPage)
                  [sqlite3Config.szPage*sqlite3Config.nPage];
    for(i=0; i<sqlite3Config.nPage; i++){ mem0.aPageFree[i] = i; }
    mem0.nPageFree = sqlite3Config.nPage;
  }else{
    sqlite3Config.pPage = 0;
    sqlite3Config.szPage = 0;
  }
  return sqlite3Config.m.xInit(sqlite3Config.m.pAppData);
}

/*
** Deinitialize the memory allocation subsystem.
*/
void sqlite3MallocEnd(void){
  sqlite3Config.m.xShutdown(sqlite3Config.m.pAppData);
  memset(&mem0, 0, sizeof(mem0));
}

/*
** Return the amount of memory currently checked out.
*/
sqlite3_int64 sqlite3_memory_used(void){
  int n, mx;
  sqlite3_int64 res;
  sqlite3_status(SQLITE_STATUS_MEMORY_USED, &n, &mx, 0);
  res = (sqlite3_int64)n;  /* Work around bug in Borland C. Ticket #3216 */
  return res;
}

/*
** Return the maximum amount of memory that has ever been
** checked out since either the beginning of this process
** or since the most recent reset.
*/
sqlite3_int64 sqlite3_memory_highwater(int resetFlag){
  int n, mx;
  sqlite3_int64 res;
  sqlite3_status(SQLITE_STATUS_MEMORY_USED, &n, &mx, resetFlag);
  res = (sqlite3_int64)mx;  /* Work around bug in Borland C. Ticket #3216 */
  return res;
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
  nowUsed = sqlite3StatusValue(SQLITE_STATUS_MEMORY_USED);
  pArg = mem0.alarmArg;
  sqlite3_mutex_leave(mem0.mutex);
  xCallback(pArg, nowUsed, nByte);
  sqlite3_mutex_enter(mem0.mutex);
  mem0.alarmBusy = 0;
}

/*
** Do a memory allocation with statistics and alarms.  Assume the
** lock is already held.
*/
static int mallocWithAlarm(int n, void **pp){
  int nFull;
  void *p;
  assert( sqlite3_mutex_held(mem0.mutex) );
  nFull = sqlite3Config.m.xRoundup(n);
  sqlite3StatusSet(SQLITE_STATUS_MALLOC_SIZE, n);
  if( mem0.alarmCallback!=0 ){
    int nUsed = sqlite3StatusValue(SQLITE_STATUS_MEMORY_USED);
    if( nUsed+nFull >= mem0.alarmThreshold ){
      sqlite3MallocAlarm(nFull);
    }
  }
  p = sqlite3Config.m.xMalloc(nFull);
  if( p==0 && mem0.alarmCallback ){
    sqlite3MallocAlarm(nFull);
    p = sqlite3Config.m.xMalloc(nFull);
  }
  if( p ) sqlite3StatusAdd(SQLITE_STATUS_MEMORY_USED, nFull);
  *pp = p;
  return nFull;
}

/*
** Allocate memory.  This routine is like sqlite3_malloc() except that it
** assumes the memory subsystem has already been initialized.
*/
void *sqlite3Malloc(int n){
  void *p;
  if( n<=0 ){
    p = 0;
  }else if( sqlite3Config.bMemstat ){
    sqlite3_mutex_enter(mem0.mutex);
    mallocWithAlarm(n, &p);
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
** Each thread may only have a single outstanding allocation from
** xScratchMalloc().  We verify this constraint in the single-threaded
** case by setting scratchAllocOut to 1 when an allocation
** is outstanding clearing it when the allocation is freed.
*/
#if SQLITE_THREADSAFE==0 && !defined(NDEBUG)
static int scratchAllocOut = 0;
#endif


/*
** Allocate memory that is to be used and released right away.
** This routine is similar to alloca() in that it is not intended
** for situations where the memory might be held long-term.  This
** routine is intended to get memory to old large transient data
** structures that would not normally fit on the stack of an
** embedded processor.
*/
void *sqlite3ScratchMalloc(int n){
  void *p;
  assert( n>0 );

#if SQLITE_THREADSAFE==0 && !defined(NDEBUG)
  /* Verify that no more than one scratch allocation per thread
  ** is outstanding at one time.  (This is only checked in the
  ** single-threaded case since checking in the multi-threaded case
  ** would be much more complicated.) */
  assert( scratchAllocOut==0 );
#endif

  if( sqlite3Config.szScratch<n ){
    goto scratch_overflow;
  }else{  
    sqlite3_mutex_enter(mem0.mutex);
    if( mem0.nScratchFree==0 ){
      sqlite3_mutex_leave(mem0.mutex);
      goto scratch_overflow;
    }else{
      int i;
      i = mem0.aScratchFree[--mem0.nScratchFree];
      sqlite3_mutex_leave(mem0.mutex);
      i *= sqlite3Config.szScratch;
      sqlite3StatusAdd(SQLITE_STATUS_SCRATCH_USED, 1);
      p = (void*)&((char*)sqlite3Config.pScratch)[i];
    }
  }
#if SQLITE_THREADSAFE==0 && !defined(NDEBUG)
  scratchAllocOut = p!=0;
#endif

  return p;

scratch_overflow:
  if( sqlite3Config.bMemstat ){
    sqlite3_mutex_enter(mem0.mutex);
    n = mallocWithAlarm(n, &p);
    if( p ) sqlite3StatusAdd(SQLITE_STATUS_SCRATCH_OVERFLOW, n);
    sqlite3_mutex_leave(mem0.mutex);
  }else{
    p = sqlite3Config.m.xMalloc(n);
  }
#if SQLITE_THREADSAFE==0 && !defined(NDEBUG)
  scratchAllocOut = p!=0;
#endif
  return p;    
}
void sqlite3ScratchFree(void *p){
  if( p ){

#if SQLITE_THREADSAFE==0 && !defined(NDEBUG)
    /* Verify that no more than one scratch allocation per thread
    ** is outstanding at one time.  (This is only checked in the
    ** single-threaded case since checking in the multi-threaded case
    ** would be much more complicated.) */
    assert( scratchAllocOut==1 );
    scratchAllocOut = 0;
#endif

    if( sqlite3Config.pScratch==0
           || p<sqlite3Config.pScratch
           || p>=(void*)mem0.aScratchFree ){
      if( sqlite3Config.bMemstat ){
        int iSize = sqlite3MallocSize(p);
        sqlite3_mutex_enter(mem0.mutex);
        sqlite3StatusAdd(SQLITE_STATUS_SCRATCH_OVERFLOW, -iSize);
        sqlite3StatusAdd(SQLITE_STATUS_MEMORY_USED, -iSize);
        sqlite3Config.m.xFree(p);
        sqlite3_mutex_leave(mem0.mutex);
      }else{
        sqlite3Config.m.xFree(p);
      }
    }else{
      int i;
      i = (u8 *)p - (u8 *)sqlite3Config.pScratch;
      i /= sqlite3Config.szScratch;
      assert( i>=0 && i<sqlite3Config.nScratch );
      sqlite3_mutex_enter(mem0.mutex);
      assert( mem0.nScratchFree<sqlite3Config.nScratch );
      mem0.aScratchFree[mem0.nScratchFree++] = i;
      sqlite3StatusAdd(SQLITE_STATUS_SCRATCH_USED, -1);
      sqlite3_mutex_leave(mem0.mutex);
    }
  }
}

/*
** Allocate memory to be used by the page cache.  Make use of the
** memory buffer provided by SQLITE_CONFIG_PAGECACHE if there is one
** and that memory is of the right size and is not completely
** consumed.  Otherwise, failover to sqlite3Malloc().
*/
void *sqlite3PageMalloc(int n){
  void *p;
  assert( n>0 );
  assert( (n & (n-1))==0 );
  assert( n>=512 && n<=32768 );

  if( sqlite3Config.szPage<n ){
    goto page_overflow;
  }else{  
    sqlite3_mutex_enter(mem0.mutex);
    if( mem0.nPageFree==0 ){
      sqlite3_mutex_leave(mem0.mutex);
      goto page_overflow;
    }else{
      int i;
      i = mem0.aPageFree[--mem0.nPageFree];
      sqlite3_mutex_leave(mem0.mutex);
      i *= sqlite3Config.szPage;
      sqlite3StatusAdd(SQLITE_STATUS_PAGECACHE_USED, 1);
      p = (void*)&((char*)sqlite3Config.pPage)[i];
    }
  }
  return p;

page_overflow:
  if( sqlite3Config.bMemstat ){
    sqlite3_mutex_enter(mem0.mutex);
    n = mallocWithAlarm(n, &p);
    if( p ) sqlite3StatusAdd(SQLITE_STATUS_PAGECACHE_OVERFLOW, n);
    sqlite3_mutex_leave(mem0.mutex);
  }else{
    p = sqlite3Config.m.xMalloc(n);
  }
  return p;    
}
void sqlite3PageFree(void *p){
  if( p ){
    if( sqlite3Config.pPage==0
           || p<sqlite3Config.pPage
           || p>=(void*)mem0.aPageFree ){
      /* In this case, the page allocation was obtained from a regular 
      ** call to sqlite3_mem_methods.xMalloc() (a page-cache-memory 
      ** "overflow"). Free the block with sqlite3_mem_methods.xFree().
      */
      if( sqlite3Config.bMemstat ){
        int iSize = sqlite3MallocSize(p);
        sqlite3_mutex_enter(mem0.mutex);
        sqlite3StatusAdd(SQLITE_STATUS_PAGECACHE_OVERFLOW, -iSize);
        sqlite3StatusAdd(SQLITE_STATUS_MEMORY_USED, -iSize);
        sqlite3Config.m.xFree(p);
        sqlite3_mutex_leave(mem0.mutex);
      }else{
        sqlite3Config.m.xFree(p);
      }
    }else{
      /* The page allocation was allocated from the sqlite3Config.pPage
      ** buffer. In this case all that is add the index of the page in
      ** the sqlite3Config.pPage array to the set of free indexes stored
      ** in the mem0.aPageFree[] array.
      */
      int i;
      i = (u8 *)p - (u8 *)sqlite3Config.pPage;
      i /= sqlite3Config.szPage;
      assert( i>=0 && i<sqlite3Config.nPage );
      sqlite3_mutex_enter(mem0.mutex);
      assert( mem0.nPageFree<sqlite3Config.nPage );
      mem0.aPageFree[mem0.nPageFree++] = i;
      sqlite3StatusAdd(SQLITE_STATUS_PAGECACHE_USED, -1);
      sqlite3_mutex_leave(mem0.mutex);
#ifndef NDEBUG
      /* Assert that a duplicate was not just inserted into aPageFree[]. */
      for(i=0; i<mem0.nPageFree-1; i++){
        assert( mem0.aPageFree[i]!=mem0.aPageFree[mem0.nPageFree-1] );
      }
#endif
    }
  }
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
    sqlite3StatusAdd(SQLITE_STATUS_MEMORY_USED, -sqlite3MallocSize(p));
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
    sqlite3StatusSet(SQLITE_STATUS_MALLOC_SIZE, nBytes);
    nNew = sqlite3Config.m.xRoundup(nBytes);
    if( nOld==nNew ){
      pNew = pOld;
    }else{
      if( sqlite3StatusValue(SQLITE_STATUS_MEMORY_USED)+nNew-nOld >= 
            mem0.alarmThreshold ){
        sqlite3MallocAlarm(nNew-nOld);
      }
      pNew = sqlite3Config.m.xRealloc(pOld, nNew);
      if( pNew==0 && mem0.alarmCallback ){
        sqlite3MallocAlarm(nBytes);
        pNew = sqlite3Config.m.xRealloc(pOld, nNew);
      }
      if( pNew ){
        sqlite3StatusAdd(SQLITE_STATUS_MEMORY_USED, nNew-nOld);
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
  zNew = sqlite3Malloc(n);
  if( zNew ) memcpy(zNew, z, n);
  return zNew;
}
char *sqlite3StrNDup(const char *z, int n){
  char *zNew;
  if( z==0 ) return 0;
  zNew = sqlite3Malloc(n+1);
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
** Create a string from the zFromat argument and the va_list that follows.
** Store the string in memory obtained from sqliteMalloc() and make *pz
** point to that string.
*/
void sqlite3SetString(char **pz, sqlite3 *db, const char *zFormat, ...){
  va_list ap;
  char *z;

  va_start(ap, zFormat);
  z = sqlite3VMPrintf(db, zFormat, ap);
  va_end(ap);
  sqlite3_free(*pz);
  *pz = z;
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

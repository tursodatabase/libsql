/*
** 2007 August 15
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
*************************************************************************
** This file contains the C functions that implement a memory
** allocation subsystem for use by SQLite.  
**
** $Id: mem2.c,v 1.7 2007/08/22 22:04:37 drh Exp $
*/

/*
** This version of the memory allocator is used only if the
** SQLITE_MEMDEBUG macro is defined and SQLITE_OMIT_MEMORY_ALLOCATION
** is not defined.
*/
#if defined(SQLITE_MEMDEBUG) && !defined(SQLITE_OMIT_MEMORY_ALLOCATION)

/*
** We will eventually construct multiple memory allocation subsystems
** suitable for use in various contexts:
**
**    *  Normal multi-threaded builds
**    *  Normal single-threaded builds
**    *  Debugging builds
**
** This version is suitable for use in debugging builds.
**
** Features:
**
**    * Every allocate has guards at both ends.
**    * New allocations are initialized with randomness
**    * Allocations are overwritten with randomness when freed
**    * Optional logs of malloc activity generated
**    * Summary of outstanding allocations with backtraces to the
**      point of allocation.
**    * The ability to simulate memory allocation failure
*/
#include "sqliteInt.h"
#include <stdio.h>

/*
** The backtrace functionality is only available with GLIBC
*/
#ifdef __GLIBC__
  extern int backtrace(void**,int);
  extern void backtrace_symbols_fd(void*const*,int,int);
#else
# define backtrace(A,B) 0
# define backtrace_symbols_fd(A,B,C)
#endif

/*
** Each memory allocation looks like this:
**
**    ----------------------------------------------------------------
**    |  backtrace pointers |  MemBlockHdr |  allocation |  EndGuard |
**    ----------------------------------------------------------------
**
** The application code sees only a pointer to the allocation.  We have
** to back up from the allocation pointer to find the MemBlockHdr.  The
** MemBlockHdr tells us the size of the allocation and the number of
** backtrace pointers.  There is also a guard word at the end of the
** MemBlockHdr.
*/
struct MemBlockHdr {
  struct MemBlockHdr *pNext, *pPrev;  /* Linked list of all unfreed memory */
  unsigned int iSize;                 /* Size of this allocation */
  unsigned short nBacktrace;          /* Number of backtraces on this alloc */
  unsigned short nBacktraceSlots;     /* Available backtrace slots */
  unsigned int iForeGuard;            /* Guard word for sanity */
};

/*
** Guard words
*/
#define FOREGUARD 0x80F5E153
#define REARGUARD 0xE4676B53

/*
** All of the static variables used by this module are collected
** into a single structure named "mem".  This is to keep the
** static variables organized and to reduce namespace pollution
** when this module is combined with other in the amalgamation.
*/
static struct {
  /*
  ** The alarm callback and its arguments.  The mem.mutex lock will
  ** be held while the callback is running.  Recursive calls into
  ** the memory subsystem are allowed, but no new callbacks will be
  ** issued.  The alarmBusy variable is set to prevent recursive
  ** callbacks.
  */
  sqlite3_uint64 alarmThreshold;
  void (*alarmCallback)(void*, sqlite3_uint64, unsigned);
  void *alarmArg;
  int alarmBusy;
  
  /*
  ** Mutex to control access to the memory allocation subsystem.
  */
  sqlite3_mutex *mutex;
  
  /*
  ** Current allocation and high-water mark.
  */
  sqlite3_uint64 nowUsed;
  sqlite3_uint64 mxUsed;
  
  /*
  ** Head and tail of a linked list of all outstanding allocations
  */
  struct MemBlockHdr *pFirst;
  struct MemBlockHdr *pLast;
  
  /*
  ** The number of levels of backtrace to save in new allocations.
  */
  int nBacktrace;

  /*
  ** These values are used to simulate malloc failures.  When
  ** iFail is 1, simulate a malloc failures and reset the value
  ** to iReset.
  */
  int iFail;    /* Decrement and fail malloc when this is 1 */
  int iReset;   /* When malloc fails set iiFail to this value */
  int iFailCnt; /* Number of failures */

  /* 
  ** sqlite3MallocDisallow() increments the following counter.
  ** sqlite3MallocAllow() decrements it.
  */
  int disallow; /* Do not allow memory allocation */
  
  
} mem = {  /* This variable holds all of the local data */
   ((sqlite3_uint64)1)<<63,    /* alarmThreshold */
   /* Everything else is initialized to zero */
};



/*
** Return the amount of memory currently checked out.
*/
sqlite3_uint64 sqlite3_memory_used(void){
  sqlite3_uint64 n;
  if( mem.mutex==0 ){
    mem.mutex = sqlite3_mutex_alloc(SQLITE_MUTEX_STATIC_MEM);
  }
  sqlite3_mutex_enter(mem.mutex);
  n = mem.nowUsed;
  sqlite3_mutex_leave(mem.mutex);  
  return n;
}

/*
** Return the maximum amount of memory that has ever been
** checked out since either the beginning of this process
** or since the most recent reset.
*/
sqlite3_uint64 sqlite3_memory_highwater(int resetFlag){
  sqlite3_uint64 n;
  if( mem.mutex==0 ){
    mem.mutex = sqlite3_mutex_alloc(SQLITE_MUTEX_STATIC_MEM);
  }
  sqlite3_mutex_enter(mem.mutex);
  n = mem.mxUsed;
  if( resetFlag ){
    mem.mxUsed = mem.nowUsed;
  }
  sqlite3_mutex_leave(mem.mutex);  
  return n;
}

/*
** Change the alarm callback
*/
int sqlite3_memory_alarm(
  void(*xCallback)(void *pArg, sqlite3_uint64 used, unsigned int N),
  void *pArg,
  sqlite3_uint64 iThreshold
){
  if( mem.mutex==0 ){
    mem.mutex = sqlite3_mutex_alloc(SQLITE_MUTEX_STATIC_MEM);
  }
  sqlite3_mutex_enter(mem.mutex);
  mem.alarmCallback = xCallback;
  mem.alarmArg = pArg;
  mem.alarmThreshold = iThreshold;
  sqlite3_mutex_leave(mem.mutex);
  return SQLITE_OK;
}

/*
** Trigger the alarm 
*/
static void sqlite3MemsysAlarm(unsigned nByte){
  void (*xCallback)(void*,sqlite3_uint64,unsigned);
  sqlite3_uint64 nowUsed;
  void *pArg;
  if( mem.alarmCallback==0 || mem.alarmBusy  ) return;
  mem.alarmBusy = 1;
  xCallback = mem.alarmCallback;
  nowUsed = mem.nowUsed;
  pArg = mem.alarmArg;
  sqlite3_mutex_leave(mem.mutex);
  xCallback(pArg, nowUsed, nByte);
  sqlite3_mutex_enter(mem.mutex);
  mem.alarmBusy = 0;
}

/*
** Given an allocation, find the MemBlockHdr for that allocation.
**
** This routine checks the guards at either end of the allocation and
** if they are incorrect it asserts.
*/
static struct MemBlockHdr *sqlite3MemsysGetHeader(void *pAllocation){
  struct MemBlockHdr *p;
  unsigned int *pInt;

  p = (struct MemBlockHdr*)pAllocation;
  p--;
  assert( p->iForeGuard==FOREGUARD );
  assert( (p->iSize & 3)==0 );
  pInt = (unsigned int*)pAllocation;
  assert( pInt[p->iSize/sizeof(unsigned int)]==REARGUARD );
  return p;
}

/*
** This routine is called once the first time a simulated memory
** failure occurs.  The sole purpose of this routine is to provide
** a convenient place to set a debugger breakpoint when debugging
** errors related to malloc() failures.
*/
static void sqlite3MemsysFailed(void){
  mem.iFailCnt = 0;
}

/*
** Allocate nByte bytes of memory.
*/
void *sqlite3_malloc(int nByte){
  struct MemBlockHdr *pHdr;
  void **pBt;
  unsigned int *pInt;
  void *p;
  unsigned int totalSize;

  if( nByte<=0 ){
    return 0;
  }
  if( mem.mutex==0 ){
    mem.mutex = sqlite3_mutex_alloc(SQLITE_MUTEX_STATIC_MEM);
  }
  sqlite3_mutex_enter(mem.mutex);
  assert( mem.disallow==0 );
  if( mem.nowUsed+nByte>=mem.alarmThreshold ){
    sqlite3MemsysAlarm(nByte);
  }
  nByte = (nByte+3)&~3;
  totalSize = nByte + sizeof(*pHdr) + sizeof(unsigned int) +
               mem.nBacktrace*sizeof(void*);
  if( mem.iFail>0 ){
    if( mem.iFail==1 ){
      p = 0;
      mem.iFail = mem.iReset;
      if( mem.iFailCnt==0 ){
        sqlite3MemsysFailed();  /* A place to set a breakpoint */
      }
      mem.iFailCnt++;
    }else{
      p = malloc(totalSize);
      mem.iFail--;
    }
  }else{
    p = malloc(totalSize);
    if( p==0 ){
      sqlite3MemsysAlarm(nByte);
      p = malloc(totalSize);
    }
  }
  if( p ){
    pBt = p;
    pHdr = (struct MemBlockHdr*)&pBt[mem.nBacktrace];
    pHdr->pNext = 0;
    pHdr->pPrev = mem.pLast;
    if( mem.pLast ){
      mem.pLast->pNext = pHdr;
    }else{
      mem.pFirst = pHdr;
    }
    mem.pLast = pHdr;
    pHdr->iForeGuard = FOREGUARD;
    pHdr->nBacktraceSlots = mem.nBacktrace;
    if( mem.nBacktrace ){
      void *aAddr[40];
      pHdr->nBacktrace = backtrace(aAddr, mem.nBacktrace+1)-1;
      memcpy(pBt, &aAddr[1], pHdr->nBacktrace*sizeof(void*));
    }else{
      pHdr->nBacktrace = 0;
    }
    pHdr->iSize = nByte;
    pInt = (unsigned int *)&pHdr[1];
    pInt[nByte/sizeof(unsigned int)] = REARGUARD;
    memset(pInt, 0x65, nByte);
    mem.nowUsed += nByte;
    if( mem.nowUsed>mem.mxUsed ){
      mem.mxUsed = mem.nowUsed;
    }
    p = (void*)pInt;
  }
  sqlite3_mutex_leave(mem.mutex);
  return p; 
}

/*
** Free memory.
*/
void sqlite3_free(void *pPrior){
  struct MemBlockHdr *pHdr;
  void **pBt;
  if( pPrior==0 ){
    return;
  }
  assert( mem.mutex!=0 );
  pHdr = sqlite3MemsysGetHeader(pPrior);
  pBt = (void**)pHdr;
  pBt -= pHdr->nBacktraceSlots;
  sqlite3_mutex_enter(mem.mutex);
  mem.nowUsed -= pHdr->iSize;
  if( pHdr->pPrev ){
    assert( pHdr->pPrev->pNext==pHdr );
    pHdr->pPrev->pNext = pHdr->pNext;
  }else{
    assert( mem.pFirst==pHdr );
    mem.pFirst = pHdr->pNext;
  }
  if( pHdr->pNext ){
    assert( pHdr->pNext->pPrev==pHdr );
    pHdr->pNext->pPrev = pHdr->pPrev;
  }else{
    assert( mem.pLast==pHdr );
    mem.pLast = pHdr->pPrev;
  }
  memset(pBt, 0x2b, sizeof(void*)*pHdr->nBacktrace + sizeof(*pHdr) +
                    pHdr->iSize + sizeof(unsigned int));
  free(pBt);
  sqlite3_mutex_leave(mem.mutex);  
}

/*
** Change the size of an existing memory allocation.
**
** For this debugging implementation, we *always* make a copy of the
** allocation into a new place in memory.  In this way, if the 
** higher level code is using pointer to the old allocation, it is 
** much more likely to break and we are much more liking to find
** the error.
*/
void *sqlite3_realloc(void *pPrior, int nByte){
  struct MemBlockHdr *pOldHdr;
  void *pNew;
  if( pPrior==0 ){
    return sqlite3_malloc(nByte);
  }
  if( nByte<=0 ){
    sqlite3_free(pPrior);
    return 0;
  }
  assert( mem.disallow==0 );
  pOldHdr = sqlite3MemsysGetHeader(pPrior);
  pNew = sqlite3_malloc(nByte);
  if( pNew ){
    memcpy(pNew, pPrior, nByte<pOldHdr->iSize ? nByte : pOldHdr->iSize);
    if( nByte>pOldHdr->iSize ){
      memset(&((char*)pNew)[pOldHdr->iSize], 0x2b, nByte - pOldHdr->iSize);
    }
    sqlite3_free(pPrior);
  }
  return pNew;
}

/*
** Set the number of backtrace levels kept for each allocation.
** A value of zero turns of backtracing.  The number is always rounded
** up to a multiple of 2.
*/
void sqlite3_memdebug_backtrace(int depth){
  if( depth<0 ){ depth = 0; }
  if( depth>20 ){ depth = 20; }
  depth = (depth+1)&0xfe;
  mem.nBacktrace = depth;
}

/*
** Open the file indicated and write a log of all unfreed memory 
** allocations into that log.
*/
void sqlite3_memdebug_dump(const char *zFilename){
  FILE *out;
  struct MemBlockHdr *pHdr;
  void **pBt;
  out = fopen(zFilename, "w");
  if( out==0 ){
    fprintf(stderr, "** Unable to output memory debug output log: %s **\n",
                    zFilename);
    return;
  }
  for(pHdr=mem.pFirst; pHdr; pHdr=pHdr->pNext){
    fprintf(out, "**** %d bytes at %p ****\n", pHdr->iSize, &pHdr[1]);
    if( pHdr->nBacktrace ){
      fflush(out);
      pBt = (void**)pHdr;
      pBt -= pHdr->nBacktraceSlots;
      backtrace_symbols_fd(pBt, pHdr->nBacktrace, fileno(out));
      fprintf(out, "\n");
    }
  }
  fclose(out);
}

/*
** This routine is used to simulate malloc failures.
**
** After calling this routine, there will be iFail successful
** memory allocations and then a failure.  If iRepeat is 1
** all subsequent memory allocations will fail.  If iRepeat is
** 0, only a single allocation will fail.  If iRepeat is negative
** then the previous setting for iRepeat is unchanged.
**
** Each call to this routine overrides the previous.  To disable
** the simulated allocation failure mechanism, set iFail to -1.
**
** This routine returns the number of simulated failures that have
** occurred since the previous call.
*/
int sqlite3_memdebug_fail(int iFail, int iRepeat){
  int n = mem.iFailCnt;
  mem.iFail = iFail+1;
  if( iRepeat>=0 ){
    mem.iReset = iRepeat;
  }
  mem.iFailCnt = 0;
  return n;
}

/*
** This routine returns the number of successful mallocs remaining until
** the next simulated malloc failure.  -1 is returned if no simulated
** failure is currently scheduled.
*/
int sqlite3_memdebug_pending(void){
  return mem.iFail-1;
}

/*
** The following two routines are used to assert that no memory
** allocations occur between one call and the next.  The use of
** these routines does not change the computed results in any way.
** These routines are like asserts.
*/
void sqlite3MallocDisallow(void){
  assert( mem.mutex!=0 );
  sqlite3_mutex_enter(mem.mutex);
  mem.disallow++;
  sqlite3_mutex_leave(mem.mutex);
}
void sqlite3MallocAllow(void){
  assert( mem.mutex );
  sqlite3_mutex_enter(mem.mutex);
  assert( mem.disallow>0 );
  mem.disallow--;
  sqlite3_mutex_leave(mem.mutex);
}

#endif /* SQLITE_MEMDEBUG && !SQLITE_OMIT_MEMORY_ALLOCATION */

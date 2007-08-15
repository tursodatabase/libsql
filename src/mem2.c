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
** $Id: mem2.c,v 1.2 2007/08/15 19:16:43 drh Exp $
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
** Mutex to control access to the memory allocation subsystem.
*/
static sqlite3_mutex *memMutex = 0;

/*
** Current allocation and high-water mark.
*/
static sqlite3_uint64 nowUsed = 0;
static sqlite3_uint64 mxUsed = 0;

/*
** The alarm callback and its arguments.  The memMutex lock will
** be held while the callback is running.  Recursive calls into
** the memory subsystem are allowed, but no new callbacks will be
** issued.  The alarmBusy variable is set to prevent recursive
** callbacks.
*/
static void (*alarmCallback)(void*, sqlite3_uint64, unsigned) = 0;
static void *alarmArg = 0;
static sqlite3_uint64 alarmThreshold = (((sqlite3_uint64)1)<<63);
static int alarmBusy = 0;


/*
** Return the amount of memory currently checked out.
*/
sqlite3_uint64 sqlite3_memory_used(void){
  sqlite3_uint64 n;
  if( memMutex==0 ){
    memMutex = sqlite3_mutex_alloc(1);
  }
  sqlite3_mutex_enter(memMutex, 1);
  n = nowUsed;
  sqlite3_mutex_leave(memMutex);  
  return n;
}

/*
** Return the maximum amount of memory that has ever been
** checked out since either the beginning of this process
** or since the most recent reset.
*/
sqlite3_uint64 sqlite3_memory_highwater(int resetFlag){
  sqlite3_uint64 n;
  if( memMutex==0 ){
    memMutex = sqlite3_mutex_alloc(1);
  }
  sqlite3_mutex_enter(memMutex, 1);
  n = mxUsed;
  if( resetFlag ){
    mxUsed = nowUsed;
  }
  sqlite3_mutex_leave(memMutex);  
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
  if( memMutex==0 ){
    memMutex = sqlite3_mutex_alloc(1);
  }
  sqlite3_mutex_enter(memMutex, 1);
  alarmCallback = xCallback;
  alarmArg = pArg;
  alarmThreshold = iThreshold;
  sqlite3_mutex_leave(memMutex);
  return SQLITE_OK;
}

/*
** Trigger the alarm 
*/
static void sqlite3MemsysAlarm(unsigned nByte){
  if( alarmCallback==0 || alarmBusy  ) return;
  alarmBusy = 1;
  alarmCallback(alarmArg, nowUsed, nByte);
  alarmBusy = 0;
}

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
** Head and tail of a linked list of all outstanding allocations
*/
static struct MemBlockHdr *pFirst = 0;
static struct MemBlockHdr *pLast = 0;

/*
** The number of levels of backtrace to save in new allocations.
*/
static int backtraceLevels = 0;

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
** Allocate nByte of memory
*/
void *sqlite3_malloc(unsigned int nByte){
  struct MemBlockHdr *pHdr;
  void **pBt;
  unsigned int *pInt;
  void *p;
  unsigned int totalSize;

  if( memMutex==0 ){
    memMutex = sqlite3_mutex_alloc(1);
  }
  sqlite3_mutex_enter(memMutex, 1);
  if( nowUsed+nByte>=alarmThreshold ){
    sqlite3MemsysAlarm(nByte);
  }
  nByte = (nByte+3)&~3;
  totalSize = nByte + sizeof(*pHdr) + sizeof(unsigned int) +
               backtraceLevels*sizeof(void*);
  p = malloc(totalSize);
  if( p==0 ){
    sqlite3MemsysAlarm(nByte);
    p = malloc(totalSize);
  }
  if( p ){
    pBt = p;
    pHdr = (struct MemBlockHdr*)&pBt[backtraceLevels];
    pHdr->pNext = 0;
    pHdr->pPrev = pLast;
    if( pLast ){
      pLast->pNext = pHdr;
    }else{
      pFirst = pHdr;
    }
    pLast = pHdr;
    pHdr->iForeGuard = FOREGUARD;
    pHdr->nBacktraceSlots = backtraceLevels;
    if( backtraceLevels ){
      void *aAddr[40];
      pHdr->nBacktrace = backtrace(aAddr, backtraceLevels+1)-1;
      memcpy(pBt, &aAddr[1], pHdr->nBacktrace*sizeof(void*));
    }else{
      pHdr->nBacktrace = 0;
    }
    pHdr->iSize = nByte;
    pInt = (unsigned int *)&pHdr[1];
    pInt[nByte/sizeof(unsigned int)] = REARGUARD;
    memset(pInt, 0x65, nByte);
    nowUsed += nByte;
    if( nowUsed>mxUsed ){
      mxUsed = nowUsed;
    }
    p = (void*)pInt;
  }
  sqlite3_mutex_leave(memMutex);
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
  assert( memMutex!=0 );
  pHdr = sqlite3MemsysGetHeader(pPrior);
  pBt = (void**)pHdr;
  pBt -= pHdr->nBacktraceSlots;
  sqlite3_mutex_enter(memMutex, 1);
  nowUsed -= pHdr->iSize;
  if( pHdr->pPrev ){
    assert( pHdr->pPrev->pNext==pHdr );
    pHdr->pPrev->pNext = pHdr->pNext;
  }else{
    assert( pFirst==pHdr );
    pFirst = pHdr->pNext;
  }
  if( pHdr->pNext ){
    assert( pHdr->pNext->pPrev==pHdr );
    pHdr->pNext->pPrev = pHdr->pPrev;
  }else{
    assert( pLast==pHdr );
    pLast = pHdr->pPrev;
  }
  memset(pBt, 0x2b, sizeof(void*)*pHdr->nBacktrace + sizeof(*pHdr) +
                    pHdr->iSize + sizeof(unsigned int));
  free(pBt);
  sqlite3_mutex_leave(memMutex);  
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
void *sqlite3_realloc(void *pPrior, unsigned int nByte){
  struct MemBlockHdr *pOldHdr;
  void *pNew;
  if( pPrior==0 ){
    return sqlite3_malloc(nByte);
  }
  if( nByte==0 ){
    sqlite3_free(pPrior);
    return 0;
  }
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
  backtraceLevels = depth;
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
  for(pHdr=pFirst; pHdr; pHdr=pHdr->pNext){
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

#endif /* SQLITE_MEMDEBUG && !SQLITE_OMIT_MEMORY_ALLOCATION */

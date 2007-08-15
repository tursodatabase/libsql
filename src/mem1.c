/*
** 2007 August 14
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
** $Id: mem1.c,v 1.2 2007/08/15 17:07:57 drh Exp $
*/

/*
** This version of the memory allocator is the default.  It is
** used when no other memory allocator is specified using compile-time
** macros.
*/
#if !defined(SQLITE_MEMDEBUG) && !defined(SQLITE_OMIT_MEMORY_ALLOCATION)

/*
** We will eventually construct multiple memory allocation subsystems
** suitable for use in various contexts:
**
**    *  Normal multi-threaded builds
**    *  Normal single-threaded builds
**    *  Debugging builds
**
** This initial version is suitable for use in normal multi-threaded
** builds.  We envision that alternative versions will be stored in
** separate source files.  #ifdefs will be used to select the code from
** one of the various memN.c source files for use in any given build.
*/
#include "sqliteInt.h"


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
** Allocate nBytes of memory
*/
void *sqlite3_malloc(unsigned int nBytes){
  sqlite3_uint64 *p;
  if( memMutex==0 ){
    memMutex = sqlite3_mutex_alloc(1);
  }
  sqlite3_mutex_enter(memMutex, 1);
  if( nowUsed+nBytes>=alarmThreshold ){
    sqlite3MemsysAlarm(nBytes);
  }
  p = malloc(nBytes+8);
  if( p==0 ){
    sqlite3MemsysAlarm(nBytes);
    p = malloc(nBytes+8);
  }
  if( p ){
    p[0] = nBytes;
    p++;
    nowUsed += nBytes;
    if( nowUsed>mxUsed ){
      mxUsed = nowUsed;
    }
  }
  sqlite3_mutex_leave(memMutex);
  return (void*)p; 
}

/*
** Free memory.
*/
void sqlite3_free(void *pPrior){
  sqlite3_uint64 *p;
  unsigned nByte;
  if( pPrior==0 ){
    return;
  }
  assert( memMutex!=0 );
  p = pPrior;
  p--;
  nByte = (unsigned int)*p;
  sqlite3_mutex_enter(memMutex, 1);
  nowUsed -= nByte;
  free(p);
  sqlite3_mutex_leave(memMutex);  
}

/*
** Change the size of an existing memory allocation
*/
void *sqlite3_realloc(void *pPrior, unsigned int nBytes){
  unsigned nOld;
  sqlite3_uint64 *p;
  if( pPrior==0 ){
    return sqlite3_malloc(nBytes);
  }
  if( nBytes==0 ){
    sqlite3_free(pPrior);
    return;
  }
  p = pPrior;
  p--;
  nOld = (unsigned int)p[0];
  assert( memMutex!=0 );
  sqlite3_mutex_enter(memMutex, 1);
  if( nowUsed+nBytes-nOld>=alarmThreshold ){
    sqlite3MemsysAlarm(nBytes-nOld);
  }
  p = realloc(p, nBytes+8);
  if( p==0 ){
    sqlite3MemsysAlarm(nBytes);
    p = realloc(p, nBytes+8);
  }
  if( p ){
    p[0] = nBytes;
    p++;
    nowUsed += nBytes-nOld;
    if( nowUsed>mxUsed ){
      mxUsed = nowUsed;
    }
  }
  sqlite3_mutex_leave(memMutex);
  return (void*)p;
}

#endif /* !SQLITE_MEMDEBUG && !SQLITE_OMIT_MEMORY_ALLOCATION */

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
** $Id: mem1.c,v 1.7 2007/08/22 20:18:22 drh Exp $
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
** Allocate nBytes of memory
*/
void *sqlite3_malloc(int nBytes){
  sqlite3_uint64 *p;
  if( nBytes<=0 ){
    return 0;
  }
  if( mem.mutex==0 ){
    mem.mutex = sqlite3_mutex_alloc(SQLITE_MUTEX_STATIC_MEM);
  }
  sqlite3_mutex_enter(mem.mutex);
  if( mem.nowUsed+nBytes>=mem.alarmThreshold ){
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
    mem.nowUsed += nBytes;
    if( mem.nowUsed>mem.mxUsed ){
      mem.mxUsed = mem.nowUsed;
    }
  }
  sqlite3_mutex_leave(mem.mutex);
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
  assert( mem.mutex!=0 );
  p = pPrior;
  p--;
  nByte = (unsigned int)*p;
  sqlite3_mutex_enter(mem.mutex);
  mem.nowUsed -= nByte;
  free(p);
  sqlite3_mutex_leave(mem.mutex);  
}

/*
** Change the size of an existing memory allocation
*/
void *sqlite3_realloc(void *pPrior, int nBytes){
  unsigned nOld;
  sqlite3_uint64 *p;
  if( pPrior==0 ){
    return sqlite3_malloc(nBytes);
  }
  if( nBytes<=0 ){
    sqlite3_free(pPrior);
    return 0;
  }
  p = pPrior;
  p--;
  nOld = (unsigned int)p[0];
  assert( mem.mutex!=0 );
  sqlite3_mutex_enter(mem.mutex);
  if( mem.nowUsed+nBytes-nOld>=mem.alarmThreshold ){
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
    mem.nowUsed += nBytes-nOld;
    if( mem.nowUsed>mem.mxUsed ){
      mem.mxUsed = mem.nowUsed;
    }
  }
  sqlite3_mutex_leave(mem.mutex);
  return (void*)p;
}

#endif /* !SQLITE_MEMDEBUG && !SQLITE_OMIT_MEMORY_ALLOCATION */

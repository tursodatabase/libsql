/*
** 2007 August 28
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
*************************************************************************
** This file contains the C functions that implement mutexes for OS/2
*/
#include "sqliteInt.h"

/*
** The code in this file is only used if SQLITE_MUTEX_OS2 is defined.
** See the mutex.h file for details.
*/
#ifdef SQLITE_MUTEX_OS2

/********************** OS/2 Mutex Implementation **********************
**
** This implementation of mutexes is built using the OS/2 API.
*/

/*
** The mutex object
** Each recursive mutex is an instance of the following structure.
*/
struct sqlite3_mutex {
  HMTX mutex;       /* Mutex controlling the lock */
  int  id;          /* Mutex type */
#ifdef SQLITE_DEBUG
 int   trace;       /* True to trace changes */
#endif
};

#ifdef SQLITE_DEBUG
#define SQLITE3_MUTEX_INITIALIZER { 0, 0, 0 }
#else
#define SQLITE3_MUTEX_INITIALIZER { 0, 0 }
#endif

/*
** Initialize and deinitialize the mutex subsystem.
*/
static int os2MutexInit(void){ return SQLITE_OK; }
static int os2MutexEnd(void){ return SQLITE_OK; }

/*
** The sqlite3_mutex_alloc() routine allocates a new
** mutex and returns a pointer to it.  If it returns NULL
** that means that a mutex could not be allocated. 
** SQLite will unwind its stack and return an error.  The argument
** to sqlite3_mutex_alloc() is one of these integer constants:
**
** <ul>
** <li>  SQLITE_MUTEX_FAST
** <li>  SQLITE_MUTEX_RECURSIVE
** <li>  SQLITE_MUTEX_STATIC_MASTER
** <li>  SQLITE_MUTEX_STATIC_MEM
** <li>  SQLITE_MUTEX_STATIC_MEM2
** <li>  SQLITE_MUTEX_STATIC_PRNG
** <li>  SQLITE_MUTEX_STATIC_LRU
** <li>  SQLITE_MUTEX_STATIC_LRU2
** </ul>
**
** The first two constants cause sqlite3_mutex_alloc() to create
** a new mutex.  The new mutex is recursive when SQLITE_MUTEX_RECURSIVE
** is used but not necessarily so when SQLITE_MUTEX_FAST is used.
** The mutex implementation does not need to make a distinction
** between SQLITE_MUTEX_RECURSIVE and SQLITE_MUTEX_FAST if it does
** not want to.  But SQLite will only request a recursive mutex in
** cases where it really needs one.  If a faster non-recursive mutex
** implementation is available on the host platform, the mutex subsystem
** might return such a mutex in response to SQLITE_MUTEX_FAST.
**
** The other allowed parameters to sqlite3_mutex_alloc() each return
** a pointer to a static preexisting mutex.  Six static mutexes are
** used by the current version of SQLite.  Future versions of SQLite
** may add additional static mutexes.  Static mutexes are for internal
** use by SQLite only.  Applications that use SQLite mutexes should
** use only the dynamic mutexes returned by SQLITE_MUTEX_FAST or
** SQLITE_MUTEX_RECURSIVE.
**
** Note that if one of the dynamic mutex parameters (SQLITE_MUTEX_FAST
** or SQLITE_MUTEX_RECURSIVE) is used then sqlite3_mutex_alloc()
** returns a different mutex on every call.  But for the static
** mutex types, the same mutex is returned on every call that has
** the same type number.
*/
static sqlite3_mutex *os2MutexAlloc(int iType){
  sqlite3_mutex *p = NULL;
  switch( iType ){
    case SQLITE_MUTEX_FAST:
    case SQLITE_MUTEX_RECURSIVE: {
      p = sqlite3MallocZero( sizeof(*p) );
      if( p ){
        p->id = iType;
        if( DosCreateMutexSem( 0, &p->mutex, 0, FALSE ) != NO_ERROR ){
          sqlite3_free( p );
          p = NULL;
        }
      }
      break;
    }
    default: {
      static volatile int isInit = 0;
      static sqlite3_mutex staticMutexes[6] = {
        SQLITE3_MUTEX_INITIALIZER,
        SQLITE3_MUTEX_INITIALIZER,
        SQLITE3_MUTEX_INITIALIZER,
        SQLITE3_MUTEX_INITIALIZER,
        SQLITE3_MUTEX_INITIALIZER,
        SQLITE3_MUTEX_INITIALIZER,
      };
      if ( !isInit ){
        APIRET rc;
        PTIB ptib;
        PPIB ppib;
        HMTX mutex;
        char name[32];
        DosGetInfoBlocks( &ptib, &ppib );
        sqlite3_snprintf( sizeof(name), name, "\\SEM32\\SQLITE%04x",
                          ppib->pib_ulpid );
        while( !isInit ){
          mutex = 0;
          rc = DosCreateMutexSem( name, &mutex, 0, FALSE);
          if( rc == NO_ERROR ){
            unsigned int i;
            if( !isInit ){
              for( i = 0; i < sizeof(staticMutexes)/sizeof(staticMutexes[0]); i++ ){
                DosCreateMutexSem( 0, &staticMutexes[i].mutex, 0, FALSE );
              }
              isInit = 1;
            }
            DosCloseMutexSem( mutex );
          }else if( rc == ERROR_DUPLICATE_NAME ){
            DosSleep( 1 );
          }else{
            return p;
          }
        }
      }
      assert( iType-2 >= 0 );
      assert( iType-2 < sizeof(staticMutexes)/sizeof(staticMutexes[0]) );
      p = &staticMutexes[iType-2];
      p->id = iType;
      break;
    }
  }
  return p;
}


/*
** This routine deallocates a previously allocated mutex.
** SQLite is careful to deallocate every mutex that it allocates.
*/
static void os2MutexFree(sqlite3_mutex *p){
#ifdef SQLITE_DEBUG
  TID tid;
  PID pid;
  ULONG ulCount;
  DosQueryMutexSem(p->mutex, &pid, &tid, &ulCount);
  assert( ulCount==0 );
  assert( p->id==SQLITE_MUTEX_FAST || p->id==SQLITE_MUTEX_RECURSIVE );
#endif
  DosCloseMutexSem( p->mutex );
  sqlite3_free( p );
}

#ifdef SQLITE_DEBUG
/*
** The sqlite3_mutex_held() and sqlite3_mutex_notheld() routine are
** intended for use inside assert() statements.
*/
static int os2MutexHeld(sqlite3_mutex *p){
  TID tid;
  PID pid;
  ULONG ulCount;
  PTIB ptib;
  DosQueryMutexSem(p->mutex, &pid, &tid, &ulCount);
  if( ulCount==0 || ( ulCount>1 && p->id!=SQLITE_MUTEX_RECURSIVE ) )
    return 0;
  DosGetInfoBlocks(&ptib, NULL);
  return tid==ptib->tib_ptib2->tib2_ultid;
}
static int os2MutexNotheld(sqlite3_mutex *p){
  TID tid;
  PID pid;
  ULONG ulCount;
  PTIB ptib;
  DosQueryMutexSem(p->mutex, &pid, &tid, &ulCount);
  if( ulCount==0 )
    return 1;
  DosGetInfoBlocks(&ptib, NULL);
  return tid!=ptib->tib_ptib2->tib2_ultid;
}
static void os2MutexTrace(sqlite3_mutex *p, char *pAction){
  TID   tid;
  PID   pid;
  ULONG ulCount;
  DosQueryMutexSem(p->mutex, &pid, &tid, &ulCount);
  printf("%s mutex %p (%d) with nRef=%ld\n", pAction, (void*)p, p->trace, ulCount);
}
#endif

/*
** The sqlite3_mutex_enter() and sqlite3_mutex_try() routines attempt
** to enter a mutex.  If another thread is already within the mutex,
** sqlite3_mutex_enter() will block and sqlite3_mutex_try() will return
** SQLITE_BUSY.  The sqlite3_mutex_try() interface returns SQLITE_OK
** upon successful entry.  Mutexes created using SQLITE_MUTEX_RECURSIVE can
** be entered multiple times by the same thread.  In such cases the,
** mutex must be exited an equal number of times before another thread
** can enter.  If the same thread tries to enter any other kind of mutex
** more than once, the behavior is undefined.
*/
static void os2MutexEnter(sqlite3_mutex *p){
  assert( p->id==SQLITE_MUTEX_RECURSIVE || os2MutexNotheld(p) );
  DosRequestMutexSem(p->mutex, SEM_INDEFINITE_WAIT);
#ifdef SQLITE_DEBUG
  if( p->trace ) os2MutexTrace(p, "enter");
#endif
}
static int os2MutexTry(sqlite3_mutex *p){
  int rc = SQLITE_BUSY;
  assert( p->id==SQLITE_MUTEX_RECURSIVE || os2MutexNotheld(p) );
  if( DosRequestMutexSem(p->mutex, SEM_IMMEDIATE_RETURN) == NO_ERROR ) {
    rc = SQLITE_OK;
#ifdef SQLITE_DEBUG
    if( p->trace ) os2MutexTrace(p, "try");
#endif
  }
  return rc;
}

/*
** The sqlite3_mutex_leave() routine exits a mutex that was
** previously entered by the same thread.  The behavior
** is undefined if the mutex is not currently entered or
** is not currently allocated.  SQLite will never do either.
*/
static void os2MutexLeave(sqlite3_mutex *p){
  assert( os2MutexHeld(p) );
  DosReleaseMutexSem(p->mutex);
#ifdef SQLITE_DEBUG
  if( p->trace ) os2MutexTrace(p, "leave");
#endif
}

sqlite3_mutex_methods const *sqlite3DefaultMutex(void){
  static const sqlite3_mutex_methods sMutex = {
    os2MutexInit,
    os2MutexEnd,
    os2MutexAlloc,
    os2MutexFree,
    os2MutexEnter,
    os2MutexTry,
    os2MutexLeave,
#ifdef SQLITE_DEBUG
    os2MutexHeld,
    os2MutexNotheld
#else
    0,
    0
#endif
  };

  return &sMutex;
}
#endif /* SQLITE_MUTEX_OS2 */

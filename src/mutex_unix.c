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
** This file contains the C functions that implement mutexes for pthreads
**
** $Id: mutex_unix.c,v 1.2 2007/08/28 22:24:35 drh Exp $
*/
#include "sqliteInt.h"

/*
** The code in this file is only used if we are compiling threadsafe
** under unix with pthreads.
**
** Note that this implementation requires a version of pthreads that
** supports recursive mutexes.
*/
#ifdef SQLITE_MUTEX_PTHREADS

#include <pthread.h>

/*
** Each recursive mutex is an instance of the following structure.
*/
struct sqlite3_mutex {
  pthread_mutex_t mutex;     /* Mutex controlling the lock */
  int id;                    /* Mutex type */
  int nRef;                  /* Number of entrances */
  pthread_t owner;           /* Thread that is within this mutex */
#ifdef SQLITE_DEBUG
  int trace;                 /* True to trace changes */
#endif
};

/*
** The sqlite3_mutex_alloc() routine allocates a new
** mutex and returns a pointer to it.  If it returns NULL
** that means that a mutex could not be allocated.  SQLite
** will unwind its stack and return an error.  The argument
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
** a pointer to a static preexisting mutex.  Three static mutexes are
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
sqlite3_mutex *sqlite3_mutex_alloc(int iType){
  static sqlite3_mutex staticMutexes[] = {
    { PTHREAD_MUTEX_INITIALIZER, },
    { PTHREAD_MUTEX_INITIALIZER, },
    { PTHREAD_MUTEX_INITIALIZER, },
    { PTHREAD_MUTEX_INITIALIZER, },
    { PTHREAD_MUTEX_INITIALIZER, },
  };
  sqlite3_mutex *p;
  switch( iType ){
    case SQLITE_MUTEX_RECURSIVE: {
      p = sqlite3MallocZero( sizeof(*p) );
      if( p ){
        pthread_mutexattr_t recursiveAttr;
        pthread_mutexattr_init(&recursiveAttr);
        pthread_mutexattr_settype(&recursiveAttr, PTHREAD_MUTEX_RECURSIVE);
        pthread_mutex_init(&p->mutex, &recursiveAttr);
        pthread_mutexattr_destroy(&recursiveAttr);
        p->id = iType;
      }
      break;
    }
    case SQLITE_MUTEX_FAST: {
      p = sqlite3MallocZero( sizeof(*p) );
      if( p ){
        p->id = iType;
        pthread_mutex_init(&p->mutex, 0);
      }
      break;
    }
    default: {
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
** This routine deallocates a previously
** allocated mutex.  SQLite is careful to deallocate every
** mutex that it allocates.
*/
void sqlite3_mutex_free(sqlite3_mutex *p){
  assert( p );
  assert( p->nRef==0 );
  assert( p->id==SQLITE_MUTEX_FAST || p->id==SQLITE_MUTEX_RECURSIVE );
  pthread_mutex_destroy(&p->mutex);
  sqlite3_free(p);
}

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
void sqlite3_mutex_enter(sqlite3_mutex *p){
  assert( p );
  assert( p->id==SQLITE_MUTEX_RECURSIVE || sqlite3_mutex_notheld(p) );
  pthread_mutex_lock(&p->mutex);
  p->owner = pthread_self();
  p->nRef++;
#ifdef SQLITE_DEBUG
  if( p->trace ){
    printf("enter mutex %p (%d) with nRef=%d\n", p, p->trace, p->nRef);
  }
#endif
}
int sqlite3_mutex_try(sqlite3_mutex *p){
  int rc;
  assert( p );
  assert( p->id==SQLITE_MUTEX_RECURSIVE || sqlite3_mutex_notheld(p) );
  if( pthread_mutex_trylock(&p->mutex)==0 ){
    p->owner = pthread_self();
    p->nRef++;
    rc = SQLITE_OK;
#ifdef SQLITE_DEBUG
    if( p->trace ){
      printf("enter mutex %p (%d) with nRef=%d\n", p, p->trace, p->nRef);
    }
#endif
  }else{
    rc = SQLITE_BUSY;
  }
  return rc;
}

/*
** The sqlite3_mutex_leave() routine exits a mutex that was
** previously entered by the same thread.  The behavior
** is undefined if the mutex is not currently entered or
** is not currently allocated.  SQLite will never do either.
*/
void sqlite3_mutex_leave(sqlite3_mutex *p){
  assert( p );
  assert( sqlite3_mutex_held(p) );
  p->nRef--;
  assert( p->nRef==0 || p->id==SQLITE_MUTEX_RECURSIVE );
#ifdef SQLITE_DEBUG
  if( p->trace ){
    printf("leave mutex %p (%d) with nRef=%d\n", p, p->trace, p->nRef);
  }
#endif
  pthread_mutex_unlock(&p->mutex);
}

/*
** The sqlite3_mutex_held() and sqlite3_mutex_notheld() routine are
** intended for use only inside assert() statements.  On some platforms,
** there might be race conditions that can cause these routines to
** deliver incorrect results.  In particular, if pthread_equal() is
** not an atomic operation, then these routines might delivery
** incorrect results.  On most platforms, pthread_equal() is a 
** comparison of two integers and is therefore atomic.  But we are
** told that HPUX is not such a platform.  If so, then these routines
** will not always work correctly on HPUX.
**
** On those platforms where pthread_equal() is not atomic, SQLite
** should be compiled without -DSQLITE_DEBUG and with -DNDEBUG to
** make sure no assert() statements are evaluated and hence these
** routines are never called.
*/
#ifndef NDEBUG
int sqlite3_mutex_held(sqlite3_mutex *p){
  return p==0 || (p->nRef!=0 && pthread_equal(p->owner, pthread_self()));
}
int sqlite3_mutex_notheld(sqlite3_mutex *p){
  return p==0 || p->nRef==0 || pthread_equal(p->owner, pthread_self())==0;
}
#endif
#endif /* SQLITE_MUTEX_PTHREAD */

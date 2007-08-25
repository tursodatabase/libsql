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
** This file contains the C functions that implement mutexes for
** use by the SQLite core.
**
** $Id: mutex.c,v 1.12 2007/08/25 16:21:30 drh Exp $
*/
/*
** If SQLITE_MUTEX_APPDEF is defined, then this whole module is
** omitted and equivalent functionality must be provided by the
** application that links against the SQLite library.
*/
#ifndef SQLITE_MUTEX_APPDEF


/* This is the beginning of internal implementation of mutexes
** for SQLite.
*/
#include "sqliteInt.h"

/*
** Figure out what version of the code to use.  The choices are
**
**   SQLITE_MUTEX_NOOP         For single-threaded applications that
**                             do not desire error checking.
**
**   SQLITE_MUTEX_NOOP_DEBUG   For single-threaded applications with
**                             error checking to help verify that mutexes
**                             are being used correctly even though they
**                             are not needed.  Used when SQLITE_DEBUG is
**                             defined on single-threaded builds.
**
**   SQLITE_MUTEX_PTHREADS     For multi-threaded applications on Unix.
**
**   SQLITE_MUTEX_WIN          For multi-threaded applications on Win32.
*/
#define SQLITE_MUTEX_NOOP 1   /* The default */
#if defined(SQLITE_DEBUG) && !SQLITE_THREADSAFE
# undef SQLITE_MUTEX_NOOP
# define SQLITE_MUTEX_NOOP_DEBUG
#endif
#if defined(SQLITE_MUTEX_NOOP) && SQLITE_THREADSAFE && OS_UNIX
# undef SQLITE_MUTEX_NOOP
# define SQLITE_MUTEX_PTHREAD
#endif
#if defined(SQLITE_MUTEX_NOOP) && SQLITE_THREADSAFE && OS_WIN
# undef SQLITE_MUTEX_NOOP
# define SQLITE_MUTEX_WIN
#endif



#ifdef SQLITE_MUTEX_NOOP
/************************ No-op Mutex Implementation **********************
**
** This first implementation of mutexes is really a no-op.  In other words,
** no real locking occurs.  This implementation is appropriate for use
** in single threaded applications which do not want the extra overhead
** of thread locking primitives.
*/

/*
** The sqlite3_mutex_alloc() routine allocates a new
** mutex and returns a pointer to it.  If it returns NULL
** that means that a mutex could not be allocated. 
*/
sqlite3_mutex *sqlite3_mutex_alloc(int idNotUsed){
  return (sqlite3_mutex*)8;
}

/*
** This routine deallocates a previously allocated mutex.
*/
void sqlite3_mutex_free(sqlite3_mutex *pNotUsed){}

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
void sqlite3_mutex_enter(sqlite3_mutex *pNotUsed){}
int sqlite3_mutex_try(sqlite3_mutex *pNotUsed){ return SQLITE_OK; }

/*
** The sqlite3_mutex_leave() routine exits a mutex that was
** previously entered by the same thread.  The behavior
** is undefined if the mutex is not currently entered or
** is not currently allocated.  SQLite will never do either.
*/
void sqlite3_mutex_leave(sqlite3_mutex *pNotUsed){}

/*
** The sqlite3_mutex_held() and sqlite3_mutex_notheld() routine are
** intended for use inside assert() statements.
*/
int sqlite3_mutex_held(sqlite3_mutex *pNotUsed){
  return 1;
}
int sqlite3_mutex_notheld(sqlite3_mutex *pNotUsed){
  return 1;
}
#endif /* SQLITE_MUTEX_NOOP */

#ifdef SQLITE_MUTEX_NOOP_DEBUG
/*************** Error-checking No-op Mutex Implementation *******************
**
** In this implementation, mutexes do not provide any mutual exclusion.
** But the error checking is provided.  This implementation is useful
** for test purposes.
*/

/*
** The mutex object
*/
struct sqlite3_mutex {
  int id;     /* The mutex type */
  int cnt;    /* Number of entries without a matching leave */
};

/*
** The sqlite3_mutex_alloc() routine allocates a new
** mutex and returns a pointer to it.  If it returns NULL
** that means that a mutex could not be allocated. 
*/
sqlite3_mutex *sqlite3_mutex_alloc(int id){
  static sqlite3_mutex aStatic[4];
  sqlite3_mutex *pNew = 0;
  switch( id ){
    case SQLITE_MUTEX_FAST:
    case SQLITE_MUTEX_RECURSIVE: {
      pNew = sqlite3_malloc(sizeof(*pNew));
      if( pNew ){
        pNew->id = id;
        pNew->cnt = 0;
      }
      break;
    }
    default: {
      assert( id-2 >= 0 );
      assert( id-2 < sizeof(aStatic)/sizeof(aStatic[0]) );
      pNew = &aStatic[id-2];
      pNew->id = id;
      break;
    }
  }
  return pNew;
}

/*
** This routine deallocates a previously allocated mutex.
*/
void sqlite3_mutex_free(sqlite3_mutex *p){
  assert( p );
  assert( p->cnt==0 );
  assert( p->id==SQLITE_MUTEX_FAST || p->id==SQLITE_MUTEX_RECURSIVE );
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
  p->cnt++;
}
int sqlite3_mutex_try(sqlite3_mutex *p){
  assert( p );
  assert( p->id==SQLITE_MUTEX_RECURSIVE || sqlite3_mutex_notheld(p) );
  p->cnt++;
  return SQLITE_OK;
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
  p->cnt--;
  assert( p->id==SQLITE_MUTEX_RECURSIVE || sqlite3_mutex_notheld(p) );
}

/*
** The sqlite3_mutex_held() and sqlite3_mutex_notheld() routine are
** intended for use inside assert() statements.
*/
int sqlite3_mutex_held(sqlite3_mutex *p){
  return p==0 || p->cnt>0;
}
int sqlite3_mutex_notheld(sqlite3_mutex *p){
  return p==0 || p->cnt==0;
}
#endif /* SQLITE_MUTEX_NOOP_DEBUG */



#ifdef SQLITE_MUTEX_PTHREAD
/******************** Pthread Mutex Implementation *********************
**
** This implementation of mutexes is built using a version of pthreads that
** has native support for recursive mutexes.
*/
#include <pthread.h>

/*
** Each recursive mutex is an instance of the following structure.
*/
struct sqlite3_mutex {
  pthread_mutex_t mutex;     /* Mutex controlling the lock */
  int id;                    /* Mutex type */
  int nRef;                  /* Number of entrances */
  pthread_t owner;           /* Thread that is within this mutex */
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
  };
  sqlite3_mutex *p;
  switch( iType ){
    case SQLITE_MUTEX_RECURSIVE: {
      static pthread_mutex_t initMutex = PTHREAD_MUTEX_INITIALIZER;
      static int isInit = 0;
      static pthread_mutexattr_t recursiveAttr;
      if( !isInit ){
        pthread_mutex_lock(&initMutex);
        if( !isInit ){
          pthread_mutexattr_init(&recursiveAttr);
          pthread_mutexattr_settype(&recursiveAttr, PTHREAD_MUTEX_RECURSIVE);
        }
        isInit = 1;
        pthread_mutex_unlock(&initMutex);
      }
      p = sqlite3MallocZero( sizeof(*p) );
      if( p ){
        p->id = iType;
        pthread_mutex_init(&p->mutex, &recursiveAttr);
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
}
int sqlite3_mutex_try(sqlite3_mutex *p){
  int rc;
  assert( p );
  assert( p->id==SQLITE_MUTEX_RECURSIVE || sqlite3_mutex_notheld(p) );
  if( pthread_mutex_trylock(&p->mutex)==0 ){
    p->owner = pthread_self();
    p->nRef++;
    rc = SQLITE_OK;
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

#ifdef SQLITE_MUTEX_WIN
/********************** Windows Mutex Implementation **********************
**
** This implementation of mutexes is built using the win32 API.
*/

/*
** Each recursive mutex is an instance of the following structure.
*/
struct sqlite3_mutex {
  CRITICAL_SECTION mutex;    /* Mutex controlling the lock */
  int id;                    /* Mutex type */
  int nRef;                  /* Number of enterances */
  DWORD owner;               /* Thread holding this mutex */
};

/*
** The sqlite3_mutex_alloc() routine allocates a new
** mutex and returns a pointer to it.  If it returns NULL
** that means that a mutex could not be allocated.  SQLite
** will unwind its stack and return an error.  The argument
** to sqlite3_mutex_alloc() is one of these integer constants:
**
** <ul>
** <li>  SQLITE_MUTEX_FAST               0
** <li>  SQLITE_MUTEX_RECURSIVE          1
** <li>  SQLITE_MUTEX_STATIC_MASTER      2
** <li>  SQLITE_MUTEX_STATIC_MEM         3
** <li>  SQLITE_MUTEX_STATIC_PRNG        4
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
  sqlite3_mutex *p;

  switch( iType ){
    case SQLITE_MUTEX_FAST:
    case SQLITE_MUTEX_RECURSIVE: {
      p = sqlite3MallocZero( sizeof(*p) );
      if( p ){
        p->id = iType;
        InitializeCriticalSection(&p->mutex);
      }
      break;
    }
    default: {
      static sqlite3_mutex staticMutexes[4];
      static int isInit = 0;
      while( !isInit ){
        static long lock = 0;
        if( InterlockedIncrement(&lock)==1 ){
          int i;
          for(i=0; i<sizeof(staticMutexes)/sizeof(staticMutexes[0]); i++){
            InitializeCriticalSection(&staticMutexes[i].mutex);
          }
          isInit = 1;
        }else{
          Sleep(1);
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
** This routine deallocates a previously
** allocated mutex.  SQLite is careful to deallocate every
** mutex that it allocates.
*/
void sqlite3_mutex_free(sqlite3_mutex *p){
  assert( p );
  assert( p->nRef==0 );
  assert( p->id==SQLITE_MUTEX_FAST || p->id==SQLITE_MUTEX_RECURSIVE );
  DeleteCriticalSection(&p->mutex);
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
  EnterCriticalSection(&p->mutex);
  p->owner = GetCurrentThreadId(); 
  p->nRef++;
}
int sqlite3_mutex_try(sqlite3_mutex *p){
  int rc;
  assert( p );
  assert( p->id==SQLITE_MUTEX_RECURSIVE || sqlite3_mutex_notheld(p) );
  if( TryEnterCriticalSection(&p->mutex) ){
    p->owner = GetCurrentThreadId();
    p->nRef++;
    rc = SQLITE_OK;
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
  assert( p->nRef>0 );
  assert( p->owner==GetCurrentThreadId() );
  p->nRef--;
  assert( p->nRef==0 || p->id==SQLITE_MUTEX_RECURSIVE );
  LeaveCriticalSection(&p->mutex);
}

/*
** The sqlite3_mutex_held() and sqlite3_mutex_notheld() routine are
** intended for use only inside assert() statements.
*/
int sqlite3_mutex_held(sqlite3_mutex *p){
  return p==0 || (p->nRef!=0 && p->owner==GetCurrentThreadId());
}
int sqlite3_mutex_notheld(sqlite3_mutex *p){
  return p==0 || p->nRef==0 || p->owner!=GetCurrentThreadId();
}
#endif /* SQLITE_MUTEX_WIN */

#endif /* !defined(SQLITE_MUTEX_APPDEF) */

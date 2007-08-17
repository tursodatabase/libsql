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
** $Id: mutex.c,v 1.4 2007/08/17 01:14:38 drh Exp $
*/

/*
** If SQLITE_MUTEX_APPDEF is defined, then this whole module is
** omitted and equivalent functionality just be provided by the
** application that links against the SQLite library.
*/
#ifndef SQLITE_MUTEX_APPDEF

/*
** The start of real code
*/
#include "sqliteInt.h"

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
  return (sqlite3_mutex*)sqlite3_mutex_alloc;
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
** The sqlite3_mutex_exit() routine exits a mutex that was
** previously entered by the same thread.  The behavior
** is undefined if the mutex is not currently entered or
** is not currently allocated.  SQLite will never do either.
*/
void sqlite3_mutex_leave(sqlite3_mutex *pNotUsed){}

#if 0
/**************** Non-recursive Pthread Mutex Implementation *****************
**
** This implementation of mutexes is built using a version of pthreads that
** does not have native support for recursive mutexes.
*/

/*
** Each recursive mutex is an instance of the following structure.
*/
struct RMutex {
  int recursiveMagic;         /* Magic number identifying this as recursive */
  int nRef;                   /* Number of entrances */
  pthread_mutex_t auxMutex;   /* Mutex controlling access to nRef and owner */
  pthread_mutex_t mainMutex;  /* Mutex controlling the lock */
  pthread_t owner;            /* Thread that is within this mutex */
};

/*
** Each fast mutex is an instance of the following structure
*/
struct FMutex {
  int fastMagic;          /* Identifies this as a fast mutex */
  pthread_mutex_t mutex;  /* The actual underlying mutex */
};

/*
** Either of the above
*/
union AnyMutex {
  struct RMutex r;
  struct FMutex f;
};

/*
** Magic numbers
*/
#define SQLITE_MTX_RECURSIVE   0x4ED886ED
#define SQLITE_MTX_STATIC      0x56FCE1B4
#define SQLITE_MTX_FAST        0x245BFD4F

/*
** Static mutexes
*/

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
  static struct FMutex staticMutexes[] = {
    { SQLITE_MTX_STATIC, PTHREAD_MUTEX_INITIALIZER },
    { SQLITE_MTX_STATIC, PTHREAD_MUTEX_INITIALIZER },
    { SQLITE_MTX_STATIC, PTHREAD_MUTEX_INITIALIZER },
  };
  sqlite3_mutex *p;
  switch( iType ){
    case SQLITE_MUTEX_FAST: {
      struct FMutex *px = sqlite3_malloc( sizeof(*px) );
      if( px ){
        px->fastMagic = SQLITE_MTX_FAST;
        pthread_mutex_init(&px->mutex, 0);
      }
      p = (sqlite3_mutex*)px;
      break;
    }
    case SQLITE_MUTEX_RECURSIVE: {
      struct RMutex *px = sqlite3_malloc( sizeof(*px) );
      if( px ){
        px->recursiveMagic = SQLITE_MTX_RECURSIVE;
        pthread_mutex_init(&px->auxMutex, 0);
        pthread_mutex_init(&px->mainMutex, 0);
        px->nRef = 0;
      }
      p = (sqlite3_mutex*)px;
      break;
    }
    default: {
      p = &staticMutexes[iType-2];
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
void sqlite3_mutex_free(sqlite3_mutex *pMutex){
  int iType = *(int*)pMutex;
  if( iType==SQLITE_MTX_FAST ){
    struct FMutex *p = (struct FMutex*)pMutex;
    pthread_mutex_destroy(&p->mutex);
    sqlite3_free(p);
  }else if( iType==SQLITE_MTX_RECURSIVE ){
    struct RMutex *p = (struct RMutex*)pMutex;
    pthread_mutex_destroy(&p->auxMutex);
    pthread_mutex_destroy(&p->mainMutex);
    sqlite3_free(p);
  }
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
void sqlite3_mutex_enter(sqlite3_mutex *pMutex){
  if( SQLITE_MTX_FAST == *(int*)pMutex ){
    struct FMutex *p = (struct FMutex*)pMutex;
    pthread_mutex_lock(&p->mutex);
  }else{
    struct RMutex *p = (struct RMutex*)pMutex;
    while(1){
      pthread_mutex_lock(&p->auxMutex);
      if( p->nRef==0 ){
        p->nRef++;
        p->owner = pthread_self();
        pthread_mutex_lock(&p->mainMutex);
        pthread_mutex_unlock(&p->auxMutex);
        break;
      }else if( pthread_equal(p->owner, pthread_self()) ){
        p->nRef++;
        pthread_mutex_unlock(&p->auxMutex);
        break;
      }else{
        pthread_mutex_unlock(&p->auxMutex);
        pthread_mutex_lock(&p->mainMutex);
        pthread_mutex_unlock(&p->mainMutex);
      }
    }
  }
}
int sqlite3_mutex_try(sqlite3_mutex *pMutex){
  if( SQLITE_MTX_FAST == *(int*)pMutex ){
    struct FMutex *p = (struct FMutex*)pMutex;
    if( pthread_mutex_trylock(&p->mutex) ){
      return SQLITE_BUSY;
    }
  }else{
    struct RMutex *p = (struct RMutex*)pMutex;
    pthread_mutex_lock(&p->auxMutex);
    if( p->nRef==0 ){
      p->nRef++;
      p->owner = pthread_self();
      pthread_mutex_lock(&p->mainMutex);
      pthread_mutex_unlock(&p->auxMutex);
    }else if( pthread_equal(p->owner, pthread_self()) ){
      p->nRef++;
      pthread_mutex_unlock(&p->auxMutex);
    }else{
      pthread_mutex_unlock(&p->auxMutex);
      return SQLITE_BUSY;
    }
  }
  return SQLITE_OK;
}

/*
** The sqlite3_mutex_exit() routine exits a mutex that was
** previously entered by the same thread.  The behavior
** is undefined if the mutex is not currently entered or
** is not currently allocated.  SQLite will never do either.
*/
void sqlite3_mutex_leave(sqlite3_mutex *pMutex){
  if( SQLITE_MTX_FAST == *(int*)pMutex ){
    struct FMutex *p = (struct FMutex*)pMutex;
    pthread_mutex_unlock(&p->mutex);
  }else{
    struct RMutex *p = (struct RMutex*)pMutex;
    pthread_mutex_lock(&p->auxMutex);
    p->nRef--;
    if( p->nRef<=0 ){
      pthread_mutex_unlock(&p->mainMutex);
    }
    pthread_mutex_unlock(&p->auxMutex);
  }
}
#endif /* non-recursive pthreads */

#endif /* !defined(SQLITE_MUTEX_APPDEF) */

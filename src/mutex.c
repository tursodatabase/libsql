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
** $Id: mutex.c,v 1.2 2007/08/16 10:09:03 danielk1977 Exp $
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
** that means that a mutex could not be allocated.  SQLite
** will unwind its stack and return an error.  The argument
** to sqlite3_mutex_alloc() is usually zero, which causes
** any space required for the mutex to be obtained from
** sqlite3_malloc().  However if the argument is a positive
** integer less than SQLITE_NUM_STATIC_MUTEX, then a pointer
** to a static mutex is returned.  There are a finite number
** of static mutexes.  Static mutexes should not be passed
** to sqlite3_mutex_free().  The allocation of a static
** mutex cannot fail.
*/
sqlite3_mutex *sqlite3_mutex_alloc(int idNotUsed){
  return (sqlite3_mutex*)sqlite3_mutex_alloc;
}

/*
** This routine deallocates a previously
** allocated mutex.  SQLite is careful to deallocate every
** mutex that it allocates.
*/
void sqlite3_mutex_free(sqlite3_mutex *pNotUsed){}

/*
** The sqlite3_mutex_enter() routine attempts to enter a
** mutex.  If another thread is already within the mutex,
** sqlite3_mutex_enter() will return SQLITE_BUSY if blockFlag
** is zero, or it will block and wait for the other thread to
** exit if blockFlag is non-zero.  Mutexes are recursive.  The
** same thread can enter a single mutex multiple times.  Each
** entrance must be matched with a corresponding exit before
** another thread is able to enter the mutex.
*/
int sqlite3_mutex_enter(sqlite3_mutex *pNotUsed, int blockFlag){
  return SQLITE_OK;
}

/*
** The sqlite3_mutex_exit() routine exits a mutex that was
** previously entered by the same thread.  The behavior
** is undefined if the mutex is not currently entered or
** is not currently allocated.  SQLite will never do either.
*/
void sqlite3_mutex_leave(sqlite3_mutex *pNotUsed){
  return;
}

/*
** The sqlite3_mutex_serialize() routine is used to serialize 
** execution of a subroutine.  The subroutine given in the argument
** is invoked.  But only one thread at a time is allowed to be
** running a subroutine using sqlite3_mutex_serialize().
*/
int sqlite3_mutex_serialize(void (*xCallback)(void*), void *pArg){
  xCallback(pArg);
  return SQLITE_OK;
}

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
  int nRef;                   /* Number of entrances */
  pthread_mutex_t auxMutex;   /* Mutex controlling access to nRef and owner */
  pthread_mutex_t mainMutex;  /* Mutex controlling the lock */
  pthread_t owner;            /* Thread that is within this mutex */
};

/*
** Static mutexes
*/
static struct RMutex rmutexes[] = {
  { 0, PTHREAD_MUTEX_INITIALIZER, PTHREAD_MUTEX_INITIALIZER, },
  { 0, PTHREAD_MUTEX_INITIALIZER, PTHREAD_MUTEX_INITIALIZER, },
  { 0, PTHREAD_MUTEX_INITIALIZER, PTHREAD_MUTEX_INITIALIZER, },
};

/*
** A mutex used for serialization.
*/
static RMutex serialMutex =
   {0, PTHREAD_MUTEX_INITIALIZER, PTHREAD_MUTEX_INITIALIZER, };

/*
** The sqlite3_mutex_alloc() routine allocates a new
** mutex and returns a pointer to it.  If it returns NULL
** that means that a mutex could not be allocated.  SQLite
** will unwind its stack and return an error.  The argument
** to sqlite3_mutex_alloc() is usually zero, which causes
** any space required for the mutex to be obtained from
** sqlite3_malloc().  However if the argument is a positive
** integer less than SQLITE_NUM_STATIC_MUTEX, then a pointer
** to a static mutex is returned.  There are a finite number
** of static mutexes.  Static mutexes should not be passed
** to sqlite3_mutex_free().  The allocation of a static
** mutex cannot fail.
*/
sqlite3_mutex *sqlite3_mutex_alloc(int id){
  struct RMutex *p;
  if( id>0 ){
    if( id>sizeof(rmutexes)/sizeof(rmutexes[0]) ){
      p = 0;
    }else{
      p = &rmutexes[id-1];
    }
  }else{
    p = sqlite3_malloc( sizeof(*p) );
    if( p ){
      p->nRef = 0;
      pthread_mutex_init(&p->mutex, 0);
    }
  }
  return (sqlite3_mutex*)p;
}

/*
** This routine deallocates a previously
** allocated mutex.  SQLite is careful to deallocate every
** mutex that it allocates.
*/
void sqlite3_mutex_free(sqlite3_mutex *pMutex){
  struct RMutex *p = (struct RMutex*)pMutex;
  assert( p->nRef==0 );
  pthread_mutex_destroy(&p->mutex);
  sqlite3_free(p);
}

/*
** The sqlite3_mutex_enter() routine attempts to enter a
** mutex.  If another thread is already within the mutex,
** sqlite3_mutex_enter() will return SQLITE_BUSY if blockFlag
** is zero, or it will block and wait for the other thread to
** exit if blockFlag is non-zero.  Mutexes are recursive.  The
** same thread can enter a single mutex multiple times.  Each
** entrance must be matched with a corresponding exit before
** another thread is able to enter the mutex.
*/
int sqlite3_mutex_enter(sqlite3_mutex *pMutex, int blockFlag){
  struct RMutex *p = (struct RMutex*)pMutex;
  while(1){
    pthread_mutex_lock(&p->auxMutex);
    if( p->nRef==0 ){
      p->nRef++;
      p->owner = pthread_self();
      pthread_mutex_lock(&p->mainMutex);
      pthread_mutex_unlock(&p->auxMutex);
      return SQLITE_OK;
    }else if( pthread_equal(p->owner, pthread_self()) ){
      p->nRef++;
      pthread_mutex_unlock(&p->auxMutex);
      return SQLITE_OK;
    }else if( !blockFlag ){
      pthread_mutex_unlock(&p->auxMutex);
      return SQLITE_BUSY;
    }else{
      pthread_mutex_unlock(&p->auxMutex);
      pthread_mutex_lock(&p->mainMutex);
      pthread_mutex_unlock(&p->mainMutex);
    }
  }
  /* NOTREACHED */
}

/*
** The sqlite3_mutex_exit() routine exits a mutex that was
** previously entered by the same thread.  The behavior
** is undefined if the mutex is not currently entered or
** is not currently allocated.  SQLite will never do either.
*/
void sqlite3_mutex_leave(sqlite3_mutex *pMutex){
  struct RMutex *p = (struct RMutex*)pMutex;
  pthread_mutex_lock(&p->auxMutex);
  p->nRef--;
  if( p->nRef<=0 ){
    pthread_mutex_unlock(&p->mainMutex);
  }
  pthread_mutex_unlock(&p->auxMutex);
}

/*
** The sqlite3_mutex_serialize() routine is used to serialize 
** execution of a subroutine.  The subroutine given in the argument
** is invoked.  But only one thread at a time is allowed to be
** running a subroutine using sqlite3_mutex_serialize().
*/
int sqlite3_mutex_serialize(void (*xCallback)(void*), void *pArg){
  sqlite3_mutex_enter(&serialMutex, 1);
  xCallback(pArg);
  sqlite3_mutex_leave(&serialMutex);
}
#endif /* non-recursive pthreads */

#endif /* !defined(SQLITE_MUTEX_APPDEF) */

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
** This file contains the C functions that implement mutexes.
**
** The implementation in this file does not provide any mutual
** exclusion and is thus suitable for use only in applications
** that use SQLite in a single thread.  But this implementation
** does do a lot of error checking on mutexes to make sure they
** are called correctly and at appropriate times.  Hence, this
** implementation is suitable for testing.
** debugging purposes
**
** $Id: mutex.c,v 1.27 2008/06/19 08:51:24 danielk1977 Exp $
*/
#include "sqliteInt.h"

#ifndef SQLITE_MUTEX_NOOP
/*
** Initialize the mutex system.
*/
int sqlite3MutexInit(void){ 
  int rc = SQLITE_OK;
  if( sqlite3Config.bCoreMutex ){
    if( !sqlite3Config.mutex.xMutexAlloc ){
      /* If the xMutexAlloc method has not been set, then the user did not
      ** install a mutex implementation via sqlite3_config() prior to 
      ** sqlite3_initialize() being called. This block copies pointers to
      ** the default implementation into the sqlite3Config structure.
      **
      ** The danger is that although sqlite3_config() is not a threadsafe
      ** API, sqlite3_initialize() is, and so multiple threads may be
      ** attempting to run this function simultaneously. To guard write
      ** access to the sqlite3Config structure, the 'MASTER' static mutex
      ** is obtained before modifying it.
      */
      sqlite3_mutex_methods *p = sqlite3DefaultMutex();
      sqlite3_mutex *pMaster = 0;
  
      rc = p->xMutexInit();
      if( rc==SQLITE_OK ){
        pMaster = p->xMutexAlloc(SQLITE_MUTEX_STATIC_MASTER);
        assert(pMaster);
        p->xMutexEnter(pMaster);
        assert( sqlite3Config.mutex.xMutexAlloc==0 
             || sqlite3Config.mutex.xMutexAlloc==p->xMutexAlloc
        );
        if( !sqlite3Config.mutex.xMutexAlloc ){
          sqlite3Config.mutex = *p;
        }
        p->xMutexLeave(pMaster);
      }
    }else{
      rc = sqlite3Config.mutex.xMutexInit();
    }
  }

  return rc;
}

/*
** Shutdown the mutex system. This call frees resources allocated by
** sqlite3MutexInit().
*/
int sqlite3MutexEnd(void){
  int rc = SQLITE_OK;
  rc = sqlite3Config.mutex.xMutexEnd();
  return rc;
}

/*
** Retrieve a pointer to a static mutex or allocate a new dynamic one.
*/
sqlite3_mutex *sqlite3_mutex_alloc(int id){
#ifndef SQLITE_OMIT_AUTOINIT
  if( sqlite3_initialize() ) return 0;
#endif
  return sqlite3Config.mutex.xMutexAlloc(id);
}

sqlite3_mutex *sqlite3MutexAlloc(int id){
  if( !sqlite3Config.bCoreMutex ){
    return 0;
  }
  return sqlite3Config.mutex.xMutexAlloc(id);
}

/*
** Free a dynamic mutex.
*/
void sqlite3_mutex_free(sqlite3_mutex *p){
  if( p ){
    sqlite3Config.mutex.xMutexFree(p);
  }
}

/*
** Obtain the mutex p. If some other thread already has the mutex, block
** until it can be obtained.
*/
void sqlite3_mutex_enter(sqlite3_mutex *p){
  if( p ){
    sqlite3Config.mutex.xMutexEnter(p);
  }
}

/*
** Obtain the mutex p. If successful, return SQLITE_OK. Otherwise, if another
** thread holds the mutex and it cannot be obtained, return SQLITE_BUSY.
*/
int sqlite3_mutex_try(sqlite3_mutex *p){
  int rc = SQLITE_OK;
  if( p ){
    return sqlite3Config.mutex.xMutexTry(p);
  }
  return rc;
}

/*
** The sqlite3_mutex_leave() routine exits a mutex that was previously
** entered by the same thread.  The behavior is undefined if the mutex 
** is not currently entered. If a NULL pointer is passed as an argument
** this function is a no-op.
*/
void sqlite3_mutex_leave(sqlite3_mutex *p){
  if( p ){
    sqlite3Config.mutex.xMutexLeave(p);
  }
}

#ifndef NDEBUG
/*
** The sqlite3_mutex_held() and sqlite3_mutex_notheld() routine are
** intended for use inside assert() statements.
*/
int sqlite3_mutex_held(sqlite3_mutex *p){
  return p==0 || sqlite3Config.mutex.xMutexHeld(p);
}
int sqlite3_mutex_notheld(sqlite3_mutex *p){
  return p==0 || sqlite3Config.mutex.xMutexNotheld(p);
}
#endif

#endif

#ifdef SQLITE_MUTEX_NOOP_DEBUG
/*
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
** The sqlite3_mutex_held() and sqlite3_mutex_notheld() routine are
** intended for use inside assert() statements.
*/
static int noopMutexHeld(sqlite3_mutex *p){
  return p==0 || p->cnt>0;
}
static int noopMutexNotheld(sqlite3_mutex *p){
  return p==0 || p->cnt==0;
}

/*
** Initialize and deinitialize the mutex subsystem.
*/
static int noopMutexInit(void){ return SQLITE_OK; }
static int noopMutexEnd(void){ return SQLITE_OK; }

/*
** The sqlite3_mutex_alloc() routine allocates a new
** mutex and returns a pointer to it.  If it returns NULL
** that means that a mutex could not be allocated. 
*/
static sqlite3_mutex *noopMutexAlloc(int id){
  static sqlite3_mutex aStatic[6];
  sqlite3_mutex *pNew = 0;
  switch( id ){
    case SQLITE_MUTEX_FAST:
    case SQLITE_MUTEX_RECURSIVE: {
      pNew = sqlite3Malloc(sizeof(*pNew));
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
static void noopMutexFree(sqlite3_mutex *p){
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
static void noopMutexEnter(sqlite3_mutex *p){
  assert( p->id==SQLITE_MUTEX_RECURSIVE || noopMutexNotheld(p) );
  p->cnt++;
}
static int noopMutexTry(sqlite3_mutex *p){
  assert( p->id==SQLITE_MUTEX_RECURSIVE || noopMutexNotheld(p) );
  p->cnt++;
  return SQLITE_OK;
}

/*
** The sqlite3_mutex_leave() routine exits a mutex that was
** previously entered by the same thread.  The behavior
** is undefined if the mutex is not currently entered or
** is not currently allocated.  SQLite will never do either.
*/
static void noopMutexLeave(sqlite3_mutex *p){
  assert( noopMutexHeld(p) );
  p->cnt--;
  assert( p->id==SQLITE_MUTEX_RECURSIVE || noopMutexNotheld(p) );
}

sqlite3_mutex_methods *sqlite3DefaultMutex(void){
  static sqlite3_mutex_methods sMutex = {
    noopMutexInit,
    noopMutexEnd,
    noopMutexAlloc,
    noopMutexFree,
    noopMutexEnter,
    noopMutexTry,
    noopMutexLeave,

    noopMutexHeld,
    noopMutexNotheld
  };

  return &sMutex;
}
#endif /* SQLITE_MUTEX_NOOP_DEBUG */

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
** This file contains code that is common across all mutex implementations.

**
** $Id: mutex.c,v 1.30 2009/02/17 16:29:11 danielk1977 Exp $
*/
#include "sqliteInt.h"

#ifndef SQLITE_MUTEX_OMIT
/*
** Initialize the mutex system.
*/
int sqlite3MutexInit(void){ 
  int rc = SQLITE_OK;
  if( sqlite3GlobalConfig.bCoreMutex ){
    if( !sqlite3GlobalConfig.mutex.xMutexAlloc ){
      /* If the xMutexAlloc method has not been set, then the user did not
      ** install a mutex implementation via sqlite3_config() prior to 
      ** sqlite3_initialize() being called. This block copies pointers to
      ** the default implementation into the sqlite3GlobalConfig structure.
      **
      ** The danger is that although sqlite3_config() is not a threadsafe
      ** API, sqlite3_initialize() is, and so multiple threads may be
      ** attempting to run this function simultaneously. To guard write
      ** access to the sqlite3GlobalConfig structure, the 'MASTER' static mutex
      ** is obtained before modifying it.
      */
      sqlite3_mutex_methods *p = sqlite3DefaultMutex();
      sqlite3_mutex *pMaster = 0;
  
      rc = p->xMutexInit();
      if( rc==SQLITE_OK ){
        pMaster = p->xMutexAlloc(SQLITE_MUTEX_STATIC_MASTER);
        assert(pMaster);
        p->xMutexEnter(pMaster);
        assert( sqlite3GlobalConfig.mutex.xMutexAlloc==0 
             || sqlite3GlobalConfig.mutex.xMutexAlloc==p->xMutexAlloc
        );
        if( !sqlite3GlobalConfig.mutex.xMutexAlloc ){
          sqlite3GlobalConfig.mutex = *p;
        }
        p->xMutexLeave(pMaster);
      }
    }else{
      rc = sqlite3GlobalConfig.mutex.xMutexInit();
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
  if( sqlite3GlobalConfig.mutex.xMutexEnd ){
    rc = sqlite3GlobalConfig.mutex.xMutexEnd();
  }
  return rc;
}

/*
** Retrieve a pointer to a static mutex or allocate a new dynamic one.
*/
sqlite3_mutex *sqlite3_mutex_alloc(int id){
#ifndef SQLITE_OMIT_AUTOINIT
  if( sqlite3_initialize() ) return 0;
#endif
  return sqlite3GlobalConfig.mutex.xMutexAlloc(id);
}

sqlite3_mutex *sqlite3MutexAlloc(int id){
  if( !sqlite3GlobalConfig.bCoreMutex ){
    return 0;
  }
  return sqlite3GlobalConfig.mutex.xMutexAlloc(id);
}

/*
** Free a dynamic mutex.
*/
void sqlite3_mutex_free(sqlite3_mutex *p){
  if( p ){
    sqlite3GlobalConfig.mutex.xMutexFree(p);
  }
}

/*
** Obtain the mutex p. If some other thread already has the mutex, block
** until it can be obtained.
*/
void sqlite3_mutex_enter(sqlite3_mutex *p){
  if( p ){
    sqlite3GlobalConfig.mutex.xMutexEnter(p);
  }
}

/*
** Obtain the mutex p. If successful, return SQLITE_OK. Otherwise, if another
** thread holds the mutex and it cannot be obtained, return SQLITE_BUSY.
*/
int sqlite3_mutex_try(sqlite3_mutex *p){
  int rc = SQLITE_OK;
  if( p ){
    return sqlite3GlobalConfig.mutex.xMutexTry(p);
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
    sqlite3GlobalConfig.mutex.xMutexLeave(p);
  }
}

#ifndef NDEBUG
/*
** The sqlite3_mutex_held() and sqlite3_mutex_notheld() routine are
** intended for use inside assert() statements.
*/
int sqlite3_mutex_held(sqlite3_mutex *p){
  return p==0 || sqlite3GlobalConfig.mutex.xMutexHeld(p);
}
int sqlite3_mutex_notheld(sqlite3_mutex *p){
  return p==0 || sqlite3GlobalConfig.mutex.xMutexNotheld(p);
}
#endif

#endif /* SQLITE_OMIT_MUTEX */

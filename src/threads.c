/*
** 2012 July 21
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
******************************************************************************
**
** This file presents a simple cross-platform threading interface for
** use internally by SQLite.
**
** A "thread" can be created using sqlite3ThreadCreate().  This thread
** runs independently of its creator until it is joined using
** sqlite3ThreadJoin(), at which point it terminates.
**
** Threads do not have to be real.  It could be that the work of the
** "thread" is done by the main thread at either the sqlite3ThreadCreate()
** or sqlite3ThreadJoin() call.  This is, in fact, what happens in
** single threaded systems.  Nothing in SQLite requires multiple threads.
** This interface exists so that applications that want to take advantage
** of multiple cores can do so, while also allowing applications to stay
** single-threaded if desired.
*/
#include "sqliteInt.h"

#if SQLITE_MAX_WORKER_THREADS>0

/********************************* Unix Pthreads ****************************/
#if SQLITE_OS_UNIX && defined(SQLITE_MUTEX_PTHREADS) && SQLITE_THREADSAFE>0

#define SQLITE_THREADS_IMPLEMENTED 1  /* Prevent the single-thread code below */
#include <pthread.h>

/* A running thread */
struct SQLiteThread {
  pthread_t tid;
  int done;
  void *pOut;
};

/* Create a new thread */
int sqlite3ThreadCreate(
  SQLiteThread **ppThread,  /* OUT: Write the thread object here */
  void *(*xTask)(void*),    /* Routine to run in a separate thread */
  void *pIn                 /* Argument passed into xTask() */
){
  SQLiteThread *p;

  assert( ppThread!=0 );
  assert( xTask!=0 );
  *ppThread = 0;
  p = sqlite3Malloc(sizeof(*p));
  if( p==0 ) return SQLITE_NOMEM;
  memset(p, 0, sizeof(*p));
  if( sqlite3GlobalConfig.bCoreMutex==0
    || pthread_create(&p->tid, 0, xTask, pIn)!=0 
  ){
    p->done = 1;
    p->pOut = xTask(pIn);
  }
  *ppThread = p;
  return SQLITE_OK;
}

/* Get the results of the thread */
int sqlite3ThreadJoin(SQLiteThread *p, void **ppOut){
  int rc;

  assert( ppOut!=0 );
  if( p==0 ) return SQLITE_NOMEM;
  if( p->done ){
    *ppOut = p->pOut;
    rc = SQLITE_OK;
  }else{
    rc = pthread_join(p->tid, ppOut);
  }
  sqlite3_free(p);
  return rc ? SQLITE_ERROR : SQLITE_OK;
}

#endif /* SQLITE_OS_UNIX && defined(SQLITE_MUTEX_PTHREADS) */
/******************************** End Unix Pthreads *************************/


/********************************* Win32 Threads ****************************/
#if SQLITE_OS_WIN && !SQLITE_OS_WINRT && SQLITE_THREADSAFE>0

#define SQLITE_THREADS_IMPLEMENTED 1  /* Prevent the single-thread code below */
#include <process.h>

/* A running thread */
struct SQLiteThread {
  uintptr_t tid;           /* The thread handle */
  void *(*xTask)(void*);   /* The routine to run as a thread */
  void *pIn;               /* Argument to xTask */
  void *pResult;           /* Result of xTask */
};

/* Thread procedure Win32 compatibility shim */
static void sqlite3ThreadProc(
  void *pArg  /* IN: Pointer to the SQLiteThread structure */
){
  SQLiteThread *p = (SQLiteThread *)pArg;

  assert( p!=0 );
  assert( p->xTask!=0 );
  p->pResult = p->xTask(p->pIn);
  _endthread();
}

/* Create a new thread */
int sqlite3ThreadCreate(
  SQLiteThread **ppThread,  /* OUT: Write the thread object here */
  void *(*xTask)(void*),    /* Routine to run in a separate thread */
  void *pIn                 /* Argument passed into xTask() */
){
  SQLiteThread *p;

  assert( ppThread!=0 );
  assert( xTask!=0 );
  *ppThread = 0;
  p = sqlite3Malloc(sizeof(*p));
  if( p==0 ) return SQLITE_NOMEM;
  if( sqlite3GlobalConfig.bCoreMutex==0 ){
    memset(p, 0, sizeof(*p));
  }else{
    p->xTask = xTask;
    p->pIn = pIn;
    p->tid = _beginthread(sqlite3ThreadProc, 0, p);
    if( p->tid==(uintptr_t)-1 ){
      memset(p, 0, sizeof(*p));
    }
  }
  if( p->xTask==0 ){
    p->pResult = xTask(pIn);
  }
  *ppThread = p;
  return SQLITE_OK;
}

/* Get the results of the thread */
int sqlite3ThreadJoin(SQLiteThread *p, void **ppOut){
  DWORD rc;

  assert( ppOut!=0 );
  if( p==0 ) return SQLITE_NOMEM;
  if( p->xTask==0 ){
    rc = WAIT_OBJECT_O;
  }else{
    rc = sqlite3Win32Wait((HANDLE)p->tid);
    assert( rc!=WAIT_IO_COMPLETION );
  }
  if( rc==WAIT_OBJECT_0 ) *ppOut = p->pResult;
  sqlite3_free(p);
  return (rc==WAIT_OBJECT_0) ? SQLITE_OK : SQLITE_ERROR;
}

#endif /* SQLITE_OS_WIN && !SQLITE_OS_WINRT */
/******************************** End Win32 Threads *************************/


/********************************* Single-Threaded **************************/
#ifndef SQLITE_THREADS_IMPLEMENTED
/*
** This implementation does not actually create a new thread.  It does the
** work of the thread in the main thread, when either the thread is created
** or when it is joined
*/

/* A running thread */
struct SQLiteThread {
  void *(*xTask)(void*);   /* The routine to run as a thread */
  void *pIn;               /* Argument to xTask */
  void *pResult;           /* Result of xTask */
};

/* Create a new thread */
int sqlite3ThreadCreate(
  SQLiteThread **ppThread,  /* OUT: Write the thread object here */
  void *(*xTask)(void*),    /* Routine to run in a separate thread */
  void *pIn                 /* Argument passed into xTask() */
){
  SQLiteThread *p;

  assert( ppThread!=0 );
  assert( xTask!=0 );
  *ppThread = 0;
  p = sqlite3Malloc(sizeof(*p));
  if( p==0 ) return SQLITE_NOMEM;
  if( (SQLITE_PTR_TO_INT(p)/17)&1 ){
    p->xTask = xTask;
    p->pIn = pIn;
  }else{
    p->xTask = 0;
    p->pResult = xTask(pIn);
  }
  *ppThread = p;
  return SQLITE_OK;
}

/* Get the results of the thread */
int sqlite3ThreadJoin(SQLiteThread *p, void **ppOut){
  assert( ppOut!=0 );
  if( p==0 ) return SQLITE_NOMEM;
  if( p->xTask ){
    *ppOut = p->xTask(p->pIn);
  }else{
    *ppOut = p->pResult;
  }
  sqlite3_free(p);
  return SQLITE_OK;
}

#endif /* !defined(SQLITE_THREADS_IMPLEMENTED) */
/****************************** End Single-Threaded *************************/
#endif /* SQLITE_MAX_WORKER_THREADS>0 */

/*
** 2005 December 14
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
*************************************************************************
**
** $Id: test_async.c,v 1.59 2009/04/23 14:58:40 danielk1977 Exp $
**
** This file contains a binding of the asynchronous IO extension interface
** (defined in ext/async/sqlite3async.h) to Tcl.
*/

#define TCL_THREADS 
#include <tcl.h>

#ifdef SQLITE_ENABLE_ASYNCIO

#include "sqlite3async.h"
#include "sqlite3.h"
#include <assert.h>


struct TestAsyncGlobal {
  int isInstalled;                     /* True when async VFS is installed */
} testasync_g = { 0 };

TCL_DECLARE_MUTEX(testasync_g_writerMutex);

/*
** sqlite3async_enable ?YES/NO?
**
** Enable or disable the asynchronous I/O backend.  This command is
** not thread-safe.  Do not call it while any database connections
** are open.
*/
static int testAsyncEnable(
  void * clientData,
  Tcl_Interp *interp,
  int objc,
  Tcl_Obj *CONST objv[]
){
  if( objc!=1 && objc!=2 ){
    Tcl_WrongNumArgs(interp, 1, objv, "?YES/NO?");
    return TCL_ERROR;
  }
  if( objc==1 ){
    Tcl_SetObjResult(interp, Tcl_NewIntObj(testasync_g.isInstalled));
  }else{
    int enable;
    if( Tcl_GetBooleanFromObj(interp, objv[1], &enable) ) return TCL_ERROR;
    if( enable ){
      sqlite3async_initialize(0, 1);
    }else{
      sqlite3async_shutdown();
    }
    testasync_g.isInstalled = enable;
  }
  return TCL_OK;
}

/*
** sqlite3async_halt  ?"now"|"idle"|"never"?
**
** Set the conditions at which the writer thread will halt.
*/
static int testAsyncHalt(
  void * clientData,
  Tcl_Interp *interp,
  int objc,
  Tcl_Obj *CONST objv[]
){
  int eWhen;
  const char *azConstant[] = { "never", "now", "idle", 0 };

  assert( SQLITEASYNC_HALT_NEVER==0 );
  assert( SQLITEASYNC_HALT_NOW==1 );
  assert( SQLITEASYNC_HALT_IDLE==2 );

  if( objc!=1 && objc!=2 ){
    Tcl_WrongNumArgs(interp, 1, objv, "?OPTION?");
    return TCL_ERROR;
  }
  if( objc==2 ){
    if( Tcl_GetIndexFromObj(interp, objv[1], azConstant, "option", 0, &eWhen) ){
      return TCL_ERROR;
    }
    sqlite3async_control(SQLITEASYNC_HALT, eWhen);
  }

  /* Always return the current value of the 'halt' option. */
  sqlite3async_control(SQLITEASYNC_GET_HALT, &eWhen);
  Tcl_SetObjResult(interp, Tcl_NewStringObj(azConstant[eWhen], -1));

  return TCL_OK;
}

/*
** sqlite3async_delay ?MS?
**
** Query or set the number of milliseconds of delay in the writer
** thread after each write operation.  The default is 0.  By increasing
** the memory delay we can simulate the effect of slow disk I/O.
*/
static int testAsyncDelay(
  void * clientData,
  Tcl_Interp *interp,
  int objc,
  Tcl_Obj *CONST objv[]
){
  int iMs;
  if( objc!=1 && objc!=2 ){
    Tcl_WrongNumArgs(interp, 1, objv, "?MS?");
    return TCL_ERROR;
  }
  if( objc==2 ){
    if( Tcl_GetIntFromObj(interp, objv[1], &iMs) ){
      return TCL_ERROR;
    }
    sqlite3async_control(SQLITEASYNC_DELAY, iMs);
  }

  /* Always return the current value of the 'delay' option. */
  sqlite3async_control(SQLITEASYNC_GET_DELAY, &iMs);
  Tcl_SetObjResult(interp, Tcl_NewIntObj(iMs));
  return TCL_OK;
}

static Tcl_ThreadCreateType tclWriterThread(ClientData pIsStarted){
  Tcl_MutexLock(&testasync_g_writerMutex);
  *((int *)pIsStarted) = 1;
  sqlite3async_run();
  Tcl_MutexUnlock(&testasync_g_writerMutex);
  TCL_THREAD_CREATE_RETURN;
}

/*
** sqlite3async_start
**
** Start a new writer thread.
*/
static int testAsyncStart(
  void * clientData,
  Tcl_Interp *interp,
  int objc,
  Tcl_Obj *CONST objv[]
){
  volatile int isStarted = 0;
  ClientData threadData = (ClientData)&isStarted;

  Tcl_ThreadId x;
  const int nStack = TCL_THREAD_STACK_DEFAULT;
  const int flags = TCL_THREAD_NOFLAGS;
  int rc;

  rc = Tcl_CreateThread(&x, tclWriterThread, threadData, nStack, flags);
  if( rc!=TCL_OK ){
    return TCL_ERROR;
  }
  while( isStarted==0 ){
#if 0
    sched_yield();
#endif
  }
  return TCL_OK;
}

/*
** sqlite3async_wait
**
** Wait for the current writer thread to terminate.
**
** If the current writer thread is set to run forever then this
** command would block forever.  To prevent that, an error is returned. 
*/
static int testAsyncWait(
  void * clientData,
  Tcl_Interp *interp,
  int objc,
  Tcl_Obj *CONST objv[]
){
  int eCond;
  if( objc!=1 ){
    Tcl_WrongNumArgs(interp, 1, objv, "");
    return TCL_ERROR;
  }

  sqlite3async_control(SQLITEASYNC_GET_HALT, &eCond);
  if( eCond==SQLITEASYNC_HALT_NEVER ){
    Tcl_AppendResult(interp, "would block forever", (char*)0);
    return TCL_ERROR;
  }

  Tcl_MutexLock(&testasync_g_writerMutex);
  Tcl_MutexUnlock(&testasync_g_writerMutex);
  return TCL_OK;
}

#endif  /* SQLITE_ENABLE_ASYNCIO */

/*
** This routine registers the custom TCL commands defined in this
** module.  This should be the only procedure visible from outside
** of this module.
*/
int Sqlitetestasync_Init(Tcl_Interp *interp){
#if SQLITE_ENABLE_ASYNCIO
  Tcl_CreateObjCommand(interp,"sqlite3async_enable",testAsyncEnable,0,0);
  Tcl_CreateObjCommand(interp,"sqlite3async_halt",testAsyncHalt,0,0);
  Tcl_CreateObjCommand(interp,"sqlite3async_delay",testAsyncDelay,0,0);
  Tcl_CreateObjCommand(interp,"sqlite3async_start",testAsyncStart,0,0);
  Tcl_CreateObjCommand(interp,"sqlite3async_wait",testAsyncWait,0,0);
#endif  /* SQLITE_ENABLE_ASYNCIO */
  return TCL_OK;
}


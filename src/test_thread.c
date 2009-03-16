/*
** 2007 September 9
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
** This file contains the implementation of some Tcl commands used to
** test that sqlite3 database handles may be concurrently accessed by 
** multiple threads. Right now this only works on unix.
**
** $Id: test_thread.c,v 1.11 2009/03/16 13:19:36 danielk1977 Exp $
*/

#include "sqliteInt.h"
#include <tcl.h>

#if SQLITE_THREADSAFE

#include <errno.h>

#if !defined(_MSC_VER)
#include <unistd.h>
#endif

/*
** One of these is allocated for each thread created by [sqlthread spawn].
*/
typedef struct SqlThread SqlThread;
struct SqlThread {
  Tcl_ThreadId parent;     /* Thread id of parent thread */
  Tcl_Interp *interp;      /* Parent interpreter */
  char *zScript;           /* The script to execute. */
  char *zVarname;          /* Varname in parent script */
};

/*
** A custom Tcl_Event type used by this module. When the event is
** handled, script zScript is evaluated in interpreter interp. If
** the evaluation throws an exception (returns TCL_ERROR), then the
** error is handled by Tcl_BackgroundError(). If no error occurs,
** the result is simply discarded.
*/
typedef struct EvalEvent EvalEvent;
struct EvalEvent {
  Tcl_Event base;          /* Base class of type Tcl_Event */
  char *zScript;           /* The script to execute. */
  Tcl_Interp *interp;      /* The interpreter to execute it in. */
};

static Tcl_ObjCmdProc sqlthread_proc;
static Tcl_ObjCmdProc clock_seconds_proc;
static Tcl_ObjCmdProc blocking_step_proc;
int Sqlitetest1_Init(Tcl_Interp *);

/*
** Handler for events of type EvalEvent.
*/
static int tclScriptEvent(Tcl_Event *evPtr, int flags){
  int rc;
  EvalEvent *p = (EvalEvent *)evPtr;
  rc = Tcl_Eval(p->interp, p->zScript);
  if( rc!=TCL_OK ){
    Tcl_BackgroundError(p->interp);
  }
  UNUSED_PARAMETER(flags);
  return 1;
}

/*
** Register an EvalEvent to evaluate the script pScript in the
** parent interpreter/thread of SqlThread p.
*/
static void postToParent(SqlThread *p, Tcl_Obj *pScript){
  EvalEvent *pEvent;
  char *zMsg;
  int nMsg;

  zMsg = Tcl_GetStringFromObj(pScript, &nMsg); 
  pEvent = (EvalEvent *)ckalloc(sizeof(EvalEvent)+nMsg+1);
  pEvent->base.nextPtr = 0;
  pEvent->base.proc = tclScriptEvent;
  pEvent->zScript = (char *)&pEvent[1];
  memcpy(pEvent->zScript, zMsg, nMsg+1);
  pEvent->interp = p->interp;

  Tcl_ThreadQueueEvent(p->parent, (Tcl_Event *)pEvent, TCL_QUEUE_TAIL);
  Tcl_ThreadAlert(p->parent);
}

/*
** The main function for threads created with [sqlthread spawn].
*/
static Tcl_ThreadCreateType tclScriptThread(ClientData pSqlThread){
  Tcl_Interp *interp;
  Tcl_Obj *pRes;
  Tcl_Obj *pList;
  int rc;
  SqlThread *p = (SqlThread *)pSqlThread;
  extern int Sqlitetest_mutex_Init(Tcl_Interp*);

  interp = Tcl_CreateInterp();
  Tcl_CreateObjCommand(interp, "clock_seconds", clock_seconds_proc, 0, 0);
  Tcl_CreateObjCommand(interp, "sqlthread", sqlthread_proc, pSqlThread, 0);
#if defined(OS_UNIX) && defined(SQLITE_ENABLE_UNLOCK_NOTIFY)
  Tcl_CreateObjCommand(interp, "sqlite3_blocking_step", blocking_step_proc,0,0);
#endif
  Sqlitetest1_Init(interp);
  Sqlitetest_mutex_Init(interp);

  rc = Tcl_Eval(interp, p->zScript);
  pRes = Tcl_GetObjResult(interp);
  pList = Tcl_NewObj();
  Tcl_IncrRefCount(pList);
  Tcl_IncrRefCount(pRes);

  if( rc!=TCL_OK ){
    Tcl_ListObjAppendElement(interp, pList, Tcl_NewStringObj("error", -1));
    Tcl_ListObjAppendElement(interp, pList, pRes);
    postToParent(p, pList);
    Tcl_DecrRefCount(pList);
    pList = Tcl_NewObj();
  }

  Tcl_ListObjAppendElement(interp, pList, Tcl_NewStringObj("set", -1));
  Tcl_ListObjAppendElement(interp, pList, Tcl_NewStringObj(p->zVarname, -1));
  Tcl_ListObjAppendElement(interp, pList, pRes);
  postToParent(p, pList);

  ckfree((void *)p);
  Tcl_DecrRefCount(pList);
  Tcl_DecrRefCount(pRes);
  Tcl_DeleteInterp(interp);
  TCL_THREAD_CREATE_RETURN;
}

/*
** sqlthread spawn VARNAME SCRIPT
**
**     Spawn a new thread with its own Tcl interpreter and run the
**     specified SCRIPT(s) in it. The thread terminates after running
**     the script. The result of the script is stored in the variable
**     VARNAME.
**
**     The caller can wait for the script to terminate using [vwait VARNAME].
*/
static int sqlthread_spawn(
  ClientData clientData,
  Tcl_Interp *interp,
  int objc,
  Tcl_Obj *CONST objv[]
){
  Tcl_ThreadId x;
  SqlThread *pNew;
  int rc;

  int nVarname; char *zVarname;
  int nScript; char *zScript;

  /* Parameters for thread creation */
  const int nStack = TCL_THREAD_STACK_DEFAULT;
  const int flags = TCL_THREAD_NOFLAGS;

  assert(objc==4);
  UNUSED_PARAMETER(clientData);
  UNUSED_PARAMETER(objc);

  zVarname = Tcl_GetStringFromObj(objv[2], &nVarname);
  zScript = Tcl_GetStringFromObj(objv[3], &nScript);

  pNew = (SqlThread *)ckalloc(sizeof(SqlThread)+nVarname+nScript+2);
  pNew->zVarname = (char *)&pNew[1];
  pNew->zScript = (char *)&pNew->zVarname[nVarname+1];
  memcpy(pNew->zVarname, zVarname, nVarname+1);
  memcpy(pNew->zScript, zScript, nScript+1);
  pNew->parent = Tcl_GetCurrentThread();
  pNew->interp = interp;

  rc = Tcl_CreateThread(&x, tclScriptThread, (void *)pNew, nStack, flags);
  if( rc!=TCL_OK ){
    Tcl_AppendResult(interp, "Error in Tcl_CreateThread()", 0);
    ckfree((char *)pNew);
    return TCL_ERROR;
  }

  return TCL_OK;
}

/*
** sqlthread parent SCRIPT
**
**     This can be called by spawned threads only. It sends the specified
**     script back to the parent thread for execution. The result of
**     evaluating the SCRIPT is returned. The parent thread must enter
**     the event loop for this to work - otherwise the caller will
**     block indefinitely.
**
**     NOTE: At the moment, this doesn't work. FIXME.
*/
static int sqlthread_parent(
  ClientData clientData,
  Tcl_Interp *interp,
  int objc,
  Tcl_Obj *CONST objv[]
){
  EvalEvent *pEvent;
  char *zMsg;
  int nMsg;
  SqlThread *p = (SqlThread *)clientData;

  assert(objc==3);
  UNUSED_PARAMETER(objc);

  if( p==0 ){
    Tcl_AppendResult(interp, "no parent thread", 0);
    return TCL_ERROR;
  }

  zMsg = Tcl_GetStringFromObj(objv[2], &nMsg);
  pEvent = (EvalEvent *)ckalloc(sizeof(EvalEvent)+nMsg+1);
  pEvent->base.nextPtr = 0;
  pEvent->base.proc = tclScriptEvent;
  pEvent->zScript = (char *)&pEvent[1];
  memcpy(pEvent->zScript, zMsg, nMsg+1);
  pEvent->interp = p->interp;
  Tcl_ThreadQueueEvent(p->parent, (Tcl_Event *)pEvent, TCL_QUEUE_TAIL);
  Tcl_ThreadAlert(p->parent);

  return TCL_OK;
}

static int xBusy(void *pArg, int nBusy){
  UNUSED_PARAMETER(pArg);
  UNUSED_PARAMETER(nBusy);
  sqlite3_sleep(50);
  return 1;             /* Try again... */
}

/*
** sqlthread open
**
**     Open a database handle and return the string representation of
**     the pointer value.
*/
static int sqlthread_open(
  ClientData clientData,
  Tcl_Interp *interp,
  int objc,
  Tcl_Obj *CONST objv[]
){
  int sqlite3TestMakePointerStr(Tcl_Interp *interp, char *zPtr, void *p);

  const char *zFilename;
  sqlite3 *db;
  int rc;
  char zBuf[100];
  extern void Md5_Register(sqlite3*);

  UNUSED_PARAMETER(clientData);
  UNUSED_PARAMETER(objc);

  zFilename = Tcl_GetString(objv[2]);
  rc = sqlite3_open(zFilename, &db);
  Md5_Register(db);
  sqlite3_busy_handler(db, xBusy, 0);
  
  if( sqlite3TestMakePointerStr(interp, zBuf, db) ) return TCL_ERROR;
  Tcl_AppendResult(interp, zBuf, 0);

  return TCL_OK;
}


/*
** sqlthread open
**
**     Return the current thread-id (Tcl_GetCurrentThread()) cast to
**     an integer.
*/
static int sqlthread_id(
  ClientData clientData,
  Tcl_Interp *interp,
  int objc,
  Tcl_Obj *CONST objv[]
){
  Tcl_ThreadId id = Tcl_GetCurrentThread();
  Tcl_SetObjResult(interp, Tcl_NewIntObj((int)id));
  UNUSED_PARAMETER(clientData);
  UNUSED_PARAMETER(objc);
  UNUSED_PARAMETER(objv);
  return TCL_OK;
}


/*
** Dispatch routine for the sub-commands of [sqlthread].
*/
static int sqlthread_proc(
  ClientData clientData,
  Tcl_Interp *interp,
  int objc,
  Tcl_Obj *CONST objv[]
){
  struct SubCommand {
    char *zName;
    Tcl_ObjCmdProc *xProc;
    int nArg;
    char *zUsage;
  } aSub[] = {
    {"parent", sqlthread_parent, 1, "SCRIPT"},
    {"spawn",  sqlthread_spawn,  2, "VARNAME SCRIPT"},
    {"open",   sqlthread_open,   1, "DBNAME"},
    {"id",     sqlthread_id,     0, ""},
    {0, 0, 0}
  };
  struct SubCommand *pSub;
  int rc;
  int iIndex;

  if( objc<2 ){
    Tcl_WrongNumArgs(interp, 1, objv, "SUB-COMMAND");
    return TCL_ERROR;
  }

  rc = Tcl_GetIndexFromObjStruct(
      interp, objv[1], aSub, sizeof(aSub[0]), "sub-command", 0, &iIndex
  );
  if( rc!=TCL_OK ) return rc;
  pSub = &aSub[iIndex];

  if( objc!=(pSub->nArg+2) ){
    Tcl_WrongNumArgs(interp, 2, objv, pSub->zUsage);
    return TCL_ERROR;
  }

  return pSub->xProc(clientData, interp, objc, objv);
}

/*
** The [clock_seconds] command. This is more or less the same as the
** regular tcl [clock seconds], except that it is available in testfixture
** when linked against both Tcl 8.4 and 8.5. Because [clock seconds] is
** implemented as a script in Tcl 8.5, it is not usually available to
** testfixture.
*/ 
static int clock_seconds_proc(
  ClientData clientData,
  Tcl_Interp *interp,
  int objc,
  Tcl_Obj *CONST objv[]
){
  Tcl_Time now;
  Tcl_GetTime(&now);
  Tcl_SetObjResult(interp, Tcl_NewIntObj(now.sec));
  UNUSED_PARAMETER(clientData);
  UNUSED_PARAMETER(objc);
  UNUSED_PARAMETER(objv);
  return TCL_OK;
}

/*************************************************************************
** This block contains the implementation of the [sqlite3_blocking_step]
** command available to threads created by [sqlthread spawn] commands. It
** is only available on UNIX for now. This is because pthread condition
** variables are used.
**
** The source code for the C functions sqlite3_blocking_step(),
** blocking_step_notify() and the structure UnlockNotification is
** automatically extracted from this file and used as part of the
** documentation for the sqlite3_unlock_notify() API function. This
** should be considered if these functions are to be extended (i.e. to 
** support windows) in the future.
*/ 
#if defined(OS_UNIX) && defined(SQLITE_ENABLE_UNLOCK_NOTIFY)

/* BEGIN_SQLITE_BLOCKING_STEP */
/* This example uses the pthreads API */
#include <pthread.h>

/*
** A pointer to an instance of this structure is passed as the user-context
** pointer when registering for an unlock-notify callback.
*/
typedef struct UnlockNotification UnlockNotification;
struct UnlockNotification {
  int fired;                           /* True after unlock event has occured */
  pthread_cond_t cond;                 /* Condition variable to wait on */
  pthread_mutex_t mutex;               /* Mutex to protect structure */
};

/*
** This function is an unlock-notify callback registered with SQLite.
*/
static void blocking_step_notify(void **apArg, int nArg){
  int i;
  for(i=0; i<nArg; i++){
    UnlockNotification *p = (UnlockNotification *)apArg[i];
    pthread_mutex_lock(&p->mutex);
    p->fired = 1;
    pthread_cond_signal(&p->cond);
    pthread_mutex_unlock(&p->mutex);
  }
}

/*
** This function is a wrapper around the SQLite function sqlite3_step().
** It functions in the same way as step(), except that if a required
** shared-cache lock cannot be obtained, this function may block waiting for
** the lock to become available. In this scenario the normal API step()
** function always returns SQLITE_LOCKED.
**
** If this function returns SQLITE_LOCKED, the caller should rollback
** the current transaction (if any) and try again later. Otherwise, the
** system may become deadlocked.
*/
int sqlite3_blocking_step(sqlite3_stmt *pStmt){
  int rc = SQLITE_OK;

  while( rc==SQLITE_OK && SQLITE_LOCKED==(rc = sqlite3_step(pStmt)) ){
    sqlite3 *db = sqlite3_db_handle(pStmt);
    UnlockNotification un;

    /* Initialize the UnlockNotification structure. */
    un.fired = 0;
    pthread_mutex_init(&un.mutex, 0);
    pthread_cond_init(&un.cond, 0);

    rc = sqlite3_unlock_notify(db, blocking_step_notify, (void *)&un);
    assert( rc==SQLITE_LOCKED || rc==SQLITE_OK );

    /* The call to sqlite3_unlock_notify() always returns either 
    ** SQLITE_LOCKED or SQLITE_OK. 
    **
    ** If SQLITE_LOCKED was returned, then the system is deadlocked. In this
    ** case this function needs to return SQLITE_LOCKED to the caller so 
    ** that it can roll back the current transaction. Simply leaving rc
    ** as it is is enough to accomplish that, as the next test of the
    ** while() condition above will fail and the current value of rc
    ** (SQLITE_LOCKED) will be returned to the caller. sqlite3_reset() is
    ** not called on the statement handle, so the caller can still use either
    ** sqlite3_finalize() or reset() to collect the statement's error code
    ** after this function returns.
    **
    ** Otherwise, if SQLITE_OK was returned, do two things:
    **
    **   1) Reset the SQL statement.
    **   2) Block until the unlock-notify callback is invoked.
    */
    if( rc==SQLITE_OK ){
      sqlite3_reset(pStmt);
      pthread_mutex_lock(&un.mutex);
      if( !un.fired ){
        pthread_cond_wait(&un.cond, &un.mutex);
      }
      pthread_mutex_unlock(&un.mutex);
    }

    /* Destroy the mutex and condition variables created at the top of
    ** the while loop. */
    pthread_cond_destroy(&un.cond);
    pthread_mutex_destroy(&un.mutex);
  }

  return rc;
}
/* END_SQLITE_BLOCKING_STEP */

/*
** Usage: sqlite3_blocking_step STMT
**
** Advance the statement to the next row.
*/
static int blocking_step_proc(
  void * clientData,
  Tcl_Interp *interp,
  int objc,
  Tcl_Obj *CONST objv[]
){
  /* Functions from test1.c */
  void *sqlite3TestTextToPtr(const char *);
  const char *sqlite3TestErrorName(int);

  sqlite3_stmt *pStmt;
  int rc;

  if( objc!=2 ){
    Tcl_WrongNumArgs(interp, 1, objv, "STMT");
    return TCL_ERROR;
  }

  pStmt = (sqlite3_stmt*)sqlite3TestTextToPtr(Tcl_GetString(objv[1]));
  rc = sqlite3_blocking_step(pStmt);

  Tcl_SetResult(interp, (char *)sqlite3TestErrorName(rc), 0);
  return TCL_OK;
}

#endif
/*
** End of implementation of [sqlite3_blocking_step].
************************************************************************/

/*
** Register commands with the TCL interpreter.
*/
int SqlitetestThread_Init(Tcl_Interp *interp){
  Tcl_CreateObjCommand(interp, "sqlthread", sqlthread_proc, 0, 0);
  Tcl_CreateObjCommand(interp, "clock_seconds", clock_seconds_proc, 0, 0);
#if defined(OS_UNIX) && defined(SQLITE_ENABLE_UNLOCK_NOTIFY)
  Tcl_CreateObjCommand(interp, "sqlite3_blocking_step", blocking_step_proc,0,0);
#endif
  return TCL_OK;
}
#else
int SqlitetestThread_Init(Tcl_Interp *interp){
  return TCL_OK;
}
#endif

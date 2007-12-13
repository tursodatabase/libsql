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
** $Id: test_thread.c,v 1.6 2007/12/13 21:54:11 drh Exp $
*/

#include "sqliteInt.h"
#include <tcl.h>

#if SQLITE_THREADSAFE && defined(TCL_THREADS)

#include <errno.h>
#include <unistd.h>

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

  interp = Tcl_CreateInterp();
  Tcl_CreateObjCommand(interp, "sqlthread", sqlthread_proc, pSqlThread, 0);
  Sqlitetest1_Init(interp);

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
  return;
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
    sqlite3_free(pNew);
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
** Register commands with the TCL interpreter.
*/
int SqlitetestThread_Init(Tcl_Interp *interp){
  Tcl_CreateObjCommand(interp, "sqlthread", sqlthread_proc, 0, 0);
  return TCL_OK;
}
#else
int SqlitetestThread_Init(Tcl_Interp *interp){
  return TCL_OK;
}
#endif

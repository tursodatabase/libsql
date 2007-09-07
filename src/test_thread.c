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
** $Id: test_thread.c,v 1.1 2007/09/07 11:29:25 danielk1977 Exp $
*/

#include "sqliteInt.h"
#if defined(OS_UNIX) && SQLITE_THREADSAFE

#include <tcl.h>
#include <pthread.h>
#include <errno.h>
#include <unistd.h>

/*
** One of these is allocated for each thread created by [sqlthread spawn].
*/
typedef struct SqlThread SqlThread;
struct SqlThread {
  int fd;            /* The pipe to send commands to the parent */
  char *zScript;     /* The script to execute. */
  char *zVarname;    /* Varname in parent script */
};

typedef struct SqlParent SqlParent;
struct SqlParent {
  Tcl_Interp *interp;
  int fd;
};

static Tcl_ObjCmdProc sqlthread_proc;

static void *tclScriptThread(void *pSqlThread){
  Tcl_Interp *interp;
  Tcl_Obj *pRes;
  Tcl_Obj *pList;

  char *zMsg;
  int nMsg;
  int rc;

  SqlThread *p = (SqlThread *)pSqlThread;

  interp = Tcl_CreateInterp();
  Tcl_CreateObjCommand(interp, "sqlthread", sqlthread_proc, pSqlThread, 0);
  Sqlitetest1_Init(interp);

  rc = Tcl_Eval(interp, p->zScript);
  pRes = Tcl_GetObjResult(interp);
  pList = Tcl_NewObj();
  Tcl_IncrRefCount(pList);

  if( rc==TCL_OK ){
    Tcl_ListObjAppendElement(interp, pList, Tcl_NewStringObj("set", -1));
    Tcl_ListObjAppendElement(interp, pList, Tcl_NewStringObj(p->zVarname, -1));
  }else{
    Tcl_ListObjAppendElement(interp, pList, Tcl_NewStringObj("error", -1));
  }
  Tcl_ListObjAppendElement(interp, pList, pRes);

  zMsg = Tcl_GetStringFromObj(pList, &nMsg); 
  write(p->fd, zMsg, nMsg+1);
  close(p->fd);
  sqlite3_free(p);
  Tcl_DecrRefCount(pList);
  Tcl_DeleteInterp(interp);

  return 0;
}

void pipe_callback(ClientData clientData, int flags){
  SqlParent *p = (SqlParent *)clientData;
  char zBuf[1024];
  int nChar;

  nChar = read(p->fd, zBuf, 1023);
  if( nChar<=0 ){
    /* Other end has been closed */
    Tcl_DeleteFileHandler(p->fd);
    sqlite3_free(p);
  }else{
    zBuf[1023] = '\0';
    if( TCL_OK!=Tcl_Eval(p->interp, zBuf) ){
      Tcl_BackgroundError(p->interp);
    }
  }
}

/*
** sqlthread spawn VARNAME SCRIPT
**
**     Spawn a new thread with it's own Tcl interpreter and run the
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
  pthread_t x;
  SqlThread *pNew;
  SqlParent *pParent;
  int fds[2];
  int rc;

  int nVarname; char *zVarname;
  int nScript; char *zScript;

  assert(objc==4);

  zVarname = Tcl_GetStringFromObj(objv[2], &nVarname);
  zScript = Tcl_GetStringFromObj(objv[3], &nScript);
  pNew = (SqlThread *)sqlite3_malloc(sizeof(SqlThread)+nVarname+nScript+2);
  if( pNew==0 ){
    Tcl_AppendResult(interp, "Malloc failure", 0);
    return TCL_ERROR;
  }
  pNew->zVarname = (char *)&pNew[1];
  pNew->zScript = (char *)&pNew->zVarname[nVarname+1];
  memcpy(pNew->zVarname, zVarname, nVarname+1);
  memcpy(pNew->zScript, zScript, nScript+1);

  pParent = (SqlParent *)sqlite3_malloc(sizeof(SqlParent));
  if( pParent==0 ){
    Tcl_AppendResult(interp, "Malloc failure", 0);
    sqlite3_free(pNew);
    return TCL_ERROR;
  }

  rc = pipe(fds);
  if( rc!=0 ){
    Tcl_AppendResult(interp, "Error in pipe(): ", strerror(errno), 0);
    sqlite3_free(pNew);
    sqlite3_free(pParent);
    return TCL_ERROR;
  }

  pParent->fd = fds[0];
  pParent->interp = interp;
  Tcl_CreateFileHandler(
      fds[0], TCL_READABLE|TCL_EXCEPTION, pipe_callback, (void *)pParent
  );

  pNew->fd = fds[1];
  rc = pthread_create(&x, 0, tclScriptThread, (void *)pNew);
  if( rc!=0 ){
    Tcl_AppendResult(interp, "Error in pthread_create(): ", strerror(errno), 0);
    Tcl_DeleteFileHandler(fds[0]);
    sqlite3_free(pNew);
    sqlite3_free(pParent);
    close(fds[0]);
    close(fds[1]);
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
#if 0
static int sqlthread_parent(
  ClientData clientData,
  Tcl_Interp *interp,
  int objc,
  Tcl_Obj *CONST objv[]
){
  char *zMsg;
  int nMsg;
  SqlThread *p = (SqlThread *)clientData;

  assert(objc==3);
  if( p==0 ){
    Tcl_AppendResult(interp, "no parent thread", 0);
    return TCL_ERROR;
  }

  zMsg = Tcl_GetStringFromObj(objv[2], &nMsg);
  write(p->fd, zMsg, nMsg+1);

  return TCL_OK;
}
#endif

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
#if 0
    {"parent", sqlthread_parent, 1, "SCRIPT"},
#endif
    {"spawn",  sqlthread_spawn,  2, "VARNAME SCRIPT"},
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


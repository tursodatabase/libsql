/*
** 2008 June 18
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
** $Id: test_mutex.c,v 1.1 2008/06/18 09:45:56 danielk1977 Exp $
*/

#include "tcl.h"
#include "sqlite3.h"
#include <stdlib.h>
#include <assert.h>
#include <string.h>

const char *sqlite3TestErrorName(int);

struct sqlite3_mutex {
  sqlite3_mutex *pReal;
  int eType;
};

static struct test_mutex_globals {
  int isInstalled;
  sqlite3_mutex_methods m;          /* Interface to "real" mutex system */
  int aCounter[8];                  /* Number of grabs of each type of mutex */
  sqlite3_mutex aStatic[6];         /* The six static mutexes */
} g;

static int counterMutexHeld(sqlite3_mutex *p){
  return g.m.xMutexHeld(p->pReal);
}

static int counterMutexNotheld(sqlite3_mutex *p){
  return g.m.xMutexNotheld(p->pReal);
}

static int counterMutexInit(void){ 
  return g.m.xMutexInit();
}

static int counterMutexEnd(void){ 
  return g.m.xMutexEnd();
}

static sqlite3_mutex *counterMutexAlloc(int eType){
  sqlite3_mutex *pReal;
  sqlite3_mutex *pRet = 0;

  assert(eType<8 && eType>=0);

  pReal = g.m.xMutexAlloc(eType);
  if( !pReal ) return 0;

  if( eType==0 || eType==1 ){
    pRet = (sqlite3_mutex *)malloc(sizeof(sqlite3_mutex));
  }else{
    pRet = &g.aStatic[eType-2];
  }

  pRet->eType = eType;
  pRet->pReal = pReal;
  return pRet;
}

static void counterMutexFree(sqlite3_mutex *p){
  g.m.xMutexFree(p->pReal);
  if( p->eType==0 || p->eType==1 ){
    free(p);
  }
}

static void counterMutexEnter(sqlite3_mutex *p){
  g.aCounter[p->eType]++;
  g.m.xMutexEnter(p->pReal);
}

static int counterMutexTry(sqlite3_mutex *p){
  g.aCounter[p->eType]++;
  return g.m.xMutexTry(p->pReal);
}

static void counterMutexLeave(sqlite3_mutex *p){
  g.m.xMutexLeave(p->pReal);
}

/*
** sqlite3_shutdown
*/
static int test_shutdown(
  void * clientData,
  Tcl_Interp *interp,
  int objc,
  Tcl_Obj *CONST objv[]
){
  int rc;

  if( objc!=1 ){
    Tcl_WrongNumArgs(interp, 1, objv, "");
    return TCL_ERROR;
  }

  rc = sqlite3_shutdown();
  Tcl_SetResult(interp, (char *)sqlite3TestErrorName(rc), TCL_VOLATILE);
  return TCL_OK;
}

/*
** sqlite3_initialize
*/
static int test_initialize(
  void * clientData,
  Tcl_Interp *interp,
  int objc,
  Tcl_Obj *CONST objv[]
){
  int rc;

  if( objc!=1 ){
    Tcl_WrongNumArgs(interp, 1, objv, "");
    return TCL_ERROR;
  }

  rc = sqlite3_initialize();
  Tcl_SetResult(interp, (char *)sqlite3TestErrorName(rc), TCL_VOLATILE);
  return TCL_OK;
}

/*
** install_mutex_counters BOOLEAN
*/
static int test_install_mutex_counters(
  void * clientData,
  Tcl_Interp *interp,
  int objc,
  Tcl_Obj *CONST objv[]
){
  int rc = SQLITE_OK;
  int isInstall;

  sqlite3_mutex_methods counter_methods = {
    counterMutexInit,
    counterMutexAlloc,
    counterMutexFree,
    counterMutexEnter,
    counterMutexTry,
    counterMutexLeave,
    counterMutexEnd,
    counterMutexHeld,
    counterMutexNotheld
  };

  if( objc!=2 ){
    Tcl_WrongNumArgs(interp, 1, objv, "BOOLEAN");
    return TCL_ERROR;
  }
  if( TCL_OK!=Tcl_GetBooleanFromObj(interp, objv[1], &isInstall) ){
    return TCL_ERROR;
  }

  assert(isInstall==0 || isInstall==1);
  assert(g.isInstalled==0 || g.isInstalled==1);
  if( isInstall==g.isInstalled ){
    Tcl_AppendResult(interp, "mutex counters are ", 0);
    Tcl_AppendResult(interp, isInstall?"already installed":"not installed", 0);
    return TCL_ERROR;
  }

  if( isInstall ){
    assert( g.m.xMutexAlloc==0 );
    rc = sqlite3_config(SQLITE_CONFIG_GETMUTEX, &g.m);
    if( rc==SQLITE_OK ){
      sqlite3_config(SQLITE_CONFIG_MUTEX, &counter_methods);
    }
  }else{
    assert( g.m.xMutexAlloc );
    rc = sqlite3_config(SQLITE_CONFIG_MUTEX, &g.m);
    memset(&g.m, 0, sizeof(sqlite3_mutex_methods));
  }

  if( rc==SQLITE_OK ){
    g.isInstalled = isInstall;
  }

  Tcl_SetResult(interp, (char *)sqlite3TestErrorName(rc), TCL_VOLATILE);
  return TCL_OK;
}

/*
** read_mutex_counters
*/
static int test_read_mutex_counters(
  void * clientData,
  Tcl_Interp *interp,
  int objc,
  Tcl_Obj *CONST objv[]
){
  Tcl_Obj *pRet;
  int ii;
  char *aName[8] = {
    "fast",        "recursive",   "static_master", "static_mem", 
    "static_mem2", "static_prng", "static_lru",    "static_lru2"
  };

  if( objc!=1 ){
    Tcl_WrongNumArgs(interp, 1, objv, "");
    return TCL_ERROR;
  }

  pRet = Tcl_NewObj();
  Tcl_IncrRefCount(pRet);
  for(ii=0; ii<8; ii++){
    Tcl_ListObjAppendElement(interp, pRet, Tcl_NewStringObj(aName[ii], -1));
    Tcl_ListObjAppendElement(interp, pRet, Tcl_NewIntObj(g.aCounter[ii]));
  }
  Tcl_SetObjResult(interp, pRet);
  Tcl_DecrRefCount(pRet);

  return TCL_OK;
}

/*
** clear_mutex_counters
*/
static int test_clear_mutex_counters(
  void * clientData,
  Tcl_Interp *interp,
  int objc,
  Tcl_Obj *CONST objv[]
){
  int ii;

  if( objc!=1 ){
    Tcl_WrongNumArgs(interp, 1, objv, "");
    return TCL_ERROR;
  }

  for(ii=0; ii<8; ii++){
    g.aCounter[ii] = 0;
  }
  return TCL_OK;
}

int Sqlitetest_mutex_Init(Tcl_Interp *interp){
  static struct {
    char *zName;
    Tcl_ObjCmdProc *xProc;
  } aCmd[] = {
    { "sqlite3_shutdown",        (Tcl_ObjCmdProc*)test_shutdown },
    { "sqlite3_initialize",      (Tcl_ObjCmdProc*)test_initialize },

    { "install_mutex_counters",  (Tcl_ObjCmdProc*)test_install_mutex_counters },
    { "read_mutex_counters",     (Tcl_ObjCmdProc*)test_read_mutex_counters },
    { "clear_mutex_counters",    (Tcl_ObjCmdProc*)test_clear_mutex_counters },
  };
  int i;
  for(i=0; i<sizeof(aCmd)/sizeof(aCmd[0]); i++){
    Tcl_CreateObjCommand(interp, aCmd[i].zName, aCmd[i].xProc, 0, 0);
  }
  memset(&g, 0, sizeof(g));
  return SQLITE_OK;
}


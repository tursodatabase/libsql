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
** $Id: test_mutex.c,v 1.15 2009/03/20 13:15:30 drh Exp $
*/

#include "tcl.h"
#include "sqlite3.h"
#include "sqliteInt.h"
#include <stdlib.h>
#include <assert.h>
#include <string.h>

/* defined in test1.c */
const char *sqlite3TestErrorName(int);

/* A countable mutex */
struct sqlite3_mutex {
  sqlite3_mutex *pReal;
  int eType;
};

/* State variables */
static struct test_mutex_globals {
  int isInstalled;              /* True if installed */
  int disableInit;              /* True to cause sqlite3_initalize() to fail */
  int disableTry;               /* True to force sqlite3_mutex_try() to fail */
  int isInit;                   /* True if initialized */
  sqlite3_mutex_methods m;      /* Interface to "real" mutex system */
  int aCounter[8];              /* Number of grabs of each type of mutex */
  sqlite3_mutex aStatic[6];     /* The six static mutexes */
} g = {0};

/* Return true if the countable mutex is currently held */
static int counterMutexHeld(sqlite3_mutex *p){
  return g.m.xMutexHeld(p->pReal);
}

/* Return true if the countable mutex is not currently held */
static int counterMutexNotheld(sqlite3_mutex *p){
  return g.m.xMutexNotheld(p->pReal);
}

/* Initialize the countable mutex interface
** Or, if g.disableInit is non-zero, then do not initialize but instead
** return the value of g.disableInit as the result code.  This can be used
** to simulate an initialization failure.
*/
static int counterMutexInit(void){ 
  int rc;
  if( g.disableInit ) return g.disableInit;
  rc = g.m.xMutexInit();
  g.isInit = 1;
  return rc;
}

/*
** Uninitialize the mutex subsystem
*/
static int counterMutexEnd(void){ 
  g.isInit = 0;
  return g.m.xMutexEnd();
}

/*
** Allocate a countable mutex
*/
static sqlite3_mutex *counterMutexAlloc(int eType){
  sqlite3_mutex *pReal;
  sqlite3_mutex *pRet = 0;

  assert( g.isInit );
  assert(eType<8 && eType>=0);

  pReal = g.m.xMutexAlloc(eType);
  if( !pReal ) return 0;

  if( eType==SQLITE_MUTEX_FAST || eType==SQLITE_MUTEX_RECURSIVE ){
    pRet = (sqlite3_mutex *)malloc(sizeof(sqlite3_mutex));
  }else{
    pRet = &g.aStatic[eType-2];
  }

  pRet->eType = eType;
  pRet->pReal = pReal;
  return pRet;
}

/*
** Free a countable mutex
*/
static void counterMutexFree(sqlite3_mutex *p){
  assert( g.isInit );
  g.m.xMutexFree(p->pReal);
  if( p->eType==SQLITE_MUTEX_FAST || p->eType==SQLITE_MUTEX_RECURSIVE ){
    free(p);
  }
}

/*
** Enter a countable mutex.  Block until entry is safe.
*/
static void counterMutexEnter(sqlite3_mutex *p){
  assert( g.isInit );
  g.aCounter[p->eType]++;
  g.m.xMutexEnter(p->pReal);
}

/*
** Try to enter a mutex.  Return true on success.
*/
static int counterMutexTry(sqlite3_mutex *p){
  assert( g.isInit );
  g.aCounter[p->eType]++;
  if( g.disableTry ) return SQLITE_BUSY;
  return g.m.xMutexTry(p->pReal);
}

/* Leave a mutex
*/
static void counterMutexLeave(sqlite3_mutex *p){
  assert( g.isInit );
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
    counterMutexEnd,
    counterMutexAlloc,
    counterMutexFree,
    counterMutexEnter,
    counterMutexTry,
    counterMutexLeave,
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
    g.disableTry = 0;
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
    "static_open", "static_prng", "static_lru",    "static_lru2"
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

/*
** Create and free a mutex.  Return the mutex pointer.  The pointer
** will be invalid since the mutex has already been freed.  The
** return pointer just checks to see if the mutex really was allocated.
*/
static int test_alloc_mutex(
  void * clientData,
  Tcl_Interp *interp,
  int objc,
  Tcl_Obj *CONST objv[]
){
#if SQLITE_THREADSAFE
  sqlite3_mutex *p = sqlite3_mutex_alloc(SQLITE_MUTEX_FAST);
  char zBuf[100];
  sqlite3_mutex_free(p);
  sqlite3_snprintf(sizeof(zBuf), zBuf, "%p", p);
  Tcl_AppendResult(interp, zBuf, (char*)0);
#endif
  return TCL_OK;
}

/*
** sqlite3_config OPTION
**
** OPTION can be either one of the keywords:
**
**            SQLITE_CONFIG_SINGLETHREAD
**            SQLITE_CONFIG_MULTITHREAD
**            SQLITE_CONFIG_SERIALIZED
**
** Or OPTION can be an raw integer.
*/
static int test_config(
  void * clientData,
  Tcl_Interp *interp,
  int objc,
  Tcl_Obj *CONST objv[]
){
  struct ConfigOption {
    const char *zName;
    int iValue;
  } aOpt[] = {
    {"singlethread", SQLITE_CONFIG_SINGLETHREAD},
    {"multithread",  SQLITE_CONFIG_MULTITHREAD},
    {"serialized",   SQLITE_CONFIG_SERIALIZED},
    {0, 0}
  };
  int s = sizeof(struct ConfigOption);
  int i;
  int rc;

  if( objc!=2 ){
    Tcl_WrongNumArgs(interp, 1, objv, "");
    return TCL_ERROR;
  }

  if( Tcl_GetIndexFromObjStruct(interp, objv[1], aOpt, s, "flag", 0, &i) ){
    if( Tcl_GetIntFromObj(interp, objv[1], &i) ){
      return TCL_ERROR;
    }
  }else{
    i = aOpt[i].iValue;
  }

  rc = sqlite3_config(i);
  Tcl_SetResult(interp, (char *)sqlite3TestErrorName(rc), TCL_VOLATILE);
  return TCL_OK;
}

static sqlite3 *getDbPointer(Tcl_Interp *pInterp, Tcl_Obj *pObj){
  sqlite3 *db;
  Tcl_CmdInfo info;
  char *zCmd = Tcl_GetString(pObj);
  if( Tcl_GetCommandInfo(pInterp, zCmd, &info) ){
    db = *((sqlite3 **)info.objClientData);
  }else{
    db = (sqlite3*)sqlite3TestTextToPtr(zCmd);
  }
  assert( db );
  return db;
}

static int test_enter_db_mutex(
  void * clientData,
  Tcl_Interp *interp,
  int objc,
  Tcl_Obj *CONST objv[]
){
  sqlite3 *db;
  if( objc!=2 ){
    Tcl_WrongNumArgs(interp, 1, objv, "DB");
    return TCL_ERROR;
  }
  db = getDbPointer(interp, objv[1]);
  if( !db ){
    return TCL_ERROR;
  }
  sqlite3_mutex_enter(sqlite3_db_mutex(db));
  return TCL_OK;
}

static int test_leave_db_mutex(
  void * clientData,
  Tcl_Interp *interp,
  int objc,
  Tcl_Obj *CONST objv[]
){
  sqlite3 *db;
  if( objc!=2 ){
    Tcl_WrongNumArgs(interp, 1, objv, "DB");
    return TCL_ERROR;
  }
  db = getDbPointer(interp, objv[1]);
  if( !db ){
    return TCL_ERROR;
  }
  sqlite3_mutex_leave(sqlite3_db_mutex(db));
  return TCL_OK;
}

int Sqlitetest_mutex_Init(Tcl_Interp *interp){
  static struct {
    char *zName;
    Tcl_ObjCmdProc *xProc;
  } aCmd[] = {
    { "sqlite3_shutdown",        (Tcl_ObjCmdProc*)test_shutdown },
    { "sqlite3_initialize",      (Tcl_ObjCmdProc*)test_initialize },
    { "sqlite3_config",          (Tcl_ObjCmdProc*)test_config },

    { "enter_db_mutex",          (Tcl_ObjCmdProc*)test_enter_db_mutex },
    { "leave_db_mutex",          (Tcl_ObjCmdProc*)test_leave_db_mutex },

    { "alloc_dealloc_mutex",     (Tcl_ObjCmdProc*)test_alloc_mutex },
    { "install_mutex_counters",  (Tcl_ObjCmdProc*)test_install_mutex_counters },
    { "read_mutex_counters",     (Tcl_ObjCmdProc*)test_read_mutex_counters },
    { "clear_mutex_counters",    (Tcl_ObjCmdProc*)test_clear_mutex_counters },
  };
  int i;
  for(i=0; i<sizeof(aCmd)/sizeof(aCmd[0]); i++){
    Tcl_CreateObjCommand(interp, aCmd[i].zName, aCmd[i].xProc, 0, 0);
  }

  Tcl_LinkVar(interp, "disable_mutex_init", 
              (char*)&g.disableInit, TCL_LINK_INT);
  Tcl_LinkVar(interp, "disable_mutex_try", 
              (char*)&g.disableTry, TCL_LINK_INT);
  return SQLITE_OK;
}

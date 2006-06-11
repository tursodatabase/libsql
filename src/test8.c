/*
** 2006 June 10
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
*************************************************************************
** Code for testing the virtual table interfaces.  This code
** is not included in the SQLite library.  It is used for automated
** testing of the SQLite library.
**
** $Id: test8.c,v 1.1 2006/06/11 23:41:56 drh Exp $
*/
#include "sqliteInt.h"
#include "tcl.h"
#include "os.h"
#include <stdlib.h>
#include <string.h>

/* Methods for the echo module */
static int echoCreate(
  sqlite3 *db,
  const sqlite3_module *pModule,
  int argc, char **argv,
  sqlite3_vtab **ppVtab
){
  int i;
  Tcl_Interp *interp = pModule->pAux;
  *ppVtab = pModule->pAux;

  Tcl_SetVar(interp, "echo_module", "xCreate", TCL_GLOBAL_ONLY);
  for(i=0; i<argc; i++){
    Tcl_SetVar(interp, "echo_module", argv[i],
                TCL_APPEND_VALUE | TCL_LIST_ELEMENT | TCL_GLOBAL_ONLY);
  }
  return 0;
}
static int echoConnect(
  sqlite3 *db,
  const sqlite3_module *pModule,
  int argc, char **argv,
  sqlite3_vtab **ppVtab
){
  int i;
  Tcl_Interp *interp = pModule->pAux;
  *ppVtab = pModule->pAux;

  Tcl_SetVar(interp, "echo_module", "xConnect", TCL_GLOBAL_ONLY);
  for(i=0; i<argc; i++){
    Tcl_SetVar(interp, "echo_module", argv[i],
                TCL_APPEND_VALUE | TCL_LIST_ELEMENT | TCL_GLOBAL_ONLY);
  }
  return 0;
}
static int echoDisconnect(sqlite3_vtab *pVtab){
  Tcl_Interp *interp = (Tcl_Interp*)pVtab;
  Tcl_SetVar(interp, "echo_module", "xDisconnect",
                TCL_APPEND_VALUE | TCL_LIST_ELEMENT | TCL_GLOBAL_ONLY);
  return 0;
}
static int echoDestroy(sqlite3_vtab *pVtab){
  Tcl_Interp *interp = (Tcl_Interp*)pVtab;
  Tcl_SetVar(interp, "echo_module", "xDestroy",
                TCL_APPEND_VALUE | TCL_LIST_ELEMENT | TCL_GLOBAL_ONLY);
  return 0;
}

/*
** A virtual table module that merely echos method calls into TCL
** variables.
*/
static sqlite3_module echoModule = {
  0,
  "echo",
  0,
  echoCreate,
  echoConnect,
  0,
  echoDisconnect, 
  echoDestroy,
};

/*
** Decode a pointer to an sqlite3 object.
*/
static int getDbPointer(Tcl_Interp *interp, const char *zA, sqlite3 **ppDb){
  *ppDb = (sqlite3*)sqlite3TextToPtr(zA);
  return TCL_OK;
}


/*
** Register the echo virtual table module.
*/
static int register_echo_module(
  ClientData clientData, /* Pointer to sqlite3_enable_XXX function */
  Tcl_Interp *interp,    /* The TCL interpreter that invoked this command */
  int objc,              /* Number of arguments */
  Tcl_Obj *CONST objv[]  /* Command arguments */
){
  sqlite3 *db;
  if( objc!=2 ){
    Tcl_WrongNumArgs(interp, 1, objv, "DB");
    return TCL_ERROR;
  }
  if( getDbPointer(interp, Tcl_GetString(objv[1]), &db) ) return TCL_ERROR;
  echoModule.pAux = interp;
#ifndef SQLITE_OMIT_VIRTUALTABLE
  sqlite3_create_module(db, "echo", &echoModule);
#endif
  return TCL_OK;
}


/*
** Register commands with the TCL interpreter.
*/
int Sqlitetest8_Init(Tcl_Interp *interp){
  static struct {
     char *zName;
     Tcl_ObjCmdProc *xProc;
     void *clientData;
  } aObjCmd[] = {
     { "register_echo_module",   register_echo_module, 0 },
  };
  int i;
  for(i=0; i<sizeof(aObjCmd)/sizeof(aObjCmd[0]); i++){
    Tcl_CreateObjCommand(interp, aObjCmd[i].zName, 
        aObjCmd[i].xProc, aObjCmd[i].clientData, 0);
  }
  return TCL_OK;
}

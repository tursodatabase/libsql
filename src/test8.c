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
** $Id: test8.c,v 1.3 2006/06/12 11:24:37 danielk1977 Exp $
*/
#include "sqliteInt.h"
#include "tcl.h"
#include "os.h"
#include <stdlib.h>
#include <string.h>

/*
** Global Tcl variable $echo_module is a list. This routine appends
** the string element zArg to that list in interpreter interp.
*/
static void appendToEchoModule(const sqlite3_module *pModule, const char *zArg){
  int flags = (TCL_APPEND_VALUE | TCL_LIST_ELEMENT | TCL_GLOBAL_ONLY);
  Tcl_SetVar((Tcl_Interp *)(pModule->pAux), "echo_module", zArg, flags);
}

/*
** This function is called from within the echo-modules xCreate and
** xConnect methods. The argc and argv arguments are copies of those 
** passed to the calling method. This function is responsible for
** calling sqlite3_declare_vtab() to declare the schema of the virtual
** table being created or connected.
**
** If the constructor was passed just one argument, i.e.:
**
**   CREATE TABLE t1 AS echo(t2);
**
** Then t2 is assumed to be the name of a *real* database table. The
** schema of the virtual table is declared by passing a copy of the 
** CREATE TABLE statement for the real table to sqlite3_declare_vtab().
** Hence, the virtual table should have exactly the same column names and 
** types as the real table.
*/
static int echoDeclareVtab(sqlite3 *db, int argc, char **argv){
  int rc = SQLITE_OK;

  if( argc==2 ){
    sqlite3_stmt *pStmt = 0;
    sqlite3_prepare(db, 
        "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = ?",
        -1, &pStmt, 0);
    sqlite3_bind_text(pStmt, 1, argv[1], -1, 0);
    if( sqlite3_step(pStmt)==SQLITE_ROW ){
      const char *zCreateTable = sqlite3_column_text(pStmt, 0);
      sqlite3_declare_vtab(db, zCreateTable);
    } else {
      rc = SQLITE_ERROR;
    }
    sqlite3_finalize(pStmt);
  }

  return rc;
}

/* Methods for the echo module */
static int echoCreate(
  sqlite3 *db,
  const sqlite3_module *pModule,
  int argc, char **argv,
  sqlite3_vtab **ppVtab
){
  int i;
  *ppVtab = pModule->pAux;

  appendToEchoModule(pModule, "xCreate");
  for(i=0; i<argc; i++){
    appendToEchoModule(pModule, argv[i]);
  }

  echoDeclareVtab(db, argc, argv);
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

  echoDeclareVtab(db, argc, argv);
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
  0,                         /* iVersion */
  "echo",                    /* zName */
  0,                         /* pAux */
  echoCreate,
  echoConnect,
  0,                         /* xBestIndex */
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

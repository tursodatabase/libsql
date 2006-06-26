/*
** 2006 June 13
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
** The emphasis of this file is a virtual table that provides
** access to TCL variables.
**
** $Id: test_tclvar.c,v 1.4 2006/06/26 11:17:51 danielk1977 Exp $
*/
#include "sqliteInt.h"
#include "tcl.h"
#include "os.h"
#include <stdlib.h>
#include <string.h>

typedef struct tclvar_vtab tclvar_vtab;
typedef struct tclvar_cursor tclvar_cursor;

/* 
** A tclvar virtual-table object 
*/
struct tclvar_vtab {
  sqlite3_vtab base;
  Tcl_Interp *interp;
};

/* A tclvar cursor object */
struct tclvar_cursor {
  sqlite3_vtab_cursor base;
  Tcl_Obj *pList1, *pList2;
  int i, j;
};

/* Methods for the tclvar module */
static int tclvarConnect(
  sqlite3 *db,
  void *pAux,
  int argc, char **argv,
  sqlite3_vtab **ppVtab
){
  tclvar_vtab *pVtab;
  static const char zSchema[] = 
     "CREATE TABLE whatever(name TEXT, arrayname TEXT, value TEXT)";
  pVtab = sqliteMalloc( sizeof(*pVtab) );
  if( pVtab==0 ) return SQLITE_NOMEM;
  *ppVtab = &pVtab->base;
  pVtab->interp = (Tcl_Interp *)pAux;
#ifndef SQLITE_OMIT_VIRTUALTABLE
  sqlite3_declare_vtab(db, zSchema);
#endif
  return SQLITE_OK;
}
/* Note that for this virtual table, the xCreate and xConnect
** methods are identical. */
static int tclvarDisconnect(sqlite3_vtab *pVtab){
  free(pVtab);
  return SQLITE_OK;
}
/* The xDisconnect and xDestroy methods are also the same */

static int tclvarOpen(sqlite3_vtab *pVTab, sqlite3_vtab_cursor **ppCursor){
  tclvar_cursor *pCur;
  pCur = sqliteMalloc(sizeof(tclvar_cursor));
  *ppCursor = &pCur->base;
  return SQLITE_OK;
}

static int tclvarClose(sqlite3_vtab_cursor *cur){
  tclvar_cursor *pCur = (tclvar_cursor *)cur;
  if( pCur->pList1 ){
    Tcl_DecrRefCount(pCur->pList1);
  }
  if( pCur->pList2 ){
    Tcl_DecrRefCount(pCur->pList2);
  }
  sqliteFree(pCur);
  return SQLITE_OK;
}

static int tclvarNext(sqlite3_vtab_cursor *cur){
  tclvar_cursor *pCur = (tclvar_cursor *)cur;
  return 0;
}

static int tclvarColumn(sqlite3_vtab_cursor *cur, sqlite3_context *ctx, int i){
  tclvar_cursor *pCur = (tclvar_cursor*)cur;
  return SQLITE_OK;
}

static int tclvarRowid(sqlite3_vtab_cursor *cur, sqlite_int64 *pRowid){
  tclvar_cursor *pCur = (tclvar_cursor*)cur;
  return SQLITE_OK;
}

static int tclvarFilter(
  sqlite3_vtab_cursor *pVtabCursor, 
  int idxNum, const char *idxStr,
  int argc, sqlite3_value **argv
){
  tclvar_cursor *pCur = (tclvar_cursor *)pVtabCursor;
  tclvar_vtab *pVtab = (tclvar_vtab *)pCur->base.pVtab;
  return 0;
}

/*
*/
static int tclvarBestIndex(sqlite3_vtab *tab, sqlite3_index_info *pIdxInfo){
  tclvar_vtab *pVtab = (tclvar_vtab *)tab;
  return SQLITE_OK;
}

/*
** A virtual table module that merely echos method calls into TCL
** variables.
*/
static sqlite3_module tclvarModule = {
  0,                         /* iVersion */
  "tclvar",                  /* zName */
  tclvarConnect,
  tclvarConnect,
  tclvarBestIndex,
  tclvarDisconnect, 
  tclvarDisconnect,
  tclvarOpen,                  /* xOpen - open a cursor */
  tclvarClose,                 /* xClose - close a cursor */
  tclvarFilter,                /* xFilter - configure scan constraints */
  tclvarNext,                  /* xNext - advance a cursor */
  0,                           /* xEof - check for end of scan */
  tclvarColumn,                /* xColumn - read data */
  tclvarRowid                  /* xRowid - read data */
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
static int register_tclvar_module(
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
#ifndef SQLITE_OMIT_VIRTUALTABLE
  sqlite3_create_module(db, "tclvar", &tclvarModule, (void *)interp);
#endif
  return TCL_OK;
}


/*
** Register commands with the TCL interpreter.
*/
int Sqlitetesttclvar_Init(Tcl_Interp *interp){
  static struct {
     char *zName;
     Tcl_ObjCmdProc *xProc;
     void *clientData;
  } aObjCmd[] = {
     { "register_tclvar_module",   register_tclvar_module, 0 },
  };
  int i;
  for(i=0; i<sizeof(aObjCmd)/sizeof(aObjCmd[0]); i++){
    Tcl_CreateObjCommand(interp, aObjCmd[i].zName, 
        aObjCmd[i].xProc, aObjCmd[i].clientData, 0);
  }
  return TCL_OK;
}

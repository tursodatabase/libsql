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
** $Id: test8.c,v 1.10 2006/06/13 15:00:55 danielk1977 Exp $
*/
#include "sqliteInt.h"
#include "tcl.h"
#include "os.h"
#include <stdlib.h>
#include <string.h>

typedef struct echo_vtab echo_vtab;
typedef struct echo_cursor echo_cursor;

/* 
** An echo virtual-table object 
**
** If it is not NULL, the aHasIndex array is allocated so that it has
** the same number of entries as there are columns in the underlying
** real table.
*/
struct echo_vtab {
  sqlite3_vtab base;
  Tcl_Interp *interp;
  sqlite3 *db;
  char *zStmt;                 /* "SELECT rowid, * FROM <real-table-name> " */

  int *aIndex;
  int nCol;
  char **aCol;
};

/* An echo cursor object */
struct echo_cursor {
  sqlite3_vtab_cursor base;
  sqlite3_stmt *pStmt;
  int errcode;                 /* Error code */
};

static int getColumnNames(
  sqlite3 *db, 
  const char *zTab,
  char ***paCol, 
  int *pnCol
){
  char **aCol = 0;
  char zBuf[1024];
  sqlite3_stmt *pStmt = 0;
  int rc = SQLITE_OK;
  int nCol;

  sprintf(zBuf, "SELECT * FROM %s", zTab);
  rc = sqlite3_prepare(db, zBuf, -1, &pStmt, 0);
  if( rc==SQLITE_OK ){
    int ii;
    nCol = sqlite3_column_count(pStmt);
    aCol = sqliteMalloc(sizeof(char *) * nCol);
    if( !aCol ){
      rc = SQLITE_NOMEM;
      goto fail;
    }
    for(ii=0; ii<nCol; ii++){
      aCol[ii] = sqlite3StrDup(sqlite3_column_name(pStmt, ii));
      if( !aCol[ii] ){
        rc = SQLITE_NOMEM;
        goto fail;
      }
    }
  }

  *paCol = aCol;
  *pnCol = nCol;

fail:
  sqlite3_finalize(pStmt);
  if( rc!=SQLITE_OK && aCol ){
    int ii;
    for(ii=0; ii<nCol; ii++){
      sqliteFree(aCol[ii]);
    }
    sqliteFree(aCol);
  }
  return rc;
}

static int getIndexArray(sqlite3 *db, const char *zTab, int **paIndex){
  char zBuf[1024];
  sqlite3_stmt *pStmt = 0;
  int nCol;
  int *aIndex = 0;
  int rc;

  sprintf(zBuf, "SELECT * FROM %s", zTab);
  rc = sqlite3_prepare(db, zBuf, -1, &pStmt, 0);
  nCol = sqlite3_column_count(pStmt);

  sqlite3_finalize(pStmt);
  pStmt = 0;
  if( rc!=SQLITE_OK ){
    goto get_index_array_out;
  }

  aIndex = (int *)sqliteMalloc(sizeof(int) * nCol);
  if( !aIndex ){
    rc = SQLITE_NOMEM;
    goto get_index_array_out;
  }

  sprintf(zBuf, "PRAGMA index_list(%s)", zTab);
  rc = sqlite3_prepare(db, zBuf, -1, &pStmt, 0);

  while( pStmt && sqlite3_step(pStmt)==SQLITE_ROW ){
    sqlite3_stmt *pStmt2 = 0;
    sprintf(zBuf, "PRAGMA index_info(%s)", sqlite3_column_text(pStmt, 1));
    rc = sqlite3_prepare(db, zBuf, -1, &pStmt2, 0);
    if( pStmt2 && sqlite3_step(pStmt2)==SQLITE_ROW ){
      int cid = sqlite3_column_int(pStmt2, 1);
      assert( cid>=0 && cid<nCol );
      aIndex[cid] = 1;
    }
    rc = sqlite3_finalize(pStmt2);
    if( rc!=SQLITE_OK ){
      sqlite3_finalize(pStmt);
      goto get_index_array_out;
    }
  }

  rc = sqlite3_finalize(pStmt);

get_index_array_out:
  if( rc!=SQLITE_OK ){
    sqliteFree(aIndex);
    aIndex = 0;
  }
  *paIndex = aIndex;
  return rc;
}

/*
** Global Tcl variable $echo_module is a list. This routine appends
** the string element zArg to that list in interpreter interp.
*/
static void appendToEchoModule(Tcl_Interp *interp, const char *zArg){
  int flags = (TCL_APPEND_VALUE | TCL_LIST_ELEMENT | TCL_GLOBAL_ONLY);
  Tcl_SetVar(interp, "echo_module", (zArg?zArg:""), flags);
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
static int echoDeclareVtab(
  echo_vtab *pVtab, 
  sqlite3 *db, 
  int argc, 
  char **argv
){
  int rc = SQLITE_OK;

  if( argc==2 ){
    sqlite3_stmt *pStmt = 0;
    sqlite3_prepare(db, 
        "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = ?",
        -1, &pStmt, 0);
    sqlite3_bind_text(pStmt, 1, argv[1], -1, 0);
    if( sqlite3_step(pStmt)==SQLITE_ROW ){
      const char *zCreateTable = sqlite3_column_text(pStmt, 0);
#ifndef SQLITE_OMIT_VIRTUALTABLE
      sqlite3_declare_vtab(db, zCreateTable);
#endif
    } else {
      rc = SQLITE_ERROR;
    }
    sqlite3_finalize(pStmt);
    pVtab->zStmt = sqlite3MPrintf("SELECT rowid, * FROM %s ", argv[1]);
    if( rc==SQLITE_OK ){
      rc = getIndexArray(db, argv[1], &pVtab->aIndex);
    }
    if( rc==SQLITE_OK ){
      rc = getColumnNames(db, argv[1], &pVtab->aCol, &pVtab->nCol);
    }
  }

  return rc;
}

static int echoConstructor(
  sqlite3 *db,
  const sqlite3_module *pModule,
  int argc, char **argv,
  sqlite3_vtab **ppVtab
){
  int i;
  echo_vtab *pVtab;

  pVtab = sqliteMalloc( sizeof(*pVtab) );

  *ppVtab = &pVtab->base;
  pVtab->base.pModule = pModule;
  pVtab->interp = pModule->pAux;
  pVtab->db = db;
  for(i=0; i<argc; i++){
    appendToEchoModule(pVtab->interp, argv[i]);
  }

  echoDeclareVtab(pVtab, db, argc, argv);
  return 0;
}

/* Methods for the echo module */
static int echoCreate(
  sqlite3 *db,
  const sqlite3_module *pModule,
  int argc, char **argv,
  sqlite3_vtab **ppVtab
){
  appendToEchoModule((Tcl_Interp *)(pModule->pAux), "xCreate");
  return echoConstructor(db, pModule, argc, argv, ppVtab);
}
static int echoConnect(
  sqlite3 *db,
  const sqlite3_module *pModule,
  int argc, char **argv,
  sqlite3_vtab **ppVtab
){
  appendToEchoModule((Tcl_Interp *)(pModule->pAux), "xConnect");
  return echoConstructor(db, pModule, argc, argv, ppVtab);
}

static int echoDestructor(sqlite3_vtab *pVtab){
  int ii;
  echo_vtab *p = (echo_vtab*)pVtab;
  sqliteFree(p->zStmt);
  sqliteFree(p->aIndex);
  for(ii=0; ii<p->nCol; ii++){
    sqliteFree(p->aCol[ii]);
  }
  sqliteFree(p->aCol);
  sqliteFree(p);
  return 0;
}

static int echoDisconnect(sqlite3_vtab *pVtab){
  appendToEchoModule(((echo_vtab *)pVtab)->interp, "xDisconnect");
  return echoDestructor(pVtab);
}
static int echoDestroy(sqlite3_vtab *pVtab){
  appendToEchoModule(((echo_vtab *)pVtab)->interp, "xDestroy");
  return echoDestructor(pVtab);
}

static int echoOpen(sqlite3_vtab *pVTab, sqlite3_vtab_cursor **ppCursor){
  echo_cursor *pCur;
  pCur = sqliteMalloc(sizeof(echo_cursor));
  *ppCursor = (sqlite3_vtab_cursor *)pCur;
  return SQLITE_OK;
}

static int echoClose(sqlite3_vtab_cursor *cur){
  echo_cursor *pCur = (echo_cursor *)cur;
  sqlite3_finalize(pCur->pStmt);
  sqliteFree(pCur);
  return SQLITE_OK;
}

static int echoNext(sqlite3_vtab_cursor *cur){
  int rc;
  echo_cursor *pCur = (echo_cursor *)cur;

  rc = sqlite3_step(pCur->pStmt);

  if( rc==SQLITE_ROW ){
    rc = 1;
  } else {
    pCur->errcode = sqlite3_finalize(pCur->pStmt);
    pCur->pStmt = 0;
    rc = 0;
  }

  return rc;
}

static int echoColumn(sqlite3_vtab_cursor *cur, sqlite3_context *ctx, int i){
  int iCol = i + 1;
  sqlite3_stmt *pStmt = ((echo_cursor *)cur)->pStmt;

  assert( sqlite3_data_count(pStmt)>iCol );
  switch( sqlite3_column_type(pStmt, iCol) ){
    case SQLITE_INTEGER:
      sqlite3_result_int64(ctx, sqlite3_column_int64(pStmt, iCol));
      break;
    case SQLITE_FLOAT:
      sqlite3_result_double(ctx, sqlite3_column_double(pStmt, iCol));
      break;
    case SQLITE_TEXT:
      sqlite3_result_text(ctx, 
          sqlite3_column_text(pStmt, iCol),
          sqlite3_column_bytes(pStmt, iCol),
          SQLITE_TRANSIENT
      );
      break;
    case SQLITE_BLOB:
      sqlite3_result_blob(ctx, 
          sqlite3_column_blob(pStmt, iCol),
          sqlite3_column_bytes(pStmt, iCol),
          SQLITE_TRANSIENT
      );
      break;
  }
  return SQLITE_OK;
}

static int echoRowid(sqlite3_vtab_cursor *cur, sqlite_int64 *pRowid){
  sqlite3_stmt *pStmt = ((echo_cursor *)cur)->pStmt;
  *pRowid = sqlite3_column_int64(pStmt, 0);
  return SQLITE_OK;
}


static int echoFilter(
  sqlite3_vtab_cursor *pVtabCursor, 
  char *zPlan, int nPlan,
  int argc, 
  sqlite3_value **argv
){
  int rc;
  int ii;

  echo_cursor *pCur = (echo_cursor *)pVtabCursor;
  echo_vtab *pVtab = (echo_vtab *)pVtabCursor->pVtab;
  sqlite3 *db = pVtab->db;

  appendToEchoModule(pVtab->interp, "xFilter");
  appendToEchoModule(pVtab->interp, zPlan);
  for(ii=0; ii<argc; ii++){
    appendToEchoModule(pVtab->interp, sqlite3_value_text(argv[ii]));
  }

  sqlite3_finalize(pCur->pStmt);
  pCur->pStmt = 0;
  rc = sqlite3_prepare(db, pVtab->zStmt, -1, &pCur->pStmt, 0);

  if( rc==SQLITE_OK ){
    rc = echoNext(pVtabCursor);
  }

  return rc;
}

/*
** The echo module implements the subset of query constraints and sort
** orders that may take advantage of SQLite indices on the underlying
** real table. For example, if the real table is declared as:
**
**     CREATE TABLE real(a, b, c);
**     CREATE INDEX real_index ON real(b);
**
** then the echo module handles WHERE or ORDER BY clauses that refer
** to the column "b", but not "a" or "c". If a multi-column index is
** present, only it's left most column is considered. 
*/
static int echoBestIndex(sqlite3_vtab *tab, sqlite3_index_info *pIdxInfo){
  int ii;
  char *zWhere = 0;
  char *zOrder = 0;
  char *zPlan = 0;
  int nPlan = 0;
  int nArg = 0;
  echo_vtab *pVtab = (echo_vtab *)tab;

  for(ii=0; ii<pIdxInfo->nConstraint; ii++){
    const struct sqlite3_index_constraint *pConstraint;
    struct sqlite3_index_constraint_usage *pUsage;

    pConstraint = &pIdxInfo->aConstraint[ii];
    pUsage = &pIdxInfo->aConstraintUsage[ii];

    int iCol = pConstraint->iColumn;
    if( pVtab->aIndex[iCol] ){
      char *zCol = pVtab->aCol[iCol];
      char *zOp = 0;
      switch( pConstraint->op ){
        case SQLITE_INDEX_CONSTRAINT_EQ:
          zOp = "="; break;
        case SQLITE_INDEX_CONSTRAINT_LT:
          zOp = "<"; break;
        case SQLITE_INDEX_CONSTRAINT_GT:
          zOp = ">"; break;
        case SQLITE_INDEX_CONSTRAINT_LE:
          zOp = "<="; break;
        case SQLITE_INDEX_CONSTRAINT_GE:
          zOp = ">="; break;
        case SQLITE_INDEX_CONSTRAINT_MATCH:
          zOp = "MATCH"; break;
      }
      if( zWhere ){
        char *zTmp = zWhere;
        zWhere = sqlite3MPrintf("%s AND %s %s ?", zWhere, zCol, zOp);
        sqliteFree(zTmp);
      } else {
        zWhere = sqlite3MPrintf("WHERE %s %s ?", zCol, zOp);
      }

      pUsage->argvIndex = ++nArg;
      pUsage->omit = 1;
    }
  }

  appendToEchoModule(pVtab->interp, "xBestIndex");;
  appendToEchoModule(pVtab->interp, zWhere);
  appendToEchoModule(pVtab->interp, zOrder);

  nPlan = 2;
  if( zWhere ){
    nPlan += strlen(zWhere);
  }
  if( zOrder ){
    nPlan += strlen(zWhere);
  }
  zPlan = sqlite3_allocate_queryplan(pIdxInfo, nPlan);
  if( zPlan ){
    sprintf(zPlan, "%s%s%s", 
        zWhere?zWhere:"", (zOrder&&zWhere)?" ":"", zOrder?zOrder:"");
  }

  sqliteFree(zWhere);
  sqliteFree(zOrder);

  return SQLITE_OK;
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
  echoBestIndex,
  echoDisconnect, 
  echoDestroy,
  echoOpen,                  /* xOpen - open a cursor */
  echoClose,                 /* xClose - close a cursor */
  echoFilter,                /* xFilter - configure scan constraints */
  echoNext,                  /* xNext - advance a cursor */
  echoColumn,                /* xColumn - read data */
  echoRowid                  /* xRowid - read data */
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

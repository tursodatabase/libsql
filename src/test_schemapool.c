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
*/

/*
** None of this works unless we have virtual tables.
*/
#if !defined(SQLITE_OMIT_VIRTUALTABLE) && defined(SQLITE_TEST)

#include "sqliteInt.h"
#include <tcl.h>

/* The code in this file defines a sqlite3 virtual-table module with
** the following schema.
*/
#define SCHEMAPOOL_SCHEMA \
"CREATE TABLE x("         \
"  cksum   INTEGER, "     \
"  nref    INTEGER, "     \
"  nschema INTEGER, "     \
"  ndelete INTEGER  "     \
")"

#define SCHEMAPOOL_NFIELD 4

typedef struct schemapool_vtab schemapool_vtab;
typedef struct schemapool_cursor schemapool_cursor;

/* A schema table object */
struct schemapool_vtab {
  sqlite3_vtab base;
};

/* A schema table cursor object */
struct schemapool_cursor {
  sqlite3_vtab_cursor base;
  sqlite3_int64 *aData;
  int iRow;
  int nRow;
};

/*
** Table destructor for the schema module.
*/
static int schemaPoolDestroy(sqlite3_vtab *pVtab){
  sqlite3_free(pVtab);
  return 0;
}

/*
** Table constructor for the schema module.
*/
static int schemaPoolCreate(
  sqlite3 *db,
  void *pAux,
  int argc, const char *const*argv,
  sqlite3_vtab **ppVtab,
  char **pzErr
){
  int rc = SQLITE_NOMEM;
  schemapool_vtab *pVtab = sqlite3_malloc(sizeof(schemapool_vtab));
  if( pVtab ){
    memset(pVtab, 0, sizeof(schemapool_vtab));
    rc = sqlite3_declare_vtab(db, SCHEMAPOOL_SCHEMA);
    if( rc!=SQLITE_OK ){
      sqlite3_free(pVtab);
      pVtab = 0;
    }
  }
  *ppVtab = (sqlite3_vtab *)pVtab;
  return rc;
}

/*
** Open a new cursor on the schema table.
*/
static int schemaPoolOpen(sqlite3_vtab *pVTab, sqlite3_vtab_cursor **ppCursor){
  int rc = SQLITE_NOMEM;
  schemapool_cursor *pCur;
  pCur = sqlite3_malloc(sizeof(schemapool_cursor));
  if( pCur ){
    memset(pCur, 0, sizeof(schemapool_cursor));
    *ppCursor = (sqlite3_vtab_cursor*)pCur;
    rc = SQLITE_OK;
  }
  return rc;
}

/*
** Close a schema table cursor.
*/
static int schemaPoolClose(sqlite3_vtab_cursor *cur){
  schemapool_cursor *pCur = (schemapool_cursor*)cur;
  sqlite3_free(pCur->aData);
  sqlite3_free(pCur);
  return SQLITE_OK;
}

/*
** Retrieve a column of data.
*/
static int schemaPoolColumn(sqlite3_vtab_cursor *cur, sqlite3_context *ctx, int i){
  schemapool_cursor *pCur = (schemapool_cursor*)cur;
  assert( i==0 || i==1 || i==2 || i==3 );
  sqlite3_result_int64(ctx, pCur->aData[pCur->iRow*SCHEMAPOOL_NFIELD + i]);
  return SQLITE_OK;
}

/*
** Retrieve the current rowid.
*/
static int schemaPoolRowid(sqlite3_vtab_cursor *cur, sqlite_int64 *pRowid){
  schemapool_cursor *pCur = (schemapool_cursor*)cur;
  *pRowid = pCur->iRow + 1;
  return SQLITE_OK;
}

static int schemaPoolEof(sqlite3_vtab_cursor *cur){
  schemapool_cursor *pCur = (schemapool_cursor*)cur;
  return pCur->iRow>=pCur->nRow;
}

/*
** Advance the cursor to the next row.
*/
static int schemaPoolNext(sqlite3_vtab_cursor *cur){
  schemapool_cursor *pCur = (schemapool_cursor*)cur;
  pCur->iRow++;
  return SQLITE_OK;
}

struct SchemaPool {
  int nRef;                       /* Number of pointers to this object */
  int nDelete;                    /* Schema objects deleted by ReleaseAll() */
  u64 cksum;                      /* Checksum for this Schema contents */
  Schema *pSchema;                /* Linked list of Schema objects */
  Schema sSchema;                 /* The single dummy schema object */
  SchemaPool *pNext;              /* Next element in schemaPoolList */
};
extern SchemaPool *sqlite3SchemaPoolList(void);

/*
** Reset a schemaPool table cursor.
*/
static int schemaPoolFilter(
  sqlite3_vtab_cursor *pVtabCursor, 
  int idxNum, const char *idxStr,
  int argc, sqlite3_value **argv
){
  SchemaPool *pSPool;
  schemapool_cursor *pCur = (schemapool_cursor*)pVtabCursor;

  sqlite3_free(pCur->aData);
  pCur->aData = 0;
  pCur->nRow = 0;
  pCur->iRow = 0;

  for(pSPool = sqlite3SchemaPoolList(); pSPool; pSPool=pSPool->pNext){
    pCur->nRow++;
  }

  if( pCur->nRow ){
    int iRow = 0;
    int nByte = SCHEMAPOOL_NFIELD * pCur->nRow * sizeof(i64);
    pCur->aData = (i64*)sqlite3_malloc(nByte);
    if( pCur->aData==0 ) return SQLITE_NOMEM;
    for(pSPool = sqlite3SchemaPoolList(); pSPool; pSPool=pSPool->pNext){
      Schema *p;
      i64 nSchema = 0;
      for(p=pSPool->pSchema; p; p=p->pNext){
        nSchema++;
      }
      pCur->aData[0 + iRow*SCHEMAPOOL_NFIELD] = pSPool->cksum;
      pCur->aData[1 + iRow*SCHEMAPOOL_NFIELD] = (i64)pSPool->nRef;
      pCur->aData[2 + iRow*SCHEMAPOOL_NFIELD] = nSchema;
      pCur->aData[3 + iRow*SCHEMAPOOL_NFIELD] = (i64)pSPool->nDelete;
      iRow++;
    }
  }

  return SQLITE_OK;
}

/*
** Analyse the WHERE condition.
*/
static int schemaPoolBestIndex(sqlite3_vtab *tab, sqlite3_index_info *pIdxInfo){
  return SQLITE_OK;
}

/*
** A virtual table module that merely echos method calls into TCL
** variables.
*/
static sqlite3_module schemaPoolModule = {
  0,                           /* iVersion */
  schemaPoolCreate,
  schemaPoolCreate,
  schemaPoolBestIndex,
  schemaPoolDestroy,
  schemaPoolDestroy,
  schemaPoolOpen,              /* xOpen - open a cursor */
  schemaPoolClose,             /* xClose - close a cursor */
  schemaPoolFilter,            /* xFilter - configure scan constraints */
  schemaPoolNext,              /* xNext - advance a cursor */
  schemaPoolEof,               /* xEof */
  schemaPoolColumn,            /* xColumn - read data */
  schemaPoolRowid,             /* xRowid - read data */
  0,                           /* xUpdate */
  0,                           /* xBegin */
  0,                           /* xSync */
  0,                           /* xCommit */
  0,                           /* xRollback */
  0,                           /* xFindMethod */
  0,                           /* xRename */
};

/*
** Decode a pointer to an sqlite3 object.
*/
extern int getDbPointer(Tcl_Interp *interp, const char *zA, sqlite3 **ppDb);

/*
** Register the schema virtual table module.
*/
static int SQLITE_TCLAPI register_schemapool_module(
  ClientData clientData, /* Not used */
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
  sqlite3_create_module(db, "schemapool", &schemaPoolModule, 0);
#endif
  return TCL_OK;
}

#endif

/*
** Register commands with the TCL interpreter.
*/
int Sqlitetestschemapool_Init(Tcl_Interp *interp){
  static struct {
     char *zName;
     Tcl_ObjCmdProc *xProc;
     void *clientData;
  } aObjCmd[] = {
     { "register_schemapool_module", register_schemapool_module, 0 },
  };
  int i;
  for(i=0; i<sizeof(aObjCmd)/sizeof(aObjCmd[0]); i++){
    Tcl_CreateObjCommand(interp, aObjCmd[i].zName, 
        aObjCmd[i].xProc, aObjCmd[i].clientData, 0);
  }
  return TCL_OK;
}


/*
** 2013 Jan 11
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
** The FS virtual table is created as follows:
**
**   CREATE VIRTUAL TABLE tbl USING fs(idx);
**
** where idx is the name of a table in the db with 2 columns.  The virtual
** table also has two columns - file path and file contents.
**
** The first column of table idx must be an IPK, and the second contains file
** paths. For example:
**
**   CREATE TABLE idx(id INTEGER PRIMARY KEY, path TEXT);
**   INSERT INTO idx VALUES(4, '/etc/passwd');
**
** Adding the row to the idx table automatically creates a row in the 
** virtual table with rowid=4, path=/etc/passwd and a text field that 
** contains data read from file /etc/passwd on disk.
*/
#include "sqliteInt.h"
#include "tcl.h"

#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#if SQLITE_OS_UNIX
# include <unistd.h>
#endif
#if SQLITE_OS_WIN
# include <io.h>
#endif

#ifndef SQLITE_OMIT_VIRTUALTABLE

typedef struct fs_vtab fs_vtab;
typedef struct fs_cursor fs_cursor;

/* 
** A fs virtual-table object 
*/
struct fs_vtab {
  sqlite3_vtab base;
  sqlite3 *db;
  char *zDb;                      /* Name of db containing zTbl */
  char *zTbl;                     /* Name of docid->file map table */
};

/* A fs cursor object */
struct fs_cursor {
  sqlite3_vtab_cursor base;
  sqlite3_stmt *pStmt;
  char *zBuf;
  int nBuf;
  int nAlloc;
};

/*
** This function is the implementation of both the xConnect and xCreate
** methods of the fs virtual table.
**
** The argv[] array contains the following:
**
**   argv[0]   -> module name  ("fs")
**   argv[1]   -> database name
**   argv[2]   -> table name
**   argv[...] -> other module argument fields.
*/
static int fsConnect(
  sqlite3 *db,
  void *pAux,
  int argc, const char *const*argv,
  sqlite3_vtab **ppVtab,
  char **pzErr
){
  fs_vtab *pVtab;
  int nByte;
  const char *zTbl;
  const char *zDb = argv[1];

  if( argc!=4 ){
    *pzErr = sqlite3_mprintf("wrong number of arguments");
    return SQLITE_ERROR;
  }
  zTbl = argv[3];

  nByte = sizeof(fs_vtab) + (int)strlen(zTbl) + 1 + (int)strlen(zDb) + 1;
  pVtab = (fs_vtab *)sqlite3MallocZero( nByte );
  if( !pVtab ) return SQLITE_NOMEM;

  pVtab->zTbl = (char *)&pVtab[1];
  pVtab->zDb = &pVtab->zTbl[strlen(zTbl)+1];
  pVtab->db = db;
  memcpy(pVtab->zTbl, zTbl, strlen(zTbl));
  memcpy(pVtab->zDb, zDb, strlen(zDb));
  *ppVtab = &pVtab->base;
  sqlite3_declare_vtab(db, "CREATE TABLE xyz(path TEXT, data TEXT)");

  return SQLITE_OK;
}
/* Note that for this virtual table, the xCreate and xConnect
** methods are identical. */

static int fsDisconnect(sqlite3_vtab *pVtab){
  sqlite3_free(pVtab);
  return SQLITE_OK;
}
/* The xDisconnect and xDestroy methods are also the same */

/*
** Open a new fs cursor.
*/
static int fsOpen(sqlite3_vtab *pVTab, sqlite3_vtab_cursor **ppCursor){
  fs_cursor *pCur;
  pCur = sqlite3MallocZero(sizeof(fs_cursor));
  *ppCursor = &pCur->base;
  return SQLITE_OK;
}

/*
** Close a fs cursor.
*/
static int fsClose(sqlite3_vtab_cursor *cur){
  fs_cursor *pCur = (fs_cursor *)cur;
  sqlite3_finalize(pCur->pStmt);
  sqlite3_free(pCur->zBuf);
  sqlite3_free(pCur);
  return SQLITE_OK;
}

static int fsNext(sqlite3_vtab_cursor *cur){
  fs_cursor *pCur = (fs_cursor *)cur;
  int rc;

  rc = sqlite3_step(pCur->pStmt);
  if( rc==SQLITE_ROW || rc==SQLITE_DONE ) rc = SQLITE_OK;

  return rc;
}

static int fsFilter(
  sqlite3_vtab_cursor *pVtabCursor, 
  int idxNum, const char *idxStr,
  int argc, sqlite3_value **argv
){
  int rc;
  fs_cursor *pCur = (fs_cursor *)pVtabCursor;
  fs_vtab *p = (fs_vtab *)(pVtabCursor->pVtab);

  assert( (idxNum==0 && argc==0) || (idxNum==1 && argc==1) );
  if( idxNum==1 ){
    char *zStmt = sqlite3_mprintf(
        "SELECT * FROM %Q.%Q WHERE rowid=?", p->zDb, p->zTbl);
    if( !zStmt ) return SQLITE_NOMEM;
    rc = sqlite3_prepare_v2(p->db, zStmt, -1, &pCur->pStmt, 0);
    sqlite3_free(zStmt);
    if( rc==SQLITE_OK ){
      sqlite3_bind_value(pCur->pStmt, 1, argv[0]);
    }
  }else{
    char *zStmt = sqlite3_mprintf("SELECT * FROM %Q.%Q", p->zDb, p->zTbl);
    if( !zStmt ) return SQLITE_NOMEM;
    rc = sqlite3_prepare_v2(p->db, zStmt, -1, &pCur->pStmt, 0);
    sqlite3_free(zStmt);
  }

  if( rc==SQLITE_OK ){
    rc = fsNext(pVtabCursor); 
  }
  return rc;
}

static int fsColumn(sqlite3_vtab_cursor *cur, sqlite3_context *ctx, int i){
  fs_cursor *pCur = (fs_cursor*)cur;

  assert( i==0 || i==1 );
  if( i==0 ){
    sqlite3_result_value(ctx, sqlite3_column_value(pCur->pStmt, 0));
  }else{
    const char *zFile = (const char *)sqlite3_column_text(pCur->pStmt, 1);
    struct stat sbuf;
    int fd;

    fd = open(zFile, O_RDONLY);
    if( fd<0 ) return SQLITE_IOERR;
    fstat(fd, &sbuf);

    if( sbuf.st_size>=pCur->nAlloc ){
      int nNew = sbuf.st_size*2;
      char *zNew;
      if( nNew<1024 ) nNew = 1024;

      zNew = sqlite3Realloc(pCur->zBuf, nNew);
      if( zNew==0 ){
        close(fd);
        return SQLITE_NOMEM;
      }
      pCur->zBuf = zNew;
      pCur->nAlloc = nNew;
    }

    read(fd, pCur->zBuf, sbuf.st_size);
    close(fd);
    pCur->nBuf = sbuf.st_size;
    pCur->zBuf[pCur->nBuf] = '\0';

    sqlite3_result_text(ctx, pCur->zBuf, -1, SQLITE_TRANSIENT);
  }
  return SQLITE_OK;
}

static int fsRowid(sqlite3_vtab_cursor *cur, sqlite_int64 *pRowid){
  fs_cursor *pCur = (fs_cursor*)cur;
  *pRowid = sqlite3_column_int64(pCur->pStmt, 0);
  return SQLITE_OK;
}

static int fsEof(sqlite3_vtab_cursor *cur){
  fs_cursor *pCur = (fs_cursor*)cur;
  return (sqlite3_data_count(pCur->pStmt)==0);
}

static int fsBestIndex(sqlite3_vtab *tab, sqlite3_index_info *pIdxInfo){
  int ii;

  for(ii=0; ii<pIdxInfo->nConstraint; ii++){
    struct sqlite3_index_constraint const *pCons = &pIdxInfo->aConstraint[ii];
    if( pCons->iColumn<0 && pCons->usable
           && pCons->op==SQLITE_INDEX_CONSTRAINT_EQ ){
      struct sqlite3_index_constraint_usage *pUsage;
      pUsage = &pIdxInfo->aConstraintUsage[ii];
      pUsage->omit = 0;
      pUsage->argvIndex = 1;
      pIdxInfo->idxNum = 1;
      pIdxInfo->estimatedCost = 1.0;
      break;
    }
  }

  return SQLITE_OK;
}

/*
** A virtual table module that provides read-only access to a
** Tcl global variable namespace.
*/
static sqlite3_module fsModule = {
  0,                         /* iVersion */
  fsConnect,
  fsConnect,
  fsBestIndex,
  fsDisconnect, 
  fsDisconnect,
  fsOpen,                      /* xOpen - open a cursor */
  fsClose,                     /* xClose - close a cursor */
  fsFilter,                    /* xFilter - configure scan constraints */
  fsNext,                      /* xNext - advance a cursor */
  fsEof,                       /* xEof - check for end of scan */
  fsColumn,                    /* xColumn - read data */
  fsRowid,                     /* xRowid - read data */
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
** Register the echo virtual table module.
*/
static int register_fs_module(
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
  sqlite3_create_module(db, "fs", &fsModule, (void *)interp);
#endif
  return TCL_OK;
}

#endif


/*
** Register commands with the TCL interpreter.
*/
int Sqlitetestfs_Init(Tcl_Interp *interp){
#ifndef SQLITE_OMIT_VIRTUALTABLE
  static struct {
     char *zName;
     Tcl_ObjCmdProc *xProc;
     void *clientData;
  } aObjCmd[] = {
     { "register_fs_module",   register_fs_module, 0 },
  };
  int i;
  for(i=0; i<sizeof(aObjCmd)/sizeof(aObjCmd[0]); i++){
    Tcl_CreateObjCommand(interp, aObjCmd[i].zName, 
        aObjCmd[i].xProc, aObjCmd[i].clientData, 0);
  }
#endif
  return TCL_OK;
}

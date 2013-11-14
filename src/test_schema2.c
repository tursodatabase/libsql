/*
** 2013-11-14
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
******************************************************************************
**
** This file contains an implementation of the "schema2" virtual table
** for displaying the content of various internal objects associated with
** the parsed schema.
*/

#ifndef SQLITE_AMALGAMATION
# include "sqliteInt.h"
#endif

#ifndef SQLITE_OMIT_VIRTUALTABLE

static const char azSchema2[] = 
  "CREATE TABLE x("
  "  dbname     STRING,"  /* Name of attached database */
  "  tblname    STRING,"  /* Name of the table */
  "  idxname    STRING,"  /* Index name or NULL if not applicable */
  "  cnum       INT,"     /* Column number or NULL if not applicable */
  "  attr       STRING,"  /* Attribute name */
  "  value      STRING "  /* Attribute value */
  ");"
;

typedef struct Schema2Table Schema2Table;
typedef struct Schema2Cursor Schema2Cursor;
typedef struct Schema2Row Schema2Row;

/*
** A single row of the result
*/
struct Schema2Row {
  const char *zDb;        /* Schema name.  db->aDb[].zName */
  const char *zTbl;       /* Table name.  Might be NULL */
  const char *zIdx;       /* Index name.  Might be NULL */
  int iCol;               /* Column number */
  const char *zAttr;      /* Attribute */
  char *zValue;           /* Value of the attribute */
  Schema2Row *pNext;      /* Next row */
};

/*
** A cursor for iterating through internal schema information
*/
struct Schema2Cursor {
  sqlite3_vtab_cursor base;       /* Base class.  Must be first */
  int iRowid;                     /* Current rowid */
  Schema2Row *pAll;               /* All rows */
  Schema2Row *pCurrent;           /* Current row */
  Schema2Row *pLast;              /* Last row */
};

/* 
** The complete Schema2 virtual table
*/
struct Schema2Table {
  sqlite3_vtab base;      /* Base class.   Must be first */
  sqlite3 *db;            /* The database connection that owns this table */
};

/*
** Connect to or create an Schema2  virtual table.
*/
static int schema2Connect(
  sqlite3 *db,
  void *pAux,
  int argc, const char *const*argv,
  sqlite3_vtab **ppVtab,
  char **pzErr
){
  Schema2Table *pTab;

  pTab = (Schema2Table *)sqlite3_malloc(sizeof(Schema2Table));
  memset(pTab, 0, sizeof(Schema2Table));
  pTab->db = db;

  sqlite3_declare_vtab(db, azSchema2);
  *ppVtab = &pTab->base;
  return SQLITE_OK;
}

/*
** Disconnect from or destroy an Schema2 virtual table.
*/
static int schema2Disconnect(sqlite3_vtab *pVtab){
  sqlite3_free(pVtab);
  return SQLITE_OK;
}

/*
** There is no "best-index". This virtual table always does a complete
** scan.
*/
static int schema2BestIndex(sqlite3_vtab *tab, sqlite3_index_info *pIdxInfo){
  pIdxInfo->estimatedCost = 10.0;
  return SQLITE_OK;
}

/*
** Open a new Schema2 cursor.
*/
static int schema2Open(sqlite3_vtab *pVTab, sqlite3_vtab_cursor **ppCursor){
  Schema2Cursor *pCsr;
  pCsr = (Schema2Cursor *)sqlite3_malloc(sizeof(Schema2Cursor));
  memset(pCsr, 0, sizeof(Schema2Cursor));
  pCsr->base.pVtab = pVTab;
  *ppCursor = (sqlite3_vtab_cursor *)pCsr;
  return SQLITE_OK;
}

static void schema2ResetCsr(Schema2Cursor *pCsr){
  Schema2Row *pRow, *pNextRow;
  for(pRow=pCsr->pAll; pRow; pRow=pNextRow){
    pNextRow = pRow->pNext;
    sqlite3_free(pRow->zValue);
    sqlite3_free(pRow);
  }
  pCsr->pAll = 0;
  pCsr->pCurrent = 0;
  pCsr->pLast = 0;
  pCsr->iRowid = 0;
}

/*
** Close a statvfs cursor.
*/
static int schema2Close(sqlite3_vtab_cursor *pCursor){
  Schema2Cursor *pCsr = (Schema2Cursor *)pCursor;
  schema2ResetCsr(pCsr);
  sqlite3_free(pCsr);
  return SQLITE_OK;
}

/*
** Move a statvfs cursor to the next entry.
*/
static int schema2Next(sqlite3_vtab_cursor *pCursor){
  Schema2Cursor *pCsr = (Schema2Cursor *)pCursor;
  if( pCsr->pCurrent==0 ){
    pCsr->pCurrent = pCsr->pAll;
  }else{
    pCsr->pCurrent = pCsr->pCurrent->pNext;
  }
  pCsr->iRowid++;
  return SQLITE_OK;
}

static int schema2Eof(sqlite3_vtab_cursor *pCursor){
  Schema2Cursor *pCsr = (Schema2Cursor *)pCursor;
  return pCsr->pCurrent==0 && pCsr->iRowid>0;
}

/* Append a single to the cursor pCsr */
static void schema2AppendRow(
  Schema2Cursor *pCsr,
  const char *zDb,
  const char *zTbl,
  const char *zIdx,
  int iCol,
  const char *zAttr,
  const char *zValue,
  ...
){
  Schema2Row *pRow = sqlite3_malloc( sizeof(*pRow) );
  va_list ap;
  if( pRow==0 );
  pRow->zDb = zDb;
  pRow->zTbl = zTbl;
  pRow->zIdx = zIdx;
  pRow->iCol = iCol;
  pRow->zAttr = zAttr;
  va_start(ap, zValue);
  pRow->zValue = sqlite3_vmprintf(zValue, ap);
  va_end(ap);
  if( pCsr->pLast==0 ){
    pCsr->pAll = pRow;
  }else{
    pCsr->pLast->pNext = pRow;
  }
  pCsr->pLast = pRow;
}

/* Append rows for index pIdx of table pTab which is part of the zDb schema */
static void schema2AppendIndex(
  Schema2Cursor *pCsr,
  const char *zDb,
  Table *pTab,
  Index *pIdx
){
  const char *zTbl = pTab->zName;
  const char *zIdx = pIdx->zName;
  int i;
  schema2AppendRow(pCsr,zDb,zTbl,zIdx,-1,"zColAff","%s",pIdx->zColAff);
  schema2AppendRow(pCsr,zDb,zTbl,zIdx,-1,"tnum","%d",pIdx->tnum);
  schema2AppendRow(pCsr,zDb,zTbl,zIdx,-1,"szIdxRow","%d",pIdx->szIdxRow);
  schema2AppendRow(pCsr,zDb,zTbl,zIdx,-1,"nKeyCol","%d",pIdx->nKeyCol);
  schema2AppendRow(pCsr,zDb,zTbl,zIdx,-1,"nColumn","%d",pIdx->nColumn);
  schema2AppendRow(pCsr,zDb,zTbl,zIdx,-1,"onError","%d",pIdx->onError);
  schema2AppendRow(pCsr,zDb,zTbl,zIdx,-1,"autoIndex","%d",pIdx->autoIndex);
  schema2AppendRow(pCsr,zDb,zTbl,zIdx,-1,"bUnordered","%d",pIdx->bUnordered);
  schema2AppendRow(pCsr,zDb,zTbl,zIdx,-1,"uniqNotNull","%d",pIdx->uniqNotNull);
  schema2AppendRow(pCsr,zDb,zTbl,zIdx,-1,"isResized","%d",pIdx->isResized);
  schema2AppendRow(pCsr,zDb,zTbl,zIdx,-1,"isCovering","%d",pIdx->isCovering);
  for(i=0; i<pIdx->nColumn; i++){
    i16 x = pIdx->aiColumn[i];
    schema2AppendRow(pCsr,zDb,zTbl,zIdx,i,"zName","%s",
                     x>=0?pTab->aCol[x].zName:"rowid");
    schema2AppendRow(pCsr,zDb,zTbl,zIdx,i,"aiRowEst","%lld",
                     (sqlite3_int64)pIdx->aiRowEst[i]);
  }
}

/* Append rows for table pTab which is part of the zDb schema */
static void schema2AppendTable(
  Schema2Cursor *pCsr,
  const char *zDb,
  Table *pTab
){
  const char *zTbl = pTab->zName;
  int i;
  Index *pIdx;
  schema2AppendRow(pCsr,zDb,zTbl,0,-1,"zColAff","%s",pTab->zColAff);
  schema2AppendRow(pCsr,zDb,zTbl,0,-1,"nRowEst",
                   "%lld",(sqlite3_int64)pTab->nRowEst);
  schema2AppendRow(pCsr,zDb,zTbl,0,-1,"tnum","%d",pTab->tnum);
  schema2AppendRow(pCsr,zDb,zTbl,0,-1,"iPKey","%d",pTab->iPKey);
  schema2AppendRow(pCsr,zDb,zTbl,0,-1,"nCol","%d",pTab->nCol);
  schema2AppendRow(pCsr,zDb,zTbl,0,-1,"nRef","%d",pTab->nRef);
  schema2AppendRow(pCsr,zDb,zTbl,0,-1,"szTabRow","%d",pTab->szTabRow);
  schema2AppendRow(pCsr,zDb,zTbl,0,-1,"tabFlags","%d",pTab->tabFlags);
  for(i=0; i<pTab->nCol; i++){
    const Column *p = pTab->aCol + i;
    schema2AppendRow(pCsr,zDb,zTbl,0,i,"zName","%s",p->zName);
    schema2AppendRow(pCsr,zDb,zTbl,0,i,"zDflt","%s",p->zDflt);
    schema2AppendRow(pCsr,zDb,zTbl,0,i,"zType","%s",p->zType);
    schema2AppendRow(pCsr,zDb,zTbl,0,i,"zColl","%s",p->zColl);
    schema2AppendRow(pCsr,zDb,zTbl,0,i,"notNull","%d",p->notNull);
    schema2AppendRow(pCsr,zDb,zTbl,0,i,"affinity","%c",p->affinity);
    schema2AppendRow(pCsr,zDb,zTbl,0,i,"szEst","%d",p->szEst);
    schema2AppendRow(pCsr,zDb,zTbl,0,i,"colFlags","%04x",p->colFlags);
  }
  for(pIdx=pTab->pIndex; pIdx; pIdx=pIdx->pNext){
    schema2AppendIndex(pCsr, zDb, pTab, pIdx);
  }
}


/* Append rows for schema object pSchema */
static void schema2AppendSchema(
  Schema2Cursor *pCsr,
  const char *zDb,
  Schema *pSchema
){
  HashElem *i;
  schema2AppendRow(pCsr,zDb,0,0,-1,"generation","%d", pSchema->iGeneration);
  schema2AppendRow(pCsr,zDb,0,0,-1,"file_format","%d", pSchema->file_format);
  schema2AppendRow(pCsr,zDb,0,0,-1,"enc","%d", pSchema->enc);
  schema2AppendRow(pCsr,zDb,0,0,-1,"flags","%d", pSchema->flags);
  schema2AppendRow(pCsr,zDb,0,0,-1,"cache_size","%d", pSchema->cache_size);
  for(i=sqliteHashFirst(&pSchema->tblHash); i; i=sqliteHashNext(i)){
    schema2AppendTable(pCsr, zDb, (Table*)sqliteHashData(i));
  }
}


static int schema2Filter(
  sqlite3_vtab_cursor *pCursor, 
  int idxNum, const char *idxStr,
  int argc, sqlite3_value **argv
){
  int iDb;
  Schema2Cursor *pCsr = (Schema2Cursor*)pCursor;
  Schema2Table *pTab = (Schema2Table*)(pCursor->pVtab);
  sqlite3 *db = pTab->db;
  schema2ResetCsr(pCsr);
  for(iDb=0; iDb<db->nDb; iDb++){
    const char *zDb = db->aDb[iDb].zName;
    Schema *pSchema = db->aDb[iDb].pSchema;
    schema2AppendRow(pCsr,zDb,0,0,-1,"safety_level",
                     "%d", db->aDb[iDb].safety_level);
    schema2AppendSchema(pCsr, zDb, pSchema);
  }
  return schema2Next(pCursor);
}

static int schema2Column(
  sqlite3_vtab_cursor *pCursor, 
  sqlite3_context *ctx, 
  int i
){
  Schema2Cursor *pCsr = (Schema2Cursor *)pCursor;
  Schema2Row *pRow = pCsr->pCurrent;
  if( pRow==0 ) return SQLITE_OK;
  switch( i ){
    case 0:            /* dbname */
      sqlite3_result_text(ctx, pRow->zDb, -1, SQLITE_STATIC);
      break;
    case 1:            /* tblname */
      if( pRow->zTbl ) sqlite3_result_text(ctx, pRow->zTbl, -1, SQLITE_STATIC);
      break;
    case 2:            /* idxname */
      if( pRow->zIdx ) sqlite3_result_text(ctx, pRow->zIdx, -1, SQLITE_STATIC);
      break;
    case 3:            /* cnum */
      if( pRow->iCol>=0 ) sqlite3_result_int(ctx, pRow->iCol);
      break;
    case 4:            /* attr */
      sqlite3_result_text(ctx, pRow->zAttr, -1, SQLITE_STATIC);
      break;
    case 5:            /* value */
      if( pRow->zValue ) sqlite3_result_text(ctx,pRow->zValue,-1,SQLITE_STATIC);
      break;
  }
  return SQLITE_OK;
}

static int schema2Rowid(sqlite3_vtab_cursor *pCursor, sqlite_int64 *pRowid){
  Schema2Cursor *pCsr = (Schema2Cursor *)pCursor;
  *pRowid = pCsr->iRowid;
  return SQLITE_OK;
}

int sqlite3_schema2_register(sqlite3 *db){
  static sqlite3_module schema2_module = {
    0,                            /* iVersion */
    schema2Connect,               /* xCreate */
    schema2Connect,               /* xConnect */
    schema2BestIndex,             /* xBestIndex */
    schema2Disconnect,            /* xDisconnect */
    schema2Disconnect,            /* xDestroy */
    schema2Open,                  /* xOpen - open a cursor */
    schema2Close,                 /* xClose - close a cursor */
    schema2Filter,                /* xFilter - configure scan constraints */
    schema2Next,                  /* xNext - advance a cursor */
    schema2Eof,                   /* xEof - check for end of scan */
    schema2Column,                /* xColumn - read data */
    schema2Rowid,                 /* xRowid - read data */
    0,                            /* xUpdate */
    0,                            /* xBegin */
    0,                            /* xSync */
    0,                            /* xCommit */
    0,                            /* xRollback */
    0,                            /* xFindMethod */
    0,                            /* xRename */
  };
  sqlite3_create_module(db, "schema2", &schema2_module, 0);
  return SQLITE_OK;
}

#endif

#if defined(SQLITE_TEST) || TCLSH==2
#include <tcl.h>

static int test_schema2(
  void *clientData,
  Tcl_Interp *interp,
  int objc,
  Tcl_Obj *CONST objv[]
){
#ifdef SQLITE_OMIT_VIRTUALTABLE
  Tcl_AppendResult(interp, "schema2 not available because of "
                           "SQLITE_OMIT_VIRTUALTABLE", (void*)0);
  return TCL_ERROR;
#else
  struct SqliteDb { sqlite3 *db; };
  char *zDb;
  Tcl_CmdInfo cmdInfo;

  if( objc!=2 ){
    Tcl_WrongNumArgs(interp, 1, objv, "DB");
    return TCL_ERROR;
  }

  zDb = Tcl_GetString(objv[1]);
  if( Tcl_GetCommandInfo(interp, zDb, &cmdInfo) ){
    sqlite3* db = ((struct SqliteDb*)cmdInfo.objClientData)->db;
    sqlite3_schema2_register(db);
  }
  return TCL_OK;
#endif
}

int SqlitetestSchema2_Init(Tcl_Interp *interp){
  Tcl_CreateObjCommand(interp, "register_schema2_vtab", test_schema2, 0, 0);
  return TCL_OK;
}
#endif /* if defined(SQLITE_TEST) || TCLSH==2 */

/*
** 2017 October 27
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
*************************************************************************
*/

#include "sqlite3ext.h"
SQLITE_EXTENSION_INIT1

#ifndef SQLITE_AMALGAMATION
# include <string.h>
# include <stdio.h>
# include <stdlib.h>
# include <assert.h>
# define ALWAYS(X)  1
# define NEVER(X)   0
  typedef unsigned char u8;
  typedef unsigned short u16;
  typedef unsigned int u32;
#define get4byte(x) (        \
    ((u32)((x)[0])<<24) +    \
    ((u32)((x)[1])<<16) +    \
    ((u32)((x)[2])<<8) +     \
    ((u32)((x)[3]))          \
)
#endif

typedef struct CidxTable CidxTable;
typedef struct CidxCursor CidxCursor;

struct CidxTable {
  sqlite3_vtab base;              /* Base class.  Must be first */
  sqlite3 *db;
};

struct CidxCursor {
  sqlite3_vtab_cursor base;       /* Base class.  Must be first */
  sqlite3_stmt *pStmt;
};

static void *cidxMalloc(int *pRc, int n){
  void *pRet = 0;
  assert( n!=0 );
  if( *pRc==SQLITE_OK ){
    pRet = sqlite3_malloc(n);
    if( pRet ){
      memset(pRet, 0, n);
    }else{
      *pRc = SQLITE_NOMEM;
    }
  }
  return pRet;
}

static void cidxCursorError(CidxCursor *pCsr, const char *zFmt, ...){
  va_list ap;
  va_start(ap, zFmt);
  assert( pCsr->base.pVtab->zErrMsg==0 );
  pCsr->base.pVtab->zErrMsg = sqlite3_vmprintf(zFmt, ap);
  va_end(ap);
}

/*
** Connect to then incremental_index_check virtual table.
*/
static int cidxConnect(
  sqlite3 *db,
  void *pAux,
  int argc, const char *const*argv,
  sqlite3_vtab **ppVtab,
  char **pzErr
){
  int rc = SQLITE_OK;
  CidxTable *pRet;

  rc = sqlite3_declare_vtab(db,
      "CREATE TABLE xyz("
      " errmsg TEXT, current_key TEXT,"
      " index_name HIDDEN, after_key HIDDEN"
      ")"
  );
  pRet = cidxMalloc(&rc, sizeof(CidxTable));
  if( pRet ){
    pRet->db = db;
  }

  *ppVtab = (sqlite3_vtab*)pRet;
  return rc;
}

/*
** Disconnect from or destroy an incremental_index_check virtual table.
*/
static int cidxDisconnect(sqlite3_vtab *pVtab){
  CidxTable *pTab = (CidxTable*)pVtab;
  sqlite3_free(pTab);
  return SQLITE_OK;
}

/*
** xBestIndex method.
*/
static int cidxBestIndex(sqlite3_vtab *tab, sqlite3_index_info *pInfo){
  int iIdxName = -1;
  int iAfterKey = -1;
  int i;

  for(i=0; i<pInfo->nConstraint; i++){
    struct sqlite3_index_constraint *p = &pInfo->aConstraint[i];
    if( p->usable==0 ) continue;
    if( p->op!=SQLITE_INDEX_CONSTRAINT_EQ ) continue;

    if( p->iColumn==2 ){
      iIdxName = i;
    }
    if( p->iColumn==3 ){
      iAfterKey = i;
    }
  }

  if( iIdxName<0 ){
    pInfo->estimatedCost = 1000000000.0;
  }else{
    pInfo->aConstraintUsage[iIdxName].argvIndex = 1;
    pInfo->aConstraintUsage[iIdxName].omit = 1;
    if( iAfterKey<0 ){
      pInfo->estimatedCost = 1000000.0;
    }else{
      pInfo->aConstraintUsage[iAfterKey].argvIndex = 2;
      pInfo->aConstraintUsage[iAfterKey].omit = 1;
      pInfo->estimatedCost = 1000.0;
    }
  }

  return SQLITE_OK;
}

/*
** Open a new btreeinfo cursor.
*/
static int cidxOpen(sqlite3_vtab *pVTab, sqlite3_vtab_cursor **ppCursor){
  CidxCursor *pRet;
  int rc = SQLITE_OK;

  pRet = cidxMalloc(&rc, sizeof(CidxCursor));

  *ppCursor = (sqlite3_vtab_cursor*)pRet;
  return rc;
}

/*
** Close a btreeinfo cursor.
*/
static int cidxClose(sqlite3_vtab_cursor *pCursor){
  CidxCursor *pCsr = (CidxCursor*)pCursor;
  sqlite3_finalize(pCsr->pStmt);
  pCsr->pStmt = 0;
  sqlite3_free(pCsr);
  return SQLITE_OK;
}

/*
** Move a btreeinfo cursor to the next entry in the file.
*/
static int cidxNext(sqlite3_vtab_cursor *pCursor){
  CidxCursor *pCsr = (CidxCursor*)pCursor;
  int rc = sqlite3_step(pCsr->pStmt);
  if( rc!=SQLITE_ROW ){
    rc = sqlite3_finalize(pCsr->pStmt);
    pCsr->pStmt = 0;
    if( rc!=SQLITE_OK ){
      sqlite3 *db = ((CidxTable*)pCsr->base.pVtab)->db;
      cidxCursorError(pCsr, "Cursor error: %s", sqlite3_errmsg(db));
    }
  }else{
    rc = SQLITE_OK;
  }
  return rc;
}

/* We have reached EOF if previous sqlite3_step() returned
** anything other than SQLITE_ROW;
*/
static int cidxEof(sqlite3_vtab_cursor *pCursor){
  CidxCursor *pCsr = (CidxCursor*)pCursor;
  return pCsr->pStmt==0;
}

static sqlite3_stmt *cidxPrepare(
  int *pRc, CidxCursor *pCsr, const char *zFmt, ...
){
  sqlite3_stmt *pRet = 0;
  char *zSql;
  va_list ap;                     /* ... printf arguments */
  va_start(ap, zFmt);

  zSql = sqlite3_vmprintf(zFmt, ap);
  if( *pRc==SQLITE_OK ){
    if( zSql==0 ){
      *pRc = SQLITE_NOMEM;
    }else{
      sqlite3 *db = ((CidxTable*)pCsr->base.pVtab)->db;
      *pRc = sqlite3_prepare_v2(db, zSql, -1, &pRet, 0);
      if( *pRc!=SQLITE_OK ){
        cidxCursorError(pCsr, "SQL error: %s", sqlite3_errmsg(db));
      }
    }
  }
  sqlite3_free(zSql);
  va_end(ap);

  return pRet;
}

static void cidxFinalize(int *pRc, sqlite3_stmt *pStmt){
  int rc = sqlite3_finalize(pStmt);
  if( *pRc==SQLITE_OK ) *pRc = rc;
}

char *cidxStrdup(int *pRc, const char *zStr){
  char *zRet = 0;
  if( *pRc==SQLITE_OK ){
    int n = strlen(zStr);
    zRet = cidxMalloc(pRc, n+1);
    if( zRet ) memcpy(zRet, zStr, n+1);
  }
  return zRet;
}

static int cidxLookupIndex(
  CidxCursor *pCsr,               /* Cursor object */
  const char *zIdx,               /* Name of index to look up */
  char **pzTab,                   /* OUT: Table name */
  char **pzCurrentKey,            /* OUT: Expression for current_key */
  char **pzOrderBy,               /* OUT: ORDER BY expression list */
  char **pzSubWhere,              /* OUT: sub-query WHERE clause */
  char **pzSubExpr                /* OUT: sub-query WHERE clause */
){
  int rc = SQLITE_OK;
  char *zTab = 0;
  char *zCurrentKey = 0;
  char *zOrderBy = 0;
  char *zSubWhere = 0;
  char *zSubExpr = 0;

  sqlite3_stmt *pFindTab = 0;
  sqlite3_stmt *pGroup = 0;
    
  /* Find the table */
  pFindTab = cidxPrepare(&rc, pCsr, 
      "SELECT tbl_name FROM sqlite_master WHERE name=%Q AND type='index'",
      zIdx
  );
  if( rc==SQLITE_OK && sqlite3_step(pFindTab)==SQLITE_ROW ){
    zTab = cidxStrdup(&rc, (const char*)sqlite3_column_text(pFindTab, 0));
  }
  cidxFinalize(&rc, pFindTab);
  if( rc==SQLITE_OK && zTab==0 ){
    rc = SQLITE_ERROR;
  }

  pGroup = cidxPrepare(&rc, pCsr,
      "SELECT group_concat("
      "  coalesce(name, 'rowid'), '|| '','' ||'"
      ") AS zCurrentKey,"
      "       group_concat("
      "  coalesce(name, 'rowid') || CASE WHEN desc THEN ' DESC' ELSE '' END,"
      "  ', '"
      ") AS zOrderBy,"
      "       group_concat("
      "         CASE WHEN key==1 THEN NULL ELSE "
      "  coalesce(name, 'rowid') || ' IS \"%w\".' || coalesce(name, 'rowid') "
      "         END,"
      "  'AND '"
      ") AS zSubWhere,"
      "       group_concat("
      "         CASE WHEN key==0 THEN NULL ELSE "
      "  coalesce(name, 'rowid') || ' IS \"%w\".' || coalesce(name, 'rowid') "
      "         END,"
      "  'AND '"
      ") AS zSubExpr "
      " FROM pragma_index_xinfo(%Q);"
      , zIdx, zIdx, zIdx
  );
  if( rc==SQLITE_OK && sqlite3_step(pGroup)==SQLITE_ROW ){
    zCurrentKey = cidxStrdup(&rc, (const char*)sqlite3_column_text(pGroup, 0));
    zOrderBy = cidxStrdup(&rc, (const char*)sqlite3_column_text(pGroup, 1));
    zSubWhere = cidxStrdup(&rc, (const char*)sqlite3_column_text(pGroup, 2));
    zSubExpr = cidxStrdup(&rc, (const char*)sqlite3_column_text(pGroup, 3));
  }
  cidxFinalize(&rc, pGroup);
  
  if( rc!=SQLITE_OK ){
    sqlite3_free(zTab);
    sqlite3_free(zCurrentKey);
    sqlite3_free(zOrderBy);
    sqlite3_free(zSubWhere);
    sqlite3_free(zSubExpr);
  }else{
    *pzTab = zTab;
    *pzCurrentKey = zCurrentKey;
    *pzOrderBy = zOrderBy;
    *pzSubWhere = zSubWhere;
    *pzSubExpr = zSubExpr;
  }

  return rc;
}

/* 
** Position a cursor back to the beginning.
*/
static int cidxFilter(
  sqlite3_vtab_cursor *pCursor, 
  int idxNum, const char *idxStr,
  int argc, sqlite3_value **argv
){
  int rc = SQLITE_OK;
  CidxCursor *pCsr = (CidxCursor*)pCursor;
  const char *zIdxName = 0;
  const char *zAfterKey = 0;

  if( argc>0 ){
    zIdxName = (const char*)sqlite3_value_text(argv[0]);
    if( argc>1 ){
      zAfterKey = (const char*)sqlite3_value_text(argv[1]);
    }
  }

  if( zIdxName ){
    char *zTab = 0;
    char *zCurrentKey = 0;
    char *zOrderBy = 0;
    char *zSubWhere = 0;
    char *zSubExpr = 0;

    rc = cidxLookupIndex(pCsr, zIdxName, 
        &zTab, &zCurrentKey, &zOrderBy, &zSubWhere, &zSubExpr
    );
    pCsr->pStmt = cidxPrepare(&rc, pCsr, 
        "SELECT (SELECT %s FROM %Q WHERE %s), %s FROM %Q AS %Q ORDER BY %s",
        zSubExpr, zTab, zSubWhere, zCurrentKey, zTab, zIdxName, zOrderBy
    );

    sqlite3_free(zTab);
    sqlite3_free(zCurrentKey);
    sqlite3_free(zOrderBy);
    sqlite3_free(zSubWhere);
    sqlite3_free(zSubExpr);
  }

  if( pCsr->pStmt ){
    assert( rc==SQLITE_OK );
    rc = cidxNext(pCursor);
  }
  return rc;
}

/* Return a column for the sqlite_btreeinfo table */
static int cidxColumn(
  sqlite3_vtab_cursor *pCursor, 
  sqlite3_context *ctx, 
  int iCol
){
  CidxCursor *pCsr = (CidxCursor*)pCursor;
  assert( iCol==0 || iCol==1 );
  if( iCol==0 ){
    const char *zVal = 0;
    if( sqlite3_column_type(pCsr->pStmt, 0)==SQLITE_INTEGER ){
      if( sqlite3_column_int(pCsr->pStmt, 0)==0 ){
        zVal = "row data mismatch";
      }
    }else{
      zVal = "row missing";
    }
    sqlite3_result_text(ctx, zVal, -1, SQLITE_STATIC);
  }else{
    sqlite3_result_value(ctx, sqlite3_column_value(pCsr->pStmt, 1));
  }
  return SQLITE_OK;
}

/* Return the ROWID for the sqlite_btreeinfo table */
static int cidxRowid(sqlite3_vtab_cursor *pCursor, sqlite_int64 *pRowid){
  *pRowid = 0;
  return SQLITE_OK;
}

/*
** Register the virtual table modules with the database handle passed
** as the only argument.
*/
static int ciInit(sqlite3 *db){
  static sqlite3_module cidx_module = {
    0,                            /* iVersion */
    0,                            /* xCreate */
    cidxConnect,                  /* xConnect */
    cidxBestIndex,                /* xBestIndex */
    cidxDisconnect,               /* xDisconnect */
    0,                            /* xDestroy */
    cidxOpen,                     /* xOpen - open a cursor */
    cidxClose,                    /* xClose - close a cursor */
    cidxFilter,                   /* xFilter - configure scan constraints */
    cidxNext,                     /* xNext - advance a cursor */
    cidxEof,                      /* xEof - check for end of scan */
    cidxColumn,                   /* xColumn - read data */
    cidxRowid,                    /* xRowid - read data */
    0,                            /* xUpdate */
    0,                            /* xBegin */
    0,                            /* xSync */
    0,                            /* xCommit */
    0,                            /* xRollback */
    0,                            /* xFindMethod */
    0,                            /* xRename */
    0,                            /* xSavepoint */
    0,                            /* xRelease */
    0,                            /* xRollbackTo */
  };
  return sqlite3_create_module(db, "incremental_index_check", &cidx_module, 0);
}

/*
** Extension load function.
*/
#ifdef _WIN32
__declspec(dllexport)
#endif
int sqlite3_checkindex_init(
  sqlite3 *db, 
  char **pzErrMsg, 
  const sqlite3_api_routines *pApi
){
  SQLITE_EXTENSION_INIT2(pApi);
  return ciInit(db);
}

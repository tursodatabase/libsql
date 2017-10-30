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

typedef struct CidxColumn CidxColumn;
struct CidxColumn {
  char *zName;
  char *zColl;
  int bDesc;
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

static char *cidxMprintf(int *pRc, const char *zFmt, ...){
  char *zRet = 0;
  va_list ap;
  va_start(ap, zFmt);
  zRet = sqlite3_vmprintf(zFmt, ap);
  if( *pRc==SQLITE_OK ){
    if( zRet==0 ){
      *pRc = SQLITE_NOMEM;
    }
  }else{
    sqlite3_free(zRet);
    zRet = 0;
  }
  va_end(ap);
  return zRet;
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
  int *pnCol,                     /* OUT: Number of columns in index */
  CidxColumn **paCol,             /* OUT: Columns */
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
  CidxColumn *aCol = 0;

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
      "  coalesce('quote(' || name || ')', 'rowid'), '|| '','' ||'"
      ") AS zCurrentKey,"
      "       group_concat("
      "  coalesce(name, 'rowid') || ' COLLATE ' || coll "
      "  || CASE WHEN desc THEN ' DESC' ELSE '' END,"
      "  ', '"
      ") AS zOrderBy,"
      "       group_concat("
      "         CASE WHEN key==1 THEN NULL ELSE "
      "  coalesce(name, 'rowid') || ' IS \"%w\".' || coalesce(name, 'rowid') "
      "         END,"
      "  ' AND '"
      ") AS zSubWhere,"
      "       group_concat("
      "         CASE WHEN key==0 THEN NULL ELSE "
      "  coalesce(name, 'rowid') || ' IS \"%w\".' || coalesce(name, 'rowid') "
      "         END,"
      "  ' AND '"
      ") AS zSubExpr,"
      "      count(*) AS nCol"
      " FROM pragma_index_xinfo(%Q);"
      , zIdx, zIdx, zIdx
  );
  if( rc==SQLITE_OK && sqlite3_step(pGroup)==SQLITE_ROW ){
    zCurrentKey = cidxStrdup(&rc, (const char*)sqlite3_column_text(pGroup, 0));
    zOrderBy = cidxStrdup(&rc, (const char*)sqlite3_column_text(pGroup, 1));
    zSubWhere = cidxStrdup(&rc, (const char*)sqlite3_column_text(pGroup, 2));
    zSubExpr = cidxStrdup(&rc, (const char*)sqlite3_column_text(pGroup, 3));
    *pnCol = sqlite3_column_int(pGroup, 4);
  }
  cidxFinalize(&rc, pGroup);

  pGroup = cidxPrepare(&rc, pCsr, "PRAGMA index_xinfo(%Q)", zIdx);
  if( rc==SQLITE_OK ){
    int nByte = 0;
    int nCol = 0;
    while( sqlite3_step(pGroup)==SQLITE_ROW ){
      const char *zName = (const char*)sqlite3_column_text(pGroup, 2);
      const char *zColl = (const char*)sqlite3_column_text(pGroup, 4);
      if( zName==0 ) zName = "rowid";
      nCol++;
      nByte += strlen(zName)+1 + strlen(zColl)+1;
    }
    rc = sqlite3_reset(pGroup);
    aCol = (CidxColumn*)cidxMalloc(&rc, sizeof(CidxColumn)*nCol + nByte);

    if( rc==SQLITE_OK ){
      int iCol = 0;
      char *z = (char*)&aCol[nCol];
      while( sqlite3_step(pGroup)==SQLITE_ROW ){
        int nName, nColl;
        const char *zName = (const char*)sqlite3_column_text(pGroup, 2);
        const char *zColl = (const char*)sqlite3_column_text(pGroup, 4);
        if( zName==0 ) zName = "rowid";

        nName = strlen(zName);
        nColl = strlen(zColl);
        memcpy(z, zName, nName);
        aCol[iCol].zName = z;
        z += nName+1;

        memcpy(z, zColl, nColl);
        aCol[iCol].zColl = z;
        z += nColl+1;

        aCol[iCol].bDesc = sqlite3_column_int(pGroup, 3);
        iCol++;
      }
    }
    cidxFinalize(&rc, pGroup);
  }
  
  if( rc!=SQLITE_OK ){
    sqlite3_free(zTab);
    sqlite3_free(zCurrentKey);
    sqlite3_free(zOrderBy);
    sqlite3_free(zSubWhere);
    sqlite3_free(zSubExpr);
    sqlite3_free(aCol);
  }else{
    *pzTab = zTab;
    *pzCurrentKey = zCurrentKey;
    *pzOrderBy = zOrderBy;
    *pzSubWhere = zSubWhere;
    *pzSubExpr = zSubExpr;
    *paCol = aCol;
  }

  return rc;
}

static int cidxDecodeAfter(
  CidxCursor *pCsr, 
  int nCol, 
  const char *zAfterKey, 
  char ***pazAfter
){
  char **azAfter;
  int rc = SQLITE_OK;
  int nAfterKey = strlen(zAfterKey);

  azAfter = cidxMalloc(&rc, sizeof(char*)*nCol + nAfterKey+1);
  if( rc==SQLITE_OK ){
    int i;
    char *zCopy = (char*)&azAfter[nCol];
    char *p = zCopy;
    memcpy(zCopy, zAfterKey, nAfterKey+1);
    for(i=0; i<nCol; i++){
      while( *p==' ' ) p++;

      /* Check NULL values */
      if( *p=='N' ){
        if( memcmp(p, "NULL", 4) ) goto parse_error;
        p += 4;
      }

      /* Check strings and blob literals */
      else if( *p=='X' || *p=='\'' ){
        azAfter[i] = p;
        if( *p=='X' ) p++;
        if( *p!='\'' ) goto parse_error;
        p++;
        while( 1 ){
          if( *p=='\0' ) goto parse_error;
          if( *p=='\'' ){
            p++;
            if( *p!='\'' ) break;
          }
          p++;
        }
      }

      /* Check numbers */
      else{
        azAfter[i] = p;
        while( (*p>='0' && *p<='9') 
            || *p=='.' || *p=='+' || *p=='-' || *p=='e' || *p=='E'
        ){
          p++;
        }
      }

      while( *p==' ' ) p++;
      if( *p!=(i==(nCol-1) ? '\0' : ',') ){
        goto parse_error;
      }
      *p++ = '\0';
    }
  }

  *pazAfter = azAfter;
  return rc;

 parse_error:
  sqlite3_free(azAfter);
  *pazAfter = 0;
  cidxCursorError(pCsr, "%s", "error parsing after value");
  return SQLITE_ERROR;
}

static char *cidxWhere(
  int *pRc, CidxColumn *aCol, char **azAfter, int iGt, int bLastIsNull
){
  char *zRet = 0;
  const char *zSep = "";
  int i;

  for(i=0; i<iGt; i++){
    zRet = cidxMprintf(pRc, "%z%s%s COLLATE %s IS %s", zRet, 
        zSep, aCol[i].zName, aCol[i].zColl, (azAfter[i] ? azAfter[i] : "NULL")
    );
    zSep = " AND ";
  }

  if( bLastIsNull ){
    zRet = cidxMprintf(pRc, "%z%s%s IS NULL", zRet, zSep, aCol[iGt].zName);
  }
  else if( azAfter[iGt] ){
    zRet = cidxMprintf(pRc, "%z%s%s COLLATE %s %s %s", zRet, 
        zSep, aCol[iGt].zName, aCol[iGt].zColl, (aCol[iGt].bDesc ? "<" : ">"), 
        azAfter[iGt]
    );
  }else{
    zRet = cidxMprintf(pRc, "%z%s%s IS NOT NULL", zRet, zSep, aCol[iGt].zName);
  }

  return zRet;
}

static char *cidxColumnList(int *pRc, CidxColumn *aCol, int nCol){
  int i;
  char *zRet = 0;
  const char *zSep = "";
  for(i=0; i<nCol; i++){
    zRet = cidxMprintf(pRc, "%z%s%s", zRet, zSep, aCol[i].zName);
    zSep = ",";
  }
  return zRet;
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
    int nCol = 0;
    char *zTab = 0;
    char *zCurrentKey = 0;
    char *zOrderBy = 0;
    char *zSubWhere = 0;
    char *zSubExpr = 0;
    char **azAfter = 0;
    CidxColumn *aCol = 0;

    rc = cidxLookupIndex(pCsr, zIdxName, 
        &nCol, &aCol, &zTab, &zCurrentKey, &zOrderBy, &zSubWhere, &zSubExpr
    );

    if( rc==SQLITE_OK && zAfterKey ){
      rc = cidxDecodeAfter(pCsr, nCol, zAfterKey, &azAfter);
    }

    if( rc || zAfterKey==0 ){
      pCsr->pStmt = cidxPrepare(&rc, pCsr, 
          "SELECT (SELECT %s FROM %Q WHERE %s), %s FROM %Q AS %Q ORDER BY %s",
          zSubExpr, zTab, zSubWhere, zCurrentKey, zTab, zIdxName, zOrderBy
      );
      /* printf("SQL: %s\n", sqlite3_sql(pCsr->pStmt)); */
    }else{
      char *zList = cidxColumnList(&rc, aCol, nCol);
      const char *zSep = "";
      char *zSql;
      int i;

      zSql = cidxMprintf(&rc, "SELECT (SELECT %s FROM %Q WHERE %s), %s FROM (",
          zSubExpr, zTab, zSubWhere, zCurrentKey
      );
      for(i=nCol-1; i>=0; i--){
        int j;
        if( aCol[i].bDesc && azAfter[i]==0 ) continue;
        for(j=0; j<2; j++){
          char *zWhere = cidxWhere(&rc, aCol, azAfter, i, j);
          zSql = cidxMprintf(&rc, 
              "%z%s SELECT * FROM (SELECT %s FROM %Q WHERE %z ORDER BY %s)",
              zSql, zSep, zList, zTab, zWhere, zOrderBy
              );
          zSep = " UNION ALL ";
          if( aCol[i].bDesc==0 ) break;
        }
      }
      zSql = cidxMprintf(&rc, "%z) AS %Q", zSql, zIdxName);
      sqlite3_free(zList);

      /* printf("SQL: %s\n", zSql); */
      pCsr->pStmt = cidxPrepare(&rc, pCsr, "%z", zSql);
    }

    sqlite3_free(zTab);
    sqlite3_free(zCurrentKey);
    sqlite3_free(zOrderBy);
    sqlite3_free(zSubWhere);
    sqlite3_free(zSubExpr);
    sqlite3_free(aCol);
    sqlite3_free(azAfter);
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

/*
** 2017 July 15
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
*/
#if !defined(SQLITE_CORE) || defined(SQLITE_ENABLE_UNIONVTAB)

#include "sqlite3ext.h"
SQLITE_EXTENSION_INIT1
#include <assert.h>
#include <string.h>

#ifndef SQLITE_OMIT_VIRTUALTABLE

typedef struct UnionCsr UnionCsr;
typedef struct UnionTab UnionTab;
typedef struct UnionSrc UnionSrc;

/*
** Each source table (row returned by the initialization query) is 
** represented by an instance of the following structure stored in the
** UnionTab.aSrc[] array.
*/
struct UnionSrc {
  char *zDb;                      /* Database containing source table */
  char *zTab;                     /* Source table name */
  sqlite3_int64 iMin;             /* Minimum rowid */
  sqlite3_int64 iMax;             /* Maximum rowid */
};

/*
** Virtual table cursor type for union vtab.
*/
struct UnionCsr {
  sqlite3_vtab_cursor base;       /* Base class - must be first */
  sqlite3_stmt *pStmt;            /* SQL statement to run */
};

/*
** Virtual table  type for union vtab.
*/
struct UnionTab {
  sqlite3_vtab base;              /* Base class - must be first */
  sqlite3 *db;                    /* Database handle */
  int nSrc;                       /* Number of elements in the aSrc[] array */
  UnionSrc *aSrc;                 /* Array of source tables, sorted by rowid */
};

/*
** If *pRc is other than SQLITE_OK when this function is called, it
** always returns NULL. Otherwise, it attempts to allocate and return
** a pointer to nByte bytes of zeroed memory. If the memory allocation
** is attempted but fails, NULL is returned and *pRc is set to 
** SQLITE_NOMEM.
*/
static void *unionMalloc(int *pRc, int nByte){
  void *pRet;
  assert( nByte>0 );
  if( *pRc==SQLITE_OK ){
    pRet = sqlite3_malloc(nByte);
    if( pRet ){
      memset(pRet, 0, nByte);
    }else{
      *pRc = SQLITE_NOMEM;
    }
  }else{
    pRet = 0;
  }
  return pRet;
}

/*
** If *pRc is other than SQLITE_OK when this function is called, it
** always returns NULL. Otherwise, it attempts to allocate and return
** a copy of the nul-terminated string passed as the second argument.
** If the allocation is attempted but fails, NULL is returned and *pRc is 
** set to SQLITE_NOMEM.
*/
static char *unionStrdup(int *pRc, const char *zIn){
  char *zRet = 0;
  if( zIn ){
    int nByte = strlen(zIn) + 1;
    zRet = unionMalloc(pRc, nByte);
    if( zRet ){
      memcpy(zRet, zIn, nByte);
    }
  }
  return zRet;
}

/*
** If the first character of the string passed as the only argument to this
** function is one of the 4 that may be used as an open quote character
** in SQL, this function assumes that the input is a well-formed quoted SQL 
** string. In this case the string is dequoted in place.
**
** If the first character of the input is not an open quote, then this
** function is a no-op.
*/
static void unionDequote(char *z){
  char q = z[0];

  /* Set stack variable q to the close-quote character */
  if( q=='[' || q=='\'' || q=='"' || q=='`' ){
    int iIn = 1;
    int iOut = 0;
    if( q=='[' ) q = ']';  
    while( z[iIn] ){
      if( z[iIn]==q ){
        if( z[iIn+1]!=q ){
          /* Character iIn was the close quote. */
          iIn++;
          break;
        }else{
          /* Character iIn and iIn+1 form an escaped quote character. Skip
          ** the input cursor past both and copy a single quote character 
          ** to the output buffer. */
          iIn += 2;
          z[iOut++] = q;
        }
      }else{
        z[iOut++] = z[iIn++];
      }
    }
    z[iOut] = '\0';
  }
}

static sqlite3_stmt *unionPrepare(
  int *pRc, 
  sqlite3 *db, 
  const char *zSql, 
  char **pzErr
){
  sqlite3_stmt *pRet = 0;
  if( *pRc==SQLITE_OK ){
    int rc = sqlite3_prepare_v2(db, zSql, -1, &pRet, 0);
    if( rc!=SQLITE_OK ){
      *pzErr = sqlite3_mprintf("sql error: %s", sqlite3_errmsg(db));
      *pRc = rc;
    }
  }
  return pRet;
}

static void unionReset(int *pRc, sqlite3_stmt *pStmt, char **pzErr){
  int rc = sqlite3_reset(pStmt);
  if( *pRc==SQLITE_OK ){
    *pRc = rc;
    if( rc && pzErr ){
      *pzErr = sqlite3_mprintf("%s", sqlite3_errmsg(sqlite3_db_handle(pStmt)));
    }
  }
}

static void unionFinalize(int *pRc, sqlite3_stmt *pStmt){
  int rc = sqlite3_finalize(pStmt);
  if( *pRc==SQLITE_OK ) *pRc = rc;
}

/*
** xDisconnect method.
*/
static int unionDisconnect(sqlite3_vtab *pVtab){
  if( pVtab ){
    UnionTab *pTab = (UnionTab*)pVtab;
    int i;
    for(i=0; i<pTab->nSrc; i++){
      sqlite3_free(pTab->aSrc[i].zDb);
      sqlite3_free(pTab->aSrc[i].zTab);
    }
    sqlite3_free(pTab->aSrc);
    sqlite3_free(pTab);
  }
  return SQLITE_OK;
}

static char *unionSourceToStr(
  int *pRc,
  UnionSrc *pSrc, 
  sqlite3_stmt *pStmt,
  char **pzErr
){
  char *zRet = 0;
  if( *pRc==SQLITE_OK ){
    sqlite3_bind_text(pStmt, 1, pSrc->zTab, -1, SQLITE_STATIC);
    sqlite3_bind_text(pStmt, 2, pSrc->zDb, -1, SQLITE_STATIC);
    if( SQLITE_ROW==sqlite3_step(pStmt) ){
      zRet = unionStrdup(pRc, (const char*)sqlite3_column_text(pStmt, 0));
    }
    unionReset(pRc, pStmt, pzErr);
    if( *pRc==SQLITE_OK && zRet==0 ){
      *pRc = SQLITE_ERROR;
      *pzErr = sqlite3_mprintf("no such table: %s%s%s",
          (pSrc->zDb ? pSrc->zDb : ""),
          (pSrc->zDb ? "." : ""),
          pSrc->zTab
      );
    }
  }
  return zRet;
}

static int unionSourceCheck(UnionTab *pTab, char **pzErr){
  const char *zSql = 
      "SELECT group_concat(quote(name) || '.' || quote(type)) "
      "FROM pragma_table_info(?, ?)";
  int rc = SQLITE_OK;

  if( pTab->nSrc==0 ){
    *pzErr = sqlite3_mprintf("no source tables configured");
    rc = SQLITE_ERROR;
  }else{
    sqlite3_stmt *pStmt = 0;
    char *z0 = 0;
    int i;

    pStmt = unionPrepare(&rc, pTab->db, zSql, pzErr);
    if( rc==SQLITE_OK ){
      z0 = unionSourceToStr(&rc, &pTab->aSrc[0], pStmt, pzErr);
    }
    for(i=1; i<pTab->nSrc; i++){
      char *z = unionSourceToStr(&rc, &pTab->aSrc[i], pStmt, pzErr);
      if( rc==SQLITE_OK && sqlite3_stricmp(z, z0) ){
        *pzErr = sqlite3_mprintf("source table schema mismatch");
        rc = SQLITE_ERROR;
      }
      sqlite3_free(z);
    }

    unionFinalize(&rc, pStmt);
    sqlite3_free(z0);
  }
  return rc;
}

/* 
** xConnect/xCreate method.
**
** The argv[] array contains the following:
**
**   argv[0]   -> module name  ("unionvtab")
**   argv[1]   -> database name
**   argv[2]   -> table name
**   argv[3]   -> SQL statement
*/
static int unionConnect(
  sqlite3 *db,
  void *pAux,
  int argc, const char *const*argv,
  sqlite3_vtab **ppVtab,
  char **pzErr
){
  UnionTab *pTab = 0;
  int rc = SQLITE_OK;

  if( sqlite3_stricmp("temp", argv[1]) ){
    /* unionvtab tables may only be created in the temp schema */
    *pzErr = sqlite3_mprintf("unionvtab tables must be created in TEMP schema");
    rc = SQLITE_ERROR;
  }else if( argc!=4 ){
    *pzErr = sqlite3_mprintf("wrong number of arguments for unionvtab");
    rc = SQLITE_ERROR;
  }else{
    int nAlloc = 0;               /* Allocated size of pTab->aSrc[] */
    sqlite3_stmt *pStmt = 0;      /* Argument statement */
    char *zSql1 = unionStrdup(&rc, argv[3]);
    char *zSql2 = 0;

    if( zSql1 ){
      unionDequote(zSql1);
      zSql2 = sqlite3_mprintf("SELECT * FROM (%s) ORDER BY 3", zSql1);
      sqlite3_free(zSql1);
      zSql1 = 0;
    }
    if( zSql2==0 ){
      rc = SQLITE_NOMEM;
    }
    pTab = unionMalloc(&rc, sizeof(UnionTab));
    pStmt = unionPrepare(&rc, db, zSql2, pzErr);

    while( rc==SQLITE_OK && SQLITE_ROW==sqlite3_step(pStmt) ){
      const char *zDb = (const char*)sqlite3_column_text(pStmt, 0);
      const char *zTab = (const char*)sqlite3_column_text(pStmt, 1);
      sqlite3_int64 iMin = sqlite3_column_int64(pStmt, 2);
      sqlite3_int64 iMax = sqlite3_column_int64(pStmt, 3);
      UnionSrc *pSrc;

      if( nAlloc<=pTab->nSrc ){
        int nNew = nAlloc ? nAlloc*2 : 8;
        UnionSrc *aNew = (UnionSrc*)sqlite3_realloc(
            pTab->aSrc, nNew*sizeof(UnionSrc)
        );
        if( aNew==0 ){
          rc = SQLITE_NOMEM;
          break;
        }else{
          memset(&aNew[pTab->nSrc], 0, (nNew-pTab->nSrc)*sizeof(UnionSrc));
          pTab->aSrc = aNew;
          nAlloc = nNew;
        }
      }

      if( iMax<iMin || (pTab->nSrc>0 && iMin<=pTab->aSrc[pTab->nSrc-1].iMax) ){
        *pzErr = sqlite3_mprintf("rowid range mismatch error");
        rc = SQLITE_ERROR;
      }

      pSrc = &pTab->aSrc[pTab->nSrc++];
      pSrc->zDb = unionStrdup(&rc, zDb);
      pSrc->zTab = unionStrdup(&rc, zTab);
      pSrc->iMin = iMin;
      pSrc->iMax = iMax;
    }
    unionFinalize(&rc, pStmt);
    pStmt = 0;
    sqlite3_free(zSql1);
    sqlite3_free(zSql2);
    zSql1 = 0;
    zSql2 = 0;

    /* Verify that all source tables exist and have compatible schemas. */
    if( rc==SQLITE_OK ){
      pTab->db = db;
      rc = unionSourceCheck(pTab, pzErr);
    }

    /* Compose a CREATE TABLE statement and pass it to declare_vtab() */
    if( rc==SQLITE_OK ){
      zSql1 = sqlite3_mprintf("SELECT "
          "'CREATE TABLE xyz('"
          "    || group_concat(quote(name) || ' ' || type, ', ')"
          "    || ')'"
          "FROM pragma_table_info(%Q, ?)", 
          pTab->aSrc[0].zTab
      );
      if( zSql1==0 ) rc = SQLITE_NOMEM;
    }
    pStmt = unionPrepare(&rc, db, zSql1, pzErr);
    if( rc==SQLITE_OK ){
      sqlite3_bind_text(pStmt, 1, pTab->aSrc[0].zDb, -1, SQLITE_STATIC);
      if( SQLITE_ROW==sqlite3_step(pStmt) ){
        const char *zDecl = (const char*)sqlite3_column_text(pStmt, 0);
        rc = sqlite3_declare_vtab(db, zDecl);
      }
    }

    unionFinalize(&rc, pStmt);
    sqlite3_free(zSql1);
  }

  if( rc!=SQLITE_OK ){
    unionDisconnect((sqlite3_vtab*)pTab);
    pTab = 0;
  }

  *ppVtab = (sqlite3_vtab*)pTab;
  return rc;
}


/*
** xOpen
*/
static int unionOpen(sqlite3_vtab *p, sqlite3_vtab_cursor **ppCursor){
  UnionCsr *pCsr;
  int rc = SQLITE_OK;
  pCsr = (UnionCsr*)unionMalloc(&rc, sizeof(UnionCsr));
  *ppCursor = &pCsr->base;
  return rc;
}

/*
** xClose
*/
static int unionClose(sqlite3_vtab_cursor *cur){
  UnionCsr *pCsr = (UnionCsr*)cur;
  sqlite3_finalize(pCsr->pStmt);
  sqlite3_free(pCsr);
  return SQLITE_OK;
}


/*
** xNext
*/
static int unionNext(sqlite3_vtab_cursor *cur){
  UnionCsr *pCsr = (UnionCsr*)cur;
  int rc;
  assert( pCsr->pStmt );
  if( sqlite3_step(pCsr->pStmt)!=SQLITE_ROW ){
    rc = sqlite3_finalize(pCsr->pStmt);
    pCsr->pStmt = 0;
  }else{
    rc = SQLITE_OK;
  }
  return rc;
}

/*
** xColumn
*/
static int unionColumn(
  sqlite3_vtab_cursor *cur,
  sqlite3_context *ctx,
  int i
){
  UnionCsr *pCsr = (UnionCsr*)cur;
  sqlite3_result_value(ctx, sqlite3_column_value(pCsr->pStmt, i+1));
  return SQLITE_OK;
}

/*
** xRowid
*/
static int unionRowid(sqlite3_vtab_cursor *cur, sqlite_int64 *pRowid){
  UnionCsr *pCsr = (UnionCsr*)cur;
  *pRowid = sqlite3_column_int64(pCsr->pStmt, 0);
  return SQLITE_OK;
}

/*
** xEof
*/
static int unionEof(sqlite3_vtab_cursor *cur){
  UnionCsr *pCsr = (UnionCsr*)cur;
  return pCsr->pStmt==0;
}

/*
** xFilter
*/
static int unionFilter(
  sqlite3_vtab_cursor *pVtabCursor, 
  int idxNum, const char *idxStr,
  int argc, sqlite3_value **argv
){
  UnionTab *pTab = (UnionTab*)(pVtabCursor->pVtab);
  UnionCsr *pCsr = (UnionCsr*)pVtabCursor;
  int rc = SQLITE_OK;
  int i;
  char *zSql = 0;

  int bMinValid = 0;
  int bMaxValid = 0;
  sqlite3_int64 iMin;
  sqlite3_int64 iMax;

  if( idxNum==SQLITE_INDEX_CONSTRAINT_EQ ){
    assert( argc==1 );
    iMin = iMax = sqlite3_value_int64(argv[0]);
    bMinValid = bMaxValid = 1;
  }else{
    if( idxNum & SQLITE_INDEX_CONSTRAINT_LE ){
      assert( argc>=1 );
      iMax = sqlite3_value_int64(argv[0]);
      bMaxValid = 1;
    }
    if( idxNum & SQLITE_INDEX_CONSTRAINT_GE ){
      assert( argc>=1 );
      iMin = sqlite3_value_int64(argv[argc-1]);
      bMinValid = 1;
    }
  }


  sqlite3_finalize(pCsr->pStmt);
  pCsr->pStmt = 0;

  for(i=0; i<pTab->nSrc; i++){
    UnionSrc *pSrc = &pTab->aSrc[i];
    if( (bMinValid && iMin>pSrc->iMax) || (bMaxValid && iMax<pSrc->iMin) ){
      continue;
    }

    zSql = sqlite3_mprintf("%z%sSELECT rowid, * FROM %s%q%s%Q"
        , zSql
        , (zSql ? " UNION ALL " : "")
        , (pSrc->zDb ? "'" : "")
        , (pSrc->zDb ? pSrc->zDb : "")
        , (pSrc->zDb ? "'." : "")
        , pSrc->zTab
    );
    if( zSql==0 ){
      rc = SQLITE_NOMEM;
      break;
    }

    if( zSql ){
      if( bMinValid && bMaxValid && iMin==iMax ){
        zSql = sqlite3_mprintf("%z WHERE rowid=%lld", zSql, iMin);
      }else{
        const char *zWhere = "WHERE";
        if( bMinValid && iMin>pSrc->iMin ){
          zSql = sqlite3_mprintf("%z WHERE rowid>=%lld", zSql, iMin);
          zWhere = "AND";
        }
        if( bMaxValid && iMax<pSrc->iMax ){
          zSql = sqlite3_mprintf("%z %s rowid<=%lld", zSql, zWhere, iMax);
        }
      }
    }
  }

  if( rc==SQLITE_OK ){
    pCsr->pStmt = unionPrepare(&rc, pTab->db, zSql, &pTab->base.zErrMsg);
  }
  sqlite3_free(zSql);

  if( rc!=SQLITE_OK ) return rc;
  return unionNext(pVtabCursor);
}

/*
** xBestIndex.
*/
static int unionBestIndex(
  sqlite3_vtab *tab,
  sqlite3_index_info *pIdxInfo
){
  int iEq = -1;
  int iLt = -1;
  int iGt = -1;
  int i;

  for(i=0; i<pIdxInfo->nConstraint; i++){
    struct sqlite3_index_constraint *p = &pIdxInfo->aConstraint[i];
    if( p->usable && p->iColumn<0 ){
      switch( p->op ){
        case SQLITE_INDEX_CONSTRAINT_EQ:
          if( iEq<0 ) iEq = i;
          break;
        case SQLITE_INDEX_CONSTRAINT_LE:
        case SQLITE_INDEX_CONSTRAINT_LT:
          if( iLt<0 ) iLt = i;
          break;
        case SQLITE_INDEX_CONSTRAINT_GE:
        case SQLITE_INDEX_CONSTRAINT_GT:
          if( iGt<0 ) iGt = i;
          break;
      }
    }
  }

  if( iEq>=0 ){
    pIdxInfo->estimatedRows = 1;
    pIdxInfo->idxFlags = SQLITE_INDEX_SCAN_UNIQUE;
    pIdxInfo->estimatedCost = 3.0;
    pIdxInfo->idxNum = SQLITE_INDEX_CONSTRAINT_EQ;
    pIdxInfo->aConstraintUsage[iEq].argvIndex = 1;
  }else{
    int iCons = 1;
    int idxNum = 0;
    sqlite3_int64 nRow = 1000000;
    if( iLt>=0 ){
      nRow = nRow / 2;
      pIdxInfo->aConstraintUsage[iLt].argvIndex = iCons++;
      idxNum |= SQLITE_INDEX_CONSTRAINT_LE;
    }
    if( iGt>=0 ){
      nRow = nRow / 2;
      pIdxInfo->aConstraintUsage[iGt].argvIndex = iCons++;
      idxNum |= SQLITE_INDEX_CONSTRAINT_GE;
    }
    pIdxInfo->estimatedRows = nRow;
    pIdxInfo->estimatedCost = 3.0 * (double)nRow;
    pIdxInfo->idxNum = idxNum;
  }

  return SQLITE_OK;
}

static int createUnionVtab(sqlite3 *db){
  static sqlite3_module unionModule = {
    0,                            /* iVersion */
    unionConnect,
    unionConnect,
    unionBestIndex,               /* xBestIndex - query planner */
    unionDisconnect, 
    unionDisconnect,
    unionOpen,                    /* xOpen - open a cursor */
    unionClose,                   /* xClose - close a cursor */
    unionFilter,                  /* xFilter - configure scan constraints */
    unionNext,                    /* xNext - advance a cursor */
    unionEof,                     /* xEof - check for end of scan */
    unionColumn,                  /* xColumn - read data */
    unionRowid,                   /* xRowid - read data */
    0,                            /* xUpdate */
    0,                            /* xBegin */
    0,                            /* xSync */
    0,                            /* xCommit */
    0,                            /* xRollback */
    0,                            /* xFindMethod */
    0,                            /* xRename */
  };

  return sqlite3_create_module(db, "unionvtab", &unionModule, 0);
}

#endif /* SQLITE_OMIT_VIRTUALTABLE */

#ifdef _WIN32
__declspec(dllexport)
#endif
int sqlite3_unionvtab_init(
  sqlite3 *db, 
  char **pzErrMsg, 
  const sqlite3_api_routines *pApi
){
  int rc = SQLITE_OK;
  SQLITE_EXTENSION_INIT2(pApi);
#ifndef SQLITE_OMIT_VIRTUALTABLE
  rc = createUnionVtab(db);
#endif
  return rc;
}

#endif /* !defined(SQLITE_CORE) || defined(SQLITE_ENABLE_UNIONVTAB) */

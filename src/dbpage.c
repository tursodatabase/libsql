/*
** 2017-10-11
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
** This file contains an implementation of the "sqlite_dbpage" virtual table.
**
** The sqlite_dbpage virtual table is used to read or write whole raw
** pages of the database file.  The pager interface is used so that 
** uncommitted changes and changes recorded in the WAL file are correctly
** retrieved.
**
** Usage example:
**
**    SELECT data FROM sqlite_dbpage('aux1') WHERE pgno=123;
**
** This is an eponymous virtual table so it does not need to be created before
** use.  The optional argument to the sqlite_dbpage() table name is the
** schema for the database file that is to be read.  The default schema is
** "main".
**
** The data field of sqlite_dbpage table can be updated.  The new
** value must be a BLOB which is the correct page size, otherwise the
** update fails.  Rows may not be deleted or inserted.
*/

#include "sqliteInt.h"   /* Requires access to internal data structures */
#if (defined(SQLITE_ENABLE_DBPAGE_VTAB) || defined(SQLITE_TEST)) \
    && !defined(SQLITE_OMIT_VIRTUALTABLE)

typedef struct DbpageTable DbpageTable;
typedef struct DbpageCursor DbpageCursor;

struct DbpageCursor {
  sqlite3_vtab_cursor base;       /* Base class.  Must be first */
  int pgno;                       /* Current page number */
  int mxPgno;                     /* Last page to visit on this scan */
};

struct DbpageTable {
  sqlite3_vtab base;              /* Base class.  Must be first */
  sqlite3 *db;                    /* The database */
  Pager *pPager;                  /* Pager being read/written */
  int iDb;                        /* Index of database to analyze */
  int szPage;                     /* Size of each page in bytes */
  int nPage;                      /* Number of pages in the file */
};

/*
** Connect to or create a dbpagevfs virtual table.
*/
static int dbpageConnect(
  sqlite3 *db,
  void *pAux,
  int argc, const char *const*argv,
  sqlite3_vtab **ppVtab,
  char **pzErr
){
  DbpageTable *pTab = 0;
  int rc = SQLITE_OK;
  int iDb;

  if( argc>=4 ){
    Token nm;
    sqlite3TokenInit(&nm, (char*)argv[3]);
    iDb = sqlite3FindDb(db, &nm);
    if( iDb<0 ){
      *pzErr = sqlite3_mprintf("no such schema: %s", argv[3]);
      return SQLITE_ERROR;
    }
  }else{
    iDb = 0;
  }
  rc = sqlite3_declare_vtab(db, 
          "CREATE TABLE x(pgno INTEGER PRIMARY KEY, data BLOB, schema HIDDEN)");
  if( rc==SQLITE_OK ){
    pTab = (DbpageTable *)sqlite3_malloc64(sizeof(DbpageTable));
    if( pTab==0 ) rc = SQLITE_NOMEM_BKPT;
  }

  assert( rc==SQLITE_OK || pTab==0 );
  if( rc==SQLITE_OK ){
    Btree *pBt = db->aDb[iDb].pBt;
    memset(pTab, 0, sizeof(DbpageTable));
    pTab->db = db;
    pTab->iDb = iDb;
    pTab->pPager = pBt ? sqlite3BtreePager(pBt) : 0;
  }

  *ppVtab = (sqlite3_vtab*)pTab;
  return rc;
}

/*
** Disconnect from or destroy a dbpagevfs virtual table.
*/
static int dbpageDisconnect(sqlite3_vtab *pVtab){
  sqlite3_free(pVtab);
  return SQLITE_OK;
}

/*
** idxNum:
**
**     0     full table scan
**     1     pgno=?1
*/
static int dbpageBestIndex(sqlite3_vtab *tab, sqlite3_index_info *pIdxInfo){
  int i;
  pIdxInfo->estimatedCost = 1.0e6;  /* Initial cost estimate */
  for(i=0; i<pIdxInfo->nConstraint; i++){
    struct sqlite3_index_constraint *p = &pIdxInfo->aConstraint[i];
    if( p->usable && p->iColumn<=0 && p->op==SQLITE_INDEX_CONSTRAINT_EQ ){
      pIdxInfo->estimatedRows = 1;
      pIdxInfo->idxFlags = SQLITE_INDEX_SCAN_UNIQUE;
      pIdxInfo->estimatedCost = 1.0;
      pIdxInfo->idxNum = 1;
      pIdxInfo->aConstraintUsage[i].argvIndex = 1;
      pIdxInfo->aConstraintUsage[i].omit = 1;
      break;
    }
  }
  if( pIdxInfo->nOrderBy>=1
   && pIdxInfo->aOrderBy[0].iColumn<=0
   && pIdxInfo->aOrderBy[0].desc==0
  ){
    pIdxInfo->orderByConsumed = 1;
  }
  return SQLITE_OK;
}

/*
** Open a new dbpagevfs cursor.
*/
static int dbpageOpen(sqlite3_vtab *pVTab, sqlite3_vtab_cursor **ppCursor){
  DbpageCursor *pCsr;

  pCsr = (DbpageCursor *)sqlite3_malloc64(sizeof(DbpageCursor));
  if( pCsr==0 ){
    return SQLITE_NOMEM_BKPT;
  }else{
    memset(pCsr, 0, sizeof(DbpageCursor));
    pCsr->base.pVtab = pVTab;
    pCsr->pgno = -1;
  }

  *ppCursor = (sqlite3_vtab_cursor *)pCsr;
  return SQLITE_OK;
}

/*
** Close a dbpagevfs cursor.
*/
static int dbpageClose(sqlite3_vtab_cursor *pCursor){
  DbpageCursor *pCsr = (DbpageCursor *)pCursor;
  sqlite3_free(pCsr);
  return SQLITE_OK;
}

/*
** Move a dbpagevfs cursor to the next entry in the file.
*/
static int dbpageNext(sqlite3_vtab_cursor *pCursor){
  int rc = SQLITE_OK;
  DbpageCursor *pCsr = (DbpageCursor *)pCursor;
  pCsr->pgno++;
  return rc;
}

static int dbpageEof(sqlite3_vtab_cursor *pCursor){
  DbpageCursor *pCsr = (DbpageCursor *)pCursor;
  return pCsr->pgno > pCsr->mxPgno;
}

static int dbpageFilter(
  sqlite3_vtab_cursor *pCursor, 
  int idxNum, const char *idxStr,
  int argc, sqlite3_value **argv
){
  DbpageCursor *pCsr = (DbpageCursor *)pCursor;
  DbpageTable *pTab = (DbpageTable *)pCursor->pVtab;
  int rc = SQLITE_OK;
  Btree *pBt = pTab->db->aDb[pTab->iDb].pBt;

  pTab->szPage = sqlite3BtreeGetPageSize(pBt);
  pTab->nPage = sqlite3BtreeLastPage(pBt);
  if( idxNum==1 ){
    pCsr->pgno = sqlite3_value_int(argv[0]);
    if( pCsr->pgno<1 || pCsr->pgno>pTab->nPage ){
      pCsr->pgno = 1;
      pCsr->mxPgno = 0;
    }else{
      pCsr->mxPgno = pCsr->pgno;
    }
  }else{
    pCsr->pgno = 1;
    pCsr->mxPgno = pTab->nPage;
  }
  return rc;
}

static int dbpageColumn(
  sqlite3_vtab_cursor *pCursor, 
  sqlite3_context *ctx, 
  int i
){
  DbpageCursor *pCsr = (DbpageCursor *)pCursor;
  DbpageTable *pTab = (DbpageTable *)pCursor->pVtab;
  int rc = SQLITE_OK;
  switch( i ){
    case 0: {           /* pgno */
      sqlite3_result_int(ctx, pCsr->pgno);
      break;
    }
    case 1: {           /* data */
      DbPage *pDbPage = 0;
      rc = sqlite3PagerGet(pTab->pPager, pCsr->pgno, (DbPage**)&pDbPage, 0);
      if( rc==SQLITE_OK ){
        sqlite3_result_blob(ctx, sqlite3PagerGetData(pDbPage), pTab->szPage,
                            SQLITE_TRANSIENT);
      }
      sqlite3PagerUnref(pDbPage);
      break;
    }
    default: {          /* schema */
      sqlite3 *db = sqlite3_context_db_handle(ctx);
      sqlite3_result_text(ctx, db->aDb[pTab->iDb].zDbSName, -1, SQLITE_STATIC);
      break;
    }
  }
  return SQLITE_OK;
}

static int dbpageRowid(sqlite3_vtab_cursor *pCursor, sqlite_int64 *pRowid){
  DbpageCursor *pCsr = (DbpageCursor *)pCursor;
  *pRowid = pCsr->pgno;
  return SQLITE_OK;
}

static int dbpageUpdate(
  sqlite3_vtab *pVtab,
  int argc,
  sqlite3_value **argv,
  sqlite_int64 *pRowid
){
  DbpageTable *pTab = (DbpageTable *)pVtab;
  int pgno;
  DbPage *pDbPage = 0;
  int rc = SQLITE_OK;
  char *zErr = 0;

  if( argc==1 ){
    zErr = "cannot delete";
    goto update_fail;
  }
  pgno = sqlite3_value_int(argv[0]);
  if( pgno<1 || pgno>pTab->nPage ){
    zErr = "bad page number";
    goto update_fail;
  }
  if( sqlite3_value_int(argv[1])!=pgno ){
    zErr = "cannot insert";
    goto update_fail;
  }
  if( sqlite3_value_type(argv[3])!=SQLITE_BLOB 
   || sqlite3_value_bytes(argv[3])!=pTab->szPage 
  ){
    zErr = "bad page value";
    goto update_fail;
  }
  rc = sqlite3PagerGet(pTab->pPager, pgno, (DbPage**)&pDbPage, 0);
  if( rc==SQLITE_OK ){
    rc = sqlite3PagerWrite(pDbPage);
    if( rc==SQLITE_OK ){
      memcpy(sqlite3PagerGetData(pDbPage),
             sqlite3_value_blob(argv[3]),
             pTab->szPage);
    }
  }
  sqlite3PagerUnref(pDbPage);
  return rc;

update_fail:
  sqlite3_free(pVtab->zErrMsg);
  pVtab->zErrMsg = sqlite3_mprintf("%s", zErr);
  return SQLITE_ERROR;
}

/*
** Invoke this routine to register the "dbpage" virtual table module
*/
int sqlite3DbpageRegister(sqlite3 *db){
  static sqlite3_module dbpage_module = {
    0,                            /* iVersion */
    dbpageConnect,                /* xCreate */
    dbpageConnect,                /* xConnect */
    dbpageBestIndex,              /* xBestIndex */
    dbpageDisconnect,             /* xDisconnect */
    dbpageDisconnect,             /* xDestroy */
    dbpageOpen,                   /* xOpen - open a cursor */
    dbpageClose,                  /* xClose - close a cursor */
    dbpageFilter,                 /* xFilter - configure scan constraints */
    dbpageNext,                   /* xNext - advance a cursor */
    dbpageEof,                    /* xEof - check for end of scan */
    dbpageColumn,                 /* xColumn - read data */
    dbpageRowid,                  /* xRowid - read data */
    dbpageUpdate,                 /* xUpdate */
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
  return sqlite3_create_module(db, "sqlite_dbpage", &dbpage_module, 0);
}
#elif defined(SQLITE_ENABLE_DBPAGE_VTAB)
int sqlite3DbpageRegister(sqlite3 *db){ return SQLITE_OK; }
#endif /* SQLITE_ENABLE_DBSTAT_VTAB */

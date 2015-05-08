/*
** 2015 May 08
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
** This is an SQLite module implementing full-text search.
*/

#if defined(SQLITE_ENABLE_FTS5)

#include "fts5Int.h"


typedef struct Fts5VocabTable Fts5VocabTable;
typedef struct Fts5VocabCursor Fts5VocabCursor;

struct Fts5VocabTable {
  sqlite3_vtab base;
  char *zFts5Tbl;                 /* Name of fts5 table */
  char *zFts5Db;                  /* Db containing fts5 table */
  sqlite3 *db;                    /* Database handle */
  Fts5Global *pGlobal;            /* FTS5 global object for this database */
};

struct Fts5VocabCursor {
  sqlite3_vtab_cursor base;
  sqlite3_stmt *pStmt;            /* Statement holding lock on pIndex */
  Fts5Index *pIndex;              /* Associated FTS5 index */

  Fts5IndexIter *pIter;           /* Iterator object */
  int bEof;                       /* True if this cursor is at EOF */
  Fts5Buffer term;                /* Current value of 'term' column */
  i64 nRow;                       /* Current value of 'row' column */
  i64 nInst;                      /* Current value of 'inst' column */
  i64 rowid;                      /* Current value of rowid column */
};


/*
** The xDisconnect() virtual table method.
*/
static int fts5VocabDisconnectMethod(sqlite3_vtab *pVtab){
  Fts5VocabTable *pTab = (Fts5VocabTable*)pVtab;
  sqlite3_free(pTab);
  return SQLITE_OK;
}

/*
** The xDestroy() virtual table method.
*/
static int fts5VocabDestroyMethod(sqlite3_vtab *pVtab){
  Fts5VocabTable *pTab = (Fts5VocabTable*)pVtab;
  sqlite3_free(pTab);
  return SQLITE_OK;
}

/*
** This function is the implementation of both the xConnect and xCreate
** methods of the FTS3 virtual table.
**
** The argv[] array contains the following:
**
**   argv[0]   -> module name  ("fts5vocab")
**   argv[1]   -> database name
**   argv[2]   -> table name
**   argv[3]   -> name of fts5 table
*/
static int fts5VocabInitVtab(
  sqlite3 *db,                    /* The SQLite database connection */
  void *pAux,                     /* Pointer to Fts5Global object */
  int argc,                       /* Number of elements in argv array */
  const char * const *argv,       /* xCreate/xConnect argument array */
  sqlite3_vtab **ppVTab,          /* Write the resulting vtab structure here */
  char **pzErr                    /* Write any error message here */
){
  const char *zSchema = "CREATE TABLE vvv(term, row, inst)";
  Fts5VocabTable *pRet = 0;
  int rc = SQLITE_OK;             /* Return code */

  if( argc!=4 ){
    *pzErr = sqlite3_mprintf("wrong number of vtable arguments");
    rc = SQLITE_ERROR;
  }else{
    int nByte;                      /* Bytes of space to allocate */
    const char *zDb = argv[1];
    const char *zTab = argv[3];
    int nDb = strlen(zDb) + 1;
    int nTab = strlen(zTab) + 1;

    rc = sqlite3_declare_vtab(db, zSchema);

    nByte = sizeof(Fts5VocabTable) + nDb + nTab;
    pRet = sqlite3Fts5MallocZero(&rc, nByte);
    if( pRet ){
      pRet->pGlobal = (Fts5Global*)pAux;
      pRet->db = db;
      pRet->zFts5Tbl = (char*)&pRet[1];
      pRet->zFts5Db = &pRet->zFts5Tbl[nTab];
      memcpy(pRet->zFts5Tbl, zTab, nTab);
      memcpy(pRet->zFts5Db, zDb, nDb);
    }
  }

  *ppVTab = (sqlite3_vtab*)pRet;
  return rc;
}


/*
** The xConnect() and xCreate() methods for the virtual table. All the
** work is done in function fts5VocabInitVtab().
*/
static int fts5VocabConnectMethod(
  sqlite3 *db,                    /* Database connection */
  void *pAux,                     /* Pointer to tokenizer hash table */
  int argc,                       /* Number of elements in argv array */
  const char * const *argv,       /* xCreate/xConnect argument array */
  sqlite3_vtab **ppVtab,          /* OUT: New sqlite3_vtab object */
  char **pzErr                    /* OUT: sqlite3_malloc'd error message */
){
  return fts5VocabInitVtab(db, pAux, argc, argv, ppVtab, pzErr);
}
static int fts5VocabCreateMethod(
  sqlite3 *db,                    /* Database connection */
  void *pAux,                     /* Pointer to tokenizer hash table */
  int argc,                       /* Number of elements in argv array */
  const char * const *argv,       /* xCreate/xConnect argument array */
  sqlite3_vtab **ppVtab,          /* OUT: New sqlite3_vtab object */
  char **pzErr                    /* OUT: sqlite3_malloc'd error message */
){
  return fts5VocabInitVtab(db, pAux, argc, argv, ppVtab, pzErr);
}

/* 
** Implementation of the xBestIndex method.
*/
static int fts5VocabBestIndexMethod(
  sqlite3_vtab *pVTab, 
  sqlite3_index_info *pInfo
){
  return SQLITE_OK;
}

/*
** Implementation of xOpen method.
*/
static int fts5VocabOpenMethod(
  sqlite3_vtab *pVTab, 
  sqlite3_vtab_cursor **ppCsr
){
  Fts5VocabTable *pTab = (Fts5VocabTable*)pVTab;
  Fts5VocabCursor *pCsr;
  int rc = SQLITE_OK;

  pCsr = (Fts5VocabCursor*)sqlite3Fts5MallocZero(&rc, sizeof(Fts5VocabCursor));
  if( pCsr ){
    char *zSql = sqlite3_mprintf(
        "SELECT t.%Q FROM %Q.%Q AS t WHERE t.%Q MATCH '*id'",
        pTab->zFts5Tbl, pTab->zFts5Db, pTab->zFts5Tbl, pTab->zFts5Tbl
    );
    if( zSql==0 ){
      rc = SQLITE_NOMEM;
    }else{
      rc = sqlite3_prepare_v2(pTab->db, zSql, -1, &pCsr->pStmt, 0);
    }
    sqlite3_free(zSql);
    if( rc==SQLITE_OK && sqlite3_step(pCsr->pStmt)==SQLITE_ROW ){
      i64 iId = sqlite3_column_int64(pCsr->pStmt, 0);
      pCsr->pIndex = sqlite3Fts5IndexFromCsrid(pTab->pGlobal, iId);
    }

    if( rc==SQLITE_OK && pCsr->pIndex==0 ){
      rc = sqlite3_finalize(pCsr->pStmt);
      pCsr->pStmt = 0;
      if( rc==SQLITE_OK ){
        pVTab->zErrMsg = sqlite3_mprintf(
            "no such fts5 table: %Q.%Q", pTab->zFts5Db, pTab->zFts5Tbl
        );
        rc = SQLITE_ERROR;
      }
    }

    if( rc!=SQLITE_OK ){
      sqlite3_free(pCsr);
      pCsr = 0;
    }
  }


  *ppCsr = (sqlite3_vtab_cursor*)pCsr;
  return rc;
}

static void fts5VocabResetCursor(Fts5VocabCursor *pCsr){
  pCsr->rowid = 0;
  sqlite3Fts5IterClose(pCsr->pIter);
  pCsr->pIter = 0;
}

/*
** Close the cursor.  For additional information see the documentation
** on the xClose method of the virtual table interface.
*/
static int fts5VocabCloseMethod(sqlite3_vtab_cursor *pCursor){
  if( pCursor ){
    Fts5VocabCursor *pCsr = (Fts5VocabCursor*)pCursor;
    fts5VocabResetCursor(pCsr);
    sqlite3Fts5BufferFree(&pCsr->term);
    sqlite3_finalize(pCsr->pStmt);
    sqlite3_free(pCsr);
  }
  return SQLITE_OK;
}


/*
** Advance the cursor to the next row in the table.
*/
static int fts5VocabNextMethod(sqlite3_vtab_cursor *pCursor){
  Fts5VocabCursor *pCsr = (Fts5VocabCursor*)pCursor;
  int rc = SQLITE_OK;

  if( sqlite3Fts5IterEof(pCsr->pIter) ){
    pCsr->bEof = 1;
  }else{
    const char *zTerm;
    int nTerm;

    zTerm = sqlite3Fts5IterTerm(pCsr->pIter, &nTerm);
    sqlite3Fts5BufferSet(&rc, &pCsr->term, nTerm, (const u8*)zTerm);
    pCsr->nInst = 0;
    pCsr->nRow = 0;
    pCsr->rowid++;

    while( 1 ){
      const u8 *pPos; int nPos;   /* Position list */
      i64 dummy = 0;
      int iOff = 0;

      rc = sqlite3Fts5IterPoslist(pCsr->pIter, &pPos, &nPos);
      if( rc!=SQLITE_OK ) break;
      while( 0==sqlite3Fts5PoslistNext64(pPos, nPos, &iOff, &dummy) ){
        pCsr->nInst++;
      }
      pCsr->nRow++;

      rc = sqlite3Fts5IterNextScan(pCsr->pIter);
      if( rc!=SQLITE_OK ) break;
      zTerm = sqlite3Fts5IterTerm(pCsr->pIter, &nTerm);
      if( nTerm!=pCsr->term.n || memcmp(zTerm, pCsr->term.p, nTerm) ) break;
      if( sqlite3Fts5IterEof(pCsr->pIter) ) break;
    }
  }
  return rc;
}

/*
** This is the xFilter implementation for the virtual table.
*/
static int fts5VocabFilterMethod(
  sqlite3_vtab_cursor *pCursor,   /* The cursor used for this query */
  int idxNum,                     /* Strategy index */
  const char *idxStr,             /* Unused */
  int nVal,                       /* Number of elements in apVal */
  sqlite3_value **apVal           /* Arguments for the indexing scheme */
){
  Fts5VocabCursor *pCsr = (Fts5VocabCursor*)pCursor;
  int rc;
  const int flags = FTS5INDEX_QUERY_SCAN;

  fts5VocabResetCursor(pCsr);
  rc = sqlite3Fts5IndexQuery(pCsr->pIndex, 0, 0, flags, &pCsr->pIter);
  if( rc==SQLITE_OK ){
    rc = fts5VocabNextMethod(pCursor);
  }

  return rc;
}

/* 
** This is the xEof method of the virtual table. SQLite calls this 
** routine to find out if it has reached the end of a result set.
*/
static int fts5VocabEofMethod(sqlite3_vtab_cursor *pCursor){
  Fts5VocabCursor *pCsr = (Fts5VocabCursor*)pCursor;
  return pCsr->bEof;
}

static int fts5VocabColumnMethod(
  sqlite3_vtab_cursor *pCursor,   /* Cursor to retrieve value from */
  sqlite3_context *pCtx,          /* Context for sqlite3_result_xxx() calls */
  int iCol                        /* Index of column to read value from */
){
  Fts5VocabCursor *pCsr = (Fts5VocabCursor*)pCursor;
  switch( iCol ){
    case 0: /* term */
      sqlite3_result_text(
          pCtx, (const char*)pCsr->term.p, pCsr->term.n, SQLITE_TRANSIENT
      );
      break;

    case 1: /* row */
      sqlite3_result_int64(pCtx, pCsr->nRow);
      break;

    case 2: /* inst */
      sqlite3_result_int64(pCtx, pCsr->nInst);
      break;

    default:
      assert( 0 );
  }
  return SQLITE_OK;
}

/* 
** This is the xRowid method. The SQLite core calls this routine to
** retrieve the rowid for the current row of the result set. fts5
** exposes %_content.docid as the rowid for the virtual table. The
** rowid should be written to *pRowid.
*/
static int fts5VocabRowidMethod(
  sqlite3_vtab_cursor *pCursor, 
  sqlite_int64 *pRowid
){
  Fts5VocabCursor *pCsr = (Fts5VocabCursor*)pCursor;
  *pRowid = pCsr->rowid;
  return SQLITE_OK;
}

int sqlite3Fts5VocabInit(Fts5Global *pGlobal, sqlite3 *db){
  static const sqlite3_module fts5Vocab = {
    /* iVersion      */ 2,
    /* xCreate       */ fts5VocabCreateMethod,
    /* xConnect      */ fts5VocabConnectMethod,
    /* xBestIndex    */ fts5VocabBestIndexMethod,
    /* xDisconnect   */ fts5VocabDisconnectMethod,
    /* xDestroy      */ fts5VocabDestroyMethod,
    /* xOpen         */ fts5VocabOpenMethod,
    /* xClose        */ fts5VocabCloseMethod,
    /* xFilter       */ fts5VocabFilterMethod,
    /* xNext         */ fts5VocabNextMethod,
    /* xEof          */ fts5VocabEofMethod,
    /* xColumn       */ fts5VocabColumnMethod,
    /* xRowid        */ fts5VocabRowidMethod,
    /* xUpdate       */ 0,
    /* xBegin        */ 0,
    /* xSync         */ 0,
    /* xCommit       */ 0,
    /* xRollback     */ 0,
    /* xFindFunction */ 0,
    /* xRename       */ 0,
    /* xSavepoint    */ 0,
    /* xRelease      */ 0,
    /* xRollbackTo   */ 0,
  };
  void *p = (void*)pGlobal;

  return sqlite3_create_module_v2(db, "fts5vocab", &fts5Vocab, p, 0);
}
#endif /* defined(SQLITE_ENABLE_FTS5) */



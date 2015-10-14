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
** This is an SQLite virtual table module implementing direct access to an
** existing FTS5 index. The module may create several different types of 
** tables:
**
** col:
**     CREATE TABLE vocab(term, col, doc, cnt, PRIMARY KEY(term, col));
**
**   One row for each term/column combination. The value of $doc is set to
**   the number of fts5 rows that contain at least one instance of term
**   $term within column $col. Field $cnt is set to the total number of 
**   instances of term $term in column $col (in any row of the fts5 table). 
**
** row:
**     CREATE TABLE vocab(term, doc, cnt, PRIMARY KEY(term));
**
**   One row for each term in the database. The value of $doc is set to
**   the number of fts5 rows that contain at least one instance of term
**   $term. Field $cnt is set to the total number of instances of term 
**   $term in the database.
*/


#include "fts5Int.h"


typedef struct Fts5VocabTable Fts5VocabTable;
typedef struct Fts5VocabCursor Fts5VocabCursor;

struct Fts5VocabTable {
  sqlite3_vtab base;
  char *zFts5Tbl;                 /* Name of fts5 table */
  char *zFts5Db;                  /* Db containing fts5 table */
  sqlite3 *db;                    /* Database handle */
  Fts5Global *pGlobal;            /* FTS5 global object for this database */
  int eType;                      /* FTS5_VOCAB_COL or ROW */
};

struct Fts5VocabCursor {
  sqlite3_vtab_cursor base;
  sqlite3_stmt *pStmt;            /* Statement holding lock on pIndex */
  Fts5Index *pIndex;              /* Associated FTS5 index */

  int bEof;                       /* True if this cursor is at EOF */
  Fts5IndexIter *pIter;           /* Term/rowid iterator object */

  /* These are used by 'col' tables only */
  int nCol;
  int iCol;
  i64 *aCnt;
  i64 *aDoc;

  /* Output values */
  i64 rowid;                      /* This table's current rowid value */
  Fts5Buffer term;                /* Current value of 'term' column */
  i64 aVal[3];                    /* Up to three columns left of 'term' */
};

#define FTS5_VOCAB_COL    0
#define FTS5_VOCAB_ROW    1

#define FTS5_VOCAB_COL_SCHEMA  "term, col, doc, cnt"
#define FTS5_VOCAB_ROW_SCHEMA  "term, doc, cnt"

/*
** Translate a string containing an fts5vocab table type to an 
** FTS5_VOCAB_XXX constant. If successful, set *peType to the output
** value and return SQLITE_OK. Otherwise, set *pzErr to an error message
** and return SQLITE_ERROR.
*/
static int fts5VocabTableType(const char *zType, char **pzErr, int *peType){
  int rc = SQLITE_OK;
  char *zCopy = sqlite3Fts5Strndup(&rc, zType, -1);
  if( rc==SQLITE_OK ){
    sqlite3Fts5Dequote(zCopy);
    if( sqlite3_stricmp(zCopy, "col")==0 ){
      *peType = FTS5_VOCAB_COL;
    }else

    if( sqlite3_stricmp(zCopy, "row")==0 ){
      *peType = FTS5_VOCAB_ROW;
    }else
    {
      *pzErr = sqlite3_mprintf("fts5vocab: unknown table type: %Q", zCopy);
      rc = SQLITE_ERROR;
    }
    sqlite3_free(zCopy);
  }

  return rc;
}


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
**
** then:
**
**   argv[3]   -> name of fts5 table
**   argv[4]   -> type of fts5vocab table
**
** or, for tables in the TEMP schema only.
**
**   argv[3]   -> name of fts5 tables database
**   argv[4]   -> name of fts5 table
**   argv[5]   -> type of fts5vocab table
*/
static int fts5VocabInitVtab(
  sqlite3 *db,                    /* The SQLite database connection */
  void *pAux,                     /* Pointer to Fts5Global object */
  int argc,                       /* Number of elements in argv array */
  const char * const *argv,       /* xCreate/xConnect argument array */
  sqlite3_vtab **ppVTab,          /* Write the resulting vtab structure here */
  char **pzErr                    /* Write any error message here */
){
  const char *azSchema[] = { 
    "CREATE TABlE vocab(" FTS5_VOCAB_COL_SCHEMA  ")", 
    "CREATE TABlE vocab(" FTS5_VOCAB_ROW_SCHEMA  ")"
  };

  Fts5VocabTable *pRet = 0;
  int rc = SQLITE_OK;             /* Return code */
  int bDb;

  bDb = (argc==6 && strlen(argv[1])==4 && memcmp("temp", argv[1], 4)==0);

  if( argc!=5 && bDb==0 ){
    *pzErr = sqlite3_mprintf("wrong number of vtable arguments");
    rc = SQLITE_ERROR;
  }else{
    int nByte;                      /* Bytes of space to allocate */
    const char *zDb = bDb ? argv[3] : argv[1];
    const char *zTab = bDb ? argv[4] : argv[3];
    const char *zType = bDb ? argv[5] : argv[4];
    int nDb = strlen(zDb)+1; 
    int nTab = strlen(zTab)+1;
    int eType;
    
    rc = fts5VocabTableType(zType, pzErr, &eType);
    if( rc==SQLITE_OK ){
      assert( eType>=0 && eType<sizeof(azSchema)/sizeof(azSchema[0]) );
      rc = sqlite3_declare_vtab(db, azSchema[eType]);
    }

    nByte = sizeof(Fts5VocabTable) + nDb + nTab;
    pRet = sqlite3Fts5MallocZero(&rc, nByte);
    if( pRet ){
      pRet->pGlobal = (Fts5Global*)pAux;
      pRet->eType = eType;
      pRet->db = db;
      pRet->zFts5Tbl = (char*)&pRet[1];
      pRet->zFts5Db = &pRet->zFts5Tbl[nTab];
      memcpy(pRet->zFts5Tbl, zTab, nTab);
      memcpy(pRet->zFts5Db, zDb, nDb);
      sqlite3Fts5Dequote(pRet->zFts5Tbl);
      sqlite3Fts5Dequote(pRet->zFts5Db);
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
  Fts5Index *pIndex = 0;
  int nCol = 0;
  Fts5VocabCursor *pCsr = 0;
  int rc = SQLITE_OK;
  sqlite3_stmt *pStmt = 0;
  char *zSql = 0;
  int nByte;

  zSql = sqlite3Fts5Mprintf(&rc,
      "SELECT t.%Q FROM %Q.%Q AS t WHERE t.%Q MATCH '*id'",
      pTab->zFts5Tbl, pTab->zFts5Db, pTab->zFts5Tbl, pTab->zFts5Tbl
  );
  if( zSql ){
    rc = sqlite3_prepare_v2(pTab->db, zSql, -1, &pStmt, 0);
  }
  sqlite3_free(zSql);
  assert( rc==SQLITE_OK || pStmt==0 );
  if( rc==SQLITE_ERROR ) rc = SQLITE_OK;

  if( pStmt && sqlite3_step(pStmt)==SQLITE_ROW ){
    i64 iId = sqlite3_column_int64(pStmt, 0);
    pIndex = sqlite3Fts5IndexFromCsrid(pTab->pGlobal, iId, &nCol);
  }

  if( rc==SQLITE_OK && pIndex==0 ){
    rc = sqlite3_finalize(pStmt);
    pStmt = 0;
    if( rc==SQLITE_OK ){
      pVTab->zErrMsg = sqlite3_mprintf(
          "no such fts5 table: %s.%s", pTab->zFts5Db, pTab->zFts5Tbl
      );
      rc = SQLITE_ERROR;
    }
  }

  nByte = nCol * sizeof(i64) * 2 + sizeof(Fts5VocabCursor);
  pCsr = (Fts5VocabCursor*)sqlite3Fts5MallocZero(&rc, nByte);
  if( pCsr ){
    pCsr->pIndex = pIndex;
    pCsr->pStmt = pStmt;
    pCsr->nCol = nCol;
    pCsr->aCnt = (i64*)&pCsr[1];
    pCsr->aDoc = &pCsr->aCnt[nCol];
  }else{
    sqlite3_finalize(pStmt);
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
  Fts5VocabCursor *pCsr = (Fts5VocabCursor*)pCursor;
  fts5VocabResetCursor(pCsr);
  sqlite3Fts5BufferFree(&pCsr->term);
  sqlite3_finalize(pCsr->pStmt);
  sqlite3_free(pCsr);
  return SQLITE_OK;
}


/*
** Advance the cursor to the next row in the table.
*/
static int fts5VocabNextMethod(sqlite3_vtab_cursor *pCursor){
  Fts5VocabCursor *pCsr = (Fts5VocabCursor*)pCursor;
  Fts5VocabTable *pTab = (Fts5VocabTable*)pCursor->pVtab;
  int rc = SQLITE_OK;

  pCsr->rowid++;

  if( pTab->eType==FTS5_VOCAB_COL ){
    for(pCsr->iCol++; pCsr->iCol<pCsr->nCol; pCsr->iCol++){
      if( pCsr->aCnt[pCsr->iCol] ) break;
    }
  }

  if( pTab->eType==FTS5_VOCAB_ROW || pCsr->iCol>=pCsr->nCol ){
    if( sqlite3Fts5IterEof(pCsr->pIter) ){
      pCsr->bEof = 1;
    }else{
      const char *zTerm;
      int nTerm;

      zTerm = sqlite3Fts5IterTerm(pCsr->pIter, &nTerm);
      sqlite3Fts5BufferSet(&rc, &pCsr->term, nTerm, (const u8*)zTerm);
      memset(pCsr->aVal, 0, sizeof(pCsr->aVal));
      memset(pCsr->aCnt, 0, pCsr->nCol * sizeof(i64));
      memset(pCsr->aDoc, 0, pCsr->nCol * sizeof(i64));
      pCsr->iCol = 0;

      assert( pTab->eType==FTS5_VOCAB_COL || pTab->eType==FTS5_VOCAB_ROW );
      while( rc==SQLITE_OK ){
        i64 dummy;
        const u8 *pPos; int nPos;   /* Position list */
        i64 iPos = 0;               /* 64-bit position read from poslist */
        int iOff = 0;               /* Current offset within position list */

        rc = sqlite3Fts5IterPoslist(pCsr->pIter, 0, &pPos, &nPos, &dummy);
        if( rc==SQLITE_OK ){
          if( pTab->eType==FTS5_VOCAB_ROW ){
            while( 0==sqlite3Fts5PoslistNext64(pPos, nPos, &iOff, &iPos) ){
              pCsr->aVal[1]++;
            }
            pCsr->aVal[0]++;
          }else{
            int iCol = -1;
            while( 0==sqlite3Fts5PoslistNext64(pPos, nPos, &iOff, &iPos) ){
              int ii = FTS5_POS2COLUMN(iPos);
              pCsr->aCnt[ii]++;
              if( iCol!=ii ){
                pCsr->aDoc[ii]++;
                iCol = ii;
              }
            }
          }
          rc = sqlite3Fts5IterNextScan(pCsr->pIter);
        }
        if( rc==SQLITE_OK ){
          zTerm = sqlite3Fts5IterTerm(pCsr->pIter, &nTerm);
          if( nTerm!=pCsr->term.n || memcmp(zTerm, pCsr->term.p, nTerm) ) break;
          if( sqlite3Fts5IterEof(pCsr->pIter) ) break;
        }
      }
    }
  }

  if( pCsr->bEof==0 && pTab->eType==FTS5_VOCAB_COL ){
    while( pCsr->aCnt[pCsr->iCol]==0 ) pCsr->iCol++;
    pCsr->aVal[0] = pCsr->iCol;
    pCsr->aVal[1] = pCsr->aDoc[pCsr->iCol];
    pCsr->aVal[2] = pCsr->aCnt[pCsr->iCol];
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
  rc = sqlite3Fts5IndexQuery(pCsr->pIndex, 0, 0, flags, 0, &pCsr->pIter);
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

    default:
      assert( iCol<4 && iCol>0 );
      sqlite3_result_int64(pCtx, pCsr->aVal[iCol-1]);
      break;
  }
  return SQLITE_OK;
}

/* 
** This is the xRowid method. The SQLite core calls this routine to
** retrieve the rowid for the current row of the result set. The
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



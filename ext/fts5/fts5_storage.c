/*
** 2014 May 31
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
*/

#include "fts5Int.h"

struct Fts5Storage {
  Fts5Config *pConfig;
  Fts5Index *pIndex;

  sqlite3_stmt *aStmt[8];
};


#if FTS5_STMT_SCAN_ASC!=0 
# error "FTS5_STMT_SCAN_ASC mismatch" 
#endif
#if FTS5_STMT_SCAN_DESC!=1 
# error "FTS5_STMT_SCAN_DESC mismatch" 
#endif
#if FTS5_STMT_LOOKUP!=2
# error "FTS5_STMT_LOOKUP mismatch" 
#endif

#define FTS5_STMT_INSERT_CONTENT  3
#define FTS5_STMT_REPLACE_CONTENT 4

#define FTS5_STMT_DELETE_CONTENT  5
#define FTS5_STMT_INSERT_DOCSIZE  6
#define FTS5_STMT_DELETE_DOCSIZE  7

/*
** Prepare the two insert statements - Fts5Storage.pInsertContent and
** Fts5Storage.pInsertDocsize - if they have not already been prepared.
** Return SQLITE_OK if successful, or an SQLite error code if an error
** occurs.
*/
static int fts5StorageGetStmt(
  Fts5Storage *p,                 /* Storage handle */
  int eStmt,                      /* FTS5_STMT_XXX constant */
  sqlite3_stmt **ppStmt           /* OUT: Prepared statement handle */
){
  int rc = SQLITE_OK;

  assert( eStmt>=0 && eStmt<ArraySize(p->aStmt) );
  if( p->aStmt[eStmt]==0 ){
    const char *azStmt[] = {
      "SELECT * FROM %Q.'%q_content' ORDER BY id ASC",  /* SCAN_ASC */
      "SELECT * FROM %Q.'%q_content' ORDER BY id DESC", /* SCAN_DESC */
      "SELECT * FROM %Q.'%q_content' WHERE rowid=?",    /* LOOKUP  */

      "INSERT INTO %Q.'%q_content' VALUES(%s)",         /* INSERT_CONTENT  */
      "REPLACE INTO %Q.'%q_content' VALUES(%s)",        /* REPLACE_CONTENT */
      "DELETE FROM %Q.'%q_content' WHERE id=?",         /* DELETE_CONTENT  */
      "INSERT INTO %Q.'%q_docsize' VALUES(?,?)",        /* INSERT_DOCSIZE  */
      "DELETE FROM %Q.'%q_docsize' WHERE id=?",         /* DELETE_DOCSIZE  */
    };
    Fts5Config *pConfig = p->pConfig;
    char *zSql = 0;

    if( eStmt==FTS5_STMT_INSERT_CONTENT || eStmt==FTS5_STMT_REPLACE_CONTENT ){
      int nCol = pConfig->nCol + 1;
      char *zBind;
      int i;

      zBind = sqlite3_malloc(1 + nCol*2);
      if( zBind ){
        for(i=0; i<nCol; i++){
          zBind[i*2] = '?';
          zBind[i*2 + 1] = ',';
        }
        zBind[i*2-1] = '\0';
        zSql = sqlite3_mprintf(azStmt[eStmt],pConfig->zDb,pConfig->zName,zBind);
        sqlite3_free(zBind);
      }
    }else{
      zSql = sqlite3_mprintf(azStmt[eStmt], pConfig->zDb, pConfig->zName);
    }

    if( zSql==0 ){
      rc = SQLITE_NOMEM;
    }else{
      rc = sqlite3_prepare_v2(pConfig->db, zSql, -1, &p->aStmt[eStmt], 0);
      sqlite3_free(zSql);
    }
  }

  *ppStmt = p->aStmt[eStmt];
  return rc;
}

/*
** Drop the shadow table with the postfix zPost (e.g. "content"). Return
** SQLITE_OK if successful or an SQLite error code otherwise.
*/
int sqlite3Fts5DropTable(Fts5Config *pConfig, const char *zPost){
  int rc;
  char *zSql = sqlite3_mprintf("DROP TABLE IF EXISTS %Q.'%q_%q'",
      pConfig->zDb, pConfig->zName, zPost
  );
  if( zSql==0 ){
    rc = SQLITE_NOMEM;
  }else{
    rc = sqlite3_exec(pConfig->db, zSql, 0, 0, 0);
    sqlite3_free(zSql);
  }
  return rc;
}

/*
** Create the shadow table named zPost, with definition zDefn. Return
** SQLITE_OK if successful, or an SQLite error code otherwise.
*/
int sqlite3Fts5CreateTable(
  Fts5Config *pConfig,            /* FTS5 configuration */
  const char *zPost,              /* Shadow table to create (e.g. "content") */
  const char *zDefn,              /* Columns etc. for shadow table */
  char **pzErr                    /* OUT: Error message */
){
  int rc;
  char *zSql = sqlite3_mprintf("CREATE TABLE %Q.'%q_%q'(%s)",
      pConfig->zDb, pConfig->zName, zPost, zDefn
  );
  if( zSql==0 ){
    rc = SQLITE_NOMEM;
  }else{
    char *zErr = 0;
    assert( *pzErr==0 );
    rc = sqlite3_exec(pConfig->db, zSql, 0, 0, &zErr);
    if( zErr ){
      *pzErr = sqlite3_mprintf(
          "fts5: error creating shadow table %q_%s: %s", 
          pConfig->zName, zPost, zErr
      );
      sqlite3_free(zErr);
    }
    sqlite3_free(zSql);
  }
  return rc;
}

/*
** Open a new Fts5Index handle. If the bCreate argument is true, create
** and initialize the underlying tables 
**
** If successful, set *pp to point to the new object and return SQLITE_OK.
** Otherwise, set *pp to NULL and return an SQLite error code.
*/
int sqlite3Fts5StorageOpen(
  Fts5Config *pConfig, 
  Fts5Index *pIndex, 
  int bCreate, 
  Fts5Storage **pp,
  char **pzErr                    /* OUT: Error message */
){
  int rc;
  Fts5Storage *p;                 /* New object */

  *pp = p = (Fts5Storage*)sqlite3_malloc(sizeof(Fts5Storage));
  if( !p ) return SQLITE_NOMEM;

  memset(p, 0, sizeof(Fts5Storage));
  p->pConfig = pConfig;
  p->pIndex = pIndex;

  if( bCreate ){
    int i;
    char *zDefn = sqlite3_malloc(32 + pConfig->nCol * 10);
    if( zDefn==0 ){
      rc = SQLITE_NOMEM;
    }else{
      int iOff = sprintf(zDefn, "id INTEGER PRIMARY KEY");
      for(i=0; i<pConfig->nCol; i++){
        iOff += sprintf(&zDefn[iOff], ", c%d", i);
      }
      rc = sqlite3Fts5CreateTable(pConfig, "content", zDefn, pzErr);
    }
    sqlite3_free(zDefn);
    if( rc==SQLITE_OK ){
      rc = sqlite3Fts5CreateTable(
          pConfig, "docsize", "id INTEGER PRIMARY KEY, sz BLOB", pzErr
      );
    }
  }

  if( rc ){
    sqlite3Fts5StorageClose(p, 0);
    *pp = 0;
  }
  return rc;
}

/*
** Close a handle opened by an earlier call to sqlite3Fts5StorageOpen().
*/
int sqlite3Fts5StorageClose(Fts5Storage *p, int bDestroy){
  int rc = SQLITE_OK;
  int i;

  /* Finalize all SQL statements */
  for(i=0; i<ArraySize(p->aStmt); i++){
    sqlite3_finalize(p->aStmt[i]);
  }

  /* If required, remove the shadow tables from the database */
  if( bDestroy ){
    rc = sqlite3Fts5DropTable(p->pConfig, "content");
    if( rc==SQLITE_OK ) sqlite3Fts5DropTable(p->pConfig, "docsize");
  }

  sqlite3_free(p);
  return rc;
}

/*
** Remove a row from the FTS table.
*/
int sqlite3Fts5StorageDelete(Fts5Storage *p, i64 iDel){
  assert( !"do this" );
  return SQLITE_OK;
}

typedef struct Fts5InsertCtx Fts5InsertCtx;
struct Fts5InsertCtx {
  Fts5Storage *pStorage;
  int iCol;
};

/*
** Tokenization callback used when inserting tokens into the FTS index.
*/
static int fts5StorageInsertCallback(
  void *pContext,                 /* Pointer to Fts5InsertCtx object */
  const char *pToken,             /* Buffer containing token */
  int nToken,                     /* Size of token in bytes */
  int iStart,                     /* Start offset of token */
  int iEnd,                       /* End offset of token */
  int iPos                        /* Position offset of token */
){
  Fts5InsertCtx *pCtx = (Fts5InsertCtx*)pContext;
  Fts5Index *pIdx = pCtx->pStorage->pIndex;
  sqlite3Fts5IndexWrite(pIdx, pCtx->iCol, iPos, pToken, nToken);
  return SQLITE_OK;
}

/*
** If a row with rowid iDel is present in the %_content table, add the
** delete-markers to the FTS index necessary to delete it. Do not actually
** remove the %_content row at this time though.
*/
static int fts5StorageDeleteFromIndex(Fts5Storage *p, i64 iDel){
  Fts5Config *pConfig = p->pConfig;
  sqlite3_stmt *pSeek;            /* SELECT to read row iDel from %_data */
  int rc;                         /* Return code */

  rc = fts5StorageGetStmt(p, FTS5_STMT_LOOKUP, &pSeek);
  if( rc==SQLITE_OK ){
    int rc2;
    sqlite3_bind_int64(pSeek, 1, iDel);
    if( sqlite3_step(pSeek)==SQLITE_ROW ){
      int iCol;
      Fts5InsertCtx ctx;
      ctx.pStorage = p;
      ctx.iCol = -1;
      sqlite3Fts5IndexBeginWrite(p->pIndex, iDel);
      for(iCol=1; iCol<=pConfig->nCol; iCol++){
        rc = sqlite3Fts5Tokenize(pConfig, 
            (const char*)sqlite3_column_text(pSeek, iCol),
            sqlite3_column_bytes(pSeek, iCol),
            (void*)&ctx,
            fts5StorageInsertCallback
        );
      }
    }
    rc2 = sqlite3_reset(pSeek);
    if( rc==SQLITE_OK ) rc = rc2;
  }

  return rc;
}

/*
** Insert a new row into the FTS table.
*/
int sqlite3Fts5StorageInsert(
  Fts5Storage *p,                 /* Storage module to write to */
  sqlite3_value **apVal,          /* Array of values passed to xUpdate() */
  int eConflict,                  /* on conflict clause */
  i64 *piRowid                    /* OUT: rowid of new record */
){
  Fts5Config *pConfig = p->pConfig;
  int rc = SQLITE_OK;             /* Return code */
  sqlite3_stmt *pInsert;          /* Statement used to write %_content table */
  int eStmt;                      /* Type of statement used on %_content */
  int i;                          /* Counter variable */
  Fts5InsertCtx ctx;              /* Tokenization callback context object */

  /* Insert the new row into the %_content table. */
  if( eConflict==SQLITE_REPLACE ){
    eStmt = FTS5_STMT_REPLACE_CONTENT;
    if( sqlite3_value_type(apVal[1])==SQLITE_INTEGER ){
      rc = fts5StorageDeleteFromIndex(p, sqlite3_value_int64(apVal[1]));
    }
  }else{
    eStmt = FTS5_STMT_INSERT_CONTENT;
  }
  if( rc==SQLITE_OK ){
    rc = fts5StorageGetStmt(p, eStmt, &pInsert);
  }
  for(i=1; rc==SQLITE_OK && i<=pConfig->nCol+1; i++){
    rc = sqlite3_bind_value(pInsert, i, apVal[i]);
  }
  if( rc==SQLITE_OK ){
    sqlite3_step(pInsert);
    rc = sqlite3_reset(pInsert);
  }
  *piRowid = sqlite3_last_insert_rowid(pConfig->db);

  /* Add new entries to the FTS index */
  sqlite3Fts5IndexBeginWrite(p->pIndex, *piRowid);
  ctx.pStorage = p;
  for(ctx.iCol=0; rc==SQLITE_OK && ctx.iCol<pConfig->nCol; ctx.iCol++){
    rc = sqlite3Fts5Tokenize(pConfig, 
        (const char*)sqlite3_value_text(apVal[ctx.iCol+2]),
        sqlite3_value_bytes(apVal[ctx.iCol+2]),
        (void*)&ctx,
        fts5StorageInsertCallback
    );
  }

  return rc;
}

/*
** Context object used by sqlite3Fts5StorageIntegrity().
*/
typedef struct Fts5IntegrityCtx Fts5IntegrityCtx;
struct Fts5IntegrityCtx {
  i64 iRowid;
  int iCol;
  u64 cksum;
  Fts5Config *pConfig;
};

/*
** Tokenization callback used by integrity check.
*/
static int fts5StorageIntegrityCallback(
  void *pContext,                 /* Pointer to Fts5InsertCtx object */
  const char *pToken,             /* Buffer containing token */
  int nToken,                     /* Size of token in bytes */
  int iStart,                     /* Start offset of token */
  int iEnd,                       /* End offset of token */
  int iPos                        /* Position offset of token */
){
  Fts5IntegrityCtx *pCtx = (Fts5IntegrityCtx*)pContext;
  pCtx->cksum ^= sqlite3Fts5IndexCksum(
      pCtx->pConfig, pCtx->iRowid, pCtx->iCol, iPos, pToken, nToken
  );
  return SQLITE_OK;
}

/*
** Check that the contents of the FTS index match that of the %_content
** table. Return SQLITE_OK if they do, or SQLITE_CORRUPT if not. Return
** some other SQLite error code if an error occurs while attempting to
** determine this.
*/
int sqlite3Fts5StorageIntegrity(Fts5Storage *p){
  Fts5Config *pConfig = p->pConfig;
  int rc;                         /* Return code */
  Fts5IntegrityCtx ctx;
  sqlite3_stmt *pScan;

  memset(&ctx, 0, sizeof(Fts5IntegrityCtx));
  ctx.pConfig = p->pConfig;

  /* Generate the expected index checksum based on the contents of the
  ** %_content table. This block stores the checksum in ctx.cksum. */
  rc = fts5StorageGetStmt(p, FTS5_STMT_SCAN_ASC, &pScan);
  if( rc==SQLITE_OK ){
    int rc2;
    while( SQLITE_ROW==sqlite3_step(pScan) ){
      int i;
      ctx.iRowid = sqlite3_column_int64(pScan, 0);
      for(i=0; rc==SQLITE_OK && i<pConfig->nCol; i++){
        ctx.iCol = i;
        rc = sqlite3Fts5Tokenize(
            pConfig, 
            (const char*)sqlite3_column_text(pScan, i+1),
            sqlite3_column_bytes(pScan, i+1),
            (void*)&ctx,
            fts5StorageIntegrityCallback
        );
      }
    }
    rc2 = sqlite3_reset(pScan);
    if( rc==SQLITE_OK ) rc = rc2;
  }

  /* Pass the expected checksum down to the FTS index module. It will
  ** verify, amongst other things, that it matches the checksum generated by
  ** inspecting the index itself.  */
  if( rc==SQLITE_OK ){
    rc = sqlite3Fts5IndexIntegrityCheck(p->pIndex, ctx.cksum);
  }

  return rc;
}

/*
** Obtain an SQLite statement handle that may be used to read data from the
** %_content table.
*/
int sqlite3Fts5StorageStmt(Fts5Storage *p, int eStmt, sqlite3_stmt **pp){
  int rc;
  assert( eStmt==FTS5_STMT_SCAN_ASC 
       || eStmt==FTS5_STMT_SCAN_DESC
       || eStmt==FTS5_STMT_LOOKUP
  );
  rc = fts5StorageGetStmt(p, eStmt, pp);
  if( rc==SQLITE_OK ){
    assert( p->aStmt[eStmt]==*pp );
    p->aStmt[eStmt] = 0;
  }
  return rc;
}

/*
** Release an SQLite statement handle obtained via an earlier call to
** sqlite3Fts5StorageStmt(). The eStmt parameter passed to this function
** must match that passed to the sqlite3Fts5StorageStmt() call.
*/
void sqlite3Fts5StorageStmtRelease(
  Fts5Storage *p, 
  int eStmt, 
  sqlite3_stmt *pStmt
){
  assert( eStmt==FTS5_STMT_SCAN_ASC
       || eStmt==FTS5_STMT_SCAN_DESC
       || eStmt==FTS5_STMT_LOOKUP
  );
  if( p->aStmt[eStmt]==0 ){
    sqlite3_reset(pStmt);
    p->aStmt[eStmt] = pStmt;
  }else{
    sqlite3_finalize(pStmt);
  }
}



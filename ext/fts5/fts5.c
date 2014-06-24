/*
** 2014 Jun 09
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

#include "fts5Int.h"

typedef struct Fts5Table Fts5Table;
typedef struct Fts5Cursor Fts5Cursor;

struct Fts5Table {
  sqlite3_vtab base;              /* Base class used by SQLite core */
  Fts5Config *pConfig;            /* Virtual table configuration */
  Fts5Index *pIndex;              /* Full-text index */
  Fts5Storage *pStorage;          /* Document store */
};

struct Fts5Cursor {
  sqlite3_vtab_cursor base;       /* Base class used by SQLite core */
  int idxNum;                     /* idxNum passed to xFilter() */
  sqlite3_stmt *pStmt;            /* Statement used to read %_content */
  int bEof;                       /* True at EOF */
};

/*
** Close a virtual table handle opened by fts5InitVtab(). If the bDestroy
** argument is non-zero, attempt delete the shadow tables from teh database
*/
static int fts5FreeVtab(Fts5Table *pTab, int bDestroy){
  int rc = SQLITE_OK;
  if( pTab ){
    int rc2;
    rc2 = sqlite3Fts5IndexClose(pTab->pIndex, bDestroy);
    if( rc==SQLITE_OK ) rc = rc2;
    rc2 = sqlite3Fts5StorageClose(pTab->pStorage, bDestroy);
    if( rc==SQLITE_OK ) rc = rc2;
    sqlite3Fts5ConfigFree(pTab->pConfig);
    sqlite3_free(pTab);
  }
  return rc;
}

/*
** The xDisconnect() virtual table method.
*/
static int fts5DisconnectMethod(sqlite3_vtab *pVtab){
  return fts5FreeVtab((Fts5Table*)pVtab, 0);
}

/*
** The xDestroy() virtual table method.
*/
static int fts5DestroyMethod(sqlite3_vtab *pVtab){
  return fts5FreeVtab((Fts5Table*)pVtab, 1);
}

/*
** This function is the implementation of both the xConnect and xCreate
** methods of the FTS3 virtual table.
**
** The argv[] array contains the following:
**
**   argv[0]   -> module name  ("fts5")
**   argv[1]   -> database name
**   argv[2]   -> table name
**   argv[...] -> "column name" and other module argument fields.
*/
static int fts5InitVtab(
  int bCreate,                    /* True for xCreate, false for xConnect */
  sqlite3 *db,                    /* The SQLite database connection */
  void *pAux,                     /* Hash table containing tokenizers */
  int argc,                       /* Number of elements in argv array */
  const char * const *argv,       /* xCreate/xConnect argument array */
  sqlite3_vtab **ppVTab,          /* Write the resulting vtab structure here */
  char **pzErr                    /* Write any error message here */
){
  int rc;                         /* Return code */
  Fts5Config *pConfig;            /* Results of parsing argc/argv */
  Fts5Table *pTab = 0;            /* New virtual table object */

  /* Parse the arguments */
  rc = sqlite3Fts5ConfigParse(db, argc, (const char**)argv, &pConfig, pzErr);
  assert( (rc==SQLITE_OK && *pzErr==0) || pConfig==0 );

  /* Allocate the new vtab object */
  if( rc==SQLITE_OK ){
    pTab = (Fts5Table*)sqlite3_malloc(sizeof(Fts5Table));
    if( pTab==0 ){
      rc = SQLITE_NOMEM;
    }else{
      memset(pTab, 0, sizeof(Fts5Table));
      pTab->pConfig = pConfig;
    }
  }

  /* Open the index sub-system */
  if( rc==SQLITE_OK ){
    rc = sqlite3Fts5IndexOpen(pConfig, bCreate, &pTab->pIndex, pzErr);
  }

  /* Open the storage sub-system */
  if( rc==SQLITE_OK ){
    rc = sqlite3Fts5StorageOpen(
        pConfig, pTab->pIndex, bCreate, &pTab->pStorage, pzErr
    );
  }

  /* Call sqlite3_declare_vtab() */
  if( rc==SQLITE_OK ){
    rc = sqlite3Fts5ConfigDeclareVtab(pConfig);
  }

  if( rc!=SQLITE_OK ){
    fts5FreeVtab(pTab, 0);
    pTab = 0;
  }
  *ppVTab = (sqlite3_vtab*)pTab;
  return rc;
}

/*
** The xConnect() and xCreate() methods for the virtual table. All the
** work is done in function fts5InitVtab().
*/
static int fts5ConnectMethod(
  sqlite3 *db,                    /* Database connection */
  void *pAux,                     /* Pointer to tokenizer hash table */
  int argc,                       /* Number of elements in argv array */
  const char * const *argv,       /* xCreate/xConnect argument array */
  sqlite3_vtab **ppVtab,          /* OUT: New sqlite3_vtab object */
  char **pzErr                    /* OUT: sqlite3_malloc'd error message */
){
  return fts5InitVtab(0, db, pAux, argc, argv, ppVtab, pzErr);
}
static int fts5CreateMethod(
  sqlite3 *db,                    /* Database connection */
  void *pAux,                     /* Pointer to tokenizer hash table */
  int argc,                       /* Number of elements in argv array */
  const char * const *argv,       /* xCreate/xConnect argument array */
  sqlite3_vtab **ppVtab,          /* OUT: New sqlite3_vtab object */
  char **pzErr                    /* OUT: sqlite3_malloc'd error message */
){
  return fts5InitVtab(1, db, pAux, argc, argv, ppVtab, pzErr);
}

/*
** The three query plans xBestIndex may choose between.
*/
#define FTS5_PLAN_SCAN    1       /* No usable constraint */
#define FTS5_PLAN_MATCH   2       /* (<tbl> MATCH ?) */
#define FTS5_PLAN_ROWID   3       /* (rowid = ?) */

#define FTS5_PLAN(idxNum) ((idxNum) & 0x7)

#define FTS5_ORDER_DESC   8       /* ORDER BY rowid DESC */
#define FTS5_ORDER_ASC   16       /* ORDER BY rowid ASC */


static int fts5FindConstraint(sqlite3_index_info *pInfo, int eOp, int iCol){
  int i;

  for(i=0; i<pInfo->nConstraint; i++){
    struct sqlite3_index_constraint *p = &pInfo->aConstraint[i];
    if( p->usable && p->iColumn==iCol && p->op==eOp ) return i;
  }

  return -1;
}

/* 
** Implementation of the xBestIndex method for FTS5 tables. There
** are three possible strategies, in order of preference:
**
**   1. Full-text search using a MATCH operator.
**   2. A by-rowid lookup.
**   3. A full-table scan.
*/
static int fts5BestIndexMethod(sqlite3_vtab *pVTab, sqlite3_index_info *pInfo){
  Fts5Table *pTab = (Fts5Table*)pVTab;
  Fts5Config *pConfig = pTab->pConfig;
  int iCons;
  int ePlan = FTS5_PLAN_SCAN;

  iCons = fts5FindConstraint(pInfo,SQLITE_INDEX_CONSTRAINT_MATCH,pConfig->nCol);
  if( iCons>=0 ){
    ePlan = FTS5_PLAN_MATCH;
    pInfo->estimatedCost = 1.0;
  }else{
    iCons = fts5FindConstraint(pInfo, SQLITE_INDEX_CONSTRAINT_EQ, -1);
    if( iCons>=0 ){
      ePlan = FTS5_PLAN_ROWID;
      pInfo->estimatedCost = 2.0;
    }
  }

  if( iCons>=0 ){
    pInfo->aConstraintUsage[iCons].argvIndex = 1;
    pInfo->aConstraintUsage[iCons].omit = 1;
  }else{
    pInfo->estimatedCost = 10000000.0;
  }

  if( pInfo->nOrderBy==1 && pInfo->aOrderBy[0].iColumn<0 ){
    pInfo->orderByConsumed = 1;
    ePlan |= pInfo->aOrderBy[0].desc ? FTS5_ORDER_DESC : FTS5_ORDER_ASC;
  }
   
  pInfo->idxNum = ePlan;
  return SQLITE_OK;
}

/*
** Implementation of xOpen method.
*/
static int fts5OpenMethod(sqlite3_vtab *pVTab, sqlite3_vtab_cursor **ppCsr){
  Fts5Cursor *pCsr;
  int rc = SQLITE_OK;
  pCsr = (Fts5Cursor*)sqlite3_malloc(sizeof(Fts5Cursor));
  if( pCsr ){
    memset(pCsr, 0, sizeof(Fts5Cursor));
  }else{
    rc = SQLITE_NOMEM;
  }
  *ppCsr = (sqlite3_vtab_cursor*)pCsr;
  return rc;
}

static int fts5StmtType(int idxNum){
  if( FTS5_PLAN(idxNum)==FTS5_PLAN_SCAN ){
    return (idxNum&FTS5_ORDER_ASC) ? FTS5_STMT_SCAN_ASC : FTS5_STMT_SCAN_DESC;
  }
  return FTS5_STMT_LOOKUP;
}

/*
** Close the cursor.  For additional information see the documentation
** on the xClose method of the virtual table interface.
*/
static int fts5CloseMethod(sqlite3_vtab_cursor *pCursor){
  Fts5Table *pTab = (Fts5Table*)(pCursor->pVtab);
  Fts5Cursor *pCsr = (Fts5Cursor*)pCursor;
  if( pCsr->pStmt ){
    int eStmt = fts5StmtType(pCsr->idxNum);
    sqlite3Fts5StorageStmtRelease(pTab->pStorage, eStmt, pCsr->pStmt);
  }
  sqlite3_free(pCsr);
  return SQLITE_OK;
}


/*
** Advance the cursor to the next row in the table that matches the 
** search criteria.
**
** Return SQLITE_OK if nothing goes wrong.  SQLITE_OK is returned
** even if we reach end-of-file.  The fts5EofMethod() will be called
** subsequently to determine whether or not an EOF was hit.
*/
static int fts5NextMethod(sqlite3_vtab_cursor *pCursor){
  Fts5Cursor *pCsr = (Fts5Cursor*)pCursor;
  int ePlan = FTS5_PLAN(pCsr->idxNum);
  int rc = SQLITE_OK;

  assert( ePlan!=FTS5_PLAN_MATCH );
  if( ePlan!=FTS5_PLAN_MATCH ){
    rc = sqlite3_step(pCsr->pStmt);
    if( rc!=SQLITE_ROW ){
      pCsr->bEof = 1;
      rc = sqlite3_reset(pCsr->pStmt);
    }else{
      rc = SQLITE_OK;
    }
  }
  
  return rc;
}

/*
** This is the xFilter interface for the virtual table.  See
** the virtual table xFilter method documentation for additional
** information.
*/
static int fts5FilterMethod(
  sqlite3_vtab_cursor *pCursor,   /* The cursor used for this query */
  int idxNum,                     /* Strategy index */
  const char *idxStr,             /* Unused */
  int nVal,                       /* Number of elements in apVal */
  sqlite3_value **apVal           /* Arguments for the indexing scheme */
){
  Fts5Table *pTab = (Fts5Table*)(pCursor->pVtab);
  Fts5Cursor *pCsr = (Fts5Cursor*)pCursor;
  int rc = SQLITE_OK;
  int ePlan = FTS5_PLAN(idxNum);
  int eStmt = fts5StmtType(idxNum);

  assert( ePlan!=FTS5_PLAN_MATCH );
  memset(&pCursor[1], 0, sizeof(Fts5Cursor) - sizeof(sqlite3_vtab_cursor));
  pCsr->idxNum = idxNum;

  rc = sqlite3Fts5StorageStmt(pTab->pStorage, eStmt, &pCsr->pStmt);
  if( ePlan==FTS5_PLAN_ROWID ){
    sqlite3_bind_value(pCsr->pStmt, 1, apVal[0]);
  }

  if( rc==SQLITE_OK ){
    rc = fts5NextMethod(pCursor);
  }
  return rc;
}

/* 
** This is the xEof method of the virtual table. SQLite calls this 
** routine to find out if it has reached the end of a result set.
*/
static int fts5EofMethod(sqlite3_vtab_cursor *pCursor){
  Fts5Cursor *pCsr = (Fts5Cursor*)pCursor;
  return pCsr->bEof;
}

/* 
** This is the xRowid method. The SQLite core calls this routine to
** retrieve the rowid for the current row of the result set. fts5
** exposes %_content.docid as the rowid for the virtual table. The
** rowid should be written to *pRowid.
*/
static int fts5RowidMethod(sqlite3_vtab_cursor *pCursor, sqlite_int64 *pRowid){
  Fts5Cursor *pCsr = (Fts5Cursor*)pCursor;
  int ePlan = FTS5_PLAN(pCsr->idxNum);
  
  assert( pCsr->bEof==0 );
  assert( ePlan!=FTS5_PLAN_MATCH );

  if( ePlan!=FTS5_PLAN_MATCH ){
    *pRowid = sqlite3_column_int64(pCsr->pStmt, 0);
  }

  return SQLITE_OK;
}

/* 
** This is the xColumn method, called by SQLite to request a value from
** the row that the supplied cursor currently points to.
*/
static int fts5ColumnMethod(
  sqlite3_vtab_cursor *pCursor,   /* Cursor to retrieve value from */
  sqlite3_context *pCtx,          /* Context for sqlite3_result_xxx() calls */
  int iCol                        /* Index of column to read value from */
){
  Fts5Cursor *pCsr = (Fts5Cursor*)pCursor;
  int ePlan = FTS5_PLAN(pCsr->idxNum);
  
  assert( pCsr->bEof==0 );
  assert( ePlan!=FTS5_PLAN_MATCH );
  if( ePlan!=FTS5_PLAN_MATCH ){
    sqlite3_result_value(pCtx, sqlite3_column_value(pCsr->pStmt, iCol+1));
  }
  return SQLITE_OK;
}

/*
** This function is called to handle an FTS INSERT command. In other words,
** an INSERT statement of the form:
**
**     INSERT INTO fts(fts) VALUES($pVal)
**
** Argument pVal is the value assigned to column "fts" by the INSERT 
** statement. This function returns SQLITE_OK if successful, or an SQLite
** error code if an error occurs.
*/
static int fts5SpecialCommand(Fts5Table *pTab, sqlite3_value *pVal){
  const char *z = (const char*)sqlite3_value_text(pVal);
  int n = sqlite3_value_bytes(pVal);
  int rc = SQLITE_ERROR;

  if( 0==sqlite3_stricmp("integrity-check", z) ){
    rc = sqlite3Fts5StorageIntegrity(pTab->pStorage);
  }else

  if( n>5 && 0==sqlite3_strnicmp("pgsz=", z, 5) ){
    int pgsz = atoi(&z[5]);
    if( pgsz<32 ) pgsz = 32;
    sqlite3Fts5IndexPgsz(pTab->pIndex, pgsz);
    rc = SQLITE_OK;
  }

  return rc;
}

/* 
** This function is the implementation of the xUpdate callback used by 
** FTS3 virtual tables. It is invoked by SQLite each time a row is to be
** inserted, updated or deleted.
*/
static int fts5UpdateMethod(
  sqlite3_vtab *pVtab,            /* Virtual table handle */
  int nArg,                       /* Size of argument array */
  sqlite3_value **apVal,          /* Array of arguments */
  sqlite_int64 *pRowid            /* OUT: The affected (or effected) rowid */
){
  Fts5Table *pTab = (Fts5Table*)pVtab;
  Fts5Config *pConfig = pTab->pConfig;
  int eType0;                     /* value_type() of apVal[0] */
  int eConflict;                  /* ON CONFLICT for this DML */
  int rc = SQLITE_OK;             /* Return code */

  assert( nArg==1 || nArg==(2 + pConfig->nCol + 1) );

  if( SQLITE_NULL!=sqlite3_value_type(apVal[2 + pConfig->nCol]) ){
    return fts5SpecialCommand(pTab, apVal[2 + pConfig->nCol]);
  }

  eType0 = sqlite3_value_type(apVal[0]);
  eConflict = sqlite3_vtab_on_conflict(pConfig->db);

  assert( eType0==SQLITE_INTEGER || eType0==SQLITE_NULL );
  if( eType0==SQLITE_INTEGER ){
    i64 iDel = sqlite3_value_int64(apVal[0]);    /* Rowid to delete */
    rc = sqlite3Fts5StorageDelete(pTab->pStorage, iDel);
  }

  if( rc==SQLITE_OK && nArg>1 ){
    rc = sqlite3Fts5StorageInsert(pTab->pStorage, apVal, eConflict, pRowid);
  }

  return rc;
}

/*
** Implementation of xSync() method. 
*/
static int fts5SyncMethod(sqlite3_vtab *pVtab){
  int rc;
  Fts5Table *pTab = (Fts5Table*)pVtab;
  rc = sqlite3Fts5IndexSync(pTab->pIndex);
  return rc;
}

/*
** Implementation of xBegin() method. 
*/
static int fts5BeginMethod(sqlite3_vtab *pVtab){
  return SQLITE_OK;
}

/*
** Implementation of xCommit() method. This is a no-op. The contents of
** the pending-terms hash-table have already been flushed into the database
** by fts5SyncMethod().
*/
static int fts5CommitMethod(sqlite3_vtab *pVtab){
  return SQLITE_OK;
}

/*
** Implementation of xRollback(). Discard the contents of the pending-terms
** hash-table. Any changes made to the database are reverted by SQLite.
*/
static int fts5RollbackMethod(sqlite3_vtab *pVtab){
  Fts5Table *pTab = (Fts5Table*)pVtab;
  int rc;
  rc = sqlite3Fts5IndexRollback(pTab->pIndex);
  return rc;
}

/*
** This routine implements the xFindFunction method for the FTS3
** virtual table.
*/
static int fts5FindFunctionMethod(
  sqlite3_vtab *pVtab,            /* Virtual table handle */
  int nArg,                       /* Number of SQL function arguments */
  const char *zName,              /* Name of SQL function */
  void (**pxFunc)(sqlite3_context*,int,sqlite3_value**), /* OUT: Result */
  void **ppArg                    /* Unused */
){
  /* No function of the specified name was found. Return 0. */
  return 0;
}

/*
** Implementation of FTS3 xRename method. Rename an fts5 table.
*/
static int fts5RenameMethod(
  sqlite3_vtab *pVtab,            /* Virtual table handle */
  const char *zName               /* New name of table */
){
  int rc = SQLITE_OK;
  return rc;
}

/*
** The xSavepoint() method.
**
** Flush the contents of the pending-terms table to disk.
*/
static int fts5SavepointMethod(sqlite3_vtab *pVtab, int iSavepoint){
  int rc = SQLITE_OK;
  return rc;
}

/*
** The xRelease() method.
**
** This is a no-op.
*/
static int fts5ReleaseMethod(sqlite3_vtab *pVtab, int iSavepoint){
  return SQLITE_OK;
}

/*
** The xRollbackTo() method.
**
** Discard the contents of the pending terms table.
*/
static int fts5RollbackToMethod(sqlite3_vtab *pVtab, int iSavepoint){
  return SQLITE_OK;
}

static const sqlite3_module fts5Module = {
  /* iVersion      */ 2,
  /* xCreate       */ fts5CreateMethod,
  /* xConnect      */ fts5ConnectMethod,
  /* xBestIndex    */ fts5BestIndexMethod,
  /* xDisconnect   */ fts5DisconnectMethod,
  /* xDestroy      */ fts5DestroyMethod,
  /* xOpen         */ fts5OpenMethod,
  /* xClose        */ fts5CloseMethod,
  /* xFilter       */ fts5FilterMethod,
  /* xNext         */ fts5NextMethod,
  /* xEof          */ fts5EofMethod,
  /* xColumn       */ fts5ColumnMethod,
  /* xRowid        */ fts5RowidMethod,
  /* xUpdate       */ fts5UpdateMethod,
  /* xBegin        */ fts5BeginMethod,
  /* xSync         */ fts5SyncMethod,
  /* xCommit       */ fts5CommitMethod,
  /* xRollback     */ fts5RollbackMethod,
  /* xFindFunction */ fts5FindFunctionMethod,
  /* xRename       */ fts5RenameMethod,
  /* xSavepoint    */ fts5SavepointMethod,
  /* xRelease      */ fts5ReleaseMethod,
  /* xRollbackTo   */ fts5RollbackToMethod,
};

int sqlite3Fts5Init(sqlite3 *db){
  int rc;
  rc = sqlite3_create_module_v2(db, "fts5", &fts5Module, 0, 0);
  if( rc==SQLITE_OK ) rc = sqlite3Fts5IndexInit(db);
  if( rc==SQLITE_OK ) rc = sqlite3Fts5ExprInit(db);
  return rc;
}


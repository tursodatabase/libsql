/*
** 2016 February 10
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

typedef sqlite3_int64 i64;

typedef struct IdxConstraint IdxConstraint;
typedef struct IdxContext IdxContext;
typedef struct IdxScan IdxScan;
typedef struct IdxWhere IdxWhere;

/*
** A single constraint. Equivalent to either "col = ?" or "col < ?".
**
** pLink:
**   ... todo ...
*/
struct IdxConstraint {
  char *zColl;                    /* Collation sequence */
  int bRange;                     /* True for range, false for eq */
  int iCol;                       /* Constrained table column */
  i64 depmask;                    /* Dependency mask */
  IdxConstraint *pNext;           /* Next constraint in pEq or pRange list */
  IdxConstraint *pLink;           /* See above */
};

/*
** A WHERE clause. Made up of IdxConstraint objects.
**
**   a=? AND b=? AND (c=? OR d=?) AND (e=? OR f=?)
**
*/
struct IdxWhere {
  IdxConstraint *pEq;             /* List of == constraints */
  IdxConstraint *pRange;          /* List of < constraints */
  IdxWhere **apOr;                /* Array of OR branches (joined by pNextOr) */
  IdxWhere *pNextOr;              /* Next in OR'd terms */
  IdxWhere *pParent;              /* Parent object (or NULL) */
};

/*
** A single scan of a single table.
*/
struct IdxScan {
  char *zTable;                   /* Name of table to scan */
  int iDb;                        /* Database containing table zTable */
  i64 covering;                   /* Mask of columns required for cov. index */
  IdxConstraint *pOrder;          /* ORDER BY columns */
  IdxWhere where;                 /* WHERE Constraints */
  IdxScan *pNextScan;             /* Next IdxScan object for same query */
};

/*
** Context object passed to idxWhereInfo()
*/
struct IdxContext {
  IdxWhere *pCurrent;             /* Current where clause */
  IdxScan *pScan;                 /* List of scan objects */
  sqlite3 *dbm;                   /* In-memory db for this analysis */
  int rc;                         /* Error code (if error has occurred) */
};

typedef struct PragmaTable PragmaTable;
typedef struct PragmaCursor PragmaCursor;

struct PragmaTable {
  sqlite3_vtab base;
  sqlite3 *db;
};

struct PragmaCursor {
  sqlite3_vtab_cursor base;
  sqlite3_stmt *pStmt;
  i64 iRowid;
};

/*
** Connect to or create a pragma virtual table.
*/
static int pragmaConnect(
  sqlite3 *db,
  void *pAux,
  int argc, const char *const*argv,
  sqlite3_vtab **ppVtab,
  char **pzErr
){
  const char *zSchema = 
    "CREATE TABLE a(tbl HIDDEN, cid, name, type, notnull, dflt_value, pk)";
  PragmaTable *pTab = 0;
  int rc = SQLITE_OK;

  rc = sqlite3_declare_vtab(db, zSchema);
  if( rc==SQLITE_OK ){
    pTab = (PragmaTable *)sqlite3_malloc64(sizeof(PragmaTable));
    if( pTab==0 ) rc = SQLITE_NOMEM;
  }

  assert( rc==SQLITE_OK || pTab==0 );
  if( rc==SQLITE_OK ){
    memset(pTab, 0, sizeof(PragmaTable));
    pTab->db = db;
  }

  *ppVtab = (sqlite3_vtab*)pTab;
  return rc;
}

/*
** Disconnect from or destroy a pragma virtual table.
*/
static int pragmaDisconnect(sqlite3_vtab *pVtab){
  sqlite3_free(pVtab);
  return SQLITE_OK;
}

/*
** xBestIndex method for pragma virtual tables.
*/
static int pragmaBestIndex(sqlite3_vtab *tab, sqlite3_index_info *pIdxInfo){
  int i;

  pIdxInfo->estimatedCost = 1.0e6;  /* Initial cost estimate */

  /* Look for a valid tbl=? constraint. */
  for(i=0; i<pIdxInfo->nConstraint; i++){
    if( pIdxInfo->aConstraint[i].usable==0 ) continue;
    if( pIdxInfo->aConstraint[i].op!=SQLITE_INDEX_CONSTRAINT_EQ ) continue;
    if( pIdxInfo->aConstraint[i].iColumn!=0 ) continue;
    pIdxInfo->idxNum = 1;
    pIdxInfo->estimatedCost = 1.0;
    pIdxInfo->aConstraintUsage[i].argvIndex = 1;
    pIdxInfo->aConstraintUsage[i].omit = 1;
    break;
  }
  if( i==pIdxInfo->nConstraint ){
    tab->zErrMsg = sqlite3_mprintf("missing required tbl=? constraint");
    return SQLITE_ERROR;
  }
  return SQLITE_OK;
}

/*
** Open a new pragma cursor.
*/
static int pragmaOpen(sqlite3_vtab *pVTab, sqlite3_vtab_cursor **ppCursor){
  PragmaTable *pTab = (PragmaTable *)pVTab;
  PragmaCursor *pCsr;

  pCsr = (PragmaCursor*)sqlite3_malloc64(sizeof(PragmaCursor));
  if( pCsr==0 ){
    return SQLITE_NOMEM;
  }else{
    memset(pCsr, 0, sizeof(PragmaCursor));
    pCsr->base.pVtab = pVTab;
  }

  *ppCursor = (sqlite3_vtab_cursor*)pCsr;
  return SQLITE_OK;
}

/*
** Move a statvfs cursor to the next entry in the file.
*/
static int pragmaNext(sqlite3_vtab_cursor *pCursor){
  PragmaCursor *pCsr = (PragmaCursor*)pCursor;
  int rc = SQLITE_OK;

  if( sqlite3_step(pCsr->pStmt)!=SQLITE_ROW ){
    rc = sqlite3_finalize(pCsr->pStmt);
    pCsr->pStmt = 0;
  }
  pCsr->iRowid++;
  return rc;
}

static int pragmaEof(sqlite3_vtab_cursor *pCursor){
  PragmaCursor *pCsr = (PragmaCursor*)pCursor;
  return pCsr->pStmt==0;
}

static int pragmaFilter(
  sqlite3_vtab_cursor *pCursor, 
  int idxNum, const char *idxStr,
  int argc, sqlite3_value **argv
){
  PragmaCursor *pCsr = (PragmaCursor*)pCursor;
  PragmaTable *pTab = (PragmaTable*)(pCursor->pVtab);
  char *zSql;
  const char *zTbl;
  int rc = SQLITE_OK;

  if( pCsr->pStmt ){
    sqlite3_finalize(pCsr->pStmt);
    pCsr->pStmt = 0;
  }
  pCsr->iRowid = 0;

  assert( argc==1 );
  zTbl = (const char*)sqlite3_value_text(argv[0]);
  zSql = sqlite3_mprintf("PRAGMA table_info(%Q)", zTbl);
  if( zSql==0 ){
    rc = SQLITE_NOMEM;
  }else{
    rc = sqlite3_prepare_v2(pTab->db, zSql, -1, &pCsr->pStmt, 0);
  }
  return pragmaNext(pCursor);;
}

/*
** xColumn method.
*/
static int pragmaColumn(
  sqlite3_vtab_cursor *pCursor, 
  sqlite3_context *ctx, 
  int iCol
){
  PragmaCursor *pCsr = (PragmaCursor *)pCursor;
  if( iCol>0 ){
    sqlite3_result_value(ctx, sqlite3_column_value(pCsr->pStmt, iCol-1));
  }
  return SQLITE_OK;
}

static int pragmaRowid(sqlite3_vtab_cursor *pCursor, sqlite_int64 *pRowid){
  PragmaCursor *pCsr = (PragmaCursor *)pCursor;
  *pRowid = pCsr->iRowid;
  return SQLITE_OK;
}

static int registerPragmaVtabs(sqlite3 *db){
  static sqlite3_module pragma_module = {
    0,                            /* iVersion */
    pragmaConnect,                /* xCreate */
    pragmaConnect,                /* xConnect */
    pragmaBestIndex,              /* xBestIndex */
    pragmaDisconnect,             /* xDisconnect */
    pragmaDisconnect,             /* xDestroy */
    pragmaOpen,                   /* xOpen - open a cursor */
    pragmaClose,                  /* xClose - close a cursor */
    pragmaFilter,                 /* xFilter - configure scan constraints */
    pragmaNext,                   /* xNext - advance a cursor */
    pragmaEof,                    /* xEof - check for end of scan */
    pragmaColumn,                 /* xColumn - read data */
    pragmaRowid,                  /* xRowid - read data */
    0,                            /* xUpdate */
    0,                            /* xBegin */
    0,                            /* xSync */
    0,                            /* xCommit */
    0,                            /* xRollback */
    0,                            /* xFindMethod */
    0,                            /* xRename */
  };
  return sqlite3_create_module(db, "pragma_table_info", &pragma_module, 0);
}

/*
** Allocate and return nByte bytes of zeroed memory using sqlite3_malloc(). 
** If the allocation fails, set *pRc to SQLITE_NOMEM and return NULL.
*/
static void *idxMalloc(int *pRc, int nByte){
  void *pRet;
  assert( *pRc==SQLITE_OK );
  assert( nByte>0 );
  pRet = sqlite3_malloc(nByte);
  if( pRet ){
    memset(pRet, 0, nByte);
  }else{
    *pRc = SQLITE_NOMEM;
  }
  return pRet;
}

/*
** Allocate and return a new IdxConstraint object. Set the IdxConstraint.zColl
** variable to point to a copy of nul-terminated string zColl.
*/
static IdxConstraint *idxNewConstraint(int *pRc, const char *zColl){
  IdxConstraint *pNew;
  int nColl = strlen(zColl);

  assert( *pRc==SQLITE_OK );
  pNew = (IdxConstraint*)idxMalloc(pRc, sizeof(IdxConstraint) * nColl + 1);
  if( pNew ){
    pNew->zColl = (char*)&pNew[1];
    memcpy(pNew->zColl, zColl, nColl+1);
  }
  return pNew;
}

/*
** SQLITE_DBCONFIG_WHEREINFO callback.
*/
static void idxWhereInfo(
  void *pCtx,                     /* Pointer to IdxContext structure */
  int eOp, 
  const char *zVal, 
  int iVal, 
  i64 mask
){
  IdxContext *p = (IdxContext*)pCtx;

#if 1
  const char *zOp = 
    eOp==SQLITE_WHEREINFO_TABLE ? "TABLE" :
    eOp==SQLITE_WHEREINFO_EQUALS ? "EQUALS" :
    eOp==SQLITE_WHEREINFO_RANGE ? "RANGE" :
    eOp==SQLITE_WHEREINFO_ORDERBY ? "ORDERBY" :
    eOp==SQLITE_WHEREINFO_NEXTOR ? "NEXTOR" :
    eOp==SQLITE_WHEREINFO_ENDOR ? "ENDOR" :
    eOp==SQLITE_WHEREINFO_BEGINOR ? "BEGINOR" :
    "!error!";
  printf("op=%s zVal=%s iVal=%d mask=%llx\n", zOp, zVal, iVal, mask);
#endif

  if( p->rc==SQLITE_OK ){
    assert( eOp==SQLITE_WHEREINFO_TABLE || p->pScan!=0 );
    switch( eOp ){
      case SQLITE_WHEREINFO_TABLE: {
        int nVal = strlen(zVal);
        IdxScan *pNew = (IdxScan*)idxMalloc(&p->rc, sizeof(IdxScan) + nVal + 1);
        if( !pNew ) return;
        pNew->zTable = (char*)&pNew[1];
        memcpy(pNew->zTable, zVal, nVal+1);
        pNew->pNextScan = p->pScan;
        pNew->covering = mask;
        p->pScan = pNew;
        p->pCurrent = &pNew->where;
        break;
      }

      case SQLITE_WHEREINFO_ORDERBY: {
        IdxConstraint *pNew = idxNewConstraint(&p->rc, zVal);
        IdxConstraint **pp;
        if( pNew==0 ) return;
        pNew->iCol = iVal;
        for(pp=&p->pScan->pOrder; *pp; pp=&(*pp)->pNext);
        *pp = pNew;
        break;
      }

      case SQLITE_WHEREINFO_EQUALS:
      case SQLITE_WHEREINFO_RANGE: {
        IdxConstraint *pNew = idxNewConstraint(&p->rc, zVal);
        if( pNew==0 ) return;
        pNew->iCol = iVal;
        pNew->depmask = mask;

        if( eOp==SQLITE_WHEREINFO_RANGE ){
          pNew->pNext = p->pCurrent->pRange;
          p->pCurrent->pRange = pNew;
        }else{
          pNew->pNext = p->pCurrent->pEq;
          p->pCurrent->pEq = pNew;
        }
        break;
      }

      case SQLITE_WHEREINFO_BEGINOR: {
        assert( 0 );
        break;
      }
      case SQLITE_WHEREINFO_ENDOR: {
        assert( 0 );
        break;
      }
      case SQLITE_WHEREINFO_NEXTOR: {
        assert( 0 );
        break;
      }
    }
  }
}

/*
** An error associated with database handle db has just occurred. Pass
** the error message to callback function xOut.
*/
static void idxDatabaseError(
  sqlite3 *db,                    /* Database handle */
  char **pzErrmsg                 /* Write error here */
){
  *pzErrmsg = sqlite3_mprintf("%s", sqlite3_errmsg(db));
}

static int idxCreateTables(sqlite3 *db, sqlite3 *dbm, IdxScan *pScan){
  int rc = SQLITE_OK;
  IdxScan *pIter;
  for(pIter=pScan; pIter; pIter=pIter->pNextScan){
  }
}

static void idxScanFree(IdxScan *pScan){
}

/*
** The xOut callback is invoked to return command output to the user. The
** second argument is always a nul-terminated string. The first argument is
** passed zero if the string contains normal output or non-zero if it is an
** error message.
*/
int shellIndexesCommand(
  sqlite3 *db,                         /* Database handle */
  const char *zSql,                    /* SQL to find indexes for */
  void (*xOut)(void*, const char*),    /* Output callback */
  void *pOutCtx,                       /* Context for xOut() */
  char **pzErrmsg                      /* OUT: Error message (sqlite3_malloc) */
){
  int rc = SQLITE_OK;
  sqlite3 *dbm = 0;
  IdxContext ctx;
  sqlite3_stmt *pStmt = 0;        /* Statement compiled from zSql */

  memset(&ctx, 0, sizeof(IdxContext));

  /* Open an in-memory database to work with. The main in-memory 
  ** database schema contains tables similar to those in the users 
  ** database (handle db). The attached in-memory db (aux) contains
  ** application tables used by the code in this file.  */
  rc = sqlite3_open(":memory:", &dbm);
  if( rc==SQLITE_OK ){
    rc = sqlite3_exec(dbm, 
        "ATTACH ':memory:' AS aux;"
        "CREATE TABLE aux.depmask(mask PRIMARY KEY) WITHOUT ROWID;"
        , 0, 0, 0
    );
  }
  if( rc!=SQLITE_OK ){
    idxDatabaseError(dbm, pzErrmsg);
    goto indexes_out;
  }

  /* Analyze the SELECT statement in zSql. */
  ctx.dbm = dbm;
  sqlite3_db_config(db, SQLITE_DBCONFIG_WHEREINFO, idxWhereInfo, (void*)&ctx);
  rc = sqlite3_prepare(db, zSql, -1, &pStmt, 0);
  sqlite3_db_config(db, SQLITE_DBCONFIG_WHEREINFO, (void*)0, (void*)0);
  if( rc!=SQLITE_OK ){
    idxDatabaseError(db, pzErrmsg);
    goto indexes_out;
  }

  /* Create tables within the main in-memory database. These tables
  ** have the same names, columns and declared types as the tables in
  ** the user database. All constraints except for PRIMARY KEY are
  ** removed. */
  rc = idxCreateTables(db, dbm, ctx.pScan);
  if( rc!=SQLITE_OK ){
    goto indexes_out;
  }

  /* Create candidate indexes within the in-memory database file */

 indexes_out:
  idxScanFree(ctx.pScan);
  sqlite3_close(dbm);
  return rc;
}



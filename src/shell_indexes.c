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

typedef struct IdxColumn IdxColumn;
typedef struct IdxTable IdxTable;

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
** A WHERE clause. Made up of IdxConstraint objects. Example WHERE clause:
**
**   a=? AND b=? AND (c=? OR d=?) AND (e=? OR f=?)
**
** The above
**
**
**
*/
struct IdxWhere {
  IdxConstraint *pEq;             /* List of == constraints */
  IdxConstraint *pRange;          /* List of < constraints */
  IdxWhere *pOr;                  /* List of OR constraints */

  IdxWhere *pNextOr;              /* Next in OR constraints of same IdxWhere */
  IdxWhere *pSibling;             /* Next branch in single OR constraint */
  IdxWhere *pParent;              /* Parent object (or NULL) */
};

/*
** A single scan of a single table.
*/
struct IdxScan {
  IdxTable *pTable;               /* Table-info */
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
  int rc;                         /* Error code (if error has occurred) */
  IdxScan *pScan;                 /* List of scan objects */
  sqlite3 *dbm;                   /* In-memory db for this analysis */
  sqlite3 *db;                    /* User database under analysis */
  sqlite3_stmt *pInsertMask;      /* To write to aux.depmask */
};

/*
** Data regarding a database table. Extracted from "PRAGMA table_info"
*/
struct IdxColumn {
  char *zName;
  char *zColl;
  int iPk;
};
struct IdxTable {
  int nCol;
  IdxColumn *aCol;
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
    "CREATE TABLE a(tbl HIDDEN, cid, name, type, isnotnull, dflt_value, pk)";
  PragmaTable *pTab = 0;
  int rc = SQLITE_OK;

  rc = sqlite3_declare_vtab(db, zSchema);
  if( rc==SQLITE_OK ){
    pTab = (PragmaTable *)sqlite3_malloc64(sizeof(PragmaTable));
    if( pTab==0 ) rc = SQLITE_NOMEM;
  }else{
    *pzErr = sqlite3_mprintf("%s", sqlite3_errmsg(db));
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

static int pragmaClose(sqlite3_vtab_cursor *pCursor){
  PragmaCursor *pCsr = (PragmaCursor*)pCursor;
  sqlite3_finalize(pCsr->pStmt);
  sqlite3_free(pCsr);
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
  if( rc ) return rc;
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

#if 0
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

        sqlite3_bind_int64(p->pInsertMask, 1, mask);
        sqlite3_step(p->pInsertMask);
        p->rc = sqlite3_reset(p->pInsertMask);
        break;
      }

      case SQLITE_WHEREINFO_BEGINOR: {
        IdxWhere *pNew = (IdxWhere*)idxMalloc(&p->rc, sizeof(IdxWhere));
        if( pNew==0 ) return;
        pNew->pParent = p->pCurrent;
        pNew->pNextOr = p->pCurrent->pOr;
        p->pCurrent->pOr = pNew;
        p->pCurrent = pNew;
        break;
      }

      case SQLITE_WHEREINFO_NEXTOR: {
        IdxWhere *pNew = (IdxWhere*)idxMalloc(&p->rc, sizeof(IdxWhere));
        if( pNew==0 ) return;
        pNew->pParent = p->pCurrent->pParent;
        assert( p->pCurrent->pSibling==0 );
        p->pCurrent->pSibling = pNew;
        p->pCurrent = pNew;
        break;
      }

      case SQLITE_WHEREINFO_ENDOR: {
        assert( p->pCurrent->pParent );
        p->pCurrent = p->pCurrent->pParent;
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

static char *idxQueryToList(
  sqlite3 *db, 
  const char *zBind,
  int *pRc,
  char **pzErrmsg,
  const char *zSql
){
  char *zRet = 0;
  if( *pRc==SQLITE_OK ){
    sqlite3_stmt *pStmt = 0;
    int rc = sqlite3_prepare_v2(db, zSql, -1, &pStmt, 0);
    if( rc==SQLITE_OK ){
      sqlite3_bind_text(pStmt, 1, zBind, -1, SQLITE_TRANSIENT);
      while( rc==SQLITE_OK && sqlite3_step(pStmt)==SQLITE_ROW ){
        const char *z = (const char*)sqlite3_column_text(pStmt, 0);
        zRet = sqlite3_mprintf("%z%s%Q", zRet, zRet?", ":"", z);
        if( zRet==0 ){
          rc = SQLITE_NOMEM;
        }
      }
      rc = sqlite3_finalize(pStmt);
    }

    if( rc ){
      idxDatabaseError(db, pzErrmsg);
      sqlite3_free(zRet);
      zRet = 0;
    }
    *pRc = rc;
  }

  return zRet;
}

static int idxGetTableInfo(
  sqlite3 *db,
  IdxScan *pScan,
  char **pzErrmsg
){
  const char *zSql = "SELECT name, pk FROM pragma_table_info(?)";
  sqlite3_stmt *p1 = 0;
  int nCol = 0;
  int nByte = sizeof(IdxTable);
  IdxTable *pNew = 0;
  int rc, rc2;
  char *pCsr;

  rc = sqlite3_prepare_v2(db, zSql, -1, &p1, 0);
  if( rc!=SQLITE_OK ){
    idxDatabaseError(db, pzErrmsg);
    return rc;
  }
  sqlite3_bind_text(p1, 1, pScan->zTable, -1, SQLITE_TRANSIENT);
  while( SQLITE_ROW==sqlite3_step(p1) ){
    const char *zCol = sqlite3_column_text(p1, 0);
    nByte += 1 + strlen(zCol);
    rc = sqlite3_table_column_metadata(
        db, "main", pScan->zTable, zCol, 0, &zCol, 0, 0, 0
    );
    nByte += 1 + strlen(zCol);
    nCol++;
  }
  rc2 = sqlite3_reset(p1);
  if( rc==SQLITE_OK ) rc = rc2;

  nByte += sizeof(IdxColumn) * nCol;
  if( rc==SQLITE_OK ){
    pNew = idxMalloc(&rc, nByte);
  }
  if( rc==SQLITE_OK ){
    pNew->aCol = (IdxColumn*)&pNew[1];
    pNew->nCol = nCol;
    pCsr = (char*)&pNew->aCol[nCol];
  }

  nCol = 0;
  while( rc==SQLITE_OK && SQLITE_ROW==sqlite3_step(p1) ){
    const char *zCol = sqlite3_column_text(p1, 0);
    int nCopy = strlen(zCol) + 1;
    pNew->aCol[nCol].zName = pCsr;
    pNew->aCol[nCol].iPk = sqlite3_column_int(p1, 1);
    memcpy(pCsr, zCol, nCopy);
    pCsr += nCopy;

    rc = sqlite3_table_column_metadata(
        db, "main", pScan->zTable, zCol, 0, &zCol, 0, 0, 0
    );
    if( rc==SQLITE_OK ){
      nCopy = strlen(zCol) + 1;
      pNew->aCol[nCol].zColl = pCsr;
      memcpy(pCsr, zCol, nCopy);
      pCsr += nCopy;
    }

    nCol++;
  }
  rc2 = sqlite3_finalize(p1);
  if( rc==SQLITE_OK ) rc = rc2;

  if( rc==SQLITE_OK ){
    pScan->pTable = pNew;
  }else{
    sqlite3_free(pNew);
  }

  return rc;
}


static int idxCreateTables(
  sqlite3 *db,                    /* User database */
  sqlite3 *dbm,                   /* In-memory database to create tables in */
  IdxScan *pScan,                 /* List of scans */
  char **pzErrmsg                 /* OUT: Error message */
){
  int rc = SQLITE_OK;
  IdxScan *pIter;
  for(pIter=pScan; pIter && rc==SQLITE_OK; pIter=pIter->pNextScan){
    int nPk = 0;
    char *zCols = 0;
    char *zPk = 0;
    char *zCreate = 0;
    int iCol;

    rc = idxGetTableInfo(db, pIter, pzErrmsg);

    for(iCol=0; rc==SQLITE_OK && iCol<pIter->pTable->nCol; iCol++){
      IdxColumn *pCol = &pIter->pTable->aCol[iCol];
      if( pCol->iPk>nPk ) nPk = pCol->iPk;
      zCols = sqlite3_mprintf("%z%s%Q", zCols, (zCols?", ":""), pCol->zName);
      if( zCols==0 ) rc = SQLITE_NOMEM;
    }

    for(iCol=1; rc==SQLITE_OK && iCol<=nPk; iCol++){
      int j;
      for(j=0; j<pIter->pTable->nCol; j++){
        IdxColumn *pCol = &pIter->pTable->aCol[j];
        if( pCol->iPk==iCol ){
          zPk = sqlite3_mprintf("%z%s%Q", zPk, (zPk?", ":""), pCol->zName);
          if( zPk==0 ) rc = SQLITE_NOMEM;
          break;
        }
      }
    }

    if( rc==SQLITE_OK ){
      if( zPk ){
        zCreate = sqlite3_mprintf("CREATE TABLE %Q(%s, PRIMARY KEY(%s))",
            pIter->zTable, zCols, zPk
        );
      }else{
        zCreate = sqlite3_mprintf("CREATE TABLE %Q(%s)", pIter->zTable, zCols);
      }
      if( zCreate==0 ) rc = SQLITE_NOMEM;
    }

    if( rc==SQLITE_OK ){
#if 1
      printf("/* %s */\n", zCreate);
#endif
      rc = sqlite3_exec(dbm, zCreate, 0, 0, pzErrmsg);
    }
    sqlite3_free(zCols);
    sqlite3_free(zPk);
    sqlite3_free(zCreate);
  }
  return rc;
}

/*
** This function is a no-op if *pRc is set to anything other than 
** SQLITE_OK when it is called.
**
** If *pRc is initially set to SQLITE_OK, then the text specified by
** the printf() style arguments is appended to zIn and the result returned
** in a buffer allocated by sqlite3_malloc(). sqlite3_free() is called on
** zIn before returning.
*/
static char *idxAppendText(int *pRc, char *zIn, const char *zFmt, ...){
  va_list ap;
  char *zAppend = 0;
  char *zRet = 0;
  int nIn = zIn ? strlen(zIn) : 0;
  int nAppend = 0;
  va_start(ap, zFmt);
  if( *pRc==SQLITE_OK ){
    zAppend = sqlite3_vmprintf(zFmt, ap);
    if( zAppend ){
      nAppend = strlen(zAppend);
      zRet = (char*)sqlite3_malloc(nIn + nAppend);
    }
    if( zAppend && zRet ){
      memcpy(zRet, zIn, nIn);
      memcpy(&zRet[nIn], zAppend, nAppend+1);
    }else{
      sqlite3_free(zRet);
      zRet = 0;
      *pRc = SQLITE_NOMEM;
    }
    sqlite3_free(zAppend);
    sqlite3_free(zIn);
  }
  va_end(ap);
  return zRet;
}

static char *idxAppendColDefn(
  int *pRc, 
  char *zIn, 
  IdxTable *pTab, 
  IdxConstraint *pCons
){
  char *zRet = zIn;
  IdxColumn *p = &pTab->aCol[pCons->iCol];
  if( zRet ) zRet = idxAppendText(pRc, zRet, ", ");
  zRet = idxAppendText(pRc, zRet, "%Q", p->zName);
  if( sqlite3_stricmp(p->zColl, pCons->zColl) ){
    zRet = idxAppendText(pRc, zRet, " COLLATE %Q", pCons->zColl);
  }
  return zRet;
}

static int idxCreateFromCons(
  sqlite3 *dbm,
  IdxScan *pScan,
  IdxConstraint *pEq, 
  IdxConstraint *pTail
){
  int rc = SQLITE_OK;
  if( pEq || pTail ){
    IdxTable *pTab = pScan->pTable;
    char *zCols = 0;
    char *zIdx = 0;
    IdxConstraint *pCons;
    int h = 0;

    for(pCons=pEq; pCons; pCons=pCons->pLink){
      zCols = idxAppendColDefn(&rc, zCols, pTab, pCons);
    }
    for(pCons=pTail; pCons; pCons=pCons->pLink){
      zCols = idxAppendColDefn(&rc, zCols, pTab, pCons);
    }

    /* Hash the list of columns to come up with a name for the index */
    if( rc==SQLITE_OK ){
      int i;
      for(i=0; zCols[i]; i++){
        h += ((h<<3) + zCols[i]);
      }

      zIdx = sqlite3_mprintf("CREATE INDEX IF NOT EXISTS "
          "'%q_idx_%08x' ON %Q(%s)", pScan->zTable, h, pScan->zTable, zCols
      );
      if( !zIdx ){
        rc = SQLITE_NOMEM;
      }else{
        rc = sqlite3_exec(dbm, zIdx, 0, 0, 0);
        printf("/* %s */\n", zIdx);
      }
    }

    sqlite3_free(zIdx);
    sqlite3_free(zCols);
  }
  return rc;
}

static int idxCreateFromWhere(
    sqlite3*, i64, IdxScan*, IdxWhere*, IdxConstraint*, IdxConstraint*
);

static int idxCreateForeachOr(
  sqlite3 *dbm, 
  i64 mask,                       /* Consider only these constraints */
  IdxScan *pScan,                 /* Create indexes for this scan */
  IdxWhere *pWhere,               /* Read constraints from here */
  IdxConstraint *pEq,             /* == constraints for inclusion */
  IdxConstraint *pTail            /* range/ORDER BY constraints for inclusion */
){
  int rc = SQLITE_OK;
  IdxWhere *p1;
  IdxWhere *p2;
  for(p1=pWhere->pOr; p1 && rc==SQLITE_OK; p1=p1->pNextOr){
    rc = idxCreateFromWhere(dbm, mask, pScan, p1, pEq, pTail);
    for(p2=p1->pSibling; p2 && rc==SQLITE_OK; p2=p2->pSibling){
      rc = idxCreateFromWhere(dbm, mask, pScan, p2, pEq, pTail);
    }
  }
  return rc;
}

static int idxCreateFromWhere(
  sqlite3 *dbm, 
  i64 mask,                       /* Consider only these constraints */
  IdxScan *pScan,                 /* Create indexes for this scan */
  IdxWhere *pWhere,               /* Read constraints from here */
  IdxConstraint *pEq,             /* == constraints for inclusion */
  IdxConstraint *pTail            /* range/ORDER BY constraints for inclusion */
){
  IdxConstraint *p1 = pEq;
  IdxConstraint *pCon;
  int rc;

  /* Gather up all the == constraints that match the mask. */
  for(pCon=pWhere->pEq; pCon; pCon=pCon->pNext){
    if( (mask & pCon->depmask)==pCon->depmask ){
      pCon->pLink = p1;
      p1 = pCon;
    }
  }

  /* Create an index using the == constraints collected above. And the
  ** range constraint/ORDER BY terms passed in by the caller, if any. */
  rc = idxCreateFromCons(dbm, pScan, p1, pTail);
  if( rc==SQLITE_OK ){
    rc = idxCreateForeachOr(dbm, mask, pScan, pWhere, p1, pTail);
  }

  /* If no range/ORDER BY passed by the caller, create a version of the
  ** index for each range constraint that matches the mask. */
  if( pTail==0 ){
    for(pCon=pWhere->pRange; rc==SQLITE_OK && pCon; pCon=pCon->pNext){
      assert( pCon->pLink==0 );
      if( (mask & pCon->depmask)==pCon->depmask ){
        rc = idxCreateFromCons(dbm, pScan, p1, pCon);
        if( rc==SQLITE_OK ){
          rc = idxCreateForeachOr(dbm, mask, pScan, pWhere, p1, pCon);
        }
      }
    }
  }

  return rc;
}

static int idxPrepareStmt(
  sqlite3 *db,                    /* Database handle to compile against */
  const char *zSql,               /* SQL statement to compile */
  sqlite3_stmt **ppStmt,          /* OUT: Compiled SQL statement */
  char **pzErrmsg                 /* OUT: sqlite3_malloc()ed error message */
){
  int rc = sqlite3_prepare_v2(db, zSql, -1, ppStmt, 0);
  if( rc!=SQLITE_OK ){
    *ppStmt = 0;
    idxDatabaseError(db, pzErrmsg);
  }
  return rc;
}

/*
** Create candidate indexes in database [dbm] based on the data in 
** linked-list pScan.
*/
static int idxCreateCandidates(
  sqlite3 *dbm,
  IdxScan *pScan,
  char **pzErrmsg
){
  int rc2;
  int rc = SQLITE_OK;
  sqlite3_stmt *pDepmask;         /* Foreach depmask */
  IdxScan *pIter;

  rc = idxPrepareStmt(dbm, "SELECT mask FROM depmask", &pDepmask, pzErrmsg);

  for(pIter=pScan; pIter && rc==SQLITE_OK; pIter=pIter->pNextScan){
    IdxWhere *pWhere = &pIter->where;
    while( SQLITE_ROW==sqlite3_step(pDepmask) && rc==SQLITE_OK ){
      i64 mask = sqlite3_column_int64(pDepmask, 0);
      rc = idxCreateFromWhere(dbm, mask, pIter, pWhere, 0, 0);
      if( rc==SQLITE_OK && pIter->pOrder ){
        rc = idxCreateFromWhere(dbm, mask, pIter, pWhere, 0, pIter->pOrder);
      }
    }
  }

  rc2 = sqlite3_finalize(pDepmask);
  if( rc==SQLITE_OK ) rc = rc2;
  return rc;
}

static void idxScanFree(IdxScan *pScan){
}

int idxFindIndexes(
  sqlite3 *dbm,                        /* Database handle */
  const char *zSql,                    /* SQL to find indexes for */
  void (*xOut)(void*, const char*),    /* Output callback */
  void *pOutCtx,                       /* Context for xOut() */
  char **pzErrmsg                      /* OUT: Error message (sqlite3_malloc) */
){
  char *zExplain;
  sqlite3_stmt *pExplain;
  int rc;

  zExplain = sqlite3_mprintf("EXPLAIN QUERY PLAN %s", zSql);
  if( zExplain==0 ){
    rc = SQLITE_NOMEM;
  }else{
    rc = idxPrepareStmt(dbm, zExplain, &pExplain, pzErrmsg);
    sqlite3_free(zExplain);
  }
  if( rc!=SQLITE_OK ) return rc;

  while( sqlite3_step(pExplain)==SQLITE_ROW ){
    int iCol;
    // for(iCol=0; iCol<sqlite3_column_count(pExplain); iCol++){ }
    xOut(pOutCtx, sqlite3_column_text(pExplain, 3));
  }
  rc = sqlite3_finalize(pExplain);
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

  rc = registerPragmaVtabs(db);
  if( rc ) return rc;
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
        "INSERT INTO aux.depmask VALUES(0);"
        , 0, 0, 0
    );
  }

  /* Prepare an INSERT statement for writing to aux.depmask */
  if( rc==SQLITE_OK ){
    rc = sqlite3_prepare_v2(dbm, 
        "INSERT OR IGNORE INTO depmask SELECT mask | ?1 FROM depmask;", -1,
        &ctx.pInsertMask, 0
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
  rc = idxCreateTables(db, dbm, ctx.pScan, pzErrmsg);
  if( rc!=SQLITE_OK ){
    goto indexes_out;
  }

  /* Create candidate indexes within the in-memory database file */
  rc = idxCreateCandidates(dbm, ctx.pScan, pzErrmsg);
  if( rc!=SQLITE_OK ){
    goto indexes_out;
  }

  rc = idxFindIndexes(dbm, zSql, xOut, pOutCtx, pzErrmsg);

 indexes_out:
  idxScanFree(ctx.pScan);
  sqlite3_close(dbm);
  return rc;
}



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
typedef sqlite3_uint64 u64;

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
**   Used to temporarily link IdxConstraint objects into lists while
**   creating candidate indexes.
*/
struct IdxConstraint {
  char *zColl;                    /* Collation sequence */
  int bRange;                     /* True for range, false for eq */
  int iCol;                       /* Constrained table column */
  i64 depmask;                    /* Dependency mask */
  int bFlag;                      /* Used by idxFindCompatible() */
  int bDesc;                      /* True if ORDER BY <expr> DESC */
  IdxConstraint *pNext;           /* Next constraint in pEq or pRange list */
  IdxConstraint *pLink;           /* See above */
};

/*
** A WHERE clause. Made up of IdxConstraint objects. Example WHERE clause:
**
**   a=? AND b=? AND ((c=? AND d=?) OR e=?) AND (f=? OR g=?) AND h>?
**
** The above is decomposed into 5 AND connected clauses. The first two are
** added to the IdxWhere.pEq linked list, the following two into 
** IdxWhere.pOr and the last into IdxWhere.pRange.
**
** IdxWhere.pEq and IdxWhere.pRange are simple linked lists of IdxConstraint
** objects linked by the IdxConstraint.pNext field.
**
** The list headed at IdxWhere.pOr and linked by IdxWhere.pNextOr contains
** all "OR" terms that belong to the current WHERE clause. In the example
** above, there are two OR terms:
**
**   ((c=? AND d=?) OR e=?)
**   (f=? OR g=?)
**
** Within an OR term, the OR connected sub-expressions are termed siblings.
** These are connected into a linked list by the pSibling pointers. Each OR
** term above consists of two siblings.
**
**   pOr -> (c=? AND d=?) -> pNextOr -> (f=?)
**               |                        |
**            pSibling                 pSibling
**               |                        |
**               V                        V
**             (e=?)                    (g=?)
**
** IdxWhere.pParent is only used while constructing a tree of IdxWhere 
** structures. It is NULL for the root IdxWhere. For all others, the parent
** WHERE clause.
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
** Context object passed to idxWhereInfo() and other functions.
*/
struct IdxContext {
  char **pzErrmsg;
  IdxWhere *pCurrent;             /* Current where clause */
  int rc;                         /* Error code (if error has occurred) */
  IdxScan *pScan;                 /* List of scan objects */
  sqlite3 *dbm;                   /* In-memory db for this analysis */
  sqlite3 *db;                    /* User database under analysis */
  sqlite3_stmt *pInsertMask;      /* To write to aux.depmask */
  i64 iIdxRowid;                  /* Rowid of first index created */
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
  u64 mask
){
  IdxContext *p = (IdxContext*)pCtx;

#if 0
  const char *zOp = 
    eOp==SQLITE_WHEREINFO_TABLE ? "TABLE" :
    eOp==SQLITE_WHEREINFO_EQUALS ? "EQUALS" :
    eOp==SQLITE_WHEREINFO_RANGE ? "RANGE" :
    eOp==SQLITE_WHEREINFO_ORDERBY ? "ORDERBY" :
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
        if( pNew==0 ) return;
        pNew->iCol = iVal;
        pNew->bDesc = (int)mask;
        if( p->pScan->pOrder==0 ){
          p->pScan->pOrder = pNew;
        }else{
          IdxConstraint *pIter;
          for(pIter=p->pScan->pOrder; pIter->pNext; pIter=pIter->pNext);
          pIter->pNext = pNew;
          pIter->pLink = pNew;
        }
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

static int idxPrepareStmt(
  sqlite3 *db,                    /* Database handle to compile against */
  sqlite3_stmt **ppStmt,          /* OUT: Compiled SQL statement */
  char **pzErrmsg,                /* OUT: sqlite3_malloc()ed error message */
  const char *zSql                /* SQL statement to compile */
){
  int rc = sqlite3_prepare_v2(db, zSql, -1, ppStmt, 0);
  if( rc!=SQLITE_OK ){
    *ppStmt = 0;
    idxDatabaseError(db, pzErrmsg);
  }
  return rc;
}

static int idxPrintfPrepareStmt(
  sqlite3 *db,                    /* Database handle to compile against */
  sqlite3_stmt **ppStmt,          /* OUT: Compiled SQL statement */
  char **pzErrmsg,                /* OUT: sqlite3_malloc()ed error message */
  const char *zFmt,               /* printf() format of SQL statement */
  ...                             /* Trailing printf() arguments */
){
  va_list ap;
  int rc;
  char *zSql;
  va_start(ap, zFmt);
  zSql = sqlite3_vmprintf(zFmt, ap);
  if( zSql==0 ){
    rc = SQLITE_NOMEM;
  }else{
    rc = idxPrepareStmt(db, ppStmt, pzErrmsg, zSql);
    sqlite3_free(zSql);
  }
  va_end(ap);
  return rc;
}

static int idxGetTableInfo(
  sqlite3 *db,
  IdxScan *pScan,
  char **pzErrmsg
){
  const char *zTbl = pScan->zTable;
  sqlite3_stmt *p1 = 0;
  int nCol = 0;
  int nByte = sizeof(IdxTable);
  IdxTable *pNew = 0;
  int rc, rc2;
  char *pCsr;

  rc = idxPrintfPrepareStmt(db, &p1, pzErrmsg, "PRAGMA table_info=%Q", zTbl);
  while( rc==SQLITE_OK && SQLITE_ROW==sqlite3_step(p1) ){
    const char *zCol = sqlite3_column_text(p1, 1);
    nByte += 1 + strlen(zCol);
    rc = sqlite3_table_column_metadata(
        db, "main", zTbl, zCol, 0, &zCol, 0, 0, 0
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
    const char *zCol = sqlite3_column_text(p1, 1);
    int nCopy = strlen(zCol) + 1;
    pNew->aCol[nCol].zName = pCsr;
    pNew->aCol[nCol].iPk = sqlite3_column_int(p1, 5);
    memcpy(pCsr, zCol, nCopy);
    pCsr += nCopy;

    rc = sqlite3_table_column_metadata(
        db, "main", zTbl, zCol, 0, &zCol, 0, 0, 0
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
    rc = idxGetTableInfo(db, pIter, pzErrmsg);

    /* Test if table has already been created. If so, jump to the next
    ** iteration of the loop.  */
    if( rc==SQLITE_OK ){
      sqlite3_stmt *pSql = 0;
      rc = idxPrintfPrepareStmt(dbm, &pSql, pzErrmsg, 
          "SELECT 1 FROM sqlite_master WHERE tbl_name = %Q", pIter->zTable
      );
      if( rc==SQLITE_OK ){
        int bSkip = 0;
        if( sqlite3_step(pSql)==SQLITE_ROW ) bSkip = 1;
        rc = sqlite3_finalize(pSql);
        if( bSkip ) continue;
      }
    }

    if( rc==SQLITE_OK ){
      int rc2;
      sqlite3_stmt *pSql = 0;
      rc = idxPrintfPrepareStmt(db, &pSql, pzErrmsg, 
          "SELECT sql FROM sqlite_master WHERE tbl_name = %Q", pIter->zTable
      );
      while( rc==SQLITE_OK && SQLITE_ROW==sqlite3_step(pSql) ){
        const char *zSql = (const char*)sqlite3_column_text(pSql, 0);
        rc = sqlite3_exec(dbm, zSql, 0, 0, pzErrmsg);
      }
      rc2 = sqlite3_finalize(pSql);
      if( rc==SQLITE_OK ) rc = rc2;
    }
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
      zRet = (char*)sqlite3_malloc(nIn + nAppend + 1);
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

static int idxIdentifierRequiresQuotes(const char *zId){
  int i;
  for(i=0; zId[i]; i++){
    if( !(zId[i]=='_')
     && !(zId[i]>='0' && zId[i]<='9')
     && !(zId[i]>='a' && zId[i]<='z')
     && !(zId[i]>='A' && zId[i]<='Z')
    ){
      return 1;
    }
  }
  return 0;
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

  if( idxIdentifierRequiresQuotes(p->zName) ){
    zRet = idxAppendText(pRc, zRet, "%Q", p->zName);
  }else{
    zRet = idxAppendText(pRc, zRet, "%s", p->zName);
  }

  if( sqlite3_stricmp(p->zColl, pCons->zColl) ){
    if( idxIdentifierRequiresQuotes(pCons->zColl) ){
      zRet = idxAppendText(pRc, zRet, " COLLATE %Q", pCons->zColl);
    }else{
      zRet = idxAppendText(pRc, zRet, " COLLATE %s", pCons->zColl);
    }
  }

  if( pCons->bDesc ){
    zRet = idxAppendText(pRc, zRet, " DESC");
  }
  return zRet;
}

/*
** Search database dbm for an index compatible with the one idxCreateFromCons()
** would create from arguments pScan, pEq and pTail. If no error occurs and 
** such an index is found, return non-zero. Or, if no such index is found,
** return zero.
**
** If an error occurs, set *pRc to an SQLite error code and return zero.
*/
static int idxFindCompatible(
  int *pRc,                       /* OUT: Error code */
  sqlite3* dbm,                   /* Database to search */
  IdxScan *pScan,                 /* Scan for table to search for index on */
  IdxConstraint *pEq,             /* List of == constraints */
  IdxConstraint *pTail            /* List of range constraints */
){
  const char *zTbl = pScan->zTable;
  sqlite3_stmt *pIdxList = 0;
  IdxConstraint *pIter;
  int nEq = 0;                    /* Number of elements in pEq */
  int rc, rc2;


  /* Count the elements in list pEq */
  for(pIter=pEq; pIter; pIter=pIter->pLink) nEq++;

  rc = idxPrintfPrepareStmt(dbm, &pIdxList, 0, "PRAGMA index_list=%Q", zTbl);
  while( rc==SQLITE_OK && sqlite3_step(pIdxList)==SQLITE_ROW ){
    int bMatch = 1;
    IdxConstraint *pT = pTail;
    sqlite3_stmt *pInfo = 0;
    const char *zIdx = (const char*)sqlite3_column_text(pIdxList, 1);

    /* Zero the IdxConstraint.bFlag values in the pEq list */
    for(pIter=pEq; pIter; pIter=pIter->pLink) pIter->bFlag = 0;

    rc = idxPrintfPrepareStmt(dbm, &pInfo, 0, "PRAGMA index_xInfo=%Q", zIdx);
    while( rc==SQLITE_OK && sqlite3_step(pInfo)==SQLITE_ROW ){
      int iIdx = sqlite3_column_int(pInfo, 0);
      int iCol = sqlite3_column_int(pInfo, 1);
      const char *zColl = (const char*)sqlite3_column_text(pInfo, 4);

      if( iIdx<nEq ){
        for(pIter=pEq; pIter; pIter=pIter->pLink){
          if( pIter->bFlag ) continue;
          if( pIter->iCol!=iCol ) continue;
          if( sqlite3_stricmp(pIter->zColl, zColl) ) continue;
          pIter->bFlag = 1;
          break;
        }
        if( pIter==0 ){
          bMatch = 0;
          break;
        }
      }else{
        if( pT ){
          if( pT->iCol!=iCol || sqlite3_stricmp(pT->zColl, zColl) ){
            bMatch = 0;
            break;
          }
          pT = pT->pLink;
        }
      }
    }
    rc2 = sqlite3_finalize(pInfo);
    if( rc==SQLITE_OK ) rc = rc2;

    if( rc==SQLITE_OK && bMatch ){
      sqlite3_finalize(pIdxList);
      return 1;
    }
  }
  rc2 = sqlite3_finalize(pIdxList);
  if( rc==SQLITE_OK ) rc = rc2;

  *pRc = rc;
  return 0;
}

static int idxCreateFromCons(
  IdxContext *pCtx,
  IdxScan *pScan,
  IdxConstraint *pEq, 
  IdxConstraint *pTail
){
  sqlite3 *dbm = pCtx->dbm;
  int rc = SQLITE_OK;
  if( (pEq || pTail) && 0==idxFindCompatible(&rc, dbm, pScan, pEq, pTail) ){
    IdxTable *pTab = pScan->pTable;
    char *zCols = 0;
    char *zIdx = 0;
    IdxConstraint *pCons;
    int h = 0;
    const char *zFmt;

    for(pCons=pEq; pCons; pCons=pCons->pLink){
      zCols = idxAppendColDefn(&rc, zCols, pTab, pCons);
    }
    for(pCons=pTail; pCons; pCons=pCons->pLink){
      zCols = idxAppendColDefn(&rc, zCols, pTab, pCons);
    }

    if( rc==SQLITE_OK ){
      /* Hash the list of columns to come up with a name for the index */
      int i;
      for(i=0; zCols[i]; i++){
        h += ((h<<3) + zCols[i]);
      }

      if( idxIdentifierRequiresQuotes(pScan->zTable) ){
        zFmt = "CREATE INDEX '%q_idx_%08x' ON %Q(%s)";
      }else{
        zFmt = "CREATE INDEX %s_idx_%08x ON %s(%s)";
      }
      zIdx = sqlite3_mprintf(zFmt, pScan->zTable, h, pScan->zTable, zCols);
      if( !zIdx ){
        rc = SQLITE_NOMEM;
      }else{
        rc = sqlite3_exec(dbm, zIdx, 0, 0, pCtx->pzErrmsg);
#if 0
        printf("CANDIDATE: %s\n", zIdx);
#endif
      }
    }
    if( rc==SQLITE_OK && pCtx->iIdxRowid==0 ){
      int rc2;
      sqlite3_stmt *pLast = 0;
      rc = idxPrepareStmt(dbm, &pLast, pCtx->pzErrmsg, 
          "SELECT max(rowid) FROM sqlite_master"
      );
      if( rc==SQLITE_OK && SQLITE_ROW==sqlite3_step(pLast) ){
        pCtx->iIdxRowid = sqlite3_column_int64(pLast, 0);
      }
      rc2 = sqlite3_finalize(pLast);
      if( rc==SQLITE_OK ) rc = rc2;
    }

    sqlite3_free(zIdx);
    sqlite3_free(zCols);
  }
  return rc;
}

static int idxCreateFromWhere(
    IdxContext*, i64, IdxScan*, IdxWhere*, IdxConstraint*, IdxConstraint*
);

static int idxCreateForeachOr(
  IdxContext *pCtx, 
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
    rc = idxCreateFromWhere(pCtx, mask, pScan, p1, pEq, pTail);
    for(p2=p1->pSibling; p2 && rc==SQLITE_OK; p2=p2->pSibling){
      rc = idxCreateFromWhere(pCtx, mask, pScan, p2, pEq, pTail);
    }
  }
  return rc;
}

/*
** Return true if list pList (linked by IdxConstraint.pLink) contains
** a constraint compatible with *p. Otherwise return false.
*/
static int idxFindConstraint(IdxConstraint *pList, IdxConstraint *p){
  IdxConstraint *pCmp;
  for(pCmp=pList; pCmp; pCmp=pCmp->pLink){
    if( p->iCol==pCmp->iCol ) return 1;
  }
  return 0;
}

static int idxCreateFromWhere(
  IdxContext *pCtx, 
  i64 mask,                       /* Consider only these constraints */
  IdxScan *pScan,                 /* Create indexes for this scan */
  IdxWhere *pWhere,               /* Read constraints from here */
  IdxConstraint *pEq,             /* == constraints for inclusion */
  IdxConstraint *pTail            /* range/ORDER BY constraints for inclusion */
){
  sqlite3 *dbm = pCtx->dbm;
  IdxConstraint *p1 = pEq;
  IdxConstraint *pCon;
  int rc;

  /* Gather up all the == constraints that match the mask. */
  for(pCon=pWhere->pEq; pCon; pCon=pCon->pNext){
    if( (mask & pCon->depmask)==pCon->depmask 
     && idxFindConstraint(p1, pCon)==0
     && idxFindConstraint(pTail, pCon)==0
    ){
      pCon->pLink = p1;
      p1 = pCon;
    }
  }

  /* Create an index using the == constraints collected above. And the
  ** range constraint/ORDER BY terms passed in by the caller, if any. */
  rc = idxCreateFromCons(pCtx, pScan, p1, pTail);
  if( rc==SQLITE_OK ){
    rc = idxCreateForeachOr(pCtx, mask, pScan, pWhere, p1, pTail);
  }

  /* If no range/ORDER BY passed by the caller, create a version of the
  ** index for each range constraint that matches the mask. */
  if( pTail==0 ){
    for(pCon=pWhere->pRange; rc==SQLITE_OK && pCon; pCon=pCon->pNext){
      assert( pCon->pLink==0 );
      if( (mask & pCon->depmask)==pCon->depmask
        && idxFindConstraint(pEq, pCon)==0
        && idxFindConstraint(pTail, pCon)==0
      ){
        rc = idxCreateFromCons(pCtx, pScan, p1, pCon);
        if( rc==SQLITE_OK ){
          rc = idxCreateForeachOr(pCtx, mask, pScan, pWhere, p1, pCon);
        }
      }
    }
  }

  return rc;
}

/*
** Create candidate indexes in database [dbm] based on the data in 
** linked-list pScan.
*/
static int idxCreateCandidates(IdxContext *pCtx){
  sqlite3 *dbm = pCtx->dbm;
  int rc2;
  int rc = SQLITE_OK;
  sqlite3_stmt *pDepmask;         /* Foreach depmask */
  IdxScan *pIter;

  rc = idxPrepareStmt(pCtx->dbm, &pDepmask, pCtx->pzErrmsg, 
      "SELECT mask FROM depmask"
  );

  for(pIter=pCtx->pScan; pIter && rc==SQLITE_OK; pIter=pIter->pNextScan){
    IdxWhere *pWhere = &pIter->where;
    while( SQLITE_ROW==sqlite3_step(pDepmask) && rc==SQLITE_OK ){
      i64 mask = sqlite3_column_int64(pDepmask, 0);
      rc = idxCreateFromWhere(pCtx, mask, pIter, pWhere, 0, 0);
      if( rc==SQLITE_OK && pIter->pOrder ){
        rc = idxCreateFromWhere(pCtx, mask, pIter, pWhere, 0, pIter->pOrder);
      }
    }
  }

  rc2 = sqlite3_finalize(pDepmask);
  if( rc==SQLITE_OK ) rc = rc2;
  return rc;
}

static void idxScanFree(IdxScan *pScan){
  IdxScan *pIter;
  IdxScan *pNext;
  for(pIter=pScan; pIter; pIter=pNext){
    pNext = pIter->pNextScan;

  }
}

int idxFindIndexes(
  IdxContext *pCtx,
  const char *zSql,                    /* SQL to find indexes for */
  void (*xOut)(void*, const char*),    /* Output callback */
  void *pOutCtx,                       /* Context for xOut() */
  char **pzErr                         /* OUT: Error message (sqlite3_malloc) */
){
  sqlite3 *dbm = pCtx->dbm;
  sqlite3_stmt *pExplain = 0;
  sqlite3_stmt *pSelect = 0;
  sqlite3_stmt *pInsert = 0;
  int rc, rc2;
  int bFound = 0;

  rc = idxPrintfPrepareStmt(dbm, &pExplain, pzErr,"EXPLAIN QUERY PLAN %s",zSql);
  if( rc==SQLITE_OK ){
    rc = idxPrepareStmt(dbm, &pSelect, pzErr, 
        "SELECT rowid, sql FROM sqlite_master WHERE name = ?"
    );
  }
  if( rc==SQLITE_OK ){
    rc = idxPrepareStmt(dbm, &pInsert, pzErr,
        "INSERT OR IGNORE INTO aux.indexes VALUES(?)"
    );
  }

  while( rc==SQLITE_OK && sqlite3_step(pExplain)==SQLITE_ROW ){
    int i;
    const char *zDetail = (const char*)sqlite3_column_text(pExplain, 3);
    int nDetail = strlen(zDetail);

    for(i=0; i<nDetail; i++){
      const char *zIdx = 0;
      if( memcmp(&zDetail[i], " USING INDEX ", 13)==0 ){
        zIdx = &zDetail[i+13];
      }else if( memcmp(&zDetail[i], " USING COVERING INDEX ", 22)==0 ){
        zIdx = &zDetail[i+22];
      }
      if( zIdx ){
        int nIdx = 0;
        while( zIdx[nIdx]!='\0' && (zIdx[nIdx]!=' ' || zIdx[nIdx+1]!='(') ){
          nIdx++;
        }
        sqlite3_bind_text(pSelect, 1, zIdx, nIdx, SQLITE_STATIC);
        if( SQLITE_ROW==sqlite3_step(pSelect) ){
          i64 iRowid = sqlite3_column_int64(pSelect, 0);
          const char *zSql = (const char*)sqlite3_column_text(pSelect, 1);
          if( iRowid>=pCtx->iIdxRowid ){
            sqlite3_bind_text(pInsert, 1, zSql, -1, SQLITE_STATIC);
            sqlite3_step(pInsert);
            rc = sqlite3_reset(pInsert);
            if( rc ) goto find_indexes_out;
          }
        }
        rc = sqlite3_reset(pSelect);
        break;
      }
    }
  }
  rc2 = sqlite3_reset(pExplain);
  if( rc==SQLITE_OK ) rc = rc2;
  if( rc==SQLITE_OK ){
    sqlite3_stmt *pLoop = 0;
    rc = idxPrepareStmt(dbm, &pLoop, pzErr, "SELECT name FROM aux.indexes");
    if( rc==SQLITE_OK ){
      while( SQLITE_ROW==sqlite3_step(pLoop) ){
        bFound = 1;
        xOut(pOutCtx, sqlite3_column_text(pLoop, 0));
      }
      rc = sqlite3_finalize(pLoop);
    }
    if( rc==SQLITE_OK ){
      if( bFound==0 ) xOut(pOutCtx, "(no new indexes)");
      xOut(pOutCtx, "");
    }
  }

  while( rc==SQLITE_OK && sqlite3_step(pExplain)==SQLITE_ROW ){
    int iSelectid = sqlite3_column_int(pExplain, 0);
    int iOrder = sqlite3_column_int(pExplain, 1);
    int iFrom = sqlite3_column_int(pExplain, 2);
    const char *zDetail = (const char*)sqlite3_column_text(pExplain, 3);
    char *zOut;

    zOut = sqlite3_mprintf("%d|%d|%d|%s", iSelectid, iOrder, iFrom, zDetail);
    if( zOut==0 ){
      rc = SQLITE_NOMEM;
    }else{
      xOut(pOutCtx, zOut);
      sqlite3_free(zOut);
    }
  }

 find_indexes_out:
  rc2 = sqlite3_finalize(pExplain);
  if( rc==SQLITE_OK ) rc = rc2;
  rc2 = sqlite3_finalize(pSelect);
  if( rc==SQLITE_OK ) rc = rc2;
  rc2 = sqlite3_finalize(pInsert);
  if( rc==SQLITE_OK ) rc = rc2;

  return rc;
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
  ctx.pzErrmsg = pzErrmsg;

  /* Open an in-memory database to work with. The main in-memory 
  ** database schema contains tables similar to those in the users 
  ** database (handle db). The attached in-memory db (aux) contains
  ** application tables used by the code in this file.  */
  rc = sqlite3_open(":memory:", &dbm);
  if( rc==SQLITE_OK ){
    rc = sqlite3_exec(dbm, 
        "ATTACH ':memory:' AS aux;"
        "CREATE TABLE aux.depmask(mask PRIMARY KEY) WITHOUT ROWID;"
        "CREATE TABLE aux.indexes(name PRIMARY KEY) WITHOUT ROWID;"
        "INSERT INTO aux.depmask VALUES(0);"
        , 0, 0, pzErrmsg
    );
  }

  /* Prepare an INSERT statement for writing to aux.depmask */
  if( rc==SQLITE_OK ){
    rc = idxPrepareStmt(dbm, &ctx.pInsertMask, pzErrmsg,
        "INSERT OR IGNORE INTO aux.depmask SELECT mask | ?1 FROM aux.depmask;"
    );
  }

  /* Analyze the SELECT statement in zSql. */
  if( rc==SQLITE_OK ){
    ctx.dbm = dbm;
    sqlite3_whereinfo_hook(db, idxWhereInfo, (void*)&ctx);
    rc = idxPrepareStmt(db, &pStmt, pzErrmsg, zSql);
    sqlite3_whereinfo_hook(db, 0, 0);
    sqlite3_finalize(pStmt);
  }

  /* Create tables within the main in-memory database. These tables
  ** have the same names, columns and declared types as the tables in
  ** the user database. All constraints except for PRIMARY KEY are
  ** removed. */
  if( rc==SQLITE_OK ){
    rc = idxCreateTables(db, dbm, ctx.pScan, pzErrmsg);
  }

  /* Create candidate indexes within the in-memory database file */
  if( rc==SQLITE_OK ){
    rc = idxCreateCandidates(&ctx);
  }

  /* Figure out which of the candidate indexes are preferred by the query
  ** planner and report the results to the user.  */
  if( rc==SQLITE_OK ){
    rc = idxFindIndexes(&ctx, zSql, xOut, pOutCtx, pzErrmsg);
  }

  idxScanFree(ctx.pScan);
  sqlite3_finalize(ctx.pInsertMask);
  sqlite3_close(dbm);
  return rc;
}



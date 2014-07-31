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
typedef struct Fts5Global Fts5Global;
typedef struct Fts5Auxiliary Fts5Auxiliary;
typedef struct Fts5Auxdata Fts5Auxdata;

/*
** A single object of this type is allocated when the FTS5 module is 
** registered with a database handle. It is used to store pointers to
** all registered FTS5 extensions - tokenizers and auxiliary functions.
*/
struct Fts5Global {
  sqlite3 *db;                    /* Associated database connection */ 
  i64 iNextId;                    /* Used to allocate unique cursor ids */
  Fts5Auxiliary *pAux;            /* First in list of all aux. functions */
  Fts5Cursor *pCsr;               /* First in list of all open cursors */
};

/*
** Each auxiliary function registered with the FTS5 module is represented
** by an object of the following type. All such objects are stored as part
** of the Fts5Global.pAux list.
*/
struct Fts5Auxiliary {
  Fts5Global *pGlobal;            /* Global context for this function */
  char *zFunc;                    /* Function name (nul-terminated) */
  void *pUserData;                /* User-data pointer */
  fts5_extension_function xFunc;  /* Callback function */
  void (*xDestroy)(void*);        /* Destructor function */
  Fts5Auxiliary *pNext;           /* Next registered auxiliary function */
};

/*
** Virtual-table object.
*/
struct Fts5Table {
  sqlite3_vtab base;              /* Base class used by SQLite core */
  Fts5Config *pConfig;            /* Virtual table configuration */
  Fts5Index *pIndex;              /* Full-text index */
  Fts5Storage *pStorage;          /* Document store */
  Fts5Global *pGlobal;            /* Global (connection wide) data */
  Fts5Cursor *pSortCsr;           /* Sort data from this cursor */
};

struct Fts5MatchPhrase {
  Fts5Buffer *pPoslist;           /* Pointer to current poslist */
  int nTerm;                      /* Size of phrase in terms */
};

/*
** pStmt:
**   SELECT rowid, <fts> FROM <fts> ORDER BY +rank;
**
** aIdx[]:
**   There is one entry in the aIdx[] array for each phrase in the query,
**   the value of which is the offset within aPoslist[] following the last 
**   byte of the position list for the corresponding phrase.
*/
struct Fts5Sorter {
  int eStmt;
  sqlite3_stmt *pStmt;
  i64 iRowid;                     /* Current rowid */
  const u8 *aPoslist;             /* Position lists for current row */
  int nIdx;                       /* Number of entries in aIdx[] */
  int aIdx[0];                    /* Offsets into aPoslist for current row */
};


/*
** Virtual-table cursor object.
*/
struct Fts5Cursor {
  sqlite3_vtab_cursor base;       /* Base class used by SQLite core */
  int idxNum;                     /* idxNum passed to xFilter() */
  sqlite3_stmt *pStmt;            /* Statement used to read %_content */
  Fts5Expr *pExpr;                /* Expression for MATCH queries */
  Fts5Sorter *pSorter;            /* Sorter for "ORDER BY rank" queries */
  int csrflags;                   /* Mask of cursor flags (see below) */
  Fts5Cursor *pNext;              /* Next cursor in Fts5Cursor.pCsr list */
  Fts5Auxiliary *pRank;           /* Rank callback (or NULL) */

  /* Variables used by auxiliary functions */
  i64 iCsrId;                     /* Cursor id */
  Fts5Auxiliary *pAux;            /* Currently executing extension function */
  Fts5Auxdata *pAuxdata;          /* First in linked list of saved aux-data */
  int *aColumnSize;               /* Values for xColumnSize() */
};

/*
** Values for Fts5Cursor.csrflags
*/
#define FTS5CSR_REQUIRE_CONTENT   0x01
#define FTS5CSR_REQUIRE_DOCSIZE   0x02
#define FTS5CSR_EOF               0x04

/*
** Macros to Set(), Clear() and Test() cursor flags.
*/
#define CsrFlagSet(pCsr, flag)   ((pCsr)->csrflags |= (flag))
#define CsrFlagClear(pCsr, flag) ((pCsr)->csrflags &= ~(flag))
#define CsrFlagTest(pCsr, flag)  ((pCsr)->csrflags & (flag))

struct Fts5Auxdata {
  Fts5Auxiliary *pAux;            /* Extension to which this belongs */
  void *pPtr;                     /* Pointer value */
  void(*xDelete)(void*);          /* Destructor */
  Fts5Auxdata *pNext;             /* Next object in linked list */
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
      pTab->pGlobal = (Fts5Global*)pAux;
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
#define FTS5_PLAN_SCAN           1       /* No usable constraint */
#define FTS5_PLAN_MATCH          2       /* (<tbl> MATCH ?) */
#define FTS5_PLAN_SORTED_MATCH   3       /* (<tbl> MATCH ? ORDER BY rank) */
#define FTS5_PLAN_ROWID          4       /* (rowid = ?) */
#define FTS5_PLAN_SOURCE         5       /* A source cursor for SORTED_MATCH */

#define FTS5_PLAN(idxNum) ((idxNum) & 0x7)

#define FTS5_ORDER_DESC   8       /* ORDER BY rowid DESC */
#define FTS5_ORDER_ASC   16       /* ORDER BY rowid ASC */

/*
** Search the object passed as the first argument for a usable constraint
** on column iCol using operator eOp. If one is found, return its index in
** the pInfo->aConstraint[] array. If no such constraint is found, return
** a negative value.
*/
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

  if( pInfo->nOrderBy==1 ){
    int iSort = pInfo->aOrderBy[0].iColumn;
    if( iSort<0 ){
      /* ORDER BY rowid [ASC|DESC] */
      pInfo->orderByConsumed = 1;
    }else if( iSort==(pConfig->nCol+1) && ePlan==FTS5_PLAN_MATCH ){
      /* ORDER BY rank [ASC|DESC] */
      pInfo->orderByConsumed = 1;
      ePlan = FTS5_PLAN_SORTED_MATCH;
    }

    if( pInfo->orderByConsumed ){
      ePlan |= pInfo->aOrderBy[0].desc ? FTS5_ORDER_DESC : FTS5_ORDER_ASC;
    }
  }
   
  pInfo->idxNum = ePlan;
  return SQLITE_OK;
}

/*
** Implementation of xOpen method.
*/
static int fts5OpenMethod(sqlite3_vtab *pVTab, sqlite3_vtab_cursor **ppCsr){
  Fts5Table *pTab = (Fts5Table*)pVTab;
  Fts5Config *pConfig = pTab->pConfig;
  Fts5Cursor *pCsr;               /* New cursor object */
  int nByte;                      /* Bytes of space to allocate */
  int rc = SQLITE_OK;             /* Return code */

  nByte = sizeof(Fts5Cursor) + pConfig->nCol * sizeof(int);
  pCsr = (Fts5Cursor*)sqlite3_malloc(nByte);
  if( pCsr ){
    Fts5Global *pGlobal = pTab->pGlobal;
    memset(pCsr, 0, nByte);
    pCsr->aColumnSize = (int*)&pCsr[1];
    pCsr->pNext = pGlobal->pCsr;
    pGlobal->pCsr = pCsr;
    pCsr->iCsrId = ++pGlobal->iNextId;
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
  Fts5Cursor **pp;
  Fts5Auxdata *pData;
  Fts5Auxdata *pNext;

  if( pCsr->pStmt ){
    int eStmt = fts5StmtType(pCsr->idxNum);
    sqlite3Fts5StorageStmtRelease(pTab->pStorage, eStmt, pCsr->pStmt);
  }
  if( pCsr->pSorter ){
    Fts5Sorter *p = pCsr->pSorter;

    /* TODO: It would be better here to use sqlite3Fts5StorageStmtRelease() 
    ** so that the statement may be reused by subsequent queries. But that 
    ** is not possible as SQLite reference counts the virtual table objects.
    ** And since pStmt reads from this very virtual table, saving it here
    ** creates a circular reference.
    **
    ** We wouldn't worry so much if SQLite had a built-in statement cache.
    */
    /* sqlite3Fts5StorageStmtRelease(pTab->pStorage, p->eStmt, p->pStmt); */
    sqlite3_finalize(p->pStmt);
    sqlite3_free(p);
  }
  
  if( pCsr->idxNum!=FTS5_PLAN_SOURCE ){
    sqlite3Fts5ExprFree(pCsr->pExpr);
  }

  for(pData=pCsr->pAuxdata; pData; pData=pNext){
    pNext = pData->pNext;
    if( pData->xDelete ) pData->xDelete(pData->pPtr);
    sqlite3_free(pData);
  }

  /* Remove the cursor from the Fts5Global.pCsr list */
  for(pp=&pTab->pGlobal->pCsr; (*pp)!=pCsr; pp=&(*pp)->pNext);
  *pp = pCsr->pNext;

  sqlite3_free(pCsr);
  return SQLITE_OK;
}

static int fts5SorterNext(Fts5Cursor *pCsr){
  Fts5Sorter *pSorter = pCsr->pSorter;
  int rc;

  rc = sqlite3_step(pSorter->pStmt);
  if( rc==SQLITE_DONE ){
    rc = SQLITE_OK;
    CsrFlagSet(pCsr, FTS5CSR_EOF);
  }else if( rc==SQLITE_ROW ){
    const u8 *a;
    const u8 *aBlob;
    int nBlob;
    int i;
    int iOff = 0;
    rc = SQLITE_OK;

    pSorter->iRowid = sqlite3_column_int64(pSorter->pStmt, 0);
    nBlob = sqlite3_column_bytes(pSorter->pStmt, 1);
    aBlob = a = sqlite3_column_blob(pSorter->pStmt, 1);

    for(i=0; i<(pSorter->nIdx-1); i++){
      int iVal;
      a += getVarint32(a, iVal);
      iOff += iVal;
      pSorter->aIdx[i] = iOff;
    }
    pSorter->aIdx[i] = &aBlob[nBlob] - a;

    pSorter->aPoslist = a;
    CsrFlagSet(pCsr, FTS5CSR_REQUIRE_CONTENT | FTS5CSR_REQUIRE_DOCSIZE );
  }

  return rc;
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

  switch( ePlan ){
    case FTS5_PLAN_MATCH:
    case FTS5_PLAN_SOURCE:
      rc = sqlite3Fts5ExprNext(pCsr->pExpr);
      if( sqlite3Fts5ExprEof(pCsr->pExpr) ){
        CsrFlagSet(pCsr, FTS5CSR_EOF);
      }
      CsrFlagSet(pCsr, FTS5CSR_REQUIRE_CONTENT | FTS5CSR_REQUIRE_DOCSIZE );
      break;

    case FTS5_PLAN_SORTED_MATCH: {
      rc = fts5SorterNext(pCsr);
      break;
    }

    default:
      rc = sqlite3_step(pCsr->pStmt);
      if( rc!=SQLITE_ROW ){
        CsrFlagSet(pCsr, FTS5CSR_EOF);
        rc = sqlite3_reset(pCsr->pStmt);
      }else{
        rc = SQLITE_OK;
      }
      break;
  }
  
  return rc;
}

static int fts5CursorFirstSorted(Fts5Table *pTab, Fts5Cursor *pCsr, int bAsc){
  Fts5Config *pConfig = pTab->pConfig;
  Fts5Sorter *pSorter;
  int nPhrase;
  int nByte;
  int eStmt;
  int rc = SQLITE_OK;
  char *zSql;
  
  nPhrase = sqlite3Fts5ExprPhraseCount(pCsr->pExpr);
  nByte = sizeof(Fts5Sorter) + sizeof(int) * nPhrase;
  pSorter = (Fts5Sorter*)sqlite3_malloc(nByte);
  if( pSorter==0 ) return SQLITE_NOMEM;
  memset(pSorter, 0, nByte);
  pSorter->nIdx = nPhrase;

  assert( FTS5_STMT_SORTER_ASC==1+FTS5_STMT_SORTER_DESC );
  assert( bAsc==0 || bAsc==1 );

  pSorter->eStmt = FTS5_STMT_SORTER_DESC+bAsc;
  rc = sqlite3Fts5StorageStmt(pTab->pStorage, pSorter->eStmt, &pSorter->pStmt);

  pCsr->pSorter = pSorter;
  if( rc==SQLITE_OK ){
    assert( pTab->pSortCsr==0 );
    pTab->pSortCsr = pCsr;
    rc = fts5SorterNext(pCsr);
    pTab->pSortCsr = 0;
  }

  if( rc!=SQLITE_OK ){
    sqlite3_finalize(pSorter->pStmt);
    sqlite3_free(pSorter);
    pCsr->pSorter = 0;
  }

  return rc;
}

static int fts5CursorFirst(Fts5Table *pTab, Fts5Cursor *pCsr, int bAsc){
  int rc;
  rc = sqlite3Fts5ExprFirst(pCsr->pExpr, pTab->pIndex, bAsc);
  if( sqlite3Fts5ExprEof(pCsr->pExpr) ){
    CsrFlagSet(pCsr, FTS5CSR_EOF);
  }
  CsrFlagSet(pCsr, FTS5CSR_REQUIRE_CONTENT | FTS5CSR_REQUIRE_DOCSIZE );
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
  int bAsc = ((idxNum & FTS5_ORDER_ASC) ? 1 : 0);
  int rc = SQLITE_OK;

  assert( pCsr->pStmt==0 );
  assert( pCsr->pExpr==0 );
  assert( pCsr->csrflags==0 );
  assert( pCsr->pRank==0 );

  if( pTab->pSortCsr ){
    pCsr->idxNum = FTS5_PLAN_SOURCE;
    pCsr->pRank = pTab->pSortCsr->pRank;
    pCsr->pExpr = pTab->pSortCsr->pExpr;
    rc = fts5CursorFirst(pTab, pCsr, bAsc);
  }else{
    int ePlan = FTS5_PLAN(idxNum);
    int eStmt = fts5StmtType(idxNum);
    pCsr->idxNum = idxNum;
    rc = sqlite3Fts5StorageStmt(pTab->pStorage, eStmt, &pCsr->pStmt);
    if( rc==SQLITE_OK ){
      if( ePlan==FTS5_PLAN_MATCH || ePlan==FTS5_PLAN_SORTED_MATCH ){
        char **pzErr = &pTab->base.zErrMsg;
        const char *zExpr = (const char*)sqlite3_value_text(apVal[0]);
        pCsr->pRank = pTab->pGlobal->pAux;
        rc = sqlite3Fts5ExprNew(pTab->pConfig, zExpr, &pCsr->pExpr, pzErr);
        if( rc==SQLITE_OK ){
          if( ePlan==FTS5_PLAN_MATCH ){
            rc = fts5CursorFirst(pTab, pCsr, bAsc);
          }else{
            rc = fts5CursorFirstSorted(pTab, pCsr, bAsc);
          }
        }
      }else{
        if( ePlan==FTS5_PLAN_ROWID ){
          sqlite3_bind_value(pCsr->pStmt, 1, apVal[0]);
        }
        rc = fts5NextMethod(pCursor);
      }
    }
  }

  return rc;
}

/* 
** This is the xEof method of the virtual table. SQLite calls this 
** routine to find out if it has reached the end of a result set.
*/
static int fts5EofMethod(sqlite3_vtab_cursor *pCursor){
  Fts5Cursor *pCsr = (Fts5Cursor*)pCursor;
  return (CsrFlagTest(pCsr, FTS5CSR_EOF) ? 1 : 0);
}

/*
** Return the rowid that the cursor currently points to.
*/
static i64 fts5CursorRowid(Fts5Cursor *pCsr){
  assert( FTS5_PLAN(pCsr->idxNum)==FTS5_PLAN_MATCH 
       || FTS5_PLAN(pCsr->idxNum)==FTS5_PLAN_SORTED_MATCH 
       || FTS5_PLAN(pCsr->idxNum)==FTS5_PLAN_SOURCE 
  );
  if( pCsr->pSorter ){
    return pCsr->pSorter->iRowid;
  }else{
    return sqlite3Fts5ExprRowid(pCsr->pExpr);
  }
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
  
  assert( CsrFlagTest(pCsr, FTS5CSR_EOF)==0 );
  switch( ePlan ){
    case FTS5_PLAN_SOURCE:
    case FTS5_PLAN_MATCH:
    case FTS5_PLAN_SORTED_MATCH:
      *pRowid = fts5CursorRowid(pCsr);
      break;

    default:
      *pRowid = sqlite3_column_int64(pCsr->pStmt, 0);
      break;
  }

  return SQLITE_OK;
}

/*
** If the cursor requires seeking (bSeekRequired flag is set), seek it.
** Return SQLITE_OK if no error occurs, or an SQLite error code otherwise.
*/
static int fts5SeekCursor(Fts5Cursor *pCsr){
  int rc = SQLITE_OK;
  if( CsrFlagTest(pCsr, FTS5CSR_REQUIRE_CONTENT) ){
    assert( pCsr->pExpr );
    sqlite3_reset(pCsr->pStmt);
    sqlite3_bind_int64(pCsr->pStmt, 1, fts5CursorRowid(pCsr));
    rc = sqlite3_step(pCsr->pStmt);
    if( rc==SQLITE_ROW ){
      rc = SQLITE_OK;
      CsrFlagClear(pCsr, FTS5CSR_REQUIRE_CONTENT);
    }else{
      rc = sqlite3_reset(pCsr->pStmt);
      if( rc==SQLITE_OK ){
        rc = SQLITE_CORRUPT_VTAB;
      }
    }
  }
  return rc;
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

  /* A delete specifies a single argument - the rowid of the row to remove.
  ** Update and insert operations pass:
  **
  **   1. The "old" rowid, or NULL.
  **   2. The "new" rowid.
  **   3. Values for each of the nCol matchable columns.
  **   4. Values for the two hidden columns (<tablename> and "rank").
  */
  assert( nArg==1 || nArg==(2 + pConfig->nCol + 2) );

  if( nArg>1 && SQLITE_NULL!=sqlite3_value_type(apVal[2 + pConfig->nCol]) ){
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

static void *fts5ApiUserData(Fts5Context *pCtx){
  Fts5Cursor *pCsr = (Fts5Cursor*)pCtx;
  return pCsr->pAux->pUserData;
}

static int fts5ApiColumnCount(Fts5Context *pCtx){
  Fts5Cursor *pCsr = (Fts5Cursor*)pCtx;
  return ((Fts5Table*)(pCsr->base.pVtab))->pConfig->nCol;
}

static int fts5ApiColumnTotalSize(
  Fts5Context *pCtx, 
  int iCol, 
  sqlite3_int64 *pnToken
){
  Fts5Cursor *pCsr = (Fts5Cursor*)pCtx;
  Fts5Table *pTab = (Fts5Table*)(pCsr->base.pVtab);
  return sqlite3Fts5StorageSize(pTab->pStorage, iCol, pnToken);
}

static int fts5ApiRowCount(Fts5Context *pCtx, i64 *pnRow){
  Fts5Cursor *pCsr = (Fts5Cursor*)pCtx;
  Fts5Table *pTab = (Fts5Table*)(pCsr->base.pVtab);
  return sqlite3Fts5StorageRowCount(pTab->pStorage, pnRow);
}

static int fts5ApiTokenize(
  Fts5Context *pCtx, 
  const char *pText, int nText, 
  void *pUserData,
  int (*xToken)(void*, const char*, int, int, int, int)
){
  Fts5Cursor *pCsr = (Fts5Cursor*)pCtx;
  Fts5Table *pTab = (Fts5Table*)(pCsr->base.pVtab);
  return sqlite3Fts5Tokenize(pTab->pConfig, pText, nText, pUserData, xToken);
}

static int fts5ApiPhraseCount(Fts5Context *pCtx){
  Fts5Cursor *pCsr = (Fts5Cursor*)pCtx;
  return sqlite3Fts5ExprPhraseCount(pCsr->pExpr);
}

static int fts5ApiPhraseSize(Fts5Context *pCtx, int iPhrase){
  Fts5Cursor *pCsr = (Fts5Cursor*)pCtx;
  return sqlite3Fts5ExprPhraseSize(pCsr->pExpr, iPhrase);
}

static sqlite3_int64 fts5ApiRowid(Fts5Context *pCtx){
  return fts5CursorRowid((Fts5Cursor*)pCtx);
}

static int fts5ApiColumnText(
  Fts5Context *pCtx, 
  int iCol, 
  const char **pz, 
  int *pn
){
  Fts5Cursor *pCsr = (Fts5Cursor*)pCtx;
  int rc = fts5SeekCursor(pCsr);
  if( rc==SQLITE_OK ){
    *pz = (const char*)sqlite3_column_text(pCsr->pStmt, iCol+1);
    *pn = sqlite3_column_bytes(pCsr->pStmt, iCol+1);
  }
  return rc;
}

static int fts5ApiColumnSize(Fts5Context *pCtx, int iCol, int *pnToken){
  Fts5Cursor *pCsr = (Fts5Cursor*)pCtx;
  Fts5Table *pTab = (Fts5Table*)(pCsr->base.pVtab);
  int rc = SQLITE_OK;

  if( CsrFlagTest(pCsr, FTS5CSR_REQUIRE_DOCSIZE) ){
    i64 iRowid = fts5CursorRowid(pCsr);
    rc = sqlite3Fts5StorageDocsize(pTab->pStorage, iRowid, pCsr->aColumnSize);
  }
  if( iCol>=0 && iCol<pTab->pConfig->nCol ){
    *pnToken = pCsr->aColumnSize[iCol];
  }else{
    *pnToken = 0;
  }
  return rc;
}

static int fts5ApiPoslist(
  Fts5Context *pCtx, 
  int iPhrase, 
  int *pi, 
  i64 *piPos 
){
  Fts5Cursor *pCsr = (Fts5Cursor*)pCtx;
  const u8 *a; int n;             /* Poslist for phrase iPhrase */
  if( pCsr->pSorter ){
    Fts5Sorter *pSorter = pCsr->pSorter;
    int i1 = (iPhrase==0 ? 0 : pSorter->aIdx[iPhrase-1]);
    n = pSorter->aIdx[iPhrase] - i1;
    a = &pSorter->aPoslist[i1];
  }else{
    n = sqlite3Fts5ExprPoslist(pCsr->pExpr, iPhrase, &a);
  }
  return sqlite3Fts5PoslistNext64(a, n, pi, piPos);
}

static int fts5ApiSetAuxdata(
  Fts5Context *pCtx,              /* Fts5 context */
  void *pPtr,                     /* Pointer to save as auxdata */
  void(*xDelete)(void*)           /* Destructor for pPtr (or NULL) */
){
  Fts5Cursor *pCsr = (Fts5Cursor*)pCtx;
  Fts5Auxdata *pData;

  for(pData=pCsr->pAuxdata; pData; pData=pData->pNext){
    if( pData->pAux==pCsr->pAux ) break;
  }

  if( pData ){
    if( pData->xDelete ){
      pData->xDelete(pData->pPtr);
    }
  }else{
    pData = (Fts5Auxdata*)sqlite3_malloc(sizeof(Fts5Auxdata));
    if( pData==0 ) return SQLITE_NOMEM;
    memset(pData, 0, sizeof(Fts5Auxdata));
    pData->pAux = pCsr->pAux;
    pData->pNext = pCsr->pAuxdata;
    pCsr->pAuxdata = pData;
  }

  pData->xDelete = xDelete;
  pData->pPtr = pPtr;
  return SQLITE_OK;
}

static void *fts5ApiGetAuxdata(Fts5Context *pCtx, int bClear){
  Fts5Cursor *pCsr = (Fts5Cursor*)pCtx;
  Fts5Auxdata *pData;
  void *pRet = 0;

  for(pData=pCsr->pAuxdata; pData; pData=pData->pNext){
    if( pData->pAux==pCsr->pAux ) break;
  }

  if( pData ){
    pRet = pData->pPtr;
    if( bClear ){
      pData->pPtr = 0;
      pData->xDelete = 0;
    }
  }

  return pRet;
}

static int fts5ApiQueryPhrase(Fts5Context*, int, void*, 
    int(*)(const Fts5ExtensionApi*, Fts5Context*, void*)
);

static const Fts5ExtensionApi sFts5Api = {
  1,                            /* iVersion */
  fts5ApiUserData,
  fts5ApiColumnCount,
  fts5ApiRowCount,
  fts5ApiColumnTotalSize,
  fts5ApiTokenize,
  fts5ApiPhraseCount,
  fts5ApiPhraseSize,
  fts5ApiRowid,
  fts5ApiColumnText,
  fts5ApiColumnSize,
  fts5ApiPoslist,
  fts5ApiQueryPhrase,
  fts5ApiSetAuxdata,
  fts5ApiGetAuxdata,
};


/*
** Implementation of API function xQueryPhrase().
*/
static int fts5ApiQueryPhrase(
  Fts5Context *pCtx, 
  int iPhrase, 
  void *pUserData,
  int(*xCallback)(const Fts5ExtensionApi*, Fts5Context*, void*)
){
  Fts5Cursor *pCsr = (Fts5Cursor*)pCtx;
  Fts5Table *pTab = (Fts5Table*)(pCsr->base.pVtab);
  int rc;
  Fts5Cursor *pNew = 0;

  rc = fts5OpenMethod(pCsr->base.pVtab, (sqlite3_vtab_cursor**)&pNew);
  if( rc==SQLITE_OK ){
    Fts5Config *pConf = pTab->pConfig;
    pNew->idxNum = FTS5_PLAN_MATCH;
    pNew->base.pVtab = (sqlite3_vtab*)pTab;
    rc = sqlite3Fts5ExprPhraseExpr(pConf, pCsr->pExpr, iPhrase, &pNew->pExpr);
  }

  if( rc==SQLITE_OK ){
    for(rc = fts5CursorFirst(pTab, pNew, 0);
        rc==SQLITE_OK && CsrFlagTest(pNew, FTS5CSR_EOF)==0;
        rc = fts5NextMethod((sqlite3_vtab_cursor*)pNew)
    ){
      rc = xCallback(&sFts5Api, (Fts5Context*)pNew, pUserData);
      if( rc!=SQLITE_OK ){
        if( rc==SQLITE_DONE ) rc = SQLITE_OK;
        break;
      }
    }
  }

  fts5CloseMethod((sqlite3_vtab_cursor*)pNew);
  return rc;
}

static void fts5ApiInvoke(
  Fts5Auxiliary *pAux,
  Fts5Cursor *pCsr,
  sqlite3_context *context,
  int argc,
  sqlite3_value **argv
){
  assert( pCsr->pAux==0 );
  pCsr->pAux = pAux;
  pAux->xFunc(&sFts5Api, (Fts5Context*)pCsr, context, argc, argv);
  pCsr->pAux = 0;
}

static void fts5ApiCallback(
  sqlite3_context *context,
  int argc,
  sqlite3_value **argv
){

  Fts5Auxiliary *pAux;
  Fts5Cursor *pCsr;
  i64 iCsrId;

  assert( argc>=1 );
  pAux = (Fts5Auxiliary*)sqlite3_user_data(context);
  iCsrId = sqlite3_value_int64(argv[0]);

  for(pCsr=pAux->pGlobal->pCsr; pCsr; pCsr=pCsr->pNext){
    if( pCsr->iCsrId==iCsrId ) break;
  }
  if( pCsr==0 ){
    char *zErr = sqlite3_mprintf("no such cursor: %lld", iCsrId);
    sqlite3_result_error(context, zErr, -1);
  }else{
    fts5ApiInvoke(pAux, pCsr, context, argc-1, &argv[1]);
  }
}

/*
** Return a "position-list blob" corresponding to the current position of
** cursor pCsr via sqlite3_result_blob(). A position-list blob contains
** the current position-list for each phrase in the query associated with
** cursor pCsr.
**
** A position-list blob begins with (nPhrase-1) varints, where nPhrase is
** the number of phrases in the query. Following the varints are the
** concatenated position lists for each phrase, in order.
**
** The first varint (if it exists) contains the size of the position list
** for phrase 0. The second (same disclaimer) contains the size of position
** list 1. And so on. There is no size field for the final position list,
** as it can be derived from the total size of the blob.
*/
static int fts5PoslistBlob(sqlite3_context *pCtx, Fts5Cursor *pCsr){
  int i;
  int rc = SQLITE_OK;
  int nPhrase = sqlite3Fts5ExprPhraseCount(pCsr->pExpr);
  Fts5Buffer val;

  memset(&val, 0, sizeof(Fts5Buffer));

  /* Append the varints */
  for(i=0; i<(nPhrase-1); i++){
    const u8 *dummy;
    int nByte = sqlite3Fts5ExprPoslist(pCsr->pExpr, i, &dummy);
    sqlite3Fts5BufferAppendVarint(&rc, &val, nByte);
  }

  /* Append the position lists */
  for(i=0; i<nPhrase; i++){
    const u8 *pPoslist;
    int nPoslist;
    nPoslist = sqlite3Fts5ExprPoslist(pCsr->pExpr, i, &pPoslist);
    sqlite3Fts5BufferAppendBlob(&rc, &val, nPoslist, pPoslist);
  }

  sqlite3_result_blob(pCtx, val.p, val.n, sqlite3_free);
  return rc;
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
  Fts5Config *pConfig = ((Fts5Table*)(pCursor->pVtab))->pConfig;
  Fts5Cursor *pCsr = (Fts5Cursor*)pCursor;
  int rc = SQLITE_OK;
  
  assert( CsrFlagTest(pCsr, FTS5CSR_EOF)==0 );

  if( iCol==pConfig->nCol ){
    if( FTS5_PLAN(pCsr->idxNum)==FTS5_PLAN_SOURCE ){
      fts5PoslistBlob(pCtx, pCsr);
    }else{
      /* User is requesting the value of the special column with the same name
      ** as the table. Return the cursor integer id number. This value is only
      ** useful in that it may be passed as the first argument to an FTS5
      ** auxiliary function.  */
      sqlite3_result_int64(pCtx, pCsr->iCsrId);
    }
  }else if( iCol==pConfig->nCol+1 ){
    /* The value of the "rank" column. */
    if( pCsr->pRank ){
      fts5ApiInvoke(pCsr->pRank, pCsr, pCtx, 0, 0);
    }
  }else{
    rc = fts5SeekCursor(pCsr);
    if( rc==SQLITE_OK ){
      sqlite3_result_value(pCtx, sqlite3_column_value(pCsr->pStmt, iCol+1));
    }
  }
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
  void **ppArg                    /* OUT: User data for *pxFunc */
){
  Fts5Table *pTab = (Fts5Table*)pVtab;
  Fts5Auxiliary *pAux;

  for(pAux=pTab->pGlobal->pAux; pAux; pAux=pAux->pNext){
    if( sqlite3_stricmp(zName, pAux->zFunc)==0 ){
      *pxFunc = fts5ApiCallback;
      *ppArg = (void*)pAux;
      return 1;
    }
  }

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

/*
** Register a new auxiliary function with global context pGlobal.
*/
int sqlite3Fts5CreateAux(
  Fts5Global *pGlobal,            /* Global context (one per db handle) */
  const char *zName,              /* Name of new function */
  void *pUserData,                /* User data for aux. function */
  fts5_extension_function xFunc,  /* Aux. function implementation */
  void(*xDestroy)(void*)          /* Destructor for pUserData */
){
  int rc = sqlite3_overload_function(pGlobal->db, zName, -1);
  if( rc==SQLITE_OK ){
    Fts5Auxiliary *pAux;
    int nByte;                      /* Bytes of space to allocate */

    nByte = sizeof(Fts5Auxiliary) + strlen(zName) + 1;
    pAux = (Fts5Auxiliary*)sqlite3_malloc(nByte);
    if( pAux ){
      memset(pAux, 0, nByte);
      pAux->zFunc = (char*)&pAux[1];
      strcpy(pAux->zFunc, zName);
      pAux->pGlobal = pGlobal;
      pAux->pUserData = pUserData;
      pAux->xFunc = xFunc;
      pAux->xDestroy = xDestroy;
      pAux->pNext = pGlobal->pAux;
      pGlobal->pAux = pAux;
    }else{
      rc = SQLITE_NOMEM;
    }
  }

  return rc;
}

static void fts5ModuleDestroy(void *pCtx){
  Fts5Auxiliary *pAux;
  Fts5Auxiliary *pNext;
  Fts5Global *pGlobal = (Fts5Global*)pCtx;
  for(pAux=pGlobal->pAux; pAux; pAux=pNext){
    pNext = pAux->pNext;
    if( pAux->xDestroy ){
      pAux->xDestroy(pAux->pUserData);
    }
    sqlite3_free(pAux);
  }
  sqlite3_free(pGlobal);
}


int sqlite3Fts5Init(sqlite3 *db){
  static const sqlite3_module fts5Mod = {
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

  int rc;
  Fts5Global *pGlobal = 0;
  pGlobal = (Fts5Global*)sqlite3_malloc(sizeof(Fts5Global));

  if( pGlobal==0 ){
    rc = SQLITE_NOMEM;
  }else{
    void *p = (void*)pGlobal;
    memset(pGlobal, 0, sizeof(Fts5Global));
    pGlobal->db = db;
    rc = sqlite3_create_module_v2(db, "fts5", &fts5Mod, p, fts5ModuleDestroy);
    if( rc==SQLITE_OK ) rc = sqlite3Fts5IndexInit(db);
    if( rc==SQLITE_OK ) rc = sqlite3Fts5ExprInit(db);
    if( rc==SQLITE_OK ) rc = sqlite3Fts5AuxInit(pGlobal);
  }
  return rc;
}



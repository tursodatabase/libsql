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

#include "sqlite3expert.h"
#include <assert.h>
#include <string.h>
#include <stdio.h>

typedef sqlite3_int64 i64;
typedef sqlite3_uint64 u64;

typedef struct IdxConstraint IdxConstraint;
typedef struct IdxContext IdxContext;
typedef struct IdxScan IdxScan;
typedef struct IdxStatement IdxStatement;
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
**   a=? AND b=? AND c=? AND d=? AND e>? AND f<?
**
** The above is decomposed into 6 AND connected clauses. The first four are
** added to the IdxWhere.pEq linked list, the following two into 
** IdxWhere.pRange.
**
** IdxWhere.pEq and IdxWhere.pRange are simple linked lists of IdxConstraint
** objects linked by the IdxConstraint.pNext field.
*/
struct IdxWhere {
  IdxConstraint *pEq;             /* List of == constraints */
  IdxConstraint *pRange;          /* List of < constraints */
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
};

struct IdxStatement {
  int iId;                        /* Statement number */
  char *zSql;                     /* SQL statement */
  char *zIdx;                     /* Indexes */
  char *zEQP;                     /* Plan */
  IdxStatement *pNext;
};


#define IDX_HASH_SIZE 1023
typedef struct IdxHashEntry IdxHashEntry;
typedef struct IdxHash IdxHash;
struct IdxHashEntry {
  char *zKey;                     /* nul-terminated key */
  char *zVal;                     /* nul-terminated value string */
  IdxHashEntry *pHashNext;        /* Next entry in same hash bucket */
  IdxHashEntry *pNext;            /* Next entry in hash */
};
struct IdxHash {
  IdxHashEntry *pFirst;
  IdxHashEntry *aHash[IDX_HASH_SIZE];
};

/*
** sqlite3expert object.
*/
struct sqlite3expert {
  sqlite3 *db;                    /* Users database */
  sqlite3 *dbm;                   /* In-memory db for this analysis */

  int bRun;                       /* True once analysis has run */
  char **pzErrmsg;

  IdxScan *pScan;                 /* List of scan objects */
  IdxStatement *pStatement;       /* List of IdxStatement objects */
  int rc;                         /* Error code from whereinfo hook */

  IdxHash hIdx;                   /* Hash containing all candidate indexes */
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

/*************************************************************************
** Start of hash table implementations.
*/
typedef struct IdxHash64Entry IdxHash64Entry;
typedef struct IdxHash64 IdxHash64;
struct IdxHash64Entry {
  u64 iVal;
  IdxHash64Entry *pNext;          /* Next entry in hash table */
  IdxHash64Entry *pHashNext;      /* Next entry in same hash bucket */
};
struct IdxHash64 {
  IdxHash64Entry *pFirst;         /* Most recently added entry in hash table */
  IdxHash64Entry *aHash[IDX_HASH_SIZE];
};

static void idxHash64Init(IdxHash64 *pHash){
  memset(pHash, 0, sizeof(IdxHash64));
}
static void idxHash64Clear(IdxHash64 *pHash){
  IdxHash64Entry *pEntry;
  IdxHash64Entry *pNext;
  for(pEntry=pHash->pFirst; pEntry; pEntry=pNext){
    pNext = pEntry->pNext;
    sqlite3_free(pEntry);
  }
  memset(pHash, 0, sizeof(IdxHash64));
}
static void idxHash64Add(int *pRc, IdxHash64 *pHash, u64 iVal){
  int iHash = (int)((iVal*7) % IDX_HASH_SIZE);
  IdxHash64Entry *pEntry;
  assert( iHash>=0 );

  for(pEntry=pHash->aHash[iHash]; pEntry; pEntry=pEntry->pHashNext){
    if( pEntry->iVal==iVal ) return;
  }
  pEntry = idxMalloc(pRc, sizeof(IdxHash64Entry));
  if( pEntry ){
    pEntry->iVal = iVal;
    pEntry->pHashNext = pHash->aHash[iHash];
    pHash->aHash[iHash] = pEntry;
    pEntry->pNext = pHash->pFirst;
    pHash->pFirst = pEntry;
  }
}

static void idxHashInit(IdxHash *pHash){
  memset(pHash, 0, sizeof(IdxHash));
}
static void idxHashClear(IdxHash *pHash){
  int i;
  for(i=0; i<IDX_HASH_SIZE; i++){
    IdxHashEntry *pEntry;
    IdxHashEntry *pNext;
    for(pEntry=pHash->aHash[i]; pEntry; pEntry=pNext){
      pNext = pEntry->pHashNext;
      sqlite3_free(pEntry);
    }
  }
  memset(pHash, 0, sizeof(IdxHash));
}
static int idxHashString(const char *z, int n){
  unsigned int ret = 0;
  int i;
  for(i=0; i<n; i++){
    ret += (ret<<3) + (unsigned char)(z[i]);
  }
  return (int)(ret % IDX_HASH_SIZE);
}

static int idxHashAdd(
  int *pRc, 
  IdxHash *pHash, 
  const char *zKey,
  const char *zVal
){
  int nKey = strlen(zKey);
  int iHash = idxHashString(zKey, nKey);
  int nVal = (zVal ? strlen(zVal) : 0);
  IdxHashEntry *pEntry;
  assert( iHash>=0 );
  for(pEntry=pHash->aHash[iHash]; pEntry; pEntry=pEntry->pHashNext){
    if( strlen(pEntry->zKey)==nKey && 0==memcmp(pEntry->zKey, zKey, nKey) ){
      return 1;
    }
  }
  pEntry = idxMalloc(pRc, sizeof(IdxHashEntry) + nKey+1 + nVal+1);
  if( pEntry ){
    pEntry->zKey = (char*)&pEntry[1];
    memcpy(pEntry->zKey, zKey, nKey);
    if( zVal ){
      pEntry->zVal = &pEntry->zKey[nKey+1];
      memcpy(pEntry->zVal, zVal, nVal);
    }
    pEntry->pHashNext = pHash->aHash[iHash];
    pHash->aHash[iHash] = pEntry;

    pEntry->pNext = pHash->pFirst;
    pHash->pFirst = pEntry;
  }
  return 0;
}

static const char *idxHashSearch(IdxHash *pHash, const char *zKey, int nKey){
  int iHash;
  IdxHashEntry *pEntry;
  if( nKey<0 ) nKey = strlen(zKey);
  iHash = idxHashString(zKey, nKey);
  assert( iHash>=0 );
  for(pEntry=pHash->aHash[iHash]; pEntry; pEntry=pEntry->pHashNext){
    if( strlen(pEntry->zKey)==nKey && 0==memcmp(pEntry->zKey, zKey, nKey) ){
      return pEntry->zVal;
    }
  }
  return 0;
}

/*
** End of hash table implementations.
**************************************************************************/

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
** sqlite3_whereinfo_hook() callback.
*/
static void idxWhereInfo(
  void *pCtx,                     /* Pointer to IdxContext structure */
  int eOp, 
  const char *zVal, 
  int iVal, 
  u64 mask
){
  sqlite3expert *p = (sqlite3expert*)pCtx;

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
          pNew->pNext = p->pScan->where.pRange;
          p->pScan->where.pRange = pNew;
        }else{
          pNew->pNext = p->pScan->where.pEq;
          p->pScan->where.pEq = pNew;
        }
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

static void idxFinalize(int *pRc, sqlite3_stmt *pStmt){
  int rc = sqlite3_finalize(pStmt);
  if( *pRc==SQLITE_OK ) *pRc = rc;
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
    const char *zCol = (const char*)sqlite3_column_text(p1, 1);
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
    const char *zCol = (const char*)sqlite3_column_text(p1, 1);
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
  idxFinalize(&rc, p1);

  if( rc==SQLITE_OK ){
    pScan->pTable = pNew;
  }else{
    sqlite3_free(pNew);
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
  int rc;

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
    idxFinalize(&rc, pInfo);

    if( rc==SQLITE_OK && bMatch ){
      sqlite3_finalize(pIdxList);
      return 1;
    }
  }
  idxFinalize(&rc, pIdxList);

  *pRc = rc;
  return 0;
}

static int idxCreateFromCons(
  sqlite3expert *p,
  IdxScan *pScan,
  IdxConstraint *pEq, 
  IdxConstraint *pTail
){
  sqlite3 *dbm = p->dbm;
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
      char *zName;                /* Index name */
      int i;
      for(i=0; zCols[i]; i++){
        h += ((h<<3) + zCols[i]);
      }
      zName = sqlite3_mprintf("%s_idx_%08x", pScan->zTable, h);
      if( zName==0 ){ 
        rc = SQLITE_NOMEM;
      }else{
        if( idxIdentifierRequiresQuotes(pScan->zTable) ){
          zFmt = "CREATE INDEX '%q' ON %Q(%s)";
        }else{
          zFmt = "CREATE INDEX %s ON %s(%s)";
        }
        zIdx = sqlite3_mprintf(zFmt, zName, pScan->zTable, zCols);
        if( !zIdx ){
          rc = SQLITE_NOMEM;
        }else{
          rc = sqlite3_exec(dbm, zIdx, 0, 0, p->pzErrmsg);
          idxHashAdd(&rc, &p->hIdx, zName, zIdx);
        }
        sqlite3_free(zName);
        sqlite3_free(zIdx);
      }
    }

    sqlite3_free(zCols);
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
  sqlite3expert *p, 
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
  rc = idxCreateFromCons(p, pScan, p1, pTail);

  /* If no range/ORDER BY passed by the caller, create a version of the
  ** index for each range constraint that matches the mask. */
  if( pTail==0 ){
    for(pCon=pWhere->pRange; rc==SQLITE_OK && pCon; pCon=pCon->pNext){
      assert( pCon->pLink==0 );
      if( (mask & pCon->depmask)==pCon->depmask
        && idxFindConstraint(pEq, pCon)==0
        && idxFindConstraint(pTail, pCon)==0
      ){
        rc = idxCreateFromCons(p, pScan, p1, pCon);
      }
    }
  }

  return rc;
}

/*
** Create candidate indexes in database [dbm] based on the data in 
** linked-list pScan.
*/
static int idxCreateCandidates(sqlite3expert *p, char **pzErr){
  int rc = SQLITE_OK;
  IdxScan *pIter;
  IdxHash64 hMask;
  idxHash64Init(&hMask);

  for(pIter=p->pScan; pIter && rc==SQLITE_OK; pIter=pIter->pNextScan){
    IdxHash64Entry *pEntry;
    IdxWhere *pWhere = &pIter->where;
    IdxConstraint *pCons;

    idxHash64Add(&rc, &hMask, 0);
    for(pCons=pIter->where.pEq; pCons; pCons=pCons->pNext){
      for(pEntry=hMask.pFirst; pEntry; pEntry=pEntry->pNext){
        idxHash64Add(&rc, &hMask, pEntry->iVal | (u64)pCons->depmask);
      }
    }

    for(pEntry=hMask.pFirst; pEntry; pEntry=pEntry->pNext){
      i64 mask = (i64)pEntry->iVal;
      rc = idxCreateFromWhere(p, mask, pIter, pWhere, 0, 0);
      if( rc==SQLITE_OK && pIter->pOrder ){
        rc = idxCreateFromWhere(p, mask, pIter, pWhere, 0, pIter->pOrder);
      }
    }

    idxHash64Clear(&hMask);
  }

  return rc;
}

/*
** Free all elements of the linked list starting from pScan up until pLast
** (pLast is not freed).
*/
static void idxScanFree(IdxScan *pScan, IdxScan *pLast){
  /* TODO! */
}

/*
** Free all elements of the linked list starting from pStatement up 
** until pLast (pLast is not freed).
*/
static void idxStatementFree(IdxStatement *pStatement, IdxStatement *pLast){
  /* TODO! */
}


int idxFindIndexes(
  sqlite3expert *p,
  char **pzErr                         /* OUT: Error message (sqlite3_malloc) */
){
  IdxStatement *pStmt;
  sqlite3 *dbm = p->dbm;
  int rc = SQLITE_OK;

  IdxHash hIdx;
  idxHashInit(&hIdx);

  for(pStmt=p->pStatement; rc==SQLITE_OK && pStmt; pStmt=pStmt->pNext){
    IdxHashEntry *pEntry;
    sqlite3_stmt *pExplain = 0;
    idxHashClear(&hIdx);
    rc = idxPrintfPrepareStmt(dbm, &pExplain, pzErr,
        "EXPLAIN QUERY PLAN %s", pStmt->zSql
    );
    while( rc==SQLITE_OK && sqlite3_step(pExplain)==SQLITE_ROW ){
      int iSelectid = sqlite3_column_int(pExplain, 0);
      int iOrder = sqlite3_column_int(pExplain, 1);
      int iFrom = sqlite3_column_int(pExplain, 2);
      const char *zDetail = (const char*)sqlite3_column_text(pExplain, 3);
      int nDetail = strlen(zDetail);
      int i;

      for(i=0; i<nDetail; i++){
        const char *zIdx = 0;
        if( memcmp(&zDetail[i], " USING INDEX ", 13)==0 ){
          zIdx = &zDetail[i+13];
        }else if( memcmp(&zDetail[i], " USING COVERING INDEX ", 22)==0 ){
          zIdx = &zDetail[i+22];
        }
        if( zIdx ){
          const char *zSql;
          int nIdx = 0;
          while( zIdx[nIdx]!='\0' && (zIdx[nIdx]!=' ' || zIdx[nIdx+1]!='(') ){
            nIdx++;
          }
          zSql = idxHashSearch(&p->hIdx, zIdx, nIdx);
          if( zSql ){
            idxHashAdd(&rc, &hIdx, zSql, 0);
            if( rc ) goto find_indexes_out;
          }
          break;
        }
      }

      pStmt->zEQP = idxAppendText(&rc, pStmt->zEQP, "%d|%d|%d|%s\n", 
          iSelectid, iOrder, iFrom, zDetail
      );
    }

    for(pEntry=hIdx.pFirst; pEntry; pEntry=pEntry->pNext){
      pStmt->zIdx = idxAppendText(&rc, pStmt->zIdx, "%s\n", pEntry->zKey);
    }
    if( pStmt->zIdx==0 ){
      pStmt->zIdx = idxAppendText(&rc, 0, "(no new indexes)\n");
    }

    idxFinalize(&rc, pExplain);
  }

 find_indexes_out:
  return rc;
}

/*
** Allocate a new sqlite3expert object.
*/
sqlite3expert *sqlite3_expert_new(sqlite3 *db, char **pzErrmsg){
  int rc = SQLITE_OK;
  sqlite3expert *pNew;

  pNew = (sqlite3expert*)idxMalloc(&rc, sizeof(sqlite3expert));
  pNew->db = db;

  /* Open an in-memory database to work with. The main in-memory 
  ** database schema contains tables similar to those in the users 
  ** database (handle db).  */
  rc = sqlite3_open(":memory:", &pNew->dbm);

  /* Copy the entire schema of database [db] into [dbm]. */
  if( rc==SQLITE_OK ){
    sqlite3_stmt *pSql;
    rc = idxPrintfPrepareStmt(pNew->db, &pSql, pzErrmsg, 
        "SELECT sql FROM sqlite_master WHERE name NOT LIKE 'sqlite_%%'"
    );
    while( rc==SQLITE_OK && SQLITE_ROW==sqlite3_step(pSql) ){
      const char *zSql = (const char*)sqlite3_column_text(pSql, 0);
      rc = sqlite3_exec(pNew->dbm, zSql, 0, 0, pzErrmsg);
    }
    idxFinalize(&rc, pSql);
  }

  /* If an error has occurred, free the new object and reutrn NULL. Otherwise,
  ** return the new sqlite3expert handle.  */
  if( rc!=SQLITE_OK ){
    sqlite3_expert_destroy(pNew);
    pNew = 0;
  }
  return pNew;
}

/*
** Add an SQL statement to the analysis.
*/
int sqlite3_expert_sql(
  sqlite3expert *p,               /* From sqlite3_expert_new() */
  const char *zSql,               /* SQL statement to add */
  char **pzErr                    /* OUT: Error message (if any) */
){
  IdxScan *pScanOrig = p->pScan;
  IdxStatement *pStmtOrig = p->pStatement;
  int rc = SQLITE_OK;
  const char *zStmt = zSql;

  if( p->bRun ) return SQLITE_MISUSE;

  sqlite3_whereinfo_hook(p->db, idxWhereInfo, p);
  while( rc==SQLITE_OK && zStmt && zStmt[0] ){
    sqlite3_stmt *pStmt = 0;
    rc = sqlite3_prepare_v2(p->db, zStmt, -1, &pStmt, &zStmt);
    if( rc==SQLITE_OK ){
      if( pStmt ){
        IdxStatement *pNew;
        const char *z = sqlite3_sql(pStmt);
        int n = strlen(z);
        pNew = (IdxStatement*)idxMalloc(&rc, sizeof(IdxStatement) + n+1);
        if( rc==SQLITE_OK ){
          pNew->zSql = (char*)&pNew[1];
          memcpy(pNew->zSql, z, n+1);
          pNew->pNext = p->pStatement;
          if( p->pStatement ) pNew->iId = p->pStatement->iId+1;
          p->pStatement = pNew;
        }
        sqlite3_finalize(pStmt);
      }
    }else{
      idxDatabaseError(p->db, pzErr);
    }
  }
  sqlite3_whereinfo_hook(p->db, 0, 0);

  if( rc!=SQLITE_OK ){
    idxScanFree(p->pScan, pScanOrig);
    idxStatementFree(p->pStatement, pStmtOrig);
    p->pScan = pScanOrig;
    p->pStatement = pStmtOrig;
  }

  return rc;
}

int sqlite3_expert_analyze(sqlite3expert *p, char **pzErr){
  int rc = SQLITE_OK;
  IdxScan *pIter;

  /* Load IdxTable objects */
  for(pIter=p->pScan; pIter && rc==SQLITE_OK; pIter=pIter->pNextScan){
    rc = idxGetTableInfo(p->dbm, pIter, pzErr);
  }


  /* Create candidate indexes within the in-memory database file */
  if( rc==SQLITE_OK ){
    rc = idxCreateCandidates(p, pzErr);
  }

  /* Figure out which of the candidate indexes are preferred by the query
  ** planner and report the results to the user.  */
  if( rc==SQLITE_OK ){
    rc = idxFindIndexes(p, pzErr);
  }

  if( rc==SQLITE_OK ){
    p->bRun = 1;
  }
  return rc;
}

int sqlite3_expert_count(sqlite3expert *p){
  int nRet = 0;
  if( p->pStatement ) nRet = p->pStatement->iId+1;
  return nRet;
}

const char *sqlite3_expert_report(sqlite3expert *p, int iStmt, int eReport){
  const char *zRet = 0;
  IdxStatement *pStmt;

  if( p->bRun==0 ) return 0;
  for(pStmt=p->pStatement; pStmt && pStmt->iId!=iStmt; pStmt=pStmt->pNext);
  if( pStmt ){
    switch( eReport ){
      case EXPERT_REPORT_SQL:
        zRet = pStmt->zSql;
        break;
      case EXPERT_REPORT_INDEXES:
        zRet = pStmt->zIdx;
        break;
      case EXPERT_REPORT_PLAN:
        zRet = pStmt->zEQP;
        break;
    }
  }
  return zRet;
}

/*
** Free an sqlite3expert object.
*/
void sqlite3_expert_destroy(sqlite3expert *p){
  sqlite3_close(p->dbm);
  idxScanFree(p->pScan, 0);
  idxStatementFree(p->pStatement, 0);
  sqlite3_free(p);
}


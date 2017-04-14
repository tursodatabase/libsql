/*
** 2017 April 09
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

#if !defined(SQLITE_TEST) || defined(SQLITE_ENABLE_WHEREINFO_HOOK)

#include "sqlite3expert.h"
#include <assert.h>
#include <string.h>
#include <stdio.h>

typedef sqlite3_int64 i64;
typedef sqlite3_uint64 u64;

typedef struct IdxColumn IdxColumn;
typedef struct IdxConstraint IdxConstraint;
typedef struct IdxScan IdxScan;
typedef struct IdxStatement IdxStatement;
typedef struct IdxTable IdxTable;

/*
** A single constraint. Equivalent to either "col = ?" or "col < ?" (or
** any other type of single-ended range constraint on a column).
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
** A single scan of a single table.
*/
struct IdxScan {
  IdxTable *pTab;                 /* Associated table object */
  int iDb;                        /* Database containing table zTable */
  i64 covering;                   /* Mask of columns required for cov. index */
  IdxConstraint *pOrder;          /* ORDER BY columns */
  IdxConstraint *pEq;             /* List of == constraints */
  IdxConstraint *pRange;          /* List of < constraints */
  IdxScan *pNextScan;             /* Next IdxScan object for same analysis */
};

/*
** Information regarding a single database table. Extracted from 
** "PRAGMA table_info" by function idxGetTableInfo().
*/
struct IdxColumn {
  char *zName;
  char *zColl;
  int iPk;
};
struct IdxTable {
  int nCol;
  char *zName;                    /* Table name */
  IdxColumn *aCol;
  IdxTable *pNext;                /* Next table in linked list of all tables */
};

/*
** Each statement being analyzed is represented by an instance of this
** structure.
*/
struct IdxStatement {
  int iId;                        /* Statement number */
  char *zSql;                     /* SQL statement */
  char *zIdx;                     /* Indexes */
  char *zEQP;                     /* Plan */
  IdxStatement *pNext;
};


/*
** A hash table for storing strings. With space for a payload string
** with each entry. Methods are:
**
**   idxHashInit()
**   idxHashClear()
**   idxHashAdd()
**   idxHashSearch()
*/
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
** A hash table for storing a set of 64-bit values. Methods are:
**
**   idxHash64Init()
**   idxHash64Clear()
**   idxHash64Add()
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

/*
** sqlite3expert object.
*/
struct sqlite3expert {
  sqlite3 *db;                    /* User database */
  sqlite3 *dbm;                   /* In-memory db for this analysis */
  sqlite3 *dbv;                   /* Vtab schema for this analysis */
  IdxTable *pTable;               /* List of all IdxTable objects */

  IdxScan *pScan;                 /* List of scan objects */
  IdxStatement *pStatement;       /* List of IdxStatement objects */
  int bRun;                       /* True once analysis has run */
  char **pzErrmsg;
  int rc;                         /* Error code from whereinfo hook */
  IdxHash hIdx;                   /* Hash containing all candidate indexes */
  char *zCandidates;              /* For EXPERT_REPORT_CANDIDATES */
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
** Initialize an IdxHash64 hash table.
*/
static void idxHash64Init(IdxHash64 *pHash){
  memset(pHash, 0, sizeof(IdxHash64));
}

/*
** Reset an IdxHash64 hash table.
*/
static void idxHash64Clear(IdxHash64 *pHash){
  IdxHash64Entry *pEntry;
  IdxHash64Entry *pNext;
  for(pEntry=pHash->pFirst; pEntry; pEntry=pNext){
    pNext = pEntry->pNext;
    sqlite3_free(pEntry);
  }
  memset(pHash, 0, sizeof(IdxHash64));
}

/*
** Add iVal to the IdxHash64 hash table passed as the second argument. This
** function is a no-op if iVal is already present in the hash table.
*/
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

/*
** Initialize an IdxHash hash table.
*/
static void idxHashInit(IdxHash *pHash){
  memset(pHash, 0, sizeof(IdxHash));
}

/*
** Reset an IdxHash hash table.
*/
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

/*
** Return the index of the hash bucket that the string specified by the
** arguments to this function belongs.
*/
static int idxHashString(const char *z, int n){
  unsigned int ret = 0;
  int i;
  for(i=0; i<n; i++){
    ret += (ret<<3) + (unsigned char)(z[i]);
  }
  return (int)(ret % IDX_HASH_SIZE);
}

/*
** If zKey is already present in the hash table, return non-zero and do
** nothing. Otherwise, add an entry with key zKey and payload string zVal to
** the hash table passed as the second argument. 
*/
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

/*
** If the hash table contains an entry with a key equal to the string
** passed as the final two arguments to this function, return a pointer
** to the payload string. Otherwise, if zKey/nKey is not present in the
** hash table, return NULL.
*/
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


/*************************************************************************
** Beginning of virtual table implementation.
*/
typedef struct ExpertVtab ExpertVtab;
struct ExpertVtab {
  sqlite3_vtab base;
  IdxTable *pTab;
  sqlite3expert *pExpert;
};

static char *expertDequote(const char *zIn){
  int n = strlen(zIn);
  char *zRet = sqlite3_malloc(n);

  assert( zIn[0]=='\'' );
  assert( zIn[n-1]=='\'' );

  if( zRet ){
    int iOut = 0;
    int iIn = 0;
    for(iIn=1; iIn<(n-1); iIn++){
      if( zIn[iIn]=='\'' ){
        assert( zIn[iIn+1]=='\'' );
        iIn++;
      }
      zRet[iOut++] = zIn[iIn];
    }
    zRet[iOut] = '\0';
  }

  return zRet;
}

/* 
** This function is the implementation of both the xConnect and xCreate
** methods of the r-tree virtual table.
**
**   argv[0]   -> module name
**   argv[1]   -> database name
**   argv[2]   -> table name
**   argv[...] -> column names...
*/
static int expertConnect(
  sqlite3 *db,
  void *pAux,
  int argc, const char *const*argv,
  sqlite3_vtab **ppVtab,
  char **pzErr
){
  sqlite3expert *pExpert = (sqlite3expert*)pAux;
  ExpertVtab *p = 0;
  int rc;

  if( argc!=4 ){
    *pzErr = sqlite3_mprintf("internal error!");
    rc = SQLITE_ERROR;
  }else{
    char *zCreateTable = expertDequote(argv[3]);
    if( zCreateTable ){
      rc = sqlite3_declare_vtab(db, zCreateTable);
      if( rc==SQLITE_OK ){
        p = idxMalloc(&rc, sizeof(ExpertVtab));
      }
      if( rc==SQLITE_OK ){
        p->pExpert = pExpert;
        p->pTab = pExpert->pTable;
        assert( sqlite3_stricmp(p->pTab->zName, argv[2])==0 );
      }
      sqlite3_free(zCreateTable);
    }else{
      rc = SQLITE_NOMEM;
    }
  }

  *ppVtab = (sqlite3_vtab*)p;
  return rc;
}

static int expertDisconnect(sqlite3_vtab *pVtab){
  ExpertVtab *p = (ExpertVtab*)pVtab;
  sqlite3_free(p);
  return SQLITE_OK;
}

static int expertBestIndex(sqlite3_vtab *pVtab, sqlite3_index_info *pIdxInfo){
  ExpertVtab *p = (ExpertVtab*)pVtab;
  sqlite3 *dbv = p->pExpert->dbv;
  int rc = SQLITE_OK;
  int n = 0;
  IdxScan *pScan;
  const int opmask = 
    SQLITE_INDEX_CONSTRAINT_EQ | SQLITE_INDEX_CONSTRAINT_GT |
    SQLITE_INDEX_CONSTRAINT_LT | SQLITE_INDEX_CONSTRAINT_GE |
    SQLITE_INDEX_CONSTRAINT_LE;

  pScan = idxMalloc(&rc, sizeof(IdxScan));
  if( pScan ){
    int i;

    /* Link the new scan object into the list */
    pScan->pTab = p->pTab;
    pScan->pNextScan = p->pExpert->pScan;
    p->pExpert->pScan = pScan;

    /* Add the constraints to the IdxScan object */
    for(i=0; i<pIdxInfo->nConstraint; i++){
      int op = pIdxInfo->aConstraint[i].op;
      if( op&opmask ){
        IdxConstraint *pNew;
        const char *zColl = sqlite3_vtab_collation(dbv, i);
        pNew = idxNewConstraint(&rc, zColl);
        if( pNew ){
          pNew->iCol = pIdxInfo->aConstraint[i].iColumn;
          if( op==SQLITE_INDEX_CONSTRAINT_EQ ){
            pNew->pNext = pScan->pEq;
            pScan->pEq = pNew;
          }else{
            pNew->bRange = 1;
            pNew->pNext = pScan->pRange;
            pScan->pRange = pNew;
          }
        }
        if( pIdxInfo->aConstraint[i].usable ){
          n++;
          pIdxInfo->aConstraintUsage[i].argvIndex = n;
        }
      }
    }

    /* Add the ORDER BY to the IdxScan object */
    for(i=pIdxInfo->nOrderBy-1; i>=0; i--){
      IdxConstraint *pNew;
      const char *zColl = sqlite3_vtab_collation(dbv, i+pIdxInfo->nConstraint);
      pNew = idxNewConstraint(&rc, zColl);
      if( pNew ){
        pNew->iCol = pIdxInfo->aOrderBy[i].iColumn;
        pNew->bDesc = pIdxInfo->aOrderBy[i].desc;
        pNew->pNext = pScan->pOrder;
        pNew->pLink = pScan->pOrder;
        pScan->pOrder = pNew;
        n++;
      }
    }
  }

  pIdxInfo->estimatedCost = 1000000.0 / n;
  return rc;
}

static int idxRegisterVtab(sqlite3expert *p){
  static sqlite3_module expertModule = {
    2,                            /* iVersion */
    expertConnect,                /* xCreate - create a table */
    expertConnect,                /* xConnect - connect to an existing table */
    expertBestIndex,              /* xBestIndex - Determine search strategy */
    expertDisconnect,             /* xDisconnect - Disconnect from a table */
    expertDisconnect,             /* xDestroy - Drop a table */
    0,                            /* xOpen - open a cursor */
    0,                            /* xClose - close a cursor */
    0,                            /* xFilter - configure scan constraints */
    0,                            /* xNext - advance a cursor */
    0,                            /* xEof */
    0,                            /* xColumn - read data */
    0,                            /* xRowid - read data */
    0,                            /* xUpdate - write data */
    0,                            /* xBegin - begin transaction */
    0,                            /* xSync - sync transaction */
    0,                            /* xCommit - commit transaction */
    0,                            /* xRollback - rollback transaction */
    0,                            /* xFindFunction - function overloading */
    0,                            /* xRename - rename the table */
    0,                            /* xSavepoint */
    0,                            /* xRelease */
    0,                            /* xRollbackTo */
  };

  return sqlite3_create_module(p->dbv, "expert", &expertModule, (void*)p);
}
/*
** End of virtual table implementation.
*************************************************************************/

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

/*
** Prepare an SQL statement.
*/
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

/*
** Prepare an SQL statement using the results of a printf() formatting.
*/
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

/*
** Finalize SQL statement pStmt. If (*pRc) is SQLITE_OK when this function
** is called, set it to the return value of sqlite3_finalize() before
** returning. Otherwise, discard the sqlite3_finalize() return value.
*/
static void idxFinalize(int *pRc, sqlite3_stmt *pStmt){
  int rc = sqlite3_finalize(pStmt);
  if( *pRc==SQLITE_OK ) *pRc = rc;
}

/*
** Attempt to allocate an IdxTable structure corresponding to table zTab
** in the main database of connection db. If successful, set (*ppOut) to
** point to the new object and return SQLITE_OK. Otherwise, return an
** SQLite error code and set (*ppOut) to NULL. In this case *pzErrmsg may be
** set to point to an error string.
**
** It is the responsibility of the caller to eventually free either the
** IdxTable object or error message using sqlite3_free().
*/
static int idxGetTableInfo(
  sqlite3 *db,                    /* Database connection to read details from */
  const char *zTab,               /* Table name */
  IdxTable **ppOut,               /* OUT: New object (if successful) */
  char **pzErrmsg                 /* OUT: Error message (if not) */
){
  sqlite3_stmt *p1 = 0;
  int nCol = 0;
  int nTab = strlen(zTab);
  int nByte = sizeof(IdxTable) + nTab + 1;
  IdxTable *pNew = 0;
  int rc, rc2;
  char *pCsr;

  rc = idxPrintfPrepareStmt(db, &p1, pzErrmsg, "PRAGMA table_info=%Q", zTab);
  while( rc==SQLITE_OK && SQLITE_ROW==sqlite3_step(p1) ){
    const char *zCol = (const char*)sqlite3_column_text(p1, 1);
    nByte += 1 + strlen(zCol);
    rc = sqlite3_table_column_metadata(
        db, "main", zTab, zCol, 0, &zCol, 0, 0, 0
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
        db, "main", zTab, zCol, 0, &zCol, 0, 0, 0
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

  if( rc!=SQLITE_OK ){
    sqlite3_free(pNew);
    pNew = 0;
  }else{
    pNew->zName = pCsr;
    memcpy(pNew->zName, zTab, nTab+1);
  }

  *ppOut = pNew;
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

/*
** Return true if zId must be quoted in order to use it as an SQL
** identifier, or false otherwise.
*/
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

/*
** This function appends an index column definition suitable for constraint
** pCons to the string passed as zIn and returns the result.
*/
static char *idxAppendColDefn(
  int *pRc,                       /* IN/OUT: Error code */
  char *zIn,                      /* Column defn accumulated so far */
  IdxTable *pTab,                 /* Table index will be created on */
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
  const char *zTbl = pScan->pTab->zName;
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
  IdxTable *pTab,
  IdxScan *pScan,
  IdxConstraint *pEq, 
  IdxConstraint *pTail
){
  sqlite3 *dbm = p->dbm;
  int rc = SQLITE_OK;
  if( (pEq || pTail) && 0==idxFindCompatible(&rc, dbm, pScan, pEq, pTail) ){
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
      const char *zTable = pScan->pTab->zName;
      char *zName;                /* Index name */
      int i;
      for(i=0; zCols[i]; i++){
        h += ((h<<3) + zCols[i]);
      }
      zName = sqlite3_mprintf("%s_idx_%08x", zTable, h);
      if( zName==0 ){ 
        rc = SQLITE_NOMEM;
      }else{
        if( idxIdentifierRequiresQuotes(zTable) ){
          zFmt = "CREATE INDEX '%q' ON %Q(%s)";
        }else{
          zFmt = "CREATE INDEX %s ON %s(%s)";
        }
        zIdx = sqlite3_mprintf(zFmt, zName, zTable, zCols);
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
  IdxTable *pTab,
  i64 mask,                       /* Consider only these constraints */
  IdxScan *pScan,                 /* Create indexes for this scan */
  IdxConstraint *pEq,             /* == constraints for inclusion */
  IdxConstraint *pTail            /* range/ORDER BY constraints for inclusion */
){
  IdxConstraint *p1 = pEq;
  IdxConstraint *pCon;
  int rc;

  /* Gather up all the == constraints that match the mask. */
  for(pCon=pScan->pEq; pCon; pCon=pCon->pNext){
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
  rc = idxCreateFromCons(p, pTab, pScan, p1, pTail);

  /* If no range/ORDER BY passed by the caller, create a version of the
  ** index for each range constraint that matches the mask. */
  if( pTail==0 ){
    for(pCon=pScan->pRange; rc==SQLITE_OK && pCon; pCon=pCon->pNext){
      assert( pCon->pLink==0 );
      if( (mask & pCon->depmask)==pCon->depmask
        && idxFindConstraint(pEq, pCon)==0
        && idxFindConstraint(pTail, pCon)==0
      ){
        rc = idxCreateFromCons(p, pTab, pScan, p1, pCon);
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
    IdxConstraint *pCons;
    IdxTable *pTab = pIter->pTab;

    idxHash64Add(&rc, &hMask, 0);
    for(pCons=pIter->pEq; pCons; pCons=pCons->pNext){
      for(pEntry=hMask.pFirst; pEntry; pEntry=pEntry->pNext){
        idxHash64Add(&rc, &hMask, pEntry->iVal | (u64)pCons->depmask);
      }
    }

    for(pEntry=hMask.pFirst; rc==SQLITE_OK && pEntry; pEntry=pEntry->pNext){
      i64 mask = (i64)pEntry->iVal;
      rc = idxCreateFromWhere(p, pTab, mask, pIter, 0, 0);
      if( rc==SQLITE_OK && pIter->pOrder ){
        rc = idxCreateFromWhere(p, pTab, mask, pIter, 0, pIter->pOrder);
      }
    }

    idxHash64Clear(&hMask);
  }

  return rc;
}

/*
** Free all elements of the linked list starting at pConstraint.
*/
static void idxConstraintFree(IdxConstraint *pConstraint){
  IdxConstraint *pNext;
  IdxConstraint *p;

  for(p=pConstraint; p; p=pNext){
    pNext = p->pNext;
    sqlite3_free(p);
  }
}

/*
** Free all elements of the linked list starting from pScan up until pLast
** (pLast is not freed).
*/
static void idxScanFree(IdxScan *pScan, IdxScan *pLast){
  IdxScan *p;
  IdxScan *pNext;
  for(p=pScan; p!=pLast; p=pNext){
    pNext = p->pNextScan;
    idxConstraintFree(p->pOrder);
    idxConstraintFree(p->pEq);
    idxConstraintFree(p->pRange);
    sqlite3_free(p);
  }
}

/*
** Free all elements of the linked list starting from pStatement up 
** until pLast (pLast is not freed).
*/
static void idxStatementFree(IdxStatement *pStatement, IdxStatement *pLast){
  IdxStatement *p;
  IdxStatement *pNext;
  for(p=pStatement; p!=pLast; p=pNext){
    pNext = p->pNext;
    sqlite3_free(p->zEQP);
    sqlite3_free(p->zIdx);
    sqlite3_free(p);
  }
}


/*
** This function is called after candidate indexes have been created. It
** runs all the queries to see which indexes they prefer, and populates
** IdxStatement.zIdx and IdxStatement.zEQP with the results.
*/
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
      pStmt->zIdx = idxAppendText(&rc, pStmt->zIdx, "%s;\n", pEntry->zKey);
    }

    idxFinalize(&rc, pExplain);
  }

 find_indexes_out:
  idxHashClear(&hIdx);
  return rc;
}

static int idxCreateVtabSchema(sqlite3expert *p, char **pzErrmsg){
  int rc = idxRegisterVtab(p);
  sqlite3_stmt *pSchema = 0;

  /* For each table in the main db schema:
  **
  **   1) Add an entry to the p->pTable list, and
  **   2) Create the equivalent virtual table in dbv.
  */
  rc = idxPrepareStmt(p->db, &pSchema, pzErrmsg,
      "SELECT type, name, sql FROM sqlite_master "
      "WHERE type IN ('table','view') ORDER BY 1"
  );
  while( rc==SQLITE_OK && SQLITE_ROW==sqlite3_step(pSchema) ){
    const char *zType = (const char*)sqlite3_column_text(pSchema, 0);
    const char *zName = (const char*)sqlite3_column_text(pSchema, 1);
    const char *zSql = (const char*)sqlite3_column_text(pSchema, 2);

    if( zType[0]=='v' ){
      rc = sqlite3_exec(p->dbv, zSql, 0, 0, pzErrmsg);
    }else{
      IdxTable *pTab;
      rc = idxGetTableInfo(p->db, zName, &pTab, pzErrmsg);
      if( rc==SQLITE_OK ){
        int i;
        char *zInner = 0;
        char *zOuter = 0;
        pTab->pNext = p->pTable;
        p->pTable = pTab;

        /* The statement the vtab will pass to sqlite3_declare_vtab() */
        zInner = idxAppendText(&rc, 0, "CREATE TABLE x(");
        for(i=0; i<pTab->nCol; i++){
          zInner = idxAppendText(&rc, zInner, "%s%Q COLLATE %s", 
              (i==0 ? "" : ", "), pTab->aCol[i].zName, pTab->aCol[i].zColl
          );
        }
        zInner = idxAppendText(&rc, zInner, ")");

        /* The CVT statement to create the vtab */
        zOuter = idxAppendText(&rc, 0, 
            "CREATE VIRTUAL TABLE %Q USING expert(%Q)", zName, zInner
        );
        if( rc==SQLITE_OK ){
          rc = sqlite3_exec(p->dbv, zOuter, 0, 0, pzErrmsg);
        }
        sqlite3_free(zInner);
        sqlite3_free(zOuter);
      }
    }
  }
  return rc;
}

/*
** Allocate a new sqlite3expert object.
*/
sqlite3expert *sqlite3_expert_new(sqlite3 *db, char **pzErrmsg){
  int rc = SQLITE_OK;
  sqlite3expert *pNew;

  pNew = (sqlite3expert*)idxMalloc(&rc, sizeof(sqlite3expert));

  /* Open two in-memory databases to work with. The "vtab database" (dbv)
  ** will contain a virtual table corresponding to each real table in
  ** the user database schema, and a copy of each view. It is used to
  ** collect information regarding the WHERE, ORDER BY and other clauses
  ** of the user's query.
  */
  if( rc==SQLITE_OK ){
    pNew->db = db;
    rc = sqlite3_open(":memory:", &pNew->dbv);
  }
  if( rc==SQLITE_OK ){
    rc = sqlite3_open(":memory:", &pNew->dbm);
  }

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

  /* Create the vtab schema */
  if( rc==SQLITE_OK ){
    rc = idxCreateVtabSchema(pNew, pzErrmsg);
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

  while( rc==SQLITE_OK && zStmt && zStmt[0] ){
    sqlite3_stmt *pStmt = 0;
    rc = sqlite3_prepare_v2(p->dbv, zStmt, -1, &pStmt, &zStmt);
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

  if( rc!=SQLITE_OK ){
    idxScanFree(p->pScan, pScanOrig);
    idxStatementFree(p->pStatement, pStmtOrig);
    p->pScan = pScanOrig;
    p->pStatement = pStmtOrig;
  }

  return rc;
}

int sqlite3_expert_analyze(sqlite3expert *p, char **pzErr){
  int rc;
  IdxHashEntry *pEntry;

  /* Create candidate indexes within the in-memory database file */
  rc = idxCreateCandidates(p, pzErr);

  /* Formulate the EXPERT_REPORT_CANDIDATES text */
  for(pEntry=p->hIdx.pFirst; pEntry; pEntry=pEntry->pNext){
    p->zCandidates = idxAppendText(&rc, p->zCandidates, "%s;\n", pEntry->zVal);
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

/*
** Return the total number of statements that have been added to this
** sqlite3expert using sqlite3_expert_sql().
*/
int sqlite3_expert_count(sqlite3expert *p){
  int nRet = 0;
  if( p->pStatement ) nRet = p->pStatement->iId+1;
  return nRet;
}

/*
** Return a component of the report.
*/
const char *sqlite3_expert_report(sqlite3expert *p, int iStmt, int eReport){
  const char *zRet = 0;
  IdxStatement *pStmt;

  if( p->bRun==0 ) return 0;
  for(pStmt=p->pStatement; pStmt && pStmt->iId!=iStmt; pStmt=pStmt->pNext);
  switch( eReport ){
    case EXPERT_REPORT_SQL:
      if( pStmt ) zRet = pStmt->zSql;
      break;
    case EXPERT_REPORT_INDEXES:
      if( pStmt ) zRet = pStmt->zIdx;
      break;
    case EXPERT_REPORT_PLAN:
      if( pStmt ) zRet = pStmt->zEQP;
      break;
    case EXPERT_REPORT_CANDIDATES:
      zRet = p->zCandidates;
      break;
  }
  return zRet;
}

/*
** Free an sqlite3expert object.
*/
void sqlite3_expert_destroy(sqlite3expert *p){
  if( p ){
    sqlite3_close(p->dbm);
    sqlite3_close(p->dbv);
    idxScanFree(p->pScan, 0);
    idxStatementFree(p->pStatement, 0);
    idxHashClear(&p->hIdx);
    sqlite3_free(p);
  }
}

#endif /* !defined(SQLITE_TEST) || defined(SQLITE_ENABLE_WHEREINFO_HOOK) */


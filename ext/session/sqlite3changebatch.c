
#if !defined(SQLITE_TEST) || (defined(SQLITE_ENABLE_SESSION) && defined(SQLITE_ENABLE_PREUPDATE_HOOK))

#include "sqlite3session.h"
#include "sqlite3changebatch.h"

#include <assert.h>
#include <string.h>

typedef struct BatchTable BatchTable;
typedef struct BatchIndex BatchIndex;
typedef struct BatchIndexEntry BatchIndexEntry;
typedef struct BatchHash BatchHash;

struct sqlite3_changebatch {
  sqlite3 *db;                    /* Database handle used to read schema */
  BatchTable *pTab;               /* First in linked list of tables */
  int iChangesetId;               /* Current changeset id */
  int iNextIdxId;                 /* Next available index id */
  int nEntry;                     /* Number of entries in hash table */
  int nHash;                      /* Number of hash buckets */
  BatchIndexEntry **apHash;       /* Array of hash buckets */
};

struct BatchTable {
  BatchIndex *pIdx;               /* First in linked list of UNIQUE indexes */
  BatchTable *pNext;              /* Next table */
  char zTab[1];                   /* Table name */
};

struct BatchIndex {
  BatchIndex *pNext;              /* Next index on same table */
  int iId;                        /* Index id (assigned internally) */
  int bPk;                        /* True for PK index */
  int nCol;                       /* Size of aiCol[] array */
  int *aiCol;                     /* Array of columns that make up index */
};

struct BatchIndexEntry {
  BatchIndexEntry *pNext;         /* Next colliding hash table entry */
  int iChangesetId;               /* Id of associated changeset */
  int iIdxId;                     /* Id of index this key is from */
  int szRecord;
  char aRecord[1];
};

/*
** Allocate and zero a block of nByte bytes. Must be freed using cbFree().
*/
static void *cbMalloc(int *pRc, int nByte){
  void *pRet;

  if( *pRc ){
    pRet = 0;
  }else{
    pRet = sqlite3_malloc(nByte);
    if( pRet ){
      memset(pRet, 0, nByte);
    }else{
      *pRc = SQLITE_NOMEM;
    }
  }

  return pRet;
}

/*
** Free an allocation made by cbMalloc().
*/
static void cbFree(void *p){
  sqlite3_free(p);
}

/*
** Return the hash bucket that pEntry belongs in.
*/
static int cbHash(sqlite3_changebatch *p, BatchIndexEntry *pEntry){
  unsigned int iHash = (unsigned int)pEntry->iIdxId;
  unsigned char *pEnd = (unsigned char*)&pEntry->aRecord[pEntry->szRecord];
  unsigned char *pIter;

  for(pIter=(unsigned char*)pEntry->aRecord; pIter<pEnd; pIter++){
    iHash += (iHash << 7) + *pIter;
  }

  return (int)(iHash % p->nHash);
}

/*
** Resize the hash table.
*/
static int cbHashResize(sqlite3_changebatch *p){
  int rc = SQLITE_OK;
  BatchIndexEntry **apNew;
  int nNew = (p->nHash ? p->nHash*2 : 512);
  int i;

  apNew = cbMalloc(&rc, sizeof(BatchIndexEntry*) * nNew);
  if( rc==SQLITE_OK ){
    int nHash = p->nHash;
    p->nHash = nNew;
    for(i=0; i<nHash; i++){
      BatchIndexEntry *pEntry;
      while( (pEntry=p->apHash[i])!=0 ){
        int iHash = cbHash(p, pEntry);
        p->apHash[i] = pEntry->pNext;
        pEntry->pNext = apNew[iHash];
        apNew[iHash] = pEntry;
      }
    }

    cbFree(p->apHash);
    p->apHash = apNew;
  }

  return rc;
}


/*
** Allocate a new sqlite3_changebatch object.
*/
int sqlite3changebatch_new(sqlite3 *db, sqlite3_changebatch **pp){
  sqlite3_changebatch *pRet;
  int rc = SQLITE_OK;
  *pp = pRet = (sqlite3_changebatch*)cbMalloc(&rc, sizeof(sqlite3_changebatch));
  if( pRet ){
    pRet->db = db;
  }
  return rc;
}

/*
** Add a BatchIndex entry for index zIdx to table pTab.
*/
static int cbAddIndex(
  sqlite3_changebatch *p, 
  BatchTable *pTab, 
  const char *zIdx, 
  int bPk
){
  int nCol = 0;
  sqlite3_stmt *pIndexInfo = 0;
  BatchIndex *pNew = 0;
  int rc;
  char *zIndexInfo;

  zIndexInfo = (char*)sqlite3_mprintf("PRAGMA main.index_info = %Q", zIdx);
  if( zIndexInfo ){
    rc = sqlite3_prepare_v2(p->db, zIndexInfo, -1, &pIndexInfo, 0);
    sqlite3_free(zIndexInfo);
  }else{
    rc = SQLITE_NOMEM;
  }

  if( rc==SQLITE_OK ){
    while( SQLITE_ROW==sqlite3_step(pIndexInfo) ){ nCol++; }
    rc = sqlite3_reset(pIndexInfo);
  }

  pNew = (BatchIndex*)cbMalloc(&rc, sizeof(BatchIndex) + sizeof(int) * nCol);
  if( rc==SQLITE_OK ){
    pNew->nCol = nCol;
    pNew->bPk = bPk;
    pNew->aiCol = (int*)&pNew[1];
    pNew->iId = p->iNextIdxId++;
    while( SQLITE_ROW==sqlite3_step(pIndexInfo) ){ 
      int i = sqlite3_column_int(pIndexInfo, 0);
      int j = sqlite3_column_int(pIndexInfo, 1);
      pNew->aiCol[i] = j;
    }
    rc = sqlite3_reset(pIndexInfo);
  }

  if( rc==SQLITE_OK ){
    pNew->pNext = pTab->pIdx;
    pTab->pIdx = pNew;
  }else{
    cbFree(pNew);
  }
  sqlite3_finalize(pIndexInfo);

  return rc;
}

/*
** Free the object passed as the first argument.
*/
static void cbFreeTable(BatchTable *pTab){
  BatchIndex *pIdx;
  BatchIndex *pIdxNext;
  for(pIdx=pTab->pIdx; pIdx; pIdx=pIdxNext){
    pIdxNext = pIdx->pNext;
    cbFree(pIdx);
  }
  cbFree(pTab);
}

/*
** Find or create the BatchTable object named zTab.
*/
static int cbFindTable(
  sqlite3_changebatch *p, 
  const char *zTab, 
  BatchTable **ppTab
){
  BatchTable *pRet = 0;
  int rc = SQLITE_OK;

  for(pRet=p->pTab; pRet; pRet=pRet->pNext){
    if( 0==sqlite3_stricmp(zTab, pRet->zTab) ) break;
  }

  if( pRet==0 ){
    int nTab = strlen(zTab);
    pRet = (BatchTable*)cbMalloc(&rc, nTab + sizeof(BatchTable));
    if( pRet ){
      sqlite3_stmt *pIndexList = 0;
      char *zIndexList = 0;
      int rc2;
      memcpy(pRet->zTab, zTab, nTab);

      zIndexList = sqlite3_mprintf("PRAGMA main.index_list = %Q", zTab);
      if( zIndexList==0 ){
        rc = SQLITE_NOMEM;
      }else{
        rc = sqlite3_prepare_v2(p->db, zIndexList, -1, &pIndexList, 0);
        sqlite3_free(zIndexList);
      }

      while( rc==SQLITE_OK && SQLITE_ROW==sqlite3_step(pIndexList) ){
        if( sqlite3_column_int(pIndexList, 2) ){
          const char *zIdx = (const char*)sqlite3_column_text(pIndexList, 1);
          const char *zTyp = (const char*)sqlite3_column_text(pIndexList, 3);
          rc = cbAddIndex(p, pRet, zIdx, (zTyp[0]=='p'));
        }
      }
      rc2 = sqlite3_finalize(pIndexList);
      if( rc==SQLITE_OK ) rc = rc2;

      if( rc==SQLITE_OK ){
        pRet->pNext = p->pTab;
        p->pTab = pRet;
      }else{
        cbFreeTable(pRet);
        pRet = 0;
      }
    }
  }

  *ppTab = pRet;
  return rc;
}

/*
** Extract value iVal from the changeset iterator passed as the first
** argument. Set *ppVal to point to the value before returning.
**
** This function attempts to extract the value using function xVal
** (which is always either sqlite3changeset_new or sqlite3changeset_old).
** If the call returns SQLITE_OK but does not supply an sqlite3_value*
** pointer, an attempt to extract the value is made using the xFallback 
** function.
*/
static int cbGetChangesetValue(
  sqlite3_changeset_iter *pIter, 
  int (*xVal)(sqlite3_changeset_iter*,int,sqlite3_value**),
  int (*xFallback)(sqlite3_changeset_iter*,int,sqlite3_value**),
  int iVal,
  sqlite3_value **ppVal
){
  int rc = xVal(pIter, iVal, ppVal);
  if( rc==SQLITE_OK && *ppVal==0 && xFallback ){
    rc = xFallback(pIter, iVal, ppVal);
  }
  return rc;
}

static int cbAddToHash(
  sqlite3_changebatch *p, 
  sqlite3_changeset_iter *pIter, 
  BatchIndex *pIdx, 
  int (*xVal)(sqlite3_changeset_iter*,int,sqlite3_value**),
  int (*xFallback)(sqlite3_changeset_iter*,int,sqlite3_value**),
  int *pbConf
){
  BatchIndexEntry *pNew;
  int sz = pIdx->nCol;
  int i;
  int iOut = 0;
  int rc = SQLITE_OK;

  for(i=0; rc==SQLITE_OK && i<pIdx->nCol; i++){
    sqlite3_value *pVal;
    rc = cbGetChangesetValue(pIter, xVal, xFallback, pIdx->aiCol[i], &pVal);
    if( rc==SQLITE_OK ){
      int eType = 0;
      if( pVal ) eType = sqlite3_value_type(pVal);
      switch( eType ){
        case 0:
        case SQLITE_NULL:
          return SQLITE_OK;

        case SQLITE_INTEGER:
          sz += 8;
          break;
        case SQLITE_FLOAT:
          sz += 8;
          break;

        default:
          assert( eType==SQLITE_TEXT || eType==SQLITE_BLOB );
          sz += sqlite3_value_bytes(pVal);
          break;
      }
    }
  }

  pNew = cbMalloc(&rc, sizeof(BatchIndexEntry) + sz);
  if( pNew ){
    pNew->iChangesetId = p->iChangesetId;
    pNew->iIdxId = pIdx->iId;
    pNew->szRecord = sz;

    for(i=0; i<pIdx->nCol; i++){
      int eType;
      sqlite3_value *pVal;
      rc = cbGetChangesetValue(pIter, xVal, xFallback, pIdx->aiCol[i], &pVal);
      if( rc!=SQLITE_OK ) break;  /* coverage: condition is never true */
      eType = sqlite3_value_type(pVal);
      pNew->aRecord[iOut++] = eType;
      switch( eType ){
        case SQLITE_INTEGER: {
          sqlite3_int64 i64 = sqlite3_value_int64(pVal);
          memcpy(&pNew->aRecord[iOut], &i64, 8);
          iOut += 8;
          break;
        }
        case SQLITE_FLOAT: {
          double d64 = sqlite3_value_double(pVal);
          memcpy(&pNew->aRecord[iOut], &d64, sizeof(double));
          iOut += sizeof(double);
          break;
        }

        default: {
          int nByte = sqlite3_value_bytes(pVal);
          const char *z = (const char*)sqlite3_value_blob(pVal);
          memcpy(&pNew->aRecord[iOut], z, nByte);
          iOut += nByte;
          break;
        }
      }
    }
  }

  if( rc==SQLITE_OK && p->nEntry>=(p->nHash/2) ){
    rc = cbHashResize(p);
  }

  if( rc==SQLITE_OK ){
    BatchIndexEntry *pIter;
    int iHash = cbHash(p, pNew);

    assert( iHash>=0 && iHash<p->nHash );
    for(pIter=p->apHash[iHash]; pIter; pIter=pIter->pNext){
      if( pNew->szRecord==pIter->szRecord 
       && 0==memcmp(pNew->aRecord, pIter->aRecord, pNew->szRecord)
      ){
        if( pNew->iChangesetId!=pIter->iChangesetId ){
          *pbConf = 1;
        }
        cbFree(pNew);
        pNew = 0;
        break;
      }
    }

    if( pNew ){
      pNew->pNext = p->apHash[iHash];
      p->apHash[iHash] = pNew;
      p->nEntry++;
    }
  }else{
    cbFree(pNew);
  }

  return rc;
}


/*
** Add a changeset to the current batch.
*/
int sqlite3changebatch_add(sqlite3_changebatch *p, void *pBuf, int nBuf){
  sqlite3_changeset_iter *pIter;  /* Iterator opened on pBuf/nBuf */
  int rc;                         /* Return code */
  int bConf = 0;                  /* Conflict was detected */

  rc = sqlite3changeset_start(&pIter, nBuf, pBuf);
  if( rc==SQLITE_OK ){
    int rc2;
    for(rc2 = sqlite3changeset_next(pIter);
        rc2==SQLITE_ROW;
        rc2 = sqlite3changeset_next(pIter)
    ){
      BatchTable *pTab;
      BatchIndex *pIdx;
      const char *zTab;           /* Table this change applies to */
      int nCol;                   /* Number of columns in table */
      int op;                     /* UPDATE, INSERT or DELETE */

      sqlite3changeset_op(pIter, &zTab, &nCol, &op, 0);
      assert( op==SQLITE_INSERT || op==SQLITE_UPDATE || op==SQLITE_DELETE );

      rc = cbFindTable(p, zTab, &pTab);
      assert( pTab || rc!=SQLITE_OK );
      if( pTab ){
        for(pIdx=pTab->pIdx; pIdx && rc==SQLITE_OK; pIdx=pIdx->pNext){
          if( op==SQLITE_UPDATE && pIdx->bPk ) continue;
          if( op==SQLITE_UPDATE || op==SQLITE_DELETE ){
            rc = cbAddToHash(p, pIter, pIdx, sqlite3changeset_old, 0, &bConf);
          }
          if( op==SQLITE_UPDATE || op==SQLITE_INSERT ){
            rc = cbAddToHash(p, pIter, pIdx, 
                sqlite3changeset_new, sqlite3changeset_old, &bConf
            );
          }
        }
      }
      if( rc!=SQLITE_OK ) break;
    }

    rc2 = sqlite3changeset_finalize(pIter);
    if( rc==SQLITE_OK ) rc = rc2;
  }

  if( rc==SQLITE_OK && bConf ){
    rc = SQLITE_CONSTRAINT;
  }
  p->iChangesetId++;
  return rc;
}

/*
** Zero an existing changebatch object.
*/
void sqlite3changebatch_zero(sqlite3_changebatch *p){
  int i;
  for(i=0; i<p->nHash; i++){
    BatchIndexEntry *pEntry;
    BatchIndexEntry *pNext;
    for(pEntry=p->apHash[i]; pEntry; pEntry=pNext){
      pNext = pEntry->pNext;
      cbFree(pEntry);
    }
  }
  cbFree(p->apHash);
  p->nHash = 0;
  p->apHash = 0;
}

/*
** Delete a changebatch object.
*/
void sqlite3changebatch_delete(sqlite3_changebatch *p){
  BatchTable *pTab;
  BatchTable *pTabNext;

  sqlite3changebatch_zero(p);
  for(pTab=p->pTab; pTab; pTab=pTabNext){
    pTabNext = pTab->pNext;
    cbFreeTable(pTab);
  }
  cbFree(p);
}

/*
** Return the db handle.
*/
sqlite3 *sqlite3changebatch_db(sqlite3_changebatch *p){
  return p->db;
}

#endif /* SQLITE_ENABLE_SESSION && SQLITE_ENABLE_PREUPDATE_HOOK */

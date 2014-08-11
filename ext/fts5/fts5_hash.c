/*
** 2014 August 11
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

typedef struct Fts5HashEntry Fts5HashEntry;

/*
** This file contains the implementation of an in-memory hash table used
** to accumuluate "term -> doclist" content before it is flused to a level-0
** segment.
*/


struct Fts5Hash {
  int *pnByte;                    /* Pointer to bytes counter */
  int nEntry;                     /* Number of entries currently in hash */
  int nSlot;                      /* Size of aSlot[] array */
  Fts5HashEntry **aSlot;          /* Array of hash slots */
};

/*
** Each entry in the hash table is represented by an object of the 
** following type. Each object, its key (zKey[]) and its current data
** are stored in a single memory allocation. The position list data 
** immediately follows the key data in memory.
**
** The data that follows the key is in a similar, but not identical format
** to the doclist data stored in the database. It is:
**
**   * Rowid, as a varint
**   * Position list, without 0x00 terminator.
**   * Size of previous position list and rowid, as a 4 byte
**     big-endian integer.
**
** iRowidOff:
**   Offset of last rowid written to data area. Relative to first byte of
**   structure.
**
** nData:
**   Bytes of data written since iRowidOff.
*/
struct Fts5HashEntry {
  Fts5HashEntry *pNext;           /* Next hash entry with same hash-key */
  
  int nAlloc;                     /* Total size of allocation */
  int iRowidOff;                  /* Offset of last rowid written */
  int nData;                      /* Total bytes of data (incl. structure) */

  int iCol;                       /* Column of last value written */
  int iPos;                       /* Position of last value written */
  i64 iRowid;                     /* Rowid of last value written */
  char zKey[0];                   /* Nul-terminated entry key */
};


/*
** Allocate a new hash table.
*/
int sqlite3Fts5HashNew(Fts5Hash **ppNew, int *pnByte){
  int rc = SQLITE_OK;
  Fts5Hash *pNew;

  *ppNew = pNew = (Fts5Hash*)sqlite3_malloc(sizeof(Fts5Hash));
  if( pNew==0 ){
    rc = SQLITE_NOMEM;
  }else{
    int nByte;
    memset(pNew, 0, sizeof(Fts5Hash));
    pNew->pnByte = pnByte;

    pNew->nSlot = 1024;
    nByte = sizeof(Fts5HashEntry*) * pNew->nSlot;
    pNew->aSlot = (Fts5HashEntry**)sqlite3_malloc(nByte);
    if( pNew->aSlot==0 ){
      sqlite3_free(pNew);
      *ppNew = 0;
      rc = SQLITE_NOMEM;
    }else{
      memset(pNew->aSlot, 0, nByte);
    }
  }
  return rc;
}

/*
** Free a hash table object.
*/
void sqlite3Fts5HashFree(Fts5Hash *pHash){
  if( pHash ){
    sqlite3Fts5HashClear(pHash);
    sqlite3_free(pHash->aSlot);
    sqlite3_free(pHash);
  }
}

/*
** Empty (but do not delete) a hash table.
*/
void sqlite3Fts5HashClear(Fts5Hash *pHash){
  int i;
  for(i=0; i<pHash->nSlot; i++){
    if( pHash->aSlot[i] ){
      sqlite3_free(pHash->aSlot[i]);
      pHash->aSlot[i] = 0;
    }
  }
}

static unsigned int fts5HashKey(Fts5Hash *pHash, const char *p, int n){
  int i;
  unsigned int h = 13;
  for(i=n-1; i>=0; i--){
    h = (h << 3) ^ h ^ p[i];
  }
  return (h % pHash->nSlot);
}

/*
** Store the 32-bit integer passed as the second argument in buffer p.
*/
static int fts5PutNativeInt(u8 *p, int i){
  assert( sizeof(i)==4 );
  memcpy(p, &i, sizeof(i));
  return sizeof(i);
}

/*
** Read and return the 32-bit integer stored in buffer p.
*/
static int fts5GetNativeU32(u8 *p){
  int i;
  assert( sizeof(i)==4 );
  memcpy(&i, p, sizeof(i));
  return i;
}

int sqlite3Fts5HashWrite(
  Fts5Hash *pHash,
  i64 iRowid,                     /* Rowid for this entry */
  int iCol,                       /* Column token appears in (-ve -> delete) */
  int iPos,                       /* Position of token within column */
  const char *pToken, int nToken  /* Token to add or remove to or from index */
){
  unsigned int iHash = fts5HashKey(pHash, pToken, nToken);
  Fts5HashEntry *p;
  u8 *pPtr;
  int nIncr = 0;                  /* Amount to increment (*pHash->pnByte) by */

  /* Attempt to locate an existing hash object */
  for(p=pHash->aSlot[iHash]; p; p=p->pNext){
    if( memcmp(p->zKey, pToken, nToken)==0 && p->zKey[nToken]==0 ) break;
  }

  /* If an existing hash entry cannot be found, create a new one. */
  if( p==0 ){
    int nByte = sizeof(Fts5HashEntry) + nToken + 1 + 64;
    if( nByte<128 ) nByte = 128;

    p = (Fts5HashEntry*)sqlite3_malloc(nByte);
    if( !p ) return SQLITE_NOMEM;
    memset(p, 0, sizeof(Fts5HashEntry));
    p->nAlloc = nByte;
    memcpy(p->zKey, pToken, nToken);
    p->zKey[nToken] = '\0';
    p->iRowidOff = p->nData = nToken + 1 + sizeof(Fts5HashEntry);
    p->nData += sqlite3PutVarint(&((u8*)p)[p->nData], iRowid);
    p->iRowid = iRowid;
    p->pNext = pHash->aSlot[iHash];
    pHash->aSlot[iHash] = p;

    nIncr += p->nData;
  }

  /* Check there is enough space to append a new entry. Worst case scenario
  ** is:
  **
  **     + 4 bytes for the previous entry size field,
  **     + 9 bytes for a new rowid,
  **     + 1 byte for a "new column" byte,
  **     + 3 bytes for a new column number (16-bit max) as a varint,
  **     + 5 bytes for the new position offset (32-bit max).
  */
  if( (p->nAlloc - p->nData) < (4 + 9 + 1 + 3 + 5) ){
    int nNew = p->nAlloc * 2;
    Fts5HashEntry *pNew;
    Fts5HashEntry **pp;
    pNew = (Fts5HashEntry*)sqlite3_realloc(p, nNew);
    if( pNew==0 ) return SQLITE_NOMEM;
    pNew->nAlloc = nNew;
    for(pp=&pHash->aSlot[iHash]; *pp!=p; pp=&(*pp)->pNext);
    *pp = pNew;
    p = pNew;
  }
  pPtr = (u8*)p;
  nIncr -= p->nData;

  /* If this is a new rowid, append the 4-byte size field for the previous
  ** entry, and the new rowid for this entry.  */
  if( iRowid!=p->iRowid ){
    p->nData += fts5PutNativeInt(&pPtr[p->nData], p->nData - p->iRowidOff);
    p->iRowidOff = p->nData;
    p->nData += sqlite3PutVarint(&pPtr[p->nData], iRowid);
    p->iCol = 0;
    p->iPos = 0;
    p->iRowid = iRowid;
  }

  if( iCol>=0 ){
    /* Append a new column value, if necessary */
    assert( iCol>=p->iCol );
    if( iCol!=p->iCol ){
      pPtr[p->nData++] = 0x01;
      p->nData += sqlite3PutVarint(&pPtr[p->nData], iCol);
      p->iCol = iCol;
      p->iPos = 0;
    }

    /* Append the new position offset */
    p->nData += sqlite3PutVarint(&pPtr[p->nData], iPos - p->iPos + 2);
    p->iPos = iPos;
  }
  nIncr += p->nData;

  *pHash->pnByte += nIncr;
  return SQLITE_OK;
}


/*
** Arguments pLeft and pRight point to linked-lists of hash-entry objects,
** each sorted in key order. This function merges the two lists into a
** single list and returns a pointer to its first element.
*/
static Fts5HashEntry *fts5HashEntryMerge(
  Fts5HashEntry *pLeft,
  Fts5HashEntry *pRight
){
  Fts5HashEntry *p1 = pLeft;
  Fts5HashEntry *p2 = pRight;
  Fts5HashEntry *pRet = 0;
  Fts5HashEntry **ppOut = &pRet;

  while( p1 || p2 ){
    if( p1==0 ){
      *ppOut = p2;
      p2 = 0;
    }else if( p2==0 ){
      *ppOut = p1;
      p1 = 0;
    }else{
      int i = 0;
      while( p1->zKey[i]==p2->zKey[i] ) i++;

      if( ((u8)p1->zKey[i])>((u8)p2->zKey[i]) ){
        /* p2 is smaller */
        *ppOut = p2;
        ppOut = &p2->pNext;
        p2 = p2->pNext;
      }else{
        /* p1 is smaller */
        *ppOut = p1;
        ppOut = &p1->pNext;
        p1 = p1->pNext;
      }
      *ppOut = 0;
    }
  }

  return pRet;
}

/*
** Extract all tokens from hash table iHash and link them into a list
** in sorted order. The hash table is cleared before returning. It is
** the responsibility of the caller to free the elements of the returned
** list.
*/
static int fts5HashEntrySort(Fts5Hash *pHash, Fts5HashEntry **ppSorted){
  const int nMergeSlot = 32;
  Fts5HashEntry **ap;
  Fts5HashEntry *pList;
  int iSlot;
  int i;

  *ppSorted = 0;
  ap = sqlite3_malloc(sizeof(Fts5HashEntry*) * nMergeSlot);
  if( !ap ) return SQLITE_NOMEM;
  memset(ap, 0, sizeof(Fts5HashEntry*) * nMergeSlot);

  for(iSlot=0; iSlot<pHash->nSlot; iSlot++){
    while( pHash->aSlot[iSlot] ){
      Fts5HashEntry *pEntry = pHash->aSlot[iSlot];
      pHash->aSlot[iSlot] = pEntry->pNext;
      pEntry->pNext = 0;
      for(i=0; ap[i]; i++){
        pEntry = fts5HashEntryMerge(pEntry, ap[i]);
        ap[i] = 0;
      }
      ap[i] = pEntry;
    }
  }

  pList = 0;
  for(i=0; i<nMergeSlot; i++){
    pList = fts5HashEntryMerge(pList, ap[i]);
  }

  sqlite3_free(ap);
  *ppSorted = pList;
  return SQLITE_OK;
}

int sqlite3Fts5HashIterate(
  Fts5Hash *pHash,
  void *pCtx,
  int (*xTerm)(void*, const char*, int),
  int (*xEntry)(void*, i64, const u8*, int),
  int (*xTermDone)(void*)
){
  Fts5HashEntry *pList;
  int rc;

  rc = fts5HashEntrySort(pHash, &pList);
  if( rc==SQLITE_OK ){
    while( pList ){
      Fts5HashEntry *pNext = pList->pNext;
      if( rc==SQLITE_OK ){
        u8 *pPtr = (u8*)pList;
        int nKey = strlen(pList->zKey);
        int iOff = pList->iRowidOff;
        int iEnd = sizeof(Fts5HashEntry) + nKey + 1;
        int nByte = pList->nData - pList->iRowidOff;

        rc = xTerm(pCtx, pList->zKey, nKey);
        while( rc==SQLITE_OK && iOff ){
          int nVarint;
          i64 iRowid;
          nVarint = getVarint(&pPtr[iOff], (u64*)&iRowid);
          rc = xEntry(pCtx, iRowid, &pPtr[iOff+nVarint], nByte-nVarint);
          if( iOff==iEnd ){
            iOff = 0;
          }else{
            nByte = fts5GetNativeU32(&pPtr[iOff-sizeof(int)]);
            iOff = iOff - sizeof(int) - nByte;
          }
        }
        if( rc==SQLITE_OK ){
          rc = xTermDone(pCtx);
        }
      }
      sqlite3_free(pList);
      pList = pNext;
    }
  }
  return rc;
}




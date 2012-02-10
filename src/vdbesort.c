/*
** 2011 July 9
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
*************************************************************************
** This file contains code for the VdbeSorter object, used in concert with
** a VdbeCursor to sort large numbers of keys (as may be required, for
** example, by CREATE INDEX statements on tables too large to fit in main
** memory).
*/

#include "sqliteInt.h"
#include "vdbeInt.h"

#ifndef SQLITE_OMIT_MERGE_SORT

typedef struct VdbeSorterIter VdbeSorterIter;
typedef struct SorterRecord SorterRecord;

/*
** NOTES ON DATA STRUCTURE USED FOR N-WAY MERGES:
**
** As keys are added to the sorter, they are written to disk in a series
** of sorted packed-memory-arrays (PMAs). The size of each PMA is roughly
** the same as the cache-size allowed for temporary databases. In order
** to allow the caller to extract keys from the sorter in sorted order,
** all PMAs currently stored on disk must be merged together. This comment
** describes the data structure used to do so. The structure supports 
** merging any number of arrays in a single pass with no redundant comparison 
** operations.
**
** The aIter[] array contains an iterator for each of the PMAs being merged.
** An aIter[] iterator either points to a valid key or else is at EOF. For 
** the purposes of the paragraphs below, we assume that the array is actually 
** N elements in size, where N is the smallest power of 2 greater to or equal 
** to the number of iterators being merged. The extra aIter[] elements are 
** treated as if they are empty (always at EOF).
**
** The aTree[] array is also N elements in size. The value of N is stored in
** the VdbeSorter.nTree variable.
**
** The final (N/2) elements of aTree[] contain the results of comparing
** pairs of iterator keys together. Element i contains the result of 
** comparing aIter[2*i-N] and aIter[2*i-N+1]. Whichever key is smaller, the
** aTree element is set to the index of it. 
**
** For the purposes of this comparison, EOF is considered greater than any
** other key value. If the keys are equal (only possible with two EOF
** values), it doesn't matter which index is stored.
**
** The (N/4) elements of aTree[] that preceed the final (N/2) described 
** above contains the index of the smallest of each block of 4 iterators.
** And so on. So that aTree[1] contains the index of the iterator that 
** currently points to the smallest key value. aTree[0] is unused.
**
** Example:
**
**     aIter[0] -> Banana
**     aIter[1] -> Feijoa
**     aIter[2] -> Elderberry
**     aIter[3] -> Currant
**     aIter[4] -> Grapefruit
**     aIter[5] -> Apple
**     aIter[6] -> Durian
**     aIter[7] -> EOF
**
**     aTree[] = { X, 5   0, 5    0, 3, 5, 6 }
**
** The current element is "Apple" (the value of the key indicated by 
** iterator 5). When the Next() operation is invoked, iterator 5 will
** be advanced to the next key in its segment. Say the next key is
** "Eggplant":
**
**     aIter[5] -> Eggplant
**
** The contents of aTree[] are updated first by comparing the new iterator
** 5 key to the current key of iterator 4 (still "Grapefruit"). The iterator
** 5 value is still smaller, so aTree[6] is set to 5. And so on up the tree.
** The value of iterator 6 - "Durian" - is now smaller than that of iterator
** 5, so aTree[3] is set to 6. Key 0 is smaller than key 6 (Banana<Durian),
** so the value written into element 1 of the array is 0. As follows:
**
**     aTree[] = { X, 0   0, 6    0, 3, 5, 6 }
**
** In other words, each time we advance to the next sorter element, log2(N)
** key comparison operations are required, where N is the number of segments
** being merged (rounded up to the next power of 2).
*/
struct VdbeSorter {
  i64 iWriteOff;                  /* Current write offset within file pTemp1 */
  i64 iReadOff;                   /* Current read offset within file pTemp1 */
  int nInMemory;                  /* Current size of pRecord list as PMA */
  int nTree;                      /* Used size of aTree/aIter (power of 2) */
  int nPMA;                       /* Number of PMAs stored in pTemp1 */
  int mnPmaSize;                  /* Minimum PMA size, in bytes */
  int mxPmaSize;                  /* Maximum PMA size, in bytes.  0==no limit */
  VdbeSorterIter *aIter;          /* Array of iterators to merge */
  int *aTree;                     /* Current state of incremental merge */
  sqlite3_file *pTemp1;           /* PMA file 1 */
  SorterRecord *pRecord;          /* Head of in-memory record list */
  UnpackedRecord *pUnpacked;      /* Used to unpack keys */
};

/*
** The following type is an iterator for a PMA. It caches the current key in 
** variables nKey/aKey. If the iterator is at EOF, pFile==0.
*/
struct VdbeSorterIter {
  i64 iReadOff;                   /* Current read offset */
  i64 iEof;                       /* 1 byte past EOF for this iterator */
  int nAlloc;                     /* Bytes of space at aAlloc */
  int nKey;                       /* Number of bytes in key */
  sqlite3_file *pFile;            /* File iterator is reading from */
  u8 *aAlloc;                     /* Allocated space */
  u8 *aKey;                       /* Pointer to current key */
};

/*
** A structure to store a single record. All in-memory records are connected
** together into a linked list headed at VdbeSorter.pRecord using the 
** SorterRecord.pNext pointer.
*/
struct SorterRecord {
  void *pVal;
  int nVal;
  SorterRecord *pNext;
};

/* Minimum allowable value for the VdbeSorter.nWorking variable */
#define SORTER_MIN_WORKING 10

/* Maximum number of segments to merge in a single pass. */
#define SORTER_MAX_MERGE_COUNT 16

/*
** Free all memory belonging to the VdbeSorterIter object passed as the second
** argument. All structure fields are set to zero before returning.
*/
static void vdbeSorterIterZero(sqlite3 *db, VdbeSorterIter *pIter){
  sqlite3DbFree(db, pIter->aAlloc);
  memset(pIter, 0, sizeof(VdbeSorterIter));
}

/*
** Advance iterator pIter to the next key in its PMA. Return SQLITE_OK if
** no error occurs, or an SQLite error code if one does.
*/
static int vdbeSorterIterNext(
  sqlite3 *db,                    /* Database handle (for sqlite3DbMalloc() ) */
  VdbeSorterIter *pIter           /* Iterator to advance */
){
  int rc;                         /* Return Code */
  int nRead;                      /* Number of bytes read */
  int nRec = 0;                   /* Size of record in bytes */
  int iOff = 0;                   /* Size of serialized size varint in bytes */

  assert( pIter->iEof>=pIter->iReadOff );
  if( pIter->iEof-pIter->iReadOff>5 ){
    nRead = 5;
  }else{
    nRead = (int)(pIter->iEof - pIter->iReadOff);
  }
  if( nRead<=0 ){
    /* This is an EOF condition */
    vdbeSorterIterZero(db, pIter);
    return SQLITE_OK;
  }

  rc = sqlite3OsRead(pIter->pFile, pIter->aAlloc, nRead, pIter->iReadOff);
  if( rc==SQLITE_OK ){
    iOff = getVarint32(pIter->aAlloc, nRec);
    if( (iOff+nRec)>nRead ){
      int nRead2;                   /* Number of extra bytes to read */
      if( (iOff+nRec)>pIter->nAlloc ){
        int nNew = pIter->nAlloc*2;
        while( (iOff+nRec)>nNew ) nNew = nNew*2;
        pIter->aAlloc = sqlite3DbReallocOrFree(db, pIter->aAlloc, nNew);
        if( !pIter->aAlloc ) return SQLITE_NOMEM;
        pIter->nAlloc = nNew;
      }
  
      nRead2 = iOff + nRec - nRead;
      rc = sqlite3OsRead(
          pIter->pFile, &pIter->aAlloc[nRead], nRead2, pIter->iReadOff+nRead
      );
    }
  }

  assert( rc!=SQLITE_OK || nRec>0 );
  pIter->iReadOff += iOff+nRec;
  pIter->nKey = nRec;
  pIter->aKey = &pIter->aAlloc[iOff];
  return rc;
}

/*
** Write a single varint, value iVal, to file-descriptor pFile. Return
** SQLITE_OK if successful, or an SQLite error code if some error occurs.
**
** The value of *piOffset when this function is called is used as the byte
** offset in file pFile to write to. Before returning, *piOffset is 
** incremented by the number of bytes written.
*/
static int vdbeSorterWriteVarint(
  sqlite3_file *pFile,            /* File to write to */
  i64 iVal,                       /* Value to write as a varint */
  i64 *piOffset                   /* IN/OUT: Write offset in file pFile */
){
  u8 aVarint[9];                  /* Buffer large enough for a varint */
  int nVarint;                    /* Number of used bytes in varint */
  int rc;                         /* Result of write() call */

  nVarint = sqlite3PutVarint(aVarint, iVal);
  rc = sqlite3OsWrite(pFile, aVarint, nVarint, *piOffset);
  *piOffset += nVarint;

  return rc;
}

/*
** Read a single varint from file-descriptor pFile. Return SQLITE_OK if
** successful, or an SQLite error code if some error occurs.
**
** The value of *piOffset when this function is called is used as the
** byte offset in file pFile from whence to read the varint. If successful
** (i.e. if no IO error occurs), then *piOffset is set to the offset of
** the first byte past the end of the varint before returning. *piVal is
** set to the integer value read. If an error occurs, the final values of
** both *piOffset and *piVal are undefined.
*/
static int vdbeSorterReadVarint(
  sqlite3_file *pFile,            /* File to read from */
  i64 *piOffset,                  /* IN/OUT: Read offset in pFile */
  i64 *piVal                      /* OUT: Value read from file */
){
  u8 aVarint[9];                  /* Buffer large enough for a varint */
  i64 iOff = *piOffset;           /* Offset in file to read from */
  int rc;                         /* Return code */

  rc = sqlite3OsRead(pFile, aVarint, 9, iOff);
  if( rc==SQLITE_OK ){
    *piOffset += getVarint(aVarint, (u64 *)piVal);
  }

  return rc;
}

/*
** Initialize iterator pIter to scan through the PMA stored in file pFile
** starting at offset iStart and ending at offset iEof-1. This function 
** leaves the iterator pointing to the first key in the PMA (or EOF if the 
** PMA is empty).
*/
static int vdbeSorterIterInit(
  sqlite3 *db,                    /* Database handle */
  VdbeSorter *pSorter,            /* Sorter object */
  i64 iStart,                     /* Start offset in pFile */
  VdbeSorterIter *pIter,          /* Iterator to populate */
  i64 *pnByte                     /* IN/OUT: Increment this value by PMA size */
){
  int rc;

  assert( pSorter->iWriteOff>iStart );
  assert( pIter->aAlloc==0 );
  pIter->pFile = pSorter->pTemp1;
  pIter->iReadOff = iStart;
  pIter->nAlloc = 128;
  pIter->aAlloc = (u8 *)sqlite3DbMallocRaw(db, pIter->nAlloc);
  if( !pIter->aAlloc ){
    rc = SQLITE_NOMEM;
  }else{
    i64 nByte;                         /* Total size of PMA in bytes */
    rc = vdbeSorterReadVarint(pSorter->pTemp1, &pIter->iReadOff, &nByte);
    *pnByte += nByte;
    pIter->iEof = pIter->iReadOff + nByte;
  }
  if( rc==SQLITE_OK ){
    rc = vdbeSorterIterNext(db, pIter);
  }
  return rc;
}


/*
** Compare key1 (buffer pKey1, size nKey1 bytes) with key2 (buffer pKey2, 
** size nKey2 bytes).  Argument pKeyInfo supplies the collation functions
** used by the comparison. If an error occurs, return an SQLite error code.
** Otherwise, return SQLITE_OK and set *pRes to a negative, zero or positive
** value, depending on whether key1 is smaller, equal to or larger than key2.
**
** If the bOmitRowid argument is non-zero, assume both keys end in a rowid
** field. For the purposes of the comparison, ignore it. Also, if bOmitRowid
** is true and key1 contains even a single NULL value, it is considered to
** be less than key2. Even if key2 also contains NULL values.
**
** If pKey2 is passed a NULL pointer, then it is assumed that the pCsr->aSpace
** has been allocated and contains an unpacked record that is used as key2.
*/
static void vdbeSorterCompare(
  VdbeCursor *pCsr,               /* Cursor object (for pKeyInfo) */
  int bOmitRowid,                 /* Ignore rowid field at end of keys */
  void *pKey1, int nKey1,         /* Left side of comparison */
  void *pKey2, int nKey2,         /* Right side of comparison */
  int *pRes                       /* OUT: Result of comparison */
){
  KeyInfo *pKeyInfo = pCsr->pKeyInfo;
  VdbeSorter *pSorter = pCsr->pSorter;
  UnpackedRecord *r2 = pSorter->pUnpacked;
  int i;

  if( pKey2 ){
    sqlite3VdbeRecordUnpack(pKeyInfo, nKey2, pKey2, r2);
  }

  if( bOmitRowid ){
    r2->nField = pKeyInfo->nField;
    assert( r2->nField>0 );
    for(i=0; i<r2->nField; i++){
      if( r2->aMem[i].flags & MEM_Null ){
        *pRes = -1;
        return;
      }
    }
    r2->flags |= UNPACKED_PREFIX_MATCH;
  }

  *pRes = sqlite3VdbeRecordCompare(nKey1, pKey1, r2);
}

/*
** This function is called to compare two iterator keys when merging 
** multiple b-tree segments. Parameter iOut is the index of the aTree[] 
** value to recalculate.
*/
static int vdbeSorterDoCompare(VdbeCursor *pCsr, int iOut){
  VdbeSorter *pSorter = pCsr->pSorter;
  int i1;
  int i2;
  int iRes;
  VdbeSorterIter *p1;
  VdbeSorterIter *p2;

  assert( iOut<pSorter->nTree && iOut>0 );

  if( iOut>=(pSorter->nTree/2) ){
    i1 = (iOut - pSorter->nTree/2) * 2;
    i2 = i1 + 1;
  }else{
    i1 = pSorter->aTree[iOut*2];
    i2 = pSorter->aTree[iOut*2+1];
  }

  p1 = &pSorter->aIter[i1];
  p2 = &pSorter->aIter[i2];

  if( p1->pFile==0 ){
    iRes = i2;
  }else if( p2->pFile==0 ){
    iRes = i1;
  }else{
    int res;
    assert( pCsr->pSorter->pUnpacked!=0 );  /* allocated in vdbeSorterMerge() */
    vdbeSorterCompare(
        pCsr, 0, p1->aKey, p1->nKey, p2->aKey, p2->nKey, &res
    );
    if( res<=0 ){
      iRes = i1;
    }else{
      iRes = i2;
    }
  }

  pSorter->aTree[iOut] = iRes;
  return SQLITE_OK;
}

/*
** Initialize the temporary index cursor just opened as a sorter cursor.
*/
int sqlite3VdbeSorterInit(sqlite3 *db, VdbeCursor *pCsr){
  int pgsz;                       /* Page size of main database */
  int mxCache;                    /* Cache size */
  VdbeSorter *pSorter;            /* The new sorter */
  char *d;                        /* Dummy */

  assert( pCsr->pKeyInfo && pCsr->pBt==0 );
  pCsr->pSorter = pSorter = sqlite3DbMallocZero(db, sizeof(VdbeSorter));
  if( pSorter==0 ){
    return SQLITE_NOMEM;
  }
  
  pSorter->pUnpacked = sqlite3VdbeAllocUnpackedRecord(pCsr->pKeyInfo, 0, 0, &d);
  if( pSorter->pUnpacked==0 ) return SQLITE_NOMEM;
  assert( pSorter->pUnpacked==(UnpackedRecord *)d );

  if( !sqlite3TempInMemory(db) ){
    pgsz = sqlite3BtreeGetPageSize(db->aDb[0].pBt);
    pSorter->mnPmaSize = SORTER_MIN_WORKING * pgsz;
    mxCache = db->aDb[0].pSchema->cache_size;
    if( mxCache<SORTER_MIN_WORKING ) mxCache = SORTER_MIN_WORKING;
    pSorter->mxPmaSize = mxCache * pgsz;
  }

  return SQLITE_OK;
}

/*
** Free the list of sorted records starting at pRecord.
*/
static void vdbeSorterRecordFree(sqlite3 *db, SorterRecord *pRecord){
  SorterRecord *p;
  SorterRecord *pNext;
  for(p=pRecord; p; p=pNext){
    pNext = p->pNext;
    sqlite3DbFree(db, p);
  }
}

/*
** Free any cursor components allocated by sqlite3VdbeSorterXXX routines.
*/
void sqlite3VdbeSorterClose(sqlite3 *db, VdbeCursor *pCsr){
  VdbeSorter *pSorter = pCsr->pSorter;
  if( pSorter ){
    if( pSorter->aIter ){
      int i;
      for(i=0; i<pSorter->nTree; i++){
        vdbeSorterIterZero(db, &pSorter->aIter[i]);
      }
      sqlite3DbFree(db, pSorter->aIter);
    }
    if( pSorter->pTemp1 ){
      sqlite3OsCloseFree(pSorter->pTemp1);
    }
    vdbeSorterRecordFree(db, pSorter->pRecord);
    sqlite3DbFree(db, pSorter->pUnpacked);
    sqlite3DbFree(db, pSorter);
    pCsr->pSorter = 0;
  }
}

/*
** Allocate space for a file-handle and open a temporary file. If successful,
** set *ppFile to point to the malloc'd file-handle and return SQLITE_OK.
** Otherwise, set *ppFile to 0 and return an SQLite error code.
*/
static int vdbeSorterOpenTempFile(sqlite3 *db, sqlite3_file **ppFile){
  int dummy;
  return sqlite3OsOpenMalloc(db->pVfs, 0, ppFile,
      SQLITE_OPEN_TEMP_JOURNAL |
      SQLITE_OPEN_READWRITE    | SQLITE_OPEN_CREATE |
      SQLITE_OPEN_EXCLUSIVE    | SQLITE_OPEN_DELETEONCLOSE, &dummy
  );
}

/*
** Merge the two sorted lists p1 and p2 into a single list.
** Set *ppOut to the head of the new list.
*/
static void vdbeSorterMerge(
  VdbeCursor *pCsr,               /* For pKeyInfo */
  SorterRecord *p1,               /* First list to merge */
  SorterRecord *p2,               /* Second list to merge */
  SorterRecord **ppOut            /* OUT: Head of merged list */
){
  SorterRecord *pFinal = 0;
  SorterRecord **pp = &pFinal;
  void *pVal2 = p2 ? p2->pVal : 0;

  while( p1 && p2 ){
    int res;
    vdbeSorterCompare(pCsr, 0, p1->pVal, p1->nVal, pVal2, p2->nVal, &res);
    if( res<=0 ){
      *pp = p1;
      pp = &p1->pNext;
      p1 = p1->pNext;
      pVal2 = 0;
    }else{
      *pp = p2;
       pp = &p2->pNext;
      p2 = p2->pNext;
      if( p2==0 ) break;
      pVal2 = p2->pVal;
    }
  }
  *pp = p1 ? p1 : p2;
  *ppOut = pFinal;
}

/*
** Sort the linked list of records headed at pCsr->pRecord. Return SQLITE_OK
** if successful, or an SQLite error code (i.e. SQLITE_NOMEM) if an error
** occurs.
*/
static int vdbeSorterSort(VdbeCursor *pCsr){
  int i;
  SorterRecord **aSlot;
  SorterRecord *p;
  VdbeSorter *pSorter = pCsr->pSorter;

  aSlot = (SorterRecord **)sqlite3MallocZero(64 * sizeof(SorterRecord *));
  if( !aSlot ){
    return SQLITE_NOMEM;
  }

  p = pSorter->pRecord;
  while( p ){
    SorterRecord *pNext = p->pNext;
    p->pNext = 0;
    for(i=0; aSlot[i]; i++){
      vdbeSorterMerge(pCsr, p, aSlot[i], &p);
      aSlot[i] = 0;
    }
    aSlot[i] = p;
    p = pNext;
  }

  p = 0;
  for(i=0; i<64; i++){
    vdbeSorterMerge(pCsr, p, aSlot[i], &p);
  }
  pSorter->pRecord = p;

  sqlite3_free(aSlot);
  return SQLITE_OK;
}


/*
** Write the current contents of the in-memory linked-list to a PMA. Return
** SQLITE_OK if successful, or an SQLite error code otherwise.
**
** The format of a PMA is:
**
**     * A varint. This varint contains the total number of bytes of content
**       in the PMA (not including the varint itself).
**
**     * One or more records packed end-to-end in order of ascending keys. 
**       Each record consists of a varint followed by a blob of data (the 
**       key). The varint is the number of bytes in the blob of data.
*/
static int vdbeSorterListToPMA(sqlite3 *db, VdbeCursor *pCsr){
  int rc = SQLITE_OK;             /* Return code */
  VdbeSorter *pSorter = pCsr->pSorter;

  if( pSorter->nInMemory==0 ){
    assert( pSorter->pRecord==0 );
    return rc;
  }

  rc = vdbeSorterSort(pCsr);

  /* If the first temporary PMA file has not been opened, open it now. */
  if( rc==SQLITE_OK && pSorter->pTemp1==0 ){
    rc = vdbeSorterOpenTempFile(db, &pSorter->pTemp1);
    assert( rc!=SQLITE_OK || pSorter->pTemp1 );
    assert( pSorter->iWriteOff==0 );
    assert( pSorter->nPMA==0 );
  }

  if( rc==SQLITE_OK ){
    i64 iOff = pSorter->iWriteOff;
    SorterRecord *p;
    SorterRecord *pNext = 0;
    static const char eightZeros[8] = { 0, 0, 0, 0, 0, 0, 0, 0 };

    pSorter->nPMA++;
    rc = vdbeSorterWriteVarint(pSorter->pTemp1, pSorter->nInMemory, &iOff);
    for(p=pSorter->pRecord; rc==SQLITE_OK && p; p=pNext){
      pNext = p->pNext;
      rc = vdbeSorterWriteVarint(pSorter->pTemp1, p->nVal, &iOff);

      if( rc==SQLITE_OK ){
        rc = sqlite3OsWrite(pSorter->pTemp1, p->pVal, p->nVal, iOff);
        iOff += p->nVal;
      }

      sqlite3DbFree(db, p);
    }

    /* This assert verifies that unless an error has occurred, the size of 
    ** the PMA on disk is the same as the expected size stored in
    ** pSorter->nInMemory. */ 
    assert( rc!=SQLITE_OK || pSorter->nInMemory==(
          iOff-pSorter->iWriteOff-sqlite3VarintLen(pSorter->nInMemory)
    ));

    pSorter->iWriteOff = iOff;
    if( rc==SQLITE_OK ){
      /* Terminate each file with 8 extra bytes so that from any offset
      ** in the file we can always read 9 bytes without a SHORT_READ error */
      rc = sqlite3OsWrite(pSorter->pTemp1, eightZeros, 8, iOff);
    }
    pSorter->pRecord = p;
  }

  return rc;
}

/*
** Add a record to the sorter.
*/
int sqlite3VdbeSorterWrite(
  sqlite3 *db,                    /* Database handle */
  VdbeCursor *pCsr,               /* Sorter cursor */
  Mem *pVal                       /* Memory cell containing record */
){
  VdbeSorter *pSorter = pCsr->pSorter;
  int rc = SQLITE_OK;             /* Return Code */
  SorterRecord *pNew;             /* New list element */

  assert( pSorter );
  pSorter->nInMemory += sqlite3VarintLen(pVal->n) + pVal->n;

  pNew = (SorterRecord *)sqlite3DbMallocRaw(db, pVal->n + sizeof(SorterRecord));
  if( pNew==0 ){
    rc = SQLITE_NOMEM;
  }else{
    pNew->pVal = (void *)&pNew[1];
    memcpy(pNew->pVal, pVal->z, pVal->n);
    pNew->nVal = pVal->n;
    pNew->pNext = pSorter->pRecord;
    pSorter->pRecord = pNew;
  }

  /* See if the contents of the sorter should now be written out. They
  ** are written out when either of the following are true:
  **
  **   * The total memory allocated for the in-memory list is greater 
  **     than (page-size * cache-size), or
  **
  **   * The total memory allocated for the in-memory list is greater 
  **     than (page-size * 10) and sqlite3HeapNearlyFull() returns true.
  */
  if( rc==SQLITE_OK && pSorter->mxPmaSize>0 && (
        (pSorter->nInMemory>pSorter->mxPmaSize)
     || (pSorter->nInMemory>pSorter->mnPmaSize && sqlite3HeapNearlyFull())
  )){
    rc = vdbeSorterListToPMA(db, pCsr);
    pSorter->nInMemory = 0;
  }

  return rc;
}

/*
** Helper function for sqlite3VdbeSorterRewind(). 
*/
static int vdbeSorterInitMerge(
  sqlite3 *db,                    /* Database handle */
  VdbeCursor *pCsr,               /* Cursor handle for this sorter */
  i64 *pnByte                     /* Sum of bytes in all opened PMAs */
){
  VdbeSorter *pSorter = pCsr->pSorter;
  int rc = SQLITE_OK;             /* Return code */
  int i;                          /* Used to iterator through aIter[] */
  i64 nByte = 0;                  /* Total bytes in all opened PMAs */

  /* Initialize the iterators. */
  for(i=0; i<SORTER_MAX_MERGE_COUNT; i++){
    VdbeSorterIter *pIter = &pSorter->aIter[i];
    rc = vdbeSorterIterInit(db, pSorter, pSorter->iReadOff, pIter, &nByte);
    pSorter->iReadOff = pIter->iEof;
    assert( rc!=SQLITE_OK || pSorter->iReadOff<=pSorter->iWriteOff );
    if( rc!=SQLITE_OK || pSorter->iReadOff>=pSorter->iWriteOff ) break;
  }

  /* Initialize the aTree[] array. */
  for(i=pSorter->nTree-1; rc==SQLITE_OK && i>0; i--){
    rc = vdbeSorterDoCompare(pCsr, i);
  }

  *pnByte = nByte;
  return rc;
}

/*
** Once the sorter has been populated, this function is called to prepare
** for iterating through its contents in sorted order.
*/
int sqlite3VdbeSorterRewind(sqlite3 *db, VdbeCursor *pCsr, int *pbEof){
  VdbeSorter *pSorter = pCsr->pSorter;
  int rc;                         /* Return code */
  sqlite3_file *pTemp2 = 0;       /* Second temp file to use */
  i64 iWrite2 = 0;                /* Write offset for pTemp2 */
  int nIter;                      /* Number of iterators used */
  int nByte;                      /* Bytes of space required for aIter/aTree */
  int N = 2;                      /* Power of 2 >= nIter */

  assert( pSorter );

  /* If no data has been written to disk, then do not do so now. Instead,
  ** sort the VdbeSorter.pRecord list. The vdbe layer will read data directly
  ** from the in-memory list.  */
  if( pSorter->nPMA==0 ){
    *pbEof = !pSorter->pRecord;
    assert( pSorter->aTree==0 );
    return vdbeSorterSort(pCsr);
  }

  /* Write the current b-tree to a PMA. Close the b-tree cursor. */
  rc = vdbeSorterListToPMA(db, pCsr);
  if( rc!=SQLITE_OK ) return rc;

  /* Allocate space for aIter[] and aTree[]. */
  nIter = pSorter->nPMA;
  if( nIter>SORTER_MAX_MERGE_COUNT ) nIter = SORTER_MAX_MERGE_COUNT;
  assert( nIter>0 );
  while( N<nIter ) N += N;
  nByte = N * (sizeof(int) + sizeof(VdbeSorterIter));
  pSorter->aIter = (VdbeSorterIter *)sqlite3DbMallocZero(db, nByte);
  if( !pSorter->aIter ) return SQLITE_NOMEM;
  pSorter->aTree = (int *)&pSorter->aIter[N];
  pSorter->nTree = N;

  do {
    int iNew;                     /* Index of new, merged, PMA */

    for(iNew=0; 
        rc==SQLITE_OK && iNew*SORTER_MAX_MERGE_COUNT<pSorter->nPMA; 
        iNew++
    ){
      i64 nWrite;                 /* Number of bytes in new PMA */

      /* If there are SORTER_MAX_MERGE_COUNT or less PMAs in file pTemp1,
      ** initialize an iterator for each of them and break out of the loop.
      ** These iterators will be incrementally merged as the VDBE layer calls
      ** sqlite3VdbeSorterNext().
      **
      ** Otherwise, if pTemp1 contains more than SORTER_MAX_MERGE_COUNT PMAs,
      ** initialize interators for SORTER_MAX_MERGE_COUNT of them. These PMAs
      ** are merged into a single PMA that is written to file pTemp2.
      */
      rc = vdbeSorterInitMerge(db, pCsr, &nWrite);
      assert( rc!=SQLITE_OK || pSorter->aIter[ pSorter->aTree[1] ].pFile );
      if( rc!=SQLITE_OK || pSorter->nPMA<=SORTER_MAX_MERGE_COUNT ){
        break;
      }

      /* Open the second temp file, if it is not already open. */
      if( pTemp2==0 ){
        assert( iWrite2==0 );
        rc = vdbeSorterOpenTempFile(db, &pTemp2);
      }

      if( rc==SQLITE_OK ){
        rc = vdbeSorterWriteVarint(pTemp2, nWrite, &iWrite2);
      }

      if( rc==SQLITE_OK ){
        int bEof = 0;
        while( rc==SQLITE_OK && bEof==0 ){
          int nToWrite;
          VdbeSorterIter *pIter = &pSorter->aIter[ pSorter->aTree[1] ];
          assert( pIter->pFile );
          nToWrite = pIter->nKey + sqlite3VarintLen(pIter->nKey);
          rc = sqlite3OsWrite(pTemp2, pIter->aAlloc, nToWrite, iWrite2);
          iWrite2 += nToWrite;
          if( rc==SQLITE_OK ){
            rc = sqlite3VdbeSorterNext(db, pCsr, &bEof);
          }
        }
      }
    }

    if( pSorter->nPMA<=SORTER_MAX_MERGE_COUNT ){
      break;
    }else{
      sqlite3_file *pTmp = pSorter->pTemp1;
      pSorter->nPMA = iNew;
      pSorter->pTemp1 = pTemp2;
      pTemp2 = pTmp;
      pSorter->iWriteOff = iWrite2;
      pSorter->iReadOff = 0;
      iWrite2 = 0;
    }
  }while( rc==SQLITE_OK );

  if( pTemp2 ){
    sqlite3OsCloseFree(pTemp2);
  }
  *pbEof = (pSorter->aIter[pSorter->aTree[1]].pFile==0);
  return rc;
}

/*
** Advance to the next element in the sorter.
*/
int sqlite3VdbeSorterNext(sqlite3 *db, VdbeCursor *pCsr, int *pbEof){
  VdbeSorter *pSorter = pCsr->pSorter;
  int rc;                         /* Return code */

  if( pSorter->aTree ){
    int iPrev = pSorter->aTree[1];/* Index of iterator to advance */
    int i;                        /* Index of aTree[] to recalculate */

    rc = vdbeSorterIterNext(db, &pSorter->aIter[iPrev]);
    for(i=(pSorter->nTree+iPrev)/2; rc==SQLITE_OK && i>0; i=i/2){
      rc = vdbeSorterDoCompare(pCsr, i);
    }

    *pbEof = (pSorter->aIter[pSorter->aTree[1]].pFile==0);
  }else{
    SorterRecord *pFree = pSorter->pRecord;
    pSorter->pRecord = pFree->pNext;
    pFree->pNext = 0;
    vdbeSorterRecordFree(db, pFree);
    *pbEof = !pSorter->pRecord;
    rc = SQLITE_OK;
  }
  return rc;
}

/*
** Return a pointer to a buffer owned by the sorter that contains the 
** current key.
*/
static void *vdbeSorterRowkey(
  VdbeSorter *pSorter,            /* Sorter object */
  int *pnKey                      /* OUT: Size of current key in bytes */
){
  void *pKey;
  if( pSorter->aTree ){
    VdbeSorterIter *pIter;
    pIter = &pSorter->aIter[ pSorter->aTree[1] ];
    *pnKey = pIter->nKey;
    pKey = pIter->aKey;
  }else{
    *pnKey = pSorter->pRecord->nVal;
    pKey = pSorter->pRecord->pVal;
  }
  return pKey;
}

/*
** Copy the current sorter key into the memory cell pOut.
*/
int sqlite3VdbeSorterRowkey(VdbeCursor *pCsr, Mem *pOut){
  VdbeSorter *pSorter = pCsr->pSorter;
  void *pKey; int nKey;           /* Sorter key to copy into pOut */

  pKey = vdbeSorterRowkey(pSorter, &nKey);
  if( sqlite3VdbeMemGrow(pOut, nKey, 0) ){
    return SQLITE_NOMEM;
  }
  pOut->n = nKey;
  MemSetTypeFlag(pOut, MEM_Blob);
  memcpy(pOut->z, pKey, nKey);

  return SQLITE_OK;
}

/*
** Compare the key in memory cell pVal with the key that the sorter cursor
** passed as the first argument currently points to. For the purposes of
** the comparison, ignore the rowid field at the end of each record.
**
** If an error occurs, return an SQLite error code (i.e. SQLITE_NOMEM).
** Otherwise, set *pRes to a negative, zero or positive value if the
** key in pVal is smaller than, equal to or larger than the current sorter
** key.
*/
int sqlite3VdbeSorterCompare(
  VdbeCursor *pCsr,               /* Sorter cursor */
  Mem *pVal,                      /* Value to compare to current sorter key */
  int *pRes                       /* OUT: Result of comparison */
){
  VdbeSorter *pSorter = pCsr->pSorter;
  void *pKey; int nKey;           /* Sorter key to compare pVal with */

  pKey = vdbeSorterRowkey(pSorter, &nKey);
  vdbeSorterCompare(pCsr, 1, pVal->z, pVal->n, pKey, nKey, pRes);
  return SQLITE_OK;
}

#endif /* #ifndef SQLITE_OMIT_MERGE_SORT */

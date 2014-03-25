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


typedef struct VdbeSorterIter VdbeSorterIter;
typedef struct SorterThread SorterThread;
typedef struct SorterRecord SorterRecord;
typedef struct SorterMerger SorterMerger;
typedef struct FileWriter FileWriter;


/*
** Maximum number of threads to use. Setting this value to 1 forces all
** operations to be single-threaded.
*/
#ifndef SQLITE_MAX_SORTER_THREAD
# define SQLITE_MAX_SORTER_THREAD 1
#endif

/*
** Candidate values for SorterThread.eWork
*/
#define SORTER_THREAD_SORT   1
#define SORTER_THREAD_TO_PMA 2
#define SORTER_THREAD_CONS   3

/*
** Much of the work performed in this module to sort the list of records is 
** broken down into smaller units that may be peformed in parallel. In order
** to perform such a unit of work, an instance of the following structure
** is configured and passed to vdbeSorterThreadMain() - either directly by 
** the main thread or via a background thread.
**
** Exactly SQLITE_MAX_SORTER_THREAD instances of this structure are allocated
** as part of each VdbeSorter object. Instances are never allocated any other
** way.
**
** When a background thread is launched to perform work, SorterThread.bDone
** is set to 0 and the SorterThread.pThread variable set to point to the
** thread handle. SorterThread.bDone is set to 1 (to indicate to the main
** thread that joining SorterThread.pThread will not block) before the thread
** exits. SorterThread.pThread and bDone are always cleared after the 
** background thread has been joined.
**
** One object (specifically, VdbeSorter.aThread[SQLITE_MAX_SORTER_THREAD-1])
** is reserved for the foreground thread.
**
** The nature of the work performed is determined by SorterThread.eWork,
** as follows:
**
**   SORTER_THREAD_SORT:
**     Sort the linked list of records at SorterThread.pList.
**
**   SORTER_THREAD_TO_PMA:
**     Sort the linked list of records at SorterThread.pList, and write
**     the results to a new PMA in temp file SorterThread.pTemp1. Open
**     the temp file if it is not already open.
**
**   SORTER_THREAD_CONS:
**     Merge existing PMAs until SorterThread.nConsolidate or fewer
**     remain in temp file SorterThread.pTemp1.
*/
struct SorterThread {
  SQLiteThread *pThread;          /* Thread handle, or NULL */
  int bDone;                      /* Set to true by pThread when finished */

  sqlite3_vfs *pVfs;              /* VFS used to open temporary files */
  KeyInfo *pKeyInfo;              /* How to compare records */
  UnpackedRecord *pUnpacked;      /* Space to unpack a record */
  int pgsz;                       /* Main database page size */

  u8 eWork;                       /* One of the SORTER_THREAD_* constants */
  int nConsolidate;               /* For THREAD_CONS, max final PMAs */
  SorterRecord *pList;            /* List of records for pThread to sort */
  int nInMemory;                  /* Expected size of PMA based on pList */

  int nPMA;                       /* Number of PMAs currently in pTemp1 */
  i64 iTemp1Off;                  /* Offset to write to in pTemp1 */
  sqlite3_file *pTemp1;           /* File to write PMAs to, or NULL */
};


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
** The (N/4) elements of aTree[] that precede the final (N/2) described 
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
struct SorterMerger {
  int nTree;                      /* Used size of aTree/aIter (power of 2) */
  int *aTree;                     /* Current state of incremental merge */
  VdbeSorterIter *aIter;          /* Array of iterators to merge data from */
};

/*
** Main sorter structure. A single instance of this is allocated for each 
** sorter cursor created by the VDBE.
*/
struct VdbeSorter {
  int nInMemory;                  /* Current size of pRecord list as PMA */
  int mnPmaSize;                  /* Minimum PMA size, in bytes */
  int mxPmaSize;                  /* Maximum PMA size, in bytes.  0==no limit */
  int bUsePMA;                    /* True if one or more PMAs created */
  SorterRecord *pRecord;          /* Head of in-memory record list */
  SorterMerger *pMerger;          /* For final merge of PMAs (by caller) */ 
  SorterThread aThread[SQLITE_MAX_SORTER_THREAD];
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
  u8 *aBuffer;                    /* Current read buffer */
  int nBuffer;                    /* Size of read buffer in bytes */
};

/*
** An instance of this structure is used to organize the stream of records
** being written to files by the merge-sort code into aligned, page-sized
** blocks.  Doing all I/O in aligned page-sized blocks helps I/O to go
** faster on many operating systems.
*/
struct FileWriter {
  int eFWErr;                     /* Non-zero if in an error state */
  u8 *aBuffer;                    /* Pointer to write buffer */
  int nBuffer;                    /* Size of write buffer in bytes */
  int iBufStart;                  /* First byte of buffer to write */
  int iBufEnd;                    /* Last byte of buffer to write */
  i64 iWriteOff;                  /* Offset of start of buffer in file */
  sqlite3_file *pFile;            /* File to write to */
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

/* The minimum PMA size is set to this value multiplied by the database
** page size in bytes.  */
#define SORTER_MIN_WORKING 10

/* Maximum number of segments to merge in a single pass. */
#define SORTER_MAX_MERGE_COUNT 16

/*
** Free all memory belonging to the VdbeSorterIter object passed as the second
** argument. All structure fields are set to zero before returning.
*/
static void vdbeSorterIterZero(VdbeSorterIter *pIter){
  sqlite3_free(pIter->aAlloc);
  sqlite3_free(pIter->aBuffer);
  memset(pIter, 0, sizeof(VdbeSorterIter));
}

/*
** Read nByte bytes of data from the stream of data iterated by object p.
** If successful, set *ppOut to point to a buffer containing the data
** and return SQLITE_OK. Otherwise, if an error occurs, return an SQLite
** error code.
**
** The buffer indicated by *ppOut may only be considered valid until the
** next call to this function.
*/
static int vdbeSorterIterRead(
  VdbeSorterIter *p,              /* Iterator */
  int nByte,                      /* Bytes of data to read */
  u8 **ppOut                      /* OUT: Pointer to buffer containing data */
){
  int iBuf;                       /* Offset within buffer to read from */
  int nAvail;                     /* Bytes of data available in buffer */
  assert( p->aBuffer );

  /* If there is no more data to be read from the buffer, read the next 
  ** p->nBuffer bytes of data from the file into it. Or, if there are less
  ** than p->nBuffer bytes remaining in the PMA, read all remaining data.  */
  iBuf = p->iReadOff % p->nBuffer;
  if( iBuf==0 ){
    int nRead;                    /* Bytes to read from disk */
    int rc;                       /* sqlite3OsRead() return code */

    /* Determine how many bytes of data to read. */
    if( (p->iEof - p->iReadOff) > (i64)p->nBuffer ){
      nRead = p->nBuffer;
    }else{
      nRead = (int)(p->iEof - p->iReadOff);
    }
    assert( nRead>0 );

    /* Read data from the file. Return early if an error occurs. */
    rc = sqlite3OsRead(p->pFile, p->aBuffer, nRead, p->iReadOff);
    assert( rc!=SQLITE_IOERR_SHORT_READ );
    if( rc!=SQLITE_OK ) return rc;
  }
  nAvail = p->nBuffer - iBuf; 

  if( nByte<=nAvail ){
    /* The requested data is available in the in-memory buffer. In this
    ** case there is no need to make a copy of the data, just return a 
    ** pointer into the buffer to the caller.  */
    *ppOut = &p->aBuffer[iBuf];
    p->iReadOff += nByte;
  }else{
    /* The requested data is not all available in the in-memory buffer.
    ** In this case, allocate space at p->aAlloc[] to copy the requested
    ** range into. Then return a copy of pointer p->aAlloc to the caller.  */
    int nRem;                     /* Bytes remaining to copy */

    /* Extend the p->aAlloc[] allocation if required. */
    if( p->nAlloc<nByte ){
      u8 *aNew;
      int nNew = p->nAlloc*2;
      while( nByte>nNew ) nNew = nNew*2;
      aNew = sqlite3Realloc(p->aAlloc, nNew);
      if( !aNew ) return SQLITE_NOMEM;
      p->nAlloc = nNew;
      p->aAlloc = aNew;
    }

    /* Copy as much data as is available in the buffer into the start of
    ** p->aAlloc[].  */
    memcpy(p->aAlloc, &p->aBuffer[iBuf], nAvail);
    p->iReadOff += nAvail;
    nRem = nByte - nAvail;

    /* The following loop copies up to p->nBuffer bytes per iteration into
    ** the p->aAlloc[] buffer.  */
    while( nRem>0 ){
      int rc;                     /* vdbeSorterIterRead() return code */
      int nCopy;                  /* Number of bytes to copy */
      u8 *aNext;                  /* Pointer to buffer to copy data from */

      nCopy = nRem;
      if( nRem>p->nBuffer ) nCopy = p->nBuffer;
      rc = vdbeSorterIterRead(p, nCopy, &aNext);
      if( rc!=SQLITE_OK ) return rc;
      assert( aNext!=p->aAlloc );
      memcpy(&p->aAlloc[nByte - nRem], aNext, nCopy);
      nRem -= nCopy;
    }

    *ppOut = p->aAlloc;
  }

  return SQLITE_OK;
}

/*
** Read a varint from the stream of data accessed by p. Set *pnOut to
** the value read.
*/
static int vdbeSorterIterVarint(VdbeSorterIter *p, u64 *pnOut){
  int iBuf;

  iBuf = p->iReadOff % p->nBuffer;
  if( iBuf && (p->nBuffer-iBuf)>=9 ){
    p->iReadOff += sqlite3GetVarint(&p->aBuffer[iBuf], pnOut);
  }else{
    u8 aVarint[16], *a;
    int i = 0, rc;
    do{
      rc = vdbeSorterIterRead(p, 1, &a);
      if( rc ) return rc;
      aVarint[(i++)&0xf] = a[0];
    }while( (a[0]&0x80)!=0 );
    sqlite3GetVarint(aVarint, pnOut);
  }

  return SQLITE_OK;
}


/*
** Advance iterator pIter to the next key in its PMA. Return SQLITE_OK if
** no error occurs, or an SQLite error code if one does.
*/
static int vdbeSorterIterNext(VdbeSorterIter *pIter){
  int rc;                         /* Return Code */
  u64 nRec = 0;                   /* Size of record in bytes */

  if( pIter->iReadOff>=pIter->iEof ){
    /* This is an EOF condition */
    vdbeSorterIterZero(pIter);
    return SQLITE_OK;
  }

  rc = vdbeSorterIterVarint(pIter, &nRec);
  if( rc==SQLITE_OK ){
    pIter->nKey = (int)nRec;
    rc = vdbeSorterIterRead(pIter, (int)nRec, &pIter->aKey);
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
  SorterThread *pThread,          /* Thread context */
  i64 iStart,                     /* Start offset in pThread->pTemp1 */
  VdbeSorterIter *pIter,          /* Iterator to populate */
  i64 *pnByte                     /* IN/OUT: Increment this value by PMA size */
){
  int rc = SQLITE_OK;
  int nBuf = pThread->pgsz;

  assert( pThread->iTemp1Off>iStart );
  assert( pIter->aAlloc==0 );
  assert( pIter->aBuffer==0 );
  pIter->pFile = pThread->pTemp1;
  pIter->iReadOff = iStart;
  pIter->nAlloc = 128;
  pIter->aAlloc = (u8*)sqlite3Malloc(pIter->nAlloc);
  pIter->nBuffer = nBuf;
  pIter->aBuffer = (u8*)sqlite3Malloc(nBuf);

  if( !pIter->aBuffer ){
    rc = SQLITE_NOMEM;
  }else{
    int iBuf;

    iBuf = iStart % nBuf;
    if( iBuf ){
      int nRead = nBuf - iBuf;
      if( (iStart + nRead) > pThread->iTemp1Off ){
        nRead = (int)(pThread->iTemp1Off - iStart);
      }
      rc = sqlite3OsRead(
          pThread->pTemp1, &pIter->aBuffer[iBuf], nRead, iStart
      );
      assert( rc!=SQLITE_IOERR_SHORT_READ );
    }

    if( rc==SQLITE_OK ){
      u64 nByte;
      pIter->iEof = pThread->iTemp1Off;
      rc = vdbeSorterIterVarint(pIter, &nByte);
      pIter->iEof = pIter->iReadOff + nByte;
      *pnByte += nByte;
    }
  }

  if( rc==SQLITE_OK ){
    rc = vdbeSorterIterNext(pIter);
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
  SorterThread *pThread,          /* Thread context (for pKeyInfo) */
  int nIgnore,                    /* Ignore the last nIgnore fields */
  const void *pKey1, int nKey1,   /* Left side of comparison */
  const void *pKey2, int nKey2,   /* Right side of comparison */
  int *pRes                       /* OUT: Result of comparison */
){
  KeyInfo *pKeyInfo = pThread->pKeyInfo;
  UnpackedRecord *r2 = pThread->pUnpacked;
  int i;

  if( pKey2 ){
    sqlite3VdbeRecordUnpack(pKeyInfo, nKey2, pKey2, r2);
  }

  if( nIgnore ){
    r2->nField = pKeyInfo->nField - nIgnore;
    assert( r2->nField>0 );
    for(i=0; i<r2->nField; i++){
      if( r2->aMem[i].flags & MEM_Null ){
        *pRes = -1;
        return;
      }
    }
    assert( r2->default_rc==0 );
  }

  *pRes = sqlite3VdbeRecordCompare(nKey1, pKey1, r2, 0);
}

/*
** This function is called to compare two iterator keys when merging 
** multiple b-tree segments. Parameter iOut is the index of the aTree[] 
** value to recalculate.
*/
static int vdbeSorterDoCompare(
  SorterThread *pThread, 
  SorterMerger *pMerger, 
  int iOut
){
  int i1;
  int i2;
  int iRes;
  VdbeSorterIter *p1;
  VdbeSorterIter *p2;

  assert( iOut<pMerger->nTree && iOut>0 );

  if( iOut>=(pMerger->nTree/2) ){
    i1 = (iOut - pMerger->nTree/2) * 2;
    i2 = i1 + 1;
  }else{
    i1 = pMerger->aTree[iOut*2];
    i2 = pMerger->aTree[iOut*2+1];
  }

  p1 = &pMerger->aIter[i1];
  p2 = &pMerger->aIter[i2];

  if( p1->pFile==0 ){
    iRes = i2;
  }else if( p2->pFile==0 ){
    iRes = i1;
  }else{
    int res;
    assert( pThread->pUnpacked!=0 );  /* allocated in vdbeSorterMerge() */
    vdbeSorterCompare(
        pThread, 0, p1->aKey, p1->nKey, p2->aKey, p2->nKey, &res
    );
    if( res<=0 ){
      iRes = i1;
    }else{
      iRes = i2;
    }
  }

  pMerger->aTree[iOut] = iRes;
  return SQLITE_OK;
}

/*
** Initialize the temporary index cursor just opened as a sorter cursor.
*/
int sqlite3VdbeSorterInit(sqlite3 *db, VdbeCursor *pCsr){
  int pgsz;                       /* Page size of main database */
  int i;                          /* Used to iterate through aThread[] */
  int mxCache;                    /* Cache size */
  VdbeSorter *pSorter;            /* The new sorter */
  KeyInfo *pKeyInfo;              /* Copy of pCsr->pKeyInfo with db==0 */
  int szKeyInfo;                  /* Size of pCsr->pKeyInfo in bytes */

  assert( pCsr->pKeyInfo && pCsr->pBt==0 );
  szKeyInfo = sizeof(KeyInfo) + (pCsr->pKeyInfo->nField-1)*sizeof(CollSeq*);
  pSorter = (VdbeSorter*)sqlite3DbMallocZero(db, sizeof(VdbeSorter)+szKeyInfo);
  pCsr->pSorter = pSorter;
  if( pSorter==0 ){
    return SQLITE_NOMEM;
  }
  pKeyInfo = (KeyInfo*)&pSorter[1];
  memcpy(pKeyInfo, pCsr->pKeyInfo, szKeyInfo);
  pKeyInfo->db = 0;
  pgsz = sqlite3BtreeGetPageSize(db->aDb[0].pBt);

  for(i=0; i<SQLITE_MAX_SORTER_THREAD; i++){
    SorterThread *pThread = &pSorter->aThread[i];
    pThread->pKeyInfo = pKeyInfo;
    pThread->pVfs = db->pVfs;
    pThread->pgsz = pgsz;
  }

  if( !sqlite3TempInMemory(db) ){
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
** Free all resources owned by the object indicated by argument pThread. All 
** fields of *pThread are zeroed before returning.
*/
static void vdbeSorterThreadCleanup(sqlite3 *db, SorterThread *pThread){
  sqlite3DbFree(db, pThread->pUnpacked);
  pThread->pUnpacked = 0;
  vdbeSorterRecordFree(0, pThread->pList);
  pThread->pList = 0;
  if( pThread->pTemp1 ){
    sqlite3OsCloseFree(pThread->pTemp1);
    pThread->pTemp1 = 0;
  }
}

/*
** Join all threads.  
*/
static int vdbeSorterJoinAll(VdbeSorter *pSorter, int rcin){
  int rc = rcin;
  int i;
  for(i=0; i<SQLITE_MAX_SORTER_THREAD; i++){
    SorterThread *pThread = &pSorter->aThread[i];
    if( pThread->pThread ){
      void *pRet;
      int rc2 = sqlite3ThreadJoin(pThread->pThread, &pRet);
      pThread->pThread = 0;
      pThread->bDone = 0;
      if( rc==SQLITE_OK ) rc = rc2;
      if( rc==SQLITE_OK ) rc = SQLITE_PTR_TO_INT(pRet);
    }
  }
  return rc;
}

/*
** Allocate a new SorterMerger object with space for nIter iterators.
*/
static SorterMerger *vdbeSorterMergerNew(int nIter){
  int N = 2;                      /* Smallest power of two >= nIter */
  int nByte;                      /* Total bytes of space to allocate */
  SorterMerger *pNew;             /* Pointer to allocated object to return */

  assert( nIter<=SORTER_MAX_MERGE_COUNT );
  while( N<nIter ) N += N;
  nByte = sizeof(SorterMerger) + N * (sizeof(int) + sizeof(VdbeSorterIter));

  pNew = (SorterMerger*)sqlite3MallocZero(nByte);
  if( pNew ){
    pNew->nTree = N;
    pNew->aIter = (VdbeSorterIter*)&pNew[1];
    pNew->aTree = (int*)&pNew->aIter[N];
  }
  return pNew;
}

/*
** Reset a merger
*/
static void vdbeSorterMergerReset(SorterMerger *pMerger){
  int i;
  if( pMerger ){
    for(i=0; i<pMerger->nTree; i++){
      vdbeSorterIterZero(&pMerger->aIter[i]);
    }
  }
}


/*
** Free the SorterMerger object passed as the only argument.
*/
static void vdbeSorterMergerFree(SorterMerger *pMerger){
  vdbeSorterMergerReset(pMerger);
  sqlite3_free(pMerger);
}

/*
** Reset a sorting cursor back to its original empty state.
*/
void sqlite3VdbeSorterReset(sqlite3 *db, VdbeSorter *pSorter){
  int i;
  vdbeSorterJoinAll(pSorter, SQLITE_OK);
  for(i=0; i<SQLITE_MAX_SORTER_THREAD; i++){
    SorterThread *pThread = &pSorter->aThread[i];
    vdbeSorterThreadCleanup(db, pThread);
  }
  vdbeSorterRecordFree(0, pSorter->pRecord);
  vdbeSorterMergerReset(pSorter->pMerger);
  pSorter->pRecord = 0;
  pSorter->nInMemory = 0;
  pSorter->bUsePMA = 0;
}

/*
** Free any cursor components allocated by sqlite3VdbeSorterXXX routines.
*/
void sqlite3VdbeSorterClose(sqlite3 *db, VdbeCursor *pCsr){
  VdbeSorter *pSorter = pCsr->pSorter;
  if( pSorter ){
    sqlite3VdbeSorterReset(db, pSorter);
    vdbeSorterMergerFree(pSorter->pMerger);
    sqlite3DbFree(db, pSorter);
    pCsr->pSorter = 0;
  }
}

/*
** Allocate space for a file-handle and open a temporary file. If successful,
** set *ppFile to point to the malloc'd file-handle and return SQLITE_OK.
** Otherwise, set *ppFile to 0 and return an SQLite error code.
*/
static int vdbeSorterOpenTempFile(sqlite3_vfs *pVfs, sqlite3_file **ppFile){
  int dummy;
  return sqlite3OsOpenMalloc(pVfs, 0, ppFile,
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
  SorterThread *pThread,          /* Calling thread context */
  SorterRecord *p1,               /* First list to merge */
  SorterRecord *p2,               /* Second list to merge */
  SorterRecord **ppOut            /* OUT: Head of merged list */
){
  SorterRecord *pFinal = 0;
  SorterRecord **pp = &pFinal;
  void *pVal2 = p2 ? p2->pVal : 0;

  while( p1 && p2 ){
    int res;
    vdbeSorterCompare(pThread, 0, p1->pVal, p1->nVal, pVal2, p2->nVal, &res);
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
** Sort the linked list of records headed at pThread->pList. Return 
** SQLITE_OK if successful, or an SQLite error code (i.e. SQLITE_NOMEM) if 
** an error occurs.
*/
static int vdbeSorterSort(SorterThread *pThread){
  int i;
  SorterRecord **aSlot;
  SorterRecord *p;

  aSlot = (SorterRecord **)sqlite3MallocZero(64 * sizeof(SorterRecord *));
  if( !aSlot ){
    return SQLITE_NOMEM;
  }

  p = pThread->pList;
  while( p ){
    SorterRecord *pNext = p->pNext;
    p->pNext = 0;
    for(i=0; aSlot[i]; i++){
      vdbeSorterMerge(pThread, p, aSlot[i], &p);
      aSlot[i] = 0;
    }
    aSlot[i] = p;
    p = pNext;
  }

  p = 0;
  for(i=0; i<64; i++){
    vdbeSorterMerge(pThread, p, aSlot[i], &p);
  }
  pThread->pList = p;

  sqlite3_free(aSlot);
  return SQLITE_OK;
}

/*
** Initialize a file-writer object.
*/
static void fileWriterInit(
  sqlite3_file *pFile,            /* File to write to */
  FileWriter *p,                  /* Object to populate */
  int nBuf,                       /* Buffer size */
  i64 iStart                      /* Offset of pFile to begin writing at */
){
  memset(p, 0, sizeof(FileWriter));
  p->aBuffer = (u8*)sqlite3Malloc(nBuf);
  if( !p->aBuffer ){
    p->eFWErr = SQLITE_NOMEM;
  }else{
    p->iBufEnd = p->iBufStart = (iStart % nBuf);
    p->iWriteOff = iStart - p->iBufStart;
    p->nBuffer = nBuf;
    p->pFile = pFile;
  }
}

/*
** Write nData bytes of data to the file-write object. Return SQLITE_OK
** if successful, or an SQLite error code if an error occurs.
*/
static void fileWriterWrite(FileWriter *p, u8 *pData, int nData){
  int nRem = nData;
  while( nRem>0 && p->eFWErr==0 ){
    int nCopy = nRem;
    if( nCopy>(p->nBuffer - p->iBufEnd) ){
      nCopy = p->nBuffer - p->iBufEnd;
    }

    memcpy(&p->aBuffer[p->iBufEnd], &pData[nData-nRem], nCopy);
    p->iBufEnd += nCopy;
    if( p->iBufEnd==p->nBuffer ){
      p->eFWErr = sqlite3OsWrite(p->pFile, 
          &p->aBuffer[p->iBufStart], p->iBufEnd - p->iBufStart, 
          p->iWriteOff + p->iBufStart
      );
      p->iBufStart = p->iBufEnd = 0;
      p->iWriteOff += p->nBuffer;
    }
    assert( p->iBufEnd<p->nBuffer );

    nRem -= nCopy;
  }
}

/*
** Flush any buffered data to disk and clean up the file-writer object.
** The results of using the file-writer after this call are undefined.
** Return SQLITE_OK if flushing the buffered data succeeds or is not 
** required. Otherwise, return an SQLite error code.
**
** Before returning, set *piEof to the offset immediately following the
** last byte written to the file.
*/
static int fileWriterFinish(FileWriter *p, i64 *piEof){
  int rc;
  if( p->eFWErr==0 && ALWAYS(p->aBuffer) && p->iBufEnd>p->iBufStart ){
    p->eFWErr = sqlite3OsWrite(p->pFile, 
        &p->aBuffer[p->iBufStart], p->iBufEnd - p->iBufStart, 
        p->iWriteOff + p->iBufStart
    );
  }
  *piEof = (p->iWriteOff + p->iBufEnd);
  sqlite3_free(p->aBuffer);
  rc = p->eFWErr;
  memset(p, 0, sizeof(FileWriter));
  return rc;
}

/*
** Write value iVal encoded as a varint to the file-write object. Return 
** SQLITE_OK if successful, or an SQLite error code if an error occurs.
*/
static void fileWriterWriteVarint(FileWriter *p, u64 iVal){
  int nByte; 
  u8 aByte[10];
  nByte = sqlite3PutVarint(aByte, iVal);
  fileWriterWrite(p, aByte, nByte);
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
static int vdbeSorterListToPMA(SorterThread *pThread){
  int rc = SQLITE_OK;             /* Return code */
  FileWriter writer;              /* Object used to write to the file */

  memset(&writer, 0, sizeof(FileWriter));
  assert( pThread->nInMemory>0 );

  /* If the first temporary PMA file has not been opened, open it now. */
  if( pThread->pTemp1==0 ){
    rc = vdbeSorterOpenTempFile(pThread->pVfs, &pThread->pTemp1);
    assert( rc!=SQLITE_OK || pThread->pTemp1 );
    assert( pThread->iTemp1Off==0 );
    assert( pThread->nPMA==0 );
  }

  if( rc==SQLITE_OK ){
    SorterRecord *p;
    SorterRecord *pNext = 0;

    fileWriterInit(pThread->pTemp1, &writer, pThread->pgsz, pThread->iTemp1Off);
    pThread->nPMA++;
    fileWriterWriteVarint(&writer, pThread->nInMemory);
    for(p=pThread->pList; p; p=pNext){
      pNext = p->pNext;
      fileWriterWriteVarint(&writer, p->nVal);
      fileWriterWrite(&writer, p->pVal, p->nVal);
      sqlite3_free(p);
    }
    pThread->pList = p;
    rc = fileWriterFinish(&writer, &pThread->iTemp1Off);
  }

  return rc;
}

/*
** Advance the SorterMerger iterator passed as the second argument to
** the next entry. Set *pbEof to true if this means the iterator has 
** reached EOF.
**
** Return SQLITE_OK if successful or an error code if an error occurs.
*/
static int vdbeSorterNext(
  SorterThread *pThread, 
  SorterMerger *pMerger, 
  int *pbEof
){
  int rc;
  int iPrev = pMerger->aTree[1];/* Index of iterator to advance */
  int i;                        /* Index of aTree[] to recalculate */

  /* Advance the current iterator */
  rc = vdbeSorterIterNext(&pMerger->aIter[iPrev]);

  /* Update contents of aTree[] */
  for(i=(pMerger->nTree+iPrev)/2; rc==SQLITE_OK && i>0; i=i/2){
    rc = vdbeSorterDoCompare(pThread, pMerger, i);
  }

  *pbEof = (pMerger->aIter[pMerger->aTree[1]].pFile==0);
  return rc;
}

/*
** The main routine for sorter-thread operations.
*/
static void *vdbeSorterThreadMain(void *pCtx){
  int rc = SQLITE_OK;
  SorterThread *pThread = (SorterThread*)pCtx;

  assert( pThread->eWork==SORTER_THREAD_SORT
       || pThread->eWork==SORTER_THREAD_TO_PMA
       || pThread->eWork==SORTER_THREAD_CONS
  );
  assert( pThread->bDone==0 );

  if( pThread->pUnpacked==0 ){
    char *pFree;
    pThread->pUnpacked = sqlite3VdbeAllocUnpackedRecord(
        pThread->pKeyInfo, 0, 0, &pFree
    );
    assert( pThread->pUnpacked==(UnpackedRecord*)pFree );
    if( pFree==0 ){
      rc = SQLITE_NOMEM;
      goto thread_out;
    }
  }

  if( pThread->eWork==SORTER_THREAD_CONS ){
    assert( pThread->pList==0 );
    while( pThread->nPMA>pThread->nConsolidate && rc==SQLITE_OK ){
      int nIter = MIN(pThread->nPMA, SORTER_MAX_MERGE_COUNT);
      sqlite3_file *pTemp2 = 0;     /* Second temp file to use */
      SorterMerger *pMerger;        /* Object for reading/merging PMA data */
      i64 iReadOff = 0;             /* Offset in pTemp1 to read from */
      i64 iWriteOff = 0;            /* Offset in pTemp2 to write to */
      int i;
      
      /* Allocate a merger object to merge PMAs together. */
      pMerger = vdbeSorterMergerNew(nIter);
      if( pMerger==0 ){
        rc = SQLITE_NOMEM;
        break;
      }

      /* Open a second temp file to write merged data to */
      rc = vdbeSorterOpenTempFile(pThread->pVfs, &pTemp2);
      if( rc!=SQLITE_OK ){
        vdbeSorterMergerFree(pMerger);
        break;
      }

      /* This loop runs once for each output PMA. Each output PMA is made
      ** of data merged from up to SORTER_MAX_MERGE_COUNT input PMAs. */
      for(i=0; i<pThread->nPMA; i+=SORTER_MAX_MERGE_COUNT){
        FileWriter writer;        /* Object for writing data to pTemp2 */
        i64 nOut = 0;             /* Bytes of data in output PMA */
        int bEof = 0;
        int rc2;

        /* Configure the merger object to read and merge data from the next 
        ** SORTER_MAX_MERGE_COUNT PMAs in pTemp1 (or from all remaining PMAs,
        ** if that is fewer). */
        int iIter;
        for(iIter=0; iIter<SORTER_MAX_MERGE_COUNT; iIter++){
          VdbeSorterIter *pIter = &pMerger->aIter[iIter];
          rc = vdbeSorterIterInit(pThread, iReadOff, pIter, &nOut);
          iReadOff = pIter->iEof;
          if( iReadOff>=pThread->iTemp1Off || rc!=SQLITE_OK ) break;
        }
        for(iIter=pMerger->nTree-1; rc==SQLITE_OK && iIter>0; iIter--){
          rc = vdbeSorterDoCompare(pThread, pMerger, iIter);
        }

        fileWriterInit(pTemp2, &writer, pThread->pgsz, iWriteOff);
        fileWriterWriteVarint(&writer, nOut);
        while( rc==SQLITE_OK && bEof==0 ){
          VdbeSorterIter *pIter = &pMerger->aIter[ pMerger->aTree[1] ];
          assert( pIter->pFile!=0 );        /* pIter is not at EOF */
          fileWriterWriteVarint(&writer, pIter->nKey);
          fileWriterWrite(&writer, pIter->aKey, pIter->nKey);
          rc = vdbeSorterNext(pThread, pMerger, &bEof);
        }
        rc2 = fileWriterFinish(&writer, &iWriteOff);
        if( rc==SQLITE_OK ) rc = rc2;
      }

      vdbeSorterMergerFree(pMerger);
      sqlite3OsCloseFree(pThread->pTemp1);
      pThread->pTemp1 = pTemp2;
      pThread->nPMA = (i / SORTER_MAX_MERGE_COUNT);
      pThread->iTemp1Off = iWriteOff;
    }
  }else{
    /* Sort the pThread->pList list */
    rc = vdbeSorterSort(pThread);

    /* If required, write the list out to a PMA. */
    if( rc==SQLITE_OK && pThread->eWork==SORTER_THREAD_TO_PMA ){
#ifdef SQLITE_DEBUG
      i64 nExpect = pThread->nInMemory
        + sqlite3VarintLen(pThread->nInMemory)
        + pThread->iTemp1Off;
#endif
      rc = vdbeSorterListToPMA(pThread);
      assert( rc!=SQLITE_OK || (nExpect==pThread->iTemp1Off) );
    }
  }

 thread_out:
  pThread->bDone = 1;
  return SQLITE_INT_TO_PTR(rc);
}

/*
** Run the activity scheduled by the object passed as the only argument
** in the current thread.
*/
static int vdbeSorterRunThread(SorterThread *pThread){
  int rc = SQLITE_PTR_TO_INT( vdbeSorterThreadMain((void*)pThread) );
  assert( pThread->bDone );
  pThread->bDone = 0;
  return rc;
}

/*
** Flush the current contents of VdbeSorter.pRecord to a new PMA, possibly
** using a background thread.
**
** If argument bFg is non-zero, the operation always uses the calling thread.
*/
static int vdbeSorterFlushPMA(sqlite3 *db, const VdbeCursor *pCsr, int bFg){
  VdbeSorter *pSorter = pCsr->pSorter;
  int rc = SQLITE_OK;
  int i;
  SorterThread *pThread;        /* Thread context used to create new PMA */

  pSorter->bUsePMA = 1;
  for(i=0; ALWAYS( i<SQLITE_MAX_SORTER_THREAD ); i++){
    pThread = &pSorter->aThread[i];
    if( pThread->bDone ){
      void *pRet;
      assert( pThread->pThread );
      rc = sqlite3ThreadJoin(pThread->pThread, &pRet);
      pThread->pThread = 0;
      pThread->bDone = 0;
      if( rc==SQLITE_OK ){
        rc = SQLITE_PTR_TO_INT(pRet);
      }
    }
    if( pThread->pThread==0 ) break;
  }

  if( rc==SQLITE_OK ){
    assert( pThread->pThread==0 && pThread->bDone==0 );
    pThread->eWork = SORTER_THREAD_TO_PMA;
    pThread->pList = pSorter->pRecord;
    pThread->nInMemory = pSorter->nInMemory;
    pSorter->nInMemory = 0;
    pSorter->pRecord = 0;

    if( bFg || i<(SQLITE_MAX_SORTER_THREAD-1) ){
      void *pCtx = (void*)pThread;
      rc = sqlite3ThreadCreate(&pThread->pThread, vdbeSorterThreadMain, pCtx);
    }else{
      /* Use the foreground thread for this operation */
      rc = vdbeSorterRunThread(pThread);
    }
  }

  return rc;
}

/*
** Add a record to the sorter.
*/
int sqlite3VdbeSorterWrite(
  sqlite3 *db,                    /* Database handle */
  const VdbeCursor *pCsr,               /* Sorter cursor */
  Mem *pVal                       /* Memory cell containing record */
){
  VdbeSorter *pSorter = pCsr->pSorter;
  int rc = SQLITE_OK;             /* Return Code */
  SorterRecord *pNew;             /* New list element */

  assert( pSorter );
  pSorter->nInMemory += sqlite3VarintLen(pVal->n) + pVal->n;

  pNew = (SorterRecord *)sqlite3Malloc(pVal->n + sizeof(SorterRecord));
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
    rc = vdbeSorterFlushPMA(db, pCsr, 0);
  }

  return rc;
}

/*
** Return the total number of PMAs in all temporary files.
*/
static int vdbeSorterCountPMA(VdbeSorter *pSorter){
  int nPMA = 0;
  int i;
  for(i=0; i<SQLITE_MAX_SORTER_THREAD; i++){
    nPMA += pSorter->aThread[i].nPMA;
  }
  return nPMA;
}

/*
** Once the sorter has been populated, this function is called to prepare
** for iterating through its contents in sorted order.
*/
int sqlite3VdbeSorterRewind(sqlite3 *db, const VdbeCursor *pCsr, int *pbEof){
  VdbeSorter *pSorter = pCsr->pSorter;
  int rc = SQLITE_OK;             /* Return code */

  assert( pSorter );

  /* If no data has been written to disk, then do not do so now. Instead,
  ** sort the VdbeSorter.pRecord list. The vdbe layer will read data directly
  ** from the in-memory list.  */
  if( pSorter->bUsePMA==0 ){
    if( pSorter->pRecord ){
      SorterThread *pThread = &pSorter->aThread[0];
      *pbEof = 0;
      pThread->pList = pSorter->pRecord;
      pThread->eWork = SORTER_THREAD_SORT;
      rc = vdbeSorterRunThread(pThread);
      pSorter->pRecord = pThread->pList;
      pThread->pList = 0;
    }else{
      *pbEof = 1;
    }
    return rc;
  }

  /* Write the current in-memory list to a PMA. */
  if( pSorter->pRecord ){
    rc = vdbeSorterFlushPMA(db, pCsr, 1);
  }

  /* Join all threads */
  rc = vdbeSorterJoinAll(pSorter, rc);

  /* If there are more than SORTER_MAX_MERGE_COUNT PMAs on disk, merge
  ** some of them together so that this is no longer the case. */
  assert( SORTER_MAX_MERGE_COUNT>=SQLITE_MAX_SORTER_THREAD );
  if( vdbeSorterCountPMA(pSorter)>SORTER_MAX_MERGE_COUNT ){
    int i;
    for(i=0; rc==SQLITE_OK && i<SQLITE_MAX_SORTER_THREAD; i++){
      SorterThread *pThread = &pSorter->aThread[i];
      if( pThread->pTemp1 ){
        pThread->nConsolidate = SORTER_MAX_MERGE_COUNT/SQLITE_MAX_SORTER_THREAD;
        pThread->eWork = SORTER_THREAD_CONS;

        if( i<(SQLITE_MAX_SORTER_THREAD-1) ){
          void *pCtx = (void*)pThread;
          rc = sqlite3ThreadCreate(&pThread->pThread,vdbeSorterThreadMain,pCtx);
        }else{
          rc = vdbeSorterRunThread(pThread);
        }
      }
    }
  }

  /* Join all threads */
  rc = vdbeSorterJoinAll(pSorter, rc);

  /* Assuming no errors have occurred, set up a merger structure to read
  ** and merge all remaining PMAs.  */
  assert( pSorter->pMerger==0 );
  if( rc==SQLITE_OK ){
    int nIter = 0;                /* Number of iterators used */
    int i;
    SorterMerger *pMerger;
    for(i=0; i<SQLITE_MAX_SORTER_THREAD; i++){
      nIter += pSorter->aThread[i].nPMA;
    }

    pSorter->pMerger = pMerger = vdbeSorterMergerNew(nIter);
    if( pMerger==0 ){
      rc = SQLITE_NOMEM;
    }else{
      int iIter = 0;
      int iThread = 0;
      for(iThread=0; iThread<SQLITE_MAX_SORTER_THREAD; iThread++){
        int iPMA;
        i64 iReadOff = 0;
        SorterThread *pThread = &pSorter->aThread[iThread];
        for(iPMA=0; iPMA<pThread->nPMA && rc==SQLITE_OK; iPMA++){
          i64 nDummy = 0;
          VdbeSorterIter *pIter = &pMerger->aIter[iIter++];
          rc = vdbeSorterIterInit(pThread, iReadOff, pIter, &nDummy);
          iReadOff = pIter->iEof;
        }
      }

      for(i=pMerger->nTree-1; rc==SQLITE_OK && i>0; i--){
        rc = vdbeSorterDoCompare(&pSorter->aThread[0], pMerger, i);
      }
    }
  }

  if( rc==SQLITE_OK ){
    *pbEof = (pSorter->pMerger->aIter[pSorter->pMerger->aTree[1]].pFile==0);
  }
  return rc;
}

/*
** Advance to the next element in the sorter.
*/
int sqlite3VdbeSorterNext(sqlite3 *db, const VdbeCursor *pCsr, int *pbEof){
  VdbeSorter *pSorter = pCsr->pSorter;
  int rc;                         /* Return code */

  if( pSorter->pMerger ){
    rc = vdbeSorterNext(&pSorter->aThread[0], pSorter->pMerger, pbEof);
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
  const VdbeSorter *pSorter,      /* Sorter object */
  int *pnKey                      /* OUT: Size of current key in bytes */
){
  void *pKey;
  if( pSorter->pMerger ){
    VdbeSorterIter *pIter;
    pIter = &pSorter->pMerger->aIter[ pSorter->pMerger->aTree[1] ];
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
int sqlite3VdbeSorterRowkey(const VdbeCursor *pCsr, Mem *pOut){
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
  const VdbeCursor *pCsr,         /* Sorter cursor */
  Mem *pVal,                      /* Value to compare to current sorter key */
  int nIgnore,                    /* Ignore this many fields at the end */
  int *pRes                       /* OUT: Result of comparison */
){
  VdbeSorter *pSorter = pCsr->pSorter;
  SorterThread *pMain = &pSorter->aThread[0];
  void *pKey; int nKey;           /* Sorter key to compare pVal with */

  pKey = vdbeSorterRowkey(pSorter, &nKey);
  vdbeSorterCompare(pMain, nIgnore, pVal->z, pVal->n, pKey, nKey, pRes);
  return SQLITE_OK;
}

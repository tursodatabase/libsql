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

/*
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
  int nWorking;                   /* Start a new b-tree after this many pages */
  int nTree;                      /* Used size of aTree/aIter (power of 2) */
  VdbeSorterIter *aIter;          /* Array of iterators to merge */
  int *aTree;                     /* Current state of incremental merge */

  i64 iWriteOff;                  /* Current write offset within file pTemp1 */
  sqlite3_file *pTemp1;           /* PMA file 1 */
  i64 *aOffset;                   /* Array of PMA offsets for file 1 */
  int nOffset;                    /* Size of aOffset[] array */
};

/*
** The following type is an iterator for a PMA. It caches the current key in 
** variables nKey/aKey. If the iterator is at EOF, pFile==0.
*/
struct VdbeSorterIter {
  i64 iReadOff;                   /* Current read offset */
  i64 iEof;                       /* 1 byte past EOF for this iterator */
  sqlite3_file *pFile;            /* File iterator is reading from */
  int nAlloc;                     /* Bytes of space at aAlloc */
  u8 *aAlloc;                     /* Allocated space */
  int nKey;                       /* Number of bytes in key */
  u8 *aKey;                       /* Pointer to current key */
};

/* Minimum allowable value for the VdbeSorter.nWorking variable */
#define SORTER_MIN_SEGMENT_SIZE 10

/* Maximum number of segments to merge in a single go */
#define SORTER_MAX_MERGE_COUNT 16

/*
** Append integer iOff to the VdbeSorter.aOffset[] array of the sorter object
** passed as the second argument. SQLITE_NOMEM is returned if an OOM error
** is encountered, or SQLITE_OK if no error occurs.
**
** TODO: The aOffset[] array may grow indefinitely. Fix this.
*/
static int vdbeSorterAppendOffset(sqlite3 *db, VdbeSorter *p, i64 iOff){
  p->aOffset = sqlite3DbReallocOrFree(
      db, p->aOffset, (p->nOffset+1)*sizeof(i64)
  );
  if( !p->aOffset ) return SQLITE_NOMEM;
  p->aOffset[p->nOffset++] = iOff;
  return SQLITE_OK;
}

/*
** Free all memory belonging to the VdbeSorterIter object passed as the second
** argument. All structure fields are set to zero before returning.
*/
static void vdbeSorterIterZero(sqlite3 *db, VdbeSorterIter *pIter){
  sqlite3DbFree(db, pIter->aAlloc);
  memset(pIter, 0, sizeof(VdbeSorterIter));
}

/*
** Advance iterator pIter to the next key in its PMA.
*/
static int vdbeSorterIterNext(
  sqlite3 *db,                    /* Database handle (for sqlite3DbMalloc() ) */
  VdbeSorterIter *pIter           /* Iterator to advance */
){
  int rc;
  int nRead;
  int nRec;
  int iOff;

  assert( pIter->nAlloc>5 );
  nRead = pIter->iEof - pIter->iReadOff;
  if( nRead>5 ) nRead = 5;

  if( nRead<=0 ){
    vdbeSorterIterZero(db, pIter);
    return SQLITE_OK;
  }

  rc = sqlite3OsRead(pIter->pFile, pIter->aAlloc, nRead, pIter->iReadOff);
  iOff = getVarint32(pIter->aAlloc, nRec);

  if( rc==SQLITE_OK && (iOff+nRec)>nRead ){
    int nRead2;
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

  assert( nRec>0 || rc!=SQLITE_OK );

  pIter->iReadOff += iOff+nRec;
  pIter->nKey = nRec;
  pIter->aKey = &pIter->aAlloc[iOff];
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
  sqlite3_file *pFile,            /* File that the PMA is stored in */
  i64 iStart,                     /* Start offset in pFile */
  i64 iEof,                       /* 1 byte past the end of the PMA in pFile */
  VdbeSorterIter *pIter           /* Iterator to populate */
){
  assert( iEof>iStart );
  assert( pIter->aAlloc==0 );
  pIter->pFile = pFile;
  pIter->iEof = iEof;
  pIter->iReadOff = iStart;
  pIter->nAlloc = 128;
  pIter->aAlloc = (u8 *)sqlite3DbMallocRaw(db, pIter->nAlloc);
  if( !pIter->aAlloc ) return SQLITE_NOMEM;
  return vdbeSorterIterNext(db, pIter);
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
    char aSpace[150];
    UnpackedRecord *r1;

    r1 = sqlite3VdbeRecordUnpack(
        pCsr->pKeyInfo, p1->nKey, p1->aKey, aSpace, sizeof(aSpace)
    );
    if( r1==0 ) return SQLITE_NOMEM;

    if( sqlite3VdbeRecordCompare(p2->nKey, p2->aKey, r1)>=0 ){
      iRes = i1;
    }else{
      iRes = i2;
    }
    sqlite3VdbeDeleteUnpackedRecord(r1);
  }

  pSorter->aTree[iOut] = iRes;
  return SQLITE_OK;
}

/*
** Initialize the temporary index cursor just opened as a sorter cursor.
*/
int sqlite3VdbeSorterInit(sqlite3 *db, VdbeCursor *pCsr){
  VdbeSorter *pSorter;            /* Allocated sorter object */

  /* Cursor must be a temp cursor and not open on an intkey table */
  assert( pCsr->pKeyInfo && pCsr->pBt );

  pSorter = sqlite3DbMallocZero(db, sizeof(VdbeSorter));
  if( !pSorter ) return SQLITE_NOMEM;
  pCsr->pSorter = pSorter;
  return SQLITE_OK;
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
    sqlite3DbFree(db, pSorter->aOffset);
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
      SQLITE_OPEN_TEMP_DB   |
      SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE |
      SQLITE_OPEN_EXCLUSIVE | SQLITE_OPEN_DELETEONCLOSE, &dummy
  );
}

/*
** Write the current contents of the b-tree to a PMA. Return SQLITE_OK
** if successful, or an SQLite error code otherwise.
*/
static int sorterBtreeToPma(sqlite3 *db, VdbeCursor *pCsr){
  int rc = SQLITE_OK;             /* Return code */
  VdbeSorter *pSorter = pCsr->pSorter;
  i64 iWriteOff = pSorter->iWriteOff;
  int res = 0;
  void *aMalloc = 0;
  int nMalloc = 0;

  rc = sqlite3BtreeFirst(pCsr->pCursor, &res);
  if( rc!=SQLITE_OK || res ) return rc;

  /* If the first temporary PMA file has not been opened, open it now. */
  if( pSorter->pTemp1==0 ){
    rc = vdbeSorterOpenTempFile(db, &pSorter->pTemp1);
    assert( rc!=SQLITE_OK || pSorter->pTemp1 );
    assert( pSorter->iWriteOff==0 );
    assert( pSorter->nOffset==0 );
    assert( pSorter->aOffset==0 );
  }

  if( rc==SQLITE_OK ){

    for(
      rc = vdbeSorterAppendOffset(db, pSorter, iWriteOff);
      rc==SQLITE_OK && res==0;
      rc = sqlite3BtreeNext(pCsr->pCursor, &res)
    ){
      i64 nKey;                   /* Size of this key in bytes */
      u8 aVarint[9];              /* Buffer containing varint(nKey) */
      int nVar;                   /* Number of bytes in aVarint[] used */

      (void)sqlite3BtreeKeySize(pCsr->pCursor, &nKey);
      nVar = sqlite3PutVarint(aVarint, nKey);
      
      /* Write the size of the record in bytes to the output file */
      rc = sqlite3OsWrite(pSorter->pTemp1, aVarint, nVar, iWriteOff);
      iWriteOff += nVar;

      /* Make sure the aMalloc[] buffer is large enough for the record */
      if( rc==SQLITE_OK && nKey>nMalloc ){
        aMalloc = sqlite3DbReallocOrFree(db, aMalloc, nKey);
        if( !aMalloc ){
          rc = SQLITE_NOMEM;
        }
      }

      /* Write the record itself to the output file */
      if( rc==SQLITE_OK ){
        rc = sqlite3BtreeKey(pCsr->pCursor, 0, nKey, aMalloc);
        if( rc==SQLITE_OK ){
          rc = sqlite3OsWrite(pSorter->pTemp1, aMalloc, nKey, iWriteOff);
          iWriteOff += nKey;
        }
      }

      if( rc!=SQLITE_OK ) break;
    }

    pSorter->iWriteOff = iWriteOff;
    sqlite3DbFree(db, aMalloc);
  }

  return rc;
}

/*
** This function is called on a sorter cursor before each row is inserted.
** If the current b-tree being constructed is already considered "full",
** a new tree is started.
*/
int sqlite3VdbeSorterWrite(sqlite3 *db, VdbeCursor *pCsr){
  int rc = SQLITE_OK;             /* Return code */
  VdbeSorter *pSorter = pCsr->pSorter;
  if( pSorter ){
    Pager *pPager = sqlite3BtreePager(pCsr->pBt);
    int nPage;                    /* Current size of temporary file in pages */

    sqlite3PagerPagecount(pPager, &nPage);

    /* If pSorter->nWorking is still zero, but the temporary file has been
    ** created in the file-system, then the most recent insert into the
    ** current b-tree segment probably caused the cache to overflow (it is
    ** also possible that sqlite3_release_memory() was called). So set the
    ** size of the working set to a little less than the current size of the 
    ** file in pages.  */
    if( pSorter->nWorking==0 && sqlite3PagerFile(pPager)->pMethods ){
      pSorter->nWorking = nPage-5;
      if( pSorter->nWorking<SORTER_MIN_SEGMENT_SIZE ){
        pSorter->nWorking = SORTER_MIN_SEGMENT_SIZE;
      }
    }

    /* If the number of pages used by the current b-tree segment is greater
    ** than the size of the working set (VdbeSorter.nWorking), start a new
    ** segment b-tree.  */
    if( pSorter->nWorking && nPage>=pSorter->nWorking ){
      BtCursor *p = pCsr->pCursor;/* Cursor structure to close and reopen */
      int iRoot;                  /* Root page of new tree */

      /* Copy the current contents of the b-tree into a PMA in sorted order.
      ** Close the currently open b-tree cursor. */
      rc = sorterBtreeToPma(db, pCsr);
      sqlite3BtreeCloseCursor(p);

      if( rc==SQLITE_OK ){
        rc = sqlite3BtreeDropTable(pCsr->pBt, 2, 0);
#ifdef SQLITE_DEBUG
        sqlite3PagerPagecount(pPager, &nPage);
        assert( rc!=SQLITE_OK || nPage==1 );
#endif
      }
      if( rc==SQLITE_OK ){
        rc = sqlite3BtreeCreateTable(pCsr->pBt, &iRoot, BTREE_BLOBKEY);
      }
      if( rc==SQLITE_OK ){
        assert( iRoot==2 );
        rc = sqlite3BtreeCursor(pCsr->pBt, iRoot, 1, pCsr->pKeyInfo, p);
      }
    }
  }
  return rc;
}

/*
** Helper function for sqlite3VdbeSorterRewind().
*/
static int vdbeSorterInitMerge(
  sqlite3 *db,
  VdbeCursor *pCsr,
  int iFirst,
  int *piNext
){
  VdbeSorter *pSorter = pCsr->pSorter;
  int rc = SQLITE_OK;
  int i;
  int N = 2;
  int nIter;                      /* Number of iterators to initialize. */

  nIter = pSorter->nOffset - iFirst;
  if( nIter>SORTER_MAX_MERGE_COUNT ){
    nIter = SORTER_MAX_MERGE_COUNT;
  }
  assert( nIter>0 );
  while( N<nIter ) N += N;

  /* Allocate aIter[] and aTree[], if required. */
  if( pSorter->aIter==0 ){
    int nByte = N * (sizeof(int) + sizeof(VdbeSorterIter));
    pSorter->aIter = (VdbeSorterIter *)sqlite3DbMallocZero(db, nByte);
    if( !pSorter->aIter ) return SQLITE_NOMEM;
    pSorter->aTree = (int *)&pSorter->aIter[N];
  }

  /* Initialize as many iterators as possible. */
  for(i=iFirst; 
      rc==SQLITE_OK && i<pSorter->nOffset && (i-iFirst)<SORTER_MAX_MERGE_COUNT; 
      i++
  ){
    int iIter = i - iFirst;

    if( rc==SQLITE_OK ){
      VdbeSorterIter *pIter = &pSorter->aIter[iIter];
      i64 iStart = pSorter->aOffset[i];
      i64 iEof;
      if( i==(pSorter->nOffset-1) ){
        iEof = pSorter->iWriteOff;
      }else{
        iEof = pSorter->aOffset[i+1];
      }
      rc = vdbeSorterIterInit(db, pSorter->pTemp1, iStart, iEof, pIter);
    }
  }
  *piNext = i;

  assert( i>iFirst );
  pSorter->nTree = N;

  /* Populate the aTree[] array. */
  for(i=N-1; rc==SQLITE_OK && i>0; i--){
    rc = vdbeSorterDoCompare(pCsr, i);
  }

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

  assert( pSorter );

  /* Write the current b-tree to a PMA. Close the b-tree cursor. */
  rc = sorterBtreeToPma(db, pCsr);
  sqlite3BtreeCloseCursor(pCsr->pCursor);
  if( rc!=SQLITE_OK ) return rc;
  if( pSorter->nOffset==0 ){
    *pbEof = 1;
    return SQLITE_OK;
  }

  while( rc==SQLITE_OK ){
    int iNext = 0;                /* Index of next segment to open */
    int iNew = 0;                 /* Index of new, merged, PMA */

    do {

      /* This call configures iterators for merging. */
      rc = vdbeSorterInitMerge(db, pCsr, iNext, &iNext);
      assert( iNext>0 );
      assert( rc!=SQLITE_OK || pSorter->aIter[ pSorter->aTree[1] ].pFile );

      if( rc==SQLITE_OK && (iNew>0 || iNext<pSorter->nOffset) ){
        int bEof = 0;

        if( pTemp2==0 ){
          rc = vdbeSorterOpenTempFile(db, &pTemp2);
        }
        if( rc==SQLITE_OK ){
          pSorter->aOffset[iNew] = iWrite2;
        }

        while( rc==SQLITE_OK && bEof==0 ){
          int nByte;
          VdbeSorterIter *pIter = &pSorter->aIter[ pSorter->aTree[1] ];
          assert( pIter->pFile );
          nByte = pIter->nKey + sqlite3VarintLen(pIter->nKey);
          rc = sqlite3OsWrite(pTemp2, pIter->aAlloc, nByte, iWrite2);
          iWrite2 += nByte;
          if( rc==SQLITE_OK ){
            rc = sqlite3VdbeSorterNext(db, pCsr, &bEof);
          }
        }
        iNew++;
      }
    }while( rc==SQLITE_OK && iNext<pSorter->nOffset );

    if( iNew==0 ){
      break;
    }else{
      sqlite3_file *pTmp = pSorter->pTemp1;
      pSorter->nOffset = iNew;
      pSorter->pTemp1 = pTemp2;
      pTemp2 = pTmp;
      pSorter->iWriteOff = iWrite2;
      iWrite2 = 0;
    }
  }

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
  int iPrev = pSorter->aTree[1];  /* Index of iterator to advance */
  int i;                          /* Index of aTree[] to recalculate */
  int rc;                         /* Return code */

  rc = vdbeSorterIterNext(db, &pSorter->aIter[iPrev]);
  for(i=(pSorter->nTree+iPrev)/2; rc==SQLITE_OK && i>0; i=i/2){
    rc = vdbeSorterDoCompare(pCsr, i);
  }

  *pbEof = (pSorter->aIter[pSorter->aTree[1]].pFile==0);
  return rc;
}

/*
** Copy the current sorter key into the memory cell pOut.
*/
int sqlite3VdbeSorterRowkey(sqlite3 *db, VdbeCursor *pCsr, Mem *pOut){
  VdbeSorter *pSorter = pCsr->pSorter;
  VdbeSorterIter *pIter;

  pIter = &pSorter->aIter[ pSorter->aTree[1] ];
  if( sqlite3VdbeMemGrow(pOut, pIter->nKey, 0) ){
    return SQLITE_NOMEM;
  }
  pOut->n = pIter->nKey;
  MemSetTypeFlag(pOut, MEM_Blob);
  memcpy(pOut->z, pIter->aKey, pIter->nKey);

  return SQLITE_OK;
}


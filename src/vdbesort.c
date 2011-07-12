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
** The aIter[] and aTree[] arrays are used to iterate through the sorter
** contents after it has been populated. To iterate through the sorter
** contents, the contents of the nRoot b-trees must be incrementally merged. 
**
** The first nRoot elements of the aIter[] array contain cursors open 
** on each of the b-trees. An aIter[] element either points to a valid
** key or else is at EOF. For the purposes of the paragraphs below, we
** assume that the array is actually N elements in size, where N is the
** smallest power of 2 greater to or equal to nRoot. The extra aIter[]
** elements are treated as if they are empty trees (always at EOF).
**
** The aTree[] array is N elements in size. The value of N is stored in
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
  int nPage;                      /* Pages in file when current tree started */
  int nRoot;                      /* Total number of segment b-trees */
  int *aRoot;                     /* Array containing root pages */

  int nAlloc;                     /* Allocated size of aIter[] and aTree[] */
  int nTree;                      /* Used size of aTree/aIter (power of 2) */
  VdbeSorterIter *aIter;          /* Array of iterators to merge */
  int *aTree;                     /* Current state of incremental merge */
};

/*
** The following type is a simple wrapper around a BtCursor. It caches the
** current key in variables nKey/aKey. If possible, aKey points to memory
** managed by the BtCursor object. In this case variable bFree is zero.
** Otherwise, aKey[] may point to a block of memory allocated using
** sqlite3DbMalloc(). In this case, bFree is non-zero.
*/
struct VdbeSorterIter {
  BtCursor *pCsr;                 /* Cursor open on b-tree */
  int bFree;                      /* True if aKey should be freed */
  int nKey;                       /* Number of bytes in key */
  u8 *aKey;                       /* Pointer to current key */
};

/* Minimum allowable value for the VdbeSorter.nWorking variable */
#define SORTER_MIN_SEGMENT_SIZE 10

/*
** Append integer iRoot to the VdbeSorter.aRoot[] array of the sorter object
** passed as the second argument. SQLITE_NOMEM is returned if an OOM error
** is encountered, or SQLITE_OK if no error occurs.
**
** TODO: The aRoot[] array may grow indefinitely. Fix this.
*/
static int vdbeSorterAppendRoot(sqlite3 *db, VdbeSorter *p, int iRoot){
  int *aNew;                      /* New VdbeSorter.aRoot[] array */

  aNew = sqlite3DbRealloc(db, p->aRoot, (p->nRoot+1)*sizeof(int));
  if( !aNew ) return SQLITE_NOMEM;
  aNew[p->nRoot] = iRoot;
  p->nRoot++;
  p->aRoot = aNew;
  return SQLITE_OK;
}

/*
** Close any cursor and free all memory belonging to the VdbeSorterIter
** object passed as the second argument. All structure fields are set
** to zero before returning.
*/
static void vdbeSorterIterZero(sqlite3 *db, VdbeSorterIter *pIter){
  if( pIter->bFree ){
    sqlite3DbFree(db, pIter->aKey);
  }
  if( pIter->pCsr ){
    sqlite3BtreeCloseCursor(pIter->pCsr);
    sqlite3DbFree(db, pIter->pCsr);
  }
  memset(pIter, 0, sizeof(VdbeSorterIter));
}

/*
** Fetch the current key pointed to by the b-tree cursor managed by pIter
** into variables VdbeSorterIter.aKey and VdbeSorterIter.nKey. Return
** SQLITE_OK if no error occurs, or an SQLite error code otherwise.
*/
static int vdbeSorterIterLoadkey(sqlite3 *db, VdbeSorterIter *pIter){
  int rc = SQLITE_OK;
  assert( pIter->pCsr );
  if( sqlite3BtreeEof(pIter->pCsr) ){
    vdbeSorterIterZero(db, pIter);
  }else{
    i64 nByte64;
    sqlite3BtreeKeySize(pIter->pCsr, &nByte64);

    if( pIter->bFree ){
      sqlite3DbFree(db, pIter->aKey);
      pIter->aKey = 0;
    }

    pIter->nKey = nByte64;
    pIter->aKey = sqlite3DbMallocRaw(db, pIter->nKey);
    pIter->bFree = 1;
    if( pIter->aKey==0 ){
      rc = SQLITE_NOMEM;
    }else{
      rc = sqlite3BtreeKey(pIter->pCsr, 0, pIter->nKey, pIter->aKey);
    }

  }
  return rc;
}

/*
** Initialize iterator pIter to scan through the b-tree with root page
** iRoot. This function leaves the iterator pointing to the first key
** in the b-tree (or EOF if the b-tree is empty).
*/
static int vdbeSorterIterInit(
  sqlite3 *db,                    /* Database handle */
  VdbeCursor *pCsr,               /* Vdbe cursor handle */
  int iRoot,                      /* Root page of b-tree to iterate */
  VdbeSorterIter *pIter           /* Pointer to iterator to initialize */
){
  VdbeSorter *pSorter = pCsr->pSorter;
  int rc;

  pIter->pCsr = (BtCursor *)sqlite3DbMallocZero(db, sqlite3BtreeCursorSize());
  if( !pIter->pCsr ){
    rc = SQLITE_NOMEM;
  }else{
    rc = sqlite3BtreeCursor(pCsr->pBt, iRoot, 1, pCsr->pKeyInfo, pIter->pCsr);
  }
  if( rc==SQLITE_OK ){
    int bDummy;
    rc = sqlite3BtreeFirst(pIter->pCsr, &bDummy);
  }
  if( rc==SQLITE_OK ){
    rc = vdbeSorterIterLoadkey(db, pIter);
  }

  return rc;
}

/*
** Advance iterator pIter to the next key in its b-tree. 
*/
static int vdbeSorterIterNext(
  sqlite3 *db, 
  VdbeCursor *pCsr, 
  VdbeSorterIter *pIter
){
  int rc;
  int bDummy;
  VdbeSorter *pSorter = pCsr->pSorter;

  rc = sqlite3BtreeNext(pIter->pCsr, &bDummy);
  if( rc==SQLITE_OK ){
    rc = vdbeSorterIterLoadkey(db, pIter);
  }

  return rc;
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

  if( p1->pCsr==0 ){
    iRes = i2;
  }else if( p2->pCsr==0 ){
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
  int rc;                         /* Return code */
  VdbeSorter *pSorter;            /* Allocated sorter object */

  /* Cursor must be a temp cursor and not open on an intkey table */
  assert( pCsr->pKeyInfo && pCsr->pBt );

  pSorter = sqlite3DbMallocZero(db, sizeof(VdbeSorter));
  if( !pSorter ) return SQLITE_NOMEM;
  pCsr->pSorter = pSorter;

  rc = vdbeSorterAppendRoot(db, pSorter, 2);
  if( rc!=SQLITE_OK ){
    sqlite3VdbeSorterClose(db, pCsr);
  }
  return rc;
}

/*
** Free any cursor components allocated by sqlite3VdbeSorterXXX routines.
*/
void sqlite3VdbeSorterClose(sqlite3 *db, VdbeCursor *pCsr){
  VdbeSorter *pSorter = pCsr->pSorter;
  if( pSorter ){
    sqlite3DbFree(db, pSorter->aRoot);
    if( pSorter->aIter ){
      int i;
      for(i=0; i<pSorter->nRoot; i++){
        vdbeSorterIterZero(db, &pSorter->aIter[i]);
      }
      sqlite3DbFree(db, pSorter->aIter);
      sqlite3DbFree(db, pSorter->aTree);
    }
    sqlite3DbFree(db, pSorter);
    pCsr->pSorter = 0;
  }
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
    if( pSorter->nWorking && nPage>=(pSorter->nPage + pSorter->nWorking) ){
      BtCursor *p = pCsr->pCursor;/* Cursor structure to close and reopen */
      int iRoot;                  /* Root page of new tree */
      sqlite3BtreeCloseCursor(p);
      rc = sqlite3BtreeCreateTable(pCsr->pBt, &iRoot, BTREE_BLOBKEY);
      if( rc==SQLITE_OK ){
        rc = vdbeSorterAppendRoot(db, pSorter, iRoot);
      }
      if( rc==SQLITE_OK ){
        rc = sqlite3BtreeCursor(pCsr->pBt, iRoot, 1, pCsr->pKeyInfo, p);
      }
      pSorter->nPage = nPage;
    }
  }
  return rc;
}

/*
** Extend the pSorter->aIter[] and pSorter->aTree[] arrays using DbRealloc().
** Return SQLITE_OK if successful, or SQLITE_NOMEM otherwise.
*/
static int vdbeSorterGrowArrays(sqlite3* db, VdbeSorter *pSorter){
  int *aTree;                     /* New aTree[] allocation */
  VdbeSorterIter *aIter;          /* New aIter[] allocation */
  int nOld = pSorter->nAlloc;     /* Current size of arrays */
  int nNew = (nOld?nOld*2:64);    /* Size of arrays after reallocation */

  /* Realloc aTree[]. */
  aTree = sqlite3DbRealloc(db, pSorter->aTree, sizeof(int)*nNew);
  if( !aTree ) return SQLITE_NOMEM;
  memset(&aTree[nOld], 0, (nNew-nOld) * sizeof(int));
  pSorter->aTree = aTree;

  /* Realloc aIter[]. */
  aIter = sqlite3DbRealloc(db, pSorter->aIter, sizeof(VdbeSorterIter)*nNew);
  if( !aIter ) return SQLITE_NOMEM;
  memset(&aIter[nOld], 0, (nNew-nOld) * sizeof(VdbeSorterIter));
  pSorter->aIter = aIter;

  /* Set VdbeSorter.nAlloc to the new size of the arrays and return OK. */
  pSorter->nAlloc = nNew;
  return SQLITE_OK;
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
  Pager *pPager = sqlite3BtreePager(pCsr->pBt);
  VdbeSorter *pSorter = pCsr->pSorter;
  int rc = SQLITE_OK;
  int i;
  int nMaxRef = (pSorter->nWorking * 9/10);
  int N = 2;

  /* Initialize as many iterators as possible. */
  for(i=iFirst; rc==SQLITE_OK && i<pSorter->nRoot; i++){
    int iIter = i - iFirst;

    assert( iIter<=pSorter->nAlloc );
    if( iIter==pSorter->nAlloc ){
      rc = vdbeSorterGrowArrays(db, pSorter);
    }

    if( rc==SQLITE_OK ){
      VdbeSorterIter *pIter = &pSorter->aIter[iIter];
      rc = vdbeSorterIterInit(db, pCsr, pSorter->aRoot[i], pIter);
      if( i>iFirst+1 ){
        int nRef = sqlite3PagerRefcount(pPager) + (i+1-iFirst);
        if( nRef>=nMaxRef ){
          i++;
          break;
        }
      }
    }
  }
  *piNext = i;

  while( (i-iFirst)>N ) N += N;
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
  int rc = SQLITE_OK;             /* Return code */
  int N;
  int i;

  VdbeSorter *pSorter = pCsr->pSorter;
  BtCursor *p = pCsr->pCursor;    /* Cursor structure */

  assert( pSorter );
  sqlite3BtreeCloseCursor(p);

  while( rc==SQLITE_OK ){
    int iNext = 0;                /* Index of next segment to open */
    int iRoot = 0;                /* aRoot[] slot if merging to a new segment */

    do {
      rc = vdbeSorterInitMerge(db, pCsr, iNext, &iNext);

      if( rc==SQLITE_OK && (iRoot>0 || iNext<pSorter->nRoot) ){
        int pgno;
        int bEof = 0;
        rc = sqlite3BtreeCreateTable(pCsr->pBt, &pgno, BTREE_BLOBKEY);
        if( rc==SQLITE_OK ){
          pSorter->aRoot[iRoot] = pgno;
          rc = sqlite3BtreeCursor(pCsr->pBt, pgno, 1, pCsr->pKeyInfo, p);
        }

        while( rc==SQLITE_OK && bEof==0 ){
          VdbeSorterIter *pIter = &pSorter->aIter[ pSorter->aTree[1] ];
          rc = sqlite3BtreeInsert(p, pIter->aKey, pIter->nKey, 0, 0, 0, 1, 0);
          if( rc==SQLITE_OK ){
            rc = sqlite3VdbeSorterNext(db, pCsr, &bEof);
          }
        }
        sqlite3BtreeCloseCursor(p);
        iRoot++;
      }
    } while( rc==SQLITE_OK && iNext<pSorter->nRoot );

    if( iRoot==0 ) break;
    pSorter->nRoot = iRoot;
  }

  *pbEof = (pSorter->aIter[pSorter->aTree[1]].pCsr==0);
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

  rc = vdbeSorterIterNext(db, pCsr, &pSorter->aIter[iPrev]);
  for(i=(pSorter->nTree+iPrev)/2; rc==SQLITE_OK && i>0; i=i/2){
    rc = vdbeSorterDoCompare(pCsr, i);
  }

  *pbEof = (pSorter->aIter[pSorter->aTree[1]].pCsr==0);
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


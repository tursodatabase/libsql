/*
** 2011-07-09
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
** a VdbeCursor to sort large numbers of keys for CREATE TABLE statements
** or by SELECT statements with ORDER BY clauses that cannot be satisfied
** using indexes and without LIMIT clauses.
**
** The VdbeSorter object implements a multi-threaded external merge sort
** algorithm that is efficient even if the number of element being sorted
** exceeds the available memory.
**
** Here is the (internal, non-API) interface between this module and the
** rest of the SQLite system:
**
**    sqlite3VdbeSorterInit()       Create a new VdbeSorter object.
**
**    sqlite3VdbeSorterWrite()      Add a single new row to the VdbeSorter
**                                  object.  The row is a binary blob in the
**                                  OP_MakeRecord format that contains both
**                                  the ORDER BY key columns and result columns
**                                  in the case of a SELECT w/ ORDER BY, or
**                                  the complete record for an index entry
**                                  in the case of a CREATE INDEX.
**
**    sqlite3VdbeSorterRewind()     Sort all content previously added.
**                                  Position the read cursor on the
**                                  first sorted element.
**
**    sqlite3VdbeSorterNext()       Advance the read cursor to the next sorted
**                                  element.
**
**    sqlite3VdbeSorterRowkey()     Return the complete binary blob for the
**                                  row currently under the read cursor.
**
**    sqlite3VdbeSorterCompare()    Compare the binary blob for the row
**                                  currently under the read cursor against
**                                  another binary blob X and report if
**                                  X is strictly less than the read cursor.
**                                  Used to enforce uniqueness in a
**                                  CREATE UNIQUE INDEX statement.
**
**    sqlite3VdbeSorterClose()      Close the VdbeSorter object and reclaim
**                                  all resources.
**
**    sqlite3VdbeSorterReset()      Refurbish the VdbeSorter for reuse.  This
**                                  is like Close() followed by Init() only
**                                  much faster.
**
** The interfaces above must be called in a particular order.  Write() can 
** only occur in between Init()/Reset() and Rewind().  Next(), Rowkey(), and
** Compare() can only occur in between Rewind() and Close()/Reset().
**
** Algorithm:
**
** Records to be sorted are initially held in memory, in the order in
** which they arrive from Write().  When the amount of memory needed exceeds
** a threshold, all in-memory records are sorted and then appended to
** a temporary file as a "Packed-Memory-Array" or "PMA" and the memory is
** reset.  There is a single temporary file used for all PMAs.  The PMAs
** are packed one after another in the file.  The VdbeSorter object keeps
** track of the number of PMAs written.
**
** When the Rewind() is seen, any records still held in memory are sorted.
** If no PMAs have been written (if all records are still held in memory)
** then subsequent Rowkey(), Next(), and Compare() operations work directly
** from memory.  But if PMAs have been written things get a little more
** complicated.
**
** When Rewind() is seen after PMAs have been written, any records still
** in memory are sorted and written as a final PMA.  Then all the PMAs
** are merged together into a single massive PMA that Next(), Rowkey(),
** and Compare() walk to extract the records in sorted order.
**
** If SQLITE_MAX_WORKER_THREADS is non-zero, various steps of the above
** algorithm might be performed in parallel by separate threads.  Threads
** are only used when one or more PMA spill to disk.  If the sort is small
** enough to fit entirely in memory, everything happens on the main thread.
*/
#include "sqliteInt.h"
#include "vdbeInt.h"

/*
** Private objects used by the sorter
*/
typedef struct MergeEngine MergeEngine;     /* Merge PMAs together */
typedef struct PmaReader PmaReader;         /* Incrementally read one PMA */
typedef struct PmaWriter PmaWriter;         /* Incrementally write on PMA */
typedef struct SorterRecord SorterRecord;   /* A record being sorted */
typedef struct SortSubtask SortSubtask;     /* A sub-task in the sort process */

typedef struct SortList SortList;
typedef struct SortFile SortFile;
typedef struct SortLevel SortLevel;


/*
** A file containing zero or more PMAs.
*/
struct SortFile {
  sqlite3_file *pFd;              /* File handle */
  i64 iOff;                       /* Current write offset */
  i64 nByte;                      /* Actual size of file */
  int nPMA;                       /* Number of PMA currently in file */
};

/*
** A list of records.
*/
struct SortList {
  SorterRecord *pRecord;          /* List of records for pTask to sort */
  int nInMemory;                  /* Expected size of PMA based on pList */
  u8 *aMemory;                    /* Records memory (or NULL) */
};

struct SortLevel {
  SortSubtask *pTask;             /* Sorter task this level is a part of */
  SQLiteThread *pThread;          /* Thread handle, or NULL */
  int bDone;                      /* Set to true by pThread when finished */
  union {
    SortFile f;                   /* Input for level 1 and greater */
    SortList l;                   /* Input for level 0 */
  } in;
  SortFile out;                   /* Level storage */
  SortLevel *pNext;               /* Next level (containing larger PMAs) */
  UnpackedRecord *pUnpacked;      /* Space to unpack a record */
};

/*
** Sorting is divided up into smaller subtasks.  Each subtask is controlled
** by an instance of this object. A Subtask might run in either the main thread
** or in a background thread.
**
** Exactly VdbeSorter.nTask instances of this object are allocated
** as part of each VdbeSorter object. Instances are never allocated any other
** way. VdbeSorter.nTask is set to the number of worker threads allowed
** (see SQLITE_CONFIG_WORKER_THREADS) plus one (the main thread).
**
** When a background thread is launched to perform work, SortSubtask.bDone
** is set to 0 and the SortSubtask.pTask variable set to point to the
** thread handle. SortSubtask.bDone is set to 1 (to indicate to the main
** thread that joining SortSubtask.pTask will not block) before the thread
** exits. SortSubtask.pTask and bDone are always cleared after the 
** background thread has been joined.
**
** One object (specifically, VdbeSorter.aTask[VdbeSorter.nTask-1])
** is reserved for the foreground thread.
**
** The nature of the work performed is determined by SortSubtask.eWork,
** as follows:
**
**   SORT_SUBTASK_SORT:
**     Sort the linked list of records at SortSubtask.pList.
**
**   SORT_SUBTASK_TO_PMA:
**     Sort the linked list of records at SortSubtask.pList, and write
**     the results to a new PMA in temp file SortSubtask.pTemp1. Open
**     the temp file if it is not already open.
**
**   SORT_SUBTASK_CONS:
**     Merge existing PMAs until SortSubtask.nConsolidate or fewer
**     remain in temp file SortSubtask.pTemp1.
*/
struct SortSubtask {
  int iId;                        /* Sub-task id */
  sqlite3 *db;                    /* Database connection */
  VdbeSorter *pSorter;            /* Sorter that owns this object */
  KeyInfo *pKeyInfo;              /* How to compare records */
  int pgsz;                       /* Main database page size */
  int nConsolidate;               /* For consolidation, max final PMAs */
  SortLevel *pLevel;              /* PMA level 0 */
};


/*
** The MergeEngine object is used to combine two or more smaller PMAs into
** one big PMA using a merge operation.  Separate PMAs all need to be
** combined into one big PMA in order to be able to step through the sorted
** records in order.
**
** The aIter[] array contains a PmaReader object for each of the PMAs being
** merged.  An aIter[] object either points to a valid key or else is at EOF.
** For the purposes of the paragraphs below, we assume that the array is
** actually N elements in size, where N is the smallest power of 2 greater
** to or equal to the number of PMAs being merged. The extra aIter[] elements
** are treated as if they are empty (always at EOF).
**
** The aTree[] array is also N elements in size. The value of N is stored in
** the MergeEngine.nTree variable.
**
** The final (N/2) elements of aTree[] contain the results of comparing
** pairs of PMA keys together. Element i contains the result of 
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
struct MergeEngine {
  int nTree;                 /* Used size of aTree/aIter (power of 2) */
  int *aTree;                /* Current state of incremental merge */
  PmaReader *aIter;          /* Array of iterators to merge data from */
};

/*
** Main sorter structure. A single instance of this is allocated for each 
** sorter cursor created by the VDBE.
*/
struct VdbeSorter {
  int mnPmaSize;                  /* Minimum PMA size, in bytes */
  int mxPmaSize;                  /* Maximum PMA size, in bytes.  0==no limit */
  int bUsePMA;                    /* True if one or more PMAs created */
  MergeEngine *pMerger;           /* For final merge of PMAs (by caller) */ 
  UnpackedRecord *pUnpacked;      /* Used by sqlite3VdbeSorterCompare */
  int iMemory;                    /* Offset of free byte in list.aMemory */
  int nMemory;                    /* Size of list.aMemory allocation in bytes */
  SortList list;                  /* In memory records */
  int iPrev;                      /* Previous PMA flushed via task iPrev */
  int nTask;                      /* Size of aTask[] array */
  SortSubtask aTask[1];           /* One or more subtasks */
};

/*
** An instance of the following object is used to read records out of a
** PMA, in sorted order.  The next key to be read is cached in nKey/aKey.
** pFile==0 at EOF.
*/
struct PmaReader {
  i64 iReadOff;                   /* Current read offset */
  i64 iEof;                       /* 1 byte past EOF for this iterator */
  int nAlloc;                     /* Bytes of space at aAlloc */
  int nKey;                       /* Number of bytes in key */
  sqlite3_file *pFile;            /* File iterator is reading from */
  u8 *aAlloc;                     /* Allocated space */
  u8 *aKey;                       /* Pointer to current key */
  u8 *aBuffer;                    /* Current read buffer */
  int nBuffer;                    /* Size of read buffer in bytes */
  u8 *aMap;                       /* Pointer to mapping of entire file */
};

/*
** An instance of this object is used for writing a PMA.
**
** The PMA is written one record at a time.  Each record is of an arbitrary
** size.  But I/O is more efficient if it occurs in page-sized blocks where
** each block is aligned on a page boundary.  This object caches writes to
** the PMA so that aligned, page-size blocks are written.
*/
struct PmaWriter {
  int eFWErr;                     /* Non-zero if in an error state */
  u8 *aBuffer;                    /* Pointer to write buffer */
  int nBuffer;                    /* Size of write buffer in bytes */
  int iBufStart;                  /* First byte of buffer to write */
  int iBufEnd;                    /* Last byte of buffer to write */
  i64 iWriteOff;                  /* Offset of start of buffer in file */
  sqlite3_file *pFile;            /* File to write to */
};

/*
** This object is the header on a single record while that record is being
** held in memory and prior to being written out as part of a PMA.
**
** How the linked list is connected depends on how memory is being managed
** by this module. If using a separate allocation for each in-memory record
** (VdbeSorter.aMemory==0), then the list is always connected using the
** SorterRecord.u.pNext pointers.
**
** Or, if using the single large allocation method (VdbeSorter.aMemory!=0),
** then while records are being accumulated the list is linked using the
** SorterRecord.u.iNext offset. This is because the aMemory[] array may
** be sqlite3Realloc()ed while records are being accumulated. Once the VM
** has finished passing records to the sorter, or when the in-memory buffer
** is full, the list is sorted. As part of the sorting process, it is
** converted to use the SorterRecord.u.pNext pointers. See function
** vdbeSorterSort() for details.
*/
struct SorterRecord {
  int nVal;                       /* Size of the record in bytes */
  union {
    SorterRecord *pNext;          /* Pointer to next record in list */
    int iNext;                    /* Offset within aMemory of next record */
  } u;
  /* The data for the record immediately follows this header */
};

/* Return a pointer to the buffer containing the record data for SorterRecord
** object p. Should be used as if:
**
**   void *SRVAL(SorterRecord *p) { return (void*)&p[1]; }
*/
#define SRVAL(p) ((void*)((SorterRecord*)(p) + 1))

/* The minimum PMA size is set to this value multiplied by the database
** page size in bytes.  */
#define SORTER_MIN_WORKING 10

/* Maximum number of PMAs that a single MergeEngine can merge */
#define SORTER_MAX_MERGE_COUNT 16

/*
** Free all memory belonging to the PmaReader object passed as the second
** argument. All structure fields are set to zero before returning.
*/
static void vdbePmaReaderClear(PmaReader *pIter){
  sqlite3_free(pIter->aAlloc);
  sqlite3_free(pIter->aBuffer);
  if( pIter->aMap ) sqlite3OsUnfetch(pIter->pFile, 0, pIter->aMap);
  memset(pIter, 0, sizeof(PmaReader));
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
static int vdbePmaReadBlob(
  PmaReader *p,                   /* Iterator */
  int nByte,                      /* Bytes of data to read */
  u8 **ppOut                      /* OUT: Pointer to buffer containing data */
){
  int iBuf;                       /* Offset within buffer to read from */
  int nAvail;                     /* Bytes of data available in buffer */

  if( p->aMap ){
    *ppOut = &p->aMap[p->iReadOff];
    p->iReadOff += nByte;
    return SQLITE_OK;
  }

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
      int rc;                     /* vdbePmaReadBlob() return code */
      int nCopy;                  /* Number of bytes to copy */
      u8 *aNext;                  /* Pointer to buffer to copy data from */

      nCopy = nRem;
      if( nRem>p->nBuffer ) nCopy = p->nBuffer;
      rc = vdbePmaReadBlob(p, nCopy, &aNext);
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
static int vdbePmaReadVarint(PmaReader *p, u64 *pnOut){
  int iBuf;

  if( p->aMap ){
    p->iReadOff += sqlite3GetVarint(&p->aMap[p->iReadOff], pnOut);
  }else{
    iBuf = p->iReadOff % p->nBuffer;
    if( iBuf && (p->nBuffer-iBuf)>=9 ){
      p->iReadOff += sqlite3GetVarint(&p->aBuffer[iBuf], pnOut);
    }else{
      u8 aVarint[16], *a;
      int i = 0, rc;
      do{
        rc = vdbePmaReadBlob(p, 1, &a);
        if( rc ) return rc;
        aVarint[(i++)&0xf] = a[0];
      }while( (a[0]&0x80)!=0 );
      sqlite3GetVarint(aVarint, pnOut);
    }
  }

  return SQLITE_OK;
}


/*
** Advance iterator pIter to the next key in its PMA. Return SQLITE_OK if
** no error occurs, or an SQLite error code if one does.
*/
static int vdbePmaReaderNext(PmaReader *pIter){
  int rc;                         /* Return Code */
  u64 nRec = 0;                   /* Size of record in bytes */

  if( pIter->iReadOff>=pIter->iEof ){
    /* This is an EOF condition */
    vdbePmaReaderClear(pIter);
    return SQLITE_OK;
  }

  rc = vdbePmaReadVarint(pIter, &nRec);
  if( rc==SQLITE_OK ){
    pIter->nKey = (int)nRec;
    rc = vdbePmaReadBlob(pIter, (int)nRec, &pIter->aKey);
  }

  return rc;
}

/*
** Initialize iterator pIter to scan through the PMA stored in file pFile
** starting at offset iStart and ending at offset iEof-1. This function 
** leaves the iterator pointing to the first key in the PMA (or EOF if the 
** PMA is empty).
*/
static int vdbePmaReaderInit(
  SortSubtask *pTask,             /* Thread context */
  SortFile *pFile,                /* File to read from */
  i64 iStart,                     /* Start offset in pTask->pTemp1 */
  PmaReader *pIter,               /* Iterator to populate */
  i64 *pnByte                     /* IN/OUT: Increment this value by PMA size */
){
  int rc = SQLITE_OK;
  int nBuf = pTask->pgsz;
  void *pMap = 0;                 /* Mapping of temp file */

  assert( pFile->iOff>iStart );
  assert( pIter->aAlloc==0 );
  assert( pIter->aBuffer==0 );
  pIter->pFile = pFile->pFd;
  pIter->iReadOff = iStart;
  pIter->nAlloc = 128;
  pIter->aAlloc = (u8*)sqlite3Malloc(pIter->nAlloc);
  if( pIter->aAlloc ){
    /* Try to xFetch() a mapping of the entire temp file. If this is possible,
    ** the PMA will be read via the mapping. Otherwise, use xRead().  */
    if( pFile->iOff<=(i64)(pTask->db->nMaxSorterMmap) ){
      rc = sqlite3OsFetch(pIter->pFile, 0, pFile->iOff, &pMap);
    }
  }else{
    rc = SQLITE_NOMEM;
  }

  if( rc==SQLITE_OK ){
    if( pMap ){
      pIter->aMap = (u8*)pMap;
    }else{
      pIter->nBuffer = nBuf;
      pIter->aBuffer = (u8*)sqlite3Malloc(nBuf);
      if( !pIter->aBuffer ){
        rc = SQLITE_NOMEM;
      }else{
        int iBuf = iStart % nBuf;
        if( iBuf ){
          int nRead = nBuf - iBuf;
          if( (iStart + nRead) > pFile->iOff ){
            nRead = (int)(pFile->iOff - iStart);
          }
          rc = sqlite3OsRead(
              pIter->pFile, &pIter->aBuffer[iBuf], nRead, iStart
          );
          assert( rc!=SQLITE_IOERR_SHORT_READ );
        }
      }
    }
  }

  if( rc==SQLITE_OK ){
    u64 nByte;                    /* Size of PMA in bytes */
    pIter->iEof = pFile->iOff;
    rc = vdbePmaReadVarint(pIter, &nByte);
    pIter->iEof = pIter->iReadOff + nByte;
    *pnByte += nByte;
  }

  if( rc==SQLITE_OK ){
    rc = vdbePmaReaderNext(pIter);
  }
  return rc;
}


/*
** Compare key1 (buffer pKey1, size nKey1 bytes) with key2 (buffer pKey2, 
** size nKey2 bytes). Use pKeyInfo for the collation sequences
** used by the comparison. Return the result of the comparison.
**
** Before returning, object pUnpacked is populated with the
** unpacked version of key2. Or, if pKey2 is passed a NULL pointer, then it 
** is assumed that the pUnpacked structure already contains the 
** unpacked key to use as key2.
**
** If an OOM error is encountered, (pUnpacked->error_rc) is set
** to SQLITE_NOMEM.
*/
static int vdbeSorterCompare(
  KeyInfo *pKeyInfo,
  UnpackedRecord *r2,
  const void *pKey1, int nKey1,   /* Left side of comparison */
  const void *pKey2, int nKey2    /* Right side of comparison */
){
  if( pKey2 ){
    sqlite3VdbeRecordUnpack(pKeyInfo, nKey2, pKey2, r2);
  }
  return sqlite3VdbeRecordCompare(nKey1, pKey1, r2, 0);
}

/*
** This function is called to compare two iterator keys when merging 
** multiple b-tree segments. Parameter iOut is the index of the aTree[] 
** value to recalculate.
*/
static int vdbeSorterDoCompare(
  SortLevel *pLvl, 
  MergeEngine *pMerger, 
  int iOut
){
  int i1;
  int i2;
  int iRes;
  PmaReader *p1;
  PmaReader *p2;

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
    assert( pLvl->pUnpacked!=0 );  /* allocated in vdbeSorterThread() */
    res = vdbeSorterCompare(pLvl->pTask->pKeyInfo, pLvl->pUnpacked,
        p1->aKey, p1->nKey, p2->aKey, p2->nKey
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
int sqlite3VdbeSorterInit(
  sqlite3 *db,                    /* Database connection (for malloc()) */
  int nField,                     /* Number of key fields in each record */
  VdbeCursor *pCsr                /* Cursor that holds the new sorter */
){
  int pgsz;                       /* Page size of main database */
  int i;                          /* Used to iterate through aTask[] */
  int mxCache;                    /* Cache size */
  VdbeSorter *pSorter;            /* The new sorter */
  KeyInfo *pKeyInfo;              /* Copy of pCsr->pKeyInfo with db==0 */
  int szKeyInfo;                  /* Size of pCsr->pKeyInfo in bytes */
  int sz;                         /* Size of pSorter in bytes */
  int rc = SQLITE_OK;
  int nWorker = (sqlite3GlobalConfig.bCoreMutex?sqlite3GlobalConfig.nWorker:0);

  assert( pCsr->pKeyInfo && pCsr->pBt==0 );
  szKeyInfo = sizeof(KeyInfo) + (pCsr->pKeyInfo->nField-1)*sizeof(CollSeq*);
  sz = sizeof(VdbeSorter) + nWorker * sizeof(SortSubtask);

  pSorter = (VdbeSorter*)sqlite3DbMallocZero(db, sz + szKeyInfo);
  pCsr->pSorter = pSorter;
  if( pSorter==0 ){
    rc = SQLITE_NOMEM;
  }else{
    pKeyInfo = (KeyInfo*)((u8*)pSorter + sz);
    memcpy(pKeyInfo, pCsr->pKeyInfo, szKeyInfo);
    pKeyInfo->db = 0;
    if( nField && nWorker==0 ) pKeyInfo->nField = nField;
    pgsz = sqlite3BtreeGetPageSize(db->aDb[0].pBt);

    pSorter->nTask = nWorker + 1;
    for(i=0; i<pSorter->nTask; i++){
      SortSubtask *pTask = &pSorter->aTask[i];
      pTask->pKeyInfo = pKeyInfo;
      pTask->pgsz = pgsz;
      pTask->db = db;
      pTask->pSorter = pSorter;
    }

    if( !sqlite3TempInMemory(db) ){
      pSorter->mnPmaSize = SORTER_MIN_WORKING * pgsz;
      mxCache = db->aDb[0].pSchema->cache_size;
      if( mxCache<SORTER_MIN_WORKING ) mxCache = SORTER_MIN_WORKING;
      pSorter->mxPmaSize = mxCache * pgsz;

      /* If the application is using memsys3 or memsys5, use a separate 
      ** allocation for each sort-key in memory. Otherwise, use a single big
      ** allocation at pSorter->aMemory for all sort-keys.  */
      if( sqlite3GlobalConfig.pHeap==0 ){
        assert( pSorter->iMemory==0 );
        pSorter->nMemory = pgsz;
        pSorter->list.aMemory = (u8*)sqlite3Malloc(pgsz);
        if( !pSorter->list.aMemory ) rc = SQLITE_NOMEM;
      }
    }
  }

  return rc;
}

/*
** Free the list of sorted records starting at pRecord.
*/
static void vdbeSorterRecordFree(sqlite3 *db, SorterRecord *pRecord){
  SorterRecord *p;
  SorterRecord *pNext;
  for(p=pRecord; p; p=pNext){
    pNext = p->u.pNext;
    sqlite3DbFree(db, p);
  }
}

/*
** Free all resources owned by the object indicated by argument pTask. 
** This does not include joining any outstanding threads. All fields of 
** *pTask are zeroed before returning.
*/
static void vdbeSortSubtaskCleanup(sqlite3 *db, SortSubtask *pTask){
  SortLevel *pLvl;
  SortLevel *pNext;
  for(pLvl=pTask->pLevel; pLvl; pLvl=pNext){
    pNext = pLvl->pNext;
    assert( pLvl->pThread==0 );
    if( pLvl==pTask->pLevel ){
      if( pLvl->in.l.aMemory==0 ){
        vdbeSorterRecordFree(0, pLvl->in.l.pRecord);
      }else{
        sqlite3_free(pLvl->in.l.aMemory);
      }
    }else{
      if( pLvl->in.f.pFd ) sqlite3OsCloseFree(pLvl->in.f.pFd);
    }
    if( pLvl->out.pFd ) sqlite3OsCloseFree(pLvl->out.pFd);
    sqlite3DbFree(db, pLvl->pUnpacked);
    sqlite3_free(pLvl);
  }
  pTask->pLevel = 0;
  pTask->nConsolidate = 0;
}

/*
** Join all threads.  
*/
#if SQLITE_MAX_WORKER_THREADS>0
static int vdbeSorterJoinAll(VdbeSorter *pSorter, int rcin){
  int rc = rcin;
  int i;
  for(i=0; i<pSorter->nTask; i++){
    SortSubtask *pTask = &pSorter->aTask[i];
    SortLevel *pLvl;
    for(pLvl=pTask->pLevel; pLvl; pLvl=pLvl->pNext){
      if( pLvl->pThread ){
        void *pRet;
        int rc2 = sqlite3ThreadJoin(pLvl->pThread, &pRet);
        pLvl->pThread = 0;
        pLvl->bDone = 0;
        if( rc==SQLITE_OK ) rc = rc2;
        if( rc==SQLITE_OK ) rc = SQLITE_PTR_TO_INT(pRet);
      }
    }
  }
  return rc;
}
#else
# define vdbeSorterJoinAll(x,rcin) (rcin)
#endif

/*
** Allocate a new MergeEngine object with space for nIter iterators.
*/
static MergeEngine *vdbeMergeEngineNew(int nIter){
  int N = 2;                      /* Smallest power of two >= nIter */
  int nByte;                      /* Total bytes of space to allocate */
  MergeEngine *pNew;              /* Pointer to allocated object to return */

  assert( nIter<=SORTER_MAX_MERGE_COUNT );
  while( N<nIter ) N += N;
  nByte = sizeof(MergeEngine) + N * (sizeof(int) + sizeof(PmaReader));

  pNew = (MergeEngine*)sqlite3MallocZero(nByte);
  if( pNew ){
    pNew->nTree = N;
    pNew->aIter = (PmaReader*)&pNew[1];
    pNew->aTree = (int*)&pNew->aIter[N];
  }
  return pNew;
}

/*
** Free the MergeEngine object passed as the only argument.
*/
static void vdbeMergeEngineFree(MergeEngine *pMerger){
  int i;
  if( pMerger ){
    for(i=0; i<pMerger->nTree; i++){
      vdbePmaReaderClear(&pMerger->aIter[i]);
    }
  }
  sqlite3_free(pMerger);
}

/*
** Reset a sorting cursor back to its original empty state.
*/
void sqlite3VdbeSorterReset(sqlite3 *db, VdbeSorter *pSorter){
  int i;
  (void)vdbeSorterJoinAll(pSorter, SQLITE_OK);
  vdbeMergeEngineFree(pSorter->pMerger);
  pSorter->pMerger = 0;
  for(i=0; i<pSorter->nTask; i++){
    SortSubtask *pTask = &pSorter->aTask[i];
    vdbeSortSubtaskCleanup(db, pTask);
  }
  if( pSorter->list.aMemory==0 ){
    vdbeSorterRecordFree(0, pSorter->list.pRecord);
  }
  pSorter->list.pRecord = 0;
  pSorter->list.nInMemory = 0;
  pSorter->bUsePMA = 0;
  pSorter->iMemory = 0;
}

/*
** Free any cursor components allocated by sqlite3VdbeSorterXXX routines.
*/
void sqlite3VdbeSorterClose(sqlite3 *db, VdbeCursor *pCsr){
  VdbeSorter *pSorter = pCsr->pSorter;
  if( pSorter ){
    sqlite3VdbeSorterReset(db, pSorter);
    vdbeMergeEngineFree(pSorter->pMerger);
    sqlite3_free(pSorter->list.aMemory);
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
static int vdbeSorterOpenTempFile(sqlite3_vfs *pVfs, sqlite3_file **ppFile){
  int rc;
  rc = sqlite3OsOpenMalloc(pVfs, 0, ppFile,
      SQLITE_OPEN_TEMP_JOURNAL |
      SQLITE_OPEN_READWRITE    | SQLITE_OPEN_CREATE |
      SQLITE_OPEN_EXCLUSIVE    | SQLITE_OPEN_DELETEONCLOSE, &rc
  );
  if( rc==SQLITE_OK ){
    i64 max = SQLITE_MAX_MMAP_SIZE;
    sqlite3OsFileControlHint( *ppFile, SQLITE_FCNTL_MMAP_SIZE, (void*)&max);
  }
  return rc;
}

/*
** Merge the two sorted lists p1 and p2 into a single list.
** Set *ppOut to the head of the new list.
*/
static void vdbeSorterMerge(
  KeyInfo *pKeyInfo,
  UnpackedRecord *r2,
  SorterRecord *p1,               /* First list to merge */
  SorterRecord *p2,               /* Second list to merge */
  SorterRecord **ppOut            /* OUT: Head of merged list */
){
  SorterRecord *pFinal = 0;
  SorterRecord **pp = &pFinal;
  void *pVal2 = p2 ? SRVAL(p2) : 0;

  while( p1 && p2 ){
    int res;
    res = vdbeSorterCompare(pKeyInfo, r2, SRVAL(p1), p1->nVal, pVal2, p2->nVal);
    if( res<=0 ){
      *pp = p1;
      pp = &p1->u.pNext;
      p1 = p1->u.pNext;
      pVal2 = 0;
    }else{
      *pp = p2;
       pp = &p2->u.pNext;
      p2 = p2->u.pNext;
      if( p2==0 ) break;
      pVal2 = SRVAL(p2);
    }
  }
  *pp = p1 ? p1 : p2;
  *ppOut = pFinal;
}

/*
** Sort the linked list of records headed at pLvl->in.l.pRecord. Return 
** SQLITE_OK if successful, or an SQLite error code (i.e. SQLITE_NOMEM) if 
** an error occurs.
*/
static int vdbeSorterSort(
  SortList *pList, 
  KeyInfo *pKeyInfo, 
  UnpackedRecord *pUnpacked
){
  int i;
  SorterRecord **aSlot;
  SorterRecord *p;

  aSlot = (SorterRecord **)sqlite3MallocZero(64 * sizeof(SorterRecord *));
  if( !aSlot ){
    return SQLITE_NOMEM;
  }

  p = pList->pRecord;
  while( p ){
    SorterRecord *pNext;
    if( pList->aMemory ){
      if( (u8*)p==pList->aMemory ){
        pNext = 0;
      }else{
        assert( p->u.iNext<sqlite3MallocSize(pList->aMemory) );
        pNext = (SorterRecord*)&pList->aMemory[p->u.iNext];
      }
    }else{
      pNext = p->u.pNext;
    }

    p->u.pNext = 0;
    for(i=0; aSlot[i]; i++){
      vdbeSorterMerge(pKeyInfo, pUnpacked, p, aSlot[i], &p);
      aSlot[i] = 0;
    }
    aSlot[i] = p;
    p = pNext;
  }

  p = 0;
  for(i=0; i<64; i++){
    vdbeSorterMerge(pKeyInfo, pUnpacked, p, aSlot[i], &p);
  }
  pList->pRecord = p;

  sqlite3_free(aSlot);
  return SQLITE_OK;
}

/*
** Initialize a PMA-writer object.
*/
static void vdbePmaWriterInit(
  sqlite3_file *pFile,            /* File to write to */
  PmaWriter *p,                   /* Object to populate */
  int nBuf,                       /* Buffer size */
  i64 iStart                      /* Offset of pFile to begin writing at */
){
  memset(p, 0, sizeof(PmaWriter));
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
** Write nData bytes of data to the PMA. Return SQLITE_OK
** if successful, or an SQLite error code if an error occurs.
*/
static void vdbePmaWriteBlob(PmaWriter *p, u8 *pData, int nData){
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
** Flush any buffered data to disk and clean up the PMA-writer object.
** The results of using the PMA-writer after this call are undefined.
** Return SQLITE_OK if flushing the buffered data succeeds or is not 
** required. Otherwise, return an SQLite error code.
**
** Before returning, set *piEof to the offset immediately following the
** last byte written to the file.
*/
static int vdbePmaWriterFinish(PmaWriter *p, i64 *piEof){
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
  memset(p, 0, sizeof(PmaWriter));
  return rc;
}

/*
** Write value iVal encoded as a varint to the PMA. Return 
** SQLITE_OK if successful, or an SQLite error code if an error occurs.
*/
static void vdbePmaWriteVarint(PmaWriter *p, u64 iVal){
  int nByte; 
  u8 aByte[10];
  nByte = sqlite3PutVarint(aByte, iVal);
  vdbePmaWriteBlob(p, aByte, nByte);
}

#if SQLITE_MAX_MMAP_SIZE>0
/*
** The first argument is a file-handle open on a temporary file. The file
** is guaranteed to be nByte bytes or smaller in size. This function
** attempts to extend the file to nByte bytes in size and to ensure that
** the VFS has memory mapped it.
**
** Whether or not the file does end up memory mapped of course depends on
** the specific VFS implementation.
*/
static void vdbeSorterExtendFile(
  sqlite3 *db, 
  SortFile *pFile,
  i64 nByte
){
  if( nByte<=(i64)(db->nMaxSorterMmap) && nByte>pFile->nByte ){
    sqlite3_file *pFd = pFile->pFd;
    int rc = sqlite3OsTruncate(pFd, nByte);
    if( rc==SQLITE_OK ){
      void *p = 0;
      sqlite3OsFetch(pFd, 0, nByte, &p);
      sqlite3OsUnfetch(pFd, 0, p);
      pFile->nByte = nByte;
    }
  }
}
#else
# define vdbeSorterExtendFile(x,y,z) SQLITE_OK
#endif

/*
** Advance the MergeEngine iterator passed as the second argument to
** the next entry. Set *pbEof to true if this means the iterator has 
** reached EOF.
**
** Return SQLITE_OK if successful or an error code if an error occurs.
*/
static int vdbeSorterNext(
  SortLevel *pLvl,
  MergeEngine *pMerger,
  int *pbEof
){
  int rc;
  int iPrev = pMerger->aTree[1];/* Index of iterator to advance */
  KeyInfo *pKeyInfo = pLvl->pTask->pKeyInfo;
  UnpackedRecord *r2 = pLvl->pUnpacked;

  /* Advance the current iterator */
  rc = vdbePmaReaderNext(&pMerger->aIter[iPrev]);

  /* Update contents of aTree[] */
  if( rc==SQLITE_OK ){
    int i;                      /* Index of aTree[] to recalculate */
    PmaReader *pIter1;     /* First iterator to compare */
    PmaReader *pIter2;     /* Second iterator to compare */
    u8 *pKey2;                  /* To pIter2->aKey, or 0 if record cached */

    /* Find the first two iterators to compare. The one that was just
    ** advanced (iPrev) and the one next to it in the array.  */
    pIter1 = &pMerger->aIter[(iPrev & 0xFFFE)];
    pIter2 = &pMerger->aIter[(iPrev | 0x0001)];
    pKey2 = pIter2->aKey;

    for(i=(pMerger->nTree+iPrev)/2; i>0; i=i/2){
      /* Compare pIter1 and pIter2. Store the result in variable iRes. */
      int iRes;
      if( pIter1->pFile==0 ){
        iRes = +1;
      }else if( pIter2->pFile==0 ){
        iRes = -1;
      }else{
        iRes = vdbeSorterCompare(pKeyInfo, r2, 
            pIter1->aKey, pIter1->nKey, pKey2, pIter2->nKey
        );
      }

      /* If pIter1 contained the smaller value, set aTree[i] to its index.
      ** Then set pIter2 to the next iterator to compare to pIter1. In this
      ** case there is no cache of pIter2 in pLvl->pUnpacked, so set
      ** pKey2 to point to the record belonging to pIter2.
      **
      ** Alternatively, if pIter2 contains the smaller of the two values,
      ** set aTree[i] to its index and update pIter1. If vdbeSorterCompare()
      ** was actually called above, then pLvl->pUnpacked now contains
      ** a value equivalent to pIter2. So set pKey2 to NULL to prevent
      ** vdbeSorterCompare() from decoding pIter2 again.
      **
      ** If the two values were equal, then the value from the oldest
      ** PMA should be considered smaller. The VdbeSorter.aIter[] array
      ** is sorted from oldest to newest, so pIter1 contains older values
      ** than pIter2 iff (pIter1<pIter2).  */
      if( iRes<0 || (iRes==0 && pIter1<pIter2) ){
        pMerger->aTree[i] = (int)(pIter1 - pMerger->aIter);
        pIter2 = &pMerger->aIter[ pMerger->aTree[i ^ 0x0001] ];
        pKey2 = pIter2->aKey;
      }else{
        if( pIter1->pFile ) pKey2 = 0;
        pMerger->aTree[i] = (int)(pIter2 - pMerger->aIter);
        pIter1 = &pMerger->aIter[ pMerger->aTree[i ^ 0x0001] ];
      }
    }
    *pbEof = (pMerger->aIter[pMerger->aTree[1]].pFile==0);
  }

  return rc;
}

static UnpackedRecord *vdbeSorterAllocUnpackedRecord(KeyInfo *pKeyInfo){
  char *pFree;
  UnpackedRecord *pRet;
  pRet = sqlite3VdbeAllocUnpackedRecord(pKeyInfo, 0, 0, &pFree);
  assert( pRet==(UnpackedRecord*)pFree );
  if( pRet ){
    pRet->nField = pKeyInfo->nField;
    pRet->errCode = 0;
  }
  return pRet;
}

#if 0
static void vdbeSorterWorkDebug(SortLevel *pLvl, const char *zEvent){
  i64 t;
  SortLevel *p;
  SortSubtask *pTask = pLvl->pTask;
  int iTask = (pTask - pTask->pSorter->aTask);
  int iLvl = 0;
  for(p=pTask->pLevel; p!=pLvl; p=p->pNext) iLvl++;
  sqlite3OsCurrentTimeInt64(pTask->db->pVfs, &t);
  fprintf(stderr, "%lld:%d.%d %s\n", t, iTask, iLvl, zEvent);
}
static void vdbeSorterRewindDebug(sqlite3 *db, const char *zEvent){
  i64 t;
  sqlite3OsCurrentTimeInt64(db->pVfs, &t);
  fprintf(stderr, "%lld:X %s\n", t, zEvent);
}
#else
# define vdbeSorterWorkDebug(x,y)
# define vdbeSorterRewindDebug(x,y)
#endif

/*
** Merge the data currently stored in (pLevel->in), if any, into a new PMA 
** stored within (pLevel->out).
*/
static int vdbeSorterWorkLevel(SortLevel *pLvl){
  int rc = SQLITE_OK;             /* Return code */
  SortSubtask *pTask = pLvl->pTask;
  MergeEngine *pMerger = 0;
  SortFile *pOut = &pLvl->out;    /* Write new PMA here */
  i64 nOut = 0;                   /* Expected size of new PMA */
  PmaWriter writer;               /* Used to write new PMA to pOut */
  int bEof = 0;

  vdbeSorterWorkDebug(pLvl, "enter");
 
  if( pLvl->pUnpacked==0 ){
    pLvl->pUnpacked = vdbeSorterAllocUnpackedRecord(pTask->pKeyInfo);
    if( pLvl->pUnpacked==0 ){
      rc = SQLITE_NOMEM;
      goto work_level_out;
    }
  }

  if( pLvl->out.pFd==0 ){
    assert( pLvl->out.iOff==0 );
    assert( pLvl->out.nByte==0 );
    assert( pLvl->out.nPMA==0 );
    rc = vdbeSorterOpenTempFile(pTask->db->pVfs, &pLvl->out.pFd);
    if( rc!=SQLITE_OK ) goto work_level_out;
  }

  if( pLvl==pTask->pLevel ){
    if( pLvl->in.l.pRecord==0 ){
      bEof = 1;
    }else{
      rc = vdbeSorterSort(&pLvl->in.l, pTask->pKeyInfo, pLvl->pUnpacked);
      nOut = pLvl->in.l.nInMemory;
    }
  }else{
    int nPMA = pLvl->in.f.nPMA;
    if( nPMA==0 ){
      bEof = 1;
    }else{
      pMerger = vdbeMergeEngineNew(nPMA);
      if( pMerger==0 ){
        rc = SQLITE_NOMEM;
      }else{
        /* Configure the merger object to read and merge data from all 
         ** PMAs at pLvl.  */
        int iIter;
        i64 iReadOff = 0;
        for(iIter=0; iIter<nPMA && rc==SQLITE_OK; iIter++){
          PmaReader *pIter = &pMerger->aIter[iIter];
          rc = vdbePmaReaderInit(pTask, &pLvl->in.f, iReadOff, pIter, &nOut);
          iReadOff = pIter->iEof;
        }

        for(iIter=pMerger->nTree-1; rc==SQLITE_OK && iIter>0; iIter--){
          rc = vdbeSorterDoCompare(pLvl, pMerger, iIter);
        }
      }
    }
  }
  if( rc!=SQLITE_OK ) goto work_level_out;

  /* If mmap is to be used, pre-extend and map the temp file. */
  vdbeSorterExtendFile(pTask->db, &pLvl->out, pLvl->out.iOff + nOut + 9);

  if( bEof==0 ){
    vdbePmaWriterInit(pOut->pFd, &writer, pTask->pgsz, pOut->iOff);
    vdbePmaWriteVarint(&writer, nOut);

    while( rc==SQLITE_OK && bEof==0 ){
      u8 *aKey;                     /* Next key to write to output */
      int nKey;                     /* Size of aKey[] in bytes */
      if( pMerger==0 ){
        aKey = SRVAL(pLvl->in.l.pRecord);
        nKey = pLvl->in.l.pRecord->nVal;
      }else{
        PmaReader *pIter = &pMerger->aIter[ pMerger->aTree[1] ];
        assert( pIter->pFile );  /* pIter is not at EOF */
        aKey = pIter->aKey;
        nKey = pIter->nKey;
      }

      vdbePmaWriteVarint(&writer, nKey);
      vdbePmaWriteBlob(&writer, aKey, nKey);

      if( pMerger==0 ){
        SorterRecord *pNext = pLvl->in.l.pRecord->u.pNext;
        if( pLvl->in.l.aMemory==0 ) sqlite3_free(pLvl->in.l.pRecord);
        pLvl->in.l.pRecord = pNext;
        bEof = (pNext==0);
      }else{
        rc = vdbeSorterNext(pLvl, pMerger, &bEof);
      }
    }
    rc = vdbePmaWriterFinish(&writer, &pOut->iOff);
    pOut->nPMA++;

    if( rc==SQLITE_OK && pMerger ){
      sqlite3OsCloseFree(pLvl->in.f.pFd);
      pLvl->in.f.pFd = 0;
    }
    vdbeMergeEngineFree(pMerger);
  }

  if( rc==SQLITE_OK && (
      (pOut->nPMA>=SORTER_MAX_MERGE_COUNT)
   || (pTask->nConsolidate && pLvl->pNext)
   || (pTask->nConsolidate && pTask->nConsolidate<pOut->nPMA)
  )){
    SortLevel *pNext = pLvl->pNext;
    if( pNext==0 ){
      pNext = (SortLevel*)sqlite3_malloc(sizeof(SortLevel));
      if( pNext==0 ){
        rc = SQLITE_NOMEM;
        goto work_level_out;
      }
      memset(pNext, 0, sizeof(SortLevel));
      pLvl->pNext = pNext;
      pNext->pTask = pTask;
    }

    /* If there is a thread running on the next level, block on it. */
#if SQLITE_MAX_WORKER_THREADS>0
    if( pNext->pThread ){
      void *pRet;
      rc = sqlite3ThreadJoin(pNext->pThread, &pRet);
      pNext->pThread = 0;
      pNext->bDone = 0;
      if( rc==SQLITE_OK ) rc = SQLITE_PTR_TO_INT(pRet);
      if( rc!=SQLITE_OK ) goto work_level_out;
    }
#endif

    pNext->in.f = pLvl->out;
    memset(&pLvl->out, 0, sizeof(pLvl->out));
  }

 work_level_out:
  vdbeSorterWorkDebug(pLvl, "exit");
  if( rc==SQLITE_OK && pLvl->pUnpacked->errCode ){
    assert( pLvl->pUnpacked->errCode==SQLITE_NOMEM );
    rc = SQLITE_NOMEM;
  }
  return rc;
}

/*
** Run the activity scheduled by the object passed as the only argument
** in the current thread.
*/
static int vdbeSorterRun(SortLevel *pLvl){
  int rc;

  assert( pLvl->bDone==0 );
  assert( pLvl->pThread==0 );
  while( 1 ){
    rc = vdbeSorterWorkLevel(pLvl);
    if( rc==SQLITE_OK && pLvl->pTask->pLevel==pLvl && pLvl->in.l.aMemory ){
      assert( pLvl->pTask->pSorter->list.aMemory==0 );
      assert( pLvl->in.l.pRecord==0 );
      pLvl->pTask->pSorter->list.aMemory = pLvl->in.l.aMemory;
      pLvl->in.l.aMemory = 0;
    }

    if( rc!=SQLITE_OK || pLvl->out.nPMA>0 ) break;
    pLvl = pLvl->pNext;
    assert( pLvl->bDone==0 );
    assert( pLvl->pThread==0 );
  }

  pLvl->bDone = 0;
  return rc;
}

#if SQLITE_MAX_WORKER_THREADS>0
static void *vdbeSorterThread(void *pCtx){
  int rc;
  SortLevel *pLvl = (SortLevel*)pCtx;

  rc = vdbeSorterWorkLevel(pLvl);
  if( rc==SQLITE_OK && pLvl->out.nPMA==0 ){
    SortLevel *pNext = pLvl->pNext;
    void *pCtx = (void*)pNext;
    assert( pNext->pThread==0 );
    rc = sqlite3ThreadCreate(&pNext->pThread, vdbeSorterThread, pCtx);
  }

  pLvl->bDone = 1;
  return SQLITE_INT_TO_PTR(rc);
}
#endif

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
  SortSubtask *pTask = 0;    /* Sub-task new PMA is written to */
  SortLevel *pLevel;         /* Level to write to */

  /* Set the use-temp-files flag. */
  pSorter->bUsePMA = 1;

  /* Select one of the sub-tasks to flush this PMA. In single threaded
  ** mode (pSorter->nTask==1), this is always aTask[0]. In multi-threaded mode,
  ** it may be any of the pSorter->nTask sub-tasks.  */
  for(i=0; i<pSorter->nTask; i++){
    pTask = &pSorter->aTask[i];
    if( pTask->pLevel==0 
     || pTask->pLevel->pThread==0 
     || pTask->pLevel->bDone 
    ){
      break;
    }
  }

  /* If the first level for this task has not been allocated, allocate it. */
  if( pTask->pLevel==0 ){
    SortLevel *pNew = (SortLevel*)sqlite3_malloc(sizeof(SortLevel));
    if( pNew==0 ){
      rc = SQLITE_NOMEM;
    }else{
      memset(pNew, 0, sizeof(SortLevel));
      pNew->pTask = pTask;
      pTask->pLevel = pNew;
    }
  }
  pLevel = pTask->pLevel;

  /* If there is a background thread using the selected task, wait for
  ** it to finish. */
#if SQLITE_MAX_WORKER_THREADS>0
  if( rc==SQLITE_OK && pLevel->pThread ){
    void *pRet = 0;
    rc = sqlite3ThreadJoin(pLevel->pThread, &pRet);
    pLevel->pThread = 0;
    pLevel->bDone = 0;
    if( rc==SQLITE_OK ) rc = SQLITE_PTR_TO_INT(pRet);
  }
#endif

  if( rc==SQLITE_OK ){
    u8 *aNewMem = 0;
    if( pSorter->list.aMemory && pSorter->nTask>1 ){
      aNewMem = pLevel->in.l.aMemory;
      if( aNewMem==0 ){
        aNewMem = sqlite3_malloc(pSorter->mxPmaSize);
        if( aNewMem==0 ) rc = SQLITE_NOMEM;
      }
    }
    assert( pLevel->in.l.pRecord==0 );
    pLevel->in.l = pSorter->list;
    pSorter->list.pRecord = 0;
    pSorter->list.nInMemory = 0;
    pSorter->list.aMemory = aNewMem;
    if( rc==SQLITE_OK ){
#if SQLITE_MAX_WORKER_THREADS>0
      if( pSorter->nTask>1 ){
        void *pCtx = (void*)pLevel;
        rc = sqlite3ThreadCreate(&pLevel->pThread, vdbeSorterThread, pCtx);
        pSorter->nMemory = aNewMem ? sqlite3MallocSize(aNewMem) : 0;
      }else
#endif
      {
        rc = vdbeSorterRun(pLevel);
      }
    }
  }

  return rc;
}

/*
** Add a record to the sorter.
*/
int sqlite3VdbeSorterWrite(
  sqlite3 *db,                    /* Database handle */
  const VdbeCursor *pCsr,         /* Sorter cursor */
  Mem *pVal                       /* Memory cell containing record */
){
  VdbeSorter *pSorter = pCsr->pSorter;
  int rc = SQLITE_OK;             /* Return Code */
  SorterRecord *pNew;             /* New list element */

  int bFlush;                     /* True to flush contents of memory to PMA */
  int nReq;                       /* Bytes of memory required */
  int nPMA;                       /* Bytes of PMA space required */

  assert( pSorter );

  /* Figure out whether or not the current contents of memory should be
  ** flushed to a PMA before continuing. If so, do so.
  **
  ** If using the single large allocation mode (pSorter->aMemory!=0), then
  ** flush the contents of memory to a new PMA if (a) at least one value is
  ** already in memory and (b) the new value will not fit in memory.
  ** 
  ** Or, if using separate allocations for each record, flush the contents
  ** of memory to a PMA if either of the following are true:
  **
  **   * The total memory allocated for the in-memory list is greater 
  **     than (page-size * cache-size), or
  **
  **   * The total memory allocated for the in-memory list is greater 
  **     than (page-size * 10) and sqlite3HeapNearlyFull() returns true.
  */
  nReq = pVal->n + sizeof(SorterRecord);
  nPMA = pVal->n + sqlite3VarintLen(pVal->n);
  if( pSorter->mxPmaSize ){
    if( pSorter->list.aMemory ){
      bFlush = pSorter->iMemory && (pSorter->iMemory+nReq) > pSorter->mxPmaSize;
    }else{
      int nInMemory = pSorter->list.nInMemory;
      bFlush = (
          (nInMemory > pSorter->mxPmaSize)
       || (nInMemory > pSorter->mnPmaSize && sqlite3HeapNearlyFull())
      );
    }
    if( bFlush ){
      rc = vdbeSorterFlushPMA(db, pCsr, 0);
      pSorter->list.nInMemory = 0;
      pSorter->iMemory = 0;
      assert( rc!=SQLITE_OK || pSorter->list.pRecord==0 );
    }
  }

  pSorter->list.nInMemory += nPMA;

  if( pSorter->list.aMemory ){
    int nMin = pSorter->iMemory + nReq;

    if( nMin>pSorter->nMemory ){
      u8 *aNew;
      int nNew = pSorter->nMemory * 2;
      while( nNew < nMin ) nNew = nNew*2;
      if( nNew > pSorter->mxPmaSize ) nNew = pSorter->mxPmaSize;
      if( nNew < nMin ) nNew = nMin;

      aNew = sqlite3Realloc(pSorter->list.aMemory, nNew);
      if( !aNew ) return SQLITE_NOMEM;
      pSorter->list.pRecord = (SorterRecord*)(
          aNew + ((u8*)pSorter->list.pRecord - pSorter->list.aMemory)
      );
      pSorter->list.aMemory = aNew;
      pSorter->nMemory = nNew;
    }

    pNew = (SorterRecord*)&pSorter->list.aMemory[pSorter->iMemory];
    pSorter->iMemory += ROUND8(nReq);
    pNew->u.iNext = (u8*)(pSorter->list.pRecord) - pSorter->list.aMemory;
  }else{
    pNew = (SorterRecord *)sqlite3Malloc(nReq);
    if( pNew==0 ){
      return SQLITE_NOMEM;
    }
    pNew->u.pNext = pSorter->list.pRecord;
  }

  memcpy(SRVAL(pNew), pVal->z, pVal->n);
  pNew->nVal = pVal->n;
  pSorter->list.pRecord = pNew;

  return rc;
}


/*
** Once the sorter has been populated by calls to sqlite3VdbeSorterWrite,
** this function is called to prepare for iterating through the records
** in sorted order.
*/
int sqlite3VdbeSorterRewind(sqlite3 *db, const VdbeCursor *pCsr, int *pbEof){
  VdbeSorter *pSorter = pCsr->pSorter;
  int rc = SQLITE_OK;             /* Return code */
  int nTask = 0;
  int i;

  assert( pSorter );

  /* If no data has been written to disk, then do not do so now. Instead,
  ** sort the VdbeSorter.list.pRecord list. The vdbe layer will read data 
  ** directly from the in-memory list.  */
  *pbEof = 0;
  if( pSorter->bUsePMA==0 ){
    if( pSorter->list.pRecord ){
      SortSubtask *pTask = &pSorter->aTask[0];
      UnpackedRecord *pUnpack = vdbeSorterAllocUnpackedRecord(pTask->pKeyInfo);
      if( pUnpack==0 ) return SQLITE_NOMEM;
      rc = vdbeSorterSort(&pSorter->list, pTask->pKeyInfo, pUnpack);
      sqlite3DbFree(db, pUnpack);
    }else{
      *pbEof = 1;
    }
    return rc;
  }

  /* Write the current in-memory list to a PMA. */
  if( pSorter->list.pRecord ){
    rc = vdbeSorterFlushPMA(db, pCsr, 0);
  }

  /* Join all threads */
  rc = vdbeSorterJoinAll(pSorter, rc);

  vdbeSorterRewindDebug(db, "rewind");

  for(i=0; i<pSorter->nTask; i++){
    if( pSorter->aTask[i].pLevel ) nTask++;
  }

  for(i=0; rc==SQLITE_OK && i<pSorter->nTask; i++){
    SortSubtask *pTask = &pSorter->aTask[i];
    if( pTask->pLevel ){
      SortLevel *pLvl = pTask->pLevel;
      pTask->nConsolidate = (SORTER_MAX_MERGE_COUNT / nTask);
#if SQLITE_MAX_WORKER_THREADS>0
      if( i<(pSorter->nTask-1) ){
        void *pCtx = (void*)pLvl;
        rc = sqlite3ThreadCreate(&pLvl->pThread, vdbeSorterThread, pCtx);
      }else
#endif
      {
        assert( pLvl->pThread==0 );
        rc = vdbeSorterRun(pLvl);
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
    MergeEngine *pMerger;
    for(i=0; i<pSorter->nTask; i++){
      SortSubtask *pTask = &pSorter->aTask[i];
      if( pTask->pLevel ){
        SortLevel *pLvl;
        for(pLvl=pTask->pLevel; pLvl->pNext; pLvl=pLvl->pNext){
          assert( pLvl->out.nPMA==0 );
        }
        nIter += pLvl->out.nPMA;
      }
    }

    pSorter->pMerger = pMerger = vdbeMergeEngineNew(nIter);
    if( pMerger==0 ){
      rc = SQLITE_NOMEM;
    }else{
      int iIter = 0;
      for(i=0; i<pSorter->nTask; i++){
        SortSubtask *pTask = &pSorter->aTask[i];
        if( pTask->pLevel ){
          int iPMA;
          i64 iReadOff = 0;
          SortLevel *pLvl;
          for(pLvl=pTask->pLevel; pLvl->pNext; pLvl=pLvl->pNext);

          for(iPMA=0; iPMA<pLvl->out.nPMA && rc==SQLITE_OK; iPMA++){
            i64 nDummy = 0;
            PmaReader *pIter = &pMerger->aIter[iIter++];
            rc = vdbePmaReaderInit(pTask, &pLvl->out, iReadOff, pIter, &nDummy);
            iReadOff = pIter->iEof;
          }
        }
      }

      for(i=pMerger->nTree-1; rc==SQLITE_OK && i>0; i--){
        rc = vdbeSorterDoCompare(pSorter->aTask[0].pLevel, pMerger, i);
      }
    }
  }

  if( rc==SQLITE_OK ){
    *pbEof = (pSorter->pMerger->aIter[pSorter->pMerger->aTree[1]].pFile==0);
  }
  vdbeSorterRewindDebug(db, "rewinddone");
  return rc;
}

/*
** Advance to the next element in the sorter.
*/
int sqlite3VdbeSorterNext(sqlite3 *db, const VdbeCursor *pCsr, int *pbEof){
  VdbeSorter *pSorter = pCsr->pSorter;
  int rc;                         /* Return code */

  if( pSorter->pMerger ){
    rc = vdbeSorterNext(pSorter->aTask[0].pLevel, pSorter->pMerger, pbEof);
  }else{
    SorterRecord *pFree = pSorter->list.pRecord;
    pSorter->list.pRecord = pFree->u.pNext;
    pFree->u.pNext = 0;
    if( pSorter->list.aMemory==0 ) vdbeSorterRecordFree(db, pFree);
    *pbEof = !pSorter->list.pRecord;
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
    PmaReader *pIter;
    pIter = &pSorter->pMerger->aIter[ pSorter->pMerger->aTree[1] ];
    *pnKey = pIter->nKey;
    pKey = pIter->aKey;
  }else{
    *pnKey = pSorter->list.pRecord->nVal;
    pKey = SRVAL(pSorter->list.pRecord);
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
** If the sorter cursor key contains any NULL values, consider it to be
** less than pVal. Even if pVal also contains NULL values.
**
** If an error occurs, return an SQLite error code (i.e. SQLITE_NOMEM).
** Otherwise, set *pRes to a negative, zero or positive value if the
** key in pVal is smaller than, equal to or larger than the current sorter
** key.
**
** This routine forms the core of the OP_SorterCompare opcode, which in
** turn is used to verify uniqueness when constructing a UNIQUE INDEX.
*/
int sqlite3VdbeSorterCompare(
  const VdbeCursor *pCsr,         /* Sorter cursor */
  Mem *pVal,                      /* Value to compare to current sorter key */
  int nIgnore,                    /* Ignore this many fields at the end */
  int *pRes                       /* OUT: Result of comparison */
){
  VdbeSorter *pSorter = pCsr->pSorter;
  UnpackedRecord *r2 = pSorter->pUnpacked;
  KeyInfo *pKeyInfo = pCsr->pKeyInfo;
  int i;
  void *pKey; int nKey;           /* Sorter key to compare pVal with */

  if( r2==0 ){
    r2 = vdbeSorterAllocUnpackedRecord(pSorter->aTask[0].pKeyInfo);
    if( r2==0 ) return SQLITE_NOMEM;
    pSorter->pUnpacked = r2;
    assert( r2->nField>=pKeyInfo->nField-nIgnore );
    r2->nField = pKeyInfo->nField-nIgnore;
  }
  assert( r2->nField==pKeyInfo->nField-nIgnore );

  pKey = vdbeSorterRowkey(pSorter, &nKey);
  sqlite3VdbeRecordUnpack(pKeyInfo, nKey, pKey, r2);
  for(i=0; i<r2->nField; i++){
    if( r2->aMem[i].flags & MEM_Null ){
      *pRes = -1;
      return SQLITE_OK;
    }
  }

  *pRes = sqlite3VdbeRecordCompare(pVal->n, pVal->z, r2, 0);
  return SQLITE_OK;
}

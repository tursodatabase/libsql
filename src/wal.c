/*
** 2010 February 1
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
*************************************************************************
**
** This file contains the implementation of a write-ahead log file used in 
** "journal_mode=wal" mode.
*/
#ifndef SQLITE_OMIT_WAL

#include "wal.h"


/*
** WRITE-AHEAD LOG (WAL) FILE FORMAT
**
** A wal file consists of a header followed by zero or more "frames".
** The header is 12 bytes in size and consists of the following three
** big-endian 32-bit unsigned integer values:
**
**     0: Database page size,
**     4: Randomly selected salt value 1,
**     8: Randomly selected salt value 2.
**
** Immediately following the header are zero or more frames. Each
** frame itself consists of a 16-byte header followed by a <page-size> bytes
** of page data. The header is broken into 4 big-endian 32-bit unsigned 
** integer values, as follows:
**
**     0: Page number.
**     4: For commit records, the size of the database image in pages 
**        after the commit. For all other records, zero.
**     8: Checksum value 1.
**    12: Checksum value 2.
*/

/* 
** WAL-INDEX FILE FORMAT
**
** The wal-index file consists of a 32-byte header region, followed by an 
** 8-byte region that contains no useful data (used to apply byte-range locks
** to), followed by the data region. 
**
** The contents of both the header and data region are specified in terms
** of 1, 2 and 4 byte unsigned integers. All integers are stored in 
** machine-endian order.  The wal-index is not a persistent file and
** so it does not need to be portable across archtectures.
**
** A wal-index file is essentially a shadow-pager map. It contains a
** mapping from database page number to the set of locations in the wal
** file that contain versions of the database page. When a database 
** client needs to read a page of data, it first queries the wal-index
** file to determine if the required version of the page is stored in
** the wal. If so, the page is read from the wal. If not, the page is
** read from the database file.
**
** Whenever a transaction is appended to the wal or a checkpoint transfers
** data from the wal into the database file, the wal-index is 
** updated accordingly.
**
** The fields in the wal-index file header are described in the comment 
** directly above the definition of struct WalIndexHdr (see below). 
** Immediately following the fields in the WalIndexHdr structure is
** an 8 byte checksum based on the contents of the header. This field is
** not the same as the iCheck1 and iCheck2 fields of the WalIndexHdr.
*/

/* Object declarations */
typedef struct WalIndexHdr WalIndexHdr;
typedef struct WalIterator WalIterator;


/*
** The following object stores a copy of the wal-index header.
**
** Member variables iCheck1 and iCheck2 contain the checksum for the
** last frame written to the wal, or 2 and 3 respectively if the log 
** is currently empty.
*/
struct WalIndexHdr {
  u32 iChange;                    /* Counter incremented each transaction */
  u32 pgsz;                       /* Database page size in bytes */
  u32 iLastPg;                    /* Address of last valid frame in log */
  u32 nPage;                      /* Size of database in pages */
  u32 iCheck1;                    /* Checkpoint value 1 */
  u32 iCheck2;                    /* Checkpoint value 2 */
};

/* Size of serialized WalIndexHdr object. */
#define WALINDEX_HDR_NFIELD (sizeof(WalIndexHdr) / sizeof(u32))

/* A block of 16 bytes beginning at WALINDEX_LOCK_OFFSET is reserved
** for locks. Since some systems only feature mandatory file-locks, we
** do not read or write data from the region of the file on which locks
** are applied.
*/
#define WALINDEX_LOCK_OFFSET   ((sizeof(WalIndexHdr))+2*sizeof(u32))
#define WALINDEX_LOCK_RESERVED 8

/* Size of header before each frame in wal */
#define WAL_FRAME_HDRSIZE 16

/* Size of write ahead log header */
#define WAL_HDRSIZE 12

/*
** Return the offset of frame iFrame in the write-ahead log file, 
** assuming a database page size of pgsz bytes. The offset returned
** is to the start of the write-ahead log frame-header.
*/
#define walFrameOffset(iFrame, pgsz) (                               \
  WAL_HDRSIZE + ((iFrame)-1)*((pgsz)+WAL_FRAME_HDRSIZE)        \
)

/*
** An open write-ahead log file is represented by an instance of the
** following object.
*/
struct Wal {
  sqlite3_vfs *pVfs;         /* The VFS used to create pFd */
  sqlite3_file *pDbFd;       /* File handle for the database file */
  sqlite3_file *pWalFd;      /* File handle for WAL file */
  u32 iCallback;             /* Value to pass to log callback (or 0) */
  int szWIndex;              /* Size of the wal-index that is mapped in mem */
  u32 *pWiData;              /* Pointer to wal-index content in memory */
  u8 lockState;              /* SQLITE_SHM_xxxx constant showing lock state */
  u8 readerType;             /* SQLITE_SHM_READ or SQLITE_SHM_READ_FULL */
  u8 exclusiveMode;          /* Non-zero if connection is in exclusive mode */
  u8 isWindexOpen;           /* True if ShmOpen() called on pDbFd */
  WalIndexHdr hdr;           /* Wal-index for current snapshot */
  char *zWalName;            /* Name of WAL file */
};


/*
** This structure is used to implement an iterator that iterates through
** all frames in the log in database page order. Where two or more frames
** correspond to the same database page, the iterator visits only the 
** frame most recently written to the log.
**
** The internals of this structure are only accessed by:
**
**   walIteratorInit() - Create a new iterator,
**   walIteratorNext() - Step an iterator,
**   walIteratorFree() - Free an iterator.
**
** This functionality is used by the checkpoint code (see walCheckpoint()).
*/
struct WalIterator {
  int nSegment;                   /* Size of WalIterator.aSegment[] array */
  int nFinal;                     /* Elements in segment nSegment-1 */
  struct WalSegment {
    int iNext;                    /* Next aIndex index */
    u8 *aIndex;                   /* Pointer to index array */
    u32 *aDbPage;                 /* Pointer to db page array */
  } aSegment[1];
};


/*
** Generate an 8 byte checksum based on the data in array aByte[] and the
** initial values of aCksum[0] and aCksum[1]. The checksum is written into
** aCksum[] before returning.
**
** The range of bytes to checksum is treated as an array of 32-bit 
** little-endian unsigned integers. For each integer X in the array, from
** start to finish, do the following:
**
**   aCksum[0] += X;
**   aCksum[1] += aCksum[0];
**
** For the calculation above, use 64-bit unsigned accumulators. Before
** returning, truncate the values to 32-bits as follows: 
**
**   aCksum[0] = (u32)(aCksum[0] + (aCksum[0]>>24));
**   aCksum[1] = (u32)(aCksum[1] + (aCksum[1]>>24));
*/
static void walChecksumBytes(u8 *aByte, int nByte, u32 *aCksum){
  u64 sum1 = aCksum[0];
  u64 sum2 = aCksum[1];
  u32 *a32 = (u32 *)aByte;
  u32 *aEnd = (u32 *)&aByte[nByte];

  assert( (nByte&0x00000003)==0 );

  if( SQLITE_LITTLEENDIAN ){
#ifdef SQLITE_DEBUG
    u8 *a = (u8 *)a32;
    assert( *a32==(a[0] + (a[1]<<8) + (a[2]<<16) + (a[3]<<24)) );
#endif
    do {
      sum1 += *a32;
      sum2 += sum1;
    } while( ++a32<aEnd );
  }else{
    do {
      u8 *a = (u8*)a32;
      sum1 += a[0] + (a[1]<<8) + (a[2]<<16) + (a[3]<<24);
      sum2 += sum1;
    } while( ++a32<aEnd );
  }

  aCksum[0] = sum1 + (sum1>>24);
  aCksum[1] = sum2 + (sum2>>24);
}

/*
** Attempt to change the lock status.
**
** When changing the lock status to SQLITE_SHM_READ, store the
** type of reader lock (either SQLITE_SHM_READ or SQLITE_SHM_READ_FULL)
** in pWal->readerType.
*/
static int walSetLock(Wal *pWal, int desiredStatus){
  int rc = SQLITE_OK;             /* Return code */
  if( pWal->exclusiveMode || pWal->lockState==desiredStatus ){
    pWal->lockState = desiredStatus;
  }else{
    int got = pWal->lockState;
    rc = sqlite3OsShmLock(pWal->pDbFd, desiredStatus, &got);
    pWal->lockState = got;
    if( got==SQLITE_SHM_READ_FULL || got==SQLITE_SHM_READ ){
      pWal->readerType = got;
      pWal->lockState = SQLITE_SHM_READ;
    }
  }
  return rc;
}

/*
** Update the header of the wal-index file.
*/
static void walIndexWriteHdr(Wal *pWal, WalIndexHdr *pHdr){
  u32 *aHdr = pWal->pWiData;                   /* Write header here */
  u32 *aCksum = &aHdr[WALINDEX_HDR_NFIELD];    /* Write header cksum here */

  assert( WALINDEX_HDR_NFIELD==sizeof(WalIndexHdr)/4 );
  assert( aHdr!=0 );
  memcpy(aHdr, pHdr, sizeof(WalIndexHdr));
  aCksum[0] = aCksum[1] = 1;
  walChecksumBytes((u8 *)aHdr, sizeof(WalIndexHdr), aCksum);
}

/*
** This function encodes a single frame header and writes it to a buffer
** supplied by the caller. A frame-header is made up of a series of 
** 4-byte big-endian integers, as follows:
**
**     0: Database page size in bytes.
**     4: Page number.
**     8: New database size (for commit frames, otherwise zero).
**    12: Frame checksum 1.
**    16: Frame checksum 2.
*/
static void walEncodeFrame(
  u32 *aCksum,                    /* IN/OUT: Checksum values */
  u32 iPage,                      /* Database page number for frame */
  u32 nTruncate,                  /* New db size (or 0 for non-commit frames) */
  int nData,                      /* Database page size (size of aData[]) */
  u8 *aData,                      /* Pointer to page data (for checksum) */
  u8 *aFrame                      /* OUT: Write encoded frame here */
){
  assert( WAL_FRAME_HDRSIZE==16 );

  sqlite3Put4byte(&aFrame[0], iPage);
  sqlite3Put4byte(&aFrame[4], nTruncate);

  walChecksumBytes(aFrame, 8, aCksum);
  walChecksumBytes(aData, nData, aCksum);

  sqlite3Put4byte(&aFrame[8], aCksum[0]);
  sqlite3Put4byte(&aFrame[12], aCksum[1]);
}

/*
** Return 1 and populate *piPage, *pnTruncate and aCksum if the 
** frame checksum looks Ok. Otherwise return 0.
*/
static int walDecodeFrame(
  u32 *aCksum,                    /* IN/OUT: Checksum values */
  u32 *piPage,                    /* OUT: Database page number for frame */
  u32 *pnTruncate,                /* OUT: New db size (or 0 if not commit) */
  int nData,                      /* Database page size (size of aData[]) */
  u8 *aData,                      /* Pointer to page data (for checksum) */
  u8 *aFrame                      /* Frame data */
){
  assert( WAL_FRAME_HDRSIZE==16 );

  walChecksumBytes(aFrame, 8, aCksum);
  walChecksumBytes(aData, nData, aCksum);

  if( aCksum[0]!=sqlite3Get4byte(&aFrame[8]) 
   || aCksum[1]!=sqlite3Get4byte(&aFrame[12]) 
  ){
    /* Checksum failed. */
    return 0;
  }

  *piPage = sqlite3Get4byte(&aFrame[0]);
  *pnTruncate = sqlite3Get4byte(&aFrame[4]);
  return 1;
}

static void walMergesort8(
  Pgno *aContent,                 /* Pages in wal */
  u8 *aBuffer,                    /* Buffer of at least *pnList items to use */
  u8 *aList,                      /* IN/OUT: List to sort */
  int *pnList                     /* IN/OUT: Number of elements in aList[] */
){
  int nList = *pnList;
  if( nList>1 ){
    int nLeft = nList / 2;        /* Elements in left list */
    int nRight = nList - nLeft;   /* Elements in right list */
    u8 *aLeft = aList;            /* Left list */
    u8 *aRight = &aList[nLeft];   /* Right list */
    int iLeft = 0;                /* Current index in aLeft */
    int iRight = 0;               /* Current index in aright */
    int iOut = 0;                 /* Current index in output buffer */

    /* TODO: Change to non-recursive version. */
    walMergesort8(aContent, aBuffer, aLeft, &nLeft);
    walMergesort8(aContent, aBuffer, aRight, &nRight);

    while( iRight<nRight || iLeft<nLeft ){
      u8 logpage;
      Pgno dbpage;

      if( (iLeft<nLeft) 
       && (iRight>=nRight || aContent[aLeft[iLeft]]<aContent[aRight[iRight]])
      ){
        logpage = aLeft[iLeft++];
      }else{
        logpage = aRight[iRight++];
      }
      dbpage = aContent[logpage];

      aBuffer[iOut++] = logpage;
      if( iLeft<nLeft && aContent[aLeft[iLeft]]==dbpage ) iLeft++;

      assert( iLeft>=nLeft || aContent[aLeft[iLeft]]>dbpage );
      assert( iRight>=nRight || aContent[aRight[iRight]]>dbpage );
    }
    memcpy(aList, aBuffer, sizeof(aList[0])*iOut);
    *pnList = iOut;
  }

#ifdef SQLITE_DEBUG
  {
    int i;
    for(i=1; i<*pnList; i++){
      assert( aContent[aList[i]] > aContent[aList[i-1]] );
    }
  }
#endif
}

/*
** Define the size of the hash tables in the wal-index file. There
** is a hash-table following every HASHTABLE_NPAGE page numbers in the
** wal-index.
*/
#define HASHTABLE_NPAGE      4096
#define HASHTABLE_DATATYPE   u16

#define HASHTABLE_NSLOT     (HASHTABLE_NPAGE*2)
#define HASHTABLE_NBYTE     (sizeof(HASHTABLE_DATATYPE)*HASHTABLE_NSLOT)

/*
** Return the index in the WalIndex.aData array that corresponds to 
** frame iFrame. The wal-index file consists of a header, followed by
** alternating "map" and "index" blocks.
*/
static int walIndexEntry(u32 iFrame){
  return (
      (WALINDEX_LOCK_OFFSET+WALINDEX_LOCK_RESERVED)/sizeof(u32)
    + (((iFrame-1)/HASHTABLE_NPAGE) * HASHTABLE_NBYTE)/sizeof(u32)
    + (iFrame-1)
  );
}

/*
** Return the minimum mapping size in bytes that can be used to read the
** wal-index up to and including frame iFrame. If iFrame is the last frame
** in a block of 256 frames, the returned byte-count includes the space
** required by the 256-byte index block.
*/
static int walMappingSize(u32 iFrame){
  const int nByte = (sizeof(u32)*HASHTABLE_NPAGE + HASHTABLE_NBYTE) ;
  return ( WALINDEX_LOCK_OFFSET 
         + WALINDEX_LOCK_RESERVED 
         + nByte * ((iFrame + HASHTABLE_NPAGE - 1)/HASHTABLE_NPAGE)
  );
}

/*
** Release our reference to the wal-index memory map, if we are holding
** it.
*/
static void walIndexUnmap(Wal *pWal){
  if( pWal->pWiData ){
    sqlite3OsShmRelease(pWal->pDbFd);
    pWal->pWiData = 0;
  }
}

/*
** Map the wal-index file into memory if it isn't already. 
**
** The reqSize parameter is the minimum required size of the mapping.
** A value of -1 means "don't care".
*/
static int walIndexMap(Wal *pWal, int reqSize){
  int rc = SQLITE_OK;
  if( pWal->pWiData==0 || reqSize>pWal->szWIndex ){
    rc = sqlite3OsShmGet(pWal->pDbFd, reqSize, &pWal->szWIndex,
                             (void**)(char*)&pWal->pWiData);
    if( rc==SQLITE_OK && pWal->pWiData==0 ){
      /* Make sure pWal->pWiData is not NULL while we are holding the
      ** lock on the mapping. */
      assert( pWal->szWIndex==0 );
      pWal->pWiData = &pWal->iCallback;
    }
    if( rc!=SQLITE_OK ){
      walIndexUnmap(pWal);
    }
  }
  return rc;
}

/*
** Remap the wal-index so that the mapping covers the full size
** of the underlying file.
**
** If enlargeTo is non-negative, then increase the size of the underlying
** storage to be at least as big as enlargeTo before remapping.
*/
static int walIndexRemap(Wal *pWal, int enlargeTo){
  int rc;
  int sz;
  rc = sqlite3OsShmSize(pWal->pDbFd, enlargeTo, &sz);
  if( rc==SQLITE_OK && sz>pWal->szWIndex ){
    walIndexUnmap(pWal);
    rc = walIndexMap(pWal, sz);
  }
  return rc;
}

/*
** Increment by which to increase the wal-index file size.
*/
#define WALINDEX_MMAP_INCREMENT (64*1024)

static int walHashKey(u32 iPage){
  return (iPage*2) % (HASHTABLE_NSLOT-1);
}


/* 
** Find the hash table and (section of the) page number array used to
** store data for WAL frame iFrame.
**
** Set output variable *paHash to point to the start of the hash table
** in the wal-index file. Set *piZero to one less than the frame 
** number of the first frame indexed by this hash table. If a
** slot in the hash table is set to N, it refers to frame number 
** (*piZero+N) in the log.
**
** Finally, set *paPgno such that for all frames F between (*piZero+1) and 
** (*piZero+HASHTABLE_NPAGE), (*paPgno)[F] is the database page number 
** associated with frame F.
*/
static void walHashFind(
  Wal *pWal,                      /* WAL handle */
  u32 iFrame,                     /* Find the hash table indexing this frame */
  HASHTABLE_DATATYPE **paHash,    /* OUT: Pointer to hash index */
  u32 **paPgno,                   /* OUT: Pointer to page number array */
  u32 *piZero                     /* OUT: Frame associated with *paPgno[0] */
){
  u32 iZero;
  u32 *aPgno;
  HASHTABLE_DATATYPE *aHash;

  iZero = ((iFrame-1)/HASHTABLE_NPAGE) * HASHTABLE_NPAGE;
  aPgno = &pWal->pWiData[walIndexEntry(iZero+1)-iZero-1];
  aHash = (HASHTABLE_DATATYPE *)&aPgno[iZero+HASHTABLE_NPAGE+1];

  /* Assert that:
  **
  **   + the mapping is large enough for this hash-table, and
  **
  **   + that aPgno[iZero+1] really is the database page number associated
  **     with the first frame indexed by this hash table.
  */
  assert( (u32*)(&aHash[HASHTABLE_NSLOT])<=&pWal->pWiData[pWal->szWIndex/4] );
  assert( walIndexEntry(iZero+1)==(&aPgno[iZero+1] - pWal->pWiData) );

  *paHash = aHash;
  *paPgno = aPgno;
  *piZero = iZero;
}


/*
** Set an entry in the wal-index map to map log frame iFrame to db 
** page iPage. Values are always appended to the wal-index (i.e. the
** value of iFrame is always exactly one more than the value passed to
** the previous call), but that restriction is not enforced or asserted
** here.
*/
static int walIndexAppend(Wal *pWal, u32 iFrame, u32 iPage){
  int rc;                         /* Return code */
  int nMapping;                   /* Required mapping size in bytes */
  
  /* Make sure the wal-index is mapped. Enlarge the mapping if required. */
  nMapping = walMappingSize(iFrame);
  rc = walIndexMap(pWal, -1);
  while( rc==SQLITE_OK && nMapping>pWal->szWIndex ){
    int nByte = pWal->szWIndex + WALINDEX_MMAP_INCREMENT;
    rc = walIndexRemap(pWal, nByte);
  }

  /* Assuming the wal-index file was successfully mapped, find the hash 
  ** table and section of of the page number array that pertain to frame 
  ** iFrame of the WAL. Then populate the page number array and the hash 
  ** table entry.
  */
  if( rc==SQLITE_OK ){
    int iKey;                     /* Hash table key */
    u32 iZero;                    /* One less than frame number of aPgno[1] */
    u32 *aPgno;                   /* Page number array */
    HASHTABLE_DATATYPE *aHash;    /* Hash table */
    int idx;                      /* Value to write to hash-table slot */

    walHashFind(pWal, iFrame, &aHash, &aPgno, &iZero);
    idx = iFrame - iZero;
    if( idx==1 ) memset(aHash, 0, HASHTABLE_NBYTE);
    aPgno[iFrame] = iPage;
    for(iKey=walHashKey(iPage); aHash[iKey]; iKey=(iKey+1)%HASHTABLE_NSLOT);
    aHash[iKey] = idx;
  }

  return rc;
}


/*
** Recover the wal-index by reading the write-ahead log file. 
** The caller must hold RECOVER lock on the wal-index file.
*/
static int walIndexRecover(Wal *pWal){
  int rc;                         /* Return Code */
  i64 nSize;                      /* Size of log file */
  WalIndexHdr hdr;              /* Recovered wal-index header */

  assert( pWal->lockState>SQLITE_SHM_READ );
  memset(&hdr, 0, sizeof(hdr));

  rc = sqlite3OsFileSize(pWal->pWalFd, &nSize);
  if( rc!=SQLITE_OK ){
    return rc;
  }

  if( nSize>WAL_FRAME_HDRSIZE ){
    u8 aBuf[WAL_FRAME_HDRSIZE];   /* Buffer to load first frame header into */
    u8 *aFrame = 0;               /* Malloc'd buffer to load entire frame */
    int nFrame;                   /* Number of bytes at aFrame */
    u8 *aData;                    /* Pointer to data part of aFrame buffer */
    int iFrame;                   /* Index of last frame read */
    i64 iOffset;                  /* Next offset to read from log file */
    int nPgsz;                    /* Page size according to the log */
    u32 aCksum[2];                /* Running checksum */

    /* Read in the first frame header in the file (to determine the 
    ** database page size).
    */
    rc = sqlite3OsRead(pWal->pWalFd, aBuf, WAL_HDRSIZE, 0);
    if( rc!=SQLITE_OK ){
      return rc;
    }

    /* If the database page size is not a power of two, or is greater than
    ** SQLITE_MAX_PAGE_SIZE, conclude that the log file contains no valid data.
    */
    nPgsz = sqlite3Get4byte(&aBuf[0]);
    if( nPgsz&(nPgsz-1) || nPgsz>SQLITE_MAX_PAGE_SIZE || nPgsz<512 ){
      goto finished;
    }
    aCksum[0] = sqlite3Get4byte(&aBuf[4]);
    aCksum[1] = sqlite3Get4byte(&aBuf[8]);

    /* Malloc a buffer to read frames into. */
    nFrame = nPgsz + WAL_FRAME_HDRSIZE;
    aFrame = (u8 *)sqlite3_malloc(nFrame);
    if( !aFrame ){
      return SQLITE_NOMEM;
    }
    aData = &aFrame[WAL_FRAME_HDRSIZE];

    /* Read all frames from the log file. */
    iFrame = 0;
    for(iOffset=WAL_HDRSIZE; (iOffset+nFrame)<=nSize; iOffset+=nFrame){
      u32 pgno;                   /* Database page number for frame */
      u32 nTruncate;              /* dbsize field from frame header */
      int isValid;                /* True if this frame is valid */

      /* Read and decode the next log frame. */
      rc = sqlite3OsRead(pWal->pWalFd, aFrame, nFrame, iOffset);
      if( rc!=SQLITE_OK ) break;
      isValid = walDecodeFrame(aCksum, &pgno, &nTruncate, nPgsz, aData, aFrame);
      if( !isValid ) break;
      rc = walIndexAppend(pWal, ++iFrame, pgno);
      if( rc!=SQLITE_OK ) break;

      /* If nTruncate is non-zero, this is a commit record. */
      if( nTruncate ){
        hdr.iCheck1 = aCksum[0];
        hdr.iCheck2 = aCksum[1];
        hdr.iLastPg = iFrame;
        hdr.nPage = nTruncate;
        hdr.pgsz = nPgsz;
      }
    }

    sqlite3_free(aFrame);
  }else{
    hdr.iCheck1 = 2;
    hdr.iCheck2 = 3;
  }

finished:
  if( rc==SQLITE_OK && hdr.iLastPg==0 ){
    rc = walIndexRemap(pWal, WALINDEX_MMAP_INCREMENT);
  }
  if( rc==SQLITE_OK ){
    walIndexWriteHdr(pWal, &hdr);
    memcpy(&pWal->hdr, &hdr, sizeof(hdr));
  }
  return rc;
}

/*
** Close an open wal-index.
*/
static void walIndexClose(Wal *pWal, int isDelete){
  if( pWal->isWindexOpen ){
    int notUsed;
    sqlite3OsShmLock(pWal->pDbFd, SQLITE_SHM_UNLOCK, &notUsed);
    sqlite3OsShmClose(pWal->pDbFd, isDelete);
    pWal->isWindexOpen = 0;
  }
}

/* 
** Open a connection to the log file associated with database zDb. The
** database file does not actually have to exist. zDb is used only to
** figure out the name of the log file to open. If the log file does not 
** exist it is created by this call.
**
** A SHARED lock should be held on the database file when this function
** is called. The purpose of this SHARED lock is to prevent any other
** client from unlinking the log or wal-index file. If another process
** were to do this just after this client opened one of these files, the
** system would be badly broken.
**
** If the log file is successfully opened, SQLITE_OK is returned and 
** *ppWal is set to point to a new WAL handle. If an error occurs,
** an SQLite error code is returned and *ppWal is left unmodified.
*/
int sqlite3WalOpen(
  sqlite3_vfs *pVfs,              /* vfs module to open wal and wal-index */
  sqlite3_file *pDbFd,            /* The open database file */
  const char *zDbName,            /* Name of the database file */
  Wal **ppWal                     /* OUT: Allocated Wal handle */
){
  int rc;                         /* Return Code */
  Wal *pRet;                      /* Object to allocate and return */
  int flags;                      /* Flags passed to OsOpen() */
  char *zWal;                     /* Name of write-ahead log file */
  int nWal;                       /* Length of zWal in bytes */

  assert( zDbName && zDbName[0] );
  assert( pDbFd );

  /* Allocate an instance of struct Wal to return. */
  *ppWal = 0;
  nWal = sqlite3Strlen30(zDbName) + 5;
  pRet = (Wal*)sqlite3MallocZero(sizeof(Wal) + pVfs->szOsFile + nWal);
  if( !pRet ){
    return SQLITE_NOMEM;
  }

  pRet->pVfs = pVfs;
  pRet->pWalFd = (sqlite3_file *)&pRet[1];
  pRet->pDbFd = pDbFd;
  pRet->zWalName = zWal = pVfs->szOsFile + (char*)pRet->pWalFd;
  sqlite3_snprintf(nWal, zWal, "%s-wal", zDbName);
  rc = sqlite3OsShmOpen(pDbFd);
  pRet->isWindexOpen = 1;

  /* Open file handle on the write-ahead log file. */
  if( rc==SQLITE_OK ){
    flags = (SQLITE_OPEN_READWRITE|SQLITE_OPEN_CREATE|SQLITE_OPEN_MAIN_JOURNAL);
    rc = sqlite3OsOpen(pVfs, zWal, pRet->pWalFd, flags, &flags);
  }

  if( rc!=SQLITE_OK ){
    walIndexClose(pRet, 0);
    sqlite3OsClose(pRet->pWalFd);
    sqlite3_free(pRet);
  }else{
    *ppWal = pRet;
  }
  return rc;
}

static int walIteratorNext(
  WalIterator *p,               /* Iterator */
  u32 *piPage,                  /* OUT: Next db page to write */
  u32 *piFrame                  /* OUT: Wal frame to read from */
){
  u32 iMin = *piPage;
  u32 iRet = 0xFFFFFFFF;
  int i;
  int nBlock = p->nFinal;

  for(i=p->nSegment-1; i>=0; i--){
    struct WalSegment *pSegment = &p->aSegment[i];
    while( pSegment->iNext<nBlock ){
      u32 iPg = pSegment->aDbPage[pSegment->aIndex[pSegment->iNext]];
      if( iPg>iMin ){
        if( iPg<iRet ){
          iRet = iPg;
          *piFrame = i*256 + 1 + pSegment->aIndex[pSegment->iNext];
        }
        break;
      }
      pSegment->iNext++;
    }
    nBlock = 256;
  }

  *piPage = iRet;
  return (iRet==0xFFFFFFFF);
}

static int walIteratorInit(Wal *pWal, WalIterator **pp){
  u32 *aData;                     /* Content of the wal-index file */
  WalIterator *p;                 /* Return value */
  int nSegment;                   /* Number of segments to merge */
  u32 iLast;                      /* Last frame in log */
  int nByte;                      /* Number of bytes to allocate */
  int i;                          /* Iterator variable */
  int nFinal;                     /* Number of unindexed entries */
  u8 *aTmp;                       /* Temp space used by merge-sort */
  int rc;                         /* Return code of walIndexMap() */

  rc = walIndexMap(pWal, walMappingSize(pWal->hdr.iLastPg));
  if( rc!=SQLITE_OK ){
    return rc;
  }
  aData = pWal->pWiData;
  iLast = pWal->hdr.iLastPg;
  nSegment = (iLast >> 8) + 1;
  nFinal = (iLast & 0x000000FF);

  nByte = sizeof(WalIterator) + (nSegment+1)*(sizeof(struct WalSegment)+256);
  p = (WalIterator *)sqlite3_malloc(nByte);
  if( !p ){
    rc = SQLITE_NOMEM;
  }else{
    u8 *aSpace;
    memset(p, 0, nByte);
    p->nSegment = nSegment;

    aSpace = (u8 *)&p->aSegment[nSegment];
    aTmp = &aSpace[nSegment*256];
    for(i=0; i<nSegment; i++){
      int j;
      int nIndex = (i==nSegment-1) ? nFinal : 256;
      p->aSegment[i].aDbPage = &aData[walIndexEntry(i*256+1)];
      p->aSegment[i].aIndex = aSpace;
      for(j=0; j<nIndex; j++){
        aSpace[j] = j;
      }
      walMergesort8(p->aSegment[i].aDbPage, aTmp, aSpace, &nIndex);
      memset(&aSpace[nIndex], aSpace[nIndex-1], 256-nIndex);
      aSpace += 256;
      p->nFinal = nIndex;
    }
  }

  *pp = p;
  return rc;
}

/* 
** Free a log iterator allocated by walIteratorInit().
*/
static void walIteratorFree(WalIterator *p){
  sqlite3_free(p);
}

/*
** Checkpoint the contents of the log file.
*/
static int walCheckpoint(
  Wal *pWal,                      /* Wal connection */
  int sync_flags,                 /* Flags for OsSync() (or 0) */
  int nBuf,                       /* Size of zBuf in bytes */
  u8 *zBuf                        /* Temporary buffer to use */
){
  int rc;                         /* Return code */
  int pgsz = pWal->hdr.pgsz;      /* Database page-size */
  WalIterator *pIter = 0;         /* Wal iterator context */
  u32 iDbpage = 0;                /* Next database page to write */
  u32 iFrame = 0;                 /* Wal frame containing data for iDbpage */

  /* Allocate the iterator */
  rc = walIteratorInit(pWal, &pIter);
  if( rc!=SQLITE_OK || pWal->hdr.iLastPg==0 ){
    goto out;
  }

  if( pWal->hdr.pgsz!=nBuf ){
    rc = SQLITE_CORRUPT_BKPT;
    goto out;
  }

  /* Sync the log file to disk */
  if( sync_flags ){
    rc = sqlite3OsSync(pWal->pWalFd, sync_flags);
    if( rc!=SQLITE_OK ) goto out;
  }

  /* Iterate through the contents of the log, copying data to the db file. */
  while( 0==walIteratorNext(pIter, &iDbpage, &iFrame) ){
    rc = sqlite3OsRead(pWal->pWalFd, zBuf, pgsz, 
        walFrameOffset(iFrame, pgsz) + WAL_FRAME_HDRSIZE
    );
    if( rc!=SQLITE_OK ) goto out;
    rc = sqlite3OsWrite(pWal->pDbFd, zBuf, pgsz, (iDbpage-1)*pgsz);
    if( rc!=SQLITE_OK ) goto out;
  }

  /* Truncate the database file */
  rc = sqlite3OsTruncate(pWal->pDbFd, ((i64)pWal->hdr.nPage*(i64)pgsz));
  if( rc!=SQLITE_OK ) goto out;

  /* Sync the database file. If successful, update the wal-index. */
  if( sync_flags ){
    rc = sqlite3OsSync(pWal->pDbFd, sync_flags);
    if( rc!=SQLITE_OK ) goto out;
  }
  pWal->hdr.iLastPg = 0;
  pWal->hdr.iCheck1 = 2;
  pWal->hdr.iCheck2 = 3;
  walIndexWriteHdr(pWal, &pWal->hdr);

  /* TODO: If a crash occurs and the current log is copied into the 
  ** database there is no problem. However, if a crash occurs while
  ** writing the next transaction into the start of the log, such that:
  **
  **   * The first transaction currently in the log is left intact, but
  **   * The second (or subsequent) transaction is damaged,
  **
  ** then the database could become corrupt.
  **
  ** The easiest thing to do would be to write and sync a dummy header
  ** into the log at this point. Unfortunately, that turns out to be
  ** an unwelcome performance hit. Alternatives are...
  */
#if 0 
  memset(zBuf, 0, WAL_FRAME_HDRSIZE);
  rc = sqlite3OsWrite(pWal->pWalFd, zBuf, WAL_FRAME_HDRSIZE, 0);
  if( rc!=SQLITE_OK ) goto out;
  rc = sqlite3OsSync(pWal->pWalFd, pWal->sync_flags);
#endif

 out:
  walIteratorFree(pIter);
  return rc;
}

/*
** Close a connection to a log file.
*/
int sqlite3WalClose(
  Wal *pWal,                      /* Wal to close */
  int sync_flags,                 /* Flags to pass to OsSync() (or 0) */
  int nBuf,
  u8 *zBuf                        /* Buffer of at least nBuf bytes */
){
  int rc = SQLITE_OK;
  if( pWal ){
    int isDelete = 0;             /* True to unlink wal and wal-index files */

    /* If an EXCLUSIVE lock can be obtained on the database file (using the
    ** ordinary, rollback-mode locking methods, this guarantees that the
    ** connection associated with this log file is the only connection to
    ** the database. In this case checkpoint the database and unlink both
    ** the wal and wal-index files.
    **
    ** The EXCLUSIVE lock is not released before returning.
    */
    rc = sqlite3OsLock(pWal->pDbFd, SQLITE_LOCK_EXCLUSIVE);
    if( rc==SQLITE_OK ){
      rc = sqlite3WalCheckpoint(pWal, sync_flags, nBuf, zBuf, 0, 0);
      if( rc==SQLITE_OK ){
        isDelete = 1;
      }
      walIndexUnmap(pWal);
    }

    walIndexClose(pWal, isDelete);
    sqlite3OsClose(pWal->pWalFd);
    if( isDelete ){
      sqlite3OsDelete(pWal->pVfs, pWal->zWalName, 0);
    }
    sqlite3_free(pWal);
  }
  return rc;
}

/*
** Try to read the wal-index header. Attempt to verify the header
** checksum. If the checksum can be verified, copy the wal-index
** header into structure pWal->hdr. If the contents of pWal->hdr are
** modified by this and pChanged is not NULL, set *pChanged to 1. 
** Otherwise leave *pChanged unmodified.
**
** If the checksum cannot be verified return non-zero. If the header
** is read successfully and the checksum verified, return zero.
*/
int walIndexTryHdr(Wal *pWal, int *pChanged){
  u32 aCksum[2] = {1, 1};
  u32 aHdr[WALINDEX_HDR_NFIELD+2];

  assert( pWal->pWiData );
  if( pWal->szWIndex==0 ){
    /* The wal-index is of size 0 bytes. This is handled in the same way
    ** as an invalid header. The caller will run recovery to construct
    ** a valid wal-index file before accessing the database.
    */
    return 1;
  }

  /* Read the header. The caller may or may not have an exclusive 
  ** (WRITE, PENDING, CHECKPOINT or RECOVER) lock on the wal-index
  ** file, meaning it is possible that an inconsistent snapshot is read
  ** from the file. If this happens, return non-zero.
  */
  memcpy(aHdr, pWal->pWiData, sizeof(aHdr));
  walChecksumBytes((u8*)aHdr, sizeof(u32)*WALINDEX_HDR_NFIELD, aCksum);
  if( aCksum[0]!=aHdr[WALINDEX_HDR_NFIELD]
   || aCksum[1]!=aHdr[WALINDEX_HDR_NFIELD+1]
  ){
    return 1;
  }

  if( memcmp(&pWal->hdr, aHdr, sizeof(WalIndexHdr)) ){
    *pChanged = 1;
    memcpy(&pWal->hdr, aHdr, sizeof(WalIndexHdr));
  }

  /* The header was successfully read. Return zero. */
  return 0;
}

/*
** Read the wal-index header from the wal-index file into structure 
** pWal->hdr. If attempting to verify the header checksum fails, try
** to recover the log before returning.
**
** If the wal-index header is successfully read, return SQLITE_OK. 
** Otherwise an SQLite error code.
*/
static int walIndexReadHdr(Wal *pWal, int *pChanged){
  int rc;                         /* Return code */
  int lockState;                  /* pWal->lockState before running recovery */

  assert( pWal->lockState>=SQLITE_SHM_READ );
  assert( pChanged );
  rc = walIndexMap(pWal, -1);
  if( rc!=SQLITE_OK ){
    return rc;
  }

  /* First attempt to read the wal-index header. This may fail for one
  ** of two reasons: (a) the wal-index does not yet exist or has been
  ** corrupted and needs to be constructed by running recovery, or (b)
  ** the caller is only holding a READ lock and made a dirty read of
  ** the wal-index header.
  **
  ** A dirty read of the wal-index header occurs if another thread or
  ** process happens to be writing to the wal-index header at roughly
  ** the same time as this thread is reading it. In this case it is 
  ** possible that an inconsistent header is read (which is detected
  ** using the header checksum mechanism).
  */
  if( walIndexTryHdr(pWal, pChanged)==0 ){
    return SQLITE_OK;
  }

  /* If the first attempt to read the header failed, lock the wal-index
  ** file with an exclusive lock and try again. If the header checksum 
  ** verification fails again, we can be sure that it is not simply a
  ** dirty read, but that the wal-index really does need to be 
  ** reconstructed by running log recovery.
  **
  ** In the paragraph above, an "exclusive lock" may be any of WRITE,
  ** PENDING, CHECKPOINT or RECOVER. If any of these are already held,
  ** no locking operations are required. If the caller currently holds
  ** a READ lock, then upgrade to a RECOVER lock before re-reading the
  ** wal-index header and revert to a READ lock before returning.
  */
  lockState = pWal->lockState;
  if( lockState>SQLITE_SHM_READ
   || SQLITE_OK==(rc = walSetLock(pWal, SQLITE_SHM_RECOVER)) 
  ){
    if( walIndexTryHdr(pWal, pChanged) ){
      *pChanged = 1;
      rc = walIndexRecover(pWal);
    }
    if( lockState==SQLITE_SHM_READ ){
      walSetLock(pWal, SQLITE_SHM_READ);
    }
  }

  return rc;
}

/*
** Lock a snapshot.
**
** If this call obtains a new read-lock and the database contents have been
** modified since the most recent call to WalCloseSnapshot() on this Wal
** connection, then *pChanged is set to 1 before returning. Otherwise, it 
** is left unmodified. This is used by the pager layer to determine whether 
** or not any cached pages may be safely reused.
*/
int sqlite3WalOpenSnapshot(Wal *pWal, int *pChanged){
  int rc;                         /* Return code */

  rc = walSetLock(pWal, SQLITE_SHM_READ);
  assert( rc!=SQLITE_OK || pWal->lockState==SQLITE_SHM_READ );

  if( rc==SQLITE_OK ){
    rc = walIndexReadHdr(pWal, pChanged);
    if( rc!=SQLITE_OK ){
      /* An error occured while attempting log recovery. */
      sqlite3WalCloseSnapshot(pWal);
    }
  }

  walIndexUnmap(pWal);
  return rc;
}

/*
** Unlock the current snapshot.
*/
void sqlite3WalCloseSnapshot(Wal *pWal){
  assert( pWal->lockState==SQLITE_SHM_READ
       || pWal->lockState==SQLITE_SHM_UNLOCK
  );
  walSetLock(pWal, SQLITE_SHM_UNLOCK);
}

/*
** Read a page from the log, if it is present. 
*/
int sqlite3WalRead(
  Wal *pWal,                      /* WAL handle */
  Pgno pgno,                      /* Database page number to read data for */
  int *pInWal,                    /* OUT: True if data is read from WAL */
  int nOut,                       /* Size of buffer pOut in bytes */
  u8 *pOut                        /* Buffer to write page data to */
){
  int rc;                         /* Return code */
  u32 iRead = 0;                  /* If !=0, WAL frame to return data from */
  u32 iLast = pWal->hdr.iLastPg;  /* Last page in WAL for this reader */
  int iHash;                      /* Used to loop through N hash tables */

  /* If the "last page" field of the wal-index header snapshot is 0, then
  ** no data will be read from the wal under any circumstances. Return early
  ** in this case to avoid the walIndexMap/Unmap overhead.
  */
  if( iLast==0 ){
    *pInWal = 0;
    return SQLITE_OK;
  }

  /* Ensure the wal-index is mapped. */
  assert( pWal->lockState==SQLITE_SHM_READ||pWal->lockState==SQLITE_SHM_WRITE );
  rc = walIndexMap(pWal, walMappingSize(iLast));
  if( rc!=SQLITE_OK ){
    return rc;
  }

  /* Search the hash table or tables for an entry matching page number
  ** pgno. Each iteration of the following for() loop searches one
  ** hash table (each hash table indexes up to HASHTABLE_NPAGE frames).
  **
  ** This code may run concurrently to the code in walIndexAppend()
  ** that adds entries to the wal-index (and possibly to this hash 
  ** table). This means the non-zero value just read from the hash 
  ** slot (aHash[iKey]) may have been added before or after the 
  ** current read transaction was opened. Values added after the
  ** read transaction was opened may have been written incorrectly -
  ** i.e. these slots may contain garbage data. However, we assume
  ** that any slots written before the current read transaction was
  ** opened remain unmodified.
  **
  ** For the reasons above, the if(...) condition featured in the inner
  ** loop of the following block is more stringent that would be required 
  ** if we had exclusive access to the hash-table:
  **
  **   (aPgno[iFrame]==pgno): 
  **     This condition filters out normal hash-table collisions.
  **
  **   (iFrame<=iLast): 
  **     This condition filters out entries that were added to the hash
  **     table after the current read-transaction had started.
  **
  **   (iFrame>iRead): 
  **     This filters out a dangerous class of garbage data. The 
  **     garbage hash slot may refer to a frame with the correct page 
  **     number, but not the most recent version of the frame. For
  **     example, if at the start of the read-transaction the log 
  **     contains three copies of the desired page in frames 2, 3 and 4,
  **     the hash table may contain the following:
  **
  **       { ..., 2, 3, 4, 0, 0, ..... }
  **
  **     The correct answer is to read data from frame 4. But a 
  **     dirty-read may potentially cause the hash-table to appear as 
  **     follows to the reader:
  **
  **       { ..., 2, 3, 4, 3, 0, ..... }
  **
  **     Without this part of the if(...) clause, the reader might
  **     incorrectly read data from frame 3 instead of 4. This would be
  **     an error.
  **
  ** It is not actually clear to the developers that such a dirty-read
  ** can occur. But if it does, it should not cause any problems.
  */
  for(iHash=iLast; iHash>0 && iRead==0; iHash-=HASHTABLE_NPAGE){
    HASHTABLE_DATATYPE *aHash;    /* Pointer to hash table */
    u32 *aPgno;                   /* Pointer to array of page numbers */
    u32 iZero;                    /* Frame number corresponding to aPgno[0] */
    int iKey;                     /* Hash slot index */

    walHashFind(pWal, iHash, &aHash, &aPgno, &iZero);
    for(iKey=walHashKey(pgno); aHash[iKey]; iKey=(iKey+1)%HASHTABLE_NSLOT){
      u32 iFrame = aHash[iKey] + iZero;
      if( iFrame<=iLast && aPgno[iFrame]==pgno && iFrame>iRead ){
        iRead = iFrame;
      }
    }
  }
  assert( iRead==0 || pWal->pWiData[walIndexEntry(iRead)]==pgno );

#ifdef SQLITE_ENABLE_EXPENSIVE_ASSERT
  /* If expensive assert() statements are available, do a linear search
  ** of the wal-index file content. Make sure the results agree with the
  ** result obtained using the hash indexes above.  */
  {
    u32 iRead2 = 0;
    u32 iTest;
    for(iTest=iLast; iTest>0; iTest--){
      if( pWal->pWiData[walIndexEntry(iTest)]==pgno ){
        iRead2 = iTest;
        break;
      }
    }
    assert( iRead==iRead2 );
  }
#endif

  /* If iRead is non-zero, then it is the log frame number that contains the
  ** required page. Read and return data from the log file.
  */
  walIndexUnmap(pWal);
  if( iRead ){
    i64 iOffset = walFrameOffset(iRead, pWal->hdr.pgsz) + WAL_FRAME_HDRSIZE;
    *pInWal = 1;
    return sqlite3OsRead(pWal->pWalFd, pOut, nOut, iOffset);
  }

  *pInWal = 0;
  return SQLITE_OK;
}


/* 
** Set *pPgno to the size of the database file (or zero, if unknown).
*/
void sqlite3WalDbsize(Wal *pWal, Pgno *pPgno){
  assert( pWal->lockState==SQLITE_SHM_READ
       || pWal->lockState==SQLITE_SHM_WRITE );
  *pPgno = pWal->hdr.nPage;
}

/* 
** This function returns SQLITE_OK if the caller may write to the database.
** Otherwise, if the caller is operating on a snapshot that has already
** been overwritten by another writer, SQLITE_BUSY is returned.
*/
int sqlite3WalWriteLock(Wal *pWal, int op){
  int rc = SQLITE_OK;
  if( op ){
    assert( pWal->lockState==SQLITE_SHM_READ );
    rc = walSetLock(pWal, SQLITE_SHM_WRITE);

    /* If this connection is not reading the most recent database snapshot,
    ** it is not possible to write to the database. In this case release
    ** the write locks and return SQLITE_BUSY.
    */
    if( rc==SQLITE_OK ){
      rc = walIndexMap(pWal, sizeof(WalIndexHdr));
      if( rc==SQLITE_OK
       && memcmp(&pWal->hdr, pWal->pWiData, sizeof(WalIndexHdr))
      ){
        rc = SQLITE_BUSY;
      }
      walIndexUnmap(pWal);
      if( rc!=SQLITE_OK ){
        walSetLock(pWal, SQLITE_SHM_READ);
      }
    }
  }else if( pWal->lockState==SQLITE_SHM_WRITE ){
    rc = walSetLock(pWal, SQLITE_SHM_READ);
  }
  return rc;
}

/*
** If any data has been written (but not committed) to the log file, this
** function moves the write-pointer back to the start of the transaction.
**
** Additionally, the callback function is invoked for each frame written
** to the log since the start of the transaction. If the callback returns
** other than SQLITE_OK, it is not invoked again and the error code is
** returned to the caller.
**
** Otherwise, if the callback function does not return an error, this
** function returns SQLITE_OK.
*/
int sqlite3WalUndo(Wal *pWal, int (*xUndo)(void *, Pgno), void *pUndoCtx){
  int rc = SQLITE_OK;
  if( pWal->lockState==SQLITE_SHM_WRITE ){
    int unused;
    Pgno iMax = pWal->hdr.iLastPg;
    Pgno iFrame;
  
    assert( pWal->pWiData==0 );
    rc = walIndexReadHdr(pWal, &unused);
    for(iFrame=pWal->hdr.iLastPg+1; rc==SQLITE_OK && iFrame<=iMax; iFrame++){
      assert( pWal->lockState==SQLITE_SHM_WRITE );
      rc = xUndo(pUndoCtx, pWal->pWiData[walIndexEntry(iFrame)]);
    }
    walIndexUnmap(pWal);
  }
  return rc;
}

/* Return an integer that records the current (uncommitted) write
** position in the WAL
*/
u32 sqlite3WalSavepoint(Wal *pWal){
  assert( pWal->lockState==SQLITE_SHM_WRITE );
  return pWal->hdr.iLastPg;
}

/* Move the write position of the WAL back to iFrame.  Called in
** response to a ROLLBACK TO command.
*/
int sqlite3WalSavepointUndo(Wal *pWal, u32 iFrame){
  int rc = SQLITE_OK;
  u8 aCksum[8];
  assert( pWal->lockState==SQLITE_SHM_WRITE );

  pWal->hdr.iLastPg = iFrame;
  if( iFrame>0 ){
    i64 iOffset = walFrameOffset(iFrame, pWal->hdr.pgsz) + sizeof(u32)*2;
    rc = sqlite3OsRead(pWal->pWalFd, aCksum, sizeof(aCksum), iOffset);
    pWal->hdr.iCheck1 = sqlite3Get4byte(&aCksum[0]);
    pWal->hdr.iCheck2 = sqlite3Get4byte(&aCksum[4]);
  }

  return rc;
}

/* 
** Write a set of frames to the log. The caller must hold the write-lock
** on the log file (obtained using sqlite3WalWriteLock()).
*/
int sqlite3WalFrames(
  Wal *pWal,                      /* Wal handle to write to */
  int nPgsz,                      /* Database page-size in bytes */
  PgHdr *pList,                   /* List of dirty pages to write */
  Pgno nTruncate,                 /* Database size after this commit */
  int isCommit,                   /* True if this is a commit */
  int sync_flags                  /* Flags to pass to OsSync() (or 0) */
){
  int rc;                         /* Used to catch return codes */
  u32 iFrame;                     /* Next frame address */
  u8 aFrame[WAL_FRAME_HDRSIZE];   /* Buffer to assemble frame-header in */
  PgHdr *p;                       /* Iterator to run through pList with. */
  u32 aCksum[2];                  /* Checksums */
  PgHdr *pLast = 0;               /* Last frame in list */
  int nLast = 0;                  /* Number of extra copies of last page */

  assert( WAL_FRAME_HDRSIZE==(4 * 2 + 2*sizeof(u32)) );
  assert( pList );
  assert( pWal->lockState==SQLITE_SHM_WRITE );
  assert( pWal->pWiData==0 );

  /* If this is the first frame written into the log, write the log 
  ** header to the start of the log file. See comments at the top of
  ** this file for a description of the log-header format.
  */
  assert( WAL_FRAME_HDRSIZE>=WAL_HDRSIZE );
  iFrame = pWal->hdr.iLastPg;
  if( iFrame==0 ){
    sqlite3Put4byte(aFrame, nPgsz);
    sqlite3_randomness(8, &aFrame[4]);
    pWal->hdr.iCheck1 = sqlite3Get4byte(&aFrame[4]);
    pWal->hdr.iCheck2 = sqlite3Get4byte(&aFrame[8]);
    rc = sqlite3OsWrite(pWal->pWalFd, aFrame, WAL_HDRSIZE, 0);
    if( rc!=SQLITE_OK ){
      return rc;
    }
  }

  aCksum[0] = pWal->hdr.iCheck1;
  aCksum[1] = pWal->hdr.iCheck2;

  /* Write the log file. */
  for(p=pList; p; p=p->pDirty){
    u32 nDbsize;                  /* Db-size field for frame header */
    i64 iOffset;                  /* Write offset in log file */

    iOffset = walFrameOffset(++iFrame, nPgsz);
    
    /* Populate and write the frame header */
    nDbsize = (isCommit && p->pDirty==0) ? nTruncate : 0;
    walEncodeFrame(aCksum, p->pgno, nDbsize, nPgsz, p->pData, aFrame);
    rc = sqlite3OsWrite(pWal->pWalFd, aFrame, sizeof(aFrame), iOffset);
    if( rc!=SQLITE_OK ){
      return rc;
    }

    /* Write the page data */
    rc = sqlite3OsWrite(pWal->pWalFd, p->pData, nPgsz, iOffset + sizeof(aFrame));
    if( rc!=SQLITE_OK ){
      return rc;
    }
    pLast = p;
  }

  /* Sync the log file if the 'isSync' flag was specified. */
  if( sync_flags ){
    i64 iSegment = sqlite3OsSectorSize(pWal->pWalFd);
    i64 iOffset = walFrameOffset(iFrame+1, nPgsz);

    assert( isCommit );

    if( iSegment<SQLITE_DEFAULT_SECTOR_SIZE ){
      iSegment = SQLITE_DEFAULT_SECTOR_SIZE;
    }
    iSegment = (((iOffset+iSegment-1)/iSegment) * iSegment);
    while( iOffset<iSegment ){
      walEncodeFrame(aCksum,pLast->pgno,nTruncate,nPgsz,pLast->pData,aFrame);
      rc = sqlite3OsWrite(pWal->pWalFd, aFrame, sizeof(aFrame), iOffset);
      if( rc!=SQLITE_OK ){
        return rc;
      }

      iOffset += WAL_FRAME_HDRSIZE;
      rc = sqlite3OsWrite(pWal->pWalFd, pLast->pData, nPgsz, iOffset); 
      if( rc!=SQLITE_OK ){
        return rc;
      }
      nLast++;
      iOffset += nPgsz;
    }

    rc = sqlite3OsSync(pWal->pWalFd, sync_flags);
  }
  assert( pWal->pWiData==0 );

  /* Append data to the log summary. It is not necessary to lock the 
  ** wal-index to do this as the RESERVED lock held on the db file
  ** guarantees that there are no other writers, and no data that may
  ** be in use by existing readers is being overwritten.
  */
  iFrame = pWal->hdr.iLastPg;
  for(p=pList; p && rc==SQLITE_OK; p=p->pDirty){
    iFrame++;
    rc = walIndexAppend(pWal, iFrame, p->pgno);
  }
  while( nLast>0 && rc==SQLITE_OK ){
    iFrame++;
    nLast--;
    rc = walIndexAppend(pWal, iFrame, pLast->pgno);
  }

  if( rc==SQLITE_OK ){
    /* Update the private copy of the header. */
    pWal->hdr.pgsz = nPgsz;
    pWal->hdr.iLastPg = iFrame;
    if( isCommit ){
      pWal->hdr.iChange++;
      pWal->hdr.nPage = nTruncate;
    }
    pWal->hdr.iCheck1 = aCksum[0];
    pWal->hdr.iCheck2 = aCksum[1];

    /* If this is a commit, update the wal-index header too. */
    if( isCommit ){
      walIndexWriteHdr(pWal, &pWal->hdr);
      pWal->iCallback = iFrame;
    }
  }

  walIndexUnmap(pWal);
  return rc;
}

/* 
** Checkpoint the database:
**
**   1. Acquire a CHECKPOINT lock
**   2. Copy the contents of the log into the database file.
**   3. Zero the wal-index header (so new readers will ignore the log).
**   4. Drop the CHECKPOINT lock.
*/
int sqlite3WalCheckpoint(
  Wal *pWal,                      /* Wal connection */
  int sync_flags,                 /* Flags to sync db file with (or 0) */
  int nBuf,                       /* Size of temporary buffer */
  u8 *zBuf,                       /* Temporary buffer to use */
  int (*xBusyHandler)(void *),    /* Pointer to busy-handler function */
  void *pBusyHandlerArg           /* Argument to pass to xBusyHandler */
){
  int rc;                         /* Return code */
  int isChanged = 0;              /* True if a new wal-index header is loaded */

  assert( pWal->pWiData==0 );

  /* Get the CHECKPOINT lock. 
  **
  ** Normally, the connection will be in UNLOCK state at this point. But
  ** if the connection is in exclusive-mode it may still be in READ state
  ** even though the upper layer has no active read-transaction (because
  ** WalCloseSnapshot() is not called in exclusive mode). The state will
  ** be set to UNLOCK when this function returns. This is Ok.
  */
  assert( (pWal->lockState==SQLITE_SHM_UNLOCK)
       || (pWal->exclusiveMode && pWal->lockState==SQLITE_SHM_READ)
  );
  do {
    rc = walSetLock(pWal, SQLITE_SHM_CHECKPOINT);
  }while( rc==SQLITE_BUSY && xBusyHandler(pBusyHandlerArg) );
  if( rc!=SQLITE_OK ){
    walSetLock(pWal, SQLITE_SHM_UNLOCK);
    return rc;
  }

  /* Copy data from the log to the database file. */
  rc = walIndexReadHdr(pWal, &isChanged);
  if( rc==SQLITE_OK ){
    rc = walCheckpoint(pWal, sync_flags, nBuf, zBuf);
  }
  if( isChanged ){
    /* If a new wal-index header was loaded before the checkpoint was 
    ** performed, then the pager-cache associated with log pWal is now
    ** out of date. So zero the cached wal-index header to ensure that
    ** next time the pager opens a snapshot on this database it knows that
    ** the cache needs to be reset.
    */
    memset(&pWal->hdr, 0, sizeof(WalIndexHdr));
  }

  /* Release the locks. */
  walIndexUnmap(pWal);
  walSetLock(pWal, SQLITE_SHM_UNLOCK);
  return rc;
}

/* Return the value to pass to a sqlite3_wal_hook callback, the
** number of frames in the WAL at the point of the last commit since
** sqlite3WalCallback() was called.  If no commits have occurred since
** the last call, then return 0.
*/
int sqlite3WalCallback(Wal *pWal){
  u32 ret = 0;
  if( pWal ){
    ret = pWal->iCallback;
    pWal->iCallback = 0;
  }
  return (int)ret;
}

/*
** This function is called to set or query the exclusive-mode flag 
** associated with the WAL connection passed as the first argument. The
** exclusive-mode flag should be set to indicate that the caller is
** holding an EXCLUSIVE lock on the database file (it does this in
** locking_mode=exclusive mode). If the EXCLUSIVE lock is to be dropped,
** the flag set by this function should be cleared before doing so.
**
** The value of the exclusive-mode flag may only be modified when
** the WAL connection is in READ state.
**
** When the flag is set, this module does not call the VFS xShmLock()
** method to obtain any locks on the wal-index (as it assumes it
** has exclusive access to the wal and wal-index files anyhow). It
** continues to hold (and does not drop) the existing READ lock on
** the wal-index.
**
** To set or clear the flag, the "op" parameter is passed 1 or 0,
** respectively. To query the flag, pass -1. In all cases, the value
** returned is the value of the exclusive-mode flag (after its value
** has been modified, if applicable).
*/
int sqlite3WalExclusiveMode(Wal *pWal, int op){
  if( op>=0 ){
    assert( pWal->lockState==SQLITE_SHM_READ );
    pWal->exclusiveMode = (u8)op;
  }
  return pWal->exclusiveMode;
}

#endif /* #ifndef SQLITE_OMIT_WAL */

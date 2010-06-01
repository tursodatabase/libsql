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
** This file contains the implementation of a write-ahead log (WAL) used in 
** "journal_mode=WAL" mode.
**
** WRITE-AHEAD LOG (WAL) FILE FORMAT
**
** A WAL file consists of a header followed by zero or more "frames".
** Each frame records the revised content of a single page from the
** database file.  All changes to the database are recorded by writing
** frames into the WAL.  Transactions commit when a frame is written that
** contains a commit marker.  A single WAL can and usually does record 
** multiple transactions.  Periodically, the content of the WAL is
** transferred back into the database file in an operation called a
** "checkpoint".
**
** A single WAL file can be used multiple times.  In other words, the
** WAL can fill up with frames and then be checkpointed and then new
** frames can overwrite the old ones.  A WAL always grows from beginning
** toward the end.  Checksums and counters attached to each frame are
** used to determine which frames within the WAL are valid and which
** are leftovers from prior checkpoints.
**
** The WAL header is 24 bytes in size and consists of the following six
** big-endian 32-bit unsigned integer values:
**
**     0: Magic number.  0x377f0682 or 0x377f0683
**     4: File format version.  Currently 3007000
**     8: Database page size.  Example: 1024
**    12: Checkpoint sequence number
**    16: Salt-1, random integer incremented with each checkpoint
**    20: Salt-2, a different random integer changing with each ckpt
**
** Immediately following the wal-header are zero or more frames. Each
** frame consists of a 24-byte frame-header followed by a <page-size> bytes
** of page data. The frame-header is broken into 6 big-endian 32-bit unsigned 
** integer values, as follows:
**
**     0: Page number.
**     4: For commit records, the size of the database image in pages 
**        after the commit. For all other records, zero.
**     8: Salt-1 (copied from the header)
**    12: Salt-2 (copied from the header)
**    16: Checksum-1.
**    20: Checksum-2.
**
** A frame is considered valid if and only if the following conditions are
** true:
**
**    (1) The salt-1 and salt-2 values in the frame-header match
**        salt values in the wal-header
**
**    (2) The checksum values in the final 8 bytes of the frame-header
**        exactly match the checksum computed consecutively on the
**        WAL header and the first 8 bytes and the content of all frames
**        up to and including the current frame.
**
** The checksum is computed using 32-bit big-endian integers if the
** magic number in the first 4 bytes of the WAL is 0x377f0683 and it
** is computed using little-endian if the magic number is 0x377f0682.
** The checksum values are always stored in the frame header in a
** big-endian format regardless of which byte order is used to compute
** the checksum.  The checksum is computed by interpreting the input as
** an even number of unsigned 32-bit integers: x[0] through x[N].  The
** 
**   for i from 0 to n-1 step 2:
**     s0 += x[i] + s1;
**     s1 += x[i+1] + s0;
**   endfor
**
** On a checkpoint, the WAL is first VFS.xSync-ed, then valid content of the
** WAL is transferred into the database, then the database is VFS.xSync-ed.
** The VFS.xSync operations server as write barriers - all writes launched
** before the xSync must complete before any write that launches after the
** xSync begins.
**
** After each checkpoint, the salt-1 value is incremented and the salt-2
** value is randomized.  This prevents old and new frames in the WAL from
** being considered valid at the same time and being checkpointing together
** following a crash.
**
** READER ALGORITHM
**
** To read a page from the database (call it page number P), a reader
** first checks the WAL to see if it contains page P.  If so, then the
** last valid instance of page P that is a followed by a commit frame
** or is a commit frame itself becomes the value read.  If the WAL
** contains no copies of page P that are valid and which are a commit
** frame or are followed by a commit frame, then page P is read from
** the database file.
**
** To start a read transaction, the reader records the index of the last
** valid frame in the WAL.  The reader uses this recorded "mxFrame" value
** for all subsequent read operations.  New transactions can be appended
** to the WAL, but as long as the reader uses its original mxFrame value
** and ignores the newly appended content, it will see a consistent snapshot
** of the database from a single point in time.  This technique allows
** multiple concurrent readers to view different versions of the database
** content simultaneously.
**
** The reader algorithm in the previous paragraphs works correctly, but 
** because frames for page P can appear anywhere within the WAL, the
** reader has to scan the entire WAL looking for page P frames.  If the
** WAL is large (multiple megabytes is typical) that scan can be slow,
** and read performance suffers.  To overcome this problem, a separate
** data structure called the wal-index is maintained to expedite the
** search for frames of a particular page.
** 
** WAL-INDEX FORMAT
**
** Conceptually, the wal-index is shared memory, though VFS implementations
** might choose to implement the wal-index using a mmapped file.  Because
** the wal-index is shared memory, SQLite does not support journal_mode=WAL 
** on a network filesystem.  All users of the database must be able to
** share memory.
**
** The wal-index is transient.  After a crash, the wal-index can (and should
** be) reconstructed from the original WAL file.  In fact, the VFS is required
** to either truncate or zero the header of the wal-index when the last
** connection to it closes.  Because the wal-index is transient, it can
** use an architecture-specific format; it does not have to be cross-platform.
** Hence, unlike the database and WAL file formats which store all values
** as big endian, the wal-index can store multi-byte values in the native
** byte order of the host computer.
**
** The purpose of the wal-index is to answer this question quickly:  Given
** a page number P, return the index of the last frame for page P in the WAL,
** or return NULL if there are no frames for page P in the WAL.
**
** The wal-index consists of a header region, followed by an one or
** more index blocks.  
**
** The wal-index header contains the total number of frames within the WAL
** in the the mxFrame field.  Each index block contains information on
** HASHTABLE_NPAGE frames.  Each index block contains two sections, a
** mapping which is a database page number for each frame, and a hash
** table used to look up frames by page number.  The mapping section is
** an array of HASHTABLE_NPAGE 32-bit page numbers.  The first entry on the
** array is the page number for the first frame; the second entry is the
** page number for the second frame; and so forth.  The last index block
** holds a total of (mxFrame%HASHTABLE_NPAGE) page numbers.  All index
** blocks other than the last are completely full with HASHTABLE_NPAGE
** page numbers.  All index blocks are the same size; the mapping section
** of the last index block merely contains unused entries if mxFrame is
** not an even multiple of HASHTABLE_NPAGE.
**
** Even without using the hash table, the last frame for page P
** can be found by scanning the mapping sections of each index block
** starting with the last index block and moving toward the first, and
** within each index block, starting at the end and moving toward the
** beginning.  The first entry that equals P corresponds to the frame
** holding the content for that page.
**
** The hash table consists of HASHTABLE_NSLOT 16-bit unsigned integers.
** HASHTABLE_NSLOT = 2*HASHTABLE_NPAGE, and there is one entry in the
** hash table for each page number in the mapping section, so the hash 
** table is never more than half full.  The expected number of collisions 
** prior to finding a match is 1.  Each entry of the hash table is an
** 1-based index of an entry in the mapping section of the same
** index block.   Let K be the 1-based index of the largest entry in
** the mapping section.  (For index blocks other than the last, K will
** always be exactly HASHTABLE_NPAGE (4096) and for the last index block
** K will be (mxFrame%HASHTABLE_NPAGE).)  Unused slots of the hash table
** contain a value of 0.
**
** To look for page P in the hash table, first compute a hash iKey on
** P as follows:
**
**      iKey = (P * 383) % HASHTABLE_NSLOT
**
** Then start scanning entries of the hash table, starting with iKey
** (wrapping around to the beginning when the end of the hash table is
** reached) until an unused hash slot is found. Let the first unused slot
** be at index iUnused.  (iUnused might be less than iKey if there was
** wrap-around.) Because the hash table is never more than half full,
** the search is guaranteed to eventually hit an unused entry.  Let 
** iMax be the value between iKey and iUnused, closest to iUnused,
** where aHash[iMax]==P.  If there is no iMax entry (if there exists
** no hash slot such that aHash[i]==p) then page P is not in the
** current index block.  Otherwise the iMax-th mapping entry of the
** current index block corresponds to the last entry that references 
** page P.
**
** A hash search begins with the last index block and moves toward the
** first index block, looking for entries corresponding to page P.  On
** average, only two or three slots in each index block need to be
** examined in order to either find the last entry for page P, or to
** establish that no such entry exists in the block.  Each index block
** holds over 4000 entries.  So two or three index blocks are sufficient
** to cover a typical 10 megabyte WAL file, assuming 1K pages.  8 or 10
** comparisons (on average) suffice to either locate a frame in the
** WAL or to establish that the frame does not exist in the WAL.  This
** is much faster than scanning the entire 10MB WAL.
**
** Note that entries are added in order of increasing K.  Hence, one
** reader might be using some value K0 and a second reader that started
** at a later time (after additional transactions were added to the WAL
** and to the wal-index) might be using a different value K1, where K1>K0.
** Both readers can use the same hash table and mapping section to get
** the correct result.  There may be entries in the hash table with
** K>K0 but to the first reader, those entries will appear to be unused
** slots in the hash table and so the first reader will get an answer as
** if no values greater than K0 had ever been inserted into the hash table
** in the first place - which is what reader one wants.  Meanwhile, the
** second reader using K1 will see additional values that were inserted
** later, which is exactly what reader two wants.  
**
** When a rollback occurs, the value of K is decreased. Hash table entries
** that correspond to frames greater than the new K value are removed
** from the hash table at this point.
*/
#ifndef SQLITE_OMIT_WAL

#include "wal.h"

/*
** Trace output macros
*/
#if defined(SQLITE_TEST) && defined(SQLITE_DEBUG)
int sqlite3WalTrace = 0;
# define WALTRACE(X)  if(sqlite3WalTrace) sqlite3DebugPrintf X
#else
# define WALTRACE(X)
#endif


/*
** Indices of various locking bytes.   WAL_NREADER is the number
** of available reader locks and should be at least 3.
*/
#define WAL_WRITE_LOCK         0
#define WAL_ALL_BUT_WRITE      1
#define WAL_CKPT_LOCK          1
#define WAL_RECOVER_LOCK       2
#define WAL_READ_LOCK(I)       (3+(I))
#define WAL_NREADER            (SQLITE_SHM_NLOCK-3)


/* Object declarations */
typedef struct WalIndexHdr WalIndexHdr;
typedef struct WalIterator WalIterator;
typedef struct WalCkptInfo WalCkptInfo;


/*
** The following object holds a copy of the wal-index header content.
**
** The actual header in the wal-index consists of two copies of this
** object.
*/
struct WalIndexHdr {
  u32 iChange;                    /* Counter incremented each transaction */
  u8 isInit;                      /* 1 when initialized */
  u8 bigEndCksum;                 /* True if checksums in WAL are big-endian */
  u16 szPage;                     /* Database page size in bytes */
  u32 mxFrame;                    /* Index of last valid frame in the WAL */
  u32 nPage;                      /* Size of database in pages */
  u32 aFrameCksum[2];             /* Checksum of last frame in log */
  u32 aSalt[2];                   /* Two salt values copied from WAL header */
  u32 aCksum[2];                  /* Checksum over all prior fields */
};

/*
** A copy of the following object occurs in the wal-index immediately
** following the second copy of the WalIndexHdr.  This object stores
** information used by checkpoint.
**
** nBackfill is the number of frames in the WAL that have been written
** back into the database. (We call the act of moving content from WAL to
** database "backfilling".)  The nBackfill number is never greater than
** WalIndexHdr.mxFrame.  nBackfill can only be increased by threads
** holding the WAL_CKPT_LOCK lock (which includes a recovery thread).
** However, a WAL_WRITE_LOCK thread can move the value of nBackfill from
** mxFrame back to zero when the WAL is reset.
**
** There is one entry in aReadMark[] for each reader lock.  If a reader
** holds read-lock K, then the value in aReadMark[K] is no greater than
** the mxFrame for that reader.  aReadMark[0] is a special case.  It
** always holds zero.  Readers holding WAL_READ_LOCK(0) always ignore 
** the entire WAL and read all content directly from the database.
**
** The value of aReadMark[K] may only be changed by a thread that
** is holding an exclusive lock on WAL_READ_LOCK(K).  Thus, the value of
** aReadMark[K] cannot changed while there is a reader is using that mark
** since the reader will be holding a shared lock on WAL_READ_LOCK(K).
**
** The checkpointer may only transfer frames from WAL to database where
** the frame numbers are less than or equal to every aReadMark[] that is
** in use (that is, every aReadMark[j] for which there is a corresponding
** WAL_READ_LOCK(j)).  New readers (usually) pick the aReadMark[] with the
** largest value and will increase an unused aReadMark[] to mxFrame if there
** is not already an aReadMark[] equal to mxFrame.  The exception to the
** previous sentence is when nBackfill equals mxFrame (meaning that everything
** in the WAL has been backfilled into the database) then new readers
** will choose aReadMark[0] which has value 0 and hence such reader will
** get all their all content directly from the database file and ignore 
** the WAL.
**
** Writers normally append new frames to the end of the WAL.  However,
** if nBackfill equals mxFrame (meaning that all WAL content has been
** written back into the database) and if no readers are using the WAL
** (in other words, if there are no WAL_READ_LOCK(i) where i>0) then
** the writer will first "reset" the WAL back to the beginning and start
** writing new content beginning at frame 1.
**
** We assume that 32-bit loads are atomic and so no locks are needed in
** order to read from any aReadMark[] entries.
*/
struct WalCkptInfo {
  u32 nBackfill;                  /* Number of WAL frames backfilled into DB */
  u32 aReadMark[WAL_NREADER];     /* Reader marks */
};


/* A block of WALINDEX_LOCK_RESERVED bytes beginning at
** WALINDEX_LOCK_OFFSET is reserved for locks. Since some systems
** only support mandatory file-locks, we do not read or write data
** from the region of the file on which locks are applied.
*/
#define WALINDEX_LOCK_OFFSET   (sizeof(WalIndexHdr)*2 + sizeof(WalCkptInfo))
#define WALINDEX_LOCK_RESERVED 16
#define WALINDEX_HDR_SIZE      (WALINDEX_LOCK_OFFSET+WALINDEX_LOCK_RESERVED)

/* Size of header before each frame in wal */
#define WAL_FRAME_HDRSIZE 24

/* Size of write ahead log header */
#define WAL_HDRSIZE 24

/* WAL magic value. Either this value, or the same value with the least
** significant bit also set (WAL_MAGIC | 0x00000001) is stored in 32-bit
** big-endian format in the first 4 bytes of a WAL file.
**
** If the LSB is set, then the checksums for each frame within the WAL
** file are calculated by treating all data as an array of 32-bit 
** big-endian words. Otherwise, they are calculated by interpreting 
** all data as 32-bit little-endian words.
*/
#define WAL_MAGIC 0x377f0682

/*
** Return the offset of frame iFrame in the write-ahead log file, 
** assuming a database page size of szPage bytes. The offset returned
** is to the start of the write-ahead log frame-header.
*/
#define walFrameOffset(iFrame, szPage) (                               \
  WAL_HDRSIZE + ((iFrame)-1)*((szPage)+WAL_FRAME_HDRSIZE)        \
)

/*
** An open write-ahead log file is represented by an instance of the
** following object.
*/
struct Wal {
  sqlite3_vfs *pVfs;         /* The VFS used to create pDbFd */
  sqlite3_file *pDbFd;       /* File handle for the database file */
  sqlite3_file *pWalFd;      /* File handle for WAL file */
  u32 iCallback;             /* Value to pass to log callback (or 0) */
  int szWIndex;              /* Size of the wal-index that is mapped in mem */
  volatile u32 *pWiData;     /* Pointer to wal-index content in memory */
  u16 szPage;                /* Database page size */
  i16 readLock;              /* Which read lock is being held.  -1 for none */
  u8 exclusiveMode;          /* Non-zero if connection is in exclusive mode */
  u8 isWIndexOpen;           /* True if ShmOpen() called on pDbFd */
  u8 writeLock;              /* True if in a write transaction */
  u8 ckptLock;               /* True if holding a checkpoint lock */
  WalIndexHdr hdr;           /* Wal-index header for current transaction */
  char *zWalName;            /* Name of WAL file */
  u32 nCkpt;                 /* Checkpoint sequence counter in the wal-header */
};

/*
** Return a pointer to the WalCkptInfo structure in the wal-index.
*/
static volatile WalCkptInfo *walCkptInfo(Wal *pWal){
  assert( pWal->pWiData!=0 );
  return (volatile WalCkptInfo*)&pWal->pWiData[sizeof(WalIndexHdr)/2];
}


/*
** This structure is used to implement an iterator that loops through
** all frames in the WAL in database page order. Where two or more frames
** correspond to the same database page, the iterator visits only the 
** frame most recently written to the WAL (in other words, the frame with
** the largest index).
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
  int iPrior;           /* Last result returned from the iterator */
  int nSegment;         /* Size of the aSegment[] array */
  int nFinal;           /* Elements in aSegment[nSegment-1]  */
  struct WalSegment {
    int iNext;              /* Next slot in aIndex[] not previously returned */
    u8 *aIndex;             /* i0, i1, i2... such that aPgno[iN] ascending */
    u32 *aPgno;             /* 256 page numbers.  Pointer to Wal.pWiData */
  } aSegment[1];        /* One for every 256 entries in the WAL */
};

/*
** The argument to this macro must be of type u32. On a little-endian
** architecture, it returns the u32 value that results from interpreting
** the 4 bytes as a big-endian value. On a big-endian architecture, it
** returns the value that would be produced by intepreting the 4 bytes
** of the input value as a little-endian integer.
*/
#define BYTESWAP32(x) ( \
    (((x)&0x000000FF)<<24) + (((x)&0x0000FF00)<<8)  \
  + (((x)&0x00FF0000)>>8)  + (((x)&0xFF000000)>>24) \
)

/*
** Generate or extend an 8 byte checksum based on the data in 
** array aByte[] and the initial values of aIn[0] and aIn[1] (or
** initial values of 0 and 0 if aIn==NULL).
**
** The checksum is written back into aOut[] before returning.
**
** nByte must be a positive multiple of 8.
*/
static void walChecksumBytes(
  int nativeCksum, /* True for native byte-order, false for non-native */
  u8 *a,           /* Content to be checksummed */
  int nByte,       /* Bytes of content in a[].  Must be a multiple of 8. */
  const u32 *aIn,  /* Initial checksum value input */
  u32 *aOut        /* OUT: Final checksum value output */
){
  u32 s1, s2;
  u32 *aData = (u32 *)a;
  u32 *aEnd = (u32 *)&a[nByte];

  if( aIn ){
    s1 = aIn[0];
    s2 = aIn[1];
  }else{
    s1 = s2 = 0;
  }

  assert( nByte>=8 );
  assert( (nByte&0x00000007)==0 );

  if( nativeCksum ){
    do {
      s1 += *aData++ + s2;
      s2 += *aData++ + s1;
    }while( aData<aEnd );
  }else{
    do {
      s1 += BYTESWAP32(aData[0]) + s2;
      s2 += BYTESWAP32(aData[1]) + s1;
      aData += 2;
    }while( aData<aEnd );
  }

  aOut[0] = s1;
  aOut[1] = s2;
}

/*
** Write the header information in pWal->hdr into the wal-index.
**
** The checksum on pWal->hdr is updated before it is written.
*/
static void walIndexWriteHdr(Wal *pWal){
  WalIndexHdr *aHdr;

  assert( pWal->writeLock );
  pWal->hdr.isInit = 1;
  walChecksumBytes(1, (u8*)&pWal->hdr, offsetof(WalIndexHdr, aCksum),
                   0, pWal->hdr.aCksum);
  aHdr = (WalIndexHdr*)pWal->pWiData;
  memcpy(&aHdr[1], &pWal->hdr, sizeof(WalIndexHdr));
  sqlite3OsShmBarrier(pWal->pDbFd);
  memcpy(&aHdr[0], &pWal->hdr, sizeof(WalIndexHdr));
}

/*
** This function encodes a single frame header and writes it to a buffer
** supplied by the caller. A frame-header is made up of a series of 
** 4-byte big-endian integers, as follows:
**
**     0: Page number.
**     4: For commit records, the size of the database image in pages 
**        after the commit. For all other records, zero.
**     8: Salt-1 (copied from the wal-header)
**    12: Salt-2 (copied from the wal-header)
**    16: Checksum-1.
**    20: Checksum-2.
*/
static void walEncodeFrame(
  Wal *pWal,                      /* The write-ahead log */
  u32 iPage,                      /* Database page number for frame */
  u32 nTruncate,                  /* New db size (or 0 for non-commit frames) */
  u8 *aData,                      /* Pointer to page data */
  u8 *aFrame                      /* OUT: Write encoded frame here */
){
  int nativeCksum;                /* True for native byte-order checksums */
  u32 *aCksum = pWal->hdr.aFrameCksum;
  assert( WAL_FRAME_HDRSIZE==24 );
  sqlite3Put4byte(&aFrame[0], iPage);
  sqlite3Put4byte(&aFrame[4], nTruncate);
  memcpy(&aFrame[8], pWal->hdr.aSalt, 8);

  nativeCksum = (pWal->hdr.bigEndCksum==SQLITE_BIGENDIAN);
  walChecksumBytes(nativeCksum, aFrame, 8, aCksum, aCksum);
  walChecksumBytes(nativeCksum, aData, pWal->szPage, aCksum, aCksum);

  sqlite3Put4byte(&aFrame[16], aCksum[0]);
  sqlite3Put4byte(&aFrame[20], aCksum[1]);
}

/*
** Check to see if the frame with header in aFrame[] and content
** in aData[] is valid.  If it is a valid frame, fill *piPage and
** *pnTruncate and return true.  Return if the frame is not valid.
*/
static int walDecodeFrame(
  Wal *pWal,                      /* The write-ahead log */
  u32 *piPage,                    /* OUT: Database page number for frame */
  u32 *pnTruncate,                /* OUT: New db size (or 0 if not commit) */
  u8 *aData,                      /* Pointer to page data (for checksum) */
  u8 *aFrame                      /* Frame data */
){
  int nativeCksum;                /* True for native byte-order checksums */
  u32 *aCksum = pWal->hdr.aFrameCksum;
  u32 pgno;                       /* Page number of the frame */
  assert( WAL_FRAME_HDRSIZE==24 );

  /* A frame is only valid if the salt values in the frame-header
  ** match the salt values in the wal-header. 
  */
  if( memcmp(&pWal->hdr.aSalt, &aFrame[8], 8)!=0 ){
    return 0;
  }

  /* A frame is only valid if the page number is creater than zero.
  */
  pgno = sqlite3Get4byte(&aFrame[0]);
  if( pgno==0 ){
    return 0;
  }

  /* A frame is only valid if a checksum of the first 16 bytes
  ** of the frame-header, and the frame-data matches
  ** the checksum in the last 8 bytes of the frame-header.
  */
  nativeCksum = (pWal->hdr.bigEndCksum==SQLITE_BIGENDIAN);
  walChecksumBytes(nativeCksum, aFrame, 8, aCksum, aCksum);
  walChecksumBytes(nativeCksum, aData, pWal->szPage, aCksum, aCksum);
  if( aCksum[0]!=sqlite3Get4byte(&aFrame[16]) 
   || aCksum[1]!=sqlite3Get4byte(&aFrame[20]) 
  ){
    /* Checksum failed. */
    return 0;
  }

  /* If we reach this point, the frame is valid.  Return the page number
  ** and the new database size.
  */
  *piPage = pgno;
  *pnTruncate = sqlite3Get4byte(&aFrame[4]);
  return 1;
}

/*
** Define the parameters of the hash tables in the wal-index file. There
** is a hash-table following every HASHTABLE_NPAGE page numbers in the
** wal-index.
**
** Changing any of these constants will alter the wal-index format and
** create incompatibilities.
*/
#define HASHTABLE_NPAGE      4096  /* Must be power of 2 and multiple of 256 */
#define HASHTABLE_DATATYPE   u16
#define HASHTABLE_HASH_1     383                  /* Should be prime */
#define HASHTABLE_NSLOT      (HASHTABLE_NPAGE*2)  /* Must be a power of 2 */
#define HASHTABLE_NBYTE      (sizeof(HASHTABLE_DATATYPE)*HASHTABLE_NSLOT)

#if defined(SQLITE_TEST) && defined(SQLITE_DEBUG)
/*
** Names of locks.  This routine is used to provide debugging output and is not
** a part of an ordinary build.
*/
static const char *walLockName(int lockIdx){
  if( lockIdx==WAL_WRITE_LOCK ){
    return "WRITE-LOCK";
  }else if( lockIdx==WAL_CKPT_LOCK ){
    return "CKPT-LOCK";
  }else if( lockIdx==WAL_RECOVER_LOCK ){
    return "RECOVER-LOCK";
  }else{
    static char zName[15];
    sqlite3_snprintf(sizeof(zName), zName, "READ-LOCK[%d]",
                     lockIdx-WAL_READ_LOCK(0));
    return zName;
  }
}
#endif /*defined(SQLITE_TEST) || defined(SQLITE_DEBUG) */
    

/*
** Set or release locks on the WAL.  Locks are either shared or exclusive.
** A lock cannot be moved directly between shared and exclusive - it must go
** through the unlocked state first.
**
** In locking_mode=EXCLUSIVE, all of these routines become no-ops.
*/
static int walLockShared(Wal *pWal, int lockIdx){
  int rc;
  if( pWal->exclusiveMode ) return SQLITE_OK;
  rc = sqlite3OsShmLock(pWal->pDbFd, lockIdx, 1,
                        SQLITE_SHM_LOCK | SQLITE_SHM_SHARED);
  WALTRACE(("WAL%p: acquire SHARED-%s %s\n", pWal,
            walLockName(lockIdx), rc ? "failed" : "ok"));
  return rc;
}
static void walUnlockShared(Wal *pWal, int lockIdx){
  if( pWal->exclusiveMode ) return;
  (void)sqlite3OsShmLock(pWal->pDbFd, lockIdx, 1,
                         SQLITE_SHM_UNLOCK | SQLITE_SHM_SHARED);
  WALTRACE(("WAL%p: release SHARED-%s\n", pWal, walLockName(lockIdx)));
}
static int walLockExclusive(Wal *pWal, int lockIdx, int n){
  int rc;
  if( pWal->exclusiveMode ) return SQLITE_OK;
  rc = sqlite3OsShmLock(pWal->pDbFd, lockIdx, n,
                        SQLITE_SHM_LOCK | SQLITE_SHM_EXCLUSIVE);
  WALTRACE(("WAL%p: acquire EXCLUSIVE-%s cnt=%d %s\n", pWal,
            walLockName(lockIdx), n, rc ? "failed" : "ok"));
  return rc;
}
static void walUnlockExclusive(Wal *pWal, int lockIdx, int n){
  if( pWal->exclusiveMode ) return;
  (void)sqlite3OsShmLock(pWal->pDbFd, lockIdx, n,
                         SQLITE_SHM_UNLOCK | SQLITE_SHM_EXCLUSIVE);
  WALTRACE(("WAL%p: release EXCLUSIVE-%s cnt=%d\n", pWal,
             walLockName(lockIdx), n));
}

/*
** Return the index in the Wal.pWiData array that corresponds to 
** frame iFrame.
**
** Wal.pWiData is an array of u32 elements that is the wal-index.
** The array begins with a header and is then followed by alternating
** "map" and "hash-table" blocks.  Each "map" block consists of
** HASHTABLE_NPAGE u32 elements which are page numbers corresponding
** to frames in the WAL file.  
**
** This routine returns an index X such that Wal.pWiData[X] is part
** of a "map" block that contains the page number of the iFrame-th
** frame in the WAL file.
*/
static int walIndexEntry(u32 iFrame){
  return (
      (WALINDEX_LOCK_OFFSET+WALINDEX_LOCK_RESERVED)/sizeof(u32)
    + (((iFrame-1)/HASHTABLE_NPAGE) * HASHTABLE_NBYTE)/sizeof(u32)
    + (iFrame-1)
  );
}

/*
** Return the minimum size of the shared-memory, in bytes, that is needed
** to support a wal-index containing frame iFrame.  The value returned
** includes the wal-index header and the complete "block" containing iFrame,
** including the hash table segment that follows the block.
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
  }
  pWal->pWiData = 0;
  pWal->szWIndex = -1;
}

/*
** Map the wal-index file into memory if it isn't already. 
**
** The reqSize parameter is the requested size of the mapping.  The
** mapping will be at least this big if the underlying storage is
** that big.  But the mapping will never grow larger than the underlying
** storage.  Use the walIndexRemap() to enlarget the storage space.
*/
static int walIndexMap(Wal *pWal, int reqSize){
  int rc = SQLITE_OK;
  if( pWal->pWiData==0 || reqSize>pWal->szWIndex ){
    walIndexUnmap(pWal);
    rc = sqlite3OsShmGet(pWal->pDbFd, reqSize, &pWal->szWIndex,
                             (void volatile**)(char volatile*)&pWal->pWiData);
    if( rc!=SQLITE_OK ){
      walIndexUnmap(pWal);
    }
  }
  return rc;
}

/*
** Enlarge the wal-index to be at least enlargeTo bytes in size and
** Remap the wal-index so that the mapping covers the full size
** of the underlying file.
**
** If enlargeTo is non-negative, then increase the size of the underlying
** storage to be at least as big as enlargeTo before remapping.
*/
static int walIndexRemap(Wal *pWal, int enlargeTo){
  int rc;
  int sz;
  assert( pWal->writeLock );
  rc = sqlite3OsShmSize(pWal->pDbFd, enlargeTo, &sz);
  if( rc==SQLITE_OK && sz>pWal->szWIndex ){
    walIndexUnmap(pWal);
    rc = walIndexMap(pWal, sz);
  }
  assert( pWal->szWIndex>=enlargeTo || rc!=SQLITE_OK );
  return rc;
}

/*
** Compute a hash on a page number.  The resulting hash value must land
** between 0 and (HASHTABLE_NSLOT-1).  The walHashNext() function advances
** the hash to the next value in the event of a collision.
*/
static int walHash(u32 iPage){
  assert( iPage>0 );
  assert( (HASHTABLE_NSLOT & (HASHTABLE_NSLOT-1))==0 );
  return (iPage*HASHTABLE_HASH_1) & (HASHTABLE_NSLOT-1);
}
static int walNextHash(int iPriorHash){
  return (iPriorHash+1)&(HASHTABLE_NSLOT-1);
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
  volatile HASHTABLE_DATATYPE **paHash,    /* OUT: Pointer to hash index */
  volatile u32 **paPgno,          /* OUT: Pointer to page number array */
  u32 *piZero                     /* OUT: Frame associated with *paPgno[0] */
){
  u32 iZero;
  volatile u32 *aPgno;
  volatile HASHTABLE_DATATYPE *aHash;

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
** Remove entries from the hash table that point to WAL slots greater
** than pWal->hdr.mxFrame.
**
** This function is called whenever pWal->hdr.mxFrame is decreased due
** to a rollback or savepoint.
**
** At most only the hash table containing pWal->hdr.mxFrame needs to be
** updated.  Any later hash tables will be automatically cleared when
** pWal->hdr.mxFrame advances to the point where those hash tables are
** actually needed.
*/
static void walCleanupHash(Wal *pWal){
  volatile HASHTABLE_DATATYPE *aHash;  /* Pointer to hash table to clear */
  volatile u32 *aPgno;                 /* Unused return from walHashFind() */
  u32 iZero;                           /* frame == (aHash[x]+iZero) */
  int iLimit;                          /* Zero values greater than this */

  assert( pWal->writeLock );
  walHashFind(pWal, pWal->hdr.mxFrame+1, &aHash, &aPgno, &iZero);
  iLimit = pWal->hdr.mxFrame - iZero;
  if( iLimit>0 ){
    int nByte;                    /* Number of bytes to zero in aPgno[] */
    int i;                        /* Used to iterate through aHash[] */
    for(i=0; i<HASHTABLE_NSLOT; i++){
      if( aHash[i]>iLimit ){
        aHash[i] = 0;
      }
    }

    /* Zero the entries in the aPgno array that correspond to frames with
    ** frame numbers greater than pWal->hdr.mxFrame. 
    */
    nByte = sizeof(u32) * (HASHTABLE_NPAGE-iLimit);
    memset((void *)&aPgno[iZero+iLimit+1], 0, nByte);
    assert( &((u8 *)&aPgno[iZero+iLimit+1])[nByte]==(u8 *)aHash );
  }

#ifdef SQLITE_ENABLE_EXPENSIVE_ASSERT
  /* Verify that the every entry in the mapping region is still reachable
  ** via the hash table even after the cleanup.
  */
  {
    int i;           /* Loop counter */
    int iKey;        /* Hash key */
    for(i=1; i<=iLimit; i++){
      for(iKey=walHash(aPgno[i+iZero]); aHash[iKey]; iKey=walNextHash(iKey)){
        if( aHash[iKey]==i ) break;
      }
      assert( aHash[iKey]==i );
    }
  }
#endif /* SQLITE_ENABLE_EXPENSIVE_ASSERT */
}


/*
** Set an entry in the wal-index that will map database page number
** pPage into WAL frame iFrame.
*/
static int walIndexAppend(Wal *pWal, u32 iFrame, u32 iPage){
  int rc;                         /* Return code */
  int nMapping;                   /* Required mapping size in bytes */
  
  /* Make sure the wal-index is mapped. Enlarge the mapping if required. */
  nMapping = walMappingSize(iFrame);
  rc = walIndexMap(pWal, nMapping);
  while( rc==SQLITE_OK && nMapping>pWal->szWIndex ){
    rc = walIndexRemap(pWal, nMapping);
  }

  /* Assuming the wal-index file was successfully mapped, find the hash 
  ** table and section of of the page number array that pertain to frame 
  ** iFrame of the WAL. Then populate the page number array and the hash 
  ** table entry.
  */
  if( rc==SQLITE_OK ){
    int iKey;                     /* Hash table key */
    u32 iZero;                    /* One less than frame number of aPgno[1] */
    volatile u32 *aPgno;                 /* Page number array */
    volatile HASHTABLE_DATATYPE *aHash;  /* Hash table */
    int idx;                             /* Value to write to hash-table slot */
    TESTONLY( int nCollide = 0;          /* Number of hash collisions */ )

    walHashFind(pWal, iFrame, &aHash, &aPgno, &iZero);
    idx = iFrame - iZero;
    if( idx==1 ){
      memset((void*)&aPgno[iZero+1], 0, HASHTABLE_NPAGE*sizeof(u32));
      memset((void*)aHash, 0, HASHTABLE_NBYTE);
    }
    assert( idx <= HASHTABLE_NSLOT/2 + 1 );

    if( aPgno[iFrame] ){
      /* If the entry in aPgno[] is already set, then the previous writer
      ** must have exited unexpectedly in the middle of a transaction (after
      ** writing one or more dirty pages to the WAL to free up memory). 
      ** Remove the remnants of that writers uncommitted transaction from 
      ** the hash-table before writing any new entries.
      */
      walCleanupHash(pWal);
      assert( !aPgno[iFrame] );
    }
    aPgno[iFrame] = iPage;
    for(iKey=walHash(iPage); aHash[iKey]; iKey=walNextHash(iKey)){
      assert( nCollide++ < idx );
    }
    aHash[iKey] = idx;

#ifdef SQLITE_ENABLE_EXPENSIVE_ASSERT
    /* Verify that the number of entries in the hash table exactly equals
    ** the number of entries in the mapping region.
    */
    {
      int i;           /* Loop counter */
      int nEntry = 0;  /* Number of entries in the hash table */
      for(i=0; i<HASHTABLE_NSLOT; i++){ if( aHash[i] ) nEntry++; }
      assert( nEntry==idx );
    }

    /* Verify that the every entry in the mapping region is reachable
    ** via the hash table.  This turns out to be a really, really expensive
    ** thing to check, so only do this occasionally - not on every
    ** iteration.
    */
    if( (idx&0x3ff)==0 ){
      int i;           /* Loop counter */
      for(i=1; i<=idx; i++){
        for(iKey=walHash(aPgno[i+iZero]); aHash[iKey]; iKey=walNextHash(iKey)){
          if( aHash[iKey]==i ) break;
        }
        assert( aHash[iKey]==i );
      }
    }
#endif /* SQLITE_ENABLE_EXPENSIVE_ASSERT */
  }


  return rc;
}


/*
** Recover the wal-index by reading the write-ahead log file. 
**
** This routine first tries to establish an exclusive lock on the
** wal-index to prevent other threads/processes from doing anything
** with the WAL or wal-index while recovery is running.  The
** WAL_RECOVER_LOCK is also held so that other threads will know
** that this thread is running recovery.  If unable to establish
** the necessary locks, this routine returns SQLITE_BUSY.
*/
static int walIndexRecover(Wal *pWal){
  int rc;                         /* Return Code */
  i64 nSize;                      /* Size of log file */
  u32 aFrameCksum[2] = {0, 0};
  int iLock;                      /* Lock offset to lock for checkpoint */
  int nLock;                      /* Number of locks to hold */

  /* Obtain an exclusive lock on all byte in the locking range not already
  ** locked by the caller. The caller is guaranteed to have locked the
  ** WAL_WRITE_LOCK byte, and may have also locked the WAL_CKPT_LOCK byte.
  ** If successful, the same bytes that are locked here are unlocked before
  ** this function returns.
  */
  assert( pWal->ckptLock==1 || pWal->ckptLock==0 );
  assert( WAL_ALL_BUT_WRITE==WAL_WRITE_LOCK+1 );
  assert( WAL_CKPT_LOCK==WAL_ALL_BUT_WRITE );
  assert( pWal->writeLock );
  iLock = WAL_ALL_BUT_WRITE + pWal->ckptLock;
  nLock = SQLITE_SHM_NLOCK - iLock;
  rc = walLockExclusive(pWal, iLock, nLock);
  if( rc ){
    return rc;
  }
  WALTRACE(("WAL%p: recovery begin...\n", pWal));

  memset(&pWal->hdr, 0, sizeof(WalIndexHdr));

  rc = sqlite3OsFileSize(pWal->pWalFd, &nSize);
  if( rc!=SQLITE_OK ){
    goto recovery_error;
  }

  if( nSize>WAL_HDRSIZE ){
    u8 aBuf[WAL_HDRSIZE];         /* Buffer to load WAL header into */
    u8 *aFrame = 0;               /* Malloc'd buffer to load entire frame */
    int szFrame;                  /* Number of bytes in buffer aFrame[] */
    u8 *aData;                    /* Pointer to data part of aFrame buffer */
    int iFrame;                   /* Index of last frame read */
    i64 iOffset;                  /* Next offset to read from log file */
    int szPage;                   /* Page size according to the log */
    u32 magic;                    /* Magic value read from WAL header */

    /* Read in the WAL header. */
    rc = sqlite3OsRead(pWal->pWalFd, aBuf, WAL_HDRSIZE, 0);
    if( rc!=SQLITE_OK ){
      goto recovery_error;
    }

    /* If the database page size is not a power of two, or is greater than
    ** SQLITE_MAX_PAGE_SIZE, conclude that the WAL file contains no valid 
    ** data. Similarly, if the 'magic' value is invalid, ignore the whole
    ** WAL file.
    */
    magic = sqlite3Get4byte(&aBuf[0]);
    szPage = sqlite3Get4byte(&aBuf[8]);
    if( (magic&0xFFFFFFFE)!=WAL_MAGIC 
     || szPage&(szPage-1) 
     || szPage>SQLITE_MAX_PAGE_SIZE 
     || szPage<512 
    ){
      goto finished;
    }
    pWal->hdr.bigEndCksum = (magic&0x00000001);
    pWal->szPage = szPage;
    pWal->nCkpt = sqlite3Get4byte(&aBuf[12]);
    memcpy(&pWal->hdr.aSalt, &aBuf[16], 8);
    walChecksumBytes(pWal->hdr.bigEndCksum==SQLITE_BIGENDIAN, 
        aBuf, WAL_HDRSIZE, 0, pWal->hdr.aFrameCksum
    );

    /* Malloc a buffer to read frames into. */
    szFrame = szPage + WAL_FRAME_HDRSIZE;
    aFrame = (u8 *)sqlite3_malloc(szFrame);
    if( !aFrame ){
      rc = SQLITE_NOMEM;
      goto recovery_error;
    }
    aData = &aFrame[WAL_FRAME_HDRSIZE];

    /* Read all frames from the log file. */
    iFrame = 0;
    for(iOffset=WAL_HDRSIZE; (iOffset+szFrame)<=nSize; iOffset+=szFrame){
      u32 pgno;                   /* Database page number for frame */
      u32 nTruncate;              /* dbsize field from frame header */
      int isValid;                /* True if this frame is valid */

      /* Read and decode the next log frame. */
      rc = sqlite3OsRead(pWal->pWalFd, aFrame, szFrame, iOffset);
      if( rc!=SQLITE_OK ) break;
      isValid = walDecodeFrame(pWal, &pgno, &nTruncate, aData, aFrame);
      if( !isValid ) break;
      rc = walIndexAppend(pWal, ++iFrame, pgno);
      if( rc!=SQLITE_OK ) break;

      /* If nTruncate is non-zero, this is a commit record. */
      if( nTruncate ){
        pWal->hdr.mxFrame = iFrame;
        pWal->hdr.nPage = nTruncate;
        pWal->hdr.szPage = szPage;
        aFrameCksum[0] = pWal->hdr.aFrameCksum[0];
        aFrameCksum[1] = pWal->hdr.aFrameCksum[1];
      }
    }

    sqlite3_free(aFrame);
  }

finished:
  if( rc==SQLITE_OK && pWal->hdr.mxFrame==0 ){
    rc = walIndexRemap(pWal, walMappingSize(1));
  }
  if( rc==SQLITE_OK ){
    pWal->hdr.aFrameCksum[0] = aFrameCksum[0];
    pWal->hdr.aFrameCksum[1] = aFrameCksum[1];
    walIndexWriteHdr(pWal);

    /* Zero the checkpoint-header. This is safe because this thread is 
    ** currently holding locks that exclude all other readers, writers and
    ** checkpointers.
    */
    memset((void *)walCkptInfo(pWal), 0, sizeof(WalCkptInfo));
  }

recovery_error:
  WALTRACE(("WAL%p: recovery %s\n", pWal, rc ? "failed" : "ok"));
  walUnlockExclusive(pWal, iLock, nLock);
  return rc;
}

/*
** Close an open wal-index.
*/
static void walIndexClose(Wal *pWal, int isDelete){
  if( pWal->isWIndexOpen ){
    sqlite3OsShmClose(pWal->pDbFd, isDelete);
    pWal->isWIndexOpen = 0;
  }
}

/* 
** Open a connection to the WAL file associated with database zDbName.
** The database file must already be opened on connection pDbFd.
**
** A SHARED lock should be held on the database file when this function
** is called. The purpose of this SHARED lock is to prevent any other
** client from unlinking the WAL or wal-index file. If another process
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

  /* In the amalgamation, the os_unix.c and os_win.c source files come before
  ** this source file.  Verify that the #defines of the locking byte offsets
  ** in os_unix.c and os_win.c agree with the WALINDEX_LOCK_OFFSET value.
  */
#ifdef WIN_SHM_BASE
  assert( WIN_SHM_BASE==WALINDEX_LOCK_OFFSET );
#endif
#ifdef UNIX_SHM_BASE
  assert( UNIX_SHM_BASE==WALINDEX_LOCK_OFFSET );
#endif


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
  pRet->szWIndex = -1;
  pRet->readLock = -1;
  sqlite3_randomness(8, &pRet->hdr.aSalt);
  pRet->zWalName = zWal = pVfs->szOsFile + (char*)pRet->pWalFd;
  sqlite3_snprintf(nWal, zWal, "%s-wal", zDbName);
  rc = sqlite3OsShmOpen(pDbFd);

  /* Open file handle on the write-ahead log file. */
  if( rc==SQLITE_OK ){
    pRet->isWIndexOpen = 1;
    flags = (SQLITE_OPEN_READWRITE|SQLITE_OPEN_CREATE|SQLITE_OPEN_MAIN_JOURNAL);
    rc = sqlite3OsOpen(pVfs, zWal, pRet->pWalFd, flags, &flags);
  }

  if( rc!=SQLITE_OK ){
    walIndexClose(pRet, 0);
    sqlite3OsClose(pRet->pWalFd);
    sqlite3_free(pRet);
  }else{
    *ppWal = pRet;
    WALTRACE(("WAL%d: opened\n", pRet));
  }
  return rc;
}

/*
** Find the smallest page number out of all pages held in the WAL that
** has not been returned by any prior invocation of this method on the
** same WalIterator object.   Write into *piFrame the frame index where
** that page was last written into the WAL.  Write into *piPage the page
** number.
**
** Return 0 on success.  If there are no pages in the WAL with a page
** number larger than *piPage, then return 1.
*/
static int walIteratorNext(
  WalIterator *p,               /* Iterator */
  u32 *piPage,                  /* OUT: The page number of the next page */
  u32 *piFrame                  /* OUT: Wal frame index of next page */
){
  u32 iMin;                     /* Result pgno must be greater than iMin */
  u32 iRet = 0xFFFFFFFF;        /* 0xffffffff is never a valid page number */
  int i;                        /* For looping through segments */
  int nBlock = p->nFinal;       /* Number of entries in current segment */

  iMin = p->iPrior;
  assert( iMin<0xffffffff );
  for(i=p->nSegment-1; i>=0; i--){
    struct WalSegment *pSegment = &p->aSegment[i];
    while( pSegment->iNext<nBlock ){
      u32 iPg = pSegment->aPgno[pSegment->aIndex[pSegment->iNext]];
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

  *piPage = p->iPrior = iRet;
  return (iRet==0xFFFFFFFF);
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
** Map the wal-index into memory owned by this thread, if it is not
** mapped already.  Then construct a WalInterator object that can be
** used to loop over all pages in the WAL in ascending order.  
**
** On success, make *pp point to the newly allocated WalInterator object
** return SQLITE_OK.  Otherwise, leave *pp unchanged and return an error
** code.
**
** The calling routine should invoke walIteratorFree() to destroy the
** WalIterator object when it has finished with it.  The caller must
** also unmap the wal-index.  But the wal-index must not be unmapped
** prior to the WalIterator object being destroyed.
*/
static int walIteratorInit(Wal *pWal, WalIterator **pp){
  u32 *aData;           /* Content of the wal-index file */
  WalIterator *p;       /* Return value */
  int nSegment;         /* Number of segments to merge */
  u32 iLast;            /* Last frame in log */
  int nByte;            /* Number of bytes to allocate */
  int i;                /* Iterator variable */
  int nFinal;           /* Number of unindexed entries */
  u8 *aTmp;             /* Temp space used by merge-sort */
  int rc;               /* Return code of walIndexMap() */
  u8 *aSpace;           /* Surplus space on the end of the allocation */

  /* Make sure the wal-index is mapped into local memory */
  rc = walIndexMap(pWal, walMappingSize(pWal->hdr.mxFrame));
  if( rc!=SQLITE_OK ){
    return rc;
  }

  /* This routine only runs while holding SQLITE_SHM_CHECKPOINT.  No other
  ** thread is able to write to shared memory while this routine is
  ** running (or, indeed, while the WalIterator object exists).  Hence,
  ** we can cast off the volatile qualifacation from shared memory
  */
  assert( pWal->ckptLock );
  aData = (u32*)pWal->pWiData;

  /* Allocate space for the WalIterator object */
  iLast = pWal->hdr.mxFrame;
  nSegment = (iLast >> 8) + 1;
  nFinal = (iLast & 0x000000FF);
  nByte = sizeof(WalIterator) + (nSegment+1)*(sizeof(struct WalSegment)+256);
  p = (WalIterator *)sqlite3_malloc(nByte);
  if( !p ){
    return SQLITE_NOMEM;
  }
  memset(p, 0, nByte);

  /* Initialize the WalIterator object.  Each 256-entry segment is
  ** presorted in order to make iterating through all entries much
  ** faster.
  */
  p->nSegment = nSegment;
  aSpace = (u8 *)&p->aSegment[nSegment];
  aTmp = &aSpace[nSegment*256];
  for(i=0; i<nSegment; i++){
    int j;
    int nIndex = (i==nSegment-1) ? nFinal : 256;
    p->aSegment[i].aPgno = &aData[walIndexEntry(i*256+1)];
    p->aSegment[i].aIndex = aSpace;
    for(j=0; j<nIndex; j++){
      aSpace[j] = j;
    }
    walMergesort8(p->aSegment[i].aPgno, aTmp, aSpace, &nIndex);
    memset(&aSpace[nIndex], aSpace[nIndex-1], 256-nIndex);
    aSpace += 256;
    p->nFinal = nIndex;
  }

  /* Return the fully initializd WalIterator object */
  *pp = p;
  return SQLITE_OK ;
}

/* 
** Free an iterator allocated by walIteratorInit().
*/
static void walIteratorFree(WalIterator *p){
  sqlite3_free(p);
}


/*
** Copy as much content as we can from the WAL back into the database file
** in response to an sqlite3_wal_checkpoint() request or the equivalent.
**
** The amount of information copies from WAL to database might be limited
** by active readers.  This routine will never overwrite a database page
** that a concurrent reader might be using.
**
** All I/O barrier operations (a.k.a fsyncs) occur in this routine when
** SQLite is in WAL-mode in synchronous=NORMAL.  That means that if 
** checkpoints are always run by a background thread or background 
** process, foreground threads will never block on a lengthy fsync call.
**
** Fsync is called on the WAL before writing content out of the WAL and
** into the database.  This ensures that if the new content is persistent
** in the WAL and can be recovered following a power-loss or hard reset.
**
** Fsync is also called on the database file if (and only if) the entire
** WAL content is copied into the database file.  This second fsync makes
** it safe to delete the WAL since the new content will persist in the
** database file.
**
** This routine uses and updates the nBackfill field of the wal-index header.
** This is the only routine tha will increase the value of nBackfill.  
** (A WAL reset or recovery will revert nBackfill to zero, but not increase
** its value.)
**
** The caller must be holding sufficient locks to ensure that no other
** checkpoint is running (in any other thread or process) at the same
** time.
*/
static int walCheckpoint(
  Wal *pWal,                      /* Wal connection */
  int sync_flags,                 /* Flags for OsSync() (or 0) */
  int nBuf,                       /* Size of zBuf in bytes */
  u8 *zBuf                        /* Temporary buffer to use */
){
  int rc;                         /* Return code */
  int szPage = pWal->hdr.szPage;  /* Database page-size */
  WalIterator *pIter = 0;         /* Wal iterator context */
  u32 iDbpage = 0;                /* Next database page to write */
  u32 iFrame = 0;                 /* Wal frame containing data for iDbpage */
  u32 mxSafeFrame;                /* Max frame that can be backfilled */
  int i;                          /* Loop counter */
  volatile WalIndexHdr *pHdr;     /* The actual wal-index header in SHM */
  volatile WalCkptInfo *pInfo;    /* The checkpoint status information */

  /* Allocate the iterator */
  rc = walIteratorInit(pWal, &pIter);
  if( rc!=SQLITE_OK || pWal->hdr.mxFrame==0 ){
    walIteratorFree(pIter);
    return rc;
  }

  /*** TODO:  Move this test out to the caller.  Make it an assert() here ***/
  if( pWal->hdr.szPage!=nBuf ){
    walIteratorFree(pIter);
    return SQLITE_CORRUPT_BKPT;
  }

  /* Compute in mxSafeFrame the index of the last frame of the WAL that is
  ** safe to write into the database.  Frames beyond mxSafeFrame might
  ** overwrite database pages that are in use by active readers and thus
  ** cannot be backfilled from the WAL.
  */
  mxSafeFrame = pWal->hdr.mxFrame;
  pHdr = (volatile WalIndexHdr*)pWal->pWiData;
  pInfo = (volatile WalCkptInfo*)&pHdr[2];
  assert( pInfo==walCkptInfo(pWal) );
  for(i=1; i<WAL_NREADER; i++){
    u32 y = pInfo->aReadMark[i];
    if( y>0 && (mxSafeFrame==0 || mxSafeFrame>=y) ){
      if( y<=pWal->hdr.mxFrame
       && walLockExclusive(pWal, WAL_READ_LOCK(i), 1)==SQLITE_OK
      ){
        pInfo->aReadMark[i] = 0;
        walUnlockExclusive(pWal, WAL_READ_LOCK(i), 1);
      }else{
        mxSafeFrame = y-1;
      }
    }
  }

  if( pInfo->nBackfill<mxSafeFrame
   && (rc = walLockExclusive(pWal, WAL_READ_LOCK(0), 1))==SQLITE_OK
  ){
    u32 nBackfill = pInfo->nBackfill;

    /* Sync the WAL to disk */
    if( sync_flags ){
      rc = sqlite3OsSync(pWal->pWalFd, sync_flags);
    }

    /* Iterate through the contents of the WAL, copying data to the db file. */
    while( rc==SQLITE_OK && 0==walIteratorNext(pIter, &iDbpage, &iFrame) ){
      if( iFrame<=nBackfill || iFrame>mxSafeFrame ) continue;
      rc = sqlite3OsRead(pWal->pWalFd, zBuf, szPage, 
          walFrameOffset(iFrame, szPage) + WAL_FRAME_HDRSIZE
      );
      if( rc!=SQLITE_OK ) break;
      rc = sqlite3OsWrite(pWal->pDbFd, zBuf, szPage, (iDbpage-1)*szPage);
      if( rc!=SQLITE_OK ) break;
    }

    /* If work was actually accomplished... */
    if( rc==SQLITE_OK && pInfo->nBackfill<mxSafeFrame ){
      pInfo->nBackfill = mxSafeFrame;
      if( mxSafeFrame==pHdr[0].mxFrame && sync_flags ){
        rc = sqlite3OsTruncate(pWal->pDbFd, ((i64)pWal->hdr.nPage*(i64)szPage));
        if( rc==SQLITE_OK && sync_flags ){
          rc = sqlite3OsSync(pWal->pDbFd, sync_flags);
        }
      }
    }

    /* Release the reader lock held while backfilling */
    walUnlockExclusive(pWal, WAL_READ_LOCK(0), 1);
  }else{
    /* Reset the return code so as not to report a checkpoint failure
    ** just because active readers prevent any backfill.
    */
    rc = SQLITE_OK;
  }

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
      pWal->exclusiveMode = 1;
      rc = sqlite3WalCheckpoint(pWal, sync_flags, nBuf, zBuf);
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
    WALTRACE(("WAL%p: closed\n", pWal));
    sqlite3_free(pWal);
  }
  return rc;
}

/*
** Try to read the wal-index header.  Return 0 on success and 1 if
** there is a problem.
**
** The wal-index is in shared memory.  Another thread or process might
** be writing the header at the same time this procedure is trying to
** read it, which might result in inconsistency.  A dirty read is detected
** by verifying that both copies of the header are the same and also by
** a checksum on the header.
**
** If and only if the read is consistent and the header is different from
** pWal->hdr, then pWal->hdr is updated to the content of the new header
** and *pChanged is set to 1.
**
** If the checksum cannot be verified return non-zero. If the header
** is read successfully and the checksum verified, return zero.
*/
int walIndexTryHdr(Wal *pWal, int *pChanged){
  u32 aCksum[2];               /* Checksum on the header content */
  WalIndexHdr h1, h2;          /* Two copies of the header content */
  WalIndexHdr *aHdr;           /* Header in shared memory */

  if( pWal->szWIndex < WALINDEX_HDR_SIZE ){
    /* The wal-index is not large enough to hold the header, then assume
    ** header is invalid. */
    return 1;
  }
  assert( pWal->pWiData );

  /* Read the header. This might happen currently with a write to the
  ** same area of shared memory on a different CPU in a SMP,
  ** meaning it is possible that an inconsistent snapshot is read
  ** from the file. If this happens, return non-zero.
  **
  ** There are two copies of the header at the beginning of the wal-index.
  ** When reading, read [0] first then [1].  Writes are in the reverse order.
  ** Memory barriers are used to prevent the compiler or the hardware from
  ** reordering the reads and writes.
  */
  aHdr = (WalIndexHdr*)pWal->pWiData;
  memcpy(&h1, &aHdr[0], sizeof(h1));
  sqlite3OsShmBarrier(pWal->pDbFd);
  memcpy(&h2, &aHdr[1], sizeof(h2));

  if( memcmp(&h1, &h2, sizeof(h1))!=0 ){
    return 1;   /* Dirty read */
  }  
  if( h1.isInit==0 ){
    return 1;   /* Malformed header - probably all zeros */
  }
  walChecksumBytes(1, (u8*)&h1, sizeof(h1)-sizeof(h1.aCksum), 0, aCksum);
  if( aCksum[0]!=h1.aCksum[0] || aCksum[1]!=h1.aCksum[1] ){
    return 1;   /* Checksum does not match */
  }

  if( memcmp(&pWal->hdr, &h1, sizeof(WalIndexHdr)) ){
    *pChanged = 1;
    memcpy(&pWal->hdr, &h1, sizeof(WalIndexHdr));
    pWal->szPage = pWal->hdr.szPage;
  }

  /* The header was successfully read. Return zero. */
  return 0;
}

/*
** Read the wal-index header from the wal-index and into pWal->hdr.
** If the wal-header appears to be corrupt, try to recover the log
** before returning.
**
** Set *pChanged to 1 if the wal-index header value in pWal->hdr is
** changed by this opertion.  If pWal->hdr is unchanged, set *pChanged
** to 0.
**
** This routine also maps the wal-index content into memory and assigns
** ownership of that mapping to the current thread.  In some implementations,
** only one thread at a time can hold a mapping of the wal-index.  Hence,
** the caller should strive to invoke walIndexUnmap() as soon as possible
** after this routine returns.
**
** If the wal-index header is successfully read, return SQLITE_OK. 
** Otherwise an SQLite error code.
*/
static int walIndexReadHdr(Wal *pWal, int *pChanged){
  int rc;                         /* Return code */
  int badHdr;                     /* True if a header read failed */

  assert( pChanged );
  rc = walIndexMap(pWal, walMappingSize(1));
  if( rc!=SQLITE_OK ){
    return rc;
  }

  /* Try once to read the header straight out.  This works most of the
  ** time.
  */
  badHdr = walIndexTryHdr(pWal, pChanged);

  /* If the first attempt failed, it might have been due to a race
  ** with a writer.  So get a WRITE lock and try again.
  */
  assert( badHdr==0 || pWal->writeLock==0 );
  if( badHdr ){
    rc = walLockExclusive(pWal, WAL_WRITE_LOCK, 1);
    if( rc==SQLITE_OK ){
      pWal->writeLock = 1;
      badHdr = walIndexTryHdr(pWal, pChanged);
      if( badHdr ){
        /* If the wal-index header is still malformed even while holding
        ** a WRITE lock, it can only mean that the header is corrupted and
        ** needs to be reconstructed.  So run recovery to do exactly that.
        */
        rc = walIndexRecover(pWal);
        *pChanged = 1;
      }
      walUnlockExclusive(pWal, WAL_WRITE_LOCK, 1);
      pWal->writeLock = 0;
    }else if( rc!=SQLITE_BUSY ){
      return rc;
    }
  }

  /* Make sure the mapping is large enough to cover the entire wal-index */
  if( rc==SQLITE_OK ){
    int szWanted = walMappingSize(pWal->hdr.mxFrame);
    if( pWal->szWIndex<szWanted ){
      rc = walIndexMap(pWal, szWanted);
    }
  }

  return rc;
}

/*
** This is the value that walTryBeginRead returns when it needs to
** be retried.
*/
#define WAL_RETRY  (-1)

/*
** Attempt to start a read transaction.  This might fail due to a race or
** other transient condition.  When that happens, it returns WAL_RETRY to
** indicate to the caller that it is safe to retry immediately.
**
** On success return SQLITE_OK.  On a permantent failure (such an
** I/O error or an SQLITE_BUSY because another process is running
** recovery) return a positive error code.
**
** On success, this routine obtains a read lock on 
** WAL_READ_LOCK(pWal->readLock).  The pWal->readLock integer is
** in the range 0 <= pWal->readLock < WAL_NREADER.  If pWal->readLock==(-1)
** that means the Wal does not hold any read lock.  The reader must not
** access any database page that is modified by a WAL frame up to and
** including frame number aReadMark[pWal->readLock].  The reader will
** use WAL frames up to and including pWal->hdr.mxFrame if pWal->readLock>0
** Or if pWal->readLock==0, then the reader will ignore the WAL
** completely and get all content directly from the database file.
** When the read transaction is completed, the caller must release the
** lock on WAL_READ_LOCK(pWal->readLock) and set pWal->readLock to -1.
**
** This routine uses the nBackfill and aReadMark[] fields of the header
** to select a particular WAL_READ_LOCK() that strives to let the
** checkpoint process do as much work as possible.  This routine might
** update values of the aReadMark[] array in the header, but if it does
** so it takes care to hold an exclusive lock on the corresponding
** WAL_READ_LOCK() while changing values.
*/
static int walTryBeginRead(Wal *pWal, int *pChanged, int useWal){
  volatile WalIndexHdr *pHdr;     /* Header of the wal-index */
  volatile WalCkptInfo *pInfo;    /* Checkpoint information in wal-index */
  u32 mxReadMark;                 /* Largest aReadMark[] value */
  int mxI;                        /* Index of largest aReadMark[] value */
  int i;                          /* Loop counter */
  int rc;                         /* Return code  */

  assert( pWal->readLock<0 );     /* Not currently locked */

  if( !useWal ){
    rc = walIndexReadHdr(pWal, pChanged);
    if( rc==SQLITE_BUSY ){
      /* If there is not a recovery running in another thread or process
      ** then convert BUSY errors to WAL_RETRY.  If recovery is known to
      ** be running, convert BUSY to BUSY_RECOVERY.  There is a race here
      ** which might cause WAL_RETRY to be returned even if BUSY_RECOVERY
      ** would be technically correct.  But the race is benign since with
      ** WAL_RETRY this routine will be called again and will probably be
      ** right on the second iteration.
      */
      rc = walLockShared(pWal, WAL_RECOVER_LOCK);
      if( rc==SQLITE_OK ){
        walUnlockShared(pWal, WAL_RECOVER_LOCK);
        rc = WAL_RETRY;
      }else if( rc==SQLITE_BUSY ){
        rc = SQLITE_BUSY_RECOVERY;
      }
    }
  }else{
    rc = walIndexMap(pWal, walMappingSize(pWal->hdr.mxFrame));
  }
  if( rc!=SQLITE_OK ){
    return rc;
  }

  pHdr = (volatile WalIndexHdr*)pWal->pWiData;
  pInfo = (volatile WalCkptInfo*)&pHdr[2];
  assert( pInfo==walCkptInfo(pWal) );
  if( !useWal && pInfo->nBackfill==pWal->hdr.mxFrame ){
    /* The WAL has been completely backfilled (or it is empty).
    ** and can be safely ignored.
    */
    rc = walLockShared(pWal, WAL_READ_LOCK(0));
    if( rc==SQLITE_OK ){
      if( pHdr->mxFrame!=pWal->hdr.mxFrame ){
        walUnlockShared(pWal, WAL_READ_LOCK(0));
        return WAL_RETRY;
      }
      pWal->readLock = 0;
      return SQLITE_OK;
    }else if( rc!=SQLITE_BUSY ){
      return rc;
    }
  }

  /* If we get this far, it means that the reader will want to use
  ** the WAL to get at content from recent commits.  The job now is
  ** to select one of the aReadMark[] entries that is closest to
  ** but not exceeding pWal->hdr.mxFrame and lock that entry.
  */
  mxReadMark = 0;
  mxI = 0;
  for(i=1; i<WAL_NREADER; i++){
    u32 thisMark = pInfo->aReadMark[i];
    if( mxReadMark<thisMark ){
      mxReadMark = thisMark;
      mxI = i;
    }
  }
  if( mxI==0 ){
    /* If we get here, it means that all of the aReadMark[] entries between
    ** 1 and WAL_NREADER-1 are zero.  Try to initialize aReadMark[1] to
    ** be mxFrame, then retry.
    */
    rc = walLockExclusive(pWal, WAL_READ_LOCK(1), 1);
    if( rc==SQLITE_OK ){
      pInfo->aReadMark[1] = pWal->hdr.mxFrame+1;
      walUnlockExclusive(pWal, WAL_READ_LOCK(1), 1);
    }
    return WAL_RETRY;
  }else{
    if( mxReadMark < pWal->hdr.mxFrame ){
      for(i=1; i<WAL_NREADER; i++){
        rc = walLockExclusive(pWal, WAL_READ_LOCK(i), 1);
        if( rc==SQLITE_OK ){
          mxReadMark = pInfo->aReadMark[i] = pWal->hdr.mxFrame+1;
          mxI = i;
          walUnlockExclusive(pWal, WAL_READ_LOCK(i), 1);
          break;
        }
      }
    }

    rc = walLockShared(pWal, WAL_READ_LOCK(mxI));
    if( rc ){
      return rc==SQLITE_BUSY ? WAL_RETRY : rc;
    }
    if( pInfo->aReadMark[mxI]!=mxReadMark
     || pHdr[0].mxFrame!=pWal->hdr.mxFrame
     || (sqlite3OsShmBarrier(pWal->pDbFd), pHdr[1].mxFrame!=pWal->hdr.mxFrame)
    ){
      walUnlockShared(pWal, WAL_READ_LOCK(mxI));
      return WAL_RETRY;
    }else{
      pWal->readLock = mxI;
    }
  }
  return rc;
}

/*
** Begin a read transaction on the database.
**
** This routine used to be called sqlite3OpenSnapshot() and with good reason:
** it takes a snapshot of the state of the WAL and wal-index for the current
** instant in time.  The current thread will continue to use this snapshot.
** Other threads might append new content to the WAL and wal-index but
** that extra content is ignored by the current thread.
**
** If the database contents have changes since the previous read
** transaction, then *pChanged is set to 1 before returning.  The
** Pager layer will use this to know that is cache is stale and
** needs to be flushed.
*/
int sqlite3WalBeginReadTransaction(Wal *pWal, int *pChanged){
  int rc;                         /* Return code */

  do{
    rc = walTryBeginRead(pWal, pChanged, 0);
  }while( rc==WAL_RETRY );
  walIndexUnmap(pWal);
  return rc;
}

/*
** Finish with a read transaction.  All this does is release the
** read-lock.
*/
void sqlite3WalEndReadTransaction(Wal *pWal){
  if( pWal->readLock>=0 ){
    walUnlockShared(pWal, WAL_READ_LOCK(pWal->readLock));
    pWal->readLock = -1;
  }
}

/*
** Read a page from the WAL, if it is present in the WAL and if the 
** current read transaction is configured to use the WAL.  
**
** The *pInWal is set to 1 if the requested page is in the WAL and
** has been loaded.  Or *pInWal is set to 0 if the page was not in 
** the WAL and needs to be read out of the database.
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
  u32 iLast = pWal->hdr.mxFrame;  /* Last page in WAL for this reader */
  int iHash;                      /* Used to loop through N hash tables */

  /* This routine is only called from within a read transaction */
  assert( pWal->readLock>=0 );

  /* If the "last page" field of the wal-index header snapshot is 0, then
  ** no data will be read from the wal under any circumstances. Return early
  ** in this case to avoid the walIndexMap/Unmap overhead.  Likewise, if
  ** pWal->readLock==0, then the WAL is ignored by the reader so
  ** return early, as if the WAL were empty.
  */
  if( iLast==0 || pWal->readLock==0 ){
    *pInWal = 0;
    return SQLITE_OK;
  }

  /* Ensure the wal-index is mapped. */
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
  ** table). This means the value just read from the hash 
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
  **     example, if at the start of the read-transaction the WAL
  **     contains three copies of the desired page in frames 2, 3 and 4,
  **     the hash table may contain the following:
  **
  **       { ..., 2, 3, 4, 99, 99, ..... }
  **
  **     The correct answer is to read data from frame 4. But a 
  **     dirty-read may potentially cause the hash-table to appear as 
  **     follows to the reader:
  **
  **       { ..., 2, 3, 4, 3, 99, ..... }
  **
  **     Without this part of the if(...) clause, the reader might
  **     incorrectly read data from frame 3 instead of 4. This would be
  **     an error.
  **
  ** It is not actually clear to the developers that such a dirty-read
  ** can occur. But if it does, it should not cause any problems.
  */
  for(iHash=iLast; iHash>0 && iRead==0; iHash-=HASHTABLE_NPAGE){
    volatile HASHTABLE_DATATYPE *aHash;  /* Pointer to hash table */
    volatile u32 *aPgno;                 /* Pointer to array of page numbers */
    u32 iZero;                    /* Frame number corresponding to aPgno[0] */
    int iKey;                     /* Hash slot index */
    int mxHash;                   /* upper bound on aHash[] values */

    walHashFind(pWal, iHash, &aHash, &aPgno, &iZero);
    mxHash = iLast - iZero;
    if( mxHash > HASHTABLE_NPAGE )  mxHash = HASHTABLE_NPAGE;
    for(iKey=walHash(pgno); aHash[iKey]; iKey=walNextHash(iKey)){
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
    i64 iOffset = walFrameOffset(iRead, pWal->hdr.szPage) + WAL_FRAME_HDRSIZE;
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
  assert( pWal->readLock>=0 );
  *pPgno = pWal->hdr.nPage;
}


/* 
** This function starts a write transaction on the WAL.
**
** A read transaction must have already been started by a prior call
** to sqlite3WalBeginReadTransaction().
**
** If another thread or process has written into the database since
** the read transaction was started, then it is not possible for this
** thread to write as doing so would cause a fork.  So this routine
** returns SQLITE_BUSY in that case and no write transaction is started.
**
** There can only be a single writer active at a time.
*/
int sqlite3WalBeginWriteTransaction(Wal *pWal){
  int rc;
  volatile WalCkptInfo *pInfo;

  /* Cannot start a write transaction without first holding a read
  ** transaction. */
  assert( pWal->readLock>=0 );

  /* Only one writer allowed at a time.  Get the write lock.  Return
  ** SQLITE_BUSY if unable.
  */
  rc = walLockExclusive(pWal, WAL_WRITE_LOCK, 1);
  if( rc ){
    return rc;
  }
  pWal->writeLock = 1;

  /* If another connection has written to the database file since the
  ** time the read transaction on this connection was started, then
  ** the write is disallowed.
  */
  rc = walIndexMap(pWal, walMappingSize(pWal->hdr.mxFrame));
  if( rc ){
    walUnlockExclusive(pWal, WAL_WRITE_LOCK, 1);
    pWal->writeLock = 0;
    return rc;
  }
  if( memcmp(&pWal->hdr, (void*)pWal->pWiData, sizeof(WalIndexHdr))!=0 ){
    walUnlockExclusive(pWal, WAL_WRITE_LOCK, 1);
    pWal->writeLock = 0;
    walIndexUnmap(pWal);
    return SQLITE_BUSY;
  }

  pInfo = walCkptInfo(pWal);
  if( pWal->readLock==0 ){
    assert( pInfo->nBackfill==pWal->hdr.mxFrame );
    if( pInfo->nBackfill>0 ){
      rc = walLockExclusive(pWal, WAL_READ_LOCK(1), WAL_NREADER-1);
      if( rc==SQLITE_OK ){
        /* If all readers are using WAL_READ_LOCK(0) (in other words if no
        ** readers are currently using the WAL) */
        pWal->nCkpt++;
        pWal->hdr.mxFrame = 0;
        sqlite3Put4byte((u8*)pWal->hdr.aSalt,
                         1 + sqlite3Get4byte((u8*)pWal->hdr.aSalt));
        sqlite3_randomness(4, &pWal->hdr.aSalt[1]);
        walIndexWriteHdr(pWal);
        pInfo->nBackfill = 0;
        memset((void*)&pInfo->aReadMark[1], 0,
               sizeof(pInfo->aReadMark)-sizeof(u32));
        rc = sqlite3OsTruncate(pWal->pDbFd, 
                               ((i64)pWal->hdr.nPage*(i64)pWal->szPage));
        walUnlockExclusive(pWal, WAL_READ_LOCK(1), WAL_NREADER-1);
      }
    }
    walUnlockShared(pWal, WAL_READ_LOCK(0));
    pWal->readLock = -1;
    do{
      int notUsed;
      rc = walTryBeginRead(pWal, &notUsed, 1);
    }while( rc==WAL_RETRY );
  }
  walIndexUnmap(pWal);
  return rc;
}

/*
** End a write transaction.  The commit has already been done.  This
** routine merely releases the lock.
*/
int sqlite3WalEndWriteTransaction(Wal *pWal){
  walUnlockExclusive(pWal, WAL_WRITE_LOCK, 1);
  pWal->writeLock = 0;
  return SQLITE_OK;
}

/*
** If any data has been written (but not committed) to the log file, this
** function moves the write-pointer back to the start of the transaction.
**
** Additionally, the callback function is invoked for each frame written
** to the WAL since the start of the transaction. If the callback returns
** other than SQLITE_OK, it is not invoked again and the error code is
** returned to the caller.
**
** Otherwise, if the callback function does not return an error, this
** function returns SQLITE_OK.
*/
int sqlite3WalUndo(Wal *pWal, int (*xUndo)(void *, Pgno), void *pUndoCtx){
  int rc = SQLITE_OK;
  if( pWal->writeLock ){
    int unused;
    Pgno iMax = pWal->hdr.mxFrame;
    Pgno iFrame;
  
    assert( pWal->pWiData==0 );
    rc = walIndexReadHdr(pWal, &unused);
    if( rc==SQLITE_OK ){
      rc = walIndexMap(pWal, walMappingSize(iMax));
    }
    if( rc==SQLITE_OK ){
      for(iFrame=pWal->hdr.mxFrame+1; rc==SQLITE_OK && iFrame<=iMax; iFrame++){
        assert( pWal->writeLock );
        rc = xUndo(pUndoCtx, pWal->pWiData[walIndexEntry(iFrame)]);
      }
      walCleanupHash(pWal);
    }
    walIndexUnmap(pWal);
  }
  return rc;
}

/* 
** Argument aWalData must point to an array of WAL_SAVEPOINT_NDATA u32 
** values. This function populates the array with values required to 
** "rollback" the write position of the WAL handle back to the current 
** point in the event of a savepoint rollback (via WalSavepointUndo()).
*/
void sqlite3WalSavepoint(Wal *pWal, u32 *aWalData){
  assert( pWal->writeLock );
  aWalData[0] = pWal->hdr.mxFrame;
  aWalData[1] = pWal->hdr.aFrameCksum[0];
  aWalData[2] = pWal->hdr.aFrameCksum[1];
}

/* 
** Move the write position of the WAL back to the point identified by
** the values in the aWalData[] array. aWalData must point to an array
** of WAL_SAVEPOINT_NDATA u32 values that has been previously populated
** by a call to WalSavepoint().
*/
int sqlite3WalSavepointUndo(Wal *pWal, u32 *aWalData){
  int rc = SQLITE_OK;
  assert( pWal->writeLock );

  assert( aWalData[0]<=pWal->hdr.mxFrame );
  if( aWalData[0]<pWal->hdr.mxFrame ){
    rc = walIndexMap(pWal, walMappingSize(pWal->hdr.mxFrame));
    pWal->hdr.mxFrame = aWalData[0];
    pWal->hdr.aFrameCksum[0] = aWalData[1];
    pWal->hdr.aFrameCksum[1] = aWalData[2];
    if( rc==SQLITE_OK ){
      walCleanupHash(pWal);
      walIndexUnmap(pWal);
    }
  }
  return rc;
}

/* 
** Write a set of frames to the log. The caller must hold the write-lock
** on the log file (obtained using sqlite3WalWriteLock()).
*/
int sqlite3WalFrames(
  Wal *pWal,                      /* Wal handle to write to */
  int szPage,                     /* Database page-size in bytes */
  PgHdr *pList,                   /* List of dirty pages to write */
  Pgno nTruncate,                 /* Database size after this commit */
  int isCommit,                   /* True if this is a commit */
  int sync_flags                  /* Flags to pass to OsSync() (or 0) */
){
  int rc;                         /* Used to catch return codes */
  u32 iFrame;                     /* Next frame address */
  u8 aFrame[WAL_FRAME_HDRSIZE];   /* Buffer to assemble frame-header in */
  PgHdr *p;                       /* Iterator to run through pList with. */
  PgHdr *pLast = 0;               /* Last frame in list */
  int nLast = 0;                  /* Number of extra copies of last page */

  assert( pList );
  assert( pWal->writeLock );
  assert( pWal->pWiData==0 );

#if defined(SQLITE_TEST) && defined(SQLITE_DEBUG)
  { int cnt; for(cnt=0, p=pList; p; p=p->pDirty, cnt++){}
    WALTRACE(("WAL%p: frame write begin. %d frames. mxFrame=%d. %s\n",
              pWal, cnt, pWal->hdr.mxFrame, isCommit ? "Commit" : "Spill"));
  }
#endif

  /* If this is the first frame written into the log, write the WAL
  ** header to the start of the WAL file. See comments at the top of
  ** this source file for a description of the WAL header format.
  */
  iFrame = pWal->hdr.mxFrame;
  if( iFrame==0 ){
    u8 aWalHdr[WAL_HDRSIZE];        /* Buffer to assembly wal-header in */
    sqlite3Put4byte(&aWalHdr[0], (WAL_MAGIC | SQLITE_BIGENDIAN));
    sqlite3Put4byte(&aWalHdr[4], 3007000);
    sqlite3Put4byte(&aWalHdr[8], szPage);
    pWal->szPage = szPage;
    pWal->hdr.bigEndCksum = SQLITE_BIGENDIAN;
    sqlite3Put4byte(&aWalHdr[12], pWal->nCkpt);
    memcpy(&aWalHdr[16], pWal->hdr.aSalt, 8);
    rc = sqlite3OsWrite(pWal->pWalFd, aWalHdr, sizeof(aWalHdr), 0);
    WALTRACE(("WAL%p: wal-header write %s\n", pWal, rc ? "failed" : "ok"));
    if( rc!=SQLITE_OK ){
      return rc;
    }
    walChecksumBytes(1, aWalHdr, sizeof(aWalHdr), 0, pWal->hdr.aFrameCksum);
  }
  assert( pWal->szPage==szPage );

    /* Write the log file. */
  for(p=pList; p; p=p->pDirty){
    u32 nDbsize;                  /* Db-size field for frame header */
    i64 iOffset;                  /* Write offset in log file */

    iOffset = walFrameOffset(++iFrame, szPage);
    
    /* Populate and write the frame header */
    nDbsize = (isCommit && p->pDirty==0) ? nTruncate : 0;
    walEncodeFrame(pWal, p->pgno, nDbsize, p->pData, aFrame);
    rc = sqlite3OsWrite(pWal->pWalFd, aFrame, sizeof(aFrame), iOffset);
    if( rc!=SQLITE_OK ){
      return rc;
    }

    /* Write the page data */
    rc = sqlite3OsWrite(pWal->pWalFd, p->pData, szPage, iOffset+sizeof(aFrame));
    if( rc!=SQLITE_OK ){
      return rc;
    }
    pLast = p;
  }

  /* Sync the log file if the 'isSync' flag was specified. */
  if( sync_flags ){
    i64 iSegment = sqlite3OsSectorSize(pWal->pWalFd);
    i64 iOffset = walFrameOffset(iFrame+1, szPage);

    assert( isCommit );
    assert( iSegment>0 );

    iSegment = (((iOffset+iSegment-1)/iSegment) * iSegment);
    while( iOffset<iSegment ){
      walEncodeFrame(pWal, pLast->pgno, nTruncate, pLast->pData, aFrame);
      rc = sqlite3OsWrite(pWal->pWalFd, aFrame, sizeof(aFrame), iOffset);
      if( rc!=SQLITE_OK ){
        return rc;
      }

      iOffset += WAL_FRAME_HDRSIZE;
      rc = sqlite3OsWrite(pWal->pWalFd, pLast->pData, szPage, iOffset); 
      if( rc!=SQLITE_OK ){
        return rc;
      }
      nLast++;
      iOffset += szPage;
    }

    rc = sqlite3OsSync(pWal->pWalFd, sync_flags);
  }
  assert( pWal->pWiData==0 );

  /* Append data to the wal-index. It is not necessary to lock the 
  ** wal-index to do this as the SQLITE_SHM_WRITE lock held on the wal-index
  ** guarantees that there are no other writers, and no data that may
  ** be in use by existing readers is being overwritten.
  */
  iFrame = pWal->hdr.mxFrame;
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
    pWal->hdr.szPage = szPage;
    pWal->hdr.mxFrame = iFrame;
    if( isCommit ){
      pWal->hdr.iChange++;
      pWal->hdr.nPage = nTruncate;
    }
    /* If this is a commit, update the wal-index header too. */
    if( isCommit ){
      walIndexWriteHdr(pWal);
      pWal->iCallback = iFrame;
    }
  }

  walIndexUnmap(pWal);
  WALTRACE(("WAL%p: frame write %s\n", pWal, rc ? "failed" : "ok"));
  return rc;
}

/* 
** This routine is called to implement sqlite3_wal_checkpoint() and
** related interfaces.
**
** Obtain a CHECKPOINT lock and then backfill as much information as
** we can from WAL into the database.
*/
int sqlite3WalCheckpoint(
  Wal *pWal,                      /* Wal connection */
  int sync_flags,                 /* Flags to sync db file with (or 0) */
  int nBuf,                       /* Size of temporary buffer */
  u8 *zBuf                        /* Temporary buffer to use */
){
  int rc;                         /* Return code */
  int isChanged = 0;              /* True if a new wal-index header is loaded */

  assert( pWal->pWiData==0 );
  assert( pWal->ckptLock==0 );

  WALTRACE(("WAL%p: checkpoint begins\n", pWal));
  rc = walLockExclusive(pWal, WAL_CKPT_LOCK, 1);
  if( rc ){
    /* Usually this is SQLITE_BUSY meaning that another thread or process
    ** is already running a checkpoint, or maybe a recovery.  But it might
    ** also be SQLITE_IOERR. */
    return rc;
  }
  pWal->ckptLock = 1;

  /* Copy data from the log to the database file. */
  rc = walIndexReadHdr(pWal, &isChanged);
  if( rc==SQLITE_OK ){
    rc = walCheckpoint(pWal, sync_flags, nBuf, zBuf);
  }
  if( isChanged ){
    /* If a new wal-index header was loaded before the checkpoint was 
    ** performed, then the pager-cache associated with pWal is now
    ** out of date. So zero the cached wal-index header to ensure that
    ** next time the pager opens a snapshot on this database it knows that
    ** the cache needs to be reset.
    */
    memset(&pWal->hdr, 0, sizeof(WalIndexHdr));
  }

  /* Release the locks. */
  walIndexUnmap(pWal);
  walUnlockExclusive(pWal, WAL_CKPT_LOCK, 1);
  pWal->ckptLock = 0;
  WALTRACE(("WAL%p: checkpoint %s\n", pWal, rc ? "failed" : "ok"));
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
** This function is called to change the WAL subsystem into or out
** of locking_mode=EXCLUSIVE.
**
** If op is zero, then attempt to change from locking_mode=EXCLUSIVE
** into locking_mode=NORMAL.  This means that we must acquire a lock
** on the pWal->readLock byte.  If the WAL is already in locking_mode=NORMAL
** or if the acquisition of the lock fails, then return 0.  If the
** transition out of exclusive-mode is successful, return 1.  This
** operation must occur while the pager is still holding the exclusive
** lock on the main database file.
**
** If op is one, then change from locking_mode=NORMAL into 
** locking_mode=EXCLUSIVE.  This means that the pWal->readLock must
** be released.  Return 1 if the transition is made and 0 if the
** WAL is already in exclusive-locking mode - meaning that this
** routine is a no-op.  The pager must already hold the exclusive lock
** on the main database file before invoking this operation.
**
** If op is negative, then do a dry-run of the op==1 case but do
** not actually change anything.  The pager uses this to see if it
** should acquire the database exclusive lock prior to invoking
** the op==1 case.
*/
int sqlite3WalExclusiveMode(Wal *pWal, int op){
  int rc;
  assert( pWal->writeLock==0 && pWal->readLock>=0 );
  if( op==0 ){
    if( pWal->exclusiveMode ){
      pWal->exclusiveMode = 0;
      if( walLockShared(pWal, WAL_READ_LOCK(pWal->readLock))!=SQLITE_OK ){
        pWal->exclusiveMode = 1;
      }
      rc = pWal->exclusiveMode==0;
    }else{
      /* No changes.  Either already in locking_mode=NORMAL or else the 
      ** acquisition of the read-lock failed.  The pager must continue to
      ** hold the database exclusive lock. */
      rc = 0;
    }
  }else if( op>0 ){
    assert( pWal->exclusiveMode==0 );
    walUnlockShared(pWal, WAL_READ_LOCK(pWal->readLock));
    pWal->exclusiveMode = 1;
    rc = 1;
  }else{
    rc = pWal->exclusiveMode==0;
  }
  return rc;
}

#endif /* #ifndef SQLITE_OMIT_WAL */

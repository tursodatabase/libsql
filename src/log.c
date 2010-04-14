
/*
** This file contains the implementation of a log file used in 
** "journal_mode=wal" mode.
*/

#include "log.h"

#include <unistd.h>
#include <fcntl.h>
#include <sys/mman.h>

typedef struct LogSummaryHdr LogSummaryHdr;
typedef struct LogSummary LogSummary;
typedef struct LogCheckpoint LogCheckpoint;
typedef struct LogLock LogLock;


/*
** The following structure may be used to store the same data that
** is stored in the log-summary header.
**
** Member variables iCheck1 and iCheck2 contain the checksum for the
** last frame written to the log, or 2 and 3 respectively if the log 
** is currently empty.
*/
struct LogSummaryHdr {
  u32 iChange;                    /* Counter incremented each transaction */
  u32 pgsz;                       /* Database page size in bytes */
  u32 iLastPg;                    /* Address of last valid frame in log */
  u32 nPage;                      /* Size of database in pages */
  u32 iCheck1;                    /* Checkpoint value 1 */
  u32 iCheck2;                    /* Checkpoint value 2 */
};

/* Size of serialized LogSummaryHdr object. */
#define LOGSUMMARY_HDR_NFIELD (sizeof(LogSummaryHdr) / sizeof(u32))

#define LOGSUMMARY_FRAME_OFFSET \
  (LOGSUMMARY_HDR_NFIELD + LOG_CKSM_BYTES/sizeof(u32))

/* Size of frame header */
#define LOG_FRAME_HDRSIZE 20

/*
** There is one instance of this structure for each log-summary object
** that this process has a connection to. They are stored in a linked
** list starting at pLogSummary (global variable).
**
** TODO: LogSummary.fd is a unix file descriptor. Unix APIs are used 
**       directly in this implementation because the VFS does not support
**       the required blocking file-locks.
*/
struct LogSummary {
  sqlite3_mutex *mutex;           /* Mutex used to protect this object */
  int nRef;                       /* Number of pointers to this structure */
  int fd;                         /* File descriptor open on log-summary */
  char *zPath;                    /* Path to associated WAL file */
  LogLock *pLock;                 /* Linked list of locks on this object */
  LogSummary *pNext;              /* Next in global list */
  int nData;                      /* Size of aData allocation/mapping */
  u32 *aData;                     /* File body */
};


/*
** The four lockable regions associated with each log-summary. A connection
** may take either a SHARED or EXCLUSIVE lock on each.
*/
#define LOG_REGION_A 0x01
#define LOG_REGION_B 0x02
#define LOG_REGION_C 0x04
#define LOG_REGION_D 0x08

/*
** A single instance of this structure is allocated as part of each 
** connection to a database log. All structures associated with the 
** same log file are linked together into a list using LogLock.pNext
** starting at LogSummary.pLock.
**
** The mLock field of the structure describes the locks (if any) 
** currently held by the connection. If a SHARED lock is held on
** any of the four locking regions, then the associated LOG_REGION_X
** bit (see above) is set. If an EXCLUSIVE lock is held on the region,
** then the (LOG_REGION_X << 8) bit is set.
*/
struct LogLock {
  LogLock *pNext;                 /* Next lock on the same log */
  u32 mLock;                      /* Mask of locks */
};

struct Log {
  LogSummary *pSummary;           /* Log file summary data */
  sqlite3_vfs *pVfs;              /* The VFS used to create pFd */
  sqlite3_file *pFd;              /* File handle for log file */
  int sync_flags;                 /* Flags to use with OsSync() */
  int isLocked;                   /* Non-zero if a snapshot is held open */
  int isWriteLocked;              /* True if this is the writer connection */
  LogSummaryHdr hdr;              /* Log summary header for current snapshot */
  LogLock lock;                   /* Lock held by this connection (if any) */
};


/*
** This structure is used to implement an iterator that iterates through
** all frames in the log in database page order. Where two or more frames
** correspond to the same database page, the iterator visits only the 
** frame most recently written to the log.
**
** The internals of this structure are only accessed by:
**
**   logCheckpointInit() - Create a new iterator,
**   logCheckpointNext() - Step an iterator,
**   logCheckpointFree() - Free an iterator.
**
** This functionality is used by the checkpoint code (see logCheckpoint()).
*/
struct LogCheckpoint {
  int nSegment;                   /* Size of LogCheckpoint.aSummary[] array */
  int nFinal;                     /* Elements in segment nSegment-1 */
  struct LogSegment {
    int iNext;                    /* Next aIndex index */
    u8 *aIndex;                   /* Pointer to index array */
    u32 *aDbPage;                 /* Pointer to db page array */
  } aSegment[1];
};


/*
** List of all LogSummary objects created by this process. Protected by
** static mutex LOG_SUMMARY_MUTEX. TODO: Should have a dedicated mutex
** here instead of borrowing the LRU mutex.
*/
#define LOG_SUMMARY_MUTEX SQLITE_MUTEX_STATIC_LRU
static LogSummary *pLogSummary = 0;

/*
** Generate an 8 byte checksum based on the data in array aByte[] and the
** initial values of aCksum[0] and aCksum[1]. The checksum is written into
** aCksum[] before returning.
*/
#define LOG_CKSM_BYTES 8
static void logChecksumBytes(u8 *aByte, int nByte, u32 *aCksum){
  u32 *z32 = (u32 *)aByte;
  int n32 = nByte / sizeof(u32);
  int i;

  assert( LOG_CKSM_BYTES==2*sizeof(u32) );
  assert( (nByte&0x00000003)==0 );

  u32 cksum0 = aCksum[0];
  u32 cksum1 = aCksum[1];

  for(i=0; i<n32; i++){
    cksum0 = (cksum0 >> 8) + (cksum0 ^ z32[i]);
    cksum1 = (cksum1 >> 8) + (cksum1 ^ z32[i]);
  }

  aCksum[0] = cksum0;
  aCksum[1] = cksum1;
}

/*
** Argument zPath must be a nul-terminated string containing a path-name.
** This function modifies the string in-place by removing any "./" or "../" 
** elements in the path. For example, the following input:
**
**   "/home/user/plans/good/../evil/./world_domination.txt"
**
** is overwritten with the 'normalized' version:
**
**   "/home/user/plans/evil/world_domination.txt"
*/
static void logNormalizePath(char *zPath){
  int i, j;
  char *z = zPath;
  int n = strlen(z);

  while( n>1 && z[n-1]=='/' ){ n--; }
  for(i=j=0; i<n; i++){
    if( z[i]=='/' ){
      if( z[i+1]=='/' ) continue;
      if( z[i+1]=='.' && i+2<n && z[i+2]=='/' ){
        i += 1;
        continue;
      }
      if( z[i+1]=='.' && i+3<n && z[i+2]=='.' && z[i+3]=='/' ){
        while( j>0 && z[j-1]!='/' ){ j--; }
        if( j>0 ){ j--; }
        i += 2;
        continue;
      }
    }
    z[j++] = z[i];
  }
  z[j] = 0;
}

/*
** Lock the summary file pSummary->fd.
*/
static int logSummaryLock(LogSummary *pSummary){
  int rc;
  struct flock f;
  memset(&f, 0, sizeof(f));
  f.l_type = F_WRLCK;
  f.l_whence = SEEK_SET;
  f.l_start = 0;
  f.l_len = 1;
  rc = fcntl(pSummary->fd, F_SETLKW, &f);
  if( rc!=0 ){
    return SQLITE_IOERR;
  }
  return SQLITE_OK;
}

/*
** Unlock the summary file pSummary->fd.
*/
static int logSummaryUnlock(LogSummary *pSummary){
  int rc;
  struct flock f;
  memset(&f, 0, sizeof(f));
  f.l_type = F_UNLCK;
  f.l_whence = SEEK_SET;
  f.l_start = 0;
  f.l_len = 1;
  rc = fcntl(pSummary->fd, F_SETLK, &f);
  if( rc!=0 ){
    return SQLITE_IOERR;
  }
  return SQLITE_OK;
}

/*
** Memory map the first nByte bytes of the summary file opened with 
** pSummary->fd at pSummary->aData. If the summary file is smaller than
** nByte bytes in size when this function is called, ftruncate() is
** used to expand it before it is mapped.
**
** It is assumed that an exclusive lock is held on the summary file
** by the caller (to protect the ftruncate()).
*/
static int logSummaryMap(LogSummary *pSummary, int nByte){
  struct stat sStat;
  int rc;
  int fd = pSummary->fd;
  void *pMap;

  assert( pSummary->aData==0 );

  /* If the file is less than nByte bytes in size, cause it to grow. */
  rc = fstat(fd, &sStat);
  if( rc!=0 ) return SQLITE_IOERR;
  if( sStat.st_size<nByte ){
    rc = ftruncate(fd, nByte);
    if( rc!=0 ) return SQLITE_IOERR;
  }

  /* Map the file. */
  pMap = mmap(0, nByte, PROT_READ|PROT_WRITE, MAP_SHARED, fd, 0);
  if( pMap==MAP_FAILED ){
    return SQLITE_IOERR;
  }
  pSummary->aData = (u32 *)pMap;
  pSummary->nData = nByte;

  return SQLITE_OK;
}

/*
** Unmap the log-summary mapping and close the file-descriptor. If
** the isTruncate argument is non-zero, truncate the log-summary file
** region to zero bytes.
**
** Regardless of the value of isTruncate, close the file-descriptor
** opened on the log-summary file.
*/
static int logSummaryUnmap(LogSummary *pSummary, int isTruncate){
  int rc = SQLITE_OK;
  if( pSummary->aData ){
    assert( pSummary->fd>0 );
    munmap(pSummary->aData, pSummary->nData);
    pSummary->aData = 0;
    if( isTruncate ){
      rc = (ftruncate(pSummary->fd, 0) ? SQLITE_IOERR : SQLITE_OK);
    }
  }
  if( pSummary->fd>0 ){
    close(pSummary->fd);
    pSummary->fd = -1;
  }
  return rc;
}


static void logSummaryWriteHdr(LogSummary *pSummary, LogSummaryHdr *pHdr){
  u32 *aData = pSummary->aData;
  memcpy(aData, pHdr, sizeof(LogSummaryHdr));
  aData[LOGSUMMARY_HDR_NFIELD] = 1;
  aData[LOGSUMMARY_HDR_NFIELD+1] = 1;
  logChecksumBytes(
    (u8 *)aData, sizeof(LogSummaryHdr), &aData[LOGSUMMARY_HDR_NFIELD]
  );
}

/*
** This function encodes a single frame header and writes it to a buffer
** supplied by the caller. A log frame-header is made up of a series of 
** 4-byte big-endian integers, as follows:
**
**     0: Database page size in bytes.
**     4: Page number.
**     8: New database size (for commit frames, otherwise zero).
**    12: Frame checksum 1.
**    16: Frame checksum 2.
*/
static void logEncodeFrame(
  u32 *aCksum,                    /* IN/OUT: Checksum values */
  u32 iPage,                      /* Database page number for frame */
  u32 nTruncate,                  /* New db size (or 0 for non-commit frames) */
  int nData,                      /* Database page size (size of aData[]) */
  u8 *aData,                      /* Pointer to page data (for checksum) */
  u8 *aFrame                      /* OUT: Write encoded frame here */
){
  assert( LOG_FRAME_HDRSIZE==20 );

  sqlite3Put4byte(&aFrame[0], nData);
  sqlite3Put4byte(&aFrame[4], iPage);
  sqlite3Put4byte(&aFrame[8], nTruncate);

  logChecksumBytes(aFrame, 12, aCksum);
  logChecksumBytes(aData, nData, aCksum);

  sqlite3Put4byte(&aFrame[12], aCksum[0]);
  sqlite3Put4byte(&aFrame[16], aCksum[1]);
}

/*
** Return 1 and populate *piPage, *pnTruncate and aCksum if the 
** frame checksum looks Ok. Otherwise return 0.
*/
static int logDecodeFrame(
  u32 *aCksum,                    /* IN/OUT: Checksum values */
  u32 *piPage,                    /* OUT: Database page number for frame */
  u32 *pnTruncate,                /* OUT: New db size (or 0 if not commit) */
  int nData,                      /* Database page size (size of aData[]) */
  u8 *aData,                      /* Pointer to page data (for checksum) */
  u8 *aFrame                      /* Frame data */
){
  logChecksumBytes(aFrame, 12, aCksum);
  logChecksumBytes(aData, nData, aCksum);

  if( aCksum[0]!=sqlite3Get4byte(&aFrame[12]) 
   || aCksum[1]!=sqlite3Get4byte(&aFrame[16]) 
  ){
    /* Checksum failed. */
    return 0;
  }

  *piPage = sqlite3Get4byte(&aFrame[4]);
  *pnTruncate = sqlite3Get4byte(&aFrame[8]);
  return 1;
}

static void logMergesort8(
  Pgno *aContent,                 /* Pages in log */
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
    logMergesort8(aContent, aBuffer, aLeft, &nLeft);
    logMergesort8(aContent, aBuffer, aRight, &nRight);

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
** Return the index in the LogSummary.aData array that corresponds to 
** frame iFrame. The log-summary file consists of a header, followed by
** alternating "map" and "index" blocks.
*/
static int logSummaryEntry(u32 iFrame){
  return ((((iFrame-1)>>8)<<6) + iFrame-1 + 2 + LOGSUMMARY_HDR_NFIELD);
}


/*
** Set an entry in the log-summary map to map log frame iFrame to db 
** page iPage. Values are always appended to the log-summary (i.e. the
** value of iFrame is always exactly one more than the value passed to
** the previous call), but that restriction is not enforced or asserted
** here.
*/
static void logSummaryAppend(LogSummary *pSummary, u32 iFrame, u32 iPage){
  u32 iSlot = logSummaryEntry(iFrame);

  /* Set the log-summary entry itself */
  pSummary->aData[iSlot] = iPage;

  /* If the frame number is a multiple of 256 (frames are numbered starting
  ** at 1), build an index of the most recently added 256 frames.
  */
  if( (iFrame&0x000000FF)==0 ){
    int i;                        /* Iterator used while initializing aIndex */
    u32 *aFrame;                  /* Pointer to array of 256 frames */
    int nIndex;                   /* Number of entries in index */
    u8 *aIndex;                   /* 256 bytes to build index in */
    u8 *aTmp;                     /* Scratch space to use while sorting */

    aFrame = &pSummary->aData[iSlot-255];
    aIndex = (u8 *)&pSummary->aData[iSlot+1];
    aTmp = &aIndex[256];

    nIndex = 256;
    for(i=0; i<256; i++) aIndex[i] = (u8)i;
    logMergesort8(aFrame, aTmp, aIndex, &nIndex);
    memset(&aIndex[nIndex], aIndex[nIndex-1], 256-nIndex);
  }
}


/*
** Recover the log-summary by reading the log file. The caller must hold 
** an exclusive lock on the log-summary file.
*/
static int logSummaryRecover(LogSummary *pSummary, sqlite3_file *pFd){
  int rc;                         /* Return Code */
  i64 nSize;                      /* Size of log file */
  LogSummaryHdr hdr;              /* Recovered log-summary header */

  memset(&hdr, 0, sizeof(hdr));

  rc = sqlite3OsFileSize(pFd, &nSize);
  if( rc!=SQLITE_OK ){
    return rc;
  }

  if( nSize>LOG_FRAME_HDRSIZE ){
    u8 aBuf[LOG_FRAME_HDRSIZE];   /* Buffer to load first frame header into */
    u8 *aFrame = 0;               /* Malloc'd buffer to load entire frame */
    int nFrame;                   /* Number of bytes at aFrame */
    u8 *aData;                    /* Pointer to data part of aFrame buffer */
    int iFrame;                   /* Index of last frame read */
    i64 iOffset;                  /* Next offset to read from log file */
    int nPgsz;                    /* Page size according to the log */
    u32 aCksum[2] = {2, 3};       /* Running checksum */

    /* Read in the first frame header in the file (to determine the 
    ** database page size).
    */
    rc = sqlite3OsRead(pFd, aBuf, LOG_FRAME_HDRSIZE, 0);
    if( rc!=SQLITE_OK ){
      return rc;
    }

    /* If the database page size is not a power of two, or is greater than
    ** SQLITE_MAX_PAGE_SIZE, conclude that the log file contains no valid data.
    */
    nPgsz = sqlite3Get4byte(&aBuf[0]);
    if( nPgsz&(nPgsz-1) || nPgsz>SQLITE_MAX_PAGE_SIZE ){
      goto finished;
    }

    /* Malloc a buffer to read frames into. */
    nFrame = nPgsz + LOG_FRAME_HDRSIZE;
    aFrame = (u8 *)sqlite3_malloc(nFrame);
    if( !aFrame ){
      return SQLITE_NOMEM;
    }
    aData = &aFrame[LOG_FRAME_HDRSIZE];

    /* Read all frames from the log file. */
    iFrame = 0;
    iOffset = 0;
    for(iOffset=0; (iOffset+nFrame)<nSize; iOffset+=nFrame){
      u32 pgno;                   /* Database page number for frame */
      u32 nTruncate;              /* dbsize field from frame header */
      int isValid;                /* True if this frame is valid */

      /* Read and decode the next log frame. */
      rc = sqlite3OsRead(pFd, aFrame, nFrame, iOffset);
      if( rc!=SQLITE_OK ) break;
      isValid = logDecodeFrame(aCksum, &pgno, &nTruncate, nPgsz, aData, aFrame);
      if( !isValid ) break;
      logSummaryAppend(pSummary, ++iFrame, pgno);

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
  logSummaryWriteHdr(pSummary, &hdr);
  return rc;
}


/*
** This function intializes the connection to the log-summary identified
** by struct pSummary.
*/
static int logSummaryInit(LogSummary *pSummary, sqlite3_file *pFd){
  int rc;                         /* Return Code */
  char *zFile;                    /* File name for summary file */

  assert( pSummary->fd<0 );
  assert( pSummary->aData==0 );
  assert( pSummary->nRef>0 );
  assert( pSummary->zPath );

  /* Open a file descriptor on the summary file. */
  zFile = sqlite3_mprintf("%s-summary", pSummary->zPath);
  if( !zFile ){
    return SQLITE_NOMEM;
  }
  pSummary->fd = open(zFile, O_RDWR|O_CREAT, S_IWUSR|S_IRUSR);
  sqlite3_free(zFile);
  if( pSummary->fd<0 ){
    return SQLITE_IOERR;
  }

  /* Grab an exclusive lock the summary file. Then mmap() it. TODO: This 
  ** code needs to be enhanced to support a growable mapping. For now, just 
  ** make the mapping very large to start with.
  */
  rc = logSummaryLock(pSummary);
  if( rc!=SQLITE_OK ) return rc;
  rc = logSummaryMap(pSummary, 512*1024);
  if( rc!=SQLITE_OK ) goto out;

  /* Grab a SHARED lock on the log file. Then try to upgrade to an EXCLUSIVE
  ** lock. If successful, then this is the first (and only) connection to
  ** the database. In this case assume the contents of the log-summary 
  ** cannot be trusted. Zero the log-summary header to make sure.
  **
  ** The SHARED lock on the log file is not released until the connection
  ** to the database is closed.
  */
  rc = sqlite3OsLock(pFd, SQLITE_LOCK_SHARED);
  if( rc!=SQLITE_OK ) goto out;
  rc = sqlite3OsLock(pFd, SQLITE_LOCK_EXCLUSIVE);
  if( rc==SQLITE_OK ){
    /* This is the first and only connection. */
    memset(pSummary->aData, 0, (LOGSUMMARY_HDR_NFIELD+2)*sizeof(u32) );
    rc = sqlite3OsUnlock(pFd, SQLITE_LOCK_SHARED);
  }else if( rc==SQLITE_BUSY ){
    rc = SQLITE_OK;
  }

 out:
  logSummaryUnlock(pSummary);
  return rc;
}

/* 
** Open a connection to the log file associated with database zDb. The
** database file does not actually have to exist. zDb is used only to
** figure out the name of the log file to open. If the log file does not 
** exist it is created by this call.
*/
int sqlite3LogOpen(
  sqlite3_vfs *pVfs,              /* vfs module to open log file with */
  const char *zDb,                /* Name of database file */
  Log **ppLog                     /* OUT: Allocated Log handle */
){
  int rc = SQLITE_OK;             /* Return Code */
  Log *pRet;                      /* Object to allocate and return */
  LogSummary *pSummary = 0;       /* Summary object */
  sqlite3_mutex *mutex = 0;       /* LOG_SUMMARY_MUTEX mutex */
  int flags;                      /* Flags passed to OsOpen() */
  char *zWal = 0;                 /* Path to WAL file */
  int nWal;                       /* Length of zWal in bytes */

  /* Zero output variables */
  assert( zDb );
  *ppLog = 0;

  /* Allocate an instance of struct Log to return. */
  pRet = (Log *)sqlite3MallocZero(sizeof(Log) + pVfs->szOsFile);
  if( !pRet ) goto out;
  pRet->pVfs = pVfs;
  pRet->pFd = (sqlite3_file *)&pRet[1];
  pRet->sync_flags = SQLITE_SYNC_NORMAL;

  /* Normalize the path name. */
  zWal = sqlite3_mprintf("%s-wal", zDb);
  if( !zWal ) goto out;
  logNormalizePath(zWal);
  flags = (SQLITE_OPEN_READWRITE|SQLITE_OPEN_CREATE|SQLITE_OPEN_MAIN_DB);
  nWal = sqlite3Strlen30(zWal);

  /* Enter the mutex that protects the linked-list of LogSummary structures */
  if( sqlite3GlobalConfig.bCoreMutex ){
    mutex = sqlite3_mutex_alloc(LOG_SUMMARY_MUTEX);
  }
  sqlite3_mutex_enter(mutex);

  /* Search for an existing log summary object in the linked list. If one 
  ** cannot be found, allocate and initialize a new object.
  */
  for(pSummary=pLogSummary; pSummary; pSummary=pSummary->pNext){
    int nPath = sqlite3Strlen30(pSummary->zPath);
    if( nWal==nPath && 0==memcmp(pSummary->zPath, zWal, nPath) ) break;
  }
  if( !pSummary ){
    int nByte = sizeof(LogSummary) + nWal + 1;
    pSummary = (LogSummary *)sqlite3MallocZero(nByte);
    if( !pSummary ){
      rc = SQLITE_NOMEM;
      goto out;
    }
    if( sqlite3GlobalConfig.bCoreMutex ){
      pSummary->mutex = sqlite3_mutex_alloc(SQLITE_MUTEX_RECURSIVE);
    }
    pSummary->zPath = (char *)&pSummary[1];
    pSummary->fd = -1;
    memcpy(pSummary->zPath, zWal, nWal);
    pSummary->pNext = pLogSummary;
    pLogSummary = pSummary;
  }
  pSummary->nRef++;
  pRet->pSummary = pSummary;

  /* Exit the mutex protecting the linked-list of LogSummary objects. */
  sqlite3_mutex_leave(mutex);
  mutex = 0;

  /* Open file handle on the log file. */
  rc = sqlite3OsOpen(pVfs, pSummary->zPath, pRet->pFd, flags, &flags);
  if( rc!=SQLITE_OK ) goto out;

  /* Object pSummary is shared between all connections to the database made
  ** by this process. So at this point it may or may not be connected to
  ** the log-summary. If it is not, connect it. Otherwise, just take the
  ** SHARED lock on the log file.
  */
  sqlite3_mutex_enter(pSummary->mutex);
  mutex = pSummary->mutex;
  if( pSummary->fd<0 ){
    rc = logSummaryInit(pSummary, pRet->pFd);
  }else{
    rc = sqlite3OsLock(pRet->pFd, SQLITE_LOCK_SHARED);
  }

  pRet->lock.pNext = pSummary->pLock;
  pSummary->pLock = &pRet->lock;

 out:
  sqlite3_mutex_leave(mutex);
  sqlite3_free(zWal);
  if( rc!=SQLITE_OK ){
    assert(0);
    if( pRet ){
      sqlite3OsClose(pRet->pFd);
      sqlite3_free(pRet);
    }
    assert( !pSummary || pSummary->nRef==0 );
    sqlite3_free(pSummary);
  }
  *ppLog = pRet;
  return rc;
}

static int logCheckpointNext(
  LogCheckpoint *p,               /* Iterator */
  u32 *piPage,                    /* OUT: Next db page to write */
  u32 *piFrame                    /* OUT: Log frame to read from */
){
  u32 iMin = *piPage;
  u32 iRet = 0xFFFFFFFF;
  int i;
  int nBlock = p->nFinal;

  for(i=p->nSegment-1; i>=0; i--){
    struct LogSegment *pSegment = &p->aSegment[i];
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

static LogCheckpoint *logCheckpointInit(Log *pLog){
  u32 *aData = pLog->pSummary->aData;
  LogCheckpoint *p;               /* Return value */
  int nSegment;                   /* Number of segments to merge */
  u32 iLast;                      /* Last frame in log */
  int nByte;                      /* Number of bytes to allocate */
  int i;                          /* Iterator variable */
  int nFinal;                     /* Number of unindexed entries */
  struct LogSegment *pFinal;      /* Final (unindexed) segment */
  u8 *aTmp;                       /* Temp space used by merge-sort */

  iLast = pLog->hdr.iLastPg;
  nSegment = (iLast >> 8) + 1;
  nFinal = (iLast & 0x000000FF);

  nByte = sizeof(LogCheckpoint) + (nSegment-1)*sizeof(struct LogSegment) + 512;
  p = (LogCheckpoint *)sqlite3_malloc(nByte);
  if( p ){
    memset(p, 0, nByte);
    p->nSegment = nSegment;
    p->nFinal = nFinal;
  }

  for(i=0; i<nSegment-1; i++){
    p->aSegment[i].aDbPage = &aData[logSummaryEntry(i*256+1)];
    p->aSegment[i].aIndex = (u8 *)&aData[logSummaryEntry(i*256+1)+256];
  }
  pFinal = &p->aSegment[nSegment-1];

  pFinal->aDbPage = &aData[logSummaryEntry((nSegment-1)*256+1)];
  pFinal->aIndex = (u8 *)&pFinal[1];
  aTmp = &pFinal->aIndex[256];
  for(i=0; i<nFinal; i++){
    pFinal->aIndex[i] = i;
  }
  logMergesort8(pFinal->aDbPage, aTmp, pFinal->aIndex, &nFinal);
  p->nFinal = nFinal;

  return p;
}

/* 
** Free a log iterator allocated by logCheckpointInit().
*/
static void logCheckpointFree(LogCheckpoint *p){
  sqlite3_free(p);
}

/*
** Checkpoint the contents of the log file.
*/
static int logCheckpoint(
  Log *pLog,                      /* Log connection */
  sqlite3_file *pFd,              /* File descriptor open on db file */
  u8 *zBuf                        /* Temporary buffer to use */
){
  int rc;                         /* Return code */
  int pgsz = pLog->hdr.pgsz;      /* Database page-size */
  LogCheckpoint *pIter = 0;       /* Log iterator context */
  u32 iDbpage = 0;                /* Next database page to write */
  u32 iFrame = 0;                 /* Log frame containing data for iDbpage */

  /* Allocate the iterator */
  pIter = logCheckpointInit(pLog);
  if( !pIter ) return SQLITE_NOMEM;

  /* Sync the log file to disk */
  rc = sqlite3OsSync(pLog->pFd, pLog->sync_flags);
  if( rc!=SQLITE_OK ) goto out;

  /* Iterate through the contents of the log, copying data to the db file. */
  while( 0==logCheckpointNext(pIter, &iDbpage, &iFrame) ){
    rc = sqlite3OsRead(pLog->pFd, zBuf, pgsz, 
        (iFrame-1) * (pgsz+LOG_FRAME_HDRSIZE) + LOG_FRAME_HDRSIZE
    );
    if( rc!=SQLITE_OK ) goto out;
    rc = sqlite3OsWrite(pFd, zBuf, pgsz, (iDbpage-1)*pgsz);
    if( rc!=SQLITE_OK ) goto out;
  }

  /* Truncate the database file */
  rc = sqlite3OsTruncate(pFd, ((i64)pLog->hdr.nPage*(i64)pgsz));
  if( rc!=SQLITE_OK ) goto out;

  /* Sync the database file. If successful, update the log-summary. */
  rc = sqlite3OsSync(pFd, pLog->sync_flags);
  if( rc!=SQLITE_OK ) goto out;
  pLog->hdr.iLastPg = 0;
  pLog->hdr.iCheck1 = 2;
  pLog->hdr.iCheck2 = 3;
  logSummaryWriteHdr(pLog->pSummary, &pLog->hdr);

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
  memset(zBuf, 0, LOG_FRAME_HDRSIZE);
  rc = sqlite3OsWrite(pLog->pFd, zBuf, LOG_FRAME_HDRSIZE, 0);
  if( rc!=SQLITE_OK ) goto out;
  rc = sqlite3OsSync(pLog->pFd, pLog->sync_flags);
#endif

 out:
  logCheckpointFree(pIter);
  return rc;
}

/*
** Close a connection to a log file.
*/
int sqlite3LogClose(
  Log *pLog,                      /* Log to close */
  sqlite3_file *pFd,              /* Database file */
  u8 *zBuf                        /* Buffer of at least page-size bytes */
){
  int rc = SQLITE_OK;
  if( pLog ){
    LogLock **ppL;
    LogSummary *pSummary = pLog->pSummary;
    sqlite3_mutex *mutex = 0;

    sqlite3_mutex_enter(pSummary->mutex);
    for(ppL=&pSummary->pLock; *ppL!=&pLog->lock; ppL=&(*ppL)->pNext);
    *ppL = pLog->lock.pNext;
    sqlite3_mutex_leave(pSummary->mutex);

    if( sqlite3GlobalConfig.bCoreMutex ){
      mutex = sqlite3_mutex_alloc(LOG_SUMMARY_MUTEX);
    }
    sqlite3_mutex_enter(mutex);

    /* Decrement the reference count on the log summary. If this is the last
    ** reference to the log summary object in this process, the object will
    ** be freed. If this is also the last connection to the database, then
    ** checkpoint the database and truncate the log and log-summary files
    ** to zero bytes in size.
    **/
    pSummary->nRef--;
    if( pSummary->nRef==0 ){
      LogSummary **pp;

      rc = logSummaryLock(pSummary);
      if( rc==SQLITE_OK ){
        int isTruncate = 0;
        int rc2 = sqlite3OsLock(pLog->pFd, SQLITE_LOCK_EXCLUSIVE);
        if( rc2==SQLITE_OK ){
          /* This is the last connection to the database (including other
          ** processes). Do three things:
          **
          **   1. Checkpoint the db.
          **   2. Truncate the log file to zero bytes.
          **   3. Truncate the log-summary file to zero bytes.
          */
          rc2 = logCheckpoint(pLog, pFd, zBuf);
          if( rc2==SQLITE_OK ){
            rc2 = sqlite3OsTruncate(pLog->pFd, 0);
          }
          isTruncate = 1;
        }else if( rc2==SQLITE_BUSY ){
          rc2 = SQLITE_OK;
        }
        logSummaryUnmap(pSummary, isTruncate);
        sqlite3OsUnlock(pLog->pFd, SQLITE_LOCK_NONE);
        rc = logSummaryUnlock(pSummary);
        if( rc2!=SQLITE_OK ) rc = rc2;
      }

      /* Remove the LogSummary object from the global list. Then free the 
      ** mutex and the object itself.
      */
      for(pp=&pLogSummary; *pp!=pSummary; pp=&(*pp)->pNext);
      *pp = (*pp)->pNext;
      sqlite3_mutex_free(pSummary->mutex);
      sqlite3_free(pSummary);
    }

    sqlite3_mutex_leave(mutex);

    /* Close the connection to the log file and free the Log handle. */
    sqlite3OsClose(pLog->pFd);
    sqlite3_free(pLog);
  }
  return rc;
}

/*
** Set the flags to pass to the sqlite3OsSync() function when syncing
** the log file.
*/
#if 0
void sqlite3LogSetSyncflags(Log *pLog, int sync_flags){
  assert( sync_flags==SQLITE_SYNC_NORMAL || sync_flags==SQLITE_SYNC_FULL );
  pLog->sync_flags = sync_flags;
}
#endif

/*
** Enter and leave the log-summary mutex. In this context, entering the
** log-summary mutex means:
**
**   1. Obtaining mutex pLog->pSummary->mutex, and
**   2. Taking an exclusive lock on the log-summary file.
**
** i.e. this mutex locks out other processes as well as other threads
** hosted in this address space.
*/
static int logEnterMutex(Log *pLog){
  LogSummary *pSummary = pLog->pSummary;
  int rc;

  sqlite3_mutex_enter(pSummary->mutex);
  rc = logSummaryLock(pSummary);
  if( rc!=SQLITE_OK ){
    sqlite3_mutex_leave(pSummary->mutex);
  }
  return rc;
}
static void logLeaveMutex(Log *pLog){
  LogSummary *pSummary = pLog->pSummary;
  logSummaryUnlock(pSummary);
  sqlite3_mutex_leave(pSummary->mutex);
}

/*
** Values for the second parameter to logLockRegion().
*/
#define LOG_UNLOCK 0
#define LOG_RDLOCK 1
#define LOG_WRLOCK 2

static int logLockRegion(Log *pLog, u32 mRegion, int op){
  LogSummary *pSummary = pLog->pSummary;
  LogLock *p;                     /* Used to iterate through in-process locks */
  u32 mOther;                     /* Locks held by other connections */
  u32 mNew;                       /* New mask for pLog */

  assert( 
       /* Writer lock operations */
          (op==LOG_WRLOCK && mRegion==(LOG_REGION_C|LOG_REGION_D))
       || (op==LOG_UNLOCK && mRegion==(LOG_REGION_C|LOG_REGION_D))

       /* Normal reader lock operations */
       || (op==LOG_RDLOCK && mRegion==(LOG_REGION_A|LOG_REGION_B))
       || (op==LOG_UNLOCK && mRegion==(LOG_REGION_A))
       || (op==LOG_UNLOCK && mRegion==(LOG_REGION_B))

       /* Region D reader lock operations */
       || (op==LOG_RDLOCK && mRegion==(LOG_REGION_D))
       || (op==LOG_UNLOCK && mRegion==(LOG_REGION_D))

       /* Checkpointer lock operations */
       || (op==LOG_WRLOCK && mRegion==(LOG_REGION_B|LOG_REGION_C))
       || (op==LOG_WRLOCK && mRegion==(LOG_REGION_A))
       || (op==LOG_UNLOCK && mRegion==(LOG_REGION_B|LOG_REGION_C))
       || (op==LOG_UNLOCK && mRegion==(LOG_REGION_A|LOG_REGION_B|LOG_REGION_C))
  );

  /* Assert that a connection never tries to go from an EXCLUSIVE to a 
  ** SHARED lock on a region. Moving from SHARED to EXCLUSIVE sometimes
  ** happens though (when a region D reader upgrades to a writer).
  */
  assert( op!=LOG_RDLOCK || 0==(pLog->lock.mLock & (mRegion<<8)) );

  sqlite3_mutex_enter(pSummary->mutex);

  /* Calculate a mask of logs held by all connections in this process apart
  ** from this one. The least significant byte of the mask contains a mask
  ** of the SHARED logs held. The next least significant byte of the mask
  ** indicates the EXCLUSIVE locks held. For example, to test if some other
  ** connection is holding a SHARED lock on region A, or an EXCLUSIVE lock
  ** on region C, do:
  **
  **   hasSharedOnA    = (mOther & (LOG_REGION_A<<0));
  **   hasExclusiveOnC = (mOther & (LOG_REGION_C<<8));
  **
  ** In all masks, if the bit in the EXCLUSIVE byte mask is set, so is the 
  ** corresponding bit in the SHARED mask.
  */
  mOther = 0;
  for(p=pSummary->pLock; p; p=p->pNext){
    assert( (p->mLock & (p->mLock<<8))==(p->mLock&0x0000FF00) );
    if( p!=&pLog->lock ){
      mOther |= p->mLock;
    }
  }

  /* If this call is to lock a region (not to unlock one), test if locks held
  ** by any other connection in this process prevent the new locks from
  ** begin granted. If so, exit the summary mutex and return SQLITE_BUSY.
  */
  if( op && (mOther & (mRegion << (op==LOG_RDLOCK ? 8 : 0))) ){
    sqlite3_mutex_leave(pSummary->mutex);
    return SQLITE_BUSY;
  }

  /* Figure out the new log mask for this connection. */
  switch( op ){
    case LOG_UNLOCK: 
      mNew = (pLog->lock.mLock & ~(mRegion|(mRegion<<8)));
      break;
    case LOG_RDLOCK:
      mNew = (pLog->lock.mLock | mRegion);
      break;
    default:
      assert( op==LOG_WRLOCK );
      mNew = (pLog->lock.mLock | (mRegion<<8) | mRegion);
      break;
  }

  /* Now modify the locks held on the log-summary file descriptor. This
  ** file descriptor is shared by all log connections in this process. 
  ** Therefore:
  **
  **   + If one or more log connections in this process hold a SHARED lock
  **     on a region, the file-descriptor should hold a SHARED lock on
  **     the file region.
  **
  **   + If a log connection in this process holds an EXCLUSIVE lock on a
  **     region, the file-descriptor should also hold an EXCLUSIVE lock on
  **     the region in question.
  **
  ** If this is an LOG_UNLOCK operation, only regions for which no other
  ** connection holds a lock should actually be unlocked. And if this
  ** is a LOG_RDLOCK operation and other connections already hold all
  ** the required SHARED locks, then no system call is required.
  */
  if( op==LOG_UNLOCK ){
    mRegion = (mRegion & ~mOther);
  }
  if( (op==LOG_WRLOCK)
   || (op==LOG_UNLOCK && mRegion) 
   || (op==LOG_RDLOCK && (mOther&mRegion)!=mRegion)
  ){
    struct LockMap {
      int iStart;                 /* Byte offset to start locking operation */
      int iLen;                   /* Length field for locking operation */
    } aMap[] = {
      /* 0000 */ {0, 0},    /* 0001 */ {4, 1}, 
      /* 0010 */ {3, 1},    /* 0011 */ {3, 2},
      /* 0100 */ {2, 1},    /* 0101 */ {0, 0}, 
      /* 0110 */ {2, 2},    /* 0111 */ {2, 3},
      /* 1000 */ {1, 1},    /* 1001 */ {0, 0}, 
      /* 1010 */ {0, 0},    /* 1011 */ {0, 0},
      /* 1100 */ {1, 2},    /* 1101 */ {0, 0}, 
      /* 1110 */ {0, 0},    /* 1111 */ {0, 0}
    };
    int rc;                       /* Return code of fcntl() */
    struct flock f;               /* Locking operation */

    assert( mRegion<ArraySize(aMap) && aMap[mRegion].iStart!=0 );

    memset(&f, 0, sizeof(f));
    f.l_type = (op==LOG_WRLOCK?F_WRLCK:(op==LOG_RDLOCK?F_RDLCK:F_UNLCK));
    f.l_whence = SEEK_SET;
    f.l_start = 32 + aMap[mRegion].iStart;
    f.l_len = aMap[mRegion].iLen;

    rc = fcntl(pSummary->fd, F_SETLK, &f);
    if( rc!=0 ){
      sqlite3_mutex_leave(pSummary->mutex);
      return SQLITE_BUSY;
    }
  }

  pLog->lock.mLock = mNew;
  sqlite3_mutex_leave(pSummary->mutex);
  return SQLITE_OK;
}

/*
** Try to read the log-summary header. Attempt to verify the header
** checksum. If the checksum can be verified, copy the log-summary
** header into structure pLog->hdr. If the contents of pLog->hdr are
** modified by this and pChanged is not NULL, set *pChanged to 1. 
** Otherwise leave *pChanged unmodified.
**
** If the checksum cannot be verified return SQLITE_ERROR.
*/
int logSummaryTryHdr(Log *pLog, int *pChanged){
  u32 aCksum[2] = {1, 1};
  u32 aHdr[LOGSUMMARY_HDR_NFIELD+2];

  /* First try to read the header without a lock. Verify the checksum
  ** before returning. This will almost always work.  
  */
  memcpy(aHdr, pLog->pSummary->aData, sizeof(aHdr));
  logChecksumBytes((u8*)aHdr, sizeof(u32)*LOGSUMMARY_HDR_NFIELD, aCksum);
  if( aCksum[0]!=aHdr[LOGSUMMARY_HDR_NFIELD]
   || aCksum[1]!=aHdr[LOGSUMMARY_HDR_NFIELD+1]
  ){
    return SQLITE_ERROR;
  }

  if( memcmp(&pLog->hdr, aHdr, sizeof(LogSummaryHdr)) ){
    if( pChanged ){
      *pChanged = 1;
    }
    memcpy(&pLog->hdr, aHdr, sizeof(LogSummaryHdr));
  }
  return SQLITE_OK;
}

/*
** Read the log-summary header from the log-summary file into structure 
** pLog->hdr. If attempting to verify the header checksum fails, try
** to recover the log before returning.
**
** If the log-summary header is successfully read, return SQLITE_OK. 
** Otherwise an SQLite error code.
*/
int logSummaryReadHdr(Log *pLog, int *pChanged){
  int rc;

  /* First try to read the header without a lock. Verify the checksum
  ** before returning. This will almost always work.  
  */
  if( SQLITE_OK==logSummaryTryHdr(pLog, pChanged) ){
    return SQLITE_OK;
  }

  /* If the first attempt to read the header failed, lock the log-summary
  ** file and try again. If the header checksum verification fails this
  ** time as well, run log recovery.
  */
  if( SQLITE_OK==(rc = logEnterMutex(pLog)) ){
    if( SQLITE_OK!=logSummaryTryHdr(pLog, pChanged) ){
      if( pChanged ){
        *pChanged = 1;
      }
      rc = logSummaryRecover(pLog->pSummary, pLog->pFd);
      if( rc==SQLITE_OK ){
        rc = logSummaryTryHdr(pLog, 0);
      }
    }
    logLeaveMutex(pLog);
  }

  return rc;
}

/*
** Lock a snapshot.
**
** If this call obtains a new read-lock and the database contents have been
** modified since the most recent call to LogCloseSnapshot() on this Log
** connection, then *pChanged is set to 1 before returning. Otherwise, it 
** is left unmodified. This is used by the pager layer to determine whether 
** or not any cached pages may be safely reused.
*/
int sqlite3LogOpenSnapshot(Log *pLog, int *pChanged){
  int rc = SQLITE_OK;
  if( pLog->isLocked==0 ){
    int nAttempt;

    /* Obtain a snapshot-lock on the log-summary file. The procedure
    ** for obtaining the snapshot log is:
    **
    **    1. Attempt a SHARED lock on regions A and B.
    **    2a. If step 1 is successful, drop the lock on region B.
    **    2b. If step 1 is unsuccessful, attempt a SHARED lock on region D.
    **    3. Repeat the above until the lock attempt in step 1 or 2b is 
    **       successful.
    **
    ** If neither of the locks can be obtained after 5 tries, presumably
    ** something is wrong (i.e. a process not following the locking protocol). 
    ** Return an error code in this case.
    */
    rc = SQLITE_BUSY;
    for(nAttempt=0; nAttempt<5 && rc==SQLITE_BUSY; nAttempt++){
      rc = logLockRegion(pLog, LOG_REGION_A|LOG_REGION_B, LOG_RDLOCK);
      if( rc==SQLITE_BUSY ){
        rc = logLockRegion(pLog, LOG_REGION_D, LOG_RDLOCK);
        if( rc==SQLITE_OK ) pLog->isLocked = LOG_REGION_D;
      }else{
        logLockRegion(pLog, LOG_REGION_B, LOG_UNLOCK);
        pLog->isLocked = LOG_REGION_A;
      }
    }
    if( rc!=SQLITE_OK ){
      return rc;
    }

    rc = logSummaryReadHdr(pLog, pChanged);
    if( rc!=SQLITE_OK ){
      /* An error occured while attempting log recovery. */
      sqlite3LogCloseSnapshot(pLog);
    }
  }
  return rc;
}

/*
** Unlock the current snapshot.
*/
void sqlite3LogCloseSnapshot(Log *pLog){
  if( pLog->isLocked ){
    assert( pLog->isLocked==LOG_REGION_A || pLog->isLocked==LOG_REGION_D );
    logLockRegion(pLog, pLog->isLocked, LOG_UNLOCK);
  }
  pLog->isLocked = 0;
}



/* 
** Read a page from the log, if it is present. 
*/
int sqlite3LogRead(Log *pLog, Pgno pgno, int *pInLog, u8 *pOut){
  u32 iRead = 0;
  u32 *aData = pLog->pSummary->aData;
  int iFrame = (pLog->hdr.iLastPg & 0xFFFFFF00);

  /* Do a linear search of the unindexed block of page-numbers (if any) 
  ** at the end of the log-summary. An alternative to this would be to
  ** build an index in private memory each time a read transaction is
  ** opened on a new snapshot.
  */
  if( pLog->hdr.iLastPg ){
    u32 *pi = &aData[logSummaryEntry(pLog->hdr.iLastPg)];
    u32 *piStop = pi - (pLog->hdr.iLastPg & 0xFF);
    while( *pi!=pgno && pi!=piStop ) pi--;
    if( pi!=piStop ){
      iRead = (pi-piStop) + iFrame;
    }
  }
  assert( iRead==0 || aData[logSummaryEntry(iRead)]==pgno );

  while( iRead==0 && iFrame>0 ){
    int iLow = 0;
    int iHigh = 255;
    u32 *aFrame;
    u8 *aIndex;

    iFrame -= 256;
    aFrame = &aData[logSummaryEntry(iFrame+1)];
    aIndex = (u8 *)&aFrame[256];

    while( iLow<=iHigh ){
      int iTest = (iLow+iHigh)>>1;
      u32 iPg = aFrame[aIndex[iTest]];

      if( iPg==pgno ){
        iRead = iFrame + 1 + aIndex[iTest];
        break;
      }
      else if( iPg<pgno ){
        iLow = iTest+1;
      }else{
        iHigh = iTest-1;
      }
    }
  }
  assert( iRead==0 || aData[logSummaryEntry(iRead)]==pgno );

  /* If iRead is non-zero, then it is the log frame number that contains the
  ** required page. Read and return data from the log file.
  */
  if( iRead ){
    i64 iOffset = (iRead-1) * (pLog->hdr.pgsz+LOG_FRAME_HDRSIZE);
    iOffset += LOG_FRAME_HDRSIZE;
    *pInLog = 1;
    return sqlite3OsRead(pLog->pFd, pOut, pLog->hdr.pgsz, iOffset);
  }

  *pInLog = 0;
  return SQLITE_OK;
}


/* 
** Set *pPgno to the size of the database file (or zero, if unknown).
*/
void sqlite3LogMaxpgno(Log *pLog, Pgno *pPgno){
  assert( pLog->isLocked );
  *pPgno = pLog->hdr.nPage;
}

/* 
** The caller must hold at least a RESERVED lock on the database file
** when invoking this function.
**
** This function returns SQLITE_OK if the caller may write to the database.
** Otherwise, if the caller is operating on a snapshot that has already
** been overwritten by another writer, SQLITE_OBE is returned.
*/
int sqlite3LogWriteLock(Log *pLog, int op){
  assert( pLog->isLocked );
  if( op ){

    /* Obtain the writer lock */
    int rc = logLockRegion(pLog, LOG_REGION_C|LOG_REGION_D, LOG_WRLOCK);
    if( rc!=SQLITE_OK ){
      return rc;
    }

    /* TODO: What if this is a region D reader? And after writing this
    ** transaction it continues to hold a read-lock on the db? Maybe we 
    ** need to switch it to a region A reader here so that unlocking C|D
    ** does not leave the connection with no lock at all.
    */
    assert( pLog->isLocked!=LOG_REGION_D );

    if( memcmp(&pLog->hdr, pLog->pSummary->aData, sizeof(pLog->hdr)) ){
      return SQLITE_BUSY;
    }
    pLog->isWriteLocked = 1;

  }else if( pLog->isWriteLocked ){
    logLockRegion(pLog, LOG_REGION_C|LOG_REGION_D, LOG_UNLOCK);
    memcpy(&pLog->hdr, pLog->pSummary->aData, sizeof(pLog->hdr));
    pLog->isWriteLocked = 0;
  }
  return SQLITE_OK;
}

/* 
** Write a set of frames to the log. The caller must hold at least a
** RESERVED lock on the database file.
*/
int sqlite3LogFrames(
  Log *pLog,                      /* Log handle to write to */
  int nPgsz,                      /* Database page-size in bytes */
  PgHdr *pList,                   /* List of dirty pages to write */
  Pgno nTruncate,                 /* Database size after this commit */
  int isCommit,                   /* True if this is a commit */
  int isSync                      /* True to sync the log file */
){
  /* Each frame has a 20 byte header, as follows:
  **
  **   + Pseudo-random salt (4 bytes)
  **   + Page number (4 bytes)
  **   + New database size, or 0 if not a commit frame (4 bytes)
  **   + Checksum (CHECKSUM_BYTES bytes);
  **
  ** The checksum is computed based on the following:
  **
  **   + The previous checksum, or {2, 3} for the first frame in the log.
  **   + The non-checksum fields of the frame header, and
  **   + The frame contents (page data).
  **
  ** This format must also be understood by the code in logSummaryRecover().
  ** The size of the frame header is used by LogRead() and LogCheckpoint().
  */
  int rc;                         /* Used to catch return codes */
  u32 iFrame;                     /* Next frame address */
  u8 aFrame[LOG_FRAME_HDRSIZE];
  PgHdr *p;                       /* Iterator to run through pList with. */
  u32 aCksum[2];

  PgHdr *pLast;                   /* Last frame in list */
  int nLast = 0;                  /* Number of extra copies of last page */

  assert( LOG_FRAME_HDRSIZE==(4 * 3 + LOG_CKSM_BYTES) );
  assert( pList );

  aCksum[0] = pLog->hdr.iCheck1;
  aCksum[1] = pLog->hdr.iCheck2;

  /* Write the log file. */
  iFrame = pLog->hdr.iLastPg;
  for(p=pList; p; p=p->pDirty){
    u32 nDbsize;                  /* Db-size field for frame header */
    i64 iOffset;                  /* Write offset in log file */

    iFrame++;
    iOffset = (iFrame-1) * (nPgsz+sizeof(aFrame));
    
    /* Populate and write the frame header */
    nDbsize = (isCommit && p->pDirty==0) ? nTruncate : 0;
    logEncodeFrame(aCksum, p->pgno, nDbsize, nPgsz, p->pData, aFrame);
    rc = sqlite3OsWrite(pLog->pFd, aFrame, sizeof(aFrame), iOffset);
    if( rc!=SQLITE_OK ){
      return rc;
    }

    /* Write the page data */
    rc = sqlite3OsWrite(pLog->pFd, p->pData, nPgsz, iOffset + sizeof(aFrame));
    if( rc!=SQLITE_OK ){
      return rc;
    }
    pLast = p;
  }

  /* Sync the log file if the 'isSync' flag was specified. */
  if( isSync ){
#if 0
    i64 iSegment = sqlite3OsSectorSize(pLog->pFd);
    i64 iOffset = iFrame * (nPgsz+sizeof(aFrame));

    if( iSegment<SQLITE_DEFAULT_SECTOR_SIZE ){
      iSegment = SQLITE_DEFAULT_SECTOR_SIZE;
    }
    iSegment = (((iOffset+iSegment-1)/iSegment) * iSegment);
    while( iOffset<iSegment ){
      logEncodeFrame(aCksum,pLast->pgno,nTruncate,nPgsz,pLast->pData,aFrame);
      rc = sqlite3OsWrite(pLog->pFd, aFrame, sizeof(aFrame), iOffset);
      if( rc!=SQLITE_OK ){
        return rc;
      }

      iOffset += LOG_FRAME_HDRSIZE;
      rc = sqlite3OsWrite(pLog->pFd, pLast->pData, nPgsz, iOffset); 
      if( rc!=SQLITE_OK ){
        return rc;
      }
      nLast++;
      iOffset += nPgsz;
    }
#endif

    rc = sqlite3OsSync(pLog->pFd, pLog->sync_flags);
    if( rc!=SQLITE_OK ){
      return rc;
    }
  }

  /* Append data to the log summary. It is not necessary to lock the 
  ** log-summary to do this as the RESERVED lock held on the db file
  ** guarantees that there are no other writers, and no data that may
  ** be in use by existing readers is being overwritten.
  */
  iFrame = pLog->hdr.iLastPg;
  for(p=pList; p; p=p->pDirty){
    iFrame++;
    logSummaryAppend(pLog->pSummary, iFrame, p->pgno);
  }
  while( nLast>0 ){
    iFrame++;
    nLast--;
    logSummaryAppend(pLog->pSummary, iFrame, pLast->pgno);
  }

  /* Update the private copy of the header. */
  pLog->hdr.pgsz = nPgsz;
  pLog->hdr.iLastPg = iFrame;
  if( isCommit ){
    pLog->hdr.iChange++;
    pLog->hdr.nPage = nTruncate;
  }
  pLog->hdr.iCheck1 = aCksum[0];
  pLog->hdr.iCheck2 = aCksum[1];

  /* If this is a commit, update the log-summary header too. */
  if( isCommit && SQLITE_OK==(rc = logEnterMutex(pLog)) ){
    logSummaryWriteHdr(pLog->pSummary, &pLog->hdr);
    logLeaveMutex(pLog);
  }

  return SQLITE_OK;
}

/* 
** Checkpoint the database:
**
**   1. Wait for an EXCLUSIVE lock on regions B and C.
**   2. Wait for an EXCLUSIVE lock on region A.
**   3. Copy the contents of the log into the database file.
**   4. Zero the log-summary header (so new readers will ignore the log).
**   5. Drop the locks obtained in steps 1 and 2.
*/
int sqlite3LogCheckpoint(
  Log *pLog,                      /* Log connection */
  sqlite3_file *pFd,              /* File descriptor open on db file */
  u8 *zBuf,                       /* Temporary buffer to use */
  int (*xBusyHandler)(void *),    /* Pointer to busy-handler function */
  void *pBusyHandlerArg           /* Argument to pass to xBusyHandler */
){
  int rc;                         /* Return code */

  /* Wait for a write-lock on regions B and C. */
  do {
    rc = logLockRegion(pLog, LOG_REGION_B|LOG_REGION_C, LOG_WRLOCK);
  }while( rc==SQLITE_BUSY && xBusyHandler(pBusyHandlerArg) );
  if( rc!=SQLITE_OK ) return rc;

  /* Wait for a write-lock on region A. */
  do {
    rc = logLockRegion(pLog, LOG_REGION_A, LOG_WRLOCK);
  }while( rc==SQLITE_BUSY && xBusyHandler(pBusyHandlerArg) );
  if( rc!=SQLITE_OK ){
    logLockRegion(pLog, LOG_REGION_B|LOG_REGION_C, LOG_UNLOCK);
    return rc;
  }

  /* Copy data from the log to the database file. */
  rc = logSummaryReadHdr(pLog, 0);
  if( rc==SQLITE_OK ){
    rc = logCheckpoint(pLog, pFd, zBuf);
  }

  /* Release the locks. */
  logLockRegion(pLog, LOG_REGION_A|LOG_REGION_B|LOG_REGION_C, LOG_UNLOCK);
  return rc;
}


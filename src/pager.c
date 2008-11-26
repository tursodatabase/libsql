/*
** 2001 September 15
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
*************************************************************************
** This is the implementation of the page cache subsystem or "pager".
** 
** The pager is used to access a database disk file.  It implements
** atomic commit and rollback through the use of a journal file that
** is separate from the database file.  The pager also implements file
** locking to prevent two processes from writing the same database
** file simultaneously, or one process from reading the database while
** another is writing.
**
** @(#) $Id: pager.c,v 1.506.2.1 2008/11/26 14:55:02 drh Exp $
*/
#ifndef SQLITE_OMIT_DISKIO
#include "sqliteInt.h"

/*
** Macros for troubleshooting.  Normally turned off
*/
#if 0
#define sqlite3DebugPrintf printf
#define PAGERTRACE1(X)       sqlite3DebugPrintf(X)
#define PAGERTRACE2(X,Y)     sqlite3DebugPrintf(X,Y)
#define PAGERTRACE3(X,Y,Z)   sqlite3DebugPrintf(X,Y,Z)
#define PAGERTRACE4(X,Y,Z,W) sqlite3DebugPrintf(X,Y,Z,W)
#define PAGERTRACE5(X,Y,Z,W,V) sqlite3DebugPrintf(X,Y,Z,W,V)
#else
#define PAGERTRACE1(X)
#define PAGERTRACE2(X,Y)
#define PAGERTRACE3(X,Y,Z)
#define PAGERTRACE4(X,Y,Z,W)
#define PAGERTRACE5(X,Y,Z,W,V)
#endif

/*
** The following two macros are used within the PAGERTRACEX() macros above
** to print out file-descriptors. 
**
** PAGERID() takes a pointer to a Pager struct as its argument. The
** associated file-descriptor is returned. FILEHANDLEID() takes an sqlite3_file
** struct as its argument.
*/
#define PAGERID(p) ((int)(p->fd))
#define FILEHANDLEID(fd) ((int)fd)

/*
** The page cache as a whole is always in one of the following
** states:
**
**   PAGER_UNLOCK        The page cache is not currently reading or 
**                       writing the database file.  There is no
**                       data held in memory.  This is the initial
**                       state.
**
**   PAGER_SHARED        The page cache is reading the database.
**                       Writing is not permitted.  There can be
**                       multiple readers accessing the same database
**                       file at the same time.
**
**   PAGER_RESERVED      This process has reserved the database for writing
**                       but has not yet made any changes.  Only one process
**                       at a time can reserve the database.  The original
**                       database file has not been modified so other
**                       processes may still be reading the on-disk
**                       database file.
**
**   PAGER_EXCLUSIVE     The page cache is writing the database.
**                       Access is exclusive.  No other processes or
**                       threads can be reading or writing while one
**                       process is writing.
**
**   PAGER_SYNCED        The pager moves to this state from PAGER_EXCLUSIVE
**                       after all dirty pages have been written to the
**                       database file and the file has been synced to
**                       disk. All that remains to do is to remove or
**                       truncate the journal file and the transaction 
**                       will be committed.
**
** The page cache comes up in PAGER_UNLOCK.  The first time a
** sqlite3PagerGet() occurs, the state transitions to PAGER_SHARED.
** After all pages have been released using sqlite_page_unref(),
** the state transitions back to PAGER_UNLOCK.  The first time
** that sqlite3PagerWrite() is called, the state transitions to
** PAGER_RESERVED.  (Note that sqlite3PagerWrite() can only be
** called on an outstanding page which means that the pager must
** be in PAGER_SHARED before it transitions to PAGER_RESERVED.)
** PAGER_RESERVED means that there is an open rollback journal.
** The transition to PAGER_EXCLUSIVE occurs before any changes
** are made to the database file, though writes to the rollback
** journal occurs with just PAGER_RESERVED.  After an sqlite3PagerRollback()
** or sqlite3PagerCommitPhaseTwo(), the state can go back to PAGER_SHARED,
** or it can stay at PAGER_EXCLUSIVE if we are in exclusive access mode.
*/
#define PAGER_UNLOCK      0
#define PAGER_SHARED      1   /* same as SHARED_LOCK */
#define PAGER_RESERVED    2   /* same as RESERVED_LOCK */
#define PAGER_EXCLUSIVE   4   /* same as EXCLUSIVE_LOCK */
#define PAGER_SYNCED      5

/*
** If the SQLITE_BUSY_RESERVED_LOCK macro is set to true at compile-time,
** then failed attempts to get a reserved lock will invoke the busy callback.
** This is off by default.  To see why, consider the following scenario:
** 
** Suppose thread A already has a shared lock and wants a reserved lock.
** Thread B already has a reserved lock and wants an exclusive lock.  If
** both threads are using their busy callbacks, it might be a long time
** be for one of the threads give up and allows the other to proceed.
** But if the thread trying to get the reserved lock gives up quickly
** (if it never invokes its busy callback) then the contention will be
** resolved quickly.
*/
#ifndef SQLITE_BUSY_RESERVED_LOCK
# define SQLITE_BUSY_RESERVED_LOCK 0
#endif

/*
** This macro rounds values up so that if the value is an address it
** is guaranteed to be an address that is aligned to an 8-byte boundary.
*/
#define FORCE_ALIGNMENT(X)   (((X)+7)&~7)

/*
** A macro used for invoking the codec if there is one
*/
#ifdef SQLITE_HAS_CODEC
# define CODEC1(P,D,N,X) if( P->xCodec!=0 ){ P->xCodec(P->pCodecArg,D,N,X); }
# define CODEC2(P,D,N,X) ((char*)(P->xCodec!=0?P->xCodec(P->pCodecArg,D,N,X):D))
#else
# define CODEC1(P,D,N,X) /* NO-OP */
# define CODEC2(P,D,N,X) ((char*)D)
#endif

/*
** A open page cache is an instance of the following structure.
**
** Pager.errCode may be set to SQLITE_IOERR, SQLITE_CORRUPT, or
** or SQLITE_FULL. Once one of the first three errors occurs, it persists
** and is returned as the result of every major pager API call.  The
** SQLITE_FULL return code is slightly different. It persists only until the
** next successful rollback is performed on the pager cache. Also,
** SQLITE_FULL does not affect the sqlite3PagerGet() and sqlite3PagerLookup()
** APIs, they may still be used successfully.
*/
struct Pager {
  sqlite3_vfs *pVfs;          /* OS functions to use for IO */
  u8 journalOpen;             /* True if journal file descriptors is valid */
  u8 journalStarted;          /* True if header of journal is synced */
  u8 useJournal;              /* Use a rollback journal on this file */
  u8 noReadlock;              /* Do not bother to obtain readlocks */
  u8 stmtOpen;                /* True if the statement subjournal is open */
  u8 stmtInUse;               /* True we are in a statement subtransaction */
  u8 stmtAutoopen;            /* Open stmt journal when main journal is opened*/
  u8 noSync;                  /* Do not sync the journal if true */
  u8 fullSync;                /* Do extra syncs of the journal for robustness */
  u8 sync_flags;              /* One of SYNC_NORMAL or SYNC_FULL */
  u8 state;                   /* PAGER_UNLOCK, _SHARED, _RESERVED, etc. */
  u8 tempFile;                /* zFilename is a temporary file */
  u8 readOnly;                /* True for a read-only database */
  u8 needSync;                /* True if an fsync() is needed on the journal */
  u8 dirtyCache;              /* True if cached pages have changed */
  u8 alwaysRollback;          /* Disable DontRollback() for all pages */
  u8 memDb;                   /* True to inhibit all file I/O */
  u8 setMaster;               /* True if a m-j name has been written to jrnl */
  u8 doNotSync;               /* Boolean. While true, do not spill the cache */
  u8 exclusiveMode;           /* Boolean. True if locking_mode==EXCLUSIVE */
  u8 journalMode;             /* On of the PAGER_JOURNALMODE_* values */
  u8 dbModified;              /* True if there are any changes to the Db */
  u8 changeCountDone;         /* Set after incrementing the change-counter */
  u8 dbSizeValid;             /* Set when dbSize is correct */
  u32 vfsFlags;               /* Flags for sqlite3_vfs.xOpen() */
  int errCode;                /* One of several kinds of errors */
  Pgno dbSize;                /* Number of pages in the file */
  Pgno origDbSize;            /* dbSize before the current change */
  Pgno stmtSize;              /* Size of database (in pages) at stmt_begin() */
  int nRec;                   /* Number of pages written to the journal */
  u32 cksumInit;              /* Quasi-random value added to every checksum */
  int stmtNRec;               /* Number of records in stmt subjournal */
  int nExtra;                 /* Add this many bytes to each in-memory page */
  int pageSize;               /* Number of bytes in a page */
  int nPage;                  /* Total number of in-memory pages */
  int mxPage;                 /* Maximum number of pages to hold in cache */
  Pgno mxPgno;                /* Maximum allowed size of the database */
  Bitvec *pInJournal;         /* One bit for each page in the database file */
  Bitvec *pInStmt;            /* One bit for each page in the database */
  Bitvec *pAlwaysRollback;    /* One bit for each page marked always-rollback */
  char *zFilename;            /* Name of the database file */
  char *zJournal;             /* Name of the journal file */
  char *zDirectory;           /* Directory hold database and journal files */
  sqlite3_file *fd, *jfd;     /* File descriptors for database and journal */
  sqlite3_file *stfd;         /* File descriptor for the statement subjournal*/
  int (*xBusyHandler)(void*); /* Function to call when busy */
  void *pBusyHandlerArg;      /* Context argument for xBusyHandler */
  i64 journalOff;             /* Current byte offset in the journal file */
  i64 journalHdr;             /* Byte offset to previous journal header */
  i64 stmtHdrOff;             /* First journal header written this statement */
  i64 stmtCksum;              /* cksumInit when statement was started */
  i64 stmtJSize;              /* Size of journal at stmt_begin() */
  u32 sectorSize;             /* Assumed sector size during rollback */
#ifdef SQLITE_TEST
  int nHit, nMiss;            /* Cache hits and missing */
  int nRead, nWrite;          /* Database pages read/written */
#endif
  void (*xReiniter)(DbPage*); /* Call this routine when reloading pages */
#ifdef SQLITE_HAS_CODEC
  void *(*xCodec)(void*,void*,Pgno,int); /* Routine for en/decoding data */
  void *pCodecArg;            /* First argument to xCodec() */
#endif
  char *pTmpSpace;            /* Pager.pageSize bytes of space for tmp use */
  char dbFileVers[16];        /* Changes whenever database file changes */
  i64 journalSizeLimit;       /* Size limit for persistent journal files */
  PCache *pPCache;            /* Pointer to page cache object */
};

/*
** The following global variables hold counters used for
** testing purposes only.  These variables do not exist in
** a non-testing build.  These variables are not thread-safe.
*/
#ifdef SQLITE_TEST
int sqlite3_pager_readdb_count = 0;    /* Number of full pages read from DB */
int sqlite3_pager_writedb_count = 0;   /* Number of full pages written to DB */
int sqlite3_pager_writej_count = 0;    /* Number of pages written to journal */
# define PAGER_INCR(v)  v++
#else
# define PAGER_INCR(v)
#endif



/*
** Journal files begin with the following magic string.  The data
** was obtained from /dev/random.  It is used only as a sanity check.
**
** Since version 2.8.0, the journal format contains additional sanity
** checking information.  If the power fails while the journal is begin
** written, semi-random garbage data might appear in the journal
** file after power is restored.  If an attempt is then made
** to roll the journal back, the database could be corrupted.  The additional
** sanity checking data is an attempt to discover the garbage in the
** journal and ignore it.
**
** The sanity checking information for the new journal format consists
** of a 32-bit checksum on each page of data.  The checksum covers both
** the page number and the pPager->pageSize bytes of data for the page.
** This cksum is initialized to a 32-bit random value that appears in the
** journal file right after the header.  The random initializer is important,
** because garbage data that appears at the end of a journal is likely
** data that was once in other files that have now been deleted.  If the
** garbage data came from an obsolete journal file, the checksums might
** be correct.  But by initializing the checksum to random value which
** is different for every journal, we minimize that risk.
*/
static const unsigned char aJournalMagic[] = {
  0xd9, 0xd5, 0x05, 0xf9, 0x20, 0xa1, 0x63, 0xd7,
};

/*
** The size of the header and of each page in the journal is determined
** by the following macros.
*/
#define JOURNAL_PG_SZ(pPager)  ((pPager->pageSize) + 8)

/*
** The journal header size for this pager. In the future, this could be
** set to some value read from the disk controller. The important
** characteristic is that it is the same size as a disk sector.
*/
#define JOURNAL_HDR_SZ(pPager) (pPager->sectorSize)

/*
** The macro MEMDB is true if we are dealing with an in-memory database.
** We do this as a macro so that if the SQLITE_OMIT_MEMORYDB macro is set,
** the value of MEMDB will be a constant and the compiler will optimize
** out code that would never execute.
*/
#ifdef SQLITE_OMIT_MEMORYDB
# define MEMDB 0
#else
# define MEMDB pPager->memDb
#endif

/*
** Page number PAGER_MJ_PGNO is never used in an SQLite database (it is
** reserved for working around a windows/posix incompatibility). It is
** used in the journal to signify that the remainder of the journal file 
** is devoted to storing a master journal name - there are no more pages to
** roll back. See comments for function writeMasterJournal() for details.
*/
/* #define PAGER_MJ_PGNO(x) (PENDING_BYTE/((x)->pageSize)) */
#define PAGER_MJ_PGNO(x) ((Pgno)((PENDING_BYTE/((x)->pageSize))+1))

/*
** The maximum legal page number is (2^31 - 1).
*/
#define PAGER_MAX_PGNO 2147483647

/*
** Return true if page *pPg has already been written to the statement
** journal (or statement snapshot has been created, if *pPg is part
** of an in-memory database).
*/
static int pageInStatement(PgHdr *pPg){
  Pager *pPager = pPg->pPager;
  return sqlite3BitvecTest(pPager->pInStmt, pPg->pgno);
}

static int pageInJournal(PgHdr *pPg){
  return sqlite3BitvecTest(pPg->pPager->pInJournal, pPg->pgno);
}

/*
** Read a 32-bit integer from the given file descriptor.  Store the integer
** that is read in *pRes.  Return SQLITE_OK if everything worked, or an
** error code is something goes wrong.
**
** All values are stored on disk as big-endian.
*/
static int read32bits(sqlite3_file *fd, i64 offset, u32 *pRes){
  unsigned char ac[4];
  int rc = sqlite3OsRead(fd, ac, sizeof(ac), offset);
  if( rc==SQLITE_OK ){
    *pRes = sqlite3Get4byte(ac);
  }
  return rc;
}

/*
** Write a 32-bit integer into a string buffer in big-endian byte order.
*/
#define put32bits(A,B)  sqlite3Put4byte((u8*)A,B)

/*
** Write a 32-bit integer into the given file descriptor.  Return SQLITE_OK
** on success or an error code is something goes wrong.
*/
static int write32bits(sqlite3_file *fd, i64 offset, u32 val){
  char ac[4];
  put32bits(ac, val);
  return sqlite3OsWrite(fd, ac, 4, offset);
}

/*
** If file pFd is open, call sqlite3OsUnlock() on it.
*/
static int osUnlock(sqlite3_file *pFd, int eLock){
  if( !pFd->pMethods ){
    return SQLITE_OK;
  }
  return sqlite3OsUnlock(pFd, eLock);
}

/*
** This function determines whether or not the atomic-write optimization
** can be used with this pager. The optimization can be used if:
**
**  (a) the value returned by OsDeviceCharacteristics() indicates that
**      a database page may be written atomically, and
**  (b) the value returned by OsSectorSize() is less than or equal
**      to the page size.
**
** If the optimization cannot be used, 0 is returned. If it can be used,
** then the value returned is the size of the journal file when it
** contains rollback data for exactly one page.
*/
#ifdef SQLITE_ENABLE_ATOMIC_WRITE
static int jrnlBufferSize(Pager *pPager){
  int dc;           /* Device characteristics */
  int nSector;      /* Sector size */
  int szPage;        /* Page size */
  sqlite3_file *fd = pPager->fd;

  if( fd->pMethods ){
    dc = sqlite3OsDeviceCharacteristics(fd);
    nSector = sqlite3OsSectorSize(fd);
    szPage = pPager->pageSize;
  }

  assert(SQLITE_IOCAP_ATOMIC512==(512>>8));
  assert(SQLITE_IOCAP_ATOMIC64K==(65536>>8));

  if( !fd->pMethods || 
       (dc & (SQLITE_IOCAP_ATOMIC|(szPage>>8)) && nSector<=szPage) ){
    return JOURNAL_HDR_SZ(pPager) + JOURNAL_PG_SZ(pPager);
  }
  return 0;
}
#endif

/*
** This function should be called when an error occurs within the pager
** code. The first argument is a pointer to the pager structure, the
** second the error-code about to be returned by a pager API function. 
** The value returned is a copy of the second argument to this function. 
**
** If the second argument is SQLITE_IOERR, SQLITE_CORRUPT, or SQLITE_FULL
** the error becomes persistent. Until the persisten error is cleared,
** subsequent API calls on this Pager will immediately return the same 
** error code.
**
** A persistent error indicates that the contents of the pager-cache 
** cannot be trusted. This state can be cleared by completely discarding 
** the contents of the pager-cache. If a transaction was active when
** the persistent error occured, then the rollback journal may need
** to be replayed.
*/
static void pager_unlock(Pager *pPager);
static int pager_error(Pager *pPager, int rc){
  int rc2 = rc & 0xff;
  assert(
       pPager->errCode==SQLITE_FULL ||
       pPager->errCode==SQLITE_OK ||
       (pPager->errCode & 0xff)==SQLITE_IOERR
  );
  if(
    rc2==SQLITE_FULL ||
    rc2==SQLITE_IOERR ||
    rc2==SQLITE_CORRUPT
  ){
    pPager->errCode = rc;
    if( pPager->state==PAGER_UNLOCK 
     && sqlite3PcacheRefCount(pPager->pPCache)==0 
    ){
      /* If the pager is already unlocked, call pager_unlock() now to
      ** clear the error state and ensure that the pager-cache is 
      ** completely empty.
      */
      pager_unlock(pPager);
    }
  }
  return rc;
}

/*
** If SQLITE_CHECK_PAGES is defined then we do some sanity checking
** on the cache using a hash function.  This is used for testing
** and debugging only.
*/
#ifdef SQLITE_CHECK_PAGES
/*
** Return a 32-bit hash of the page data for pPage.
*/
static u32 pager_datahash(int nByte, unsigned char *pData){
  u32 hash = 0;
  int i;
  for(i=0; i<nByte; i++){
    hash = (hash*1039) + pData[i];
  }
  return hash;
}
static u32 pager_pagehash(PgHdr *pPage){
  return pager_datahash(pPage->pPager->pageSize, (unsigned char *)pPage->pData);
}
static void pager_set_pagehash(PgHdr *pPage){
  pPage->pageHash = pager_pagehash(pPage);
}

/*
** The CHECK_PAGE macro takes a PgHdr* as an argument. If SQLITE_CHECK_PAGES
** is defined, and NDEBUG is not defined, an assert() statement checks
** that the page is either dirty or still matches the calculated page-hash.
*/
#define CHECK_PAGE(x) checkPage(x)
static void checkPage(PgHdr *pPg){
  Pager *pPager = pPg->pPager;
  assert( !pPg->pageHash || pPager->errCode
      || (pPg->flags&PGHDR_DIRTY) || pPg->pageHash==pager_pagehash(pPg) );
}

#else
#define pager_datahash(X,Y)  0
#define pager_pagehash(X)  0
#define CHECK_PAGE(x)
#endif  /* SQLITE_CHECK_PAGES */

/*
** When this is called the journal file for pager pPager must be open.
** The master journal file name is read from the end of the file and 
** written into memory supplied by the caller. 
**
** zMaster must point to a buffer of at least nMaster bytes allocated by
** the caller. This should be sqlite3_vfs.mxPathname+1 (to ensure there is
** enough space to write the master journal name). If the master journal
** name in the journal is longer than nMaster bytes (including a
** nul-terminator), then this is handled as if no master journal name
** were present in the journal.
**
** If no master journal file name is present zMaster[0] is set to 0 and
** SQLITE_OK returned.
*/
static int readMasterJournal(sqlite3_file *pJrnl, char *zMaster, u32 nMaster){
  int rc;
  u32 len;
  i64 szJ;
  u32 cksum;
  u32 u;                   /* Unsigned loop counter */
  unsigned char aMagic[8]; /* A buffer to hold the magic header */

  zMaster[0] = '\0';

  rc = sqlite3OsFileSize(pJrnl, &szJ);
  if( rc!=SQLITE_OK || szJ<16 ) return rc;

  rc = read32bits(pJrnl, szJ-16, &len);
  if( rc!=SQLITE_OK ) return rc;

  if( len>=nMaster ){
    return SQLITE_OK;
  }

  rc = read32bits(pJrnl, szJ-12, &cksum);
  if( rc!=SQLITE_OK ) return rc;

  rc = sqlite3OsRead(pJrnl, aMagic, 8, szJ-8);
  if( rc!=SQLITE_OK || memcmp(aMagic, aJournalMagic, 8) ) return rc;

  rc = sqlite3OsRead(pJrnl, zMaster, len, szJ-16-len);
  if( rc!=SQLITE_OK ){
    return rc;
  }
  zMaster[len] = '\0';

  /* See if the checksum matches the master journal name */
  for(u=0; u<len; u++){
    cksum -= zMaster[u];
   }
  if( cksum ){
    /* If the checksum doesn't add up, then one or more of the disk sectors
    ** containing the master journal filename is corrupted. This means
    ** definitely roll back, so just return SQLITE_OK and report a (nul)
    ** master-journal filename.
    */
    zMaster[0] = '\0';
  }
   
  return SQLITE_OK;
}

/*
** Seek the journal file descriptor to the next sector boundary where a
** journal header may be read or written. Pager.journalOff is updated with
** the new seek offset.
**
** i.e for a sector size of 512:
**
** Input Offset              Output Offset
** ---------------------------------------
** 0                         0
** 512                       512
** 100                       512
** 2000                      2048
** 
*/
static void seekJournalHdr(Pager *pPager){
  i64 offset = 0;
  i64 c = pPager->journalOff;
  if( c ){
    offset = ((c-1)/JOURNAL_HDR_SZ(pPager) + 1) * JOURNAL_HDR_SZ(pPager);
  }
  assert( offset%JOURNAL_HDR_SZ(pPager)==0 );
  assert( offset>=c );
  assert( (offset-c)<JOURNAL_HDR_SZ(pPager) );
  pPager->journalOff = offset;
}

/*
** Write zeros over the header of the journal file.  This has the
** effect of invalidating the journal file and committing the
** transaction.
*/
static int zeroJournalHdr(Pager *pPager, int doTruncate){
  int rc = SQLITE_OK;
  static const char zeroHdr[28] = {0};

  if( pPager->journalOff ){
    i64 iLimit = pPager->journalSizeLimit;

    IOTRACE(("JZEROHDR %p\n", pPager))
    if( doTruncate || iLimit==0 ){
      rc = sqlite3OsTruncate(pPager->jfd, 0);
    }else{
      rc = sqlite3OsWrite(pPager->jfd, zeroHdr, sizeof(zeroHdr), 0);
    }
    if( rc==SQLITE_OK && !pPager->noSync ){
      rc = sqlite3OsSync(pPager->jfd, SQLITE_SYNC_DATAONLY|pPager->sync_flags);
    }

    /* At this point the transaction is committed but the write lock 
    ** is still held on the file. If there is a size limit configured for 
    ** the persistent journal and the journal file currently consumes more
    ** space than that limit allows for, truncate it now. There is no need
    ** to sync the file following this operation.
    */
    if( rc==SQLITE_OK && iLimit>0 ){
      i64 sz;
      rc = sqlite3OsFileSize(pPager->jfd, &sz);
      if( rc==SQLITE_OK && sz>iLimit ){
        rc = sqlite3OsTruncate(pPager->jfd, iLimit);
      }
    }
  }
  return rc;
}

/*
** The journal file must be open when this routine is called. A journal
** header (JOURNAL_HDR_SZ bytes) is written into the journal file at the
** current location.
**
** The format for the journal header is as follows:
** - 8 bytes: Magic identifying journal format.
** - 4 bytes: Number of records in journal, or -1 no-sync mode is on.
** - 4 bytes: Random number used for page hash.
** - 4 bytes: Initial database page count.
** - 4 bytes: Sector size used by the process that wrote this journal.
** - 4 bytes: Database page size.
** 
** Followed by (JOURNAL_HDR_SZ - 28) bytes of unused space.
*/
static int writeJournalHdr(Pager *pPager){
  int rc = SQLITE_OK;
  char *zHeader = pPager->pTmpSpace;
  u32 nHeader = pPager->pageSize;
  u32 nWrite;

  if( nHeader>JOURNAL_HDR_SZ(pPager) ){
    nHeader = JOURNAL_HDR_SZ(pPager);
  }

  if( pPager->stmtHdrOff==0 ){
    pPager->stmtHdrOff = pPager->journalOff;
  }

  seekJournalHdr(pPager);
  pPager->journalHdr = pPager->journalOff;

  memcpy(zHeader, aJournalMagic, sizeof(aJournalMagic));

  /* 
  ** Write the nRec Field - the number of page records that follow this
  ** journal header. Normally, zero is written to this value at this time.
  ** After the records are added to the journal (and the journal synced, 
  ** if in full-sync mode), the zero is overwritten with the true number
  ** of records (see syncJournal()).
  **
  ** A faster alternative is to write 0xFFFFFFFF to the nRec field. When
  ** reading the journal this value tells SQLite to assume that the
  ** rest of the journal file contains valid page records. This assumption
  ** is dangerous, as if a failure occured whilst writing to the journal
  ** file it may contain some garbage data. There are two scenarios
  ** where this risk can be ignored:
  **
  **   * When the pager is in no-sync mode. Corruption can follow a
  **     power failure in this case anyway.
  **
  **   * When the SQLITE_IOCAP_SAFE_APPEND flag is set. This guarantees
  **     that garbage data is never appended to the journal file.
  */
  assert(pPager->fd->pMethods||pPager->noSync);
  if( (pPager->noSync) || (pPager->journalMode==PAGER_JOURNALMODE_MEMORY)
   || (sqlite3OsDeviceCharacteristics(pPager->fd)&SQLITE_IOCAP_SAFE_APPEND) 
  ){
    put32bits(&zHeader[sizeof(aJournalMagic)], 0xffffffff);
  }else{
    put32bits(&zHeader[sizeof(aJournalMagic)], 0);
  }

  /* The random check-hash initialiser */ 
  sqlite3_randomness(sizeof(pPager->cksumInit), &pPager->cksumInit);
  put32bits(&zHeader[sizeof(aJournalMagic)+4], pPager->cksumInit);
  /* The initial database size */
  put32bits(&zHeader[sizeof(aJournalMagic)+8], pPager->dbSize);
  /* The assumed sector size for this process */
  put32bits(&zHeader[sizeof(aJournalMagic)+12], pPager->sectorSize);
  if( pPager->journalHdr==0 ){
    /* The page size */
    put32bits(&zHeader[sizeof(aJournalMagic)+16], pPager->pageSize);
  }

  for(nWrite=0; rc==SQLITE_OK&&nWrite<JOURNAL_HDR_SZ(pPager); nWrite+=nHeader){
    IOTRACE(("JHDR %p %lld %d\n", pPager, pPager->journalHdr, nHeader))
    rc = sqlite3OsWrite(pPager->jfd, zHeader, nHeader, pPager->journalOff);
    pPager->journalOff += nHeader;
  }

  return rc;
}

/*
** The journal file must be open when this is called. A journal header file
** (JOURNAL_HDR_SZ bytes) is read from the current location in the journal
** file. See comments above function writeJournalHdr() for a description of
** the journal header format.
**
** If the header is read successfully, *nRec is set to the number of
** page records following this header and *dbSize is set to the size of the
** database before the transaction began, in pages. Also, pPager->cksumInit
** is set to the value read from the journal header. SQLITE_OK is returned
** in this case.
**
** If the journal header file appears to be corrupted, SQLITE_DONE is
** returned and *nRec and *dbSize are not set.  If JOURNAL_HDR_SZ bytes
** cannot be read from the journal file an error code is returned.
*/
static int readJournalHdr(
  Pager *pPager, 
  i64 journalSize,
  u32 *pNRec, 
  u32 *pDbSize
){
  int rc;
  unsigned char aMagic[8]; /* A buffer to hold the magic header */
  i64 jrnlOff;
  int iPageSize;

  seekJournalHdr(pPager);
  if( pPager->journalOff+JOURNAL_HDR_SZ(pPager) > journalSize ){
    return SQLITE_DONE;
  }
  jrnlOff = pPager->journalOff;

  rc = sqlite3OsRead(pPager->jfd, aMagic, sizeof(aMagic), jrnlOff);
  if( rc ) return rc;
  jrnlOff += sizeof(aMagic);

  if( memcmp(aMagic, aJournalMagic, sizeof(aMagic))!=0 ){
    return SQLITE_DONE;
  }

  rc = read32bits(pPager->jfd, jrnlOff, pNRec);
  if( rc ) return rc;

  rc = read32bits(pPager->jfd, jrnlOff+4, &pPager->cksumInit);
  if( rc ) return rc;

  rc = read32bits(pPager->jfd, jrnlOff+8, pDbSize);
  if( rc ) return rc;

  rc = read32bits(pPager->jfd, jrnlOff+16, (u32 *)&iPageSize);
  if( rc==SQLITE_OK 
   && iPageSize>=512 
   && iPageSize<=SQLITE_MAX_PAGE_SIZE 
   && ((iPageSize-1)&iPageSize)==0 
  ){
    u16 pagesize = iPageSize;
    rc = sqlite3PagerSetPagesize(pPager, &pagesize);
  }
  if( rc ) return rc;

  /* Update the assumed sector-size to match the value used by 
  ** the process that created this journal. If this journal was
  ** created by a process other than this one, then this routine
  ** is being called from within pager_playback(). The local value
  ** of Pager.sectorSize is restored at the end of that routine.
  */
  rc = read32bits(pPager->jfd, jrnlOff+12, &pPager->sectorSize);
  if( rc ) return rc;
  if( (pPager->sectorSize & (pPager->sectorSize-1))!=0
         || pPager->sectorSize>0x1000000 ){
    return SQLITE_DONE;
  }

  pPager->journalOff += JOURNAL_HDR_SZ(pPager);
  return SQLITE_OK;
}


/*
** Write the supplied master journal name into the journal file for pager
** pPager at the current location. The master journal name must be the last
** thing written to a journal file. If the pager is in full-sync mode, the
** journal file descriptor is advanced to the next sector boundary before
** anything is written. The format is:
**
** + 4 bytes: PAGER_MJ_PGNO.
** + N bytes: length of master journal name.
** + 4 bytes: N
** + 4 bytes: Master journal name checksum.
** + 8 bytes: aJournalMagic[].
**
** The master journal page checksum is the sum of the bytes in the master
** journal name.
**
** If zMaster is a NULL pointer (occurs for a single database transaction), 
** this call is a no-op.
*/
static int writeMasterJournal(Pager *pPager, const char *zMaster){
  int rc;
  int len; 
  int i; 
  i64 jrnlOff;
  i64 jrnlSize;
  u32 cksum = 0;
  char zBuf[sizeof(aJournalMagic)+2*4];

  if( !zMaster || pPager->setMaster ) return SQLITE_OK;
  if( pPager->journalMode==PAGER_JOURNALMODE_MEMORY ) return SQLITE_OK;
  pPager->setMaster = 1;

  len = strlen(zMaster);
  for(i=0; i<len; i++){
    cksum += zMaster[i];
  }

  /* If in full-sync mode, advance to the next disk sector before writing
  ** the master journal name. This is in case the previous page written to
  ** the journal has already been synced.
  */
  if( pPager->fullSync ){
    seekJournalHdr(pPager);
  }
  jrnlOff = pPager->journalOff;
  pPager->journalOff += (len+20);

  rc = write32bits(pPager->jfd, jrnlOff, PAGER_MJ_PGNO(pPager));
  if( rc!=SQLITE_OK ) return rc;
  jrnlOff += 4;

  rc = sqlite3OsWrite(pPager->jfd, zMaster, len, jrnlOff);
  if( rc!=SQLITE_OK ) return rc;
  jrnlOff += len;

  put32bits(zBuf, len);
  put32bits(&zBuf[4], cksum);
  memcpy(&zBuf[8], aJournalMagic, sizeof(aJournalMagic));
  rc = sqlite3OsWrite(pPager->jfd, zBuf, 8+sizeof(aJournalMagic), jrnlOff);
  jrnlOff += 8+sizeof(aJournalMagic);
  pPager->needSync = !pPager->noSync;

  /* If the pager is in peristent-journal mode, then the physical 
  ** journal-file may extend past the end of the master-journal name
  ** and 8 bytes of magic data just written to the file. This is 
  ** dangerous because the code to rollback a hot-journal file
  ** will not be able to find the master-journal name to determine 
  ** whether or not the journal is hot. 
  **
  ** Easiest thing to do in this scenario is to truncate the journal 
  ** file to the required size.
  */ 
  if( (rc==SQLITE_OK)
   && (rc = sqlite3OsFileSize(pPager->jfd, &jrnlSize))==SQLITE_OK
   && jrnlSize>jrnlOff
  ){
    rc = sqlite3OsTruncate(pPager->jfd, jrnlOff);
  }
  return rc;
}

/*
** Find a page in the hash table given its page number.  Return
** a pointer to the page or NULL if not found.
*/
static PgHdr *pager_lookup(Pager *pPager, Pgno pgno){
  PgHdr *p;
  sqlite3PcacheFetch(pPager->pPCache, pgno, 0, &p);
  return p;
}

/*
** Clear the in-memory cache.  This routine
** sets the state of the pager back to what it was when it was first
** opened.  Any outstanding pages are invalidated and subsequent attempts
** to access those pages will likely result in a coredump.
*/
static void pager_reset(Pager *pPager){
  if( pPager->errCode ) return;
  sqlite3PcacheClear(pPager->pPCache);
}

/*
** Unlock the database file. 
**
** If the pager is currently in error state, discard the contents of 
** the cache and reset the Pager structure internal state. If there is
** an open journal-file, then the next time a shared-lock is obtained
** on the pager file (by this or any other process), it will be
** treated as a hot-journal and rolled back.
*/
static void pager_unlock(Pager *pPager){
  if( !pPager->exclusiveMode ){
    int rc = osUnlock(pPager->fd, NO_LOCK);
    if( rc ) pPager->errCode = rc;
    pPager->dbSizeValid = 0;
    IOTRACE(("UNLOCK %p\n", pPager))

    /* Always close the journal file when dropping the database lock.
    ** Otherwise, another connection with journal_mode=delete might
    ** delete the file out from under us.
    */
    if( pPager->journalOpen ){
      sqlite3OsClose(pPager->jfd);
      pPager->journalOpen = 0;
      sqlite3BitvecDestroy(pPager->pInJournal);
      pPager->pInJournal = 0;
      sqlite3BitvecDestroy(pPager->pAlwaysRollback);
      pPager->pAlwaysRollback = 0;
    }

    /* If Pager.errCode is set, the contents of the pager cache cannot be
    ** trusted. Now that the pager file is unlocked, the contents of the
    ** cache can be discarded and the error code safely cleared.
    */
    if( pPager->errCode ){
      if( rc==SQLITE_OK ) pPager->errCode = SQLITE_OK;
      pager_reset(pPager);
      if( pPager->stmtOpen ){
        sqlite3OsClose(pPager->stfd);
        sqlite3BitvecDestroy(pPager->pInStmt);
        pPager->pInStmt = 0;
      }
      pPager->stmtOpen = 0;
      pPager->stmtInUse = 0;
      pPager->journalOff = 0;
      pPager->journalStarted = 0;
      pPager->stmtAutoopen = 0;
      pPager->origDbSize = 0;
    }

    pPager->state = PAGER_UNLOCK;
    pPager->changeCountDone = 0;
  }
}

/*
** Execute a rollback if a transaction is active and unlock the 
** database file. If the pager has already entered the error state, 
** do not attempt the rollback.
*/
static void pagerUnlockAndRollback(Pager *p){
  if( p->errCode==SQLITE_OK && p->state>=PAGER_RESERVED ){
    sqlite3BeginBenignMalloc();
    sqlite3PagerRollback(p);
    sqlite3EndBenignMalloc();
  }
  pager_unlock(p);
}

/*
** This routine ends a transaction.  A transaction is ended by either
** a COMMIT or a ROLLBACK.
**
** When this routine is called, the pager has the journal file open and
** a RESERVED or EXCLUSIVE lock on the database.  This routine will release
** the database lock and acquires a SHARED lock in its place if that is
** the appropriate thing to do.  Release locks usually is appropriate,
** unless we are in exclusive access mode or unless this is a 
** COMMIT AND BEGIN or ROLLBACK AND BEGIN operation.
**
** The journal file is either deleted or truncated.
**
** TODO: Consider keeping the journal file open for temporary databases.
** This might give a performance improvement on windows where opening
** a file is an expensive operation.
*/
static int pager_end_transaction(Pager *pPager, int hasMaster){
  int rc = SQLITE_OK;
  int rc2 = SQLITE_OK;
  if( pPager->state<PAGER_RESERVED ){
    return SQLITE_OK;
  }
  sqlite3PagerStmtCommit(pPager);
  if( pPager->stmtOpen && !pPager->exclusiveMode ){
    sqlite3OsClose(pPager->stfd);
    pPager->stmtOpen = 0;
  }
  if( pPager->journalOpen ){
    if( pPager->journalMode==PAGER_JOURNALMODE_MEMORY ){
      int isMemoryJournal = sqlite3IsMemJournal(pPager->jfd);
      sqlite3OsClose(pPager->jfd);
      pPager->journalOpen = 0;
      if( !isMemoryJournal ){
        rc = sqlite3OsDelete(pPager->pVfs, pPager->zJournal, 0);
      }
    }else if( pPager->journalMode==PAGER_JOURNALMODE_TRUNCATE
         && (rc = sqlite3OsTruncate(pPager->jfd, 0))==SQLITE_OK ){
      pPager->journalOff = 0;
      pPager->journalStarted = 0;
    }else if( pPager->exclusiveMode 
     || pPager->journalMode==PAGER_JOURNALMODE_PERSIST
    ){
      rc = zeroJournalHdr(pPager, hasMaster);
      pager_error(pPager, rc);
      pPager->journalOff = 0;
      pPager->journalStarted = 0;
    }else{
      assert( pPager->journalMode==PAGER_JOURNALMODE_DELETE || rc );
      sqlite3OsClose(pPager->jfd);
      pPager->journalOpen = 0;
      if( rc==SQLITE_OK && !pPager->tempFile ){
        rc = sqlite3OsDelete(pPager->pVfs, pPager->zJournal, 0);
      }
    }
    sqlite3BitvecDestroy(pPager->pInJournal);
    pPager->pInJournal = 0;
    sqlite3BitvecDestroy(pPager->pAlwaysRollback);
    pPager->pAlwaysRollback = 0;
#ifdef SQLITE_CHECK_PAGES
    sqlite3PcacheIterateDirty(pPager->pPCache, pager_set_pagehash);
#endif
    sqlite3PcacheCleanAll(pPager->pPCache);
    pPager->dirtyCache = 0;
    pPager->nRec = 0;
  }else{
    assert( pPager->pInJournal==0 );
  }

  if( !pPager->exclusiveMode ){
    rc2 = osUnlock(pPager->fd, SHARED_LOCK);
    pPager->state = PAGER_SHARED;
  }else if( pPager->state==PAGER_SYNCED ){
    pPager->state = PAGER_EXCLUSIVE;
  }
  pPager->origDbSize = 0;
  pPager->setMaster = 0;
  pPager->needSync = 0;
  /* lruListSetFirstSynced(pPager); */
  if( !MEMDB ){
    pPager->dbSizeValid = 0;
  }
  pPager->dbModified = 0;

  return (rc==SQLITE_OK?rc2:rc);
}

/*
** Compute and return a checksum for the page of data.
**
** This is not a real checksum.  It is really just the sum of the 
** random initial value and the page number.  We experimented with
** a checksum of the entire data, but that was found to be too slow.
**
** Note that the page number is stored at the beginning of data and
** the checksum is stored at the end.  This is important.  If journal
** corruption occurs due to a power failure, the most likely scenario
** is that one end or the other of the record will be changed.  It is
** much less likely that the two ends of the journal record will be
** correct and the middle be corrupt.  Thus, this "checksum" scheme,
** though fast and simple, catches the mostly likely kind of corruption.
**
** FIX ME:  Consider adding every 200th (or so) byte of the data to the
** checksum.  That way if a single page spans 3 or more disk sectors and
** only the middle sector is corrupt, we will still have a reasonable
** chance of failing the checksum and thus detecting the problem.
*/
static u32 pager_cksum(Pager *pPager, const u8 *aData){
  u32 cksum = pPager->cksumInit;
  int i = pPager->pageSize-200;
  while( i>0 ){
    cksum += aData[i];
    i -= 200;
  }
  return cksum;
}

/*
** Read a single page from the journal file opened on file descriptor
** jfd.  Playback this one page.
**
** The isMainJrnl flag is true if this is the main rollback journal and
** false for the statement journal.  The main rollback journal uses
** checksums - the statement journal does not.
*/
static int pager_playback_one_page(
  Pager *pPager,       /* The pager being played back */
  sqlite3_file *jfd,   /* The file that is the journal being rolled back */
  i64 offset,          /* Offset of the page within the journal */
  int isMainJrnl       /* True for main rollback journal. False for Stmt jrnl */
){
  int rc;
  PgHdr *pPg;                   /* An existing page in the cache */
  Pgno pgno;                    /* The page number of a page in journal */
  u32 cksum;                    /* Checksum used for sanity checking */
  u8 *aData = (u8 *)pPager->pTmpSpace;   /* Temp storage for a page */

  /* isMainJrnl should be true for the main journal and false for
  ** statement journals.  Verify that this is always the case
  */
  assert( jfd == (isMainJrnl ? pPager->jfd : pPager->stfd) );
  assert( aData );

  rc = read32bits(jfd, offset, &pgno);
  if( rc!=SQLITE_OK ) return rc;
  rc = sqlite3OsRead(jfd, aData, pPager->pageSize, offset+4);
  if( rc!=SQLITE_OK ) return rc;
  pPager->journalOff += pPager->pageSize + 4;

  /* Sanity checking on the page.  This is more important that I originally
  ** thought.  If a power failure occurs while the journal is being written,
  ** it could cause invalid data to be written into the journal.  We need to
  ** detect this invalid data (with high probability) and ignore it.
  */
  if( pgno==0 || pgno==PAGER_MJ_PGNO(pPager) ){
    return SQLITE_DONE;
  }
  if( pgno>(unsigned)pPager->dbSize ){
    return SQLITE_OK;
  }
  if( isMainJrnl ){
    rc = read32bits(jfd, offset+pPager->pageSize+4, &cksum);
    if( rc ) return rc;
    pPager->journalOff += 4;
    if( pager_cksum(pPager, aData)!=cksum ){
      return SQLITE_DONE;
    }
  }

  assert( pPager->state==PAGER_RESERVED || pPager->state>=PAGER_EXCLUSIVE );

  /* If the pager is in RESERVED state, then there must be a copy of this
  ** page in the pager cache. In this case just update the pager cache,
  ** not the database file. The page is left marked dirty in this case.
  **
  ** An exception to the above rule: If the database is in no-sync mode
  ** and a page is moved during an incremental vacuum then the page may
  ** not be in the pager cache. Later: if a malloc() or IO error occurs
  ** during a Movepage() call, then the page may not be in the cache
  ** either. So the condition described in the above paragraph is not
  ** assert()able.
  **
  ** If in EXCLUSIVE state, then we update the pager cache if it exists
  ** and the main file. The page is then marked not dirty.
  **
  ** Ticket #1171:  The statement journal might contain page content that is
  ** different from the page content at the start of the transaction.
  ** This occurs when a page is changed prior to the start of a statement
  ** then changed again within the statement.  When rolling back such a
  ** statement we must not write to the original database unless we know
  ** for certain that original page contents are synced into the main rollback
  ** journal.  Otherwise, a power loss might leave modified data in the
  ** database file without an entry in the rollback journal that can
  ** restore the database to its original form.  Two conditions must be
  ** met before writing to the database files. (1) the database must be
  ** locked.  (2) we know that the original page content is fully synced
  ** in the main journal either because the page is not in cache or else
  ** the page is marked as needSync==0.
  **
  ** 2008-04-14:  When attempting to vacuum a corrupt database file, it
  ** is possible to fail a statement on a database that does not yet exist.
  ** Do not attempt to write if database file has never been opened.
  */
  pPg = pager_lookup(pPager, pgno);
  PAGERTRACE4("PLAYBACK %d page %d hash(%08x)\n",
               PAGERID(pPager), pgno, pager_datahash(pPager->pageSize, aData));
  if( (pPager->state>=PAGER_EXCLUSIVE)
   && (pPg==0 || 0==(pPg->flags&PGHDR_NEED_SYNC))
   && (pPager->fd->pMethods)
  ){
    i64 ofst = (pgno-1)*(i64)pPager->pageSize;
    rc = sqlite3OsWrite(pPager->fd, aData, pPager->pageSize, ofst);
  }
  if( pPg ){
    /* No page should ever be explicitly rolled back that is in use, except
    ** for page 1 which is held in use in order to keep the lock on the
    ** database active. However such a page may be rolled back as a result
    ** of an internal error resulting in an automatic call to
    ** sqlite3PagerRollback().
    */
    void *pData;
    pData = pPg->pData;
    memcpy(pData, aData, pPager->pageSize);
    if( pPager->xReiniter ){
      pPager->xReiniter(pPg);
    }
    if( isMainJrnl ){
      sqlite3PcacheMakeClean(pPg);
    }
#ifdef SQLITE_CHECK_PAGES
    pPg->pageHash = pager_pagehash(pPg);
#endif
    /* If this was page 1, then restore the value of Pager.dbFileVers.
    ** Do this before any decoding. */
    if( pgno==1 ){
      memcpy(&pPager->dbFileVers, &((u8*)pData)[24],sizeof(pPager->dbFileVers));
    }

    /* Decode the page just read from disk */
    CODEC1(pPager, pData, pPg->pgno, 3);
    sqlite3PcacheRelease(pPg);
  }
  return rc;
}

/*
** Parameter zMaster is the name of a master journal file. A single journal
** file that referred to the master journal file has just been rolled back.
** This routine checks if it is possible to delete the master journal file,
** and does so if it is.
**
** Argument zMaster may point to Pager.pTmpSpace. So that buffer is not 
** available for use within this function.
**
**
** The master journal file contains the names of all child journals.
** To tell if a master journal can be deleted, check to each of the
** children.  If all children are either missing or do not refer to
** a different master journal, then this master journal can be deleted.
*/
static int pager_delmaster(Pager *pPager, const char *zMaster){
  sqlite3_vfs *pVfs = pPager->pVfs;
  int rc;
  int master_open = 0;
  sqlite3_file *pMaster;
  sqlite3_file *pJournal;
  char *zMasterJournal = 0; /* Contents of master journal file */
  i64 nMasterJournal;       /* Size of master journal file */

  /* Open the master journal file exclusively in case some other process
  ** is running this routine also. Not that it makes too much difference.
  */
  pMaster = (sqlite3_file *)sqlite3Malloc(pVfs->szOsFile * 2);
  pJournal = (sqlite3_file *)(((u8 *)pMaster) + pVfs->szOsFile);
  if( !pMaster ){
    rc = SQLITE_NOMEM;
  }else{
    int flags = (SQLITE_OPEN_READONLY|SQLITE_OPEN_MASTER_JOURNAL);
    rc = sqlite3OsOpen(pVfs, zMaster, pMaster, flags, 0);
  }
  if( rc!=SQLITE_OK ) goto delmaster_out;
  master_open = 1;

  rc = sqlite3OsFileSize(pMaster, &nMasterJournal);
  if( rc!=SQLITE_OK ) goto delmaster_out;

  if( nMasterJournal>0 ){
    char *zJournal;
    char *zMasterPtr = 0;
    int nMasterPtr = pPager->pVfs->mxPathname+1;

    /* Load the entire master journal file into space obtained from
    ** sqlite3_malloc() and pointed to by zMasterJournal. 
    */
    zMasterJournal = (char *)sqlite3Malloc(nMasterJournal + nMasterPtr);
    if( !zMasterJournal ){
      rc = SQLITE_NOMEM;
      goto delmaster_out;
    }
    zMasterPtr = &zMasterJournal[nMasterJournal];
    rc = sqlite3OsRead(pMaster, zMasterJournal, nMasterJournal, 0);
    if( rc!=SQLITE_OK ) goto delmaster_out;

    zJournal = zMasterJournal;
    while( (zJournal-zMasterJournal)<nMasterJournal ){
      int exists;
      rc = sqlite3OsAccess(pVfs, zJournal, SQLITE_ACCESS_EXISTS, &exists);
      if( rc!=SQLITE_OK ){
        goto delmaster_out;
      }
      if( exists ){
        /* One of the journals pointed to by the master journal exists.
        ** Open it and check if it points at the master journal. If
        ** so, return without deleting the master journal file.
        */
        int c;
        int flags = (SQLITE_OPEN_READONLY|SQLITE_OPEN_MAIN_JOURNAL);
        rc = sqlite3OsOpen(pVfs, zJournal, pJournal, flags, 0);
        if( rc!=SQLITE_OK ){
          goto delmaster_out;
        }

        rc = readMasterJournal(pJournal, zMasterPtr, nMasterPtr);
        sqlite3OsClose(pJournal);
        if( rc!=SQLITE_OK ){
          goto delmaster_out;
        }

        c = zMasterPtr[0]!=0 && strcmp(zMasterPtr, zMaster)==0;
        if( c ){
          /* We have a match. Do not delete the master journal file. */
          goto delmaster_out;
        }
      }
      zJournal += (strlen(zJournal)+1);
    }
  }
  
  rc = sqlite3OsDelete(pVfs, zMaster, 0);

delmaster_out:
  if( zMasterJournal ){
    sqlite3_free(zMasterJournal);
  }  
  if( master_open ){
    sqlite3OsClose(pMaster);
  }
  sqlite3_free(pMaster);
  return rc;
}


static void pager_truncate_cache(Pager *pPager);

/*
** Truncate the main file of the given pager to the number of pages
** indicated. Also truncate the cached representation of the file.
**
** Might might be the case that the file on disk is smaller than nPage.
** This can happen, for example, if we are in the middle of a transaction
** which has extended the file size and the new pages are still all held
** in cache, then an INSERT or UPDATE does a statement rollback.  Some
** operating system implementations can get confused if you try to
** truncate a file to some size that is larger than it currently is,
** so detect this case and write a single zero byte to the end of the new
** file instead.
*/
static int pager_truncate(Pager *pPager, Pgno nPage){
  int rc = SQLITE_OK;
  if( pPager->state>=PAGER_EXCLUSIVE && pPager->fd->pMethods ){
    i64 currentSize, newSize;
    rc = sqlite3OsFileSize(pPager->fd, &currentSize);
    newSize = pPager->pageSize*(i64)nPage;
    if( rc==SQLITE_OK && currentSize!=newSize ){
      if( currentSize>newSize ){
        rc = sqlite3OsTruncate(pPager->fd, newSize);
      }else{
        rc = sqlite3OsWrite(pPager->fd, "", 1, newSize-1);
      }
    }
  }
  if( rc==SQLITE_OK ){
    pPager->dbSize = nPage;
    pager_truncate_cache(pPager);
  }
  return rc;
}

/*
** Set the sectorSize for the given pager.
**
** The sector size is at least as big as the sector size reported
** by sqlite3OsSectorSize().  The minimum sector size is 512.
*/
static void setSectorSize(Pager *pPager){
  assert(pPager->fd->pMethods||pPager->tempFile);
  if( !pPager->tempFile ){
    /* Sector size doesn't matter for temporary files. Also, the file
    ** may not have been opened yet, in whcih case the OsSectorSize()
    ** call will segfault.
    */
    pPager->sectorSize = sqlite3OsSectorSize(pPager->fd);
  }
  if( pPager->sectorSize<512 ){
    pPager->sectorSize = 512;
  }
}

/*
** Playback the journal and thus restore the database file to
** the state it was in before we started making changes.  
**
** The journal file format is as follows: 
**
**  (1)  8 byte prefix.  A copy of aJournalMagic[].
**  (2)  4 byte big-endian integer which is the number of valid page records
**       in the journal.  If this value is 0xffffffff, then compute the
**       number of page records from the journal size.
**  (3)  4 byte big-endian integer which is the initial value for the 
**       sanity checksum.
**  (4)  4 byte integer which is the number of pages to truncate the
**       database to during a rollback.
**  (5)  4 byte big-endian integer which is the sector size.  The header
**       is this many bytes in size.
**  (6)  4 byte big-endian integer which is the page case.
**  (7)  4 byte integer which is the number of bytes in the master journal
**       name.  The value may be zero (indicate that there is no master
**       journal.)
**  (8)  N bytes of the master journal name.  The name will be nul-terminated
**       and might be shorter than the value read from (5).  If the first byte
**       of the name is \000 then there is no master journal.  The master
**       journal name is stored in UTF-8.
**  (9)  Zero or more pages instances, each as follows:
**        +  4 byte page number.
**        +  pPager->pageSize bytes of data.
**        +  4 byte checksum
**
** When we speak of the journal header, we mean the first 8 items above.
** Each entry in the journal is an instance of the 9th item.
**
** Call the value from the second bullet "nRec".  nRec is the number of
** valid page entries in the journal.  In most cases, you can compute the
** value of nRec from the size of the journal file.  But if a power
** failure occurred while the journal was being written, it could be the
** case that the size of the journal file had already been increased but
** the extra entries had not yet made it safely to disk.  In such a case,
** the value of nRec computed from the file size would be too large.  For
** that reason, we always use the nRec value in the header.
**
** If the nRec value is 0xffffffff it means that nRec should be computed
** from the file size.  This value is used when the user selects the
** no-sync option for the journal.  A power failure could lead to corruption
** in this case.  But for things like temporary table (which will be
** deleted when the power is restored) we don't care.  
**
** If the file opened as the journal file is not a well-formed
** journal file then all pages up to the first corrupted page are rolled
** back (or no pages if the journal header is corrupted). The journal file
** is then deleted and SQLITE_OK returned, just as if no corruption had
** been encountered.
**
** If an I/O or malloc() error occurs, the journal-file is not deleted
** and an error code is returned.
*/
static int pager_playback(Pager *pPager, int isHot){
  sqlite3_vfs *pVfs = pPager->pVfs;
  i64 szJ;                 /* Size of the journal file in bytes */
  u32 nRec;                /* Number of Records in the journal */
  u32 u;                   /* Unsigned loop counter */
  Pgno mxPg = 0;           /* Size of the original file in pages */
  int rc;                  /* Result code of a subroutine */
  int res = 1;             /* Value returned by sqlite3OsAccess() */
  char *zMaster = 0;       /* Name of master journal file if any */

  /* Figure out how many records are in the journal.  Abort early if
  ** the journal is empty.
  */
  assert( pPager->journalOpen );
  rc = sqlite3OsFileSize(pPager->jfd, &szJ);
  if( rc!=SQLITE_OK || szJ==0 ){
    goto end_playback;
  }

  /* Read the master journal name from the journal, if it is present.
  ** If a master journal file name is specified, but the file is not
  ** present on disk, then the journal is not hot and does not need to be
  ** played back.
  */
  zMaster = pPager->pTmpSpace;
  rc = readMasterJournal(pPager->jfd, zMaster, pPager->pVfs->mxPathname+1);
  if( rc==SQLITE_OK && zMaster[0] ){
    rc = sqlite3OsAccess(pVfs, zMaster, SQLITE_ACCESS_EXISTS, &res);
  }
  zMaster = 0;
  if( rc!=SQLITE_OK || !res ){
    goto end_playback;
  }
  pPager->journalOff = 0;

  /* This loop terminates either when the readJournalHdr() call returns
  ** SQLITE_DONE or an IO error occurs. */
  while( 1 ){

    /* Read the next journal header from the journal file.  If there are
    ** not enough bytes left in the journal file for a complete header, or
    ** it is corrupted, then a process must of failed while writing it.
    ** This indicates nothing more needs to be rolled back.
    */
    rc = readJournalHdr(pPager, szJ, &nRec, &mxPg);
    if( rc!=SQLITE_OK ){ 
      if( rc==SQLITE_DONE ){
        rc = SQLITE_OK;
      }
      goto end_playback;
    }

    /* If nRec is 0xffffffff, then this journal was created by a process
    ** working in no-sync mode. This means that the rest of the journal
    ** file consists of pages, there are no more journal headers. Compute
    ** the value of nRec based on this assumption.
    */
    if( nRec==0xffffffff ){
      assert( pPager->journalOff==JOURNAL_HDR_SZ(pPager) );
      nRec = (szJ - JOURNAL_HDR_SZ(pPager))/JOURNAL_PG_SZ(pPager);
    }

    /* If nRec is 0 and this rollback is of a transaction created by this
    ** process and if this is the final header in the journal, then it means
    ** that this part of the journal was being filled but has not yet been
    ** synced to disk.  Compute the number of pages based on the remaining
    ** size of the file.
    **
    ** The third term of the test was added to fix ticket #2565.
    */
    if( nRec==0 && !isHot &&
        pPager->journalHdr+JOURNAL_HDR_SZ(pPager)==pPager->journalOff ){
      nRec = (szJ - pPager->journalOff) / JOURNAL_PG_SZ(pPager);
    }

    /* If this is the first header read from the journal, truncate the
    ** database file back to its original size.
    */
    if( pPager->journalOff==JOURNAL_HDR_SZ(pPager) ){
      rc = pager_truncate(pPager, mxPg);
      if( rc!=SQLITE_OK ){
        goto end_playback;
      }
    }

    /* Copy original pages out of the journal and back into the database file.
    */
    for(u=0; u<nRec; u++){
      rc = pager_playback_one_page(pPager, pPager->jfd, pPager->journalOff, 1);
      if( rc!=SQLITE_OK ){
        if( rc==SQLITE_DONE ){
          rc = SQLITE_OK;
          pPager->journalOff = szJ;
          break;
        }else{
          /* If we are unable to rollback, then the database is probably
          ** going to end up being corrupt.  It is corrupt to us, anyhow.
          ** Perhaps the next process to come along can fix it....
          */
          rc = SQLITE_CORRUPT_BKPT;
          goto end_playback;
        }
      }
    }
  }
  /*NOTREACHED*/
  assert( 0 );

end_playback:
  if( rc==SQLITE_OK ){
    zMaster = pPager->pTmpSpace;
    rc = readMasterJournal(pPager->jfd, zMaster, pPager->pVfs->mxPathname+1);
  }
  if( rc==SQLITE_OK ){
    rc = pager_end_transaction(pPager, zMaster[0]!='\0');
  }
  if( rc==SQLITE_OK && zMaster[0] && res ){
    /* If there was a master journal and this routine will return success,
    ** see if it is possible to delete the master journal.
    */
    rc = pager_delmaster(pPager, zMaster);
  }

  /* The Pager.sectorSize variable may have been updated while rolling
  ** back a journal created by a process with a different sector size
  ** value. Reset it to the correct value for this process.
  */
  setSectorSize(pPager);
  return rc;
}

/*
** Playback the statement journal.
**
** This is similar to playing back the transaction journal but with
** a few extra twists.
**
**    (1)  The number of pages in the database file at the start of
**         the statement is stored in pPager->stmtSize, not in the
**         journal file itself.
**
**    (2)  In addition to playing back the statement journal, also
**         playback all pages of the transaction journal beginning
**         at offset pPager->stmtJSize.
*/
static int pager_stmt_playback(Pager *pPager){
  i64 szJ;                 /* Size of the full journal */
  i64 hdrOff;
  int nRec;                /* Number of Records */
  int i;                   /* Loop counter */
  int rc;

  szJ = pPager->journalOff;

  /* Set hdrOff to be the offset just after the end of the last journal
  ** page written before the first journal-header for this statement
  ** transaction was written, or the end of the file if no journal
  ** header was written.
  */
  hdrOff = pPager->stmtHdrOff;
  assert( pPager->fullSync || !hdrOff );
  if( !hdrOff ){
    hdrOff = szJ;
  }
  
  /* Truncate the database back to its original size.
  */
  rc = pager_truncate(pPager, pPager->stmtSize);
  assert( pPager->state>=PAGER_SHARED );

  /* Figure out how many records are in the statement journal.
  */
  assert( pPager->stmtInUse && pPager->journalOpen );
  nRec = pPager->stmtNRec;
  
  /* Copy original pages out of the statement journal and back into the
  ** database file.  Note that the statement journal omits checksums from
  ** each record since power-failure recovery is not important to statement
  ** journals.
  */
  for(i=0; i<nRec; i++){
    i64 offset = i*(4+pPager->pageSize);
    rc = pager_playback_one_page(pPager, pPager->stfd, offset, 0);
    assert( rc!=SQLITE_DONE );
    if( rc!=SQLITE_OK ) goto end_stmt_playback;
  }

  /* Now roll some pages back from the transaction journal. Pager.stmtJSize
  ** was the size of the journal file when this statement was started, so
  ** everything after that needs to be rolled back, either into the
  ** database, the memory cache, or both.
  **
  ** If it is not zero, then Pager.stmtHdrOff is the offset to the start
  ** of the first journal header written during this statement transaction.
  */
  pPager->journalOff = pPager->stmtJSize;
  pPager->cksumInit = pPager->stmtCksum;
  while( pPager->journalOff < hdrOff ){
    rc = pager_playback_one_page(pPager, pPager->jfd, pPager->journalOff, 1);
    assert( rc!=SQLITE_DONE );
    if( rc!=SQLITE_OK ) goto end_stmt_playback;
  }

  while( pPager->journalOff < szJ ){
    u32 nJRec;         /* Number of Journal Records */
    u32 dummy;
    rc = readJournalHdr(pPager, szJ, &nJRec, &dummy);
    if( rc!=SQLITE_OK ){
      assert( rc!=SQLITE_DONE );
      goto end_stmt_playback;
    }
    if( nJRec==0 ){
      nJRec = (szJ - pPager->journalOff) / (pPager->pageSize+8);
    }
    for(i=nJRec-1; i>=0 && pPager->journalOff < szJ; i--){
      rc = pager_playback_one_page(pPager, pPager->jfd, pPager->journalOff, 1);
      assert( rc!=SQLITE_DONE );
      if( rc!=SQLITE_OK ) goto end_stmt_playback;
    }
  }

  pPager->journalOff = szJ;
  
end_stmt_playback:
  if( rc==SQLITE_OK) {
    pPager->journalOff = szJ;
    /* pager_reload_cache(pPager); */
  }
  return rc;
}

/*
** Change the maximum number of in-memory pages that are allowed.
*/
void sqlite3PagerSetCachesize(Pager *pPager, int mxPage){
  sqlite3PcacheSetCachesize(pPager->pPCache, mxPage);
}

/*
** Adjust the robustness of the database to damage due to OS crashes
** or power failures by changing the number of syncs()s when writing
** the rollback journal.  There are three levels:
**
**    OFF       sqlite3OsSync() is never called.  This is the default
**              for temporary and transient files.
**
**    NORMAL    The journal is synced once before writes begin on the
**              database.  This is normally adequate protection, but
**              it is theoretically possible, though very unlikely,
**              that an inopertune power failure could leave the journal
**              in a state which would cause damage to the database
**              when it is rolled back.
**
**    FULL      The journal is synced twice before writes begin on the
**              database (with some additional information - the nRec field
**              of the journal header - being written in between the two
**              syncs).  If we assume that writing a
**              single disk sector is atomic, then this mode provides
**              assurance that the journal will not be corrupted to the
**              point of causing damage to the database during rollback.
**
** Numeric values associated with these states are OFF==1, NORMAL=2,
** and FULL=3.
*/
#ifndef SQLITE_OMIT_PAGER_PRAGMAS
void sqlite3PagerSetSafetyLevel(Pager *pPager, int level, int bFullFsync){
  pPager->noSync =  level==1 || pPager->tempFile;
  pPager->fullSync = level==3 && !pPager->tempFile;
  pPager->sync_flags = (bFullFsync?SQLITE_SYNC_FULL:SQLITE_SYNC_NORMAL);
  if( pPager->noSync ) pPager->needSync = 0;
}
#endif

/*
** The following global variable is incremented whenever the library
** attempts to open a temporary file.  This information is used for
** testing and analysis only.  
*/
#ifdef SQLITE_TEST
int sqlite3_opentemp_count = 0;
#endif

/*
** Open a temporary file. 
**
** Write the file descriptor into *fd.  Return SQLITE_OK on success or some
** other error code if we fail. The OS will automatically delete the temporary
** file when it is closed.
*/
static int sqlite3PagerOpentemp(
  Pager *pPager,        /* The pager object */
  sqlite3_file *pFile,  /* Write the file descriptor here */
  int vfsFlags          /* Flags passed through to the VFS */
){
  int rc;

#ifdef SQLITE_TEST
  sqlite3_opentemp_count++;  /* Used for testing and analysis only */
#endif

  vfsFlags |=  SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE |
            SQLITE_OPEN_EXCLUSIVE | SQLITE_OPEN_DELETEONCLOSE;
  rc = sqlite3OsOpen(pPager->pVfs, 0, pFile, vfsFlags, 0);
  assert( rc!=SQLITE_OK || pFile->pMethods );
  return rc;
}

static int pagerStress(void *,PgHdr *);

/*
** Create a new page cache and put a pointer to the page cache in *ppPager.
** The file to be cached need not exist.  The file is not locked until
** the first call to sqlite3PagerGet() and is only held open until the
** last page is released using sqlite3PagerUnref().
**
** If zFilename is NULL then a randomly-named temporary file is created
** and used as the file to be cached.  The file will be deleted
** automatically when it is closed.
**
** If zFilename is ":memory:" then all information is held in cache.
** It is never written to disk.  This can be used to implement an
** in-memory database.
*/
int sqlite3PagerOpen(
  sqlite3_vfs *pVfs,       /* The virtual file system to use */
  Pager **ppPager,         /* Return the Pager structure here */
  const char *zFilename,   /* Name of the database file to open */
  int nExtra,              /* Extra bytes append to each in-memory page */
  int flags,               /* flags controlling this file */
  int vfsFlags             /* flags passed through to sqlite3_vfs.xOpen() */
){
  u8 *pPtr;
  Pager *pPager = 0;
  int rc = SQLITE_OK;
  int i;
  int tempFile = 0;
  int memDb = 0;
  int readOnly = 0;
  int useJournal = (flags & PAGER_OMIT_JOURNAL)==0;
  int noReadlock = (flags & PAGER_NO_READLOCK)!=0;
  int journalFileSize;
  int pcacheSize = sqlite3PcacheSize();
  int szPageDflt = SQLITE_DEFAULT_PAGE_SIZE;
  char *zPathname = 0;
  int nPathname = 0;

  if( sqlite3JournalSize(pVfs)>sqlite3MemJournalSize() ){
    journalFileSize = sqlite3JournalSize(pVfs);
  }else{
    journalFileSize = sqlite3MemJournalSize();
  }

  /* The default return is a NULL pointer */
  *ppPager = 0;

  /* Compute and store the full pathname in an allocated buffer pointed
  ** to by zPathname, length nPathname. Or, if this is a temporary file,
  ** leave both nPathname and zPathname set to 0.
  */
  if( zFilename && zFilename[0] ){
    nPathname = pVfs->mxPathname+1;
    zPathname = sqlite3Malloc(nPathname*2);
    if( zPathname==0 ){
      return SQLITE_NOMEM;
    }
#ifndef SQLITE_OMIT_MEMORYDB
    if( strcmp(zFilename,":memory:")==0 ){
      memDb = 1;
      zPathname[0] = 0;
    }else
#endif
    {
      rc = sqlite3OsFullPathname(pVfs, zFilename, nPathname, zPathname);
    }
    if( rc!=SQLITE_OK ){
      sqlite3_free(zPathname);
      return rc;
    }
    nPathname = strlen(zPathname);
  }

  /* Allocate memory for the pager structure */
  pPager = sqlite3MallocZero(
    sizeof(*pPager) +           /* Pager structure */
    pcacheSize      +           /* PCache object */
    journalFileSize +           /* The journal file structure */ 
    pVfs->szOsFile  +           /* The main db file */
    journalFileSize * 2 +       /* The two journal files */ 
    3*nPathname + 40            /* zFilename, zDirectory, zJournal */
  );
  if( !pPager ){
    sqlite3_free(zPathname);
    return SQLITE_NOMEM;
  }
  pPager->pPCache = (PCache *)&pPager[1];
  pPtr = ((u8 *)&pPager[1]) + pcacheSize;
  pPager->vfsFlags = vfsFlags;
  pPager->fd = (sqlite3_file*)&pPtr[pVfs->szOsFile*0];
  pPager->stfd = (sqlite3_file*)&pPtr[pVfs->szOsFile];
  pPager->jfd = (sqlite3_file*)&pPtr[pVfs->szOsFile+journalFileSize];
  pPager->zFilename = (char*)&pPtr[pVfs->szOsFile+2*journalFileSize];
  pPager->zDirectory = &pPager->zFilename[nPathname+1];
  pPager->zJournal = &pPager->zDirectory[nPathname+1];
  pPager->pVfs = pVfs;
  if( zPathname ){
    memcpy(pPager->zFilename, zPathname, nPathname+1);
    sqlite3_free(zPathname);
  }

  /* Open the pager file.
  */
  if( zFilename && zFilename[0] && !memDb ){
    if( nPathname>(pVfs->mxPathname - (int)sizeof("-journal")) ){
      rc = SQLITE_CANTOPEN;
    }else{
      int fout = 0;
      rc = sqlite3OsOpen(pVfs, pPager->zFilename, pPager->fd,
                         pPager->vfsFlags, &fout);
      readOnly = (fout&SQLITE_OPEN_READONLY);

      /* If the file was successfully opened for read/write access,
      ** choose a default page size in case we have to create the
      ** database file. The default page size is the maximum of:
      **
      **    + SQLITE_DEFAULT_PAGE_SIZE,
      **    + The value returned by sqlite3OsSectorSize()
      **    + The largest page size that can be written atomically.
      */
      if( rc==SQLITE_OK && !readOnly ){
        int iSectorSize = sqlite3OsSectorSize(pPager->fd);
        if( szPageDflt<iSectorSize ){
          szPageDflt = iSectorSize;
        }
#ifdef SQLITE_ENABLE_ATOMIC_WRITE
        {
          int iDc = sqlite3OsDeviceCharacteristics(pPager->fd);
          int ii;
          assert(SQLITE_IOCAP_ATOMIC512==(512>>8));
          assert(SQLITE_IOCAP_ATOMIC64K==(65536>>8));
          assert(SQLITE_MAX_DEFAULT_PAGE_SIZE<=65536);
          for(ii=szPageDflt; ii<=SQLITE_MAX_DEFAULT_PAGE_SIZE; ii=ii*2){
            if( iDc&(SQLITE_IOCAP_ATOMIC|(ii>>8)) ) szPageDflt = ii;
          }
        }
#endif
        if( szPageDflt>SQLITE_MAX_DEFAULT_PAGE_SIZE ){
          szPageDflt = SQLITE_MAX_DEFAULT_PAGE_SIZE;
        }
      }
    }
  }else{
    /* If a temporary file is requested, it is not opened immediately.
    ** In this case we accept the default page size and delay actually
    ** opening the file until the first call to OsWrite().
    **
    ** This branch is also run for an in-memory database. An in-memory
    ** database is the same as a temp-file that is never written out to
    ** disk and uses an in-memory rollback journal.
    */ 
    tempFile = 1;
    pPager->state = PAGER_EXCLUSIVE;
  }

  if( pPager && rc==SQLITE_OK ){
    pPager->pTmpSpace = sqlite3PageMalloc(szPageDflt);
  }

  /* If an error occured in either of the blocks above.
  ** Free the Pager structure and close the file.
  ** Since the pager is not allocated there is no need to set 
  ** any Pager.errMask variables.
  */
  if( !pPager || !pPager->pTmpSpace ){
    sqlite3OsClose(pPager->fd);
    sqlite3_free(pPager);
    return ((rc==SQLITE_OK)?SQLITE_NOMEM:rc);
  }
  nExtra = FORCE_ALIGNMENT(nExtra);
  sqlite3PcacheOpen(szPageDflt, nExtra, !memDb,
                    !memDb?pagerStress:0, (void *)pPager, pPager->pPCache);

  PAGERTRACE3("OPEN %d %s\n", FILEHANDLEID(pPager->fd), pPager->zFilename);
  IOTRACE(("OPEN %p %s\n", pPager, pPager->zFilename))

  /* Fill in Pager.zDirectory[] */
  memcpy(pPager->zDirectory, pPager->zFilename, nPathname+1);
  for(i=strlen(pPager->zDirectory); i>0 && pPager->zDirectory[i-1]!='/'; i--){}
  if( i>0 ) pPager->zDirectory[i-1] = 0;

  /* Fill in Pager.zJournal[] */
  if( zPathname ){
    memcpy(pPager->zJournal, pPager->zFilename, nPathname);
    memcpy(&pPager->zJournal[nPathname], "-journal", 9);
  }else{
    pPager->zJournal = 0;
  }

  /* pPager->journalOpen = 0; */
  pPager->useJournal = useJournal;
  pPager->noReadlock = noReadlock && readOnly;
  /* pPager->stmtOpen = 0; */
  /* pPager->stmtInUse = 0; */
  /* pPager->nRef = 0; */
  pPager->dbSizeValid = memDb;
  pPager->pageSize = szPageDflt;
  /* pPager->stmtSize = 0; */
  /* pPager->stmtJSize = 0; */
  /* pPager->nPage = 0; */
  pPager->mxPage = 100;
  pPager->mxPgno = SQLITE_MAX_PAGE_COUNT;
  /* pPager->state = PAGER_UNLOCK; */
  assert( pPager->state == (tempFile ? PAGER_EXCLUSIVE : PAGER_UNLOCK) );
  /* pPager->errMask = 0; */
  pPager->tempFile = tempFile;
  assert( tempFile==PAGER_LOCKINGMODE_NORMAL 
          || tempFile==PAGER_LOCKINGMODE_EXCLUSIVE );
  assert( PAGER_LOCKINGMODE_EXCLUSIVE==1 );
  pPager->exclusiveMode = tempFile; 
  pPager->memDb = memDb;
  pPager->readOnly = readOnly;
  /* pPager->needSync = 0; */
  pPager->noSync = pPager->tempFile || !useJournal;
  pPager->fullSync = (pPager->noSync?0:1);
  pPager->sync_flags = SQLITE_SYNC_NORMAL;
  /* pPager->pFirst = 0; */
  /* pPager->pFirstSynced = 0; */
  /* pPager->pLast = 0; */
  pPager->nExtra = nExtra;
  pPager->journalSizeLimit = SQLITE_DEFAULT_JOURNAL_SIZE_LIMIT;
  assert(pPager->fd->pMethods||tempFile);
  setSectorSize(pPager);
  if( memDb ){
    pPager->journalMode = PAGER_JOURNALMODE_MEMORY;
  }
  /* pPager->xBusyHandler = 0; */
  /* pPager->pBusyHandlerArg = 0; */
  /* memset(pPager->aHash, 0, sizeof(pPager->aHash)); */
  *ppPager = pPager;
  return SQLITE_OK;
}

/*
** Set the busy handler function.
*/
void sqlite3PagerSetBusyhandler(
  Pager *pPager, 
  int (*xBusyHandler)(void *),
  void *pBusyHandlerArg
){  
  pPager->xBusyHandler = xBusyHandler;
  pPager->pBusyHandlerArg = pBusyHandlerArg;
}

/*
** Set the reinitializer for this pager.  If not NULL, the reinitializer
** is called when the content of a page in cache is restored to its original
** value as a result of a rollback.  The callback gives higher-level code
** an opportunity to restore the EXTRA section to agree with the restored
** page data.
*/
void sqlite3PagerSetReiniter(Pager *pPager, void (*xReinit)(DbPage*)){
  pPager->xReiniter = xReinit;
}

/*
** Set the page size to *pPageSize. If the suggest new page size is
** inappropriate, then an alternative page size is set to that
** value before returning.
*/
int sqlite3PagerSetPagesize(Pager *pPager, u16 *pPageSize){
  int rc = pPager->errCode;
  if( rc==SQLITE_OK ){
    u16 pageSize = *pPageSize;
    assert( pageSize==0 || (pageSize>=512 && pageSize<=SQLITE_MAX_PAGE_SIZE) );
    if( pageSize && pageSize!=pPager->pageSize 
     && (pPager->memDb==0 || pPager->dbSize==0)
     && sqlite3PcacheRefCount(pPager->pPCache)==0 
    ){
      char *pNew = (char *)sqlite3PageMalloc(pageSize);
      if( !pNew ){
        rc = SQLITE_NOMEM;
      }else{
        pager_reset(pPager);
        pPager->pageSize = pageSize;
        if( !pPager->memDb ) setSectorSize(pPager);
        sqlite3PageFree(pPager->pTmpSpace);
        pPager->pTmpSpace = pNew;
        sqlite3PcacheSetPageSize(pPager->pPCache, pageSize);
      }
    }
    *pPageSize = pPager->pageSize;
  }
  return rc;
}

/*
** Return a pointer to the "temporary page" buffer held internally
** by the pager.  This is a buffer that is big enough to hold the
** entire content of a database page.  This buffer is used internally
** during rollback and will be overwritten whenever a rollback
** occurs.  But other modules are free to use it too, as long as
** no rollbacks are happening.
*/
void *sqlite3PagerTempSpace(Pager *pPager){
  return pPager->pTmpSpace;
}

/*
** Attempt to set the maximum database page count if mxPage is positive. 
** Make no changes if mxPage is zero or negative.  And never reduce the
** maximum page count below the current size of the database.
**
** Regardless of mxPage, return the current maximum page count.
*/
int sqlite3PagerMaxPageCount(Pager *pPager, int mxPage){
  if( mxPage>0 ){
    pPager->mxPgno = mxPage;
  }
  sqlite3PagerPagecount(pPager, 0);
  return pPager->mxPgno;
}

/*
** The following set of routines are used to disable the simulated
** I/O error mechanism.  These routines are used to avoid simulated
** errors in places where we do not care about errors.
**
** Unless -DSQLITE_TEST=1 is used, these routines are all no-ops
** and generate no code.
*/
#ifdef SQLITE_TEST
extern int sqlite3_io_error_pending;
extern int sqlite3_io_error_hit;
static int saved_cnt;
void disable_simulated_io_errors(void){
  saved_cnt = sqlite3_io_error_pending;
  sqlite3_io_error_pending = -1;
}
void enable_simulated_io_errors(void){
  sqlite3_io_error_pending = saved_cnt;
}
#else
# define disable_simulated_io_errors()
# define enable_simulated_io_errors()
#endif

/*
** Read the first N bytes from the beginning of the file into memory
** that pDest points to. 
**
** No error checking is done. The rational for this is that this function 
** may be called even if the file does not exist or contain a header. In 
** these cases sqlite3OsRead() will return an error, to which the correct 
** response is to zero the memory at pDest and continue.  A real IO error 
** will presumably recur and be picked up later (Todo: Think about this).
*/
int sqlite3PagerReadFileheader(Pager *pPager, int N, unsigned char *pDest){
  int rc = SQLITE_OK;
  memset(pDest, 0, N);
  assert(pPager->fd->pMethods||pPager->tempFile);
  if( pPager->fd->pMethods ){
    IOTRACE(("DBHDR %p 0 %d\n", pPager, N))
    rc = sqlite3OsRead(pPager->fd, pDest, N, 0);
    if( rc==SQLITE_IOERR_SHORT_READ ){
      rc = SQLITE_OK;
    }
  }
  return rc;
}

/*
** Return the total number of pages in the disk file associated with
** pPager. 
**
** If the PENDING_BYTE lies on the page directly after the end of the
** file, then consider this page part of the file too. For example, if
** PENDING_BYTE is byte 4096 (the first byte of page 5) and the size of the
** file is 4096 bytes, 5 is returned instead of 4.
*/
int sqlite3PagerPagecount(Pager *pPager, int *pnPage){
  i64 n = 0;
  int rc;
  assert( pPager!=0 );
  if( pPager->errCode ){
    rc = pPager->errCode;
    return rc;
  }
  if( pPager->dbSizeValid ){
    n = pPager->dbSize;
  } else {
    assert(pPager->fd->pMethods||pPager->tempFile);
    if( (pPager->fd->pMethods)
     && (rc = sqlite3OsFileSize(pPager->fd, &n))!=SQLITE_OK ){
      pager_error(pPager, rc);
      return rc;
    }
    if( n>0 && n<pPager->pageSize ){
      n = 1;
    }else{
      n /= pPager->pageSize;
    }
    if( pPager->state!=PAGER_UNLOCK ){
      pPager->dbSize = n;
      pPager->dbSizeValid = 1;
    }
  }
  if( n==(PENDING_BYTE/pPager->pageSize) ){
    n++;
  }
  if( n>pPager->mxPgno ){
    pPager->mxPgno = n;
  }
  if( pnPage ){
    *pnPage = n;
  }
  return SQLITE_OK;
}

/*
** Forward declaration
*/
static int syncJournal(Pager*);

/*
** This routine is used to truncate the cache when a database
** is truncated.  Drop from the cache all pages whose pgno is
** larger than pPager->dbSize and is unreferenced.
**
** Referenced pages larger than pPager->dbSize are zeroed.
**
** Actually, at the point this routine is called, it would be
** an error to have a referenced page.  But rather than delete
** that page and guarantee a subsequent segfault, it seems better
** to zero it and hope that we error out sanely.
*/
static void pager_truncate_cache(Pager *pPager){
  sqlite3PcacheTruncate(pPager->pPCache, pPager->dbSize);
}

/*
** Try to obtain a lock on a file.  Invoke the busy callback if the lock
** is currently not available.  Repeat until the busy callback returns
** false or until the lock succeeds.
**
** Return SQLITE_OK on success and an error code if we cannot obtain
** the lock.
*/
static int pager_wait_on_lock(Pager *pPager, int locktype){
  int rc;

  /* The OS lock values must be the same as the Pager lock values */
  assert( PAGER_SHARED==SHARED_LOCK );
  assert( PAGER_RESERVED==RESERVED_LOCK );
  assert( PAGER_EXCLUSIVE==EXCLUSIVE_LOCK );

  /* If the file is currently unlocked then the size must be unknown */
  assert( pPager->state>=PAGER_SHARED || pPager->dbSizeValid==0 );

  if( pPager->state>=locktype ){
    rc = SQLITE_OK;
  }else{
    do {
      rc = sqlite3OsLock(pPager->fd, locktype);
    }while( rc==SQLITE_BUSY && pPager->xBusyHandler(pPager->pBusyHandlerArg) );
    if( rc==SQLITE_OK ){
      pPager->state = locktype;
      IOTRACE(("LOCK %p %d\n", pPager, locktype))
    }
  }
  return rc;
}

/*
** Truncate the file to the number of pages specified.
*/
int sqlite3PagerTruncate(Pager *pPager, Pgno nPage){
  int rc = SQLITE_OK;
  assert( pPager->state>=PAGER_SHARED );

  sqlite3PagerPagecount(pPager, 0);
  if( pPager->errCode ){
    rc = pPager->errCode;
  }else if( nPage<pPager->dbSize ){
    rc = syncJournal(pPager);
    if( rc==SQLITE_OK ){
      /* Get an exclusive lock on the database before truncating. */
      rc = pager_wait_on_lock(pPager, EXCLUSIVE_LOCK);
    }
    if( rc==SQLITE_OK ){
      rc = pager_truncate(pPager, nPage);
    }
  }

  return rc;
}

/*
** Shutdown the page cache.  Free all memory and close all files.
**
** If a transaction was in progress when this routine is called, that
** transaction is rolled back.  All outstanding pages are invalidated
** and their memory is freed.  Any attempt to use a page associated
** with this page cache after this function returns will likely
** result in a coredump.
**
** This function always succeeds. If a transaction is active an attempt
** is made to roll it back. If an error occurs during the rollback 
** a hot journal may be left in the filesystem but no error is returned
** to the caller.
*/
int sqlite3PagerClose(Pager *pPager){

  disable_simulated_io_errors();
  sqlite3BeginBenignMalloc();
  pPager->errCode = 0;
  pPager->exclusiveMode = 0;
  pager_reset(pPager);
  if( !MEMDB ){
    pagerUnlockAndRollback(pPager);
  }
  enable_simulated_io_errors();
  sqlite3EndBenignMalloc();
  PAGERTRACE2("CLOSE %d\n", PAGERID(pPager));
  IOTRACE(("CLOSE %p\n", pPager))
  if( pPager->journalOpen ){
    sqlite3OsClose(pPager->jfd);
  }
  sqlite3BitvecDestroy(pPager->pInJournal);
  sqlite3BitvecDestroy(pPager->pAlwaysRollback);
  if( pPager->stmtOpen ){
    sqlite3OsClose(pPager->stfd);
  }
  sqlite3OsClose(pPager->fd);
  /* Temp files are automatically deleted by the OS
  ** if( pPager->tempFile ){
  **   sqlite3OsDelete(pPager->zFilename);
  ** }
  */

  sqlite3PageFree(pPager->pTmpSpace);
  sqlite3PcacheClose(pPager->pPCache);
  sqlite3_free(pPager);
  return SQLITE_OK;
}

#if !defined(NDEBUG) || defined(SQLITE_TEST)
/*
** Return the page number for the given page data.
*/
Pgno sqlite3PagerPagenumber(DbPage *p){
  return p->pgno;
}
#endif

/*
** Increment the reference count for a page.  The input pointer is
** a reference to the page data.
*/
int sqlite3PagerRef(DbPage *pPg){
  sqlite3PcacheRef(pPg);
  return SQLITE_OK;
}

/*
** Sync the journal.  In other words, make sure all the pages that have
** been written to the journal have actually reached the surface of the
** disk.  It is not safe to modify the original database file until after
** the journal has been synced.  If the original database is modified before
** the journal is synced and a power failure occurs, the unsynced journal
** data would be lost and we would be unable to completely rollback the
** database changes.  Database corruption would occur.
** 
** This routine also updates the nRec field in the header of the journal.
** (See comments on the pager_playback() routine for additional information.)
** If the sync mode is FULL, two syncs will occur.  First the whole journal
** is synced, then the nRec field is updated, then a second sync occurs.
**
** For temporary databases, we do not care if we are able to rollback
** after a power failure, so no sync occurs.
**
** If the IOCAP_SEQUENTIAL flag is set for the persistent media on which
** the database is stored, then OsSync() is never called on the journal
** file. In this case all that is required is to update the nRec field in
** the journal header.
**
** This routine clears the needSync field of every page current held in
** memory.
*/
static int syncJournal(Pager *pPager){
  int rc = SQLITE_OK;

  /* Sync the journal before modifying the main database
  ** (assuming there is a journal and it needs to be synced.)
  */
  if( pPager->needSync ){
    assert( !pPager->tempFile );
    if( pPager->journalMode!=PAGER_JOURNALMODE_MEMORY ){
      int iDc = sqlite3OsDeviceCharacteristics(pPager->fd);
      assert( pPager->journalOpen );

      if( 0==(iDc&SQLITE_IOCAP_SAFE_APPEND) ){
        /* Write the nRec value into the journal file header. If in
        ** full-synchronous mode, sync the journal first. This ensures that
        ** all data has really hit the disk before nRec is updated to mark
        ** it as a candidate for rollback.
        **
        ** This is not required if the persistent media supports the
        ** SAFE_APPEND property. Because in this case it is not possible 
        ** for garbage data to be appended to the file, the nRec field
        ** is populated with 0xFFFFFFFF when the journal header is written
        ** and never needs to be updated.
        */
        i64 jrnlOff;
        if( pPager->fullSync && 0==(iDc&SQLITE_IOCAP_SEQUENTIAL) ){
          PAGERTRACE2("SYNC journal of %d\n", PAGERID(pPager));
          IOTRACE(("JSYNC %p\n", pPager))
          rc = sqlite3OsSync(pPager->jfd, pPager->sync_flags);
          if( rc!=0 ) return rc;
        }

        jrnlOff = pPager->journalHdr + sizeof(aJournalMagic);
        IOTRACE(("JHDR %p %lld %d\n", pPager, jrnlOff, 4));
        rc = write32bits(pPager->jfd, jrnlOff, pPager->nRec);
        if( rc ) return rc;
      }
      if( 0==(iDc&SQLITE_IOCAP_SEQUENTIAL) ){
        PAGERTRACE2("SYNC journal of %d\n", PAGERID(pPager));
        IOTRACE(("JSYNC %p\n", pPager))
        rc = sqlite3OsSync(pPager->jfd, pPager->sync_flags| 
          (pPager->sync_flags==SQLITE_SYNC_FULL?SQLITE_SYNC_DATAONLY:0)
        );
        if( rc!=0 ) return rc;
      }
      pPager->journalStarted = 1;
    }
    pPager->needSync = 0;

    /* Erase the needSync flag from every page.
    */
    sqlite3PcacheClearSyncFlags(pPager->pPCache);
  }

  return rc;
}

/*
** Given a list of pages (connected by the PgHdr.pDirty pointer) write
** every one of those pages out to the database file. No calls are made
** to the page-cache to mark the pages as clean. It is the responsibility
** of the caller to use PcacheCleanAll() or PcacheMakeClean() to mark
** the pages as clean.
*/
static int pager_write_pagelist(PgHdr *pList){
  Pager *pPager;
  int rc;

  if( pList==0 ) return SQLITE_OK;
  pPager = pList->pPager;

  /* At this point there may be either a RESERVED or EXCLUSIVE lock on the
  ** database file. If there is already an EXCLUSIVE lock, the following
  ** calls to sqlite3OsLock() are no-ops.
  **
  ** Moving the lock from RESERVED to EXCLUSIVE actually involves going
  ** through an intermediate state PENDING.   A PENDING lock prevents new
  ** readers from attaching to the database but is unsufficient for us to
  ** write.  The idea of a PENDING lock is to prevent new readers from
  ** coming in while we wait for existing readers to clear.
  **
  ** While the pager is in the RESERVED state, the original database file
  ** is unchanged and we can rollback without having to playback the
  ** journal into the original database file.  Once we transition to
  ** EXCLUSIVE, it means the database file has been changed and any rollback
  ** will require a journal playback.
  */
  rc = pager_wait_on_lock(pPager, EXCLUSIVE_LOCK);
  if( rc!=SQLITE_OK ){
    return rc;
  }

  while( pList ){

    /* If the file has not yet been opened, open it now. */
    if( !pPager->fd->pMethods ){
      assert(pPager->tempFile);
      rc = sqlite3PagerOpentemp(pPager, pPager->fd, pPager->vfsFlags);
      if( rc ) return rc;
    }

    /* If there are dirty pages in the page cache with page numbers greater
    ** than Pager.dbSize, this means sqlite3PagerTruncate() was called to
    ** make the file smaller (presumably by auto-vacuum code). Do not write
    ** any such pages to the file.
    */
    if( pList->pgno<=pPager->dbSize && 0==(pList->flags&PGHDR_DONT_WRITE) ){
      i64 offset = (pList->pgno-1)*(i64)pPager->pageSize;
      char *pData = CODEC2(pPager, pList->pData, pList->pgno, 6);
      PAGERTRACE4("STORE %d page %d hash(%08x)\n",
                   PAGERID(pPager), pList->pgno, pager_pagehash(pList));
      IOTRACE(("PGOUT %p %d\n", pPager, pList->pgno));
      rc = sqlite3OsWrite(pPager->fd, pData, pPager->pageSize, offset);
      PAGER_INCR(sqlite3_pager_writedb_count);
      PAGER_INCR(pPager->nWrite);
      if( pList->pgno==1 ){
        memcpy(&pPager->dbFileVers, &pData[24], sizeof(pPager->dbFileVers));
      }
    }
#ifndef NDEBUG
    else{
      PAGERTRACE3("NOSTORE %d page %d\n", PAGERID(pPager), pList->pgno);
    }
#endif
    if( rc ) return rc;
#ifdef SQLITE_CHECK_PAGES
    pList->pageHash = pager_pagehash(pList);
#endif
    pList = pList->pDirty;
  }

  return SQLITE_OK;
}

/*
** This function is called by the pcache layer when it has reached some
** soft memory limit. The argument is a pointer to a purgeable Pager 
** object. This function attempts to make a single dirty page that has no
** outstanding references (if one exists) clean so that it can be recycled 
** by the pcache layer.
*/
static int pagerStress(void *p, PgHdr *pPg){
  Pager *pPager = (Pager *)p;
  int rc = SQLITE_OK;

  if( pPager->doNotSync ){
    return SQLITE_OK;
  }

  assert( pPg->flags&PGHDR_DIRTY );
  if( pPager->errCode==SQLITE_OK ){
    if( pPg->flags&PGHDR_NEED_SYNC ){
      rc = syncJournal(pPager);
      if( rc==SQLITE_OK && pPager->fullSync && 
        !(pPager->journalMode==PAGER_JOURNALMODE_MEMORY) &&
        !(sqlite3OsDeviceCharacteristics(pPager->fd)&SQLITE_IOCAP_SAFE_APPEND)
      ){
        pPager->nRec = 0;
        rc = writeJournalHdr(pPager);
      }
    }
    if( rc==SQLITE_OK ){
      pPg->pDirty = 0;
      rc = pager_write_pagelist(pPg);
    }
    if( rc!=SQLITE_OK ){
      pager_error(pPager, rc);
    }
  }

  if( rc==SQLITE_OK ){
    sqlite3PcacheMakeClean(pPg);
  }
  return rc;
}


/*
** Return 1 if there is a hot journal on the given pager.
** A hot journal is one that needs to be played back.
**
** If the current size of the database file is 0 but a journal file
** exists, that is probably an old journal left over from a prior
** database with the same name.  Just delete the journal.
**
** Return negative if unable to determine the status of the journal.
**
** This routine does not open the journal file to examine its
** content.  Hence, the journal might contain the name of a master
** journal file that has been deleted, and hence not be hot.  Or
** the header of the journal might be zeroed out.  This routine
** does not discover these cases of a non-hot journal - if the
** journal file exists and is not empty this routine assumes it
** is hot.  The pager_playback() routine will discover that the
** journal file is not really hot and will no-op.
*/
static int hasHotJournal(Pager *pPager, int *pExists){
  sqlite3_vfs *pVfs = pPager->pVfs;
  int rc = SQLITE_OK;
  int exists;
  int locked;
  assert( pPager!=0 );
  assert( pPager->useJournal );
  assert( pPager->fd->pMethods );
  *pExists = 0;
  rc = sqlite3OsAccess(pVfs, pPager->zJournal, SQLITE_ACCESS_EXISTS, &exists);
  if( rc==SQLITE_OK && exists ){
    rc = sqlite3OsCheckReservedLock(pPager->fd, &locked);
  }
  if( rc==SQLITE_OK && exists && !locked ){
    int nPage;
    rc = sqlite3PagerPagecount(pPager, &nPage);
    if( rc==SQLITE_OK ){
     if( nPage==0 ){
        sqlite3OsDelete(pVfs, pPager->zJournal, 0);
      }else{
        *pExists = 1;
      }
    }
  }
  return rc;
}

/*
** Read the content of page pPg out of the database file.
*/
static int readDbPage(Pager *pPager, PgHdr *pPg, Pgno pgno){
  int rc;
  i64 offset;
  assert( MEMDB==0 );
  assert(pPager->fd->pMethods||pPager->tempFile);
  if( !pPager->fd->pMethods ){
    return SQLITE_IOERR_SHORT_READ;
  }
  offset = (pgno-1)*(i64)pPager->pageSize;
  rc = sqlite3OsRead(pPager->fd, pPg->pData, pPager->pageSize, offset);
  PAGER_INCR(sqlite3_pager_readdb_count);
  PAGER_INCR(pPager->nRead);
  IOTRACE(("PGIN %p %d\n", pPager, pgno));
  if( pgno==1 ){
    memcpy(&pPager->dbFileVers, &((u8*)pPg->pData)[24],
                                              sizeof(pPager->dbFileVers));
  }
  CODEC1(pPager, pPg->pData, pPg->pgno, 3);
  PAGERTRACE4("FETCH %d page %d hash(%08x)\n",
               PAGERID(pPager), pPg->pgno, pager_pagehash(pPg));
  return rc;
}


/*
** This function is called to obtain the shared lock required before
** data may be read from the pager cache. If the shared lock has already
** been obtained, this function is a no-op.
**
** Immediately after obtaining the shared lock (if required), this function
** checks for a hot-journal file. If one is found, an emergency rollback
** is performed immediately.
*/
static int pagerSharedLock(Pager *pPager){
  int rc = SQLITE_OK;
  int isErrorReset = 0;

  /* If this database is opened for exclusive access, has no outstanding 
  ** page references and is in an error-state, now is the chance to clear
  ** the error. Discard the contents of the pager-cache and treat any
  ** open journal file as a hot-journal.
  */
  if( !MEMDB && pPager->exclusiveMode 
   && sqlite3PcacheRefCount(pPager->pPCache)==0 && pPager->errCode 
  ){
    if( pPager->journalOpen ){
      isErrorReset = 1;
    }
    pPager->errCode = SQLITE_OK;
    pager_reset(pPager);
  }

  /* If the pager is still in an error state, do not proceed. The error 
  ** state will be cleared at some point in the future when all page 
  ** references are dropped and the cache can be discarded.
  */
  if( pPager->errCode && pPager->errCode!=SQLITE_FULL ){
    return pPager->errCode;
  }

  if( pPager->state==PAGER_UNLOCK || isErrorReset ){
    sqlite3_vfs *pVfs = pPager->pVfs;
    int isHotJournal;
    assert( !MEMDB );
    assert( sqlite3PcacheRefCount(pPager->pPCache)==0 );
    if( !pPager->noReadlock ){
      rc = pager_wait_on_lock(pPager, SHARED_LOCK);
      if( rc!=SQLITE_OK ){
        assert( pPager->state==PAGER_UNLOCK );
        return pager_error(pPager, rc);
      }
      assert( pPager->state>=SHARED_LOCK );
    }

    /* If a journal file exists, and there is no RESERVED lock on the
    ** database file, then it either needs to be played back or deleted.
    */
    if( !isErrorReset ){
      rc = hasHotJournal(pPager, &isHotJournal);
      if( rc!=SQLITE_OK ){
        goto failed;
      }
    }
    if( isErrorReset || isHotJournal ){
      /* Get an EXCLUSIVE lock on the database file. At this point it is
      ** important that a RESERVED lock is not obtained on the way to the
      ** EXCLUSIVE lock. If it were, another process might open the
      ** database file, detect the RESERVED lock, and conclude that the
      ** database is safe to read while this process is still rolling it 
      ** back.
      ** 
      ** Because the intermediate RESERVED lock is not requested, the
      ** second process will get to this point in the code and fail to
      ** obtain its own EXCLUSIVE lock on the database file.
      */
      if( pPager->state<EXCLUSIVE_LOCK ){
        rc = sqlite3OsLock(pPager->fd, EXCLUSIVE_LOCK);
        if( rc!=SQLITE_OK ){
          rc = pager_error(pPager, rc);
          goto failed;
        }
        pPager->state = PAGER_EXCLUSIVE;
      }
 
      /* Open the journal for read/write access. This is because in 
      ** exclusive-access mode the file descriptor will be kept open and
      ** possibly used for a transaction later on. On some systems, the
      ** OsTruncate() call used in exclusive-access mode also requires
      ** a read/write file handle.
      */
      if( !isErrorReset && pPager->journalOpen==0 ){
        int res;
        rc = sqlite3OsAccess(pVfs,pPager->zJournal,SQLITE_ACCESS_EXISTS,&res);
        if( rc==SQLITE_OK ){
          if( res ){
            int fout = 0;
            int f = SQLITE_OPEN_READWRITE|SQLITE_OPEN_MAIN_JOURNAL;
            assert( !pPager->tempFile );
            rc = sqlite3OsOpen(pVfs, pPager->zJournal, pPager->jfd, f, &fout);
            assert( rc!=SQLITE_OK || pPager->jfd->pMethods );
            if( rc==SQLITE_OK && fout&SQLITE_OPEN_READONLY ){
              rc = SQLITE_CANTOPEN;
              sqlite3OsClose(pPager->jfd);
            }
          }else{
            /* If the journal does not exist, that means some other process
            ** has already rolled it back */
            rc = SQLITE_BUSY;
          }
        }
      }
      if( rc!=SQLITE_OK ){
        goto failed;
      }
      pPager->journalOpen = 1;
      pPager->journalStarted = 0;
      pPager->journalOff = 0;
      pPager->setMaster = 0;
      pPager->journalHdr = 0;
 
      /* Playback and delete the journal.  Drop the database write
      ** lock and reacquire the read lock.
      */
      rc = pager_playback(pPager, 1);
      if( rc!=SQLITE_OK ){
        rc = pager_error(pPager, rc);
        goto failed;
      }
      assert(pPager->state==PAGER_SHARED || 
          (pPager->exclusiveMode && pPager->state>PAGER_SHARED)
      );
    }

    if( sqlite3PcachePagecount(pPager->pPCache)>0 ){
      /* The shared-lock has just been acquired on the database file
      ** and there are already pages in the cache (from a previous
      ** read or write transaction).  Check to see if the database
      ** has been modified.  If the database has changed, flush the
      ** cache.
      **
      ** Database changes is detected by looking at 15 bytes beginning
      ** at offset 24 into the file.  The first 4 of these 16 bytes are
      ** a 32-bit counter that is incremented with each change.  The
      ** other bytes change randomly with each file change when
      ** a codec is in use.
      ** 
      ** There is a vanishingly small chance that a change will not be 
      ** detected.  The chance of an undetected change is so small that
      ** it can be neglected.
      */
      char dbFileVers[sizeof(pPager->dbFileVers)];
      sqlite3PagerPagecount(pPager, 0);

      if( pPager->errCode ){
        rc = pPager->errCode;
        goto failed;
      }

      assert( pPager->dbSizeValid );
      if( pPager->dbSize>0 ){
        IOTRACE(("CKVERS %p %d\n", pPager, sizeof(dbFileVers)));
        rc = sqlite3OsRead(pPager->fd, &dbFileVers, sizeof(dbFileVers), 24);
        if( rc!=SQLITE_OK ){
          goto failed;
        }
      }else{
        memset(dbFileVers, 0, sizeof(dbFileVers));
      }

      if( memcmp(pPager->dbFileVers, dbFileVers, sizeof(dbFileVers))!=0 ){
        pager_reset(pPager);
      }
    }
    assert( pPager->exclusiveMode || pPager->state<=PAGER_SHARED );
    if( pPager->state==PAGER_UNLOCK ){
      pPager->state = PAGER_SHARED;
    }
  }

 failed:
  if( rc!=SQLITE_OK ){
    /* pager_unlock() is a no-op for exclusive mode and in-memory databases. */
    pager_unlock(pPager);
  }
  return rc;
}

/*
** Make sure we have the content for a page.  If the page was
** previously acquired with noContent==1, then the content was
** just initialized to zeros instead of being read from disk.
** But now we need the real data off of disk.  So make sure we
** have it.  Read it in if we do not have it already.
*/
static int pager_get_content(PgHdr *pPg){
  if( pPg->flags&PGHDR_NEED_READ ){
    int rc = readDbPage(pPg->pPager, pPg, pPg->pgno);
    if( rc==SQLITE_OK ){
      pPg->flags &= ~PGHDR_NEED_READ;
    }else{
      return rc;
    }
  }
  return SQLITE_OK;
}

/*
** If the reference count has reached zero, and the pager is not in the
** middle of a write transaction or opened in exclusive mode, unlock it.
*/ 
static void pagerUnlockIfUnused(Pager *pPager){
  if( (sqlite3PcacheRefCount(pPager->pPCache)==0)
    && (!pPager->exclusiveMode || pPager->journalOff>0) 
  ){
    pagerUnlockAndRollback(pPager);
  }
}

/*
** Drop a page from the cache using sqlite3PcacheDrop().
**
** If this means there are now no pages with references to them, a rollback
** occurs and the lock on the database is removed.
*/
static void pagerDropPage(DbPage *pPg){
  Pager *pPager = pPg->pPager;
  sqlite3PcacheDrop(pPg);
  pagerUnlockIfUnused(pPager);
}

/*
** Acquire a page.
**
** A read lock on the disk file is obtained when the first page is acquired. 
** This read lock is dropped when the last page is released.
**
** This routine works for any page number greater than 0.  If the database
** file is smaller than the requested page, then no actual disk
** read occurs and the memory image of the page is initialized to
** all zeros.  The extra data appended to a page is always initialized
** to zeros the first time a page is loaded into memory.
**
** The acquisition might fail for several reasons.  In all cases,
** an appropriate error code is returned and *ppPage is set to NULL.
**
** See also sqlite3PagerLookup().  Both this routine and Lookup() attempt
** to find a page in the in-memory cache first.  If the page is not already
** in memory, this routine goes to disk to read it in whereas Lookup()
** just returns 0.  This routine acquires a read-lock the first time it
** has to go to disk, and could also playback an old journal if necessary.
** Since Lookup() never goes to disk, it never has to deal with locks
** or journal files.
**
** If noContent is false, the page contents are actually read from disk.
** If noContent is true, it means that we do not care about the contents
** of the page at this time, so do not do a disk read.  Just fill in the
** page content with zeros.  But mark the fact that we have not read the
** content by setting the PgHdr.needRead flag.  Later on, if 
** sqlite3PagerWrite() is called on this page or if this routine is
** called again with noContent==0, that means that the content is needed
** and the disk read should occur at that point.
*/
int sqlite3PagerAcquire(
  Pager *pPager,      /* The pager open on the database file */
  Pgno pgno,          /* Page number to fetch */
  DbPage **ppPage,    /* Write a pointer to the page here */
  int noContent       /* Do not bother reading content from disk if true */
){
  PgHdr *pPg = 0;
  int rc;

  assert( pPager->state==PAGER_UNLOCK 
       || sqlite3PcacheRefCount(pPager->pPCache)>0 
       || pgno==1
  );

  /* The maximum page number is 2^31. Return SQLITE_CORRUPT if a page
  ** number greater than this, or zero, is requested.
  */
  if( pgno>PAGER_MAX_PGNO || pgno==0 || pgno==PAGER_MJ_PGNO(pPager) ){
    return SQLITE_CORRUPT_BKPT;
  }

  /* Make sure we have not hit any critical errors.
  */ 
  assert( pPager!=0 );
  *ppPage = 0;

  /* If this is the first page accessed, then get a SHARED lock
  ** on the database file. pagerSharedLock() is a no-op if 
  ** a database lock is already held.
  */
  rc = pagerSharedLock(pPager);
  if( rc!=SQLITE_OK ){
    return rc;
  }
  assert( pPager->state!=PAGER_UNLOCK );

  rc = sqlite3PcacheFetch(pPager->pPCache, pgno, 1, &pPg);
  if( rc!=SQLITE_OK ){
    return rc;
  }
  if( pPg->pPager==0 ){
    /* The pager cache has created a new page. Its content needs to 
    ** be initialized.
    */
    int nMax;
    PAGER_INCR(pPager->nMiss);
    pPg->pPager = pPager;
    memset(pPg->pExtra, 0, pPager->nExtra);

    rc = sqlite3PagerPagecount(pPager, &nMax);
    if( rc!=SQLITE_OK ){
      sqlite3PagerUnref(pPg);
      return rc;
    }

    if( nMax<(int)pgno || MEMDB || noContent ){
      if( pgno>pPager->mxPgno ){
        sqlite3PagerUnref(pPg);
        return SQLITE_FULL;
      }
      memset(pPg->pData, 0, pPager->pageSize);
      if( noContent ){
        pPg->flags |= PGHDR_NEED_READ;
      }
      IOTRACE(("ZERO %p %d\n", pPager, pgno));
    }else{
      rc = readDbPage(pPager, pPg, pgno);
      if( rc!=SQLITE_OK && rc!=SQLITE_IOERR_SHORT_READ ){
        /* sqlite3PagerUnref(pPg); */
        pagerDropPage(pPg);
        return rc;
      }
    }
#ifdef SQLITE_CHECK_PAGES
    pPg->pageHash = pager_pagehash(pPg);
#endif
  }else{
    /* The requested page is in the page cache. */
    assert(sqlite3PcacheRefCount(pPager->pPCache)>0 || pgno==1);
    PAGER_INCR(pPager->nHit);
    if( !noContent ){
      rc = pager_get_content(pPg);
      if( rc ){
        sqlite3PagerUnref(pPg);
        return rc;
      }
    }
  }

  *ppPage = pPg;
  return SQLITE_OK;
}

/*
** Acquire a page if it is already in the in-memory cache.  Do
** not read the page from disk.  Return a pointer to the page,
** or 0 if the page is not in cache.
**
** See also sqlite3PagerGet().  The difference between this routine
** and sqlite3PagerGet() is that _get() will go to the disk and read
** in the page if the page is not already in cache.  This routine
** returns NULL if the page is not in cache or if a disk I/O error 
** has ever happened.
*/
DbPage *sqlite3PagerLookup(Pager *pPager, Pgno pgno){
  PgHdr *pPg = 0;
  assert( pPager!=0 );
  assert( pgno!=0 );

  if( (pPager->state!=PAGER_UNLOCK)
   && (pPager->errCode==SQLITE_OK || pPager->errCode==SQLITE_FULL)
  ){
    sqlite3PcacheFetch(pPager->pPCache, pgno, 0, &pPg);
  }

  return pPg;
}

/*
** Release a page.
**
** If the number of references to the page drop to zero, then the
** page is added to the LRU list.  When all references to all pages
** are released, a rollback occurs and the lock on the database is
** removed.
*/
int sqlite3PagerUnref(DbPage *pPg){
  if( pPg ){
    Pager *pPager = pPg->pPager;
    sqlite3PcacheRelease(pPg);
    pagerUnlockIfUnused(pPager);
  }
  return SQLITE_OK;
}

/*
** Create a journal file for pPager.  There should already be a RESERVED
** or EXCLUSIVE lock on the database file when this routine is called.
**
** Return SQLITE_OK if everything.  Return an error code and release the
** write lock if anything goes wrong.
*/
static int pager_open_journal(Pager *pPager){
  sqlite3_vfs *pVfs = pPager->pVfs;
  int flags = (SQLITE_OPEN_READWRITE|SQLITE_OPEN_EXCLUSIVE|SQLITE_OPEN_CREATE);

  int rc;
  assert( pPager->state>=PAGER_RESERVED );
  assert( pPager->useJournal );
  assert( pPager->pInJournal==0 );
  sqlite3PagerPagecount(pPager, 0);
  pPager->pInJournal = sqlite3BitvecCreate(pPager->dbSize);
  if( pPager->pInJournal==0 ){
    rc = SQLITE_NOMEM;
    goto failed_to_open_journal;
  }

  if( pPager->journalOpen==0 ){
    if( pPager->tempFile ){
      flags |= (SQLITE_OPEN_DELETEONCLOSE|SQLITE_OPEN_TEMP_JOURNAL);
    }else{
      flags |= (SQLITE_OPEN_MAIN_JOURNAL);
    }
    if( pPager->journalMode==PAGER_JOURNALMODE_MEMORY ){
      sqlite3MemJournalOpen(pPager->jfd);
      rc = SQLITE_OK;
    }else{
#ifdef SQLITE_ENABLE_ATOMIC_WRITE
      rc = sqlite3JournalOpen(
          pVfs, pPager->zJournal, pPager->jfd, flags, jrnlBufferSize(pPager)
      );
#else
      rc = sqlite3OsOpen(pVfs, pPager->zJournal, pPager->jfd, flags, 0);
#endif
    }
    assert( rc!=SQLITE_OK || pPager->jfd->pMethods );
    pPager->journalOff = 0;
    pPager->setMaster = 0;
    pPager->journalHdr = 0;
    if( rc!=SQLITE_OK ){
      if( rc==SQLITE_NOMEM ){
        sqlite3OsDelete(pVfs, pPager->zJournal, 0);
      }
      goto failed_to_open_journal;
    }
  }
  pPager->journalOpen = 1;
  pPager->journalStarted = 0;
  pPager->needSync = 0;
  pPager->nRec = 0;
  if( pPager->errCode ){
    rc = pPager->errCode;
    goto failed_to_open_journal;
  }
  pPager->origDbSize = pPager->dbSize;

  rc = writeJournalHdr(pPager);

  if( pPager->stmtAutoopen && rc==SQLITE_OK ){
    rc = sqlite3PagerStmtBegin(pPager);
  }
  if( rc!=SQLITE_OK && rc!=SQLITE_NOMEM && rc!=SQLITE_IOERR_NOMEM ){
    rc = pager_end_transaction(pPager, 0);
    if( rc==SQLITE_OK ){
      rc = SQLITE_FULL;
    }
  }
  return rc;

failed_to_open_journal:
  sqlite3BitvecDestroy(pPager->pInJournal);
  pPager->pInJournal = 0;
  return rc;
}

/*
** Acquire a write-lock on the database.  The lock is removed when
** the any of the following happen:
**
**   *  sqlite3PagerCommitPhaseTwo() is called.
**   *  sqlite3PagerRollback() is called.
**   *  sqlite3PagerClose() is called.
**   *  sqlite3PagerUnref() is called to on every outstanding page.
**
** The first parameter to this routine is a pointer to any open page of the
** database file.  Nothing changes about the page - it is used merely to
** acquire a pointer to the Pager structure and as proof that there is
** already a read-lock on the database.
**
** The second parameter indicates how much space in bytes to reserve for a
** master journal file-name at the start of the journal when it is created.
**
** A journal file is opened if this is not a temporary file.  For temporary
** files, the opening of the journal file is deferred until there is an
** actual need to write to the journal.
**
** If the database is already reserved for writing, this routine is a no-op.
**
** If exFlag is true, go ahead and get an EXCLUSIVE lock on the file
** immediately instead of waiting until we try to flush the cache.  The
** exFlag is ignored if a transaction is already active.
*/
int sqlite3PagerBegin(DbPage *pPg, int exFlag){
  Pager *pPager = pPg->pPager;
  int rc = SQLITE_OK;
  assert( pPg->nRef>0 );
  assert( pPager->state!=PAGER_UNLOCK );
  if( pPager->state==PAGER_SHARED ){
    assert( pPager->pInJournal==0 );
    assert( !MEMDB );
    rc = sqlite3OsLock(pPager->fd, RESERVED_LOCK);
    if( rc==SQLITE_OK ){
      pPager->state = PAGER_RESERVED;
      if( exFlag ){
        rc = pager_wait_on_lock(pPager, EXCLUSIVE_LOCK);
      }
    }
    if( rc!=SQLITE_OK ){
      return rc;
    }
    pPager->dirtyCache = 0;
    PAGERTRACE2("TRANSACTION %d\n", PAGERID(pPager));
    if( pPager->useJournal && !pPager->tempFile
           && pPager->journalMode!=PAGER_JOURNALMODE_OFF ){
      rc = pager_open_journal(pPager);
    }
  }else if( pPager->journalOpen && pPager->journalOff==0 ){
    /* This happens when the pager was in exclusive-access mode the last
    ** time a (read or write) transaction was successfully concluded
    ** by this connection. Instead of deleting the journal file it was 
    ** kept open and either was truncated to 0 bytes or its header was
    ** overwritten with zeros.
    */
    assert( pPager->nRec==0 );
    assert( pPager->origDbSize==0 );
    assert( pPager->pInJournal==0 );
    sqlite3PagerPagecount(pPager, 0);
    pPager->pInJournal = sqlite3BitvecCreate( pPager->dbSize );
    if( !pPager->pInJournal ){
      rc = SQLITE_NOMEM;
    }else{
      pPager->origDbSize = pPager->dbSize;
      rc = writeJournalHdr(pPager);
    }
  }
  assert( !pPager->journalOpen || pPager->journalOff>0 || rc!=SQLITE_OK );
  return rc;
}

/*
** Mark a data page as writeable.  The page is written into the journal 
** if it is not there already.  This routine must be called before making
** changes to a page.
**
** The first time this routine is called, the pager creates a new
** journal and acquires a RESERVED lock on the database.  If the RESERVED
** lock could not be acquired, this routine returns SQLITE_BUSY.  The
** calling routine must check for that return value and be careful not to
** change any page data until this routine returns SQLITE_OK.
**
** If the journal file could not be written because the disk is full,
** then this routine returns SQLITE_FULL and does an immediate rollback.
** All subsequent write attempts also return SQLITE_FULL until there
** is a call to sqlite3PagerCommit() or sqlite3PagerRollback() to
** reset.
*/
static int pager_write(PgHdr *pPg){
  void *pData = pPg->pData;
  Pager *pPager = pPg->pPager;
  int rc = SQLITE_OK;

  /* Check for errors
  */
  if( pPager->errCode ){ 
    return pPager->errCode;
  }
  if( pPager->readOnly ){
    return SQLITE_PERM;
  }

  assert( !pPager->setMaster );

  CHECK_PAGE(pPg);

  /* If this page was previously acquired with noContent==1, that means
  ** we didn't really read in the content of the page.  This can happen
  ** (for example) when the page is being moved to the freelist.  But
  ** now we are (perhaps) moving the page off of the freelist for
  ** reuse and we need to know its original content so that content
  ** can be stored in the rollback journal.  So do the read at this
  ** time.
  */
  rc = pager_get_content(pPg);
  if( rc ){
    return rc;
  }

  /* Mark the page as dirty.  If the page has already been written
  ** to the journal then we can return right away.
  */
  sqlite3PcacheMakeDirty(pPg);
  if( pageInJournal(pPg) && (pageInStatement(pPg) || pPager->stmtInUse==0) ){
    pPager->dirtyCache = 1;
    pPager->dbModified = 1;
  }else{

    /* If we get this far, it means that the page needs to be
    ** written to the transaction journal or the ckeckpoint journal
    ** or both.
    **
    ** First check to see that the transaction journal exists and
    ** create it if it does not.
    */
    assert( pPager->state!=PAGER_UNLOCK );
    rc = sqlite3PagerBegin(pPg, 0);
    if( rc!=SQLITE_OK ){
      return rc;
    }
    assert( pPager->state>=PAGER_RESERVED );
    if( !pPager->journalOpen && pPager->useJournal
          && pPager->journalMode!=PAGER_JOURNALMODE_OFF ){
      rc = pager_open_journal(pPager);
      if( rc!=SQLITE_OK ) return rc;
    }
    pPager->dirtyCache = 1;
    pPager->dbModified = 1;
  
    /* The transaction journal now exists and we have a RESERVED or an
    ** EXCLUSIVE lock on the main database file.  Write the current page to
    ** the transaction journal if it is not there already.
    */
    if( !pageInJournal(pPg) && pPager->journalOpen ){
      if( pPg->pgno<=pPager->origDbSize ){
        u32 cksum;
        char *pData2;

        /* We should never write to the journal file the page that
        ** contains the database locks.  The following assert verifies
        ** that we do not. */
        assert( pPg->pgno!=PAGER_MJ_PGNO(pPager) );
        pData2 = CODEC2(pPager, pData, pPg->pgno, 7);
        cksum = pager_cksum(pPager, (u8*)pData2);
        rc = write32bits(pPager->jfd, pPager->journalOff, pPg->pgno);
        if( rc==SQLITE_OK ){
          rc = sqlite3OsWrite(pPager->jfd, pData2, pPager->pageSize,
                              pPager->journalOff + 4);
          pPager->journalOff += pPager->pageSize+4;
        }
        if( rc==SQLITE_OK ){
          rc = write32bits(pPager->jfd, pPager->journalOff, cksum);
          pPager->journalOff += 4;
        }
        IOTRACE(("JOUT %p %d %lld %d\n", pPager, pPg->pgno, 
                 pPager->journalOff, pPager->pageSize));
        PAGER_INCR(sqlite3_pager_writej_count);
        PAGERTRACE5("JOURNAL %d page %d needSync=%d hash(%08x)\n",
             PAGERID(pPager), pPg->pgno, 
             ((pPg->flags&PGHDR_NEED_SYNC)?1:0), pager_pagehash(pPg));

        /* An error has occured writing to the journal file. The 
        ** transaction will be rolled back by the layer above.
        */
        if( rc!=SQLITE_OK ){
          return rc;
        }

        pPager->nRec++;
        assert( pPager->pInJournal!=0 );
        sqlite3BitvecSet(pPager->pInJournal, pPg->pgno);
        if( !pPager->noSync ){
          pPg->flags |= PGHDR_NEED_SYNC;
        }
        if( pPager->stmtInUse ){
          sqlite3BitvecSet(pPager->pInStmt, pPg->pgno);
        }
      }else{
        if( !pPager->journalStarted && !pPager->noSync ){
          pPg->flags |= PGHDR_NEED_SYNC;
        }
        PAGERTRACE4("APPEND %d page %d needSync=%d\n",
                PAGERID(pPager), pPg->pgno,
               ((pPg->flags&PGHDR_NEED_SYNC)?1:0));
      }
      if( pPg->flags&PGHDR_NEED_SYNC ){
        pPager->needSync = 1;
      }
    }
  
    /* If the statement journal is open and the page is not in it,
    ** then write the current page to the statement journal.  Note that
    ** the statement journal format differs from the standard journal format
    ** in that it omits the checksums and the header.
    */
    if( pPager->stmtInUse 
     && !pageInStatement(pPg) 
     && pPg->pgno<=pPager->stmtSize 
    ){
      i64 offset = pPager->stmtNRec*(4+pPager->pageSize);
      char *pData2 = CODEC2(pPager, pData, pPg->pgno, 7);
      assert( pageInJournal(pPg) || pPg->pgno>pPager->origDbSize );
      rc = write32bits(pPager->stfd, offset, pPg->pgno);
      if( rc==SQLITE_OK ){
        rc = sqlite3OsWrite(pPager->stfd, pData2, pPager->pageSize, offset+4);
      }
      PAGERTRACE3("STMT-JOURNAL %d page %d\n", PAGERID(pPager), pPg->pgno);
      if( rc!=SQLITE_OK ){
        return rc;
      }
      pPager->stmtNRec++;
      assert( pPager->pInStmt!=0 );
      sqlite3BitvecSet(pPager->pInStmt, pPg->pgno);
    }
  }

  /* Update the database size and return.
  */
  assert( pPager->state>=PAGER_SHARED );
  if( pPager->dbSize<pPg->pgno ){
    pPager->dbSize = pPg->pgno;
    if( pPager->dbSize==(PAGER_MJ_PGNO(pPager)-1) ){
      pPager->dbSize++;
    }
  }
  return rc;
}

/*
** This function is used to mark a data-page as writable. It uses 
** pager_write() to open a journal file (if it is not already open)
** and write the page *pData to the journal.
**
** The difference between this function and pager_write() is that this
** function also deals with the special case where 2 or more pages
** fit on a single disk sector. In this case all co-resident pages
** must have been written to the journal file before returning.
*/
int sqlite3PagerWrite(DbPage *pDbPage){
  int rc = SQLITE_OK;

  PgHdr *pPg = pDbPage;
  Pager *pPager = pPg->pPager;
  Pgno nPagePerSector = (pPager->sectorSize/pPager->pageSize);

  if( nPagePerSector>1 ){
    Pgno nPageCount;          /* Total number of pages in database file */
    Pgno pg1;                 /* First page of the sector pPg is located on. */
    int nPage;                /* Number of pages starting at pg1 to journal */
    int ii;
    int needSync = 0;

    /* Set the doNotSync flag to 1. This is because we cannot allow a journal
    ** header to be written between the pages journaled by this function.
    */
    assert( !MEMDB );
    assert( pPager->doNotSync==0 );
    pPager->doNotSync = 1;

    /* This trick assumes that both the page-size and sector-size are
    ** an integer power of 2. It sets variable pg1 to the identifier
    ** of the first page of the sector pPg is located on.
    */
    pg1 = ((pPg->pgno-1) & ~(nPagePerSector-1)) + 1;

    sqlite3PagerPagecount(pPager, (int *)&nPageCount);
    if( pPg->pgno>nPageCount ){
      nPage = (pPg->pgno - pg1)+1;
    }else if( (pg1+nPagePerSector-1)>nPageCount ){
      nPage = nPageCount+1-pg1;
    }else{
      nPage = nPagePerSector;
    }
    assert(nPage>0);
    assert(pg1<=pPg->pgno);
    assert((pg1+nPage)>pPg->pgno);

    for(ii=0; ii<nPage && rc==SQLITE_OK; ii++){
      Pgno pg = pg1+ii;
      PgHdr *pPage;
      if( pg==pPg->pgno || !sqlite3BitvecTest(pPager->pInJournal, pg) ){
        if( pg!=PAGER_MJ_PGNO(pPager) ){
          rc = sqlite3PagerGet(pPager, pg, &pPage);
          if( rc==SQLITE_OK ){
            rc = pager_write(pPage);
            if( pPage->flags&PGHDR_NEED_SYNC ){
              needSync = 1;
            }
            sqlite3PagerUnref(pPage);
          }
        }
      }else if( (pPage = pager_lookup(pPager, pg))!=0 ){
        if( pPage->flags&PGHDR_NEED_SYNC ){
          needSync = 1;
        }
        sqlite3PagerUnref(pPage);
      }
    }

    /* If the PgHdr.needSync flag is set for any of the nPage pages 
    ** starting at pg1, then it needs to be set for all of them. Because
    ** writing to any of these nPage pages may damage the others, the
    ** journal file must contain sync()ed copies of all of them
    ** before any of them can be written out to the database file.
    */
    if( needSync ){
      assert( !MEMDB && pPager->noSync==0 );
      for(ii=0; ii<nPage && needSync; ii++){
        PgHdr *pPage = pager_lookup(pPager, pg1+ii);
        if( pPage ) pPage->flags |= PGHDR_NEED_SYNC;
        sqlite3PagerUnref(pPage);
      }
      assert(pPager->needSync);
    }

    assert( pPager->doNotSync==1 );
    pPager->doNotSync = 0;
  }else{
    rc = pager_write(pDbPage);
  }
  return rc;
}

/*
** Return TRUE if the page given in the argument was previously passed
** to sqlite3PagerWrite().  In other words, return TRUE if it is ok
** to change the content of the page.
*/
#ifndef NDEBUG
int sqlite3PagerIswriteable(DbPage *pPg){
  return pPg->flags&PGHDR_DIRTY;
}
#endif

/*
** A call to this routine tells the pager that it is not necessary to
** write the information on page pPg back to the disk, even though
** that page might be marked as dirty.  This happens, for example, when
** the page has been added as a leaf of the freelist and so its
** content no longer matters.
**
** The overlying software layer calls this routine when all of the data
** on the given page is unused.  The pager marks the page as clean so
** that it does not get written to disk.
**
** Tests show that this optimization, together with the
** sqlite3PagerDontRollback() below, more than double the speed
** of large INSERT operations and quadruple the speed of large DELETEs.
**
** When this routine is called, set the alwaysRollback flag to true.
** Subsequent calls to sqlite3PagerDontRollback() for the same page
** will thereafter be ignored.  This is necessary to avoid a problem
** where a page with data is added to the freelist during one part of
** a transaction then removed from the freelist during a later part
** of the same transaction and reused for some other purpose.  When it
** is first added to the freelist, this routine is called.  When reused,
** the sqlite3PagerDontRollback() routine is called.  But because the
** page contains critical data, we still need to be sure it gets
** rolled back in spite of the sqlite3PagerDontRollback() call.
*/
int sqlite3PagerDontWrite(DbPage *pDbPage){
  PgHdr *pPg = pDbPage;
  Pager *pPager = pPg->pPager;
  int rc;

  if( pPg->pgno>pPager->origDbSize ){
    return SQLITE_OK;
  }
  if( pPager->pAlwaysRollback==0 ){
    assert( pPager->pInJournal );
    pPager->pAlwaysRollback = sqlite3BitvecCreate(pPager->origDbSize);
    if( !pPager->pAlwaysRollback ){
      return SQLITE_NOMEM;
    }
  }
  rc = sqlite3BitvecSet(pPager->pAlwaysRollback, pPg->pgno);

  if( rc==SQLITE_OK && (pPg->flags&PGHDR_DIRTY) && !pPager->stmtInUse ){
    assert( pPager->state>=PAGER_SHARED );
    if( pPager->dbSize==pPg->pgno && pPager->origDbSize<pPager->dbSize ){
      /* If this pages is the last page in the file and the file has grown
      ** during the current transaction, then do NOT mark the page as clean.
      ** When the database file grows, we must make sure that the last page
      ** gets written at least once so that the disk file will be the correct
      ** size. If you do not write this page and the size of the file
      ** on the disk ends up being too small, that can lead to database
      ** corruption during the next transaction.
      */
    }else{
      PAGERTRACE3("DONT_WRITE page %d of %d\n", pPg->pgno, PAGERID(pPager));
      IOTRACE(("CLEAN %p %d\n", pPager, pPg->pgno))
      pPg->flags |= PGHDR_DONT_WRITE;
#ifdef SQLITE_CHECK_PAGES
      pPg->pageHash = pager_pagehash(pPg);
#endif
    }
  }
  return rc;
}

/*
** A call to this routine tells the pager that if a rollback occurs,
** it is not necessary to restore the data on the given page.  This
** means that the pager does not have to record the given page in the
** rollback journal.
**
** If we have not yet actually read the content of this page (if
** the PgHdr.needRead flag is set) then this routine acts as a promise
** that we will never need to read the page content in the future.
** so the needRead flag can be cleared at this point.
*/
void sqlite3PagerDontRollback(DbPage *pPg){
  Pager *pPager = pPg->pPager;

  assert( pPager->state>=PAGER_RESERVED );

  /* If the journal file is not open, or DontWrite() has been called on
  ** this page (DontWrite() sets the alwaysRollback flag), then this
  ** function is a no-op.
  */
  if( pPager->journalOpen==0 
   || sqlite3BitvecTest(pPager->pAlwaysRollback, pPg->pgno)
   || pPg->pgno>pPager->origDbSize
  ){
    return;
  }

#ifdef SQLITE_SECURE_DELETE
  if( sqlite3BitvecTest(pPager->pInJournal, pPg->pgno)!=0
   || pPg->pgno>pPager->origDbSize ){
    return;
  }
#endif

  /* If SECURE_DELETE is disabled, then there is no way that this
  ** routine can be called on a page for which sqlite3PagerDontWrite()
  ** has not been previously called during the same transaction.
  ** And if DontWrite() has previously been called, the following
  ** conditions must be met.
  **
  ** (Later:)  Not true.  If the database is corrupted by having duplicate
  ** pages on the freelist (ex: corrupt9.test) then the following is not
  ** necessarily true:
  */
  /* assert( !pPg->inJournal && (int)pPg->pgno <= pPager->origDbSize ); */

  assert( pPager->pInJournal!=0 );
  sqlite3BitvecSet(pPager->pInJournal, pPg->pgno);
  pPg->flags &= ~PGHDR_NEED_READ;
  if( pPager->stmtInUse ){
    assert( pPager->stmtSize >= pPager->origDbSize );
    sqlite3BitvecSet(pPager->pInStmt, pPg->pgno);
  }
  PAGERTRACE3("DONT_ROLLBACK page %d of %d\n", pPg->pgno, PAGERID(pPager));
  IOTRACE(("GARBAGE %p %d\n", pPager, pPg->pgno))
}


/*
** This routine is called to increment the database file change-counter,
** stored at byte 24 of the pager file.
*/
static int pager_incr_changecounter(Pager *pPager, int isDirect){
  PgHdr *pPgHdr;
  u32 change_counter;
  int rc = SQLITE_OK;

#ifndef SQLITE_ENABLE_ATOMIC_WRITE
  assert( isDirect==0 );  /* isDirect is only true for atomic writes */
#endif
  if( !pPager->changeCountDone ){
    /* Open page 1 of the file for writing. */
    rc = sqlite3PagerGet(pPager, 1, &pPgHdr);
    if( rc!=SQLITE_OK ) return rc;

    if( !isDirect ){
      rc = sqlite3PagerWrite(pPgHdr);
      if( rc!=SQLITE_OK ){
        sqlite3PagerUnref(pPgHdr);
        return rc;
      }
    }

    /* Increment the value just read and write it back to byte 24. */
    change_counter = sqlite3Get4byte((u8*)pPager->dbFileVers);
    change_counter++;
    put32bits(((char*)pPgHdr->pData)+24, change_counter);

#ifdef SQLITE_ENABLE_ATOMIC_WRITE
    if( isDirect && pPager->fd->pMethods ){
      const void *zBuf = pPgHdr->pData;
      rc = sqlite3OsWrite(pPager->fd, zBuf, pPager->pageSize, 0);
    }
#endif

    /* Release the page reference. */
    sqlite3PagerUnref(pPgHdr);
    pPager->changeCountDone = 1;
  }
  return rc;
}

/*
** Sync the pager file to disk.
*/
int sqlite3PagerSync(Pager *pPager){
  int rc;
  if( MEMDB ){
    rc = SQLITE_OK;
  }else{
    rc = sqlite3OsSync(pPager->fd, pPager->sync_flags);
  }
  return rc;
}

/*
** Sync the database file for the pager pPager. zMaster points to the name
** of a master journal file that should be written into the individual
** journal file. zMaster may be NULL, which is interpreted as no master
** journal (a single database transaction).
**
** This routine ensures that the journal is synced, all dirty pages written
** to the database file and the database file synced. The only thing that
** remains to commit the transaction is to delete the journal file (or
** master journal file if specified).
**
** Note that if zMaster==NULL, this does not overwrite a previous value
** passed to an sqlite3PagerCommitPhaseOne() call.
**
** If parameter nTrunc is non-zero, then the pager file is truncated to
** nTrunc pages (this is used by auto-vacuum databases).
**
** If the final parameter - noSync - is true, then the database file itself
** is not synced. The caller must call sqlite3PagerSync() directly to
** sync the database file before calling CommitPhaseTwo() to delete the
** journal file in this case.
*/
int sqlite3PagerCommitPhaseOne(
  Pager *pPager, 
  const char *zMaster, 
  Pgno nTrunc,
  int noSync
){
  int rc = SQLITE_OK;

  if( pPager->errCode ){
    return pPager->errCode;
  }

  /* If no changes have been made, we can leave the transaction early.
  */
  if( pPager->dbModified==0 &&
        (pPager->journalMode!=PAGER_JOURNALMODE_DELETE ||
          pPager->exclusiveMode!=0) ){
    assert( pPager->dirtyCache==0 || pPager->journalOpen==0 );
    return SQLITE_OK;
  }

  PAGERTRACE4("DATABASE SYNC: File=%s zMaster=%s nTrunc=%d\n", 
      pPager->zFilename, zMaster, nTrunc);

  /* If this is an in-memory db, or no pages have been written to, or this
  ** function has already been called, it is a no-op.
  */
  if( pPager->state!=PAGER_SYNCED && !MEMDB && pPager->dirtyCache ){
    PgHdr *pPg;

#ifdef SQLITE_ENABLE_ATOMIC_WRITE
    /* The atomic-write optimization can be used if all of the
    ** following are true:
    **
    **    + The file-system supports the atomic-write property for
    **      blocks of size page-size, and
    **    + This commit is not part of a multi-file transaction, and
    **    + Exactly one page has been modified and store in the journal file.
    **
    ** If the optimization can be used, then the journal file will never
    ** be created for this transaction.
    */
    int useAtomicWrite;
    pPg = sqlite3PcacheDirtyList(pPager->pPCache);
    useAtomicWrite = (
        !zMaster && 
        pPager->journalOpen &&
        pPager->journalOff==jrnlBufferSize(pPager) && 
        nTrunc==0 && 
        (pPg==0 || pPg->pDirty==0)
    );
    assert( pPager->journalOpen || pPager->journalMode==PAGER_JOURNALMODE_OFF );
    if( useAtomicWrite ){
      /* Update the nRec field in the journal file. */
      int offset = pPager->journalHdr + sizeof(aJournalMagic);
      assert(pPager->nRec==1);
      rc = write32bits(pPager->jfd, offset, pPager->nRec);

      /* Update the db file change counter. The following call will modify
      ** the in-memory representation of page 1 to include the updated
      ** change counter and then write page 1 directly to the database
      ** file. Because of the atomic-write property of the host file-system, 
      ** this is safe.
      */
      if( rc==SQLITE_OK ){
        rc = pager_incr_changecounter(pPager, 1);
      }
    }else{
      rc = sqlite3JournalCreate(pPager->jfd);
    }

    if( !useAtomicWrite && rc==SQLITE_OK )
#endif

    /* If a master journal file name has already been written to the
    ** journal file, then no sync is required. This happens when it is
    ** written, then the process fails to upgrade from a RESERVED to an
    ** EXCLUSIVE lock. The next time the process tries to commit the
    ** transaction the m-j name will have already been written.
    */
    if( !pPager->setMaster ){
      rc = pager_incr_changecounter(pPager, 0);
      if( rc!=SQLITE_OK ) goto sync_exit;
      if( pPager->journalMode!=PAGER_JOURNALMODE_OFF ){
#ifndef SQLITE_OMIT_AUTOVACUUM
        if( nTrunc!=0 ){
          /* If this transaction has made the database smaller, then all pages
          ** being discarded by the truncation must be written to the journal
          ** file.
          */
          Pgno i;
          Pgno iSkip = PAGER_MJ_PGNO(pPager);
          for( i=nTrunc+1; i<=pPager->origDbSize; i++ ){
            if( !sqlite3BitvecTest(pPager->pInJournal, i) && i!=iSkip ){
              rc = sqlite3PagerGet(pPager, i, &pPg);
              if( rc!=SQLITE_OK ) goto sync_exit;
              rc = sqlite3PagerWrite(pPg);
              sqlite3PagerUnref(pPg);
              if( rc!=SQLITE_OK ) goto sync_exit;
            }
          } 
        }
#endif
        rc = writeMasterJournal(pPager, zMaster);
        if( rc!=SQLITE_OK ) goto sync_exit;
        rc = syncJournal(pPager);
      }
    }
    if( rc!=SQLITE_OK ) goto sync_exit;

#ifndef SQLITE_OMIT_AUTOVACUUM
    if( nTrunc!=0 ){
      rc = sqlite3PagerTruncate(pPager, nTrunc);
      if( rc!=SQLITE_OK ) goto sync_exit;
    }
#endif

    /* Write all dirty pages to the database file */
    pPg = sqlite3PcacheDirtyList(pPager->pPCache);
    rc = pager_write_pagelist(pPg);
    if( rc!=SQLITE_OK ){
      assert( rc!=SQLITE_IOERR_BLOCKED );
      /* The error might have left the dirty list all fouled up here,
      ** but that does not matter because if the if the dirty list did
      ** get corrupted, then the transaction will roll back and
      ** discard the dirty list.  There is an assert in
      ** pager_get_all_dirty_pages() that verifies that no attempt
      ** is made to use an invalid dirty list.
      */
      goto sync_exit;
    }
    sqlite3PcacheCleanAll(pPager->pPCache);

    /* Sync the database file. */
    if( !pPager->noSync && !noSync ){
      rc = sqlite3OsSync(pPager->fd, pPager->sync_flags);
    }
    IOTRACE(("DBSYNC %p\n", pPager))

    pPager->state = PAGER_SYNCED;
  }else if( MEMDB && nTrunc!=0 ){
    rc = sqlite3PagerTruncate(pPager, nTrunc);
  }

sync_exit:
  if( rc==SQLITE_IOERR_BLOCKED ){
    /* pager_incr_changecounter() may attempt to obtain an exclusive
     * lock to spill the cache and return IOERR_BLOCKED. But since 
     * there is no chance the cache is inconsistent, it is
     * better to return SQLITE_BUSY.
     */
    rc = SQLITE_BUSY;
  }
  return rc;
}


/*
** Commit all changes to the database and release the write lock.
**
** If the commit fails for any reason, a rollback attempt is made
** and an error code is returned.  If the commit worked, SQLITE_OK
** is returned.
*/
int sqlite3PagerCommitPhaseTwo(Pager *pPager){
  int rc = SQLITE_OK;

  if( pPager->errCode ){
    return pPager->errCode;
  }
  if( pPager->state<PAGER_RESERVED ){
    return SQLITE_ERROR;
  }
  if( pPager->dbModified==0 &&
        (pPager->journalMode!=PAGER_JOURNALMODE_DELETE ||
          pPager->exclusiveMode!=0) ){
    assert( pPager->dirtyCache==0 || pPager->journalOpen==0 );
    return SQLITE_OK;
  }
  PAGERTRACE2("COMMIT %d\n", PAGERID(pPager));
  assert( pPager->state==PAGER_SYNCED || MEMDB || !pPager->dirtyCache );
  rc = pager_end_transaction(pPager, pPager->setMaster);
  rc = pager_error(pPager, rc);
  return rc;
}

/*
** Rollback all changes.  The database falls back to PAGER_SHARED mode.
** All in-memory cache pages revert to their original data contents.
** The journal is deleted.
**
** This routine cannot fail unless some other process is not following
** the correct locking protocol or unless some other
** process is writing trash into the journal file (SQLITE_CORRUPT) or
** unless a prior malloc() failed (SQLITE_NOMEM).  Appropriate error
** codes are returned for all these occasions.  Otherwise,
** SQLITE_OK is returned.
*/
int sqlite3PagerRollback(Pager *pPager){
  int rc = SQLITE_OK;
  PAGERTRACE2("ROLLBACK %d\n", PAGERID(pPager));
  if( !pPager->dirtyCache || !pPager->journalOpen ){
    rc = pager_end_transaction(pPager, pPager->setMaster);
  }else if( pPager->errCode && pPager->errCode!=SQLITE_FULL ){
    if( pPager->state>=PAGER_EXCLUSIVE ){
      pager_playback(pPager, 0);
    }
    rc = pPager->errCode;
  }else{
    if( pPager->state==PAGER_RESERVED ){
      int rc2;
      rc = pager_playback(pPager, 0);
      rc2 = pager_end_transaction(pPager, pPager->setMaster);
      if( rc==SQLITE_OK ){
        rc = rc2;
      }
    }else{
      rc = pager_playback(pPager, 0);
    }

    if( !MEMDB ){
      pPager->dbSizeValid = 0;
    }

    /* If an error occurs during a ROLLBACK, we can no longer trust the pager
    ** cache. So call pager_error() on the way out to make any error 
    ** persistent.
    */
    rc = pager_error(pPager, rc);
  }
  return rc;
}

/*
** Return TRUE if the database file is opened read-only.  Return FALSE
** if the database is (in theory) writable.
*/
int sqlite3PagerIsreadonly(Pager *pPager){
  return pPager->readOnly;
}

/*
** Return the number of references to the pager.
*/
int sqlite3PagerRefcount(Pager *pPager){
  return sqlite3PcacheRefCount(pPager->pPCache);
}

/*
** Return the number of references to the specified page.
*/
int sqlite3PagerPageRefcount(DbPage *pPage){
  return sqlite3PcachePageRefcount(pPage);
}

#ifdef SQLITE_TEST
/*
** This routine is used for testing and analysis only.
*/
int *sqlite3PagerStats(Pager *pPager){
  static int a[11];
  a[0] = sqlite3PcacheRefCount(pPager->pPCache);
  a[1] = sqlite3PcachePagecount(pPager->pPCache);
  a[2] = sqlite3PcacheGetCachesize(pPager->pPCache);
  a[3] = pPager->dbSizeValid ? (int) pPager->dbSize : -1;
  a[4] = pPager->state;
  a[5] = pPager->errCode;
  a[6] = pPager->nHit;
  a[7] = pPager->nMiss;
  a[8] = 0;  /* Used to be pPager->nOvfl */
  a[9] = pPager->nRead;
  a[10] = pPager->nWrite;
  return a;
}
int sqlite3PagerIsMemdb(Pager *pPager){
  return MEMDB;
}
#endif

/*
** Set the statement rollback point.
**
** This routine should be called with the transaction journal already
** open.  A new statement journal is created that can be used to rollback
** changes of a single SQL command within a larger transaction.
*/
static int pagerStmtBegin(Pager *pPager){
  int rc;
  assert( !pPager->stmtInUse );
  assert( pPager->state>=PAGER_SHARED );
  assert( pPager->dbSizeValid );
  PAGERTRACE2("STMT-BEGIN %d\n", PAGERID(pPager));
  if( !pPager->journalOpen ){
    pPager->stmtAutoopen = 1;
    return SQLITE_OK;
  }
  assert( pPager->journalOpen );
  assert( pPager->pInStmt==0 );
  pPager->pInStmt = sqlite3BitvecCreate(pPager->dbSize);
  if( pPager->pInStmt==0 ){
    /* sqlite3OsLock(pPager->fd, SHARED_LOCK); */
    return SQLITE_NOMEM;
  }
  pPager->stmtJSize = pPager->journalOff;
  pPager->stmtSize = pPager->dbSize;
  pPager->stmtHdrOff = 0;
  pPager->stmtCksum = pPager->cksumInit;
  if( !pPager->stmtOpen ){
    if( pPager->journalMode==PAGER_JOURNALMODE_MEMORY ){
      sqlite3MemJournalOpen(pPager->stfd);
    }else{
      rc = sqlite3PagerOpentemp(pPager, pPager->stfd, SQLITE_OPEN_SUBJOURNAL);
      if( rc ){
        goto stmt_begin_failed;
      }
    }
    pPager->stmtOpen = 1;
    pPager->stmtNRec = 0;
  }
  pPager->stmtInUse = 1;
  return SQLITE_OK;
 
stmt_begin_failed:
  if( pPager->pInStmt ){
    sqlite3BitvecDestroy(pPager->pInStmt);
    pPager->pInStmt = 0;
  }
  return rc;
}
int sqlite3PagerStmtBegin(Pager *pPager){
  int rc;
  rc = pagerStmtBegin(pPager);
  return rc;
}

/*
** Commit a statement.
*/
int sqlite3PagerStmtCommit(Pager *pPager){
  if( pPager->stmtInUse ){
    PAGERTRACE2("STMT-COMMIT %d\n", PAGERID(pPager));
    sqlite3BitvecDestroy(pPager->pInStmt);
    pPager->pInStmt = 0;
    pPager->stmtNRec = 0;
    pPager->stmtInUse = 0;
    if( sqlite3IsMemJournal(pPager->stfd) ){
      sqlite3OsTruncate(pPager->stfd, 0);
    }
  }
  pPager->stmtAutoopen = 0;
  return SQLITE_OK;
}

/*
** Rollback a statement.
*/
int sqlite3PagerStmtRollback(Pager *pPager){
  int rc;
  if( pPager->stmtInUse ){
    PAGERTRACE2("STMT-ROLLBACK %d\n", PAGERID(pPager));
    rc = pager_stmt_playback(pPager);
    sqlite3PagerStmtCommit(pPager);
  }else{
    rc = SQLITE_OK;
  }
  pPager->stmtAutoopen = 0;
  return rc;
}

/*
** Return the full pathname of the database file.
*/
const char *sqlite3PagerFilename(Pager *pPager){
  return pPager->zFilename;
}

/*
** Return the VFS structure for the pager.
*/
const sqlite3_vfs *sqlite3PagerVfs(Pager *pPager){
  return pPager->pVfs;
}

/*
** Return the file handle for the database file associated
** with the pager.  This might return NULL if the file has
** not yet been opened.
*/
sqlite3_file *sqlite3PagerFile(Pager *pPager){
  return pPager->fd;
}

/*
** Return the directory of the database file.
*/
const char *sqlite3PagerDirname(Pager *pPager){
  return pPager->zDirectory;
}

/*
** Return the full pathname of the journal file.
*/
const char *sqlite3PagerJournalname(Pager *pPager){
  return pPager->zJournal;
}

/*
** Return true if fsync() calls are disabled for this pager.  Return FALSE
** if fsync()s are executed normally.
*/
int sqlite3PagerNosync(Pager *pPager){
  return pPager->noSync;
}

#ifdef SQLITE_HAS_CODEC
/*
** Set the codec for this pager
*/
void sqlite3PagerSetCodec(
  Pager *pPager,
  void *(*xCodec)(void*,void*,Pgno,int),
  void *pCodecArg
){
  pPager->xCodec = xCodec;
  pPager->pCodecArg = pCodecArg;
}
#endif

#ifndef SQLITE_OMIT_AUTOVACUUM
/*
** Move the page pPg to location pgno in the file.
**
** There must be no references to the page previously located at
** pgno (which we call pPgOld) though that page is allowed to be
** in cache.  If the page previously located at pgno is not already
** in the rollback journal, it is not put there by by this routine.
**
** References to the page pPg remain valid. Updating any
** meta-data associated with pPg (i.e. data stored in the nExtra bytes
** allocated along with the page) is the responsibility of the caller.
**
** A transaction must be active when this routine is called. It used to be
** required that a statement transaction was not active, but this restriction
** has been removed (CREATE INDEX needs to move a page when a statement
** transaction is active).
**
** If the fourth argument, isCommit, is non-zero, then this page is being
** moved as part of a database reorganization just before the transaction 
** is being committed. In this case, it is guaranteed that the database page 
** pPg refers to will not be written to again within this transaction.
*/
int sqlite3PagerMovepage(Pager *pPager, DbPage *pPg, Pgno pgno, int isCommit){
  PgHdr *pPgOld;  /* The page being overwritten. */
  Pgno needSyncPgno = 0;

  assert( pPg->nRef>0 );

  PAGERTRACE5("MOVE %d page %d (needSync=%d) moves to %d\n", 
      PAGERID(pPager), pPg->pgno, (pPg->flags&PGHDR_NEED_SYNC)?1:0, pgno);
  IOTRACE(("MOVE %p %d %d\n", pPager, pPg->pgno, pgno))

  pager_get_content(pPg);

  /* If the journal needs to be sync()ed before page pPg->pgno can
  ** be written to, store pPg->pgno in local variable needSyncPgno.
  **
  ** If the isCommit flag is set, there is no need to remember that
  ** the journal needs to be sync()ed before database page pPg->pgno 
  ** can be written to. The caller has already promised not to write to it.
  */
  if( (pPg->flags&PGHDR_NEED_SYNC) && !isCommit ){
    needSyncPgno = pPg->pgno;
    assert( pageInJournal(pPg) || pPg->pgno>pPager->origDbSize );
    assert( pPg->flags&PGHDR_DIRTY );
    assert( pPager->needSync );
  }

  /* If the cache contains a page with page-number pgno, remove it
  ** from its hash chain. Also, if the PgHdr.needSync was set for 
  ** page pgno before the 'move' operation, it needs to be retained 
  ** for the page moved there.
  */
  pPg->flags &= ~PGHDR_NEED_SYNC;
  pPgOld = pager_lookup(pPager, pgno);
  assert( !pPgOld || pPgOld->nRef==1 );
  if( pPgOld ){
    pPg->flags |= (pPgOld->flags&PGHDR_NEED_SYNC);
  }

  sqlite3PcacheMove(pPg, pgno);
  if( pPgOld ){
    sqlite3PcacheDrop(pPgOld);
  }

  sqlite3PcacheMakeDirty(pPg);
  pPager->dirtyCache = 1;
  pPager->dbModified = 1;

  if( needSyncPgno ){
    /* If needSyncPgno is non-zero, then the journal file needs to be 
    ** sync()ed before any data is written to database file page needSyncPgno.
    ** Currently, no such page exists in the page-cache and the 
    ** "is journaled" bitvec flag has been set. This needs to be remedied by
    ** loading the page into the pager-cache and setting the PgHdr.needSync 
    ** flag.
    **
    ** If the attempt to load the page into the page-cache fails, (due
    ** to a malloc() or IO failure), clear the bit in the pInJournal[]
    ** array. Otherwise, if the page is loaded and written again in
    ** this transaction, it may be written to the database file before
    ** it is synced into the journal file. This way, it may end up in
    ** the journal file twice, but that is not a problem.
    **
    ** The sqlite3PagerGet() call may cause the journal to sync. So make
    ** sure the Pager.needSync flag is set too.
    */
    int rc;
    PgHdr *pPgHdr;
    assert( pPager->needSync );
    rc = sqlite3PagerGet(pPager, needSyncPgno, &pPgHdr);
    if( rc!=SQLITE_OK ){
      if( pPager->pInJournal && needSyncPgno<=pPager->origDbSize ){
        sqlite3BitvecClear(pPager->pInJournal, needSyncPgno);
      }
      return rc;
    }
    pPager->needSync = 1;
    assert( pPager->noSync==0 && !MEMDB );
    pPgHdr->flags |= PGHDR_NEED_SYNC;
    sqlite3PcacheMakeDirty(pPgHdr);
    sqlite3PagerUnref(pPgHdr);
  }

  return SQLITE_OK;
}
#endif

/*
** Return a pointer to the data for the specified page.
*/
void *sqlite3PagerGetData(DbPage *pPg){
  assert( pPg->nRef>0 || pPg->pPager->memDb );
  return pPg->pData;
}

/*
** Return a pointer to the Pager.nExtra bytes of "extra" space 
** allocated along with the specified page.
*/
void *sqlite3PagerGetExtra(DbPage *pPg){
  Pager *pPager = pPg->pPager;
  return (pPager?pPg->pExtra:0);
}

/*
** Get/set the locking-mode for this pager. Parameter eMode must be one
** of PAGER_LOCKINGMODE_QUERY, PAGER_LOCKINGMODE_NORMAL or 
** PAGER_LOCKINGMODE_EXCLUSIVE. If the parameter is not _QUERY, then
** the locking-mode is set to the value specified.
**
** The returned value is either PAGER_LOCKINGMODE_NORMAL or
** PAGER_LOCKINGMODE_EXCLUSIVE, indicating the current (possibly updated)
** locking-mode.
*/
int sqlite3PagerLockingMode(Pager *pPager, int eMode){
  assert( eMode==PAGER_LOCKINGMODE_QUERY
            || eMode==PAGER_LOCKINGMODE_NORMAL
            || eMode==PAGER_LOCKINGMODE_EXCLUSIVE );
  assert( PAGER_LOCKINGMODE_QUERY<0 );
  assert( PAGER_LOCKINGMODE_NORMAL>=0 && PAGER_LOCKINGMODE_EXCLUSIVE>=0 );
  if( eMode>=0 && !pPager->tempFile ){
    pPager->exclusiveMode = eMode;
  }
  return (int)pPager->exclusiveMode;
}

/*
** Get/set the journal-mode for this pager. Parameter eMode must be one of:
**
**    PAGER_JOURNALMODE_QUERY
**    PAGER_JOURNALMODE_DELETE
**    PAGER_JOURNALMODE_TRUNCATE
**    PAGER_JOURNALMODE_PERSIST
**    PAGER_JOURNALMODE_OFF
**
** If the parameter is not _QUERY, then the journal-mode is set to the
** value specified.
**
** The returned indicate the current (possibly updated)
** journal-mode.
*/
int sqlite3PagerJournalMode(Pager *pPager, int eMode){
  if( !MEMDB ){
    assert( eMode==PAGER_JOURNALMODE_QUERY
              || eMode==PAGER_JOURNALMODE_DELETE
              || eMode==PAGER_JOURNALMODE_TRUNCATE
              || eMode==PAGER_JOURNALMODE_PERSIST
              || eMode==PAGER_JOURNALMODE_OFF 
              || eMode==PAGER_JOURNALMODE_MEMORY );
    assert( PAGER_JOURNALMODE_QUERY<0 );
    if( eMode>=0 ){
      pPager->journalMode = eMode;
    }else{
      assert( eMode==PAGER_JOURNALMODE_QUERY );
    }
  }
  return (int)pPager->journalMode;
}

/*
** Get/set the size-limit used for persistent journal files.
*/
i64 sqlite3PagerJournalSizeLimit(Pager *pPager, i64 iLimit){
  if( iLimit>=-1 ){
    pPager->journalSizeLimit = iLimit;
  }
  return pPager->journalSizeLimit;
}

#endif /* SQLITE_OMIT_DISKIO */

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
** @(#) $Id: pager.c,v 1.107 2004/05/12 13:30:08 drh Exp $
*/
#include "os.h"         /* Must be first to enable large file support */
#include "sqliteInt.h"
#include "pager.h"
#include <assert.h>
#include <string.h>

/*
** Macros for troubleshooting.  Normally turned off
*/
#if 0
static Pager *mainPager = 0;
#define SET_PAGER(X)  if( mainPager==0 ) mainPager = (X)
#define CLR_PAGER(X)  if( mainPager==(X) ) mainPager = 0
#define TRACE1(X)     if( pPager==mainPager ) fprintf(stderr,X)
#define TRACE2(X,Y)   if( pPager==mainPager ) fprintf(stderr,X,Y)
#define TRACE3(X,Y,Z) if( pPager==mainPager ) fprintf(stderr,X,Y,Z)
#else
#define SET_PAGER(X)
#define CLR_PAGER(X)
#define TRACE1(X)
#define TRACE2(X,Y)
#define TRACE3(X,Y,Z)
#endif


/*
** The page cache as a whole is always in one of the following
** states:
**
**   SQLITE_UNLOCK       The page cache is not currently reading or 
**                       writing the database file.  There is no
**                       data held in memory.  This is the initial
**                       state.
**
**   SQLITE_READLOCK     The page cache is reading the database.
**                       Writing is not permitted.  There can be
**                       multiple readers accessing the same database
**                       file at the same time.
**
**   SQLITE_WRITELOCK    The page cache is writing the database.
**                       Access is exclusive.  No other processes or
**                       threads can be reading or writing while one
**                       process is writing.
**
** The page cache comes up in SQLITE_UNLOCK.  The first time a
** sqlite_page_get() occurs, the state transitions to SQLITE_READLOCK.
** After all pages have been released using sqlite_page_unref(),
** the state transitions back to SQLITE_UNLOCK.  The first time
** that sqlite_page_write() is called, the state transitions to
** SQLITE_WRITELOCK.  (Note that sqlite_page_write() can only be
** called on an outstanding page which means that the pager must
** be in SQLITE_READLOCK before it transitions to SQLITE_WRITELOCK.)
** The sqlite_page_rollback() and sqlite_page_commit() functions 
** transition the state from SQLITE_WRITELOCK back to SQLITE_READLOCK.
*/
#define SQLITE_UNLOCK      0
#define SQLITE_READLOCK    1
#define SQLITE_WRITELOCK   2


/*
** Each in-memory image of a page begins with the following header.
** This header is only visible to this pager module.  The client
** code that calls pager sees only the data that follows the header.
**
** Client code should call sqlite3pager_write() on a page prior to making
** any modifications to that page.  The first time sqlite3pager_write()
** is called, the original page contents are written into the rollback
** journal and PgHdr.inJournal and PgHdr.needSync are set.  Later, once
** the journal page has made it onto the disk surface, PgHdr.needSync
** is cleared.  The modified page cannot be written back into the original
** database file until the journal pages has been synced to disk and the
** PgHdr.needSync has been cleared.
**
** The PgHdr.dirty flag is set when sqlite3pager_write() is called and
** is cleared again when the page content is written back to the original
** database file.
*/
typedef struct PgHdr PgHdr;
struct PgHdr {
  Pager *pPager;                 /* The pager to which this page belongs */
  Pgno pgno;                     /* The page number for this page */
  PgHdr *pNextHash, *pPrevHash;  /* Hash collision chain for PgHdr.pgno */
  PgHdr *pNextFree, *pPrevFree;  /* Freelist of pages where nRef==0 */
  PgHdr *pNextAll;               /* A list of all pages */
  PgHdr *pNextStmt, *pPrevStmt;  /* List of pages in the statement journal */
  u8 inJournal;                  /* TRUE if has been written to journal */
  u8 inStmt;                     /* TRUE if in the statement subjournal */
  u8 dirty;                      /* TRUE if we need to write back changes */
  u8 needSync;                   /* Sync journal before writing this page */
  u8 alwaysRollback;             /* Disable dont_rollback() for this page */
  short int nRef;                /* Number of users of this page */
  PgHdr *pDirty;                 /* Dirty pages sorted by PgHdr.pgno */
  /* SQLITE_PAGE_SIZE bytes of page data follow this header */
  /* Pager.nExtra bytes of local data follow the page data */
};

/*
** For an in-memory only database, some extra information is recorded about
** each page so that changes can be rolled back.  (Journal files are not
** used for in-memory databases.)  The following information is added to
** the end of every EXTRA block for in-memory databases.
**
** This information could have been added directly to the PgHdr structure.
** But then it would take up an extra 8 bytes of storage on every PgHdr
** even for disk-based databases.  Splitting it out saves 8 bytes.  This
** is only a savings of 0.8% but those percentages add up.
*/
typedef struct PgHistory PgHistory;
struct PgHistory {
  u8 *pOrig;     /* Original page text.  Restore to this on a full rollback */
  u8 *pStmt;     /* Text as it was at the beginning of the current statement */
};

/*
** A macro used for invoking the codec if there is one
*/
#ifdef SQLITE_HAS_CODEC
# define CODEC(P,D,N,X) if( P->xCodec ){ P->xCodec(P->pCodecArg,D,N,X); }
#else
# define CODEC(P,D,N,X)
#endif

/*
** Convert a pointer to a PgHdr into a pointer to its data
** and back again.
*/
#define PGHDR_TO_DATA(P)  ((void*)(&(P)[1]))
#define DATA_TO_PGHDR(D)  (&((PgHdr*)(D))[-1])
#define PGHDR_TO_EXTRA(P) ((void*)&((char*)(&(P)[1]))[SQLITE_PAGE_SIZE])
#define PGHDR_TO_HIST(P,PGR)  \
            ((PgHistory*)&((char*)(&(P)[1]))[(PGR)->pageSize+(PGR)->nExtra])

/*
** How big to make the hash table used for locating in-memory pages
** by page number.
*/
#define N_PG_HASH 2048

/*
** Hash a page number
*/
#define pager_hash(PN)  ((PN)&(N_PG_HASH-1))

/*
** A open page cache is an instance of the following structure.
*/
struct Pager {
  char *zFilename;            /* Name of the database file */
  char *zJournal;             /* Name of the journal file */
  char *zDirectory;           /* Directory hold database and journal files */
  OsFile fd, jfd;             /* File descriptors for database and journal */
  OsFile stfd;                /* File descriptor for the statement subjournal*/
  int dbSize;                 /* Number of pages in the file */
  int origDbSize;             /* dbSize before the current change */
  int stmtSize;               /* Size of database (in pages) at stmt_begin() */
  off_t stmtJSize;            /* Size of journal at stmt_begin() */
  int nRec;                   /* Number of pages written to the journal */
  u32 cksumInit;              /* Quasi-random value added to every checksum */
  int stmtNRec;               /* Number of records in stmt subjournal */
  int nExtra;                 /* Add this many bytes to each in-memory page */
  void (*xDestructor)(void*); /* Call this routine when freeing pages */
  int nPage;                  /* Total number of in-memory pages */
  int nRef;                   /* Number of in-memory pages with PgHdr.nRef>0 */
  int mxPage;                 /* Maximum number of pages to hold in cache */
  int nHit, nMiss, nOvfl;     /* Cache hits, missing, and LRU overflows */
  void (*xCodec)(void*,void*,Pgno,int); /* Routine for en/decoding data */
  void *pCodecArg;            /* First argument to xCodec() */
  int pageSize;               /* Page size in bytes */
  u8 journalOpen;             /* True if journal file descriptors is valid */
  u8 journalStarted;          /* True if header of journal is synced */
  u8 useJournal;              /* Use a rollback journal on this file */
  u8 stmtOpen;                /* True if the statement subjournal is open */
  u8 stmtInUse;               /* True we are in a statement subtransaction */
  u8 stmtAutoopen;            /* Open stmt journal when main journal is opened*/
  u8 noSync;                  /* Do not sync the journal if true */
  u8 fullSync;                /* Do extra syncs of the journal for robustness */
  u8 state;                   /* SQLITE_UNLOCK, _READLOCK or _WRITELOCK */
  u8 errMask;                 /* One of several kinds of errors */
  u8 tempFile;                /* zFilename is a temporary file */
  u8 readOnly;                /* True for a read-only database */
  u8 needSync;                /* True if an fsync() is needed on the journal */
  u8 dirtyFile;               /* True if database file has changed in any way */
  u8 alwaysRollback;          /* Disable dont_rollback() for all pages */
  u8 memDb;                   /* True to inhibit all file I/O */
  u8 *aInJournal;             /* One bit for each page in the database file */
  u8 *aInStmt;                /* One bit for each page in the database */
  PgHdr *pFirst, *pLast;      /* List of free pages */
  PgHdr *pFirstSynced;        /* First free page with PgHdr.needSync==0 */
  PgHdr *pAll;                /* List of all pages */
  PgHdr *pStmt;               /* List of pages in the statement subjournal */
  PgHdr *aHash[N_PG_HASH];    /* Hash table to map page number of PgHdr */
};

/*
** These are bits that can be set in Pager.errMask.
*/
#define PAGER_ERR_FULL     0x01  /* a write() failed */
#define PAGER_ERR_MEM      0x02  /* malloc() failed */
#define PAGER_ERR_LOCK     0x04  /* error in the locking protocol */
#define PAGER_ERR_CORRUPT  0x08  /* database or journal corruption */
#define PAGER_ERR_DISK     0x10  /* general disk I/O error - bad hard drive? */

/*
** The journal file contains page records in the following
** format.
**
** Actually, this structure is the complete page record for pager
** formats less than 3.  Beginning with format 3, this record is surrounded
** by two checksums.
*/
typedef struct PageRecord PageRecord;
struct PageRecord {
  Pgno pgno;                      /* The page number */
  char aData[SQLITE_PAGE_SIZE];   /* Original data for page pgno */
};

/*
** Journal files begin with the following magic string.  The data
** was obtained from /dev/random.  It is used only as a sanity check.
**
** There are three journal formats (so far). The 1st journal format writes
** 32-bit integers in the byte-order of the host machine.  New
** formats writes integers as big-endian.  All new journals use the
** new format, but we have to be able to read an older journal in order
** to rollback journals created by older versions of the library.
**
** The 3rd journal format (added for 2.8.0) adds additional sanity
** checking information to the journal.  If the power fails while the
** journal is being written, semi-random garbage data might appear in
** the journal file after power is restored.  If an attempt is then made
** to roll the journal back, the database could be corrupted.  The additional
** sanity checking data is an attempt to discover the garbage in the
** journal and ignore it.
**
** The sanity checking information for the 3rd journal format consists
** of a 32-bit checksum on each page of data.  The checksum covers both
** the page number and the SQLITE_PAGE_SIZE bytes of data for the page.
** This cksum is initialized to a 32-bit random value that appears in the
** journal file right after the header.  The random initializer is important,
** because garbage data that appears at the end of a journal is likely
** data that was once in other files that have now been deleted.  If the
** garbage data came from an obsolete journal file, the checksums might
** be correct.  But by initializing the checksum to random value which
** is different for every journal, we minimize that risk.
*/
static const unsigned char aJournalMagic1[] = {
  0xd9, 0xd5, 0x05, 0xf9, 0x20, 0xa1, 0x63, 0xd4,
};
static const unsigned char aJournalMagic2[] = {
  0xd9, 0xd5, 0x05, 0xf9, 0x20, 0xa1, 0x63, 0xd5,
};
static const unsigned char aJournalMagic3[] = {
  0xd9, 0xd5, 0x05, 0xf9, 0x20, 0xa1, 0x63, 0xd6,
};
#define JOURNAL_FORMAT_1 1
#define JOURNAL_FORMAT_2 2
#define JOURNAL_FORMAT_3 3

/*
** The following integer determines what format to use when creating
** new primary journal files.  By default we always use format 3.
** When testing, we can set this value to older journal formats in order to
** make sure that newer versions of the library are able to rollback older
** journal files.
**
** Note that statement journals always use format 2 and omit the header.
*/
#ifdef SQLITE_TEST
int journal_format = 3;
#else
# define journal_format 3
#endif

/*
** The size of the header and of each page in the journal varies according
** to which journal format is being used.  The following macros figure out
** the sizes based on format numbers.
*/
#define JOURNAL_HDR_SZ(X) \
   (sizeof(aJournalMagic1) + sizeof(Pgno) + ((X)>=3)*2*sizeof(u32))
#define JOURNAL_PG_SZ(X) \
   (SQLITE_PAGE_SIZE + sizeof(Pgno) + ((X)>=3)*sizeof(u32))

/*
** Enable reference count tracking here:
*/
#ifdef SQLITE_TEST
  int pager3_refinfo_enable = 0;
  static void pager_refinfo(PgHdr *p){
    static int cnt = 0;
    if( !pager3_refinfo_enable ) return;
    printf(
       "REFCNT: %4d addr=0x%08x nRef=%d\n",
       p->pgno, (int)PGHDR_TO_DATA(p), p->nRef
    );
    cnt++;   /* Something to set a breakpoint on */
  }
# define REFINFO(X)  pager_refinfo(X)
#else
# define REFINFO(X)
#endif

/*
** Read a 32-bit integer from the given file descriptor.  Store the integer
** that is read in *pRes.  Return SQLITE_OK if everything worked, or an
** error code is something goes wrong.
**
** If the journal format is 2 or 3, read a big-endian integer.  If the
** journal format is 1, read an integer in the native byte-order of the
** host machine.
*/
static int read32bits(int format, OsFile *fd, u32 *pRes){
  u32 res;
  int rc;
  rc = sqlite3OsRead(fd, &res, sizeof(res));
  if( rc==SQLITE_OK && format>JOURNAL_FORMAT_1 ){
    unsigned char ac[4];
    memcpy(ac, &res, 4);
    res = (ac[0]<<24) | (ac[1]<<16) | (ac[2]<<8) | ac[3];
  }
  *pRes = res;
  return rc;
}

/*
** Write a 32-bit integer into the given file descriptor.  Return SQLITE_OK
** on success or an error code is something goes wrong.
**
** If the journal format is 2 or 3, write the integer as 4 big-endian
** bytes.  If the journal format is 1, write the integer in the native
** byte order.  In normal operation, only formats 2 and 3 are used.
** Journal format 1 is only used for testing.
*/
static int write32bits(OsFile *fd, u32 val){
  unsigned char ac[4];
  if( journal_format<=1 ){
    return sqlite3OsWrite(fd, &val, 4);
  }
  ac[0] = (val>>24) & 0xff;
  ac[1] = (val>>16) & 0xff;
  ac[2] = (val>>8) & 0xff;
  ac[3] = val & 0xff;
  return sqlite3OsWrite(fd, ac, 4);
}

/*
** Write a 32-bit integer into a page header right before the
** page data.  This will overwrite the PgHdr.pDirty pointer.
**
** The integer is big-endian for formats 2 and 3 and native byte order
** for journal format 1.
*/
static void store32bits(u32 val, PgHdr *p, int offset){
  unsigned char *ac;
  ac = &((unsigned char*)PGHDR_TO_DATA(p))[offset];
  if( journal_format<=1 ){
    memcpy(ac, &val, 4);
  }else{
    ac[0] = (val>>24) & 0xff;
    ac[1] = (val>>16) & 0xff;
    ac[2] = (val>>8) & 0xff;
    ac[3] = val & 0xff;
  }
}


/*
** Convert the bits in the pPager->errMask into an approprate
** return code.
*/
static int pager_errcode(Pager *pPager){
  int rc = SQLITE_OK;
  if( pPager->errMask & PAGER_ERR_LOCK )    rc = SQLITE_PROTOCOL;
  if( pPager->errMask & PAGER_ERR_DISK )    rc = SQLITE_IOERR;
  if( pPager->errMask & PAGER_ERR_FULL )    rc = SQLITE_FULL;
  if( pPager->errMask & PAGER_ERR_MEM )     rc = SQLITE_NOMEM;
  if( pPager->errMask & PAGER_ERR_CORRUPT ) rc = SQLITE_CORRUPT;
  return rc;
}

/*
** Add or remove a page from the list of all pages that are in the
** statement journal.
**
** The Pager keeps a separate list of pages that are currently in
** the statement journal.  This helps the sqlite3pager_stmt_commit()
** routine run MUCH faster for the common case where there are many
** pages in memory but only a few are in the statement journal.
*/
static void page_add_to_stmt_list(PgHdr *pPg){
  Pager *pPager = pPg->pPager;
  if( pPg->inStmt ) return;
  assert( pPg->pPrevStmt==0 && pPg->pNextStmt==0 );
  pPg->pPrevStmt = 0;
  if( pPager->pStmt ){
    pPager->pStmt->pPrevStmt = pPg;
  }
  pPg->pNextStmt = pPager->pStmt;
  pPager->pStmt = pPg;
  pPg->inStmt = 1;
}
static void page_remove_from_stmt_list(PgHdr *pPg){
  if( !pPg->inStmt ) return;
  if( pPg->pPrevStmt ){
    assert( pPg->pPrevStmt->pNextStmt==pPg );
    pPg->pPrevStmt->pNextStmt = pPg->pNextStmt;
  }else{
    assert( pPg->pPager->pStmt==pPg );
    pPg->pPager->pStmt = pPg->pNextStmt;
  }
  if( pPg->pNextStmt ){
    assert( pPg->pNextStmt->pPrevStmt==pPg );
    pPg->pNextStmt->pPrevStmt = pPg->pPrevStmt;
  }
  pPg->pNextStmt = 0;
  pPg->pPrevStmt = 0;
  pPg->inStmt = 0;
}

/*
** Find a page in the hash table given its page number.  Return
** a pointer to the page or NULL if not found.
*/
static PgHdr *pager_lookup(Pager *pPager, Pgno pgno){
  PgHdr *p = pPager->aHash[pager_hash(pgno)];
  while( p && p->pgno!=pgno ){
    p = p->pNextHash;
  }
  return p;
}

/*
** Unlock the database and clear the in-memory cache.  This routine
** sets the state of the pager back to what it was when it was first
** opened.  Any outstanding pages are invalidated and subsequent attempts
** to access those pages will likely result in a coredump.
*/
static void pager_reset(Pager *pPager){
  PgHdr *pPg, *pNext;
  for(pPg=pPager->pAll; pPg; pPg=pNext){
    pNext = pPg->pNextAll;
    sqliteFree(pPg);
  }
  pPager->pFirst = 0;
  pPager->pFirstSynced = 0;
  pPager->pLast = 0;
  pPager->pAll = 0;
  memset(pPager->aHash, 0, sizeof(pPager->aHash));
  pPager->nPage = 0;
  if( pPager->state>=SQLITE_WRITELOCK ){
    sqlite3pager_rollback(pPager);
  }
  sqlite3OsUnlock(&pPager->fd);
  pPager->state = SQLITE_UNLOCK;
  pPager->dbSize = -1;
  pPager->nRef = 0;
  assert( pPager->journalOpen==0 );
}

/*
** When this routine is called, the pager has the journal file open and
** a write lock on the database.  This routine releases the database
** write lock and acquires a read lock in its place.  The journal file
** is deleted and closed.
**
** TODO: Consider keeping the journal file open for temporary databases.
** This might give a performance improvement on windows where opening
** a file is an expensive operation.
*/
static int pager_unwritelock(Pager *pPager){
  int rc;
  PgHdr *pPg;
  if( pPager->state<SQLITE_WRITELOCK ) return SQLITE_OK;
  sqlite3pager_stmt_commit(pPager);
  if( pPager->stmtOpen ){
    sqlite3OsClose(&pPager->stfd);
    pPager->stmtOpen = 0;
  }
  if( pPager->journalOpen ){
    sqlite3OsClose(&pPager->jfd);
    pPager->journalOpen = 0;
    sqlite3OsDelete(pPager->zJournal);
    sqliteFree( pPager->aInJournal );
    pPager->aInJournal = 0;
    for(pPg=pPager->pAll; pPg; pPg=pPg->pNextAll){
      pPg->inJournal = 0;
      pPg->dirty = 0;
      pPg->needSync = 0;
    }
  }else{
    assert( pPager->dirtyFile==0 || pPager->useJournal==0 );
  }
  rc = sqlite3OsReadLock(&pPager->fd);
  if( rc==SQLITE_OK ){
    pPager->state = SQLITE_READLOCK;
  }else{
    /* This can only happen if a process does a BEGIN, then forks and the
    ** child process does the COMMIT.  Because of the semantics of unix
    ** file locking, the unlock will fail.
    */
    pPager->state = SQLITE_UNLOCK;
  }
  return rc;
}

/*
** Compute and return a checksum for the page of data.
**
** This is not a real checksum.  It is really just the sum of the 
** random initial value and the page number.  We considered do a checksum
** of the database, but that was found to be too slow.
*/
static u32 pager_cksum(Pager *pPager, Pgno pgno, const char *aData){
  u32 cksum = pPager->cksumInit + pgno;
  return cksum;
}

/*
** Read a single page from the journal file opened on file descriptor
** jfd.  Playback this one page.
**
** There are three different journal formats.  The format parameter determines
** which format is used by the journal that is played back.
*/
static int pager_playback_one_page(Pager *pPager, OsFile *jfd, int format){
  int rc;
  PgHdr *pPg;              /* An existing page in the cache */
  PageRecord pgRec;
  u32 cksum;

  rc = read32bits(format, jfd, &pgRec.pgno);
  if( rc!=SQLITE_OK ) return rc;
  rc = sqlite3OsRead(jfd, &pgRec.aData, sizeof(pgRec.aData));
  if( rc!=SQLITE_OK ) return rc;

  /* Sanity checking on the page.  This is more important that I originally
  ** thought.  If a power failure occurs while the journal is being written,
  ** it could cause invalid data to be written into the journal.  We need to
  ** detect this invalid data (with high probability) and ignore it.
  */
  if( pgRec.pgno==0 ){
    return SQLITE_DONE;
  }
  if( pgRec.pgno>(unsigned)pPager->dbSize ){
    return SQLITE_OK;
  }
  if( format>=JOURNAL_FORMAT_3 ){
    rc = read32bits(format, jfd, &cksum);
    if( rc ) return rc;
    if( pager_cksum(pPager, pgRec.pgno, pgRec.aData)!=cksum ){
      return SQLITE_DONE;
    }
  }

  /* Playback the page.  Update the in-memory copy of the page
  ** at the same time, if there is one.
  */
  pPg = pager_lookup(pPager, pgRec.pgno);
  TRACE2("PLAYBACK %d\n", pgRec.pgno);
  sqlite3OsSeek(&pPager->fd, (pgRec.pgno-1)*(off_t)SQLITE_PAGE_SIZE);
  rc = sqlite3OsWrite(&pPager->fd, pgRec.aData, SQLITE_PAGE_SIZE);
  if( pPg ){
    /* No page should ever be rolled back that is in use, except for page
    ** 1 which is held in use in order to keep the lock on the database
    ** active.
    */
    assert( pPg->nRef==0 || pPg->pgno==1 );
    memcpy(PGHDR_TO_DATA(pPg), pgRec.aData, SQLITE_PAGE_SIZE);
    if( pPager->xDestructor ){
      pPager->xDestructor(PGHDR_TO_DATA(pPg));
    }
    pPg->dirty = 0;
    pPg->needSync = 0;
    CODEC(pPager, PGHDR_TO_DATA(pPg), pPg->pgno, 3);
  }
  return rc;
}

/*
** Playback the journal and thus restore the database file to
** the state it was in before we started making changes.  
**
** The journal file format is as follows: 
**
**    *  8 byte prefix.  One of the aJournalMagic123 vectors defined
**       above.  The format of the journal file is determined by which
**       of the three prefix vectors is seen.
**    *  4 byte big-endian integer which is the number of valid page records
**       in the journal.  If this value is 0xffffffff, then compute the
**       number of page records from the journal size.  This field appears
**       in format 3 only.
**    *  4 byte big-endian integer which is the initial value for the 
**       sanity checksum.  This field appears in format 3 only.
**    *  4 byte integer which is the number of pages to truncate the
**       database to during a rollback.
**    *  Zero or more pages instances, each as follows:
**        +  4 byte page number.
**        +  SQLITE_PAGE_SIZE bytes of data.
**        +  4 byte checksum (format 3 only)
**
** When we speak of the journal header, we mean the first 4 bullets above.
** Each entry in the journal is an instance of the 5th bullet.  Note that
** bullets 2 and 3 only appear in format-3 journals.
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
** Journal formats 1 and 2 do not have an nRec value in the header so we
** have to compute nRec from the file size.  This has risks (as described
** above) which is why all persistent tables have been changed to use
** format 3.
**
** If the file opened as the journal file is not a well-formed
** journal file then the database will likely already be
** corrupted, so the PAGER_ERR_CORRUPT bit is set in pPager->errMask
** and SQLITE_CORRUPT is returned.  If it all works, then this routine
** returns SQLITE_OK.
*/
static int pager_playback(Pager *pPager, int useJournalSize){
  off_t szJ;               /* Size of the journal file in bytes */
  int nRec;                /* Number of Records in the journal */
  int i;                   /* Loop counter */
  Pgno mxPg = 0;           /* Size of the original file in pages */
  int format;              /* Format of the journal file. */
  unsigned char aMagic[sizeof(aJournalMagic1)];
  int rc;

  /* Figure out how many records are in the journal.  Abort early if
  ** the journal is empty.
  */
  assert( pPager->journalOpen );
  sqlite3OsSeek(&pPager->jfd, 0);
  rc = sqlite3OsFileSize(&pPager->jfd, &szJ);
  if( rc!=SQLITE_OK ){
    goto end_playback;
  }

  /* If the journal file is too small to contain a complete header,
  ** it must mean that the process that created the journal was just
  ** beginning to write the journal file when it died.  In that case,
  ** the database file should have still been completely unchanged.
  ** Nothing needs to be rolled back.  We can safely ignore this journal.
  */
  if( szJ < sizeof(aMagic)+sizeof(Pgno) ){
    goto end_playback;
  }

  /* Read the beginning of the journal and truncate the
  ** database file back to its original size.
  */
  rc = sqlite3OsRead(&pPager->jfd, aMagic, sizeof(aMagic));
  if( rc!=SQLITE_OK ){
    rc = SQLITE_PROTOCOL;
    goto end_playback;
  }
  if( memcmp(aMagic, aJournalMagic3, sizeof(aMagic))==0 ){
    format = JOURNAL_FORMAT_3;
  }else if( memcmp(aMagic, aJournalMagic2, sizeof(aMagic))==0 ){
    format = JOURNAL_FORMAT_2;
  }else if( memcmp(aMagic, aJournalMagic1, sizeof(aMagic))==0 ){
    format = JOURNAL_FORMAT_1;
  }else{
    rc = SQLITE_PROTOCOL;
    goto end_playback;
  }
  if( format>=JOURNAL_FORMAT_3 ){
    if( szJ < sizeof(aMagic) + 3*sizeof(u32) ){
      /* Ignore the journal if it is too small to contain a complete
      ** header.  We already did this test once above, but at the prior
      ** test, we did not know the journal format and so we had to assume
      ** the smallest possible header.  Now we know the header is bigger
      ** than the minimum so we test again.
      */
      goto end_playback;
    }
    rc = read32bits(format, &pPager->jfd, (u32*)&nRec);
    if( rc ) goto end_playback;
    rc = read32bits(format, &pPager->jfd, &pPager->cksumInit);
    if( rc ) goto end_playback;
    if( nRec==0xffffffff || useJournalSize ){
      nRec = (szJ - JOURNAL_HDR_SZ(3))/JOURNAL_PG_SZ(3);
    }
  }else{
    nRec = (szJ - JOURNAL_HDR_SZ(2))/JOURNAL_PG_SZ(2);
    assert( nRec*JOURNAL_PG_SZ(2)+JOURNAL_HDR_SZ(2)==szJ );
  }
  rc = read32bits(format, &pPager->jfd, &mxPg);
  if( rc!=SQLITE_OK ){
    goto end_playback;
  }
  assert( pPager->origDbSize==0 || pPager->origDbSize==mxPg );
  rc = sqlite3OsTruncate(&pPager->fd, SQLITE_PAGE_SIZE*(off_t)mxPg);
  if( rc!=SQLITE_OK ){
    goto end_playback;
  }
  pPager->dbSize = mxPg;
  
  /* Copy original pages out of the journal and back into the database file.
  */
  for(i=0; i<nRec; i++){
    rc = pager_playback_one_page(pPager, &pPager->jfd, format);
    if( rc!=SQLITE_OK ){
      if( rc==SQLITE_DONE ){
        rc = SQLITE_OK;
      }
      break;
    }
  }

  /* Pages that have been written to the journal but never synced
  ** where not restored by the loop above.  We have to restore those
  ** pages by reading them back from the original database.
  */
  if( rc==SQLITE_OK ){
    PgHdr *pPg;
    for(pPg=pPager->pAll; pPg; pPg=pPg->pNextAll){
      char zBuf[SQLITE_PAGE_SIZE];
      if( !pPg->dirty ) continue;
      if( (int)pPg->pgno <= pPager->origDbSize ){
        sqlite3OsSeek(&pPager->fd, SQLITE_PAGE_SIZE*(off_t)(pPg->pgno-1));
        rc = sqlite3OsRead(&pPager->fd, zBuf, SQLITE_PAGE_SIZE);
        TRACE2("REFETCH %d\n", pPg->pgno);
        CODEC(pPager, zBuf, pPg->pgno, 2);
        if( rc ) break;
      }else{
        memset(zBuf, 0, SQLITE_PAGE_SIZE);
      }
      if( pPg->nRef==0 || memcmp(zBuf, PGHDR_TO_DATA(pPg), SQLITE_PAGE_SIZE) ){
        memcpy(PGHDR_TO_DATA(pPg), zBuf, SQLITE_PAGE_SIZE);
        memset(PGHDR_TO_EXTRA(pPg), 0, pPager->nExtra);
      }
      pPg->needSync = 0;
      pPg->dirty = 0;
    }
  }

end_playback:
  if( rc!=SQLITE_OK ){
    pager_unwritelock(pPager);
    pPager->errMask |= PAGER_ERR_CORRUPT;
    rc = SQLITE_CORRUPT;
  }else{
    rc = pager_unwritelock(pPager);
  }
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
  off_t szJ;               /* Size of the full journal */
  int nRec;                /* Number of Records */
  int i;                   /* Loop counter */
  int rc;

  /* Truncate the database back to its original size.
  */
  rc = sqlite3OsTruncate(&pPager->fd, SQLITE_PAGE_SIZE*(off_t)pPager->stmtSize);
  pPager->dbSize = pPager->stmtSize;

  /* Figure out how many records are in the statement journal.
  */
  assert( pPager->stmtInUse && pPager->journalOpen );
  sqlite3OsSeek(&pPager->stfd, 0);
  nRec = pPager->stmtNRec;
  
  /* Copy original pages out of the statement journal and back into the
  ** database file.  Note that the statement journal always uses format
  ** 2 instead of format 3 since it does not need to be concerned with
  ** power failures corrupting the journal and can thus omit the checksums.
  */
  for(i=nRec-1; i>=0; i--){
    rc = pager_playback_one_page(pPager, &pPager->stfd, 2);
    assert( rc!=SQLITE_DONE );
    if( rc!=SQLITE_OK ) goto end_stmt_playback;
  }

  /* Figure out how many pages need to be copied out of the transaction
  ** journal.
  */
  rc = sqlite3OsSeek(&pPager->jfd, pPager->stmtJSize);
  if( rc!=SQLITE_OK ){
    goto end_stmt_playback;
  }
  rc = sqlite3OsFileSize(&pPager->jfd, &szJ);
  if( rc!=SQLITE_OK ){
    goto end_stmt_playback;
  }
  nRec = (szJ - pPager->stmtJSize)/JOURNAL_PG_SZ(journal_format);
  for(i=nRec-1; i>=0; i--){
    rc = pager_playback_one_page(pPager, &pPager->jfd, journal_format);
    if( rc!=SQLITE_OK ){
      assert( rc!=SQLITE_DONE );
      goto end_stmt_playback;
    }
  }
  
end_stmt_playback:
  if( rc!=SQLITE_OK ){
    pPager->errMask |= PAGER_ERR_CORRUPT;
    rc = SQLITE_CORRUPT;
  }
  return rc;
}

/*
** Change the maximum number of in-memory pages that are allowed.
**
** The maximum number is the absolute value of the mxPage parameter.
** If mxPage is negative, the noSync flag is also set.  noSync bypasses
** calls to sqlite3OsSync().  The pager runs much faster with noSync on,
** but if the operating system crashes or there is an abrupt power 
** failure, the database file might be left in an inconsistent and
** unrepairable state.  
*/
void sqlite3pager_set_cachesize(Pager *pPager, int mxPage){
  if( mxPage>=0 ){
    pPager->noSync = pPager->tempFile;
    if( pPager->noSync==0 ) pPager->needSync = 0;
  }else{
    pPager->noSync = 1;
    mxPage = -mxPage;
  }
  if( mxPage>10 ){
    pPager->mxPage = mxPage;
  }
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
void sqlite3pager_set_safety_level(Pager *pPager, int level){
  pPager->noSync =  level==1 || pPager->tempFile;
  pPager->fullSync = level==3 && !pPager->tempFile;
  if( pPager->noSync==0 ) pPager->needSync = 0;
}

/*
** Open a temporary file.  Write the name of the file into zName
** (zName must be at least SQLITE_TEMPNAME_SIZE bytes long.)  Write
** the file descriptor into *fd.  Return SQLITE_OK on success or some
** other error code if we fail.
**
** The OS will automatically delete the temporary file when it is
** closed.
*/
static int sqlite3pager_opentemp(char *zFile, OsFile *fd){
  int cnt = 8;
  int rc;
  do{
    cnt--;
    sqlite3OsTempFileName(zFile);
    rc = sqlite3OsOpenExclusive(zFile, fd, 1);
  }while( cnt>0 && rc!=SQLITE_OK );
  return rc;
}

/*
** Create a new page cache and put a pointer to the page cache in *ppPager.
** The file to be cached need not exist.  The file is not locked until
** the first call to sqlite3pager_get() and is only held open until the
** last page is released using sqlite3pager_unref().
**
** If zFilename is NULL then a randomly-named temporary file is created
** and used as the file to be cached.  The file will be deleted
** automatically when it is closed.
*/
int sqlite3pager_open(
  Pager **ppPager,         /* Return the Pager structure here */
  const char *zFilename,   /* Name of the database file to open */
  int mxPage,              /* Max number of in-memory cache pages */
  int nExtra,              /* Extra bytes append to each in-memory page */
  int useJournal           /* TRUE to use a rollback journal on this file */
){
  Pager *pPager;
  char *zFullPathname;
  int nameLen;
  OsFile fd;
  int rc, i;
  int tempFile;
  int memDb = 0;
  int readOnly = 0;
  char zTemp[SQLITE_TEMPNAME_SIZE];

  *ppPager = 0;
  if( sqlite3_malloc_failed ){
    return SQLITE_NOMEM;
  }
  if( zFilename && zFilename[0] ){
    if( strcmp(zFilename,":memory:")==0 ){
      memDb = 1;
      zFullPathname = sqliteMalloc(4);
      if( zFullPathname ) strcpy(zFullPathname, "nil");
      rc = SQLITE_OK;
    }else{
      zFullPathname = sqlite3OsFullPathname(zFilename);
      rc = sqlite3OsOpenReadWrite(zFullPathname, &fd, &readOnly);
      tempFile = 0;
    }
  }else{
    rc = sqlite3pager_opentemp(zTemp, &fd);
    zFilename = zTemp;
    zFullPathname = sqlite3OsFullPathname(zFilename);
    tempFile = 1;
  }
  if( sqlite3_malloc_failed ){
    return SQLITE_NOMEM;
  }
  if( rc!=SQLITE_OK ){
    sqliteFree(zFullPathname);
    return SQLITE_CANTOPEN;
  }
  nameLen = strlen(zFullPathname);
  pPager = sqliteMalloc( sizeof(*pPager) + nameLen*3 + 30 );
  if( pPager==0 ){
    sqlite3OsClose(&fd);
    sqliteFree(zFullPathname);
    return SQLITE_NOMEM;
  }
  SET_PAGER(pPager);
  pPager->zFilename = (char*)&pPager[1];
  pPager->zDirectory = &pPager->zFilename[nameLen+1];
  pPager->zJournal = &pPager->zDirectory[nameLen+1];
  strcpy(pPager->zFilename, zFullPathname);
  strcpy(pPager->zDirectory, zFullPathname);
  for(i=nameLen; i>0 && pPager->zDirectory[i-1]!='/'; i--){}
  if( i>0 ) pPager->zDirectory[i-1] = 0;
  strcpy(pPager->zJournal, zFullPathname);
  sqliteFree(zFullPathname);
  strcpy(&pPager->zJournal[nameLen], "-journal");
  pPager->fd = fd;
  pPager->journalOpen = 0;
  pPager->useJournal = useJournal && !memDb;
  pPager->stmtOpen = 0;
  pPager->stmtInUse = 0;
  pPager->nRef = 0;
  pPager->dbSize = memDb-1;
  pPager->pageSize = SQLITE_PAGE_SIZE;
  pPager->stmtSize = 0;
  pPager->stmtJSize = 0;
  pPager->nPage = 0;
  pPager->mxPage = mxPage>5 ? mxPage : 10;
  pPager->state = SQLITE_UNLOCK;
  pPager->errMask = 0;
  pPager->tempFile = tempFile;
  pPager->memDb = memDb;
  pPager->readOnly = readOnly;
  pPager->needSync = 0;
  pPager->noSync = pPager->tempFile || !useJournal;
  pPager->pFirst = 0;
  pPager->pFirstSynced = 0;
  pPager->pLast = 0;
  pPager->nExtra = nExtra;
  memset(pPager->aHash, 0, sizeof(pPager->aHash));
  *ppPager = pPager;
  return SQLITE_OK;
}

/*
** Set the destructor for this pager.  If not NULL, the destructor is called
** when the reference count on each page reaches zero.  The destructor can
** be used to clean up information in the extra segment appended to each page.
**
** The destructor is not called as a result sqlite3pager_close().  
** Destructors are only called by sqlite3pager_unref().
*/
void sqlite3pager_set_destructor(Pager *pPager, void (*xDesc)(void*)){
  pPager->xDestructor = xDesc;
}

/*
** Return the total number of pages in the disk file associated with
** pPager.
*/
int sqlite3pager_pagecount(Pager *pPager){
  off_t n;
  assert( pPager!=0 );
  if( pPager->dbSize>=0 ){
    return pPager->dbSize;
  }
  if( sqlite3OsFileSize(&pPager->fd, &n)!=SQLITE_OK ){
    pPager->errMask |= PAGER_ERR_DISK;
    return 0;
  }
  n /= SQLITE_PAGE_SIZE;
  if( pPager->state!=SQLITE_UNLOCK ){
    pPager->dbSize = n;
  }
  return n;
}

/*
** Forward declaration
*/
static int syncJournal(Pager*);


/*
** Unlink a page from the free list (the list of all pages where nRef==0)
** and from its hash collision chain.
*/
static void unlinkPage(PgHdr *pPg){
  Pager *pPager = pPg->pPager;

  /* Keep the pFirstSynced pointer pointing at the first synchronized page */
  if( pPg==pPager->pFirstSynced ){
    PgHdr *p = pPg->pNextFree;
    while( p && p->needSync ){ p = p->pNextFree; }
    pPager->pFirstSynced = p;
  }

  /* Unlink from the freelist */
  if( pPg->pPrevFree ){
    pPg->pPrevFree->pNextFree = pPg->pNextFree;
  }else{
    assert( pPager->pFirst==pPg );
    pPager->pFirst = pPg->pNextFree;
  }
  if( pPg->pNextFree ){
    pPg->pNextFree->pPrevFree = pPg->pPrevFree;
  }else{
    assert( pPager->pLast==pPg );
    pPager->pLast = pPg->pPrevFree;
  }
  pPg->pNextFree = pPg->pPrevFree = 0;

  /* Unlink from the pgno hash table */
  if( pPg->pNextHash ){
    pPg->pNextHash->pPrevHash = pPg->pPrevHash;
  }
  if( pPg->pPrevHash ){
    pPg->pPrevHash->pNextHash = pPg->pNextHash;
  }else{
    int h = pager_hash(pPg->pgno);
    assert( pPager->aHash[h]==pPg );
    pPager->aHash[h] = pPg->pNextHash;
  }
  pPg->pNextHash = pPg->pPrevHash = 0;
}

/*
** This routine is used to truncate an in-memory database.  Delete
** every pages whose pgno is larger than pPager->dbSize and is unreferenced.
** Referenced pages larger than pPager->dbSize are zeroed.
*/
static void memoryTruncate(Pager *pPager){
  PgHdr *pPg;
  PgHdr **ppPg;
  int dbSize = pPager->dbSize;

  ppPg = &pPager->pAll;
  while( (pPg = *ppPg)!=0 ){
    if( pPg->pgno<=dbSize ){
      ppPg = &pPg->pNextAll;
    }else if( pPg->nRef>0 ){
      memset(PGHDR_TO_DATA(pPg), 0, pPager->pageSize);
      ppPg = &pPg->pNextAll;
    }else{
      *ppPg = pPg->pNextAll;
      unlinkPage(pPg);
      sqliteFree(pPg);
      pPager->nPage--;
    }
  }
}

/*
** Truncate the file to the number of pages specified.
*/
int sqlite3pager_truncate(Pager *pPager, Pgno nPage){
  int rc;
  if( pPager->dbSize<0 ){
    sqlite3pager_pagecount(pPager);
  }
  if( pPager->errMask!=0 ){
    rc = pager_errcode(pPager);
    return rc;
  }
  if( nPage>=(unsigned)pPager->dbSize ){
    return SQLITE_OK;
  }
  if( pPager->memDb ){
    pPager->dbSize = nPage;
    memoryTruncate(pPager);
    return SQLITE_OK;
  }
  syncJournal(pPager);
  rc = sqlite3OsTruncate(&pPager->fd, SQLITE_PAGE_SIZE*(off_t)nPage);
  if( rc==SQLITE_OK ){
    pPager->dbSize = nPage;
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
*/
int sqlite3pager_close(Pager *pPager){
  PgHdr *pPg, *pNext;
  switch( pPager->state ){
    case SQLITE_WRITELOCK: {
      sqlite3pager_rollback(pPager);
      if( !pPager->memDb ){
        sqlite3OsUnlock(&pPager->fd);
      }
      assert( pPager->journalOpen==0 );
      break;
    }
    case SQLITE_READLOCK: {
      if( !pPager->memDb ){
        sqlite3OsUnlock(&pPager->fd);
      }
      break;
    }
    default: {
      /* Do nothing */
      break;
    }
  }
  for(pPg=pPager->pAll; pPg; pPg=pNext){
    pNext = pPg->pNextAll;
    sqliteFree(pPg);
  }
  if( !pPager->memDb ){
    sqlite3OsClose(&pPager->fd);
  }
  assert( pPager->journalOpen==0 );
  /* Temp files are automatically deleted by the OS
  ** if( pPager->tempFile ){
  **   sqlite3OsDelete(pPager->zFilename);
  ** }
  */
  CLR_PAGER(pPager);
  if( pPager->zFilename!=(char*)&pPager[1] ){
    assert( 0 );  /* Cannot happen */
    sqliteFree(pPager->zFilename);
    sqliteFree(pPager->zJournal);
    sqliteFree(pPager->zDirectory);
  }
  sqliteFree(pPager);
  return SQLITE_OK;
}

/*
** Return the page number for the given page data.
*/
Pgno sqlite3pager_pagenumber(void *pData){
  PgHdr *p = DATA_TO_PGHDR(pData);
  return p->pgno;
}

/*
** The page_ref() function increments the reference count for a page.
** If the page is currently on the freelist (the reference count is zero) then
** remove it from the freelist.
**
** For non-test systems, page_ref() is a macro that calls _page_ref()
** online of the reference count is zero.  For test systems, page_ref()
** is a real function so that we can set breakpoints and trace it.
*/
static void _page_ref(PgHdr *pPg){
  if( pPg->nRef==0 ){
    /* The page is currently on the freelist.  Remove it. */
    if( pPg==pPg->pPager->pFirstSynced ){
      PgHdr *p = pPg->pNextFree;
      while( p && p->needSync ){ p = p->pNextFree; }
      pPg->pPager->pFirstSynced = p;
    }
    if( pPg->pPrevFree ){
      pPg->pPrevFree->pNextFree = pPg->pNextFree;
    }else{
      pPg->pPager->pFirst = pPg->pNextFree;
    }
    if( pPg->pNextFree ){
      pPg->pNextFree->pPrevFree = pPg->pPrevFree;
    }else{
      pPg->pPager->pLast = pPg->pPrevFree;
    }
    pPg->pPager->nRef++;
  }
  pPg->nRef++;
  REFINFO(pPg);
}
#ifdef SQLITE_TEST
  static void page_ref(PgHdr *pPg){
    if( pPg->nRef==0 ){
      _page_ref(pPg);
    }else{
      pPg->nRef++;
      REFINFO(pPg);
    }
  }
#else
# define page_ref(P)   ((P)->nRef==0?_page_ref(P):(void)(P)->nRef++)
#endif

/*
** Increment the reference count for a page.  The input pointer is
** a reference to the page data.
*/
int sqlite3pager_ref(void *pData){
  PgHdr *pPg = DATA_TO_PGHDR(pData);
  page_ref(pPg);
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
** after a power failure, so sync occurs.
**
** This routine clears the needSync field of every page current held in
** memory.
*/
static int syncJournal(Pager *pPager){
  PgHdr *pPg;
  int rc = SQLITE_OK;

  /* Sync the journal before modifying the main database
  ** (assuming there is a journal and it needs to be synced.)
  */
  if( pPager->needSync ){
    if( !pPager->tempFile ){
      assert( pPager->journalOpen );
      /* assert( !pPager->noSync ); // noSync might be set if synchronous
      ** was turned off after the transaction was started.  Ticket #615 */
#ifndef NDEBUG
      {
        /* Make sure the pPager->nRec counter we are keeping agrees
        ** with the nRec computed from the size of the journal file.
        */
        off_t hdrSz, pgSz, jSz;
        hdrSz = JOURNAL_HDR_SZ(journal_format);
        pgSz = JOURNAL_PG_SZ(journal_format);
        rc = sqlite3OsFileSize(&pPager->jfd, &jSz);
        if( rc!=0 ) return rc;
        assert( pPager->nRec*pgSz+hdrSz==jSz );
      }
#endif
      if( journal_format>=3 ){
        /* Write the nRec value into the journal file header */
        off_t szJ;
        if( pPager->fullSync ){
          TRACE1("SYNC\n");
          rc = sqlite3OsSync(&pPager->jfd);
          if( rc!=0 ) return rc;
        }
        sqlite3OsSeek(&pPager->jfd, sizeof(aJournalMagic1));
        rc = write32bits(&pPager->jfd, pPager->nRec);
        if( rc ) return rc;
        szJ = JOURNAL_HDR_SZ(journal_format) +
                 pPager->nRec*JOURNAL_PG_SZ(journal_format);
        sqlite3OsSeek(&pPager->jfd, szJ);
      }
      TRACE1("SYNC\n");
      rc = sqlite3OsSync(&pPager->jfd);
      if( rc!=0 ) return rc;
      pPager->journalStarted = 1;
    }
    pPager->needSync = 0;

    /* Erase the needSync flag from every page.
    */
    for(pPg=pPager->pAll; pPg; pPg=pPg->pNextAll){
      pPg->needSync = 0;
    }
    pPager->pFirstSynced = pPager->pFirst;
  }

#ifndef NDEBUG
  /* If the Pager.needSync flag is clear then the PgHdr.needSync
  ** flag must also be clear for all pages.  Verify that this
  ** invariant is true.
  */
  else{
    for(pPg=pPager->pAll; pPg; pPg=pPg->pNextAll){
      assert( pPg->needSync==0 );
    }
    assert( pPager->pFirstSynced==pPager->pFirst );
  }
#endif

  return rc;
}

/*
** Given a list of pages (connected by the PgHdr.pDirty pointer) write
** every one of those pages out to the database file and mark them all
** as clean.
*/
static int pager_write_pagelist(PgHdr *pList){
  Pager *pPager;
  int rc;

  if( pList==0 ) return SQLITE_OK;
  pPager = pList->pPager;
  while( pList ){
    assert( pList->dirty );
    sqlite3OsSeek(&pPager->fd, (pList->pgno-1)*(off_t)SQLITE_PAGE_SIZE);
    CODEC(pPager, PGHDR_TO_DATA(pList), pList->pgno, 6);
    TRACE2("STORE %d\n", pList->pgno);
    rc = sqlite3OsWrite(&pPager->fd, PGHDR_TO_DATA(pList), SQLITE_PAGE_SIZE);
    CODEC(pPager, PGHDR_TO_DATA(pList), pList->pgno, 0);
    if( rc ) return rc;
    pList->dirty = 0;
    pList = pList->pDirty;
  }
  return SQLITE_OK;
}

/*
** Collect every dirty page into a dirty list and
** return a pointer to the head of that list.  All pages are
** collected even if they are still in use.
*/
static PgHdr *pager_get_all_dirty_pages(Pager *pPager){
  PgHdr *p, *pList;
  pList = 0;
  for(p=pPager->pAll; p; p=p->pNextAll){
    if( p->dirty ){
      p->pDirty = pList;
      pList = p;
    }
  }
  return pList;
}

/*
** Acquire a page.
**
** A read lock on the disk file is obtained when the first page is acquired. 
** This read lock is dropped when the last page is released.
**
** A _get works for any page number greater than 0.  If the database
** file is smaller than the requested page, then no actual disk
** read occurs and the memory image of the page is initialized to
** all zeros.  The extra data appended to a page is always initialized
** to zeros the first time a page is loaded into memory.
**
** The acquisition might fail for several reasons.  In all cases,
** an appropriate error code is returned and *ppPage is set to NULL.
**
** See also sqlite3pager_lookup().  Both this routine and _lookup() attempt
** to find a page in the in-memory cache first.  If the page is not already
** in memory, this routine goes to disk to read it in whereas _lookup()
** just returns 0.  This routine acquires a read-lock the first time it
** has to go to disk, and could also playback an old journal if necessary.
** Since _lookup() never goes to disk, it never has to deal with locks
** or journal files.
*/
int sqlite3pager_get(Pager *pPager, Pgno pgno, void **ppPage){
  PgHdr *pPg;
  int rc;

  /* Make sure we have not hit any critical errors.
  */ 
  assert( pPager!=0 );
  assert( pgno!=0 );
  *ppPage = 0;
  if( pPager->errMask & ~(PAGER_ERR_FULL) ){
    return pager_errcode(pPager);
  }

  /* If this is the first page accessed, then get a read lock
  ** on the database file.
  */
  if( pPager->nRef==0 && !pPager->memDb ){
    rc = sqlite3OsReadLock(&pPager->fd);
    if( rc!=SQLITE_OK ){
      return rc;
    }
    pPager->state = SQLITE_READLOCK;

    /* If a journal file exists, try to play it back.
    */
    if( pPager->useJournal && sqlite3OsFileExists(pPager->zJournal) ){
       int rc;

       /* Get a write lock on the database
       */
       rc = sqlite3OsWriteLock(&pPager->fd);
       if( rc!=SQLITE_OK ){
         if( sqlite3OsUnlock(&pPager->fd)!=SQLITE_OK ){
           /* This should never happen! */
           rc = SQLITE_INTERNAL;
         }
         return rc;
       }
       pPager->state = SQLITE_WRITELOCK;

       /* Open the journal for reading only.  Return SQLITE_BUSY if
       ** we are unable to open the journal file. 
       **
       ** The journal file does not need to be locked itself.  The
       ** journal file is never open unless the main database file holds
       ** a write lock, so there is never any chance of two or more
       ** processes opening the journal at the same time.
       */
       rc = sqlite3OsOpenReadOnly(pPager->zJournal, &pPager->jfd);
       if( rc!=SQLITE_OK ){
         rc = sqlite3OsUnlock(&pPager->fd);
         assert( rc==SQLITE_OK );
         return SQLITE_BUSY;
       }
       pPager->journalOpen = 1;
       pPager->journalStarted = 0;

       /* Playback and delete the journal.  Drop the database write
       ** lock and reacquire the read lock.
       */
       rc = pager_playback(pPager, 0);
       if( rc!=SQLITE_OK ){
         return rc;
       }
    }
    pPg = 0;
  }else{
    /* Search for page in cache */
    pPg = pager_lookup(pPager, pgno);
    if( pPager->memDb && pPager->state==SQLITE_UNLOCK ){
      pPager->state = SQLITE_READLOCK;
    }
  }
  if( pPg==0 ){
    /* The requested page is not in the page cache. */
    int h;
    pPager->nMiss++;
    if( pPager->nPage<pPager->mxPage || pPager->pFirst==0 || pPager->memDb ){
      /* Create a new page */
      pPg = sqliteMallocRaw( sizeof(*pPg) + SQLITE_PAGE_SIZE 
                              + sizeof(u32) + pPager->nExtra
                              + pPager->memDb*sizeof(PgHistory) );
      if( pPg==0 ){
        pager_unwritelock(pPager);
        pPager->errMask |= PAGER_ERR_MEM;
        return SQLITE_NOMEM;
      }
      memset(pPg, 0, sizeof(*pPg));
      if( pPager->memDb ){
        memset(PGHDR_TO_HIST(pPg, pPager), 0, sizeof(PgHistory));
      }
      pPg->pPager = pPager;
      pPg->pNextAll = pPager->pAll;
      pPager->pAll = pPg;
      pPager->nPage++;
    }else{
      /* Find a page to recycle.  Try to locate a page that does not
      ** require us to do an fsync() on the journal.
      */
      pPg = pPager->pFirstSynced;

      /* If we could not find a page that does not require an fsync()
      ** on the journal file then fsync the journal file.  This is a
      ** very slow operation, so we work hard to avoid it.  But sometimes
      ** it can't be helped.
      */
      if( pPg==0 ){
        int rc = syncJournal(pPager);
        if( rc!=0 ){
          sqlite3pager_rollback(pPager);
          return SQLITE_IOERR;
        }
        pPg = pPager->pFirst;
      }
      assert( pPg->nRef==0 );

      /* Write the page to the database file if it is dirty.
      */
      if( pPg->dirty ){
        assert( pPg->needSync==0 );
        pPg->pDirty = 0;
        rc = pager_write_pagelist( pPg );
        if( rc!=SQLITE_OK ){
          sqlite3pager_rollback(pPager);
          return SQLITE_IOERR;
        }
      }
      assert( pPg->dirty==0 );

      /* If the page we are recycling is marked as alwaysRollback, then
      ** set the global alwaysRollback flag, thus disabling the
      ** sqlite_dont_rollback() optimization for the rest of this transaction.
      ** It is necessary to do this because the page marked alwaysRollback
      ** might be reloaded at a later time but at that point we won't remember
      ** that is was marked alwaysRollback.  This means that all pages must
      ** be marked as alwaysRollback from here on out.
      */
      if( pPg->alwaysRollback ){
        pPager->alwaysRollback = 1;
      }

      /* Unlink the old page from the free list and the hash table
      */
      unlinkPage(pPg);
      pPager->nOvfl++;
    }
    pPg->pgno = pgno;
    if( pPager->aInJournal && (int)pgno<=pPager->origDbSize ){
      sqlite3CheckMemory(pPager->aInJournal, pgno/8);
      assert( pPager->journalOpen );
      pPg->inJournal = (pPager->aInJournal[pgno/8] & (1<<(pgno&7)))!=0;
      pPg->needSync = 0;
    }else{
      pPg->inJournal = 0;
      pPg->needSync = 0;
    }
    if( pPager->aInStmt && (int)pgno<=pPager->stmtSize
             && (pPager->aInStmt[pgno/8] & (1<<(pgno&7)))!=0 ){
      page_add_to_stmt_list(pPg);
    }else{
      page_remove_from_stmt_list(pPg);
    }
    pPg->dirty = 0;
    pPg->nRef = 1;
    REFINFO(pPg);
    pPager->nRef++;
    h = pager_hash(pgno);
    pPg->pNextHash = pPager->aHash[h];
    pPager->aHash[h] = pPg;
    if( pPg->pNextHash ){
      assert( pPg->pNextHash->pPrevHash==0 );
      pPg->pNextHash->pPrevHash = pPg;
    }
    if( pPager->nExtra>0 ){
      memset(PGHDR_TO_EXTRA(pPg), 0, pPager->nExtra);
    }
    if( pPager->dbSize<0 ) sqlite3pager_pagecount(pPager);
    if( pPager->errMask!=0 ){
      sqlite3pager_unref(PGHDR_TO_DATA(pPg));
      rc = pager_errcode(pPager);
      return rc;
    }
    if( pPager->dbSize<(int)pgno ){
      memset(PGHDR_TO_DATA(pPg), 0, SQLITE_PAGE_SIZE);
    }else{
      int rc;
      assert( pPager->memDb==0 );
      sqlite3OsSeek(&pPager->fd, (pgno-1)*(off_t)SQLITE_PAGE_SIZE);
      rc = sqlite3OsRead(&pPager->fd, PGHDR_TO_DATA(pPg), SQLITE_PAGE_SIZE);
      TRACE2("FETCH %d\n", pPg->pgno);
      CODEC(pPager, PGHDR_TO_DATA(pPg), pPg->pgno, 3);
      if( rc!=SQLITE_OK ){
        off_t fileSize;
        if( sqlite3OsFileSize(&pPager->fd,&fileSize)!=SQLITE_OK
               || fileSize>=pgno*SQLITE_PAGE_SIZE ){
          sqlite3pager_unref(PGHDR_TO_DATA(pPg));
          return rc;
        }else{
          memset(PGHDR_TO_DATA(pPg), 0, SQLITE_PAGE_SIZE);
        }
      }
    }
  }else{
    /* The requested page is in the page cache. */
    pPager->nHit++;
    page_ref(pPg);
  }
  *ppPage = PGHDR_TO_DATA(pPg);
  return SQLITE_OK;
}

/*
** Acquire a page if it is already in the in-memory cache.  Do
** not read the page from disk.  Return a pointer to the page,
** or 0 if the page is not in cache.
**
** See also sqlite3pager_get().  The difference between this routine
** and sqlite3pager_get() is that _get() will go to the disk and read
** in the page if the page is not already in cache.  This routine
** returns NULL if the page is not in cache or if a disk I/O error 
** has ever happened.
*/
void *sqlite3pager_lookup(Pager *pPager, Pgno pgno){
  PgHdr *pPg;

  assert( pPager!=0 );
  assert( pgno!=0 );
  if( pPager->errMask & ~(PAGER_ERR_FULL) ){
    return 0;
  }
  pPg = pager_lookup(pPager, pgno);
  if( pPg==0 ) return 0;
  page_ref(pPg);
  return PGHDR_TO_DATA(pPg);
}

/*
** Release a page.
**
** If the number of references to the page drop to zero, then the
** page is added to the LRU list.  When all references to all pages
** are released, a rollback occurs and the lock on the database is
** removed.
*/
int sqlite3pager_unref(void *pData){
  PgHdr *pPg;

  /* Decrement the reference count for this page
  */
  pPg = DATA_TO_PGHDR(pData);
  assert( pPg->nRef>0 );
  pPg->nRef--;
  REFINFO(pPg);

  /* When the number of references to a page reach 0, call the
  ** destructor and add the page to the freelist.
  */
  if( pPg->nRef==0 ){
    Pager *pPager;
    pPager = pPg->pPager;
    pPg->pNextFree = 0;
    pPg->pPrevFree = pPager->pLast;
    pPager->pLast = pPg;
    if( pPg->pPrevFree ){
      pPg->pPrevFree->pNextFree = pPg;
    }else{
      pPager->pFirst = pPg;
    }
    if( pPg->needSync==0 && pPager->pFirstSynced==0 ){
      pPager->pFirstSynced = pPg;
    }
    if( pPager->xDestructor ){
      pPager->xDestructor(pData);
    }
  
    /* When all pages reach the freelist, drop the read lock from
    ** the database file.
    */
    pPager->nRef--;
    assert( pPager->nRef>=0 );
    if( pPager->nRef==0 && !pPager->memDb ){
      pager_reset(pPager);
    }
  }
  return SQLITE_OK;
}

/*
** Create a journal file for pPager.  There should already be a write
** lock on the database file when this routine is called.
**
** Return SQLITE_OK if everything.  Return an error code and release the
** write lock if anything goes wrong.
*/
static int pager_open_journal(Pager *pPager){
  int rc;
  assert( pPager->state==SQLITE_WRITELOCK );
  assert( pPager->journalOpen==0 );
  assert( pPager->useJournal );
  sqlite3pager_pagecount(pPager);
  pPager->aInJournal = sqliteMalloc( pPager->dbSize/8 + 1 );
  if( pPager->aInJournal==0 ){
    sqlite3OsReadLock(&pPager->fd);
    pPager->state = SQLITE_READLOCK;
    return SQLITE_NOMEM;
  }
  rc = sqlite3OsOpenExclusive(pPager->zJournal, &pPager->jfd,pPager->tempFile);
  if( rc!=SQLITE_OK ){
    sqliteFree(pPager->aInJournal);
    pPager->aInJournal = 0;
    sqlite3OsReadLock(&pPager->fd);
    pPager->state = SQLITE_READLOCK;
    return SQLITE_CANTOPEN;
  }
  sqlite3OsOpenDirectory(pPager->zDirectory, &pPager->jfd);
  pPager->journalOpen = 1;
  pPager->journalStarted = 0;
  pPager->needSync = 0;
  pPager->alwaysRollback = 0;
  pPager->nRec = 0;
  if( pPager->errMask!=0 ){
    rc = pager_errcode(pPager);
    return rc;
  }
  pPager->origDbSize = pPager->dbSize;
  if( journal_format==JOURNAL_FORMAT_3 ){
    rc = sqlite3OsWrite(&pPager->jfd, aJournalMagic3, sizeof(aJournalMagic3));
    if( rc==SQLITE_OK ){
      rc = write32bits(&pPager->jfd, pPager->noSync ? 0xffffffff : 0);
    }
    if( rc==SQLITE_OK ){
      sqlite3Randomness(sizeof(pPager->cksumInit), &pPager->cksumInit);
      rc = write32bits(&pPager->jfd, pPager->cksumInit);
    }
  }else if( journal_format==JOURNAL_FORMAT_2 ){
    rc = sqlite3OsWrite(&pPager->jfd, aJournalMagic2, sizeof(aJournalMagic2));
  }else{
    assert( journal_format==JOURNAL_FORMAT_1 );
    rc = sqlite3OsWrite(&pPager->jfd, aJournalMagic1, sizeof(aJournalMagic1));
  }
  if( rc==SQLITE_OK ){
    rc = write32bits(&pPager->jfd, pPager->dbSize);
  }
  if( pPager->stmtAutoopen && rc==SQLITE_OK ){
    rc = sqlite3pager_stmt_begin(pPager);
  }
  if( rc!=SQLITE_OK ){
    rc = pager_unwritelock(pPager);
    if( rc==SQLITE_OK ){
      rc = SQLITE_FULL;
    }
  }
  return rc;  
}

/*
** Acquire a write-lock on the database.  The lock is removed when
** the any of the following happen:
**
**   *  sqlite3pager_commit() is called.
**   *  sqlite3pager_rollback() is called.
**   *  sqlite3pager_close() is called.
**   *  sqlite3pager_unref() is called to on every outstanding page.
**
** The parameter to this routine is a pointer to any open page of the
** database file.  Nothing changes about the page - it is used merely
** to acquire a pointer to the Pager structure and as proof that there
** is already a read-lock on the database.
**
** A journal file is opened if this is not a temporary file.  For
** temporary files, the opening of the journal file is deferred until
** there is an actual need to write to the journal.
**
** If the database is already write-locked, this routine is a no-op.
*/
int sqlite3pager_begin(void *pData){
  PgHdr *pPg = DATA_TO_PGHDR(pData);
  Pager *pPager = pPg->pPager;
  int rc = SQLITE_OK;
  assert( pPg->nRef>0 );
  assert( pPager->state!=SQLITE_UNLOCK );
  if( pPager->state==SQLITE_READLOCK ){
    assert( pPager->aInJournal==0 );
    if( pPager->memDb ){
      pPager->state = SQLITE_WRITELOCK;
      pPager->origDbSize = pPager->dbSize;
    }else{
      rc = sqlite3OsWriteLock(&pPager->fd);
      if( rc!=SQLITE_OK ){
        return rc;
      }
      pPager->state = SQLITE_WRITELOCK;
      pPager->dirtyFile = 0;
      TRACE1("TRANSACTION\n");
      if( pPager->useJournal && !pPager->tempFile ){
        rc = pager_open_journal(pPager);
      }
    }
  }
  return rc;
}

/*
** Mark a data page as writeable.  The page is written into the journal 
** if it is not there already.  This routine must be called before making
** changes to a page.
**
** The first time this routine is called, the pager creates a new
** journal and acquires a write lock on the database.  If the write
** lock could not be acquired, this routine returns SQLITE_BUSY.  The
** calling routine must check for that return value and be careful not to
** change any page data until this routine returns SQLITE_OK.
**
** If the journal file could not be written because the disk is full,
** then this routine returns SQLITE_FULL and does an immediate rollback.
** All subsequent write attempts also return SQLITE_FULL until there
** is a call to sqlite3pager_commit() or sqlite3pager_rollback() to
** reset.
*/
int sqlite3pager_write(void *pData){
  PgHdr *pPg = DATA_TO_PGHDR(pData);
  Pager *pPager = pPg->pPager;
  int rc = SQLITE_OK;

  /* Check for errors
  */
  if( pPager->errMask ){ 
    return pager_errcode(pPager);
  }
  if( pPager->readOnly ){
    return SQLITE_PERM;
  }

  /* Mark the page as dirty.  If the page has already been written
  ** to the journal then we can return right away.
  */
  pPg->dirty = 1;
  if( pPg->inJournal && (pPg->inStmt || pPager->stmtInUse==0) ){
    pPager->dirtyFile = 1;
    return SQLITE_OK;
  }

  /* If we get this far, it means that the page needs to be
  ** written to the transaction journal or the ckeckpoint journal
  ** or both.
  **
  ** First check to see that the transaction journal exists and
  ** create it if it does not.
  */
  assert( pPager->state!=SQLITE_UNLOCK );
  rc = sqlite3pager_begin(pData);
  if( rc!=SQLITE_OK ){
    return rc;
  }
  assert( pPager->state==SQLITE_WRITELOCK );
  if( !pPager->journalOpen && pPager->useJournal ){
    rc = pager_open_journal(pPager);
    if( rc!=SQLITE_OK ) return rc;
  }
  assert( pPager->journalOpen || !pPager->useJournal );
  pPager->dirtyFile = 1;

  /* The transaction journal now exists and we have a write lock on the
  ** main database file.  Write the current page to the transaction 
  ** journal if it is not there already.
  */
  if( !pPg->inJournal && (pPager->useJournal || pPager->memDb) ){
    if( (int)pPg->pgno <= pPager->origDbSize ){
      int szPg;
      u32 saved;
      if( pPager->memDb ){
        PgHistory *pHist = PGHDR_TO_HIST(pPg, pPager);
        TRACE2("JOURNAL %d\n", pPg->pgno);
        assert( pHist->pOrig==0 );
        pHist->pOrig = sqliteMallocRaw( pPager->pageSize );
        if( pHist->pOrig ){
          memcpy(pHist->pOrig, PGHDR_TO_DATA(pPg), pPager->pageSize);
        }
        pPg->inJournal = 1;
      }else {
        if( journal_format>=JOURNAL_FORMAT_3 ){
          u32 cksum = pager_cksum(pPager, pPg->pgno, pData);
          saved = *(u32*)PGHDR_TO_EXTRA(pPg);
          store32bits(cksum, pPg, SQLITE_PAGE_SIZE);
          szPg = SQLITE_PAGE_SIZE+8;
        }else{
          szPg = SQLITE_PAGE_SIZE+4;
        }
        store32bits(pPg->pgno, pPg, -4);
        CODEC(pPager, pData, pPg->pgno, 7);
        rc = sqlite3OsWrite(&pPager->jfd, &((char*)pData)[-4], szPg);
        TRACE3("JOURNAL %d %d\n", pPg->pgno, pPg->needSync);
        CODEC(pPager, pData, pPg->pgno, 0);
        if( journal_format>=JOURNAL_FORMAT_3 ){
          *(u32*)PGHDR_TO_EXTRA(pPg) = saved;
        }
        if( rc!=SQLITE_OK ){
          sqlite3pager_rollback(pPager);
          pPager->errMask |= PAGER_ERR_FULL;
          return rc;
        }
        pPager->nRec++;
        assert( pPager->aInJournal!=0 );
        pPager->aInJournal[pPg->pgno/8] |= 1<<(pPg->pgno&7);
        pPg->needSync = !pPager->noSync;
        pPg->inJournal = 1;
        if( pPager->stmtInUse ){
          pPager->aInStmt[pPg->pgno/8] |= 1<<(pPg->pgno&7);
          page_add_to_stmt_list(pPg);
        }
      }
    }else{
      pPg->needSync = !pPager->journalStarted && !pPager->noSync;
      TRACE3("APPEND %d %d\n", pPg->pgno, pPg->needSync);
    }
    if( pPg->needSync ){
      pPager->needSync = 1;
    }
  }

  /* If the statement journal is open and the page is not in it,
  ** then write the current page to the statement journal.  Note that
  ** the statement journal always uses the simplier format 2 that lacks
  ** checksums.  The header is also omitted from the statement journal.
  */
  if( pPager->stmtInUse && !pPg->inStmt && (int)pPg->pgno<=pPager->stmtSize ){
    assert( pPg->inJournal || (int)pPg->pgno>pPager->origDbSize );
    if( pPager->memDb ){
      PgHistory *pHist = PGHDR_TO_HIST(pPg, pPager);
      assert( pHist->pStmt==0 );
      pHist->pStmt = sqliteMallocRaw( pPager->pageSize );
      if( pHist->pStmt ){
        memcpy(pHist->pStmt, PGHDR_TO_DATA(pPg), pPager->pageSize);
      }
      TRACE2("STMT-JOURNAL %d\n", pPg->pgno);
    }else{
      store32bits(pPg->pgno, pPg, -4);
      CODEC(pPager, pData, pPg->pgno, 7);
      rc = sqlite3OsWrite(&pPager->stfd, ((char*)pData)-4, SQLITE_PAGE_SIZE+4);
      TRACE2("STMT-JOURNAL %d\n", pPg->pgno);
      CODEC(pPager, pData, pPg->pgno, 0);
      if( rc!=SQLITE_OK ){
        sqlite3pager_rollback(pPager);
        pPager->errMask |= PAGER_ERR_FULL;
        return rc;
      }
      pPager->stmtNRec++;
      assert( pPager->aInStmt!=0 );
      pPager->aInStmt[pPg->pgno/8] |= 1<<(pPg->pgno&7);
    }
    page_add_to_stmt_list(pPg);
  }

  /* Update the database size and return.
  */
  if( pPager->dbSize<(int)pPg->pgno ){
    pPager->dbSize = pPg->pgno;
  }
  return rc;
}

/*
** Return TRUE if the page given in the argument was previously passed
** to sqlite3pager_write().  In other words, return TRUE if it is ok
** to change the content of the page.
*/
int sqlite3pager_iswriteable(void *pData){
  PgHdr *pPg = DATA_TO_PGHDR(pData);
  return pPg->dirty;
}

/*
** Replace the content of a single page with the information in the third
** argument.
*/
int sqlite3pager_overwrite(Pager *pPager, Pgno pgno, void *pData){
  void *pPage;
  int rc;

  rc = sqlite3pager_get(pPager, pgno, &pPage);
  if( rc==SQLITE_OK ){
    rc = sqlite3pager_write(pPage);
    if( rc==SQLITE_OK ){
      memcpy(pPage, pData, SQLITE_PAGE_SIZE);
    }
    sqlite3pager_unref(pPage);
  }
  return rc;
}

/*
** A call to this routine tells the pager that it is not necessary to
** write the information on page "pgno" back to the disk, even though
** that page might be marked as dirty.
**
** The overlying software layer calls this routine when all of the data
** on the given page is unused.  The pager marks the page as clean so
** that it does not get written to disk.
**
** Tests show that this optimization, together with the
** sqlite3pager_dont_rollback() below, more than double the speed
** of large INSERT operations and quadruple the speed of large DELETEs.
**
** When this routine is called, set the alwaysRollback flag to true.
** Subsequent calls to sqlite3pager_dont_rollback() for the same page
** will thereafter be ignored.  This is necessary to avoid a problem
** where a page with data is added to the freelist during one part of
** a transaction then removed from the freelist during a later part
** of the same transaction and reused for some other purpose.  When it
** is first added to the freelist, this routine is called.  When reused,
** the dont_rollback() routine is called.  But because the page contains
** critical data, we still need to be sure it gets rolled back in spite
** of the dont_rollback() call.
*/
void sqlite3pager_dont_write(Pager *pPager, Pgno pgno){
  PgHdr *pPg;

  pPg = pager_lookup(pPager, pgno);
  pPg->alwaysRollback = 1;
  if( pPg && pPg->dirty ){
    if( pPager->dbSize==(int)pPg->pgno && pPager->origDbSize<pPager->dbSize ){
      /* If this pages is the last page in the file and the file has grown
      ** during the current transaction, then do NOT mark the page as clean.
      ** When the database file grows, we must make sure that the last page
      ** gets written at least once so that the disk file will be the correct
      ** size. If you do not write this page and the size of the file
      ** on the disk ends up being too small, that can lead to database
      ** corruption during the next transaction.
      */
    }else{
      TRACE2("DONT_WRITE %d\n", pgno);
      pPg->dirty = 0;
    }
  }
}

/*
** A call to this routine tells the pager that if a rollback occurs,
** it is not necessary to restore the data on the given page.  This
** means that the pager does not have to record the given page in the
** rollback journal.
*/
void sqlite3pager_dont_rollback(void *pData){
  PgHdr *pPg = DATA_TO_PGHDR(pData);
  Pager *pPager = pPg->pPager;

  if( pPager->state!=SQLITE_WRITELOCK || pPager->journalOpen==0 ) return;
  if( pPg->alwaysRollback || pPager->alwaysRollback || pPager->memDb ) return;
  if( !pPg->inJournal && (int)pPg->pgno <= pPager->origDbSize ){
    assert( pPager->aInJournal!=0 );
    pPager->aInJournal[pPg->pgno/8] |= 1<<(pPg->pgno&7);
    pPg->inJournal = 1;
    if( pPager->stmtInUse ){
      pPager->aInStmt[pPg->pgno/8] |= 1<<(pPg->pgno&7);
      page_add_to_stmt_list(pPg);
    }
    TRACE2("DONT_ROLLBACK %d\n", pPg->pgno);
  }
  if( pPager->stmtInUse && !pPg->inStmt && (int)pPg->pgno<=pPager->stmtSize ){
    assert( pPg->inJournal || (int)pPg->pgno>pPager->origDbSize );
    assert( pPager->aInStmt!=0 );
    pPager->aInStmt[pPg->pgno/8] |= 1<<(pPg->pgno&7);
    page_add_to_stmt_list(pPg);
  }
}


/*
** Clear a PgHistory block
*/
static void clearHistory(PgHistory *pHist){
  sqliteFree(pHist->pOrig);
  sqliteFree(pHist->pStmt);
  pHist->pOrig = 0;
  pHist->pStmt = 0;
}

/*
** Commit all changes to the database and release the write lock.
**
** If the commit fails for any reason, a rollback attempt is made
** and an error code is returned.  If the commit worked, SQLITE_OK
** is returned.
*/
int sqlite3pager_commit(Pager *pPager){
  int rc;
  PgHdr *pPg;

  if( pPager->errMask==PAGER_ERR_FULL ){
    rc = sqlite3pager_rollback(pPager);
    if( rc==SQLITE_OK ){
      rc = SQLITE_FULL;
    }
    return rc;
  }
  if( pPager->errMask!=0 ){
    rc = pager_errcode(pPager);
    return rc;
  }
  if( pPager->state!=SQLITE_WRITELOCK ){
    return SQLITE_ERROR;
  }
  TRACE1("COMMIT\n");
  if( pPager->memDb ){
    pPg = pager_get_all_dirty_pages(pPager);
    while( pPg ){
      clearHistory(PGHDR_TO_HIST(pPg, pPager));
      pPg->dirty = 0;
      pPg->inJournal = 0;
      pPg->inStmt = 0;
      pPg->pPrevStmt = pPg->pNextStmt = 0;
      pPg = pPg->pDirty;
    }
    pPager->pStmt = 0;
    pPager->state = SQLITE_READLOCK;
    return SQLITE_OK;
  }
  if( pPager->dirtyFile==0 ){
    /* Exit early (without doing the time-consuming sqlite3OsSync() calls)
    ** if there have been no changes to the database file. */
    assert( pPager->needSync==0 );
    rc = pager_unwritelock(pPager);
    pPager->dbSize = -1;
    return rc;
  }
  assert( pPager->journalOpen );
  rc = syncJournal(pPager);
  if( rc!=SQLITE_OK ){
    goto commit_abort;
  }
  pPg = pager_get_all_dirty_pages(pPager);
  if( pPg ){
    rc = pager_write_pagelist(pPg);
    if( rc || (!pPager->noSync && sqlite3OsSync(&pPager->fd)!=SQLITE_OK) ){
      goto commit_abort;
    }
  }
  rc = pager_unwritelock(pPager);
  pPager->dbSize = -1;
  return rc;

  /* Jump here if anything goes wrong during the commit process.
  */
commit_abort:
  rc = sqlite3pager_rollback(pPager);
  if( rc==SQLITE_OK ){
    rc = SQLITE_FULL;
  }
  return rc;
}

/*
** Rollback all changes.  The database falls back to read-only mode.
** All in-memory cache pages revert to their original data contents.
** The journal is deleted.
**
** This routine cannot fail unless some other process is not following
** the correct locking protocol (SQLITE_PROTOCOL) or unless some other
** process is writing trash into the journal file (SQLITE_CORRUPT) or
** unless a prior malloc() failed (SQLITE_NOMEM).  Appropriate error
** codes are returned for all these occasions.  Otherwise,
** SQLITE_OK is returned.
*/
int sqlite3pager_rollback(Pager *pPager){
  int rc;
  TRACE1("ROLLBACK\n");
  if( pPager->memDb ){
    PgHdr *p;
    for(p=pPager->pAll; p; p=p->pNextAll){
      PgHistory *pHist;
      if( !p->dirty ) continue;
      pHist = PGHDR_TO_HIST(p, pPager);
      if( pHist->pOrig ){
        memcpy(PGHDR_TO_DATA(p), pHist->pOrig, pPager->pageSize);
        TRACE2("ROLLBACK-PAGE %d\n", p->pgno);
      }else{
        TRACE2("PAGE %d is clean\n", p->pgno);
      }
      clearHistory(pHist);
      p->dirty = 0;
      p->inJournal = 0;
      p->inStmt = 0;
      p->pPrevStmt = p->pNextStmt = 0;
    }
    pPager->pStmt = 0;
    pPager->dbSize = pPager->origDbSize;
    memoryTruncate(pPager);
    pPager->stmtInUse = 0;
    pPager->state = SQLITE_READLOCK;
    return SQLITE_OK;
  }

  if( !pPager->dirtyFile || !pPager->journalOpen ){
    rc = pager_unwritelock(pPager);
    pPager->dbSize = -1;
    return rc;
  }

  if( pPager->errMask!=0 && pPager->errMask!=PAGER_ERR_FULL ){
    if( pPager->state>=SQLITE_WRITELOCK ){
      pager_playback(pPager, 1);
    }
    return pager_errcode(pPager);
  }
  if( pPager->state!=SQLITE_WRITELOCK ){
    return SQLITE_OK;
  }
  rc = pager_playback(pPager, 1);
  if( rc!=SQLITE_OK ){
    rc = SQLITE_CORRUPT;
    pPager->errMask |= PAGER_ERR_CORRUPT;
  }
  pPager->dbSize = -1;
  return rc;
}

/*
** Return TRUE if the database file is opened read-only.  Return FALSE
** if the database is (in theory) writable.
*/
int sqlite3pager_isreadonly(Pager *pPager){
  return pPager->readOnly;
}

/*
** This routine is used for testing and analysis only.
*/
int *sqlite3pager_stats(Pager *pPager){
  static int a[9];
  a[0] = pPager->nRef;
  a[1] = pPager->nPage;
  a[2] = pPager->mxPage;
  a[3] = pPager->dbSize;
  a[4] = pPager->state;
  a[5] = pPager->errMask;
  a[6] = pPager->nHit;
  a[7] = pPager->nMiss;
  a[8] = pPager->nOvfl;
  return a;
}

/*
** Set the statement rollback point.
**
** This routine should be called with the transaction journal already
** open.  A new statement journal is created that can be used to rollback
** changes of a single SQL command within a larger transaction.
*/
int sqlite3pager_stmt_begin(Pager *pPager){
  int rc;
  char zTemp[SQLITE_TEMPNAME_SIZE];
  assert( !pPager->stmtInUse );
  TRACE1("STMT-BEGIN\n");
  if( pPager->memDb ){
    pPager->stmtInUse = 1;
    pPager->stmtSize = pPager->dbSize;
    return SQLITE_OK;
  }
  if( !pPager->journalOpen ){
    pPager->stmtAutoopen = 1;
    return SQLITE_OK;
  }
  assert( pPager->journalOpen );
  pPager->aInStmt = sqliteMalloc( pPager->dbSize/8 + 1 );
  if( pPager->aInStmt==0 ){
    sqlite3OsReadLock(&pPager->fd);
    return SQLITE_NOMEM;
  }
#ifndef NDEBUG
  rc = sqlite3OsFileSize(&pPager->jfd, &pPager->stmtJSize);
  if( rc ) goto stmt_begin_failed;
  assert( pPager->stmtJSize == 
    pPager->nRec*JOURNAL_PG_SZ(journal_format)+JOURNAL_HDR_SZ(journal_format) );
#endif
  pPager->stmtJSize = pPager->nRec*JOURNAL_PG_SZ(journal_format)
                         + JOURNAL_HDR_SZ(journal_format);
  pPager->stmtSize = pPager->dbSize;
  if( !pPager->stmtOpen ){
    rc = sqlite3pager_opentemp(zTemp, &pPager->stfd);
    if( rc ) goto stmt_begin_failed;
    pPager->stmtOpen = 1;
    pPager->stmtNRec = 0;
  }
  pPager->stmtInUse = 1;
  return SQLITE_OK;
 
stmt_begin_failed:
  if( pPager->aInStmt ){
    sqliteFree(pPager->aInStmt);
    pPager->aInStmt = 0;
  }
  return rc;
}

/*
** Commit a statement.
*/
int sqlite3pager_stmt_commit(Pager *pPager){
  if( pPager->stmtInUse ){
    PgHdr *pPg, *pNext;
    TRACE1("STMT-COMMIT\n");
    if( !pPager->memDb ){
      sqlite3OsSeek(&pPager->stfd, 0);
      /* sqlite3OsTruncate(&pPager->stfd, 0); */
      sqliteFree( pPager->aInStmt );
      pPager->aInStmt = 0;
    }
    for(pPg=pPager->pStmt; pPg; pPg=pNext){
      pNext = pPg->pNextStmt;
      assert( pPg->inStmt );
      pPg->inStmt = 0;
      pPg->pPrevStmt = pPg->pNextStmt = 0;
      if( pPager->memDb ){
        PgHistory *pHist = PGHDR_TO_HIST(pPg, pPager);
        sqliteFree(pHist->pStmt);
        pHist->pStmt = 0;
      }
    }
    pPager->stmtNRec = 0;
    pPager->stmtInUse = 0;
    pPager->pStmt = 0;
  }
  pPager->stmtAutoopen = 0;
  return SQLITE_OK;
}

/*
** Rollback a statement.
*/
int sqlite3pager_stmt_rollback(Pager *pPager){
  int rc;
  if( pPager->stmtInUse ){
    TRACE1("STMT-ROLLBACK\n");
    if( pPager->memDb ){
      PgHdr *pPg;
      for(pPg=pPager->pStmt; pPg; pPg=pPg->pNextStmt){
        PgHistory *pHist = PGHDR_TO_HIST(pPg, pPager);
        if( pHist->pStmt ){
          memcpy(PGHDR_TO_DATA(pPg), pHist->pStmt, pPager->pageSize);
          sqliteFree(pHist->pStmt);
          pHist->pStmt = 0;
        }
      }
      pPager->dbSize = pPager->stmtSize;
      memoryTruncate(pPager);
      rc = SQLITE_OK;
    }else{
      rc = pager_stmt_playback(pPager);
    }
    sqlite3pager_stmt_commit(pPager);
  }else{
    rc = SQLITE_OK;
  }
  pPager->stmtAutoopen = 0;
  return rc;
}

/*
** Return the full pathname of the database file.
*/
const char *sqlite3pager_filename(Pager *pPager){
  return pPager->zFilename;
}

/*
** Set the codec for this pager
*/
void sqlite3pager_set_codec(
  Pager *pPager,
  void (*xCodec)(void*,void*,Pgno,int),
  void *pCodecArg
){
  pPager->xCodec = xCodec;
  pPager->pCodecArg = pCodecArg;
}

#ifdef SQLITE_TEST
/*
** Print a listing of all referenced pages and their ref count.
*/
void sqlite3pager_refdump(Pager *pPager){
  PgHdr *pPg;
  for(pPg=pPager->pAll; pPg; pPg=pPg->pNextAll){
    if( pPg->nRef<=0 ) continue;
    printf("PAGE %3d addr=0x%08x nRef=%d\n", 
       pPg->pgno, (int)PGHDR_TO_DATA(pPg), pPg->nRef);
  }
}
#endif

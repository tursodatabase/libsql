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
** @(#) $Id: pager.c,v 1.23 2001/09/19 13:58:44 drh Exp $
*/
#include "sqliteInt.h"
#include "pager.h"
#include "os.h"
#include <assert.h>
#include <string.h>

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
*/
typedef struct PgHdr PgHdr;
struct PgHdr {
  Pager *pPager;                 /* The pager to which this page belongs */
  Pgno pgno;                     /* The page number for this page */
  PgHdr *pNextHash, *pPrevHash;  /* Hash collision chain for PgHdr.pgno */
  int nRef;                      /* Number of users of this page */
  PgHdr *pNextFree, *pPrevFree;  /* Freelist of pages where nRef==0 */
  PgHdr *pNextAll, *pPrevAll;    /* A list of all pages */
  char inJournal;                /* TRUE if has been written to journal */
  char dirty;                    /* TRUE if we need to write back changes */
  /* SQLITE_PAGE_SIZE bytes of page data follow this header */
  /* Pager.nExtra bytes of local data follow the page data */
};

/*
** Convert a pointer to a PgHdr into a pointer to its data
** and back again.
*/
#define PGHDR_TO_DATA(P)  ((void*)(&(P)[1]))
#define DATA_TO_PGHDR(D)  (&((PgHdr*)(D))[-1])
#define PGHDR_TO_EXTRA(P) ((void*)&((char*)(&(P)[1]))[SQLITE_PAGE_SIZE])

/*
** How big to make the hash table used for locating in-memory pages
** by page number.  Knuth says this should be a prime number.
*/
#define N_PG_HASH 373

/*
** A open page cache is an instance of the following structure.
*/
struct Pager {
  char *zFilename;            /* Name of the database file */
  char *zJournal;             /* Name of the journal file */
  OsFile fd, jfd;             /* File descriptors for database and journal */
  int journalOpen;            /* True if journal file descriptors is valid */
  int dbSize;                 /* Number of pages in the file */
  int origDbSize;             /* dbSize before the current change */
  int nExtra;                 /* Add this many bytes to each in-memory page */
  void (*xDestructor)(void*); /* Call this routine when freeing pages */
  int nPage;                  /* Total number of in-memory pages */
  int nRef;                   /* Number of in-memory pages with PgHdr.nRef>0 */
  int mxPage;                 /* Maximum number of pages to hold in cache */
  int nHit, nMiss, nOvfl;     /* Cache hits, missing, and LRU overflows */
  unsigned char state;        /* SQLITE_UNLOCK, _READLOCK or _WRITELOCK */
  unsigned char errMask;      /* One of several kinds of errors */
  unsigned char tempFile;     /* zFilename is a temporary file */
  unsigned char readOnly;     /* True for a read-only database */
  unsigned char needSync;     /* True if an fsync() is needed on the journal */
  unsigned char *aInJournal;  /* One bit for each page in the database file */
  PgHdr *pFirst, *pLast;      /* List of free pages */
  PgHdr *pAll;                /* List of all pages */
  PgHdr *aHash[N_PG_HASH];    /* Hash table to map page number of PgHdr */
};

/*
** These are bits that can be set in Pager.errMask.
*/
#define PAGER_ERR_FULL     0x01  /* a write() failed */
#define PAGER_ERR_MEM      0x02  /* malloc() failed */
#define PAGER_ERR_LOCK     0x04  /* error in the locking protocol */
#define PAGER_ERR_CORRUPT  0x08  /* database or journal corruption */

/*
** The journal file contains page records in the following
** format.
*/
typedef struct PageRecord PageRecord;
struct PageRecord {
  Pgno pgno;                     /* The page number */
  char aData[SQLITE_PAGE_SIZE];  /* Original data for page pgno */
};

/*
** Journal files begin with the following magic string.  The data
** was obtained from /dev/random.  It is used only as a sanity check.
*/
static const unsigned char aJournalMagic[] = {
  0xd9, 0xd5, 0x05, 0xf9, 0x20, 0xa1, 0x63, 0xd4,
};

/*
** Hash a page number
*/
#define pager_hash(PN)  ((PN)%N_PG_HASH)

/*
** Enable reference count tracking here:
*/
#if SQLITE_TEST
  int pager_refinfo_enable = 0;
  static void pager_refinfo(PgHdr *p){
    static int cnt = 0;
    if( !pager_refinfo_enable ) return;
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
** Convert the bits in the pPager->errMask into an approprate
** return code.
*/
static int pager_errcode(Pager *pPager){
  int rc = SQLITE_OK;
  if( pPager->errMask & PAGER_ERR_LOCK )    rc = SQLITE_PROTOCOL;
  if( pPager->errMask & PAGER_ERR_FULL )    rc = SQLITE_FULL;
  if( pPager->errMask & PAGER_ERR_MEM )     rc = SQLITE_NOMEM;
  if( pPager->errMask & PAGER_ERR_CORRUPT ) rc = SQLITE_CORRUPT;
  return rc;
}

/*
** Find a page in the hash table given its page number.  Return
** a pointer to the page or NULL if not found.
*/
static PgHdr *pager_lookup(Pager *pPager, Pgno pgno){
  PgHdr *p = pPager->aHash[pgno % N_PG_HASH];
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
  pPager->pLast = 0;
  pPager->pAll = 0;
  memset(pPager->aHash, 0, sizeof(pPager->aHash));
  pPager->nPage = 0;
  if( pPager->state==SQLITE_WRITELOCK ){
    sqlitepager_rollback(pPager);
  }
  sqliteOsUnlock(pPager->fd);
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
** We have to release the write lock before acquiring the read lock,
** so there is a race condition where another process can get the lock
** while we are not holding it.  But, no other process should do this
** because we are also holding a lock on the journal, and no process
** should get a write lock on the database without first getting a lock
** on the journal.  So this routine should never fail.  But it can fail
** if another process is not playing by the rules.  If it does fail,
** all in-memory cache pages are invalidated, the PAGER_ERR_LOCK bit
** is set in pPager->errMask, and this routine returns SQLITE_PROTOCOL.
** SQLITE_OK is returned on success.
*/
static int pager_unwritelock(Pager *pPager){
  int rc;
  PgHdr *pPg;
  if( pPager->state!=SQLITE_WRITELOCK ) return SQLITE_OK;
  sqliteOsUnlock(pPager->fd);
  rc = sqliteOsLock(pPager->fd, 0);
  sqliteOsClose(pPager->jfd);
  pPager->journalOpen = 0;
  sqliteOsDelete(pPager->zJournal);
  sqliteFree( pPager->aInJournal );
  pPager->aInJournal = 0;
  for(pPg=pPager->pAll; pPg; pPg=pPg->pNextAll){
    pPg->inJournal = 0;
    pPg->dirty = 0;
  }
  if( rc!=SQLITE_OK ){
    pPager->state = SQLITE_UNLOCK;
    rc = SQLITE_PROTOCOL;
    pPager->errMask |= PAGER_ERR_LOCK;
  }else{
    rc = SQLITE_OK;
    pPager->state = SQLITE_READLOCK;
  }
  return rc;
}

/*
** Playback the journal and thus restore the database file to
** the state it was in before we started making changes.  
**
** The journal file format is as follows:  There is an initial
** file-type string for sanity checking.  Then there is a single
** Pgno number which is the number of pages in the database before
** changes were made.  The database is truncated to this size.
** Next come zero or more page records where each page record
** consists of a Pgno and SQLITE_PAGE_SIZE bytes of data.  See
** the PageRecord structure for details.
**
** For playback, the pages have to be read from the journal in
** reverse order and put back into the original database file.
**
** If the file opened as the journal file is not a well-formed
** journal file (as determined by looking at the magic number
** at the beginning) then this routine returns SQLITE_PROTOCOL.
** If any other errors occur during playback, the database will
** likely be corrupted, so the PAGER_ERR_CORRUPT bit is set in
** pPager->errMask and SQLITE_CORRUPT is returned.  If it all
** works, then this routine returns SQLITE_OK.
*/
static int pager_playback(Pager *pPager){
  int nRec;                /* Number of Records */
  int i;                   /* Loop counter */
  Pgno mxPg = 0;           /* Size of the original file in pages */
  PgHdr *pPg;              /* An existing page in the cache */
  PageRecord pgRec;
  unsigned char aMagic[sizeof(aJournalMagic)];
  int rc;

  /* Read the beginning of the journal and truncate the
  ** database file back to its original size.
  */
  assert( pPager->journalOpen );
  sqliteOsSeek(pPager->jfd, 0);
  rc = sqliteOsRead(pPager->jfd, aMagic, sizeof(aMagic));
  if( rc!=SQLITE_OK || memcmp(aMagic,aJournalMagic,sizeof(aMagic))!=0 ){
    return SQLITE_PROTOCOL;
  }
  rc = sqliteOsRead(pPager->jfd, &mxPg, sizeof(mxPg));
  if( rc!=SQLITE_OK ){
    return SQLITE_PROTOCOL;
  }
  sqliteOsTruncate(pPager->fd, mxPg*SQLITE_PAGE_SIZE);
  pPager->dbSize = mxPg;
  
  /* Begin reading the journal beginning at the end and moving
  ** toward the beginning.
  */
  if( sqliteOsFileSize(pPager->jfd, &nRec)!=SQLITE_OK ){
    return SQLITE_OK;
  }
  nRec = (nRec - (sizeof(aMagic)+sizeof(Pgno))) / sizeof(PageRecord);

  /* Process segments beginning with the last and working backwards
  ** to the first.
  */
  for(i=nRec-1; i>=0; i--){
    /* Seek to the beginning of the segment */
    off_t ofst;
    ofst = i*sizeof(PageRecord) + sizeof(aMagic) + sizeof(Pgno);
    rc = sqliteOsSeek(pPager->jfd, ofst);
    if( rc!=SQLITE_OK ) break;
    rc = sqliteOsRead(pPager->jfd, &pgRec, sizeof(pgRec));
    if( rc!=SQLITE_OK ) break;

    /* Sanity checking on the page */
    if( pgRec.pgno>mxPg || pgRec.pgno==0 ){
      rc = SQLITE_CORRUPT;
      break;
    }

    /* Playback the page.  Update the in-memory copy of the page
    ** at the same time, if there is one.
    */
    pPg = pager_lookup(pPager, pgRec.pgno);
    if( pPg ){
      memcpy(PGHDR_TO_DATA(pPg), pgRec.aData, SQLITE_PAGE_SIZE);
      memset(PGHDR_TO_EXTRA(pPg), 0, pPager->nExtra);
    }
    rc = sqliteOsSeek(pPager->fd, (pgRec.pgno-1)*SQLITE_PAGE_SIZE);
    if( rc!=SQLITE_OK ) break;
    rc = sqliteOsWrite(pPager->fd, pgRec.aData, SQLITE_PAGE_SIZE);
    if( rc!=SQLITE_OK ) break;
  }
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
** Change the maximum number of in-memory pages that are allowed.
*/
void sqlitepager_set_cachesize(Pager *pPager, int mxPage){
  if( mxPage>10 ){
    pPager->mxPage = mxPage;
  }
}

/*
** Create a new page cache and put a pointer to the page cache in *ppPager.
** The file to be cached need not exist.  The file is not locked until
** the first call to sqlitepager_get() and is only held open until the
** last page is released using sqlitepager_unref().
*/
int sqlitepager_open(
  Pager **ppPager,         /* Return the Pager structure here */
  const char *zFilename,   /* Name of the database file to open */
  int mxPage,              /* Max number of in-memory cache pages */
  int nExtra               /* Extra bytes append to each in-memory page */
){
  Pager *pPager;
  int nameLen;
  OsFile fd;
  int rc;
  int tempFile;
  int readOnly = 0;
  char zTemp[SQLITE_TEMPNAME_SIZE];

  *ppPager = 0;
  if( sqlite_malloc_failed ){
    return SQLITE_NOMEM;
  }
  if( zFilename ){
    rc = sqliteOsOpenReadWrite(zFilename, &fd, &readOnly);
    tempFile = 0;
  }else{
    int cnt = 8;
    sqliteOsTempFileName(zTemp);
    do{
      cnt--;
      sqliteOsTempFileName(zTemp);
      rc = sqliteOsOpenExclusive(zTemp, &fd);
    }while( cnt>0 && rc!=SQLITE_OK );
    zFilename = zTemp;
    tempFile = 1;
  }
  if( rc!=SQLITE_OK ){
    return SQLITE_CANTOPEN;
  }
  nameLen = strlen(zFilename);
  pPager = sqliteMalloc( sizeof(*pPager) + nameLen*2 + 30 );
  if( pPager==0 ){
    sqliteOsClose(fd);
    return SQLITE_NOMEM;
  }
  pPager->zFilename = (char*)&pPager[1];
  pPager->zJournal = &pPager->zFilename[nameLen+1];
  strcpy(pPager->zFilename, zFilename);
  strcpy(pPager->zJournal, zFilename);
  strcpy(&pPager->zJournal[nameLen], "-journal");
  pPager->fd = fd;
  pPager->journalOpen = 0;
  pPager->nRef = 0;
  pPager->dbSize = -1;
  pPager->nPage = 0;
  pPager->mxPage = mxPage>5 ? mxPage : 10;
  pPager->state = SQLITE_UNLOCK;
  pPager->errMask = 0;
  pPager->tempFile = tempFile;
  pPager->readOnly = readOnly;
  pPager->needSync = 0;
  pPager->pFirst = 0;
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
** The destructor is not called as a result sqlitepager_close().  
** Destructors are only called by sqlitepager_unref().
*/
void sqlitepager_set_destructor(Pager *pPager, void (*xDesc)(void*)){
  pPager->xDestructor = xDesc;
}

/*
** Return the total number of pages in the disk file associated with
** pPager.
*/
int sqlitepager_pagecount(Pager *pPager){
  int n;
  assert( pPager!=0 );
  if( pPager->dbSize>=0 ){
    return pPager->dbSize;
  }
  if( sqliteOsFileSize(pPager->fd, &n)!=SQLITE_OK ){
    return 0;
  }
  n /= SQLITE_PAGE_SIZE;
  if( pPager->state!=SQLITE_UNLOCK ){
    pPager->dbSize = n;
  }
  return n;
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
int sqlitepager_close(Pager *pPager){
  PgHdr *pPg, *pNext;
  switch( pPager->state ){
    case SQLITE_WRITELOCK: {
      sqlitepager_rollback(pPager);
      sqliteOsUnlock(pPager->fd);
      assert( pPager->journalOpen==0 );
      break;
    }
    case SQLITE_READLOCK: {
      sqliteOsUnlock(pPager->fd);
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
  sqliteOsClose(pPager->fd);
  assert( pPager->journalOpen==0 );
  if( pPager->tempFile ){
    sqliteOsDelete(pPager->zFilename);
  }
  sqliteFree(pPager);
  return SQLITE_OK;
}

/*
** Return the page number for the given page data.
*/
Pgno sqlitepager_pagenumber(void *pData){
  PgHdr *p = DATA_TO_PGHDR(pData);
  return p->pgno;
}

/*
** Increment the reference count for a page.  If the page is
** currently on the freelist (the reference count is zero) then
** remove it from the freelist.
*/
static void page_ref(PgHdr *pPg){
  if( pPg->nRef==0 ){
    /* The page is currently on the freelist.  Remove it. */
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

/*
** Increment the reference count for a page.  The input pointer is
** a reference to the page data.
*/
int sqlitepager_ref(void *pData){
  PgHdr *pPg = DATA_TO_PGHDR(pData);
  page_ref(pPg);
  return SQLITE_OK;
}

/*
** Sync the journal and then write all free dirty pages to the database
** file.
**
** Writing all free dirty pages to the database after the sync is a
** non-obvious optimization.  fsync() is an expensive operation so we
** want to minimize the number that occur.  So after an fsync() is forced
** and we are free to write dirty pages back to the database, it is best
** to go ahead and do as much of that as possible to minimize the chance
** of having to do another fsync() later on.  Writing dirty free pages
** in this way make database operations go up to 10 times faster.
*/
static int syncAllPages(Pager *pPager){
  PgHdr *pPg;
  int rc = SQLITE_OK;
  if( pPager->needSync ){
    rc = sqliteOsSync(pPager->jfd);
    if( rc!=0 ) return rc;
    pPager->needSync = 0;
  }
  for(pPg=pPager->pFirst; pPg; pPg=pPg->pNextFree){
    if( pPg->dirty ){
      sqliteOsSeek(pPager->fd, (pPg->pgno-1)*SQLITE_PAGE_SIZE);
      rc = sqliteOsWrite(pPager->fd, PGHDR_TO_DATA(pPg), SQLITE_PAGE_SIZE);
      if( rc!=SQLITE_OK ) break;
      pPg->dirty = 0;
    }
  }
  return SQLITE_OK;
}

/*
** Acquire a page.
**
** A read lock on the disk file is obtained when the first page acquired. 
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
** See also sqlitepager_lookup().  Both this routine and _lookup() attempt
** to find a page in the in-memory cache first.  If the page is not already
** in memory, this routine goes to disk to read it in whereas _lookup()
** just returns 0.  This routine acquires a read-lock the first time it
** has to go to disk, and could also playback an old journal if necessary.
** Since _lookup() never goes to disk, it never has to deal with locks
** or journal files.
*/
int sqlitepager_get(Pager *pPager, Pgno pgno, void **ppPage){
  PgHdr *pPg;

  /* Make sure we have not hit any critical errors.
  */ 
  if( pPager==0 || pgno==0 ){
    return SQLITE_ERROR;
  }
  if( pPager->errMask & ~(PAGER_ERR_FULL) ){
    return pager_errcode(pPager);
  }

  /* If this is the first page accessed, then get a read lock
  ** on the database file.
  */
  if( pPager->nRef==0 ){
    if( sqliteOsLock(pPager->fd, 0)!=SQLITE_OK ){
      *ppPage = 0;
      return SQLITE_BUSY;
    }
    pPager->state = SQLITE_READLOCK;

    /* If a journal file exists, try to play it back.
    */
    if( sqliteOsFileExists(pPager->zJournal) ){
       int rc;

       /* Open the journal for exclusive access.  Return SQLITE_BUSY if
       ** we cannot get exclusive access to the journal file
       */
       rc = sqliteOsOpenReadOnly(pPager->zJournal, &pPager->jfd);
       if( rc==SQLITE_OK ){
         pPager->journalOpen = 1;
       }
       if( rc!=SQLITE_OK || sqliteOsLock(pPager->jfd, 1)!=SQLITE_OK ){
         if( pPager->journalOpen ){
           sqliteOsClose(pPager->jfd);
           pPager->journalOpen = 0;
         }
         sqliteOsUnlock(pPager->fd);
         *ppPage = 0;
         return SQLITE_BUSY;
       }

       /* Get a write lock on the database */
       sqliteOsUnlock(pPager->fd);
       if( sqliteOsLock(pPager->fd, 1)!=SQLITE_OK ){
         sqliteOsClose(pPager->jfd);
         pPager->journalOpen = 0;
         *ppPage = 0;
         return SQLITE_PROTOCOL;
       }
       pPager->state = SQLITE_WRITELOCK;

       /* Playback and delete the journal.  Drop the database write
       ** lock and reacquire the read lock.
       */
       rc = pager_playback(pPager);
       if( rc!=SQLITE_OK ){
         return rc;
       }
    }
    pPg = 0;
  }else{
    /* Search for page in cache */
    pPg = pager_lookup(pPager, pgno);
  }
  if( pPg==0 ){
    /* The requested page is not in the page cache. */
    int h;
    pPager->nMiss++;
    if( pPager->nPage<pPager->mxPage || pPager->pFirst==0 ){
      /* Create a new page */
      pPg = sqliteMalloc( sizeof(*pPg) + SQLITE_PAGE_SIZE + pPager->nExtra );
      if( pPg==0 ){
        *ppPage = 0;
        pager_unwritelock(pPager);
        pPager->errMask |= PAGER_ERR_MEM;
        return SQLITE_NOMEM;
      }
      pPg->pPager = pPager;
      pPg->pNextAll = pPager->pAll;
      if( pPager->pAll ){
        pPager->pAll->pPrevAll = pPg;
      }
      pPg->pPrevAll = 0;
      pPager->pAll = pPg;
      pPager->nPage++;
    }else{
      /* Recycle an older page.  First locate the page to be recycled.
      ** Try to find one that is not dirty and is near the head of
      ** of the free list */
      int cnt = pPager->mxPage/2;
      pPg = pPager->pFirst;
      while( pPg->dirty && 0<cnt-- && pPg->pNextFree ){
        pPg = pPg->pNextFree;
      }

      /* If we could not find a page that has not been used recently
      ** and which is not dirty, then sync the journal and write all
      ** dirty free pages into the database file, thus making them
      ** clean pages and available for recycling.
      **
      ** We have to sync the journal before writing a page to the main
      ** database.  But syncing is a very slow operation.  So after a
      ** sync, it is best to write everything we can back to the main
      ** database to minimize the risk of having to sync again in the
      ** near future.  That is way we write all dirty pages after a
      ** sync.
      */
      if( pPg==0 || pPg->dirty ){
        int rc = syncAllPages(pPager);
        if( rc!=0 ){
          sqlitepager_rollback(pPager);
          *ppPage = 0;
          return SQLITE_IOERR;
        }
        pPg = pPager->pFirst;
      }
      assert( pPg->nRef==0 );
      assert( pPg->dirty==0 );

      /* Unlink the old page from the free list and the hash table
      */
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
      if( pPg->pNextHash ){
        pPg->pNextHash->pPrevHash = pPg->pPrevHash;
      }
      if( pPg->pPrevHash ){
        pPg->pPrevHash->pNextHash = pPg->pNextHash;
      }else{
        h = pager_hash(pPg->pgno);
        assert( pPager->aHash[h]==pPg );
        pPager->aHash[h] = pPg->pNextHash;
      }
      pPg->pNextHash = pPg->pPrevHash = 0;
      pPager->nOvfl++;
    }
    pPg->pgno = pgno;
    if( pPager->aInJournal && pgno<=pPager->origDbSize ){
      pPg->inJournal = (pPager->aInJournal[pgno/8] & (1<<(pgno&7)))!=0;
    }else{
      pPg->inJournal = 0;
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
    if( pPager->dbSize<0 ) sqlitepager_pagecount(pPager);
    if( pPager->dbSize<pgno ){
      memset(PGHDR_TO_DATA(pPg), 0, SQLITE_PAGE_SIZE);
    }else{
      sqliteOsSeek(pPager->fd, (pgno-1)*SQLITE_PAGE_SIZE);
      sqliteOsRead(pPager->fd, PGHDR_TO_DATA(pPg), SQLITE_PAGE_SIZE);
    }
    if( pPager->nExtra>0 ){
      memset(PGHDR_TO_EXTRA(pPg), 0, pPager->nExtra);
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
** See also sqlitepager_get().  The difference between this routine
** and sqlitepager_get() is that _get() will go to the disk and read
** in the page if the page is not already in cache.  This routine
** returns NULL if the page is not in cache or if a disk I/O error 
** has ever happened.
*/
void *sqlitepager_lookup(Pager *pPager, Pgno pgno){
  PgHdr *pPg;

  /* Make sure we have not hit any critical errors.
  */ 
  if( pPager==0 || pgno==0 ){
    return 0;
  }
  if( pPager->errMask & ~(PAGER_ERR_FULL) ){
    return 0;
  }
  if( pPager->nRef==0 ){
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
int sqlitepager_unref(void *pData){
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
    if( pPager->xDestructor ){
      pPager->xDestructor(pData);
    }
  
    /* When all pages reach the freelist, drop the read lock from
    ** the database file.
    */
    pPager->nRef--;
    assert( pPager->nRef>=0 );
    if( pPager->nRef==0 ){
      pager_reset(pPager);
    }
  }
  return SQLITE_OK;
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
** is a call to sqlitepager_commit() or sqlitepager_rollback() to
** reset.
*/
int sqlitepager_write(void *pData){
  PgHdr *pPg = DATA_TO_PGHDR(pData);
  Pager *pPager = pPg->pPager;
  int rc = SQLITE_OK;

  if( pPager->errMask ){ 
    return pager_errcode(pPager);
  }
  if( pPager->readOnly ){
    return SQLITE_PERM;
  }
  pPg->dirty = 1;
  if( pPg->inJournal ){ return SQLITE_OK; }
  assert( pPager->state!=SQLITE_UNLOCK );
  if( pPager->state==SQLITE_READLOCK ){
    assert( pPager->aInJournal==0 );
    pPager->aInJournal = sqliteMalloc( pPager->dbSize/8 + 1 );
    if( pPager->aInJournal==0 ){
      return SQLITE_NOMEM;
    }
    rc = sqliteOsOpenExclusive(pPager->zJournal, &pPager->jfd);
    if( rc!=SQLITE_OK ){
      return SQLITE_CANTOPEN;
    }
    pPager->journalOpen = 1;
    pPager->needSync = 0;
    if( sqliteOsLock(pPager->jfd, 1)!=SQLITE_OK ){
      sqliteOsClose(pPager->jfd);
      pPager->journalOpen = 0;
      return SQLITE_BUSY;
    }
    sqliteOsUnlock(pPager->fd);
    if( sqliteOsLock(pPager->fd, 1)!=SQLITE_OK ){
      sqliteOsClose(pPager->jfd);
      pPager->journalOpen = 0;
      pPager->state = SQLITE_UNLOCK;
      pPager->errMask |= PAGER_ERR_LOCK;
      return SQLITE_PROTOCOL;
    }
    pPager->state = SQLITE_WRITELOCK;
    sqlitepager_pagecount(pPager);
    pPager->origDbSize = pPager->dbSize;
    rc = sqliteOsWrite(pPager->jfd, aJournalMagic, sizeof(aJournalMagic));
    if( rc==SQLITE_OK ){
      rc = sqliteOsWrite(pPager->jfd, &pPager->dbSize, sizeof(Pgno));
    }
    if( rc!=SQLITE_OK ){
      rc = pager_unwritelock(pPager);
      if( rc==SQLITE_OK ) rc = SQLITE_FULL;
      return rc;
    }
  }
  assert( pPager->state==SQLITE_WRITELOCK );
  assert( pPager->journalOpen );
  if( pPg->pgno <= pPager->origDbSize ){
    rc = sqliteOsWrite(pPager->jfd, &pPg->pgno, sizeof(Pgno));
    if( rc==SQLITE_OK ){
      rc = sqliteOsWrite(pPager->jfd, pData, SQLITE_PAGE_SIZE);
    }
    if( rc!=SQLITE_OK ){
      sqlitepager_rollback(pPager);
      pPager->errMask |= PAGER_ERR_FULL;
      return rc;
    }
    assert( pPager->aInJournal!=0 );
    pPager->aInJournal[pPg->pgno/8] |= 1<<(pPg->pgno&7);
    pPager->needSync = 1;
  }
  pPg->inJournal = 1;
  if( pPager->dbSize<pPg->pgno ){
    pPager->dbSize = pPg->pgno;
  }
  return rc;
}

/*
** Return TRUE if the page given in the argument was previous passed
** to sqlitepager_write().  In other words, return TRUE if it is ok
** to change the content of the page.
*/
int sqlitepager_iswriteable(void *pData){
  PgHdr *pPg = DATA_TO_PGHDR(pData);
  return pPg->dirty;
}

/*
** Commit all changes to the database and release the write lock.
**
** If the commit fails for any reason, a rollback attempt is made
** and an error code is returned.  If the commit worked, SQLITE_OK
** is returned.
*/
int sqlitepager_commit(Pager *pPager){
  int rc;
  PgHdr *pPg;

  if( pPager->errMask==PAGER_ERR_FULL ){
    rc = sqlitepager_rollback(pPager);
    if( rc==SQLITE_OK ) rc = SQLITE_FULL;
    return rc;
  }
  if( pPager->errMask!=0 ){
    rc = pager_errcode(pPager);
    return rc;
  }
  if( pPager->state!=SQLITE_WRITELOCK ){
    return SQLITE_ERROR;
  }
  assert( pPager->journalOpen );
  if( pPager->needSync && sqliteOsSync(pPager->jfd)!=SQLITE_OK ){
    goto commit_abort;
  }
  for(pPg=pPager->pAll; pPg; pPg=pPg->pNextAll){
    if( pPg->dirty==0 ) continue;
    rc = sqliteOsSeek(pPager->fd, (pPg->pgno-1)*SQLITE_PAGE_SIZE);
    if( rc!=SQLITE_OK ) goto commit_abort;
    rc = sqliteOsWrite(pPager->fd, PGHDR_TO_DATA(pPg), SQLITE_PAGE_SIZE);
    if( rc!=SQLITE_OK ) goto commit_abort;
  }
  if( sqliteOsSync(pPager->fd)!=SQLITE_OK ) goto commit_abort;
  rc = pager_unwritelock(pPager);
  pPager->dbSize = -1;
  return rc;

  /* Jump here if anything goes wrong during the commit process.
  */
commit_abort:
  rc = sqlitepager_rollback(pPager);
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
int sqlitepager_rollback(Pager *pPager){
  int rc;
  if( pPager->errMask!=0 && pPager->errMask!=PAGER_ERR_FULL ){
    return pager_errcode(pPager);
  }
  if( pPager->state!=SQLITE_WRITELOCK ){
    return SQLITE_OK;
  }
  rc = pager_playback(pPager);
  if( rc!=SQLITE_OK ){
    rc = SQLITE_CORRUPT;
    pPager->errMask |= PAGER_ERR_CORRUPT;
  }
  pPager->dbSize = -1;
  return rc;
};

/*
** Return TRUE if the database file is opened read-only.  Return FALSE
** if the database is (in theory) writable.
*/
int sqlitepager_isreadonly(Pager *pPager){
  return pPager->readOnly;
}

/*
** This routine is used for testing and analysis only.
*/
int *sqlitepager_stats(Pager *pPager){
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

#if SQLITE_TEST
/*
** Print a listing of all referenced pages and their ref count.
*/
void sqlitepager_refdump(Pager *pPager){
  PgHdr *pPg;
  for(pPg=pPager->pAll; pPg; pPg=pPg->pNextAll){
    if( pPg->nRef<=0 ) continue;
    printf("PAGE %3d addr=0x%08x nRef=%d\n", 
       pPg->pgno, (int)PGHDR_TO_DATA(pPg), pPg->nRef);
  }
}
#endif

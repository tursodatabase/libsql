/*
** Copyright (c) 2001 D. Richard Hipp
**
** This program is free software; you can redistribute it and/or
** modify it under the terms of the GNU General Public
** License as published by the Free Software Foundation; either
** version 2 of the License, or (at your option) any later version.
**
** This program is distributed in the hope that it will be useful,
** but WITHOUT ANY WARRANTY; without even the implied warranty of
** MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
** General Public License for more details.
** 
** You should have received a copy of the GNU General Public
** License along with this library; if not, write to the
** Free Software Foundation, Inc., 59 Temple Place - Suite 330,
** Boston, MA  02111-1307, USA.
**
** Author contact information:
**   drh@hwaci.com
**   http://www.hwaci.com/drh/
**
*************************************************************************
** This is the implementation of the page cache subsystem.
** 
** The page cache is used to access a database file.  The pager journals
** all writes in order to support rollback.  Locking is used to limit
** access to one or more reader or on writer.
**
** @(#) $Id: pager.c,v 1.2 2001/04/14 16:38:23 drh Exp $
*/
#include "pager.h"
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>
#include <assert.h>

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
** The page cache comes up in PCS_UNLOCK.  The first time a
** sqlite_page_get() occurs, the state transitions to PCS_READLOCK.
** After all pages have been released using sqlite_page_unref(),
** the state transitions back to PCS_UNLOCK.  The first time
** that sqlite_page_write() is called, the state transitions to
** PCS_WRITELOCK.  The sqlite_page_rollback() and sqlite_page_commit()
** functions transition the state back to PCS_READLOCK.
*/
#define SQLITE_UNLOCK      0
#define SQLITE_READLOCK    1
#define SQLITE_WRITELOCK   2

/*
** Each in-memory image of a page begins with the following header.
*/
struct PgHdr {
  Pager *pPager;                 /* The pager to which this page belongs */
  Pgno pgno;                     /* The page number for this page */
  PgHdr *pNextHash, *pPrevHash;  /* Hash collision chain for PgHdr.pgno */
  int nRef;                      /* Number of users of this page */
  PgHdr *pNext, *pPrev;          /* Freelist of pages where nRef==0 */
  char inJournal;                /* TRUE if has been written to journal */
  char dirty;                    /* TRUE if we need to write back changes */
  /* SQLITE_PAGE_SIZE bytes of page data follow this header */
};

/*
** Convert a pointer to a PgHdr into a pointer to its data
** and back again.
*/
#define PGHDR_TO_DATA(P)  ((void*)(&(P)[1]))
#define DATA_TO_PGHDR(D)  (&((PgHdr*)(D))[-1])

/*
** The number of page numbers that will fit on one page.
*/
#define SQLITE_INDEX_SIZE   (SQLITE_PAGE_SIZE/sizeof(Pgno))

/*
** How big to make the hash table used for locating in-memory pages
** by page number.
*/
#define N_PG_HASH 353

/*
** A open page cache is an instance of the following structure.
*/
struct Pager {
  char *zFilename;            /* Name of the database file */
  char *zJournal;             /* Name of the journal file */
  int fd, jfd;                /* File descriptors for database and journal */
  int nRef;                   /* Sum of PgHdr.nRef */
  int dbSize;                 /* Number of pages in the file */
  int origDbSize;             /* dbSize before the current change */
  int jSize;                  /* Number of pages in the journal */
  int nIdx;                   /* Number of entries in aIdx[] */
  int nPage;                  /* Total number of in-memory pages */
  int mxPage;                 /* Maximum number of pages to hold in cache */
  char state;                 /* SQLITE_UNLOCK, _READLOCK or _WRITELOCK */
  char ioErr;                 /* True if an I/O error has occurred */
  PgHdr *pFirst, *pLast;      /* List of free pages */
  PgHdr *aHash[N_PG_HASH];    /* Hash table to map page number of PgHdr */
  Pgno aIdx[SQLITE_INDEX_SIZE];  /* Current journal index page */
};

/*
** Hash a page number
*/
#define sqlite_pager_hash(PN)  ((PN)%N_PG_HASH)

/*
** Attempt to acquire a read lock (if wrlock==0) or a write lock (if wrlock==1)
** on the database file.  Return 0 on success and non-zero if the lock 
** could not be acquired.
*/
static int sqlite_pager_lock(int fd, int wrlock){
  struct flock lock;
  lock.l_type = write_lock ? F_WRLCK : F_RDLCK;
  return fcntl(fd, F_SETLK, &lock)!=0;
}

/*
** Unlock the database file.
*/
static int sqlite_pager_unlock(fd){
  struct flock lock;
  lock.l_type = F_UNLCK;
  return fcntl(fd, F_SETLK, &lock)!=0;
}

/*
** Find a page in the hash table given its page number.  Return
** a pointer to the page or NULL if not found.
*/
static PgHdr *sqlite_pager_lookup(Pager *pPager, Pgno pgno){
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
static void sqlite_pager_reset(Pager *pPager){
  PgHdr *pPg, *pNext;
  for(pPg=pPager->pFirst; pPg; pPg=pNext){
    pNext = pPg->pNext;
    sqlite_free(pPg);
  }
  pPager->pFirst = 0;
  pPager->pNext = 0;
  memset(pPager->aHash, 0, sizeof(pPager->aHash));
  pPager->nPage = 0;
  if( pPager->state==SQLITE_WRITELOCK ){
    sqlite_pager_rollback(pPager);
  }
  sqlite_pager_unlock(pPager->fd);
  pPager->state = SQLITE_UNLOCK;
  pPager->nRef = 0;
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
** all in-memory cache pages are invalidated and this routine returns
** SQLITE_PROTOCOL.  SQLITE_OK is returned on success.
*/
static int sqlite_pager_unwritelock(Pager *pPager){
  int rc;
  assert( pPager->state==SQLITE_WRITELOCK );
  sqlite_pager_unlock(pPager->fd);
  rc = sqlite_pager_lock(pPager->fd, 0);
  unlink(pPager->zJournal);
  close(pPager->jfd);
  pPager->jfd = -1;
  if( rc!=SQLITE_OK ){
    pPager->state = SQLITE_UNLOCK;
    sqlite_pager_reset(pPager);
    rc = SQLITE_PROTOCOL;
  }else{
    pPager->state = SQLITE_READLOCK;
  }
  return rc;
}


/*
** Playback the journal and thus restore the database file to
** the state it was in before we started making changes.  
**
** A journal consists of multiple segments.  Every segment begins
** with a single page containing SQLITE_INDEX_SIZE page numbers.  This
** first page is called the index.  Most segments have SQLITE_INDEX_SIZE
** additional pages after the index.  The N-th page after the index
** contains the contents of a page in the database file before that
** page was changed.  The N-th entry in the index tells which page
** of the index file the data is for.
**
** The first segment of a journal is formatted slightly differently.
** The first segment contains an index but only SQLITE_INDEX_SIZE-1
** data pages.  The first page number in the index is actually the
** total number of pages in the original file.  This number is used
** to truncate the original database file back to its original size.
** The second number in the index page is the page number for the
** first data page.  And so forth.
**
** We really need to playback the journal beginning at the end
** and working backwards toward the beginning.  That way changes
** to the database are undone in the reverse order from the way they
** were applied.  This is important if the same page is changed
** more than once.  But many operating systems work more efficiently
** if data is read forward instead of backwards.  So for efficiency
** we want to read the data in the forward direction.
**
** This routine starts with the last segment and works backwards
** toward the first.  Within each segment, however, data is read
** in the forward direction for efficiency.  Care is taken that
** only the first appearance of each page is copied over to the
** database file.  If a page appears in the index more than once,
** only the first occurrance is written.  A hash table is used to
** keep track of  which pages have been written and which have not.
*/
static int sqlite_pager_playback(Pager *pPager){
  int nSeg;                           /* Number of segments */
  int i, j;                           /* Loop counters */
  Pgno mxPg = 0;                      /* Size of the original file in pages */
  struct stat statbuf;                /* Used to size the journal */
  Pgno aIndex[SQLITE_INDEX_SIZE];     /* The index page */
  char aBuf[SQLITE_PAGE_SIZE];        /* Page transfer buffer */
  Pgno aHash[SQLITE_INDEX_SIZE*2-1];  /* Hash table for pages read so far */
  int rc;

  /* Figure out how many segments are in the journal.  Remember that
  ** the first segment is one page shorter than the others and that
  ** the last segment may be incomplete.
  */
  if( fstat(pPager->jfd; &statbuf)!=0 ){
    return SQLITE_OK;
  }
  if( statbuf.st_size <= SQLITE_INDEX_SIZE*SQLITE_PAGE_SIZE ){
    nSeg = 1;
  }else{
    int nPage = statbuf.st_size/SQLITE_PAGE_SIZE;
    nPage -= SQLITE_INDEX_SIZE;
    nSeg = 1 + nPage/(SQLITE_INDEX_SIZE+1);
  }

  /* Process segments beginning with the last and working backwards
  ** to the first.
  */
  for(i=nSeg-1; i>=0; i--){
    /* Seek to the beginning of the segment */
    sqlite_pager_seekpage(pPager->jfd, 
        i>0 ? i*(SQLITE_INDEX_SIZE + 1) - 1 : 0,
        SEEK_SET
    );

    /* Initialize the hash table used to avoid copying duplicate pages */
    memset(aHash, 0, sizeof(aHash));

    /* Read the index page */
    sqlite_pager_readpage(pPager->jfd, aIndex);

    /* Extract the original file size from the first index entry if this
    ** is the first segment */   
    if( i==0 ){
      mxPg = aIndex[0];
      aIndex[0] = 0;
    }

    /* Process pages of this segment in forward order
    */
    for(j=0; j<SQLITE_INDEX_SIZE; j++){
      Pgno pgno = aIndex[i];
      void *pBuf;
      PgHdr *pPg;

      /* 0 means "no such page".  Skip zero entries */
      if( pgno==0 ) continue;

      /* Check to see if pgno is in the hash table.  Skip this
      ** entry if it is.
      */
      h = pgno % (SQLITE_PAGE_SIZE-1);
      while( aHash[h]!=0 && aHash[h]!=pgno ){
        h++;
        if( h>=SQLITE_PAGE_SIZE-1 ) h = 0;
      }
      if( aHash[h]==pgno ){
        lseek(pPager->jfd, SQLITE_PAGE_SIZE, SEEK_CUR);
        continue;
      }
      aHash[h] = pgno;

      /* Playback the page.  Update the in-memory copy of the page
      ** at the same time, if there is one.
      */
      pPg = sqlite_pager_lookup(pPager, pgno);
      if( pPg ){
        pBuf = PGHDR_TO_DATA(pPg);
      }else{
        pBuf = aBuf;
      }
      sqlite_pager_readpage(pPager->jfd, pBuf);
      sqlite_pager_seekpage(pPager->fd, pgno, SEEK_SET);
      rc = sqlite_pager_writepage(pPager->fd, pBuf);
      if( rc!=SQLITE_OK ) return rc;
    }
  }

  /* Truncate the database back to its original size
  */
  if( mxPg>0 ){
    ftrucate(pPager->fd, mxPg * SQLITE_PAGE_SIZE);
  }
  return SQLITE_OK;
}

/*
** Create a new page cache and put a pointer to the page cache in *ppPager.
** The file to be cached need not exist.  The file is not opened until
** the first call to sqlite_pager_get() and is only held open until the
** last page is released using sqlite_pager_unref().
*/
int sqlite_pager_open(Pager **ppPager, const char *zFilename, int mxPage){
  Pager *pPager;
  int nameLen;
  int fd;

  fd = open(zFilename, O_RDWR, 0644);
  if( fd<0 ){
    return SQLITE_CANTOPEN;
  }
  nameLen = strlen(zFilename);
  pPager = sqliteMalloc( sizeof(*pPager) + nameLen*2 + 30 );
  if( pPager==0 ) return SQLITE_NOMEM;
  pPager->zFilename = (char*)&pPager[1];
  pPager->zJournal = &pPager->zFilename[nameLen+1];
  strcpy(pPager->zFilename, zFilename);
  strcpy(pPager->zJournal, zFilename);
  strcpy(&pPager->zJournal[nameLen], "-journal");
  pPager->fd = fd;
  pPager->jfd = -1;
  pPager->nRef = 0;
  pPager->dbSize = -1;
  pPager->nPage = 0;
  pPager->mxPage = mxPage>10 ? mxPage : 10;
  pPager->state = SQLITE_UNLOCK;
  pPager->pFirst = 0;
  pPager->pLast = 0;
  memset(pPager->aHash, 0, sizeof(pPager->aHash));
  *ppPager = pPager;
  return SQLITE_OK;
}

/*
** Return the total number of pages in the file opened by pPager.
*/
int sqlite_pager_pagecount(Pager *pPager){
  int n;
  struct stat statbuf;
  if( pPager->dbSize>=0 ){
    return pPager->dbSize;
  }
  if( fstat(pPager->fd, &statbuf)!=0 ){
    n = 0;
  }else{
    n = statbuf.st_size/SQLITE_PAGE_SIZE;
  }
  if( pPager->state!=SQLITE_NOLOCK ){
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
int sqlite_pager_close(Pager *pPager){
  int i;
  PgHdr *pPg;
  switch( pPager->state ){
    case SQLITE_WRITELOCK: {
      sqlite_pager_rollback(pPager);
      sqlite_pager_unlock(pPager->fd);
      break;
    }
    case SQLITE_READLOCK: {
      sqlite_pager_unlock(pPager->fd);
      break;
    }
    default: {
      /* Do nothing */
      break;
    }
  }
  for(i=0; i<N_PG_HASH; i++){
    PgHdr *pNext;
    for(pPg=pPager->aHash[i]; pPg; pPg=pNext){
      pNext = pPg->pNextHash;
      sqliteFree(pPg);
    }
  }
  if( pPager->fd>=0 ) close(pPager->fd);
  assert( pPager->jfd<0 );
  sqliteFree(pPager);
  return SQLITE_OK;
}

/*
** Return the page number for the given page data
*/
int sqlite_pager_pagenumber(void *pData){
  PgHdr *p = DATA_TO_PGHDR(pData);
  return p->pgno;
}

/*
** Acquire a page
*/
int sqlite_pager_get(Pager *pPager, int pgno, void **ppPage){
  PgHdr *pPg;

  /* If this is the first page accessed, then get a read lock
  ** on the database file.
  */
  if( pPager->nRef==0 ){
    if( sqlite_pager_lock(pPager->fd, 0)!=0 ){
      *ppPage = 0;
      return SQLITE_BUSY;
    }

    /* If a journal file exists, try to play it back.
    */
    if( access(pPager->zJournal,0)==0 ){
       int rc;

       /* Open the journal for exclusive access.  Return SQLITE_BUSY if
       ** we cannot get exclusive access to the journal file
       */
       pPager->jfd = open(pPager->zJournal, O_RDONLY, 0);
       if( pPager->jfd<0 || sqlite_pager_lock(pPager->jfd, 1)!=0 ){
         if( pPager->jfd>=0 ){ close(pPager->jfd); pPager->jfd = -1; }
         sqlite_pager_unlock(pPager->fd);
         *ppPage = 0;
         return SQLITE_BUSY;
       }

       /* Get a write lock on the database */
       sqlite_pager_unlock(pPager->fd);
       if( sqlite_pager_lock(pPager->fd, 1)!=0 ){
         *ppPage = 0;
         return SQLITE_PROTOCOL;
       }

       /* Playback and delete the journal.  Drop the database write
       ** lock and reacquire the read lock.
       */
       sqlite_pager_playback(pPager);
       rc = sqlite_pager_unwritelock(pPager);
       if( rc!=SQLITE_OK ){ return SQLITE_PROTOCOL; }
    }
    pPg = 0;
  }else{
    /* Search for page in cache */
    pPg = sqlite_pager_lookup(pPager, pgno);
  }
  if( pPg==0 ){
    int h;
    if( pPager->nPage<pPager->mxPage || pPager->pFirst==0 ){
      /* Create a new page */
      pPg = sqlite_malloc( sizeof(*pPg) + SQLITE_PAGE_SIZE );
      pPg->pPager = pPager;
    }else{
      /* Recycle an older page */
      pPg = pPager->pFirst;
      if( pPg->dirty ){
        int rc;
        sqlite_pager_seekpage(pPager->fd, pPg->pgno, SEEK_SET);
        rc = sqlite_pager_writepage(pPager->fd, PGHDR_TO_DATA(pPg));
        if( rc!=SQLITE_OK ){
          *ppPage = 0;
          return rc;
        }
      } 
      pPager->pFirst = pPg->pNext;
      if( pPager->pFirst ){
        pPager->pFirst->pPrev = 0;
      }else{
        pPager->pLast = 0;
      }
      if( pPg->pNextHash ){
        pPg->pNextHash->pPrevHash = pPg->pPrevHash;
      }
      if( pPg->pPrevHash ){
        pPg->pPrevHash->pNextHash = pPg->pNextHash;
      }else{
        h = sqlite_pager_hash(pPg->pgno);
        assert( pPager->aHash[h]==pPg );
        pPager->aHash[h] = pPg->pNextHash;
      }
    }
    pPg->pgno = pgno;
    pPg->inJournal = 0;
    pPg->dirty = 0;
    pPg->nRef = 1;
    h = sqlite_pager_hash(pgno);
    pPg->pNextHash = pPager->aHash[h];
    pPager->aHash[h] = pPg;
    if( pPg->pNextHash ){
      assert( pPg->pNextHash->pPrevHash==0 );
      pPg->pNextHash->pPrevHash = pPg;
    }
    sqlite_pager_seekpage(pPager->fd, pgno, SEEK_SET);
    sqlite_pager_readpage(pPager->fd, PGHDR_TO_DATA(pPg));
  }else{
    if( pPg->nRef==0 ){
      if( pPg->pPrev ){
        pPg->pPrev->pNext = pPg->pNext;
      }else{
        pPager->pFirst = pPg->pNext;
      }
      if( pPg->pNext ){
        pPg->pNext->pPrev = pPg->pPrev;
      }else{
        pPager->pLast = pPg->pPrev;
      }
    }
    pPg->nRef++;
  }
  *ppPage = PGHDR_TO_DATA(pPg);
  return SQLITE_OK;
}

/*
** Release a page.
**
** If the number of references to the page drop to zero, then the
** page is added to the LRU list.  When all references to all pages
** are released, a rollback occurs, and the lock on the database is
** removed.
*/
int sqlite_pager_unref(void *pData){
  Pager *pPager;
  PgHdr *pPg;
  pPg = DATA_TO_PGHDR(pData);
  assert( pPg->nRef>0 );
  pPager = pPg->pPager;
  pPg->nRef--;
  if( pPg->nRef==0 ){
    pPg->pNext = 0;
    pPg->pPrev = pPager->pLast;
    pPager->pLast = pPg;
    if( pPg->pPrev ){
      pPg->pPrev->pNext = pPg;
    }else{
      pPager->pFirst = pPg;
    }
  }
  pPager->nRef--;
  assert( pPager->nRef>=0 );
  if( pPager->nRef==0 ){
    sqlite_pager_reset(pPager);
  }
}

/*
** Mark a data page as writeable.  The page is written into the journal 
** if it is not there already.  This routine must be called before making
** changes to a page.
**
** The first time this routine is called, the pager creates a new
** journal and acquires a write lock on the database.  If the write
** lock could not be acquired, this routine returns SQLITE_BUSY.  The
** calling routine must check for that routine and be careful not to
** change any page data until this routine returns SQLITE_OK.
*/
int sqlite_pager_write(void *pData){
  PgHdr *pPg = DATA_TO_PGHDR(pData);
  Pager *pPager = pPg->pPager;
  int rc;

  if( pPg->inJournal ){ return SQLITE_OK; }
  if( pPager->state==SQLITE_UNLOCK ){ return SQLITE_PROTOCOL; }
  if( pPager->state==SQLITE_READLOCK ){
    pPager->jfd = open(pPager->zJournal, O_RDWR|O_CREAT, 0644);
    if( pPager->jfd<0 ){
      return SQLITE_CANTOPEN;
    }
    if( sqlite_pager_lock(pPager->jfd, 1) ){
      close(pPager->jfd);
      pPager->jfd = -1;
      return SQLITE_BUSY;
    }
    sqlite_pager_unlock(pPager->fd);
    if( sqlite_pager_lock(pPager->fd, 1) ){
      close(pPager->jfd);
      pPager->jfd = -1;
      pPager->state = SQLITE_UNLOCK;
      sqlite_pager_reset(pPager);
      return SQLITE_PROTOCOL;
    }
    pPager->state = SQLITE_WRITELOCK;
    pPager->jSize = 1;
    pPager->aIdx[0] = pPager->dbSize;
    pPager->origDbSize = pPager->dbSize;
    pPager->nIdx = 1;
  }
  /* Write this page to the journal */
  assert( pPager->jfd>=0 );
  if( pPg->pgno >= pPager->origDbSize ){
    sqlite_pager_seekpage(pPager->fd, pPg->pgno, SEEK_SET);
    rc = sqlite_pager_writepage(pPager->fd, pData);
    pPg->inJournal = 1;
    return rc;
  }
  pPager->aIdx[pPager->nIdx++] = pPg->pgno;
  sqlite_pager_seekpage(pPager->jfd, pPager->jSize++, SEEK_SET);
  rc = sqlite_pager_write(pPager->jfd, pData);
  pPg->inJournal = 1;
  if( pPager->nIdx==SQLITE_INDEX_SIZE ){
    sqlite_pager_seekpage(pPager->jfd, pPager->idxPgno, SEEK_SET);
    rc = sqlite_pager_writepage(pPager->jfd, &pPager->aIdx);
    pPager->nIdx = 0;
    pPager->jSize++;
  }
  return rc;
}

/*
** Commit all changes to the database and release the write lock.
*/
int sqlite_pager_commit(Pager*){
  int i, rc;
  PgHdr *pPg;
  assert( pPager->state==SQLITE_WRITELOCK );
  assert( pPager->jfd>=0 );
  memset(&pPager->aIdx[&pPager->nIdx], 0, 
          (SQLITE_INDEX_SIZE - pPager->nIdx)*sizeof(Pgno));
  sqlite_pager_seekpage(pPager->jfd, pPager->idxPgno, SEEK_SET);
  rc = sqlite_pager_writepage(pPager->jfd, &pPager->aIdx);
  if( fsync(pPager->jfd) ){
    return SQLITE_IOERR;
  }
  for(i=0; i<N_PG_HASH; i++){
    for(pPg=pPager->aHash[i]; pPg; pPg=pPg->pNextHash){
      if( pPg->dirty==0 ) continue;
      rc = sqlite_pager_seekpage(pPager->fd, pPg->pgno, SEEK_SET);
      if( rc!=SQLITE_OK ) return rc;
      rc = sqlite_pager_writePage(pPager->fd, PGHDR_TO_DATA(pPg));
      if( rc!=SQLITE_OK ) return rc;
    }
  }
  if( fsync(pPager->fd) ){
    return SQLITE_IOERR;
  }
  rc = sqlite_pager_unwritelock(pPager);
  return rc;
}

/*
** Rollback all changes.  The database falls back to read-only mode.
** All in-memory cache pages revert to their original data contents.
** The journal is deleted.
*/
int sqlite_pager_rollback(Pager *pPager){
  int rc;
  if( pPager->state!=SQLITE_WRITELOCK ) return SQLITE_OK;
  memset(&pPager->aIdx[&pPager->nIdx], 0, 
          (SQLITE_INDEX_SIZE - pPager->nIdx)*sizeof(Pgno));
  sqlite_pager_seekpage(pPager->jfd, pPager->idxPgno, SEEK_SET);
  rc = sqlite_pager_writepage(pPager->jfd, &pPager->aIdx);
  rc = sqlite_pager_playback(pPager);
  if( rc!=SQLITE_OK ){
    rc = sqlite_pager_unwritelock(pPager);
  }
  return rc;
};

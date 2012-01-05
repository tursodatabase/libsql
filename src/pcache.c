/*
** 2008 August 05
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
*************************************************************************
** This file implements that page cache.
*/
#include "sqliteInt.h"

/*
** A complete page cache is an instance of this structure.
*/
struct PCache {
  PgHdr *pDirty, *pDirtyTail;         /* List of dirty pages in LRU order */
  PgHdr *pSynced;                     /* Last synced page in dirty page list */
  int nRef;                           /* Number of referenced pages */
  int szCache;                        /* Configured cache size */
  int szPage;                         /* Size of every page in this cache */
  int szExtra;                        /* Size of extra space for each page */
  int bPurgeable;                     /* True if pages are on backing store */
  int (*xStress)(void*,PgHdr*);       /* Call to try make a page clean */
  void *pStress;                      /* Argument to xStress */
  sqlite3_pcache *pCache;             /* Pluggable cache module */
  PgHdr *pPage1;                      /* Reference to page 1 */
};

/*
** Some of the assert() macros in this code are too expensive to run
** even during normal debugging.  Use them only rarely on long-running
** tests.  Enable the expensive asserts using the
** -DSQLITE_ENABLE_EXPENSIVE_ASSERT=1 compile-time option.
*/
#ifdef SQLITE_ENABLE_EXPENSIVE_ASSERT
# define expensive_assert(X)  assert(X)
#else
# define expensive_assert(X)
#endif

/********************************** Linked List Management ********************/

#if !defined(NDEBUG) && defined(SQLITE_ENABLE_EXPENSIVE_ASSERT)
/*
** Check that the pCache->pSynced variable is set correctly. If it
** is not, either fail an assert or return zero. Otherwise, return
** non-zero. This is only used in debugging builds, as follows:
**
**   expensive_assert( pcacheCheckSynced(pCache) );
*/
static int pcacheCheckSynced(PCache *pCache){
  PgHdr *p;
  for(p=pCache->pDirtyTail; p!=pCache->pSynced; p=p->pDirtyPrev){
    assert( p->nRef || (p->flags&PGHDR_NEED_SYNC) );
  }
  return (p==0 || p->nRef || (p->flags&PGHDR_NEED_SYNC)==0);
}
#endif /* !NDEBUG && SQLITE_ENABLE_EXPENSIVE_ASSERT */

/*
** Remove page pPage from the list of dirty pages.
*/
static void pcacheRemoveFromDirtyList(PgHdr *pPage){
  PCache *p = pPage->pCache;

  assert( pPage->pDirtyNext || pPage==p->pDirtyTail );
  assert( pPage->pDirtyPrev || pPage==p->pDirty );

  /* Update the PCache1.pSynced variable if necessary. */
  if( p->pSynced==pPage ){
    PgHdr *pSynced = pPage->pDirtyPrev;
    while( pSynced && (pSynced->flags&PGHDR_NEED_SYNC) ){
      pSynced = pSynced->pDirtyPrev;
    }
    p->pSynced = pSynced;
  }

  if( pPage->pDirtyNext ){
    pPage->pDirtyNext->pDirtyPrev = pPage->pDirtyPrev;
  }else{
    assert( pPage==p->pDirtyTail );
    p->pDirtyTail = pPage->pDirtyPrev;
  }
  if( pPage->pDirtyPrev ){
    pPage->pDirtyPrev->pDirtyNext = pPage->pDirtyNext;
  }else{
    assert( pPage==p->pDirty );
    p->pDirty = pPage->pDirtyNext;
  }
  pPage->pDirtyNext = 0;
  pPage->pDirtyPrev = 0;

  expensive_assert( pcacheCheckSynced(p) );
}

/*
** Add page pPage to the head of the dirty list (PCache1.pDirty is set to
** pPage).
*/
static void pcacheAddToDirtyList(PgHdr *pPage){
  PCache *p = pPage->pCache;

  assert( pPage->pDirtyNext==0 && pPage->pDirtyPrev==0 && p->pDirty!=pPage );

  pPage->pDirtyNext = p->pDirty;
  if( pPage->pDirtyNext ){
    assert( pPage->pDirtyNext->pDirtyPrev==0 );
    pPage->pDirtyNext->pDirtyPrev = pPage;
  }
  p->pDirty = pPage;
  if( !p->pDirtyTail ){
    p->pDirtyTail = pPage;
  }
  if( !p->pSynced && 0==(pPage->flags&PGHDR_NEED_SYNC) ){
    p->pSynced = pPage;
  }
  expensive_assert( pcacheCheckSynced(p) );
}

/*
** Wrapper around the pluggable caches xUnpin method. If the cache is
** being used for an in-memory database, this function is a no-op.
*/
static void pcacheUnpin(PgHdr *p){
  PCache *pCache = p->pCache;
  if( pCache->bPurgeable ){
    if( p->pgno==1 ){
      pCache->pPage1 = 0;
    }
    sqlite3GlobalConfig.pcache2.xUnpin(pCache->pCache, p->pPage, 0);
  }
}

/*************************************************** General Interfaces ******
**
** Initialize and shutdown the page cache subsystem. Neither of these 
** functions are threadsafe.
*/
int sqlite3PcacheInitialize(void){
  if( sqlite3GlobalConfig.pcache2.xInit==0 ){
    /* IMPLEMENTATION-OF: R-26801-64137 If the xInit() method is NULL, then the
    ** built-in default page cache is used instead of the application defined
    ** page cache. */
    sqlite3PCacheSetDefault();
  }
  return sqlite3GlobalConfig.pcache2.xInit(sqlite3GlobalConfig.pcache2.pArg);
}
void sqlite3PcacheShutdown(void){
  if( sqlite3GlobalConfig.pcache2.xShutdown ){
    /* IMPLEMENTATION-OF: R-26000-56589 The xShutdown() method may be NULL. */
    sqlite3GlobalConfig.pcache2.xShutdown(sqlite3GlobalConfig.pcache2.pArg);
  }
}

/*
** Return the size in bytes of a PCache object.
*/
int sqlite3PcacheSize(void){ return sizeof(PCache); }

/*
** Create a new PCache object. Storage space to hold the object
** has already been allocated and is passed in as the p pointer. 
** The caller discovers how much space needs to be allocated by 
** calling sqlite3PcacheSize().
*/
void sqlite3PcacheOpen(
  int szPage,                  /* Size of every page */
  int szExtra,                 /* Extra space associated with each page */
  int bPurgeable,              /* True if pages are on backing store */
  int (*xStress)(void*,PgHdr*),/* Call to try to make pages clean */
  void *pStress,               /* Argument to xStress */
  PCache *p                    /* Preallocated space for the PCache */
){
  memset(p, 0, sizeof(PCache));
  p->szPage = szPage;
  p->szExtra = szExtra;
  p->bPurgeable = bPurgeable;
  p->xStress = xStress;
  p->pStress = pStress;
  p->szCache = 100;
}

/*
** Change the page size for PCache object. The caller must ensure that there
** are no outstanding page references when this function is called.
*/
void sqlite3PcacheSetPageSize(PCache *pCache, int szPage){
  assert( pCache->nRef==0 && pCache->pDirty==0 );
  if( pCache->pCache ){
    sqlite3GlobalConfig.pcache2.xDestroy(pCache->pCache);
    pCache->pCache = 0;
    pCache->pPage1 = 0;
  }
  pCache->szPage = szPage;
}

/*
** Compute the number of pages of cache requested.
*/
static int numberOfCachePages(PCache *p){
  if( p->szCache>=0 ){
    return p->szCache;
  }else{
    return (int)((-1024*(i64)p->szCache)/(p->szPage+p->szExtra));
  }
}

/*
** Try to obtain a page from the cache.
*/
int sqlite3PcacheFetch(
  PCache *pCache,       /* Obtain the page from this cache */
  Pgno pgno,            /* Page number to obtain */
  int createFlag,       /* If true, create page if it does not exist already */
  PgHdr **ppPage        /* Write the page here */
){
  sqlite3_pcache_page *pPage = 0;
  PgHdr *pPgHdr = 0;
  int eCreate;

  assert( pCache!=0 );
  assert( createFlag==1 || createFlag==0 );
  assert( pgno>0 );

  /* If the pluggable cache (sqlite3_pcache*) has not been allocated,
  ** allocate it now.
  */
  if( !pCache->pCache && createFlag ){
    sqlite3_pcache *p;
    p = sqlite3GlobalConfig.pcache2.xCreate(
        pCache->szPage, pCache->szExtra + sizeof(PgHdr), pCache->bPurgeable
    );
    if( !p ){
      return SQLITE_NOMEM;
    }
    sqlite3GlobalConfig.pcache2.xCachesize(p, numberOfCachePages(pCache));
    pCache->pCache = p;
  }

  eCreate = createFlag * (1 + (!pCache->bPurgeable || !pCache->pDirty));
  if( pCache->pCache ){
    pPage = sqlite3GlobalConfig.pcache2.xFetch(pCache->pCache, pgno, eCreate);
  }

  if( !pPage && eCreate==1 ){
    PgHdr *pPg;

    /* Find a dirty page to write-out and recycle. First try to find a 
    ** page that does not require a journal-sync (one with PGHDR_NEED_SYNC
    ** cleared), but if that is not possible settle for any other 
    ** unreferenced dirty page.
    */
    expensive_assert( pcacheCheckSynced(pCache) );
    for(pPg=pCache->pSynced; 
        pPg && (pPg->nRef || (pPg->flags&PGHDR_NEED_SYNC)); 
        pPg=pPg->pDirtyPrev
    );
    pCache->pSynced = pPg;
    if( !pPg ){
      for(pPg=pCache->pDirtyTail; pPg && pPg->nRef; pPg=pPg->pDirtyPrev);
    }
    if( pPg ){
      int rc;
#ifdef SQLITE_LOG_CACHE_SPILL
      sqlite3_log(SQLITE_FULL, 
                  "spill page %d making room for %d - cache used: %d/%d",
                  pPg->pgno, pgno,
                  sqlite3GlobalConfig.pcache.xPagecount(pCache->pCache),
                  numberOfCachePages(pCache));
#endif
      rc = pCache->xStress(pCache->pStress, pPg);
      if( rc!=SQLITE_OK && rc!=SQLITE_BUSY ){
        return rc;
      }
    }

    pPage = sqlite3GlobalConfig.pcache2.xFetch(pCache->pCache, pgno, 2);
  }

  if( pPage ){
    pPgHdr = (PgHdr *)pPage->pExtra;

    if( !pPgHdr->pPage ){
      memset(pPgHdr, 0, sizeof(PgHdr));
      pPgHdr->pPage = pPage;
      pPgHdr->pData = pPage->pBuf;
      pPgHdr->pExtra = (void *)&pPgHdr[1];
      memset(pPgHdr->pExtra, 0, pCache->szExtra);
      pPgHdr->pCache = pCache;
      pPgHdr->pgno = pgno;
    }
    assert( pPgHdr->pCache==pCache );
    assert( pPgHdr->pgno==pgno );
    assert( pPgHdr->pData==pPage->pBuf );
    assert( pPgHdr->pExtra==(void *)&pPgHdr[1] );

    if( 0==pPgHdr->nRef ){
      pCache->nRef++;
    }
    pPgHdr->nRef++;
    if( pgno==1 ){
      pCache->pPage1 = pPgHdr;
    }
  }
  *ppPage = pPgHdr;
  return (pPgHdr==0 && eCreate) ? SQLITE_NOMEM : SQLITE_OK;
}

/*
** Decrement the reference count on a page. If the page is clean and the
** reference count drops to 0, then it is made elible for recycling.
*/
void sqlite3PcacheRelease(PgHdr *p){
  assert( p->nRef>0 );
  p->nRef--;
  if( p->nRef==0 ){
    PCache *pCache = p->pCache;
    pCache->nRef--;
    if( (p->flags&PGHDR_DIRTY)==0 ){
      pcacheUnpin(p);
    }else{
      /* Move the page to the head of the dirty list. */
      pcacheRemoveFromDirtyList(p);
      pcacheAddToDirtyList(p);
    }
  }
}

/*
** Increase the reference count of a supplied page by 1.
*/
void sqlite3PcacheRef(PgHdr *p){
  assert(p->nRef>0);
  p->nRef++;
}

/*
** Drop a page from the cache. There must be exactly one reference to the
** page. This function deletes that reference, so after it returns the
** page pointed to by p is invalid.
*/
void sqlite3PcacheDrop(PgHdr *p){
  PCache *pCache;
  assert( p->nRef==1 );
  if( p->flags&PGHDR_DIRTY ){
    pcacheRemoveFromDirtyList(p);
  }
  pCache = p->pCache;
  pCache->nRef--;
  if( p->pgno==1 ){
    pCache->pPage1 = 0;
  }
  sqlite3GlobalConfig.pcache2.xUnpin(pCache->pCache, p->pPage, 1);
}

/*
** Make sure the page is marked as dirty. If it isn't dirty already,
** make it so.
*/
void sqlite3PcacheMakeDirty(PgHdr *p){
  p->flags &= ~PGHDR_DONT_WRITE;
  assert( p->nRef>0 );
  if( 0==(p->flags & PGHDR_DIRTY) ){
    p->flags |= PGHDR_DIRTY;
    pcacheAddToDirtyList( p);
  }
}

/*
** Make sure the page is marked as clean. If it isn't clean already,
** make it so.
*/
void sqlite3PcacheMakeClean(PgHdr *p){
  if( (p->flags & PGHDR_DIRTY) ){
    pcacheRemoveFromDirtyList(p);
    p->flags &= ~(PGHDR_DIRTY|PGHDR_NEED_SYNC);
    if( p->nRef==0 ){
      pcacheUnpin(p);
    }
  }
}

/*
** Make every page in the cache clean.
*/
void sqlite3PcacheCleanAll(PCache *pCache){
  PgHdr *p;
  while( (p = pCache->pDirty)!=0 ){
    sqlite3PcacheMakeClean(p);
  }
}

/*
** Clear the PGHDR_NEED_SYNC flag from all dirty pages.
*/
void sqlite3PcacheClearSyncFlags(PCache *pCache){
  PgHdr *p;
  for(p=pCache->pDirty; p; p=p->pDirtyNext){
    p->flags &= ~PGHDR_NEED_SYNC;
  }
  pCache->pSynced = pCache->pDirtyTail;
}

/*
** Change the page number of page p to newPgno. 
*/
void sqlite3PcacheMove(PgHdr *p, Pgno newPgno){
  PCache *pCache = p->pCache;
  assert( p->nRef>0 );
  assert( newPgno>0 );
  sqlite3GlobalConfig.pcache2.xRekey(pCache->pCache, p->pPage, p->pgno,newPgno);
  p->pgno = newPgno;
  if( (p->flags&PGHDR_DIRTY) && (p->flags&PGHDR_NEED_SYNC) ){
    pcacheRemoveFromDirtyList(p);
    pcacheAddToDirtyList(p);
  }
}

/*
** Drop every cache entry whose page number is greater than "pgno". The
** caller must ensure that there are no outstanding references to any pages
** other than page 1 with a page number greater than pgno.
**
** If there is a reference to page 1 and the pgno parameter passed to this
** function is 0, then the data area associated with page 1 is zeroed, but
** the page object is not dropped.
*/
void sqlite3PcacheTruncate(PCache *pCache, Pgno pgno){
  if( pCache->pCache ){
    PgHdr *p;
    PgHdr *pNext;
    for(p=pCache->pDirty; p; p=pNext){
      pNext = p->pDirtyNext;
      /* This routine never gets call with a positive pgno except right
      ** after sqlite3PcacheCleanAll().  So if there are dirty pages,
      ** it must be that pgno==0.
      */
      assert( p->pgno>0 );
      if( ALWAYS(p->pgno>pgno) ){
        assert( p->flags&PGHDR_DIRTY );
        sqlite3PcacheMakeClean(p);
      }
    }
    if( pgno==0 && pCache->pPage1 ){
      memset(pCache->pPage1->pData, 0, pCache->szPage);
      pgno = 1;
    }
    sqlite3GlobalConfig.pcache2.xTruncate(pCache->pCache, pgno+1);
  }
}

/*
** Close a cache.
*/
void sqlite3PcacheClose(PCache *pCache){
  if( pCache->pCache ){
    sqlite3GlobalConfig.pcache2.xDestroy(pCache->pCache);
  }
}

/* 
** Discard the contents of the cache.
*/
void sqlite3PcacheClear(PCache *pCache){
  sqlite3PcacheTruncate(pCache, 0);
}

/*
** Merge two lists of pages connected by pDirty and in pgno order.
** Do not both fixing the pDirtyPrev pointers.
*/
static PgHdr *pcacheMergeDirtyList(PgHdr *pA, PgHdr *pB){
  PgHdr result, *pTail;
  pTail = &result;
  while( pA && pB ){
    if( pA->pgno<pB->pgno ){
      pTail->pDirty = pA;
      pTail = pA;
      pA = pA->pDirty;
    }else{
      pTail->pDirty = pB;
      pTail = pB;
      pB = pB->pDirty;
    }
  }
  if( pA ){
    pTail->pDirty = pA;
  }else if( pB ){
    pTail->pDirty = pB;
  }else{
    pTail->pDirty = 0;
  }
  return result.pDirty;
}

/*
** Sort the list of pages in accending order by pgno.  Pages are
** connected by pDirty pointers.  The pDirtyPrev pointers are
** corrupted by this sort.
**
** Since there cannot be more than 2^31 distinct pages in a database,
** there cannot be more than 31 buckets required by the merge sorter.
** One extra bucket is added to catch overflow in case something
** ever changes to make the previous sentence incorrect.
*/
#define N_SORT_BUCKET  32
static PgHdr *pcacheSortDirtyList(PgHdr *pIn){
  PgHdr *a[N_SORT_BUCKET], *p;
  int i;
  memset(a, 0, sizeof(a));
  while( pIn ){
    p = pIn;
    pIn = p->pDirty;
    p->pDirty = 0;
    for(i=0; ALWAYS(i<N_SORT_BUCKET-1); i++){
      if( a[i]==0 ){
        a[i] = p;
        break;
      }else{
        p = pcacheMergeDirtyList(a[i], p);
        a[i] = 0;
      }
    }
    if( NEVER(i==N_SORT_BUCKET-1) ){
      /* To get here, there need to be 2^(N_SORT_BUCKET) elements in
      ** the input list.  But that is impossible.
      */
      a[i] = pcacheMergeDirtyList(a[i], p);
    }
  }
  p = a[0];
  for(i=1; i<N_SORT_BUCKET; i++){
    p = pcacheMergeDirtyList(p, a[i]);
  }
  return p;
}

/*
** Return a list of all dirty pages in the cache, sorted by page number.
*/
PgHdr *sqlite3PcacheDirtyList(PCache *pCache){
  PgHdr *p;
  for(p=pCache->pDirty; p; p=p->pDirtyNext){
    p->pDirty = p->pDirtyNext;
  }
  return pcacheSortDirtyList(pCache->pDirty);
}

/* 
** Return the total number of referenced pages held by the cache.
*/
int sqlite3PcacheRefCount(PCache *pCache){
  return pCache->nRef;
}

/*
** Return the number of references to the page supplied as an argument.
*/
int sqlite3PcachePageRefcount(PgHdr *p){
  return p->nRef;
}

/* 
** Return the total number of pages in the cache.
*/
int sqlite3PcachePagecount(PCache *pCache){
  int nPage = 0;
  if( pCache->pCache ){
    nPage = sqlite3GlobalConfig.pcache2.xPagecount(pCache->pCache);
  }
  return nPage;
}

#ifdef SQLITE_TEST
/*
** Get the suggested cache-size value.
*/
int sqlite3PcacheGetCachesize(PCache *pCache){
  return numberOfCachePages(pCache);
}
#endif

/*
** Set the suggested cache-size value.
*/
void sqlite3PcacheSetCachesize(PCache *pCache, int mxPage){
  pCache->szCache = mxPage;
  if( pCache->pCache ){
    sqlite3GlobalConfig.pcache2.xCachesize(pCache->pCache,
                                           numberOfCachePages(pCache));
  }
}

/*
** Free up as much memory as possible from the page cache.
*/
void sqlite3PcacheShrink(PCache *pCache){
  if( pCache->pCache ){
    sqlite3GlobalConfig.pcache2.xShrink(pCache->pCache);
  }
}

#if defined(SQLITE_CHECK_PAGES) || defined(SQLITE_DEBUG)
/*
** For all dirty pages currently in the cache, invoke the specified
** callback. This is only used if the SQLITE_CHECK_PAGES macro is
** defined.
*/
void sqlite3PcacheIterateDirty(PCache *pCache, void (*xIter)(PgHdr *)){
  PgHdr *pDirty;
  for(pDirty=pCache->pDirty; pDirty; pDirty=pDirty->pDirtyNext){
    xIter(pDirty);
  }
}
#endif

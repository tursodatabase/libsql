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
**
** @(#) $Id: pcache.c,v 1.24 2008/08/29 09:10:03 danielk1977 Exp $
*/
#include "sqliteInt.h"

/*
** A complete page cache is an instance of this structure.
**
** A cache may only be deleted by its owner and while holding the
** SQLITE_MUTEX_STATUS_LRU mutex.
*/
struct PCache {
  /*********************************************************************
  ** The first group of elements may be read or written at any time by
  ** the cache owner without holding the mutex.  No thread other than the
  ** cache owner is permitted to access these elements at any time.
  */
  PgHdr *pDirty, *pDirtyTail;         /* List of dirty pages in LRU order */
  PgHdr *pSynced;                     /* Last synced page in dirty page list */
  int nRef;                           /* Number of pinned pages */
  int nPinned;                        /* Number of pinned and/or dirty pages */
  int nMax;                           /* Configured cache size */
  int nMin;                           /* Configured minimum cache size */
  /**********************************************************************
  ** The next group of elements are fixed when the cache is created and
  ** may not be changed afterwards.  These elements can read at any time by
  ** the cache owner or by any thread holding the the mutex.  Non-owner
  ** threads must hold the mutex when reading these elements to prevent
  ** the entire PCache object from being deleted during the read.
  */
  int szPage;                         /* Size of every page in this cache */
  int szExtra;                        /* Size of extra space for each page */
  int bPurgeable;                     /* True if pages are on backing store */
  void (*xDestroy)(PgHdr*);           /* Called when refcnt goes 1->0 */
  int (*xStress)(void*,PgHdr*);       /* Call to try make a page clean */
  void *pStress;                      /* Argument to xStress */
  /**********************************************************************
  ** The final group of elements can only be accessed while holding the
  ** mutex.  Both the cache owner and any other thread must hold the mutex
  ** to read or write any of these elements.
  */
  int nPage;                          /* Total number of pages in apHash */
  int nHash;                          /* Number of slots in apHash[] */
  PgHdr **apHash;                     /* Hash table for fast lookup by pgno */
  PgHdr *pClean;                      /* List of clean pages in use */
};

/*
** Free slots in the page block allocator
*/
typedef struct PgFreeslot PgFreeslot;
struct PgFreeslot {
  PgFreeslot *pNext;  /* Next free slot */
};

/*
** Global data for the page cache.
*/
static struct PCacheGlobal {
  int isInit;                         /* True when initialized */
  sqlite3_mutex *mutex;               /* static mutex MUTEX_STATIC_LRU */

  int nMaxPage;                       /* Sum of nMaxPage for purgeable caches */
  int nMinPage;                       /* Sum of nMinPage for purgeable caches */
  int nCurrentPage;                   /* Number of purgeable pages allocated */
  PgHdr *pLruHead, *pLruTail;         /* LRU list of unused clean pgs */

  /* Variables related to SQLITE_CONFIG_PAGECACHE settings. */
  int szSlot;                         /* Size of each free slot */
  void *pStart, *pEnd;                /* Bounds of pagecache malloc range */
  PgFreeslot *pFree;                  /* Free page blocks */
} pcache = {0};

/*
** All global variables used by this module (all of which are grouped 
** together in global structure "pcache" above) are protected by the static 
** SQLITE_MUTEX_STATIC_LRU mutex. A pointer to this mutex is stored in
** variable "pcache.mutex".
**
** Some elements of the PCache and PgHdr structures are protected by the 
** SQLITE_MUTEX_STATUS_LRU mutex and other are not.  The protected
** elements are grouped at the end of the structures and are clearly
** marked.
**
** Use the following macros must surround all access (read or write)
** of protected elements.  The mutex is not recursive and may not be
** entered more than once.  The pcacheMutexHeld() macro should only be
** used within an assert() to verify that the mutex is being held.
*/
#define pcacheEnterMutex() sqlite3_mutex_enter(pcache.mutex)
#define pcacheExitMutex()  sqlite3_mutex_leave(pcache.mutex)
#define pcacheMutexHeld()  sqlite3_mutex_held(pcache.mutex)

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
** This routine verifies that the number of entries in the hash table
** is pCache->nPage.  This routine is used within assert() statements
** only and is therefore disabled during production builds.
*/
static int pcacheCheckHashCount(PCache *pCache){
  int i;
  int nPage = 0;
  for(i=0; i<pCache->nHash; i++){
    PgHdr *p;
    for(p=pCache->apHash[i]; p; p=p->pNextHash){
      nPage++;
    }
  }
  assert( nPage==pCache->nPage );
  return 1;
}
#endif /* !NDEBUG && SQLITE_ENABLE_EXPENSIVE_ASSERT */


#if !defined(NDEBUG) && defined(SQLITE_ENABLE_EXPENSIVE_ASSERT)
/*
** Based on the current value of PCache.nRef and the contents of the
** PCache.pDirty list, return the expected value of the PCache.nPinned
** counter. This is only used in debugging builds, as follows:
**
**   expensive_assert( pCache->nPinned==pcachePinnedCount(pCache) );
*/
static int pcachePinnedCount(PCache *pCache){
  PgHdr *p;
  int nPinned = pCache->nRef;
  for(p=pCache->pDirty; p; p=p->pNext){
    if( p->nRef==0 ){
      nPinned++;
    }
  }
  return nPinned;
}
#endif /* !NDEBUG && SQLITE_ENABLE_EXPENSIVE_ASSERT */


#if !defined(NDEBUG) && defined(SQLITE_ENABLE_EXPENSIVE_ASSERT)
/*
** Check that the pCache->pSynced variable is set correctly. If it
** is not, either fail an assert or return zero. Otherwise, return
** non-zero. This is only used in debugging builds, as follows:
**
**   expensive_assert( pcacheCheckSynced(pCache) );
*/
static int pcacheCheckSynced(PCache *pCache){
  PgHdr *p = pCache->pDirtyTail;
  for(p=pCache->pDirtyTail; p!=pCache->pSynced; p=p->pPrev){
    assert( p->nRef || (p->flags&PGHDR_NEED_SYNC) );
  }
  return (p==0 || p->nRef || (p->flags&PGHDR_NEED_SYNC)==0);
}
#endif /* !NDEBUG && SQLITE_ENABLE_EXPENSIVE_ASSERT */



/*
** Remove a page from its hash table (PCache.apHash[]).
*/
static void pcacheRemoveFromHash(PgHdr *pPage){
  assert( pcacheMutexHeld() );
  if( pPage->pPrevHash ){
    pPage->pPrevHash->pNextHash = pPage->pNextHash;
  }else{
    PCache *pCache = pPage->pCache;
    u32 h = pPage->pgno % pCache->nHash;
    assert( pCache->apHash[h]==pPage );
    pCache->apHash[h] = pPage->pNextHash;
  }
  if( pPage->pNextHash ){
    pPage->pNextHash->pPrevHash = pPage->pPrevHash;
  }
  pPage->pCache->nPage--;
  expensive_assert( pcacheCheckHashCount(pPage->pCache) );
}

/*
** Insert a page into the hash table
**
** The mutex must be held by the caller.
*/
static void pcacheAddToHash(PgHdr *pPage){
  PCache *pCache = pPage->pCache;
  u32 h = pPage->pgno % pCache->nHash;
  assert( pcacheMutexHeld() );
  pPage->pNextHash = pCache->apHash[h];
  pPage->pPrevHash = 0;
  if( pCache->apHash[h] ){
    pCache->apHash[h]->pPrevHash = pPage;
  }
  pCache->apHash[h] = pPage;
  pCache->nPage++;
  expensive_assert( pcacheCheckHashCount(pCache) );
}

/*
** Attempt to increase the size the hash table to contain
** at least nHash buckets.
*/
static int pcacheResizeHash(PCache *pCache, int nHash){
  PgHdr *p;
  PgHdr **pNew;
  assert( pcacheMutexHeld() );
#ifdef SQLITE_MALLOC_SOFT_LIMIT
  if( nHash*sizeof(PgHdr*)>SQLITE_MALLOC_SOFT_LIMIT ){
    nHash = SQLITE_MALLOC_SOFT_LIMIT/sizeof(PgHdr *);
  }
#endif
  pcacheExitMutex();
  pNew = (PgHdr **)sqlite3Malloc(sizeof(PgHdr*)*nHash);
  pcacheEnterMutex();
  if( !pNew ){
    return SQLITE_NOMEM;
  }
  memset(pNew, 0, sizeof(PgHdr *)*nHash);
  sqlite3_free(pCache->apHash);
  pCache->apHash = pNew;
  pCache->nHash = nHash;
  pCache->nPage = 0;
 
  for(p=pCache->pClean; p; p=p->pNext){
    pcacheAddToHash(p);
  }
  for(p=pCache->pDirty; p; p=p->pNext){
    pcacheAddToHash(p);
  }
  return SQLITE_OK;
}

/*
** Remove a page from a linked list that is headed by *ppHead.
** *ppHead is either PCache.pClean or PCache.pDirty.
*/
static void pcacheRemoveFromList(PgHdr **ppHead, PgHdr *pPage){
  int isDirtyList = (ppHead==&pPage->pCache->pDirty);
  assert( ppHead==&pPage->pCache->pClean || ppHead==&pPage->pCache->pDirty );
  assert( pcacheMutexHeld() || ppHead!=&pPage->pCache->pClean );

  if( pPage->pPrev ){
    pPage->pPrev->pNext = pPage->pNext;
  }else{
    assert( *ppHead==pPage );
    *ppHead = pPage->pNext;
  }
  if( pPage->pNext ){
    pPage->pNext->pPrev = pPage->pPrev;
  }

  if( isDirtyList ){
    PCache *pCache = pPage->pCache;
    assert( pPage->pNext || pCache->pDirtyTail==pPage );
    if( !pPage->pNext ){
      pCache->pDirtyTail = pPage->pPrev;
    }
    if( pCache->pSynced==pPage ){
      PgHdr *pSynced = pPage->pPrev;
      while( pSynced && (pSynced->flags&PGHDR_NEED_SYNC) ){
        pSynced = pSynced->pPrev;
      }
      pCache->pSynced = pSynced;
    }
  }
}

/*
** Add a page from a linked list that is headed by *ppHead.
** *ppHead is either PCache.pClean or PCache.pDirty.
*/
static void pcacheAddToList(PgHdr **ppHead, PgHdr *pPage){
  int isDirtyList = (ppHead==&pPage->pCache->pDirty);
  assert( ppHead==&pPage->pCache->pClean || ppHead==&pPage->pCache->pDirty );

  if( (*ppHead) ){
    (*ppHead)->pPrev = pPage;
  }
  pPage->pNext = *ppHead;
  pPage->pPrev = 0;
  *ppHead = pPage;

  if( isDirtyList ){
    PCache *pCache = pPage->pCache;
    if( !pCache->pDirtyTail ){
      assert( pPage->pNext==0 );
      pCache->pDirtyTail = pPage;
    }
    if( !pCache->pSynced && 0==(pPage->flags&PGHDR_NEED_SYNC) ){
      pCache->pSynced = pPage;
    }
  }
}

/*
** Remove a page from the global LRU list
*/
static void pcacheRemoveFromLruList(PgHdr *pPage){
  assert( sqlite3_mutex_held(pcache.mutex) );
  assert( (pPage->flags&PGHDR_DIRTY)==0 );
  if( pPage->pCache->bPurgeable==0 ) return;
  if( pPage->pNextLru ){
    assert( pcache.pLruTail!=pPage );
    pPage->pNextLru->pPrevLru = pPage->pPrevLru;
  }else{
    assert( pcache.pLruTail==pPage );
    pcache.pLruTail = pPage->pPrevLru;
  }
  if( pPage->pPrevLru ){
    assert( pcache.pLruHead!=pPage );
    pPage->pPrevLru->pNextLru = pPage->pNextLru;
  }else{
    assert( pcache.pLruHead==pPage );
    pcache.pLruHead = pPage->pNextLru;
  }
}

/*
** Add a page to the global LRU list.  The page is normally added
** to the front of the list so that it will be the last page recycled.
** However, if the PGHDR_REUSE_UNLIKELY bit is set, the page is added
** to the end of the LRU list so that it will be the next to be recycled.
*/
static void pcacheAddToLruList(PgHdr *pPage){
  assert( sqlite3_mutex_held(pcache.mutex) );
  assert( (pPage->flags&PGHDR_DIRTY)==0 );
  if( pPage->pCache->bPurgeable==0 ) return;
  if( pcache.pLruTail && (pPage->flags & PGHDR_REUSE_UNLIKELY)!=0 ){
    /* If reuse is unlikely.  Put the page at the end of the LRU list
    ** where it will be recycled sooner rather than later. 
    */
    assert( pcache.pLruHead );
    pPage->pNextLru = 0;
    pPage->pPrevLru = pcache.pLruTail;
    pcache.pLruTail->pNextLru = pPage;
    pcache.pLruTail = pPage;
    pPage->flags &= ~PGHDR_REUSE_UNLIKELY;
  }else{
    /* If reuse is possible. the page goes at the beginning of the LRU
    ** list so that it will be the last to be recycled.
    */
    if( pcache.pLruHead ){
      pcache.pLruHead->pPrevLru = pPage;
    }
    pPage->pNextLru = pcache.pLruHead;
    pcache.pLruHead = pPage;
    pPage->pPrevLru = 0;
    if( pcache.pLruTail==0 ){
      pcache.pLruTail = pPage;
    }
  }
}

/*********************************************** Memory Allocation ***********
**
** Initialize the page cache memory pool.
**
** This must be called at start-time when no page cache lines are
** checked out. This function is not threadsafe.
*/
void sqlite3PCacheBufferSetup(void *pBuf, int sz, int n){
  PgFreeslot *p;
  sz &= ~7;
  pcache.szSlot = sz;
  pcache.pStart = pBuf;
  pcache.pFree = 0;
  while( n-- ){
    p = (PgFreeslot*)pBuf;
    p->pNext = pcache.pFree;
    pcache.pFree = p;
    pBuf = (void*)&((char*)pBuf)[sz];
  }
  pcache.pEnd = pBuf;
}

/*
** Allocate a page cache line.  Look in the page cache memory pool first
** and use an element from it first if available.  If nothing is available
** in the page cache memory pool, go to the general purpose memory allocator.
*/
void *pcacheMalloc(int sz, PCache *pCache){
  assert( sqlite3_mutex_held(pcache.mutex) );
  if( sz<=pcache.szSlot && pcache.pFree ){
    PgFreeslot *p = pcache.pFree;
    pcache.pFree = p->pNext;
    sqlite3StatusSet(SQLITE_STATUS_PAGECACHE_SIZE, sz);
    sqlite3StatusAdd(SQLITE_STATUS_PAGECACHE_USED, 1);
    return (void*)p;
  }else{
    void *p;

    /* Allocate a new buffer using sqlite3Malloc. Before doing so, exit the
    ** global pcache mutex and unlock the pager-cache object pCache. This is 
    ** so that if the attempt to allocate a new buffer causes the the 
    ** configured soft-heap-limit to be breached, it will be possible to
    ** reclaim memory from this pager-cache.
    */
    pcacheExitMutex();
    p = sqlite3Malloc(sz);
    pcacheEnterMutex();

    if( p ){
      sz = sqlite3MallocSize(p);
      sqlite3StatusAdd(SQLITE_STATUS_PAGECACHE_OVERFLOW, sz);
    }
    return p;
  }
}
void *sqlite3PageMalloc(sz){
  void *p;
  pcacheEnterMutex();
  p = pcacheMalloc(sz, 0);
  pcacheExitMutex();
  return p;
}

/*
** Release a pager memory allocation
*/
void pcacheFree(void *p){
  assert( sqlite3_mutex_held(pcache.mutex) );
  if( p==0 ) return;
  if( p>=pcache.pStart && p<pcache.pEnd ){
    PgFreeslot *pSlot;
    sqlite3StatusAdd(SQLITE_STATUS_PAGECACHE_USED, -1);
    pSlot = (PgFreeslot*)p;
    pSlot->pNext = pcache.pFree;
    pcache.pFree = pSlot;
  }else{
    int iSize = sqlite3MallocSize(p);
    sqlite3StatusAdd(SQLITE_STATUS_PAGECACHE_OVERFLOW, -iSize);
    sqlite3_free(p);
  }
}
void sqlite3PageFree(void *p){
  pcacheEnterMutex();
  pcacheFree(p);
  pcacheExitMutex();
}

/*
** Allocate a new page.
*/
static PgHdr *pcachePageAlloc(PCache *pCache){
  PgHdr *p;
  int sz = sizeof(*p) + pCache->szPage + pCache->szExtra;
  assert( sqlite3_mutex_held(pcache.mutex) );
  p = pcacheMalloc(sz, pCache);
  if( p==0 ) return 0;
  memset(p, 0, sizeof(PgHdr));
  p->pData = (void*)&p[1];
  p->pExtra = (void*)&((char*)p->pData)[pCache->szPage];
  if( pCache->bPurgeable ){
    pcache.nCurrentPage++;
  }
  return p;
}

/*
** Deallocate a page
*/
static void pcachePageFree(PgHdr *p){
  assert( sqlite3_mutex_held(pcache.mutex) );
  if( p->pCache->bPurgeable ){
    pcache.nCurrentPage--;
  }
  pcacheFree(p->apSave[0]);
  pcacheFree(p->apSave[1]);
  pcacheFree(p);
}

#ifdef SQLITE_ENABLE_MEMORY_MANAGEMENT
/*
** Return the number of bytes that will be returned to the heap when
** the argument is passed to pcachePageFree().
*/
static int pcachePageSize(PgHdr *p){
  assert( sqlite3_mutex_held(pcache.mutex) );
  assert( !pcache.pStart );
  assert( p->apSave[0]==0 );
  assert( p->apSave[1]==0 );
  assert( p && p->pCache );
  return sqlite3MallocSize(p);
}
#endif

/*
** Attempt to 'recycle' a page from the global LRU list. Only clean,
** unreferenced pages from purgeable caches are eligible for recycling.
**
** This function removes page pcache.pLruTail from the global LRU list,
** and from the hash-table and PCache.pClean list of the owner pcache.
** There should be no other references to the page.
**
** A pointer to the recycled page is returned, or NULL if no page is
** eligible for recycling.
*/
static PgHdr *pcacheRecyclePage(){
  PgHdr *p = 0;
  assert( sqlite3_mutex_held(pcache.mutex) );

  if( (p=pcache.pLruTail) ){
    assert( (p->flags&PGHDR_DIRTY)==0 );
    pcacheRemoveFromLruList(p);
    pcacheRemoveFromHash(p);
    pcacheRemoveFromList(&p->pCache->pClean, p);
  }

  return p;
}

/*
** Obtain space for a page. Try to recycle an old page if the limit on the 
** number of pages has been reached. If the limit has not been reached or
** there are no pages eligible for recycling, allocate a new page.
**
** Return a pointer to the new page, or NULL if an OOM condition occurs.
*/
static int pcacheRecycleOrAlloc(PCache *pCache, PgHdr **ppPage){
  PgHdr *p = 0;

  int szPage = pCache->szPage;
  int szExtra = pCache->szExtra;

  assert( pcache.isInit );
  assert( sqlite3_mutex_held(pcache.mutex) );

  *ppPage = 0;

  /* If we have reached the limit for pinned/dirty pages, and there is at
  ** least one dirty page, invoke the xStress callback to cause a page to
  ** become clean.
  */
  expensive_assert( pCache->nPinned==pcachePinnedCount(pCache) );
  expensive_assert( pcacheCheckSynced(pCache) );
  if( pCache->xStress
   && pCache->pDirty
   && pCache->nPinned>=(pcache.nMaxPage+pCache->nMin-pcache.nMinPage)
  ){
    PgHdr *pPg;
    assert(pCache->pDirtyTail);

    for(pPg=pCache->pSynced; 
        pPg && (pPg->nRef || (pPg->flags&PGHDR_NEED_SYNC)); 
        pPg=pPg->pPrev
    );
    if( !pPg ){
      for(pPg=pCache->pDirtyTail; pPg && pPg->nRef; pPg=pPg->pPrev);
    }
    if( pPg ){
      int rc;
      pcacheExitMutex();
      rc = pCache->xStress(pCache->pStress, pPg);
      pcacheEnterMutex();
      if( rc!=SQLITE_OK && rc!=SQLITE_BUSY ){
        return rc;
      }
    }
  }

  /* If the global page limit has been reached, try to recycle a page. */
  if( pCache->bPurgeable && pcache.nCurrentPage>=pcache.nMaxPage ){
    p = pcacheRecyclePage();
  }

  /* If a page has been recycled but it is the wrong size, free it. */
  if( p && (p->pCache->szPage!=szPage || p->pCache->szPage!=szExtra) ){
    pcachePageFree(p);
    p = 0;
  }

  if( !p ){
    p = pcachePageAlloc(pCache);
  }

  *ppPage = p;
  return (p?SQLITE_OK:SQLITE_NOMEM);
}

/*************************************************** General Interfaces ******
**
** Initialize and shutdown the page cache subsystem. Neither of these 
** functions are threadsafe.
*/
int sqlite3PcacheInitialize(void){
  assert( pcache.isInit==0 );
  memset(&pcache, 0, sizeof(pcache));
  if( sqlite3Config.bCoreMutex ){
    /* No need to check the return value of sqlite3_mutex_alloc(). 
    ** Allocating a static mutex cannot fail.
    */
    pcache.mutex = sqlite3_mutex_alloc(SQLITE_MUTEX_STATIC_LRU);
  }
  pcache.isInit = 1;
  return SQLITE_OK;
}
void sqlite3PcacheShutdown(void){
  memset(&pcache, 0, sizeof(pcache));
}

/*
** Return the size in bytes of a PCache object.
*/
int sqlite3PcacheSize(void){ return sizeof(PCache); }

/*
** Create a new PCache object.  Storage space to hold the object
** has already been allocated and is passed in as the p pointer.
*/
void sqlite3PcacheOpen(
  int szPage,                  /* Size of every page */
  int szExtra,                 /* Extra space associated with each page */
  int bPurgeable,              /* True if pages are on backing store */
  void (*xDestroy)(PgHdr*),    /* Called to destroy a page */
  int (*xStress)(void*,PgHdr*),/* Call to try to make pages clean */
  void *pStress,               /* Argument to xStress */
  PCache *p                    /* Preallocated space for the PCache */
){
  assert( pcache.isInit );
  memset(p, 0, sizeof(PCache));
  p->szPage = szPage;
  p->szExtra = szExtra;
  p->bPurgeable = bPurgeable;
  p->xDestroy = xDestroy;
  p->xStress = xStress;
  p->pStress = pStress;
  p->nMax = 100;
  p->nMin = 10;

  pcacheEnterMutex();
  if( bPurgeable ){
    pcache.nMaxPage += p->nMax;
    pcache.nMinPage += p->nMin;
  }

  pcacheExitMutex();
}

/*
** Change the page size for PCache object.  This can only happen
** when the cache is empty.
*/
void sqlite3PcacheSetPageSize(PCache *pCache, int szPage){
  assert(pCache->nPage==0);
  pCache->szPage = szPage;
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
  int rc = SQLITE_OK;
  PgHdr *pPage = 0;

  assert( pcache.isInit );
  assert( pCache!=0 );
  assert( pgno>0 );
  expensive_assert( pCache->nPinned==pcachePinnedCount(pCache) );

  pcacheEnterMutex();

  /* Search the hash table for the requested page. Exit early if it is found. */
  if( pCache->apHash ){
    u32 h = pgno % pCache->nHash;
    for(pPage=pCache->apHash[h]; pPage; pPage=pPage->pNextHash){
      if( pPage->pgno==pgno ){
        if( pPage->nRef==0 ){
          if( 0==(pPage->flags&PGHDR_DIRTY) ){
            pcacheRemoveFromLruList(pPage);
            pCache->nPinned++;
          }
          pCache->nRef++;
        }
        pPage->nRef++;
        break;
      }
    }
  }

  if( !pPage && createFlag ){
    if( pCache->nHash<=pCache->nPage ){
      rc = pcacheResizeHash(pCache, pCache->nHash<256 ? 256 : pCache->nHash*2);
    }
    if( rc==SQLITE_OK ){
      rc = pcacheRecycleOrAlloc(pCache, &pPage);
    }
    if( rc==SQLITE_OK ){
      pPage->pPager = 0;
      pPage->flags = 0;
      pPage->pDirty = 0;
      pPage->pgno = pgno;
      pPage->pCache = pCache;
      pPage->nRef = 1;
      pCache->nRef++;
      pCache->nPinned++;
      pcacheAddToList(&pCache->pClean, pPage);
      pcacheAddToHash(pPage);
    }
  }

  pcacheExitMutex();

  *ppPage = pPage;
  expensive_assert( pCache->nPinned==pcachePinnedCount(pCache) );
  assert( pPage || !createFlag || rc!=SQLITE_OK );
  return rc;
}

/*
** Dereference a page.  When the reference count reaches zero,
** move the page to the LRU list if it is clean.
*/
void sqlite3PcacheRelease(PgHdr *p){
  assert( p->nRef>0 );
  p->nRef--;
  if( p->nRef==0 ){
    PCache *pCache = p->pCache;
    if( p->pCache->xDestroy ){
      p->pCache->xDestroy(p);
    }
    pCache->nRef--;
    if( (p->flags&PGHDR_DIRTY)==0 ){
      pCache->nPinned--;
      pcacheEnterMutex();
      if( pcache.nCurrentPage>pcache.nMaxPage ){
        pcacheRemoveFromList(&pCache->pClean, p);
        pcacheRemoveFromHash(p);
        pcachePageFree(p);
      }else{
        pcacheAddToLruList(p);
      }
      pcacheExitMutex();
    }else{
      /* Move the page to the head of the caches dirty list. */
      pcacheRemoveFromList(&pCache->pDirty, p);
      pcacheAddToList(&pCache->pDirty, p);
    }
  }
}

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
  assert( 0==(p->flags&PGHDR_DIRTY) );
  pCache = p->pCache;
  pCache->nRef--;
  pCache->nPinned--;
  pcacheEnterMutex();
  pcacheRemoveFromList(&pCache->pClean, p);
  pcacheRemoveFromHash(p);
  pcachePageFree(p);
  pcacheExitMutex();
}

/*
** Make sure the page is marked as dirty.  If it isn't dirty already,
** make it so.
*/
void sqlite3PcacheMakeDirty(PgHdr *p){
  PCache *pCache;
  p->flags &= ~PGHDR_DONT_WRITE;
  if( p->flags & PGHDR_DIRTY ) return;
  assert( (p->flags & PGHDR_DIRTY)==0 );
  assert( p->nRef>0 );
  pCache = p->pCache;
  pcacheEnterMutex();
  pcacheRemoveFromList(&pCache->pClean, p);
  pcacheAddToList(&pCache->pDirty, p);
  pcacheExitMutex();
  p->flags |= PGHDR_DIRTY;
}

void pcacheMakeClean(PgHdr *p){
  PCache *pCache = p->pCache;
  assert( p->apSave[0]==0 && p->apSave[1]==0 );
  assert( p->flags & PGHDR_DIRTY );
  pcacheRemoveFromList(&pCache->pDirty, p);
  pcacheAddToList(&pCache->pClean, p);
  p->flags &= ~PGHDR_DIRTY;
  if( p->nRef==0 ){
    pcacheAddToLruList(p);
    pCache->nPinned--;
  }
  expensive_assert( pCache->nPinned==pcachePinnedCount(pCache) );
}

/*
** Make sure the page is marked as clean.  If it isn't clean already,
** make it so.
*/
void sqlite3PcacheMakeClean(PgHdr *p){
  if( (p->flags & PGHDR_DIRTY) ){
    pcacheEnterMutex();
    pcacheMakeClean(p);
    pcacheExitMutex();
  }
}

/*
** Make every page in the cache clean.
*/
void sqlite3PcacheCleanAll(PCache *pCache){
  PgHdr *p;
  pcacheEnterMutex();
  while( (p = pCache->pDirty)!=0 ){
    assert( p->apSave[0]==0 && p->apSave[1]==0 );
    pcacheRemoveFromList(&pCache->pDirty, p);
    p->flags &= ~PGHDR_DIRTY;
    pcacheAddToList(&pCache->pClean, p);
    if( p->nRef==0 ){
      pcacheAddToLruList(p);
      pCache->nPinned--;
    }
  }
  sqlite3PcacheAssertFlags(pCache, 0, PGHDR_DIRTY);
  expensive_assert( pCache->nPinned==pcachePinnedCount(pCache) );
  pcacheExitMutex();
}

/*
** Change the page number of page p to newPgno. If newPgno is 0, then the
** page object is added to the clean-list and the PGHDR_REUSE_UNLIKELY 
** flag set.
*/
void sqlite3PcacheMove(PgHdr *p, Pgno newPgno){
  assert( p->nRef>0 );
  pcacheEnterMutex();
  pcacheRemoveFromHash(p);
  p->pgno = newPgno;
  if( newPgno==0 ){
    p->flags |= PGHDR_REUSE_UNLIKELY;
    pcacheFree(p->apSave[0]);
    pcacheFree(p->apSave[1]);
    p->apSave[0] = 0;
    p->apSave[1] = 0;
    if( (p->flags & PGHDR_DIRTY) ){
      pcacheMakeClean(p);
    }
  }
  pcacheAddToHash(p);
  pcacheExitMutex();
}

/*
** Remove all content from a page cache
*/
void pcacheClear(PCache *pCache){
  PgHdr *p, *pNext;
  assert( sqlite3_mutex_held(pcache.mutex) );
  for(p=pCache->pClean; p; p=pNext){
    pNext = p->pNext;
    pcacheRemoveFromLruList(p);
    pcachePageFree(p);
  }
  for(p=pCache->pDirty; p; p=pNext){
    pNext = p->pNext;
    pcachePageFree(p);
  }
  pCache->pClean = 0;
  pCache->pDirty = 0;
  pCache->pDirtyTail = 0;
  pCache->nPage = 0;
  pCache->nPinned = 0;
  memset(pCache->apHash, 0, pCache->nHash*sizeof(pCache->apHash[0]));
}


/*
** Drop every cache entry whose page number is greater than "pgno".
*/
void sqlite3PcacheTruncate(PCache *pCache, Pgno pgno){
  PgHdr *p, *pNext;
  PgHdr *pDirty = pCache->pDirty;
  pcacheEnterMutex();
  for(p=pCache->pClean; p||pDirty; p=pNext){
    if( !p ){
      p = pDirty;
      pDirty = 0;
    }
    pNext = p->pNext;
    if( p->pgno>pgno ){
      if( p->nRef==0 ){
        pcacheRemoveFromHash(p);
        if( p->flags&PGHDR_DIRTY ){
          pcacheRemoveFromList(&pCache->pDirty, p);
          pCache->nPinned--;
        }else{
          pcacheRemoveFromList(&pCache->pClean, p);
          pcacheRemoveFromLruList(p);
        }
        pcachePageFree(p);
      }else{
        /* If there are references to the page, it cannot be freed. In this
        ** case, zero the page content instead.
        */
        memset(p->pData, 0, pCache->szPage);
      }
    }
  }
  pcacheExitMutex();
}

/*
** If there are currently more than pcache.nMaxPage pages allocated, try
** to recycle pages to reduce the number allocated to pcache.nMaxPage.
*/
static void pcacheEnforceMaxPage(){
  PgHdr *p;
  assert( sqlite3_mutex_held(pcache.mutex) );
  while( pcache.nCurrentPage>pcache.nMaxPage && (p = pcacheRecyclePage()) ){
    pcachePageFree(p);
  }
}

/*
** Close a cache.
*/
void sqlite3PcacheClose(PCache *pCache){
  pcacheEnterMutex();

  /* Free all the pages used by this pager and remove them from the LRU list. */
  pcacheClear(pCache);
  if( pCache->bPurgeable ){
    pcache.nMaxPage -= pCache->nMax;
    pcache.nMinPage -= pCache->nMin;
    pcacheEnforceMaxPage();
  }
  sqlite3_free(pCache->apHash);
  pcacheExitMutex();
}

/*
** Preserve the content of the page.  It is assumed that the content
** has not been preserved already.
**
** If idJournal==0 then this is for the overall transaction.
** If idJournal==1 then this is for the statement journal.
**
** This routine is used for in-memory databases only.
**
** Return SQLITE_OK or SQLITE_NOMEM if a memory allocation fails.
*/
int sqlite3PcachePreserve(PgHdr *p, int idJournal){
  void *x;
  int sz;
  assert( p->pCache->bPurgeable==0 );
  assert( p->apSave[idJournal]==0 );
  sz = p->pCache->szPage;
  p->apSave[idJournal] = x = sqlite3PageMalloc( sz );
  if( x==0 ) return SQLITE_NOMEM;
  memcpy(x, p->pData, sz);
  return SQLITE_OK;
}

/*
** Commit a change previously preserved.
*/
void sqlite3PcacheCommit(PCache *pCache, int idJournal){
  PgHdr *p;
  pcacheEnterMutex();     /* Mutex is required to call pcacheFree() */
  for(p=pCache->pDirty; p; p=p->pNext){
    if( p->apSave[idJournal] ){
      pcacheFree(p->apSave[idJournal]);
      p->apSave[idJournal] = 0;
    }
  }
  pcacheExitMutex();
}

/*
** Rollback a change previously preserved.
*/
void sqlite3PcacheRollback(PCache *pCache, int idJournal){
  PgHdr *p;
  int sz;
  pcacheEnterMutex();     /* Mutex is required to call pcacheFree() */
  sz = pCache->szPage;
  for(p=pCache->pDirty; p; p=p->pNext){
    if( p->apSave[idJournal] ){
      memcpy(p->pData, p->apSave[idJournal], sz);
      pcacheFree(p->apSave[idJournal]);
      p->apSave[idJournal] = 0;
    }
  }
  pcacheExitMutex();
}

/* 
** Assert flags settings on all pages.  Debugging only.
*/
void sqlite3PcacheAssertFlags(PCache *pCache, int trueMask, int falseMask){
  PgHdr *p;
  for(p=pCache->pDirty; p; p=p->pNext){
    assert( (p->flags&trueMask)==trueMask );
    assert( (p->flags&falseMask)==0 );
  }
  for(p=pCache->pClean; p; p=p->pNext){
    assert( (p->flags&trueMask)==trueMask );
    assert( (p->flags&falseMask)==0 );
  }
}

/* 
** Discard the contents of the cache.
*/
int sqlite3PcacheClear(PCache *pCache){
  assert(pCache->nRef==0);
  pcacheEnterMutex();
  pcacheClear(pCache);
  pcacheExitMutex();
  return SQLITE_OK;
}

/*
** Merge two lists of pages connected by pDirty and in pgno order.
** Do not both fixing the pPrevDirty pointers.
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
** connected by pDirty pointers.  The pPrevDirty pointers are
** corrupted by this sort.
*/
#define N_SORT_BUCKET_ALLOC 25
#define N_SORT_BUCKET       25
#ifdef SQLITE_TEST
  int sqlite3_pager_n_sort_bucket = 0;
  #undef N_SORT_BUCKET
  #define N_SORT_BUCKET \
   (sqlite3_pager_n_sort_bucket?sqlite3_pager_n_sort_bucket:N_SORT_BUCKET_ALLOC)
#endif
static PgHdr *pcacheSortDirtyList(PgHdr *pIn){
  PgHdr *a[N_SORT_BUCKET_ALLOC], *p;
  int i;
  memset(a, 0, sizeof(a));
  while( pIn ){
    p = pIn;
    pIn = p->pDirty;
    p->pDirty = 0;
    for(i=0; i<N_SORT_BUCKET-1; i++){
      if( a[i]==0 ){
        a[i] = p;
        break;
      }else{
        p = pcacheMergeDirtyList(a[i], p);
        a[i] = 0;
      }
    }
    if( i==N_SORT_BUCKET-1 ){
      /* Coverage: To get here, there need to be 2^(N_SORT_BUCKET) 
      ** elements in the input list. This is possible, but impractical.
      ** Testing this line is the point of global variable
      ** sqlite3_pager_n_sort_bucket.
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
  for(p=pCache->pDirty; p; p=p->pNext){
    p->pDirty = p->pNext;
  }
  return pcacheSortDirtyList(pCache->pDirty);
}

/* 
** Return the total number of outstanding page references.
*/
int sqlite3PcacheRefCount(PCache *pCache){
  return pCache->nRef;
}

/* 
** Return the total number of pages in the cache.
*/
int sqlite3PcachePagecount(PCache *pCache){
  assert( pCache->nPage>=0 );
  return pCache->nPage;
}

#ifdef SQLITE_CHECK_PAGES
/*
** This function is used by the pager.c module to iterate through all 
** pages in the cache. At present, this is only required if the
** SQLITE_CHECK_PAGES macro (used for debugging) is specified.
*/
void sqlite3PcacheIterate(PCache *pCache, void (*xIter)(PgHdr *)){
  PgHdr *p;
  for(p=pCache->pClean; p; p=p->pNext){
    xIter(p);
  }
  for(p=pCache->pDirty; p; p=p->pNext){
    xIter(p);
  }
}
#endif

/* 
** Set flags on all pages in the page cache 
*/
void sqlite3PcacheSetFlags(PCache *pCache, int andMask, int orMask){
  PgHdr *p;

  assert( (orMask&PGHDR_NEED_SYNC)==0 );

  /* Obtain the global mutex before modifying any PgHdr.flags variables 
  ** or traversing the LRU list.
  */ 
  pcacheEnterMutex();

  for(p=pCache->pDirty; p; p=p->pNext){
    p->flags = (p->flags&andMask)|orMask;
  }
  for(p=pCache->pClean; p; p=p->pNext){
    p->flags = (p->flags&andMask)|orMask;
  }

  if( 0==(andMask&PGHDR_NEED_SYNC) ){
    pCache->pSynced = pCache->pDirtyTail;
    assert( !pCache->pSynced || (pCache->pSynced->flags&PGHDR_NEED_SYNC)==0 );
  }

  pcacheExitMutex();
}

/*
** Set the suggested cache-size value.
*/
int sqlite3PcacheGetCachesize(PCache *pCache){
  return pCache->nMax;
}

/*
** Set the suggested cache-size value.
*/
void sqlite3PcacheSetCachesize(PCache *pCache, int mxPage){
  if( mxPage<10 ){
    mxPage = 10;
  }
  if( pCache->bPurgeable ){
    pcacheEnterMutex();
    pcache.nMaxPage -= pCache->nMax;
    pcache.nMaxPage += mxPage;
    pcacheEnforceMaxPage();
    pcacheExitMutex();
  }
  pCache->nMax = mxPage;
}

#ifdef SQLITE_ENABLE_MEMORY_MANAGEMENT
/*
** This function is called to free superfluous dynamically allocated memory
** held by the pager system. Memory in use by any SQLite pager allocated
** by the current thread may be sqlite3_free()ed.
**
** nReq is the number of bytes of memory required. Once this much has
** been released, the function returns. The return value is the total number 
** of bytes of memory released.
*/
int sqlite3PcacheReleaseMemory(int nReq){
  int nFree = 0;
  if( pcache.pStart==0 ){
    PgHdr *p;
    pcacheEnterMutex();
    while( (nReq<0 || nFree<nReq) && (p=pcacheRecyclePage()) ){
      nFree += pcachePageSize(p);
      pcachePageFree(p);
    }
    pcacheExitMutex();
  }
  return nFree;
}
#endif /* SQLITE_ENABLE_MEMORY_MANAGEMENT */

#ifdef SQLITE_TEST
void sqlite3PcacheStats(
  int *pnCurrent,
  int *pnMax,
  int *pnMin,
  int *pnRecyclable
){
  PgHdr *p;
  int nRecyclable = 0;
  for(p=pcache.pLruHead; p; p=p->pNextLru){
    nRecyclable++;
  }

  *pnCurrent = pcache.nCurrentPage;
  *pnMax = pcache.nMaxPage;
  *pnMin = pcache.nMinPage;
  *pnRecyclable = nRecyclable;
}
#endif


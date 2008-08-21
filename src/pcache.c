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
** @(#) $Id: pcache.c,v 1.6 2008/08/21 15:54:01 danielk1977 Exp $
*/
#include "sqliteInt.h"

/*
** A complete page cache is an instance of this structure.
*/
struct PCache {
  PCache *pNextAll, *pPrevAll;        /* List of all page caches */
  int szPage;                         /* Size of every page in this cache */
  int szExtra;                        /* Size of extra space for each page */
  int nHash;                          /* Number of slots in apHash[] */
  int nPage;                          /* Total number of pages in apHash */
  int nMax;                           /* Configured cache size */
  PgHdr **apHash;                     /* Hash table for fast lookup by pgno */
  int bPurgeable;                     /* True if pages are on backing store */
  void (*xDestroy)(PgHdr*);           /* Called when refcnt goes 1->0 */
  int (*xStress)(void*);              /* Call to try to make pages clean */
  void *pStress;                      /* Argument to xStress */
  PgHdr *pClean;                      /* List of clean pages in use */
  PgHdr *pDirty;                      /* List of dirty pages */
  int nRef;                           /* Number of outstanding page refs */

  int iInUseMM;
  int iInUseDB;
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
**
** The maximum number of cached pages stored by the system is determined
** by the pcache.mxPage and pcache.mxPagePurgeable variables. If
** mxPage is non-zero, then the system tries to limit the number of
** cached pages stored to mxPage. In this case mxPagePurgeable is not 
** used.
**
** If mxPage is zero, then the system tries to limit the number of
** pages held by purgable caches to mxPagePurgeable.
*/
static struct PCacheGlobal {
  int isInit;                         /* True when initialized */
  sqlite3_mutex *mutex_mem2;          /* static mutex MUTEX_STATIC_MEM2 */
  sqlite3_mutex *mutex_lru;           /* static mutex MUTEX_STATIC_LRU */
  PCache *pAll;                       /* list of all page caches */
  int nPage;                          /* Number of pages */
  int nPurgeable;                     /* Number of pages in purgable caches */
  int mxPage;                         /* Globally configured page maximum */
  int mxPagePurgeable;                /* Purgeable page maximum */
  PgHdr *pLruHead, *pLruTail;         /* Global LRU list of unused pages */
  int szSlot;                         /* Size of each free slot */
  void *pStart, *pEnd;                /* Bounds of pagecache malloc range */
  PgFreeslot *pFree;                  /* Free page blocks */
} pcache = {0};

/*
** All global variables used by this module (most of which are grouped 
** together in global structure "pcache" above) except the list of all
** pager-caches starting with pcache.pAll, are protected by the static 
** SQLITE_MUTEX_STATIC_LRU mutex. A pointer to this mutex is stored in
** variable "pcache.mutex_lru".
**
** The list of all pager-caches (PCache structures) headed by pcache.pAll 
** is protected by SQLITE_MUTEX_STATIC_MEM2.
**
** Access to the contents of the individual PCache structures is not 
** protected. It is the job of the caller to ensure that these structures
** are accessed in a thread-safe manner. However, this module provides the
** functions sqlite3PcacheLock() and sqlite3PcacheUnlock() that may be used
** by the caller to increment/decrement a lock-count on an individual 
** pager-cache object. This module guarantees that the xStress() callback
** will not be invoked on a pager-cache with a non-zero lock-count except
** from within a call to sqlite3PcacheFetch() on the same pager. A call
** to sqlite3PcacheLock() may block if such an xStress() call is currently 
** underway.
**
** Before the xStress callback of a pager-cache (PCache) is invoked, the
** SQLITE_MUTEX_STATIC_MEM2 mutex is obtained and the SQLITE_MUTEX_STATIC_LRU 
** mutex released (in that order) before making the call.
**
** Deadlock within the module is avoided by never blocking on the MEM2 
** mutex while the LRU mutex is held.
*/

#define pcacheEnterGlobal() sqlite3_mutex_enter(pcache.mutex_lru)
#define pcacheExitGlobal()  sqlite3_mutex_leave(pcache.mutex_lru)

/*
** Increment the reference count on both page p and its cache by n.
*/
static void pcacheRef(PgHdr *p, int n){
  /* This next block assert()s that the number of references to the 
  ** PCache is the sum of the number of references to all pages in
  ** the PCache. This is a bit expensive to leave turned on all the 
  ** time, even in debugging builds.
  */
#if 0
  PgHdr *pHdr;
  int nRef = 0;
  for(pHdr=p->pCache->pClean; pHdr; pHdr=pHdr->pNext) nRef += pHdr->nRef;
  for(pHdr=p->pCache->pDirty; pHdr; pHdr=pHdr->pNext) nRef += pHdr->nRef;
  assert( p->pCache->nRef==nRef );
#endif
  p->nRef += n;
  p->pCache->nRef += n;
}

/********************************** Linked List Management ********************/

#ifndef NDEBUG
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
#endif

/*
** Remove a page from its hash table (PCache.apHash[]).
*/
static void pcacheRemoveFromHash(PgHdr *pPage){
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
  assert( pcacheCheckHashCount(pPage->pCache) );
}

/*
** Insert a page into the hash table
*/
static void pcacheAddToHash(PgHdr *pPage){
  PCache *pCache = pPage->pCache;
  u32 h = pPage->pgno % pCache->nHash;
  pPage->pNextHash = pCache->apHash[h];
  pPage->pPrevHash = 0;
  if( pCache->apHash[h] ){
    pCache->apHash[h]->pPrevHash = pPage;
  }
  pCache->apHash[h] = pPage;
  pCache->nPage++;
  assert( pcacheCheckHashCount(pCache) );
}

/*
** Attempt to increase the size the hash table to contain
** at least nHash buckets.
*/
static int pcacheResizeHash(PCache *pCache, int nHash){
#ifdef SQLITE_MALLOC_SOFT_LIMIT
  if( nHash*sizeof(PgHdr*)>SQLITE_MALLOC_SOFT_LIMIT ){
    nHash = SQLITE_MALLOC_SOFT_LIMIT/sizeof(PgHdr *);
  }
#endif
  if( nHash>pCache->nHash ){
    PgHdr *p;
    PgHdr **pNew = (PgHdr **)sqlite3_malloc(sizeof(PgHdr*)*nHash);
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
  }
  return SQLITE_OK;
}

/*
** Remove a page from a linked list that is headed by *ppHead.
** *ppHead is either PCache.pClean or PCache.pDirty.
*/
static void pcacheRemoveFromList(PgHdr **ppHead, PgHdr *pPage){
  if( pPage->pPrev ){
    pPage->pPrev->pNext = pPage->pNext;
  }else{
    assert( *ppHead==pPage );
    *ppHead = pPage->pNext;
  }
  if( pPage->pNext ){
    pPage->pNext->pPrev = pPage->pPrev;
  }
}

/*
** Add a page from a linked list that is headed by *ppHead.
** *ppHead is either PCache.pClean or PCache.pDirty.
*/
static void pcacheAddToList(PgHdr **ppHead, PgHdr *pPage){
  if( (*ppHead) ){
    (*ppHead)->pPrev = pPage;
  }
  pPage->pNext = *ppHead;
  pPage->pPrev = 0;
  *ppHead = pPage;
}

/*
** Remove a page from the global LRU list
*/
static void pcacheRemoveFromLruList(PgHdr *pPage){
  assert( sqlite3_mutex_held(pcache.mutex_lru) );
  if( pPage->pCache->bPurgeable==0 ) return;
  if( pPage->pNextLru ){
    pPage->pNextLru->pPrevLru = pPage->pPrevLru;
  }else{
    assert( pcache.pLruTail==pPage );
    pcache.pLruTail = pPage->pPrevLru;
  }
  if( pPage->pPrevLru ){
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
  assert( sqlite3_mutex_held(pcache.mutex_lru) );
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
  assert( sqlite3_mutex_held(pcache.mutex_lru) );
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
    ** reclaim memory from this pager-cache. Because sqlite3PcacheLock() 
    ** might block on the MEM2 mutex, it has to be called before re-entering
    ** the global LRU mutex.
    */
    pcacheExitGlobal();
    sqlite3PcacheUnlock(pCache);
    p = sqlite3Malloc(sz);
    sqlite3PcacheLock(pCache);
    pcacheEnterGlobal();

    if( p ){
      sz = sqlite3MallocSize(p);
      sqlite3StatusAdd(SQLITE_STATUS_PAGECACHE_OVERFLOW, sz);
    }
    return p;
  }
}
void *sqlite3PageMalloc(sz){
  void *p;
  pcacheEnterGlobal();
  p = pcacheMalloc(sz, 0);
  pcacheExitGlobal();
  return p;
}

/*
** Release a pager memory allocation
*/
void pcacheFree(void *p){
  assert( sqlite3_mutex_held(pcache.mutex_lru) );
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
  pcacheEnterGlobal();
  pcacheFree(p);
  pcacheExitGlobal();
}

/*
** Allocate a new page.
*/
static PgHdr *pcachePageAlloc(PCache *pCache){
  PgHdr *p;
  int sz = sizeof(*p) + pCache->szPage + pCache->szExtra;
  assert( sqlite3_mutex_held(pcache.mutex_lru) );
  p = pcacheMalloc(sz, pCache);
  if( p==0 ) return 0;
  memset(p, 0, sizeof(PgHdr));
  p->pData = (void*)&p[1];
  p->pExtra = (void*)&((char*)p->pData)[pCache->szPage];
  pcache.nPage++;
  if( pCache->bPurgeable ){
    pcache.nPurgeable++;
  }

  return p;
}

/*
** Deallocate a page
*/
static void pcachePageFree(PgHdr *p){
  assert( sqlite3_mutex_held(pcache.mutex_lru) );
  pcache.nPage--;
  if( p->pCache->bPurgeable ){
    pcache.nPurgeable--;
  }
  pcacheFree(p->apSave[0]);
  pcacheFree(p->apSave[1]);
  pcacheFree(p);
}

/*
** Return the number of bytes that will be returned to the heap when
** the argument is passed to pcachePageFree().
*/
static int pcachePageSize(PgHdr *p){
  assert( sqlite3_mutex_held(pcache.mutex_lru) );
  assert( !pcache.pStart );
  assert( p->apSave[0]==0 );
  assert( p->apSave[1]==0 );
  assert( p && p->pCache );
  return sqlite3MallocSize(p);
}

static PgHdr *pcacheRecycle(PCache *pCache){
  PCache *pCsr;
  PgHdr *p = 0;

  assert( pcache.isInit );
  assert( sqlite3_mutex_held(pcache.mutex_lru) );

  if( !pcache.pLruTail && SQLITE_OK==sqlite3_mutex_try(pcache.mutex_mem2) ){

    /* Invoke xStress() callbacks until the LRU list contains at least one
    ** page that can be reused or until the xStress() callback of all
    ** caches has been invoked.
    */
    for(pCsr=pcache.pAll; pCsr&&!pcache.pLruTail; pCsr=pCsr->pNextAll){
      assert( pCsr->iInUseMM==0 );
      pCsr->iInUseMM = 1;
      if( pCsr->xStress && (pCsr->iInUseDB==0 || pCache==pCsr) ){
        pcacheExitGlobal();
        pCsr->xStress(pCsr->pStress);
        pcacheEnterGlobal();
      }
      pCsr->iInUseMM = 0;
    }

    sqlite3_mutex_leave(pcache.mutex_mem2);
  }

  p = pcache.pLruTail;

  if( p ){
    pcacheRemoveFromLruList(p);
    pcacheRemoveFromHash(p);
    pcacheRemoveFromList(&p->pCache->pClean, p);

    /* If the always-rollback flag is set on the page being recycled, set 
    ** the always-rollback flag on the corresponding pager.
    */
    if( p->flags&PGHDR_ALWAYS_ROLLBACK ){
      assert(p->pPager);
      sqlite3PagerAlwaysRollback(p->pPager);
    }
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
static PgHdr *pcacheRecycleOrAlloc(PCache *pCache){
  PgHdr *p = 0;

  int szPage = pCache->szPage;
  int szExtra = pCache->szExtra;
  int bPurg = pCache->bPurgeable;

  assert( pcache.isInit );
  assert( sqlite3_mutex_notheld(pcache.mutex_lru) );

  pcacheEnterGlobal();

  if( (pcache.mxPage && pcache.nPage>=pcache.mxPage) 
   || (!pcache.mxPage && bPurg && pcache.nPurgeable>=pcache.mxPagePurgeable)
  ){
    /* If the above test succeeds, then try to obtain a buffer by recycling
    ** an existing page. */
    p = pcacheRecycle(pCache);
  }

  if( p && (p->pCache->szPage!=szPage || p->pCache->szExtra!=szExtra) ){
    pcachePageFree(p);
    p = 0;
  }

  if( !p ){
    p = pcachePageAlloc(pCache);
  }

  pcacheExitGlobal();
  return p;
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
    pcache.mutex_lru = sqlite3_mutex_alloc(SQLITE_MUTEX_STATIC_LRU);
    pcache.mutex_mem2 = sqlite3_mutex_alloc(SQLITE_MUTEX_STATIC_MEM2);
    if( pcache.mutex_lru==0 || pcache.mutex_mem2==0 ){
      return SQLITE_NOMEM;
    }
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
  int (*xStress)(void*),       /* Call to try to make pages clean */
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

  if( bPurgeable ){
    pcacheEnterGlobal();
    pcache.mxPagePurgeable += p->nMax;
    pcacheExitGlobal();
  }

  /* Add the new pager-cache to the list of caches starting at pcache.pAll */
  assert( sqlite3_mutex_notheld(pcache.mutex_lru) );
  sqlite3_mutex_enter(pcache.mutex_mem2);
  p->pNextAll = pcache.pAll;
  if( pcache.pAll ){
    pcache.pAll->pPrevAll = p;
  }
  p->pPrevAll = 0;
  pcache.pAll = p;
  sqlite3_mutex_leave(pcache.mutex_mem2);
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
  PgHdr *pPage;
  assert( pcache.isInit );
  assert( pCache!=0 );
  assert( pgno>0 );
  assert( pCache->iInUseDB || pCache->iInUseMM );

  /* Search the hash table for the requested page. Exit early if it is found. */
  if( pCache->apHash ){
    u32 h = pgno % pCache->nHash;
    for(pPage=pCache->apHash[h]; pPage; pPage=pPage->pNextHash){
      if( pPage->pgno==pgno ){
        if( pPage->nRef==0 && (pPage->flags & PGHDR_DIRTY)==0 ){
          pcacheEnterGlobal();
          pcacheRemoveFromLruList(pPage);
          pcacheExitGlobal();
        }
        pcacheRef(pPage, 1);
        *ppPage = pPage;
        return SQLITE_OK;
      }
    }
  }

  if( createFlag ){
    if( pCache->nHash<=pCache->nPage ){
      int rc = pcacheResizeHash(pCache, pCache->nHash<256?256:pCache->nHash*2);
      if( rc!=SQLITE_OK ){
        return rc;
      }
    }

    pPage = pcacheRecycleOrAlloc(pCache);
    *ppPage = pPage;
    if( pPage==0 ){
      return SQLITE_NOMEM;
    }

    pPage->pPager = 0;
    pPage->flags = 0;
    pPage->pDirty = 0;
    pPage->nRef = 0;
    pPage->pgno = pgno;
    pPage->pCache = pCache;
    pcacheRef(pPage, 1);
    pcacheAddToList(&pCache->pClean, pPage);
    pcacheAddToHash(pPage);
  }else{
    *ppPage = 0;
  }

  return SQLITE_OK;
}

/*
** Dereference a page.  When the reference count reaches zero,
** move the page to the LRU list if it is clean.
*/
void sqlite3PcacheRelease(PgHdr *p){
  assert( p->nRef>0 );
  assert( p->pCache->iInUseDB || p->pCache->iInUseMM );
  pcacheRef(p, -1);
  if( p->nRef!=0 ) return;
  if( p->pCache->xDestroy ){
    p->pCache->xDestroy(p);
  }
  if( (p->flags & PGHDR_DIRTY)!=0 ) return;
  pcacheEnterGlobal();
  pcacheAddToLruList(p);
  pcacheExitGlobal();
}

void sqlite3PcacheRef(PgHdr *p){
  assert(p->nRef>=0);
  pcacheRef(p, 1);
}

/*
** Drop a page from the cache.  This should be the only reference to
** the page.
*/
void sqlite3PcacheDrop(PgHdr *p){
  PCache *pCache;
  assert( p->pCache->iInUseDB );
  assert( p->nRef==1 );
  pCache = p->pCache;
  pCache->nRef--;
  if( p->flags & PGHDR_DIRTY ){
    pcacheRemoveFromList(&pCache->pDirty, p);
  }else{
    pcacheRemoveFromList(&pCache->pClean, p);
  }
  pcacheRemoveFromHash(p);
  pcacheEnterGlobal();
  pcachePageFree(p);
  pcacheExitGlobal();
}

/*
** Make sure the page is marked as dirty.  If it isn't dirty already,
** make it so.
*/
void sqlite3PcacheMakeDirty(PgHdr *p){
  PCache *pCache;
  assert( p->pCache->iInUseDB );
  if( p->flags & PGHDR_DIRTY ) return;
  assert( (p->flags & PGHDR_DIRTY)==0 );
  assert( p->nRef>0 );
  pCache = p->pCache;
  pcacheRemoveFromList(&pCache->pClean, p);
  pcacheAddToList(&pCache->pDirty, p);
  p->flags |= PGHDR_DIRTY;
}

/*
** Make sure the page is marked as clean.  If it isn't clean already,
** make it so.
*/
void sqlite3PcacheMakeClean(PgHdr *p){
  PCache *pCache;
  assert( p->pCache->iInUseDB || p->pCache->iInUseMM );
  if( (p->flags & PGHDR_DIRTY)==0 ) return;
  assert( p->apSave[0]==0 && p->apSave[1]==0 );
  assert( p->flags & PGHDR_DIRTY );
  /* assert( p->nRef>0 ); */
  pCache = p->pCache;
  pcacheRemoveFromList(&pCache->pDirty, p);
  pcacheAddToList(&pCache->pClean, p);
  p->flags &= ~PGHDR_DIRTY;
  if( p->nRef==0 ){
    pcacheEnterGlobal();
    pcacheAddToLruList(p);
    pcacheExitGlobal();
  }
}

/*
** Make every page in the cache clean.
*/
void sqlite3PcacheCleanAll(PCache *pCache){
  PgHdr *p;
  assert( pCache->iInUseDB );
  while( (p = pCache->pDirty)!=0 ){
    assert( p->apSave[0]==0 && p->apSave[1]==0 );
    pcacheRemoveFromList(&pCache->pDirty, p);
    pcacheAddToList(&pCache->pClean, p);
    p->flags &= ~PGHDR_DIRTY;
    if( p->nRef==0 ){
      pcacheEnterGlobal();
      pcacheAddToLruList(p);
      pcacheExitGlobal();
    }
  }
}

/*
** Change the page number of page p to newPgno. If newPgno is 0, then the
** page object is added to the clean-list and the PGHDR_REUSE_UNLIKELY 
** flag set.
*/
void sqlite3PcacheMove(PgHdr *p, Pgno newPgno){
  assert( p->pCache->iInUseDB );
  pcacheRemoveFromHash(p);
  p->pgno = newPgno;
  if( newPgno==0 ){
    p->flags |= PGHDR_REUSE_UNLIKELY;
    pcacheEnterGlobal();
    pcacheFree(p->apSave[0]);
    pcacheFree(p->apSave[1]);
    pcacheExitGlobal();
    p->apSave[0] = 0;
    p->apSave[1] = 0;
    sqlite3PcacheMakeClean(p);
  }
  pcacheAddToHash(p);
}

/*
** Set the global maximum number of pages. Return the previous value.
*/
void sqlite3PcacheGlobalMax(int mx){
  pcacheEnterGlobal();
  pcache.mxPage = mx;
  pcacheExitGlobal();
}

/*
** Remove all content from a page cache
*/
void pcacheClear(PCache *pCache){
  PgHdr *p, *pNext;
  assert( sqlite3_mutex_held(pcache.mutex_lru) );
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
  pCache->nPage = 0;
  memset(pCache->apHash, 0, pCache->nHash*sizeof(pCache->apHash[0]));
}


/*
** Drop every cache entry whose page number is greater than "pgno".
*/
void sqlite3PcacheTruncate(PCache *pCache, Pgno pgno){
  PgHdr *p, *pNext;
  PgHdr *pDirty = pCache->pDirty;
  assert( pCache->iInUseDB );
  pcacheEnterGlobal();
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
        }else{
          pcacheRemoveFromLruList(p);
          pcacheRemoveFromList(&pCache->pClean, p);
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
  pcacheExitGlobal();
}


/*
** Close a cache.
*/
void sqlite3PcacheClose(PCache *pCache){
  assert( pCache->iInUseDB==1 );

  /* Free all the pages used by this pager and remove them from the LRU
  ** list. This requires the protection of the MUTEX_STATIC_LRU mutex.
  */
  pcacheEnterGlobal();
  pcacheClear(pCache);
  if( pCache->bPurgeable ){
    pcache.mxPagePurgeable -= pCache->nMax;
  }
  sqlite3_free(pCache->apHash);
  pcacheExitGlobal();

  /* Now remove the pager-cache structure itself from the list of
  ** all such structures headed by pcache.pAll. This required the
  ** MUTEX_STATIC_MEM2 mutex.
  */
  assert( sqlite3_mutex_notheld(pcache.mutex_lru) );
  sqlite3_mutex_enter(pcache.mutex_mem2);
  assert(pCache==pcache.pAll || pCache->pPrevAll);
  assert(pCache->pNextAll==0 || pCache->pNextAll->pPrevAll==pCache);
  assert(pCache->pPrevAll==0 || pCache->pPrevAll->pNextAll==pCache);
  if( pCache->pPrevAll ){
    pCache->pPrevAll->pNextAll = pCache->pNextAll;
  }else{
    pcache.pAll = pCache->pNextAll;
  }
  if( pCache->pNextAll ){
    pCache->pNextAll->pPrevAll = pCache->pPrevAll;
  }
  sqlite3_mutex_leave(pcache.mutex_mem2);
}

/*
** Preserve the content of the page, if it has not been preserved
** already.  If idJournal==0 then this is for the overall transaction.
** If idJournal==1 then this is for the statement journal.
**
** This routine is used for in-memory databases only.
**
** Return SQLITE_OK or SQLITE_NOMEM if a memory allocation fails.
*/
int sqlite3PcachePreserve(PgHdr *p, int idJournal){
  void *x;
  int sz;
  assert( p->pCache->iInUseDB );
  assert( p->pCache->bPurgeable==0 );
  if( !p->apSave[idJournal] ){
    sz = p->pCache->szPage;
    p->apSave[idJournal] = x = sqlite3PageMalloc( sz );
    if( x==0 ) return SQLITE_NOMEM;
    memcpy(x, p->pData, sz);
  }
  return SQLITE_OK;
}

/*
** Commit a change previously preserved.
*/
void sqlite3PcacheCommit(PCache *pCache, int idJournal){
  PgHdr *p;
  assert( pCache->iInUseDB );
  pcacheEnterGlobal();     /* Mutex is required to call pcacheFree() */
  for(p=pCache->pDirty; p; p=p->pNext){
    if( p->apSave[idJournal] ){
      pcacheFree(p->apSave[idJournal]);
      p->apSave[idJournal] = 0;
    }
  }
  pcacheExitGlobal();
}

/*
** Rollback a change previously preserved.
*/
void sqlite3PcacheRollback(PCache *pCache, int idJournal){
  PgHdr *p;
  int sz;
  assert( pCache->iInUseDB );
  pcacheEnterGlobal();     /* Mutex is required to call pcacheFree() */
  sz = pCache->szPage;
  for(p=pCache->pDirty; p; p=p->pNext){
    if( p->apSave[idJournal] ){
      memcpy(p->pData, p->apSave[idJournal], sz);
      pcacheFree(p->apSave[idJournal]);
      p->apSave[idJournal] = 0;
    }
  }
  pcacheExitGlobal();
}

/* 
** Assert flags settings on all pages.  Debugging only.
*/
void sqlite3PcacheAssertFlags(PCache *pCache, int trueMask, int falseMask){
  PgHdr *p;
  assert( pCache->iInUseDB || pCache->iInUseMM );
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
  assert( pCache->iInUseDB );
  assert(pCache->nRef==0);
  pcacheEnterGlobal();
  pcacheClear(pCache);
  pcacheExitGlobal();
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
  assert( pCache->iInUseDB );
  for(p=pCache->pDirty; p; p=p->pNext){
    p->pDirty = p->pNext;
  }
  return pcacheSortDirtyList(pCache->pDirty);
}

/*
** This function searches cache pCache for a dirty page for which the
** reference count is zero. If such a page can be found, the PgHdr.pDirty
** pointer is set to 0 and a pointer to the page is returned. If no
** such page is found, 0 is returned.
**
** This is used by the pager module to implement the xStress callback.
*/
PgHdr *sqlite3PcacheDirtyPage(PCache *pCache){
  PgHdr *p = 0;
#if 1
  PgHdr *pIter;
  Pgno min_pgno;
  assert( pCache->iInUseMM );
  for(pIter=pCache->pDirty; pIter; pIter=pIter->pNext){
    if( pIter->nRef==0 && (p==0 || pIter->pgno<min_pgno) ){
      p = pIter;
      min_pgno = pIter->pgno;
    }
  }
#else
  assert( pCache->iInUseMM );
  for(p=pCache->pDirty; p && p->nRef; p=p->pNext);
#endif
  assert( pCache->iInUseMM );
  if( p ){
    p->pDirty = 0;
  }
  return p;
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
  assert( pCache->iInUseDB || pCache->iInUseMM );
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
  assert( pCache->iInUseDB || pCache->iInUseMM );
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
  assert( pCache->iInUseDB || pCache->iInUseMM );
  for(p=pCache->pDirty; p; p=p->pNext){
    p->flags = (p->flags&andMask)|orMask;
  }
  for(p=pCache->pClean; p; p=p->pNext){
    p->flags = (p->flags&andMask)|orMask;
  }
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
    pcacheEnterGlobal();
    pcache.mxPagePurgeable -= pCache->nMax;
    pcache.mxPagePurgeable += mxPage;
    pcacheExitGlobal();
  }
  pCache->nMax = mxPage;
}

/*
** Lock a pager-cache.
*/
void sqlite3PcacheLock(PCache *pCache){
  if( pCache ){
    assert( sqlite3_mutex_notheld(pcache.mutex_lru) );
    pCache->iInUseDB++;
    if( pCache->iInUseMM && pCache->iInUseDB==1 ){
      pCache->iInUseDB = 0;
      sqlite3_mutex_enter(pcache.mutex_mem2);
      assert( pCache->iInUseMM==0 && pCache->iInUseDB==0 );
      pCache->iInUseDB = 1;
      sqlite3_mutex_leave(pcache.mutex_mem2);
    }
  }
}

/*
** Unlock a pager-cache.
*/
void sqlite3PcacheUnlock(PCache *pCache){
  if( pCache ){
    pCache->iInUseDB--;
    assert( pCache->iInUseDB>=0 );
  }
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
    pcacheEnterGlobal();
    while( (nReq<0 || nFree<nReq) && (p=pcacheRecycle(0)) ){
      nFree += pcachePageSize(p);
      pcachePageFree(p);
    }
    pcacheExitGlobal();
  }
  return nFree;
}
#endif /* SQLITE_ENABLE_MEMORY_MANAGEMENT */


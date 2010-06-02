/*
** 2008 November 05
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
** This file implements the default page cache implementation (the
** sqlite3_pcache interface). It also contains part of the implementation
** of the SQLITE_CONFIG_PAGECACHE and sqlite3_release_memory() features.
** If the default page cache implementation is overriden, then neither of
** these two features are available.
*/

#include "sqliteInt.h"

typedef struct PCache1 PCache1;
typedef struct PgHdr1 PgHdr1;
typedef struct PgFreeslot PgFreeslot;

/* Pointers to structures of this type are cast and returned as 
** opaque sqlite3_pcache* handles
*/
struct PCache1 {
  /* Cache configuration parameters. Page size (szPage) and the purgeable
  ** flag (bPurgeable) are set when the cache is created. nMax may be 
  ** modified at any time by a call to the pcache1CacheSize() method.
  ** The global mutex must be held when accessing nMax.
  */
  int szPage;                         /* Size of allocated pages in bytes */
  int bPurgeable;                     /* True if cache is purgeable */
  unsigned int nMin;                  /* Minimum number of pages reserved */
  unsigned int nMax;                  /* Configured "cache_size" value */

  /* Hash table of all pages. The following variables may only be accessed
  ** when the accessor is holding the global mutex (see pcache1EnterMutex() 
  ** and pcache1LeaveMutex()).
  */
  unsigned int nRecyclable;           /* Number of pages in the LRU list */
  unsigned int nPage;                 /* Total number of pages in apHash */
  unsigned int nHash;                 /* Number of slots in apHash[] */
  PgHdr1 **apHash;                    /* Hash table for fast lookup by key */

  unsigned int iMaxKey;               /* Largest key seen since xTruncate() */
};

/*
** Each cache entry is represented by an instance of the following 
** structure. A buffer of PgHdr1.pCache->szPage bytes is allocated 
** directly before this structure in memory (see the PGHDR1_TO_PAGE() 
** macro below).
*/
struct PgHdr1 {
  unsigned int iKey;             /* Key value (page number) */
  PgHdr1 *pNext;                 /* Next in hash table chain */
  PCache1 *pCache;               /* Cache that currently owns this page */
  PgHdr1 *pLruNext;              /* Next in LRU list of unpinned pages */
  PgHdr1 *pLruPrev;              /* Previous in LRU list of unpinned pages */
};

/*
** Free slots in the allocator used to divide up the buffer provided using
** the SQLITE_CONFIG_PAGECACHE mechanism.
*/
struct PgFreeslot {
  PgFreeslot *pNext;  /* Next free slot */
};

/*
** Global data used by this cache.
*/
static SQLITE_WSD struct PCacheGlobal {
  sqlite3_mutex *mutex;               /* static mutex MUTEX_STATIC_LRU */

  int nMaxPage;                       /* Sum of nMaxPage for purgeable caches */
  int nMinPage;                       /* Sum of nMinPage for purgeable caches */
  int nCurrentPage;                   /* Number of purgeable pages allocated */
  PgHdr1 *pLruHead, *pLruTail;        /* LRU list of unpinned pages */

  /* Variables related to SQLITE_CONFIG_PAGECACHE settings. */
  int szSlot;                         /* Size of each free slot */
  void *pStart, *pEnd;                /* Bounds of pagecache malloc range */
  PgFreeslot *pFree;                  /* Free page blocks */
  int isInit;                         /* True if initialized */
} pcache1_g;

/*
** All code in this file should access the global structure above via the
** alias "pcache1". This ensures that the WSD emulation is used when
** compiling for systems that do not support real WSD.
*/
#define pcache1 (GLOBAL(struct PCacheGlobal, pcache1_g))

/*
** When a PgHdr1 structure is allocated, the associated PCache1.szPage
** bytes of data are located directly before it in memory (i.e. the total
** size of the allocation is sizeof(PgHdr1)+PCache1.szPage byte). The
** PGHDR1_TO_PAGE() macro takes a pointer to a PgHdr1 structure as
** an argument and returns a pointer to the associated block of szPage
** bytes. The PAGE_TO_PGHDR1() macro does the opposite: its argument is
** a pointer to a block of szPage bytes of data and the return value is
** a pointer to the associated PgHdr1 structure.
**
**   assert( PGHDR1_TO_PAGE(PAGE_TO_PGHDR1(pCache, X))==X );
*/
#define PGHDR1_TO_PAGE(p)    (void*)(((char*)p) - p->pCache->szPage)
#define PAGE_TO_PGHDR1(c, p) (PgHdr1*)(((char*)p) + c->szPage)

/*
** Macros to enter and leave the global LRU mutex.
*/
#define pcache1EnterMutex() sqlite3_mutex_enter(pcache1.mutex)
#define pcache1LeaveMutex() sqlite3_mutex_leave(pcache1.mutex)

/******************************************************************************/
/******** Page Allocation/SQLITE_CONFIG_PCACHE Related Functions **************/

/*
** This function is called during initialization if a static buffer is 
** supplied to use for the page-cache by passing the SQLITE_CONFIG_PAGECACHE
** verb to sqlite3_config(). Parameter pBuf points to an allocation large
** enough to contain 'n' buffers of 'sz' bytes each.
*/
void sqlite3PCacheBufferSetup(void *pBuf, int sz, int n){
  if( pcache1.isInit ){
    PgFreeslot *p;
    sz = ROUNDDOWN8(sz);
    pcache1.szSlot = sz;
    pcache1.pStart = pBuf;
    pcache1.pFree = 0;
    while( n-- ){
      p = (PgFreeslot*)pBuf;
      p->pNext = pcache1.pFree;
      pcache1.pFree = p;
      pBuf = (void*)&((char*)pBuf)[sz];
    }
    pcache1.pEnd = pBuf;
  }
}

/*
** Malloc function used within this file to allocate space from the buffer
** configured using sqlite3_config(SQLITE_CONFIG_PAGECACHE) option. If no 
** such buffer exists or there is no space left in it, this function falls 
** back to sqlite3Malloc().
*/
static void *pcache1Alloc(int nByte){
  void *p;
  assert( sqlite3_mutex_held(pcache1.mutex) );
  if( nByte<=pcache1.szSlot && pcache1.pFree ){
    assert( pcache1.isInit );
    p = (PgHdr1 *)pcache1.pFree;
    pcache1.pFree = pcache1.pFree->pNext;
    sqlite3StatusSet(SQLITE_STATUS_PAGECACHE_SIZE, nByte);
    sqlite3StatusAdd(SQLITE_STATUS_PAGECACHE_USED, 1);
  }else{

    /* Allocate a new buffer using sqlite3Malloc. Before doing so, exit the
    ** global pcache mutex and unlock the pager-cache object pCache. This is 
    ** so that if the attempt to allocate a new buffer causes the the 
    ** configured soft-heap-limit to be breached, it will be possible to
    ** reclaim memory from this pager-cache.
    */
    pcache1LeaveMutex();
    p = sqlite3Malloc(nByte);
    pcache1EnterMutex();
    if( p ){
      int sz = sqlite3MallocSize(p);
      sqlite3StatusAdd(SQLITE_STATUS_PAGECACHE_OVERFLOW, sz);
    }
    sqlite3MemdebugSetType(p, MEMTYPE_PCACHE);
  }
  return p;
}

/*
** Free an allocated buffer obtained from pcache1Alloc().
*/
static void pcache1Free(void *p){
  assert( sqlite3_mutex_held(pcache1.mutex) );
  if( p==0 ) return;
  if( p>=pcache1.pStart && p<pcache1.pEnd ){
    PgFreeslot *pSlot;
    sqlite3StatusAdd(SQLITE_STATUS_PAGECACHE_USED, -1);
    pSlot = (PgFreeslot*)p;
    pSlot->pNext = pcache1.pFree;
    pcache1.pFree = pSlot;
  }else{
    int iSize;
    assert( sqlite3MemdebugHasType(p, MEMTYPE_PCACHE) );
    sqlite3MemdebugSetType(p, MEMTYPE_HEAP);
    iSize = sqlite3MallocSize(p);
    sqlite3StatusAdd(SQLITE_STATUS_PAGECACHE_OVERFLOW, -iSize);
    sqlite3_free(p);
  }
}

/*
** Allocate a new page object initially associated with cache pCache.
*/
static PgHdr1 *pcache1AllocPage(PCache1 *pCache){
  int nByte = sizeof(PgHdr1) + pCache->szPage;
  void *pPg = pcache1Alloc(nByte);
  PgHdr1 *p;
  if( pPg ){
    p = PAGE_TO_PGHDR1(pCache, pPg);
    if( pCache->bPurgeable ){
      pcache1.nCurrentPage++;
    }
  }else{
    p = 0;
  }
  return p;
}

/*
** Free a page object allocated by pcache1AllocPage().
**
** The pointer is allowed to be NULL, which is prudent.  But it turns out
** that the current implementation happens to never call this routine
** with a NULL pointer, so we mark the NULL test with ALWAYS().
*/
static void pcache1FreePage(PgHdr1 *p){
  if( ALWAYS(p) ){
    if( p->pCache->bPurgeable ){
      pcache1.nCurrentPage--;
    }
    pcache1Free(PGHDR1_TO_PAGE(p));
  }
}

/*
** Malloc function used by SQLite to obtain space from the buffer configured
** using sqlite3_config(SQLITE_CONFIG_PAGECACHE) option. If no such buffer
** exists, this function falls back to sqlite3Malloc().
*/
void *sqlite3PageMalloc(int sz){
  void *p;
  pcache1EnterMutex();
  p = pcache1Alloc(sz);
  pcache1LeaveMutex();
  return p;
}

/*
** Free an allocated buffer obtained from sqlite3PageMalloc().
*/
void sqlite3PageFree(void *p){
  pcache1EnterMutex();
  pcache1Free(p);
  pcache1LeaveMutex();
}

/******************************************************************************/
/******** General Implementation Functions ************************************/

/*
** This function is used to resize the hash table used by the cache passed
** as the first argument.
**
** The global mutex must be held when this function is called.
*/
static int pcache1ResizeHash(PCache1 *p){
  PgHdr1 **apNew;
  unsigned int nNew;
  unsigned int i;

  assert( sqlite3_mutex_held(pcache1.mutex) );

  nNew = p->nHash*2;
  if( nNew<256 ){
    nNew = 256;
  }

  pcache1LeaveMutex();
  if( p->nHash ){ sqlite3BeginBenignMalloc(); }
  apNew = (PgHdr1 **)sqlite3_malloc(sizeof(PgHdr1 *)*nNew);
  if( p->nHash ){ sqlite3EndBenignMalloc(); }
  pcache1EnterMutex();
  if( apNew ){
    memset(apNew, 0, sizeof(PgHdr1 *)*nNew);
    for(i=0; i<p->nHash; i++){
      PgHdr1 *pPage;
      PgHdr1 *pNext = p->apHash[i];
      while( (pPage = pNext)!=0 ){
        unsigned int h = pPage->iKey % nNew;
        pNext = pPage->pNext;
        pPage->pNext = apNew[h];
        apNew[h] = pPage;
      }
    }
    sqlite3_free(p->apHash);
    p->apHash = apNew;
    p->nHash = nNew;
  }

  return (p->apHash ? SQLITE_OK : SQLITE_NOMEM);
}

/*
** This function is used internally to remove the page pPage from the 
** global LRU list, if is part of it. If pPage is not part of the global
** LRU list, then this function is a no-op.
**
** The global mutex must be held when this function is called.
*/
static void pcache1PinPage(PgHdr1 *pPage){
  assert( sqlite3_mutex_held(pcache1.mutex) );
  if( pPage && (pPage->pLruNext || pPage==pcache1.pLruTail) ){
    if( pPage->pLruPrev ){
      pPage->pLruPrev->pLruNext = pPage->pLruNext;
    }
    if( pPage->pLruNext ){
      pPage->pLruNext->pLruPrev = pPage->pLruPrev;
    }
    if( pcache1.pLruHead==pPage ){
      pcache1.pLruHead = pPage->pLruNext;
    }
    if( pcache1.pLruTail==pPage ){
      pcache1.pLruTail = pPage->pLruPrev;
    }
    pPage->pLruNext = 0;
    pPage->pLruPrev = 0;
    pPage->pCache->nRecyclable--;
  }
}


/*
** Remove the page supplied as an argument from the hash table 
** (PCache1.apHash structure) that it is currently stored in.
**
** The global mutex must be held when this function is called.
*/
static void pcache1RemoveFromHash(PgHdr1 *pPage){
  unsigned int h;
  PCache1 *pCache = pPage->pCache;
  PgHdr1 **pp;

  h = pPage->iKey % pCache->nHash;
  for(pp=&pCache->apHash[h]; (*pp)!=pPage; pp=&(*pp)->pNext);
  *pp = (*pp)->pNext;

  pCache->nPage--;
}

/*
** If there are currently more than pcache.nMaxPage pages allocated, try
** to recycle pages to reduce the number allocated to pcache.nMaxPage.
*/
static void pcache1EnforceMaxPage(void){
  assert( sqlite3_mutex_held(pcache1.mutex) );
  while( pcache1.nCurrentPage>pcache1.nMaxPage && pcache1.pLruTail ){
    PgHdr1 *p = pcache1.pLruTail;
    pcache1PinPage(p);
    pcache1RemoveFromHash(p);
    pcache1FreePage(p);
  }
}

/*
** Discard all pages from cache pCache with a page number (key value) 
** greater than or equal to iLimit. Any pinned pages that meet this 
** criteria are unpinned before they are discarded.
**
** The global mutex must be held when this function is called.
*/
static void pcache1TruncateUnsafe(
  PCache1 *pCache, 
  unsigned int iLimit 
){
  TESTONLY( unsigned int nPage = 0; )      /* Used to assert pCache->nPage is correct */
  unsigned int h;
  assert( sqlite3_mutex_held(pcache1.mutex) );
  for(h=0; h<pCache->nHash; h++){
    PgHdr1 **pp = &pCache->apHash[h]; 
    PgHdr1 *pPage;
    while( (pPage = *pp)!=0 ){
      if( pPage->iKey>=iLimit ){
        pCache->nPage--;
        *pp = pPage->pNext;
        pcache1PinPage(pPage);
        pcache1FreePage(pPage);
      }else{
        pp = &pPage->pNext;
        TESTONLY( nPage++; )
      }
    }
  }
  assert( pCache->nPage==nPage );
}

/******************************************************************************/
/******** sqlite3_pcache Methods **********************************************/

/*
** Implementation of the sqlite3_pcache.xInit method.
*/
static int pcache1Init(void *NotUsed){
  UNUSED_PARAMETER(NotUsed);
  assert( pcache1.isInit==0 );
  memset(&pcache1, 0, sizeof(pcache1));
  if( sqlite3GlobalConfig.bCoreMutex ){
    pcache1.mutex = sqlite3_mutex_alloc(SQLITE_MUTEX_STATIC_LRU);
  }
  pcache1.isInit = 1;
  return SQLITE_OK;
}

/*
** Implementation of the sqlite3_pcache.xShutdown method.
** Note that the static mutex allocated in xInit does 
** not need to be freed.
*/
static void pcache1Shutdown(void *NotUsed){
  UNUSED_PARAMETER(NotUsed);
  assert( pcache1.isInit!=0 );
  memset(&pcache1, 0, sizeof(pcache1));
}

/*
** Implementation of the sqlite3_pcache.xCreate method.
**
** Allocate a new cache.
*/
static sqlite3_pcache *pcache1Create(int szPage, int bPurgeable){
  PCache1 *pCache;

  pCache = (PCache1 *)sqlite3_malloc(sizeof(PCache1));
  if( pCache ){
    memset(pCache, 0, sizeof(PCache1));
    pCache->szPage = szPage;
    pCache->bPurgeable = (bPurgeable ? 1 : 0);
    if( bPurgeable ){
      pCache->nMin = 10;
      pcache1EnterMutex();
      pcache1.nMinPage += pCache->nMin;
      pcache1LeaveMutex();
    }
  }
  return (sqlite3_pcache *)pCache;
}

/*
** Implementation of the sqlite3_pcache.xCachesize method. 
**
** Configure the cache_size limit for a cache.
*/
static void pcache1Cachesize(sqlite3_pcache *p, int nMax){
  PCache1 *pCache = (PCache1 *)p;
  if( pCache->bPurgeable ){
    pcache1EnterMutex();
    pcache1.nMaxPage += (nMax - pCache->nMax);
    pCache->nMax = nMax;
    pcache1EnforceMaxPage();
    pcache1LeaveMutex();
  }
}

/*
** Implementation of the sqlite3_pcache.xPagecount method. 
*/
static int pcache1Pagecount(sqlite3_pcache *p){
  int n;
  pcache1EnterMutex();
  n = ((PCache1 *)p)->nPage;
  pcache1LeaveMutex();
  return n;
}

/*
** Implementation of the sqlite3_pcache.xFetch method. 
**
** Fetch a page by key value.
**
** Whether or not a new page may be allocated by this function depends on
** the value of the createFlag argument.  0 means do not allocate a new
** page.  1 means allocate a new page if space is easily available.  2 
** means to try really hard to allocate a new page.
**
** For a non-purgeable cache (a cache used as the storage for an in-memory
** database) there is really no difference between createFlag 1 and 2.  So
** the calling function (pcache.c) will never have a createFlag of 1 on
** a non-purgable cache.
**
** There are three different approaches to obtaining space for a page,
** depending on the value of parameter createFlag (which may be 0, 1 or 2).
**
**   1. Regardless of the value of createFlag, the cache is searched for a 
**      copy of the requested page. If one is found, it is returned.
**
**   2. If createFlag==0 and the page is not already in the cache, NULL is
**      returned.
**
**   3. If createFlag is 1, and the page is not already in the cache,
**      and if either of the following are true, return NULL:
**
**       (a) the number of pages pinned by the cache is greater than
**           PCache1.nMax, or
**       (b) the number of pages pinned by the cache is greater than
**           the sum of nMax for all purgeable caches, less the sum of 
**           nMin for all other purgeable caches. 
**
**   4. If none of the first three conditions apply and the cache is marked
**      as purgeable, and if one of the following is true:
**
**       (a) The number of pages allocated for the cache is already 
**           PCache1.nMax, or
**
**       (b) The number of pages allocated for all purgeable caches is
**           already equal to or greater than the sum of nMax for all
**           purgeable caches,
**
**      then attempt to recycle a page from the LRU list. If it is the right
**      size, return the recycled buffer. Otherwise, free the buffer and
**      proceed to step 5. 
**
**   5. Otherwise, allocate and return a new page buffer.
*/
static void *pcache1Fetch(sqlite3_pcache *p, unsigned int iKey, int createFlag){
  unsigned int nPinned;
  PCache1 *pCache = (PCache1 *)p;
  PgHdr1 *pPage = 0;

  assert( pCache->bPurgeable || createFlag!=1 );
  pcache1EnterMutex();
  if( createFlag==1 ) sqlite3BeginBenignMalloc();

  /* Search the hash table for an existing entry. */
  if( pCache->nHash>0 ){
    unsigned int h = iKey % pCache->nHash;
    for(pPage=pCache->apHash[h]; pPage&&pPage->iKey!=iKey; pPage=pPage->pNext);
  }

  if( pPage || createFlag==0 ){
    pcache1PinPage(pPage);
    goto fetch_out;
  }

  /* Step 3 of header comment. */
  nPinned = pCache->nPage - pCache->nRecyclable;
  if( createFlag==1 && (
        nPinned>=(pcache1.nMaxPage+pCache->nMin-pcache1.nMinPage)
     || nPinned>=(pCache->nMax * 9 / 10)
  )){
    goto fetch_out;
  }

  if( pCache->nPage>=pCache->nHash && pcache1ResizeHash(pCache) ){
    goto fetch_out;
  }

  /* Step 4. Try to recycle a page buffer if appropriate. */
  if( pCache->bPurgeable && pcache1.pLruTail && (
     (pCache->nPage+1>=pCache->nMax) || pcache1.nCurrentPage>=pcache1.nMaxPage
  )){
    pPage = pcache1.pLruTail;
    pcache1RemoveFromHash(pPage);
    pcache1PinPage(pPage);
    if( pPage->pCache->szPage!=pCache->szPage ){
      pcache1FreePage(pPage);
      pPage = 0;
    }else{
      pcache1.nCurrentPage -= (pPage->pCache->bPurgeable - pCache->bPurgeable);
    }
  }

  /* Step 5. If a usable page buffer has still not been found, 
  ** attempt to allocate a new one. 
  */
  if( !pPage ){
    pPage = pcache1AllocPage(pCache);
  }

  if( pPage ){
    unsigned int h = iKey % pCache->nHash;
    pCache->nPage++;
    pPage->iKey = iKey;
    pPage->pNext = pCache->apHash[h];
    pPage->pCache = pCache;
    pPage->pLruPrev = 0;
    pPage->pLruNext = 0;
    *(void **)(PGHDR1_TO_PAGE(pPage)) = 0;
    pCache->apHash[h] = pPage;
  }

fetch_out:
  if( pPage && iKey>pCache->iMaxKey ){
    pCache->iMaxKey = iKey;
  }
  if( createFlag==1 ) sqlite3EndBenignMalloc();
  pcache1LeaveMutex();
  return (pPage ? PGHDR1_TO_PAGE(pPage) : 0);
}


/*
** Implementation of the sqlite3_pcache.xUnpin method.
**
** Mark a page as unpinned (eligible for asynchronous recycling).
*/
static void pcache1Unpin(sqlite3_pcache *p, void *pPg, int reuseUnlikely){
  PCache1 *pCache = (PCache1 *)p;
  PgHdr1 *pPage = PAGE_TO_PGHDR1(pCache, pPg);
 
  assert( pPage->pCache==pCache );
  pcache1EnterMutex();

  /* It is an error to call this function if the page is already 
  ** part of the global LRU list.
  */
  assert( pPage->pLruPrev==0 && pPage->pLruNext==0 );
  assert( pcache1.pLruHead!=pPage && pcache1.pLruTail!=pPage );

  if( reuseUnlikely || pcache1.nCurrentPage>pcache1.nMaxPage ){
    pcache1RemoveFromHash(pPage);
    pcache1FreePage(pPage);
  }else{
    /* Add the page to the global LRU list. Normally, the page is added to
    ** the head of the list (last page to be recycled). However, if the 
    ** reuseUnlikely flag passed to this function is true, the page is added
    ** to the tail of the list (first page to be recycled).
    */
    if( pcache1.pLruHead ){
      pcache1.pLruHead->pLruPrev = pPage;
      pPage->pLruNext = pcache1.pLruHead;
      pcache1.pLruHead = pPage;
    }else{
      pcache1.pLruTail = pPage;
      pcache1.pLruHead = pPage;
    }
    pCache->nRecyclable++;
  }

  pcache1LeaveMutex();
}

/*
** Implementation of the sqlite3_pcache.xRekey method. 
*/
static void pcache1Rekey(
  sqlite3_pcache *p,
  void *pPg,
  unsigned int iOld,
  unsigned int iNew
){
  PCache1 *pCache = (PCache1 *)p;
  PgHdr1 *pPage = PAGE_TO_PGHDR1(pCache, pPg);
  PgHdr1 **pp;
  unsigned int h; 
  assert( pPage->iKey==iOld );
  assert( pPage->pCache==pCache );

  pcache1EnterMutex();

  h = iOld%pCache->nHash;
  pp = &pCache->apHash[h];
  while( (*pp)!=pPage ){
    pp = &(*pp)->pNext;
  }
  *pp = pPage->pNext;

  h = iNew%pCache->nHash;
  pPage->iKey = iNew;
  pPage->pNext = pCache->apHash[h];
  pCache->apHash[h] = pPage;
  if( iNew>pCache->iMaxKey ){
    pCache->iMaxKey = iNew;
  }

  pcache1LeaveMutex();
}

/*
** Implementation of the sqlite3_pcache.xTruncate method. 
**
** Discard all unpinned pages in the cache with a page number equal to
** or greater than parameter iLimit. Any pinned pages with a page number
** equal to or greater than iLimit are implicitly unpinned.
*/
static void pcache1Truncate(sqlite3_pcache *p, unsigned int iLimit){
  PCache1 *pCache = (PCache1 *)p;
  pcache1EnterMutex();
  if( iLimit<=pCache->iMaxKey ){
    pcache1TruncateUnsafe(pCache, iLimit);
    pCache->iMaxKey = iLimit-1;
  }
  pcache1LeaveMutex();
}

/*
** Implementation of the sqlite3_pcache.xDestroy method. 
**
** Destroy a cache allocated using pcache1Create().
*/
static void pcache1Destroy(sqlite3_pcache *p){
  PCache1 *pCache = (PCache1 *)p;
  pcache1EnterMutex();
  pcache1TruncateUnsafe(pCache, 0);
  pcache1.nMaxPage -= pCache->nMax;
  pcache1.nMinPage -= pCache->nMin;
  pcache1EnforceMaxPage();
  pcache1LeaveMutex();
  sqlite3_free(pCache->apHash);
  sqlite3_free(pCache);
}

/*
** This function is called during initialization (sqlite3_initialize()) to
** install the default pluggable cache module, assuming the user has not
** already provided an alternative.
*/
void sqlite3PCacheSetDefault(void){
  static const sqlite3_pcache_methods defaultMethods = {
    0,                       /* pArg */
    pcache1Init,             /* xInit */
    pcache1Shutdown,         /* xShutdown */
    pcache1Create,           /* xCreate */
    pcache1Cachesize,        /* xCachesize */
    pcache1Pagecount,        /* xPagecount */
    pcache1Fetch,            /* xFetch */
    pcache1Unpin,            /* xUnpin */
    pcache1Rekey,            /* xRekey */
    pcache1Truncate,         /* xTruncate */
    pcache1Destroy           /* xDestroy */
  };
  sqlite3_config(SQLITE_CONFIG_PCACHE, &defaultMethods);
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
  if( pcache1.pStart==0 ){
    PgHdr1 *p;
    pcache1EnterMutex();
    while( (nReq<0 || nFree<nReq) && (p=pcache1.pLruTail) ){
      nFree += sqlite3MallocSize(PGHDR1_TO_PAGE(p));
      pcache1PinPage(p);
      pcache1RemoveFromHash(p);
      pcache1FreePage(p);
    }
    pcache1LeaveMutex();
  }
  return nFree;
}
#endif /* SQLITE_ENABLE_MEMORY_MANAGEMENT */

#ifdef SQLITE_TEST
/*
** This function is used by test procedures to inspect the internal state
** of the global cache.
*/
void sqlite3PcacheStats(
  int *pnCurrent,      /* OUT: Total number of pages cached */
  int *pnMax,          /* OUT: Global maximum cache size */
  int *pnMin,          /* OUT: Sum of PCache1.nMin for purgeable caches */
  int *pnRecyclable    /* OUT: Total number of pages available for recycling */
){
  PgHdr1 *p;
  int nRecyclable = 0;
  for(p=pcache1.pLruHead; p; p=p->pLruNext){
    nRecyclable++;
  }
  *pnCurrent = pcache1.nCurrentPage;
  *pnMax = pcache1.nMaxPage;
  *pnMin = pcache1.nMinPage;
  *pnRecyclable = nRecyclable;
}
#endif

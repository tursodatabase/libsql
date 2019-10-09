/*
** 2019-10-02
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
** Lookaside memory allocation functions used throughout sqlite.
*/

#include "sqliteInt.h"
#include "lookaside.h"

/*
** Return the number of LookasideSlot elements on the linked list
*/
static u32 countLookasideSlots(LookasideSlot *p){
  u32 cnt = 0;
  while( p ){
    p = p->pNext;
    cnt++;
  }
  return cnt;
}

/*
** Count the number of slots of lookaside memory that are outstanding
*/
int sqlite3LookasideUsed(Lookaside *pLookaside, int *pHighwater){
  u32 nInit = countLookasideSlots(pLookaside->pInit);
  u32 nFree = countLookasideSlots(pLookaside->pFree);
  if( pHighwater ) *pHighwater = pLookaside->nSlot - nInit;
  return pLookaside->nSlot - (nInit+nFree);
}

void sqlite3LookasideResetUsed(Lookaside *pLookaside){
  LookasideSlot *p = pLookaside->pFree;
  if( p ){
    while( p->pNext ) p = p->pNext;
    p->pNext = pLookaside->pInit;
    pLookaside->pInit = pLookaside->pFree;
    pLookaside->pFree = 0;
  }
}

#ifndef SQLITE_OMIT_LOOKASIDE

static void *lookasideSlotAlloc(Lookaside *pLookaside, u64 n){
  LookasideSlot *pBuf;
  if( (pBuf = pLookaside->pFree)!=0 ){
    pLookaside->pFree = pBuf->pNext;
    pLookaside->anStat[0]++;
    return (void*)pBuf;
  }else if( (pBuf = pLookaside->pInit)!=0 ){
    pLookaside->pInit = pBuf->pNext;
    pLookaside->anStat[0]++;
    return (void*)pBuf;
  }else{
    pLookaside->anStat[2]++;
    return 0;
  }
}

static void lookasideSlotFree(Lookaside *pLookaside, void *p){
  LookasideSlot *pBuf = (LookasideSlot*)p;
# ifdef SQLITE_DEBUG
  /* Scribble over the content in the buffer being freed */
  memset(p, 0xaa, pLookaside->szTrue);
# endif
  pBuf->pNext = pLookaside->pFree;
  pLookaside->pFree = pBuf;
}

# ifndef SQLITE_OMIT_MINI_LOOKASIDE
#  ifndef SQLITE_MINI_LOOKASIDE_MIN_SLOT_SIZE
#   define SQLITE_MINI_LOOKASIDE_MIN_SLOT_SIZE 128
#  endif

static void *miniLookasideAlloc(Lookaside *pLookaside, u16 n){
  void *p = 0;
  LookasideSlot *pSlot;
  int iMiniSlot;
  
  assert( n<=pLookaside->szMini );
  
  if( !pLookaside->pMini ){
    pSlot = lookasideSlotAlloc(pLookaside, pLookaside->szTrue);
    if( !pSlot ){
      return p;
    }
    bzero(pSlot, sizeof(LookasideSlot));
    pLookaside->pMini = pSlot;
  }else{
    pSlot = pLookaside->pMini;
    assert( pSlot->bMembership );
  }
  
  assert( pSlot->bMembership < (1<<pLookaside->nMini)-1 );

  iMiniSlot = __builtin_ffs(~pSlot->bMembership) - 1;
  assert(iMiniSlot < pLookaside->nMini);
  assert( (pSlot->bMembership&(1<<iMiniSlot))==0 );
  pSlot->bMembership |= 1<<iMiniSlot;

  p = (char *)pSlot + sizeof(LookasideSlot) + (pLookaside->szMini * iMiniSlot);

  /* Remove slot from pMini if it is full of sub-allocations */
  if( pSlot->bMembership == (1<<pLookaside->nMini)-1 ){
    /* Slot is full, dequeue from list */
    if( pSlot->pNext ){
      assert( pSlot->pNext->pPrev == pSlot );
      pSlot->pNext->pPrev = pSlot->pPrev;
    }
    if( pSlot->pPrev ){
      assert( pSlot->pPrev->pNext == pSlot );
      pSlot->pPrev->pNext = pSlot->pNext;
    }else{
      assert( pLookaside->pMini == pSlot );
      pLookaside->pMini = pSlot->pNext;
    }
    pSlot->pNext = pSlot->pPrev = 0;
  }
  return p;
}

static void miniLookasideFree(Lookaside *pLookaside, void *p){
  int iSlotNum = ((u8*)p - (u8*)pLookaside->pStart) / pLookaside->szTrue;
  LookasideSlot *pSlot = (LookasideSlot *)(iSlotNum * pLookaside->szTrue + (u8*)pLookaside->pStart);
  int iMiniSlot = ((u8*)p - ((u8*)pSlot + sizeof(LookasideSlot))) / pLookaside->szMini;
  
  assert( pSlot->bMembership );
  assert( pSlot->bMembership < (1<<pLookaside->nMini) );
  assert( iMiniSlot<pLookaside->nMini );
  
  /* Return slot to pMini list if it was full */
  if( pSlot->bMembership == (1<<pLookaside->nMini)-1 ){
    assert( pSlot->pNext == pSlot->pPrev && pSlot->pPrev == 0 );
    if( pLookaside->pMini ){
      assert( !pLookaside->pMini->pPrev );
      pSlot->pNext = pLookaside->pMini;
      pSlot->pNext->pPrev = pSlot;
    }
    pLookaside->pMini = pSlot;
  }
  
  pSlot->bMembership &= ~(1<<iMiniSlot);
#ifdef SQLITE_DEBUG
  memset(p, 0xaa, pLookaside->szMini);
#endif
  
  /* Return slot to the lookaside pool if it is empty */
  if( pSlot->bMembership == 0 ){
    if( pSlot->pNext ){
      assert( pSlot->pNext->pPrev == pSlot );
      pSlot->pNext->pPrev = pSlot->pPrev;
    }
    if( pSlot->pPrev ){
      assert( pSlot->pPrev->pNext == pSlot );
      pSlot->pPrev->pNext = pSlot->pNext;
    }else{
      assert( pLookaside->pMini==pSlot );
      pLookaside->pMini = pSlot->pNext;
    }
    lookasideSlotFree(pLookaside, pSlot);
  }
}

# else
#  define miniLookasideAlloc(A, B) lookasideSlotAlloc(A, B)
#  define miniLookasideFree(A, B) lookasideSlowFree(A, B)
# endif /* !SQLITE_OMIT_MINI_LOOKASIDE */

int sqlite3LookasideOpen(void *pBuf, int sz, int cnt, Lookaside *pLookaside){
  void *pStart;

  if( sqlite3LookasideUsed(pLookaside,0)>0 ){
    return SQLITE_BUSY;
  }
  /* Free any existing lookaside buffer for this handle before
  ** allocating a new one so we don't have to have space for
  ** both at the same time.
  */
  if( pLookaside->bMalloced ){
    sqlite3_free(pLookaside->pStart);
  }
  /* The size of a lookaside slot after ROUNDDOWN8 needs to be larger
  ** than sizeof(LookasideSlot) to be useful.
  */
  sz = ROUNDDOWN8(sz);  /* IMP: R-33038-09382 */
  if( sz<=(int)sizeof(LookasideSlot*) ) sz = 0;
  if( cnt<0 ) cnt = 0;
  if( sz==0 || cnt==0 ){
    sz = 0;
    pStart = 0;
  }else if( pBuf==0 ){
    sqlite3BeginBenignMalloc();
    pStart = sqlite3Malloc( sz*(sqlite3_int64)cnt );  /* IMP: R-61949-35727 */
    sqlite3EndBenignMalloc();
    if( pStart ) cnt = sqlite3MallocSize(pStart)/sz;
  }else{
    pStart = pBuf;
  }
  pLookaside->pStart = pStart;
  pLookaside->pInit = 0;
  pLookaside->pFree = 0;
  pLookaside->sz = (u16)sz;
  pLookaside->szTrue = (u16)sz;
#ifndef SQLITE_OMIT_MINILOOKASIDE
  pLookaside->pMini = 0;
  pLookaside->nMini = (sz - sizeof(LookasideSlot)) / SQLITE_MINI_LOOKASIDE_MIN_SLOT_SIZE;
  if( pLookaside->nMini ){
    pLookaside->szMini = ((sz - sizeof(LookasideSlot)) / pLookaside->nMini) & ~(sizeof(void *) - 1);
  }else{
    pLookaside->szMini = 0;
  }
#endif /* SQLITE_OMIT_MINILOOKASIDE */
  if( pStart ){
    int i;
    LookasideSlot *p;
    assert( sz > (int)sizeof(LookasideSlot*) );
    pLookaside->nSlot = cnt;
    p = (LookasideSlot*)pStart;
    for(i=cnt-1; i>=0; i--){
      p->pNext = pLookaside->pInit;
      pLookaside->pInit = p;
      p = (LookasideSlot*)&((u8*)p)[sz];
    }
    pLookaside->pEnd = p;
    pLookaside->bDisable = 0;
    pLookaside->bMalloced = pBuf==0 ?1:0;
  }else{
    pLookaside->pStart = 0;
    pLookaside->pEnd = 0;
    pLookaside->bDisable = 1;
    pLookaside->bMalloced = 0;
    pLookaside->nSlot = 0;
  }
  return SQLITE_OK;
}

void sqlite3LookasideClose(Lookaside *pLookaside){
  assert( sqlite3LookasideUsed(pLookaside,0)==0 );
  if( pLookaside->bMalloced ){
    sqlite3_free(pLookaside->pStart);
  }
}

int sqlite3IsLookaside(Lookaside *pLookaside, void *p){
  return SQLITE_WITHIN(p, pLookaside->pStart, pLookaside->pEnd);
}

/*
** Returns a pointer to a region at least n bytes in size, or NULL if the
** lookaside allocator has exhausted its available memory.
*/
void *sqlite3LookasideAlloc(Lookaside *pLookaside, u64 n){
  if( n>pLookaside->sz ){
    if( !pLookaside->bDisable ){
      pLookaside->anStat[1]++;
    }
    return 0;
  }
  if( n<=pLookaside->szMini && pLookaside->nMini > 1 ){
    return miniLookasideAlloc(pLookaside, n);
  }
  return lookasideSlotAlloc(pLookaside, n);
}

/*
** Free memory previously obtained from sqlite3LookasideAlloc().
*/
void sqlite3LookasideFree(Lookaside *pLookaside, void *p){
  assert( sqlite3IsLookaside(pLookaside, p) );
  if( ((u8*)p - (u8*)pLookaside->pStart) % pLookaside->szTrue == 0 ){
    lookasideSlotFree(pLookaside, p);
  }else{
    miniLookasideFree(pLookaside, p);
  }
}

/*
** Return the size of a memory allocation previously obtained from
** sqlite3LookasideAlloc().
*/
int sqlite3LookasideSize(Lookaside *pLookaside, void *p){
  assert(sqlite3IsLookaside(pLookaside, p));

# ifndef SQLITE_OMIT_MINI_LOOKASIDE
  if( ((u8*)p - (u8*)pLookaside->pStart) % pLookaside->szTrue != 0 ){
    return pLookaside->szMini;
  }else
#endif /* SQLITE_OMIT_MINI_LOOKASIDE */
  {
    return pLookaside->szTrue;
  }
}

#endif /* !SQLITE_OMIT_LOOKASIDE */

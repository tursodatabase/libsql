/*
** 2008 July 24
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
** This file contains an alternative memory allocation system for SQLite.
** This system is implemented as a wrapper around the system provided
** by the operating system - vanilla malloc(), realloc() and free().
**
** This system differentiates between requests for "small" allocations 
** (by default those of 128 bytes or less) and "large" allocations (all
** others). The 256 byte threshhold is configurable at runtime.
**
** All requests for large allocations are passed through to the 
** default system.
**
** Requests for small allocations are met by allocating space within
** one or more larger "chunks" of memory obtained from the default
** memory allocation system. Chunks of memory are usually 64KB or 
** larger. The algorithm used to manage space within each chunk is
** the same as that used by mem5.c. 
**
** This strategy is designed to prevent the default memory allocation
** system (usually the system malloc) from suffering from heap 
** fragmentation. On some systems, heap fragmentation can cause a 
** significant real-time slowdown.
**
** $Id: mem6.c,v 1.7 2008/07/28 19:34:53 drh Exp $
*/

#ifdef SQLITE_ENABLE_MEMSYS6

#include "sqliteInt.h"

/*
** Maximum size of any "small" allocation is ((1<<LOGMAX)*Mem6Chunk.nAtom).
** Mem6Chunk.nAtom is always at least 8, so this is not a practical
** limitation
*/
#define LOGMAX 30

/*
** Default value for the "small" allocation size threshold.
*/
#define SMALL_MALLOC_DEFAULT_THRESHOLD 256

/*
** Minimum size for a memory chunk.
*/
#define MIN_CHUNKSIZE (1<<16)

#define LOG2_MINALLOC 4


typedef struct Mem6Chunk Mem6Chunk;
typedef struct Mem6Link Mem6Link;

/*
** A minimum allocation is an instance of the following structure.
** Larger allocations are an array of these structures where the
** size of the array is a power of 2.
*/
struct Mem6Link {
  int next;       /* Index of next free chunk */
  int prev;       /* Index of previous free chunk */
};

/*
** Masks used for mem5.aCtrl[] elements.
*/
#define CTRL_LOGSIZE  0x1f    /* Log2 Size of this block relative to POW2_MIN */
#define CTRL_FREE     0x20    /* True if not checked out */

struct Mem6Chunk {
  Mem6Chunk *pNext;

  /*
  ** Lists of free blocks of various sizes.
  */
  int aiFreelist[LOGMAX+1];

  int nCheckedOut; /* Number of currently outstanding allocations */

  /*
  ** Space for tracking which blocks are checked out and the size
  ** of each block. One byte per block.
  */
  u8 *aCtrl;

  /*
  ** Memory available for allocation
  */
  int nAtom;       /* Smallest possible allocation in bytes */
  int nBlock;      /* Number of nAtom sized blocks in zPool */
  u8 *zPool;       /* Pointer to memory chunk from which allocations are made */
};

#define MEM6LINK(idx) ((Mem6Link *)(&pChunk->zPool[(idx)*pChunk->nAtom]))

struct Mem6Global {
  int nMinAlloc;                  /* Minimum allowed allocation size */
  int nThreshold;                 /* Allocs larger than this go to malloc() */
  int nLogThreshold;              /* log2 of (nThreshold/nMinAlloc) */
  sqlite3_mutex *mutex;
  Mem6Chunk *pChunk;              /* Singly linked list of all memory chunks */
} mem6;

/*
** Unlink the chunk at pChunk->aPool[i] from list it is currently
** on.  It should be found on pChunk->aiFreelist[iLogsize].
*/
static void memsys6Unlink(Mem6Chunk *pChunk, int i, int iLogsize){
  int next, prev;
  assert( i>=0 && i<pChunk->nBlock );
  assert( iLogsize>=0 && iLogsize<=mem6.nLogThreshold );
  assert( (pChunk->aCtrl[i] & CTRL_LOGSIZE)==iLogsize );

  next = MEM6LINK(i)->next;
  prev = MEM6LINK(i)->prev;
  if( prev<0 ){
    pChunk->aiFreelist[iLogsize] = next;
  }else{
    MEM6LINK(prev)->next = next;
  }
  if( next>=0 ){
    MEM6LINK(next)->prev = prev;
  }
}

/*
** Link the chunk at mem5.aPool[i] so that is on the iLogsize
** free list.
*/
static void memsys6Link(Mem6Chunk *pChunk, int i, int iLogsize){
  int x;
  assert( i>=0 && i<pChunk->nBlock );
  assert( iLogsize>=0 && iLogsize<=mem6.nLogThreshold );
  assert( (pChunk->aCtrl[i] & CTRL_LOGSIZE)==iLogsize );

  x = MEM6LINK(i)->next = pChunk->aiFreelist[iLogsize];
  MEM6LINK(i)->prev = -1;
  if( x>=0 ){
    assert( x<pChunk->nBlock );
    MEM6LINK(x)->prev = i;
  }
  pChunk->aiFreelist[iLogsize] = i;
}


/*
** Find the first entry on the freelist iLogsize.  Unlink that
** entry and return its index. 
*/
static int memsys6UnlinkFirst(Mem6Chunk *pChunk, int iLogsize){
  int i;
  int iFirst;

  assert( iLogsize>=0 && iLogsize<=mem6.nLogThreshold );
  i = iFirst = pChunk->aiFreelist[iLogsize];
  assert( iFirst>=0 );
  memsys6Unlink(pChunk, iFirst, iLogsize);
  return iFirst;
}

static int roundupLog2(int n){
  static const char LogTable256[256] = {
    0,                                                    /* 1 */
    1,                                                    /* 2 */
    2, 2,                                                 /* 3..4 */
    3, 3, 3, 3,                                           /* 5..8 */
    4, 4, 4, 4, 4, 4, 4, 4,                               /* 9..16 */
    5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5,       /* 17..32 */
    6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6,
    6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6,       /* 33..64 */
    7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7,
    7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7,
    7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7,
    7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7,       /* 65..128 */
    8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8,
    8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8,
    8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8,
    8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8,
    8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8,
    8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8,
    8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8,
    8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8,       /* 129..256 */
  };

  assert(n<=(1<<16) && n>0);
  if( n<=256 ) return LogTable256[n-1];
  return LogTable256[(n>>8) - ((n&0xFF)?0:1)] + 8;
}

/*
** Allocate and return a block of (pChunk->nAtom << iLogsize) bytes from chunk
** pChunk. If the allocation request cannot be satisfied, return 0.
*/
static void *chunkMalloc(Mem6Chunk *pChunk, int iLogsize){
  int i;           /* Index of a mem5.aPool[] slot */
  int iBin;        /* Index into mem5.aiFreelist[] */

  /* Make sure mem5.aiFreelist[iLogsize] contains at least one free
  ** block.  If not, then split a block of the next larger power of
  ** two in order to create a new free block of size iLogsize.
  */
  for(iBin=iLogsize; pChunk->aiFreelist[iBin]<0 && iBin<=mem6.nLogThreshold; iBin++){}
  if( iBin>mem6.nLogThreshold ) return 0;
  i = memsys6UnlinkFirst(pChunk, iBin);
  while( iBin>iLogsize ){
    int newSize;
    iBin--;
    newSize = 1 << iBin;
    pChunk->aCtrl[i+newSize] = CTRL_FREE | iBin;
    memsys6Link(pChunk, i+newSize, iBin);
  }
  pChunk->aCtrl[i] = iLogsize;

  /* Return a pointer to the allocated memory. */
  pChunk->nCheckedOut++;
  return (void*)&pChunk->zPool[i*pChunk->nAtom];
}

/*
** Free the allocation pointed to by p, which is guaranteed to be non-zero
** and a part of chunk object pChunk.
*/
static void chunkFree(Mem6Chunk *pChunk, void *pOld){
  u32 size, iLogsize;
  int iBlock;             

  /* Set iBlock to the index of the block pointed to by pOld in 
  ** the array of pChunk->nAtom byte blocks pointed to by pChunk->zPool.
  */
  iBlock = ((u8 *)pOld-pChunk->zPool)/pChunk->nAtom;

  /* Check that the pointer pOld points to a valid, non-free block. */
  assert( iBlock>=0 && iBlock<pChunk->nBlock );
  assert( ((u8 *)pOld-pChunk->zPool)%pChunk->nAtom==0 );
  assert( (pChunk->aCtrl[iBlock] & CTRL_FREE)==0 );

  iLogsize = pChunk->aCtrl[iBlock] & CTRL_LOGSIZE;
  size = 1<<iLogsize;
  assert( iBlock+size-1<pChunk->nBlock );

  pChunk->aCtrl[iBlock] |= CTRL_FREE;
  pChunk->aCtrl[iBlock+size-1] |= CTRL_FREE;

  pChunk->aCtrl[iBlock] = CTRL_FREE | iLogsize;
  while( iLogsize<mem6.nLogThreshold ){
    int iBuddy;
    if( (iBlock>>iLogsize) & 1 ){
      iBuddy = iBlock - size;
    }else{
      iBuddy = iBlock + size;
    }
    assert( iBuddy>=0 );
    if( (iBuddy+(1<<iLogsize))>pChunk->nBlock ) break;
    if( pChunk->aCtrl[iBuddy]!=(CTRL_FREE | iLogsize) ) break;
    memsys6Unlink(pChunk, iBuddy, iLogsize);
    iLogsize++;
    if( iBuddy<iBlock ){
      pChunk->aCtrl[iBuddy] = CTRL_FREE | iLogsize;
      pChunk->aCtrl[iBlock] = 0;
      iBlock = iBuddy;
    }else{
      pChunk->aCtrl[iBlock] = CTRL_FREE | iLogsize;
      pChunk->aCtrl[iBuddy] = 0;
    }
    size *= 2;
  }
  pChunk->nCheckedOut--;
  memsys6Link(pChunk, iBlock, iLogsize);
}

/*
** Return the actual size of the block pointed to by p, which is guaranteed
** to have been allocated from chunk pChunk.
*/
static int chunkSize(Mem6Chunk *pChunk, void *p){
  int iSize = 0;
  if( p ){
    int i = ((u8 *)p-pChunk->zPool)/pChunk->nAtom;
    assert( i>=0 && i<pChunk->nBlock );
    iSize = pChunk->nAtom * (1 << (pChunk->aCtrl[i]&CTRL_LOGSIZE));
  }
  return iSize;
}

/*
** Return true if there are currently no outstanding allocations.
*/
static int chunkIsEmpty(Mem6Chunk *pChunk){
  return (pChunk->nCheckedOut==0);
}

/*
** Initialize the buffer zChunk, which is nChunk bytes in size, as
** an Mem6Chunk object. Return a copy of the zChunk pointer.
*/
static Mem6Chunk *chunkInit(u8 *zChunk, int nChunk, int nMinAlloc){
  int ii;
  int iOffset;
  Mem6Chunk *pChunk = (Mem6Chunk *)zChunk;

  assert( nChunk>sizeof(Mem6Chunk) );
  assert( nMinAlloc>sizeof(Mem6Link) );

  memset(pChunk, 0, sizeof(Mem6Chunk));
  pChunk->nAtom = nMinAlloc;
  pChunk->nBlock = ((nChunk-sizeof(Mem6Chunk)) / (pChunk->nAtom+sizeof(u8)));

  pChunk->zPool = (u8 *)&pChunk[1];
  pChunk->aCtrl = &pChunk->zPool[pChunk->nBlock*pChunk->nAtom];

  for(ii=0; ii<=mem6.nLogThreshold; ii++){
    pChunk->aiFreelist[ii] = -1;
  }

  iOffset = 0;
  for(ii=mem6.nLogThreshold; ii>=0; ii--){
    int nAlloc = (1<<ii);
    while( (iOffset+nAlloc)<=pChunk->nBlock ){
      pChunk->aCtrl[iOffset] = ii | CTRL_FREE;
      memsys6Link(pChunk, iOffset, ii);
      iOffset += nAlloc;
    }
  }

  return pChunk;
}


static void mem6Enter(void){
  sqlite3_mutex_enter(mem6.mutex);
}

static void mem6Leave(void){
  sqlite3_mutex_leave(mem6.mutex);
}

/*
** Based on the number and size of the currently allocated chunks, return
** the size of the next chunk to allocate, in bytes.
*/
static int nextChunkSize(void){
  int iTotal = MIN_CHUNKSIZE;
  Mem6Chunk *p;
  for(p=mem6.pChunk; p; p=p->pNext){
    iTotal = iTotal*2;
  }
  return iTotal;
}

static void freeChunk(Mem6Chunk *pChunk){
  Mem6Chunk **pp = &mem6.pChunk;
  for( pp=&mem6.pChunk; *pp!=pChunk; pp = &(*pp)->pNext );
  *pp = (*pp)->pNext;
  free(pChunk);
}

static void *memsys6Malloc(int nByte){
  Mem6Chunk *pChunk;
  void *p = 0;
  int nTotal = nByte+8;
  int iOffset = 0;

  if( nTotal>mem6.nThreshold ){
    p = malloc(nTotal);
  }else{
    int iLogsize = 0;
    if( nTotal>(1<<LOG2_MINALLOC) ){
      iLogsize = roundupLog2(nTotal) - LOG2_MINALLOC;
    }
    mem6Enter();
    for(pChunk=mem6.pChunk; pChunk; pChunk=pChunk->pNext){
      p = chunkMalloc(pChunk, iLogsize);
      if( p ){
        break;
      }
    }
    if( !p ){
      int iSize = nextChunkSize();
      p = malloc(iSize);
      if( p ){
        pChunk = chunkInit((u8 *)p, iSize, mem6.nMinAlloc);
        pChunk->pNext = mem6.pChunk;
        mem6.pChunk = pChunk;
        p = chunkMalloc(pChunk, iLogsize);
        assert(p);
      }
    }
    iOffset = ((u8*)p - (u8*)pChunk);
    mem6Leave();
  }

  if( !p ){
    return 0;
  }
  ((u32 *)p)[0] = iOffset;
  ((u32 *)p)[1] = nByte;
  return &((u32 *)p)[2];
}

static int memsys6Size(void *pPrior){
  if( pPrior==0 ) return 0;
  return ((u32*)pPrior)[-1];
}

static void memsys6Free(void *pPrior){
  int iSlot;
  void *p = &((u32 *)pPrior)[-2];
  iSlot = ((u32 *)p)[0];
  if( iSlot ){
    Mem6Chunk *pChunk;
    mem6Enter();
    pChunk = (Mem6Chunk *)(&((u8 *)p)[-1 * iSlot]);
    chunkFree(pChunk, p);
    if( chunkIsEmpty(pChunk) ){
      freeChunk(pChunk);
    }
    mem6Leave();
  }else{
    free(p);
  }
}

static void *memsys6Realloc(void *p, int nByte){
  void *p2;

  if( p && nByte<=memsys6Size(p) ){
    p2 = p;
  }else{
    p2 = memsys6Malloc(nByte);
    if( p && p2 ){
      memcpy(p2, p, memsys6Size(p));
      memsys6Free(p);
    }
  }

  return p2;
}

static int memsys6Roundup(int n){
  if( n>mem6.nThreshold ){
    return n;
  }else{
    return (1<<roundupLog2(n));
  }
}

static int memsys6Init(void *pCtx){
  u8 bMemstat = sqlite3Config.bMemstat;
  mem6.nMinAlloc = (1 << LOG2_MINALLOC);
  mem6.pChunk = 0;
  mem6.nThreshold = sqlite3Config.nSmall;
  if( mem6.nThreshold<=0 ){
    mem6.nThreshold = SMALL_MALLOC_DEFAULT_THRESHOLD;
  }
  mem6.nLogThreshold = roundupLog2(mem6.nThreshold) - LOG2_MINALLOC;
  if( !bMemstat ){
    mem6.mutex = sqlite3MutexAlloc(SQLITE_MUTEX_STATIC_MEM);
  }
  return SQLITE_OK;
}

static void memsys6Shutdown(void *pCtx){
  memset(&mem6, 0, sizeof(mem6));
}

/*
** This routine is the only routine in this file with external 
** linkage. It returns a pointer to a static sqlite3_mem_methods
** struct populated with the memsys6 methods.
*/
const sqlite3_mem_methods *sqlite3MemGetMemsys6(void){
  static const sqlite3_mem_methods memsys6Methods = {
     memsys6Malloc,
     memsys6Free,
     memsys6Realloc,
     memsys6Size,
     memsys6Roundup,
     memsys6Init,
     memsys6Shutdown,
     0
  };
  return &memsys6Methods;
}

#endif

/*
** 2007 October 14
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
*************************************************************************
** This file contains the C functions that implement a memory
** allocation subsystem for use by SQLite. 
**
** This version of the memory allocation subsystem omits all
** use of malloc(). The SQLite user supplies a block of memory
** before calling sqlite3_initialize() from which allocations
** are made and returned by the xMalloc() and xRealloc() 
** implementations. Once sqlite3_initialize() has been called,
** the amount of memory available to SQLite is fixed and cannot
** be changed.
**
** This version of the memory allocation subsystem is included
** in the build only if SQLITE_ENABLE_MEMSYS5 is defined.
**
** $Id: mem5.c,v 1.11 2008/07/16 12:25:32 drh Exp $
*/
#include "sqliteInt.h"

/*
** This version of the memory allocator is used only when 
** SQLITE_POW2_MEMORY_SIZE is defined.
*/
#ifdef SQLITE_ENABLE_MEMSYS5

/*
** Log2 of the minimum size of an allocation.  For example, if
** 4 then all allocations will be rounded up to at least 16 bytes.
** If 5 then all allocations will be rounded up to at least 32 bytes.
*/
#ifndef SQLITE_POW2_LOGMIN
# define SQLITE_POW2_LOGMIN 6
#endif

/*
** Log2 of the maximum size of an allocation.
*/
#ifndef SQLITE_POW2_LOGMAX
# define SQLITE_POW2_LOGMAX 20
#endif
#define POW2_MAX (((unsigned int)1)<<SQLITE_POW2_LOGMAX)

/*
** Number of distinct allocation sizes.
*/
#define NSIZE (SQLITE_POW2_LOGMAX - SQLITE_POW2_LOGMIN + 1)

/*
** A minimum allocation is an instance of the following structure.
** Larger allocations are an array of these structures where the
** size of the array is a power of 2.
*/
typedef struct Mem5Link Mem5Link;
struct Mem5Link {
  int next;       /* Index of next free chunk */
  int prev;       /* Index of previous free chunk */
};

/*
** Maximum size of any allocation is ((1<<LOGMAX)*mem5.nAtom). Since
** mem5.nAtom is always at least 8, this is not really a practical
** limitation.
*/
#define LOGMAX 30

/*
** Masks used for mem5.aCtrl[] elements.
*/
#define CTRL_LOGSIZE  0x1f    /* Log2 Size of this block relative to POW2_MIN */
#define CTRL_FREE     0x20    /* True if not checked out */

/*
** All of the static variables used by this module are collected
** into a single structure named "mem5".  This is to keep the
** static variables organized and to reduce namespace pollution
** when this module is combined with other in the amalgamation.
*/
static struct {
  /*
  ** The alarm callback and its arguments.  The mem5.mutex lock will
  ** be held while the callback is running.  Recursive calls into
  ** the memory subsystem are allowed, but no new callbacks will be
  ** issued.  The alarmBusy variable is set to prevent recursive
  ** callbacks.
  */
  sqlite3_int64 alarmThreshold;
  void (*alarmCallback)(void*, sqlite3_int64,int);
  void *alarmArg;
  int alarmBusy;
  
  /*
  ** Mutex to control access to the memory allocation subsystem.
  */
  sqlite3_mutex *mutex;

  /*
  ** Performance statistics
  */
  u64 nAlloc;         /* Total number of calls to malloc */
  u64 totalAlloc;     /* Total of all malloc calls - includes internal frag */
  u64 totalExcess;    /* Total internal fragmentation */
  u32 currentOut;     /* Current checkout, including internal fragmentation */
  u32 currentCount;   /* Current number of distinct checkouts */
  u32 maxOut;         /* Maximum instantaneous currentOut */
  u32 maxCount;       /* Maximum instantaneous currentCount */
  u32 maxRequest;     /* Largest allocation (exclusive of internal frag) */
  
  /*
  ** Lists of free blocks of various sizes.
  */
  int aiFreelist[LOGMAX+1];

  /*
  ** Space for tracking which blocks are checked out and the size
  ** of each block.  One byte per block.
  */
  u8 *aCtrl;

  /*
  ** Memory available for allocation
  */
  int nAtom;       /* Smallest possible allocation in bytes */
  int nBlock;      /* Number of nAtom sized blocks in zPool */
  u8 *zPool;
} mem5;

#define MEM5LINK(idx) ((Mem5Link *)(&mem5.zPool[(idx)*mem5.nAtom]))

/*
** Unlink the chunk at mem5.aPool[i] from list it is currently
** on.  It should be found on mem5.aiFreelist[iLogsize].
*/
static void memsys5Unlink(int i, int iLogsize){
  int next, prev;
  assert( i>=0 && i<mem5.nBlock );
  assert( iLogsize>=0 && iLogsize<=LOGMAX );
  assert( (mem5.aCtrl[i] & CTRL_LOGSIZE)==iLogsize );

  next = MEM5LINK(i)->next;
  prev = MEM5LINK(i)->prev;
  if( prev<0 ){
    mem5.aiFreelist[iLogsize] = next;
  }else{
    MEM5LINK(prev)->next = next;
  }
  if( next>=0 ){
    MEM5LINK(next)->prev = prev;
  }
}

/*
** Link the chunk at mem5.aPool[i] so that is on the iLogsize
** free list.
*/
static void memsys5Link(int i, int iLogsize){
  int x;
  assert( sqlite3_mutex_held(mem5.mutex) );
  assert( i>=0 && i<mem5.nBlock );
  assert( iLogsize>=0 && iLogsize<=LOGMAX );
  assert( (mem5.aCtrl[i] & CTRL_LOGSIZE)==iLogsize );

  x = MEM5LINK(i)->next = mem5.aiFreelist[iLogsize];
  MEM5LINK(i)->prev = -1;
  if( x>=0 ){
    assert( x<mem5.nBlock );
    MEM5LINK(x)->prev = i;
  }
  mem5.aiFreelist[iLogsize] = i;
}

/*
** If the STATIC_MEM mutex is not already held, obtain it now. The mutex
** will already be held (obtained by code in malloc.c) if
** sqlite3Config.bMemStat is true.
*/
static void memsys5Enter(void){
  if( sqlite3Config.bMemstat==0 && mem5.mutex==0 ){
    mem5.mutex = sqlite3MutexAlloc(SQLITE_MUTEX_STATIC_MEM);
  }
  sqlite3_mutex_enter(mem5.mutex);
}
static void memsys5Leave(void){
  sqlite3_mutex_leave(mem5.mutex);
}

/*
** Return the size of an outstanding allocation, in bytes.  The
** size returned omits the 8-byte header overhead.  This only
** works for chunks that are currently checked out.
*/
static int memsys5Size(void *p){
  int iSize = 0;
  if( p ){
    int i = ((u8 *)p-mem5.zPool)/mem5.nAtom;
    assert( i>=0 && i<mem5.nBlock );
    iSize = mem5.nAtom * (1 << (mem5.aCtrl[i]&CTRL_LOGSIZE));
  }
  return iSize;
}

/*
** Find the first entry on the freelist iLogsize.  Unlink that
** entry and return its index. 
*/
static int memsys5UnlinkFirst(int iLogsize){
  int i;
  int iFirst;

  assert( iLogsize>=0 && iLogsize<=LOGMAX );
  i = iFirst = mem5.aiFreelist[iLogsize];
  assert( iFirst>=0 );
  while( i>0 ){
    if( i<iFirst ) iFirst = i;
    i = MEM5LINK(i)->next;
  }
  memsys5Unlink(iFirst, iLogsize);
  return iFirst;
}

/*
** Return a block of memory of at least nBytes in size.
** Return NULL if unable.
*/
static void *memsys5MallocUnsafe(int nByte){
  int i;           /* Index of a mem5.aPool[] slot */
  int iBin;        /* Index into mem5.aiFreelist[] */
  int iFullSz;     /* Size of allocation rounded up to power of 2 */
  int iLogsize;    /* Log2 of iFullSz/POW2_MIN */

  /* Keep track of the maximum allocation request.  Even unfulfilled
  ** requests are counted */
  if( nByte>mem5.maxRequest ){
    mem5.maxRequest = nByte;
  }

  /* Round nByte up to the next valid power of two */
  if( nByte>POW2_MAX ) return 0;
  for(iFullSz=mem5.nAtom, iLogsize=0; iFullSz<nByte; iFullSz *= 2, iLogsize++){}

  /* Make sure mem5.aiFreelist[iLogsize] contains at least one free
  ** block.  If not, then split a block of the next larger power of
  ** two in order to create a new free block of size iLogsize.
  */
  for(iBin=iLogsize; mem5.aiFreelist[iBin]<0 && iBin<=LOGMAX; iBin++){}
  if( iBin>LOGMAX ) return 0;
  i = memsys5UnlinkFirst(iBin);
  while( iBin>iLogsize ){
    int newSize;

    iBin--;
    newSize = 1 << iBin;
    mem5.aCtrl[i+newSize] = CTRL_FREE | iBin;
    memsys5Link(i+newSize, iBin);
  }
  mem5.aCtrl[i] = iLogsize;

  /* Update allocator performance statistics. */
  mem5.nAlloc++;
  mem5.totalAlloc += iFullSz;
  mem5.totalExcess += iFullSz - nByte;
  mem5.currentCount++;
  mem5.currentOut += iFullSz;
  if( mem5.maxCount<mem5.currentCount ) mem5.maxCount = mem5.currentCount;
  if( mem5.maxOut<mem5.currentOut ) mem5.maxOut = mem5.currentOut;

  /* Return a pointer to the allocated memory. */
  return (void*)&mem5.zPool[i*mem5.nAtom];
}

/*
** Free an outstanding memory allocation.
*/
static void memsys5FreeUnsafe(void *pOld){
  u32 size, iLogsize;
  int iBlock;             

  /* Set iBlock to the index of the block pointed to by pOld in 
  ** the array of mem5.nAtom byte blocks pointed to by mem5.zPool.
  */
  iBlock = ((u8 *)pOld-mem5.zPool)/mem5.nAtom;

  /* Check that the pointer pOld points to a valid, non-free block. */
  assert( iBlock>=0 && iBlock<mem5.nBlock );
  assert( ((u8 *)pOld-mem5.zPool)%mem5.nAtom==0 );
  assert( (mem5.aCtrl[iBlock] & CTRL_FREE)==0 );

  iLogsize = mem5.aCtrl[iBlock] & CTRL_LOGSIZE;
  size = 1<<iLogsize;
  assert( iBlock+size-1<mem5.nBlock );

  mem5.aCtrl[iBlock] |= CTRL_FREE;
  mem5.aCtrl[iBlock+size-1] |= CTRL_FREE;
  assert( mem5.currentCount>0 );
  assert( mem5.currentOut>=0 );
  mem5.currentCount--;
  mem5.currentOut -= size*mem5.nAtom;
  assert( mem5.currentOut>0 || mem5.currentCount==0 );
  assert( mem5.currentCount>0 || mem5.currentOut==0 );

  mem5.aCtrl[iBlock] = CTRL_FREE | iLogsize;
  while( iLogsize<LOGMAX ){
    int iBuddy;
    if( (iBlock>>iLogsize) & 1 ){
      iBuddy = iBlock - size;
    }else{
      iBuddy = iBlock + size;
    }
    assert( iBuddy>=0 );
    if( (iBuddy+(1<<iLogsize))>mem5.nBlock ) break;
    if( mem5.aCtrl[iBuddy]!=(CTRL_FREE | iLogsize) ) break;
    memsys5Unlink(iBuddy, iLogsize);
    iLogsize++;
    if( iBuddy<iBlock ){
      mem5.aCtrl[iBuddy] = CTRL_FREE | iLogsize;
      mem5.aCtrl[iBlock] = 0;
      iBlock = iBuddy;
    }else{
      mem5.aCtrl[iBlock] = CTRL_FREE | iLogsize;
      mem5.aCtrl[iBuddy] = 0;
    }
    size *= 2;
  }
  memsys5Link(iBlock, iLogsize);
}

/*
** Allocate nBytes of memory
*/
static void *memsys5Malloc(int nBytes){
  sqlite3_int64 *p = 0;
  if( nBytes>0 ){
    memsys5Enter();
    p = memsys5MallocUnsafe(nBytes);
    memsys5Leave();
  }
  return (void*)p; 
}

/*
** Free memory.
*/
static void memsys5Free(void *pPrior){
  if( pPrior==0 ){
assert(0);
    return;
  }
  memsys5Enter();
  memsys5FreeUnsafe(pPrior);
  memsys5Leave();  
}

/*
** Change the size of an existing memory allocation
*/
static void *memsys5Realloc(void *pPrior, int nBytes){
  int nOld;
  void *p;
  if( pPrior==0 ){
    return memsys5Malloc(nBytes);
  }
  if( nBytes<=0 ){
    memsys5Free(pPrior);
    return 0;
  }
  nOld = memsys5Size(pPrior);
  if( nBytes<=nOld ){
    return pPrior;
  }
  memsys5Enter();
  p = memsys5MallocUnsafe(nBytes);
  if( p ){
    memcpy(p, pPrior, nOld);
    memsys5FreeUnsafe(pPrior);
  }
  memsys5Leave();
  return p;
}

/*
** Round up a request size to the next valid allocation size.
*/
static int memsys5Roundup(int n){
  int iFullSz;
  for(iFullSz=mem5.nAtom; iFullSz<n; iFullSz *= 2);
  return iFullSz;
}

static int memsys5Log(int iValue){
  int iLog;
  for(iLog=0; (1<<iLog)<iValue; iLog++);
  return iLog;
}

/*
** Initialize this module.
*/
static int memsys5Init(void *NotUsed){
  int ii;
  int nByte = sqlite3Config.nHeap;
  u8 *zByte = (u8 *)sqlite3Config.pHeap;
  int nMinLog;                 /* Log of minimum allocation size in bytes*/
  int iOffset;

  if( !zByte ){
    return SQLITE_ERROR;
  }

  nMinLog = memsys5Log(sqlite3Config.mnReq);
  mem5.nAtom = (1<<nMinLog);
  while( sizeof(Mem5Link)>mem5.nAtom ){
    mem5.nAtom = mem5.nAtom << 1;
  }

  mem5.nBlock = (nByte / (mem5.nAtom+sizeof(u8)));
  mem5.zPool = zByte;
  mem5.aCtrl = (u8 *)&mem5.zPool[mem5.nBlock*mem5.nAtom];

  for(ii=0; ii<=LOGMAX; ii++){
    mem5.aiFreelist[ii] = -1;
  }

  iOffset = 0;
  for(ii=LOGMAX; ii>=0; ii--){
    int nAlloc = (1<<ii);
    if( (iOffset+nAlloc)<=mem5.nBlock ){
      mem5.aCtrl[iOffset] = ii | CTRL_FREE;
      memsys5Link(iOffset, ii);
      iOffset += nAlloc;
    }
    assert((iOffset+nAlloc)>mem5.nBlock);
  }

  return SQLITE_OK;
}

/*
** Deinitialize this module.
*/
static void memsys5Shutdown(void *NotUsed){
  return;
}

/*
** Open the file indicated and write a log of all unfreed memory 
** allocations into that log.
*/
void sqlite3Memsys5Dump(const char *zFilename){
#ifdef SQLITE_DEBUG
  FILE *out;
  int i, j, n;
  int nMinLog;

  if( zFilename==0 || zFilename[0]==0 ){
    out = stdout;
  }else{
    out = fopen(zFilename, "w");
    if( out==0 ){
      fprintf(stderr, "** Unable to output memory debug output log: %s **\n",
                      zFilename);
      return;
    }
  }
  memsys5Enter();
  nMinLog = memsys5Log(mem5.nAtom);
  for(i=0; i<=LOGMAX && i+nMinLog<32; i++){
    for(n=0, j=mem5.aiFreelist[i]; j>=0; j = MEM5LINK(j)->next, n++){}
    fprintf(out, "freelist items of size %d: %d\n", mem5.nAtom << i, n);
  }
  fprintf(out, "mem5.nAlloc       = %llu\n", mem5.nAlloc);
  fprintf(out, "mem5.totalAlloc   = %llu\n", mem5.totalAlloc);
  fprintf(out, "mem5.totalExcess  = %llu\n", mem5.totalExcess);
  fprintf(out, "mem5.currentOut   = %u\n", mem5.currentOut);
  fprintf(out, "mem5.currentCount = %u\n", mem5.currentCount);
  fprintf(out, "mem5.maxOut       = %u\n", mem5.maxOut);
  fprintf(out, "mem5.maxCount     = %u\n", mem5.maxCount);
  fprintf(out, "mem5.maxRequest   = %u\n", mem5.maxRequest);
  memsys5Leave();
  if( out==stdout ){
    fflush(stdout);
  }else{
    fclose(out);
  }
#endif
}

/*
** This routine is the only routine in this file with external 
** linkage. It returns a pointer to a static sqlite3_mem_methods
** struct populated with the memsys5 methods.
*/
const sqlite3_mem_methods *sqlite3MemGetMemsys5(void){
  static const sqlite3_mem_methods memsys5Methods = {
     memsys5Malloc,
     memsys5Free,
     memsys5Realloc,
     memsys5Size,
     memsys5Roundup,
     memsys5Init,
     memsys5Shutdown,
     0
  };
  return &memsys5Methods;
}

#endif /* SQLITE_ENABLE_MEMSYS5 */

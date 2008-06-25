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
** $Id: mem5.c,v 1.8 2008/06/25 14:57:54 danielk1977 Exp $
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
#define POW2_MIN (1<<SQLITE_POW2_LOGMIN)

/*
** Log2 of the maximum size of an allocation.
*/
#ifndef SQLITE_POW2_LOGMAX
# define SQLITE_POW2_LOGMAX 18
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
typedef struct Mem5Block Mem5Block;
struct Mem5Block {
  union {
    char aData[POW2_MIN];
    struct {
      int next;       /* Index in mem5.aPool[] of next free chunk */
      int prev;       /* Index in mem5.aPool[] of previous free chunk */
    } list;
  } u;
};

/*
** The size in blocks of an POW2_MAX allocation
*/
#define SZ_MAX (1<<(NSIZE-1))

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
  int aiFreelist[NSIZE];

  /*
  ** Space for tracking which blocks are checked out and the size
  ** of each block.  One byte per block.
  */
  u8 *aCtrl;

  /*
  ** Memory available for allocation
  */
  int nBlock;
  Mem5Block *aPool;
} mem5;

/*
** Unlink the chunk at mem5.aPool[i] from list it is currently
** on.  It should be found on mem5.aiFreelist[iLogsize].
*/
static void memsys5Unlink(int i, int iLogsize){
  int next, prev;
  assert( i>=0 && i<mem5.nBlock );
  assert( iLogsize>=0 && iLogsize<NSIZE );
  assert( (mem5.aCtrl[i] & CTRL_LOGSIZE)==iLogsize );

  next = mem5.aPool[i].u.list.next;
  prev = mem5.aPool[i].u.list.prev;
  if( prev<0 ){
    mem5.aiFreelist[iLogsize] = next;
  }else{
    mem5.aPool[prev].u.list.next = next;
  }
  if( next>=0 ){
    mem5.aPool[next].u.list.prev = prev;
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
  assert( iLogsize>=0 && iLogsize<NSIZE );
  assert( (mem5.aCtrl[i] & CTRL_LOGSIZE)==iLogsize );

  mem5.aPool[i].u.list.next = x = mem5.aiFreelist[iLogsize];
  mem5.aPool[i].u.list.prev = -1;
  if( x>=0 ){
    assert( x<mem5.nBlock );
    mem5.aPool[x].u.list.prev = i;
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
    int i = ((Mem5Block*)p) - mem5.aPool;
    assert( i>=0 && i<mem5.nBlock );
    iSize = 1 << ((mem5.aCtrl[i]&CTRL_LOGSIZE) + SQLITE_POW2_LOGMIN);
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

  assert( iLogsize>=0 && iLogsize<NSIZE );
  i = iFirst = mem5.aiFreelist[iLogsize];
  assert( iFirst>=0 );
  while( i>0 ){
    if( i<iFirst ) iFirst = i;
    i = mem5.aPool[i].u.list.next;
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
  for(iFullSz=POW2_MIN, iLogsize=0; iFullSz<nByte; iFullSz *= 2, iLogsize++){}

  /* Make sure mem5.aiFreelist[iLogsize] contains at least one free
  ** block.  If not, then split a block of the next larger power of
  ** two in order to create a new free block of size iLogsize.
  */
  for(iBin=iLogsize; mem5.aiFreelist[iBin]<0 && iBin<NSIZE; iBin++){}
  if( iBin>=NSIZE ) return 0;
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
  return (void*)&mem5.aPool[i];
}

/*
** Free an outstanding memory allocation.
*/
static void memsys5FreeUnsafe(void *pOld){
  u32 size, iLogsize;
  int i;

  i = ((Mem5Block*)pOld) - mem5.aPool;
  assert( i>=0 && i<mem5.nBlock );
  assert( (mem5.aCtrl[i] & CTRL_FREE)==0 );
  iLogsize = mem5.aCtrl[i] & CTRL_LOGSIZE;
  size = 1<<iLogsize;
  assert( i+size-1<mem5.nBlock );
  mem5.aCtrl[i] |= CTRL_FREE;
  mem5.aCtrl[i+size-1] |= CTRL_FREE;
  assert( mem5.currentCount>0 );
  assert( mem5.currentOut>=0 );
  mem5.currentCount--;
  mem5.currentOut -= size*POW2_MIN;
  assert( mem5.currentOut>0 || mem5.currentCount==0 );
  assert( mem5.currentCount>0 || mem5.currentOut==0 );

  mem5.aCtrl[i] = CTRL_FREE | iLogsize;
  while( iLogsize<NSIZE-1 ){
    int iBuddy;

    if( (i>>iLogsize) & 1 ){
      iBuddy = i - size;
    }else{
      iBuddy = i + size;
    }
    assert( iBuddy>=0 && iBuddy<mem5.nBlock );
    if( mem5.aCtrl[iBuddy]!=(CTRL_FREE | iLogsize) ) break;
    memsys5Unlink(iBuddy, iLogsize);
    iLogsize++;
    if( iBuddy<i ){
      mem5.aCtrl[iBuddy] = CTRL_FREE | iLogsize;
      mem5.aCtrl[i] = 0;
      i = iBuddy;
    }else{
      mem5.aCtrl[i] = CTRL_FREE | iLogsize;
      mem5.aCtrl[iBuddy] = 0;
    }
    size *= 2;
  }
  memsys5Link(i, iLogsize);
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
  for(iFullSz=POW2_MIN; iFullSz<n; iFullSz *= 2);
  return iFullSz;
}

/*
** Initialize this module.
*/
static int memsys5Init(void *NotUsed){
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
  for(i=0; i<NSIZE; i++){
    for(n=0, j=mem5.aiFreelist[i]; j>=0; j = mem5.aPool[j].u.list.next, n++){}
    fprintf(out, "freelist items of size %d: %d\n", POW2_MIN << i, n);
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
** linkage.
**
** Populate the low-level memory allocation function pointers in
** sqlite3Config.m with pointers to the routines in this file. The
** arguments specify the block of memory to manage.
**
** This routine is only called by sqlite3_config(), and therefore
** is not required to be threadsafe (it is not).
*/
void sqlite3MemSetMemsys5(u8 *zByte, int nByte){
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
  int i;

  mem5.nBlock = (nByte / (sizeof(Mem5Block)+sizeof(u8)));
  mem5.nBlock -= (mem5.nBlock%SZ_MAX);
  mem5.aPool = (Mem5Block *)zByte;
  mem5.aCtrl = (u8 *)&mem5.aPool[mem5.nBlock];

  assert( sizeof(Mem5Block)==POW2_MIN );
  assert( mem5.nBlock>=SZ_MAX );
  assert( (mem5.nBlock%SZ_MAX)==0 );

  for(i=0; i<NSIZE; i++) mem5.aiFreelist[i] = -1;
  for(i=0; i<=mem5.nBlock-SZ_MAX; i += SZ_MAX){
    mem5.aCtrl[i] = (NSIZE-1) | CTRL_FREE;
    memsys5Link(i, NSIZE-1);
  }

  /* Configure the functions to call to allocate memory. */
  sqlite3_config(SQLITE_CONFIG_MALLOC, &memsys5Methods);
}

#endif /* SQLITE_ENABLE_MEMSYS5 */

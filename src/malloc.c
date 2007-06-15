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
** Memory allocation functions used throughout sqlite.
**
**
** $Id: malloc.c,v 1.3 2007/06/15 20:29:20 drh Exp $
*/
#include "sqliteInt.h"
#include "os.h"
#include <stdarg.h>
#include <ctype.h>

/*
** MALLOC WRAPPER ARCHITECTURE
**
** The sqlite code accesses dynamic memory allocation/deallocation by invoking
** the following six APIs (which may be implemented as macros).
**
**     sqlite3Malloc()
**     sqlite3MallocRaw()
**     sqlite3Realloc()
**     sqlite3ReallocOrFree()
**     sqlite3Free()
**     sqlite3AllocSize()
**
** The function sqlite3FreeX performs the same task as sqlite3Free and is
** guaranteed to be a real function. The same holds for sqlite3MallocX
**
** The above APIs are implemented in terms of the functions provided in the
** operating-system interface. The OS interface is never accessed directly
** by code outside of this file.
**
**     sqlite3OsMalloc()
**     sqlite3OsRealloc()
**     sqlite3OsFree()
**     sqlite3OsAllocationSize()
**
** Functions sqlite3MallocRaw() and sqlite3Realloc() may invoke 
** sqlite3_release_memory() if a call to sqlite3OsMalloc() or
** sqlite3OsRealloc() fails (or if the soft-heap-limit for the thread is
** exceeded). Function sqlite3Malloc() usually invokes
** sqlite3MallocRaw().
**
** MALLOC TEST WRAPPER ARCHITECTURE
**
** The test wrapper provides extra test facilities to ensure the library 
** does not leak memory and handles the failure of the underlying OS level
** allocation system correctly. It is only present if the library is 
** compiled with the SQLITE_MEMDEBUG macro set.
**
**     * Guardposts to detect overwrites.
**     * Ability to cause a specific Malloc() or Realloc() to fail.
**     * Audit outstanding memory allocations (i.e check for leaks).
*/

#define MAX(x,y) ((x)>(y)?(x):(y))

#if defined(SQLITE_ENABLE_MEMORY_MANAGEMENT) && !defined(SQLITE_OMIT_DISKIO)
/*
** Set the soft heap-size limit for the current thread. Passing a negative
** value indicates no limit.
*/
void sqlite3_soft_heap_limit(int n){
  ThreadData *pTd = sqlite3ThreadData();
  if( pTd ){
    pTd->nSoftHeapLimit = n;
  }
  sqlite3ReleaseThreadData();
}

/*
** Release memory held by SQLite instances created by the current thread.
*/
int sqlite3_release_memory(int n){
  return sqlite3PagerReleaseMemory(n);
}
#else
/* If SQLITE_ENABLE_MEMORY_MANAGEMENT is not defined, then define a version
** of sqlite3_release_memory() to be used by other code in this file.
** This is done for no better reason than to reduce the number of 
** pre-processor #ifndef statements.
*/
#define sqlite3_release_memory(x) 0    /* 0 == no memory freed */
#endif

#ifdef SQLITE_MEMDEBUG
/*--------------------------------------------------------------------------
** Begin code for memory allocation system test layer.
**
** Memory debugging is turned on by defining the SQLITE_MEMDEBUG macro.
**
** SQLITE_MEMDEBUG==1    -> Fence-posting only (thread safe) 
** SQLITE_MEMDEBUG==2    -> Fence-posting + linked list of allocations (not ts)
** SQLITE_MEMDEBUG==3    -> Above + backtraces (not thread safe, req. glibc)
*/

/* Figure out whether or not to store backtrace() information for each malloc.
** The backtrace() function is only used if SQLITE_MEMDEBUG is set to 2 or 
** greater and glibc is in use. If we don't want to use backtrace(), then just
** define it as an empty macro and set the amount of space reserved to 0.
*/
#if defined(__GLIBC__) && SQLITE_MEMDEBUG>2
  extern int backtrace(void **, int);
  #define TESTALLOC_STACKSIZE 128
  #define TESTALLOC_STACKFRAMES ((TESTALLOC_STACKSIZE-8)/sizeof(void*))
#else
  #define backtrace(x, y)
  #define TESTALLOC_STACKSIZE 0
  #define TESTALLOC_STACKFRAMES 0
#endif

/*
** Number of 32-bit guard words.  This should probably be a multiple of
** 2 since on 64-bit machines we want the value returned by sqliteMalloc()
** to be 8-byte aligned.
*/
#ifndef TESTALLOC_NGUARD
# define TESTALLOC_NGUARD 2
#endif

/*
** Size reserved for storing file-name along with each malloc()ed blob.
*/
#define TESTALLOC_FILESIZE 64

/*
** Size reserved for storing the user string. Each time a Malloc() or Realloc()
** call succeeds, up to TESTALLOC_USERSIZE bytes of the string pointed to by
** sqlite3_malloc_id are stored along with the other test system metadata.
*/
#define TESTALLOC_USERSIZE 64
const char *sqlite3_malloc_id = 0;

/*
** Blocks used by the test layer have the following format:
**
**        <sizeof(void *) pNext pointer>
**        <sizeof(void *) pPrev pointer>
**        <TESTALLOC_NGUARD 32-bit guard words>
**            <The application level allocation>
**        <TESTALLOC_NGUARD 32-bit guard words>
**        <32-bit line number>
**        <TESTALLOC_FILESIZE bytes containing null-terminated file name>
**        <TESTALLOC_STACKSIZE bytes of backtrace() output>
*/ 

#define TESTALLOC_OFFSET_GUARD1(p)    (sizeof(void *) * 2)
#define TESTALLOC_OFFSET_DATA(p) ( \
  TESTALLOC_OFFSET_GUARD1(p) + sizeof(u32) * TESTALLOC_NGUARD \
)
#define TESTALLOC_OFFSET_GUARD2(p) ( \
  TESTALLOC_OFFSET_DATA(p) + sqlite3OsAllocationSize(p) - TESTALLOC_OVERHEAD \
)
#define TESTALLOC_OFFSET_LINENUMBER(p) ( \
  TESTALLOC_OFFSET_GUARD2(p) + sizeof(u32) * TESTALLOC_NGUARD \
)
#define TESTALLOC_OFFSET_FILENAME(p) ( \
  TESTALLOC_OFFSET_LINENUMBER(p) + sizeof(u32) \
)
#define TESTALLOC_OFFSET_USER(p) ( \
  TESTALLOC_OFFSET_FILENAME(p) + TESTALLOC_FILESIZE \
)
#define TESTALLOC_OFFSET_STACK(p) ( \
  TESTALLOC_OFFSET_USER(p) + TESTALLOC_USERSIZE + 8 - \
  (TESTALLOC_OFFSET_USER(p) % 8) \
)

#define TESTALLOC_OVERHEAD ( \
  sizeof(void *)*2 +                   /* pPrev and pNext pointers */   \
  TESTALLOC_NGUARD*sizeof(u32)*2 +              /* Guard words */       \
  sizeof(u32) + TESTALLOC_FILESIZE +   /* File and line number */       \
  TESTALLOC_USERSIZE +                 /* User string */                \
  TESTALLOC_STACKSIZE                  /* backtrace() stack */          \
)


/*
** For keeping track of the number of mallocs and frees.   This
** is used to check for memory leaks.  The iMallocFail and iMallocReset
** values are used to simulate malloc() failures during testing in 
** order to verify that the library correctly handles an out-of-memory
** condition.
*/
int sqlite3_nMalloc;         /* Number of sqliteMalloc() calls */
int sqlite3_nFree;           /* Number of sqliteFree() calls */
int sqlite3_memUsed;         /* TODO Total memory obtained from malloc */
int sqlite3_memMax;          /* TODO Mem usage high-water mark */
int sqlite3_iMallocFail;     /* Fail sqliteMalloc() after this many calls */
int sqlite3_iMallocReset = -1; /* When iMallocFail reaches 0, set to this */

void *sqlite3_pFirst = 0;         /* Pointer to linked list of allocations */
int sqlite3_nMaxAlloc = 0;        /* High water mark of ThreadData.nAlloc */
int sqlite3_mallocDisallowed = 0; /* assert() in sqlite3Malloc() if set */
int sqlite3_isFail = 0;           /* True if all malloc calls should fail */
const char *sqlite3_zFile = 0;    /* Filename to associate debug info with */
int sqlite3_iLine = 0;            /* Line number for debug info */
int sqlite3_mallocfail_trace = 0; /* Print a msg on malloc fail if true */

/*
** Check for a simulated memory allocation failure.  Return true if
** the failure should be simulated.  Return false to proceed as normal.
*/
int sqlite3TestMallocFail(){
  if( sqlite3_isFail ){
    return 1;
  }
  if( sqlite3_iMallocFail>=0 ){
    sqlite3_iMallocFail--;
    if( sqlite3_iMallocFail==0 ){
      sqlite3_iMallocFail = sqlite3_iMallocReset;
      sqlite3_isFail = 1;
      if( sqlite3_mallocfail_trace ){
         sqlite3DebugPrintf("###_malloc_fails_###\n");
      }
      return 1;
    }
  }
  return 0;
}

/*
** The argument is a pointer returned by sqlite3OsMalloc() or xRealloc().
** assert() that the first and last (TESTALLOC_NGUARD*4) bytes are set to the
** values set by the applyGuards() function.
*/
static void checkGuards(u32 *p)
{
  int i;
  char *zAlloc = (char *)p;
  char *z;

  /* First set of guard words */
  z = &zAlloc[TESTALLOC_OFFSET_GUARD1(p)];
  for(i=0; i<TESTALLOC_NGUARD; i++){
    assert(((u32 *)z)[i]==0xdead1122);
  }

  /* Second set of guard words */
  z = &zAlloc[TESTALLOC_OFFSET_GUARD2(p)];
  for(i=0; i<TESTALLOC_NGUARD; i++){
    u32 guard = 0;
    memcpy(&guard, &z[i*sizeof(u32)], sizeof(u32));
    assert(guard==0xdead3344);
  }
}

/*
** The argument is a pointer returned by sqlite3OsMalloc() or Realloc(). The
** first and last (TESTALLOC_NGUARD*4) bytes are set to known values for use as 
** guard-posts.
*/
static void applyGuards(u32 *p)
{
  int i;
  char *z;
  char *zAlloc = (char *)p;

  /* First set of guard words */
  z = &zAlloc[TESTALLOC_OFFSET_GUARD1(p)];
  for(i=0; i<TESTALLOC_NGUARD; i++){
    ((u32 *)z)[i] = 0xdead1122;
  }

  /* Second set of guard words */
  z = &zAlloc[TESTALLOC_OFFSET_GUARD2(p)];
  for(i=0; i<TESTALLOC_NGUARD; i++){
    static const int guard = 0xdead3344;
    memcpy(&z[i*sizeof(u32)], &guard, sizeof(u32));
  }

  /* Line number */
  z = &((char *)z)[TESTALLOC_NGUARD*sizeof(u32)];             /* Guard words */
  z = &zAlloc[TESTALLOC_OFFSET_LINENUMBER(p)];
  memcpy(z, &sqlite3_iLine, sizeof(u32));

  /* File name */
  z = &zAlloc[TESTALLOC_OFFSET_FILENAME(p)];
  strncpy(z, sqlite3_zFile, TESTALLOC_FILESIZE);
  z[TESTALLOC_FILESIZE - 1] = '\0';

  /* User string */
  z = &zAlloc[TESTALLOC_OFFSET_USER(p)];
  z[0] = 0;
  if( sqlite3_malloc_id ){
    strncpy(z, sqlite3_malloc_id, TESTALLOC_USERSIZE);
    z[TESTALLOC_USERSIZE-1] = 0;
  }

  /* backtrace() stack */
  z = &zAlloc[TESTALLOC_OFFSET_STACK(p)];
  backtrace((void **)z, TESTALLOC_STACKFRAMES);

  /* Sanity check to make sure checkGuards() is working */
  checkGuards(p);
}

/*
** The argument is a malloc()ed pointer as returned by the test-wrapper.
** Return a pointer to the Os level allocation.
*/
static void *getOsPointer(void *p)
{
  char *z = (char *)p;
  return (void *)(&z[-1 * TESTALLOC_OFFSET_DATA(p)]);
}


#if SQLITE_MEMDEBUG>1
/*
** The argument points to an Os level allocation. Link it into the threads list
** of allocations.
*/
static void linkAlloc(void *p){
  void **pp = (void **)p;
  pp[0] = 0;
  pp[1] = sqlite3_pFirst;
  if( sqlite3_pFirst ){
    ((void **)sqlite3_pFirst)[0] = p;
  }
  sqlite3_pFirst = p;
}

/*
** The argument points to an Os level allocation. Unlinke it from the threads
** list of allocations.
*/
static void unlinkAlloc(void *p)
{
  void **pp = (void **)p;
  if( p==sqlite3_pFirst ){
    assert(!pp[0]);
    assert(!pp[1] || ((void **)(pp[1]))[0]==p);
    sqlite3_pFirst = pp[1];
    if( sqlite3_pFirst ){
      ((void **)sqlite3_pFirst)[0] = 0;
    }
  }else{
    void **pprev = pp[0];
    void **pnext = pp[1];
    assert(pprev);
    assert(pprev[1]==p);
    pprev[1] = (void *)pnext;
    if( pnext ){
      assert(pnext[0]==p);
      pnext[0] = (void *)pprev;
    }
  }
}

/*
** Pointer p is a pointer to an OS level allocation that has just been
** realloc()ed. Set the list pointers that point to this entry to it's new
** location.
*/
static void relinkAlloc(void *p)
{
  void **pp = (void **)p;
  if( pp[0] ){
    ((void **)(pp[0]))[1] = p;
  }else{
    sqlite3_pFirst = p;
  }
  if( pp[1] ){
    ((void **)(pp[1]))[0] = p;
  }
}
#else
#define linkAlloc(x)
#define relinkAlloc(x)
#define unlinkAlloc(x)
#endif

/*
** This function sets the result of the Tcl interpreter passed as an argument
** to a list containing an entry for each currently outstanding call made to 
** sqliteMalloc and friends by the current thread. Each list entry is itself a
** list, consisting of the following (in order):
**
**     * The number of bytes allocated
**     * The __FILE__ macro at the time of the sqliteMalloc() call.
**     * The __LINE__ macro ...
**     * The value of the sqlite3_malloc_id variable ...
**     * The output of backtrace() (if available) ...
**
** Todo: We could have a version of this function that outputs to stdout, 
** to debug memory leaks when Tcl is not available.
*/
#if defined(TCLSH) && defined(SQLITE_DEBUG) && SQLITE_MEMDEBUG>1
#include <tcl.h>
int sqlite3OutstandingMallocs(Tcl_Interp *interp){
  void *p;
  Tcl_Obj *pRes = Tcl_NewObj();
  Tcl_IncrRefCount(pRes);


  for(p=sqlite3_pFirst; p; p=((void **)p)[1]){
    Tcl_Obj *pEntry = Tcl_NewObj();
    Tcl_Obj *pStack = Tcl_NewObj();
    char *z;
    u32 iLine;
    int nBytes = sqlite3OsAllocationSize(p) - TESTALLOC_OVERHEAD;
    char *zAlloc = (char *)p;
    int i;

    Tcl_ListObjAppendElement(0, pEntry, Tcl_NewIntObj(nBytes));

    z = &zAlloc[TESTALLOC_OFFSET_FILENAME(p)];
    Tcl_ListObjAppendElement(0, pEntry, Tcl_NewStringObj(z, -1));

    z = &zAlloc[TESTALLOC_OFFSET_LINENUMBER(p)];
    memcpy(&iLine, z, sizeof(u32));
    Tcl_ListObjAppendElement(0, pEntry, Tcl_NewIntObj(iLine));

    z = &zAlloc[TESTALLOC_OFFSET_USER(p)];
    Tcl_ListObjAppendElement(0, pEntry, Tcl_NewStringObj(z, -1));

    z = &zAlloc[TESTALLOC_OFFSET_STACK(p)];
    for(i=0; i<TESTALLOC_STACKFRAMES; i++){
      char zHex[128];
      sqlite3_snprintf(sizeof(zHex), zHex, "%p", ((void **)z)[i]);
      Tcl_ListObjAppendElement(0, pStack, Tcl_NewStringObj(zHex, -1));
    }

    Tcl_ListObjAppendElement(0, pEntry, pStack);
    Tcl_ListObjAppendElement(0, pRes, pEntry);
  }

  Tcl_ResetResult(interp);
  Tcl_SetObjResult(interp, pRes);
  Tcl_DecrRefCount(pRes);
  return TCL_OK;
}
#endif

/*
** This is the test layer's wrapper around sqlite3OsMalloc().
*/
static void * OSMALLOC(int n){
  sqlite3OsEnterMutex();
#ifdef SQLITE_ENABLE_MEMORY_MANAGEMENT
  sqlite3_nMaxAlloc = 
      MAX(sqlite3_nMaxAlloc, sqlite3ThreadDataReadOnly()->nAlloc);
#endif
  assert( !sqlite3_mallocDisallowed );
  if( !sqlite3TestMallocFail() ){
    u32 *p;
    p = (u32 *)sqlite3OsMalloc(n + TESTALLOC_OVERHEAD);
    assert(p);
    sqlite3_nMalloc++;
    applyGuards(p);
    linkAlloc(p);
    sqlite3OsLeaveMutex();
    return (void *)(&p[TESTALLOC_NGUARD + 2*sizeof(void *)/sizeof(u32)]);
  }
  sqlite3OsLeaveMutex();
  return 0;
}

static int OSSIZEOF(void *p){
  if( p ){
    u32 *pOs = (u32 *)getOsPointer(p);
    return sqlite3OsAllocationSize(pOs) - TESTALLOC_OVERHEAD;
  }
  return 0;
}

/*
** This is the test layer's wrapper around sqlite3OsFree(). The argument is a
** pointer to the space allocated for the application to use.
*/
static void OSFREE(void *pFree){
  u32 *p;         /* Pointer to the OS-layer allocation */
  sqlite3OsEnterMutex();
  p = (u32 *)getOsPointer(pFree);
  checkGuards(p);
  unlinkAlloc(p);
  memset(pFree, 0x55, OSSIZEOF(pFree));
  sqlite3OsFree(p);
  sqlite3_nFree++;
  sqlite3OsLeaveMutex();
}

/*
** This is the test layer's wrapper around sqlite3OsRealloc().
*/
static void * OSREALLOC(void *pRealloc, int n){
#ifdef SQLITE_ENABLE_MEMORY_MANAGEMENT
  sqlite3_nMaxAlloc = 
      MAX(sqlite3_nMaxAlloc, sqlite3ThreadDataReadOnly()->nAlloc);
#endif
  assert( !sqlite3_mallocDisallowed );
  if( !sqlite3TestMallocFail() ){
    u32 *p = (u32 *)getOsPointer(pRealloc);
    checkGuards(p);
    p = sqlite3OsRealloc(p, n + TESTALLOC_OVERHEAD);
    applyGuards(p);
    relinkAlloc(p);
    return (void *)(&p[TESTALLOC_NGUARD + 2*sizeof(void *)/sizeof(u32)]);
  }
  return 0;
}

static void OSMALLOC_FAILED(){
  sqlite3_isFail = 0;
}

#else
/* Define macros to call the sqlite3OsXXX interface directly if 
** the SQLITE_MEMDEBUG macro is not defined.
*/
#define OSMALLOC(x)        sqlite3OsMalloc(x)
#define OSREALLOC(x,y)     sqlite3OsRealloc(x,y)
#define OSFREE(x)          sqlite3OsFree(x)
#define OSSIZEOF(x)        sqlite3OsAllocationSize(x)
#define OSMALLOC_FAILED()

#endif  /* SQLITE_MEMDEBUG */
/*
** End code for memory allocation system test layer.
**--------------------------------------------------------------------------*/

/*
** This routine is called when we are about to allocate n additional bytes
** of memory.  If the new allocation will put is over the soft allocation
** limit, then invoke sqlite3_release_memory() to try to release some
** memory before continuing with the allocation.
**
** This routine also makes sure that the thread-specific-data (TSD) has
** be allocated.  If it has not and can not be allocated, then return
** false.  The updateMemoryUsedCount() routine below will deallocate
** the TSD if it ought to be.
**
** If SQLITE_ENABLE_MEMORY_MANAGEMENT is not defined, this routine is
** a no-op
*/ 
#ifdef SQLITE_ENABLE_MEMORY_MANAGEMENT
static int enforceSoftLimit(int n){
  ThreadData *pTsd = sqlite3ThreadData();
  if( pTsd==0 ){
    return 0;
  }
  assert( pTsd->nAlloc>=0 );
  if( n>0 && pTsd->nSoftHeapLimit>0 ){
    while( pTsd->nAlloc+n>pTsd->nSoftHeapLimit && sqlite3_release_memory(n) ){}
  }
  return 1;
}
#else
# define enforceSoftLimit(X)  1
#endif

/*
** Update the count of total outstanding memory that is held in
** thread-specific-data (TSD).  If after this update the TSD is
** no longer being used, then deallocate it.
**
** If SQLITE_ENABLE_MEMORY_MANAGEMENT is not defined, this routine is
** a no-op
*/
#ifdef SQLITE_ENABLE_MEMORY_MANAGEMENT
static void updateMemoryUsedCount(int n){
  ThreadData *pTsd = sqlite3ThreadData();
  if( pTsd ){
    pTsd->nAlloc += n;
    assert( pTsd->nAlloc>=0 );
    if( pTsd->nAlloc==0 && pTsd->nSoftHeapLimit==0 ){
      sqlite3ReleaseThreadData();
    }
  }
}
#else
#define updateMemoryUsedCount(x)  /* no-op */
#endif

/*
** Allocate and return N bytes of uninitialised memory by calling
** sqlite3OsMalloc(). If the Malloc() call fails, attempt to free memory 
** by calling sqlite3_release_memory().
*/
void *sqlite3MallocRaw(int n, int doMemManage){
  void *p = 0;
  if( n>0 && !sqlite3MallocFailed() && (!doMemManage || enforceSoftLimit(n)) ){
    while( (p = OSMALLOC(n))==0 && sqlite3_release_memory(n) ){}
    if( !p ){
      sqlite3FailedMalloc();
      OSMALLOC_FAILED();
    }else if( doMemManage ){
      updateMemoryUsedCount(OSSIZEOF(p));
    }
  }
  return p;
}

/*
** Resize the allocation at p to n bytes by calling sqlite3OsRealloc(). The
** pointer to the new allocation is returned.  If the Realloc() call fails,
** attempt to free memory by calling sqlite3_release_memory().
*/
void *sqlite3Realloc(void *p, int n){
  if( sqlite3MallocFailed() ){
    return 0;
  }

  if( !p ){
    return sqlite3Malloc(n, 1);
  }else{
    void *np = 0;
#ifdef SQLITE_ENABLE_MEMORY_MANAGEMENT
    int origSize = OSSIZEOF(p);
#endif
    if( enforceSoftLimit(n - origSize) ){
      while( (np = OSREALLOC(p, n))==0 && sqlite3_release_memory(n) ){}
      if( !np ){
        sqlite3FailedMalloc();
        OSMALLOC_FAILED();
      }else{
        updateMemoryUsedCount(OSSIZEOF(np) - origSize);
      }
    }
    return np;
  }
}

/*
** Free the memory pointed to by p. p must be either a NULL pointer or a 
** value returned by a previous call to sqlite3Malloc() or sqlite3Realloc().
*/
void sqlite3FreeX(void *p){
  if( p ){
    updateMemoryUsedCount(0 - OSSIZEOF(p));
    OSFREE(p);
  }
}

/*
** A version of sqliteMalloc() that is always a function, not a macro.
** Currently, this is used only to alloc to allocate the parser engine.
*/
void *sqlite3MallocX(int n){
  return sqliteMalloc(n);
}

/*
** sqlite3Malloc
** sqlite3ReallocOrFree
**
** These two are implemented as wrappers around sqlite3MallocRaw(), 
** sqlite3Realloc() and sqlite3Free().
*/ 
void *sqlite3Malloc(int n, int doMemManage){
  void *p = sqlite3MallocRaw(n, doMemManage);
  if( p ){
    memset(p, 0, n);
  }
  return p;
}
void *sqlite3ReallocOrFree(void *p, int n){
  void *pNew;
  pNew = sqlite3Realloc(p, n);
  if( !pNew ){
    sqlite3FreeX(p);
  }
  return pNew;
}

/*
** sqlite3ThreadSafeMalloc() and sqlite3ThreadSafeFree() are used in those
** rare scenarios where sqlite may allocate memory in one thread and free
** it in another. They are exactly the same as sqlite3Malloc() and 
** sqlite3Free() except that:
**
**   * The allocated memory is not included in any calculations with 
**     respect to the soft-heap-limit, and
**
**   * sqlite3ThreadSafeMalloc() must be matched with ThreadSafeFree(),
**     not sqlite3Free(). Calling sqlite3Free() on memory obtained from
**     ThreadSafeMalloc() will cause an error somewhere down the line.
*/
#ifdef SQLITE_ENABLE_MEMORY_MANAGEMENT
void *sqlite3ThreadSafeMalloc(int n){
  (void)ENTER_MALLOC;
  return sqlite3Malloc(n, 0);
}
void sqlite3ThreadSafeFree(void *p){
  (void)ENTER_MALLOC;
  if( p ){
    OSFREE(p);
  }
}
#endif


/*
** Return the number of bytes allocated at location p. p must be either 
** a NULL pointer (in which case 0 is returned) or a pointer returned by 
** sqlite3Malloc(), sqlite3Realloc() or sqlite3ReallocOrFree().
**
** The number of bytes allocated does not include any overhead inserted by 
** any malloc() wrapper functions that may be called. So the value returned
** is the number of bytes that were available to SQLite using pointer p, 
** regardless of how much memory was actually allocated.
*/
#ifdef SQLITE_ENABLE_MEMORY_MANAGEMENT
int sqlite3AllocSize(void *p){
  return OSSIZEOF(p);
}
#endif

/*
** Make a copy of a string in memory obtained from sqliteMalloc(). These 
** functions call sqlite3MallocRaw() directly instead of sqliteMalloc(). This
** is because when memory debugging is turned on, these two functions are 
** called via macros that record the current file and line number in the
** ThreadData structure.
*/
char *sqlite3StrDup(const char *z){
  char *zNew;
  int n;
  if( z==0 ) return 0;
  n = strlen(z)+1;
  zNew = sqlite3MallocRaw(n, 1);
  if( zNew ) memcpy(zNew, z, n);
  return zNew;
}
char *sqlite3StrNDup(const char *z, int n){
  char *zNew;
  if( z==0 ) return 0;
  zNew = sqlite3MallocRaw(n+1, 1);
  if( zNew ){
    memcpy(zNew, z, n);
    zNew[n] = 0;
  }
  return zNew;
}

/*
** Create a string from the 2nd and subsequent arguments (up to the
** first NULL argument), store the string in memory obtained from
** sqliteMalloc() and make the pointer indicated by the 1st argument
** point to that string.  The 1st argument must either be NULL or 
** point to memory obtained from sqliteMalloc().
*/
void sqlite3SetString(char **pz, ...){
  va_list ap;
  int nByte;
  const char *z;
  char *zResult;

  assert( pz!=0 );
  nByte = 1;
  va_start(ap, pz);
  while( (z = va_arg(ap, const char*))!=0 ){
    nByte += strlen(z);
  }
  va_end(ap);
  sqliteFree(*pz);
  *pz = zResult = sqliteMallocRaw( nByte );
  if( zResult==0 ){
    return;
  }
  *zResult = 0;
  va_start(ap, pz);
  while( (z = va_arg(ap, const char*))!=0 ){
    int n = strlen(z);
    memcpy(zResult, z, n);
    zResult += n;
  }
  zResult[0] = 0;
  va_end(ap);
}


/*
** This function must be called before exiting any API function (i.e. 
** returning control to the user) that has called sqlite3Malloc or
** sqlite3Realloc.
**
** The returned value is normally a copy of the second argument to this
** function. However, if a malloc() failure has occured since the previous
** invocation SQLITE_NOMEM is returned instead. 
**
** If the first argument, db, is not NULL and a malloc() error has occured,
** then the connection error-code (the value returned by sqlite3_errcode())
** is set to SQLITE_NOMEM.
*/
int sqlite3_mallocHasFailed = 0;
int sqlite3ApiExit(sqlite3* db, int rc){
  if( sqlite3MallocFailed() ){
    sqlite3_mallocHasFailed = 0;
    sqlite3OsLeaveMutex();
    sqlite3Error(db, SQLITE_NOMEM, 0);
    rc = SQLITE_NOMEM;
  }
  return rc & (db ? db->errMask : 0xff);
}

/* 
** Set the "malloc has failed" condition to true for this thread.
*/
void sqlite3FailedMalloc(){
  if( !sqlite3MallocFailed() ){
    sqlite3OsEnterMutex();
    assert( sqlite3_mallocHasFailed==0 );
    sqlite3_mallocHasFailed = 1;
  }
}

#ifdef SQLITE_MEMDEBUG
/*
** This function sets a flag in the thread-specific-data structure that will
** cause an assert to fail if sqliteMalloc() or sqliteRealloc() is called.
*/
void sqlite3MallocDisallow(){
  assert( sqlite3_mallocDisallowed>=0 );
  sqlite3_mallocDisallowed++;
}

/*
** This function clears the flag set in the thread-specific-data structure set
** by sqlite3MallocDisallow().
*/
void sqlite3MallocAllow(){
  assert( sqlite3_mallocDisallowed>0 );
  sqlite3_mallocDisallowed--;
}
#endif

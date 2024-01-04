/*
** Name:        mem_secure.c
** Purpose:     Memory manager for SQLite3 Multiple Ciphers
** Author:      Ulrich Telle
** Created:     2023-09-17
** Copyright:   (c) 2023 Ulrich Telle
** License:     MIT
*/

/* For memset, memset_s */
#include <string.h>

#ifdef _WIN32
/* For SecureZeroMemory */
#include <windows.h>
#include <winbase.h>
#endif

SQLITE_PRIVATE void sqlite3mcSecureZeroMemory(void* v, size_t n)
{
#ifdef _WIN32
  SecureZeroMemory(v, n);
#elif defined(__DARWIN__) || defined(__STDC_LIB_EXT1__)
  /* memset_s() is available since OS X 10.9, */
  /* and may be available on other platforms. */
  memset_s(v, n, 0, n);
#elif defined(__OpenBSD__) || (defined(__FreeBSD__) && __FreeBSD__ >= 11)
  /* Non-standard function */
  explicit_bzero(v, n);
#else
  /* Generic implementation based on volatile pointers */
  static void* (* const volatile memset_sec)(void*, int, size_t) = &memset;
  memset_sec(v, 0, n);
#endif
}

#if SQLITE3MC_SECURE_MEMORY

/* Flag indicating whether securing memory allocations is initialized */
static volatile int mcSecureMemoryInitialized = 0;
/* Flag indicating whether memory allocations will be secured */
static volatile int mcSecureMemoryFlag = 0;

/* Map of default memory allocation methods */
static volatile sqlite3_mem_methods mcDefaultMemoryMethods;

#if SQLITE3MC_ENABLE_RANDOM_FILL_MEMORY

/*
** Fill a buffer with pseudo-random bytes.  This is used to preset
** the content of a new memory allocation to unpredictable values and
** to clear the content of a freed allocation to unpredictable values.
*/
static void mcRandomFill(char* pBuf, int nByte)
{
  unsigned int x, y, r;
  x = SQLITE_PTR_TO_INT(pBuf);
  y = nByte | 1;
  while( nByte >= 4 )
  {
    x = (x>>1) ^ (-(int)(x&1) & 0xd0000001);
    y = y*1103515245 + 12345;
    r = x ^ y;
    *(int*)pBuf = r;
    pBuf += 4;
    nByte -= 4;
  }
  while( nByte-- > 0 )
  {
    x = (x>>1) ^ (-(int)(x&1) & 0xd0000001);
    y = y*1103515245 + 12345;
    r = x ^ y;
    *(pBuf++) = r & 0xff;
  }
}

#endif

/*
** Return the size of an allocation
*/
static int mcMemorySize(void* pBuf)
{
  return mcDefaultMemoryMethods.xSize(pBuf);
}

/*
** Memory allocation function
*/
static void* mcMemoryAlloc(int nByte)
{
  return mcDefaultMemoryMethods.xMalloc(nByte);
}

/*
** Free a prior allocation
*/
static void mcMemoryFree(void* pPrior)
{
  if (mcSecureMemoryFlag)
  {
#if SQLITE3MC_USE_RANDOM_FILL_MEMORY
    int nSize = mcMemorySize(pPrior);
    mcRandomFill((char*) pPrior, nSize)
#else
    int nSize = mcMemorySize(pPrior);
    sqlite3mcSecureZeroMemory(pPrior, 0, nSize);
#endif
  }
  mcDefaultMemoryMethods.xFree(pPrior);
}

/*
** Resize an allocation
*/
static void* mcMemoryRealloc(void* pPrior, int nByte)
{
  void* pNew = NULL;
  if (mcSecureMemoryFlag)
  {
    int nPriorSize = mcMemorySize(pPrior);
    if (nByte == 0)
    {
      /* New size = 0, just free prior memory */
      mcMemoryFree(pPrior);
      return NULL;
    }
    else if (!pPrior)
    {
      /* Prior size = 0, just allocate new memory */
      return mcMemoryAlloc(nByte);
    }
    else if(nByte <= nPriorSize)
    {
      /* New size less or equal prior size, do nothing - we do not shrink allocations */
      return pPrior;
    }
    else
    {
      /* New size greater than prior size, reallocate memory */
      pNew = mcMemoryAlloc(nByte);
      if (pNew)
      {
        memcpy(pNew, pPrior, nPriorSize);
        mcMemoryFree(pPrior);
      }
      return pNew;
    }
  }
  else
  {
    return mcDefaultMemoryMethods.xRealloc(pPrior, nByte);
  }
}

/*
** Round up request size to allocation size
*/
static int mcMemoryRoundup(int nByte)
{
  return mcDefaultMemoryMethods.xRoundup(nByte);
}

/*
** Initialize the memory allocator
*/
static int mcMemoryInit(void* pAppData)
{
  return mcDefaultMemoryMethods.xInit(pAppData);
}

/*
** Deinitialize the memory allocator
*/
static void mcMemoryShutdown(void* pAppData)
{
  mcDefaultMemoryMethods.xShutdown(pAppData);
}

static sqlite3_mem_methods mcSecureMemoryMethods =
{
  mcMemoryAlloc,
  mcMemoryFree,
  mcMemoryRealloc,
  mcMemorySize,
  mcMemoryRoundup,
  mcMemoryInit,
  mcMemoryShutdown,
  0
};

SQLITE_PRIVATE void sqlite3mcSetMemorySecurity(int value)
{
  /* memory security can be changed only, if locking is not enabled */
  if (mcSecureMemoryFlag < 2)
  {
    mcSecureMemoryFlag = (value >= 0 && value <= 2) ? value : 0;
  }
}

SQLITE_PRIVATE int sqlite3mcGetMemorySecurity()
{
  return mcSecureMemoryFlag;
}

#endif /* SQLITE3MC_SECURE_MEMORY */

SQLITE_PRIVATE void sqlite3mcInitMemoryMethods()
{
#if SQLITE3MC_SECURE_MEMORY
  if (!mcSecureMemoryInitialized)
  {
    if (sqlite3_config(SQLITE_CONFIG_GETMALLOC, &mcDefaultMemoryMethods) != SQLITE_OK ||
      sqlite3_config(SQLITE_CONFIG_MALLOC, &mcSecureMemoryMethods) != SQLITE_OK)
    {
      mcSecureMemoryFlag = mcSecureMemoryInitialized = 0;
    }
    else
    {
      mcSecureMemoryInitialized = 1;
    }
  }
#endif /* SQLITE3MC_SECURE_MEMORY */
}


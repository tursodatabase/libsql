/*
** 2004 May 22
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
******************************************************************************
**
** This file contains code that is specific to windows.
*/
#include "sqliteInt.h"
#if SQLITE_OS_WIN               /* This file is used for windows only */


/*
** A Note About Memory Allocation:
**
** This driver uses malloc()/free() directly rather than going through
** the SQLite-wrappers sqlite3_malloc()/sqlite3_free().  Those wrappers
** are designed for use on embedded systems where memory is scarce and
** malloc failures happen frequently.  Win32 does not typically run on
** embedded systems, and when it does the developers normally have bigger
** problems to worry about than running out of memory.  So there is not
** a compelling need to use the wrappers.
**
** But there is a good reason to not use the wrappers.  If we use the
** wrappers then we will get simulated malloc() failures within this
** driver.  And that causes all kinds of problems for our tests.  We
** could enhance SQLite to deal with simulated malloc failures within
** the OS driver, but the code to deal with those failure would not
** be exercised on Linux (which does not need to malloc() in the driver)
** and so we would have difficulty writing coverage tests for that
** code.  Better to leave the code out, we think.
**
** The point of this discussion is as follows:  When creating a new
** OS layer for an embedded system, if you use this file as an example,
** avoid the use of malloc()/free().  Those routines work ok on windows
** desktops but not so well in embedded systems.
*/

#include <winbase.h>

#ifdef __CYGWIN__
# include <sys/cygwin.h>
#endif

/*
** Macros used to determine whether or not to use threads.
*/
#if defined(THREADSAFE) && THREADSAFE
# define SQLITE_W32_THREADS 1
#endif

/*
** Include code that is common to all os_*.c files
*/
#include "os_common.h"

/*
** Some microsoft compilers lack this definition.
*/
#ifndef INVALID_FILE_ATTRIBUTES
# define INVALID_FILE_ATTRIBUTES ((DWORD)-1) 
#endif

/*
** Determine if we are dealing with WindowsCE - which has a much
** reduced API.
*/
#if SQLITE_OS_WINCE
# define AreFileApisANSI() 1
# define FormatMessageW(a,b,c,d,e,f,g) 0
#endif

/* Forward references */
typedef struct winShm winShm;           /* A connection to shared-memory */
typedef struct winShmNode winShmNode;   /* A region of shared-memory */

/*
** WinCE lacks native support for file locking so we have to fake it
** with some code of our own.
*/
#if SQLITE_OS_WINCE
typedef struct winceLock {
  int nReaders;       /* Number of reader locks obtained */
  BOOL bPending;      /* Indicates a pending lock has been obtained */
  BOOL bReserved;     /* Indicates a reserved lock has been obtained */
  BOOL bExclusive;    /* Indicates an exclusive lock has been obtained */
} winceLock;
#endif

/*
** The winFile structure is a subclass of sqlite3_file* specific to the win32
** portability layer.
*/
typedef struct winFile winFile;
struct winFile {
  const sqlite3_io_methods *pMethod; /*** Must be first ***/
  sqlite3_vfs *pVfs;      /* The VFS used to open this file */
  HANDLE h;               /* Handle for accessing the file */
  unsigned char locktype; /* Type of lock currently held on this file */
  short sharedLockByte;   /* Randomly chosen byte used as a shared lock */
  DWORD lastErrno;        /* The Windows errno from the last I/O error */
  DWORD sectorSize;       /* Sector size of the device file is on */
  winShm *pShm;           /* Instance of shared memory on this file */
  const char *zPath;      /* Full pathname of this file */
#if SQLITE_OS_WINCE
  WCHAR *zDeleteOnClose;  /* Name of file to delete when closing */
  HANDLE hMutex;          /* Mutex used to control access to shared lock */  
  HANDLE hShared;         /* Shared memory segment used for locking */
  winceLock local;        /* Locks obtained by this instance of winFile */
  winceLock *shared;      /* Global shared lock memory for the file  */
#endif
};

/*
** Forward prototypes.
*/
static int getSectorSize(
    sqlite3_vfs *pVfs,
    const char *zRelative     /* UTF-8 file name */
);

/*
** The following variable is (normally) set once and never changes
** thereafter.  It records whether the operating system is Win95
** or WinNT.
**
** 0:   Operating system unknown.
** 1:   Operating system is Win95.
** 2:   Operating system is WinNT.
**
** In order to facilitate testing on a WinNT system, the test fixture
** can manually set this value to 1 to emulate Win98 behavior.
*/
#ifdef SQLITE_TEST
int sqlite3_os_type = 0;
#else
static int sqlite3_os_type = 0;
#endif

/*
** Return true (non-zero) if we are running under WinNT, Win2K, WinXP,
** or WinCE.  Return false (zero) for Win95, Win98, or WinME.
**
** Here is an interesting observation:  Win95, Win98, and WinME lack
** the LockFileEx() API.  But we can still statically link against that
** API as long as we don't call it when running Win95/98/ME.  A call to
** this routine is used to determine if the host is Win95/98/ME or
** WinNT/2K/XP so that we will know whether or not we can safely call
** the LockFileEx() API.
*/
#if SQLITE_OS_WINCE
# define isNT()  (1)
#else
  static int isNT(void){
    if( sqlite3_os_type==0 ){
      OSVERSIONINFO sInfo;
      sInfo.dwOSVersionInfoSize = sizeof(sInfo);
      GetVersionEx(&sInfo);
      sqlite3_os_type = sInfo.dwPlatformId==VER_PLATFORM_WIN32_NT ? 2 : 1;
    }
    return sqlite3_os_type==2;
  }
#endif /* SQLITE_OS_WINCE */

/*
** Convert a UTF-8 string to microsoft unicode (UTF-16?). 
**
** Space to hold the returned string is obtained from malloc.
*/
static WCHAR *utf8ToUnicode(const char *zFilename){
  int nChar;
  WCHAR *zWideFilename;

  nChar = MultiByteToWideChar(CP_UTF8, 0, zFilename, -1, NULL, 0);
  zWideFilename = malloc( nChar*sizeof(zWideFilename[0]) );
  if( zWideFilename==0 ){
    return 0;
  }
  nChar = MultiByteToWideChar(CP_UTF8, 0, zFilename, -1, zWideFilename, nChar);
  if( nChar==0 ){
    free(zWideFilename);
    zWideFilename = 0;
  }
  return zWideFilename;
}

/*
** Convert microsoft unicode to UTF-8.  Space to hold the returned string is
** obtained from malloc().
*/
static char *unicodeToUtf8(const WCHAR *zWideFilename){
  int nByte;
  char *zFilename;

  nByte = WideCharToMultiByte(CP_UTF8, 0, zWideFilename, -1, 0, 0, 0, 0);
  zFilename = malloc( nByte );
  if( zFilename==0 ){
    return 0;
  }
  nByte = WideCharToMultiByte(CP_UTF8, 0, zWideFilename, -1, zFilename, nByte,
                              0, 0);
  if( nByte == 0 ){
    free(zFilename);
    zFilename = 0;
  }
  return zFilename;
}

/*
** Convert an ansi string to microsoft unicode, based on the
** current codepage settings for file apis.
** 
** Space to hold the returned string is obtained
** from malloc.
*/
static WCHAR *mbcsToUnicode(const char *zFilename){
  int nByte;
  WCHAR *zMbcsFilename;
  int codepage = AreFileApisANSI() ? CP_ACP : CP_OEMCP;

  nByte = MultiByteToWideChar(codepage, 0, zFilename, -1, NULL,0)*sizeof(WCHAR);
  zMbcsFilename = malloc( nByte*sizeof(zMbcsFilename[0]) );
  if( zMbcsFilename==0 ){
    return 0;
  }
  nByte = MultiByteToWideChar(codepage, 0, zFilename, -1, zMbcsFilename, nByte);
  if( nByte==0 ){
    free(zMbcsFilename);
    zMbcsFilename = 0;
  }
  return zMbcsFilename;
}

/*
** Convert microsoft unicode to multibyte character string, based on the
** user's Ansi codepage.
**
** Space to hold the returned string is obtained from
** malloc().
*/
static char *unicodeToMbcs(const WCHAR *zWideFilename){
  int nByte;
  char *zFilename;
  int codepage = AreFileApisANSI() ? CP_ACP : CP_OEMCP;

  nByte = WideCharToMultiByte(codepage, 0, zWideFilename, -1, 0, 0, 0, 0);
  zFilename = malloc( nByte );
  if( zFilename==0 ){
    return 0;
  }
  nByte = WideCharToMultiByte(codepage, 0, zWideFilename, -1, zFilename, nByte,
                              0, 0);
  if( nByte == 0 ){
    free(zFilename);
    zFilename = 0;
  }
  return zFilename;
}

/*
** Convert multibyte character string to UTF-8.  Space to hold the
** returned string is obtained from malloc().
*/
char *sqlite3_win32_mbcs_to_utf8(const char *zFilename){
  char *zFilenameUtf8;
  WCHAR *zTmpWide;

  zTmpWide = mbcsToUnicode(zFilename);
  if( zTmpWide==0 ){
    return 0;
  }
  zFilenameUtf8 = unicodeToUtf8(zTmpWide);
  free(zTmpWide);
  return zFilenameUtf8;
}

/*
** Convert UTF-8 to multibyte character string.  Space to hold the 
** returned string is obtained from malloc().
*/
static char *utf8ToMbcs(const char *zFilename){
  char *zFilenameMbcs;
  WCHAR *zTmpWide;

  zTmpWide = utf8ToUnicode(zFilename);
  if( zTmpWide==0 ){
    return 0;
  }
  zFilenameMbcs = unicodeToMbcs(zTmpWide);
  free(zTmpWide);
  return zFilenameMbcs;
}

#if SQLITE_OS_WINCE
/*************************************************************************
** This section contains code for WinCE only.
*/
/*
** WindowsCE does not have a localtime() function.  So create a
** substitute.
*/
#include <time.h>
struct tm *__cdecl localtime(const time_t *t)
{
  static struct tm y;
  FILETIME uTm, lTm;
  SYSTEMTIME pTm;
  sqlite3_int64 t64;
  t64 = *t;
  t64 = (t64 + 11644473600)*10000000;
  uTm.dwLowDateTime = (DWORD)(t64 & 0xFFFFFFFF);
  uTm.dwHighDateTime= (DWORD)(t64 >> 32);
  FileTimeToLocalFileTime(&uTm,&lTm);
  FileTimeToSystemTime(&lTm,&pTm);
  y.tm_year = pTm.wYear - 1900;
  y.tm_mon = pTm.wMonth - 1;
  y.tm_wday = pTm.wDayOfWeek;
  y.tm_mday = pTm.wDay;
  y.tm_hour = pTm.wHour;
  y.tm_min = pTm.wMinute;
  y.tm_sec = pTm.wSecond;
  return &y;
}

/* This will never be called, but defined to make the code compile */
#define GetTempPathA(a,b)

#define LockFile(a,b,c,d,e)       winceLockFile(&a, b, c, d, e)
#define UnlockFile(a,b,c,d,e)     winceUnlockFile(&a, b, c, d, e)
#define LockFileEx(a,b,c,d,e,f)   winceLockFileEx(&a, b, c, d, e, f)

#define HANDLE_TO_WINFILE(a) (winFile*)&((char*)a)[-(int)offsetof(winFile,h)]

/*
** Acquire a lock on the handle h
*/
static void winceMutexAcquire(HANDLE h){
   DWORD dwErr;
   do {
     dwErr = WaitForSingleObject(h, INFINITE);
   } while (dwErr != WAIT_OBJECT_0 && dwErr != WAIT_ABANDONED);
}
/*
** Release a lock acquired by winceMutexAcquire()
*/
#define winceMutexRelease(h) ReleaseMutex(h)

/*
** Create the mutex and shared memory used for locking in the file
** descriptor pFile
*/
static BOOL winceCreateLock(const char *zFilename, winFile *pFile){
  WCHAR *zTok;
  WCHAR *zName = utf8ToUnicode(zFilename);
  BOOL bInit = TRUE;

  /* Initialize the local lockdata */
  ZeroMemory(&pFile->local, sizeof(pFile->local));

  /* Replace the backslashes from the filename and lowercase it
  ** to derive a mutex name. */
  zTok = CharLowerW(zName);
  for (;*zTok;zTok++){
    if (*zTok == '\\') *zTok = '_';
  }

  /* Create/open the named mutex */
  pFile->hMutex = CreateMutexW(NULL, FALSE, zName);
  if (!pFile->hMutex){
    pFile->lastErrno = GetLastError();
    free(zName);
    return FALSE;
  }

  /* Acquire the mutex before continuing */
  winceMutexAcquire(pFile->hMutex);
  
  /* Since the names of named mutexes, semaphores, file mappings etc are 
  ** case-sensitive, take advantage of that by uppercasing the mutex name
  ** and using that as the shared filemapping name.
  */
  CharUpperW(zName);
  pFile->hShared = CreateFileMappingW(INVALID_HANDLE_VALUE, NULL,
                                       PAGE_READWRITE, 0, sizeof(winceLock),
                                       zName);  

  /* Set a flag that indicates we're the first to create the memory so it 
  ** must be zero-initialized */
  if (GetLastError() == ERROR_ALREADY_EXISTS){
    bInit = FALSE;
  }

  free(zName);

  /* If we succeeded in making the shared memory handle, map it. */
  if (pFile->hShared){
    pFile->shared = (winceLock*)MapViewOfFile(pFile->hShared, 
             FILE_MAP_READ|FILE_MAP_WRITE, 0, 0, sizeof(winceLock));
    /* If mapping failed, close the shared memory handle and erase it */
    if (!pFile->shared){
      pFile->lastErrno = GetLastError();
      CloseHandle(pFile->hShared);
      pFile->hShared = NULL;
    }
  }

  /* If shared memory could not be created, then close the mutex and fail */
  if (pFile->hShared == NULL){
    winceMutexRelease(pFile->hMutex);
    CloseHandle(pFile->hMutex);
    pFile->hMutex = NULL;
    return FALSE;
  }
  
  /* Initialize the shared memory if we're supposed to */
  if (bInit) {
    ZeroMemory(pFile->shared, sizeof(winceLock));
  }

  winceMutexRelease(pFile->hMutex);
  return TRUE;
}

/*
** Destroy the part of winFile that deals with wince locks
*/
static void winceDestroyLock(winFile *pFile){
  if (pFile->hMutex){
    /* Acquire the mutex */
    winceMutexAcquire(pFile->hMutex);

    /* The following blocks should probably assert in debug mode, but they
       are to cleanup in case any locks remained open */
    if (pFile->local.nReaders){
      pFile->shared->nReaders --;
    }
    if (pFile->local.bReserved){
      pFile->shared->bReserved = FALSE;
    }
    if (pFile->local.bPending){
      pFile->shared->bPending = FALSE;
    }
    if (pFile->local.bExclusive){
      pFile->shared->bExclusive = FALSE;
    }

    /* De-reference and close our copy of the shared memory handle */
    UnmapViewOfFile(pFile->shared);
    CloseHandle(pFile->hShared);

    /* Done with the mutex */
    winceMutexRelease(pFile->hMutex);    
    CloseHandle(pFile->hMutex);
    pFile->hMutex = NULL;
  }
}

/* 
** An implementation of the LockFile() API of windows for wince
*/
static BOOL winceLockFile(
  HANDLE *phFile,
  DWORD dwFileOffsetLow,
  DWORD dwFileOffsetHigh,
  DWORD nNumberOfBytesToLockLow,
  DWORD nNumberOfBytesToLockHigh
){
  winFile *pFile = HANDLE_TO_WINFILE(phFile);
  BOOL bReturn = FALSE;

  UNUSED_PARAMETER(dwFileOffsetHigh);
  UNUSED_PARAMETER(nNumberOfBytesToLockHigh);

  if (!pFile->hMutex) return TRUE;
  winceMutexAcquire(pFile->hMutex);

  /* Wanting an exclusive lock? */
  if (dwFileOffsetLow == (DWORD)SHARED_FIRST
       && nNumberOfBytesToLockLow == (DWORD)SHARED_SIZE){
    if (pFile->shared->nReaders == 0 && pFile->shared->bExclusive == 0){
       pFile->shared->bExclusive = TRUE;
       pFile->local.bExclusive = TRUE;
       bReturn = TRUE;
    }
  }

  /* Want a read-only lock? */
  else if (dwFileOffsetLow == (DWORD)SHARED_FIRST &&
           nNumberOfBytesToLockLow == 1){
    if (pFile->shared->bExclusive == 0){
      pFile->local.nReaders ++;
      if (pFile->local.nReaders == 1){
        pFile->shared->nReaders ++;
      }
      bReturn = TRUE;
    }
  }

  /* Want a pending lock? */
  else if (dwFileOffsetLow == (DWORD)PENDING_BYTE && nNumberOfBytesToLockLow == 1){
    /* If no pending lock has been acquired, then acquire it */
    if (pFile->shared->bPending == 0) {
      pFile->shared->bPending = TRUE;
      pFile->local.bPending = TRUE;
      bReturn = TRUE;
    }
  }

  /* Want a reserved lock? */
  else if (dwFileOffsetLow == (DWORD)RESERVED_BYTE && nNumberOfBytesToLockLow == 1){
    if (pFile->shared->bReserved == 0) {
      pFile->shared->bReserved = TRUE;
      pFile->local.bReserved = TRUE;
      bReturn = TRUE;
    }
  }

  winceMutexRelease(pFile->hMutex);
  return bReturn;
}

/*
** An implementation of the UnlockFile API of windows for wince
*/
static BOOL winceUnlockFile(
  HANDLE *phFile,
  DWORD dwFileOffsetLow,
  DWORD dwFileOffsetHigh,
  DWORD nNumberOfBytesToUnlockLow,
  DWORD nNumberOfBytesToUnlockHigh
){
  winFile *pFile = HANDLE_TO_WINFILE(phFile);
  BOOL bReturn = FALSE;

  UNUSED_PARAMETER(dwFileOffsetHigh);
  UNUSED_PARAMETER(nNumberOfBytesToUnlockHigh);

  if (!pFile->hMutex) return TRUE;
  winceMutexAcquire(pFile->hMutex);

  /* Releasing a reader lock or an exclusive lock */
  if (dwFileOffsetLow == (DWORD)SHARED_FIRST){
    /* Did we have an exclusive lock? */
    if (pFile->local.bExclusive){
      assert(nNumberOfBytesToUnlockLow == (DWORD)SHARED_SIZE);
      pFile->local.bExclusive = FALSE;
      pFile->shared->bExclusive = FALSE;
      bReturn = TRUE;
    }

    /* Did we just have a reader lock? */
    else if (pFile->local.nReaders){
      assert(nNumberOfBytesToUnlockLow == (DWORD)SHARED_SIZE || nNumberOfBytesToUnlockLow == 1);
      pFile->local.nReaders --;
      if (pFile->local.nReaders == 0)
      {
        pFile->shared->nReaders --;
      }
      bReturn = TRUE;
    }
  }

  /* Releasing a pending lock */
  else if (dwFileOffsetLow == (DWORD)PENDING_BYTE && nNumberOfBytesToUnlockLow == 1){
    if (pFile->local.bPending){
      pFile->local.bPending = FALSE;
      pFile->shared->bPending = FALSE;
      bReturn = TRUE;
    }
  }
  /* Releasing a reserved lock */
  else if (dwFileOffsetLow == (DWORD)RESERVED_BYTE && nNumberOfBytesToUnlockLow == 1){
    if (pFile->local.bReserved) {
      pFile->local.bReserved = FALSE;
      pFile->shared->bReserved = FALSE;
      bReturn = TRUE;
    }
  }

  winceMutexRelease(pFile->hMutex);
  return bReturn;
}

/*
** An implementation of the LockFileEx() API of windows for wince
*/
static BOOL winceLockFileEx(
  HANDLE *phFile,
  DWORD dwFlags,
  DWORD dwReserved,
  DWORD nNumberOfBytesToLockLow,
  DWORD nNumberOfBytesToLockHigh,
  LPOVERLAPPED lpOverlapped
){
  UNUSED_PARAMETER(dwReserved);
  UNUSED_PARAMETER(nNumberOfBytesToLockHigh);

  /* If the caller wants a shared read lock, forward this call
  ** to winceLockFile */
  if (lpOverlapped->Offset == (DWORD)SHARED_FIRST &&
      dwFlags == 1 &&
      nNumberOfBytesToLockLow == (DWORD)SHARED_SIZE){
    return winceLockFile(phFile, SHARED_FIRST, 0, 1, 0);
  }
  return FALSE;
}
/*
** End of the special code for wince
*****************************************************************************/
#endif /* SQLITE_OS_WINCE */

/*****************************************************************************
** The next group of routines implement the I/O methods specified
** by the sqlite3_io_methods object.
******************************************************************************/

/*
** Close a file.
**
** It is reported that an attempt to close a handle might sometimes
** fail.  This is a very unreasonable result, but windows is notorious
** for being unreasonable so I do not doubt that it might happen.  If
** the close fails, we pause for 100 milliseconds and try again.  As
** many as MX_CLOSE_ATTEMPT attempts to close the handle are made before
** giving up and returning an error.
*/
#define MX_CLOSE_ATTEMPT 3
static int winClose(sqlite3_file *id){
  int rc, cnt = 0;
  winFile *pFile = (winFile*)id;

  assert( id!=0 );
  assert( pFile->pShm==0 );
  OSTRACE(("CLOSE %d\n", pFile->h));
  do{
    rc = CloseHandle(pFile->h);
  }while( rc==0 && ++cnt < MX_CLOSE_ATTEMPT && (Sleep(100), 1) );
#if SQLITE_OS_WINCE
#define WINCE_DELETION_ATTEMPTS 3
  winceDestroyLock(pFile);
  if( pFile->zDeleteOnClose ){
    int cnt = 0;
    while(
           DeleteFileW(pFile->zDeleteOnClose)==0
        && GetFileAttributesW(pFile->zDeleteOnClose)!=0xffffffff 
        && cnt++ < WINCE_DELETION_ATTEMPTS
    ){
       Sleep(100);  /* Wait a little before trying again */
    }
    free(pFile->zDeleteOnClose);
  }
#endif
  OSTRACE(("CLOSE %d %s\n", pFile->h, rc ? "ok" : "failed"));
  OpenCounter(-1);
  return rc ? SQLITE_OK : SQLITE_IOERR;
}

/*
** Some microsoft compilers lack this definition.
*/
#ifndef INVALID_SET_FILE_POINTER
# define INVALID_SET_FILE_POINTER ((DWORD)-1)
#endif

/*
** Read data from a file into a buffer.  Return SQLITE_OK if all
** bytes were read successfully and SQLITE_IOERR if anything goes
** wrong.
*/
static int winRead(
  sqlite3_file *id,          /* File to read from */
  void *pBuf,                /* Write content into this buffer */
  int amt,                   /* Number of bytes to read */
  sqlite3_int64 offset       /* Begin reading at this offset */
){
  LONG upperBits = (LONG)((offset>>32) & 0x7fffffff);
  LONG lowerBits = (LONG)(offset & 0xffffffff);
  DWORD rc;
  winFile *pFile = (winFile*)id;
  DWORD error;
  DWORD got;

  assert( id!=0 );
  SimulateIOError(return SQLITE_IOERR_READ);
  OSTRACE(("READ %d lock=%d\n", pFile->h, pFile->locktype));
  rc = SetFilePointer(pFile->h, lowerBits, &upperBits, FILE_BEGIN);
  if( rc==INVALID_SET_FILE_POINTER && (error=GetLastError())!=NO_ERROR ){
    pFile->lastErrno = error;
    return SQLITE_FULL;
  }
  if( !ReadFile(pFile->h, pBuf, amt, &got, 0) ){
    pFile->lastErrno = GetLastError();
    return SQLITE_IOERR_READ;
  }
  if( got==(DWORD)amt ){
    return SQLITE_OK;
  }else{
    /* Unread parts of the buffer must be zero-filled */
    memset(&((char*)pBuf)[got], 0, amt-got);
    return SQLITE_IOERR_SHORT_READ;
  }
}

/*
** Write data from a buffer into a file.  Return SQLITE_OK on success
** or some other error code on failure.
*/
static int winWrite(
  sqlite3_file *id,         /* File to write into */
  const void *pBuf,         /* The bytes to be written */
  int amt,                  /* Number of bytes to write */
  sqlite3_int64 offset      /* Offset into the file to begin writing at */
){
  LONG upperBits = (LONG)((offset>>32) & 0x7fffffff);
  LONG lowerBits = (LONG)(offset & 0xffffffff);
  DWORD rc;
  winFile *pFile = (winFile*)id;
  DWORD error;
  DWORD wrote = 0;

  assert( id!=0 );
  SimulateIOError(return SQLITE_IOERR_WRITE);
  SimulateDiskfullError(return SQLITE_FULL);
  OSTRACE(("WRITE %d lock=%d\n", pFile->h, pFile->locktype));
  rc = SetFilePointer(pFile->h, lowerBits, &upperBits, FILE_BEGIN);
  if( rc==INVALID_SET_FILE_POINTER && (error=GetLastError())!=NO_ERROR ){
    pFile->lastErrno = error;
    return SQLITE_FULL;
  }
  assert( amt>0 );
  while(
     amt>0
     && (rc = WriteFile(pFile->h, pBuf, amt, &wrote, 0))!=0
     && wrote>0
  ){
    amt -= wrote;
    pBuf = &((char*)pBuf)[wrote];
  }
  if( !rc || amt>(int)wrote ){
    pFile->lastErrno = GetLastError();
    return SQLITE_FULL;
  }
  return SQLITE_OK;
}

/*
** Truncate an open file to a specified size
*/
static int winTruncate(sqlite3_file *id, sqlite3_int64 nByte){
  LONG upperBits = (LONG)((nByte>>32) & 0x7fffffff);
  LONG lowerBits = (LONG)(nByte & 0xffffffff);
  DWORD dwRet;
  winFile *pFile = (winFile*)id;
  DWORD error;
  int rc = SQLITE_OK;

  assert( id!=0 );
  OSTRACE(("TRUNCATE %d %lld\n", pFile->h, nByte));
  SimulateIOError(return SQLITE_IOERR_TRUNCATE);
  dwRet = SetFilePointer(pFile->h, lowerBits, &upperBits, FILE_BEGIN);
  if( dwRet==INVALID_SET_FILE_POINTER && (error=GetLastError())!=NO_ERROR ){
    pFile->lastErrno = error;
    rc = SQLITE_IOERR_TRUNCATE;
  /* SetEndOfFile will fail if nByte is negative */
  }else if( !SetEndOfFile(pFile->h) ){
    pFile->lastErrno = GetLastError();
    rc = SQLITE_IOERR_TRUNCATE;
  }
  OSTRACE(("TRUNCATE %d %lld %s\n", pFile->h, nByte, rc==SQLITE_OK ? "ok" : "failed"));
  return rc;
}

#ifdef SQLITE_TEST
/*
** Count the number of fullsyncs and normal syncs.  This is used to test
** that syncs and fullsyncs are occuring at the right times.
*/
int sqlite3_sync_count = 0;
int sqlite3_fullsync_count = 0;
#endif

/*
** Make sure all writes to a particular file are committed to disk.
*/
static int winSync(sqlite3_file *id, int flags){
#ifndef SQLITE_NO_SYNC
  winFile *pFile = (winFile*)id;

  assert( id!=0 );
  OSTRACE(("SYNC %d lock=%d\n", pFile->h, pFile->locktype));
#else
  UNUSED_PARAMETER(id);
#endif
#ifndef SQLITE_TEST
  UNUSED_PARAMETER(flags);
#else
  if( flags & SQLITE_SYNC_FULL ){
    sqlite3_fullsync_count++;
  }
  sqlite3_sync_count++;
#endif
  /* If we compiled with the SQLITE_NO_SYNC flag, then syncing is a
  ** no-op
  */
#ifdef SQLITE_NO_SYNC
    return SQLITE_OK;
#else
  if( FlushFileBuffers(pFile->h) ){
    return SQLITE_OK;
  }else{
    pFile->lastErrno = GetLastError();
    return SQLITE_IOERR;
  }
#endif
}

/*
** Determine the current size of a file in bytes
*/
static int winFileSize(sqlite3_file *id, sqlite3_int64 *pSize){
  DWORD upperBits;
  DWORD lowerBits;
  winFile *pFile = (winFile*)id;
  DWORD error;

  assert( id!=0 );
  SimulateIOError(return SQLITE_IOERR_FSTAT);
  lowerBits = GetFileSize(pFile->h, &upperBits);
  if(   (lowerBits == INVALID_FILE_SIZE)
     && ((error = GetLastError()) != NO_ERROR) )
  {
    pFile->lastErrno = error;
    return SQLITE_IOERR_FSTAT;
  }
  *pSize = (((sqlite3_int64)upperBits)<<32) + lowerBits;
  return SQLITE_OK;
}

/*
** LOCKFILE_FAIL_IMMEDIATELY is undefined on some Windows systems.
*/
#ifndef LOCKFILE_FAIL_IMMEDIATELY
# define LOCKFILE_FAIL_IMMEDIATELY 1
#endif

/*
** Acquire a reader lock.
** Different API routines are called depending on whether or not this
** is Win95 or WinNT.
*/
static int getReadLock(winFile *pFile){
  int res;
  if( isNT() ){
    OVERLAPPED ovlp;
    ovlp.Offset = SHARED_FIRST;
    ovlp.OffsetHigh = 0;
    ovlp.hEvent = 0;
    res = LockFileEx(pFile->h, LOCKFILE_FAIL_IMMEDIATELY,
                     0, SHARED_SIZE, 0, &ovlp);
/* isNT() is 1 if SQLITE_OS_WINCE==1, so this else is never executed. 
*/
#if SQLITE_OS_WINCE==0
  }else{
    int lk;
    sqlite3_randomness(sizeof(lk), &lk);
    pFile->sharedLockByte = (short)((lk & 0x7fffffff)%(SHARED_SIZE - 1));
    res = LockFile(pFile->h, SHARED_FIRST+pFile->sharedLockByte, 0, 1, 0);
#endif
  }
  if( res == 0 ){
    pFile->lastErrno = GetLastError();
  }
  return res;
}

/*
** Undo a readlock
*/
static int unlockReadLock(winFile *pFile){
  int res;
  if( isNT() ){
    res = UnlockFile(pFile->h, SHARED_FIRST, 0, SHARED_SIZE, 0);
/* isNT() is 1 if SQLITE_OS_WINCE==1, so this else is never executed. 
*/
#if SQLITE_OS_WINCE==0
  }else{
    res = UnlockFile(pFile->h, SHARED_FIRST + pFile->sharedLockByte, 0, 1, 0);
#endif
  }
  if( res == 0 ){
    pFile->lastErrno = GetLastError();
  }
  return res;
}

/*
** Lock the file with the lock specified by parameter locktype - one
** of the following:
**
**     (1) SHARED_LOCK
**     (2) RESERVED_LOCK
**     (3) PENDING_LOCK
**     (4) EXCLUSIVE_LOCK
**
** Sometimes when requesting one lock state, additional lock states
** are inserted in between.  The locking might fail on one of the later
** transitions leaving the lock state different from what it started but
** still short of its goal.  The following chart shows the allowed
** transitions and the inserted intermediate states:
**
**    UNLOCKED -> SHARED
**    SHARED -> RESERVED
**    SHARED -> (PENDING) -> EXCLUSIVE
**    RESERVED -> (PENDING) -> EXCLUSIVE
**    PENDING -> EXCLUSIVE
**
** This routine will only increase a lock.  The winUnlock() routine
** erases all locks at once and returns us immediately to locking level 0.
** It is not possible to lower the locking level one step at a time.  You
** must go straight to locking level 0.
*/
static int winLock(sqlite3_file *id, int locktype){
  int rc = SQLITE_OK;    /* Return code from subroutines */
  int res = 1;           /* Result of a windows lock call */
  int newLocktype;       /* Set pFile->locktype to this value before exiting */
  int gotPendingLock = 0;/* True if we acquired a PENDING lock this time */
  winFile *pFile = (winFile*)id;
  DWORD error = NO_ERROR;

  assert( id!=0 );
  OSTRACE(("LOCK %d %d was %d(%d)\n",
           pFile->h, locktype, pFile->locktype, pFile->sharedLockByte));

  /* If there is already a lock of this type or more restrictive on the
  ** OsFile, do nothing. Don't use the end_lock: exit path, as
  ** sqlite3OsEnterMutex() hasn't been called yet.
  */
  if( pFile->locktype>=locktype ){
    return SQLITE_OK;
  }

  /* Make sure the locking sequence is correct
  */
  assert( pFile->locktype!=NO_LOCK || locktype==SHARED_LOCK );
  assert( locktype!=PENDING_LOCK );
  assert( locktype!=RESERVED_LOCK || pFile->locktype==SHARED_LOCK );

  /* Lock the PENDING_LOCK byte if we need to acquire a PENDING lock or
  ** a SHARED lock.  If we are acquiring a SHARED lock, the acquisition of
  ** the PENDING_LOCK byte is temporary.
  */
  newLocktype = pFile->locktype;
  if(   (pFile->locktype==NO_LOCK)
     || (   (locktype==EXCLUSIVE_LOCK)
         && (pFile->locktype==RESERVED_LOCK))
  ){
    int cnt = 3;
    while( cnt-->0 && (res = LockFile(pFile->h, PENDING_BYTE, 0, 1, 0))==0 ){
      /* Try 3 times to get the pending lock.  The pending lock might be
      ** held by another reader process who will release it momentarily.
      */
      OSTRACE(("could not get a PENDING lock. cnt=%d\n", cnt));
      Sleep(1);
    }
    gotPendingLock = res;
    if( !res ){
      error = GetLastError();
    }
  }

  /* Acquire a shared lock
  */
  if( locktype==SHARED_LOCK && res ){
    assert( pFile->locktype==NO_LOCK );
    res = getReadLock(pFile);
    if( res ){
      newLocktype = SHARED_LOCK;
    }else{
      error = GetLastError();
    }
  }

  /* Acquire a RESERVED lock
  */
  if( locktype==RESERVED_LOCK && res ){
    assert( pFile->locktype==SHARED_LOCK );
    res = LockFile(pFile->h, RESERVED_BYTE, 0, 1, 0);
    if( res ){
      newLocktype = RESERVED_LOCK;
    }else{
      error = GetLastError();
    }
  }

  /* Acquire a PENDING lock
  */
  if( locktype==EXCLUSIVE_LOCK && res ){
    newLocktype = PENDING_LOCK;
    gotPendingLock = 0;
  }

  /* Acquire an EXCLUSIVE lock
  */
  if( locktype==EXCLUSIVE_LOCK && res ){
    assert( pFile->locktype>=SHARED_LOCK );
    res = unlockReadLock(pFile);
    OSTRACE(("unreadlock = %d\n", res));
    res = LockFile(pFile->h, SHARED_FIRST, 0, SHARED_SIZE, 0);
    if( res ){
      newLocktype = EXCLUSIVE_LOCK;
    }else{
      error = GetLastError();
      OSTRACE(("error-code = %d\n", error));
      getReadLock(pFile);
    }
  }

  /* If we are holding a PENDING lock that ought to be released, then
  ** release it now.
  */
  if( gotPendingLock && locktype==SHARED_LOCK ){
    UnlockFile(pFile->h, PENDING_BYTE, 0, 1, 0);
  }

  /* Update the state of the lock has held in the file descriptor then
  ** return the appropriate result code.
  */
  if( res ){
    rc = SQLITE_OK;
  }else{
    OSTRACE(("LOCK FAILED %d trying for %d but got %d\n", pFile->h,
           locktype, newLocktype));
    pFile->lastErrno = error;
    rc = SQLITE_BUSY;
  }
  pFile->locktype = (u8)newLocktype;
  return rc;
}

/*
** This routine checks if there is a RESERVED lock held on the specified
** file by this or any other process. If such a lock is held, return
** non-zero, otherwise zero.
*/
static int winCheckReservedLock(sqlite3_file *id, int *pResOut){
  int rc;
  winFile *pFile = (winFile*)id;

  assert( id!=0 );
  if( pFile->locktype>=RESERVED_LOCK ){
    rc = 1;
    OSTRACE(("TEST WR-LOCK %d %d (local)\n", pFile->h, rc));
  }else{
    rc = LockFile(pFile->h, RESERVED_BYTE, 0, 1, 0);
    if( rc ){
      UnlockFile(pFile->h, RESERVED_BYTE, 0, 1, 0);
    }
    rc = !rc;
    OSTRACE(("TEST WR-LOCK %d %d (remote)\n", pFile->h, rc));
  }
  *pResOut = rc;
  return SQLITE_OK;
}

/*
** Lower the locking level on file descriptor id to locktype.  locktype
** must be either NO_LOCK or SHARED_LOCK.
**
** If the locking level of the file descriptor is already at or below
** the requested locking level, this routine is a no-op.
**
** It is not possible for this routine to fail if the second argument
** is NO_LOCK.  If the second argument is SHARED_LOCK then this routine
** might return SQLITE_IOERR;
*/
static int winUnlock(sqlite3_file *id, int locktype){
  int type;
  winFile *pFile = (winFile*)id;
  int rc = SQLITE_OK;
  assert( pFile!=0 );
  assert( locktype<=SHARED_LOCK );
  OSTRACE(("UNLOCK %d to %d was %d(%d)\n", pFile->h, locktype,
          pFile->locktype, pFile->sharedLockByte));
  type = pFile->locktype;
  if( type>=EXCLUSIVE_LOCK ){
    UnlockFile(pFile->h, SHARED_FIRST, 0, SHARED_SIZE, 0);
    if( locktype==SHARED_LOCK && !getReadLock(pFile) ){
      /* This should never happen.  We should always be able to
      ** reacquire the read lock */
      rc = SQLITE_IOERR_UNLOCK;
    }
  }
  if( type>=RESERVED_LOCK ){
    UnlockFile(pFile->h, RESERVED_BYTE, 0, 1, 0);
  }
  if( locktype==NO_LOCK && type>=SHARED_LOCK ){
    unlockReadLock(pFile);
  }
  if( type>=PENDING_LOCK ){
    UnlockFile(pFile->h, PENDING_BYTE, 0, 1, 0);
  }
  pFile->locktype = (u8)locktype;
  return rc;
}

/*
** Control and query of the open file handle.
*/
static int winFileControl(sqlite3_file *id, int op, void *pArg){
  switch( op ){
    case SQLITE_FCNTL_LOCKSTATE: {
      *(int*)pArg = ((winFile*)id)->locktype;
      return SQLITE_OK;
    }
    case SQLITE_LAST_ERRNO: {
      *(int*)pArg = (int)((winFile*)id)->lastErrno;
      return SQLITE_OK;
    }
  }
  return SQLITE_ERROR;
}

/*
** Return the sector size in bytes of the underlying block device for
** the specified file. This is almost always 512 bytes, but may be
** larger for some devices.
**
** SQLite code assumes this function cannot fail. It also assumes that
** if two files are created in the same file-system directory (i.e.
** a database and its journal file) that the sector size will be the
** same for both.
*/
static int winSectorSize(sqlite3_file *id){
  assert( id!=0 );
  return (int)(((winFile*)id)->sectorSize);
}

/*
** Return a vector of device characteristics.
*/
static int winDeviceCharacteristics(sqlite3_file *id){
  UNUSED_PARAMETER(id);
  return 0;
}

/****************************************************************************
********************************* Shared Memory *****************************
**
** The next subdivision of code manages the shared-memory primitives.
*/
#ifndef SQLITE_OMIT_WAL

/*
** Helper functions to obtain and relinquish the global mutex. The
** global mutex is used to protect the winLockInfo objects used by 
** this file, all of which may be shared by multiple threads.
**
** Function winShmMutexHeld() is used to assert() that the global mutex 
** is held when required. This function is only used as part of assert() 
** statements. e.g.
**
**   winShmEnterMutex()
**     assert( winShmMutexHeld() );
**   winEnterLeave()
*/
static void winShmEnterMutex(void){
  sqlite3_mutex_enter(sqlite3MutexAlloc(SQLITE_MUTEX_STATIC_MASTER));
}
static void winShmLeaveMutex(void){
  sqlite3_mutex_leave(sqlite3MutexAlloc(SQLITE_MUTEX_STATIC_MASTER));
}
#ifdef SQLITE_DEBUG
static int winShmMutexHeld(void) {
  return sqlite3_mutex_held(sqlite3MutexAlloc(SQLITE_MUTEX_STATIC_MASTER));
}
#endif

/*
** Object used to represent a single file opened and mmapped to provide
** shared memory.  When multiple threads all reference the same
** log-summary, each thread has its own winFile object, but they all
** point to a single instance of this object.  In other words, each
** log-summary is opened only once per process.
**
** winShmMutexHeld() must be true when creating or destroying
** this object or while reading or writing the following fields:
**
**      nRef
**      pNext 
**
** The following fields are read-only after the object is created:
** 
**      fid
**      zFilename
**
** Either winShmNode.mutex must be held or winShmNode.nRef==0 and
** winShmMutexHeld() is true when reading or writing any other field
** in this structure.
**
** To avoid deadlocks, mutex and mutexBuf are always released in the
** reverse order that they are acquired.  mutexBuf is always acquired
** first and released last.  This invariant is check by asserting
** sqlite3_mutex_notheld() on mutex whenever mutexBuf is acquired or
** released.
*/
struct winShmNode {
  sqlite3_mutex *mutex;      /* Mutex to access this object */
  sqlite3_mutex *mutexBuf;   /* Mutex to access zBuf[] */
  char *zFilename;           /* Name of the file */
  winFile hFile;             /* File handle from winOpen */
  HANDLE hMap;               /* File handle from CreateFileMapping */
  DWORD lastErrno;           /* The Windows errno from the last I/O error */
  int szMap;                 /* Size of the mapping of file into memory */
  char *pMMapBuf;            /* Where currently mmapped().  NULL if unmapped */
  int nRef;                  /* Number of winShm objects pointing to this */
  winShm *pFirst;            /* All winShm objects pointing to this */
  winShmNode *pNext;         /* Next in list of all winShmNode objects */
#ifdef SQLITE_DEBUG
  u8 exclMask;               /* Mask of exclusive locks held */
  u8 sharedMask;             /* Mask of shared locks held */
  u8 nextShmId;              /* Next available winShm.id value */
#endif
};

/*
** A global array of all winShmNode objects.
**
** The winShmMutexHeld() must be true while reading or writing this list.
*/
static winShmNode *winShmNodeList = 0;

/*
** Structure used internally by this VFS to record the state of an
** open shared memory connection.
**
** winShm.pFile->mutex must be held while reading or writing the
** winShm.pNext and winShm.locks[] elements.
**
** The winShm.pFile element is initialized when the object is created
** and is read-only thereafter.
*/
struct winShm {
  winShmNode *pShmNode;      /* The underlying winShmNode object */
  winShm *pNext;             /* Next winShm with the same winShmNode */
  u8 lockState;              /* Current lock state */
  u8 hasMutex;               /* True if holding the winShmNode mutex */
  u8 hasMutexBuf;            /* True if holding pFile->mutexBuf */
  u8 sharedMask;             /* Mask of shared locks held */
  u8 exclMask;               /* Mask of exclusive locks held */
#ifdef SQLITE_DEBUG
  u8 id;                     /* Id of this connection with its winShmNode */
#endif
};

/*
** Size increment by which shared memory grows
*/
#define SQLITE_WIN_SHM_INCR  4096

/*
** Constants used for locking
*/
#define WIN_SHM_BASE      32        /* Byte offset of the first lock byte */
#define WIN_SHM_DMS       0x01      /* Mask for Dead-Man-Switch lock */
#define WIN_SHM_A         0x10      /* Mask for region locks... */
#define WIN_SHM_B         0x20
#define WIN_SHM_C         0x40
#define WIN_SHM_D         0x80

#ifdef SQLITE_DEBUG
/*
** Return a pointer to a nul-terminated string in static memory that
** describes a locking mask.  The string is of the form "MSABCD" with
** each character representing a lock.  "M" for MUTEX, "S" for DMS, 
** and "A" through "D" for the region locks.  If a lock is held, the
** letter is shown.  If the lock is not held, the letter is converted
** to ".".
**
** This routine is for debugging purposes only and does not appear
** in a production build.
*/
static const char *winShmLockString(u8 mask){
  static char zBuf[48];
  static int iBuf = 0;
  char *z;

  z = &zBuf[iBuf];
  iBuf += 8;
  if( iBuf>=sizeof(zBuf) ) iBuf = 0;

  z[0] = (mask & WIN_SHM_DMS)   ? 'S' : '.';
  z[1] = (mask & WIN_SHM_A)     ? 'A' : '.';
  z[2] = (mask & WIN_SHM_B)     ? 'B' : '.';
  z[3] = (mask & WIN_SHM_C)     ? 'C' : '.';
  z[4] = (mask & WIN_SHM_D)     ? 'D' : '.';
  z[5] = 0;
  return z;
}
#endif /* SQLITE_DEBUG */

/*
** Apply posix advisory locks for all bytes identified in lockMask.
**
** lockMask might contain multiple bits but all bits are guaranteed
** to be contiguous.
**
** Locks block if the mask is exactly WIN_SHM_C and are non-blocking
** otherwise.
*/
#define _SHM_UNLCK  1
#define _SHM_RDLCK  2
#define _SHM_WRLCK  3
static int winShmSystemLock(
  winShmNode *pFile,    /* Apply locks to this open shared-memory segment */
  int lockType,         /* _SHM_UNLCK, _SHM_RDLCK, or _SHM_WRLCK */
  u8 lockMask           /* Which bytes to lock or unlock */
){
  OVERLAPPED ovlp;
  DWORD dwFlags;
  int nBytes;           /* Number of bytes to lock */
  int i;                /* Offset into the locking byte range */
  int rc = 0;           /* Result code form Lock/UnlockFileEx() */
  u8 mask;              /* Mask of bits in lockMask */

  /* Access to the winShmNode object is serialized by the caller */
  assert( sqlite3_mutex_held(pFile->mutex) || pFile->nRef==0 );

  /* Initialize the locking parameters */
  if( lockMask==WIN_SHM_C && lockType!=_SHM_UNLCK ){
    dwFlags = 0;
    OSTRACE(("SHM-LOCK %d requesting blocking lock %s\n", 
             pFile->hFile.h,
             winShmLockString(lockMask)));
  }else{
    dwFlags = LOCKFILE_FAIL_IMMEDIATELY;
    OSTRACE(("SHM-LOCK %d requesting %s %s\n", 
             pFile->hFile.h,
             lockType!=_SHM_UNLCK ? "lock" : "unlock", 
             winShmLockString(lockMask)));
  }
  if( lockType == _SHM_WRLCK ) dwFlags |= LOCKFILE_EXCLUSIVE_LOCK;

  /* Find the first bit in lockMask that is set */
  for(i=0, mask=0x01; mask!=0 && (lockMask&mask)==0; mask <<= 1, i++){}
  assert( mask!=0 );
  memset(&ovlp, 0, sizeof(OVERLAPPED));
  ovlp.Offset = i+WIN_SHM_BASE;
  nBytes = 1;

  /* Extend the locking range for each additional bit that is set */
  mask <<= 1;
  while( mask!=0 && (lockMask & mask)!=0 ){
    nBytes++;
    mask <<= 1;
  }

  /* Verify that all bits set in lockMask are contiguous */
  assert( mask==0 || (lockMask & ~(mask | (mask-1)))==0 );

  /* Release/Acquire the system-level lock */
  if( lockType==_SHM_UNLCK ){
    for(i=0; i<nBytes; i++, ovlp.Offset++){
      rc = UnlockFileEx(pFile->hFile.h, 0, 1, 0, &ovlp);
      if( !rc ) break;
    }
  }else{
    /* release old individual byte locks (if any)
    ** and set new individual byte locks */
    for(i=0; i<nBytes; i++, ovlp.Offset++){
      UnlockFileEx(pFile->hFile.h, 0, 1, 0, &ovlp);
      rc = LockFileEx(pFile->hFile.h, dwFlags, 0, 1, 0, &ovlp);
      if( !rc ) break;
    }
  }
  if( !rc ){
    OSTRACE(("SHM-LOCK %d %s ERROR 0x%08lx\n", 
             pFile->hFile.h,
             lockType==_SHM_UNLCK ? "UnlockFileEx" : "LockFileEx",
             GetLastError()));
    /* release individual byte locks (if any) */
    ovlp.Offset-=i;
    for(i=0; i<nBytes; i++, ovlp.Offset++){
      UnlockFileEx(pFile->hFile.h, 0, 1, 0, &ovlp);
    }
  }
  rc = (rc!=0) ? SQLITE_OK : SQLITE_BUSY;

  /* Update the global lock state and do debug tracing */
#ifdef SQLITE_DEBUG
  OSTRACE(("SHM-LOCK %d ", pFile->hFile.h));
  if( rc==SQLITE_OK ){
    if( lockType==_SHM_UNLCK ){
      OSTRACE(("unlock ok"));
      pFile->exclMask &= ~lockMask;
      pFile->sharedMask &= ~lockMask;
    }else if( lockType==_SHM_RDLCK ){
      OSTRACE(("read-lock ok"));
      pFile->exclMask &= ~lockMask;
      pFile->sharedMask |= lockMask;
    }else{
      assert( lockType==_SHM_WRLCK );
      OSTRACE(("write-lock ok"));
      pFile->exclMask |= lockMask;
      pFile->sharedMask &= ~lockMask;
    }
  }else{
    if( lockType==_SHM_UNLCK ){
      OSTRACE(("unlock failed"));
    }else if( lockType==_SHM_RDLCK ){
      OSTRACE(("read-lock failed"));
    }else{
      assert( lockType==_SHM_WRLCK );
      OSTRACE(("write-lock failed"));
    }
  }
  OSTRACE((" - change requested %s - afterwards %s:%s\n",
           winShmLockString(lockMask),
           winShmLockString(pFile->sharedMask),
           winShmLockString(pFile->exclMask)));
#endif

  return rc;
}

/*
** For connection p, unlock all of the locks identified by the unlockMask
** parameter.
*/
static int winShmUnlock(
  winShmNode *pFile,   /* The underlying shared-memory file */
  winShm *p,           /* The connection to be unlocked */
  u8 unlockMask         /* Mask of locks to be unlocked */
){
  int rc;      /* Result code */
  winShm *pX; /* For looping over all sibling connections */
  u8 allMask;  /* Union of locks held by connections other than "p" */

  /* Access to the winShmNode object is serialized by the caller */
  assert( sqlite3_mutex_held(pFile->mutex) );

  /* don't attempt to unlock anything we don't have locks for */
  if( (unlockMask & (p->exclMask|p->sharedMask)) != unlockMask ){
    OSTRACE(("SHM-LOCK %d unlocking more than we have locked - requested %s - have %s\n",
             pFile->hFile.h,
             winShmLockString(unlockMask),
             winShmLockString(p->exclMask|p->sharedMask)));
    unlockMask &= (p->exclMask|p->sharedMask);
  }

  /* Compute locks held by sibling connections */
  allMask = 0;
  for(pX=pFile->pFirst; pX; pX=pX->pNext){
    if( pX==p ) continue;
    assert( (pX->exclMask & (p->exclMask|p->sharedMask))==0 );
    allMask |= pX->sharedMask;
  }

  /* Unlock the system-level locks */
  if( (unlockMask & allMask)!=unlockMask ){
    rc = winShmSystemLock(pFile, _SHM_UNLCK, unlockMask & ~allMask);
  }else{
    rc = SQLITE_OK;
  }

  /* Undo the local locks */
  if( rc==SQLITE_OK ){
    p->exclMask &= ~unlockMask;
    p->sharedMask &= ~unlockMask;
  } 
  return rc;
}

/*
** Get reader locks for connection p on all locks in the readMask parameter.
*/
static int winShmSharedLock(
  winShmNode *pFile,   /* The underlying shared-memory file */
  winShm *p,           /* The connection to get the shared locks */
  u8 readMask           /* Mask of shared locks to be acquired */
){
  int rc;        /* Result code */
  winShm *pX;   /* For looping over all sibling connections */
  u8 allShared;  /* Union of locks held by connections other than "p" */

  /* Access to the winShmNode object is serialized by the caller */
  assert( sqlite3_mutex_held(pFile->mutex) );

  /* Find out which shared locks are already held by sibling connections.
  ** If any sibling already holds an exclusive lock, go ahead and return
  ** SQLITE_BUSY.
  */
  allShared = 0;
  for(pX=pFile->pFirst; pX; pX=pX->pNext){
    if( pX==p ) continue;
    if( (pX->exclMask & readMask)!=0 ) return SQLITE_BUSY;
    allShared |= pX->sharedMask;
  }

  /* Get shared locks at the system level, if necessary */
  if( (~allShared) & readMask ){
    rc = winShmSystemLock(pFile, _SHM_RDLCK, readMask);
  }else{
    rc = SQLITE_OK;
  }

  /* Get the local shared locks */
  if( rc==SQLITE_OK ){
    p->sharedMask |= readMask;
  }
  return rc;
}

/*
** For connection p, get an exclusive lock on all locks identified in
** the writeMask parameter.
*/
static int winShmExclusiveLock(
  winShmNode *pFile,    /* The underlying shared-memory file */
  winShm *p,            /* The connection to get the exclusive locks */
  u8 writeMask           /* Mask of exclusive locks to be acquired */
){
  int rc;        /* Result code */
  winShm *pX;   /* For looping over all sibling connections */

  /* Access to the winShmNode object is serialized by the caller */
  assert( sqlite3_mutex_held(pFile->mutex) );

  /* Make sure no sibling connections hold locks that will block this
  ** lock.  If any do, return SQLITE_BUSY right away.
  */
  for(pX=pFile->pFirst; pX; pX=pX->pNext){
    if( pX==p ) continue;
    if( (pX->exclMask & writeMask)!=0 ) return SQLITE_BUSY;
    if( (pX->sharedMask & writeMask)!=0 ) return SQLITE_BUSY;
  }

  /* Get the exclusive locks at the system level.  Then if successful
  ** also mark the local connection as being locked.
  */
  rc = winShmSystemLock(pFile, _SHM_WRLCK, writeMask);
  if( rc==SQLITE_OK ){
    p->sharedMask &= ~writeMask;
    p->exclMask |= writeMask;
  }
  return rc;
}

/*
** Purge the winShmNodeList list of all entries with winShmNode.nRef==0.
**
** This is not a VFS shared-memory method; it is a utility function called
** by VFS shared-memory methods.
*/
static void winShmPurge(void){
  winShmNode **pp;
  winShmNode *p;
  assert( winShmMutexHeld() );
  pp = &winShmNodeList;
  while( (p = *pp)!=0 ){
    if( p->nRef==0 ){
      if( p->mutex ) sqlite3_mutex_free(p->mutex);
      if( p->mutexBuf ) sqlite3_mutex_free(p->mutexBuf);
      if( p->pMMapBuf ){
        UnmapViewOfFile(p->pMMapBuf);
      }
      if( INVALID_HANDLE_VALUE != p->hMap ){
        CloseHandle(p->hMap);
      }
      if( p->hFile.h != INVALID_HANDLE_VALUE ) {
        winClose((sqlite3_file *)&p->hFile);
      }
      *pp = p->pNext;
      sqlite3_free(p);
    }else{
      pp = &p->pNext;
    }
  }
}

/* Forward references to VFS methods */
static int winOpen(sqlite3_vfs*,const char*,sqlite3_file*,int,int*);
static int winDelete(sqlite3_vfs *,const char*,int);

/*
** Open a shared-memory area.  This particular implementation uses
** mmapped files.
**
** zName is a filename used to identify the shared-memory area.  The
** implementation does not (and perhaps should not) use this name
** directly, but rather use it as a template for finding an appropriate
** name for the shared-memory storage.  In this implementation, the
** string "-index" is appended to zName and used as the name of the
** mmapped file.
**
** When opening a new shared-memory file, if no other instances of that
** file are currently open, in this process or in other processes, then
** the file must be truncated to zero length or have its header cleared.
*/
static int winShmOpen(
  sqlite3_file *fd      /* The file to which to attach shared memory */
){
  struct winFile *pDbFd;             /* Database to which to attach SHM */
  struct winShm *p;                  /* The connection to be opened */
  struct winShmNode *pShmNode = 0;   /* The underlying mmapped file */
  int rc;                            /* Result code */
  struct winShmNode *pNew;           /* Newly allocated winShmNode */
  int nName;                         /* Size of zName in bytes */

  pDbFd = (winFile*)fd;
  assert( pDbFd->pShm==0 );    /* Not previously opened */

  /* Allocate space for the new sqlite3_shm object.  Also speculatively
  ** allocate space for a new winShmNode and filename.
  */
  p = sqlite3_malloc( sizeof(*p) );
  if( p==0 ) return SQLITE_NOMEM;
  memset(p, 0, sizeof(*p));
  nName = sqlite3Strlen30(pDbFd->zPath);
  pNew = sqlite3_malloc( sizeof(*pShmNode) + nName + 15 );
  if( pNew==0 ){
    sqlite3_free(p);
    return SQLITE_NOMEM;
  }
  memset(pNew, 0, sizeof(*pNew));
  pNew->zFilename = (char*)&pNew[1];
  sqlite3_snprintf(nName+15, pNew->zFilename, "%s-wal-index", pDbFd->zPath);

  /* Look to see if there is an existing winShmNode that can be used.
  ** If no matching winShmNode currently exists, create a new one.
  */
  winShmEnterMutex();
  for(pShmNode = winShmNodeList; pShmNode; pShmNode=pShmNode->pNext){
    /* TBD need to come up with better match here.  Perhaps
    ** use FILE_ID_BOTH_DIR_INFO Structure.
    */
    if( sqlite3StrICmp(pShmNode->zFilename, pNew->zFilename)==0 ) break;
  }
  if( pShmNode ){
    sqlite3_free(pNew);
  }else{
    pShmNode = pNew;
    pNew = 0;
    pShmNode->pMMapBuf = NULL;
    pShmNode->hMap = INVALID_HANDLE_VALUE;
    ((winFile*)(&pShmNode->hFile))->h = INVALID_HANDLE_VALUE;
    pShmNode->pNext = winShmNodeList;
    winShmNodeList = pShmNode;

    pShmNode->mutex = sqlite3_mutex_alloc(SQLITE_MUTEX_FAST);
    if( pShmNode->mutex==0 ){
      rc = SQLITE_NOMEM;
      goto shm_open_err;
    }
    pShmNode->mutexBuf = sqlite3_mutex_alloc(SQLITE_MUTEX_FAST);
    if( pShmNode->mutexBuf==0 ){
      rc = SQLITE_NOMEM;
      goto shm_open_err;
    }
    rc = winOpen(pDbFd->pVfs,
                 pShmNode->zFilename,             /* Name of the file (UTF-8) */
                 (sqlite3_file*)&pShmNode->hFile,  /* File handle here */
                 SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, /* Mode flags */
                 0);
    if( SQLITE_OK!=rc ){
      rc = SQLITE_CANTOPEN_BKPT;
      goto shm_open_err;
    }

    /* Check to see if another process is holding the dead-man switch.
    ** If not, truncate the file to zero length. 
    */
    if( winShmSystemLock(pShmNode, _SHM_WRLCK, WIN_SHM_DMS)==SQLITE_OK ){
      rc = winTruncate((sqlite3_file *)&pShmNode->hFile, 0);
    }
    if( rc==SQLITE_OK ){
      rc = winShmSystemLock(pShmNode, _SHM_RDLCK, WIN_SHM_DMS);
    }
    if( rc ) goto shm_open_err;
  }

  /* Make the new connection a child of the winShmNode */
  p->pShmNode = pShmNode;
  p->pNext = pShmNode->pFirst;
#ifdef SQLITE_DEBUG
  p->id = pShmNode->nextShmId++;
#endif
  pShmNode->pFirst = p;
  pShmNode->nRef++;
  pDbFd->pShm = p;
  winShmLeaveMutex();
  return SQLITE_OK;

  /* Jump here on any error */
shm_open_err:
  winShmSystemLock(pShmNode, _SHM_UNLCK, WIN_SHM_DMS);
  winShmPurge();                 /* This call frees pShmNode if required */
  sqlite3_free(p);
  sqlite3_free(pNew);
  winShmLeaveMutex();
  return rc;
}

/*
** Close a connection to shared-memory.  Delete the underlying 
** storage if deleteFlag is true.
*/
static int winShmClose(
  sqlite3_file *fd,          /* Database holding shared memory */
  int deleteFlag             /* Delete after closing if true */
){
  winFile *pDbFd;       /* Database holding shared-memory */
  winShm *p;            /* The connection to be closed */
  winShmNode *pShmNode; /* The underlying shared-memory file */
  winShm **pp;          /* For looping over sibling connections */

  pDbFd = (winFile*)fd;
  p = pDbFd->pShm;
  pShmNode = p->pShmNode;

  /* Verify that the connection being closed holds no locks */
  assert( p->exclMask==0 );
  assert( p->sharedMask==0 );

  /* Remove connection p from the set of connections associated
  ** with pShmNode */
  sqlite3_mutex_enter(pShmNode->mutex);
  for(pp=&pShmNode->pFirst; (*pp)!=p; pp = &(*pp)->pNext){}
  *pp = p->pNext;

  /* Free the connection p */
  sqlite3_free(p);
  pDbFd->pShm = 0;
  sqlite3_mutex_leave(pShmNode->mutex);

  /* If pShmNode->nRef has reached 0, then close the underlying
  ** shared-memory file, too */
  winShmEnterMutex();
  assert( pShmNode->nRef>0 );
  pShmNode->nRef--;
  if( pShmNode->nRef==0 ){
    if( deleteFlag ) winDelete(pDbFd->pVfs, pShmNode->zFilename, 0);
    winShmPurge();
  }
  winShmLeaveMutex();

  return SQLITE_OK;
}

/*
** Query and/or changes the size of the underlying storage for
** a shared-memory segment.  The reqSize parameter is the new size
** of the underlying storage, or -1 to do just a query.  The size
** of the underlying storage (after resizing if resizing occurs) is
** written into pNewSize.
**
** This routine does not (necessarily) change the size of the mapping 
** of the underlying storage into memory.  Use xShmGet() to change
** the mapping size.
**
** The reqSize parameter is the minimum size requested.  The implementation
** is free to expand the storage to some larger amount if it chooses.
*/
static int winShmSize(
  sqlite3_file *fd,         /* Database holding the shared memory */
  int reqSize,              /* Requested size.  -1 for query only */
  int *pNewSize             /* Write new size here */
){
  winFile *pDbFd = (winFile*)fd;
  winShm *p = pDbFd->pShm;
  winShmNode *pShmNode = p->pShmNode;
  int rc = SQLITE_OK;

  *pNewSize = 0;
  if( reqSize>=0 ){
    sqlite3_int64 sz;
    rc = winFileSize((sqlite3_file *)&pShmNode->hFile, &sz);
    if( SQLITE_OK==rc ){
      reqSize = (reqSize + SQLITE_WIN_SHM_INCR - 1)/SQLITE_WIN_SHM_INCR;
      reqSize *= SQLITE_WIN_SHM_INCR;
      if( reqSize>sz ){
        rc = winTruncate((sqlite3_file *)&pShmNode->hFile, reqSize);
      }
    }
  }
  if( SQLITE_OK==rc ){
    sqlite3_int64 sz;
    rc = winFileSize((sqlite3_file *)&pShmNode->hFile, &sz);
    if( SQLITE_OK==rc ){
      *pNewSize = (int)sz;
    }else{
      rc = SQLITE_IOERR;
    }
  }
  return rc;
}


/*
** Map the shared storage into memory.  The minimum size of the
** mapping should be reqMapSize if reqMapSize is positive.  If
** reqMapSize is zero or negative, the implementation can choose
** whatever mapping size is convenient.
**
** *ppBuf is made to point to the memory which is a mapping of the
** underlying storage.  A mutex is acquired to prevent other threads
** from running while *ppBuf is in use in order to prevent other threads
** remapping *ppBuf out from under this thread.  The winShmRelease()
** call will release the mutex.  However, if the lock state is CHECKPOINT,
** the mutex is not acquired because CHECKPOINT will never remap the
** buffer.  RECOVER might remap, though, so CHECKPOINT will acquire
** the mutex if and when it promotes to RECOVER.
**
** RECOVER needs to be atomic.  The same mutex that prevents *ppBuf from
** being remapped also prevents more than one thread from being in
** RECOVER at a time.  But, RECOVER sometimes wants to remap itself.
** To prevent RECOVER from losing its lock while remapping, the
** mutex is not released by winShmRelease() when in RECOVER.
**
** *pNewMapSize is set to the size of the mapping.
**
** *ppBuf and *pNewMapSize might be NULL and zero if no space has
** yet been allocated to the underlying storage.
*/
static int winShmGet(
  sqlite3_file *fd,        /* The database file holding the shared memory */
  int reqMapSize,          /* Requested size of mapping. -1 means don't care */
  int *pNewMapSize,        /* Write new size of mapping here */
  void **ppBuf             /* Write mapping buffer origin here */
){
  winFile *pDbFd = (winFile*)fd;
  winShm *p = pDbFd->pShm;
  winShmNode *pShmNode = p->pShmNode;
  int rc = SQLITE_OK;

  if( p->lockState!=SQLITE_SHM_CHECKPOINT && p->hasMutexBuf==0 ){
    assert( sqlite3_mutex_notheld(pShmNode->mutex) );
    sqlite3_mutex_enter(pShmNode->mutexBuf);
    p->hasMutexBuf = 1;
  }
  sqlite3_mutex_enter(pShmNode->mutex);
  if( pShmNode->szMap==0 || reqMapSize>pShmNode->szMap ){
    int actualSize;
    if( winShmSize(fd, -1, &actualSize)==SQLITE_OK
     && reqMapSize<actualSize
    ){
      reqMapSize = actualSize;
    }
    if( pShmNode->pMMapBuf ){
      if( !UnmapViewOfFile(pShmNode->pMMapBuf) ){
        pShmNode->lastErrno = GetLastError();
        rc = SQLITE_IOERR;
      }
      CloseHandle(pShmNode->hMap);
      pShmNode->hMap = INVALID_HANDLE_VALUE;
    }
    if( SQLITE_OK == rc ){
      pShmNode->pMMapBuf = 0;
      if( reqMapSize == 0 ){
        /* can't create 0 byte file mapping in Windows */
        pShmNode->szMap = 0;
      }else{
        /* create the file mapping object */
        if( INVALID_HANDLE_VALUE == pShmNode->hMap ){
          /* TBD provide an object name to each file
          ** mapping so it can be re-used across processes.
          */
          pShmNode->hMap = CreateFileMapping(pShmNode->hFile.h,
                                          NULL,
                                          PAGE_READWRITE,
                                          0,
                                          reqMapSize,
                                          NULL);
        }
        if( NULL==pShmNode->hMap ){
          pShmNode->lastErrno = GetLastError();
          rc = SQLITE_IOERR;
          pShmNode->szMap = 0;
          pShmNode->hMap = INVALID_HANDLE_VALUE;
        }else{
          pShmNode->pMMapBuf = MapViewOfFile(pShmNode->hMap,
                                          FILE_MAP_WRITE | FILE_MAP_READ,
                                          0,
                                          0,
                                          reqMapSize);
          if( !pShmNode->pMMapBuf ){
            pShmNode->lastErrno = GetLastError();
            rc = SQLITE_IOERR;
            pShmNode->szMap = 0;
          }else{
            pShmNode->szMap = reqMapSize;
          }
        }
      }
    }
  }
  *pNewMapSize = pShmNode->szMap;
  *ppBuf = pShmNode->pMMapBuf;
  sqlite3_mutex_leave(pShmNode->mutex);
  return rc;
}

/*
** Release the lock held on the shared memory segment so that other
** threads are free to resize it if necessary.
**
** If the lock is not currently held, this routine is a harmless no-op.
**
** If the shared-memory object is in lock state RECOVER, then we do not
** really want to release the lock, so in that case too, this routine
** is a no-op.
*/
static int winShmRelease(sqlite3_file *fd){
  winFile *pDbFd = (winFile*)fd;
  winShm *p = pDbFd->pShm;
  if( p->hasMutexBuf && p->lockState!=SQLITE_SHM_RECOVER ){
    winShmNode *pShmNode = p->pShmNode;
    assert( sqlite3_mutex_notheld(pShmNode->mutex) );
    sqlite3_mutex_leave(pShmNode->mutexBuf);
    p->hasMutexBuf = 0;
  }
  return SQLITE_OK;
}

/*
** Symbolic names for LOCK states used for debugging.
*/
#ifdef SQLITE_DEBUG
static const char *azLkName[] = {
  "UNLOCK",
  "READ",
  "READ_FULL",
  "WRITE",
  "PENDING",
  "CHECKPOINT",
  "RECOVER"
};
#endif


/*
** Change the lock state for a shared-memory segment.
*/
static int winShmLock(
  sqlite3_file *fd,          /* Database holding the shared memory */
  int desiredLock,           /* One of SQLITE_SHM_xxxxx locking states */
  int *pGotLock              /* The lock you actually got */
){
  winFile *pDbFd = (winFile*)fd;
  winShm *p = pDbFd->pShm;
  winShmNode *pShmNode = p->pShmNode;
  int rc = SQLITE_PROTOCOL;

  /* Note that SQLITE_SHM_READ_FULL and SQLITE_SHM_PENDING are never
  ** directly requested; they are side effects from requesting
  ** SQLITE_SHM_READ and SQLITE_SHM_CHECKPOINT, respectively.
  */
  assert( desiredLock==SQLITE_SHM_UNLOCK
       || desiredLock==SQLITE_SHM_READ
       || desiredLock==SQLITE_SHM_WRITE
       || desiredLock==SQLITE_SHM_CHECKPOINT
       || desiredLock==SQLITE_SHM_RECOVER );

  /* Return directly if this is just a lock state query, or if
  ** the connection is already in the desired locking state.
  */
  if( desiredLock==p->lockState
   || (desiredLock==SQLITE_SHM_READ && p->lockState==SQLITE_SHM_READ_FULL)
  ){
    OSTRACE(("SHM-LOCK %d shmid-%d, pid-%d request %s and got %s\n",
             pShmNode->hFile.h,
             p->id, (int)GetCurrentProcessId(), azLkName[desiredLock],
             azLkName[p->lockState]));
    if( pGotLock ) *pGotLock = p->lockState;
    return SQLITE_OK;
  }

  OSTRACE(("SHM-LOCK %d shmid-%d, pid-%d request %s->%s\n",
           pShmNode->hFile.h,
           p->id, (int)GetCurrentProcessId(), azLkName[p->lockState], 
           azLkName[desiredLock]));
  
  if( desiredLock==SQLITE_SHM_RECOVER && !p->hasMutexBuf ){
    assert( sqlite3_mutex_notheld(pShmNode->mutex) );
    sqlite3_mutex_enter(pShmNode->mutexBuf);
    p->hasMutexBuf = 1;
  }
  sqlite3_mutex_enter(pShmNode->mutex);
  switch( desiredLock ){
    case SQLITE_SHM_UNLOCK: {
      assert( p->lockState!=SQLITE_SHM_RECOVER );
      winShmUnlock(pShmNode, p, WIN_SHM_A|WIN_SHM_B|WIN_SHM_C|WIN_SHM_D);
      rc = SQLITE_OK;
      p->lockState = SQLITE_SHM_UNLOCK;
      break;
    }
    case SQLITE_SHM_READ: {
      if( p->lockState==SQLITE_SHM_UNLOCK ){
        int nAttempt;
        rc = SQLITE_BUSY;
        assert( p->lockState==SQLITE_SHM_UNLOCK );
        for(nAttempt=0; nAttempt<5 && rc==SQLITE_BUSY; nAttempt++){
          rc = winShmSharedLock(pShmNode, p, WIN_SHM_A|WIN_SHM_B);
          if( rc==SQLITE_BUSY ){
            rc = winShmSharedLock(pShmNode, p, WIN_SHM_D);
            if( rc==SQLITE_OK ){
              p->lockState = SQLITE_SHM_READ_FULL;
            }
          }else{
            winShmUnlock(pShmNode, p, WIN_SHM_B);
            p->lockState = SQLITE_SHM_READ;
          }
        }
      }else{
       assert( p->lockState==SQLITE_SHM_WRITE
               || p->lockState==SQLITE_SHM_RECOVER );
        rc = winShmSharedLock(pShmNode, p, WIN_SHM_A);
        winShmUnlock(pShmNode, p, WIN_SHM_C|WIN_SHM_D);
        p->lockState = SQLITE_SHM_READ;
      }
      break;
    }
    case SQLITE_SHM_WRITE: {
      assert( p->lockState==SQLITE_SHM_READ 
              || p->lockState==SQLITE_SHM_READ_FULL );
      rc = winShmExclusiveLock(pShmNode, p, WIN_SHM_C|WIN_SHM_D);
      if( rc==SQLITE_OK ){
        p->lockState = SQLITE_SHM_WRITE;
      }
      break;
    }
    case SQLITE_SHM_CHECKPOINT: {
      assert( p->lockState==SQLITE_SHM_UNLOCK
           || p->lockState==SQLITE_SHM_PENDING
      );
      if( p->lockState==SQLITE_SHM_UNLOCK ){
        rc = winShmExclusiveLock(pShmNode, p, WIN_SHM_B|WIN_SHM_C);
        if( rc==SQLITE_OK ){
          p->lockState = SQLITE_SHM_PENDING;
        }
      }
      if( p->lockState==SQLITE_SHM_PENDING ){
        rc = winShmExclusiveLock(pShmNode, p, WIN_SHM_A);
        if( rc==SQLITE_OK ){
          p->lockState = SQLITE_SHM_CHECKPOINT;
        }
      }
      break;
    }
    default: {
      assert( desiredLock==SQLITE_SHM_RECOVER );
      assert( p->lockState==SQLITE_SHM_READ
           || p->lockState==SQLITE_SHM_READ_FULL
      );
      assert( sqlite3_mutex_held(pShmNode->mutexBuf) );
      rc = winShmExclusiveLock(pShmNode, p, WIN_SHM_C);
      if( rc==SQLITE_OK ){
        p->lockState = SQLITE_SHM_RECOVER;
      }
      break;
    }
  }
  sqlite3_mutex_leave(pShmNode->mutex);
  OSTRACE(("SHM-LOCK %d shmid-%d, pid-%d got %s\n",
           pShmNode->hFile.h, 
           p->id, (int)GetCurrentProcessId(), azLkName[p->lockState]));
  if( pGotLock ) *pGotLock = p->lockState;
  return rc;
}

#else
# define winShmOpen    0
# define winShmSize    0
# define winShmGet     0
# define winShmRelease 0
# define winShmLock    0
# define winShmClose   0
#endif /* #ifndef SQLITE_OMIT_WAL */
/*
***************************** End Shared Memory *****************************
****************************************************************************/

/*
** This vector defines all the methods that can operate on an
** sqlite3_file for win32.
*/
static const sqlite3_io_methods winIoMethod = {
  2,                        /* iVersion */
  winClose,
  winRead,
  winWrite,
  winTruncate,
  winSync,
  winFileSize,
  winLock,
  winUnlock,
  winCheckReservedLock,
  winFileControl,
  winSectorSize,
  winDeviceCharacteristics,
  winShmOpen,              /* xShmOpen */
  winShmSize,              /* xShmSize */
  winShmGet,               /* xShmGet */
  winShmRelease,           /* xShmRelease */
  winShmLock,              /* xShmLock */
  winShmClose              /* xShmClose */
};

/***************************************************************************
** Here ends the I/O methods that form the sqlite3_io_methods object.
**
** The next block of code implements the VFS methods.
****************************************************************************/

/*
** Convert a UTF-8 filename into whatever form the underlying
** operating system wants filenames in.  Space to hold the result
** is obtained from malloc and must be freed by the calling
** function.
*/
static void *convertUtf8Filename(const char *zFilename){
  void *zConverted = 0;
  if( isNT() ){
    zConverted = utf8ToUnicode(zFilename);
/* isNT() is 1 if SQLITE_OS_WINCE==1, so this else is never executed. 
*/
#if SQLITE_OS_WINCE==0
  }else{
    zConverted = utf8ToMbcs(zFilename);
#endif
  }
  /* caller will handle out of memory */
  return zConverted;
}

/*
** Create a temporary file name in zBuf.  zBuf must be big enough to
** hold at pVfs->mxPathname characters.
*/
static int getTempname(int nBuf, char *zBuf){
  static char zChars[] =
    "abcdefghijklmnopqrstuvwxyz"
    "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    "0123456789";
  size_t i, j;
  char zTempPath[MAX_PATH+1];
  if( sqlite3_temp_directory ){
    sqlite3_snprintf(MAX_PATH-30, zTempPath, "%s", sqlite3_temp_directory);
  }else if( isNT() ){
    char *zMulti;
    WCHAR zWidePath[MAX_PATH];
    GetTempPathW(MAX_PATH-30, zWidePath);
    zMulti = unicodeToUtf8(zWidePath);
    if( zMulti ){
      sqlite3_snprintf(MAX_PATH-30, zTempPath, "%s", zMulti);
      free(zMulti);
    }else{
      return SQLITE_NOMEM;
    }
/* isNT() is 1 if SQLITE_OS_WINCE==1, so this else is never executed. 
** Since the ASCII version of these Windows API do not exist for WINCE,
** it's important to not reference them for WINCE builds.
*/
#if SQLITE_OS_WINCE==0
  }else{
    char *zUtf8;
    char zMbcsPath[MAX_PATH];
    GetTempPathA(MAX_PATH-30, zMbcsPath);
    zUtf8 = sqlite3_win32_mbcs_to_utf8(zMbcsPath);
    if( zUtf8 ){
      sqlite3_snprintf(MAX_PATH-30, zTempPath, "%s", zUtf8);
      free(zUtf8);
    }else{
      return SQLITE_NOMEM;
    }
#endif
  }
  for(i=sqlite3Strlen30(zTempPath); i>0 && zTempPath[i-1]=='\\'; i--){}
  zTempPath[i] = 0;
  sqlite3_snprintf(nBuf-30, zBuf,
                   "%s\\"SQLITE_TEMP_FILE_PREFIX, zTempPath);
  j = sqlite3Strlen30(zBuf);
  sqlite3_randomness(20, &zBuf[j]);
  for(i=0; i<20; i++, j++){
    zBuf[j] = (char)zChars[ ((unsigned char)zBuf[j])%(sizeof(zChars)-1) ];
  }
  zBuf[j] = 0;
  OSTRACE(("TEMP FILENAME: %s\n", zBuf));
  return SQLITE_OK; 
}

/*
** The return value of getLastErrorMsg
** is zero if the error message fits in the buffer, or non-zero
** otherwise (if the message was truncated).
*/
static int getLastErrorMsg(int nBuf, char *zBuf){
  /* FormatMessage returns 0 on failure.  Otherwise it
  ** returns the number of TCHARs written to the output
  ** buffer, excluding the terminating null char.
  */
  DWORD error = GetLastError();
  DWORD dwLen = 0;
  char *zOut = 0;

  if( isNT() ){
    WCHAR *zTempWide = NULL;
    dwLen = FormatMessageW(FORMAT_MESSAGE_ALLOCATE_BUFFER | FORMAT_MESSAGE_FROM_SYSTEM | FORMAT_MESSAGE_IGNORE_INSERTS,
                           NULL,
                           error,
                           0,
                           (LPWSTR) &zTempWide,
                           0,
                           0);
    if( dwLen > 0 ){
      /* allocate a buffer and convert to UTF8 */
      zOut = unicodeToUtf8(zTempWide);
      /* free the system buffer allocated by FormatMessage */
      LocalFree(zTempWide);
    }
/* isNT() is 1 if SQLITE_OS_WINCE==1, so this else is never executed. 
** Since the ASCII version of these Windows API do not exist for WINCE,
** it's important to not reference them for WINCE builds.
*/
#if SQLITE_OS_WINCE==0
  }else{
    char *zTemp = NULL;
    dwLen = FormatMessageA(FORMAT_MESSAGE_ALLOCATE_BUFFER | FORMAT_MESSAGE_FROM_SYSTEM | FORMAT_MESSAGE_IGNORE_INSERTS,
                           NULL,
                           error,
                           0,
                           (LPSTR) &zTemp,
                           0,
                           0);
    if( dwLen > 0 ){
      /* allocate a buffer and convert to UTF8 */
      zOut = sqlite3_win32_mbcs_to_utf8(zTemp);
      /* free the system buffer allocated by FormatMessage */
      LocalFree(zTemp);
    }
#endif
  }
  if( 0 == dwLen ){
    sqlite3_snprintf(nBuf, zBuf, "OsError 0x%x (%u)", error, error);
  }else{
    /* copy a maximum of nBuf chars to output buffer */
    sqlite3_snprintf(nBuf, zBuf, "%s", zOut);
    /* free the UTF8 buffer */
    free(zOut);
  }
  return 0;
}

/*
** Open a file.
*/
static int winOpen(
  sqlite3_vfs *pVfs,        /* Not used */
  const char *zName,        /* Name of the file (UTF-8) */
  sqlite3_file *id,         /* Write the SQLite file handle here */
  int flags,                /* Open mode flags */
  int *pOutFlags            /* Status return flags */
){
  HANDLE h;
  DWORD dwDesiredAccess;
  DWORD dwShareMode;
  DWORD dwCreationDisposition;
  DWORD dwFlagsAndAttributes = 0;
#if SQLITE_OS_WINCE
  int isTemp = 0;
#endif
  winFile *pFile = (winFile*)id;
  void *zConverted;                 /* Filename in OS encoding */
  const char *zUtf8Name = zName;    /* Filename in UTF-8 encoding */
  char zTmpname[MAX_PATH+1];        /* Buffer used to create temp filename */

  assert( id!=0 );
  UNUSED_PARAMETER(pVfs);

  pFile->h = INVALID_HANDLE_VALUE;

  /* If the second argument to this function is NULL, generate a 
  ** temporary file name to use 
  */
  if( !zUtf8Name ){
    int rc = getTempname(MAX_PATH+1, zTmpname);
    if( rc!=SQLITE_OK ){
      return rc;
    }
    zUtf8Name = zTmpname;
  }

  /* Convert the filename to the system encoding. */
  zConverted = convertUtf8Filename(zUtf8Name);
  if( zConverted==0 ){
    return SQLITE_NOMEM;
  }

  if( flags & SQLITE_OPEN_READWRITE ){
    dwDesiredAccess = GENERIC_READ | GENERIC_WRITE;
  }else{
    dwDesiredAccess = GENERIC_READ;
  }
  /* SQLITE_OPEN_EXCLUSIVE is used to make sure that a new file is 
  ** created. SQLite doesn't use it to indicate "exclusive access" 
  ** as it is usually understood.
  */
  assert(!(flags & SQLITE_OPEN_EXCLUSIVE) || (flags & SQLITE_OPEN_CREATE));
  if( flags & SQLITE_OPEN_EXCLUSIVE ){
    /* Creates a new file, only if it does not already exist. */
    /* If the file exists, it fails. */
    dwCreationDisposition = CREATE_NEW;
  }else if( flags & SQLITE_OPEN_CREATE ){
    /* Open existing file, or create if it doesn't exist */
    dwCreationDisposition = OPEN_ALWAYS;
  }else{
    /* Opens a file, only if it exists. */
    dwCreationDisposition = OPEN_EXISTING;
  }
  dwShareMode = FILE_SHARE_READ | FILE_SHARE_WRITE;
  if( flags & SQLITE_OPEN_DELETEONCLOSE ){
#if SQLITE_OS_WINCE
    dwFlagsAndAttributes = FILE_ATTRIBUTE_HIDDEN;
    isTemp = 1;
#else
    dwFlagsAndAttributes = FILE_ATTRIBUTE_TEMPORARY
                               | FILE_ATTRIBUTE_HIDDEN
                               | FILE_FLAG_DELETE_ON_CLOSE;
#endif
  }else{
    dwFlagsAndAttributes = FILE_ATTRIBUTE_NORMAL;
  }
  /* Reports from the internet are that performance is always
  ** better if FILE_FLAG_RANDOM_ACCESS is used.  Ticket #2699. */
#if SQLITE_OS_WINCE
  dwFlagsAndAttributes |= FILE_FLAG_RANDOM_ACCESS;
#endif
  if( isNT() ){
    h = CreateFileW((WCHAR*)zConverted,
       dwDesiredAccess,
       dwShareMode,
       NULL,
       dwCreationDisposition,
       dwFlagsAndAttributes,
       NULL
    );
/* isNT() is 1 if SQLITE_OS_WINCE==1, so this else is never executed. 
** Since the ASCII version of these Windows API do not exist for WINCE,
** it's important to not reference them for WINCE builds.
*/
#if SQLITE_OS_WINCE==0
  }else{
    h = CreateFileA((char*)zConverted,
       dwDesiredAccess,
       dwShareMode,
       NULL,
       dwCreationDisposition,
       dwFlagsAndAttributes,
       NULL
    );
#endif
  }
  OSTRACE(("OPEN %d %s 0x%lx %s\n", 
           h, zName, dwDesiredAccess, 
           h==INVALID_HANDLE_VALUE ? "failed" : "ok"));
  if( h==INVALID_HANDLE_VALUE ){
    free(zConverted);
    if( flags & SQLITE_OPEN_READWRITE ){
      return winOpen(pVfs, zName, id, 
             ((flags|SQLITE_OPEN_READONLY)&~SQLITE_OPEN_READWRITE), pOutFlags);
    }else{
      return SQLITE_CANTOPEN_BKPT;
    }
  }
  if( pOutFlags ){
    if( flags & SQLITE_OPEN_READWRITE ){
      *pOutFlags = SQLITE_OPEN_READWRITE;
    }else{
      *pOutFlags = SQLITE_OPEN_READONLY;
    }
  }
  memset(pFile, 0, sizeof(*pFile));
  pFile->pMethod = &winIoMethod;
  pFile->h = h;
  pFile->lastErrno = NO_ERROR;
  pFile->pVfs = pVfs;
  pFile->pShm = 0;
  pFile->zPath = zName;
  pFile->sectorSize = getSectorSize(pVfs, zUtf8Name);
#if SQLITE_OS_WINCE
  if( (flags & (SQLITE_OPEN_READWRITE|SQLITE_OPEN_MAIN_DB)) ==
               (SQLITE_OPEN_READWRITE|SQLITE_OPEN_MAIN_DB)
       && !winceCreateLock(zName, pFile)
  ){
    CloseHandle(h);
    free(zConverted);
    return SQLITE_CANTOPEN_BKPT;
  }
  if( isTemp ){
    pFile->zDeleteOnClose = zConverted;
  }else
#endif
  {
    free(zConverted);
  }
  OpenCounter(+1);
  return SQLITE_OK;
}

/*
** Delete the named file.
**
** Note that windows does not allow a file to be deleted if some other
** process has it open.  Sometimes a virus scanner or indexing program
** will open a journal file shortly after it is created in order to do
** whatever it does.  While this other process is holding the
** file open, we will be unable to delete it.  To work around this
** problem, we delay 100 milliseconds and try to delete again.  Up
** to MX_DELETION_ATTEMPTs deletion attempts are run before giving
** up and returning an error.
*/
#define MX_DELETION_ATTEMPTS 5
static int winDelete(
  sqlite3_vfs *pVfs,          /* Not used on win32 */
  const char *zFilename,      /* Name of file to delete */
  int syncDir                 /* Not used on win32 */
){
  int cnt = 0;
  DWORD rc;
  DWORD error = 0;
  void *zConverted = convertUtf8Filename(zFilename);
  UNUSED_PARAMETER(pVfs);
  UNUSED_PARAMETER(syncDir);
  if( zConverted==0 ){
    return SQLITE_NOMEM;
  }
  SimulateIOError(return SQLITE_IOERR_DELETE);
  if( isNT() ){
    do{
      DeleteFileW(zConverted);
    }while(   (   ((rc = GetFileAttributesW(zConverted)) != INVALID_FILE_ATTRIBUTES)
               || ((error = GetLastError()) == ERROR_ACCESS_DENIED))
           && (++cnt < MX_DELETION_ATTEMPTS)
           && (Sleep(100), 1) );
/* isNT() is 1 if SQLITE_OS_WINCE==1, so this else is never executed. 
** Since the ASCII version of these Windows API do not exist for WINCE,
** it's important to not reference them for WINCE builds.
*/
#if SQLITE_OS_WINCE==0
  }else{
    do{
      DeleteFileA(zConverted);
    }while(   (   ((rc = GetFileAttributesA(zConverted)) != INVALID_FILE_ATTRIBUTES)
               || ((error = GetLastError()) == ERROR_ACCESS_DENIED))
           && (++cnt < MX_DELETION_ATTEMPTS)
           && (Sleep(100), 1) );
#endif
  }
  free(zConverted);
  OSTRACE(("DELETE \"%s\" %s\n", zFilename,
       ( (rc==INVALID_FILE_ATTRIBUTES) && (error==ERROR_FILE_NOT_FOUND)) ?
         "ok" : "failed" ));
 
  return (   (rc == INVALID_FILE_ATTRIBUTES) 
          && (error == ERROR_FILE_NOT_FOUND)) ? SQLITE_OK : SQLITE_IOERR_DELETE;
}

/*
** Check the existance and status of a file.
*/
static int winAccess(
  sqlite3_vfs *pVfs,         /* Not used on win32 */
  const char *zFilename,     /* Name of file to check */
  int flags,                 /* Type of test to make on this file */
  int *pResOut               /* OUT: Result */
){
  DWORD attr;
  int rc = 0;
  void *zConverted = convertUtf8Filename(zFilename);
  UNUSED_PARAMETER(pVfs);
  if( zConverted==0 ){
    return SQLITE_NOMEM;
  }
  if( isNT() ){
    attr = GetFileAttributesW((WCHAR*)zConverted);
/* isNT() is 1 if SQLITE_OS_WINCE==1, so this else is never executed. 
** Since the ASCII version of these Windows API do not exist for WINCE,
** it's important to not reference them for WINCE builds.
*/
#if SQLITE_OS_WINCE==0
  }else{
    attr = GetFileAttributesA((char*)zConverted);
#endif
  }
  free(zConverted);
  switch( flags ){
    case SQLITE_ACCESS_READ:
    case SQLITE_ACCESS_EXISTS:
      rc = attr!=INVALID_FILE_ATTRIBUTES;
      break;
    case SQLITE_ACCESS_READWRITE:
      rc = (attr & FILE_ATTRIBUTE_READONLY)==0;
      break;
    default:
      assert(!"Invalid flags argument");
  }
  *pResOut = rc;
  return SQLITE_OK;
}


/*
** Turn a relative pathname into a full pathname.  Write the full
** pathname into zOut[].  zOut[] will be at least pVfs->mxPathname
** bytes in size.
*/
static int winFullPathname(
  sqlite3_vfs *pVfs,            /* Pointer to vfs object */
  const char *zRelative,        /* Possibly relative input path */
  int nFull,                    /* Size of output buffer in bytes */
  char *zFull                   /* Output buffer */
){
  
#if defined(__CYGWIN__)
  UNUSED_PARAMETER(nFull);
  cygwin_conv_to_full_win32_path(zRelative, zFull);
  return SQLITE_OK;
#endif

#if SQLITE_OS_WINCE
  UNUSED_PARAMETER(nFull);
  /* WinCE has no concept of a relative pathname, or so I am told. */
  sqlite3_snprintf(pVfs->mxPathname, zFull, "%s", zRelative);
  return SQLITE_OK;
#endif

#if !SQLITE_OS_WINCE && !defined(__CYGWIN__)
  int nByte;
  void *zConverted;
  char *zOut;
  UNUSED_PARAMETER(nFull);
  zConverted = convertUtf8Filename(zRelative);
  if( isNT() ){
    WCHAR *zTemp;
    nByte = GetFullPathNameW((WCHAR*)zConverted, 0, 0, 0) + 3;
    zTemp = malloc( nByte*sizeof(zTemp[0]) );
    if( zTemp==0 ){
      free(zConverted);
      return SQLITE_NOMEM;
    }
    GetFullPathNameW((WCHAR*)zConverted, nByte, zTemp, 0);
    free(zConverted);
    zOut = unicodeToUtf8(zTemp);
    free(zTemp);
/* isNT() is 1 if SQLITE_OS_WINCE==1, so this else is never executed. 
** Since the ASCII version of these Windows API do not exist for WINCE,
** it's important to not reference them for WINCE builds.
*/
#if SQLITE_OS_WINCE==0
  }else{
    char *zTemp;
    nByte = GetFullPathNameA((char*)zConverted, 0, 0, 0) + 3;
    zTemp = malloc( nByte*sizeof(zTemp[0]) );
    if( zTemp==0 ){
      free(zConverted);
      return SQLITE_NOMEM;
    }
    GetFullPathNameA((char*)zConverted, nByte, zTemp, 0);
    free(zConverted);
    zOut = sqlite3_win32_mbcs_to_utf8(zTemp);
    free(zTemp);
#endif
  }
  if( zOut ){
    sqlite3_snprintf(pVfs->mxPathname, zFull, "%s", zOut);
    free(zOut);
    return SQLITE_OK;
  }else{
    return SQLITE_NOMEM;
  }
#endif
}

/*
** Get the sector size of the device used to store
** file.
*/
static int getSectorSize(
    sqlite3_vfs *pVfs,
    const char *zRelative     /* UTF-8 file name */
){
  DWORD bytesPerSector = SQLITE_DEFAULT_SECTOR_SIZE;
  /* GetDiskFreeSpace is not supported under WINCE */
#if SQLITE_OS_WINCE
  UNUSED_PARAMETER(pVfs);
  UNUSED_PARAMETER(zRelative);
#else
  char zFullpath[MAX_PATH+1];
  int rc;
  DWORD dwRet = 0;
  DWORD dwDummy;

  /*
  ** We need to get the full path name of the file
  ** to get the drive letter to look up the sector
  ** size.
  */
  rc = winFullPathname(pVfs, zRelative, MAX_PATH, zFullpath);
  if( rc == SQLITE_OK )
  {
    void *zConverted = convertUtf8Filename(zFullpath);
    if( zConverted ){
      if( isNT() ){
        /* trim path to just drive reference */
        WCHAR *p = zConverted;
        for(;*p;p++){
          if( *p == '\\' ){
            *p = '\0';
            break;
          }
        }
        dwRet = GetDiskFreeSpaceW((WCHAR*)zConverted,
                                  &dwDummy,
                                  &bytesPerSector,
                                  &dwDummy,
                                  &dwDummy);
      }else{
        /* trim path to just drive reference */
        char *p = (char *)zConverted;
        for(;*p;p++){
          if( *p == '\\' ){
            *p = '\0';
            break;
          }
        }
        dwRet = GetDiskFreeSpaceA((char*)zConverted,
                                  &dwDummy,
                                  &bytesPerSector,
                                  &dwDummy,
                                  &dwDummy);
      }
      free(zConverted);
    }
    if( !dwRet ){
      bytesPerSector = SQLITE_DEFAULT_SECTOR_SIZE;
    }
  }
#endif
  return (int) bytesPerSector; 
}

#ifndef SQLITE_OMIT_LOAD_EXTENSION
/*
** Interfaces for opening a shared library, finding entry points
** within the shared library, and closing the shared library.
*/
/*
** Interfaces for opening a shared library, finding entry points
** within the shared library, and closing the shared library.
*/
static void *winDlOpen(sqlite3_vfs *pVfs, const char *zFilename){
  HANDLE h;
  void *zConverted = convertUtf8Filename(zFilename);
  UNUSED_PARAMETER(pVfs);
  if( zConverted==0 ){
    return 0;
  }
  if( isNT() ){
    h = LoadLibraryW((WCHAR*)zConverted);
/* isNT() is 1 if SQLITE_OS_WINCE==1, so this else is never executed. 
** Since the ASCII version of these Windows API do not exist for WINCE,
** it's important to not reference them for WINCE builds.
*/
#if SQLITE_OS_WINCE==0
  }else{
    h = LoadLibraryA((char*)zConverted);
#endif
  }
  free(zConverted);
  return (void*)h;
}
static void winDlError(sqlite3_vfs *pVfs, int nBuf, char *zBufOut){
  UNUSED_PARAMETER(pVfs);
  getLastErrorMsg(nBuf, zBufOut);
}
void (*winDlSym(sqlite3_vfs *pVfs, void *pHandle, const char *zSymbol))(void){
  UNUSED_PARAMETER(pVfs);
#if SQLITE_OS_WINCE
  /* The GetProcAddressA() routine is only available on wince. */
  return (void(*)(void))GetProcAddressA((HANDLE)pHandle, zSymbol);
#else
  /* All other windows platforms expect GetProcAddress() to take
  ** an Ansi string regardless of the _UNICODE setting */
  return (void(*)(void))GetProcAddress((HANDLE)pHandle, zSymbol);
#endif
}
void winDlClose(sqlite3_vfs *pVfs, void *pHandle){
  UNUSED_PARAMETER(pVfs);
  FreeLibrary((HANDLE)pHandle);
}
#else /* if SQLITE_OMIT_LOAD_EXTENSION is defined: */
  #define winDlOpen  0
  #define winDlError 0
  #define winDlSym   0
  #define winDlClose 0
#endif


/*
** Write up to nBuf bytes of randomness into zBuf.
*/
static int winRandomness(sqlite3_vfs *pVfs, int nBuf, char *zBuf){
  int n = 0;
  UNUSED_PARAMETER(pVfs);
#if defined(SQLITE_TEST)
  n = nBuf;
  memset(zBuf, 0, nBuf);
#else
  if( sizeof(SYSTEMTIME)<=nBuf-n ){
    SYSTEMTIME x;
    GetSystemTime(&x);
    memcpy(&zBuf[n], &x, sizeof(x));
    n += sizeof(x);
  }
  if( sizeof(DWORD)<=nBuf-n ){
    DWORD pid = GetCurrentProcessId();
    memcpy(&zBuf[n], &pid, sizeof(pid));
    n += sizeof(pid);
  }
  if( sizeof(DWORD)<=nBuf-n ){
    DWORD cnt = GetTickCount();
    memcpy(&zBuf[n], &cnt, sizeof(cnt));
    n += sizeof(cnt);
  }
  if( sizeof(LARGE_INTEGER)<=nBuf-n ){
    LARGE_INTEGER i;
    QueryPerformanceCounter(&i);
    memcpy(&zBuf[n], &i, sizeof(i));
    n += sizeof(i);
  }
#endif
  return n;
}


/*
** Sleep for a little while.  Return the amount of time slept.
*/
static int winSleep(sqlite3_vfs *pVfs, int microsec){
  Sleep((microsec+999)/1000);
  UNUSED_PARAMETER(pVfs);
  return ((microsec+999)/1000)*1000;
}

/*
** The following variable, if set to a non-zero value, is interpreted as
** the number of seconds since 1970 and is used to set the result of
** sqlite3OsCurrentTime() during testing.
*/
#ifdef SQLITE_TEST
int sqlite3_current_time = 0;  /* Fake system time in seconds since 1970. */
#endif

/*
** Find the current time (in Universal Coordinated Time).  Write into *piNow
** the current time and date as a Julian Day number times 86_400_000.  In
** other words, write into *piNow the number of milliseconds since the Julian
** epoch of noon in Greenwich on November 24, 4714 B.C according to the
** proleptic Gregorian calendar.
**
** On success, return 0.  Return 1 if the time and date cannot be found.
*/
static int winCurrentTimeInt64(sqlite3_vfs *pVfs, sqlite3_int64 *piNow){
  /* FILETIME structure is a 64-bit value representing the number of 
     100-nanosecond intervals since January 1, 1601 (= JD 2305813.5). 
  */
  FILETIME ft;
  static const sqlite3_int64 winFiletimeEpoch = 23058135*(sqlite3_int64)8640000;
#ifdef SQLITE_TEST
  static const sqlite3_int64 unixEpoch = 24405875*(sqlite3_int64)8640000;
#endif
  /* 2^32 - to avoid use of LL and warnings in gcc */
  static const sqlite3_int64 max32BitValue = 
      (sqlite3_int64)2000000000 + (sqlite3_int64)2000000000 + (sqlite3_int64)294967296;

#if SQLITE_OS_WINCE
  SYSTEMTIME time;
  GetSystemTime(&time);
  /* if SystemTimeToFileTime() fails, it returns zero. */
  if (!SystemTimeToFileTime(&time,&ft)){
    return 1;
  }
#else
  GetSystemTimeAsFileTime( &ft );
#endif

  *piNow = winFiletimeEpoch +
            ((((sqlite3_int64)ft.dwHighDateTime)*max32BitValue) + 
               (sqlite3_int64)ft.dwLowDateTime)/(sqlite3_int64)1000;

#ifdef SQLITE_TEST
  if( sqlite3_current_time ){
    *piNow = 1000*(sqlite3_int64)sqlite3_current_time + unixEpoch;
  }
#endif
  UNUSED_PARAMETER(pVfs);
  return 0;
}

/*
** Find the current time (in Universal Coordinated Time).  Write the
** current time and date as a Julian Day number into *prNow and
** return 0.  Return 1 if the time and date cannot be found.
*/
int winCurrentTime(sqlite3_vfs *pVfs, double *prNow){
  int rc;
  sqlite3_int64 i;
  rc = winCurrentTimeInt64(pVfs, &i);
  if( !rc ){
    *prNow = i/86400000.0;
  }
  return rc;
}

/*
** The idea is that this function works like a combination of
** GetLastError() and FormatMessage() on windows (or errno and
** strerror_r() on unix). After an error is returned by an OS
** function, SQLite calls this function with zBuf pointing to
** a buffer of nBuf bytes. The OS layer should populate the
** buffer with a nul-terminated UTF-8 encoded error message
** describing the last IO error to have occurred within the calling
** thread.
**
** If the error message is too large for the supplied buffer,
** it should be truncated. The return value of xGetLastError
** is zero if the error message fits in the buffer, or non-zero
** otherwise (if the message was truncated). If non-zero is returned,
** then it is not necessary to include the nul-terminator character
** in the output buffer.
**
** Not supplying an error message will have no adverse effect
** on SQLite. It is fine to have an implementation that never
** returns an error message:
**
**   int xGetLastError(sqlite3_vfs *pVfs, int nBuf, char *zBuf){
**     assert(zBuf[0]=='\0');
**     return 0;
**   }
**
** However if an error message is supplied, it will be incorporated
** by sqlite into the error message available to the user using
** sqlite3_errmsg(), possibly making IO errors easier to debug.
*/
static int winGetLastError(sqlite3_vfs *pVfs, int nBuf, char *zBuf){
  UNUSED_PARAMETER(pVfs);
  return getLastErrorMsg(nBuf, zBuf);
}



/*
** Initialize and deinitialize the operating system interface.
*/
int sqlite3_os_init(void){
  static sqlite3_vfs winVfs = {
    2,                   /* iVersion */
    sizeof(winFile),     /* szOsFile */
    MAX_PATH,            /* mxPathname */
    0,                   /* pNext */
    "win32",             /* zName */
    0,                   /* pAppData */
    winOpen,             /* xOpen */
    winDelete,           /* xDelete */
    winAccess,           /* xAccess */
    winFullPathname,     /* xFullPathname */
    winDlOpen,           /* xDlOpen */
    winDlError,          /* xDlError */
    winDlSym,            /* xDlSym */
    winDlClose,          /* xDlClose */
    winRandomness,       /* xRandomness */
    winSleep,            /* xSleep */
    winCurrentTime,      /* xCurrentTime */
    winGetLastError,     /* xGetLastError */
    0,                   /* xRename */
    winCurrentTimeInt64, /* xCurrentTimeInt64 */
  };

  sqlite3_vfs_register(&winVfs, 1);
  return SQLITE_OK; 
}
int sqlite3_os_end(void){ 
  return SQLITE_OK;
}

#endif /* SQLITE_OS_WIN */

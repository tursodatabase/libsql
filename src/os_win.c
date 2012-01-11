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
** This file contains code that is specific to Windows.
*/
#include "sqliteInt.h"
#if SQLITE_OS_WIN               /* This file is used for Windows only */

#ifdef __CYGWIN__
# include <sys/cygwin.h>
#endif

/*
** Include code that is common to all os_*.c files
*/
#include "os_common.h"

/*
** Some Microsoft compilers lack this definition.
*/
#ifndef INVALID_FILE_ATTRIBUTES
# define INVALID_FILE_ATTRIBUTES ((DWORD)-1) 
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
  u8 locktype;            /* Type of lock currently held on this file */
  short sharedLockByte;   /* Randomly chosen byte used as a shared lock */
  u8 ctrlFlags;           /* Flags.  See WINFILE_* below */
  DWORD lastErrno;        /* The Windows errno from the last I/O error */
  winShm *pShm;           /* Instance of shared memory on this file */
  const char *zPath;      /* Full pathname of this file */
  int szChunk;            /* Chunk size configured by FCNTL_CHUNK_SIZE */
#if SQLITE_OS_WINCE
  LPWSTR zDeleteOnClose;  /* Name of file to delete when closing */
  HANDLE hMutex;          /* Mutex used to control access to shared lock */  
  HANDLE hShared;         /* Shared memory segment used for locking */
  winceLock local;        /* Locks obtained by this instance of winFile */
  winceLock *shared;      /* Global shared lock memory for the file  */
#endif
};

/*
** Allowed values for winFile.ctrlFlags
*/
#define WINFILE_PERSIST_WAL     0x04   /* Persistent WAL mode */
#define WINFILE_PSOW            0x10   /* SQLITE_IOCAP_POWERSAFE_OVERWRITE */

/*
 * If compiled with SQLITE_WIN32_MALLOC on Windows, we will use the
 * various Win32 API heap functions instead of our own.
 */
#ifdef SQLITE_WIN32_MALLOC
/*
 * The initial size of the Win32-specific heap.  This value may be zero.
 */
#ifndef SQLITE_WIN32_HEAP_INIT_SIZE
#  define SQLITE_WIN32_HEAP_INIT_SIZE ((SQLITE_DEFAULT_CACHE_SIZE) * \
                                       (SQLITE_DEFAULT_PAGE_SIZE) + 4194304)
#endif

/*
 * The maximum size of the Win32-specific heap.  This value may be zero.
 */
#ifndef SQLITE_WIN32_HEAP_MAX_SIZE
#  define SQLITE_WIN32_HEAP_MAX_SIZE  (0)
#endif

/*
 * The extra flags to use in calls to the Win32 heap APIs.  This value may be
 * zero for the default behavior.
 */
#ifndef SQLITE_WIN32_HEAP_FLAGS
#  define SQLITE_WIN32_HEAP_FLAGS     (0)
#endif

/*
** The winMemData structure stores information required by the Win32-specific
** sqlite3_mem_methods implementation.
*/
typedef struct winMemData winMemData;
struct winMemData {
#ifndef NDEBUG
  u32 magic;    /* Magic number to detect structure corruption. */
#endif
  HANDLE hHeap; /* The handle to our heap. */
  BOOL bOwned;  /* Do we own the heap (i.e. destroy it on shutdown)? */
};

#ifndef NDEBUG
#define WINMEM_MAGIC     0x42b2830b
#endif

static struct winMemData win_mem_data = {
#ifndef NDEBUG
  WINMEM_MAGIC,
#endif
  NULL, FALSE
};

#ifndef NDEBUG
#define winMemAssertMagic() assert( win_mem_data.magic==WINMEM_MAGIC )
#else
#define winMemAssertMagic()
#endif

#define winMemGetHeap() win_mem_data.hHeap

static void *winMemMalloc(int nBytes);
static void winMemFree(void *pPrior);
static void *winMemRealloc(void *pPrior, int nBytes);
static int winMemSize(void *p);
static int winMemRoundup(int n);
static int winMemInit(void *pAppData);
static void winMemShutdown(void *pAppData);

const sqlite3_mem_methods *sqlite3MemGetWin32(void);
#endif /* SQLITE_WIN32_MALLOC */

/*
** The following variable is (normally) set once and never changes
** thereafter.  It records whether the operating system is Win9x
** or WinNT.
**
** 0:   Operating system unknown.
** 1:   Operating system is Win9x.
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
** Many system calls are accessed through pointer-to-functions so that
** they may be overridden at runtime to facilitate fault injection during
** testing and sandboxing.  The following array holds the names and pointers
** to all overrideable system calls.
*/
#if !SQLITE_OS_WINCE
#  define SQLITE_WIN32_HAS_ANSI
#endif

#if SQLITE_OS_WINCE || SQLITE_OS_WINNT
#  define SQLITE_WIN32_HAS_WIDE
#endif

#ifndef SYSCALL
#  define SYSCALL sqlite3_syscall_ptr
#endif

#if SQLITE_OS_WINCE
/*
** These macros are necessary because Windows CE does not natively support the
** Win32 APIs LockFile, UnlockFile, and LockFileEx.
 */

#  define LockFile(a,b,c,d,e)       winceLockFile(&a, b, c, d, e)
#  define UnlockFile(a,b,c,d,e)     winceUnlockFile(&a, b, c, d, e)
#  define LockFileEx(a,b,c,d,e,f)   winceLockFileEx(&a, b, c, d, e, f)

/*
** These are the special syscall hacks for Windows CE.  The locking related
** defines here refer to the macros defined just above.
 */

#  define osAreFileApisANSI()       1
#  define osLockFile                LockFile
#  define osUnlockFile              UnlockFile
#  define osLockFileEx              LockFileEx
#endif

static struct win_syscall {
  const char *zName;            /* Name of the sytem call */
  sqlite3_syscall_ptr pCurrent; /* Current value of the system call */
  sqlite3_syscall_ptr pDefault; /* Default value */
} aSyscall[] = {
#if !SQLITE_OS_WINCE
  { "AreFileApisANSI",         (SYSCALL)AreFileApisANSI,         0 },

#define osAreFileApisANSI ((BOOL(WINAPI*)(VOID))aSyscall[0].pCurrent)
#else
  { "AreFileApisANSI",         (SYSCALL)0,                       0 },
#endif

#if SQLITE_OS_WINCE && defined(SQLITE_WIN32_HAS_WIDE)
  { "CharLowerW",              (SYSCALL)CharLowerW,              0 },
#else
  { "CharLowerW",              (SYSCALL)0,                       0 },
#endif

#define osCharLowerW ((LPWSTR(WINAPI*)(LPWSTR))aSyscall[1].pCurrent)

#if SQLITE_OS_WINCE && defined(SQLITE_WIN32_HAS_WIDE)
  { "CharUpperW",              (SYSCALL)CharUpperW,              0 },
#else
  { "CharUpperW",              (SYSCALL)0,                       0 },
#endif

#define osCharUpperW ((LPWSTR(WINAPI*)(LPWSTR))aSyscall[2].pCurrent)

  { "CloseHandle",             (SYSCALL)CloseHandle,             0 },

#define osCloseHandle ((BOOL(WINAPI*)(HANDLE))aSyscall[3].pCurrent)

#if defined(SQLITE_WIN32_HAS_ANSI)
  { "CreateFileA",             (SYSCALL)CreateFileA,             0 },
#else
  { "CreateFileA",             (SYSCALL)0,                       0 },
#endif

#define osCreateFileA ((HANDLE(WINAPI*)(LPCSTR,DWORD,DWORD, \
        LPSECURITY_ATTRIBUTES,DWORD,DWORD,HANDLE))aSyscall[4].pCurrent)

#if defined(SQLITE_WIN32_HAS_WIDE)
  { "CreateFileW",             (SYSCALL)CreateFileW,             0 },
#else
  { "CreateFileW",             (SYSCALL)0,                       0 },
#endif

#define osCreateFileW ((HANDLE(WINAPI*)(LPCWSTR,DWORD,DWORD, \
        LPSECURITY_ATTRIBUTES,DWORD,DWORD,HANDLE))aSyscall[5].pCurrent)

  { "CreateFileMapping",       (SYSCALL)CreateFileMapping,       0 },

#define osCreateFileMapping ((HANDLE(WINAPI*)(HANDLE,LPSECURITY_ATTRIBUTES, \
        DWORD,DWORD,DWORD,LPCTSTR))aSyscall[6].pCurrent)

#if defined(SQLITE_WIN32_HAS_WIDE)
  { "CreateFileMappingW",      (SYSCALL)CreateFileMappingW,      0 },
#else
  { "CreateFileMappingW",      (SYSCALL)0,                       0 },
#endif

#define osCreateFileMappingW ((HANDLE(WINAPI*)(HANDLE,LPSECURITY_ATTRIBUTES, \
        DWORD,DWORD,DWORD,LPCWSTR))aSyscall[7].pCurrent)

#if defined(SQLITE_WIN32_HAS_WIDE)
  { "CreateMutexW",            (SYSCALL)CreateMutexW,            0 },
#else
  { "CreateMutexW",            (SYSCALL)0,                       0 },
#endif

#define osCreateMutexW ((HANDLE(WINAPI*)(LPSECURITY_ATTRIBUTES,BOOL, \
        LPCWSTR))aSyscall[8].pCurrent)

#if defined(SQLITE_WIN32_HAS_ANSI)
  { "DeleteFileA",             (SYSCALL)DeleteFileA,             0 },
#else
  { "DeleteFileA",             (SYSCALL)0,                       0 },
#endif

#define osDeleteFileA ((BOOL(WINAPI*)(LPCSTR))aSyscall[9].pCurrent)

#if defined(SQLITE_WIN32_HAS_WIDE)
  { "DeleteFileW",             (SYSCALL)DeleteFileW,             0 },
#else
  { "DeleteFileW",             (SYSCALL)0,                       0 },
#endif

#define osDeleteFileW ((BOOL(WINAPI*)(LPCWSTR))aSyscall[10].pCurrent)

#if SQLITE_OS_WINCE
  { "FileTimeToLocalFileTime", (SYSCALL)FileTimeToLocalFileTime, 0 },
#else
  { "FileTimeToLocalFileTime", (SYSCALL)0,                       0 },
#endif

#define osFileTimeToLocalFileTime ((BOOL(WINAPI*)(CONST FILETIME*, \
        LPFILETIME))aSyscall[11].pCurrent)

#if SQLITE_OS_WINCE
  { "FileTimeToSystemTime",    (SYSCALL)FileTimeToSystemTime,    0 },
#else
  { "FileTimeToSystemTime",    (SYSCALL)0,                       0 },
#endif

#define osFileTimeToSystemTime ((BOOL(WINAPI*)(CONST FILETIME*, \
        LPSYSTEMTIME))aSyscall[12].pCurrent)

  { "FlushFileBuffers",        (SYSCALL)FlushFileBuffers,        0 },

#define osFlushFileBuffers ((BOOL(WINAPI*)(HANDLE))aSyscall[13].pCurrent)

#if defined(SQLITE_WIN32_HAS_ANSI)
  { "FormatMessageA",          (SYSCALL)FormatMessageA,          0 },
#else
  { "FormatMessageA",          (SYSCALL)0,                       0 },
#endif

#define osFormatMessageA ((DWORD(WINAPI*)(DWORD,LPCVOID,DWORD,DWORD,LPSTR, \
        DWORD,va_list*))aSyscall[14].pCurrent)

#if defined(SQLITE_WIN32_HAS_WIDE)
  { "FormatMessageW",          (SYSCALL)FormatMessageW,          0 },
#else
  { "FormatMessageW",          (SYSCALL)0,                       0 },
#endif

#define osFormatMessageW ((DWORD(WINAPI*)(DWORD,LPCVOID,DWORD,DWORD,LPWSTR, \
        DWORD,va_list*))aSyscall[15].pCurrent)

  { "FreeLibrary",             (SYSCALL)FreeLibrary,             0 },

#define osFreeLibrary ((BOOL(WINAPI*)(HMODULE))aSyscall[16].pCurrent)

  { "GetCurrentProcessId",     (SYSCALL)GetCurrentProcessId,     0 },

#define osGetCurrentProcessId ((DWORD(WINAPI*)(VOID))aSyscall[17].pCurrent)

#if !SQLITE_OS_WINCE && defined(SQLITE_WIN32_HAS_ANSI)
  { "GetDiskFreeSpaceA",       (SYSCALL)GetDiskFreeSpaceA,       0 },
#else
  { "GetDiskFreeSpaceA",       (SYSCALL)0,                       0 },
#endif

#define osGetDiskFreeSpaceA ((BOOL(WINAPI*)(LPCSTR,LPDWORD,LPDWORD,LPDWORD, \
        LPDWORD))aSyscall[18].pCurrent)

#if !SQLITE_OS_WINCE && defined(SQLITE_WIN32_HAS_WIDE)
  { "GetDiskFreeSpaceW",       (SYSCALL)GetDiskFreeSpaceW,       0 },
#else
  { "GetDiskFreeSpaceW",       (SYSCALL)0,                       0 },
#endif

#define osGetDiskFreeSpaceW ((BOOL(WINAPI*)(LPCWSTR,LPDWORD,LPDWORD,LPDWORD, \
        LPDWORD))aSyscall[19].pCurrent)

#if defined(SQLITE_WIN32_HAS_ANSI)
  { "GetFileAttributesA",      (SYSCALL)GetFileAttributesA,      0 },
#else
  { "GetFileAttributesA",      (SYSCALL)0,                       0 },
#endif

#define osGetFileAttributesA ((DWORD(WINAPI*)(LPCSTR))aSyscall[20].pCurrent)

#if defined(SQLITE_WIN32_HAS_WIDE)
  { "GetFileAttributesW",      (SYSCALL)GetFileAttributesW,      0 },
#else
  { "GetFileAttributesW",      (SYSCALL)0,                       0 },
#endif

#define osGetFileAttributesW ((DWORD(WINAPI*)(LPCWSTR))aSyscall[21].pCurrent)

#if defined(SQLITE_WIN32_HAS_WIDE)
  { "GetFileAttributesExW",    (SYSCALL)GetFileAttributesExW,    0 },
#else
  { "GetFileAttributesExW",    (SYSCALL)0,                       0 },
#endif

#define osGetFileAttributesExW ((BOOL(WINAPI*)(LPCWSTR,GET_FILEEX_INFO_LEVELS, \
        LPVOID))aSyscall[22].pCurrent)

  { "GetFileSize",             (SYSCALL)GetFileSize,             0 },

#define osGetFileSize ((DWORD(WINAPI*)(HANDLE,LPDWORD))aSyscall[23].pCurrent)

#if !SQLITE_OS_WINCE && defined(SQLITE_WIN32_HAS_ANSI)
  { "GetFullPathNameA",        (SYSCALL)GetFullPathNameA,        0 },
#else
  { "GetFullPathNameA",        (SYSCALL)0,                       0 },
#endif

#define osGetFullPathNameA ((DWORD(WINAPI*)(LPCSTR,DWORD,LPSTR, \
        LPSTR*))aSyscall[24].pCurrent)

#if !SQLITE_OS_WINCE && defined(SQLITE_WIN32_HAS_WIDE)
  { "GetFullPathNameW",        (SYSCALL)GetFullPathNameW,        0 },
#else
  { "GetFullPathNameW",        (SYSCALL)0,                       0 },
#endif

#define osGetFullPathNameW ((DWORD(WINAPI*)(LPCWSTR,DWORD,LPWSTR, \
        LPWSTR*))aSyscall[25].pCurrent)

  { "GetLastError",            (SYSCALL)GetLastError,            0 },

#define osGetLastError ((DWORD(WINAPI*)(VOID))aSyscall[26].pCurrent)

#if SQLITE_OS_WINCE
  /* The GetProcAddressA() routine is only available on Windows CE. */
  { "GetProcAddressA",         (SYSCALL)GetProcAddressA,         0 },
#else
  /* All other Windows platforms expect GetProcAddress() to take
  ** an ANSI string regardless of the _UNICODE setting */
  { "GetProcAddressA",         (SYSCALL)GetProcAddress,          0 },
#endif

#define osGetProcAddressA ((FARPROC(WINAPI*)(HMODULE, \
        LPCSTR))aSyscall[27].pCurrent)

  { "GetSystemInfo",           (SYSCALL)GetSystemInfo,           0 },

#define osGetSystemInfo ((VOID(WINAPI*)(LPSYSTEM_INFO))aSyscall[28].pCurrent)

  { "GetSystemTime",           (SYSCALL)GetSystemTime,           0 },

#define osGetSystemTime ((VOID(WINAPI*)(LPSYSTEMTIME))aSyscall[29].pCurrent)

#if !SQLITE_OS_WINCE
  { "GetSystemTimeAsFileTime", (SYSCALL)GetSystemTimeAsFileTime, 0 },
#else
  { "GetSystemTimeAsFileTime", (SYSCALL)0,                       0 },
#endif

#define osGetSystemTimeAsFileTime ((VOID(WINAPI*)( \
        LPFILETIME))aSyscall[30].pCurrent)

#if defined(SQLITE_WIN32_HAS_ANSI)
  { "GetTempPathA",            (SYSCALL)GetTempPathA,            0 },
#else
  { "GetTempPathA",            (SYSCALL)0,                       0 },
#endif

#define osGetTempPathA ((DWORD(WINAPI*)(DWORD,LPSTR))aSyscall[31].pCurrent)

#if defined(SQLITE_WIN32_HAS_WIDE)
  { "GetTempPathW",            (SYSCALL)GetTempPathW,            0 },
#else
  { "GetTempPathW",            (SYSCALL)0,                       0 },
#endif

#define osGetTempPathW ((DWORD(WINAPI*)(DWORD,LPWSTR))aSyscall[32].pCurrent)

  { "GetTickCount",            (SYSCALL)GetTickCount,            0 },

#define osGetTickCount ((DWORD(WINAPI*)(VOID))aSyscall[33].pCurrent)

#if defined(SQLITE_WIN32_HAS_ANSI)
  { "GetVersionExA",           (SYSCALL)GetVersionExA,           0 },
#else
  { "GetVersionExA",           (SYSCALL)0,                       0 },
#endif

#define osGetVersionExA ((BOOL(WINAPI*)( \
        LPOSVERSIONINFOA))aSyscall[34].pCurrent)

  { "HeapAlloc",               (SYSCALL)HeapAlloc,               0 },

#define osHeapAlloc ((LPVOID(WINAPI*)(HANDLE,DWORD, \
        SIZE_T))aSyscall[35].pCurrent)

  { "HeapCreate",              (SYSCALL)HeapCreate,              0 },

#define osHeapCreate ((HANDLE(WINAPI*)(DWORD,SIZE_T, \
        SIZE_T))aSyscall[36].pCurrent)

  { "HeapDestroy",             (SYSCALL)HeapDestroy,             0 },

#define osHeapDestroy ((BOOL(WINAPI*)(HANDLE))aSyscall[37].pCurrent)

  { "HeapFree",                (SYSCALL)HeapFree,                0 },

#define osHeapFree ((BOOL(WINAPI*)(HANDLE,DWORD,LPVOID))aSyscall[38].pCurrent)

  { "HeapReAlloc",             (SYSCALL)HeapReAlloc,             0 },

#define osHeapReAlloc ((LPVOID(WINAPI*)(HANDLE,DWORD,LPVOID, \
        SIZE_T))aSyscall[39].pCurrent)

  { "HeapSize",                (SYSCALL)HeapSize,                0 },

#define osHeapSize ((SIZE_T(WINAPI*)(HANDLE,DWORD, \
        LPCVOID))aSyscall[40].pCurrent)

  { "HeapValidate",            (SYSCALL)HeapValidate,            0 },

#define osHeapValidate ((BOOL(WINAPI*)(HANDLE,DWORD, \
        LPCVOID))aSyscall[41].pCurrent)

#if defined(SQLITE_WIN32_HAS_ANSI)
  { "LoadLibraryA",            (SYSCALL)LoadLibraryA,            0 },
#else
  { "LoadLibraryA",            (SYSCALL)0,                       0 },
#endif

#define osLoadLibraryA ((HMODULE(WINAPI*)(LPCSTR))aSyscall[42].pCurrent)

#if defined(SQLITE_WIN32_HAS_WIDE)
  { "LoadLibraryW",            (SYSCALL)LoadLibraryW,            0 },
#else
  { "LoadLibraryW",            (SYSCALL)0,                       0 },
#endif

#define osLoadLibraryW ((HMODULE(WINAPI*)(LPCWSTR))aSyscall[43].pCurrent)

  { "LocalFree",               (SYSCALL)LocalFree,               0 },

#define osLocalFree ((HLOCAL(WINAPI*)(HLOCAL))aSyscall[44].pCurrent)

#if !SQLITE_OS_WINCE
  { "LockFile",                (SYSCALL)LockFile,                0 },

#define osLockFile ((BOOL(WINAPI*)(HANDLE,DWORD,DWORD,DWORD, \
        DWORD))aSyscall[45].pCurrent)
#else
  { "LockFile",                (SYSCALL)0,                       0 },
#endif

#if !SQLITE_OS_WINCE
  { "LockFileEx",              (SYSCALL)LockFileEx,              0 },

#define osLockFileEx ((BOOL(WINAPI*)(HANDLE,DWORD,DWORD,DWORD,DWORD, \
        LPOVERLAPPED))aSyscall[46].pCurrent)
#else
  { "LockFileEx",              (SYSCALL)0,                       0 },
#endif

  { "MapViewOfFile",           (SYSCALL)MapViewOfFile,           0 },

#define osMapViewOfFile ((LPVOID(WINAPI*)(HANDLE,DWORD,DWORD,DWORD, \
        SIZE_T))aSyscall[47].pCurrent)

  { "MultiByteToWideChar",     (SYSCALL)MultiByteToWideChar,     0 },

#define osMultiByteToWideChar ((int(WINAPI*)(UINT,DWORD,LPCSTR,int,LPWSTR, \
        int))aSyscall[48].pCurrent)

  { "QueryPerformanceCounter", (SYSCALL)QueryPerformanceCounter, 0 },

#define osQueryPerformanceCounter ((BOOL(WINAPI*)( \
        LARGE_INTEGER*))aSyscall[49].pCurrent)

  { "ReadFile",                (SYSCALL)ReadFile,                0 },

#define osReadFile ((BOOL(WINAPI*)(HANDLE,LPVOID,DWORD,LPDWORD, \
        LPOVERLAPPED))aSyscall[50].pCurrent)

  { "SetEndOfFile",            (SYSCALL)SetEndOfFile,            0 },

#define osSetEndOfFile ((BOOL(WINAPI*)(HANDLE))aSyscall[51].pCurrent)

  { "SetFilePointer",          (SYSCALL)SetFilePointer,          0 },

#define osSetFilePointer ((DWORD(WINAPI*)(HANDLE,LONG,PLONG, \
        DWORD))aSyscall[52].pCurrent)

  { "Sleep",                   (SYSCALL)Sleep,                   0 },

#define osSleep ((VOID(WINAPI*)(DWORD))aSyscall[53].pCurrent)

  { "SystemTimeToFileTime",    (SYSCALL)SystemTimeToFileTime,    0 },

#define osSystemTimeToFileTime ((BOOL(WINAPI*)(CONST SYSTEMTIME*, \
        LPFILETIME))aSyscall[54].pCurrent)

#if !SQLITE_OS_WINCE
  { "UnlockFile",              (SYSCALL)UnlockFile,              0 },

#define osUnlockFile ((BOOL(WINAPI*)(HANDLE,DWORD,DWORD,DWORD, \
        DWORD))aSyscall[55].pCurrent)
#else
  { "UnlockFile",              (SYSCALL)0,                       0 },
#endif

#if !SQLITE_OS_WINCE
  { "UnlockFileEx",            (SYSCALL)UnlockFileEx,            0 },

#define osUnlockFileEx ((BOOL(WINAPI*)(HANDLE,DWORD,DWORD,DWORD, \
        LPOVERLAPPED))aSyscall[56].pCurrent)
#else
  { "UnlockFileEx",            (SYSCALL)0,                       0 },
#endif

  { "UnmapViewOfFile",         (SYSCALL)UnmapViewOfFile,         0 },

#define osUnmapViewOfFile ((BOOL(WINAPI*)(LPCVOID))aSyscall[57].pCurrent)

  { "WideCharToMultiByte",     (SYSCALL)WideCharToMultiByte,     0 },

#define osWideCharToMultiByte ((int(WINAPI*)(UINT,DWORD,LPCWSTR,int,LPSTR,int, \
        LPCSTR,LPBOOL))aSyscall[58].pCurrent)

  { "WriteFile",               (SYSCALL)WriteFile,               0 },

#define osWriteFile ((BOOL(WINAPI*)(HANDLE,LPCVOID,DWORD,LPDWORD, \
        LPOVERLAPPED))aSyscall[59].pCurrent)

}; /* End of the overrideable system calls */

/*
** This is the xSetSystemCall() method of sqlite3_vfs for all of the
** "win32" VFSes.  Return SQLITE_OK opon successfully updating the
** system call pointer, or SQLITE_NOTFOUND if there is no configurable
** system call named zName.
*/
static int winSetSystemCall(
  sqlite3_vfs *pNotUsed,        /* The VFS pointer.  Not used */
  const char *zName,            /* Name of system call to override */
  sqlite3_syscall_ptr pNewFunc  /* Pointer to new system call value */
){
  unsigned int i;
  int rc = SQLITE_NOTFOUND;

  UNUSED_PARAMETER(pNotUsed);
  if( zName==0 ){
    /* If no zName is given, restore all system calls to their default
    ** settings and return NULL
    */
    rc = SQLITE_OK;
    for(i=0; i<sizeof(aSyscall)/sizeof(aSyscall[0]); i++){
      if( aSyscall[i].pDefault ){
        aSyscall[i].pCurrent = aSyscall[i].pDefault;
      }
    }
  }else{
    /* If zName is specified, operate on only the one system call
    ** specified.
    */
    for(i=0; i<sizeof(aSyscall)/sizeof(aSyscall[0]); i++){
      if( strcmp(zName, aSyscall[i].zName)==0 ){
        if( aSyscall[i].pDefault==0 ){
          aSyscall[i].pDefault = aSyscall[i].pCurrent;
        }
        rc = SQLITE_OK;
        if( pNewFunc==0 ) pNewFunc = aSyscall[i].pDefault;
        aSyscall[i].pCurrent = pNewFunc;
        break;
      }
    }
  }
  return rc;
}

/*
** Return the value of a system call.  Return NULL if zName is not a
** recognized system call name.  NULL is also returned if the system call
** is currently undefined.
*/
static sqlite3_syscall_ptr winGetSystemCall(
  sqlite3_vfs *pNotUsed,
  const char *zName
){
  unsigned int i;

  UNUSED_PARAMETER(pNotUsed);
  for(i=0; i<sizeof(aSyscall)/sizeof(aSyscall[0]); i++){
    if( strcmp(zName, aSyscall[i].zName)==0 ) return aSyscall[i].pCurrent;
  }
  return 0;
}

/*
** Return the name of the first system call after zName.  If zName==NULL
** then return the name of the first system call.  Return NULL if zName
** is the last system call or if zName is not the name of a valid
** system call.
*/
static const char *winNextSystemCall(sqlite3_vfs *p, const char *zName){
  int i = -1;

  UNUSED_PARAMETER(p);
  if( zName ){
    for(i=0; i<ArraySize(aSyscall)-1; i++){
      if( strcmp(zName, aSyscall[i].zName)==0 ) break;
    }
  }
  for(i++; i<ArraySize(aSyscall); i++){
    if( aSyscall[i].pCurrent!=0 ) return aSyscall[i].zName;
  }
  return 0;
}

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
      OSVERSIONINFOA sInfo;
      sInfo.dwOSVersionInfoSize = sizeof(sInfo);
      osGetVersionExA(&sInfo);
      sqlite3_os_type = sInfo.dwPlatformId==VER_PLATFORM_WIN32_NT ? 2 : 1;
    }
    return sqlite3_os_type==2;
  }
#endif /* SQLITE_OS_WINCE */

#ifdef SQLITE_WIN32_MALLOC
/*
** Allocate nBytes of memory.
*/
static void *winMemMalloc(int nBytes){
  HANDLE hHeap;
  void *p;

  winMemAssertMagic();
  hHeap = winMemGetHeap();
  assert( hHeap!=0 );
  assert( hHeap!=INVALID_HANDLE_VALUE );
#ifdef SQLITE_WIN32_MALLOC_VALIDATE
  assert ( osHeapValidate(hHeap, SQLITE_WIN32_HEAP_FLAGS, NULL) );
#endif
  assert( nBytes>=0 );
  p = osHeapAlloc(hHeap, SQLITE_WIN32_HEAP_FLAGS, (SIZE_T)nBytes);
  if( !p ){
    sqlite3_log(SQLITE_NOMEM, "failed to HeapAlloc %u bytes (%d), heap=%p",
                nBytes, osGetLastError(), (void*)hHeap);
  }
  return p;
}

/*
** Free memory.
*/
static void winMemFree(void *pPrior){
  HANDLE hHeap;

  winMemAssertMagic();
  hHeap = winMemGetHeap();
  assert( hHeap!=0 );
  assert( hHeap!=INVALID_HANDLE_VALUE );
#ifdef SQLITE_WIN32_MALLOC_VALIDATE
  assert ( osHeapValidate(hHeap, SQLITE_WIN32_HEAP_FLAGS, pPrior) );
#endif
  if( !pPrior ) return; /* Passing NULL to HeapFree is undefined. */
  if( !osHeapFree(hHeap, SQLITE_WIN32_HEAP_FLAGS, pPrior) ){
    sqlite3_log(SQLITE_NOMEM, "failed to HeapFree block %p (%d), heap=%p",
                pPrior, osGetLastError(), (void*)hHeap);
  }
}

/*
** Change the size of an existing memory allocation
*/
static void *winMemRealloc(void *pPrior, int nBytes){
  HANDLE hHeap;
  void *p;

  winMemAssertMagic();
  hHeap = winMemGetHeap();
  assert( hHeap!=0 );
  assert( hHeap!=INVALID_HANDLE_VALUE );
#ifdef SQLITE_WIN32_MALLOC_VALIDATE
  assert ( osHeapValidate(hHeap, SQLITE_WIN32_HEAP_FLAGS, pPrior) );
#endif
  assert( nBytes>=0 );
  if( !pPrior ){
    p = osHeapAlloc(hHeap, SQLITE_WIN32_HEAP_FLAGS, (SIZE_T)nBytes);
  }else{
    p = osHeapReAlloc(hHeap, SQLITE_WIN32_HEAP_FLAGS, pPrior, (SIZE_T)nBytes);
  }
  if( !p ){
    sqlite3_log(SQLITE_NOMEM, "failed to %s %u bytes (%d), heap=%p",
                pPrior ? "HeapReAlloc" : "HeapAlloc", nBytes, osGetLastError(),
                (void*)hHeap);
  }
  return p;
}

/*
** Return the size of an outstanding allocation, in bytes.
*/
static int winMemSize(void *p){
  HANDLE hHeap;
  SIZE_T n;

  winMemAssertMagic();
  hHeap = winMemGetHeap();
  assert( hHeap!=0 );
  assert( hHeap!=INVALID_HANDLE_VALUE );
#ifdef SQLITE_WIN32_MALLOC_VALIDATE
  assert ( osHeapValidate(hHeap, SQLITE_WIN32_HEAP_FLAGS, NULL) );
#endif
  if( !p ) return 0;
  n = osHeapSize(hHeap, SQLITE_WIN32_HEAP_FLAGS, p);
  if( n==(SIZE_T)-1 ){
    sqlite3_log(SQLITE_NOMEM, "failed to HeapSize block %p (%d), heap=%p",
                p, osGetLastError(), (void*)hHeap);
    return 0;
  }
  return (int)n;
}

/*
** Round up a request size to the next valid allocation size.
*/
static int winMemRoundup(int n){
  return n;
}

/*
** Initialize this module.
*/
static int winMemInit(void *pAppData){
  winMemData *pWinMemData = (winMemData *)pAppData;

  if( !pWinMemData ) return SQLITE_ERROR;
  assert( pWinMemData->magic==WINMEM_MAGIC );
  if( !pWinMemData->hHeap ){
    pWinMemData->hHeap = osHeapCreate(SQLITE_WIN32_HEAP_FLAGS,
                                      SQLITE_WIN32_HEAP_INIT_SIZE,
                                      SQLITE_WIN32_HEAP_MAX_SIZE);
    if( !pWinMemData->hHeap ){
      sqlite3_log(SQLITE_NOMEM,
          "failed to HeapCreate (%d), flags=%u, initSize=%u, maxSize=%u",
          osGetLastError(), SQLITE_WIN32_HEAP_FLAGS,
          SQLITE_WIN32_HEAP_INIT_SIZE, SQLITE_WIN32_HEAP_MAX_SIZE);
      return SQLITE_NOMEM;
    }
    pWinMemData->bOwned = TRUE;
  }
  assert( pWinMemData->hHeap!=0 );
  assert( pWinMemData->hHeap!=INVALID_HANDLE_VALUE );
#ifdef SQLITE_WIN32_MALLOC_VALIDATE
  assert( osHeapValidate(pWinMemData->hHeap, SQLITE_WIN32_HEAP_FLAGS, NULL) );
#endif
  return SQLITE_OK;
}

/*
** Deinitialize this module.
*/
static void winMemShutdown(void *pAppData){
  winMemData *pWinMemData = (winMemData *)pAppData;

  if( !pWinMemData ) return;
  if( pWinMemData->hHeap ){
    assert( pWinMemData->hHeap!=INVALID_HANDLE_VALUE );
#ifdef SQLITE_WIN32_MALLOC_VALIDATE
    assert( osHeapValidate(pWinMemData->hHeap, SQLITE_WIN32_HEAP_FLAGS, NULL) );
#endif
    if( pWinMemData->bOwned ){
      if( !osHeapDestroy(pWinMemData->hHeap) ){
        sqlite3_log(SQLITE_NOMEM, "failed to HeapDestroy (%d), heap=%p",
                    osGetLastError(), (void*)pWinMemData->hHeap);
      }
      pWinMemData->bOwned = FALSE;
    }
    pWinMemData->hHeap = NULL;
  }
}

/*
** Populate the low-level memory allocation function pointers in
** sqlite3GlobalConfig.m with pointers to the routines in this file. The
** arguments specify the block of memory to manage.
**
** This routine is only called by sqlite3_config(), and therefore
** is not required to be threadsafe (it is not).
*/
const sqlite3_mem_methods *sqlite3MemGetWin32(void){
  static const sqlite3_mem_methods winMemMethods = {
    winMemMalloc,
    winMemFree,
    winMemRealloc,
    winMemSize,
    winMemRoundup,
    winMemInit,
    winMemShutdown,
    &win_mem_data
  };
  return &winMemMethods;
}

void sqlite3MemSetDefault(void){
  sqlite3_config(SQLITE_CONFIG_MALLOC, sqlite3MemGetWin32());
}
#endif /* SQLITE_WIN32_MALLOC */

/*
** Convert a UTF-8 string to Microsoft Unicode (UTF-16?). 
**
** Space to hold the returned string is obtained from malloc.
*/
static LPWSTR utf8ToUnicode(const char *zFilename){
  int nChar;
  LPWSTR zWideFilename;

  nChar = osMultiByteToWideChar(CP_UTF8, 0, zFilename, -1, NULL, 0);
  if( nChar==0 ){
    return 0;
  }
  zWideFilename = sqlite3_malloc( nChar*sizeof(zWideFilename[0]) );
  if( zWideFilename==0 ){
    return 0;
  }
  nChar = osMultiByteToWideChar(CP_UTF8, 0, zFilename, -1, zWideFilename,
                                nChar);
  if( nChar==0 ){
    sqlite3_free(zWideFilename);
    zWideFilename = 0;
  }
  return zWideFilename;
}

/*
** Convert Microsoft Unicode to UTF-8.  Space to hold the returned string is
** obtained from sqlite3_malloc().
*/
static char *unicodeToUtf8(LPCWSTR zWideFilename){
  int nByte;
  char *zFilename;

  nByte = osWideCharToMultiByte(CP_UTF8, 0, zWideFilename, -1, 0, 0, 0, 0);
  if( nByte == 0 ){
    return 0;
  }
  zFilename = sqlite3_malloc( nByte );
  if( zFilename==0 ){
    return 0;
  }
  nByte = osWideCharToMultiByte(CP_UTF8, 0, zWideFilename, -1, zFilename, nByte,
                                0, 0);
  if( nByte == 0 ){
    sqlite3_free(zFilename);
    zFilename = 0;
  }
  return zFilename;
}

/*
** Convert an ANSI string to Microsoft Unicode, based on the
** current codepage settings for file apis.
** 
** Space to hold the returned string is obtained
** from sqlite3_malloc.
*/
static LPWSTR mbcsToUnicode(const char *zFilename){
  int nByte;
  LPWSTR zMbcsFilename;
  int codepage = osAreFileApisANSI() ? CP_ACP : CP_OEMCP;

  nByte = osMultiByteToWideChar(codepage, 0, zFilename, -1, NULL,
                                0)*sizeof(WCHAR);
  if( nByte==0 ){
    return 0;
  }
  zMbcsFilename = sqlite3_malloc( nByte*sizeof(zMbcsFilename[0]) );
  if( zMbcsFilename==0 ){
    return 0;
  }
  nByte = osMultiByteToWideChar(codepage, 0, zFilename, -1, zMbcsFilename,
                                nByte);
  if( nByte==0 ){
    sqlite3_free(zMbcsFilename);
    zMbcsFilename = 0;
  }
  return zMbcsFilename;
}

/*
** Convert Microsoft Unicode to multi-byte character string, based on the
** user's ANSI codepage.
**
** Space to hold the returned string is obtained from
** sqlite3_malloc().
*/
static char *unicodeToMbcs(LPCWSTR zWideFilename){
  int nByte;
  char *zFilename;
  int codepage = osAreFileApisANSI() ? CP_ACP : CP_OEMCP;

  nByte = osWideCharToMultiByte(codepage, 0, zWideFilename, -1, 0, 0, 0, 0);
  if( nByte == 0 ){
    return 0;
  }
  zFilename = sqlite3_malloc( nByte );
  if( zFilename==0 ){
    return 0;
  }
  nByte = osWideCharToMultiByte(codepage, 0, zWideFilename, -1, zFilename,
                                nByte, 0, 0);
  if( nByte == 0 ){
    sqlite3_free(zFilename);
    zFilename = 0;
  }
  return zFilename;
}

/*
** Convert multibyte character string to UTF-8.  Space to hold the
** returned string is obtained from sqlite3_malloc().
*/
char *sqlite3_win32_mbcs_to_utf8(const char *zFilename){
  char *zFilenameUtf8;
  LPWSTR zTmpWide;

  zTmpWide = mbcsToUnicode(zFilename);
  if( zTmpWide==0 ){
    return 0;
  }
  zFilenameUtf8 = unicodeToUtf8(zTmpWide);
  sqlite3_free(zTmpWide);
  return zFilenameUtf8;
}

/*
** Convert UTF-8 to multibyte character string.  Space to hold the 
** returned string is obtained from sqlite3_malloc().
*/
char *sqlite3_win32_utf8_to_mbcs(const char *zFilename){
  char *zFilenameMbcs;
  LPWSTR zTmpWide;

  zTmpWide = utf8ToUnicode(zFilename);
  if( zTmpWide==0 ){
    return 0;
  }
  zFilenameMbcs = unicodeToMbcs(zTmpWide);
  sqlite3_free(zTmpWide);
  return zFilenameMbcs;
}


/*
** The return value of getLastErrorMsg
** is zero if the error message fits in the buffer, or non-zero
** otherwise (if the message was truncated).
*/
static int getLastErrorMsg(DWORD lastErrno, int nBuf, char *zBuf){
  /* FormatMessage returns 0 on failure.  Otherwise it
  ** returns the number of TCHARs written to the output
  ** buffer, excluding the terminating null char.
  */
  DWORD dwLen = 0;
  char *zOut = 0;

  if( isNT() ){
    LPWSTR zTempWide = NULL;
    dwLen = osFormatMessageW(FORMAT_MESSAGE_ALLOCATE_BUFFER |
                             FORMAT_MESSAGE_FROM_SYSTEM |
                             FORMAT_MESSAGE_IGNORE_INSERTS,
                             NULL,
                             lastErrno,
                             0,
                             (LPWSTR) &zTempWide,
                             0,
                             0);
    if( dwLen > 0 ){
      /* allocate a buffer and convert to UTF8 */
      sqlite3BeginBenignMalloc();
      zOut = unicodeToUtf8(zTempWide);
      sqlite3EndBenignMalloc();
      /* free the system buffer allocated by FormatMessage */
      osLocalFree(zTempWide);
    }
/* isNT() is 1 if SQLITE_OS_WINCE==1, so this else is never executed. 
** Since the ANSI version of these Windows API do not exist for WINCE,
** it's important to not reference them for WINCE builds.
*/
#if SQLITE_OS_WINCE==0
  }else{
    char *zTemp = NULL;
    dwLen = osFormatMessageA(FORMAT_MESSAGE_ALLOCATE_BUFFER |
                             FORMAT_MESSAGE_FROM_SYSTEM |
                             FORMAT_MESSAGE_IGNORE_INSERTS,
                             NULL,
                             lastErrno,
                             0,
                             (LPSTR) &zTemp,
                             0,
                             0);
    if( dwLen > 0 ){
      /* allocate a buffer and convert to UTF8 */
      sqlite3BeginBenignMalloc();
      zOut = sqlite3_win32_mbcs_to_utf8(zTemp);
      sqlite3EndBenignMalloc();
      /* free the system buffer allocated by FormatMessage */
      osLocalFree(zTemp);
    }
#endif
  }
  if( 0 == dwLen ){
    sqlite3_snprintf(nBuf, zBuf, "OsError 0x%x (%u)", lastErrno, lastErrno);
  }else{
    /* copy a maximum of nBuf chars to output buffer */
    sqlite3_snprintf(nBuf, zBuf, "%s", zOut);
    /* free the UTF8 buffer */
    sqlite3_free(zOut);
  }
  return 0;
}

/*
**
** This function - winLogErrorAtLine() - is only ever called via the macro
** winLogError().
**
** This routine is invoked after an error occurs in an OS function.
** It logs a message using sqlite3_log() containing the current value of
** error code and, if possible, the human-readable equivalent from 
** FormatMessage.
**
** The first argument passed to the macro should be the error code that
** will be returned to SQLite (e.g. SQLITE_IOERR_DELETE, SQLITE_CANTOPEN). 
** The two subsequent arguments should be the name of the OS function that
** failed and the the associated file-system path, if any.
*/
#define winLogError(a,b,c,d)   winLogErrorAtLine(a,b,c,d,__LINE__)
static int winLogErrorAtLine(
  int errcode,                    /* SQLite error code */
  DWORD lastErrno,                /* Win32 last error */
  const char *zFunc,              /* Name of OS function that failed */
  const char *zPath,              /* File path associated with error */
  int iLine                       /* Source line number where error occurred */
){
  char zMsg[500];                 /* Human readable error text */
  int i;                          /* Loop counter */

  zMsg[0] = 0;
  getLastErrorMsg(lastErrno, sizeof(zMsg), zMsg);
  assert( errcode!=SQLITE_OK );
  if( zPath==0 ) zPath = "";
  for(i=0; zMsg[i] && zMsg[i]!='\r' && zMsg[i]!='\n'; i++){}
  zMsg[i] = 0;
  sqlite3_log(errcode,
      "os_win.c:%d: (%d) %s(%s) - %s",
      iLine, lastErrno, zFunc, zPath, zMsg
  );

  return errcode;
}

/*
** The number of times that a ReadFile(), WriteFile(), and DeleteFile()
** will be retried following a locking error - probably caused by 
** antivirus software.  Also the initial delay before the first retry.
** The delay increases linearly with each retry.
*/
#ifndef SQLITE_WIN32_IOERR_RETRY
# define SQLITE_WIN32_IOERR_RETRY 10
#endif
#ifndef SQLITE_WIN32_IOERR_RETRY_DELAY
# define SQLITE_WIN32_IOERR_RETRY_DELAY 25
#endif
static int win32IoerrRetry = SQLITE_WIN32_IOERR_RETRY;
static int win32IoerrRetryDelay = SQLITE_WIN32_IOERR_RETRY_DELAY;

/*
** If a ReadFile() or WriteFile() error occurs, invoke this routine
** to see if it should be retried.  Return TRUE to retry.  Return FALSE
** to give up with an error.
*/
static int retryIoerr(int *pnRetry, DWORD *pError){
  DWORD e = osGetLastError();
  if( *pnRetry>=win32IoerrRetry ){
    if( pError ){
      *pError = e;
    }
    return 0;
  }
  if( e==ERROR_ACCESS_DENIED ||
      e==ERROR_LOCK_VIOLATION ||
      e==ERROR_SHARING_VIOLATION ){
    osSleep(win32IoerrRetryDelay*(1+*pnRetry));
    ++*pnRetry;
    return 1;
  }
  if( pError ){
    *pError = e;
  }
  return 0;
}

/*
** Log a I/O error retry episode.
*/
static void logIoerr(int nRetry){
  if( nRetry ){
    sqlite3_log(SQLITE_IOERR, 
      "delayed %dms for lock/sharing conflict",
      win32IoerrRetryDelay*nRetry*(nRetry+1)/2
    );
  }
}

#if SQLITE_OS_WINCE
/*************************************************************************
** This section contains code for WinCE only.
*/
/*
** Windows CE does not have a localtime() function.  So create a
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
  osFileTimeToLocalFileTime(&uTm,&lTm);
  osFileTimeToSystemTime(&lTm,&pTm);
  y.tm_year = pTm.wYear - 1900;
  y.tm_mon = pTm.wMonth - 1;
  y.tm_wday = pTm.wDayOfWeek;
  y.tm_mday = pTm.wDay;
  y.tm_hour = pTm.wHour;
  y.tm_min = pTm.wMinute;
  y.tm_sec = pTm.wSecond;
  return &y;
}

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
  LPWSTR zTok;
  LPWSTR zName;
  BOOL bInit = TRUE;

  zName = utf8ToUnicode(zFilename);
  if( zName==0 ){
    /* out of memory */
    return FALSE;
  }

  /* Initialize the local lockdata */
  memset(&pFile->local, 0, sizeof(pFile->local));

  /* Replace the backslashes from the filename and lowercase it
  ** to derive a mutex name. */
  zTok = osCharLowerW(zName);
  for (;*zTok;zTok++){
    if (*zTok == '\\') *zTok = '_';
  }

  /* Create/open the named mutex */
  pFile->hMutex = osCreateMutexW(NULL, FALSE, zName);
  if (!pFile->hMutex){
    pFile->lastErrno = osGetLastError();
    winLogError(SQLITE_ERROR, pFile->lastErrno, "winceCreateLock1", zFilename);
    sqlite3_free(zName);
    return FALSE;
  }

  /* Acquire the mutex before continuing */
  winceMutexAcquire(pFile->hMutex);
  
  /* Since the names of named mutexes, semaphores, file mappings etc are 
  ** case-sensitive, take advantage of that by uppercasing the mutex name
  ** and using that as the shared filemapping name.
  */
  osCharUpperW(zName);
  pFile->hShared = osCreateFileMappingW(INVALID_HANDLE_VALUE, NULL,
                                        PAGE_READWRITE, 0, sizeof(winceLock),
                                        zName);  

  /* Set a flag that indicates we're the first to create the memory so it 
  ** must be zero-initialized */
  if (osGetLastError() == ERROR_ALREADY_EXISTS){
    bInit = FALSE;
  }

  sqlite3_free(zName);

  /* If we succeeded in making the shared memory handle, map it. */
  if (pFile->hShared){
    pFile->shared = (winceLock*)osMapViewOfFile(pFile->hShared, 
             FILE_MAP_READ|FILE_MAP_WRITE, 0, 0, sizeof(winceLock));
    /* If mapping failed, close the shared memory handle and erase it */
    if (!pFile->shared){
      pFile->lastErrno = osGetLastError();
      winLogError(SQLITE_ERROR, pFile->lastErrno,
               "winceCreateLock2", zFilename);
      osCloseHandle(pFile->hShared);
      pFile->hShared = NULL;
    }
  }

  /* If shared memory could not be created, then close the mutex and fail */
  if (pFile->hShared == NULL){
    winceMutexRelease(pFile->hMutex);
    osCloseHandle(pFile->hMutex);
    pFile->hMutex = NULL;
    return FALSE;
  }
  
  /* Initialize the shared memory if we're supposed to */
  if (bInit) {
    memset(pFile->shared, 0, sizeof(winceLock));
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
    osUnmapViewOfFile(pFile->shared);
    osCloseHandle(pFile->hShared);

    /* Done with the mutex */
    winceMutexRelease(pFile->hMutex);    
    osCloseHandle(pFile->hMutex);
    pFile->hMutex = NULL;
  }
}

/* 
** An implementation of the LockFile() API of Windows for CE
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
** An implementation of the UnlockFile API of Windows for CE
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
** An implementation of the LockFileEx() API of Windows for CE
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
** Some Microsoft compilers lack this definition.
*/
#ifndef INVALID_SET_FILE_POINTER
# define INVALID_SET_FILE_POINTER ((DWORD)-1)
#endif

/*
** Move the current position of the file handle passed as the first 
** argument to offset iOffset within the file. If successful, return 0. 
** Otherwise, set pFile->lastErrno and return non-zero.
*/
static int seekWinFile(winFile *pFile, sqlite3_int64 iOffset){
  LONG upperBits;                 /* Most sig. 32 bits of new offset */
  LONG lowerBits;                 /* Least sig. 32 bits of new offset */
  DWORD dwRet;                    /* Value returned by SetFilePointer() */
  DWORD lastErrno;                /* Value returned by GetLastError() */

  upperBits = (LONG)((iOffset>>32) & 0x7fffffff);
  lowerBits = (LONG)(iOffset & 0xffffffff);

  /* API oddity: If successful, SetFilePointer() returns a dword 
  ** containing the lower 32-bits of the new file-offset. Or, if it fails,
  ** it returns INVALID_SET_FILE_POINTER. However according to MSDN, 
  ** INVALID_SET_FILE_POINTER may also be a valid new offset. So to determine 
  ** whether an error has actually occured, it is also necessary to call 
  ** GetLastError().
  */
  dwRet = osSetFilePointer(pFile->h, lowerBits, &upperBits, FILE_BEGIN);

  if( (dwRet==INVALID_SET_FILE_POINTER
      && ((lastErrno = osGetLastError())!=NO_ERROR)) ){
    pFile->lastErrno = lastErrno;
    winLogError(SQLITE_IOERR_SEEK, pFile->lastErrno,
             "seekWinFile", pFile->zPath);
    return 1;
  }

  return 0;
}

/*
** Close a file.
**
** It is reported that an attempt to close a handle might sometimes
** fail.  This is a very unreasonable result, but Windows is notorious
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
    rc = osCloseHandle(pFile->h);
    /* SimulateIOError( rc=0; cnt=MX_CLOSE_ATTEMPT; ); */
  }while( rc==0 && ++cnt < MX_CLOSE_ATTEMPT && (osSleep(100), 1) );
#if SQLITE_OS_WINCE
#define WINCE_DELETION_ATTEMPTS 3
  winceDestroyLock(pFile);
  if( pFile->zDeleteOnClose ){
    int cnt = 0;
    while(
           osDeleteFileW(pFile->zDeleteOnClose)==0
        && osGetFileAttributesW(pFile->zDeleteOnClose)!=0xffffffff 
        && cnt++ < WINCE_DELETION_ATTEMPTS
    ){
       osSleep(100);  /* Wait a little before trying again */
    }
    sqlite3_free(pFile->zDeleteOnClose);
  }
#endif
  OSTRACE(("CLOSE %d %s\n", pFile->h, rc ? "ok" : "failed"));
  OpenCounter(-1);
  return rc ? SQLITE_OK
            : winLogError(SQLITE_IOERR_CLOSE, osGetLastError(),
                          "winClose", pFile->zPath);
}

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
  winFile *pFile = (winFile*)id;  /* file handle */
  DWORD nRead;                    /* Number of bytes actually read from file */
  int nRetry = 0;                 /* Number of retrys */

  assert( id!=0 );
  SimulateIOError(return SQLITE_IOERR_READ);
  OSTRACE(("READ %d lock=%d\n", pFile->h, pFile->locktype));

  if( seekWinFile(pFile, offset) ){
    return SQLITE_FULL;
  }
  while( !osReadFile(pFile->h, pBuf, amt, &nRead, 0) ){
    DWORD lastErrno;
    if( retryIoerr(&nRetry, &lastErrno) ) continue;
    pFile->lastErrno = lastErrno;
    return winLogError(SQLITE_IOERR_READ, pFile->lastErrno,
             "winRead", pFile->zPath);
  }
  logIoerr(nRetry);
  if( nRead<(DWORD)amt ){
    /* Unread parts of the buffer must be zero-filled */
    memset(&((char*)pBuf)[nRead], 0, amt-nRead);
    return SQLITE_IOERR_SHORT_READ;
  }

  return SQLITE_OK;
}

/*
** Write data from a buffer into a file.  Return SQLITE_OK on success
** or some other error code on failure.
*/
static int winWrite(
  sqlite3_file *id,               /* File to write into */
  const void *pBuf,               /* The bytes to be written */
  int amt,                        /* Number of bytes to write */
  sqlite3_int64 offset            /* Offset into the file to begin writing at */
){
  int rc;                         /* True if error has occured, else false */
  winFile *pFile = (winFile*)id;  /* File handle */
  int nRetry = 0;                 /* Number of retries */

  assert( amt>0 );
  assert( pFile );
  SimulateIOError(return SQLITE_IOERR_WRITE);
  SimulateDiskfullError(return SQLITE_FULL);

  OSTRACE(("WRITE %d lock=%d\n", pFile->h, pFile->locktype));

  rc = seekWinFile(pFile, offset);
  if( rc==0 ){
    u8 *aRem = (u8 *)pBuf;        /* Data yet to be written */
    int nRem = amt;               /* Number of bytes yet to be written */
    DWORD nWrite;                 /* Bytes written by each WriteFile() call */
    DWORD lastErrno = NO_ERROR;   /* Value returned by GetLastError() */

    while( nRem>0 ){
      if( !osWriteFile(pFile->h, aRem, nRem, &nWrite, 0) ){
        if( retryIoerr(&nRetry, &lastErrno) ) continue;
        break;
      }
      if( nWrite<=0 ) break;
      aRem += nWrite;
      nRem -= nWrite;
    }
    if( nRem>0 ){
      pFile->lastErrno = lastErrno;
      rc = 1;
    }
  }

  if( rc ){
    if(   ( pFile->lastErrno==ERROR_HANDLE_DISK_FULL )
       || ( pFile->lastErrno==ERROR_DISK_FULL )){
      return SQLITE_FULL;
    }
    return winLogError(SQLITE_IOERR_WRITE, pFile->lastErrno,
             "winWrite", pFile->zPath);
  }else{
    logIoerr(nRetry);
  }
  return SQLITE_OK;
}

/*
** Truncate an open file to a specified size
*/
static int winTruncate(sqlite3_file *id, sqlite3_int64 nByte){
  winFile *pFile = (winFile*)id;  /* File handle object */
  int rc = SQLITE_OK;             /* Return code for this function */

  assert( pFile );

  OSTRACE(("TRUNCATE %d %lld\n", pFile->h, nByte));
  SimulateIOError(return SQLITE_IOERR_TRUNCATE);

  /* If the user has configured a chunk-size for this file, truncate the
  ** file so that it consists of an integer number of chunks (i.e. the
  ** actual file size after the operation may be larger than the requested
  ** size).
  */
  if( pFile->szChunk>0 ){
    nByte = ((nByte + pFile->szChunk - 1)/pFile->szChunk) * pFile->szChunk;
  }

  /* SetEndOfFile() returns non-zero when successful, or zero when it fails. */
  if( seekWinFile(pFile, nByte) ){
    rc = winLogError(SQLITE_IOERR_TRUNCATE, pFile->lastErrno,
             "winTruncate1", pFile->zPath);
  }else if( 0==osSetEndOfFile(pFile->h) ){
    pFile->lastErrno = osGetLastError();
    rc = winLogError(SQLITE_IOERR_TRUNCATE, pFile->lastErrno,
             "winTruncate2", pFile->zPath);
  }

  OSTRACE(("TRUNCATE %d %lld %s\n", pFile->h, nByte, rc ? "failed" : "ok"));
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
  /*
  ** Used only when SQLITE_NO_SYNC is not defined.
   */
  BOOL rc;
#endif
#if !defined(NDEBUG) || !defined(SQLITE_NO_SYNC) || \
    (defined(SQLITE_TEST) && defined(SQLITE_DEBUG))
  /*
  ** Used when SQLITE_NO_SYNC is not defined and by the assert() and/or
  ** OSTRACE() macros.
   */
  winFile *pFile = (winFile*)id;
#else
  UNUSED_PARAMETER(id);
#endif

  assert( pFile );
  /* Check that one of SQLITE_SYNC_NORMAL or FULL was passed */
  assert((flags&0x0F)==SQLITE_SYNC_NORMAL
      || (flags&0x0F)==SQLITE_SYNC_FULL
  );

  OSTRACE(("SYNC %d lock=%d\n", pFile->h, pFile->locktype));

  /* Unix cannot, but some systems may return SQLITE_FULL from here. This
  ** line is to test that doing so does not cause any problems.
  */
  SimulateDiskfullError( return SQLITE_FULL );

#ifndef SQLITE_TEST
  UNUSED_PARAMETER(flags);
#else
  if( (flags&0x0F)==SQLITE_SYNC_FULL ){
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
  rc = osFlushFileBuffers(pFile->h);
  SimulateIOError( rc=FALSE );
  if( rc ){
    return SQLITE_OK;
  }else{
    pFile->lastErrno = osGetLastError();
    return winLogError(SQLITE_IOERR_FSYNC, pFile->lastErrno,
             "winSync", pFile->zPath);
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
  DWORD lastErrno;

  assert( id!=0 );
  SimulateIOError(return SQLITE_IOERR_FSTAT);
  lowerBits = osGetFileSize(pFile->h, &upperBits);
  if(   (lowerBits == INVALID_FILE_SIZE)
     && ((lastErrno = osGetLastError())!=NO_ERROR) )
  {
    pFile->lastErrno = lastErrno;
    return winLogError(SQLITE_IOERR_FSTAT, pFile->lastErrno,
             "winFileSize", pFile->zPath);
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
** is Win9x or WinNT.
*/
static int getReadLock(winFile *pFile){
  int res;
  if( isNT() ){
    OVERLAPPED ovlp;
    ovlp.Offset = SHARED_FIRST;
    ovlp.OffsetHigh = 0;
    ovlp.hEvent = 0;
    res = osLockFileEx(pFile->h, LOCKFILE_FAIL_IMMEDIATELY,
                       0, SHARED_SIZE, 0, &ovlp);
/* isNT() is 1 if SQLITE_OS_WINCE==1, so this else is never executed. 
*/
#if SQLITE_OS_WINCE==0
  }else{
    int lk;
    sqlite3_randomness(sizeof(lk), &lk);
    pFile->sharedLockByte = (short)((lk & 0x7fffffff)%(SHARED_SIZE - 1));
    res = osLockFile(pFile->h, SHARED_FIRST+pFile->sharedLockByte, 0, 1, 0);
#endif
  }
  if( res == 0 ){
    pFile->lastErrno = osGetLastError();
    /* No need to log a failure to lock */
  }
  return res;
}

/*
** Undo a readlock
*/
static int unlockReadLock(winFile *pFile){
  int res;
  DWORD lastErrno;
  if( isNT() ){
    res = osUnlockFile(pFile->h, SHARED_FIRST, 0, SHARED_SIZE, 0);
/* isNT() is 1 if SQLITE_OS_WINCE==1, so this else is never executed. 
*/
#if SQLITE_OS_WINCE==0
  }else{
    res = osUnlockFile(pFile->h, SHARED_FIRST + pFile->sharedLockByte, 0, 1, 0);
#endif
  }
  if( res==0 && ((lastErrno = osGetLastError())!=ERROR_NOT_LOCKED) ){
    pFile->lastErrno = lastErrno;
    winLogError(SQLITE_IOERR_UNLOCK, pFile->lastErrno,
             "unlockReadLock", pFile->zPath);
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
  int res = 1;           /* Result of a Windows lock call */
  int newLocktype;       /* Set pFile->locktype to this value before exiting */
  int gotPendingLock = 0;/* True if we acquired a PENDING lock this time */
  winFile *pFile = (winFile*)id;
  DWORD lastErrno = NO_ERROR;

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
    while( cnt-->0 && (res = osLockFile(pFile->h, PENDING_BYTE, 0, 1, 0))==0 ){
      /* Try 3 times to get the pending lock.  This is needed to work
      ** around problems caused by indexing and/or anti-virus software on
      ** Windows systems.
      ** If you are using this code as a model for alternative VFSes, do not
      ** copy this retry logic.  It is a hack intended for Windows only.
      */
      OSTRACE(("could not get a PENDING lock. cnt=%d\n", cnt));
      if( cnt ) osSleep(1);
    }
    gotPendingLock = res;
    if( !res ){
      lastErrno = osGetLastError();
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
      lastErrno = osGetLastError();
    }
  }

  /* Acquire a RESERVED lock
  */
  if( locktype==RESERVED_LOCK && res ){
    assert( pFile->locktype==SHARED_LOCK );
    res = osLockFile(pFile->h, RESERVED_BYTE, 0, 1, 0);
    if( res ){
      newLocktype = RESERVED_LOCK;
    }else{
      lastErrno = osGetLastError();
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
    res = osLockFile(pFile->h, SHARED_FIRST, 0, SHARED_SIZE, 0);
    if( res ){
      newLocktype = EXCLUSIVE_LOCK;
    }else{
      lastErrno = osGetLastError();
      OSTRACE(("error-code = %d\n", lastErrno));
      getReadLock(pFile);
    }
  }

  /* If we are holding a PENDING lock that ought to be released, then
  ** release it now.
  */
  if( gotPendingLock && locktype==SHARED_LOCK ){
    osUnlockFile(pFile->h, PENDING_BYTE, 0, 1, 0);
  }

  /* Update the state of the lock has held in the file descriptor then
  ** return the appropriate result code.
  */
  if( res ){
    rc = SQLITE_OK;
  }else{
    OSTRACE(("LOCK FAILED %d trying for %d but got %d\n", pFile->h,
           locktype, newLocktype));
    pFile->lastErrno = lastErrno;
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

  SimulateIOError( return SQLITE_IOERR_CHECKRESERVEDLOCK; );

  assert( id!=0 );
  if( pFile->locktype>=RESERVED_LOCK ){
    rc = 1;
    OSTRACE(("TEST WR-LOCK %d %d (local)\n", pFile->h, rc));
  }else{
    rc = osLockFile(pFile->h, RESERVED_BYTE, 0, 1, 0);
    if( rc ){
      osUnlockFile(pFile->h, RESERVED_BYTE, 0, 1, 0);
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
    osUnlockFile(pFile->h, SHARED_FIRST, 0, SHARED_SIZE, 0);
    if( locktype==SHARED_LOCK && !getReadLock(pFile) ){
      /* This should never happen.  We should always be able to
      ** reacquire the read lock */
      rc = winLogError(SQLITE_IOERR_UNLOCK, osGetLastError(),
               "winUnlock", pFile->zPath);
    }
  }
  if( type>=RESERVED_LOCK ){
    osUnlockFile(pFile->h, RESERVED_BYTE, 0, 1, 0);
  }
  if( locktype==NO_LOCK && type>=SHARED_LOCK ){
    unlockReadLock(pFile);
  }
  if( type>=PENDING_LOCK ){
    osUnlockFile(pFile->h, PENDING_BYTE, 0, 1, 0);
  }
  pFile->locktype = (u8)locktype;
  return rc;
}

/*
** If *pArg is inititially negative then this is a query.  Set *pArg to
** 1 or 0 depending on whether or not bit mask of pFile->ctrlFlags is set.
**
** If *pArg is 0 or 1, then clear or set the mask bit of pFile->ctrlFlags.
*/
static void winModeBit(winFile *pFile, unsigned char mask, int *pArg){
  if( *pArg<0 ){
    *pArg = (pFile->ctrlFlags & mask)!=0;
  }else if( (*pArg)==0 ){
    pFile->ctrlFlags &= ~mask;
  }else{
    pFile->ctrlFlags |= mask;
  }
}

/*
** Control and query of the open file handle.
*/
static int winFileControl(sqlite3_file *id, int op, void *pArg){
  winFile *pFile = (winFile*)id;
  switch( op ){
    case SQLITE_FCNTL_LOCKSTATE: {
      *(int*)pArg = pFile->locktype;
      return SQLITE_OK;
    }
    case SQLITE_LAST_ERRNO: {
      *(int*)pArg = (int)pFile->lastErrno;
      return SQLITE_OK;
    }
    case SQLITE_FCNTL_CHUNK_SIZE: {
      pFile->szChunk = *(int *)pArg;
      return SQLITE_OK;
    }
    case SQLITE_FCNTL_SIZE_HINT: {
      if( pFile->szChunk>0 ){
        sqlite3_int64 oldSz;
        int rc = winFileSize(id, &oldSz);
        if( rc==SQLITE_OK ){
          sqlite3_int64 newSz = *(sqlite3_int64*)pArg;
          if( newSz>oldSz ){
            SimulateIOErrorBenign(1);
            rc = winTruncate(id, newSz);
            SimulateIOErrorBenign(0);
          }
        }
        return rc;
      }
      return SQLITE_OK;
    }
    case SQLITE_FCNTL_PERSIST_WAL: {
      winModeBit(pFile, WINFILE_PERSIST_WAL, (int*)pArg);
      return SQLITE_OK;
    }
    case SQLITE_FCNTL_POWERSAFE_OVERWRITE: {
      winModeBit(pFile, WINFILE_PSOW, (int*)pArg);
      return SQLITE_OK;
    }
    case SQLITE_FCNTL_VFSNAME: {
      *(char**)pArg = sqlite3_mprintf("win32");
      return SQLITE_OK;
    }
    case SQLITE_FCNTL_WIN32_AV_RETRY: {
      int *a = (int*)pArg;
      if( a[0]>0 ){
        win32IoerrRetry = a[0];
      }else{
        a[0] = win32IoerrRetry;
      }
      if( a[1]>0 ){
        win32IoerrRetryDelay = a[1];
      }else{
        a[1] = win32IoerrRetryDelay;
      }
      return SQLITE_OK;
    }
  }
  return SQLITE_NOTFOUND;
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
  (void)id;
  return SQLITE_DEFAULT_SECTOR_SIZE;
}

/*
** Return a vector of device characteristics.
*/
static int winDeviceCharacteristics(sqlite3_file *id){
  winFile *p = (winFile*)id;
  return SQLITE_IOCAP_UNDELETABLE_WHEN_OPEN |
         ((p->ctrlFlags & WINFILE_PSOW)?SQLITE_IOCAP_POWERSAFE_OVERWRITE:0);
}

#ifndef SQLITE_OMIT_WAL

/* 
** Windows will only let you create file view mappings
** on allocation size granularity boundaries.
** During sqlite3_os_init() we do a GetSystemInfo()
** to get the granularity size.
*/
SYSTEM_INFO winSysInfo;

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
**   winShmLeaveMutex()
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
*/
struct winShmNode {
  sqlite3_mutex *mutex;      /* Mutex to access this object */
  char *zFilename;           /* Name of the file */
  winFile hFile;             /* File handle from winOpen */

  int szRegion;              /* Size of shared-memory regions */
  int nRegion;               /* Size of array apRegion */
  struct ShmRegion {
    HANDLE hMap;             /* File handle from CreateFileMapping */
    void *pMap;
  } *aRegion;
  DWORD lastErrno;           /* The Windows errno from the last I/O error */

  int nRef;                  /* Number of winShm objects pointing to this */
  winShm *pFirst;            /* All winShm objects pointing to this */
  winShmNode *pNext;         /* Next in list of all winShmNode objects */
#ifdef SQLITE_DEBUG
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
** The following fields are initialized when this object is created and
** are read-only thereafter:
**
**    winShm.pShmNode
**    winShm.id
**
** All other fields are read/write.  The winShm.pShmNode->mutex must be held
** while accessing any read/write fields.
*/
struct winShm {
  winShmNode *pShmNode;      /* The underlying winShmNode object */
  winShm *pNext;             /* Next winShm with the same winShmNode */
  u8 hasMutex;               /* True if holding the winShmNode mutex */
  u16 sharedMask;            /* Mask of shared locks held */
  u16 exclMask;              /* Mask of exclusive locks held */
#ifdef SQLITE_DEBUG
  u8 id;                     /* Id of this connection with its winShmNode */
#endif
};

/*
** Constants used for locking
*/
#define WIN_SHM_BASE   ((22+SQLITE_SHM_NLOCK)*4)        /* first lock byte */
#define WIN_SHM_DMS    (WIN_SHM_BASE+SQLITE_SHM_NLOCK)  /* deadman switch */

/*
** Apply advisory locks for all n bytes beginning at ofst.
*/
#define _SHM_UNLCK  1
#define _SHM_RDLCK  2
#define _SHM_WRLCK  3
static int winShmSystemLock(
  winShmNode *pFile,    /* Apply locks to this open shared-memory segment */
  int lockType,         /* _SHM_UNLCK, _SHM_RDLCK, or _SHM_WRLCK */
  int ofst,             /* Offset to first byte to be locked/unlocked */
  int nByte             /* Number of bytes to lock or unlock */
){
  OVERLAPPED ovlp;
  DWORD dwFlags;
  int rc = 0;           /* Result code form Lock/UnlockFileEx() */

  /* Access to the winShmNode object is serialized by the caller */
  assert( sqlite3_mutex_held(pFile->mutex) || pFile->nRef==0 );

  /* Initialize the locking parameters */
  dwFlags = LOCKFILE_FAIL_IMMEDIATELY;
  if( lockType == _SHM_WRLCK ) dwFlags |= LOCKFILE_EXCLUSIVE_LOCK;

  memset(&ovlp, 0, sizeof(OVERLAPPED));
  ovlp.Offset = ofst;

  /* Release/Acquire the system-level lock */
  if( lockType==_SHM_UNLCK ){
    rc = osUnlockFileEx(pFile->hFile.h, 0, nByte, 0, &ovlp);
  }else{
    rc = osLockFileEx(pFile->hFile.h, dwFlags, 0, nByte, 0, &ovlp);
  }
  
  if( rc!= 0 ){
    rc = SQLITE_OK;
  }else{
    pFile->lastErrno =  osGetLastError();
    rc = SQLITE_BUSY;
  }

  OSTRACE(("SHM-LOCK %d %s %s 0x%08lx\n", 
           pFile->hFile.h,
           rc==SQLITE_OK ? "ok" : "failed",
           lockType==_SHM_UNLCK ? "UnlockFileEx" : "LockFileEx",
           pFile->lastErrno));

  return rc;
}

/* Forward references to VFS methods */
static int winOpen(sqlite3_vfs*,const char*,sqlite3_file*,int,int*);
static int winDelete(sqlite3_vfs *,const char*,int);

/*
** Purge the winShmNodeList list of all entries with winShmNode.nRef==0.
**
** This is not a VFS shared-memory method; it is a utility function called
** by VFS shared-memory methods.
*/
static void winShmPurge(sqlite3_vfs *pVfs, int deleteFlag){
  winShmNode **pp;
  winShmNode *p;
  BOOL bRc;
  assert( winShmMutexHeld() );
  pp = &winShmNodeList;
  while( (p = *pp)!=0 ){
    if( p->nRef==0 ){
      int i;
      if( p->mutex ) sqlite3_mutex_free(p->mutex);
      for(i=0; i<p->nRegion; i++){
        bRc = osUnmapViewOfFile(p->aRegion[i].pMap);
        OSTRACE(("SHM-PURGE pid-%d unmap region=%d %s\n",
                 (int)osGetCurrentProcessId(), i,
                 bRc ? "ok" : "failed"));
        bRc = osCloseHandle(p->aRegion[i].hMap);
        OSTRACE(("SHM-PURGE pid-%d close region=%d %s\n",
                 (int)osGetCurrentProcessId(), i,
                 bRc ? "ok" : "failed"));
      }
      if( p->hFile.h != INVALID_HANDLE_VALUE ){
        SimulateIOErrorBenign(1);
        winClose((sqlite3_file *)&p->hFile);
        SimulateIOErrorBenign(0);
      }
      if( deleteFlag ){
        SimulateIOErrorBenign(1);
        sqlite3BeginBenignMalloc();
        winDelete(pVfs, p->zFilename, 0);
        sqlite3EndBenignMalloc();
        SimulateIOErrorBenign(0);
      }
      *pp = p->pNext;
      sqlite3_free(p->aRegion);
      sqlite3_free(p);
    }else{
      pp = &p->pNext;
    }
  }
}

/*
** Open the shared-memory area associated with database file pDbFd.
**
** When opening a new shared-memory file, if no other instances of that
** file are currently open, in this process or in other processes, then
** the file must be truncated to zero length or have its header cleared.
*/
static int winOpenSharedMemory(winFile *pDbFd){
  struct winShm *p;                  /* The connection to be opened */
  struct winShmNode *pShmNode = 0;   /* The underlying mmapped file */
  int rc;                            /* Result code */
  struct winShmNode *pNew;           /* Newly allocated winShmNode */
  int nName;                         /* Size of zName in bytes */

  assert( pDbFd->pShm==0 );    /* Not previously opened */

  /* Allocate space for the new sqlite3_shm object.  Also speculatively
  ** allocate space for a new winShmNode and filename.
  */
  p = sqlite3_malloc( sizeof(*p) );
  if( p==0 ) return SQLITE_IOERR_NOMEM;
  memset(p, 0, sizeof(*p));
  nName = sqlite3Strlen30(pDbFd->zPath);
  pNew = sqlite3_malloc( sizeof(*pShmNode) + nName + 17 );
  if( pNew==0 ){
    sqlite3_free(p);
    return SQLITE_IOERR_NOMEM;
  }
  memset(pNew, 0, sizeof(*pNew) + nName + 17);
  pNew->zFilename = (char*)&pNew[1];
  sqlite3_snprintf(nName+15, pNew->zFilename, "%s-shm", pDbFd->zPath);
  sqlite3FileSuffix3(pDbFd->zPath, pNew->zFilename); 

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
    ((winFile*)(&pShmNode->hFile))->h = INVALID_HANDLE_VALUE;
    pShmNode->pNext = winShmNodeList;
    winShmNodeList = pShmNode;

    pShmNode->mutex = sqlite3_mutex_alloc(SQLITE_MUTEX_FAST);
    if( pShmNode->mutex==0 ){
      rc = SQLITE_IOERR_NOMEM;
      goto shm_open_err;
    }

    rc = winOpen(pDbFd->pVfs,
                 pShmNode->zFilename,             /* Name of the file (UTF-8) */
                 (sqlite3_file*)&pShmNode->hFile,  /* File handle here */
                 SQLITE_OPEN_WAL | SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, /* Mode flags */
                 0);
    if( SQLITE_OK!=rc ){
      goto shm_open_err;
    }

    /* Check to see if another process is holding the dead-man switch.
    ** If not, truncate the file to zero length. 
    */
    if( winShmSystemLock(pShmNode, _SHM_WRLCK, WIN_SHM_DMS, 1)==SQLITE_OK ){
      rc = winTruncate((sqlite3_file *)&pShmNode->hFile, 0);
      if( rc!=SQLITE_OK ){
        rc = winLogError(SQLITE_IOERR_SHMOPEN, osGetLastError(),
                 "winOpenShm", pDbFd->zPath);
      }
    }
    if( rc==SQLITE_OK ){
      winShmSystemLock(pShmNode, _SHM_UNLCK, WIN_SHM_DMS, 1);
      rc = winShmSystemLock(pShmNode, _SHM_RDLCK, WIN_SHM_DMS, 1);
    }
    if( rc ) goto shm_open_err;
  }

  /* Make the new connection a child of the winShmNode */
  p->pShmNode = pShmNode;
#ifdef SQLITE_DEBUG
  p->id = pShmNode->nextShmId++;
#endif
  pShmNode->nRef++;
  pDbFd->pShm = p;
  winShmLeaveMutex();

  /* The reference count on pShmNode has already been incremented under
  ** the cover of the winShmEnterMutex() mutex and the pointer from the
  ** new (struct winShm) object to the pShmNode has been set. All that is
  ** left to do is to link the new object into the linked list starting
  ** at pShmNode->pFirst. This must be done while holding the pShmNode->mutex 
  ** mutex.
  */
  sqlite3_mutex_enter(pShmNode->mutex);
  p->pNext = pShmNode->pFirst;
  pShmNode->pFirst = p;
  sqlite3_mutex_leave(pShmNode->mutex);
  return SQLITE_OK;

  /* Jump here on any error */
shm_open_err:
  winShmSystemLock(pShmNode, _SHM_UNLCK, WIN_SHM_DMS, 1);
  winShmPurge(pDbFd->pVfs, 0);      /* This call frees pShmNode if required */
  sqlite3_free(p);
  sqlite3_free(pNew);
  winShmLeaveMutex();
  return rc;
}

/*
** Close a connection to shared-memory.  Delete the underlying 
** storage if deleteFlag is true.
*/
static int winShmUnmap(
  sqlite3_file *fd,          /* Database holding shared memory */
  int deleteFlag             /* Delete after closing if true */
){
  winFile *pDbFd;       /* Database holding shared-memory */
  winShm *p;            /* The connection to be closed */
  winShmNode *pShmNode; /* The underlying shared-memory file */
  winShm **pp;          /* For looping over sibling connections */

  pDbFd = (winFile*)fd;
  p = pDbFd->pShm;
  if( p==0 ) return SQLITE_OK;
  pShmNode = p->pShmNode;

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
    winShmPurge(pDbFd->pVfs, deleteFlag);
  }
  winShmLeaveMutex();

  return SQLITE_OK;
}

/*
** Change the lock state for a shared-memory segment.
*/
static int winShmLock(
  sqlite3_file *fd,          /* Database file holding the shared memory */
  int ofst,                  /* First lock to acquire or release */
  int n,                     /* Number of locks to acquire or release */
  int flags                  /* What to do with the lock */
){
  winFile *pDbFd = (winFile*)fd;        /* Connection holding shared memory */
  winShm *p = pDbFd->pShm;              /* The shared memory being locked */
  winShm *pX;                           /* For looping over all siblings */
  winShmNode *pShmNode = p->pShmNode;
  int rc = SQLITE_OK;                   /* Result code */
  u16 mask;                             /* Mask of locks to take or release */

  assert( ofst>=0 && ofst+n<=SQLITE_SHM_NLOCK );
  assert( n>=1 );
  assert( flags==(SQLITE_SHM_LOCK | SQLITE_SHM_SHARED)
       || flags==(SQLITE_SHM_LOCK | SQLITE_SHM_EXCLUSIVE)
       || flags==(SQLITE_SHM_UNLOCK | SQLITE_SHM_SHARED)
       || flags==(SQLITE_SHM_UNLOCK | SQLITE_SHM_EXCLUSIVE) );
  assert( n==1 || (flags & SQLITE_SHM_EXCLUSIVE)!=0 );

  mask = (u16)((1U<<(ofst+n)) - (1U<<ofst));
  assert( n>1 || mask==(1<<ofst) );
  sqlite3_mutex_enter(pShmNode->mutex);
  if( flags & SQLITE_SHM_UNLOCK ){
    u16 allMask = 0; /* Mask of locks held by siblings */

    /* See if any siblings hold this same lock */
    for(pX=pShmNode->pFirst; pX; pX=pX->pNext){
      if( pX==p ) continue;
      assert( (pX->exclMask & (p->exclMask|p->sharedMask))==0 );
      allMask |= pX->sharedMask;
    }

    /* Unlock the system-level locks */
    if( (mask & allMask)==0 ){
      rc = winShmSystemLock(pShmNode, _SHM_UNLCK, ofst+WIN_SHM_BASE, n);
    }else{
      rc = SQLITE_OK;
    }

    /* Undo the local locks */
    if( rc==SQLITE_OK ){
      p->exclMask &= ~mask;
      p->sharedMask &= ~mask;
    } 
  }else if( flags & SQLITE_SHM_SHARED ){
    u16 allShared = 0;  /* Union of locks held by connections other than "p" */

    /* Find out which shared locks are already held by sibling connections.
    ** If any sibling already holds an exclusive lock, go ahead and return
    ** SQLITE_BUSY.
    */
    for(pX=pShmNode->pFirst; pX; pX=pX->pNext){
      if( (pX->exclMask & mask)!=0 ){
        rc = SQLITE_BUSY;
        break;
      }
      allShared |= pX->sharedMask;
    }

    /* Get shared locks at the system level, if necessary */
    if( rc==SQLITE_OK ){
      if( (allShared & mask)==0 ){
        rc = winShmSystemLock(pShmNode, _SHM_RDLCK, ofst+WIN_SHM_BASE, n);
      }else{
        rc = SQLITE_OK;
      }
    }

    /* Get the local shared locks */
    if( rc==SQLITE_OK ){
      p->sharedMask |= mask;
    }
  }else{
    /* Make sure no sibling connections hold locks that will block this
    ** lock.  If any do, return SQLITE_BUSY right away.
    */
    for(pX=pShmNode->pFirst; pX; pX=pX->pNext){
      if( (pX->exclMask & mask)!=0 || (pX->sharedMask & mask)!=0 ){
        rc = SQLITE_BUSY;
        break;
      }
    }
  
    /* Get the exclusive locks at the system level.  Then if successful
    ** also mark the local connection as being locked.
    */
    if( rc==SQLITE_OK ){
      rc = winShmSystemLock(pShmNode, _SHM_WRLCK, ofst+WIN_SHM_BASE, n);
      if( rc==SQLITE_OK ){
        assert( (p->sharedMask & mask)==0 );
        p->exclMask |= mask;
      }
    }
  }
  sqlite3_mutex_leave(pShmNode->mutex);
  OSTRACE(("SHM-LOCK shmid-%d, pid-%d got %03x,%03x %s\n",
           p->id, (int)osGetCurrentProcessId(), p->sharedMask, p->exclMask,
           rc ? "failed" : "ok"));
  return rc;
}

/*
** Implement a memory barrier or memory fence on shared memory.  
**
** All loads and stores begun before the barrier must complete before
** any load or store begun after the barrier.
*/
static void winShmBarrier(
  sqlite3_file *fd          /* Database holding the shared memory */
){
  UNUSED_PARAMETER(fd);
  /* MemoryBarrier(); // does not work -- do not know why not */
  winShmEnterMutex();
  winShmLeaveMutex();
}

/*
** This function is called to obtain a pointer to region iRegion of the 
** shared-memory associated with the database file fd. Shared-memory regions 
** are numbered starting from zero. Each shared-memory region is szRegion 
** bytes in size.
**
** If an error occurs, an error code is returned and *pp is set to NULL.
**
** Otherwise, if the isWrite parameter is 0 and the requested shared-memory
** region has not been allocated (by any client, including one running in a
** separate process), then *pp is set to NULL and SQLITE_OK returned. If 
** isWrite is non-zero and the requested shared-memory region has not yet 
** been allocated, it is allocated by this function.
**
** If the shared-memory region has already been allocated or is allocated by
** this call as described above, then it is mapped into this processes 
** address space (if it is not already), *pp is set to point to the mapped 
** memory and SQLITE_OK returned.
*/
static int winShmMap(
  sqlite3_file *fd,               /* Handle open on database file */
  int iRegion,                    /* Region to retrieve */
  int szRegion,                   /* Size of regions */
  int isWrite,                    /* True to extend file if necessary */
  void volatile **pp              /* OUT: Mapped memory */
){
  winFile *pDbFd = (winFile*)fd;
  winShm *p = pDbFd->pShm;
  winShmNode *pShmNode;
  int rc = SQLITE_OK;

  if( !p ){
    rc = winOpenSharedMemory(pDbFd);
    if( rc!=SQLITE_OK ) return rc;
    p = pDbFd->pShm;
  }
  pShmNode = p->pShmNode;

  sqlite3_mutex_enter(pShmNode->mutex);
  assert( szRegion==pShmNode->szRegion || pShmNode->nRegion==0 );

  if( pShmNode->nRegion<=iRegion ){
    struct ShmRegion *apNew;           /* New aRegion[] array */
    int nByte = (iRegion+1)*szRegion;  /* Minimum required file size */
    sqlite3_int64 sz;                  /* Current size of wal-index file */

    pShmNode->szRegion = szRegion;

    /* The requested region is not mapped into this processes address space.
    ** Check to see if it has been allocated (i.e. if the wal-index file is
    ** large enough to contain the requested region).
    */
    rc = winFileSize((sqlite3_file *)&pShmNode->hFile, &sz);
    if( rc!=SQLITE_OK ){
      rc = winLogError(SQLITE_IOERR_SHMSIZE, osGetLastError(),
               "winShmMap1", pDbFd->zPath);
      goto shmpage_out;
    }

    if( sz<nByte ){
      /* The requested memory region does not exist. If isWrite is set to
      ** zero, exit early. *pp will be set to NULL and SQLITE_OK returned.
      **
      ** Alternatively, if isWrite is non-zero, use ftruncate() to allocate
      ** the requested memory region.
      */
      if( !isWrite ) goto shmpage_out;
      rc = winTruncate((sqlite3_file *)&pShmNode->hFile, nByte);
      if( rc!=SQLITE_OK ){
        rc = winLogError(SQLITE_IOERR_SHMSIZE, osGetLastError(),
                 "winShmMap2", pDbFd->zPath);
        goto shmpage_out;
      }
    }

    /* Map the requested memory region into this processes address space. */
    apNew = (struct ShmRegion *)sqlite3_realloc(
        pShmNode->aRegion, (iRegion+1)*sizeof(apNew[0])
    );
    if( !apNew ){
      rc = SQLITE_IOERR_NOMEM;
      goto shmpage_out;
    }
    pShmNode->aRegion = apNew;

    while( pShmNode->nRegion<=iRegion ){
      HANDLE hMap;                /* file-mapping handle */
      void *pMap = 0;             /* Mapped memory region */
     
      hMap = osCreateFileMapping(pShmNode->hFile.h, 
          NULL, PAGE_READWRITE, 0, nByte, NULL
      );
      OSTRACE(("SHM-MAP pid-%d create region=%d nbyte=%d %s\n",
               (int)osGetCurrentProcessId(), pShmNode->nRegion, nByte,
               hMap ? "ok" : "failed"));
      if( hMap ){
        int iOffset = pShmNode->nRegion*szRegion;
        int iOffsetShift = iOffset % winSysInfo.dwAllocationGranularity;
        pMap = osMapViewOfFile(hMap, FILE_MAP_WRITE | FILE_MAP_READ,
            0, iOffset - iOffsetShift, szRegion + iOffsetShift
        );
        OSTRACE(("SHM-MAP pid-%d map region=%d offset=%d size=%d %s\n",
                 (int)osGetCurrentProcessId(), pShmNode->nRegion, iOffset,
                 szRegion, pMap ? "ok" : "failed"));
      }
      if( !pMap ){
        pShmNode->lastErrno = osGetLastError();
        rc = winLogError(SQLITE_IOERR_SHMMAP, pShmNode->lastErrno,
                 "winShmMap3", pDbFd->zPath);
        if( hMap ) osCloseHandle(hMap);
        goto shmpage_out;
      }

      pShmNode->aRegion[pShmNode->nRegion].pMap = pMap;
      pShmNode->aRegion[pShmNode->nRegion].hMap = hMap;
      pShmNode->nRegion++;
    }
  }

shmpage_out:
  if( pShmNode->nRegion>iRegion ){
    int iOffset = iRegion*szRegion;
    int iOffsetShift = iOffset % winSysInfo.dwAllocationGranularity;
    char *p = (char *)pShmNode->aRegion[iRegion].pMap;
    *pp = (void *)&p[iOffsetShift];
  }else{
    *pp = 0;
  }
  sqlite3_mutex_leave(pShmNode->mutex);
  return rc;
}

#else
# define winShmMap     0
# define winShmLock    0
# define winShmBarrier 0
# define winShmUnmap   0
#endif /* #ifndef SQLITE_OMIT_WAL */

/*
** Here ends the implementation of all sqlite3_file methods.
**
********************** End sqlite3_file Methods *******************************
******************************************************************************/

/*
** This vector defines all the methods that can operate on an
** sqlite3_file for win32.
*/
static const sqlite3_io_methods winIoMethod = {
  2,                              /* iVersion */
  winClose,                       /* xClose */
  winRead,                        /* xRead */
  winWrite,                       /* xWrite */
  winTruncate,                    /* xTruncate */
  winSync,                        /* xSync */
  winFileSize,                    /* xFileSize */
  winLock,                        /* xLock */
  winUnlock,                      /* xUnlock */
  winCheckReservedLock,           /* xCheckReservedLock */
  winFileControl,                 /* xFileControl */
  winSectorSize,                  /* xSectorSize */
  winDeviceCharacteristics,       /* xDeviceCharacteristics */
  winShmMap,                      /* xShmMap */
  winShmLock,                     /* xShmLock */
  winShmBarrier,                  /* xShmBarrier */
  winShmUnmap                     /* xShmUnmap */
};

/****************************************************************************
**************************** sqlite3_vfs methods ****************************
**
** This division contains the implementation of methods on the
** sqlite3_vfs object.
*/

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
    zConverted = sqlite3_win32_utf8_to_mbcs(zFilename);
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
  char zTempPath[MAX_PATH+2];

  /* It's odd to simulate an io-error here, but really this is just
  ** using the io-error infrastructure to test that SQLite handles this
  ** function failing. 
  */
  SimulateIOError( return SQLITE_IOERR );

  if( sqlite3_temp_directory ){
    sqlite3_snprintf(MAX_PATH-30, zTempPath, "%s", sqlite3_temp_directory);
  }else if( isNT() ){
    char *zMulti;
    WCHAR zWidePath[MAX_PATH];
    osGetTempPathW(MAX_PATH-30, zWidePath);
    zMulti = unicodeToUtf8(zWidePath);
    if( zMulti ){
      sqlite3_snprintf(MAX_PATH-30, zTempPath, "%s", zMulti);
      sqlite3_free(zMulti);
    }else{
      return SQLITE_IOERR_NOMEM;
    }
/* isNT() is 1 if SQLITE_OS_WINCE==1, so this else is never executed. 
** Since the ANSI version of these Windows API do not exist for WINCE,
** it's important to not reference them for WINCE builds.
*/
#if SQLITE_OS_WINCE==0
  }else{
    char *zUtf8;
    char zMbcsPath[MAX_PATH];
    osGetTempPathA(MAX_PATH-30, zMbcsPath);
    zUtf8 = sqlite3_win32_mbcs_to_utf8(zMbcsPath);
    if( zUtf8 ){
      sqlite3_snprintf(MAX_PATH-30, zTempPath, "%s", zUtf8);
      sqlite3_free(zUtf8);
    }else{
      return SQLITE_IOERR_NOMEM;
    }
#endif
  }

  /* Check that the output buffer is large enough for the temporary file 
  ** name. If it is not, return SQLITE_ERROR.
  */
  if( (sqlite3Strlen30(zTempPath) + sqlite3Strlen30(SQLITE_TEMP_FILE_PREFIX) + 18) >= nBuf ){
    return SQLITE_ERROR;
  }

  for(i=sqlite3Strlen30(zTempPath); i>0 && zTempPath[i-1]=='\\'; i--){}
  zTempPath[i] = 0;

  sqlite3_snprintf(nBuf-18, zBuf,
                   "%s\\"SQLITE_TEMP_FILE_PREFIX, zTempPath);
  j = sqlite3Strlen30(zBuf);
  sqlite3_randomness(15, &zBuf[j]);
  for(i=0; i<15; i++, j++){
    zBuf[j] = (char)zChars[ ((unsigned char)zBuf[j])%(sizeof(zChars)-1) ];
  }
  zBuf[j] = 0;
  zBuf[j+1] = 0;

  OSTRACE(("TEMP FILENAME: %s\n", zBuf));
  return SQLITE_OK; 
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
  DWORD lastErrno;
  DWORD dwDesiredAccess;
  DWORD dwShareMode;
  DWORD dwCreationDisposition;
  DWORD dwFlagsAndAttributes = 0;
#if SQLITE_OS_WINCE
  int isTemp = 0;
#endif
  winFile *pFile = (winFile*)id;
  void *zConverted;              /* Filename in OS encoding */
  const char *zUtf8Name = zName; /* Filename in UTF-8 encoding */
  int cnt = 0;

  /* If argument zPath is a NULL pointer, this function is required to open
  ** a temporary file. Use this buffer to store the file name in.
  */
  char zTmpname[MAX_PATH+2];     /* Buffer used to create temp filename */

  int rc = SQLITE_OK;            /* Function Return Code */
#if !defined(NDEBUG) || SQLITE_OS_WINCE
  int eType = flags&0xFFFFFF00;  /* Type of file to open */
#endif

  int isExclusive  = (flags & SQLITE_OPEN_EXCLUSIVE);
  int isDelete     = (flags & SQLITE_OPEN_DELETEONCLOSE);
  int isCreate     = (flags & SQLITE_OPEN_CREATE);
#ifndef NDEBUG
  int isReadonly   = (flags & SQLITE_OPEN_READONLY);
#endif
  int isReadWrite  = (flags & SQLITE_OPEN_READWRITE);

#ifndef NDEBUG
  int isOpenJournal = (isCreate && (
        eType==SQLITE_OPEN_MASTER_JOURNAL 
     || eType==SQLITE_OPEN_MAIN_JOURNAL 
     || eType==SQLITE_OPEN_WAL
  ));
#endif

  /* Check the following statements are true: 
  **
  **   (a) Exactly one of the READWRITE and READONLY flags must be set, and 
  **   (b) if CREATE is set, then READWRITE must also be set, and
  **   (c) if EXCLUSIVE is set, then CREATE must also be set.
  **   (d) if DELETEONCLOSE is set, then CREATE must also be set.
  */
  assert((isReadonly==0 || isReadWrite==0) && (isReadWrite || isReadonly));
  assert(isCreate==0 || isReadWrite);
  assert(isExclusive==0 || isCreate);
  assert(isDelete==0 || isCreate);

  /* The main DB, main journal, WAL file and master journal are never 
  ** automatically deleted. Nor are they ever temporary files.  */
  assert( (!isDelete && zName) || eType!=SQLITE_OPEN_MAIN_DB );
  assert( (!isDelete && zName) || eType!=SQLITE_OPEN_MAIN_JOURNAL );
  assert( (!isDelete && zName) || eType!=SQLITE_OPEN_MASTER_JOURNAL );
  assert( (!isDelete && zName) || eType!=SQLITE_OPEN_WAL );

  /* Assert that the upper layer has set one of the "file-type" flags. */
  assert( eType==SQLITE_OPEN_MAIN_DB      || eType==SQLITE_OPEN_TEMP_DB 
       || eType==SQLITE_OPEN_MAIN_JOURNAL || eType==SQLITE_OPEN_TEMP_JOURNAL 
       || eType==SQLITE_OPEN_SUBJOURNAL   || eType==SQLITE_OPEN_MASTER_JOURNAL 
       || eType==SQLITE_OPEN_TRANSIENT_DB || eType==SQLITE_OPEN_WAL
  );

  assert( id!=0 );
  UNUSED_PARAMETER(pVfs);

  pFile->h = INVALID_HANDLE_VALUE;

  /* If the second argument to this function is NULL, generate a 
  ** temporary file name to use 
  */
  if( !zUtf8Name ){
    assert(isDelete && !isOpenJournal);
    rc = getTempname(MAX_PATH+2, zTmpname);
    if( rc!=SQLITE_OK ){
      return rc;
    }
    zUtf8Name = zTmpname;
  }

  /* Database filenames are double-zero terminated if they are not
  ** URIs with parameters.  Hence, they can always be passed into
  ** sqlite3_uri_parameter().
  */
  assert( (eType!=SQLITE_OPEN_MAIN_DB) || (flags & SQLITE_OPEN_URI) ||
        zUtf8Name[strlen(zUtf8Name)+1]==0 );

  /* Convert the filename to the system encoding. */
  zConverted = convertUtf8Filename(zUtf8Name);
  if( zConverted==0 ){
    return SQLITE_IOERR_NOMEM;
  }

  if( isReadWrite ){
    dwDesiredAccess = GENERIC_READ | GENERIC_WRITE;
  }else{
    dwDesiredAccess = GENERIC_READ;
  }

  /* SQLITE_OPEN_EXCLUSIVE is used to make sure that a new file is 
  ** created. SQLite doesn't use it to indicate "exclusive access" 
  ** as it is usually understood.
  */
  if( isExclusive ){
    /* Creates a new file, only if it does not already exist. */
    /* If the file exists, it fails. */
    dwCreationDisposition = CREATE_NEW;
  }else if( isCreate ){
    /* Open existing file, or create if it doesn't exist */
    dwCreationDisposition = OPEN_ALWAYS;
  }else{
    /* Opens a file, only if it exists. */
    dwCreationDisposition = OPEN_EXISTING;
  }

  dwShareMode = FILE_SHARE_READ | FILE_SHARE_WRITE;

  if( isDelete ){
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
    while( (h = osCreateFileW((LPCWSTR)zConverted,
                              dwDesiredAccess,
                              dwShareMode, NULL,
                              dwCreationDisposition,
                              dwFlagsAndAttributes,
                              NULL))==INVALID_HANDLE_VALUE &&
                              retryIoerr(&cnt, &lastErrno) ){}
/* isNT() is 1 if SQLITE_OS_WINCE==1, so this else is never executed. 
** Since the ANSI version of these Windows API do not exist for WINCE,
** it's important to not reference them for WINCE builds.
*/
#if SQLITE_OS_WINCE==0
  }else{
    while( (h = osCreateFileA((LPCSTR)zConverted,
                              dwDesiredAccess,
                              dwShareMode, NULL,
                              dwCreationDisposition,
                              dwFlagsAndAttributes,
                              NULL))==INVALID_HANDLE_VALUE &&
                              retryIoerr(&cnt, &lastErrno) ){}
#endif
  }

  logIoerr(cnt);

  OSTRACE(("OPEN %d %s 0x%lx %s\n", 
           h, zName, dwDesiredAccess, 
           h==INVALID_HANDLE_VALUE ? "failed" : "ok"));

  if( h==INVALID_HANDLE_VALUE ){
    pFile->lastErrno = lastErrno;
    winLogError(SQLITE_CANTOPEN, pFile->lastErrno, "winOpen", zUtf8Name);
    sqlite3_free(zConverted);
    if( isReadWrite && !isExclusive ){
      return winOpen(pVfs, zName, id, 
             ((flags|SQLITE_OPEN_READONLY)&~(SQLITE_OPEN_CREATE|SQLITE_OPEN_READWRITE)), pOutFlags);
    }else{
      return SQLITE_CANTOPEN_BKPT;
    }
  }

  if( pOutFlags ){
    if( isReadWrite ){
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
  if( sqlite3_uri_boolean(zName, "psow", SQLITE_POWERSAFE_OVERWRITE) ){
    pFile->ctrlFlags |= WINFILE_PSOW;
  }

#if SQLITE_OS_WINCE
  if( isReadWrite && eType==SQLITE_OPEN_MAIN_DB
       && !winceCreateLock(zName, pFile)
  ){
    osCloseHandle(h);
    sqlite3_free(zConverted);
    return SQLITE_CANTOPEN_BKPT;
  }
  if( isTemp ){
    pFile->zDeleteOnClose = zConverted;
  }else
#endif
  {
    sqlite3_free(zConverted);
  }

  OpenCounter(+1);
  return rc;
}

/*
** Delete the named file.
**
** Note that Windows does not allow a file to be deleted if some other
** process has it open.  Sometimes a virus scanner or indexing program
** will open a journal file shortly after it is created in order to do
** whatever it does.  While this other process is holding the
** file open, we will be unable to delete it.  To work around this
** problem, we delay 100 milliseconds and try to delete again.  Up
** to MX_DELETION_ATTEMPTs deletion attempts are run before giving
** up and returning an error.
*/
static int winDelete(
  sqlite3_vfs *pVfs,          /* Not used on win32 */
  const char *zFilename,      /* Name of file to delete */
  int syncDir                 /* Not used on win32 */
){
  int cnt = 0;
  int rc;
  DWORD lastErrno;
  void *zConverted;
  UNUSED_PARAMETER(pVfs);
  UNUSED_PARAMETER(syncDir);

  SimulateIOError(return SQLITE_IOERR_DELETE);
  zConverted = convertUtf8Filename(zFilename);
  if( zConverted==0 ){
    return SQLITE_IOERR_NOMEM;
  }
  if( isNT() ){
    rc = 1;
    while( osGetFileAttributesW(zConverted)!=INVALID_FILE_ATTRIBUTES &&
         (rc = osDeleteFileW(zConverted))==0 && retryIoerr(&cnt, &lastErrno) ){}
    rc = rc ? SQLITE_OK : SQLITE_ERROR;
/* isNT() is 1 if SQLITE_OS_WINCE==1, so this else is never executed. 
** Since the ANSI version of these Windows API do not exist for WINCE,
** it's important to not reference them for WINCE builds.
*/
#if SQLITE_OS_WINCE==0
  }else{
    rc = 1;
    while( osGetFileAttributesA(zConverted)!=INVALID_FILE_ATTRIBUTES &&
         (rc = osDeleteFileA(zConverted))==0 && retryIoerr(&cnt, &lastErrno) ){}
    rc = rc ? SQLITE_OK : SQLITE_ERROR;
#endif
  }
  if( rc ){
    rc = winLogError(SQLITE_IOERR_DELETE, lastErrno,
             "winDelete", zFilename);
  }else{
    logIoerr(cnt);
  }
  sqlite3_free(zConverted);
  OSTRACE(("DELETE \"%s\" %s\n", zFilename, (rc ? "failed" : "ok" )));
  return rc;
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
  DWORD lastErrno;
  void *zConverted;
  UNUSED_PARAMETER(pVfs);

  SimulateIOError( return SQLITE_IOERR_ACCESS; );
  zConverted = convertUtf8Filename(zFilename);
  if( zConverted==0 ){
    return SQLITE_IOERR_NOMEM;
  }
  if( isNT() ){
    int cnt = 0;
    WIN32_FILE_ATTRIBUTE_DATA sAttrData;
    memset(&sAttrData, 0, sizeof(sAttrData));
    while( !(rc = osGetFileAttributesExW((LPCWSTR)zConverted,
                             GetFileExInfoStandard, 
                             &sAttrData)) && retryIoerr(&cnt, &lastErrno) ){}
    if( rc ){
      /* For an SQLITE_ACCESS_EXISTS query, treat a zero-length file
      ** as if it does not exist.
      */
      if(    flags==SQLITE_ACCESS_EXISTS
          && sAttrData.nFileSizeHigh==0 
          && sAttrData.nFileSizeLow==0 ){
        attr = INVALID_FILE_ATTRIBUTES;
      }else{
        attr = sAttrData.dwFileAttributes;
      }
    }else{
      logIoerr(cnt);
      if( lastErrno!=ERROR_FILE_NOT_FOUND ){
        winLogError(SQLITE_IOERR_ACCESS, lastErrno, "winAccess", zFilename);
        sqlite3_free(zConverted);
        return SQLITE_IOERR_ACCESS;
      }else{
        attr = INVALID_FILE_ATTRIBUTES;
      }
    }
/* isNT() is 1 if SQLITE_OS_WINCE==1, so this else is never executed. 
** Since the ANSI version of these Windows API do not exist for WINCE,
** it's important to not reference them for WINCE builds.
*/
#if SQLITE_OS_WINCE==0
  }else{
    attr = osGetFileAttributesA((char*)zConverted);
#endif
  }
  sqlite3_free(zConverted);
  switch( flags ){
    case SQLITE_ACCESS_READ:
    case SQLITE_ACCESS_EXISTS:
      rc = attr!=INVALID_FILE_ATTRIBUTES;
      break;
    case SQLITE_ACCESS_READWRITE:
      rc = attr!=INVALID_FILE_ATTRIBUTES &&
             (attr & FILE_ATTRIBUTE_READONLY)==0;
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
  SimulateIOError( return SQLITE_ERROR );
  UNUSED_PARAMETER(nFull);
  cygwin_conv_to_full_win32_path(zRelative, zFull);
  return SQLITE_OK;
#endif

#if SQLITE_OS_WINCE
  SimulateIOError( return SQLITE_ERROR );
  UNUSED_PARAMETER(nFull);
  /* WinCE has no concept of a relative pathname, or so I am told. */
  sqlite3_snprintf(pVfs->mxPathname, zFull, "%s", zRelative);
  return SQLITE_OK;
#endif

#if !SQLITE_OS_WINCE && !defined(__CYGWIN__)
  int nByte;
  void *zConverted;
  char *zOut;

  /* If this path name begins with "/X:", where "X" is any alphabetic
  ** character, discard the initial "/" from the pathname.
  */
  if( zRelative[0]=='/' && sqlite3Isalpha(zRelative[1]) && zRelative[2]==':' ){
    zRelative++;
  }

  /* It's odd to simulate an io-error here, but really this is just
  ** using the io-error infrastructure to test that SQLite handles this
  ** function failing. This function could fail if, for example, the
  ** current working directory has been unlinked.
  */
  SimulateIOError( return SQLITE_ERROR );
  UNUSED_PARAMETER(nFull);
  zConverted = convertUtf8Filename(zRelative);
  if( zConverted==0 ){
    return SQLITE_IOERR_NOMEM;
  }
  if( isNT() ){
    LPWSTR zTemp;
    nByte = osGetFullPathNameW((LPCWSTR)zConverted, 0, 0, 0) + 3;
    zTemp = sqlite3_malloc( nByte*sizeof(zTemp[0]) );
    if( zTemp==0 ){
      sqlite3_free(zConverted);
      return SQLITE_IOERR_NOMEM;
    }
    osGetFullPathNameW((LPCWSTR)zConverted, nByte, zTemp, 0);
    sqlite3_free(zConverted);
    zOut = unicodeToUtf8(zTemp);
    sqlite3_free(zTemp);
/* isNT() is 1 if SQLITE_OS_WINCE==1, so this else is never executed. 
** Since the ANSI version of these Windows API do not exist for WINCE,
** it's important to not reference them for WINCE builds.
*/
#if SQLITE_OS_WINCE==0
  }else{
    char *zTemp;
    nByte = osGetFullPathNameA((char*)zConverted, 0, 0, 0) + 3;
    zTemp = sqlite3_malloc( nByte*sizeof(zTemp[0]) );
    if( zTemp==0 ){
      sqlite3_free(zConverted);
      return SQLITE_IOERR_NOMEM;
    }
    osGetFullPathNameA((char*)zConverted, nByte, zTemp, 0);
    sqlite3_free(zConverted);
    zOut = sqlite3_win32_mbcs_to_utf8(zTemp);
    sqlite3_free(zTemp);
#endif
  }
  if( zOut ){
    sqlite3_snprintf(pVfs->mxPathname, zFull, "%s", zOut);
    sqlite3_free(zOut);
    return SQLITE_OK;
  }else{
    return SQLITE_IOERR_NOMEM;
  }
#endif
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
    h = osLoadLibraryW((LPCWSTR)zConverted);
/* isNT() is 1 if SQLITE_OS_WINCE==1, so this else is never executed. 
** Since the ANSI version of these Windows API do not exist for WINCE,
** it's important to not reference them for WINCE builds.
*/
#if SQLITE_OS_WINCE==0
  }else{
    h = osLoadLibraryA((char*)zConverted);
#endif
  }
  sqlite3_free(zConverted);
  return (void*)h;
}
static void winDlError(sqlite3_vfs *pVfs, int nBuf, char *zBufOut){
  UNUSED_PARAMETER(pVfs);
  getLastErrorMsg(osGetLastError(), nBuf, zBufOut);
}
static void (*winDlSym(sqlite3_vfs *pVfs, void *pHandle, const char *zSymbol))(void){
  UNUSED_PARAMETER(pVfs);
  return (void(*)(void))osGetProcAddressA((HANDLE)pHandle, zSymbol);
}
static void winDlClose(sqlite3_vfs *pVfs, void *pHandle){
  UNUSED_PARAMETER(pVfs);
  osFreeLibrary((HANDLE)pHandle);
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
    osGetSystemTime(&x);
    memcpy(&zBuf[n], &x, sizeof(x));
    n += sizeof(x);
  }
  if( sizeof(DWORD)<=nBuf-n ){
    DWORD pid = osGetCurrentProcessId();
    memcpy(&zBuf[n], &pid, sizeof(pid));
    n += sizeof(pid);
  }
  if( sizeof(DWORD)<=nBuf-n ){
    DWORD cnt = osGetTickCount();
    memcpy(&zBuf[n], &cnt, sizeof(cnt));
    n += sizeof(cnt);
  }
  if( sizeof(LARGE_INTEGER)<=nBuf-n ){
    LARGE_INTEGER i;
    osQueryPerformanceCounter(&i);
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
  osSleep((microsec+999)/1000);
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
** On success, return SQLITE_OK.  Return SQLITE_ERROR if the time and date 
** cannot be found.
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
  osGetSystemTime(&time);
  /* if SystemTimeToFileTime() fails, it returns zero. */
  if (!osSystemTimeToFileTime(&time,&ft)){
    return SQLITE_ERROR;
  }
#else
  osGetSystemTimeAsFileTime( &ft );
#endif

  *piNow = winFiletimeEpoch +
            ((((sqlite3_int64)ft.dwHighDateTime)*max32BitValue) + 
               (sqlite3_int64)ft.dwLowDateTime)/(sqlite3_int64)10000;

#ifdef SQLITE_TEST
  if( sqlite3_current_time ){
    *piNow = 1000*(sqlite3_int64)sqlite3_current_time + unixEpoch;
  }
#endif
  UNUSED_PARAMETER(pVfs);
  return SQLITE_OK;
}

/*
** Find the current time (in Universal Coordinated Time).  Write the
** current time and date as a Julian Day number into *prNow and
** return 0.  Return 1 if the time and date cannot be found.
*/
static int winCurrentTime(sqlite3_vfs *pVfs, double *prNow){
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
** GetLastError() and FormatMessage() on Windows (or errno and
** strerror_r() on Unix). After an error is returned by an OS
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
  return getLastErrorMsg(osGetLastError(), nBuf, zBuf);
}

/*
** Initialize and deinitialize the operating system interface.
*/
int sqlite3_os_init(void){
  static sqlite3_vfs winVfs = {
    3,                   /* iVersion */
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
    winCurrentTimeInt64, /* xCurrentTimeInt64 */
    winSetSystemCall,    /* xSetSystemCall */
    winGetSystemCall,    /* xGetSystemCall */
    winNextSystemCall,   /* xNextSystemCall */
  };

  /* Double-check that the aSyscall[] array has been constructed
  ** correctly.  See ticket [bb3a86e890c8e96ab] */
  assert( ArraySize(aSyscall)==60 );

#ifndef SQLITE_OMIT_WAL
  /* get memory map allocation granularity */
  memset(&winSysInfo, 0, sizeof(SYSTEM_INFO));
  osGetSystemInfo(&winSysInfo);
  assert(winSysInfo.dwAllocationGranularity > 0);
#endif

  sqlite3_vfs_register(&winVfs, 1);
  return SQLITE_OK; 
}

int sqlite3_os_end(void){ 
  return SQLITE_OK;
}

#endif /* SQLITE_OS_WIN */

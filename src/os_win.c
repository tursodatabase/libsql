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
#include "os.h"          /* Must be first to enable large file support */
#if OS_WIN               /* This file is used for windows only */
#include "sqliteInt.h"

#include <winbase.h>

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
** Delete the named file
*/
int sqlite3OsDelete(const char *zFilename){
  DeleteFile(zFilename);
  return SQLITE_OK;
}

/*
** Return TRUE if the named file exists.
*/
int sqlite3OsFileExists(const char *zFilename){
  return GetFileAttributes(zFilename) != 0xffffffff;
}

/*
** Attempt to open a file for both reading and writing.  If that
** fails, try opening it read-only.  If the file does not exist,
** try to create it.
**
** On success, a handle for the open file is written to *id
** and *pReadonly is set to 0 if the file was opened for reading and
** writing or 1 if the file was opened read-only.  The function returns
** SQLITE_OK.
**
** On failure, the function returns SQLITE_CANTOPEN and leaves
** *id and *pReadonly unchanged.
*/
int sqlite3OsOpenReadWrite(
  const char *zFilename,
  OsFile *id,
  int *pReadonly
){
  HANDLE h = CreateFile(zFilename,
     GENERIC_READ | GENERIC_WRITE,
     FILE_SHARE_READ | FILE_SHARE_WRITE,
     NULL,
     OPEN_ALWAYS,
     FILE_ATTRIBUTE_NORMAL | FILE_FLAG_RANDOM_ACCESS,
     NULL
  );
  if( h==INVALID_HANDLE_VALUE ){
    h = CreateFile(zFilename,
       GENERIC_READ,
       FILE_SHARE_READ,
       NULL,
       OPEN_ALWAYS,
       FILE_ATTRIBUTE_NORMAL | FILE_FLAG_RANDOM_ACCESS,
       NULL
    );
    if( h==INVALID_HANDLE_VALUE ){
      return SQLITE_CANTOPEN;
    }
    *pReadonly = 1;
  }else{
    *pReadonly = 0;
  }
  id->h = h;
  id->locked = 0;
  OpenCounter(+1);
  return SQLITE_OK;
}


/*
** Attempt to open a new file for exclusive access by this process.
** The file will be opened for both reading and writing.  To avoid
** a potential security problem, we do not allow the file to have
** previously existed.  Nor do we allow the file to be a symbolic
** link.
**
** If delFlag is true, then make arrangements to automatically delete
** the file when it is closed.
**
** On success, write the file handle into *id and return SQLITE_OK.
**
** On failure, return SQLITE_CANTOPEN.
*/
int sqlite3OsOpenExclusive(const char *zFilename, OsFile *id, int delFlag){
  HANDLE h;
  int fileflags;
  if( delFlag ){
    fileflags = FILE_ATTRIBUTE_TEMPORARY | FILE_FLAG_RANDOM_ACCESS 
                     | FILE_FLAG_DELETE_ON_CLOSE;
  }else{
    fileflags = FILE_FLAG_RANDOM_ACCESS;
  }
  h = CreateFile(zFilename,
     GENERIC_READ | GENERIC_WRITE,
     0,
     NULL,
     CREATE_ALWAYS,
     fileflags,
     NULL
  );
  if( h==INVALID_HANDLE_VALUE ){
    return SQLITE_CANTOPEN;
  }
  id->h = h;
  id->locked = 0;
  OpenCounter(+1);
  return SQLITE_OK;
}

/*
** Attempt to open a new file for read-only access.
**
** On success, write the file handle into *id and return SQLITE_OK.
**
** On failure, return SQLITE_CANTOPEN.
*/
int sqlite3OsOpenReadOnly(const char *zFilename, OsFile *id){
  HANDLE h = CreateFile(zFilename,
     GENERIC_READ,
     0,
     NULL,
     OPEN_EXISTING,
     FILE_ATTRIBUTE_NORMAL | FILE_FLAG_RANDOM_ACCESS,
     NULL
  );
  if( h==INVALID_HANDLE_VALUE ){
    return SQLITE_CANTOPEN;
  }
  id->h = h;
  id->locked = 0;
  OpenCounter(+1);
  return SQLITE_OK;
}

/*
** Attempt to open a file descriptor for the directory that contains a
** file.  This file descriptor can be used to fsync() the directory
** in order to make sure the creation of a new file is actually written
** to disk.
**
** This routine is only meaningful for Unix.  It is a no-op under
** windows since windows does not support hard links.
**
** On success, a handle for a previously open file is at *id is
** updated with the new directory file descriptor and SQLITE_OK is
** returned.
**
** On failure, the function returns SQLITE_CANTOPEN and leaves
** *id unchanged.
*/
int sqlite3OsOpenDirectory(
  const char *zDirname,
  OsFile *id
){
  return SQLITE_OK;
}

/*
** Create a temporary file name in zBuf.  zBuf must be big enough to
** hold at least SQLITE_TEMPNAME_SIZE characters.
*/
int sqlite3OsTempFileName(char *zBuf){
  static char zChars[] =
    "abcdefghijklmnopqrstuvwxyz"
    "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    "0123456789";
  int i, j;
  char zTempPath[SQLITE_TEMPNAME_SIZE];
  GetTempPath(SQLITE_TEMPNAME_SIZE-30, zTempPath);
  for(i=strlen(zTempPath); i>0 && zTempPath[i-1]=='\\'; i--){}
  zTempPath[i] = 0;
  for(;;){
    sprintf(zBuf, "%s\\"TEMP_FILE_PREFIX, zTempPath);
    j = strlen(zBuf);
    sqlite3Randomness(15, &zBuf[j]);
    for(i=0; i<15; i++, j++){
      zBuf[j] = (char)zChars[ ((unsigned char)zBuf[j])%(sizeof(zChars)-1) ];
    }
    zBuf[j] = 0;
    if( !sqlite3OsFileExists(zBuf) ) break;
  }
  return SQLITE_OK; 
}

/*
** Close a file.
*/
int sqlite3OsClose(OsFile *id){
  CloseHandle(id->h);
  OpenCounter(-1);
  return SQLITE_OK;
}

/*
** Read data from a file into a buffer.  Return SQLITE_OK if all
** bytes were read successfully and SQLITE_IOERR if anything goes
** wrong.
*/
int sqlite3OsRead(OsFile *id, void *pBuf, int amt){
  DWORD got;
  SimulateIOError(SQLITE_IOERR);
  TRACE2("READ %d\n", last_page);
  if( !ReadFile(id->h, pBuf, amt, &got, 0) ){
    got = 0;
  }
  if( got==(DWORD)amt ){
    return SQLITE_OK;
  }else{
    return SQLITE_IOERR;
  }
}

/*
** Write data from a buffer into a file.  Return SQLITE_OK on success
** or some other error code on failure.
*/
int sqlite3OsWrite(OsFile *id, const void *pBuf, int amt){
  int rc;
  DWORD wrote;
  SimulateIOError(SQLITE_IOERR);
  TRACE2("WRITE %d\n", last_page);
  while( amt>0 && (rc = WriteFile(id->h, pBuf, amt, &wrote, 0))!=0 && wrote>0 ){
    amt -= wrote;
    pBuf = &((char*)pBuf)[wrote];
  }
  if( !rc || amt>(int)wrote ){
    return SQLITE_FULL;
  }
  return SQLITE_OK;
}

/*
** Move the read/write pointer in a file.
*/
int sqlite3OsSeek(OsFile *id, off_t offset){
  LONG upperBits = offset>>32;
  LONG lowerBits = offset & 0xffffffff;
  DWORD rc;
  SEEK(offset/1024 + 1);
  rc = SetFilePointer(id->h, lowerBits, &upperBits, FILE_BEGIN);
  /* TRACE3("SEEK rc=0x%x upper=0x%x\n", rc, upperBits); */
  return SQLITE_OK;
}

/*
** Make sure all writes to a particular file are committed to disk.
*/
int sqlite3OsSync(OsFile *id){
  if( FlushFileBuffers(id->h) ){
    return SQLITE_OK;
  }else{
    return SQLITE_IOERR;
  }
}

/*
** Truncate an open file to a specified size
*/
int sqlite3OsTruncate(OsFile *id, off_t nByte){
  LONG upperBits = nByte>>32;
  SimulateIOError(SQLITE_IOERR);
  SetFilePointer(id->h, nByte, &upperBits, FILE_BEGIN);
  SetEndOfFile(id->h);
  return SQLITE_OK;
}

/*
** Determine the current size of a file in bytes
*/
int sqlite3OsFileSize(OsFile *id, off_t *pSize){
  DWORD upperBits, lowerBits;
  SimulateIOError(SQLITE_IOERR);
  lowerBits = GetFileSize(id->h, &upperBits);
  *pSize = (((off_t)upperBits)<<32) + lowerBits;
  return SQLITE_OK;
}

/*
** Return true (non-zero) if we are running under WinNT, Win2K or WinXP.
** Return false (zero) for Win95, Win98, or WinME.
**
** Here is an interesting observation:  Win95, Win98, and WinME lack
** the LockFileEx() API.  But we can still statically link against that
** API as long as we don't call it win running Win95/98/ME.  A call to
** this routine is used to determine if the host is Win95/98/ME or
** WinNT/2K/XP so that we will know whether or not we can safely call
** the LockFileEx() API.
*/
int isNT(void){
  static int osType = 0;   /* 0=unknown 1=win95 2=winNT */
  if( osType==0 ){
    OSVERSIONINFO sInfo;
    sInfo.dwOSVersionInfoSize = sizeof(sInfo);
    GetVersionEx(&sInfo);
    osType = sInfo.dwPlatformId==VER_PLATFORM_WIN32_NT ? 2 : 1;
  }
  return osType==2;
}

/*
** Windows file locking notes:
**
** We cannot use LockFileEx() or UnlockFileEx() on Win95/98/ME because
** those functions are not available.  So we use only LockFile() and
** UnlockFile().
**
** LockFile() prevents not just writing but also reading by other processes.
** (This is a design error on the part of Windows, but there is nothing
** we can do about that.)  So the region used for locking is at the
** end of the file where it is unlikely to ever interfere with an
** actual read attempt.
**
** A database read lock is obtained by locking a single randomly-chosen 
** byte out of a specific range of bytes. The lock byte is obtained at 
** random so two separate readers can probably access the file at the 
** same time, unless they are unlucky and choose the same lock byte.
** A database write lock is obtained by locking all bytes in the range.
** There can only be one writer.
**
** A lock is obtained on the first byte of the lock range before acquiring
** either a read lock or a write lock.  This prevents two processes from
** attempting to get a lock at a same time.  The semantics of 
** sqlite3OsReadLock() require that if there is already a write lock, that
** lock is converted into a read lock atomically.  The lock on the first
** byte allows us to drop the old write lock and get the read lock without
** another process jumping into the middle and messing us up.  The same
** argument applies to sqlite3OsWriteLock().
**
** On WinNT/2K/XP systems, LockFileEx() and UnlockFileEx() are available,
** which means we can use reader/writer locks.  When reader writer locks
** are used, the lock is placed on the same range of bytes that is used
** for probabilistic locking in Win95/98/ME.  Hence, the locking scheme
** will support two or more Win95 readers or two or more WinNT readers.
** But a single Win95 reader will lock out all WinNT readers and a single
** WinNT reader will lock out all other Win95 readers.
**
** The following #defines specify the range of bytes used for locking.
** N_LOCKBYTE is the number of bytes available for doing the locking.
** The first byte used to hold the lock while the lock is changing does
** not count toward this number.  FIRST_LOCKBYTE is the address of
** the first byte in the range of bytes used for locking.
*/
#define N_LOCKBYTE       10239
#define FIRST_LOCKBYTE   (0xffffffff - N_LOCKBYTE)

int sqlite3OsLock(OsFile *id, int locktype){
  return SQLITE_OK;
}

int sqlite3OsCheckWriteLock(OsFile *id){
  return 0;
}

/*
** Change the status of the lock on the file "id" to be a readlock.
** If the file was write locked, then this reduces the lock to a read.
** If the file was read locked, then this acquires a new read lock.
**
** Return SQLITE_OK on success and SQLITE_BUSY on failure.  If this
** library was compiled with large file support (LFS) but LFS is not
** available on the host, then an SQLITE_NOLFS is returned.
*/
int sqlite3OsReadLock(OsFile *id){
  int rc;
  if( id->locked>0 ){
    rc = SQLITE_OK;
  }else{
    int lk;
    int res;
    int cnt = 100;
    sqlite3Randomness(sizeof(lk), &lk);
    lk = (lk & 0x7fffffff)%N_LOCKBYTE + 1;
    while( cnt-->0 && (res = LockFile(id->h, FIRST_LOCKBYTE, 0, 1, 0))==0 ){
      Sleep(1);
    }
    if( res ){
      UnlockFile(id->h, FIRST_LOCKBYTE+1, 0, N_LOCKBYTE, 0);
      if( isNT() ){
        OVERLAPPED ovlp;
        ovlp.Offset = FIRST_LOCKBYTE+1;
        ovlp.OffsetHigh = 0;
        ovlp.hEvent = 0;
        res = LockFileEx(id->h, LOCKFILE_FAIL_IMMEDIATELY, 
                          0, N_LOCKBYTE, 0, &ovlp);
      }else{
        res = LockFile(id->h, FIRST_LOCKBYTE+lk, 0, 1, 0);
      }
      UnlockFile(id->h, FIRST_LOCKBYTE, 0, 1, 0);
    }
    if( res ){
      id->locked = lk;
      rc = SQLITE_OK;
    }else{
      rc = SQLITE_BUSY;
    }
  }
  return rc;
}

/*
** Change the lock status to be an exclusive or write lock.  Return
** SQLITE_OK on success and SQLITE_BUSY on a failure.  If this
** library was compiled with large file support (LFS) but LFS is not
** available on the host, then an SQLITE_NOLFS is returned.
*/
int sqlite3OsWriteLock(OsFile *id){
  int rc;
  if( id->locked<0 ){
    rc = SQLITE_OK;
  }else{
    int res;
    int cnt = 100;
    while( cnt-->0 && (res = LockFile(id->h, FIRST_LOCKBYTE, 0, 1, 0))==0 ){
      Sleep(1);
    }
    if( res ){
      if( id->locked>0 ){
        if( isNT() ){
          UnlockFile(id->h, FIRST_LOCKBYTE+1, 0, N_LOCKBYTE, 0);
        }else{
          res = UnlockFile(id->h, FIRST_LOCKBYTE + id->locked, 0, 1, 0);
        }
      }
      if( res ){
        res = LockFile(id->h, FIRST_LOCKBYTE+1, 0, N_LOCKBYTE, 0);
      }else{
        res = 0;
      }
      UnlockFile(id->h, FIRST_LOCKBYTE, 0, 1, 0);
    }
    if( res ){
      id->locked = -1;
      rc = SQLITE_OK;
    }else{
      rc = SQLITE_BUSY;
    }
  }
  return rc;
}

/*
** Unlock the given file descriptor.  If the file descriptor was
** not previously locked, then this routine is a no-op.  If this
** library was compiled with large file support (LFS) but LFS is not
** available on the host, then an SQLITE_NOLFS is returned.
*/
int sqlite3OsUnlock(OsFile *id){
  int rc;
  if( id->locked==0 ){
    rc = SQLITE_OK;
  }else if( isNT() || id->locked<0 ){
    UnlockFile(id->h, FIRST_LOCKBYTE+1, 0, N_LOCKBYTE, 0);
    rc = SQLITE_OK;
    id->locked = 0;
  }else{
    UnlockFile(id->h, FIRST_LOCKBYTE+id->locked, 0, 1, 0);
    rc = SQLITE_OK;
    id->locked = 0;
  }
  return rc;
}

/*
** Get information to seed the random number generator.  The seed
** is written into the buffer zBuf[256].  The calling function must
** supply a sufficiently large buffer.
*/
int sqlite3OsRandomSeed(char *zBuf){
  /* We have to initialize zBuf to prevent valgrind from reporting
  ** errors.  The reports issued by valgrind are incorrect - we would
  ** prefer that the randomness be increased by making use of the
  ** uninitialized space in zBuf - but valgrind errors tend to worry
  ** some users.  Rather than argue, it seems easier just to initialize
  ** the whole array and silence valgrind, even if that means less randomness
  ** in the random seed.
  **
  ** When testing, initializing zBuf[] to zero is all we do.  That means
  ** that we always use the same random number sequence.* This makes the
  ** tests repeatable.
  */
  memset(zBuf, 0, 256);
  GetSystemTime((LPSYSTEMTIME)zBuf);
  return SQLITE_OK;
}

/*
** Sleep for a little while.  Return the amount of time slept.
*/
int sqlite3OsSleep(int ms){
  Sleep(ms);
  return ms;
}

/*
** Static variables used for thread synchronization
*/
static int inMutex = 0;
#ifdef SQLITE_W32_THREADS
  static CRITICAL_SECTION cs;
#endif

/*
** The following pair of routine implement mutual exclusion for
** multi-threaded processes.  Only a single thread is allowed to
** executed code that is surrounded by EnterMutex() and LeaveMutex().
**
** SQLite uses only a single Mutex.  There is not much critical
** code and what little there is executes quickly and without blocking.
*/
void sqlite3OsEnterMutex(){
#ifdef SQLITE_W32_THREADS
  static int isInit = 0;
  while( !isInit ){
    static long lock = 0;
    if( InterlockedIncrement(&lock)==1 ){
      InitializeCriticalSection(&cs);
      isInit = 1;
    }else{
      Sleep(1);
    }
  }
  EnterCriticalSection(&cs);
#endif
  assert( !inMutex );
  inMutex = 1;
}
void sqlite3OsLeaveMutex(){
  assert( inMutex );
  inMutex = 0;
#ifdef SQLITE_W32_THREADS
  LeaveCriticalSection(&cs);
#endif
}

/*
** Turn a relative pathname into a full pathname.  Return a pointer
** to the full pathname stored in space obtained from sqliteMalloc().
** The calling function is responsible for freeing this space once it
** is no longer needed.
*/
char *sqlite3OsFullPathname(const char *zRelative){
  char *zNotUsed;
  char *zFull;
  int nByte;
  nByte = GetFullPathName(zRelative, 0, 0, &zNotUsed) + 1;
  zFull = sqliteMalloc( nByte );
  if( zFull==0 ) return 0;
  GetFullPathName(zRelative, nByte, zFull, &zNotUsed);
  return zFull;
}

/*
** The following variable, if set to a non-zero value, becomes the result
** returned from sqlite3OsCurrentTime().  This is used for testing.
*/
#ifdef SQLITE_TEST
int sqlite3_current_time = 0;
#endif

/*
** Find the current time (in Universal Coordinated Time).  Write the
** current time and date as a Julian Day number into *prNow and
** return 0.  Return 1 if the time and date cannot be found.
*/
int sqlite3OsCurrentTime(double *prNow){
  FILETIME ft;
  /* FILETIME structure is a 64-bit value representing the number of 
     100-nanosecond intervals since January 1, 1601 (= JD 2305813.5). 
  */
  double now;
  GetSystemTimeAsFileTime( &ft );
  now = ((double)ft.dwHighDateTime) * 4294967296.0; 
  *prNow = (now + ft.dwLowDateTime)/864000000000.0 + 2305813.5;
#ifdef SQLITE_TEST
  if( sqlite3_current_time ){
    *prNow = sqlite3_current_time/86400.0 + 2440587.5;
  }
#endif
  return 0;
}

#endif /* OS_WIN */

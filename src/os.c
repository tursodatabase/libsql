/*
** 2001 September 16
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
** This file contains code that is specific to particular operating
** systems.  The purpose of this file is to provide a uniform abstraction
** on which the rest of SQLite can operate.
*/
#include "sqliteInt.h"
#include "os.h"

#ifndef OS_UNIX
# ifndef OS_WIN
#  define OS_UNIX 1
# else
#  define OS_UNIX 0
# endif
#endif
#ifndef OS_WIN
# define OS_WIN 0
#endif
#if OS_UNIX
# include <unistd.h>
# include <fcntl.h>
# include <sys/stat.h>
# include <time.h>
#endif
#if OS_WIN
# include <winbase.h>
#endif


#if OS_UNIX
/*
** Here is the dirt on POSIX advisory locks:  ANSI STD 1003.1 (1996)
** section 6.5.2.2 lines 483 through 490 specify that when a process
** sets or clears a lock, that operation overrides any prior locks set
** by the same process.  It does not explicitly say so, but this implies
** that it overrides locks set by the same process using a different
** file descriptor.  Consider this test case:
**
**       int fd1 = open("./file1", O_RDWR|O_CREAT, 0644);
**       int fd2 = open("./file2", O_RDWR|O_CREAT, 0644);
**
** Suppose ./file1 and ./file2 are really be the same file (because
** one is a hard or symbolic link to the other) then if you set
** an exclusive lock on fd1, then try to get an exclusive lock
** on fd2, it works.  I would have expected the second lock to
** fail since there was already a lock on the file due to fd1.
** But not so.  Since both locks came from the same process, the
** second overrides the first, even though they were on different
** file descriptors opened on different file names.
**
** Bummer.  If you ask me, this is broken.  Badly broken.  It means
** that we cannot use POSIX locks to synchronize file access among
** competing threads of the same process.  POSIX locks will work fine
** to synchronize access for threads in separate processes, but not
** threads within the same process.
**
** To work around the problem, SQLite has to manage file locks internally
** on its own.  Whenever a new database is opened, we have to find the
** specific inode of the database file (the inode is determined by the
** st_dev and st_ino fields of the stat structure that fstat() fills in)
** and check for locks already existing on that inode.  When locks are
** created or removed, we have to look at our own internal record of the
** locks to see if another thread has previously set a lock on that same
** inode.
**
** The OsFile structure for POSIX is no longer just an integer file
** descriptor.  It is now a structure that holds the integer file
** descriptor and a pointer to a structure that describes the internal
** locks on the corresponding inode.  There is one locking structure
** per inode, so if the same inode is opened twice, both OsFile structures
** point to the same locking structure.  The locking structure keeps
** a reference count (so we will know when to delete it) and a "cnt"
** field that tells us its internal lock status.  cnt==0 means the
** file is unlocked.  cnt==-1 means the file has an exclusive lock.
** cnt>0 means there are cnt shared locks on the file.
**
** Any attempt to lock or unlock a file first checks the locking
** structure.  The fcntl() system call is only invoked to set a 
** POSIX lock if the internal lock structure transitions between
** a locked and an unlocked state.
*/

/*
** An instance of the following structure serves as the key used
** to locate a particular lockInfo structure given its inode. 
*/
struct inodeKey {
  dev_t dev;   /* Device number */
  ino_t ino;   /* Inode number */
};

/*
** An instance of the following structure is allocated for each inode.
** A single inode can have multiple file descriptors, so each OsFile
** structure contains a pointer to an instance of this object and this
** object keeps a count of the number of OsFiles pointing to it.
*/
struct lockInfo {
  struct inodeKey key;  /* The lookup key */
  int cnt;              /* 0: unlocked.  -1: write lock.  1...: read lock. */
  int nRef;             /* Number of pointers to this structure */
};

/* 
** This hash table maps inodes (in the form of inodeKey structures) into
** pointers to lockInfo structures.
*/
static Hash lockHash = { SQLITE_HASH_BINARY, 0, 0, 0, 0, 0 };

/*
** Given a file descriptor, locate a lockInfo structure that describes
** that file descriptor.  Create a new one if necessary.  NULL might
** be returned if malloc() fails.
*/
static struct lockInfo *findLockInfo(int fd){
  int rc;
  struct inodeKey key;
  struct stat statbuf;
  struct lockInfo *pInfo;
  rc = fstat(fd, &statbuf);
  if( rc!=0 ) return 0;
  memset(&key, 0, sizeof(key));
  key.dev = statbuf.st_dev;
  key.ino = statbuf.st_ino;
  pInfo = (struct lockInfo*)sqliteHashFind(&lockHash, &key, sizeof(key));
  if( pInfo==0 ){
    struct lockInfo *pOld;
    pInfo = sqliteMalloc( sizeof(*pInfo) );
    if( pInfo==0 ) return 0;
    pInfo->key = key;
    pInfo->nRef = 1;
    pInfo->cnt = 0;
    pOld = sqliteHashInsert(&lockHash, &pInfo->key, sizeof(key), pInfo);
    if( pOld!=0 ){
      assert( pOld==pInfo );
      sqliteFree(pInfo);
      pInfo = 0;
    }
  }else{
    pInfo->nRef++;
  }
  return pInfo;
}

/*
** Release a lockInfo structure previously allocated by findLockInfo().
*/
static void releaseLockInfo(struct lockInfo *pInfo){
  pInfo->nRef--;
  if( pInfo->nRef==0 ){
    sqliteHashInsert(&lockHash, &pInfo->key, sizeof(pInfo->key), 0);
    sqliteFree(pInfo);
  }
}
#endif  /** POSIX advisory lock work-around **/

/*
** If we compile with the SQLITE_TEST macro set, then the following block
** of code will give us the ability to simulate a disk I/O error.  This
** is used for testing the I/O recovery logic.
*/
#ifdef SQLITE_TEST
int sqlite_io_error_pending = 0;
#define SimulateIOError(A)  \
   if( sqlite_io_error_pending ) \
     if( sqlite_io_error_pending-- == 1 ){ local_ioerr(); return A; }
static void local_ioerr(){
  sqlite_io_error_pending = 0;  /* Really just a place to set a breakpoint */
}
#else
#define SimulateIOError(A)
#endif


/*
** Delete the named file
*/
int sqliteOsDelete(const char *zFilename){
#if OS_UNIX
  unlink(zFilename);
#endif
#if OS_WIN
  DeleteFile(zFilename);
#endif
  return SQLITE_OK;
}

/*
** Return TRUE if the named file exists.
*/
int sqliteOsFileExists(const char *zFilename){
#if OS_UNIX
  return access(zFilename, 0)==0;
#endif
#if OS_WIN
  HANDLE h;
  h = CreateFile(zFilename,
    GENERIC_READ,
    0,
    NULL,
    OPEN_EXISTING,
    FILE_ATTRIBUTE_NORMAL | FILE_FLAG_RANDOM_ACCESS,
    NULL
  );
  if( h!=INVALID_HANDLE_VALUE ){
    CloseHandle(h);
    return 1;
  }
  return 0;
#endif
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
** *pResulst and *pReadonly unchanged.
*/
int sqliteOsOpenReadWrite(
  const char *zFilename,
  OsFile *id,
  int *pReadonly
){
#if OS_UNIX
  id->fd = open(zFilename, O_RDWR|O_CREAT, 0644);
  if( id->fd<0 ){
    id->fd = open(zFilename, O_RDONLY);
    if( id->fd<0 ){
      return SQLITE_CANTOPEN; 
    }
    *pReadonly = 1;
  }else{
    *pReadonly = 0;
  }
  sqliteOsEnterMutex();
  id->pLock = findLockInfo(id->fd);
  sqliteOsLeaveMutex();
  if( id->pLock==0 ){
    close(id->fd);
    return SQLITE_NOMEM;
  }
  id->locked = 0;
  return SQLITE_OK;
#endif
#if OS_WIN
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
  return SQLITE_OK;
#endif
}


/*
** Attempt to open a new file for exclusive access by this process.
** The file will be opened for both reading and writing.  To avoid
** a potential security problem, we do not allow the file to have
** previously existed.  Nor do we allow the file to be a symbolic
** link.
**
** On success, write the file handle into *id and return SQLITE_OK.
**
** On failure, return SQLITE_CANTOPEN.
*/
int sqliteOsOpenExclusive(const char *zFilename, OsFile *id){
#if OS_UNIX
  if( access(zFilename, 0)==0 ){
    return SQLITE_CANTOPEN;
  }
#ifndef O_NOFOLLOW
# define O_NOFOLLOW 0
#endif
  id->fd = open(zFilename, O_RDWR|O_CREAT|O_EXCL|O_NOFOLLOW, 0600);
  if( id->fd<0 ){
    return SQLITE_CANTOPEN;
  }
  sqliteOsEnterMutex();
  id->pLock = findLockInfo(id->fd);
  sqliteOsLeaveMutex();
  if( id->pLock==0 ){
    close(id->fd);
    unlink(zFilename);
    return SQLITE_NOMEM;
  }
  id->locked = 0;
  return SQLITE_OK;
#endif
#if OS_WIN
  HANDLE h = CreateFile(zFilename,
     GENERIC_READ | GENERIC_WRITE,
     0,
     NULL,
     CREATE_ALWAYS,
     FILE_ATTRIBUTE_NORMAL | FILE_FLAG_RANDOM_ACCESS,
     NULL
  );
  if( h==INVALID_HANDLE_VALUE ){
    return SQLITE_CANTOPEN;
  }
  id->h = h;
  id->locked = 0;
  return SQLITE_OK;
#endif
}

/*
** Attempt to open a new file for read-only access.
**
** On success, write the file handle into *id and return SQLITE_OK.
**
** On failure, return SQLITE_CANTOPEN.
*/
int sqliteOsOpenReadOnly(const char *zFilename, OsFile *id){
#if OS_UNIX
  id->fd = open(zFilename, O_RDONLY);
  if( id->fd<0 ){
    return SQLITE_CANTOPEN;
  }
  sqliteOsEnterMutex();
  id->pLock = findLockInfo(id->fd);
  sqliteOsLeaveMutex();
  if( id->pLock==0 ){
    close(id->fd);
    return SQLITE_NOMEM;
  }
  id->locked = 0;
  return SQLITE_OK;
#endif
#if OS_WIN
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
  return SQLITE_OK;
#endif
}

/*
** Create a temporary file name in zBuf.  zBuf must be big enough to
** hold at least SQLITE_TEMPNAME_SIZE characters.
*/
int sqliteOsTempFileName(char *zBuf){
#if OS_UNIX
  static const char *azDirs[] = {
     ".",
     "/var/tmp",
     "/usr/tmp",
     "/tmp",
  };
  static char zChars[] =
    "abcdefghijklmnopqrstuvwxyz"
    "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    "0123456789";
  int i, j;
  struct stat buf;
  const char *zDir = ".";
  for(i=0; i<sizeof(azDirs)/sizeof(azDirs[0]); i++){
    if( stat(azDirs[i], &buf) ) continue;
    if( !S_ISDIR(buf.st_mode) ) continue;
    if( access(azDirs[i], 07) ) continue;
    zDir = azDirs[i];
    break;
  }
  do{
    sprintf(zBuf, "%s/sqlite_", zDir);
    j = strlen(zBuf);
    for(i=0; i<15; i++){
      int n = sqliteRandomByte() % (sizeof(zChars)-1);
      zBuf[j++] = zChars[n];
    }
    zBuf[j] = 0;
  }while( access(zBuf,0)==0 );
#endif
#if OS_WIN
  static char zChars[] =
    "abcdefghijklmnopqrstuvwxyz"
    "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    "0123456789";
  int i, j;
  char zTempPath[SQLITE_TEMPNAME_SIZE];
  GetTempPath(SQLITE_TEMPNAME_SIZE-30, zTempPath);
  for(;;){
    sprintf(zBuf, "%s/sqlite_", zTempPath);
    j = strlen(zBuf);
    for(i=0; i<15; i++){
      int n = sqliteRandomByte() % sizeof(zChars);
      zBuf[j++] = zChars[n];
    }
    zBuf[j] = 0;
    if( !sqliteOsFileExists(zBuf) ) break;
  }
#endif
  return SQLITE_OK; 
}

/*
** Close a file
*/
int sqliteOsClose(OsFile *id){
#if OS_UNIX
  close(id->fd);
  sqliteOsEnterMutex();
  releaseLockInfo(id->pLock);
  sqliteOsLeaveMutex();
  return SQLITE_OK;
#endif
#if OS_WIN
  CloseHandle(id->h);
  return SQLITE_OK;
#endif
}

/*
** Read data from a file into a buffer.  Return SQLITE_OK if all
** bytes were read successfully and SQLITE_IOERR if anything goes
** wrong.
*/
int sqliteOsRead(OsFile *id, void *pBuf, int amt){
#if OS_UNIX
  int got;
  SimulateIOError(SQLITE_IOERR);
  got = read(id->fd, pBuf, amt);
  if( got<0 ) got = 0;
  return got==amt ? SQLITE_OK : SQLITE_IOERR;
#endif
#if OS_WIN
  DWORD got;
  SimulateIOError(SQLITE_IOERR);
  if( !ReadFile(id->h, pBuf, amt, &got, 0) ){
    got = 0;
  }
  return got==amt ? SQLITE_OK : SQLITE_IOERR;
#endif
}

/*
** Write data from a buffer into a file.  Return SQLITE_OK on success
** or some other error code on failure.
*/
int sqliteOsWrite(OsFile *id, const void *pBuf, int amt){
#if OS_UNIX
  int wrote;
  SimulateIOError(SQLITE_IOERR);
  wrote = write(id->fd, pBuf, amt);
  if( wrote<amt ) return SQLITE_FULL;
  return SQLITE_OK;
#endif
#if OS_WIN
  DWORD wrote;
  SimulateIOError(SQLITE_IOERR);
  if( !WriteFile(id->h, pBuf, amt, &wrote, 0) || wrote<amt ){
    return SQLITE_FULL;
  }
  return SQLITE_OK;
#endif
}

/*
** Move the read/write pointer in a file.
*/
int sqliteOsSeek(OsFile *id, int offset){
#if OS_UNIX
  lseek(id->fd, offset, SEEK_SET);
  return SQLITE_OK;
#endif
#if OS_WIN
  SetFilePointer(id->h, offset, 0, FILE_BEGIN);
  return SQLITE_OK;
#endif
}

/*
** Make sure all writes to a particular file are committed to disk.
*/
int sqliteOsSync(OsFile *id){
  SimulateIOError(SQLITE_IOERR);
#if OS_UNIX
  return fsync(id->fd)==0 ? SQLITE_OK : SQLITE_IOERR;
#endif
#if OS_WIN
  return FlushFileBuffers(id->h) ? SQLITE_OK : SQLITE_IOERR;
#endif
}

/*
** Truncate an open file to a specified size
*/
int sqliteOsTruncate(OsFile *id, int nByte){
  SimulateIOError(SQLITE_IOERR);
#if OS_UNIX
  return ftruncate(id->fd, nByte)==0 ? SQLITE_OK : SQLITE_IOERR;
#endif
#if OS_WIN
  SetFilePointer(id->h, nByte, 0, FILE_BEGIN);
  SetEndOfFile(id->h);
  return SQLITE_OK;
#endif
}

/*
** Determine the current size of a file in bytes
*/
int sqliteOsFileSize(OsFile *id, int *pSize){
#if OS_UNIX
  struct stat buf;
  SimulateIOError(SQLITE_IOERR);
  if( fstat(id->fd, &buf)!=0 ){
    return SQLITE_IOERR;
  }
  *pSize = buf.st_size;
  return SQLITE_OK;
#endif
#if OS_WIN
  SimulateIOError(SQLITE_IOERR);
  *pSize = GetFileSize(id->h, 0);
  return SQLITE_OK;
#endif
}


/*
** Change the status of the lock on the file "id" to be a readlock.
** If the file was write locked, then this reduces the lock to a read.
** If the file was read locked, then this acquires a new read lock.
**
** Return SQLITE_OK on success and SQLITE_BUSY on failure.
*/
int sqliteOsReadLock(OsFile *id){
#if OS_UNIX
  int rc;
  sqliteOsEnterMutex();
  if( id->pLock->cnt>0 ){
    if( !id->locked ){
      id->pLock->cnt++;
      id->locked = 1;
    }
    rc = SQLITE_OK;
  }else if( id->locked || id->pLock->cnt==0 ){
    struct flock lock;
    lock.l_type = F_RDLCK;
    lock.l_whence = SEEK_SET;
    lock.l_start = lock.l_len = 0L;
    if( fcntl(id->fd, F_SETLK, &lock)!=0 ){
      rc = SQLITE_BUSY;
    }else{
      rc = SQLITE_OK;
      id->pLock->cnt = 1;
      id->locked = 1;
    }
  }else{
    rc = SQLITE_BUSY;
  }
  sqliteOsLeaveMutex();
  return rc;
#endif
#if OS_WIN
  int rc;
  if( id->locked ){
    rc = SQLITE_OK;
  }else if( LockFile(id->h, 0, 0, 1024, 0) ){
    rc = SQLITE_OK;
    id->locked = 1;
  }else{
    rc = SQLITE_BUSY;
  }
  return rc;
#endif
}

/*
** Change the lock status to be an exclusive or write lock.  Return
** SQLITE_OK on success and SQLITE_BUSY on a failure.
*/
int sqliteOsWriteLock(OsFile *id){
#if OS_UNIX
  int rc;
  sqliteOsEnterMutex();
  if( id->pLock->cnt==0 || (id->pLock->cnt==1 && id->locked==1) ){
    struct flock lock;
    lock.l_type = F_WRLCK;
    lock.l_whence = SEEK_SET;
    lock.l_start = lock.l_len = 0L;
    if( fcntl(id->fd, F_SETLK, &lock)!=0 ){
      rc = SQLITE_BUSY;
    }else{
      rc = SQLITE_OK;
      id->pLock->cnt = -1;
      id->locked = 1;
    }
  }else{
    rc = SQLITE_BUSY;
  }
  sqliteOsLeaveMutex();
  return rc;
#endif
#if OS_WIN
  int rc;
  if( id->locked ){
    rc = SQLITE_OK;
  }else if( LockFile(id->h, 0, 0, 1024, 0) ){
    rc = SQLITE_OK;
    id->locked = 1;
  }else{
    rc = SQLITE_BUSY;
  }
  return rc;
#endif
}

/*
** Unlock the given file descriptor.  If the file descriptor was
** not previously locked, then this routine is a no-op.
*/
int sqliteOsUnlock(OsFile *id){
#if OS_UNIX
  int rc;
  if( !id->locked ) return SQLITE_OK;
  sqliteOsEnterMutex();
  assert( id->pLock->cnt!=0 );
  if( id->pLock->cnt>1 ){
    id->pLock->cnt--;
    rc = SQLITE_OK;
  }else{
    struct flock lock;
    lock.l_type = F_UNLCK;
    lock.l_whence = SEEK_SET;
    lock.l_start = lock.l_len = 0L;
    if( fcntl(id->fd, F_SETLK, &lock)!=0 ){
      rc = SQLITE_BUSY;
    }else{
      rc = SQLITE_OK;
      id->pLock->cnt = 0;
    }
  }
  sqliteOsLeaveMutex();
  id->locked = 0;
  return rc;
#endif
#if OS_WIN
  int rc;
  if( !id->locked ){
    rc = SQLITE_OK;
  }else if( UnlockFile(id->h, 0, 0, 1024, 0) ){
    rc = SQLITE_OK;
    id->locked = 0;
  }else{
    rc = SQLITE_BUSY;
  }
  return rc;
#endif
}

/*
** Get information to seed the random number generator.
*/
int sqliteOsRandomSeed(char *zBuf){
  static int once = 1;
#if OS_UNIX
  int pid;
  time((time_t*)zBuf);
  pid = getpid();
  memcpy(&zBuf[sizeof(time_t)], &pid, sizeof(pid));
#endif
#if OS_WIN
  GetSystemTime((LPSYSTEMTIME)zBuf);
#endif
  if( once ){
    int seed;
    memcpy(&seed, zBuf, sizeof(seed));
    srand(seed);
    once = 0;
  }
  return SQLITE_OK;
}

/*
** Sleep for a little while.  Return the amount of time slept.
*/
int sqliteOsSleep(int ms){
#if OS_UNIX
#if defined(HAVE_USLEEP) && HAVE_USLEEP
  usleep(ms*1000);
  return ms;
#else
  sleep((ms+999)/1000);
  return 1000*((ms+999)/1000);
#endif
#endif
#if OS_WIN
  Sleep(ms);
  return ms;
#endif
}

/*
** The following pair of routine implement mutual exclusion for
** multi-threaded processes.  Only a single thread is allowed to
** executed code that is surrounded by EnterMutex() and LeaveMutex().
**
** SQLite uses only a single Mutex.  There is not much critical
** code and what little there is executes quickly and without blocking.
**
****** TBD:  The mutex is currently unimplemented.  Until it is
****** implemented, SQLite is not threadsafe.
*/
static int inMutex = 0;
void sqliteOsEnterMutex(){
  assert( !inMutex );
  inMutex = 1;
}
void sqliteOsLeaveMutex(){
  assert( inMutex );
  inMutex = 0;
}

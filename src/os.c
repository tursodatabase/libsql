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
** systems.  The purpose of this file is to provide a uniform abstract
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
** An instance of the following structure serves as the key used
** to locate a particular lockInfo structure given its inode. 
*/
struct inodeKey {
  dev_t dev;   /* Device number */
  ino_t ino;   /* Inode number */
};

/*
** An instance of the following structure is allocated for each inode.
** A single inode can have multiple file descriptors, so each OsFile structure
** contains a pointer to an instance of this object.
*/
struct lockInfo {
  struct inodeKey key;  /* The lookup key */
  int cnt;              /* 0: unlocked.  -1: write lock.  >=1: read lock */
  int nRef;             /* Number of pointers to this structure */
};

/* 
** This hash table maps inodes (in the form of inodeKey structures) into
** pointers to lockInfo structures.
*/
static Hash lockHash = { SQLITE_HASH_BINARY, 0, 0, 0, 0, 0 };

/*
** Given a file descriptor, locate a lockInfo structure that describes
** that file descriptor.  Create a new one if necessary.
*/
static struct lockInfo *findLockInfo(int fd){
  int rc;
  struct inodeKey key;
  struct stat statbuf;
  struct lockInfo *pInfo;
  rc = fstat(fd, &statbuf);
  if( rc!=0 ) return 0;
  key.dev = statbuf.st_dev;
  key.ino = statbuf.st_ino;
  pInfo = (struct lockInfo*)sqliteHashFind(&lockHash, &key, sizeof(key));
  if( pInfo==0 ){
    pInfo = sqliteMalloc( sizeof(*pInfo) );
    pInfo->key = key;
    pInfo->nRef = 1;
    pInfo->cnt = 0;
    sqliteHashInsert(&lockHash, &pInfo->key, sizeof(key), pInfo);
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
** On success, a handle for the open file is written to *pResult
** and *pReadonly is set to 0 if the file was opened for reading and
** writing or 1 if the file was opened read-only.  The function returns
** SQLITE_OK.
**
** On failure, the function returns SQLITE_CANTOPEN and leaves
** *pResulst and *pReadonly unchanged.
*/
int sqliteOsOpenReadWrite(
  const char *zFilename,
  OsFile *pResult,
  int *pReadonly
){
#if OS_UNIX
  OsFile s;
  s.fd = open(zFilename, O_RDWR|O_CREAT, 0644);
  if( s.fd<0 ){
    s.fd = open(zFilename, O_RDONLY);
    if( s.fd<0 ){
      return SQLITE_CANTOPEN; 
    }
    *pReadonly = 1;
  }else{
    *pReadonly = 0;
  }
  sqliteOsEnterMutex();
  s.pLock = findLockInfo(s.fd);
  sqliteOsLeaveMutex();
  if( s.pLock==0 ){
    close(s.fd);
    return SQLITE_NOMEM;
  }
  *pResult = s;
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
    HANDLE h = CreateFile(zFilename,
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
  *pResult = h;
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
** On success, write the file handle into *pResult and return SQLITE_OK.
**
** On failure, return SQLITE_CANTOPEN.
*/
int sqliteOsOpenExclusive(const char *zFilename, OsFile *pResult){
#if OS_UNIX
  OsFile s;
  if( access(zFilename, 0)==0 ){
    return SQLITE_CANTOPEN;
  }
#ifndef O_NOFOLLOW
# define O_NOFOLLOW 0
#endif
  s.fd = open(zFilename, O_RDWR|O_CREAT|O_EXCL|O_NOFOLLOW, 0600);
  if( s.fd<0 ){
    return SQLITE_CANTOPEN;
  }
  sqliteOsEnterMutex();
  s.pLock = findLockInfo(s.fd);
  sqliteOsLeaveMutex();
  if( s.pLock==0 ){
    close(s.fd);
    return SQLITE_NOMEM;
  }
  *pResult = s;
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
  *pResult = h;
  return SQLITE_OK;
#endif
}

/*
** Attempt to open a new file for read-only access.
**
** On success, write the file handle into *pResult and return SQLITE_OK.
**
** On failure, return SQLITE_CANTOPEN.
*/
int sqliteOsOpenReadOnly(const char *zFilename, OsFile *pResult){
#if OS_UNIX
  OsFile s;
  s.fd = open(zFilename, O_RDONLY);
  if( s.fd<0 ){
    return SQLITE_CANTOPEN;
  }
  sqliteOsEnterMutex();
  s.pLock = findLockInfo(s.fd);
  sqliteOsLeaveMutex();
  if( s.pLock==0 ){
    close(s.fd);
    return SQLITE_NOMEM;
  }
  *pResult = s;
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
  *pResult = h;
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
int sqliteOsClose(OsFile id){
#if OS_UNIX
  close(id.fd);
  sqliteOsEnterMutex();
  releaseLockInfo(id.pLock);
  sqliteOsLeaveMutex();
  return SQLITE_OK;
#endif
#if OS_WIN
  CloseHandle(id);
  return SQLITE_OK;
#endif
}

/*
** Read data from a file into a buffer.  Return the number of
** bytes actually read.
*/
int sqliteOsRead(OsFile id, void *pBuf, int amt){
#if OS_UNIX
  int got;
  got = read(id.fd, pBuf, amt);
  if( got<0 ) got = 0;
  return got==amt ? SQLITE_OK : SQLITE_IOERR;
#endif
#if OS_WIN
  DWORD got;
  if( !ReadFile(id, pBuf, amt, &got, 0) ){
    got = 0;
  }
  return got==amt ? SQLITE_OK : SQLITE_IOERR;
#endif
}

/*
** Write data from a buffer into a file.  Return SQLITE_OK on success
** or some other error code on failure.
*/
int sqliteOsWrite(OsFile id, const void *pBuf, int amt){
#if OS_UNIX
  int wrote;
  wrote = write(id.fd, pBuf, amt);
  if( wrote<amt ) return SQLITE_FULL;
  return SQLITE_OK;
#endif
#if OS_WIN
  DWORD wrote;
  if( !WriteFile(id, pBuf, amt, &wrote, 0) || wrote<amt ){
    return SQLITE_FULL;
  }
  return SQLITE_OK;
#endif
}

/*
** Move the read/write pointer in a file.
*/
int sqliteOsSeek(OsFile id, int offset){
#if OS_UNIX
  lseek(id.fd, offset, SEEK_SET);
  return SQLITE_OK;
#endif
#if OS_WIN
  SetFilePointer(id, offset, 0, FILE_BEGIN);
  return SQLITE_OK;
#endif
}

/*
** Make sure all writes to a particular file are committed to disk.
*/
int sqliteOsSync(OsFile id){
#if OS_UNIX
  return fsync(id.fd)==0 ? SQLITE_OK : SQLITE_IOERR;
#endif
#if OS_WIN
  return FlushFileBuffers(id) ? SQLITE_OK : SQLITE_IOERR;
#endif
}

/*
** Truncate an open file to a specified size
*/
int sqliteOsTruncate(OsFile id, int nByte){
#if OS_UNIX
  return ftruncate(id.fd, nByte)==0 ? SQLITE_OK : SQLITE_IOERR;
#endif
#if OS_WIN
  SetFilePointer(id, nByte, 0, FILE_BEGIN);
  SetEndOfFile(id);
  return SQLITE_OK;
#endif
}

/*
** Determine the current size of a file in bytes
*/
int sqliteOsFileSize(OsFile id, int *pSize){
#if OS_UNIX
  struct stat buf;
  if( fstat(id.fd, &buf)!=0 ){
    return SQLITE_IOERR;
  }
  *pSize = buf.st_size;
  return SQLITE_OK;
#endif
#if OS_WIN
  *pSize = GetFileSize(id, 0);
  return SQLITE_OK;
#endif
}


/*
** Get a read or write lock on a file.
*/
int sqliteOsLock(OsFile id, int wrlock){
#if OS_UNIX
  int rc;
  int needSysLock;
  sqliteOsEnterMutex();
  if( wrlock ){
    if( id.pLock->cnt!=0 ){
      rc = SQLITE_BUSY;
    }else{
      rc = SQLITE_OK;
      id.pLock->cnt = -1;
      needSysLock = 1;
    }
  }else{
    if( id.pLock<0 ){
      rc = SQLITE_BUSY;
    }else{
      rc = SQLITE_OK;
      needSysLock = id.pLock->cnt==0;
      id.pLock->cnt++;
    }
  }
  sqliteOsLeaveMutex();      
  if( rc==SQLITE_OK && needSysLock ){ 
    struct flock lock;
    lock.l_type = wrlock ? F_WRLCK : F_RDLCK;
    lock.l_whence = SEEK_SET;
    lock.l_start = lock.l_len = 0L;
    rc = fcntl(id.fd, F_SETLK, &lock)==0 ? SQLITE_OK : SQLITE_BUSY;
  }
  return rc;
#endif
#if OS_WIN
  if( !LockFile(id, 0, 0, 1024, 0) ){
    return SQLITE_BUSY;
  }
  return SQLITE_OK;
#endif
}

/*
** Release the read or write lock from a file.
*/
int sqliteOsUnlock(OsFile id){
#if OS_UNIX
  int rc;
  int needSysUnlock;

  sqliteOsEnterMutex();
  if( id.pLock->cnt<0 ){
    needSysUnlock = 1;
    id.pLock->cnt = 0;
  }else if( id.pLock->cnt>0 ){
    id.pLock->cnt--;
    needSysUnlock = id.pLock->cnt==0;
  }else{
    rc = SQLITE_OK;
    needSysUnlock = 0;
  }
  sqliteOsLeaveMutex();
  if( needSysUnlock ){
    struct flock lock;
    lock.l_type = F_UNLCK;
    lock.l_whence = SEEK_SET;
    lock.l_start = lock.l_len = 0L;
    rc = fcntl(id.fd, F_SETLK, &lock)==0 ? SQLITE_OK : SQLITE_IOERR;
  }
  return rc;
#endif
#if OS_WIN
  return UnlockFile(id, 0, 0, 1024, 0) ? SQLITE_OK : SQLITE_IOERR;
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

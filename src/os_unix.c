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
** This file contains the VFS implementation for unix-like operating systems
** include Linux, MacOSX, *BSD, QNX, VxWorks, AIX, HPUX, and others.
**
** There are actually several different VFS implementations in this file.
** The differences are in the way that file locking is done.  The default
** implementation uses Posix Advisory Locks.  Alternative implementations
** use flock(), dot-files, various proprietary locking schemas, or simply
** skip locking all together.
**
** This source file is organized into divisions where the logic for various
** subfunctions is contained within the appropriate division.  PLEASE
** KEEP THE STRUCTURE OF THIS FILE INTACT.  New code should be placed
** in the correct division and should be clearly labeled.
**
** The layout of divisions is as follows:
**
**   *  General-purpose declarations and utility functions.
**   *  Unique file ID logic used by VxWorks.
**   *  Various locking primitive implementations (all except proxy locking):
**      + for Posix Advisory Locks
**      + for no-op locks
**      + for dot-file locks
**      + for flock() locking
**      + for named semaphore locks (VxWorks only)
**      + for AFP filesystem locks (MacOSX only)
**   *  sqlite3_file methods not associated with locking.
**   *  Definitions of sqlite3_io_methods objects for all locking
**      methods plus "finder" functions for each locking method.
**   *  sqlite3_vfs method implementations.
**   *  Locking primitives for the proxy uber-locking-method. (MacOSX only)
**   *  Definitions of sqlite3_vfs objects for all locking methods
**      plus implementations of sqlite3_os_init() and sqlite3_os_end().
*/
#include "sqliteInt.h"
#if SQLITE_OS_UNIX              /* This file is used on unix only */

/*
** There are various methods for file locking used for concurrency
** control:
**
**   1. POSIX locking (the default),
**   2. No locking,
**   3. Dot-file locking,
**   4. flock() locking,
**   5. AFP locking (OSX only),
**   6. Named POSIX semaphores (VXWorks only),
**   7. proxy locking. (OSX only)
**
** Styles 4, 5, and 7 are only available of SQLITE_ENABLE_LOCKING_STYLE
** is defined to 1.  The SQLITE_ENABLE_LOCKING_STYLE also enables automatic
** selection of the appropriate locking style based on the filesystem
** where the database is located.  
*/
#if !defined(SQLITE_ENABLE_LOCKING_STYLE)
#  if defined(__APPLE__)
#    define SQLITE_ENABLE_LOCKING_STYLE 1
#  else
#    define SQLITE_ENABLE_LOCKING_STYLE 0
#  endif
#endif

/*
** Define the OS_VXWORKS pre-processor macro to 1 if building on 
** vxworks, or 0 otherwise.
*/
#ifndef OS_VXWORKS
#  if defined(__RTP__) || defined(_WRS_KERNEL)
#    define OS_VXWORKS 1
#  else
#    define OS_VXWORKS 0
#  endif
#endif

/*
** These #defines should enable >2GB file support on Posix if the
** underlying operating system supports it.  If the OS lacks
** large file support, these should be no-ops.
**
** Large file support can be disabled using the -DSQLITE_DISABLE_LFS switch
** on the compiler command line.  This is necessary if you are compiling
** on a recent machine (ex: RedHat 7.2) but you want your code to work
** on an older machine (ex: RedHat 6.0).  If you compile on RedHat 7.2
** without this option, LFS is enable.  But LFS does not exist in the kernel
** in RedHat 6.0, so the code won't work.  Hence, for maximum binary
** portability you should omit LFS.
**
** The previous paragraph was written in 2005.  (This paragraph is written
** on 2008-11-28.) These days, all Linux kernels support large files, so
** you should probably leave LFS enabled.  But some embedded platforms might
** lack LFS in which case the SQLITE_DISABLE_LFS macro might still be useful.
*/
#ifndef SQLITE_DISABLE_LFS
# define _LARGE_FILE       1
# ifndef _FILE_OFFSET_BITS
#   define _FILE_OFFSET_BITS 64
# endif
# define _LARGEFILE_SOURCE 1
#endif

/*
** standard include files.
*/
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <time.h>
#include <sys/time.h>
#include <errno.h>

#if SQLITE_ENABLE_LOCKING_STYLE
# include <sys/ioctl.h>
# if OS_VXWORKS
#  include <semaphore.h>
#  include <limits.h>
# else
#  include <sys/file.h>
#  include <sys/param.h>
#  include <sys/mount.h>
# endif
#endif /* SQLITE_ENABLE_LOCKING_STYLE */

/*
** If we are to be thread-safe, include the pthreads header and define
** the SQLITE_UNIX_THREADS macro.
*/
#if SQLITE_THREADSAFE
# include <pthread.h>
# define SQLITE_UNIX_THREADS 1
#endif

/*
** Default permissions when creating a new file
*/
#ifndef SQLITE_DEFAULT_FILE_PERMISSIONS
# define SQLITE_DEFAULT_FILE_PERMISSIONS 0644
#endif

/*
 ** Default permissions when creating auto proxy dir
 */
#ifndef SQLITE_DEFAULT_PROXYDIR_PERMISSIONS
# define SQLITE_DEFAULT_PROXYDIR_PERMISSIONS 0755
#endif

/*
** Maximum supported path-length.
*/
#define MAX_PATHNAME 512

/*
** Only set the lastErrno if the error code is a real error and not 
** a normal expected return code of SQLITE_BUSY or SQLITE_OK
*/
#define IS_LOCK_ERROR(x)  ((x != SQLITE_OK) && (x != SQLITE_BUSY))


/*
** Sometimes, after a file handle is closed by SQLite, the file descriptor
** cannot be closed immediately. In these cases, instances of the following
** structure are used to store the file descriptor while waiting for an
** opportunity to either close or reuse it.
*/
typedef struct UnixUnusedFd UnixUnusedFd;
struct UnixUnusedFd {
  int fd;                   /* File descriptor to close */
  int flags;                /* Flags this file descriptor was opened with */
  UnixUnusedFd *pNext;      /* Next unused file descriptor on same file */
};

/*
** The unixFile structure is subclass of sqlite3_file specific to the unix
** VFS implementations.
*/
typedef struct unixFile unixFile;
struct unixFile {
  sqlite3_io_methods const *pMethod;  /* Always the first entry */
  struct unixOpenCnt *pOpen;       /* Info about all open fd's on this inode */
  struct unixLockInfo *pLock;      /* Info about locks on this inode */
  int h;                           /* The file descriptor */
  int dirfd;                       /* File descriptor for the directory */
  unsigned char locktype;          /* The type of lock held on this fd */
  int lastErrno;                   /* The unix errno from the last I/O error */
  void *lockingContext;            /* Locking style specific state */
  UnixUnusedFd *pUnused;           /* Pre-allocated UnixUnusedFd */
  int fileFlags;                   /* Miscellanous flags */
#if SQLITE_ENABLE_LOCKING_STYLE
  int openFlags;                   /* The flags specified at open() */
#endif
#if SQLITE_THREADSAFE && defined(__linux__)
  pthread_t tid;                   /* The thread that "owns" this unixFile */
#endif
#if OS_VXWORKS
  int isDelete;                    /* Delete on close if true */
  struct vxworksFileId *pId;       /* Unique file ID */
#endif
#ifndef NDEBUG
  /* The next group of variables are used to track whether or not the
  ** transaction counter in bytes 24-27 of database files are updated
  ** whenever any part of the database changes.  An assertion fault will
  ** occur if a file is updated without also updating the transaction
  ** counter.  This test is made to avoid new problems similar to the
  ** one described by ticket #3584. 
  */
  unsigned char transCntrChng;   /* True if the transaction counter changed */
  unsigned char dbUpdate;        /* True if any part of database file changed */
  unsigned char inNormalWrite;   /* True if in a normal write operation */
#endif
#ifdef SQLITE_TEST
  /* In test mode, increase the size of this structure a bit so that 
  ** it is larger than the struct CrashFile defined in test6.c.
  */
  char aPadding[32];
#endif
};

/*
** The following macros define bits in unixFile.fileFlags
*/
#define SQLITE_WHOLE_FILE_LOCKING  0x0001   /* Use whole-file locking */

/*
** Include code that is common to all os_*.c files
*/
#include "os_common.h"

/*
** Define various macros that are missing from some systems.
*/
#ifndef O_LARGEFILE
# define O_LARGEFILE 0
#endif
#ifdef SQLITE_DISABLE_LFS
# undef O_LARGEFILE
# define O_LARGEFILE 0
#endif
#ifndef O_NOFOLLOW
# define O_NOFOLLOW 0
#endif
#ifndef O_BINARY
# define O_BINARY 0
#endif

/*
** The DJGPP compiler environment looks mostly like Unix, but it
** lacks the fcntl() system call.  So redefine fcntl() to be something
** that always succeeds.  This means that locking does not occur under
** DJGPP.  But it is DOS - what did you expect?
*/
#ifdef __DJGPP__
# define fcntl(A,B,C) 0
#endif

/*
** The threadid macro resolves to the thread-id or to 0.  Used for
** testing and debugging only.
*/
#if SQLITE_THREADSAFE
#define threadid pthread_self()
#else
#define threadid 0
#endif


/*
** Helper functions to obtain and relinquish the global mutex. The
** global mutex is used to protect the unixOpenCnt, unixLockInfo and
** vxworksFileId objects used by this file, all of which may be 
** shared by multiple threads.
**
** Function unixMutexHeld() is used to assert() that the global mutex 
** is held when required. This function is only used as part of assert() 
** statements. e.g.
**
**   unixEnterMutex()
**     assert( unixMutexHeld() );
**   unixEnterLeave()
*/
static void unixEnterMutex(void){
  sqlite3_mutex_enter(sqlite3MutexAlloc(SQLITE_MUTEX_STATIC_MASTER));
}
static void unixLeaveMutex(void){
  sqlite3_mutex_leave(sqlite3MutexAlloc(SQLITE_MUTEX_STATIC_MASTER));
}
#ifdef SQLITE_DEBUG
static int unixMutexHeld(void) {
  return sqlite3_mutex_held(sqlite3MutexAlloc(SQLITE_MUTEX_STATIC_MASTER));
}
#endif


#ifdef SQLITE_DEBUG
/*
** Helper function for printing out trace information from debugging
** binaries. This returns the string represetation of the supplied
** integer lock-type.
*/
static const char *locktypeName(int locktype){
  switch( locktype ){
    case NO_LOCK: return "NONE";
    case SHARED_LOCK: return "SHARED";
    case RESERVED_LOCK: return "RESERVED";
    case PENDING_LOCK: return "PENDING";
    case EXCLUSIVE_LOCK: return "EXCLUSIVE";
  }
  return "ERROR";
}
#endif

#ifdef SQLITE_LOCK_TRACE
/*
** Print out information about all locking operations.
**
** This routine is used for troubleshooting locks on multithreaded
** platforms.  Enable by compiling with the -DSQLITE_LOCK_TRACE
** command-line option on the compiler.  This code is normally
** turned off.
*/
static int lockTrace(int fd, int op, struct flock *p){
  char *zOpName, *zType;
  int s;
  int savedErrno;
  if( op==F_GETLK ){
    zOpName = "GETLK";
  }else if( op==F_SETLK ){
    zOpName = "SETLK";
  }else{
    s = fcntl(fd, op, p);
    sqlite3DebugPrintf("fcntl unknown %d %d %d\n", fd, op, s);
    return s;
  }
  if( p->l_type==F_RDLCK ){
    zType = "RDLCK";
  }else if( p->l_type==F_WRLCK ){
    zType = "WRLCK";
  }else if( p->l_type==F_UNLCK ){
    zType = "UNLCK";
  }else{
    assert( 0 );
  }
  assert( p->l_whence==SEEK_SET );
  s = fcntl(fd, op, p);
  savedErrno = errno;
  sqlite3DebugPrintf("fcntl %d %d %s %s %d %d %d %d\n",
     threadid, fd, zOpName, zType, (int)p->l_start, (int)p->l_len,
     (int)p->l_pid, s);
  if( s==(-1) && op==F_SETLK && (p->l_type==F_RDLCK || p->l_type==F_WRLCK) ){
    struct flock l2;
    l2 = *p;
    fcntl(fd, F_GETLK, &l2);
    if( l2.l_type==F_RDLCK ){
      zType = "RDLCK";
    }else if( l2.l_type==F_WRLCK ){
      zType = "WRLCK";
    }else if( l2.l_type==F_UNLCK ){
      zType = "UNLCK";
    }else{
      assert( 0 );
    }
    sqlite3DebugPrintf("fcntl-failure-reason: %s %d %d %d\n",
       zType, (int)l2.l_start, (int)l2.l_len, (int)l2.l_pid);
  }
  errno = savedErrno;
  return s;
}
#define fcntl lockTrace
#endif /* SQLITE_LOCK_TRACE */



/*
** This routine translates a standard POSIX errno code into something
** useful to the clients of the sqlite3 functions.  Specifically, it is
** intended to translate a variety of "try again" errors into SQLITE_BUSY
** and a variety of "please close the file descriptor NOW" errors into 
** SQLITE_IOERR
** 
** Errors during initialization of locks, or file system support for locks,
** should handle ENOLCK, ENOTSUP, EOPNOTSUPP separately.
*/
static int sqliteErrorFromPosixError(int posixError, int sqliteIOErr) {
  switch (posixError) {
  case 0: 
    return SQLITE_OK;
    
  case EAGAIN:
  case ETIMEDOUT:
  case EBUSY:
  case EINTR:
  case ENOLCK:  
    /* random NFS retry error, unless during file system support 
     * introspection, in which it actually means what it says */
    return SQLITE_BUSY;
    
  case EACCES: 
    /* EACCES is like EAGAIN during locking operations, but not any other time*/
    if( (sqliteIOErr == SQLITE_IOERR_LOCK) || 
	(sqliteIOErr == SQLITE_IOERR_UNLOCK) || 
	(sqliteIOErr == SQLITE_IOERR_RDLOCK) ||
	(sqliteIOErr == SQLITE_IOERR_CHECKRESERVEDLOCK) ){
      return SQLITE_BUSY;
    }
    /* else fall through */
  case EPERM: 
    return SQLITE_PERM;
    
  case EDEADLK:
    return SQLITE_IOERR_BLOCKED;
    
#if EOPNOTSUPP!=ENOTSUP
  case EOPNOTSUPP: 
    /* something went terribly awry, unless during file system support 
     * introspection, in which it actually means what it says */
#endif
#ifdef ENOTSUP
  case ENOTSUP: 
    /* invalid fd, unless during file system support introspection, in which 
     * it actually means what it says */
#endif
  case EIO:
  case EBADF:
  case EINVAL:
  case ENOTCONN:
  case ENODEV:
  case ENXIO:
  case ENOENT:
  case ESTALE:
  case ENOSYS:
    /* these should force the client to close the file and reconnect */
    
  default: 
    return sqliteIOErr;
  }
}



/******************************************************************************
****************** Begin Unique File ID Utility Used By VxWorks ***************
**
** On most versions of unix, we can get a unique ID for a file by concatenating
** the device number and the inode number.  But this does not work on VxWorks.
** On VxWorks, a unique file id must be based on the canonical filename.
**
** A pointer to an instance of the following structure can be used as a
** unique file ID in VxWorks.  Each instance of this structure contains
** a copy of the canonical filename.  There is also a reference count.  
** The structure is reclaimed when the number of pointers to it drops to
** zero.
**
** There are never very many files open at one time and lookups are not
** a performance-critical path, so it is sufficient to put these
** structures on a linked list.
*/
struct vxworksFileId {
  struct vxworksFileId *pNext;  /* Next in a list of them all */
  int nRef;                     /* Number of references to this one */
  int nName;                    /* Length of the zCanonicalName[] string */
  char *zCanonicalName;         /* Canonical filename */
};

#if OS_VXWORKS
/* 
** All unique filenames are held on a linked list headed by this
** variable:
*/
static struct vxworksFileId *vxworksFileList = 0;

/*
** Simplify a filename into its canonical form
** by making the following changes:
**
**  * removing any trailing and duplicate /
**  * convert /./ into just /
**  * convert /A/../ where A is any simple name into just /
**
** Changes are made in-place.  Return the new name length.
**
** The original filename is in z[0..n-1].  Return the number of
** characters in the simplified name.
*/
static int vxworksSimplifyName(char *z, int n){
  int i, j;
  while( n>1 && z[n-1]=='/' ){ n--; }
  for(i=j=0; i<n; i++){
    if( z[i]=='/' ){
      if( z[i+1]=='/' ) continue;
      if( z[i+1]=='.' && i+2<n && z[i+2]=='/' ){
        i += 1;
        continue;
      }
      if( z[i+1]=='.' && i+3<n && z[i+2]=='.' && z[i+3]=='/' ){
        while( j>0 && z[j-1]!='/' ){ j--; }
        if( j>0 ){ j--; }
        i += 2;
        continue;
      }
    }
    z[j++] = z[i];
  }
  z[j] = 0;
  return j;
}

/*
** Find a unique file ID for the given absolute pathname.  Return
** a pointer to the vxworksFileId object.  This pointer is the unique
** file ID.
**
** The nRef field of the vxworksFileId object is incremented before
** the object is returned.  A new vxworksFileId object is created
** and added to the global list if necessary.
**
** If a memory allocation error occurs, return NULL.
*/
static struct vxworksFileId *vxworksFindFileId(const char *zAbsoluteName){
  struct vxworksFileId *pNew;         /* search key and new file ID */
  struct vxworksFileId *pCandidate;   /* For looping over existing file IDs */
  int n;                              /* Length of zAbsoluteName string */

  assert( zAbsoluteName[0]=='/' );
  n = (int)strlen(zAbsoluteName);
  pNew = sqlite3_malloc( sizeof(*pNew) + (n+1) );
  if( pNew==0 ) return 0;
  pNew->zCanonicalName = (char*)&pNew[1];
  memcpy(pNew->zCanonicalName, zAbsoluteName, n+1);
  n = vxworksSimplifyName(pNew->zCanonicalName, n);

  /* Search for an existing entry that matching the canonical name.
  ** If found, increment the reference count and return a pointer to
  ** the existing file ID.
  */
  unixEnterMutex();
  for(pCandidate=vxworksFileList; pCandidate; pCandidate=pCandidate->pNext){
    if( pCandidate->nName==n 
     && memcmp(pCandidate->zCanonicalName, pNew->zCanonicalName, n)==0
    ){
       sqlite3_free(pNew);
       pCandidate->nRef++;
       unixLeaveMutex();
       return pCandidate;
    }
  }

  /* No match was found.  We will make a new file ID */
  pNew->nRef = 1;
  pNew->nName = n;
  pNew->pNext = vxworksFileList;
  vxworksFileList = pNew;
  unixLeaveMutex();
  return pNew;
}

/*
** Decrement the reference count on a vxworksFileId object.  Free
** the object when the reference count reaches zero.
*/
static void vxworksReleaseFileId(struct vxworksFileId *pId){
  unixEnterMutex();
  assert( pId->nRef>0 );
  pId->nRef--;
  if( pId->nRef==0 ){
    struct vxworksFileId **pp;
    for(pp=&vxworksFileList; *pp && *pp!=pId; pp = &((*pp)->pNext)){}
    assert( *pp==pId );
    *pp = pId->pNext;
    sqlite3_free(pId);
  }
  unixLeaveMutex();
}
#endif /* OS_VXWORKS */
/*************** End of Unique File ID Utility Used By VxWorks ****************
******************************************************************************/


/******************************************************************************
*************************** Posix Advisory Locking ****************************
**
** POSIX advisory locks are broken by design.  ANSI STD 1003.1 (1996)
** section 6.5.2.2 lines 483 through 490 specify that when a process
** sets or clears a lock, that operation overrides any prior locks set
** by the same process.  It does not explicitly say so, but this implies
** that it overrides locks set by the same process using a different
** file descriptor.  Consider this test case:
**
**       int fd1 = open("./file1", O_RDWR|O_CREAT, 0644);
**       int fd2 = open("./file2", O_RDWR|O_CREAT, 0644);
**
** Suppose ./file1 and ./file2 are really the same file (because
** one is a hard or symbolic link to the other) then if you set
** an exclusive lock on fd1, then try to get an exclusive lock
** on fd2, it works.  I would have expected the second lock to
** fail since there was already a lock on the file due to fd1.
** But not so.  Since both locks came from the same process, the
** second overrides the first, even though they were on different
** file descriptors opened on different file names.
**
** This means that we cannot use POSIX locks to synchronize file access
** among competing threads of the same process.  POSIX locks will work fine
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
** (Aside: The use of inode numbers as unique IDs does not work on VxWorks.
** For VxWorks, we have to use the alternative unique ID system based on
** canonical filename and implemented in the previous division.)
**
** The sqlite3_file structure for POSIX is no longer just an integer file
** descriptor.  It is now a structure that holds the integer file
** descriptor and a pointer to a structure that describes the internal
** locks on the corresponding inode.  There is one locking structure
** per inode, so if the same inode is opened twice, both unixFile structures
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
**
** But wait:  there are yet more problems with POSIX advisory locks.
**
** If you close a file descriptor that points to a file that has locks,
** all locks on that file that are owned by the current process are
** released.  To work around this problem, each unixFile structure contains
** a pointer to an unixOpenCnt structure.  There is one unixOpenCnt structure
** per open inode, which means that multiple unixFile can point to a single
** unixOpenCnt.  When an attempt is made to close an unixFile, if there are
** other unixFile open on the same inode that are holding locks, the call
** to close() the file descriptor is deferred until all of the locks clear.
** The unixOpenCnt structure keeps a list of file descriptors that need to
** be closed and that list is walked (and cleared) when the last lock
** clears.
**
** Yet another problem:  LinuxThreads do not play well with posix locks.
**
** Many older versions of linux use the LinuxThreads library which is
** not posix compliant.  Under LinuxThreads, a lock created by thread
** A cannot be modified or overridden by a different thread B.
** Only thread A can modify the lock.  Locking behavior is correct
** if the appliation uses the newer Native Posix Thread Library (NPTL)
** on linux - with NPTL a lock created by thread A can override locks
** in thread B.  But there is no way to know at compile-time which
** threading library is being used.  So there is no way to know at
** compile-time whether or not thread A can override locks on thread B.
** We have to do a run-time check to discover the behavior of the
** current process.
**
** On systems where thread A is unable to modify locks created by
** thread B, we have to keep track of which thread created each
** lock.  Hence there is an extra field in the key to the unixLockInfo
** structure to record this information.  And on those systems it
** is illegal to begin a transaction in one thread and finish it
** in another.  For this latter restriction, there is no work-around.
** It is a limitation of LinuxThreads.
*/

/*
** Set or check the unixFile.tid field.  This field is set when an unixFile
** is first opened.  All subsequent uses of the unixFile verify that the
** same thread is operating on the unixFile.  Some operating systems do
** not allow locks to be overridden by other threads and that restriction
** means that sqlite3* database handles cannot be moved from one thread
** to another while locks are held.
**
** Version 3.3.1 (2006-01-15):  unixFile can be moved from one thread to
** another as long as we are running on a system that supports threads
** overriding each others locks (which is now the most common behavior)
** or if no locks are held.  But the unixFile.pLock field needs to be
** recomputed because its key includes the thread-id.  See the 
** transferOwnership() function below for additional information
*/
#if SQLITE_THREADSAFE && defined(__linux__)
# define SET_THREADID(X)   (X)->tid = pthread_self()
# define CHECK_THREADID(X) (threadsOverrideEachOthersLocks==0 && \
                            !pthread_equal((X)->tid, pthread_self()))
#else
# define SET_THREADID(X)
# define CHECK_THREADID(X) 0
#endif

/*
** An instance of the following structure serves as the key used
** to locate a particular unixOpenCnt structure given its inode.  This
** is the same as the unixLockKey except that the thread ID is omitted.
*/
struct unixFileId {
  dev_t dev;                  /* Device number */
#if OS_VXWORKS
  struct vxworksFileId *pId;  /* Unique file ID for vxworks. */
#else
  ino_t ino;                  /* Inode number */
#endif
};

/*
** An instance of the following structure serves as the key used
** to locate a particular unixLockInfo structure given its inode.
**
** If threads cannot override each others locks (LinuxThreads), then we
** set the unixLockKey.tid field to the thread ID.  If threads can override
** each others locks (Posix and NPTL) then tid is always set to zero.
** tid is omitted if we compile without threading support or on an OS
** other than linux.
*/
struct unixLockKey {
  struct unixFileId fid;  /* Unique identifier for the file */
#if SQLITE_THREADSAFE && defined(__linux__)
  pthread_t tid;  /* Thread ID of lock owner. Zero if not using LinuxThreads */
#endif
};

/*
** An instance of the following structure is allocated for each open
** inode.  Or, on LinuxThreads, there is one of these structures for
** each inode opened by each thread.
**
** A single inode can have multiple file descriptors, so each unixFile
** structure contains a pointer to an instance of this object and this
** object keeps a count of the number of unixFile pointing to it.
*/
struct unixLockInfo {
  struct unixLockKey lockKey;     /* The lookup key */
  int cnt;                        /* Number of SHARED locks held */
  int locktype;                   /* One of SHARED_LOCK, RESERVED_LOCK etc. */
  int nRef;                       /* Number of pointers to this structure */
  struct unixLockInfo *pNext;     /* List of all unixLockInfo objects */
  struct unixLockInfo *pPrev;     /*    .... doubly linked */
};

/*
** An instance of the following structure is allocated for each open
** inode.  This structure keeps track of the number of locks on that
** inode.  If a close is attempted against an inode that is holding
** locks, the close is deferred until all locks clear by adding the
** file descriptor to be closed to the pending list.
**
** TODO:  Consider changing this so that there is only a single file
** descriptor for each open file, even when it is opened multiple times.
** The close() system call would only occur when the last database
** using the file closes.
*/
struct unixOpenCnt {
  struct unixFileId fileId;   /* The lookup key */
  int nRef;                   /* Number of pointers to this structure */
  int nLock;                  /* Number of outstanding locks */
  UnixUnusedFd *pUnused;      /* Unused file descriptors to close */
#if OS_VXWORKS
  sem_t *pSem;                     /* Named POSIX semaphore */
  char aSemName[MAX_PATHNAME+2];   /* Name of that semaphore */
#endif
  struct unixOpenCnt *pNext, *pPrev;   /* List of all unixOpenCnt objects */
};

/*
** Lists of all unixLockInfo and unixOpenCnt objects.  These used to be hash
** tables.  But the number of objects is rarely more than a dozen and
** never exceeds a few thousand.  And lookup is not on a critical
** path so a simple linked list will suffice.
*/
static struct unixLockInfo *lockList = 0;
static struct unixOpenCnt *openList = 0;

/*
** This variable remembers whether or not threads can override each others
** locks.
**
**    0:  No.  Threads cannot override each others locks.  (LinuxThreads)
**    1:  Yes.  Threads can override each others locks.  (Posix & NLPT)
**   -1:  We don't know yet.
**
** On some systems, we know at compile-time if threads can override each
** others locks.  On those systems, the SQLITE_THREAD_OVERRIDE_LOCK macro
** will be set appropriately.  On other systems, we have to check at
** runtime.  On these latter systems, SQLTIE_THREAD_OVERRIDE_LOCK is
** undefined.
**
** This variable normally has file scope only.  But during testing, we make
** it a global so that the test code can change its value in order to verify
** that the right stuff happens in either case.
*/
#if SQLITE_THREADSAFE && defined(__linux__)
#  ifndef SQLITE_THREAD_OVERRIDE_LOCK
#    define SQLITE_THREAD_OVERRIDE_LOCK -1
#  endif
#  ifdef SQLITE_TEST
int threadsOverrideEachOthersLocks = SQLITE_THREAD_OVERRIDE_LOCK;
#  else
static int threadsOverrideEachOthersLocks = SQLITE_THREAD_OVERRIDE_LOCK;
#  endif
#endif

/*
** This structure holds information passed into individual test
** threads by the testThreadLockingBehavior() routine.
*/
struct threadTestData {
  int fd;                /* File to be locked */
  struct flock lock;     /* The locking operation */
  int result;            /* Result of the locking operation */
};

#if SQLITE_THREADSAFE && defined(__linux__)
/*
** This function is used as the main routine for a thread launched by
** testThreadLockingBehavior(). It tests whether the shared-lock obtained
** by the main thread in testThreadLockingBehavior() conflicts with a
** hypothetical write-lock obtained by this thread on the same file.
**
** The write-lock is not actually acquired, as this is not possible if 
** the file is open in read-only mode (see ticket #3472).
*/ 
static void *threadLockingTest(void *pArg){
  struct threadTestData *pData = (struct threadTestData*)pArg;
  pData->result = fcntl(pData->fd, F_GETLK, &pData->lock);
  return pArg;
}
#endif /* SQLITE_THREADSAFE && defined(__linux__) */


#if SQLITE_THREADSAFE && defined(__linux__)
/*
** This procedure attempts to determine whether or not threads
** can override each others locks then sets the 
** threadsOverrideEachOthersLocks variable appropriately.
*/
static void testThreadLockingBehavior(int fd_orig){
  int fd;
  int rc;
  struct threadTestData d;
  struct flock l;
  pthread_t t;

  fd = dup(fd_orig);
  if( fd<0 ) return;
  memset(&l, 0, sizeof(l));
  l.l_type = F_RDLCK;
  l.l_len = 1;
  l.l_start = 0;
  l.l_whence = SEEK_SET;
  rc = fcntl(fd_orig, F_SETLK, &l);
  if( rc!=0 ) return;
  memset(&d, 0, sizeof(d));
  d.fd = fd;
  d.lock = l;
  d.lock.l_type = F_WRLCK;
  if( pthread_create(&t, 0, threadLockingTest, &d)==0 ){
    pthread_join(t, 0);
  }
  close(fd);
  if( d.result!=0 ) return;
  threadsOverrideEachOthersLocks = (d.lock.l_type==F_UNLCK);
}
#endif /* SQLITE_THREADSAFE && defined(__linux__) */

/*
** Release a unixLockInfo structure previously allocated by findLockInfo().
**
** The mutex entered using the unixEnterMutex() function must be held
** when this function is called.
*/
static void releaseLockInfo(struct unixLockInfo *pLock){
  assert( unixMutexHeld() );
  if( pLock ){
    pLock->nRef--;
    if( pLock->nRef==0 ){
      if( pLock->pPrev ){
        assert( pLock->pPrev->pNext==pLock );
        pLock->pPrev->pNext = pLock->pNext;
      }else{
        assert( lockList==pLock );
        lockList = pLock->pNext;
      }
      if( pLock->pNext ){
        assert( pLock->pNext->pPrev==pLock );
        pLock->pNext->pPrev = pLock->pPrev;
      }
      sqlite3_free(pLock);
    }
  }
}

/*
** Release a unixOpenCnt structure previously allocated by findLockInfo().
**
** The mutex entered using the unixEnterMutex() function must be held
** when this function is called.
*/
static void releaseOpenCnt(struct unixOpenCnt *pOpen){
  assert( unixMutexHeld() );
  if( pOpen ){
    pOpen->nRef--;
    if( pOpen->nRef==0 ){
      if( pOpen->pPrev ){
        assert( pOpen->pPrev->pNext==pOpen );
        pOpen->pPrev->pNext = pOpen->pNext;
      }else{
        assert( openList==pOpen );
        openList = pOpen->pNext;
      }
      if( pOpen->pNext ){
        assert( pOpen->pNext->pPrev==pOpen );
        pOpen->pNext->pPrev = pOpen->pPrev;
      }
#if SQLITE_THREADSAFE && defined(__linux__)
      assert( !pOpen->pUnused || threadsOverrideEachOthersLocks==0 );
#endif

      /* If pOpen->pUnused is not null, then memory and file-descriptors
      ** are leaked.
      **
      ** This will only happen if, under Linuxthreads, the user has opened
      ** a transaction in one thread, then attempts to close the database
      ** handle from another thread (without first unlocking the db file).
      ** This is a misuse.  */
      sqlite3_free(pOpen);
    }
  }
}

/*
** Given a file descriptor, locate unixLockInfo and unixOpenCnt structures that
** describes that file descriptor.  Create new ones if necessary.  The
** return values might be uninitialized if an error occurs.
**
** The mutex entered using the unixEnterMutex() function must be held
** when this function is called.
**
** Return an appropriate error code.
*/
static int findLockInfo(
  unixFile *pFile,               /* Unix file with file desc used in the key */
  struct unixLockInfo **ppLock,  /* Return the unixLockInfo structure here */
  struct unixOpenCnt **ppOpen    /* Return the unixOpenCnt structure here */
){
  int rc;                        /* System call return code */
  int fd;                        /* The file descriptor for pFile */
  struct unixLockKey lockKey;    /* Lookup key for the unixLockInfo structure */
  struct unixFileId fileId;      /* Lookup key for the unixOpenCnt struct */
  struct stat statbuf;           /* Low-level file information */
  struct unixLockInfo *pLock = 0;/* Candidate unixLockInfo object */
  struct unixOpenCnt *pOpen;     /* Candidate unixOpenCnt object */

  assert( unixMutexHeld() );

  /* Get low-level information about the file that we can used to
  ** create a unique name for the file.
  */
  fd = pFile->h;
  rc = fstat(fd, &statbuf);
  if( rc!=0 ){
    pFile->lastErrno = errno;
#ifdef EOVERFLOW
    if( pFile->lastErrno==EOVERFLOW ) return SQLITE_NOLFS;
#endif
    return SQLITE_IOERR;
  }

#ifdef __APPLE__
  /* On OS X on an msdos filesystem, the inode number is reported
  ** incorrectly for zero-size files.  See ticket #3260.  To work
  ** around this problem (we consider it a bug in OS X, not SQLite)
  ** we always increase the file size to 1 by writing a single byte
  ** prior to accessing the inode number.  The one byte written is
  ** an ASCII 'S' character which also happens to be the first byte
  ** in the header of every SQLite database.  In this way, if there
  ** is a race condition such that another thread has already populated
  ** the first page of the database, no damage is done.
  */
  if( statbuf.st_size==0 ){
    rc = write(fd, "S", 1);
    if( rc!=1 ){
      return SQLITE_IOERR;
    }
    rc = fstat(fd, &statbuf);
    if( rc!=0 ){
      pFile->lastErrno = errno;
      return SQLITE_IOERR;
    }
  }
#endif

  memset(&lockKey, 0, sizeof(lockKey));
  lockKey.fid.dev = statbuf.st_dev;
#if OS_VXWORKS
  lockKey.fid.pId = pFile->pId;
#else
  lockKey.fid.ino = statbuf.st_ino;
#endif
#if SQLITE_THREADSAFE && defined(__linux__)
  if( threadsOverrideEachOthersLocks<0 ){
    testThreadLockingBehavior(fd);
  }
  lockKey.tid = threadsOverrideEachOthersLocks ? 0 : pthread_self();
#endif
  fileId = lockKey.fid;
  if( ppLock!=0 ){
    pLock = lockList;
    while( pLock && memcmp(&lockKey, &pLock->lockKey, sizeof(lockKey)) ){
      pLock = pLock->pNext;
    }
    if( pLock==0 ){
      pLock = sqlite3_malloc( sizeof(*pLock) );
      if( pLock==0 ){
        rc = SQLITE_NOMEM;
        goto exit_findlockinfo;
      }
      pLock->lockKey = lockKey;
      pLock->nRef = 1;
      pLock->cnt = 0;
      pLock->locktype = 0;
      pLock->pNext = lockList;
      pLock->pPrev = 0;
      if( lockList ) lockList->pPrev = pLock;
      lockList = pLock;
    }else{
      pLock->nRef++;
    }
    *ppLock = pLock;
  }
  if( ppOpen!=0 ){
    pOpen = openList;
    while( pOpen && memcmp(&fileId, &pOpen->fileId, sizeof(fileId)) ){
      pOpen = pOpen->pNext;
    }
    if( pOpen==0 ){
      pOpen = sqlite3_malloc( sizeof(*pOpen) );
      if( pOpen==0 ){
        releaseLockInfo(pLock);
        rc = SQLITE_NOMEM;
        goto exit_findlockinfo;
      }
      memset(pOpen, 0, sizeof(*pOpen));
      pOpen->fileId = fileId;
      pOpen->nRef = 1;
      pOpen->pNext = openList;
      if( openList ) openList->pPrev = pOpen;
      openList = pOpen;
    }else{
      pOpen->nRef++;
    }
    *ppOpen = pOpen;
  }

exit_findlockinfo:
  return rc;
}

/*
** If we are currently in a different thread than the thread that the
** unixFile argument belongs to, then transfer ownership of the unixFile
** over to the current thread.
**
** A unixFile is only owned by a thread on systems that use LinuxThreads.
**
** Ownership transfer is only allowed if the unixFile is currently unlocked.
** If the unixFile is locked and an ownership is wrong, then return
** SQLITE_MISUSE.  SQLITE_OK is returned if everything works.
*/
#if SQLITE_THREADSAFE && defined(__linux__)
static int transferOwnership(unixFile *pFile){
  int rc;
  pthread_t hSelf;
  if( threadsOverrideEachOthersLocks ){
    /* Ownership transfers not needed on this system */
    return SQLITE_OK;
  }
  hSelf = pthread_self();
  if( pthread_equal(pFile->tid, hSelf) ){
    /* We are still in the same thread */
    OSTRACE1("No-transfer, same thread\n");
    return SQLITE_OK;
  }
  if( pFile->locktype!=NO_LOCK ){
    /* We cannot change ownership while we are holding a lock! */
    return SQLITE_MISUSE;
  }
  OSTRACE4("Transfer ownership of %d from %d to %d\n",
            pFile->h, pFile->tid, hSelf);
  pFile->tid = hSelf;
  if (pFile->pLock != NULL) {
    releaseLockInfo(pFile->pLock);
    rc = findLockInfo(pFile, &pFile->pLock, 0);
    OSTRACE5("LOCK    %d is now %s(%s,%d)\n", pFile->h,
           locktypeName(pFile->locktype),
           locktypeName(pFile->pLock->locktype), pFile->pLock->cnt);
    return rc;
  } else {
    return SQLITE_OK;
  }
}
#else  /* if not SQLITE_THREADSAFE */
  /* On single-threaded builds, ownership transfer is a no-op */
# define transferOwnership(X) SQLITE_OK
#endif /* SQLITE_THREADSAFE */


/*
** This routine checks if there is a RESERVED lock held on the specified
** file by this or any other process. If such a lock is held, set *pResOut
** to a non-zero value otherwise *pResOut is set to zero.  The return value
** is set to SQLITE_OK unless an I/O error occurs during lock checking.
*/
static int unixCheckReservedLock(sqlite3_file *id, int *pResOut){
  int rc = SQLITE_OK;
  int reserved = 0;
  unixFile *pFile = (unixFile*)id;

  SimulateIOError( return SQLITE_IOERR_CHECKRESERVEDLOCK; );

  assert( pFile );
  unixEnterMutex(); /* Because pFile->pLock is shared across threads */

  /* Check if a thread in this process holds such a lock */
  if( pFile->pLock->locktype>SHARED_LOCK ){
    reserved = 1;
  }

  /* Otherwise see if some other process holds it.
  */
#ifndef __DJGPP__
  if( !reserved ){
    struct flock lock;
    lock.l_whence = SEEK_SET;
    lock.l_start = RESERVED_BYTE;
    lock.l_len = 1;
    lock.l_type = F_WRLCK;
    if (-1 == fcntl(pFile->h, F_GETLK, &lock)) {
      int tErrno = errno;
      rc = sqliteErrorFromPosixError(tErrno, SQLITE_IOERR_CHECKRESERVEDLOCK);
      pFile->lastErrno = tErrno;
    } else if( lock.l_type!=F_UNLCK ){
      reserved = 1;
    }
  }
#endif
  
  unixLeaveMutex();
  OSTRACE4("TEST WR-LOCK %d %d %d\n", pFile->h, rc, reserved);

  *pResOut = reserved;
  return rc;
}

/*
** Perform a file locking operation on a range of bytes in a file.
** The "op" parameter should be one of F_RDLCK, F_WRLCK, or F_UNLCK.
** Return 0 on success or -1 for failure.  On failure, write the error
** code into *pErrcode.
**
** If the SQLITE_WHOLE_FILE_LOCKING bit is clear, then only lock
** the range of bytes on the locking page between SHARED_FIRST and
** SHARED_SIZE.  If SQLITE_WHOLE_FILE_LOCKING is set, then lock all
** bytes from 0 up to but not including PENDING_BYTE, and all bytes
** that follow SHARED_FIRST.
**
** In other words, of SQLITE_WHOLE_FILE_LOCKING if false (the historical
** default case) then only lock a small range of bytes from SHARED_FIRST
** through SHARED_FIRST+SHARED_SIZE-1.  But if SQLITE_WHOLE_FILE_LOCKING is
** true then lock every byte in the file except for PENDING_BYTE and
** RESERVED_BYTE.
**
** SQLITE_WHOLE_FILE_LOCKING=true overlaps SQLITE_WHOLE_FILE_LOCKING=false
** and so the locking schemes are compatible.  One type of lock will
** effectively exclude the other type.  The reason for using the
** SQLITE_WHOLE_FILE_LOCKING=true is that by indicating the full range
** of bytes to be read or written, we give hints to NFS to help it
** maintain cache coherency.  On the other hand, whole file locking
** is slower, so we don't want to use it except for NFS.
*/
static int rangeLock(unixFile *pFile, int op, int *pErrcode){
  struct flock lock;
  int rc;
  lock.l_type = op;
  lock.l_start = SHARED_FIRST;
  lock.l_whence = SEEK_SET;
  if( (pFile->fileFlags & SQLITE_WHOLE_FILE_LOCKING)==0 ){
    lock.l_len = SHARED_SIZE;
    rc = fcntl(pFile->h, F_SETLK, &lock);
    *pErrcode = errno;
  }else{
    lock.l_len = 0;
    rc = fcntl(pFile->h, F_SETLK, &lock);
    *pErrcode = errno;
    if( NEVER(op==F_UNLCK) || rc!=(-1) ){
      lock.l_start = 0;
      lock.l_len = PENDING_BYTE;
      rc = fcntl(pFile->h, F_SETLK, &lock);
      if( ALWAYS(op!=F_UNLCK) && rc==(-1) ){
        *pErrcode = errno;
        lock.l_type = F_UNLCK;
        lock.l_start = SHARED_FIRST;
        lock.l_len = 0;
        fcntl(pFile->h, F_SETLK, &lock);
      }
    }
  }
  return rc;
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
** This routine will only increase a lock.  Use the sqlite3OsUnlock()
** routine to lower a locking level.
*/
static int unixLock(sqlite3_file *id, int locktype){
  /* The following describes the implementation of the various locks and
  ** lock transitions in terms of the POSIX advisory shared and exclusive
  ** lock primitives (called read-locks and write-locks below, to avoid
  ** confusion with SQLite lock names). The algorithms are complicated
  ** slightly in order to be compatible with windows systems simultaneously
  ** accessing the same database file, in case that is ever required.
  **
  ** Symbols defined in os.h indentify the 'pending byte' and the 'reserved
  ** byte', each single bytes at well known offsets, and the 'shared byte
  ** range', a range of 510 bytes at a well known offset.
  **
  ** To obtain a SHARED lock, a read-lock is obtained on the 'pending
  ** byte'.  If this is successful, a random byte from the 'shared byte
  ** range' is read-locked and the lock on the 'pending byte' released.
  **
  ** A process may only obtain a RESERVED lock after it has a SHARED lock.
  ** A RESERVED lock is implemented by grabbing a write-lock on the
  ** 'reserved byte'. 
  **
  ** A process may only obtain a PENDING lock after it has obtained a
  ** SHARED lock. A PENDING lock is implemented by obtaining a write-lock
  ** on the 'pending byte'. This ensures that no new SHARED locks can be
  ** obtained, but existing SHARED locks are allowed to persist. A process
  ** does not have to obtain a RESERVED lock on the way to a PENDING lock.
  ** This property is used by the algorithm for rolling back a journal file
  ** after a crash.
  **
  ** An EXCLUSIVE lock, obtained after a PENDING lock is held, is
  ** implemented by obtaining a write-lock on the entire 'shared byte
  ** range'. Since all other locks require a read-lock on one of the bytes
  ** within this range, this ensures that no other locks are held on the
  ** database. 
  **
  ** The reason a single byte cannot be used instead of the 'shared byte
  ** range' is that some versions of windows do not support read-locks. By
  ** locking a random byte from a range, concurrent SHARED locks may exist
  ** even if the locking primitive used is always a write-lock.
  */
  int rc = SQLITE_OK;
  unixFile *pFile = (unixFile*)id;
  struct unixLockInfo *pLock = pFile->pLock;
  struct flock lock;
  int s = 0;
  int tErrno;

  assert( pFile );
  OSTRACE7("LOCK    %d %s was %s(%s,%d) pid=%d\n", pFile->h,
      locktypeName(locktype), locktypeName(pFile->locktype),
      locktypeName(pLock->locktype), pLock->cnt , getpid());

  /* If there is already a lock of this type or more restrictive on the
  ** unixFile, do nothing. Don't use the end_lock: exit path, as
  ** unixEnterMutex() hasn't been called yet.
  */
  if( pFile->locktype>=locktype ){
    OSTRACE3("LOCK    %d %s ok (already held)\n", pFile->h,
            locktypeName(locktype));
    return SQLITE_OK;
  }

  /* Make sure the locking sequence is correct.
  **  (1) We never move from unlocked to anything higher than shared lock.
  **  (2) SQLite never explicitly requests a pendig lock.
  **  (3) A shared lock is always held when a reserve lock is requested.
  */
  assert( pFile->locktype!=NO_LOCK || locktype==SHARED_LOCK );
  assert( locktype!=PENDING_LOCK );
  assert( locktype!=RESERVED_LOCK || pFile->locktype==SHARED_LOCK );

  /* This mutex is needed because pFile->pLock is shared across threads
  */
  unixEnterMutex();

  /* Make sure the current thread owns the pFile.
  */
  rc = transferOwnership(pFile);
  if( rc!=SQLITE_OK ){
    unixLeaveMutex();
    return rc;
  }
  pLock = pFile->pLock;

  /* If some thread using this PID has a lock via a different unixFile*
  ** handle that precludes the requested lock, return BUSY.
  */
  if( (pFile->locktype!=pLock->locktype && 
          (pLock->locktype>=PENDING_LOCK || locktype>SHARED_LOCK))
  ){
    rc = SQLITE_BUSY;
    goto end_lock;
  }

  /* If a SHARED lock is requested, and some thread using this PID already
  ** has a SHARED or RESERVED lock, then increment reference counts and
  ** return SQLITE_OK.
  */
  if( locktype==SHARED_LOCK && 
      (pLock->locktype==SHARED_LOCK || pLock->locktype==RESERVED_LOCK) ){
    assert( locktype==SHARED_LOCK );
    assert( pFile->locktype==0 );
    assert( pLock->cnt>0 );
    pFile->locktype = SHARED_LOCK;
    pLock->cnt++;
    pFile->pOpen->nLock++;
    goto end_lock;
  }


  /* A PENDING lock is needed before acquiring a SHARED lock and before
  ** acquiring an EXCLUSIVE lock.  For the SHARED lock, the PENDING will
  ** be released.
  */
  lock.l_len = 1L;
  lock.l_whence = SEEK_SET;
  if( locktype==SHARED_LOCK 
      || (locktype==EXCLUSIVE_LOCK && pFile->locktype<PENDING_LOCK)
  ){
    lock.l_type = (locktype==SHARED_LOCK?F_RDLCK:F_WRLCK);
    lock.l_start = PENDING_BYTE;
    s = fcntl(pFile->h, F_SETLK, &lock);
    if( s==(-1) ){
      tErrno = errno;
      rc = sqliteErrorFromPosixError(tErrno, SQLITE_IOERR_LOCK);
      if( IS_LOCK_ERROR(rc) ){
        pFile->lastErrno = tErrno;
      }
      goto end_lock;
    }
  }


  /* If control gets to this point, then actually go ahead and make
  ** operating system calls for the specified lock.
  */
  if( locktype==SHARED_LOCK ){
    assert( pLock->cnt==0 );
    assert( pLock->locktype==0 );

    /* Now get the read-lock */
    s = rangeLock(pFile, F_RDLCK, &tErrno);

    /* Drop the temporary PENDING lock */
    lock.l_start = PENDING_BYTE;
    lock.l_len = 1L;
    lock.l_type = F_UNLCK;
    if( fcntl(pFile->h, F_SETLK, &lock)!=0 ){
      if( s != -1 ){
        /* This could happen with a network mount */
        tErrno = errno; 
        rc = sqliteErrorFromPosixError(tErrno, SQLITE_IOERR_UNLOCK); 
        if( IS_LOCK_ERROR(rc) ){
          pFile->lastErrno = tErrno;
        }
        goto end_lock;
      }
    }
    if( s==(-1) ){
      rc = sqliteErrorFromPosixError(tErrno, SQLITE_IOERR_LOCK);
      if( IS_LOCK_ERROR(rc) ){
        pFile->lastErrno = tErrno;
      }
    }else{
      pFile->locktype = SHARED_LOCK;
      pFile->pOpen->nLock++;
      pLock->cnt = 1;
    }
  }else if( locktype==EXCLUSIVE_LOCK && pLock->cnt>1 ){
    /* We are trying for an exclusive lock but another thread in this
    ** same process is still holding a shared lock. */
    rc = SQLITE_BUSY;
  }else{
    /* The request was for a RESERVED or EXCLUSIVE lock.  It is
    ** assumed that there is a SHARED or greater lock on the file
    ** already.
    */
    assert( 0!=pFile->locktype );
    lock.l_type = F_WRLCK;
    switch( locktype ){
      case RESERVED_LOCK:
        lock.l_start = RESERVED_BYTE;
        s = fcntl(pFile->h, F_SETLK, &lock);
        tErrno = errno;
        break;
      case EXCLUSIVE_LOCK:
        s = rangeLock(pFile, F_WRLCK, &tErrno);
        break;
      default:
        assert(0);
    }
    if( s==(-1) ){
      rc = sqliteErrorFromPosixError(tErrno, SQLITE_IOERR_LOCK);
      if( IS_LOCK_ERROR(rc) ){
        pFile->lastErrno = tErrno;
      }
    }
  }
  

#ifndef NDEBUG
  /* Set up the transaction-counter change checking flags when
  ** transitioning from a SHARED to a RESERVED lock.  The change
  ** from SHARED to RESERVED marks the beginning of a normal
  ** write operation (not a hot journal rollback).
  */
  if( rc==SQLITE_OK
   && pFile->locktype<=SHARED_LOCK
   && locktype==RESERVED_LOCK
  ){
    pFile->transCntrChng = 0;
    pFile->dbUpdate = 0;
    pFile->inNormalWrite = 1;
  }
#endif


  if( rc==SQLITE_OK ){
    pFile->locktype = locktype;
    pLock->locktype = locktype;
  }else if( locktype==EXCLUSIVE_LOCK ){
    pFile->locktype = PENDING_LOCK;
    pLock->locktype = PENDING_LOCK;
  }

end_lock:
  unixLeaveMutex();
  OSTRACE4("LOCK    %d %s %s\n", pFile->h, locktypeName(locktype), 
      rc==SQLITE_OK ? "ok" : "failed");
  return rc;
}

/*
** Close all file descriptors accumuated in the unixOpenCnt->pUnused list.
** If all such file descriptors are closed without error, the list is
** cleared and SQLITE_OK returned.
**
** Otherwise, if an error occurs, then successfully closed file descriptor
** entries are removed from the list, and SQLITE_IOERR_CLOSE returned. 
** not deleted and SQLITE_IOERR_CLOSE returned.
*/ 
static int closePendingFds(unixFile *pFile){
  int rc = SQLITE_OK;
  struct unixOpenCnt *pOpen = pFile->pOpen;
  UnixUnusedFd *pError = 0;
  UnixUnusedFd *p;
  UnixUnusedFd *pNext;
  for(p=pOpen->pUnused; p; p=pNext){
    pNext = p->pNext;
    if( close(p->fd) ){
      pFile->lastErrno = errno;
      rc = SQLITE_IOERR_CLOSE;
      p->pNext = pError;
      pError = p;
    }else{
      sqlite3_free(p);
    }
  }
  pOpen->pUnused = pError;
  return rc;
}

/*
** Add the file descriptor used by file handle pFile to the corresponding
** pUnused list.
*/
static void setPendingFd(unixFile *pFile){
  struct unixOpenCnt *pOpen = pFile->pOpen;
  UnixUnusedFd *p = pFile->pUnused;
  p->pNext = pOpen->pUnused;
  pOpen->pUnused = p;
  pFile->h = -1;
  pFile->pUnused = 0;
}

/*
** Lower the locking level on file descriptor pFile to locktype.  locktype
** must be either NO_LOCK or SHARED_LOCK.
**
** If the locking level of the file descriptor is already at or below
** the requested locking level, this routine is a no-op.
*/
static int unixUnlock(sqlite3_file *id, int locktype){
  unixFile *pFile = (unixFile*)id; /* The open file */
  struct unixLockInfo *pLock;      /* Structure describing current lock state */
  struct flock lock;               /* Information passed into fcntl() */
  int rc = SQLITE_OK;              /* Return code from this interface */
  int h;                           /* The underlying file descriptor */
  int tErrno;                      /* Error code from system call errors */

  assert( pFile );
  OSTRACE7("UNLOCK  %d %d was %d(%d,%d) pid=%d\n", pFile->h, locktype,
      pFile->locktype, pFile->pLock->locktype, pFile->pLock->cnt, getpid());

  assert( locktype<=SHARED_LOCK );
  if( pFile->locktype<=locktype ){
    return SQLITE_OK;
  }
  if( CHECK_THREADID(pFile) ){
    return SQLITE_MISUSE;
  }
  unixEnterMutex();
  h = pFile->h;
  pLock = pFile->pLock;
  assert( pLock->cnt!=0 );
  if( pFile->locktype>SHARED_LOCK ){
    assert( pLock->locktype==pFile->locktype );
    SimulateIOErrorBenign(1);
    SimulateIOError( h=(-1) )
    SimulateIOErrorBenign(0);

#ifndef NDEBUG
    /* When reducing a lock such that other processes can start
    ** reading the database file again, make sure that the
    ** transaction counter was updated if any part of the database
    ** file changed.  If the transaction counter is not updated,
    ** other connections to the same file might not realize that
    ** the file has changed and hence might not know to flush their
    ** cache.  The use of a stale cache can lead to database corruption.
    */
    assert( pFile->inNormalWrite==0
         || pFile->dbUpdate==0
         || pFile->transCntrChng==1 );
    pFile->inNormalWrite = 0;
#endif


    if( locktype==SHARED_LOCK ){
      if( rangeLock(pFile, F_RDLCK, &tErrno)==(-1) ){
        rc = sqliteErrorFromPosixError(tErrno, SQLITE_IOERR_RDLOCK);
        if( IS_LOCK_ERROR(rc) ){
          pFile->lastErrno = tErrno;
        }
        goto end_unlock;
      }
    }
    lock.l_type = F_UNLCK;
    lock.l_whence = SEEK_SET;
    lock.l_start = PENDING_BYTE;
    lock.l_len = 2L;  assert( PENDING_BYTE+1==RESERVED_BYTE );
    if( fcntl(h, F_SETLK, &lock)!=(-1) ){
      pLock->locktype = SHARED_LOCK;
    }else{
      tErrno = errno;
      rc = sqliteErrorFromPosixError(tErrno, SQLITE_IOERR_UNLOCK);
      if( IS_LOCK_ERROR(rc) ){
        pFile->lastErrno = tErrno;
      }
      goto end_unlock;
    }
  }
  if( locktype==NO_LOCK ){
    struct unixOpenCnt *pOpen;

    /* Decrement the shared lock counter.  Release the lock using an
    ** OS call only when all threads in this same process have released
    ** the lock.
    */
    pLock->cnt--;
    if( pLock->cnt==0 ){
      lock.l_type = F_UNLCK;
      lock.l_whence = SEEK_SET;
      lock.l_start = lock.l_len = 0L;
      SimulateIOErrorBenign(1);
      SimulateIOError( h=(-1) )
      SimulateIOErrorBenign(0);
      if( fcntl(h, F_SETLK, &lock)!=(-1) ){
        pLock->locktype = NO_LOCK;
      }else{
        tErrno = errno;
        rc = sqliteErrorFromPosixError(tErrno, SQLITE_IOERR_UNLOCK);
        if( IS_LOCK_ERROR(rc) ){
          pFile->lastErrno = tErrno;
        }
        pLock->locktype = NO_LOCK;
        pFile->locktype = NO_LOCK;
      }
    }

    /* Decrement the count of locks against this same file.  When the
    ** count reaches zero, close any other file descriptors whose close
    ** was deferred because of outstanding locks.
    */
    pOpen = pFile->pOpen;
    pOpen->nLock--;
    assert( pOpen->nLock>=0 );
    if( pOpen->nLock==0 ){
      int rc2 = closePendingFds(pFile);
      if( rc==SQLITE_OK ){
        rc = rc2;
      }
    }
  }
	
end_unlock:
  unixLeaveMutex();
  if( rc==SQLITE_OK ) pFile->locktype = locktype;
  return rc;
}

/*
** This function performs the parts of the "close file" operation 
** common to all locking schemes. It closes the directory and file
** handles, if they are valid, and sets all fields of the unixFile
** structure to 0.
**
** It is *not* necessary to hold the mutex when this routine is called,
** even on VxWorks.  A mutex will be acquired on VxWorks by the
** vxworksReleaseFileId() routine.
*/
static int closeUnixFile(sqlite3_file *id){
  unixFile *pFile = (unixFile*)id;
  if( pFile ){
    if( pFile->dirfd>=0 ){
      int err = close(pFile->dirfd);
      if( err ){
        pFile->lastErrno = errno;
        return SQLITE_IOERR_DIR_CLOSE;
      }else{
        pFile->dirfd=-1;
      }
    }
    if( pFile->h>=0 ){
      int err = close(pFile->h);
      if( err ){
        pFile->lastErrno = errno;
        return SQLITE_IOERR_CLOSE;
      }
    }
#if OS_VXWORKS
    if( pFile->pId ){
      if( pFile->isDelete ){
        unlink(pFile->pId->zCanonicalName);
      }
      vxworksReleaseFileId(pFile->pId);
      pFile->pId = 0;
    }
#endif
    OSTRACE2("CLOSE   %-3d\n", pFile->h);
    OpenCounter(-1);
    sqlite3_free(pFile->pUnused);
    memset(pFile, 0, sizeof(unixFile));
  }
  return SQLITE_OK;
}

/*
** Close a file.
*/
static int unixClose(sqlite3_file *id){
  int rc = SQLITE_OK;
  if( id ){
    unixFile *pFile = (unixFile *)id;
    unixUnlock(id, NO_LOCK);
    unixEnterMutex();
    if( pFile->pOpen && pFile->pOpen->nLock ){
      /* If there are outstanding locks, do not actually close the file just
      ** yet because that would clear those locks.  Instead, add the file
      ** descriptor to pOpen->pUnused list.  It will be automatically closed 
      ** when the last lock is cleared.
      */
      setPendingFd(pFile);
    }
    releaseLockInfo(pFile->pLock);
    releaseOpenCnt(pFile->pOpen);
    rc = closeUnixFile(id);
    unixLeaveMutex();
  }
  return rc;
}

/************** End of the posix advisory lock implementation *****************
******************************************************************************/

/******************************************************************************
****************************** No-op Locking **********************************
**
** Of the various locking implementations available, this is by far the
** simplest:  locking is ignored.  No attempt is made to lock the database
** file for reading or writing.
**
** This locking mode is appropriate for use on read-only databases
** (ex: databases that are burned into CD-ROM, for example.)  It can
** also be used if the application employs some external mechanism to
** prevent simultaneous access of the same database by two or more
** database connections.  But there is a serious risk of database
** corruption if this locking mode is used in situations where multiple
** database connections are accessing the same database file at the same
** time and one or more of those connections are writing.
*/

static int nolockCheckReservedLock(sqlite3_file *NotUsed, int *pResOut){
  UNUSED_PARAMETER(NotUsed);
  *pResOut = 0;
  return SQLITE_OK;
}
static int nolockLock(sqlite3_file *NotUsed, int NotUsed2){
  UNUSED_PARAMETER2(NotUsed, NotUsed2);
  return SQLITE_OK;
}
static int nolockUnlock(sqlite3_file *NotUsed, int NotUsed2){
  UNUSED_PARAMETER2(NotUsed, NotUsed2);
  return SQLITE_OK;
}

/*
** Close the file.
*/
static int nolockClose(sqlite3_file *id) {
  return closeUnixFile(id);
}

/******************* End of the no-op lock implementation *********************
******************************************************************************/

/******************************************************************************
************************* Begin dot-file Locking ******************************
**
** The dotfile locking implementation uses the existance of separate lock
** files in order to control access to the database.  This works on just
** about every filesystem imaginable.  But there are serious downsides:
**
**    (1)  There is zero concurrency.  A single reader blocks all other
**         connections from reading or writing the database.
**
**    (2)  An application crash or power loss can leave stale lock files
**         sitting around that need to be cleared manually.
**
** Nevertheless, a dotlock is an appropriate locking mode for use if no
** other locking strategy is available.
**
** Dotfile locking works by creating a file in the same directory as the
** database and with the same name but with a ".lock" extension added.
** The existance of a lock file implies an EXCLUSIVE lock.  All other lock
** types (SHARED, RESERVED, PENDING) are mapped into EXCLUSIVE.
*/

/*
** The file suffix added to the data base filename in order to create the
** lock file.
*/
#define DOTLOCK_SUFFIX ".lock"

/*
** This routine checks if there is a RESERVED lock held on the specified
** file by this or any other process. If such a lock is held, set *pResOut
** to a non-zero value otherwise *pResOut is set to zero.  The return value
** is set to SQLITE_OK unless an I/O error occurs during lock checking.
**
** In dotfile locking, either a lock exists or it does not.  So in this
** variation of CheckReservedLock(), *pResOut is set to true if any lock
** is held on the file and false if the file is unlocked.
*/
static int dotlockCheckReservedLock(sqlite3_file *id, int *pResOut) {
  int rc = SQLITE_OK;
  int reserved = 0;
  unixFile *pFile = (unixFile*)id;

  SimulateIOError( return SQLITE_IOERR_CHECKRESERVEDLOCK; );
  
  assert( pFile );

  /* Check if a thread in this process holds such a lock */
  if( pFile->locktype>SHARED_LOCK ){
    /* Either this connection or some other connection in the same process
    ** holds a lock on the file.  No need to check further. */
    reserved = 1;
  }else{
    /* The lock is held if and only if the lockfile exists */
    const char *zLockFile = (const char*)pFile->lockingContext;
    reserved = access(zLockFile, 0)==0;
  }
  OSTRACE4("TEST WR-LOCK %d %d %d\n", pFile->h, rc, reserved);
  *pResOut = reserved;
  return rc;
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
** This routine will only increase a lock.  Use the sqlite3OsUnlock()
** routine to lower a locking level.
**
** With dotfile locking, we really only support state (4): EXCLUSIVE.
** But we track the other locking levels internally.
*/
static int dotlockLock(sqlite3_file *id, int locktype) {
  unixFile *pFile = (unixFile*)id;
  int fd;
  char *zLockFile = (char *)pFile->lockingContext;
  int rc = SQLITE_OK;


  /* If we have any lock, then the lock file already exists.  All we have
  ** to do is adjust our internal record of the lock level.
  */
  if( pFile->locktype > NO_LOCK ){
    pFile->locktype = locktype;
#if !OS_VXWORKS
    /* Always update the timestamp on the old file */
    utimes(zLockFile, NULL);
#endif
    return SQLITE_OK;
  }
  
  /* grab an exclusive lock */
  fd = open(zLockFile,O_RDONLY|O_CREAT|O_EXCL,0600);
  if( fd<0 ){
    /* failed to open/create the file, someone else may have stolen the lock */
    int tErrno = errno;
    if( EEXIST == tErrno ){
      rc = SQLITE_BUSY;
    } else {
      rc = sqliteErrorFromPosixError(tErrno, SQLITE_IOERR_LOCK);
      if( IS_LOCK_ERROR(rc) ){
        pFile->lastErrno = tErrno;
      }
    }
    return rc;
  } 
  if( close(fd) ){
    pFile->lastErrno = errno;
    rc = SQLITE_IOERR_CLOSE;
  }
  
  /* got it, set the type and return ok */
  pFile->locktype = locktype;
  return rc;
}

/*
** Lower the locking level on file descriptor pFile to locktype.  locktype
** must be either NO_LOCK or SHARED_LOCK.
**
** If the locking level of the file descriptor is already at or below
** the requested locking level, this routine is a no-op.
**
** When the locking level reaches NO_LOCK, delete the lock file.
*/
static int dotlockUnlock(sqlite3_file *id, int locktype) {
  unixFile *pFile = (unixFile*)id;
  char *zLockFile = (char *)pFile->lockingContext;

  assert( pFile );
  OSTRACE5("UNLOCK  %d %d was %d pid=%d\n", pFile->h, locktype,
	   pFile->locktype, getpid());
  assert( locktype<=SHARED_LOCK );
  
  /* no-op if possible */
  if( pFile->locktype==locktype ){
    return SQLITE_OK;
  }

  /* To downgrade to shared, simply update our internal notion of the
  ** lock state.  No need to mess with the file on disk.
  */
  if( locktype==SHARED_LOCK ){
    pFile->locktype = SHARED_LOCK;
    return SQLITE_OK;
  }
  
  /* To fully unlock the database, delete the lock file */
  assert( locktype==NO_LOCK );
  if( unlink(zLockFile) ){
    int rc = 0;
    int tErrno = errno;
    if( ENOENT != tErrno ){
      rc = sqliteErrorFromPosixError(tErrno, SQLITE_IOERR_UNLOCK);
    }
    if( IS_LOCK_ERROR(rc) ){
      pFile->lastErrno = tErrno;
    }
    return rc; 
  }
  pFile->locktype = NO_LOCK;
  return SQLITE_OK;
}

/*
** Close a file.  Make sure the lock has been released before closing.
*/
static int dotlockClose(sqlite3_file *id) {
  int rc;
  if( id ){
    unixFile *pFile = (unixFile*)id;
    dotlockUnlock(id, NO_LOCK);
    sqlite3_free(pFile->lockingContext);
  }
  rc = closeUnixFile(id);
  return rc;
}
/****************** End of the dot-file lock implementation *******************
******************************************************************************/

/******************************************************************************
************************** Begin flock Locking ********************************
**
** Use the flock() system call to do file locking.
**
** flock() locking is like dot-file locking in that the various
** fine-grain locking levels supported by SQLite are collapsed into
** a single exclusive lock.  In other words, SHARED, RESERVED, and
** PENDING locks are the same thing as an EXCLUSIVE lock.  SQLite
** still works when you do this, but concurrency is reduced since
** only a single process can be reading the database at a time.
**
** Omit this section if SQLITE_ENABLE_LOCKING_STYLE is turned off or if
** compiling for VXWORKS.
*/
#if SQLITE_ENABLE_LOCKING_STYLE && !OS_VXWORKS

/*
** This routine checks if there is a RESERVED lock held on the specified
** file by this or any other process. If such a lock is held, set *pResOut
** to a non-zero value otherwise *pResOut is set to zero.  The return value
** is set to SQLITE_OK unless an I/O error occurs during lock checking.
*/
static int flockCheckReservedLock(sqlite3_file *id, int *pResOut){
  int rc = SQLITE_OK;
  int reserved = 0;
  unixFile *pFile = (unixFile*)id;
  
  SimulateIOError( return SQLITE_IOERR_CHECKRESERVEDLOCK; );
  
  assert( pFile );
  
  /* Check if a thread in this process holds such a lock */
  if( pFile->locktype>SHARED_LOCK ){
    reserved = 1;
  }
  
  /* Otherwise see if some other process holds it. */
  if( !reserved ){
    /* attempt to get the lock */
    int lrc = flock(pFile->h, LOCK_EX | LOCK_NB);
    if( !lrc ){
      /* got the lock, unlock it */
      lrc = flock(pFile->h, LOCK_UN);
      if ( lrc ) {
        int tErrno = errno;
        /* unlock failed with an error */
        lrc = sqliteErrorFromPosixError(tErrno, SQLITE_IOERR_UNLOCK); 
        if( IS_LOCK_ERROR(lrc) ){
          pFile->lastErrno = tErrno;
          rc = lrc;
        }
      }
    } else {
      int tErrno = errno;
      reserved = 1;
      /* someone else might have it reserved */
      lrc = sqliteErrorFromPosixError(tErrno, SQLITE_IOERR_LOCK); 
      if( IS_LOCK_ERROR(lrc) ){
        pFile->lastErrno = tErrno;
        rc = lrc;
      }
    }
  }
  OSTRACE4("TEST WR-LOCK %d %d %d\n", pFile->h, rc, reserved);

#ifdef SQLITE_IGNORE_FLOCK_LOCK_ERRORS
  if( (rc & SQLITE_IOERR) == SQLITE_IOERR ){
    rc = SQLITE_OK;
    reserved=1;
  }
#endif /* SQLITE_IGNORE_FLOCK_LOCK_ERRORS */
  *pResOut = reserved;
  return rc;
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
** flock() only really support EXCLUSIVE locks.  We track intermediate
** lock states in the sqlite3_file structure, but all locks SHARED or
** above are really EXCLUSIVE locks and exclude all other processes from
** access the file.
**
** This routine will only increase a lock.  Use the sqlite3OsUnlock()
** routine to lower a locking level.
*/
static int flockLock(sqlite3_file *id, int locktype) {
  int rc = SQLITE_OK;
  unixFile *pFile = (unixFile*)id;

  assert( pFile );

  /* if we already have a lock, it is exclusive.  
  ** Just adjust level and punt on outta here. */
  if (pFile->locktype > NO_LOCK) {
    pFile->locktype = locktype;
    return SQLITE_OK;
  }
  
  /* grab an exclusive lock */
  
  if (flock(pFile->h, LOCK_EX | LOCK_NB)) {
    int tErrno = errno;
    /* didn't get, must be busy */
    rc = sqliteErrorFromPosixError(tErrno, SQLITE_IOERR_LOCK);
    if( IS_LOCK_ERROR(rc) ){
      pFile->lastErrno = tErrno;
    }
  } else {
    /* got it, set the type and return ok */
    pFile->locktype = locktype;
  }
  OSTRACE4("LOCK    %d %s %s\n", pFile->h, locktypeName(locktype), 
           rc==SQLITE_OK ? "ok" : "failed");
#ifdef SQLITE_IGNORE_FLOCK_LOCK_ERRORS
  if( (rc & SQLITE_IOERR) == SQLITE_IOERR ){
    rc = SQLITE_BUSY;
  }
#endif /* SQLITE_IGNORE_FLOCK_LOCK_ERRORS */
  return rc;
}


/*
** Lower the locking level on file descriptor pFile to locktype.  locktype
** must be either NO_LOCK or SHARED_LOCK.
**
** If the locking level of the file descriptor is already at or below
** the requested locking level, this routine is a no-op.
*/
static int flockUnlock(sqlite3_file *id, int locktype) {
  unixFile *pFile = (unixFile*)id;
  
  assert( pFile );
  OSTRACE5("UNLOCK  %d %d was %d pid=%d\n", pFile->h, locktype,
           pFile->locktype, getpid());
  assert( locktype<=SHARED_LOCK );
  
  /* no-op if possible */
  if( pFile->locktype==locktype ){
    return SQLITE_OK;
  }
  
  /* shared can just be set because we always have an exclusive */
  if (locktype==SHARED_LOCK) {
    pFile->locktype = locktype;
    return SQLITE_OK;
  }
  
  /* no, really, unlock. */
  int rc = flock(pFile->h, LOCK_UN);
  if (rc) {
    int r, tErrno = errno;
    r = sqliteErrorFromPosixError(tErrno, SQLITE_IOERR_UNLOCK);
    if( IS_LOCK_ERROR(r) ){
      pFile->lastErrno = tErrno;
    }
#ifdef SQLITE_IGNORE_FLOCK_LOCK_ERRORS
    if( (r & SQLITE_IOERR) == SQLITE_IOERR ){
      r = SQLITE_BUSY;
    }
#endif /* SQLITE_IGNORE_FLOCK_LOCK_ERRORS */
    
    return r;
  } else {
    pFile->locktype = NO_LOCK;
    return SQLITE_OK;
  }
}

/*
** Close a file.
*/
static int flockClose(sqlite3_file *id) {
  if( id ){
    flockUnlock(id, NO_LOCK);
  }
  return closeUnixFile(id);
}

#endif /* SQLITE_ENABLE_LOCKING_STYLE && !OS_VXWORK */

/******************* End of the flock lock implementation *********************
******************************************************************************/

/******************************************************************************
************************ Begin Named Semaphore Locking ************************
**
** Named semaphore locking is only supported on VxWorks.
**
** Semaphore locking is like dot-lock and flock in that it really only
** supports EXCLUSIVE locking.  Only a single process can read or write
** the database file at a time.  This reduces potential concurrency, but
** makes the lock implementation much easier.
*/
#if OS_VXWORKS

/*
** This routine checks if there is a RESERVED lock held on the specified
** file by this or any other process. If such a lock is held, set *pResOut
** to a non-zero value otherwise *pResOut is set to zero.  The return value
** is set to SQLITE_OK unless an I/O error occurs during lock checking.
*/
static int semCheckReservedLock(sqlite3_file *id, int *pResOut) {
  int rc = SQLITE_OK;
  int reserved = 0;
  unixFile *pFile = (unixFile*)id;

  SimulateIOError( return SQLITE_IOERR_CHECKRESERVEDLOCK; );
  
  assert( pFile );

  /* Check if a thread in this process holds such a lock */
  if( pFile->locktype>SHARED_LOCK ){
    reserved = 1;
  }
  
  /* Otherwise see if some other process holds it. */
  if( !reserved ){
    sem_t *pSem = pFile->pOpen->pSem;
    struct stat statBuf;

    if( sem_trywait(pSem)==-1 ){
      int tErrno = errno;
      if( EAGAIN != tErrno ){
        rc = sqliteErrorFromPosixError(tErrno, SQLITE_IOERR_CHECKRESERVEDLOCK);
        pFile->lastErrno = tErrno;
      } else {
        /* someone else has the lock when we are in NO_LOCK */
        reserved = (pFile->locktype < SHARED_LOCK);
      }
    }else{
      /* we could have it if we want it */
      sem_post(pSem);
    }
  }
  OSTRACE4("TEST WR-LOCK %d %d %d\n", pFile->h, rc, reserved);

  *pResOut = reserved;
  return rc;
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
** Semaphore locks only really support EXCLUSIVE locks.  We track intermediate
** lock states in the sqlite3_file structure, but all locks SHARED or
** above are really EXCLUSIVE locks and exclude all other processes from
** access the file.
**
** This routine will only increase a lock.  Use the sqlite3OsUnlock()
** routine to lower a locking level.
*/
static int semLock(sqlite3_file *id, int locktype) {
  unixFile *pFile = (unixFile*)id;
  int fd;
  sem_t *pSem = pFile->pOpen->pSem;
  int rc = SQLITE_OK;

  /* if we already have a lock, it is exclusive.  
  ** Just adjust level and punt on outta here. */
  if (pFile->locktype > NO_LOCK) {
    pFile->locktype = locktype;
    rc = SQLITE_OK;
    goto sem_end_lock;
  }
  
  /* lock semaphore now but bail out when already locked. */
  if( sem_trywait(pSem)==-1 ){
    rc = SQLITE_BUSY;
    goto sem_end_lock;
  }

  /* got it, set the type and return ok */
  pFile->locktype = locktype;

 sem_end_lock:
  return rc;
}

/*
** Lower the locking level on file descriptor pFile to locktype.  locktype
** must be either NO_LOCK or SHARED_LOCK.
**
** If the locking level of the file descriptor is already at or below
** the requested locking level, this routine is a no-op.
*/
static int semUnlock(sqlite3_file *id, int locktype) {
  unixFile *pFile = (unixFile*)id;
  sem_t *pSem = pFile->pOpen->pSem;

  assert( pFile );
  assert( pSem );
  OSTRACE5("UNLOCK  %d %d was %d pid=%d\n", pFile->h, locktype,
	   pFile->locktype, getpid());
  assert( locktype<=SHARED_LOCK );
  
  /* no-op if possible */
  if( pFile->locktype==locktype ){
    return SQLITE_OK;
  }
  
  /* shared can just be set because we always have an exclusive */
  if (locktype==SHARED_LOCK) {
    pFile->locktype = locktype;
    return SQLITE_OK;
  }
  
  /* no, really unlock. */
  if ( sem_post(pSem)==-1 ) {
    int rc, tErrno = errno;
    rc = sqliteErrorFromPosixError(tErrno, SQLITE_IOERR_UNLOCK);
    if( IS_LOCK_ERROR(rc) ){
      pFile->lastErrno = tErrno;
    }
    return rc; 
  }
  pFile->locktype = NO_LOCK;
  return SQLITE_OK;
}

/*
 ** Close a file.
 */
static int semClose(sqlite3_file *id) {
  if( id ){
    unixFile *pFile = (unixFile*)id;
    semUnlock(id, NO_LOCK);
    assert( pFile );
    unixEnterMutex();
    releaseLockInfo(pFile->pLock);
    releaseOpenCnt(pFile->pOpen);
    unixLeaveMutex();
    closeUnixFile(id);
  }
  return SQLITE_OK;
}

#endif /* OS_VXWORKS */
/*
** Named semaphore locking is only available on VxWorks.
**
*************** End of the named semaphore lock implementation ****************
******************************************************************************/


/******************************************************************************
*************************** Begin AFP Locking *********************************
**
** AFP is the Apple Filing Protocol.  AFP is a network filesystem found
** on Apple Macintosh computers - both OS9 and OSX.
**
** Third-party implementations of AFP are available.  But this code here
** only works on OSX.
*/

#if defined(__APPLE__) && SQLITE_ENABLE_LOCKING_STYLE
/*
** The afpLockingContext structure contains all afp lock specific state
*/
typedef struct afpLockingContext afpLockingContext;
struct afpLockingContext {
  unsigned long long sharedByte;
  const char *dbPath;             /* Name of the open file */
};

struct ByteRangeLockPB2
{
  unsigned long long offset;        /* offset to first byte to lock */
  unsigned long long length;        /* nbr of bytes to lock */
  unsigned long long retRangeStart; /* nbr of 1st byte locked if successful */
  unsigned char unLockFlag;         /* 1 = unlock, 0 = lock */
  unsigned char startEndFlag;       /* 1=rel to end of fork, 0=rel to start */
  int fd;                           /* file desc to assoc this lock with */
};

#define afpfsByteRangeLock2FSCTL        _IOWR('z', 23, struct ByteRangeLockPB2)

/*
** This is a utility for setting or clearing a bit-range lock on an
** AFP filesystem.
** 
** Return SQLITE_OK on success, SQLITE_BUSY on failure.
*/
static int afpSetLock(
  const char *path,              /* Name of the file to be locked or unlocked */
  unixFile *pFile,               /* Open file descriptor on path */
  unsigned long long offset,     /* First byte to be locked */
  unsigned long long length,     /* Number of bytes to lock */
  int setLockFlag                /* True to set lock.  False to clear lock */
){
  struct ByteRangeLockPB2 pb;
  int err;
  
  pb.unLockFlag = setLockFlag ? 0 : 1;
  pb.startEndFlag = 0;
  pb.offset = offset;
  pb.length = length; 
  pb.fd = pFile->h;
  
  OSTRACE6("AFPSETLOCK [%s] for %d%s in range %llx:%llx\n", 
    (setLockFlag?"ON":"OFF"), pFile->h, (pb.fd==-1?"[testval-1]":""),
    offset, length);
  err = fsctl(path, afpfsByteRangeLock2FSCTL, &pb, 0);
  if ( err==-1 ) {
    int rc;
    int tErrno = errno;
    OSTRACE4("AFPSETLOCK failed to fsctl() '%s' %d %s\n",
             path, tErrno, strerror(tErrno));
#ifdef SQLITE_IGNORE_AFP_LOCK_ERRORS
    rc = SQLITE_BUSY;
#else
    rc = sqliteErrorFromPosixError(tErrno,
                    setLockFlag ? SQLITE_IOERR_LOCK : SQLITE_IOERR_UNLOCK);
#endif /* SQLITE_IGNORE_AFP_LOCK_ERRORS */
    if( IS_LOCK_ERROR(rc) ){
      pFile->lastErrno = tErrno;
    }
    return rc;
  } else {
    return SQLITE_OK;
  }
}

/*
** This routine checks if there is a RESERVED lock held on the specified
** file by this or any other process. If such a lock is held, set *pResOut
** to a non-zero value otherwise *pResOut is set to zero.  The return value
** is set to SQLITE_OK unless an I/O error occurs during lock checking.
*/
static int afpCheckReservedLock(sqlite3_file *id, int *pResOut){
  int rc = SQLITE_OK;
  int reserved = 0;
  unixFile *pFile = (unixFile*)id;
  
  SimulateIOError( return SQLITE_IOERR_CHECKRESERVEDLOCK; );
  
  assert( pFile );
  afpLockingContext *context = (afpLockingContext *) pFile->lockingContext;
  
  /* Check if a thread in this process holds such a lock */
  if( pFile->locktype>SHARED_LOCK ){
    reserved = 1;
  }
  
  /* Otherwise see if some other process holds it.
   */
  if( !reserved ){
    /* lock the RESERVED byte */
    int lrc = afpSetLock(context->dbPath, pFile, RESERVED_BYTE, 1,1);  
    if( SQLITE_OK==lrc ){
      /* if we succeeded in taking the reserved lock, unlock it to restore
      ** the original state */
      lrc = afpSetLock(context->dbPath, pFile, RESERVED_BYTE, 1, 0);
    } else {
      /* if we failed to get the lock then someone else must have it */
      reserved = 1;
    }
    if( IS_LOCK_ERROR(lrc) ){
      rc=lrc;
    }
  }
  
  OSTRACE4("TEST WR-LOCK %d %d %d\n", pFile->h, rc, reserved);
  
  *pResOut = reserved;
  return rc;
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
** This routine will only increase a lock.  Use the sqlite3OsUnlock()
** routine to lower a locking level.
*/
static int afpLock(sqlite3_file *id, int locktype){
  int rc = SQLITE_OK;
  unixFile *pFile = (unixFile*)id;
  afpLockingContext *context = (afpLockingContext *) pFile->lockingContext;
  
  assert( pFile );
  OSTRACE5("LOCK    %d %s was %s pid=%d\n", pFile->h,
         locktypeName(locktype), locktypeName(pFile->locktype), getpid());

  /* If there is already a lock of this type or more restrictive on the
  ** unixFile, do nothing. Don't use the afp_end_lock: exit path, as
  ** unixEnterMutex() hasn't been called yet.
  */
  if( pFile->locktype>=locktype ){
    OSTRACE3("LOCK    %d %s ok (already held)\n", pFile->h,
           locktypeName(locktype));
    return SQLITE_OK;
  }

  /* Make sure the locking sequence is correct
  */
  assert( pFile->locktype!=NO_LOCK || locktype==SHARED_LOCK );
  assert( locktype!=PENDING_LOCK );
  assert( locktype!=RESERVED_LOCK || pFile->locktype==SHARED_LOCK );
  
  /* This mutex is needed because pFile->pLock is shared across threads
  */
  unixEnterMutex();

  /* Make sure the current thread owns the pFile.
  */
  rc = transferOwnership(pFile);
  if( rc!=SQLITE_OK ){
    unixLeaveMutex();
    return rc;
  }
    
  /* A PENDING lock is needed before acquiring a SHARED lock and before
  ** acquiring an EXCLUSIVE lock.  For the SHARED lock, the PENDING will
  ** be released.
  */
  if( locktype==SHARED_LOCK 
      || (locktype==EXCLUSIVE_LOCK && pFile->locktype<PENDING_LOCK)
  ){
    int failed;
    failed = afpSetLock(context->dbPath, pFile, PENDING_BYTE, 1, 1);
    if (failed) {
      rc = failed;
      goto afp_end_lock;
    }
  }
  
  /* If control gets to this point, then actually go ahead and make
  ** operating system calls for the specified lock.
  */
  if( locktype==SHARED_LOCK ){
    int lk, lrc1, lrc2, lrc1Errno;
    
    /* Now get the read-lock SHARED_LOCK */
    /* note that the quality of the randomness doesn't matter that much */
    lk = random(); 
    context->sharedByte = (lk & 0x7fffffff)%(SHARED_SIZE - 1);
    lrc1 = afpSetLock(context->dbPath, pFile, 
          SHARED_FIRST+context->sharedByte, 1, 1);
    if( IS_LOCK_ERROR(lrc1) ){
      lrc1Errno = pFile->lastErrno;
    }
    /* Drop the temporary PENDING lock */
    lrc2 = afpSetLock(context->dbPath, pFile, PENDING_BYTE, 1, 0);
    
    if( IS_LOCK_ERROR(lrc1) ) {
      pFile->lastErrno = lrc1Errno;
      rc = lrc1;
      goto afp_end_lock;
    } else if( IS_LOCK_ERROR(lrc2) ){
      rc = lrc2;
      goto afp_end_lock;
    } else if( lrc1 != SQLITE_OK ) {
      rc = lrc1;
    } else {
      pFile->locktype = SHARED_LOCK;
      pFile->pOpen->nLock++;
    }
  }else{
    /* The request was for a RESERVED or EXCLUSIVE lock.  It is
    ** assumed that there is a SHARED or greater lock on the file
    ** already.
    */
    int failed = 0;
    assert( 0!=pFile->locktype );
    if (locktype >= RESERVED_LOCK && pFile->locktype < RESERVED_LOCK) {
        /* Acquire a RESERVED lock */
        failed = afpSetLock(context->dbPath, pFile, RESERVED_BYTE, 1,1);
    }
    if (!failed && locktype == EXCLUSIVE_LOCK) {
      /* Acquire an EXCLUSIVE lock */
        
      /* Remove the shared lock before trying the range.  we'll need to 
      ** reestablish the shared lock if we can't get the  afpUnlock
      */
      if( !(failed = afpSetLock(context->dbPath, pFile, SHARED_FIRST +
                         context->sharedByte, 1, 0)) ){
        int failed2 = SQLITE_OK;
        /* now attemmpt to get the exclusive lock range */
        failed = afpSetLock(context->dbPath, pFile, SHARED_FIRST, 
                               SHARED_SIZE, 1);
        if( failed && (failed2 = afpSetLock(context->dbPath, pFile, 
                       SHARED_FIRST + context->sharedByte, 1, 1)) ){
          /* Can't reestablish the shared lock.  Sqlite can't deal, this is
          ** a critical I/O error
          */
          rc = ((failed & SQLITE_IOERR) == SQLITE_IOERR) ? failed2 : 
               SQLITE_IOERR_LOCK;
          goto afp_end_lock;
        } 
      }else{
        rc = failed; 
      }
    }
    if( failed ){
      rc = failed;
    }
  }
  
  if( rc==SQLITE_OK ){
    pFile->locktype = locktype;
  }else if( locktype==EXCLUSIVE_LOCK ){
    pFile->locktype = PENDING_LOCK;
  }
  
afp_end_lock:
  unixLeaveMutex();
  OSTRACE4("LOCK    %d %s %s\n", pFile->h, locktypeName(locktype), 
         rc==SQLITE_OK ? "ok" : "failed");
  return rc;
}

/*
** Lower the locking level on file descriptor pFile to locktype.  locktype
** must be either NO_LOCK or SHARED_LOCK.
**
** If the locking level of the file descriptor is already at or below
** the requested locking level, this routine is a no-op.
*/
static int afpUnlock(sqlite3_file *id, int locktype) {
  int rc = SQLITE_OK;
  unixFile *pFile = (unixFile*)id;
  afpLockingContext *pCtx = (afpLockingContext *) pFile->lockingContext;

  assert( pFile );
  OSTRACE5("UNLOCK  %d %d was %d pid=%d\n", pFile->h, locktype,
         pFile->locktype, getpid());

  assert( locktype<=SHARED_LOCK );
  if( pFile->locktype<=locktype ){
    return SQLITE_OK;
  }
  if( CHECK_THREADID(pFile) ){
    return SQLITE_MISUSE;
  }
  unixEnterMutex();
  if( pFile->locktype>SHARED_LOCK ){
    
    if( pFile->locktype==EXCLUSIVE_LOCK ){
      rc = afpSetLock(pCtx->dbPath, pFile, SHARED_FIRST, SHARED_SIZE, 0);
      if( rc==SQLITE_OK && locktype==SHARED_LOCK ){
        /* only re-establish the shared lock if necessary */
        int sharedLockByte = SHARED_FIRST+pCtx->sharedByte;
        rc = afpSetLock(pCtx->dbPath, pFile, sharedLockByte, 1, 1);
      }
    }
    if( rc==SQLITE_OK && pFile->locktype>=PENDING_LOCK ){
      rc = afpSetLock(pCtx->dbPath, pFile, PENDING_BYTE, 1, 0);
    } 
    if( rc==SQLITE_OK && pFile->locktype>=RESERVED_LOCK ){
      rc = afpSetLock(pCtx->dbPath, pFile, RESERVED_BYTE, 1, 0);
    }
  }else if( locktype==NO_LOCK ){
    /* clear the shared lock */
    int sharedLockByte = SHARED_FIRST+pCtx->sharedByte;
    rc = afpSetLock(pCtx->dbPath, pFile, sharedLockByte, 1, 0);
  }

  if( rc==SQLITE_OK ){
    if( locktype==NO_LOCK ){
      struct unixOpenCnt *pOpen = pFile->pOpen;
      pOpen->nLock--;
      assert( pOpen->nLock>=0 );
      if( pOpen->nLock==0 ){
        rc = closePendingFds(pFile);
      }
    }
  }
  unixLeaveMutex();
  if( rc==SQLITE_OK ){
    pFile->locktype = locktype;
  }
  return rc;
}

/*
** Close a file & cleanup AFP specific locking context 
*/
static int afpClose(sqlite3_file *id) {
  if( id ){
    unixFile *pFile = (unixFile*)id;
    afpUnlock(id, NO_LOCK);
    unixEnterMutex();
    if( pFile->pOpen && pFile->pOpen->nLock ){
      /* If there are outstanding locks, do not actually close the file just
      ** yet because that would clear those locks.  Instead, add the file
      ** descriptor to pOpen->aPending.  It will be automatically closed when
      ** the last lock is cleared.
      */
      setPendingFd(pFile);
    }
    releaseOpenCnt(pFile->pOpen);
    sqlite3_free(pFile->lockingContext);
    closeUnixFile(id);
    unixLeaveMutex();
  }
  return SQLITE_OK;
}

#endif /* defined(__APPLE__) && SQLITE_ENABLE_LOCKING_STYLE */
/*
** The code above is the AFP lock implementation.  The code is specific
** to MacOSX and does not work on other unix platforms.  No alternative
** is available.  If you don't compile for a mac, then the "unix-afp"
** VFS is not available.
**
********************* End of the AFP lock implementation **********************
******************************************************************************/


/******************************************************************************
**************** Non-locking sqlite3_file methods *****************************
**
** The next division contains implementations for all methods of the 
** sqlite3_file object other than the locking methods.  The locking
** methods were defined in divisions above (one locking method per
** division).  Those methods that are common to all locking modes
** are gather together into this division.
*/

/*
** Seek to the offset passed as the second argument, then read cnt 
** bytes into pBuf. Return the number of bytes actually read.
**
** NB:  If you define USE_PREAD or USE_PREAD64, then it might also
** be necessary to define _XOPEN_SOURCE to be 500.  This varies from
** one system to another.  Since SQLite does not define USE_PREAD
** any any form by default, we will not attempt to define _XOPEN_SOURCE.
** See tickets #2741 and #2681.
**
** To avoid stomping the errno value on a failed read the lastErrno value
** is set before returning.
*/
static int seekAndRead(unixFile *id, sqlite3_int64 offset, void *pBuf, int cnt){
  int got;
  i64 newOffset;
  TIMER_START;
#if defined(USE_PREAD)
  got = pread(id->h, pBuf, cnt, offset);
  SimulateIOError( got = -1 );
#elif defined(USE_PREAD64)
  got = pread64(id->h, pBuf, cnt, offset);
  SimulateIOError( got = -1 );
#else
  newOffset = lseek(id->h, offset, SEEK_SET);
  SimulateIOError( newOffset-- );
  if( newOffset!=offset ){
    if( newOffset == -1 ){
      ((unixFile*)id)->lastErrno = errno;
    }else{
      ((unixFile*)id)->lastErrno = 0;			
    }
    return -1;
  }
  got = read(id->h, pBuf, cnt);
#endif
  TIMER_END;
  if( got<0 ){
    ((unixFile*)id)->lastErrno = errno;
  }
  OSTRACE5("READ    %-3d %5d %7lld %llu\n", id->h, got, offset, TIMER_ELAPSED);
  return got;
}

/*
** Read data from a file into a buffer.  Return SQLITE_OK if all
** bytes were read successfully and SQLITE_IOERR if anything goes
** wrong.
*/
static int unixRead(
  sqlite3_file *id, 
  void *pBuf, 
  int amt,
  sqlite3_int64 offset
){
  unixFile *pFile = (unixFile *)id;
  int got;
  assert( id );

  /* If this is a database file (not a journal, master-journal or temp
  ** file), the bytes in the locking range should never be read or written. */
  assert( pFile->pUnused==0
       || offset>=PENDING_BYTE+512
       || offset+amt<=PENDING_BYTE 
  );

  got = seekAndRead(pFile, offset, pBuf, amt);
  if( got==amt ){
    return SQLITE_OK;
  }else if( got<0 ){
    /* lastErrno set by seekAndRead */
    return SQLITE_IOERR_READ;
  }else{
    pFile->lastErrno = 0; /* not a system error */
    /* Unread parts of the buffer must be zero-filled */
    memset(&((char*)pBuf)[got], 0, amt-got);
    return SQLITE_IOERR_SHORT_READ;
  }
}

/*
** Seek to the offset in id->offset then read cnt bytes into pBuf.
** Return the number of bytes actually read.  Update the offset.
**
** To avoid stomping the errno value on a failed write the lastErrno value
** is set before returning.
*/
static int seekAndWrite(unixFile *id, i64 offset, const void *pBuf, int cnt){
  int got;
  i64 newOffset;
  TIMER_START;
#if defined(USE_PREAD)
  got = pwrite(id->h, pBuf, cnt, offset);
#elif defined(USE_PREAD64)
  got = pwrite64(id->h, pBuf, cnt, offset);
#else
  newOffset = lseek(id->h, offset, SEEK_SET);
  if( newOffset!=offset ){
    if( newOffset == -1 ){
      ((unixFile*)id)->lastErrno = errno;
    }else{
      ((unixFile*)id)->lastErrno = 0;			
    }
    return -1;
  }
  got = write(id->h, pBuf, cnt);
#endif
  TIMER_END;
  if( got<0 ){
    ((unixFile*)id)->lastErrno = errno;
  }

  OSTRACE5("WRITE   %-3d %5d %7lld %llu\n", id->h, got, offset, TIMER_ELAPSED);
  return got;
}


/*
** Write data from a buffer into a file.  Return SQLITE_OK on success
** or some other error code on failure.
*/
static int unixWrite(
  sqlite3_file *id, 
  const void *pBuf, 
  int amt,
  sqlite3_int64 offset 
){
  unixFile *pFile = (unixFile*)id;
  int wrote = 0;
  assert( id );
  assert( amt>0 );

  /* If this is a database file (not a journal, master-journal or temp
  ** file), the bytes in the locking range should never be read or written. */
  assert( pFile->pUnused==0
       || offset>=PENDING_BYTE+512
       || offset+amt<=PENDING_BYTE 
  );

#ifndef NDEBUG
  /* If we are doing a normal write to a database file (as opposed to
  ** doing a hot-journal rollback or a write to some file other than a
  ** normal database file) then record the fact that the database
  ** has changed.  If the transaction counter is modified, record that
  ** fact too.
  */
  if( pFile->inNormalWrite ){
    pFile->dbUpdate = 1;  /* The database has been modified */
    if( offset<=24 && offset+amt>=27 ){
      int rc;
      char oldCntr[4];
      SimulateIOErrorBenign(1);
      rc = seekAndRead(pFile, 24, oldCntr, 4);
      SimulateIOErrorBenign(0);
      if( rc!=4 || memcmp(oldCntr, &((char*)pBuf)[24-offset], 4)!=0 ){
        pFile->transCntrChng = 1;  /* The transaction counter has changed */
      }
    }
  }
#endif

  while( amt>0 && (wrote = seekAndWrite(pFile, offset, pBuf, amt))>0 ){
    amt -= wrote;
    offset += wrote;
    pBuf = &((char*)pBuf)[wrote];
  }
  SimulateIOError(( wrote=(-1), amt=1 ));
  SimulateDiskfullError(( wrote=0, amt=1 ));
  if( amt>0 ){
    if( wrote<0 ){
      /* lastErrno set by seekAndWrite */
      return SQLITE_IOERR_WRITE;
    }else{
      pFile->lastErrno = 0; /* not a system error */
      return SQLITE_FULL;
    }
  }
  return SQLITE_OK;
}

#ifdef SQLITE_TEST
/*
** Count the number of fullsyncs and normal syncs.  This is used to test
** that syncs and fullsyncs are occurring at the right times.
*/
int sqlite3_sync_count = 0;
int sqlite3_fullsync_count = 0;
#endif

/*
** We do not trust systems to provide a working fdatasync().  Some do.
** Others do no.  To be safe, we will stick with the (slower) fsync().
** If you know that your system does support fdatasync() correctly,
** then simply compile with -Dfdatasync=fdatasync
*/
#if !defined(fdatasync) && !defined(__linux__)
# define fdatasync fsync
#endif

/*
** Define HAVE_FULLFSYNC to 0 or 1 depending on whether or not
** the F_FULLFSYNC macro is defined.  F_FULLFSYNC is currently
** only available on Mac OS X.  But that could change.
*/
#ifdef F_FULLFSYNC
# define HAVE_FULLFSYNC 1
#else
# define HAVE_FULLFSYNC 0
#endif


/*
** The fsync() system call does not work as advertised on many
** unix systems.  The following procedure is an attempt to make
** it work better.
**
** The SQLITE_NO_SYNC macro disables all fsync()s.  This is useful
** for testing when we want to run through the test suite quickly.
** You are strongly advised *not* to deploy with SQLITE_NO_SYNC
** enabled, however, since with SQLITE_NO_SYNC enabled, an OS crash
** or power failure will likely corrupt the database file.
**
** SQLite sets the dataOnly flag if the size of the file is unchanged.
** The idea behind dataOnly is that it should only write the file content
** to disk, not the inode.  We only set dataOnly if the file size is 
** unchanged since the file size is part of the inode.  However, 
** Ted Ts'o tells us that fdatasync() will also write the inode if the
** file size has changed.  The only real difference between fdatasync()
** and fsync(), Ted tells us, is that fdatasync() will not flush the
** inode if the mtime or owner or other inode attributes have changed.
** We only care about the file size, not the other file attributes, so
** as far as SQLite is concerned, an fdatasync() is always adequate.
** So, we always use fdatasync() if it is available, regardless of
** the value of the dataOnly flag.
*/
static int full_fsync(int fd, int fullSync, int dataOnly){
  int rc;

  /* The following "ifdef/elif/else/" block has the same structure as
  ** the one below. It is replicated here solely to avoid cluttering 
  ** up the real code with the UNUSED_PARAMETER() macros.
  */
#ifdef SQLITE_NO_SYNC
  UNUSED_PARAMETER(fd);
  UNUSED_PARAMETER(fullSync);
  UNUSED_PARAMETER(dataOnly);
#elif HAVE_FULLFSYNC
  UNUSED_PARAMETER(dataOnly);
#else
  UNUSED_PARAMETER(fullSync);
  UNUSED_PARAMETER(dataOnly);
#endif

  /* Record the number of times that we do a normal fsync() and 
  ** FULLSYNC.  This is used during testing to verify that this procedure
  ** gets called with the correct arguments.
  */
#ifdef SQLITE_TEST
  if( fullSync ) sqlite3_fullsync_count++;
  sqlite3_sync_count++;
#endif

  /* If we compiled with the SQLITE_NO_SYNC flag, then syncing is a
  ** no-op
  */
#ifdef SQLITE_NO_SYNC
  rc = SQLITE_OK;
#elif HAVE_FULLFSYNC
  if( fullSync ){
    rc = fcntl(fd, F_FULLFSYNC, 0);
  }else{
    rc = 1;
  }
  /* If the FULLFSYNC failed, fall back to attempting an fsync().
  ** It shouldn't be possible for fullfsync to fail on the local 
  ** file system (on OSX), so failure indicates that FULLFSYNC
  ** isn't supported for this file system. So, attempt an fsync 
  ** and (for now) ignore the overhead of a superfluous fcntl call.  
  ** It'd be better to detect fullfsync support once and avoid 
  ** the fcntl call every time sync is called.
  */
  if( rc ) rc = fsync(fd);

#else 
  rc = fdatasync(fd);
#if OS_VXWORKS
  if( rc==-1 && errno==ENOTSUP ){
    rc = fsync(fd);
  }
#endif /* OS_VXWORKS */
#endif /* ifdef SQLITE_NO_SYNC elif HAVE_FULLFSYNC */

  if( OS_VXWORKS && rc!= -1 ){
    rc = 0;
  }
  return rc;
}

/*
** Make sure all writes to a particular file are committed to disk.
**
** If dataOnly==0 then both the file itself and its metadata (file
** size, access time, etc) are synced.  If dataOnly!=0 then only the
** file data is synced.
**
** Under Unix, also make sure that the directory entry for the file
** has been created by fsync-ing the directory that contains the file.
** If we do not do this and we encounter a power failure, the directory
** entry for the journal might not exist after we reboot.  The next
** SQLite to access the file will not know that the journal exists (because
** the directory entry for the journal was never created) and the transaction
** will not roll back - possibly leading to database corruption.
*/
static int unixSync(sqlite3_file *id, int flags){
  int rc;
  unixFile *pFile = (unixFile*)id;

  int isDataOnly = (flags&SQLITE_SYNC_DATAONLY);
  int isFullsync = (flags&0x0F)==SQLITE_SYNC_FULL;

  /* Check that one of SQLITE_SYNC_NORMAL or FULL was passed */
  assert((flags&0x0F)==SQLITE_SYNC_NORMAL
      || (flags&0x0F)==SQLITE_SYNC_FULL
  );

  /* Unix cannot, but some systems may return SQLITE_FULL from here. This
  ** line is to test that doing so does not cause any problems.
  */
  SimulateDiskfullError( return SQLITE_FULL );

  assert( pFile );
  OSTRACE2("SYNC    %-3d\n", pFile->h);
  rc = full_fsync(pFile->h, isFullsync, isDataOnly);
  SimulateIOError( rc=1 );
  if( rc ){
    pFile->lastErrno = errno;
    return SQLITE_IOERR_FSYNC;
  }
  if( pFile->dirfd>=0 ){
    int err;
    OSTRACE4("DIRSYNC %-3d (have_fullfsync=%d fullsync=%d)\n", pFile->dirfd,
            HAVE_FULLFSYNC, isFullsync);
#ifndef SQLITE_DISABLE_DIRSYNC
    /* The directory sync is only attempted if full_fsync is
    ** turned off or unavailable.  If a full_fsync occurred above,
    ** then the directory sync is superfluous.
    */
    if( (!HAVE_FULLFSYNC || !isFullsync) && full_fsync(pFile->dirfd,0,0) ){
       /*
       ** We have received multiple reports of fsync() returning
       ** errors when applied to directories on certain file systems.
       ** A failed directory sync is not a big deal.  So it seems
       ** better to ignore the error.  Ticket #1657
       */
       /* pFile->lastErrno = errno; */
       /* return SQLITE_IOERR; */
    }
#endif
    err = close(pFile->dirfd); /* Only need to sync once, so close the */
    if( err==0 ){              /* directory when we are done */
      pFile->dirfd = -1;
    }else{
      pFile->lastErrno = errno;
      rc = SQLITE_IOERR_DIR_CLOSE;
    }
  }
  return rc;
}

/*
** Truncate an open file to a specified size
*/
static int unixTruncate(sqlite3_file *id, i64 nByte){
  int rc;
  assert( id );
  SimulateIOError( return SQLITE_IOERR_TRUNCATE );
  rc = ftruncate(((unixFile*)id)->h, (off_t)nByte);
  if( rc ){
    ((unixFile*)id)->lastErrno = errno;
    return SQLITE_IOERR_TRUNCATE;
  }else{
    return SQLITE_OK;
  }
}

/*
** Determine the current size of a file in bytes
*/
static int unixFileSize(sqlite3_file *id, i64 *pSize){
  int rc;
  struct stat buf;
  assert( id );
  rc = fstat(((unixFile*)id)->h, &buf);
  SimulateIOError( rc=1 );
  if( rc!=0 ){
    ((unixFile*)id)->lastErrno = errno;
    return SQLITE_IOERR_FSTAT;
  }
  *pSize = buf.st_size;

  /* When opening a zero-size database, the findLockInfo() procedure
  ** writes a single byte into that file in order to work around a bug
  ** in the OS-X msdos filesystem.  In order to avoid problems with upper
  ** layers, we need to report this file size as zero even though it is
  ** really 1.   Ticket #3260.
  */
  if( *pSize==1 ) *pSize = 0;


  return SQLITE_OK;
}

#if SQLITE_ENABLE_LOCKING_STYLE && defined(__APPLE__)
/*
** Handler for proxy-locking file-control verbs.  Defined below in the
** proxying locking division.
*/
static int proxyFileControl(sqlite3_file*,int,void*);
#endif


/*
** Information and control of an open file handle.
*/
static int unixFileControl(sqlite3_file *id, int op, void *pArg){
  switch( op ){
    case SQLITE_FCNTL_LOCKSTATE: {
      *(int*)pArg = ((unixFile*)id)->locktype;
      return SQLITE_OK;
    }
    case SQLITE_LAST_ERRNO: {
      *(int*)pArg = ((unixFile*)id)->lastErrno;
      return SQLITE_OK;
    }
#ifndef NDEBUG
    /* The pager calls this method to signal that it has done
    ** a rollback and that the database is therefore unchanged and
    ** it hence it is OK for the transaction change counter to be
    ** unchanged.
    */
    case SQLITE_FCNTL_DB_UNCHANGED: {
      ((unixFile*)id)->dbUpdate = 0;
      return SQLITE_OK;
    }
#endif
#if SQLITE_ENABLE_LOCKING_STYLE && defined(__APPLE__)
    case SQLITE_SET_LOCKPROXYFILE:
    case SQLITE_GET_LOCKPROXYFILE: {
      return proxyFileControl(id,op,pArg);
    }
#endif /* SQLITE_ENABLE_LOCKING_STYLE && defined(__APPLE__) */
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
static int unixSectorSize(sqlite3_file *NotUsed){
  UNUSED_PARAMETER(NotUsed);
  return SQLITE_DEFAULT_SECTOR_SIZE;
}

/*
** Return the device characteristics for the file. This is always 0 for unix.
*/
static int unixDeviceCharacteristics(sqlite3_file *NotUsed){
  UNUSED_PARAMETER(NotUsed);
  return 0;
}

/*
** Here ends the implementation of all sqlite3_file methods.
**
********************** End sqlite3_file Methods *******************************
******************************************************************************/

/*
** This division contains definitions of sqlite3_io_methods objects that
** implement various file locking strategies.  It also contains definitions
** of "finder" functions.  A finder-function is used to locate the appropriate
** sqlite3_io_methods object for a particular database file.  The pAppData
** field of the sqlite3_vfs VFS objects are initialized to be pointers to
** the correct finder-function for that VFS.
**
** Most finder functions return a pointer to a fixed sqlite3_io_methods
** object.  The only interesting finder-function is autolockIoFinder, which
** looks at the filesystem type and tries to guess the best locking
** strategy from that.
**
** For finder-funtion F, two objects are created:
**
**    (1) The real finder-function named "FImpt()".
**
**    (2) A constant pointer to this function named just "F".
**
**
** A pointer to the F pointer is used as the pAppData value for VFS
** objects.  We have to do this instead of letting pAppData point
** directly at the finder-function since C90 rules prevent a void*
** from be cast into a function pointer.
**
**
** Each instance of this macro generates two objects:
**
**   *  A constant sqlite3_io_methods object call METHOD that has locking
**      methods CLOSE, LOCK, UNLOCK, CKRESLOCK.
**
**   *  An I/O method finder function called FINDER that returns a pointer
**      to the METHOD object in the previous bullet.
*/
#define IOMETHODS(FINDER, METHOD, CLOSE, LOCK, UNLOCK, CKLOCK)               \
static const sqlite3_io_methods METHOD = {                                   \
   1,                          /* iVersion */                                \
   CLOSE,                      /* xClose */                                  \
   unixRead,                   /* xRead */                                   \
   unixWrite,                  /* xWrite */                                  \
   unixTruncate,               /* xTruncate */                               \
   unixSync,                   /* xSync */                                   \
   unixFileSize,               /* xFileSize */                               \
   LOCK,                       /* xLock */                                   \
   UNLOCK,                     /* xUnlock */                                 \
   CKLOCK,                     /* xCheckReservedLock */                      \
   unixFileControl,            /* xFileControl */                            \
   unixSectorSize,             /* xSectorSize */                             \
   unixDeviceCharacteristics   /* xDeviceCapabilities */                     \
};                                                                           \
static const sqlite3_io_methods *FINDER##Impl(const char *z, unixFile *p){   \
  UNUSED_PARAMETER(z); UNUSED_PARAMETER(p);                                  \
  return &METHOD;                                                            \
}                                                                            \
static const sqlite3_io_methods *(*const FINDER)(const char*,unixFile *p)    \
    = FINDER##Impl;

/*
** Here are all of the sqlite3_io_methods objects for each of the
** locking strategies.  Functions that return pointers to these methods
** are also created.
*/
IOMETHODS(
  posixIoFinder,            /* Finder function name */
  posixIoMethods,           /* sqlite3_io_methods object name */
  unixClose,                /* xClose method */
  unixLock,                 /* xLock method */
  unixUnlock,               /* xUnlock method */
  unixCheckReservedLock     /* xCheckReservedLock method */
)
IOMETHODS(
  nolockIoFinder,           /* Finder function name */
  nolockIoMethods,          /* sqlite3_io_methods object name */
  nolockClose,              /* xClose method */
  nolockLock,               /* xLock method */
  nolockUnlock,             /* xUnlock method */
  nolockCheckReservedLock   /* xCheckReservedLock method */
)
IOMETHODS(
  dotlockIoFinder,          /* Finder function name */
  dotlockIoMethods,         /* sqlite3_io_methods object name */
  dotlockClose,             /* xClose method */
  dotlockLock,              /* xLock method */
  dotlockUnlock,            /* xUnlock method */
  dotlockCheckReservedLock  /* xCheckReservedLock method */
)

#if SQLITE_ENABLE_LOCKING_STYLE && !OS_VXWORKS
IOMETHODS(
  flockIoFinder,            /* Finder function name */
  flockIoMethods,           /* sqlite3_io_methods object name */
  flockClose,               /* xClose method */
  flockLock,                /* xLock method */
  flockUnlock,              /* xUnlock method */
  flockCheckReservedLock    /* xCheckReservedLock method */
)
#endif

#if OS_VXWORKS
IOMETHODS(
  semIoFinder,              /* Finder function name */
  semIoMethods,             /* sqlite3_io_methods object name */
  semClose,                 /* xClose method */
  semLock,                  /* xLock method */
  semUnlock,                /* xUnlock method */
  semCheckReservedLock      /* xCheckReservedLock method */
)
#endif

#if defined(__APPLE__) && SQLITE_ENABLE_LOCKING_STYLE
IOMETHODS(
  afpIoFinder,              /* Finder function name */
  afpIoMethods,             /* sqlite3_io_methods object name */
  afpClose,                 /* xClose method */
  afpLock,                  /* xLock method */
  afpUnlock,                /* xUnlock method */
  afpCheckReservedLock      /* xCheckReservedLock method */
)
#endif

/*
** The "Whole File Locking" finder returns the same set of methods as
** the posix locking finder.  But it also sets the SQLITE_WHOLE_FILE_LOCKING
** flag to force the posix advisory locks to cover the whole file instead
** of just a small span of bytes near the 1GiB boundary.  Whole File Locking
** is useful on NFS-mounted files since it helps NFS to maintain cache
** coherency.  But it is a detriment to other filesystems since it runs
** slower.
*/
static const sqlite3_io_methods *posixWflIoFinderImpl(const char*z, unixFile*p){
  UNUSED_PARAMETER(z);
  p->fileFlags = SQLITE_WHOLE_FILE_LOCKING;
  return &posixIoMethods;
}
static const sqlite3_io_methods 
  *(*const posixWflIoFinder)(const char*,unixFile *p) = posixWflIoFinderImpl;

/*
** The proxy locking method is a "super-method" in the sense that it
** opens secondary file descriptors for the conch and lock files and
** it uses proxy, dot-file, AFP, and flock() locking methods on those
** secondary files.  For this reason, the division that implements
** proxy locking is located much further down in the file.  But we need
** to go ahead and define the sqlite3_io_methods and finder function
** for proxy locking here.  So we forward declare the I/O methods.
*/
#if defined(__APPLE__) && SQLITE_ENABLE_LOCKING_STYLE
static int proxyClose(sqlite3_file*);
static int proxyLock(sqlite3_file*, int);
static int proxyUnlock(sqlite3_file*, int);
static int proxyCheckReservedLock(sqlite3_file*, int*);
IOMETHODS(
  proxyIoFinder,            /* Finder function name */
  proxyIoMethods,           /* sqlite3_io_methods object name */
  proxyClose,               /* xClose method */
  proxyLock,                /* xLock method */
  proxyUnlock,              /* xUnlock method */
  proxyCheckReservedLock    /* xCheckReservedLock method */
)
#endif


#if defined(__APPLE__) && SQLITE_ENABLE_LOCKING_STYLE
/* 
** This "finder" function attempts to determine the best locking strategy 
** for the database file "filePath".  It then returns the sqlite3_io_methods
** object that implements that strategy.
**
** This is for MacOSX only.
*/
static const sqlite3_io_methods *autolockIoFinderImpl(
  const char *filePath,    /* name of the database file */
  unixFile *pNew           /* open file object for the database file */
){
  static const struct Mapping {
    const char *zFilesystem;              /* Filesystem type name */
    const sqlite3_io_methods *pMethods;   /* Appropriate locking method */
  } aMap[] = {
    { "hfs",    &posixIoMethods },
    { "ufs",    &posixIoMethods },
    { "afpfs",  &afpIoMethods },
#ifdef SQLITE_ENABLE_AFP_LOCKING_SMB
    { "smbfs",  &afpIoMethods },
#else
    { "smbfs",  &flockIoMethods },
#endif
    { "webdav", &nolockIoMethods },
    { 0, 0 }
  };
  int i;
  struct statfs fsInfo;
  struct flock lockInfo;

  if( !filePath ){
    /* If filePath==NULL that means we are dealing with a transient file
    ** that does not need to be locked. */
    return &nolockIoMethods;
  }
  if( statfs(filePath, &fsInfo) != -1 ){
    if( fsInfo.f_flags & MNT_RDONLY ){
      return &nolockIoMethods;
    }
    for(i=0; aMap[i].zFilesystem; i++){
      if( strcmp(fsInfo.f_fstypename, aMap[i].zFilesystem)==0 ){
        return aMap[i].pMethods;
      }
    }
  }

  /* Default case. Handles, amongst others, "nfs".
  ** Test byte-range lock using fcntl(). If the call succeeds, 
  ** assume that the file-system supports POSIX style locks. 
  */
  lockInfo.l_len = 1;
  lockInfo.l_start = 0;
  lockInfo.l_whence = SEEK_SET;
  lockInfo.l_type = F_RDLCK;
  if( fcntl(pNew->h, F_GETLK, &lockInfo)!=-1 ) {
    pNew->fileFlags = SQLITE_WHOLE_FILE_LOCKING;
    return &posixIoMethods;
  }else{
    return &dotlockIoMethods;
  }
}
static const sqlite3_io_methods 
  *(*const autolockIoFinder)(const char*,unixFile*) = autolockIoFinderImpl;

#endif /* defined(__APPLE__) && SQLITE_ENABLE_LOCKING_STYLE */

#if OS_VXWORKS && SQLITE_ENABLE_LOCKING_STYLE
/* 
** This "finder" function attempts to determine the best locking strategy 
** for the database file "filePath".  It then returns the sqlite3_io_methods
** object that implements that strategy.
**
** This is for VXWorks only.
*/
static const sqlite3_io_methods *autolockIoFinderImpl(
  const char *filePath,    /* name of the database file */
  unixFile *pNew           /* the open file object */
){
  struct flock lockInfo;

  if( !filePath ){
    /* If filePath==NULL that means we are dealing with a transient file
    ** that does not need to be locked. */
    return &nolockIoMethods;
  }

  /* Test if fcntl() is supported and use POSIX style locks.
  ** Otherwise fall back to the named semaphore method.
  */
  lockInfo.l_len = 1;
  lockInfo.l_start = 0;
  lockInfo.l_whence = SEEK_SET;
  lockInfo.l_type = F_RDLCK;
  if( fcntl(pNew->h, F_GETLK, &lockInfo)!=-1 ) {
    return &posixIoMethods;
  }else{
    return &semIoMethods;
  }
}
static const sqlite3_io_methods 
  *(*const autolockIoFinder)(const char*,unixFile*) = autolockIoFinderImpl;

#endif /* OS_VXWORKS && SQLITE_ENABLE_LOCKING_STYLE */

/*
** An abstract type for a pointer to a IO method finder function:
*/
typedef const sqlite3_io_methods *(*finder_type)(const char*,unixFile*);


/****************************************************************************
**************************** sqlite3_vfs methods ****************************
**
** This division contains the implementation of methods on the
** sqlite3_vfs object.
*/

/*
** Initialize the contents of the unixFile structure pointed to by pId.
*/
static int fillInUnixFile(
  sqlite3_vfs *pVfs,      /* Pointer to vfs object */
  int h,                  /* Open file descriptor of file being opened */
  int dirfd,              /* Directory file descriptor */
  sqlite3_file *pId,      /* Write to the unixFile structure here */
  const char *zFilename,  /* Name of the file being opened */
  int noLock,             /* Omit locking if true */
  int isDelete            /* Delete on close if true */
){
  const sqlite3_io_methods *pLockingStyle;
  unixFile *pNew = (unixFile *)pId;
  int rc = SQLITE_OK;

  assert( pNew->pLock==NULL );
  assert( pNew->pOpen==NULL );

  /* Parameter isDelete is only used on vxworks. Express this explicitly 
  ** here to prevent compiler warnings about unused parameters.
  */
  UNUSED_PARAMETER(isDelete);

  OSTRACE3("OPEN    %-3d %s\n", h, zFilename);    
  pNew->h = h;
  pNew->dirfd = dirfd;
  SET_THREADID(pNew);
  pNew->fileFlags = 0;

#if OS_VXWORKS
  pNew->pId = vxworksFindFileId(zFilename);
  if( pNew->pId==0 ){
    noLock = 1;
    rc = SQLITE_NOMEM;
  }
#endif

  if( noLock ){
    pLockingStyle = &nolockIoMethods;
  }else{
    pLockingStyle = (**(finder_type*)pVfs->pAppData)(zFilename, pNew);
#if SQLITE_ENABLE_LOCKING_STYLE
    /* Cache zFilename in the locking context (AFP and dotlock override) for
    ** proxyLock activation is possible (remote proxy is based on db name)
    ** zFilename remains valid until file is closed, to support */
    pNew->lockingContext = (void*)zFilename;
#endif
  }

  if( pLockingStyle == &posixIoMethods ){
    unixEnterMutex();
    rc = findLockInfo(pNew, &pNew->pLock, &pNew->pOpen);
    if( rc!=SQLITE_OK ){
      /* If an error occured in findLockInfo(), close the file descriptor
      ** immediately, before releasing the mutex. findLockInfo() may fail
      ** in two scenarios:
      **
      **   (a) A call to fstat() failed.
      **   (b) A malloc failed.
      **
      ** Scenario (b) may only occur if the process is holding no other
      ** file descriptors open on the same file. If there were other file
      ** descriptors on this file, then no malloc would be required by
      ** findLockInfo(). If this is the case, it is quite safe to close
      ** handle h - as it is guaranteed that no posix locks will be released
      ** by doing so.
      **
      ** If scenario (a) caused the error then things are not so safe. The
      ** implicit assumption here is that if fstat() fails, things are in
      ** such bad shape that dropping a lock or two doesn't matter much.
      */
      close(h);
      h = -1;
    }
    unixLeaveMutex();
  }

#if SQLITE_ENABLE_LOCKING_STYLE && defined(__APPLE__)
  else if( pLockingStyle == &afpIoMethods ){
    /* AFP locking uses the file path so it needs to be included in
    ** the afpLockingContext.
    */
    afpLockingContext *pCtx;
    pNew->lockingContext = pCtx = sqlite3_malloc( sizeof(*pCtx) );
    if( pCtx==0 ){
      rc = SQLITE_NOMEM;
    }else{
      /* NB: zFilename exists and remains valid until the file is closed
      ** according to requirement F11141.  So we do not need to make a
      ** copy of the filename. */
      pCtx->dbPath = zFilename;
      srandomdev();
      unixEnterMutex();
      rc = findLockInfo(pNew, NULL, &pNew->pOpen);
      unixLeaveMutex();        
    }
  }
#endif

  else if( pLockingStyle == &dotlockIoMethods ){
    /* Dotfile locking uses the file path so it needs to be included in
    ** the dotlockLockingContext 
    */
    char *zLockFile;
    int nFilename;
    nFilename = (int)strlen(zFilename) + 6;
    zLockFile = (char *)sqlite3_malloc(nFilename);
    if( zLockFile==0 ){
      rc = SQLITE_NOMEM;
    }else{
      sqlite3_snprintf(nFilename, zLockFile, "%s" DOTLOCK_SUFFIX, zFilename);
    }
    pNew->lockingContext = zLockFile;
  }

#if OS_VXWORKS
  else if( pLockingStyle == &semIoMethods ){
    /* Named semaphore locking uses the file path so it needs to be
    ** included in the semLockingContext
    */
    unixEnterMutex();
    rc = findLockInfo(pNew, &pNew->pLock, &pNew->pOpen);
    if( (rc==SQLITE_OK) && (pNew->pOpen->pSem==NULL) ){
      char *zSemName = pNew->pOpen->aSemName;
      int n;
      sqlite3_snprintf(MAX_PATHNAME, zSemName, "/%s.sem",
                       pNew->pId->zCanonicalName);
      for( n=1; zSemName[n]; n++ )
        if( zSemName[n]=='/' ) zSemName[n] = '_';
      pNew->pOpen->pSem = sem_open(zSemName, O_CREAT, 0666, 1);
      if( pNew->pOpen->pSem == SEM_FAILED ){
        rc = SQLITE_NOMEM;
        pNew->pOpen->aSemName[0] = '\0';
      }
    }
    unixLeaveMutex();
  }
#endif
  
  pNew->lastErrno = 0;
#if OS_VXWORKS
  if( rc!=SQLITE_OK ){
    unlink(zFilename);
    isDelete = 0;
  }
  pNew->isDelete = isDelete;
#endif
  if( rc!=SQLITE_OK ){
    if( dirfd>=0 ) close(dirfd); /* silent leak if fail, already in error */
    if( h>=0 ) close(h);
  }else{
    pNew->pMethod = pLockingStyle;
    OpenCounter(+1);
  }
  return rc;
}

/*
** Open a file descriptor to the directory containing file zFilename.
** If successful, *pFd is set to the opened file descriptor and
** SQLITE_OK is returned. If an error occurs, either SQLITE_NOMEM
** or SQLITE_CANTOPEN is returned and *pFd is set to an undefined
** value.
**
** If SQLITE_OK is returned, the caller is responsible for closing
** the file descriptor *pFd using close().
*/
static int openDirectory(const char *zFilename, int *pFd){
  int ii;
  int fd = -1;
  char zDirname[MAX_PATHNAME+1];

  sqlite3_snprintf(MAX_PATHNAME, zDirname, "%s", zFilename);
  for(ii=(int)strlen(zDirname); ii>1 && zDirname[ii]!='/'; ii--);
  if( ii>0 ){
    zDirname[ii] = '\0';
    fd = open(zDirname, O_RDONLY|O_BINARY, 0);
    if( fd>=0 ){
#ifdef FD_CLOEXEC
      fcntl(fd, F_SETFD, fcntl(fd, F_GETFD, 0) | FD_CLOEXEC);
#endif
      OSTRACE3("OPENDIR %-3d %s\n", fd, zDirname);
    }
  }
  *pFd = fd;
  return (fd>=0?SQLITE_OK:SQLITE_CANTOPEN);
}

/*
** Create a temporary file name in zBuf.  zBuf must be allocated
** by the calling process and must be big enough to hold at least
** pVfs->mxPathname bytes.
*/
static int getTempname(int nBuf, char *zBuf){
  static const char *azDirs[] = {
     0,
     0,
     "/var/tmp",
     "/usr/tmp",
     "/tmp",
     ".",
  };
  static const unsigned char zChars[] =
    "abcdefghijklmnopqrstuvwxyz"
    "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    "0123456789";
  unsigned int i, j;
  struct stat buf;
  const char *zDir = ".";

  /* It's odd to simulate an io-error here, but really this is just
  ** using the io-error infrastructure to test that SQLite handles this
  ** function failing. 
  */
  SimulateIOError( return SQLITE_IOERR );

  azDirs[0] = sqlite3_temp_directory;
  if (NULL == azDirs[1]) {
    azDirs[1] = getenv("TMPDIR");
  }
  
  for(i=0; i<sizeof(azDirs)/sizeof(azDirs[0]); i++){
    if( azDirs[i]==0 ) continue;
    if( stat(azDirs[i], &buf) ) continue;
    if( !S_ISDIR(buf.st_mode) ) continue;
    if( access(azDirs[i], 07) ) continue;
    zDir = azDirs[i];
    break;
  }

  /* Check that the output buffer is large enough for the temporary file 
  ** name. If it is not, return SQLITE_ERROR.
  */
  if( (strlen(zDir) + strlen(SQLITE_TEMP_FILE_PREFIX) + 17) >= (size_t)nBuf ){
    return SQLITE_ERROR;
  }

  do{
    sqlite3_snprintf(nBuf-17, zBuf, "%s/"SQLITE_TEMP_FILE_PREFIX, zDir);
    j = (int)strlen(zBuf);
    sqlite3_randomness(15, &zBuf[j]);
    for(i=0; i<15; i++, j++){
      zBuf[j] = (char)zChars[ ((unsigned char)zBuf[j])%(sizeof(zChars)-1) ];
    }
    zBuf[j] = 0;
  }while( access(zBuf,0)==0 );
  return SQLITE_OK;
}

#if SQLITE_ENABLE_LOCKING_STYLE && defined(__APPLE__)
/*
** Routine to transform a unixFile into a proxy-locking unixFile.
** Implementation in the proxy-lock division, but used by unixOpen()
** if SQLITE_PREFER_PROXY_LOCKING is defined.
*/
static int proxyTransformUnixFile(unixFile*, const char*);
#endif

/*
** Search for an unused file descriptor that was opened on the database 
** file (not a journal or master-journal file) identified by pathname
** zPath with SQLITE_OPEN_XXX flags matching those passed as the second
** argument to this function.
**
** Such a file descriptor may exist if a database connection was closed
** but the associated file descriptor could not be closed because some
** other file descriptor open on the same file is holding a file-lock.
** Refer to comments in the unixClose() function and the lengthy comment
** describing "Posix Advisory Locking" at the start of this file for 
** further details. Also, ticket #4018.
**
** If a suitable file descriptor is found, then it is returned. If no
** such file descriptor is located, -1 is returned.
*/
static UnixUnusedFd *findReusableFd(const char *zPath, int flags){
  UnixUnusedFd *pUnused = 0;

  /* Do not search for an unused file descriptor on vxworks. Not because
  ** vxworks would not benefit from the change (it might, we're not sure),
  ** but because no way to test it is currently available. It is better 
  ** not to risk breaking vxworks support for the sake of such an obscure 
  ** feature.  */
#if !OS_VXWORKS
  struct stat sStat;                   /* Results of stat() call */

  /* A stat() call may fail for various reasons. If this happens, it is
  ** almost certain that an open() call on the same path will also fail.
  ** For this reason, if an error occurs in the stat() call here, it is
  ** ignored and -1 is returned. The caller will try to open a new file
  ** descriptor on the same path, fail, and return an error to SQLite.
  **
  ** Even if a subsequent open() call does succeed, the consequences of
  ** not searching for a resusable file descriptor are not dire.  */
  if( 0==stat(zPath, &sStat) ){
    struct unixOpenCnt *pO;
    struct unixFileId id;
    id.dev = sStat.st_dev;
    id.ino = sStat.st_ino;

    unixEnterMutex();
    for(pO=openList; pO && memcmp(&id, &pO->fileId, sizeof(id)); pO=pO->pNext);
    if( pO ){
      UnixUnusedFd **pp;
      for(pp=&pO->pUnused; *pp && (*pp)->flags!=flags; pp=&((*pp)->pNext));
      pUnused = *pp;
      if( pUnused ){
        *pp = pUnused->pNext;
      }
    }
    unixLeaveMutex();
  }
#endif    /* if !OS_VXWORKS */
  return pUnused;
}

/*
** Open the file zPath.
** 
** Previously, the SQLite OS layer used three functions in place of this
** one:
**
**     sqlite3OsOpenReadWrite();
**     sqlite3OsOpenReadOnly();
**     sqlite3OsOpenExclusive();
**
** These calls correspond to the following combinations of flags:
**
**     ReadWrite() ->     (READWRITE | CREATE)
**     ReadOnly()  ->     (READONLY) 
**     OpenExclusive() -> (READWRITE | CREATE | EXCLUSIVE)
**
** The old OpenExclusive() accepted a boolean argument - "delFlag". If
** true, the file was configured to be automatically deleted when the
** file handle closed. To achieve the same effect using this new 
** interface, add the DELETEONCLOSE flag to those specified above for 
** OpenExclusive().
*/
static int unixOpen(
  sqlite3_vfs *pVfs,           /* The VFS for which this is the xOpen method */
  const char *zPath,           /* Pathname of file to be opened */
  sqlite3_file *pFile,         /* The file descriptor to be filled in */
  int flags,                   /* Input flags to control the opening */
  int *pOutFlags               /* Output flags returned to SQLite core */
){
  unixFile *p = (unixFile *)pFile;
  int fd = -1;                   /* File descriptor returned by open() */
  int dirfd = -1;                /* Directory file descriptor */
  int openFlags = 0;             /* Flags to pass to open() */
  int eType = flags&0xFFFFFF00;  /* Type of file to open */
  int noLock;                    /* True to omit locking primitives */
  int rc = SQLITE_OK;            /* Function Return Code */

  int isExclusive  = (flags & SQLITE_OPEN_EXCLUSIVE);
  int isDelete     = (flags & SQLITE_OPEN_DELETEONCLOSE);
  int isCreate     = (flags & SQLITE_OPEN_CREATE);
  int isReadonly   = (flags & SQLITE_OPEN_READONLY);
  int isReadWrite  = (flags & SQLITE_OPEN_READWRITE);

  /* If creating a master or main-file journal, this function will open
  ** a file-descriptor on the directory too. The first time unixSync()
  ** is called the directory file descriptor will be fsync()ed and close()d.
  */
  int isOpenDirectory = (isCreate && 
      (eType==SQLITE_OPEN_MASTER_JOURNAL || eType==SQLITE_OPEN_MAIN_JOURNAL)
  );

  /* If argument zPath is a NULL pointer, this function is required to open
  ** a temporary file. Use this buffer to store the file name in.
  */
  char zTmpname[MAX_PATHNAME+1];
  const char *zName = zPath;

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

  /* The main DB, main journal, and master journal are never automatically
  ** deleted. Nor are they ever temporary files.  */
  assert( (!isDelete && zName) || eType!=SQLITE_OPEN_MAIN_DB );
  assert( (!isDelete && zName) || eType!=SQLITE_OPEN_MAIN_JOURNAL );
  assert( (!isDelete && zName) || eType!=SQLITE_OPEN_MASTER_JOURNAL );

  /* Assert that the upper layer has set one of the "file-type" flags. */
  assert( eType==SQLITE_OPEN_MAIN_DB      || eType==SQLITE_OPEN_TEMP_DB 
       || eType==SQLITE_OPEN_MAIN_JOURNAL || eType==SQLITE_OPEN_TEMP_JOURNAL 
       || eType==SQLITE_OPEN_SUBJOURNAL   || eType==SQLITE_OPEN_MASTER_JOURNAL 
       || eType==SQLITE_OPEN_TRANSIENT_DB
  );

  memset(p, 0, sizeof(unixFile));

  if( eType==SQLITE_OPEN_MAIN_DB ){
    UnixUnusedFd *pUnused;
    pUnused = findReusableFd(zName, flags);
    if( pUnused ){
      fd = pUnused->fd;
    }else{
      pUnused = sqlite3_malloc(sizeof(*pUnused));
      if( !pUnused ){
        return SQLITE_NOMEM;
      }
    }
    p->pUnused = pUnused;
  }else if( !zName ){
    /* If zName is NULL, the upper layer is requesting a temp file. */
    assert(isDelete && !isOpenDirectory);
    rc = getTempname(MAX_PATHNAME+1, zTmpname);
    if( rc!=SQLITE_OK ){
      return rc;
    }
    zName = zTmpname;
  }

  /* Determine the value of the flags parameter passed to POSIX function
  ** open(). These must be calculated even if open() is not called, as
  ** they may be stored as part of the file handle and used by the 
  ** 'conch file' locking functions later on.  */
  if( isReadonly )  openFlags |= O_RDONLY;
  if( isReadWrite ) openFlags |= O_RDWR;
  if( isCreate )    openFlags |= O_CREAT;
  if( isExclusive ) openFlags |= (O_EXCL|O_NOFOLLOW);
  openFlags |= (O_LARGEFILE|O_BINARY);

  if( fd<0 ){
    mode_t openMode = (isDelete?0600:SQLITE_DEFAULT_FILE_PERMISSIONS);
    fd = open(zName, openFlags, openMode);
    OSTRACE4("OPENX   %-3d %s 0%o\n", fd, zName, openFlags);
    if( fd<0 && errno!=EISDIR && isReadWrite && !isExclusive ){
      /* Failed to open the file for read/write access. Try read-only. */
      flags &= ~(SQLITE_OPEN_READWRITE|SQLITE_OPEN_CREATE);
      openFlags &= ~(O_RDWR|O_CREAT);
      flags |= SQLITE_OPEN_READONLY;
      openFlags |= O_RDONLY;
      fd = open(zName, openFlags, openMode);
    }
    if( fd<0 ){
      rc = SQLITE_CANTOPEN;
      goto open_finished;
    }
  }
  assert( fd>=0 );
  if( pOutFlags ){
    *pOutFlags = flags;
  }

  if( p->pUnused ){
    p->pUnused->fd = fd;
    p->pUnused->flags = flags;
  }

  if( isDelete ){
#if OS_VXWORKS
    zPath = zName;
#else
    unlink(zName);
#endif
  }
#if SQLITE_ENABLE_LOCKING_STYLE
  else{
    p->openFlags = openFlags;
  }
#endif

  if( isOpenDirectory ){
    rc = openDirectory(zPath, &dirfd);
    if( rc!=SQLITE_OK ){
      /* It is safe to close fd at this point, because it is guaranteed not
      ** to be open on a database file. If it were open on a database file,
      ** it would not be safe to close as this would release any locks held
      ** on the file by this process.  */
      assert( eType!=SQLITE_OPEN_MAIN_DB );
      close(fd);             /* silently leak if fail, already in error */
      goto open_finished;
    }
  }

#ifdef FD_CLOEXEC
  fcntl(fd, F_SETFD, fcntl(fd, F_GETFD, 0) | FD_CLOEXEC);
#endif

  noLock = eType!=SQLITE_OPEN_MAIN_DB;

#if SQLITE_PREFER_PROXY_LOCKING
  if( zPath!=NULL && !noLock && pVfs->xOpen ){
    char *envforce = getenv("SQLITE_FORCE_PROXY_LOCKING");
    int useProxy = 0;

    /* SQLITE_FORCE_PROXY_LOCKING==1 means force always use proxy, 0 means 
    ** never use proxy, NULL means use proxy for non-local files only.  */
    if( envforce!=NULL ){
      useProxy = atoi(envforce)>0;
    }else{
      struct statfs fsInfo;
      if( statfs(zPath, &fsInfo) == -1 ){
        /* In theory, the close(fd) call is sub-optimal. If the file opened
        ** with fd is a database file, and there are other connections open
        ** on that file that are currently holding advisory locks on it,
        ** then the call to close() will cancel those locks. In practice,
        ** we're assuming that statfs() doesn't fail very often. At least
        ** not while other file descriptors opened by the same process on
        ** the same file are working.  */
        p->lastErrno = errno;
        if( dirfd>=0 ){
          close(dirfd); /* silently leak if fail, in error */
        }
        close(fd); /* silently leak if fail, in error */
        rc = SQLITE_IOERR_ACCESS;
        goto open_finished;
      }
      useProxy = !(fsInfo.f_flags&MNT_LOCAL);
    }
    if( useProxy ){
      rc = fillInUnixFile(pVfs, fd, dirfd, pFile, zPath, noLock, isDelete);
      if( rc==SQLITE_OK ){
        rc = proxyTransformUnixFile((unixFile*)pFile, ":auto:");
      }
      goto open_finished;
    }
  }
#endif
  
  rc = fillInUnixFile(pVfs, fd, dirfd, pFile, zPath, noLock, isDelete);
open_finished:
  if( rc!=SQLITE_OK ){
    sqlite3_free(p->pUnused);
  }
  return rc;
}


/*
** Delete the file at zPath. If the dirSync argument is true, fsync()
** the directory after deleting the file.
*/
static int unixDelete(
  sqlite3_vfs *NotUsed,     /* VFS containing this as the xDelete method */
  const char *zPath,        /* Name of file to be deleted */
  int dirSync               /* If true, fsync() directory after deleting file */
){
  int rc = SQLITE_OK;
  UNUSED_PARAMETER(NotUsed);
  SimulateIOError(return SQLITE_IOERR_DELETE);
  unlink(zPath);
#ifndef SQLITE_DISABLE_DIRSYNC
  if( dirSync ){
    int fd;
    rc = openDirectory(zPath, &fd);
    if( rc==SQLITE_OK ){
#if OS_VXWORKS
      if( fsync(fd)==-1 )
#else
      if( fsync(fd) )
#endif
      {
        rc = SQLITE_IOERR_DIR_FSYNC;
      }
      if( close(fd)&&!rc ){
        rc = SQLITE_IOERR_DIR_CLOSE;
      }
    }
  }
#endif
  return rc;
}

/*
** Test the existance of or access permissions of file zPath. The
** test performed depends on the value of flags:
**
**     SQLITE_ACCESS_EXISTS: Return 1 if the file exists
**     SQLITE_ACCESS_READWRITE: Return 1 if the file is read and writable.
**     SQLITE_ACCESS_READONLY: Return 1 if the file is readable.
**
** Otherwise return 0.
*/
static int unixAccess(
  sqlite3_vfs *NotUsed,   /* The VFS containing this xAccess method */
  const char *zPath,      /* Path of the file to examine */
  int flags,              /* What do we want to learn about the zPath file? */
  int *pResOut            /* Write result boolean here */
){
  int amode = 0;
  UNUSED_PARAMETER(NotUsed);
  SimulateIOError( return SQLITE_IOERR_ACCESS; );
  switch( flags ){
    case SQLITE_ACCESS_EXISTS:
      amode = F_OK;
      break;
    case SQLITE_ACCESS_READWRITE:
      amode = W_OK|R_OK;
      break;
    case SQLITE_ACCESS_READ:
      amode = R_OK;
      break;

    default:
      assert(!"Invalid flags argument");
  }
  *pResOut = (access(zPath, amode)==0);
  return SQLITE_OK;
}


/*
** Turn a relative pathname into a full pathname. The relative path
** is stored as a nul-terminated string in the buffer pointed to by
** zPath. 
**
** zOut points to a buffer of at least sqlite3_vfs.mxPathname bytes 
** (in this case, MAX_PATHNAME bytes). The full-path is written to
** this buffer before returning.
*/
static int unixFullPathname(
  sqlite3_vfs *pVfs,            /* Pointer to vfs object */
  const char *zPath,            /* Possibly relative input path */
  int nOut,                     /* Size of output buffer in bytes */
  char *zOut                    /* Output buffer */
){

  /* It's odd to simulate an io-error here, but really this is just
  ** using the io-error infrastructure to test that SQLite handles this
  ** function failing. This function could fail if, for example, the
  ** current working directory has been unlinked.
  */
  SimulateIOError( return SQLITE_ERROR );

  assert( pVfs->mxPathname==MAX_PATHNAME );
  UNUSED_PARAMETER(pVfs);

  zOut[nOut-1] = '\0';
  if( zPath[0]=='/' ){
    sqlite3_snprintf(nOut, zOut, "%s", zPath);
  }else{
    int nCwd;
    if( getcwd(zOut, nOut-1)==0 ){
      return SQLITE_CANTOPEN;
    }
    nCwd = (int)strlen(zOut);
    sqlite3_snprintf(nOut-nCwd, &zOut[nCwd], "/%s", zPath);
  }
  return SQLITE_OK;
}


#ifndef SQLITE_OMIT_LOAD_EXTENSION
/*
** Interfaces for opening a shared library, finding entry points
** within the shared library, and closing the shared library.
*/
#include <dlfcn.h>
static void *unixDlOpen(sqlite3_vfs *NotUsed, const char *zFilename){
  UNUSED_PARAMETER(NotUsed);
  return dlopen(zFilename, RTLD_NOW | RTLD_GLOBAL);
}

/*
** SQLite calls this function immediately after a call to unixDlSym() or
** unixDlOpen() fails (returns a null pointer). If a more detailed error
** message is available, it is written to zBufOut. If no error message
** is available, zBufOut is left unmodified and SQLite uses a default
** error message.
*/
static void unixDlError(sqlite3_vfs *NotUsed, int nBuf, char *zBufOut){
  char *zErr;
  UNUSED_PARAMETER(NotUsed);
  unixEnterMutex();
  zErr = dlerror();
  if( zErr ){
    sqlite3_snprintf(nBuf, zBufOut, "%s", zErr);
  }
  unixLeaveMutex();
}
static void (*unixDlSym(sqlite3_vfs *NotUsed, void *p, const char*zSym))(void){
  /* 
  ** GCC with -pedantic-errors says that C90 does not allow a void* to be
  ** cast into a pointer to a function.  And yet the library dlsym() routine
  ** returns a void* which is really a pointer to a function.  So how do we
  ** use dlsym() with -pedantic-errors?
  **
  ** Variable x below is defined to be a pointer to a function taking
  ** parameters void* and const char* and returning a pointer to a function.
  ** We initialize x by assigning it a pointer to the dlsym() function.
  ** (That assignment requires a cast.)  Then we call the function that
  ** x points to.  
  **
  ** This work-around is unlikely to work correctly on any system where
  ** you really cannot cast a function pointer into void*.  But then, on the
  ** other hand, dlsym() will not work on such a system either, so we have
  ** not really lost anything.
  */
  void (*(*x)(void*,const char*))(void);
  UNUSED_PARAMETER(NotUsed);
  x = (void(*(*)(void*,const char*))(void))dlsym;
  return (*x)(p, zSym);
}
static void unixDlClose(sqlite3_vfs *NotUsed, void *pHandle){
  UNUSED_PARAMETER(NotUsed);
  dlclose(pHandle);
}
#else /* if SQLITE_OMIT_LOAD_EXTENSION is defined: */
  #define unixDlOpen  0
  #define unixDlError 0
  #define unixDlSym   0
  #define unixDlClose 0
#endif

/*
** Write nBuf bytes of random data to the supplied buffer zBuf.
*/
static int unixRandomness(sqlite3_vfs *NotUsed, int nBuf, char *zBuf){
  UNUSED_PARAMETER(NotUsed);
  assert((size_t)nBuf>=(sizeof(time_t)+sizeof(int)));

  /* We have to initialize zBuf to prevent valgrind from reporting
  ** errors.  The reports issued by valgrind are incorrect - we would
  ** prefer that the randomness be increased by making use of the
  ** uninitialized space in zBuf - but valgrind errors tend to worry
  ** some users.  Rather than argue, it seems easier just to initialize
  ** the whole array and silence valgrind, even if that means less randomness
  ** in the random seed.
  **
  ** When testing, initializing zBuf[] to zero is all we do.  That means
  ** that we always use the same random number sequence.  This makes the
  ** tests repeatable.
  */
  memset(zBuf, 0, nBuf);
#if !defined(SQLITE_TEST)
  {
    int pid, fd;
    fd = open("/dev/urandom", O_RDONLY);
    if( fd<0 ){
      time_t t;
      time(&t);
      memcpy(zBuf, &t, sizeof(t));
      pid = getpid();
      memcpy(&zBuf[sizeof(t)], &pid, sizeof(pid));
      assert( sizeof(t)+sizeof(pid)<=(size_t)nBuf );
      nBuf = sizeof(t) + sizeof(pid);
    }else{
      nBuf = read(fd, zBuf, nBuf);
      close(fd);
    }
  }
#endif
  return nBuf;
}


/*
** Sleep for a little while.  Return the amount of time slept.
** The argument is the number of microseconds we want to sleep.
** The return value is the number of microseconds of sleep actually
** requested from the underlying operating system, a number which
** might be greater than or equal to the argument, but not less
** than the argument.
*/
static int unixSleep(sqlite3_vfs *NotUsed, int microseconds){
#if OS_VXWORKS
  struct timespec sp;

  sp.tv_sec = microseconds / 1000000;
  sp.tv_nsec = (microseconds % 1000000) * 1000;
  nanosleep(&sp, NULL);
  UNUSED_PARAMETER(NotUsed);
  return microseconds;
#elif defined(HAVE_USLEEP) && HAVE_USLEEP
  usleep(microseconds);
  UNUSED_PARAMETER(NotUsed);
  return microseconds;
#else
  int seconds = (microseconds+999999)/1000000;
  sleep(seconds);
  UNUSED_PARAMETER(NotUsed);
  return seconds*1000000;
#endif
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
** Find the current time (in Universal Coordinated Time).  Write the
** current time and date as a Julian Day number into *prNow and
** return 0.  Return 1 if the time and date cannot be found.
*/
static int unixCurrentTime(sqlite3_vfs *NotUsed, double *prNow){
#if defined(SQLITE_OMIT_FLOATING_POINT)
  time_t t;
  time(&t);
  *prNow = (((sqlite3_int64)t)/8640 + 24405875)/10;
#elif defined(NO_GETTOD)
  time_t t;
  time(&t);
  *prNow = t/86400.0 + 2440587.5;
#elif OS_VXWORKS
  struct timespec sNow;
  clock_gettime(CLOCK_REALTIME, &sNow);
  *prNow = 2440587.5 + sNow.tv_sec/86400.0 + sNow.tv_nsec/86400000000000.0;
#else
  struct timeval sNow;
  gettimeofday(&sNow, 0);
  *prNow = 2440587.5 + sNow.tv_sec/86400.0 + sNow.tv_usec/86400000000.0;
#endif

#ifdef SQLITE_TEST
  if( sqlite3_current_time ){
    *prNow = sqlite3_current_time/86400.0 + 2440587.5;
  }
#endif
  UNUSED_PARAMETER(NotUsed);
  return 0;
}

/*
** We added the xGetLastError() method with the intention of providing
** better low-level error messages when operating-system problems come up
** during SQLite operation.  But so far, none of that has been implemented
** in the core.  So this routine is never called.  For now, it is merely
** a place-holder.
*/
static int unixGetLastError(sqlite3_vfs *NotUsed, int NotUsed2, char *NotUsed3){
  UNUSED_PARAMETER(NotUsed);
  UNUSED_PARAMETER(NotUsed2);
  UNUSED_PARAMETER(NotUsed3);
  return 0;
}

/*
************************ End of sqlite3_vfs methods ***************************
******************************************************************************/

/******************************************************************************
************************** Begin Proxy Locking ********************************
**
** Proxy locking is a "uber-locking-method" in this sense:  It uses the
** other locking methods on secondary lock files.  Proxy locking is a
** meta-layer over top of the primitive locking implemented above.  For
** this reason, the division that implements of proxy locking is deferred
** until late in the file (here) after all of the other I/O methods have
** been defined - so that the primitive locking methods are available
** as services to help with the implementation of proxy locking.
**
****
**
** The default locking schemes in SQLite use byte-range locks on the
** database file to coordinate safe, concurrent access by multiple readers
** and writers [http://sqlite.org/lockingv3.html].  The five file locking
** states (UNLOCKED, PENDING, SHARED, RESERVED, EXCLUSIVE) are implemented
** as POSIX read & write locks over fixed set of locations (via fsctl),
** on AFP and SMB only exclusive byte-range locks are available via fsctl
** with _IOWR('z', 23, struct ByteRangeLockPB2) to track the same 5 states.
** To simulate a F_RDLCK on the shared range, on AFP a randomly selected
** address in the shared range is taken for a SHARED lock, the entire
** shared range is taken for an EXCLUSIVE lock):
**
**      PENDING_BYTE        0x40000000		   	
**      RESERVED_BYTE       0x40000001
**      SHARED_RANGE        0x40000002 -> 0x40000200
**
** This works well on the local file system, but shows a nearly 100x
** slowdown in read performance on AFP because the AFP client disables
** the read cache when byte-range locks are present.  Enabling the read
** cache exposes a cache coherency problem that is present on all OS X
** supported network file systems.  NFS and AFP both observe the
** close-to-open semantics for ensuring cache coherency
** [http://nfs.sourceforge.net/#faq_a8], which does not effectively
** address the requirements for concurrent database access by multiple
** readers and writers
** [http://www.nabble.com/SQLite-on-NFS-cache-coherency-td15655701.html].
**
** To address the performance and cache coherency issues, proxy file locking
** changes the way database access is controlled by limiting access to a
** single host at a time and moving file locks off of the database file
** and onto a proxy file on the local file system.  
**
**
** Using proxy locks
** -----------------
**
** C APIs
**
**  sqlite3_file_control(db, dbname, SQLITE_SET_LOCKPROXYFILE,
**                       <proxy_path> | ":auto:");
**  sqlite3_file_control(db, dbname, SQLITE_GET_LOCKPROXYFILE, &<proxy_path>);
**
**
** SQL pragmas
**
**  PRAGMA [database.]lock_proxy_file=<proxy_path> | :auto:
**  PRAGMA [database.]lock_proxy_file
**
** Specifying ":auto:" means that if there is a conch file with a matching
** host ID in it, the proxy path in the conch file will be used, otherwise
** a proxy path based on the user's temp dir
** (via confstr(_CS_DARWIN_USER_TEMP_DIR,...)) will be used and the
** actual proxy file name is generated from the name and path of the
** database file.  For example:
**
**       For database path "/Users/me/foo.db" 
**       The lock path will be "<tmpdir>/sqliteplocks/_Users_me_foo.db:auto:")
**
** Once a lock proxy is configured for a database connection, it can not
** be removed, however it may be switched to a different proxy path via
** the above APIs (assuming the conch file is not being held by another
** connection or process). 
**
**
** How proxy locking works
** -----------------------
**
** Proxy file locking relies primarily on two new supporting files: 
**
**   *  conch file to limit access to the database file to a single host
**      at a time
**
**   *  proxy file to act as a proxy for the advisory locks normally
**      taken on the database
**
** The conch file - to use a proxy file, sqlite must first "hold the conch"
** by taking an sqlite-style shared lock on the conch file, reading the
** contents and comparing the host's unique host ID (see below) and lock
** proxy path against the values stored in the conch.  The conch file is
** stored in the same directory as the database file and the file name
** is patterned after the database file name as ".<databasename>-conch".
** If the conch file does not exist, or it's contents do not match the
** host ID and/or proxy path, then the lock is escalated to an exclusive
** lock and the conch file contents is updated with the host ID and proxy
** path and the lock is downgraded to a shared lock again.  If the conch
** is held by another process (with a shared lock), the exclusive lock
** will fail and SQLITE_BUSY is returned.
**
** The proxy file - a single-byte file used for all advisory file locks
** normally taken on the database file.   This allows for safe sharing
** of the database file for multiple readers and writers on the same
** host (the conch ensures that they all use the same local lock file).
**
** There is a third file - the host ID file - used as a persistent record
** of a unique identifier for the host, a 128-byte unique host id file
** in the path defined by the HOSTIDPATH macro (default value is
** /Library/Caches/.com.apple.sqliteConchHostId).
**
** Requesting the lock proxy does not immediately take the conch, it is
** only taken when the first request to lock database file is made.  
** This matches the semantics of the traditional locking behavior, where
** opening a connection to a database file does not take a lock on it.
** The shared lock and an open file descriptor are maintained until 
** the connection to the database is closed. 
**
** The proxy file and the lock file are never deleted so they only need
** to be created the first time they are used.
**
** Configuration options
** ---------------------
**
**  SQLITE_PREFER_PROXY_LOCKING
**
**       Database files accessed on non-local file systems are
**       automatically configured for proxy locking, lock files are
**       named automatically using the same logic as
**       PRAGMA lock_proxy_file=":auto:"
**    
**  SQLITE_PROXY_DEBUG
**
**       Enables the logging of error messages during host id file
**       retrieval and creation
**
**  HOSTIDPATH
**
**       Overrides the default host ID file path location
**
**  LOCKPROXYDIR
**
**       Overrides the default directory used for lock proxy files that
**       are named automatically via the ":auto:" setting
**
**  SQLITE_DEFAULT_PROXYDIR_PERMISSIONS
**
**       Permissions to use when creating a directory for storing the
**       lock proxy files, only used when LOCKPROXYDIR is not set.
**    
**    
** As mentioned above, when compiled with SQLITE_PREFER_PROXY_LOCKING,
** setting the environment variable SQLITE_FORCE_PROXY_LOCKING to 1 will
** force proxy locking to be used for every database file opened, and 0
** will force automatic proxy locking to be disabled for all database
** files (explicity calling the SQLITE_SET_LOCKPROXYFILE pragma or
** sqlite_file_control API is not affected by SQLITE_FORCE_PROXY_LOCKING).
*/

/*
** Proxy locking is only available on MacOSX 
*/
#if defined(__APPLE__) && SQLITE_ENABLE_LOCKING_STYLE

#ifdef SQLITE_TEST
/* simulate multiple hosts by creating unique hostid file paths */
int sqlite3_hostid_num = 0;
#endif

/*
** The proxyLockingContext has the path and file structures for the remote 
** and local proxy files in it
*/
typedef struct proxyLockingContext proxyLockingContext;
struct proxyLockingContext {
  unixFile *conchFile;         /* Open conch file */
  char *conchFilePath;         /* Name of the conch file */
  unixFile *lockProxy;         /* Open proxy lock file */
  char *lockProxyPath;         /* Name of the proxy lock file */
  char *dbPath;                /* Name of the open file */
  int conchHeld;               /* True if the conch is currently held */
  void *oldLockingContext;     /* Original lockingcontext to restore on close */
  sqlite3_io_methods const *pOldMethod;     /* Original I/O methods for close */
};

/* HOSTIDLEN and CONCHLEN both include space for the string 
** terminating nul 
*/
#define HOSTIDLEN         128
#define CONCHLEN          (MAXPATHLEN+HOSTIDLEN+1)
#ifndef HOSTIDPATH
# define HOSTIDPATH       "/Library/Caches/.com.apple.sqliteConchHostId"
#endif

/* basically a copy of unixRandomness with different
** test behavior built in */
static int proxyGenerateHostID(char *pHostID){
  int pid, fd, len;
  unsigned char *key = (unsigned char *)pHostID;
  
  memset(key, 0, HOSTIDLEN);
  len = 0;
  fd = open("/dev/urandom", O_RDONLY);
  if( fd>=0 ){
    len = read(fd, key, HOSTIDLEN);
    close(fd); /* silently leak the fd if it fails */
  }
  if( len < HOSTIDLEN ){
    time_t t;
    time(&t);
    memcpy(key, &t, sizeof(t));
    pid = getpid();
    memcpy(&key[sizeof(t)], &pid, sizeof(pid));
  }
  
#ifdef MAKE_PRETTY_HOSTID
  {
    int i;
    /* filter the bytes into printable ascii characters and NUL terminate */
    key[(HOSTIDLEN-1)] = 0x00;
    for( i=0; i<(HOSTIDLEN-1); i++ ){
      unsigned char pa = key[i]&0x7F;
      if( pa<0x20 ){
        key[i] = (key[i]&0x80 == 0x80) ? pa+0x40 : pa+0x20;
      }else if( pa==0x7F ){
        key[i] = (key[i]&0x80 == 0x80) ? pa=0x20 : pa+0x7E;
      }
    }
  }
#endif
  return SQLITE_OK;
}

/* writes the host id path to path, path should be an pre-allocated buffer
** with enough space for a path 
*/
static void proxyGetHostIDPath(char *path, size_t len){
  strlcpy(path, HOSTIDPATH, len);
#ifdef SQLITE_TEST
  if( sqlite3_hostid_num>0 ){
    char suffix[2] = "1";
    suffix[0] = suffix[0] + sqlite3_hostid_num;
    strlcat(path, suffix, len);
  }
#endif
  OSTRACE3("GETHOSTIDPATH  %s pid=%d\n", path, getpid());
}

/* get the host ID from a sqlite hostid file stored in the 
** user-specific tmp directory, create the ID if it's not there already 
*/
static int proxyGetHostID(char *pHostID, int *pError){
  int fd;
  char path[MAXPATHLEN]; 
  size_t len;
  int rc=SQLITE_OK;

  proxyGetHostIDPath(path, MAXPATHLEN);
  /* try to create the host ID file, if it already exists read the contents */
  fd = open(path, O_CREAT|O_WRONLY|O_EXCL, 0644);
  if( fd<0 ){
    int err=errno;
		
    if( err!=EEXIST ){
#ifdef SQLITE_PROXY_DEBUG /* set the sqlite error message instead */
      fprintf(stderr, "sqlite error creating host ID file %s: %s\n",
              path, strerror(err));
#endif
      return SQLITE_PERM;
    }
    /* couldn't create the file, read it instead */
    fd = open(path, O_RDONLY|O_EXCL);
    if( fd<0 ){
#ifdef SQLITE_PROXY_DEBUG /* set the sqlite error message instead */
      int err = errno;
      fprintf(stderr, "sqlite error opening host ID file %s: %s\n",
              path, strerror(err));
#endif
      return SQLITE_PERM;
    }
    len = pread(fd, pHostID, HOSTIDLEN, 0);
    if( len<0 ){
      *pError = errno;
      rc = SQLITE_IOERR_READ;
    }else if( len<HOSTIDLEN ){
      *pError = 0;
      rc = SQLITE_IOERR_SHORT_READ;
    }
    close(fd); /* silently leak the fd if it fails */
    OSTRACE3("GETHOSTID  read %s pid=%d\n", pHostID, getpid());
    return rc;
  }else{
    /* we're creating the host ID file (use a random string of bytes) */
    proxyGenerateHostID(pHostID);
    len = pwrite(fd, pHostID, HOSTIDLEN, 0);
    if( len<0 ){
      *pError = errno;
      rc = SQLITE_IOERR_WRITE;
    }else if( len<HOSTIDLEN ){
      *pError = 0;
      rc = SQLITE_IOERR_WRITE;
    }
    close(fd); /* silently leak the fd if it fails */
    OSTRACE3("GETHOSTID  wrote %s pid=%d\n", pHostID, getpid());
    return rc;
  }
}

static int proxyGetLockPath(const char *dbPath, char *lPath, size_t maxLen){
  int len;
  int dbLen;
  int i;

#ifdef LOCKPROXYDIR
  len = strlcpy(lPath, LOCKPROXYDIR, maxLen);
#else
# ifdef _CS_DARWIN_USER_TEMP_DIR
  {
    confstr(_CS_DARWIN_USER_TEMP_DIR, lPath, maxLen);
    len = strlcat(lPath, "sqliteplocks", maxLen);
    if( mkdir(lPath, SQLITE_DEFAULT_PROXYDIR_PERMISSIONS) ){
      /* if mkdir fails, handle as lock file creation failure */
#  ifdef SQLITE_DEBUG
      int err = errno;
      if( err!=EEXIST ){
        fprintf(stderr, "proxyGetLockPath: mkdir(%s,0%o) error %d %s\n", lPath,
                SQLITE_DEFAULT_PROXYDIR_PERMISSIONS, err, strerror(err));
      }
#  endif
    }else{
      OSTRACE3("GETLOCKPATH  mkdir %s pid=%d\n", lPath, getpid());
    }
    
  }
# else
  len = strlcpy(lPath, "/tmp/", maxLen);
# endif
#endif

  if( lPath[len-1]!='/' ){
    len = strlcat(lPath, "/", maxLen);
  }
  
  /* transform the db path to a unique cache name */
  dbLen = (int)strlen(dbPath);
  for( i=0; i<dbLen && (i+len+7)<maxLen; i++){
    char c = dbPath[i];
    lPath[i+len] = (c=='/')?'_':c;
  }
  lPath[i+len]='\0';
  strlcat(lPath, ":auto:", maxLen);
  return SQLITE_OK;
}

/*
** Create a new VFS file descriptor (stored in memory obtained from
** sqlite3_malloc) and open the file named "path" in the file descriptor.
**
** The caller is responsible not only for closing the file descriptor
** but also for freeing the memory associated with the file descriptor.
*/
static int proxyCreateUnixFile(const char *path, unixFile **ppFile) {
  unixFile *pNew;
  int flags = SQLITE_OPEN_MAIN_DB|SQLITE_OPEN_CREATE|SQLITE_OPEN_READWRITE;
  int rc = SQLITE_OK;
  sqlite3_vfs dummyVfs;

  pNew = (unixFile *)sqlite3_malloc(sizeof(unixFile));
  if( !pNew ){
    return SQLITE_NOMEM;
  }
  memset(pNew, 0, sizeof(unixFile));

  /* Call unixOpen() to open the proxy file. The flags passed to unixOpen()
  ** suggest that the file being opened is a "main database". This is
  ** necessary as other file types do not necessarily support locking. It
  ** is better to use unixOpen() instead of opening the file directly with
  ** open(), as unixOpen() sets up the various mechanisms required to
  ** make sure a call to close() does not cause the system to discard
  ** POSIX locks prematurely.
  **
  ** It is important that the xOpen member of the VFS object passed to 
  ** unixOpen() is NULL. This tells unixOpen() may try to open a proxy-file 
  ** for the proxy-file (creating a potential infinite loop).
  */
  dummyVfs.pAppData = (void*)&autolockIoFinder;
  dummyVfs.xOpen = 0;
  rc = unixOpen(&dummyVfs, path, (sqlite3_file *)pNew, flags, &flags);
  if( rc==SQLITE_OK && (flags&SQLITE_OPEN_READONLY) ){
    pNew->pMethod->xClose((sqlite3_file *)pNew);
    rc = SQLITE_CANTOPEN;
  }

  if( rc!=SQLITE_OK ){
    sqlite3_free(pNew);
    pNew = 0;
  }

  *ppFile = pNew;
  return rc;
}

/* takes the conch by taking a shared lock and read the contents conch, if 
** lockPath is non-NULL, the host ID and lock file path must match.  A NULL 
** lockPath means that the lockPath in the conch file will be used if the 
** host IDs match, or a new lock path will be generated automatically 
** and written to the conch file.
*/
static int proxyTakeConch(unixFile *pFile){
  proxyLockingContext *pCtx = (proxyLockingContext *)pFile->lockingContext; 
  
  if( pCtx->conchHeld>0 ){
    return SQLITE_OK;
  }else{
    unixFile *conchFile = pCtx->conchFile;
    char testValue[CONCHLEN];
    char conchValue[CONCHLEN];
    char lockPath[MAXPATHLEN];
    char *tLockPath = NULL;
    int rc = SQLITE_OK;
    int readRc = SQLITE_OK;
    int syncPerms = 0;

    OSTRACE4("TAKECONCH  %d for %s pid=%d\n", conchFile->h,
             (pCtx->lockProxyPath ? pCtx->lockProxyPath : ":auto:"), getpid());

    rc = conchFile->pMethod->xLock((sqlite3_file*)conchFile, SHARED_LOCK);
    if( rc==SQLITE_OK ){
      int pError = 0;
      memset(testValue, 0, CONCHLEN); /* conch is fixed size */
      rc = proxyGetHostID(testValue, &pError);
      if( (rc&0xff)==SQLITE_IOERR ){
        pFile->lastErrno = pError;
      }
      if( pCtx->lockProxyPath ){
        strlcpy(&testValue[HOSTIDLEN], pCtx->lockProxyPath, MAXPATHLEN);
      }
    }
    if( rc!=SQLITE_OK ){
      goto end_takeconch;
    }
    
    readRc = unixRead((sqlite3_file *)conchFile, conchValue, CONCHLEN, 0);
    if( readRc!=SQLITE_IOERR_SHORT_READ ){
      if( readRc!=SQLITE_OK ){
        if( (rc&0xff)==SQLITE_IOERR ){
          pFile->lastErrno = conchFile->lastErrno;
        }
        rc = readRc;
        goto end_takeconch;
      }
      /* if the conch has data compare the contents */
      if( !pCtx->lockProxyPath ){
        /* for auto-named local lock file, just check the host ID and we'll
         ** use the local lock file path that's already in there */
        if( !memcmp(testValue, conchValue, HOSTIDLEN) ){
          tLockPath = (char *)&conchValue[HOSTIDLEN];
          goto end_takeconch;
        }
      }else{
        /* we've got the conch if conchValue matches our path and host ID */
        if( !memcmp(testValue, conchValue, CONCHLEN) ){
          goto end_takeconch;
        }
      }
    }else{
      /* a short read means we're "creating" the conch (even though it could 
      ** have been user-intervention), if we acquire the exclusive lock,
      ** we'll try to match the current on-disk permissions of the database
      */
      syncPerms = 1;
    }
    
    /* either conch was emtpy or didn't match */
    if( !pCtx->lockProxyPath ){
      proxyGetLockPath(pCtx->dbPath, lockPath, MAXPATHLEN);
      tLockPath = lockPath;
      strlcpy(&testValue[HOSTIDLEN], lockPath, MAXPATHLEN);
    }
    
    /* update conch with host and path (this will fail if other process
     ** has a shared lock already) */
    rc = conchFile->pMethod->xLock((sqlite3_file*)conchFile, EXCLUSIVE_LOCK);
    if( rc==SQLITE_OK ){
      rc = unixWrite((sqlite3_file *)conchFile, testValue, CONCHLEN, 0);
      if( rc==SQLITE_OK && syncPerms ){
        struct stat buf;
        int err = fstat(pFile->h, &buf);
        if( err==0 ){
          /* try to match the database file permissions, ignore failure */
#ifndef SQLITE_PROXY_DEBUG
          fchmod(conchFile->h, buf.st_mode);
#else
          if( fchmod(conchFile->h, buf.st_mode)!=0 ){
            int code = errno;
            fprintf(stderr, "fchmod %o FAILED with %d %s\n",
                             buf.st_mode, code, strerror(code));
          } else {
            fprintf(stderr, "fchmod %o SUCCEDED\n",buf.st_mode);
          }
        }else{
          int code = errno;
          fprintf(stderr, "STAT FAILED[%d] with %d %s\n", 
                          err, code, strerror(code));
#endif
        }
      }
    }
    conchFile->pMethod->xUnlock((sqlite3_file*)conchFile, SHARED_LOCK);
  
end_takeconch:
    OSTRACE2("TRANSPROXY: CLOSE  %d\n", pFile->h);
    if( rc==SQLITE_OK && pFile->openFlags ){
      if( pFile->h>=0 ){
#ifdef STRICT_CLOSE_ERROR
        if( close(pFile->h) ){
          pFile->lastErrno = errno;
          return SQLITE_IOERR_CLOSE;
        }
#else
        close(pFile->h); /* silently leak fd if fail */
#endif
      }
      pFile->h = -1;
      int fd = open(pCtx->dbPath, pFile->openFlags,
                    SQLITE_DEFAULT_FILE_PERMISSIONS);
      OSTRACE2("TRANSPROXY: OPEN  %d\n", fd);
      if( fd>=0 ){
        pFile->h = fd;
      }else{
        rc=SQLITE_CANTOPEN; /* SQLITE_BUSY? proxyTakeConch called
                               during locking */
      }
    }
    if( rc==SQLITE_OK && !pCtx->lockProxy ){
      char *path = tLockPath ? tLockPath : pCtx->lockProxyPath;
      /* ACS: Need to make a copy of path sometimes */
      rc = proxyCreateUnixFile(path, &pCtx->lockProxy);
    }
    if( rc==SQLITE_OK ){
      pCtx->conchHeld = 1;

      if( tLockPath ){
        pCtx->lockProxyPath = sqlite3DbStrDup(0, tLockPath);
        if( pCtx->lockProxy->pMethod == &afpIoMethods ){
          ((afpLockingContext *)pCtx->lockProxy->lockingContext)->dbPath =
                     pCtx->lockProxyPath;
        }
      }
    } else {
      conchFile->pMethod->xUnlock((sqlite3_file*)conchFile, NO_LOCK);
    }
    OSTRACE3("TAKECONCH  %d %s\n", conchFile->h, rc==SQLITE_OK?"ok":"failed");
    return rc;
  }
}

/*
** If pFile holds a lock on a conch file, then release that lock.
*/
static int proxyReleaseConch(unixFile *pFile){
  int rc;                     /* Subroutine return code */
  proxyLockingContext *pCtx;  /* The locking context for the proxy lock */
  unixFile *conchFile;        /* Name of the conch file */

  pCtx = (proxyLockingContext *)pFile->lockingContext;
  conchFile = pCtx->conchFile;
  OSTRACE4("RELEASECONCH  %d for %s pid=%d\n", conchFile->h,
           (pCtx->lockProxyPath ? pCtx->lockProxyPath : ":auto:"), 
           getpid());
  pCtx->conchHeld = 0;
  rc = conchFile->pMethod->xUnlock((sqlite3_file*)conchFile, NO_LOCK);
  OSTRACE3("RELEASECONCH  %d %s\n", conchFile->h,
           (rc==SQLITE_OK ? "ok" : "failed"));
  return rc;
}

/*
** Given the name of a database file, compute the name of its conch file.
** Store the conch filename in memory obtained from sqlite3_malloc().
** Make *pConchPath point to the new name.  Return SQLITE_OK on success
** or SQLITE_NOMEM if unable to obtain memory.
**
** The caller is responsible for ensuring that the allocated memory
** space is eventually freed.
**
** *pConchPath is set to NULL if a memory allocation error occurs.
*/
static int proxyCreateConchPathname(char *dbPath, char **pConchPath){
  int i;                        /* Loop counter */
  int len = (int)strlen(dbPath); /* Length of database filename - dbPath */
  char *conchPath;              /* buffer in which to construct conch name */

  /* Allocate space for the conch filename and initialize the name to
  ** the name of the original database file. */  
  *pConchPath = conchPath = (char *)sqlite3_malloc(len + 8);
  if( conchPath==0 ){
    return SQLITE_NOMEM;
  }
  memcpy(conchPath, dbPath, len+1);
  
  /* now insert a "." before the last / character */
  for( i=(len-1); i>=0; i-- ){
    if( conchPath[i]=='/' ){
      i++;
      break;
    }
  }
  conchPath[i]='.';
  while ( i<len ){
    conchPath[i+1]=dbPath[i];
    i++;
  }

  /* append the "-conch" suffix to the file */
  memcpy(&conchPath[i+1], "-conch", 7);
  assert( (int)strlen(conchPath) == len+7 );

  return SQLITE_OK;
}


/* Takes a fully configured proxy locking-style unix file and switches
** the local lock file path 
*/
static int switchLockProxyPath(unixFile *pFile, const char *path) {
  proxyLockingContext *pCtx = (proxyLockingContext*)pFile->lockingContext;
  char *oldPath = pCtx->lockProxyPath;
  int rc = SQLITE_OK;

  if( pFile->locktype!=NO_LOCK ){
    return SQLITE_BUSY;
  }  

  /* nothing to do if the path is NULL, :auto: or matches the existing path */
  if( !path || path[0]=='\0' || !strcmp(path, ":auto:") ||
    (oldPath && !strncmp(oldPath, path, MAXPATHLEN)) ){
    return SQLITE_OK;
  }else{
    unixFile *lockProxy = pCtx->lockProxy;
    pCtx->lockProxy=NULL;
    pCtx->conchHeld = 0;
    if( lockProxy!=NULL ){
      rc=lockProxy->pMethod->xClose((sqlite3_file *)lockProxy);
      if( rc ) return rc;
      sqlite3_free(lockProxy);
    }
    sqlite3_free(oldPath);
    pCtx->lockProxyPath = sqlite3DbStrDup(0, path);
  }
  
  return rc;
}

/*
** pFile is a file that has been opened by a prior xOpen call.  dbPath
** is a string buffer at least MAXPATHLEN+1 characters in size.
**
** This routine find the filename associated with pFile and writes it
** int dbPath.
*/
static int proxyGetDbPathForUnixFile(unixFile *pFile, char *dbPath){
#if defined(__APPLE__)
  if( pFile->pMethod == &afpIoMethods ){
    /* afp style keeps a reference to the db path in the filePath field 
    ** of the struct */
    assert( (int)strlen((char*)pFile->lockingContext)<=MAXPATHLEN );
    strcpy(dbPath, ((afpLockingContext *)pFile->lockingContext)->dbPath);
  }else
#endif
  if( pFile->pMethod == &dotlockIoMethods ){
    /* dot lock style uses the locking context to store the dot lock
    ** file path */
    int len = strlen((char *)pFile->lockingContext) - strlen(DOTLOCK_SUFFIX);
    memcpy(dbPath, (char *)pFile->lockingContext, len + 1);
  }else{
    /* all other styles use the locking context to store the db file path */
    assert( strlen((char*)pFile->lockingContext)<=MAXPATHLEN );
    strcpy(dbPath, (char *)pFile->lockingContext);
  }
  return SQLITE_OK;
}

/*
** Takes an already filled in unix file and alters it so all file locking 
** will be performed on the local proxy lock file.  The following fields
** are preserved in the locking context so that they can be restored and 
** the unix structure properly cleaned up at close time:
**  ->lockingContext
**  ->pMethod
*/
static int proxyTransformUnixFile(unixFile *pFile, const char *path) {
  proxyLockingContext *pCtx;
  char dbPath[MAXPATHLEN+1];       /* Name of the database file */
  char *lockPath=NULL;
  int rc = SQLITE_OK;
  
  if( pFile->locktype!=NO_LOCK ){
    return SQLITE_BUSY;
  }
  proxyGetDbPathForUnixFile(pFile, dbPath);
  if( !path || path[0]=='\0' || !strcmp(path, ":auto:") ){
    lockPath=NULL;
  }else{
    lockPath=(char *)path;
  }
  
  OSTRACE4("TRANSPROXY  %d for %s pid=%d\n", pFile->h,
           (lockPath ? lockPath : ":auto:"), getpid());

  pCtx = sqlite3_malloc( sizeof(*pCtx) );
  if( pCtx==0 ){
    return SQLITE_NOMEM;
  }
  memset(pCtx, 0, sizeof(*pCtx));

  rc = proxyCreateConchPathname(dbPath, &pCtx->conchFilePath);
  if( rc==SQLITE_OK ){
    rc = proxyCreateUnixFile(pCtx->conchFilePath, &pCtx->conchFile);
  }  
  if( rc==SQLITE_OK && lockPath ){
    pCtx->lockProxyPath = sqlite3DbStrDup(0, lockPath);
  }

  if( rc==SQLITE_OK ){
    /* all memory is allocated, proxys are created and assigned, 
    ** switch the locking context and pMethod then return.
    */
    pCtx->dbPath = sqlite3DbStrDup(0, dbPath);
    pCtx->oldLockingContext = pFile->lockingContext;
    pFile->lockingContext = pCtx;
    pCtx->pOldMethod = pFile->pMethod;
    pFile->pMethod = &proxyIoMethods;
  }else{
    if( pCtx->conchFile ){ 
      rc = pCtx->conchFile->pMethod->xClose((sqlite3_file *)pCtx->conchFile);
      if( rc ) return rc;
      sqlite3_free(pCtx->conchFile);
    }
    sqlite3_free(pCtx->conchFilePath); 
    sqlite3_free(pCtx);
  }
  OSTRACE3("TRANSPROXY  %d %s\n", pFile->h,
           (rc==SQLITE_OK ? "ok" : "failed"));
  return rc;
}


/*
** This routine handles sqlite3_file_control() calls that are specific
** to proxy locking.
*/
static int proxyFileControl(sqlite3_file *id, int op, void *pArg){
  switch( op ){
    case SQLITE_GET_LOCKPROXYFILE: {
      unixFile *pFile = (unixFile*)id;
      if( pFile->pMethod == &proxyIoMethods ){
        proxyLockingContext *pCtx = (proxyLockingContext*)pFile->lockingContext;
        proxyTakeConch(pFile);
        if( pCtx->lockProxyPath ){
          *(const char **)pArg = pCtx->lockProxyPath;
        }else{
          *(const char **)pArg = ":auto: (not held)";
        }
      } else {
        *(const char **)pArg = NULL;
      }
      return SQLITE_OK;
    }
    case SQLITE_SET_LOCKPROXYFILE: {
      unixFile *pFile = (unixFile*)id;
      int rc = SQLITE_OK;
      int isProxyStyle = (pFile->pMethod == &proxyIoMethods);
      if( pArg==NULL || (const char *)pArg==0 ){
        if( isProxyStyle ){
          /* turn off proxy locking - not supported */
          rc = SQLITE_ERROR /*SQLITE_PROTOCOL? SQLITE_MISUSE?*/;
        }else{
          /* turn off proxy locking - already off - NOOP */
          rc = SQLITE_OK;
        }
      }else{
        const char *proxyPath = (const char *)pArg;
        if( isProxyStyle ){
          proxyLockingContext *pCtx = 
            (proxyLockingContext*)pFile->lockingContext;
          if( !strcmp(pArg, ":auto:") 
           || (pCtx->lockProxyPath &&
               !strncmp(pCtx->lockProxyPath, proxyPath, MAXPATHLEN))
          ){
            rc = SQLITE_OK;
          }else{
            rc = switchLockProxyPath(pFile, proxyPath);
          }
        }else{
          /* turn on proxy file locking */
          rc = proxyTransformUnixFile(pFile, proxyPath);
        }
      }
      return rc;
    }
    default: {
      assert( 0 );  /* The call assures that only valid opcodes are sent */
    }
  }
  /*NOTREACHED*/
  return SQLITE_ERROR;
}

/*
** Within this division (the proxying locking implementation) the procedures
** above this point are all utilities.  The lock-related methods of the
** proxy-locking sqlite3_io_method object follow.
*/


/*
** This routine checks if there is a RESERVED lock held on the specified
** file by this or any other process. If such a lock is held, set *pResOut
** to a non-zero value otherwise *pResOut is set to zero.  The return value
** is set to SQLITE_OK unless an I/O error occurs during lock checking.
*/
static int proxyCheckReservedLock(sqlite3_file *id, int *pResOut) {
  unixFile *pFile = (unixFile*)id;
  int rc = proxyTakeConch(pFile);
  if( rc==SQLITE_OK ){
    proxyLockingContext *pCtx = (proxyLockingContext *)pFile->lockingContext;
    unixFile *proxy = pCtx->lockProxy;
    return proxy->pMethod->xCheckReservedLock((sqlite3_file*)proxy, pResOut);
  }
  return rc;
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
** This routine will only increase a lock.  Use the sqlite3OsUnlock()
** routine to lower a locking level.
*/
static int proxyLock(sqlite3_file *id, int locktype) {
  unixFile *pFile = (unixFile*)id;
  int rc = proxyTakeConch(pFile);
  if( rc==SQLITE_OK ){
    proxyLockingContext *pCtx = (proxyLockingContext *)pFile->lockingContext;
    unixFile *proxy = pCtx->lockProxy;
    rc = proxy->pMethod->xLock((sqlite3_file*)proxy, locktype);
    pFile->locktype = proxy->locktype;
  }
  return rc;
}


/*
** Lower the locking level on file descriptor pFile to locktype.  locktype
** must be either NO_LOCK or SHARED_LOCK.
**
** If the locking level of the file descriptor is already at or below
** the requested locking level, this routine is a no-op.
*/
static int proxyUnlock(sqlite3_file *id, int locktype) {
  unixFile *pFile = (unixFile*)id;
  int rc = proxyTakeConch(pFile);
  if( rc==SQLITE_OK ){
    proxyLockingContext *pCtx = (proxyLockingContext *)pFile->lockingContext;
    unixFile *proxy = pCtx->lockProxy;
    rc = proxy->pMethod->xUnlock((sqlite3_file*)proxy, locktype);
    pFile->locktype = proxy->locktype;
  }
  return rc;
}

/*
** Close a file that uses proxy locks.
*/
static int proxyClose(sqlite3_file *id) {
  if( id ){
    unixFile *pFile = (unixFile*)id;
    proxyLockingContext *pCtx = (proxyLockingContext *)pFile->lockingContext;
    unixFile *lockProxy = pCtx->lockProxy;
    unixFile *conchFile = pCtx->conchFile;
    int rc = SQLITE_OK;
    
    if( lockProxy ){
      rc = lockProxy->pMethod->xUnlock((sqlite3_file*)lockProxy, NO_LOCK);
      if( rc ) return rc;
      rc = lockProxy->pMethod->xClose((sqlite3_file*)lockProxy);
      if( rc ) return rc;
      sqlite3_free(lockProxy);
      pCtx->lockProxy = 0;
    }
    if( conchFile ){
      if( pCtx->conchHeld ){
        rc = proxyReleaseConch(pFile);
        if( rc ) return rc;
      }
      rc = conchFile->pMethod->xClose((sqlite3_file*)conchFile);
      if( rc ) return rc;
      sqlite3_free(conchFile);
    }
    sqlite3_free(pCtx->lockProxyPath);
    sqlite3_free(pCtx->conchFilePath);
    sqlite3_free(pCtx->dbPath);
    /* restore the original locking context and pMethod then close it */
    pFile->lockingContext = pCtx->oldLockingContext;
    pFile->pMethod = pCtx->pOldMethod;
    sqlite3_free(pCtx);
    return pFile->pMethod->xClose(id);
  }
  return SQLITE_OK;
}



#endif /* defined(__APPLE__) && SQLITE_ENABLE_LOCKING_STYLE */
/*
** The proxy locking style is intended for use with AFP filesystems.
** And since AFP is only supported on MacOSX, the proxy locking is also
** restricted to MacOSX.
** 
**
******************* End of the proxy lock implementation **********************
******************************************************************************/

/*
** Initialize the operating system interface.
**
** This routine registers all VFS implementations for unix-like operating
** systems.  This routine, and the sqlite3_os_end() routine that follows,
** should be the only routines in this file that are visible from other
** files.
**
** This routine is called once during SQLite initialization and by a
** single thread.  The memory allocation and mutex subsystems have not
** necessarily been initialized when this routine is called, and so they
** should not be used.
*/
int sqlite3_os_init(void){ 
  /* 
  ** The following macro defines an initializer for an sqlite3_vfs object.
  ** The name of the VFS is NAME.  The pAppData is a pointer to a pointer
  ** to the "finder" function.  (pAppData is a pointer to a pointer because
  ** silly C90 rules prohibit a void* from being cast to a function pointer
  ** and so we have to go through the intermediate pointer to avoid problems
  ** when compiling with -pedantic-errors on GCC.)
  **
  ** The FINDER parameter to this macro is the name of the pointer to the
  ** finder-function.  The finder-function returns a pointer to the
  ** sqlite_io_methods object that implements the desired locking
  ** behaviors.  See the division above that contains the IOMETHODS
  ** macro for addition information on finder-functions.
  **
  ** Most finders simply return a pointer to a fixed sqlite3_io_methods
  ** object.  But the "autolockIoFinder" available on MacOSX does a little
  ** more than that; it looks at the filesystem type that hosts the 
  ** database file and tries to choose an locking method appropriate for
  ** that filesystem time.
  */
  #define UNIXVFS(VFSNAME, FINDER) {                        \
    1,                    /* iVersion */                    \
    sizeof(unixFile),     /* szOsFile */                    \
    MAX_PATHNAME,         /* mxPathname */                  \
    0,                    /* pNext */                       \
    VFSNAME,              /* zName */                       \
    (void*)&FINDER,       /* pAppData */                    \
    unixOpen,             /* xOpen */                       \
    unixDelete,           /* xDelete */                     \
    unixAccess,           /* xAccess */                     \
    unixFullPathname,     /* xFullPathname */               \
    unixDlOpen,           /* xDlOpen */                     \
    unixDlError,          /* xDlError */                    \
    unixDlSym,            /* xDlSym */                      \
    unixDlClose,          /* xDlClose */                    \
    unixRandomness,       /* xRandomness */                 \
    unixSleep,            /* xSleep */                      \
    unixCurrentTime,      /* xCurrentTime */                \
    unixGetLastError      /* xGetLastError */               \
  }

  /*
  ** All default VFSes for unix are contained in the following array.
  **
  ** Note that the sqlite3_vfs.pNext field of the VFS object is modified
  ** by the SQLite core when the VFS is registered.  So the following
  ** array cannot be const.
  */
  static sqlite3_vfs aVfs[] = {
#if SQLITE_ENABLE_LOCKING_STYLE && (OS_VXWORKS || defined(__APPLE__))
    UNIXVFS("unix",          autolockIoFinder ),
#else
    UNIXVFS("unix",          posixIoFinder ),
#endif
    UNIXVFS("unix-none",     nolockIoFinder ),
    UNIXVFS("unix-dotfile",  dotlockIoFinder ),
    UNIXVFS("unix-wfl",      posixWflIoFinder ),
#if OS_VXWORKS
    UNIXVFS("unix-namedsem", semIoFinder ),
#endif
#if SQLITE_ENABLE_LOCKING_STYLE
    UNIXVFS("unix-posix",    posixIoFinder ),
#if !OS_VXWORKS
    UNIXVFS("unix-flock",    flockIoFinder ),
#endif
#endif
#if SQLITE_ENABLE_LOCKING_STYLE && defined(__APPLE__)
    UNIXVFS("unix-afp",      afpIoFinder ),
    UNIXVFS("unix-proxy",    proxyIoFinder ),
#endif
  };
  unsigned int i;          /* Loop counter */

  /* Register all VFSes defined in the aVfs[] array */
  for(i=0; i<(sizeof(aVfs)/sizeof(sqlite3_vfs)); i++){
    sqlite3_vfs_register(&aVfs[i], i==0);
  }
  return SQLITE_OK; 
}

/*
** Shutdown the operating system interface.
**
** Some operating systems might need to do some cleanup in this routine,
** to release dynamically allocated objects.  But not on unix.
** This routine is a no-op for unix.
*/
int sqlite3_os_end(void){ 
  return SQLITE_OK; 
}
 
#endif /* SQLITE_OS_UNIX */

/*
** 2005 December 14
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
*************************************************************************
**
** This file contains an example implementation of an asynchronous IO 
** backend for SQLite. It is used to test that the concept of asynchronous 
** IO in SQLite is valid.
*/

#include "sqliteInt.h"
#include "os.h"
#include <tcl.h>

#ifndef THREADSAFE
# define THREADSAFE 0
#endif

/*
** This test uses pthreads and hence only works on unix and with
** a threadsafe build of SQLite.
*/
#if OS_UNIX && THREADSAFE && defined(SQLITE_ENABLE_REDEF_IO)

#include <pthread.h>
#include <sched.h>

#define MIN(x,y) ((x)<(y)?(x):(y))
#define MAX(x,y) ((x)>(y)?(x):(y))

typedef struct AsyncWrite AsyncWrite;
typedef struct AsyncFile AsyncFile;

#if 0
# define TRACE(X,Y) \
    fprintf(stderr,"THRD=%d: ", (int)pthread_self()); \
    fprintf(stderr,X,Y);
#else
# define TRACE(X,Y) /* noop */
#endif

/*
** TODO:
**     * File locks...
*/

/*
** THREAD SAFETY NOTES
**
** Basic rules:
**
**     * Both read and write access to the global write-op queue must be 
**       protected by the async.queueMutex.
**
**     * The file handles from the underlying system are assumed not to 
**       be thread safe.
**
**     * See the last paragraph under "sqlite3_async_flush() Threads" for
**       an assumption to do with file-handle synchronization by the Os.
**
** File system operations (invoked by SQLite thread):
**
**     xOpenXXX (three versions)
**     xDelete
**     xFileExists
**     xSyncDirectory
**
** File handle operations (invoked by SQLite thread):
**
**     The following operations add an entry to the global write-op list. They
**     prepare the entry, acquire the mutex momentarily while list pointers are 
**     manipulated to insert the new entry, and release the mutex.
**    
**         asyncWrite, asyncClose, asyncTruncate, asyncSync, 
**         asyncSetFullSync, asyncOpenDirectory.
**    
**     Read operations. Both of these read from both the underlying file and
**     the write-op list. So we grab the mutex for the whole call (even 
**     while performing a blocking read on the file).
**    
**         asyncRead, asyncFileSize.
**    
**     These locking primitives become no-ops. Files are always opened for 
**     exclusive access when using this IO backend:
**    
**         asyncLock, asyncUnlock, asyncLockState, asyncCheckReservedLock
**    
**     The sqlite3OsFileHandle() function is currently only used when 
**     debugging the pager module. Unless sqlite3OsClose() is called on the
**     file (shouldn't be possible for other reasons), the underlying 
**     implementations are safe to call without grabbing any mutex. So we just
**     go ahead and call it no matter what any other thread is doing.
**
**         asyncFileHandle.
**
**     Calling this method just manipulates the AsyncFile.iOffset variable. 
**     Since this variable is never accessed by writer thread, this
**     function does not require the mutex.  Actual calls to OsSeek() take 
**     place just before OsWrite() or OsRead(), which are always protected by 
**     the mutex.
**    
**         asyncSeek.
**
** The writer thread:
**
**     The async.writerMutex is used to make sure only there is only
**     a single writer thread running at a time.
**
**     Inside the writer thread is a loop that works like this:
**
**         WHILE (write-op list is not empty)
**             Do IO operation at head of write-op list
**             Remove entry from head of write-op list
**         END WHILE
**
**     The async.queueMutex is always held during the <write-op list is 
**     not empty> test, and when the entry is removed from the head
**     of the write-op list. Sometimes it is held for the interim
**     period (while the IO is  performed), and sometimes it is
**     relinquished. It is relinquished if (a) the IO op is an
**     ASYNC_CLOSE or (b) when the file handle was opened, two of
**     the underlying systems handles were opened on the same
**     file-system entry.
**
**     If condition (b) above is true, then one file-handle 
**     (AsyncFile.pBaseRead) is used exclusively by sqlite threads to read the
**     file, the other (AsyncFile.pBaseWrite) by sqlite3_async_flush() 
**     threads to perform write() operations. This means that read 
**     operations are not blocked by asynchronous writes (although 
**     asynchronous writes may still be blocked by reads).
**
**     This assumes that the OS keeps two handles open on the same file
**     properly in sync. That is, any read operation that starts after a
**     write operation on the same file system entry has completed returns
**     data consistent with the write. We also assume that if one thread 
**     reads a file while another is writing it all bytes other than the
**     ones actually being written contain valid data.
**
**     If the above assumptions are not true, set the preprocessor symbol
**     SQLITE_ASYNC_TWO_FILEHANDLES to 0.
*/

#ifndef SQLITE_ASYNC_TWO_FILEHANDLES
/* #define SQLITE_ASYNC_TWO_FILEHANDLES 0 */
#define SQLITE_ASYNC_TWO_FILEHANDLES 1
#endif

/*
** State information is held in the static variable "async" defined
** as follows:
*/
static struct TestAsyncStaticData {
  pthread_mutex_t queueMutex;  /* Mutex for access to write operation queue */
  pthread_mutex_t writerMutex; /* Prevents multiple writer threads */
  pthread_cond_t queueSignal;  /* For waking up sleeping writer thread */
  pthread_cond_t emptySignal;  /* Notify when the write queue is empty */
  AsyncWrite *pQueueFirst;     /* Next write operation to be processed */
  AsyncWrite *pQueueLast;      /* Last write operation on the list */
  volatile int ioDelay;             /* Extra delay between write operations */
  volatile int writerHaltWhenIdle;  /* Writer thread halts when queue empty */
  volatile int writerHaltNow;       /* Writer thread halts after next op */
} async = {
  PTHREAD_MUTEX_INITIALIZER,
  PTHREAD_MUTEX_INITIALIZER,
  PTHREAD_COND_INITIALIZER,
  PTHREAD_COND_INITIALIZER,
};

/* Possible values of AsyncWrite.op */
#define ASYNC_WRITE         1
#define ASYNC_SYNC          2
#define ASYNC_TRUNCATE      3
#define ASYNC_CLOSE         4
#define ASYNC_OPENDIRECTORY 5
#define ASYNC_SETFULLSYNC   6

#define ASYNC_DELETE        7
#define ASYNC_OPENEXCLUSIVE 8
#define ASYNC_SYNCDIRECTORY 9

/*
** The interpretation of the iOffset and nByte variables varies depending 
** on the value of AsyncWrite.op:
**
** ASYNC_WRITE:
**     iOffset -> Offset in file to write to.
**     nByte   -> Number of bytes of data to write (pointed to by zBuf).
**
** ASYNC_SYNC:
**     iOffset -> Unused.
**     nByte   -> Value of "fullsync" flag to pass to sqlite3OsSync().
**
** ASYNC_TRUNCATE:
**     iOffset -> Size to truncate file to.
**     nByte   -> Unused.
**
** ASYNC_CLOSE:
**     iOffset -> Unused.
**     nByte   -> Unused.
**
** ASYNC_OPENDIRECTORY:
**     iOffset -> Unused.
**     nByte   -> Number of bytes of zBuf points to (directory name).
**
** ASYNC_SETFULLSYNC:
**     iOffset -> Unused.
**     nByte   -> New value for the full-sync flag.
**
**
** ASYNC_DELETE:
**     iOffset -> Unused.
**     nByte   -> Number of bytes of zBuf points to (file name).
**
** ASYNC_OPENEXCLUSIVE:
**     iOffset -> Value of "delflag".
**     nByte   -> Number of bytes of zBuf points to (file name).
**
** For an ASYNC_WRITE operation, zBuf points to the data to write to the file. 
** This space is sqliteMalloc()d along with the AsyncWrite structure in a
** single blob, so is deleted when sqliteFree() is called on the parent 
** structure.
*/
struct AsyncWrite {
  AsyncFile *pFile;   /* File to write data to or sync */
  int op;             /* One of ASYNC_xxx etc. */
  i64 iOffset;        /* See above */
  int nByte;          /* See above */
  char *zBuf;         /* Data to write to file (or NULL if op!=ASYNC_WRITE) */
  AsyncWrite *pNext;  /* Next write operation (to any file) */
};

/* 
** The AsyncFile structure is a subclass of OsFile used for asynchronous IO.
*/
struct AsyncFile {
  IoMethod *pMethod;   /* Must be first */
  i64 iOffset;         /* Current seek() offset in file */
  OsFile *pBaseRead;   /* Read handle to the underlying Os file */
  OsFile *pBaseWrite;  /* Write handle to the underlying Os file */
};

/*
** Add an entry to the end of the global write-op list. pWrite should point 
** to an AsyncWrite structure allocated using sqliteMalloc(). A future call 
** to sqlite3_async_flush() is responsible for calling sqliteFree().
**
** Once an AsyncWrite structure has been added to the list, it must not be
** read or modified by the caller (in case another thread calls
** sqlite3_async_flush() ).
*/
static void addAsyncWrite(AsyncWrite *pWrite){
  pthread_mutex_lock(&async.queueMutex);
  assert( !pWrite->pNext );
  if( async.pQueueLast ){
    assert( async.pQueueFirst );
    async.pQueueLast->pNext = pWrite;
  }else{
    async.pQueueFirst = pWrite;
  }
  async.pQueueLast = pWrite;
  TRACE("PUSH %p\n", pWrite);
  pthread_mutex_unlock(&async.queueMutex);
  pthread_cond_signal(&async.queueSignal);
}

/*
** This is a utility function to allocate and populate a new AsyncWrite
** structure and insert it (via addAsyncWrite() ) into the global list.
*/
static int addNewAsyncWrite(
  AsyncFile *pFile, 
  int op, 
  i64 iOffset, 
  int nByte,
  const char *zByte
){
  AsyncWrite *p = sqlite3OsMalloc(sizeof(AsyncWrite) + (zByte?nByte:0));
  if( !p ){
    return SQLITE_NOMEM;
  }
  p->op = op;
  p->iOffset = iOffset;
  p->nByte = nByte;
  p->pFile = pFile;
  p->pNext = 0;
  if( zByte ){
    p->zBuf = (char *)&p[1];
    memcpy(p->zBuf, zByte, nByte);
  }else{
    p->zBuf = 0;
  }
  addAsyncWrite(p);
  return SQLITE_OK;
}

/*
** Close the file. This just adds an entry to the write-op list, the file is
** not actually closed.
*/
static int asyncClose(OsFile **pId){
  return addNewAsyncWrite((AsyncFile *)*pId, ASYNC_CLOSE, 0, 0, 0);
}

/*
** Implementation of sqlite3OsWrite() for asynchronous files. Instead of 
** writing to the underlying file, this function adds an entry to the end of
** the global AsyncWrite list. Either SQLITE_OK or SQLITE_NOMEM may be
** returned.
*/
static int asyncWrite(OsFile *id, const void *pBuf, int amt){
  AsyncFile *pFile = (AsyncFile *)id;
  int rc = addNewAsyncWrite(pFile, ASYNC_WRITE, pFile->iOffset, amt, pBuf);
  pFile->iOffset += (i64)amt;
  return rc;
}

/*
** Truncate the file to nByte bytes in length. This just adds an entry to 
** the write-op list, no IO actually takes place.
*/
static int asyncTruncate(OsFile *id, i64 nByte){
  return addNewAsyncWrite((AsyncFile *)id, ASYNC_TRUNCATE, nByte, 0, 0);
}

/*
** Open the directory identified by zName and associate it with the 
** specified file. This just adds an entry to the write-op list, the 
** directory is opened later by sqlite3_async_flush().
*/
static int asyncOpenDirectory(OsFile *id, const char *zName){
  AsyncFile *pFile = (AsyncFile *)id;
  return addNewAsyncWrite(pFile, ASYNC_OPENDIRECTORY, 0, strlen(zName)+1,zName);
}

/*
** Sync the file. This just adds an entry to the write-op list, the 
** sync() is done later by sqlite3_async_flush().
*/
static int asyncSync(OsFile *id, int fullsync){
  return addNewAsyncWrite((AsyncFile *)id, ASYNC_SYNC, 0, fullsync, 0);
}

/*
** Set (or clear) the full-sync flag on the underlying file. This operation
** is queued and performed later by sqlite3_async_flush().
*/
static void asyncSetFullSync(OsFile *id, int value){
  addNewAsyncWrite((AsyncFile *)id, ASYNC_SETFULLSYNC, 0, value, 0);
}

/*
** Read data from the file. First we read from the filesystem, then adjust 
** the contents of the buffer based on ASYNC_WRITE operations in the 
** write-op queue. Todo: Do we need to think about ASYNC_TRUNCATE in 
** this method as well?
**
** This method holds the mutex from start to finish.
*/
static int asyncRead(OsFile *id, void *obuf, int amt){
  int rc = SQLITE_OK;
  i64 filesize;
  int nRead;
  AsyncFile *pFile = (AsyncFile *)id;

  /* Grab the write queue mutex for the duration of the call */
  pthread_mutex_lock(&async.queueMutex);

  if( pFile->pBaseRead ){
    rc = sqlite3OsFileSize(pFile->pBaseRead, &filesize);
    if( rc!=SQLITE_OK ){
      goto asyncread_out;
    }
    rc = sqlite3OsSeek(pFile->pBaseRead, pFile->iOffset);
    if( rc!=SQLITE_OK ){
      goto asyncread_out;
    }
    nRead = MIN(filesize - pFile->iOffset, amt);
    if( nRead>0 ){
      rc = sqlite3OsRead(((AsyncFile *)id)->pBaseRead, obuf, nRead);
    }
  }

  if( rc==SQLITE_OK ){
    AsyncWrite *p;
    i64 iOffset = pFile->iOffset;           /* Current seek offset */

    for(p=async.pQueueFirst; p; p = p->pNext){
      if( p->pFile==pFile && p->op==ASYNC_WRITE ){
        int iBeginIn = (p->iOffset - iOffset);
        int iBeginOut = (iOffset - p->iOffset);
        int nCopy;

        if( iBeginIn<0 ) iBeginIn = 0;
        if( iBeginOut<0 ) iBeginOut = 0;
        nCopy = MIN(p->nByte-iBeginIn, amt-iBeginOut);

        if( nCopy>0 ){
          memcpy(&((char *)obuf)[iBeginOut], &p->zBuf[iBeginIn], nCopy);
        }
      }
    }

    pFile->iOffset += (i64)amt;
  }

asyncread_out:
  pthread_mutex_unlock(&async.queueMutex);
  return rc;
}

/*
** Seek to the specified offset. This just adjusts the AsyncFile.iOffset 
** variable - calling seek() on the underlying file is defered until the 
** next read() or write() operation. 
*/
static int asyncSeek(OsFile *id, i64 offset){
  AsyncFile *pFile = (AsyncFile *)id;
  pFile->iOffset = offset;
  return SQLITE_OK;
}

/*
** Read the size of the file. First we read the size of the file system 
** entry, then adjust for any ASYNC_WRITE or ASYNC_TRUNCATE operations 
** currently in the write-op list. 
**
** This method holds the mutex from start to finish.
*/
int asyncFileSize(OsFile *id, i64 *pSize){
  int rc = SQLITE_OK;
  i64 s = 0;
  OsFile *pBase;

  pthread_mutex_lock(&async.queueMutex);

  /* Read the filesystem size from the base file. If pBaseRead is NULL, this
  ** means the file hasn't been opened yet. In this case all relevant data 
  ** must be in the write-op queue anyway, so we can omit reading from the
  ** file-system.
  */
  pBase = ((AsyncFile *)id)->pBaseRead;
  if( pBase ){
    rc = sqlite3OsFileSize(pBase, &s);
  }

  if( rc==SQLITE_OK ){
    AsyncWrite *p;
    for(p=async.pQueueFirst; p; p = p->pNext){
      if( p->pFile==(AsyncFile *)id ){
        switch( p->op ){
          case ASYNC_WRITE:
            s = MAX(p->iOffset + (i64)(p->nByte), s);
            break;
          case ASYNC_TRUNCATE:
            s = MIN(s, p->nByte);
            break;
        }
      }
    }
    *pSize = s;
  }
  pthread_mutex_unlock(&async.queueMutex);
  return rc;
}

/*
** Return the operating system file handle. This is only used for debugging 
** at the moment anyway.
*/
static int asyncFileHandle(OsFile *id){
  return sqlite3OsFileHandle(((AsyncFile *)id)->pBaseRead);
}

static int asyncLock(OsFile *id, int lockType){
  return SQLITE_OK;
}
static int asyncUnlock(OsFile *id, int lockType){
  return SQLITE_OK;
}

/*
** This function is called when the pager layer first opens a database file
** and is checking for a hot-journal.
*/
static int asyncCheckReservedLock(OsFile *id){
  return SQLITE_OK;
}

/* 
** This is broken. But sqlite3OsLockState() is only used for testing anyway.
*/
static int asyncLockState(OsFile *id){
  return SQLITE_OK;
}

/*
** The following variables hold pointers to the original versions of
** certain OS-layer interface routines - routines that this module
** overrides.
*/
static int (*xOrigOpenReadWrite)(const char*, OsFile**, int*) = 0;
static int (*xOrigOpenExclusive)(const char*, OsFile**, int) = 0;
static int (*xOrigOpenReadOnly)(const char*, OsFile**) = 0;
static int (*xOrigDelete)(const char*) = 0;
static int (*xOrigFileExists)(const char*) = 0;
static int (*xOrigSyncDirectory)(const char*) = 0;


static int asyncOpenFile(
  const char *zName, 
  OsFile **pFile, 
  OsFile *pBaseRead,
  int openSecondFile
){
  int rc;
  AsyncFile *p;
  OsFile *pBaseWrite = 0;

  static IoMethod iomethod = {
    asyncClose,
    asyncOpenDirectory,
    asyncRead,
    asyncWrite,
    asyncSeek,
    asyncTruncate,
    asyncSync,
    asyncSetFullSync,
    asyncFileHandle,
    asyncFileSize,
    asyncLock,
    asyncUnlock,
    asyncLockState,
    asyncCheckReservedLock
  };

  if( openSecondFile && SQLITE_ASYNC_TWO_FILEHANDLES ){
    int dummy;
    rc = xOrigOpenReadWrite(zName, &pBaseWrite, &dummy);
    if( rc!=SQLITE_OK ){
      goto error_out;
    }
  }

  p = (AsyncFile *)sqlite3OsMalloc(sizeof(AsyncFile));
  if( !p ){
    rc = SQLITE_NOMEM;
    goto error_out;
  }
  memset(p, 0, sizeof(AsyncFile));
  
  p->pMethod = &iomethod;
  p->pBaseRead = pBaseRead;
  p->pBaseWrite = pBaseWrite;
  
  *pFile = (OsFile *)p;
  return SQLITE_OK;

error_out:
  assert(!p);
  sqlite3OsClose(&pBaseRead);
  sqlite3OsClose(&pBaseWrite);
  *pFile = 0;
  return rc;
}

/*
** The async-IO backends implementation of the three functions used to open
** a file (xOpenExclusive, xOpenReadWrite and xOpenReadOnly). Most of the 
** work is done in function asyncOpenFile() - see above.
*/
static int asyncOpenExclusive(const char *z, OsFile **ppFile, int delFlag){
  int rc = asyncOpenFile(z, ppFile, 0, 0);
  if( rc==SQLITE_OK ){
    AsyncFile *pFile = (AsyncFile *)(*ppFile);
    int nByte = strlen(z)+1;
    i64 i = (i64)(delFlag);
    rc = addNewAsyncWrite(pFile, ASYNC_OPENEXCLUSIVE, i, nByte, z);
    if( rc!=SQLITE_OK ){
      sqlite3OsFree(pFile);
      *ppFile = 0;
    }
  }
  return rc;
}
static int asyncOpenReadOnly(const char *z, OsFile **ppFile){
  OsFile *pBase = 0;
  int rc = xOrigOpenReadOnly(z, &pBase);
  if( rc==SQLITE_OK ){
    rc = asyncOpenFile(z, ppFile, pBase, 0);
  }
  return rc;
}
static int asyncOpenReadWrite(const char *z, OsFile **ppFile, int *pReadOnly){
  OsFile *pBase = 0;
  int rc = xOrigOpenReadWrite(z, &pBase, pReadOnly);
  if( rc==SQLITE_OK ){
    rc = asyncOpenFile(z, ppFile, pBase, (*pReadOnly ? 0 : 1));
  }
  return rc;
}

/*
** Implementation of sqlite3OsDelete. Add an entry to the end of the 
** write-op queue to perform the delete.
*/
static int asyncDelete(const char *z){
  return addNewAsyncWrite(0, ASYNC_DELETE, 0, strlen(z)+1, z);
}

/*
** Implementation of sqlite3OsSyncDirectory. Add an entry to the end of the 
** write-op queue to perform the directory sync.
*/
static int asyncSyncDirectory(const char *z){
  return addNewAsyncWrite(0, ASYNC_SYNCDIRECTORY, 0, strlen(z)+1, z);
}

/*
** Implementation of sqlite3OsFileExists. Return true if file 'z' exists
** in the file system. 
**
** This method holds the mutex from start to finish.
*/
static int asyncFileExists(const char *z){
  int ret;
  AsyncWrite *p;

  pthread_mutex_lock(&async.queueMutex);

  /* See if the real file system contains the specified file.  */
  ret = xOrigFileExists(z);
  
  for(p=async.pQueueFirst; p; p = p->pNext){
    if( p->op==ASYNC_DELETE && 0==strcmp(p->zBuf, z) ){
      ret = 0;
    }else if( p->op==ASYNC_OPENEXCLUSIVE && 0==strcmp(p->zBuf, z) ){
      ret = 1;
    }
  }

  pthread_mutex_unlock(&async.queueMutex);
  return ret;
}

/*
** Call this routine to enable or disable the
** asynchronous IO features implemented in this file. 
**
** This routine is not even remotely threadsafe.  Do not call
** this routine while any SQLite database connections are open.
*/
static void asyncEnable(int enable){
  if( enable && xOrigOpenReadWrite==0 ){
    xOrigOpenReadWrite = sqlite3Os.xOpenReadWrite;
    xOrigOpenReadOnly = sqlite3Os.xOpenReadOnly;
    xOrigOpenExclusive = sqlite3Os.xOpenExclusive;
    xOrigDelete = sqlite3Os.xDelete;
    xOrigFileExists = sqlite3Os.xFileExists;
    xOrigSyncDirectory = sqlite3Os.xSyncDirectory;

    sqlite3Os.xOpenReadWrite = asyncOpenReadWrite;
    sqlite3Os.xOpenReadOnly = asyncOpenReadOnly;
    sqlite3Os.xOpenExclusive = asyncOpenExclusive;
    sqlite3Os.xDelete = asyncDelete;
    sqlite3Os.xFileExists = asyncFileExists;
    sqlite3Os.xSyncDirectory = asyncSyncDirectory;
  }
  if( !enable && xOrigOpenReadWrite!=0 ){
    sqlite3Os.xOpenReadWrite = xOrigOpenReadWrite;
    sqlite3Os.xOpenReadOnly = xOrigOpenReadOnly;
    sqlite3Os.xOpenExclusive = xOrigOpenExclusive;
    sqlite3Os.xDelete = xOrigDelete;
    sqlite3Os.xFileExists = xOrigFileExists;
    sqlite3Os.xSyncDirectory = xOrigSyncDirectory;

    xOrigOpenReadWrite = 0;
    xOrigOpenReadOnly = 0;
    xOrigOpenExclusive = 0;
    xOrigDelete = 0;
    xOrigFileExists = 0;
    xOrigSyncDirectory = 0;
  }
}

/* 
** This procedure runs in a separate thread, reading messages off of the
** write queue and processing them one by one.  
**
** If async.writerHaltNow is true, then this procedure exits
** after processing a single message.
**
** If async.writerHaltWhenIdle is true, then this procedure exits when
** the write queue is empty.
**
** If both of the above variables are false, this procedure runs
** indefinately, waiting for operations to be added to the write queue
** and processing them in the order in which they arrive.
**
** An artifical delay of async.ioDelay milliseconds is inserted before
** each write operation in order to simulate the effect of a slow disk.
**
** Only one instance of this procedure may be running at a time.
*/
static void *asyncWriterThread(void *NotUsed){
  AsyncWrite *p = 0;
  int rc = SQLITE_OK;

  if( pthread_mutex_trylock(&async.writerMutex) ){
    return 0;
  }
  while( async.writerHaltNow==0 ){
    int holdingMutex;
    OsFile *pBase = 0;

    pthread_mutex_lock(&async.queueMutex);
    holdingMutex = 1;
    while( (p = async.pQueueFirst)==0 ){
      pthread_cond_broadcast(&async.emptySignal);
      if( async.writerHaltWhenIdle ){
        pthread_mutex_unlock(&async.queueMutex);
        break;
      }else{
        TRACE("IDLE\n", 0);
        pthread_cond_wait(&async.queueSignal, &async.queueMutex);
        TRACE("WAKEUP\n", 0);
      }
    }
    if( p==0 ) break;
    TRACE("PROCESSING %p\n", p);

    /* Right now this thread is holding the mutex on the write-op queue.
    ** Variable 'p' points to the first entry in the write-op queue. In
    ** the general case, we hold on to the mutex for the entire body of
    ** the loop. 
    **
    ** However in the cases enumerated below, we relinquish the mutex,
    ** perform the IO, and then re-request the mutex before removing 'p' from
    ** the head of the write-op queue. The idea is to increase concurrency with
    ** sqlite threads.
    **
    **     * An ASYNC_CLOSE operation.
    **     * An ASYNC_OPENEXCLUSIVE operation. For this one, we relinquish 
    **       the mutex, call the underlying xOpenExclusive() function, then
    **       re-aquire the mutex before seting the AsyncFile.pBaseRead 
    **       variable.
    **     * ASYNC_SYNC and ASYNC_WRITE operations, if 
    **       SQLITE_ASYNC_TWO_FILEHANDLES was set at compile time and two
    **       file-handles are open for the particular file being "synced".
    */
    if( p->pFile ){
      pBase = p->pFile->pBaseWrite;
      if( 
        p->op==ASYNC_CLOSE || 
        p->op==ASYNC_OPENEXCLUSIVE ||
        (pBase && (p->op==ASYNC_SYNC || p->op==ASYNC_WRITE) ) 
      ){
        pthread_mutex_unlock(&async.queueMutex);
        holdingMutex = 0;
      }
      if( !pBase ){
        pBase = p->pFile->pBaseRead;
      }
    }

    switch( p->op ){
      case ASYNC_WRITE:
        assert( pBase );
        rc = sqlite3OsSeek(pBase, p->iOffset);
        if( rc==SQLITE_OK ){
          rc = sqlite3OsWrite(pBase, (const void *)(p->zBuf), p->nByte);
        }
        break;

      case ASYNC_SYNC:
        assert( pBase );
        rc = sqlite3OsSync(pBase, p->nByte);
        break;

      case ASYNC_TRUNCATE:
        assert( pBase );
        rc = sqlite3OsTruncate(pBase, p->nByte);
        break;

      case ASYNC_CLOSE:
        sqlite3OsClose(&p->pFile->pBaseRead);
        sqlite3OsClose(&p->pFile->pBaseWrite);
        sqlite3OsFree(p->pFile);
        break;

      case ASYNC_OPENDIRECTORY:
        assert( pBase );
        sqlite3OsOpenDirectory(pBase, p->zBuf);
        break;

      case ASYNC_SETFULLSYNC:
        assert( pBase );
        sqlite3OsSetFullSync(pBase, p->nByte);
        break;

      case ASYNC_DELETE:
        rc = xOrigDelete(p->zBuf);
        break;

      case ASYNC_SYNCDIRECTORY:
        rc = xOrigSyncDirectory(p->zBuf);
        break;

      case ASYNC_OPENEXCLUSIVE: {
        AsyncFile *pFile = p->pFile;
        int delFlag = ((p->iOffset)?1:0);
        OsFile *pBase = 0;
        rc = xOrigOpenExclusive(p->zBuf, &pBase, delFlag);
        assert( holdingMutex==0 );
        pthread_mutex_lock(&async.queueMutex);
        holdingMutex = 1;
        if( rc==SQLITE_OK ){
          pFile->pBaseRead = pBase;
        }
        break;
      }

      default: assert(!"Illegal value for AsyncWrite.op");
    }

    /* If we didn't hang on to the mutex during the IO op, obtain it now
    ** so that the AsyncWrite structure can be safely removed from the 
    ** global write-op queue.
    */
    if( !holdingMutex ){
      pthread_mutex_lock(&async.queueMutex);
      holdingMutex = 1;
    }
    TRACE("UNLINK %p\n", p);
    if( rc==SQLITE_OK ){
      if( p==async.pQueueLast ){
        async.pQueueLast = 0;
      }
      async.pQueueFirst = p->pNext;
    }
    assert( holdingMutex );

    /* Drop the queue mutex before continuing to the next write operation
    ** in order to give other threads a chance to work with the write queue.
    */
    pthread_mutex_unlock(&async.queueMutex);
    if( async.ioDelay>0 ){
      sqlite3OsSleep(async.ioDelay);
    }else{
      sched_yield();
    }
  }
  pthread_mutex_unlock(&async.writerMutex);
  return 0;
}

/**************************************************************************
** The remaining code defines a Tcl interface for testing the asynchronous
** IO implementation in this file.
**
** To adapt the code to a non-TCL environment, delete or comment out
** the code that follows.
*/

/*
** sqlite3async_enable ?YES/NO?
**
** Enable or disable the asynchronous I/O backend.  This command is
** not thread-safe.  Do not call it while any database connections
** are open.
*/
static int testAsyncEnable(
  void * clientData,
  Tcl_Interp *interp,
  int objc,
  Tcl_Obj *CONST objv[]
){
  if( objc!=1 && objc!=2 ){
    Tcl_WrongNumArgs(interp, 1, objv, "?YES/NO?");
    return TCL_ERROR;
  }
  if( objc==1 ){
    Tcl_SetObjResult(interp, Tcl_NewBooleanObj(xOrigOpenReadWrite!=0));
  }else{
    int en;
    if( Tcl_GetBooleanFromObj(interp, objv[1], &en) ) return TCL_ERROR;
    asyncEnable(en);
  }
  return TCL_OK;
}

/*
** sqlite3async_halt  "now"|"idle"|"never"
**
** Set the conditions at which the writer thread will halt.
*/
static int testAsyncHalt(
  void * clientData,
  Tcl_Interp *interp,
  int objc,
  Tcl_Obj *CONST objv[]
){
  const char *zCond;
  if( objc!=2 ){
    Tcl_WrongNumArgs(interp, 1, objv, "\"now\"|\"idle\"|\"never\"");
    return TCL_ERROR;
  }
  zCond = Tcl_GetString(objv[1]);
  if( strcmp(zCond, "now")==0 ){
    async.writerHaltNow = 1;
    pthread_cond_broadcast(&async.queueSignal);
  }else if( strcmp(zCond, "idle")==0 ){
    async.writerHaltWhenIdle = 1;
    async.writerHaltNow = 0;
    pthread_cond_broadcast(&async.queueSignal);
  }else if( strcmp(zCond, "never")==0 ){
    async.writerHaltWhenIdle = 0;
    async.writerHaltNow = 0;
  }else{
    Tcl_AppendResult(interp, 
      "should be one of: \"now\", \"idle\", or \"never\"", (char*)0);
    return TCL_ERROR;
  }
  return TCL_OK;
}

/*
** sqlite3async_delay ?MS?
**
** Query or set the number of milliseconds of delay in the writer
** thread after each write operation.  The default is 0.  By increasing
** the memory delay we can simulate the effect of slow disk I/O.
*/
static int testAsyncDelay(
  void * clientData,
  Tcl_Interp *interp,
  int objc,
  Tcl_Obj *CONST objv[]
){
  if( objc!=1 && objc!=2 ){
    Tcl_WrongNumArgs(interp, 1, objv, "?MS?");
    return TCL_ERROR;
  }
  if( objc==1 ){
    Tcl_SetObjResult(interp, Tcl_NewIntObj(async.ioDelay));
  }else{
    int ioDelay;
    if( Tcl_GetIntFromObj(interp, objv[1], &ioDelay) ) return TCL_ERROR;
    async.ioDelay = ioDelay;
  }
  return TCL_OK;
}

/*
** sqlite3async_start
**
** Start a new writer thread.
*/
static int testAsyncStart(
  void * clientData,
  Tcl_Interp *interp,
  int objc,
  Tcl_Obj *CONST objv[]
){
  pthread_t x;
  int rc;
  rc = pthread_create(&x, 0, asyncWriterThread, 0);
  if( rc ){
    Tcl_AppendResult(interp, "failed to create the thread", 0);
    return TCL_ERROR;
  }
  pthread_detach(x);
  return TCL_OK;
}

/*
** sqlite3async_wait
**
** Wait for the current writer thread to terminate.
**
** If the current writer thread is set to run forever then this
** command would block forever.  To prevent that, an error is returned. 
*/
static int testAsyncWait(
  void * clientData,
  Tcl_Interp *interp,
  int objc,
  Tcl_Obj *CONST objv[]
){
  if( async.writerHaltNow==0 && async.writerHaltWhenIdle==0 ){
    Tcl_AppendResult(interp, "would block forever", (char*)0);
    return TCL_ERROR;
  }
  TRACE("WAIT\n",0);
  pthread_cond_broadcast(&async.queueSignal);
  pthread_mutex_lock(&async.writerMutex);
  pthread_mutex_unlock(&async.writerMutex);
  return TCL_OK;
}


#endif  /* OS_UNIX and THREADSAFE and defined(SQLITE_ENABLE_REDEF_IO) */

/*
** This routine registers the custom TCL commands defined in this
** module.  This should be the only procedure visible from outside
** of this module.
*/
int Sqlitetestasync_Init(Tcl_Interp *interp){
#if OS_UNIX && THREADSAFE && defined(SQLITE_ENABLE_REDEF_IO)
  Tcl_CreateObjCommand(interp,"sqlite3async_enable",testAsyncEnable,0,0);
  Tcl_CreateObjCommand(interp,"sqlite3async_halt",testAsyncHalt,0,0);
  Tcl_CreateObjCommand(interp,"sqlite3async_delay",testAsyncDelay,0,0);
  Tcl_CreateObjCommand(interp,"sqlite3async_start",testAsyncStart,0,0);
  Tcl_CreateObjCommand(interp,"sqlite3async_wait",testAsyncWait,0,0);
#endif  /* OS_UNIX and THREADSAFE and defined(SQLITE_ENABLE_REDEF_IO) */
  return TCL_OK;
}

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
** backend for SQLite.
**
** WHAT IS ASYNCHRONOUS I/O?
**
** With asynchronous I/O, write requests are handled by a separate thread
** running in the background.  This means that the thread that initiates
** a database write does not have to wait for (sometimes slow) disk I/O
** to occur.  The write seems to happen very quickly, though in reality
** it is happening at its usual slow pace in the background.
**
** Asynchronous I/O appears to give better responsiveness, but at a price.
** You lose the Durable property.  With the default I/O backend of SQLite,
** once a write completes, you know that the information you wrote is
** safely on disk.  With the asynchronous I/O, this is no the case.  If
** your program crashes or if you take a power lose after the database
** write but before the asynchronous write thread has completed, then the
** database change might never make it to disk and the next user of the
** database might not see your change.
**
** You lose Durability with asynchronous I/O, but you still retain the
** other parts of ACID:  Atomic,  Consistent, and Isolated.  Many
** appliations get along fine without the Durablity.
**
** HOW IT WORKS
**
** Asynchronous I/O works by overloading the OS-layer disk I/O routines
** with modified versions that store the data to be written in queue of
** pending write operations.  Look at the asyncEnable() subroutine to see
** how overloading works.  Six os-layer routines are overloaded:
**
**     sqlite3OsOpenReadWrite;
**     sqlite3OsOpenReadOnly;
**     sqlite3OsOpenExclusive;
**     sqlite3OsDelete;
**     sqlite3OsFileExists;
**     sqlite3OsSyncDirectory;
**
** The original implementations of these routines are saved and are
** used by the writer thread to do the real I/O.  The substitute
** implementations typically put the I/O operation on a queue
** to be handled later by the writer thread, though read operations
** must be handled right away, obviously.
**
** Asynchronous I/O is disabled by setting the os-layer interface routines
** back to their original values.
**
** LIMITATIONS
**
** This demonstration code is deliberately kept simple in order to keep
** the main ideas clear and easy to understand.  Real applications that
** want to do asynchronous I/O might want to add additional capabilities.
** For example, in this demonstration if writes are happening at a steady
** stream that exceeds the I/O capability of the background writer thread,
** the queue of pending write operations will grow without bound until we
** run out of memory.  Users of this technique may want to keep track of
** the quantity of pending writes and stop accepting new write requests
** when the buffer gets to be too big.
*/

#include "sqliteInt.h"
#include <tcl.h>

/*
** This test uses pthreads and hence only works on unix and with
** a threadsafe build of SQLite.
*/
#if OS_UNIX && SQLITE_THREADSAFE

/*
** This demo uses pthreads.  If you do not have a pthreads implementation
** for your operating system, you will need to recode the threading 
** logic.
*/
#include <pthread.h>
#include <sched.h>

/* Useful macros used in several places */
#define MIN(x,y) ((x)<(y)?(x):(y))
#define MAX(x,y) ((x)>(y)?(x):(y))

/* Forward references */
typedef struct AsyncWrite AsyncWrite;
typedef struct AsyncFile AsyncFile;
typedef struct AsyncFileData AsyncFileData;

/* Enable for debugging */
static int sqlite3async_trace = 0;
# define ASYNC_TRACE(X) if( sqlite3async_trace ) asyncTrace X
static void asyncTrace(const char *zFormat, ...){
  char *z;
  va_list ap;
  va_start(ap, zFormat);
  z = sqlite3_vmprintf(zFormat, ap);
  va_end(ap);
  fprintf(stderr, "[%d] %s", (int)pthread_self(), z);
  sqlite3_free(z);
}

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
**     * See the last two paragraphs under "The Writer Thread" for
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
**         asyncWrite, asyncClose, asyncTruncate, asyncSync, 
**         asyncSetFullSync, asyncOpenDirectory.
**    
**     The operations above add an entry to the global write-op list. They
**     prepare the entry, acquire the async.queueMutex momentarily while
**     list pointers are  manipulated to insert the new entry, then release
**     the mutex and signal the writer thread to wake up in case it happens
**     to be asleep.
**
**    
**         asyncRead, asyncFileSize.
**
**     Read operations. Both of these read from both the underlying file
**     first then adjust their result based on pending writes in the 
**     write-op queue.   So async.queueMutex is held for the duration
**     of these operations to prevent other threads from changing the
**     queue in mid operation.
**    
**
**         asyncLock, asyncUnlock, asyncLockState, asyncCheckReservedLock
**    
**     These primitives implement in-process locking using a hash table
**     on the file name.  Files are locked correctly for connections coming
**     from the same process.  But other processes cannot see these locks
**     and will therefore not honor them.
**
**
**         asyncFileHandle.
**    
**     The sqlite3OsFileHandle() function is currently only used when 
**     debugging the pager module. Unless sqlite3OsClose() is called on the
**     file (shouldn't be possible for other reasons), the underlying 
**     implementations are safe to call without grabbing any mutex. So we just
**     go ahead and call it no matter what any other threads are doing.
**
**    
**         asyncSeek.
**
**     Calling this method just manipulates the AsyncFile.iOffset variable. 
**     Since this variable is never accessed by writer thread, this
**     function does not require the mutex.  Actual calls to OsSeek() take 
**     place just before OsWrite() or OsRead(), which are always protected by 
**     the mutex.
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
**     period (while the IO is performed), and sometimes it is
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
  pthread_mutex_t lockMutex;   /* For access to aLock hash table */
  pthread_cond_t queueSignal;  /* For waking up sleeping writer thread */
  pthread_cond_t emptySignal;  /* Notify when the write queue is empty */
  AsyncWrite *pQueueFirst;     /* Next write operation to be processed */
  AsyncWrite *pQueueLast;      /* Last write operation on the list */
  Hash aLock;                  /* Files locked */
  volatile int ioDelay;             /* Extra delay between write operations */
  volatile int writerHaltWhenIdle;  /* Writer thread halts when queue empty */
  volatile int writerHaltNow;       /* Writer thread halts after next op */
  int ioError;                 /* True if an IO error has occured */
  int nFile;                   /* Number of open files (from sqlite pov) */
} async = {
  PTHREAD_MUTEX_INITIALIZER,
  PTHREAD_MUTEX_INITIALIZER,
  PTHREAD_MUTEX_INITIALIZER,
  PTHREAD_COND_INITIALIZER,
  PTHREAD_COND_INITIALIZER,
};

/* Possible values of AsyncWrite.op */
#define ASYNC_NOOP          0
#define ASYNC_WRITE         1
#define ASYNC_SYNC          2
#define ASYNC_TRUNCATE      3
#define ASYNC_CLOSE         4
#define ASYNC_OPENDIRECTORY 5
#define ASYNC_SETFULLSYNC   6
#define ASYNC_DELETE        7
#define ASYNC_OPENEXCLUSIVE 8
#define ASYNC_SYNCDIRECTORY 9

/* Names of opcodes.  Used for debugging only.
** Make sure these stay in sync with the macros above!
*/
static const char *azOpcodeName[] = {
  "NOOP", "WRITE", "SYNC", "TRUNCATE", "CLOSE",
  "OPENDIR", "SETFULLSYNC", "DELETE", "OPENEX", "SYNCDIR",
};

/*
** Entries on the write-op queue are instances of the AsyncWrite
** structure, defined here.
**
** The interpretation of the iOffset and nByte variables varies depending 
** on the value of AsyncWrite.op:
**
** ASYNC_WRITE:
**     iOffset -> Offset in file to write to.
**     nByte   -> Number of bytes of data to write (pointed to by zBuf).
**
** ASYNC_SYNC:
**     nByte   -> flags to pass to sqlite3OsSync().
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
**     iOffset -> Contains the "syncDir" flag.
**     nByte   -> Number of bytes of zBuf points to (file name).
**
** ASYNC_OPENEXCLUSIVE:
**     iOffset -> Value of "delflag".
**     nByte   -> Number of bytes of zBuf points to (file name).
**
**
** For an ASYNC_WRITE operation, zBuf points to the data to write to the file. 
** This space is sqlite3_malloc()d along with the AsyncWrite structure in a
** single blob, so is deleted when sqlite3_free() is called on the parent 
** structure.
*/
struct AsyncWrite {
  AsyncFileData *pFileData;    /* File to write data to or sync */
  int op;                      /* One of ASYNC_xxx etc. */
  i64 iOffset;        /* See above */
  int nByte;          /* See above */
  char *zBuf;         /* Data to write to file (or NULL if op!=ASYNC_WRITE) */
  AsyncWrite *pNext;  /* Next write operation (to any file) */
};

/* 
** The AsyncFile structure is a subclass of sqlite3_file used for 
** asynchronous IO. 
**
** All of the actual data for the structure is stored in the structure
** pointed to by AsyncFile.pData, which is allocated as part of the
** sqlite3OsOpen() using sqlite3_malloc(). The reason for this is that the
** lifetime of the AsyncFile structure is ended by the caller after OsClose()
** is called, but the data in AsyncFileData may be required by the
** writer thread after that point.
*/
struct AsyncFile {
  sqlite3_io_methods *pMethod;
  AsyncFileData *pData;
};
struct AsyncFileData {
  char *zName;               /* Underlying OS filename - used for debugging */
  int nName;                 /* Number of characters in zName */
  sqlite3_file *pBaseRead;   /* Read handle to the underlying Os file */
  sqlite3_file *pBaseWrite;  /* Write handle to the underlying Os file */
};

/*
** Add an entry to the end of the global write-op list. pWrite should point 
** to an AsyncWrite structure allocated using sqlite3_malloc().  The writer
** thread will call sqlite3_free() to free the structure after the specified
** operation has been completed.
**
** Once an AsyncWrite structure has been added to the list, it becomes the
** property of the writer thread and must not be read or modified by the
** caller.  
*/
static void addAsyncWrite(AsyncWrite *pWrite){
  /* We must hold the queue mutex in order to modify the queue pointers */
  pthread_mutex_lock(&async.queueMutex);

  /* Add the record to the end of the write-op queue */
  assert( !pWrite->pNext );
  if( async.pQueueLast ){
    assert( async.pQueueFirst );
    async.pQueueLast->pNext = pWrite;
  }else{
    async.pQueueFirst = pWrite;
  }
  async.pQueueLast = pWrite;
  ASYNC_TRACE(("PUSH %p (%s %s %d)\n", pWrite, azOpcodeName[pWrite->op],
         pWrite->pFileData ? pWrite->pFileData->zName : "-", pWrite->iOffset));

  if( pWrite->op==ASYNC_CLOSE ){
    async.nFile--;
    if( async.nFile==0 ){
      async.ioError = SQLITE_OK;
    }
  }

  /* Drop the queue mutex */
  pthread_mutex_unlock(&async.queueMutex);

  /* The writer thread might have been idle because there was nothing
  ** on the write-op queue for it to do.  So wake it up. */
  pthread_cond_signal(&async.queueSignal);
}

/*
** Increment async.nFile in a thread-safe manner.
*/
static void incrOpenFileCount(){
  /* We must hold the queue mutex in order to modify async.nFile */
  pthread_mutex_lock(&async.queueMutex);
  if( async.nFile==0 ){
    async.ioError = SQLITE_OK;
  }
  async.nFile++;
  pthread_mutex_unlock(&async.queueMutex);
}

/*
** This is a utility function to allocate and populate a new AsyncWrite
** structure and insert it (via addAsyncWrite() ) into the global list.
*/
static int addNewAsyncWrite(
  AsyncFileData *pFileData, 
  int op, 
  i64 iOffset, 
  int nByte,
  const char *zByte
){
  AsyncWrite *p;
  if( op!=ASYNC_CLOSE && async.ioError ){
    return async.ioError;
  }
  p = sqlite3_malloc(sizeof(AsyncWrite) + (zByte?nByte:0));
  if( !p ){
    return SQLITE_NOMEM;
  }
  p->op = op;
  p->iOffset = iOffset;
  p->nByte = nByte;
  p->pFileData = pFileData;
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
static int asyncClose(sqlite3_file *pFile){
  AsyncFileData *p = ((AsyncFile *)pFile)->pData;
  return addNewAsyncWrite(p, ASYNC_CLOSE, 0, 0, 0);
}

/*
** Implementation of sqlite3OsWrite() for asynchronous files. Instead of 
** writing to the underlying file, this function adds an entry to the end of
** the global AsyncWrite list. Either SQLITE_OK or SQLITE_NOMEM may be
** returned.
*/
static int asyncWrite(sqlite3_file *pFile, const void *pBuf, int amt, i64 iOff){
  AsyncFileData *p = ((AsyncFile *)pFile)->pData;
  return addNewAsyncWrite(p, ASYNC_WRITE, iOff, amt, pBuf);
}

/*
** Read data from the file. First we read from the filesystem, then adjust 
** the contents of the buffer based on ASYNC_WRITE operations in the 
** write-op queue.
**
** This method holds the mutex from start to finish.
*/
static int asyncRead(sqlite3_file *pFile, void *zOut, int iAmt, i64 iOffset){
  AsyncFileData *p = ((AsyncFile *)pFile)->pData;
  int rc = SQLITE_OK;
  i64 filesize;
  int nRead;
  sqlite3_file *pBase = p->pBaseRead;

  /* If an I/O error has previously occurred in this virtual file 
  ** system, then all subsequent operations fail.
  */
  if( async.ioError!=SQLITE_OK ){
    return async.ioError;
  }

  /* Grab the write queue mutex for the duration of the call */
  pthread_mutex_lock(&async.queueMutex);

  if( pBase->pMethods ){
    rc = sqlite3OsFileSize(pBase, &filesize);
    if( rc!=SQLITE_OK ){
      goto asyncread_out;
    }
    nRead = MIN(filesize - iOffset, iAmt);
    if( nRead>0 ){
      rc = sqlite3OsRead(pBase, zOut, nRead, iOffset);
      ASYNC_TRACE(("READ %s %d bytes at %d\n", p->zName, nRead, iOffset));
    }
  }

  if( rc==SQLITE_OK ){
    AsyncWrite *pWrite;

    for(pWrite=async.pQueueFirst; pWrite; pWrite = pWrite->pNext){
      if( pWrite->pFileData==p && pWrite->op==ASYNC_WRITE ){
        int iBeginOut = (pWrite->iOffset-iOffset);
        int iBeginIn = -iBeginOut;
        int nCopy;

        if( iBeginIn<0 ) iBeginIn = 0;
        if( iBeginOut<0 ) iBeginOut = 0;
        nCopy = MIN(pWrite->nByte-iBeginIn, iAmt-iBeginOut);

        if( nCopy>0 ){
          memcpy(&((char *)zOut)[iBeginOut], &pWrite->zBuf[iBeginIn], nCopy);
          ASYNC_TRACE(("OVERREAD %d bytes at %d\n", nCopy, iBeginOut+iOffset));
        }
      }
    }
  }

asyncread_out:
  pthread_mutex_unlock(&async.queueMutex);
  return rc;
}

/*
** Truncate the file to nByte bytes in length. This just adds an entry to 
** the write-op list, no IO actually takes place.
*/
static int asyncTruncate(sqlite3_file *pFile, i64 nByte){
  AsyncFileData *p = ((AsyncFile *)pFile)->pData;
  return addNewAsyncWrite(p, ASYNC_TRUNCATE, nByte, 0, 0);
}

/*
** Sync the file. This just adds an entry to the write-op list, the 
** sync() is done later by sqlite3_async_flush().
*/
static int asyncSync(sqlite3_file *pFile, int flags){
  AsyncFileData *p = ((AsyncFile *)pFile)->pData;
  return addNewAsyncWrite(p, ASYNC_SYNC, 0, flags, 0);
}

/*
** Read the size of the file. First we read the size of the file system 
** entry, then adjust for any ASYNC_WRITE or ASYNC_TRUNCATE operations 
** currently in the write-op list. 
**
** This method holds the mutex from start to finish.
*/
int asyncFileSize(sqlite3_file *pFile, i64 *piSize){
  AsyncFileData *p = ((AsyncFile *)pFile)->pData;
  int rc = SQLITE_OK;
  i64 s = 0;
  sqlite3_file *pBase;

  pthread_mutex_lock(&async.queueMutex);

  /* Read the filesystem size from the base file. If pBaseRead is NULL, this
  ** means the file hasn't been opened yet. In this case all relevant data 
  ** must be in the write-op queue anyway, so we can omit reading from the
  ** file-system.
  */
  pBase = p->pBaseRead;
  if( pBase->pMethods ){
    rc = sqlite3OsFileSize(pBase, &s);
  }

  if( rc==SQLITE_OK ){
    AsyncWrite *pWrite;
    for(pWrite=async.pQueueFirst; pWrite; pWrite = pWrite->pNext){
      if( pWrite->pFileData==p ){
        switch( pWrite->op ){
          case ASYNC_WRITE:
            s = MAX(pWrite->iOffset + (i64)(pWrite->nByte), s);
            break;
          case ASYNC_TRUNCATE:
            s = MIN(s, pWrite->iOffset);
            break;
        }
      }
    }
    *piSize = s;
  }
  pthread_mutex_unlock(&async.queueMutex);
  return rc;
}

/*
** No disk locking is performed.  We keep track of locks locally in
** the async.aLock hash table.  Locking should appear to work the same
** as with standard (unmodified) SQLite as long as all connections 
** come from this one process.  Connections from external processes
** cannot see our internal hash table (obviously) and will thus not
** honor our locks.
*/
static int asyncLock(sqlite3_file *pFile, int lockType){
  AsyncFileData *p = ((AsyncFile *)pFile)->pData;
  ASYNC_TRACE(("LOCK %d (%s)\n", lockType, p->zName));
  pthread_mutex_lock(&async.lockMutex);
  sqlite3HashInsert(&async.aLock, p->zName, p->nName, (void*)lockType);
  pthread_mutex_unlock(&async.lockMutex);
  return SQLITE_OK;
}
static int asyncUnlock(sqlite3_file *pFile, int lockType){
  return asyncLock(pFile, lockType);
}

/*
** This function is called when the pager layer first opens a database file
** and is checking for a hot-journal.
*/
static int asyncCheckReservedLock(sqlite3_file *pFile){
  AsyncFileData *p = ((AsyncFile *)pFile)->pData;
  int rc;
  pthread_mutex_lock(&async.lockMutex);
  rc = (int)sqlite3HashFind(&async.aLock, p->zName, p->nName);
  pthread_mutex_unlock(&async.lockMutex);
  ASYNC_TRACE(("CHECK-LOCK %d (%s)\n", rc, p->zName));
  return rc>SHARED_LOCK;
}

/* 
** This is a no-op, as the asynchronous backend does not support locking.
*/
static int asyncFileControl(sqlite3_file *id, int op, void *pArg){
  return SQLITE_ERROR;
}

/* 
** Return the device characteristics and sector-size of the device. It
** is not tricky to implement these correctly, as this backend might 
** not have an open file handle at this point.
*/
static int asyncSectorSize(sqlite3_file *pFile){
  return 512;
}
static int asyncDeviceCharacteristics(sqlite3_file *pFile){
  return 0;
}

/*
** Open a file.
*/
static int asyncOpen(
  sqlite3_vfs *pAsyncVfs,
  const char *zName,
  sqlite3_file *pFile,
  int flags,
  int *pOutFlags
){
  static sqlite3_io_methods async_methods = {
    1,                               /* iVersion */
    asyncClose,                      /* xClose */
    asyncRead,                       /* xRead */
    asyncWrite,                      /* xWrite */
    asyncTruncate,                   /* xTruncate */
    asyncSync,                       /* xSync */
    asyncFileSize,                   /* xFileSize */
    asyncLock,                       /* xLock */
    asyncUnlock,                     /* xUnlock */
    asyncCheckReservedLock,          /* xCheckReservedLock */
    asyncFileControl,                /* xFileControl */
    asyncSectorSize,                 /* xSectorSize */
    asyncDeviceCharacteristics       /* xDeviceCharacteristics */
  };

  sqlite3_vfs *pVfs = (sqlite3_vfs *)pAsyncVfs->pAppData;
  AsyncFile *p = (AsyncFile *)pFile;
  int nName = strlen(zName);;
  int rc;
  int nByte;
  AsyncFileData *pData;

  nByte = (
    sizeof(AsyncFileData) +        /* AsyncFileData structure */
    2 * pVfs->szOsFile +           /* AsyncFileData.zName */
    nName + 1                      /* AsyncFileData.pBaseRead and pBaseWrite */
  );
  pData = sqlite3_malloc(nByte);
  if( !pData ){
    return SQLITE_NOMEM;
  }
  memset(pData, 0, nByte);
  pData->zName = (char *)&pData[1];
  pData->nName = nName;
  pData->pBaseRead = (sqlite3_file *)&pData->zName[nName+1];
  pData->pBaseWrite = (sqlite3_file *)&pData->zName[nName+1+pVfs->szOsFile];
  memcpy(pData->zName, zName, nName+1);

  if( flags&SQLITE_OPEN_EXCLUSIVE ){
    rc = addNewAsyncWrite(pData, ASYNC_OPENEXCLUSIVE, (i64)flags, 0, 0);
    if( pOutFlags ) *pOutFlags = flags;
  }else{
    rc = sqlite3OsOpen(pVfs, zName, pData->pBaseRead, flags, pOutFlags);
    if( rc==SQLITE_OK && ((*pOutFlags)&SQLITE_OPEN_READWRITE) ){
      rc = sqlite3OsOpen(pVfs, zName, pData->pBaseWrite, flags, 0);
    }
  }

  if( rc==SQLITE_OK ){
    p->pMethod = &async_methods;
    p->pData = pData;
    incrOpenFileCount();
  }else{
    sqlite3OsClose(pData->pBaseRead);
    sqlite3OsClose(pData->pBaseWrite);
    sqlite3_free(pData);
  }

  return rc;
}

/*
** Implementation of sqlite3OsDelete. Add an entry to the end of the 
** write-op queue to perform the delete.
*/
static int asyncDelete(sqlite3_vfs *pAsyncVfs, const char *z, int syncDir){
  return addNewAsyncWrite(0, ASYNC_DELETE, syncDir, strlen(z)+1, z);
}

/*
** Implementation of sqlite3OsAccess. This method holds the mutex from
** start to finish.
*/
static int asyncAccess(sqlite3_vfs *pAsyncVfs, const char *zName, int flags){
  int ret;
  AsyncWrite *p;
  sqlite3_vfs *pVfs = (sqlite3_vfs *)pAsyncVfs->pAppData;

  assert(flags==SQLITE_ACCESS_READWRITE 
      || flags==SQLITE_ACCESS_READ 
      || flags==SQLITE_ACCESS_EXISTS 
  );

  pthread_mutex_lock(&async.queueMutex);
  ret = sqlite3OsAccess(pVfs, zName, flags);
  if( flags==SQLITE_ACCESS_EXISTS ){
    for(p=async.pQueueFirst; p; p = p->pNext){
      if( p->op==ASYNC_DELETE && 0==strcmp(p->zBuf, zName) ){
        ret = 0;
      }else if( p->op==ASYNC_OPENEXCLUSIVE 
             && 0==strcmp(p->pFileData->zName, zName) 
      ){
        ret = 1;
      }
    }
  }
  ASYNC_TRACE(("ACCESS(%s): %s = %d\n", 
    flags==SQLITE_ACCESS_READWRITE?"read-write":
    flags==SQLITE_ACCESS_READ?"read":"exists"
    , zName, ret)
  );
  pthread_mutex_unlock(&async.queueMutex);
  return ret;
}

static int asyncGetTempName(sqlite3_vfs *pAsyncVfs, char *zBufOut){
  sqlite3_vfs *pVfs = (sqlite3_vfs *)pAsyncVfs->pAppData;
  return pVfs->xGetTempName(pVfs, zBufOut);
}
static int asyncFullPathname(
  sqlite3_vfs *pAsyncVfs, 
  const char *zPath, 
  char *zPathOut
){
  sqlite3_vfs *pVfs = (sqlite3_vfs *)pAsyncVfs->pAppData;
  return sqlite3OsFullPathname(pVfs, zPath, zPathOut);
}
static void *asyncDlOpen(sqlite3_vfs *pAsyncVfs, const char *zPath){
  sqlite3_vfs *pVfs = (sqlite3_vfs *)pAsyncVfs->pAppData;
  return pVfs->xDlOpen(pVfs, zPath);
}
static void asyncDlError(sqlite3_vfs *pAsyncVfs, int nByte, char *zErrMsg){
  sqlite3_vfs *pVfs = (sqlite3_vfs *)pAsyncVfs->pAppData;
  pVfs->xDlError(pVfs, nByte, zErrMsg);
}
static void *asyncDlSym(
  sqlite3_vfs *pAsyncVfs, 
  void *pHandle, 
  const char *zSymbol
){
  sqlite3_vfs *pVfs = (sqlite3_vfs *)pAsyncVfs->pAppData;
  return pVfs->xDlSym(pVfs, pHandle, zSymbol);
}
static void asyncDlClose(sqlite3_vfs *pAsyncVfs, void *pHandle){
  sqlite3_vfs *pVfs = (sqlite3_vfs *)pAsyncVfs->pAppData;
  pVfs->xDlClose(pVfs, pHandle);
}
static int asyncRandomness(sqlite3_vfs *pAsyncVfs, int nByte, char *zBufOut){
  sqlite3_vfs *pVfs = (sqlite3_vfs *)pAsyncVfs->pAppData;
  return pVfs->xRandomness(pVfs, nByte, zBufOut);
}
static int asyncSleep(sqlite3_vfs *pAsyncVfs, int nMicro){
  sqlite3_vfs *pVfs = (sqlite3_vfs *)pAsyncVfs->pAppData;
  return pVfs->xSleep(pVfs, nMicro);
}
static int asyncCurrentTime(sqlite3_vfs *pAsyncVfs, double *pTimeOut){
  sqlite3_vfs *pVfs = (sqlite3_vfs *)pAsyncVfs->pAppData;
  return pVfs->xCurrentTime(pVfs, pTimeOut);
}

static sqlite3_vfs async_vfs = {
  1,                    /* iVersion */
  sizeof(AsyncFile),    /* szOsFile */
  0,                    /* mxPathname */
  0,                    /* pNext */
  "async",              /* zName */
  0,                    /* pAppData */
  asyncOpen,            /* xOpen */
  asyncDelete,          /* xDelete */
  asyncAccess,          /* xAccess */
  asyncGetTempName,     /* xGetTempName */
  asyncFullPathname,    /* xFullPathname */
  asyncDlOpen,          /* xDlOpen */
  asyncDlError,         /* xDlError */
  asyncDlSym,           /* xDlSym */
  asyncDlClose,         /* xDlClose */
  asyncRandomness,      /* xDlError */
  asyncSleep,           /* xDlSym */
  asyncCurrentTime      /* xDlClose */
};

/*
** Call this routine to enable or disable the
** asynchronous IO features implemented in this file. 
**
** This routine is not even remotely threadsafe.  Do not call
** this routine while any SQLite database connections are open.
*/
static void asyncEnable(int enable){
  if( enable ){
    if( !async_vfs.pAppData ){
      async_vfs.pAppData = (void *)sqlite3_vfs_find(0);
      async_vfs.mxPathname = ((sqlite3_vfs *)async_vfs.pAppData)->mxPathname;
      sqlite3_vfs_register(&async_vfs, 1);
      sqlite3HashInit(&async.aLock, SQLITE_HASH_BINARY, 1);
    }
  }else{
    if( async_vfs.pAppData ){
      sqlite3_vfs_unregister(&async_vfs);
      async_vfs.pAppData = 0;
      sqlite3HashClear(&async.aLock);
    }
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
  sqlite3_vfs *pVfs = (sqlite3_vfs *)(async_vfs.pAppData);
  AsyncWrite *p = 0;
  int rc = SQLITE_OK;
  int holdingMutex = 0;

  if( pthread_mutex_trylock(&async.writerMutex) ){
    return 0;
  }
  while( async.writerHaltNow==0 ){
    sqlite3_file *pBase = 0;

    if( !holdingMutex ){
      pthread_mutex_lock(&async.queueMutex);
    }
    while( (p = async.pQueueFirst)==0 ){
      pthread_cond_broadcast(&async.emptySignal);
      if( async.writerHaltWhenIdle ){
        pthread_mutex_unlock(&async.queueMutex);
        break;
      }else{
        ASYNC_TRACE(("IDLE\n"));
        pthread_cond_wait(&async.queueSignal, &async.queueMutex);
        ASYNC_TRACE(("WAKEUP\n"));
      }
    }
    if( p==0 ) break;
    holdingMutex = 1;

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
    if( async.ioError!=SQLITE_OK && p->op!=ASYNC_CLOSE ){
      p->op = ASYNC_NOOP;
    }
    if( p->pFileData ){
      pBase = p->pFileData->pBaseWrite;
      if( 
        p->op==ASYNC_CLOSE || 
        p->op==ASYNC_OPENEXCLUSIVE ||
        (pBase->pMethods && (p->op==ASYNC_SYNC || p->op==ASYNC_WRITE) ) 
      ){
        pthread_mutex_unlock(&async.queueMutex);
        holdingMutex = 0;
      }
      if( !pBase->pMethods ){
        pBase = p->pFileData->pBaseRead;
      }
    }

    switch( p->op ){
      case ASYNC_NOOP:
        break;

      case ASYNC_WRITE:
        assert( pBase );
        ASYNC_TRACE(("WRITE %s %d bytes at %d\n",
                p->pFileData->zName, p->nByte, p->iOffset));
        rc = sqlite3OsWrite(pBase, (void *)(p->zBuf), p->nByte, p->iOffset);
        break;

      case ASYNC_SYNC:
        assert( pBase );
        ASYNC_TRACE(("SYNC %s\n", p->pFileData->zName));
        rc = sqlite3OsSync(pBase, p->nByte);
        break;

      case ASYNC_TRUNCATE:
        assert( pBase );
        ASYNC_TRACE(("TRUNCATE %s to %d bytes\n", 
                p->pFileData->zName, p->iOffset));
        rc = sqlite3OsTruncate(pBase, p->iOffset);
        break;

      case ASYNC_CLOSE:
        ASYNC_TRACE(("CLOSE %s\n", p->pFileData->zName));
        sqlite3OsClose(p->pFileData->pBaseWrite);
        sqlite3OsClose(p->pFileData->pBaseRead);
        sqlite3_free(p->pFileData);
        break;

      case ASYNC_DELETE:
        ASYNC_TRACE(("DELETE %s\n", p->zBuf));
        rc = sqlite3OsDelete(pVfs, p->zBuf, (int)p->iOffset);
        break;

      case ASYNC_OPENEXCLUSIVE: {
        int flags = (int)p->iOffset;
        AsyncFileData *pData = p->pFileData;
        ASYNC_TRACE(("OPEN %s flags=%d\n", p->zBuf, (int)p->iOffset));
        assert(pData->pBaseRead->pMethods==0 && pData->pBaseWrite->pMethods==0);
        rc = sqlite3OsOpen(pVfs, pData->zName, pData->pBaseRead, flags, 0);
        assert( holdingMutex==0 );
        pthread_mutex_lock(&async.queueMutex);
        holdingMutex = 1;
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
    /* ASYNC_TRACE(("UNLINK %p\n", p)); */
    if( p==async.pQueueLast ){
      async.pQueueLast = 0;
    }
    async.pQueueFirst = p->pNext;
    sqlite3_free(p);
    assert( holdingMutex );

    /* An IO error has occured. We cannot report the error back to the
    ** connection that requested the I/O since the error happened 
    ** asynchronously.  The connection has already moved on.  There 
    ** really is nobody to report the error to.
    **
    ** The file for which the error occured may have been a database or
    ** journal file. Regardless, none of the currently queued operations
    ** associated with the same database should now be performed. Nor should
    ** any subsequently requested IO on either a database or journal file 
    ** handle for the same database be accepted until the main database
    ** file handle has been closed and reopened.
    **
    ** Furthermore, no further IO should be queued or performed on any file
    ** handle associated with a database that may have been part of a 
    ** multi-file transaction that included the database associated with 
    ** the IO error (i.e. a database ATTACHed to the same handle at some 
    ** point in time).
    */
    if( rc!=SQLITE_OK ){
      async.ioError = rc;
    }

    /* Drop the queue mutex before continuing to the next write operation
    ** in order to give other threads a chance to work with the write queue.
    */
    if( !async.pQueueFirst || !async.ioError ){
      sqlite3ApiExit(0, 0);
      pthread_mutex_unlock(&async.queueMutex);
      holdingMutex = 0;
      if( async.ioDelay>0 ){
        sqlite3OsSleep(pVfs, async.ioDelay);
      }else{
        sched_yield();
      }
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
    Tcl_SetObjResult(interp, Tcl_NewBooleanObj(async_vfs.pAppData!=0));
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
  int cnt = 10;
  if( async.writerHaltNow==0 && async.writerHaltWhenIdle==0 ){
    Tcl_AppendResult(interp, "would block forever", (char*)0);
    return TCL_ERROR;
  }

  while( cnt-- && !pthread_mutex_trylock(&async.writerMutex) ){
    pthread_mutex_unlock(&async.writerMutex);
    sched_yield();
  }
  if( cnt>=0 ){
    ASYNC_TRACE(("WAIT\n"));
    pthread_mutex_lock(&async.queueMutex);
    pthread_cond_broadcast(&async.queueSignal);
    pthread_mutex_unlock(&async.queueMutex);
    pthread_mutex_lock(&async.writerMutex);
    pthread_mutex_unlock(&async.writerMutex);
  }else{
    ASYNC_TRACE(("NO-WAIT\n"));
  }
  return TCL_OK;
}


#endif  /* OS_UNIX and SQLITE_THREADSAFE and defined(SQLITE_ENABLE_REDEF_IO) */

/*
** This routine registers the custom TCL commands defined in this
** module.  This should be the only procedure visible from outside
** of this module.
*/
int Sqlitetestasync_Init(Tcl_Interp *interp){
#if OS_UNIX && SQLITE_THREADSAFE && defined(SQLITE_ENABLE_REDEF_IO)
  Tcl_CreateObjCommand(interp,"sqlite3async_enable",testAsyncEnable,0,0);
  Tcl_CreateObjCommand(interp,"sqlite3async_halt",testAsyncHalt,0,0);
  Tcl_CreateObjCommand(interp,"sqlite3async_delay",testAsyncDelay,0,0);
  Tcl_CreateObjCommand(interp,"sqlite3async_start",testAsyncStart,0,0);
  Tcl_CreateObjCommand(interp,"sqlite3async_wait",testAsyncWait,0,0);
  Tcl_LinkVar(interp, "sqlite3async_trace",
      (char*)&sqlite3async_trace, TCL_LINK_INT);
#endif  /* OS_UNIX and SQLITE_THREADSAFE and defined(SQLITE_ENABLE_REDEF_IO) */
  return TCL_OK;
}

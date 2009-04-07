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
** $Id: test_async.c,v 1.57 2009/04/07 11:21:29 danielk1977 Exp $
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
** safely on disk.  With the asynchronous I/O, this is not the case.  If
** your program crashes or if a power loss occurs after the database
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
** Asynchronous I/O works by creating a special SQLite "vfs" structure
** and registering it with sqlite3_vfs_register(). When files opened via 
** this vfs are written to (using sqlite3OsWrite()), the data is not 
** written directly to disk, but is placed in the "write-queue" to be
** handled by the background thread.
**
** When files opened with the asynchronous vfs are read from 
** (using sqlite3OsRead()), the data is read from the file on 
** disk and the write-queue, so that from the point of view of
** the vfs reader the OsWrite() appears to have already completed.
**
** The special vfs is registered (and unregistered) by calls to 
** function asyncEnable() (see below).
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
**
** LOCKING + CONCURRENCY
**
** Multiple connections from within a single process that use this
** implementation of asynchronous IO may access a single database
** file concurrently. From the point of view of the user, if all
** connections are from within a single process, there is no difference
** between the concurrency offered by "normal" SQLite and SQLite
** using the asynchronous backend.
**
** If connections from within multiple database files may access the
** database file, the ENABLE_FILE_LOCKING symbol (see below) must be
** defined. If it is not defined, then no locks are established on 
** the database file. In this case, if multiple processes access 
** the database file, corruption will quickly result.
**
** If ENABLE_FILE_LOCKING is defined (the default), then connections 
** from within multiple processes may access a single database file 
** without risking corruption. However concurrency is reduced as
** follows:
**
**   * When a connection using asynchronous IO begins a database
**     transaction, the database is locked immediately. However the
**     lock is not released until after all relevant operations
**     in the write-queue have been flushed to disk. This means
**     (for example) that the database may remain locked for some 
**     time after a "COMMIT" or "ROLLBACK" is issued.
**
**   * If an application using asynchronous IO executes transactions
**     in quick succession, other database users may be effectively
**     locked out of the database. This is because when a BEGIN
**     is executed, a database lock is established immediately. But
**     when the corresponding COMMIT or ROLLBACK occurs, the lock
**     is not released until the relevant part of the write-queue 
**     has been flushed through. As a result, if a COMMIT is followed
**     by a BEGIN before the write-queue is flushed through, the database 
**     is never unlocked,preventing other processes from accessing 
**     the database.
**
** Defining ENABLE_FILE_LOCKING when using an NFS or other remote 
** file-system may slow things down, as synchronous round-trips to the 
** server may be required to establish database file locks.
*/
#define ENABLE_FILE_LOCKING

#ifndef SQLITE_AMALGAMATION
# include "sqliteInt.h"
# include <assert.h>
# include <string.h>
#endif
#include <tcl.h>

/*
** This test uses pthreads and hence only works on unix and with
** a threadsafe build of SQLite.
*/
#if SQLITE_OS_UNIX && SQLITE_THREADSAFE

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
typedef struct AsyncFileLock AsyncFileLock;
typedef struct AsyncLock AsyncLock;

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
**       protected by the async.queueMutex. As are the async.ioError and
**       async.nFile variables.
**
**     * The async.pLock list and all AsyncLock and AsyncFileLock
**       structures must be protected by the async.lockMutex mutex.
**
**     * The file handles from the underlying system are not assumed to 
**       be thread safe.
**
**     * See the last two paragraphs under "The Writer Thread" for
**       an assumption to do with file-handle synchronization by the Os.
**
** Deadlock prevention:
**
**     There are three mutex used by the system: the "writer" mutex, 
**     the "queue" mutex and the "lock" mutex. Rules are:
**
**     * It is illegal to block on the writer mutex when any other mutex
**       are held, and 
**
**     * It is illegal to block on the queue mutex when the lock mutex
**       is held.
**
**     i.e. mutex's must be grabbed in the order "writer", "queue", "lock".
**
** File system operations (invoked by SQLite thread):
**
**     xOpen
**     xDelete
**     xFileExists
**
** File handle operations (invoked by SQLite thread):
**
**         asyncWrite, asyncClose, asyncTruncate, asyncSync 
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
**         asyncLock, asyncUnlock, asyncCheckReservedLock
**    
**     These primitives implement in-process locking using a hash table
**     on the file name.  Files are locked correctly for connections coming
**     from the same process.  But other processes cannot see these locks
**     and will therefore not honor them.
**
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
** as the following structure.
**
** Both async.ioError and async.nFile are protected by async.queueMutex.
*/
static struct TestAsyncStaticData {
  pthread_mutex_t lockMutex;   /* For access to aLock hash table */
  pthread_mutex_t queueMutex;  /* Mutex for access to write operation queue */
  pthread_mutex_t writerMutex; /* Prevents multiple writer threads */
  pthread_cond_t queueSignal;  /* For waking up sleeping writer thread */
  pthread_cond_t emptySignal;  /* Notify when the write queue is empty */
  AsyncWrite *pQueueFirst;     /* Next write operation to be processed */
  AsyncWrite *pQueueLast;      /* Last write operation on the list */
  AsyncLock *pLock;            /* Linked list of all AsyncLock structures */
  volatile int ioDelay;             /* Extra delay between write operations */
  volatile int writerHaltWhenIdle;  /* Writer thread halts when queue empty */
  volatile int writerHaltNow;       /* Writer thread halts after next op */
  int ioError;                 /* True if an IO error has occurred */
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
#define ASYNC_DELETE        5
#define ASYNC_OPENEXCLUSIVE 6
#define ASYNC_UNLOCK        7

/* Names of opcodes.  Used for debugging only.
** Make sure these stay in sync with the macros above!
*/
static const char *azOpcodeName[] = {
  "NOOP", "WRITE", "SYNC", "TRUNCATE", "CLOSE", "DELETE", "OPENEX", "UNLOCK"
};

/*
** Entries on the write-op queue are instances of the AsyncWrite
** structure, defined here.
**
** The interpretation of the iOffset and nByte variables varies depending 
** on the value of AsyncWrite.op:
**
** ASYNC_NOOP:
**     No values used.
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
** ASYNC_DELETE:
**     iOffset -> Contains the "syncDir" flag.
**     nByte   -> Number of bytes of zBuf points to (file name).
**
** ASYNC_OPENEXCLUSIVE:
**     iOffset -> Value of "delflag".
**     nByte   -> Number of bytes of zBuf points to (file name).
**
** ASYNC_UNLOCK:
**     nByte   -> Argument to sqlite3OsUnlock().
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
  sqlite_int64 iOffset;        /* See above */
  int nByte;          /* See above */
  char *zBuf;         /* Data to write to file (or NULL if op!=ASYNC_WRITE) */
  AsyncWrite *pNext;  /* Next write operation (to any file) */
};

/*
** An instance of this structure is created for each distinct open file 
** (i.e. if two handles are opened on the one file, only one of these
** structures is allocated) and stored in the async.aLock hash table. The
** keys for async.aLock are the full pathnames of the opened files.
**
** AsyncLock.pList points to the head of a linked list of AsyncFileLock
** structures, one for each handle currently open on the file.
**
** If the opened file is not a main-database (the SQLITE_OPEN_MAIN_DB is
** not passed to the sqlite3OsOpen() call), or if ENABLE_FILE_LOCKING is 
** not defined at compile time, variables AsyncLock.pFile and 
** AsyncLock.eLock are never used. Otherwise, pFile is a file handle
** opened on the file in question and used to obtain the file-system 
** locks required by database connections within this process.
**
** See comments above the asyncLock() function for more details on 
** the implementation of database locking used by this backend.
*/
struct AsyncLock {
  char *zFile;
  int nFile;
  sqlite3_file *pFile;
  int eLock;
  AsyncFileLock *pList;
  AsyncLock *pNext;           /* Next in linked list headed by async.pLock */
};

/*
** An instance of the following structure is allocated along with each
** AsyncFileData structure (see AsyncFileData.lock), but is only used if the
** file was opened with the SQLITE_OPEN_MAIN_DB.
*/
struct AsyncFileLock {
  int eLock;                /* Internally visible lock state (sqlite pov) */
  int eAsyncLock;           /* Lock-state with write-queue unlock */
  AsyncFileLock *pNext;
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
  AsyncFileLock lock;        /* Lock state for this handle */
  AsyncLock *pLock;          /* AsyncLock object for this file system entry */
  AsyncWrite closeOp;        /* Preallocated close operation */
};

/*
** The following async_XXX functions are debugging wrappers around the
** corresponding pthread_XXX functions:
**
**     pthread_mutex_lock();
**     pthread_mutex_unlock();
**     pthread_mutex_trylock();
**     pthread_cond_wait();
**
** It is illegal to pass any mutex other than those stored in the
** following global variables of these functions.
**
**     async.queueMutex
**     async.writerMutex
**     async.lockMutex
**
** If NDEBUG is defined, these wrappers do nothing except call the 
** corresponding pthreads function. If NDEBUG is not defined, then the
** following variables are used to store the thread-id (as returned
** by pthread_self()) currently holding the mutex, or 0 otherwise:
**
**     asyncdebug.queueMutexHolder
**     asyncdebug.writerMutexHolder
**     asyncdebug.lockMutexHolder
**
** These variables are used by some assert() statements that verify
** the statements made in the "Deadlock Prevention" notes earlier
** in this file.
*/
#ifndef NDEBUG

static struct TestAsyncDebugData {
  pthread_t lockMutexHolder;
  pthread_t queueMutexHolder;
  pthread_t writerMutexHolder;
} asyncdebug = {0, 0, 0};

/*
** Wrapper around pthread_mutex_lock(). Checks that we have not violated
** the anti-deadlock rules (see "Deadlock prevention" above).
*/
static int async_mutex_lock(pthread_mutex_t *pMutex){
  int iIdx;
  int rc;
  pthread_mutex_t *aMutex = (pthread_mutex_t *)(&async);
  pthread_t *aHolder = (pthread_t *)(&asyncdebug);

  /* The code in this 'ifndef NDEBUG' block depends on a certain alignment
   * of the variables in TestAsyncStaticData and TestAsyncDebugData. The
   * following assert() statements check that this has not been changed.
   *
   * Really, these only need to be run once at startup time.
   */
  assert(&(aMutex[0])==&async.lockMutex);
  assert(&(aMutex[1])==&async.queueMutex);
  assert(&(aMutex[2])==&async.writerMutex);
  assert(&(aHolder[0])==&asyncdebug.lockMutexHolder);
  assert(&(aHolder[1])==&asyncdebug.queueMutexHolder);
  assert(&(aHolder[2])==&asyncdebug.writerMutexHolder);

  assert( pthread_self()!=0 );
  for(iIdx=0; iIdx<3; iIdx++){
    if( pMutex==&aMutex[iIdx] ) break;

    /* This is the key assert(). Here we are checking that if the caller
     * is trying to block on async.writerMutex, neither of the other two
     * mutex are held. If the caller is trying to block on async.queueMutex,
     * lockMutex is not held.
     */
    assert(!pthread_equal(aHolder[iIdx], pthread_self()));
  }
  assert(iIdx<3);

  rc = pthread_mutex_lock(pMutex);
  if( rc==0 ){
    assert(aHolder[iIdx]==0);
    aHolder[iIdx] = pthread_self();
  }
  return rc;
}

/*
** Wrapper around pthread_mutex_unlock().
*/
static int async_mutex_unlock(pthread_mutex_t *pMutex){
  int iIdx;
  int rc;
  pthread_mutex_t *aMutex = (pthread_mutex_t *)(&async);
  pthread_t *aHolder = (pthread_t *)(&asyncdebug);

  for(iIdx=0; iIdx<3; iIdx++){
    if( pMutex==&aMutex[iIdx] ) break;
  }
  assert(iIdx<3);

  assert(pthread_equal(aHolder[iIdx], pthread_self()));
  aHolder[iIdx] = 0;
  rc = pthread_mutex_unlock(pMutex);
  assert(rc==0);

  return 0;
}

/*
** Wrapper around pthread_mutex_trylock().
*/
static int async_mutex_trylock(pthread_mutex_t *pMutex){
  int iIdx;
  int rc;
  pthread_mutex_t *aMutex = (pthread_mutex_t *)(&async);
  pthread_t *aHolder = (pthread_t *)(&asyncdebug);

  for(iIdx=0; iIdx<3; iIdx++){
    if( pMutex==&aMutex[iIdx] ) break;
  }
  assert(iIdx<3);

  rc = pthread_mutex_trylock(pMutex);
  if( rc==0 ){
    assert(aHolder[iIdx]==0);
    aHolder[iIdx] = pthread_self();
  }
  return rc;
}

/*
** Wrapper around pthread_cond_wait().
*/
static int async_cond_wait(pthread_cond_t *pCond, pthread_mutex_t *pMutex){
  int iIdx;
  int rc;
  pthread_mutex_t *aMutex = (pthread_mutex_t *)(&async);
  pthread_t *aHolder = (pthread_t *)(&asyncdebug);

  for(iIdx=0; iIdx<3; iIdx++){
    if( pMutex==&aMutex[iIdx] ) break;
  }
  assert(iIdx<3);

  assert(pthread_equal(aHolder[iIdx],pthread_self()));
  aHolder[iIdx] = 0;
  rc = pthread_cond_wait(pCond, pMutex);
  if( rc==0 ){
    aHolder[iIdx] = pthread_self();
  }
  return rc;
}

/*
** Assert that the mutex is held by the current thread.
*/
static void assert_mutex_is_held(pthread_mutex_t *pMutex){
  int iIdx;
  pthread_mutex_t *aMutex = (pthread_mutex_t *)(&async);
  pthread_t *aHolder = (pthread_t *)(&asyncdebug);

  for(iIdx=0; iIdx<3; iIdx++){
    if( pMutex==&aMutex[iIdx] ) break;
  }
  assert(iIdx<3);
  assert( aHolder[iIdx]==pthread_self() );
}

/* Call our async_XX wrappers instead of selected pthread_XX functions */
#define pthread_mutex_lock    async_mutex_lock
#define pthread_mutex_unlock  async_mutex_unlock
#define pthread_mutex_trylock async_mutex_trylock
#define pthread_cond_wait     async_cond_wait

#else    /* if defined(NDEBUG) */

#define assert_mutex_is_held(X)    /* A no-op when not debugging */

#endif   /* !defined(NDEBUG) */

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
  if( pWrite->op!=ASYNC_UNLOCK ){
    pthread_mutex_lock(&async.queueMutex);
  }

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
  }

  /* Drop the queue mutex */
  if( pWrite->op!=ASYNC_UNLOCK ){
    pthread_mutex_unlock(&async.queueMutex);
  }

  /* The writer thread might have been idle because there was nothing
  ** on the write-op queue for it to do.  So wake it up. */
  pthread_cond_signal(&async.queueSignal);
}

/*
** Increment async.nFile in a thread-safe manner.
*/
static void incrOpenFileCount(void){
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
  sqlite3_int64 iOffset, 
  int nByte,
  const char *zByte
){
  AsyncWrite *p;
  if( op!=ASYNC_CLOSE && async.ioError ){
    return async.ioError;
  }
  p = sqlite3_malloc(sizeof(AsyncWrite) + (zByte?nByte:0));
  if( !p ){
    /* The upper layer does not expect operations like OsWrite() to
    ** return SQLITE_NOMEM. This is partly because under normal conditions
    ** SQLite is required to do rollback without calling malloc(). So
    ** if malloc() fails here, treat it as an I/O error. The above
    ** layer knows how to handle that.
    */
    return SQLITE_IOERR;
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

  /* Unlock the file, if it is locked */
  pthread_mutex_lock(&async.lockMutex);
  p->lock.eLock = 0;
  pthread_mutex_unlock(&async.lockMutex);

  addAsyncWrite(&p->closeOp);
  return SQLITE_OK;
}

/*
** Implementation of sqlite3OsWrite() for asynchronous files. Instead of 
** writing to the underlying file, this function adds an entry to the end of
** the global AsyncWrite list. Either SQLITE_OK or SQLITE_NOMEM may be
** returned.
*/
static int asyncWrite(
  sqlite3_file *pFile, 
  const void *pBuf, 
  int amt, 
  sqlite3_int64 iOff
){
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
static int asyncRead(
  sqlite3_file *pFile, 
  void *zOut, 
  int iAmt, 
  sqlite3_int64 iOffset
){
  AsyncFileData *p = ((AsyncFile *)pFile)->pData;
  int rc = SQLITE_OK;
  sqlite3_int64 filesize;
  int nRead;
  sqlite3_file *pBase = p->pBaseRead;

  /* Grab the write queue mutex for the duration of the call */
  pthread_mutex_lock(&async.queueMutex);

  /* If an I/O error has previously occurred in this virtual file 
  ** system, then all subsequent operations fail.
  */
  if( async.ioError!=SQLITE_OK ){
    rc = async.ioError;
    goto asyncread_out;
  }

  if( pBase->pMethods ){
    rc = pBase->pMethods->xFileSize(pBase, &filesize);
    if( rc!=SQLITE_OK ){
      goto asyncread_out;
    }
    nRead = MIN(filesize - iOffset, iAmt);
    if( nRead>0 ){
      rc = pBase->pMethods->xRead(pBase, zOut, nRead, iOffset);
      ASYNC_TRACE(("READ %s %d bytes at %d\n", p->zName, nRead, iOffset));
    }
  }

  if( rc==SQLITE_OK ){
    AsyncWrite *pWrite;
    char *zName = p->zName;

    for(pWrite=async.pQueueFirst; pWrite; pWrite = pWrite->pNext){
      if( pWrite->op==ASYNC_WRITE && (
        (pWrite->pFileData==p) ||
        (zName && pWrite->pFileData->zName==zName)
      )){
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
static int asyncTruncate(sqlite3_file *pFile, sqlite3_int64 nByte){
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
int asyncFileSize(sqlite3_file *pFile, sqlite3_int64 *piSize){
  AsyncFileData *p = ((AsyncFile *)pFile)->pData;
  int rc = SQLITE_OK;
  sqlite3_int64 s = 0;
  sqlite3_file *pBase;

  pthread_mutex_lock(&async.queueMutex);

  /* Read the filesystem size from the base file. If pBaseRead is NULL, this
  ** means the file hasn't been opened yet. In this case all relevant data 
  ** must be in the write-op queue anyway, so we can omit reading from the
  ** file-system.
  */
  pBase = p->pBaseRead;
  if( pBase->pMethods ){
    rc = pBase->pMethods->xFileSize(pBase, &s);
  }

  if( rc==SQLITE_OK ){
    AsyncWrite *pWrite;
    for(pWrite=async.pQueueFirst; pWrite; pWrite = pWrite->pNext){
      if( pWrite->op==ASYNC_DELETE 
       && p->zName 
       && strcmp(p->zName, pWrite->zBuf)==0 
      ){
        s = 0;
      }else if( pWrite->pFileData && (
          (pWrite->pFileData==p) 
       || (p->zName && pWrite->pFileData->zName==p->zName) 
      )){
        switch( pWrite->op ){
          case ASYNC_WRITE:
            s = MAX(pWrite->iOffset + (sqlite3_int64)(pWrite->nByte), s);
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
** Lock or unlock the actual file-system entry.
*/
static int getFileLock(AsyncLock *pLock){
  int rc = SQLITE_OK;
  AsyncFileLock *pIter;
  int eRequired = 0;

  if( pLock->pFile ){
    for(pIter=pLock->pList; pIter; pIter=pIter->pNext){
      assert(pIter->eAsyncLock>=pIter->eLock);
      if( pIter->eAsyncLock>eRequired ){
        eRequired = pIter->eAsyncLock;
        assert(eRequired>=0 && eRequired<=SQLITE_LOCK_EXCLUSIVE);
      }
    }

    if( eRequired>pLock->eLock ){
      rc = pLock->pFile->pMethods->xLock(pLock->pFile, eRequired);
      if( rc==SQLITE_OK ){
        pLock->eLock = eRequired;
      }
    }
    else if( eRequired<pLock->eLock && eRequired<=SQLITE_LOCK_SHARED ){
      rc = pLock->pFile->pMethods->xUnlock(pLock->pFile, eRequired);
      if( rc==SQLITE_OK ){
        pLock->eLock = eRequired;
      }
    }
  }

  return rc;
}

/*
** Return the AsyncLock structure from the global async.pLock list 
** associated with the file-system entry identified by path zName 
** (a string of nName bytes). If no such structure exists, return 0.
*/
static AsyncLock *findLock(const char *zName, int nName){
  AsyncLock *p = async.pLock;
  while( p && (p->nFile!=nName || memcmp(p->zFile, zName, nName)) ){
    p = p->pNext;
  }
  return p;
}

/*
** The following two methods - asyncLock() and asyncUnlock() - are used
** to obtain and release locks on database files opened with the
** asynchronous backend.
*/
static int asyncLock(sqlite3_file *pFile, int eLock){
  int rc = SQLITE_OK;
  AsyncFileData *p = ((AsyncFile *)pFile)->pData;

  if( p->zName ){
    pthread_mutex_lock(&async.lockMutex);
    if( p->lock.eLock<eLock ){
      AsyncLock *pLock = p->pLock;
      AsyncFileLock *pIter;
      assert(pLock && pLock->pList);
      for(pIter=pLock->pList; pIter; pIter=pIter->pNext){
        if( pIter!=&p->lock && (
          (eLock==SQLITE_LOCK_EXCLUSIVE && pIter->eLock>=SQLITE_LOCK_SHARED) ||
          (eLock==SQLITE_LOCK_PENDING && pIter->eLock>=SQLITE_LOCK_RESERVED) ||
          (eLock==SQLITE_LOCK_RESERVED && pIter->eLock>=SQLITE_LOCK_RESERVED) ||
          (eLock==SQLITE_LOCK_SHARED && pIter->eLock>=SQLITE_LOCK_PENDING)
        )){
          rc = SQLITE_BUSY;
        }
      }
      if( rc==SQLITE_OK ){
        p->lock.eLock = eLock;
        p->lock.eAsyncLock = MAX(p->lock.eAsyncLock, eLock);
      }
      assert(p->lock.eAsyncLock>=p->lock.eLock);
      if( rc==SQLITE_OK ){
        rc = getFileLock(pLock);
      }
    }
    pthread_mutex_unlock(&async.lockMutex);
  }

  ASYNC_TRACE(("LOCK %d (%s) rc=%d\n", eLock, p->zName, rc));
  return rc;
}
static int asyncUnlock(sqlite3_file *pFile, int eLock){
  int rc = SQLITE_OK;
  AsyncFileData *p = ((AsyncFile *)pFile)->pData;
  if( p->zName ){
    AsyncFileLock *pLock = &p->lock;
    pthread_mutex_lock(&async.queueMutex);
    pthread_mutex_lock(&async.lockMutex);
    pLock->eLock = MIN(pLock->eLock, eLock);
    rc = addNewAsyncWrite(p, ASYNC_UNLOCK, 0, eLock, 0);
    pthread_mutex_unlock(&async.lockMutex);
    pthread_mutex_unlock(&async.queueMutex);
  }
  return rc;
}

/*
** This function is called when the pager layer first opens a database file
** and is checking for a hot-journal.
*/
static int asyncCheckReservedLock(sqlite3_file *pFile, int *pResOut){
  int ret = 0;
  AsyncFileLock *pIter;
  AsyncFileData *p = ((AsyncFile *)pFile)->pData;

  pthread_mutex_lock(&async.lockMutex);
  for(pIter=p->pLock->pList; pIter; pIter=pIter->pNext){
    if( pIter->eLock>=SQLITE_LOCK_RESERVED ){
      ret = 1;
    }
  }
  pthread_mutex_unlock(&async.lockMutex);

  ASYNC_TRACE(("CHECK-LOCK %d (%s)\n", ret, p->zName));
  *pResOut = ret;
  return SQLITE_OK;
}

/* 
** sqlite3_file_control() implementation.
*/
static int asyncFileControl(sqlite3_file *id, int op, void *pArg){
  switch( op ){
    case SQLITE_FCNTL_LOCKSTATE: {
      pthread_mutex_lock(&async.lockMutex);
      *(int*)pArg = ((AsyncFile*)id)->pData->lock.eLock;
      pthread_mutex_unlock(&async.lockMutex);
      return SQLITE_OK;
    }
  }
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

static int unlinkAsyncFile(AsyncFileData *pData){
  AsyncFileLock **ppIter;
  int rc = SQLITE_OK;

  if( pData->zName ){
    AsyncLock *pLock = pData->pLock;
    for(ppIter=&pLock->pList; *ppIter; ppIter=&((*ppIter)->pNext)){
      if( (*ppIter)==&pData->lock ){
        *ppIter = pData->lock.pNext;
        break;
      }
    }
    if( !pLock->pList ){
      AsyncLock **pp;
      if( pLock->pFile ){
        pLock->pFile->pMethods->xClose(pLock->pFile);
      }
      for(pp=&async.pLock; *pp!=pLock; pp=&((*pp)->pNext));
      *pp = pLock->pNext;
      sqlite3_free(pLock);
    }else{
      rc = getFileLock(pLock);
    }
  }

  return rc;
}

/*
** The parameter passed to this function is a copy of a 'flags' parameter
** passed to this modules xOpen() method. This function returns true
** if the file should be opened asynchronously, or false if it should
** be opened immediately.
**
** If the file is to be opened asynchronously, then asyncOpen() will add
** an entry to the event queue and the file will not actually be opened
** until the event is processed. Otherwise, the file is opened directly
** by the caller.
*/
static int doAsynchronousOpen(int flags){
  return (flags&SQLITE_OPEN_CREATE) && (
      (flags&SQLITE_OPEN_MAIN_JOURNAL) ||
      (flags&SQLITE_OPEN_TEMP_JOURNAL) ||
      (flags&SQLITE_OPEN_DELETEONCLOSE)
  );
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
  int nName = 0;
  int rc = SQLITE_OK;
  int nByte;
  AsyncFileData *pData;
  AsyncLock *pLock = 0;
  char *z;
  int isAsyncOpen = doAsynchronousOpen(flags);

  /* If zName is NULL, then the upper layer is requesting an anonymous file */
  if( zName ){
    nName = strlen(zName)+1;
  }

  nByte = (
    sizeof(AsyncFileData) +        /* AsyncFileData structure */
    2 * pVfs->szOsFile +           /* AsyncFileData.pBaseRead and pBaseWrite */
    nName                          /* AsyncFileData.zName */
  ); 
  z = sqlite3_malloc(nByte);
  if( !z ){
    return SQLITE_NOMEM;
  }
  memset(z, 0, nByte);
  pData = (AsyncFileData*)z;
  z += sizeof(pData[0]);
  pData->pBaseRead = (sqlite3_file*)z;
  z += pVfs->szOsFile;
  pData->pBaseWrite = (sqlite3_file*)z;
  pData->closeOp.pFileData = pData;
  pData->closeOp.op = ASYNC_CLOSE;

  if( zName ){
    z += pVfs->szOsFile;
    pData->zName = z;
    pData->nName = nName;
    memcpy(pData->zName, zName, nName);
  }

  if( !isAsyncOpen ){
    int flagsout;
    rc = pVfs->xOpen(pVfs, pData->zName, pData->pBaseRead, flags, &flagsout);
    if( rc==SQLITE_OK && (flagsout&SQLITE_OPEN_READWRITE) ){
      rc = pVfs->xOpen(pVfs, pData->zName, pData->pBaseWrite, flags, 0);
    }
    if( pOutFlags ){
      *pOutFlags = flagsout;
    }
  }

  pthread_mutex_lock(&async.lockMutex);

  if( zName && rc==SQLITE_OK ){
    pLock = findLock(pData->zName, pData->nName);
    if( !pLock ){
      int nByte = pVfs->szOsFile + sizeof(AsyncLock) + pData->nName + 1; 
      pLock = (AsyncLock *)sqlite3_malloc(nByte);
      if( pLock ){
        memset(pLock, 0, nByte);
#ifdef ENABLE_FILE_LOCKING
        if( flags&SQLITE_OPEN_MAIN_DB ){
          pLock->pFile = (sqlite3_file *)&pLock[1];
          rc = pVfs->xOpen(pVfs, pData->zName, pLock->pFile, flags, 0);
          if( rc!=SQLITE_OK ){
            sqlite3_free(pLock);
            pLock = 0;
          }
        }
#endif
        if( pLock ){
          pLock->nFile = pData->nName;
          pLock->zFile = &((char *)(&pLock[1]))[pVfs->szOsFile];
          memcpy(pLock->zFile, pData->zName, pLock->nFile);
          pLock->pNext = async.pLock;
          async.pLock = pLock;
        }
      }else{
        rc = SQLITE_NOMEM;
      }
    }
  }

  if( rc==SQLITE_OK ){
    p->pMethod = &async_methods;
    p->pData = pData;

    /* Link AsyncFileData.lock into the linked list of 
    ** AsyncFileLock structures for this file.
    */
    if( zName ){
      pData->lock.pNext = pLock->pList;
      pLock->pList = &pData->lock;
      pData->zName = pLock->zFile;
    }
  }else{
    if( pData->pBaseRead->pMethods ){
      pData->pBaseRead->pMethods->xClose(pData->pBaseRead);
    }
    if( pData->pBaseWrite->pMethods ){
      pData->pBaseWrite->pMethods->xClose(pData->pBaseWrite);
    }
    sqlite3_free(pData);
  }

  pthread_mutex_unlock(&async.lockMutex);

  if( rc==SQLITE_OK ){
    incrOpenFileCount();
    pData->pLock = pLock;
  }

  if( rc==SQLITE_OK && isAsyncOpen ){
    rc = addNewAsyncWrite(pData, ASYNC_OPENEXCLUSIVE, (sqlite3_int64)flags,0,0);
    if( rc==SQLITE_OK ){
      if( pOutFlags ) *pOutFlags = flags;
    }else{
      pthread_mutex_lock(&async.lockMutex);
      unlinkAsyncFile(pData);
      pthread_mutex_unlock(&async.lockMutex);
      sqlite3_free(pData);
    }
  }
  if( rc!=SQLITE_OK ){
    p->pMethod = 0;
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
static int asyncAccess(
  sqlite3_vfs *pAsyncVfs, 
  const char *zName, 
  int flags,
  int *pResOut
){
  int rc;
  int ret;
  AsyncWrite *p;
  sqlite3_vfs *pVfs = (sqlite3_vfs *)pAsyncVfs->pAppData;

  assert(flags==SQLITE_ACCESS_READWRITE 
      || flags==SQLITE_ACCESS_READ 
      || flags==SQLITE_ACCESS_EXISTS 
  );

  pthread_mutex_lock(&async.queueMutex);
  rc = pVfs->xAccess(pVfs, zName, flags, &ret);
  if( rc==SQLITE_OK && flags==SQLITE_ACCESS_EXISTS ){
    for(p=async.pQueueFirst; p; p = p->pNext){
      if( p->op==ASYNC_DELETE && 0==strcmp(p->zBuf, zName) ){
        ret = 0;
      }else if( p->op==ASYNC_OPENEXCLUSIVE 
             && p->pFileData->zName
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
  *pResOut = ret;
  return rc;
}

/*
** Fill in zPathOut with the full path to the file identified by zPath.
*/
static int asyncFullPathname(
  sqlite3_vfs *pAsyncVfs, 
  const char *zPath, 
  int nPathOut,
  char *zPathOut
){
  int rc;
  sqlite3_vfs *pVfs = (sqlite3_vfs *)pAsyncVfs->pAppData;
  rc = pVfs->xFullPathname(pVfs, zPath, nPathOut, zPathOut);

  /* Because of the way intra-process file locking works, this backend
  ** needs to return a canonical path. The following block assumes the
  ** file-system uses unix style paths. 
  */
  if( rc==SQLITE_OK ){
    int i, j;
    int n = nPathOut;
    char *z = zPathOut;
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
  }

  return rc;
}
static void *asyncDlOpen(sqlite3_vfs *pAsyncVfs, const char *zPath){
  sqlite3_vfs *pVfs = (sqlite3_vfs *)pAsyncVfs->pAppData;
  return pVfs->xDlOpen(pVfs, zPath);
}
static void asyncDlError(sqlite3_vfs *pAsyncVfs, int nByte, char *zErrMsg){
  sqlite3_vfs *pVfs = (sqlite3_vfs *)pAsyncVfs->pAppData;
  pVfs->xDlError(pVfs, nByte, zErrMsg);
}
static void (*asyncDlSym(
  sqlite3_vfs *pAsyncVfs, 
  void *pHandle, 
  const char *zSymbol
))(void){
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
    }
  }else{
    if( async_vfs.pAppData ){
      sqlite3_vfs_unregister(&async_vfs);
      async_vfs.pAppData = 0;
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
static void *asyncWriterThread(void *pIsStarted){
  sqlite3_vfs *pVfs = (sqlite3_vfs *)(async_vfs.pAppData);
  AsyncWrite *p = 0;
  int rc = SQLITE_OK;
  int holdingMutex = 0;

  if( pthread_mutex_trylock(&async.writerMutex) ){
    return 0;
  }
  (*(int *)pIsStarted) = 1;
  while( async.writerHaltNow==0 ){
    int doNotFree = 0;
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
        rc = pBase->pMethods->xWrite(pBase, (void *)(p->zBuf), p->nByte, p->iOffset);
        break;

      case ASYNC_SYNC:
        assert( pBase );
        ASYNC_TRACE(("SYNC %s\n", p->pFileData->zName));
        rc = pBase->pMethods->xSync(pBase, p->nByte);
        break;

      case ASYNC_TRUNCATE:
        assert( pBase );
        ASYNC_TRACE(("TRUNCATE %s to %d bytes\n", 
                p->pFileData->zName, p->iOffset));
        rc = pBase->pMethods->xTruncate(pBase, p->iOffset);
        break;

      case ASYNC_CLOSE: {
        AsyncFileData *pData = p->pFileData;
        ASYNC_TRACE(("CLOSE %s\n", p->pFileData->zName));
        if( pData->pBaseWrite->pMethods ){
          pData->pBaseWrite->pMethods->xClose(pData->pBaseWrite);
        }
        if( pData->pBaseRead->pMethods ){
          pData->pBaseRead->pMethods->xClose(pData->pBaseRead);
        }

        /* Unlink AsyncFileData.lock from the linked list of AsyncFileLock 
        ** structures for this file. Obtain the async.lockMutex mutex 
        ** before doing so.
        */
        pthread_mutex_lock(&async.lockMutex);
        rc = unlinkAsyncFile(pData);
        pthread_mutex_unlock(&async.lockMutex);

        if( !holdingMutex ){
          pthread_mutex_lock(&async.queueMutex);
          holdingMutex = 1;
        }
        assert_mutex_is_held(&async.queueMutex);
        async.pQueueFirst = p->pNext;
        sqlite3_free(pData);
        doNotFree = 1;
        break;
      }

      case ASYNC_UNLOCK: {
        AsyncWrite *pIter;
        AsyncFileData *pData = p->pFileData;
        int eLock = p->nByte;

        /* When a file is locked by SQLite using the async backend, it is 
        ** locked within the 'real' file-system synchronously. When it is
        ** unlocked, an ASYNC_UNLOCK event is added to the write-queue to
        ** unlock the file asynchronously. The design of the async backend
        ** requires that the 'real' file-system file be locked from the
        ** time that SQLite first locks it (and probably reads from it)
        ** until all asynchronous write events that were scheduled before
        ** SQLite unlocked the file have been processed.
        **
        ** This is more complex if SQLite locks and unlocks the file multiple
        ** times in quick succession. For example, if SQLite does: 
        ** 
        **   lock, write, unlock, lock, write, unlock
        **
        ** Each "lock" operation locks the file immediately. Each "write" 
        ** and "unlock" operation adds an event to the event queue. If the
        ** second "lock" operation is performed before the first "unlock"
        ** operation has been processed asynchronously, then the first
        ** "unlock" cannot be safely processed as is, since this would mean
        ** the file was unlocked when the second "write" operation is
        ** processed. To work around this, when processing an ASYNC_UNLOCK
        ** operation, SQLite:
        **
        **   1) Unlocks the file to the minimum of the argument passed to
        **      the xUnlock() call and the current lock from SQLite's point
        **      of view, and
        **
        **   2) Only unlocks the file at all if this event is the last
        **      ASYNC_UNLOCK event on this file in the write-queue.
        */ 
        assert( holdingMutex==1 );
        assert( async.pQueueFirst==p );
        for(pIter=async.pQueueFirst->pNext; pIter; pIter=pIter->pNext){
          if( pIter->pFileData==pData && pIter->op==ASYNC_UNLOCK ) break;
        }
        if( !pIter ){
          pthread_mutex_lock(&async.lockMutex);
          pData->lock.eAsyncLock = MIN(
              pData->lock.eAsyncLock, MAX(pData->lock.eLock, eLock)
          );
          assert(pData->lock.eAsyncLock>=pData->lock.eLock);
          rc = getFileLock(pData->pLock);
          pthread_mutex_unlock(&async.lockMutex);
        }
        break;
      }

      case ASYNC_DELETE:
        ASYNC_TRACE(("DELETE %s\n", p->zBuf));
        rc = pVfs->xDelete(pVfs, p->zBuf, (int)p->iOffset);
        break;

      case ASYNC_OPENEXCLUSIVE: {
        int flags = (int)p->iOffset;
        AsyncFileData *pData = p->pFileData;
        ASYNC_TRACE(("OPEN %s flags=%d\n", p->zBuf, (int)p->iOffset));
        assert(pData->pBaseRead->pMethods==0 && pData->pBaseWrite->pMethods==0);
        rc = pVfs->xOpen(pVfs, pData->zName, pData->pBaseRead, flags, 0);
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
    if( !doNotFree ){
      assert_mutex_is_held(&async.queueMutex);
      async.pQueueFirst = p->pNext;
      sqlite3_free(p);
    }
    assert( holdingMutex );

    /* An IO error has occurred. We cannot report the error back to the
    ** connection that requested the I/O since the error happened 
    ** asynchronously.  The connection has already moved on.  There 
    ** really is nobody to report the error to.
    **
    ** The file for which the error occurred may have been a database or
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

    if( async.ioError && !async.pQueueFirst ){
      pthread_mutex_lock(&async.lockMutex);
      if( 0==async.pLock ){
        async.ioError = SQLITE_OK;
      }
      pthread_mutex_unlock(&async.lockMutex);
    }

    /* Drop the queue mutex before continuing to the next write operation
    ** in order to give other threads a chance to work with the write queue.
    */
    if( !async.pQueueFirst || !async.ioError ){
      pthread_mutex_unlock(&async.queueMutex);
      holdingMutex = 0;
      if( async.ioDelay>0 ){
        pVfs->xSleep(pVfs, async.ioDelay);
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
  volatile int isStarted = 0;
  rc = pthread_create(&x, 0, asyncWriterThread, (void *)&isStarted);
  if( rc ){
    Tcl_AppendResult(interp, "failed to create the thread", 0);
    return TCL_ERROR;
  }
  pthread_detach(x);
  while( isStarted==0 ){
    sched_yield();
  }
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


#endif  /* SQLITE_OS_UNIX and SQLITE_THREADSAFE */

/*
** This routine registers the custom TCL commands defined in this
** module.  This should be the only procedure visible from outside
** of this module.
*/
int Sqlitetestasync_Init(Tcl_Interp *interp){
#if SQLITE_OS_UNIX && SQLITE_THREADSAFE
  Tcl_CreateObjCommand(interp,"sqlite3async_enable",testAsyncEnable,0,0);
  Tcl_CreateObjCommand(interp,"sqlite3async_halt",testAsyncHalt,0,0);
  Tcl_CreateObjCommand(interp,"sqlite3async_delay",testAsyncDelay,0,0);
  Tcl_CreateObjCommand(interp,"sqlite3async_start",testAsyncStart,0,0);
  Tcl_CreateObjCommand(interp,"sqlite3async_wait",testAsyncWait,0,0);
  Tcl_LinkVar(interp, "sqlite3async_trace",
      (char*)&sqlite3async_trace, TCL_LINK_INT);
#endif  /* SQLITE_OS_UNIX and SQLITE_THREADSAFE */
  return TCL_OK;
}

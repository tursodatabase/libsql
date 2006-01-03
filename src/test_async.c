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

#define MIN(x,y) ((x)<(y)?(x):(y))
#define MAX(x,y) ((x)>(y)?(x):(y))

typedef struct AsyncWrite AsyncWrite;
typedef struct AsyncFile AsyncFile;

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
**       protected by the sqlite3Os mutex functions.
**     * The file handles from the underlying system are assumed not to 
**       be thread safe.
**     * See the last paragraph under "sqlite3_async_flush() Threads" for
**       an assumption to do with file-handle synchronization by the Os.
**
** File system operations (invoked by SQLite thread):
**
**     xOpenXXX (three versions)
**     xDelete
**     xFileExists
**
**     Todo:
**         xSyncDirectory
**
** File handle operations (invoked by SQLite thread):
**
**     The following operations add an entry to the global write-op list. They
**     prepare the entry, aquire the mutex momentarily while list pointers are 
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
**     Since this variable is never accessed by an sqlite3_async_flush() thread,
**     this function does not require the mutex. Actual calls to OsSeek() take 
**     place just before OsWrite() or OsRead(), which are always protected by 
**     the mutex.
**    
**         asyncSeek.
**
** sqlite3_async_flush() (any thread):
**
**     A pseudo-mutex (a global boolean variable) is used to make sure only 
**     one thread is inside the sqlite3_async_flush() thread at any one time.
**     If the variable is set when a thread enters _flush(), then it 
**     immediately returns SQLITE_BUSY. Otherwise, it sets the variable, 
**     executes the body of the function, and clears the variable just before
**     returning. Both read and write access to said global variable 
**     (sqlite3_asyncIoBusy) is protected by sqlite3Os mutex, of course.
**
**     Inside sqlite3_async_flush() is a loop that works like this:
**
**         WHILE (write-op list is not empty)
**             Do IO operation at head of write-op list
**             Remove entry from head of write-op list
**         END WHILE
**
**     The mutex is always obtained during the <write-op list is not empty>
**     test, and when the entry is removed from the head of the write-op 
**     list. Sometimes it is held for the interim period (while the IO is
**     performed), and sometimes it is relinquished. It is relinquished if
**     (a) the IO op is an ASYNC_CLOSE or (b) when the file handle was 
**     opened, two of the underlying systems handles were opened on the
**     same file-system entry.
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
** First and last elements of the global write-op list. 
**
** Whenever an OsWrite(), OsSync(), OsTrunc() or OsClose() operation is
** requested, instead of performing the file IO immediately, a new AsyncWrite
** structure is allocated and added to the global linked list that starts at
** sqlite3_asyncListFirst. The next time to sqlite3_async_flush() is called,
** all operations are realised and the list elements deleted.
*/
static AsyncWrite *sqlite3_asyncListFirst = 0;
static AsyncWrite *sqlite3_asyncListLast = 0;

/* True after an IO error has occured */
/* static int *sqlite3_asyncIoError = 0; */

/* True if some thread is currently inside sqlite3_async_flush() */
static int sqlite3_asyncIoBusy = 0;

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
  sqlite3_os_enter_mutex();
  assert( !pWrite->pNext );
  if( sqlite3_asyncListLast ){
    assert( sqlite3_asyncListFirst );
    sqlite3_asyncListLast->pNext = pWrite;
  }else{
    sqlite3_asyncListFirst = pWrite;
  }
  sqlite3_asyncListLast = pWrite;
  sqlite3_os_leave_mutex();
}

/*
** The caller should already hold the mutex when this is called.
*/
static void removeAsyncWrite(AsyncWrite *p){
  assert( p==sqlite3_asyncListFirst );
  assert( sqlite3_asyncListLast );
  if( sqlite3_asyncListFirst==sqlite3_asyncListLast ){
    assert( !sqlite3_asyncListFirst->pNext );
    sqlite3_asyncListLast = 0;
  }
  sqlite3_asyncListFirst = sqlite3_asyncListFirst->pNext;
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
  AsyncWrite *p = sqlite3Os.xMalloc(sizeof(AsyncWrite) + (zByte?nByte:0));
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

  /* Grab the mutex for the duration of the call */
  sqlite3_os_enter_mutex();

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

    for(p=sqlite3_asyncListFirst; p; p = p->pNext){
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
  sqlite3_os_leave_mutex();
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
  sqlite3_os_enter_mutex();

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
    for(p=sqlite3_asyncListFirst; p; p = p->pNext){
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
  sqlite3_os_leave_mutex();
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
** The three file-open functions for the underlying file system layer.
*/
static int (*xOrigOpenReadWrite)(const char*, OsFile**, int*) = 0;
static int (*xOrigOpenExclusive)(const char*, OsFile**, int) = 0;
static int (*xOrigOpenReadOnly)(const char*, OsFile**) = 0;

/*
** Pointers to the original versions of other overridden file-system 
** operations.
*/
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

  p = (AsyncFile *)sqlite3Os.xMalloc(sizeof(AsyncFile));
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
      sqlite3Os.xFree(pFile);
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
** Implementation of sqlite3Os.xDelete. Add an entry to the end of the 
** write-op queue to perform the delete.
*/
static int asyncDelete(const char *z){
  return addNewAsyncWrite(0, ASYNC_DELETE, 0, strlen(z)+1, z);
}

/*
** Implementation of sqlite3Os.xDelete. Add an entry to the end of the 
** write-op queue to perform the delete.
*/
static int asyncSyncDirectory(const char *z){
  return addNewAsyncWrite(0, ASYNC_SYNCDIRECTORY, 0, strlen(z)+1, z);
}

/*
** Implementation of sqlite3Os.xFileExists. Return true if file 'z' exists
** in the file system. 
**
** This method holds the mutex from start to finish.
*/
static int asyncFileExists(const char *z){
  int ret;
  AsyncWrite *p;
  sqlite3_os_enter_mutex();

  /* See if the real file system contains the specified file.  */
  ret = xOrigFileExists(z);
  
  for(p=sqlite3_asyncListFirst; p; p = p->pNext){
    if( p->op==ASYNC_DELETE && 0==strcmp(p->zBuf, z) ){
      ret = 0;
    }else if( p->op==ASYNC_OPENEXCLUSIVE && 0==strcmp(p->zBuf, z) ){
      ret = 1;
    }
  }

  sqlite3_os_leave_mutex();
  return ret;
}

/*
** The following routine is one of two exported symbols in this module (along
** with sqlite3_async_flush(), see below). This routine should be called
** once to enable the asynchronous IO features implemented in this file. If 
** the features are successfully enabled (or if they have already been 
** enabled) then SQLITE_OK is returned. Otherwise, SQLITE_MISUSE.
*/
int sqlite3_async_enable(void){
  if( xOrigOpenReadWrite==0 ){
#define ROUTINE(a,b,c) {(void**)&a,SQLITE_OS_ROUTINE_ ## b,(void *)c}
    struct ReplacementOp {
      void ** pOldRoutine;
      int eRoutine;
      void * pNewRoutine;
    } aRoutines[] = {
      ROUTINE(xOrigOpenReadWrite, OPENREADWRITE, asyncOpenReadWrite),
      ROUTINE(xOrigOpenReadOnly, OPENREADONLY, asyncOpenReadOnly), 
      ROUTINE(xOrigOpenExclusive, OPENEXCLUSIVE, asyncOpenExclusive), 
      ROUTINE(xOrigDelete, DELETE, asyncDelete), 
      ROUTINE(xOrigFileExists, FILEEXISTS, asyncFileExists), 
      ROUTINE(xOrigSyncDirectory, SYNCDIRECTORY, asyncSyncDirectory)
    };
#undef ROUTINE
    int i;

    sqlite3_os_enter_mutex();
    for(i=0; i<sizeof(aRoutines)/sizeof(aRoutines[0]); i++){
      struct ReplacementOp *p = &aRoutines[i];
      *(p->pOldRoutine) = sqlite3_os_routine_set(p->eRoutine, p->pNewRoutine);
    }
    sqlite3_os_leave_mutex();
  }
  return SQLITE_OK;
}

/* 
** This function is called externally to perform queued write and sync
** operations. It returns when an IO error occurs or there are no more queued
** operations to perform.
*/
int sqlite3_async_flush(void){
  AsyncWrite *p = 0;
  int rc = SQLITE_OK;

  /* Grab the mutex and set the sqlite3_asyncIoBusy flag to make sure this
  ** is the only thread performing an sqlite3_async_flush() at this time.
  ** Or, if some other thread is already inside this function, return 
  ** SQLITE_BUSY to the caller.
  */
  sqlite3_os_enter_mutex();
  if( sqlite3_asyncIoBusy ){
    sqlite3_os_leave_mutex();
    return SQLITE_BUSY;
  }
  sqlite3_asyncIoBusy = 1;

  while( (p = sqlite3_asyncListFirst) && rc==SQLITE_OK ){
    int isInsideMutex = 1;

    /* Right now this thread is holding the global mutex. Variable 'p' points
    ** to the first entry in the write-op queue. In the general case, we
    ** hold on to the mutex for the entire body of the loop. 
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
    OsFile *pBase = 0;
    if( p->pFile ){
      pBase = p->pFile->pBaseWrite;
      if( 
        p->op==ASYNC_CLOSE || 
        p->op==ASYNC_OPENEXCLUSIVE ||
        (pBase && (p->op==ASYNC_SYNC || p->op==ASYNC_WRITE) ) 
      ){
        sqlite3_os_leave_mutex();
        isInsideMutex = 0;
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
        sqlite3Os.xFree(p->pFile);
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

        sqlite3_os_enter_mutex();
        isInsideMutex = 1;
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
    if( !isInsideMutex ){
      sqlite3_os_enter_mutex();
    }
    if( rc==SQLITE_OK ){
      removeAsyncWrite(p);
      sqlite3Os.xFree(p);
    }
  }

  /* Clear the io-busy flag and exit the mutex */
  assert( sqlite3_asyncIoBusy );
  sqlite3_asyncIoBusy = 0;
  sqlite3_os_leave_mutex();

  return rc;
}

/*
** The following code defines a Tcl interface for testing the asynchronous 
** IO implementation in this file.
*/
#if defined(SQLITE_TEST) && defined(TCLSH)

#include <tcl.h>

/*
** sqlite3_async_enable
*/
static int testAsyncEnable(
  void * clientData,
  Tcl_Interp *interp,
  int objc,
  Tcl_Obj *CONST objv[]
){
  if( sqlite3_async_enable() ){
    Tcl_SetResult(interp, "sqlite3_async_enable() failed", TCL_STATIC);
    return TCL_ERROR;
  }
  return TCL_OK;
}

/*
** This is the main proc for a thread spawned by the Tcl command 
** [sqlite3_async_flush -start]. The client data is a pointer to an integer
** variable that will be set to non-zero when this thread should exit.
*/
static void testAsyncFlushThread(ClientData clientData){
  int *pStop = (int *)clientData;
  int rc = 0;

  /* Run in a loop until an IO error occurs or we are told to stop via 
  ** the *pStop variable. Each iteration of the loop, call 
  ** sqlite3_async_flush() and then sleep for a tenth of a second.
  */
  while( !(*pStop) && !rc ){
    rc = sqlite3_async_flush();
    assert( rc==SQLITE_OK );
    Tcl_Sleep(100);
  }
  if( rc==0 ){
    rc = sqlite3_async_flush();
  }
  Tcl_ExitThread(rc);
}

/*
** sqlite3_async_flush
** sqlite3_async_flush -start
** sqlite3_async_flush -stop
*/
static int testAsyncFlush(
  void * clientData,
  Tcl_Interp *interp,
  int objc,
  Tcl_Obj *CONST objv[]
){
  static Tcl_ThreadId thread_id = 0;
  static int stop = 0;

  assert(stop==0);

  if( objc!=1 && objc!=2 ){
    Tcl_WrongNumArgs(interp, 1, objv, "?-start | -stop?");
    return TCL_ERROR;
  }

  if( objc==2 ){
    char *zOpt = Tcl_GetString(objv[1]);
    if( 0==strcmp(zOpt, "-start") ){
      /* Unless it is already running, kick off the _flush() thread */
      if( thread_id ){
        Tcl_AppendResult(interp, "Thread has already started", 0);
        return TCL_ERROR;
      }else{
        int rc = Tcl_CreateThread(
          &thread_id, 
          testAsyncFlushThread, 
          &stop, 
          TCL_THREAD_STACK_DEFAULT, 
          TCL_THREAD_JOINABLE
        );
        if( rc!=TCL_OK ){
          Tcl_AppendResult(interp, "Tcl_CreateThread() failed", 0);
          return TCL_ERROR;
        }
      }
    }else if( 0==strcmp(zOpt, "-stop") ){
      int dummy;
      stop = 1;
      Tcl_JoinThread(thread_id, &dummy);
      stop = 0;
      thread_id = 0;
    }else{
      Tcl_AppendResult(interp, "Invalid option: \"", zOpt, "\"", 0);
      return TCL_ERROR;
    }
  }else if( sqlite3_async_flush() ){
    Tcl_SetResult(interp, "sqlite3_async_flush() failed", TCL_STATIC);
    return TCL_ERROR;
  }
  return TCL_OK;
}

int Sqlitetestasync_Init(Tcl_Interp *interp){
  Tcl_CreateObjCommand(interp,"sqlite3_async_enable",testAsyncEnable,0,0);
  Tcl_CreateObjCommand(interp,"sqlite3_async_flush",testAsyncFlush,0,0);
  return TCL_OK;
}

#endif


/*
** 2006 Feb 14
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
** This file contains code that is specific to OS/2.
*/

#include "sqliteInt.h"

#if SQLITE_OS_OS2

/*
** A Note About Memory Allocation:
**
** This driver uses malloc()/free() directly rather than going through
** the SQLite-wrappers sqlite3_malloc()/sqlite3_free().  Those wrappers
** are designed for use on embedded systems where memory is scarce and
** malloc failures happen frequently.  OS/2 does not typically run on
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
** avoid the use of malloc()/free().  Those routines work ok on OS/2
** desktops but not so well in embedded systems.
*/

/*
** Macros used to determine whether or not to use threads.
*/
#if defined(SQLITE_THREADSAFE) && SQLITE_THREADSAFE
# define SQLITE_OS2_THREADS 1
#endif

/*
** Include code that is common to all os_*.c files
*/
#include "os_common.h"

/* Forward references */
typedef struct os2File os2File;         /* The file structure */
typedef struct os2ShmNode os2ShmNode;   /* A shared descritive memory node */
typedef struct os2ShmLink os2ShmLink;   /* A connection to shared-memory */

/*
** The os2File structure is subclass of sqlite3_file specific for the OS/2
** protability layer.
*/
struct os2File {
  const sqlite3_io_methods *pMethod;  /* Always the first entry */
  HFILE h;                  /* Handle for accessing the file */
  int flags;                /* Flags provided to os2Open() */
  int locktype;             /* Type of lock currently held on this file */
  int szChunk;              /* Chunk size configured by FCNTL_CHUNK_SIZE */
  char *zFullPathCp;        /* Full path name of this file */
  os2ShmLink *pShmLink;     /* Instance of shared memory on this file */
};

#define LOCK_TIMEOUT 10L /* the default locking timeout */

/*
** Missing from some versions of the OS/2 toolkit -
** used to allocate from high memory if possible
*/
#ifndef OBJ_ANY
# define OBJ_ANY 0x00000400
#endif

/*****************************************************************************
** The next group of routines implement the I/O methods specified
** by the sqlite3_io_methods object.
******************************************************************************/

/*
** Close a file.
*/
static int os2Close( sqlite3_file *id ){
  APIRET rc;
  os2File *pFile = (os2File*)id;

  assert( id!=0 );
  OSTRACE(( "CLOSE %d (%s)\n", pFile->h, pFile->zFullPathCp ));

  rc = DosClose( pFile->h );

  if( pFile->flags & SQLITE_OPEN_DELETEONCLOSE )
    DosForceDelete( (PSZ)pFile->zFullPathCp );

  free( pFile->zFullPathCp );
  pFile->zFullPathCp = NULL;
  pFile->locktype = NO_LOCK;
  pFile->h = (HFILE)-1;
  pFile->flags = 0;

  OpenCounter( -1 );
  return rc == NO_ERROR ? SQLITE_OK : SQLITE_IOERR;
}

/*
** Read data from a file into a buffer.  Return SQLITE_OK if all
** bytes were read successfully and SQLITE_IOERR if anything goes
** wrong.
*/
static int os2Read(
  sqlite3_file *id,               /* File to read from */
  void *pBuf,                     /* Write content into this buffer */
  int amt,                        /* Number of bytes to read */
  sqlite3_int64 offset            /* Begin reading at this offset */
){
  ULONG fileLocation = 0L;
  ULONG got;
  os2File *pFile = (os2File*)id;
  assert( id!=0 );
  SimulateIOError( return SQLITE_IOERR_READ );
  OSTRACE(( "READ %d lock=%d\n", pFile->h, pFile->locktype ));
  if( DosSetFilePtr(pFile->h, offset, FILE_BEGIN, &fileLocation) != NO_ERROR ){
    return SQLITE_IOERR;
  }
  if( DosRead( pFile->h, pBuf, amt, &got ) != NO_ERROR ){
    return SQLITE_IOERR_READ;
  }
  if( got == (ULONG)amt )
    return SQLITE_OK;
  else {
    /* Unread portions of the input buffer must be zero-filled */
    memset(&((char*)pBuf)[got], 0, amt-got);
    return SQLITE_IOERR_SHORT_READ;
  }
}

/*
** Write data from a buffer into a file.  Return SQLITE_OK on success
** or some other error code on failure.
*/
static int os2Write(
  sqlite3_file *id,               /* File to write into */
  const void *pBuf,               /* The bytes to be written */
  int amt,                        /* Number of bytes to write */
  sqlite3_int64 offset            /* Offset into the file to begin writing at */
){
  ULONG fileLocation = 0L;
  APIRET rc = NO_ERROR;
  ULONG wrote;
  os2File *pFile = (os2File*)id;
  assert( id!=0 );
  SimulateIOError( return SQLITE_IOERR_WRITE );
  SimulateDiskfullError( return SQLITE_FULL );
  OSTRACE(( "WRITE %d lock=%d\n", pFile->h, pFile->locktype ));
  if( DosSetFilePtr(pFile->h, offset, FILE_BEGIN, &fileLocation) != NO_ERROR ){
    return SQLITE_IOERR;
  }
  assert( amt>0 );
  while( amt > 0 &&
         ( rc = DosWrite( pFile->h, (PVOID)pBuf, amt, &wrote ) ) == NO_ERROR &&
         wrote > 0
  ){
    amt -= wrote;
    pBuf = &((char*)pBuf)[wrote];
  }

  return ( rc != NO_ERROR || amt > (int)wrote ) ? SQLITE_FULL : SQLITE_OK;
}

/*
** Truncate an open file to a specified size
*/
static int os2Truncate( sqlite3_file *id, i64 nByte ){
  APIRET rc;
  os2File *pFile = (os2File*)id;
  assert( id!=0 );
  OSTRACE(( "TRUNCATE %d %lld\n", pFile->h, nByte ));
  SimulateIOError( return SQLITE_IOERR_TRUNCATE );

  /* If the user has configured a chunk-size for this file, truncate the
  ** file so that it consists of an integer number of chunks (i.e. the
  ** actual file size after the operation may be larger than the requested
  ** size).
  */
  if( pFile->szChunk ){
    nByte = ((nByte + pFile->szChunk - 1)/pFile->szChunk) * pFile->szChunk;
  }
  
  rc = DosSetFileSize( pFile->h, nByte );
  return rc == NO_ERROR ? SQLITE_OK : SQLITE_IOERR_TRUNCATE;
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
static int os2Sync( sqlite3_file *id, int flags ){
  os2File *pFile = (os2File*)id;
  OSTRACE(( "SYNC %d lock=%d\n", pFile->h, pFile->locktype ));
#ifdef SQLITE_TEST
  if( flags & SQLITE_SYNC_FULL){
    sqlite3_fullsync_count++;
  }
  sqlite3_sync_count++;
#endif
  /* If we compiled with the SQLITE_NO_SYNC flag, then syncing is a
  ** no-op
  */
#ifdef SQLITE_NO_SYNC
  UNUSED_PARAMETER(pFile);
  return SQLITE_OK;
#else
  return DosResetBuffer( pFile->h ) == NO_ERROR ? SQLITE_OK : SQLITE_IOERR;
#endif
}

/*
** Determine the current size of a file in bytes
*/
static int os2FileSize( sqlite3_file *id, sqlite3_int64 *pSize ){
  APIRET rc = NO_ERROR;
  FILESTATUS3 fsts3FileInfo;
  memset(&fsts3FileInfo, 0, sizeof(fsts3FileInfo));
  assert( id!=0 );
  SimulateIOError( return SQLITE_IOERR_FSTAT );
  rc = DosQueryFileInfo( ((os2File*)id)->h, FIL_STANDARD, &fsts3FileInfo, sizeof(FILESTATUS3) );
  if( rc == NO_ERROR ){
    *pSize = fsts3FileInfo.cbFile;
    return SQLITE_OK;
  }else{
    return SQLITE_IOERR_FSTAT;
  }
}

/*
** Acquire a reader lock.
*/
static int getReadLock( os2File *pFile ){
  FILELOCK  LockArea,
            UnlockArea;
  APIRET res;
  memset(&LockArea, 0, sizeof(LockArea));
  memset(&UnlockArea, 0, sizeof(UnlockArea));
  LockArea.lOffset = SHARED_FIRST;
  LockArea.lRange = SHARED_SIZE;
  UnlockArea.lOffset = 0L;
  UnlockArea.lRange = 0L;
  res = DosSetFileLocks( pFile->h, &UnlockArea, &LockArea, LOCK_TIMEOUT, 1L );
  OSTRACE(( "GETREADLOCK %d res=%d\n", pFile->h, res ));
  return res;
}

/*
** Undo a readlock
*/
static int unlockReadLock( os2File *id ){
  FILELOCK  LockArea,
            UnlockArea;
  APIRET res;
  memset(&LockArea, 0, sizeof(LockArea));
  memset(&UnlockArea, 0, sizeof(UnlockArea));
  LockArea.lOffset = 0L;
  LockArea.lRange = 0L;
  UnlockArea.lOffset = SHARED_FIRST;
  UnlockArea.lRange = SHARED_SIZE;
  res = DosSetFileLocks( id->h, &UnlockArea, &LockArea, LOCK_TIMEOUT, 1L );
  OSTRACE(( "UNLOCK-READLOCK file handle=%d res=%d?\n", id->h, res ));
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
** This routine will only increase a lock.  The os2Unlock() routine
** erases all locks at once and returns us immediately to locking level 0.
** It is not possible to lower the locking level one step at a time.  You
** must go straight to locking level 0.
*/
static int os2Lock( sqlite3_file *id, int locktype ){
  int rc = SQLITE_OK;       /* Return code from subroutines */
  APIRET res = NO_ERROR;    /* Result of an OS/2 lock call */
  int newLocktype;       /* Set pFile->locktype to this value before exiting */
  int gotPendingLock = 0;/* True if we acquired a PENDING lock this time */
  FILELOCK  LockArea,
            UnlockArea;
  os2File *pFile = (os2File*)id;
  memset(&LockArea, 0, sizeof(LockArea));
  memset(&UnlockArea, 0, sizeof(UnlockArea));
  assert( pFile!=0 );
  OSTRACE(( "LOCK %d %d was %d\n", pFile->h, locktype, pFile->locktype ));

  /* If there is already a lock of this type or more restrictive on the
  ** os2File, do nothing. Don't use the end_lock: exit path, as
  ** sqlite3_mutex_enter() hasn't been called yet.
  */
  if( pFile->locktype>=locktype ){
    OSTRACE(( "LOCK %d %d ok (already held)\n", pFile->h, locktype ));
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
  if( pFile->locktype==NO_LOCK
      || (locktype==EXCLUSIVE_LOCK && pFile->locktype==RESERVED_LOCK)
  ){
    LockArea.lOffset = PENDING_BYTE;
    LockArea.lRange = 1L;
    UnlockArea.lOffset = 0L;
    UnlockArea.lRange = 0L;

    /* wait longer than LOCK_TIMEOUT here not to have to try multiple times */
    res = DosSetFileLocks( pFile->h, &UnlockArea, &LockArea, 100L, 0L );
    if( res == NO_ERROR ){
      gotPendingLock = 1;
      OSTRACE(( "LOCK %d pending lock boolean set.  res=%d\n", pFile->h, res ));
    }
  }

  /* Acquire a shared lock
  */
  if( locktype==SHARED_LOCK && res == NO_ERROR ){
    assert( pFile->locktype==NO_LOCK );
    res = getReadLock(pFile);
    if( res == NO_ERROR ){
      newLocktype = SHARED_LOCK;
    }
    OSTRACE(( "LOCK %d acquire shared lock. res=%d\n", pFile->h, res ));
  }

  /* Acquire a RESERVED lock
  */
  if( locktype==RESERVED_LOCK && res == NO_ERROR ){
    assert( pFile->locktype==SHARED_LOCK );
    LockArea.lOffset = RESERVED_BYTE;
    LockArea.lRange = 1L;
    UnlockArea.lOffset = 0L;
    UnlockArea.lRange = 0L;
    res = DosSetFileLocks( pFile->h, &UnlockArea, &LockArea, LOCK_TIMEOUT, 0L );
    if( res == NO_ERROR ){
      newLocktype = RESERVED_LOCK;
    }
    OSTRACE(( "LOCK %d acquire reserved lock. res=%d\n", pFile->h, res ));
  }

  /* Acquire a PENDING lock
  */
  if( locktype==EXCLUSIVE_LOCK && res == NO_ERROR ){
    newLocktype = PENDING_LOCK;
    gotPendingLock = 0;
    OSTRACE(( "LOCK %d acquire pending lock. pending lock boolean unset.\n",
               pFile->h ));
  }

  /* Acquire an EXCLUSIVE lock
  */
  if( locktype==EXCLUSIVE_LOCK && res == NO_ERROR ){
    assert( pFile->locktype>=SHARED_LOCK );
    res = unlockReadLock(pFile);
    OSTRACE(( "unreadlock = %d\n", res ));
    LockArea.lOffset = SHARED_FIRST;
    LockArea.lRange = SHARED_SIZE;
    UnlockArea.lOffset = 0L;
    UnlockArea.lRange = 0L;
    res = DosSetFileLocks( pFile->h, &UnlockArea, &LockArea, LOCK_TIMEOUT, 0L );
    if( res == NO_ERROR ){
      newLocktype = EXCLUSIVE_LOCK;
    }else{
      OSTRACE(( "OS/2 error-code = %d\n", res ));
      getReadLock(pFile);
    }
    OSTRACE(( "LOCK %d acquire exclusive lock.  res=%d\n", pFile->h, res ));
  }

  /* If we are holding a PENDING lock that ought to be released, then
  ** release it now.
  */
  if( gotPendingLock && locktype==SHARED_LOCK ){
    int r;
    LockArea.lOffset = 0L;
    LockArea.lRange = 0L;
    UnlockArea.lOffset = PENDING_BYTE;
    UnlockArea.lRange = 1L;
    r = DosSetFileLocks( pFile->h, &UnlockArea, &LockArea, LOCK_TIMEOUT, 0L );
    OSTRACE(( "LOCK %d unlocking pending/is shared. r=%d\n", pFile->h, r ));
  }

  /* Update the state of the lock has held in the file descriptor then
  ** return the appropriate result code.
  */
  if( res == NO_ERROR ){
    rc = SQLITE_OK;
  }else{
    OSTRACE(( "LOCK FAILED %d trying for %d but got %d\n", pFile->h,
              locktype, newLocktype ));
    rc = SQLITE_BUSY;
  }
  pFile->locktype = newLocktype;
  OSTRACE(( "LOCK %d now %d\n", pFile->h, pFile->locktype ));
  return rc;
}

/*
** This routine checks if there is a RESERVED lock held on the specified
** file by this or any other process. If such a lock is held, return
** non-zero, otherwise zero.
*/
static int os2CheckReservedLock( sqlite3_file *id, int *pOut ){
  int r = 0;
  os2File *pFile = (os2File*)id;
  assert( pFile!=0 );
  if( pFile->locktype>=RESERVED_LOCK ){
    r = 1;
    OSTRACE(( "TEST WR-LOCK %d %d (local)\n", pFile->h, r ));
  }else{
    FILELOCK  LockArea,
              UnlockArea;
    APIRET rc = NO_ERROR;
    memset(&LockArea, 0, sizeof(LockArea));
    memset(&UnlockArea, 0, sizeof(UnlockArea));
    LockArea.lOffset = RESERVED_BYTE;
    LockArea.lRange = 1L;
    UnlockArea.lOffset = 0L;
    UnlockArea.lRange = 0L;
    rc = DosSetFileLocks( pFile->h, &UnlockArea, &LockArea, LOCK_TIMEOUT, 0L );
    OSTRACE(( "TEST WR-LOCK %d lock reserved byte rc=%d\n", pFile->h, rc ));
    if( rc == NO_ERROR ){
      APIRET rcu = NO_ERROR; /* return code for unlocking */
      LockArea.lOffset = 0L;
      LockArea.lRange = 0L;
      UnlockArea.lOffset = RESERVED_BYTE;
      UnlockArea.lRange = 1L;
      rcu = DosSetFileLocks( pFile->h, &UnlockArea, &LockArea, LOCK_TIMEOUT, 0L );
      OSTRACE(( "TEST WR-LOCK %d unlock reserved byte r=%d\n", pFile->h, rcu ));
    }
    r = !(rc == NO_ERROR);
    OSTRACE(( "TEST WR-LOCK %d %d (remote)\n", pFile->h, r ));
  }
  *pOut = r;
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
static int os2Unlock( sqlite3_file *id, int locktype ){
  int type;
  os2File *pFile = (os2File*)id;
  APIRET rc = SQLITE_OK;
  APIRET res = NO_ERROR;
  FILELOCK  LockArea,
            UnlockArea;
  memset(&LockArea, 0, sizeof(LockArea));
  memset(&UnlockArea, 0, sizeof(UnlockArea));
  assert( pFile!=0 );
  assert( locktype<=SHARED_LOCK );
  OSTRACE(( "UNLOCK %d to %d was %d\n", pFile->h, locktype, pFile->locktype ));
  type = pFile->locktype;
  if( type>=EXCLUSIVE_LOCK ){
    LockArea.lOffset = 0L;
    LockArea.lRange = 0L;
    UnlockArea.lOffset = SHARED_FIRST;
    UnlockArea.lRange = SHARED_SIZE;
    res = DosSetFileLocks( pFile->h, &UnlockArea, &LockArea, LOCK_TIMEOUT, 0L );
    OSTRACE(( "UNLOCK %d exclusive lock res=%d\n", pFile->h, res ));
    if( locktype==SHARED_LOCK && getReadLock(pFile) != NO_ERROR ){
      /* This should never happen.  We should always be able to
      ** reacquire the read lock */
      OSTRACE(( "UNLOCK %d to %d getReadLock() failed\n", pFile->h, locktype ));
      rc = SQLITE_IOERR_UNLOCK;
    }
  }
  if( type>=RESERVED_LOCK ){
    LockArea.lOffset = 0L;
    LockArea.lRange = 0L;
    UnlockArea.lOffset = RESERVED_BYTE;
    UnlockArea.lRange = 1L;
    res = DosSetFileLocks( pFile->h, &UnlockArea, &LockArea, LOCK_TIMEOUT, 0L );
    OSTRACE(( "UNLOCK %d reserved res=%d\n", pFile->h, res ));
  }
  if( locktype==NO_LOCK && type>=SHARED_LOCK ){
    res = unlockReadLock(pFile);
    OSTRACE(( "UNLOCK %d is %d want %d res=%d\n",
              pFile->h, type, locktype, res ));
  }
  if( type>=PENDING_LOCK ){
    LockArea.lOffset = 0L;
    LockArea.lRange = 0L;
    UnlockArea.lOffset = PENDING_BYTE;
    UnlockArea.lRange = 1L;
    res = DosSetFileLocks( pFile->h, &UnlockArea, &LockArea, LOCK_TIMEOUT, 0L );
    OSTRACE(( "UNLOCK %d pending res=%d\n", pFile->h, res ));
  }
  pFile->locktype = locktype;
  OSTRACE(( "UNLOCK %d now %d\n", pFile->h, pFile->locktype ));
  return rc;
}

/*
** Control and query of the open file handle.
*/
static int os2FileControl(sqlite3_file *id, int op, void *pArg){
  switch( op ){
    case SQLITE_FCNTL_LOCKSTATE: {
      *(int*)pArg = ((os2File*)id)->locktype;
      OSTRACE(( "FCNTL_LOCKSTATE %d lock=%d\n",
                ((os2File*)id)->h, ((os2File*)id)->locktype ));
      return SQLITE_OK;
    }
    case SQLITE_FCNTL_CHUNK_SIZE: {
      ((os2File*)id)->szChunk = *(int*)pArg;
      return SQLITE_OK;
    }
    case SQLITE_FCNTL_SIZE_HINT: {
      sqlite3_int64 sz = *(sqlite3_int64*)pArg;
      SimulateIOErrorBenign(1);
      os2Truncate(id, sz);
      SimulateIOErrorBenign(0);
      return SQLITE_OK;
    }
    case SQLITE_FCNTL_SYNC_OMITTED: {
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
static int os2SectorSize(sqlite3_file *id){
  UNUSED_PARAMETER(id);
  return SQLITE_DEFAULT_SECTOR_SIZE;
}

/*
** Return a vector of device characteristics.
*/
static int os2DeviceCharacteristics(sqlite3_file *id){
  UNUSED_PARAMETER(id);
  return SQLITE_IOCAP_UNDELETABLE_WHEN_OPEN;
}


/*
** Character set conversion objects used by conversion routines.
*/
static UconvObject ucUtf8 = NULL; /* convert between UTF-8 and UCS-2 */
static UconvObject uclCp = NULL;  /* convert between local codepage and UCS-2 */

/*
** Helper function to initialize the conversion objects from and to UTF-8.
*/
static void initUconvObjects( void ){
  if( UniCreateUconvObject( UTF_8, &ucUtf8 ) != ULS_SUCCESS )
    ucUtf8 = NULL;
  if ( UniCreateUconvObject( (UniChar *)L"@path=yes", &uclCp ) != ULS_SUCCESS )
    uclCp = NULL;
}

/*
** Helper function to free the conversion objects from and to UTF-8.
*/
static void freeUconvObjects( void ){
  if ( ucUtf8 )
    UniFreeUconvObject( ucUtf8 );
  if ( uclCp )
    UniFreeUconvObject( uclCp );
  ucUtf8 = NULL;
  uclCp = NULL;
}

/*
** Helper function to convert UTF-8 filenames to local OS/2 codepage.
** The two-step process: first convert the incoming UTF-8 string
** into UCS-2 and then from UCS-2 to the current codepage.
** The returned char pointer has to be freed.
*/
static char *convertUtf8PathToCp( const char *in ){
  UniChar tempPath[CCHMAXPATH];
  char *out = (char *)calloc( CCHMAXPATH, 1 );

  if( !out )
    return NULL;

  if( !ucUtf8 || !uclCp )
    initUconvObjects();

  /* determine string for the conversion of UTF-8 which is CP1208 */
  if( UniStrToUcs( ucUtf8, tempPath, (char *)in, CCHMAXPATH ) != ULS_SUCCESS )
    return out; /* if conversion fails, return the empty string */

  /* conversion for current codepage which can be used for paths */
  UniStrFromUcs( uclCp, out, tempPath, CCHMAXPATH );

  return out;
}

/*
** Helper function to convert filenames from local codepage to UTF-8.
** The two-step process: first convert the incoming codepage-specific
** string into UCS-2 and then from UCS-2 to the codepage of UTF-8.
** The returned char pointer has to be freed.
**
** This function is non-static to be able to use this in shell.c and
** similar applications that take command line arguments.
*/
char *convertCpPathToUtf8( const char *in ){
  UniChar tempPath[CCHMAXPATH];
  char *out = (char *)calloc( CCHMAXPATH, 1 );

  if( !out )
    return NULL;

  if( !ucUtf8 || !uclCp )
    initUconvObjects();

  /* conversion for current codepage which can be used for paths */
  if( UniStrToUcs( uclCp, tempPath, (char *)in, CCHMAXPATH ) != ULS_SUCCESS )
    return out; /* if conversion fails, return the empty string */

  /* determine string for the conversion of UTF-8 which is CP1208 */
  UniStrFromUcs( ucUtf8, out, tempPath, CCHMAXPATH );

  return out;
}


#ifndef SQLITE_OMIT_WAL

/*
** Use main database file for interprocess locking. If un-defined
** a separate file is created for this purpose. The file will be
** used only to set file locks. There will be no data written to it.
*/
#define SQLITE_OS2_NO_WAL_LOCK_FILE     

#if 0
static void _ERR_TRACE( const char *fmt, ... ) {
  va_list  ap;
  va_start(ap, fmt);
  vfprintf(stderr, fmt, ap);
  fflush(stderr);
}
#define ERR_TRACE(rc, msg)        \
        if( (rc) != SQLITE_OK ) _ERR_TRACE msg;
#else
#define ERR_TRACE(rc, msg)
#endif

/*
** Helper functions to obtain and relinquish the global mutex. The
** global mutex is used to protect os2ShmNodeList.
**
** Function os2ShmMutexHeld() is used to assert() that the global mutex 
** is held when required. This function is only used as part of assert() 
** statements. e.g.
**
**   os2ShmEnterMutex()
**     assert( os2ShmMutexHeld() );
**   os2ShmLeaveMutex()
*/
static void os2ShmEnterMutex(void){
  sqlite3_mutex_enter(sqlite3MutexAlloc(SQLITE_MUTEX_STATIC_MASTER));
}
static void os2ShmLeaveMutex(void){
  sqlite3_mutex_leave(sqlite3MutexAlloc(SQLITE_MUTEX_STATIC_MASTER));
}
#ifdef SQLITE_DEBUG
static int os2ShmMutexHeld(void) {
  return sqlite3_mutex_held(sqlite3MutexAlloc(SQLITE_MUTEX_STATIC_MASTER));
}
int GetCurrentProcessId(void) {
  PPIB pib;
  DosGetInfoBlocks(NULL, &pib);
  return (int)pib->pib_ulpid;
}
#endif

/*
** Object used to represent a the shared memory area for a single log file.
** When multiple threads all reference the same log-summary, each thread has
** its own os2File object, but they all point to a single instance of this 
** object.  In other words, each log-summary is opened only once per process.
**
** os2ShmMutexHeld() must be true when creating or destroying
** this object or while reading or writing the following fields:
**
**      nRef
**      pNext 
**
** The following fields are read-only after the object is created:
** 
**      szRegion
**      hLockFile
**      shmBaseName
**
** Either os2ShmNode.mutex must be held or os2ShmNode.nRef==0 and
** os2ShmMutexHeld() is true when reading or writing any other field
** in this structure.
**
*/
struct os2ShmNode {
  sqlite3_mutex *mutex;      /* Mutex to access this object */
  os2ShmNode *pNext;         /* Next in list of all os2ShmNode objects */

  int szRegion;              /* Size of shared-memory regions */

  int nRegion;               /* Size of array apRegion */
  void **apRegion;           /* Array of pointers to shared-memory regions */

  int nRef;                  /* Number of os2ShmLink objects pointing to this */
  os2ShmLink *pFirst;        /* First os2ShmLink object pointing to this */

  HFILE hLockFile;           /* File used for inter-process memory locking */
  char shmBaseName[1];       /* Name of the memory object !!! must last !!! */
};


/*
** Structure used internally by this VFS to record the state of an
** open shared memory connection.
**
** The following fields are initialized when this object is created and
** are read-only thereafter:
**
**    os2Shm.pShmNode
**    os2Shm.id
**
** All other fields are read/write.  The os2Shm.pShmNode->mutex must be held
** while accessing any read/write fields.
*/
struct os2ShmLink {
  os2ShmNode *pShmNode;      /* The underlying os2ShmNode object */
  os2ShmLink *pNext;         /* Next os2Shm with the same os2ShmNode */
  u32 sharedMask;            /* Mask of shared locks held */
  u32 exclMask;              /* Mask of exclusive locks held */
#ifdef SQLITE_DEBUG
  u8 id;                     /* Id of this connection with its os2ShmNode */
#endif
};


/*
** A global list of all os2ShmNode objects.
**
** The os2ShmMutexHeld() must be true while reading or writing this list.
*/
static os2ShmNode *os2ShmNodeList = NULL;

/*
** Constants used for locking
*/
#ifdef  SQLITE_OS2_NO_WAL_LOCK_FILE
#define OS2_SHM_BASE   (PENDING_BYTE + 0x10000)         /* first lock byte */
#else
#define OS2_SHM_BASE   ((22+SQLITE_SHM_NLOCK)*4)        /* first lock byte */
#endif

#define OS2_SHM_DMS    (OS2_SHM_BASE+SQLITE_SHM_NLOCK)  /* deadman switch */

/*
** Apply advisory locks for all n bytes beginning at ofst.
*/
#define _SHM_UNLCK  1   /* no lock */
#define _SHM_RDLCK  2   /* shared lock, no wait */
#define _SHM_WRLCK  3   /* exlusive lock, no wait */
#define _SHM_WRLCK_WAIT 4 /* exclusive lock, wait */
static int os2ShmSystemLock(
  os2ShmNode *pNode,    /* Apply locks to this open shared-memory segment */
  int lockType,         /* _SHM_UNLCK, _SHM_RDLCK, _SHM_WRLCK or _SHM_WRLCK_WAIT */
  int ofst,             /* Offset to first byte to be locked/unlocked */
  int nByte             /* Number of bytes to lock or unlock */
){
  APIRET rc;
  FILELOCK area;
  ULONG mode, timeout;

  /* Access to the os2ShmNode object is serialized by the caller */
  assert( sqlite3_mutex_held(pNode->mutex) || pNode->nRef==0 );

  mode = 1;     /* shared lock */
  timeout = 0;  /* no wait */
  area.lOffset = ofst;
  area.lRange = nByte;

  switch( lockType ) {
    case _SHM_WRLCK_WAIT:
      timeout = (ULONG)-1;      /* wait forever */
    case _SHM_WRLCK:
      mode = 0;                 /* exclusive lock */
    case _SHM_RDLCK:
      rc = DosSetFileLocks(pNode->hLockFile, 
                           NULL, &area, timeout, mode);
      break;
    /* case _SHM_UNLCK: */
    default:
      rc = DosSetFileLocks(pNode->hLockFile, 
                           &area, NULL, 0, 0);
      break;
  }
                          
  OSTRACE(("SHM-LOCK %d %s %s 0x%08lx\n", 
           pNode->hLockFile,
           rc==SQLITE_OK ? "ok" : "failed",
           lockType==_SHM_UNLCK ? "Unlock" : "Lock",
           rc));

  ERR_TRACE(rc, ("os2ShmSystemLock: %d %s\n", rc, pNode->shmBaseName))

  return ( rc == 0 ) ?  SQLITE_OK : SQLITE_BUSY;
}

/*
** Find an os2ShmNode in global list or allocate a new one, if not found.
**
** This is not a VFS shared-memory method; it is a utility function called
** by VFS shared-memory methods.
*/
static int os2OpenSharedMemory( os2File *fd, int szRegion ) {
  os2ShmLink *pLink;
  os2ShmNode *pNode;
  int cbShmName, rc = SQLITE_OK;
  char shmName[CCHMAXPATH + 30];
#ifndef SQLITE_OS2_NO_WAL_LOCK_FILE
  ULONG action;
#endif
  
  /* We need some additional space at the end to append the region number */
  cbShmName = sprintf(shmName, "\\SHAREMEM\\%s", fd->zFullPathCp );
  if( cbShmName >= CCHMAXPATH-8 )
    return SQLITE_IOERR_SHMOPEN; 

  /* Replace colon in file name to form a valid shared memory name */
  shmName[10+1] = '!';

  /* Allocate link object (we free it later in case of failure) */
  pLink = sqlite3_malloc( sizeof(*pLink) );
  if( !pLink )
    return SQLITE_NOMEM;

  /* Access node list */
  os2ShmEnterMutex();

  /* Find node by it's shared memory base name */
  for( pNode = os2ShmNodeList; 
       pNode && stricmp(shmName, pNode->shmBaseName) != 0; 
       pNode = pNode->pNext )   ;

  /* Not found: allocate a new node */
  if( !pNode ) {
    pNode = sqlite3_malloc( sizeof(*pNode) + cbShmName );
    if( pNode ) {
      memset(pNode, 0, sizeof(*pNode) );
      pNode->szRegion = szRegion;
      pNode->hLockFile = (HFILE)-1;      
      strcpy(pNode->shmBaseName, shmName);

#ifdef SQLITE_OS2_NO_WAL_LOCK_FILE
      if( DosDupHandle(fd->h, &pNode->hLockFile) != 0 ) {
#else
      sprintf(shmName, "%s-lck", fd->zFullPathCp);
      if( DosOpen((PSZ)shmName, &pNode->hLockFile, &action, 0, FILE_NORMAL, 
                  OPEN_ACTION_OPEN_IF_EXISTS | OPEN_ACTION_CREATE_IF_NEW,
                  OPEN_ACCESS_READWRITE | OPEN_SHARE_DENYNONE | 
                  OPEN_FLAGS_NOINHERIT | OPEN_FLAGS_FAIL_ON_ERROR,
                  NULL) != 0 ) {
#endif
        sqlite3_free(pNode);  
        rc = SQLITE_IOERR;
      } else {
        pNode->mutex = sqlite3_mutex_alloc(SQLITE_MUTEX_FAST);
        if( !pNode->mutex ) {
          sqlite3_free(pNode);  
          rc = SQLITE_NOMEM;
        }
      }   
    } else {
      rc = SQLITE_NOMEM;
    }
    
    if( rc == SQLITE_OK ) {
      pNode->pNext = os2ShmNodeList;
      os2ShmNodeList = pNode;
    } else {
      pNode = NULL;
    }
  } else if( pNode->szRegion != szRegion ) {
    rc = SQLITE_IOERR_SHMSIZE;
    pNode = NULL;
  }

  if( pNode ) {
    sqlite3_mutex_enter(pNode->mutex);

    memset(pLink, 0, sizeof(*pLink));

    pLink->pShmNode = pNode;
    pLink->pNext = pNode->pFirst;
    pNode->pFirst = pLink;
    pNode->nRef++;

    fd->pShmLink = pLink;

    sqlite3_mutex_leave(pNode->mutex);
    
  } else {
    /* Error occured. Free our link object. */
    sqlite3_free(pLink);  
  }

  os2ShmLeaveMutex();

  ERR_TRACE(rc, ("os2OpenSharedMemory: %d  %s\n", rc, fd->zFullPathCp))  
  
  return rc;
}

/*
** Purge the os2ShmNodeList list of all entries with nRef==0.
**
** This is not a VFS shared-memory method; it is a utility function called
** by VFS shared-memory methods.
*/
static void os2PurgeShmNodes( int deleteFlag ) {
  os2ShmNode *pNode;
  os2ShmNode **ppNode;

  os2ShmEnterMutex();
  
  ppNode = &os2ShmNodeList;

  while( *ppNode ) {
    pNode = *ppNode;

    if( pNode->nRef == 0 ) {
      *ppNode = pNode->pNext;   
     
      if( pNode->apRegion ) {
        /* Prevent other processes from resizing the shared memory */
        os2ShmSystemLock(pNode, _SHM_WRLCK_WAIT, OS2_SHM_DMS, 1);

        while( pNode->nRegion-- ) {
#ifdef SQLITE_DEBUG
          int rc = 
#endif          
          DosFreeMem(pNode->apRegion[pNode->nRegion]);

          OSTRACE(("SHM-PURGE pid-%d unmap region=%d %s\n",
                  (int)GetCurrentProcessId(), pNode->nRegion,
                  rc == 0 ? "ok" : "failed"));
        }

        /* Allow other processes to resize the shared memory */
        os2ShmSystemLock(pNode, _SHM_UNLCK, OS2_SHM_DMS, 1);

        sqlite3_free(pNode->apRegion);
      }  

      DosClose(pNode->hLockFile);
      
#ifndef SQLITE_OS2_NO_WAL_LOCK_FILE
      if( deleteFlag ) {
         char fileName[CCHMAXPATH];
         /* Skip "\\SHAREMEM\\" */
         sprintf(fileName, "%s-lck", pNode->shmBaseName + 10);
         /* restore colon */
         fileName[1] = ':';
         
         DosForceDelete(fileName); 
      }
#endif

      sqlite3_mutex_free(pNode->mutex);

      sqlite3_free(pNode);
      
    } else {
      ppNode = &pNode->pNext;
    }
  } 

  os2ShmLeaveMutex();
}

/*
** This function is called to obtain a pointer to region iRegion of the
** shared-memory associated with the database file id. Shared-memory regions
** are numbered starting from zero. Each shared-memory region is szRegion
** bytes in size.
**
** If an error occurs, an error code is returned and *pp is set to NULL.
**
** Otherwise, if the bExtend parameter is 0 and the requested shared-memory
** region has not been allocated (by any client, including one running in a
** separate process), then *pp is set to NULL and SQLITE_OK returned. If
** bExtend is non-zero and the requested shared-memory region has not yet
** been allocated, it is allocated by this function.
**
** If the shared-memory region has already been allocated or is allocated by
** this call as described above, then it is mapped into this processes
** address space (if it is not already), *pp is set to point to the mapped
** memory and SQLITE_OK returned.
*/
static int os2ShmMap(
  sqlite3_file *id,               /* Handle open on database file */
  int iRegion,                    /* Region to retrieve */
  int szRegion,                   /* Size of regions */
  int bExtend,                    /* True to extend block if necessary */
  void volatile **pp              /* OUT: Mapped memory */
){
  PVOID pvTemp;
  void **apRegion;
  os2ShmNode *pNode;
  int n, rc = SQLITE_OK;
  char shmName[CCHMAXPATH];
  os2File *pFile = (os2File*)id;
  
  *pp = NULL;

  if( !pFile->pShmLink )
    rc = os2OpenSharedMemory( pFile, szRegion );
  
  if( rc == SQLITE_OK ) {
    pNode = pFile->pShmLink->pShmNode ;
    
    sqlite3_mutex_enter(pNode->mutex);
    
    assert( szRegion==pNode->szRegion );

    /* Unmapped region ? */
    if( iRegion >= pNode->nRegion ) {
      /* Prevent other processes from resizing the shared memory */
      os2ShmSystemLock(pNode, _SHM_WRLCK_WAIT, OS2_SHM_DMS, 1);

      apRegion = sqlite3_realloc(
        pNode->apRegion, (iRegion + 1) * sizeof(apRegion[0]));

      if( apRegion ) {
        pNode->apRegion = apRegion;

        while( pNode->nRegion <= iRegion ) {
          sprintf(shmName, "%s-%u", 
                  pNode->shmBaseName, pNode->nRegion);

          if( DosGetNamedSharedMem(&pvTemp, (PSZ)shmName, 
                PAG_READ | PAG_WRITE) != NO_ERROR ) {
            if( !bExtend )
              break;

            if( DosAllocSharedMem(&pvTemp, (PSZ)shmName, szRegion,
                  PAG_READ | PAG_WRITE | PAG_COMMIT | OBJ_ANY) != NO_ERROR && 
                DosAllocSharedMem(&pvTemp, (PSZ)shmName, szRegion,
                  PAG_READ | PAG_WRITE | PAG_COMMIT) != NO_ERROR ) { 
              rc = SQLITE_NOMEM;
              break;
            }
          }

          apRegion[pNode->nRegion++] = pvTemp;
        }

        /* zero out remaining entries */ 
        for( n = pNode->nRegion; n <= iRegion; n++ )
          pNode->apRegion[n] = NULL;

        /* Return this region (maybe zero) */
        *pp = pNode->apRegion[iRegion];
      } else {
        rc = SQLITE_NOMEM;
      }

      /* Allow other processes to resize the shared memory */
      os2ShmSystemLock(pNode, _SHM_UNLCK, OS2_SHM_DMS, 1);
      
    } else {
      /* Region has been mapped previously */
      *pp = pNode->apRegion[iRegion];
    }

    sqlite3_mutex_leave(pNode->mutex);
  } 

  ERR_TRACE(rc, ("os2ShmMap: %s iRgn = %d, szRgn = %d, bExt = %d : %d\n", 
                 pFile->zFullPathCp, iRegion, szRegion, bExtend, rc))
          
  return rc;
}

/*
** Close a connection to shared-memory.  Delete the underlying
** storage if deleteFlag is true.
**
** If there is no shared memory associated with the connection then this
** routine is a harmless no-op.
*/
static int os2ShmUnmap(
  sqlite3_file *id,               /* The underlying database file */
  int deleteFlag                  /* Delete shared-memory if true */
){
  os2File *pFile = (os2File*)id;
  os2ShmLink *pLink = pFile->pShmLink;
  
  if( pLink ) {
    int nRef = -1;
    os2ShmLink **ppLink;
    os2ShmNode *pNode = pLink->pShmNode;

    sqlite3_mutex_enter(pNode->mutex);
    
    for( ppLink = &pNode->pFirst;
         *ppLink && *ppLink != pLink;
         ppLink = &(*ppLink)->pNext )   ;
         
    assert(*ppLink);

    if( *ppLink ) {
      *ppLink = pLink->pNext;
      nRef = --pNode->nRef;
    } else {
      ERR_TRACE(1, ("os2ShmUnmap: link not found ! %s\n", 
                    pNode->shmBaseName))
    }
    
    pFile->pShmLink = NULL;
    sqlite3_free(pLink);

    sqlite3_mutex_leave(pNode->mutex);
    
    if( nRef == 0 )
      os2PurgeShmNodes( deleteFlag );
  }

  return SQLITE_OK;
}

/*
** Change the lock state for a shared-memory segment.
**
** Note that the relationship between SHAREd and EXCLUSIVE locks is a little
** different here than in posix.  In xShmLock(), one can go from unlocked
** to shared and back or from unlocked to exclusive and back.  But one may
** not go from shared to exclusive or from exclusive to shared.
*/
static int os2ShmLock(
  sqlite3_file *id,          /* Database file holding the shared memory */
  int ofst,                  /* First lock to acquire or release */
  int n,                     /* Number of locks to acquire or release */
  int flags                  /* What to do with the lock */
){
  u32 mask;                             /* Mask of locks to take or release */
  int rc = SQLITE_OK;                   /* Result code */
  os2File *pFile = (os2File*)id;
  os2ShmLink *p = pFile->pShmLink;      /* The shared memory being locked */
  os2ShmLink *pX;                       /* For looping over all siblings */
  os2ShmNode *pShmNode = p->pShmNode;   /* Our node */
  
  assert( ofst>=0 && ofst+n<=SQLITE_SHM_NLOCK );
  assert( n>=1 );
  assert( flags==(SQLITE_SHM_LOCK | SQLITE_SHM_SHARED)
       || flags==(SQLITE_SHM_LOCK | SQLITE_SHM_EXCLUSIVE)
       || flags==(SQLITE_SHM_UNLOCK | SQLITE_SHM_SHARED)
       || flags==(SQLITE_SHM_UNLOCK | SQLITE_SHM_EXCLUSIVE) );
  assert( n==1 || (flags & SQLITE_SHM_EXCLUSIVE)!=0 );

  mask = (u32)((1U<<(ofst+n)) - (1U<<ofst));
  assert( n>1 || mask==(1<<ofst) );


  sqlite3_mutex_enter(pShmNode->mutex);

  if( flags & SQLITE_SHM_UNLOCK ){
    u32 allMask = 0; /* Mask of locks held by siblings */

    /* See if any siblings hold this same lock */
    for(pX=pShmNode->pFirst; pX; pX=pX->pNext){
      if( pX==p ) continue;
      assert( (pX->exclMask & (p->exclMask|p->sharedMask))==0 );
      allMask |= pX->sharedMask;
    }

    /* Unlock the system-level locks */
    if( (mask & allMask)==0 ){
      rc = os2ShmSystemLock(pShmNode, _SHM_UNLCK, ofst+OS2_SHM_BASE, n);
    }else{
      rc = SQLITE_OK;
    }

    /* Undo the local locks */
    if( rc==SQLITE_OK ){
      p->exclMask &= ~mask;
      p->sharedMask &= ~mask;
    } 
  }else if( flags & SQLITE_SHM_SHARED ){
    u32 allShared = 0;  /* Union of locks held by connections other than "p" */

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
        rc = os2ShmSystemLock(pShmNode, _SHM_RDLCK, ofst+OS2_SHM_BASE, n);
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
      rc = os2ShmSystemLock(pShmNode, _SHM_WRLCK, ofst+OS2_SHM_BASE, n);
      if( rc==SQLITE_OK ){
        assert( (p->sharedMask & mask)==0 );
        p->exclMask |= mask;
      }
    }
  }

  sqlite3_mutex_leave(pShmNode->mutex);
  
  OSTRACE(("SHM-LOCK shmid-%d, pid-%d got %03x,%03x %s\n",
           p->id, (int)GetCurrentProcessId(), p->sharedMask, p->exclMask,
           rc ? "failed" : "ok"));

  ERR_TRACE(rc, ("os2ShmLock: ofst = %d, n = %d, flags = 0x%x -> %d \n", 
                 ofst, n, flags, rc))
                  
  return rc; 
}

/*
** Implement a memory barrier or memory fence on shared memory.
**
** All loads and stores begun before the barrier must complete before
** any load or store begun after the barrier.
*/
static void os2ShmBarrier(
  sqlite3_file *id                /* Database file holding the shared memory */
){
  UNUSED_PARAMETER(id);
  os2ShmEnterMutex();
  os2ShmLeaveMutex();
}

#else
# define os2ShmMap     0
# define os2ShmLock    0
# define os2ShmBarrier 0
# define os2ShmUnmap   0
#endif /* #ifndef SQLITE_OMIT_WAL */


/*
** This vector defines all the methods that can operate on an
** sqlite3_file for os2.
*/
static const sqlite3_io_methods os2IoMethod = {
  2,                              /* iVersion */
  os2Close,                       /* xClose */
  os2Read,                        /* xRead */
  os2Write,                       /* xWrite */
  os2Truncate,                    /* xTruncate */
  os2Sync,                        /* xSync */
  os2FileSize,                    /* xFileSize */
  os2Lock,                        /* xLock */
  os2Unlock,                      /* xUnlock */
  os2CheckReservedLock,           /* xCheckReservedLock */
  os2FileControl,                 /* xFileControl */
  os2SectorSize,                  /* xSectorSize */
  os2DeviceCharacteristics,       /* xDeviceCharacteristics */
  os2ShmMap,                      /* xShmMap */
  os2ShmLock,                     /* xShmLock */
  os2ShmBarrier,                  /* xShmBarrier */
  os2ShmUnmap                     /* xShmUnmap */
};


/***************************************************************************
** Here ends the I/O methods that form the sqlite3_io_methods object.
**
** The next block of code implements the VFS methods.
****************************************************************************/

/*
** Create a temporary file name in zBuf.  zBuf must be big enough to
** hold at pVfs->mxPathname characters.
*/
static int getTempname(int nBuf, char *zBuf ){
  static const char zChars[] =
    "abcdefghijklmnopqrstuvwxyz"
    "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    "0123456789";
  int i, j;
  PSZ zTempPathCp;      
  char zTempPath[CCHMAXPATH];
  ULONG ulDriveNum, ulDriveMap;
  
  /* It's odd to simulate an io-error here, but really this is just
  ** using the io-error infrastructure to test that SQLite handles this
  ** function failing. 
  */
  SimulateIOError( return SQLITE_IOERR );

  if( sqlite3_temp_directory ) {
    sqlite3_snprintf(CCHMAXPATH-30, zTempPath, "%s", sqlite3_temp_directory);
  } else if( DosScanEnv( (PSZ)"TEMP",   &zTempPathCp ) == NO_ERROR ||
             DosScanEnv( (PSZ)"TMP",    &zTempPathCp ) == NO_ERROR ||
             DosScanEnv( (PSZ)"TMPDIR", &zTempPathCp ) == NO_ERROR ) {
    char *zTempPathUTF = convertCpPathToUtf8( (char *)zTempPathCp );
    sqlite3_snprintf(CCHMAXPATH-30, zTempPath, "%s", zTempPathUTF);
    free( zTempPathUTF );
  } else if( DosQueryCurrentDisk( &ulDriveNum, &ulDriveMap ) == NO_ERROR ) {
    zTempPath[0] = (char)('A' + ulDriveNum - 1);
    zTempPath[1] = ':'; 
    zTempPath[2] = '\0'; 
  } else {
    zTempPath[0] = '\0'; 
  }
  
  /* Strip off a trailing slashes or backslashes, otherwise we would get *
   * multiple (back)slashes which causes DosOpen() to fail.              *
   * Trailing spaces are not allowed, either.                            */
  j = sqlite3Strlen30(zTempPath);
  while( j > 0 && ( zTempPath[j-1] == '\\' || zTempPath[j-1] == '/' || 
                    zTempPath[j-1] == ' ' ) ){
    j--;
  }
  zTempPath[j] = '\0';
  
  /* We use 20 bytes to randomize the name */
  sqlite3_snprintf(nBuf-22, zBuf,
                   "%s\\"SQLITE_TEMP_FILE_PREFIX, zTempPath);
  j = sqlite3Strlen30(zBuf);
  sqlite3_randomness( 20, &zBuf[j] );
  for( i = 0; i < 20; i++, j++ ){
    zBuf[j] = zChars[ ((unsigned char)zBuf[j])%(sizeof(zChars)-1) ];
  }
  zBuf[j] = 0;

  OSTRACE(( "TEMP FILENAME: %s\n", zBuf ));
  return SQLITE_OK;
}


/*
** Turn a relative pathname into a full pathname.  Write the full
** pathname into zFull[].  zFull[] will be at least pVfs->mxPathname
** bytes in size.
*/
static int os2FullPathname(
  sqlite3_vfs *pVfs,          /* Pointer to vfs object */
  const char *zRelative,      /* Possibly relative input path */
  int nFull,                  /* Size of output buffer in bytes */
  char *zFull                 /* Output buffer */
){
  char *zRelativeCp = convertUtf8PathToCp( zRelative );
  char zFullCp[CCHMAXPATH] = "\0";
  char *zFullUTF;
  APIRET rc = DosQueryPathInfo( (PSZ)zRelativeCp, FIL_QUERYFULLNAME, 
                                zFullCp, CCHMAXPATH );
  free( zRelativeCp );
  zFullUTF = convertCpPathToUtf8( zFullCp );
  sqlite3_snprintf( nFull, zFull, zFullUTF );
  free( zFullUTF );
  return rc == NO_ERROR ? SQLITE_OK : SQLITE_IOERR;
}


/*
** Open a file.
*/
static int os2Open(
  sqlite3_vfs *pVfs,            /* Not used */
  const char *zName,            /* Name of the file (UTF-8) */
  sqlite3_file *id,             /* Write the SQLite file handle here */
  int flags,                    /* Open mode flags */
  int *pOutFlags                /* Status return flags */
){
  HFILE h;
  ULONG ulOpenFlags = 0;
  ULONG ulOpenMode = 0;
  ULONG ulAction = 0;
  ULONG rc;
  os2File *pFile = (os2File*)id;
  const char *zUtf8Name = zName;
  char *zNameCp;
  char  zTmpname[CCHMAXPATH];

  int isExclusive  = (flags & SQLITE_OPEN_EXCLUSIVE);
  int isCreate     = (flags & SQLITE_OPEN_CREATE);
  int isReadWrite  = (flags & SQLITE_OPEN_READWRITE);
#ifndef NDEBUG
  int isDelete     = (flags & SQLITE_OPEN_DELETEONCLOSE);
  int isReadonly   = (flags & SQLITE_OPEN_READONLY);
  int eType        = (flags & 0xFFFFFF00);
  int isOpenJournal = (isCreate && (
        eType==SQLITE_OPEN_MASTER_JOURNAL 
     || eType==SQLITE_OPEN_MAIN_JOURNAL 
     || eType==SQLITE_OPEN_WAL
  ));
#endif

  UNUSED_PARAMETER(pVfs);
  assert( id!=0 );

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

  memset( pFile, 0, sizeof(*pFile) );
  pFile->h = (HFILE)-1;

  /* If the second argument to this function is NULL, generate a 
  ** temporary file name to use 
  */
  if( !zUtf8Name ){
    assert(isDelete && !isOpenJournal);
    rc = getTempname(CCHMAXPATH, zTmpname);
    if( rc!=SQLITE_OK ){
      return rc;
    }
    zUtf8Name = zTmpname;
  }

  if( isReadWrite ){
    ulOpenMode |= OPEN_ACCESS_READWRITE;
  }else{
    ulOpenMode |= OPEN_ACCESS_READONLY;
  }

  /* Open in random access mode for possibly better speed.  Allow full
  ** sharing because file locks will provide exclusive access when needed.
  ** The handle should not be inherited by child processes and we don't 
  ** want popups from the critical error handler.
  */
  ulOpenMode |= OPEN_FLAGS_RANDOM | OPEN_SHARE_DENYNONE | 
                OPEN_FLAGS_NOINHERIT | OPEN_FLAGS_FAIL_ON_ERROR;

  /* SQLITE_OPEN_EXCLUSIVE is used to make sure that a new file is 
  ** created. SQLite doesn't use it to indicate "exclusive access" 
  ** as it is usually understood.
  */
  if( isExclusive ){
    /* Creates a new file, only if it does not already exist. */
    /* If the file exists, it fails. */
    ulOpenFlags |= OPEN_ACTION_CREATE_IF_NEW | OPEN_ACTION_FAIL_IF_EXISTS;
  }else if( isCreate ){
    /* Open existing file, or create if it doesn't exist */
    ulOpenFlags |= OPEN_ACTION_CREATE_IF_NEW | OPEN_ACTION_OPEN_IF_EXISTS;
  }else{
    /* Opens a file, only if it exists. */
    ulOpenFlags |= OPEN_ACTION_FAIL_IF_NEW | OPEN_ACTION_OPEN_IF_EXISTS;
  }

  zNameCp = convertUtf8PathToCp( zUtf8Name );
  rc = DosOpen( (PSZ)zNameCp,
                &h,
                &ulAction,
                0L,
                FILE_NORMAL,
                ulOpenFlags,
                ulOpenMode,
                (PEAOP2)NULL );
  free( zNameCp );

  if( rc != NO_ERROR ){
    OSTRACE(( "OPEN Invalid handle rc=%d: zName=%s, ulAction=%#lx, ulFlags=%#lx, ulMode=%#lx\n",
              rc, zUtf8Name, ulAction, ulOpenFlags, ulOpenMode ));

    if( isReadWrite ){
      return os2Open( pVfs, zName, id,
                      ((flags|SQLITE_OPEN_READONLY)&~(SQLITE_OPEN_CREATE|SQLITE_OPEN_READWRITE)),
                      pOutFlags );
    }else{
      return SQLITE_CANTOPEN;
    }
  }

  if( pOutFlags ){
    *pOutFlags = isReadWrite ? SQLITE_OPEN_READWRITE : SQLITE_OPEN_READONLY;
  }

  os2FullPathname( pVfs, zUtf8Name, sizeof( zTmpname ), zTmpname );
  pFile->zFullPathCp = convertUtf8PathToCp( zTmpname );
  pFile->pMethod = &os2IoMethod;
  pFile->flags = flags;
  pFile->h = h;

  OpenCounter(+1);
  OSTRACE(( "OPEN %d pOutFlags=%d\n", pFile->h, pOutFlags ));
  return SQLITE_OK;
}

/*
** Delete the named file.
*/
static int os2Delete(
  sqlite3_vfs *pVfs,                     /* Not used on os2 */
  const char *zFilename,                 /* Name of file to delete */
  int syncDir                            /* Not used on os2 */
){
  APIRET rc;
  char *zFilenameCp;
  SimulateIOError( return SQLITE_IOERR_DELETE );
  zFilenameCp = convertUtf8PathToCp( zFilename );
  rc = DosDelete( (PSZ)zFilenameCp );
  free( zFilenameCp );
  OSTRACE(( "DELETE \"%s\"\n", zFilename ));
  return (rc == NO_ERROR ||
          rc == ERROR_FILE_NOT_FOUND ||
          rc == ERROR_PATH_NOT_FOUND ) ? SQLITE_OK : SQLITE_IOERR_DELETE;
}

/*
** Check the existance and status of a file.
*/
static int os2Access(
  sqlite3_vfs *pVfs,        /* Not used on os2 */
  const char *zFilename,    /* Name of file to check */
  int flags,                /* Type of test to make on this file */
  int *pOut                 /* Write results here */
){
  APIRET rc;
  FILESTATUS3 fsts3ConfigInfo;
  char *zFilenameCp;

  UNUSED_PARAMETER(pVfs);
  SimulateIOError( return SQLITE_IOERR_ACCESS; );
  
  zFilenameCp = convertUtf8PathToCp( zFilename );
  rc = DosQueryPathInfo( (PSZ)zFilenameCp, FIL_STANDARD,
                         &fsts3ConfigInfo, sizeof(FILESTATUS3) );
  free( zFilenameCp );
  OSTRACE(( "ACCESS fsts3ConfigInfo.attrFile=%d flags=%d rc=%d\n",
            fsts3ConfigInfo.attrFile, flags, rc ));

  switch( flags ){
    case SQLITE_ACCESS_EXISTS:
      /* For an SQLITE_ACCESS_EXISTS query, treat a zero-length file
      ** as if it does not exist.
      */
      if( fsts3ConfigInfo.cbFile == 0 ) 
        rc = ERROR_FILE_NOT_FOUND;
      break;
    case SQLITE_ACCESS_READ:
      break;
    case SQLITE_ACCESS_READWRITE:
      if( fsts3ConfigInfo.attrFile & FILE_READONLY )
        rc = ERROR_ACCESS_DENIED;
      break;
    default:
      rc = ERROR_FILE_NOT_FOUND;
      assert( !"Invalid flags argument" );
  }

  *pOut = (rc == NO_ERROR);
  OSTRACE(( "ACCESS %s flags %d: rc=%d\n", zFilename, flags, *pOut ));

  return SQLITE_OK;
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
static void *os2DlOpen(sqlite3_vfs *pVfs, const char *zFilename){
  HMODULE hmod;
  APIRET rc;
  char *zFilenameCp = convertUtf8PathToCp(zFilename);
  rc = DosLoadModule(NULL, 0, (PSZ)zFilenameCp, &hmod);
  free(zFilenameCp);
  return rc != NO_ERROR ? 0 : (void*)hmod;
}
/*
** A no-op since the error code is returned on the DosLoadModule call.
** os2Dlopen returns zero if DosLoadModule is not successful.
*/
static void os2DlError(sqlite3_vfs *pVfs, int nBuf, char *zBufOut){
/* no-op */
}
static void (*os2DlSym(sqlite3_vfs *pVfs, void *pHandle, const char *zSymbol))(void){
  PFN pfn;
  APIRET rc;
  rc = DosQueryProcAddr((HMODULE)pHandle, 0L, (PSZ)zSymbol, &pfn);
  if( rc != NO_ERROR ){
    /* if the symbol itself was not found, search again for the same
     * symbol with an extra underscore, that might be needed depending
     * on the calling convention */
    char _zSymbol[256] = "_";
    strncat(_zSymbol, zSymbol, 254);
    rc = DosQueryProcAddr((HMODULE)pHandle, 0L, (PSZ)_zSymbol, &pfn);
  }
  return rc != NO_ERROR ? 0 : (void(*)(void))pfn;
}
static void os2DlClose(sqlite3_vfs *pVfs, void *pHandle){
  DosFreeModule((HMODULE)pHandle);
}
#else /* if SQLITE_OMIT_LOAD_EXTENSION is defined: */
  #define os2DlOpen 0
  #define os2DlError 0
  #define os2DlSym 0
  #define os2DlClose 0
#endif


/*
** Write up to nBuf bytes of randomness into zBuf.
*/
static int os2Randomness(sqlite3_vfs *pVfs, int nBuf, char *zBuf ){
  int n = 0;
#if defined(SQLITE_TEST)
  n = nBuf;
  memset(zBuf, 0, nBuf);
#else
  int i;                           
  PPIB ppib;
  PTIB ptib;
  DATETIME dt; 
  static unsigned c = 0;
  /* Ordered by variation probability */
  static ULONG svIdx[6] = { QSV_MS_COUNT, QSV_TIME_LOW,
                            QSV_MAXPRMEM, QSV_MAXSHMEM,
                            QSV_TOTAVAILMEM, QSV_TOTRESMEM };

  /* 8 bytes; timezone and weekday don't increase the randomness much */
  if( (int)sizeof(dt)-3 <= nBuf - n ){
    c += 0x0100;
    DosGetDateTime(&dt);
    dt.year = (USHORT)((dt.year - 1900) | c);
    memcpy(&zBuf[n], &dt, sizeof(dt)-3);
    n += sizeof(dt)-3;
  }

  /* 4 bytes; PIDs and TIDs are 16 bit internally, so combine them */
  if( (int)sizeof(ULONG) <= nBuf - n ){
    DosGetInfoBlocks(&ptib, &ppib);
    *(PULONG)&zBuf[n] = MAKELONG(ppib->pib_ulpid,
                                 ptib->tib_ptib2->tib2_ultid);
    n += sizeof(ULONG);
  }

  /* Up to 6 * 4 bytes; variables depend on the system state */
  for( i = 0; i < 6 && (int)sizeof(ULONG) <= nBuf - n; i++ ){
    DosQuerySysInfo(svIdx[i], svIdx[i], 
                    (PULONG)&zBuf[n], sizeof(ULONG));
    n += sizeof(ULONG);
  } 
#endif

  return n;
}

/*
** Sleep for a little while.  Return the amount of time slept.
** The argument is the number of microseconds we want to sleep.
** The return value is the number of microseconds of sleep actually
** requested from the underlying operating system, a number which
** might be greater than or equal to the argument, but not less
** than the argument.
*/
static int os2Sleep( sqlite3_vfs *pVfs, int microsec ){
  DosSleep( (microsec/1000) );
  return microsec;
}

/*
** The following variable, if set to a non-zero value, becomes the result
** returned from sqlite3OsCurrentTime().  This is used for testing.
*/
#ifdef SQLITE_TEST
int sqlite3_current_time = 0;
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
static int os2CurrentTimeInt64(sqlite3_vfs *pVfs, sqlite3_int64 *piNow){
#ifdef SQLITE_TEST
  static const sqlite3_int64 unixEpoch = 24405875*(sqlite3_int64)8640000;
#endif
  int year, month, datepart, timepart;
 
  DATETIME dt;
  DosGetDateTime( &dt );

  year = dt.year;
  month = dt.month;

  /* Calculations from http://www.astro.keele.ac.uk/~rno/Astronomy/hjd.html
  ** http://www.astro.keele.ac.uk/~rno/Astronomy/hjd-0.1.c
  ** Calculate the Julian days
  */
  datepart = (int)dt.day - 32076 +
    1461*(year + 4800 + (month - 14)/12)/4 +
    367*(month - 2 - (month - 14)/12*12)/12 -
    3*((year + 4900 + (month - 14)/12)/100)/4;

  /* Time in milliseconds, hours to noon added */
  timepart = 12*3600*1000 + dt.hundredths*10 + dt.seconds*1000 +
    ((int)dt.minutes + dt.timezone)*60*1000 + dt.hours*3600*1000;

  *piNow = (sqlite3_int64)datepart*86400*1000 + timepart;
   
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
static int os2CurrentTime( sqlite3_vfs *pVfs, double *prNow ){
  int rc;
  sqlite3_int64 i;
  rc = os2CurrentTimeInt64(pVfs, &i);
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
static int os2GetLastError(sqlite3_vfs *pVfs, int nBuf, char *zBuf){
  assert(zBuf[0]=='\0');
  return 0;
}

/*
** Initialize and deinitialize the operating system interface.
*/
int sqlite3_os_init(void){
  static sqlite3_vfs os2Vfs = {
    3,                 /* iVersion */
    sizeof(os2File),   /* szOsFile */
    CCHMAXPATH,        /* mxPathname */
    0,                 /* pNext */
    "os2",             /* zName */
    0,                 /* pAppData */

    os2Open,           /* xOpen */
    os2Delete,         /* xDelete */
    os2Access,         /* xAccess */
    os2FullPathname,   /* xFullPathname */
    os2DlOpen,         /* xDlOpen */
    os2DlError,        /* xDlError */
    os2DlSym,          /* xDlSym */
    os2DlClose,        /* xDlClose */
    os2Randomness,     /* xRandomness */
    os2Sleep,          /* xSleep */
    os2CurrentTime,    /* xCurrentTime */
    os2GetLastError,   /* xGetLastError */
    os2CurrentTimeInt64, /* xCurrentTimeInt64 */
    0,                 /* xSetSystemCall */
    0,                 /* xGetSystemCall */
    0                  /* xNextSystemCall */
  };
  sqlite3_vfs_register(&os2Vfs, 1);
  initUconvObjects();
/*  sqlite3OSTrace = 1; */
  return SQLITE_OK;
}
int sqlite3_os_end(void){
  freeUconvObjects();
  return SQLITE_OK;
}

#endif /* SQLITE_OS_OS2 */

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
#include "os.h"

#if OS_OS2

/*
** Macros used to determine whether or not to use threads.
*/
#if defined(THREADSAFE) && THREADSAFE
# define SQLITE_OS2_THREADS 1
#endif

/*
** Include code that is common to all os_*.c files
*/
#include "os_common.h"

/*
** The os2File structure is subclass of OsFile specific for the OS/2
** protability layer.
*/
typedef struct os2File os2File;
struct os2File {
  IoMethod const *pMethod;  /* Always the first entry */
  HFILE h;                  /* Handle for accessing the file */
  int delOnClose;           /* True if file is to be deleted on close */
  char* pathToDel;          /* Name of file to delete on close */
  unsigned char locktype;   /* Type of lock currently held on this file */
};

/*
** Do not include any of the File I/O interface procedures if the
** SQLITE_OMIT_DISKIO macro is defined (indicating that there database
** will be in-memory only)
*/
#ifndef SQLITE_OMIT_DISKIO

/*
** Delete the named file
*/
int sqlite3Os2Delete( const char *zFilename ){
  DosDelete( (PSZ)zFilename );
  TRACE2( "DELETE \"%s\"\n", zFilename );
  return SQLITE_OK;
}

/*
** Return TRUE if the named file exists.
*/
int sqlite3Os2FileExists( const char *zFilename ){
  FILESTATUS3 fsts3ConfigInfo;
  memset(&fsts3ConfigInfo, 0, sizeof(fsts3ConfigInfo));
  return DosQueryPathInfo( (PSZ)zFilename, FIL_STANDARD,
        &fsts3ConfigInfo, sizeof(FILESTATUS3) ) == NO_ERROR;
}

/* Forward declaration */
int allocateOs2File( os2File *pInit, OsFile **pld );

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
int sqlite3Os2OpenReadWrite(
  const char *zFilename,
  OsFile **pld,
  int *pReadonly
){
  os2File  f;
  HFILE    hf;
  ULONG    ulAction;
  APIRET   rc;

  assert( *pld == 0 );
  rc = DosOpen( (PSZ)zFilename, &hf, &ulAction, 0L,
            FILE_ARCHIVED | FILE_NORMAL,
                OPEN_ACTION_CREATE_IF_NEW | OPEN_ACTION_OPEN_IF_EXISTS,
                OPEN_FLAGS_FAIL_ON_ERROR | OPEN_FLAGS_RANDOM |
                    OPEN_SHARE_DENYNONE | OPEN_ACCESS_READWRITE, (PEAOP2)NULL );
  if( rc != NO_ERROR ){
    rc = DosOpen( (PSZ)zFilename, &hf, &ulAction, 0L,
            FILE_ARCHIVED | FILE_NORMAL,
                OPEN_ACTION_CREATE_IF_NEW | OPEN_ACTION_OPEN_IF_EXISTS,
                OPEN_FLAGS_FAIL_ON_ERROR | OPEN_FLAGS_RANDOM |
                        OPEN_SHARE_DENYWRITE | OPEN_ACCESS_READONLY, (PEAOP2)NULL );
    if( rc != NO_ERROR ){
        return SQLITE_CANTOPEN;
    }
    *pReadonly = 1;
  }
  else{
    *pReadonly = 0;
  }
  f.h = hf;
  f.locktype = NO_LOCK;
  f.delOnClose = 0;
  f.pathToDel = NULL;
  OpenCounter(+1);
  TRACE3( "OPEN R/W %d \"%s\"\n", hf, zFilename );
  return allocateOs2File( &f, pld );
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
int sqlite3Os2OpenExclusive( const char *zFilename, OsFile **pld, int delFlag ){
  os2File  f;
  HFILE    hf;
  ULONG    ulAction;
  APIRET   rc;

  assert( *pld == 0 );
  rc = DosOpen( (PSZ)zFilename, &hf, &ulAction, 0L, FILE_NORMAL,
            OPEN_ACTION_CREATE_IF_NEW | OPEN_ACTION_REPLACE_IF_EXISTS,
            OPEN_FLAGS_FAIL_ON_ERROR | OPEN_FLAGS_RANDOM |
                OPEN_SHARE_DENYREADWRITE | OPEN_ACCESS_READWRITE, (PEAOP2)NULL );
  if( rc != NO_ERROR ){
    return SQLITE_CANTOPEN;
  }

  f.h = hf;
  f.locktype = NO_LOCK;
  f.delOnClose = delFlag ? 1 : 0;
  f.pathToDel = delFlag ? sqlite3OsFullPathname( zFilename ) : NULL;
  OpenCounter( +1 );
  if( delFlag ) DosForceDelete( sqlite3OsFullPathname( zFilename ) );
  TRACE3( "OPEN EX %d \"%s\"\n", hf, sqlite3OsFullPathname ( zFilename ) );
  return allocateOs2File( &f, pld );
}

/*
** Attempt to open a new file for read-only access.
**
** On success, write the file handle into *id and return SQLITE_OK.
**
** On failure, return SQLITE_CANTOPEN.
*/
int sqlite3Os2OpenReadOnly( const char *zFilename, OsFile **pld ){
  os2File  f;
  HFILE    hf;
  ULONG    ulAction;
  APIRET   rc;

  assert( *pld == 0 );
  rc = DosOpen( (PSZ)zFilename, &hf, &ulAction, 0L,
            FILE_NORMAL, OPEN_ACTION_OPEN_IF_EXISTS,
            OPEN_FLAGS_FAIL_ON_ERROR | OPEN_FLAGS_RANDOM |
                OPEN_SHARE_DENYWRITE | OPEN_ACCESS_READONLY, (PEAOP2)NULL );
  if( rc != NO_ERROR ){
    return SQLITE_CANTOPEN;
  }
  f.h = hf;
  f.locktype = NO_LOCK;
  f.delOnClose = 0;
  f.pathToDel = NULL;
  OpenCounter( +1 );
  TRACE3( "OPEN RO %d \"%s\"\n", hf, zFilename );
  return allocateOs2File( &f, pld );
}

/*
** Attempt to open a file descriptor for the directory that contains a
** file.  This file descriptor can be used to fsync() the directory
** in order to make sure the creation of a new file is actually written
** to disk.
**
** This routine is only meaningful for Unix.  It is a no-op under
** OS/2 since OS/2 does not support hard links.
**
** On success, a handle for a previously open file is at *id is
** updated with the new directory file descriptor and SQLITE_OK is
** returned.
**
** On failure, the function returns SQLITE_CANTOPEN and leaves
** *id unchanged.
*/
int os2OpenDirectory(
  OsFile *id,
  const char *zDirname
){
  return SQLITE_OK;
}

/*
** If the following global variable points to a string which is the
** name of a directory, then that directory will be used to store
** temporary files.
*/
char *sqlite3_temp_directory = 0;

/*
** Create a temporary file name in zBuf.  zBuf must be big enough to
** hold at least SQLITE_TEMPNAME_SIZE characters.
*/
int sqlite3Os2TempFileName( char *zBuf ){
  static const unsigned char zChars[] =
    "abcdefghijklmnopqrstuvwxyz"
    "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    "0123456789";
  int i, j;
  PSZ zTempPath = 0;
  if( DosScanEnv( "TEMP", &zTempPath ) ){
    if( DosScanEnv( "TMP", &zTempPath ) ){
      if( DosScanEnv( "TMPDIR", &zTempPath ) ){
           ULONG ulDriveNum = 0, ulDriveMap = 0;
           DosQueryCurrentDisk( &ulDriveNum, &ulDriveMap );
           sprintf( zTempPath, "%c:", (char)( 'A' + ulDriveNum - 1 ) );
      }
    }
  }
  for(;;){
      sprintf( zBuf, "%s\\"TEMP_FILE_PREFIX, zTempPath );
      j = strlen( zBuf );
      sqlite3Randomness( 15, &zBuf[j] );
      for( i = 0; i < 15; i++, j++ ){
        zBuf[j] = (char)zChars[ ((unsigned char)zBuf[j])%(sizeof(zChars)-1) ];
      }
      zBuf[j] = 0;
      if( !sqlite3OsFileExists( zBuf ) ) break;
  }
  TRACE2( "TEMP FILENAME: %s\n", zBuf );
  return SQLITE_OK;
}

/*
** Close a file.
*/
int os2Close( OsFile **pld ){
  os2File *pFile;
  if( pld && (pFile = (os2File*)*pld)!=0 ){
    TRACE2( "CLOSE %d\n", pFile->h );
    DosClose( pFile->h );
    pFile->locktype = NO_LOCK;
    if( pFile->delOnClose != 0 ){
        DosForceDelete( pFile->pathToDel );
    }
    *pld = 0;
    OpenCounter( -1 );
  }

  return SQLITE_OK;
}

/*
** Read data from a file into a buffer.  Return SQLITE_OK if all
** bytes were read successfully and SQLITE_IOERR if anything goes
** wrong.
*/
int os2Read( OsFile *id, void *pBuf, int amt ){
  ULONG got;
  assert( id!=0 );
  SimulateIOError( SQLITE_IOERR );
  TRACE3( "READ %d lock=%d\n", ((os2File*)id)->h, ((os2File*)id)->locktype );
  DosRead( ((os2File*)id)->h, pBuf, amt, &got );
  return (got == (ULONG)amt) ? SQLITE_OK : SQLITE_IOERR;
}

/*
** Write data from a buffer into a file.  Return SQLITE_OK on success
** or some other error code on failure.
*/
int os2Write( OsFile *id, const void *pBuf, int amt ){
  APIRET rc=NO_ERROR;
  ULONG wrote;
  assert( id!=0 );
  SimulateIOError( SQLITE_IOERR );
  SimulateDiskfullError;
  TRACE3( "WRITE %d lock=%d\n", ((os2File*)id)->h, ((os2File*)id)->locktype );
  while( amt > 0 &&
      (rc = DosWrite( ((os2File*)id)->h, (PVOID)pBuf, amt, &wrote )) && wrote > 0 ){
      amt -= wrote;
      pBuf = &((char*)pBuf)[wrote];
  }

  return ( rc != NO_ERROR || amt > (int)wrote ) ? SQLITE_FULL : SQLITE_OK;
}

/*
** Move the read/write pointer in a file.
*/
int os2Seek( OsFile *id, i64 offset ){
  APIRET rc;
  ULONG filePointer = 0L;
  assert( id!=0 );
  rc = DosSetFilePtr( ((os2File*)id)->h, offset, FILE_BEGIN, &filePointer );
  TRACE3( "SEEK %d %lld\n", ((os2File*)id)->h, offset );
  return rc == NO_ERROR ? SQLITE_OK : SQLITE_IOERR;
}

/*
** Make sure all writes to a particular file are committed to disk.
*/
int os2Sync( OsFile *id, int dataOnly ){
  assert( id!=0 );
  TRACE3( "SYNC %d lock=%d\n", ((os2File*)id)->h, ((os2File*)id)->locktype );
  return DosResetBuffer( ((os2File*)id)->h ) ? SQLITE_IOERR : SQLITE_OK;
}

/*
** Sync the directory zDirname. This is a no-op on operating systems other
** than UNIX.
*/
int sqlite3Os2SyncDirectory( const char *zDirname ){
  SimulateIOError( SQLITE_IOERR );
  return SQLITE_OK;
}

/*
** Truncate an open file to a specified size
*/
int os2Truncate( OsFile *id, i64 nByte ){
  APIRET rc;
  ULONG upperBits = nByte>>32;
  assert( id!=0 );
  TRACE3( "TRUNCATE %d %lld\n", ((os2File*)id)->h, nByte );
  SimulateIOError( SQLITE_IOERR );
  rc = DosSetFilePtr( ((os2File*)id)->h, nByte, FILE_BEGIN, &upperBits );
  if( rc != NO_ERROR ){
    return SQLITE_IOERR;
  }
  rc = DosSetFilePtr( ((os2File*)id)->h, 0L, FILE_END, &upperBits );
  return rc == NO_ERROR ? SQLITE_OK : SQLITE_IOERR;
}

/*
** Determine the current size of a file in bytes
*/
int os2FileSize( OsFile *id, i64 *pSize ){
  APIRET rc;
  FILESTATUS3 fsts3FileInfo;
  memset(&fsts3FileInfo, 0, sizeof(fsts3FileInfo));
  assert( id!=0 );
  SimulateIOError( SQLITE_IOERR );
  rc = DosQueryFileInfo( ((os2File*)id)->h, FIL_STANDARD, &fsts3FileInfo, sizeof(FILESTATUS3) );
  if( rc == NO_ERROR ){
    *pSize = fsts3FileInfo.cbFile;
    return SQLITE_OK;
  }
  else{
    return SQLITE_IOERR;
  }
}

/*
** Acquire a reader lock.
*/
static int getReadLock( os2File *id ){
  FILELOCK  LockArea,
            UnlockArea;
  memset(&LockArea, 0, sizeof(LockArea));
  memset(&UnlockArea, 0, sizeof(UnlockArea));
  LockArea.lOffset = SHARED_FIRST;
  LockArea.lRange = SHARED_SIZE;
  UnlockArea.lOffset = 0L;
  UnlockArea.lRange = 0L;
  return DosSetFileLocks( id->h, &UnlockArea, &LockArea, 2000L, 1L );
}

/*
** Undo a readlock
*/
static int unlockReadLock( os2File *id ){
  FILELOCK  LockArea,
            UnlockArea;
  memset(&LockArea, 0, sizeof(LockArea));
  memset(&UnlockArea, 0, sizeof(UnlockArea));
  LockArea.lOffset = 0L;
  LockArea.lRange = 0L;
  UnlockArea.lOffset = SHARED_FIRST;
  UnlockArea.lRange = SHARED_SIZE;
  return DosSetFileLocks( id->h, &UnlockArea, &LockArea, 2000L, 1L );
}

#ifndef SQLITE_OMIT_PAGER_PRAGMAS
/*
** Check that a given pathname is a directory and is writable
**
*/
int sqlite3Os2IsDirWritable( char *zDirname ){
  FILESTATUS3 fsts3ConfigInfo;
  APIRET rc = NO_ERROR;
  memset(&fsts3ConfigInfo, 0, sizeof(fsts3ConfigInfo));
  if( zDirname==0 ) return 0;
  if( strlen(zDirname)>CCHMAXPATH ) return 0;
  rc = DosQueryPathInfo( (PSZ)zDirname, FIL_STANDARD, &fsts3ConfigInfo, sizeof(FILESTATUS3) );
  if( rc != NO_ERROR ) return 0;
  if( (fsts3ConfigInfo.attrFile & FILE_DIRECTORY) != FILE_DIRECTORY ) return 0;

  return 1;
}
#endif /* SQLITE_OMIT_PAGER_PRAGMAS */

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
int os2Lock( OsFile *id, int locktype ){
  APIRET rc = SQLITE_OK;    /* Return code from subroutines */
  APIRET res = 1;           /* Result of a windows lock call */
  int newLocktype;       /* Set id->locktype to this value before exiting */
  int gotPendingLock = 0;/* True if we acquired a PENDING lock this time */
  FILELOCK  LockArea,
            UnlockArea;
  os2File *pFile = (os2File*)id;
  memset(&LockArea, 0, sizeof(LockArea));
  memset(&UnlockArea, 0, sizeof(UnlockArea));
  assert( pFile!=0 );
  TRACE4( "LOCK %d %d was %d\n", pFile->h, locktype, pFile->locktype );

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
  if( pFile->locktype==NO_LOCK
   || (locktype==EXCLUSIVE_LOCK && pFile->locktype==RESERVED_LOCK)
  ){
    int cnt = 3;

    LockArea.lOffset = PENDING_BYTE;
    LockArea.lRange = 1L;
    UnlockArea.lOffset = 0L;
    UnlockArea.lRange = 0L;

    while( cnt-->0 && (res = DosSetFileLocks( pFile->h, &UnlockArea, &LockArea, 2000L, 1L) )!=NO_ERROR ){
      /* Try 3 times to get the pending lock.  The pending lock might be
      ** held by another reader process who will release it momentarily.
      */
      TRACE2( "could not get a PENDING lock. cnt=%d\n", cnt );
      DosSleep(1);
    }
    gotPendingLock = res;
  }

  /* Acquire a shared lock
  */
  if( locktype==SHARED_LOCK && res ){
    assert( pFile->locktype==NO_LOCK );
    res = getReadLock(pFile);
    if( res == NO_ERROR ){
      newLocktype = SHARED_LOCK;
    }
  }

  /* Acquire a RESERVED lock
  */
  if( locktype==RESERVED_LOCK && res ){
    assert( pFile->locktype==SHARED_LOCK );
    LockArea.lOffset = RESERVED_BYTE;
    LockArea.lRange = 1L;
    UnlockArea.lOffset = 0L;
    UnlockArea.lRange = 0L;
    res = DosSetFileLocks( pFile->h, &UnlockArea, &LockArea, 2000L, 1L );
    if( res == NO_ERROR ){
      newLocktype = RESERVED_LOCK;
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
    TRACE2( "unreadlock = %d\n", res );
    LockArea.lOffset = SHARED_FIRST;
    LockArea.lRange = SHARED_SIZE;
    UnlockArea.lOffset = 0L;
    UnlockArea.lRange = 0L;
    res = DosSetFileLocks( pFile->h, &UnlockArea, &LockArea, 2000L, 1L );
    if( res == NO_ERROR ){
      newLocktype = EXCLUSIVE_LOCK;
    }else{
      TRACE2( "error-code = %d\n", res );
    }
  }

  /* If we are holding a PENDING lock that ought to be released, then
  ** release it now.
  */
  if( gotPendingLock && locktype==SHARED_LOCK ){
    LockArea.lOffset = 0L;
    LockArea.lRange = 0L;
    UnlockArea.lOffset = PENDING_BYTE;
    UnlockArea.lRange = 1L;
    DosSetFileLocks( pFile->h, &UnlockArea, &LockArea, 2000L, 1L );
  }

  /* Update the state of the lock has held in the file descriptor then
  ** return the appropriate result code.
  */
  if( res == NO_ERROR ){
    rc = SQLITE_OK;
  }else{
    TRACE4( "LOCK FAILED %d trying for %d but got %d\n", pFile->h,
           locktype, newLocktype );
    rc = SQLITE_BUSY;
  }
  pFile->locktype = newLocktype;
  return rc;
}

/*
** This routine checks if there is a RESERVED lock held on the specified
** file by this or any other process. If such a lock is held, return
** non-zero, otherwise zero.
*/
int os2CheckReservedLock( OsFile *id ){
  APIRET rc;
  os2File *pFile = (os2File*)id;
  assert( pFile!=0 );
  if( pFile->locktype>=RESERVED_LOCK ){
    rc = 1;
    TRACE3( "TEST WR-LOCK %d %d (local)\n", pFile->h, rc );
  }else{
    FILELOCK  LockArea,
              UnlockArea;
    memset(&LockArea, 0, sizeof(LockArea));
    memset(&UnlockArea, 0, sizeof(UnlockArea));
    LockArea.lOffset = RESERVED_BYTE;
    LockArea.lRange = 1L;
    UnlockArea.lOffset = 0L;
    UnlockArea.lRange = 0L;
    rc = DosSetFileLocks( pFile->h, &UnlockArea, &LockArea, 2000L, 1L );
    if( rc == NO_ERROR ){
      LockArea.lOffset = 0L;
      LockArea.lRange = 0L;
      UnlockArea.lOffset = RESERVED_BYTE;
      UnlockArea.lRange = 1L;
      rc = DosSetFileLocks( pFile->h, &UnlockArea, &LockArea, 2000L, 1L );
    }
    TRACE3( "TEST WR-LOCK %d %d (remote)\n", pFile->h, rc );
  }
  return rc;
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
int os2Unlock( OsFile *id, int locktype ){
  int type;
  APIRET rc = SQLITE_OK;
  os2File *pFile = (os2File*)id;
  FILELOCK  LockArea,
            UnlockArea;
  memset(&LockArea, 0, sizeof(LockArea));
  memset(&UnlockArea, 0, sizeof(UnlockArea));
  assert( pFile!=0 );
  assert( locktype<=SHARED_LOCK );
  TRACE4( "UNLOCK %d to %d was %d\n", pFile->h, locktype, pFile->locktype );
  type = pFile->locktype;
  if( type>=EXCLUSIVE_LOCK ){
    LockArea.lOffset = 0L;
    LockArea.lRange = 0L;
    UnlockArea.lOffset = SHARED_FIRST;
    UnlockArea.lRange = SHARED_SIZE;
    DosSetFileLocks( pFile->h, &UnlockArea, &LockArea, 2000L, 1L );
    if( locktype==SHARED_LOCK && getReadLock(pFile) != NO_ERROR ){
      /* This should never happen.  We should always be able to
      ** reacquire the read lock */
      rc = SQLITE_IOERR;
    }
  }
  if( type>=RESERVED_LOCK ){
    LockArea.lOffset = 0L;
    LockArea.lRange = 0L;
    UnlockArea.lOffset = RESERVED_BYTE;
    UnlockArea.lRange = 1L;
    DosSetFileLocks( pFile->h, &UnlockArea, &LockArea, 2000L, 1L );
  }
  if( locktype==NO_LOCK && type>=SHARED_LOCK ){
    unlockReadLock(pFile);
  }
  if( type>=PENDING_LOCK ){
    LockArea.lOffset = 0L;
    LockArea.lRange = 0L;
    UnlockArea.lOffset = PENDING_BYTE;
    UnlockArea.lRange = 1L;
    DosSetFileLocks( pFile->h, &UnlockArea, &LockArea, 2000L, 1L );
  }
  pFile->locktype = locktype;
  return rc;
}

/*
** Turn a relative pathname into a full pathname.  Return a pointer
** to the full pathname stored in space obtained from sqliteMalloc().
** The calling function is responsible for freeing this space once it
** is no longer needed.
*/
char *sqlite3Os2FullPathname( const char *zRelative ){
  char *zFull = 0;
  if( strchr(zRelative, ':') ){
    sqlite3SetString( &zFull, zRelative, (char*)0 );
  }else{
    char zBuff[SQLITE_TEMPNAME_SIZE - 2] = {0};
    char zDrive[1] = {0};
    ULONG cbzFullLen = SQLITE_TEMPNAME_SIZE;
    ULONG ulDriveNum = 0;
    ULONG ulDriveMap = 0;
    DosQueryCurrentDisk( &ulDriveNum, &ulDriveMap );
    DosQueryCurrentDir( 0L, zBuff, &cbzFullLen );
    zFull = sqliteMalloc( cbzFullLen );
    sprintf( zDrive, "%c", (char)('A' + ulDriveNum - 1) );
    sqlite3SetString( &zFull, zDrive, ":\\", zBuff, "\\", zRelative, (char*)0 );
  }
  return zFull;
}

/*
** The fullSync option is meaningless on os2, or correct me if I'm wrong.  This is a no-op.
** From os_unix.c: Change the value of the fullsync flag in the given file descriptor.
** From os_unix.c: ((unixFile*)id)->fullSync = v;
*/
static void os2SetFullSync( OsFile *id, int v ){
  return;
}

/*
** Return the underlying file handle for an OsFile
*/
static int os2FileHandle( OsFile *id ){
  return (int)((os2File*)id)->h;
}

/*
** Return an integer that indices the type of lock currently held
** by this handle.  (Used for testing and analysis only.)
*/
static int os2LockState( OsFile *id ){
  return ((os2File*)id)->locktype;
}

/*
** This vector defines all the methods that can operate on an OsFile
** for os2.
*/
static const IoMethod sqlite3Os2IoMethod = {
  os2Close,
  os2OpenDirectory,
  os2Read,
  os2Write,
  os2Seek,
  os2Truncate,
  os2Sync,
  os2SetFullSync,
  os2FileHandle,
  os2FileSize,
  os2Lock,
  os2Unlock,
  os2LockState,
  os2CheckReservedLock,
};

/*
** Allocate memory for an OsFile.  Initialize the new OsFile
** to the value given in pInit and return a pointer to the new
** OsFile.  If we run out of memory, close the file and return NULL.
*/
int allocateOs2File( os2File *pInit, OsFile **pld ){
  os2File *pNew;
  pNew = sqliteMalloc( sizeof(*pNew) );
  if( pNew==0 ){
    DosClose( pInit->h );
    *pld = 0;
    return SQLITE_NOMEM;
  }else{
    *pNew = *pInit;
    pNew->pMethod = &sqlite3Os2IoMethod;
    pNew->locktype = NO_LOCK;
    *pld = (OsFile*)pNew;
    OpenCounter(+1);
    return SQLITE_OK;
  }
}

#endif /* SQLITE_OMIT_DISKIO */
/***************************************************************************
** Everything above deals with file I/O.  Everything that follows deals
** with other miscellanous aspects of the operating system interface
****************************************************************************/

/*
** Get information to seed the random number generator.  The seed
** is written into the buffer zBuf[256].  The calling function must
** supply a sufficiently large buffer.
*/
int sqlite3Os2RandomSeed( char *zBuf ){
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
  memset( zBuf, 0, 256 );
  DosGetDateTime( (PDATETIME)zBuf );
  return SQLITE_OK;
}

/*
** Sleep for a little while.  Return the amount of time slept.
*/
int sqlite3Os2Sleep( int ms ){
  DosSleep( ms );
  return ms;
}

/*
** Static variables used for thread synchronization
*/
static int inMutex = 0;
#ifdef SQLITE_OS2_THREADS
static ULONG mutexOwner;
#endif

/*
** The following pair of routines implement mutual exclusion for
** multi-threaded processes.  Only a single thread is allowed to
** executed code that is surrounded by EnterMutex() and LeaveMutex().
**
** SQLite uses only a single Mutex.  There is not much critical
** code and what little there is executes quickly and without blocking.
*/
void sqlite3Os2EnterMutex(){
  PTIB ptib;
#ifdef SQLITE_OS2_THREADS
  DosEnterCritSec();
  DosGetInfoBlocks( &ptib, NULL );
  mutexOwner = ptib->tib_ptib2->tib2_ultid;
#endif
  assert( !inMutex );
  inMutex = 1;
}
void sqlite3Os2LeaveMutex(){
  PTIB ptib;
  assert( inMutex );
  inMutex = 0;
#ifdef SQLITE_OS2_THREADS
  DosGetInfoBlocks( &ptib, NULL );
  assert( mutexOwner == ptib->tib_ptib2->tib2_ultid );
  DosExitCritSec();
#endif
}

/*
** Return TRUE if the mutex is currently held.
**
** If the thisThreadOnly parameter is true, return true if and only if the
** calling thread holds the mutex.  If the parameter is false, return
** true if any thread holds the mutex.
*/
int sqlite3Os2InMutex( int thisThreadOnly ){
#ifdef SQLITE_OS2_THREADS
  PTIB ptib;
  DosGetInfoBlocks( &ptib, NULL );
  return inMutex>0 && (thisThreadOnly==0 || mutexOwner==ptib->tib_ptib2->tib2_ultid);
#else
  return inMutex>0;
#endif
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
int sqlite3Os2CurrentTime( double *prNow ){
  double now;
  USHORT second, minute, hour,
         day, month, year;
  DATETIME dt;
  DosGetDateTime( &dt );
  second = (USHORT)dt.seconds;
  minute = (USHORT)dt.minutes + dt.timezone;
  hour = (USHORT)dt.hours;
  day = (USHORT)dt.day;
  month = (USHORT)dt.month;
  year = (USHORT)dt.year;

  /* Calculations from http://www.astro.keele.ac.uk/~rno/Astronomy/hjd.html
     http://www.astro.keele.ac.uk/~rno/Astronomy/hjd-0.1.c */
  /* Calculate the Julian days */
  now = day - 32076 +
    1461*(year + 4800 + (month - 14)/12)/4 +
    367*(month - 2 - (month - 14)/12*12)/12 -
    3*((year + 4900 + (month - 14)/12)/100)/4;

  /* Add the fractional hours, mins and seconds */
  now += (hour + 12.0)/24.0;
  now += minute/1440.0;
  now += second/86400.0;
  *prNow = now;
#ifdef SQLITE_TEST
  if( sqlite3_current_time ){
    *prNow = sqlite3_current_time/86400.0 + 2440587.5;
  }
#endif
  return 0;
}

/*
** Remember the number of thread-specific-data blocks allocated.
** Use this to verify that we are not leaking thread-specific-data.
** Ticket #1601
*/
#ifdef SQLITE_TEST
int sqlite3_tsd_count = 0;
# define TSD_COUNTER_INCR InterlockedIncrement( &sqlite3_tsd_count )
# define TSD_COUNTER_DECR InterlockedDecrement( &sqlite3_tsd_count )
#else
# define TSD_COUNTER_INCR  /* no-op */
# define TSD_COUNTER_DECR  /* no-op */
#endif

/*
** If called with allocateFlag>1, then return a pointer to thread
** specific data for the current thread.  Allocate and zero the
** thread-specific data if it does not already exist necessary.
**
** If called with allocateFlag==0, then check the current thread
** specific data.  Return it if it exists.  If it does not exist,
** then return NULL.
**
** If called with allocateFlag<0, check to see if the thread specific
** data is allocated and is all zero.  If it is then deallocate it.
** Return a pointer to the thread specific data or NULL if it is
** unallocated or gets deallocated.
*/
ThreadData *sqlite3Os2ThreadSpecificData( int allocateFlag ){
  static ThreadData **s_ppTsd = NULL;
  static const ThreadData zeroData = {0, 0, 0};
  ThreadData *pTsd;

  if( !s_ppTsd ){
    sqlite3OsEnterMutex();
    if( !s_ppTsd ){
      PULONG pul;
      APIRET rc = DosAllocThreadLocalMemory(1, &pul);
      if( rc != NO_ERROR ){
        sqlite3OsLeaveMutex();
        return 0;
      }
      s_ppTsd = (ThreadData **)pul;
    }
    sqlite3OsLeaveMutex();
  }
  pTsd = *s_ppTsd;
  if( allocateFlag>0 ){
    if( !pTsd ){
      pTsd = sqlite3OsMalloc( sizeof(zeroData) );
      if( pTsd ){
        *pTsd = zeroData;
        *s_ppTsd = pTsd;
        TSD_COUNTER_INCR;
      }
    }
  }else if( pTsd!=0 && allocateFlag<0
              && memcmp( pTsd, &zeroData, sizeof(ThreadData) )==0 ){
    sqlite3OsFree(pTsd);
    *s_ppTsd = NULL;
    TSD_COUNTER_DECR;
    pTsd = 0;
  }
  return pTsd;
}
#endif /* OS_OS2 */

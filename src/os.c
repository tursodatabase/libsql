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

#if OS_UNIX
# include <fcntl.h>
# include <sys/stat.h>
# include <unistd.h>
# include <time.h>
#endif
#if OS_WIN
# include <winbase.h>
#endif


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
int sqliteOsOpenReadWrite(char *zFilename, OsFile *pResult, int *pReadonly){
#if OS_UNIX
  int fd = open(zFilename, O_RDWR|O_CREAT, 0644);
  if( fd<0 ){
    fd = open(zFilename, O_RDONLY);
    if( fd<0 ){
      return SQLITE_CANTOPEN; 
    }
    *pReadonly = 1;
  }else{
    *pReadonly = 0;
  }
  *pResult = fd;
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
int sqliteOsOpenExclusive(char *zFilename, OsFile *pResult){
#if OS_UNIX
  struct stat buf;
  time_t now;
  int fd;
  if( access(zFilename, 0)==0 ){
    return SQLITE_CANTOPEN;
  }
#ifndef O_NOFOLLOW
# define O_NOFOLLOW 0
#endif
  fd = open(zFilename, O_RDWR|O_CREAT|O_EXCL|O_NOFOLLOW, 0600);
  if( fd<0 ){
    return SQLITE_CANTOPEN;
  }
  *pResult = fd;
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
  char *zDir = ".";
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
      int n = sqliteRandomByte() % sizeof(zChars);
      zBuf[j++] = zChars[n];
    }
    zBuf[j] = 0;
  }while( access(zBuf,0)==0 );
#endif
#if OS_WIN
  int i, j;
  HANDLE h;
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
    h = CreateFile(zBuf,
      GENERIC_READ,
      0,
      NULL,
      OPEN_EXISTING,
      FILE_ATTRIBUTE_NORMAL | FILE_FLAG_RANDOM_ACCESS,
      NULL
    );
    if( h!=INVALID_FILE_HANDLE ){
      CloseHandle(h);
      continue;
    }
    break;
  }
#endif
  return SQLITE_OK; 
}

/*
** Close a file
*/
int sqliteOsClose(OsFile id){
#if OS_UNIX
  close(id);
  return SQLITE_OK;
#endif
#if OS_WIN
  CloseHandle(id);
  return SQLITE_OK;
#endif
}

/*
** Read data from a file into a buffer
*/

/*
** Write data from a buffer into a file
*/
/*
** Move the read/write pointer in a file.
*/
/*
** Make sure all writes to a particular file are committed to disk.
*/
/*
** Truncate an open file to a specified size
*/
/*
** Determine the current size of a file in bytes
*/
/*
** Get a read or write lock on a file.
*/
/*
** Release the read or write lock from a file.
*/
/*
** Get information to seed the random number generator.
*/
/*
** Sleep for a little while.
*/



#endif /* OS_WIN */

/*
** 2016 September 10
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
*************************************************************************
** This file contains test code to delete an SQLite database and all
** of its associated files. Associated files include:
**
**   * The journal file.
**   * The wal file.
**   * The SQLITE_ENABLE_8_3_NAMES version of the db, journal or wal files.
**   * Files created by the test_multiplex.c module to extend any of the 
**     above.
*/

#if SQLITE_OS_WIN
#  include <io.h>
#  define F_OK 0
#else
#  include <unistd.h>
#endif
#include <string.h>
#include <errno.h>
#include "sqlite3.h"

/* The following #defines are copied from test_multiplex.c */
#ifndef MX_CHUNK_NUMBER 
# define MX_CHUNK_NUMBER 299
#endif
#ifndef SQLITE_MULTIPLEX_JOURNAL_8_3_OFFSET
# define SQLITE_MULTIPLEX_JOURNAL_8_3_OFFSET 400
#endif
#ifndef SQLITE_MULTIPLEX_WAL_8_3_OFFSET
# define SQLITE_MULTIPLEX_WAL_8_3_OFFSET 700
#endif

/*
** This routine is a copy of (most of) the code from SQLite function
** sqlite3FileSuffix3(). It modifies the filename in buffer z in the
** same way as SQLite does when in 8.3 filenames mode.
*/
static void sqlite3Delete83Name(char *z){
  int i, sz;
  sz = (int)strlen(z);
  for(i=sz-1; i>0 && z[i]!='/' && z[i]!='.'; i--){}
  if( z[i]=='.' && (sz>i+4) ) memmove(&z[i+1], &z[sz-3], 4);
}

/*
** zFile is a filename. Assuming no error occurs, if this file exists, 
** set *pbExists to true and unlink it. Or, if the file does not exist,
** set *pbExists to false before returning.
**
** If an error occurs, the value of errno is returned. Or, if no error
** occurs, zero is returned.
*/
static int sqlite3DeleteUnlinkIfExists(const char *zFile, int *pbExists){
  int rc;
  rc = access(zFile, F_OK);
  if( rc ){
    if( errno==ENOENT ){ 
      if( pbExists ) *pbExists = 0;
      return 0; 
    }
    return errno;
  }
  if( pbExists ) *pbExists = 1;
  rc = unlink(zFile);
  if( rc ) return errno;
  return 0;
}

/*
** Delete the database file identified by the string argument passed to this
** function. The string must contain a filename, not an SQLite URI.
*/
int sqlite3_delete_database(
  const char *zFile               /* File to delete */
){
  char *zBuf;                     /* Buffer to sprintf() filenames to */
  int nBuf;                       /* Size of buffer in bytes */
  int rc = 0;                     /* System error code */
  int i;                          /* Iterate through azFmt[] and aMFile[] */

  const char *azFmt[] = { "%s", "%s-journal", "%s-wal", "%s-shm" };

  struct MFile {
    const char *zFmt;
    int iOffset;
    int b83;
  } aMFile[] = {
    { "%s%03d",         0,   0 },
    { "%s-journal%03d", 0,   0 },
    { "%s-wal%03d",     0,   0 },
    { "%s%03d",         0,   1 },
    { "%s-journal%03d", SQLITE_MULTIPLEX_JOURNAL_8_3_OFFSET, 1 },
    { "%s-wal%03d",     SQLITE_MULTIPLEX_WAL_8_3_OFFSET, 1 },
  };

  /* Allocate a buffer large enough for any of the files that need to be
  ** deleted.  */
  nBuf = (int)strlen(zFile) + 100;
  zBuf = (char*)sqlite3_malloc(nBuf);
  if( zBuf==0 ) return SQLITE_NOMEM;

  /* Delete both the regular and 8.3 filenames versions of the database,
  ** journal, wal and shm files.  */
  for(i=0; rc==0 && i<sizeof(azFmt)/sizeof(azFmt[0]); i++){
    sqlite3_snprintf(nBuf, zBuf, azFmt[i], zFile);
    rc = sqlite3DeleteUnlinkIfExists(zBuf, 0);
    if( rc==0 && i!=0 ){
      sqlite3Delete83Name(zBuf);
      rc = sqlite3DeleteUnlinkIfExists(zBuf, 0);
    }
  }

  /* Delete any multiplexor files */
  for(i=0; rc==0 && i<sizeof(aMFile)/sizeof(aMFile[0]); i++){
    struct MFile *p = &aMFile[i];
    int iChunk;
    for(iChunk=1; iChunk<=MX_CHUNK_NUMBER; iChunk++){
      int bExists;
      sqlite3_snprintf(nBuf, zBuf, p->zFmt, zFile, iChunk+p->iOffset);
      if( p->b83 ) sqlite3Delete83Name(zBuf);
      rc = sqlite3DeleteUnlinkIfExists(zBuf, &bExists);
      if( bExists==0 || rc!=0 ) break;
    }
  }

  sqlite3_free(zBuf);
  return (rc ? SQLITE_ERROR : SQLITE_OK);
}

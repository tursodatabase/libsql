/*
** 2014-06-13
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
** This SQLite extension implements SQL functions readfile() and
** writefile().
*/
#include "sqlite3ext.h"
SQLITE_EXTENSION_INIT1
#include <stdio.h>

/*
** Implementation of the "readfile(X)" SQL function.  The entire content
** of the file named X is read and returned as a BLOB.  NULL is returned
** if the file does not exist or is unreadable.
*/
static void readfileFunc(
  sqlite3_context *context,
  int argc,
  sqlite3_value **argv
){
  const char *zName;
  FILE *in;
  long nIn;
  void *pBuf;

  zName = (const char*)sqlite3_value_text(argv[0]);
  if( zName==0 ) return;
  in = fopen(zName, "rb");
  if( in==0 ) return;
  fseek(in, 0, SEEK_END);
  nIn = ftell(in);
  rewind(in);
  pBuf = sqlite3_malloc( nIn );
  if( pBuf && 1==fread(pBuf, nIn, 1, in) ){
    sqlite3_result_blob(context, pBuf, nIn, sqlite3_free);
  }else{
    sqlite3_free(pBuf);
  }
  fclose(in);
}

/*
** Implementation of the "writefile(X,Y)" SQL function.  The argument Y
** is written into file X.  The number of bytes written is returned.  Or
** NULL is returned if something goes wrong, such as being unable to open
** file X for writing.
*/
static void writefileFunc(
  sqlite3_context *context,
  int argc,
  sqlite3_value **argv
){
  FILE *out;
  const char *z;
  sqlite3_int64 rc;
  const char *zFile;

  zFile = (const char*)sqlite3_value_text(argv[0]);
  if( zFile==0 ) return;
  out = fopen(zFile, "wb");
  if( out==0 ) return;
  z = (const char*)sqlite3_value_blob(argv[1]);
  if( z==0 ){
    rc = 0;
  }else{
    rc = fwrite(z, 1, sqlite3_value_bytes(argv[1]), out);
  }
  fclose(out);
  sqlite3_result_int64(context, rc);
}


#ifdef _WIN32
__declspec(dllexport)
#endif
int sqlite3_fileio_init(
  sqlite3 *db, 
  char **pzErrMsg, 
  const sqlite3_api_routines *pApi
){
  int rc = SQLITE_OK;
  SQLITE_EXTENSION_INIT2(pApi);
  (void)pzErrMsg;  /* Unused parameter */
  rc = sqlite3_create_function(db, "readfile", 1, SQLITE_UTF8, 0,
                               readfileFunc, 0, 0);
  if( rc==SQLITE_OK ){
    rc = sqlite3_create_function(db, "writefile", 2, SQLITE_UTF8, 0,
                                 writefileFunc, 0, 0);
  }
  return rc;
}

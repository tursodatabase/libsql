/*
** 2018-02-09
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
** SQL functions for z-order (Morton code) transformations.
**
**      zorder(X0,X0,..,xN)      Generate an N+1 dimension Morton code
**
**      unzorder(Z,N,I)          Extract the I-th dimension from N-dimensional
**                               Morton code Z.
**
** Compiling:
**
**   (linux)    gcc -fPIC -shared zorder.c -o zorder.so
**   (mac)      clang -fPIC -dynamiclib zorder.c -o zorder.dylib
**   (windows)  cl zorder.c -link -dll -out:zorder.dll
**
** Usage example:
**
**     .load ./zorder
**     SELECT zorder(1,2,3,4);
*/
#include "sqlite3ext.h"
SQLITE_EXTENSION_INIT1
#include <assert.h>
#include <string.h>

/*
** Functions:     zorder(X0,X1,....)
**
** Convert integers X0, X1, ... into morton code.  There must be at least
** two arguments.  There may be no more than 24 arguments.
**
** The output is a signed 64-bit integer.  If any argument is too large
** to be successfully encoded into a morton code, an error is raised.
*/
static void zorderFunc(
  sqlite3_context *context,
  int argc,
  sqlite3_value **argv
){
  sqlite3_int64 z, x[24];
  int i, j;
  z = 0;
  if( argc<2 || argc>24 ){
    sqlite3_result_error(context,
       "zorder() needs between 2 and 24 arguments4", -1);
    return;
  }
  for(i=0; i<argc; i++){
    x[i] = sqlite3_value_int64(argv[i]);
  }
  for(i=0; i<63; i++){
    j = i%argc;
    z |= (x[j]&1)<<i;
    x[j] >>= 1;
  }
  sqlite3_result_int64(context, z);
  for(i=0; i<argc; i++){
    if( x[i] ){
      char *z = sqlite3_mprintf(
        "the %r argument to zorder() (%lld) is too large "
        "for a 64-bit %d-dimensional Morton code",
        i+1, sqlite3_value_int64(argv[i]), argc);
      sqlite3_result_error(context, z, -1);
      sqlite3_free(z);
      break;
    }
  }
}


/*
** Function:     unzorder(Z,N,K)
**
** Assuming that Z is an N-dimensional Morton code, extract the K-th
** dimension.  K is between 0 and N-1.  N must be between 2 and 24.
*/
static void unzorderFunc(
  sqlite3_context *context,
  int argc,
  sqlite3_value **argv
){
  sqlite3_int64 z, n, i, x;
  int j, k;
  z = sqlite3_value_int64(argv[0]);
  n = sqlite3_value_int64(argv[1]);
  if( n<2 || n>24 ){
    sqlite3_result_error(context,
      "N argument to unzorder(Z,N,K) should be between 2 and 24",
      -1);
    return;
  }
  i = sqlite3_value_int64(argv[2]);
  if( i<0 || i>=n ){
    sqlite3_result_error(context,
      "K argument to unzorder(Z,N,K) should be between 0 and N-1", -1);
    return;
  }
  x = 0;
  for(k=0, j=i; j<63; j+=n, k++){
    x |= ((z>>j)&1)<<k;
  }
  sqlite3_result_int64(context, x);
}


#ifdef _WIN32
__declspec(dllexport)
#endif
int sqlite3_zorder_init(
  sqlite3 *db, 
  char **pzErrMsg, 
  const sqlite3_api_routines *pApi
){
  int rc = SQLITE_OK;
  SQLITE_EXTENSION_INIT2(pApi);
  (void)pzErrMsg;  /* Unused parameter */
  rc = sqlite3_create_function(db, "zorder", -1, SQLITE_UTF8, 0,
                               zorderFunc, 0, 0);
  if( rc==SQLITE_OK ){
    rc = sqlite3_create_function(db, "unzorder", 3, SQLITE_UTF8, 0,
                               unzorderFunc, 0, 0);
  }
  return rc;
}

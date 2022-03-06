/*
** 2022 Feb 28
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
*************************************************************************
** Test extension for testing the shell's .load -shellext ... function.
** gcc -shared -fPIC -Wall -I$srcdir -I.. -g test_shellext.c -o test_shellext.so
*/
#include <stdio.h>
#include "shext_linkage.h"

SQLITE_EXTENSION_INIT1;

DEFINE_SHDB_TO_SHEXT_API(shext_api);

/*
** Extension load function.
*/
#ifdef _WIN32
__declspec(dllexport)
#endif
int sqlite3_testshellext_init(
  sqlite3 *db,
  char **pzErrMsg,
  const sqlite3_api_routines *pApi
){
  int nErr = 0;
  ShellExtensionLink *papi;
  SQLITE_EXTENSION_INIT2(pApi);
  papi = shext_api(db);
  if( papi ){
    printf("Got papi, equality=%d\n", &papi->zErrMsg==pzErrMsg);
  }
  else
    printf("No papi pointer.\n");
  return nErr ? SQLITE_ERROR : SQLITE_OK;
}

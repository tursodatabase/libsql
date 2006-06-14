/*
** 2006 June 14
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
*************************************************************************
** Test extension for testing the sqlite3_load_extension() function.
**
** $Id: test_loadext.c,v 1.1 2006/06/14 10:38:03 danielk1977 Exp $
*/

#include "sqlite3ext.h"
SQLITE_EXTENSION_INIT1

/*
** The half() SQL function returns half of its input value.
*/
static void halfFunc(
  sqlite3_context *context,
  int argc,
  sqlite3_value **argv
){
  sqlite3_result_double(context, 0.5*sqlite3_value_double(argv[0]));
}

/*
** Extension load function.
*/
int testloadext_init(
  sqlite3 *db, 
  char **pzErrMsg, 
  const sqlite3_api_routines *pApi
){
  SQLITE_EXTENSION_INIT2(pApi);
  sqlite3_create_function(db, "half", 1, SQLITE_ANY, 0, halfFunc, 0, 0);
  return 0;
}

/*
** Another extension entry point. This one always fails.
*/
int testbrokenext_init(
  sqlite3 *db, 
  char **pzErrMsg, 
  const sqlite3_api_routines *pApi
){
  char *zErr;
  SQLITE_EXTENSION_INIT2(pApi);
  zErr = sqlite3_mprintf("broken!");
  *pzErrMsg = zErr;
  return 1;
}

/*
** 2025-10-14
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
** Test the ability of run-time extension loading to use the
** very latest interfaces.
**
** Compile something like this:
**
** Linux:  gcc -g -fPIC shared testloadext.c -o testloadext.so
**
** Mac:    cc -g -fPIC -dynamiclib testloadext.c -o testloadext.dylib
**
** Win11:  cl testloadext.c -link -dll -out:testloadext.dll
*/
#include "sqlite3ext.h"
SQLITE_EXTENSION_INIT1
#include <assert.h>
#include <string.h>

/*
** Implementation of the set_errmsg(CODE,MSG) SQL function.
**
** Raise an error that has numeric code CODE and text message MSG
** using the sqlite3_set_errmsg() API.
*/
static void seterrmsgfunc(
  sqlite3_context *context,
  int argc,
  sqlite3_value **argv
){
  sqlite3 *db;
  char *zRes;
  int rc;
  assert( argc==2 );
  db = sqlite3_context_db_handle(context);
  rc = sqlite3_set_errmsg(db, 
     sqlite3_value_int(argv[0]),
     sqlite3_value_text(argv[1]));
  zRes = sqlite3_mprintf("%d %d %s",
              rc, sqlite3_errcode(db), sqlite3_errmsg(db));
  sqlite3_result_text64(context, zRes, strlen(zRes),
                        SQLITE_TRANSIENT, SQLITE_UTF8);
  sqlite3_free(zRes);
}

/*
** Implementation of the tempbuf_spill() SQL function.
**
** Return the value of SQLITE_DBSTATUS_TEMPBUF_SPILL.
*/
static void tempbuf_spill_func(
  sqlite3_context *context,
  int argc,
  sqlite3_value **argv
){
  sqlite3 *db;
  sqlite3_int64 iHi = 0, iCur = 0;
  int rc;
  int bReset;
  assert( argc==1 );
  bReset = sqlite3_value_int(argv[0]);
  db = sqlite3_context_db_handle(context);
  (void)sqlite3_db_status64(db, SQLITE_DBSTATUS_TEMPBUF_SPILL,
                            &iCur, &iHi, bReset);
  sqlite3_result_int64(context, iCur);
}


#ifdef _WIN32
__declspec(dllexport)
#endif
int sqlite3_testloadext_init(
  sqlite3 *db, 
  char **pzErrMsg, 
  const sqlite3_api_routines *pApi
){
  int rc = SQLITE_OK;
  SQLITE_EXTENSION_INIT2(pApi);
  (void)pzErrMsg;  /* Unused parameter */
  rc = sqlite3_create_function(db, "set_errmsg", 2,
                   SQLITE_UTF8,
                   0, seterrmsgfunc, 0, 0);
  if( rc ) return rc;
  rc = sqlite3_create_function(db, "tempbuf_spill", 1,
                   SQLITE_UTF8,
                   0, tempbuf_spill_func, 0, 0);
  if( rc ) return rc;
  return SQLITE_OK;
}

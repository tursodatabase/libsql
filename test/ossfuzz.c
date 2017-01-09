/*
** This module interfaces SQLite to the Google OSS-Fuzz, fuzzer as a service.
** (https://github.com/google/oss-fuzz)
*/
#include <stddef.h>
#include <stdint.h>
#include "sqlite3.h"

#ifndef SQLITE_OMIT_PROGRESS_CALLBACK
/*
** Progress handler callback
*/
static int progress_handler(void *pReturn) {
  return *(int*)pReturn;
}
#endif

/*
** Callback for sqlite3_exec().
*/
static int exec_handler(void *pCnt, int argc, char **argv, char **namev){
  int i;
  if( argv ){
    for(i=0; i<argc; i++) sqlite3_free(sqlite3_mprintf("%s", argv[i]));
  }
  return ((*(int*)pCnt)--)<=0;
}

/*
** Main entry point.  The fuzzer invokes this function with each
** fuzzed input.
*/
int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
  int progressArg = 0;     /* 1 causes progress handler abort */
  int execCnt = 0;         /* Abort row callback when count reaches zero */
  char *zErrMsg = 0;       /* Error message returned by sqlite_exec() */
  sqlite3 *db;             /* The database connection */
  uint8_t uSelector;       /* First byte of input data[] */
  int rc;                  /* Return code from various interfaces */
  char *zSql;              /* Zero-terminated copy of data[] */

  if( size<3 ) return 0;   /* Early out if unsufficient data */

  /* Extract the selector byte from the beginning of the input.  But only
  ** do this if the second byte is a \n.  If the second byte is not \n,
  ** then use a default selector */
  if( data[1]=='\n' ){
    uSelector = data[0];  data += 2; size -= 2;
  }else{
    uSelector = 0xfd;
  }

  /* Open the database connection.  Only use an in-memory database. */
  rc = sqlite3_open_v2("fuzz.db", &db,
           SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_MEMORY, 0);
  if( rc ) return 0;

#ifndef SQLITE_OMIT_PROGRESS_CALLBACK
  /* Bit 0 of the selector enables progress callbacks.  Bit 1 is the
  ** return code from progress callbacks */
  if( uSelector & 1 ){
    sqlite3_progress_handler(db, 4, progress_handler, (void*)&progressArg);
  }
#endif
  uSelector >>= 1;
  progressArg = uSelector & 1;  uSelector >>= 1;

  /* Bit 2 of the selector enables foreign key constraints */
  sqlite3_db_config(db, SQLITE_DBCONFIG_ENABLE_FKEY, uSelector&1, &rc);
  uSelector >>= 1;

  /* Remaining bits of the selector determine a limit on the number of
  ** output rows */
  execCnt = uSelector + 1;

  /* Run the SQL.  The sqlite_exec() interface expects a zero-terminated
  ** string, so make a copy. */
  zSql = sqlite3_mprintf("%.*s", (int)size, data);
  sqlite3_exec(db, zSql, exec_handler, (void*)&execCnt, &zErrMsg);

  /* Cleanup and return */
  sqlite3_free(zErrMsg);
  sqlite3_free(zSql);
  sqlite3_close(db);
  return 0;
}

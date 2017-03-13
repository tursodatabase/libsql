/*
** This module interfaces SQLite to the Google OSS-Fuzz, fuzzer as a service.
** (https://github.com/google/oss-fuzz)
*/
#include <stddef.h>
#include <stdint.h>
#include "sqlite3.h"

/* Return the current real-world time in milliseconds since the
** Julian epoch (-4714-11-24).
*/
static sqlite3_int64 timeOfDay(void){
  static sqlite3_vfs *clockVfs = 0;
  sqlite3_int64 t;
  if( clockVfs==0 ) clockVfs = sqlite3_vfs_find(0);
  if( clockVfs->iVersion>=2 && clockVfs->xCurrentTimeInt64!=0 ){
    clockVfs->xCurrentTimeInt64(clockVfs, &t);
  }else{
    double r;
    clockVfs->xCurrentTime(clockVfs, &r);
    t = (sqlite3_int64)(r*86400000.0);
  }
  return t;
}

#ifndef SQLITE_OMIT_PROGRESS_CALLBACK
/*
** Progress handler callback.
**
** The argument is the cutoff-time after which all processing should
** stop.  So return non-zero if the cut-off time is exceeded.
*/
static int progress_handler(void *pReturn) {
  sqlite3_int64 iCutoffTime = *(sqlite3_int64*)pReturn;
  return timeOfDay()>=iCutoffTime;
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
  int execCnt = 0;         /* Abort row callback when count reaches zero */
  char *zErrMsg = 0;       /* Error message returned by sqlite_exec() */
  sqlite3 *db;             /* The database connection */
  uint8_t uSelector;       /* First byte of input data[] */
  int rc;                  /* Return code from various interfaces */
  char *zSql;              /* Zero-terminated copy of data[] */
  sqlite3_int64 iCutoff;   /* Cutoff timer */

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
  /* Invoke the progress handler frequently to check to see if we
  ** are taking too long.  The progress handler will return true
  ** (which will block further processing) if more than 10 seconds have
  ** elapsed since the start of the test.
  */
  iCutoff = timeOfDay() + 10000;  /* Now + 10 seconds */
  sqlite3_progress_handler(db, 10, progress_handler, (void*)&iCutoff);
#endif

  /* Bit 1 of the selector enables foreign key constraints */
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

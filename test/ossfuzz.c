/*
** This module interfaces SQLite to the Google OSS-Fuzz, fuzzer as a service.
** (https://github.com/google/oss-fuzz)
*/
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include "sqlite3.h"

/* Global debugging settings.  OSS-Fuzz will have all debugging turned
** off.  But if LLVMFuzzerTestOneInput() is called interactively from
** the ossshell utility program, then these flags might be set.
*/
static unsigned mDebug = 0;
#define FUZZ_SQL_TRACE       0x0001   /* Set an sqlite3_trace() callback */
#define FUZZ_SHOW_MAX_DELAY  0x0002   /* Show maximum progress callback delay */
#define FUZZ_SHOW_ERRORS     0x0004   /* Print error messages from SQLite */

/* The ossshell utility program invokes this interface to see the
** debugging flags.  Unused by OSS-Fuzz.
*/
void ossfuzz_set_debug_flags(unsigned x){
  mDebug = x;
}

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

/* An instance of the following object is passed by pointer as the
** client data to various callbacks.
*/
typedef struct FuzzCtx {
  sqlite3 *db;               /* The database connection */
  sqlite3_int64 iCutoffTime; /* Stop processing at this time. */
  sqlite3_int64 iLastCb;     /* Time recorded for previous progress callback */
  sqlite3_int64 mxInterval;  /* Longest interval between two progress calls */
  unsigned nCb;              /* Number of progress callbacks */
} FuzzCtx;

#ifndef SQLITE_OMIT_PROGRESS_CALLBACK
/*
** Progress handler callback.
**
** The argument is the cutoff-time after which all processing should
** stop.  So return non-zero if the cut-off time is exceeded.
*/
static int progress_handler(void *pClientData) {
  FuzzCtx *p = (FuzzCtx*)pClientData;
  sqlite3_int64 iNow = timeOfDay();
  int rc = iNow>=p->iCutoffTime;
  sqlite3_int64 iDiff = iNow - p->iLastCb;
  if( iDiff > p->mxInterval ) p->mxInterval = iDiff;
  p->nCb++;
  return rc;
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
  uint8_t uSelector;       /* First byte of input data[] */
  int rc;                  /* Return code from various interfaces */
  char *zSql;              /* Zero-terminated copy of data[] */
  FuzzCtx cx;              /* Fuzzing context */

  memset(&cx, 0, sizeof(cx));
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
  rc = sqlite3_open_v2("fuzz.db", &cx.db,
           SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_MEMORY, 0);
  if( rc ) return 0;

#ifndef SQLITE_OMIT_PROGRESS_CALLBACK
  /* Invoke the progress handler frequently to check to see if we
  ** are taking too long.  The progress handler will return true
  ** (which will block further processing) if more than 10 seconds have
  ** elapsed since the start of the test.
  */
  cx.iLastCb = timeOfDay();
  cx.iCutoffTime = cx.iLastCb + 10000;  /* Now + 10 seconds */
  sqlite3_progress_handler(cx.db, 10, progress_handler, (void*)&cx);
#endif

  /* Set a limit on the maximum size of a prepared statement */
  sqlite3_limit(cx.db, SQLITE_LIMIT_VDBE_OP, 25000);

  /* Bit 1 of the selector enables foreign key constraints */
  sqlite3_db_config(cx.db, SQLITE_DBCONFIG_ENABLE_FKEY, uSelector&1, &rc);
  uSelector >>= 1;

  /* Remaining bits of the selector determine a limit on the number of
  ** output rows */
  execCnt = uSelector + 1;

  /* Run the SQL.  The sqlite_exec() interface expects a zero-terminated
  ** string, so make a copy. */
  zSql = sqlite3_mprintf("%.*s", (int)size, data);
  sqlite3_exec(cx.db, zSql, exec_handler, (void*)&execCnt, &zErrMsg);

  /* Show any errors */
  if( (mDebug & FUZZ_SHOW_ERRORS)!=0 && zErrMsg ){
    printf("Error: %s\n", zErrMsg);
  }

  /* Cleanup and return */
  sqlite3_free(zErrMsg);
  sqlite3_free(zSql);
  sqlite3_exec(cx.db, "PRAGMA temp_store_directory=''", 0, 0, 0);
  sqlite3_close(cx.db);

  if( mDebug & FUZZ_SHOW_MAX_DELAY ){
    printf("Progress callback count....... %d\n", cx.nCb);
    printf("Max time between callbacks.... %d ms\n", (int)cx.mxInterval);
  }
  return 0;
}

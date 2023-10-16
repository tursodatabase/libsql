/*
** 2021-05-12
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
*************************************************************************
**
** Testing threading behavior when multiple database connections in separate
** threads of the same process are all talking to the same database file.
**
** For best results, ensure that SQLite is compiled with HAVE_USLEEP=1
**
** Only works on unix platforms.
**
** Usage:
**
**      ./threadtest5  ?DATABASE?
**
** If DATABASE is omitted, it defaults to using file:/mem?vfs=memdb.
*/
#include "sqlite3.h"
#include <pthread.h>
#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <stdarg.h>

/* Name of the in-memory database */
static char *zDbName = 0;

/* True for debugging */
static int eVerbose = 0;

/* If rc is not SQLITE_OK, then print an error message and stop
** the test.
*/
static void error_out(int rc, const char *zCtx, int lineno){
  if( rc!=SQLITE_OK ){
    fprintf(stderr, "error %d at %d in \"%s\"\n", rc, lineno, zCtx);
    exit(-1);
  }
}

#if 0
/* Return the number of milliseconds since the Julian epoch (-4714-11-24).
*/
static sqlite3_int64 gettime(void){
  sqlite3_int64 tm;
  sqlite3_vfs *pVfs = sqlite3_vfs_find(0);
  pVfs->xCurrentTimeInt64(pVfs, &tm);
  return tm;
}
#endif

/* Run the SQL in the second argument.
*/
static int exec(
  sqlite3 *db,
  const char *zId,
  int lineno,
  const char *zFormat,
  ...
){
  int rc;
  va_list ap;
  char *zSql;
  va_start(ap, zFormat);
  zSql = sqlite3_vmprintf(zFormat, ap);
  va_end(ap);
  if( eVerbose){
    printf("%s:%d: [%s]\n", zId, lineno, zSql);
    fflush(stdout);
  }
  rc = sqlite3_exec(db, zSql, 0, 0, 0);
  if( rc && eVerbose ){
    printf("%s:%d: return-code %d\n", zId, lineno, rc);
    fflush(stdout);
  }
  sqlite3_free(zSql);
  return rc;
}

/* Generate a perpared statement from the input SQL
*/
static sqlite3_stmt *prepare(
  sqlite3 *db,
  const char *zId,
  int lineno,
  const char *zFormat,
  ...
){
  int rc;
  va_list ap;
  char *zSql;
  sqlite3_stmt *pStmt = 0;
  va_start(ap, zFormat);
  zSql = sqlite3_vmprintf(zFormat, ap);
  va_end(ap);
  if( eVerbose){
    printf("%s:%d: [%s]\n", zId, lineno, zSql);
    fflush(stdout);
  }

  rc = sqlite3_prepare_v2(db, zSql, -1, &pStmt, 0);
  if( rc ){
    printf("%s:%d: ERROR - %s\n", zId, lineno, sqlite3_errmsg(db));
    exit(-1);
  }
  sqlite3_free(zSql);
  return pStmt;
}

/*
** Wait for table zTable to exist in the schema.
*/
static void waitOnTable(sqlite3 *db, const char *zWorker, const char *zTable){
  while(1){
    int eFound = 0;
    sqlite3_stmt *q = prepare(db, zWorker, __LINE__,
             "SELECT 1 FROM sqlite_schema WHERE name=%Q", zTable);
    if( sqlite3_step(q)==SQLITE_ROW && sqlite3_column_int(q,0)!=0 ){
      eFound = 1;
    }
    sqlite3_finalize(q);
    if( eFound ) return;
    sqlite3_sleep(1);
  }
}

/*
** Return true if x is  a prime number
*/
static int isPrime(int x){
  int i;
  if( x<2 ) return 1;
  for(i=2; i*i<=x; i++){
    if( (x%i)==0 ) return 0;
  }
  return 1;
}

/* Each worker thread runs an instance of the following */
static void *worker(void *pArg){
  int rc;
  const char *zName = (const char*)pArg;
  sqlite3 *db = 0;

  if( eVerbose ){
    printf("%s: startup\n", zName);
    fflush(stdout);
  }

  rc = sqlite3_open(zDbName, &db);
  error_out(rc, "sqlite3_open", __LINE__);
  sqlite3_busy_timeout(db, 2000);

  while( 1 ){
    sqlite3_stmt *q1;
    int tid = -1;
    q1 = prepare(db, zName, __LINE__,
            "UPDATE task SET doneby=%Q"
            " WHERE tid=(SELECT tid FROM task WHERE doneby IS NULL LIMIT 1)"
            "RETURNING tid", zName
    );
    if( sqlite3_step(q1)==SQLITE_ROW ){
      tid = sqlite3_column_int(q1,0);
    }
    sqlite3_finalize(q1);
    if( tid<0 ) break;
    if( eVerbose ){
      printf("%s: starting task %d\n", zName, tid);
      fflush(stdout);
    }
    if( tid==1 ){
      exec(db, zName, __LINE__,
         "CREATE TABLE IF NOT EXISTS p1(x INTEGER PRIMARY KEY);"
      );
    }else if( tid>=2 && tid<=51 ){
      int a, b, i;
      waitOnTable(db, zName, "p1");
      a = (tid-2)*200 + 1;
      b = a+200;
      for(i=a; i<b; i++){
        if( isPrime(i) ){
          exec(db, zName, __LINE__,
              "INSERT INTO p1(x) VALUES(%d)", i);
        }
      }
    }else if( tid==52 ){
      exec(db, zName, __LINE__,
         "CREATE TABLE IF NOT EXISTS p2(x INTEGER PRIMARY KEY);"
         "WITH RECURSIVE"
         "  c(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM c WHERE x<10000)"
         "INSERT INTO p2(x) SELECT x FROM c;"
      );
    }else if( tid>=53 && tid<=62 ){
      int a, b, i;
      waitOnTable(db, zName, "p2");
      a = (tid-53)*10 + 2;
      b = a+9;
      for(i=a; i<=b; i++){
        exec(db, zName, __LINE__,
          "DELETE FROM p2 WHERE x>%d AND (x %% %d)==0", i, i);
      }
    }
    if( eVerbose ){
      printf("%s: completed task %d\n", zName, tid);
      fflush(stdout);
    }
    sqlite3_sleep(1);
  }

  sqlite3_close(db);

  if( eVerbose ){
    printf("%s: exit\n", zName);
    fflush(stdout);
  }
  return 0;
}

/* Print a usage comment and die */
static void usage(const char *argv0){
  printf("Usage: %s [options]\n", argv0);
  printf(
    "  -num-workers N      Run N worker threads\n"
    "  -v                  Debugging output\n"
  );
  exit(1);
}

/* Maximum number of threads */
#define MX_WORKER 100

/*
** Main routine
*/
int main(int argc, char **argv){
  int i;
  int nWorker = 4;
  int rc;
  sqlite3 *db = 0;
  sqlite3_stmt *q;
  pthread_t aWorker[MX_WORKER];
  char aWorkerName[MX_WORKER][8];

  for(i=1; i<argc; i++){
    const char *zArg = argv[i];
    if( zArg[0]!='-' ){
      if( zDbName==0 ){
        zDbName = argv[i];
        continue;
      }
      printf("unknown argument: %s\n", zArg);
      usage(argv[0]);
    }
    if( zArg[1]=='-' ) zArg++;
    if( strcmp(zArg, "-v")==0 ){
      eVerbose = 1;
      continue;
    }
    if( strcmp(zArg, "-num-workers")==0 && i+1<argc ){
      nWorker = atoi(argv[++i]);
      if( nWorker<1 || nWorker>MX_WORKER ){
        printf("number of threads must be between 1 and %d\n", MX_WORKER);
        exit(1);
      }
      continue;
    }
    printf("unknown option: %s\n", argv[i]);
    usage(argv[0]);
  }
  if( zDbName==0 ) zDbName = "file:/mem?vfs=memdb";

  sqlite3_config(SQLITE_CONFIG_URI, (int)1);
  rc = sqlite3_open(zDbName, &db);
  error_out(rc, "sqlite3_open", __LINE__);

  rc = exec(db, "SETUP", __LINE__,
    "DROP TABLE IF EXISTS task;\n"
    "DROP TABLE IF EXISTS p1;\n"
    "DROP TABLE IF EXISTS p2;\n"
    "DROP TABLE IF EXISTS verify;\n"
    "CREATE TABLE IF NOT EXISTS task(\n"
    "  tid INTEGER PRIMARY KEY,\n"
    "  doneby TEXT\n"
    ");\n"
    "WITH RECURSIVE c(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM c WHERE x<100)"
    "INSERT INTO task(tid) SELECT x FROM c;\n"
  );
  error_out(rc, "sqlite3_exec", __LINE__);

  for(i=0; i<nWorker; i++){
    sqlite3_snprintf(sizeof(aWorkerName[i]), aWorkerName[i],
             "W%02d", i);
    pthread_create(&aWorker[i], 0, worker, aWorkerName[i]);
  }
  for(i=0; i<nWorker; i++){
    pthread_join(aWorker[i], 0);
  }

  for(i=0; i<nWorker; i++){
    q = prepare(db, "MAIN", __LINE__,
          "SELECT group_concat(tid,',') FROM task WHERE doneby=%Q",
          aWorkerName[i]);
    if( sqlite3_step(q)==SQLITE_ROW ){
      printf("%s: %s\n", aWorkerName[i], sqlite3_column_text(q,0));
    }
    sqlite3_finalize(q);
  }
  q = prepare(db, "MAIN", __LINE__, "SELECT count(*) FROM p2");
  if( sqlite3_step(q)!=SQLITE_ROW || sqlite3_column_int(q,0)<10 ){
    printf("incorrect result\n");
    exit(-1);
  }
  sqlite3_finalize(q);
  q = prepare(db, "MAIN", __LINE__, "SELECT x FROM p1 EXCEPT SELECT x FROM p2");
  if( sqlite3_step(q)==SQLITE_ROW ){
    printf("incorrect result\n");
    exit(-1);
  }
  sqlite3_finalize(q);
  q = prepare(db, "MAIN", __LINE__, "SELECT x FROM p2 EXCEPT SELECT x FROM p1");
  if( sqlite3_step(q)==SQLITE_ROW ){
    printf("incorrect result\n");
    exit(-1);
  }
  sqlite3_finalize(q);
  printf("OK\n");

  sqlite3_close(db);
  return 0;
}

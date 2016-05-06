

#include <sqlite3.h>

#include <stdlib.h>

#include "tt3_core.c"


typedef struct Config Config;

struct Config {
  int nIPT;                       /* --inserts-per-transaction */
  int nThread;                    /* --threads */
  int nSecond;                    /* --seconds */
  int bMutex;                     /* --mutex */

  int bRm;                        /* --rm */
  sqlite3_mutex *pMutex;
};

static char *thread_main(int iTid, void *pArg){
  Config *pConfig = (Config*)pArg;
  Error err = {0};                /* Error code and message */
  Sqlite db = {0};                /* SQLite database connection */
  int nAttempt = 0;               /* Attempted transactions */
  int nCommit = 0;                /* Successful transactions */
  int j;

  opendb(&err, &db, "xyz.db", 0);
  sqlite3_busy_handler(db.db, 0, 0);
  execsql(&err, &db, "PRAGMA wal_autocheckpoint = 0");

  while( !timetostop(&err) ){
    execsql(&err, &db, "BEGIN CONCURRENT");
    for(j=0; j<pConfig->nIPT; j++){
      execsql(&err, &db, 
          "INSERT INTO t1 VALUES"
          "(randomblob(10), randomblob(20), randomblob(30), randomblob(200))"
      );
    }
    sqlite3_mutex_enter(pConfig->pMutex);
    execsql(&err, &db, "COMMIT");
    sqlite3_mutex_leave(pConfig->pMutex);
    nAttempt++;
    if( err.rc==SQLITE_OK ){
      nCommit++;
    }else{
      clear_error(&err, SQLITE_BUSY);
      execsql(&err, &db, "ROLLBACK");
    }
  }

  closedb(&err, &db);
  return sqlite3_mprintf("%d/%d successful commits", nCommit, nAttempt);
}

static void usage(char *zName){
  fprintf(stderr, "Usage: %s ?SWITCHES?\n", zName);
  fprintf(stderr, "\n");
  fprintf(stderr, "where switches are\n");
  fprintf(stderr, "  --seconds N\n");
  fprintf(stderr, "  --inserts N\n");
  fprintf(stderr, "  --threads N\n");
  fprintf(stderr, "  --rm BOOL\n");
  fprintf(stderr, "  --mutex BOOL\n");
  fprintf(stderr, "\n");
  exit(-1);
}

int main(int argc, char **argv){
  Error err = {0};                /* Error code and message */
  Sqlite db = {0};                /* SQLite database connection */
  Threadset threads = {0};        /* Test threads */
  Config sConfig = {5, 3, 5};
  int i;

  for(i=1; i<argc; i++){
    char *z = argv[i];
    int n = strlen(z);
    if( n>=3 && 0==sqlite3_strnicmp(z, "--seconds", n) ){
      if( (++i)==argc ) usage(argv[0]);
      sConfig.nSecond = atoi(argv[i]);
    }

    else if( n>=3 && 0==sqlite3_strnicmp(z, "--inserts", n) ){
      if( (++i)==argc ) usage(argv[0]);
      sConfig.nIPT = atoi(argv[i]);
    }

    else if( n>=3 && 0==sqlite3_strnicmp(z, "--threads", n) ){
      if( (++i)==argc ) usage(argv[0]);
      sConfig.nThread = atoi(argv[i]);
    }

    else if( n>=3 && 0==sqlite3_strnicmp(z, "--rm", n) ){
      if( (++i)==argc ) usage(argv[0]);
      sConfig.bRm = atoi(argv[i]);
    }

    else if( n>=3 && 0==sqlite3_strnicmp(z, "--mutex", n) ){
      if( (++i)==argc ) usage(argv[0]);
      sConfig.bMutex = atoi(argv[i]);
    }

    else usage(argv[0]);
  }

  printf("With: --threads %d --inserts %d --seconds %d --rm %d --mutex %d\n",
      sConfig.nThread, sConfig.nIPT, sConfig.nSecond, sConfig.bRm, 
      sConfig.bMutex
  );

  /* Ensure the schema has been created */
  if( sConfig.bMutex ){
    sConfig.pMutex = sqlite3_mutex_alloc(SQLITE_MUTEX_RECURSIVE);
  }
  opendb(&err, &db, "xyz.db", sConfig.bRm);

  sql_script(&err, &db,
      "PRAGMA journal_mode = wal;"
      "CREATE TABLE IF NOT EXISTS t1(a PRIMARY KEY, b, c, d) WITHOUT ROWID;"
      "CREATE INDEX IF NOT EXISTS t1b ON t1(b);"
      "CREATE INDEX IF NOT EXISTS t1c ON t1(c);"
  );
  closedb(&err, &db);

  setstoptime(&err, sConfig.nSecond*1000);
  for(i=0; i<sConfig.nThread; i++){
    launch_thread(&err, &threads, thread_main, (void*)&sConfig);
  }
  join_all_threads(&err, &threads);

  sqlite3_mutex_free(sConfig.pMutex);
  print_and_free_err(&err);
  return 0;
}

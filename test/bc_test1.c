/*
** 2016-05-07
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
*************************************************************************
*/


#include <sqlite3.h>
#include <stdlib.h>
#include <stddef.h>
#include "tt3_core.c"


typedef struct Config Config;
struct Config {
  int nIPT;                       /* --inserts-per-transaction */
  int nThread;                    /* --threads */
  int nSecond;                    /* --seconds */
  int bMutex;                     /* --mutex */
  int nAutoCkpt;                  /* --autockpt */
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
  sql_script_printf(&err, &db, 
      "PRAGMA wal_autocheckpoint = %d;"
      "PRAGMA synchronous = 0;", pConfig->nAutoCkpt
  );

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

int main(int argc, const char **argv){
  Error err = {0};                /* Error code and message */
  Sqlite db = {0};                /* SQLite database connection */
  Threadset threads = {0};        /* Test threads */
  Config conf = {5, 3, 5};
  int i;

  CmdlineArg apArg[] = {
    { "--seconds", CMDLINE_INT,  offsetof(Config, nSecond) },
    { "--inserts", CMDLINE_INT,  offsetof(Config, nIPT) },
    { "--threads", CMDLINE_INT,  offsetof(Config, nThread) },
    { "--mutex",   CMDLINE_BOOL, offsetof(Config, bMutex) },
    { "--rm",      CMDLINE_BOOL, offsetof(Config, bRm) },
    { "--autockpt",CMDLINE_INT,  offsetof(Config, nAutoCkpt) },
    { 0, 0, 0 }
  };

  cmdline_process(apArg, argc, argv, (void*)&conf);
  if( err.rc==SQLITE_OK ){
    char *z = cmdline_construct(apArg, (void*)&conf);
    printf("With: %s\n", z);
    sqlite3_free(z);
  }

  /* Ensure the schema has been created */
  if( conf.bMutex ){
    conf.pMutex = sqlite3_mutex_alloc(SQLITE_MUTEX_RECURSIVE);
  }
  opendb(&err, &db, "xyz.db", conf.bRm);

  sql_script(&err, &db,
      "PRAGMA journal_mode = wal;"
      "CREATE TABLE IF NOT EXISTS t1(a PRIMARY KEY, b, c, d) WITHOUT ROWID;"
      "CREATE INDEX IF NOT EXISTS t1b ON t1(b);"
      "CREATE INDEX IF NOT EXISTS t1c ON t1(c);"
  );

  setstoptime(&err, conf.nSecond*1000);
  for(i=0; i<conf.nThread; i++){
    launch_thread(&err, &threads, thread_main, (void*)&conf);
  }
  join_all_threads(&err, &threads);

  if( err.rc==SQLITE_OK ){
    printf("Database is %dK\n", (int)(filesize(&err, "xyz.db") / 1024));
  }
  if( err.rc==SQLITE_OK ){
    printf("Wal file is %dK\n", (int)(filesize(&err, "xyz.db-wal") / 1024));
  }

  closedb(&err, &db);
  sqlite3_mutex_free(conf.pMutex);
  print_and_free_err(&err);
  return 0;
}

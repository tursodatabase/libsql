/*
** 2011-02-02
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
*************************************************************************
** This file is part of the test program "threadtest3". Despite being a C
** file it is not compiled separately, but included by threadtest3.c using
** the #include directive normally used with header files.
**
** This file contains the implementation of test cases:
**
**     bcwal2_1
*/

static char *bcwal2_1_checkpointer(int iTid, void *pArg){
  Error err = {0};                /* Error code and message */
  Sqlite db = {0};                /* SQLite database connection */
  int nIter = 0;

  opendb(&err, &db, "test.db", 0);
  while( !timetostop(&err) ){
    sql_script(&err, &db, "PRAGMA wal_checkpoint;");
    nIter++;
  }
  closedb(&err, &db);

  print_and_free_err(&err);
  return sqlite3_mprintf("%d iterations", nIter);
}

static char *bcwal2_1_integrity(int iTid, void *pArg){
  Error err = {0};                /* Error code and message */
  Sqlite db = {0};                /* SQLite database connection */
  int nIter = 0;

  opendb(&err, &db, "test.db", 0);
  while( !timetostop(&err) ){
    // integrity_check(&err, &db);
    sql_script(&err, &db, "SELECT * FROM t1;");
    nIter++;
  }
  closedb(&err, &db);

  print_and_free_err(&err);
  return sqlite3_mprintf("%d integrity-checks", nIter);
}

static char *bcwal2_1_writer(int iTid, void *pArg){
  Error err = {0};                /* Error code and message */
  Sqlite db = {0};                /* SQLite database connection */
  int nWrite = 0;                 /* Writes so far */
  int nBusy = 0;                  /* Busy errors so far */
  sqlite3_mutex *pMutex = sqlite3_mutex_alloc(SQLITE_MUTEX_STATIC_APP1);

  opendb(&err, &db, "test.db", 0);
  while( !timetostop(&err) ){

    sql_script(&err, &db,
        "PRAGMA wal_autocheckpoint = 0;"
        "BEGIN CONCURRENT;"
        "  REPLACE INTO t1 VALUES( abs(random() % 100000), "
        "     hex(randomblob( abs( random() % 200 ) + 50 ))"
        "  );"
    );

    if( err.rc==SQLITE_OK ){
      sqlite3_mutex_enter(pMutex);
      sql_script(&err, &db, "COMMIT");
      sqlite3_mutex_leave(pMutex);
      if( err.rc==SQLITE_OK ){
        nWrite++;
      }else{
        clear_error(&err, SQLITE_BUSY);
        sql_script(&err, &db, "ROLLBACK");
        nBusy++;
      }

      assert( err.rc!=SQLITE_OK || sqlite3_get_autocommit(db.db)==1 );
    }
  }
  closedb(&err, &db);

  print_and_free_err(&err);
  return sqlite3_mprintf("%d successful writes, %d busy", nWrite, nBusy);
}

static void bcwal2_1(int nMs){
  Error err = {0};
  Sqlite db = {0};
  Threadset threads = {0};

  opendb(&err, &db, "test.db", 1);
  sql_script(&err, &db, 
      "PRAGMA page_size = 1024;"
      "PRAGMA journal_mode = wal2;"
      "CREATE TABLE t1(ii INTEGER PRIMARY KEY, tt TEXT);"
      "CREATE INDEX t1tt ON t1(tt);"
  );

  setstoptime(&err, nMs);

  launch_thread(&err, &threads, bcwal2_1_writer, 0);
  launch_thread(&err, &threads, bcwal2_1_writer, 0);
  launch_thread(&err, &threads, bcwal2_1_writer, 0);
  launch_thread(&err, &threads, bcwal2_1_integrity, 0);
  launch_thread(&err, &threads, bcwal2_1_checkpointer, 0);

  join_all_threads(&err, &threads);

  /* Do a final integrity-check on the db */
  integrity_check(&err, &db);
  closedb(&err, &db);

  print_and_free_err(&err);
}


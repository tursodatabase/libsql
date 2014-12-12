/*
** 2014 December 9
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
**
*/


/*
** Thread 1. CREATE and DROP a table.
*/
static char *stress_thread_1(int iTid, int iArg){
  Error err = {0};                /* Error code and message */
  Sqlite db = {0};                /* SQLite database connection */

  opendb(&err, &db, "test.db", 0);
  while( !timetostop(&err) ){
    sql_script(&err, &db, "CREATE TABLE IF NOT EXISTS t1(a PRIMARY KEY, b)");
    clear_error(&err, SQLITE_LOCKED);
    sql_script(&err, &db, "DROP TABLE IF EXISTS t1");
    clear_error(&err, SQLITE_LOCKED);
  }
  closedb(&err, &db);
  print_and_free_err(&err);
  return sqlite3_mprintf("ok");
}

/*
** Thread 2. Open and close database connections.
*/
static char *stress_thread_2(int iTid, int iArg){
  Error err = {0};                /* Error code and message */
  Sqlite db = {0};                /* SQLite database connection */
  while( !timetostop(&err) ){
    opendb(&err, &db, "test.db", 0);
    sql_script(&err, &db, "SELECT * FROM sqlite_master;");
    clear_error(&err, SQLITE_LOCKED);
    closedb(&err, &db);
  }
  print_and_free_err(&err);
  return sqlite3_mprintf("ok");
}

/*
** Thread 3. Attempt many small SELECT statements.
*/
static char *stress_thread_3(int iTid, int iArg){
  Error err = {0};                /* Error code and message */
  Sqlite db = {0};                /* SQLite database connection */

  int i1 = 0;
  int i2 = 0;

  opendb(&err, &db, "test.db", 0);
  while( !timetostop(&err) ){
    sql_script(&err, &db, "SELECT * FROM t1 ORDER BY a;");
    i1++;
    if( err.rc ) i2++;
    clear_error(&err, SQLITE_LOCKED);
    clear_error(&err, SQLITE_ERROR);
  }
  closedb(&err, &db);
  print_and_free_err(&err);
  return sqlite3_mprintf("read t1 %d/%d attempts", i2, i1);
}

/*
** Thread 5. Attempt INSERT statements.
*/
static char *stress_thread_4(int iTid, int iArg){
  Error err = {0};                /* Error code and message */
  Sqlite db = {0};                /* SQLite database connection */
  int i1 = 0;
  int i2 = 0;
  opendb(&err, &db, "test.db", 0);
  while( !timetostop(&err) ){
    if( iArg ){
      closedb(&err, &db);
      opendb(&err, &db, "test.db", 0);
    }
    sql_script(&err, &db, 
        "WITH loop(i) AS (SELECT 1 UNION ALL SELECT i+1 FROM loop LIMIT 200) "
        "INSERT INTO t1 VALUES(randomblob(60), randomblob(60));"
    );
    i1++;
    if( err.rc ) i2++;
    clear_error(&err, SQLITE_LOCKED);
    clear_error(&err, SQLITE_ERROR);
  }
  closedb(&err, &db);
  print_and_free_err(&err);
  return sqlite3_mprintf("wrote t1 %d/%d attempts", i2, i1);
}

/*
** Thread 6. Attempt DELETE operations.
*/
static char *stress_thread_5(int iTid, int iArg){
  Error err = {0};                /* Error code and message */
  Sqlite db = {0};                /* SQLite database connection */

  int i1 = 0;
  int i2 = 0;

  opendb(&err, &db, "test.db", 0);
  while( !timetostop(&err) ){
    i64 i = (i1 % 4);
    if( iArg ){
      closedb(&err, &db);
      opendb(&err, &db, "test.db", 0);
    }
    execsql(&err, &db, "DELETE FROM t1 WHERE (rowid % 4)==:i", &i);
    i1++;
    if( err.rc ) i2++;
    clear_error(&err, SQLITE_LOCKED);
    clear_error(&err, SQLITE_ERROR);
  }
  closedb(&err, &db);
  print_and_free_err(&err);
  return sqlite3_mprintf("deleted from t1 %d/%d attempts", i2, i1);
}


static void stress1(int nMs){
  Error err = {0};
  Sqlite db = {0};
  Threadset threads = {0};


  setstoptime(&err, nMs);

  sqlite3_enable_shared_cache(1);

  launch_thread(&err, &threads, stress_thread_1, 0);
  launch_thread(&err, &threads, stress_thread_1, 0);

  launch_thread(&err, &threads, stress_thread_2, 0);
  launch_thread(&err, &threads, stress_thread_2, 0);

  launch_thread(&err, &threads, stress_thread_3, 0);
  launch_thread(&err, &threads, stress_thread_3, 0);

  launch_thread(&err, &threads, stress_thread_4, 0);
  launch_thread(&err, &threads, stress_thread_4, 0);

  launch_thread(&err, &threads, stress_thread_5, 0);
  launch_thread(&err, &threads, stress_thread_5, 1);

  join_all_threads(&err, &threads);
  sqlite3_enable_shared_cache(0);

  print_and_free_err(&err);
}

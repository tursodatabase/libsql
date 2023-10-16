/*
** 2020 September 5
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
*/
static char *shared_thread1(int iTid, void *pArg){
  Error err = {0};                /* Error code and message */

  while( !timetostop(&err) ){
    Sqlite db = {0};              /* SQLite database connection */
    opendb(&err, &db, "test.db", 0);
    sql_script(&err, &db, "SELECT * FROM t1");
    closedb(&err, &db);
  }
  print_and_free_err(&err);
  return sqlite3_mprintf("done!");
}


static void shared1(int nMs){
  Error err = {0};
  Sqlite db = {0};              /* SQLite database connection */
  Threadset threads = {0};
  int ii;

  opendb(&err, &db, "test.db", 1);
  sql_script(&err, &db, "CREATE TABLE t1(x)");
  closedb(&err, &db);

  setstoptime(&err, nMs);
  sqlite3_enable_shared_cache(1);

  for(ii=0; ii<5; ii++){
    launch_thread(&err, &threads, shared_thread1, 0);
  }

  join_all_threads(&err, &threads);
  sqlite3_enable_shared_cache(0);

  print_and_free_err(&err);
}


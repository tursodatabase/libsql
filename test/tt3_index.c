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
**     create_drop_index_1
*/


static char *create_drop_index_thread(int iTid, int iArg){
  Error err = {0};                /* Error code and message */
  Sqlite db = {0};                /* SQLite database connection */

  while( !timetostop(&err) ){
    opendb(&err, &db, "test.db", 0);

    sql_script(&err, &db, 

      "DROP INDEX IF EXISTS i1;"
      "DROP INDEX IF EXISTS i2;"
      "DROP INDEX IF EXISTS i3;"
      "DROP INDEX IF EXISTS i4;"

      "CREATE INDEX IF NOT EXISTS i1 ON t1(a);"
      "CREATE INDEX IF NOT EXISTS i2 ON t1(b);"
      "CREATE INDEX IF NOT EXISTS i3 ON t1(c);"
      "CREATE INDEX IF NOT EXISTS i4 ON t1(d);"

      "SELECT * FROM t1 ORDER BY a;"
      "SELECT * FROM t1 ORDER BY b;"
      "SELECT * FROM t1 ORDER BY c;"
      "SELECT * FROM t1 ORDER BY d;"
    );

    closedb(&err, &db);
  }

  print_and_free_err(&err);
  return sqlite3_mprintf("ok");
}

static void create_drop_index_1(int nMs){
  Error err = {0};
  Sqlite db = {0};
  Threadset threads = {0};

  opendb(&err, &db, "test.db", 1);
  sql_script(&err, &db, 
     "CREATE TABLE t1(a, b, c, d);"
     "WITH data(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM data WHERE x<100) "
     "INSERT INTO t1 SELECT x,x,x,x FROM data;"
  );
  closedb(&err, &db);

  setstoptime(&err, nMs);

  sqlite3_enable_shared_cache(1);
  launch_thread(&err, &threads, create_drop_index_thread, 0);
  launch_thread(&err, &threads, create_drop_index_thread, 0);
  launch_thread(&err, &threads, create_drop_index_thread, 0);
  launch_thread(&err, &threads, create_drop_index_thread, 0);
  launch_thread(&err, &threads, create_drop_index_thread, 0);
  sqlite3_enable_shared_cache(0);

  join_all_threads(&err, &threads);
  print_and_free_err(&err);
}

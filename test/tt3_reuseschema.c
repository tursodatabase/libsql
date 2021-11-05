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
**     reuse_schema_1
*/


static char *reuse_schema_thread(int iTid, void *pArg){
  Error err = {0};                /* Error code and message */
  Sqlite db = {0};                /* SQLite database connection */
  int iRep = 0;

  while( !timetostop(&err) ){
    int f = SQLITE_OPEN_READWRITE|SQLITE_OPEN_SHARED_SCHEMA;
    opendb(&err, &db, "test.db", 0, f);

    execsql_i64(&err, &db, "SELECT count(*) FROM t1");
    sql_script(&err, &db, "ATTACH 'test.db2' AS aux");
    execsql_i64(&err, &db, "SELECT count(*) FROM t1");

    closedb(&err, &db);
    iRep++;
  }

  print_and_free_err(&err);
  return sqlite3_mprintf("%d", iRep);
}

static void reuse_schema_1(int nMs){
  Error err = {0};
  Sqlite db = {0};
  Threadset threads = {0};

  opendb(&err, &db, "test.db", 1, 0);
  sql_script(&err, &db, 
     "CREATE TABLE t1(a, b, c, d);"
     "WITH data(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM data WHERE x<100) "
     "INSERT INTO t1 SELECT x,x,x,x FROM data;"
  );
  closedb(&err, &db);
  opendb(&err, &db, "test.db2", 1, 0);
  sql_script(&err, &db, 
#ifdef SQLITE_ENABLE_FTS5
     "CREATE VIRTUAL TABLE t2 USING fts5(a, b, c, d);"
#else
     "CREATE TABLE t2(a, b, c, d);"
#endif
     "WITH data(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM data WHERE x<100) "
     "INSERT INTO t2 SELECT x*2,x*2,x*2,x*2 FROM data;"
  );
  closedb(&err, &db);

  setstoptime(&err, nMs);

  launch_thread(&err, &threads, reuse_schema_thread, 0);
  launch_thread(&err, &threads, reuse_schema_thread, 0);
  launch_thread(&err, &threads, reuse_schema_thread, 0);
  launch_thread(&err, &threads, reuse_schema_thread, 0);
  launch_thread(&err, &threads, reuse_schema_thread, 0);

  join_all_threads(&err, &threads);
  sqlite3_enable_shared_cache(0);
  print_and_free_err(&err);
}

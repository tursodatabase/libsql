/*
** 2010-07-22
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
** The code in this file runs a few multi-threaded test cases using the
** SQLite library. It can be compiled to an executable on unix using the
** following command:
**
**   gcc -O2 threadtest3.c sqlite3.c -ldl -lpthread -lm
**
** Even though threadtest3.c is the only C source code file mentioned on
** the compiler command-line, #include macros are used to pull in additional
** C code files named "tt3_*.c".
**
** After compiling, run this program with an optional argument telling
** which test to run.  All tests are run if no argument is given.  The
** argument can be a glob pattern to match multiple tests.  Examples:
**
**        ./a.out                 -- Run all tests
**        ./a.out walthread3      -- Run the "walthread3" test
**        ./a.out 'wal*'          -- Run all of the wal* tests
**        ./a.out --help          -- List all available tests
**
** The exit status is non-zero if any test fails.
*/





#include <sqlite3.h>

#include "test_multiplex.h"
#include "tt3_core.c"

/* Required to link test_multiplex.c */
#ifndef SQLITE_OMIT_WSD
int sqlite3PendingByte = 0x40000000;
#endif


/*************************************************************************
**************************************************************************
**************************************************************************
** End infrastructure. Begin tests.
*/

#define WALTHREAD1_NTHREAD  10
#define WALTHREAD3_NTHREAD  6

static char *walthread1_thread(int iTid, void *pArg){
  Error err = {0};                /* Error code and message */
  Sqlite db = {0};                /* SQLite database connection */
  int nIter = 0;                  /* Iterations so far */

  opendb(&err, &db, "test.db", 0);
  while( !timetostop(&err) ){
    const char *azSql[] = {
      "SELECT md5sum(x) FROM t1 WHERE rowid != (SELECT max(rowid) FROM t1)",
      "SELECT x FROM t1 WHERE rowid = (SELECT max(rowid) FROM t1)",
    };
    char *z1, *z2, *z3;

    execsql(&err, &db, "BEGIN");
    integrity_check(&err, &db);
    z1 = execsql_text(&err, &db, 1, azSql[0]);
    z2 = execsql_text(&err, &db, 2, azSql[1]);
    z3 = execsql_text(&err, &db, 3, azSql[0]);
    execsql(&err, &db, "COMMIT");

    if( strcmp(z1, z2) || strcmp(z1, z3) ){
      test_error(&err, "Failed read: %s %s %s", z1, z2, z3);
    }

    sql_script(&err, &db,
        "BEGIN;"
          "INSERT INTO t1 VALUES(randomblob(100));"
          "INSERT INTO t1 VALUES(randomblob(100));"
          "INSERT INTO t1 SELECT md5sum(x) FROM t1;"
        "COMMIT;"
    );
    nIter++;
  }
  closedb(&err, &db);

  print_and_free_err(&err);
  return sqlite3_mprintf("%d iterations", nIter);
}

static char *walthread1_ckpt_thread(int iTid, void *pArg){
  Error err = {0};                /* Error code and message */
  Sqlite db = {0};                /* SQLite database connection */
  int nCkpt = 0;                  /* Checkpoints so far */

  opendb(&err, &db, "test.db", 0);
  while( !timetostop(&err) ){
    usleep(500*1000);
    execsql(&err, &db, "PRAGMA wal_checkpoint");
    if( err.rc==SQLITE_OK ) nCkpt++;
    clear_error(&err, SQLITE_BUSY);
  }
  closedb(&err, &db);

  print_and_free_err(&err);
  return sqlite3_mprintf("%d checkpoints", nCkpt);
}

static void walthread1(int nMs){
  Error err = {0};                /* Error code and message */
  Sqlite db = {0};                /* SQLite database connection */
  Threadset threads = {0};        /* Test threads */
  int i;                          /* Iterator variable */

  opendb(&err, &db, "test.db", 1);
  sql_script(&err, &db,
      "PRAGMA journal_mode = WAL;"
      "CREATE TABLE t1(x PRIMARY KEY);"
      "INSERT INTO t1 VALUES(randomblob(100));"
      "INSERT INTO t1 VALUES(randomblob(100));"
      "INSERT INTO t1 SELECT md5sum(x) FROM t1;"
  );
  closedb(&err, &db);

  setstoptime(&err, nMs);
  for(i=0; i<WALTHREAD1_NTHREAD; i++){
    launch_thread(&err, &threads, walthread1_thread, 0);
  }
  launch_thread(&err, &threads, walthread1_ckpt_thread, 0);
  join_all_threads(&err, &threads);

  print_and_free_err(&err);
}

static char *walthread2_thread(int iTid, void *pArg){
  Error err = {0};                /* Error code and message */
  Sqlite db = {0};                /* SQLite database connection */
  int anTrans[2] = {0, 0};        /* Number of WAL and Rollback transactions */
  int iArg = PTR2INT(pArg);

  const char *zJournal = "PRAGMA journal_mode = WAL";
  if( iArg ){ zJournal = "PRAGMA journal_mode = DELETE"; }

  while( !timetostop(&err) ){
    int journal_exists = 0;
    int wal_exists = 0;

    opendb(&err, &db, "test.db", 0);

    sql_script(&err, &db, zJournal);
    clear_error(&err, SQLITE_BUSY);
    sql_script(&err, &db, "BEGIN");
    sql_script(&err, &db, "INSERT INTO t1 VALUES(NULL, randomblob(100))");

    journal_exists = (filesize(&err, "test.db-journal") >= 0);
    wal_exists = (filesize(&err, "test.db-wal") >= 0);
    if( (journal_exists+wal_exists)!=1 ){
      test_error(&err, "File system looks incorrect (%d, %d)", 
          journal_exists, wal_exists
      );
    }
    anTrans[journal_exists]++;

    sql_script(&err, &db, "COMMIT");
    integrity_check(&err, &db);
    closedb(&err, &db);
  }

  print_and_free_err(&err);
  return sqlite3_mprintf("W %d R %d", anTrans[0], anTrans[1]);
}

static void walthread2(int nMs){
  Error err = {0};
  Sqlite db = {0};
  Threadset threads = {0};

  opendb(&err, &db, "test.db", 1);
  sql_script(&err, &db, "CREATE TABLE t1(x INTEGER PRIMARY KEY, y UNIQUE)");
  closedb(&err, &db);

  setstoptime(&err, nMs);
  launch_thread(&err, &threads, walthread2_thread, 0);
  launch_thread(&err, &threads, walthread2_thread, 0);
  launch_thread(&err, &threads, walthread2_thread, (void*)1);
  launch_thread(&err, &threads, walthread2_thread, (void*)1);
  join_all_threads(&err, &threads);

  print_and_free_err(&err);
}

static char *walthread3_thread(int iTid, void *pArg){
  Error err = {0};                /* Error code and message */
  Sqlite db = {0};                /* SQLite database connection */
  i64 iNextWrite;                 /* Next value this thread will write */
  int iArg = PTR2INT(pArg);

  opendb(&err, &db, "test.db", 0);
  sql_script(&err, &db, "PRAGMA wal_autocheckpoint = 10");

  iNextWrite = iArg+1;
  while( 1 ){
    i64 sum1;
    i64 sum2;
    int stop = 0;                 /* True to stop executing (test timed out) */

    while( 0==(stop = timetostop(&err)) ){
      i64 iMax = execsql_i64(&err, &db, "SELECT max(cnt) FROM t1");
      if( iMax+1==iNextWrite ) break;
    }
    if( stop ) break;

    sum1 = execsql_i64(&err, &db, "SELECT sum(cnt) FROM t1");
    sum2 = execsql_i64(&err, &db, "SELECT sum(sum1) FROM t1");
    execsql_i64(&err, &db, 
        "INSERT INTO t1 VALUES(:iNextWrite, :iSum1, :iSum2)",
        &iNextWrite, &sum1, &sum2
    );
    integrity_check(&err, &db);

    iNextWrite += WALTHREAD3_NTHREAD;
  }

  closedb(&err, &db);
  print_and_free_err(&err);
  return 0;
}

static void walthread3(int nMs){
  Error err = {0};
  Sqlite db = {0};
  Threadset threads = {0};
  int i;

  opendb(&err, &db, "test.db", 1);
  sql_script(&err, &db, 
      "PRAGMA journal_mode = WAL;"
      "CREATE TABLE t1(cnt PRIMARY KEY, sum1, sum2);"
      "CREATE INDEX i1 ON t1(sum1);"
      "CREATE INDEX i2 ON t1(sum2);"
      "INSERT INTO t1 VALUES(0, 0, 0);"
  );
  closedb(&err, &db);

  setstoptime(&err, nMs);
  for(i=0; i<WALTHREAD3_NTHREAD; i++){
    launch_thread(&err, &threads, walthread3_thread, INT2PTR(i));
  }
  join_all_threads(&err, &threads);

  print_and_free_err(&err);
}

static char *walthread4_reader_thread(int iTid, void *pArg){
  Error err = {0};                /* Error code and message */
  Sqlite db = {0};                /* SQLite database connection */

  opendb(&err, &db, "test.db", 0);
  while( !timetostop(&err) ){
    integrity_check(&err, &db);
  }
  closedb(&err, &db);

  print_and_free_err(&err);
  return 0;
}

static char *walthread4_writer_thread(int iTid, void *pArg){
  Error err = {0};                /* Error code and message */
  Sqlite db = {0};                /* SQLite database connection */
  i64 iRow = 1;

  opendb(&err, &db, "test.db", 0);
  sql_script(&err, &db, "PRAGMA wal_autocheckpoint = 15;");
  while( !timetostop(&err) ){
    execsql_i64(
        &err, &db, "REPLACE INTO t1 VALUES(:iRow, randomblob(300))", &iRow
    );
    iRow++;
    if( iRow==10 ) iRow = 0;
  }
  closedb(&err, &db);

  print_and_free_err(&err);
  return 0;
}

static void walthread4(int nMs){
  Error err = {0};
  Sqlite db = {0};
  Threadset threads = {0};

  opendb(&err, &db, "test.db", 1);
  sql_script(&err, &db, 
      "PRAGMA journal_mode = WAL;"
      "CREATE TABLE t1(a INTEGER PRIMARY KEY, b UNIQUE);"
  );
  closedb(&err, &db);

  setstoptime(&err, nMs);
  launch_thread(&err, &threads, walthread4_reader_thread, 0);
  launch_thread(&err, &threads, walthread4_writer_thread, 0);
  join_all_threads(&err, &threads);

  print_and_free_err(&err);
}

static char *walthread5_thread(int iTid, void *pArg){
  Error err = {0};                /* Error code and message */
  Sqlite db = {0};                /* SQLite database connection */
  i64 nRow;

  opendb(&err, &db, "test.db", 0);
  nRow = execsql_i64(&err, &db, "SELECT count(*) FROM t1");
  closedb(&err, &db);

  if( nRow!=65536 ) test_error(&err, "Bad row count: %d", (int)nRow);
  print_and_free_err(&err);
  return 0;
}
static void walthread5(int nMs){
  Error err = {0};
  Sqlite db = {0};
  Threadset threads = {0};

  opendb(&err, &db, "test.db", 1);
  sql_script(&err, &db, 
      "PRAGMA wal_autocheckpoint = 0;"
      "PRAGMA page_size = 1024;"
      "PRAGMA journal_mode = WAL;"
      "CREATE TABLE t1(x);"
      "BEGIN;"
      "INSERT INTO t1 VALUES(randomblob(900));"
      "INSERT INTO t1 SELECT randomblob(900) FROM t1;      /*     2 */"
      "INSERT INTO t1 SELECT randomblob(900) FROM t1;      /*     4 */"
      "INSERT INTO t1 SELECT randomblob(900) FROM t1;      /*     8 */"
      "INSERT INTO t1 SELECT randomblob(900) FROM t1;      /*    16 */"
      "INSERT INTO t1 SELECT randomblob(900) FROM t1;      /*    32 */"
      "INSERT INTO t1 SELECT randomblob(900) FROM t1;      /*    64 */"
      "INSERT INTO t1 SELECT randomblob(900) FROM t1;      /*   128 */"
      "INSERT INTO t1 SELECT randomblob(900) FROM t1;      /*   256 */"
      "INSERT INTO t1 SELECT randomblob(900) FROM t1;      /*   512 */"
      "INSERT INTO t1 SELECT randomblob(900) FROM t1;      /*  1024 */"
      "INSERT INTO t1 SELECT randomblob(900) FROM t1;      /*  2048 */"
      "INSERT INTO t1 SELECT randomblob(900) FROM t1;      /*  4096 */"
      "INSERT INTO t1 SELECT randomblob(900) FROM t1;      /*  8192 */"
      "INSERT INTO t1 SELECT randomblob(900) FROM t1;      /* 16384 */"
      "INSERT INTO t1 SELECT randomblob(900) FROM t1;      /* 32768 */"
      "INSERT INTO t1 SELECT randomblob(900) FROM t1;      /* 65536 */"
      "COMMIT;"
  );
  filecopy(&err, "test.db", "test_sv.db");
  filecopy(&err, "test.db-wal", "test_sv.db-wal");
  closedb(&err, &db);

  filecopy(&err, "test_sv.db", "test.db");
  filecopy(&err, "test_sv.db-wal", "test.db-wal");

  if( err.rc==SQLITE_OK ){
    printf("  WAL file is %d bytes,", (int)filesize(&err,"test.db-wal"));
    printf(" DB file is %d.\n", (int)filesize(&err,"test.db"));
  }

  setstoptime(&err, nMs);
  launch_thread(&err, &threads, walthread5_thread, 0);
  launch_thread(&err, &threads, walthread5_thread, 0);
  launch_thread(&err, &threads, walthread5_thread, 0);
  launch_thread(&err, &threads, walthread5_thread, 0);
  launch_thread(&err, &threads, walthread5_thread, 0);
  join_all_threads(&err, &threads);

  if( err.rc==SQLITE_OK ){
    printf("  WAL file is %d bytes,", (int)filesize(&err,"test.db-wal"));
    printf(" DB file is %d.\n", (int)filesize(&err,"test.db"));
  }

  print_and_free_err(&err);
}

/*------------------------------------------------------------------------
** Test case "cgt_pager_1"
*/
#define CALLGRINDTEST1_NROW 10000
static void cgt_pager_1_populate(Error *pErr, Sqlite *pDb){
  const char *zInsert = "INSERT INTO t1 VALUES(:iRow, zeroblob(:iBlob))";
  i64 iRow;
  sql_script(pErr, pDb, "BEGIN");
  for(iRow=1; iRow<=CALLGRINDTEST1_NROW; iRow++){
    i64 iBlob = 600 + (iRow%300);
    execsql(pErr, pDb, zInsert, &iRow, &iBlob);
  }
  sql_script(pErr, pDb, "COMMIT");
}
static void cgt_pager_1_update(Error *pErr, Sqlite *pDb){
  const char *zUpdate = "UPDATE t1 SET b = zeroblob(:iBlob) WHERE a = :iRow";
  i64 iRow;
  sql_script(pErr, pDb, "BEGIN");
  for(iRow=1; iRow<=CALLGRINDTEST1_NROW; iRow++){
    i64 iBlob = 600 + ((iRow+100)%300);
    execsql(pErr, pDb, zUpdate, &iBlob, &iRow);
  }
  sql_script(pErr, pDb, "COMMIT");
}
static void cgt_pager_1_read(Error *pErr, Sqlite *pDb){
  i64 iRow;
  sql_script(pErr, pDb, "BEGIN");
  for(iRow=1; iRow<=CALLGRINDTEST1_NROW; iRow++){
    execsql(pErr, pDb, "SELECT * FROM t1 WHERE a = :iRow", &iRow);
  }
  sql_script(pErr, pDb, "COMMIT");
}
static void cgt_pager_1(int nMs){
  void (*xSub)(Error *, Sqlite *);
  Error err = {0};
  Sqlite db = {0};

  opendb(&err, &db, "test.db", 1);
  sql_script(&err, &db,
      "PRAGMA cache_size = 2000;"
      "PRAGMA page_size = 1024;"
      "CREATE TABLE t1(a INTEGER PRIMARY KEY, b BLOB);"
  );

  xSub = cgt_pager_1_populate; xSub(&err, &db);
  xSub = cgt_pager_1_update;   xSub(&err, &db);
  xSub = cgt_pager_1_read;     xSub(&err, &db);

  closedb(&err, &db);
  print_and_free_err(&err);
}

/*------------------------------------------------------------------------
** Test case "dynamic_triggers"
**
**   Two threads executing statements that cause deeply nested triggers
**   to fire. And one thread busily creating and deleting triggers. This
**   is an attempt to find a bug reported to us.
*/

static char *dynamic_triggers_1(int iTid, void *pArg){
  Error err = {0};                /* Error code and message */
  Sqlite db = {0};                /* SQLite database connection */
  int nDrop = 0;
  int nCreate = 0;

  opendb(&err, &db, "test.db", 0);
  while( !timetostop(&err) ){
    int i;

    for(i=1; i<9; i++){
      char *zSql = sqlite3_mprintf(
        "CREATE TRIGGER itr%d BEFORE INSERT ON t%d BEGIN "
          "INSERT INTO t%d VALUES(new.x, new.y);"
        "END;", i, i, i+1
      );
      execsql(&err, &db, zSql);
      sqlite3_free(zSql);
      nCreate++;
    }

    for(i=1; i<9; i++){
      char *zSql = sqlite3_mprintf(
        "CREATE TRIGGER dtr%d BEFORE DELETE ON t%d BEGIN "
          "DELETE FROM t%d WHERE x = old.x; "
        "END;", i, i, i+1
      );
      execsql(&err, &db, zSql);
      sqlite3_free(zSql);
      nCreate++;
    }

    for(i=1; i<9; i++){
      char *zSql = sqlite3_mprintf("DROP TRIGGER itr%d", i);
      execsql(&err, &db, zSql);
      sqlite3_free(zSql);
      nDrop++;
    }

    for(i=1; i<9; i++){
      char *zSql = sqlite3_mprintf("DROP TRIGGER dtr%d", i);
      execsql(&err, &db, zSql);
      sqlite3_free(zSql);
      nDrop++;
    }
  }
  closedb(&err, &db);

  print_and_free_err(&err);
  return sqlite3_mprintf("%d created, %d dropped", nCreate, nDrop);
}

static char *dynamic_triggers_2(int iTid, void *pArg){
  Error err = {0};                /* Error code and message */
  Sqlite db = {0};                /* SQLite database connection */
  i64 iVal = 0;
  int nInsert = 0;
  int nDelete = 0;

  opendb(&err, &db, "test.db", 0);
  while( !timetostop(&err) ){
    do {
      iVal = (iVal+1)%100;
      execsql(&err, &db, "INSERT INTO t1 VALUES(:iX, :iY+1)", &iVal, &iVal);
      nInsert++;
    } while( iVal );

    do {
      iVal = (iVal+1)%100;
      execsql(&err, &db, "DELETE FROM t1 WHERE x = :iX", &iVal);
      nDelete++;
    } while( iVal );
  }
  closedb(&err, &db);

  print_and_free_err(&err);
  return sqlite3_mprintf("%d inserts, %d deletes", nInsert, nDelete);
}

static void dynamic_triggers(int nMs){
  Error err = {0};
  Sqlite db = {0};
  Threadset threads = {0};

  opendb(&err, &db, "test.db", 1);
  sql_script(&err, &db, 
      "PRAGMA page_size = 1024;"
      "PRAGMA journal_mode = WAL;"
      "CREATE TABLE t1(x, y);"
      "CREATE TABLE t2(x, y);"
      "CREATE TABLE t3(x, y);"
      "CREATE TABLE t4(x, y);"
      "CREATE TABLE t5(x, y);"
      "CREATE TABLE t6(x, y);"
      "CREATE TABLE t7(x, y);"
      "CREATE TABLE t8(x, y);"
      "CREATE TABLE t9(x, y);"
  );
  closedb(&err, &db);

  setstoptime(&err, nMs);

  sqlite3_enable_shared_cache(1);
  launch_thread(&err, &threads, dynamic_triggers_2, 0);
  launch_thread(&err, &threads, dynamic_triggers_2, 0);

  sleep(2);
  sqlite3_enable_shared_cache(0);

  launch_thread(&err, &threads, dynamic_triggers_2, 0);
  launch_thread(&err, &threads, dynamic_triggers_1, 0);

  join_all_threads(&err, &threads);

  print_and_free_err(&err);
}



#include "tt3_checkpoint.c"
#include "tt3_index.c"
#include "tt3_lookaside1.c"
#include "tt3_vacuum.c"
#include "tt3_stress.c"
#include "tt3_shared.c"

int main(int argc, char **argv){
  struct ThreadTest {
    void (*xTest)(int);   /* Routine for running this test */
    const char *zTest;    /* Name of this test */
    int nMs;              /* How long to run this test, in milliseconds */
  } aTest[] = {
    { walthread1, "walthread1", 20000 },
    { walthread2, "walthread2", 20000 },
    { walthread3, "walthread3", 20000 },
    { walthread4, "walthread4", 20000 },
    { walthread5, "walthread5",  1000 },
    
    { cgt_pager_1,      "cgt_pager_1", 0 },
    { dynamic_triggers, "dynamic_triggers", 20000 },

    { checkpoint_starvation_1, "checkpoint_starvation_1", 10000 },
    { checkpoint_starvation_2, "checkpoint_starvation_2", 10000 },

    { create_drop_index_1, "create_drop_index_1", 10000 },
    { lookaside1,          "lookaside1", 10000 },
    { vacuum1,             "vacuum1", 10000 },
    { stress1,             "stress1", 10000 },
    { stress2,             "stress2", 60000 },
    { shared1,             "shared1", 10000 },
  };
  static char *substArgv[] = { 0, "*", 0 };
  int i, iArg;
  int nTestfound = 0;

  sqlite3_config(SQLITE_CONFIG_MULTITHREAD);
  if( argc<2 ){
    argc = 2;
    argv = substArgv;
  }

  /* Loop through the command-line arguments to ensure that each argument
  ** selects at least one test. If not, assume there is a typo on the 
  ** command-line and bail out with the usage message.  */
  for(iArg=1; iArg<argc; iArg++){
    const char *zArg = argv[iArg];
    if( zArg[0]=='-' ){
      if( sqlite3_stricmp(zArg, "-multiplexor")==0 ){
        /* Install the multiplexor VFS as the default */
        int rc = sqlite3_multiplex_initialize(0, 1);
        if( rc!=SQLITE_OK ){
          fprintf(stderr, "Failed to install multiplexor VFS (%d)\n", rc);
          return 253;
        }
      }
      else {
        goto usage;
      }

      continue;
    }

    for(i=0; i<sizeof(aTest)/sizeof(aTest[0]); i++){
      if( sqlite3_strglob(zArg, aTest[i].zTest)==0 ) break;
    }
    if( i>=sizeof(aTest)/sizeof(aTest[0]) ) goto usage;   
  }

  for(iArg=1; iArg<argc; iArg++){
    if( argv[iArg][0]=='-' ) continue;
    for(i=0; i<sizeof(aTest)/sizeof(aTest[0]); i++){
      char const *z = aTest[i].zTest;
      if( sqlite3_strglob(argv[iArg],z)==0 ){
        printf("Running %s for %d seconds...\n", z, aTest[i].nMs/1000);
        fflush(stdout);
        aTest[i].xTest(aTest[i].nMs);
        nTestfound++;
      }
    }
  }
  if( nTestfound==0 ) goto usage;

  printf("%d errors out of %d tests\n", nGlobalErr, nTestfound);
  return (nGlobalErr>0 ? 255 : 0);

 usage:
  printf("Usage: %s [-multiplexor] [testname|testprefix*]...\n", argv[0]);
  printf("Available tests are:\n");
  for(i=0; i<sizeof(aTest)/sizeof(aTest[0]); i++){
    printf("   %s\n", aTest[i].zTest);
  }

  return 254;
}

/*
** 2018-10-26
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
** This program is designed for fuzz-testing SQLite database files using
** the -fsanitize=fuzzer option of clang.
**
** The -fsanitize=fuzzer option causes a main() to be inserted automatically.
** That main() invokes LLVMFuzzerTestOneInput(D,S) to be invoked repeatedly.
** Each D is a fuzzed database file.  The code in this file runs various
** SQL statements against that database, trying to provoke a failure.
**
** For best results the seed database files should have these tables:
**
**   Table "t1" with columns "a" and "b"
**   Tables "t2" and "t3 with the same number of compatible columns
**       "t3" should have a column names "x"
**   Table "t4" with a column "x" that is compatible with t3.x.
**
** Any of these tables can be virtual tables, for example FTS or RTree tables.
**
** To run this test:
**
**     mkdir dir
**     cp dbfuzz2-seed*.db dir
**     clang-6.0 -I. -g -O1 -fsanitize=fuzzer \
**       -DTHREADSAFE=0 -DSQLITE_ENABLE_DESERIALIZE \
**       -DSQLITE_ENABLE_DBSTAT_VTAB dbfuzz2.c sqlite3.c -ldl
**     ./a.out dir
*/
#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdarg.h>
#include <ctype.h>
#include <stdint.h>
#include "sqlite3.h"

/*
** This is the is the SQL that is run against the database.
*/
static const char *azSql[] = {
  "PRAGMA integrity_check;",
  "SELECT * FROM sqlite_master;",
  "SELECT sum(length(name)) FROM dbstat;",
  "UPDATE t1 SET b=a, a=b WHERE a<b;",
  "ALTER TABLE t1 RENAME TO alkjalkjdfiiiwuer987lkjwer82mx97sf98788s9789s;"
  "INSERT INTO t3 SELECT * FROM t2;",
  "DELETE FROM t3 WHERE x IN (SELECT x FROM t4);",
  "REINDEX;"
  "DROP TABLE t3;",
  "VACUUM;",
};

int LLVMFuzzerTestOneInput(const uint8_t *aData, size_t nByte){
  unsigned char *a;
  sqlite3 *db;
  int rc;
  int i;

  rc = sqlite3_open(":memory:", &db);
  if( rc ) return 1;
  a = sqlite3_malloc64(nByte);
  if( a==0 ) return 1;
  memcpy(a, aData, nByte);
  sqlite3_deserialize(db, "main", a, nByte, nByte,
        SQLITE_DESERIALIZE_RESIZEABLE |
        SQLITE_DESERIALIZE_FREEONCLOSE);
  for(i=0; i<sizeof(azSql)/sizeof(azSql[0]); i++){
    sqlite3_exec(db, azSql[i], 0, 0, 0);
  }
  sqlite3_close(db);
  return 0;
}

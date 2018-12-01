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
  "ALTER TABLE t1 RENAME TO alkjalkjdfiiiwuer987lkjwer82mx97sf98788s9789s;",
  "INSERT INTO t3 SELECT * FROM t2;",
  "DELETE FROM t3 WHERE x IN (SELECT x FROM t4);",
  "REINDEX;",
  "DROP TABLE t3;",
  "VACUUM;",
};

/* Output verbosity level.  0 means complete silence */
int eVerbosity = 0;

/* libFuzzer invokes this routine with fuzzed database files (in aData).
** This routine run SQLite against the malformed database to see if it
** can provoke a failure or malfunction.
*/
int LLVMFuzzerTestOneInput(const uint8_t *aData, size_t nByte){
  unsigned char *a;
  sqlite3 *db;
  int rc;
  int i;

  if( eVerbosity>=1 ){
    printf("************** nByte=%d ***************\n", (int)nByte);
    fflush(stdout);
  }
  rc = sqlite3_open(0, &db);
  if( rc ) return 1;
  a = sqlite3_malloc64(nByte+1);
  if( a==0 ) return 1;
  memcpy(a, aData, nByte);
  sqlite3_deserialize(db, "main", a, nByte, nByte,
        SQLITE_DESERIALIZE_RESIZEABLE |
        SQLITE_DESERIALIZE_FREEONCLOSE);
  for(i=0; i<sizeof(azSql)/sizeof(azSql[0]); i++){
    if( eVerbosity>=1 ){
      printf("%s\n", azSql[i]);
      fflush(stdout);
    }
    sqlite3_exec(db, azSql[i], 0, 0, 0);
  }
  rc = sqlite3_close(db);
  if( rc!=SQLITE_OK ){
    fprintf(stdout, "sqlite3_close() returns %d\n", rc);
  }
  if( sqlite3_memory_used()!=0 ){
    int nAlloc = 0;
    int nNotUsed = 0;
    sqlite3_status(SQLITE_STATUS_MALLOC_COUNT, &nAlloc, &nNotUsed, 0);
    fprintf(stderr,"Memory leak: %lld bytes in %d allocations\n",
            sqlite3_memory_used(), nAlloc);
    exit(1);
  }
  return 0;
}

/* libFuzzer invokes this routine once when the executable starts, to
** process the command-line arguments.
*/
int LLVMFuzzerInitialize(int *pArgc, char ***pArgv){
  int i, j;
  int argc = *pArgc;
  char **newArgv;
  char **argv = *pArgv;
  newArgv = malloc( sizeof(char*)*(argc+1) );
  if( newArgv==0 ) return 0;
  newArgv[0] = argv[0];
  for(i=j=1; i<argc; i++){
    char *z = argv[i];
    if( z[0]=='-' ){
      z++;
      if( z[0]=='-' ) z++;
      if( strcmp(z,"v")==0 ){
        eVerbosity++;
        continue;
      }
    }
    newArgv[j++] = argv[i];
  }
  newArgv[j] = 0;
  *pArgv = newArgv;
  *pArgc = j;
  return 0;
}

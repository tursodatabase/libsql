/*
** 2018-12-04
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
** This file implements a utility program used to help determine which
** indexes in a database schema are used and unused, and how often specific
** indexes are used.
*/
#include "sqlite3.h"
#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <string.h>

static void usage(const char *argv0){
  printf("Usage: %s DATABASE LOG\n\n", argv0);
  printf(
    "DATABASE is an SQLite database against which various statements\n"
    "have been run.  The SQL text is stored in LOG.  LOG is an SQLite\n"
    "database with this schema:\n"
    "\n"
    "    CREATE TABLE sqllog(sql TEXT);\n"
    "\n"
    "This utility program analyzes statements contained in LOG and prints\n"
    "a report showing how many times each index in DATABASE is used by the\n"
    "statements in LOG.\n"
    "\n"
    "DATABASE only needs to contain the schema used by the statements in\n"
    "LOG. The content can be removed from DATABASE.\n"
  );
  printf("\nAnalysis will be done by SQLite version %s dated %.20s\n"
         "checkin number %.40s. Different versions\n"
         "of SQLite might use different indexes.\n",
         sqlite3_libversion(), sqlite3_sourceid(), sqlite3_sourceid()+21);
  exit(1);
}

int main(int argc, char **argv){
  sqlite3 *db = 0;          /* The main database */
  sqlite3_stmt *pStmt = 0;  /* a query */
  char *zSql;
  int nErr = 0;
  int rc;

  if( argc!=3 ) usage(argv[0]);
  rc = sqlite3_open_v2(argv[1], &db, SQLITE_OPEN_READONLY, 0);
  if( rc ){
    printf("Cannot open \"%s\" for reading: %s\n", argv[1], sqlite3_errmsg(db));
    goto errorOut;
  }
  rc = sqlite3_prepare_v2(db, "SELECT * FROM sqlite_master", -1, &pStmt, 0);
  if( rc ){
    printf("Cannot read the schema from \"%s\" - %s\n", argv[1],
           sqlite3_errmsg(db));
    goto errorOut;
  }
  sqlite3_finalize(pStmt);
  pStmt = 0;
  rc = sqlite3_exec(db, 
     "CREATE TABLE temp.idxu(\n"
     "  tbl TEXT,\n"
     "  idx TEXT,\n"
     "  cnt INT,\n"
     "  PRIMARY KEY(idx)\n"
     ") WITHOUT ROWID;", 0, 0, 0);
  if( rc ){
    printf("Cannot create the result table - %s\n",
           sqlite3_errmsg(db));
    goto errorOut;
  }
  rc = sqlite3_exec(db,
     "INSERT INTO temp.idxu(tbl,idx,cnt)"
     " SELECT tbl_name, name, 0 FROM sqlite_master"
     " WHERE type='index' AND sql IS NOT NULL", 0, 0, 0);

  /* Open the LOG database */
  zSql = sqlite3_mprintf("ATTACH %Q AS log", argv[2]);
  rc = sqlite3_exec(db, zSql, 0, 0, 0);
  sqlite3_free(zSql);
  if( rc ){
    printf("Cannot open the LOG database \"%s\" - %s\n",
           argv[2], sqlite3_errmsg(db));
    goto errorOut;
  }
  rc = sqlite3_prepare_v2(db, "SELECT sql, rowid FROM log.sqllog",
                          -1, &pStmt, 0);
  if( rc ){
    printf("Cannot read the SQLLOG table in the LOG database \"%s\" - %s\n",
           argv[2], sqlite3_errmsg(db));
    goto errorOut;
  }

  /* Update the counts based on LOG */
  while( sqlite3_step(pStmt)==SQLITE_ROW ){
    const char *zLog = (const char*)sqlite3_column_text(pStmt, 0);
    sqlite3_stmt *pS2;
    if( zLog==0 ) continue;
    zSql = sqlite3_mprintf("EXPLAIN QUERY PLAN %s", zLog);
    rc = sqlite3_prepare_v2(db, zSql, -1, &pS2, 0);
    sqlite3_free(zSql);
    if( rc ){
      printf("Cannot compile LOG entry %d (%s): %s\n",
             sqlite3_column_int(pStmt, 1), zLog, sqlite3_errmsg(db));
      nErr++;
    }else{
      while( sqlite3_step(pS2)==SQLITE_ROW ){
        const char *zExplain = (const char*)sqlite3_column_text(pS2,3);
        const char *z1, *z2;
        int n;
        /* printf("EXPLAIN: %s\n", zExplain); */
        z1 = strstr(zExplain, " USING INDEX ");
        if( z1==0 ) continue;
        z1 += 13;
        for(z2=z1+1; z2[1] && z2[1]!='('; z2++){}
        n = z2 - z1;
        zSql = sqlite3_mprintf(
          "UPDATE temp.idxu SET cnt=cnt+1 WHERE idx='%.*q'", n, z1
        );
        /* printf("sql: %s\n", zSql); */
        sqlite3_exec(db, zSql, 0, 0, 0);
        sqlite3_free(zSql);
      }
    }
    sqlite3_finalize(pS2);
  }
  sqlite3_finalize(pStmt);

  /* Generate the report */
  rc = sqlite3_prepare_v2(db,
     "SELECT tbl, idx, cnt, "
     "   (SELECT group_concat(name,',') FROM pragma_index_info(idx))"
     " FROM temp.idxu, main.sqlite_master"
     " WHERE temp.idxu.tbl=main.sqlite_master.tbl_name"
     "   AND temp.idxu.idx=main.sqlite_master.name"
     " ORDER BY cnt DESC, tbl, idx",
     -1, &pStmt, 0);
  if( rc ){
    printf("Cannot query the result table - %s\n",
           sqlite3_errmsg(db));
    goto errorOut;
  }
  while( sqlite3_step(pStmt)==SQLITE_ROW ){
    printf("%10d %s on %s(%s)\n", 
       sqlite3_column_int(pStmt, 2),
       sqlite3_column_text(pStmt, 1),
       sqlite3_column_text(pStmt, 0),
       sqlite3_column_text(pStmt, 3));
  }
  sqlite3_finalize(pStmt);
  pStmt = 0;

errorOut:
  sqlite3_finalize(pStmt);
  sqlite3_close(db);
  return nErr;
}

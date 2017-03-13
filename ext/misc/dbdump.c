/*
** 2016-03-13
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
******************************************************************************
**
** This file implements a C-language subroutine that converts the content
** of an SQLite database into UTF-8 text SQL statements that can be used
** to exactly recreate the original database.
**
** A prototype of the implemented subroutine is this:
**
**   int sqlite3_db_dump(
**          sqlite3 *db,
**          const char *zSchema,
**          const char *zTable,
**          void (*xCallback)(void*, const char*),
**          void *pArg
**   );
**
** The db parameter is the database connection.  zSchema is the schema within
** that database which is to be dumped.  Usually the zSchema is "main" but
** can also be "temp" or any ATTACH-ed database.  If zTable is not NULL, then
** only the content of that one table is dumped.  If zTable is NULL, then all
** tables are dumped.
**
** The generate text is passed to xCallback() in multiple calls.  The second
** argument to xCallback() is a copy of the pArg parameter.  The first
** argument is some of the output text that this routine generates.  The
** signature to xCallback() is designed to make it compatible with fputs().
**
** The sqlite3_db_dump() subroutine returns SQLITE_OK on success or some error
** code if it encounters a problem.
**
** If this file is compiled with -DDBDUMP_STANDALONE then a "main()" routine
** is included so that this routine becomes a command-line utility.  The
** command-line utility takes two or three arguments which are the name
** of the database file, the schema, and optionally the table, forming the
** first three arguments of a single call to the library routine.
*/
#include "sqlite3.h"

/*
** Convert an SQLite database into SQL statements that will recreate that
** database.
*/
int sqlite3_db_dump(
  sqlite3 *db,               /* The database connection */
  const char *zSchema,       /* Which schema to dump.  Usually "main". */
  const char *zTable,        /* Which table to dump.  NULL means everything. */
  int (*xCallback)(const char*,void*),   /* Output sent to this callback */
  void *pArg                             /* Second argument of the callback */
){
  return SQLITE_OK;
}



/* The generic subroutine is above.  The code the follows implements
** the command-line interface.
*/
#ifdef DBDUMP_STANDALONE
#include <stdio.h>

/*
** Command-line interface
*/
int main(int argc, char **argv){
  sqlite3 *db;
  const char *zDb;
  const char *zSchema;
  const char *zTable = 0;
  int rc;

  if( argc<2 || argc>4 ){
    fprintf(stderr, "Usage: %s DATABASE ?SCHEMA? ?TABLE?\n", argv[0]);
    return 1;
  }
  zDb = argv[1];
  zSchema = argc>=3 ? argv[2] : "main";
  zTable = argc==4 ? argv[3] : 0;

  rc = sqlite3_open(zDb, &db);
  if( rc ){
    fprintf(stderr, "Cannot open \"%s\": %s\n", zDb, sqlite3_errmsg(db));
    sqlite3_close(db);
    return 1;
  }
  rc = sqlite3_db_dump(db, zSchema, zTable, 
          (int(*)(const char*,void*))fputs, (void*)stdout);
  if( rc ){
    fprintf(stderr, "Error: sqlite3_db_dump() returns %d\n", rc);
  }
  sqlite3_close(db);
  return rc!=SQLITE_OK;  
}
#endif /* DBDUMP_STANDALONE */

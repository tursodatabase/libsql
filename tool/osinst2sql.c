/*
******************************************************************************
*/

#include <sqlite3.h>

#include <assert.h>
#include <stdio.h>

int sqlite3_vfslog_register(sqlite3 *db);
 
static int xCallback(void *pUnused, int nArg, char **azArg, char **azName){
  char *zPrint = 0;

  assert( nArg==7 );
  zPrint = sqlite3_mprintf(
      "INSERT INTO osinst VALUES(%Q, %Q, %s, %s, %s, %s, %s);",
      azArg[0], azArg[1], azArg[2], azArg[3], azArg[4], azArg[5], azArg[6]
  );
  printf("%s\n", zPrint);
  sqlite3_free(zPrint);
  return SQLITE_OK;
}

int main(int argc, char **argv){
  sqlite3 *db = 0;
  int i;

  sqlite3_open("", &db);
  sqlite3_vfslog_register(db);

  printf("BEGIN;\n");
  printf("CREATE TABLE IF NOT EXISTS osinst(\n");
  printf("    event    TEXT,      -- xOpen, xRead etc.\n");
  printf("    file     TEXT,      -- Name of file this call applies to\n");
  printf("    time     INTEGER,   -- Timestamp\n");
  printf("    clicks   INTEGER,   -- Time spent in call\n");
  printf("    rc       INTEGER,   -- Return value\n");
  printf("    size     INTEGER,   -- Bytes read or written\n");
  printf("    offset   INTEGER    -- File offset read or written\n");
  printf(");\n");

  for(i=1; i<argc; i++){
    char *zSql = 0;
    sqlite3_exec(db, "DROP TABLE IF EXISTS osinst;", 0, 0, 0);
    zSql = sqlite3_mprintf(
        "CREATE VIRTUAL TABLE osinst USING vfslog(%Q)", argv[i]
    );
    sqlite3_exec(db, zSql, 0, 0, 0);
    sqlite3_free(zSql);

    sqlite3_exec(db, "SELECT * FROM osinst", xCallback, 0, 0);
  }

  printf("COMMIT;\n");
  sqlite3_close(db);
  return 0;
}

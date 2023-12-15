#include <assert.h>
#include <stdio.h>

#include "crsqlite.h"
#include "rust.h"

int crsql_close(sqlite3 *db);
int syncLeftToRight(sqlite3 *db1, sqlite3 *db2, sqlite3_int64 since);

static void testSandbox() {
  printf("Sandbox\n");
  sqlite3 *db1;
  sqlite3 *db2;
  int rc;
  rc = sqlite3_open(":memory:", &db1);
  rc += sqlite3_open(":memory:", &db2);

  rc +=
      sqlite3_exec(db1, "CREATE TABLE foo (a primary key not null);", 0, 0, 0);
  rc +=
      sqlite3_exec(db2, "CREATE TABLE foo (a primary key not null);", 0, 0, 0);
  rc += sqlite3_exec(db1, "SELECT crsql_as_crr('foo')", 0, 0, 0);
  rc += sqlite3_exec(db2, "SELECT crsql_as_crr('foo')", 0, 0, 0);
  rc += sqlite3_exec(db1, "INSERT INTO foo VALUES (1)", 0, 0, 0);
  assert(rc == SQLITE_OK);

  assert(syncLeftToRight(db1, db2, 0) == SQLITE_OK);

  crsql_close(db1);
  crsql_close(db2);
  printf("\t\e[0;32mSuccess\e[0m\n");
}

void crsqlSandboxSuite() {
  testSandbox();
  printf("\e[47m\e[1;30mSuite: sandbox\e[0m\n");
}

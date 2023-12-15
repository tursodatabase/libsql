#include <assert.h>
#include <stdio.h>

#include "crsqlite.h"
#include "rust.h"

int crsql_close(sqlite3 *db);

static void testTableIsNotCrr() {
  printf("TableIsNotCrr\n");
  sqlite3 *db;
  int rc;
  rc = sqlite3_open(":memory:", &db);

  rc =
      sqlite3_exec(db, "CREATE TABLE foo (a PRIMARY KEY NOT NULL, b)", 0, 0, 0);
  assert(rc == SQLITE_OK);
  assert(crsql_is_crr(db, "foo") == 0);

  crsql_close(db);
  printf("\t\e[0;32mSuccess\e[0m\n");
}

static void testCrrIsCrr() {
  printf("CrrIsCrr\n");
  sqlite3 *db;
  int rc;
  rc = sqlite3_open(":memory:", &db);

  rc =
      sqlite3_exec(db, "CREATE TABLE foo (a PRIMARY KEY NOT NULL, b)", 0, 0, 0);
  assert(rc == SQLITE_OK);
  rc = sqlite3_exec(db, "SELECT crsql_as_crr('foo')", 0, 0, 0);
  assert(rc == SQLITE_OK);

  assert(crsql_is_crr(db, "foo") == 1);

  crsql_close(db);
  printf("\t\e[0;32mSuccess\e[0m\n");
}

static void testDestroyedCrrIsNotCrr() {
  printf("DestroyedCrrIsNotCrr\n");
  sqlite3 *db;
  int rc;
  rc = sqlite3_open(":memory:", &db);

  rc =
      sqlite3_exec(db, "CREATE TABLE foo (a PRIMARY KEY NOT NULL, b)", 0, 0, 0);
  assert(rc == SQLITE_OK);
  rc = sqlite3_exec(db, "SELECT crsql_as_crr('foo')", 0, 0, 0);
  assert(rc == SQLITE_OK);
  rc = sqlite3_exec(db, "SELECT crsql_as_table('foo')", 0, 0, 0);
  assert(rc == SQLITE_OK);
  assert(crsql_is_crr(db, "foo") == 0);

  crsql_close(db);
  printf("\t\e[0;32mSuccess\e[0m\n");
}

void crsqlIsCrrTestSuite() {
  printf("\e[47m\e[1;30mSuite: is_crr\e[0m\n");

  testTableIsNotCrr();
  testCrrIsCrr();
  testDestroyedCrrIsNotCrr();
}

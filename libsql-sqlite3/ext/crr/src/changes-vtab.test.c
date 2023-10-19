#include "changes-vtab.h"

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "consts.h"
#include "crsqlite.h"

int crsql_close(sqlite3 *db);

static void testManyPkTable() {
  printf("ManyPkTable\n");

  sqlite3 *db;
  sqlite3_stmt *pStmt;
  int rc;
  rc = sqlite3_open(":memory:", &db);

  rc = sqlite3_exec(db, "CREATE TABLE foo (a, b, c, primary key (a, b));", 0, 0,
                    0);
  rc += sqlite3_exec(db, "SELECT crsql_as_crr('foo');", 0, 0, 0);
  assert(rc == SQLITE_OK);
  rc += sqlite3_exec(db, "INSERT INTO foo VALUES (4,5,6);", 0, 0, 0);
  assert(rc == SQLITE_OK);

  rc += sqlite3_prepare_v2(db, "SELECT [table], quote(pk) FROM crsql_changes",
                           -1, &pStmt, 0);
  assert(rc == SQLITE_OK);

  while (sqlite3_step(pStmt) == SQLITE_ROW) {
    const unsigned char *pk = sqlite3_column_text(pStmt, 1);
    // pk: 4, 5
    // X'0209040905'
    // 02 -> columns
    // 09 -> 1 byte integer
    // 04 -> 4
    // 09 -> 1 byte integer
    // 05 -> 5
    assert(strcmp("X'0209040905'", (char *)pk) == 0);
  }

  sqlite3_finalize(pStmt);
  crsql_close(db);
  printf("\t\e[0;32mSuccess\e[0m\n");
}

static void assertCount(sqlite3 *db, const char *sql, int expected) {
  sqlite3_stmt *pStmt;
  int rc = sqlite3_prepare_v2(db, sql, -1, &pStmt, 0);
  assert(rc == SQLITE_OK);
  assert(sqlite3_step(pStmt) == SQLITE_ROW);
  printf("expected: %d, actual: %d\n", expected, sqlite3_column_int(pStmt, 0));
  assert(sqlite3_column_int(pStmt, 0) == expected);
  sqlite3_finalize(pStmt);
}

static void testFilters() {
  printf("Filters\n");

  sqlite3 *db;
  int rc;
  rc = sqlite3_open(":memory:", &db);

  rc = sqlite3_exec(db, "CREATE TABLE foo (a primary key, b);", 0, 0, 0);
  rc += sqlite3_exec(db, "SELECT crsql_as_crr('foo');", 0, 0, 0);
  assert(rc == SQLITE_OK);
  rc += sqlite3_exec(db, "INSERT INTO foo VALUES (1,2);", 0, 0, 0);
  rc += sqlite3_exec(db, "INSERT INTO foo VALUES (2,3);", 0, 0, 0);
  rc += sqlite3_exec(db, "INSERT INTO foo VALUES (3,4);", 0, 0, 0);
  assert(rc == SQLITE_OK);

  printf("no filters\n");
  // 6 - 1 for each row creation, 1 for each b
  assertCount(db, "SELECT count(*) FROM crsql_changes", 3);

  // now test:
  // 1. site_id comparison
  // 2. db_version comparison

  printf("is null\n");
  assertCount(db, "SELECT count(*) FROM crsql_changes WHERE site_id IS NULL",
              3);

  printf("is not null\n");
  assertCount(
      db, "SELECT count(*) FROM crsql_changes WHERE site_id IS NOT NULL", 0);

  printf("equals\n");
  assertCount(
      db, "SELECT count(*) FROM crsql_changes WHERE site_id = crsql_site_id()",
      0);

  // 0 rows is actually correct ANSI sql behavior. NULLs are never equal, or not
  // equal, to anything in ANSI SQL. So users must use `IS NOT` to check rather
  // than !=.
  //
  // https://stackoverflow.com/questions/60017275/why-null-is-not-equal-to-anything-is-a-false-statement
  printf("not equals\n");
  assertCount(
      db, "SELECT count(*) FROM crsql_changes WHERE site_id != crsql_site_id()",
      0);

  printf("is not\n");
  // All rows are currently null for site_id
  assertCount(
      db,
      "SELECT count(*) FROM crsql_changes WHERE site_id IS NOT crsql_site_id()",
      3);

  // compare on db_version _and_ site_id

  // compare upper and lower bound on db_version
  printf("double bounded version\n");
  assertCount(db,
              "SELECT count(*) FROM crsql_changes WHERE db_version >= 1 AND "
              "db_version < 2",
              1);

  printf("OR condition\n");
  assertCount(db,
              "SELECT count(*) FROM crsql_changes WHERE db_version > 2 OR "
              "site_id IS NULL",
              3);

  // compare on pks, table name, other not perfectly supported columns

  crsql_close(db);
  printf("\t\e[0;32mSuccess\e[0m\n");
}

// test value extraction under all filter conditions

// static void testSinglePksTable()
// {
// }

// static void testOnlyPkTable()
// {
// }

// static void testSciNotation()
// {
// }

// static void testHex()
// {
// }

void crsqlChangesVtabTestSuite() {
  printf("\e[47m\e[1;30mSuite: crsql_changesVtab\e[0m\n");
  testManyPkTable();
  testFilters();
}

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

  rc += sqlite3_prepare_v2(db, "SELECT * FROM crsql_changes()", -1, &pStmt, 0);
  assert(rc == SQLITE_OK);

  while (sqlite3_step(pStmt) == SQLITE_ROW) {
    const unsigned char *pk = sqlite3_column_text(pStmt, 1);
    assert(strcmp("4|5", (char *)pk) == 0);
  }

  sqlite3_finalize(pStmt);
  crsql_close(db);
  printf("\t\e[0;32mSuccess\e[0m\n");
}

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
}

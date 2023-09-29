#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "consts.h"
#include "crsqlite.h"
#include "rust.h"
#include "tableinfo.h"
#include "util.h"

int crsql_close(sqlite3 *db);

// This would be more testable if we could test
// query construction rather than actual table creation.
// testing actual table creation requires views and base crr to
// be in place.
static void testCreateTriggers() {
  printf("CreateTriggers\n");

  sqlite3 *db = 0;
  crsql_TableInfo *tableInfo;
  char *errMsg = 0;
  int rc = sqlite3_open(":memory:", &db);

  rc =
      sqlite3_exec(db, "CREATE TABLE \"foo\" (\"a\" PRIMARY KEY, \"b\", \"c\")",
                   0, 0, &errMsg);
  rc = crsql_getTableInfo(db, "foo", &tableInfo, &errMsg);

  if (rc == SQLITE_OK) {
    rc = crsql_create_crr_triggers(db, tableInfo, &errMsg);
  }

  crsql_freeTableInfo(tableInfo);
  if (rc != SQLITE_OK) {
    crsql_close(db);
    printf("err: %s | rc: %d\n", errMsg, rc);
    sqlite3_free(errMsg);
    assert(0);
  }

  sqlite3_free(errMsg);
  crsql_close(db);

  printf("\t\e[0;32mSuccess\e[0m\n");
}

void crsqlTriggersTestSuite() {
  printf("\e[47m\e[1;30mSuite: crsqlTriggers\e[0m\n");

  testCreateTriggers();
  // testTriggerSyncBitInteraction(); <-- implemented in rust tests
}
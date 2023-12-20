#include <stdio.h>
#include <string.h>

#include "sqlite3ext.h"
SQLITE_EXTENSION_INIT3

#define SUITE(N) if (strcmp(suite, "all") == 0 || strcmp(suite, N) == 0)

int crsql_close(sqlite3 *db) {
  int rc = SQLITE_OK;
  rc += sqlite3_exec(db, "SELECT crsql_finalize()", 0, 0, 0);
  rc += sqlite3_close(db);
  return rc;
}

// void crsqlTableInfoTestSuite();
void crsqlTestSuite();
// void crsqlTriggersTestSuite();
// void crsqlChangesVtabReadTestSuite();
void crsqlChangesVtabTestSuite();
void crsqlChangesVtabCommonTestSuite();
void crsqlExtDataTestSuite();
void crsqlFractSuite();
void crsqlIsCrrTestSuite();
void rowsImpactedTestSuite();
void crsqlChangesVtabRowidTestSuite();
void crsqlSandboxSuite();
void crsql_integration_check();

int main(int argc, char *argv[]) {
  char *suite = "all";
  if (argc == 2) {
    suite = argv[1];
  }

  SUITE("vtab") crsqlChangesVtabTestSuite();
  SUITE("extdata") crsqlExtDataTestSuite();
  // integration tests should come at the end given fixing unit tests will
  // likely fix integration tests
  SUITE("crsql") crsqlTestSuite();
  SUITE("fract") crsqlFractSuite();
  SUITE("is_crr") crsqlIsCrrTestSuite();
  SUITE("rows_impacted") rowsImpactedTestSuite();
  SUITE("rowid") crsqlChangesVtabRowidTestSuite();
  SUITE("sandbox") crsqlSandboxSuite();
  SUITE("rust_integration") crsql_integration_check();

  sqlite3_shutdown();
}

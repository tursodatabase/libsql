#include "changes-vtab-write.h"

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "consts.h"
#include "crsqlite.h"

int crsql_close(sqlite3 *db);

// static void memTestMergeInsert()
// {
//   // test delete case
//   // test nothing to merge case
//   // test normal merge
//   // test error / early returns
// }

// static void testMergeInsert()
// {
// }

// static void testChangesTabConflictSets()
// {
// }

// static void testDidCidWin()
// {
//   printf("AllChangedCids\n");

//   int rc = SQLITE_OK;
//   sqlite3 *db;
//   rc = sqlite3_open(":memory:", &db);
//   char *err = 0;

//   // test
//   // crsql_allChangedCids(
//   //   db,
//   //   "",
//   //   "",
//   //   "",

//   // );

//   printf("\t\e[0;32mSuccess\e[0m\n");
// }

void crsqlChangesVtabWriteTestSuite() {
  printf("\e[47m\e[1;30mSuite: crsql_changesVtabWrite\e[0m\n");

  // TODO: most vtab write cases are covered in `crsqlite.test.c`
  // we should, however, create tests that are narrower in scope here.

  // testDidCidWin();
}
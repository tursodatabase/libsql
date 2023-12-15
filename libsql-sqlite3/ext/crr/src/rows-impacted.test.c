#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "crsqlite.h"

int crsql_close(sqlite3 *db);

static sqlite3 *createDb() {
  int rc = SQLITE_OK;
  sqlite3 *db;
  rc = sqlite3_open(":memory:", &db);
  rc +=
      sqlite3_exec(db, "CREATE TABLE foo (a primary key not null, b)", 0, 0, 0);
  rc += sqlite3_exec(db, "SELECT crsql_as_crr('foo')", 0, 0, 0);
  assert(rc == SQLITE_OK);
  return db;
}

static void testSingleInsertSingleTx() {
  printf("SingleInsertSingleTx\n");
  int rc = SQLITE_OK;
  char *err = 0;
  sqlite3 *db = createDb();
  sqlite3_stmt *pStmt = 0;

  rc = sqlite3_exec(db, "BEGIN", 0, 0, 0);
  rc += sqlite3_exec(db,
                     "INSERT INTO crsql_changes VALUES ('foo', X'010901', 'b', "
                     "2, 1, 1, NULL, 1, 1)",
                     0, 0, &err);
  sqlite3_prepare_v2(db, "SELECT crsql_rows_impacted()", -1, &pStmt, 0);
  sqlite3_step(pStmt);
  // creation + setting of column
  assert(sqlite3_column_int(pStmt, 0) == 1);
  sqlite3_finalize(pStmt);
  rc += sqlite3_exec(db, "COMMIT", 0, 0, 0);
  assert(rc == SQLITE_OK);

  // rows impacted gets reset
  sqlite3_prepare_v2(db, "SELECT crsql_rows_impacted()", -1, &pStmt, 0);
  sqlite3_step(pStmt);
  int impacted = sqlite3_column_int(pStmt, 0);
  assert(impacted == 0);
  sqlite3_finalize(pStmt);

  crsql_close(db);
  printf("\t\e[0;32mSuccess\e[0m\n");
}

static void testManyInsertsInATx() {
  printf("ManyInsertsInATx\n");
  int rc = SQLITE_OK;
  char *err = 0;
  sqlite3 *db = createDb();
  sqlite3_stmt *pStmt = 0;

  rc = sqlite3_exec(db, "BEGIN", 0, 0, 0);
  rc += sqlite3_exec(db,
                     "INSERT INTO crsql_changes VALUES ('foo', X'010901', 'b', "
                     "2, 1, 1, NULL, 1, 1)",
                     0, 0, &err);
  rc += sqlite3_exec(db,
                     "INSERT INTO crsql_changes VALUES ('foo', X'010902', 'b', "
                     "2, 1, 1, NULL, 1, 1)",
                     0, 0, &err);
  rc += sqlite3_exec(db,
                     "INSERT INTO crsql_changes VALUES ('foo', X'010903', 'b', "
                     "2, 1, 1, NULL, 1, 1)",
                     0, 0, &err);
  sqlite3_prepare_v2(db, "SELECT crsql_rows_impacted()", -1, &pStmt, 0);
  sqlite3_step(pStmt);
  assert(sqlite3_column_int(pStmt, 0) == 3);
  sqlite3_finalize(pStmt);
  rc += sqlite3_exec(db, "COMMIT", 0, 0, 0);
  assert(rc == SQLITE_OK);

  // rows impacted gets reset
  sqlite3_prepare_v2(db, "SELECT crsql_rows_impacted()", -1, &pStmt, 0);
  sqlite3_step(pStmt);
  assert(sqlite3_column_int(pStmt, 0) == 0);
  sqlite3_finalize(pStmt);

  crsql_close(db);
  printf("\t\e[0;32mSuccess\e[0m\n");
}

static void testMultipartInsertInTx() {
  printf("MultipartInsertInTx\n");
  int rc = SQLITE_OK;
  char *err = 0;
  sqlite3 *db = createDb();
  sqlite3_stmt *pStmt = 0;

  rc = sqlite3_exec(db, "BEGIN", 0, 0, 0);
  rc += sqlite3_exec(db,
                     "INSERT INTO crsql_changes VALUES ('foo', X'010901', 'b', "
                     "2, 1, 1, NULL, 1, 1), "
                     "('foo', X'010902', 'b', 2, 1, 1, NULL, 1, 1), ('foo', "
                     "X'010903', 'b', 2, 1, 1, NULL, 1, 1)",
                     0, 0, &err);
  sqlite3_prepare_v2(db, "SELECT crsql_rows_impacted()", -1, &pStmt, 0);
  sqlite3_step(pStmt);
  assert(sqlite3_column_int(pStmt, 0) == 3);
  sqlite3_finalize(pStmt);
  rc += sqlite3_exec(db, "COMMIT", 0, 0, 0);
  assert(rc == SQLITE_OK);

  // rows impacted gets reset
  sqlite3_prepare_v2(db, "SELECT crsql_rows_impacted()", -1, &pStmt, 0);
  sqlite3_step(pStmt);
  assert(sqlite3_column_int(pStmt, 0) == 0);
  sqlite3_finalize(pStmt);

  crsql_close(db);
  printf("\t\e[0;32mSuccess\e[0m\n");
}

// count should reset between transactions
static void testManyTxns() {
  printf("ManyTxns\n");
  int rc = SQLITE_OK;
  char *err = 0;
  sqlite3 *db = createDb();
  sqlite3_stmt *pStmt = 0;

  rc = sqlite3_exec(db, "BEGIN", 0, 0, 0);
  rc += sqlite3_exec(db,
                     "INSERT INTO crsql_changes VALUES ('foo', X'010901', 'b', "
                     "2, 1, 1, NULL, 1, 1)",
                     0, 0, &err);
  sqlite3_prepare_v2(db, "SELECT crsql_rows_impacted()", -1, &pStmt, 0);
  sqlite3_step(pStmt);
  assert(sqlite3_column_int(pStmt, 0) == 1);
  sqlite3_finalize(pStmt);
  rc += sqlite3_exec(db, "COMMIT", 0, 0, 0);
  assert(rc == SQLITE_OK);

  rc = sqlite3_exec(db, "BEGIN", 0, 0, 0);
  rc += sqlite3_exec(db,
                     "INSERT INTO crsql_changes VALUES ('foo', X'010902', 'b', "
                     "2, 1, 1, NULL, 1, 1)",
                     0, 0, &err);
  rc += sqlite3_exec(db,
                     "INSERT INTO crsql_changes VALUES ('foo', X'010903', 'b', "
                     "2, 1, 1, NULL, 1, 1)",
                     0, 0, &err);
  sqlite3_prepare_v2(db, "SELECT crsql_rows_impacted()", -1, &pStmt, 0);
  sqlite3_step(pStmt);
  int impacted = sqlite3_column_int(pStmt, 0);
  assert(impacted == 2);
  sqlite3_finalize(pStmt);
  rc += sqlite3_exec(db, "COMMIT", 0, 0, 0);
  assert(rc == SQLITE_OK);

  crsql_close(db);
  printf("\t\e[0;32mSuccess\e[0m\n");
}

// You can't do this. `crsql_rows_impacted` is evaulated before the insert is
// run thus the right value can never be returned by `RETURNING` in this setup.
// static void testReturningInTx() {
//   printf("RetruningInTx\n");
//   int rc = SQLITE_OK;
//   char *err = 0;
//   sqlite3 *db = createDb();
//   sqlite3_stmt *pStmt = 0;

//   rc = sqlite3_exec(db, "BEGIN", 0, 0, 0);
//   rc += sqlite3_prepare_v2(
//       db,
//       "INSERT INTO crsql_changes VALUES ('foo', 1, 'b', 2, 1, 1, NULL), "
//       "('foo', 2, 'b', 2, 1, 1, NULL), ('foo', 3, 'b', 2, 1, 1, NULL) "
//       "RETURNING crsql_rows_impacted()",
//       -1, &pStmt, 0);
//   rc += sqlite3_step(pStmt);
//   int impacted = sqlite3_column_int(pStmt, 0);
//   printf("impacted: %d\n", impacted);
//   assert(impacted == 3);
//   sqlite3_finalize(pStmt);
//   rc += sqlite3_exec(db, "COMMIT", 0, 0, 0);
//   assert(rc == SQLITE_OK);

//   crsql_close(db);
//   printf("\t\e[0;32mSuccess\e[0m\n");
// }

static void testUpdateThatDoesNotChangeAnything() {
  printf("UpdateThatDoesNotChangeAnything\n");
  int rc = SQLITE_OK;
  char *err = 0;
  sqlite3 *db = createDb();
  sqlite3_stmt *pStmt = 0;

  rc = sqlite3_exec(db, "INSERT INTO foo VALUES (1, 2)", 0, 0, 0);

  rc += sqlite3_exec(db, "BEGIN", 0, 0, 0);
  rc += sqlite3_exec(db,
                     "INSERT INTO crsql_changes VALUES ('foo', "
                     "crsql_pack_columns(1), 'b', 2, 1, 1, NULL, 1, 1)",
                     0, 0, &err);
  sqlite3_prepare_v2(db, "SELECT crsql_rows_impacted()", -1, &pStmt, 0);
  sqlite3_step(pStmt);
  assert(sqlite3_column_int(pStmt, 0) == 0);
  sqlite3_finalize(pStmt);
  rc += sqlite3_exec(db, "COMMIT", 0, 0, 0);
  assert(rc == SQLITE_OK);

  // now test value <
  rc += sqlite3_exec(db, "BEGIN", 0, 0, 0);
  rc += sqlite3_exec(db,
                     "INSERT INTO crsql_changes VALUES ('foo', "
                     "crsql_pack_columns(1), 'b', 0, 1, 1, NULL, 1, 1)",
                     0, 0, &err);
  sqlite3_prepare_v2(db, "SELECT crsql_rows_impacted()", -1, &pStmt, 0);
  sqlite3_step(pStmt);
  assert(sqlite3_column_int(pStmt, 0) == 0);
  sqlite3_finalize(pStmt);
  rc += sqlite3_exec(db, "COMMIT", 0, 0, 0);
  assert(rc == SQLITE_OK);

  // now test clock <
  rc += sqlite3_exec(db, "BEGIN", 0, 0, 0);
  rc += sqlite3_exec(db,
                     "INSERT INTO crsql_changes VALUES ('foo', "
                     "crsql_pack_columns(1), 'b', 2, 0, 0, NULL, 1, 1)",
                     0, 0, &err);
  sqlite3_prepare_v2(db, "SELECT crsql_rows_impacted()", -1, &pStmt, 0);
  sqlite3_step(pStmt);
  assert(sqlite3_column_int(pStmt, 0) == 0);
  sqlite3_finalize(pStmt);
  rc += sqlite3_exec(db, "COMMIT", 0, 0, 0);
  assert(rc == SQLITE_OK);

  crsql_close(db);
  printf("\t\e[0;32mSuccess\e[0m\n");
}

static void testDeleteThatDoesNotChangeAnything() {
  printf("DeleteThatDoesNotChangeAnything\n");
  int rc = SQLITE_OK;
  char *err = 0;
  sqlite3 *db = createDb();
  sqlite3_stmt *pStmt = 0;

  rc = sqlite3_exec(db, "INSERT INTO foo VALUES (1, 2)", 0, 0, 0);
  rc = sqlite3_exec(db, "DELETE FROM foo", 0, 0, 0);

  rc += sqlite3_exec(db, "BEGIN", 0, 0, 0);
  rc += sqlite3_exec(
      db,
      "INSERT INTO crsql_changes VALUES ('foo', crsql_pack_columns(1), "
      "'-1', NULL, 2, 2, NULL, 1, 1)",  //__crsql_del
      0, 0, &err);
  sqlite3_prepare_v2(db, "SELECT crsql_rows_impacted()", -1, &pStmt, 0);
  sqlite3_step(pStmt);
  assert(sqlite3_column_int(pStmt, 0) == 0);
  sqlite3_finalize(pStmt);
  rc += sqlite3_exec(db, "COMMIT", 0, 0, 0);
  assert(rc == SQLITE_OK);

  crsql_close(db);
  printf("\t\e[0;32mSuccess\e[0m\n");
}

static void testDelete() {
  printf("Delete\n");
  int rc = SQLITE_OK;
  char *err = 0;
  sqlite3 *db = createDb();
  sqlite3_stmt *pStmt = 0;

  rc = sqlite3_exec(db, "INSERT INTO foo VALUES (1, 2)", 0, 0, 0);

  rc += sqlite3_exec(db, "BEGIN", 0, 0, 0);
  rc += sqlite3_exec(db,
                     "INSERT INTO crsql_changes VALUES ('foo', X'010901', "
                     "'-1', NULL, 2, 2, NULL, 2, 1)",  //__crsql_del
                     0, 0, &err);
  sqlite3_prepare_v2(db, "SELECT crsql_rows_impacted()", -1, &pStmt, 0);
  sqlite3_step(pStmt);
  assert(sqlite3_column_int(pStmt, 0) == 1);
  sqlite3_finalize(pStmt);
  rc += sqlite3_exec(db, "COMMIT", 0, 0, 0);
  assert(rc == SQLITE_OK);

  crsql_close(db);
  printf("\t\e[0;32mSuccess\e[0m\n");
}

static void testCreateThatDoesNotChangeAnything() {
  printf("UpdateThatDoesNotChangeAnything\n");
  int rc = SQLITE_OK;
  char *err = 0;
  sqlite3 *db = createDb();
  sqlite3_stmt *pStmt = 0;

  rc = sqlite3_exec(db, "INSERT INTO foo VALUES (1, 2)", 0, 0, 0);

  rc += sqlite3_exec(db, "BEGIN", 0, 0, 0);
  rc += sqlite3_exec(db,
                     "INSERT INTO crsql_changes VALUES ('foo', X'010901', 'b', "
                     "2, 1, 1, NULL, 1, 1)",
                     0, 0, &err);
  sqlite3_prepare_v2(db, "SELECT crsql_rows_impacted()", -1, &pStmt, 0);
  sqlite3_step(pStmt);
  assert(sqlite3_column_int(pStmt, 0) == 0);
  sqlite3_finalize(pStmt);
  rc += sqlite3_exec(db, "COMMIT", 0, 0, 0);
  assert(rc == SQLITE_OK);

  crsql_close(db);
  printf("\t\e[0;32mSuccess\e[0m\n");
}

static void testValueWin() {
  printf("ValueWin\n");
  int rc = SQLITE_OK;
  char *err = 0;
  sqlite3 *db = createDb();
  sqlite3_stmt *pStmt = 0;

  rc = sqlite3_exec(db, "INSERT INTO foo VALUES (1, 2)", 0, 0, 0);

  rc = sqlite3_exec(db, "BEGIN", 0, 0, 0);
  rc += sqlite3_exec(db,
                     "INSERT INTO crsql_changes VALUES ('foo', X'010901', 'b', "
                     "3, 1, 1, X'00000000000000000000000000000000', 1, 1)",
                     0, 0, &err);
  sqlite3_prepare_v2(db, "SELECT crsql_rows_impacted()", -1, &pStmt, 0);
  sqlite3_step(pStmt);
  assert(sqlite3_column_int(pStmt, 0) == 1);
  sqlite3_finalize(pStmt);
  rc += sqlite3_exec(db, "COMMIT", 0, 0, 0);
  assert(rc == SQLITE_OK);

  crsql_close(db);
  printf("\t\e[0;32mSuccess\e[0m\n");
}

static void testClockWin() {
  printf("ClockWin\n");
  int rc = SQLITE_OK;
  char *err = 0;
  sqlite3 *db = createDb();
  sqlite3_stmt *pStmt = 0;

  rc = sqlite3_exec(db, "INSERT INTO foo VALUES (1, 2)", 0, 0, 0);

  rc = sqlite3_exec(db, "BEGIN", 0, 0, 0);
  rc += sqlite3_exec(db,
                     "INSERT INTO crsql_changes VALUES ('foo', X'010901', 'b', "
                     "2, 2, 2, NULL, 1, 1)",
                     0, 0, &err);
  sqlite3_prepare_v2(db, "SELECT crsql_rows_impacted()", -1, &pStmt, 0);
  sqlite3_step(pStmt);
  assert(sqlite3_column_int(pStmt, 0) == 1);
  sqlite3_finalize(pStmt);
  rc += sqlite3_exec(db, "COMMIT", 0, 0, 0);
  assert(rc == SQLITE_OK);

  crsql_close(db);
  printf("\t\e[0;32mSuccess\e[0m\n");
}

void rowsImpactedTestSuite() {
  printf("\e[47m\e[1;30mSuite: rows_impacted\e[0m\n");

  testSingleInsertSingleTx();
  testManyInsertsInATx();
  testMultipartInsertInTx();
  testManyTxns();
  testUpdateThatDoesNotChangeAnything();
  testDeleteThatDoesNotChangeAnything();
  testCreateThatDoesNotChangeAnything();
  testValueWin();
  testClockWin();
  testDelete();
}
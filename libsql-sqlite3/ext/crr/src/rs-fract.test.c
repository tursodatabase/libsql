#include <assert.h>
#include <stdio.h>
#include <string.h>

#include "crsqlite.h"
int crsql_close(sqlite3 *db);

static void testAsOrdered() {
  printf("AsOrdered\n");

  sqlite3 *db;
  int rc;

  rc = sqlite3_open(":memory:", &db);
  rc += sqlite3_exec(
      db,
      "CREATE TABLE todo (id primary key not null, list_id, ordering, "
      "content, complete);",
      0, 0, 0);
  rc += sqlite3_exec(
      db, "CREATE INDEX todo_list_id_ordering ON todo (list_id, ordering);", 0,
      0, 0);
  assert(rc == SQLITE_OK);

  // Test 1 list column
  rc += sqlite3_exec(
      db, "SELECT crsql_fract_as_ordered('todo', 'ordering', 'list_id')", 0, 0,
      0);
  assert(rc == SQLITE_OK);

  // Test idempotency
  rc += sqlite3_exec(
      db, "SELECT crsql_fract_as_ordered('todo', 'ordering', 'list_id')", 0, 0,
      0);
  assert(rc == SQLITE_OK);

  // test prepend
  rc += sqlite3_exec(db, "INSERT INTO todo VALUES (1, 1, -1, 'head', false)", 0,
                     0, 0);
  assert(rc == SQLITE_OK);
  sqlite3_stmt *pStmt;
  rc += sqlite3_prepare_v2(db, "SELECT ordering FROM todo WHERE id = 1", -1,
                           &pStmt, 0);
  assert(rc == SQLITE_OK);
  sqlite3_step(pStmt);
  const unsigned char *order = sqlite3_column_text(pStmt, 0);
  assert(strcmp((const char *)order, "a ") == 0);
  sqlite3_finalize(pStmt);

  // test append
  rc += sqlite3_exec(db, "INSERT INTO todo VALUES (3, 1, 1, 'tail', false)", 0,
                     0, 0);
  assert(rc == SQLITE_OK);
  rc += sqlite3_prepare_v2(db, "SELECT ordering FROM todo WHERE id = 3", -1,
                           &pStmt, 0);
  assert(rc == SQLITE_OK);
  sqlite3_step(pStmt);
  order = sqlite3_column_text(pStmt, 0);
  assert(strcmp((const char *)order, "a!") == 0);
  sqlite3_finalize(pStmt);

  // test insert after head
  sqlite3_exec(db,
               "INSERT INTO todo_fractindex (id, list_id, content, "
               "complete, after_id) VALUES (2, 1, 'mid', false, 1)",
               0, 0, 0);
  assert(rc == SQLITE_OK);
  rc += sqlite3_prepare_v2(
      db, "SELECT id, ordering FROM todo ORDER BY ordering ASC", -1, &pStmt, 0);
  assert(rc == SQLITE_OK);
  int i = 1;
  while (sqlite3_step(pStmt) == SQLITE_ROW) {
    assert(sqlite3_column_int(pStmt, 0) == i);
    if (i == 2) {
      assert(strcmp((const char *)sqlite3_column_text(pStmt, 1), "a P") == 0);
    }
    i += 1;
  }
  sqlite3_finalize(pStmt);

  // prepend a list with items via -1 trick
  rc = sqlite3_exec(db,
                    "INSERT INTO todo (id, list_id, content, "
                    "complete, ordering) VALUES (0, 1, 'mid', false, -1)",
                    0, 0, 0);
  assert(rc == SQLITE_OK);
  rc += sqlite3_prepare_v2(db, "SELECT ordering FROM todo WHERE id = 0", -1,
                           &pStmt, 0);
  assert(rc == SQLITE_OK);
  sqlite3_step(pStmt);
  order = sqlite3_column_text(pStmt, 0);
  assert(strcmp((const char *)order, "Z~") == 0);
  sqlite3_finalize(pStmt);

  // append to a list with items via 1 trick
  rc = sqlite3_exec(db,
                    "INSERT INTO todo (id, list_id, content, "
                    "complete, ordering) VALUES (4, 1, 'mid', false, 1)",
                    0, 0, 0);
  assert(rc == SQLITE_OK);
  rc += sqlite3_prepare_v2(db, "SELECT ordering FROM todo WHERE id = 4", -1,
                           &pStmt, 0);
  assert(rc == SQLITE_OK);
  sqlite3_step(pStmt);
  order = sqlite3_column_text(pStmt, 0);
  assert(strcmp((const char *)order, "a\"") == 0);
  sqlite3_finalize(pStmt);

  // before head via view and null
  sqlite3_exec(db,
               "INSERT INTO todo_fractindex (id, list_id, content, "
               "complete, after_id) VALUES (-1, 1, 'firstfirst', false, NULL)",
               0, 0, 0);
  assert(rc == SQLITE_OK);
  rc += sqlite3_prepare_v2(db, "SELECT ordering FROM todo WHERE id = -1", -1,
                           &pStmt, 0);
  assert(rc == SQLITE_OK);
  sqlite3_step(pStmt);
  order = sqlite3_column_text(pStmt, 0);
  assert(strcmp((const char *)order, "Z}") == 0);
  sqlite3_finalize(pStmt);

  // after tail view view
  sqlite3_exec(db,
               "INSERT INTO todo_fractindex (id, list_id, content, "
               "complete, after_id) VALUES (5, 1, 'lastlast', false, 4)",
               0, 0, 0);
  assert(rc == SQLITE_OK);
  rc += sqlite3_prepare_v2(db, "SELECT ordering FROM todo WHERE id = 5", -1,
                           &pStmt, 0);
  assert(rc == SQLITE_OK);
  sqlite3_step(pStmt);
  order = sqlite3_column_text(pStmt, 0);
  assert(strcmp((const char *)order, "a#") == 0);
  sqlite3_finalize(pStmt);

  // test move after
  sqlite3_exec(db, "UPDATE todo_fractindex SET after_id = 4 WHERE id = 3", 0, 0,
               0);
  assert(rc == SQLITE_OK);
  rc += sqlite3_prepare_v2(db, "SELECT ordering FROM todo WHERE id = 3", -1,
                           &pStmt, 0);
  assert(rc == SQLITE_OK);
  sqlite3_step(pStmt);
  order = sqlite3_column_text(pStmt, 0);
  assert(strcmp((const char *)order, "a\"P") == 0);
  sqlite3_finalize(pStmt);

  /*
  -1 -> Z}
  0 -> Z~
  1 -> a
  2 -> ?
  4 -> a"
  3 -> a"P
  5 -> a#
  */

  // insert between / insert after
  sqlite3_exec(db,
               "INSERT INTO todo_fractindex (id, list_id, content, "
               "complete, after_id) VALUES (2, 1, 'blark', false, 1)",
               0, 0, 0);
  assert(rc == SQLITE_OK);
  rc += sqlite3_prepare_v2(db, "SELECT ordering FROM todo WHERE id = 2", -1,
                           &pStmt, 0);
  assert(rc == SQLITE_OK);
  sqlite3_step(pStmt);
  order = sqlite3_column_text(pStmt, 0);
  assert(strcmp((const char *)order, "a P") == 0);
  sqlite3_finalize(pStmt);

  /*
  -1 -> Z}
  0 -> Z~
  1 -> a
  2 -> a P
  4 -> a"
  3 -> a"P
  5 -> a#
  */

  // move before
  // move 3 before 4
  sqlite3_exec(db, "UPDATE todo_fractindex SET after_id = 2 WHERE id = 3", 0, 0,
               0);
  assert(rc == SQLITE_OK);
  rc += sqlite3_prepare_v2(db, "SELECT ordering FROM todo WHERE id = 3", -1,
                           &pStmt, 0);
  assert(rc == SQLITE_OK);
  sqlite3_step(pStmt);
  order = sqlite3_column_text(pStmt, 0);
  assert(strcmp((const char *)order, "a!") == 0);
  sqlite3_finalize(pStmt);

  /*
  -1 -> Z}
  0 -> Z~
  1 -> a
  2 -> a P
  3 -> a!
  4 -> a"
  5 -> a#
  */

  // make some collisions
  rc = sqlite3_exec(
      db,
      "INSERT INTO todo (id, list_id, content, complete, ordering) "
      "VALUES (6, 1, 'xx', false, 'a!')",
      0, 0, 0);
  assert(rc == SQLITE_OK);
  // 3 & 6 collide, try insertion after 3
  // 3 should be moved down and the new insertion should get position between
  // 3's new position and old position
  sqlite3_exec(db,
               "INSERT INTO todo_fractindex (id, list_id, content, "
               "complete, after_id) VALUES (7, 1, 'xx', false, 3)",
               0, 0, 0);
  assert(rc == SQLITE_OK);
  rc += sqlite3_prepare_v2(db, "SELECT ordering FROM todo WHERE id = 7", -1,
                           &pStmt, 0);
  assert(rc == SQLITE_OK);
  sqlite3_step(pStmt);
  order = sqlite3_column_text(pStmt, 0);
  assert(strcmp((const char *)order, "a t") == 0);
  sqlite3_finalize(pStmt);

  rc += sqlite3_prepare_v2(db, "SELECT ordering FROM todo WHERE id = 3", -1,
                           &pStmt, 0);
  assert(rc == SQLITE_OK);
  sqlite3_step(pStmt);
  order = sqlite3_column_text(pStmt, 0);
  assert(strcmp((const char *)order, "a h") == 0);
  sqlite3_finalize(pStmt);

  rc += sqlite3_prepare_v2(db, "SELECT ordering FROM todo WHERE id = 6", -1,
                           &pStmt, 0);
  assert(rc == SQLITE_OK);
  sqlite3_step(pStmt);
  order = sqlite3_column_text(pStmt, 0);
  assert(strcmp((const char *)order, "a!") == 0);
  sqlite3_finalize(pStmt);

  // Test many list column

  // Schema change and re-run test

  // Test no list columns
  // rc += sqlite3_exec(db, "SELECT crsql_fract_as_ordered('todo', 'ordering')",
  // 0,
  //                    0, 0);
  // assert(rc == SQLITE_OK);

  // Test insert into other lists is independent

  printf("\t\e[0;32mSuccess\e[0m\n");
  crsql_close(db);
}

void crsqlFractSuite() {
  printf("\e[47m\e[1;30mSuite: fract\e[0m\n");

  testAsOrdered();
}

/*
sqlite3_prepare_v2(db,
                     "SELECT count(*) FROM todo WHERE list_id = 1 "
                     "AND ordering = (SELECT ordering FROM "
                     "todo WHERE id = 1)",
                     -1, &pStmt, 0);
  sqlite3_step(pStmt);
  int count = sqlite3_column_int(pStmt, 0);
  printf("Count: %d\n", count);
  sqlite3_finalize(pStmt);

  sqlite3_prepare_v2(db, "SELECT ordering FROM todo WHERE id = 1", -1, &pStmt,
                     0);
  sqlite3_step(pStmt);
  order = sqlite3_column_text(pStmt, 0);
  printf("Order 1: %s\n", order);
  sqlite3_finalize(pStmt);

  sqlite3_prepare_v2(db,
                     "SELECT ordering FROM todo WHERE list_id = 1 AND ordering "
                     "> (SELECT ordering FROM todo WHERE list_id = 1) LIMIT 1",
                     -1, &pStmt, 0);
  sqlite3_step(pStmt);
  order = sqlite3_column_text(pStmt, 0);
  printf("Order 2: %s\n", order);
  sqlite3_finalize(pStmt);

  sqlite3_prepare_v2(db, "SELECT crsql_fract_key_between('a0', 'a1')", -1,
                     &pStmt, 0);
  sqlite3_step(pStmt);
  order = sqlite3_column_text(pStmt, 0);
  printf("Order 1.5: %s\n", order);
  sqlite3_finalize(pStmt);
*/
#include "util.h"

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "consts.h"
#include "crsqlite.h"
#include "tableinfo.h"

#ifndef CHECK_OK
#define CHECK_OK         \
  if (rc != SQLITE_OK) { \
    goto fail;           \
  }
#endif

int crsql_close(sqlite3 *db);

static void testGetVersionUnionQuery() {
  int numRows_tc1 = 1;
  char *tableNames_tc1[] = {
      "tbl_name",
      "foo",
  };
  int numRows_tc2 = 3;
  char *tableNames_tc2[] = {"tbl_name", "foo", "bar", "baz"};
  char *query;
  printf("GetVersionUnionQuery\n");

  query = crsql_getDbVersionUnionQuery(numRows_tc1, tableNames_tc1);
  assert(strcmp(query,
                "SELECT max(version) as version FROM (SELECT "
                "max(__crsql_db_version) as version FROM \"foo\"  )") == 0);
  sqlite3_free(query);

  query = crsql_getDbVersionUnionQuery(numRows_tc2, tableNames_tc2);
  assert(strcmp(query,
                "SELECT max(version) as version FROM (SELECT "
                "max(__crsql_db_version) as version FROM \"foo\" UNION SELECT "
                "max(__crsql_db_version) as version FROM \"bar\" UNION SELECT "
                "max(__crsql_db_version) as version FROM \"baz\"  )") == 0);
  sqlite3_free(query);

  printf("\t\e[0;32mSuccess\e[0m\n");
}

static void testDoesTableExist() {
  sqlite3 *db;
  int rc;
  printf("DoesTableExist\n");

  rc = sqlite3_open(":memory:", &db);
  if (rc) {
    fprintf(stderr, "Can't open database: %s\n", sqlite3_errmsg(db));
    crsql_close(db);
    return;
  }

  assert(crsql_doesTableExist(db, "foo") == 0);
  sqlite3_exec(db, "CREATE TABLE foo (a, b)", 0, 0, 0);
  assert(crsql_doesTableExist(db, "foo") == 1);

  crsql_close(db);
  printf("\t\e[0;32mSuccess\e[0m\n");
}

static void testGetCount() {
  sqlite3 *db = 0;
  int rc = SQLITE_OK;
  printf("GetCount\n");

  rc = sqlite3_open(":memory:", &db);
  sqlite3_exec(db, "CREATE TABLE foo (a); INSERT INTO foo VALUES (1);", 0, 0,
               0);
  rc = crsql_getCount(db, "SELECT count(*) FROM foo");

  assert(rc == 1);
  sqlite3_exec(db, "INSERT INTO foo VALUES (1);", 0, 0, 0);
  rc = crsql_getCount(db, "SELECT count(*) FROM foo");
  assert(rc == 2);

  crsql_close(db);
  printf("\t\e[0;32mSuccess\e[0m\n");
}

static void testJoinWith() {
  printf("JoinWith\n");
  char dest[13];
  char *src[] = {"one", "two", "four"};

  crsql_joinWith(dest, src, 3, ',');

  assert(strcmp(dest, "one,two,four") == 0);
  printf("\t\e[0;32mSuccess\e[0m\n");
}

static void testGetIndexedCols() {
  printf("GetIndexedCols\n");

  sqlite3 *db = 0;
  int rc = SQLITE_OK;
  char **indexedCols = 0;
  int indexedColsLen;
  char *pErrMsg = 0;

  rc = sqlite3_open(":memory:", &db);
  sqlite3_exec(db, "CREATE TABLE foo (a);", 0, 0, 0);
  sqlite3_exec(db, "CREATE TABLE bar (a primary key);", 0, 0, 0);

  rc = crsql_getIndexedCols(db, "sqlite_autoindex_foo_1", &indexedCols,
                            &indexedColsLen, &pErrMsg);
  CHECK_OK

  assert(indexedColsLen == 0);
  assert(indexedCols == 0);

  rc = crsql_getIndexedCols(db, "sqlite_autoindex_bar_1", &indexedCols,
                            &indexedColsLen, &pErrMsg);
  CHECK_OK

  assert(indexedColsLen == 1);
  assert(strcmp(indexedCols[0], "a") == 0);

  sqlite3_free(indexedCols[0]);
  sqlite3_free(indexedCols);

  crsql_close(db);
  printf("\t\e[0;32mSuccess\e[0m\n");
  return;

fail:
  crsql_close(db);
  sqlite3_free(pErrMsg);
  printf("bad return code: %d\n", rc);
}

static void testAsIdentifierListStr() {
  printf("AsIdentifierListStr\n");

  char *tc1[] = {"one", "two", "three"};
  char *res;

  res = crsql_asIdentifierListStr(tc1, 3, ',');

  assert(strcmp(res, "\"one\",\"two\",\"three\"") == 0);
  assert(strlen(res) == 19);
  sqlite3_free(res);

  printf("\t\e[0;32mSuccess\e[0m\n");
}

static char *join2map(const char *in) {
  return sqlite3_mprintf("foo %s bar", in);
}

static void testJoin2() {
  printf("Join2\n");
  char *tc0[] = {};
  char *tc1[] = {"one"};
  char *tc2[] = {"one", "two"};
  char *result;

  result = crsql_join2(&join2map, tc0, 0, ", ");
  assert(result == 0);

  result = crsql_join2(&join2map, tc1, 1, ", ");
  assert(strcmp(result, "foo one bar") == 0);
  sqlite3_free(result);

  result = crsql_join2(&join2map, tc2, 2, ", ");
  assert(strcmp(result, "foo one bar, foo two bar") == 0);
  sqlite3_free(result);

  printf("\t\e[0;32mSuccess\e[0m\n");
}

void testSiteIdCmp() {
  printf("SiteIdCmp\n");

  char left[1] = {0x00};
  char right[1] = {0x00};

  assert(crsql_siteIdCmp(left, 1, right, 1) == 0);

  left[0] = 0x0a;
  assert(crsql_siteIdCmp(left, 1, right, 1) == 1);

  right[0] = 0x10;
  assert(crsql_siteIdCmp(left, 1, right, 1) == -1);

  char left2[2] = {0x00, 0x00};
  right[0] = 0x00;
  assert(crsql_siteIdCmp(left2, 2, right, 1) == 1);

  char right2[2] = {0x00, 0x00};
  left[0] = 0x00;
  assert(crsql_siteIdCmp(left, 1, right2, 2) == -1);

  left[0] = 0x0a;
  assert(crsql_siteIdCmp(left, 1, right2, 2) == 1);

  right[0] = 0x11;
  assert(crsql_siteIdCmp(left2, 2, right, 1) == -1);

  printf("\t\e[0;32mSuccess\e[0m\n");
}

#define FREE_PARTS(L)           \
  for (int i = 0; i < L; ++i) { \
    sqlite3_free(parts[i]);     \
  }                             \
  sqlite3_free(parts);

void testSplitQuoteConcat() {
  // test NULL
  char **parts = crsql_splitQuoteConcat("NULL", 1);
  assert(strcmp(parts[0], "NULL") == 0);
  FREE_PARTS(1)

  // test num
  parts = crsql_splitQuoteConcat("1.23", 1);
  assert(strcmp(parts[0], "1.23") == 0);
  FREE_PARTS(1)

  // test empty string
  parts = crsql_splitQuoteConcat("''", 1);
  assert(strcmp(parts[0], "''") == 0);
  FREE_PARTS(1)

  // test string
  parts = crsql_splitQuoteConcat("'this is a''string'''", 1);
  assert(strcmp(parts[0], "'this is a''string'''") == 0);
  FREE_PARTS(1)

  parts = crsql_splitQuoteConcat("'this is another'", 1);
  assert(strcmp(parts[0], "'this is another'") == 0);
  FREE_PARTS(1)

  // test hex
  parts = crsql_splitQuoteConcat("X'aa'", 1);
  assert(strcmp(parts[0], "X'aa'") == 0);
  FREE_PARTS(1)

  // test many nulls
  parts = crsql_splitQuoteConcat("NULL|NULL|NULL", 3);
  assert(strcmp(parts[0], "NULL") == 0);
  assert(strcmp(parts[1], "NULL") == 0);
  assert(strcmp(parts[2], "NULL") == 0);
  FREE_PARTS(3)

  // test many nums
  parts = crsql_splitQuoteConcat("12|23324|2.2", 3);
  assert(strcmp(parts[0], "12") == 0);
  assert(strcmp(parts[1], "23324") == 0);
  assert(strcmp(parts[2], "2.2") == 0);
  FREE_PARTS(3)

  // test many empty strings
  parts = crsql_splitQuoteConcat("''|''|''", 3);
  assert(strcmp(parts[0], "''") == 0);
  assert(strcmp(parts[1], "''") == 0);
  assert(strcmp(parts[2], "''") == 0);
  FREE_PARTS(3)

  // test many hex
  parts = crsql_splitQuoteConcat("X'aa'|X'ff'|X'cc'", 3);
  assert(strcmp(parts[0], "X'aa'") == 0);
  assert(strcmp(parts[1], "X'ff'") == 0);
  assert(strcmp(parts[2], "X'cc'") == 0);
  FREE_PARTS(3)

  // test many strings
  parts = crsql_splitQuoteConcat("'foo'|'bar'|'ba''z'", 3);
  assert(strcmp(parts[0], "'foo'") == 0);
  assert(strcmp(parts[1], "'bar'") == 0);
  assert(strcmp(parts[2], "'ba''z'") == 0);
  FREE_PARTS(3)

  // test not enough parts
  parts = crsql_splitQuoteConcat("'foo'|'bar'", 3);
  assert(parts == 0);

  // test too many parts
  parts = crsql_splitQuoteConcat("'foo'|'bar'|1", 2);
  assert(parts == 0);

  // test combinations of types
  parts = crsql_splitQuoteConcat("'foo'|'bar'|1", 3);
  assert(strcmp(parts[0], "'foo'") == 0);
  assert(strcmp(parts[1], "'bar'") == 0);
  assert(strcmp(parts[2], "1") == 0);
  FREE_PARTS(3)

  parts = crsql_splitQuoteConcat("X'foo'|123|NULL", 3);
  assert(strcmp(parts[0], "X'foo'") == 0);
  assert(strcmp(parts[1], "123") == 0);
  assert(strcmp(parts[2], "NULL") == 0);
  FREE_PARTS(3)

  // test incorrectly escaped string
  parts = crsql_splitQuoteConcat("'dude''", 1);
  assert(parts == 0);
  parts = crsql_splitQuoteConcat("'du'de'", 1);
  assert(parts == 0);

  // test unquoted string
  parts = crsql_splitQuoteConcat("s", 1);
  assert(parts == 0);

  // test digits with chars
  parts = crsql_splitQuoteConcat("12s", 1);
  assert(parts == 0);

  // test X str
  parts = crsql_splitQuoteConcat("Xs", 1);
  assert(parts == 0);
  parts = crsql_splitQuoteConcat("X's", 1);
  assert(parts == 0);
  parts = crsql_splitQuoteConcat("X's''", 1);
  assert(parts == 0);

  // test string missing end quote
  parts = crsql_splitQuoteConcat("'s", 1);
  assert(parts == 0);
}

void crsqlUtilTestSuite() {
  printf("\e[47m\e[1;30mSuite: crsql_util\e[0m\n");

  testGetVersionUnionQuery();
  testDoesTableExist();
  testGetCount();
  testJoinWith();
  testGetIndexedCols();
  testAsIdentifierListStr();
  testJoin2();
  testSiteIdCmp();
  testSplitQuoteConcat();

  // TODO: test pk pulling and correct sorting of pks
  // TODO: create a fn to create test tables for all tests.
}
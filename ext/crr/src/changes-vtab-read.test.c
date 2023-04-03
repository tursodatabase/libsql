#include "changes-vtab-read.h"

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "consts.h"
#include "crsqlite.h"

int crsql_close(sqlite3 *db);

static void testChangesQueryForTable() {
  printf("ChangeQueryForTable\n");
  int rc = SQLITE_OK;
  sqlite3 *db;
  char *err = 0;
  crsql_TableInfo *tblInfo = 0;
  rc = sqlite3_open(":memory:", &db);

  rc += sqlite3_exec(db, "create table foo (a primary key, b);", 0, 0, &err);
  rc += sqlite3_exec(db, "select crsql_as_crr('foo');", 0, 0, &err);
  rc += crsql_getTableInfo(db, "foo", &tblInfo, &err);
  assert(rc == SQLITE_OK);

  char *query = crsql_changesQueryForTable(tblInfo, 6);

  assert(strcmp(query,
                "SELECT      \'foo\' as tbl,      quote(\"a\") as pks,      "
                "__crsql_col_name as cid,      __crsql_col_version as "
                "col_vrsn,      __crsql_db_version as db_vrsn,      "
                "__crsql_site_id as site_id    FROM \"foo__crsql_clock\"    "
                "WHERE      site_id IS NOT ?    AND      db_vrsn > ?") == 0);
  sqlite3_free(query);

  query = crsql_changesQueryForTable(tblInfo, 8);
  assert(strcmp(query,
                "SELECT      \'foo\' as tbl,      quote(\"a\") as pks,      "
                "__crsql_col_name as cid,      __crsql_col_version as "
                "col_vrsn,      __crsql_db_version as db_vrsn,      "
                "__crsql_site_id as site_id    FROM \"foo__crsql_clock\"    "
                "WHERE      site_id IS  ?    AND      db_vrsn > ?") == 0);
  sqlite3_free(query);

  printf("\t\e[0;32mSuccess\e[0m\n");

  sqlite3_free(err);
  crsql_freeTableInfo(tblInfo);
  crsql_close(db);
  assert(rc == SQLITE_OK);
}

static void testChangesUnionQuery() {
  printf("ChangesUnionQuery\n");

  int rc = SQLITE_OK;
  sqlite3 *db;
  char *err = 0;
  crsql_TableInfo **tblInfos = sqlite3_malloc(2 * sizeof(crsql_TableInfo *));
  rc = sqlite3_open(":memory:", &db);

  rc += sqlite3_exec(db, "create table foo (a primary key, b);", 0, 0, &err);
  rc += sqlite3_exec(db, "create table bar (\"x\" primary key, [y]);", 0, 0,
                     &err);
  rc += sqlite3_exec(db, "select crsql_as_crr('foo');", 0, 0, &err);
  rc += sqlite3_exec(db, "select crsql_as_crr('bar');", 0, 0, &err);
  rc += crsql_getTableInfo(db, "foo", &tblInfos[0], &err);
  rc += crsql_getTableInfo(db, "bar", &tblInfos[1], &err);
  assert(rc == SQLITE_OK);

  char *query = crsql_changesUnionQuery(tblInfos, 2, 6);
  assert(
      strcmp(
          query,
          "SELECT tbl, pks, cid, col_vrsn, db_vrsn, site_id FROM (SELECT      "
          "\'foo\' as tbl,      quote(\"a\") as pks,      __crsql_col_name as "
          "cid,      __crsql_col_version as col_vrsn,      __crsql_db_version "
          "as db_vrsn,      __crsql_site_id as site_id    FROM "
          "\"foo__crsql_clock\"    WHERE      site_id IS NOT ?    AND      "
          "db_vrsn > ? UNION SELECT      \'bar\' as tbl,      quote(\"x\") as "
          "pks,      __crsql_col_name as cid,      __crsql_col_version as "
          "col_vrsn,      __crsql_db_version as db_vrsn,      __crsql_site_id "
          "as site_id    FROM \"bar__crsql_clock\"    WHERE      site_id IS "
          "NOT ?    AND      db_vrsn > ?) ORDER BY db_vrsn, tbl ASC") == 0);
  sqlite3_free(query);

  query = crsql_changesUnionQuery(tblInfos, 2, 8);
  assert(
      strcmp(
          query,
          "SELECT tbl, pks, cid, col_vrsn, db_vrsn, site_id FROM (SELECT      "
          "\'foo\' as tbl,      quote(\"a\") as pks,      __crsql_col_name as "
          "cid,      __crsql_col_version as col_vrsn,      __crsql_db_version "
          "as db_vrsn,      __crsql_site_id as site_id    FROM "
          "\"foo__crsql_clock\"    WHERE      site_id IS  ?    AND      "
          "db_vrsn > ? UNION SELECT      \'bar\' as tbl,      quote(\"x\") as "
          "pks,      __crsql_col_name as cid,      __crsql_col_version as "
          "col_vrsn,      __crsql_db_version as db_vrsn,      __crsql_site_id "
          "as site_id    FROM \"bar__crsql_clock\"    WHERE      site_id IS  ? "
          "   AND      db_vrsn > ?) ORDER BY db_vrsn, tbl ASC") == 0);
  sqlite3_free(query);

  printf("\t\e[0;32mSuccess\e[0m\n");
  sqlite3_free(err);
  crsql_freeAllTableInfos(tblInfos, 2);
  crsql_close(db);
  assert(rc == SQLITE_OK);
}

static void testRowPatchDataQuery() {
  printf("RowPatchDataQuery\n");

  int rc = SQLITE_OK;
  sqlite3 *db;
  char *err = 0;
  crsql_TableInfo *tblInfo = 0;
  rc = sqlite3_open(":memory:", &db);

  rc += sqlite3_exec(db, "create table foo (a primary key, b, c, d);", 0, 0,
                     &err);
  rc += sqlite3_exec(db, "select crsql_as_crr('foo');", 0, 0, &err);
  rc += sqlite3_exec(db, "insert into foo values(1, 'cb', 'cc', 'cd')", 0, 0,
                     &err);
  rc += crsql_getTableInfo(db, "foo", &tblInfo, &err);
  assert(rc == SQLITE_OK);

  // TC1: single pk table, 1 col change
  const char *cid = "b";
  char *pks = "1";
  char *q = crsql_rowPatchDataQuery(db, tblInfo, cid, pks);
  assert(strcmp(q, "SELECT quote(\"b\") FROM \"foo\" WHERE \"a\" = 1") == 0);
  sqlite3_free(q);

  printf("\t\e[0;32mSuccess\e[0m\n");
  sqlite3_free(err);
  crsql_freeTableInfo(tblInfo);
  crsql_close(db);
  assert(rc == SQLITE_OK);
}

void crsqlChangesVtabReadTestSuite() {
  printf("\e[47m\e[1;30mSuite: crsql_changesVtabRead\e[0m\n");
  testChangesQueryForTable();
  testChangesUnionQuery();
  testRowPatchDataQuery();
}

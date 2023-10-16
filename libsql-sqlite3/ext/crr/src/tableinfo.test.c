#include "tableinfo.h"

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "consts.h"
#include "crsqlite.h"
#include "util.h"

int crsql_close(sqlite3 *db);

static void testGetTableInfo() {
  printf("GetTableInfo\n");
  sqlite3 *db = 0;
  crsql_TableInfo *tableInfo = 0;
  char *errMsg = 0;
  int rc = SQLITE_OK;

  rc = sqlite3_open(":memory:", &db);

  sqlite3_exec(db, "CREATE TABLE foo (a INT NOT NULL, b)", 0, 0, 0);
  rc = crsql_getTableInfo(db, "foo", &tableInfo, &errMsg);

  if (rc != SQLITE_OK) {
    printf("err: %s %d\n", errMsg, rc);
    sqlite3_free(errMsg);
    crsql_close(db);
    assert(0);
    return;
  }

  assert(tableInfo->baseColsLen == 2);
  assert(tableInfo->baseCols[0].cid == 0);
  assert(strcmp(tableInfo->baseCols[0].name, "a") == 0);
  assert(strcmp(tableInfo->baseCols[0].type, "INT") == 0);
  assert(tableInfo->baseCols[0].notnull == 1);
  assert(tableInfo->baseCols[0].pk == 0);

  assert(tableInfo->pksLen == 0);
  assert(tableInfo->pks == 0);

  assert(tableInfo->nonPksLen == 2);
  assert(tableInfo->nonPks[0].cid == 0);
  assert(strcmp(tableInfo->nonPks[0].name, "a") == 0);
  assert(strcmp(tableInfo->nonPks[0].type, "INT") == 0);
  assert(tableInfo->nonPks[0].notnull == 1);
  assert(tableInfo->nonPks[0].pk == 0);

  crsql_freeTableInfo(tableInfo);

  sqlite3_exec(db, "CREATE TABLE bar (a PRIMARY KEY, b)", 0, 0, 0);
  rc = crsql_getTableInfo(db, "bar", &tableInfo, &errMsg);
  if (rc != SQLITE_OK) {
    printf("err: %s %d\n", errMsg, rc);
    sqlite3_free(errMsg);
    crsql_close(db);
    assert(0);
    return;
  }

  assert(tableInfo->baseColsLen == 2);
  assert(tableInfo->baseCols[0].cid == 0);
  assert(strcmp(tableInfo->baseCols[0].name, "a") == 0);
  assert(strcmp(tableInfo->baseCols[0].type, "") == 0);
  assert(tableInfo->baseCols[0].notnull == 0);
  assert(tableInfo->baseCols[0].pk == 1);

  assert(tableInfo->pksLen == 1);
  assert(tableInfo->nonPksLen == 1);

  crsql_freeTableInfo(tableInfo);

  printf("\t\e[0;32mSuccess\e[0m\n");
  crsql_close(db);
}

static void testAsIdentifierList() {
  printf("AsIdentifierList\n");

  crsql_ColumnInfo tc1[3];
  tc1[0].name = "one";
  tc1[1].name = "two";
  tc1[2].name = "three";

  crsql_ColumnInfo tc2[0];

  crsql_ColumnInfo tc3[1];
  tc3[0].name = "one";
  char *result;

  result = crsql_asIdentifierList(tc1, 3, 0);
  assert(strcmp(result, "\"one\",\"two\",\"three\"") == 0);
  sqlite3_free(result);

  result = crsql_asIdentifierList(tc2, 0, 0);
  assert(result == 0);
  sqlite3_free(result);

  result = crsql_asIdentifierList(tc3, 1, 0);
  assert(strcmp(result, "\"one\"") == 0);
  sqlite3_free(result);

  printf("\t\e[0;32mSuccess\e[0m\n");
}

static void testFindTableInfo() {
  printf("FindTableInfo\n");

  crsql_TableInfo **tblInfos = sqlite3_malloc(3 * sizeof(crsql_TableInfo *));
  for (int i = 0; i < 3; ++i) {
    tblInfos[i] = sqlite3_malloc(sizeof(crsql_TableInfo));
    tblInfos[i]->tblName = sqlite3_mprintf("%d", i);
  }

  assert(crsql_findTableInfo(tblInfos, 3, "0") == tblInfos[0]);
  assert(crsql_findTableInfo(tblInfos, 3, "1") == tblInfos[1]);
  assert(crsql_findTableInfo(tblInfos, 3, "2") == tblInfos[2]);
  assert(crsql_findTableInfo(tblInfos, 3, "3") == 0);

  for (int i = 0; i < 3; ++i) {
    sqlite3_free(tblInfos[i]->tblName);
    sqlite3_free(tblInfos[i]);
  }
  sqlite3_free(tblInfos);

  printf("\t\e[0;32mSuccess\e[0m\n");
}

static void testQuoteConcat() {
  printf("QuoteConcat\n");

  int len = 3;
  crsql_ColumnInfo colInfos[3];

  colInfos[0].name = "a";
  colInfos[1].name = "b";
  colInfos[2].name = "c";

  char *quoted = crsql_quoteConcat(colInfos, len);

  assert(strcmp(quoted,
                "quote(\"a\") || '|' || quote(\"b\") || '|' || quote(\"c\")") ==
         0);

  sqlite3_free(quoted);
  printf("\t\e[0;32mSuccess\e[0m\n");
}

static void testIsTableCompatible() {
  printf("IsTableCompatible\n");
  sqlite3 *db = 0;
  char *errmsg = 0;
  int rc = SQLITE_OK;

  rc = sqlite3_open(":memory:", &db);
  // no pks
  rc += sqlite3_exec(db, "CREATE TABLE foo (a)", 0, 0, 0);
  assert(rc == SQLITE_OK);
  rc = crsql_isTableCompatible(db, "foo", &errmsg);
  assert(rc == 0);
  sqlite3_free(errmsg);

  // pks
  rc = sqlite3_exec(db, "CREATE TABLE bar (a primary key)", 0, 0, 0);
  assert(rc == SQLITE_OK);
  rc = crsql_isTableCompatible(db, "bar", &errmsg);
  assert(rc == 1);

  // pks + other non unique indices
  rc = sqlite3_exec(db, "CREATE TABLE baz (a primary key, b)", 0, 0, 0);
  rc += sqlite3_exec(db, "CREATE INDEX bar_i ON baz (b)", 0, 0, 0);
  assert(rc == SQLITE_OK);
  rc = crsql_isTableCompatible(db, "bar", &errmsg);
  assert(rc == 1);

  // pks + other unique indices
  rc = sqlite3_exec(db, "CREATE TABLE fuzz (a primary key, b)", 0, 0, 0);
  rc += sqlite3_exec(db, "CREATE UNIQUE INDEX fuzz_i ON fuzz (b)", 0, 0, 0);
  assert(rc == SQLITE_OK);
  rc = crsql_isTableCompatible(db, "fuzz", &errmsg);
  assert(rc == 0);
  sqlite3_free(errmsg);

  // not null and no dflt
  rc = sqlite3_exec(db, "CREATE TABLE buzz (a primary key, b NOT NULL)", 0, 0,
                    0);
  assert(rc == SQLITE_OK);
  rc = crsql_isTableCompatible(db, "buzz", &errmsg);
  assert(rc == 0);
  sqlite3_free(errmsg);

  // not null and dflt
  rc = sqlite3_exec(
      db, "CREATE TABLE boom (a primary key, b NOT NULL DEFAULT 1)", 0, 0, 0);
  assert(rc == SQLITE_OK);
  rc = crsql_isTableCompatible(db, "boom", &errmsg);
  assert(rc == 1);

  // fk constraint
  rc = sqlite3_exec(
      db,
      "CREATE TABLE zoom (a primary key, b, FOREIGN KEY(b) REFERENCES foo(a))",
      0, 0, 0);
  assert(rc == SQLITE_OK);
  rc = crsql_isTableCompatible(db, "zoom", &errmsg);
  assert(rc == 0);
  sqlite3_free(errmsg);

  // strict mode should be ok
  rc = sqlite3_exec(db, "CREATE TABLE atable (\"id\" TEXT PRIMARY KEY) STRICT",
                    0, 0, 0);
  assert(rc == SQLITE_OK);
  rc = crsql_isTableCompatible(db, "atable", &errmsg);
  assert(rc == 1);

  rc = sqlite3_exec(
      db, "CREATE TABLE atable2 (\"id\" TEXT PRIMARY KEY, x TEXT) STRICT;", 0,
      0, 0);
  assert(rc == SQLITE_OK);
  rc = crsql_isTableCompatible(db, "atable2", &errmsg);
  assert(rc == 1);

  rc = sqlite3_exec(db,
                    "CREATE TABLE ydoc (\
      doc_id TEXT,\
      yhash BLOB,\
      yval BLOB,\
      primary key (doc_id, yhash)\
    ) STRICT;",
                    0, 0, 0);
  assert(rc == SQLITE_OK);
  rc = crsql_isTableCompatible(db, "atable2", &errmsg);
  assert(rc == 1);

  printf("\t\e[0;32mSuccess\e[0m\n");
  crsql_close(db);
}

void crsqlTableInfoTestSuite() {
  printf("\e[47m\e[1;30mSuite: crsql_tableInfo\e[0m\n");

  testAsIdentifierList();
  testGetTableInfo();
  testFindTableInfo();
  testQuoteConcat();
  testIsTableCompatible();
  // testPullAllTableInfos();
}
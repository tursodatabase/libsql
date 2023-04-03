#include "changes-vtab-common.h"

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "consts.h"
#include "crsqlite.h"

#ifndef CHECK_OK
#define CHECK_OK         \
  if (rc != SQLITE_OK) { \
    goto fail;           \
  }
#endif

static void testExtractWhereList() {
  printf("ExtractWhereList\n");
  crsql_ColumnInfo columnInfos[3];

  columnInfos[0].name = "foo";
  columnInfos[1].name = "bar";
  columnInfos[2].name = "baz";

  // Test not enough parts
  char *whereList = crsql_extractWhereList(columnInfos, 3, "");
  assert(whereList == 0);
  sqlite3_free(whereList);

  // Test too many parts
  whereList = crsql_extractWhereList(columnInfos, 3, "'a'|'b'|'c'|'d'");
  assert(whereList == 0);

  // Just right
  whereList = crsql_extractWhereList(columnInfos, 3, "'a'|'b'|'c'");
  assert(strcmp("\"foo\" = 'a' AND \"bar\" = 'b' AND \"baz\" = 'c'",
                whereList) == 0);
  sqlite3_free(whereList);

  printf("\t\e[0;32mSuccess\e[0m\n");
}

static void testQuoteConcatedValuesAsList() {
  printf("QuoteConcatedValuesAsList\n");

  char *l = crsql_quoteConcatedValuesAsList("5", 1);
  assert(strcmp(l, "5") == 0);
  sqlite3_free(l);

  l = crsql_quoteConcatedValuesAsList("'h'", 1);
  assert(strcmp(l, "'h'") == 0);
  sqlite3_free(l);

  l = crsql_quoteConcatedValuesAsList("'h'|1|X'aa'", 3);
  assert(strcmp(l, "'h',1,X'aa'") == 0);
  sqlite3_free(l);

  printf("\t\e[0;32mSuccess\e[0m\n");
}

static void testQuotedValuesAsList() {
  printf("QuotedValuesAsList\n");
  printf("\t\e[0;32mSuccess\e[0m\n");
}

void crsqlChangesVtabCommonTestSuite() {
  printf("\e[47m\e[1;30mSuite: crsql_changesVtabCommon\e[0m\n");
  testExtractWhereList();
  testQuoteConcatedValuesAsList();
  testQuotedValuesAsList();
}

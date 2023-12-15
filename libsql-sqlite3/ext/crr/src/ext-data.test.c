#include "ext-data.h"

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "consts.h"
#include "crsqlite.h"

int crsql_close(sqlite3 *db);

static void textNewExtData() {
  printf("NewExtData\n");
  sqlite3 *db;
  int rc = SQLITE_OK;
  rc = sqlite3_open(":memory:", &db);
  assert(rc == SQLITE_OK);
  unsigned char *siteIdBuffer =
      sqlite3_malloc(SITE_ID_LEN * sizeof *(siteIdBuffer));
  crsql_ExtData *pExtData = crsql_newExtData(db, siteIdBuffer);

  assert(pExtData->dbVersion == -1);
  // statement used to determine schema version
  assert(pExtData->pPragmaSchemaVersionStmt != 0);
  // statement used to determine data version
  assert(pExtData->pPragmaDataVersionStmt != 0);
  // last schema version fetched -- none so -1
  assert(pExtData->pragmaSchemaVersion == -1);
  // same as above
  assert(pExtData->pragmaSchemaVersionForTableInfos == -1);
  // set in initSiteId
  assert(pExtData->siteId != 0);
  // no db version extraction yet
  assert(pExtData->pDbVersionStmt == 0);
  // table info allocated to an empty vec
  assert(pExtData->tableInfos != 0);

  // data version should have been fetched
  assert(pExtData->pragmaDataVersion != -1);

  crsql_finalize(pExtData);
  crsql_freeExtData(pExtData);
  crsql_close(db);
  printf("\t\e[0;32mSuccess\e[0m\n");
}

// valgrind will let us know if we failed to free something
// mainly testing that we can finalize + free without error
static void testFreeExtData() {
  printf("FreeExtData\n");
  sqlite3 *db;
  int rc;
  rc = sqlite3_open(":memory:", &db);
  assert(rc == SQLITE_OK);
  unsigned char *siteIdBuffer = sqlite3_malloc(SITE_ID_LEN * sizeof(char *));
  crsql_ExtData *pExtData = crsql_newExtData(db, siteIdBuffer);

  crsql_finalize(pExtData);
  crsql_freeExtData(pExtData);
  crsql_close(db);
  printf("\t\e[0;32mSuccess\e[0m\n");
}

static void testFinalize() {
  printf("FinalizeExtData\n");
  sqlite3 *db;
  int rc;
  rc = sqlite3_open(":memory:", &db);
  assert(rc == SQLITE_OK);
  unsigned char *siteIdBuffer = sqlite3_malloc(SITE_ID_LEN * sizeof(char *));
  crsql_ExtData *pExtData = crsql_newExtData(db, siteIdBuffer);

  crsql_finalize(pExtData);
  assert(pExtData->pDbVersionStmt == 0);
  assert(pExtData->pPragmaSchemaVersionStmt == 0);

  // finalizing twice should be a no-op
  crsql_finalize(pExtData);
  crsql_freeExtData(pExtData);
  crsql_close(db);
  printf("\t\e[0;32mSuccess\e[0m\n");
}

static void testFetchPragmaSchemaVersion() {
  printf("FetchPragmaSchemaVersion\n");
  sqlite3 *db;
  int rc;
  int didChange = 0;
  rc = sqlite3_open(":memory:", &db);
  assert(rc == SQLITE_OK);
  unsigned char *siteIdBuffer = sqlite3_malloc(SITE_ID_LEN * sizeof(char *));
  crsql_ExtData *pExtData = crsql_newExtData(db, siteIdBuffer);

  // fetch the schema info for db version update
  didChange = crsql_fetchPragmaSchemaVersion(db, pExtData, 0);
  assert(pExtData->pragmaSchemaVersion != -1);
  assert(didChange == 1);

  // fetch the schema info for table info
  didChange = crsql_fetchPragmaSchemaVersion(db, pExtData, 1);
  assert(didChange == 1);
  assert(pExtData->pragmaSchemaVersionForTableInfos != -1);

  // re-fetch both with no schema change, should be same value
  int oldVersion = pExtData->pragmaSchemaVersion;
  didChange = crsql_fetchPragmaSchemaVersion(db, pExtData, 0);
  assert(oldVersion == pExtData->pragmaSchemaVersion);
  assert(didChange == 0);

  oldVersion = pExtData->pragmaSchemaVersionForTableInfos;
  didChange = crsql_fetchPragmaSchemaVersion(db, pExtData, 1);
  assert(oldVersion == pExtData->pragmaSchemaVersionForTableInfos);
  assert(didChange == 0);

  // now make a schema modification
  sqlite3_exec(db, "CREATE TABLE foo (a)", 0, 0, 0);
  oldVersion = pExtData->pragmaSchemaVersion;
  didChange = crsql_fetchPragmaSchemaVersion(db, pExtData, 0);
  assert(oldVersion != pExtData->pragmaSchemaVersion);
  assert(didChange == 1);

  oldVersion = pExtData->pragmaSchemaVersionForTableInfos;
  didChange = crsql_fetchPragmaSchemaVersion(db, pExtData, 1);
  assert(oldVersion != pExtData->pragmaSchemaVersionForTableInfos);
  assert(didChange == 1);

  // re-fetch both with no schema change again, should be same value
  oldVersion = pExtData->pragmaSchemaVersion;
  didChange = crsql_fetchPragmaSchemaVersion(db, pExtData, 0);
  assert(oldVersion == pExtData->pragmaSchemaVersion);
  assert(didChange == 0);

  oldVersion = pExtData->pragmaSchemaVersionForTableInfos;
  didChange = crsql_fetchPragmaSchemaVersion(db, pExtData, 1);
  assert(oldVersion == pExtData->pragmaSchemaVersionForTableInfos);
  assert(didChange == 0);

  crsql_finalize(pExtData);
  crsql_freeExtData(pExtData);
  crsql_close(db);
  printf("\t\e[0;32mSuccess\e[0m\n");
}

static void testFetchPragmaDataVersion() {
  printf("FetchPragmaDataVersion\n");
  remove("testFetchPragmaDataVersion.db");
  sqlite3 *db1;
  sqlite3 *db2;
  int rc;
  char *errmsg = 0;
  rc = sqlite3_open("testFetchPragmaDataVersion.db", &db1);
  assert(rc == SQLITE_OK);
  rc = sqlite3_open("testFetchPragmaDataVersion.db", &db2);
  assert(rc == SQLITE_OK);

  rc = sqlite3_exec(db1, "CREATE TABLE fpdv (a)", 0, 0, &errmsg);
  assert(rc == SQLITE_OK);

  unsigned char *siteIdBuffer = sqlite3_malloc(SITE_ID_LEN * sizeof(char *));
  crsql_ExtData *pExtData1 = crsql_newExtData(db1, siteIdBuffer);
  siteIdBuffer = sqlite3_malloc(SITE_ID_LEN * sizeof(char *));
  crsql_ExtData *pExtData2 = crsql_newExtData(db2, siteIdBuffer);

  // should not change after init
  rc = crsql_fetchPragmaDataVersion(db1, pExtData1);
  assert(rc == 0);
  rc = crsql_fetchPragmaDataVersion(db2, pExtData2);
  assert(rc == 0);

  // should not change if write was issued on its connection
  rc = sqlite3_exec(db1, "INSERT INTO fpdv VALUES (1)", 0, 0, 0);
  assert(rc == SQLITE_OK);
  rc = crsql_fetchPragmaDataVersion(db1, pExtData1);
  assert(rc == 0);

  // should change if write was issued on another connection
  rc = crsql_fetchPragmaDataVersion(db2, pExtData2);
  assert(rc == 1);

  // should not change after updating itself
  rc = crsql_fetchPragmaDataVersion(db2, pExtData2);
  assert(rc == 0);

  // same test but in reverse direction
  rc = sqlite3_exec(db2, "INSERT INTO fpdv VALUES (1)", 0, 0, 0);
  assert(rc == SQLITE_OK);
  rc = crsql_fetchPragmaDataVersion(db2, pExtData2);
  assert(rc == 0);

  rc = crsql_fetchPragmaDataVersion(db1, pExtData1);
  assert(rc == 1);

  // should not change after updating itself
  rc = crsql_fetchPragmaDataVersion(db1, pExtData1);
  assert(rc == 0);

  crsql_finalize(pExtData1);
  crsql_freeExtData(pExtData1);
  crsql_close(db1);
  crsql_finalize(pExtData2);
  crsql_freeExtData(pExtData2);
  crsql_close(db2);
  remove("testFetchPragmaDataVersion.db");
  printf("\t\e[0;32mSuccess\e[0m\n");
}

static void testRecreateDbVersionStmt() {
  printf("RecreateDbVersionStmt\n");
  sqlite3 *db;
  int rc;
  rc = sqlite3_open(":memory:", &db);
  unsigned char *siteIdBuffer = sqlite3_malloc(SITE_ID_LEN * sizeof(char *));
  crsql_ExtData *pExtData = crsql_newExtData(db, siteIdBuffer);

  rc = crsql_recreate_db_version_stmt(db, pExtData);

  // there are no clock tables yet. nothing to create.
  assert(rc == -1);
  assert(pExtData->pDbVersionStmt == 0);

  sqlite3_exec(db, "CREATE TABLE foo (a primary key not null, b);", 0, 0, 0);
  sqlite3_exec(db, "SELECT crsql_as_crr('foo')", 0, 0, 0);

  rc = crsql_recreate_db_version_stmt(db, pExtData);
  assert(rc == 0);
  assert(pExtData->pDbVersionStmt != 0);

  // recreating while a created statement exists isn't an error
  rc = crsql_recreate_db_version_stmt(db, pExtData);
  assert(rc == 0);
  assert(pExtData->pDbVersionStmt != 0);

  crsql_finalize(pExtData);
  assert(pExtData->pDbVersionStmt == 0);
  crsql_freeExtData(pExtData);
  crsql_close(db);
  printf("\t\e[0;32mSuccess\e[0m\n");
}

void crsqlExtDataTestSuite() {
  printf("\e[47m\e[1;30mSuite: crsql_ExtData\e[0m\n");
  textNewExtData();
  testFreeExtData();
  testFinalize();
  testFetchPragmaSchemaVersion();
  testRecreateDbVersionStmt();
  testFetchPragmaDataVersion();
}

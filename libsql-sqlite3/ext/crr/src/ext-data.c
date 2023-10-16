#include "ext-data.h"

#include "consts.h"
#include "get-table.h"
#include "util.h"

crsql_ExtData *crsql_newExtData(sqlite3 *db) {
  crsql_ExtData *pExtData = sqlite3_malloc(sizeof *pExtData);

  pExtData->pPragmaSchemaVersionStmt = 0;
  int rc = sqlite3_prepare_v3(db, "PRAGMA schema_version", -1,
                              SQLITE_PREPARE_PERSISTENT,
                              &(pExtData->pPragmaSchemaVersionStmt), 0);
  if (rc != SQLITE_OK) {
    sqlite3_finalize(pExtData->pPragmaSchemaVersionStmt);
    return 0;
  }
  pExtData->pPragmaDataVersionStmt = 0;
  rc = sqlite3_prepare_v3(db, "PRAGMA data_version", -1,
                          SQLITE_PREPARE_PERSISTENT,
                          &(pExtData->pPragmaDataVersionStmt), 0);
  if (rc != SQLITE_OK) {
    sqlite3_finalize(pExtData->pPragmaDataVersionStmt);
    sqlite3_finalize(pExtData->pPragmaSchemaVersionStmt);
    return 0;
  }

  if (rc != SQLITE_OK) {
    sqlite3_finalize(pExtData->pPragmaDataVersionStmt);
    sqlite3_finalize(pExtData->pPragmaSchemaVersionStmt);
    return 0;
  }

  pExtData->dbVersion = -1;
  pExtData->pragmaSchemaVersion = -1;
  pExtData->pragmaDataVersion = -1;
  pExtData->pragmaSchemaVersionForTableInfos = -1;
  pExtData->siteId = sqlite3_malloc(SITE_ID_LEN * sizeof *(pExtData->siteId));
  pExtData->pDbVersionStmt = 0;
  pExtData->zpTableInfos = 0;
  pExtData->tableInfosLen = 0;

  rc = crsql_fetchPragmaDataVersion(db, pExtData);
  if (rc == -1) {
    return 0;
  }

  return pExtData;
}

void crsql_freeExtData(crsql_ExtData *pExtData) {
  sqlite3_free(pExtData->siteId);
  sqlite3_finalize(pExtData->pDbVersionStmt);
  sqlite3_finalize(pExtData->pPragmaSchemaVersionStmt);
  sqlite3_finalize(pExtData->pPragmaDataVersionStmt);
  crsql_freeAllTableInfos(pExtData->zpTableInfos, pExtData->tableInfosLen);
  sqlite3_free(pExtData);
}

// Should _only_ be called when disconnecting from the db
// for some reason finalization in extension unload methods doesn't
// work as expected
// see https://sqlite.org/forum/forumpost/c94f943821
// `freeExtData` is called after finalization when the extension unloads
void crsql_finalize(crsql_ExtData *pExtData) {
  sqlite3_finalize(pExtData->pDbVersionStmt);
  sqlite3_finalize(pExtData->pPragmaSchemaVersionStmt);
  sqlite3_finalize(pExtData->pPragmaDataVersionStmt);
  pExtData->pDbVersionStmt = 0;
  pExtData->pPragmaSchemaVersionStmt = 0;
  pExtData->pPragmaDataVersionStmt = 0;
}

#define DB_VERSION_SCHEMA_VERSION 0
#define TABLE_INFO_SCHEMA_VERSION 1

int crsql_fetchPragmaSchemaVersion(sqlite3 *db, crsql_ExtData *pExtData,
                                   int which) {
  int rc = sqlite3_step(pExtData->pPragmaSchemaVersionStmt);
  if (rc == SQLITE_ROW) {
    int version = sqlite3_column_int(pExtData->pPragmaSchemaVersionStmt, 0);
    sqlite3_reset(pExtData->pPragmaSchemaVersionStmt);
    if (which == DB_VERSION_SCHEMA_VERSION) {
      if (version > pExtData->pragmaSchemaVersion) {
        pExtData->pragmaSchemaVersion = version;
        return 1;
      }
    } else {
      if (version > pExtData->pragmaSchemaVersionForTableInfos) {
        pExtData->pragmaSchemaVersionForTableInfos = version;
        return 1;
      }
    }

    return 0;
  } else {
    sqlite3_reset(pExtData->pPragmaSchemaVersionStmt);
  }

  return -1;
}

int crsql_fetchPragmaDataVersion(sqlite3 *db, crsql_ExtData *pExtData) {
  int rc = sqlite3_step(pExtData->pPragmaDataVersionStmt);
  if (rc != SQLITE_ROW) {
    sqlite3_reset(pExtData->pPragmaDataVersionStmt);
    return -1;
  }

  int version = sqlite3_column_int(pExtData->pPragmaDataVersionStmt, 0);
  sqlite3_reset(pExtData->pPragmaDataVersionStmt);

  if (version != pExtData->pragmaDataVersion) {
    pExtData->pragmaDataVersion = version;
    return 1;
  }

  return 0;
}

int crsql_recreateDbVersionStmt(sqlite3 *db, crsql_ExtData *pExtData) {
  char *zSql = 0;
  char **rClockTableNames = 0;
  int rNumRows = 0;
  int rNumCols = 0;
  int rc = SQLITE_OK;

  sqlite3_finalize(pExtData->pDbVersionStmt);
  pExtData->pDbVersionStmt = 0;

  crsql_get_table(db, CLOCK_TABLES_SELECT, &rClockTableNames, &rNumRows,
                  &rNumCols, 0);

  if (rc != SQLITE_OK) {
    crsql_free_table(rClockTableNames);
    return rc;
  }

  if (rNumRows == 0) {
    crsql_free_table(rClockTableNames);
    return -1;
  }

  zSql = crsql_getDbVersionUnionQuery(rNumRows, rClockTableNames);
  crsql_free_table(rClockTableNames);

  rc = sqlite3_prepare_v3(db, zSql, -1, SQLITE_PREPARE_PERSISTENT,
                          &(pExtData->pDbVersionStmt), 0);
  sqlite3_free(zSql);

  if (rc != SQLITE_OK) {
    sqlite3_finalize(pExtData->pDbVersionStmt);
    pExtData->pDbVersionStmt = 0;
  }

  return rc;
}

int crsql_fetchDbVersionFromStorage(sqlite3 *db, crsql_ExtData *pExtData,
                                    char **errmsg) {
  int rc = SQLITE_OK;
  int bSchemaChanged = 0;

  // version was not cached
  // check if the schema changed and rebuild version stmt if so
  if (pExtData->pDbVersionStmt == 0) {
    bSchemaChanged = 1;
  } else {
    bSchemaChanged =
        crsql_fetchPragmaSchemaVersion(db, pExtData, DB_VERSION_SCHEMA_VERSION);
  }

  if (bSchemaChanged < 0) {
    *errmsg = sqlite3_mprintf("failed to fetch the pragma schema version");
    return SQLITE_ERROR;
  }

  if (bSchemaChanged > 0) {
    rc = crsql_recreateDbVersionStmt(db, pExtData);
    if (rc == -1) {
      // this means there are no clock tables / this is a clean db
      pExtData->dbVersion = MIN_POSSIBLE_DB_VERSION;
      return SQLITE_OK;
    }
    if (rc != SQLITE_OK) {
      *errmsg = sqlite3_mprintf("failed to create the db version statement");
      return rc;
    }
  }

  rc = sqlite3_step(pExtData->pDbVersionStmt);
  // no rows? We're a fresh db with the min starting version
  if (rc == SQLITE_DONE) {
    rc = sqlite3_reset(pExtData->pDbVersionStmt);
    pExtData->dbVersion = MIN_POSSIBLE_DB_VERSION;
    if (rc != SQLITE_OK) {
      *errmsg = sqlite3_mprintf("failed to reset the version statement");
    }
    return rc;
  }

  if (rc != SQLITE_ROW) {
    sqlite3_reset(pExtData->pDbVersionStmt);
    *errmsg = sqlite3_mprintf("errors when stepping version statement");
    return SQLITE_ERROR;
  }

  int type = sqlite3_column_type(pExtData->pDbVersionStmt, 0);
  if (type == SQLITE_NULL) {
    // No rows? We're at min version
    rc = sqlite3_reset(pExtData->pDbVersionStmt);
    pExtData->dbVersion = MIN_POSSIBLE_DB_VERSION;
    if (rc != SQLITE_OK) {
      *errmsg = sqlite3_mprintf("failed to reset the version statement(2)");
    }
    return rc;
  }

  pExtData->dbVersion = sqlite3_column_int64(pExtData->pDbVersionStmt, 0);
  return sqlite3_reset(pExtData->pDbVersionStmt);
}

/**
 * This will return the db version if it exists in `pExtData`
 *
 * If it does not exist there, it will fetch the current db version
 * from the database.
 *
 * `pExtData->dbVersion` is cleared on every tx commit or rollback.
 */
int crsql_getDbVersion(sqlite3 *db, crsql_ExtData *pExtData, char **errmsg) {
  int rc = SQLITE_OK;

  // version is cached. We clear the cached version
  // at the end of each transaction so it is safe to return this
  // without checking the schema version.
  // It is an error to use crsqlite in such a way that you modify
  // a schema and fetch changes in the same transaction.
  rc = crsql_fetchPragmaDataVersion(db, pExtData);
  if (rc == -1) {
    *errmsg = sqlite3_mprintf("failed to fetch PRAGMA data_version");
    return SQLITE_ERROR;
  }
  if (pExtData->dbVersion != -1 && rc == 0) {
    return SQLITE_OK;
  }

  rc = crsql_fetchDbVersionFromStorage(db, pExtData, errmsg);
  return rc;
}

/**
 * Should only ever be called when absolutely required.
 * This can be an expensive operation.
 *
 * (1) checks if the db schema has changed
 * (2) if so, re-pulls table infos after de-allocating previous set of table
 * infos
 *
 * due to 2, nobody should ever save a reference
 * to a table info or contained object.
 *
 * This is called in two cases:
 * (1) in `xFilter` of the changes-vtab to ensure we hit the right tables for
 * changes (2) in `xUpdate` of the changes-vtab to ensure we apply received
 * changed correctly
 */
int crsql_ensureTableInfosAreUpToDate(sqlite3 *db, crsql_ExtData *pExtData,
                                      char **errmsg) {
  int rc = SQLITE_OK;

  int bSchemaChanged =
      crsql_fetchPragmaSchemaVersion(db, pExtData, TABLE_INFO_SCHEMA_VERSION);
  if (bSchemaChanged < 0) {
    return SQLITE_ERROR;
  }

  if (bSchemaChanged || pExtData->zpTableInfos == 0) {
    // clean up old table infos
    crsql_freeAllTableInfos(pExtData->zpTableInfos, pExtData->tableInfosLen);

    // re-fetch table infos
    rc = crsql_pullAllTableInfos(db, &(pExtData->zpTableInfos),
                                 &(pExtData->tableInfosLen), errmsg);
    if (rc != SQLITE_OK) {
      crsql_freeAllTableInfos(pExtData->zpTableInfos, pExtData->tableInfosLen);
      pExtData->zpTableInfos = 0;
      pExtData->tableInfosLen = 0;
      return rc;
    }
  }

  return rc;
}

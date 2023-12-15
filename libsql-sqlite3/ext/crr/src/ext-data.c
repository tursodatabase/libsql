#include "ext-data.h"

#include "consts.h"

void crsql_clear_stmt_cache(crsql_ExtData *pExtData);
void crsql_init_table_info_vec(crsql_ExtData *pExtData);
void crsql_drop_table_info_vec(crsql_ExtData *pExtData);

crsql_ExtData *crsql_newExtData(sqlite3 *db, unsigned char *siteIdBuffer) {
  crsql_ExtData *pExtData = sqlite3_malloc(sizeof *pExtData);

  pExtData->pPragmaSchemaVersionStmt = 0;
  int rc = sqlite3_prepare_v3(db, "PRAGMA schema_version", -1,
                              SQLITE_PREPARE_PERSISTENT,
                              &(pExtData->pPragmaSchemaVersionStmt), 0);
  pExtData->pPragmaDataVersionStmt = 0;
  rc += sqlite3_prepare_v3(db, "PRAGMA data_version", -1,
                           SQLITE_PREPARE_PERSISTENT,
                           &(pExtData->pPragmaDataVersionStmt), 0);
  pExtData->pSetSyncBitStmt = 0;
  rc += sqlite3_prepare_v3(db, SET_SYNC_BIT, -1, SQLITE_PREPARE_PERSISTENT,
                           &(pExtData->pSetSyncBitStmt), 0);
  pExtData->pClearSyncBitStmt = 0;
  rc += sqlite3_prepare_v3(db, CLEAR_SYNC_BIT, -1, SQLITE_PREPARE_PERSISTENT,
                           &(pExtData->pClearSyncBitStmt), 0);

  pExtData->pSetSiteIdOrdinalStmt = 0;
  rc += sqlite3_prepare_v3(
      db, "INSERT INTO crsql_site_id (site_id) VALUES (?) RETURNING ordinal",
      -1, SQLITE_PREPARE_PERSISTENT, &(pExtData->pSetSiteIdOrdinalStmt), 0);

  pExtData->pSelectSiteIdOrdinalStmt = 0;
  rc += sqlite3_prepare_v3(
      db, "SELECT ordinal FROM crsql_site_id WHERE site_id = ?", -1,
      SQLITE_PREPARE_PERSISTENT, &(pExtData->pSelectSiteIdOrdinalStmt), 0);

  pExtData->pSelectClockTablesStmt = 0;
  rc +=
      sqlite3_prepare_v3(db, CLOCK_TABLES_SELECT, -1, SQLITE_PREPARE_PERSISTENT,
                         &(pExtData->pSelectClockTablesStmt), 0);

  pExtData->dbVersion = -1;
  pExtData->pendingDbVersion = -1;
  pExtData->seq = 0;
  pExtData->pragmaSchemaVersion = -1;
  pExtData->pragmaDataVersion = -1;
  pExtData->pragmaSchemaVersionForTableInfos = -1;
  pExtData->siteId = siteIdBuffer;
  pExtData->pDbVersionStmt = 0;
  pExtData->tableInfos = 0;
  pExtData->rowsImpacted = 0;
  pExtData->updatedTableInfosThisTx = 0;
  crsql_init_table_info_vec(pExtData);

  int pv = crsql_fetchPragmaDataVersion(db, pExtData);
  if (pv == -1 || rc != SQLITE_OK) {
    crsql_freeExtData(pExtData);
    return 0;
  }

  return pExtData;
}

void crsql_freeExtData(crsql_ExtData *pExtData) {
  sqlite3_free(pExtData->siteId);
  sqlite3_finalize(pExtData->pDbVersionStmt);
  sqlite3_finalize(pExtData->pPragmaSchemaVersionStmt);
  sqlite3_finalize(pExtData->pPragmaDataVersionStmt);
  sqlite3_finalize(pExtData->pSetSyncBitStmt);
  sqlite3_finalize(pExtData->pClearSyncBitStmt);
  sqlite3_finalize(pExtData->pSetSiteIdOrdinalStmt);
  sqlite3_finalize(pExtData->pSelectSiteIdOrdinalStmt);
  sqlite3_finalize(pExtData->pSelectClockTablesStmt);
  crsql_clear_stmt_cache(pExtData);
  crsql_drop_table_info_vec(pExtData);
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
  sqlite3_finalize(pExtData->pSetSyncBitStmt);
  sqlite3_finalize(pExtData->pClearSyncBitStmt);
  sqlite3_finalize(pExtData->pSetSiteIdOrdinalStmt);
  sqlite3_finalize(pExtData->pSelectSiteIdOrdinalStmt);
  sqlite3_finalize(pExtData->pSelectClockTablesStmt);
  crsql_clear_stmt_cache(pExtData);
  pExtData->pDbVersionStmt = 0;
  pExtData->pPragmaSchemaVersionStmt = 0;
  pExtData->pPragmaDataVersionStmt = 0;
  pExtData->pSetSyncBitStmt = 0;
  pExtData->pClearSyncBitStmt = 0;
  pExtData->pSetSiteIdOrdinalStmt = 0;
  pExtData->pSelectSiteIdOrdinalStmt = 0;
  pExtData->pSelectClockTablesStmt = 0;
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

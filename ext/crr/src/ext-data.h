#ifndef CRSQLITE_EXTDATA_H
#define CRSQLITE_EXTDATA_H

#include "sqlite3ext.h"
SQLITE_EXTENSION_INIT3
#include "tableinfo.h"

// NOTE: any changes here must be updated in `c.rs` until we've finished porting
// to rust.
typedef struct crsql_ExtData crsql_ExtData;
struct crsql_ExtData {
  // perma statement -- used to check db schema version
  sqlite3_stmt *pPragmaSchemaVersionStmt;
  sqlite3_stmt *pPragmaDataVersionStmt;
  int pragmaDataVersion;

  // this gets set at the start of each transaction on the first invocation
  // to crsql_next_db_version()
  // and re-set on transaction commit or rollback.
  sqlite3_int64 dbVersion;
  // the version that the db will be set to at the end of the transaction
  // if that transaction were to commit at the time this value is checked.
  sqlite3_int64 pendingDbVersion;
  int pragmaSchemaVersion;

  // we need another schema version number that tracks when we checked it
  // for zpTableInfos.
  int pragmaSchemaVersionForTableInfos;

  unsigned char *siteId;
  sqlite3_stmt *pDbVersionStmt;
  crsql_TableInfo **zpTableInfos;
  int tableInfosLen;

  // tracks the number of rows impacted by all inserts into crsql_changes in the
  // current transaction. This number is reset on transaction commit.
  int rowsImpacted;

  int seq;

  sqlite3_stmt *pSetSyncBitStmt;
  sqlite3_stmt *pClearSyncBitStmt;
  sqlite3_stmt *pSetSiteIdOrdinalStmt;
  sqlite3_stmt *pSelectSiteIdOrdinalStmt;
  void *pStmtCache;
};

crsql_ExtData *crsql_newExtData(sqlite3 *db, unsigned char *siteIdBuffer);
void crsql_freeExtData(crsql_ExtData *pExtData);
int crsql_fetchPragmaSchemaVersion(sqlite3 *db, crsql_ExtData *pExtData,
                                   int which);
int crsql_fetchPragmaDataVersion(sqlite3 *db, crsql_ExtData *pExtData);
int crsql_recreateDbVersionStmt(sqlite3 *db, crsql_ExtData *pExtData);
int crsql_fetchDbVersionFromStorage(sqlite3 *db, crsql_ExtData *pExtData,
                                    char **errmsg);
int crsql_getDbVersion(sqlite3 *db, crsql_ExtData *pExtData, char **errmsg);
void crsql_finalize(crsql_ExtData *pExtData);
int crsql_ensureTableInfosAreUpToDate(sqlite3 *db, crsql_ExtData *pExtData,
                                      char **errmsg);

#endif
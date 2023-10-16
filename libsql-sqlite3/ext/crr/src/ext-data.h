#ifndef CRSQLITE_EXTDATA_H
#define CRSQLITE_EXTDATA_H

#include "sqlite3ext.h"
SQLITE_EXTENSION_INIT3

#include "tableinfo.h"

typedef struct crsql_ExtData crsql_ExtData;
struct crsql_ExtData {
  // perma statement -- used to check db schema version
  sqlite3_stmt *pPragmaSchemaVersionStmt;
  sqlite3_stmt *pPragmaDataVersionStmt;
  int pragmaDataVersion;

  // this gets set at the start of each transaction on the first invocation
  // to crsql_nextdbversion()
  // and re-set on transaction commit or rollback.
  sqlite3_int64 dbVersion;
  int pragmaSchemaVersion;

  // we need another schema version number that tracks when we checked it
  // for zpTableInfos.
  int pragmaSchemaVersionForTableInfos;

  unsigned char *siteId;
  sqlite3_stmt *pDbVersionStmt;
  crsql_TableInfo **zpTableInfos;
  int tableInfosLen;
};

crsql_ExtData *crsql_newExtData(sqlite3 *db);
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
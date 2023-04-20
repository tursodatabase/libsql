#include "changes-vtab-write.h"

#include <string.h>

#include "changes-vtab-common.h"
#include "changes-vtab.h"
#include "consts.h"
#include "crsqlite.h"
#include "ext-data.h"
#include "tableinfo.h"
#include "util.h"

/**
 *
 */
int crsql_didCidWin(sqlite3 *db, const unsigned char *localSiteId,
                    const char *insertTbl, const char *pkWhereList,
                    const char *colName, const char *sanitizedInsertVal,
                    sqlite3_int64 colVersion, char **errmsg) {
  char *zSql = 0;

  zSql = sqlite3_mprintf(
      "SELECT __crsql_col_version FROM \"%s__crsql_clock\" WHERE %s AND %Q = "
      "__crsql_col_name",
      insertTbl, pkWhereList, colName);

  // run zSql
  sqlite3_stmt *pStmt = 0;
  int rc = sqlite3_prepare_v2(db, zSql, -1, &pStmt, 0);
  sqlite3_free(zSql);

  if (rc != SQLITE_OK) {
    sqlite3_finalize(pStmt);
    *errmsg =
        sqlite3_mprintf("Failed preparing stmt to select local column version");
    return -1;
  }

  rc = sqlite3_step(pStmt);
  if (rc == SQLITE_DONE) {
    sqlite3_finalize(pStmt);
    // no rows returned
    // we of course win if there's nothing there.
    return 1;
  }

  if (rc != SQLITE_ROW) {
    sqlite3_finalize(pStmt);
    *errmsg = sqlite3_mprintf(
        "Bad return code (%d) when selecting local column version", rc);
    return -1;
  }

  sqlite3_int64 localVersion = sqlite3_column_int64(pStmt, 0);
  sqlite3_finalize(pStmt);

  if (colVersion > localVersion) {
    return 1;
  } else if (colVersion < localVersion) {
    return 0;
  }

  // else -- versions are equal
  // - pull curr value
  // - compare for tie break
  // TODO: pull bytes and memcmp instead of strcmp?
  zSql = sqlite3_mprintf("SELECT quote(\"%w\") FROM \"%w\" WHERE %s", colName,
                         insertTbl, pkWhereList);
  rc = sqlite3_prepare_v2(db, zSql, -1, &pStmt, 0);
  sqlite3_free(zSql);

  if (rc != SQLITE_OK) {
    sqlite3_finalize(pStmt);
    *errmsg = sqlite3_mprintf(
        "could not prepare statement to find row to merge with. %s", insertTbl);
    return -1;
  }

  rc = sqlite3_step(pStmt);
  if (rc != SQLITE_ROW) {
    *errmsg = sqlite3_mprintf("could not find row to merge with for tbl %s",
                              insertTbl);
    sqlite3_finalize(pStmt);
    return -1;
  }

  const char *localValue = (const char *)sqlite3_column_text(pStmt, 0);
  int ret = strcmp(sanitizedInsertVal, localValue);
  sqlite3_finalize(pStmt);

  return ret > 0;
}

#define DELETED_LOCALLY -1
int crsql_checkForLocalDelete(sqlite3 *db, const char *tblName,
                              char *pkWhereList) {
  char *zSql = sqlite3_mprintf(
      "SELECT count(*) FROM \"%s__crsql_clock\" WHERE %s AND "
      "__crsql_col_name "
      "= %Q",
      tblName, pkWhereList, DELETE_CID_SENTINEL);
  sqlite3_stmt *pStmt;
  int rc = sqlite3_prepare(db, zSql, -1, &pStmt, 0);
  sqlite3_free(zSql);

  if (rc != SQLITE_OK) {
    sqlite3_finalize(pStmt);
    return rc;
  }

  rc = sqlite3_step(pStmt);
  if (rc != SQLITE_ROW) {
    sqlite3_finalize(pStmt);
    return SQLITE_ERROR;
  }

  int count = sqlite3_column_int(pStmt, 0);
  sqlite3_finalize(pStmt);
  if (count == 1) {
    return DELETED_LOCALLY;
  }

  return SQLITE_OK;
}

int crsql_setWinnerClock(sqlite3 *db, crsql_TableInfo *tblInfo,
                         const char *pkIdentifierList, const char *pkValsStr,
                         const char *insertColName, sqlite3_int64 insertColVrsn,
                         sqlite3_int64 insertDbVrsn, const void *insertSiteId,
                         int insertSiteIdLen) {
  int rc = SQLITE_OK;
  char *zSql = sqlite3_mprintf(
      "INSERT OR REPLACE INTO \"%s__crsql_clock\" \
      (%s, \"__crsql_col_name\", \"__crsql_col_version\", \"__crsql_db_version\", \"__crsql_site_id\")\
      VALUES (\
        %s,\
        %Q,\
        %lld,\
        MAX(crsql_nextdbversion(), %lld),\
        ?\
      )",
      tblInfo->tblName, pkIdentifierList, pkValsStr, insertColName,
      insertColVrsn, insertDbVrsn);

  sqlite3_stmt *pStmt = 0;
  rc = sqlite3_prepare_v2(db, zSql, -1, &pStmt, 0);
  sqlite3_free(zSql);

  if (rc != SQLITE_OK) {
    sqlite3_finalize(pStmt);
    return rc;
  }

  if (insertSiteId == 0) {
    sqlite3_bind_null(pStmt, 1);
  } else {
    sqlite3_bind_blob(pStmt, 1, insertSiteId, insertSiteIdLen,
                      SQLITE_TRANSIENT);
  }

  rc = sqlite3_step(pStmt);
  sqlite3_finalize(pStmt);

  if (rc == SQLITE_DONE) {
    return SQLITE_OK;
  } else {
    return SQLITE_ERROR;
  }
}

int crsql_mergePkOnlyInsert(sqlite3 *db, crsql_TableInfo *tblInfo,
                            const char *pkValsStr, const char *pkIdentifiers,
                            sqlite3_int64 remoteColVersion,
                            sqlite3_int64 remoteDbVersion,
                            const void *remoteSiteId, int remoteSiteIdLen) {
  char *zSql = sqlite3_mprintf("INSERT OR IGNORE INTO \"%s\" (%s) VALUES (%s)",
                               tblInfo->tblName, pkIdentifiers, pkValsStr);
  int rc = sqlite3_exec(db, SET_SYNC_BIT, 0, 0, 0);
  if (rc != SQLITE_OK) {
    sqlite3_free(zSql);
    return rc;
  }

  rc = sqlite3_exec(db, zSql, 0, 0, 0);
  sqlite3_free(zSql);
  sqlite3_exec(db, CLEAR_SYNC_BIT, 0, 0, 0);
  if (rc != SQLITE_OK) {
    return rc;
  }

  // TODO: if insert was ignored, no reason to change clock
  return crsql_setWinnerClock(db, tblInfo, pkIdentifiers, pkValsStr,
                              PKS_ONLY_CID_SENTINEL, remoteColVersion,
                              remoteDbVersion, remoteSiteId, remoteSiteIdLen);
}

int crsql_mergeDelete(sqlite3 *db, crsql_TableInfo *tblInfo,
                      const char *pkWhereList, const char *pkValsStr,
                      const char *pkIdentifiers, sqlite3_int64 remoteColVersion,
                      sqlite3_int64 remoteDbVersion, const void *remoteSiteId,
                      int remoteSiteIdLen) {
  char *zSql = sqlite3_mprintf("DELETE FROM \"%s\" WHERE %s", tblInfo->tblName,
                               pkWhereList);
  int rc = sqlite3_exec(db, SET_SYNC_BIT, 0, 0, 0);
  if (rc != SQLITE_OK) {
    sqlite3_free(zSql);
    return rc;
  }

  rc = sqlite3_exec(db, zSql, 0, 0, 0);
  sqlite3_free(zSql);
  sqlite3_exec(db, CLEAR_SYNC_BIT, 0, 0, 0);
  if (rc != SQLITE_OK) {
    return rc;
  }

  return crsql_setWinnerClock(db, tblInfo, pkIdentifiers, pkValsStr,
                              DELETE_CID_SENTINEL, remoteColVersion,
                              remoteDbVersion, remoteSiteId, remoteSiteIdLen);
}

int crsql_mergeInsert(sqlite3_vtab *pVTab, int argc, sqlite3_value **argv,
                      sqlite3_int64 *pRowid, char **errmsg) {
  // he argv[1] parameter is the rowid of a new row to be inserted into the
  // virtual table. If argv[1] is an SQL NULL, then the implementation must
  // choose a rowid for the newly inserted row
  int rc = 0;
  crsql_Changes_vtab *pTab = (crsql_Changes_vtab *)pVTab;
  sqlite3 *db = pTab->db;
  char *zSql = 0;

  rc = crsql_ensureTableInfosAreUpToDate(db, pTab->pExtData, errmsg);

  if (rc != SQLITE_OK) {
    *errmsg = sqlite3_mprintf("Failed to update crr table information");
    return rc;
  }

  // column values exist in argv[2] and following.
  const int insertTblLen =
      sqlite3_value_bytes(argv[2 + CHANGES_SINCE_VTAB_TBL]);
  if (insertTblLen > MAX_TBL_NAME_LEN) {
    *errmsg = sqlite3_mprintf("crsql - table name exceeded max length");
    return SQLITE_ERROR;
  }
  // safe given we only use this if it exactly matches a table name
  // from tblInfo
  const unsigned char *insertTbl =
      sqlite3_value_text(argv[2 + CHANGES_SINCE_VTAB_TBL]);
  // `splitQuoteConcat` will validate these
  const unsigned char *insertPks =
      sqlite3_value_text(argv[2 + CHANGES_SINCE_VTAB_PK]);

  int inesrtColNameLen = sqlite3_value_bytes(argv[2 + CHANGES_SINCE_VTAB_CID]);
  if (inesrtColNameLen > MAX_TBL_NAME_LEN) {
    *errmsg = sqlite3_mprintf("column name exceeded max length");
    return SQLITE_ERROR;
  }
  const char *insertColName =
      (const char *)sqlite3_value_text(argv[2 + CHANGES_SINCE_VTAB_CID]);

  // `splitQuoteConcat` will validate these -- even tho 1 val should do
  // splitquoteconcat for the validation
  const unsigned char *insertVal =
      sqlite3_value_text(argv[2 + CHANGES_SINCE_VTAB_CVAL]);
  sqlite3_int64 insertColVrsn =
      sqlite3_value_int64(argv[2 + CHANGES_SINCE_VTAB_COL_VRSN]);
  sqlite3_int64 insertDbVrsn =
      sqlite3_value_int64(argv[2 + CHANGES_SINCE_VTAB_DB_VRSN]);

  int insertSiteIdLen =
      sqlite3_value_bytes(argv[2 + CHANGES_SINCE_VTAB_SITE_ID]);
  if (insertSiteIdLen > SITE_ID_LEN) {
    *errmsg = sqlite3_mprintf("crsql - site id exceeded max length");
    return SQLITE_ERROR;
  }
  // safe given we only use siteid via `bind`
  const void *insertSiteId =
      sqlite3_value_blob(argv[2 + CHANGES_SINCE_VTAB_SITE_ID]);

  crsql_TableInfo *tblInfo = crsql_findTableInfo(pTab->pExtData->zpTableInfos,
                                                 pTab->pExtData->tableInfosLen,
                                                 (const char *)insertTbl);

  if (tblInfo == 0) {
    *errmsg = sqlite3_mprintf(
        "crsql - could not find the schema information for table %s",
        insertTbl);
    return SQLITE_ERROR;
  }

  int isDelete = strcmp(DELETE_CID_SENTINEL, insertColName) == 0;
  int isPkOnly = strcmp(PKS_ONLY_CID_SENTINEL, insertColName) == 0;

  char *pkWhereList = crsql_extractWhereList(tblInfo->pks, tblInfo->pksLen,
                                             (const char *)insertPks);
  if (pkWhereList == 0) {
    *errmsg =
        sqlite3_mprintf("crsql - failed decoding primary keys for insert");
    return SQLITE_ERROR;
  }

  rc = crsql_checkForLocalDelete(db, tblInfo->tblName, pkWhereList);
  if (rc == DELETED_LOCALLY) {
    rc = SQLITE_OK;
    // delete wins. we're all done.
    sqlite3_free(pkWhereList);
    return rc;
  }

  // This happens if the state is a delete
  // We must `checkForLocalDelete` prior to merging a delete (happens above).
  // mergeDelete assumes we've already checked for a local delete.
  char *pkValsStr =
      crsql_quoteConcatedValuesAsList((const char *)insertPks, tblInfo->pksLen);
  if (pkValsStr == 0) {
    sqlite3_free(pkWhereList);
    *errmsg = sqlite3_mprintf("Failed sanitizing pk values");
    return SQLITE_ERROR;
  }

  char *pkIdentifierList =
      crsql_asIdentifierList(tblInfo->pks, tblInfo->pksLen, 0);
  if (isDelete) {
    rc = crsql_mergeDelete(db, tblInfo, pkWhereList, pkValsStr,
                           pkIdentifierList, insertColVrsn, insertDbVrsn,
                           insertSiteId, insertSiteIdLen);

    sqlite3_free(pkWhereList);
    sqlite3_free(pkValsStr);
    sqlite3_free(pkIdentifierList);
    return rc;
  }

  if (isPkOnly ||
      !crsql_columnExists(insertColName, tblInfo->nonPks, tblInfo->nonPksLen)) {
    rc = crsql_mergePkOnlyInsert(db, tblInfo, pkValsStr, pkIdentifierList,
                                 insertColVrsn, insertDbVrsn, insertSiteId,
                                 insertSiteIdLen);
    sqlite3_free(pkWhereList);
    sqlite3_free(pkValsStr);
    sqlite3_free(pkIdentifierList);
    return rc;
  }

  char **sanitizedInsertVal =
      crsql_splitQuoteConcat((const char *)insertVal, 1);

  if (sanitizedInsertVal == 0) {
    sqlite3_free(pkValsStr);
    sqlite3_free(pkIdentifierList);
    *errmsg = sqlite3_mprintf("Failed sanitizing value for changeset (%s)",
                              insertVal);
    return SQLITE_ERROR;
  }

  int doesCidWin = crsql_didCidWin(
      db, pTab->pExtData->siteId, tblInfo->tblName, pkWhereList, insertColName,
      sanitizedInsertVal[0], insertColVrsn, errmsg);
  sqlite3_free(pkWhereList);
  if (doesCidWin == -1 || doesCidWin == 0) {
    sqlite3_free(pkValsStr);
    sqlite3_free(pkIdentifierList);
    sqlite3_free(sanitizedInsertVal[0]);
    sqlite3_free(sanitizedInsertVal);
    // doesCidWin == 0? compared against our clocks, nothing wins. OK and
    // Done.
    if (doesCidWin == -1 && *errmsg == 0) {
      *errmsg = sqlite3_mprintf("Failed computing cid win");
    }
    return doesCidWin == 0 ? SQLITE_OK : SQLITE_ERROR;
  }

  zSql = sqlite3_mprintf(
      "INSERT INTO \"%w\" (%s, \"%w\")\
      VALUES (%s, %s)\
      ON CONFLICT DO UPDATE\
      SET \"%w\" = %s",
      tblInfo->tblName, pkIdentifierList, insertColName, pkValsStr,
      sanitizedInsertVal[0], insertColName, sanitizedInsertVal[0]);

  sqlite3_free(sanitizedInsertVal[0]);
  sqlite3_free(sanitizedInsertVal);

  rc = sqlite3_exec(db, SET_SYNC_BIT, 0, 0, errmsg);
  if (rc != SQLITE_OK) {
    sqlite3_free(pkValsStr);
    sqlite3_free(pkIdentifierList);
    sqlite3_exec(db, CLEAR_SYNC_BIT, 0, 0, 0);
    *errmsg = sqlite3_mprintf("Failed setting sync bit");
    return rc;
  }

  rc = sqlite3_exec(db, zSql, 0, 0, errmsg);
  sqlite3_free(zSql);
  sqlite3_exec(db, CLEAR_SYNC_BIT, 0, 0, 0);

  if (rc != SQLITE_OK) {
    sqlite3_free(pkValsStr);
    sqlite3_free(pkIdentifierList);
    *errmsg = sqlite3_mprintf("Failed inserting changeset");
    return rc;
  }

  rc = crsql_setWinnerClock(db, tblInfo, pkIdentifierList, pkValsStr,
                            insertColName, insertColVrsn, insertDbVrsn,
                            insertSiteId, insertSiteIdLen);
  sqlite3_free(pkIdentifierList);
  sqlite3_free(pkValsStr);

  if (rc != SQLITE_OK) {
    *errmsg = sqlite3_mprintf("Failed updating winner clock");
  }

  // TODO: ... this isn't really guaranteed to be unique across
  // the table.
  // Is it fine if we prevent anyone from using `rowid` on a vtab?
  // or must we convert to `without rowid`?
  *pRowid = insertDbVrsn;
  return rc;
}
#include "triggers.h"

#include <stdint.h>
#include <string.h>

#include "consts.h"
#include "tableinfo.h"
#include "util.h"

int crsql_createInsertTrigger(sqlite3 *db, crsql_TableInfo *tableInfo,
                              char **err) {
  char *zSql;
  char *pkList = 0;
  char *pkNewList = 0;
  int rc = SQLITE_OK;
  char *joinedSubTriggers;

  pkList = crsql_asIdentifierList(tableInfo->pks, tableInfo->pksLen, 0);
  pkNewList = crsql_asIdentifierList(tableInfo->pks, tableInfo->pksLen, "NEW.");

  joinedSubTriggers = crsql_insertTriggerQuery(tableInfo, pkList, pkNewList);

  zSql = sqlite3_mprintf(
      "CREATE TRIGGER IF NOT EXISTS \"%s__crsql_itrig\"\
      AFTER INSERT ON \"%s\"\
    BEGIN\
      %s\
    END;",
      tableInfo->tblName, tableInfo->tblName, joinedSubTriggers);

  sqlite3_free(joinedSubTriggers);

  rc = sqlite3_exec(db, zSql, 0, 0, err);
  sqlite3_free(zSql);

  sqlite3_free(pkList);
  sqlite3_free(pkNewList);

  return rc;
}

char *crsql_insertTriggerQuery(crsql_TableInfo *tableInfo, char *pkList,
                               char *pkNewList) {
  const int length = tableInfo->nonPksLen == 0 ? 1 : tableInfo->nonPksLen;
  char **subTriggers = sqlite3_malloc(length * sizeof(char *));
  char *joinedSubTriggers;

  // We need a CREATE_SENTINEL to stand in for the create event so we can
  // replicate PKs If we have a create sentinel how will we insert the created
  // rows without a requirement of nullability on every column? Keep some
  // event data for create that represents the initial state of the row?
  // Future improvement.
  if (tableInfo->nonPksLen == 0) {
    subTriggers[0] = sqlite3_mprintf(
        "INSERT INTO \"%s__crsql_clock\" (\
        %s,\
        __crsql_col_name,\
        __crsql_col_version,\
        __crsql_db_version,\
        __crsql_site_id\
      ) SELECT \
        %s,\
        %Q,\
        1,\
        crsql_nextdbversion(),\
        NULL\
      WHERE crsql_internal_sync_bit() = 0 ON CONFLICT DO UPDATE SET\
        __crsql_col_version = __crsql_col_version + 1,\
        __crsql_db_version = crsql_nextdbversion(),\
        __crsql_site_id = NULL;\n",
        tableInfo->tblName, pkList, pkNewList, PKS_ONLY_CID_SENTINEL);
  }
  for (int i = 0; i < tableInfo->nonPksLen; ++i) {
    subTriggers[i] = sqlite3_mprintf(
        "INSERT INTO \"%s__crsql_clock\" (\
        %s,\
        __crsql_col_name,\
        __crsql_col_version,\
        __crsql_db_version,\
        __crsql_site_id\
      ) SELECT \
        %s,\
        %Q,\
        1,\
        crsql_nextdbversion(),\
        NULL\
      WHERE crsql_internal_sync_bit() = 0 ON CONFLICT DO UPDATE SET\
        __crsql_col_version = __crsql_col_version + 1,\
        __crsql_db_version = crsql_nextdbversion(),\
        __crsql_site_id = NULL;\n",
        tableInfo->tblName, pkList, pkNewList, tableInfo->nonPks[i].name);
  }

  joinedSubTriggers = crsql_join(subTriggers, length);

  for (int i = 0; i < length; ++i) {
    sqlite3_free(subTriggers[i]);
  }
  sqlite3_free(subTriggers);

  return joinedSubTriggers;
}

// TODO (#50): we need to handle the case where someone _changes_ a primary key
// column's value we should:
// 1. detect this
// 2. treat _every_ column as updated
// 3. write a delete sentinel against the _old_ pk combination
//
// 1 is moot.
// 2 is done via changing trigger conditions to: `WHERE sync_bit = 0 AND (NEW.c
// != OLD.c OR NEW.pk_c1 != OLD.pk_c1 OR NEW.pk_c2 != ...) 3 is done with a new
// trigger based on only pks
int crsql_createUpdateTrigger(sqlite3 *db, crsql_TableInfo *tableInfo,
                              char **err) {
  char *zSql;
  char *pkList = 0;
  char *pkNewList = 0;
  int rc = SQLITE_OK;
  const int length = tableInfo->nonPksLen == 0 ? 1 : tableInfo->nonPksLen;
  char **subTriggers = sqlite3_malloc(length * sizeof(char *));
  char *joinedSubTriggers;

  pkList = crsql_asIdentifierList(tableInfo->pks, tableInfo->pksLen, 0);
  pkNewList = crsql_asIdentifierList(tableInfo->pks, tableInfo->pksLen, "NEW.");

  // If we updated a table that _only_ has primary key columns
  // this is the same thing as
  // a
  // 1. delete of the old row
  // followed by
  // 2. create of a new row
  // SQLite already calls the delete trigger for the old row
  // for case 1 so that's covered.
  //
  // TODO: Do we not also need to record a creation event
  // if a pk was changed for a non pk only table?
  if (tableInfo->nonPksLen == 0) {
    subTriggers[0] = sqlite3_mprintf(
        "INSERT INTO \"%s__crsql_clock\" (\
        %s,\
        __crsql_col_name,\
        __crsql_col_version,\
        __crsql_db_version,\
        __crsql_site_id\
      ) SELECT \
        %s,\
        %Q,\
        1,\
        crsql_nextdbversion(),\
        NULL\
      WHERE crsql_internal_sync_bit() = 0 ON CONFLICT DO UPDATE SET\
        __crsql_col_version = __crsql_col_version + 1,\
        __crsql_db_version = crsql_nextdbversion(),\
        __crsql_site_id = NULL;\n",
        tableInfo->tblName, pkList, pkNewList, PKS_ONLY_CID_SENTINEL);
  }

  for (int i = 0; i < tableInfo->nonPksLen; ++i) {
    // updates are conditionally inserted on the new value not being
    // the same as the old value.
    subTriggers[i] = sqlite3_mprintf(
        "INSERT INTO \"%s__crsql_clock\" (\
        %s,\
        __crsql_col_name,\
        __crsql_col_version,\
        __crsql_db_version,\
        __crsql_site_id\
      ) SELECT %s, %Q, 1, crsql_nextdbversion(), NULL WHERE crsql_internal_sync_bit() = 0 AND NEW.\"%w\" != OLD.\"%w\"\
      ON CONFLICT DO UPDATE SET\
        __crsql_col_version = __crsql_col_version + 1,\
        __crsql_db_version = crsql_nextdbversion(),\
        __crsql_site_id = NULL;\n",
        tableInfo->tblName, pkList, pkNewList, tableInfo->nonPks[i].name,
        tableInfo->nonPks[i].name, tableInfo->nonPks[i].name);
  }
  joinedSubTriggers = crsql_join(subTriggers, length);

  for (int i = 0; i < length; ++i) {
    sqlite3_free(subTriggers[i]);
  }
  sqlite3_free(subTriggers);

  zSql = sqlite3_mprintf(
      "CREATE TRIGGER IF NOT EXISTS \"%s__crsql_utrig\"\
      AFTER UPDATE ON \"%s\"\
    BEGIN\
      %s\
    END;",
      tableInfo->tblName, tableInfo->tblName, joinedSubTriggers);

  sqlite3_free(joinedSubTriggers);

  rc = sqlite3_exec(db, zSql, 0, 0, err);
  sqlite3_free(zSql);

  sqlite3_free(pkList);
  sqlite3_free(pkNewList);

  return rc;
}

char *crsql_deleteTriggerQuery(crsql_TableInfo *tableInfo) {
  char *zSql;
  char *pkList = 0;
  char *pkOldList = 0;

  pkList = crsql_asIdentifierList(tableInfo->pks, tableInfo->pksLen, 0);
  pkOldList = crsql_asIdentifierList(tableInfo->pks, tableInfo->pksLen, "OLD.");

  zSql = sqlite3_mprintf(
      "CREATE TRIGGER IF NOT EXISTS \"%s__crsql_dtrig\"\
      AFTER DELETE ON \"%s\"\
    BEGIN\
      INSERT INTO \"%s__crsql_clock\" (\
        %s,\
        __crsql_col_name,\
        __crsql_col_version,\
        __crsql_db_version,\
        __crsql_site_id\
      ) SELECT \
        %s,\
        %Q,\
        1,\
        crsql_nextdbversion(),\
        NULL\
      WHERE crsql_internal_sync_bit() = 0 ON CONFLICT DO UPDATE SET\
      __crsql_col_version = __crsql_col_version + 1,\
      __crsql_db_version = crsql_nextdbversion(),\
      __crsql_site_id = NULL;\
      END; ",
      tableInfo->tblName, tableInfo->tblName, tableInfo->tblName, pkList,
      pkOldList, DELETE_CID_SENTINEL);

  sqlite3_free(pkList);
  sqlite3_free(pkOldList);

  return zSql;
}

int crsql_createDeleteTrigger(sqlite3 *db, crsql_TableInfo *tableInfo,
                              char **err) {
  int rc = SQLITE_OK;

  char *zSql = crsql_deleteTriggerQuery(tableInfo);
  rc = sqlite3_exec(db, zSql, 0, 0, err);
  sqlite3_free(zSql);

  return rc;
}

int crsql_createCrrTriggers(sqlite3 *db, crsql_TableInfo *tableInfo,
                            char **err) {
  int rc = crsql_createInsertTrigger(db, tableInfo, err);
  if (rc == SQLITE_OK) {
    rc = crsql_createUpdateTrigger(db, tableInfo, err);
  }
  if (rc == SQLITE_OK) {
    rc = crsql_createDeleteTrigger(db, tableInfo, err);
  }

  return rc;
}

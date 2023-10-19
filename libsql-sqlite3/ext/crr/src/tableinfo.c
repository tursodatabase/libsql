#include "tableinfo.h"

#include <assert.h>
#include <ctype.h>
#include <stdlib.h>
#include <string.h>

#include "consts.h"
#include "crsqlite.h"
#include "get-table.h"
#include "util.h"

void crsql_freeColumnInfoContents(crsql_ColumnInfo *columnInfo) {
  sqlite3_free(columnInfo->name);
  sqlite3_free(columnInfo->type);
}

static void crsql_freeColumnInfos(crsql_ColumnInfo *columnInfos, int len) {
  if (columnInfos == 0) {
    return;
  }

  int i = 0;
  for (i = 0; i < len; ++i) {
    crsql_freeColumnInfoContents(&columnInfos[i]);
  }

  sqlite3_free(columnInfos);
}

int crsql_numPks(crsql_ColumnInfo *colInfos, int colInfosLen) {
  int ret = 0;
  int i = 0;

  for (i = 0; i < colInfosLen; ++i) {
    if (colInfos[i].pk > 0) {
      ++ret;
    }
  }

  return ret;
}

static int cmpPks(const void *a, const void *b) {
  return (((crsql_ColumnInfo *)a)->pk - ((crsql_ColumnInfo *)b)->pk);
}

crsql_ColumnInfo *crsql_pks(crsql_ColumnInfo *colInfos, int colInfosLen,
                            int *pPksLen) {
  int numPks = crsql_numPks(colInfos, colInfosLen);
  crsql_ColumnInfo *ret = 0;
  int i = 0;
  int j = 0;
  *pPksLen = numPks;

  if (numPks == 0) {
    return 0;
  }

  ret = sqlite3_malloc(numPks * sizeof *ret);
  for (i = 0; i < colInfosLen; ++i) {
    if (colInfos[i].pk > 0) {
      assert(j < numPks);
      ret[j] = colInfos[i];
      ++j;
    }
  }

  qsort(ret, numPks, sizeof(crsql_ColumnInfo), cmpPks);

  assert(j == numPks);
  return ret;
}

crsql_ColumnInfo *crsql_nonPks(crsql_ColumnInfo *colInfos, int colInfosLen,
                               int *pNonPksLen) {
  int nonPksLen = colInfosLen - crsql_numPks(colInfos, colInfosLen);
  crsql_ColumnInfo *ret = 0;
  int i = 0;
  int j = 0;
  *pNonPksLen = nonPksLen;

  if (nonPksLen == 0) {
    return 0;
  }

  ret = sqlite3_malloc(nonPksLen * sizeof *ret);
  for (i = 0; i < colInfosLen; ++i) {
    if (colInfos[i].pk == 0) {
      assert(j < nonPksLen);
      ret[j] = colInfos[i];
      ++j;
    }
  }

  assert(j == nonPksLen);
  return ret;
}

/**
 * Constructs a table info based on the results of pragma
 * statements against the base table.
 */
static crsql_TableInfo *crsql_tableInfo(const char *tblName,
                                        crsql_ColumnInfo *colInfos,
                                        int colInfosLen) {
  crsql_TableInfo *ret = sqlite3_malloc(sizeof *ret);

  ret->baseCols = colInfos;
  ret->baseColsLen = colInfosLen;

  ret->tblName = crsql_strdup(tblName);

  ret->nonPks =
      crsql_nonPks(ret->baseCols, ret->baseColsLen, &(ret->nonPksLen));
  ret->pks = crsql_pks(ret->baseCols, ret->baseColsLen, &(ret->pksLen));

  return ret;
}

/**
 * Given a table name, return the table info that describes that table.
 * TableInfo is a struct that represents the results
 * of pragma_table_info, pragma_index_list, pragma_index_info on a given table
 * and its inidces as well as some extra fields to facilitate crr creation.
 */
int crsql_getTableInfo(sqlite3 *db, const char *tblName,
                       crsql_TableInfo **pTableInfo, char **pErrMsg) {
  char *zSql = 0;
  int rc = SQLITE_OK;
  sqlite3_stmt *pStmt = 0;
  int numColInfos = 0;
  int i = 0;
  crsql_ColumnInfo *columnInfos = 0;

  zSql =
      sqlite3_mprintf("select count(*) from pragma_table_info('%s')", tblName);
  numColInfos = crsql_getCount(db, zSql);
  sqlite3_free(zSql);

  if (numColInfos < 0) {
    *pErrMsg = sqlite3_mprintf("Failed to find columns for crr -- %s", tblName);
    return numColInfos;
  }

  zSql = sqlite3_mprintf(
      "select \"cid\", \"name\", \"type\", \"notnull\", \"pk\" from "
      "pragma_table_info('%s') order by cid asc",
      tblName);
  rc = sqlite3_prepare_v2(db, zSql, -1, &pStmt, 0);
  sqlite3_free(zSql);

  if (rc != SQLITE_OK) {
    *pErrMsg =
        sqlite3_mprintf("Failed to prepare select for crr -- %s", tblName);
    sqlite3_finalize(pStmt);
    return rc;
  }

  rc = sqlite3_step(pStmt);
  if (rc != SQLITE_ROW) {
    *pErrMsg = sqlite3_mprintf("Failed to parse crr definition -- %s", tblName);
    sqlite3_finalize(pStmt);
    return rc;
  }
  columnInfos = sqlite3_malloc(numColInfos * sizeof *columnInfos);
  while (rc == SQLITE_ROW) {
    if (i >= numColInfos) {
      sqlite3_finalize(pStmt);
      for (int j = 0; j < i; ++j) {
        crsql_freeColumnInfoContents(&columnInfos[j]);
      }
      sqlite3_free(columnInfos);
      return SQLITE_ERROR;
    }

    columnInfos[i].cid = sqlite3_column_int(pStmt, 0);

    columnInfos[i].name =
        crsql_strdup((const char *)sqlite3_column_text(pStmt, 1));
    columnInfos[i].type =
        crsql_strdup((const char *)sqlite3_column_text(pStmt, 2));

    columnInfos[i].notnull = sqlite3_column_int(pStmt, 3);
    columnInfos[i].pk = sqlite3_column_int(pStmt, 4);

    ++i;
    rc = sqlite3_step(pStmt);
  }
  sqlite3_finalize(pStmt);

  if (i < numColInfos) {
    for (int j = 0; j < i; ++j) {
      crsql_freeColumnInfoContents(&columnInfos[j]);
    }
    sqlite3_free(columnInfos);
    *pErrMsg = sqlite3_mprintf(
        "Number of fetched columns did not match expected number of "
        "columns");
    return SQLITE_ERROR;
  }

  *pTableInfo = crsql_tableInfo(tblName, columnInfos, numColInfos);

  return SQLITE_OK;
}

void crsql_freeTableInfo(crsql_TableInfo *tableInfo) {
  if (tableInfo == 0) {
    return;
  }
  // baseCols is a superset of all other col arrays
  // and will free their contents.
  crsql_freeColumnInfos(tableInfo->baseCols, tableInfo->baseColsLen);

  // the arrays themselves of course still need freeing
  sqlite3_free(tableInfo->tblName);
  sqlite3_free(tableInfo->pks);
  sqlite3_free(tableInfo->nonPks);

  sqlite3_free(tableInfo);
}

void crsql_freeAllTableInfos(crsql_TableInfo **tableInfos, int len) {
  for (int i = 0; i < len; ++i) {
    crsql_freeTableInfo(tableInfos[i]);
  }
  sqlite3_free(tableInfos);
}

crsql_TableInfo *crsql_findTableInfo(crsql_TableInfo **tblInfos, int len,
                                     const char *tblName) {
  for (int i = 0; i < len; ++i) {
    if (strcmp(tblInfos[i]->tblName, tblName) == 0) {
      return tblInfos[i];
    }
  }

  return 0;
}

int crsql_indexofTableInfo(crsql_TableInfo **tblInfos, int len,
                           const char *tblName) {
  for (int i = 0; i < len; ++i) {
    if (strcmp(tblInfos[i]->tblName, tblName) == 0) {
      return i;
    }
  }

  return -1;
}

sqlite3_int64 crsql_slabRowid(int idx, sqlite3_int64 rowid) {
  if (idx < 0) {
    return -1;
  }

  sqlite3_int64 modulo = rowid % ROWID_SLAB_SIZE;
  return idx * ROWID_SLAB_SIZE + modulo;
}

/**
 * Pulls all table infos for all crrs present in the database.
 * Run once at vtab initialization -- see docs on crsql_Changes_vtab
 * for the constraints this creates.
 */
int crsql_pullAllTableInfos(sqlite3 *db, crsql_TableInfo ***pzpTableInfos,
                            int *rTableInfosLen, char **errmsg) {
  char **zzClockTableNames = 0;
  int rNumCols = 0;
  int rNumRows = 0;
  int rc = SQLITE_OK;

  // Find all clock tables
  rc = crsql_get_table(db, CLOCK_TABLES_SELECT, &zzClockTableNames, &rNumRows,
                       &rNumCols, 0);

  if (rc != SQLITE_OK) {
    *errmsg = sqlite3_mprintf("crsql internal error discovering crr tables.");
    crsql_free_table(zzClockTableNames);
    return SQLITE_ERROR;
  }

  if (rNumRows == 0) {
    crsql_free_table(zzClockTableNames);
    return SQLITE_OK;
  }

  crsql_TableInfo **tableInfos =
      sqlite3_malloc(rNumRows * sizeof(crsql_TableInfo *));
  memset(tableInfos, 0, rNumRows * sizeof(crsql_TableInfo *));
  for (int i = 0; i < rNumRows; ++i) {
    // +1 since tableNames includes a row for column headers
    // Strip __crsql_clock suffix.
    char *baseTableName =
        crsql_strndup(zzClockTableNames[i + 1],
                      strlen(zzClockTableNames[i + 1]) - __CRSQL_CLOCK_LEN);
    rc = crsql_getTableInfo(db, baseTableName, &tableInfos[i], errmsg);
    sqlite3_free(baseTableName);

    if (rc != SQLITE_OK) {
      crsql_free_table(zzClockTableNames);
      crsql_freeAllTableInfos(tableInfos, rNumRows);
      return rc;
    }
  }

  crsql_free_table(zzClockTableNames);

  *pzpTableInfos = tableInfos;
  *rTableInfosLen = rNumRows;

  return SQLITE_OK;
}

int crsql_isTableCompatible(sqlite3 *db, const char *tblName, char **errmsg) {
  // No unique indices besides primary key
  sqlite3_stmt *pStmt = 0;
  char *zSql = sqlite3_mprintf(
      "SELECT count(*) FROM pragma_index_list('%s') WHERE \"origin\" != 'pk' "
      "AND \"unique\" = 1",
      tblName);
  int rc = sqlite3_prepare_v2(db, zSql, -1, &pStmt, 0);
  sqlite3_free(zSql);

  if (rc != SQLITE_OK) {
    *errmsg =
        sqlite3_mprintf("Failed to analyze index information for %s", tblName);
    return 0;
  }

  rc = sqlite3_step(pStmt);
  if (rc == SQLITE_ROW) {
    int count = sqlite3_column_int(pStmt, 0);
    sqlite3_finalize(pStmt);
    if (count != 0) {
      *errmsg = sqlite3_mprintf(
          "Table %s has unique indices besides the primary key. This is "
          "not "
          "allowed for CRRs",
          tblName);
      return 0;
    }
  } else {
    sqlite3_finalize(pStmt);
    return 0;
  }

  // Must have a primary key
  zSql = sqlite3_mprintf(
      // pragma_index_list does not include primary keys that alias rowid...
      // hence why we cannot use `select * from pragma_index_list where origin =
      // pk`
      "SELECT count(*) FROM pragma_table_info('%s') WHERE \"pk\" > 0", tblName);
  rc = sqlite3_prepare_v2(db, zSql, -1, &pStmt, 0);
  sqlite3_free(zSql);

  if (rc != SQLITE_OK) {
    *errmsg = sqlite3_mprintf(
        "Failed to analyze primary key information for %s", tblName);
    return 0;
  }

  rc = sqlite3_step(pStmt);
  if (rc == SQLITE_ROW) {
    int count = sqlite3_column_int(pStmt, 0);
    sqlite3_finalize(pStmt);
    if (count == 0) {
      *errmsg = sqlite3_mprintf(
          "Table %s has no primary key. CRRs must have a primary key", tblName);
      return 0;
    }
  } else {
    sqlite3_finalize(pStmt);
    return 0;
  }

  // No auto-increment primary keys
  zSql =
      "SELECT 1 FROM sqlite_master WHERE name = ? AND type = 'table' AND sql "
      "LIKE '%autoincrement%' limit 1";
  rc = sqlite3_prepare_v2(db, zSql, -1, &pStmt, 0);

  rc += sqlite3_bind_text(pStmt, 1, tblName, -1, SQLITE_STATIC);
  if (rc != SQLITE_OK) {
    *errmsg = sqlite3_mprintf("Failed to analyze autoincrement status for %s",
                              tblName);
    return 0;
  }
  rc = sqlite3_step(pStmt);
  sqlite3_finalize(pStmt);
  if (rc == SQLITE_ROW) {
    *errmsg = sqlite3_mprintf(
        "%s has auto-increment primary keys. This is likely a mistake as two "
        "concurrent nodes will assign unrelated rows the same primary key. "
        "Either use a primary key that represents the identity of your row or "
        "use a database friendly UUID such as UUIDv7",
        tblName);
    return 0;
  } else if (rc != SQLITE_DONE) {
    return 0;
  }

  // No checked foreign key constraints
  zSql = sqlite3_mprintf("SELECT count(*) FROM pragma_foreign_key_list('%s')",
                         tblName);
  rc = sqlite3_prepare_v2(db, zSql, -1, &pStmt, 0);
  sqlite3_free(zSql);

  if (rc != SQLITE_OK) {
    *errmsg = sqlite3_mprintf(
        "Failed to analyze primary key information for %s", tblName);
    return 0;
  }

  rc = sqlite3_step(pStmt);
  if (rc == SQLITE_ROW) {
    int count = sqlite3_column_int(pStmt, 0);
    sqlite3_finalize(pStmt);
    if (count != 0) {
      *errmsg = sqlite3_mprintf(
          "Table %s has checked foreign key constraints. CRRs may have foreign "
          "keys but must not have "
          "checked foreign key constraints as they can be violated by row "
          "level "
          "security or replication.",
          tblName);
      return 0;
    }
  } else {
    sqlite3_finalize(pStmt);
    return 0;
  }

  // check for default value or nullable
  zSql = sqlite3_mprintf(
      "SELECT count(*) FROM pragma_table_xinfo('%s') WHERE \"notnull\" = 1 "
      "AND "
      "\"dflt_value\" IS NULL AND \"pk\" = 0",
      tblName);
  rc = sqlite3_prepare_v2(db, zSql, -1, &pStmt, 0);
  sqlite3_free(zSql);

  if (rc != SQLITE_OK) {
    *errmsg = sqlite3_mprintf(
        "Failed to analyze default value information for %s", tblName);
    return 0;
  }

  rc = sqlite3_step(pStmt);
  if (rc == SQLITE_ROW) {
    int count = sqlite3_column_int(pStmt, 0);
    sqlite3_finalize(pStmt);
    if (count != 0) {
      *errmsg = sqlite3_mprintf(
          "Table %s has a NOT NULL column without a DEFAULT VALUE. This "
          "is not "
          "allowed as it prevents forwards and backwards compatability "
          "between "
          "schema versions. Make the column nullable or assign a default "
          "value "
          "to it.",
          tblName);
      return 0;
    }
  } else {
    sqlite3_finalize(pStmt);
    return 0;
  }

  return 1;
}

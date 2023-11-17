#include "changes-vtab.h"

#include <assert.h>
#include <stdint.h>
#include <string.h>

#include "consts.h"
#include "crsqlite.h"
#include "ext-data.h"
#include "rust.h"
#include "util.h"

int crsql_changes_next(sqlite3_vtab_cursor *cur);

/**
 * Created when the virtual table is initialized.
 * This happens when the vtab is first used in a given connection.
 * The method allocated the crsql_Changes_vtab for use for the duration
 * of the connection.
 */
static int changesConnect(sqlite3 *db, void *pAux, int argc,
                          const char *const *argv, sqlite3_vtab **ppVtab,
                          char **pzErr) {
  crsql_Changes_vtab *pNew;
  int rc;

  rc = sqlite3_declare_vtab(
      db,
      "CREATE TABLE x([table] TEXT NOT NULL, [pk] BLOB NOT NULL, [cid] TEXT "
      "NOT NULL, [val] ANY, [col_version] INTEGER NOT NULL, [db_version] "
      "INTEGER NOT NULL, [site_id] BLOB, [cl] INTEGER NOT NULL, [seq] "
      "INTEGER NOT NULL)");
  if (rc != SQLITE_OK) {
    *pzErr = sqlite3_mprintf("Could not define the table");
    return rc;
  }
  pNew = sqlite3_malloc(sizeof(*pNew));
  *ppVtab = (sqlite3_vtab *)pNew;
  if (pNew == 0) {
    *pzErr = sqlite3_mprintf("Out of memory");
    return SQLITE_NOMEM;
  }
  memset(pNew, 0, sizeof(*pNew));
  pNew->db = db;
  pNew->pExtData = (crsql_ExtData *)pAux;

  rc = crsql_ensureTableInfosAreUpToDate(db, pNew->pExtData,
                                         &(*ppVtab)->zErrMsg);
  if (rc != SQLITE_OK) {
    *pzErr = sqlite3_mprintf("Could not update table infos");
    sqlite3_free(pNew);
    return rc;
  }

  return rc;
}

/**
 * Called when the connection closes to free
 * all resources allocated by `changesConnect`
 *
 * I.e., free everything in `crsql_Changes_vtab` / `pVtab`
 */
static int changesDisconnect(sqlite3_vtab *pVtab) {
  crsql_Changes_vtab *p = (crsql_Changes_vtab *)pVtab;
  // ext data is free by other registered extensions
  sqlite3_free(p);
  return SQLITE_OK;
}

/**
 * Called to allocate a cursor for use in executing a query against
 * the virtual table.
 */
static int changesOpen(sqlite3_vtab *p, sqlite3_vtab_cursor **ppCursor) {
  crsql_Changes_cursor *pCur;
  pCur = sqlite3_malloc(sizeof(*pCur));
  if (pCur == 0) {
    return SQLITE_NOMEM;
  }
  memset(pCur, 0, sizeof(*pCur));
  *ppCursor = &pCur->base;
  pCur->pTab = (crsql_Changes_vtab *)p;
  return SQLITE_OK;
}

static int changesCrsrFinalize(crsql_Changes_cursor *crsr) {
  // Assign pointers to null after freeing
  // since we can get into this twice for the same cursor object.
  int rc = SQLITE_OK;
  rc += sqlite3_finalize(crsr->pChangesStmt);
  crsr->pChangesStmt = 0;
  if (crsr->pRowStmt != 0) {
    rc += sqlite3_clear_bindings(crsr->pRowStmt);
    rc += sqlite3_reset(crsr->pRowStmt);
  }
  crsr->pRowStmt = 0;

  crsr->dbVersion = MIN_POSSIBLE_DB_VERSION;

  return rc;
}

/**
 * Called to reclaim all of the resources allocated in `changesOpen`
 * once a query against the virtual table has completed.
 *
 * We, of course, do not de-allocated the `pTab` reference
 * given `pTab` must persist for the life of the connection.
 *
 * `pChangesStmt` and `pRowStmt` must be finalized.
 *
 * `colVrsns` does not need to be freed as it comes from
 * `pChangesStmt` thus finalizing `pChangesStmt` will
 * release `colVrsnsr`
 */
static int changesClose(sqlite3_vtab_cursor *cur) {
  crsql_Changes_cursor *pCur = (crsql_Changes_cursor *)cur;
  changesCrsrFinalize(pCur);
  sqlite3_free(pCur);
  return SQLITE_OK;
}

/**
 * Invoked to kick off the pulling of rows from the virtual table.
 * Provides the constraints with which the vtab can work with
 * to compute what rows to pull.
 *
 * Provided constraints are filled in by the changesBestIndex method.
 */
int crsql_changes_filter(sqlite3_vtab_cursor *pVtabCursor, int idxNum,
                         const char *idxStr, int argc, sqlite3_value **argv);

/*
** SQLite will invoke this method one or more times while planning a query
** that uses the virtual table.  This routine needs to create
** a query plan for each invocation and compute an estimated cost for that
** plan.
** TODO: should we support `where table` filters?
*/
int crsql_changes_best_index(sqlite3_vtab *tab, sqlite3_index_info *pIdxInfo);

int crsql_changes_update(sqlite3_vtab *pVTab, int argc, sqlite3_value **argv,
                         sqlite3_int64 *pRowid);
// If xBegin is not defined xCommit is not called.
int crsql_changes_begin(sqlite3_vtab *pVTab);
int crsql_changes_commit(sqlite3_vtab *pVTab);
int crsql_changes_rowid(sqlite3_vtab_cursor *cur, sqlite_int64 *pRowid);
int crsql_changes_column(
    sqlite3_vtab_cursor *cur, /* The cursor */
    sqlite3_context *ctx,     /* First argument to sqlite3_result_...() */
    int i                     /* Which column to return */
);
int crsql_changes_eof(sqlite3_vtab_cursor *cur);

sqlite3_module crsql_changesModule = {
    /* iVersion    */ 0,
    /* xCreate     */ 0,
    /* xConnect    */ changesConnect,
    /* xBestIndex  */ crsql_changes_best_index,
    /* xDisconnect */ changesDisconnect,
    /* xDestroy    */ 0,
    /* xOpen       */ changesOpen,
    /* xClose      */ changesClose,
    /* xFilter     */ crsql_changes_filter,
    /* xNext       */ crsql_changes_next,
    /* xEof        */ crsql_changes_eof,
    /* xColumn     */ crsql_changes_column,
    /* xRowid      */ crsql_changes_rowid,
    /* xUpdate     */ crsql_changes_update,
    /* xBegin      */ crsql_changes_begin,
    /* xSync       */ 0,
    /* xCommit     */ crsql_changes_commit,
    /* xRollback   */ 0,
    /* xFindMethod */ 0,
    /* xRename     */ 0,
    /* xSavepoint  */ 0,
    /* xRelease    */ 0,
    /* xRollbackTo */ 0,
    /* xShadowName */ 0};

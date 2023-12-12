/**
 * The changes virtual table is an eponymous virtual table which can be used
 * to fetch and apply patches to a db.
 *
 * To fetch a changeset:
 * ```sql
 * SELECT * FROM crsql_chages WHERE site_id IS NOT SITE_ID AND version > V
 * ```
 *
 * The site id parameter is used to prevent a site from fetching its own
 * changes that were patched into the remote.
 *
 * The version parameter is used to get changes after a specific version.
 * Sites should keep track of the latest version they've received from other
 * sites and use that number as a cursor to fetch future changes.
 *
 * The changes table has the following columns:
 * 1. table - the name of the table the patch is from
 * 2. pk - the primary key(s) that identify the row to be patched. If the
 *    table has many columns that comprise the primary key then
 *    the values are quote concatenated in pk order.
 * 3. col_vals - the values to patch. quote concatenated in cid order.
 * 4. col_versions - the cids of the changed columns and the versions of those
 * columns
 * 5. version - the min version of the patch. Used for filtering and for sites
 * to update their "last seen" version from other sites
 * 6. site_id - the site_id that is responsible for the update. If this is 0
 *    then the update was made locally.
 *
 * To apply a changeset:
 * ```sql
 * INSERT INTO changes (table, pk, col_vals, col_versions, site_id) VALUES
 * (...)
 * ```
 */
#ifndef CHANGES_VTAB_H
#define CHANGES_VTAB_H

#if !defined(SQLITEINT_H)
#include "sqlite3ext.h"
#endif
SQLITE_EXTENSION_INIT3

#include <stdint.h>

#include "crsqlite.h"
#include "ext-data.h"

extern sqlite3_module crsql_changesModule;

/**
 * Data maintained by the virtual table across
 * queries.
 *
 * Per-query data is kept on crsql_Changes_cursor
 */
typedef struct crsql_Changes_vtab crsql_Changes_vtab;
struct crsql_Changes_vtab {
  sqlite3_vtab base;
  sqlite3 *db;

  crsql_ExtData *pExtData;
};

/**
 * Cursor used to return patches.
 * This is instantiated per-query and updated
 * on each row being returned.
 *
 * Contains a reference to the vtab structure in order
 * get a handle on the db which to fetch from
 * the underlying crr tables.
 *
 * Most columns are passed-through from
 * `pChangesStmt` and `pRowStmt` which are stepped
 * in each call to `changesNext`.
 *
 * `colVersion` is copied given it is unclear
 * what the behavior is of calling `sqlite3_column_x` on
 * the same column multiple times with, potentially,
 * different types.
 *
 * `colVersions` is used in the implementation as
 * a text column in order to fetch the correct columns
 * from the physical row.
 *
 * Everything allocated here must be constructed in
 * changesOpen and released in changesCrsrFinalize
 */
#define ROW_TYPE_UPDATE 0
#define ROW_TYPE_DELETE 1
#define ROW_TYPE_PKONLY 2

typedef struct crsql_Changes_cursor crsql_Changes_cursor;
struct crsql_Changes_cursor {
  sqlite3_vtab_cursor base;

  crsql_Changes_vtab *pTab;

  sqlite3_stmt *pChangesStmt;
  sqlite3_stmt *pRowStmt;

  sqlite3_int64 dbVersion;
  int rowType;

  sqlite3_int64 changesRowid;
  int tblInfoIdx;
};

#endif
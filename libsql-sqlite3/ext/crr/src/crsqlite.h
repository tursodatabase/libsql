#ifndef CRSQLITE_H
#define CRSQLITE_H

#include "sqlite3ext.h"
SQLITE_EXTENSION_INIT3

#include <stdint.h>

#include "tableinfo.h"

#ifndef UNIT_TEST
#define STATIC static
#else
#define STATIC
#endif

int crsql_createClockTable(sqlite3 *db, crsql_TableInfo *tableInfo, char **err);
int crsql_backfill_table(sqlite3_context *context, const char *tblName,
                         const char **zpkNames, int pkCount,
                         const char **zNonPkNames, int nonPkCount);
int crsql_is_crr(sqlite3 *db, const char *tblName);

#endif

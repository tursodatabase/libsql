#ifndef CRSQLITE_TRIGGERS_H
#define CRSQLITE_TRIGGERS_H

#include <ctype.h>

#include "crsqlite.h"

int crsql_createCrrTriggers(sqlite3 *db, crsql_TableInfo *tableInfo,
                            char **err);

int crsql_createInsertTrigger(sqlite3 *db, crsql_TableInfo *tableInfo,
                              char **err);

int crsql_createUpdateTrigger(sqlite3 *db, crsql_TableInfo *tableInfo,
                              char **err);

int crsql_createDeleteTrigger(sqlite3 *db, crsql_TableInfo *tableInfo,
                              char **err);
char *crsql_deleteTriggerQuery(crsql_TableInfo *tableInfo);

char *crsql_insertTriggerQuery(crsql_TableInfo *tableInfo, char *pkList,
                               char *pkNewList);
int crsql_remove_crr_triggers_if_exist(sqlite3 *db, const char *tblName);

#endif

#ifdef LIBSQL_ENABLE_BOTTOMLESS_WAL

#include "sqlite3ext.h"
SQLITE_EXTENSION_INIT1
LIBSQL_EXTENSION_INIT1

#include <stdio.h>

extern void bottomless_tracing_init();
extern void bottomless_init();
extern struct libsql_wal_methods* bottomless_methods(struct libsql_wal_methods*);

int sqlite3_bottomless_init(
  sqlite3 *db, 
  char **pzErrMsg, 
  const sqlite3_api_routines *pApi,
  const libsql_api_routines *pLibsqlApi
) {
  // yes, racy
  static int initialized = 0;
  if (initialized == 0) {
    initialized = 1;
  } else {
    return 0;
  }

  SQLITE_EXTENSION_INIT2(pApi);
  LIBSQL_EXTENSION_INIT2(pLibsqlApi);

  bottomless_tracing_init();
  bottomless_init();
  struct libsql_wal_methods *orig = libsql_wal_methods_find(0);
  if (!orig) {
    return SQLITE_ERROR;
  }
  struct libsql_wal_methods *methods = bottomless_methods(orig);

  if (methods) {
    int rc = libsql_wal_methods_register(methods);
    return rc == SQLITE_OK ? SQLITE_OK_LOAD_PERMANENTLY : rc;
  }
  // It's not fatal to fail to instantiate methods - it will be logged.
  return SQLITE_OK_LOAD_PERMANENTLY;
}

int libsqlBottomlessInit(sqlite3 *db) {
  return sqlite3_bottomless_init(db, NULL, NULL, NULL);
}

#endif

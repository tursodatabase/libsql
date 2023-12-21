#include "crsqlite.h"
SQLITE_EXTENSION_INIT1
#ifdef LIBSQL
LIBSQL_EXTENSION_INIT1
#endif

#include <assert.h>
#include <ctype.h>
#include <limits.h>
#include <stdint.h>
#include <string.h>

#include "changes-vtab.h"
#include "consts.h"
#include "ext-data.h"
#include "rust.h"

// see
// https://github.com/chromium/chromium/commit/579b3dd0ea41a40da8a61ab87a8b0bc39e158998
// & https://github.com/rust-lang/rust/issues/73632 &
// https://sourcegraph.com/github.com/chromium/chromium/-/commit/579b3dd0ea41a40da8a61ab87a8b0bc39e158998?visible=1
#ifdef CRSQLITE_WASM
unsigned char __rust_no_alloc_shim_is_unstable;
#endif

int crsql_compact_post_alter(sqlite3 *db, const char *tblName,
                             crsql_ExtData *pExtData, char **errmsg);

static void freeConnectionExtData(void *pUserData) {
  crsql_ExtData *pExtData = (crsql_ExtData *)pUserData;

  crsql_freeExtData(pExtData);
}

static int commitHook(void *pUserData) {
  crsql_ExtData *pExtData = (crsql_ExtData *)pUserData;

  pExtData->dbVersion = pExtData->pendingDbVersion;
  pExtData->pendingDbVersion = -1;
  pExtData->seq = 0;
  pExtData->updatedTableInfosThisTx = 0;
  return SQLITE_OK;
}

static void rollbackHook(void *pUserData) {
  crsql_ExtData *pExtData = (crsql_ExtData *)pUserData;

  pExtData->pendingDbVersion = -1;
  pExtData->seq = 0;
  pExtData->updatedTableInfosThisTx = 0;
}

#ifdef LIBSQL
static void closeHook(void *pUserData, sqlite3 *db) {
  crsql_ExtData *pExtData = (crsql_ExtData *)pUserData;
  crsql_finalize(pExtData);
}
#endif

void *sqlite3_crsqlrustbundle_init(sqlite3 *db, char **pzErrMsg,
                                   const sqlite3_api_routines *pApi);

#ifdef _WIN32
__declspec(dllexport)
#endif
    int sqlite3_crsqlite_init(sqlite3 *db, char **pzErrMsg,
                              const sqlite3_api_routines *pApi
#ifdef LIBSQL
                              ,
                              const libsql_api_routines *pLibsqlApi
#endif
    ) {
  int rc = SQLITE_OK;

  SQLITE_EXTENSION_INIT2(pApi);
#ifdef LIBSQL
  LIBSQL_EXTENSION_INIT2(pLibsqlApi);
#endif

  // TODO: should be moved lower once we finish migrating to rust.
  // RN it is safe here since the rust bundle init is largely just reigstering
  // function pointers. we need to init the rust bundle otherwise sqlite api
  // methods are not isntalled when we start calling rust
  crsql_ExtData *pExtData = sqlite3_crsqlrustbundle_init(db, pzErrMsg, pApi);
  if (pExtData == 0) {
    return SQLITE_ERROR;
  }

  if (rc == SQLITE_OK) {
    rc = sqlite3_create_module_v2(db, "crsql_changes", &crsql_changesModule,
                                  pExtData, 0);
  }

  if (rc == SQLITE_OK) {
#ifdef LIBSQL
    libsql_close_hook(db, closeHook, pExtData);
#endif
    // TODO: get the prior callback so we can call it rather than replace
    // it?
    sqlite3_commit_hook(db, commitHook, pExtData);
    sqlite3_rollback_hook(db, rollbackHook, pExtData);
  }

  return rc;
}
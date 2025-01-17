// Copyright (c) 2023 Anton Zhiyanov, MIT License
// https://github.com/nalgeon/sqlean

// Sqlean extensions bundle.

#include "sqlite3ext.h"
SQLITE_EXTENSION_INIT1

// include most of the extensions,
#include "crypto/extension.h"
#include "define/extension.h"
#include "fileio/extension.h"
#include "fuzzy/extension.h"
#if !defined(_WIN32)
#include "ipaddr/extension.h"
#endif
#include "math/extension.h"
#include "regexp/extension.h"
#include "stats/extension.h"
#include "text/extension.h"
#include "time/extension.h"
#include "unicode/extension.h"
#include "uuid/extension.h"
#include "vsv/extension.h"

#include "sqlean.h"

// Returns the current Sqlean version.
static void sqlean_version(sqlite3_context* context, int argc, sqlite3_value** argv) {
    sqlite3_result_text(context, SQLEAN_VERSION, -1, SQLITE_STATIC);
}

#ifdef _WIN32
__declspec(dllexport)
#endif
    int sqlite3_sqlean_init(sqlite3* db, char** errmsg_ptr, const sqlite3_api_routines* api) {
    (void)errmsg_ptr;
    SQLITE_EXTENSION_INIT2(api);
    static const int flags = SQLITE_UTF8 | SQLITE_INNOCUOUS | SQLITE_DETERMINISTIC;
    sqlite3_create_function(db, "sqlean_version", 0, flags, 0, sqlean_version, 0, 0);
    crypto_init(db);
    define_init(db);
    fileio_init(db);
    fuzzy_init(db);
#if !defined(_WIN32)
    ipaddr_init(db);
#endif
    math_init(db);
    regexp_init(db);
    stats_init(db);
    text_init(db);
#if !defined(_WIN32) || defined(_WIN64)
    time_init(db);
#endif
    unicode_init(db);
    uuid_init(db);
    vsv_init(db);
    return SQLITE_OK;
}

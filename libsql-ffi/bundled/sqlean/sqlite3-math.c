// Originally from SQLite 3.42.0 source code (func.c), Public Domain

// Modified by Anton Zhiyanov, MIT License
// https://github.com/nalgeon/sqlean/

// SQLite math functions.

#include "sqlite3ext.h"
SQLITE_EXTENSION_INIT1

#include "math/extension.h"
#include "sqlean.h"

// Returns the current Sqlean version.
static void sqlean_version(sqlite3_context* context, int argc, sqlite3_value** argv) {
    sqlite3_result_text(context, SQLEAN_VERSION, -1, SQLITE_STATIC);
}

#ifdef _WIN32
__declspec(dllexport)
#endif
    int sqlite3_math_init(sqlite3* db, char** errmsg_ptr, const sqlite3_api_routines* api) {
    (void)errmsg_ptr;
    SQLITE_EXTENSION_INIT2(api);
    static const int flags = SQLITE_UTF8 | SQLITE_INNOCUOUS | SQLITE_DETERMINISTIC;
    sqlite3_create_function(db, "sqlean_version", 0, flags, 0, sqlean_version, 0, 0);
    return math_init(db);
}

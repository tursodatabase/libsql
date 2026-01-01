// Originally by D. Richard Hipp, Public Domain
// https://www.sqlite.org/src/file/ext/misc/fileio.c

// Modified by Anton Zhiyanov, MIT License
// https://github.com/nalgeon/sqlean/

// Read and write files in SQLite.

#include "sqlite3ext.h"
SQLITE_EXTENSION_INIT1

#include "fileio/extension.h"
#include "sqlean.h"

// Returns the current Sqlean version.
static void sqlean_version(sqlite3_context* context, int argc, sqlite3_value** argv) {
    sqlite3_result_text(context, SQLEAN_VERSION, -1, SQLITE_STATIC);
}

#ifdef _WIN32
__declspec(dllexport)
#endif
    int sqlite3_fileio_init(sqlite3* db, char** errmsg_ptr, const sqlite3_api_routines* api) {
    (void)errmsg_ptr;
    SQLITE_EXTENSION_INIT2(api);
    static const int flags = SQLITE_UTF8 | SQLITE_INNOCUOUS | SQLITE_DETERMINISTIC;
    sqlite3_create_function(db, "sqlean_version", 0, flags, 0, sqlean_version, 0, 0);
    return fileio_init(db);
}

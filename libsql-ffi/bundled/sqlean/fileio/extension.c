// Copyright (c) 2023 Anton Zhiyanov, MIT License
// https://github.com/nalgeon/sqlean

// Read and write files in SQLite.

#include "sqlite3ext.h"
SQLITE_EXTENSION_INIT3

#include "fileio/fileio.h"

int fileio_init(sqlite3* db) {
    fileio_scalar_init(db);
    fileio_ls_init(db);
    fileio_scan_init(db);
    return SQLITE_OK;
}

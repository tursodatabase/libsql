// Copyright (c) 2023 Anton Zhiyanov, MIT License
// https://github.com/nalgeon/sqlean

// User-defined functions in SQLite.

#include "sqlite3ext.h"
SQLITE_EXTENSION_INIT3

#include "define/define.h"

int define_init(sqlite3* db) {
    int status = define_manage_init(db);
    define_eval_init(db);
    define_module_init(db);
    return status;
}

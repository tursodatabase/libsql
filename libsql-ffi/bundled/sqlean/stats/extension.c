// Copyright (c) 2023 Anton Zhiyanov, MIT License
// https://github.com/nalgeon/sqlean

// Statistical functions for SQLite.

#include "sqlite3ext.h"
SQLITE_EXTENSION_INIT3

#include "stats/stats.h"

int stats_init(sqlite3* db) {
    stats_scalar_init(db);
    stats_series_init(db);
    return SQLITE_OK;
}

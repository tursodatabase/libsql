// Copyright (c) 2023 Anton Zhiyanov, MIT License
// https://github.com/nalgeon/sqlean

// Statistical functions for SQLite.

#ifndef STATS_INTERNAL_H
#define STATS_INTERNAL_H

#include "sqlite3ext.h"

int stats_scalar_init(sqlite3* db);
int stats_series_init(sqlite3* db);

#endif /* STATS_INTERNAL_H */

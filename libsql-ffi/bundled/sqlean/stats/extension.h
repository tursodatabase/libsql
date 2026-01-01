// Copyright (c) 2023 Anton Zhiyanov, MIT License
// https://github.com/nalgeon/sqlean

// Statistical functions for SQLite.

#ifndef STATS_EXTENSION_H
#define STATS_EXTENSION_H

#include "sqlite3ext.h"

int stats_init(sqlite3* db);

#endif /* STATS_EXTENSION_H */

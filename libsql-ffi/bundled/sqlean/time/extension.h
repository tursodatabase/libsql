// Copyright (c) 2024 Anton Zhiyanov, MIT License
// https://github.com/nalgeon/sqlean

// SQLite extension for working with time.

#ifndef TIME_EXTENSION_H
#define TIME_EXTENSION_H

#include "sqlite3ext.h"

int time_init(sqlite3* db);

#endif /* TIME_EXTENSION_H */

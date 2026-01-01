// Copyright (c) 2023 Anton Zhiyanov, MIT License
// https://github.com/nalgeon/sqlean

// CSV files as virtual tables in SQLite

#ifndef VSV_EXTENSION_H
#define VSV_EXTENSION_H

#include "sqlite3ext.h"

int vsv_init(sqlite3* db);

#endif /* VSV_EXTENSION_H */

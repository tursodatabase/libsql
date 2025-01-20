// Copyright (c) 2023 Anton Zhiyanov, MIT License
// https://github.com/nalgeon/sqlean

// User-defined functions in SQLite.

#ifndef DEFINE_EXTENSION_H
#define DEFINE_EXTENSION_H

#include "sqlite3ext.h"

int define_init(sqlite3* db);

#endif /* DEFINE_EXTENSION_H */

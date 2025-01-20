// Copyright (c) 2023 Anton Zhiyanov, MIT License
// https://github.com/nalgeon/sqlean

// SQLite extension for working with regular expressions.

#ifndef REGEXP_EXTENSION_H
#define REGEXP_EXTENSION_H

#include "sqlite3ext.h"

int regexp_init(sqlite3* db);

#endif /* REGEXP_EXTENSION_H */

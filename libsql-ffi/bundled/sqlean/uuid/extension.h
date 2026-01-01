// Copyright (c) 2023 Anton Zhiyanov, MIT License
// https://github.com/nalgeon/sqlean

// Universally Unique IDentifiers (UUIDs) in SQLite

#ifndef UUID_EXTENSION_H
#define UUID_EXTENSION_H

#include "sqlite3ext.h"

int uuid_init(sqlite3* db);

#endif /* UUID_EXTENSION_H */

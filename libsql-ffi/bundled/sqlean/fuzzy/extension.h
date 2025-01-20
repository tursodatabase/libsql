// Copyright (c) 2023 Anton Zhiyanov, MIT License
// https://github.com/nalgeon/sqlean

// Fuzzy string matching and phonetics.

#ifndef FUZZY_EXTENSION_H
#define FUZZY_EXTENSION_H

#include "sqlite3ext.h"

int fuzzy_init(sqlite3* db);

#endif /* FUZZY_EXTENSION_H */

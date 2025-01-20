// Copyright (c) 2023 Anton Zhiyanov, MIT License
// https://github.com/nalgeon/sqlean

// SQLite extension for working with text.

#ifndef TEXT_EXTENSION_H
#define TEXT_EXTENSION_H

#include "sqlite3ext.h"

int text_init(sqlite3* db);

#endif /* TEXT_EXTENSION_H */

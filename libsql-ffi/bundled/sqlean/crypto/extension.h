// Copyright (c) 2023 Anton Zhiyanov, MIT License
// https://github.com/nalgeon/sqlean

// SQLite hash and encode/decode functions.

#ifndef CRYPTO_EXTENSION_H
#define CRYPTO_EXTENSION_H

#include "sqlite3ext.h"

int crypto_init(sqlite3* db);

#endif /* CRYPTO_EXTENSION_H */

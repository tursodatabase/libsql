// Copyright (c) 2023 Anton Zhiyanov, MIT License
// https://github.com/nalgeon/sqlean

// Read and write files in SQLite.

#ifndef FILEIO_EXTENSION_H
#define FILEIO_EXTENSION_H

#include "sqlite3ext.h"

int fileio_init(sqlite3* db);

#endif /* FILEIO_EXTENSION_H */

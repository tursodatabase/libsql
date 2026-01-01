// Copyright (c) 2023 Anton Zhiyanov, MIT License
// https://github.com/nalgeon/sqlean

// Read and write files in SQLite.

#ifndef FILEIO_INTERNAL_H
#define FILEIO_INTERNAL_H

#include "sqlite3ext.h"

int fileio_ls_init(sqlite3* db);
int fileio_scalar_init(sqlite3* db);
int fileio_scan_init(sqlite3* db);

#endif /* FILEIO_INTERNAL_H */

// Copyright (c) 2023 Anton Zhiyanov, MIT License
// https://github.com/nalgeon/sqlean

// User-defined functions in SQLite.

#ifndef DEFINE_INTERNAL_H
#define DEFINE_INTERNAL_H

#include "sqlite3ext.h"

int define_save_function(sqlite3* db, const char* name, const char* type, const char* body);

int define_eval_init(sqlite3* db);
int define_manage_init(sqlite3* db);
int define_module_init(sqlite3* db);

#endif /* DEFINE_INTERNAL_H */

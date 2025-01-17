// Copyright (c) 2021 Vincent Bernat, MIT License
// https://github.com/nalgeon/sqlean

// IP address manipulation in SQLite.

#ifndef IPADDR_EXTENSION_H
#define IPADDR_EXTENSION_H

#include "sqlite3ext.h"

int ipaddr_init(sqlite3* db);

#endif /* IPADDR_EXTENSION_H */

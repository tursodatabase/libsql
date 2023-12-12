#ifndef CRSQLITE_H
#define CRSQLITE_H

#include "sqlite3ext.h"
SQLITE_EXTENSION_INIT3

#include <stdint.h>

#ifndef UNIT_TEST
#define STATIC static
#else
#define STATIC
#endif

#endif

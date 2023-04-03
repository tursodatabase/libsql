#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <stdint.h>

#include "sqlite3ext.h"
SQLITE_EXTENSION_INIT3

extern "C" int LLVMFuzzerTestOneInput(const uint8_t *Data, size_t Size)
{
    return 0;
}

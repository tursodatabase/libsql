// Copyright (c) 2023 Anton Zhiyanov, MIT License
// https://github.com/nalgeon/sqlean

// UTF-8 characters (runes) <-> C string conversions.

#ifndef RUNES_H
#define RUNES_H

#include <stdint.h>
#include <stdlib.h>

int32_t* runes_from_cstring(const char* const str, size_t length);
char* runes_to_cstring(const int32_t* runes, size_t length);

#endif /* RUNES_H */

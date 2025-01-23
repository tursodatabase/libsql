// Copyright (c) 2023 Anton Zhiyanov, MIT License
// https://github.com/nalgeon/sqlean

// UTF-8 characters (runes) <-> C string conversions.

#include <assert.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>

#include "text/runes.h"
#include "text/utf8/utf8.h"

// runes_from_cstring creates an array of runes from a C string.
int32_t* runes_from_cstring(const char* const str, size_t length) {
    assert(length > 0);
    int32_t* runes = calloc(length, sizeof(int32_t));
    if (runes == NULL) {
        return NULL;
    }

    utf8_decode_t d = {.state = 0};
    const char* s = str;
    size_t idx = 0;
    while (idx < length && *s != 0) {
        do {
            utf8_decode(&d, (uint8_t)*s++);
        } while (d.state);
        runes[idx] = d.codep;
        idx += 1;
    }

    return runes;
}

// runes_to_cstring creates a C string from an array of runes.
char* runes_to_cstring(const int32_t* runes, size_t length) {
    char* str;
    if (length == 0) {
        str = calloc(1, sizeof(char));
        return str;
    }

    size_t maxlen = length * sizeof(int32_t) + 1;
    str = malloc(maxlen);
    if (str == NULL) {
        return NULL;
    }

    char* at = str;
    for (size_t i = 0; i < length; i++) {
        at += utf8_encode(at, runes[i]);
    }
    *at = '\0';
    at += 1;

    if ((size_t)(at - str) < maxlen) {
        // shrink to real size
        size_t size = at - str;
        str = realloc(str, size);
    }
    return str;
}

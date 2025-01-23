// Copyright (c) 2024 Anton Zhiyanov, MIT License
// https://github.com/nalgeon/sqlean

// Case conversion functions for utf8 strings.

#include <stdbool.h>
#include <stdint.h>
#include <string.h>

#include "text/utf8/rune.h"
#include "text/utf8/utf8.h"

// utf8_transform converts the utf8 string s using the transform function.
static bool utf8_transform(char* s, size_t n, uint32_t (*transform)(uint32_t)) {
    utf8_decode_t d = {.state = 0};
    while ((n > 0) & (*s != 0)) {
        size_t i = 0;
        do {
            utf8_decode(&d, (uint8_t)s[i++]);
        } while (d.state);
        uint32_t c = transform(d.codep);
        int len = utf8_encode(s, c);
        if (len == 0) {
            return false;
        }
        s += len;
        n -= len;
    }
    return true;
}

// utf8_tolower converts the utf8 string s to lowercase.
// Returns true if successful, false if an error occurred.
bool utf8_tolower(char* s, size_t n) {
    return utf8_transform(s, n, rune_tolower);
}

// utf8_toupper converts the utf8 string s to uppercase.
bool utf8_toupper(char* s, size_t n) {
    return utf8_transform(s, n, rune_toupper);
}

// utf8_casefold converts the utf8 string s to folded-case.
bool utf8_casefold(char* s, size_t n) {
    return utf8_transform(s, n, rune_casefold);
}

// utf8_totitle converts the utf8 string s to title-case.
bool utf8_totitle(char* s, size_t n) {
    utf8_decode_t d = {.state = 0};
    bool upper = true;
    while ((n > 0) & (*s != 0)) {
        size_t i = 0;
        do {
            utf8_decode(&d, (uint8_t)s[i++]);
        } while (d.state);
        uint32_t c = upper ? rune_toupper(d.codep) : rune_tolower(d.codep);
        int len = utf8_encode(s, c);
        if (len == 0) {
            return false;
        }
        upper = !rune_isword(d.codep);
        s += len;
        n -= len;
    }
    return true;
}

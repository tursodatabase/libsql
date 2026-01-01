// Copyright (c) 2023 Anton Zhiyanov, MIT License
// https://github.com/nalgeon/sqlean

// Rune (UTF-8) string data structure.

#ifndef RSTRING_H
#define RSTRING_H

#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>

// RuneString is a string composed of UTF-8 characters (runes).
typedef struct {
    // array of utf-8 characters
    const int32_t* runes;
    // number of characters in the string
    size_t length;
    // number of bytes in the string
    size_t size;
    // indicates whether the string owns the array
    // and should free the memory when destroyed
    bool owning;
} RuneString;

// RuneString methods.
RuneString rstring_new(void);
RuneString rstring_from_cstring(const char* const utf8str);
char* rstring_to_cstring(RuneString str);
void rstring_free(RuneString str);

int32_t rstring_at(RuneString str, size_t idx);
RuneString rstring_slice(RuneString str, int start, int end);
RuneString rstring_substring(RuneString str, size_t start, size_t length);

int rstring_index(RuneString str, RuneString other);
int rstring_last_index(RuneString str, RuneString other);
bool rstring_like(RuneString pattern, RuneString str);

RuneString rstring_translate(RuneString str, RuneString from, RuneString to);
RuneString rstring_reverse(RuneString str);

RuneString rstring_trim_left(RuneString str, RuneString chars);
RuneString rstring_trim_right(RuneString str, RuneString chars);
RuneString rstring_trim(RuneString str, RuneString chars);
RuneString rstring_pad_left(RuneString str, size_t length, RuneString fill);
RuneString rstring_pad_right(RuneString str, size_t length, RuneString fill);

void rstring_print(RuneString str);

#endif /* RSTRING_H */

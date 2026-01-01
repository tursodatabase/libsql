// Copyright (c) 2023 Anton Zhiyanov, MIT License
// https://github.com/nalgeon/sqlean

// Byte string data structure.

#ifndef BSTRING_H
#define BSTRING_H

#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>

// ByteString is a string composed of bytes.
typedef struct {
    // array of bytes
    const char* bytes;
    // number of bytes in the string
    size_t length;
    // indicates whether the string owns the array
    // and should free the memory when destroyed
    bool owning;
} ByteString;

// ByteString methods.
ByteString bstring_new(void);
ByteString bstring_from_cstring(const char* const cstring, size_t length);
const char* bstring_to_cstring(ByteString str);
void bstring_free(ByteString str);

char bstring_at(ByteString str, size_t idx);
ByteString bstring_slice(ByteString str, int start, int end);
ByteString bstring_substring(ByteString str, size_t start, size_t length);

int bstring_index(ByteString str, ByteString other);
int bstring_last_index(ByteString str, ByteString other);
bool bstring_contains(ByteString str, ByteString other);
bool bstring_equals(ByteString str, ByteString other);
bool bstring_has_prefix(ByteString str, ByteString other);
bool bstring_has_suffix(ByteString str, ByteString other);
size_t bstring_count(ByteString str, ByteString other);

ByteString bstring_split_part(ByteString str, ByteString sep, size_t part);
ByteString bstring_join(ByteString* strings, size_t count, ByteString sep);
ByteString bstring_concat(ByteString* strings, size_t count);
ByteString bstring_repeat(ByteString str, size_t count);

ByteString bstring_replace(ByteString str, ByteString old, ByteString new, size_t max_count);
ByteString bstring_replace_all(ByteString str, ByteString old, ByteString new);
ByteString bstring_reverse(ByteString str);

ByteString bstring_trim_left(ByteString str);
ByteString bstring_trim_right(ByteString str);
ByteString bstring_trim(ByteString str);

void bstring_print(ByteString str);

#endif /* BSTRING_H */

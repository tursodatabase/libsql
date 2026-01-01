// Copyright (c) 2023 Anton Zhiyanov, MIT License
// https://github.com/nalgeon/sqlean

// Byte string data structure.

#include <assert.h>
#include <ctype.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "text/bstring.h"

// bstring_new creates an empty string.
ByteString bstring_new(void) {
    char* bytes = "\0";
    ByteString str = {.bytes = bytes, .length = 0, .owning = false};
    return str;
}

// bstring_from_cstring creates a new string that wraps an existing C string.
ByteString bstring_from_cstring(const char* const cstring, size_t length) {
    ByteString str = {.bytes = cstring, .length = length, .owning = false};
    return str;
}

// bstring_clone creates a new string by copying an existing C string.
static ByteString bstring_clone(const char* const cstring, size_t length) {
    char* bytes = calloc(length + 1, sizeof(char));
    if (bytes == NULL) {
        ByteString str = {NULL, 0, true};
        return str;
    }
    memcpy(bytes, cstring, length);
    ByteString str = {bytes, length, true};
    return str;
}

// bstring_to_cstring converts the string to a zero-terminated C string.
const char* bstring_to_cstring(ByteString str) {
    if (str.bytes == NULL) {
        return NULL;
    }
    return str.bytes;
}

// bstring_free destroys the string, freeing resources if necessary.
void bstring_free(ByteString str) {
    if (str.owning && str.bytes != NULL) {
        free((void*)str.bytes);
    }
}

// bstring_at returns a character by its index in the string.
char bstring_at(ByteString str, size_t idx) {
    if (str.length == 0) {
        return 0;
    }
    if (idx < 0 || idx >= str.length) {
        return 0;
    };
    return str.bytes[idx];
}

// bstring_slice returns a slice of the string,
// from the `start` index (inclusive) to the `end` index (non-inclusive).
// Negative `start` and `end` values count from the end of the string.
ByteString bstring_slice(ByteString str, int start, int end) {
    if (str.length == 0) {
        return bstring_new();
    }

    // adjusted start index
    start = start < 0 ? (int)str.length + start : start;
    // python-compatible: treat negative start index larger than the length of the string as zero
    start = start < 0 ? 0 : start;
    // adjusted start index should be less the the length of the string
    if (start >= (int)str.length) {
        return bstring_new();
    }

    // adjusted end index
    end = end < 0 ? (int)str.length + end : end;
    // python-compatible: treat end index larger than the length of the string
    // as equal to the length
    end = end > (int)str.length ? (int)str.length : end;
    // adjusted end index should be >= 0
    if (end < 0) {
        return bstring_new();
    }

    // adjusted start index should be less than adjusted end index
    if (start >= end) {
        return bstring_new();
    }

    char* at = (char*)str.bytes + start;
    size_t length = end - start;
    ByteString slice = bstring_clone(at, length);
    return slice;
}

// bstring_substring returns a substring of `length` characters,
// starting from the `start` index.
ByteString bstring_substring(ByteString str, size_t start, size_t length) {
    if (length > str.length - start) {
        length = str.length - start;
    }
    return bstring_slice(str, start, start + length);
}

// bstring_contains_after checks if the other string is a substring of the original string,
// starting at the `start` index.
static bool bstring_contains_after(ByteString str, ByteString other, size_t start) {
    if (start + other.length > str.length) {
        return false;
    }
    for (size_t idx = 0; idx < other.length; idx++) {
        if (str.bytes[start + idx] != other.bytes[idx]) {
            return false;
        }
    }
    return true;
}

// bstring_index_char returns the first index of the character in the string
// after the `start` index, inclusive.
static int bstring_index_char(ByteString str, char chr, size_t start) {
    for (size_t idx = start; idx < str.length; idx++) {
        if (str.bytes[idx] == chr) {
            return idx;
        }
    }
    return -1;
}

// bstring_last_index_char returns the last index of the character in the string
// before the `end` index, inclusive.
static int bstring_last_index_char(ByteString str, char chr, size_t end) {
    if (end >= str.length) {
        return -1;
    }
    for (int idx = end; idx >= 0; idx--) {
        if (str.bytes[idx] == chr) {
            return idx;
        }
    }
    return -1;
}

// bstring_index_after returns the index of the substring in the original string
// after the `start` index, inclusive.
static int bstring_index_after(ByteString str, ByteString other, size_t start) {
    if (other.length == 0) {
        return start;
    }
    if (str.length == 0 || other.length > str.length) {
        return -1;
    }

    size_t cur_idx = start;
    while (cur_idx < str.length) {
        int match_idx = bstring_index_char(str, other.bytes[0], cur_idx);
        if (match_idx == -1) {
            return match_idx;
        }
        if (bstring_contains_after(str, other, match_idx)) {
            return match_idx;
        }
        cur_idx = match_idx + 1;
    }
    return -1;
}

// bstring_index returns the first index of the substring in the original string.
int bstring_index(ByteString str, ByteString other) {
    return bstring_index_after(str, other, 0);
}

// bstring_last_index returns the last index of the substring in the original string.
int bstring_last_index(ByteString str, ByteString other) {
    if (other.length == 0) {
        return str.length - 1;
    }
    if (str.length == 0 || other.length > str.length) {
        return -1;
    }

    int cur_idx = str.length - 1;
    while (cur_idx >= 0) {
        int match_idx = bstring_last_index_char(str, other.bytes[0], cur_idx);
        if (match_idx == -1) {
            return match_idx;
        }
        if (bstring_contains_after(str, other, match_idx)) {
            return match_idx;
        }
        cur_idx = match_idx - 1;
    }

    return -1;
}

// bstring_contains checks if the string contains the substring.
bool bstring_contains(ByteString str, ByteString other) {
    return bstring_index(str, other) != -1;
}

// bstring_equals checks if two strings are equal character by character.
bool bstring_equals(ByteString str, ByteString other) {
    if (str.bytes == NULL && other.bytes == NULL) {
        return true;
    }
    if (str.bytes == NULL || other.bytes == NULL) {
        return false;
    }
    if (str.length != other.length) {
        return false;
    }
    return bstring_contains_after(str, other, 0);
}

// bstring_has_prefix checks if the string starts with the `other` substring.
bool bstring_has_prefix(ByteString str, ByteString other) {
    return bstring_index(str, other) == 0;
}

// bstring_has_suffix checks if the string ends with the `other` substring.
bool bstring_has_suffix(ByteString str, ByteString other) {
    if (other.length == 0) {
        return true;
    }
    int idx = bstring_last_index(str, other);
    return idx < 0 ? false : (size_t)idx == (str.length - other.length);
}

// bstring_count counts how many times the `other` substring is contained in the original string.
size_t bstring_count(ByteString str, ByteString other) {
    if (str.length == 0 || other.length == 0 || other.length > str.length) {
        return 0;
    }

    size_t count = 0;
    size_t char_idx = 0;
    while (char_idx < str.length) {
        int match_idx = bstring_index_after(str, other, char_idx);
        if (match_idx == -1) {
            break;
        }
        count += 1;
        char_idx = match_idx + other.length;
    }

    return count;
}

// bstring_split_part splits the string by the separator and returns the nth part (0-based).
ByteString bstring_split_part(ByteString str, ByteString sep, size_t part) {
    if (str.length == 0 || sep.length > str.length) {
        return bstring_new();
    }
    if (sep.length == 0) {
        if (part == 0) {
            return bstring_slice(str, 0, str.length);
        } else {
            return bstring_new();
        }
    }

    size_t found = 0;
    size_t prev_idx = 0;
    size_t char_idx = 0;
    while (char_idx < str.length) {
        int match_idx = bstring_index_after(str, sep, char_idx);
        if (match_idx == -1) {
            break;
        }
        if (found == part) {
            return bstring_slice(str, prev_idx, match_idx);
        }
        found += 1;
        prev_idx = match_idx + sep.length;
        char_idx = match_idx + sep.length;
    }

    if (found == part) {
        return bstring_slice(str, prev_idx, str.length);
    }

    return bstring_new();
}

// bstring_join joins strings using the separator and returns the resulting string.
ByteString bstring_join(ByteString* strings, size_t count, ByteString sep) {
    // calculate total string length
    size_t total_length = 0;
    for (size_t idx = 0; idx < count; idx++) {
        ByteString str = strings[idx];
        total_length += str.length;
        // no separator after the last one
        if (idx != count - 1) {
            total_length += sep.length;
        }
    }

    // allocate memory for the bytes
    size_t total_size = total_length * sizeof(char);
    char* bytes = malloc(total_size + 1);
    if (bytes == NULL) {
        ByteString str = {NULL, 0, false};
        return str;
    }

    // copy bytes from each string with separator in between
    char* at = bytes;
    for (size_t idx = 0; idx < count; idx++) {
        ByteString str = strings[idx];
        memcpy(at, str.bytes, str.length);
        at += str.length;
        if (idx != count - 1 && sep.length != 0) {
            memcpy(at, sep.bytes, sep.length);
            at += sep.length;
        }
    }

    bytes[total_length] = '\0';
    ByteString str = {bytes, total_length, true};
    return str;
}

// bstring_concat concatenates strings and returns the resulting string.
ByteString bstring_concat(ByteString* strings, size_t count) {
    ByteString sep = bstring_new();
    return bstring_join(strings, count, sep);
}

// bstring_repeat concatenates the string to itself a given number of times
// and returns the resulting string.
ByteString bstring_repeat(ByteString str, size_t count) {
    // calculate total string length
    size_t total_length = str.length * count;

    // allocate memory for the bytes
    size_t total_size = total_length * sizeof(char);
    char* bytes = malloc(total_size + 1);
    if (bytes == NULL) {
        ByteString res = {NULL, 0, false};
        return res;
    }

    // copy bytes
    char* at = bytes;
    for (size_t idx = 0; idx < count; idx++) {
        memcpy(at, str.bytes, str.length);
        at += str.length;
    }

    bytes[total_size] = '\0';
    ByteString res = {bytes, total_length, true};
    return res;
}

// bstring_replace replaces the `old` substring with the `new` substring in the original string,
// but not more than `max_count` times.
ByteString bstring_replace(ByteString str, ByteString old, ByteString new, size_t max_count) {
    // count matches of the old string in the source string
    size_t count = bstring_count(str, old);
    if (count == 0) {
        return bstring_slice(str, 0, str.length);
    }

    // limit the number of replacements
    if (max_count >= 0 && count > max_count) {
        count = max_count;
    }

    // k matches split string into (k+1) parts
    // allocate an array for them
    size_t parts_count = count + 1;
    ByteString* strings = malloc(parts_count * sizeof(ByteString));
    if (strings == NULL) {
        ByteString res = {NULL, 0, false};
        return res;
    }

    // split the source string where it matches the old string
    // and fill the strings array with these parts
    size_t part_idx = 0;
    size_t char_idx = 0;
    while (char_idx < str.length && part_idx < count) {
        int match_idx = bstring_index_after(str, old, char_idx);
        if (match_idx == -1) {
            break;
        }
        // slice from the prevoius match to the current match
        strings[part_idx] = bstring_slice(str, char_idx, match_idx);
        part_idx += 1;
        char_idx = match_idx + old.length;
    }
    // "tail" from the last match to the end of the source string
    strings[part_idx] = bstring_slice(str, char_idx, str.length);

    // join all the parts using new string as a separator
    ByteString res = bstring_join(strings, parts_count, new);
    // free string parts
    for (size_t idx = 0; idx < parts_count; idx++) {
        bstring_free(strings[idx]);
    }
    free(strings);
    return res;
}

// bstring_replace_all replaces all `old` substrings with the `new` substrings
// in the original string.
ByteString bstring_replace_all(ByteString str, ByteString old, ByteString new) {
    return bstring_replace(str, old, new, -1);
}

// bstring_reverse returns the reversed string.
ByteString bstring_reverse(ByteString str) {
    ByteString res = bstring_clone(str.bytes, str.length);
    char* bytes = (char*)res.bytes;
    for (size_t i = 0; i < str.length / 2; i++) {
        char r = bytes[i];
        bytes[i] = bytes[str.length - 1 - i];
        bytes[str.length - 1 - i] = r;
    }
    return res;
}

// bstring_trim_left trims whitespaces from the beginning of the string.
ByteString bstring_trim_left(ByteString str) {
    if (str.length == 0) {
        return bstring_new();
    }
    size_t idx = 0;
    for (; idx < str.length; idx++) {
        if (!isspace(str.bytes[idx])) {
            break;
        }
    }
    return bstring_slice(str, idx, str.length);
}

// bstring_trim_right trims whitespaces from the end of the string.
ByteString bstring_trim_right(ByteString str) {
    if (str.length == 0) {
        return bstring_new();
    }
    size_t idx = str.length - 1;
    for (; idx >= 0; idx--) {
        if (!isspace(str.bytes[idx])) {
            break;
        }
    }
    return bstring_slice(str, 0, idx + 1);
}

// bstring_trim trims whitespaces from the beginning and end of the string.
ByteString bstring_trim(ByteString str) {
    if (str.length == 0) {
        return bstring_new();
    }
    size_t left = 0;
    for (; left < str.length; left++) {
        if (!isspace(str.bytes[left])) {
            break;
        }
    }
    size_t right = str.length - 1;
    for (; right >= 0; right--) {
        if (!isspace(str.bytes[right])) {
            break;
        }
    }
    return bstring_slice(str, left, right + 1);
}

// bstring_print prints the string to stdout.
void bstring_print(ByteString str) {
    if (str.bytes == NULL) {
        printf("<null>\n");
        return;
    }
    printf("'%s' (len=%zu)\n", str.bytes, str.length);
}

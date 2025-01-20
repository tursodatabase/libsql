// Copyright (c) 2023 Anton Zhiyanov, MIT License
// https://github.com/nalgeon/sqlean

// Rune (UTF-8) string data structure.

#include <assert.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "text/rstring.h"
#include "text/runes.h"
#include "text/utf8/rune.h"

// utf8_length returns the number of utf-8 characters in a string.
static size_t utf8_length(const char* str) {
    size_t length = 0;

    while (*str != '\0') {
        if (0xf0 == (0xf8 & *str)) {
            // 4-byte utf8 code point (began with 0b11110xxx)
            str += 4;
        } else if (0xe0 == (0xf0 & *str)) {
            // 3-byte utf8 code point (began with 0b1110xxxx)
            str += 3;
        } else if (0xc0 == (0xe0 & *str)) {
            // 2-byte utf8 code point (began with 0b110xxxxx)
            str += 2;
        } else {  // if (0x00 == (0x80 & *s)) {
            // 1-byte ascii (began with 0b0xxxxxxx)
            str += 1;
        }

        // no matter the bytes we marched s forward by, it was
        // only 1 utf8 codepoint
        length++;
    }

    return length;
}

// rstring_new creates an empty string.
RuneString rstring_new(void) {
    RuneString str = {.runes = NULL, .length = 0, .size = 0, .owning = true};
    return str;
}

// rstring_from_runes creates a new string from an array of utf-8 characters.
// `owning` indicates whether the string owns the array and should free the memory when destroyed.
static RuneString rstring_from_runes(const int32_t* const runes, size_t length, bool owning) {
    RuneString str = {
        .runes = runes, .length = length, .size = length * sizeof(int32_t), .owning = owning};
    return str;
}

// rstring_from_cstring creates a new string from a zero-terminated C string.
RuneString rstring_from_cstring(const char* const utf8str) {
    size_t length = utf8_length(utf8str);
    int32_t* runes = length > 0 ? runes_from_cstring(utf8str, length) : NULL;
    return rstring_from_runes(runes, length, true);
}

// rstring_to_cstring converts the string to a zero-terminated C string.
char* rstring_to_cstring(RuneString str) {
    return runes_to_cstring(str.runes, str.length);
}

// rstring_free destroys the string, freeing resources if necessary.
void rstring_free(RuneString str) {
    if (str.owning && str.runes != NULL) {
        free((void*)str.runes);
    }
}

// rstring_at returns a character by its index in the string.
int32_t rstring_at(RuneString str, size_t idx) {
    if (str.length == 0) {
        return 0;
    }
    if (idx < 0 || idx >= str.length) {
        return 0;
    };
    return str.runes[idx];
}

// rstring_slice returns a slice of the string,
// from the `start` index (inclusive) to the `end` index (non-inclusive).
// Negative `start` and `end` values count from the end of the string.
RuneString rstring_slice(RuneString str, int start, int end) {
    if (str.length == 0) {
        return rstring_new();
    }

    // adjusted start index
    start = start < 0 ? (int)str.length + start : start;
    // python-compatible: treat negative start index larger than the length of the string as zero
    start = start < 0 ? 0 : start;
    // adjusted start index should be less the the length of the string
    if (start >= (int)str.length) {
        return rstring_new();
    }

    // adjusted end index
    end = end < 0 ? (int)str.length + end : end;
    // python-compatible: treat end index larger than the length of the string
    // as equal to the length
    end = end > (int)str.length ? (int)str.length : end;
    // adjusted end index should be >= 0
    if (end < 0) {
        return rstring_new();
    }

    // adjusted start index should be less than adjusted end index
    if (start >= end) {
        return rstring_new();
    }

    int32_t* at = (int32_t*)str.runes + start;
    size_t length = end - start;
    RuneString slice = rstring_from_runes(at, length, false);
    return slice;
}

// rstring_substring returns a substring of `length` characters,
// starting from the `start` index.
RuneString rstring_substring(RuneString str, size_t start, size_t length) {
    if (length > str.length - start) {
        length = str.length - start;
    }
    return rstring_slice(str, start, start + length);
}

// rstring_contains_after checks if the other string is a substring of the original string,
// starting at the `start` index.
static bool rstring_contains_after(RuneString str, RuneString other, size_t start) {
    if (start + other.length > str.length) {
        return false;
    }
    for (size_t idx = 0; idx < other.length; idx++) {
        if (str.runes[start + idx] != other.runes[idx]) {
            return false;
        }
    }
    return true;
}

// rstring_index_char returns the first index of the character in the string
// after the `start` index, inclusive.
static int rstring_index_char(RuneString str, int32_t rune, size_t start) {
    for (size_t idx = start; idx < str.length; idx++) {
        if (str.runes[idx] == rune) {
            return idx;
        }
    }
    return -1;
}

// rstring_index_char returns the last index of the character in the string
// before the `end` index, inclusive.
static int rstring_last_index_char(RuneString str, int32_t rune, size_t end) {
    if (end >= str.length) {
        return -1;
    }
    for (int idx = end; idx >= 0; idx--) {
        if (str.runes[idx] == rune) {
            return idx;
        }
    }
    return -1;
}

// rstring_index_after returns the index of the substring in the original string
// after the `start` index, inclusive.
static int rstring_index_after(RuneString str, RuneString other, size_t start) {
    if (other.length == 0) {
        return start;
    }
    if (str.length == 0 || other.length > str.length) {
        return -1;
    }

    size_t cur_idx = start;
    while (cur_idx < str.length) {
        int match_idx = rstring_index_char(str, other.runes[0], cur_idx);
        if (match_idx == -1) {
            return match_idx;
        }
        if (rstring_contains_after(str, other, match_idx)) {
            return match_idx;
        }
        cur_idx = match_idx + 1;
    }
    return -1;
}

// rstring_index returns the first index of the substring in the original string.
int rstring_index(RuneString str, RuneString other) {
    return rstring_index_after(str, other, 0);
}

// rstring_last_index returns the last index of the substring in the original string.
int rstring_last_index(RuneString str, RuneString other) {
    if (other.length == 0) {
        return str.length - 1;
    }
    if (str.length == 0 || other.length > str.length) {
        return -1;
    }

    int cur_idx = str.length - 1;
    while (cur_idx >= 0) {
        int match_idx = rstring_last_index_char(str, other.runes[0], cur_idx);
        if (match_idx == -1) {
            return match_idx;
        }
        if (rstring_contains_after(str, other, match_idx)) {
            return match_idx;
        }
        cur_idx = match_idx - 1;
    }

    return -1;
}

// rstring_like returns true if the string matches a LIKE pattern.
bool rstring_like(RuneString pattern, RuneString str) {
    size_t pidx = 0, sidx = 0, star_idx = SIZE_MAX, match = 0;

    while (sidx < str.length) {
        int32_t prune = (pidx < pattern.length) ? pattern.runes[pidx] : 0;
        int32_t srune = str.runes[sidx];

        if (prune == '%') {
            star_idx = ++pidx;
            match = ++sidx;
            if (pidx == pattern.length) {
                return true;
            }
        } else if (prune == '_' || rune_casefold(prune) == rune_casefold(srune)) {
            pidx++;
            sidx++;
        } else if (star_idx != SIZE_MAX) {
            pidx = star_idx;
            sidx = match++;
        } else {
            return false;
        }
    }

    while (pidx < pattern.length && pattern.runes[pidx] == '%') {
        pidx++;
    }
    return pidx == pattern.length;
}

// rstring_translate replaces each string character that matches a character in the `from` set with
// the corresponding character in the `to` set. If `from` is longer than `to`, occurrences of the
// extra characters in `from` are deleted.
RuneString rstring_translate(RuneString str, RuneString from, RuneString to) {
    if (str.length == 0) {
        return rstring_new();
    }

    // empty mapping, return the original string
    if (from.length == 0) {
        return rstring_from_runes(str.runes, str.length, false);
    }

    // resulting string can be no longer than the original one
    int32_t* runes = calloc(str.length, sizeof(int32_t));
    if (runes == NULL) {
        return rstring_new();
    }

    // but it may be shorter, so we should track its length separately
    size_t length = 0;
    // perform the translation
    for (size_t idx = 0; idx < str.length; idx++) {
        size_t k = 0;
        // map idx-th character in str `from` -> `to`
        for (; k < from.length && k < to.length; k++) {
            if (str.runes[idx] == from.runes[k]) {
                runes[length] = to.runes[k];
                length++;
                break;
            }
        }
        // if `from` is longer than `to`, ingore idx-th character found in `from`
        bool ignore = false;
        for (; k < from.length; k++) {
            if (str.runes[idx] == from.runes[k]) {
                ignore = true;
                break;
            }
        }
        // else copy idx-th character as is
        if (!ignore) {
            runes[length] = str.runes[idx];
            length++;
        }
    }

    return rstring_from_runes(runes, length, true);
}

// rstring_reverse returns the reversed string.
RuneString rstring_reverse(RuneString str) {
    int32_t* runes = (int32_t*)str.runes;
    for (size_t i = 0; i < str.length / 2; i++) {
        int32_t r = runes[i];
        runes[i] = runes[str.length - 1 - i];
        runes[str.length - 1 - i] = r;
    }
    RuneString res = rstring_from_runes(runes, str.length, false);
    return res;
}

// rstring_trim_left trims certain characters from the beginning of the string.
RuneString rstring_trim_left(RuneString str, RuneString chars) {
    if (str.length == 0) {
        return rstring_new();
    }
    size_t idx = 0;
    for (; idx < str.length; idx++) {
        if (rstring_index_char(chars, str.runes[idx], 0) == -1) {
            break;
        }
    }
    return rstring_slice(str, idx, str.length);
}

// rstring_trim_right trims certain characters from the end of the string.
RuneString rstring_trim_right(RuneString str, RuneString chars) {
    if (str.length == 0) {
        return rstring_new();
    }
    int idx = str.length - 1;
    for (; idx >= 0; idx--) {
        if (rstring_index_char(chars, str.runes[idx], 0) == -1) {
            break;
        }
    }
    return rstring_slice(str, 0, idx + 1);
}

// rstring_trim trims certain characters from the beginning and end of the string.
RuneString rstring_trim(RuneString str, RuneString chars) {
    if (str.length == 0) {
        return rstring_new();
    }
    size_t left = 0;
    for (; left < str.length; left++) {
        if (rstring_index_char(chars, str.runes[left], 0) == -1) {
            break;
        }
    }
    int right = str.length - 1;
    for (; right >= 0; right--) {
        if (rstring_index_char(chars, str.runes[right], 0) == -1) {
            break;
        }
    }
    return rstring_slice(str, left, right + 1);
}

// rstring_pad_left pads the string to the specified length by prepending `fill` characters.
// If the string is already longer than the specified length, it is truncated on the right.
RuneString rstring_pad_left(RuneString str, size_t length, RuneString fill) {
    if (str.length >= length) {
        // If the string is already longer than length, return a truncated version of the string
        return rstring_substring(str, 0, length);
    }

    if (fill.length == 0) {
        // If the fill string is empty, return the original string
        return rstring_from_runes(str.runes, str.length, false);
    }

    // Calculate the number of characters to pad
    size_t pad_langth = length - str.length;

    // Allocate memory for the padded string
    size_t new_size = (str.length + pad_langth) * sizeof(int32_t);
    int32_t* new_runes = malloc(new_size);
    if (new_runes == NULL) {
        return rstring_new();
    }

    // Copy the fill characters to the beginning of the new string
    for (size_t i = 0; i < pad_langth; i++) {
        new_runes[i] = fill.runes[i % fill.length];
    }

    // Copy the original string to the end of the new string
    memcpy(&new_runes[pad_langth], str.runes, str.size);

    // Return the new string
    RuneString new_str = rstring_from_runes(new_runes, length, true);
    return new_str;
}

// rstring_pad_right pads the string to the specified length by appending `fill` characters.
// If the string is already longer than the specified length, it is truncated on the right.
RuneString rstring_pad_right(RuneString str, size_t length, RuneString fill) {
    if (str.length >= length) {
        // If the string is already longer than length, return a truncated version of the string
        return rstring_substring(str, 0, length);
    }

    if (fill.length == 0) {
        // If the fill string is empty, return the original string
        return rstring_from_runes(str.runes, str.length, false);
    }

    // Calculate the number of characters to pad
    size_t pad_length = length - str.length;

    // Allocate memory for the padded string
    size_t new_size = (str.length + pad_length) * sizeof(int32_t);
    int32_t* new_runes = malloc(new_size);
    if (new_runes == NULL) {
        return rstring_new();
    }

    // Copy the original string to the beginning of the new string
    memcpy(new_runes, str.runes, str.size);

    // Copy the fill characters to the end of the new string
    for (size_t i = str.length; i < length; i++) {
        new_runes[i] = fill.runes[(i - str.length) % fill.length];
    }

    // Return the new string
    RuneString new_str = rstring_from_runes(new_runes, length, true);
    return new_str;
}

// rstring_print prints the string to stdout.
void rstring_print(RuneString str) {
    if (str.length == 0) {
        printf("'' (len=0)\n");
        return;
    }
    printf("'");
    for (size_t i = 0; i < str.length; i++) {
        printf("%08x ", str.runes[i]);
    }
    printf("' (len=%zu)", str.length);
    printf("\n");
}

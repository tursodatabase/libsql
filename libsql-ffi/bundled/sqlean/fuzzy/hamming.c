// Copyright (c) 2014 Ross Bayer, MIT License
// https://github.com/Rostepher/libstrcmp

#include <assert.h>
#include <stddef.h>
#include <string.h>

#include "fuzzy/common.h"

/// Computes and returns the hamming distance between two strings. Both strings
/// must have the same length and not be NULL. More information about the
/// algorithm can be found here:
///     http://en.wikipedia.org/wiki/Hamming_distance
///
/// @param str1 first non NULL string
/// @param str2 second non NULL string
///
/// @returns hamming distance or -1 if str1 and st2 did not have the same
///     length or if one or both str1 and str2 were NULL
int hamming(const char* str1, const char* str2) {
    // strings cannot be NULL
    assert(str1 != NULL);
    assert(str2 != NULL);

    size_t str1_len = strlen(str1);
    size_t str2_len = strlen(str2);

    // handle cases where strings have different lengths
    if (str1_len != str2_len) {
        return -1;
    }

    // return 0 if strings are both empty, but not NULL
    if (str1_len == 0 && str2_len == 0) {
        return 0;
    }

    int dist = 0;
    while (str1_len > 0 && str2_len > 0) {
        dist += (NOT_EQ(*str1, *str2));
        str1++, str2++;
        str1_len--, str2_len--;
    }

    return dist;
}

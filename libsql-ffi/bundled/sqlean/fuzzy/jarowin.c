// Copyright (c) 2014 Ross Bayer, MIT License
// https://github.com/Rostepher/libstrcmp

#include <assert.h>
#include <stdbool.h>
#include <stdlib.h>
#include <string.h>

#include "fuzzy/common.h"

/// Calculates and returns the Jaro distance of two non NULL strings.
/// More information about the algorithm can be found here:
///     http://en.wikipedia.org/wiki/Jaro-Winkler_distance
///
/// @param str1 first non NULL string
/// @param str2 second non NULL string
///
/// @returns the jaro distance of str1 and str2
double jaro(const char* str1, const char* str2) {
    // strings cannot be NULL
    assert(str1 != NULL);
    assert(str2 != NULL);

    int str1_len = strlen(str1);
    int str2_len = strlen(str2);

    // if both strings are empty return 1
    // if only one of the strings is empty return 0
    if (str1_len == 0) {
        return (str2_len == 0) ? 1.0 : 0.0;
    }

    // max distance between two chars to be considered matching
    // floor() is ommitted due to integer division rules
    int match_dist = (int)MAX(str1_len, str2_len) / 2 - 1;

    // arrays of bools that signify if that char in the matcing string has a
    // match
    int* str1_matches = calloc(str1_len, sizeof(int));
    int* str2_matches = calloc(str2_len, sizeof(int));

    // number of matches and transpositions
    double matches = 0.0;
    double trans = 0.0;

    // find the matches
    for (int i = 0; i < str1_len; i++) {
        // start and end take into account the match distance
        int start = MAX(0, i - match_dist);
        int end = MIN(i + match_dist + 1, str2_len);

        for (int k = start; k < end; k++) {
            // if str2 already has a match or str1 and str2 are not equal
            // continue
            if (str2_matches[k] || NOT_EQ(str1[i], str2[k])) {
                continue;
            }

            // otherwise assume there is a match
            str1_matches[i] = true;
            str2_matches[k] = true;
            matches++;
            break;
        }
    }

    // if there are no matches return 0
    if (matches == 0) {
        free(str1_matches);
        free(str2_matches);
        return 0.0;
    }

    // count transpositions
    int k = 0;
    for (int i = 0; i < str1_len; i++) {
        // if there are no matches in str1 continue
        if (!str1_matches[i]) {
            continue;
        }

        // while there is no match in str2 increment k
        while (!str2_matches[k]) {
            k++;
        }

        // increment trans
        if (NOT_EQ(str1[i], str2[k])) {
            trans++;
        }

        k++;
    }

    // divide the number of transpositions by two as per the algorithm specs
    // this division is valid because the counted transpositions include both
    // instances of the transposed characters.
    trans /= 2.0;

    // free allocated memory
    free(str1_matches);
    free(str2_matches);

    // return the jaro distance
    return ((matches / str1_len) + (matches / str2_len) + ((matches - trans) / matches)) / 3.0;
}

/// Calculates and returns the Jaro-Winkler distance of two non NULL strings.
/// More information about the algorithm can be found here:
///     http://en.wikipedia.org/wiki/Jaro-Winkler_distance
///
/// @param str1 first non NULL string
/// @param str2 second non NULL string
///
/// @returns the jaro-winkler distance of str1 and str2
double jaro_winkler(const char* str1, const char* str2) {
    // strings cannot be NULL
    assert(str1 != NULL);
    assert(str2 != NULL);

    // compute the jaro distance
    double dist = jaro(str1, str2);

    // finds the number of common terms in the first 3 strings, max 3.
    int prefix_length = 0;
    if (strlen(str1) != 0 && strlen(str2) != 0) {
        while (prefix_length < 3 && EQ(*str1++, *str2++)) {
            prefix_length++;
        }
    }

    // 0.1 is the default scaling factor
    return dist + prefix_length * 0.1 * (1 - dist);
}

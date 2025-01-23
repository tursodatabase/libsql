// Copyright (c) 2014 Ross Bayer, MIT License
// https://github.com/Rostepher/libstrcmp

#include <assert.h>
#include <stdlib.h>
#include <string.h>

#include "fuzzy/common.h"

/// Calculates and returns the Levenshtein distance of two non NULL strings.
/// More information about the algorithm can be found here:
///     https://en.wikipedia.org/wiki/Levenshtein_distance
///
/// @param str1 first non NULL string
/// @param str2 second non NULL string
///
/// @returns the levenshtein distance of str1 and str2
unsigned levenshtein(const char* str1, const char* str2) {
    // strings cannot be NULL
    assert(str1 != NULL);
    assert(str2 != NULL);

    size_t str1_len = strlen(str1);
    size_t str2_len = strlen(str2);

    // handle cases where one or both strings are empty
    if (str1_len == 0) {
        return str2_len;
    }
    if (str2_len == 0) {
        return str1_len;
    }

    // remove common substring
    while (str1_len > 0 && str2_len > 0 && EQ(str1[0], str2[0])) {
        str1++, str2++;
        str1_len--, str2_len--;
    }

    // declare variables
    unsigned row, col;
    unsigned last_diag = 0, cur, cost;

    // initialize array to hold values
    unsigned* vector = calloc(str1_len + 1, sizeof(unsigned));
    for (col = 1; col <= str1_len; col++) {
        vector[col] = col;
    }

    // itterate through the imagined rows of arrays
    for (row = 1; row <= str2_len + 1; row++) {
        vector[0] = row;
        last_diag = row - 1;  // remember the last first slot

        // itterate throught each member of the vector
        for (col = 1; col <= str1_len; col++) {
            // remember the diagonal before overwriting the array
            cur = vector[col];

            // calculate the cost
            cost = EQ(str1[col - 1], str2[row - 1]) ? 0 : 1;

            // determine min of the possible values
            vector[col] = MIN3(vector[col] + 1, vector[col - 1] + 1, last_diag + cost);

            // remember the new last_diag
            last_diag = cur;
        }
    }

    free(vector);
    return last_diag;
}

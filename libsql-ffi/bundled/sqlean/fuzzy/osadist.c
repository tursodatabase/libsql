// Copyright (c) 2014 Ross Bayer, MIT License
// https://github.com/Rostepher/libstrcmp

#include <assert.h>
#include <stdlib.h>
#include <string.h>

#include "fuzzy/common.h"

/// Computes and returns the Optimal String Alignment distance for two non NULL
/// strings. More information about the algorithm can be found here:
///     https://en.wikipedia.org/wiki/Damerau-Levenshtein_distance
///
/// @param str1 first non NULL string
/// @param str2 second non NULL string
///
/// @returns optimal string alignment distance for str1 and str2
unsigned optimal_string_alignment(const char* str1, const char* str2) {
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

    unsigned row, col, cost, result;

    // initialize matrix to hold distance values
    unsigned** matrix = malloc((str1_len + 1) * sizeof(unsigned*));
    for (unsigned i = 0; i <= str1_len; i++) {
        matrix[i] = calloc((str2_len + 1), sizeof(unsigned));
    }

    // set all the starting values
    matrix[0][0] = 0;
    for (row = 1; row <= str1_len; row++) {
        matrix[row][0] = row;
    }
    for (col = 1; col <= str2_len; col++) {
        matrix[0][col] = col;
    }

    // itterate through and fill in the matrix
    for (row = 1; row <= str1_len; row++) {
        for (col = 1; col <= str2_len; col++) {
            cost = EQ(str1[row - 1], str2[col - 1]) ? 0 : 1;

            matrix[row][col] = MIN3(matrix[row - 1][col] + 1,        // deletion
                                    matrix[row][col - 1] + 1,        // insertion
                                    matrix[row - 1][col - 1] + cost  // substitution
            );

            // transpositions
            if (row > 1 && col > 1 && EQ(str1[row], str2[col - 1]) &&
                EQ(str1[row - 1], str2[col])) {
                matrix[row][col] = MIN(matrix[row][col], matrix[row - 2][col - 2] + cost);
            }
        }
    }

    result = matrix[str1_len][str2_len];

    // free allocated memory
    for (unsigned i = 0; i < str1_len + 1; i++) {
        free(matrix[i]);
    }
    free(matrix);

    return result;
}

// Copyright (c) 2014 Ross Bayer, MIT License
// https://github.com/Rostepher/libstrcmp

#include <assert.h>
#include <stdlib.h>
#include <string.h>

#include "fuzzy/common.h"

/// Calculates and returns the Damerau-Levenshtein distance of two non NULL
/// strings. More information about the algorithm can be found here:
///     https://en.wikipedia.org/wiki/Damerau-Levenshtein_distance
///
/// @param str1 first non NULL string
/// @param str2 second non NULL string
///
/// @returns Damerau-Levenshtein distance of str1 and str2
unsigned damerau_levenshtein(const char* str1, const char* str2) {
    // strings cannot be NULL
    assert(str1 != NULL);
    assert(str2 != NULL);

    // size of the alphabet
    const unsigned alpha_size = 255;

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

    const unsigned INFINITY = str1_len + str2_len;
    unsigned row, col;

    // create "dictionary"
    unsigned* dict = calloc(alpha_size, sizeof(unsigned));

    size_t m_rows = str1_len + 2;  // matrix rows
    size_t m_cols = str2_len + 2;  // matrix cols

    // matrix to hold computed values
    unsigned** matrix = malloc(m_rows * sizeof(unsigned*));
    for (unsigned i = 0; i < m_rows; i++) {
        matrix[i] = calloc(m_cols, sizeof(unsigned));
    }

    // set all the starting values and add all characters to the dict
    matrix[0][0] = INFINITY;
    for (row = 1; row < m_rows; row++) {
        matrix[row][0] = INFINITY;
        matrix[row][1] = row - 1;
    }
    for (col = 1; col < m_cols; col++) {
        matrix[0][col] = INFINITY;
        matrix[1][col] = col - 1;
    }

    unsigned db;
    unsigned i, k;
    unsigned cost;

    // fill in the matrix
    for (row = 1; row <= str1_len; row++) {
        db = 0;

        for (col = 1; col <= str2_len; col++) {
            i = dict[(unsigned)str2[col - 1]];
            k = db;
            cost = EQ(str1[row - 1], str2[col - 1]) ? 0 : 1;

            if (cost == 0) {
                db = col;
            }

            matrix[row + 1][col + 1] =
                MIN4(matrix[row][col] + cost, matrix[row + 1][col] + 1, matrix[row][col + 1] + 1,
                     matrix[i][k] + (row - i - 1) + (col - k - 1) + 1);
        }

        dict[(unsigned)str1[row - 1]] = row;
    }

    unsigned result = matrix[m_rows - 1][m_cols - 1];

    // free allocated memory
    free(dict);
    for (unsigned i = 0; i < m_rows; i++) {
        free(matrix[i]);
    }
    free(matrix);

    return result;
}

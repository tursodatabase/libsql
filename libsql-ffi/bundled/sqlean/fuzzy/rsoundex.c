// Copyright (c) 2014 Ross Bayer, MIT License
// https://github.com/Rostepher/libstrcmp

#include <assert.h>
#include <ctype.h>
#include <stdlib.h>
#include <string.h>

#include "fuzzy/common.h"

/// Helper function that returns the numeric code for a given char as specified
/// by the refined soundex algorithm.
///
/// @param c char to encode
///
/// @returns char representation of the number associated with the given char
static char rsoundex_encode(const char c) {
    switch (tolower(c)) {
        case 'b':
        case 'p':
            return '1';

        case 'f':
        case 'v':
            return '2';

        case 'c':
        case 'k':
        case 's':
            return '3';

        case 'g':
        case 'j':
            return '4';

        case 'q':
        case 'x':
        case 'z':
            return '5';

        case 'd':
        case 't':
            return '6';

        case 'l':
            return '7';

        case 'm':
        case 'n':
            return '8';

        case 'r':
            return '9';

        default:
            break;
    }

    return '0';
}

/// Computes and returns the soundex representation of a given non NULL string.
/// More information about the algorithm can be found here:
///     http://ntz-develop.blogspot.com/2011/03/phonetic-algorithms.html
///
/// @param str non NULL string to encode
///
/// @returns soundex representation of str
char* refined_soundex(const char* str) {
    // string cannot be NULL
    assert(str != NULL);

    size_t str_len = strlen(str);

    // final code buffer
    char* code = malloc((str_len + 1) * sizeof(char));

    // temporary buffer to encode string
    char* buf = malloc((str_len + 1) * sizeof(char));

    // set first value to first char in str
    code[0] = toupper(str[0]);

    // number of digits in code
    unsigned d = 1;

    // encode all chars in str
    for (unsigned i = 0; i < str_len; i++)
        buf[i] = rsoundex_encode(str[i]);

    // add all viable chars to code
    char prev = '\0';
    for (unsigned i = 0; i < str_len; i++) {
        // check if current char in buf is not the same as previous char
        if (NOT_EQ(buf[i], prev)) {
            // add digit to the code
            code[d] = buf[i];

            // increment digit counter
            d++;

            // set prev to current char
            prev = buf[i];
        }
    }

    // allocate space for final code
    // d will be length of the code + 1
    char* result = malloc((d + 1) * sizeof(char));

    // copy final code into result and null terminate
    for (unsigned i = 0; i < d; i++) {
        result[i] = code[i];
    }
    result[d] = '\0';

    free(code);
    free(buf);

    return result;
}

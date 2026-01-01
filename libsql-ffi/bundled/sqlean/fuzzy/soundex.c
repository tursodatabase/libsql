// Copyright (c) 2014 Ross Bayer, MIT License
// https://github.com/Rostepher/libstrcmp

#include <assert.h>
#include <ctype.h>
#include <stdlib.h>
#include <string.h>

#include "fuzzy/common.h"

/// Helper function that returns the numeric code for a given char as specified
/// by the soundex algorithm.
///
/// @param c char to encode
///
/// @returns char representation of the number associated with the given char
static char soundex_encode(const char c) {
    switch (tolower(c)) {
        case 'b':
        case 'f':
        case 'p':
        case 'v':
            return '1';

        case 'c':
        case 'g':
        case 'j':
        case 'k':
        case 'q':
        case 's':
        case 'x':
        case 'z':
            return '2';

        case 'd':
        case 't':
            return '3';

        case 'l':
            return '4';

        case 'm':
        case 'n':
            return '5';

        case 'r':
            return '6';

        default:
            break;
    }

    return '0';
}

/// Computes and returns the soundex representation of a given non NULL string.
/// More information about the algorithm can be found here:
///     https://en.wikipedia.org/wiki/Soundex
///
/// @param str non NULL string to encode
///
/// @returns soundex representation of str
char* soundex(const char* str) {
    // string cannot be NULL
    assert(str != NULL);

    size_t str_len = strlen(str);

    // allocate space for final code and null terminator
    char* code = malloc(5 * sizeof(char));

    // temporary buffer to encode string
    char* buf = malloc((str_len + 1) * sizeof(char));

    // set first value to first char in str
    code[0] = toupper(str[0]);

    // number of digits in code
    unsigned d = 1;

    // encode all chars in str
    for (unsigned i = 0; i < str_len; i++) {
        buf[i] = soundex_encode(str[i]);
    }

    // add all viable chars to code
    for (unsigned i = 1; i < str_len && d < 4; i++) {
        // check if current char in buf is not the same as previous char
        // and that the current char is not '0'
        if (NOT_EQ(buf[i], buf[i - 1]) && NOT_EQ(buf[i], '0')) {
            // if digits separated by an 'h' or 'w' are the same, skip them
            if (i > 1 && EQ(buf[i], buf[i - 2]) && strchr("hw", str[i - 1])) {
                continue;
            }

            // add digit to the code
            code[d] = buf[i];

            // increment digit counter
            d++;
        }
    }

    // pad the end of code with '0' if too short
    while (d < 4) {
        code[d] = '0';
        d++;
    }

    // null terminate string
    code[d] = '\0';
    free(buf);

    return code;
}

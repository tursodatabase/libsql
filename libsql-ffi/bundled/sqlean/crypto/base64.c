// Copyright (c) 2023 Anton Zhiyanov, MIT License
// https://github.com/nalgeon/sqlean

// Base64 encoding/decoding (RFC 4648)

#include <stdint.h>
#include <stdlib.h>
#include <string.h>

static const char base64_chars[] =
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

uint8_t* base64_encode(const uint8_t* src, size_t len, size_t* out_len) {
    uint8_t* encoded = NULL;
    size_t i, j;
    uint32_t octets;

    *out_len = ((len + 2) / 3) * 4;
    encoded = malloc(*out_len + 1);
    if (encoded == NULL) {
        *out_len = 0;
        return NULL;
    }

    for (i = 0, j = 0; i < len; i += 3, j += 4) {
        octets =
            (src[i] << 16) | ((i + 1 < len ? src[i + 1] : 0) << 8) | (i + 2 < len ? src[i + 2] : 0);
        encoded[j] = base64_chars[(octets >> 18) & 0x3f];
        encoded[j + 1] = base64_chars[(octets >> 12) & 0x3f];
        encoded[j + 2] = base64_chars[(octets >> 6) & 0x3f];
        encoded[j + 3] = base64_chars[octets & 0x3f];
    }

    if (len % 3 == 1) {
        encoded[*out_len - 1] = '=';
        encoded[*out_len - 2] = '=';
    } else if (len % 3 == 2) {
        encoded[*out_len - 1] = '=';
    }

    encoded[*out_len] = '\0';
    return encoded;
}

static const uint8_t base64_table[] = {
    // Map base64 characters to their corresponding values
    ['A'] = 0,  ['B'] = 1,  ['C'] = 2,  ['D'] = 3,  ['E'] = 4,  ['F'] = 5,  ['G'] = 6,  ['H'] = 7,
    ['I'] = 8,  ['J'] = 9,  ['K'] = 10, ['L'] = 11, ['M'] = 12, ['N'] = 13, ['O'] = 14, ['P'] = 15,
    ['Q'] = 16, ['R'] = 17, ['S'] = 18, ['T'] = 19, ['U'] = 20, ['V'] = 21, ['W'] = 22, ['X'] = 23,
    ['Y'] = 24, ['Z'] = 25, ['a'] = 26, ['b'] = 27, ['c'] = 28, ['d'] = 29, ['e'] = 30, ['f'] = 31,
    ['g'] = 32, ['h'] = 33, ['i'] = 34, ['j'] = 35, ['k'] = 36, ['l'] = 37, ['m'] = 38, ['n'] = 39,
    ['o'] = 40, ['p'] = 41, ['q'] = 42, ['r'] = 43, ['s'] = 44, ['t'] = 45, ['u'] = 46, ['v'] = 47,
    ['w'] = 48, ['x'] = 49, ['y'] = 50, ['z'] = 51, ['0'] = 52, ['1'] = 53, ['2'] = 54, ['3'] = 55,
    ['4'] = 56, ['5'] = 57, ['6'] = 58, ['7'] = 59, ['8'] = 60, ['9'] = 61, ['+'] = 62, ['/'] = 63,
};

uint8_t* base64_decode(const uint8_t* src, size_t len, size_t* out_len) {
    if (len % 4 != 0) {
        return NULL;
    }

    size_t padding = 0;
    if (src[len - 1] == '=') {
        padding++;
    }
    if (src[len - 2] == '=') {
        padding++;
    }

    *out_len = (len / 4) * 3 - padding;
    uint8_t* decoded = malloc(*out_len);
    if (decoded == NULL) {
        *out_len = 0;
        return NULL;
    }

    for (size_t i = 0, j = 0; i < len; i += 4, j += 3) {
        uint32_t block = 0;
        for (size_t k = 0; k < 4; k++) {
            block <<= 6;
            if (src[i + k] == '=') {
                padding--;
            } else {
                uint8_t index = base64_table[src[i + k]];
                if (index == 0 && src[i + k] != 'A') {
                    free(decoded);
                    return NULL;
                }
                block |= index;
            }
        }

        decoded[j] = (block >> 16) & 0xFF;
        if (j + 1 < *out_len) {
            decoded[j + 1] = (block >> 8) & 0xFF;
        }
        if (j + 2 < *out_len) {
            decoded[j + 2] = block & 0xFF;
        }
    }

    return decoded;
}

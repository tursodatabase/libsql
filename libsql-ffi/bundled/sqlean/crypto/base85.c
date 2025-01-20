// Originally by Fränz Friederes, MIT License
// https://github.com/cryptii/cryptii/blob/main/src/Encoder/Ascii85.js

// Modified by Anton Zhiyanov, MIT License
// https://github.com/nalgeon/sqlean/

// Base85 (Ascii85) encoding/decoding

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

uint8_t* base85_encode(const uint8_t* src, size_t len, size_t* out_len) {
    uint8_t* encoded = malloc(len * 5 / 4 + 5);
    if (encoded == NULL) {
        *out_len = 0;
        return NULL;
    }

    // Encode each tuple of 4 bytes
    uint32_t digits[5], tuple;
    size_t pos = 0;
    for (size_t i = 0; i < len; i += 4) {
        // Read 32-bit unsigned integer from bytes following the
        // big-endian convention (most significant byte first)
        tuple = (((src[i]) << 24) + ((src[i + 1] << 16) & 0xFF0000) + ((src[i + 2] << 8) & 0xFF00) +
                 ((src[i + 3]) & 0xFF));

        if (tuple > 0) {
            // Calculate 5 digits by repeatedly dividing
            // by 85 and taking the remainder
            for (size_t j = 0; j < 5; j++) {
                digits[4 - j] = tuple % 85;
                tuple = tuple / 85;
            }

            // Omit final characters added due to bytes of padding
            size_t num_padding = 0;
            if (len < i + 4) {
                num_padding = (i + 4) - len;
            }
            for (size_t j = 0; j < 5 - num_padding; j++) {
                encoded[pos++] = digits[j] + 33;
            }
        } else {
            // An all-zero tuple is encoded as a single character
            encoded[pos++] = 'z';
        }
    }

    *out_len = len * 5 / 4 + (len % 4 ? 1 : 0);
    encoded[*out_len] = '\0';
    return encoded;
}

uint8_t* base85_decode(const uint8_t* src, size_t len, size_t* out_len) {
    uint8_t* decoded = malloc(len * 4 / 5);
    if (decoded == NULL) {
        *out_len = 0;
        return NULL;
    }

    uint8_t digits[5], tupleBytes[4];
    uint32_t tuple;
    size_t pos = 0;
    for (size_t i = 0; i < len;) {
        if (src[i] == 'z') {
            // A single character encodes an all-zero tuple
            decoded[pos++] = 0;
            decoded[pos++] = 0;
            decoded[pos++] = 0;
            decoded[pos++] = 0;
            i++;
        } else {
            // Retrieve radix-85 digits of tuple
            for (int k = 0; k < 5; k++) {
                if (i + k < len) {
                    uint8_t digit = src[i + k] - 33;
                    if (digit < 0 || digit > 84) {
                        *out_len = 0;
                        free(decoded);
                        return NULL;
                    }
                    digits[k] = digit;
                } else {
                    digits[k] = 84;  // Pad with 'u'
                }
            }

            // Create 32-bit binary number from digits and handle padding
            // tuple = a * 85^4 + b * 85^3 + c * 85^2 + d * 85 + e
            tuple = digits[0] * 52200625 + digits[1] * 614125 + digits[2] * 7225 + digits[3] * 85 +
                    digits[4];

            // Get bytes from tuple
            tupleBytes[0] = (tuple >> 24) & 0xff;
            tupleBytes[1] = (tuple >> 16) & 0xff;
            tupleBytes[2] = (tuple >> 8) & 0xff;
            tupleBytes[3] = tuple & 0xff;

            // Remove bytes of padding
            int padding = 0;
            if (i + 4 >= len) {
                padding = i + 4 - len;
            }

            // Append bytes to result
            for (int k = 0; k < 4 - padding; k++) {
                decoded[pos++] = tupleBytes[k];
            }
            i += 5;
        }
    }

    *out_len = len * 4 / 5;
    return decoded;
}

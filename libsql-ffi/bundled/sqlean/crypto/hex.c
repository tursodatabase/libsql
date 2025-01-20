// Copyright (c) 2023 Anton Zhiyanov, MIT License
// https://github.com/nalgeon/sqlean

// Hex encoding/decoding

#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>

uint8_t* hex_encode(const uint8_t* src, size_t len, size_t* out_len) {
    *out_len = len * 2;
    uint8_t* encoded = malloc(*out_len + 1);
    if (encoded == NULL) {
        *out_len = 0;
        return NULL;
    }
    for (size_t i = 0; i < len; i++) {
        sprintf((char*)encoded + (i * 2), "%02x", src[i]);
    }
    encoded[*out_len] = '\0';
    *out_len = len * 2;
    return encoded;
}

uint8_t* hex_decode(const uint8_t* src, size_t len, size_t* out_len) {
    if (len % 2 != 0) {
        // input length must be even
        return NULL;
    }

    size_t decoded_len = len / 2;
    uint8_t* decoded = malloc(decoded_len);
    if (decoded == NULL) {
        *out_len = 0;
        return NULL;
    }

    for (size_t i = 0; i < decoded_len; i++) {
        uint8_t hi = src[i * 2];
        uint8_t lo = src[i * 2 + 1];

        if (hi >= '0' && hi <= '9') {
            hi -= '0';
        } else if (hi >= 'A' && hi <= 'F') {
            hi -= 'A' - 10;
        } else if (hi >= 'a' && hi <= 'f') {
            hi -= 'a' - 10;
        } else {
            // invalid character
            free(decoded);
            return NULL;
        }

        if (lo >= '0' && lo <= '9') {
            lo -= '0';
        } else if (lo >= 'A' && lo <= 'F') {
            lo -= 'A' - 10;
        } else if (lo >= 'a' && lo <= 'f') {
            lo -= 'a' - 10;
        } else {
            // invalid character
            free(decoded);
            return NULL;
        }

        decoded[i] = (hi << 4) | lo;
    }

    *out_len = decoded_len;
    return decoded;
}

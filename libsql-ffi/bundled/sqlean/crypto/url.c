// Originally by Fränz Friederes, MIT License
// https://github.com/cryptii/cryptii/blob/main/src/Encoder/URL.js

// Modified by Anton Zhiyanov, MIT License
// https://github.com/nalgeon/sqlean/

// URL-escape encoding/decoding

#include <ctype.h>
#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

const char* url_chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_.~";

uint8_t hex_to_ascii(char c) {
    if (isdigit(c)) {
        return c - '0';
    } else {
        return tolower(c) - 'a' + 10;
    }
}

uint8_t* url_encode(const uint8_t* src, size_t len, size_t* out_len) {
    size_t encoded_len = 0;
    for (size_t i = 0; i < len; i++) {
        if (strchr(url_chars, src[i]) == NULL) {
            encoded_len += 3;
        } else {
            encoded_len += 1;
        }
    }

    uint8_t* encoded = malloc(encoded_len + 1);
    if (encoded == NULL) {
        *out_len = 0;
        return NULL;
    }

    size_t pos = 0;
    for (size_t i = 0; i < len; i++) {
        if (strchr(url_chars, src[i]) == NULL) {
            encoded[pos++] = '%';
            encoded[pos++] = "0123456789ABCDEF"[src[i] >> 4];
            encoded[pos++] = "0123456789ABCDEF"[src[i] & 0x0F];
        } else {
            encoded[pos++] = src[i];
        }
    }
    encoded[pos] = '\0';

    *out_len = pos;
    return encoded;
}

uint8_t* url_decode(const uint8_t* src, size_t len, size_t* out_len) {
    uint8_t* decoded = malloc(len);
    if (decoded == NULL) {
        *out_len = 0;
        return NULL;
    }

    size_t pos = 0;
    for (size_t i = 0; i < len; i++) {
        if (src[i] == '%') {
            if (i + 2 >= len || !isxdigit(src[i + 1]) || !isxdigit(src[i + 2])) {
                free(decoded);
                return NULL;
            }
            decoded[pos++] = (hex_to_ascii(src[i + 1]) << 4) | hex_to_ascii(src[i + 2]);
            i += 2;
        } else if (src[i] == '+') {
            decoded[pos++] = ' ';
        } else {
            decoded[pos++] = src[i];
        }
    }

    *out_len = pos;
    return decoded;
}

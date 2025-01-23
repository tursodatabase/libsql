// Copyright (c) 2023 Anton Zhiyanov, MIT License
// https://github.com/nalgeon/sqlean

// Base32 encoding/decoding (RFC 4648)

#include <stdint.h>
#include <stdlib.h>
#include <string.h>

static const char base32_chars[] = "ABCDEFGHIJKLMNOPQRSTUVWXYZ234567";

uint8_t* base32_encode(const uint8_t* src, size_t len, size_t* out_len) {
    *out_len = ((len + 4) / 5) * 8;
    uint8_t* encoded = malloc(*out_len + 1);
    if (encoded == NULL) {
        *out_len = 0;
        return NULL;
    }

    for (size_t i = 0, j = 0; i < len;) {
        uint32_t octet0 = i < len ? src[i++] : 0;
        uint32_t octet1 = i < len ? src[i++] : 0;
        uint32_t octet2 = i < len ? src[i++] : 0;
        uint32_t octet3 = i < len ? src[i++] : 0;
        uint32_t octet4 = i < len ? src[i++] : 0;

        encoded[j++] = base32_chars[octet0 >> 3];
        encoded[j++] = base32_chars[((octet0 & 0x07) << 2) | (octet1 >> 6)];
        encoded[j++] = base32_chars[(octet1 >> 1) & 0x1F];
        encoded[j++] = base32_chars[((octet1 & 0x01) << 4) | (octet2 >> 4)];
        encoded[j++] = base32_chars[((octet2 & 0x0F) << 1) | (octet3 >> 7)];
        encoded[j++] = base32_chars[(octet3 >> 2) & 0x1F];
        encoded[j++] = base32_chars[((octet3 & 0x03) << 3) | (octet4 >> 5)];
        encoded[j++] = base32_chars[octet4 & 0x1F];
    }

    if (len % 5 != 0) {
        size_t padding = 7 - (len % 5) * 8 / 5;
        for (size_t i = 0; i < padding; i++) {
            encoded[*out_len - padding + i] = '=';
        }
    }

    encoded[*out_len] = '\0';
    return encoded;
}

uint8_t* base32_decode(const uint8_t* src, size_t len, size_t* out_len) {
    while (len > 0 && src[len - 1] == '=') {
        len--;
    }
    *out_len = len * 5 / 8;
    uint8_t* decoded = malloc(*out_len);
    if (decoded == NULL) {
        *out_len = 0;
        return NULL;
    }

    size_t bits = 0, value = 0, count = 0;
    for (size_t i = 0; i < len; i++) {
        uint8_t c = src[i];
        if (c >= 'A' && c <= 'Z') {
            c -= 'A';
        } else if (c >= '2' && c <= '7') {
            c -= '2' - 26;
        } else {
            continue;
        }
        value = (value << 5) | c;
        bits += 5;
        if (bits >= 8) {
            decoded[count++] = (uint8_t)(value >> (bits - 8));
            bits -= 8;
        }
    }
    if (bits >= 5 || (value & ((1 << bits) - 1)) != 0) {
        free(decoded);
        return NULL;
    }
    *out_len = count;
    return decoded;
}

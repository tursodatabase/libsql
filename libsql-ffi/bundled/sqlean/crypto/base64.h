// Copyright (c) 2023 Anton Zhiyanov, MIT License
// https://github.com/nalgeon/sqlean

// Base64 encoding/decoding (RFC 4648)

#ifndef BASE64_H
#define BASE64_H

#include <stddef.h>
#include <stdint.h>

uint8_t* base64_encode(const uint8_t* src, size_t len, size_t* out_len);
uint8_t* base64_decode(const uint8_t* src, size_t len, size_t* out_len);

#endif /* BASE64_H */

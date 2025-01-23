// Copyright (c) 2023 Anton Zhiyanov, MIT License
// https://github.com/nalgeon/sqlean

// Base32 encoding/decoding (RFC 4648)

#ifndef _BASE32_H_
#define _BASE32_H_

#include <stdint.h>

uint8_t* base32_encode(const uint8_t* src, size_t len, size_t* out_len);
uint8_t* base32_decode(const uint8_t* src, size_t len, size_t* out_len);

#endif /* _BASE32_H_ */

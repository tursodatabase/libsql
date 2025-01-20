// Copyright (c) 2023 Anton Zhiyanov, MIT License
// https://github.com/nalgeon/sqlean

// Hex encoding/decoding

#ifndef _HEX_H_
#define _HEX_H_

#include <stddef.h>
#include <stdint.h>

uint8_t* hex_encode(const uint8_t* src, size_t len, size_t* out_len);
uint8_t* hex_decode(const uint8_t* src, size_t len, size_t* out_len);

#endif /* _HEX_H_ */

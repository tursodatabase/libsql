// Copyright (c) 2023 Anton Zhiyanov, MIT License
// https://github.com/nalgeon/sqlean

// Base85 (Ascii85) encoding/decoding

#ifndef _BASE85_H_
#define _BASE85_H_

#include <stddef.h>
#include <stdint.h>

uint8_t* base85_encode(const uint8_t* src, size_t len, size_t* out_len);
uint8_t* base85_decode(const uint8_t* src, size_t len, size_t* out_len);

#endif /* _BASE85_H_ */

// Copyright (c) 2023 Anton Zhiyanov, MIT License
// https://github.com/nalgeon/sqlean

// URL-escape encoding/decoding

#ifndef _URL_H_
#define _URL_H_

#include <stddef.h>
#include <stdint.h>

uint8_t* url_encode(const uint8_t* src, size_t len, size_t* out_len);
uint8_t* url_decode(const uint8_t* src, size_t len, size_t* out_len);

#endif /* _URL_H_ */

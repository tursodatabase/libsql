// Created by: Peter Tripp (@notpeter)
// Public Domain

#ifndef __BLAKE3_H__
#define __BLAKE3_H__

#include "crypto/blake3_reference_impl.h"

void* blake3_init();
void blake3_update(blake3_hasher* ctx, const unsigned char data[], size_t len);
int blake3_final(blake3_hasher* ctx, unsigned char hash[]);

#endif

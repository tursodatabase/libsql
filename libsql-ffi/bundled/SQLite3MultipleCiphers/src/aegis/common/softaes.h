/*
** Name:        softaes.h
** Purpose:     Header for API of AES software implementation
** Copyright:   (c) 2023-2024 Frank Denis
** SPDX-License-Identifier: MIT
*/

#ifndef AEGIS_SOFTAES_H
#define AEGIS_SOFTAES_H

#include <stdint.h>

#include "common.h"

typedef struct SoftAesBlock {
    uint32_t w0;
    uint32_t w1;
    uint32_t w2;
    uint32_t w3;
} SoftAesBlock;

static SoftAesBlock
softaes_block_encrypt(const SoftAesBlock block, const SoftAesBlock rk);

static inline SoftAesBlock
softaes_block_load(const uint8_t in[16])
{
#ifdef NATIVE_LITTLE_ENDIAN
    SoftAesBlock out;
    memcpy(&out, in, 16);
#else
    const SoftAesBlock out = { AEGIS_LOAD32_LE(in + 0), AEGIS_LOAD32_LE(in + 4), AEGIS_LOAD32_LE(in + 8),
                               AEGIS_LOAD32_LE(in + 12) };
#endif
    return out;
}

static inline SoftAesBlock
softaes_block_load64x2(const uint64_t a, const uint64_t b)
{
    const SoftAesBlock out = { (uint32_t) b, (uint32_t) (b >> 32), (uint32_t) a,
                               (uint32_t) (a >> 32) };
    return out;
}

static inline void
softaes_block_store(uint8_t out[16], const SoftAesBlock in)
{
#ifdef NATIVE_LITTLE_ENDIAN
    memcpy(out, &in, 16);
#else
    AEGIS_STORE32_LE(out + 0, in.w0);
    AEGIS_STORE32_LE(out + 4, in.w1);
    AEGIS_STORE32_LE(out + 8, in.w2);
    AEGIS_STORE32_LE(out + 12, in.w3);
#endif
}

static inline SoftAesBlock
softaes_block_xor(const SoftAesBlock a, const SoftAesBlock b)
{
    const SoftAesBlock out = { a.w0 ^ b.w0, a.w1 ^ b.w1, a.w2 ^ b.w2, a.w3 ^ b.w3 };
    return out;
}

static inline SoftAesBlock
softaes_block_and(const SoftAesBlock a, const SoftAesBlock b)
{
    const SoftAesBlock out = { a.w0 & b.w0, a.w1 & b.w1, a.w2 & b.w2, a.w3 & b.w3 };
    return out;
}

#endif /* AEGIS_SOFTAES_H */

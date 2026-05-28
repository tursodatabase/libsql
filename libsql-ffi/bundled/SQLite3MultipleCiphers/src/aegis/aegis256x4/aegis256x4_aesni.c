/*
** Name:        aegis256x4_aesni.c
** Purpose:     Implementation of AEGIS-256x4 - AES-NI
** Copyright:   (c) 2023-2024 Frank Denis
** SPDX-License-Identifier: MIT
*/

#if defined(__i386__) || defined(_M_IX86) || defined(__x86_64__) || defined(_M_AMD64)

#include <errno.h>
#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#include "../common/common.h"
#include "aegis256x4.h"
#include "aegis256x4_aesni.h"

#ifdef __clang__
#  pragma clang attribute push(__attribute__((target("aes,avx"))), apply_to = function)
#elif defined(__GNUC__)
#  pragma GCC target("aes,avx")
#endif

#include <immintrin.h>
#include <wmmintrin.h>

#define AES_BLOCK_LENGTH 64

typedef struct {
    __m128i b0;
    __m128i b1;
    __m128i b2;
    __m128i b3;
} aegis256x4_aes_block_t;

#define AEGIS_AES_BLOCK_T aegis256x4_aes_block_t
#define AEGIS_BLOCKS      aegis256x4_blocks
#define AEGIS_STATE      _aegis256x4_state
#define AEGIS_MAC_STATE  _aegis256x4_mac_state

#define AEGIS_FUNC_PREFIX  aegis256x4_impl

#include "../common/func_names_define.h"

static inline AEGIS_AES_BLOCK_T
AEGIS_AES_BLOCK_XOR(const AEGIS_AES_BLOCK_T a, const AEGIS_AES_BLOCK_T b)
{
    return (AEGIS_AES_BLOCK_T) { _mm_xor_si128(a.b0, b.b0), _mm_xor_si128(a.b1, b.b1),
                           _mm_xor_si128(a.b2, b.b2), _mm_xor_si128(a.b3, b.b3) };
}

static inline AEGIS_AES_BLOCK_T
AEGIS_AES_BLOCK_AND(const AEGIS_AES_BLOCK_T a, const AEGIS_AES_BLOCK_T b)
{
    return (AEGIS_AES_BLOCK_T) { _mm_and_si128(a.b0, b.b0), _mm_and_si128(a.b1, b.b1),
                           _mm_and_si128(a.b2, b.b2), _mm_and_si128(a.b3, b.b3) };
}

static inline AEGIS_AES_BLOCK_T
AEGIS_AES_BLOCK_LOAD(const uint8_t *a)
{
    return (AEGIS_AES_BLOCK_T) { _mm_loadu_si128((const __m128i *) (const void *) a),
                           _mm_loadu_si128((const __m128i *) (const void *) (a + 16)),
                           _mm_loadu_si128((const __m128i *) (const void *) (a + 32)),
                           _mm_loadu_si128((const __m128i *) (const void *) (a + 48)) };
}

static inline AEGIS_AES_BLOCK_T
AEGIS_AES_BLOCK_LOAD_64x2(uint64_t a, uint64_t b)
{
    const __m128i t = _mm_set_epi64x((long long) a, (long long) b);
    return (AEGIS_AES_BLOCK_T) { t, t, t, t };
}

static inline void
AEGIS_AES_BLOCK_STORE(uint8_t *a, const AEGIS_AES_BLOCK_T b)
{
    _mm_storeu_si128((__m128i *) (void *) a, b.b0);
    _mm_storeu_si128((__m128i *) (void *) (a + 16), b.b1);
    _mm_storeu_si128((__m128i *) (void *) (a + 32), b.b2);
    _mm_storeu_si128((__m128i *) (void *) (a + 48), b.b3);
}

static inline AEGIS_AES_BLOCK_T
AEGIS_AES_ENC(const AEGIS_AES_BLOCK_T a, const AEGIS_AES_BLOCK_T b)
{
    return (AEGIS_AES_BLOCK_T) { _mm_aesenc_si128(a.b0, b.b0), _mm_aesenc_si128(a.b1, b.b1),
                           _mm_aesenc_si128(a.b2, b.b2), _mm_aesenc_si128(a.b3, b.b3) };
}

static inline void
AEGIS_update(AEGIS_AES_BLOCK_T *const state, const AEGIS_AES_BLOCK_T d)
{
    AEGIS_AES_BLOCK_T tmp;

    tmp      = state[5];
    state[5] = AEGIS_AES_ENC(state[4], state[5]);
    state[4] = AEGIS_AES_ENC(state[3], state[4]);
    state[3] = AEGIS_AES_ENC(state[2], state[3]);
    state[2] = AEGIS_AES_ENC(state[1], state[2]);
    state[1] = AEGIS_AES_ENC(state[0], state[1]);
    state[0] = AEGIS_AES_BLOCK_XOR(AEGIS_AES_ENC(tmp, state[0]), d);
}

#include "aegis256x4_common.h"

struct aegis256x4_implementation aegis256x4_aesni_implementation = {
#include "../common/func_table.h"
};

#include "../common/type_names_undefine.h"
#include "../common/func_names_undefine.h"

#ifdef __clang__
#  pragma clang attribute pop
#endif

#endif

/*
** Name:        aegis128x2_aesni.c
** Purpose:     Implementation of AEGIS-128x2 - AES-NI
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
#include "aegis128x2.h"
#include "aegis128x2_aesni.h"

#ifdef __clang__
#  pragma clang attribute push(__attribute__((target("aes,avx"))), apply_to = function)
#elif defined(__GNUC__)
#  pragma GCC target("aes,avx")
#endif

#include <immintrin.h>
#include <wmmintrin.h>

#define AES_BLOCK_LENGTH 32

typedef struct {
    __m128i b0;
    __m128i b1;
} aegis128x2_aes_block_t;

#define AEGIS_AES_BLOCK_T aegis128x2_aes_block_t
#define AEGIS_BLOCKS      aegis128x2_blocks
#define AEGIS_STATE      _aegis128x2_state
#define AEGIS_MAC_STATE  _aegis128x2_mac_state

#define AEGIS_FUNC_PREFIX  aegis128x2_impl

#include "../common/func_names_define.h"

static inline AEGIS_AES_BLOCK_T
AEGIS_AES_BLOCK_XOR(const AEGIS_AES_BLOCK_T a, const AEGIS_AES_BLOCK_T b)
{
    return (AEGIS_AES_BLOCK_T) { _mm_xor_si128(a.b0, b.b0), _mm_xor_si128(a.b1, b.b1) };
}

static inline AEGIS_AES_BLOCK_T
AEGIS_AES_BLOCK_AND(const AEGIS_AES_BLOCK_T a, const AEGIS_AES_BLOCK_T b)
{
    return (AEGIS_AES_BLOCK_T) { _mm_and_si128(a.b0, b.b0), _mm_and_si128(a.b1, b.b1) };
}

static inline AEGIS_AES_BLOCK_T
AEGIS_AES_BLOCK_LOAD(const uint8_t *a)
{
    return (AEGIS_AES_BLOCK_T) { _mm_loadu_si128((const __m128i *) (const void *) a),
                           _mm_loadu_si128((const __m128i *) (const void *) (a + 16)) };
}

static inline AEGIS_AES_BLOCK_T
AEGIS_AES_BLOCK_LOAD_64x2(uint64_t a, uint64_t b)
{
    const __m128i t = _mm_set_epi64x((long long) a, (long long) b);
    return (AEGIS_AES_BLOCK_T) { t, t };
}

static inline void
AEGIS_AES_BLOCK_STORE(uint8_t *a, const AEGIS_AES_BLOCK_T b)
{
    _mm_storeu_si128((__m128i *) (void *) a, b.b0);
    _mm_storeu_si128((__m128i *) (void *) (a + 16), b.b1);
}

static inline AEGIS_AES_BLOCK_T
AEGIS_AES_ENC(const AEGIS_AES_BLOCK_T a, const AEGIS_AES_BLOCK_T b)
{
    return (AEGIS_AES_BLOCK_T) { _mm_aesenc_si128(a.b0, b.b0), _mm_aesenc_si128(a.b1, b.b1) };
}

static inline void
AEGIS_update(AEGIS_AES_BLOCK_T *const state, const AEGIS_AES_BLOCK_T d1, const AEGIS_AES_BLOCK_T d2)
{
    AEGIS_AES_BLOCK_T tmp;

    tmp      = state[7];
    state[7] = AEGIS_AES_ENC(state[6], state[7]);
    state[6] = AEGIS_AES_ENC(state[5], state[6]);
    state[5] = AEGIS_AES_ENC(state[4], state[5]);
    state[4] = AEGIS_AES_ENC(state[3], state[4]);
    state[3] = AEGIS_AES_ENC(state[2], state[3]);
    state[2] = AEGIS_AES_ENC(state[1], state[2]);
    state[1] = AEGIS_AES_ENC(state[0], state[1]);
    state[0] = AEGIS_AES_ENC(tmp, state[0]);

    state[0] = AEGIS_AES_BLOCK_XOR(state[0], d1);
    state[4] = AEGIS_AES_BLOCK_XOR(state[4], d2);
}

#include "aegis128x2_common.h"

struct aegis128x2_implementation aegis128x2_aesni_implementation = {
#include "../common/func_table.h"
};

#include "../common/type_names_undefine.h"
#include "../common/func_names_undefine.h"

#ifdef __clang__
#  pragma clang attribute pop
#endif

#endif

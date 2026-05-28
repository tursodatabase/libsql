/*
** Name:        aegis256x2_avx2.c
** Purpose:     Implementation of AEGIS-256x2 - AES-NI AVX2
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
#include "aegis256x2.h"
#include "aegis256x2_avx2.h"

#ifdef HAVE_VAESINTRIN_H

#ifdef __clang__
#  pragma clang attribute push(__attribute__((target("vaes,avx2"))), apply_to = function)
#elif defined(__GNUC__)
#  pragma GCC target("vaes,avx2")
#endif

#include <immintrin.h>

#define AES_BLOCK_LENGTH 32

typedef __m256i aegis256x2_avx2_aes_block_t;

#define AEGIS_AES_BLOCK_T aegis256x2_avx2_aes_block_t
#define AEGIS_BLOCKS      aegis256x2_avx2_blocks
#define AEGIS_STATE      _aegis256x2_avx2_state
#define AEGIS_MAC_STATE  _aegis256x2_avx2_mac_state

#define AEGIS_FUNC_PREFIX  aegis256x2_avx2_impl

#include "../common/func_names_define.h"

static inline AEGIS_AES_BLOCK_T
AEGIS_AES_BLOCK_XOR(const AEGIS_AES_BLOCK_T a, const AEGIS_AES_BLOCK_T b)
{
  return _mm256_xor_si256(a, b);
}

static inline AEGIS_AES_BLOCK_T
AEGIS_AES_BLOCK_AND(const AEGIS_AES_BLOCK_T a, const AEGIS_AES_BLOCK_T b)
{
  return _mm256_and_si256(a, b);
}

static inline AEGIS_AES_BLOCK_T
AEGIS_AES_BLOCK_LOAD(const uint8_t *a)
{
  return _mm256_loadu_si256((const AEGIS_AES_BLOCK_T *) (const void *) a);
}

static inline AEGIS_AES_BLOCK_T
AEGIS_AES_BLOCK_LOAD_64x2(uint64_t a, uint64_t b)
{
  return _mm256_broadcastsi128_si256(_mm_set_epi64x(a, b));
}

static inline void
AEGIS_AES_BLOCK_STORE(uint8_t *a, const AEGIS_AES_BLOCK_T b)
{
  _mm256_storeu_si256((AEGIS_AES_BLOCK_T *) (void *) a, b);
}

static inline AEGIS_AES_BLOCK_T
AEGIS_AES_ENC(const AEGIS_AES_BLOCK_T a, const AEGIS_AES_BLOCK_T b)
{
  return _mm256_aesenc_epi128(a, b);
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

#include "aegis256x2_common.h"

struct aegis256x2_implementation aegis256x2_avx2_implementation = {
#include "../common/func_table.h"
};

#include "../common/type_names_undefine.h"
#include "../common/func_names_undefine.h"

#ifdef __clang__
#  pragma clang attribute pop
#endif

#endif /* HAVE_VAESINTRIN_H */

#endif

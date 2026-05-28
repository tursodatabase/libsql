/*
** Name:        aegis128l_aesni.c
** Purpose:     Implementation of AEGIS-128L - AES-NI
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
#include "aegis128l.h"
#include "aegis128l_aesni.h"

#ifdef __clang__
#  pragma clang attribute push(__attribute__((target("aes,avx"))), apply_to = function)
#elif defined(__GNUC__)
#  pragma GCC target("aes,avx")
#endif

#include <immintrin.h>
#include <wmmintrin.h>

#define AES_BLOCK_LENGTH 16

typedef __m128i aegis128l_aes_block_t;

#define AEGIS_AES_BLOCK_T aegis128l_aes_block_t
#define AEGIS_BLOCKS      aegis128l_blocks
#define AEGIS_STATE      _aegis128l_state
#define AEGIS_MAC_STATE  _aegis128l_mac_state

#define AEGIS_FUNC_PREFIX  aegis128l_impl

#include "../common/func_names_define.h"

static inline AEGIS_AES_BLOCK_T
AEGIS_AES_BLOCK_XOR(const AEGIS_AES_BLOCK_T a, const AEGIS_AES_BLOCK_T b)
{
  return _mm_xor_si128(a, b);
}

static inline AEGIS_AES_BLOCK_T
AEGIS_AES_BLOCK_AND(const AEGIS_AES_BLOCK_T a, const AEGIS_AES_BLOCK_T b)
{
  return _mm_and_si128(a, b);
}

static inline AEGIS_AES_BLOCK_T
AEGIS_AES_BLOCK_LOAD(const uint8_t *a)
{
  return _mm_loadu_si128((const AEGIS_AES_BLOCK_T *) (const void *) a);
}

static inline AEGIS_AES_BLOCK_T
AEGIS_AES_BLOCK_LOAD_64x2(uint64_t a, uint64_t b)
{
  return _mm_set_epi64x((long long) a, (long long) b);
}

static inline void
AEGIS_AES_BLOCK_STORE(uint8_t *a, const AEGIS_AES_BLOCK_T b)
{
  _mm_storeu_si128((AEGIS_AES_BLOCK_T *) (void *) a, b);
}

static inline AEGIS_AES_BLOCK_T
AEGIS_AES_ENC(const AEGIS_AES_BLOCK_T a, const AEGIS_AES_BLOCK_T b)
{
  return _mm_aesenc_si128(a, b);
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

#include "aegis128l_common.h"

struct aegis128l_implementation aegis128l_aesni_implementation = {
#include "../common/func_table.h"
};

#include "../common/type_names_undefine.h"
#include "../common/func_names_undefine.h"

#ifdef __clang__
#  pragma clang attribute pop
#endif

#endif

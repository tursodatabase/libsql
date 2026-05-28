/*
** Name:        aegis128l_altivec.c
** Purpose:     Implementation of AEGIS-128L - AltiVec
** Copyright:   (c) 2023-2024 Frank Denis
** SPDX-License-Identifier: MIT
*/

#if defined(__ALTIVEC__) && defined(__CRYPTO__)

#include <errno.h>
#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#include "../common/common.h"
#include "aegis128l.h"
#include "aegis128l_altivec.h"

#include <altivec.h>

#ifdef __clang__
#  pragma clang attribute push(__attribute__((target("altivec,crypto"))), apply_to = function)
#elif defined(__GNUC__)
#  pragma GCC target("altivec,crypto")
#endif

#define AES_BLOCK_LENGTH 16

typedef vector unsigned char aegis128l_aes_block_t;

#define AEGIS_AES_BLOCK_T aegis128l_aes_block_t
#define AEGIS_BLOCKS      aegis128l_blocks
#define AEGIS_STATE      _aegis128l_state
#define AEGIS_MAC_STATE  _aegis128l_mac_state

#define AEGIS_FUNC_PREFIX  aegis128l_impl

#include "../common/func_names_define.h"

static inline AEGIS_AES_BLOCK_T
AEGIS_AES_BLOCK_XOR(const AEGIS_AES_BLOCK_T a, const AEGIS_AES_BLOCK_T b)
{
  return vec_xor(a, b);
}

static inline AEGIS_AES_BLOCK_T
AEGIS_AES_BLOCK_AND(const AEGIS_AES_BLOCK_T a, const AEGIS_AES_BLOCK_T b)
{
  return vec_and(a, b);
}

static inline AEGIS_AES_BLOCK_T
AEGIS_AES_BLOCK_LOAD(const uint8_t *a)
{
  return vec_xl_be(0, (const unsigned char *) a);
}

static inline AEGIS_AES_BLOCK_T
AEGIS_AES_BLOCK_LOAD_64x2(uint64_t a, uint64_t b)
{
  return (AEGIS_AES_BLOCK_T) vec_revb(vec_insert(a, vec_promote((unsigned long long) b, 1), 0));
}

static inline void
AEGIS_AES_BLOCK_STORE(uint8_t *a, const AEGIS_AES_BLOCK_T b)
{
  vec_xst_be(b, 0, (unsigned char *) a);
}

static inline AEGIS_AES_BLOCK_T
AEGIS_AES_ENC(const AEGIS_AES_BLOCK_T a, const AEGIS_AES_BLOCK_T b)
{
  return (AEGIS_AES_BLOCK_T) vec_cipher_be(a, b);
}

static inline void
AEGIS_update(AEGIS_AES_BLOCK_T *const state, const AEGIS_AES_BLOCK_T d1, const AEGIS_AES_BLOCK_T d2)
{
    AEGIS_AES_BLOCK_T tmp;

    tmp      = state[7];
    state[7] = AEGIS_AES_ENC(state[6], state[7]);
    state[6] = AEGIS_AES_ENC(state[5], state[6]);
    state[5] = AEGIS_AES_ENC(state[4], state[5]);
    state[4] = AEGIS_AES_BLOCK_XOR(AEGIS_AES_ENC(state[3], state[4]), d2);
    state[3] = AEGIS_AES_ENC(state[2], state[3]);
    state[2] = AEGIS_AES_ENC(state[1], state[2]);
    state[1] = AEGIS_AES_ENC(state[0], state[1]);
    state[0] = AEGIS_AES_BLOCK_XOR(AEGIS_AES_ENC(tmp, state[0]), d1);
}

#include "aegis128l_common.h"

struct aegis128l_implementation aegis128l_altivec_implementation = {
#include "../common/func_table.h"
};

#include "../common/type_names_undefine.h"
#include "../common/func_names_undefine.h"

#ifdef __clang__
#  pragma clang attribute pop
#endif

#endif

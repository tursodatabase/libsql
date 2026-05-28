/*
** Name:        aegis128l_armcrypto.c
** Purpose:     Implementation of AEGIS-128L - ARM-Crypto
** Copyright:   (c) 2023-2024 Frank Denis
** SPDX-License-Identifier: MIT
*/

#include "../common/aeshardware.h"

#if HAS_AEGIS_AES_HARDWARE == AEGIS_AES_HARDWARE_NEON

#include <errno.h>
#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#include "../common/common.h"
#include "aegis128l.h"
#include "aegis128l_armcrypto.h"

#ifndef __ARM_FEATURE_CRYPTO
#  define __ARM_FEATURE_CRYPTO 1
#endif
#ifndef __ARM_FEATURE_AES
#  define __ARM_FEATURE_AES 1
#endif

#ifdef USE_ARM64_NEON_H
#include <arm64_neon.h>
#else
#include <arm_neon.h>
#endif

#ifdef __clang__
#  pragma clang attribute push(__attribute__((target("neon,crypto,aes"))), \
                                     apply_to = function)
#elif defined(__GNUC__)
#  pragma GCC target("+simd+crypto")
#endif

#define AES_BLOCK_LENGTH 16

typedef uint8x16_t aegis128l_aes_block_t;

#define AEGIS_AES_BLOCK_T aegis128l_aes_block_t
#define AEGIS_BLOCKS      aegis128l_blocks
#define AEGIS_STATE      _aegis128l_state
#define AEGIS_MAC_STATE  _aegis128l_mac_state

#define AEGIS_FUNC_PREFIX  aegis128l_impl

#include "../common/func_names_define.h"

static inline AEGIS_AES_BLOCK_T
AEGIS_AES_BLOCK_XOR(const AEGIS_AES_BLOCK_T a, const AEGIS_AES_BLOCK_T b)
{
  return veorq_u8(a, b);
}

static inline AEGIS_AES_BLOCK_T
AEGIS_AES_BLOCK_AND(const AEGIS_AES_BLOCK_T a, const AEGIS_AES_BLOCK_T b)
{
  return vandq_u8(a, b);
}

static inline AEGIS_AES_BLOCK_T
AEGIS_AES_BLOCK_LOAD(const uint8_t *a)
{
  return vld1q_u8(a);
}

static inline AEGIS_AES_BLOCK_T
AEGIS_AES_BLOCK_LOAD_64x2(uint64_t a, uint64_t b)
{
  return vreinterpretq_u8_u64(vsetq_lane_u64(a, vmovq_n_u64(b), 1));
}

static inline void
AEGIS_AES_BLOCK_STORE(uint8_t *a, const AEGIS_AES_BLOCK_T b)
{
  vst1q_u8(a, b);
}

static inline AEGIS_AES_BLOCK_T
AEGIS_AES_ENC(const AEGIS_AES_BLOCK_T a, const AEGIS_AES_BLOCK_T b)
{
  return veorq_u8(vaesmcq_u8(vaeseq_u8(a, vmovq_n_u8(0))), b);
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

struct aegis128l_implementation aegis128l_armcrypto_implementation = {
#include "../common/func_table.h"
};

#include "../common/type_names_undefine.h"
#include "../common/func_names_undefine.h"

#ifdef __clang__
#  pragma clang attribute pop
#endif

#endif

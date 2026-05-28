/*
** Name:        aegis128x4_armcrypto.c
** Purpose:     Implementation of AEGIS-128x4 - ARM-Crypto
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
#include "aegis128x4.h"
#include "aegis128x4_armcrypto.h"

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

#define AES_BLOCK_LENGTH 64

typedef struct {
    uint8x16_t b0;
    uint8x16_t b1;
    uint8x16_t b2;
    uint8x16_t b3;
} aegis128x4_aes_block_t;

#define AEGIS_AES_BLOCK_T aegis128x4_aes_block_t
#define AEGIS_BLOCKS      aegis128x4_blocks
#define AEGIS_STATE      _aegis128x4_state
#define AEGIS_MAC_STATE  _aegis128x4_mac_state

#define AEGIS_FUNC_PREFIX          aegis128x4_impl

#include "../common/func_names_define.h"

static inline AEGIS_AES_BLOCK_T
AEGIS_AES_BLOCK_XOR(const AEGIS_AES_BLOCK_T a, const AEGIS_AES_BLOCK_T b)
{
    return (AEGIS_AES_BLOCK_T) { veorq_u8(a.b0, b.b0), veorq_u8(a.b1, b.b1), veorq_u8(a.b2, b.b2),
                           veorq_u8(a.b3, b.b3) };
}

static inline AEGIS_AES_BLOCK_T
AEGIS_AES_BLOCK_AND(const AEGIS_AES_BLOCK_T a, const AEGIS_AES_BLOCK_T b)
{
    return (AEGIS_AES_BLOCK_T) { vandq_u8(a.b0, b.b0), vandq_u8(a.b1, b.b1), vandq_u8(a.b2, b.b2),
                           vandq_u8(a.b3, b.b3) };
}

static inline AEGIS_AES_BLOCK_T
AEGIS_AES_BLOCK_LOAD(const uint8_t *a)
{
    return (AEGIS_AES_BLOCK_T) { vld1q_u8(a), vld1q_u8(a + 16), vld1q_u8(a + 32), vld1q_u8(a + 48) };
}

static inline AEGIS_AES_BLOCK_T
AEGIS_AES_BLOCK_LOAD_64x2(uint64_t a, uint64_t b)
{
    const uint8x16_t t = vreinterpretq_u8_u64(vsetq_lane_u64((a), vmovq_n_u64(b), 1));
    return (AEGIS_AES_BLOCK_T) { t, t, t, t };
}
static inline void
AEGIS_AES_BLOCK_STORE(uint8_t *a, const AEGIS_AES_BLOCK_T b)
{
    vst1q_u8(a, b.b0);
    vst1q_u8(a + 16, b.b1);
    vst1q_u8(a + 32, b.b2);
    vst1q_u8(a + 48, b.b3);
}

static inline AEGIS_AES_BLOCK_T
AEGIS_AES_ENC(const AEGIS_AES_BLOCK_T a, const AEGIS_AES_BLOCK_T b)
{
    return (AEGIS_AES_BLOCK_T) { veorq_u8(vaesmcq_u8(vaeseq_u8((a.b0), vmovq_n_u8(0))), (b.b0)),
                           veorq_u8(vaesmcq_u8(vaeseq_u8((a.b1), vmovq_n_u8(0))), (b.b1)),
                           veorq_u8(vaesmcq_u8(vaeseq_u8((a.b2), vmovq_n_u8(0))), (b.b2)),
                           veorq_u8(vaesmcq_u8(vaeseq_u8((a.b3), vmovq_n_u8(0))), (b.b3)) };
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

#include "aegis128x4_common.h"

struct aegis128x4_implementation aegis128x4_armcrypto_implementation = {
#include "../common/func_table.h"
};

#include "../common/type_names_undefine.h"
#include "../common/func_names_undefine.h"

#ifdef __clang__
#  pragma clang attribute pop
#endif

#endif

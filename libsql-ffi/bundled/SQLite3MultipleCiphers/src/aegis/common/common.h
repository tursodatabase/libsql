/*
** Name:        common.h
** Purpose:     Common header for AEGIS implementations
** Copyright:   (c) 2023-2024 Frank Denis
** SPDX-License-Identifier: MIT
*/

#ifndef AEGIS_COMMON_H
#define AEGIS_COMMON_H

#include <errno.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>

#include "../include/aegis.h"
#include "cpu.h"

#ifdef __linux__
#    define HAVE_SYS_AUXV_H
#    define HAVE_GETAUXVAL
#endif
#ifdef __ANDROID_API__
#    if __ANDROID_API__ < 18
#        undef HAVE_GETAUXVAL
#    endif
#    if defined(__clang__) || defined(__GNUC__)
#        if __has_include(<cpu-features.h>)
#            define HAVE_ANDROID_GETCPUFEATURES
#        endif
#    endif
#endif
#if defined(__i386__) || defined(_M_IX86) || defined(__x86_64__) || defined(_M_X64) || defined(_M_AMD64)

#    define HAVE_CPUID
#    define NATIVE_LITTLE_ENDIAN
#    if defined(__clang__) || defined(__GNUC__)
#        define HAVE_AVX_ASM
#    endif
#endif
#if HAS_AEGIS_AES_HARDWARE == AEGIS_AES_HARDWARE_NI
#    define HAVE_AVXINTRIN_H
#    define HAVE_AVX2INTRIN_H
#    define HAVE_AVX512FINTRIN_H
#    define HAVE_TMMINTRIN_H
#    define HAVE_WMMINTRIN_H
#    define HAVE_VAESINTRIN_H
#    ifdef __GNUC__
#        if !__has_include(<vaesintrin.h>)
#            undef HAVE_VAESINTRIN_H
#        endif
#    endif
#endif

#if defined(__BYTE_ORDER__) && __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
#    ifndef NATIVE_LITTLE_ENDIAN
#        define NATIVE_LITTLE_ENDIAN
#    endif
#endif

#if defined(__INTEL_COMPILER) || defined(_MSC_VER)
#    define CRYPTO_ALIGN(x) __declspec(align(x))
#else
#    define CRYPTO_ALIGN(x) __attribute__((aligned(x)))
#endif

#define AEGIS_LOAD32_LE(SRC) aegis_load32_le(SRC)
static inline uint32_t
aegis_load32_le(const uint8_t src[4])
{
#ifdef NATIVE_LITTLE_ENDIAN
    uint32_t w;
    memcpy(&w, src, sizeof w);
    return w;
#else
    uint32_t w = (uint32_t) src[0];
    w |= (uint32_t) src[1] << 8;
    w |= (uint32_t) src[2] << 16;
    w |= (uint32_t) src[3] << 24;
    return w;
#endif
}

#define AEGIS_STORE32_LE(DST, W) aegis_store32_le((DST), (W))
static inline void
aegis_store32_le(uint8_t dst[4], uint32_t w)
{
#ifdef NATIVE_LITTLE_ENDIAN
    memcpy(dst, &w, sizeof w);
#else
    dst[0] = (uint8_t) w;
    w >>= 8;
    dst[1] = (uint8_t) w;
    w >>= 8;
    dst[2] = (uint8_t) w;
    w >>= 8;
    dst[3] = (uint8_t) w;
#endif
}

#define AEGIS_ROTL32(X, B) aegis_rotl32((X), (B))
static inline uint32_t
aegis_rotl32(const uint32_t x, const int b)
{
    return (x << b) | (x >> (32 - b));
}

#define COMPILER_ASSERT(X) (void) sizeof(char[(X) ? 1 : -1])

#ifndef ERANGE
#    define ERANGE 34
#endif
#ifndef EINVAL
#    define EINVAL 22
#endif

#define AEGIS_CONCAT(A,B) AEGIS_CONCAT_(A,B)
#define AEGIS_CONCAT_(A,B) A##B
#define AEGIS_FUNC(name) AEGIS_CONCAT(AEGIS_FUNC_PREFIX,AEGIS_CONCAT(_,name))

#define AEGIS_API_IMPL_LIST_STD                                           \
    .encrypt_detached              = AEGIS_encrypt_detached,              \
    .decrypt_detached              = AEGIS_decrypt_detached,              \
    .encrypt_unauthenticated       = AEGIS_encrypt_unauthenticated,       \
    .decrypt_unauthenticated       = AEGIS_decrypt_unauthenticated,       \
    .stream                        = AEGIS_stream,
#define AEGIS_API_IMPL_LIST_INC                                           \
    .state_init                    = AEGIS_state_init,                    \
    .state_encrypt_update          = AEGIS_state_encrypt_update,          \
    .state_encrypt_detached_final  = AEGIS_state_encrypt_detached_final,  \
    .state_encrypt_final           = AEGIS_state_encrypt_final,           \
    .state_decrypt_detached_update = AEGIS_state_decrypt_detached_update, \
    .state_decrypt_detached_final  = AEGIS_state_decrypt_detached_final,
#define AEGIS_API_IMPL_LIST_MAC                                           \
    .state_mac_init                = AEGIS_state_mac_init,                \
    .state_mac_update              = AEGIS_state_mac_update,              \
    .state_mac_final               = AEGIS_state_mac_final,               \
    .state_mac_reset               = AEGIS_state_mac_reset,               \
    .state_mac_clone               = AEGIS_state_mac_clone,

#if 0
#define AEGIS_API_IMPL_LIST                                               \
    .encrypt_detached              = AEGIS_encrypt_detached,              \
    .decrypt_detached              = AEGIS_decrypt_detached,              \
    .encrypt_unauthenticated       = AEGIS_encrypt_unauthenticated,       \
    .decrypt_unauthenticated       = AEGIS_decrypt_unauthenticated,       \
    .stream                        = AEGIS_stream,                        \
    .state_init                    = AEGIS_state_init,                    \
    .state_encrypt_update          = AEGIS_state_encrypt_update,          \
    .state_encrypt_detached_final  = AEGIS_state_encrypt_detached_final,  \
    .state_encrypt_final           = AEGIS_state_encrypt_final,           \
    .state_decrypt_detached_update = AEGIS_state_decrypt_detached_update, \
    .state_decrypt_detached_final  = AEGIS_state_decrypt_detached_final,  \
    .state_mac_init                = AEGIS_state_mac_init,                \
    .state_mac_update              = AEGIS_state_mac_update,              \
    .state_mac_final               = AEGIS_state_mac_final,               \
    .state_mac_reset               = AEGIS_state_mac_reset,               \
    .state_mac_clone               = AEGIS_state_mac_clone,
#endif

#endif /* AEGIS_COMMON_H */

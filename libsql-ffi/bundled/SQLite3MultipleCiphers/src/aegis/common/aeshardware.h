/*
** Name:        aeshardware.h
** Purpose:     Header for AES hardware support detection
** Copyright:   (c) 2023-2024 Frank Denis
** SPDX-License-Identifier: MIT
*/

#ifndef AEGIS_AES_HARDWARE_H
#define AEGIS_AES_HARDWARE_H

#define AEGIS_AES_HARDWARE_NONE     0
#define AEGIS_AES_HARDWARE_NI       1
#define AEGIS_AES_HARDWARE_NEON     2
#define AEGIS_AES_HARDWARE_ALTIVEC  3

#ifndef AEGIS_OMIT_AES_HARDWARE_SUPPORT

#if defined __ARM_FEATURE_CRYPTO
#define HAS_AEGIS_AES_HARDWARE AEGIS_AES_HARDWARE_NEON


/* --- CLang --- */
#elif defined(__clang__)

#if __has_attribute(target) && __has_include(<wmmintrin.h>) && (defined(__x86_64__) || defined(__i386))
#define HAS_AEGIS_AES_HARDWARE AEGIS_AES_HARDWARE_NI

#elif __has_attribute(target) && __has_include(<arm_neon.h>) && (defined(__aarch64__))
#define HAS_AEGIS_AES_HARDWARE AEGIS_AES_HARDWARE_NEON

#endif


/* --- GNU C/C++ */
#elif defined(__GNUC__)

#if (__GNUC__ > 4 || (__GNUC__ == 4 && __GNUC_MINOR__ >= 4)) && (defined(__x86_64__) || defined(__i386))
#define HAS_AEGIS_AES_HARDWARE AEGIS_AES_HARDWARE_NI
#elif defined(__aarch64__) || (defined(__arm__) && defined(__ARM_NEON))
#define HAS_AEGIS_AES_HARDWARE AEGIS_AES_HARDWARE_NEON
#endif


/* --- Visual C/C++ --- */
#elif defined (_MSC_VER)

/* Architecture: x86 or x86_64 */
#if (defined(_M_X64) || defined(_M_IX86)) && _MSC_FULL_VER >= 150030729
#define HAS_AEGIS_AES_HARDWARE AEGIS_AES_HARDWARE_NI

/* Architecture: ARM 64-bit */
#elif defined(_M_ARM64)
#define HAS_AEGIS_AES_HARDWARE AEGIS_AES_HARDWARE_NEON

/* Use header <arm64_neon.h> instead of <arm_neon.h> */
#ifndef USE_ARM64_NEON_H
#define USE_ARM64_NEON_H
#endif

/* Architecture: ARM 32-bit */
#elif defined _M_ARM
#define HAS_AEGIS_AES_HARDWARE AEGIS_AES_HARDWARE_NEON

/* The following #define is required to enable intrinsic definitions
   that do not omit one of the parameters for vaes[ed]q_u8 */
#ifndef _ARM_USE_NEW_NEON_INTRINSICS
#define _ARM_USE_NEW_NEON_INTRINSICS
#endif

#endif

#else

#define HAS_AEGIS_AES_HARDWARE AEGIS_AES_HARDWARE_NONE

#endif

#if HAS_AEGIS_AES_HARDWARE == AEGIS_AES_HARDWARE_NONE

/* Original checks of libaegis */
#if defined(__ARM_FEATURE_CRYPTO) && defined(__ARM_FEATURE_AES) && defined(__ARM_NEON)
#    define HAS_AEGIS_AES_HARDWARE AEGIS_AES_HARDWARE_NEON
#elif defined(__AES__) && defined(__AVX__)
#    define HAS_AEGIS_AES_HARDWARE AEGIS_AES_HARDWARE_NI
#elif defined(__ALTIVEC__) && defined(__CRYPTO__)
#    define HAS_AEGIS_AES_HARDWARE AEGIS_AES_HARDWARE_ALTIVEC
#endif

#endif

#else /* AEGIS_OMIT_AES_HARDWARE_SUPPORT defined */

/* Omit AES hardware support */
#define HAS_AEGIS_AES_HARDWARE AEGIS_AES_HARDWARE_NONE

#endif /* SQLITE3MC_OMIT_AES_HARDWARE_SUPPORT */

#endif /* AEGIS_AES_HARDWARE_H */

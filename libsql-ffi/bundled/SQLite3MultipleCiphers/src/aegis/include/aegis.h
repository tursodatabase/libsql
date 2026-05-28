/*
** Name:        aegis.h
** Purpose:     Header for AEGIS API
** Copyright:   (c) 2023-2024 Frank Denis
** SPDX-License-Identifier: MIT
*/

#ifndef AEGIS_H
#define AEGIS_H

#include <stdint.h>

#if !defined(__clang__) && !defined(__GNUC__)
#    ifdef __attribute__
#        undef __attribute__
#    endif
#    define __attribute__(a)
#endif

#ifndef CRYPTO_ALIGN
#    if defined(__INTEL_COMPILER) || defined(_MSC_VER)
#        define CRYPTO_ALIGN(x) __declspec(align(x))
#    else
#        define CRYPTO_ALIGN(x) __attribute__((aligned(x)))
#    endif
#endif

#ifndef AEGIS_API
#define AEGIS_API
#endif
#ifndef AEGIS_PRIVATE
#define AEGIS_PRIVATE static
#endif

#include "aegis128l.h"
#include "aegis128x2.h"
#include "aegis128x4.h"
#include "aegis256.h"
#include "aegis256x2.h"
#include "aegis256x4.h"

#ifdef __cplusplus
extern "C" {
#endif

/* Initialize the AEGIS library.
 *
 * This function does runtime CPU capability detection, and must be called once
 * in your application before doing anything else with the library.
 *
 * If you don't, AEGIS will still work, but it may be much slower.
 *
 * The function can be called multiple times but is not thread-safe.
 */
AEGIS_API
int aegis_init(void);

/* Compare two 16-byte blocks for equality.
 *
 * This function is designed to be used in constant-time code.
 *
 * Returns 0 if the blocks are equal, -1 otherwise.
 */
AEGIS_API
int aegis_verify_16(const uint8_t *x, const uint8_t *y) __attribute__((warn_unused_result));

/* Compare two 32-byte blocks for equality.
 *
 * This function is designed to be used in constant-time code.
 *
 * Returns 0 if the blocks are equal, -1 otherwise.
 */
AEGIS_API
int aegis_verify_32(const uint8_t *x, const uint8_t *y) __attribute__((warn_unused_result));

#ifdef __cplusplus
}
#endif

#endif /* AEGIS_H */

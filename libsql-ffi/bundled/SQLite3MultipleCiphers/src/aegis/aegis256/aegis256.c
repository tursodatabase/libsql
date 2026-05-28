/*
** Name:        aegis256.c
** Purpose:     Implementation of AEGIS-256
** Copyright:   (c) 2023-2024 Frank Denis
** SPDX-License-Identifier: MIT
*/

#include <errno.h>
#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#include "../common/common.h"
#include "../common/cpu.h"
#include "aegis256.h"
#if 0
#include "aegis256_aesni.h"
#include "aegis256_altivec.h"
#include "aegis256_armcrypto.h"
#endif

#if HAS_AEGIS_AES_HARDWARE == AEGIS_AES_HARDWARE_NONE
#include "aegis256_soft.h"
static const aegis256_implementation *implementation_256 = &aegis256_soft_implementation;
#elif HAS_AEGIS_AES_HARDWARE == AEGIS_AES_HARDWARE_NEON
static const aegis256_implementation *implementation_256 = &aegis256_armcrypto_implementation;
#elif HAS_AEGIS_AES_HARDWARE == AEGIS_AES_HARDWARE_NI
static const aegis256_implementation *implementation_256 = &aegis256_aesni_implementation;
#elif HAS_AEGIS_AES_HARDWARE == AEGIS_AES_HARDWARE_ALTIVEC
static const aegis256_implementation *implementation_256 = &aegis256_altivec_implementation;
#else
#error "Unsupported architecture"
#endif

AEGIS_API
size_t
aegis256_keybytes(void)
{
    return aegis256_KEYBYTES;
}

AEGIS_API
size_t
aegis256_npubbytes(void)
{
    return aegis256_NPUBBYTES;
}

AEGIS_API
size_t
aegis256_abytes_min(void)
{
    return aegis256_ABYTES_MIN;
}

AEGIS_API
size_t
aegis256_abytes_max(void)
{
    return aegis256_ABYTES_MAX;
}

AEGIS_API
size_t
aegis256_tailbytes_max(void)
{
    return aegis256_TAILBYTES_MAX;
}

AEGIS_API
int
aegis256_encrypt_detached(uint8_t *c, uint8_t *mac, size_t maclen, const uint8_t *m, size_t mlen,
                          const uint8_t *ad, size_t adlen, const uint8_t *npub, const uint8_t *k)
{
    if (maclen != 16 && maclen != 32) {
        errno = EINVAL;
        return -1;
    }
    return implementation_256->encrypt_detached(c, mac, maclen, m, mlen, ad, adlen, npub, k);
}

AEGIS_API
int
aegis256_decrypt_detached(uint8_t *m, const uint8_t *c, size_t clen, const uint8_t *mac,
                          size_t maclen, const uint8_t *ad, size_t adlen, const uint8_t *npub,
                          const uint8_t *k)
{
    if (maclen != 16 && maclen != 32) {
        errno = EINVAL;
        return -1;
    }
    return implementation_256->decrypt_detached(m, c, clen, mac, maclen, ad, adlen, npub, k);
}

AEGIS_API
int
aegis256_encrypt(uint8_t *c, size_t maclen, const uint8_t *m, size_t mlen, const uint8_t *ad,
                 size_t adlen, const uint8_t *npub, const uint8_t *k)
{
    return aegis256_encrypt_detached(c, c + mlen, maclen, m, mlen, ad, adlen, npub, k);
}

AEGIS_API
int
aegis256_decrypt(uint8_t *m, const uint8_t *c, size_t clen, size_t maclen, const uint8_t *ad,
                 size_t adlen, const uint8_t *npub, const uint8_t *k)
{
    int ret = -1;

    if (clen >= maclen) {
        ret = aegis256_decrypt_detached(m, c, clen - maclen, c + clen - maclen, maclen, ad, adlen,
                                        npub, k);
    }
    return ret;
}

#ifndef AEGIS_OMIT_INCREMENTAL

AEGIS_API
void
aegis256_state_init(aegis256_state *st_, const uint8_t *ad, size_t adlen, const uint8_t *npub,
                    const uint8_t *k)
{
    memset(st_, 0, sizeof *st_);
    implementation_256->state_init(st_, ad, adlen, npub, k);
}

AEGIS_API
int
aegis256_state_encrypt_update(aegis256_state *st_, uint8_t *c, size_t clen_max, size_t *written,
                              const uint8_t *m, size_t mlen)
{
    return implementation_256->state_encrypt_update(st_, c, clen_max, written, m, mlen);
}

AEGIS_API
int
aegis256_state_encrypt_detached_final(aegis256_state *st_, uint8_t *c, size_t clen_max,
                                      size_t *written, uint8_t *mac, size_t maclen)
{
    if (maclen != 16 && maclen != 32) {
        errno = EINVAL;
        return -1;
    }
    return implementation_256->state_encrypt_detached_final(st_, c, clen_max, written, mac, maclen);
}

AEGIS_API
int
aegis256_state_encrypt_final(aegis256_state *st_, uint8_t *c, size_t clen_max, size_t *written,
                             size_t maclen)
{
    if (maclen != 16 && maclen != 32) {
        errno = EINVAL;
        return -1;
    }
    return implementation_256->state_encrypt_final(st_, c, clen_max, written, maclen);
}

AEGIS_API
int
aegis256_state_decrypt_detached_update(aegis256_state *st_, uint8_t *m, size_t mlen_max,
                                       size_t *written, const uint8_t *c, size_t clen)
{
    return implementation_256->state_decrypt_detached_update(st_, m, mlen_max, written, c, clen);
}

AEGIS_API
int
aegis256_state_decrypt_detached_final(aegis256_state *st_, uint8_t *m, size_t mlen_max,
                                      size_t *written, const uint8_t *mac, size_t maclen)
{
    if (maclen != 16 && maclen != 32) {
        errno = EINVAL;
        return -1;
    }
    return implementation_256->state_decrypt_detached_final(st_, m, mlen_max, written, mac, maclen);
}

#endif /* AEGIS_OMIT_INCREMENTAL */

AEGIS_API
void
aegis256_stream(uint8_t *out, size_t len, const uint8_t *npub, const uint8_t *k)
{
    implementation_256->stream(out, len, npub, k);
}

AEGIS_API
void
aegis256_encrypt_unauthenticated(uint8_t *c, const uint8_t *m, size_t mlen, const uint8_t *npub,
                                 const uint8_t *k)
{
    implementation_256->encrypt_unauthenticated(c, m, mlen, npub, k);
}

AEGIS_API
void
aegis256_decrypt_unauthenticated(uint8_t *m, const uint8_t *c, size_t clen, const uint8_t *npub,
                                 const uint8_t *k)
{
    implementation_256->decrypt_unauthenticated(m, c, clen, npub, k);
}

#ifndef AEGIS_OMIT_MAC_API

AEGIS_API
void
aegis256_mac_init(aegis256_mac_state *st_, const uint8_t *k, const uint8_t *npub)
{
    implementation_256->state_mac_init(st_, npub, k);
}

AEGIS_API
int
aegis256_mac_update(aegis256_mac_state *st_, const uint8_t *m, size_t mlen)
{
    return implementation_256->state_mac_update(st_, m, mlen);
}

AEGIS_API
int
aegis256_mac_final(aegis256_mac_state *st_, uint8_t *mac, size_t maclen)
{
    if (maclen != 16 && maclen != 32) {
        errno = EINVAL;
        return -1;
    }
    return implementation_256->state_mac_final(st_, mac, maclen);
}

AEGIS_API
int
aegis256_mac_verify(aegis256_mac_state *st_, const uint8_t *mac, size_t maclen)
{
    uint8_t expected_mac[32];

    switch (maclen) {
    case 16:
        implementation_256->state_mac_final(st_, expected_mac, maclen);
        return aegis_verify_16(expected_mac, mac);
    case 32:
        implementation_256->state_mac_final(st_, expected_mac, maclen);
        return aegis_verify_32(expected_mac, mac);
    default:
        errno = EINVAL;
        return -1;
    }
}

AEGIS_API
void
aegis256_mac_reset(aegis256_mac_state *st_)
{
    implementation_256->state_mac_reset(st_);
}

AEGIS_API
void
aegis256_mac_state_clone(aegis256_mac_state *dst, const aegis256_mac_state *src)
{
    implementation_256->state_mac_clone(dst, src);
}

#endif /* AEGIS_OMIT_MAC_API */

AEGIS_PRIVATE
int
aegis256_pick_best_implementation(void)
{
    implementation_256 = &aegis256_soft_implementation;

#if HAS_AEGIS_AES_HARDWARE == AEGIS_AES_HARDWARE_NEON
    if (aegis_runtime_has_armcrypto()) {
        implementation_256 = &aegis256_armcrypto_implementation;
        return 0;
    }
#endif

#if HAS_AEGIS_AES_HARDWARE == AEGIS_AES_HARDWARE_NI
    if (aegis_runtime_has_aesni() && aegis_runtime_has_avx()) {
        implementation_256 = &aegis256_aesni_implementation;
        return 0;
    }
#endif

#if HAS_AEGIS_AES_HARDWARE == AEGIS_AES_HARDWARE_ALTIVEC
    if (aegis_runtime_has_altivec()) {
        implementation_256 = &aegis256_altivec_implementation;
        return 0;
    }
#endif

    return 0; /* LCOV_EXCL_LINE */
}

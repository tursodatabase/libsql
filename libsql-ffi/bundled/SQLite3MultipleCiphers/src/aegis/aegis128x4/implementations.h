/*
** Name:        implementations.h
** Purpose:     Header for implementation structure of AEGIS-128x4
** Copyright:   (c) 2023-2024 Frank Denis
** SPDX-License-Identifier: MIT
*/

#ifndef AEGIS128X4_IMPLEMENTATIONS_H
#define AEGIS128X4_IMPLEMENTATIONS_H

#include <stddef.h>
#include <stdint.h>

#include "aegis128x4.h"

typedef struct aegis128x4_implementation {
    int (*encrypt_detached)(uint8_t *c, uint8_t *mac, size_t maclen, const uint8_t *m, size_t mlen,
                            const uint8_t *ad, size_t adlen, const uint8_t *npub, const uint8_t *k);
    int (*decrypt_detached)(uint8_t *m, const uint8_t *c, size_t clen, const uint8_t *mac,
                            size_t maclen, const uint8_t *ad, size_t adlen, const uint8_t *npub,
                            const uint8_t *k);
    void (*stream)(uint8_t *out, size_t len, const uint8_t *npub, const uint8_t *k);
    void (*encrypt_unauthenticated)(uint8_t *c, const uint8_t *m, size_t mlen, const uint8_t *npub,
                                    const uint8_t *k);
    void (*decrypt_unauthenticated)(uint8_t *m, const uint8_t *c, size_t clen, const uint8_t *npub,
                                    const uint8_t *k);
#ifndef AEGIS_OMIT_INCREMENTAL
    void (*state_init)(aegis128x4_state *st_, const uint8_t *ad, size_t adlen, const uint8_t *npub,
                       const uint8_t *k);
    int (*state_encrypt_update)(aegis128x4_state *st_, uint8_t *c, size_t clen_max, size_t *written,
                                const uint8_t *m, size_t mlen);
    int (*state_encrypt_detached_final)(aegis128x4_state *st_, uint8_t *c, size_t clen_max,
                                        size_t *written, uint8_t *mac, size_t maclen);
    int (*state_encrypt_final)(aegis128x4_state *st_, uint8_t *c, size_t clen_max, size_t *written,
                               size_t maclen);
    int (*state_decrypt_detached_update)(aegis128x4_state *st_, uint8_t *m, size_t mlen_max,
                                         size_t *written, const uint8_t *c, size_t clen);
    int (*state_decrypt_detached_final)(aegis128x4_state *st_, uint8_t *m, size_t mlen_max,
                                        size_t *written, const uint8_t *mac, size_t maclen);
#endif /* AEGIS_OMIT_INCREMENTAL */
#ifndef AEGIS_OMIT_MAC_API
    void (*state_mac_init)(aegis128x4_mac_state *st_, const uint8_t *npub, const uint8_t *k);
    int (*state_mac_update)(aegis128x4_mac_state *st_, const uint8_t *ad, size_t adlen);
    int (*state_mac_final)(aegis128x4_mac_state *st_, uint8_t *mac, size_t maclen);
    void (*state_mac_reset)(aegis128x4_mac_state *st);
    void (*state_mac_clone)(aegis128x4_mac_state *dst, const aegis128x4_mac_state *src);
#endif /* AEGIS_OMIT_MAC_API */
} aegis128x4_implementation;

#endif /* AEGIS128X4_IMPLEMENTATIONS_H */

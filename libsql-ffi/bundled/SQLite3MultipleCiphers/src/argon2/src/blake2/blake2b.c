/*
 * Argon2 reference source code package - reference C implementations
 *
 * Copyright 2015
 * Daniel Dinu, Dmitry Khovratovich, Jean-Philippe Aumasson, and Samuel Neves
 *
 * You may use this work under the terms of a Creative Commons CC0 1.0
 * License/Waiver or the Apache Public License 2.0, at your option. The terms of
 * these licenses can be found at:
 *
 * - CC0 1.0 Universal : https://creativecommons.org/publicdomain/zero/1.0
 * - Apache 2.0        : https://www.apache.org/licenses/LICENSE-2.0
 *
 * You should have received a copy of both of these licenses along with this
 * software. If not, they may be obtained at the above URLs.
 */

#include <stdint.h>
#include <string.h>
#include <stdio.h>

#include "blake2.h"
#include "blake2-impl.h"

static const uint64_t blake2b_IV[8] = {
    UINT64_C(0x6a09e667f3bcc908), UINT64_C(0xbb67ae8584caa73b),
    UINT64_C(0x3c6ef372fe94f82b), UINT64_C(0xa54ff53a5f1d36f1),
    UINT64_C(0x510e527fade682d1), UINT64_C(0x9b05688c2b3e6c1f),
    UINT64_C(0x1f83d9abfb41bd6b), UINT64_C(0x5be0cd19137e2179)};

static const unsigned int blake2b_sigma[12][16] = {
    {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15},
    {14, 10, 4, 8, 9, 15, 13, 6, 1, 12, 0, 2, 11, 7, 5, 3},
    {11, 8, 12, 0, 5, 2, 15, 13, 10, 14, 3, 6, 7, 1, 9, 4},
    {7, 9, 3, 1, 13, 12, 11, 14, 2, 6, 5, 10, 4, 0, 15, 8},
    {9, 0, 5, 7, 2, 4, 10, 15, 14, 1, 11, 12, 6, 8, 3, 13},
    {2, 12, 6, 10, 0, 11, 8, 3, 4, 13, 7, 5, 15, 14, 1, 9},
    {12, 5, 1, 15, 14, 13, 4, 10, 0, 7, 6, 3, 9, 2, 8, 11},
    {13, 11, 7, 14, 12, 1, 3, 9, 5, 0, 15, 4, 8, 6, 2, 10},
    {6, 15, 14, 9, 11, 3, 0, 8, 12, 2, 13, 7, 1, 4, 10, 5},
    {10, 2, 8, 4, 7, 6, 1, 5, 15, 11, 9, 14, 3, 12, 13, 0},
    {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15},
    {14, 10, 4, 8, 9, 15, 13, 6, 1, 12, 0, 2, 11, 7, 5, 3},
};

static BLAKE2_INLINE void blake2b_set_lastnode(blake2b_state *BS) {
    BS->f[1] = (uint64_t)-1;
}

static BLAKE2_INLINE void blake2b_set_lastblock(blake2b_state *BS) {
    if (BS->last_node) {
        blake2b_set_lastnode(BS);
    }
    BS->f[0] = (uint64_t)-1;
}

static BLAKE2_INLINE void blake2b_increment_counter(blake2b_state *BS,
                                                    uint64_t inc) {
    BS->t[0] += inc;
    BS->t[1] += (BS->t[0] < inc);
}

static BLAKE2_INLINE void blake2b_invalidate_state(blake2b_state *BS) {
    _argon2_clear_internal_memory(BS, sizeof(*BS));      /* wipe */
    blake2b_set_lastblock(BS); /* invalidate for further use */
}

static BLAKE2_INLINE void blake2b_init0(blake2b_state *BS) {
    memset(BS, 0, sizeof(*BS));
    memcpy(BS->h, blake2b_IV, sizeof(BS->h));
}

BLAKE2B_API
int blake2b_init_param(blake2b_state *BS, const blake2b_param *BP) {
    const unsigned char *p = (const unsigned char *)BP;
    unsigned int i;

    if (NULL == BP || NULL == BS) {
        return -1;
    }

    blake2b_init0(BS);
    /* IV XOR Parameter Block */
    for (i = 0; i < 8; ++i) {
        BS->h[i] ^= _blake2b_load64(&p[i * sizeof(BS->h[i])]);
    }
    BS->outlen = BP->digest_length;
    return 0;
}

/* Sequential blake2b initialization */
BLAKE2B_API
int blake2b_init(blake2b_state *BS, size_t outlen) {
    blake2b_param BP;

    if (BS == NULL) {
        return -1;
    }

    if ((outlen == 0) || (outlen > BLAKE2B_OUTBYTES)) {
        blake2b_invalidate_state(BS);
        return -1;
    }

    /* Setup Parameter Block for unkeyed BLAKE2 */
    BP.digest_length = (uint8_t)outlen;
    BP.key_length = 0;
    BP.fanout = 1;
    BP.depth = 1;
    BP.leaf_length = 0;
    BP.node_offset = 0;
    BP.node_depth = 0;
    BP.inner_length = 0;
    memset(BP.reserved, 0, sizeof(BP.reserved));
    memset(BP.salt, 0, sizeof(BP.salt));
    memset(BP.personal, 0, sizeof(BP.personal));

    return blake2b_init_param(BS, &BP);
}

BLAKE2B_API
int blake2b_init_key(blake2b_state *BS, size_t outlen, const void *key,
                     size_t keylen) {
    blake2b_param BP;

    if (BS == NULL) {
        return -1;
    }

    if ((outlen == 0) || (outlen > BLAKE2B_OUTBYTES)) {
        blake2b_invalidate_state(BS);
        return -1;
    }

    if ((key == 0) || (keylen == 0) || (keylen > BLAKE2B_KEYBYTES)) {
        blake2b_invalidate_state(BS);
        return -1;
    }

    /* Setup Parameter Block for keyed BLAKE2 */
    BP.digest_length = (uint8_t)outlen;
    BP.key_length = (uint8_t)keylen;
    BP.fanout = 1;
    BP.depth = 1;
    BP.leaf_length = 0;
    BP.node_offset = 0;
    BP.node_depth = 0;
    BP.inner_length = 0;
    memset(BP.reserved, 0, sizeof(BP.reserved));
    memset(BP.salt, 0, sizeof(BP.salt));
    memset(BP.personal, 0, sizeof(BP.personal));

    if (blake2b_init_param(BS, &BP) < 0) {
        blake2b_invalidate_state(BS);
        return -1;
    }

    {
        uint8_t block[BLAKE2B_BLOCKBYTES];
        memset(block, 0, BLAKE2B_BLOCKBYTES);
        memcpy(block, key, keylen);
        blake2b_update(BS, block, BLAKE2B_BLOCKBYTES);
        /* Burn the key from stack */
        _argon2_clear_internal_memory(block, BLAKE2B_BLOCKBYTES);
    }
    return 0;
}

static void blake2b_compress(blake2b_state *BS, const uint8_t *block) {
    uint64_t m[16];
    uint64_t v[16];
    unsigned int i, r;

    for (i = 0; i < 16; ++i) {
        m[i] = _blake2b_load64(block + i * sizeof(m[i]));
    }

    for (i = 0; i < 8; ++i) {
        v[i] = BS->h[i];
    }

    v[8] = blake2b_IV[0];
    v[9] = blake2b_IV[1];
    v[10] = blake2b_IV[2];
    v[11] = blake2b_IV[3];
    v[12] = blake2b_IV[4] ^ BS->t[0];
    v[13] = blake2b_IV[5] ^ BS->t[1];
    v[14] = blake2b_IV[6] ^ BS->f[0];
    v[15] = blake2b_IV[7] ^ BS->f[1];

#define BLAKE2B_G(r, i, a, b, c, d)                                                    \
    do {                                                                       \
        a = a + b + m[blake2b_sigma[r][2 * i + 0]];                            \
        d = _blake2b_rotr64(d ^ a, 32);                                                 \
        c = c + d;                                                             \
        b = _blake2b_rotr64(b ^ c, 24);                                                 \
        a = a + b + m[blake2b_sigma[r][2 * i + 1]];                            \
        d = _blake2b_rotr64(d ^ a, 16);                                                 \
        c = c + d;                                                             \
        b = _blake2b_rotr64(b ^ c, 63);                                                 \
    } while ((void)0, 0)

#define BLAKE2B_ROUND(r)                                                               \
    do {                                                                       \
        BLAKE2B_G(r, 0, v[0], v[4], v[8], v[12]);                                      \
        BLAKE2B_G(r, 1, v[1], v[5], v[9], v[13]);                                      \
        BLAKE2B_G(r, 2, v[2], v[6], v[10], v[14]);                                     \
        BLAKE2B_G(r, 3, v[3], v[7], v[11], v[15]);                                     \
        BLAKE2B_G(r, 4, v[0], v[5], v[10], v[15]);                                     \
        BLAKE2B_G(r, 5, v[1], v[6], v[11], v[12]);                                     \
        BLAKE2B_G(r, 6, v[2], v[7], v[8], v[13]);                                      \
        BLAKE2B_G(r, 7, v[3], v[4], v[9], v[14]);                                      \
    } while ((void)0, 0)

    for (r = 0; r < 12; ++r) {
        BLAKE2B_ROUND(r);
    }

    for (i = 0; i < 8; ++i) {
        BS->h[i] = BS->h[i] ^ v[i] ^ v[i + 8];
    }

#undef BLAKE2B_G
#undef BLAKE2B_ROUND
}

BLAKE2B_API
int blake2b_update(blake2b_state *BS, const void *in, size_t inlen) {
    const uint8_t *pin = (const uint8_t *)in;

    if (inlen == 0) {
        return 0;
    }

    /* Sanity check */
    if (BS == NULL || in == NULL) {
        return -1;
    }

    /* Is this a reused state? */
    if (BS->f[0] != 0) {
        return -1;
    }

    if (BS->buflen + inlen > BLAKE2B_BLOCKBYTES) {
        /* Complete current block */
        size_t left = BS->buflen;
        size_t fill = BLAKE2B_BLOCKBYTES - left;
        memcpy(&BS->buf[left], pin, fill);
        blake2b_increment_counter(BS, BLAKE2B_BLOCKBYTES);
        blake2b_compress(BS, BS->buf);
        BS->buflen = 0;
        inlen -= fill;
        pin += fill;
        /* Avoid buffer copies when possible */
        while (inlen > BLAKE2B_BLOCKBYTES) {
            blake2b_increment_counter(BS, BLAKE2B_BLOCKBYTES);
            blake2b_compress(BS, pin);
            inlen -= BLAKE2B_BLOCKBYTES;
            pin += BLAKE2B_BLOCKBYTES;
        }
    }
    memcpy(&BS->buf[BS->buflen], pin, inlen);
    BS->buflen += (unsigned int)inlen;
    return 0;
}

BLAKE2B_API
int blake2b_final(blake2b_state *BS, void *out, size_t outlen) {
    uint8_t buffer[BLAKE2B_OUTBYTES] = {0};
    unsigned int i;

    /* Sanity checks */
    if (BS == NULL || out == NULL || outlen < BS->outlen) {
        return -1;
    }

    /* Is this a reused state? */
    if (BS->f[0] != 0) {
        return -1;
    }

    blake2b_increment_counter(BS, BS->buflen);
    blake2b_set_lastblock(BS);
    memset(&BS->buf[BS->buflen], 0, BLAKE2B_BLOCKBYTES - BS->buflen); /* Padding */
    blake2b_compress(BS, BS->buf);

    for (i = 0; i < 8; ++i) { /* Output full hash to temp buffer */
      _blake2b_store64(buffer + sizeof(BS->h[i]) * i, BS->h[i]);
    }

    memcpy(out, buffer, BS->outlen);
    _argon2_clear_internal_memory(buffer, sizeof(buffer));
    _argon2_clear_internal_memory(BS->buf, sizeof(BS->buf));
    _argon2_clear_internal_memory(BS->h, sizeof(BS->h));
    return 0;
}

BLAKE2B_API
int blake2b(void *out, size_t outlen, const void *in, size_t inlen,
            const void *key, size_t keylen) {
    blake2b_state BS;
    int ret = -1;

    /* Verify parameters */
    if (NULL == in && inlen > 0) {
        goto fail;
    }

    if (NULL == out || outlen == 0 || outlen > BLAKE2B_OUTBYTES) {
        goto fail;
    }

    if ((NULL == key && keylen > 0) || keylen > BLAKE2B_KEYBYTES) {
        goto fail;
    }

    if (keylen > 0) {
        if (blake2b_init_key(&BS, outlen, key, keylen) < 0) {
            goto fail;
        }
    } else {
        if (blake2b_init(&BS, outlen) < 0) {
            goto fail;
        }
    }

    if (blake2b_update(&BS, in, inlen) < 0) {
        goto fail;
    }
    ret = blake2b_final(&BS, out, outlen);

fail:
    _argon2_clear_internal_memory(&BS, sizeof(BS));
    return ret;
}

/* Argon2 Team - Begin Code */
BLAKE2B_API
int blake2b_long(void *pout, size_t outlen, const void *in, size_t inlen) {
    uint8_t *out = (uint8_t *)pout;
    blake2b_state blake_state;
    uint8_t outlen_bytes[sizeof(uint32_t)] = {0};
    int ret = -1;

    if (outlen > UINT32_MAX) {
        goto fail;
    }

    /* Ensure little-endian byte order! */
    _blake2b_store32(outlen_bytes, (uint32_t)outlen);

#define BLAKE2B_TRY(statement)                                                         \
    do {                                                                       \
        ret = statement;                                                       \
        if (ret < 0) {                                                         \
            goto fail;                                                         \
        }                                                                      \
    } while ((void)0, 0)

    if (outlen <= BLAKE2B_OUTBYTES) {
        BLAKE2B_TRY(blake2b_init(&blake_state, outlen));
        BLAKE2B_TRY(blake2b_update(&blake_state, outlen_bytes, sizeof(outlen_bytes)));
        BLAKE2B_TRY(blake2b_update(&blake_state, in, inlen));
        BLAKE2B_TRY(blake2b_final(&blake_state, out, outlen));
    } else {
        uint32_t toproduce;
        uint8_t out_buffer[BLAKE2B_OUTBYTES];
        uint8_t in_buffer[BLAKE2B_OUTBYTES];
        BLAKE2B_TRY(blake2b_init(&blake_state, BLAKE2B_OUTBYTES));
        BLAKE2B_TRY(blake2b_update(&blake_state, outlen_bytes, sizeof(outlen_bytes)));
        BLAKE2B_TRY(blake2b_update(&blake_state, in, inlen));
        BLAKE2B_TRY(blake2b_final(&blake_state, out_buffer, BLAKE2B_OUTBYTES));
        memcpy(out, out_buffer, BLAKE2B_OUTBYTES / 2);
        out += BLAKE2B_OUTBYTES / 2;
        toproduce = (uint32_t)outlen - BLAKE2B_OUTBYTES / 2;

        while (toproduce > BLAKE2B_OUTBYTES) {
            memcpy(in_buffer, out_buffer, BLAKE2B_OUTBYTES);
            BLAKE2B_TRY(blake2b(out_buffer, BLAKE2B_OUTBYTES, in_buffer,
                        BLAKE2B_OUTBYTES, NULL, 0));
            memcpy(out, out_buffer, BLAKE2B_OUTBYTES / 2);
            out += BLAKE2B_OUTBYTES / 2;
            toproduce -= BLAKE2B_OUTBYTES / 2;
        }

        memcpy(in_buffer, out_buffer, BLAKE2B_OUTBYTES);
        BLAKE2B_TRY(blake2b(out_buffer, toproduce, in_buffer, BLAKE2B_OUTBYTES, NULL,
                    0));
        memcpy(out, out_buffer, toproduce);
    }
fail:
    _argon2_clear_internal_memory(&blake_state, sizeof(blake_state));
    return ret;
#undef BLAKE2B_TRY
}
/* Argon2 Team - End Code */

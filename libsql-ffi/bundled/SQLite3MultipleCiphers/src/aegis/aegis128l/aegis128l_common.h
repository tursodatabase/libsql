/*
** Name:        aegis128l_common.h
** Purpose:     Common implementation for AEGIS-128L
** Copyright:   (c) 2023-2024 Frank Denis
** SPDX-License-Identifier: MIT
*/

#define AEGIS_RATE      32
#define AEGIS_ALIGNMENT 32

typedef AEGIS_AES_BLOCK_T AEGIS_BLOCKS[8];

#define AEGIS_init     AEGIS_FUNC(init)
#define AEGIS_mac      AEGIS_FUNC(mac)
#define AEGIS_absorb   AEGIS_FUNC(absorb)
#define AEGIS_enc      AEGIS_FUNC(enc)
#define AEGIS_dec      AEGIS_FUNC(dec)
#define AEGIS_declast  AEGIS_FUNC(declast)

static void
AEGIS_init(const uint8_t *key, const uint8_t *nonce, AEGIS_AES_BLOCK_T *const state)
{
    static CRYPTO_ALIGN(AES_BLOCK_LENGTH)
        const uint8_t c0_[AES_BLOCK_LENGTH] = { 0x00, 0x01, 0x01, 0x02, 0x03, 0x05, 0x08, 0x0d,
                                                0x15, 0x22, 0x37, 0x59, 0x90, 0xe9, 0x79, 0x62 };
    static CRYPTO_ALIGN(AES_BLOCK_LENGTH)
        const uint8_t c1_[AES_BLOCK_LENGTH] = { 0xdb, 0x3d, 0x18, 0x55, 0x6d, 0xc2, 0x2f, 0xf1,
                                                0x20, 0x11, 0x31, 0x42, 0x73, 0xb5, 0x28, 0xdd };

    const AEGIS_AES_BLOCK_T c0 = AEGIS_AES_BLOCK_LOAD(c0_);
    const AEGIS_AES_BLOCK_T c1 = AEGIS_AES_BLOCK_LOAD(c1_);
    AEGIS_AES_BLOCK_T       k;
    AEGIS_AES_BLOCK_T       n;
    int               i;

    k = AEGIS_AES_BLOCK_LOAD(key);
    n = AEGIS_AES_BLOCK_LOAD(nonce);

    state[0] = AEGIS_AES_BLOCK_XOR(k, n);
    state[1] = c1;
    state[2] = c0;
    state[3] = c1;
    state[4] = AEGIS_AES_BLOCK_XOR(k, n);
    state[5] = AEGIS_AES_BLOCK_XOR(k, c0);
    state[6] = AEGIS_AES_BLOCK_XOR(k, c1);
    state[7] = AEGIS_AES_BLOCK_XOR(k, c0);
    for (i = 0; i < 10; i++) {
      AEGIS_update(state, n, k);
    }
}

static void
AEGIS_mac(uint8_t *mac, size_t maclen, uint64_t adlen, uint64_t mlen, AEGIS_AES_BLOCK_T *const state)
{
    AEGIS_AES_BLOCK_T tmp;
    int         i;

    tmp = AEGIS_AES_BLOCK_LOAD_64x2(mlen << 3, adlen << 3);
    tmp = AEGIS_AES_BLOCK_XOR(tmp, state[2]);

    for (i = 0; i < 7; i++) {
      AEGIS_update(state, tmp, tmp);
    }

    if (maclen == 16) {
        tmp = AEGIS_AES_BLOCK_XOR(state[6], AEGIS_AES_BLOCK_XOR(state[5], state[4]));
        tmp = AEGIS_AES_BLOCK_XOR(tmp, AEGIS_AES_BLOCK_XOR(state[3], state[2]));
        tmp = AEGIS_AES_BLOCK_XOR(tmp, AEGIS_AES_BLOCK_XOR(state[1], state[0]));
        AEGIS_AES_BLOCK_STORE(mac, tmp);
    } else if (maclen == 32) {
        tmp = AEGIS_AES_BLOCK_XOR(state[3], state[2]);
        tmp = AEGIS_AES_BLOCK_XOR(tmp, AEGIS_AES_BLOCK_XOR(state[1], state[0]));
        AEGIS_AES_BLOCK_STORE(mac, tmp);
        tmp = AEGIS_AES_BLOCK_XOR(state[7], state[6]);
        tmp = AEGIS_AES_BLOCK_XOR(tmp, AEGIS_AES_BLOCK_XOR(state[5], state[4]));
        AEGIS_AES_BLOCK_STORE(mac + 16, tmp);
    } else {
        memset(mac, 0, maclen);
    }
}

static inline void
AEGIS_absorb(const uint8_t *const src, AEGIS_AES_BLOCK_T *const state)
{
    AEGIS_AES_BLOCK_T msg0, msg1;

    msg0 = AEGIS_AES_BLOCK_LOAD(src);
    msg1 = AEGIS_AES_BLOCK_LOAD(src + AES_BLOCK_LENGTH);
    AEGIS_update(state, msg0, msg1);
}

static void
AEGIS_enc(uint8_t *const dst, const uint8_t *const src, AEGIS_AES_BLOCK_T *const state)
{
    AEGIS_AES_BLOCK_T msg0, msg1;
    AEGIS_AES_BLOCK_T tmp0, tmp1;

    msg0 = AEGIS_AES_BLOCK_LOAD(src);
    msg1 = AEGIS_AES_BLOCK_LOAD(src + AES_BLOCK_LENGTH);
    tmp0 = AEGIS_AES_BLOCK_XOR(msg0, state[6]);
    tmp0 = AEGIS_AES_BLOCK_XOR(tmp0, state[1]);
    tmp1 = AEGIS_AES_BLOCK_XOR(msg1, state[5]);
    tmp1 = AEGIS_AES_BLOCK_XOR(tmp1, state[2]);
    tmp0 = AEGIS_AES_BLOCK_XOR(tmp0, AEGIS_AES_BLOCK_AND(state[2], state[3]));
    tmp1 = AEGIS_AES_BLOCK_XOR(tmp1, AEGIS_AES_BLOCK_AND(state[6], state[7]));
    AEGIS_AES_BLOCK_STORE(dst, tmp0);
    AEGIS_AES_BLOCK_STORE(dst + AES_BLOCK_LENGTH, tmp1);

    AEGIS_update(state, msg0, msg1);
}

static void
AEGIS_dec(uint8_t *const dst, const uint8_t *const src, AEGIS_AES_BLOCK_T *const state)
{
    AEGIS_AES_BLOCK_T msg0, msg1;

    msg0 = AEGIS_AES_BLOCK_LOAD(src);
    msg1 = AEGIS_AES_BLOCK_LOAD(src + AES_BLOCK_LENGTH);
    msg0 = AEGIS_AES_BLOCK_XOR(msg0, state[6]);
    msg0 = AEGIS_AES_BLOCK_XOR(msg0, state[1]);
    msg1 = AEGIS_AES_BLOCK_XOR(msg1, state[5]);
    msg1 = AEGIS_AES_BLOCK_XOR(msg1, state[2]);
    msg0 = AEGIS_AES_BLOCK_XOR(msg0, AEGIS_AES_BLOCK_AND(state[2], state[3]));
    msg1 = AEGIS_AES_BLOCK_XOR(msg1, AEGIS_AES_BLOCK_AND(state[6], state[7]));
    AEGIS_AES_BLOCK_STORE(dst, msg0);
    AEGIS_AES_BLOCK_STORE(dst + AES_BLOCK_LENGTH, msg1);

    AEGIS_update(state, msg0, msg1);
}

static void
AEGIS_declast(uint8_t *const dst, const uint8_t *const src, size_t len,
                  AEGIS_AES_BLOCK_T *const state)
{
    uint8_t     pad[AEGIS_RATE];
    AEGIS_AES_BLOCK_T msg0, msg1;

    memset(pad, 0, sizeof pad);
    memcpy(pad, src, len);

    msg0 = AEGIS_AES_BLOCK_LOAD(pad);
    msg1 = AEGIS_AES_BLOCK_LOAD(pad + AES_BLOCK_LENGTH);
    msg0 = AEGIS_AES_BLOCK_XOR(msg0, state[6]);
    msg0 = AEGIS_AES_BLOCK_XOR(msg0, state[1]);
    msg1 = AEGIS_AES_BLOCK_XOR(msg1, state[5]);
    msg1 = AEGIS_AES_BLOCK_XOR(msg1, state[2]);
    msg0 = AEGIS_AES_BLOCK_XOR(msg0, AEGIS_AES_BLOCK_AND(state[2], state[3]));
    msg1 = AEGIS_AES_BLOCK_XOR(msg1, AEGIS_AES_BLOCK_AND(state[6], state[7]));
    AEGIS_AES_BLOCK_STORE(pad, msg0);
    AEGIS_AES_BLOCK_STORE(pad + AES_BLOCK_LENGTH, msg1);

    memset(pad + len, 0, sizeof pad - len);
    memcpy(dst, pad, len);

    msg0 = AEGIS_AES_BLOCK_LOAD(pad);
    msg1 = AEGIS_AES_BLOCK_LOAD(pad + AES_BLOCK_LENGTH);

    AEGIS_update(state, msg0, msg1);
}

static int
AEGIS_encrypt_detached(uint8_t *c, uint8_t *mac, size_t maclen, const uint8_t *m, size_t mlen,
                       const uint8_t *ad, size_t adlen, const uint8_t *npub, const uint8_t *k)
{
    AEGIS_BLOCKS                    state;
    CRYPTO_ALIGN(AEGIS_ALIGNMENT) uint8_t src[AEGIS_RATE];
    CRYPTO_ALIGN(AEGIS_ALIGNMENT) uint8_t dst[AEGIS_RATE];
    size_t                          i;

    AEGIS_init(k, npub, state);

    for (i = 0; i + AEGIS_RATE <= adlen; i += AEGIS_RATE) {
        AEGIS_absorb(ad + i, state);
    }
    if (adlen % AEGIS_RATE) {
        memset(src, 0, AEGIS_RATE);
        memcpy(src, ad + i, adlen % AEGIS_RATE);
        AEGIS_absorb(src, state);
    }
    for (i = 0; i + AEGIS_RATE <= mlen; i += AEGIS_RATE) {
      AEGIS_enc(c + i, m + i, state);
    }
    if (mlen % AEGIS_RATE) {
        memset(src, 0, AEGIS_RATE);
        memcpy(src, m + i, mlen % AEGIS_RATE);
        AEGIS_enc(dst, src, state);
        memcpy(c + i, dst, mlen % AEGIS_RATE);
    }

    AEGIS_mac(mac, maclen, adlen, mlen, state);

    return 0;
}

static int
AEGIS_decrypt_detached(uint8_t *m, const uint8_t *c, size_t clen, const uint8_t *mac, size_t maclen,
                       const uint8_t *ad, size_t adlen, const uint8_t *npub, const uint8_t *k)
{
    AEGIS_BLOCKS                    state;
    CRYPTO_ALIGN(AEGIS_ALIGNMENT) uint8_t src[AEGIS_RATE];
    CRYPTO_ALIGN(AEGIS_ALIGNMENT) uint8_t dst[AEGIS_RATE];
    CRYPTO_ALIGN(16) uint8_t        computed_mac[32];
    const size_t                    mlen = clen;
    size_t                          i;
    int                             ret;

    AEGIS_init(k, npub, state);

    for (i = 0; i + AEGIS_RATE <= adlen; i += AEGIS_RATE) {
        AEGIS_absorb(ad + i, state);
    }
    if (adlen % AEGIS_RATE) {
        memset(src, 0, AEGIS_RATE);
        memcpy(src, ad + i, adlen % AEGIS_RATE);
        AEGIS_absorb(src, state);
    }
    if (m != NULL) {
        for (i = 0; i + AEGIS_RATE <= mlen; i += AEGIS_RATE) {
            AEGIS_dec(m + i, c + i, state);
        }
    } else {
        for (i = 0; i + AEGIS_RATE <= mlen; i += AEGIS_RATE) {
            AEGIS_dec(dst, c + i, state);
        }
    }
    if (mlen % AEGIS_RATE) {
        if (m != NULL) {
            AEGIS_declast(m + i, c + i, mlen % AEGIS_RATE, state);
        } else {
            AEGIS_declast(dst, c + i, mlen % AEGIS_RATE, state);
        }
    }

    COMPILER_ASSERT(sizeof computed_mac >= 32);
    AEGIS_mac(computed_mac, maclen, adlen, mlen, state);
    ret = -1;
    if (maclen == 16) {
        ret = aegis_verify_16(computed_mac, mac);
    } else if (maclen == 32) {
        ret = aegis_verify_32(computed_mac, mac);
    }
    if (ret != 0 && m != NULL) {
        memset(m, 0, mlen);
    }
    return ret;
}

static void
AEGIS_stream(uint8_t *out, size_t len, const uint8_t *npub, const uint8_t *k)
{
    AEGIS_BLOCKS                    state;
    CRYPTO_ALIGN(AEGIS_ALIGNMENT) uint8_t src[AEGIS_RATE];
    CRYPTO_ALIGN(AEGIS_ALIGNMENT) uint8_t dst[AEGIS_RATE];
    size_t                          i;

    memset(src, 0, sizeof src);
    if (npub == NULL) {
        npub = src;
    }

    AEGIS_init(k, npub, state);

    for (i = 0; i + AEGIS_RATE <= len; i += AEGIS_RATE) {
      AEGIS_enc(out + i, src, state);
    }
    if (len % AEGIS_RATE) {
      AEGIS_enc(dst, src, state);
        memcpy(out + i, dst, len % AEGIS_RATE);
    }
}

static void
AEGIS_encrypt_unauthenticated(uint8_t *c, const uint8_t *m, size_t mlen, const uint8_t *npub,
                              const uint8_t *k)
{
    AEGIS_BLOCKS                    state;
    CRYPTO_ALIGN(AEGIS_ALIGNMENT) uint8_t src[AEGIS_RATE];
    CRYPTO_ALIGN(AEGIS_ALIGNMENT) uint8_t dst[AEGIS_RATE];
    size_t                          i;

    AEGIS_init(k, npub, state);

    for (i = 0; i + AEGIS_RATE <= mlen; i += AEGIS_RATE) {
      AEGIS_enc(c + i, m + i, state);
    }
    if (mlen % AEGIS_RATE) {
        memset(src, 0, AEGIS_RATE);
        memcpy(src, m + i, mlen % AEGIS_RATE);
        AEGIS_enc(dst, src, state);
        memcpy(c + i, dst, mlen % AEGIS_RATE);
    }
}

static void
AEGIS_decrypt_unauthenticated(uint8_t *m, const uint8_t *c, size_t clen, const uint8_t *npub,
                              const uint8_t *k)
{
    AEGIS_BLOCKS state;
    const size_t mlen = clen;
    size_t       i;

    AEGIS_init(k, npub, state);

    for (i = 0; i + AEGIS_RATE <= mlen; i += AEGIS_RATE) {
        AEGIS_dec(m + i, c + i, state);
    }
    if (mlen % AEGIS_RATE) {
        AEGIS_declast(m + i, c + i, mlen % AEGIS_RATE, state);
    }
}

typedef struct AEGIS_STATE {
    AEGIS_BLOCKS blocks;
    uint8_t      buf[AEGIS_RATE];
    uint64_t     adlen;
    uint64_t     mlen;
    size_t       pos;
} AEGIS_STATE;

typedef struct AEGIS_MAC_STATE {
    AEGIS_BLOCKS blocks;
    AEGIS_BLOCKS blocks0;
    uint8_t      buf[AEGIS_RATE];
    uint64_t     adlen;
    size_t       pos;
} AEGIS_MAC_STATE;

#ifndef AEGIS_OMIT_INCREMENTAL

static void
AEGIS_state_init(aegis128l_state *st_, const uint8_t *ad, size_t adlen, const uint8_t *npub,
                 const uint8_t *k)
{
    AEGIS_BLOCKS            blocks;
    AEGIS_STATE *const st =
        (AEGIS_STATE *) ((((uintptr_t) &st_->opaque) + (AEGIS_ALIGNMENT - 1)) &
                              ~(uintptr_t) (AEGIS_ALIGNMENT - 1));
    size_t i;

    COMPILER_ASSERT((sizeof *st) + AEGIS_ALIGNMENT <= sizeof *st_);
    st->mlen = 0;
    st->pos  = 0;

    memcpy(blocks, st->blocks, sizeof blocks);

    AEGIS_init(k, npub, blocks);
    for (i = 0; i + AEGIS_RATE <= adlen; i += AEGIS_RATE) {
        AEGIS_absorb(ad + i, blocks);
    }
    if (adlen % AEGIS_RATE) {
        memset(st->buf, 0, AEGIS_RATE);
        memcpy(st->buf, ad + i, adlen % AEGIS_RATE);
        AEGIS_absorb(st->buf, blocks);
    }
    st->adlen = adlen;

    memcpy(st->blocks, blocks, sizeof blocks);
}

static int
AEGIS_state_encrypt_update(aegis128l_state *st_, uint8_t *c, size_t clen_max, size_t *written,
                          const uint8_t *m, size_t mlen)
{
    AEGIS_BLOCKS            blocks;
    AEGIS_STATE *const st =
        (AEGIS_STATE *) ((((uintptr_t) &st_->opaque) + (AEGIS_ALIGNMENT - 1)) &
                              ~(uintptr_t) (AEGIS_ALIGNMENT - 1));
    size_t i = 0;
    size_t left;

    memcpy(blocks, st->blocks, sizeof blocks);

    *written = 0;
    st->mlen += mlen;
    if (st->pos != 0) {
        const size_t available = (sizeof st->buf) - st->pos;
        const size_t n         = mlen < available ? mlen : available;

        if (n != 0) {
            memcpy(st->buf + st->pos, m + i, n);
            m += n;
            mlen -= n;
            st->pos += n;
        }
        if (st->pos == sizeof st->buf) {
            if (clen_max < AEGIS_RATE) {
                errno = ERANGE;
                return -1;
            }
            clen_max -= AEGIS_RATE;
            AEGIS_enc(c, st->buf, blocks);
            *written += AEGIS_RATE;
            c += AEGIS_RATE;
            st->pos = 0;
        } else {
            return 0;
        }
    }
    if (clen_max < (mlen & ~(size_t) (AEGIS_RATE - 1))) {
        errno = ERANGE;
        return -1;
    }
    for (i = 0; i + AEGIS_RATE <= mlen; i += AEGIS_RATE) {
      AEGIS_enc(c + i, m + i, blocks);
    }
    *written += i;
    left = mlen % AEGIS_RATE;
    if (left != 0) {
        memcpy(st->buf, m + i, left);
        st->pos = left;
    }

    memcpy(st->blocks, blocks, sizeof blocks);

    return 0;
}

static int
AEGIS_state_encrypt_detached_final(aegis128l_state *st_, uint8_t *c, size_t clen_max, size_t *written,
                                   uint8_t *mac, size_t maclen)
{
    AEGIS_BLOCKS            blocks;
    AEGIS_STATE *const st =
        (AEGIS_STATE *) ((((uintptr_t) &st_->opaque) + (AEGIS_ALIGNMENT - 1)) &
                              ~(uintptr_t) (AEGIS_ALIGNMENT - 1));
    CRYPTO_ALIGN(AEGIS_ALIGNMENT) uint8_t src[AEGIS_RATE];
    CRYPTO_ALIGN(AEGIS_ALIGNMENT) uint8_t dst[AEGIS_RATE];

    memcpy(blocks, st->blocks, sizeof blocks);

    *written = 0;
    if (clen_max < st->pos) {
        errno = ERANGE;
        return -1;
    }
    if (st->pos != 0) {
        memset(src, 0, sizeof src);
        memcpy(src, st->buf, st->pos);
        AEGIS_enc(dst, src, blocks);
        memcpy(c, dst, st->pos);
    }
    AEGIS_mac(mac, maclen, st->adlen, st->mlen, blocks);

    *written = st->pos;

    memcpy(st->blocks, blocks, sizeof blocks);

    return 0;
}

static int
AEGIS_state_encrypt_final(aegis128l_state *st_, uint8_t *c, size_t clen_max, size_t *written,
                          size_t maclen)
{
    AEGIS_BLOCKS            blocks;
    AEGIS_STATE *const st =
        (AEGIS_STATE *) ((((uintptr_t) &st_->opaque) + (AEGIS_ALIGNMENT - 1)) &
                              ~(uintptr_t) (AEGIS_ALIGNMENT - 1));
    CRYPTO_ALIGN(AEGIS_ALIGNMENT) uint8_t src[AEGIS_RATE];
    CRYPTO_ALIGN(AEGIS_ALIGNMENT) uint8_t dst[AEGIS_RATE];

    memcpy(blocks, st->blocks, sizeof blocks);

    *written = 0;
    if (clen_max < st->pos + maclen) {
        errno = ERANGE;
        return -1;
    }
    if (st->pos != 0) {
        memset(src, 0, sizeof src);
        memcpy(src, st->buf, st->pos);
        AEGIS_enc(dst, src, blocks);
        memcpy(c, dst, st->pos);
    }
    AEGIS_mac(c + st->pos, maclen, st->adlen, st->mlen, blocks);

    *written = st->pos + maclen;

    memcpy(st->blocks, blocks, sizeof blocks);

    return 0;
}

static int
AEGIS_state_decrypt_detached_update(aegis128l_state *st_, uint8_t *m, size_t mlen_max, size_t *written,
                                    const uint8_t *c, size_t clen)
{
    AEGIS_BLOCKS            blocks;
    AEGIS_STATE *const st =
        (AEGIS_STATE *) ((((uintptr_t) &st_->opaque) + (AEGIS_ALIGNMENT - 1)) &
                              ~(uintptr_t) (AEGIS_ALIGNMENT - 1));
    CRYPTO_ALIGN(AEGIS_ALIGNMENT) uint8_t dst[AEGIS_RATE];
    size_t                          i = 0;
    size_t                          left;

    memcpy(blocks, st->blocks, sizeof blocks);

    *written = 0;
    st->mlen += clen;

    if (st->pos != 0) {
        const size_t available = (sizeof st->buf) - st->pos;
        const size_t n         = clen < available ? clen : available;

        if (n != 0) {
            memcpy(st->buf + st->pos, c, n);
            c += n;
            clen -= n;
            st->pos += n;
        }
        if (st->pos < (sizeof st->buf)) {
            return 0;
        }
        st->pos = 0;
        if (m != NULL) {
            if (mlen_max < AEGIS_RATE) {
                errno = ERANGE;
                return -1;
            }
            mlen_max -= AEGIS_RATE;
            AEGIS_dec(m, st->buf, blocks);
            m += AEGIS_RATE;
        } else {
            AEGIS_dec(dst, st->buf, blocks);
        }
        *written += AEGIS_RATE;
    }
    if (m != NULL) {
        if (mlen_max < (clen % AEGIS_RATE)) {
            errno = ERANGE;
            return -1;
        }
        for (i = 0; i + AEGIS_RATE <= clen; i += AEGIS_RATE) {
            AEGIS_dec(m + i, c + i, blocks);
        }
    } else {
        for (i = 0; i + AEGIS_RATE <= clen; i += AEGIS_RATE) {
            AEGIS_dec(dst, c + i, blocks);
        }
    }
    *written += i;
    left = clen % AEGIS_RATE;
    if (left) {
        memcpy(st->buf, c + i, left);
        st->pos = left;
    }

    memcpy(st->blocks, blocks, sizeof blocks);

    return 0;
}

static int
AEGIS_state_decrypt_detached_final(aegis128l_state *st_, uint8_t *m, size_t mlen_max, size_t *written,
                                   const uint8_t *mac, size_t maclen)
{
    AEGIS_BLOCKS                    blocks;
    CRYPTO_ALIGN(16) uint8_t        computed_mac[32];
    CRYPTO_ALIGN(AEGIS_ALIGNMENT) uint8_t dst[AEGIS_RATE];
    AEGIS_STATE *const         st =
        (AEGIS_STATE *) ((((uintptr_t) &st_->opaque) + (AEGIS_ALIGNMENT - 1)) &
                              ~(uintptr_t) (AEGIS_ALIGNMENT - 1));
    int ret;

    memcpy(blocks, st->blocks, sizeof blocks);

    *written = 0;
    if (st->pos != 0) {
        if (m != NULL) {
            if (mlen_max < st->pos) {
                errno = ERANGE;
                return -1;
            }
            AEGIS_declast(m, st->buf, st->pos, blocks);
        } else {
            AEGIS_declast(dst, st->buf, st->pos, blocks);
        }
    }
    AEGIS_mac(computed_mac, maclen, st->adlen, st->mlen, blocks);
    ret = -1;
    if (maclen == 16) {
        ret = aegis_verify_16(computed_mac, mac);
    } else if (maclen == 32) {
        ret = aegis_verify_32(computed_mac, mac);
    }
    if (ret == 0) {
        *written = st->pos;
    } else {
        memset(m, 0, st->pos);
    }

    memcpy(st->blocks, blocks, sizeof blocks);

    return ret;
}

#endif /* AEGIS_OMIT_INCREMENTAL */

#ifndef AEGIS_OMIT_MAC_API

static void
AEGIS_state_mac_init(aegis128l_mac_state *st_, const uint8_t *npub, const uint8_t *k)
{
    AEGIS_BLOCKS                blocks;
    AEGIS_MAC_STATE *const st =
        (AEGIS_MAC_STATE *) ((((uintptr_t) &st_->opaque) + (AEGIS_ALIGNMENT - 1)) &
                                  ~(uintptr_t) (AEGIS_ALIGNMENT - 1));

    COMPILER_ASSERT((sizeof *st) + AEGIS_ALIGNMENT <= sizeof *st_);
    st->pos = 0;

    memcpy(blocks, st->blocks, sizeof blocks);

    AEGIS_init(k, npub, blocks);

    memcpy(st->blocks0, blocks, sizeof blocks);
    memcpy(st->blocks, blocks, sizeof blocks);
    st->adlen = 0;
}

static int
AEGIS_state_mac_update(aegis128l_mac_state *st_, const uint8_t *ad, size_t adlen)
{
    AEGIS_BLOCKS            blocks;
    AEGIS_MAC_STATE *const st =
        (AEGIS_MAC_STATE *) ((((uintptr_t) &st_->opaque) + (AEGIS_ALIGNMENT - 1)) &
                              ~(uintptr_t) (AEGIS_ALIGNMENT - 1));
    size_t i;
    size_t left;

    memcpy(blocks, st->blocks, sizeof blocks);

    left = st->adlen % AEGIS_RATE;
    st->adlen += adlen;
    if (left != 0) {
        if (left + adlen < AEGIS_RATE) {
            memcpy(st->buf + left, ad, adlen);
            return 0;
        }
        memcpy(st->buf + left, ad, AEGIS_RATE - left);
        AEGIS_absorb(st->buf, blocks);
        ad += AEGIS_RATE - left;
        adlen -= AEGIS_RATE - left;
    }
    for (i = 0; i + AEGIS_RATE * 2 <= adlen; i += AEGIS_RATE * 2) {
        AEGIS_AES_BLOCK_T msg0, msg1, msg2, msg3;

        msg0 = AEGIS_AES_BLOCK_LOAD(ad + i + AES_BLOCK_LENGTH * 0);
        msg1 = AEGIS_AES_BLOCK_LOAD(ad + i + AES_BLOCK_LENGTH * 1);
        msg2 = AEGIS_AES_BLOCK_LOAD(ad + i + AES_BLOCK_LENGTH * 2);
        msg3 = AEGIS_AES_BLOCK_LOAD(ad + i + AES_BLOCK_LENGTH * 3);
        COMPILER_ASSERT(AES_BLOCK_LENGTH * 4 == AEGIS_RATE * 2);

        AEGIS_update(blocks, msg0, msg1);
        AEGIS_update(blocks, msg2, msg3);
    }
    for (; i + AEGIS_RATE <= adlen; i += AEGIS_RATE) {
        AEGIS_absorb(ad + i, blocks);
    }
    if (i < adlen) {
        memset(st->buf, 0, AEGIS_RATE);
        memcpy(st->buf, ad + i, adlen - i);
    }

    memcpy(st->blocks, blocks, sizeof blocks);

    return 0;
}

static int
AEGIS_state_mac_final(aegis128l_mac_state *st_, uint8_t *mac, size_t maclen)
{
    AEGIS_BLOCKS            blocks;
    AEGIS_MAC_STATE *const st =
        (AEGIS_MAC_STATE *) ((((uintptr_t) &st_->opaque) + (AEGIS_ALIGNMENT - 1)) &
                              ~(uintptr_t) (AEGIS_ALIGNMENT - 1));
    size_t left;

    memcpy(blocks, st->blocks, sizeof blocks);

    left = st->adlen % AEGIS_RATE;
    if (left != 0) {
        memset(st->buf + left, 0, AEGIS_RATE - left);
        AEGIS_absorb(st->buf, blocks);
    }
    AEGIS_mac(mac, maclen, st->adlen, maclen, blocks);

    memcpy(st->blocks, blocks, sizeof blocks);

    return 0;
}

static void
AEGIS_state_mac_reset(aegis128l_mac_state *st_)
{
    AEGIS_MAC_STATE *const st =
        (AEGIS_MAC_STATE *) ((((uintptr_t) &st_->opaque) + (AEGIS_ALIGNMENT - 1)) &
                                  ~(uintptr_t) (AEGIS_ALIGNMENT - 1));
    st->adlen = 0;
    st->pos   = 0;
    memcpy(st->blocks, st->blocks0, sizeof(AEGIS_BLOCKS));
}

static void
AEGIS_state_mac_clone(aegis128l_mac_state *dst, const aegis128l_mac_state *src)
{
    AEGIS_MAC_STATE *const dst_ =
        (AEGIS_MAC_STATE *) ((((uintptr_t) &dst->opaque) + (AEGIS_ALIGNMENT - 1)) &
                              ~(uintptr_t) (AEGIS_ALIGNMENT - 1));
    const AEGIS_MAC_STATE *const src_ =
        (const AEGIS_MAC_STATE *) ((((uintptr_t) &src->opaque) + (AEGIS_ALIGNMENT - 1)) &
                                    ~(uintptr_t) (AEGIS_ALIGNMENT - 1));
    *dst_ = *src_;
}

#endif /* AEGIS_OMIT_MAC_API */

#undef AEGIS_RATE
#undef AEGIS_ALIGNMENT

#undef AEGIS_init
#undef AEGIS_mac
#undef AEGIS_absorb
#undef AEGIS_enc
#undef AEGIS_dec
#undef AEGIS_declast

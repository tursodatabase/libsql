/*
** Name:        aead.c
** Purpose:     Stream encryption/decryption with Ascon
** Based on:    Public domain Ascon reference implementation
**              and optimized variants for 32- and 64-bit
**              (see https://github.com/ascon/ascon-c)
** Remarks:     API functions adapted for use in SQLite3 Multiple Ciphers
** Modified by: Ulrich Telle
** Copyright:   (c) 2023-2023 Ulrich Telle
** License:     MIT
*/

#include "api.h"
#include "ascon.h"
#include "crypto_aead.h"
#include "permutations.h"
#include "printstate.h"

#if !ASCON_INLINE_MODE
#undef forceinline
#define forceinline
#endif

#ifdef ASCON_AEAD_RATE

forceinline void ascon_loadkey(ascon_key_t* key, const uint8_t* k) {
#if CRYPTO_KEYBYTES == 16
  key->x[0] = ASCON_LOAD(k, 8);
  key->x[1] = ASCON_LOAD(k + 8, 8);
#else /* CRYPTO_KEYBYTES == 20 */
  key->x[0] = ASCON_KEYROT(0, ASCON_LOADBYTES(k, 4));
  key->x[1] = ASCON_LOADBYTES(k + 4, 8);
  key->x[2] = ASCON_LOADBYTES(k + 12, 8);
#endif
}

forceinline void ascon_initaead(ascon_state_t* s, const ascon_key_t* key,
                                const uint8_t* npub) {
#if CRYPTO_KEYBYTES == 16
  if (ASCON_AEAD_RATE == 8) s->x[0] = ASCON_128_IV;
  if (ASCON_AEAD_RATE == 16) s->x[0] = ASCON_128A_IV;
  s->x[1] = key->x[0];
  s->x[2] = key->x[1];
#else /* CRYPTO_KEYBYTES == 20 */
  s->x[0] = key->x[0] ^ ASCON_80PQ_IV;
  s->x[1] = key->x[1];
  s->x[2] = key->x[2];
#endif
  s->x[3] = ASCON_LOAD(npub, 8);
  s->x[4] = ASCON_LOAD(npub + 8, 8);
  ascon_printstate("init 1st key xor", s);
  ASCON_P(s, 12);
#if CRYPTO_KEYBYTES == 16
  s->x[3] ^= key->x[0];
  s->x[4] ^= key->x[1];
#else /* CRYPTO_KEYBYTES == 20 */
  s->x[2] ^= key->x[0];
  s->x[3] ^= key->x[1];
  s->x[4] ^= key->x[2];
#endif
  ascon_printstate("init 2nd key xor", s);
}

forceinline void ascon_adata(ascon_state_t* s, const uint8_t* ad,
                             uint64_t adlen) {
  const int nr = (ASCON_AEAD_RATE == 8) ? 6 : 8;
  if (adlen) {
    /* full associated data blocks */
    while (adlen >= ASCON_AEAD_RATE) {
      s->x[0] ^= ASCON_LOAD(ad, 8);
      if (ASCON_AEAD_RATE == 16) s->x[1] ^= ASCON_LOAD(ad + 8, 8);
      ascon_printstate("absorb adata", s);
      ASCON_P(s, nr);
      ad += ASCON_AEAD_RATE;
      adlen -= ASCON_AEAD_RATE;
    }
    /* final associated data block */
    uint64_t* px = &s->x[0];
    if (ASCON_AEAD_RATE == 16 && adlen >= 8) {
      s->x[0] ^= ASCON_LOAD(ad, 8);
      px = &s->x[1];
      ad += 8;
      adlen -= 8;
    }
    *px ^= ASCON_PAD(adlen);
    if (adlen) *px ^= ASCON_LOADBYTES(ad, adlen);
    ascon_printstate("pad adata", s);
    ASCON_P(s, nr);
  }
  /* domain separation */
  s->x[4] ^= 1;
  ascon_printstate("domain separation", s);
}

forceinline void ascon_encrypt(ascon_state_t* s, uint8_t* c, const uint8_t* m,
                               uint64_t mlen) {
  const int nr = (ASCON_AEAD_RATE == 8) ? 6 : 8;
  /* full plaintext blocks */
  while (mlen >= ASCON_AEAD_RATE) {
    s->x[0] ^= ASCON_LOAD(m, 8);
    ASCON_STORE(c, s->x[0], 8);
    if (ASCON_AEAD_RATE == 16) {
      s->x[1] ^= ASCON_LOAD(m + 8, 8);
      ASCON_STORE(c + 8, s->x[1], 8);
    }
    ascon_printstate("absorb plaintext", s);
    ASCON_P(s, nr);
    m += ASCON_AEAD_RATE;
    c += ASCON_AEAD_RATE;
    mlen -= ASCON_AEAD_RATE;
  }
  /* final plaintext block */
  uint64_t* px = &s->x[0];
  if (ASCON_AEAD_RATE == 16 && mlen >= 8) {
    s->x[0] ^= ASCON_LOAD(m, 8);
    ASCON_STORE(c, s->x[0], 8);
    px = &s->x[1];
    m += 8;
    c += 8;
    mlen -= 8;
  }
  *px ^= ASCON_PAD(mlen);
  if (mlen) {
    *px ^= ASCON_LOADBYTES(m, mlen);
    ASCON_STOREBYTES(c, *px, mlen);
  }
  ascon_printstate("pad plaintext", s);
}

forceinline void ascon_decrypt(ascon_state_t* s, uint8_t* m, const uint8_t* c,
                               uint64_t clen) {
  const int nr = (ASCON_AEAD_RATE == 8) ? 6 : 8;
  /* full ciphertext blocks */
  while (clen >= ASCON_AEAD_RATE) {
    uint64_t cx = ASCON_LOAD(c, 8);
    s->x[0] ^= cx;
    ASCON_STORE(m, s->x[0], 8);
    s->x[0] = cx;
    if (ASCON_AEAD_RATE == 16) {
      cx = ASCON_LOAD(c + 8, 8);
      s->x[1] ^= cx;
      ASCON_STORE(m + 8, s->x[1], 8);
      s->x[1] = cx;
    }
    ascon_printstate("insert ciphertext", s);
    ASCON_P(s, nr);
    m += ASCON_AEAD_RATE;
    c += ASCON_AEAD_RATE;
    clen -= ASCON_AEAD_RATE;
  }
  /* final ciphertext block */
  uint64_t* px = &s->x[0];
  if (ASCON_AEAD_RATE == 16 && clen >= 8) {
    uint64_t cx = ASCON_LOAD(c, 8);
    s->x[0] ^= cx;
    ASCON_STORE(m, s->x[0], 8);
    s->x[0] = cx;
    px = &s->x[1];
    m += 8;
    c += 8;
    clen -= 8;
  }
  *px ^= ASCON_PAD(clen);
  if (clen) {
    uint64_t cx = ASCON_LOADBYTES(c, clen);
    *px ^= cx;
    ASCON_STOREBYTES(m, *px, clen);
    *px = ASCON_CLEAR(*px, clen);
    *px ^= cx;
  }
  ascon_printstate("pad ciphertext", s);
}

forceinline void ascon_final(ascon_state_t* s, const ascon_key_t* key) {
#if CRYPTO_KEYBYTES == 16
  if (ASCON_AEAD_RATE == 8) {
    s->x[1] ^= key->x[0];
    s->x[2] ^= key->x[1];
  } else {
    s->x[2] ^= key->x[0];
    s->x[3] ^= key->x[1];
  }
#else /* CRYPTO_KEYBYTES == 20 */
  s->x[1] ^= KEYROT(key->x[0], key->x[1]);
  s->x[2] ^= KEYROT(key->x[1], key->x[2]);
  s->x[3] ^= KEYROT(key->x[2], 0);
#endif
  ascon_printstate("final 1st key xor", s);
  ASCON_P(s, 12);
#if CRYPTO_KEYBYTES == 16
  s->x[3] ^= key->x[0];
  s->x[4] ^= key->x[1];
#else /* CRYPTO_KEYBYTES == 20 */
  s->x[3] ^= key->x[1];
  s->x[4] ^= key->x[2];
#endif
  ascon_printstate("final 2nd key xor", s);
}

int ascon_aead_encrypt(uint8_t* ctext,
                       uint8_t tag[ASCON_AEAD_TAG_LEN],
                       const uint8_t* mtext, uint64_t mlen,
                       const uint8_t* ad, uint64_t adlen,
                       const uint8_t nonce[ASCON_AEAD_NONCE_LEN],
                       const uint8_t k[ASCON_AEAD_KEY_LEN])
{
  ascon_state_t s;
  /* perform ascon computation */
  ascon_key_t key;
  ascon_loadkey(&key, k);
  ascon_initaead(&s, &key, nonce);
  ascon_adata(&s, ad, adlen);
  ascon_encrypt(&s, ctext, mtext, mlen);
  ascon_final(&s, &key);
  /* set tag */
  ASCON_STOREBYTES(tag, s.x[3], 8);
  ASCON_STOREBYTES(tag + 8, s.x[4], 8);
  sqlite3mcSecureZeroMemory(&s, sizeof(ascon_state_t));
  sqlite3mcSecureZeroMemory(&key, sizeof(ascon_key_t));
  return 0;
}

int ascon_aead_decrypt(uint8_t* mtext,
                       const uint8_t* ctext, uint64_t clen,
                       const uint8_t* ad, uint64_t adlen,
                       const uint8_t tag[ASCON_AEAD_TAG_LEN],
                       const uint8_t nonce[ASCON_AEAD_NONCE_LEN],
                       const uint8_t k[ASCON_AEAD_KEY_LEN])
{
  int rc = 0;
  ascon_state_t s;
  if (clen < CRYPTO_ABYTES) return -1;
  /* perform ascon computation */
  ascon_key_t key;
  ascon_loadkey(&key, k);
  ascon_initaead(&s, &key, nonce);
  ascon_adata(&s, ad, adlen);
  ascon_decrypt(&s, mtext, ctext, clen);
  ascon_final(&s, &key);
  /* verify tag (should be constant time, check compiler output) */
  s.x[3] ^= ASCON_LOADBYTES(tag, 8);
  s.x[4] ^= ASCON_LOADBYTES(tag + 8, 8);
  rc = ASCON_NOTZERO(s.x[3], s.x[4]);
  sqlite3mcSecureZeroMemory(&s, sizeof(ascon_state_t));
  sqlite3mcSecureZeroMemory(&key, sizeof(ascon_key_t));
  return rc;
}

#endif

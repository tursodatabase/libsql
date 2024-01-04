#ifndef ASCON_H_
#define ASCON_H_

#include <stdint.h>

#include "api.h"
#include "config.h"

typedef union {
  uint64_t x[5];
  uint32_t w[5][2];
  uint8_t b[5][8];
} ascon_state_t;

#ifdef ASCON_AEAD_RATE

#define ASCON_KEYWORDS (CRYPTO_KEYBYTES + 7) / 8

typedef union {
  uint64_t x[ASCON_KEYWORDS];
  uint32_t w[ASCON_KEYWORDS][2];
  uint8_t b[ASCON_KEYWORDS][8];
} ascon_key_t;

#if !ASCON_INLINE_MODE

void ascon_loadkey(ascon_key_t* key, const uint8_t* k);
void ascon_initaead(ascon_state_t* s, const ascon_key_t* key,
                    const uint8_t* npub);
void ascon_adata(ascon_state_t* s, const uint8_t* ad, uint64_t adlen);
void ascon_encrypt(ascon_state_t* s, uint8_t* c, const uint8_t* m,
                   uint64_t mlen);
void ascon_decrypt(ascon_state_t* s, uint8_t* m, const uint8_t* c,
                   uint64_t clen);
void ascon_final(ascon_state_t* s, const ascon_key_t* k);

#endif

#endif

#ifdef ASCON_HASH_BYTES

#if !ASCON_INLINE_MODE

void ascon_inithash(ascon_state_t* s);
void ascon_absorb(ascon_state_t* s, const uint8_t* in, uint64_t inlen);
void ascon_squeeze(ascon_state_t* s, uint8_t* out, uint64_t outlen);

#endif

#endif

#endif /* ASCON_H_ */

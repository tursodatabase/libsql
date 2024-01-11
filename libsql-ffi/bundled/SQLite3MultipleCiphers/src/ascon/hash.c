/*
** Name:        hash.c
** Purpose:     Hash algorithm with Ascon
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
#include "crypto_hash.h"
#include "permutations.h"
#include "printstate.h"

#if !ASCON_INLINE_MODE
#undef forceinline
#define forceinline
#endif

#ifdef ASCON_HASH_BYTES

forceinline void ascon_inithash(ascon_state_t* s) {
  int i;
  /* initialize */
#ifdef ASCON_PRINT_STATE
#if ASCON_HASH_BYTES == 32 && ASCON_HASH_ROUNDS == 12
  s->x[0] = ASCON_HASH_IV;
#elif ASCON_HASH_BYTES == 32 && ASCON_HASH_ROUNDS == 8
  s->x[0] = ASCON_HASHA_IV;
#elif ASCON_HASH_BYTES == 0 && ASCON_HASH_ROUNDS == 12
  s->x[0] = ASCON_XOF_IV;
#elif ASCON_HASH_BYTES == 0 && ASCON_HASH_ROUNDS == 8
  s->x[0] = ASCON_XOFA_IV;
#endif
  for (i = 1; i < 5; ++i) s->x[i] = 0;
  ascon_printstate("initial value", s);
  ASCON_P(s, 12);
#endif
#if ASCON_HASH_BYTES == 32 && ASCON_HASH_ROUNDS == 12
  const uint64_t iv[5] = {ASCON_HASH_IV0, ASCON_HASH_IV1, ASCON_HASH_IV2,
                          ASCON_HASH_IV3, ASCON_HASH_IV4};
#elif ASCON_HASH_BYTES == 32 && ASCON_HASH_ROUNDS == 8
  const uint64_t iv[5] = {ASCON_HASHA_IV0, ASCON_HASHA_IV1, ASCON_HASHA_IV2,
                          ASCON_HASHA_IV3, ASCON_HASHA_IV4};
#elif ASCON_HASH_BYTES == 0 && ASCON_HASH_ROUNDS == 12
  const uint64_t iv[5] = {ASCON_XOF_IV0, ASCON_XOF_IV1, ASCON_XOF_IV2,
                          ASCON_XOF_IV3, ASCON_XOF_IV4};
#elif ASCON_HASH_BYTES == 0 && ASCON_HASH_ROUNDS == 8
  const uint64_t iv[5] = {ASCON_XOFA_IV0, ASCON_XOFA_IV1, ASCON_XOFA_IV2,
                          ASCON_XOFA_IV3, ASCON_XOFA_IV4};
#endif
  for (i = 0; i < 5; ++i) s->x[i] = (iv[i]);
  ascon_printstate("initialization", s);
}

forceinline void ascon_absorb(ascon_state_t* s, const uint8_t* in,
                              uint64_t inlen) {
  /* absorb full plaintext blocks */
  while (inlen >= ASCON_HASH_RATE) {
    s->x[0] ^= ASCON_LOAD(in, 8);
    ascon_printstate("absorb plaintext", s);
    ASCON_P(s, ASCON_HASH_ROUNDS);
    in += ASCON_HASH_RATE;
    inlen -= ASCON_HASH_RATE;
  }
  /* absorb final plaintext block */
  s->x[0] ^= ASCON_LOADBYTES(in, inlen);
  s->x[0] ^= ASCON_PAD(inlen);
  ascon_printstate("pad plaintext", s);
}

forceinline void ascon_squeeze(ascon_state_t* s, uint8_t* out,
                               uint64_t outlen) {
  /* squeeze full output blocks */
  ASCON_P(s, 12);
  while (outlen > ASCON_HASH_RATE) {
    ASCON_STORE(out, s->x[0], 8);
    ascon_printstate("squeeze output", s);
    ASCON_P(s, ASCON_HASH_ROUNDS);
    out += ASCON_HASH_RATE;
    outlen -= ASCON_HASH_RATE;
  }
  /* squeeze final output block */
  ASCON_STOREBYTES(out, s->x[0], outlen);
  ascon_printstate("squeeze output", s);
}

int ascon_hash(uint8_t* out, const uint8_t* in, uint64_t inlen)
{
  ascon_state_t s;
  ascon_inithash(&s);
  ascon_absorb(&s, in, inlen);
  ascon_squeeze(&s, out, ASCON_HASH_BYTES);
  sqlite3mcSecureZeroMemory(&s, sizeof(ascon_state_t));
  return 0;
}

#endif

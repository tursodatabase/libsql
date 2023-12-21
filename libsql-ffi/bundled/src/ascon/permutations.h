#ifndef PERMUTATIONS_H_
#define PERMUTATIONS_H_

#include <stdint.h>

#include "api.h"
#include "ascon.h"
#include "config.h"
#include "constants.h"
#include "printstate.h"
#include "round.h"

forceinline void ASCON_P12ROUNDS(ascon_state_t* s) {
  ASCON_ROUND(s, ASCON_RC0);
  ASCON_ROUND(s, ASCON_RC1);
  ASCON_ROUND(s, ASCON_RC2);
  ASCON_ROUND(s, ASCON_RC3);
  ASCON_ROUND(s, ASCON_RC4);
  ASCON_ROUND(s, ASCON_RC5);
  ASCON_ROUND(s, ASCON_RC6);
  ASCON_ROUND(s, ASCON_RC7);
  ASCON_ROUND(s, ASCON_RC8);
  ASCON_ROUND(s, ASCON_RC9);
  ASCON_ROUND(s, ASCON_RCa);
  ASCON_ROUND(s, ASCON_RCb);
}

forceinline void ASCON_P8ROUNDS(ascon_state_t* s) {
  ASCON_ROUND(s, ASCON_RC4);
  ASCON_ROUND(s, ASCON_RC5);
  ASCON_ROUND(s, ASCON_RC6);
  ASCON_ROUND(s, ASCON_RC7);
  ASCON_ROUND(s, ASCON_RC8);
  ASCON_ROUND(s, ASCON_RC9);
  ASCON_ROUND(s, ASCON_RCa);
  ASCON_ROUND(s, ASCON_RCb);
}

forceinline void ASCON_P6ROUNDS(ascon_state_t* s) {
  ASCON_ROUND(s, ASCON_RC6);
  ASCON_ROUND(s, ASCON_RC7);
  ASCON_ROUND(s, ASCON_RC8);
  ASCON_ROUND(s, ASCON_RC9);
  ASCON_ROUND(s, ASCON_RCa);
  ASCON_ROUND(s, ASCON_RCb);
}

#if ASCON_INLINE_PERM && ASCON_UNROLL_LOOPS

forceinline void ASCON_P(ascon_state_t* s, int nr) {
  if (nr == 12) ASCON_P12ROUNDS(s);
  if (nr == 8) ASCON_P8ROUNDS(s);
  if (nr == 6) ASCON_P6ROUNDS(s);
}

#elif !ASCON_INLINE_PERM && ASCON_UNROLL_LOOPS

void ASCON_P12(ascon_state_t* s);
void ASCON_P8(ascon_state_t* s);
void ASCON_P6(ascon_state_t* s);

forceinline void ASCON_P(ascon_state_t* s, int nr) {
  if (nr == 12) ASCON_P12(s);
  if (nr == 8) ASCON_P8(s);
  if (nr == 6) ASCON_P6(s);
}

#elif ASCON_INLINE_PERM && !ASCON_UNROLL_LOOPS

forceinline void ASCON_P(ascon_state_t* s, int nr) { ASCON_PROUNDS(s, nr); }

#else /* !ASCON_INLINE_PERM && !ASCON_UNROLL_LOOPS */

void ASCON_P(ascon_state_t* s, int nr);

#endif

#endif /* PERMUTATIONS_H_ */

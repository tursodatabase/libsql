#ifndef ROUND64_H_
#define ROUND64_H_

#include "ascon.h"
#include "constants.h"
#include "forceinline.h"
#include "printstate.h"
#include "word.h"

forceinline void ASCON_ROUND(ascon_state_t* s, uint8_t C) {
  ascon_state_t t;
  /* round constant */
  s->x[2] ^= C;
  /* s-box layer */
  s->x[0] ^= s->x[4];
  s->x[4] ^= s->x[3];
  s->x[2] ^= s->x[1];
  t.x[0] = s->x[0] ^ (~s->x[1] & s->x[2]);
  t.x[2] = s->x[2] ^ (~s->x[3] & s->x[4]);
  t.x[4] = s->x[4] ^ (~s->x[0] & s->x[1]);
  t.x[1] = s->x[1] ^ (~s->x[2] & s->x[3]);
  t.x[3] = s->x[3] ^ (~s->x[4] & s->x[0]);
  t.x[1] ^= t.x[0];
  t.x[3] ^= t.x[2];
  t.x[0] ^= t.x[4];
  /* linear layer */
  s->x[2] = t.x[2] ^ ASCON_ROR(t.x[2], 6 - 1);
  s->x[3] = t.x[3] ^ ASCON_ROR(t.x[3], 17 - 10);
  s->x[4] = t.x[4] ^ ASCON_ROR(t.x[4], 41 - 7);
  s->x[0] = t.x[0] ^ ASCON_ROR(t.x[0], 28 - 19);
  s->x[1] = t.x[1] ^ ASCON_ROR(t.x[1], 61 - 39);
  s->x[2] = t.x[2] ^ ASCON_ROR(s->x[2], 1);
  s->x[3] = t.x[3] ^ ASCON_ROR(s->x[3], 10);
  s->x[4] = t.x[4] ^ ASCON_ROR(s->x[4], 7);
  s->x[0] = t.x[0] ^ ASCON_ROR(s->x[0], 19);
  s->x[1] = t.x[1] ^ ASCON_ROR(s->x[1], 39);
  s->x[2] = ~s->x[2];
  ascon_printstate(" round output", s);
}

forceinline void ASCON_PROUNDS(ascon_state_t* s, int nr) {
  int i = ASCON_START(nr);
  do {
    ASCON_ROUND(s, ASCON_RC(i));
    i += ASCON_INC;
  } while (i != ASCON_END);
}

#endif /* ROUND64_H_ */

#ifndef ROUND32_H_
#define ROUND32_H_

#include "ascon.h"
#include "constants.h"
#include "forceinline.h"
#include "printstate.h"
#include "word.h"

forceinline void ASCON_ROUND(ascon_state_t* s, uint8_t C) {
  uint64_t xtemp;
  /* round constant */
  s->x[2] ^= C;
  /* s-box layer */
  s->x[0] ^= s->x[4];
  s->x[4] ^= s->x[3];
  s->x[2] ^= s->x[1];
  xtemp = s->x[0] & ~s->x[4];
  s->x[0] ^= s->x[2] & ~s->x[1];
  s->x[2] ^= s->x[4] & ~s->x[3];
  s->x[4] ^= s->x[1] & ~s->x[0];
  s->x[1] ^= s->x[3] & ~s->x[2];
  s->x[3] ^= xtemp;
  s->x[1] ^= s->x[0];
  s->x[3] ^= s->x[2];
  s->x[0] ^= s->x[4];
  s->x[2] = ~s->x[2];
  /* linear layer */
  s->x[0] ^=
      (s->x[0] >> 19) ^ (s->x[0] << 45) ^ (s->x[0] >> 28) ^ (s->x[0] << 36);
  s->x[1] ^=
      (s->x[1] >> 61) ^ (s->x[1] << 3) ^ (s->x[1] >> 39) ^ (s->x[1] << 25);
  s->x[2] ^=
      (s->x[2] >> 1) ^ (s->x[2] << 63) ^ (s->x[2] >> 6) ^ (s->x[2] << 58);
  s->x[3] ^=
      (s->x[3] >> 10) ^ (s->x[3] << 54) ^ (s->x[3] >> 17) ^ (s->x[3] << 47);
  s->x[4] ^=
      (s->x[4] >> 7) ^ (s->x[4] << 57) ^ (s->x[4] >> 41) ^ (s->x[4] << 23);
  ascon_printstate(" round output", s);
}

forceinline void ASCON_PROUNDS(ascon_state_t* s, int nr) {
  int i = ASCON_START(nr);
  do {
    ASCON_ROUND(s, ASCON_RC(i));
    i += ASCON_INC;
  } while (i != ASCON_END);
}

#endif /* ROUND32_H_ */

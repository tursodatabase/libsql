#ifdef ASCON_PRINT_STATE

#include "printstate.h"

#include <inttypes.h>
#include <stdio.h>
#include <string.h>

#ifndef WORDTOU64
#define WORDTOU64
#endif

#ifndef U64BIG
#define U64BIG
#endif

void printword(const char* text, const uint64_t x) {
  printf("%s=%016" PRIx64, text, U64BIG(WORDTOU64(x)));
}

void printstate(const char* text, const ascon_state_t* s) {
  int i;
  printf("%s:", text);
  for (i = strlen(text); i < 17; ++i) printf(" ");
  printword(" x0", s->x[0]);
  printword(" x1", s->x[1]);
  printword(" x2", s->x[2]);
  printword(" x3", s->x[3]);
  printword(" x4", s->x[4]);
#ifdef ASCON_PRINT_BI
  printf(" ");
  printf(" x0=%08x_%08x", s->w[0][1], s->w[0][0]);
  printf(" x1=%08x_%08x", s->w[1][1], s->w[1][0]);
  printf(" x2=%08x_%08x", s->w[2][1], s->w[2][0]);
  printf(" x3=%08x_%08x", s->w[3][1], s->w[3][0]);
  printf(" x4=%08x_%08x", s->w[4][1], s->w[4][0]);
#endif
  printf("\n");
}

#endif

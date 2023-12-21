#ifndef WORD_H_
#define WORD_H_

#include <stdint.h>
#include <string.h>

#include "bendian.h"
#include "forceinline.h"

typedef union {
  uint64_t x;
  uint32_t w[2];
  uint8_t b[8];
} word_t;

#define ASCON_U64TOWORD(x) ASCON_U64BIG(x)
#define ASCON_WORDTOU64(x) ASCON_U64BIG(x)

forceinline uint64_t ASCON_ROR(uint64_t x, int n) { return x >> n | x << (-n & 63); }

forceinline uint64_t ASCON_KEYROT(uint64_t lo2hi, uint64_t hi2lo) {
  return lo2hi << 32 | hi2lo >> 32;
}

forceinline int ASCON_NOTZERO(uint64_t a, uint64_t b) {
  uint64_t result = a | b;
  result |= result >> 32;
  result |= result >> 16;
  result |= result >> 8;
  return ((((int)(result & 0xff) - 1) >> 8) & 1) - 1;
}

forceinline uint64_t ASCON_PAD(int i) { return 0x80ull << (56 - 8 * i); }

forceinline uint64_t ASCON_PRFS_MLEN(uint64_t len) { return len << 51; }

forceinline uint64_t ASCON_CLEAR(uint64_t w, int n) {
  /* undefined for n == 0 */
  uint64_t mask = ~0ull >> (8 * n);
  return w & mask;
}

forceinline uint64_t ASCON_MASK(int n) {
  /* undefined for n == 0 */
  return ~0ull >> (64 - 8 * n);
}

forceinline uint64_t ASCON_LOAD(const uint8_t* bytes, int n) {
  uint64_t x = *(uint64_t*)bytes & ASCON_MASK(n);
  return ASCON_U64TOWORD(x);
}

forceinline void ASCON_STORE(uint8_t* bytes, uint64_t w, int n) {
  *(uint64_t*)bytes &= ~ASCON_MASK(n);
  *(uint64_t*)bytes |= ASCON_WORDTOU64(w);
}

forceinline uint64_t ASCON_LOADBYTES(const uint8_t* bytes, int n) {
  uint64_t x = 0;
  memcpy(&x, bytes, n);
  return ASCON_U64TOWORD(x);
}

forceinline void ASCON_STOREBYTES(uint8_t* bytes, uint64_t w, int n) {
  uint64_t x = ASCON_WORDTOU64(w);
  memcpy(bytes, &x, n);
}

#endif /* WORD_H_ */

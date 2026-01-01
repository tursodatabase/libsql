// Originally from blake3 reference implementation, Public Domain
// https://github.com/oconnor663/blake3_reference_impl_c

#ifndef _BLAKE3_REFERENCE_IMPL_H
#define _BLAKE3_REFERENCE_IMPL_H

#include <stddef.h>
#include <stdint.h>

#define BLAKE3_OUT_LEN 32
#define BLAKE3_KEY_LEN 32
#define BLAKE3_BLOCK_LEN 64
#define BLAKE3_CHUNK_LEN 1024

// This struct is private.
typedef struct _blake3_chunk_state {
  uint32_t chaining_value[8];
  uint64_t chunk_counter;
  uint8_t block[BLAKE3_BLOCK_LEN];
  uint8_t block_len;
  uint8_t blocks_compressed;
  uint32_t flags;
} _blake3_chunk_state;

// An incremental hasher that can accept any number of writes.
typedef struct blake3_hasher {
  _blake3_chunk_state chunk_state;
  uint32_t key_words[8];
  uint32_t cv_stack[8 * 54]; // Space for 54 subtree chaining values:
  uint8_t cv_stack_len;      // 2^54 * CHUNK_LEN = 2^64
  uint32_t flags;
} blake3_hasher;

void blake3_hasher_init(blake3_hasher *self);
void blake3_hasher_init_keyed(blake3_hasher *self,
                              const uint8_t key[BLAKE3_KEY_LEN]);
void blake3_hasher_init_derive_key(blake3_hasher *self, const char *context);
void blake3_hasher_update(blake3_hasher *self, const void *input,
                          size_t input_len);
void blake3_hasher_finalize(const blake3_hasher *self, void *out,
                            size_t out_len);

#endif // _BLAKE3_REFERENCE_IMPL_H

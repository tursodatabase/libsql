// Adapted from https://sqlite.org/src/file/ext/misc/sha1.c
// Public domain

#ifndef __SHA1_H__
#define __SHA1_H__

#include <stddef.h>

#define SHA1_BLOCK_SIZE 20

typedef struct SHA1Context {
    unsigned int state[5];
    unsigned int count[2];
    unsigned char buffer[64];
} SHA1Context;

void* sha1_init();
void sha1_update(SHA1Context* ctx, const unsigned char data[], size_t len);
int sha1_final(SHA1Context* ctx, unsigned char hash[]);

#endif

// Created by: Peter Tripp (@notpeter)
// Public Domain

#include <stdlib.h>
#include <memory.h>
#include "crypto/blake3.h"

void* blake3_init() {
    blake3_hasher* context;
    context = malloc(sizeof(blake3_hasher));
    if (!context)
        return NULL;
    blake3_hasher_init(context);
    return context;
}

void blake3_update(blake3_hasher* ctx, const unsigned char* data, size_t len) {
    blake3_hasher_update(ctx, data, len);
}

int blake3_final(blake3_hasher* ctx, unsigned char hash[]) {
    blake3_hasher_finalize(ctx, hash, BLAKE3_OUT_LEN);
    free(ctx);
    return BLAKE3_OUT_LEN;
}

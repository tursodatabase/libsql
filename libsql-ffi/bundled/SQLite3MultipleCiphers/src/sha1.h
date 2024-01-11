/* public api for steve reid's public domain SHA-1 implementation */
/* this file is in the public domain */

#ifndef SHA1_H_
#define SHA1_H_ (1)

/** SHA-1 Context */
typedef struct {
    uint32_t h[5];
    /**< Context state */
    uint32_t count[2];
    /**< Counter       */
    uint8_t buffer[64]; /**< SHA-1 buffer  */
} sha1_ctx;

#define SHA1_BLOCK_SIZE 64
/** SHA-1 Digest size in bytes */
#define SHA1_DIGEST_SIZE 20

void sha1_init(sha1_ctx *context);

void sha1_update(sha1_ctx *context, const void *p, size_t len);

void sha1_final(sha1_ctx *context, uint8_t digest[SHA1_DIGEST_SIZE]);

void sha1_transform(sha1_ctx *context, const uint8_t buffer[64]);

#endif /* SHA1_H_ */

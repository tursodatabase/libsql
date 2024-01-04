/*
** Name:        api.h
** Purpose:     Definition of preprocessor symbols
** Based on:    Public domain Ascon reference implementation
**              and optimized variants for 32- and 64-bit
**              (see https://github.com/ascon/ascon-c)
** Remarks:     API functions adapted for use in SQLite3 Multiple Ciphers
**              Combined symbols from AEAD and HASH
** Modified by: Ulrich Telle
** Copyright:   (c) 2023-2023 Ulrich Telle
** License:     MIT
*/

#define CRYPTO_VERSION "1.2.7"
#define CRYPTO_KEYBYTES 16
#define CRYPTO_NSECBYTES 0
#define CRYPTO_NPUBBYTES 16
#define CRYPTO_ABYTES 16
#define CRYPTO_NOOVERLAP 1
#define ASCON_AEAD_RATE 8

#define CRYPTO_BYTES 32
#define ASCON_HASH_BYTES 32 /* HASH */
#define ASCON_HASH_ROUNDS 12

#define ASCON_AEAD_KEY_LEN CRYPTO_KEYBYTES
#define ASCON_AEAD_NONCE_LEN CRYPTO_NPUBBYTES
#define ASCON_AEAD_TAG_LEN CRYPTO_ABYTES
#define ASCON_SALT_LEN CRYPTO_NPUBBYTES

/*
** Name:        hash.c
** Purpose:     API definition for Hash algorithm with Ascon
** Based on:    Public domain Ascon reference implementation
**              and optimized variants for 32- and 64-bit
**              (see https://github.com/ascon/ascon-c)
** Remarks:     API functions adapted for use in SQLite3 Multiple Ciphers
** Modified by: Ulrich Telle
** Copyright:   (c) 2023-2023 Ulrich Telle
** License:     MIT
*/

#ifndef CRYPTO_AEAD_H
#define CRYPTO_AEAD_H

#include <stddef.h>

/*
** Encryption using ASCON-AEAD.
**
** \param ctext Output buffer for encrypted text (same length as plain text)
** \param tag Output buffer for tag with fixed length of ASCON_AEAD_TAG_LEN
** \param mtext Input buffer with plain message text
** \param mlen Length of message text
** \param ad Input buffer with associated data
** \param adlen Length of associated data
** \param nonce Buffer with nonce data
** \param k Buffer with key data
*/
int ascon_aead_encrypt(uint8_t* ctext, uint8_t tag[ASCON_AEAD_TAG_LEN],
                       const uint8_t* mtext, uint64_t mlen,
                       const uint8_t* ad, uint64_t adlen,
                       const uint8_t nonce[ASCON_AEAD_NONCE_LEN],
                       const uint8_t k[ASCON_AEAD_KEY_LEN]);

/*
** Encryption using ASCON-AEAD.
**
** \param mtext Output buffer with decrypted plain message text  (same length as encrypted text)
** \param ctext Input buffer for encrypted text
** \param clen Length of encrypted text
** \param ad Input buffer with associated data
** \param adlen Length of associated data
** \param tag Input buffer for expected tag with fixed length of ASCON_AEAD_TAG_LEN
** \param nonce Buffer with nonce data
** \param k Buffer with key data
*/
int ascon_aead_decrypt(uint8_t* mtext, const uint8_t* ctext, uint64_t clen,
                       const uint8_t* ad, uint64_t adlen,
                       const uint8_t tag[ASCON_AEAD_TAG_LEN],
                       const uint8_t nonce[ASCON_AEAD_NONCE_LEN],
                       const uint8_t k[ASCON_AEAD_KEY_LEN]);

#endif

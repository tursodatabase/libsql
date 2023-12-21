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

#ifndef CRYPTO_HASH_H
#define CRYPTO_HASH_H

#include <stddef.h>

/*
** Derives hash value using ASCON-HASH.
**
** \param out Output buffer for hash with fixed length of ASCON_HASH_BYTES
** \param in Buffer with input data
** \param passwordlen Length of input data in bytes
*/
int ascon_hash(uint8_t* out, const uint8_t* in, uint64_t inlen);

#endif

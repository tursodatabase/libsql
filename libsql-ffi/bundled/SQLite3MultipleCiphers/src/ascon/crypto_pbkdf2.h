/*
** Name:        pbkdf2.h
** Purpose:     API definition of PBKDF2 algoritm with Ascon
**              Password-based key derivation function based on ASCON.
**              (see https://tools.ietf.org/html/rfc8018)
** Based on:    Public domain Ascon reference implementation
**              and optimized variants for 32- and 64-bit
**              (see https://github.com/ascon/ascon-c)
**              and the paper "Additional Modes for ASCON Version 1.1"
**              by Rhys Weatherley, Southern Storm Software, Pty Ltd.
** Remarks:     API functions adapted for use in SQLite3 Multiple Ciphers
** Created by:  Ulrich Telle
** Copyright:   (c) 2023-2023 Ulrich Telle
** License:     MIT
*/

#ifndef ASCON_PBKDF2_H
#define ASCON_PBKDF2_H

#include <stddef.h>

/*
** Default output block size for ASCON-PBKDF2
*/
#define ASCON_PBKDF2_SIZE 32

/*
** Derives key material using ASCON-PBKDF2.
**
** \param out Output buffer for generated key material
** \param outlen Number of bytes in generated key material
** \param password Password bytes
** \param passwordlen Length of password in bytes
** \param salt Salt bytes
** \param saltlen Number of bytes in the salt
** \param count Number of iterations to perform
*/
void ascon_pbkdf2(uint8_t* out, uint32_t outlen,
                  const uint8_t* password, uint32_t passwordlen,
                  const uint8_t* salt, uint32_t saltlen, uint32_t count)

#endif

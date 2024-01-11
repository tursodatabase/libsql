/*
** Name:        sqlite3mc_config.h
** Purpose:     Header file for SQLite3 Multiple Ciphers compile-time configuration
** Author:      Ulrich Telle
** Created:     2021-09-27
** Copyright:   (c) 2019-2023 Ulrich Telle
** License:     MIT
*/

#ifndef SQLITE3MC_CONFIG_H_
#define SQLITE3MC_CONFIG_H_

/*
** Definitions of supported ciphers
*/

/*
** Compatibility with wxSQLite3
*/
#ifdef WXSQLITE3_HAVE_CIPHER_AES_128_CBC
#define HAVE_CIPHER_AES_128_CBC WXSQLITE3_HAVE_CIPHER_AES_128_CBC
#endif

#ifdef WXSQLITE3_HAVE_CIPHER_AES_256_CBC
#define HAVE_CIPHER_AES_256_CBC WXSQLITE3_HAVE_CIPHER_AES_256_CBC
#endif

#ifdef WXSQLITE3_HAVE_CIPHER_CHACHA20
#define HAVE_CIPHER_CHACHA20 WXSQLITE3_HAVE_CIPHER_CHACHA20
#endif

#ifdef WXSQLITE3_HAVE_CIPHER_SQLCIPHER
#define HAVE_CIPHER_SQLCIPHER WXSQLITE3_HAVE_CIPHER_SQLCIPHER
#endif

#ifdef WXSQLITE3_HAVE_CIPHER_RC4
#define HAVE_CIPHER_RC4 WXSQLITE3_HAVE_CIPHER_RC4
#endif

#ifdef WXSQLITE3_HAVE_CIPHER_ASCON128
#define HAVE_CIPHER_ASCON128 WXSQLITE3_HAVE_CIPHER_ASCON128
#endif

/*
** Actual definitions of supported ciphers
*/
#ifndef HAVE_CIPHER_AES_128_CBC
#define HAVE_CIPHER_AES_128_CBC 1
#endif

#ifndef HAVE_CIPHER_AES_256_CBC
#define HAVE_CIPHER_AES_256_CBC 1
#endif

#ifndef HAVE_CIPHER_CHACHA20
#define HAVE_CIPHER_CHACHA20 1
#endif

#ifndef HAVE_CIPHER_SQLCIPHER
#define HAVE_CIPHER_SQLCIPHER 1
#endif

#ifndef HAVE_CIPHER_RC4
#define HAVE_CIPHER_RC4 1
#endif

#ifndef HAVE_CIPHER_ASCON128
#define HAVE_CIPHER_ASCON128 1
#endif

/*
** Disable all built-in ciphers on request
*/

#if 0
#define SQLITE3MC_OMIT_BUILTIN_CIPHERS
#endif

#ifdef SQLITE3MC_OMIT_BUILTIN_CIPHERS
#undef HAVE_CIPHER_AES_128_CBC
#undef HAVE_CIPHER_AES_256_CBC
#undef HAVE_CIPHER_CHACHA20
#undef HAVE_CIPHER_SQLCIPHER
#undef HAVE_CIPHER_RC4
#undef HAVE_CIPHER_ASCON128
#define HAVE_CIPHER_AES_128_CBC 0
#define HAVE_CIPHER_AES_256_CBC 0
#define HAVE_CIPHER_CHACHA20    0
#define HAVE_CIPHER_SQLCIPHER   0
#define HAVE_CIPHER_RC4         0
#define HAVE_CIPHER_ASCON128    0
#endif

/*
** Check that at least one cipher is be supported
*/
#if HAVE_CIPHER_AES_128_CBC == 0 &&  \
    HAVE_CIPHER_AES_256_CBC == 0 &&  \
    HAVE_CIPHER_CHACHA20    == 0 &&  \
    HAVE_CIPHER_SQLCIPHER   == 0 &&  \
    HAVE_CIPHER_RC4         == 0
#pragma message ("sqlite3mc_config.h: WARNING - No built-in cipher scheme enabled!")
#endif

/*
** Compile-time configuration
*/

/*
** Selection of default cipher scheme
**
** A specific default cipher scheme can be selected by defining
** the symbol CODEC_TYPE using one of the cipher scheme values
** CODEC_TYPE_AES128, CODEC_TYPE_AES256, CODEC_TYPE_CHACHA20,
** CODEC_TYPE_SQLCIPHER, or CODEC_TYPE_RC4.
**
** If the symbol CODEC_TYPE is not defined, CODEC_TYPE_CHACHA20
** is selected as the default cipher scheme.
*/
#if 0
#define CODEC_TYPE CODEC_TYPE_CHACHA20
#endif

/*
** Selection of legacy mode
**
** A) CODEC_TYPE_AES128 and CODEC_TYPE_AES256
**    Defining the symbol WXSQLITE3_USE_OLD_ENCRYPTION_SCHEME
**    selects the legacy mode for both cipher schemes.
**
** B) CODEC_TYPE_CHACHA20
**    Defining the symbol SQLITE3MC_USE_SQLEET_LEGACY
**    selects the legacy mode.
**
** C) CODEC_TYPE_SQLCIPHER
**    Defining the symbol SQLITE3MC_USE_SQLEET_LEGACY
**    selects the legacy mode.
**
** D) CODEC_TYPE_RC4
**    This cipher scheme is available in legacy mode only.
*/

#if 0
#define WXSQLITE3_USE_OLD_ENCRYPTION_SCHEME
#endif

#if 0
#define SQLITE3MC_USE_SQLEET_LEGACY
#endif

#if 0
#define SQLITE3MC_USE_SQLCIPHER_LEGACY
#endif

/*
** Selection of default version for SQLCipher scheme
**
** A specific default version can be selected by defining
** the symbol SQLCIPHER_VERSION_DEFAULT using one of the
** supported version values (SQLCIPHER_VERSION_1,
** SQLCIPHER_VERSION_2, SQLCIPHER_VERSION_3, SQLCIPHER_VERSION_4).
**
** If the symbol SQLCIPHER_VERSION_DEFAULT is not defined,
** version 4 (SQLCIPHER_VERSION_4) is selected as the default value.
*/

#if 0
#define SQLCIPHER_VERSION_DEFAULT SQLCIPHER_VERSION_4
#endif

#endif

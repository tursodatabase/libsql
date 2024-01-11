/*
** Name:        sqlite3mc.h
** Purpose:     Header file for SQLite3 Multiple Ciphers support
** Author:      Ulrich Telle
** Created:     2020-03-01
** Copyright:   (c) 2019-2023 Ulrich Telle
** License:     MIT
*/

#ifndef SQLITE3MC_H_
#define SQLITE3MC_H_

/*
** Define SQLite3 Multiple Ciphers version information
*/
#include "sqlite3mc_version.h"

/*
** Define SQLite3 API
*/
#include "sqlite3.h"

#ifdef SQLITE_USER_AUTHENTICATION
#include "sqlite3userauth.h"
#endif

/*
** Symbols for ciphers
*/
#define CODEC_TYPE_UNKNOWN     0
#define CODEC_TYPE_AES128      1
#define CODEC_TYPE_AES256      2
#define CODEC_TYPE_CHACHA20    3
#define CODEC_TYPE_SQLCIPHER   4
#define CODEC_TYPE_RC4         5
#define CODEC_TYPE_ASCON128    6
#define CODEC_TYPE_MAX_BUILTIN 6

/*
** Definition of API functions
*/

/*
** Define Windows specific SQLite API functions (not defined in sqlite3.h)
*/
#if SQLITE_OS_WIN == 1

#ifdef __cplusplus
extern "C" {
#endif

SQLITE_API int sqlite3_win32_set_directory(unsigned long type, void* zValue);

#ifdef __cplusplus
}
#endif

#endif

#ifdef __cplusplus
extern "C" {
#endif

/*
** Specify the key for an encrypted database.
** This routine should be called right after sqlite3_open().
**
** Arguments:
**   db       - Database to be encrypted
**   zDbName  - Name of the database (e.g. "main")
**   pKey     - Passphrase
**   nKey     - Length of passphrase
*/
SQLITE_API int sqlite3_key(sqlite3* db, const void* pKey, int nKey);
SQLITE_API int sqlite3_key_v2(sqlite3* db, const char* zDbName, const void* pKey, int nKey);

/*
** Change the key on an open database.
** If the current database is not encrypted, this routine will encrypt
** it.  If pNew==0 or nNew==0, the database is decrypted.
**
** Arguments:
**   db       - Database to be encrypted
**   zDbName  - Name of the database (e.g. "main")
**   pKey     - Passphrase
**   nKey     - Length of passphrase
*/
SQLITE_API int sqlite3_rekey(sqlite3* db, const void* pKey, int nKey);
SQLITE_API int sqlite3_rekey_v2(sqlite3* db, const char* zDbName, const void* pKey, int nKey);

/*
** Specify the activation key for a SEE database.
** Unless activated, none of the SEE routines will work.
**
** Arguments:
**   zPassPhrase  - Activation phrase
**
** Note: Provided only for API compatibility with SEE.
** Encryption support of SQLite3 Multi Cipher is always enabled.
*/
SQLITE_API void sqlite3_activate_see(const char* zPassPhrase);

/*
** Define functions for the configuration of the wxSQLite3 encryption extension
*/
SQLITE_API int sqlite3mc_cipher_count();
SQLITE_API int sqlite3mc_cipher_index(const char* cipherName);
SQLITE_API const char* sqlite3mc_cipher_name(int cipherIndex);
SQLITE_API int sqlite3mc_config(sqlite3* db, const char* paramName, int newValue);
SQLITE_API int sqlite3mc_config_cipher(sqlite3* db, const char* cipherName, const char* paramName, int newValue);
SQLITE_API unsigned char* sqlite3mc_codec_data(sqlite3* db, const char* zDbName, const char* paramName);
SQLITE_API const char* sqlite3mc_version();

#ifdef SQLITE3MC_WXSQLITE3_COMPATIBLE
SQLITE_API int wxsqlite3_config(sqlite3* db, const char* paramName, int newValue);
SQLITE_API int wxsqlite3_config_cipher(sqlite3* db, const char* cipherName, const char* paramName, int newValue);
SQLITE_API unsigned char* wxsqlite3_codec_data(sqlite3* db, const char* zDbName, const char* paramName);
#endif

/*
** Structures and functions to dynamically register a cipher
*/

/*
** Structure for a single cipher configuration parameter
**
** Components:
**   m_name      - name of parameter (1st char = alpha, rest = alphanumeric or underscore, max 63 characters)
**   m_value     - current/transient parameter value
**   m_default   - default parameter value
**   m_minValue  - minimum valid parameter value
**   m_maxValue  - maximum valid parameter value
*/
typedef struct _CipherParams
{
  char* m_name;
  int   m_value;
  int   m_default;
  int   m_minValue;
  int   m_maxValue;
} CipherParams;

/*
** Structure for a cipher API
**
** Components:
**   m_name            - name of cipher (1st char = alpha, rest = alphanumeric or underscore, max 63 characters)
**   m_allocateCipher  - Function pointer for function AllocateCipher
**   m_freeCipher      - Function pointer for function FreeCipher
**   m_cloneCipher     - Function pointer for function CloneCipher
**   m_getLegacy       - Function pointer for function GetLegacy
**   m_getPageSize     - Function pointer for function GetPageSize
**   m_getReserved     - Function pointer for function GetReserved
**   m_getSalt         - Function pointer for function GetSalt
**   m_generateKey     - Function pointer for function GenerateKey
**   m_encryptPage     - Function pointer for function EncryptPage
**   m_decryptPage     - Function pointer for function DecryptPage
*/

typedef struct BtShared BtSharedMC;

typedef void* (*AllocateCipher_t)(sqlite3* db);
typedef void  (*FreeCipher_t)(void* cipher);
typedef void  (*CloneCipher_t)(void* cipherTo, void* cipherFrom);
typedef int   (*GetLegacy_t)(void* cipher);
typedef int   (*GetPageSize_t)(void* cipher);
typedef int   (*GetReserved_t)(void* cipher);
typedef unsigned char* (*GetSalt_t)(void* cipher);
typedef void  (*GenerateKey_t)(void* cipher, BtSharedMC* pBt, char* userPassword, int passwordLength, int rekey, unsigned char* cipherSalt);
typedef int   (*EncryptPage_t)(void* cipher, int page, unsigned char* data, int len, int reserved);
typedef int   (*DecryptPage_t)(void* cipher, int page, unsigned char* data, int len, int reserved, int hmacCheck);

typedef struct _CipherDescriptor
{
  char* m_name;
  AllocateCipher_t m_allocateCipher;
  FreeCipher_t     m_freeCipher;
  CloneCipher_t    m_cloneCipher;
  GetLegacy_t      m_getLegacy;
  GetPageSize_t    m_getPageSize;
  GetReserved_t    m_getReserved;
  GetSalt_t        m_getSalt;
  GenerateKey_t    m_generateKey;
  EncryptPage_t    m_encryptPage;
  DecryptPage_t    m_decryptPage;
} CipherDescriptor;

/*
** Register a cipher
**
** Arguments:
**   desc         - Cipher descriptor structure
**   params       - Cipher configuration parameter table
**   makeDefault  - flag whether to make the cipher the default cipher
**
** Returns:
**   SQLITE_OK     - the cipher could be registered successfully
**   SQLITE_ERROR  - the cipher could not be registered
*/
SQLITE_API int sqlite3mc_register_cipher(const CipherDescriptor* desc, const CipherParams* params, int makeDefault);

#ifdef __cplusplus
}
#endif

/*
** Define public SQLite3 Multiple Ciphers VFS interface
*/
#include "sqlite3mc_vfs.h"

#endif

/*
** Name:        cipher_common.h
** Purpose:     Header for the ciphers of SQLite3 Multiple Ciphers
** Author:      Ulrich Telle
** Created:     2020-02-02
** Copyright:   (c) 2006-2022 Ulrich Telle
** License:     MIT
*/

#ifndef CIPHER_COMMON_H_
#define CIPHER_COMMON_H_

#include "sqlite3mc.h"

/*
// ATTENTION: Macro similar to that in pager.c
// TODO: Check in case of new version of SQLite
*/
#define WX_PAGER_MJ_PGNO(x) ((PENDING_BYTE/(x))+1)

#define CODEC_TYPE_DEFAULT CODEC_TYPE_CHACHA20

#ifndef CODEC_TYPE
#define CODEC_TYPE CODEC_TYPE_DEFAULT
#endif

#if CODEC_TYPE < 1 || CODEC_TYPE > CODEC_TYPE_MAX_BUILTIN
#error "Invalid codec type selected"
#endif

/*
** Define the maximum number of ciphers that can be registered
*/

/* Use a reasonable upper limit for the maximum number of ciphers */
#define CODEC_COUNT_LIMIT 16

#ifdef SQLITE3MC_MAX_CODEC_COUNT
/* Allow at least to register all built-in ciphers, but use a reasonable upper limit */
#if SQLITE3MC_MAX_CODEC_COUNT >= CODEC_TYPE_MAX_BUILTIN && SQLITE3MC_MAX_CODEC_COUNT <= CODEC_COUNT_LIMIT
#define CODEC_COUNT_MAX SQLITE3MC_MAX_CODEC_COUNT
#else
#error "Maximum cipher count not in range [CODEC_TYPE_MAX_BUILTIN .. CODEC_COUNT_LIMIT]"
#endif
#else
#define CODEC_COUNT_MAX CODEC_COUNT_LIMIT
#endif

#define CIPHER_NAME_MAXLEN 32
#define CIPHER_PARAMS_COUNT_MAX 64

#define MAXKEYLENGTH     32
#define KEYLENGTH_AES128 16
#define KEYLENGTH_AES256 32
#define KEYSALT_LENGTH   16

#define CODEC_SHA_ITER 4001

typedef struct _CodecParameter
{
  char* m_name;
  int           m_id;
  CipherParams* m_params;
} CodecParameter;

typedef struct _Codec
{
  int           m_isEncrypted;
  int           m_hmacCheck;
  int           m_walLegacy;
  /* Read cipher */
  int           m_hasReadCipher;
  int           m_readCipherType;
  void*         m_readCipher;
  int           m_readReserved;
  /* Write cipher */
  int           m_hasWriteCipher;
  int           m_writeCipherType;
  void*         m_writeCipher;
  int           m_writeReserved;

  sqlite3*      m_db; /* Pointer to DB */
#if 0
  Btree*        m_bt; /* Pointer to B-tree used by DB */
#endif
  BtShared*     m_btShared; /* Pointer to shared B-tree used by DB */
  unsigned char m_page[SQLITE_MAX_PAGE_SIZE + 24];
  int           m_pageSize;
  int           m_reserved;
  int           m_hasKeySalt;
  unsigned char m_keySalt[KEYSALT_LENGTH];
} Codec;

#define CIPHER_PARAMS_SENTINEL  { "", 0, 0, 0, 0 }
#define CIPHER_PAGE1_OFFSET 24

SQLITE_PRIVATE int sqlite3mcGetCipherParameter(CipherParams* cipherParams, const char* paramName);

SQLITE_PRIVATE int sqlite3mcGetCipherType(sqlite3* db);

SQLITE_PRIVATE CipherParams* sqlite3mcGetCipherParams(sqlite3* db, const char* cipherName);

SQLITE_PRIVATE int sqlite3mcCodecInit(Codec* codec);

SQLITE_PRIVATE void sqlite3mcCodecTerm(Codec* codec);

SQLITE_PRIVATE void sqlite3mcClearKeySalt(Codec* codec);

SQLITE_PRIVATE int sqlite3mcCodecSetup(Codec* codec, int cipherType, char* userPassword, int passwordLength);

SQLITE_PRIVATE int sqlite3mcSetupWriteCipher(Codec* codec, int cipherType, char* userPassword, int passwordLength);

SQLITE_PRIVATE void sqlite3mcSetIsEncrypted(Codec* codec, int isEncrypted);

SQLITE_PRIVATE void sqlite3mcSetReadCipherType(Codec* codec, int cipherType);

SQLITE_PRIVATE void sqlite3mcSetWriteCipherType(Codec* codec, int cipherType);

SQLITE_PRIVATE void sqlite3mcSetHasReadCipher(Codec* codec, int hasReadCipher);

SQLITE_PRIVATE void sqlite3mcSetHasWriteCipher(Codec* codec, int hasWriteCipher);

SQLITE_PRIVATE void sqlite3mcSetDb(Codec* codec, sqlite3* db);

SQLITE_PRIVATE void sqlite3mcSetBtree(Codec* codec, Btree* bt);

SQLITE_PRIVATE void sqlite3mcSetReadReserved(Codec* codec, int reserved);

SQLITE_PRIVATE void sqlite3mcSetWriteReserved(Codec* codec, int reserved);

SQLITE_PRIVATE int sqlite3mcIsEncrypted(Codec* codec);

SQLITE_PRIVATE int sqlite3mcHasReadCipher(Codec* codec);

SQLITE_PRIVATE int sqlite3mcHasWriteCipher(Codec* codec);

SQLITE_PRIVATE BtShared* sqlite3mcGetBtShared(Codec* codec);

SQLITE_PRIVATE int sqlite3mcGetPageSize(Codec* codec);

SQLITE_PRIVATE int sqlite3mcGetReadReserved(Codec* codec);

SQLITE_PRIVATE int sqlite3mcGetWriteReserved(Codec* codec);

SQLITE_PRIVATE unsigned char* sqlite3mcGetPageBuffer(Codec* codec);

SQLITE_PRIVATE int sqlite3mcGetLegacyReadCipher(Codec* codec);

SQLITE_PRIVATE int sqlite3mcGetLegacyWriteCipher(Codec* codec);

SQLITE_PRIVATE int sqlite3mcGetPageSizeReadCipher(Codec* codec);

SQLITE_PRIVATE int sqlite3mcGetPageSizeWriteCipher(Codec* codec);

SQLITE_PRIVATE int sqlite3mcGetReservedReadCipher(Codec* codec);

SQLITE_PRIVATE int sqlite3mcGetReservedWriteCipher(Codec* codec);

SQLITE_PRIVATE int sqlite3mcReservedEqual(Codec* codec);

SQLITE_PRIVATE unsigned char* sqlite3mcGetSaltWriteCipher(Codec* codec);

SQLITE_PRIVATE int sqlite3mcCodecCopy(Codec* codec, Codec* other);

SQLITE_PRIVATE void sqlite3mcGenerateReadKey(Codec* codec, char* userPassword, int passwordLength, unsigned char* cipherSalt);

SQLITE_PRIVATE void sqlite3mcGenerateWriteKey(Codec* codec, char* userPassword, int passwordLength, unsigned char* cipherSalt);

SQLITE_PRIVATE int sqlite3mcEncrypt(Codec* codec, int page, unsigned char* data, int len, int useWriteKey);

SQLITE_PRIVATE int sqlite3mcDecrypt(Codec* codec, int page, unsigned char* data, int len);

SQLITE_PRIVATE int sqlite3mcCopyCipher(Codec* codec, int read2write);

SQLITE_PRIVATE void sqlite3mcPadPassword(char* password, int pswdlen, unsigned char pswd[32]);

SQLITE_PRIVATE void sqlite3mcRC4(unsigned char* key, int keylen, unsigned char* textin, int textlen, unsigned char* textout);

SQLITE_PRIVATE void sqlite3mcGetMD5Binary(unsigned char* data, int length, unsigned char* digest);

SQLITE_PRIVATE void sqlite3mcGetSHABinary(unsigned char* data, int length, unsigned char* digest);

SQLITE_PRIVATE void sqlite3mcGenerateInitialVector(int seed, unsigned char iv[16]);

SQLITE_PRIVATE int sqlite3mcIsHexKey(const unsigned char* hex, int len);

SQLITE_PRIVATE int sqlite3mcConvertHex2Int(char c);

SQLITE_PRIVATE void sqlite3mcConvertHex2Bin(const unsigned char* hex, int len, unsigned char* bin);

SQLITE_PRIVATE int sqlite3mcConfigureFromUri(sqlite3* db, const char *zDbName, int configDefault);

SQLITE_PRIVATE void sqlite3mcConfigureSQLCipherVersion(sqlite3* db, int configDefault, int legacyVersion);

SQLITE_PRIVATE int sqlite3mcCodecAttach(sqlite3* db, int nDb, const char* zPath, const void* zKey, int nKey);

SQLITE_PRIVATE void sqlite3mcCodecGetKey(sqlite3* db, int nDb, void** zKey, int* nKey);

SQLITE_PRIVATE void sqlite3mcSecureZeroMemory(void* v, size_t n);

/* Debugging */

#if 0
#define SQLITE3MC_DEBUG
#define SQLITE3MC_DEBUG_DATA
#endif

#ifdef SQLITE3MC_DEBUG
#define SQLITE3MC_DEBUG_LOG(...)  { fprintf(stdout, __VA_ARGS__); fflush(stdout); }
#else
#define SQLITE3MC_DEBUG_LOG(...)
#endif

#ifdef SQLITE3MC_DEBUG_DATA
#define SQLITE3MC_DEBUG_HEX(DESC,BUFFER,LEN)  \
  { \
    int count; \
    printf(DESC); \
    for (count = 0; count < LEN; ++count) \
    { \
      if (count % 16 == 0) printf("\n%05x: ", count); \
      printf("%02x ", ((unsigned char*) BUFFER)[count]); \
    } \
    printf("\n"); \
    fflush(stdout); \
  }
#else
#define SQLITE3MC_DEBUG_HEX(DESC,BUFFER,LEN)
#endif

/*
** If encryption was enabled and WAL journal mode was used,
** SQLite3 Multiple Ciphers encrypted the WAL journal frames up to version 1.2.5
** within the VFS implementation. As a consequence the WAL journal file was not
** compatible with legacy encryption implementations (for example, System.Data.SQLite
** or SQLCipher). Additionally, the implementation of the WAL journal encryption
** was broken, because reading and writing of complete WAL frames was not handled
** correctly. Usually, operating in WAL journal mode worked nevertheless, but after
** crashes the WAL journal file could be corrupted leading to data loss.
**
** Version 1.3.0 introduced a new way to handle WAL journal encryption. The advantage
** is that the WAL journal file is now compatible with legacy encryption implementations.
** Unfortunately the new implementation is not compatible with that used up to version
** 1.2.5. To be able to access WAL journals created by prior versions, the configuration
** parameter 'mc_legacy_wal' was introduced. If the parameter is set to 1, then the
** prior WAL journal encryption mode is used. The default of this parameter can be set
** at compile time by setting the symbol SQLITE3MC_LEGACY_WAL accordingly, but the actual
** value can also be set at runtime using the pragma or the URI parameter 'mc_legacy_wal'.
**
** In principle, operating generally in WAL legacy mode is possible, but it is strongly
** recommended to use the WAL legacy mode only to recover WAL journals left behind by
** prior versions without data loss.
*/
#ifndef SQLITE3MC_LEGACY_WAL
#define SQLITE3MC_LEGACY_WAL 0
#endif

#endif

/*
** Name:        cipher_sqlcipher.c
** Purpose:     Implementation of cipher SQLCipher (version 1 to 4)
** Author:      Ulrich Telle
** Created:     2020-02-02
** Copyright:   (c) 2006-2020 Ulrich Telle
** License:     MIT
*/

#include "cipher_common.h"

/* --- SQLCipher AES256CBC-HMAC cipher --- */
#if HAVE_CIPHER_SQLCIPHER

#define CIPHER_NAME_SQLCIPHER "sqlcipher"

/*
** Configuration parameters for "sqlcipher"
**
** - kdf_iter        : number of iterations for key derivation
** - fast_kdf_iter   : number of iterations for hmac key
** - hmac_use        : flag whether to use hmac
** - hmac_pgno       : storage type for page number in hmac (native, le, be)
** - hmac_salt_mask  : mask byte for hmac salt
*/

#define SQLCIPHER_FAST_KDF_ITER     2
#define SQLCIPHER_HMAC_USE          1
#define SQLCIPHER_HMAC_PGNO_LE      1
#define SQLCIPHER_HMAC_PGNO_BE      2
#define SQLCIPHER_HMAC_PGNO_NATIVE  0
#define SQLCIPHER_HMAC_SALT_MASK    0x3a

#define SQLCIPHER_KDF_ALGORITHM_SHA1   0
#define SQLCIPHER_KDF_ALGORITHM_SHA256 1
#define SQLCIPHER_KDF_ALGORITHM_SHA512 2

#define SQLCIPHER_HMAC_ALGORITHM_SHA1   0
#define SQLCIPHER_HMAC_ALGORITHM_SHA256 1
#define SQLCIPHER_HMAC_ALGORITHM_SHA512 2

#define SQLCIPHER_VERSION_1   1
#define SQLCIPHER_VERSION_2   2
#define SQLCIPHER_VERSION_3   3
#define SQLCIPHER_VERSION_4   4
#define SQLCIPHER_VERSION_MAX SQLCIPHER_VERSION_4

#ifndef SQLCIPHER_VERSION_DEFAULT
#define SQLCIPHER_VERSION_DEFAULT SQLCIPHER_VERSION_4
#endif

#ifdef SQLITE3MC_USE_SQLCIPHER_LEGACY
#define SQLCIPHER_LEGACY_DEFAULT   SQLCIPHER_VERSION_DEFAULT
#else
#define SQLCIPHER_LEGACY_DEFAULT   0
#endif

#if SQLCIPHER_VERSION_DEFAULT < SQLCIPHER_VERSION_4
#define SQLCIPHER_KDF_ITER          64000
#define SQLCIPHER_LEGACY_PAGE_SIZE  1024
#define SQLCIPHER_KDF_ALGORITHM     SQLCIPHER_KDF_ALGORITHM_SHA1
#define SQLCIPHER_HMAC_ALGORITHM    SQLCIPHER_HMAC_ALGORITHM_SHA1
#else
#define SQLCIPHER_KDF_ITER          256000
#define SQLCIPHER_LEGACY_PAGE_SIZE  4096
#define SQLCIPHER_KDF_ALGORITHM  SQLCIPHER_KDF_ALGORITHM_SHA512
#define SQLCIPHER_HMAC_ALGORITHM SQLCIPHER_HMAC_ALGORITHM_SHA512
#endif

SQLITE_PRIVATE CipherParams mcSQLCipherParams[] =
{
  { "legacy",                SQLCIPHER_LEGACY_DEFAULT,   SQLCIPHER_LEGACY_DEFAULT,   0, SQLCIPHER_VERSION_MAX },
  { "legacy_page_size",      SQLCIPHER_LEGACY_PAGE_SIZE, SQLCIPHER_LEGACY_PAGE_SIZE, 0, SQLITE_MAX_PAGE_SIZE },
  { "kdf_iter",              SQLCIPHER_KDF_ITER,         SQLCIPHER_KDF_ITER,         1, 0x7fffffff },
  { "fast_kdf_iter",         SQLCIPHER_FAST_KDF_ITER,    SQLCIPHER_FAST_KDF_ITER,    1, 0x7fffffff },
  { "hmac_use",              SQLCIPHER_HMAC_USE,         SQLCIPHER_HMAC_USE,         0, 1 },
  { "hmac_pgno",             SQLCIPHER_HMAC_PGNO_LE,     SQLCIPHER_HMAC_PGNO_LE,     0, 2 },
  { "hmac_salt_mask",        SQLCIPHER_HMAC_SALT_MASK,   SQLCIPHER_HMAC_SALT_MASK,   0x00, 0xff },
  { "kdf_algorithm",         SQLCIPHER_KDF_ALGORITHM,    SQLCIPHER_KDF_ALGORITHM,    0, 2 },
  { "hmac_algorithm",        SQLCIPHER_HMAC_ALGORITHM,   SQLCIPHER_HMAC_ALGORITHM,   0, 2 },
  { "plaintext_header_size", 0,                          0,                          0, 100 /* restrict to db header size */ },
  CIPHER_PARAMS_SENTINEL
};

#define KEYLENGTH_SQLCIPHER       32
#define SALTLENGTH_SQLCIPHER      16
#define MAX_HMAC_LENGTH_SQLCIPHER SHA512_DIGEST_SIZE
#define PAGE_NONCE_LEN_SQLCIPHER  16

typedef struct _sqlCipherCipher
{
  int       m_legacy;
  int       m_legacyPageSize;
  int       m_kdfIter;
  int       m_fastKdfIter;
  int       m_hmacUse;
  int       m_hmacPgno;
  int       m_hmacSaltMask;
  int       m_kdfAlgorithm;
  int       m_hmacAlgorithm;
  int       m_plaintextHeaderSize;
  int       m_keyLength;
  uint8_t   m_key[KEYLENGTH_SQLCIPHER];
  uint8_t   m_salt[SALTLENGTH_SQLCIPHER];
  uint8_t   m_hmacKey[KEYLENGTH_SQLCIPHER];
  Rijndael* m_aes;
} SQLCipherCipher;

static void*
AllocateSQLCipherCipher(sqlite3* db)
{
  SQLCipherCipher* sqlCipherCipher = (SQLCipherCipher*) sqlite3_malloc(sizeof(SQLCipherCipher));
  if (sqlCipherCipher != NULL)
  {
    sqlCipherCipher->m_aes = (Rijndael*)sqlite3_malloc(sizeof(Rijndael));
    if (sqlCipherCipher->m_aes != NULL)
    {
      sqlCipherCipher->m_keyLength = KEYLENGTH_SQLCIPHER;
      memset(sqlCipherCipher->m_key, 0, KEYLENGTH_SQLCIPHER);
      memset(sqlCipherCipher->m_salt, 0, SALTLENGTH_SQLCIPHER);
      memset(sqlCipherCipher->m_hmacKey, 0, KEYLENGTH_SQLCIPHER);
      RijndaelCreate(sqlCipherCipher->m_aes);
    }
    else
    {
      sqlite3_free(sqlCipherCipher);
      sqlCipherCipher = NULL;
    }
  }
  if (sqlCipherCipher != NULL)
  {
    CipherParams* cipherParams = sqlite3mcGetCipherParams(db, CIPHER_NAME_SQLCIPHER);
    sqlCipherCipher->m_legacy = sqlite3mcGetCipherParameter(cipherParams, "legacy");
    sqlCipherCipher->m_legacyPageSize = sqlite3mcGetCipherParameter(cipherParams, "legacy_page_size");
    sqlCipherCipher->m_kdfIter = sqlite3mcGetCipherParameter(cipherParams, "kdf_iter");
    sqlCipherCipher->m_fastKdfIter = sqlite3mcGetCipherParameter(cipherParams, "fast_kdf_iter");
    sqlCipherCipher->m_hmacUse = sqlite3mcGetCipherParameter(cipherParams, "hmac_use");
    sqlCipherCipher->m_hmacPgno = sqlite3mcGetCipherParameter(cipherParams, "hmac_pgno");
    sqlCipherCipher->m_hmacSaltMask = sqlite3mcGetCipherParameter(cipherParams, "hmac_salt_mask");
    sqlCipherCipher->m_kdfAlgorithm = sqlite3mcGetCipherParameter(cipherParams, "kdf_algorithm");
    sqlCipherCipher->m_hmacAlgorithm = sqlite3mcGetCipherParameter(cipherParams, "hmac_algorithm");
    if (sqlCipherCipher->m_legacy >= SQLCIPHER_VERSION_4)
    {
      int plaintextHeaderSize = sqlite3mcGetCipherParameter(cipherParams, "plaintext_header_size");
      sqlCipherCipher->m_plaintextHeaderSize = (plaintextHeaderSize >=0 && plaintextHeaderSize <= 100 && plaintextHeaderSize % 16 == 0) ? plaintextHeaderSize : 0;
    }
    else
    {
      sqlCipherCipher->m_plaintextHeaderSize = 0;
    }
  }
  return sqlCipherCipher;
}

static void
FreeSQLCipherCipher(void* cipher)
{
  SQLCipherCipher* sqlCipherCipher = (SQLCipherCipher*) cipher;
  memset(sqlCipherCipher->m_aes, 0, sizeof(Rijndael));
  sqlite3_free(sqlCipherCipher->m_aes);
  memset(sqlCipherCipher, 0, sizeof(SQLCipherCipher));
  sqlite3_free(sqlCipherCipher);
}

static void
CloneSQLCipherCipher(void* cipherTo, void* cipherFrom)
{
  SQLCipherCipher* sqlCipherCipherTo = (SQLCipherCipher*) cipherTo;
  SQLCipherCipher* sqlCipherCipherFrom = (SQLCipherCipher*) cipherFrom;
  sqlCipherCipherTo->m_legacy = sqlCipherCipherFrom->m_legacy;
  sqlCipherCipherTo->m_legacyPageSize = sqlCipherCipherFrom->m_legacyPageSize;
  sqlCipherCipherTo->m_kdfIter = sqlCipherCipherFrom->m_kdfIter;
  sqlCipherCipherTo->m_fastKdfIter = sqlCipherCipherFrom->m_fastKdfIter;
  sqlCipherCipherTo->m_hmacUse = sqlCipherCipherFrom->m_hmacUse;
  sqlCipherCipherTo->m_hmacPgno = sqlCipherCipherFrom->m_hmacPgno;
  sqlCipherCipherTo->m_hmacSaltMask = sqlCipherCipherFrom->m_hmacSaltMask;
  sqlCipherCipherTo->m_kdfAlgorithm = sqlCipherCipherFrom->m_kdfAlgorithm;
  sqlCipherCipherTo->m_hmacAlgorithm = sqlCipherCipherFrom->m_hmacAlgorithm;
  sqlCipherCipherTo->m_plaintextHeaderSize = sqlCipherCipherFrom->m_plaintextHeaderSize;
  sqlCipherCipherTo->m_keyLength = sqlCipherCipherFrom->m_keyLength;
  memcpy(sqlCipherCipherTo->m_key, sqlCipherCipherFrom->m_key, KEYLENGTH_SQLCIPHER);
  memcpy(sqlCipherCipherTo->m_salt, sqlCipherCipherFrom->m_salt, SALTLENGTH_SQLCIPHER);
  memcpy(sqlCipherCipherTo->m_hmacKey, sqlCipherCipherFrom->m_hmacKey, KEYLENGTH_SQLCIPHER);
  RijndaelInvalidate(sqlCipherCipherTo->m_aes);
  RijndaelInvalidate(sqlCipherCipherFrom->m_aes);
}

static int
GetLegacySQLCipherCipher(void* cipher)
{
  SQLCipherCipher* sqlCipherCipher = (SQLCipherCipher*)cipher;
  return sqlCipherCipher->m_legacy;
}

static int
GetPageSizeSQLCipherCipher(void* cipher)
{
  SQLCipherCipher* sqlCipherCipher = (SQLCipherCipher*) cipher;
  int pageSize = 0;
  if (sqlCipherCipher->m_legacy != 0)
  {
    pageSize = sqlCipherCipher->m_legacyPageSize;
    if ((pageSize < 512) || (pageSize > SQLITE_MAX_PAGE_SIZE) || (((pageSize - 1) & pageSize) != 0))
    {
      pageSize = 0;
    }
  }
  return pageSize;
}

static int
GetReservedSQLCipherCipher(void* cipher)
{
  SQLCipherCipher* sqlCipherCipher = (SQLCipherCipher*) cipher;
  int reserved = SALTLENGTH_SQLCIPHER;
  if (sqlCipherCipher->m_hmacUse != 0)
  {
    switch (sqlCipherCipher->m_hmacAlgorithm)
    {
      case SQLCIPHER_HMAC_ALGORITHM_SHA1:
      case SQLCIPHER_HMAC_ALGORITHM_SHA256:
        reserved += SHA256_DIGEST_SIZE;
        break;
      case SQLCIPHER_HMAC_ALGORITHM_SHA512:
      default:
        reserved += SHA512_DIGEST_SIZE;
        break;
    }
  }
  return reserved;
}

static unsigned char*
GetSaltSQLCipherCipher(void* cipher)
{
  SQLCipherCipher* sqlCipherCipher = (SQLCipherCipher*) cipher;
  return sqlCipherCipher->m_salt;
}

static void
GenerateKeySQLCipherCipher(void* cipher, BtShared* pBt, char* userPassword, int passwordLength, int rekey, unsigned char* cipherSalt)
{
  SQLCipherCipher* sqlCipherCipher = (SQLCipherCipher*) cipher;

  Pager *pPager = pBt->pPager;
  sqlite3_file* fd = (isOpen(pPager->fd)) ? pPager->fd : NULL;

  if (rekey || fd == NULL || sqlite3OsRead(fd, sqlCipherCipher->m_salt, SALTLENGTH_SQLCIPHER, 0) != SQLITE_OK)
  {
    chacha20_rng(sqlCipherCipher->m_salt, SALTLENGTH_SQLCIPHER);
  }
  else if (cipherSalt != NULL)
  {
    memcpy(sqlCipherCipher->m_salt, cipherSalt, SALTLENGTH_SQLCIPHER);
  }

  if (passwordLength == ((KEYLENGTH_SQLCIPHER * 2) + 3) &&
      sqlite3_strnicmp(userPassword, "x'", 2) == 0 &&
    sqlite3mcIsHexKey((unsigned char*) (userPassword + 2), KEYLENGTH_SQLCIPHER * 2) != 0)
  {
    sqlite3mcConvertHex2Bin((unsigned char*) (userPassword + 2), passwordLength - 3, sqlCipherCipher->m_key);
  }
  else if (passwordLength == (((KEYLENGTH_SQLCIPHER + SALTLENGTH_SQLCIPHER) * 2) + 3) &&
           sqlite3_strnicmp(userPassword, "x'", 2) == 0 &&
           sqlite3mcIsHexKey((unsigned char*) (userPassword + 2), (KEYLENGTH_SQLCIPHER + SALTLENGTH_SQLCIPHER) * 2) != 0)
  {
    sqlite3mcConvertHex2Bin((unsigned char*) (userPassword + 2), KEYLENGTH_SQLCIPHER * 2, sqlCipherCipher->m_key);
    sqlite3mcConvertHex2Bin((unsigned char*) (userPassword + 2 + KEYLENGTH_SQLCIPHER * 2), SALTLENGTH_SQLCIPHER * 2, sqlCipherCipher->m_salt);
  }
  else
  {
    switch (sqlCipherCipher->m_kdfAlgorithm)
    {
      case SQLCIPHER_KDF_ALGORITHM_SHA1:
        fastpbkdf2_hmac_sha1((unsigned char*) userPassword, passwordLength,
                             sqlCipherCipher->m_salt, SALTLENGTH_SQLCIPHER,
                             sqlCipherCipher->m_kdfIter,
                             sqlCipherCipher->m_key, KEYLENGTH_SQLCIPHER);
        break;
      case SQLCIPHER_KDF_ALGORITHM_SHA256:
        fastpbkdf2_hmac_sha256((unsigned char*) userPassword, passwordLength,
                               sqlCipherCipher->m_salt, SALTLENGTH_SQLCIPHER,
                               sqlCipherCipher->m_kdfIter,
                               sqlCipherCipher->m_key, KEYLENGTH_SQLCIPHER);
        break;
      case SQLCIPHER_KDF_ALGORITHM_SHA512:
      default:
        fastpbkdf2_hmac_sha512((unsigned char*) userPassword, passwordLength,
                               sqlCipherCipher->m_salt, SALTLENGTH_SQLCIPHER,
                               sqlCipherCipher->m_kdfIter,
                               sqlCipherCipher->m_key, KEYLENGTH_SQLCIPHER);
        break;
    }
  }

  if (sqlCipherCipher->m_hmacUse != 0)
  {
    int j;
    unsigned char hmacSaltMask = sqlCipherCipher->m_hmacSaltMask;
    unsigned char hmacSalt[SALTLENGTH_SQLCIPHER];
    memcpy(hmacSalt, sqlCipherCipher->m_salt, SALTLENGTH_SQLCIPHER);
    for (j = 0; j < SALTLENGTH_SQLCIPHER; ++j)
    {
      hmacSalt[j] ^= hmacSaltMask;
    }
    switch (sqlCipherCipher->m_hmacAlgorithm)
    {
      case SQLCIPHER_HMAC_ALGORITHM_SHA1:
        fastpbkdf2_hmac_sha1(sqlCipherCipher->m_key, KEYLENGTH_SQLCIPHER,
                             hmacSalt, SALTLENGTH_SQLCIPHER,
                             sqlCipherCipher->m_fastKdfIter,
                             sqlCipherCipher->m_hmacKey, KEYLENGTH_SQLCIPHER);
      break;
      case SQLCIPHER_HMAC_ALGORITHM_SHA256:
        fastpbkdf2_hmac_sha256(sqlCipherCipher->m_key, KEYLENGTH_SQLCIPHER,
                               hmacSalt, SALTLENGTH_SQLCIPHER,
                               sqlCipherCipher->m_fastKdfIter,
                               sqlCipherCipher->m_hmacKey, KEYLENGTH_SQLCIPHER);
        break;
      case SQLCIPHER_HMAC_ALGORITHM_SHA512:
      default:
        fastpbkdf2_hmac_sha512(sqlCipherCipher->m_key, KEYLENGTH_SQLCIPHER,
                               hmacSalt, SALTLENGTH_SQLCIPHER,
                               sqlCipherCipher->m_fastKdfIter,
                               sqlCipherCipher->m_hmacKey, KEYLENGTH_SQLCIPHER);
        break;
    }
  }
}

static int
GetHmacSizeSQLCipherCipher(int algorithm)
{
  int hmacSize = SHA512_DIGEST_SIZE;
  switch (algorithm)
  {
    case SQLCIPHER_HMAC_ALGORITHM_SHA1:
      hmacSize = SHA1_DIGEST_SIZE;
      break;
    case SQLCIPHER_HMAC_ALGORITHM_SHA256:
    case SQLCIPHER_HMAC_ALGORITHM_SHA512:
    default:
      hmacSize = SHA512_DIGEST_SIZE;
      break;
  }
  return hmacSize;
}

static int
EncryptPageSQLCipherCipher(void* cipher, int page, unsigned char* data, int len, int reserved)
{
  SQLCipherCipher* sqlCipherCipher = (SQLCipherCipher*) cipher;
  int rc = SQLITE_OK;
  int legacy = sqlCipherCipher->m_legacy;
  int nReserved = (reserved == 0 && legacy == 0) ? 0 : GetReservedSQLCipherCipher(cipher);
  int n = len - nReserved;
  int offset = (page == 1) ? (sqlCipherCipher->m_legacy != 0) ? 16 : 24 : 0;
  int blen;
  unsigned char iv[64];
  int usePlaintextHeader = 0;

  /* Check whether a plaintext header should be used */
  if (page == 1 && sqlCipherCipher->m_legacy >= SQLCIPHER_VERSION_4 && sqlCipherCipher->m_plaintextHeaderSize > 0)
  {
    usePlaintextHeader = 1;
    offset = sqlCipherCipher->m_plaintextHeaderSize;
  }

  /* Check whether number of required reserved bytes and actually reserved bytes match */
  if ((legacy == 0 && nReserved > reserved) || ((legacy != 0 && nReserved != reserved)))
  {
    return SQLITE_CORRUPT;
  }

  /* Generate nonce (64 bytes) */
  memset(iv, 0, 64);
  if (nReserved > 0)
  {
    chacha20_rng(iv, 64);
  }
  else
  {
    sqlite3mcGenerateInitialVector(page, iv);
  }

  RijndaelInit(sqlCipherCipher->m_aes, RIJNDAEL_Direction_Mode_CBC, RIJNDAEL_Direction_Encrypt, sqlCipherCipher->m_key, RIJNDAEL_Direction_KeyLength_Key32Bytes, iv);
  blen = RijndaelBlockEncrypt(sqlCipherCipher->m_aes, data + offset, (n - offset) * 8, data + offset);
  if (nReserved > 0)
  {
    memcpy(data + n, iv, nReserved);
  }
  if (page == 1 && usePlaintextHeader == 0)
  {
    memcpy(data, sqlCipherCipher->m_salt, SALTLENGTH_SQLCIPHER);
  }

  /* hmac calculation */
  if (sqlCipherCipher->m_hmacUse == 1 && nReserved > 0)
  {
    unsigned char pgno_raw[4];
    unsigned char hmac_out[64];
    int hmac_size = GetHmacSizeSQLCipherCipher(sqlCipherCipher->m_hmacAlgorithm);

    if (sqlCipherCipher->m_hmacPgno == SQLCIPHER_HMAC_PGNO_LE)
    {
      STORE32_LE(pgno_raw, page);
    }
    else if (sqlCipherCipher->m_hmacPgno == SQLCIPHER_HMAC_PGNO_BE)
    {
      STORE32_BE(pgno_raw, page);
    }
    else
    {
      memcpy(pgno_raw, &page, 4);
    }
    sqlcipher_hmac(sqlCipherCipher->m_hmacAlgorithm, sqlCipherCipher->m_hmacKey, KEYLENGTH_SQLCIPHER, data + offset, n + PAGE_NONCE_LEN_SQLCIPHER - offset, pgno_raw, 4, hmac_out);
    memcpy(data + n + PAGE_NONCE_LEN_SQLCIPHER, hmac_out, hmac_size);
  }

  return rc;
}

static int
DecryptPageSQLCipherCipher(void* cipher, int page, unsigned char* data, int len, int reserved, int hmacCheck)
{
  SQLCipherCipher* sqlCipherCipher = (SQLCipherCipher*) cipher;
  int rc = SQLITE_OK;
  int legacy = sqlCipherCipher->m_legacy;
  int nReserved = (reserved == 0 && legacy == 0) ? 0 : GetReservedSQLCipherCipher(cipher);
  int n = len - nReserved;
  int offset = (page == 1) ? (sqlCipherCipher->m_legacy != 0) ? 16 : 24 : 0;
  int hmacOk = 1;
  int blen;
  unsigned char iv[128];
  int usePlaintextHeader = 0;

  /* Check whether a plaintext header should be used */
  if (page == 1 && sqlCipherCipher->m_legacy >= SQLCIPHER_VERSION_4 && sqlCipherCipher->m_plaintextHeaderSize > 0)
  {
    usePlaintextHeader = 1;
    offset = sqlCipherCipher->m_plaintextHeaderSize;
  }

  /* Check whether number of required reserved bytes and actually reserved bytes match */
  if ((legacy == 0 && nReserved > reserved) || ((legacy != 0 && nReserved != reserved)))
  {
    return (page == 1) ? SQLITE_NOTADB : SQLITE_CORRUPT;
  }

  /* Get nonce from buffer */
  if (nReserved > 0)
  {
    memcpy(iv, data + n, nReserved);
  }
  else
  {
    sqlite3mcGenerateInitialVector(page, iv);
  }

  /* hmac check */
  if (sqlCipherCipher->m_hmacUse == 1 && nReserved > 0 && hmacCheck != 0)
  {
    unsigned char pgno_raw[4];
    unsigned char hmac_out[64];
    int hmac_size = GetHmacSizeSQLCipherCipher(sqlCipherCipher->m_hmacAlgorithm);
    if (sqlCipherCipher->m_hmacPgno == SQLCIPHER_HMAC_PGNO_LE)
    {
      STORE32_LE(pgno_raw, page);
    }
    else if (sqlCipherCipher->m_hmacPgno == SQLCIPHER_HMAC_PGNO_BE)
    {
      STORE32_BE(pgno_raw, page);
    }
    else
    {
      memcpy(pgno_raw, &page, 4);
    }
    sqlcipher_hmac(sqlCipherCipher->m_hmacAlgorithm, sqlCipherCipher->m_hmacKey, KEYLENGTH_SQLCIPHER, data + offset, n + PAGE_NONCE_LEN_SQLCIPHER - offset, pgno_raw, 4, hmac_out);
    hmacOk = (memcmp(data + n + PAGE_NONCE_LEN_SQLCIPHER, hmac_out, hmac_size) == 0);
  }

  if (hmacOk != 0)
  {
    RijndaelInit(sqlCipherCipher->m_aes, RIJNDAEL_Direction_Mode_CBC, RIJNDAEL_Direction_Decrypt, sqlCipherCipher->m_key, RIJNDAEL_Direction_KeyLength_Key32Bytes, iv);
    blen = RijndaelBlockDecrypt(sqlCipherCipher->m_aes, data + offset, (n - offset) * 8, data + offset);
    if (nReserved > 0)
    {
      memcpy(data + n, iv, nReserved);
    }
    if (page == 1 && usePlaintextHeader == 0)
    {
      memcpy(data, SQLITE_FILE_HEADER, 16);
    }
  }
  else
  {
    /* Bad MAC */
    rc = (page == 1) ? SQLITE_NOTADB : SQLITE_CORRUPT;
  }

  return rc;
}
SQLITE_PRIVATE const CipherDescriptor mcSQLCipherDescriptor =
{
  CIPHER_NAME_SQLCIPHER,
  AllocateSQLCipherCipher,
  FreeSQLCipherCipher,
  CloneSQLCipherCipher,
  GetLegacySQLCipherCipher,
  GetPageSizeSQLCipherCipher,
  GetReservedSQLCipherCipher,
  GetSaltSQLCipherCipher,
  GenerateKeySQLCipherCipher,
  EncryptPageSQLCipherCipher,
  DecryptPageSQLCipherCipher
};
#endif

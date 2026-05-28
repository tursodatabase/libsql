/*
** Name:        cipher_aegis.c
** Purpose:     Implementation of cipher AEGIS
** Author:      Ulrich Telle
** Created:     2024-12-10
** Copyright:   (c) 2024-2024 Ulrich Telle
** License:     MIT
*/

#include "cipher_common.h"

/* --- Aegis --- */
#if HAVE_CIPHER_AEGIS

#define CIPHER_NAME_AEGIS "aegis"

/*
** Configuration parameters for "aegis"
**
** - tcost     : number of iterations for key derivation with Argon2
** - mcost     : amount of memory in kB for key derivation with Argon2
** - pcost     : parallelism, number of threads for key derivation with Argon2
** - algorithm : AEGIS variant to be used for page encryption
*/

#define AEGIS_ALGORITHM_128L  1
#define AEGIS_ALGORITHM_128X2 2
#define AEGIS_ALGORITHM_128X4 3
#define AEGIS_ALGORITHM_256   4
#define AEGIS_ALGORITHM_256X2 5
#define AEGIS_ALGORITHM_256X4 6

#define AEGIS_ALGORITHM_MIN      AEGIS_ALGORITHM_128L
#define AEGIS_ALGORITHM_MAX      AEGIS_ALGORITHM_256X4
#define AEGIS_ALGORITHM_DEFAULT  AEGIS_ALGORITHM_256

#define AEGIS_TCOST_DEFAULT  2
#define AEGIS_MCOST_DEFAULT  (19*1024)
#define AEGIS_PCOST_DEFAULT  1

SQLITE_PRIVATE const char* mcAegisAlgorithmNames[AEGIS_ALGORITHM_MAX+1] =
{ "", "aegis-128l", "aegis-128x2", "aegis-128x4", "aegis-256", "aegis-256x2", "aegis-256x4" };

SQLITE_PRIVATE int sqlite3mcAegisAlgorithmToIndex(const char* algorithmName)
{
  int j = 0;
  for (j = AEGIS_ALGORITHM_MIN; j <= AEGIS_ALGORITHM_MAX; j++)
  {
    if (sqlite3_stricmp(algorithmName, mcAegisAlgorithmNames[j]) == 0)
      break;
  }
  if (j <= AEGIS_ALGORITHM_MAX)
    return j;
  else
    return -1;
}

SQLITE_PRIVATE const char* sqlite3mcAegisAlgorithmToString(int algorithmIndex)
{
  if (algorithmIndex >= AEGIS_ALGORITHM_MIN && algorithmIndex <= AEGIS_ALGORITHM_MAX)
    return mcAegisAlgorithmNames[algorithmIndex];
  else
    return "unknown";
}

typedef int (*AegisEncryptDetached_t)(uint8_t* c, uint8_t* mac, size_t maclen, 
                                      const uint8_t* m, size_t mlen,
                                      const uint8_t* ad, size_t adlen,
                                      const uint8_t* npub, const uint8_t* k);

typedef int (*AegisDecryptDetached_t)(uint8_t* m, const uint8_t* c, size_t clen,
                                      const uint8_t* mac, size_t maclen,
                                      const uint8_t* ad, size_t adlen,
                                      const uint8_t* npub, const uint8_t* k);

typedef void (*AegisEncryptUnauthenticated_t)(uint8_t* c, const uint8_t* m, size_t mlen,
                                              const uint8_t* npub, const uint8_t* k);

typedef void (*AegisDecryptUnauthenticated_t)(uint8_t* m, const uint8_t* c, size_t clen,
                                              const uint8_t* npub, const uint8_t* k);

typedef void (*AegisStream_t)(uint8_t* out, size_t len, const uint8_t* npub, const uint8_t* k);

typedef struct _AegisCryptFunctions
{
  AegisEncryptDetached_t encrypt;
  AegisDecryptDetached_t decrypt;
  AegisEncryptUnauthenticated_t encryptNoTag;
  AegisEncryptUnauthenticated_t decryptNoTag;
  AegisStream_t stream;
} AegisCryptFunctions;

SQLITE_PRIVATE const AegisCryptFunctions mcAegisCryptFunctions[] =
{
  { NULL,                        NULL }, /* Dummy entry */
  { aegis128l_encrypt_detached,         aegis128l_decrypt_detached,
    aegis128l_encrypt_unauthenticated,  aegis128l_decrypt_unauthenticated,
    aegis128l_stream },
  { aegis128x2_encrypt_detached,        aegis128x2_decrypt_detached,
    aegis128x2_encrypt_unauthenticated, aegis128x2_decrypt_unauthenticated,
    aegis128x2_stream },
  { aegis128x4_encrypt_detached,        aegis128x4_decrypt_detached,
    aegis128x4_encrypt_unauthenticated, aegis128x4_decrypt_unauthenticated,
    aegis128x4_stream },
  { aegis256_encrypt_detached,          aegis256_decrypt_detached,
    aegis256_encrypt_unauthenticated,   aegis256_decrypt_unauthenticated,
    aegis256_stream },
  { aegis256x2_encrypt_detached,        aegis256x2_decrypt_detached,
    aegis256x2_encrypt_unauthenticated, aegis256x2_decrypt_unauthenticated,
    aegis256x2_stream },
  { aegis256x4_encrypt_detached,        aegis256x4_decrypt_detached,
    aegis256x4_encrypt_unauthenticated, aegis256x4_decrypt_unauthenticated,
    aegis256x4_stream }
};

SQLITE_PRIVATE CipherParams mcAegisParams[] =
{
  { "tcost",     AEGIS_TCOST_DEFAULT,     AEGIS_TCOST_DEFAULT,     1,                   0x7fffffff },
  { "mcost",     AEGIS_MCOST_DEFAULT,     AEGIS_MCOST_DEFAULT,     1,                   0x7fffffff },
  { "pcost",     AEGIS_PCOST_DEFAULT,     AEGIS_PCOST_DEFAULT,     1,                   0x7fffffff },
  { "algorithm", AEGIS_ALGORITHM_DEFAULT, AEGIS_ALGORITHM_DEFAULT, AEGIS_ALGORITHM_MIN, AEGIS_ALGORITHM_MAX },
  CIPHER_PARAMS_SENTINEL
};

#define KEYLENGTH_AEGIS_128       16
#define KEYLENGTH_AEGIS_256       32
#define KEYLENGTH_AEGIS_MAX       32

#define PAGE_NONCE_LEN_AEGIS_128  16
#define PAGE_NONCE_LEN_AEGIS_256  32
#define PAGE_NONCE_LEN_AEGIS_MAX  32

#if AEGIS_ALGORITHM_DEFAULT < AEGIS_ALGORITHM_256
#define KEYLENGTH_AEGIS_DEFAULT      KEYLENGTH_AEGIS_128
#define PAGE_NONCE_LEN_AEGIS_DEFAULT PAGE_NONCE_LEN_AEGIS_128
#else
#define KEYLENGTH_AEGIS_DEFAULT      KEYLENGTH_AEGIS_256
#define PAGE_NONCE_LEN_AEGIS_DEFAULT PAGE_NONCE_LEN_AEGIS_256
#endif

#define SALTLENGTH_AEGIS          16

#define PAGE_TAG_LEN_AEGIS        32
#define PAGE_RESERVED_AEGIS   (PAGE_NONCE_LEN_AEGIS + PAGE_TAG_LEN_AEGIS)

#define OTK_LEN_MAX_AEGIS (PAGE_TAG_LEN_AEGIS + PAGE_NONCE_LEN_AEGIS_MAX + 4)

typedef struct _aegisCipher
{
  int     m_argon2Tcost;
  int     m_argon2Mcost;
  int     m_argon2Pcost;
  int     m_aegisAlgorithm;
  int     m_keyLength;
  int     m_nonceLength;
  uint8_t m_key[KEYLENGTH_AEGIS_MAX];
  uint8_t m_salt[SALTLENGTH_AEGIS];
} AegisCipher;

static void*
AllocateAegisCipher(sqlite3* db)
{
  AegisCipher* aegisCipher = (AegisCipher*) sqlite3_malloc(sizeof(AegisCipher));
  if (aegisCipher != NULL)
  {
    memset(aegisCipher, 0, sizeof(AegisCipher));
    aegisCipher->m_keyLength = 0;
    memset(aegisCipher->m_key, 0, KEYLENGTH_AEGIS_MAX);
    memset(aegisCipher->m_salt, 0, SALTLENGTH_AEGIS);
  }
  if (aegisCipher != NULL)
  {
    CipherParams* cipherParams = sqlite3mcGetCipherParams(db, CIPHER_NAME_AEGIS);
    aegisCipher->m_argon2Tcost = sqlite3mcGetCipherParameter(cipherParams, "tcost");
    aegisCipher->m_argon2Mcost = sqlite3mcGetCipherParameter(cipherParams, "mcost");
    aegisCipher->m_argon2Pcost = sqlite3mcGetCipherParameter(cipherParams, "pcost");
    aegisCipher->m_aegisAlgorithm = sqlite3mcGetCipherParameter(cipherParams, "algorithm");
    if (aegisCipher->m_aegisAlgorithm < AEGIS_ALGORITHM_256)
    {
      aegisCipher->m_keyLength = KEYLENGTH_AEGIS_128;
      aegisCipher->m_nonceLength = PAGE_NONCE_LEN_AEGIS_128;
    }
    else
    {
      aegisCipher->m_keyLength = KEYLENGTH_AEGIS_256;
      aegisCipher->m_nonceLength = PAGE_NONCE_LEN_AEGIS_256;
    }
  }
  return aegisCipher;
}

static void
FreeAegisCipher(void* cipher)
{
  AegisCipher* aegisCipher = (AegisCipher*) cipher;
  memset(aegisCipher, 0, sizeof(AegisCipher));
  sqlite3_free(aegisCipher);
}

static void
CloneAegisCipher(void* cipherTo, void* cipherFrom)
{
  AegisCipher* aegisCipherTo = (AegisCipher*) cipherTo;
  AegisCipher* aegisCipherFrom = (AegisCipher*) cipherFrom;

  aegisCipherTo->m_argon2Tcost = aegisCipherFrom->m_argon2Tcost;
  aegisCipherTo->m_argon2Mcost = aegisCipherFrom->m_argon2Mcost;
  aegisCipherTo->m_argon2Pcost = aegisCipherFrom->m_argon2Pcost;

  aegisCipherTo->m_aegisAlgorithm = aegisCipherFrom->m_aegisAlgorithm;
  aegisCipherTo->m_keyLength = aegisCipherFrom->m_keyLength;
  aegisCipherTo->m_nonceLength = aegisCipherFrom->m_nonceLength;

  memcpy(aegisCipherTo->m_key, aegisCipherFrom->m_key, aegisCipherFrom->m_keyLength);
  memcpy(aegisCipherTo->m_salt, aegisCipherFrom->m_salt, SALTLENGTH_AEGIS);
}

static int
GetLegacyAegisCipher(void* cipher)
{
  AegisCipher* aegisCipher = (AegisCipher*) cipher;
  return 0;
}

static int
GetPageSizeAegisCipher(void* cipher)
{
  AegisCipher* aegisCipher = (AegisCipher*) cipher;
  int pageSize = 0;
  return pageSize;
}

static int
GetReservedAegisCipher(void* cipher)
{
  AegisCipher* aegisCipher = (AegisCipher*) cipher;
  return (aegisCipher->m_nonceLength + PAGE_TAG_LEN_AEGIS);
}

static unsigned char*
GetSaltAegisCipher(void* cipher)
{
  AegisCipher* aegisCipher = (AegisCipher*) cipher;
  return aegisCipher->m_salt;
}

static void
GenerateKeyAegisCipher(void* cipher, char* userPassword, int passwordLength, int rekey, unsigned char* cipherSalt)
{
  AegisCipher* aegisCipher = (AegisCipher*) cipher;
  int bypass = 0;

  int keyOnly = 1;
  if (rekey || cipherSalt == NULL)
  {
    chacha20_rng(aegisCipher->m_salt, SALTLENGTH_AEGIS);
    keyOnly = 0;
  }
  else
  {
    memcpy(aegisCipher->m_salt, cipherSalt, SALTLENGTH_AEGIS);
  }

  /* Bypass key derivation if the key string starts with "raw:" */
  if (passwordLength > 4 && !memcmp(userPassword, "raw:", 4))
  {
    const int nRaw = passwordLength - 4;
    const unsigned char* zRaw = (const unsigned char*) userPassword + 4;
    if (nRaw == aegisCipher->m_keyLength)
    {
      /* Binary key */
      memcpy(aegisCipher->m_key, zRaw, aegisCipher->m_keyLength);
      bypass = 1;
    }
    else if (nRaw == aegisCipher->m_keyLength + SALTLENGTH_AEGIS)
    {
      /* Binary key and salt) */
      if (!keyOnly)
      {
        memcpy(aegisCipher->m_salt, zRaw + aegisCipher->m_keyLength, SALTLENGTH_AEGIS);
      }
      memcpy(aegisCipher->m_key, zRaw, aegisCipher->m_keyLength);
      bypass = 1;
    }
    else if (nRaw == 2 * aegisCipher->m_keyLength)
    {
      /* Hex-encoded key */
      if (sqlite3mcIsHexKey(zRaw, nRaw) != 0)
      {
        sqlite3mcConvertHex2Bin(zRaw, nRaw, aegisCipher->m_key);
        bypass = 1;
      }
    }
    else if (nRaw == 2 * (aegisCipher->m_keyLength + SALTLENGTH_AEGIS))
    {
      /* Hex-encoded key and salt */
      if (sqlite3mcIsHexKey(zRaw, nRaw) != 0)
      {
        sqlite3mcConvertHex2Bin(zRaw, 2 * aegisCipher->m_keyLength, aegisCipher->m_key);
        if (!keyOnly)
        {
          sqlite3mcConvertHex2Bin(zRaw + 2 * aegisCipher->m_keyLength, 2 * SALTLENGTH_AEGIS, aegisCipher->m_salt);
        }
        bypass = 1;
      }
    }
  }

  if (!bypass)
  {
    int rc = argon2id_hash_raw((uint32_t) aegisCipher->m_argon2Tcost,
                               (uint32_t) aegisCipher->m_argon2Mcost,
                               (uint32_t) aegisCipher->m_argon2Pcost,
                               userPassword, passwordLength,
                               aegisCipher->m_salt, SALTLENGTH_AEGIS,
                               aegisCipher->m_key, aegisCipher->m_keyLength);
  }
  SQLITE3MC_DEBUG_LOG("generate: codec=%p pFile=%p\n", aegisCipher, fd);
  SQLITE3MC_DEBUG_HEX("generate  key:", aegisCipher->m_key, aegisCipher->m_keyLength);
  SQLITE3MC_DEBUG_HEX("generate salt:", aegisCipher->m_salt, SALTLENGTH_AEGIS);
}

static int
AegisGenNonce(AegisCipher* aegisCipher, uint8_t* out, int outLength, int page)
{
  uint8_t nonce[PAGE_NONCE_LEN_AEGIS_MAX];
  memset(nonce, 0, PAGE_NONCE_LEN_AEGIS_MAX);
  STORE32_LE(out, page);
  STORE32_LE(out + 4, page);
  mcAegisCryptFunctions[aegisCipher->m_aegisAlgorithm].stream(out, outLength, nonce, aegisCipher->m_key);
  return 0;
}

static int
AegisGenOtk(AegisCipher* aegisCipher, uint8_t* out, int outLength, uint8_t* nonce, int nonceLength, int page)
{
  mcAegisCryptFunctions[aegisCipher->m_aegisAlgorithm].stream(out, outLength, nonce, aegisCipher->m_key);
  STORE32_BE(out + (outLength - 4), page);
  return 0;
}

static int
EncryptPageAegisCipher(void* cipher, int page, unsigned char* data, int len, int reserved)
{
  AegisCipher* aegisCipher = (AegisCipher*) cipher;
  int rc = SQLITE_OK;
  int nReserved = (reserved == 0) ? 0 : GetReservedAegisCipher(cipher);
  int n = len - nReserved;
  uint64_t mlen = n;

  /* Generate one-time keys */
  uint8_t otk[OTK_LEN_MAX_AEGIS];
  int offset;
  memset(otk, 0, OTK_LEN_MAX_AEGIS);

  /* Check whether number of required reserved bytes and actually reserved bytes match */
  if (nReserved > reserved)
  {
    return SQLITE_CORRUPT;
  }

  if (nReserved > 0)
  {
    /* Encrypt and authenticate */

    /* Generate nonce */
    chacha20_rng(data + n + PAGE_TAG_LEN_AEGIS, aegisCipher->m_nonceLength);
    AegisGenOtk(aegisCipher, otk, aegisCipher->m_keyLength + aegisCipher->m_nonceLength,
                data + n + PAGE_TAG_LEN_AEGIS, aegisCipher->m_nonceLength, page);

    offset = (page == 1) ? CIPHER_PAGE1_OFFSET : 0;
    mcAegisCryptFunctions[aegisCipher->m_aegisAlgorithm].encrypt(
      data + offset, data + n, PAGE_TAG_LEN_AEGIS,
      data + offset, mlen - offset, 
      NULL, 0, otk + aegisCipher->m_keyLength, otk);
    
    if (page == 1)
    {
      memcpy(data, aegisCipher->m_salt, SALTLENGTH_AEGIS);
    }
  }
  else
  {
    /* Encrypt only */
    uint8_t nonce[PAGE_NONCE_LEN_AEGIS_MAX];
    AegisGenNonce(aegisCipher, nonce, aegisCipher->m_nonceLength, page);
    AegisGenOtk(aegisCipher, otk, aegisCipher->m_keyLength + aegisCipher->m_nonceLength,
                nonce, aegisCipher->m_nonceLength, page);

    /* Encrypt */
    offset = (page == 1) ? CIPHER_PAGE1_OFFSET : 0;
    mcAegisCryptFunctions[aegisCipher->m_aegisAlgorithm].encryptNoTag(
      data + offset, 
      data + offset, mlen - offset,
      otk + aegisCipher->m_keyLength, otk);

    if (page == 1)
    {
      memcpy(data, aegisCipher->m_salt, SALTLENGTH_AEGIS);
    }
  }

  return rc;
}

static int
DecryptPageAegisCipher(void* cipher, int page, unsigned char* data, int len, int reserved, int hmacCheck)
{
  AegisCipher* aegisCipher = (AegisCipher*) cipher;
  int rc = SQLITE_OK;
  int nReserved = (reserved == 0) ? 0 : GetReservedAegisCipher(cipher);
  int n = len - nReserved;
  uint64_t clen = n;
  int tagOk;

  /* Generate one-time keys */
  uint8_t otk[OTK_LEN_MAX_AEGIS];
  int offset;
  memset(otk, 0, OTK_LEN_MAX_AEGIS);

  /* Check whether number of required reserved bytes and actually reserved bytes match */
  if (nReserved > reserved)
  {
    return (page == 1) ? SQLITE_NOTADB : SQLITE_CORRUPT;
  }

  if (nReserved > 0)
  {
    /* Decrypt and verify MAC */
    AegisGenOtk(aegisCipher, otk, aegisCipher->m_keyLength + aegisCipher->m_nonceLength,
                data + n + PAGE_TAG_LEN_AEGIS, aegisCipher->m_nonceLength, page);

    /* Determine MAC and decrypt */
    offset = (page == 1) ? CIPHER_PAGE1_OFFSET : 0;

    if (hmacCheck != 0)
    {
      /* Verify the MAC */
      tagOk = mcAegisCryptFunctions[aegisCipher->m_aegisAlgorithm].decrypt(
                data + offset,
                data + offset, clen - offset,
                data + n, PAGE_TAG_LEN_AEGIS,
                NULL, 0, otk + aegisCipher->m_keyLength, otk);
      if (tagOk != 0)
      {
        SQLITE3MC_DEBUG_LOG("decrypt: codec=%p page=%d\n", aegisCipher, page);
        SQLITE3MC_DEBUG_HEX("decrypt key:", aegisCipher->m_key, aegisCipher->m_keyLength);
        SQLITE3MC_DEBUG_HEX("decrypt otk:", otk, 64);
        SQLITE3MC_DEBUG_HEX("decrypt data+00:", data, 16);
        SQLITE3MC_DEBUG_HEX("decrypt data+24:", data + 24, 16);
        SQLITE3MC_DEBUG_HEX("decrypt data+n:", data + n, PAGE_TAG_LEN_AEGIS);
        /* Bad MAC */
        rc = (page == 1) ? SQLITE_NOTADB : SQLITE_CORRUPT;
      }
    }
    else
    {
      mcAegisCryptFunctions[aegisCipher->m_aegisAlgorithm].decryptNoTag(
        data + offset,
        data + offset, clen - offset,
        otk + aegisCipher->m_keyLength, otk);
    }

    if (page == 1 && rc == SQLITE_OK)
    {
      memcpy(data, SQLITE_FILE_HEADER, 16);
    }
  }
  else
  {
    /* Decrypt only */
    uint8_t nonce[PAGE_NONCE_LEN_AEGIS_MAX];
    AegisGenNonce(aegisCipher, nonce, aegisCipher->m_nonceLength, page);
    AegisGenOtk(aegisCipher, otk, aegisCipher->m_keyLength + aegisCipher->m_nonceLength,
                nonce, aegisCipher->m_nonceLength, page);

    /* Decrypt */
    offset = (page == 1) ? CIPHER_PAGE1_OFFSET : 0;
    mcAegisCryptFunctions[aegisCipher->m_aegisAlgorithm].decryptNoTag(
      data + offset,
      data + offset, clen - offset,
      otk + aegisCipher->m_keyLength, otk);

    if (page == 1)
    {
      memcpy(data, SQLITE_FILE_HEADER, 16);
    }
  }

  return rc;
}

SQLITE_PRIVATE const CipherDescriptor mcAegisDescriptor =
{
  CIPHER_NAME_AEGIS,
  AllocateAegisCipher,
  FreeAegisCipher,
  CloneAegisCipher,
  GetLegacyAegisCipher,
  GetPageSizeAegisCipher,
  GetReservedAegisCipher,
  GetSaltAegisCipher,
  GenerateKeyAegisCipher,
  EncryptPageAegisCipher,
  DecryptPageAegisCipher
};
#endif

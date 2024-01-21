/*
** Name:        cipher_wxaes256.c
** Purpose:     Implementation of cipher wxSQLite3 AES 256-bit
** Author:      Ulrich Telle
** Created:     2020-02-02
** Copyright:   (c) 2006-2020 Ulrich Telle
** License:     MIT
*/

#include "cipher_common.h"

/* --- AES 256-bit cipher (wxSQLite3) --- */
#if HAVE_CIPHER_AES_256_CBC

#define CIPHER_NAME_AES256 "aes256cbc"

/*
** Configuration parameters for "aes256cbc"
**
** - legacy mode : compatibility with first version (page 1 encrypted)
**                 possible values:  1 = yes, 0 = no (default)
** - kdf_iter : number of iterations for key derivation
*/

#ifdef WXSQLITE3_USE_OLD_ENCRYPTION_SCHEME
#define AES256_LEGACY_DEFAULT 1
#else
#define AES256_LEGACY_DEFAULT 0
#endif

SQLITE_PRIVATE CipherParams mcAES256Params[] =
{
  { "legacy",            AES256_LEGACY_DEFAULT, AES256_LEGACY_DEFAULT, 0, 1 },
  { "legacy_page_size",  0,                     0,                     0, SQLITE_MAX_PAGE_SIZE },
  { "kdf_iter",          CODEC_SHA_ITER,        CODEC_SHA_ITER,        1, 0x7fffffff },
  CIPHER_PARAMS_SENTINEL
};


typedef struct _AES256Cipher
{
  int       m_legacy;
  int       m_legacyPageSize;
  int       m_kdfIter;
  int       m_keyLength;
  uint8_t   m_key[KEYLENGTH_AES256];
  Rijndael* m_aes;
} AES256Cipher;

static void*
AllocateAES256Cipher(sqlite3* db)
{
  AES256Cipher* aesCipher = (AES256Cipher*) sqlite3_malloc(sizeof(AES256Cipher));
  if (aesCipher != NULL)
  {
    aesCipher->m_aes = (Rijndael*) sqlite3_malloc(sizeof(Rijndael));
    if (aesCipher->m_aes != NULL)
    {
      aesCipher->m_keyLength = KEYLENGTH_AES256;
      memset(aesCipher->m_key, 0, KEYLENGTH_AES256);
      RijndaelCreate(aesCipher->m_aes);
    }
    else
    {
      sqlite3_free(aesCipher);
      aesCipher = NULL;
    }
  }
  if (aesCipher != NULL)
  {
    CipherParams* cipherParams = sqlite3mcGetCipherParams(db, CIPHER_NAME_AES256);
    aesCipher->m_legacy = sqlite3mcGetCipherParameter(cipherParams, "legacy");
    aesCipher->m_legacyPageSize = sqlite3mcGetCipherParameter(cipherParams, "legacy_page_size");
    aesCipher->m_kdfIter = sqlite3mcGetCipherParameter(cipherParams, "kdf_iter");
  }
  return aesCipher;
}

static void
FreeAES256Cipher(void* cipher)
{
  AES256Cipher* aesCipher = (AES256Cipher*) cipher;
  memset(aesCipher->m_aes, 0, sizeof(Rijndael));
  sqlite3_free(aesCipher->m_aes);
  memset(aesCipher, 0, sizeof(AES256Cipher));
  sqlite3_free(aesCipher);
}

static void
CloneAES256Cipher(void* cipherTo, void* cipherFrom)
{
  AES256Cipher* aesCipherTo = (AES256Cipher*) cipherTo;
  AES256Cipher* aesCipherFrom = (AES256Cipher*) cipherFrom;
  aesCipherTo->m_legacy = aesCipherFrom->m_legacy;
  aesCipherTo->m_legacyPageSize = aesCipherFrom->m_legacyPageSize;
  aesCipherTo->m_kdfIter = aesCipherFrom->m_kdfIter;
  aesCipherTo->m_keyLength = aesCipherFrom->m_keyLength;
  memcpy(aesCipherTo->m_key, aesCipherFrom->m_key, KEYLENGTH_AES256);
  RijndaelInvalidate(aesCipherTo->m_aes);
  RijndaelInvalidate(aesCipherFrom->m_aes);
}

static int
GetLegacyAES256Cipher(void* cipher)
{
  AES256Cipher* aesCipher = (AES256Cipher*)cipher;
  return aesCipher->m_legacy;
}

static int
GetPageSizeAES256Cipher(void* cipher)
{
  AES256Cipher* aesCipher = (AES256Cipher*) cipher;
  int pageSize = 0;
  if (aesCipher->m_legacy != 0)
  {
    pageSize = aesCipher->m_legacyPageSize;
    if ((pageSize < 512) || (pageSize > SQLITE_MAX_PAGE_SIZE) || (((pageSize - 1) & pageSize) != 0))
    {
      pageSize = 0;
    }
  }
  return pageSize;
}

static int
GetReservedAES256Cipher(void* cipher)
{
  return 0;
}

static unsigned char*
GetSaltAES256Cipher(void* cipher)
{
  return NULL;
}

static void
GenerateKeyAES256Cipher(void* cipher, BtShared* pBt, char* userPassword, int passwordLength, int rekey, unsigned char* cipherSalt)
{
  AES256Cipher* aesCipher = (AES256Cipher*) cipher;
  unsigned char userPad[32];
  unsigned char digest[KEYLENGTH_AES256];
  int keyLength = KEYLENGTH_AES256;
  int k;

  /* Pad password */
  sqlite3mcPadPassword(userPassword, passwordLength, userPad);

  sha256(userPad, 32, digest);
  for (k = 0; k < CODEC_SHA_ITER; ++k)
  {
    sha256(digest, KEYLENGTH_AES256, digest);
  }
  memcpy(aesCipher->m_key, digest, aesCipher->m_keyLength);
}

// Assumes the digest is at least KEYLENGTH_AES256 bytes long (32),
// generates the key in the digest.
void libsql_generate_aes256_key(char *userPassword, int passwordLength, char *digest) {
  unsigned char userPad[32];
  int keyLength = KEYLENGTH_AES256;
  int k;

  /* Pad password */
  sqlite3mcPadPassword(userPassword, passwordLength, userPad);

  sha256(userPad, 32, digest);
  for (k = 0; k < CODEC_SHA_ITER; ++k)
  {
    sha256(digest, KEYLENGTH_AES256, digest);
  }
}

static int
EncryptPageAES256Cipher(void* cipher, int page, unsigned char* data, int len, int reserved)
{
  AES256Cipher* aesCipher = (AES256Cipher*) cipher;
  int rc = SQLITE_OK;
  if (aesCipher->m_legacy != 0)
  {
    /* Use the legacy encryption scheme */
    unsigned char* key = aesCipher->m_key;
    rc = sqlite3mcAES256(aesCipher->m_aes, page, 1, key, data, len, data);
  }
  else
  {
    unsigned char dbHeader[8];
    int offset = 0;
    unsigned char* key = aesCipher->m_key;
    if (page == 1)
    {
      /* Save the header bytes remaining unencrypted */
      memcpy(dbHeader, data + 16, 8);
      offset = 16;
      sqlite3mcAES256(aesCipher->m_aes, page, 1, key, data, 16, data);
    }
    rc = sqlite3mcAES256(aesCipher->m_aes, page, 1, key, data + offset, len - offset, data + offset);
    if (page == 1)
    {
      /* Move the encrypted header bytes 16..23 to a safe position */
      memcpy(data + 8, data + 16, 8);
      /* Restore the unencrypted header bytes 16..23 */
      memcpy(data + 16, dbHeader, 8);
    }
  }
  return rc;
}

static int
DecryptPageAES256Cipher(void* cipher, int page, unsigned char* data, int len, int reserved, int hmacCheck)
{
  AES256Cipher* aesCipher = (AES256Cipher*) cipher;
  int rc = SQLITE_OK;
  if (aesCipher->m_legacy != 0)
  {
    /* Use the legacy encryption scheme */
    rc = sqlite3mcAES256(aesCipher->m_aes, page, 0, aesCipher->m_key, data, len, data);
  }
  else
  {
    unsigned char dbHeader[8];
    int dbPageSize;
    int offset = 0;
    if (page == 1)
    {
      /* Save (unencrypted) header bytes 16..23 */
      memcpy(dbHeader, data + 16, 8);
      /* Determine page size */
      dbPageSize = (dbHeader[0] << 8) | (dbHeader[1] << 16);
      /* Check whether the database header is valid */
      /* If yes, the database follows the new encryption scheme, otherwise use the previous encryption scheme */
      if ((dbPageSize >= 512) && (dbPageSize <= SQLITE_MAX_PAGE_SIZE) && (((dbPageSize - 1) & dbPageSize) == 0) &&
        (dbHeader[5] == 0x40) && (dbHeader[6] == 0x20) && (dbHeader[7] == 0x20))
      {
        /* Restore encrypted bytes 16..23 for new encryption scheme */
        memcpy(data + 16, data + 8, 8);
        offset = 16;
      }
    }
    rc = sqlite3mcAES256(aesCipher->m_aes, page, 0, aesCipher->m_key, data + offset, len - offset, data + offset);
    if (page == 1 && offset != 0)
    {
      /* Verify the database header */
      if (memcmp(dbHeader, data + 16, 8) == 0)
      {
        memcpy(data, SQLITE_FILE_HEADER, 16);
      }
    }
  }
  return rc;
}

SQLITE_PRIVATE const CipherDescriptor mcAES256Descriptor =
{
  CIPHER_NAME_AES256,
  AllocateAES256Cipher,
  FreeAES256Cipher,
  CloneAES256Cipher,
  GetLegacyAES256Cipher,
  GetPageSizeAES256Cipher,
  GetReservedAES256Cipher,
  GetSaltAES256Cipher,
  GenerateKeyAES256Cipher,
  EncryptPageAES256Cipher,
  DecryptPageAES256Cipher
};
#endif

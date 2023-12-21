/*
** Name:        cipher_wxaes128.c
** Purpose:     Implementation of cipher wxSQLite3 AES 128-bit
** Author:      Ulrich Telle
** Created:     2020-02-02
** Copyright:   (c) 2006-2020 Ulrich Telle
** License:     MIT
*/

#include "cipher_common.h"

/* --- AES 128-bit cipher (wxSQLite3) --- */
#if HAVE_CIPHER_AES_128_CBC

#define CIPHER_NAME_AES128 "aes128cbc"

/*
** Configuration parameters for "aes128cbc"
**
** - legacy mode : compatibility with first version (page 1 encrypted)
**                 possible values:  1 = yes, 0 = no (default)
*/

#ifdef WXSQLITE3_USE_OLD_ENCRYPTION_SCHEME
#define AES128_LEGACY_DEFAULT 1
#else
#define AES128_LEGACY_DEFAULT 0
#endif

SQLITE_PRIVATE CipherParams mcAES128Params[] =
{
  { "legacy",            AES128_LEGACY_DEFAULT, AES128_LEGACY_DEFAULT, 0, 1 },
  { "legacy_page_size",  0,                     0,                     0, SQLITE_MAX_PAGE_SIZE },
  CIPHER_PARAMS_SENTINEL
};

typedef struct _AES128Cipher
{
  int       m_legacy;
  int       m_legacyPageSize;
  int       m_keyLength;
  uint8_t   m_key[KEYLENGTH_AES128];
  Rijndael* m_aes;
} AES128Cipher;

static void*
AllocateAES128Cipher(sqlite3* db)
{
  AES128Cipher* aesCipher = (AES128Cipher*) sqlite3_malloc(sizeof(AES128Cipher));
  if (aesCipher != NULL)
  {
    aesCipher->m_aes = (Rijndael*) sqlite3_malloc(sizeof(Rijndael));
    if (aesCipher->m_aes != NULL)
    {
      aesCipher->m_keyLength = KEYLENGTH_AES128;
      memset(aesCipher->m_key, 0, KEYLENGTH_AES128);
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
    CipherParams* cipherParams = sqlite3mcGetCipherParams(db, CIPHER_NAME_AES128);
    aesCipher->m_legacy = sqlite3mcGetCipherParameter(cipherParams, "legacy");
    aesCipher->m_legacyPageSize = sqlite3mcGetCipherParameter(cipherParams, "legacy_page_size");
  }
  return aesCipher;
}

static void
FreeAES128Cipher(void* cipher)
{
  AES128Cipher* localCipher = (AES128Cipher*) cipher;
  memset(localCipher->m_aes, 0, sizeof(Rijndael));
  sqlite3_free(localCipher->m_aes);
  memset(localCipher, 0, sizeof(AES128Cipher));
  sqlite3_free(localCipher);
}

static void
CloneAES128Cipher(void* cipherTo, void* cipherFrom)
{
  AES128Cipher* aesCipherTo = (AES128Cipher*) cipherTo;
  AES128Cipher* aesCipherFrom = (AES128Cipher*) cipherFrom;
  aesCipherTo->m_legacy = aesCipherFrom->m_legacy;
  aesCipherTo->m_legacyPageSize = aesCipherFrom->m_legacyPageSize;
  aesCipherTo->m_keyLength = aesCipherFrom->m_keyLength;
  memcpy(aesCipherTo->m_key, aesCipherFrom->m_key, KEYLENGTH_AES128);
  RijndaelInvalidate(aesCipherTo->m_aes);
  RijndaelInvalidate(aesCipherFrom->m_aes);
}

static int
GetLegacyAES128Cipher(void* cipher)
{
  AES128Cipher* aesCipher = (AES128Cipher*)cipher;
  return aesCipher->m_legacy;
}

static int
GetPageSizeAES128Cipher(void* cipher)
{
  AES128Cipher* aesCipher = (AES128Cipher*) cipher;
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
GetReservedAES128Cipher(void* cipher)
{
  return 0;
}

static unsigned char*
GetSaltAES128Cipher(void* cipher)
{
  return NULL;
}

static void
GenerateKeyAES128Cipher(void* cipher, BtShared* pBt, char* userPassword, int passwordLength, int rekey, unsigned char* cipherSalt)
{
  AES128Cipher* aesCipher = (AES128Cipher*) cipher;
  unsigned char userPad[32];
  unsigned char ownerPad[32];
  unsigned char ownerKey[32];

  unsigned char mkey[MD5_HASHBYTES];
  unsigned char digest[MD5_HASHBYTES];
  int keyLength = MD5_HASHBYTES;
  int i, j, k;
  MD5_CTX ctx;

  /* Pad passwords */
  sqlite3mcPadPassword(userPassword, passwordLength, userPad);
  sqlite3mcPadPassword("", 0, ownerPad);

  /* Compute owner key */

  MD5_Init(&ctx);
  MD5_Update(&ctx, ownerPad, 32);
  MD5_Final(digest, &ctx);

  /* only use for the input as many bit as the key consists of */
  for (k = 0; k < 50; ++k)
  {
    MD5_Init(&ctx);
    MD5_Update(&ctx, digest, keyLength);
    MD5_Final(digest, &ctx);
  }
  memcpy(ownerKey, userPad, 32);
  for (i = 0; i < 20; ++i)
  {
    for (j = 0; j < keyLength; ++j)
    {
      mkey[j] = (digest[j] ^ i);
    }
    sqlite3mcRC4(mkey, keyLength, ownerKey, 32, ownerKey);
  }

  /* Compute encryption key */

  MD5_Init(&ctx);
  MD5_Update(&ctx, userPad, 32);
  MD5_Update(&ctx, ownerKey, 32);
  MD5_Final(digest, &ctx);

  /* only use the really needed bits as input for the hash */
  for (k = 0; k < 50; ++k)
  {
    MD5_Init(&ctx);
    MD5_Update(&ctx, digest, keyLength);
    MD5_Final(digest, &ctx);
  }
  memcpy(aesCipher->m_key, digest, aesCipher->m_keyLength);
}

static int
EncryptPageAES128Cipher(void* cipher, int page, unsigned char* data, int len, int reserved)
{
  AES128Cipher* aesCipher = (AES128Cipher*) cipher;
  int rc = SQLITE_OK;
  if (aesCipher->m_legacy != 0)
  {
    /* Use the legacy encryption scheme */
    unsigned char* key = aesCipher->m_key;
    rc = sqlite3mcAES128(aesCipher->m_aes, page, 1, key, data, len, data);
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
      sqlite3mcAES128(aesCipher->m_aes, page, 1, key, data, 16, data);
    }
    rc = sqlite3mcAES128(aesCipher->m_aes, page, 1, key, data + offset, len - offset, data + offset);
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
DecryptPageAES128Cipher(void* cipher, int page, unsigned char* data, int len, int reserved, int hmacCheck)
{
  AES128Cipher* aesCipher = (AES128Cipher*) cipher;
  int rc = SQLITE_OK;
  if (aesCipher->m_legacy != 0)
  {
    /* Use the legacy encryption scheme */
    rc = sqlite3mcAES128(aesCipher->m_aes, page, 0, aesCipher->m_key, data, len, data);
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
    rc = sqlite3mcAES128(aesCipher->m_aes, page, 0, aesCipher->m_key, data + offset, len - offset, data + offset);
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

SQLITE_PRIVATE const CipherDescriptor mcAES128Descriptor =
{
  CIPHER_NAME_AES128,
  AllocateAES128Cipher,
  FreeAES128Cipher,
  CloneAES128Cipher,
  GetLegacyAES128Cipher,
  GetPageSizeAES128Cipher,
  GetReservedAES128Cipher,
  GetSaltAES128Cipher,
  GenerateKeyAES128Cipher,
  EncryptPageAES128Cipher,
  DecryptPageAES128Cipher
};
#endif

/*
** Name:        cipher_sds_rc4.c
** Purpose:     Implementation of cipher System.Data.SQLite3 RC4
** Author:      Ulrich Telle
** Created:     2020-02-02
** Copyright:   (c) 2006-2020 Ulrich Telle
** License:     MIT
*/

#include "cipher_common.h"

/* --- RC4 cipher (System.Data.SQLite) --- */
#if HAVE_CIPHER_RC4

#define CIPHER_NAME_RC4 "rc4"

/*
** Configuration parameters for "rc4"
**
** - legacy mode : compatibility with System.Data.SQLite encryption
**                 (page 1 fully encrypted)
**                 only legacy mode is supported
**                 possible value:  1 = yes
*/

#define RC4_LEGACY_DEFAULT 1

SQLITE_PRIVATE CipherParams mcRC4Params[] =
{
  { "legacy",           RC4_LEGACY_DEFAULT, RC4_LEGACY_DEFAULT, RC4_LEGACY_DEFAULT, RC4_LEGACY_DEFAULT },
  { "legacy_page_size", 0,                  0,                  0,                  SQLITE_MAX_PAGE_SIZE },
  CIPHER_PARAMS_SENTINEL
};


#define KEYLENGTH_RC4 16

typedef struct _RC4Cipher
{
  int       m_legacy;
  int       m_legacyPageSize;
  int       m_keyLength;
  uint8_t   m_key[KEYLENGTH_RC4];
} RC4Cipher;

static void*
AllocateRC4Cipher(sqlite3* db)
{
  RC4Cipher* rc4Cipher = (RC4Cipher*) sqlite3_malloc(sizeof(RC4Cipher));
  if (rc4Cipher != NULL)
  {
    rc4Cipher->m_keyLength = KEYLENGTH_RC4;
    memset(rc4Cipher->m_key, 0, KEYLENGTH_RC4);
  }
  if (rc4Cipher != NULL)
  {
    CipherParams* cipherParams = sqlite3mcGetCipherParams(db, CIPHER_NAME_RC4);
    rc4Cipher->m_legacy = sqlite3mcGetCipherParameter(cipherParams, "legacy");
    rc4Cipher->m_legacyPageSize = sqlite3mcGetCipherParameter(cipherParams, "legacy_page_size");
  }
  return rc4Cipher;
}

static void
FreeRC4Cipher(void* cipher)
{
  RC4Cipher* localCipher = (RC4Cipher*) cipher;
  memset(localCipher, 0, sizeof(RC4Cipher));
  sqlite3_free(localCipher);
}

static void
CloneRC4Cipher(void* cipherTo, void* cipherFrom)
{
  RC4Cipher* rc4CipherTo = (RC4Cipher*) cipherTo;
  RC4Cipher* rc4CipherFrom = (RC4Cipher*) cipherFrom;
  rc4CipherTo->m_legacy = rc4CipherFrom->m_legacy;
  rc4CipherTo->m_legacyPageSize = rc4CipherFrom->m_legacyPageSize;
  rc4CipherTo->m_keyLength = rc4CipherFrom->m_keyLength;
  memcpy(rc4CipherTo->m_key, rc4CipherFrom->m_key, KEYLENGTH_RC4);
}

static int
GetLegacyRC4Cipher(void* cipher)
{
  RC4Cipher* rc4Cipher = (RC4Cipher*)cipher;
  return rc4Cipher->m_legacy;
}

static int
GetPageSizeRC4Cipher(void* cipher)
{
  RC4Cipher* rc4Cipher = (RC4Cipher*) cipher;
  int pageSize = 0;
  if (rc4Cipher->m_legacy != 0)
  {
    pageSize = rc4Cipher->m_legacyPageSize;
    if ((pageSize < 512) || (pageSize > SQLITE_MAX_PAGE_SIZE) || (((pageSize - 1) & pageSize) != 0))
    {
      pageSize = 0;
    }
  }
  return pageSize;
}

static int
GetReservedRC4Cipher(void* cipher)
{
  return 0;
}

static unsigned char*
GetSaltRC4Cipher(void* cipher)
{
  return NULL;
}

static void
GenerateKeyRC4Cipher(void* cipher, BtShared* pBt, char* userPassword, int passwordLength, int rekey, unsigned char* cipherSalt)
{
  RC4Cipher* rc4Cipher = (RC4Cipher*) cipher;
  unsigned char digest[SHA1_DIGEST_SIZE];
  sha1_ctx ctx;

  sha1_init(&ctx);
  sha1_update(&ctx, userPassword, passwordLength);
  sha1_final(&ctx, digest);

  memcpy(rc4Cipher->m_key, digest, 16);
/*  memset(rc4Cipher->m_key+5, 0, rc4Cipher->m_keyLength-5);*/
}

static int
EncryptPageRC4Cipher(void* cipher, int page, unsigned char* data, int len, int reserved)
{
  RC4Cipher* rc4Cipher = (RC4Cipher*) cipher;
  int rc = SQLITE_OK;

  /* Use the legacy encryption scheme */
  unsigned char* key = rc4Cipher->m_key;
  sqlite3mcRC4(key, rc4Cipher->m_keyLength, data, len, data);

  return rc;
}

static int
DecryptPageRC4Cipher(void* cipher, int page, unsigned char* data, int len, int reserved, int hmacCheck)
{
  RC4Cipher* rc4Cipher = (RC4Cipher*) cipher;
  int rc = SQLITE_OK;

  /* Use the legacy encryption scheme */
  sqlite3mcRC4(rc4Cipher->m_key, rc4Cipher->m_keyLength, data, len, data);

  return rc;
}

SQLITE_PRIVATE const CipherDescriptor mcRC4Descriptor =
{
  CIPHER_NAME_RC4,
  AllocateRC4Cipher,
  FreeRC4Cipher,
  CloneRC4Cipher,
  GetLegacyRC4Cipher,
  GetPageSizeRC4Cipher,
  GetReservedRC4Cipher,
  GetSaltRC4Cipher,
  GenerateKeyRC4Cipher,
  EncryptPageRC4Cipher,
  DecryptPageRC4Cipher
};
#endif

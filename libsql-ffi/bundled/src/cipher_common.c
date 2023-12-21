/*
** Name:        cipher_common.c
** Purpose:     Implementation of SQLite codecs
** Author:      Ulrich Telle
** Created:     2020-02-02
** Copyright:   (c) 2006-2022 Ulrich Telle
** License:     MIT
*/

#include "cipher_common.h"

static unsigned char padding[] =
"\x28\xBF\x4E\x5E\x4E\x75\x8A\x41\x64\x00\x4E\x56\xFF\xFA\x01\x08\x2E\x2E\x00\xB6\xD0\x68\x3E\x80\x2F\x0C\xA9\xFE\x64\x53\x69\x7A";

/* --- Codec Descriptor Table --- */

/*
** Common configuration parameters
**
** - cipher     : default cipher type
** - hmac_check : flag whether page hmac should be verified on read
*/

static CipherParams commonParams[] =
{
  { "cipher",          CODEC_TYPE_UNKNOWN,   CODEC_TYPE_UNKNOWN, 1, CODEC_COUNT_MAX },
  { "hmac_check",                       1,                    1, 0,               1 },
  { "mc_legacy_wal", SQLITE3MC_LEGACY_WAL, SQLITE3MC_LEGACY_WAL, 0,               1 },
  CIPHER_PARAMS_SENTINEL
};

#define CIPHER_NAME_GLOBAL "global"

static CodecParameter globalCommonParams   = { CIPHER_NAME_GLOBAL, CODEC_TYPE_UNKNOWN, commonParams };
static CodecParameter globalSentinelParams = { "",                 CODEC_TYPE_UNKNOWN, NULL };

SQLITE_PRIVATE int
sqlite3mcGetCipherParameter(CipherParams* cipherParams, const char* paramName)
{
  int value = -1;
  for (; cipherParams->m_name[0] != 0; ++cipherParams)
  {
    if (sqlite3_stricmp(paramName, cipherParams->m_name) == 0) break;
  }
  if (cipherParams->m_name[0] != 0)
  {
    value = cipherParams->m_value;
    cipherParams->m_value = cipherParams->m_default;
  }
  return value;
}

typedef struct _CipherName
{
  char m_name[CIPHER_NAME_MAXLEN];
} CipherName;

static int globalCipherCount = 0;
static char* globalSentinelName = "";
static CipherName globalCipherNameTable[CODEC_COUNT_LIMIT + 2] = { 0 };
static CodecParameter globalCodecParameterTable[CODEC_COUNT_LIMIT + 2];

SQLITE_PRIVATE CodecParameter*
sqlite3mcCloneCodecParameterTable()
{
  /* Count number of codecs and cipher parameters */
  int nTables = 0;
  int nParams = 0;
  int j, k, n;
  CipherParams* cloneCipherParams;
  CodecParameter* cloneCodecParams;

  for (j = 0; globalCodecParameterTable[j].m_name[0] != 0; ++j)
  {
    CipherParams* params = globalCodecParameterTable[j].m_params;
    for (k = 0; params[k].m_name[0] != 0; ++k);
    nParams += k;
  }
  nTables = j;

  /* Allocate memory for cloned codec parameter tables (including sentinel for each table) */
  cloneCipherParams = (CipherParams*) sqlite3_malloc((nParams + nTables) * sizeof(CipherParams));
  cloneCodecParams = (CodecParameter*) sqlite3_malloc((nTables + 1) * sizeof(CodecParameter));

  /* Create copy of tables */
  if (cloneCodecParams != NULL)
  {
    int offset = 0;
    for (j = 0; j < nTables; ++j)
    {
      CipherParams* params = globalCodecParameterTable[j].m_params;
      cloneCodecParams[j].m_name = globalCodecParameterTable[j].m_name;
      cloneCodecParams[j].m_id = globalCodecParameterTable[j].m_id;
      cloneCodecParams[j].m_params = &cloneCipherParams[offset];
      for (n = 0; params[n].m_name[0] != 0; ++n);
      /* Copy all parameters of the current table (including sentinel) */
      for (k = 0; k <= n; ++k)
      {
        cloneCipherParams[offset + k].m_name     = params[k].m_name;
        cloneCipherParams[offset + k].m_value    = params[k].m_value;
        cloneCipherParams[offset + k].m_default  = params[k].m_default;
        cloneCipherParams[offset + k].m_minValue = params[k].m_minValue;
        cloneCipherParams[offset + k].m_maxValue = params[k].m_maxValue;
      }
      offset += (n + 1);
    }
    cloneCodecParams[nTables].m_name = globalCodecParameterTable[nTables].m_name;
    cloneCodecParams[nTables].m_id = globalCodecParameterTable[nTables].m_id;
    cloneCodecParams[nTables].m_params = NULL;
  }
  else
  {
    sqlite3_free(cloneCipherParams);
  }
  return cloneCodecParams;
}

SQLITE_PRIVATE void
sqlite3mcFreeCodecParameterTable(CodecParameter* codecParams)
{
  sqlite3_free(codecParams[0].m_params);
  sqlite3_free(codecParams);
}

static const CipherDescriptor mcSentinelDescriptor =
{
  "", NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
};

static const CipherDescriptor mcDummyDescriptor =
{
  "@dummy@", NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
};

static CipherDescriptor globalCodecDescriptorTable[CODEC_COUNT_MAX + 1];

/* --- Codec --- */

SQLITE_PRIVATE CodecParameter*
sqlite3mcGetCodecParams(sqlite3* db);

SQLITE_PRIVATE int
sqlite3mcGetCipherType(sqlite3* db)
{
  CodecParameter* codecParams = (db != NULL) ? sqlite3mcGetCodecParams(db) : globalCodecParameterTable;
  CipherParams* cipherParamTable = (codecParams != NULL) ? codecParams[0].m_params : commonParams;
  int cipherType = CODEC_TYPE;
  CipherParams* cipher = cipherParamTable;
  for (; cipher->m_name[0] != 0; ++cipher)
  {
    if (sqlite3_stricmp("cipher", cipher->m_name) == 0) break;
  }
  if (cipher->m_name[0] != 0)
  {
    cipherType = cipher->m_value;
    cipher->m_value = cipher->m_default;
  }
  return cipherType;
}

SQLITE_PRIVATE CipherParams*
sqlite3mcGetCipherParams(sqlite3* db, const char* cipherName)
{
  int j = 0;
  int cipherType = sqlite3mc_cipher_index(cipherName);
  CodecParameter* codecParams = (db != NULL) ? sqlite3mcGetCodecParams(db) : globalCodecParameterTable;
  if (codecParams == NULL)
  {
    codecParams = globalCodecParameterTable;
  }
  if (cipherType > 0)
  {
    for (j = 1; codecParams[j].m_id > 0; ++j)
    {
      if (cipherType == codecParams[j].m_id) break;
    }
  }
  CipherParams* cipherParamTable = codecParams[j].m_params;
  return cipherParamTable;
}

SQLITE_PRIVATE int
sqlite3mcCodecInit(Codec* codec)
{
  int rc = SQLITE_OK;
  if (codec != NULL)
  {
    codec->m_isEncrypted = 0;
    codec->m_hmacCheck = 1;
    codec->m_walLegacy = 0;

    codec->m_hasReadCipher = 0;
    codec->m_readCipherType = CODEC_TYPE_UNKNOWN;
    codec->m_readCipher = NULL;
    codec->m_readReserved = -1;

    codec->m_hasWriteCipher = 0;
    codec->m_writeCipherType = CODEC_TYPE_UNKNOWN;
    codec->m_writeCipher = NULL;
    codec->m_writeReserved = -1;

    codec->m_db = NULL;
#if 0
    codec->m_bt = NULL;
#endif
    codec->m_btShared = NULL;
    memset(codec->m_page, 0, sizeof(codec->m_page));
    codec->m_pageSize = 0;
    codec->m_reserved = 0;
    codec->m_hasKeySalt = 0;
    memset(codec->m_keySalt, 0, sizeof(codec->m_keySalt));
  }
  else
  {
    rc = SQLITE_NOMEM;
  }
  return rc;
}

SQLITE_PRIVATE void
sqlite3mcCodecTerm(Codec* codec)
{
  if (codec->m_readCipher != NULL)
  {
    globalCodecDescriptorTable[codec->m_readCipherType - 1].m_freeCipher(codec->m_readCipher);
    codec->m_readCipher = NULL;
  }
  if (codec->m_writeCipher != NULL)
  {
    globalCodecDescriptorTable[codec->m_writeCipherType - 1].m_freeCipher(codec->m_writeCipher);
    codec->m_writeCipher = NULL;
  }
  memset(codec, 0, sizeof(Codec));
}

SQLITE_PRIVATE void
sqlite3mcClearKeySalt(Codec* codec)
{
  codec->m_hasKeySalt = 0;
  memset(codec->m_keySalt, 0, sizeof(codec->m_keySalt));
}

SQLITE_PRIVATE int
sqlite3mcCodecSetup(Codec* codec, int cipherType, char* userPassword, int passwordLength)
{
  int rc = SQLITE_OK;
  CipherParams* globalParams = sqlite3mcGetCipherParams(codec->m_db, CIPHER_NAME_GLOBAL);
  codec->m_isEncrypted = 1;
  codec->m_hmacCheck = sqlite3mcGetCipherParameter(globalParams, "hmac_check");
  codec->m_walLegacy = sqlite3mcGetCipherParameter(globalParams, "mc_legacy_wal");
  codec->m_hasReadCipher = 1;
  codec->m_hasWriteCipher = 1;
  codec->m_readCipherType = cipherType;
  codec->m_readCipher = globalCodecDescriptorTable[codec->m_readCipherType-1].m_allocateCipher(codec->m_db);
  if (codec->m_readCipher != NULL)
  {
    unsigned char* keySalt = (codec->m_hasKeySalt != 0) ? codec->m_keySalt : NULL;
    sqlite3mcGenerateReadKey(codec, userPassword, passwordLength, keySalt);
    rc = sqlite3mcCopyCipher(codec, 1);
  }
  else
  {
    rc = SQLITE_NOMEM;
  }
  return rc;
}

SQLITE_PRIVATE int
sqlite3mcSetupWriteCipher(Codec* codec, int cipherType, char* userPassword, int passwordLength)
{
  int rc = SQLITE_OK;
  CipherParams* globalParams = sqlite3mcGetCipherParams(codec->m_db, CIPHER_NAME_GLOBAL);
  if (codec->m_writeCipher != NULL)
  {
    globalCodecDescriptorTable[codec->m_writeCipherType-1].m_freeCipher(codec->m_writeCipher);
  }
  codec->m_isEncrypted = 1;
  codec->m_hmacCheck = sqlite3mcGetCipherParameter(globalParams, "hmac_check");
  codec->m_walLegacy = sqlite3mcGetCipherParameter(globalParams, "mc_legacy_wal");
  codec->m_hasWriteCipher = 1;
  codec->m_writeCipherType = cipherType;
  codec->m_writeCipher = globalCodecDescriptorTable[codec->m_writeCipherType-1].m_allocateCipher(codec->m_db);
  if (codec->m_writeCipher != NULL)
  {
    unsigned char* keySalt = (codec->m_hasKeySalt != 0) ? codec->m_keySalt : NULL;
    sqlite3mcGenerateWriteKey(codec, userPassword, passwordLength, keySalt);
  }
  else
  {
    rc = SQLITE_NOMEM;
  }
  return rc;
}

SQLITE_PRIVATE void
sqlite3mcSetIsEncrypted(Codec* codec, int isEncrypted)
{
  codec->m_isEncrypted = isEncrypted;
}

SQLITE_PRIVATE void
sqlite3mcSetReadCipherType(Codec* codec, int cipherType)
{
  codec->m_readCipherType = cipherType;
}

SQLITE_PRIVATE void
sqlite3mcSetWriteCipherType(Codec* codec, int cipherType)
{
  codec->m_writeCipherType = cipherType;
}

SQLITE_PRIVATE void
sqlite3mcSetHasReadCipher(Codec* codec, int hasReadCipher)
{
  codec->m_hasReadCipher = hasReadCipher;
}

SQLITE_PRIVATE void
sqlite3mcSetHasWriteCipher(Codec* codec, int hasWriteCipher)
{
  codec->m_hasWriteCipher = hasWriteCipher;
}

SQLITE_PRIVATE void
sqlite3mcSetDb(Codec* codec, sqlite3* db)
{
  codec->m_db = db;
}

SQLITE_PRIVATE void
sqlite3mcSetBtree(Codec* codec, Btree* bt)
{
#if 0
  codec->m_bt = bt;
#endif
  codec->m_btShared = bt->pBt;
}

SQLITE_PRIVATE void
sqlite3mcSetReadReserved(Codec* codec, int reserved)
{
  codec->m_readReserved = reserved;
}

SQLITE_PRIVATE void
sqlite3mcSetWriteReserved(Codec* codec, int reserved)
{
  codec->m_writeReserved = reserved;
}

SQLITE_PRIVATE int
sqlite3mcIsEncrypted(Codec* codec)
{
  return codec->m_isEncrypted;
}

SQLITE_PRIVATE int
sqlite3mcHasReadCipher(Codec* codec)
{
  return codec->m_hasReadCipher;
}

SQLITE_PRIVATE int
sqlite3mcHasWriteCipher(Codec* codec)
{
  return codec->m_hasWriteCipher;
}

SQLITE_PRIVATE BtShared*
sqlite3mcGetBtShared(Codec* codec)
{
  return codec->m_btShared;
}

SQLITE_PRIVATE int
sqlite3mcGetPageSize(Codec* codec)
{
  return codec->m_btShared->pageSize;
}

SQLITE_PRIVATE int
sqlite3mcGetReadReserved(Codec* codec)
{
  return codec->m_readReserved;
}

SQLITE_PRIVATE int
sqlite3mcGetWriteReserved(Codec* codec)
{
  return codec->m_writeReserved;
}

SQLITE_PRIVATE unsigned char*
sqlite3mcGetPageBuffer(Codec* codec)
{
  return &codec->m_page[4];
}

SQLITE_PRIVATE int
sqlite3mcGetLegacyReadCipher(Codec* codec)
{
  int legacy = (codec->m_hasReadCipher  && codec->m_readCipher != NULL) ? globalCodecDescriptorTable[codec->m_readCipherType - 1].m_getLegacy(codec->m_readCipher) : 0;
  return legacy;
}

SQLITE_PRIVATE int
sqlite3mcGetLegacyWriteCipher(Codec* codec)
{
  int legacy = (codec->m_hasWriteCipher && codec->m_writeCipher != NULL) ? globalCodecDescriptorTable[codec->m_writeCipherType - 1].m_getLegacy(codec->m_writeCipher) : -1;
  return legacy;
}

SQLITE_PRIVATE int
sqlite3mcGetPageSizeReadCipher(Codec* codec)
{
  int pageSize = (codec->m_hasReadCipher  && codec->m_readCipher != NULL) ? globalCodecDescriptorTable[codec->m_readCipherType - 1].m_getPageSize(codec->m_readCipher) : 0;
  return pageSize;
}

SQLITE_PRIVATE int
sqlite3mcGetPageSizeWriteCipher(Codec* codec)
{
  int pageSize = (codec->m_hasWriteCipher && codec->m_writeCipher != NULL) ? globalCodecDescriptorTable[codec->m_writeCipherType - 1].m_getPageSize(codec->m_writeCipher) : -1;
  return pageSize;
}

SQLITE_PRIVATE int
sqlite3mcGetReservedReadCipher(Codec* codec)
{
  int reserved = (codec->m_hasReadCipher  && codec->m_readCipher != NULL) ? globalCodecDescriptorTable[codec->m_readCipherType-1].m_getReserved(codec->m_readCipher) : -1;
  return reserved;
}

SQLITE_PRIVATE int
sqlite3mcGetReservedWriteCipher(Codec* codec)
{
  int reserved = (codec->m_hasWriteCipher && codec->m_writeCipher != NULL) ? globalCodecDescriptorTable[codec->m_writeCipherType-1].m_getReserved(codec->m_writeCipher) : -1;
  return reserved;
}

SQLITE_PRIVATE int
sqlite3mcReservedEqual(Codec* codec)
{
  int readReserved  = (codec->m_hasReadCipher  && codec->m_readCipher  != NULL) ? globalCodecDescriptorTable[codec->m_readCipherType-1].m_getReserved(codec->m_readCipher)   : -1;
  int writeReserved = (codec->m_hasWriteCipher && codec->m_writeCipher != NULL) ? globalCodecDescriptorTable[codec->m_writeCipherType-1].m_getReserved(codec->m_writeCipher) : -1;
  return (readReserved == writeReserved);
}

SQLITE_PRIVATE unsigned char*
sqlite3mcGetSaltWriteCipher(Codec* codec)
{
  unsigned char* salt = (codec->m_hasWriteCipher && codec->m_writeCipher != NULL) ? globalCodecDescriptorTable[codec->m_writeCipherType - 1].m_getSalt(codec->m_writeCipher) : NULL;
  return salt;
}

SQLITE_PRIVATE int
sqlite3mcCodecCopy(Codec* codec, Codec* other)
{
  int rc = SQLITE_OK;
  codec->m_isEncrypted = other->m_isEncrypted;
  codec->m_hmacCheck = other->m_hmacCheck;
  codec->m_walLegacy = other->m_walLegacy;
  codec->m_hasReadCipher = other->m_hasReadCipher;
  codec->m_hasWriteCipher = other->m_hasWriteCipher;
  codec->m_readCipherType = other->m_readCipherType;
  codec->m_writeCipherType = other->m_writeCipherType;
  codec->m_readCipher = NULL;
  codec->m_writeCipher = NULL;
  codec->m_readReserved = other->m_readReserved;
  codec->m_writeReserved = other->m_writeReserved;

  if (codec->m_hasReadCipher)
  {
    codec->m_readCipher = globalCodecDescriptorTable[codec->m_readCipherType - 1].m_allocateCipher(codec->m_db);
    if (codec->m_readCipher != NULL)
    {
      globalCodecDescriptorTable[codec->m_readCipherType - 1].m_cloneCipher(codec->m_readCipher, other->m_readCipher);
    }
    else
    {
      rc = SQLITE_NOMEM;
    }
  }

  if (codec->m_hasWriteCipher)
  {
    codec->m_writeCipher = globalCodecDescriptorTable[codec->m_writeCipherType - 1].m_allocateCipher(codec->m_db);
    if (codec->m_writeCipher != NULL)
    {
      globalCodecDescriptorTable[codec->m_writeCipherType - 1].m_cloneCipher(codec->m_writeCipher, other->m_writeCipher);
    }
    else
    {
      rc = SQLITE_NOMEM;
    }
  }
  codec->m_db = other->m_db;
#if 0
  codec->m_bt = other->m_bt;
#endif
  codec->m_btShared = other->m_btShared;
  return rc;
}

SQLITE_PRIVATE int
sqlite3mcCopyCipher(Codec* codec, int read2write)
{
  int rc = SQLITE_OK;
  if (read2write)
  {
    if (codec->m_writeCipher != NULL && codec->m_writeCipherType != codec->m_readCipherType)
    {
      globalCodecDescriptorTable[codec->m_writeCipherType-1].m_freeCipher(codec->m_writeCipher);
      codec->m_writeCipher = NULL;
    }
    if (codec->m_writeCipher == NULL)
    {
      codec->m_writeCipherType = codec->m_readCipherType;
      codec->m_writeCipher = globalCodecDescriptorTable[codec->m_writeCipherType-1].m_allocateCipher(codec->m_db);
    }
    if (codec->m_writeCipher != NULL)
    {
      globalCodecDescriptorTable[codec->m_writeCipherType-1].m_cloneCipher(codec->m_writeCipher, codec->m_readCipher);
    }
    else
    {
      rc = SQLITE_NOMEM;
    }
  }
  else
  {
    if (codec->m_readCipher != NULL && codec->m_readCipherType != codec->m_writeCipherType)
    {
      globalCodecDescriptorTable[codec->m_readCipherType-1].m_freeCipher(codec->m_readCipher);
      codec->m_readCipher = NULL;
    }
    if (codec->m_readCipher == NULL)
    {
      codec->m_readCipherType = codec->m_writeCipherType;
      codec->m_readCipher = globalCodecDescriptorTable[codec->m_readCipherType-1].m_allocateCipher(codec->m_db);
    }
    if (codec->m_readCipher != NULL)
    {
      globalCodecDescriptorTable[codec->m_readCipherType-1].m_cloneCipher(codec->m_readCipher, codec->m_writeCipher);
    }
    else
    {
      rc = SQLITE_NOMEM;
    }
  }
  return rc;
}

SQLITE_PRIVATE void
sqlite3mcPadPassword(char* password, int pswdlen, unsigned char pswd[32])
{
  int j;
  int p = 0;
  int m = pswdlen;
  if (m > 32) m = 32;

  for (j = 0; j < m; j++)
  {
    pswd[p++] = (unsigned char) password[j];
  }
  for (j = 0; p < 32 && j < 32; j++)
  {
    pswd[p++] = padding[j];
  }
}

SQLITE_PRIVATE void
sqlite3mcGenerateReadKey(Codec* codec, char* userPassword, int passwordLength, unsigned char* cipherSalt)
{
  globalCodecDescriptorTable[codec->m_readCipherType-1].m_generateKey(codec->m_readCipher, codec->m_btShared, userPassword, passwordLength, 0, cipherSalt);
}

SQLITE_PRIVATE void
sqlite3mcGenerateWriteKey(Codec* codec, char* userPassword, int passwordLength, unsigned char* cipherSalt)
{
  globalCodecDescriptorTable[codec->m_writeCipherType-1].m_generateKey(codec->m_writeCipher, codec->m_btShared, userPassword, passwordLength, 1, cipherSalt);
}

SQLITE_PRIVATE int
sqlite3mcEncrypt(Codec* codec, int page, unsigned char* data, int len, int useWriteKey)
{
  int cipherType = (useWriteKey) ? codec->m_writeCipherType : codec->m_readCipherType;
  void* cipher = (useWriteKey) ? codec->m_writeCipher : codec->m_readCipher;
  int reserved = (useWriteKey) ? (codec->m_writeReserved >= 0) ? codec->m_writeReserved : codec->m_reserved
                               : (codec->m_readReserved >= 0) ? codec->m_readReserved : codec->m_reserved;
  return globalCodecDescriptorTable[cipherType-1].m_encryptPage(cipher, page, data, len, reserved);
}

SQLITE_PRIVATE int
sqlite3mcDecrypt(Codec* codec, int page, unsigned char* data, int len)
{
  int cipherType = codec->m_readCipherType;
  void* cipher = codec->m_readCipher;
  int reserved = (codec->m_readReserved >= 0) ? codec->m_readReserved : codec->m_reserved;
  return globalCodecDescriptorTable[cipherType-1].m_decryptPage(cipher, page, data, len, reserved, codec->m_hmacCheck);
}

#if HAVE_CIPHER_SQLCIPHER

SQLITE_PRIVATE void
sqlite3mcConfigureSQLCipherVersion(sqlite3* db, int configDefault, int legacyVersion)
{
  static char* stdNames[] = { "legacy_page_size",         "kdf_iter",         "hmac_use",         "kdf_algorithm",         "hmac_algorithm",         NULL };
  static char* defNames[] = { "default:legacy_page_size", "default:kdf_iter", "default:hmac_use", "default:kdf_algorithm", "default:hmac_algorithm", NULL };
  static int versionParams[SQLCIPHER_VERSION_MAX][5] =
  {
    { 1024,   4000, 0, SQLCIPHER_KDF_ALGORITHM_SHA1,   SQLCIPHER_HMAC_ALGORITHM_SHA1   }, 
    { 1024,   4000, 1, SQLCIPHER_KDF_ALGORITHM_SHA1,   SQLCIPHER_HMAC_ALGORITHM_SHA1   },
    { 1024,  64000, 1, SQLCIPHER_KDF_ALGORITHM_SHA1,   SQLCIPHER_HMAC_ALGORITHM_SHA1   },
    { 4096, 256000, 1, SQLCIPHER_KDF_ALGORITHM_SHA512, SQLCIPHER_HMAC_ALGORITHM_SHA512 }
  };
  if (legacyVersion > 0 && legacyVersion <= SQLCIPHER_VERSION_MAX)
  {
    char** names = (configDefault != 0) ? defNames : stdNames;
    int* values = &versionParams[legacyVersion - 1][0];
    int j;
    for (j = 0; names[j] != NULL; ++j)
    {
      sqlite3mc_config_cipher(db, "sqlcipher", names[j], values[j]);
    }
  }
}

#endif

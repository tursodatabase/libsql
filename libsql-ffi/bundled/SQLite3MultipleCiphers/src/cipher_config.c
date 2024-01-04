/*
** Name:        cipher_config.c
** Purpose:     Configuration of SQLite codecs
** Author:      Ulrich Telle
** Created:     2020-03-02
** Copyright:   (c) 2006-2023 Ulrich Telle
** License:     MIT
*/

#include "cipher_common.h"
#include "cipher_config.h"

/* --- Codec --- */

SQLITE_PRIVATE int
sqlite3mcGetGlobalCipherCount();

SQLITE_PRIVATE Codec*
sqlite3mcGetCodec(sqlite3* db, const char* zDbName);

SQLITE_PRIVATE void
sqlite3mcConfigTable(sqlite3_context* context, int argc, sqlite3_value** argv)
{
  CodecParameter* codecParams = (CodecParameter*) sqlite3_user_data(context);
  assert(argc == 0);
  sqlite3_result_pointer(context, codecParams, "sqlite3mc_codec_params", 0);
}

SQLITE_PRIVATE CodecParameter*
sqlite3mcGetCodecParams(sqlite3* db)
{
  CodecParameter* codecParams = NULL;
  sqlite3_stmt* pStmt = 0;
  int rc = sqlite3_prepare_v2(db, "SELECT sqlite3mc_config_table();", -1, &pStmt, 0);
  if (rc == SQLITE_OK)
  {
    if (SQLITE_ROW == sqlite3_step(pStmt))
    {
      sqlite3_value* ptrValue = sqlite3_column_value(pStmt, 0);
      codecParams = (CodecParameter*) sqlite3_value_pointer(ptrValue, "sqlite3mc_codec_params");
    }
    sqlite3_finalize(pStmt);
  }
  return codecParams;
}

SQLITE_API int
sqlite3mc_config(sqlite3* db, const char* paramName, int newValue)
{
  int value = -1;
  CodecParameter* codecParams;
  int hasDefaultPrefix = 0;
  int hasMinPrefix = 0;
  int hasMaxPrefix = 0;
  CipherParams* param;

#ifndef SQLITE_OMIT_AUTOINIT
  if (sqlite3_initialize()) return value;
#endif

  if (paramName == NULL || (db == NULL && newValue >= 0))
  {
    return value;
  }

  codecParams = (db != NULL) ? sqlite3mcGetCodecParams(db) : globalCodecParameterTable;
  if (codecParams == NULL)
  {
    return value;
  }

  if (sqlite3_strnicmp(paramName, "default:", 8) == 0)
  {
    hasDefaultPrefix = 1;
    paramName += 8;
  }
  if (sqlite3_strnicmp(paramName, "min:", 4) == 0)
  {
    hasMinPrefix = 1;
    paramName += 4;
  }
  if (sqlite3_strnicmp(paramName, "max:", 4) == 0)
  {
    hasMaxPrefix = 1;
    paramName += 4;
  }

  param = codecParams[0].m_params;
  for (; param->m_name[0] != 0; ++param)
  {
    if (sqlite3_stricmp(paramName, param->m_name) == 0) break;
  }
  if (param->m_name[0] != 0)
  {
    int cipherCount = sqlite3mcGetGlobalCipherCount();
    if (db != NULL)
    {
      sqlite3_mutex_enter(db->mutex);
    }
    else
    {
      sqlite3_mutex_enter(sqlite3_mutex_alloc(SQLITE_MUTEX_STATIC_MAIN));
    }
    value = (hasDefaultPrefix) ? param->m_default : (hasMinPrefix) ? param->m_minValue : (hasMaxPrefix) ? param->m_maxValue : param->m_value;
    if (!hasMinPrefix && !hasMaxPrefix && newValue >= 0 && newValue >= param->m_minValue && newValue <= param->m_maxValue)
    {
      int allowChange = 1;

      /* Allow cipher change only if new cipher is actually available */
      if (sqlite3_stricmp(paramName, "cipher") == 0)
      {
        allowChange = newValue > 0 && newValue <= cipherCount;
      }

      if (allowChange)
      {
        /* Do not allow to change the default value for parameter "hmac_check" */
        if (hasDefaultPrefix && (sqlite3_stricmp(paramName, "hmac_check") != 0))
        {
          param->m_default = newValue;
        }
        param->m_value = newValue;
        value = newValue;
      }
    }
    if (db != NULL)
    {
      sqlite3_mutex_leave(db->mutex);
    }
    else
    {
      sqlite3_mutex_leave(sqlite3_mutex_alloc(SQLITE_MUTEX_STATIC_MAIN));
    }
  }
  return value;
}

SQLITE_API int
sqlite3mc_cipher_count()
{
#ifndef SQLITE_OMIT_AUTOINIT
  if (sqlite3_initialize()) return 0;
#endif
  return sqlite3mcGetGlobalCipherCount();
}

SQLITE_API int
sqlite3mc_cipher_index(const char* cipherName)
{
  int count;
  int j;
#ifndef SQLITE_OMIT_AUTOINIT
  if (sqlite3_initialize()) return -1;
#endif
  count = sqlite3mcGetGlobalCipherCount();
  j = 0;
  for (j = 0; j < count && globalCodecDescriptorTable[j].m_name[0] != 0; ++j)
  {
    if (sqlite3_stricmp(cipherName, globalCodecDescriptorTable[j].m_name) == 0) break;
  }
  return (j < count && globalCodecDescriptorTable[j].m_name[0] != 0) ? j + 1 : -1;
}

SQLITE_API const char*
sqlite3mc_cipher_name(int cipherIndex)
{
  static char cipherName[CIPHER_NAME_MAXLEN] = "";
  int count;
  int j;
#ifndef SQLITE_OMIT_AUTOINIT
  if( sqlite3_initialize() ) return cipherName;
#endif
  count = sqlite3mcGetGlobalCipherCount();
  j = 0;
  cipherName[0] = '\0';
  if (cipherIndex > 0 && cipherIndex <= count)
  {
    for (j = 0; j < count && globalCodecDescriptorTable[j].m_name[0] != 0; ++j)
    {
      if (cipherIndex == j + 1) break;
    }
    if (j < count && globalCodecDescriptorTable[j].m_name[0] != 0)
    {
      strncpy(cipherName, globalCodecDescriptorTable[j].m_name, CIPHER_NAME_MAXLEN - 1);
      cipherName[CIPHER_NAME_MAXLEN - 1] = '\0';
    }
  }
  return cipherName;
}

SQLITE_API int
sqlite3mc_config_cipher(sqlite3* db, const char* cipherName, const char* paramName, int newValue)
{
  int value = -1;
  CodecParameter* codecParams;
  CipherParams* cipherParamTable = NULL;
  int j = 0;

#ifndef SQLITE_OMIT_AUTOINIT
  if (sqlite3_initialize()) return value;
#endif

  if (cipherName == NULL || paramName == NULL)
  {
    sqlite3_log(SQLITE_WARNING,
                "sqlite3mc_config_cipher: cipher name ('%s*) or parameter ('%s*) missing",
                (cipherName == NULL) ? "" : cipherName, (paramName == NULL) ? "" : paramName);
    return value;
  }
  else if (db == NULL && newValue >= 0)
  {
    sqlite3_log(SQLITE_WARNING,
                "sqlite3mc_config_cipher: global change of parameter '%s' for cipher '%s' not supported",
                paramName, cipherName);
    return value;
  }

  codecParams = (db != NULL) ? sqlite3mcGetCodecParams(db) : globalCodecParameterTable;
  if (codecParams == NULL)
  {
    sqlite3_log(SQLITE_WARNING,
                "sqlite3mc_config_cipher: codec parameter table not found");
    return value;
  }

  for (j = 0; codecParams[j].m_name[0] != 0; ++j)
  {
    if (sqlite3_stricmp(cipherName, codecParams[j].m_name) == 0) break;
  }
  if (codecParams[j].m_name[0] != 0)
  {
    cipherParamTable = codecParams[j].m_params;
  }

  if (cipherParamTable != NULL)
  {
    int hasDefaultPrefix = 0;
    int hasMinPrefix = 0;
    int hasMaxPrefix = 0;
    CipherParams* param = cipherParamTable;

    if (sqlite3_strnicmp(paramName, "default:", 8) == 0)
    {
      hasDefaultPrefix = 1;
      paramName += 8;
    }
    if (sqlite3_strnicmp(paramName, "min:", 4) == 0)
    {
      hasMinPrefix = 1;
      paramName += 4;
    }
    if (sqlite3_strnicmp(paramName, "max:", 4) == 0)
    {
      hasMaxPrefix = 1;
      paramName += 4;
    }

#if HAVE_CIPHER_SQLCIPHER
    /* Special handling for SQLCipher legacy mode */
    if (db != NULL &&
        sqlite3_stricmp(cipherName, "sqlcipher") == 0 &&
        sqlite3_stricmp(paramName, "legacy") == 0)
    {
      if (!hasMinPrefix && !hasMaxPrefix)
      {
        if (newValue > 0 && newValue <= SQLCIPHER_VERSION_MAX)
        {
          sqlite3mcConfigureSQLCipherVersion(db, hasDefaultPrefix, newValue);
        }
        else if (newValue != -1)
        {
          sqlite3_log(SQLITE_WARNING,
                      "sqlite3mc_config_cipher: SQLCipher legacy version %d out of range [%d..%d]",
                      newValue, 1, SQLCIPHER_VERSION_MAX);
        }
      }
    }
#endif

    for (; param->m_name[0] != 0; ++param)
    {
      if (sqlite3_stricmp(paramName, param->m_name) == 0) break;
    }
    if (param->m_name[0] != 0)
    {
      if (db != NULL)
      {
        sqlite3_mutex_enter(db->mutex);
      }
      else
      {
        sqlite3_mutex_enter(sqlite3_mutex_alloc(SQLITE_MUTEX_STATIC_MASTER));
      }
      value = (hasDefaultPrefix) ? param->m_default : (hasMinPrefix) ? param->m_minValue : (hasMaxPrefix) ? param->m_maxValue : param->m_value;
      if (!hasMinPrefix && !hasMaxPrefix)
      {
        if (newValue >= 0 && newValue >= param->m_minValue && newValue <= param->m_maxValue)
        {
          if (hasDefaultPrefix)
          {
            param->m_default = newValue;
          }
          param->m_value = newValue;
          value = newValue;
        }
        else if (newValue != -1)
        {
          sqlite3_log(SQLITE_WARNING,
                      "sqlite3mc_config_cipher: Value %d for parameter '%s' of cipher '%s' out of range [%d..%d]",
                      newValue, paramName, cipherName, param->m_minValue, param->m_maxValue);
        }
      }
      if (db != NULL)
      {
        sqlite3_mutex_leave(db->mutex);
      }
      else
      {
        sqlite3_mutex_leave(sqlite3_mutex_alloc(SQLITE_MUTEX_STATIC_MASTER));
      }
    }
  }
  return value;
}

SQLITE_API unsigned char*
sqlite3mc_codec_data(sqlite3* db, const char* zDbName, const char* paramName)
{
  unsigned char* result = NULL;
#ifndef SQLITE_OMIT_AUTOINIT
  if (sqlite3_initialize()) return NULL;
#endif
  if (db != NULL && paramName != NULL)
  {
    int iDb = (zDbName != NULL) ? sqlite3FindDbName(db, zDbName) : 0;
    int toRaw = 0;
    if (sqlite3_strnicmp(paramName, "raw:", 4) == 0)
    {
      toRaw = 1;
      paramName += 4;
    }
    if ((sqlite3_stricmp(paramName, "cipher_salt") == 0) && (iDb >= 0))
    {
      /* Check whether database is encrypted */
      Codec* codec = sqlite3mcGetCodec(db, zDbName);
      if (codec != NULL && sqlite3mcIsEncrypted(codec) && sqlite3mcHasWriteCipher(codec))
      {
        unsigned char* salt = sqlite3mcGetSaltWriteCipher(codec);
        if (salt != NULL)
        {
          if (!toRaw)
          {
            int j;
            result = sqlite3_malloc(32 + 1);
            for (j = 0; j < 16; ++j)
            {
              result[j * 2] = hexdigits[(salt[j] >> 4) & 0x0F];
              result[j * 2 + 1] = hexdigits[(salt[j]) & 0x0F];
            }
            result[32] = '\0';
          }
          else
          {
            result = sqlite3_malloc(16 + 1);
            memcpy(result, salt, 16);
            result[16] = '\0';
          }
        }
      }
    }
  }
  return result;
}

SQLITE_PRIVATE void
sqlite3mcCodecDataSql(sqlite3_context* context, int argc, sqlite3_value** argv)
{
  const char* nameParam1 = NULL;
  const char* nameParam2 = NULL;

  assert(argc == 1 || argc == 2);
  /* NULL values are not allowed for the first 2 arguments */
  if (SQLITE_NULL == sqlite3_value_type(argv[0]) || (argc > 1 && SQLITE_NULL == sqlite3_value_type(argv[1])))
  {
    sqlite3_result_null(context);
    return;
  }

  /* Determine parameter name */
  nameParam1 = (const char*) sqlite3_value_text(argv[0]);

  /* Determine schema name if given */
  if (argc == 2)
  {
    nameParam2 = (const char*) sqlite3_value_text(argv[1]);
  }

  /* Check for known parameter name(s) */
  if (sqlite3_stricmp(nameParam1, "cipher_salt") == 0)
  {
    /* Determine key salt */
    sqlite3* db = sqlite3_context_db_handle(context);
    const char* salt = (const char*) sqlite3mc_codec_data(db, nameParam2, "cipher_salt");
    if (salt != NULL)
    {
      sqlite3_result_text(context, salt, -1, sqlite3_free);
    }
    else
    {
      sqlite3_result_null(context);
    }
  }
  else
  {
    sqlite3_result_null(context);
  }
}

SQLITE_PRIVATE void
sqlite3mcConfigParams(sqlite3_context* context, int argc, sqlite3_value** argv)
{
  CodecParameter* codecParams;
  const char* nameParam1;
  int hasDefaultPrefix = 0;
  int hasMinPrefix = 0;
  int hasMaxPrefix = 0;
  CipherParams* param1;
  CipherParams* cipherParamTable = NULL;
  int isCommonParam1;
  int isCipherParam1 = 0;

  assert(argc == 1 || argc == 2 || argc == 3);
  /* NULL values are not allowed for the first 2 arguments */
  if (SQLITE_NULL == sqlite3_value_type(argv[0]) || (argc > 1 && SQLITE_NULL == sqlite3_value_type(argv[1])))
  {
    sqlite3_result_null(context);
    return;
  }

  codecParams = (CodecParameter*)sqlite3_user_data(context);

  /* Check first argument whether it is a common parameter */
  /* If the first argument is a common parameter, param1 will point to its parameter table entry */
  nameParam1 = (const char*)sqlite3_value_text(argv[0]);
  if (sqlite3_strnicmp(nameParam1, "default:", 8) == 0)
  {
    hasDefaultPrefix = 1;
    nameParam1 += 8;
  }
  if (sqlite3_strnicmp(nameParam1, "min:", 4) == 0)
  {
    hasMinPrefix = 1;
    nameParam1 += 4;
  }
  if (sqlite3_strnicmp(nameParam1, "max:", 4) == 0)
  {
    hasMaxPrefix = 1;
    nameParam1 += 4;
  }

  param1 = codecParams[0].m_params;
  cipherParamTable = NULL;
  for (; param1->m_name[0] != 0; ++param1)
  {
    if (sqlite3_stricmp(nameParam1, param1->m_name) == 0) break;
  }
  isCommonParam1 = param1->m_name[0] != 0;

  /* Check first argument whether it is a cipher name, if it wasn't a common parameter */
  /* If the first argument is a cipher name, cipherParamTable will point to the corresponding cipher parameter table */
  if (!isCommonParam1)
  {
    if (!hasDefaultPrefix && !hasMinPrefix && !hasMaxPrefix)
    {
      int j = 0;
      for (j = 0; codecParams[j].m_name[0] != 0; ++j)
      {
        if (sqlite3_stricmp(nameParam1, codecParams[j].m_name) == 0) break;
      }
      isCipherParam1 = codecParams[j].m_name[0] != 0;
      if (isCipherParam1)
      {
        cipherParamTable = codecParams[j].m_params;
      }
    }
    if (!isCipherParam1)
    {
      /* Prefix not allowed for cipher names or cipher name not found */
      sqlite3_result_null(context);
      return;
    }
  }

  if (argc == 1)
  {
    /* Return value of param1 */
    if (isCommonParam1)
    {
      int value = (hasDefaultPrefix) ? param1->m_default : (hasMinPrefix) ? param1->m_minValue : (hasMaxPrefix) ? param1->m_maxValue : param1->m_value;
      if (sqlite3_stricmp(nameParam1, "cipher") == 0)
      {
        sqlite3_result_text(context, globalCodecDescriptorTable[value - 1].m_name, -1, SQLITE_STATIC);
      }
      else
      {
        sqlite3_result_int(context, value);
      }
    }
    else if (isCipherParam1)
    {
      /* Return a list of available parameters for the requested cipher */
      int nParams = 0;
      int lenTotal = 0;
      int j;
      for (j = 0; cipherParamTable[j].m_name[0] != 0; ++j)
      {
        ++nParams;
        lenTotal += (int) strlen(cipherParamTable[j].m_name);
      }
      if (nParams > 0)
      {
        char* paramList = (char*)sqlite3_malloc(lenTotal + nParams);
        if (paramList != NULL)
        {
          char* p = paramList;
          strcpy(paramList, cipherParamTable[0].m_name);
          for (j = 1; j < nParams; ++j)
          {
            strcat(paramList, ",");
            strcat(paramList, cipherParamTable[j].m_name);
          }
          sqlite3_result_text(context, paramList, -1, sqlite3_free);
        }
        else
        {
          /* Not enough memory to allocate the result */
          sqlite3_result_error_nomem(context);
        }
      }
      else
      {
        /* Cipher has no parameters */
        sqlite3_result_null(context);
      }
    }
  }
  else
  {
    /* 2 or more arguments */
    int arg2Type = sqlite3_value_type(argv[1]);
    if (argc == 2 && isCommonParam1)
    {
      /* Set value of common parameter */
      if (sqlite3_stricmp(nameParam1, "cipher") == 0)
      {
        /* 2nd argument is a cipher name */
        if (arg2Type == SQLITE_TEXT)
        {
          const char* nameCipher = (const char*)sqlite3_value_text(argv[1]);
          int j = 0;
          for (j = 0; globalCodecDescriptorTable[j].m_name[0] != 0; ++j)
          {
            if (sqlite3_stricmp(nameCipher, globalCodecDescriptorTable[j].m_name) == 0) break;
          }
          if (globalCodecDescriptorTable[j].m_name[0] != 0)
          {
            if (hasDefaultPrefix)
            {
              param1->m_default = j + 1;
            }
            param1->m_value = j + 1;
            sqlite3_result_text(context, globalCodecDescriptorTable[j].m_name, -1, SQLITE_STATIC);
          }
          else
          {
            /* No match for cipher name found */
            sqlite3_result_null(context);
          }
        }
        else
        {
          /* Invalid parameter type */
          sqlite3_result_null(context);
        }
      }
      else if (arg2Type == SQLITE_INTEGER)
      {
        /* Check that parameter value is within allowed range */
        int value = sqlite3_value_int(argv[1]);
        if (value >= param1->m_minValue && value <= param1->m_maxValue)
        {
          /* Do not allow to change the default value for parameter "hmac_check" */
          if (hasDefaultPrefix && (sqlite3_stricmp(nameParam1, "hmac_check") != 0))
          {
            param1->m_default = value;
          }
          param1->m_value = value;
          sqlite3_result_int(context, value);
        }
        else
        {
          /* Parameter value not within allowed range */
          sqlite3_result_null(context);
        }
      }
      else
      {
        sqlite3_result_null(context);
      }
    }
    else if (isCipherParam1 && arg2Type == SQLITE_TEXT)
    {
      /* get or set cipher parameter */
      const char* nameParam2 = (const char*)sqlite3_value_text(argv[1]);
      CipherParams* param2 = cipherParamTable;
      hasDefaultPrefix = 0;
      if (sqlite3_strnicmp(nameParam2, "default:", 8) == 0)
      {
        hasDefaultPrefix = 1;
        nameParam2 += 8;
      }
      hasMinPrefix = 0;
      if (sqlite3_strnicmp(nameParam2, "min:", 4) == 0)
      {
        hasMinPrefix = 1;
        nameParam2 += 4;
      }
      hasMaxPrefix = 0;
      if (sqlite3_strnicmp(nameParam2, "max:", 4) == 0)
      {
        hasMaxPrefix = 1;
        nameParam2 += 4;
      }
      for (; param2->m_name[0] != 0; ++param2)
      {
        if (sqlite3_stricmp(nameParam2, param2->m_name) == 0) break;
      }

#if HAVE_CIPHER_SQLCIPHER
      /* Special handling for SQLCipher legacy mode */
      if (argc == 3 &&
        sqlite3_stricmp(nameParam1, "sqlcipher") == 0 &&
        sqlite3_stricmp(nameParam2, "legacy") == 0)
      {
        if (!hasMinPrefix && !hasMaxPrefix && sqlite3_value_type(argv[2]) == SQLITE_INTEGER)
        {
          int legacy = sqlite3_value_int(argv[2]);
          if (legacy > 0 && legacy <= SQLCIPHER_VERSION_MAX)
          {
            sqlite3* db = sqlite3_context_db_handle(context);
            sqlite3mcConfigureSQLCipherVersion(db, hasDefaultPrefix, legacy);
          }
        }
      }
#endif

      if (param2->m_name[0] != 0)
      {
        if (argc == 2)
        {
          /* Return parameter value */
          int value = (hasDefaultPrefix) ? param2->m_default : (hasMinPrefix) ? param2->m_minValue : (hasMaxPrefix) ? param2->m_maxValue : param2->m_value;
          sqlite3_result_int(context, value);
        }
        else if (!hasMinPrefix && !hasMaxPrefix && sqlite3_value_type(argv[2]) == SQLITE_INTEGER)
        {
          /* Change cipher parameter value */
          int value = sqlite3_value_int(argv[2]);
          if (value >= param2->m_minValue && value <= param2->m_maxValue)
          {
            if (hasDefaultPrefix)
            {
              param2->m_default = value;
            }
            param2->m_value = value;
            sqlite3_result_int(context, value);
          }
          else
          {
            /* Cipher parameter value not within allowed range */
            sqlite3_result_null(context);
          }
        }
        else
        {
          /* Only current value and default value of a parameter can be changed */
          sqlite3_result_null(context);
        }
      }
      else
      {
        /* Cipher parameter not found */
        sqlite3_result_null(context);
      }
    }
    else
    {
      /* Cipher has no parameters */
      sqlite3_result_null(context);
    }
  }
}

SQLITE_PRIVATE int
sqlite3mcConfigureFromUri(sqlite3* db, const char *zDbName, int configDefault)
{
  int rc = SQLITE_OK;

  /* Check URI parameters if database filename is available */
  const char* dbFileName = zDbName;
  if (dbFileName != NULL)
  {
    /* Check whether cipher is specified */
    const char* cipherName = sqlite3_uri_parameter(dbFileName, "cipher");
    if (cipherName != NULL)
    {
      int j = 0;
      CipherParams* cipherParams = NULL;

      /* Try to locate the cipher name */
      for (j = 1; globalCodecParameterTable[j].m_name[0] != 0; ++j)
      {
        if (sqlite3_stricmp(cipherName, globalCodecParameterTable[j].m_name) == 0) break;
      }

      /* j is the index of the cipher name, if found */
      cipherParams = (globalCodecParameterTable[j].m_name[0] != 0) ? globalCodecParameterTable[j].m_params : NULL;
      if (cipherParams != NULL)
      {
        /*
        ** Flag whether to skip the legacy parameter
        ** Currently enabled only in case of the SQLCipher scheme
        */
        int skipLegacy = 0;
        /* Set global parameters (cipher and hmac_check) */
        int hmacCheck = sqlite3_uri_boolean(dbFileName, "hmac_check", 1);
        int walLegacy = sqlite3_uri_boolean(dbFileName, "mc_legacy_wal", 0);
        if (configDefault)
        {
          sqlite3mc_config(db, "default:cipher", globalCodecParameterTable[j].m_id);
        }
        else
        {
          sqlite3mc_config(db, "cipher", globalCodecParameterTable[j].m_id);
        }
        if (!hmacCheck)
        {
          sqlite3mc_config(db, "hmac_check", hmacCheck);
        }
        sqlite3mc_config(db, "mc_legacy_wal", walLegacy);

#if HAVE_CIPHER_SQLCIPHER
        /* Special handling for SQLCipher */
        if (sqlite3_stricmp(cipherName, "sqlcipher") == 0)
        {
          int legacy = (int) sqlite3_uri_int64(dbFileName, "legacy", 0);
          if (legacy > 0 && legacy <= SQLCIPHER_VERSION_MAX)
          {
            char* param = (configDefault) ? "default:legacy" : "legacy";
            sqlite3mc_config_cipher(db, cipherName, param, legacy);
            skipLegacy = 1;
          }
        }
#endif

        /* Check all cipher specific parameters */
        for (j = 0; cipherParams[j].m_name[0] != 0; ++j)
        {
          if (skipLegacy && sqlite3_stricmp(cipherParams[j].m_name, "legacy") == 0) continue;

          int value = (int) sqlite3_uri_int64(dbFileName, cipherParams[j].m_name, -1);
          if (value >= 0)
          {
            /* Configure cipher parameter if it was given in the URI */
            char* param = (configDefault) ? sqlite3_mprintf("default:%s", cipherParams[j].m_name) : cipherParams[j].m_name;
            sqlite3mc_config_cipher(db, cipherName, param, value);
            if (configDefault)
            {
              sqlite3_free(param);
            }
          }
        }
      }
      else
      {
        rc = SQLITE_ERROR;
        sqlite3ErrorWithMsg(db, rc, "unknown cipher '%s'", cipherName);
      }
    }
  }
  return rc;
}

#ifdef SQLITE3MC_WXSQLITE3_COMPATIBLE
SQLITE_API int
wxsqlite3_config(sqlite3* db, const char* paramName, int newValue)
{
  return sqlite3mc_config(db, paramName, newValue);
}

SQLITE_API int
wxsqlite3_config_cipher(sqlite3* db, const char* cipherName, const char* paramName, int newValue)
{
  return sqlite3mc_config_cipher(db, cipherName, paramName, newValue);
}

SQLITE_API unsigned char*
wxsqlite3_codec_data(sqlite3* db, const char* zDbName, const char* paramName)
{
  return sqlite3mc_codec_data(db, zDbName, paramName);
}
#endif

/*
** Functions called from patched SQLite version
*/

int libsql_extra_pragma(sqlite3* db, const char* zDbName, void* pArg)
{
    int configDefault;
    char* pragmaName;
    char* pragmaValue;
    int dbIndex = (zDbName) ? sqlite3FindDbName(db, zDbName) : 0;
    int rc = SQLITE_NOTFOUND;
    if (dbIndex < 0 && zDbName != NULL)
    {
      /* Unknown schema name */
      return rc;
    }

    configDefault = (dbIndex <= 0);
    pragmaName = ((char**) pArg)[1];
    pragmaValue = ((char**) pArg)[2];
    if (sqlite3StrICmp(pragmaName, "cipher") == 0)
    {
      int cipherId = -1;
      if (pragmaValue != NULL)
      {
        int j = 1;
        /* Try to locate the cipher name */
        for (j = 1; globalCodecParameterTable[j].m_name[0] != 0; ++j)
        {
          if (sqlite3_stricmp(pragmaValue, globalCodecParameterTable[j].m_name) == 0) break;
        }
        cipherId = (globalCodecParameterTable[j].m_name[0] != 0) ? globalCodecParameterTable[j].m_id : CODEC_TYPE_UNKNOWN;
      }

      /* cipherId is the numeric id of the cipher name, if found */
      if ((cipherId == -1) || (cipherId > 0 && cipherId <= CODEC_COUNT_MAX))
      {
        int value;
        if (configDefault)
        {
          value = sqlite3mc_config(db, "default:cipher", cipherId);
        }
        else
        {
          value = sqlite3mc_config(db, "cipher", cipherId);
        }
        rc = SQLITE_OK;
        ((char**)pArg)[0] = sqlite3_mprintf("%s", globalCodecDescriptorTable[value - 1].m_name);
      }
      else
      {
        ((char**) pArg)[0] = sqlite3_mprintf("Cipher '%s' unknown.", pragmaValue);
        rc = SQLITE_ERROR;
      }
    }
    else if (sqlite3StrICmp(pragmaName, "hmac_check") == 0)
    {
      int hmacCheck = (pragmaValue != NULL) ? sqlite3GetBoolean(pragmaValue, 1) : -1;
      int value = sqlite3mc_config(db, "hmac_check", hmacCheck);
      ((char**)pArg)[0] = sqlite3_mprintf("%d", value);
      rc = SQLITE_OK;
    }
    else if (sqlite3StrICmp(pragmaName, "mc_legacy_wal") == 0)
    {
      int walLegacy = (pragmaValue != NULL) ? sqlite3GetBoolean(pragmaValue, 0) : -1;
      int value = sqlite3mc_config(db, "mc_legacy_wal", walLegacy);
      ((char**)pArg)[0] = sqlite3_mprintf("%d", value);
      rc = SQLITE_OK;
    }
    else if (sqlite3StrICmp(pragmaName, "key") == 0)
    {
      rc = sqlite3_key_v2(db, zDbName, pragmaValue, -1);
      if (rc == SQLITE_OK)
      {
        ((char**)pArg)[0] = sqlite3_mprintf("ok");
      }
      else
      {
        if (db->pErr)
        {
          const char* z = (const char*)sqlite3_value_text(db->pErr);
          if (z && sqlite3Strlen30(z) > 0)
          {
            ((char**)pArg)[0] = sqlite3_mprintf(z);
          }
        }
      }
    }
    else if (sqlite3StrICmp(pragmaName, "hexkey") == 0)
    {
      int nValue = sqlite3Strlen30(pragmaValue);
      if (((nValue & 1) == 0) && (sqlite3mcIsHexKey((const unsigned char*) pragmaValue, nValue) != 0))
      {
        unsigned char* zHexKey = sqlite3_malloc(nValue/2);
        sqlite3mcConvertHex2Bin((const unsigned char*) pragmaValue, nValue, zHexKey);
        rc = sqlite3_key_v2(db, zDbName, zHexKey, nValue/2);
        sqlite3_free(zHexKey);
        if (rc == SQLITE_OK)
        {
          ((char**)pArg)[0] = sqlite3_mprintf("ok");
        }
        else
        {
          if (db->pErr)
          {
            const char* z = (const char*)sqlite3_value_text(db->pErr);
            if (z && sqlite3Strlen30(z) > 0)
            {
              ((char**)pArg)[0] = sqlite3_mprintf(z);
            }
          }
        }
      }
      else
      {
        rc = SQLITE_ERROR;
        ((char**)pArg)[0] = sqlite3_mprintf("Malformed hex string");
      }
    }
    else if (sqlite3StrICmp(pragmaName, "rekey") == 0)
    {
      rc = sqlite3_rekey_v2(db, zDbName, pragmaValue, -1);
      if (rc == SQLITE_OK)
      {
        ((char**)pArg)[0] = sqlite3_mprintf("ok");
      }
      else
      {
        if (db->pErr)
        {
          const char* z = (const char*) sqlite3_value_text(db->pErr);
          if (z && sqlite3Strlen30(z) > 0)
          {
            ((char**)pArg)[0] = sqlite3_mprintf(z);
          }
        }
      }
    }
    else if (sqlite3StrICmp(pragmaName, "hexrekey") == 0)
    {
      int nValue = sqlite3Strlen30(pragmaValue);
      if (((nValue & 1) == 0) && (sqlite3mcIsHexKey((const unsigned char*) pragmaValue, nValue) != 0))
      {
        unsigned char* zHexKey = sqlite3_malloc(nValue/2);
        sqlite3mcConvertHex2Bin((const unsigned char*) pragmaValue, nValue, zHexKey);
        rc = sqlite3_rekey_v2(db, zDbName, zHexKey, nValue/2);
        sqlite3_free(zHexKey);
        if (rc == SQLITE_OK)
        {
          ((char**)pArg)[0] = sqlite3_mprintf("ok");
        }
        else
        {
          if (db->pErr)
          {
            const char* z = (const char*)sqlite3_value_text(db->pErr);
            if (z && sqlite3Strlen30(z) > 0)
            {
              ((char**)pArg)[0] = sqlite3_mprintf(z);
            }
          }
        }
      }
      else
      {
        rc = SQLITE_ERROR;
        ((char**)pArg)[0] = sqlite3_mprintf("Malformed hex string");
      }
    }
#if SQLITE3MC_SECURE_MEMORY
    else if (sqlite3StrICmp(pragmaName, "memory_security") == 0)
    {
      if (pragmaValue)
      {
        int intValue = -1;
        if (0 == sqlite3StrICmp(pragmaValue, "none"))
        {
          intValue = SECURE_MEMORY_NONE;
        }
        else if (0 == sqlite3StrICmp(pragmaValue, "fill") )
        {
          intValue = SECURE_MEMORY_FILL;
        }
#if SQLITE3MC_ENABLE_MEMLOCK
        else if (0 == sqlite3StrICmp(pragmaValue, "lock") )
        {
          intValue = SECURE_MEMORY_LOCK;
        }
#endif
        else
        {
          intValue = sqlite3Atoi(pragmaValue);
#if SQLITE3MC_ENABLE_MEMLOCK
          intValue = (intValue >=0 && intValue <= 2) ? intValue : -1;
#else
          intValue = (intValue >=0 && intValue <= 1) ? intValue : -1;
#endif
        }
        if (intValue >= 0)
        {
          sqlite3mcSetMemorySecurity(intValue);
          rc = SQLITE_OK;
          ((char**)pArg)[0] = sqlite3_mprintf("%d", intValue);
        }
        else
        {
          rc = SQLITE_ERROR;
          ((char**) pArg)[0] = sqlite3_mprintf("Secure memory option '%s' invalid.", pragmaValue);
        }
      }
      else
      {
        rc = SQLITE_OK;
        ((char**)pArg)[0] = sqlite3_mprintf("%d", sqlite3mcGetMemorySecurity());
      }
    }
#endif /* SQLITE3MC_SECURE_MEMORY */
    else
    {
      int j;
      int intValue = (pragmaValue != NULL) ? 0 : -1;
      int isIntValue = (pragmaValue != NULL) ? (sqlite3GetInt32(pragmaValue, &intValue) != 0) : 1;

      /* Determine cipher */
      int cipher = sqlite3mc_config(db, "cipher", -1);
      CipherParams* cipherParams = NULL;

      /* Try to locate the cipher name */
      for (j = 1; globalCodecParameterTable[j].m_name[0] != 0; ++j)
      {
        if (cipher == globalCodecParameterTable[j].m_id) break;
      }

      /* j is the index of the cipher name, if found */
      cipherParams = (globalCodecParameterTable[j].m_name[0] != 0) ? globalCodecParameterTable[j].m_params : NULL;
      if (cipherParams != NULL)
      {
        const char* cipherName = globalCodecParameterTable[j].m_name;
        for (j = 0; cipherParams[j].m_name[0] != 0; ++j)
        {
          if (sqlite3_stricmp(pragmaName, cipherParams[j].m_name) == 0) break;
        }
        if (cipherParams[j].m_name[0] != 0)
        {
          char* param = (configDefault) ? sqlite3_mprintf("default:%s", pragmaName) : pragmaName;
          if (isIntValue)
          {
            int value = sqlite3mc_config_cipher(db, cipherName, param, intValue);
            ((char**)pArg)[0] = sqlite3_mprintf("%d", value);
            rc = SQLITE_OK;
          }
          else
          {
            ((char**) pArg)[0] = sqlite3_mprintf("Malformed integer value '%s'.", pragmaValue);
            rc = SQLITE_ERROR;
          }
          if (configDefault)
          {
            sqlite3_free(param);
          }
        }
      }
    }
  return rc;
}

/*
** Process URI filename query parameters relevant to the SQLite Encryption
** Extension.  Return true if any of the relevant query parameters are
** seen and return false if not.
*/
SQLITE_PRIVATE int
sqlite3mcCodecQueryParameters(sqlite3* db, const char* zDb, const char* zUri)
{
  int rc = 1;
  const char* zKey;
  if ((zKey = sqlite3_uri_parameter(zUri, "hexkey")) != 0 && zKey[0])
  {
    u8 iByte;
    int i;
    int nKey = sqlite3Strlen30(zKey);
    char* zDecoded = sqlite3_malloc(nKey);
    for (i = 0, iByte = 0; i < nKey && sqlite3Isxdigit(zKey[i]); i++)
    {
      iByte = (iByte << 4) + sqlite3HexToInt(zKey[i]);
      if ((i & 1) != 0) zDecoded[i/2] = iByte;
    }
    sqlite3_key_v2(db, zDb, zDecoded, i/2);
    sqlite3_free(zDecoded);
  }
  else if ((zKey = sqlite3_uri_parameter(zUri, "key")) != 0)
  {
    sqlite3_key_v2(db, zDb, zKey, sqlite3Strlen30(zKey));
  }
  else if ((zKey = sqlite3_uri_parameter(zUri, "textkey")) != 0)
  {
    sqlite3_key_v2(db, zDb, zKey, -1);
  }
  else
  {
    rc = 0;
  }
  return rc;
}

SQLITE_PRIVATE int
sqlite3mcHandleAttachKey(sqlite3* db, const char* zName, const char* zPath, sqlite3_value* pKey, char** zErrDyn)
{
  int rc = SQLITE_OK;
  int nKey;
  char* zKey;
  int keyType = sqlite3_value_type(pKey);
  switch (keyType)
  {
    case SQLITE_INTEGER:
    case SQLITE_FLOAT:
      /* Invalid data type for key */
      *zErrDyn = sqlite3DbStrDup(db, "Invalid key value");
      rc = SQLITE_ERROR;
      break;

    case SQLITE_TEXT:
    case SQLITE_BLOB:
      /* Key parameter specified in ATTACH statement */
      nKey = sqlite3_value_bytes(pKey);
      zKey = (char*) sqlite3_value_blob(pKey);
      rc = sqlite3mcCodecAttach(db, db->nDb - 1, zPath, zKey, nKey);
      break;

    case SQLITE_NULL:
      /* No key specified.  Use the key from URI filename, or if none,
      ** use the key from the main database. */
      if (sqlite3mcCodecQueryParameters(db, zName, zPath) == 0)
      {
        sqlite3mcCodecGetKey(db, 0, (void**) &zKey, &nKey);
        if (nKey)
        {
          rc = sqlite3mcCodecAttach(db, db->nDb - 1, zPath, zKey, nKey);
        }
      }
      break;
  }

  return rc;
}

SQLITE_PRIVATE int
sqlite3mcHandleMainKey(sqlite3* db, const char* zPath)
{
  int rc = sqlite3mcConfigureFromUri(db, zPath, 1);
  if (rc == SQLITE_OK)
  {
    sqlite3mcCodecQueryParameters(db, "main", zPath);
  }
  return rc;
}

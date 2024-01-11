/*
** Name:        codecext.c
** Purpose:     Implementation of SQLite codec API
** Author:      Ulrich Telle
** Created:     2006-12-06
** Copyright:   (c) 2006-2022 Ulrich Telle
** License:     MIT
*/

/*
** "Special" version of function sqlite3BtreeSetPageSize
** This version allows to reduce the number of reserved bytes per page,
** while the original version allows only to increase it.
** Needed to reclaim reserved space on decrypting a database.
*/
SQLITE_PRIVATE int
sqlite3mcBtreeSetPageSize(Btree* p, int pageSize, int nReserve, int iFix)
{
  int rc = SQLITE_OK;
  int x;
  BtShared* pBt = p->pBt;
  assert(nReserve >= 0 && nReserve <= 255);
  sqlite3BtreeEnter(p);
  pBt->nReserveWanted = nReserve;
  x = pBt->pageSize - pBt->usableSize;
  if (nReserve < 0) nReserve = x;
  if (pBt->btsFlags & BTS_PAGESIZE_FIXED)
  {
    sqlite3BtreeLeave(p);
    return SQLITE_READONLY;
  }
  assert(nReserve >= 0 && nReserve <= 255);
  if (pageSize >= 512 && pageSize <= SQLITE_MAX_PAGE_SIZE &&
    ((pageSize - 1) & pageSize) == 0)
  {
    assert((pageSize & 7) == 0);
    assert(!pBt->pCursor);
    pBt->pageSize = (u32)pageSize;
    freeTempSpace(pBt);
  }
  rc = sqlite3PagerSetPagesize(pBt->pPager, &pBt->pageSize, nReserve);
  pBt->usableSize = pBt->pageSize - (u16)nReserve;
  if (iFix) pBt->btsFlags |= BTS_PAGESIZE_FIXED;
  sqlite3BtreeLeave(p);
  return rc;
}

/*
** Include a "special" version of the VACUUM command
*/
#include "rekeyvacuum.c"

#include "cipher_common.h"

SQLITE_API void
sqlite3_activate_see(const char *info)
{
}

/*
** Free the encryption data structure associated with a pager instance.
** (called from the modified code in pager.c) 
*/
SQLITE_PRIVATE void
sqlite3mcCodecFree(void *pCodecArg)
{
  if (pCodecArg)
  {
    sqlite3mcCodecTerm(pCodecArg);
    sqlite3_free(pCodecArg);
    pCodecArg = NULL;
  }
}

SQLITE_PRIVATE void
sqlite3mcCodecSizeChange(void *pArg, int pageSize, int reservedSize)
{
  Codec* pCodec = (Codec*) pArg;
  pCodec->m_pageSize = pageSize;
  pCodec->m_reserved = reservedSize;
}

static void
mcReportCodecError(BtShared* pBt, int error)
{
  pBt->db->errCode = error;
  pBt->pPager->errCode = error;
  if (error != SQLITE_OK)
  {
    pBt->pPager->eState = PAGER_ERROR;
  }
  setGetterMethod(pBt->pPager);
  if (error == SQLITE_OK)
  {
    /* Clear cache to force reread of database after a new passphrase has been set */
    sqlite3PagerClearCache(pBt->pPager);
  }
}

/*
// Encrypt/Decrypt functionality, called by pager.c
*/
SQLITE_PRIVATE void*
sqlite3mcCodec(void* pCodecArg, void* data, Pgno nPageNum, int nMode)
{
  int rc = SQLITE_OK;
  Codec* codec = NULL;
  int pageSize;
  if (pCodecArg == NULL)
  {
    return data;
  }
  codec = (Codec*) pCodecArg;
  if (!sqlite3mcIsEncrypted(codec))
  {
    return data;
  }

  pageSize = sqlite3mcGetPageSize(codec);

  switch(nMode)
  {
    case 0: /* Undo a "case 7" journal file encryption */
    case 2: /* Reload a page */
    case 3: /* Load a page */
      if (sqlite3mcHasReadCipher(codec))
      {
        rc = sqlite3mcDecrypt(codec, nPageNum, (unsigned char*) data, pageSize);
        if (rc != SQLITE_OK) mcReportCodecError(sqlite3mcGetBtShared(codec), rc);
      }
      break;

    case 6: /* Encrypt a page for the main database file */
      if (sqlite3mcHasWriteCipher(codec))
      {
        unsigned char* pageBuffer = sqlite3mcGetPageBuffer(codec);
        memcpy(pageBuffer, data, pageSize);
        data = pageBuffer;
        rc = sqlite3mcEncrypt(codec, nPageNum, (unsigned char*) data, pageSize, 1);
        if (rc != SQLITE_OK) mcReportCodecError(sqlite3mcGetBtShared(codec), rc);
      }
      break;

    case 7: /* Encrypt a page for the journal file */
      /* Under normal circumstances, the readkey is the same as the writekey.  However,
         when the database is being rekeyed, the readkey is not the same as the writekey.
         The rollback journal must be written using the original key for the
         database file because it is, by nature, a rollback journal.
         Therefore, for case 7, when the rollback is being written, always encrypt using
         the database's readkey, which is guaranteed to be the same key that was used to
         read the original data.
      */
      if (sqlite3mcHasReadCipher(codec))
      {
        unsigned char* pageBuffer = sqlite3mcGetPageBuffer(codec);
        memcpy(pageBuffer, data, pageSize);
        data = pageBuffer;
        rc = sqlite3mcEncrypt(codec, nPageNum, (unsigned char*) data, pageSize, 0);
        if (rc != SQLITE_OK) mcReportCodecError(sqlite3mcGetBtShared(codec), rc);
      }
      break;
  }
  return data;
}

SQLITE_PRIVATE Codec*
sqlite3mcGetMainCodec(sqlite3* db);

SQLITE_PRIVATE void
sqlite3mcSetCodec(sqlite3* db, const char* zDbName, const char* zFileName, Codec* codec);

static int
mcAdjustBtree(Btree* pBt, int nPageSize, int nReserved, int isLegacy)
{
  int rc = SQLITE_OK;
  Pager* pager = sqlite3BtreePager(pBt);
  int pagesize = sqlite3BtreeGetPageSize(pBt);
  sqlite3BtreeSecureDelete(pBt, 1);
  if (nPageSize > 0)
  {
    pagesize = nPageSize;
  }

  /* Adjust the page size and the reserved area */
  if (pager->pageSize != pagesize || pager->nReserve != nReserved)
  {
    if (isLegacy != 0)
    {
      pBt->pBt->btsFlags &= ~BTS_PAGESIZE_FIXED;
    }
    rc = sqlite3BtreeSetPageSize(pBt, pagesize, nReserved, 0);
  }
  return rc;
}

static int
sqlite3mcCodecAttach(sqlite3* db, int nDb, const char* zPath, const void* zKey, int nKey)
{
  /* Attach a key to a database. */
  const char* zDbName = db->aDb[nDb].zDbSName;
  const char* dbFileName = sqlite3_db_filename(db, zDbName);
  Codec* codec = (Codec*) sqlite3_malloc(sizeof(Codec));
  int rc = (codec != NULL) ? sqlite3mcCodecInit(codec) : SQLITE_NOMEM;
  if (rc != SQLITE_OK)
  {
    /* Unable to allocate memory for the codec base structure */
    return rc;
  }

  sqlite3_mutex_enter(db->mutex);
  sqlite3mcSetDb(codec, db);

  /* No key specified, could mean either use the main db's encryption or no encryption */
  if (zKey == NULL || nKey <= 0)
  {
    /* No key specified */
    if (nDb != 0 && nKey > 0)
    {
      /* Main database possibly encrypted, no key explicitly given for attached database */
      Codec* mainCodec = sqlite3mcGetMainCodec(db);
      /* Attached database, therefore use the key of main database, if main database is encrypted */
      if (mainCodec != NULL && sqlite3mcIsEncrypted(mainCodec))
      {
        rc = sqlite3mcCodecCopy(codec, mainCodec);
        if (rc == SQLITE_OK)
        {
          int pageSize = sqlite3mcGetPageSizeWriteCipher(codec);
          int reserved = sqlite3mcGetReservedWriteCipher(codec);
          sqlite3mcSetBtree(codec, db->aDb[nDb].pBt);
          mcAdjustBtree(db->aDb[nDb].pBt, pageSize, reserved, sqlite3mcGetLegacyWriteCipher(codec));
          sqlite3mcCodecSizeChange(codec, pageSize, reserved);
          sqlite3mcSetCodec(db, zDbName, dbFileName, codec);
        }
        else
        {
          /* Replicating main codec failed, do not attach incomplete codec */
          sqlite3mcCodecFree(codec);
        }
      }
      else
      {
        /* Main database not encrypted */
        sqlite3mcCodecFree(codec);
      }
    }
    else
    {
      /* Main database not encrypted, no key given for attached database */
      sqlite3mcCodecFree(codec);
      /* Remove codec for main database */
      if (nDb == 0 && nKey == 0)
      {
        sqlite3mcSetCodec(db, zDbName, dbFileName, NULL);
      }
    }
  }
  else
  {
    if (dbFileName != NULL)
    {
      /* Check whether key salt is provided in URI */
      const unsigned char* cipherSalt = (const unsigned char*)sqlite3_uri_parameter(dbFileName, "cipher_salt");
      if ((cipherSalt != NULL) && (strlen((const char*)cipherSalt) >= 2 * KEYSALT_LENGTH) && sqlite3mcIsHexKey(cipherSalt, 2 * KEYSALT_LENGTH))
      {
        codec->m_hasKeySalt = 1;
        sqlite3mcConvertHex2Bin(cipherSalt, 2 * KEYSALT_LENGTH, codec->m_keySalt);
      }
    }

    /* Configure cipher from URI in case of attached database */
    if (nDb > 0)
    {
      rc = sqlite3mcConfigureFromUri(db, dbFileName, 0);
    }
    if (rc == SQLITE_OK)
    {
      /* Key specified, setup encryption key for database */
      sqlite3mcSetBtree(codec, db->aDb[nDb].pBt);
      rc = sqlite3mcCodecSetup(codec, sqlite3mcGetCipherType(db), (char*) zKey, nKey);
      sqlite3mcClearKeySalt(codec);
    }
    if (rc == SQLITE_OK)
    {
      int pageSize = sqlite3mcGetPageSizeWriteCipher(codec);
      int reserved = sqlite3mcGetReservedWriteCipher(codec);
      mcAdjustBtree(db->aDb[nDb].pBt, pageSize, reserved, sqlite3mcGetLegacyWriteCipher(codec));
      sqlite3mcCodecSizeChange(codec, pageSize, reserved);
      sqlite3mcSetCodec(db, zDbName, dbFileName, codec);
    }
    else
    {
      /* Setting up codec failed, do not attach incomplete codec */
      sqlite3mcCodecFree(codec);
    }
  }

  sqlite3_mutex_leave(db->mutex);

  return rc;
}

SQLITE_PRIVATE void
sqlite3mcCodecGetKey(sqlite3* db, int nDb, void** zKey, int* nKey)
{
  /*
  ** The unencrypted password is not stored for security reasons
  ** therefore always return NULL
  ** If the main database is encrypted a key length of 1 is returned.
  ** In that case an attached database will get the same encryption key
  ** as the main database if no key was explicitly given for the attached database.
  */
  Codec* codec = sqlite3mcGetCodec(db, db->aDb[nDb].zDbSName);
  int keylen = (codec != NULL && sqlite3mcIsEncrypted(codec)) ? 1 : 0;
  *zKey = NULL;
  *nKey = keylen;
}

SQLITE_API int
sqlite3_key(sqlite3 *db, const void *zKey, int nKey)
{
  /* The key is only set for the main database, not the temp database  */
  return sqlite3_key_v2(db, "main", zKey, nKey);
}

SQLITE_API int
sqlite3_key_v2(sqlite3* db, const char* zDbName, const void* zKey, int nKey)
{
  int rc = SQLITE_ERROR;
  if (zKey != NULL && nKey < 0)
  {
    /* Key is zero-terminated string */
    nKey = sqlite3Strlen30((const char*) zKey);
  }
  /* Database handle db and key must be given, but key length 0 is allowed */
  if ((db != NULL) && (zKey != NULL) && (nKey >= 0))
  {
    int dbIndex;
    const char* dbFileName = sqlite3_db_filename(db, zDbName);
    if (dbFileName == NULL || dbFileName[0] == 0)
    {
      sqlite3ErrorWithMsg(db, rc, "Setting key not supported for in-memory or temporary databases.");
      return rc;
    }
    /* Configure cipher from URI parameters if requested */
    if (sqlite3FindFunction(db, "sqlite3mc_config_table", 0, SQLITE_UTF8, 0) == NULL)
    {
      /*
      ** Encryption extension of database connection not yet initialized;
      ** that is, sqlite3_key_v2 was called from the internal open function.
      ** Therefore the URI should be checked for encryption configuration parameters.
      */
      rc = sqlite3mcConfigureFromUri(db, dbFileName, 0);
    }

    /* The key is only set for the main database, not the temp database  */
    dbIndex = (zDbName) ? sqlite3FindDbName(db, zDbName) : 0;
    if (dbIndex >= 0)
    {
      rc = sqlite3mcCodecAttach(db, dbIndex, dbFileName, zKey, nKey);
    }
    else
    {
      rc = SQLITE_ERROR;
      sqlite3ErrorWithMsg(db, rc, "Setting key failed. Database '%s' not found.", zDbName);
    }
  }
  return rc;
}

SQLITE_API int
sqlite3_rekey_v2(sqlite3* db, const char* zDbName, const void* zKey, int nKey)
{
  /* Changes the encryption key for an existing database. */
  const char* dbFileName;
  int dbIndex;
  Btree* pBt;
  int nPagesize;
  int nReserved;
  Pager* pPager;
  Codec* codec;
  int rc = SQLITE_ERROR;
  if (zKey != NULL && nKey < 0)
  {
    /* Key is zero-terminated string */
    nKey = sqlite3Strlen30((const char*) zKey);
  }
  dbFileName = sqlite3_db_filename(db, zDbName);
  dbIndex = (zDbName) ? sqlite3FindDbName(db, zDbName) : 0;
  if (dbIndex < 0)
  {
    sqlite3ErrorWithMsg(db, rc, "Rekeying failed. Database '%s' not found.", zDbName);
    return rc;
  }
  if (dbFileName == NULL || dbFileName[0] == 0)
  {
    sqlite3ErrorWithMsg(db, rc, "Rekeying not supported for in-memory or temporary databases.");
    return rc;
  }
  pBt = db->aDb[dbIndex].pBt;
  nPagesize = sqlite3BtreeGetPageSize(pBt);

  sqlite3BtreeEnter(pBt);
  nReserved = sqlite3BtreeGetReserveNoMutex(pBt);
  sqlite3BtreeLeave(pBt);

  pPager = sqlite3BtreePager(pBt);
  codec = sqlite3mcGetCodec(db, zDbName);

  if (pagerUseWal(pPager))
  {
    sqlite3ErrorWithMsg(db, rc, "Rekeying is not supported in WAL journal mode.");
    return rc;
  }
  
  if ((zKey == NULL || nKey == 0) && (codec == NULL || !sqlite3mcIsEncrypted(codec)))
  {
    /* Database not encrypted and key not specified, therefore do nothing	*/
    return SQLITE_OK;
  }

  sqlite3_mutex_enter(db->mutex);

  if (codec == NULL || !sqlite3mcIsEncrypted(codec))
  {
    /* Database not encrypted, but key specified, therefore encrypt database	*/
    if (codec == NULL)
    {
      codec = (Codec*) sqlite3_malloc(sizeof(Codec));
      rc = (codec != NULL) ? sqlite3mcCodecInit(codec) : SQLITE_NOMEM;
    }
    if (rc == SQLITE_OK)
    {
      sqlite3mcSetDb(codec, db);
      sqlite3mcSetBtree(codec, pBt);
      rc = sqlite3mcSetupWriteCipher(codec, sqlite3mcGetCipherType(db), (char*) zKey, nKey);
    }
    if (rc == SQLITE_OK)
    {
      int nPagesizeWriteCipher = sqlite3mcGetPageSizeWriteCipher(codec);
      if (nPagesizeWriteCipher <= 0 || nPagesize == nPagesizeWriteCipher)
      {
        int nReservedWriteCipher;
        sqlite3mcSetHasReadCipher(codec, 0); /* Original database is not encrypted */
        mcAdjustBtree(pBt, sqlite3mcGetPageSizeWriteCipher(codec), sqlite3mcGetReservedWriteCipher(codec), sqlite3mcGetLegacyWriteCipher(codec));
        sqlite3mcSetCodec(db, zDbName, dbFileName, codec);
        nReservedWriteCipher = sqlite3mcGetReservedWriteCipher(codec);
        sqlite3mcCodecSizeChange(codec, nPagesize, nReservedWriteCipher);
        if (nReserved != nReservedWriteCipher)
        {
          /* Use VACUUM to change the number of reserved bytes */
          char* err = NULL;
          sqlite3mcSetReadReserved(codec, nReserved);
          sqlite3mcSetWriteReserved(codec, nReservedWriteCipher);
          rc = sqlite3mcRunVacuumForRekey(&err, db, dbIndex, NULL, nReservedWriteCipher);
          if (rc != SQLITE_OK && err != NULL)
          {
            sqlite3ErrorWithMsg(db, rc, err);
          }
          goto leave_rekey;
        }
      }
      else
      {
        /* Pagesize cannot be changed for an encrypted database */
        rc = SQLITE_ERROR;
        sqlite3ErrorWithMsg(db, rc, "Rekeying failed. Pagesize cannot be changed for an encrypted database.");
        goto leave_rekey;
      }
    }
    else
    {
      return rc;
    }
  }
  else if (zKey == NULL || nKey == 0)
  {
    /* Database encrypted, but key not specified, therefore decrypt database */
    /* Keep read key, drop write key */
    sqlite3mcSetHasWriteCipher(codec, 0);
    if (nReserved > 0)
    {
      /* Use VACUUM to change the number of reserved bytes */
      char* err = NULL;
      sqlite3mcSetReadReserved(codec, nReserved);
      sqlite3mcSetWriteReserved(codec, 0);
      rc = sqlite3mcRunVacuumForRekey(&err, db, dbIndex, NULL, 0);
      if (rc != SQLITE_OK && err != NULL)
      {
        sqlite3ErrorWithMsg(db, rc, err);
      }
      goto leave_rekey;
    }
  }
  else
  {
    /* Database encrypted and key specified, therefore re-encrypt database with new key */
    /* Keep read key, change write key to new key */
    rc = sqlite3mcSetupWriteCipher(codec, sqlite3mcGetCipherType(db), (char*) zKey, nKey);
    if (rc == SQLITE_OK)
    {
      int nPagesizeWriteCipher = sqlite3mcGetPageSizeWriteCipher(codec);
      if (nPagesizeWriteCipher <= 0 || nPagesize == nPagesizeWriteCipher)
      {
        int nReservedWriteCipher = sqlite3mcGetReservedWriteCipher(codec);
        if (nReserved != nReservedWriteCipher)
        {
          /* Use VACUUM to change the number of reserved bytes */
          char* err = NULL;
          sqlite3mcSetReadReserved(codec, nReserved);
          sqlite3mcSetWriteReserved(codec, nReservedWriteCipher);
          rc = sqlite3mcRunVacuumForRekey(&err, db, dbIndex, NULL, nReservedWriteCipher);
          if (rc != SQLITE_OK && err != NULL)
          {
            sqlite3ErrorWithMsg(db, rc, err);
          }
          goto leave_rekey;
        }
      }
      else
      {
        /* Pagesize cannot be changed for an encrypted database */
        rc = SQLITE_ERROR;
        sqlite3ErrorWithMsg(db, rc, "Rekeying failed. Pagesize cannot be changed for an encrypted database.");
        goto leave_rekey;
      }
    }
    else
    {
      /* Setup of write cipher failed */
      sqlite3ErrorWithMsg(db, rc, "Rekeying failed. Setup of write cipher failed.");
      goto leave_rekey;
    }
  }

  /* Start transaction */
  rc = sqlite3BtreeBeginTrans(pBt, 1, 0);
  if (!rc)
  {
    int pageSize = sqlite3BtreeGetPageSize(pBt);
    Pgno nSkip = WX_PAGER_MJ_PGNO(pageSize);
    DbPage *pPage;
    Pgno n;
    /* Rewrite all pages using the new encryption key (if specified) */
    Pgno nPage;
    int nPageCount = -1;
    sqlite3PagerPagecount(pPager, &nPageCount);
    nPage = nPageCount;

    for (n = 1; rc == SQLITE_OK && n <= nPage; n++)
    {
      if (n == nSkip) continue;
      rc = sqlite3PagerGet(pPager, n, &pPage, 0);
      if (!rc)
      {
        rc = sqlite3PagerWrite(pPage);
        sqlite3PagerUnref(pPage);
      }
    }
  }

  if (rc == SQLITE_OK)
  {
    /* Commit transaction if all pages could be rewritten */
    rc = sqlite3BtreeCommit(pBt);
  }
  if (rc != SQLITE_OK)
  {
    /* Rollback in case of error */
    sqlite3BtreeRollback(pBt, SQLITE_OK, 0);
  }

leave_rekey:
  sqlite3_mutex_leave(db->mutex);

/*leave_final:*/
  if (rc == SQLITE_OK)
  {
    /* Set read key equal to write key if necessary */
    if (sqlite3mcHasWriteCipher(codec))
    {
      sqlite3mcCopyCipher(codec, 0);
      sqlite3mcSetHasReadCipher(codec, 1);
    }
    else
    {
      sqlite3mcSetIsEncrypted(codec, 0);
    }
  }
  else
  {
    /* Restore write key if necessary */
    if (sqlite3mcHasReadCipher(codec))
    {
      sqlite3mcCopyCipher(codec, 1);
    }
    else
    {
      sqlite3mcSetIsEncrypted(codec, 0);
    }
  }
  /* Reset reserved for read and write key */
  sqlite3mcSetReadReserved(codec, -1);
  sqlite3mcSetWriteReserved(codec, -1);

  if (!sqlite3mcIsEncrypted(codec))
  {
    /* Remove codec for unencrypted database */
    sqlite3mcSetCodec(db, zDbName, dbFileName, NULL);
  }
  return rc;
}

SQLITE_API int
sqlite3_rekey(sqlite3 *db, const void *zKey, int nKey)
{
  return sqlite3_rekey_v2(db, "main", zKey, nKey);
}

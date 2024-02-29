/*
** Name:        sqlite3mc_vfs.c
** Purpose:     Implementation of SQLite VFS for Multiple Ciphers
** Author:      Ulrich Telle
** Created:     2020-02-28
** Copyright:   (c) 2020-2023 Ulrich Telle
** License:     MIT
*/

#include "sqlite3mc_vfs.h"
#include "sqlite3.h"
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include "mystdint.h"

/*
** Type definitions
*/

typedef struct sqlite3mc_file sqlite3mc_file;
typedef struct sqlite3mc_vfs sqlite3mc_vfs;

/*
** SQLite3 Multiple Ciphers file structure
*/

struct sqlite3mc_file
{
  sqlite3_file base;           /* sqlite3_file I/O methods */
  sqlite3_file* pFile;         /* Real underlying OS file */
  sqlite3mc_vfs* pVfsMC;       /* Pointer to the sqlite3mc_vfs object */
  const char* zFileName;       /* File name */
  int openFlags;               /* Open flags */
  sqlite3mc_file* pMainNext;   /* Next main db file */
  sqlite3mc_file* pMainDb;     /* Main database to which this one is attached */
  Codec* codec;                /* Codec if encrypted */
  int pageNo;                  /* Page number (in case of journal files) */
};

/*
** SQLite3 Multiple Ciphers VFS structure
*/

struct sqlite3mc_vfs
{
  sqlite3_vfs base;      /* Multiple Ciphers VFS shim methods */
  sqlite3_mutex* mutex;  /* Mutex to protect pMain */
  sqlite3mc_file* pMain; /* List of main database files */
};

#define REALVFS(p) ((sqlite3_vfs*)(((sqlite3mc_vfs*)(p))->base.pAppData))
#define REALFILE(p) (((sqlite3mc_file*)(p))->pFile)

/*
** Prototypes for VFS methods
*/

static int mcVfsOpen(sqlite3_vfs* pVfs, const char* zName, sqlite3_file* pFile, int flags, int* pOutFlags);
static int mcVfsDelete(sqlite3_vfs* pVfs, const char* zName, int syncDir);
static int mcVfsAccess(sqlite3_vfs* pVfs, const char* zName, int flags, int* pResOut);
static int mcVfsFullPathname(sqlite3_vfs* pVfs, const char* zName, int nOut, char* zOut);
static void* mcVfsDlOpen(sqlite3_vfs* pVfs, const char* zFilename);
static void mcVfsDlError(sqlite3_vfs* pVfs, int nByte, char* zErrMsg);
static void (*mcVfsDlSym(sqlite3_vfs* pVfs, void* p, const char* zSymbol))(void);
static void mcVfsDlClose(sqlite3_vfs* pVfs, void* p);
static int mcVfsRandomness(sqlite3_vfs* pVfs, int nByte, char* zOut);
static int mcVfsSleep(sqlite3_vfs* pVfs, int microseconds);
static int mcVfsCurrentTime(sqlite3_vfs* pVfs, double* pOut);
static int mcVfsGetLastError(sqlite3_vfs* pVfs, int nErr, char* zOut);
static int mcVfsCurrentTimeInt64(sqlite3_vfs* pVfs, sqlite3_int64* pOut);
static int mcVfsSetSystemCall(sqlite3_vfs* pVfs, const char* zName, sqlite3_syscall_ptr pNewFunc);
static sqlite3_syscall_ptr mcVfsGetSystemCall(sqlite3_vfs* pVfs, const char* zName);
static const char* mcVfsNextSystemCall(sqlite3_vfs* pVfs, const char* zName);

/*
** Prototypes for IO methods
*/

static int mcIoClose(sqlite3_file* pFile);
static int mcIoRead(sqlite3_file* pFile, void*, int iAmt, sqlite3_int64 iOfst);
static int mcIoWrite(sqlite3_file* pFile,const void*,int iAmt, sqlite3_int64 iOfst);
static int mcIoTruncate(sqlite3_file* pFile, sqlite3_int64 size);
static int mcIoSync(sqlite3_file* pFile, int flags);
static int mcIoFileSize(sqlite3_file* pFile, sqlite3_int64* pSize);
static int mcIoLock(sqlite3_file* pFile, int lock);
static int mcIoUnlock(sqlite3_file* pFile, int lock);
static int mcIoCheckReservedLock(sqlite3_file* pFile, int *pResOut);
static int mcIoFileControl(sqlite3_file* pFile, int op, void *pArg);
static int mcIoSectorSize(sqlite3_file* pFile);
static int mcIoDeviceCharacteristics(sqlite3_file* pFile);
static int mcIoShmMap(sqlite3_file* pFile, int iPg, int pgsz, int map, void volatile** p);
static int mcIoShmLock(sqlite3_file* pFile, int offset, int n, int flags);
static void mcIoShmBarrier(sqlite3_file* pFile);
static int mcIoShmUnmap(sqlite3_file* pFile, int deleteFlag);
static int mcIoFetch(sqlite3_file* pFile, sqlite3_int64 iOfst, int iAmt, void** pp);
static int mcIoUnfetch(sqlite3_file* pFile, sqlite3_int64 iOfst, void* p);

#define SQLITE3MC_VFS_NAME ("multipleciphers")

/*
** Header sizes of WAL journal files
*/
static const int walFrameHeaderSize = 24;
static const int walFileHeaderSize = 32;

/*
** Global I/O method structure of SQLite3 Multiple Ciphers VFS
*/

#define IOMETHODS_VERSION_MIN 1
#define IOMETHODS_VERSION_MAX 3

static sqlite3_io_methods mcIoMethodsGlobal1 =
{
  1,                          /* iVersion */
  mcIoClose,                  /* xClose */
  mcIoRead,                   /* xRead */
  mcIoWrite,                  /* xWrite */
  mcIoTruncate,               /* xTruncate */
  mcIoSync,                   /* xSync */
  mcIoFileSize,               /* xFileSize */
  mcIoLock,                   /* xLock */
  mcIoUnlock,                 /* xUnlock */
  mcIoCheckReservedLock,      /* xCheckReservedLock */
  mcIoFileControl,            /* xFileControl */
  mcIoSectorSize,             /* xSectorSize */
  mcIoDeviceCharacteristics,  /* xDeviceCharacteristics */
  0,                          /* xShmMap */
  0,                          /* xShmLock */
  0,                          /* xShmBarrier */
  0,                          /* xShmUnmap */
  0,                          /* xFetch */
  0,                          /* xUnfetch */
};

static sqlite3_io_methods mcIoMethodsGlobal2 =
{
  2,                          /* iVersion */
  mcIoClose,                  /* xClose */
  mcIoRead,                   /* xRead */
  mcIoWrite,                  /* xWrite */
  mcIoTruncate,               /* xTruncate */
  mcIoSync,                   /* xSync */
  mcIoFileSize,               /* xFileSize */
  mcIoLock,                   /* xLock */
  mcIoUnlock,                 /* xUnlock */
  mcIoCheckReservedLock,      /* xCheckReservedLock */
  mcIoFileControl,            /* xFileControl */
  mcIoSectorSize,             /* xSectorSize */
  mcIoDeviceCharacteristics,  /* xDeviceCharacteristics */
  mcIoShmMap,                 /* xShmMap */
  mcIoShmLock,                /* xShmLock */
  mcIoShmBarrier,             /* xShmBarrier */
  mcIoShmUnmap,               /* xShmUnmap */
  0,                          /* xFetch */
  0,                          /* xUnfetch */
};

static sqlite3_io_methods mcIoMethodsGlobal3 =
{
  3,                          /* iVersion */
  mcIoClose,                  /* xClose */
  mcIoRead,                   /* xRead */
  mcIoWrite,                  /* xWrite */
  mcIoTruncate,               /* xTruncate */
  mcIoSync,                   /* xSync */
  mcIoFileSize,               /* xFileSize */
  mcIoLock,                   /* xLock */
  mcIoUnlock,                 /* xUnlock */
  mcIoCheckReservedLock,      /* xCheckReservedLock */
  mcIoFileControl,            /* xFileControl */
  mcIoSectorSize,             /* xSectorSize */
  mcIoDeviceCharacteristics,  /* xDeviceCharacteristics */
  mcIoShmMap,                 /* xShmMap */
  mcIoShmLock,                /* xShmLock */
  mcIoShmBarrier,             /* xShmBarrier */
  mcIoShmUnmap,               /* xShmUnmap */
  mcIoFetch,                  /* xFetch */
  mcIoUnfetch,                /* xUnfetch */
};

static sqlite3_io_methods* mcIoMethodsGlobal[] =
  { 0, &mcIoMethodsGlobal1 , &mcIoMethodsGlobal2 , &mcIoMethodsGlobal3 };

/*
** Internal functions
*/

/*
** Add an item to the list of main database files, if it is not already present.
*/
static void mcMainListAdd(sqlite3mc_file* pFile)
{
  assert( (pFile->openFlags & SQLITE_OPEN_MAIN_DB) );
  sqlite3_mutex_enter(pFile->pVfsMC->mutex);
  pFile->pMainNext = pFile->pVfsMC->pMain;
  pFile->pVfsMC->pMain = pFile;
  sqlite3_mutex_leave(pFile->pVfsMC->mutex);
}

/*
** Remove an item from the list of main database files.
*/
static void mcMainListRemove(sqlite3mc_file* pFile)
{
  sqlite3mc_file** pMainPrev;
  sqlite3_mutex_enter(pFile->pVfsMC->mutex);
  for (pMainPrev = &pFile->pVfsMC->pMain; *pMainPrev && *pMainPrev != pFile; pMainPrev = &((*pMainPrev)->pMainNext)){}
  if (*pMainPrev) *pMainPrev = pFile->pMainNext;
  pFile->pMainNext = 0;
  sqlite3_mutex_leave(pFile->pVfsMC->mutex);
}

/*
** Given that zFileName points to a buffer containing a database file name passed to 
** either the xOpen() or xAccess() VFS method, search the list of main database files
** for a file handle opened by the same database connection on the corresponding
** database file.
*/
static sqlite3mc_file* mcFindDbMainFileName(sqlite3mc_vfs* mcVfs, const char* zFileName)
{
  sqlite3mc_file* pDb;
  sqlite3_mutex_enter(mcVfs->mutex);
  for (pDb = mcVfs->pMain; pDb && pDb->zFileName != zFileName; pDb = pDb->pMainNext){}
  sqlite3_mutex_leave(mcVfs->mutex);
  return pDb;
}

/*
** Find a pointer to the Multiple Ciphers VFS in use for a database connection.
*/
static sqlite3mc_vfs* mcFindVfs(sqlite3* db, const char* zDbName)
{
  sqlite3mc_vfs* pVfsMC = NULL;
  if (db->pVfs && db->pVfs->xOpen == mcVfsOpen)
  {
    /* The top level VFS is a Multiple Ciphers VFS */
    pVfsMC = (sqlite3mc_vfs*)(db->pVfs);
  }
  else
  {
    /*
    ** The top level VFS is not a Multiple Ciphers VFS.
    ** Retrieve the VFS names stack.
    */
    char* zVfsNameStack = 0;
    if ((sqlite3_file_control(db, zDbName, SQLITE_FCNTL_VFSNAME, &zVfsNameStack) == SQLITE_OK) && (zVfsNameStack != NULL))
    {
      /* Search for the name prefix of a Multiple Ciphers VFS. */
      char* zVfsName = strstr(zVfsNameStack, SQLITE3MC_VFS_NAME);
      if (zVfsName != NULL)
      {
        /* The prefix was found, now determine the full VFS name. */
        char* zVfsNameEnd = zVfsName + strlen(SQLITE3MC_VFS_NAME);
        if (*zVfsNameEnd == '-')
        {
          for (++zVfsNameEnd; *zVfsNameEnd != '/'  && *zVfsNameEnd != 0; ++zVfsNameEnd);
          if (*zVfsNameEnd == '/') *zVfsNameEnd = 0;

          /* Find a pointer to the VFS with the determined name. */
          sqlite3_vfs* pVfs = sqlite3_vfs_find(zVfsName);
          if (pVfs && pVfs->xOpen == mcVfsOpen)
          {
            pVfsMC = (sqlite3mc_vfs*) pVfs;
          }
        }
      }
      sqlite3_free(zVfsNameStack);
    }
  }
  return pVfsMC;
}

/*
** Find the codec of the database file
** corresponding to the database schema name.
*/
SQLITE_PRIVATE Codec* sqlite3mcGetCodec(sqlite3* db, const char* zDbName)
{
  Codec* codec = NULL;
  sqlite3mc_vfs* pVfsMC = mcFindVfs(db, zDbName);

  if (pVfsMC)
  {
    const char* dbFileName = sqlite3_db_filename(db, zDbName);
    sqlite3mc_file* pDbMain = mcFindDbMainFileName(pVfsMC, dbFileName);
    if (pDbMain)
    {
      codec = pDbMain->codec;
    }
  }
  return codec;
}

/*
** Find the codec of the main database file.
*/
SQLITE_PRIVATE Codec* sqlite3mcGetMainCodec(sqlite3* db)
{
  return sqlite3mcGetCodec(db, "main");
}

/*
** Set the codec of the database file with the given database file name.
**
** The parameter db, the handle of the database connection, is currently
** not used to determine the database file handle, for which the codec
** should be set. The reason is that for shared cache mode the database
** connection handle is not unique, and it is not even clear which
** connection handle is actually valid, because the association between
** connection handles and database file handles is not maintained properly.
*/
SQLITE_PRIVATE void sqlite3mcSetCodec(sqlite3* db, const char* zDbName, const char* zFileName, Codec* codec)
{
  sqlite3mc_file* pDbMain = NULL;
  sqlite3mc_vfs* pVfsMC = mcFindVfs(db, zDbName);
  if (pVfsMC)
  {
    pDbMain = mcFindDbMainFileName((sqlite3mc_vfs*)(db->pVfs), zFileName);
  }
  if (pDbMain)
  {
    Codec* prevCodec = pDbMain->codec;
    Codec* msgCodec = (codec) ? codec : prevCodec;
    if (msgCodec)
    {
      /* Reset error state of pager */
      mcReportCodecError(sqlite3mcGetBtShared(msgCodec), SQLITE_OK);
    }
    if (prevCodec)
    {
      /*
      ** Free a codec that was already associated with this main database file handle
      */
      sqlite3mcCodecFree(prevCodec);
    }
    pDbMain->codec = codec;
  }
  else
  {
    /*
    ** No main database file handle found, free codec
    */
    sqlite3mcCodecFree(codec);
  }
}

/*
** This function is called by the wal module when writing page content
** into the log file.
**
** This function returns a pointer to a buffer containing the encrypted
** page content. If a malloc fails, this function may return NULL.
*/
int libsql_pager_codec_impl(libsql_pghdr* pPg, void **ret)
{
  int rc = SQLITE_NOMEM;
  if (!pPg || !pPg->pPager) {
    return SQLITE_MISUSE_BKPT;
  }

  sqlite3_file* pFile = sqlite3PagerFile(pPg->pPager);

  void* aData = 0;
  if (pFile->pMethods == &mcIoMethodsGlobal1 || 
      pFile->pMethods == &mcIoMethodsGlobal2 || 
      pFile->pMethods == &mcIoMethodsGlobal3)
  {
    sqlite3mc_file* mcFile = (sqlite3mc_file*) pFile;
    Codec* codec = mcFile->codec;
    if (codec != 0 && codec->m_walLegacy == 0 && sqlite3mcIsEncrypted(codec))
    {
      aData = sqlite3mcCodec(codec, pPg->pData, pPg->pgno, 6);
    }
    else
    {
      aData = (char*) pPg->pData;
    }
  }
  else
  {
    aData = (char*) pPg->pData;
  }

  if (aData) {
    *ret = aData;
    rc = SQLITE_OK;
  }
  return rc;
}

/*
** Implementation of VFS methods
*/

static int mcVfsOpen(sqlite3_vfs* pVfs, const char* zName, sqlite3_file* pFile, int flags, int* pOutFlags)
{
  int rc;
  sqlite3mc_vfs* mcVfs = (sqlite3mc_vfs*) pVfs;
  sqlite3mc_file* mcFile = (sqlite3mc_file*) pFile;
  mcFile->pFile = (sqlite3_file*) &mcFile[1];
  mcFile->pVfsMC = mcVfs;
  mcFile->openFlags = flags;
  mcFile->zFileName = zName;
  mcFile->codec = 0;
  mcFile->pMainDb = 0;
  mcFile->pMainNext = 0;
  mcFile->pageNo = 0;

  if (zName)
  {
    if (flags & SQLITE_OPEN_MAIN_DB)
    {
      mcFile->zFileName = zName;
      SQLITE3MC_DEBUG_LOG("mcVfsOpen MAIN: mcFile=%p fileName=%s\n", mcFile, mcFile->zFileName);
    }
    else if (flags & SQLITE_OPEN_TEMP_DB)
    {
      mcFile->zFileName = zName;
      SQLITE3MC_DEBUG_LOG("mcVfsOpen TEMP: mcFile=%p fileName=%s\n", mcFile, mcFile->zFileName);
    }
#if 0
    else if (flags & SQLITE_OPEN_TRANSIENT_DB)
    {
      /*
      ** TODO: When does SQLite open a transient DB? Could/Should it be encrypted?
      */
    }
#endif
    else if (flags & SQLITE_OPEN_MAIN_JOURNAL)
    {
      const char* dbFileName = sqlite3_filename_database(zName);
      mcFile->pMainDb = mcFindDbMainFileName(mcFile->pVfsMC, dbFileName);
      mcFile->zFileName = zName;
      SQLITE3MC_DEBUG_LOG("mcVfsOpen MAIN Journal: mcFile=%p fileName=%s dbFileName=%s\n", mcFile, mcFile->zFileName, dbFileName);
    }
#if 0
    else if (flags & SQLITE_OPEN_TEMP_JOURNAL)
    {
      /*
      ** TODO: When does SQLite open a temporary journal? Could/Should it be encrypted?
      */
    }
#endif
    else if (flags & SQLITE_OPEN_SUBJOURNAL)
    {
      const char* dbFileName = sqlite3_filename_database(zName);
      mcFile->pMainDb = mcFindDbMainFileName(mcFile->pVfsMC, dbFileName);
      mcFile->zFileName = zName;
      SQLITE3MC_DEBUG_LOG("mcVfsOpen SUB Journal: mcFile=%p fileName=%s dbFileName=%s\n", mcFile, mcFile->zFileName, dbFileName);
    }
#if 0
    else if (flags & SQLITE_OPEN_MASTER_JOURNAL)
    {
      /*
      ** Master journal contains only administrative information
      ** No encryption necessary
      */
    }
#endif
    else if (flags & SQLITE_OPEN_WAL)
    {
      const char* dbFileName = sqlite3_filename_database(zName);
      mcFile->pMainDb = mcFindDbMainFileName(mcFile->pVfsMC, dbFileName);
      mcFile->zFileName = zName;
      SQLITE3MC_DEBUG_LOG("mcVfsOpen WAL Journal: mcFile=%p fileName=%s dbFileName=%s\n", mcFile, mcFile->zFileName, dbFileName);
    }
  }

  rc = REALVFS(pVfs)->xOpen(REALVFS(pVfs), zName, mcFile->pFile, flags, pOutFlags);
  if (rc == SQLITE_OK)
  {
    /*
    ** Real open succeeded
    ** Initialize methods (use same version number as underlying implementation
    ** Register main database files
    */
    int ioMethodsVersion = mcFile->pFile->pMethods->iVersion;
    if (ioMethodsVersion < IOMETHODS_VERSION_MIN ||
        ioMethodsVersion > IOMETHODS_VERSION_MAX)
    {
      /* If version out of range, use highest known version */
      ioMethodsVersion = IOMETHODS_VERSION_MAX;
    }
    pFile->pMethods = mcIoMethodsGlobal[ioMethodsVersion];
    if (flags & SQLITE_OPEN_MAIN_DB)
    {
      mcMainListAdd(mcFile);
    }
  }
  return rc;
}

static int mcVfsDelete(sqlite3_vfs* pVfs, const char* zName, int syncDir)
{
  return REALVFS(pVfs)->xDelete(REALVFS(pVfs), zName, syncDir);
}

static int mcVfsAccess(sqlite3_vfs* pVfs, const char* zName, int flags, int* pResOut)
{
  return REALVFS(pVfs)->xAccess(REALVFS(pVfs), zName, flags, pResOut);
}

static int mcVfsFullPathname(sqlite3_vfs* pVfs, const char* zName, int nOut, char* zOut)
{
  return REALVFS(pVfs)->xFullPathname(REALVFS(pVfs), zName, nOut, zOut);
}

static void* mcVfsDlOpen(sqlite3_vfs* pVfs, const char* zFilename)
{
  return REALVFS(pVfs)->xDlOpen(REALVFS(pVfs), zFilename);
}

static void mcVfsDlError(sqlite3_vfs* pVfs, int nByte, char* zErrMsg)
{
  REALVFS(pVfs)->xDlError(REALVFS(pVfs), nByte, zErrMsg);
}

static void (*mcVfsDlSym(sqlite3_vfs* pVfs, void* p, const char* zSymbol))(void)
{
  return REALVFS(pVfs)->xDlSym(REALVFS(pVfs), p, zSymbol);
}

static void mcVfsDlClose(sqlite3_vfs* pVfs, void* p)
{
  REALVFS(pVfs)->xDlClose(REALVFS(pVfs), p);
}

static int mcVfsRandomness(sqlite3_vfs* pVfs, int nByte, char* zOut)
{
  return REALVFS(pVfs)->xRandomness(REALVFS(pVfs), nByte, zOut);
}

static int mcVfsSleep(sqlite3_vfs* pVfs, int microseconds)
{
  return REALVFS(pVfs)->xSleep(REALVFS(pVfs), microseconds);
}

static int mcVfsCurrentTime(sqlite3_vfs* pVfs, double* pOut)
{
  return REALVFS(pVfs)->xCurrentTime(REALVFS(pVfs), pOut);
}

static int mcVfsGetLastError(sqlite3_vfs* pVfs, int code, char* pOut)
{
  return REALVFS(pVfs)->xGetLastError(REALVFS(pVfs), code, pOut);
}

static int mcVfsCurrentTimeInt64(sqlite3_vfs* pVfs, sqlite3_int64* pOut)
{
  return REALVFS(pVfs)->xCurrentTimeInt64(REALVFS(pVfs), pOut);
}

static int mcVfsSetSystemCall(sqlite3_vfs* pVfs, const char* zName, sqlite3_syscall_ptr pNewFunc)
{
  return REALVFS(pVfs)->xSetSystemCall(REALVFS(pVfs), zName, pNewFunc);
}

static sqlite3_syscall_ptr mcVfsGetSystemCall(sqlite3_vfs* pVfs, const char* zName)
{
  return REALVFS(pVfs)->xGetSystemCall(REALVFS(pVfs), zName);
}

static const char* mcVfsNextSystemCall(sqlite3_vfs* pVfs, const char* zName)
{
  return REALVFS(pVfs)->xNextSystemCall(REALVFS(pVfs), zName);
}

/*
** IO methods
*/

static int mcIoClose(sqlite3_file* pFile)
{
  int rc;
  sqlite3mc_file* p = (sqlite3mc_file*) pFile;

  /*
  ** Unregister main database files
  */
  if (p->openFlags & SQLITE_OPEN_MAIN_DB)
  {
    mcMainListRemove(p);
  }

  /*
  ** Release codec memory
  */
  if (p->codec)
  {
    sqlite3mcCodecFree(p->codec);
    p->codec = 0;
  }

  assert(p->pMainNext == 0 && p->pVfsMC->pMain != p);
  rc = REALFILE(pFile)->pMethods->xClose(REALFILE(pFile));
  return rc;
}

/*
** Read operation on main database file
*/
static int mcReadMainDb(sqlite3_file* pFile, void* buffer, int count, sqlite3_int64 offset)
{
  int rc = SQLITE_OK;
  sqlite3mc_file* mcFile = (sqlite3mc_file*) pFile;

  /*
  ** Special case: read 16 bytes salt from beginning of database file without decrypting
  */
  if (offset == 0 && count == 16)
  {
    return rc;
  }

  if (mcFile->codec != 0 && sqlite3mcIsEncrypted(mcFile->codec))
  {
    const int pageSize = sqlite3mcGetPageSize(mcFile->codec);
    const int deltaOffset = offset % pageSize;
    const int deltaCount = count % pageSize;
    if (deltaOffset || deltaCount)
    {
      /*
      ** Read partial page
      */
      int pageNo = 0;
      void* bufferDecrypted = 0;
      const sqlite3_int64 prevOffset = offset - deltaOffset;
      unsigned char* pageBuffer = sqlite3mcGetPageBuffer(mcFile->codec);

      /*
      ** Read complete page from file
      */
      rc = REALFILE(pFile)->pMethods->xRead(REALFILE(pFile), pageBuffer, pageSize, prevOffset);
      if (rc == SQLITE_IOERR_SHORT_READ)
      {
        return rc;
      }

      /*
      ** Determine page number and decrypt page buffer
      */
      pageNo = prevOffset / pageSize + 1;
      bufferDecrypted = sqlite3mcCodec(mcFile->codec, pageBuffer, pageNo, 3);

      /*
      ** Return the requested content
      */
      if (deltaOffset)
      {
        memcpy(buffer, pageBuffer + deltaOffset, count);
      }
      else
      {
        memcpy(buffer, pageBuffer, count);
      }
    }
    else
    {
      /*
      ** Read full page(s)
      **
      ** In fact, SQLite reads only one database page at a time.
      ** This would allow to remove the page loop below.
      */
      unsigned char* data = (unsigned char*) buffer;
      int pageNo = offset / pageSize + 1;
      int nPages = count / pageSize;
      int iPage;
      for (iPage = 0; iPage < nPages; ++iPage)
      {
        void* bufferDecrypted = sqlite3mcCodec(mcFile->codec, data, pageNo, 3);
        data += pageSize;
        offset += pageSize;
        ++pageNo;
      }
    }
  }
  return rc;
}

/*
** Read operation on main journal file
*/
static int mcReadMainJournal(sqlite3_file* pFile, const void* buffer, int count, sqlite3_int64 offset)
{
  int rc = SQLITE_OK;
  sqlite3mc_file* mcFile = (sqlite3mc_file*) pFile;
  Codec* codec = (mcFile->pMainDb) ? mcFile->pMainDb->codec : 0;

  if (codec != 0 && sqlite3mcIsEncrypted(codec))
  {
    const int pageSize = sqlite3mcGetPageSize(codec);

    if (count == pageSize && mcFile->pageNo != 0)
    {
      /*
      ** Decrypt the page buffer, but only if the page number is valid
      */
      void* bufferDecrypted = sqlite3mcCodec(codec, (char*) buffer, mcFile->pageNo, 3);
      mcFile->pageNo = 0;
    }
    else if (count == 4)
    {
      /*
      ** SQLite always reads the page number from the journal file
      ** immediately before the corresponding page content is read.
      */
      mcFile->pageNo = sqlite3Get4byte(buffer);
    }
  }
  return rc;
}

/*
** Read operation on subjournal file
*/
static int mcReadSubJournal(sqlite3_file* pFile, const void* buffer, int count, sqlite3_int64 offset)
{
  int rc = SQLITE_OK;
  sqlite3mc_file* mcFile = (sqlite3mc_file*) pFile;
  Codec* codec = (mcFile->pMainDb) ? mcFile->pMainDb->codec : 0;

  if (codec != 0 && sqlite3mcIsEncrypted(codec))
  {
    const int pageSize = sqlite3mcGetPageSize(codec);

    if (count == pageSize && mcFile->pageNo != 0)
    {
      /*
      ** Decrypt the page buffer, but only if the page number is valid
      */
      void* bufferDecrypted = sqlite3mcCodec(codec, (char*) buffer, mcFile->pageNo, 3);
    }
    else if (count == 4)
    {
      /*
      ** SQLite always reads the page number from the journal file
      ** immediately before the corresponding page content is read.
      */
      mcFile->pageNo = sqlite3Get4byte(buffer);
    }
  }
  return rc;
}

/*
** Read operation on WAL journal file
*/
static int mcReadWal(sqlite3_file* pFile, const void* buffer, int count, sqlite3_int64 offset)
{
  int rc = SQLITE_OK;
  sqlite3mc_file* mcFile = (sqlite3mc_file*) pFile;
  Codec* codec = (mcFile->pMainDb) ? mcFile->pMainDb->codec : 0;

  if (codec != 0 && sqlite3mcIsEncrypted(codec))
  {
    const int pageSize = sqlite3mcGetPageSize(codec);

    if (count == pageSize)
    {
      int pageNo = 0;
      unsigned char ac[4];

      /*
      ** Determine page number
      **
      ** It is necessary to explicitly read the page number from the frame header.
      */
      rc = REALFILE(pFile)->pMethods->xRead(REALFILE(pFile), ac, 4, offset - walFrameHeaderSize);
      if (rc == SQLITE_OK)
      {
        pageNo = sqlite3Get4byte(ac);
      }

      /*
      ** Decrypt page content if page number is valid
      */
      if (pageNo != 0)
      {
        void* bufferDecrypted = sqlite3mcCodec(codec, (char*)buffer, pageNo, 3);
      }
    }
    else if (codec->m_walLegacy != 0 && count == pageSize + walFrameHeaderSize)
    {
      int pageNo = sqlite3Get4byte(buffer);

      /*
      ** Decrypt page content if page number is valid
      */
      if (pageNo != 0)
      {
        void* bufferDecrypted = sqlite3mcCodec(codec, (char*)buffer+walFrameHeaderSize, pageNo, 3);
      }
    }
  }
  return rc;
}

static int mcIoRead(sqlite3_file* pFile, void* buffer, int count, sqlite3_int64 offset)
{
  sqlite3mc_file* mcFile = (sqlite3mc_file*) pFile;
  int rc = REALFILE(pFile)->pMethods->xRead(REALFILE(pFile), buffer, count, offset);
  if (rc == SQLITE_IOERR_SHORT_READ)
  {
    return rc;
  }

  if (mcFile->openFlags & SQLITE_OPEN_MAIN_DB)
  {
    rc = mcReadMainDb(pFile, buffer, count, offset);
  }
#if 0
  else if (mcFile->openFlags & SQLITE_OPEN_TEMP_DB)
  {
    /*
    ** TODO: Could/Should a temporary database file be encrypted?
    */
  }
#endif
#if 0
  else if (mcFile->openFlags & SQLITE_OPEN_TRANSIENT_DB)
  {
    /*
    ** TODO: Could/Should a transient database file be encrypted?
    */
  }
#endif
  else if (mcFile->openFlags & SQLITE_OPEN_MAIN_JOURNAL)
  {
    rc = mcReadMainJournal(pFile, buffer, count, offset);
  }
#if 0
  else if (mcFile->openFlags & SQLITE_OPEN_TEMP_JOURNAL)
  {
    /*
    ** TODO: Could/Should a temporary journal file be encrypted?
    */
  }
#endif
  else if (mcFile->openFlags & SQLITE_OPEN_SUBJOURNAL)
  {
    rc = mcReadSubJournal(pFile, buffer, count, offset);
  }
#if 0
  else if (mcFile->openFlags & SQLITE_OPEN_MASTER_JOURNAL)
  {
    /*
    ** Master journal contains only administrative information
    ** No encryption necessary
    */
  }
#endif
  else if (mcFile->openFlags & SQLITE_OPEN_WAL)
  {
    rc = mcReadWal(pFile, buffer, count, offset);
  }
  return rc;
}

/*
** Write operation on main database file
*/
static int mcWriteMainDb(sqlite3_file* pFile, const void* buffer, int count, sqlite3_int64 offset)
{
  int rc = SQLITE_OK;
  sqlite3mc_file* mcFile = (sqlite3mc_file*) pFile;

  if (mcFile->codec != 0 && sqlite3mcIsEncrypted(mcFile->codec))
  {
    const int pageSize = sqlite3mcGetPageSize(mcFile->codec);
    const int deltaOffset = offset % pageSize;
    const int deltaCount = count % pageSize;

    if (deltaOffset || deltaCount)
    {
      /*
      ** Write partial page
      **
      ** SQLite does never write partial database pages.
      ** Therefore no encryption is required in this case.
      */
      rc = REALFILE(pFile)->pMethods->xWrite(REALFILE(pFile), buffer, count, offset);
    }
    else
    {
      /*
      ** Write full page(s)
      **
      ** In fact, SQLite writes only one database page at a time.
      ** This would allow to remove the page loop below.
      */
      char* data = (char*) buffer;
      int pageNo = offset / pageSize + 1;
      int nPages = count / pageSize;
      int iPage;
      for (iPage = 0; iPage < nPages; ++iPage)
      {
        void* bufferEncrypted = sqlite3mcCodec(mcFile->codec, data, pageNo, 6);
        rc = REALFILE(pFile)->pMethods->xWrite(REALFILE(pFile), bufferEncrypted, pageSize, offset);
        data += pageSize;
        offset += pageSize;
        ++pageNo;
      }
    }
  }
  else
  {
    /*
    ** Write buffer without encryption
    */
    rc = REALFILE(pFile)->pMethods->xWrite(REALFILE(pFile), buffer, count, offset);
  }
  return rc;
}

/*
** Write operation on main journal file
*/
static int mcWriteMainJournal(sqlite3_file* pFile, const void* buffer, int count, sqlite3_int64 offset)
{
  int rc = SQLITE_OK;
  sqlite3mc_file* mcFile = (sqlite3mc_file*) pFile;
  Codec* codec = (mcFile->pMainDb) ? mcFile->pMainDb->codec : 0;

  if (codec != 0 && sqlite3mcIsEncrypted(codec))
  {
    const int pageSize = sqlite3mcGetPageSize(codec);
    const int frameSize = pageSize + 4 + 4;

    if (count == pageSize && mcFile->pageNo != 0)
    {
      /*
      ** Encrypt the page buffer, but only if the page number is valid
      */
      void* bufferEncrypted = sqlite3mcCodec(codec, (char*) buffer, mcFile->pageNo, 7);
      rc = REALFILE(pFile)->pMethods->xWrite(REALFILE(pFile), bufferEncrypted, pageSize, offset);
    }
    else
    {
      /*
      ** Write buffer without encryption
      */
      rc = REALFILE(pFile)->pMethods->xWrite(REALFILE(pFile), buffer, count, offset);
      if (count == 4)
      {
        /*
        ** SQLite always writes the page number to the journal file
        ** immediately before the corresponding page content is written.
        */
        mcFile->pageNo = (rc == SQLITE_OK) ? sqlite3Get4byte(buffer) : 0;
      }
    }
  }
  else
  {
    /*
    ** Write buffer without encryption
    */
    rc = REALFILE(pFile)->pMethods->xWrite(REALFILE(pFile), buffer, count, offset);
  }
  return rc;
}

/*
** Write operation on subjournal file
*/
static int mcWriteSubJournal(sqlite3_file* pFile, const void* buffer, int count, sqlite3_int64 offset)
{
  int rc = SQLITE_OK;
  sqlite3mc_file* mcFile = (sqlite3mc_file*) pFile;
  Codec* codec = (mcFile->pMainDb) ? mcFile->pMainDb->codec : 0;

  if (codec != 0 && sqlite3mcIsEncrypted(codec))
  {
    const int pageSize = sqlite3mcGetPageSize(codec);
    const int frameSize = pageSize + 4;

    if (count == pageSize && mcFile->pageNo != 0)
    {
      /*
      ** Encrypt the page buffer, but only if the page number is valid
      */
      void* bufferEncrypted = sqlite3mcCodec(codec, (char*) buffer, mcFile->pageNo, 7);
      rc = REALFILE(pFile)->pMethods->xWrite(REALFILE(pFile), bufferEncrypted, pageSize, offset);
    }
    else
    {
      /*
      ** Write buffer without encryption
      */
      rc = REALFILE(pFile)->pMethods->xWrite(REALFILE(pFile), buffer, count, offset);
      if (count == 4)
      {
        /*
        ** SQLite always writes the page number to the journal file
        ** immediately before the corresponding page content is written.
        */
        mcFile->pageNo = (rc == SQLITE_OK) ? sqlite3Get4byte(buffer) : 0;
      }
    }
  }
  else
  {
    /*
    ** Write buffer without encryption
    */
    rc = REALFILE(pFile)->pMethods->xWrite(REALFILE(pFile), buffer, count, offset);
  }
  return rc;
}

/*
** Write operation on WAL journal file
*/
static int mcWriteWal(sqlite3_file* pFile, const void* buffer, int count, sqlite3_int64 offset)
{
  int rc = SQLITE_OK;
  sqlite3mc_file* mcFile = (sqlite3mc_file*) pFile;
  Codec* codec = (mcFile->pMainDb) ? mcFile->pMainDb->codec : 0;

  if (codec != 0 && codec->m_walLegacy != 0 && sqlite3mcIsEncrypted(codec))
  {
    const int pageSize = sqlite3mcGetPageSize(codec);

    if (count == pageSize)
    {
      int pageNo = 0;
      unsigned char ac[4];

      /*
      ** Read the corresponding page number from the file
      **
      ** In WAL mode SQLite does not write the page number of a page to file
      ** immediately before writing the corresponding page content.
      ** Page numbers and checksums are written to file independently.
      ** Therefore it is necessary to explicitly read the page number
      ** on writing to file the content of a page.
      */
      rc = REALFILE(pFile)->pMethods->xRead(REALFILE(pFile), ac, 4, offset - walFrameHeaderSize);
      if (rc == SQLITE_OK)
      {
        pageNo = sqlite3Get4byte(ac);
      }

      if (pageNo != 0)
      {
        /*
        ** Encrypt the page buffer, but only if the page number is valid
        */
        void* bufferEncrypted = sqlite3mcCodec(codec, (char*) buffer, pageNo, 7);
        rc = REALFILE(pFile)->pMethods->xWrite(REALFILE(pFile), bufferEncrypted, pageSize, offset);
      }
      else
      {
        /*
        ** Write buffer without encryption if the page number could not be determined
        */
        rc = REALFILE(pFile)->pMethods->xWrite(REALFILE(pFile), buffer, count, offset);
      }
    }
    else if (count == pageSize + walFrameHeaderSize)
    {
      int pageNo = sqlite3Get4byte(buffer);
      if (pageNo != 0)
      {
        /*
        ** Encrypt the page buffer, but only if the page number is valid
        */
        void* bufferEncrypted = sqlite3mcCodec(codec, (char*)buffer+walFrameHeaderSize, pageNo, 7);
        rc = REALFILE(pFile)->pMethods->xWrite(REALFILE(pFile), buffer, walFrameHeaderSize, offset);
        rc = REALFILE(pFile)->pMethods->xWrite(REALFILE(pFile), bufferEncrypted, pageSize, offset+walFrameHeaderSize);
      }
      else
      {
        /*
        ** Write buffer without encryption if the page number could not be determined
        */
        rc = REALFILE(pFile)->pMethods->xWrite(REALFILE(pFile), buffer, count, offset);
      }
    }
    else
    {
      /*
      ** Write buffer without encryption if it is not a database page
      */
      rc = REALFILE(pFile)->pMethods->xWrite(REALFILE(pFile), buffer, count, offset);
    }
  }
  else
  {
    /*
    ** Write buffer without encryption
    */
    rc = REALFILE(pFile)->pMethods->xWrite(REALFILE(pFile), buffer, count, offset);
  }
  return rc;
}

static int mcIoWrite(sqlite3_file* pFile, const void* buffer, int count, sqlite3_int64 offset)
{
  int rc = SQLITE_OK;
  int doDefault = 1;
  sqlite3mc_file* mcFile = (sqlite3mc_file*) pFile;

  if (mcFile->openFlags & SQLITE_OPEN_MAIN_DB)
  {
    rc = mcWriteMainDb(pFile, buffer, count, offset);
  }
#if 0
  else if (mcFile->openFlags & SQLITE_OPEN_TEMP_DB)
  {
    /*
    ** TODO: Could/Should a temporary database file be encrypted?
    */
  }
#endif
#if 0
  else if (mcFile->openFlags & SQLITE_OPEN_TRANSIENT_DB)
  {
    /*
    ** TODO: Could/Should a transient database file be encrypted?
    */
  }
#endif
  else if (mcFile->openFlags & SQLITE_OPEN_MAIN_JOURNAL)
  {
    rc = mcWriteMainJournal(pFile, buffer, count, offset);
  }
#if 0
  else if (mcFile->openFlags & SQLITE_OPEN_TEMP_JOURNAL)
  {
    /*
    ** TODO: Could/Should a temporary journal file be encrypted?
    */
  }
#endif
  else if (mcFile->openFlags & SQLITE_OPEN_SUBJOURNAL)
  {
    rc = mcWriteSubJournal(pFile, buffer, count, offset);
}
#if 0
  else if (mcFile->openFlags & SQLITE_OPEN_MASTER_JOURNAL)
  {
    /*
    ** Master journal contains only administrative information
    ** No encryption necessary
    */
  }
#endif
  /*
  ** The page content is encrypted in memory in the WAL journal handler.
  ** This provides for compatibility with legacy applications using the
  ** previous SQLITE_HAS_CODEC encryption API.
  */
  else if (mcFile->openFlags & SQLITE_OPEN_WAL)
  {
    rc = mcWriteWal(pFile, buffer, count, offset);
  }
  else
  {
    rc = REALFILE(pFile)->pMethods->xWrite(REALFILE(pFile), buffer, count, offset);
  }
  return rc;
}

static int mcIoTruncate(sqlite3_file* pFile, sqlite3_int64 size)
{
  return REALFILE(pFile)->pMethods->xTruncate(REALFILE(pFile), size);
}

static int mcIoSync(sqlite3_file* pFile, int flags)
{
  return REALFILE(pFile)->pMethods->xSync(REALFILE(pFile), flags);
}

static int mcIoFileSize(sqlite3_file* pFile, sqlite3_int64* pSize)
{
  return REALFILE(pFile)->pMethods->xFileSize(REALFILE(pFile), pSize);
}

static int mcIoLock(sqlite3_file* pFile, int lock)
{
  return REALFILE(pFile)->pMethods->xLock(REALFILE(pFile), lock);
}

static int mcIoUnlock(sqlite3_file* pFile, int lock)
{
  return REALFILE(pFile)->pMethods->xUnlock(REALFILE(pFile), lock);
}

static int mcIoCheckReservedLock(sqlite3_file* pFile, int* pResOut)
{
  return REALFILE(pFile)->pMethods->xCheckReservedLock(REALFILE(pFile), pResOut);
}

static int mcIoFileControl(sqlite3_file* pFile, int op, void* pArg)
{
  int rc = SQLITE_OK;
  int doReal = 1;
  sqlite3mc_file* p = (sqlite3mc_file*) pFile;

  switch (op)
  {
    case SQLITE_FCNTL_PDB:
      {
#if 0
        /*
        ** pArg points to the sqlite3* handle for which the database file was opened.
        ** In shared cache mode this function is invoked for every use of the database
        ** file in a connection. Unfortunately there is no notification, when a database
        ** file is no longer used by a connection (close in normal mode).
        **
        ** For now, the database handle will not be stored in the file object.
        ** In the future, this behaviour may be changed, especially, if shared cache mode
        ** is disabled. Shared cache mode is enabled for backward compatibility only, its
        ** use is not recommended. A future version of SQLite might disable it by default.
        */
        sqlite3* db = *((sqlite3**) pArg);
#endif
    }
      break;
    case SQLITE_FCNTL_PRAGMA:
      {
        /*
        ** Handle pragmas specific to this database file
        */
#if 0
        /*
        ** SQLite invokes this function for all pragmas, which are related to the schema
        ** associated with this database file. In case of an unknown pragma, this function
        ** should return SQLITE_NOTFOUND. However, since this VFS is just a shim, handling
        ** of the pragma is forwarded to the underlying real VFS in such a case.
        **
        ** For now, all pragmas are handled at the connection level.
        ** For this purpose the SQLite's pragma handling is intercepted.
        ** The latter requires a patch of SQLite's amalgamation code.
        ** Maybe a future version will be able to abandon the patch.
        */
        char* pragmaName = ((char**) pArg)[1];
        char* pragmaValue = ((char**) pArg)[2];
        if (sqlite3StrICmp(pragmaName, "...") == 0)
        {
          /* Action */
          /* ((char**) pArg)[0] = sqlite3_mprintf("error msg.");*/
          doReal = 0;
        }
#endif
      }
      break;
    default:
      break;
  }
  if (doReal)
  {
    rc = REALFILE(pFile)->pMethods->xFileControl(REALFILE(pFile), op, pArg);
    if (rc == SQLITE_OK && op == SQLITE_FCNTL_VFSNAME)
    {
      sqlite3mc_vfs* pVfsMC = p->pVfsMC;
      char* zIn = *(char**)pArg;
      char* zOut = sqlite3_mprintf("%s/%z", pVfsMC->base.zName, zIn);
      *(char**)pArg = zOut;
      if (zOut == 0) rc = SQLITE_NOMEM;
    }
  }
  return rc;
}

static int mcIoSectorSize(sqlite3_file* pFile)
{
  if (REALFILE(pFile)->pMethods->xSectorSize)
    return REALFILE(pFile)->pMethods->xSectorSize(REALFILE(pFile));
  else
    return SQLITE_DEFAULT_SECTOR_SIZE;
}

static int mcIoDeviceCharacteristics(sqlite3_file* pFile)
{
  return REALFILE(pFile)->pMethods->xDeviceCharacteristics(REALFILE(pFile));
}

static int mcIoShmMap(sqlite3_file* pFile, int iPg, int pgsz, int map, void volatile** p)
{
  return REALFILE(pFile)->pMethods->xShmMap(REALFILE(pFile), iPg, pgsz, map, p);
}

static int mcIoShmLock(sqlite3_file* pFile, int offset, int n, int flags)
{
  return REALFILE(pFile)->pMethods->xShmLock(REALFILE(pFile), offset, n, flags);
}

static void mcIoShmBarrier(sqlite3_file* pFile)
{
  REALFILE(pFile)->pMethods->xShmBarrier(REALFILE(pFile));
}

static int mcIoShmUnmap(sqlite3_file* pFile, int deleteFlag)
{
  return REALFILE(pFile)->pMethods->xShmUnmap(REALFILE(pFile), deleteFlag);
}

static int mcIoFetch(sqlite3_file* pFile, sqlite3_int64 iOfst, int iAmt, void** pp)
{
  return REALFILE(pFile)->pMethods->xFetch(REALFILE(pFile), iOfst, iAmt, pp);
}

static int mcIoUnfetch( sqlite3_file* pFile, sqlite3_int64 iOfst, void* p)
{
  return REALFILE(pFile)->pMethods->xUnfetch(REALFILE(pFile), iOfst, p);
}

/*
** SQLite3 Multiple Ciphers internal API functions
*/

/*
** Check the requested VFS
*/
SQLITE_PRIVATE int
sqlite3mcCheckVfs(const char* zVfs)
{
  int rc = SQLITE_OK;
  sqlite3_vfs* pVfs = sqlite3_vfs_find(zVfs);
  if (pVfs == NULL)
  {
    /* VFS not found */
    int prefixLen = (int) strlen(SQLITE3MC_VFS_NAME);
    if (strncmp(zVfs, SQLITE3MC_VFS_NAME, prefixLen) == 0)
    {
      /* VFS name starts with prefix. */
      const char* zVfsNameEnd = zVfs + strlen(SQLITE3MC_VFS_NAME);
      if (*zVfsNameEnd == '-')
      {
        /* Prefix separator found, determine the name of the real VFS. */
        const char* zVfsReal = zVfsNameEnd + 1;
        pVfs = sqlite3_vfs_find(zVfsReal);
        if (pVfs != NULL)
        {
          /* Real VFS exists */
          /* Create VFS with encryption support based on real VFS */
          rc = sqlite3mc_vfs_create(zVfsReal, 0);
        }
      }
    }
  }
  return rc;
}

int libsql_pager_has_codec_impl(struct Pager* pPager)
{
  int hasCodec = 0;
  sqlite3mc_vfs* pVfsMC = NULL;
  sqlite3_vfs* pVfs = pPager->pVfs;

  /* Check whether the VFS stack of the pager contains a Multiple Ciphers VFS */
  for (; pVfs; pVfs = pVfs->pNext)
  {
    if (pVfs && pVfs->xOpen == mcVfsOpen)
    {
      /* Multiple Ciphers VFS found */
      pVfsMC = (sqlite3mc_vfs*)(pVfs);
      break;
    }
  }

  /* Check whether codec is enabled for associated database file */
  if (pVfsMC)
  {
    sqlite3mc_file* mcFile = mcFindDbMainFileName(pVfsMC, pPager->zFilename);
    if (mcFile)
    {
      Codec* codec = mcFile->codec;
      hasCodec = (codec != 0 && sqlite3mcIsEncrypted(codec));
    }
  }
  return hasCodec;
}

/*
** SQLite3 Multiple Ciphers external API functions
*/

static void mcVfsDestroy(sqlite3_vfs* pVfs)
{
  if (pVfs && pVfs->xOpen == mcVfsOpen)
  {
    /* Destroy the VFS instance only if no file is referring to it any longer */
    if (((sqlite3mc_vfs*) pVfs)->pMain == 0)
    {
      sqlite3_mutex_free(((sqlite3mc_vfs*)pVfs)->mutex);
      sqlite3_vfs_unregister(pVfs);
      sqlite3_free(pVfs);
    }
  }
}

/*
** Unregister and destroy a Multiple Ciphers VFS
** created by an earlier call to sqlite3mc_vfs_create().
*/
SQLITE_API void sqlite3mc_vfs_destroy(const char* zName)
{
  mcVfsDestroy(sqlite3_vfs_find(zName));
}

/*
** Create a Multiple Ciphers VFS based on the underlying VFS with name given by zVfsReal.
** If makeDefault is true, the VFS is set as the default VFS.
*/
SQLITE_API int sqlite3mc_vfs_create(const char* zVfsReal, int makeDefault)
{
  static sqlite3_vfs mcVfsTemplate =
  {
    3,                      /* iVersion */
    0,                      /* szOsFile */
    1024,                   /* mxPathname */
    0,                      /* pNext */
    0,                      /* zName */
    0,                      /* pAppData */
    mcVfsOpen,              /* xOpen */
    mcVfsDelete,            /* xDelete */
    mcVfsAccess,            /* xAccess */
    mcVfsFullPathname,      /* xFullPathname */
#ifndef SQLITE_OMIT_LOAD_EXTENSION
    mcVfsDlOpen,            /* xDlOpen */
    mcVfsDlError,           /* xDlError */
    mcVfsDlSym,             /* xDlSym */
    mcVfsDlClose,           /* xDlClose */
#else
    0, 0, 0, 0,
#endif
    mcVfsRandomness,        /* xRandomness */
    mcVfsSleep,             /* xSleep */
    mcVfsCurrentTime,       /* xCurrentTime */
    mcVfsGetLastError,      /* xGetLastError */
    mcVfsCurrentTimeInt64,  /* xCurrentTimeInt64 */
    mcVfsSetSystemCall,     /* xSetSystemCall */
    mcVfsGetSystemCall,     /* xGetSystemCall */
    mcVfsNextSystemCall     /* xNextSystemCall */
  };
  sqlite3mc_vfs* pVfsNew = 0;  /* Newly allocated VFS */
  sqlite3_vfs* pVfsReal = sqlite3_vfs_find(zVfsReal); /* Real VFS */
  int rc;

  if (pVfsReal)
  {
    size_t nPrefix = strlen(SQLITE3MC_VFS_NAME);
    size_t nRealName = strlen(pVfsReal->zName);
    size_t nName =  nPrefix + nRealName + 1;
    size_t nByte = sizeof(sqlite3mc_vfs) + nName + 1;
    pVfsNew = (sqlite3mc_vfs*) sqlite3_malloc64(nByte);
    if (pVfsNew)
    {
      char* zSpace = (char*) &pVfsNew[1];
      memset(pVfsNew, 0, nByte);
      memcpy(&pVfsNew->base, &mcVfsTemplate, sizeof(sqlite3_vfs));
      pVfsNew->base.iVersion = pVfsReal->iVersion;
      pVfsNew->base.pAppData = pVfsReal;
      pVfsNew->base.mxPathname = pVfsReal->mxPathname;
      pVfsNew->base.szOsFile = sizeof(sqlite3mc_file) + pVfsReal->szOsFile;

      /* Set name of new VFS as combination of the multiple ciphers prefix and the name of the underlying VFS */
      pVfsNew->base.zName = (const char*) zSpace;
      memcpy(zSpace, SQLITE3MC_VFS_NAME, nPrefix);
      memcpy(zSpace + nPrefix, "-", 1);
      memcpy(zSpace + nPrefix + 1, pVfsReal->zName, nRealName);

      /* Allocate the mutex and register the new VFS */
      pVfsNew->mutex = sqlite3_mutex_alloc(SQLITE_MUTEX_RECURSIVE);
      if (pVfsNew->mutex)
      {
        rc = sqlite3_vfs_register(&pVfsNew->base, makeDefault);
        if (rc != SQLITE_OK)
        {
          sqlite3_mutex_free(pVfsNew->mutex);
        }
      }
      else
      {
        /* Mutex could not be allocated */
        rc = SQLITE_NOMEM;
      }
      if (rc != SQLITE_OK)
      {
        /* Mutex could not be allocated or new VFS could not be registered */
        sqlite3_free(pVfsNew);
      }
    }
    else
    {
      /* New VFS could not be allocated */
      rc = SQLITE_NOMEM;
    }
  }
  else
  {
    /* Underlying VFS not found */
    rc = SQLITE_NOTFOUND;
  }
  return rc;
}

/*
** Shutdown all registered SQLite3 Multiple Ciphers VFSs
*/
SQLITE_API void sqlite3mc_vfs_shutdown()
{
  sqlite3_vfs* pVfs;
  sqlite3_vfs* pVfsNext;
  for (pVfs = sqlite3_vfs_find(0); pVfs; pVfs = pVfsNext)
  {
    pVfsNext = pVfs->pNext;
    mcVfsDestroy(pVfs);
  }
}

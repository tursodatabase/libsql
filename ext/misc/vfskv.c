/*
** 2022-09-06
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
******************************************************************************
**
** This file contains an experimental VFS layer that operates on a
** Key/Value storage engine where both keys and values must be pure
** text.
*/
#include "sqlite3.h"
#include <string.h>
#include <assert.h>
#include <stdio.h>
#include <stat/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <assert.h>


/*****************************************************************************
** The low-level storage engine
*/
typedef struct KVStorage KVStorage;
struct KVStorage {
  char *zDir;
  char zKey[50];
};

static KVStorage *kvstorageOpen(void);
static void kvstorageClose(KVStorage*);
static int kvstorageWrite(KVStorage*, const char *zKey, const char *zData);
static int kvstorageDelete(KVStorage*, const char *zKey);
static int kvstorageSize(KVStorage*, const char *zKey);
static int kvstorageRead(KVStorage*, const char *zKey, char *zBuf, int nBuf);


/*
** Forward declaration of objects used by this utility
*/
typedef struct KVVfsVfs KVVfsVfs;
typedef struct KVVfsFile KVVfsFile;

struct KVVfsVfs {
  sqlite3_vfs base;               /* VFS methods */
  KVStorage *pStore;              /* Single command KV storage object */
  KVVfsFile *pFiles;              /* List of open KVVfsFile objects */
};

struct KVVfsFile {
  sqlite3_file base;              /* IO methods */
  KVVfsVfs *pVfs;                 /* The VFS to which this file belongs */
  KVVfsFile *pNext;               /* Next in list of all files */
  int isJournal;                  /* True if this is a journal file */
  int nJrnl;                      /* Space allocated for aJrnl[] */
  char *aJrnl;                    /* Journal content */
};

/*
** Methods for KVVfsFile
*/
static int kvvfsClose(sqlite3_file*);
static int kvvfsRead(sqlite3_file*, void*, int iAmt, sqlite3_int64 iOfst);
static int kvvfsWrite(sqlite3_file*,const void*,int iAmt, sqlite3_int64 iOfst);
static int kvvfsTruncate(sqlite3_file*, sqlite3_int64 size);
static int kvvfsSync(sqlite3_file*, int flags);
static int kvvfsFileSize(sqlite3_file*, sqlite3_int64 *pSize);
static int kvvfsLock(sqlite3_file*, int);
static int kvvfsUnlock(sqlite3_file*, int);
static int kvvfsCheckReservedLock(sqlite3_file*, int *pResOut);
static int kvvfsFileControl(sqlite3_file*, int op, void *pArg);
static int kvvfsSectorSize(sqlite3_file*);
static int kvvfsDeviceCharacteristics(sqlite3_file*);

/*
** Methods for KVVfsVfs
*/
static int kvvfsOpen(sqlite3_vfs*, const char *, sqlite3_file*, int , int *);
static int kvvfsDelete(sqlite3_vfs*, const char *zName, int syncDir);
static int kvvfsAccess(sqlite3_vfs*, const char *zName, int flags, int *);
static int kvvfsFullPathname(sqlite3_vfs*, const char *zName, int, char *zOut);
static void *kvvfsDlOpen(sqlite3_vfs*, const char *zFilename);
static void kvvfsDlError(sqlite3_vfs*, int nByte, char *zErrMsg);
static void (*kvvfsDlSym(sqlite3_vfs *pVfs, void *p, const char*zSym))(void);
static void kvvfsDlClose(sqlite3_vfs*, void*);
static int kvvfsRandomness(sqlite3_vfs*, int nByte, char *zOut);
static int kvvfsSleep(sqlite3_vfs*, int microseconds);
static int kvvfsCurrentTime(sqlite3_vfs*, double*);
static int kvvfsGetLastError(sqlite3_vfs*, int, char *);
static int kvvfsCurrentTimeInt64(sqlite3_vfs*, sqlite3_int64*);

static KVVfsVfs kvvfs_vfs = {
  {
    1,                            /* iVersion */
    sizeof(KVVfsFile),            /* szOsFile */
    1024,                         /* mxPathname */
    0,                            /* pNext */
    "kvvfs",                      /* zName */
    0,                            /* pAppData */
    kvvfsOpen,                    /* xOpen */
    kvvfsDelete,                  /* xDelete */
    kvvfsAccess,                  /* xAccess */
    kvvfsFullPathname,            /* xFullPathname */
    kvvfsDlOpen,                  /* xDlOpen */
    0,                            /* xDlError */
    0,                            /* xDlSym */
    0,                            /* xDlClose */
    kvvfsRandomness,              /* xRandomness */
    kvvfsSleep,                   /* xSleep */
    kvvfsCurrentTime,             /* xCurrentTime */
    0,                            /* xGetLastError */
    kvvfsCurrentTimeInt64,        /* xCurrentTimeInt64 */
  },
  0,
  0
};

static sqlite3_io_methods kvvfs_io_methods = {
  1,                              /* iVersion */
  kvvfsClose,                     /* xClose */
  kvvfsRead,                      /* xRead */
  kvvfsWrite,                     /* xWrite */
  kvvfsTruncate,                  /* xTruncate */
  kvvfsSync,                      /* xSync */
  kvvfsFileSize,                  /* xFileSize */
  kvvfsLock,                      /* xLock */
  kvvfsUnlock,                    /* xUnlock */
  kvvfsCheckReservedLock,         /* xCheckReservedLock */
  kvvfsFileControl,               /* xFileControl */
  kvvfsSectorSize,                /* xSectorSize */
  kvvfsDeviceCharacteristics      /* xDeviceCharacteristics */
  0,                              /* xShmMap */
  0,                              /* xShmLock */
  0,                              /* xShmBarrier */
  0,                              /* xShmUnmap */
  0,                              /* xFetch */
  0                               /* xUnfetch */
};

/****** Storage subsystem **************************************************/

/* Allocate a new storage subsystem.
** Return NULL if OOM
*/
static KVStorage *kvstorageOpen(void){
  KVStorage *pStore;
  pStore = sqlite3_malloc64( sizeof(*pStore) );
  if( pStore==0 ) return 0;
  memset(pStore, 0, sizeof(*pStore));
  return pStore;
}

/* Free all resources associated with the storage subsystem */
static void kvstorageClose(KVStorage *pStore){
  sqlite3_free(pStore);
}

/* Expand the key name with an appropriate prefix and put the result
** in pStore->zKey[]
*/
static void kvstorageMakeKey(KVStorage *pStore, const char *zKey){
  sqlite3_snprintf(sizeof(pStore->zKey), pStore->zKey, "kvvfs-%s", zKey);
}

/* Write content into a key.  zKey is of limited size.  zData should be
** pure text.  In other words, zData has already been encoded.
**
** Return the number of errors.
*/
static int kvstorageWrite(
  KVStorage *pStore,
  const char *zKey,
  const char *zData
){
  FILE *fd;
  kvstorageMakeKey(pStore, zKey);
  fd = fopen(pStore->zKey, "wb");
  if( fd==0 ) return 1;
  if( fd ){
    fputs(zData, fd);
    fclose(fd);
  }
  return 0;
}

/* Delete a key
*/
static int kvstorageDelete(KVStorage *pStore, const char *zKey){
  kvstorageMakeKey(pStore, zKey);
  unlink(pStore->zKey);
  return 0;
}

/* Read the value associated with a key and put the result in the first
** nBuf bytes of zBuf[].  The value might be truncated if zBuf is not large
** enough to hold it all.  The value put into zBuf will always be zero
** terminated.
**
** Return the total number of bytes in the data, without truncation, and
** not counting the final zero terminator.   Return -1 if the key does
** not exist.
**
** If nBuf==0 then this routine simply returns the size of the data without
** actually reading it.
*/
static int kvstorageRead(
  KVStorage *pStore,
  const char *zKey,
  char *zBuf,
  int nBuf
){
  FILE *fd;
  struct stat buf;
  kvstorageMakeKey(pStore, zKey);
  if( access(pStore->zKey, R_OK)!=0
   || stat(pStore->zKey, &buf)!=0
   || !S_ISREG(buf.st_mode)
  ){
    return -1;
  }
  if( nBuf<0 ){
    return (int)buf.st_size;
  }else if( nBuf==1 ){
    zBuf[0] = 0;
    return (int)buf.st_size;
  }
  if( nBuf-1 > buf.st_size ){
    nBuf = buf.st_size + 1;
  }
  fd = fopen(pStore->zKey, "rb");
  if( fd==0 ) return -1;
  fread(zBuf, nBuf-1, 1, fd);
  fclose(fd);
  return nBuf-1;
}


/****** The main VFS code **************************************************/

/*
** Close an kvvfs-file.
*/
static int kvvfsClose(sqlite3_file *pProtoFile){
  KVVfsFile *pFile = (KVVfsFile *)pProtoFile;
  KVVfsVfs *pVfs = pFile->pVfs;

  if( pVfs->pFiles==pFile ){
    pVfs->pFiles = pFile->pNext;
    if( pVfs->pFiles==0 ){
      kvstorageClose(pVfs->pStore);
      pVfs->pStore = 0;
    }
  }else{
    KVVfsFile *pX = pVfs->pFiles;
    while( 1 ){
      assert( pX );
      if( pX->pNext==pFile ){
        pX->pNext = pFile->pNext;
        break;
      }
      pX = pX->pNext; 
    }
  }
  sqlite3_free(pFile->aData);
  sqlite3_free(pFile);
  return SQLITE_OK;
}

/*
** Encode binary into the text encoded used to persist on disk.
** The output text is stored in aOut[], which must be at least
** nData+1 bytes in length.
**
** Return the actual length of the encoded text, not counting the
** zero terminator at the end.
*/
static int kvvfsEncode(const char *aData, int nData, char *aOut){
  int i, j;
  const unsigned *a = (const unsigned char*)aData;
  for(i=j=0; i<nData; i++){
    unsigned char c = a[i];
    if( c!=0 ){
      aOut[j++] = "0123456789ABCDEF"[c>>4];
      aOut[j++] = "0123456789ABCDEF"[c&0xf];
    }else{
      /* A sequence of 1 or more zeros is stored as a little-endian
      ** base-26 number using a..z as the digits. So one zero is "b".
      ** Two zeros is "c". 25 zeros is "z", 26 zeros is "ba" and so forth.
      */
      int k;
      for(k=1; a[i+k]==0 && i+k<nData; k++){}
      i += k;
      while( k>0 ){
        aOut[j++] = 'a'+(k%26);
        k /= 26;
      }
    }
  }
  aOut[j] = 0;
  return j;
}

/* Convert hex to binary */
static char kvvfsHexToBinary(char c){
  if( c>='0' && c<='9' ) return c - '0';
  if( c>='a' && c<='f' ) return c - 'a' + 10;
  return 0;
}

/*
** Decode the text encoding back to binary.  The binary content is
** written into pOut, which must be at least nOut bytes in length.
*/
static int kvvfsDecode(const char *aIn, char *aOut, int nOut){
  char *aOut;
  int i, j, k;
  int c;
  i = 0;
  j = 0;
  while( (c = aIn[i])!=0 ){
    if( c>='a' ){
      int n = 0;
      while( c>='a' && c<='z' ){
        n = n*26 + c - 'a';
        c = aIn[++i];
      }
      if( j+n>nOut ) return -1;
      while( n-->0 ){
        aOut[j++] = 0;
      }
    }else{
      if( j>nOut ) return -1;
      aOut[j] = kvvfsHexToBinary(aIn[i])<<4;
      i++;
      aOut[j] += kvvfsHexToBinary(aIn[i]);
      i++;
    }
  }
  return j;
}

/*
** Read from the -journal file.
*/
static int kvvfsReadFromJournal(
  KVVfsFile *pFile, 
  void *zBuf, 
  int iAmt, 
  sqlite_int64 iOfst
){
  assert( pFile->isJournal );
  if( pFile->aJrnl==0 ){
    int szTxt = kvstorageRead(pFile->pVfs->pStore, "journal", 0, 0);
    char *aTxt;
    if( szTxt<=4 ){
      return SQLITE_IOERR;
    }
    aTxt = sqlite3_malloc64( szTxt+1 );
    if( aTxt==0 ) return SQLITE_NOMEM;
    kvstorageRead(pFile->pVfs->pStore, "journal", aTxt, szTxt+1);
    kvvfsDecodeJournal(pFile, aTxt, szTxt);
    sqlite3_free(aTxt);
    if( pFile->aData==0 ) return SQLITE_IOERR;
  }
  if( iOfst+iAmt>pFile->nJrnl ){
    return SQLITE_IOERR_SHORT_READ;
  }
  mcmcpy(zBuf, pFile->aJrnl+iOfst, iAmt);
  return SQLITE_OK;
}

/*
** Read from the database file.
*/
static int kvvfsReadFromDb(
  KVVfsFile *pFile, 
  void *zBuf, 
  int iAmt, 
  sqlite_int64 iOfst
){
  return SQLITE_IOERR;
}


/*
** Read data from an kvvfs-file.
*/
static int kvvfsRead(
  sqlite3_file *pProtoFile, 
  void *zBuf, 
  int iAmt, 
  sqlite_int64 iOfst
){
  int rc;
  KVVfsFile *pFile = (KVVfsFile *)pProtoFile;
  if( pFile->isJournal ){
    rc = kvvfsReadFromJournal(pFile,zBuf,iAmt,iOfst);
  }else{
    rc = kvvfsReadFromDb(pFile,zBuf,iAmt,iOfst);
  }
  return rc;
}

/*
** Write into the -journal file.
*/
static int kvvfsWriteToJournal(
  KVVfsFile *pFile, 
  const void *zBuf, 
  int iAmt, 
  sqlite_int64 iOfst
){
  return SQLITE_IOERR;
}

/*
** Read from the database file.
*/
static int kvvfsWriteToDb(
  KVVfsFile *pFile, 
  const void *zBuf, 
  int iAmt, 
  sqlite_int64 iOfst
){
  return SQLITE_IOERR;
}


/*
** Write data into the kvvfs-file.
*/
static int kvvfsWrite(
  sqlite3_file *pProtoFile, 
  const void *zBuf, 
  int iAmt, 
  sqlite_int64 iOfst
){
  int rc;
  KVVfsFile *pFile = (KVVfsFile *)pProtoFile;
  if( pFile->isJournal ){
    rc = kvvfsWriteToJournal(pFile,zBuf,iAmt,iOfst);
  }else{
    rc = kvvfsWriteToDb(pFile,zBuf,iAmt,iOfst);
  }
  return rc;
}

/*
** Truncate an kvvfs-file.
*/
static int kvvfsTruncate(sqlite3_file *pFile, sqlite_int64 size){
  int rc;
  KVVfsFile *pFile = (KVVfsFile *)pProtoFile;
  rc = SQLITE_IOERR;
  return rc;
}

/*
** Sync an kvvfs-file.
*/
static int kvvfsSync(sqlite3_file *pFile, int flags){
  int rc;
  KVVfsFile *pFile = (KVVfsFile *)pProtoFile;
  rc = SQLITE_IOERR;
  return rc;
}

/*
** Return the current file-size of an kvvfs-file.
*/
static int kvvfsFileSize(sqlite3_file *pFile, sqlite_int64 *pSize){
  int rc;
  KVVfsFile *pFile = (KVVfsFile *)pProtoFile;
  *pSize = 0;
  rc = SQLITE_IOERR;
  return rc;
}

/*
** Lock an kvvfs-file.
*/
static int kvvfsLock(sqlite3_file *pFile, int eLock){
  int rc;
  KVVfsFile *pFile = (KVVfsFile *)pProtoFile;
  rc = SQLITE_IOERR;
  return rc;
}

/*
** Unlock an kvvfs-file.
*/
static int kvvfsUnlock(sqlite3_file *pFile, int eLock){
  int rc;
  KVVfsFile *pFile = (KVVfsFile *)pProtoFile;
  rc = SQLITE_IOERR;
  return rc;
}

/*
** Check if another file-handle holds a RESERVED lock on an kvvfs-file.
*/
static int kvvfsCheckReservedLock(sqlite3_file *pFile, int *pResOut){
  int rc;
  KVVfsFile *pFile = (KVVfsFile *)pProtoFile;
  *pResOut = 0;
  rc = SQLITE_IOERR;
  return rc;
}

/*
** File control method. For custom operations on an kvvfs-file.
*/
static int kvvfsFileControl(sqlite3_file *pFile, int op, void *pArg){
  int rc;
  KVVfsFile *pFile = (KVVfsFile *)pProtoFile;
  rc = SQLITE_NOTFOUND;
  return rc;
}

/*
** Return the sector-size in bytes for an kvvfs-file.
*/
static int kvvfsSectorSize(sqlite3_file *pFile){
  return 4096;
}

/*
** Return the device characteristic flags supported by an kvvfs-file.
*/
static int kvvfsDeviceCharacteristics(sqlite3_file *pFile){
  return 0;
}


/*
** Open an kvvfs file handle.
*/
static int kvvfsOpen(
  sqlite3_vfs *pProtoVfs,
  const char *zName,
  sqlite3_file *pProtoFile,
  int flags,
  int *pOutFlags
){
  int rc;
  KVVfsFile *pFile = (KVVfsFile*)pProtoFile;
  KVVfsVfs *pVfs = (KVVfsVfs*)pProtoVfs;

  
  return rc;
}

/*
** Delete the file located at zPath. If the dirSync argument is true,
** ensure the file-system modifications are synced to disk before
** returning.
*/
static int kvvfsDelete(sqlite3_vfs *pVfs, const char *zPath, int dirSync){
  KVVfsVfs *p = (KVVfsVfs*)pVfs;
  if( sqlite3_strglob("*-journal",zPath)==0 ){
    kvstorageDelete(p->pStore, "journal");
  }
  return SQLITE_OK;
}

/*
** Test for access permissions. Return true if the requested permission
** is available, or false otherwise.
*/
static int kvvfsAccess(
  sqlite3_vfs *pVfs, 
  const char *zPath, 
  int flags, 
  int *pResOut
){
  *pResOut = 1;
  return SQLITE_OK;
}

/*
** Populate buffer zOut with the full canonical pathname corresponding
** to the pathname in zPath. zOut is guaranteed to point to a buffer
** of at least (INST_MAX_PATHNAME+1) bytes.
*/
static int kvvfsFullPathname(
  sqlite3_vfs *pVfs, 
  const char *zPath, 
  int nOut, 
  char *zOut
){
  size_t nPath = strlen(zPath);
  if( nOut<nPath+1 ) nPath = nOut - 1;
  memcpy(zOut, zPath, nPath);
  zPath[nPath] = 0;
  return SQLITE_OK;
}

/*
** Open the dynamic library located at zPath and return a handle.
*/
static void *kvvfsDlOpen(sqlite3_vfs *pVfs, const char *zPath){
  return 0;
}

/*
** Populate the buffer pointed to by zBufOut with nByte bytes of 
** random data.
*/
static int kvvfsRandomness(sqlite3_vfs *pVfs, int nByte, char *zBufOut){
  memset(zBufOut, 0, nByte);
  return nByte;
}

/*
** Sleep for nMicro microseconds. Return the number of microseconds 
** actually slept.
*/
static int kvvfsSleep(sqlite3_vfs *pVfs, int nMicro){
  return SQLITE_OK;
}

/*
** Return the current time as a Julian Day number in *pTimeOut.
*/
static int kvvfsCurrentTime(sqlite3_vfs *pVfs, double *pTimeOut){
  *pTimeOut = 2459829.13362986;
  return SQLITE_OK;
}
static int kvvfsCurrentTimeInt64(sqlite3_vfs *pVfs, sqlite3_int64 *pTimeOut){
  *pTimeOut = (sqlite3_int64)(2459829.13362986*86400000.0);
  return SQLITE_OK;
}

/*
** Register debugvfs as the default VFS for this process.
*/
int sqlite3_register_kvvfs(const char *zArg){
  return sqlite3_vfs_register(&kvvfs_vfs.base, 1);
}

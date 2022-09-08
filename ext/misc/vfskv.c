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


/* All information about the database */
struct KVVfsVfs {
  sqlite3_vfs base;               /* VFS methods */
  KVStorage *pStore;              /* Single command KV storage object */
  KVVfsFile *pFiles;              /* List of open KVVfsFile objects */
};

/* A single open file.  There are only two files represented by this
** VFS - the database and the rollback journal.
*/
struct KVVfsFile {
  sqlite3_file base;              /* IO methods */
  KVVfsVfs *pVfs;                 /* The VFS to which this file belongs */
  KVVfsFile *pNext;               /* Next in list of all files */
  int isJournal;                  /* True if this is a journal file */
  unsigned int nJrnl;             /* Space allocated for aJrnl[] */
  char *aJrnl;                    /* Journal content */
  int szPage;                     /* Last known page size */
  sqlite3_int64 szDb;             /* Database file size.  -1 means unknown */
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
      ** Two zeros is "c". 25 zeros is "z", 26 zeros is "ab", 27 is "bb",
      ** and so forth.
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
**
** The return value is the number of bytes actually written into aOut[].
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
      int mult = 1;
      while( c>='a' && c<='z' ){
        n += (c - 'a')*mult;
        mult *= 26;
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
** Decode a complete journal file.  Allocate space in pFile->aJrnl
** and store the decoding there.  Or leave pFile->aJrnl set to NULL
** if an error is encountered.
**
** The first few characters of the text encoding will be a
** base-26 number (digits a..z) that is the total number of bytes
** in the decoding.  Use this number to size the initial allocation.
*/
static void kvvfsDecodeJournal(
  KVVfsFile *pFile,      /* Store decoding in pFile->aJrnl */
  const char *zTxt,      /* Text encoding.  Zero-terminated */
  int nTxt               /* Bytes in zTxt, excluding zero terminator */
){
  unsigned int n;
  int c, i, mult;
  i = 0;
  mult = 1;
  while( (c = zTxt[i])>='a' && c<='z' ){
    n += (zTxt[i] - 'a')*mult;
    mult *= 26;
    i++;
  }
  sqlite3_free(pFile->aJrnl);
  pFile->aJrnl = sqlite3_malloc64( n );
  if( pFile->aJrnl==0 ){
    pFile->nJrnl = 0;
    return;
  }
  pFile->nJrnl = n;
  n = kvvfsDecode(zTxt+i, pFile->aJrnl, pFile->nJrnl);
  if( n<pFile->nJrnl ){
    sqlite3_free(pFile->aJrnl);
    pFile->aJrnl = 0;
    pFile->nJrnl = 0;
  }
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
  unsigned int pgno;
  KVVfsPage *pPage;
  int got;
  char zKey[30];
  char aData[131073];
  assert( pFile->szDb>=0 );
  assert( iOfst>=0 );
  assert( iAmt>=0 );
  if( (iOfst % iAmt)!=0 ){
    return SQLITE_IOERR_READ;
  }
  if( iAmt!=100 || iOfst!=0 ){
    if( (iAmt & (iAmt-1))!=0 || iAmt<512 || iAmt>65536 ){
      return SQLITE_IOERR_READ;
    }
    pFile->szPage = iAmt;
  }
  pgno = 1 + iOfst/iAmt;
  sqlite3_snprintf(sizeof(zKey), zKey, "pg-%u", pgno)
  got = kvstorageRead(pFile->pVfs->pStore, zKey, aData, sizeof(aData)-1);
  if( got<0 ){
    return SQLITE_IOERR_READ;
  }
  aData[got] = 0;
  n = kvvfsDecode(aData, zBuf, iAmt);
  if( n<iAmt ){
    memset(zBuf+n, 0, iAmt-n);
    return SQLITE_IOERR_SHORT_READ;
  }
  return SQLITE_OK;
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
** Read or write the "sz" element, containing the database file size.
*/
static sqlite3_int64 kvvfsReadFileSize(KVVfsFile *pFile){
  char zData[50];
  zData[0] = 0;
  kvstorageRead(pFile->pVfs->pStore, "sz", zData, sizeof(zData)-1);
  return strtoll(zData, 0, 0);
}
static void kvvfsWriteFileSize(KVVfsFile *pFile, sqlite3_int64 sz){
  char zData[50];
  sqlite3_snprintf(sizeof(zData), zData, "%lld", sz);
  kvstorageWrite(pFile->pVfs->pStore, "sz", zData);
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
  sqlite3_int64 iEnd = iOfst+iAmt;
  if( iEnd>=0x10000000 ) return SQLITE_FULL;
  if( pFile->aJrnl==0 || pFile->nJrnl<iEnd ){
    char *aNew = sqlite3_realloc(pFile->aJrnl, iEnd);
    if( aNew==0 ){
      return SQLITE_IOERR_NOMEM;
    }
    pFile->aJrnl = aNew;
    if( pFile->nJrnl<iOfst ){
      memset(pFile->aJrnl+pFile->nJrnl, 0, iOfst-pFile->nJrnl);
    }
    pFile->nJrnl = iEnd;
  }
  memcpy(pFile->aJrnl+iOfst, zBuf, iAmt);
  return SQLITE_OK;
}

/*
** Write into the database file.
*/
static int kvvfsWriteToDb(
  KVVfsFile *pFile, 
  const void *zBuf, 
  int iAmt, 
  sqlite_int64 iOfst
){
  unsigned int pgno;
  char zKey[30];
  char aData[131073];
  assert( iAmt>=512 && iAmt<=65536 );
  assert( (iAmt & (iAmt-1))==0 );
  pgno = 1 + iOfst/iAmt;
  sqlite3_snprintf(sizeof(zKey), zKey, "pg-%u", pgno)
  nData = kvvfsEncode(zBuf, iAmt, aData);
  kvstorageWrite(pFile->pVfs->pStore, zKey, aData);
  if( iOfst+iAmt > pFile->szDb ){
    pFile->szDb = iOfst + iAmt;
  }
  return SQLITE_OK;
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
static int kvvfsTruncate(sqlite3_file *pProtoFile, sqlite_int64 size){
  KVVfsFile *pFile = (KVVfsFile *)pProtoFile;
  if( pFile->isJournal ){
    assert( size==0 );
    kvstorageDelete(pFile->pVfs->pStore, "journal");
    sqlite3_free(pFile->aData);
    pFile->aData = 0;
    pFile->nData = 0;
    return SQLITE_OK;
  }
  if( pFile->szDb>size
   && pFile->szPage>0 
   && (size % pFile->szPage)==0
  ){
    char zKey[50];
    unsigned int pgno, pgnoMax;
    pgno = 1 + size/pFile->szPage;
    pgnoMax = 2 + pFile->szDb/pFile->szPage;
    while( pgno<=pgnoMax ){
      sqlite3_snprintf(sizeof(zKey), zKey, "pg-%u", pgno);
      kvstorageDelete(pFile->pVfs->pStore, zKey);
      pgno++;
    }
    pFile->szDb = size;
    kvvfsWriteFileSize(pFile->pVfs->pStore, size);
    return SQLITE_OK;
  }
  return SQLITE_IOERR;
}

/*
** Sync an kvvfs-file.
*/
static int kvvfsSync(sqlite3_file *pProtoFile, int flags){
  int i, k, n;
  KVVfsFile *pFile = (KVVfsFile *)pProtoFile;
  char *zOut;
  if( !pFile->isJournal ){
    if( pFile->szDb>0 ){
      kvvfsWriteFileSize(pFile, pFile->szDb);
    }
    return SQLITE_OK;
  }
  if( pFile->nJrnl<=0 ){
     return kvvfsTruncate(pProtoFile, 0);
  }
  zOut = sqlite3_malloc64( pFile->nJrnl*2 + 50 );
  if( zOut==0 ){
    return SQLITE_IOERR_NOMEM;
  }
  n = pFile->nJrnl;
  i = 0;
  do{
    zOut[i++] = 'a' + (n%26);
    n /= 26;
  }while( n>0 );
  kvvfsEncode(pFile->aJrnl, pFile->nJrnl, &zOut[i]);
  kvstorageWrite(pFile->pVfs->pStore, "journal", zOut);
  sqlite3_free(zOut);
  return rc;
}

/*
** Return the current file-size of an kvvfs-file.
*/
static int kvvfsFileSize(sqlite3_file *pFile, sqlite_int64 *pSize){
  KVVfsFile *pFile = (KVVfsFile *)pProtoFile;
  if( pFile->isJournal ){
    *pSize = pFile->nJrnl;
  }else{
    *pSize = pFile->szDb;
  }
  return SQLITE_OK;
}

/*
** Lock an kvvfs-file.
*/
static int kvvfsLock(sqlite3_file *pFile, int eLock){
  KVVfsFile *pFile = (KVVfsFile *)pProtoFile;
  assert( !pFile->isJournal );
  if( eLock!=SQLITE_LOCK_NONE ){
    pFile->szDb = kvvfsReadFileSize(pFile);
  }
  return SQLITE_OK;
}

/*
** Unlock an kvvfs-file.
*/
static int kvvfsUnlock(sqlite3_file *pFile, int eLock){
  KVVfsFile *pFile = (KVVfsFile *)pProtoFile;
  assert( !pFile->isJournal );
  if( eLock==SQLITE_LOCK_NONE ){
    pFile->szDb = -1;
  }
  return SQLITE_OK;
}

/*
** Check if another file-handle holds a RESERVED lock on an kvvfs-file.
*/
static int kvvfsCheckReservedLock(sqlite3_file *pFile, int *pResOut){
  int rc;
  KVVfsFile *pFile = (KVVfsFile *)pProtoFile;
  *pResOut = 0;
  rc = SQLITE_OK;
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
  return 512;
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
  KVVfsFile *pFile = (KVVfsFile*)pProtoFile;
  KVVfsVfs *pVfs = (KVVfsVfs*)pProtoVfs;
  pFile->aJrnl = 0;
  pFile->nJrnl = 0;
  pFile->isJournal = sqlite3_strglob("*-journal", zName)==0;
  pFile->szPage = -1;
  pFile->szDb = -1;
  
  return SQLITE_OK;
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
  sqlite3_vfs *pProtoVfs, 
  const char *zPath, 
  int flags, 
  int *pResOut
){
  KVVfsVfs *pVfs = (KVVfsVfs*)pProtoVfs;
  if( sqlite3_strglob("*-journal", zPath)==0 ){
    *pResOut = kvstorageRead(pVfs->pStore, "journal", 0, 0)>0;
  }else{
    *pResOut = 1;
  }
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

/*
** 2004 May 22
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
** This file contains code that is specific to Unix systems. It is used
** for testing SQLite only.
*/
#include "os.h"          /* Must be first to enable large file support */
#if OS_TEST              /* This file is used for the test backend only */
#include "sqliteInt.h"

#define sqlite3OsOpenReadWrite     sqlite3RealOpenReadWrite
#define sqlite3OsOpenExclusive     sqlite3RealOpenExclusive
#define sqlite3OsOpenReadOnly      sqlite3RealOpenReadOnly
#define sqlite3OsOpenDirectory     sqlite3RealOpenDirectory
#define sqlite3OsClose             sqlite3RealClose
#define sqlite3OsRead              sqlite3RealRead
#define sqlite3OsWrite             sqlite3RealWrite
#define sqlite3OsSeek              sqlite3RealSeek
#define sqlite3OsSync              sqlite3RealSync
#define sqlite3OsTruncate          sqlite3RealTruncate
#define sqlite3OsFileSize          sqlite3RealFileSize
#define sqlite3OsFileModTime       sqlite3RealFileModTime
#define sqlite3OsLock              sqlite3RealLock
#define sqlite3OsUnlock            sqlite3RealUnlock
#define sqlite3OsCheckReservedLock sqlite3RealCheckReservedLock

#define OsFile OsRealFile
#define OS_UNIX 1
#include "os_unix.c"
#undef OS_UNIX
#undef OsFile

#undef sqlite3OsOpenReadWrite     
#undef sqlite3OsOpenExclusive     
#undef sqlite3OsOpenReadOnly      
#undef sqlite3OsOpenDirectory     
#undef sqlite3OsClose             
#undef sqlite3OsRead              
#undef sqlite3OsWrite             
#undef sqlite3OsSeek              
#undef sqlite3OsSync              
#undef sqlite3OsTruncate          
#undef sqlite3OsFileSize          
#undef sqlite3OsFileModTime       
#undef sqlite3OsLock              
#undef sqlite3OsUnlock            
#undef sqlite3OsCheckReservedLock 

#define BLOCKSIZE 512
#define BLOCK_OFFSET(x) ((x) * BLOCKSIZE)


/*
** The crash-seed. Accessed via functions crashseed() and
** sqlite3SetCrashseed().
*/
static int crashseed_var = 0;

/*
** This function is used to set the value of the 'crash-seed' integer.
**
** If the crash-seed is 0, the default value, then whenever sqlite3OsSync()
** or sqlite3OsClose() is called, the write cache is written to disk before
** the os_unix.c Sync() or Close() function is called.
**
** If the crash-seed is non-zero, then it is used to determine a subset of
** the write-cache to actually write to disk before calling Sync() or
** Close() in os_unix.c. The actual subset of writes selected is not
** significant, except that it is constant for a given value of the
** crash-seed and cache contents. Before returning, exit(-1) is invoked.
*/
void sqlite3SetCrashseed(int seed){
  sqlite3OsEnterMutex();
  crashseed_var = seed;
  sqlite3OsLeaveMutex();
}

/*
** Retrieve the current value of the crash-seed.
*/
static int crashseed(){
  int i;
  sqlite3OsEnterMutex();
  i = crashseed_var;
  sqlite3OsLeaveMutex();
  return i;
}

/*
** Initialise the os_test.c specific fields of pFile.
*/
static void initFile(OsFile *pFile){
  pFile->nMaxWrite = 0; 
  pFile->nBlk = 0; 
  pFile->apBlk = 0; 
}

/*
** Return the current seek offset from the start of the file. This
** is unix-only code.
*/
static off_t osTell(OsFile *pFile){
  return lseek(pFile->fd.h, 0, SEEK_CUR);
}

/*
** Load block 'blk' into the cache of pFile.
*/
static int cacheBlock(OsFile *pFile, int blk){
  if( blk>=pFile->nBlk ){
    int n = ((pFile->nBlk * 2) + 100 + blk);
    pFile->apBlk = (u8 **)sqliteRealloc(pFile->apBlk, n * sizeof(u8*));
    if( !pFile->apBlk ) return SQLITE_NOMEM;
    pFile->nBlk = n;
  }

  if( !pFile->apBlk[blk] ){
    off_t filesize;
    int rc;

    u8 *p = sqliteMalloc(BLOCKSIZE);
    if( !p ) return SQLITE_NOMEM;
    pFile->apBlk[blk] = p;

    rc = sqlite3RealFileSize(&pFile->fd, &filesize);
    if( rc!=SQLITE_OK ) return rc;

    if( BLOCK_OFFSET(blk)<filesize ){
      int len = BLOCKSIZE;
      rc = sqlite3RealSeek(&pFile->fd, blk*BLOCKSIZE);
      if( BLOCK_OFFSET(blk+1)>filesize ){
        len = filesize - BLOCK_OFFSET(blk);
      }
      if( rc!=SQLITE_OK ) return rc;
      rc = sqlite3RealRead(&pFile->fd, p, len);
      if( rc!=SQLITE_OK ) return rc;
    }
  }

  return SQLITE_OK;
}

/*
** Write the cache of pFile to disk. If crash is non-zero, randomly
** skip blocks when writing. The cache is deleted before returning.
*/
static int writeCache2(OsFile *pFile, int crash){
  int i;
  int nMax = pFile->nMaxWrite;
  off_t offset;
  int rc = SQLITE_OK;

  offset = osTell(pFile);
  for(i=0; i<pFile->nBlk; i++){
    u8 *p = pFile->apBlk[i];
    if( p ){
      int skip = 0;
      if( crash ){
        char random;
        sqlite3Randomness(1, &random);
        if( random & 0x01 ) skip = 1;
      }

      if( rc==SQLITE_OK ){
        rc = sqlite3RealSeek(&pFile->fd, BLOCK_OFFSET(i));
      }
      if( rc==SQLITE_OK && !skip ){
        int len = BLOCKSIZE;
        if( BLOCK_OFFSET(i+1)>nMax ){
          len = nMax-BLOCK_OFFSET(i);
        }
        rc = sqlite3RealWrite(&pFile->fd, p, len);
      }
      sqliteFree(p);
    }
  }
  sqliteFree(pFile->apBlk);
  pFile->nBlk = 0;
  pFile->apBlk = 0;
  pFile->nMaxWrite = 0;

  if( rc==SQLITE_OK ){
    rc = sqlite3RealSeek(&pFile->fd, offset);
  }
  return rc;
}

/*
** Write the cache to disk.
*/
static int writeCache(OsFile *pFile){
  if( crashseed() ){
    /* FIX ME: writeCache2() should be called on all open files
    ** here. */
    writeCache2(pFile, 1);
    exit(-1);
  }else{
    return writeCache2(pFile, 0);
  }
}

/*
** Close the file.
*/
int sqlite3OsClose(OsFile *id){
  if( !id->fd.isOpen ) return SQLITE_OK;
  writeCache(id);
  sqlite3RealClose(&id->fd);
  return SQLITE_OK;
}

int sqlite3OsRead(OsFile *id, void *pBuf, int amt){
  off_t offset;       /* The current offset from the start of the file */
  off_t end;          /* The byte just past the last byte read */
  int blk;            /* Block number the read starts on */
  int i;
  u8 *zCsr;
  int rc = SQLITE_OK;

  offset = osTell(id);
  end = offset+amt;
  blk = (offset/BLOCKSIZE);

  zCsr = (u8 *)pBuf;
  for(i=blk; i*BLOCKSIZE<end; i++){
    int off = 0;
    int len = 0;


    if( BLOCK_OFFSET(i) < offset ){
      off = offset-BLOCK_OFFSET(i);
    }
    len = BLOCKSIZE - off;
    if( BLOCK_OFFSET(i+1) > end ){
      len = len - (BLOCK_OFFSET(i+1)-end);
    }

    if( i<id->nBlk && id->apBlk[i]){
      u8 *pBlk = id->apBlk[i];
      memcpy(zCsr, &pBlk[off], len);
    }else{
      rc = sqlite3RealSeek(&id->fd, BLOCK_OFFSET(i) + off);
      if( rc!=SQLITE_OK ) return rc;
      rc = sqlite3RealRead(&id->fd, zCsr, len);
      if( rc!=SQLITE_OK ) return rc;
    }

    zCsr += len;
  }
  assert( zCsr==&((u8 *)pBuf)[amt] );

  rc = sqlite3RealSeek(&id->fd, end);
  return rc;
}

int sqlite3OsWrite(OsFile *id, const void *pBuf, int amt){
  off_t offset;       /* The current offset from the start of the file */
  off_t end;          /* The byte just past the last byte written */
  int blk;            /* Block number the write starts on */
  int i;
  const u8 *zCsr;
  int rc = SQLITE_OK;

  offset = osTell(id);
  end = offset+amt;
  blk = (offset/BLOCKSIZE);

  zCsr = (u8 *)pBuf;
  for(i=blk; i*BLOCKSIZE<end; i++){
    u8 *pBlk;
    int off = 0;
    int len = 0;

    /* Make sure the block is in the cache */
    rc = cacheBlock(id, i);
    if( rc!=SQLITE_OK ) return rc;

    /* Write into the cache */
    pBlk = id->apBlk[i];
    assert( pBlk );

    if( BLOCK_OFFSET(i) < offset ){
      off = offset-BLOCK_OFFSET(i);
    }
    len = BLOCKSIZE - off;
    if( BLOCK_OFFSET(i+1) > end ){
      len = len - (BLOCK_OFFSET(i+1)-end);
    }
    memcpy(&pBlk[off], zCsr, len);
    zCsr += len;
  }
  if( id->nMaxWrite<end ){
    id->nMaxWrite = end;
  }
  assert( zCsr==&((u8 *)pBuf)[amt] );

  rc = sqlite3RealSeek(&id->fd, end);
  return rc;
}

/*
** Sync the file. First flush the write-cache to disk, then call the
** real sync() function.
*/
int sqlite3OsSync(OsFile *id){
  int rc = writeCache(id);
  if( rc!=SQLITE_OK ) return rc;
  rc = sqlite3RealSync(&id->fd);
  return rc;
}

/*
** Truncate the file. Set the internal OsFile.nMaxWrite variable to the new
** file size to ensure that nothing in the write-cache past this point
** is written to disk.
*/
int sqlite3OsTruncate(OsFile *id, off_t nByte){
  id->nMaxWrite = nByte;
  return sqlite3RealTruncate(&id->fd, nByte);
}

/*
** Return the size of the file. If the cache contains a write that extended
** the file, then return this size instead of the on-disk size.
*/
int sqlite3OsFileSize(OsFile *id, off_t *pSize){
  int rc = sqlite3RealFileSize(&id->fd, pSize);
  if( rc==SQLITE_OK && pSize && *pSize<id->nMaxWrite ){
    *pSize = id->nMaxWrite;
  }
  return rc;
}

/*
** The three functions used to open files. All that is required is to
** initialise the os_test.c specific fields and then call the corresponding
** os_unix.c function to really open the file.
*/
int sqlite3OsOpenReadWrite(const char *zFilename, OsFile *id, int *pReadonly){
  initFile(id);
  return sqlite3RealOpenReadWrite(zFilename, &id->fd, pReadonly);
}
int sqlite3OsOpenExclusive(const char *zFilename, OsFile *id, int delFlag){
  initFile(id);
  return sqlite3RealOpenExclusive(zFilename, &id->fd, delFlag);
}
int sqlite3OsOpenReadOnly(const char *zFilename, OsFile *id){
  initFile(id);
  return sqlite3RealOpenReadOnly(zFilename, &id->fd);
}

/*
** These six function calls are passed straight through to the os_unix.c
** backend.
*/
int sqlite3OsSeek(OsFile *id, off_t offset){
  return sqlite3RealSeek(&id->fd, offset);
}
int sqlite3OsCheckReservedLock(OsFile *id){
  return sqlite3RealCheckReservedLock(&id->fd);
}
int sqlite3OsLock(OsFile *id, int locktype){
  return sqlite3RealLock(&id->fd, locktype);
}
int sqlite3OsUnlock(OsFile *id, int locktype){
  return sqlite3RealUnlock(&id->fd, locktype);
}
int sqlite3OsFileModTime(OsFile *id, double *prNow){
  return sqlite3RealFileModTime(&id->fd, prNow);
}
int sqlite3OsOpenDirectory(const char *zDirname, OsFile *id){
  return sqlite3RealOpenDirectory(zDirname, &id->fd);
}

#endif /* OS_TEST */

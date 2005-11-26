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
** This file contains code that modified the OS layer in order to simulate
** the effect on the database file of an OS crash or power failure.  This
** is used to test the ability of SQLite to recover from those situations.
*/
#if SQLITE_TEST          /* This file is used for the testing only */
#include "sqliteInt.h"
#include "os.h"
#include "tcl.h"

/*
** A copy of the original sqlite3Io structure
*/
static struct sqlite3IoVtbl origIo;

/*
** The pAux part of OsFile points to this structure.
*/
typedef struct OsTestFile OsTestFile;
struct OsTestFile {
  u8 **apBlk;         /* Array of blocks that have been written to. */
  int nBlk;           /* Size of apBlock. */
  int nMaxWrite;      /* Largest offset written to. */
  char *zName;        /* File name */
  OsFile *pBase;      /* Base class */
  OsTestFile *pNext;  /* Next in a list of them all */
};

/*
** Size of a simulated disk block
*/
#define BLOCKSIZE 512
#define BLOCK_OFFSET(x) ((x) * BLOCKSIZE)


/*
** The following variables control when a simulated crash occurs.
**
** If iCrashDelay is non-zero, then zCrashFile contains (full path) name of
** a file that SQLite will call sqlite3OsSync() on. Each time this happens
** iCrashDelay is decremented. If iCrashDelay is zero after being
** decremented, a "crash" occurs during the sync() operation.
**
** In other words, a crash occurs the iCrashDelay'th time zCrashFile is
** synced.
*/
static int iCrashDelay = 0;
static char zCrashFile[500];

/*
** Set the value of the two crash parameters.
*/
static void setCrashParams(int iDelay, char const *zFile){
  sqlite3OsEnterMutex();
  assert( strlen(zFile)<sizeof(zCrashFile) );
  strcpy(zCrashFile, zFile);
  iCrashDelay = iDelay;
  sqlite3OsLeaveMutex();
}

/*
** File zPath is being sync()ed. Return non-zero if this should
** cause a crash.
*/
static int crashRequired(char const *zPath){
  int r;
  int n;
  sqlite3OsEnterMutex();
  n = strlen(zCrashFile);
  if( zCrashFile[n-1]=='*' ){
    n--;
  }else if( strlen(zPath)>n ){
    n = strlen(zPath);
  }
  r = 0;
  if( iCrashDelay>0 && strncmp(zPath, zCrashFile, n)==0 ){
    iCrashDelay--;
    if( iCrashDelay<=0 ){
      r = 1;
    }
  }
  sqlite3OsLeaveMutex();
  return r;
}

/*
** A list of all open files.
*/
static OsTestFile *pAllFiles = 0;

/*
** Initialise the os_test.c specific fields of pFile.
*/
static void initFile(OsFile *id, char const *zName){
  OsTestFile *pFile = sqliteMalloc(sizeof(OsTestFile) + strlen(zName)+1);
  id->pAux = pFile;
  pFile->nMaxWrite = 0; 
  pFile->nBlk = 0; 
  pFile->apBlk = 0; 
  pFile->zName = (char *)(&pFile[1]);
  strcpy(pFile->zName, zName);
  pFile->pBase = id;
  pFile->pNext = pAllFiles;
  pAllFiles = pFile;
}

/*
** Undo the work done by initFile. Delete the OsTestFile structure
** and unlink the structure from the pAllFiles list.
*/
static void closeFile(OsFile *id){
  OsTestFile *pFile = (OsTestFile*)id->pAux;
  if( pFile==pAllFiles ){
    pAllFiles = pFile->pNext;
  }else{
    OsTestFile *p;
    for(p=pAllFiles; p->pNext!=pFile; p=p->pNext ){
      assert( p );
    }
    p->pNext = pFile->pNext;
  }
  sqliteFree(pFile);
}

/*
** Return the current seek offset from the start of the file. This
** is unix-only code.
*/
static i64 osTell(OsTestFile *pFile){
  return lseek(pFile->pBase->h, 0, SEEK_CUR);
}

/*
** Load block 'blk' into the cache of pFile.
*/
static int cacheBlock(OsTestFile *pFile, int blk){
  if( blk>=pFile->nBlk ){
    int n = ((pFile->nBlk * 2) + 100 + blk);
    /* if( pFile->nBlk==0 ){ printf("DIRTY %s\n", pFile->zName); } */
    pFile->apBlk = (u8 **)sqliteRealloc(pFile->apBlk, n * sizeof(u8*));
    if( !pFile->apBlk ) return SQLITE_NOMEM;
    memset(&pFile->apBlk[pFile->nBlk], 0, (n - pFile->nBlk)*sizeof(u8*));
    pFile->nBlk = n;
  }

  if( !pFile->apBlk[blk] ){
    i64 filesize;
    int rc;

    u8 *p = sqliteMalloc(BLOCKSIZE);
    if( !p ) return SQLITE_NOMEM;
    pFile->apBlk[blk] = p;

    rc = origIo.xFileSize(pFile->pBase, &filesize);
    if( rc!=SQLITE_OK ) return rc;

    if( BLOCK_OFFSET(blk)<filesize ){
      int len = BLOCKSIZE;
      rc = origIo.xSeek(pFile->pBase, blk*BLOCKSIZE);
      if( BLOCK_OFFSET(blk+1)>filesize ){
        len = filesize - BLOCK_OFFSET(blk);
      }
      if( rc!=SQLITE_OK ) return rc;
      rc = origIo.xRead(pFile->pBase, p, len);
      if( rc!=SQLITE_OK ) return rc;
    }
  }

  return SQLITE_OK;
}

/*
** Write the cache of pFile to disk. If crash is non-zero, randomly
** skip blocks when writing. The cache is deleted before returning.
*/
static int writeCache2(OsTestFile *pFile, int crash){
  int i;
  int nMax = pFile->nMaxWrite;
  i64 offset;
  int rc = SQLITE_OK;

  offset = osTell(pFile);
  for(i=0; i<pFile->nBlk; i++){
    u8 *p = pFile->apBlk[i];
    if( p ){
      int skip = 0;
      int trash = 0;
      if( crash ){
        char random;
        sqlite3Randomness(1, &random);
        if( random & 0x01 ){
          if( random & 0x02 ){
            trash = 1;
#ifdef TRACE_WRITECACHE
printf("Trashing block %d of %s\n", i, pFile->zName); 
#endif
          }else{
            skip = 1;
#ifdef TRACE_WRITECACHE
printf("Skiping block %d of %s\n", i, pFile->zName); 
#endif
          }
        }else{
#ifdef TRACE_WRITECACHE
printf("Writing block %d of %s\n", i, pFile->zName); 
#endif
        }
      }
      if( rc==SQLITE_OK ){
        rc = origIo.xSeek(pFile->pBase, BLOCK_OFFSET(i));
      }
      if( rc==SQLITE_OK && !skip ){
        int len = BLOCKSIZE;
        if( BLOCK_OFFSET(i+1)>nMax ){
          len = nMax-BLOCK_OFFSET(i);
        }
        if( len>0 ){
          if( trash ){
            sqlite3Randomness(len, p);
          }
          rc = origIo.xWrite(pFile->pBase, p, len);
        }
      }
      sqliteFree(p);
    }
  }
  sqliteFree(pFile->apBlk);
  pFile->nBlk = 0;
  pFile->apBlk = 0;
  pFile->nMaxWrite = 0;

  if( rc==SQLITE_OK ){
    rc = origIo.xSeek(pFile->pBase, offset);
  }
  return rc;
}

/*
** Write the cache to disk.
*/
static int writeCache(OsTestFile *pFile){
  if( pFile->apBlk ){
    int c = crashRequired(pFile->zName);
    if( c ){
      OsTestFile *p;
#ifdef TRACE_WRITECACHE
      printf("\nCrash during sync of %s\n", pFile->zName);
#endif
      for(p=pAllFiles; p; p=p->pNext){
        writeCache2(p, 1);
      }
      exit(-1);
    }else{
      return writeCache2(pFile, 0);
    }
  }
  return SQLITE_OK;
}

/*
** Close the file.
*/
static int crashClose(OsFile *id){
  if( id->pAux ) return SQLITE_OK;
  if( id->isOpen ){
    /* printf("CLOSE %s (%d blocks)\n", (*id)->zName, (*id)->nBlk); */
    writeCache((OsTestFile*)id->pAux);
    origIo.xClose(id);
  }
  closeFile(id);
  return SQLITE_OK;
}

static int crashRead(OsFile *id, void *pBuf, int amt){
  i64 offset;       /* The current offset from the start of the file */
  i64 end;          /* The byte just past the last byte read */
  int blk;            /* Block number the read starts on */
  int i;
  u8 *zCsr;
  int rc = SQLITE_OK;
  OsTestFile *pFile = (OsTestFile*)id->pAux;

  offset = osTell(pFile);
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

    if( i<pFile->nBlk && pFile->apBlk[i]){
      u8 *pBlk = pFile->apBlk[i];
      memcpy(zCsr, &pBlk[off], len);
    }else{
      rc = origIo.xSeek(id, BLOCK_OFFSET(i) + off);
      if( rc!=SQLITE_OK ) return rc;
      rc = origIo.xRead(id, zCsr, len);
      if( rc!=SQLITE_OK ) return rc;
    }

    zCsr += len;
  }
  assert( zCsr==&((u8 *)pBuf)[amt] );

  rc = origIo.xSeek(id, end);
  return rc;
}

static int crashWrite(OsFile *id, const void *pBuf, int amt){
  i64 offset;       /* The current offset from the start of the file */
  i64 end;          /* The byte just past the last byte written */
  int blk;            /* Block number the write starts on */
  int i;
  const u8 *zCsr;
  int rc = SQLITE_OK;
  OsTestFile *pFile =  (OsTestFile*)id->pAux;

  offset = osTell(pFile);
  end = offset+amt;
  blk = (offset/BLOCKSIZE);

  zCsr = (u8 *)pBuf;
  for(i=blk; i*BLOCKSIZE<end; i++){
    u8 *pBlk;
    int off = 0;
    int len = 0;

    /* Make sure the block is in the cache */
    rc = cacheBlock(pFile, i);
    if( rc!=SQLITE_OK ) return rc;

    /* Write into the cache */
    pBlk = pFile->apBlk[i];
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
  if( pFile->nMaxWrite<end ){
    pFile->nMaxWrite = end;
  }
  assert( zCsr==&((u8 *)pBuf)[amt] );

  rc = origIo.xSeek(id, end);
  return rc;
}

/*
** Sync the file. First flush the write-cache to disk, then call the
** real sync() function.
*/
static int crashSync(OsFile *id, int dataOnly){
  int rc;
  /* printf("SYNC %s (%d blocks)\n", (*id)->zName, (*id)->nBlk); */
  rc = writeCache((OsTestFile*)id->pAux);
  if( rc!=SQLITE_OK ) return rc;
  rc = origIo.xSync(id, dataOnly);
  return rc;
}

/*
** Truncate the file. Set the internal OsFile.nMaxWrite variable to the new
** file size to ensure that nothing in the write-cache past this point
** is written to disk.
*/
static int crashTruncate(OsFile *id, i64 nByte){
  OsTestFile *pFile = (OsTestFile*)id->pAux;
  pFile->nMaxWrite = nByte;
  return origIo.xTruncate(id, nByte);
}

/*
** Return the size of the file. If the cache contains a write that extended
** the file, then return this size instead of the on-disk size.
*/
static int crashFileSize(OsFile *id, i64 *pSize){
  int rc = origIo.xFileSize(id, pSize);
  OsTestFile *pFile = (OsTestFile*)id->pAux;
  if( rc==SQLITE_OK && pSize && *pSize<pFile->nMaxWrite ){
    *pSize = pFile->nMaxWrite;
  }
  return rc;
}

/*
** The three functions used to open files. All that is required is to
** initialise the os_test.c specific fields and then call the corresponding
** os_unix.c function to really open the file.
*/
static int crashOpenReadWrite(const char *zFilename, OsFile *id, int *pRdonly){
  initFile(id, zFilename);
  return origIo.xOpenReadWrite(zFilename, id, pRdonly);
}
static int crashOpenExclusive(const char *zFilename, OsFile *id, int delFlag){
  initFile(id, zFilename);
  return origIo.xOpenExclusive(zFilename, id, delFlag);
}
static int crashOpenReadOnly(const char *zFilename, OsFile *id){
  initFile(id, zFilename);
  return origIo.xOpenReadOnly(zFilename, id);
}

/*
** tclcmd:   sqlite_crashparams DELAY CRASHFILE
**
** This procedure implements a TCL command that enables crash testing
** in testfixture.  Once enabled, crash testing cannot be disabled.
*/
static int crashParamsObjCmd(
  void * clientData,
  Tcl_Interp *interp,
  int objc,
  Tcl_Obj *CONST objv[]
){
  int delay;
  const char *zFile;
  int nFile;
  if( objc!=3 ){
    Tcl_WrongNumArgs(interp, 1, objv, "DELAY CRASHFILE");
    return TCL_ERROR;
  }
  if( Tcl_GetIntFromObj(interp, objv[1], &delay) ) return TCL_ERROR;
  zFile = Tcl_GetStringFromObj(objv[2], &nFile);
  if( nFile>=sizeof(zCrashFile)-1 ){
    Tcl_AppendResult(interp, "crash file name too big", 0);
    return TCL_ERROR;
  }
  setCrashParams(delay, zFile);
  origIo = sqlite3Io;
  sqlite3Io.xRead = crashRead;
  sqlite3Io.xWrite = crashWrite;
  sqlite3Io.xClose = crashClose;
  sqlite3Io.xSync = crashSync;
  sqlite3Io.xTruncate = crashTruncate;
  sqlite3Io.xFileSize = crashFileSize;
  sqlite3Io.xOpenReadWrite = crashOpenReadWrite;
  sqlite3Io.xOpenExclusive = crashOpenExclusive;
  sqlite3Io.xOpenReadOnly = crashOpenReadOnly;
  return TCL_OK;
}

/*
** This procedure registers the TCL procedures defined in this file.
*/
int Sqlitetest6_Init(Tcl_Interp *interp){
  Tcl_CreateObjCommand(interp, "sqlite3_crashparams", crashParamsObjCmd, 0, 0);
  return TCL_OK;
}

#endif /* SQLITE_TEST */

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

#ifndef SQLITE_OMIT_DISKIO  /* This file is a no-op if disk I/O is disabled */

/*
** crashFile is a subclass of OsFile that is taylored for the
** crash test module.
*/
typedef struct crashFile crashFile;
struct crashFile {
  IoMethod const *pMethod; /* Must be first */
  u8 **apBlk;              /* Array of blocks that have been written to. */
  int nBlk;                /* Size of apBlock. */
  i64 offset;              /* Next character to be read from the file */
  int nMaxWrite;           /* Largest offset written to. */
  char *zName;             /* File name */
  OsFile *pBase;           /* The real file */
  crashFile *pNext;        /* Next in a list of them all */
};

/*
** Size of a simulated disk block. Default is 512 bytes.
*/
static int BLOCKSIZE = 512;
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
** A list of all open files.
*/
static crashFile *pAllFiles = 0;

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
** Set the value of the simulated disk block size.
*/
static void setBlocksize(int iBlockSize){
  sqlite3OsEnterMutex();
  assert( !pAllFiles );
  BLOCKSIZE = iBlockSize;
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

/* Forward reference */
static void initFile(OsFile **pId, char const *zName, OsFile *pBase);

/*
** Undo the work done by initFile. Delete the OsFile structure
** and unlink the structure from the pAllFiles list.
*/
static void closeFile(crashFile **pId){
  crashFile *pFile = *pId;
  if( pFile==pAllFiles ){
    pAllFiles = pFile->pNext;
  }else{
    crashFile *p;
    for(p=pAllFiles; p->pNext!=pFile; p=p->pNext ){
      assert( p );
    }
    p->pNext = pFile->pNext;
  }
  sqliteFree(*pId);
  *pId = 0;
}

/*
** Read block 'blk' off of the real disk file and into the cache of pFile.
*/
static int readBlockIntoCache(crashFile *pFile, int blk){
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

    rc = sqlite3OsFileSize(pFile->pBase, &filesize);
    if( rc!=SQLITE_OK ) return rc;

    if( BLOCK_OFFSET(blk)<filesize ){
      int len = BLOCKSIZE;
      rc = sqlite3OsSeek(pFile->pBase, blk*BLOCKSIZE);
      if( BLOCK_OFFSET(blk+1)>filesize ){
        len = filesize - BLOCK_OFFSET(blk);
      }
      if( rc!=SQLITE_OK ) return rc;
      rc = sqlite3OsRead(pFile->pBase, p, len);
      if( rc!=SQLITE_OK ) return rc;
    }
  }

  return SQLITE_OK;
}

/*
** Write the cache of pFile to disk. If crash is non-zero, randomly
** skip blocks when writing. The cache is deleted before returning.
*/
static int writeCache2(crashFile *pFile, int crash){
  int i;
  int nMax = pFile->nMaxWrite;
  int rc = SQLITE_OK;

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
        rc = sqlite3OsSeek(pFile->pBase, BLOCK_OFFSET(i));
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
          rc = sqlite3OsWrite(pFile->pBase, p, len);
        }
      }
      sqliteFree(p);
    }
  }
  sqliteFree(pFile->apBlk);
  pFile->nBlk = 0;
  pFile->apBlk = 0;
  pFile->nMaxWrite = 0;
  return rc;
}

/*
** Write the cache to disk.
*/
static int writeCache(crashFile *pFile){
  if( pFile->apBlk ){
    int c = crashRequired(pFile->zName);
    if( c ){
      crashFile *p;
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
static int crashClose(OsFile **pId){
  crashFile *pFile = (crashFile*)*pId;
  if( pFile ){
    /* printf("CLOSE %s (%d blocks)\n", pFile->zName, pFile->nBlk); */
    writeCache(pFile);
    sqlite3OsClose(&pFile->pBase);
  }
  closeFile(&pFile);
  *pId = 0;
  return SQLITE_OK;
}

static int crashSeek(OsFile *id, i64 offset){
  ((crashFile*)id)->offset = offset;
  return SQLITE_OK;
}

static int crashRead(OsFile *id, void *pBuf, int amt){
  i64 offset;       /* The current offset from the start of the file */
  i64 end;          /* The byte just past the last byte read */
  int blk;            /* Block number the read starts on */
  int i;
  u8 *zCsr;
  int rc = SQLITE_OK;
  crashFile *pFile = (crashFile*)id;

  offset = pFile->offset;
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
      rc = sqlite3OsSeek(pFile->pBase, BLOCK_OFFSET(i) + off);
      if( rc!=SQLITE_OK ) return rc;
      rc = sqlite3OsRead(pFile->pBase, zCsr, len);
      if( rc!=SQLITE_OK ) return rc;
    }

    zCsr += len;
  }
  assert( zCsr==&((u8 *)pBuf)[amt] );

  pFile->offset = end;
  return rc;
}

static int crashWrite(OsFile *id, const void *pBuf, int amt){
  i64 offset;       /* The current offset from the start of the file */
  i64 end;          /* The byte just past the last byte written */
  int blk;            /* Block number the write starts on */
  int i;
  const u8 *zCsr;
  int rc = SQLITE_OK;
  crashFile *pFile = (crashFile*)id;

  offset = pFile->offset;
  end = offset+amt;
  blk = (offset/BLOCKSIZE);

  zCsr = (u8 *)pBuf;
  for(i=blk; i*BLOCKSIZE<end; i++){
    u8 *pBlk;
    int off = 0;
    int len = 0;

    /* Make sure the block is in the cache */
    rc = readBlockIntoCache(pFile, i);
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
  pFile->offset = end;
  return rc;
}

/*
** Sync the file. First flush the write-cache to disk, then call the
** real sync() function.
*/
static int crashSync(OsFile *id, int dataOnly){
  return writeCache((crashFile*)id);
}

/*
** Truncate the file. Set the internal OsFile.nMaxWrite variable to the new
** file size to ensure that nothing in the write-cache past this point
** is written to disk.
*/
static int crashTruncate(OsFile *id, i64 nByte){
  crashFile *pFile = (crashFile*)id;
  pFile->nMaxWrite = nByte;
  return sqlite3OsTruncate(pFile->pBase, nByte);
}

/*
** Return the size of the file. If the cache contains a write that extended
** the file, then return this size instead of the on-disk size.
*/
static int crashFileSize(OsFile *id, i64 *pSize){
  crashFile *pFile = (crashFile*)id;
  int rc = sqlite3OsFileSize(pFile->pBase, pSize);
  if( rc==SQLITE_OK && pSize && *pSize<pFile->nMaxWrite ){
    *pSize = pFile->nMaxWrite;
  }
  return rc;
}

/*
** Set this global variable to 1 to enable crash testing.
*/
int sqlite3CrashTestEnable = 0;

/*
** The three functions used to open files. All that is required is to
** initialise the os_test.c specific fields and then call the corresponding
** os_unix.c function to really open the file.
*/
int sqlite3CrashOpenReadWrite(const char *zFilename, OsFile **pId,int *pRdonly){
  OsFile *pBase = 0;
  int rc;

  sqlite3CrashTestEnable = 0;
  rc = sqlite3OsOpenReadWrite(zFilename, &pBase, pRdonly);
  sqlite3CrashTestEnable = 1;
  if( !rc ){
    initFile(pId, zFilename, pBase);
  }
  return rc;
}
int sqlite3CrashOpenExclusive(const char *zFilename, OsFile **pId, int delFlag){
  OsFile *pBase = 0;
  int rc;

  sqlite3CrashTestEnable = 0;
  rc = sqlite3OsOpenExclusive(zFilename, &pBase, delFlag);
  sqlite3CrashTestEnable = 1;
  if( !rc ){
    initFile(pId, zFilename, pBase);
  }
  return rc;
}
int sqlite3CrashOpenReadOnly(const char *zFilename, OsFile **pId, int NotUsed){
  OsFile *pBase = 0;
  int rc;

  sqlite3CrashTestEnable = 0;
  rc = sqlite3OsOpenReadOnly(zFilename, &pBase);
  sqlite3CrashTestEnable = 1;
  if( !rc ){
    initFile(pId, zFilename, pBase);
  }
  return rc;
}

/*
** OpenDirectory is a no-op
*/
static int crashOpenDir(OsFile *id, const char *zName){
  return SQLITE_OK;
}

/*
** Locking primitives are passed through into the underlying
** file descriptor.
*/
int crashLock(OsFile *id, int lockType){
  return sqlite3OsLock(((crashFile*)id)->pBase, lockType);
}
int crashUnlock(OsFile *id, int lockType){
  return sqlite3OsUnlock(((crashFile*)id)->pBase, lockType);
}
int crashCheckReservedLock(OsFile *id){
  return sqlite3OsCheckReservedLock(((crashFile*)id)->pBase);
}
void crashSetFullSync(OsFile *id, int setting){
  return;  /* This is a no-op */
}
int crashLockState(OsFile *id){
  return sqlite3OsLockState(((crashFile*)id)->pBase);
}

/*
** Return the underlying file handle.
*/
int crashFileHandle(OsFile *id){
#if defined(SQLITE_TEST) || defined(SQLITE_DEBUG)
  return sqlite3OsFileHandle(((crashFile*)id)->pBase);
#endif
  return 0;
}

/*
** Return the simulated file-system sector size.
*/
int crashSectorSize(OsFile *id){
  return BLOCKSIZE;
}

/*
** This vector defines all the methods that can operate on an OsFile
** for the crash tester.
*/
static const IoMethod crashIoMethod = {
  crashClose,
  crashOpenDir,
  crashRead,
  crashWrite,
  crashSeek,
  crashTruncate,
  crashSync,
  crashSetFullSync,
  crashFileHandle,
  crashFileSize,
  crashLock,
  crashUnlock,
  crashLockState,
  crashCheckReservedLock,
  crashSectorSize,
};


/*
** Initialise the os_test.c specific fields of pFile.
*/
static void initFile(OsFile **pId, char const *zName, OsFile *pBase){
  crashFile *pFile = sqliteMalloc(sizeof(crashFile) + strlen(zName)+1);
  pFile->pMethod = &crashIoMethod;
  pFile->nMaxWrite = 0; 
  pFile->offset = 0;
  pFile->nBlk = 0; 
  pFile->apBlk = 0; 
  pFile->zName = (char *)(&pFile[1]);
  strcpy(pFile->zName, zName);
  pFile->pBase = pBase;
  pFile->pNext = pAllFiles;
  pAllFiles = pFile;
  *pId = (OsFile*)pFile;
}


/*
** tclcmd:   sqlite_crashparams DELAY CRASHFILE ?BLOCKSIZE?
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
  int iDelay;
  const char *zFile;
  int nFile;

  if( objc!=3 && objc!=4 ){
    Tcl_WrongNumArgs(interp, 1, objv, "DELAY CRASHFILE ?BLOCKSIZE?");
    return TCL_ERROR;
  }
  if( Tcl_GetIntFromObj(interp, objv[1], &iDelay) ) return TCL_ERROR;
  zFile = Tcl_GetStringFromObj(objv[2], &nFile);
  if( nFile>=sizeof(zCrashFile)-1 ){
    Tcl_AppendResult(interp, "crash file name too big", 0);
    return TCL_ERROR;
  }
  setCrashParams(iDelay, zFile);
  if( objc==4 ){
    int iBlockSize = 0;
    if( Tcl_GetIntFromObj(interp, objv[3], &iBlockSize) ) return TCL_ERROR;
    if( pAllFiles ){
      char *zErr = "Cannot modify blocksize after opening files";
      Tcl_SetResult(interp, zErr, TCL_STATIC);
      return TCL_ERROR;
    }
    setBlocksize(iBlockSize);
  }
  sqlite3CrashTestEnable = 1;
  return TCL_OK;
}

#endif /* SQLITE_OMIT_DISKIO */

/*
** This procedure registers the TCL procedures defined in this file.
*/
int Sqlitetest6_Init(Tcl_Interp *interp){
#ifndef SQLITE_OMIT_DISKIO
  Tcl_CreateObjCommand(interp, "sqlite3_crashparams", crashParamsObjCmd, 0, 0);
#endif
  return TCL_OK;
}

#endif /* SQLITE_TEST */

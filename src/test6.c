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

typedef struct CrashFile CrashFile;
typedef struct CrashGlobal CrashGlobal;
typedef struct WriteBuffer WriteBuffer;

/*
** Method:
**
**   This layer is implemented as a wrapper around the "real" 
**   sqlite3_file object for the host system. Each time data is 
**   written to the file object, instead of being written to the
**   underlying file, the write operation is stored in an in-memory 
**   structure (type WriteBuffer). This structure is placed at the
**   end of a global ordered list (the write-list).
**
**   When data is read from a file object, the requested region is
**   first retrieved from the real file. The write-list is then 
**   traversed and data copied from any overlapping WriteBuffer 
**   structures to the output buffer. i.e. a read() operation following
**   one or more write() operations works as expected, even if no
**   data has actually been written out to the real file.
**
**   When a fsync() operation is performed, an operating system crash 
**   may be simulated, in which case exit(-1) is called (the call to 
**   xSync() never returns). Whether or not a crash is simulated,
**   the data associated with a subset of the WriteBuffer structures 
**   stored in the write-list is written to the real underlying files 
**   and the entries removed from the write-list. If a crash is simulated,
**   a subset of the buffers may be corrupted before the data is written.
**
**   The exact subset of the write-list written and/or corrupted is
**   determined by the simulated device characteristics and sector-size.
**
** "Normal" mode:
**
**   Normal mode is used when the simulated device has none of the
**   SQLITE_IOCAP_XXX flags set.
**
**   In normal mode, if the fsync() is not a simulated crash, the 
**   write-list is traversed from beginning to end. Each WriteBuffer
**   structure associated with the file handle used to call xSync()
**   is written to the real file and removed from the write-list.
**
**   If a crash is simulated, one of the following takes place for 
**   each WriteBuffer in the write-list, regardless of which 
**   file-handle it is associated with:
**
**     1. The buffer is correctly written to the file, just as if
**        a crash were not being simulated.
**
**     2. Nothing is done.
**
**     3. Garbage data is written to all sectors of the file that 
**        overlap the region specified by the WriteBuffer. Or garbage
**        data is written to some contiguous section within the 
**        overlapped sectors.
**
** Device Characteristic flag handling:
**
**   If the IOCAP_ATOMIC flag is set, then option (3) above is 
**   never selected.
**
**   If the IOCAP_ATOMIC512 flag is set, and the WriteBuffer represents
**   an aligned write() of an integer number of 512 byte regions, then
**   option (3) above is never selected. Instead, each 512 byte region
**   is either correctly written or left completely untouched. Similar
**   logic governs the behaviour if any of the other ATOMICXXX flags
**   is set.
**
**   If either the IOCAP_SAFEAPPEND or IOCAP_SEQUENTIAL flags are set
**   and a crash is being simulated, then an entry of the write-list is
**   selected at random. Everything in the list after the selected entry 
**   is discarded before processing begins.
**
**   If IOCAP_SEQUENTIAL is set and a crash is being simulated, option 
**   (1) is selected for all write-list entries except the last. If a 
**   crash is not being simulated, then all entries in the write-list
**   that occur before at least one write() on the file-handle specified
**   as part of the xSync() are written to their associated real files.
**
**   If IOCAP_SAFEAPPEND is set and the first byte written by the write()
**   operation is one byte past the current end of the file, then option
**   (1) is always selected.
*/

/*
** Each write operation in the write-list is represented by an instance
** of the following structure.
**
** If zBuf is 0, then this structure represents a call to xTruncate(), 
** not xWrite(). In that case, iOffset is the size that the file is
** truncated to.
*/
struct WriteBuffer {
  i64 iOffset;                 /* Byte offset of the start of this write() */
  int nBuf;                    /* Number of bytes written */
  u8 *zBuf;                    /* Pointer to copy of written data */
  CrashFile *pFile;            /* File this write() applies to */

  WriteBuffer *pNext;          /* Next in CrashGlobal.pWriteList */
};

struct CrashFile {
  const sqlite3_io_methods *pMethod;   /* Must be first */
  sqlite3_file *pRealFile;             /* Underlying "real" file handle */
  char *zName;
};

struct CrashGlobal {
  WriteBuffer *pWriteList;     /* Head of write-list */

  int iSectorSize;             /* Value of simulated sector size */
  int iDeviceCharacteristics;  /* Value of simulated device characteristics */

  int iCrash;                  /* Crash on the iCrash'th call to xSync() */
  char zCrashFile[500];        /* Crash during an xSync() on this file */ 
};

static CrashGlobal g = {0, SQLITE_DEFAULT_SECTOR_SIZE, 0, 0};

/*
** Set this global variable to 1 to enable crash testing.
*/
int sqlite3CrashTestEnable = 0;

/*
** Flush the write-list as if xSync() had been called on file handle
** pFile. If isCrash is true, simulate a crash.
*/
static int writeListSync(CrashFile *pFile, int isCrash){
  int rc = SQLITE_OK;
  int iDc = g.iDeviceCharacteristics;

  WriteBuffer *pWrite;
  WriteBuffer **ppPtr;

  /* Set pFinal to point to the last element of the write-list that
  ** is associated with file handle pFile.
  */
  WriteBuffer *pFinal = 0;
  if( !isCrash ){
    for(pWrite=g.pWriteList; pWrite; pWrite=pWrite->pNext){
      if( pWrite->pFile==pFile ){
        pFinal = pWrite;
      }
    }
  }

  ppPtr = &g.pWriteList;
  for(pWrite=*ppPtr; rc==SQLITE_OK && pWrite; pWrite=*ppPtr){

    /* (eAction==1)      -> write block out normally,
    ** (eAction==2)      -> do nothing,
    ** (eAction==3)      -> trash sectors.
    */
    int eAction = 0;
    if( !isCrash ){
      eAction = 2;
      if( (pWrite->pFile==pFile || iDc&SQLITE_IOCAP_SEQUENTIAL) ){
        eAction = 1;
      }
    }else{
      char random;
      sqlite3Randomness(1, &random);

      if( iDc&SQLITE_IOCAP_ATOMIC || pWrite->zBuf==0 ){
        random &= 0x01;
      }

      if( (random&0x06)==0x06 ){
        eAction = 3;
      }else{
        eAction = ((random&0x01)?2:1);
      }
    }

    switch( eAction ){
      case 1: {               /* Write out correctly */
        if( pWrite->zBuf ){
          rc = sqlite3OsWrite(
              pFile->pRealFile, pWrite->zBuf, pWrite->nBuf, pWrite->iOffset
          );
        }else{
          rc = sqlite3OsTruncate(pFile->pRealFile, pWrite->iOffset);
        }
        *ppPtr = pWrite->pNext;
        sqlite3_free(pWrite);
        break;
      }
      case 2: {               /* Do nothing */
        ppPtr = &pWrite->pNext;
        break;
      }
      case 3: {               /* Trash sectors */
        u8 *zGarbage;
        sqlite3_int64 iFirst = (pWrite->iOffset%g.iSectorSize);
        sqlite3_int64 iLast = (pWrite->iOffset+pWrite->nBuf-1)%g.iSectorSize;

        zGarbage = sqlite3_malloc(g.iSectorSize);
        if( zGarbage ){
          sqlite3_int64 i;
          for(i=iFirst; rc==SQLITE_OK && i<=iLast; i++){
            sqlite3Randomness(g.iSectorSize, zGarbage); 
            rc = sqlite3OsWrite(
              pFile->pRealFile, zGarbage, g.iSectorSize, i*g.iSectorSize
            );
          }
          sqlite3_free(zGarbage);
        }else{
          rc = SQLITE_NOMEM;
        }

        ppPtr = &pWrite->pNext;
        break;
      }

      default:
        assert(!"Cannot happen");
    }

    if( pWrite==pFinal ) break;
  }

  if( rc==SQLITE_OK && isCrash ){
    exit(-1);
  }

  return rc;
}

/*
** Add an entry to the end of the write-list.
*/
static int writeListAppend(
  sqlite3_file *pFile,
  sqlite3_int64 iOffset,
  const u8 *zBuf,
  int nBuf
){
  WriteBuffer *pNew;

  assert((zBuf && nBuf) || (!nBuf && !zBuf));

  pNew = (WriteBuffer *)sqlite3MallocZero(sizeof(WriteBuffer) + nBuf);
  pNew->iOffset = iOffset;
  pNew->nBuf = nBuf;
  pNew->pFile = (CrashFile *)pFile;
  if( zBuf ){
    pNew->zBuf = (u8 *)&pNew[1];
    memcpy(pNew->zBuf, zBuf, nBuf);
  }

  if( g.pWriteList ){
    WriteBuffer *pEnd;
    for(pEnd=g.pWriteList; pEnd->pNext; pEnd=pEnd->pNext);
    pEnd->pNext = pNew;
  }else{
    g.pWriteList = pNew;
  }
  
  return SQLITE_OK;
}

/*
** Close a crash-file.
*/
static int cfClose(sqlite3_file *pFile){
  CrashFile *pCrash = (CrashFile *)pFile;
  writeListSync(pCrash, 0);
  sqlite3OsClose(&pCrash->pRealFile);
  return SQLITE_OK;
}

/*
** Read data from a crash-file.
*/
static int cfRead(
  sqlite3_file *pFile, 
  void *zBuf, 
  int iAmt, 
  sqlite_int64 iOfst
){
  CrashFile *pCrash = (CrashFile *)pFile;
  sqlite3_int64 iSize;
  int rc;
  WriteBuffer *pWrite;

  /* Check the file-size to see if this is a short-read */
  rc = sqlite3OsFileSize(pFile, &iSize);
  if( rc!=SQLITE_OK ){
    return rc;
  }
  if( iSize<(iOfst+iAmt) ){
    return SQLITE_IOERR_SHORT_READ;
  }

  /* Zero the output buffer */
  memset(zBuf, 0, iAmt);

  /* Read some data from the real file */
  rc = sqlite3OsFileSize(pCrash->pRealFile, &iSize);
  if( rc==SQLITE_OK && iSize>iOfst ){
    int nRead = iAmt;
    if( iSize<(iOfst+iAmt) ){
      nRead = iSize - iOfst;
    }
    rc = sqlite3OsRead(pCrash->pRealFile, zBuf, nRead, iOfst);
  }

  /* Fill in the buffer by traversing the write-list */
  for(pWrite=g.pWriteList; rc==SQLITE_OK && pWrite; pWrite=pWrite->pNext){
    if( pWrite->pFile==pCrash ){
      int iWriteOffset;
      int nWriteBuf;
      u8 *zWriteBuf;

      iWriteOffset = pWrite->iOffset - iOfst;
      nWriteBuf = pWrite->nBuf;
      zWriteBuf = pWrite->zBuf;
      if( iWriteOffset<0 ){
        nWriteBuf += iWriteOffset;
        zWriteBuf -= iWriteOffset;
        iWriteOffset = 0;
      }
      if( (iWriteOffset+nWriteBuf)>iAmt ){
        nWriteBuf = iAmt - iWriteOffset;
      }
      
      if( pWrite->zBuf && nWriteBuf>0){
        /* Copy data to the buffer */
        memcpy(&((u8 *)zBuf)[iWriteOffset], zWriteBuf, nWriteBuf);
      }

      if( pWrite->zBuf==0 && iWriteOffset<iAmt ){
        /* Zero part of the buffer to simulate a truncate */
        memset(&((u8 *)zBuf)[iWriteOffset], 0, iAmt-iWriteOffset);
      }
    }
  }

  return rc;
}

/*
** Write data to a crash-file.
*/
static int cfWrite(
  sqlite3_file *pFile, 
  const void *zBuf, 
  int iAmt, 
  sqlite_int64 iOfst
){
  return writeListAppend(pFile, iOfst, zBuf, iAmt);
}

/*
** Truncate a crash-file.
*/
static int cfTruncate(sqlite3_file *pFile, sqlite_int64 size){
  return writeListAppend(pFile, size, 0, 0);
}

/*
** Sync a crash-file.
*/
static int cfSync(sqlite3_file *pFile, int flags){
  CrashFile *pCrash = (CrashFile *)pFile;
  int isCrash = 0;

  if( 0==strcmp(pCrash->zName, g.zCrashFile) ){
    if( (--g.iCrash==0) ){
      isCrash = 1;
    }
  }

  return writeListSync(pCrash, isCrash);
}

/*
** Return the current file-size of the crash-file.
*/
static int cfFileSize(sqlite3_file *pFile, sqlite_int64 *pSize){
  CrashFile *pCrash = (CrashFile *)pFile;
  WriteBuffer *pWrite;
  int rc;
  sqlite_int64 iSize;

  rc = sqlite3OsFileSize(pCrash->pRealFile, &iSize);
  if( rc!=SQLITE_OK ){
    return rc;
  }

  for(pWrite=g.pWriteList; pWrite; pWrite=pWrite->pNext){
    sqlite_int64 iEnd = pWrite->nBuf+pWrite->iOffset;
    if( pWrite->pFile==pCrash && (pWrite->zBuf==0 || iEnd>iSize) ){
      iSize = iEnd;
    }
  }
  *pSize = iSize;

  return SQLITE_OK;
}

/*
** Calls related to file-locks are passed on to the real file handle.
*/
static int cfLock(sqlite3_file *pFile, int eLock){
  return sqlite3OsLock(((CrashFile *)pFile)->pRealFile, eLock);
}
static int cfUnlock(sqlite3_file *pFile, int eLock){
  return sqlite3OsUnlock(((CrashFile *)pFile)->pRealFile, eLock);
}
static int cfCheckReservedLock(sqlite3_file *pFile){
  return sqlite3OsCheckReservedLock(((CrashFile *)pFile)->pRealFile);
}
static int cfBreakLock(sqlite3_file *pFile){
  return sqlite3OsBreakLock(((CrashFile *)pFile)->pRealFile);
}

/*
** The xSectorSize() and xDeviceCharacteristics() functions return
** the global values configured by the [sqlite_crashparams] tcl
*  interface.
*/
static int cfSectorSize(sqlite3_file *pFile){
  return g.iSectorSize;
}
static int cfDeviceCharacteristics(sqlite3_file *pFile){
  return g.iDeviceCharacteristics;
}

static const sqlite3_io_methods CrashFileVtab = {
  1,                            /* iVersion */
  cfClose,                      /* xClose */
  cfRead,                       /* xRead */
  cfWrite,                      /* xWrite */
  cfTruncate,                   /* xTruncate */
  cfSync,                       /* xSync */
  cfFileSize,                   /* xFileSize */
  cfLock,                       /* xLock */
  cfUnlock,                     /* xUnlock */
  cfCheckReservedLock,          /* xCheckReservedLock */
  cfBreakLock,                  /* xBreakLock */
  cfSectorSize,                 /* xSectorSize */
  cfDeviceCharacteristics       /* xDeviceCharacteristics */
};

/*
** Open a crash-file file handle. The vfs pVfs is used to open
** the underlying real file.
*/
int sqlite3CrashFileOpen(
  sqlite3_vfs *pVfs,
  const char *zName,
  sqlite3_file *pFile,
  int flags,
  int *pOutFlags
){
  CrashFile *pWrapper = (CrashFile *)pFile;
  int rc = SQLITE_NOMEM;
  sqlite3_file *pReal;
  pReal = (sqlite3_file *)sqlite3_malloc(pVfs->szOsFile);
  if( pReal ){
    pWrapper->pMethod = &CrashFileVtab;
    pWrapper->zName = (char *)zName;
    rc = pVfs->xOpen(pVfs->pAppData, zName, pReal, flags, pOutFlags);
    if( rc==SQLITE_OK ){
      pWrapper->pRealFile = pFile;
    }else{
      sqlite3_free(pReal);
    }
  }
  return rc;
}

int sqlite3CrashFileWrap(
  sqlite3_file *pFile,
  const char *zName,
  sqlite3_file **ppWrapper
){
  CrashFile *pWrapper;
  pWrapper = (CrashFile *)sqlite3_malloc(sizeof(CrashFile)+strlen(zName)+1);
  if( !pWrapper ){
    return SQLITE_NOMEM;
  }

  pWrapper->pMethod = &CrashFileVtab;
  pWrapper->pRealFile = pFile;
  pWrapper->zName = (char *)&pWrapper[1];
  memcpy(pWrapper->zName, zName, strlen(zName)+1);

  *ppWrapper = (sqlite3_file *)pWrapper;
  return SQLITE_OK;
}


int sqlite3CrashFileSize(){
  return (int)sizeof(CrashFile);
}

/*
** tclcmd:   sqlite_crashparams ?OPTIONS? DELAY CRASHFILE
**
** This procedure implements a TCL command that enables crash testing
** in testfixture.  Once enabled, crash testing cannot be disabled.
**
** Available options are "-characteristics" and "-sectorsize". Both require
** an argument. For -sectorsize, this is the simulated sector size in
** bytes. For -characteristics, the argument must be a list of io-capability
** flags to simulate. Valid flags are "atomic", "atomic512", "atomic1K",
** "atomic2K", "atomic4K", "atomic8K", "atomic16K", "atomic32K", 
** "atomic64K", "sequential" and "safe_append".
**
** Example:
**
**   sqlite_crashparams -sect 1024 -char {atomic sequential} ./test.db 1
**
*/
static int crashParamsObjCmd(
  void * clientData,
  Tcl_Interp *interp,
  int objc,
  Tcl_Obj *CONST objv[]
){
  sqlite3_vfs *pVfs;
  int i;
  int iDelay;
  const char *zCrashFile;
  int nCrashFile;

  int iDc = 0;
  int iSectorSize = 0;
  int setSectorsize = 0;
  int setDeviceChar = 0;

  struct DeviceFlag {
    char *zName;
    int iValue;
  } aFlag[] = {
    { "atomic",      SQLITE_IOCAP_ATOMIC      },
    { "atomic512",   SQLITE_IOCAP_ATOMIC512   },
    { "atomic1k",    SQLITE_IOCAP_ATOMIC1K    },
    { "atomic2k",    SQLITE_IOCAP_ATOMIC2K    },
    { "atomic4k",    SQLITE_IOCAP_ATOMIC4K    },
    { "atomic8k",    SQLITE_IOCAP_ATOMIC8K    },
    { "atomic16k",   SQLITE_IOCAP_ATOMIC16K   },
    { "atomic32k",   SQLITE_IOCAP_ATOMIC32K   },
    { "atomic64k",   SQLITE_IOCAP_ATOMIC64K   },
    { "sequential",  SQLITE_IOCAP_SEQUENTIAL  },
    { "safe_append", SQLITE_IOCAP_SAFE_APPEND },
    { 0, 0 }
  };
  
  if( objc<3 ){
    Tcl_WrongNumArgs(interp, 1, objv, "?OPTIONS? DELAY CRASHFILE");
    goto error;
  }

  pVfs = sqlite3_find_vfs(0);
  zCrashFile = sqlite3_malloc(pVfs->mxPathname);
  sqlite3OsFullPathname(pVfs, Tcl_GetString(objv[objc-1]), zCrashFile);
  nCrashFile = strlen(zCrashFile);
  if( nCrashFile>=sizeof(g.zCrashFile) ){
    Tcl_AppendResult(interp, "Filename is too long: \"", zCrashFile, "\"", 0);
    goto error;
  }
  if( Tcl_GetIntFromObj(interp, objv[objc-2], &iDelay) ){
    goto error;
  }

  for(i=1; i<(objc-2); i+=2){
    int nOpt;
    char *zOpt = Tcl_GetStringFromObj(objv[i], &nOpt);

    if( (nOpt>11 || nOpt<2 || strncmp("-sectorsize", zOpt, nOpt)) 
     && (nOpt>16 || nOpt<2 || strncmp("-characteristics", zOpt, nOpt))
    ){
      Tcl_AppendResult(interp, 
        "Bad option: \"", zOpt, 
        "\" - must be \"-characteristics\" or \"-sectorsize\"", 0
      );
      goto error;
    }
    if( i==objc-3 ){
      Tcl_AppendResult(interp, "Option requires an argument: \"", zOpt, "\"",0);
      goto error;
    }

    if( zOpt[1]=='s' ){
      if( Tcl_GetIntFromObj(interp, objv[i+1], &iSectorSize) ){
        goto error;
      }
      setSectorsize = 1;
    }else{
      int j;
      Tcl_Obj **apObj;
      int nObj;
      if( Tcl_ListObjGetElements(interp, objv[i+1], &nObj, &apObj) ){
        goto error;
      }
      for(j=0; j<nObj; j++){
        int rc;
        int iChoice;
        Tcl_Obj *pFlag = Tcl_DuplicateObj(apObj[j]);
        Tcl_IncrRefCount(pFlag);
        Tcl_UtfToLower(Tcl_GetString(pFlag));
 
        rc = Tcl_GetIndexFromObjStruct(
            interp, pFlag, aFlag, sizeof(aFlag[0]), "no such flag", 0, &iChoice
        );
        Tcl_DecrRefCount(pFlag);
        if( rc ){
          goto error;
        }

        iDc |= aFlag[iChoice].iValue;
      }
      setDeviceChar = 1;
    }
  }

  if( setDeviceChar ){
    g.iDeviceCharacteristics = iDc;
  }
  if( setSectorsize ){
    g.iSectorSize = iSectorSize;
  }
  g.iCrash = iDelay;
  memcpy(g.zCrashFile, zCrashFile, nCrashFile+1);
  sqlite3_free(zCrashFile);
  sqlite3CrashTestEnable = 1;
  return TCL_OK;

error:
  sqlite3_free(zCrashFile);
  return TCL_ERROR;
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

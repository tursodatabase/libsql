/*
** 2010 May 05
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
*/
#if SQLITE_TEST          /* This file is used for testing only */

#include "sqlite3.h"
#include "sqliteInt.h"

typedef struct Testvfs Testvfs;
typedef struct TestvfsShm TestvfsShm;
typedef struct TestvfsBuffer TestvfsBuffer;
typedef struct TestvfsFile TestvfsFile;

/*
** An open file handle.
*/
struct TestvfsFile {
  sqlite3_file base;              /* Base class.  Must be first */
  sqlite3_vfs *pVfs;              /* The VFS */
  const char *zFilename;          /* Filename as passed to xOpen() */
  sqlite3_file *pReal;            /* The real, underlying file descriptor */
  Tcl_Obj *pShmId;                /* Shared memory id for Tcl callbacks */
  TestvfsBuffer *pShm;            /* Shared memory buffer */
};


/*
** An instance of this structure is allocated for each VFS created. The
** sqlite3_vfs.pAppData field of the VFS structure registered with SQLite
** is set to point to it.
*/
struct Testvfs {
  char *zName;                    /* Name of this VFS */
  sqlite3_vfs *pParent;           /* The VFS to use for file IO */
  sqlite3_vfs *pVfs;              /* The testvfs registered with SQLite */
  Tcl_Interp *interp;             /* Interpreter to run script in */
  Tcl_Obj *pScript;               /* Script to execute */
  int nScript;                    /* Number of elements in array apScript */
  Tcl_Obj **apScript;             /* Array version of pScript */
  TestvfsBuffer *pBuffer;         /* List of shared buffers */
  int isNoshm;

  int mask;
  int iIoerrCnt;
  int ioerr;
  int nIoerrFail;
};

/*
** The Testvfs.mask variable is set to a combination of the following.
** If a bit is clear in Testvfs.mask, then calls made by SQLite to the 
** corresponding VFS method is ignored for purposes of:
**
**   + Simulating IO errors, and
**   + Invoking the Tcl callback script.
*/
#define TESTVFS_SHMOPEN_MASK    0x00000001
#define TESTVFS_SHMSIZE_MASK    0x00000002
#define TESTVFS_SHMGET_MASK     0x00000004
#define TESTVFS_SHMRELEASE_MASK 0x00000008
#define TESTVFS_SHMLOCK_MASK    0x00000010
#define TESTVFS_SHMBARRIER_MASK 0x00000020
#define TESTVFS_SHMCLOSE_MASK   0x00000040

#define TESTVFS_OPEN_MASK       0x00000080
#define TESTVFS_SYNC_MASK       0x00000100
#define TESTVFS_ALL_MASK        0x000001FF

/*
** A shared-memory buffer.
*/
struct TestvfsBuffer {
  char *zFile;                    /* Associated file name */
  int n;                          /* Size of allocated buffer in bytes */
  u8 *a;                          /* Buffer allocated using ckalloc() */
  int nRef;                       /* Number of references to this object */
  TestvfsBuffer *pNext;           /* Next in linked list of all buffers */
};


#define PARENTVFS(x) (((Testvfs *)((x)->pAppData))->pParent)

#define TESTVFS_MAX_ARGS 12


/*
** Method declarations for TestvfsFile.
*/
static int tvfsClose(sqlite3_file*);
static int tvfsRead(sqlite3_file*, void*, int iAmt, sqlite3_int64 iOfst);
static int tvfsWrite(sqlite3_file*,const void*,int iAmt, sqlite3_int64 iOfst);
static int tvfsTruncate(sqlite3_file*, sqlite3_int64 size);
static int tvfsSync(sqlite3_file*, int flags);
static int tvfsFileSize(sqlite3_file*, sqlite3_int64 *pSize);
static int tvfsLock(sqlite3_file*, int);
static int tvfsUnlock(sqlite3_file*, int);
static int tvfsCheckReservedLock(sqlite3_file*, int *);
static int tvfsFileControl(sqlite3_file*, int op, void *pArg);
static int tvfsSectorSize(sqlite3_file*);
static int tvfsDeviceCharacteristics(sqlite3_file*);

/*
** Method declarations for tvfs_vfs.
*/
static int tvfsOpen(sqlite3_vfs*, const char *, sqlite3_file*, int , int *);
static int tvfsDelete(sqlite3_vfs*, const char *zName, int syncDir);
static int tvfsAccess(sqlite3_vfs*, const char *zName, int flags, int *);
static int tvfsFullPathname(sqlite3_vfs*, const char *zName, int, char *zOut);
#ifndef SQLITE_OMIT_LOAD_EXTENSION
static void *tvfsDlOpen(sqlite3_vfs*, const char *zFilename);
static void tvfsDlError(sqlite3_vfs*, int nByte, char *zErrMsg);
static void (*tvfsDlSym(sqlite3_vfs*,void*, const char *zSymbol))(void);
static void tvfsDlClose(sqlite3_vfs*, void*);
#endif /* SQLITE_OMIT_LOAD_EXTENSION */
static int tvfsRandomness(sqlite3_vfs*, int nByte, char *zOut);
static int tvfsSleep(sqlite3_vfs*, int microseconds);
static int tvfsCurrentTime(sqlite3_vfs*, double*);

static int tvfsShmOpen(sqlite3_file*);
static int tvfsShmSize(sqlite3_file*, int , int *);
static int tvfsShmGet(sqlite3_file*, int , int *, volatile void **);
static int tvfsShmRelease(sqlite3_file*);
static int tvfsShmLock(sqlite3_file*, int , int, int);
static void tvfsShmBarrier(sqlite3_file*);
static int tvfsShmClose(sqlite3_file*, int);

static sqlite3_io_methods tvfs_io_methods = {
  2,                            /* iVersion */
  tvfsClose,                      /* xClose */
  tvfsRead,                       /* xRead */
  tvfsWrite,                      /* xWrite */
  tvfsTruncate,                   /* xTruncate */
  tvfsSync,                       /* xSync */
  tvfsFileSize,                   /* xFileSize */
  tvfsLock,                       /* xLock */
  tvfsUnlock,                     /* xUnlock */
  tvfsCheckReservedLock,          /* xCheckReservedLock */
  tvfsFileControl,                /* xFileControl */
  tvfsSectorSize,                 /* xSectorSize */
  tvfsDeviceCharacteristics,      /* xDeviceCharacteristics */
  tvfsShmOpen,                    /* xShmOpen */
  tvfsShmSize,                    /* xShmSize */
  tvfsShmGet,                     /* xShmGet */
  tvfsShmRelease,                 /* xShmRelease */
  tvfsShmLock,                    /* xShmLock */
  tvfsShmBarrier,                 /* xShmBarrier */
  tvfsShmClose                    /* xShmClose */
};

static int tvfsResultCode(Testvfs *p, int *pRc){
  struct errcode {
    int eCode;
    const char *zCode;
  } aCode[] = {
    { SQLITE_OK,     "SQLITE_OK"     },
    { SQLITE_ERROR,  "SQLITE_ERROR"  },
    { SQLITE_IOERR,  "SQLITE_IOERR"  },
    { SQLITE_LOCKED, "SQLITE_LOCKED" },
    { SQLITE_BUSY,   "SQLITE_BUSY"   },
  };

  const char *z;
  int i;

  z = Tcl_GetStringResult(p->interp);
  for(i=0; i<ArraySize(aCode); i++){
    if( 0==strcmp(z, aCode[i].zCode) ){
      *pRc = aCode[i].eCode;
      return 1;
    }
  }

  return 0;
}


static void tvfsExecTcl(
  Testvfs *p, 
  const char *zMethod,
  Tcl_Obj *arg1,
  Tcl_Obj *arg2,
  Tcl_Obj *arg3
){
  int rc;                         /* Return code from Tcl_EvalObj() */
  int nArg;                       /* Elements in eval'd list */
  int nScript;
  Tcl_Obj ** ap;

  assert( p->pScript );

  if( !p->apScript ){
    int nByte;
    int i;
    if( TCL_OK!=Tcl_ListObjGetElements(p->interp, p->pScript, &nScript, &ap) ){
      Tcl_BackgroundError(p->interp);
      Tcl_ResetResult(p->interp);
      return;
    }
    p->nScript = nScript;
    nByte = (nScript+TESTVFS_MAX_ARGS)*sizeof(Tcl_Obj *);
    p->apScript = (Tcl_Obj **)ckalloc(nByte);
    memset(p->apScript, 0, nByte);
    for(i=0; i<nScript; i++){
      p->apScript[i] = ap[i];
    }
  }

  p->apScript[p->nScript] = Tcl_NewStringObj(zMethod, -1);
  p->apScript[p->nScript+1] = arg1;
  p->apScript[p->nScript+2] = arg2;
  p->apScript[p->nScript+3] = arg3;

  for(nArg=p->nScript; p->apScript[nArg]; nArg++){
    Tcl_IncrRefCount(p->apScript[nArg]);
  }

  rc = Tcl_EvalObjv(p->interp, nArg, p->apScript, TCL_EVAL_GLOBAL);
  if( rc!=TCL_OK ){
    Tcl_BackgroundError(p->interp);
    Tcl_ResetResult(p->interp);
  }

  for(nArg=p->nScript; p->apScript[nArg]; nArg++){
    Tcl_DecrRefCount(p->apScript[nArg]);
    p->apScript[nArg] = 0;
  }
}


/*
** Close an tvfs-file.
*/
static int tvfsClose(sqlite3_file *pFile){
  TestvfsFile *p = (TestvfsFile *)pFile;
  if( p->pShmId ){
    Tcl_DecrRefCount(p->pShmId);
    p->pShmId = 0;
  }
  if( pFile->pMethods ){
    ckfree((char *)pFile->pMethods);
  }
  return sqlite3OsClose(p->pReal);
}

/*
** Read data from an tvfs-file.
*/
static int tvfsRead(
  sqlite3_file *pFile, 
  void *zBuf, 
  int iAmt, 
  sqlite_int64 iOfst
){
  TestvfsFile *p = (TestvfsFile *)pFile;
  return sqlite3OsRead(p->pReal, zBuf, iAmt, iOfst);
}

/*
** Write data to an tvfs-file.
*/
static int tvfsWrite(
  sqlite3_file *pFile, 
  const void *zBuf, 
  int iAmt, 
  sqlite_int64 iOfst
){
  TestvfsFile *p = (TestvfsFile *)pFile;
  return sqlite3OsWrite(p->pReal, zBuf, iAmt, iOfst);
}

/*
** Truncate an tvfs-file.
*/
static int tvfsTruncate(sqlite3_file *pFile, sqlite_int64 size){
  TestvfsFile *p = (TestvfsFile *)pFile;
  return sqlite3OsTruncate(p->pReal, size);
}

/*
** Sync an tvfs-file.
*/
static int tvfsSync(sqlite3_file *pFile, int flags){
  int rc = SQLITE_OK;
  TestvfsFile *pFd = (TestvfsFile *)pFile;
  Testvfs *p = (Testvfs *)pFd->pVfs->pAppData;

  if( p->pScript && p->mask&TESTVFS_SYNC_MASK ){
    char *zFlags;

    switch( flags ){
      case SQLITE_SYNC_NORMAL:
        zFlags = "normal";
        break;
      case SQLITE_SYNC_FULL:
        zFlags = "full";
        break;
      case SQLITE_SYNC_NORMAL|SQLITE_SYNC_DATAONLY:
        zFlags = "normal|dataonly";
        break;
      case SQLITE_SYNC_FULL|SQLITE_SYNC_DATAONLY:
        zFlags = "full|dataonly";
        break;
      default:
        assert(0);
    }

    tvfsExecTcl(p, "xSync", 
        Tcl_NewStringObj(pFd->zFilename, -1), pFd->pShmId,
        Tcl_NewStringObj(zFlags, -1)
    );
    tvfsResultCode(p, &rc);
  }

  if( rc==SQLITE_OK ){
    rc = sqlite3OsSync(pFd->pReal, flags);
  }

  return rc;
}

/*
** Return the current file-size of an tvfs-file.
*/
static int tvfsFileSize(sqlite3_file *pFile, sqlite_int64 *pSize){
  TestvfsFile *p = (TestvfsFile *)pFile;
  return sqlite3OsFileSize(p->pReal, pSize);
}

/*
** Lock an tvfs-file.
*/
static int tvfsLock(sqlite3_file *pFile, int eLock){
  TestvfsFile *p = (TestvfsFile *)pFile;
  return sqlite3OsLock(p->pReal, eLock);
}

/*
** Unlock an tvfs-file.
*/
static int tvfsUnlock(sqlite3_file *pFile, int eLock){
  TestvfsFile *p = (TestvfsFile *)pFile;
  return sqlite3OsUnlock(p->pReal, eLock);
}

/*
** Check if another file-handle holds a RESERVED lock on an tvfs-file.
*/
static int tvfsCheckReservedLock(sqlite3_file *pFile, int *pResOut){
  TestvfsFile *p = (TestvfsFile *)pFile;
  return sqlite3OsCheckReservedLock(p->pReal, pResOut);
}

/*
** File control method. For custom operations on an tvfs-file.
*/
static int tvfsFileControl(sqlite3_file *pFile, int op, void *pArg){
  TestvfsFile *p = (TestvfsFile *)pFile;
  return sqlite3OsFileControl(p->pReal, op, pArg);
}

/*
** Return the sector-size in bytes for an tvfs-file.
*/
static int tvfsSectorSize(sqlite3_file *pFile){
  TestvfsFile *p = (TestvfsFile *)pFile;
  return sqlite3OsSectorSize(p->pReal);
}

/*
** Return the device characteristic flags supported by an tvfs-file.
*/
static int tvfsDeviceCharacteristics(sqlite3_file *pFile){
  TestvfsFile *p = (TestvfsFile *)pFile;
  return sqlite3OsDeviceCharacteristics(p->pReal);
}

/*
** Open an tvfs file handle.
*/
static int tvfsOpen(
  sqlite3_vfs *pVfs,
  const char *zName,
  sqlite3_file *pFile,
  int flags,
  int *pOutFlags
){
  int rc;
  TestvfsFile *pFd = (TestvfsFile *)pFile;
  Tcl_Obj *pId = 0;
  Testvfs *p = (Testvfs *)pVfs->pAppData;

  pFd->pShm = 0;
  pFd->pShmId = 0;
  pFd->zFilename = zName;
  pFd->pVfs = pVfs;
  pFd->pReal = (sqlite3_file *)&pFd[1];

  /* Evaluate the Tcl script: 
  **
  **   SCRIPT xOpen FILENAME
  **
  ** If the script returns an SQLite error code other than SQLITE_OK, an
  ** error is returned to the caller. If it returns SQLITE_OK, the new
  ** connection is named "anon". Otherwise, the value returned by the
  ** script is used as the connection name.
  */
  Tcl_ResetResult(p->interp);
  if( p->pScript && p->mask&TESTVFS_OPEN_MASK ){
    tvfsExecTcl(p, "xOpen", Tcl_NewStringObj(pFd->zFilename, -1), 0, 0);
    if( tvfsResultCode(p, &rc) ){
      if( rc!=SQLITE_OK ) return rc;
    }else{
      pId = Tcl_GetObjResult(p->interp);
    }
  }
  if( !pId ){
    pId = Tcl_NewStringObj("anon", -1);
  }
  Tcl_IncrRefCount(pId);
  pFd->pShmId = pId;
  Tcl_ResetResult(p->interp);

  rc = sqlite3OsOpen(PARENTVFS(pVfs), zName, pFd->pReal, flags, pOutFlags);
  if( pFd->pReal->pMethods ){
    sqlite3_io_methods *pMethods;
    pMethods = (sqlite3_io_methods *)ckalloc(sizeof(sqlite3_io_methods));
    memcpy(pMethods, &tvfs_io_methods, sizeof(sqlite3_io_methods));
    if( ((Testvfs *)pVfs->pAppData)->isNoshm ){
      pMethods->xShmOpen = 0;
      pMethods->xShmGet = 0;
      pMethods->xShmSize = 0;
      pMethods->xShmRelease = 0;
      pMethods->xShmClose = 0;
      pMethods->xShmLock = 0;
      pMethods->xShmBarrier = 0;
    }
    pFile->pMethods = pMethods;
  }

  return rc;
}

/*
** Delete the file located at zPath. If the dirSync argument is true,
** ensure the file-system modifications are synced to disk before
** returning.
*/
static int tvfsDelete(sqlite3_vfs *pVfs, const char *zPath, int dirSync){
  return sqlite3OsDelete(PARENTVFS(pVfs), zPath, dirSync);
}

/*
** Test for access permissions. Return true if the requested permission
** is available, or false otherwise.
*/
static int tvfsAccess(
  sqlite3_vfs *pVfs, 
  const char *zPath, 
  int flags, 
  int *pResOut
){
  return sqlite3OsAccess(PARENTVFS(pVfs), zPath, flags, pResOut);
}

/*
** Populate buffer zOut with the full canonical pathname corresponding
** to the pathname in zPath. zOut is guaranteed to point to a buffer
** of at least (DEVSYM_MAX_PATHNAME+1) bytes.
*/
static int tvfsFullPathname(
  sqlite3_vfs *pVfs, 
  const char *zPath, 
  int nOut, 
  char *zOut
){
  return sqlite3OsFullPathname(PARENTVFS(pVfs), zPath, nOut, zOut);
}

#ifndef SQLITE_OMIT_LOAD_EXTENSION
/*
** Open the dynamic library located at zPath and return a handle.
*/
static void *tvfsDlOpen(sqlite3_vfs *pVfs, const char *zPath){
  return sqlite3OsDlOpen(PARENTVFS(pVfs), zPath);
}

/*
** Populate the buffer zErrMsg (size nByte bytes) with a human readable
** utf-8 string describing the most recent error encountered associated 
** with dynamic libraries.
*/
static void tvfsDlError(sqlite3_vfs *pVfs, int nByte, char *zErrMsg){
  sqlite3OsDlError(PARENTVFS(pVfs), nByte, zErrMsg);
}

/*
** Return a pointer to the symbol zSymbol in the dynamic library pHandle.
*/
static void (*tvfsDlSym(sqlite3_vfs *pVfs, void *p, const char *zSym))(void){
  return sqlite3OsDlSym(PARENTVFS(pVfs), p, zSym);
}

/*
** Close the dynamic library handle pHandle.
*/
static void tvfsDlClose(sqlite3_vfs *pVfs, void *pHandle){
  sqlite3OsDlClose(PARENTVFS(pVfs), pHandle);
}
#endif /* SQLITE_OMIT_LOAD_EXTENSION */

/*
** Populate the buffer pointed to by zBufOut with nByte bytes of 
** random data.
*/
static int tvfsRandomness(sqlite3_vfs *pVfs, int nByte, char *zBufOut){
  return sqlite3OsRandomness(PARENTVFS(pVfs), nByte, zBufOut);
}

/*
** Sleep for nMicro microseconds. Return the number of microseconds 
** actually slept.
*/
static int tvfsSleep(sqlite3_vfs *pVfs, int nMicro){
  return sqlite3OsSleep(PARENTVFS(pVfs), nMicro);
}

/*
** Return the current time as a Julian Day number in *pTimeOut.
*/
static int tvfsCurrentTime(sqlite3_vfs *pVfs, double *pTimeOut){
  return PARENTVFS(pVfs)->xCurrentTime(PARENTVFS(pVfs), pTimeOut);
}

static void tvfsGrowBuffer(TestvfsFile *pFd, int reqSize, int *pNewSize){
  TestvfsBuffer *pBuffer = pFd->pShm;
  if( reqSize>pBuffer->n ){
    pBuffer->a = (u8 *)ckrealloc((char *)pBuffer->a, reqSize);
    memset(&pBuffer->a[pBuffer->n], 0x55, reqSize-pBuffer->n);
    pBuffer->n = reqSize;
  }
  *pNewSize = pBuffer->n;
}

static int tvfsInjectIoerr(Testvfs *p){
  int ret = 0;
  if( p->ioerr ){
    p->iIoerrCnt--;
    if( p->iIoerrCnt==0 || (p->iIoerrCnt<0 && p->ioerr==2) ){
      ret = 1;
      p->nIoerrFail++;
    }
  }
  return ret;
}

static int tvfsShmOpen(
  sqlite3_file *pFileDes
){
  Testvfs *p;
  int rc = SQLITE_OK;             /* Return code */
  TestvfsBuffer *pBuffer;         /* Buffer to open connection to */
  TestvfsFile *pFd;               /* The testvfs file structure */

  pFd = (TestvfsFile*)pFileDes;
  p = (Testvfs *)pFd->pVfs->pAppData;
  assert( pFd->pShmId && pFd->pShm==0 );

  /* Evaluate the Tcl script: 
  **
  **   SCRIPT xShmOpen FILENAME
  */
  Tcl_ResetResult(p->interp);
  if( p->pScript && p->mask&TESTVFS_SHMOPEN_MASK ){
    tvfsExecTcl(p, "xShmOpen", Tcl_NewStringObj(pFd->zFilename, -1), 0, 0);
    if( tvfsResultCode(p, &rc) ){
      if( rc!=SQLITE_OK ) return rc;
    }
  }

  assert( rc==SQLITE_OK );
  if( p->mask&TESTVFS_SHMOPEN_MASK && tvfsInjectIoerr(p) ){
    return SQLITE_IOERR;
  }

  /* Search for a TestvfsBuffer. Create a new one if required. */
  for(pBuffer=p->pBuffer; pBuffer; pBuffer=pBuffer->pNext){
    if( 0==strcmp(pFd->zFilename, pBuffer->zFile) ) break;
  }
  if( !pBuffer ){
    int nByte = sizeof(TestvfsBuffer) + strlen(pFd->zFilename) + 1;
    pBuffer = (TestvfsBuffer *)ckalloc(nByte);
    memset(pBuffer, 0, nByte);
    pBuffer->zFile = (char *)&pBuffer[1];
    strcpy(pBuffer->zFile, pFd->zFilename);
    pBuffer->pNext = p->pBuffer;
    p->pBuffer = pBuffer;
  }

  /* Connect the TestvfsBuffer to the new TestvfsShm handle and return. */
  pBuffer->nRef++;
  pFd->pShm = pBuffer;
  return SQLITE_OK;
}

static int tvfsShmSize(
  sqlite3_file *pFile,
  int reqSize,
  int *pNewSize
){
  int rc = SQLITE_OK;
  TestvfsFile *pFd = (TestvfsFile *)pFile;
  Testvfs *p = (Testvfs *)(pFd->pVfs->pAppData);

  if( p->pScript && p->mask&TESTVFS_SHMSIZE_MASK ){
    tvfsExecTcl(p, "xShmSize", 
        Tcl_NewStringObj(pFd->pShm->zFile, -1), pFd->pShmId, 0
    );
    tvfsResultCode(p, &rc);
  }
  if( rc==SQLITE_OK && p->mask&TESTVFS_SHMSIZE_MASK && tvfsInjectIoerr(p) ){
    rc = SQLITE_IOERR;
  }
  if( rc==SQLITE_OK ){
    tvfsGrowBuffer(pFd, reqSize, pNewSize);
  }
  return rc;
}

static int tvfsShmGet(
  sqlite3_file *pFile, 
  int reqMapSize, 
  int *pMapSize, 
  volatile void **pp
){
  int rc = SQLITE_OK;
  TestvfsFile *pFd = (TestvfsFile *)pFile;
  Testvfs *p = (Testvfs *)(pFd->pVfs->pAppData);

  if( p->pScript && p->mask&TESTVFS_SHMGET_MASK ){
    tvfsExecTcl(p, "xShmGet", 
        Tcl_NewStringObj(pFd->pShm->zFile, -1), pFd->pShmId, 
        Tcl_NewIntObj(reqMapSize)
    );
    tvfsResultCode(p, &rc);
  }
  if( rc==SQLITE_OK && p->mask&TESTVFS_SHMGET_MASK && tvfsInjectIoerr(p) ){
    rc = SQLITE_IOERR;
  }

  *pMapSize = pFd->pShm->n;
  *pp = pFd->pShm->a;
  return rc;
}

static int tvfsShmRelease(sqlite3_file *pFile){
  int rc = SQLITE_OK;
  TestvfsFile *pFd = (TestvfsFile *)pFile;
  Testvfs *p = (Testvfs *)(pFd->pVfs->pAppData);

  if( p->pScript && p->mask&TESTVFS_SHMRELEASE_MASK ){
    tvfsExecTcl(p, "xShmRelease", 
        Tcl_NewStringObj(pFd->pShm->zFile, -1), pFd->pShmId, 0
    );
    tvfsResultCode(p, &rc);
  }

  return rc;
}

static int tvfsShmLock(
  sqlite3_file *pFile,
  int ofst,
  int n,
  int flags
){
  int rc = SQLITE_OK;
  TestvfsFile *pFd = (TestvfsFile *)pFile;
  Testvfs *p = (Testvfs *)(pFd->pVfs->pAppData);
  int nLock;
  char zLock[80];

  if( p->pScript && p->mask&TESTVFS_SHMLOCK_MASK ){
    sqlite3_snprintf(sizeof(zLock), zLock, "%d %d", ofst, n);
    nLock = strlen(zLock);
    if( flags & SQLITE_SHM_LOCK ){
      strcpy(&zLock[nLock], " lock");
    }else{
      strcpy(&zLock[nLock], " unlock");
    }
    nLock += strlen(&zLock[nLock]);
    if( flags & SQLITE_SHM_SHARED ){
      strcpy(&zLock[nLock], " shared");
    }else{
      strcpy(&zLock[nLock], " exclusive");
    }
    tvfsExecTcl(p, "xShmLock", 
        Tcl_NewStringObj(pFd->pShm->zFile, -1), pFd->pShmId,
        Tcl_NewStringObj(zLock, -1)
    );
    tvfsResultCode(p, &rc);
  }

  if( rc==SQLITE_OK && p->mask&TESTVFS_SHMLOCK_MASK && tvfsInjectIoerr(p) ){
    rc = SQLITE_IOERR;
  }
  return rc;
}

static void tvfsShmBarrier(sqlite3_file *pFile){
  TestvfsFile *pFd = (TestvfsFile *)pFile;
  Testvfs *p = (Testvfs *)(pFd->pVfs->pAppData);

  if( p->pScript && p->mask&TESTVFS_SHMBARRIER_MASK ){
    tvfsExecTcl(p, "xShmBarrier", 
        Tcl_NewStringObj(pFd->pShm->zFile, -1), pFd->pShmId, 0
    );
  }
}

static int tvfsShmClose(
  sqlite3_file *pFile,
  int deleteFlag
){
  int rc = SQLITE_OK;
  TestvfsFile *pFd = (TestvfsFile *)pFile;
  Testvfs *p = (Testvfs *)(pFd->pVfs->pAppData);
  TestvfsBuffer *pBuffer = pFd->pShm;

  assert( pFd->pShmId && pFd->pShm );
#if 0
  assert( (deleteFlag!=0)==(pBuffer->nRef==1) );
#endif

  if( p->pScript && p->mask&TESTVFS_SHMCLOSE_MASK ){
    tvfsExecTcl(p, "xShmClose", 
        Tcl_NewStringObj(pFd->pShm->zFile, -1), pFd->pShmId, 0
    );
    tvfsResultCode(p, &rc);
  }

  pBuffer->nRef--;
  if( pBuffer->nRef==0 ){
    TestvfsBuffer **pp;
    for(pp=&p->pBuffer; *pp!=pBuffer; pp=&((*pp)->pNext));
    *pp = (*pp)->pNext;
    ckfree((char *)pBuffer->a);
    ckfree((char *)pBuffer);
  }
  pFd->pShm = 0;

  return rc;
}

static int testvfs_obj_cmd(
  ClientData cd,
  Tcl_Interp *interp,
  int objc,
  Tcl_Obj *CONST objv[]
){
  Testvfs *p = (Testvfs *)cd;

  static const char *CMD_strs[] = { 
    "shm",   "delete",   "filter",   "ioerr",   "script", 0 
  };
  enum DB_enum { 
    CMD_SHM, CMD_DELETE, CMD_FILTER, CMD_IOERR, CMD_SCRIPT
  };

  int i;
  
  if( objc<2 ){
    Tcl_WrongNumArgs(interp, 1, objv, "SUBCOMMAND ...");
    return TCL_ERROR;
  }
  if( Tcl_GetIndexFromObj(interp, objv[1], CMD_strs, "subcommand", 0, &i) ){
    return TCL_ERROR;
  }
  Tcl_ResetResult(interp);

  switch( (enum DB_enum)i ){
    case CMD_SHM: {
      TestvfsBuffer *pBuffer;
      char *zName;
      if( objc!=3 && objc!=4 ){
        Tcl_WrongNumArgs(interp, 2, objv, "FILE ?VALUE?");
        return TCL_ERROR;
      }
      zName = Tcl_GetString(objv[2]);
      for(pBuffer=p->pBuffer; pBuffer; pBuffer=pBuffer->pNext){
        if( 0==strcmp(pBuffer->zFile, zName) ) break;
      }
      if( !pBuffer ){
        Tcl_AppendResult(interp, "no such file: ", zName, 0);
        return TCL_ERROR;
      }
      if( objc==4 ){
        int n;
        u8 *a = Tcl_GetByteArrayFromObj(objv[3], &n);
        pBuffer->a = (u8 *)ckrealloc((char *)pBuffer->a, n);
        pBuffer->n = n;
        memcpy(pBuffer->a, a, n);
      }
      Tcl_SetObjResult(interp, Tcl_NewByteArrayObj(pBuffer->a, pBuffer->n));
      break;
    }

    case CMD_FILTER: {
      static struct VfsMethod {
        char *zName;
        int mask;
      } vfsmethod [] = {
        { "xShmOpen",    TESTVFS_SHMOPEN_MASK },
        { "xShmSize",    TESTVFS_SHMSIZE_MASK },
        { "xShmGet",     TESTVFS_SHMGET_MASK },
        { "xShmRelease", TESTVFS_SHMRELEASE_MASK },
        { "xShmLock",    TESTVFS_SHMLOCK_MASK },
        { "xShmBarrier", TESTVFS_SHMBARRIER_MASK },
        { "xShmClose",   TESTVFS_SHMCLOSE_MASK },
        { "xSync",       TESTVFS_SYNC_MASK },
        { "xOpen",       TESTVFS_OPEN_MASK },
      };
      Tcl_Obj **apElem = 0;
      int nElem = 0;
      int i;
      int mask = 0;
      if( objc!=3 ){
        Tcl_WrongNumArgs(interp, 2, objv, "LIST");
        return TCL_ERROR;
      }
      if( Tcl_ListObjGetElements(interp, objv[2], &nElem, &apElem) ){
        return TCL_ERROR;
      }
      Tcl_ResetResult(interp);
      for(i=0; i<nElem; i++){
        int iMethod;
        char *zElem = Tcl_GetString(apElem[i]);
        for(iMethod=0; iMethod<ArraySize(vfsmethod); iMethod++){
          if( strcmp(zElem, vfsmethod[iMethod].zName)==0 ){
            mask |= vfsmethod[iMethod].mask;
            break;
          }
        }
        if( iMethod==ArraySize(vfsmethod) ){
          Tcl_AppendResult(interp, "unknown method: ", zElem, 0);
          return TCL_ERROR;
        }
      }
      p->mask = mask;
      break;
    }

    case CMD_SCRIPT: {
      if( objc==3 ){
        int nByte;
        if( p->pScript ){
          Tcl_DecrRefCount(p->pScript);
          ckfree((char *)p->apScript);
          p->apScript = 0;
          p->nScript = 0;
        }
        Tcl_GetStringFromObj(objv[2], &nByte);
        if( nByte>0 ){
          p->pScript = Tcl_DuplicateObj(objv[2]);
          Tcl_IncrRefCount(p->pScript);
        }
      }else if( objc!=2 ){
        Tcl_WrongNumArgs(interp, 2, objv, "?SCRIPT?");
        return TCL_ERROR;
      }

      Tcl_ResetResult(interp);
      if( p->pScript ) Tcl_SetObjResult(interp, p->pScript);

      break;
    }

    /*
    ** TESTVFS ioerr ?IFAIL PERSIST?
    **
    **   Where IFAIL is an integer and PERSIST is boolean.
    */
    case CMD_IOERR: {
      int iRet = p->nIoerrFail;

      p->nIoerrFail = 0;
      p->ioerr = 0;
      p->iIoerrCnt = 0;

      if( objc==4 ){
        int iCnt, iPersist;
        if( TCL_OK!=Tcl_GetIntFromObj(interp, objv[2], &iCnt)
         || TCL_OK!=Tcl_GetBooleanFromObj(interp, objv[3], &iPersist)
        ){
          return TCL_ERROR;
        }
        p->ioerr = (iCnt>0) + iPersist;
        p->iIoerrCnt = iCnt;
      }else if( objc!=2 ){
        Tcl_AppendResult(interp, "Bad args", 0);
        return TCL_ERROR;
      }
      Tcl_SetObjResult(interp, Tcl_NewIntObj(iRet));
      break;
    }

    case CMD_DELETE: {
      Tcl_DeleteCommand(interp, Tcl_GetString(objv[0]));
      break;
    }
  }

  return TCL_OK;
}

static void testvfs_obj_del(ClientData cd){
  Testvfs *p = (Testvfs *)cd;
  if( p->pScript ) Tcl_DecrRefCount(p->pScript);
  sqlite3_vfs_unregister(p->pVfs);
  ckfree((char *)p->apScript);
  ckfree((char *)p->pVfs);
  ckfree((char *)p);
}

/*
** Usage:  testvfs VFSNAME ?SWITCHES?
**
** Switches are:
**
**   -noshm   BOOLEAN             (True to omit shm methods. Default false)
**   -default BOOLEAN             (True to make the vfs default. Default false)
**
** This command creates two things when it is invoked: an SQLite VFS, and
** a Tcl command. Both are named VFSNAME. The VFS is installed. It is not
** installed as the default VFS.
**
** The VFS passes all file I/O calls through to the underlying VFS.
**
** Whenever one of the xShmSize, xShmGet or xShmRelease methods of the VFS
** are invoked, the SCRIPT is executed as follows:
**
**   SCRIPT xShmSize    FILENAME ID
**   SCRIPT xShmGet     FILENAME ID
**   SCRIPT xShmRelease FILENAME ID
**
** The value returned by the invocation of SCRIPT above is interpreted as
** an SQLite error code and returned to SQLite. Either a symbolic 
** "SQLITE_OK" or numeric "0" value may be returned.
**
** The contents of the shared-memory buffer associated with a given file
** may be read and set using the following command:
**
**   VFSNAME shm FILENAME ?NEWVALUE?
**
** When the xShmLock method is invoked by SQLite, the following script is
** run:
**
**   SCRIPT xShmLock    FILENAME ID LOCK
**
** where LOCK is of the form "OFFSET NBYTE lock/unlock shared/exclusive"
*/
static int testvfs_cmd(
  ClientData cd,
  Tcl_Interp *interp,
  int objc,
  Tcl_Obj *CONST objv[]
){
  static sqlite3_vfs tvfs_vfs = {
    2,                            /* iVersion */
    sizeof(TestvfsFile),            /* szOsFile */
    0,                            /* mxPathname */
    0,                            /* pNext */
    0,                            /* zName */
    0,                            /* pAppData */
    tvfsOpen,                     /* xOpen */
    tvfsDelete,                   /* xDelete */
    tvfsAccess,                   /* xAccess */
    tvfsFullPathname,             /* xFullPathname */
#ifndef SQLITE_OMIT_LOAD_EXTENSION
    tvfsDlOpen,                   /* xDlOpen */
    tvfsDlError,                  /* xDlError */
    tvfsDlSym,                    /* xDlSym */
    tvfsDlClose,                  /* xDlClose */
#else
    0,                            /* xDlOpen */
    0,                            /* xDlError */
    0,                            /* xDlSym */
    0,                            /* xDlClose */
#endif /* SQLITE_OMIT_LOAD_EXTENSION */
    tvfsRandomness,               /* xRandomness */
    tvfsSleep,                    /* xSleep */
    tvfsCurrentTime,              /* xCurrentTime */
    0,                            /* xGetLastError */
    0,
    0,
  };

  Testvfs *p;                     /* New object */
  sqlite3_vfs *pVfs;              /* New VFS */
  char *zVfs;
  int nByte;                      /* Bytes of space to allocate at p */

  int i;
  int isNoshm = 0;                /* True if -noshm is passed */
  int isDefault = 0;              /* True if -default is passed */

  if( objc<2 || 0!=(objc%2) ) goto bad_args;
  for(i=2; i<objc; i += 2){
    int nSwitch;
    char *zSwitch;

    zSwitch = Tcl_GetStringFromObj(objv[i], &nSwitch); 
    if( nSwitch>2 && 0==strncmp("-noshm", zSwitch, nSwitch) ){
      if( Tcl_GetBooleanFromObj(interp, objv[i+1], &isNoshm) ){
        return TCL_ERROR;
      }
    }
    else if( nSwitch>2 && 0==strncmp("-default", zSwitch, nSwitch) ){
      if( Tcl_GetBooleanFromObj(interp, objv[i+1], &isDefault) ){
        return TCL_ERROR;
      }
    }
    else{
      goto bad_args;
    }
  }

  zVfs = Tcl_GetString(objv[1]);
  nByte = sizeof(Testvfs) + strlen(zVfs)+1;
  p = (Testvfs *)ckalloc(nByte);
  memset(p, 0, nByte);

  p->pParent = sqlite3_vfs_find(0);
  p->interp = interp;

  p->zName = (char *)&p[1];
  memcpy(p->zName, zVfs, strlen(zVfs)+1);

  pVfs = (sqlite3_vfs *)ckalloc(sizeof(sqlite3_vfs));
  memcpy(pVfs, &tvfs_vfs, sizeof(sqlite3_vfs));
  pVfs->pAppData = (void *)p;
  pVfs->zName = p->zName;
  pVfs->mxPathname = p->pParent->mxPathname;
  pVfs->szOsFile += p->pParent->szOsFile;
  p->pVfs = pVfs;
  p->isNoshm = isNoshm;
  p->mask = TESTVFS_ALL_MASK;

  Tcl_CreateObjCommand(interp, zVfs, testvfs_obj_cmd, p, testvfs_obj_del);
  sqlite3_vfs_register(pVfs, isDefault);

  return TCL_OK;

 bad_args:
  Tcl_WrongNumArgs(interp, 1, objv, "VFSNAME ?-noshm BOOL? ?-default BOOL?");
  return TCL_ERROR;
}

int Sqlitetestvfs_Init(Tcl_Interp *interp){
  Tcl_CreateObjCommand(interp, "testvfs", testvfs_cmd, 0, 0);
  return TCL_OK;
}

#endif

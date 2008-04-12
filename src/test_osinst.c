/*
** 2008 April 10
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
** This file contains the implementation of an SQLite vfs wrapper that
** adds instrumentation to all vfs and file methods. C and Tcl interfaces
** are provided to control the instrumentation.
*/

/*
** C interface:
**
**   sqlite3_instvfs_create()
**   sqlite3_instvfs_destroy()
**   sqlite3_instvfs_configure()
**
**   sqlite3_instvfs_reset()
**   sqlite3_instvfs_get()
**
**   sqlite3_instvfs_binarylog
**
** Tcl interface (omitted if SQLITE_TEST is not set):
** 
**   sqlite3_instvfs create NAME ?PARENT?
**
**       Create and register new vfs called $NAME, which is a wrapper around
**       the existing vfs $PARENT. If the PARENT argument is omitted, the
**       new vfs is a wrapper around the current default vfs.
**
**   sqlite3_instvfs destroy NAME
**
**       Deregister and destroy the vfs named $NAME, which must have been
**       created by an earlier invocation of [sqlite3_instvfs create].
**
**   sqlite3_instvfs configure NAME SCRIPT
**
**       Configure the callback script for the vfs $NAME, which much have
**       been created by an earlier invocation of [sqlite3_instvfs create].
**       After a callback script has been configured, it is invoked each
**       time a vfs or file method is called by SQLite. Before invoking
**       the callback script, five arguments are appended to it:
**
**         * The name of the invoked method - i.e. "xRead".
**
**         * The time consumed by the method call as measured by hwtime() (an
**           integer value)
**
**         * A string value with a different meaning for different calls. 
**           For file methods, the name of the file being operated on. For
**           other methods it is the filename argument, if any.
**
**         * A 32-bit integer value with a call-specific meaning.
**
**         * A 64-bit integer value. For xRead() and xWrite() calls this
**           is the file offset being written to or read from. Unused by
**           all other calls.
**
**   sqlite3_instvfs reset NAME
**
**       Zero the internal event counters associated with vfs $NAME, 
**       which must have been created by an earlier invocation of 
**       [sqlite3_instvfs create].
**
**   sqlite3_instvfs report NAME
**
**       Return the values of the internal event counters associated 
**       with vfs $NAME. The report format is a list with one element
**       for each method call (xWrite, xRead etc.). Each element is
**       itself a list with three elements:
**
**         * The name of the method call - i.e. "xWrite",
**         * The total number of calls to the method (an integer).
**         * The aggregate time consumed by all calls to the method as
**           measured by hwtime() (an integer).
*/

#include "sqlite3.h"
#include <string.h>
#include <assert.h>

/*
** Maximum pathname length supported by the inst backend.
*/
#define INST_MAX_PATHNAME 512


/* File methods */
/* Vfs methods */
#define OS_ACCESS            1
#define OS_CHECKRESERVEDLOCK 2
#define OS_CLOSE             3
#define OS_CURRENTTIME       4
#define OS_DELETE            5
#define OS_DEVCHAR           6
#define OS_FILECONTROL       7
#define OS_FILESIZE          8
#define OS_FULLPATHNAME      9
#define OS_GETTEMPNAME       10
#define OS_LOCK              11
#define OS_OPEN              12
#define OS_RANDOMNESS        13
#define OS_READ              14 
#define OS_SECTORSIZE        15
#define OS_SLEEP             16
#define OS_SYNC              17
#define OS_TRUNCATE          18
#define OS_UNLOCK            19
#define OS_WRITE             20

#define OS_NUMEVENTS         21

struct InstVfs {
  sqlite3_vfs base;
  sqlite3_vfs *pVfs;
  
  void *pClient;
  void (*xDel)(void *);
  void (*xCall)(void *, int, sqlite3_int64, const char *, int, int, sqlite3_int64);

  /* Counters */
  sqlite3_int64 aTime[OS_NUMEVENTS];
  int aCount[OS_NUMEVENTS];
};
typedef struct InstVfs InstVfs;

#define REALVFS(p) (((InstVfs *)(p))->pVfs)

typedef struct inst_file inst_file;
struct inst_file {
  sqlite3_file base;
  sqlite3_file *pReal;
  InstVfs *pInstVfs;
  const char *zName;
  int flags;
};

/*
** Method declarations for inst_file.
*/
static int instClose(sqlite3_file*);
static int instRead(sqlite3_file*, void*, int iAmt, sqlite3_int64 iOfst);
static int instWrite(sqlite3_file*,const void*,int iAmt, sqlite3_int64 iOfst);
static int instTruncate(sqlite3_file*, sqlite3_int64 size);
static int instSync(sqlite3_file*, int flags);
static int instFileSize(sqlite3_file*, sqlite3_int64 *pSize);
static int instLock(sqlite3_file*, int);
static int instUnlock(sqlite3_file*, int);
static int instCheckReservedLock(sqlite3_file*);
static int instFileControl(sqlite3_file*, int op, void *pArg);
static int instSectorSize(sqlite3_file*);
static int instDeviceCharacteristics(sqlite3_file*);

/*
** Method declarations for inst_vfs.
*/
static int instOpen(sqlite3_vfs*, const char *, sqlite3_file*, int , int *);
static int instDelete(sqlite3_vfs*, const char *zName, int syncDir);
static int instAccess(sqlite3_vfs*, const char *zName, int flags);
static int instGetTempName(sqlite3_vfs*, int nOut, char *zOut);
static int instFullPathname(sqlite3_vfs*, const char *zName, int, char *zOut);
static void *instDlOpen(sqlite3_vfs*, const char *zFilename);
static void instDlError(sqlite3_vfs*, int nByte, char *zErrMsg);
static void *instDlSym(sqlite3_vfs*,void*, const char *zSymbol);
static void instDlClose(sqlite3_vfs*, void*);
static int instRandomness(sqlite3_vfs*, int nByte, char *zOut);
static int instSleep(sqlite3_vfs*, int microseconds);
static int instCurrentTime(sqlite3_vfs*, double*);

static sqlite3_vfs inst_vfs = {
  1,                      /* iVersion */
  sizeof(inst_file),      /* szOsFile */
  INST_MAX_PATHNAME,      /* mxPathname */
  0,                      /* pNext */
  0,                      /* zName */
  0,                      /* pAppData */
  instOpen,               /* xOpen */
  instDelete,             /* xDelete */
  instAccess,             /* xAccess */
  instGetTempName,        /* xGetTempName */
  instFullPathname,       /* xFullPathname */
  instDlOpen,             /* xDlOpen */
  instDlError,            /* xDlError */
  instDlSym,              /* xDlSym */
  instDlClose,            /* xDlClose */
  instRandomness,         /* xRandomness */
  instSleep,              /* xSleep */
  instCurrentTime         /* xCurrentTime */
};

static sqlite3_io_methods inst_io_methods = {
  1,                            /* iVersion */
  instClose,                      /* xClose */
  instRead,                       /* xRead */
  instWrite,                      /* xWrite */
  instTruncate,                   /* xTruncate */
  instSync,                       /* xSync */
  instFileSize,                   /* xFileSize */
  instLock,                       /* xLock */
  instUnlock,                     /* xUnlock */
  instCheckReservedLock,          /* xCheckReservedLock */
  instFileControl,                /* xFileControl */
  instSectorSize,                 /* xSectorSize */
  instDeviceCharacteristics       /* xDeviceCharacteristics */
};

/*
** The following routine only works on pentium-class processors.
** It uses the RDTSC opcode to read the cycle count value out of the
** processor and returns that value.  This can be used for high-res
** profiling.
*/
#if defined(i386) || defined(__i386__) || defined(_M_IX86)
__inline__ unsigned long long int osinst_hwtime(void){
   unsigned int lo, hi;
   /* We cannot use "=A", since this would use %rax on x86_64 */
   __asm__ __volatile__ ("rdtsc" : "=a" (lo), "=d" (hi));
   return (unsigned long long int)hi << 32 | lo;
}
#else
  static unsigned long long int osinst_hwtime(void){ return 0; }
#endif

#define OS_TIME_IO(eEvent, A, B, Call) {     \
  inst_file *p = (inst_file *)pFile;         \
  InstVfs *pInstVfs = p->pInstVfs;           \
  int rc;                                    \
  sqlite3_int64 t = osinst_hwtime();         \
  rc = Call;                                 \
  t = osinst_hwtime() - t;                   \
  pInstVfs->aTime[eEvent] += t;              \
  pInstVfs->aCount[eEvent] += 1;             \
  if( pInstVfs->xCall ){                     \
    pInstVfs->xCall(pInstVfs->pClient, eEvent, t, p->zName, p->flags, A, B); \
  }                                          \
  return rc;                                 \
}

#define OS_TIME_VFS(eEvent, Z, flags, A, B, Call) {      \
  InstVfs *pInstVfs = (InstVfs *)pVfs;   \
  int rc;                                \
  sqlite3_int64 t = hwtime();                      \
  rc = Call;                             \
  t = hwtime() - t;                      \
  pInstVfs->aTime[eEvent] += t;          \
  pInstVfs->aCount[eEvent] += 1;         \
  if( pInstVfs->xCall ){                 \
    pInstVfs->xCall(pInstVfs->pClient, eEvent, t, Z, flags, A, B); \
  }                                      \
  return rc;                             \
}

/*
** Close an inst-file.
*/
static int instClose(sqlite3_file *pFile){
  OS_TIME_IO(OS_CLOSE, 0, 0, p->pReal->pMethods->xClose(p->pReal));
}

/*
** Read data from an inst-file.
*/
static int instRead(
  sqlite3_file *pFile, 
  void *zBuf, 
  int iAmt, 
  sqlite_int64 iOfst
){
  OS_TIME_IO(OS_READ, iAmt, iOfst, p->pReal->pMethods->xRead(p->pReal, zBuf, iAmt, iOfst));
}

/*
** Write data to an inst-file.
*/
static int instWrite(
  sqlite3_file *pFile,
  const void *z,
  int iAmt,
  sqlite_int64 iOfst
){
  OS_TIME_IO(OS_WRITE, iAmt, iOfst, p->pReal->pMethods->xWrite(p->pReal, z, iAmt, iOfst));
}

/*
** Truncate an inst-file.
*/
static int instTruncate(sqlite3_file *pFile, sqlite_int64 size){
  OS_TIME_IO(OS_TRUNCATE, 0, size, p->pReal->pMethods->xTruncate(p->pReal, size));
}

/*
** Sync an inst-file.
*/
static int instSync(sqlite3_file *pFile, int flags){
  OS_TIME_IO(OS_SYNC, flags, 0, p->pReal->pMethods->xSync(p->pReal, flags));
}

/*
** Return the current file-size of an inst-file.
*/
static int instFileSize(sqlite3_file *pFile, sqlite_int64 *pSize){
  OS_TIME_IO(OS_FILESIZE, 0, 0, p->pReal->pMethods->xFileSize(p->pReal, pSize));
}

/*
** Lock an inst-file.
*/
static int instLock(sqlite3_file *pFile, int eLock){
  OS_TIME_IO(OS_LOCK, eLock, 0, p->pReal->pMethods->xLock(p->pReal, eLock));
}

/*
** Unlock an inst-file.
*/
static int instUnlock(sqlite3_file *pFile, int eLock){
  OS_TIME_IO(OS_UNLOCK, eLock, 0, p->pReal->pMethods->xUnlock(p->pReal, eLock));
}

/*
** Check if another file-handle holds a RESERVED lock on an inst-file.
*/
static int instCheckReservedLock(sqlite3_file *pFile){
  OS_TIME_IO(OS_CHECKRESERVEDLOCK, 0, 0, p->pReal->pMethods->xCheckReservedLock(p->pReal));
}

/*
** File control method. For custom operations on an inst-file.
*/
static int instFileControl(sqlite3_file *pFile, int op, void *pArg){
  OS_TIME_IO(OS_FILECONTROL, 0, 0, p->pReal->pMethods->xFileControl(p->pReal, op, pArg));
}

/*
** Return the sector-size in bytes for an inst-file.
*/
static int instSectorSize(sqlite3_file *pFile){
  OS_TIME_IO(OS_SECTORSIZE, 0, 0, p->pReal->pMethods->xSectorSize(p->pReal));
}

/*
** Return the device characteristic flags supported by an inst-file.
*/
static int instDeviceCharacteristics(sqlite3_file *pFile){
  OS_TIME_IO(OS_DEVCHAR, 0, 0, p->pReal->pMethods->xDeviceCharacteristics(p->pReal));
}

/*
** Open an inst file handle.
*/
static int instOpen(
  sqlite3_vfs *pVfs,
  const char *zName,
  sqlite3_file *pFile,
  int flags,
  int *pOutFlags
){
  inst_file *p = (inst_file *)pFile;
  pFile->pMethods = &inst_io_methods;
  p->pReal = (sqlite3_file *)&p[1];
  p->pInstVfs = (InstVfs *)pVfs;
  p->zName = zName;
  p->flags = flags;

  OS_TIME_VFS(OS_OPEN, zName, flags, 0, 0,
    REALVFS(pVfs)->xOpen(REALVFS(pVfs), zName, p->pReal, flags, pOutFlags)
  );
}

/*
** Delete the file located at zPath. If the dirSync argument is true,
** ensure the file-system modifications are synced to disk before
** returning.
*/
static int instDelete(sqlite3_vfs *pVfs, const char *zPath, int dirSync){
  OS_TIME_VFS(OS_DELETE, zPath, 0, dirSync, 0,
    REALVFS(pVfs)->xDelete(REALVFS(pVfs), zPath, dirSync) 
  );
}

/*
** Test for access permissions. Return true if the requested permission
** is available, or false otherwise.
*/
static int instAccess(sqlite3_vfs *pVfs, const char *zPath, int flags){
  OS_TIME_VFS(OS_ACCESS, zPath, 0, flags, 0, 
    REALVFS(pVfs)->xAccess(REALVFS(pVfs), zPath, flags) 
  );
}

/*
** Populate buffer zBufOut with a pathname suitable for use as a 
** temporary file. zBufOut is guaranteed to point to a buffer of 
** at least (INST_MAX_PATHNAME+1) bytes.
*/
static int instGetTempName(sqlite3_vfs *pVfs, int nOut, char *zBufOut){
  OS_TIME_VFS( OS_GETTEMPNAME, 0, 0, 0, 0,
    REALVFS(pVfs)->xGetTempname(REALVFS(pVfs), nOut, zBufOut);
  );
}

/*
** Populate buffer zOut with the full canonical pathname corresponding
** to the pathname in zPath. zOut is guaranteed to point to a buffer
** of at least (INST_MAX_PATHNAME+1) bytes.
*/
static int instFullPathname(
  sqlite3_vfs *pVfs, 
  const char *zPath, 
  int nOut, 
  char *zOut
){
  OS_TIME_VFS( OS_FULLPATHNAME, zPath, 0, 0, 0,
    REALVFS(pVfs)->xFullPathname(REALVFS(pVfs), zPath, nOut, zOut);
  );
}

/*
** Open the dynamic library located at zPath and return a handle.
*/
static void *instDlOpen(sqlite3_vfs *pVfs, const char *zPath){
  return REALVFS(pVfs)->xDlOpen(REALVFS(pVfs), zPath);
}

/*
** Populate the buffer zErrMsg (size nByte bytes) with a human readable
** utf-8 string describing the most recent error encountered associated 
** with dynamic libraries.
*/
static void instDlError(sqlite3_vfs *pVfs, int nByte, char *zErrMsg){
  REALVFS(pVfs)->xDlError(REALVFS(pVfs), nByte, zErrMsg);
}

/*
** Return a pointer to the symbol zSymbol in the dynamic library pHandle.
*/
static void *instDlSym(sqlite3_vfs *pVfs, void *pHandle, const char *zSymbol){
  return REALVFS(pVfs)->xDlSym(REALVFS(pVfs), pHandle, zSymbol);
}

/*
** Close the dynamic library handle pHandle.
*/
static void instDlClose(sqlite3_vfs *pVfs, void *pHandle){
  REALVFS(pVfs)->xDlClose(REALVFS(pVfs), pHandle);
}

/*
** Populate the buffer pointed to by zBufOut with nByte bytes of 
** random data.
*/
static int instRandomness(sqlite3_vfs *pVfs, int nByte, char *zBufOut){
  OS_TIME_VFS( OS_RANDOMNESS, 0, 0, nByte, 0,
    REALVFS(pVfs)->xRandomness(REALVFS(pVfs), nByte, zBufOut);
  );
}

/*
** Sleep for nMicro microseconds. Return the number of microseconds 
** actually slept.
*/
static int instSleep(sqlite3_vfs *pVfs, int nMicro){
  OS_TIME_VFS( OS_SLEEP, 0, 0, nMicro, 0, 
    REALVFS(pVfs)->xSleep(REALVFS(pVfs), nMicro) 
  );
}

/*
** Return the current time as a Julian Day number in *pTimeOut.
*/
static int instCurrentTime(sqlite3_vfs *pVfs, double *pTimeOut){
  OS_TIME_VFS( OS_CURRENTTIME, 0, 0, 0, 0,
    REALVFS(pVfs)->xCurrentTime(REALVFS(pVfs), pTimeOut) 
  );
}

sqlite3_vfs *sqlite3_instvfs_create(const char *zName, const char *zParent){
  int nByte;
  InstVfs *p;
  sqlite3_vfs *pParent;

  pParent = sqlite3_vfs_find(zParent);
  if( !pParent ){
    return 0;
  }

  nByte = strlen(zName) + 1 + sizeof(InstVfs);
  p = (InstVfs *)sqlite3_malloc(nByte);
  if( p ){
    char *zCopy = (char *)&p[1];
    memset(p, 0, nByte);
    memcpy(p, &inst_vfs, sizeof(sqlite3_vfs));
    p->pVfs = pParent;
    memcpy(zCopy, zName, strlen(zName));
    p->base.zName = (const char *)zCopy;
    p->base.szOsFile += pParent->szOsFile;
    sqlite3_vfs_register((sqlite3_vfs *)p, 0);
  }

  return (sqlite3_vfs *)p;
}

void sqlite3_instvfs_configure(
  sqlite3_vfs *pVfs,
  void (*xCall)(void*, int, sqlite3_int64, const char*, int, int, sqlite3_int64),
  void *pClient,
  void (*xDel)(void *)
){
  InstVfs *p = (InstVfs *)pVfs;
  assert( pVfs->xOpen==instOpen );
  if( p->xDel ){
    p->xDel(p->pClient);
  }
  p->xCall = xCall;
  p->xDel = xDel;
  p->pClient = pClient;
}

void sqlite3_instvfs_destroy(sqlite3_vfs *pVfs){
  if( pVfs ){
    sqlite3_vfs_unregister(pVfs);
    sqlite3_instvfs_configure(pVfs, 0, 0, 0);
    sqlite3_free(pVfs);
  }
}

void sqlite3_instvfs_reset(sqlite3_vfs *pVfs){
  InstVfs *p = (InstVfs *)pVfs;
  assert( pVfs->xOpen==instOpen );
  memset(p->aTime, 0, sizeof(sqlite3_int64)*OS_NUMEVENTS);
  memset(p->aCount, 0, sizeof(int)*OS_NUMEVENTS);
}

const char *sqlite3_instvfs_name(int eEvent){
  const char *zEvent = 0;

  switch( eEvent ){
    case OS_CLOSE:             zEvent = "xClose"; break;
    case OS_READ:              zEvent = "xRead"; break;
    case OS_WRITE:             zEvent = "xWrite"; break;
    case OS_TRUNCATE:          zEvent = "xTruncate"; break;
    case OS_SYNC:              zEvent = "xSync"; break;
    case OS_FILESIZE:          zEvent = "xFilesize"; break;
    case OS_LOCK:              zEvent = "xLock"; break;
    case OS_UNLOCK:            zEvent = "xUnlock"; break;
    case OS_CHECKRESERVEDLOCK: zEvent = "xCheckReservedLock"; break;
    case OS_FILECONTROL:       zEvent = "xFileControl"; break;
    case OS_SECTORSIZE:        zEvent = "xSectorSize"; break;
    case OS_DEVCHAR:           zEvent = "xDeviceCharacteristics"; break;
    case OS_OPEN:              zEvent = "xOpen"; break;
    case OS_DELETE:            zEvent = "xDelete"; break;
    case OS_ACCESS:            zEvent = "xAccess"; break;
    case OS_GETTEMPNAME:       zEvent = "xGetTempName"; break;
    case OS_FULLPATHNAME:      zEvent = "xFullPathname"; break;
    case OS_RANDOMNESS:        zEvent = "xRandomness"; break;
    case OS_SLEEP:             zEvent = "xSleep"; break;
    case OS_CURRENTTIME:       zEvent = "xCurrentTime"; break;
  }

  return zEvent;
}

void sqlite3_instvfs_get(
  sqlite3_vfs *pVfs, 
  int eEvent, 
  const char **pzEvent, 
  sqlite3_int64 *pnClick, 
  int *pnCall
){
  InstVfs *p = (InstVfs *)pVfs;
  assert( pVfs->xOpen==instOpen );
  if( eEvent<1 || eEvent>=OS_NUMEVENTS ){
    *pzEvent = 0;
    *pnClick = 0;
    *pnCall = 0;
    return;
  }

  *pzEvent = sqlite3_instvfs_name(eEvent);
  *pnClick = p->aTime[eEvent];
  *pnCall = p->aCount[eEvent];
}

#define BINARYLOG_BUFFERSIZE 1024

struct InstVfsBinaryLog {
  int nBuf;
  char *zBuf;
  sqlite3_int64 iOffset;
  sqlite3_file *pOut;
  char *zOut;                       /* Log file name */
};
typedef struct InstVfsBinaryLog InstVfsBinaryLog;

static void put32bits(unsigned char *p, unsigned int v){
  p[0] = v>>24;
  p[1] = v>>16;
  p[2] = v>>8;
  p[3] = v;
}

static void binarylog_xcall(
  void *p,
  int eEvent,
  sqlite3_int64 nClick,
  const char *zName,
  int flags,
  int nByte,
  sqlite3_int64 iOffset
){
  InstVfsBinaryLog *pLog = (InstVfsBinaryLog *)p;
  unsigned char *zRec;
  if( (20+pLog->nBuf)>BINARYLOG_BUFFERSIZE ){
    sqlite3_file *pFile = pLog->pOut;
    pFile->pMethods->xWrite(pFile, pLog->zBuf, pLog->nBuf, pLog->iOffset);
    pLog->iOffset += pLog->nBuf;
    pLog->nBuf = 0;
  }
  zRec = (unsigned char *)&pLog->zBuf[pLog->nBuf];
  put32bits(&zRec[0], eEvent);
  put32bits(&zRec[4], (int)nClick);
  put32bits(&zRec[8], flags);
  put32bits(&zRec[12], nByte);
  put32bits(&zRec[16], (int)iOffset);
  pLog->nBuf += 20;
}

static void binarylog_xdel(void *p){
  /* Close the log file and free the memory allocated for the 
  ** InstVfsBinaryLog structure.
  */
  InstVfsBinaryLog *pLog = (InstVfsBinaryLog *)p;
  sqlite3_file *pFile = pLog->pOut;
  if( pLog->nBuf ){
    pFile->pMethods->xWrite(pFile, pLog->zBuf, pLog->nBuf, pLog->iOffset);
  }
  pFile->pMethods->xClose(pFile);
  sqlite3_free(pLog->zBuf);
  sqlite3_free(pLog);
}

sqlite3_vfs *sqlite3_instvfs_binarylog(
  const char *zVfs,
  const char *zParentVfs, 
  const char *zLog
){
  InstVfsBinaryLog *p;
  sqlite3_vfs *pVfs;
  sqlite3_vfs *pParent;
  int nByte;
  int flags;
  int rc;

  pParent = sqlite3_vfs_find(zParentVfs);
  if( !pParent ){
    return 0;
  }

  nByte = sizeof(InstVfsBinaryLog) + pParent->szOsFile + pParent->mxPathname+1;

  p = (InstVfsBinaryLog *)sqlite3_malloc(nByte);
  memset(p, 0, nByte);
  p->zBuf = sqlite3_malloc(BINARYLOG_BUFFERSIZE);
  p->zOut = (char *)&p[1];
  p->pOut = (sqlite3_file *)&p->zOut[pParent->mxPathname+1];
  pParent->xFullPathname(pParent, zLog, pParent->mxPathname, p->zOut);

  flags = SQLITE_OPEN_READWRITE|SQLITE_OPEN_CREATE|SQLITE_OPEN_MASTER_JOURNAL;
  pParent->xDelete(pParent, p->zOut, 0);
  rc = pParent->xOpen(pParent, p->zOut, p->pOut, flags, &flags);
  if( rc==SQLITE_OK ){
    rc = p->pOut->pMethods->xWrite(p->pOut, "sqlite_ostrace1.....", 20, 0);
    p->iOffset = 20;
  }
  if( rc ){
    binarylog_xdel(p);
    return 0;
  }

  pVfs = sqlite3_instvfs_create(zVfs, zParentVfs);
  if( pVfs ){
    sqlite3_instvfs_configure(pVfs, binarylog_xcall, p, binarylog_xdel);
  }

  return pVfs;
}

/**************************************************************************
***************************************************************************
** Tcl interface starts here.
*/
#if SQLITE_TEST

#include <tcl.h>

struct InstVfsCall {
  Tcl_Interp *interp;
  Tcl_Obj *pScript;
};
typedef struct InstVfsCall InstVfsCall;

static void test_instvfs_xcall(
  void *p,
  int eEvent,
  sqlite3_int64 nClick,
  const char *zName,
  int flags,
  int nByte,
  sqlite3_int64 iOffset
){
  int rc;
  InstVfsCall *pCall = (InstVfsCall *)p;
  Tcl_Obj *pObj = Tcl_DuplicateObj( pCall->pScript);
  const char *zEvent = sqlite3_instvfs_name(eEvent);

  Tcl_IncrRefCount(pObj);
  Tcl_ListObjAppendElement(0, pObj, Tcl_NewStringObj(zEvent, -1));
  Tcl_ListObjAppendElement(0, pObj, Tcl_NewWideIntObj(nClick));
  Tcl_ListObjAppendElement(0, pObj, Tcl_NewStringObj(zName, -1));
  Tcl_ListObjAppendElement(0, pObj, Tcl_NewIntObj(nByte));
  Tcl_ListObjAppendElement(0, pObj, Tcl_NewWideIntObj(iOffset));

  rc = Tcl_EvalObjEx(pCall->interp, pObj, TCL_EVAL_GLOBAL|TCL_EVAL_DIRECT);
  if( rc ){
    Tcl_BackgroundError(pCall->interp);
  }
  Tcl_DecrRefCount(pObj);
}

static void test_instvfs_xdel(void *p){
  InstVfsCall *pCall = (InstVfsCall *)p;
  Tcl_DecrRefCount(pCall->pScript);
  sqlite3_free(pCall);
}

static int test_sqlite3_instvfs(
  void * clientData,
  Tcl_Interp *interp,
  int objc,
  Tcl_Obj *CONST objv[]
){
  static const char *IV_strs[] = 
               { "create",  "destroy",  "reset",  "report", "configure", "binarylog", 0 };
  enum IV_enum { IV_CREATE, IV_DESTROY, IV_RESET, IV_REPORT, IV_CONFIGURE, IV_BINARYLOG };
  int iSub;

  if( objc<2 ){
    Tcl_WrongNumArgs(interp, 1, objv, "SUB-COMMAND ...");
  }
  if( Tcl_GetIndexFromObj(interp, objv[1], IV_strs, "sub-command", 0, &iSub) ){
    return TCL_ERROR;
  }

  switch( (enum IV_enum)iSub ){
    case IV_CREATE: {
      char *zParent = 0;
      sqlite3_vfs *p;
      int isDefault = 0;
      if( objc>2 && 0==strcmp("-default", Tcl_GetString(objv[2])) ){
        isDefault = 1;
      }
      if( (objc-isDefault)!=4 && (objc-isDefault)!=3 ){
        Tcl_WrongNumArgs(interp, 2, objv, "?-default? NAME ?PARENT-VFS?");
        return TCL_ERROR;
      }
      if( objc==(4+isDefault) ){
        zParent = Tcl_GetString(objv[3+isDefault]);
      }
      p = sqlite3_instvfs_create(Tcl_GetString(objv[2+isDefault]), zParent);
      if( !p ){
        Tcl_AppendResult(interp, "error creating vfs ", 0);
        return TCL_ERROR;
      }
      if( isDefault ){
        sqlite3_vfs_register(p, 1);
      }
      Tcl_SetObjResult(interp, objv[2]);
      break;
    }
    case IV_BINARYLOG: {
      char *zName = 0;
      char *zLog = 0;
      sqlite3_vfs *p;
      int isDefault = 0;
      if( objc>2 && 0==strcmp("-default", Tcl_GetString(objv[2])) ){
        isDefault = 1;
      }
      if( (objc-isDefault)!=4 ){
        Tcl_WrongNumArgs(interp, 2, objv, "?-default? NAME LOGFILE");
        return TCL_ERROR;
      }
      zName = Tcl_GetString(objv[2+isDefault]);
      zLog = Tcl_GetString(objv[3+isDefault]);
      p = sqlite3_instvfs_binarylog(zName, 0, zLog);
      if( !p ){
        Tcl_AppendResult(interp, "error creating vfs ", 0);
        return TCL_ERROR;
      }
      if( isDefault ){
        sqlite3_vfs_register(p, 1);
      }
      Tcl_SetObjResult(interp, objv[2]);
      break;
    }

    case IV_CONFIGURE: {
      InstVfsCall *pCall;

      sqlite3_vfs *p;
      if( objc!=4 ){
        Tcl_WrongNumArgs(interp, 2, objv, "NAME SCRIPT");
        return TCL_ERROR;
      }
      p = sqlite3_vfs_find(Tcl_GetString(objv[2]));
      if( !p || p->xOpen!=instOpen ){
        Tcl_AppendResult(interp, "no such vfs: ", Tcl_GetString(objv[2]), 0);
        return TCL_ERROR;
      }

      if( strlen(Tcl_GetString(objv[3])) ){
        pCall = (InstVfsCall *)sqlite3_malloc(sizeof(InstVfsCall));
        pCall->interp = interp;
        pCall->pScript = Tcl_DuplicateObj(objv[3]);
        Tcl_IncrRefCount(pCall->pScript);
        sqlite3_instvfs_configure(p, 
            test_instvfs_xcall, (void *)pCall, test_instvfs_xdel
        );
      }else{
        sqlite3_instvfs_configure(p, 0, 0, 0);
      }
      break;
    }

    case IV_REPORT:
    case IV_DESTROY:
    case IV_RESET: {
      sqlite3_vfs *p;
      if( objc!=3 ){
        Tcl_WrongNumArgs(interp, 2, objv, "NAME");
        return TCL_ERROR;
      }
      p = sqlite3_vfs_find(Tcl_GetString(objv[2]));
      if( !p || p->xOpen!=instOpen ){
        Tcl_AppendResult(interp, "no such vfs: ", Tcl_GetString(objv[2]), 0);
        return TCL_ERROR;
      }

      if( ((enum IV_enum)iSub)==IV_DESTROY ){
        sqlite3_instvfs_destroy(p);
      }
      if( ((enum IV_enum)iSub)==IV_RESET ){
        sqlite3_instvfs_reset(p);
      }
      if( ((enum IV_enum)iSub)==IV_REPORT ){
        int ii;
        Tcl_Obj *pRet = Tcl_NewObj();

        const char *zName = (char *)1;
        sqlite3_int64 nClick;
        int nCall;
        for(ii=1; zName; ii++){
          sqlite3_instvfs_get(p, ii, &zName, &nClick, &nCall);
          if( zName ){
            Tcl_Obj *pElem = Tcl_NewObj();
            Tcl_ListObjAppendElement(0, pElem, Tcl_NewStringObj(zName, -1));
            Tcl_ListObjAppendElement(0, pElem, Tcl_NewIntObj(nCall));
            Tcl_ListObjAppendElement(0, pElem, Tcl_NewWideIntObj(nClick));
            Tcl_ListObjAppendElement(0, pRet, pElem);
          }
        }

        Tcl_SetObjResult(interp, pRet);
      }

      break;
    }
  }

  return TCL_OK;
}

int SqlitetestOsinst_Init(Tcl_Interp *interp){
  Tcl_CreateObjCommand(interp, "sqlite3_instvfs", test_sqlite3_instvfs, 0, 0);
  return TCL_OK;
}

#endif

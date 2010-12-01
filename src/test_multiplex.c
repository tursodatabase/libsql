/*
** 2010 October 28
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
*************************************************************************
**
** This file contains a VFS "shim" - a layer that sits in between the
** pager and the real VFS.
**
** This particular shim enforces a multiplex system on DB files.  
** This shim shards/partitions a single DB file into smaller 
** "chunks" such that the total DB file size may exceed the maximum
** file size of the underlying file system.
**
*/
#include "sqlite3.h"
#include <string.h>
#include <assert.h>
#include "sqliteInt.h"

/************************ Shim Definitions ******************************/

/* This is the limit on the chunk size.  It may be changed by calling
** the sqlite3_multiplex_set() interface.
*/
#define SQLITE_MULTIPLEX_CHUNK_SIZE 0x40000000
/* Default limit on number of chunks.  Care should be taken
** so that values for chunks numbers fit in the SQLITE_MULTIPLEX_EXT_FMT
** format specifier. It may be changed by calling
** the sqlite3_multiplex_set() interface.
*/
#define SQLITE_MULTIPLEX_MAX_CHUNKS 32

/* If SQLITE_MULTIPLEX_EXT_OVWR is defined, the 
** last SQLITE_MULTIPLEX_EXT_SZ characters of the 
** filename will be overwritten, otherwise, the 
** multiplex extension is simply appended to the filename.
** Ex.  (undefined) test.db -> test.db01
**      (defined)   test.db -> test.01
** Chunk 0 does not have a modified extension.
*/
#define SQLITE_MULTIPLEX_EXT_FMT    "%02d"
#define SQLITE_MULTIPLEX_EXT_SZ     2

/************************ Object Definitions ******************************/

/* Forward declaration of all object types */
typedef struct multiplexGroup multiplexGroup;
typedef struct multiplexConn multiplexConn;

/*
** A "multiplex group" is a collection of files that collectively
** makeup a single SQLite DB file.  This allows the size of the DB
** to exceed the limits imposed by the file system.
**
** There is an instance of the following object for each defined multiplex
** group.
*/
struct multiplexGroup {
  sqlite3_file **pReal;            /* Handles to each chunk */
  char *bOpen;                     /* 0 if chunk not opened */
  char *zName;                     /* Base filename of this group */
  int nName;                       /* Length of base filename */
  int flags;                       /* Flags used for original opening */
  multiplexGroup *pNext, *pPrev;   /* Doubly linked list of all group objects */
};

/*
** An instance of the following object represents each open connection
** to a file that is multiplex'ed.  This object is a 
** subclass of sqlite3_file.  The sqlite3_file object for the underlying
** VFS is appended to this structure.
*/
struct multiplexConn {
  sqlite3_file base;              /* Base class - must be first */
  multiplexGroup *pGroup;         /* The underlying group of files */
};

/************************* Global Variables **********************************/
/*
** All global variables used by this file are containing within the following
** gMultiplex structure.
*/
static struct {
  /* The pOrigVfs is the real, original underlying VFS implementation.
  ** Most operations pass-through to the real VFS.  This value is read-only
  ** during operation.  It is only modified at start-time and thus does not
  ** require a mutex.
  */
  sqlite3_vfs *pOrigVfs;

  /* The sThisVfs is the VFS structure used by this shim.  It is initialized
  ** at start-time and thus does not require a mutex
  */
  sqlite3_vfs sThisVfs;

  /* The sIoMethods defines the methods used by sqlite3_file objects 
  ** associated with this shim.  It is initialized at start-time and does
  ** not require a mutex.
  **
  ** When the underlying VFS is called to open a file, it might return 
  ** either a version 1 or a version 2 sqlite3_file object.  This shim
  ** has to create a wrapper sqlite3_file of the same version.  Hence
  ** there are two I/O method structures, one for version 1 and the other
  ** for version 2.
  */
  sqlite3_io_methods sIoMethodsV1;
  sqlite3_io_methods sIoMethodsV2;

  /* True when this shim has been initialized.
  */
  int isInitialized;

  /* For run-time access any of the other global data structures in this
  ** shim, the following mutex must be held.
  */
  sqlite3_mutex *pMutex;

  /* List of multiplexGroup objects.
  */
  multiplexGroup *pGroups;

  /* Chunk params.
  */
  int nChunkSize;
  int nMaxChunks;

  /* Storage for temp file names.  Allocated during 
  ** initialization to the max pathname of the underlying VFS.
  */
  char *zName;

} gMultiplex;

/************************* Utility Routines *********************************/
/*
** Acquire and release the mutex used to serialize access to the
** list of multiplexGroups.
*/
static void multiplexEnter(void){ sqlite3_mutex_enter(gMultiplex.pMutex); }
static void multiplexLeave(void){ sqlite3_mutex_leave(gMultiplex.pMutex); }

/* Translate an sqlite3_file* that is really a multiplexGroup* into
** the sqlite3_file* for the underlying original VFS.
*/
static sqlite3_file *multiplexSubOpen(multiplexConn *pConn, int iChunk, int *rc, int *pOutFlags){
  multiplexGroup *pGroup = pConn->pGroup;
  sqlite3_vfs *pOrigVfs = gMultiplex.pOrigVfs;        /* Real VFS */
  if( iChunk<gMultiplex.nMaxChunks ){
    sqlite3_file *pSubOpen = pGroup->pReal[iChunk];    /* Real file descriptor */
    if( !pGroup->bOpen[iChunk] ){
      memcpy(gMultiplex.zName, pGroup->zName, pGroup->nName+1);
      if( iChunk ){
#ifdef SQLITE_MULTIPLEX_EXT_OVWR
        sqlite3_snprintf(SQLITE_MULTIPLEX_EXT_SZ+1, gMultiplex.zName+pGroup->nName-SQLITE_MULTIPLEX_EXT_SZ, SQLITE_MULTIPLEX_EXT_FMT, iChunk);
#else
        sqlite3_snprintf(SQLITE_MULTIPLEX_EXT_SZ+1, gMultiplex.zName+pGroup->nName, SQLITE_MULTIPLEX_EXT_FMT, iChunk);
#endif
      }
      *rc = pOrigVfs->xOpen(pOrigVfs, gMultiplex.zName, pSubOpen, pGroup->flags, pOutFlags);
      if( *rc==SQLITE_OK ){
        pGroup->bOpen[iChunk] = -1;
        return pSubOpen;
      }
      return NULL;
    }
    *rc = SQLITE_OK;
    return pSubOpen;
  }
  *rc = SQLITE_FULL;
  return NULL;
}

/************************* VFS Method Wrappers *****************************/

/*
** This is the xOpen method used for the "multiplex" VFS.
**
** Most of the work is done by the underlying original VFS.  This method
** simply links the new file into the appropriate multiplex group if it is a
** file that needs to be tracked.
*/
static int multiplexOpen(
  sqlite3_vfs *pVfs,         /* The multiplex VFS */
  const char *zName,         /* Name of file to be opened */
  sqlite3_file *pConn,       /* Fill in this file descriptor */
  int flags,                 /* Flags to control the opening */
  int *pOutFlags             /* Flags showing results of opening */
){
  int rc;                                        /* Result code */
  multiplexConn *pMultiplexOpen;                 /* The new multiplex file descriptor */
  multiplexGroup *pGroup;                        /* Corresponding multiplexGroup object */
  sqlite3_file *pSubOpen;                        /* Real file descriptor */
  sqlite3_vfs *pOrigVfs = gMultiplex.pOrigVfs;   /* Real VFS */
  int nName = sqlite3Strlen30(zName);
  int i;
  int sz;

  UNUSED_PARAMETER(pVfs);

  /* We need to create a group structure and manage
  ** access to this group of files.
  */
  multiplexEnter();
  pMultiplexOpen = (multiplexConn*)pConn;
  /* allocate space for group */
  sz = sizeof(multiplexGroup)                         /* multiplexGroup */
     + (sizeof(sqlite3_file *)*gMultiplex.nMaxChunks) /* pReal[] */
     + (pOrigVfs->szOsFile*gMultiplex.nMaxChunks)     /* *pReal */
     + gMultiplex.nMaxChunks                          /* bOpen[] */
     + nName + 1;                                     /* zName */
#ifndef SQLITE_MULTIPLEX_EXT_OVWR
  sz += SQLITE_MULTIPLEX_EXT_SZ;
  assert(nName+SQLITE_MULTIPLEX_EXT_SZ < pOrigVfs->mxPathname);
#else
  assert(nName >= SQLITE_MULTIPLEX_EXT_SZ);
  assert(nName < pOrigVfs->mxPathname);
#endif
  pGroup = sqlite3_malloc( sz );
  if( pGroup==0 ){
    rc=SQLITE_NOMEM;
  }else{
    /* assign pointers to extra space allocated */
    char *p = (char *)&pGroup[1];
    pMultiplexOpen->pGroup = pGroup;
    memset(pGroup, 0, sz);
    pGroup->pReal = (sqlite3_file **)p;
    p += (sizeof(sqlite3_file *)*gMultiplex.nMaxChunks);
    for(i=0; i<gMultiplex.nMaxChunks; i++){
      pGroup->pReal[i] = (sqlite3_file *)p;
      p += pOrigVfs->szOsFile;
    }
    pGroup->bOpen = p;
    p += gMultiplex.nMaxChunks;
    pGroup->zName = p;
    /* save off base filename, name length, and original open flags  */
    memcpy(pGroup->zName, zName, nName+1);
    pGroup->nName = nName;
    pGroup->flags = flags;
    pSubOpen = multiplexSubOpen(pMultiplexOpen, 0, &rc, pOutFlags);
    if( pSubOpen ){
      if( pSubOpen->pMethods->iVersion==1 ){
        pMultiplexOpen->base.pMethods = &gMultiplex.sIoMethodsV1;
      }else{
        pMultiplexOpen->base.pMethods = &gMultiplex.sIoMethodsV2;
      }
      /* place this group at the head of our list */
      pGroup->pNext = gMultiplex.pGroups;
      if( gMultiplex.pGroups ) gMultiplex.pGroups->pPrev = pGroup;
      gMultiplex.pGroups = pGroup;
    }else{
      sqlite3_free(pGroup);
    }
  }
  multiplexLeave();
  return rc;
}

/*
** This is the xDelete method used for the "multiplex" VFS.
** It attempts to delete the filename specified, as well
** as additional files with the SQLITE_MULTIPLEX_EXT_FMT extension.
*/
static int multiplexDelete(
  sqlite3_vfs *pVfs,         /* The multiplex VFS */
  const char *zName,         /* Name of file to delete */
  int syncDir
){
  sqlite3_vfs *pOrigVfs = gMultiplex.pOrigVfs;   /* Real VFS */
  int rc = SQLITE_OK;
  int nName = sqlite3Strlen30(zName);
  int i;

  UNUSED_PARAMETER(pVfs);

  multiplexEnter();
  memcpy(gMultiplex.zName, zName, nName+1);
  for(i=0; i<gMultiplex.nMaxChunks; i++){
    int rc2;
    int exists = 0;
    if( i ){
#ifdef SQLITE_MULTIPLEX_EXT_OVWR
        sqlite3_snprintf(SQLITE_MULTIPLEX_EXT_SZ+1, gMultiplex.zName+nName-SQLITE_MULTIPLEX_EXT_SZ, SQLITE_MULTIPLEX_EXT_FMT, i);
#else
        sqlite3_snprintf(SQLITE_MULTIPLEX_EXT_SZ+1, gMultiplex.zName+nName, SQLITE_MULTIPLEX_EXT_FMT, i);
#endif
    }
    rc2 = pOrigVfs->xAccess(pOrigVfs, gMultiplex.zName, SQLITE_ACCESS_EXISTS, &exists);
    if( rc2==SQLITE_OK && exists){
      /* if it exists, delete it */
      rc2 = pOrigVfs->xDelete(pOrigVfs, gMultiplex.zName, syncDir);
      if( rc2!=SQLITE_OK ) rc = rc2;
    }else{
      /* stop at first "gap" */
      break;
    }
  }
  multiplexLeave();
  return rc;
}

static int multiplexAccess(sqlite3_vfs *a, const char *b, int c, int *d){
  return gMultiplex.pOrigVfs->xAccess(gMultiplex.pOrigVfs, b, c, d);
}
static int multiplexFullPathname(sqlite3_vfs *a, const char *b, int c, char *d){
  return gMultiplex.pOrigVfs->xFullPathname(gMultiplex.pOrigVfs, b, c, d);
}
static void *multiplexDlOpen(sqlite3_vfs *a, const char *b){
  return gMultiplex.pOrigVfs->xDlOpen(gMultiplex.pOrigVfs, b);
}
static void multiplexDlError(sqlite3_vfs *a, int b, char *c){
  gMultiplex.pOrigVfs->xDlError(gMultiplex.pOrigVfs, b, c);
}
static void (*multiplexDlSym(sqlite3_vfs *a, void *b, const char *c))(void){
  return gMultiplex.pOrigVfs->xDlSym(gMultiplex.pOrigVfs, b, c);
}
static void multiplexDlClose(sqlite3_vfs *a, void *b){
  gMultiplex.pOrigVfs->xDlClose(gMultiplex.pOrigVfs, b);
}
static int multiplexRandomness(sqlite3_vfs *a, int b, char *c){
  return gMultiplex.pOrigVfs->xRandomness(gMultiplex.pOrigVfs, b, c);
}
static int multiplexSleep(sqlite3_vfs *a, int b){
  return gMultiplex.pOrigVfs->xSleep(gMultiplex.pOrigVfs, b);
}
static int multiplexCurrentTime(sqlite3_vfs *a, double *b){
  return gMultiplex.pOrigVfs->xCurrentTime(gMultiplex.pOrigVfs, b);
}
static int multiplexGetLastError(sqlite3_vfs *a, int b, char *c){
  return gMultiplex.pOrigVfs->xGetLastError(gMultiplex.pOrigVfs, b, c);
}
static int multiplexCurrentTimeInt64(sqlite3_vfs *a, sqlite3_int64 *b){
  return gMultiplex.pOrigVfs->xCurrentTimeInt64(gMultiplex.pOrigVfs, b);
}

/************************ I/O Method Wrappers *******************************/

/* xClose requests get passed through to the original VFS.
** We loop over all open chunk handles and close them.
** The group structure for this file is unlinked from 
** our list of groups and freed.
*/
static int multiplexClose(sqlite3_file *pConn){
  multiplexConn *p = (multiplexConn*)pConn;
  multiplexGroup *pGroup = p->pGroup;
  int rc = SQLITE_OK;
  int i;
  multiplexEnter();
  /* close any open handles */
  for(i=0; i<gMultiplex.nMaxChunks; i++){
    if( pGroup->bOpen[i] ){
      sqlite3_file *pSubOpen = pGroup->pReal[i];
      int rc2 = pSubOpen->pMethods->xClose(pSubOpen);
      if( rc2!=SQLITE_OK ) rc = rc2;
      pGroup->bOpen[i] = 0;
    }
  }
  /* remove from linked list */
  if( pGroup->pNext ) pGroup->pNext->pPrev = pGroup->pPrev;
  if( pGroup->pPrev ){
    pGroup->pPrev->pNext = pGroup->pNext;
  }else{
    gMultiplex.pGroups = pGroup->pNext;
  }
  sqlite3_free(pGroup);
  multiplexLeave();
  return rc;
}

/* Pass xRead requests thru to the original VFS after
** determining the correct chunk to operate on.
** Break up reads across chunk boundaries.
*/
static int multiplexRead(
  sqlite3_file *pConn,
  void *pBuf,
  int iAmt,
  sqlite3_int64 iOfst
){
  multiplexConn *p = (multiplexConn*)pConn;
  int rc = SQLITE_OK;
  multiplexEnter();
  while( iAmt > 0 ){
    int i = (int)(iOfst/gMultiplex.nChunkSize);
    sqlite3_file *pSubOpen = multiplexSubOpen(p, i, &rc, NULL);
    if( pSubOpen ){
      int extra = ((int)(iOfst % gMultiplex.nChunkSize) + iAmt) - gMultiplex.nChunkSize;
      if( extra<0 ) extra = 0;
      iAmt -= extra;
      rc = pSubOpen->pMethods->xRead(pSubOpen, pBuf, iAmt, iOfst%gMultiplex.nChunkSize);
      if( rc!=SQLITE_OK ) break;
      pBuf = (char *)pBuf + iAmt;
      iOfst += iAmt;
      iAmt = extra;
    }else{
      rc = SQLITE_IOERR_READ;
      break;
    }
  }
  multiplexLeave();
  return rc;
}

/* Pass xWrite requests thru to the original VFS after
** determining the correct chunk to operate on.
** Break up writes across chunk boundaries.
*/
static int multiplexWrite(
  sqlite3_file *pConn,
  const void *pBuf,
  int iAmt,
  sqlite3_int64 iOfst
){
  multiplexConn *p = (multiplexConn*)pConn;
  int rc = SQLITE_OK;
  multiplexEnter();
  while( iAmt > 0 ){
    int i = (int)(iOfst/gMultiplex.nChunkSize);
    sqlite3_file *pSubOpen = multiplexSubOpen(p, i, &rc, NULL);
    if( pSubOpen ){
      int extra = ((int)(iOfst % gMultiplex.nChunkSize) + iAmt) - gMultiplex.nChunkSize;
      if( extra<0 ) extra = 0;
      iAmt -= extra;
      rc = pSubOpen->pMethods->xWrite(pSubOpen, pBuf, iAmt, iOfst%gMultiplex.nChunkSize);
      if( rc!=SQLITE_OK ) break;
      pBuf = (char *)pBuf + iAmt;
      iOfst += iAmt;
      iAmt = extra;
    }else{
      rc = SQLITE_IOERR_WRITE;
      break;
    }
  }
  multiplexLeave();
  return rc;
}

/* Pass xTruncate requests thru to the original VFS after
** determining the correct chunk to operate on.  Delete any
** chunks above the truncate mark.
*/
static int multiplexTruncate(sqlite3_file *pConn, sqlite3_int64 size){
  multiplexConn *p = (multiplexConn*)pConn;
  multiplexGroup *pGroup = p->pGroup;
  int rc = SQLITE_OK;
  int rc2;
  int i;
  sqlite3_file *pSubOpen;
  sqlite3_vfs *pOrigVfs = gMultiplex.pOrigVfs;   /* Real VFS */
  multiplexEnter();
  memcpy(gMultiplex.zName, pGroup->zName, pGroup->nName+1);
  /* delete the chunks above the truncate limit */
  for(i=(int)(size/gMultiplex.nChunkSize)+1; i<gMultiplex.nMaxChunks; i++){
    /* close any open chunks before deleting them */
    if( pGroup->bOpen[i] ){
      pSubOpen = pGroup->pReal[i];
      rc2 = pSubOpen->pMethods->xClose(pSubOpen);
      if( rc2!=SQLITE_OK ) rc = SQLITE_IOERR_TRUNCATE;
      pGroup->bOpen[i] = 0;
    }
#ifdef SQLITE_MULTIPLEX_EXT_OVWR
    sqlite3_snprintf(SQLITE_MULTIPLEX_EXT_SZ+1, gMultiplex.zName+pGroup->nName-SQLITE_MULTIPLEX_EXT_SZ, SQLITE_MULTIPLEX_EXT_FMT, i);
#else
    sqlite3_snprintf(SQLITE_MULTIPLEX_EXT_SZ+1, gMultiplex.zName+pGroup->nName, SQLITE_MULTIPLEX_EXT_FMT, i);
#endif
    rc2 = pOrigVfs->xDelete(pOrigVfs, gMultiplex.zName, 0);
    if( rc2!=SQLITE_OK ) rc = SQLITE_IOERR_TRUNCATE;
  }
  pSubOpen = multiplexSubOpen(p, (int)(size/gMultiplex.nChunkSize), &rc2, NULL);
  if( pSubOpen ){
    rc2 = pSubOpen->pMethods->xTruncate(pSubOpen, size%gMultiplex.nChunkSize);
    if( rc2!=SQLITE_OK ) rc = rc2;
  }else{
    rc = SQLITE_IOERR_TRUNCATE;
  }
  multiplexLeave();
  return rc;
}

/* Pass xSync requests through to the original VFS without change
*/
static int multiplexSync(sqlite3_file *pConn, int flags){
  multiplexConn *p = (multiplexConn*)pConn;
  multiplexGroup *pGroup = p->pGroup;
  int rc = SQLITE_OK;
  int i;
  multiplexEnter();
  for(i=0; i<gMultiplex.nMaxChunks; i++){
    /* if we don't have it open, we don't need to sync it */
    if( pGroup->bOpen[i] ){
      sqlite3_file *pSubOpen = pGroup->pReal[i];
      int rc2 = pSubOpen->pMethods->xSync(pSubOpen, flags);
      if( rc2!=SQLITE_OK ) rc = rc2;
    }
  }
  multiplexLeave();
  return rc;
}

/* Pass xFileSize requests through to the original VFS.
** Aggregate the size of all the chunks before returning.
*/
static int multiplexFileSize(sqlite3_file *pConn, sqlite3_int64 *pSize){
  multiplexConn *p = (multiplexConn*)pConn;
  multiplexGroup *pGroup = p->pGroup;
  int rc = SQLITE_OK;
  int rc2;
  int i;
  multiplexEnter();
  *pSize = 0;
  for(i=0; i<gMultiplex.nMaxChunks; i++){
    sqlite3_file *pSubOpen = NULL;
    /* if not opened already, check to see if the chunk exists */
    if( pGroup->bOpen[i] ){
      pSubOpen = pGroup->pReal[i];
    }else{
      sqlite3_vfs *pOrigVfs = gMultiplex.pOrigVfs;   /* Real VFS */
      int exists = 0;
      memcpy(gMultiplex.zName, pGroup->zName, pGroup->nName+1);
      if( i ){
#ifdef SQLITE_MULTIPLEX_EXT_OVWR
        sqlite3_snprintf(SQLITE_MULTIPLEX_EXT_SZ+1, gMultiplex.zName+pGroup->nName-SQLITE_MULTIPLEX_EXT_SZ, SQLITE_MULTIPLEX_EXT_FMT, i);
#else
        sqlite3_snprintf(SQLITE_MULTIPLEX_EXT_SZ+1, gMultiplex.zName+pGroup->nName, SQLITE_MULTIPLEX_EXT_FMT, i);
#endif
      }
      rc2 = pOrigVfs->xAccess(pOrigVfs, gMultiplex.zName, SQLITE_ACCESS_EXISTS, &exists);
      if( rc2==SQLITE_OK && exists){
        /* if it exists, open it */
        pSubOpen = multiplexSubOpen(p, i, &rc, NULL);
      }else{
        /* stop at first "gap" */
        break;
      }
    }
    if( pSubOpen ){
      sqlite3_int64 sz;
      rc2 = pSubOpen->pMethods->xFileSize(pSubOpen, &sz);
      if( rc2!=SQLITE_OK ){
        rc = rc2;
      }else{
        if( sz>gMultiplex.nChunkSize ){
          rc = SQLITE_IOERR_FSTAT;
        }
        *pSize += sz;
      }
    }else{
      break;
    }
  }
  multiplexLeave();
  return rc;
}

/* Pass xLock requests through to the original VFS unchanged.
*/
static int multiplexLock(sqlite3_file *pConn, int lock){
  multiplexConn *p = (multiplexConn*)pConn;
  int rc;
  sqlite3_file *pSubOpen = multiplexSubOpen(p, 0, &rc, NULL);
  if( pSubOpen ){
    return pSubOpen->pMethods->xLock(pSubOpen, lock);
  }
  return SQLITE_BUSY;
}

/* Pass xUnlock requests through to the original VFS unchanged.
*/
static int multiplexUnlock(sqlite3_file *pConn, int lock){
  multiplexConn *p = (multiplexConn*)pConn;
  int rc;
  sqlite3_file *pSubOpen = multiplexSubOpen(p, 0, &rc, NULL);
  if( pSubOpen ){
    return pSubOpen->pMethods->xUnlock(pSubOpen, lock);
  }
  return SQLITE_IOERR_UNLOCK;
}

/* Pass xCheckReservedLock requests through to the original VFS unchanged.
*/
static int multiplexCheckReservedLock(sqlite3_file *pConn, int *pResOut){
  multiplexConn *p = (multiplexConn*)pConn;
  int rc;
  sqlite3_file *pSubOpen = multiplexSubOpen(p, 0, &rc, NULL);
  if( pSubOpen ){
    return pSubOpen->pMethods->xCheckReservedLock(pSubOpen, pResOut);
  }
  return SQLITE_IOERR_CHECKRESERVEDLOCK;
}

/* Pass xFileControl requests through to the original VFS unchanged.
*/
static int multiplexFileControl(sqlite3_file *pConn, int op, void *pArg){
  multiplexConn *p = (multiplexConn*)pConn;
  int rc;
  sqlite3_file *pSubOpen;
  if ( op==SQLITE_FCNTL_SIZE_HINT || op==SQLITE_FCNTL_CHUNK_SIZE ) return SQLITE_OK;
  pSubOpen = multiplexSubOpen(p, 0, &rc, NULL);
  if( pSubOpen ){
    return pSubOpen->pMethods->xFileControl(pSubOpen, op, pArg);
  }
  return SQLITE_ERROR;
}

/* Pass xSectorSize requests through to the original VFS unchanged.
*/
static int multiplexSectorSize(sqlite3_file *pConn){
  multiplexConn *p = (multiplexConn*)pConn;
  int rc;
  sqlite3_file *pSubOpen = multiplexSubOpen(p, 0, &rc, NULL);
  if( pSubOpen ){
    return pSubOpen->pMethods->xSectorSize(pSubOpen);
  }
  return SQLITE_DEFAULT_SECTOR_SIZE;
}

/* Pass xDeviceCharacteristics requests through to the original VFS unchanged.
*/
static int multiplexDeviceCharacteristics(sqlite3_file *pConn){
  multiplexConn *p = (multiplexConn*)pConn;
  int rc;
  sqlite3_file *pSubOpen = multiplexSubOpen(p, 0, &rc, NULL);
  if( pSubOpen ){
    return pSubOpen->pMethods->xDeviceCharacteristics(pSubOpen);
  }
  return 0;
}

/* Pass xShmMap requests through to the original VFS unchanged.
*/
static int multiplexShmMap(
  sqlite3_file *pConn,            /* Handle open on database file */
  int iRegion,                    /* Region to retrieve */
  int szRegion,                   /* Size of regions */
  int bExtend,                    /* True to extend file if necessary */
  void volatile **pp              /* OUT: Mapped memory */
){
  multiplexConn *p = (multiplexConn*)pConn;
  int rc;
  sqlite3_file *pSubOpen = multiplexSubOpen(p, 0, &rc, NULL);
  if( pSubOpen ){
    return pSubOpen->pMethods->xShmMap(pSubOpen, iRegion, szRegion, bExtend, pp);
  }
  return SQLITE_IOERR;
}

/* Pass xShmLock requests through to the original VFS unchanged.
*/
static int multiplexShmLock(
  sqlite3_file *pConn,       /* Database file holding the shared memory */
  int ofst,                  /* First lock to acquire or release */
  int n,                     /* Number of locks to acquire or release */
  int flags                  /* What to do with the lock */
){
  multiplexConn *p = (multiplexConn*)pConn;
  int rc;
  sqlite3_file *pSubOpen = multiplexSubOpen(p, 0, &rc, NULL);
  if( pSubOpen ){
    return pSubOpen->pMethods->xShmLock(pSubOpen, ofst, n, flags);
  }
  return SQLITE_BUSY;
}

/* Pass xShmBarrier requests through to the original VFS unchanged.
*/
static void multiplexShmBarrier(sqlite3_file *pConn){
  multiplexConn *p = (multiplexConn*)pConn;
  int rc;
  sqlite3_file *pSubOpen = multiplexSubOpen(p, 0, &rc, NULL);
  if( pSubOpen ){
    pSubOpen->pMethods->xShmBarrier(pSubOpen);
  }
}

/* Pass xShmUnmap requests through to the original VFS unchanged.
*/
static int multiplexShmUnmap(sqlite3_file *pConn, int deleteFlag){
  multiplexConn *p = (multiplexConn*)pConn;
  int rc;
  sqlite3_file *pSubOpen = multiplexSubOpen(p, 0, &rc, NULL);
  if( pSubOpen ){
    return pSubOpen->pMethods->xShmUnmap(pSubOpen, deleteFlag);
  }
  return SQLITE_OK;
}

/************************** Public Interfaces *****************************/
/*
** Initialize the multiplex VFS shim.  Use the VFS named zOrigVfsName
** as the VFS that does the actual work.  Use the default if
** zOrigVfsName==NULL.  
**
** The multiplex VFS shim is named "multiplex".  It will become the default
** VFS if makeDefault is non-zero.
**
** THIS ROUTINE IS NOT THREADSAFE.  Call this routine exactly once
** during start-up.
*/
int sqlite3_multiplex_initialize(const char *zOrigVfsName, int makeDefault){
  sqlite3_vfs *pOrigVfs;
  if( gMultiplex.isInitialized ) return SQLITE_MISUSE;
  pOrigVfs = sqlite3_vfs_find(zOrigVfsName);
  if( pOrigVfs==0 ) return SQLITE_ERROR;
  assert( pOrigVfs!=&gMultiplex.sThisVfs );
  gMultiplex.pMutex = sqlite3_mutex_alloc(SQLITE_MUTEX_FAST);
  if( !gMultiplex.pMutex ){
    return SQLITE_NOMEM;
  }
  gMultiplex.zName = sqlite3_malloc(pOrigVfs->mxPathname);
  if( !gMultiplex.zName ){
    sqlite3_mutex_free(gMultiplex.pMutex);
    return SQLITE_NOMEM;
  }
  gMultiplex.nChunkSize = SQLITE_MULTIPLEX_CHUNK_SIZE;
  gMultiplex.nMaxChunks = SQLITE_MULTIPLEX_MAX_CHUNKS;
  gMultiplex.pGroups = NULL;
  gMultiplex.isInitialized = 1;
  gMultiplex.pOrigVfs = pOrigVfs;
  gMultiplex.sThisVfs = *pOrigVfs;
  gMultiplex.sThisVfs.szOsFile += sizeof(multiplexConn);
  gMultiplex.sThisVfs.zName = "multiplex";
  gMultiplex.sThisVfs.xOpen = multiplexOpen;
  gMultiplex.sThisVfs.xDelete = multiplexDelete;
  gMultiplex.sThisVfs.xAccess = multiplexAccess;
  gMultiplex.sThisVfs.xFullPathname = multiplexFullPathname;
  gMultiplex.sThisVfs.xDlOpen = multiplexDlOpen;
  gMultiplex.sThisVfs.xDlError = multiplexDlError;
  gMultiplex.sThisVfs.xDlSym = multiplexDlSym;
  gMultiplex.sThisVfs.xDlClose = multiplexDlClose;
  gMultiplex.sThisVfs.xRandomness = multiplexRandomness;
  gMultiplex.sThisVfs.xSleep = multiplexSleep;
  gMultiplex.sThisVfs.xCurrentTime = multiplexCurrentTime;
  gMultiplex.sThisVfs.xGetLastError = multiplexGetLastError;
  gMultiplex.sThisVfs.xCurrentTimeInt64 = multiplexCurrentTimeInt64;

  gMultiplex.sIoMethodsV1.iVersion = 1;
  gMultiplex.sIoMethodsV1.xClose = multiplexClose;
  gMultiplex.sIoMethodsV1.xRead = multiplexRead;
  gMultiplex.sIoMethodsV1.xWrite = multiplexWrite;
  gMultiplex.sIoMethodsV1.xTruncate = multiplexTruncate;
  gMultiplex.sIoMethodsV1.xSync = multiplexSync;
  gMultiplex.sIoMethodsV1.xFileSize = multiplexFileSize;
  gMultiplex.sIoMethodsV1.xLock = multiplexLock;
  gMultiplex.sIoMethodsV1.xUnlock = multiplexUnlock;
  gMultiplex.sIoMethodsV1.xCheckReservedLock = multiplexCheckReservedLock;
  gMultiplex.sIoMethodsV1.xFileControl = multiplexFileControl;
  gMultiplex.sIoMethodsV1.xSectorSize = multiplexSectorSize;
  gMultiplex.sIoMethodsV1.xDeviceCharacteristics = multiplexDeviceCharacteristics;
  gMultiplex.sIoMethodsV2 = gMultiplex.sIoMethodsV1;
  gMultiplex.sIoMethodsV2.iVersion = 2;
  gMultiplex.sIoMethodsV2.xShmMap = multiplexShmMap;
  gMultiplex.sIoMethodsV2.xShmLock = multiplexShmLock;
  gMultiplex.sIoMethodsV2.xShmBarrier = multiplexShmBarrier;
  gMultiplex.sIoMethodsV2.xShmUnmap = multiplexShmUnmap;
  sqlite3_vfs_register(&gMultiplex.sThisVfs, makeDefault);
  return SQLITE_OK;
}

/*
** Shutdown the multiplex system.
**
** All SQLite database connections must be closed before calling this
** routine.
**
** THIS ROUTINE IS NOT THREADSAFE.  Call this routine exactly once while
** shutting down in order to free all remaining multiplex groups.
*/
int sqlite3_multiplex_shutdown(void){
  if( gMultiplex.isInitialized==0 ) return SQLITE_MISUSE;
  if( gMultiplex.pGroups ) return SQLITE_MISUSE;
  gMultiplex.isInitialized = 0;
  sqlite3_free(gMultiplex.zName);
  sqlite3_mutex_free(gMultiplex.pMutex);
  sqlite3_vfs_unregister(&gMultiplex.sThisVfs);
  memset(&gMultiplex, 0, sizeof(gMultiplex));
  return SQLITE_OK;
}

/*
** Adjust chunking params.  VFS should be initialized first.
** No files should be open.  Re-intializing will reset these
** to the default.
*/
int sqlite3_multiplex_set(
  int nChunkSize,                 /* Max chunk size */
  int nMaxChunks                  /* Max number of chunks */
){
  if( !gMultiplex.isInitialized ) return SQLITE_MISUSE;
  if( gMultiplex.pGroups ) return SQLITE_MISUSE;
  if( nChunkSize<32 ) return SQLITE_MISUSE;
  if( nMaxChunks<1 ) return SQLITE_MISUSE;
  if( nMaxChunks>99 ) return SQLITE_MISUSE;
  multiplexEnter();
  gMultiplex.nChunkSize = nChunkSize;
  gMultiplex.nMaxChunks = nMaxChunks;
  multiplexLeave();
  return SQLITE_OK;
}

/***************************** Test Code ***********************************/
#ifdef SQLITE_TEST
#include <tcl.h>

extern const char *sqlite3TestErrorName(int);


/*
** tclcmd: sqlite3_multiplex_initialize NAME MAKEDEFAULT
*/
static int test_multiplex_initialize(
  void * clientData,
  Tcl_Interp *interp,
  int objc,
  Tcl_Obj *CONST objv[]
){
  const char *zName;              /* Name of new multiplex VFS */
  int makeDefault;                /* True to make the new VFS the default */
  int rc;                         /* Value returned by multiplex_initialize() */

  UNUSED_PARAMETER(clientData);

  /* Process arguments */
  if( objc!=3 ){
    Tcl_WrongNumArgs(interp, 1, objv, "NAME MAKEDEFAULT");
    return TCL_ERROR;
  }
  zName = Tcl_GetString(objv[1]);
  if( Tcl_GetBooleanFromObj(interp, objv[2], &makeDefault) ) return TCL_ERROR;
  if( zName[0]=='\0' ) zName = 0;

  /* Call sqlite3_multiplex_initialize() */
  rc = sqlite3_multiplex_initialize(zName, makeDefault);
  Tcl_SetResult(interp, (char *)sqlite3TestErrorName(rc), TCL_STATIC);

  return TCL_OK;
}

/*
** tclcmd: sqlite3_multiplex_shutdown
*/
static int test_multiplex_shutdown(
  void * clientData,
  Tcl_Interp *interp,
  int objc,
  Tcl_Obj *CONST objv[]
){
  int rc;                         /* Value returned by multiplex_shutdown() */

  UNUSED_PARAMETER(clientData);

  if( objc!=1 ){
    Tcl_WrongNumArgs(interp, 1, objv, "");
    return TCL_ERROR;
  }

  /* Call sqlite3_multiplex_shutdown() */
  rc = sqlite3_multiplex_shutdown();
  Tcl_SetResult(interp, (char *)sqlite3TestErrorName(rc), TCL_STATIC);

  return TCL_OK;
}

/*
** tclcmd: sqlite3_multiplex_set CHUNK_SIZE MAX_CHUNKS
*/
static int test_multiplex_set(
  void * clientData,
  Tcl_Interp *interp,
  int objc,
  Tcl_Obj *CONST objv[]
){
  int nChunkSize;                 /* Max chunk size */
  int nMaxChunks;                 /* Max number of chunks */
  int rc;                         /* Value returned by sqlite3_multiplex_set() */

  UNUSED_PARAMETER(clientData);

  /* Process arguments */
  if( objc!=3 ){
    Tcl_WrongNumArgs(interp, 1, objv, "CHUNK_SIZE MAX_CHUNKS");
    return TCL_ERROR;
  }
  if( Tcl_GetIntFromObj(interp, objv[1], &nChunkSize) ) return TCL_ERROR;
  if( Tcl_GetIntFromObj(interp, objv[2], &nMaxChunks) ) return TCL_ERROR;

  /* Invoke sqlite3_multiplex_set() */
  rc = sqlite3_multiplex_set(nChunkSize, nMaxChunks);

  Tcl_SetResult(interp, (char *)sqlite3TestErrorName(rc), TCL_STATIC);
  return TCL_OK;
}

/*
** tclcmd:  sqlite3_multiplex_dump
*/
static int test_multiplex_dump(
  void * clientData,
  Tcl_Interp *interp,
  int objc,
  Tcl_Obj *CONST objv[]
){
  Tcl_Obj *pResult;
  Tcl_Obj *pGroupTerm;
  multiplexGroup *pGroup;
  int i;
  int nChunks = 0;

  UNUSED_PARAMETER(clientData);
  UNUSED_PARAMETER(objc);
  UNUSED_PARAMETER(objv);

  pResult = Tcl_NewObj();
  multiplexEnter();
  for(pGroup=gMultiplex.pGroups; pGroup; pGroup=pGroup->pNext){
    pGroupTerm = Tcl_NewObj();

    pGroup->zName[pGroup->nName] = '\0';
    Tcl_ListObjAppendElement(interp, pGroupTerm,
          Tcl_NewStringObj(pGroup->zName, -1));
    Tcl_ListObjAppendElement(interp, pGroupTerm,
          Tcl_NewIntObj(pGroup->nName));
    Tcl_ListObjAppendElement(interp, pGroupTerm,
          Tcl_NewIntObj(pGroup->flags));

    /* count number of chunks with open handles */
    for(i=0; i<gMultiplex.nMaxChunks; i++){
      if( pGroup->bOpen[i] ) nChunks++;
    }
    Tcl_ListObjAppendElement(interp, pGroupTerm,
          Tcl_NewIntObj(nChunks));

    Tcl_ListObjAppendElement(interp, pGroupTerm,
          Tcl_NewIntObj(gMultiplex.nChunkSize));
    Tcl_ListObjAppendElement(interp, pGroupTerm,
          Tcl_NewIntObj(gMultiplex.nMaxChunks));

    Tcl_ListObjAppendElement(interp, pResult, pGroupTerm);
  }
  multiplexLeave();
  Tcl_SetObjResult(interp, pResult);
  return TCL_OK;
}

/*
** This routine registers the custom TCL commands defined in this
** module.  This should be the only procedure visible from outside
** of this module.
*/
int Sqlitemultiplex_Init(Tcl_Interp *interp){
  static struct {
     char *zName;
     Tcl_ObjCmdProc *xProc;
  } aCmd[] = {
    { "sqlite3_multiplex_initialize", test_multiplex_initialize },
    { "sqlite3_multiplex_shutdown", test_multiplex_shutdown },
    { "sqlite3_multiplex_set", test_multiplex_set },
    { "sqlite3_multiplex_dump", test_multiplex_dump },
  };
  int i;

  for(i=0; i<sizeof(aCmd)/sizeof(aCmd[0]); i++){
    Tcl_CreateObjCommand(interp, aCmd[i].zName, aCmd[i].xProc, 0, 0);
  }

  return TCL_OK;
}
#endif

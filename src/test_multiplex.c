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
** pager and the real VFS - that breaks up a very large database file
** into two or more smaller files on disk.  This is useful, for example,
** in order to support large, multi-gigabyte databases on older filesystems
** that limit the maximum file size to 2 GiB.
**
** USAGE:
**
** Compile this source file and link it with your application.  Then
** at start-time, invoke the following procedure:
**
**   int sqlite3_multiplex_initialize(
**      const char *zOrigVfsName,    // The underlying real VFS
**      int makeDefault              // True to make multiplex the default VFS
**   );
**
** The procedure call above will create and register a new VFS shim named
** "multiplex".  The multiplex VFS will use the VFS named by zOrigVfsName to
** do the actual disk I/O.  (The zOrigVfsName parameter may be NULL, in 
** which case the default VFS at the moment sqlite3_multiplex_initialize()
** is called will be used as the underlying real VFS.)  
**
** If the makeDefault parameter is TRUE then multiplex becomes the new
** default VFS.  Otherwise, you can use the multiplex VFS by specifying
** "multiplex" as the 4th parameter to sqlite3_open_v2() or by employing
** URI filenames and adding "vfs=multiplex" as a parameter to the filename
** URI.
**
** The multiplex VFS allows databases up to 32 GiB in size.  But it splits
** the files up into 1 GiB pieces, so that they will work even on filesystems
** that do not support large files.
*/
#include "sqlite3.h"
#include <string.h>
#include <assert.h>
#include "test_multiplex.h"

#ifndef SQLITE_CORE
  #define SQLITE_CORE 1  /* Disable the API redefinition in sqlite3ext.h */
#endif
#include "sqlite3ext.h"

/* 
** These should be defined to be the same as the values in 
** sqliteInt.h.  They are defined seperately here so that
** the multiplex VFS shim can be built as a loadable 
** module.
*/
#define UNUSED_PARAMETER(x) (void)(x)
#define MAX_PAGE_SIZE       0x10000
#define DEFAULT_SECTOR_SIZE 0x1000

/*
** For a build without mutexes, no-op the mutex calls.
*/
#if defined(SQLITE_THREADSAFE) && SQLITE_THREADSAFE==0
#define sqlite3_mutex_alloc(X)    ((sqlite3_mutex*)8)
#define sqlite3_mutex_free(X)
#define sqlite3_mutex_enter(X)
#define sqlite3_mutex_try(X)      SQLITE_OK
#define sqlite3_mutex_leave(X)
#define sqlite3_mutex_held(X)     ((void)(X),1)
#define sqlite3_mutex_notheld(X)  ((void)(X),1)
#endif /* SQLITE_THREADSAFE==0 */


/************************ Shim Definitions ******************************/

#define SQLITE_MULTIPLEX_VFS_NAME "multiplex"

/* This is the limit on the chunk size.  It may be changed by calling
** the xFileControl() interface.  It will be rounded up to a 
** multiple of MAX_PAGE_SIZE.  We default it here to 1GB.
*/
#define SQLITE_MULTIPLEX_CHUNK_SIZE (MAX_PAGE_SIZE*16384)

/* Default limit on number of chunks.  Care should be taken
** so that values for chunks numbers fit in the SQLITE_MULTIPLEX_EXT_FMT
** format specifier. It may be changed by calling
** the xFileControl() interface.
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
  char *bOpen;                     /* array of bools - 0 if chunk not opened */
  char *zName;                     /* Base filename of this group */
  int nName;                       /* Length of base filename */
  int flags;                       /* Flags used for original opening */
  int nChunkSize;                  /* Chunk size used for this group */
  int nMaxChunks;                  /* Max number of chunks for this group */
  int bEnabled;                    /* TRUE to use Multiplex VFS for this file */
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

/*
** Compute a string length that is limited to what can be stored in
** lower 30 bits of a 32-bit signed integer.
**
** The value returned will never be negative.  Nor will it ever be greater
** than the actual length of the string.  For very long strings (greater
** than 1GiB) the value returned might be less than the true string length.
*/
static int multiplexStrlen30(const char *z){
  const char *z2 = z;
  if( z==0 ) return 0;
  while( *z2 ){ z2++; }
  return 0x3fffffff & (int)(z2 - z);
}

/*
** Create a temporary file name in zBuf.  zBuf must be big enough to
** hold at pOrigVfs->mxPathname characters.  This function departs
** from the traditional temporary name generation in the os_win
** and os_unix VFS in several ways, but is necessary so that 
** the file name is known for temporary files (like those used 
** during vacuum.)
**
** N.B. This routine assumes your underlying VFS is ok with using
** "/" as a directory seperator.  This is the default for UNIXs
** and is allowed (even mixed) for most versions of Windows.
*/
static int multiplexGetTempname(sqlite3_vfs *pOrigVfs, int nBuf, char *zBuf){
  static char zChars[] =
    "abcdefghijklmnopqrstuvwxyz"
    "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    "0123456789";
  int i,j;
  int attempts = 0;
  int exists = 0;
  int rc = SQLITE_ERROR;

  /* Check that the output buffer is large enough for 
  ** pVfs->mxPathname characters.
  */
  if( pOrigVfs->mxPathname <= nBuf ){
    char *zTmp = sqlite3_malloc(pOrigVfs->mxPathname);
    if( zTmp==0 ) return SQLITE_NOMEM;

    /* sqlite3_temp_directory should always be less than
    ** pVfs->mxPathname characters.
    */
    sqlite3_snprintf(pOrigVfs->mxPathname,
                     zTmp,
                     "%s/",
                     sqlite3_temp_directory ? sqlite3_temp_directory : ".");
    rc = pOrigVfs->xFullPathname(pOrigVfs, zTmp, nBuf, zBuf);
    sqlite3_free(zTmp);
    if( rc ) return rc;

    /* Check that the output buffer is large enough for the temporary file 
    ** name.
    */
    j = multiplexStrlen30(zBuf);
    if( (j + 8 + 1 + 3 + 1) <= nBuf ){
      /* Make 3 attempts to generate a unique name. */
      do {
        attempts++;
        sqlite3_randomness(8, &zBuf[j]);
        for(i=0; i<8; i++){
          zBuf[j+i] = (char)zChars[ ((unsigned char)zBuf[j+i])%(sizeof(zChars)-1) ];
        }
        memcpy(&zBuf[j+i], ".tmp", 5);
        rc = pOrigVfs->xAccess(pOrigVfs, zBuf, SQLITE_ACCESS_EXISTS, &exists);
      } while ( (rc==SQLITE_OK) && exists && (attempts<3) );
      if( rc==SQLITE_OK && exists ){
        rc = SQLITE_ERROR;
      }
    }
  }

  return rc;
}

/* Translate an sqlite3_file* that is really a multiplexGroup* into
** the sqlite3_file* for the underlying original VFS.
*/
static sqlite3_file *multiplexSubOpen(multiplexConn *pConn, int iChunk, int *rc, int *pOutFlags){
  multiplexGroup *pGroup = pConn->pGroup;
  sqlite3_vfs *pOrigVfs = gMultiplex.pOrigVfs;        /* Real VFS */
  if( iChunk<pGroup->nMaxChunks ){
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

/*
** This is the implementation of the multiplex_control() SQL function.
*/
static void multiplexControlFunc(
  sqlite3_context *context,
  int argc,
  sqlite3_value **argv
){
  int rc = SQLITE_OK;
  sqlite3 *db = sqlite3_context_db_handle(context);
  int op;
  int iVal;

  if( !db || argc!=2 ){ 
    rc = SQLITE_ERROR; 
  }else{
    /* extract params */
    op = sqlite3_value_int(argv[0]);
    iVal = sqlite3_value_int(argv[1]);
    /* map function op to file_control op */
    switch( op ){
      case 1: 
        op = MULTIPLEX_CTRL_ENABLE; 
        break;
      case 2: 
        op = MULTIPLEX_CTRL_SET_CHUNK_SIZE; 
        break;
      case 3: 
        op = MULTIPLEX_CTRL_SET_MAX_CHUNKS; 
        break;
      default:
        rc = SQLITE_NOTFOUND;
        break;
    }
  }
  if( rc==SQLITE_OK ){
    rc = sqlite3_file_control(db, 0, op, &iVal);
  }
  sqlite3_result_error_code(context, rc);
}

/*
** This is the entry point to register the auto-extension for the 
** multiplex_control() function.
*/
static int multiplexFuncInit(
  sqlite3 *db, 
  char **pzErrMsg, 
  const sqlite3_api_routines *pApi
){
  int rc;
  rc = sqlite3_create_function(db, "multiplex_control", 2, SQLITE_ANY, 
      0, multiplexControlFunc, 0, 0);
  return rc;
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
  int rc = SQLITE_OK;                            /* Result code */
  multiplexConn *pMultiplexOpen;                 /* The new multiplex file descriptor */
  multiplexGroup *pGroup;                        /* Corresponding multiplexGroup object */
  sqlite3_file *pSubOpen;                        /* Real file descriptor */
  sqlite3_vfs *pOrigVfs = gMultiplex.pOrigVfs;   /* Real VFS */
  int nName;
  int i;
  int sz;

  UNUSED_PARAMETER(pVfs);

  /* We need to create a group structure and manage
  ** access to this group of files.
  */
  multiplexEnter();
  pMultiplexOpen = (multiplexConn*)pConn;

  /* If the second argument to this function is NULL, generate a 
  ** temporary file name to use.  This will be handled by the
  ** original xOpen method.  We just need to allocate space for
  ** it.
  */
  if( !zName ){
    rc = multiplexGetTempname(pOrigVfs, pOrigVfs->mxPathname, gMultiplex.zName);
    zName = gMultiplex.zName;
  }

  if( rc==SQLITE_OK ){
    /* allocate space for group */
    nName = multiplexStrlen30(zName);
    sz = sizeof(multiplexGroup)                                /* multiplexGroup */
       + (sizeof(sqlite3_file *)*SQLITE_MULTIPLEX_MAX_CHUNKS)  /* pReal[] */
       + (pOrigVfs->szOsFile*SQLITE_MULTIPLEX_MAX_CHUNKS)      /* *pReal */
       + SQLITE_MULTIPLEX_MAX_CHUNKS                           /* bOpen[] */
       + nName + 1;                                            /* zName */
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
    }
  }

  if( rc==SQLITE_OK ){
    /* assign pointers to extra space allocated */
    char *p = (char *)&pGroup[1];
    pMultiplexOpen->pGroup = pGroup;
    memset(pGroup, 0, sz);
    pGroup->bEnabled = -1;
    pGroup->nChunkSize = SQLITE_MULTIPLEX_CHUNK_SIZE;
    pGroup->nMaxChunks = SQLITE_MULTIPLEX_MAX_CHUNKS;
    pGroup->pReal = (sqlite3_file **)p;
    p += (sizeof(sqlite3_file *)*pGroup->nMaxChunks);
    for(i=0; i<pGroup->nMaxChunks; i++){
      pGroup->pReal[i] = (sqlite3_file *)p;
      p += pOrigVfs->szOsFile;
    }
    /* bOpen[] vals should all be zero from memset above */
    pGroup->bOpen = p;
    p += pGroup->nMaxChunks;
    pGroup->zName = p;
    /* save off base filename, name length, and original open flags  */
    memcpy(pGroup->zName, zName, nName+1);
    pGroup->nName = nName;
    pGroup->flags = flags;
    pSubOpen = multiplexSubOpen(pMultiplexOpen, 0, &rc, pOutFlags);
    if( pSubOpen ){
      /* if this file is already larger than chunk size, disable 
      ** the multiplex feature.
      */
      sqlite3_int64 sz;
      int rc2 = pSubOpen->pMethods->xFileSize(pSubOpen, &sz);
      if( (rc2==SQLITE_OK) && (sz>pGroup->nChunkSize) ){
        pGroup->bEnabled = 0;
      }
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
  int nName = multiplexStrlen30(zName);
  int i;

  UNUSED_PARAMETER(pVfs);

  multiplexEnter();
  memcpy(gMultiplex.zName, zName, nName+1);
  for(i=0; i<SQLITE_MULTIPLEX_MAX_CHUNKS; i++){
    int rc2;
    int exists = 0;
    if( i ){
#ifdef SQLITE_MULTIPLEX_EXT_OVWR
        sqlite3_snprintf(SQLITE_MULTIPLEX_EXT_SZ+1, 
            gMultiplex.zName+nName-SQLITE_MULTIPLEX_EXT_SZ, 
            SQLITE_MULTIPLEX_EXT_FMT, i);
#else
        sqlite3_snprintf(SQLITE_MULTIPLEX_EXT_SZ+1, 
            gMultiplex.zName+nName, 
            SQLITE_MULTIPLEX_EXT_FMT, i);
#endif
    }
    rc2 = pOrigVfs->xAccess(pOrigVfs, gMultiplex.zName, 
        SQLITE_ACCESS_EXISTS, &exists);
    if( rc2==SQLITE_OK && exists ){
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
  for(i=0; i<pGroup->nMaxChunks; i++){
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
  multiplexGroup *pGroup = p->pGroup;
  int rc = SQLITE_OK;
  multiplexEnter();
  if( !pGroup->bEnabled ){
    sqlite3_file *pSubOpen = multiplexSubOpen(p, 0, &rc, NULL);
    rc = ( !pSubOpen ) ? SQLITE_IOERR_READ : pSubOpen->pMethods->xRead(pSubOpen, pBuf, iAmt, iOfst);
  }else{
    while( iAmt > 0 ){
      int i = (int)(iOfst / pGroup->nChunkSize);
      sqlite3_file *pSubOpen = multiplexSubOpen(p, i, &rc, NULL);
      if( pSubOpen ){
        int extra = ((int)(iOfst % pGroup->nChunkSize) + iAmt) - pGroup->nChunkSize;
        if( extra<0 ) extra = 0;
        iAmt -= extra;
        rc = pSubOpen->pMethods->xRead(pSubOpen, pBuf, iAmt, iOfst % pGroup->nChunkSize);
        if( rc!=SQLITE_OK ) break;
        pBuf = (char *)pBuf + iAmt;
        iOfst += iAmt;
        iAmt = extra;
      }else{
        rc = SQLITE_IOERR_READ;
        break;
      }
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
  multiplexGroup *pGroup = p->pGroup;
  int rc = SQLITE_OK;
  multiplexEnter();
  if( !pGroup->bEnabled ){
    sqlite3_file *pSubOpen = multiplexSubOpen(p, 0, &rc, NULL);
    rc = ( !pSubOpen ) ? SQLITE_IOERR_WRITE : pSubOpen->pMethods->xWrite(pSubOpen, pBuf, iAmt, iOfst);
  }else{
    while( iAmt > 0 ){
      int i = (int)(iOfst / pGroup->nChunkSize);
      sqlite3_file *pSubOpen = multiplexSubOpen(p, i, &rc, NULL);
      if( pSubOpen ){
        int extra = ((int)(iOfst % pGroup->nChunkSize) + iAmt) - pGroup->nChunkSize;
        if( extra<0 ) extra = 0;
        iAmt -= extra;
        rc = pSubOpen->pMethods->xWrite(pSubOpen, pBuf, iAmt, iOfst % pGroup->nChunkSize);
        if( rc!=SQLITE_OK ) break;
        pBuf = (char *)pBuf + iAmt;
        iOfst += iAmt;
        iAmt = extra;
      }else{
        rc = SQLITE_IOERR_WRITE;
        break;
      }
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
  multiplexEnter();
  if( !pGroup->bEnabled ){
    sqlite3_file *pSubOpen = multiplexSubOpen(p, 0, &rc, NULL);
    rc = ( !pSubOpen ) ? SQLITE_IOERR_TRUNCATE : pSubOpen->pMethods->xTruncate(pSubOpen, size);
  }else{
    int rc2;
    int i;
    sqlite3_file *pSubOpen;
    sqlite3_vfs *pOrigVfs = gMultiplex.pOrigVfs;   /* Real VFS */
    memcpy(gMultiplex.zName, pGroup->zName, pGroup->nName+1);
    /* delete the chunks above the truncate limit */
    for(i=(int)(size / pGroup->nChunkSize)+1; i<pGroup->nMaxChunks; i++){
      /* close any open chunks before deleting them */
      if( pGroup->bOpen[i] ){
        pSubOpen = pGroup->pReal[i];
        rc2 = pSubOpen->pMethods->xClose(pSubOpen);
        if( rc2!=SQLITE_OK ) rc = SQLITE_IOERR_TRUNCATE;
        pGroup->bOpen[i] = 0;
      }
#ifdef SQLITE_MULTIPLEX_EXT_OVWR
      sqlite3_snprintf(SQLITE_MULTIPLEX_EXT_SZ+1, 
          gMultiplex.zName+pGroup->nName-SQLITE_MULTIPLEX_EXT_SZ, 
          SQLITE_MULTIPLEX_EXT_FMT, i);
#else
      sqlite3_snprintf(SQLITE_MULTIPLEX_EXT_SZ+1, 
          gMultiplex.zName+pGroup->nName, 
          SQLITE_MULTIPLEX_EXT_FMT, i);
#endif
      rc2 = pOrigVfs->xDelete(pOrigVfs, gMultiplex.zName, 0);
      if( rc2!=SQLITE_OK ) rc = SQLITE_IOERR_TRUNCATE;
    }
    pSubOpen = multiplexSubOpen(p, (int)(size / pGroup->nChunkSize), &rc2, NULL);
    if( pSubOpen ){
      rc2 = pSubOpen->pMethods->xTruncate(pSubOpen, size % pGroup->nChunkSize);
      if( rc2!=SQLITE_OK ) rc = rc2;
    }else{
      rc = SQLITE_IOERR_TRUNCATE;
    }
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
  for(i=0; i<pGroup->nMaxChunks; i++){
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
  if( !pGroup->bEnabled ){
    sqlite3_file *pSubOpen = multiplexSubOpen(p, 0, &rc, NULL);
    rc = ( !pSubOpen ) ? SQLITE_IOERR_FSTAT : pSubOpen->pMethods->xFileSize(pSubOpen, pSize);
  }else{
    *pSize = 0;
    for(i=0; i<pGroup->nMaxChunks; i++){
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
          sqlite3_snprintf(SQLITE_MULTIPLEX_EXT_SZ+1, 
              gMultiplex.zName+pGroup->nName-SQLITE_MULTIPLEX_EXT_SZ, 
              SQLITE_MULTIPLEX_EXT_FMT, i);
#else
          sqlite3_snprintf(SQLITE_MULTIPLEX_EXT_SZ+1, 
              gMultiplex.zName+pGroup->nName, 
              SQLITE_MULTIPLEX_EXT_FMT, i);
#endif
        }
        rc2 = pOrigVfs->xAccess(pOrigVfs, gMultiplex.zName, 
            SQLITE_ACCESS_EXISTS, &exists);
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
          if( sz>pGroup->nChunkSize ){
            rc = SQLITE_IOERR_FSTAT;
          }
          *pSize += sz;
        }
      }else{
        break;
      }
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

/* Pass xFileControl requests through to the original VFS unchanged,
** except for any MULTIPLEX_CTRL_* requests here.
*/
static int multiplexFileControl(sqlite3_file *pConn, int op, void *pArg){
  multiplexConn *p = (multiplexConn*)pConn;
  multiplexGroup *pGroup = p->pGroup;
  int rc = SQLITE_ERROR;
  sqlite3_file *pSubOpen;

  if( !gMultiplex.isInitialized ) return SQLITE_MISUSE;
  switch( op ){
    case MULTIPLEX_CTRL_ENABLE:
      if( pArg ) {
        int bEnabled = *(int *)pArg;
        pGroup->bEnabled = bEnabled;
        rc = SQLITE_OK;
      }
      break;
    case MULTIPLEX_CTRL_SET_CHUNK_SIZE:
      if( pArg ) {
        int nChunkSize = *(int *)pArg;
        if( nChunkSize<1 ){
          rc = SQLITE_MISUSE;
        }else{
          /* Round up to nearest multiple of MAX_PAGE_SIZE. */
          nChunkSize = (nChunkSize + (MAX_PAGE_SIZE-1));
          nChunkSize &= ~(MAX_PAGE_SIZE-1);
          pGroup->nChunkSize = nChunkSize;
          rc = SQLITE_OK;
        }
      }
      break;
    case MULTIPLEX_CTRL_SET_MAX_CHUNKS:
      if( pArg ) {
        int nMaxChunks = *(int *)pArg;
        if(( nMaxChunks<1 ) || ( nMaxChunks>SQLITE_MULTIPLEX_MAX_CHUNKS )){
          rc = SQLITE_MISUSE;
        }else{
          pGroup->nMaxChunks = nMaxChunks;
          rc = SQLITE_OK;
        }
      }
      break;
    case SQLITE_FCNTL_SIZE_HINT:
    case SQLITE_FCNTL_CHUNK_SIZE:
      /* no-op these */
      rc = SQLITE_OK;
      break;
    default:
      pSubOpen = multiplexSubOpen(p, 0, &rc, NULL);
      if( pSubOpen ){
        rc = pSubOpen->pMethods->xFileControl(pSubOpen, op, pArg);
      }
      break;
  }
  return rc;
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
  return DEFAULT_SECTOR_SIZE;
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
** CAPI: Initialize the multiplex VFS shim - sqlite3_multiplex_initialize()
**
** Use the VFS named zOrigVfsName as the VFS that does the actual work.  
** Use the default if zOrigVfsName==NULL.  
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
  gMultiplex.pGroups = NULL;
  gMultiplex.isInitialized = 1;
  gMultiplex.pOrigVfs = pOrigVfs;
  gMultiplex.sThisVfs = *pOrigVfs;
  gMultiplex.sThisVfs.szOsFile += sizeof(multiplexConn);
  gMultiplex.sThisVfs.zName = SQLITE_MULTIPLEX_VFS_NAME;
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

  sqlite3_auto_extension((void*)multiplexFuncInit);

  return SQLITE_OK;
}

/*
** CAPI: Shutdown the multiplex system - sqlite3_multiplex_shutdown()
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
    for(i=0; i<pGroup->nMaxChunks; i++){
      if( pGroup->bOpen[i] ) nChunks++;
    }
    Tcl_ListObjAppendElement(interp, pGroupTerm,
          Tcl_NewIntObj(nChunks));

    Tcl_ListObjAppendElement(interp, pGroupTerm,
          Tcl_NewIntObj(pGroup->nChunkSize));
    Tcl_ListObjAppendElement(interp, pGroupTerm,
          Tcl_NewIntObj(pGroup->nMaxChunks));

    Tcl_ListObjAppendElement(interp, pResult, pGroupTerm);
  }
  multiplexLeave();
  Tcl_SetObjResult(interp, pResult);
  return TCL_OK;
}

/*
** Tclcmd: test_multiplex_control HANDLE DBNAME SUB-COMMAND ?INT-VALUE?
*/
static int test_multiplex_control(
  ClientData cd,
  Tcl_Interp *interp,
  int objc,
  Tcl_Obj *CONST objv[]
){
  int rc;                         /* Return code from file_control() */
  int idx;                        /* Index in aSub[] */
  Tcl_CmdInfo cmdInfo;            /* Command info structure for HANDLE */
  sqlite3 *db;                    /* Underlying db handle for HANDLE */
  int iValue = 0;
  void *pArg = 0;

  struct SubCommand {
    const char *zName;
    int op;
    int argtype;
  } aSub[] = {
    { "enable",       MULTIPLEX_CTRL_ENABLE,           1 },
    { "chunk_size",   MULTIPLEX_CTRL_SET_CHUNK_SIZE,   1 },
    { "max_chunks",   MULTIPLEX_CTRL_SET_MAX_CHUNKS,   1 },
    { 0, 0, 0 }
  };

  if( objc!=5 ){
    Tcl_WrongNumArgs(interp, 1, objv, "HANDLE DBNAME SUB-COMMAND INT-VALUE");
    return TCL_ERROR;
  }

  if( 0==Tcl_GetCommandInfo(interp, Tcl_GetString(objv[1]), &cmdInfo) ){
    Tcl_AppendResult(interp, "expected database handle, got \"", 0);
    Tcl_AppendResult(interp, Tcl_GetString(objv[1]), "\"", 0);
    return TCL_ERROR;
  }else{
    db = *(sqlite3 **)cmdInfo.objClientData;
  }

  rc = Tcl_GetIndexFromObjStruct(
      interp, objv[3], aSub, sizeof(aSub[0]), "sub-command", 0, &idx
  );
  if( rc!=TCL_OK ) return rc;

  switch( aSub[idx].argtype ){
    case 1:
      if( Tcl_GetIntFromObj(interp, objv[4], &iValue) ){
        return TCL_ERROR;
      }
      pArg = (void *)&iValue;
      break;
    default:
      Tcl_WrongNumArgs(interp, 4, objv, "SUB-COMMAND");
      return TCL_ERROR;
  }

  rc = sqlite3_file_control(db, Tcl_GetString(objv[2]), aSub[idx].op, pArg);
  Tcl_SetResult(interp, (char *)sqlite3TestErrorName(rc), TCL_STATIC);
  return (rc==SQLITE_OK) ? TCL_OK : TCL_ERROR;
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
    { "sqlite3_multiplex_dump", test_multiplex_dump },
    { "sqlite3_multiplex_control", test_multiplex_control },
  };
  int i;

  for(i=0; i<sizeof(aCmd)/sizeof(aCmd[0]); i++){
    Tcl_CreateObjCommand(interp, aCmd[i].zName, aCmd[i].xProc, 0, 0);
  }

  return TCL_OK;
}
#endif

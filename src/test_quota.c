/*
** 2010 September 31
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
** This particular shim enforces a quota system on files.  One or more
** database files are in a "quota group" that is defined by a GLOB
** pattern.  A quota is set for the combined size of all files in the
** the group.  A quota of zero means "no limit".  If the total size
** of all files in the quota group is met or exceeded, then new
** write requests that attempt to enlarge a file fail with SQLITE_FULL.
**
** However, before returning SQLITE_FULL, the write requests invoke
** a callback function that is configurable for each quota group.
** This callback has the opportunity to enlarge the quota.  If the
** callback does enlarge the quota such that the total size of all
** files within the group is less than the new quota, then the write
** continues as if nothing had happened.
*/
#include "sqlite3.h"

/************************ Object Definitions ******************************/

/*
** This module contains a table of filename patterns that have size
** quotas.  The quota applies to the sum of the sizes of all open
** database files whose names match the GLOB pattern.
**
** Each quota is an instance of the following object.  Quotas must
** be established (using sqlite3_quota_set()) prior to opening any
** of the database connections that access files governed by the
** quota.
**
** Each entry in the quota table is an instance of the following object.
*/
typedef struct quotaGroup quotaGroup;
struct quotaGroup {
  const char *zPattern;          /* Filename pattern to be quotaed */
  sqlite3_int64 iLimit;          /* Upper bound on total file size */
  sqlite3_int64 iSize;           /* Current size of all files */
  void (*xCallback)(             /* Callback invoked when going over quota */
     const char *zFilename,         /* Name of file whose size increases */
     sqlite3_int64 *piLimit,        /* IN/OUT: The current limit */
     sqlite3_int64 iSize,           /* Total size of all files in the group */
     void *pArg                     /* Client data */
  );
  void *pArg;                    /* Third argument to the xCallback() */
  int nRef;                      /* Number of files in the group references. */
  quotaGroup *pNext, **ppPrev;   /* Doubly linked list of all quota objects */
};

/*
** An instance of the following object represents each file that
** participates in quota tracking.  The sqlite3_file object for the
** underlying VFS is appended to this structure.
*/
typedef struct quotaFile quotaFile;
struct quotaFile {
  sqlite3_file base;            /* Base class - must be first */
  const char *zFilename;        /* Name of this file */
  quotaGroup *pGroup;           /* Upper bound on file size */
  /* The underlying VFS sqlite3_file is appended to this object */
};

/************************* Global Variables **********************************/
/*
** All global variables used by this file are containing within the following
** gQuota structure.
*/
static struct {
  /* The pOrigVfs is a pointer to the real underlying VFS implementation.
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

  /* True when this shim as been initialized.
  */
  int isInitialized;

  /* For run-time access any of the other global data structures in this
  ** shim, the following mutex must be held.
  */
  sqlite3_mutex *pMutex;

  /* List of quotaGroup objects.
  */
  quotaGroup *pGroup;

} gQuota;

/************************* Utility Routines *********************************/
/*
** Acquire and release the mutex used to serialize access to the
** list of quotaGroups.
*/
static void quotaEnter(void){ sqlite3_mutex_enter(gQuota.pMutex); }
static void quotaLeave(void){ sqlite3_mutex_leave(gQuota.pMutex); }


/* If the reference count and threshold for a quotaGroup are both
** zero, then destroy the quotaGroup.
*/
static void quotaGroupDeref(quotaFile *p){
  if( p->nRef==0 && p->iLimit==0 ){
    if( p->pNext ) p->pNext->ppPrev = p->ppPrev;
    if( p->ppPrev ) *p->ppPrev = p->pNext;
    sqlite3_free(p);
  }
}

/*
** Return TRUE if string z matches glob pattern zGlob.
**
** Globbing rules:
**
**      '*'       Matches any sequence of zero or more characters.
**
**      '?'       Matches exactly one character.
**
**     [...]      Matches one character from the enclosed list of
**                characters.
**
**     [^...]     Matches one character not in the enclosed list.
**
*/
static int strglob(const char *zGlob, const char *z){
  int c, c2;
  int invert;
  int seen;

  while( (c = (*(zGlob++)))!=0 ){
    if( c=='*' ){
      while( (c=(*(zGlob++))) == '*' || c=='?' ){
        if( c=='?' && (*(z++))==0 ) return 0;
      }
      if( c==0 ){
        return 1;
      }else if( c=='[' ){
        while( *z && th3strglob(zGlob-1,z)==0 ){
          z++;
        }
        return (*z)!=0;
      }
      while( (c2 = (*(z++)))!=0 ){
        while( c2!=c ){
          c2 = *(z++);
          if( c2==0 ) return 0;
        }
        if( th3strglob(zGlob,z) ) return 1;
      }
      return 0;
    }else if( c=='?' ){
      if( (*(z++))==0 ) return 0;
    }else if( c=='[' ){
      int prior_c = 0;
      seen = 0;
      invert = 0;
      c = *(z++);
      if( c==0 ) return 0;
      c2 = *(zGlob++);
      if( c2=='^' ){
        invert = 1;
        c2 = *(zGlob++);
      }
      if( c2==']' ){
        if( c==']' ) seen = 1;
        c2 = *(zGlob++);
      }
      while( c2 && c2!=']' ){
        if( c2=='-' && zGlob[0]!=']' && zGlob[0]!=0 && prior_c>0 ){
          c2 = *(zGlob++);
          if( c>=prior_c && c<=c2 ) seen = 1;
          prior_c = 0;
        }else{
          if( c==c2 ){
            seen = 1;
          }
          prior_c = c2;
        }
        c2 = *(zGlob++);
      }
      if( c2==0 || (seen ^ invert)==0 ) return 0;
    }else{
      if( c!=(*(z++)) ) return 0;
    }
  }
  return *z==0;
}


/* Find a quotaGroup given the filename.
** Return a pointer to the quotaFile object.  return NULL if not found.
*/
static quotaGroup *quotaGroupFind(const char *zFilename){
  quotaGroup *p;
  for(p=pGroup; p && strglob(p->zPattern, zFilename)==0; p=p->pNext){}
  return p;
}

/* Translate an sqlite3_file* that is really a quotaFile* into
** an sqlite3_file* for the underlying original VFS.
*/
static sqlite3_file *quotaSubFile(sqlite3_file *pFile){
  quotaFile *p = (quotaFile*)pFile;
  return (sqlite3_file*)&p[1];
}

/************************* VFS Method Wrappers *****************************/
/*
** This is the xOpen method used for the "quota" VFS.
**
** Most of the work is done by the underlying original VFS.  This method
** simply links the new file into the quota group if it is a file that
** needs to be tracked.
*/
static int quotaOpen(
  sqlite3_vfs *pVfs,
  const char *zName,
  sqlite3_file *pFile,
  int flags,
  int *pOutFlags
){
  int rc;
  quotaFile *pQuotaFile;
  sqlite3_file *pSubFile;

  /* If the file is not a main database file or a WAL, then use the
  ** normal xOpen method.
  */
  if( (flags & (SQLITE_OPEN_MAIN_DB|SQLITE_OPEN_MAIN_WAL))==0 ){
    return  gQuota.pOrigVfs->xOpen(gQuota.pOrigVfs, zName,
                                   pFile, flags, pOutFlags);
  }

  /* If the name of the file does not match any quota group, then
  ** use the normal xOpen method.
  */
  quotaEnter();
  pGroup = quotaFindGroup(zName);
  if( pGroup==0 ){
    return  gQuota.pOrigVfs->xOpen(gQuota.pOrigVfs, zName,
                                   pFile, flags, pOutFlags);
  }

  /* If we get to this point, it means the file needs
  ** to be quota tracked.
  */
  pQuotaFile = (quotaFile*)pFile;
  pSubFile = quotaSubFile(pFile);
  rc = gQuota.pOrigVfs->xOpen(gQuota.pOrigVfs, zName,
                              pSubFile, flags, pOutFlags);
  if( rc==SQLITE_OK ){
    pQuotaFile->iSize = 0;
    pQuotaFile->pGroup = pGroup;
    pGroup->nRef++;
    quotaLeave();
    pQuotaFile->zFilename = zName;
    if( pSubFile->pMethods.iVersion==1 ){
      pQuotaFile->base.pMethods = &gQuota.sIoMethodsV1;
    }else{
      pQuotaFile->base.pMethods = &gQuota.sIoMethodsV2;
    }
  }
  return rc;
}

/************************ I/O Method Wrappers *******************************/

/* xClose requests get passed through to the original VFS.  But we
** also have to unlink the quotaFile from the quotaGroup.
*/
static int quotaClose(sqlite3_file *pFile){
  quotaFile *p = (quotaFile*)pFile;
  quotaGroup *pGroup = p->pGroup;
  sqlite3_file *pSubFile = quotaSubFile(pFile);
  int rc;
  rc = pSubFile->pMethods.xClose(pSubFile);
  quotaEnter();
  pGroup->nRef--;
  pGroup->iSize -= p->iSize;
  quotaGroupDeref(pGroup);
  quotaLeave();
  return rc;
}

/* Pass xRead requests directory thru to the original VFS without
** further processing.
*/
static int quotaRead(
  sqlite3_file *pFile,
  void *pBuf,
  int iAmt,
  sqlite3_int64 iOfst
){
  sqlite3_file *pSubFile = quotaSubFile(pFile);
  return pSubFile->pMethods.xRead(pSubFile, pBuf, iAmt, iOfst);
}

/* Check xWrite requests to see if they expand the file.  If they do,
** the perform a quota check before passing them through to the
** original VFS.
*/
static int quotaWrite(
  sqlite3_file *pFile,
  void *pBuf,
  int iAmt,
  sqlite3_int64 iOfst
){
  quotaFile *p = (quotaFile*)pFile;
  sqlite3_file *pSubFile = quotaSubFile(pFile);
  sqlite3_int64 iEnd = iOfst+iAmt;
  quotaGroup *pGroup;
  sqlite3_int64 szNew;

  if( p->iSize<iEnd ){
    pGroup = p->pGroup;
    quotaEnter();
    szNew = pGroup->iSize - p->iSize + iEnd;
    if( szNew >= pGroup->iLimit && pGroup->iLimit > 0 ){
      if( pGroup->xCallback ){
        pGroup->xCallback(p->zFilename, &pGroup->iLimit, szNew, 
                          pGroup->pArg);
      }
      if( szNew >= pGroup->iLimit && pGroup->iLimit > 0 ){
        quotaLeave();
        return SQLITE_FULL;
      }
    }
    pGroup->iSize = szNew;
    quotaLeave();
  }
  return pSubFile->pMethods.xWrite(pSubFile, pBuf, iAmt, iOfst);
}

/* Pass xTruncate requests thru to the original VFS.  If the
** success, update the file size.
*/
static int quotaTruncate(sqlite3_file *pFile, sqlite3_int64 size){
  quotaFile *p = (quotaFile*)pFile);
  sqlite3_file *pSubFile = quotaSubFile(pFile);
  int rc = pSubFile->pMethods.xTruncate(pSubFile, size);
  quotaGroup *pGroup = p->pGroup;
  if( rc==SQLITE_OK ){
    quotaEnter();
    pGroup->iSize -= p->iSize;
    p->iSize = size;
    pGroup->iSize += size;
    quotaLeave();
  }
  return rc;
}

/* Pass xSync requests through to the original VFS without change
*/
static int quotaSync(sqlite3_file *pFile, int flags){
  sqlite3_file *pSubFile = quotaSubFile(pFile);
  return pSubFile->pMethods.xSync(pSubFile, flags);
}

/* Pass xFileSize requests through to the original VFS but then
** update the quotaGroup with the new size before returning.
*/
static int quotaFileSize(sqlite3_file *pFile, sqlite3_int64 *pSize){
  quotaFile *p = (quotaFile*)pFile;
  sqlite3_file *pSubFile = quotaSubFile(pFile);
  quotaGroup *pGroup;
  sqlite3_int64 sz;
  int rc;

  rc = pSubFile->pMethods.xFileSize(pSubFile, &sz);
  if( rc==SQLITE_OK ){
    pGroup = p->pGroup;
    quotaEnter();
    pGroup->iSize -= p->iSize;
    p->iSize = sz;
    pGroup->iSize += sz;
    quotaLeave();
    *pSize = sz;
  }
  return rc;
}

/* Pass xLock requests through to the original VFS unchanged.
*/
static int quotaLock(sqlite3_file *pFile, int lock){
  sqlite3_file *pSubFile = quotaSubFile(pFile);
  return pSubFile->pMethods.xLock(pSubFile, lock);
}

/* Pass xUnlock requests through to the original VFS unchanged.
*/
static int quotaUnlock(sqlite3_file *pFile, int lock){
  sqlite3_file *pSubFile = quotaSubFile(pFile);
  return pSubFile->pMethods.xUnlock(pSubFile, lock);
}

/* Pass xCheckReservedLock requests through to the original VFS unchanged.
*/
static int quotaCheckReservedLock(sqlite3_file *pFile, int *pResOut){
  sqlite3_file *pSubFile = quotaSubFile(pFile);
  return pSubFile->pMethods.xCheckReservedLock(pSubFile, pResOut);
}

/* Pass xFileControl requests through to the original VFS unchanged.
*/
static int quotaFileControl(sqlite3_file *pFile, int op, void *pArg){
  sqlite3_file *pSubFile = quotaSubFile(pFile);
  return pSubFile->pMethods.xFileControl(pSubFile, op, pArg);
}

/* Pass xSectorSize requests through to the original VFS unchanged.
*/
static int quotaSectorSize(sqlite3_file *pFile){
  sqlite3_file *pSubFile = quotaSubFile(pFile);
  return pSubFile->pMethods.xSectorSize(pSubFile);
}

/* Pass xDeviceCharacteristics requests through to the original VFS unchanged.
*/
static int quotaDeviceCharacteristics(sqlite3_file *pFile){
  sqlite3_file *pSubFile = quotaSubFile(pFile);
  return pSubFile->pMethods.xDeviceCharacteristics(pSubFile);
}

/* Pass xShmMap requests through to the original VFS unchanged.
*/
static int quotaShmMap(
  sqlite3_file *pFile,            /* Handle open on database file */
  int iRegion,                    /* Region to retrieve */
  int szRegion,                   /* Size of regions */
  int bExtend,                    /* True to extend file if necessary */
  void volatile **pp              /* OUT: Mapped memory */
){
  sqlite3_file *pSubFile = quotaSubFile(pFile);
  return pSubFile->pMethods.xShmMap(pSubFile, iRegion, szRegion, bExtend, pp);
}

/* Pass xShmLock requests through to the original VFS unchanged.
*/
static int quotaShmLock(
  sqlite3_file *pFile,       /* Database file holding the shared memory */
  int ofst,                  /* First lock to acquire or release */
  int n,                     /* Number of locks to acquire or release */
  int flags                  /* What to do with the lock */
){
  sqlite3_file *pSubFile = quotaSubFile(pFile);
  return pSubFile->pMethods.xShmLock(pSubFile, ofst, n, flags);
}

/* Pass xShmBarrier requests through to the original VFS unchanged.
*/
static int quotaShmBarrier(sqlite3_file *pFile){
  sqlite3_file *pSubFile = quotaSubFile(pFile);
  return pSubFile->pMethods.xShmBarrier(pSubFile);
}

/* Pass xShmUnmap requests through to the original VFS unchanged.
*/
static int quotaShmUnmap(sqlite3_file *pFile, int deleteFlag){
  sqlite3_file *pSubFile = quotaSubFile(pFile);
  return pSubFile->pMethods.xShmUnmap(pSubFile, deleteFlag);
}

/************************** Public Interfaces *****************************/
/*
** Initialize the quota VFS shim.  Use the VFS named zOrigVfsName
** as the VFS that does the actual work.  Use the default if
** zOrigVfsName==NULL.  
**
** The quota VFS shim is named "quota".  It will become the default
** VFS if makeDefault is non-zero.
**
** THIS ROUTINE IS NOT THREADSAFE.  Call this routine exactly once
** during start-up.
*/
int sqlite3_quota_initialize(const char *zOrigVfsName, int makeDefault){
  sqlite3_vfs *pOrigVfs;
  if( gQuota.isInitialize ) return SQLITE_MISUSE;
  gQuota.isInitialized = 1;
  pOrigVfs = sqlite3_vfs_find(zOrigVfsName);
  if( pOrigVfs==0 ) return SQLITE_ERROR;
  gQuota.pOrigVfs = pOrigVfs;
  gQuota.sThisVfs = *pOrigVfs;
  gQuota.sThisVfs.xOpen = quotaOpen;
  gQuota.sThisVfs.szOsFile += sizeof(quotaFile);
  gQuota.sThisVfs.zName = "quota";
  gQuota.sIoMethodsV1.iVersion = 1;
  gQuota.sIoMethodsV1.xClose = quotaClose;
  gQuota.sIoMethodsV1.xRead = quotaRead;
  gQuota.sIoMethodsV1.xWrite = quotaWrite;
  gQuota.sIoMethodsV1.xTruncate = quotaTruncate;
  gQuota.sIoMethodsV1.xSync = quotaSync;
  gQuota.sIoMethodsV1.xFileSize = quotaFileSize;
  gQuota.sIoMethodsV1.xLock = quotaLock;
  gQuota.sIoMethodsV1.xUnlock = quotaUnlock;
  gQuota.sIoMethodsV1.xCheckReservedLock = quotaCheckReservedLock;
  gQuota.sIoMethodsV1.xFileControl = quotaFileControl;
  gQuota.sIoMethodsV1.xSectorSize = quotaSectoSize;
  gQuota.sIoMethodsV1.xDeviceCharacteristics = quotaDeviceCharacteristics;
  gQuota.sIoMethodsV2 = gQuota.sIoMethodsV1;
  gQuota.sIoMethodsV2.iVersion = 2;
  gQuota.sIoMethodsV2.xShmMap = quotaShmMap;
  gQuota.sIoMethodsV2.xShmLock = quotaShmLock;
  gQuota.sIoMethodsV2.xShmBarrier = quotaShmBarrier;
  gQuota.sIoMethodsV2.xShmUnmap = quotaShmUnmap;
  gQuota.pMutex = sqlite3_mutex_alloc(SQLITE_MUTEX_FAST);
}

/*
** Shutdown the quota system.
**
** All SQLite database connections must be closed before calling this
** routine.
**
** THIS ROUTINE IS NOT THREADSAFE.  Call this routine exactly one while
** shutting down in order to free all remaining quota groups.
*/
int sqlite3_quota_shutdown(void){
  if( gQuota.isInitialized==0 ) return SQLITE_MISUSE;
  gQuota.isInitialized = 0;
  sqlite3_mutex_free(gQuota.pMutex);
  while( gQuota.pGroup ){
    quotaGroup *p = gQuota.pGroup;
    gQuota.pGroup = p->pNext;
    if( p->nRef ) return SQLITE_MISUSE;
    sqlite3_free(p);
  }
  memset(&gQuota, 0, sizeof(gQuota));
}

/*
** Create or destroy a quota group.
**
** The quota group is defined by the zPattern.  When calling this routine
** with a zPattern for a quota group that already exists, this routine
** merely updates the iLimit, xCallback, and pArg values for that quota
** group.  If zPattern is new, then a new quota group is created.
**
** If the iLimit for a quota group is set to zero, then the quota group
** is disabled and will be deleted when the last database connection using
** the quota group is closed.
**
** Calling this routine on a zPattern that does not exist and with a
** zero iLimit is a no-op.
**
** A quota group must exist with a non-zero iLimit prior to opening
** database connections if those connections are to participate in the
** quota group.  Creating a quota group does not effect database connections
** that are already open.
*/
int sqlite3_quota_set(
  const char *zPattern,          /* The filename pattern */
  sqlite3_int64 iLimit,          /* New quota to set for this quota group */
  void (*xCallback)(             /* Callback invoked when going over quota */
     const char *zFilename,         /* Name of file whose size increases */
     sqlite3_int64 *piLimit,        /* IN/OUT: The current limit */
     sqlite3_int64 iSize,           /* Total size of all files in the group */
     void *pArg                     /* Client data */
  );
  void *pArg                     /* client data passed thru to callback */
){
  quotaGroup *pGroup;
  quotaEnter();
  pGroup = gQuota.pGroup;
  while( pGroup && strcmp(pGroup->zPattern, zPattern)!=0 ){
    pGroup = pGroup->pNext;
  }
  if( pGroup==0 && iLimit>0 ){
    int nPattern = strlen(zPattern);
    pGroup = sqlite3_malloc( sizeof(*pGroup) + nPattern + 1 );
    if( pGroup==0 ){
      quotaLeave();
      return SQLITE_NOMEM;
    }
    memset(pGroup, 0, sizeof(*pGroup));
    pGroup->zPattern = (char*)&pGroup[1];
    memcpy(pGroup->zPattern, zPattern, nPattern+1);
    pGroup->pNext = gQuota.pGroup;
    if( gQuota.pGroup ) gQuota.pGroup->ppPrev = &pGroup->pNext;
    pGroup->ppPrev = &gQuota.pNext;
  }
  pGroup->iLimit = iLimit;
  pGroup->xCallback = xCallback;
  pGroup->pArg = pArg;
  quotaGroupDeref(pGroup);
  quotaLeave();
  return SQLITE_OK;
}

  
/***************************** Test Code ***********************************/
#ifdef SQLITE_TEST
#include <tcl.h>
/*
** This routine registers the custom TCL commands defined in this
** module.  This should be the only procedure visible from outside
** of this module.
*/
int Sqlitequota_Init(Tcl_Interp *interp){
#ifdef SQLITE_ENABLE_ASYNCIO
#endif  /* SQLITE_ENABLE_ASYNCIO */
  return TCL_OK;
}
#endif

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
#include <string.h>
#include <assert.h>

/************************ Object Definitions ******************************/

/* Forward declaration of all object types */
typedef struct quotaGroup quotaGroup;
typedef struct quotaOpen quotaOpen;
typedef struct quotaFile quotaFile;

/*
** A "quota group" is a collection of files whose collective size we want
** to limit.  Each quota group is defined by a GLOB pattern.
**
** There is an instance of the following object for each defined quota
** group.  This object records the GLOB pattern that defines which files
** belong to the quota group.  The object also remembers the size limit
** for the group (the quota) and the callback to be invoked when the
** sum of the sizes of the files within the group goes over the limit.
**
** A quota group must be established (using sqlite3_quota_set(...))
** prior to opening any of the database connections that access files
** within the quota group.
*/
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
  quotaGroup *pNext, **ppPrev;   /* Doubly linked list of all quota objects */
  quotaFile *pFile;              /* Files within this group */
};

/*
** An instance of this structure represents a single file that is part
** of a quota group.  A single file can be opened multiple times.  In
** order keep multiple openings of the same file from causing the size
** of the file to count against the quota multiple times, each file
** has a unique instance of this object and multiple open connections
** to the same file each point to a single instance of this object.
*/
struct quotaFile {
  char *zFilename;                /* Name of this file */
  quotaGroup *pGroup;             /* Upper bound on file size */
  sqlite3_int64 iSize;            /* Current size of this file */
  int nRef;                       /* Number of times this file is open */
  quotaFile *pNext, **ppPrev;     /* Linked list of files in the same group */
};

/*
** An instance of the following object represents each open connection
** to a file that participates in quota tracking.  This object is a 
** subclass of sqlite3_file.  The sqlite3_file object for the underlying
** VFS is appended to this structure.
*/
struct quotaOpen {
  sqlite3_file base;              /* Base class - must be first */
  quotaFile *pFile;               /* The underlying file */
  /* The underlying VFS sqlite3_file is appended to this object */
};

/************************* Global Variables **********************************/
/*
** All global variables used by this file are containing within the following
** gQuota structure.
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
static void quotaGroupDeref(quotaGroup *p){
  if( p->pFile==0 && p->iLimit==0 ){
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
        while( *z && strglob(zGlob-1,z)==0 ){
          z++;
        }
        return (*z)!=0;
      }
      while( (c2 = (*(z++)))!=0 ){
        while( c2!=c ){
          c2 = *(z++);
          if( c2==0 ) return 0;
        }
        if( strglob(zGlob,z) ) return 1;
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
**
** Return a pointer to the quotaOpen object. Return NULL if not found.
*/
static quotaGroup *quotaGroupFind(const char *zFilename){
  quotaGroup *p;
  for(p=gQuota.pGroup; p && strglob(p->zPattern, zFilename)==0; p=p->pNext){}
  return p;
}

/* Translate an sqlite3_file* that is really a quotaOpen* into
** the sqlite3_file* for the underlying original VFS.
*/
static sqlite3_file *quotaSubOpen(sqlite3_file *pOpen){
  quotaOpen *p = (quotaOpen*)pOpen;
  return (sqlite3_file*)&p[1];
}

/************************* VFS Method Wrappers *****************************/
/*
** This is the xOpen method used for the "quota" VFS.
**
** Most of the work is done by the underlying original VFS.  This method
** simply links the new file into the appropriate quota group if it is a
** file that needs to be tracked.
*/
static int quotaxOpen(
  sqlite3_vfs *pVfs,          /* The quota VFS */
  const char *zName,          /* Name of file to be opened */
  sqlite3_file *pOpen,        /* Fill in this file descriptor */
  int flags,                  /* Flags to control the opening */
  int *pOutFlags              /* Flags showing results of opening */
){
  int rc;                                    /* Result code */         
  quotaOpen *pQuotaOpen;                     /* The new quota file descriptor */
  quotaFile *pFile;                          /* Corresponding quotaFile obj */
  quotaGroup *pGroup;                        /* The group file belongs to */
  sqlite3_file *pSubOpen;                    /* Real file descriptor */
  sqlite3_vfs *pOrigVfs = gQuota.pOrigVfs;   /* Real VFS */

  /* If the file is not a main database file or a WAL, then use the
  ** normal xOpen method.
  */
  if( (flags & (SQLITE_OPEN_MAIN_DB|SQLITE_OPEN_WAL))==0 ){
    return pOrigVfs->xOpen(pOrigVfs, zName, pOpen, flags, pOutFlags);
  }

  /* If the name of the file does not match any quota group, then
  ** use the normal xOpen method.
  */
  quotaEnter();
  pGroup = quotaGroupFind(zName);
  if( pGroup==0 ){
    rc = pOrigVfs->xOpen(pOrigVfs, zName, pOpen, flags, pOutFlags);
  }else{
    /* If we get to this point, it means the file needs to be quota tracked.
    */
    pQuotaOpen = (quotaOpen*)pOpen;
    pSubOpen = quotaSubOpen(pOpen);
    rc = pOrigVfs->xOpen(pOrigVfs, zName, pSubOpen, flags, pOutFlags);
    if( rc==SQLITE_OK ){
      for(pFile=pGroup->pFile; pFile && strcmp(pFile->zFilename, zName);
          pFile=pFile->pNext){}
      if( pFile==0 ){
        int nName = strlen(zName);
        pFile = sqlite3_malloc( sizeof(*pFile) + nName + 1 );
        if( pFile==0 ){
          quotaLeave();
          pSubOpen->pMethods->xClose(pSubOpen);
          return SQLITE_NOMEM;
        }
        memset(pFile, 0, sizeof(*pFile));
        pFile->zFilename = (char*)&pFile[1];
        memcpy(pFile->zFilename, zName, nName+1);
        pFile->pNext = pGroup->pFile;
        if( pGroup->pFile ) pGroup->pFile->ppPrev = &pFile->pNext;
        pFile->ppPrev = &pGroup->pFile;
        pGroup->pFile = pFile;
        pFile->pGroup = pGroup;
      }
      pFile->nRef++;
      pQuotaOpen->pFile = pFile;
      if( pSubOpen->pMethods->iVersion==1 ){
        pQuotaOpen->base.pMethods = &gQuota.sIoMethodsV1;
      }else{
        pQuotaOpen->base.pMethods = &gQuota.sIoMethodsV2;
      }
    }
  }
  quotaLeave();
  return rc;
}

/************************ I/O Method Wrappers *******************************/

/* xClose requests get passed through to the original VFS.  But we
** also have to unlink the quotaOpen from the quotaGroup.
*/
static int quotaClose(sqlite3_file *pOpen){
  quotaOpen *p = (quotaOpen*)pOpen;
  quotaFile *pFile = p->pFile;
  sqlite3_file *pSubOpen = quotaSubOpen(pOpen);
  int rc;
  rc = pSubOpen->pMethods->xClose(pSubOpen);
  quotaEnter();
  pFile->nRef--;
  if( pFile->nRef==0 ){
    quotaGroup *pGroup = pFile->pGroup;
    pGroup->iSize -= pFile->iSize;
    if( pFile->pNext ) pFile->pNext->ppPrev = pFile->ppPrev;
    *pFile->ppPrev = pFile->pNext;
    quotaGroupDeref(pGroup);
    sqlite3_free(pFile);
  }
  quotaLeave();
  return rc;
}

/* Pass xRead requests directory thru to the original VFS without
** further processing.
*/
static int quotaRead(
  sqlite3_file *pOpen,
  void *pBuf,
  int iAmt,
  sqlite3_int64 iOfst
){
  sqlite3_file *pSubOpen = quotaSubOpen(pOpen);
  return pSubOpen->pMethods->xRead(pSubOpen, pBuf, iAmt, iOfst);
}

/* Check xWrite requests to see if they expand the file.  If they do,
** the perform a quota check before passing them through to the
** original VFS.
*/
static int quotaWrite(
  sqlite3_file *pOpen,
  const void *pBuf,
  int iAmt,
  sqlite3_int64 iOfst
){
  quotaOpen *p = (quotaOpen*)pOpen;
  sqlite3_file *pSubOpen = quotaSubOpen(pOpen);
  sqlite3_int64 iEnd = iOfst+iAmt;
  quotaGroup *pGroup;
  quotaFile *pFile = p->pFile;
  sqlite3_int64 szNew;

  if( pFile->iSize<iEnd ){
    pGroup = pFile->pGroup;
    quotaEnter();
    szNew = pGroup->iSize - pFile->iSize + iEnd;
    if( szNew>pGroup->iLimit && pGroup->iLimit>0 ){
      if( pGroup->xCallback ){
        pGroup->xCallback(pFile->zFilename, &pGroup->iLimit, szNew, 
                          pGroup->pArg);
      }
      if( szNew>pGroup->iLimit && pGroup->iLimit>0 ){
        quotaLeave();
        return SQLITE_FULL;
      }
    }
    pGroup->iSize = szNew;
    pFile->iSize = iEnd;
    quotaLeave();
  }
  return pSubOpen->pMethods->xWrite(pSubOpen, pBuf, iAmt, iOfst);
}

/* Pass xTruncate requests thru to the original VFS.  If the
** success, update the file size.
*/
static int quotaTruncate(sqlite3_file *pOpen, sqlite3_int64 size){
  quotaOpen *p = (quotaOpen*)pOpen;
  sqlite3_file *pSubOpen = quotaSubOpen(pOpen);
  int rc = pSubOpen->pMethods->xTruncate(pSubOpen, size);
  quotaFile *pFile = p->pFile;
  quotaGroup *pGroup;
  if( rc==SQLITE_OK ){
    quotaEnter();
    pGroup = pFile->pGroup;
    pGroup->iSize -= pFile->iSize;
    pFile->iSize = size;
    pGroup->iSize += size;
    quotaLeave();
  }
  return rc;
}

/* Pass xSync requests through to the original VFS without change
*/
static int quotaSync(sqlite3_file *pOpen, int flags){
  sqlite3_file *pSubOpen = quotaSubOpen(pOpen);
  return pSubOpen->pMethods->xSync(pSubOpen, flags);
}

/* Pass xFileSize requests through to the original VFS but then
** update the quotaGroup with the new size before returning.
*/
static int quotaFileSize(sqlite3_file *pOpen, sqlite3_int64 *pSize){
  quotaOpen *p = (quotaOpen*)pOpen;
  sqlite3_file *pSubOpen = quotaSubOpen(pOpen);
  quotaFile *pFile = p->pFile;
  quotaGroup *pGroup;
  sqlite3_int64 sz;
  int rc;

  rc = pSubOpen->pMethods->xFileSize(pSubOpen, &sz);
  if( rc==SQLITE_OK ){
    quotaEnter();
    pGroup = pFile->pGroup;
    pGroup->iSize -= pFile->iSize;
    pFile->iSize = sz;
    pGroup->iSize += sz;
    quotaLeave();
    *pSize = sz;
  }
  return rc;
}

/* Pass xLock requests through to the original VFS unchanged.
*/
static int quotaLock(sqlite3_file *pOpen, int lock){
  sqlite3_file *pSubOpen = quotaSubOpen(pOpen);
  return pSubOpen->pMethods->xLock(pSubOpen, lock);
}

/* Pass xUnlock requests through to the original VFS unchanged.
*/
static int quotaUnlock(sqlite3_file *pOpen, int lock){
  sqlite3_file *pSubOpen = quotaSubOpen(pOpen);
  return pSubOpen->pMethods->xUnlock(pSubOpen, lock);
}

/* Pass xCheckReservedLock requests through to the original VFS unchanged.
*/
static int quotaCheckReservedLock(sqlite3_file *pOpen, int *pResOut){
  sqlite3_file *pSubOpen = quotaSubOpen(pOpen);
  return pSubOpen->pMethods->xCheckReservedLock(pSubOpen, pResOut);
}

/* Pass xFileControl requests through to the original VFS unchanged.
*/
static int quotaOpenControl(sqlite3_file *pOpen, int op, void *pArg){
  sqlite3_file *pSubOpen = quotaSubOpen(pOpen);
  return pSubOpen->pMethods->xFileControl(pSubOpen, op, pArg);
}

/* Pass xSectorSize requests through to the original VFS unchanged.
*/
static int quotaSectorSize(sqlite3_file *pOpen){
  sqlite3_file *pSubOpen = quotaSubOpen(pOpen);
  return pSubOpen->pMethods->xSectorSize(pSubOpen);
}

/* Pass xDeviceCharacteristics requests through to the original VFS unchanged.
*/
static int quotaDeviceCharacteristics(sqlite3_file *pOpen){
  sqlite3_file *pSubOpen = quotaSubOpen(pOpen);
  return pSubOpen->pMethods->xDeviceCharacteristics(pSubOpen);
}

/* Pass xShmMap requests through to the original VFS unchanged.
*/
static int quotaShmMap(
  sqlite3_file *pOpen,            /* Handle open on database file */
  int iRegion,                    /* Region to retrieve */
  int szRegion,                   /* Size of regions */
  int bExtend,                    /* True to extend file if necessary */
  void volatile **pp              /* OUT: Mapped memory */
){
  sqlite3_file *pSubOpen = quotaSubOpen(pOpen);
  return pSubOpen->pMethods->xShmMap(pSubOpen, iRegion, szRegion, bExtend, pp);
}

/* Pass xShmLock requests through to the original VFS unchanged.
*/
static int quotaShmLock(
  sqlite3_file *pOpen,       /* Database file holding the shared memory */
  int ofst,                  /* First lock to acquire or release */
  int n,                     /* Number of locks to acquire or release */
  int flags                  /* What to do with the lock */
){
  sqlite3_file *pSubOpen = quotaSubOpen(pOpen);
  return pSubOpen->pMethods->xShmLock(pSubOpen, ofst, n, flags);
}

/* Pass xShmBarrier requests through to the original VFS unchanged.
*/
static void quotaShmBarrier(sqlite3_file *pOpen){
  sqlite3_file *pSubOpen = quotaSubOpen(pOpen);
  pSubOpen->pMethods->xShmBarrier(pSubOpen);
}

/* Pass xShmUnmap requests through to the original VFS unchanged.
*/
static int quotaShmUnmap(sqlite3_file *pOpen, int deleteFlag){
  sqlite3_file *pSubOpen = quotaSubOpen(pOpen);
  return pSubOpen->pMethods->xShmUnmap(pSubOpen, deleteFlag);
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
  if( gQuota.isInitialized ) return SQLITE_MISUSE;
  pOrigVfs = sqlite3_vfs_find(zOrigVfsName);
  if( pOrigVfs==0 ) return SQLITE_ERROR;
  assert( pOrigVfs!=&gQuota.sThisVfs );
  gQuota.pMutex = sqlite3_mutex_alloc(SQLITE_MUTEX_FAST);
  if( !gQuota.pMutex ){
    return SQLITE_NOMEM;
  }
  gQuota.isInitialized = 1;
  gQuota.pOrigVfs = pOrigVfs;
  gQuota.sThisVfs = *pOrigVfs;
  gQuota.sThisVfs.xOpen = quotaxOpen;
  gQuota.sThisVfs.szOsFile += sizeof(quotaOpen);
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
  gQuota.sIoMethodsV1.xFileControl = quotaOpenControl;
  gQuota.sIoMethodsV1.xSectorSize = quotaSectorSize;
  gQuota.sIoMethodsV1.xDeviceCharacteristics = quotaDeviceCharacteristics;
  gQuota.sIoMethodsV2 = gQuota.sIoMethodsV1;
  gQuota.sIoMethodsV2.iVersion = 2;
  gQuota.sIoMethodsV2.xShmMap = quotaShmMap;
  gQuota.sIoMethodsV2.xShmLock = quotaShmLock;
  gQuota.sIoMethodsV2.xShmBarrier = quotaShmBarrier;
  gQuota.sIoMethodsV2.xShmUnmap = quotaShmUnmap;
  sqlite3_vfs_register(&gQuota.sThisVfs, makeDefault);
  return SQLITE_OK;
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
  quotaGroup *p;
  if( gQuota.isInitialized==0 ) return SQLITE_MISUSE;
  for(p=gQuota.pGroup; p; p=p->pNext){
    if( p->pFile ) return SQLITE_MISUSE;
  }
  while( gQuota.pGroup ){
    quotaGroup *p = gQuota.pGroup;
    gQuota.pGroup = p->pNext;
    sqlite3_free(p);
  }
  gQuota.isInitialized = 0;
  sqlite3_mutex_free(gQuota.pMutex);
  sqlite3_vfs_unregister(&gQuota.sThisVfs);
  memset(&gQuota, 0, sizeof(gQuota));
  return SQLITE_OK;
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
** quota group.  Creating a quota group does not affect database connections
** that are already open.
*/
int sqlite3_quota_set(
  const char *zPattern,           /* The filename pattern */
  sqlite3_int64 iLimit,           /* New quota to set for this quota group */
  void (*xCallback)(              /* Callback invoked when going over quota */
     const char *zFilename,         /* Name of file whose size increases */
     sqlite3_int64 *piLimit,        /* IN/OUT: The current limit */
     sqlite3_int64 iSize,           /* Total size of all files in the group */
     void *pArg                     /* Client data */
  ),
  void *pArg                      /* client data passed thru to callback */
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
    memcpy((char *)pGroup->zPattern, zPattern, nPattern+1);
    if( gQuota.pGroup ) gQuota.pGroup->ppPrev = &pGroup->pNext;
    pGroup->pNext = gQuota.pGroup;
    pGroup->ppPrev = &gQuota.pGroup;
    gQuota.pGroup = pGroup;
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

typedef struct TclQuotaCallback TclQuotaCallback;
struct TclQuotaCallback {
  Tcl_Interp *interp;
  Tcl_Obj *pScript;
  TclQuotaCallback *pNext;
};
static TclQuotaCallback *pQuotaCallbackList = 0;

extern const char *sqlite3TestErrorName(int);

static void tclQuotaCallback(
  const char *zFilename,          /* Name of file whose size increases */
  sqlite3_int64 *piLimit,         /* IN/OUT: The current limit */
  sqlite3_int64 iSize,            /* Total size of all files in the group */
  void *pArg                      /* Client data */
){
  TclQuotaCallback *p;            /* Callback script object */
  Tcl_Obj *pEval;                 /* Script to evaluate */
  Tcl_Obj *pVarname;              /* Name of variable to pass as 2nd arg */
  unsigned int rnd;               /* Random part of pVarname */
  int rc;                         /* Tcl error code */

  p = (TclQuotaCallback *)pArg;

  pVarname = Tcl_NewStringObj("::piLimit_", -1);
  Tcl_IncrRefCount(pVarname);
  sqlite3_randomness(sizeof(rnd), (void *)&rnd);
  Tcl_AppendObjToObj(pVarname, Tcl_NewIntObj((int)(rnd&0x7FFFFFFF)));
  Tcl_ObjSetVar2(p->interp, pVarname, 0, Tcl_NewWideIntObj(*piLimit), 0);

  pEval = Tcl_DuplicateObj(p->pScript);
  Tcl_IncrRefCount(pEval);
  Tcl_ListObjAppendElement(0, pEval, Tcl_NewStringObj(zFilename, -1));
  Tcl_ListObjAppendElement(0, pEval, pVarname);
  Tcl_ListObjAppendElement(0, pEval, Tcl_NewWideIntObj(iSize));
  rc = Tcl_EvalObjEx(p->interp, pEval, TCL_EVAL_GLOBAL);

  if( rc==TCL_OK ){
    Tcl_Obj *pLimit = Tcl_ObjGetVar2(p->interp, pVarname, 0, 0);
    rc = Tcl_GetWideIntFromObj(p->interp, pLimit, piLimit);
    Tcl_UnsetVar(p->interp, Tcl_GetString(pVarname), 0);
  }

  Tcl_DecrRefCount(pEval);
  Tcl_DecrRefCount(pVarname);
  if( rc!=TCL_OK ) Tcl_BackgroundError(p->interp);
}

/*
** tclcmd: sqlite3_quota_initialize NAME MAKEDEFAULT
*/
static int test_quota_initialize(
  void * clientData,
  Tcl_Interp *interp,
  int objc,
  Tcl_Obj *CONST objv[]
){
  const char *zName;              /* Name of new quota VFS */
  int makeDefault;                /* True to make the new VFS the default */
  int rc;                         /* Value returned by quota_initialize() */

  /* Process arguments */
  if( objc!=3 ){
    Tcl_WrongNumArgs(interp, 1, objv, "NAME MAKEDEFAULT");
    return TCL_ERROR;
  }
  zName = Tcl_GetString(objv[1]);
  if( Tcl_GetBooleanFromObj(interp, objv[2], &makeDefault) ) return TCL_ERROR;
  if( zName[0]=='\0' ) zName = 0;

  /* Call sqlite3_quota_initialize() */
  rc = sqlite3_quota_initialize(zName, makeDefault);
  Tcl_SetResult(interp, (char *)sqlite3TestErrorName(rc), TCL_STATIC);

  return TCL_OK;
}

/*
** tclcmd: sqlite3_quota_shutdown
*/
static int test_quota_shutdown(
  void * clientData,
  Tcl_Interp *interp,
  int objc,
  Tcl_Obj *CONST objv[]
){
  int rc;                         /* Value returned by quota_shutdown() */

  if( objc!=1 ){
    Tcl_WrongNumArgs(interp, 1, objv, "");
    return TCL_ERROR;
  }

  /* Call sqlite3_quota_shutdown() */
  rc = sqlite3_quota_shutdown();
  Tcl_SetResult(interp, (char *)sqlite3TestErrorName(rc), TCL_STATIC);

  /* If the quota system was successfully shut down, delete all the quota
  ** callback script objects in the global linked list.
  */
  if( rc==SQLITE_OK ){
    TclQuotaCallback *p, *pNext;
    for(p=pQuotaCallbackList; p; p=pNext){
      pNext = p->pNext;
      Tcl_DecrRefCount(p->pScript);
      ckfree((char *)p);
    }
    pQuotaCallbackList = 0;
  }

  return TCL_OK;
}

/*
** tclcmd: sqlite3_quota_set PATTERN LIMIT SCRIPT
*/
static int test_quota_set(
  void * clientData,
  Tcl_Interp *interp,
  int objc,
  Tcl_Obj *CONST objv[]
){
  const char *zPattern;           /* File pattern to configure */
  sqlite3_int64 iLimit;           /* Initial quota in bytes */
  Tcl_Obj *pScript;               /* Tcl script to invoke to increase quota */
  int rc;                         /* Value returned by quota_set() */
  TclQuotaCallback *p;            /* Callback object */

  /* Process arguments */
  if( objc!=4 ){
    Tcl_WrongNumArgs(interp, 1, objv, "PATTERN LIMIT SCRIPT");
    return TCL_ERROR;
  }
  zPattern = Tcl_GetString(objv[1]);
  if( Tcl_GetWideIntFromObj(interp, objv[2], &iLimit) ) return TCL_ERROR;
  pScript = objv[3];

  /* Allocate a TclQuotaCallback object */
  p = (TclQuotaCallback *)ckalloc(sizeof(TclQuotaCallback));
  memset(p, 0, sizeof(TclQuotaCallback));

  /* Invoke sqlite3_quota_set() */
  rc = sqlite3_quota_set(zPattern, iLimit, tclQuotaCallback, (void *)p);
  if( rc!=SQLITE_OK ){
    ckfree((char *)p);
  }else{
    Tcl_IncrRefCount(pScript);
    p->interp = interp;
    p->pScript = pScript;
    p->pNext = pQuotaCallbackList;
    pQuotaCallbackList = p;
  }

  Tcl_SetResult(interp, (char *)sqlite3TestErrorName(rc), TCL_STATIC);
  return TCL_OK;
}

/*
** This routine registers the custom TCL commands defined in this
** module.  This should be the only procedure visible from outside
** of this module.
*/
int Sqlitequota_Init(Tcl_Interp *interp){
  static struct {
     char *zName;
     Tcl_ObjCmdProc *xProc;
  } aCmd[] = {
    { "sqlite3_quota_initialize", test_quota_initialize },
    { "sqlite3_quota_shutdown", test_quota_shutdown },
    { "sqlite3_quota_set", test_quota_set },
  };
  int i;

  for(i=0; i<sizeof(aCmd)/sizeof(aCmd[0]); i++){
    Tcl_CreateObjCommand(interp, aCmd[i].zName, aCmd[i].xProc, 0, 0);
  }

  return TCL_OK;
}
#endif

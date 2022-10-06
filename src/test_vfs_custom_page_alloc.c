/* libSQL extension
******************************************************************************
**
** This file contains an implementation of a VFS shim which overwrites
** the page allocation/deallocation mechanisms.
**
** USAGE:
**
** This source file exports a single symbol which is the name of a
** function:
**
**   int vfsCustomPageAlloc_register(
**     const char *zTraceName,                                         // Name of the newly constructed VFS
**     const char *zOldVfsName,                                        // Name of the underlying VFS
**     int (*xAllocatePage)(sqlite3_file*, unsigned *, unsigned char), // Custom page allocator
**     int (*xFreePage)(sqlite3_file* fd, unsigned pgno),              // Custom page deallocator
**     int makeDefault                                                 // Make the new VFS the default
**   );
**
** It can be later used to test whether the custom page allocation/deallocation
** works properly.
**
*/

#include <stdlib.h>
#include <string.h>
#include "sqlite3.h"

typedef struct vfs_custom_page_alloc_file {
  sqlite3_file base;        /* Base class.  Must be first */
  sqlite3_vfs *pRootVfs;    /* The real underlying filesystem */
  sqlite3_file *pReal;      /* The real underlying file */
} vfs_custom_page_alloc_file;

/*
** Return SQLITE_OK on success.  
**
** SQLITE_NOMEM is returned in the case of a memory allocation error.
** SQLITE_NOTFOUND is returned if zOldVfsName does not exist.
*/

int vfsCustomPageAlloc_register(
   const char *zName,                                              /* Name of the newly constructed VFS */
   const char *zOldVfsName,                                        /* Name of the underlying VFS */
   int (*xAllocatePage)(sqlite3_file*, unsigned *, unsigned char), /* Custom page allocator */
   int (*xFreePage)(sqlite3_file* fd, unsigned pgno),              /* Custom page deallocator */
   int makeDefault                                                 /* True to make the new VFS the default */
){
  sqlite3_vfs *pNew;
  sqlite3_vfs *pRoot;
  int nName;
  int nByte;

  pRoot = sqlite3_vfs_find(zOldVfsName);
  if( pRoot==0 ) return SQLITE_NOTFOUND;
  nName = strlen(zName);
  nByte = sizeof(*pNew) + sizeof(pRoot) + nName + 1;
  pNew = sqlite3_malloc( nByte );
  if( pNew==0 ) return SQLITE_NOMEM;
  memset(pNew, 0, nByte);
  pNew->zName = (char*)&pNew[1];
  memcpy((char*)&pNew[1], zName, nName+1);
  pNew->iVersion = pRoot->iVersion;
  pNew->szOsFile = pRoot->szOsFile;
  pNew->mxPathname = pRoot->mxPathname;
  pNew->zName = zName;
  pNew->pAppData = pRoot->pAppData;
  pNew->xOpen = pRoot->xOpen;
  pNew->xDelete = pRoot->xDelete;
  pNew->xAccess = pRoot->xAccess;
  pNew->xFullPathname = pRoot->xFullPathname;
  pNew->xDlOpen = pRoot->xDlOpen;
  pNew->xDlError = pRoot->xDlError;
  pNew->xDlSym = pRoot->xDlSym;
  pNew->xDlClose = pRoot->xDlClose;
  pNew->xRandomness = pRoot->xRandomness;
  pNew->xSleep = pRoot->xSleep;
  pNew->xCurrentTime = pRoot->xCurrentTime;
  pNew->xGetLastError = pRoot->xGetLastError;
  if( pNew->iVersion>=2 ){
    pNew->xCurrentTimeInt64 = pRoot->xCurrentTimeInt64;
    if( pNew->iVersion>=3 ){
      pNew->xSetSystemCall = pRoot->xSetSystemCall;
      pNew->xGetSystemCall = pRoot->xGetSystemCall;
      pNew->xNextSystemCall = pRoot->xNextSystemCall;
    }
  }
  pNew->xAllocatePage = xAllocatePage;
  pNew->xFreePage = xFreePage;

  return sqlite3_vfs_register(pNew, makeDefault);
}

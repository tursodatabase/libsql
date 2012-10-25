/*
** 2012 October 23
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
** This file contains code implements a VFS shim that attempts to simulate
** a NAND-flash SSD in order to estimate the Write Amplification Factor
** (WAF) for a typical SQLite workload.
**
** This simulator is single-threaded, for simplicity.
**
** USAGE:
**
** This source file exports a single symbol which is the name of a
** function:
**
**   int ssdsim_register(
**     const char *zBaseVfsName,     // Name of the underlying real VFS
**     int makeDefault               // Make the new VFS the default
**   );
*/
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <assert.h>
#include "sqlite3.h"

/* Forward declaration of objects */
typedef struct ssdsim_state ssdsim_state;
typedef struct ssdsim_inode ssdsim_inode;
typedef struct ssdsim_file ssdsim_file;

/*
** Each file on disk
*/
struct ssdsim_inode {
  ssdsim_inode *pNext;      /* Next inode in a list of them all */
  char *zPath;              /* Full pathname of the file */
  sqlite3_int64 len;        /* Size of the file in bytes */
  int *aiPage;               /* Array of logical page numbers */
  ssdsim_file *pFiles;      /* List of open file descriptors */
  int inodeFlags;           /* SSDSIM_* flags */
  int nShmRegion;           /* Number of allocated shared memory regions */
  int szShmRegion;          /* Size of each shared-memory region */
  char **apShm;             /* Shared memory regions */
};

#define SSDSIM_DELETEONCLOSE   0x0001

/*
** Each open file
*/
struct ssdsim_file {
  sqlite3_file base;            /* Base class.  Must be first */
  ssdsim_file *pNext;           /* Next opening of the same inode */
  ssdsim_inode *pInode;         /* The file */
  signed char eLock;            /* Lock state for this connection */
  unsigned char shmOpen;        /* True if SHM is open */
  unsigned short shmReadLock;   /* Shared locks held by the shared memory */
  unsigned short shmWriteLock;  /* Exclusive locks held by the shared memory */
  int openFlags;                /* xOpen() flags used to open this connection */
};

/*
** Page status values
*/
#define SSDSIM_FREE      0
#define SSDSIM_WRITTEN   1
#define SSDSIM_OBSOLETE  2

/*
** Global state of the SSD simulator
*/
struct ssdsim_state {
  int szPage;               /* Size of each page in bytes */
  int szEBlock;             /* Size of an erase block in bytes */
  sqlite3_int64 szDisk;     /* Total disk space in bytes */
  int nPage;                /* Number of slots allocated in apPage[] */
  int nEBlock;              /* Nubmer of erase blocks */
  int nDealloc;             /* Number of reusable logical page numbers */
  int mxAlloc;              /* Maximum allocated logical page number */
  unsigned char **apPage;   /* Memory to hold physical pages */
  int *aDealloc;            /* Array of reuseable logical page numbers */
  int *pageMap;             /* Mapping from logical to physical pages */
  unsigned char *eStat;     /* Status of each page */
  unsigned int *nErase;     /* Number of erasures for each erase block */
  ssdsim_inode *pInode;     /* List of all inodes */
  int traceFlag;            /* True to trace operation */
  int nHostWrite;           /* Number of pages written by the application */
  int nNANDWrite;           /* Number of pages written to NAND-flash */
  sqlite3_vfs *pBase;       /* True underlying VFS */
};
static ssdsim_state g;

/*
** Method declarations for ssdsim_file.
*/
static int ssdsimClose(sqlite3_file*);
static int ssdsimRead(sqlite3_file*, void*, int iAmt, sqlite3_int64 iOfst);
static int ssdsimWrite(sqlite3_file*,const void*,int iAmt, sqlite3_int64);
static int ssdsimTruncate(sqlite3_file*, sqlite3_int64 size);
static int ssdsimSync(sqlite3_file*, int flags);
static int ssdsimFileSize(sqlite3_file*, sqlite3_int64 *pSize);
static int ssdsimLock(sqlite3_file*, int);
static int ssdsimUnlock(sqlite3_file*, int);
static int ssdsimCheckReservedLock(sqlite3_file*, int *);
static int ssdsimFileControl(sqlite3_file*, int op, void *pArg);
static int ssdsimSectorSize(sqlite3_file*);
static int ssdsimDeviceCharacteristics(sqlite3_file*);
static int ssdsimShmLock(sqlite3_file*,int,int,int);
static int ssdsimShmMap(sqlite3_file*,int,int,int, void volatile **);
static void ssdsimShmBarrier(sqlite3_file*);
static int ssdsimShmUnmap(sqlite3_file*,int);

/*
** Method declarations for ssdsim_vfs.
*/
static int ssdsimOpen(sqlite3_vfs*, const char *, sqlite3_file*, int , int *);
static int ssdsimDelete(sqlite3_vfs*, const char *zName, int syncDir);
static int ssdsimAccess(sqlite3_vfs*, const char *zName, int flags, int *);
static int ssdsimFullPathname(sqlite3_vfs*, const char *zName, int, char *);
static void *ssdsimDlOpen(sqlite3_vfs*, const char *zFilename);
static void ssdsimDlError(sqlite3_vfs*, int nByte, char *zErrMsg);
static void (*ssdsimDlSym(sqlite3_vfs*,void*, const char *zSymbol))(void);
static void ssdsimDlClose(sqlite3_vfs*, void*);
static int ssdsimRandomness(sqlite3_vfs*, int nByte, char *zOut);
static int ssdsimSleep(sqlite3_vfs*, int microseconds);
static int ssdsimCurrentTime(sqlite3_vfs*, double*);
static int ssdsimGetLastError(sqlite3_vfs*, int, char*);
static int ssdsimCurrentTimeInt64(sqlite3_vfs*, sqlite3_int64*);
static int ssdsimSetSystemCall(sqlite3_vfs*,const char*, sqlite3_syscall_ptr);
static sqlite3_syscall_ptr ssdsimGetSystemCall(sqlite3_vfs*, const char *);
static const char *ssdsimNextSystemCall(sqlite3_vfs*, const char *zName);

/*
** Trace operation.
*/
static void ssdsimTrace(const char *zFormat, ...){
  if( g.traceFlag ){
    va_list ap;
    char *zMsg;
    va_start(ap, zFormat);
    vprintf(zFormat, ap);
    va_end(ap);
  }
}

/*
** Clear all memory associated with the ssd simulator
*/
static void ssdsimShutdown(void){
  int i;
  for(i=0; i<g.nPage; i++) sqlite3_free(g.apPage[i]);
  sqlite3_free(g.apPage);
  g.apPage = 0;
  sqlite3_free(g.aDealloc);
  g.aDealloc = 0;
  g.nDealloc = 0;
  g.mxAlloc = 0;
  g.nPage = 0;
}

/*
** Initialize the ssdsim system.
*/
static void ssdsimInit(void){
  int nPage;
  if( g.nPage ) return;
  if( g.szPage==0 ) g.szPage = 4096;
  if( g.szEBlock==0 ) g.szEBlock = 262144;
  if( g.szDisk==0 ) g.szDisk = 67108864;
  g.nPage = g.szDisk/g.szPage;
  g.nEBlock = g.szDisk/g.szEBlock;
  sqlite3_free(g.apPage);
  g.apPage = sqlite3_malloc( sizeof(g.apPage[0])*g.nPage );
  if( g.apPage==0 ){ ssdsimShutdown(); return; }
  memset(g.apPage, 0, sizeof(g.apPage[0])*nPage);
  g.aDealloc = sqlite3_malloc( sizeof(g.aDealloc[0])*g.nPage );
  if( g.aDealloc==0 ){ ssdsimShutdown(); return; }
  g.nDealloc = 0;
  g.mxAlloc = 0;
  g.nHostWrite = 0;
  g.nNANDWrite = 0;
}

/*
** Allocate a new, unused logical page number
*/
static int ssdsimCoreLpnAlloc(void){
  if( g.nDealloc ){
    return g.aDealloc[--g.nDealloc];
  }else if( g.mxAlloc>=g.nPage ){
    return -1;
  }else{
    return g.mxAlloc++;
  }
}

/*
** Indicate that the content of a logical page will never again be
** read.
*/
static void ssdsimCoreTrim(int lpn){
}

/*
** Deallocate a logical page number, indicating that it is no longer
** in use.
*/
static int ssdsimCoreLpnDealloc(int lpn){
  g.aDealloc[g.nDealloc++] = lpn;
}

/*
** Translate a logical page number into a physical page number.
*/
static int ssdsimCoreLpnToPpn(int lpn, int writeFlag){
  int ppn = lpn;
  if( g.apPage[ppn]==0 ){
    if( writeFlag ){
      g.apPage[ppn] = sqlite3_malloc( g.szPage );
    }
    if( g.apPage[ppn]==0 ) ppn = -1;
  }
  return ppn;
}

/*
** Indicate that a transaction boundary has occurred
*/
static int ssdsimCoreSync(void){
}


/*
** Truncate an inode
*/
static void ssdsimTruncateInode(ssdsim_inode *pInode, sqlite3_int64 size){
  if( pInode->len > size ){
    int nOld = pInode->len/g.szPage;
    int nNew = size/g.szPage;
    int i;
    for(i=nOld; i>nNew; i--){
      ssdsimCoreLpnDealloc(pInode->aiPage[i]);
    }
    pInode->len = size;
  }
}

/*
** Delete an inode
*/
static void ssdsimDeleteInode(ssdsim_inode *pInode){
  if( pInode->pFiles ){
    pInode->inodeFlags |= SSDSIM_DELETEONCLOSE;
    return;
  }
  ssdsimTruncateInode(pInode, 0);
  sqlite3_free(pInode->apShm);
  sqlite3_free(pInode->aiPage);
  if( g.pInode==pInode ){
    g.pInode = pInode->pNext;
  }else{
    ssdsim_inode *pX;
    for(pX=g.pInode; pX && pX->pNext!=pInode; pX=pX->pNext){}
    if( pX ) pX->pNext = pInode->pNext;
  }
  sqlite3_free(pInode);
}


/*
** Close an ssdsim-file.
*/
static int ssdsimClose(sqlite3_file *pFile){
  ssdsim_file *p = (ssdsim_file *)pFile;
  int rc;
  ssdsim_inode *pInode = p->pInode;
  if( p==pInode->pFiles ){
    pInode->pFiles = p->pNext;
    if( (pInode->inodeFlags & SSDSIM_DELETEONCLOSE)!=0 ){
      ssdsimDeleteInode(pInode);
    }
  }else{
    ssdsim_file *pX;
    for(pX = pInode->pFiles; pX && pX->pNext!=p; pX=pX->pNext){}
    if( pX ) pX->pNext = p->pNext;
  }
  memset(p, 0, sizeof(*p));
  return SQLITE_OK;
}

/*
** Read data from an ssdsim-file.
*/
static int ssdsimRead(
  sqlite3_file *pFile, 
  void *zBuf, 
  int iAmt, 
  sqlite_int64 iOfst
){
  ssdsim_file *p = (ssdsim_file *)pFile;
  ssdsim_inode *pInode = p->pInode;
  int rc = SQLITE_OK;
  int lpn, ppn, n;
  unsigned char *pOut = (unsigned char*)zBuf;
  unsigned char *pContent;
  while( iAmt>0 ){
    if( iAmt+iOfst>pInode->len ){
      rc = SQLITE_IOERR_SHORT_READ;
      iAmt = pInode->len - iOfst;
      if( iAmt<=0 ) break;
    }
    lpn = pInode->aiPage[iOfst/g.szPage];
    ppn = ssdsimCoreLpnToPpn(lpn, 0);
    n = iAmt;
    if( (iOfst+n-1)*g.szPage > lpn ){
      n = (lpn+1)*g.szPage - iOfst;
    }
    if( ppn>=0 && ppn<g.nPage && (pContent = g.apPage[ppn])!=0 ){
      memcpy(pOut, &pContent[iOfst%g.szPage], n);
    }else{
      memset(pOut, 0, n);
    }
    iOfst += n;
    iAmt -= n;
    pOut += n;
  }
  return rc;
}

/*
** Write data to an ssdsim-file.
*/
static int ssdsimWrite(
  sqlite3_file *pFile, 
  const void *zBuf, 
  int iAmt, 
  sqlite_int64 iOfst
){
  ssdsim_file *p = (ssdsim_file *)pFile;
  ssdsim_inode *pInode = p->pInode;
  int rc = SQLITE_OK;
  int pn, lpn, ppn;
  sqlite3_int64 lenNew;
  const unsigned char *pIn = (const unsigned char*)zBuf;
  unsigned char *pDest;

  lenNew = iOfst+iAmt;
  if( lenNew <= pInode->len ){
    lenNew = pInode->len;
  }else{
    int nOld, nNew;
    int *aiPage;
    nOld = pInode->len/g.szPage;
    nNew = (iOfst+iAmt)/g.szPage;
    if( nOld<nNew ){
      aiPage = sqlite3_realloc(pInode->aiPage, nNew*sizeof(int));
      if( aiPage==0 ) return SQLITE_NOMEM;
      memset(aiPage+nOld, 0xff, sizeof(int)*(nNew - nOld));
      pInode->aiPage = aiPage;
    }
  }
  while( iAmt>0 ){
    int n;
    lpn = pInode->aiPage[iOfst/g.szPage];
    if( lpn<0 ){
      lpn = ssdsimCoreLpnAlloc();
      if( lpn<0 ) return SQLITE_FULL;
      pInode->aiPage[iOfst/g.szPage];
    }
    ppn = ssdsimCoreLpnToPpn(lpn, 1);
    if( ppn<0 ) return SQLITE_NOMEM;
    n = iAmt;
    if( (iOfst+n-1)*g.szPage > lpn ){
      n = (lpn+1)*g.szPage - iOfst;
    }
    pDest = g.apPage[ppn];
    memcpy(pDest, pIn, n);
    iOfst += n;
    iAmt -= n;
    pIn += n;
  }
  pInode->len = lenNew;
  return rc;
}

/*
** Truncate an ssdsim-file.
*/
static int ssdsimTruncate(sqlite3_file *pFile, sqlite_int64 size){
  ssdsim_file *p = (ssdsim_file *)pFile;
  ssdsim_inode *pInode = p->pInode;
  ssdsimTruncateInode(pInode, size);
  return SQLITE_OK;
}

/*
** Sync an ssdsim-file.
*/
static int ssdsimSync(sqlite3_file *pFile, int flags){
  ssdsim_file *p = (ssdsim_file *)pFile;
  ssdsim_inode *pInode = p->pInode;
  ssdsimCoreSync();
  return SQLITE_OK;
}

/*
** Return the current file-size of an ssdsim-file.
*/
static int ssdsimFileSize(sqlite3_file *pFile, sqlite_int64 *pSize){
  ssdsim_file *p = (ssdsim_file *)pFile;
  ssdsim_inode *pInode = p->pInode;
  *pSize = pInode->len;
  return SQLITE_OK;
}

/*
** Return the name of a lock.
*/
static const char *lockName(int eLock){
  const char *azLockNames[] = {
     "NONE", "SHARED", "RESERVED", "PENDING", "EXCLUSIVE"
  };
  if( eLock<0 || eLock>=sizeof(azLockNames)/sizeof(azLockNames[0]) ){
    return "???";
  }else{
    return azLockNames[eLock];
  }
}

/*
** Lock an ssdsim-file.
*/
static int ssdsimLock(sqlite3_file *pFile, int eLock){
  ssdsim_file *p = (ssdsim_file *)pFile;
  ssdsim_inode *pInode = p->pInode;
  ssdsim_file *pF;
  int rc = SQLITE_OK;
  if( eLock==SQLITE_LOCK_SHARED ){
    for(pF=pInode->pFiles; pF; pF=pF->pNext){
      if( pF!=p && pF->eLock>=SQLITE_LOCK_PENDING ) return SQLITE_BUSY;
    }
  }else if( eLock>=SQLITE_LOCK_RESERVED ){
    for(pF=pInode->pFiles; pF; pF=pF->pNext){
      if( pF!=p && pF->eLock>=SQLITE_LOCK_RESERVED ) return SQLITE_BUSY;
    }
  }else if( eLock==SQLITE_LOCK_EXCLUSIVE ){
    for(pF=pInode->pFiles; pF; pF=pF->pNext){
      if( pF!=p && pF->eLock>=SQLITE_LOCK_SHARED ){
        eLock = SQLITE_LOCK_PENDING;
        rc = SQLITE_BUSY;
      }
    }
  }
  p->eLock = eLock;
  return rc;
}

/*
** Unlock an ssdsim-file.
*/
static int ssdsimUnlock(sqlite3_file *pFile, int eLock){
  ssdsim_file *p = (ssdsim_file *)pFile;
  if( p->eLock>eLock ) p->eLock = eLock;
  return SQLITE_OK;
}

/*
** Check if another file-handle holds a RESERVED lock on an ssdsim-file.
*/
static int ssdsimCheckReservedLock(sqlite3_file *pFile, int *pResOut){
  ssdsim_file *p = (ssdsim_file *)pFile;
  ssdsim_inode *pInode = p->pInode;
  ssdsim_file *pF;
  int rc = 0;
  for(pF=pInode->pFiles; pF; pF=pF->pNext){
    if( pF!=p && pF->eLock>=SQLITE_LOCK_RESERVED ){
      rc = 1;
      break;
    }
  }
  *pResOut = rc;
  return SQLITE_OK;
}

/*
** File control method. For custom operations on an ssdsim-file.
*/
static int ssdsimFileControl(sqlite3_file *pFile, int op, void *pArg){
  ssdsim_file *p = (ssdsim_file *)pFile;
  ssdsim_inode *pInode = p->pInode;
  switch( op ){
    case SQLITE_FCNTL_LOCKSTATE: {
      *(int*)pArg = p->eLock;
      return SQLITE_OK;
    }
    case SQLITE_FCNTL_VFSNAME: {
      *(char**)pArg = sqlite3_mprintf("ssdsim");
      return SQLITE_OK;
    }
    case SQLITE_FCNTL_PRAGMA: {
#if 0
      const char *const* a = (const char*const*)pArg;
      sqlite3_snprintf(sizeof(zBuf), zBuf, "PRAGMA,[%s,%s]",a[1],a[2]);
      zOp = zBuf;
#endif
      break;
    }
    default: {
      break;
    }
  }
  return SQLITE_NOTFOUND;
}

/*
** Return the sector-size in bytes for an ssdsim-file.
*/
static int ssdsimSectorSize(sqlite3_file *pFile){
  return g.szPage;
}

/*
** Return the device characteristic flags supported by an ssdsim-file.
*/
static int ssdsimDeviceCharacteristics(sqlite3_file *pFile){
  return 
     SQLITE_IOCAP_ATOMIC |
     SQLITE_IOCAP_POWERSAFE_OVERWRITE |
     SQLITE_IOCAP_SAFE_APPEND |
     SQLITE_IOCAP_SEQUENTIAL |
     SQLITE_IOCAP_UNDELETABLE_WHEN_OPEN |
     0;
}

/*
** Shared-memory operations.
*/
static int ssdsimShmLock(sqlite3_file *pFile, int ofst, int n, int flags){
  ssdsim_file *p = (ssdsim_file *)pFile;
  ssdsim_inode *pInode = p->pInode;
  ssdsim_file *pF;
  unsigned int lockMask = 0;

  /* Constraints on the SQLite core: */
  assert( ofst>=0 && ofst+n<=SQLITE_SHM_NLOCK );
  assert( n>=1 );
  assert( flags==(SQLITE_SHM_LOCK | SQLITE_SHM_SHARED)
       || flags==(SQLITE_SHM_LOCK | SQLITE_SHM_EXCLUSIVE)
       || flags==(SQLITE_SHM_UNLOCK | SQLITE_SHM_SHARED)
       || flags==(SQLITE_SHM_UNLOCK | SQLITE_SHM_EXCLUSIVE) );
  assert( n==1 || (flags & SQLITE_SHM_EXCLUSIVE)!=0 );

  /* Mask of bits involved in this lock */
  lockMask = (1<<(ofst+n)) - (1<<ofst);

  /* The unlock case */
  if( flags & SQLITE_SHM_UNLOCK ){
    p->shmWriteLock &= ~lockMask;
    p->shmReadLock &= ~lockMask;
    return SQLITE_OK;
  }

  /* The shared-lock case */
  if( flags & SQLITE_SHM_SHARED ){
    /* Disallow if any sibling (including ourself) holds an exclusive lock */
    for(pF=pInode->pFiles; pF; pF=pF->pNext){
      if( pF->shmWriteLock & lockMask ){
        return SQLITE_BUSY;
      }
    }
    p->shmReadLock |= lockMask;
    return SQLITE_OK;
  }

  /* The rest of this procedure is the exclusive lock case */
  assert( flags & SQLITE_SHM_EXCLUSIVE );

  /* Disallow an exclusive if any kind of lock is held by any other */
  for(pF=pInode->pFiles; pF; pF=pF->pNext){
    if( pF==p ) continue;
    if( (pF->shmWriteLock & lockMask)!=0 ){
      return SQLITE_BUSY;
    }
    if( (pF->shmReadLock & lockMask)!=0 ){
      return SQLITE_BUSY;
    }
  }
  p->shmWriteLock |= lockMask;
  return SQLITE_OK;
}
static int ssdsimShmMap(
  sqlite3_file *pFile, 
  int iRegion, 
  int szRegion, 
  int isWrite, 
  void volatile **pp
){
  ssdsim_file *p = (ssdsim_file *)pFile;
  ssdsim_inode *pInode = p->pInode;
  char **apShm;
  int i;
  if( p->shmOpen==0 ){
    p->shmOpen = 1;
    p->shmReadLock = 0;
    p->shmWriteLock = 0;
  }
  if( pInode->nShmRegion<=iRegion ){
    if( isWrite==0 ){
      *pp = 0;
      return SQLITE_OK;
    }
    apShm = sqlite3_realloc(pInode->apShm, 
                            (iRegion+1)*sizeof(pInode->apShm[0]));
    if( apShm==0 ) return SQLITE_NOMEM;
    pInode->apShm = apShm;
    for(i=pInode->nShmRegion; i<=iRegion; i++){
      apShm[i] = sqlite3_malloc(szRegion);
      if( apShm[i]==0 ) return SQLITE_NOMEM;
      memset(apShm[i], 0, szRegion);
      pInode->nShmRegion = i+1;
    }
    pInode->szShmRegion = szRegion;
  }
  *pp = pInode->apShm[iRegion];
  return SQLITE_OK;
}
static void ssdsimShmBarrier(sqlite3_file *pFile){
  /* noop */
}
static int ssdsimShmUnmap(sqlite3_file *pFile, int delFlag){
  ssdsim_file *p = (ssdsim_file *)pFile;
  ssdsim_inode *pInode = p->pInode;
  if( p->shmOpen ){
    ssdsim_file *pF;
    unsigned char shmOpen = 0;
    p->shmOpen = 0;
    for(pF=pInode->pFiles; pF; pF=pF->pNext) shmOpen |= pF->shmOpen;
    if( !shmOpen ){
      int i;
      for(i=0; i<pInode->nShmRegion; i++) sqlite3_free(pInode->apShm[i]);
      sqlite3_free(pInode->apShm);
      pInode->apShm = 0;
      pInode->nShmRegion = 0;
    }
  }
  return SQLITE_OK;
}

static const sqlite3_io_methods ssdsim_io_methods = {
  /* iVersion               */ 2,
  /* xClose                 */ ssdsimClose,
  /* xRead                  */ ssdsimRead,
  /* xWrite                 */ ssdsimWrite,
  /* xTruncate              */ ssdsimTruncate,
  /* xSync                  */ ssdsimSync,
  /* xFileSize              */ ssdsimFileSize,
  /* xLock                  */ ssdsimLock,
  /* xUnlock                */ ssdsimUnlock,
  /* xCheckReservedLock     */ ssdsimCheckReservedLock,
  /* xFileControl           */ ssdsimFileControl,
  /* xSectorSize            */ ssdsimSectorSize,
  /* xDeviceCharacteristics */ ssdsimDeviceCharacteristics,
  /* xShmMap                */ ssdsimShmMap,
  /* xShmLock               */ ssdsimShmLock,
  /* xShmBarrier            */ ssdsimShmBarrier,
  /* xShmUnmap              */ ssdsimShmUnmap
};

/*
** Find an inode given its name.
*/
static ssdsim_inode *ssdsimFindInode(const char *zName){
  ssdsim_inode *pInode;
  for(pInode=g.pInode; pInode; pInode=pInode->pNext){
    if( strcmp(pInode->zPath, zName)==0 ) break;
  }
  return pInode;
}

/*
** Open an ssdsim file handle.
*/
static int ssdsimOpen(
  sqlite3_vfs *pVfs,
  const char *zName,
  sqlite3_file *pFile,
  int flags,
  int *pOutFlags
){
  int rc;
  ssdsim_file *p = (ssdsim_file *)pFile;
  ssdsim_inode *pInode = ssdsimFindInode(zName);
  if( pInode==0 ){
    int n = (int)strlen(zName);
    pInode = sqlite3_malloc( sizeof(*pInode) + n + 1 );
    if( pInode==0 ) return SQLITE_NOMEM;
    memset(pInode, 0, sizeof(*pInode));
    pInode->pNext = g.pInode;
    g.pInode = pInode;
    pInode->zPath = (char*)&pInode[1];
    strcpy(pInode->zPath, zName);
    pInode->len = 0;
    pInode->aiPage = 0;
    pInode->pFiles = 0;
    pInode->inodeFlags = 0;
    pInode->nShmRegion = 0;
    pInode->szShmRegion = 0;
    pInode->apShm = 0;
    if( flags & SQLITE_OPEN_DELETEONCLOSE ){
      pInode->inodeFlags |= SSDSIM_DELETEONCLOSE;
    }
  }
  p->pInode = pInode;
  p->pNext = pInode->pFiles;
  pInode->pFiles = p;
  p->eLock = 0;
  p->shmOpen = 0;
  p->shmReadLock = 0;
  p->shmWriteLock = 0;
  p->openFlags = flags;
  p->base.pMethods = &ssdsim_io_methods;
  return SQLITE_OK;
}

/*
** Delete the file located at zPath. If the dirSync argument is true,
** ensure the file-system modifications are synced to disk before
** returning.
*/
static int ssdsimDelete(sqlite3_vfs *pVfs, const char *zPath, int dirSync){
  ssdsim_inode *pInode = ssdsimFindInode(zPath);
  if( pInode==0 ) return SQLITE_NOTFOUND;
  ssdsimDeleteInode(pInode);
  return SQLITE_OK;
}

/*
** Test for access permissions. Return true if the requested permission
** is available, or false otherwise.
*/
static int ssdsimAccess(
  sqlite3_vfs *pVfs, 
  const char *zPath, 
  int flags, 
  int *pResOut
){
  ssdsim_inode *pInode = ssdsimFindInode(zPath);
  *pResOut = pInode!=0;
  return SQLITE_OK;
}

/*
** Populate buffer zOut with the full canonical pathname corresponding
** to the pathname in zPath. zOut is guaranteed to point to a buffer
** of at least (DEVSYM_MAX_PATHNAME+1) bytes.
*/
static int ssdsimFullPathname(
  sqlite3_vfs *pVfs, 
  const char *zPath, 
  int nOut, 
  char *zOut
){
  while( zPath[0]=='/' ) zPath++;
  sqlite3_snprintf(nOut, zOut, "/%s", zPath);
  return SQLITE_OK;
}

/*
** Open the dynamic library located at zPath and return a handle.
*/
static void *ssdsimDlOpen(sqlite3_vfs *pVfs, const char *zPath){
  return 0;
}

/*
** Populate the buffer zErrMsg (size nByte bytes) with a human readable
** utf-8 string describing the most recent error encountered associated 
** with dynamic libraries.
*/
static void ssdsimDlError(sqlite3_vfs *pVfs, int nByte, char *zErrMsg){
  sqlite3_snprintf(nByte, zErrMsg, "not supported by this VFS");
}

/*
** Return a pointer to the symbol zSymbol in the dynamic library pHandle.
*/
static void (*ssdsimDlSym(sqlite3_vfs *pVfs,void *p,const char *zSym))(void){
  return 0;
}

/*
** Close the dynamic library handle pHandle.
*/
static void ssdsimDlClose(sqlite3_vfs *pVfs, void *pHandle){
}

/*
** Populate the buffer pointed to by zBufOut with nByte bytes of 
** random data.
*/
static int ssdsimRandomness(sqlite3_vfs *pVfs, int nByte, char *zBufOut){
  return g.pBase->xRandomness(g.pBase, nByte, zBufOut);
}

/*
** Sleep for nMicro microseconds. Return the number of microseconds 
** actually slept.
*/
static int ssdsimSleep(sqlite3_vfs *pVfs, int nMicro){
  return g.pBase->xSleep(g.pBase, nMicro);
}

/*
** Return the current time as a Julian Day number in *pTimeOut.
*/
static int ssdsimCurrentTime(sqlite3_vfs *pVfs, double *pTimeOut){
  return g.pBase->xCurrentTime(g.pBase, pTimeOut);
}
static int ssdsimCurrentTimeInt64(sqlite3_vfs *pVfs, sqlite3_int64 *pTimeOut){
  return g.pBase->xCurrentTimeInt64(g.pBase, pTimeOut);
}

/*
** Return th3 emost recent error code and message
*/
static int ssdsimGetLastError(sqlite3_vfs *pVfs, int iErr, char *zErr){
  return SQLITE_OK;
}

/*
** Override system calls.
*/
static int ssdsimSetSystemCall(
  sqlite3_vfs *pVfs,
  const char *zName,
  sqlite3_syscall_ptr pFunc
){
  return SQLITE_NOTFOUND;
}
static sqlite3_syscall_ptr ssdsimGetSystemCall(
  sqlite3_vfs *pVfs,
  const char *zName
){
  return 0;
}
static const char *ssdsimNextSystemCall(sqlite3_vfs *pVfs, const char *zName){
  return 0;
}

static sqlite3_vfs ssdsim_vfs = {
  /* iVersion          */ 3,
  /* szOsFile          */ sizeof(ssdsim_file),
  /* mxPathname        */ 1024,
  /* pNext             */ 0,
  /* zName             */ "ssdsim",
  /* pAppData          */ 0,
  /* xOpen             */ ssdsimOpen,
  /* xDelete           */ ssdsimDelete,
  /* xAccess           */ ssdsimAccess,
  /* xFullPathname     */ ssdsimFullPathname,
  /* xDlOpen           */ ssdsimDlOpen,
  /* xDlError          */ ssdsimDlError,
  /* xDlSym            */ ssdsimDlSym,
  /* xDlClose          */ ssdsimDlClose,
  /* xRandomness       */ ssdsimRandomness,
  /* xSleep            */ ssdsimSleep,
  /* xCurrentTime      */ ssdsimCurrentTime,
  /* xGetLastError     */ ssdsimGetLastError,
  /* xCurrentTimeInt64 */ ssdsimCurrentTimeInt64,
  /* xSetSystemCall    */ ssdsimSetSystemCall,
  /* xGetSystemCall    */ ssdsimGetSystemCall,
  /* xNextSystemCall   */ ssdsimNextSystemCall
};

/*
** Clients invoke this routine to register the SSD simulator
*/
int ssdsim_register(
   const char *zBaseName,          /* Name of the underlying VFS */
   const char *zParams,            /* Configuration parameter */
   int makeDefault                 /* True to make the new VFS the default */
){
  sqlite3_vfs *pNew;
  sqlite3_vfs *pRoot;

  if( g.pBase ) return SQLITE_ERROR;
  g.pBase = sqlite3_vfs_find(zBaseName);
  if( g.pBase==0 ) return SQLITE_NOTFOUND;
  return sqlite3_vfs_register(&ssdsim_vfs, makeDefault);
}

/*
** Clients invoke this routine to get SSD simulator write-amplification
** statistics.
*/
void ssdsim_report(FILE *pOut, int reportNum){
  fprintf(pOut, "host page writes...... %9d\n", g.nHostWrite);
  fprintf(pOut, "NAND page writes...... %9d\n", g.nNANDWrite);
  if( g.nHostWrite>0 ){
    fprintf(pOut, "write amplification... %11.2f\n",
            (double)g.nNANDWrite/(double)g.nHostWrite);
  }
}

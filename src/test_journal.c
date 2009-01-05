/*
** 2008 Jan 22
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
** This file contains code for a VFS layer that acts as a wrapper around
** an existing VFS. The code in this file attempts to detect a specific
** bug in SQLite - writing data to a database file page when:
**
**   a) the original page data is not stored in a synced portion of the
**      journal file, and
**   b) the page was not a free-list leaf page when the transaction was
**      first opened.
**
** $Id: test_journal.c,v 1.4 2009/01/05 17:15:00 danielk1977 Exp $
*/
#if SQLITE_TEST          /* This file is used for testing only */

#include "sqlite3.h"
#include "sqliteInt.h"

/*
** Maximum pathname length supported by the jt backend.
*/
#define JT_MAX_PATHNAME 512

/*
** Name used to identify this VFS.
*/
#define JT_VFS_NAME "jt"

typedef struct jt_file jt_file;
struct jt_file {
  sqlite3_file base;
  const char *zName;       /* Name of open file */
  int flags;               /* Flags the file was opened with */

  /* The following are only used by database file file handles */
  int eLock;               /* Current lock held on the file */
  u32 nPage;               /* Size of file in pages when transaction started */
  u32 nPagesize;           /* Page size when transaction started */
  Bitvec *pWritable;       /* Bitvec of pages that may be written to the file */

  jt_file *pNext;          /* All files are stored in a linked list */
  sqlite3_file *pReal;     /* The file handle for the underlying vfs */
};

/*
** Method declarations for jt_file.
*/
static int jtClose(sqlite3_file*);
static int jtRead(sqlite3_file*, void*, int iAmt, sqlite3_int64 iOfst);
static int jtWrite(sqlite3_file*,const void*,int iAmt, sqlite3_int64 iOfst);
static int jtTruncate(sqlite3_file*, sqlite3_int64 size);
static int jtSync(sqlite3_file*, int flags);
static int jtFileSize(sqlite3_file*, sqlite3_int64 *pSize);
static int jtLock(sqlite3_file*, int);
static int jtUnlock(sqlite3_file*, int);
static int jtCheckReservedLock(sqlite3_file*, int *);
static int jtFileControl(sqlite3_file*, int op, void *pArg);
static int jtSectorSize(sqlite3_file*);
static int jtDeviceCharacteristics(sqlite3_file*);

/*
** Method declarations for jt_vfs.
*/
static int jtOpen(sqlite3_vfs*, const char *, sqlite3_file*, int , int *);
static int jtDelete(sqlite3_vfs*, const char *zName, int syncDir);
static int jtAccess(sqlite3_vfs*, const char *zName, int flags, int *);
static int jtFullPathname(sqlite3_vfs*, const char *zName, int, char *zOut);
#ifndef SQLITE_OMIT_LOAD_EXTENSION
static void *jtDlOpen(sqlite3_vfs*, const char *zFilename);
static void jtDlError(sqlite3_vfs*, int nByte, char *zErrMsg);
static void (*jtDlSym(sqlite3_vfs*,void*, const char *zSymbol))(void);
static void jtDlClose(sqlite3_vfs*, void*);
#endif /* SQLITE_OMIT_LOAD_EXTENSION */
static int jtRandomness(sqlite3_vfs*, int nByte, char *zOut);
static int jtSleep(sqlite3_vfs*, int microseconds);
static int jtCurrentTime(sqlite3_vfs*, double*);

static sqlite3_vfs jt_vfs = {
  1,                             /* iVersion */
  sizeof(jt_file),               /* szOsFile */
  JT_MAX_PATHNAME,               /* mxPathname */
  0,                             /* pNext */
  JT_VFS_NAME,                   /* zName */
  0,                             /* pAppData */
  jtOpen,                        /* xOpen */
  jtDelete,                      /* xDelete */
  jtAccess,                      /* xAccess */
  jtFullPathname,                /* xFullPathname */
#ifndef SQLITE_OMIT_LOAD_EXTENSION
  jtDlOpen,                      /* xDlOpen */
  jtDlError,                     /* xDlError */
  jtDlSym,                       /* xDlSym */
  jtDlClose,                     /* xDlClose */
#else
  0,                             /* xDlOpen */
  0,                             /* xDlError */
  0,                             /* xDlSym */
  0,                             /* xDlClose */
#endif /* SQLITE_OMIT_LOAD_EXTENSION */
  jtRandomness,                  /* xRandomness */
  jtSleep,                       /* xSleep */
  jtCurrentTime                  /* xCurrentTime */
};

static sqlite3_io_methods jt_io_methods = {
  1,                             /* iVersion */
  jtClose,                       /* xClose */
  jtRead,                        /* xRead */
  jtWrite,                       /* xWrite */
  jtTruncate,                    /* xTruncate */
  jtSync,                        /* xSync */
  jtFileSize,                    /* xFileSize */
  jtLock,                        /* xLock */
  jtUnlock,                      /* xUnlock */
  jtCheckReservedLock,           /* xCheckReservedLock */
  jtFileControl,                 /* xFileControl */
  jtSectorSize,                  /* xSectorSize */
  jtDeviceCharacteristics        /* xDeviceCharacteristics */
};

struct JtGlobal {
  sqlite3_vfs *pVfs;
  jt_file *pList;
};
static struct JtGlobal g = {0, 0};

static void closeTransaction(jt_file *p){
  sqlite3BitvecDestroy(p->pWritable);
  p->pWritable = 0;
}

/*
** Close an jt-file.
*/
static int jtClose(sqlite3_file *pFile){
  jt_file **pp;
  jt_file *p = (jt_file *)pFile;

  if( p->zName ){
    for(pp=&g.pList; *pp!=p; pp=&(*pp)->pNext);
    *pp = p->pNext;
  }

  return sqlite3OsClose(p->pReal);
}

/*
** Read data from an jt-file.
*/
static int jtRead(
  sqlite3_file *pFile, 
  void *zBuf, 
  int iAmt, 
  sqlite_int64 iOfst
){
  jt_file *p = (jt_file *)pFile;
  return sqlite3OsRead(p->pReal, zBuf, iAmt, iOfst);
}


static jt_file *locateDatabaseHandle(const char *zJournal){
  jt_file *pMain;
  for(pMain=g.pList; pMain; pMain=pMain->pNext){
    int nName = strlen(zJournal) - strlen("-journal");
    if( (pMain->flags&SQLITE_OPEN_MAIN_DB)
     && (strlen(pMain->zName)==nName)
     && 0==memcmp(pMain->zName, zJournal, nName)
     && (pMain->eLock>=SQLITE_LOCK_RESERVED)
    ){
      break;
    }
  }
  return pMain;
}


static u32 decodeUint32(const unsigned char *z){
  return (z[0]<<24) + (z[1]<<16) + (z[2]<<8) + z[3];
}

static int readFreelist(jt_file *pMain){
  int rc;
  sqlite3_file *p = pMain->pReal;
  sqlite3_int64 iSize;

  rc = sqlite3OsFileSize(p, &iSize);
  if( rc==SQLITE_OK && iSize>=pMain->nPagesize ){
    unsigned char *zBuf = (unsigned char *)malloc(pMain->nPagesize);
    u32 iTrunk;

    rc = sqlite3OsRead(p, zBuf, pMain->nPagesize, 0);
    iTrunk = decodeUint32(&zBuf[32]);
    while( rc==SQLITE_OK && iTrunk>0 ){
      u32 nLeaf;
      u32 iLeaf;
      sqlite3_int64 iOff = (iTrunk-1)*pMain->nPagesize;
      rc = sqlite3OsRead(p, zBuf, pMain->nPagesize, iOff);
      nLeaf = decodeUint32(&zBuf[4]);
      for(iLeaf=0; rc==SQLITE_OK && iLeaf<nLeaf; iLeaf++){
        u32 pgno = decodeUint32(&zBuf[8+4*iLeaf]);
        sqlite3BitvecSet(pMain->pWritable, pgno);
      }
      iTrunk = decodeUint32(zBuf);
    }

    free(zBuf);
  }

  return rc;
}

/*
** The first argument, zBuf, points to a buffer containing a 28 byte
** serialized journal header. This function deserializes four of the
** integer fields contained in the journal header and writes their
** values to the output variables.
*/
static int decodeJournalHdr(
  const unsigned char *zBuf,         /* Input: 28 byte journal header */
  u32 *pnRec,                        /* Out: Number of journalled records */
  u32 *pnPage,                       /* Out: Original database page count */
  u32 *pnSector,                     /* Out: Sector size in bytes */
  u32 *pnPagesize                    /* Out: Page size in bytes */
){
  unsigned char aMagic[] = { 0xd9, 0xd5, 0x05, 0xf9, 0x20, 0xa1, 0x63, 0xd7 };
  if( memcmp(aMagic, zBuf, 8) ) return 1;
  if( pnRec ) *pnRec = decodeUint32(&zBuf[8]);
  if( pnPage ) *pnPage = decodeUint32(&zBuf[16]);
  if( pnSector ) *pnSector = decodeUint32(&zBuf[20]);
  if( pnPagesize ) *pnPagesize = decodeUint32(&zBuf[24]);
  return 0;
}

/*
** Write data to an jt-file.
*/
static int jtWrite(
  sqlite3_file *pFile, 
  const void *zBuf, 
  int iAmt, 
  sqlite_int64 iOfst
){
  jt_file *p = (jt_file *)pFile;
  if( p->flags&SQLITE_OPEN_MAIN_JOURNAL && iOfst==0 ){
    jt_file *pMain = locateDatabaseHandle(p->zName);
    assert( pMain );

    if( decodeJournalHdr(zBuf, 0, &pMain->nPage, 0, &pMain->nPagesize) ){
      /* Zeroing the first journal-file header. This is the end of a
      ** transaction. */
      closeTransaction(pMain);
    }else{
      /* Writing the first journal header to a journal file. This happens
      ** when a transaction is first started.  */
      int rc;
      pMain->pWritable = sqlite3BitvecCreate(pMain->nPage);
      if( !pMain->pWritable ){
	return SQLITE_IOERR_NOMEM;
      }
      rc = readFreelist(pMain);
      if( rc!=SQLITE_OK ){
        return rc;
      }
    }
  }

  if( p->flags&SQLITE_OPEN_MAIN_DB && p->pWritable && iAmt==p->nPagesize ){
    u32 pgno = iOfst/p->nPagesize + 1;
    assert( pgno>p->nPage || sqlite3BitvecTest(p->pWritable, pgno) );
  }

  return sqlite3OsWrite(p->pReal, zBuf, iAmt, iOfst);
}

/*
** Truncate an jt-file.
*/
static int jtTruncate(sqlite3_file *pFile, sqlite_int64 size){
  jt_file *p = (jt_file *)pFile;
  if( p->flags&SQLITE_OPEN_MAIN_JOURNAL && size==0 ){
    /* Truncating a journal file. This is the end of a transaction. */
    jt_file *pMain = locateDatabaseHandle(p->zName);
    closeTransaction(pMain);
  }
  return sqlite3OsTruncate(p->pReal, size);
}

/*
** The first argument to this function is a handle open on a journal file.
** This function reads the journal file and adds the page number for each
** page in the journal to the Bitvec object passed as the second argument.
*/
static int readJournalFile(jt_file *p, jt_file *pMain){
  int rc;
  unsigned char zBuf[28];
  sqlite3_file *pReal = p->pReal;
  sqlite3_int64 iOff = 0;
  sqlite3_int64 iSize = 0;

  rc = sqlite3OsFileSize(p->pReal, &iSize);
  while( rc==SQLITE_OK && iOff<iSize ){
    u32 nRec, nPage, nSector, nPagesize;
    u32 ii;
    rc = sqlite3OsRead(pReal, zBuf, 28, iOff);
    if( rc!=SQLITE_OK 
     || decodeJournalHdr(zBuf, &nRec, &nPage, &nSector, &nPagesize) 
    ){
      goto finish_rjf;
    }
    iOff += nSector;
    if( nRec==0 ){
      /* A trick. There might be another journal-header immediately 
      ** following this one. In this case, 0 records means 0 records, 
      ** not "read until the end of the file". See also ticket #2565.
      */
      if( iSize>=(nRec+nSector) ){
        rc = sqlite3OsRead(pReal, zBuf, 28, iOff);
        if( rc!=SQLITE_OK || 0==decodeJournalHdr(zBuf, 0, 0, 0, 0) ){
          continue;
        }
      }
      nRec = (iSize - iOff)/(pMain->nPagesize + 8);
    }
    for(ii=0; rc==SQLITE_OK && ii<nRec && iOff<iSize; ii++){
      u32 pgno;
      rc = sqlite3OsRead(pReal, zBuf, 4, iOff);
      if( rc==SQLITE_OK ){
        pgno = decodeUint32(zBuf);
        iOff += (8 + pMain->nPagesize);
        if( pgno>0 && pgno<=pMain->nPage ){
          sqlite3BitvecSet(pMain->pWritable, pgno);
        }
      }
    }

    iOff = ((iOff + (nSector-1)) / nSector) * nSector;
  }

finish_rjf:
  if( rc==SQLITE_IOERR_SHORT_READ ){
    rc = SQLITE_OK;
  }
  return rc;
}

/*
** Sync an jt-file.
*/
static int jtSync(sqlite3_file *pFile, int flags){
  jt_file *p = (jt_file *)pFile;

  if( p->flags&SQLITE_OPEN_MAIN_JOURNAL ){
    int rc;
    jt_file *pMain;                   /* The associated database file */

    /* The journal file is being synced. At this point, we inspect the 
    ** contents of the file up to this point and set each bit in the 
    ** jt_file.pWritable bitvec of the main database file associated with
    ** this journal file.
    */
    pMain = locateDatabaseHandle(p->zName);
    assert(pMain);

    /* Set the bitvec values */
    if( pMain->pWritable ){
      rc = readJournalFile(p, pMain);
      if( rc!=SQLITE_OK ){
        return rc;
      }
    }
  }

  return sqlite3OsSync(p->pReal, flags);
}

/*
** Return the current file-size of an jt-file.
*/
static int jtFileSize(sqlite3_file *pFile, sqlite_int64 *pSize){
  jt_file *p = (jt_file *)pFile;
  return sqlite3OsFileSize(p->pReal, pSize);
}

/*
** Lock an jt-file.
*/
static int jtLock(sqlite3_file *pFile, int eLock){
  int rc;
  jt_file *p = (jt_file *)pFile;
  rc = sqlite3OsLock(p->pReal, eLock);
  if( rc==SQLITE_OK && eLock>p->eLock ){
    p->eLock = eLock;
  }
  return rc;
}

/*
** Unlock an jt-file.
*/
static int jtUnlock(sqlite3_file *pFile, int eLock){
  int rc;
  jt_file *p = (jt_file *)pFile;
  rc = sqlite3OsUnlock(p->pReal, eLock);
  if( rc==SQLITE_OK && eLock<p->eLock ){
    p->eLock = eLock;
  }
  return rc;
}

/*
** Check if another file-handle holds a RESERVED lock on an jt-file.
*/
static int jtCheckReservedLock(sqlite3_file *pFile, int *pResOut){
  jt_file *p = (jt_file *)pFile;
  return sqlite3OsCheckReservedLock(p->pReal, pResOut);
}

/*
** File control method. For custom operations on an jt-file.
*/
static int jtFileControl(sqlite3_file *pFile, int op, void *pArg){
  jt_file *p = (jt_file *)pFile;
  return sqlite3OsFileControl(p->pReal, op, pArg);
}

/*
** Return the sector-size in bytes for an jt-file.
*/
static int jtSectorSize(sqlite3_file *pFile){
  jt_file *p = (jt_file *)pFile;
  return sqlite3OsSectorSize(p->pReal);
}

/*
** Return the device characteristic flags supported by an jt-file.
*/
static int jtDeviceCharacteristics(sqlite3_file *pFile){
  jt_file *p = (jt_file *)pFile;
  return sqlite3OsDeviceCharacteristics(p->pReal);
}

/*
** Open an jt file handle.
*/
static int jtOpen(
  sqlite3_vfs *pVfs,
  const char *zName,
  sqlite3_file *pFile,
  int flags,
  int *pOutFlags
){
  int rc;
  jt_file *p = (jt_file *)pFile;
  p->pReal = (sqlite3_file *)&p[1];
  p->pReal->pMethods = 0;
  rc = sqlite3OsOpen(g.pVfs, zName, p->pReal, flags, pOutFlags);
  assert( rc==SQLITE_OK || p->pReal->pMethods==0 );
  if( rc==SQLITE_OK ){
    pFile->pMethods = &jt_io_methods;
    p->eLock = 0;
    p->zName = zName;
    p->flags = flags;
    p->pNext = 0;
    p->pWritable = 0;
    if( zName ){
      p->pNext = g.pList;
      g.pList = p;
    }
  }
  return rc;
}

/*
** Delete the file located at zPath. If the dirSync argument is true,
** ensure the file-system modifications are synced to disk before
** returning.
*/
static int jtDelete(sqlite3_vfs *pVfs, const char *zPath, int dirSync){
  int nPath = strlen(zPath);
  if( nPath>8 && 0==strcmp("-journal", &zPath[nPath-8]) ){
    /* Deleting a journal file. The end of a transaction. */
    jt_file *pMain = locateDatabaseHandle(zPath);
    if( pMain ){
      closeTransaction(pMain);
    }
  }

  return sqlite3OsDelete(g.pVfs, zPath, dirSync);
}

/*
** Test for access permissions. Return true if the requested permission
** is available, or false otherwise.
*/
static int jtAccess(
  sqlite3_vfs *pVfs, 
  const char *zPath, 
  int flags, 
  int *pResOut
){
  return sqlite3OsAccess(g.pVfs, zPath, flags, pResOut);
}

/*
** Populate buffer zOut with the full canonical pathname corresponding
** to the pathname in zPath. zOut is guaranteed to point to a buffer
** of at least (JT_MAX_PATHNAME+1) bytes.
*/
static int jtFullPathname(
  sqlite3_vfs *pVfs, 
  const char *zPath, 
  int nOut, 
  char *zOut
){
  return sqlite3OsFullPathname(g.pVfs, zPath, nOut, zOut);
}

#ifndef SQLITE_OMIT_LOAD_EXTENSION
/*
** Open the dynamic library located at zPath and return a handle.
*/
static void *jtDlOpen(sqlite3_vfs *pVfs, const char *zPath){
  return sqlite3OsDlOpen(g.pVfs, zPath);
}

/*
** Populate the buffer zErrMsg (size nByte bytes) with a human readable
** utf-8 string describing the most recent error encountered associated 
** with dynamic libraries.
*/
static void jtDlError(sqlite3_vfs *pVfs, int nByte, char *zErrMsg){
  sqlite3OsDlError(g.pVfs, nByte, zErrMsg);
}

/*
** Return a pointer to the symbol zSymbol in the dynamic library pHandle.
*/
static void (*jtDlSym(sqlite3_vfs *pVfs, void *p, const char *zSym))(void){
  return sqlite3OsDlSym(g.pVfs, p, zSym);
}

/*
** Close the dynamic library handle pHandle.
*/
static void jtDlClose(sqlite3_vfs *pVfs, void *pHandle){
  sqlite3OsDlClose(g.pVfs, pHandle);
}
#endif /* SQLITE_OMIT_LOAD_EXTENSION */

/*
** Populate the buffer pointed to by zBufOut with nByte bytes of 
** random data.
*/
static int jtRandomness(sqlite3_vfs *pVfs, int nByte, char *zBufOut){
  return sqlite3OsRandomness(g.pVfs, nByte, zBufOut);
}

/*
** Sleep for nMicro microseconds. Return the number of microseconds 
** actually slept.
*/
static int jtSleep(sqlite3_vfs *pVfs, int nMicro){
  return sqlite3OsSleep(g.pVfs, nMicro);
}

/*
** Return the current time as a Julian Day number in *pTimeOut.
*/
static int jtCurrentTime(sqlite3_vfs *pVfs, double *pTimeOut){
  return sqlite3OsCurrentTime(g.pVfs, pTimeOut);
}

int jt_register(char *zWrap, int isDefault){
  g.pVfs = sqlite3_vfs_find(zWrap);
  if( g.pVfs==0 ){
    return SQLITE_ERROR;
  }
  jt_vfs.szOsFile += g.pVfs->szOsFile;
  sqlite3_vfs_register(&jt_vfs, isDefault);
  return SQLITE_OK;
}

void jt_unregister(){
  sqlite3_vfs_unregister(&jt_vfs);
}

#endif

/*
** 2017-12-26
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
** This file implements a virtual table for reading and writing ZIP archive
** files.
**
** Usage example:
**
**     SELECT name, sz, datetime(mtime,'unixepoch') FROM zipfile($filename);
**
** Current limitations:
**
**    *  No support for encryption
**    *  No support for ZIP archives spanning multiple files
**    *  No support for zip64 extensions
**    *  Only the "inflate/deflate" (zlib) compression method is supported
*/
#include "sqlite3ext.h"
SQLITE_EXTENSION_INIT1
#include <stdio.h>
#include <string.h>
#include <assert.h>

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#if !defined(_WIN32) && !defined(WIN32)
#  include <unistd.h>
#  include <dirent.h>
#  include <utime.h>
#else
#  include <io.h>
#endif
#include <time.h>
#include <errno.h>

#include <zlib.h>

#ifndef SQLITE_OMIT_VIRTUALTABLE

#ifndef SQLITE_AMALGAMATION
typedef sqlite3_int64 i64;
typedef unsigned char u8;
typedef unsigned short u16;
typedef unsigned long u32;
#define MIN(a,b) ((a)<(b) ? (a) : (b))
#endif

static const char ZIPFILE_SCHEMA[] = 
  "CREATE TABLE y("
    "name PRIMARY KEY,"  /* 0: Name of file in zip archive */
    "mode,"              /* 1: POSIX mode for file */
    "mtime,"             /* 2: Last modification time (secs since 1970)*/
    "sz,"                /* 3: Size of object */
    "rawdata,"           /* 4: Raw data */
    "data,"              /* 5: Uncompressed data */
    "method,"            /* 6: Compression method (integer) */
    "z HIDDEN"           /* 7: Name of zip file */
  ") WITHOUT ROWID;";

#define ZIPFILE_F_COLUMN_IDX 7    /* Index of column "file" in the above */
#define ZIPFILE_BUFFER_SIZE (64*1024)


/*
** Magic numbers used to read and write zip files.
**
** ZIPFILE_NEWENTRY_MADEBY:
**   Use this value for the "version-made-by" field in new zip file
**   entries. The upper byte indicates "unix", and the lower byte 
**   indicates that the zip file matches pkzip specification 3.0. 
**   This is what info-zip seems to do.
**
** ZIPFILE_NEWENTRY_REQUIRED:
**   Value for "version-required-to-extract" field of new entries.
**   Version 2.0 is required to support folders and deflate compression.
**
** ZIPFILE_NEWENTRY_FLAGS:
**   Value for "general-purpose-bit-flags" field of new entries. Bit
**   11 means "utf-8 filename and comment".
**
** ZIPFILE_SIGNATURE_CDS:
**   First 4 bytes of a valid CDS record.
**
** ZIPFILE_SIGNATURE_LFH:
**   First 4 bytes of a valid LFH record.
*/
#define ZIPFILE_EXTRA_TIMESTAMP   0x5455
#define ZIPFILE_NEWENTRY_MADEBY   ((3<<8) + 30)
#define ZIPFILE_NEWENTRY_REQUIRED 20
#define ZIPFILE_NEWENTRY_FLAGS    0x800
#define ZIPFILE_SIGNATURE_CDS     0x02014b50
#define ZIPFILE_SIGNATURE_LFH     0x04034b50
#define ZIPFILE_SIGNATURE_EOCD    0x06054b50
#define ZIPFILE_LFH_FIXED_SZ      30

/*
** Set the error message contained in context ctx to the results of
** vprintf(zFmt, ...).
*/
static void zipfileCtxErrorMsg(sqlite3_context *ctx, const char *zFmt, ...){
  char *zMsg = 0;
  va_list ap;
  va_start(ap, zFmt);
  zMsg = sqlite3_vmprintf(zFmt, ap);
  sqlite3_result_error(ctx, zMsg, -1);
  sqlite3_free(zMsg);
  va_end(ap);
}


/*
*** 4.3.16  End of central directory record:
***
***   end of central dir signature    4 bytes  (0x06054b50)
***   number of this disk             2 bytes
***   number of the disk with the
***   start of the central directory  2 bytes
***   total number of entries in the
***   central directory on this disk  2 bytes
***   total number of entries in
***   the central directory           2 bytes
***   size of the central directory   4 bytes
***   offset of start of central
***   directory with respect to
***   the starting disk number        4 bytes
***   .ZIP file comment length        2 bytes
***   .ZIP file comment       (variable size)
*/
typedef struct ZipfileEOCD ZipfileEOCD;
struct ZipfileEOCD {
  u16 iDisk;
  u16 iFirstDisk;
  u16 nEntry;
  u16 nEntryTotal;
  u32 nSize;
  u32 iOffset;
};

/*
*** 4.3.12  Central directory structure:
***
*** ...
***
***   central file header signature   4 bytes  (0x02014b50)
***   version made by                 2 bytes
***   version needed to extract       2 bytes
***   general purpose bit flag        2 bytes
***   compression method              2 bytes
***   last mod file time              2 bytes
***   last mod file date              2 bytes
***   crc-32                          4 bytes
***   compressed size                 4 bytes
***   uncompressed size               4 bytes
***   file name length                2 bytes
***   extra field length              2 bytes
***   file comment length             2 bytes
***   disk number start               2 bytes
***   internal file attributes        2 bytes
***   external file attributes        4 bytes
***   relative offset of local header 4 bytes
*/
typedef struct ZipfileCDS ZipfileCDS;
struct ZipfileCDS {
  u16 iVersionMadeBy;
  u16 iVersionExtract;
  u16 flags;
  u16 iCompression;
  u16 mTime;
  u16 mDate;
  u32 crc32;
  u32 szCompressed;
  u32 szUncompressed;
  u16 nFile;
  u16 nExtra;
  u16 nComment;
  u16 iDiskStart;
  u16 iInternalAttr;
  u32 iExternalAttr;
  u32 iOffset;
  char *zFile;                    /* Filename (sqlite3_malloc()) */
};

/*
*** 4.3.7  Local file header:
***
***   local file header signature     4 bytes  (0x04034b50)
***   version needed to extract       2 bytes
***   general purpose bit flag        2 bytes
***   compression method              2 bytes
***   last mod file time              2 bytes
***   last mod file date              2 bytes
***   crc-32                          4 bytes
***   compressed size                 4 bytes
***   uncompressed size               4 bytes
***   file name length                2 bytes
***   extra field length              2 bytes
***   
*/
typedef struct ZipfileLFH ZipfileLFH;
struct ZipfileLFH {
  u16 iVersionExtract;
  u16 flags;
  u16 iCompression;
  u16 mTime;
  u16 mDate;
  u32 crc32;
  u32 szCompressed;
  u32 szUncompressed;
  u16 nFile;
  u16 nExtra;
};

typedef struct ZipfileEntry ZipfileEntry;
struct ZipfileEntry {
  ZipfileCDS cds;            /* Parsed CDS record */
  u32 mUnixTime;             /* Modification time, in UNIX format */
  u8 *aExtra;                /* cds.nExtra+cds.nComment bytes of extra data */
  i64 iDataOff;
  u8 *aData;                 /* cds.szCompressed bytes of compressed data */
  ZipfileEntry *pNext;       /* Next element in in-memory CDS */
};

/* 
** Cursor type for recursively iterating through a directory structure.
*/
typedef struct ZipfileCsr ZipfileCsr;
struct ZipfileCsr {
  sqlite3_vtab_cursor base;  /* Base class - must be first */
  i64 iId;                   /* Cursor ID */
  u8 bEof;                   /* True when at EOF */
  u8 bNoop;                  /* If next xNext() call is no-op */

  /* Used outside of write transactions */
  FILE *pFile;               /* Zip file */
  i64 iNextOff;              /* Offset of next record in central directory */
  ZipfileEOCD eocd;          /* Parse of central directory record */

  ZipfileEntry *pFreeEntry;

  ZipfileEntry *pCurrent;    /* Current entry */
  ZipfileCsr *pCsrNext;      /* Next cursor on same virtual table */
};

/*
** Possible values for ZipfileCsr.eType. Set in zipfileFilter().
*/
#define ZIPFILE_CSR_LIST 1        /* Cursor reads from ZipfileTab.pFirstEntry */
#define ZIPFILE_CSR_FILE 2        /* Cursor reads from file on disk */

/*
** Values for ZipfileCsr.flags.
*/
#define ZIPFILE_NOOP_NEXT   0x0002     /* Next xNext() is a no-op */

typedef struct ZipfileTab ZipfileTab;
struct ZipfileTab {
  sqlite3_vtab base;         /* Base class - must be first */
  char *zFile;               /* Zip file this table accesses (may be NULL) */
  u8 *aBuffer;               /* Temporary buffer used for various tasks */

  ZipfileCsr *pCsrList;      /* List of cursors */
  i64 iNextCsrid;

  /* The following are used by write transactions only */
  ZipfileEntry *pFirstEntry; /* Linked list of all files (if pWriteFd!=0) */
  ZipfileEntry *pLastEntry;  /* Last element in pFirstEntry list */
  FILE *pWriteFd;            /* File handle open on zip archive */
  i64 szCurrent;             /* Current size of zip archive */
  i64 szOrig;                /* Size of archive at start of transaction */
};

static void zipfileDequote(char *zIn){
  char q = zIn[0];
  if( q=='"' || q=='\'' || q=='`' || q=='[' ){
    char c;
    int iIn = 1;
    int iOut = 0;
    if( q=='[' ) q = ']';
    while( (c = zIn[iIn++]) ){
      if( c==q ){
        if( zIn[iIn++]!=q ) break;
      }
      zIn[iOut++] = c;
    }
    zIn[iOut] = '\0';
  }
}

/*
** Construct a new ZipfileTab virtual table object.
** 
**   argv[0]   -> module name  ("zipfile")
**   argv[1]   -> database name
**   argv[2]   -> table name
**   argv[...] -> "column name" and other module argument fields.
*/
static int zipfileConnect(
  sqlite3 *db,
  void *pAux,
  int argc, const char *const*argv,
  sqlite3_vtab **ppVtab,
  char **pzErr
){
  int nByte = sizeof(ZipfileTab) + ZIPFILE_BUFFER_SIZE;
  int nFile = 0;
  const char *zFile = 0;
  ZipfileTab *pNew = 0;
  int rc;

  if( argc>3 ){
    zFile = argv[3];
    nFile = (int)strlen(zFile)+1;
  }

  rc = sqlite3_declare_vtab(db, ZIPFILE_SCHEMA);
  if( rc==SQLITE_OK ){
    pNew = (ZipfileTab*)sqlite3_malloc(nByte+nFile);
    if( pNew==0 ) return SQLITE_NOMEM;
    memset(pNew, 0, nByte+nFile);
    pNew->aBuffer = (u8*)&pNew[1];
    if( zFile ){
      pNew->zFile = (char*)&pNew->aBuffer[ZIPFILE_BUFFER_SIZE];
      memcpy(pNew->zFile, zFile, nFile);
      zipfileDequote(pNew->zFile);
    }
  }
  *ppVtab = (sqlite3_vtab*)pNew;
  return rc;
}

static void zipfileEntryFree(ZipfileEntry *p){
  if( p ){
    sqlite3_free(p->cds.zFile);
    sqlite3_free(p);
  }
}

static void zipfileCleanupTransaction(ZipfileTab *pTab){
  ZipfileEntry *pEntry;
  ZipfileEntry *pNext;

  if( pTab->pWriteFd ){
    fclose(pTab->pWriteFd);
    pTab->pWriteFd = 0;
  }
  for(pEntry=pTab->pFirstEntry; pEntry; pEntry=pNext){
    pNext = pEntry->pNext;
    zipfileEntryFree(pEntry);
  }
  pTab->pFirstEntry = 0;
  pTab->pLastEntry = 0;
  pTab->szCurrent = 0;
  pTab->szOrig = 0;
}

/*
** This method is the destructor for zipfile vtab objects.
*/
static int zipfileDisconnect(sqlite3_vtab *pVtab){
  zipfileCleanupTransaction((ZipfileTab*)pVtab);
  sqlite3_free(pVtab);
  return SQLITE_OK;
}

/*
** Constructor for a new ZipfileCsr object.
*/
static int zipfileOpen(sqlite3_vtab *p, sqlite3_vtab_cursor **ppCsr){
  ZipfileTab *pTab = (ZipfileTab*)p;
  ZipfileCsr *pCsr;
  pCsr = sqlite3_malloc(sizeof(*pCsr));
  *ppCsr = (sqlite3_vtab_cursor*)pCsr;
  if( pCsr==0 ){
    return SQLITE_NOMEM;
  }
  memset(pCsr, 0, sizeof(*pCsr));
  pCsr->iId = ++pTab->iNextCsrid;
  pCsr->pCsrNext = pTab->pCsrList;
  pTab->pCsrList = pCsr;
  return SQLITE_OK;
}

/*
** Reset a cursor back to the state it was in when first returned
** by zipfileOpen().
*/
static void zipfileResetCursor(ZipfileCsr *pCsr){
  ZipfileEntry *p;
  ZipfileEntry *pNext;

  pCsr->bEof = 0;
  if( pCsr->pFile ){
    fclose(pCsr->pFile);
    pCsr->pFile = 0;
    zipfileEntryFree(pCsr->pCurrent);
    pCsr->pCurrent = 0;
  }

  for(p=pCsr->pFreeEntry; p; p=pNext){
    pNext = p->pNext;
    zipfileEntryFree(p);
  }
}

/*
** Destructor for an ZipfileCsr.
*/
static int zipfileClose(sqlite3_vtab_cursor *cur){
  ZipfileCsr *pCsr = (ZipfileCsr*)cur;
  ZipfileTab *pTab = (ZipfileTab*)(pCsr->base.pVtab);
  ZipfileCsr **pp;
  zipfileResetCursor(pCsr);

  /* Remove this cursor from the ZipfileTab.pCsrList list. */
  for(pp=&pTab->pCsrList; *pp; pp=&((*pp)->pCsrNext)){
    if( *pp==pCsr ){ 
      *pp = pCsr->pCsrNext;
      break;
    }
  }

  sqlite3_free(pCsr);
  return SQLITE_OK;
}

/*
** Set the error message for the virtual table associated with cursor
** pCsr to the results of vprintf(zFmt, ...).
*/
static void zipfileSetErrmsg(ZipfileCsr *pCsr, const char *zFmt, ...){
  va_list ap;
  va_start(ap, zFmt);
  pCsr->base.pVtab->zErrMsg = sqlite3_vmprintf(zFmt, ap);
  va_end(ap);
}

static int zipfileReadData(
  FILE *pFile,                    /* Read from this file */
  u8 *aRead,                      /* Read into this buffer */
  int nRead,                      /* Number of bytes to read */
  i64 iOff,                       /* Offset to read from */
  char **pzErrmsg                 /* OUT: Error message (from sqlite3_malloc) */
){
  size_t n;
  fseek(pFile, (long)iOff, SEEK_SET);
  n = fread(aRead, 1, nRead, pFile);
  if( (int)n!=nRead ){
    *pzErrmsg = sqlite3_mprintf("error in fread()");
    return SQLITE_ERROR;
  }
  return SQLITE_OK;
}

static int zipfileAppendData(
  ZipfileTab *pTab,
  const u8 *aWrite,
  int nWrite
){
  size_t n;
  fseek(pTab->pWriteFd, (long)pTab->szCurrent, SEEK_SET);
  n = fwrite(aWrite, 1, nWrite, pTab->pWriteFd);
  if( (int)n!=nWrite ){
    pTab->base.zErrMsg = sqlite3_mprintf("error in fwrite()");
    return SQLITE_ERROR;
  }
  pTab->szCurrent += nWrite;
  return SQLITE_OK;
}

static u16 zipfileGetU16(const u8 *aBuf){
  return (aBuf[1] << 8) + aBuf[0];
}
static u32 zipfileGetU32(const u8 *aBuf){
  return ((u32)(aBuf[3]) << 24)
       + ((u32)(aBuf[2]) << 16)
       + ((u32)(aBuf[1]) <<  8)
       + ((u32)(aBuf[0]) <<  0);
}

static void zipfilePutU16(u8 *aBuf, u16 val){
  aBuf[0] = val & 0xFF;
  aBuf[1] = (val>>8) & 0xFF;
}
static void zipfilePutU32(u8 *aBuf, u32 val){
  aBuf[0] = val & 0xFF;
  aBuf[1] = (val>>8) & 0xFF;
  aBuf[2] = (val>>16) & 0xFF;
  aBuf[3] = (val>>24) & 0xFF;
}

#define zipfileRead32(aBuf) ( aBuf+=4, zipfileGetU32(aBuf-4) )
#define zipfileRead16(aBuf) ( aBuf+=2, zipfileGetU16(aBuf-2) )

#define zipfileWrite32(aBuf,val) { zipfilePutU32(aBuf,val); aBuf+=4; }
#define zipfileWrite16(aBuf,val) { zipfilePutU16(aBuf,val); aBuf+=2; }

static u8* zipfileCsrBuffer(ZipfileCsr *pCsr){
  return ((ZipfileTab*)(pCsr->base.pVtab))->aBuffer;
}

/*
** Magic numbers used to read CDS records.
*/
#define ZIPFILE_CDS_FIXED_SZ         46
#define ZIPFILE_CDS_NFILE_OFF        28
#define ZIPFILE_CDS_SZCOMPRESSED_OFF 20

/*
** Decode the CDS record in buffer aBuf into (*pCDS). Return SQLITE_ERROR
** if the record is not well-formed, or SQLITE_OK otherwise.
*/
static int zipfileReadCDS(u8 *aBuf, ZipfileCDS *pCDS){
  u8 *aRead = aBuf;
  u32 sig = zipfileRead32(aRead);
  int rc = SQLITE_OK;
  if( sig!=ZIPFILE_SIGNATURE_CDS ){
    rc = SQLITE_ERROR;
  }else{
    pCDS->iVersionMadeBy = zipfileRead16(aRead);
    pCDS->iVersionExtract = zipfileRead16(aRead);
    pCDS->flags = zipfileRead16(aRead);
    pCDS->iCompression = zipfileRead16(aRead);
    pCDS->mTime = zipfileRead16(aRead);
    pCDS->mDate = zipfileRead16(aRead);
    pCDS->crc32 = zipfileRead32(aRead);
    pCDS->szCompressed = zipfileRead32(aRead);
    pCDS->szUncompressed = zipfileRead32(aRead);
    assert( aRead==&aBuf[ZIPFILE_CDS_NFILE_OFF] );
    pCDS->nFile = zipfileRead16(aRead);
    pCDS->nExtra = zipfileRead16(aRead);
    pCDS->nComment = zipfileRead16(aRead);
    pCDS->iDiskStart = zipfileRead16(aRead);
    pCDS->iInternalAttr = zipfileRead16(aRead);
    pCDS->iExternalAttr = zipfileRead32(aRead);
    pCDS->iOffset = zipfileRead32(aRead);
    assert( aRead==&aBuf[ZIPFILE_CDS_FIXED_SZ] );
  }

  return rc;
}

/*
** The general format of an extra field is:
**
**   Header ID    2 bytes
**   Data Size    2 bytes
**   Data         N bytes
*/
static int zipfileScanExtra(u8 *aExtra, int nExtra, u32 *pmTime){
  int ret = 0;
  u8 *p = aExtra;
  u8 *pEnd = &aExtra[nExtra];

  while( p<pEnd ){
    u16 id = zipfileRead16(p);
    u16 nByte = zipfileRead16(p);

    switch( id ){
      case ZIPFILE_EXTRA_TIMESTAMP: {
        u8 b = p[0];
        if( b & 0x01 ){     /* 0x01 -> modtime is present */
          *pmTime = zipfileGetU32(&p[1]);
          ret = 1;
        }
        break;
      }
    }

    p += nByte;
  }
  return ret;
}

/*
** "Standard" MS-DOS time format:
**
**   File modification time:
**     Bits 00-04: seconds divided by 2
**     Bits 05-10: minute
**     Bits 11-15: hour
**   File modification date:
**     Bits 00-04: day
**     Bits 05-08: month (1-12)
**     Bits 09-15: years from 1980 
*/
static time_t zipfileMtime(ZipfileCDS *pCDS){
  struct tm t;
  memset(&t, 0, sizeof(t));
  t.tm_sec = (pCDS->mTime & 0x1F)*2;
  t.tm_min = (pCDS->mTime >> 5) & 0x2F;
  t.tm_hour = (pCDS->mTime >> 11) & 0x1F;

  t.tm_mday = (pCDS->mDate & 0x1F);
  t.tm_mon = ((pCDS->mDate >> 5) & 0x0F) - 1;
  t.tm_year = 80 + ((pCDS->mDate >> 9) & 0x7F);

  return mktime(&t);
}

static int zipfileReadLFH(
  u8 *aBuffer,
  ZipfileLFH *pLFH
){
  u8 *aRead = aBuffer;
  int rc = SQLITE_OK;

  u32 sig = zipfileRead32(aRead);
  if( sig!=ZIPFILE_SIGNATURE_LFH ){
    rc = SQLITE_ERROR;
  }else{
    pLFH->iVersionExtract = zipfileRead16(aRead);
    pLFH->flags = zipfileRead16(aRead);
    pLFH->iCompression = zipfileRead16(aRead);
    pLFH->mTime = zipfileRead16(aRead);
    pLFH->mDate = zipfileRead16(aRead);
    pLFH->crc32 = zipfileRead32(aRead);
    pLFH->szCompressed = zipfileRead32(aRead);
    pLFH->szUncompressed = zipfileRead32(aRead);
    pLFH->nFile = zipfileRead16(aRead);
    pLFH->nExtra = zipfileRead16(aRead);
  }
  return rc;
}

/*
** Read a Zip archive CDS header from offset iOff of file pFile. Return
** SQLITE_OK if successful, or an SQLite error code otherwise.
*/
static int zipfileGetEntry(
  ZipfileTab *pTab,               /* Store any error message here */
  const u8 *aBlob,                /* Pointer to in-memory file image */
  int nBlob,                      /* Size of aBlob[] in bytes */
  FILE *pFile,                    /* If aBlob==0, read from this file */
  i64 iOff,
  ZipfileEntry **ppEntry
){
  u8 *aRead;
  char **pzErr = &pTab->base.zErrMsg;
  int rc = SQLITE_OK;

  if( aBlob==0 ){
    aRead = pTab->aBuffer;
    rc = zipfileReadData(pFile, aRead, ZIPFILE_CDS_FIXED_SZ, iOff, pzErr);
  }else{
    aRead = (u8*)&aBlob[iOff];
  }

  if( rc==SQLITE_OK ){
    int nAlloc;
    ZipfileEntry *pNew;

    int nFile = zipfileGetU16(&aRead[ZIPFILE_CDS_NFILE_OFF]);
    int nExtra = zipfileGetU16(&aRead[ZIPFILE_CDS_NFILE_OFF+2]);
    nExtra += zipfileGetU16(&aRead[ZIPFILE_CDS_NFILE_OFF+4]);

    nAlloc = sizeof(ZipfileEntry) + nExtra;
    if( aBlob ){
      nAlloc += zipfileGetU32(&aRead[ZIPFILE_CDS_SZCOMPRESSED_OFF]);
    }

    pNew = (ZipfileEntry*)sqlite3_malloc(nAlloc);
    if( pNew==0 ){
      rc = SQLITE_NOMEM;
    }else{
      memset(pNew, 0, sizeof(ZipfileEntry));
      rc = zipfileReadCDS(aRead, &pNew->cds);
      if( rc!=SQLITE_OK ){
        *pzErr = sqlite3_mprintf("failed to read CDS at offset %lld", iOff);
      }else if( aBlob==0 ){
        rc = zipfileReadData(
            pFile, aRead, nExtra+nFile, iOff+ZIPFILE_CDS_FIXED_SZ, pzErr
        );
      }else{
        aRead = (u8*)&aBlob[iOff + ZIPFILE_CDS_FIXED_SZ];
      }
    }

    if( rc==SQLITE_OK ){
      u32 *pt = &pNew->mUnixTime;
      pNew->cds.zFile = sqlite3_mprintf("%.*s", nFile, aRead); 
      pNew->aExtra = (u8*)&pNew[1];
      memcpy(pNew->aExtra, &aRead[nFile], nExtra);
      if( pNew->cds.zFile==0 ){
        rc = SQLITE_NOMEM;
      }else if( 0==zipfileScanExtra(&aRead[nFile], pNew->cds.nExtra, pt) ){
        pNew->mUnixTime = zipfileMtime(&pNew->cds);
      }
    }

    if( rc==SQLITE_OK ){
      static const int szFix = ZIPFILE_LFH_FIXED_SZ;
      ZipfileLFH lfh;
      if( pFile ){
        rc = zipfileReadData(pFile, aRead, szFix, pNew->cds.iOffset, pzErr);
      }else{
        aRead = &aBlob[pNew->cds.iOffset];
      }

      rc = zipfileReadLFH(aRead, &lfh);
      if( rc==SQLITE_OK ){
        pNew->iDataOff =  pNew->cds.iOffset + ZIPFILE_LFH_FIXED_SZ;
        pNew->iDataOff += lfh.nFile + lfh.nExtra;
        if( aBlob && pNew->cds.szCompressed ){
          pNew->aData = &pNew->aExtra[nExtra];
          memcpy(pNew->aData, &aBlob[pNew->iDataOff], pNew->cds.szCompressed);
        }
      }else{
        *pzErr = sqlite3_mprintf("failed to read LFH at offset %d", 
            (int)pNew->cds.iOffset
        );
      }
    }

    if( rc!=SQLITE_OK ){
      zipfileEntryFree(pNew);
    }else{
      *ppEntry = pNew;
    }
  }

  return rc;
}

static FILE *zipfileGetFd(ZipfileCsr *pCsr){
  if( pCsr->pFile ) return pCsr->pFile;
  return ((ZipfileTab*)(pCsr->base.pVtab))->pWriteFd;
}


/*
** Advance an ZipfileCsr to its next row of output.
*/
static int zipfileNext(sqlite3_vtab_cursor *cur){
  ZipfileCsr *pCsr = (ZipfileCsr*)cur;
  int rc = SQLITE_OK;

  if( pCsr->pFile ){
    i64 iEof = pCsr->eocd.iOffset + pCsr->eocd.nSize;
    zipfileEntryFree(pCsr->pCurrent);
    pCsr->pCurrent = 0;
    if( pCsr->iNextOff>=iEof ){
      pCsr->bEof = 1;
    }else{
      ZipfileEntry *p = 0;
      ZipfileTab *pTab = (ZipfileTab*)(cur->pVtab);
      rc = zipfileGetEntry(pTab, 0, 0, pCsr->pFile, pCsr->iNextOff, &p);
      if( rc==SQLITE_OK ){
        pCsr->iNextOff += ZIPFILE_CDS_FIXED_SZ;
        pCsr->iNextOff += (int)p->cds.nExtra + p->cds.nFile + p->cds.nComment;
      }
      pCsr->pCurrent = p;
    }
  }else{
    if( !pCsr->bNoop ){
      pCsr->pCurrent = pCsr->pCurrent->pNext;
    }
    if( pCsr->pCurrent==0 ){
      pCsr->bEof = 1;
    }
  }

  pCsr->bNoop = 0;
  return rc;
}

static void zipfileMtimeToDos(ZipfileCDS *pCds, u32 mTime){
  time_t t = (time_t)mTime;
  struct tm res;

#if !defined(_WIN32) && !defined(WIN32)
  localtime_r(&t, &res);
#else
  memcpy(&res, localtime(&t), sizeof(struct tm));
#endif

  pCds->mTime = (u16)(
    (res.tm_sec / 2) + 
    (res.tm_min << 5) +
    (res.tm_hour << 11));

  pCds->mDate = (u16)(
    (res.tm_mday-1) +
    ((res.tm_mon+1) << 5) +
    ((res.tm_year-80) << 9));
}

static void zipfileInflate(
  sqlite3_context *pCtx,          /* Store error here, if any */
  const u8 *aIn,                  /* Compressed data */
  int nIn,                        /* Size of buffer aIn[] in bytes */
  int nOut                        /* Expected output size */
){
  u8 *aRes = sqlite3_malloc(nOut);
  if( aRes==0 ){
    sqlite3_result_error_nomem(pCtx);
  }else{
    int err;
    z_stream str;
    memset(&str, 0, sizeof(str));

    str.next_in = (Byte*)aIn;
    str.avail_in = nIn;
    str.next_out = (Byte*)aRes;
    str.avail_out = nOut;

    err = inflateInit2(&str, -15);
    if( err!=Z_OK ){
      zipfileCtxErrorMsg(pCtx, "inflateInit2() failed (%d)", err);
    }else{
      err = inflate(&str, Z_NO_FLUSH);
      if( err!=Z_STREAM_END ){
        zipfileCtxErrorMsg(pCtx, "inflate() failed (%d)", err);
      }else{
        sqlite3_result_blob(pCtx, aRes, nOut, SQLITE_TRANSIENT);
      }
    }
    sqlite3_free(aRes);
    inflateEnd(&str);
  }
}

static int zipfileDeflate(
  ZipfileTab *pTab,               /* Set error message here */
  const u8 *aIn, int nIn,         /* Input */
  u8 **ppOut, int *pnOut          /* Output */
){
  int nAlloc = (int)compressBound(nIn);
  u8 *aOut;
  int rc = SQLITE_OK;

  aOut = (u8*)sqlite3_malloc(nAlloc);
  if( aOut==0 ){
    rc = SQLITE_NOMEM;
  }else{
    int res;
    z_stream str;
    memset(&str, 0, sizeof(str));
    str.next_in = (Bytef*)aIn;
    str.avail_in = nIn;
    str.next_out = aOut;
    str.avail_out = nAlloc;

    deflateInit2(&str, 9, Z_DEFLATED, -15, 8, Z_DEFAULT_STRATEGY);
    res = deflate(&str, Z_FINISH);

    if( res==Z_STREAM_END ){
      *ppOut = aOut;
      *pnOut = (int)str.total_out;
    }else{
      sqlite3_free(aOut);
      pTab->base.zErrMsg = sqlite3_mprintf("zipfile: deflate() error");
      rc = SQLITE_ERROR;
    }
    deflateEnd(&str);
  }

  return rc;
}


/*
** Return values of columns for the row at which the series_cursor
** is currently pointing.
*/
static int zipfileColumn(
  sqlite3_vtab_cursor *cur,   /* The cursor */
  sqlite3_context *ctx,       /* First argument to sqlite3_result_...() */
  int i                       /* Which column to return */
){
  ZipfileCsr *pCsr = (ZipfileCsr*)cur;
  ZipfileCDS *pCDS = &pCsr->pCurrent->cds;
  int rc = SQLITE_OK;
  switch( i ){
    case 0:   /* name */
      sqlite3_result_text(ctx, pCDS->zFile, -1, SQLITE_TRANSIENT);
      break;
    case 1:   /* mode */
      /* TODO: Whether or not the following is correct surely depends on
      ** the platform on which the archive was created.  */
      sqlite3_result_int(ctx, pCDS->iExternalAttr >> 16);
      break;
    case 2: { /* mtime */
      sqlite3_result_int64(ctx, pCsr->pCurrent->mUnixTime);
      break;
    }
    case 3: { /* sz */
      if( sqlite3_vtab_nochange(ctx)==0 ){
        sqlite3_result_int64(ctx, pCDS->szUncompressed);
      }
      break;
    }
    case 4:   /* rawdata */
      if( sqlite3_vtab_nochange(ctx) ) break;
    case 5: { /* data */
      if( i==4 || pCDS->iCompression==0 || pCDS->iCompression==8 ){
        int sz = pCDS->szCompressed;
        int szFinal = pCDS->szUncompressed;
        if( szFinal>0 ){
          u8 *aBuf;
          u8 *aFree = 0;
          if( pCsr->pCurrent->aData ){
            aBuf = pCsr->pCurrent->aData;
          }else{
            aBuf = aFree = sqlite3_malloc(sz);
            if( aBuf==0 ){
              rc = SQLITE_NOMEM;
            }else{
              FILE *pFile = zipfileGetFd(pCsr);
              if( rc==SQLITE_OK ){
                rc = zipfileReadData(pFile, aBuf, sz, pCsr->pCurrent->iDataOff,
                    &pCsr->base.pVtab->zErrMsg
                );
              }
            }
          }
          if( rc==SQLITE_OK ){
            if( i==5 && pCDS->iCompression ){
              zipfileInflate(ctx, aBuf, sz, szFinal);
            }else{
              sqlite3_result_blob(ctx, aBuf, sz, SQLITE_TRANSIENT);
            }
          }
          sqlite3_free(aFree);
        }else{
          /* Figure out if this is a directory or a zero-sized file. Consider
          ** it to be a directory either if the mode suggests so, or if
          ** the final character in the name is '/'.  */
          u32 mode = pCDS->iExternalAttr >> 16;
          if( !(mode & S_IFDIR) && pCDS->zFile[pCDS->nFile-1]!='/' ){
            sqlite3_result_blob(ctx, "", 0, SQLITE_STATIC);
          }
        }
      }
      break;
    }
    case 6:   /* method */
      sqlite3_result_int(ctx, pCDS->iCompression);
      break;
    case 7:   /* z */
      sqlite3_result_int64(ctx, pCsr->iId);
      break;
  }

  return rc;
}

/*
** Return the rowid for the current row.
*/
static int zipfileRowid(sqlite3_vtab_cursor *cur, sqlite_int64 *pRowid){
  assert( 0 );
  return SQLITE_OK;
}

/*
** Return TRUE if the cursor has been moved off of the last
** row of output.
*/
static int zipfileEof(sqlite3_vtab_cursor *cur){
  ZipfileCsr *pCsr = (ZipfileCsr*)cur;
  return pCsr->bEof;
}

/*
*/
static int zipfileReadEOCD(
  ZipfileTab *pTab,               /* Return errors here */
  const u8 *aBlob,                /* Pointer to in-memory file image */
  int nBlob,                      /* Size of aBlob[] in bytes */
  FILE *pFile,                    /* Read from this file if aBlob==0 */
  ZipfileEOCD *pEOCD              /* Object to populate */
){
  u8 *aRead = pTab->aBuffer;      /* Temporary buffer */
  int nRead;                      /* Bytes to read from file */
  int rc = SQLITE_OK;

  if( aBlob==0 ){
    i64 iOff;                     /* Offset to read from */
    i64 szFile;                   /* Total size of file in bytes */
    fseek(pFile, 0, SEEK_END);
    szFile = (i64)ftell(pFile);
    if( szFile==0 ){
      memset(pEOCD, 0, sizeof(ZipfileEOCD));
      return SQLITE_OK;
    }
    nRead = (int)(MIN(szFile, ZIPFILE_BUFFER_SIZE));
    iOff = szFile - nRead;
    rc = zipfileReadData(pFile, aRead, nRead, iOff, &pTab->base.zErrMsg);
  }else{
    nRead = (int)(MIN(nBlob, ZIPFILE_BUFFER_SIZE));
    aRead = (u8*)&aBlob[nBlob-nRead];
  }

  if( rc==SQLITE_OK ){
    int i;

    /* Scan backwards looking for the signature bytes */
    for(i=nRead-20; i>=0; i--){
      if( aRead[i]==0x50 && aRead[i+1]==0x4b 
       && aRead[i+2]==0x05 && aRead[i+3]==0x06 
      ){
        break;
      }
    }
    if( i<0 ){
      pTab->base.zErrMsg = sqlite3_mprintf(
          "cannot find end of central directory record"
      );
      return SQLITE_ERROR;
    }

    aRead += i+4;
    pEOCD->iDisk = zipfileRead16(aRead);
    pEOCD->iFirstDisk = zipfileRead16(aRead);
    pEOCD->nEntry = zipfileRead16(aRead);
    pEOCD->nEntryTotal = zipfileRead16(aRead);
    pEOCD->nSize = zipfileRead32(aRead);
    pEOCD->iOffset = zipfileRead32(aRead);
  }

  return SQLITE_OK;
}

/*
** Add object pNew to the end of the linked list that begins at
** ZipfileTab.pFirstEntry and ends with pLastEntry.
*/
static void zipfileAddEntry(
  ZipfileTab *pTab, 
  ZipfileEntry *pBefore, 
  ZipfileEntry *pNew
){
  assert( (pTab->pFirstEntry==0)==(pTab->pLastEntry==0) );
  assert( pNew->pNext==0 );
  if( pBefore==0 ){
    if( pTab->pFirstEntry==0 ){
      pTab->pFirstEntry = pTab->pLastEntry = pNew;
    }else{
      assert( pTab->pLastEntry->pNext==0 );
      pTab->pLastEntry->pNext = pNew;
      pTab->pLastEntry = pNew;
    }
  }else{
    ZipfileEntry **pp;
    for(pp=&pTab->pFirstEntry; *pp!=pBefore; pp=&((*pp)->pNext));
    pNew->pNext = pBefore;
    *pp = pNew;
  }
}

static int zipfileLoadDirectory(ZipfileTab *pTab, const u8 *aBlob, int nBlob){
  ZipfileEOCD eocd;
  int rc;
  int i;
  i64 iOff;

  rc = zipfileReadEOCD(pTab, aBlob, nBlob, pTab->pWriteFd, &eocd);
  iOff = eocd.iOffset;
  for(i=0; rc==SQLITE_OK && i<eocd.nEntry; i++){
    ZipfileEntry *pNew = 0;
    rc = zipfileGetEntry(pTab, aBlob, nBlob, pTab->pWriteFd, iOff, &pNew);

    if( rc==SQLITE_OK ){
      zipfileAddEntry(pTab, 0, pNew);
      iOff += ZIPFILE_CDS_FIXED_SZ;
      iOff += (int)pNew->cds.nExtra + pNew->cds.nFile + pNew->cds.nComment;
    }
  }
  return rc;
}

/*
** xFilter callback.
*/
static int zipfileFilter(
  sqlite3_vtab_cursor *cur, 
  int idxNum, const char *idxStr,
  int argc, sqlite3_value **argv
){
  ZipfileTab *pTab = (ZipfileTab*)cur->pVtab;
  ZipfileCsr *pCsr = (ZipfileCsr*)cur;
  const char *zFile;              /* Zip file to scan */
  int rc = SQLITE_OK;             /* Return Code */
  int bInMemory = 0;              /* True for an in-memory zipfile */

  zipfileResetCursor(pCsr);

  if( pTab->zFile ){
    zFile = pTab->zFile;
  }else if( idxNum==0 ){
    /* Error. This is an eponymous virtual table and the user has not 
    ** supplied a file name. */
    zipfileSetErrmsg(pCsr, "table function zipfile() requires an argument");
    return SQLITE_ERROR;
  }else if( sqlite3_value_type(argv[0])==SQLITE_BLOB ){
    const u8 *aBlob = (const u8*)sqlite3_value_blob(argv[0]);
    int nBlob = sqlite3_value_bytes(argv[0]);
    assert( pTab->pFirstEntry==0 );
    rc = zipfileLoadDirectory(pTab, aBlob, nBlob);
    pCsr->pFreeEntry = pTab->pFirstEntry;
    pTab->pFirstEntry = pTab->pLastEntry = 0;
    if( rc!=SQLITE_OK ) return rc;
    bInMemory = 1;
  }else{
    zFile = (const char*)sqlite3_value_text(argv[0]);
  }

  if( 0==pTab->pWriteFd && 0==bInMemory ){
    pCsr->pFile = fopen(zFile, "rb");
    if( pCsr->pFile==0 ){
      zipfileSetErrmsg(pCsr, "cannot open file: %s", zFile);
      rc = SQLITE_ERROR;
    }else{
      rc = zipfileReadEOCD(pTab, 0, 0, pCsr->pFile, &pCsr->eocd);
      if( rc==SQLITE_OK ){
        if( pCsr->eocd.nEntry==0 ){
          pCsr->bEof = 1;
        }else{
          pCsr->iNextOff = pCsr->eocd.iOffset;
          rc = zipfileNext(cur);
        }
      }
    }
  }else{
    pCsr->bNoop = 1;
    pCsr->pCurrent = pCsr->pFreeEntry ? pCsr->pFreeEntry : pTab->pFirstEntry;
    rc = zipfileNext(cur);
  }

  return rc;
}

/*
** xBestIndex callback.
*/
static int zipfileBestIndex(
  sqlite3_vtab *tab,
  sqlite3_index_info *pIdxInfo
){
  int i;

  for(i=0; i<pIdxInfo->nConstraint; i++){
    const struct sqlite3_index_constraint *pCons = &pIdxInfo->aConstraint[i];
    if( pCons->usable==0 ) continue;
    if( pCons->op!=SQLITE_INDEX_CONSTRAINT_EQ ) continue;
    if( pCons->iColumn!=ZIPFILE_F_COLUMN_IDX ) continue;
    break;
  }

  if( i<pIdxInfo->nConstraint ){
    pIdxInfo->aConstraintUsage[i].argvIndex = 1;
    pIdxInfo->aConstraintUsage[i].omit = 1;
    pIdxInfo->estimatedCost = 1000.0;
    pIdxInfo->idxNum = 1;
  }else{
    pIdxInfo->estimatedCost = (double)(((sqlite3_int64)1) << 50);
    pIdxInfo->idxNum = 0;
  }

  return SQLITE_OK;
}

static ZipfileEntry *zipfileNewEntry(const char *zPath){
  ZipfileEntry *pNew;
  pNew = sqlite3_malloc(sizeof(ZipfileEntry));
  if( pNew ){
    memset(pNew, 0, sizeof(ZipfileEntry));
    pNew->cds.zFile = sqlite3_mprintf("%s", zPath);
    if( pNew->cds.zFile==0 ){
      sqlite3_free(pNew);
      pNew = 0;
    }
  }
  return pNew;
}

static int zipfileAppendEntry(
  ZipfileTab *pTab,
  ZipfileCDS *pCds,
  const char *zPath,              /* Path for new entry */
  int nPath,                      /* strlen(zPath) */
  const u8 *pData,
  int nData,
  u32 mTime
){
  u8 *aBuf = pTab->aBuffer;
  int rc;

  pCds->nExtra = 9;

  zipfileWrite32(aBuf, ZIPFILE_SIGNATURE_LFH);
  zipfileWrite16(aBuf, pCds->iVersionExtract);
  zipfileWrite16(aBuf, pCds->flags);
  zipfileWrite16(aBuf, pCds->iCompression);
  zipfileWrite16(aBuf, pCds->mTime);
  zipfileWrite16(aBuf, pCds->mDate);
  zipfileWrite32(aBuf, pCds->crc32);
  zipfileWrite32(aBuf, pCds->szCompressed);
  zipfileWrite32(aBuf, pCds->szUncompressed);
  zipfileWrite16(aBuf, (u16)nPath);
  zipfileWrite16(aBuf, pCds->nExtra);
  assert( aBuf==&pTab->aBuffer[ZIPFILE_LFH_FIXED_SZ] );
  rc = zipfileAppendData(pTab, pTab->aBuffer, (int)(aBuf - pTab->aBuffer));
  if( rc==SQLITE_OK ){
    rc = zipfileAppendData(pTab, (const u8*)zPath, nPath);
  }

  if( rc==SQLITE_OK && pCds->nExtra ){
    aBuf = pTab->aBuffer;
    zipfileWrite16(aBuf, ZIPFILE_EXTRA_TIMESTAMP);
    zipfileWrite16(aBuf, 5);
    *aBuf++ = 0x01;
    zipfileWrite32(aBuf, mTime);
    rc = zipfileAppendData(pTab, pTab->aBuffer, 9);
  }

  if( rc==SQLITE_OK ){
    rc = zipfileAppendData(pTab, pData, nData);
  }

  return rc;
}

static int zipfileGetMode(
  ZipfileTab *pTab, 
  sqlite3_value *pVal, 
  u32 defaultMode,                /* Value to use if pVal IS NULL */
  u32 *pMode
){
  const char *z = (const char*)sqlite3_value_text(pVal);
  u32 mode = 0;
  if( z==0 ){
    mode = defaultMode;
  }else if( z[0]>='0' && z[0]<='9' ){
    mode = (unsigned int)sqlite3_value_int(pVal);
  }else{
    const char zTemplate[11] = "-rwxrwxrwx";
    int i;
    if( strlen(z)!=10 ) goto parse_error;
    switch( z[0] ){
      case '-': mode |= S_IFREG; break;
      case 'd': mode |= S_IFDIR; break;
#if !defined(_WIN32) && !defined(WIN32)
      case 'l': mode |= S_IFLNK; break;
#endif
      default: goto parse_error;
    }
    for(i=1; i<10; i++){
      if( z[i]==zTemplate[i] ) mode |= 1 << (9-i);
      else if( z[i]!='-' ) goto parse_error;
    }
  }
  *pMode = mode;
  return SQLITE_OK;

 parse_error:
  pTab->base.zErrMsg = sqlite3_mprintf("zipfile: parse error in mode: %s", z);
  return SQLITE_ERROR;
}

/*
** Both (const char*) arguments point to nul-terminated strings. Argument
** nB is the value of strlen(zB). This function returns 0 if the strings are
** identical, ignoring any trailing '/' character in either path.  */
static int zipfileComparePath(const char *zA, const char *zB, int nB){
  int nA = (int)strlen(zA);
  if( zA[nA-1]=='/' ) nA--;
  if( zB[nB-1]=='/' ) nB--;
  if( nA==nB && memcmp(zA, zB, nA)==0 ) return 0;
  return 1;
}

/*
** xUpdate method.
*/
static int zipfileUpdate(
  sqlite3_vtab *pVtab, 
  int nVal, 
  sqlite3_value **apVal, 
  sqlite_int64 *pRowid
){
  ZipfileTab *pTab = (ZipfileTab*)pVtab;
  int rc = SQLITE_OK;             /* Return Code */
  ZipfileEntry *pNew = 0;         /* New in-memory CDS entry */

  u32 mode = 0;                   /* Mode for new entry */
  i64 mTime = 0;                  /* Modification time for new entry */
  i64 sz = 0;                     /* Uncompressed size */
  const char *zPath = 0;          /* Path for new entry */
  int nPath = 0;                  /* strlen(zPath) */
  const u8 *pData = 0;            /* Pointer to buffer containing content */
  int nData = 0;                  /* Size of pData buffer in bytes */
  int iMethod = 0;                /* Compression method for new entry */
  u8 *pFree = 0;                  /* Free this */
  char *zFree = 0;                /* Also free this */
  ZipfileEntry *pOld = 0;
  int bIsDir = 0;
  u32 iCrc32 = 0;

  assert( pTab->zFile );
  assert( pTab->pWriteFd );

  /* If this is a DELETE or UPDATE, find the archive entry to delete. */
  if( sqlite3_value_type(apVal[0])!=SQLITE_NULL ){
    const char *zDelete = (const char*)sqlite3_value_text(apVal[0]);
    int nDelete = (int)strlen(zDelete);
    for(pOld=pTab->pFirstEntry; 1; pOld=pOld->pNext){
      if( zipfileComparePath(pOld->cds.zFile, zDelete, nDelete)==0 ){
        break;
      }
      assert( pOld->pNext );
    }
  }

  if( nVal>1 ){
    /* Check that "sz" and "rawdata" are both NULL: */
    if( sqlite3_value_type(apVal[5])!=SQLITE_NULL
     || sqlite3_value_type(apVal[6])!=SQLITE_NULL
    ){
      rc = SQLITE_CONSTRAINT;
    }

    if( rc==SQLITE_OK ){
      if( sqlite3_value_type(apVal[7])==SQLITE_NULL ){
        /* data=NULL. A directory */
        bIsDir = 1;
      }else{
        /* Value specified for "data", and possibly "method". This must be
        ** a regular file or a symlink. */
        const u8 *aIn = sqlite3_value_blob(apVal[7]);
        int nIn = sqlite3_value_bytes(apVal[7]);
        int bAuto = sqlite3_value_type(apVal[8])==SQLITE_NULL;

        iMethod = sqlite3_value_int(apVal[8]);
        sz = nIn;
        pData = aIn;
        nData = nIn;
        if( iMethod!=0 && iMethod!=8 ){
          rc = SQLITE_CONSTRAINT;
        }else{
          if( bAuto || iMethod ){
            int nCmp;
            rc = zipfileDeflate(pTab, aIn, nIn, &pFree, &nCmp);
            if( rc==SQLITE_OK ){
              if( iMethod || nCmp<nIn ){
                iMethod = 8;
                pData = pFree;
                nData = nCmp;
              }
            }
          }
          iCrc32 = crc32(0, aIn, nIn);
        }
      }
    }

    if( rc==SQLITE_OK ){
      rc = zipfileGetMode(pTab, apVal[3], 
          (bIsDir ? (S_IFDIR + 0755) : (S_IFREG + 0644)), &mode
      );
      if( rc==SQLITE_OK && (bIsDir == ((mode & S_IFDIR)==0)) ){
        /* The "mode" attribute is a directory, but data has been specified.
        ** Or vice-versa - no data but "mode" is a file or symlink.  */
        rc = SQLITE_CONSTRAINT;
      }
    }

    if( rc==SQLITE_OK ){
      zPath = (const char*)sqlite3_value_text(apVal[2]);
      nPath = (int)strlen(zPath);
      if( sqlite3_value_type(apVal[4])==SQLITE_NULL ){
        mTime = (sqlite3_int64)time(0);
      }else{
        mTime = sqlite3_value_int64(apVal[4]);
      }
    }

    if( rc==SQLITE_OK && bIsDir ){
      /* For a directory, check that the last character in the path is a
      ** '/'. This appears to be required for compatibility with info-zip
      ** (the unzip command on unix). It does not create directories
      ** otherwise.  */
      if( zPath[nPath-1]!='/' ){
        zFree = sqlite3_mprintf("%s/", zPath);
        if( zFree==0 ){ rc = SQLITE_NOMEM; }
        zPath = (const char*)zFree;
        nPath++;
      }
    }

    /* Check that we're not inserting a duplicate entry */
    if( pOld==0 && rc==SQLITE_OK ){
      ZipfileEntry *p;
      for(p=pTab->pFirstEntry; p; p=p->pNext){
        if( zipfileComparePath(p->cds.zFile, zPath, nPath)==0 ){
          rc = SQLITE_CONSTRAINT;
          break;
        }
      }
    }

    if( rc==SQLITE_OK ){
      /* Create the new CDS record. */
      pNew = zipfileNewEntry(zPath);
      if( pNew==0 ){
        rc = SQLITE_NOMEM;
      }else{
        pNew->cds.iVersionMadeBy = ZIPFILE_NEWENTRY_MADEBY;
        pNew->cds.iVersionExtract = ZIPFILE_NEWENTRY_REQUIRED;
        pNew->cds.flags = ZIPFILE_NEWENTRY_FLAGS;
        pNew->cds.iCompression = (u16)iMethod;
        zipfileMtimeToDos(&pNew->cds, (u32)mTime);
        pNew->cds.crc32 = iCrc32;
        pNew->cds.szCompressed = nData;
        pNew->cds.szUncompressed = (u32)sz;
        pNew->cds.iExternalAttr = (mode<<16);
        pNew->cds.iOffset = (u32)pTab->szCurrent;
        pNew->cds.nFile = nPath;
        pNew->mUnixTime = (u32)mTime;
        rc = zipfileAppendEntry(
            pTab, &pNew->cds, zPath, nPath, pData, nData, pNew->mUnixTime
        );
        zipfileAddEntry(pTab, pOld, pNew);
      }
    }
  }

  if( rc==SQLITE_OK && pOld ){
    ZipfileEntry **pp;
    ZipfileCsr *pCsr;
    for(pCsr=pTab->pCsrList; pCsr; pCsr=pCsr->pCsrNext){
      if( pCsr->pCurrent==pOld ){
        pCsr->pCurrent = pOld->pNext;
        pCsr->bNoop = 1;
      }
    }
    for(pp=&pTab->pFirstEntry; (*pp)!=pOld; pp=&((*pp)->pNext));
    *pp = (*pp)->pNext;
    zipfileEntryFree(pOld);
  }

  sqlite3_free(pFree);
  sqlite3_free(zFree);
  return rc;
}

static int zipfileAppendEOCD(ZipfileTab *pTab, ZipfileEOCD *p){
  u8 *aBuf = pTab->aBuffer;

  zipfileWrite32(aBuf, ZIPFILE_SIGNATURE_EOCD);
  zipfileWrite16(aBuf, p->iDisk);
  zipfileWrite16(aBuf, p->iFirstDisk);
  zipfileWrite16(aBuf, p->nEntry);
  zipfileWrite16(aBuf, p->nEntryTotal);
  zipfileWrite32(aBuf, p->nSize);
  zipfileWrite32(aBuf, p->iOffset);
  zipfileWrite16(aBuf, 0);        /* Size of trailing comment in bytes*/

  assert( (aBuf-pTab->aBuffer)==22 );
  return zipfileAppendData(pTab, pTab->aBuffer, (int)(aBuf - pTab->aBuffer));
}

static int zipfileBegin(sqlite3_vtab *pVtab){
  ZipfileTab *pTab = (ZipfileTab*)pVtab;
  int rc = SQLITE_OK;

  assert( pTab->pWriteFd==0 );

  /* This table is only writable if a default archive path was specified 
  ** as part of the CREATE VIRTUAL TABLE statement. */
  if( pTab->zFile==0 ){
    pTab->base.zErrMsg = sqlite3_mprintf(
        "zipfile: writing requires a default archive"
    );
    return SQLITE_ERROR;
  }

  /* Open a write fd on the file. Also load the entire central directory
  ** structure into memory. During the transaction any new file data is 
  ** appended to the archive file, but the central directory is accumulated
  ** in main-memory until the transaction is committed.  */
  pTab->pWriteFd = fopen(pTab->zFile, "ab+");
  if( pTab->pWriteFd==0 ){
    pTab->base.zErrMsg = sqlite3_mprintf(
        "zipfile: failed to open file %s for writing", pTab->zFile
    );
    rc = SQLITE_ERROR;
  }else{
    fseek(pTab->pWriteFd, 0, SEEK_END);
    pTab->szCurrent = pTab->szOrig = (i64)ftell(pTab->pWriteFd);
    rc = zipfileLoadDirectory(pTab, 0, 0);
  }

  if( rc!=SQLITE_OK ){
    zipfileCleanupTransaction(pTab);
  }

  return rc;
}

/*
** Serialize the CDS structure into buffer aBuf[]. Return the number
** of bytes written.
*/
static int zipfileSerializeCDS(ZipfileEntry *pEntry, u8 *aBuf){
  u8 *a = aBuf;
  ZipfileCDS *pCDS = &pEntry->cds;

  if( pEntry->aExtra==0 ){
    pCDS->nExtra = 9;
  }

  zipfileWrite32(a, ZIPFILE_SIGNATURE_CDS);
  zipfileWrite16(a, pCDS->iVersionMadeBy);
  zipfileWrite16(a, pCDS->iVersionExtract);
  zipfileWrite16(a, pCDS->flags);
  zipfileWrite16(a, pCDS->iCompression);
  zipfileWrite16(a, pCDS->mTime);
  zipfileWrite16(a, pCDS->mDate);
  zipfileWrite32(a, pCDS->crc32);
  zipfileWrite32(a, pCDS->szCompressed);
  zipfileWrite32(a, pCDS->szUncompressed);
  assert( a==&aBuf[ZIPFILE_CDS_NFILE_OFF] );
  zipfileWrite16(a, pCDS->nFile);
  zipfileWrite16(a, pCDS->nExtra);
  zipfileWrite16(a, pCDS->nComment);
  zipfileWrite16(a, pCDS->iDiskStart);
  zipfileWrite16(a, pCDS->iInternalAttr);
  zipfileWrite32(a, pCDS->iExternalAttr);
  zipfileWrite32(a, pCDS->iOffset);

  memcpy(a, pCDS->zFile, pCDS->nFile);
  a += pCDS->nFile;

  if( pEntry->aExtra ){
    int n = (int)pCDS->nExtra + (int)pCDS->nComment;
    memcpy(a, pEntry->aExtra, n);
    a += n;
  }else{
    assert( pCDS->nExtra==9 );
    zipfileWrite16(a, ZIPFILE_EXTRA_TIMESTAMP);
    zipfileWrite16(a, 5);
    *a++ = 0x01;
    zipfileWrite32(a, pEntry->mUnixTime);
  }

  return a-aBuf;
}

static int zipfileCommit(sqlite3_vtab *pVtab){
  ZipfileTab *pTab = (ZipfileTab*)pVtab;
  int rc = SQLITE_OK;
  if( pTab->pWriteFd ){
    i64 iOffset = pTab->szCurrent;
    ZipfileEntry *p;
    ZipfileEOCD eocd;
    int nEntry = 0;

    /* Write out all entries */
    for(p=pTab->pFirstEntry; rc==SQLITE_OK && p; p=p->pNext){
      int n = zipfileSerializeCDS(p, pTab->aBuffer);
      rc = zipfileAppendData(pTab, pTab->aBuffer, n);
      nEntry++;
    }

    /* Write out the EOCD record */
    eocd.iDisk = 0;
    eocd.iFirstDisk = 0;
    eocd.nEntry = (u16)nEntry;
    eocd.nEntryTotal = (u16)nEntry;
    eocd.nSize = (u32)(pTab->szCurrent - iOffset);
    eocd.iOffset = (u32)iOffset;
    rc = zipfileAppendEOCD(pTab, &eocd);

    zipfileCleanupTransaction(pTab);
  }
  return rc;
}

static int zipfileRollback(sqlite3_vtab *pVtab){
  return zipfileCommit(pVtab);
}

static ZipfileCsr *zipfileFindCursor(ZipfileTab *pTab, i64 iId){
  ZipfileCsr *pCsr;
  for(pCsr=pTab->pCsrList; pCsr; pCsr=pCsr->pCsrNext){
    if( iId==pCsr->iId ) break;
  }
  return pCsr;
}

static void zipfileFunctionCds(
  sqlite3_context *context,
  int argc,
  sqlite3_value **argv
){
  ZipfileCsr *pCsr;
  ZipfileTab *pTab = (ZipfileTab*)sqlite3_user_data(context);
  assert( argc>0 );

  pCsr = zipfileFindCursor(pTab, sqlite3_value_int64(argv[0]));
  if( pCsr ){
    ZipfileCDS *p = &pCsr->pCurrent->cds;
    char *zRes = sqlite3_mprintf("{"
        "\"version-made-by\" : %u, "
        "\"version-to-extract\" : %u, "
        "\"flags\" : %u, "
        "\"compression\" : %u, "
        "\"time\" : %u, "
        "\"date\" : %u, "
        "\"crc32\" : %u, "
        "\"compressed-size\" : %u, "
        "\"uncompressed-size\" : %u, "
        "\"file-name-length\" : %u, "
        "\"extra-field-length\" : %u, "
        "\"file-comment-length\" : %u, "
        "\"disk-number-start\" : %u, "
        "\"internal-attr\" : %u, "
        "\"external-attr\" : %u, "
        "\"offset\" : %u }",
        (u32)p->iVersionMadeBy, (u32)p->iVersionExtract,
        (u32)p->flags, (u32)p->iCompression,
        (u32)p->mTime, (u32)p->mDate,
        (u32)p->crc32, (u32)p->szCompressed,
        (u32)p->szUncompressed, (u32)p->nFile,
        (u32)p->nExtra, (u32)p->nComment,
        (u32)p->iDiskStart, (u32)p->iInternalAttr,
        (u32)p->iExternalAttr, (u32)p->iOffset
    );

    if( zRes==0 ){
      sqlite3_result_error_nomem(context);
    }else{
      sqlite3_result_text(context, zRes, -1, SQLITE_TRANSIENT);
      sqlite3_free(zRes);
    }
  }
}


/*
** xFindFunction method.
*/
static int zipfileFindFunction(
  sqlite3_vtab *pVtab,            /* Virtual table handle */
  int nArg,                       /* Number of SQL function arguments */
  const char *zName,              /* Name of SQL function */
  void (**pxFunc)(sqlite3_context*,int,sqlite3_value**), /* OUT: Result */
  void **ppArg                    /* OUT: User data for *pxFunc */
){
  if( nArg>0 ){
    if( sqlite3_stricmp("zipfile_cds", zName)==0 ){
      *pxFunc = zipfileFunctionCds;
      *ppArg = (void*)pVtab;
      return 1;
    }
  }

  return 0;
}

/*
** Register the "zipfile" virtual table.
*/
static int zipfileRegister(sqlite3 *db){
  static sqlite3_module zipfileModule = {
    1,                         /* iVersion */
    zipfileConnect,            /* xCreate */
    zipfileConnect,            /* xConnect */
    zipfileBestIndex,          /* xBestIndex */
    zipfileDisconnect,         /* xDisconnect */
    zipfileDisconnect,         /* xDestroy */
    zipfileOpen,               /* xOpen - open a cursor */
    zipfileClose,              /* xClose - close a cursor */
    zipfileFilter,             /* xFilter - configure scan constraints */
    zipfileNext,               /* xNext - advance a cursor */
    zipfileEof,                /* xEof - check for end of scan */
    zipfileColumn,             /* xColumn - read data */
    zipfileRowid,              /* xRowid - read data */
    zipfileUpdate,             /* xUpdate */
    zipfileBegin,              /* xBegin */
    0,                         /* xSync */
    zipfileCommit,             /* xCommit */
    zipfileRollback,           /* xRollback */
    zipfileFindFunction,       /* xFindMethod */
    0,                         /* xRename */
  };

  int rc = sqlite3_create_module(db, "zipfile"  , &zipfileModule, 0);
  if( rc==SQLITE_OK ){
    rc = sqlite3_overload_function(db, "zipfile_cds", -1);
  }
  return rc;
}
#else         /* SQLITE_OMIT_VIRTUALTABLE */
# define zipfileRegister(x) SQLITE_OK
#endif

#ifdef _WIN32
__declspec(dllexport)
#endif
int sqlite3_zipfile_init(
  sqlite3 *db, 
  char **pzErrMsg, 
  const sqlite3_api_routines *pApi
){
  SQLITE_EXTENSION_INIT2(pApi);
  (void)pzErrMsg;  /* Unused parameter */
  return zipfileRegister(db);
}

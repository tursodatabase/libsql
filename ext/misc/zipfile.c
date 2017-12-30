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
*/
#include "sqlite3ext.h"
SQLITE_EXTENSION_INIT1
#include <stdio.h>
#include <string.h>
#include <assert.h>

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <dirent.h>
#include <time.h>
#include <utime.h>
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

#define ZIPFILE_SCHEMA "CREATE TABLE y("                           \
  "name,      /* Name of file in zip archive */"                   \
  "mode,      /* POSIX mode for file */"                           \
  "mtime,     /* Last modification time in seconds since epoch */" \
  "sz,        /* Size of object */"                                \
  "data,      /* Data stored in zip file (possibly compressed) */" \
  "method,    /* Compression method (integer) */"                  \
  "f HIDDEN   /* Name of zip file */"                              \
");"

#define ZIPFILE_F_COLUMN_IDX 6    /* Index of column "f" in the above */
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

/* 
** Cursor type for recursively iterating through a directory structure.
*/
typedef struct ZipfileCsr ZipfileCsr;

struct ZipfileCsr {
  sqlite3_vtab_cursor base;  /* Base class - must be first */
  i64 iRowid;                /* Rowid for current row */
  FILE *pFile;               /* Zip file */
  i64 nByte;                 /* Size of zip file on disk */
  int bEof;                  /* True when at EOF */
  i64 iNextOff;              /* Offset of next record in central directory */
  ZipfileEOCD eocd;          /* Parse of central directory record */
  ZipfileCDS cds;            /* Central Directory Structure */
  ZipfileLFH lfh;            /* Local File Header for current entry */
  i64 iDataOff;              /* Offset in zipfile to data */
  u32 mTime;                 /* Extended mtime value */
  int flags;                 /* Flags byte (see below for bits) */
};

/*
** Values for ZipfileCsr.flags.
*/
#define ZIPFILE_MTIME_VALID 0x0001

typedef struct ZipfileEntry ZipfileEntry;
struct ZipfileEntry {
  char *zPath;               /* Path of zipfile entry */
  i64 iRowid;                /* Rowid for this value if queried */
  u8 *aCdsEntry;             /* Buffer containing entire CDS entry */
  int nCdsEntry;             /* Size of buffer aCdsEntry[] in bytes */
  ZipfileEntry *pNext;       /* Next element in in-memory CDS */
};

typedef struct ZipfileTab ZipfileTab;
struct ZipfileTab {
  sqlite3_vtab base;         /* Base class - must be first */
  char *zFile;               /* Zip file this table accesses (may be NULL) */
  u8 *aBuffer;               /* Temporary buffer used for various tasks */

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
    nFile = strlen(zFile)+1;
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

/*
** This method is the destructor for zipfile vtab objects.
*/
static int zipfileDisconnect(sqlite3_vtab *pVtab){
  sqlite3_free(pVtab);
  return SQLITE_OK;
}

/*
** Constructor for a new ZipfileCsr object.
*/
static int zipfileOpen(sqlite3_vtab *p, sqlite3_vtab_cursor **ppCsr){
  ZipfileCsr *pCsr;
  pCsr = sqlite3_malloc(sizeof(*pCsr));
  *ppCsr = (sqlite3_vtab_cursor*)pCsr;
  if( pCsr==0 ){
    return SQLITE_NOMEM;
  }
  memset(pCsr, 0, sizeof(*pCsr));
  return SQLITE_OK;
}

/*
** Reset a cursor back to the state it was in when first returned
** by zipfileOpen().
*/
static void zipfileResetCursor(ZipfileCsr *pCsr){
  sqlite3_free(pCsr->cds.zFile);
  pCsr->cds.zFile = 0;
  pCsr->iRowid = 0;
  pCsr->bEof = 0;
  if( pCsr->pFile ){
    fclose(pCsr->pFile);
    pCsr->pFile = 0;
  }
}

/*
** Destructor for an ZipfileCsr.
*/
static int zipfileClose(sqlite3_vtab_cursor *cur){
  ZipfileCsr *pCsr = (ZipfileCsr*)cur;
  zipfileResetCursor(pCsr);
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
  fseek(pFile, iOff, SEEK_SET);
  n = fread(aRead, 1, nRead, pFile);
  if( n!=nRead ){
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
  fseek(pTab->pWriteFd, pTab->szCurrent, SEEK_SET);
  n = fwrite(aWrite, 1, nWrite, pTab->pWriteFd);
  if( n!=nWrite ){
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
#define ZIPFILE_CDS_FIXED_SZ  46
#define ZIPFILE_CDS_NFILE_OFF 28

static int zipfileReadCDS(ZipfileCsr *pCsr){
  char **pzErr = &pCsr->base.pVtab->zErrMsg;
  u8 *aRead = zipfileCsrBuffer(pCsr);
  int rc;

  sqlite3_free(pCsr->cds.zFile);
  pCsr->cds.zFile = 0;

  rc = zipfileReadData(
      pCsr->pFile, aRead, ZIPFILE_CDS_FIXED_SZ, pCsr->iNextOff, pzErr
  );
  if( rc==SQLITE_OK ){
    u32 sig = zipfileRead32(aRead);
    if( sig!=ZIPFILE_SIGNATURE_CDS ){
      zipfileSetErrmsg(pCsr,"failed to read CDS at offset %lld",pCsr->iNextOff);
      rc = SQLITE_ERROR;
    }else{
      int nRead;
      pCsr->cds.iVersionMadeBy = zipfileRead16(aRead);
      pCsr->cds.iVersionExtract = zipfileRead16(aRead);
      pCsr->cds.flags = zipfileRead16(aRead);
      pCsr->cds.iCompression = zipfileRead16(aRead);
      pCsr->cds.mTime = zipfileRead16(aRead);
      pCsr->cds.mDate = zipfileRead16(aRead);
      pCsr->cds.crc32 = zipfileRead32(aRead);
      pCsr->cds.szCompressed = zipfileRead32(aRead);
      pCsr->cds.szUncompressed = zipfileRead32(aRead);
      assert( aRead==zipfileCsrBuffer(pCsr)+ZIPFILE_CDS_NFILE_OFF );
      pCsr->cds.nFile = zipfileRead16(aRead);
      pCsr->cds.nExtra = zipfileRead16(aRead);
      pCsr->cds.nComment = zipfileRead16(aRead);
      pCsr->cds.iDiskStart = zipfileRead16(aRead);
      pCsr->cds.iInternalAttr = zipfileRead16(aRead);
      pCsr->cds.iExternalAttr = zipfileRead32(aRead);
      pCsr->cds.iOffset = zipfileRead32(aRead);

      assert( aRead==zipfileCsrBuffer(pCsr)+ZIPFILE_CDS_FIXED_SZ );

      nRead = pCsr->cds.nFile + pCsr->cds.nExtra;
      aRead = zipfileCsrBuffer(pCsr);
      pCsr->iNextOff += ZIPFILE_CDS_FIXED_SZ;
      rc = zipfileReadData(pCsr->pFile, aRead, nRead, pCsr->iNextOff, pzErr);

      if( rc==SQLITE_OK ){
        pCsr->cds.zFile = sqlite3_mprintf("%.*s", (int)pCsr->cds.nFile, aRead);
        pCsr->iNextOff += pCsr->cds.nFile;
        pCsr->iNextOff += pCsr->cds.nExtra;
        pCsr->iNextOff += pCsr->cds.nComment;
      }

      /* Scan the cds.nExtra bytes of "extra" fields for any that can
      ** be interpreted. The general format of an extra field is:
      **
      **   Header ID    2 bytes
      **   Data Size    2 bytes
      **   Data         N bytes
      **
      */
      if( rc==SQLITE_OK ){
        u8 *p = &aRead[pCsr->cds.nFile];
        u8 *pEnd = &p[pCsr->cds.nExtra];

        while( p<pEnd ){
          u16 id = zipfileRead16(p);
          u16 nByte = zipfileRead16(p);

          switch( id ){
            case ZIPFILE_EXTRA_TIMESTAMP: {
              u8 b = p[0];
              if( b & 0x01 ){     /* 0x01 -> modtime is present */
                pCsr->mTime = zipfileGetU32(&p[1]);
                pCsr->flags |= ZIPFILE_MTIME_VALID;
              }
              break;
            }
          }

          p += nByte;
        }
      }
    }
  }

  return rc;
}

static int zipfileReadLFH(ZipfileCsr *pCsr){
  char **pzErr = &pCsr->base.pVtab->zErrMsg;
  static const int szFix = ZIPFILE_LFH_FIXED_SZ;
  u8 *aRead = zipfileCsrBuffer(pCsr);
  int rc;

  rc = zipfileReadData(pCsr->pFile, aRead, szFix, pCsr->cds.iOffset, pzErr);
  if( rc==SQLITE_OK ){
    u32 sig = zipfileRead32(aRead);
    if( sig!=ZIPFILE_SIGNATURE_LFH ){
      zipfileSetErrmsg(pCsr, "failed to read LFH at offset %d", 
          (int)pCsr->cds.iOffset
      );
      rc = SQLITE_ERROR;
    }else{
      pCsr->lfh.iVersionExtract = zipfileRead16(aRead);
      pCsr->lfh.flags = zipfileRead16(aRead);
      pCsr->lfh.iCompression = zipfileRead16(aRead);
      pCsr->lfh.mTime = zipfileRead16(aRead);
      pCsr->lfh.mDate = zipfileRead16(aRead);
      pCsr->lfh.crc32 = zipfileRead32(aRead);
      pCsr->lfh.szCompressed = zipfileRead32(aRead);
      pCsr->lfh.szUncompressed = zipfileRead32(aRead);
      pCsr->lfh.nFile = zipfileRead16(aRead);
      pCsr->lfh.nExtra = zipfileRead16(aRead);
      assert( aRead==zipfileCsrBuffer(pCsr)+szFix );
      pCsr->iDataOff = pCsr->cds.iOffset+szFix+pCsr->lfh.nFile+pCsr->lfh.nExtra;
    }
  }

  return rc;
}


/*
** Advance an ZipfileCsr to its next row of output.
*/
static int zipfileNext(sqlite3_vtab_cursor *cur){
  ZipfileCsr *pCsr = (ZipfileCsr*)cur;
  i64 iEof = pCsr->eocd.iOffset + pCsr->eocd.nSize;
  int rc = SQLITE_OK;

  if( pCsr->iNextOff>=iEof ){
    pCsr->bEof = 1;
  }else{
    pCsr->iRowid++;
    pCsr->flags = 0;
    rc = zipfileReadCDS(pCsr);
    if( rc==SQLITE_OK ){
      rc = zipfileReadLFH(pCsr);
    }
  }
  return rc;
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
static time_t zipfileMtime(ZipfileCsr *pCsr){
  struct tm t;
  memset(&t, 0, sizeof(t));
  t.tm_sec = (pCsr->cds.mTime & 0x1F)*2;
  t.tm_min = (pCsr->cds.mTime >> 5) & 0x2F;
  t.tm_hour = (pCsr->cds.mTime >> 11) & 0x1F;

  t.tm_mday = (pCsr->cds.mDate & 0x1F);
  t.tm_mon = ((pCsr->cds.mDate >> 5) & 0x0F) - 1;
  t.tm_year = 80 + ((pCsr->cds.mDate >> 9) & 0x7F);

  return mktime(&t);
}

static void zipfileMtimeToDos(ZipfileCDS *pCds, u32 mTime){
  time_t t = (time_t)mTime;
  struct tm res;
  localtime_r(&t, &res);

  pCds->mTime = 
    (res.tm_sec / 2) + 
    (res.tm_min << 5) +
    (res.tm_hour << 11);

  pCds->mDate = 
    (res.tm_mday-1) +
    ((res.tm_mon+1) << 5) +
    ((res.tm_year-80) << 9);
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
  int rc = SQLITE_OK;
  switch( i ){
    case 0:   /* name */
      sqlite3_result_text(ctx, pCsr->cds.zFile, -1, SQLITE_TRANSIENT);
      break;
    case 1:   /* mode */
      /* TODO: Whether or not the following is correct surely depends on
      ** the platform on which the archive was created.  */
      sqlite3_result_int(ctx, pCsr->cds.iExternalAttr >> 16);
      break;
    case 2: { /* mtime */
      if( pCsr->flags & ZIPFILE_MTIME_VALID ){
        sqlite3_result_int64(ctx, pCsr->mTime);
      }else{
        sqlite3_result_int64(ctx, zipfileMtime(pCsr));
      }
      break;
    }
    case 3: { /* sz */
      sqlite3_result_int64(ctx, pCsr->cds.szUncompressed);
      break;
    }
    case 4: { /* data */
      int sz = pCsr->cds.szCompressed;
      if( sz>0 ){
        u8 *aBuf = sqlite3_malloc(sz);
        if( aBuf==0 ){
          rc = SQLITE_NOMEM;
        }else{
          rc = zipfileReadData(pCsr->pFile, aBuf, sz, pCsr->iDataOff,
              &pCsr->base.pVtab->zErrMsg
          );
        }
        if( rc==SQLITE_OK ){
          sqlite3_result_blob(ctx, aBuf, sz, SQLITE_TRANSIENT);
          sqlite3_free(aBuf);
        }
      }
      break;
    }
    case 5:   /* method */
      sqlite3_result_int(ctx, pCsr->cds.iCompression);
      break;
  }

  return SQLITE_OK;
}

/*
** Return the rowid for the current row. In this implementation, the
** first row returned is assigned rowid value 1, and each subsequent
** row a value 1 more than that of the previous.
*/
static int zipfileRowid(sqlite3_vtab_cursor *cur, sqlite_int64 *pRowid){
  ZipfileCsr *pCsr = (ZipfileCsr*)cur;
  *pRowid = pCsr->iRowid;
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
  FILE *pFile,                    /* Read from this file */
  ZipfileEOCD *pEOCD              /* Object to populate */
){
  u8 *aRead = pTab->aBuffer;      /* Temporary buffer */
  i64 szFile;                     /* Total size of file in bytes */
  int nRead;                      /* Bytes to read from file */
  i64 iOff;                       /* Offset to read from */

  fseek(pFile, 0, SEEK_END);
  szFile = (i64)ftell(pFile);
  if( szFile==0 ){
    return SQLITE_EMPTY;
  }
  nRead = (int)(MIN(szFile, ZIPFILE_BUFFER_SIZE));
  iOff = szFile - nRead;

  int rc = zipfileReadData(pFile, aRead, nRead, iOff, &pTab->base.zErrMsg);
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

#if 0
    printf("iDisk=%d  iFirstDisk=%d  nEntry=%d  "
           "nEntryTotal=%d  nSize=%d  iOffset=%d", 
           (int)pEOCD->iDisk, (int)pEOCD->iFirstDisk, (int)pEOCD->nEntry,
           (int)pEOCD->nEntryTotal, (int)pEOCD->nSize, (int)pEOCD->iOffset
    );
#endif
  }

  return SQLITE_OK;
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

  zipfileResetCursor(pCsr);

  assert( idxNum==argc && (idxNum==0 || idxNum==1) );
  if( idxNum==0 ){
    ZipfileTab *pTab = (ZipfileTab*)cur->pVtab;
    zFile = pTab->zFile;
    if( zFile==0 ){
      /* Error. This is an eponymous virtual table and the user has not 
      ** supplied a file name. */
      zipfileSetErrmsg(pCsr, "table function zipfile() requires an argument");
      return SQLITE_ERROR;
    }
  }else{
    zFile = (const char*)sqlite3_value_text(argv[0]);
  }
  pCsr->pFile = fopen(zFile, "rb");
  if( pCsr->pFile==0 ){
    zipfileSetErrmsg(pCsr, "cannot open file: %s", zFile);
    rc = SQLITE_ERROR;
  }else{
    rc = zipfileReadEOCD(pTab, pCsr->pFile, &pCsr->eocd);
    if( rc==SQLITE_OK ){
      pCsr->iNextOff = pCsr->eocd.iOffset;
      rc = zipfileNext(cur);
    }else if( rc==SQLITE_EMPTY ){
      rc = SQLITE_OK;
      pCsr->bEof = 1;
    }
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

/*
** Add object pNew to the end of the linked list that begins at
** ZipfileTab.pFirstEntry and ends with pLastEntry.
*/
static void zipfileAddEntry(ZipfileTab *pTab, ZipfileEntry *pNew){
  assert( (pTab->pFirstEntry==0)==(pTab->pLastEntry==0) );
  assert( pNew->pNext==0 );
  if( pTab->pFirstEntry==0 ){
    pTab->pFirstEntry = pTab->pLastEntry = pNew;
  }else{
    assert( pTab->pLastEntry->pNext==0 );
    pTab->pLastEntry->pNext = pNew;
    pTab->pLastEntry = pNew;
  }
}

static int zipfileLoadDirectory(ZipfileTab *pTab){
  ZipfileEOCD eocd;
  int rc;

  rc = zipfileReadEOCD(pTab, pTab->pWriteFd, &eocd);
  if( rc==SQLITE_OK ){
    int i;
    int iOff = 0;
    u8 *aBuf = sqlite3_malloc(eocd.nSize);
    if( aBuf==0 ){
      rc = SQLITE_NOMEM;
    }else{
      rc = zipfileReadData(
          pTab->pWriteFd, aBuf, eocd.nSize, eocd.iOffset, &pTab->base.zErrMsg
      );
    }

    for(i=0; rc==SQLITE_OK && i<eocd.nEntry; i++){
      u16 nFile;
      u16 nExtra;
      u16 nComment;
      ZipfileEntry *pNew;
      u8 *aRec = &aBuf[iOff];

      nFile = zipfileGetU16(&aRec[ZIPFILE_CDS_NFILE_OFF]);
      nExtra = zipfileGetU16(&aRec[ZIPFILE_CDS_NFILE_OFF+2]);
      nComment = zipfileGetU16(&aRec[ZIPFILE_CDS_NFILE_OFF+4]);

      pNew = sqlite3_malloc(
          sizeof(ZipfileEntry) 
        + nFile+1 
        + ZIPFILE_CDS_FIXED_SZ+nFile+nExtra+nComment
      );
      if( pNew==0 ){
        rc = SQLITE_NOMEM;
      }else{
        pNew->zPath = (char*)&pNew[1];
        memcpy(pNew->zPath, &aBuf[ZIPFILE_CDS_FIXED_SZ], nFile);
        pNew->zPath[nFile] = '\0';
        pNew->aCdsEntry = (u8*)&pNew->zPath[nFile+1];
        pNew->nCdsEntry = ZIPFILE_CDS_FIXED_SZ+nFile+nExtra+nComment;
        pNew->pNext = 0;
        memcpy(pNew->aCdsEntry, aRec, pNew->nCdsEntry);
        zipfileAddEntry(pTab, pNew);
      }

      iOff += ZIPFILE_CDS_FIXED_SZ+nFile+nExtra+nComment;
    }

    sqlite3_free(aBuf);
  }else if( rc==SQLITE_EMPTY ){
    rc = SQLITE_OK;
  }

  return rc;
}

static ZipfileEntry *zipfileNewEntry(
  ZipfileCDS *pCds,               /* Values for fixed size part of CDS */
  const char *zPath,              /* Path for new entry */
  int nPath,                      /* strlen(zPath) */
  u32 mTime                       /* Modification time (or 0) */
){
  u8 *aWrite;
  ZipfileEntry *pNew;
  pCds->nFile = nPath;
  pCds->nExtra = mTime ? 9 : 0;
  pNew = (ZipfileEntry*)sqlite3_malloc(
    sizeof(ZipfileEntry) + 
    nPath+1 + 
    ZIPFILE_CDS_FIXED_SZ + nPath + pCds->nExtra
  );

  if( pNew ){
    pNew->zPath = (char*)&pNew[1];
    pNew->aCdsEntry = (u8*)&pNew->zPath[nPath+1];
    pNew->nCdsEntry = ZIPFILE_CDS_FIXED_SZ + nPath + pCds->nExtra;
    pNew->pNext = 0;
    memcpy(pNew->zPath, zPath, nPath+1);

    aWrite = pNew->aCdsEntry;
    zipfileWrite32(aWrite, ZIPFILE_SIGNATURE_CDS);
    zipfileWrite16(aWrite, pCds->iVersionMadeBy);
    zipfileWrite16(aWrite, pCds->iVersionExtract);
    zipfileWrite16(aWrite, pCds->flags);
    zipfileWrite16(aWrite, pCds->iCompression);
    zipfileWrite16(aWrite, pCds->mTime);
    zipfileWrite16(aWrite, pCds->mDate);
    zipfileWrite32(aWrite, pCds->crc32);
    zipfileWrite32(aWrite, pCds->szCompressed);
    zipfileWrite32(aWrite, pCds->szUncompressed);
    zipfileWrite16(aWrite, pCds->nFile);
    zipfileWrite16(aWrite, pCds->nExtra);
    zipfileWrite16(aWrite, pCds->nComment);      assert( pCds->nComment==0 );
    zipfileWrite16(aWrite, pCds->iDiskStart);
    zipfileWrite16(aWrite, pCds->iInternalAttr);
    zipfileWrite32(aWrite, pCds->iExternalAttr);
    zipfileWrite32(aWrite, pCds->iOffset);
    assert( aWrite==&pNew->aCdsEntry[ZIPFILE_CDS_FIXED_SZ] );
    memcpy(aWrite, zPath, nPath);
    if( pCds->nExtra ){
      aWrite += nPath;
      zipfileWrite16(aWrite, ZIPFILE_EXTRA_TIMESTAMP);
      zipfileWrite16(aWrite, 5);
      *aWrite++ = 0x01;
      zipfileWrite32(aWrite, mTime);
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

  zipfileWrite32(aBuf, ZIPFILE_SIGNATURE_LFH);
  zipfileWrite16(aBuf, pCds->iVersionExtract);
  zipfileWrite16(aBuf, pCds->flags);
  zipfileWrite16(aBuf, pCds->iCompression);
  zipfileWrite16(aBuf, pCds->mTime);
  zipfileWrite16(aBuf, pCds->mDate);
  zipfileWrite32(aBuf, pCds->crc32);
  zipfileWrite32(aBuf, pCds->szCompressed);
  zipfileWrite32(aBuf, pCds->szUncompressed);
  zipfileWrite16(aBuf, nPath);
  zipfileWrite16(aBuf, pCds->nExtra);
  assert( aBuf==&pTab->aBuffer[ZIPFILE_LFH_FIXED_SZ] );
  rc = zipfileAppendData(pTab, pTab->aBuffer, aBuf - pTab->aBuffer);
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

static int zipfileGetMode(ZipfileTab *pTab, sqlite3_value *pVal, int *pMode){
  const char *z = (const char*)sqlite3_value_text(pVal);
  int mode = 0;
  if( z==0 || (z[0]>=0 && z[0]<=9) ){
    mode = sqlite3_value_int(pVal);
  }else{
    const char zTemplate[10] = "-rwxrwxrwx";
    int i;
    if( strlen(z)!=10 ) goto parse_error;
    switch( z[0] ){
      case '-': mode |= S_IFREG; break;
      case 'd': mode |= S_IFDIR; break;
      case 'l': mode |= S_IFLNK; break;
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

  int mode;                       /* Mode for new entry */
  i64 mTime;                      /* Modification time for new entry */
  i64 sz;                         /* Uncompressed size */
  const char *zPath;              /* Path for new entry */
  int nPath;                      /* strlen(zPath) */
  const u8 *pData;                /* Pointer to buffer containing content */
  int nData;                      /* Size of pData buffer in bytes */
  int iMethod = 0;                /* Compression method for new entry */
  u8 *pFree = 0;                  /* Free this */
  ZipfileCDS cds;                 /* New Central Directory Structure entry */

  if( sqlite3_value_type(apVal[0])!=SQLITE_NULL ){
    pTab->base.zErrMsg = sqlite3_mprintf(
        "zipfile: UPDATE/DELETE not yet supported"
    );
    return SQLITE_ERROR;
  }

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
  if( pTab->pWriteFd==0 ){
    pTab->pWriteFd = fopen(pTab->zFile, "ab+");
    if( pTab->pWriteFd==0 ){
      pTab->base.zErrMsg = sqlite3_mprintf(
          "zipfile: failed to open file %s for writing", pTab->zFile
      );
      return SQLITE_ERROR;
    }
    fseek(pTab->pWriteFd, 0, SEEK_END);
    pTab->szCurrent = pTab->szOrig = (i64)ftell(pTab->pWriteFd);
    rc = zipfileLoadDirectory(pTab);
    if( rc!=SQLITE_OK ) return rc;
  }

  zPath = (const char*)sqlite3_value_text(apVal[2]);
  nPath = strlen(zPath);
  rc = zipfileGetMode(pTab, apVal[3], &mode);
  if( rc!=SQLITE_OK ) return rc;
  mTime = sqlite3_value_int64(apVal[4]);
  sz = sqlite3_value_int(apVal[5]);
  pData = sqlite3_value_blob(apVal[6]);
  nData = sqlite3_value_bytes(apVal[6]);

  /* If a NULL value is inserted into the 'method' column, do automatic
  ** compression. */
  if( nData>0 && sqlite3_value_type(apVal[7])==SQLITE_NULL ){
    pFree = (u8*)sqlite3_malloc(nData);
    if( pFree==0 ){
      rc = SQLITE_NOMEM;
    }else{
      int res;
      z_stream str;
      memset(&str, 0, sizeof(str));
      str.next_in = (z_const Bytef*)pData;
      str.avail_in = nData;
      str.next_out = pFree;
      str.avail_out = nData;
      deflateInit2(&str, 9, Z_DEFLATED, -15, 8, Z_DEFAULT_STRATEGY);
      res = deflate(&str, Z_FINISH);
      if( res==Z_STREAM_END ){
        pData = pFree;
        nData = str.total_out;
        iMethod = 8;
      }else if( res!=Z_OK ){
        pTab->base.zErrMsg = sqlite3_mprintf("zipfile: deflate() error");
        rc = SQLITE_ERROR;
      }
      deflateEnd(&str);
    }
  }else{
    iMethod = sqlite3_value_int(apVal[7]);
    if( iMethod<0 || iMethod>65535 ){
      pTab->base.zErrMsg = sqlite3_mprintf(
          "zipfile: invalid compression method: %d", iMethod
      );
      rc = SQLITE_ERROR;
    }
  }

  if( rc==SQLITE_OK ){
    /* Create the new CDS record. */
    memset(&cds, 0, sizeof(cds));
    cds.iVersionMadeBy = ZIPFILE_NEWENTRY_MADEBY;
    cds.iVersionExtract = ZIPFILE_NEWENTRY_REQUIRED;
    cds.flags = ZIPFILE_NEWENTRY_FLAGS;
    cds.iCompression = iMethod;
    zipfileMtimeToDos(&cds, (u32)mTime);
    cds.crc32 = crc32(0, pData, nData);
    cds.szCompressed = nData;
    cds.szUncompressed = sz;
    cds.iExternalAttr = (mode<<16);
    cds.iOffset = pTab->szCurrent;
    pNew = zipfileNewEntry(&cds, zPath, nPath, (u32)mTime);
    if( pNew==0 ){
      rc = SQLITE_NOMEM;
    }else{
      zipfileAddEntry(pTab, pNew);
    }
  }

  /* Append the new header+file to the archive */
  if( rc==SQLITE_OK ){
    rc = zipfileAppendEntry(pTab, &cds, zPath, nPath, pData, nData, (u32)mTime);
  }

  sqlite3_free(pFree);
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
  return zipfileAppendData(pTab, pTab->aBuffer, aBuf - pTab->aBuffer);
}

static void zipfileCleanupTransaction(ZipfileTab *pTab){
  ZipfileEntry *pEntry;
  ZipfileEntry *pNext;

  for(pEntry=pTab->pFirstEntry; pEntry; pEntry=pNext){
    pNext = pEntry->pNext;
    sqlite3_free(pEntry);
  }
  pTab->pFirstEntry = 0;
  pTab->pLastEntry = 0;
  fclose(pTab->pWriteFd);
  pTab->pWriteFd = 0;
  pTab->szCurrent = 0;
  pTab->szOrig = 0;
}

static int zipfileBegin(sqlite3_vtab *pVtab){
  return SQLITE_OK;
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
      rc = zipfileAppendData(pTab, p->aCdsEntry, p->nCdsEntry);
      nEntry++;
    }

    /* Write out the EOCD record */
    eocd.iDisk = 0;
    eocd.iFirstDisk = 0;
    eocd.nEntry = nEntry;
    eocd.nEntryTotal = nEntry;
    eocd.nSize = pTab->szCurrent - iOffset;;
    eocd.iOffset = iOffset;
    rc = zipfileAppendEOCD(pTab, &eocd);

    zipfileCleanupTransaction(pTab);
  }
  return rc;
}

static int zipfileRollback(sqlite3_vtab *pVtab){
  return zipfileCommit(pVtab);
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
    0,                         /* xFindMethod */
    0,                         /* xRename */
  };

  int rc = sqlite3_create_module(db, "zipfile"  , &zipfileModule, 0);
  return rc;
}
#else         /* SQLITE_OMIT_VIRTUALTABLE */
# define zipfileRegister(x) SQLITE_OK
#endif

/*
** zipfile_uncompress(DATA, SZ, METHOD)
*/
static void zipfileUncompressFunc(
  sqlite3_context *context,
  int argc,
  sqlite3_value **argv
){
  int iMethod;

  iMethod = sqlite3_value_int(argv[2]);
  if( iMethod==0 ){
    sqlite3_result_value(context, argv[0]);
  }else if( iMethod==8 ){
    Byte *res;
    int sz = sqlite3_value_int(argv[1]);
    z_stream str;
    memset(&str, 0, sizeof(str));
    str.next_in = (Byte*)sqlite3_value_blob(argv[0]);
    str.avail_in = sqlite3_value_bytes(argv[0]);
    res = str.next_out = (Byte*)sqlite3_malloc(sz);
    if( res==0 ){
      sqlite3_result_error_nomem(context);
    }else{
      int err;
      str.avail_out = sz;

      err = inflateInit2(&str, -15);
      if( err!=Z_OK ){
        zipfileCtxErrorMsg(context, "inflateInit2() failed (%d)", err);
      }else{
        err = inflate(&str, Z_NO_FLUSH);
        if( err!=Z_STREAM_END ){
          zipfileCtxErrorMsg(context, "inflate() failed (%d)", err);
        }else{
          sqlite3_result_blob(context, res, sz, SQLITE_TRANSIENT);
        }
      }
      sqlite3_free(res);
      inflateEnd(&str);
    }
  }else{
    zipfileCtxErrorMsg(context, "unrecognized compression method: %d", iMethod);
  }
}

#ifdef _WIN32
__declspec(dllexport)
#endif
int sqlite3_zipfile_init(
  sqlite3 *db, 
  char **pzErrMsg, 
  const sqlite3_api_routines *pApi
){
  int rc = SQLITE_OK;
  SQLITE_EXTENSION_INIT2(pApi);
  (void)pzErrMsg;  /* Unused parameter */
  rc = sqlite3_create_function(db, "zipfile_uncompress", 3,
      SQLITE_UTF8, 0, zipfileUncompressFunc, 0, 0
  );
  if( rc!=SQLITE_OK ) return rc;
  return zipfileRegister(db);
}


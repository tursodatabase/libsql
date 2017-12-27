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
  int flags;
  u8 *aBuffer;               /* Buffer used for various tasks */
};

#define ZIPFILE_MTIME_VALID 0x0001

typedef struct ZipfileTab ZipfileTab;
struct ZipfileTab {
  sqlite3_vtab base;         /* Base class - must be first */
};

/*
** Construct a new ZipfileTab virtual table object.
*/
static int zipfileConnect(
  sqlite3 *db,
  void *pAux,
  int argc, const char *const*argv,
  sqlite3_vtab **ppVtab,
  char **pzErr
){
  ZipfileTab *pNew = 0;
  int rc;

  rc = sqlite3_declare_vtab(db, ZIPFILE_SCHEMA);
  if( rc==SQLITE_OK ){
    pNew = (ZipfileTab*)sqlite3_malloc( sizeof(*pNew) );
    if( pNew==0 ) return SQLITE_NOMEM;
    memset(pNew, 0, sizeof(*pNew));
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
  pCsr = sqlite3_malloc( sizeof(*pCsr) + ZIPFILE_BUFFER_SIZE);
  if( pCsr==0 ) return SQLITE_NOMEM;
  memset(pCsr, 0, sizeof(*pCsr));
  pCsr->aBuffer = (u8*)&pCsr[1];
  *ppCsr = &pCsr->base;
  return SQLITE_OK;
}

/*
** Reset a cursor back to the state it was in when first returned
** by zipfileOpen().
*/
static void zipfileResetCursor(ZipfileCsr *pCsr){
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

static int zipfileReadData(ZipfileCsr *pCsr, u8 *aRead, int nRead, i64 iOff){
  size_t n;
  fseek(pCsr->pFile, iOff, SEEK_SET);
  n = fread(aRead, 1, nRead, pCsr->pFile);
  if( n!=nRead ){
    zipfileSetErrmsg(pCsr, "error in fread()");
    return SQLITE_ERROR;
  }
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

#define zipfileRead32(aBuf) ( aBuf+=4, zipfileGetU32(aBuf-4) )
#define zipfileRead16(aBuf) ( aBuf+=2, zipfileGetU16(aBuf-2) )

static int zipfileReadCDS(ZipfileCsr *pCsr){
  static const int szFix = 46;    /* Size of fixed-size part of CDS */
  u8 *aRead = pCsr->aBuffer;
  int rc;

  rc = zipfileReadData(pCsr, aRead, szFix, pCsr->iNextOff);
  if( rc==SQLITE_OK ){
    u32 sig = zipfileRead32(aRead);
    if( sig!=0x02014b50 ){
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
      pCsr->cds.nFile = zipfileRead16(aRead);
      pCsr->cds.nExtra = zipfileRead16(aRead);
      pCsr->cds.nComment = zipfileRead16(aRead);
      pCsr->cds.iDiskStart = zipfileRead16(aRead);
      pCsr->cds.iInternalAttr = zipfileRead16(aRead);
      pCsr->cds.iExternalAttr = zipfileRead32(aRead);
      pCsr->cds.iOffset = zipfileRead32(aRead);

      assert( aRead==&pCsr->aBuffer[szFix] );

      nRead = pCsr->cds.nFile + pCsr->cds.nExtra;
      aRead = pCsr->aBuffer;
      rc = zipfileReadData(pCsr, aRead, nRead, pCsr->iNextOff+szFix);

      if( rc==SQLITE_OK ){
        pCsr->cds.zFile = sqlite3_mprintf("%.*s", (int)pCsr->cds.nFile, aRead);
        pCsr->iNextOff += szFix;
        pCsr->iNextOff += pCsr->cds.nFile;
        pCsr->iNextOff += pCsr->cds.nExtra;
        pCsr->iNextOff += pCsr->cds.nComment;
      }

      /* Scan the "extra" fields */
      if( rc==SQLITE_OK ){
        u8 *p = &aRead[pCsr->cds.nFile];
        u8 *pEnd = &p[pCsr->cds.nExtra];

        while( p<pEnd ){
          u16 id = zipfileRead16(p);
          u16 nByte = zipfileRead16(p);

          switch( id ){
            case 0x5455: {        /* Extended timestamp */
              u8 b = p[0];
              if( b & 0x01 ){     /* 0x01 -> modtime is present */
                pCsr->mTime = zipfileGetU32(&p[1]);
                pCsr->flags |= ZIPFILE_MTIME_VALID;
              }
              break;
            }

            case 0x7875:          /* Info-ZIP Unix (new) */
              break;
          }

          p += nByte;
        }
      }
    }
  }

  return rc;
}

static int zipfileReadLFH(ZipfileCsr *pCsr){
  static const int szFix = 30;    /* Size of fixed-size part of LFH */
  u8 *aRead = pCsr->aBuffer;
  int rc;

  rc = zipfileReadData(pCsr, aRead, szFix, pCsr->cds.iOffset);
  if( rc==SQLITE_OK ){
    u32 sig = zipfileRead32(aRead);
    if( sig!=0x04034b50 ){
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
      assert( aRead==&pCsr->aBuffer[szFix] );
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
          rc = zipfileReadData(pCsr, aBuf, sz, pCsr->iDataOff);
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
** The zip file has been successfully opened (so pCsr->pFile is valid). 
** This function attempts to locate and read the End of central
** directory record from the file.
**
*/
static int zipfileReadEOCD(ZipfileCsr *pCsr, ZipfileEOCD *pEOCD){
  u8 *aRead = pCsr->aBuffer;
  int nRead = (int)(MIN(pCsr->nByte, ZIPFILE_BUFFER_SIZE));
  i64 iOff = pCsr->nByte - nRead;

  int rc = zipfileReadData(pCsr, aRead, nRead, iOff);
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
      zipfileSetErrmsg(pCsr, "cannot find end of central directory record");
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
  ZipfileCsr *pCsr = (ZipfileCsr*)cur;
  const char *zFile;              /* Zip file to scan */
  int rc = SQLITE_OK;             /* Return Code */

  zipfileResetCursor(pCsr);

  assert( idxNum==argc && (idxNum==0 || idxNum==1) );
  if( idxNum==0 ){
    /* Error. User did not supply a file name. */
    zipfileSetErrmsg(pCsr, "table function zipfile() requires an argument");
    return SQLITE_ERROR;
  }

  zFile = sqlite3_value_text(argv[0]);
  pCsr->pFile = fopen(zFile, "rb");
  if( pCsr->pFile==0 ){
    zipfileSetErrmsg(pCsr, "cannot open file: %s", zFile);
    rc = SQLITE_ERROR;
  }else{
    fseek(pCsr->pFile, 0, SEEK_END);
    pCsr->nByte = (i64)ftell(pCsr->pFile);
    rc = zipfileReadEOCD(pCsr, &pCsr->eocd);
    if( rc==SQLITE_OK ){
      pCsr->iNextOff = pCsr->eocd.iOffset;
      rc = zipfileNext(cur);
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
** Register the "zipfile" virtual table.
*/
static int zipfileRegister(sqlite3 *db){
  static sqlite3_module zipfileModule = {
    0,                         /* iVersion */
    0,                         /* xCreate */
    zipfileConnect,            /* xConnect */
    zipfileBestIndex,          /* xBestIndex */
    zipfileDisconnect,         /* xDisconnect */
    0,                         /* xDestroy */
    zipfileOpen,               /* xOpen - open a cursor */
    zipfileClose,              /* xClose - close a cursor */
    zipfileFilter,             /* xFilter - configure scan constraints */
    zipfileNext,               /* xNext - advance a cursor */
    zipfileEof,                /* xEof - check for end of scan */
    zipfileColumn,             /* xColumn - read data */
    zipfileRowid,              /* xRowid - read data */
    0,                         /* xUpdate */
    0,                         /* xBegin */
    0,                         /* xSync */
    0,                         /* xCommit */
    0,                         /* xRollback */
    0,                         /* xFindMethod */
    0,                         /* xRename */
  };

  int rc = sqlite3_create_module(db, "zipfile"  , &zipfileModule, 0);
  return rc;
}
#else         /* SQLITE_OMIT_VIRTUALTABLE */
# define zipfileRegister(x) SQLITE_OK
#endif

#include <zlib.h>

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


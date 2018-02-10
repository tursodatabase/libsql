/*
** 2018-02-10
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
******************************************************************************
*/

#include "sqlite3ext.h"
SQLITE_EXTENSION_INIT1
#ifndef SQLITE_OMIT_VIRTUALTABLE

#ifndef SQLITE_AMALGAMATION
typedef sqlite3_int64 i64;
typedef sqlite3_uint64 u64;
typedef unsigned char u8;
typedef unsigned short u16;
typedef unsigned long u32;
#define MIN(a,b) ((a)<(b) ? (a) : (b))

#if defined(SQLITE_COVERAGE_TEST) || defined(SQLITE_MUTATION_TEST)
# define ALWAYS(X)      (1)
# define NEVER(X)       (0)
#elif !defined(NDEBUG)
# define ALWAYS(X)      ((X)?1:(assert(0),0))
# define NEVER(X)       ((X)?(assert(0),1):0)
#else
# define ALWAYS(X)      (X)
# define NEVER(X)       (X)
#endif
#endif   /* SQLITE_AMALGAMATION */

#define ZONEFILE_MAGIC_NUMBER 0x464B3138

#define ZONEFILE_SZ_HEADER 26

#define ZONEFILE_DEFAULT_MAXAUTOFRAMESIZE (64*1024)
#define ZONEFILE_DEFAULT_ENCRYPTION       0
#define ZONEFILE_DEFAULT_COMPRESSION      0


#include <stdio.h>
#include <string.h>
#include <assert.h>

typedef struct ZonefileWrite ZonefileWrite;
struct ZonefileWrite {
  int compressionTypeIndexData;
  int compressionTypeContent;
  int encryptionType;
  int maxAutoFrameSize;
};

typedef struct ZonefileBuffer ZonefileBuffer;
struct ZonefileBuffer {
  u8 *a;
  int n;
  int nAlloc;
};

/*
** Set the error message contained in context ctx to the results of
** vprintf(zFmt, ...).
*/
static void zonefileCtxError(sqlite3_context *ctx, const char *zFmt, ...){
  char *zMsg = 0;
  va_list ap;
  va_start(ap, zFmt);
  zMsg = sqlite3_vmprintf(zFmt, ap);
  sqlite3_result_error(ctx, zMsg, -1);
  sqlite3_free(zMsg);
  va_end(ap);
}

static void zonefileTransferError(sqlite3_context *pCtx){
  sqlite3 *db = sqlite3_context_db_handle(pCtx);
  const char *zErr = sqlite3_errmsg(db);
  sqlite3_result_error(pCtx, zErr, -1);
}

static sqlite3_stmt *zonefilePrepare(
  sqlite3_context *pCtx,
  const char *zFmt,
  ...
){
  sqlite3_stmt *pRet = 0;
  va_list ap;
  char *zSql;
  va_start(ap, zFmt);
  zSql = sqlite3_vmprintf(zFmt, ap);
  if( zSql ){
    sqlite3 *db = sqlite3_context_db_handle(pCtx);
    int rc = sqlite3_prepare(db, zSql, -1, &pRet, 0);
    if( rc!=SQLITE_OK ){
      zonefileTransferError(pCtx);
    }
  }else{
    sqlite3_result_error_nomem(pCtx);
  }
  return pRet;
}

/*
** Return zero if the two SQL values passed as arguments are equal, or
** non-zero otherwise. Values with different types are considered unequal,
** even if they both contain the same numeric value (e.g. 2 and 2.0).
*/
static int zonefileCompareValue(sqlite3_value *p1, sqlite3_value *p2){
  int eType;
  assert( p1 );
  if( p2==0 ) return 1;
  eType = sqlite3_value_type(p1);
  if( sqlite3_value_type(p2)!=eType ) return 1;
  switch( eType ){
    case SQLITE_INTEGER:
      return sqlite3_value_int64(p1)==sqlite3_value_int64(p2);
    case SQLITE_FLOAT:
      return sqlite3_value_double(p1)==sqlite3_value_double(p2);
    case SQLITE_TEXT:
    case SQLITE_BLOB: {
      int n1 = sqlite3_value_bytes(p1);
      int n2 = sqlite3_value_bytes(p2);
      if( n1!=n2 ) return 1;
      return memcmp(sqlite3_value_blob(p1), sqlite3_value_blob(p2), n1);
    }
    default:
      assert( eType==SQLITE_NULL);
  }

  return 0;
}

int zonefileIsAutoFrame(sqlite3_value *pFrame){
  return (
      sqlite3_value_type(pFrame)==SQLITE_INTEGER 
   && sqlite3_value_int64(pFrame)==-1
  );
}

static int zonefileGetParams(
  sqlite3_context *pCtx,          /* Leave any error message here */
  const char *zJson,              /* JSON configuration parameter (or NULL) */
  ZonefileWrite *p                /* Populate this object before returning */
){
  memset(p, 0, sizeof(ZonefileWrite));
  p->maxAutoFrameSize = ZONEFILE_DEFAULT_MAXAUTOFRAMESIZE;
  return SQLITE_OK;
}

/*
** Check that there is room in buffer pBuf for at least nByte bytes more 
** data. If not, attempt to allocate more space. If the allocation attempt
** fails, leave an error message in context pCtx and return SQLITE_ERROR.
**
** If no error occurs, SQLITE_OK is returned.
*/
static int zonefileBufferGrow(
  sqlite3_context *pCtx, 
  ZonefileBuffer *pBuf, 
  int nByte
){
  int nReq = pBuf->n + nByte;
  if( nReq>pBuf->nAlloc ){
    u8 *aNew;
    int nNew = pBuf->nAlloc ? pBuf->nAlloc*2 : 128;
    while( nNew<nReq ) nNew = nNew*2;
    aNew = sqlite3_realloc(pBuf->a, nNew);
    if( aNew==0 ){
      sqlite3_result_error_nomem(pCtx);
      return SQLITE_ERROR;
    }
    pBuf->a = aNew;
    pBuf->nAlloc = nNew;
  }
  return SQLITE_OK;
}

static void zonefileBufferFree(ZonefileBuffer *pBuf){
  sqlite3_free(pBuf->a);
  memset(pBuf, 0, sizeof(ZonefileBuffer));
}

static void zonefilePut32(u8 *aBuf, u32 v){
  aBuf[0] = (v >> 24) & 0xFF;
  aBuf[1] = (v >> 16) & 0xFF;
  aBuf[2] = (v >>  8) & 0xFF;
  aBuf[3] = v & 0xFF;
}

static void zonefileAppend32(ZonefileBuffer *pBuf, u32 v){
  zonefilePut32(&pBuf->a[pBuf->n], v);
  pBuf->n += 4;
}

static void zonefileAppend64(ZonefileBuffer *pBuf, u64 v){
  zonefileAppend32(pBuf, v>>32);
  zonefileAppend32(pBuf, v & 0xFFFFFFFF);
}

static void zonefileAppendBlob(ZonefileBuffer *pBuf, const u8 *p, int n){
  memcpy(&pBuf->a[pBuf->n], p, n);
  pBuf->n += n;
}

static int zonefileWrite(FILE *pFd, const u8 *aBuf, int nBuf){
  size_t res = fwrite(aBuf, 1, nBuf, pFd);
  return res!=nBuf ? SQLITE_ERROR : SQLITE_OK;
}

/*
** Function:     zonefile_write(F,T[,J])
*/
static void zonefileWriteFunc(
  sqlite3_context *pCtx,       /* Context object */
  int objc,                       /* Number of SQL arguments */
  sqlite3_value **objv            /* Array of SQL arguments */
){
  const char *zFile = 0;          /* File to write to */
  const char *zTbl = 0;           /* Database object to read from */
  const char *zJson = 0;          /* JSON configuration parameters */
  ZonefileWrite sWrite;           /* Decoded JSON parameters */
  int nKey = 0;                   /* Number of keys in new zonefile */
  int nFrame = 0;                 /* Number of frames in new zonefile */
  int szFrame = 0;                /* Size of current frame */
  sqlite3_stmt *pStmt = 0;        /* SQL used to read data from source table */
  FILE *pFd = 0;
  int rc;
  sqlite3_value *pPrev = 0;

  ZonefileBuffer sFrameIdx = {0, 0, 0};
  ZonefileBuffer sKeyIdx = {0, 0, 0};
  ZonefileBuffer sFrames = {0, 0, 0};
  u8 aHdr[ZONEFILE_SZ_HEADER];    /* Space to assemble zonefile header */

  assert( objc==2 || objc==3 );
  zFile = (const char*)sqlite3_value_text(objv[0]);
  zTbl = (const char*)sqlite3_value_text(objv[1]);
  if( objc==3 ){
    zJson = (const char*)sqlite3_value_text(objv[2]);
  }
  if( zonefileGetParams(pCtx, zJson, &sWrite) ) return;

  /* Prepare the SQL statement used to read data from the source table. This
  ** also serves to verify the suitability of the source table schema. */
  pStmt = zonefilePrepare(pCtx, 
      "SELECT k, frame, v FROM %Q ORDER BY frame, idx, k", zTbl
  );
  if( pStmt==0 ) return;

  /* Open a file-handle used to write out the zonefile */ 
  pFd = fopen(zFile, "w");
  if( pFd==0 ){
    zonefileCtxError(pCtx, "error opening file \"%s\" (fopen())", zFile);
    sqlite3_finalize(pStmt);
    return;
  }

  while( SQLITE_ROW==sqlite3_step(pStmt) ){
    sqlite3_int64 k = sqlite3_column_int64(pStmt, 0);
    sqlite3_value *pFrame = sqlite3_column_value(pStmt, 1);
    int nBlob = sqlite3_column_bytes(pStmt, 2);
    const u8 *pBlob = (const u8*)sqlite3_column_blob(pStmt, 2);

    int bAuto = zonefileIsAutoFrame(pFrame);
    if( zonefileCompareValue(pFrame, pPrev) 
     || (bAuto && szFrame && (szFrame+nBlob)>sWrite.maxAutoFrameSize)
    ){
      /* Add new entry to sFrameIdx */
      szFrame = 0;
      if( zonefileBufferGrow(pCtx, &sFrameIdx, 4) ) goto zone_write_out;
      zonefileAppend32(&sFrameIdx, sFrames.n);
      sqlite3_value_free(pPrev);
      pPrev = sqlite3_value_dup(pFrame);
      if( pPrev==0 ){
        sqlite3_result_error_nomem(pCtx);
        goto zone_write_out;
      }
      nFrame++;
    }

    /* Add new entry to sKeyIdx */
    if( zonefileBufferGrow(pCtx, &sKeyIdx, 20) ) goto zone_write_out;
    zonefileAppend64(&sKeyIdx, k);
    zonefileAppend32(&sKeyIdx, nFrame-1);
    zonefileAppend32(&sKeyIdx, szFrame);
    zonefileAppend32(&sKeyIdx, nBlob);

    /* Add data for new entry to sFrames */
    if( zonefileBufferGrow(pCtx, &sFrames, nBlob) ) goto zone_write_out;
    zonefileAppendBlob(&sFrames, pBlob, nBlob);
    szFrame += nBlob;
    nKey++;
  }

  /* Create the zonefile header in the in-memory buffer */
  zonefilePut32(&aHdr[0], ZONEFILE_MAGIC_NUMBER);
  aHdr[4] = sWrite.compressionTypeIndexData;
  aHdr[5] = sWrite.compressionTypeContent;
  zonefilePut32(&aHdr[6], 0);     /* Compression dictionary byte offset */
  zonefilePut32(&aHdr[10], ZONEFILE_SZ_HEADER + sFrameIdx.n + sKeyIdx.n); 
  zonefilePut32(&aHdr[14], nFrame);
  zonefilePut32(&aHdr[18], nKey);
  aHdr[22] = sWrite.encryptionType;
  aHdr[23] = 0;                   /* Encryption key index */
  aHdr[24] = 0;                   /* extended header version */
  aHdr[25] = 0;                   /* extended header size */
  assert( ZONEFILE_SZ_HEADER==26 );

  rc = zonefileWrite(pFd, aHdr, ZONEFILE_SZ_HEADER);
  if( rc==SQLITE_OK ) rc = zonefileWrite(pFd, sFrameIdx.a, sFrameIdx.n);
  if( rc==SQLITE_OK ) rc = zonefileWrite(pFd, sKeyIdx.a, sKeyIdx.n);
  if( rc==SQLITE_OK ) rc = zonefileWrite(pFd, sFrames.a, sFrames.n);
  if( rc ){
    zonefileCtxError(pCtx, "error writing file \"%s\" (fwrite())", zFile);
    goto zone_write_out;
  }

  if( fclose(pFd) ){
    zonefileCtxError(pCtx, "error writing file \"%s\" (fclose())", zFile);
  }
  pFd = 0;

 zone_write_out:
  if( pFd ) fclose(pFd);
  sqlite3_finalize(pStmt);
  zonefileBufferFree(&sFrameIdx);
  zonefileBufferFree(&sKeyIdx);
  zonefileBufferFree(&sFrames);
}

/*
** Register the "zonefile" extensions.
*/
static int zonefileRegister(sqlite3 *db){
  struct Func {
    const char *z;
    int n;
    void (*x)(sqlite3_context*,int,sqlite3_value**);
  } aFunc[] = {
    { "zonefile_write", 2, zonefileWriteFunc },
    { "zonefile_write", 3, zonefileWriteFunc },
  };
  int rc = SQLITE_OK;
  int i;

  for(i=0; rc==SQLITE_OK && i<sizeof(aFunc)/sizeof(aFunc[0]); i++){
    struct Func *p = &aFunc[i];
    rc = sqlite3_create_function(db, p->z, p->n, SQLITE_ANY, 0, p->x, 0, 0);
  }

  return rc;
}

#else         /* SQLITE_OMIT_VIRTUALTABLE */
# define zonefileRegister(x) SQLITE_OK
#endif

#ifdef _WIN32
__declspec(dllexport)
#endif
int sqlite3_zonefile_init(
  sqlite3 *db, 
  char **pzErrMsg, 
  const sqlite3_api_routines *pApi
){
  SQLITE_EXTENSION_INIT2(pApi);
  (void)pzErrMsg;  /* Unused parameter */
  return zonefileRegister(db);
}

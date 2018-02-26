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

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>

/*
** Default values for various zonefile_write() parameters.
*/
#define ZONEFILE_DEFAULT_MAXAUTOFRAMESIZE (64*1024)
#define ZONEFILE_DEFAULT_ENCRYPTION       1
#define ZONEFILE_DEFAULT_COMPRESSION      0
#define ZONEFILE_DEFAULT_DICTSIZE         (64*1024)

/* 
** Value to use for the first 4 bytes of a zonefile file header.
*/
#define ZONEFILE_MAGIC_NUMBER 0x464B3138

/*
** Size of a zonefile header. And of each entry in the 
** ZonefileIndex.keyOffsets array.
*/
#define ZONEFILE_SZ_HEADER           32
#define ZONEFILE_SZ_KEYOFFSETS_ENTRY 20

/*
** Constants for supported compression types. These are copied from the
** published file format spec.
*/
#define ZONEFILE_COMPRESSION_NONE             0
#define ZONEFILE_COMPRESSION_ZSTD             1
#define ZONEFILE_COMPRESSION_ZSTD_GLOBAL_DICT 2
#define ZONEFILE_COMPRESSION_ZLIB             3
#define ZONEFILE_COMPRESSION_BROTLI           4
#define ZONEFILE_COMPRESSION_LZ4              5
#define ZONEFILE_COMPRESSION_LZ4HC            6

/*
** Schema for "zonefile" virtual table.
*/
#define ZONEFILE_SCHEMA          \
  "CREATE TABLE z1("             \
  "  k INTEGER PRIMARY KEY,"     \
  "  v BLOB,"                    \
  "  fileid INTEGER,"            \
  "  sz INTEGER"                 \
  ")"

/*
** Schema for "zonefile_files" virtual table.
*/
#define ZONEFILE_FILES_SCHEMA    \
  "CREATE TABLE z2("             \
  "  filename TEXT,"             \
  "  ekey BLOB,"                 \
  "  header JSON HIDDEN"         \
  ")"


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

/*
** Forward declarations for encryption/decryption functions.
**
** If this module is not compiled with SQLITE_HAVE_ZONEFILE_CODEC, then
** implementations of the following type and functions that support the
** mock encryption method "xor" only are provided. Alternatively, the
** application may append a more functional implementation of the following 
** type and functions to this file before compiling it with
** SQLITE_HAVE_ZONEFILE_CODEC defined.
*/
typedef struct ZonefileCodec ZonefileCodec;
static int zonefileCodecCreate(
    int,int,unsigned char*,int,ZonefileCodec**,char**);
static int zonefileCodecNonceSize(ZonefileCodec*);
static void zonefileCodecEncode(ZonefileCodec*, unsigned char*, int);
static void zonefileCodecDecode(ZonefileCodec*, unsigned char*, int);
static void zonefileCodecDestroy(ZonefileCodec*);

#ifndef SQLITE_HAVE_ZONEFILE_CODEC
typedef struct ZonefileCodec ZonefileCodec;

struct ZonefileCodec {
  u8 aKey[16];
  int bEncrypt;                   /* Second parameter passed to Create() */
};

/* Create a new encryption module instance using algorithm iAlg.
**
**   iAlg==1   AES128 CTR
**   iAlg==2   AES128 CBC
**   iAlg==3   AES256 CTR
**   iAlg==4   AES256 CBC
**   iAlg==5   XOR          Testing use only
**
** If the requested algorithm is not available, the routine returns
** a NULL pointer.  NULL is also returned on a OOM error.
**
** Use zonefileCodecDestroy() to reclaim memory.
*/
static int zonefileCodecCreate(
  int iAlg, 
  int bEncrypt,                   /* True for encryption, zero for decryption */
  unsigned char *pKey, int nKey, 
  ZonefileCodec **pp, 
  char **pzErr
){
  ZonefileCodec *pRet;
  int rc = SQLITE_OK;
  
  if( iAlg!=5 ){
    *pzErr = sqlite3_mprintf("unsupported encryption method: %d", iAlg);
    rc = SQLITE_ERROR;
  }else{
    *pp = pRet = (ZonefileCodec*)sqlite3_malloc(sizeof(ZonefileCodec));
    if( pRet==0 ){
      rc = SQLITE_NOMEM;
    }else{
      int i;
      for(i=0; i<sizeof(pRet->aKey); i++){
        pRet->aKey[i] = pKey[i % nKey];
      }
      pRet->bEncrypt = bEncrypt;
    }
  }

  return rc;
}

/* Return the size of the nonce used for the given encryption module */
static int zonefileCodecNonceSize(ZonefileCodec *pCodec){
  return 16;
}

/* Encrypt in-place.
**
** The size of the content will grow by the nonce size.  Hence, the
** buffer must have at least nonce bytes of extra space available at
** the end to accommodate that growth.  When persisting results, be
** sure to include the extra bytes.
*/
static void zonefileCodecEncode(
  ZonefileCodec *pCodec, 
  unsigned char *pIn, int nIn
){
  int i;
  u8 *aNonce = &pIn[nIn];
  assert( pCodec->bEncrypt );
  sqlite3_randomness(16, aNonce);
  for(i=0; i<nIn; i++){
    pIn[i] = pIn[i] ^ aNonce[i%16] ^ pCodec->aKey[i%16];
  }
}

/* Decrypt in-place.
**
** The size of the decrypted text will be less than the input buffer
** by nonce-size bytes.
*/
static void zonefileCodecDecode(
  ZonefileCodec *pCodec, 
  unsigned char *pIn, int nIn
){
  int i;
  u8 *aNonce = &pIn[nIn-16];
  assert( pCodec->bEncrypt==0 );
  for(i=0; i<nIn-16; i++){
    pIn[i] = pIn[i] ^ aNonce[i%16] ^ pCodec->aKey[i%16];
  }
}

/* Destroy an encryption module.
** It is harmless to pass in a NULL pointer.
*/
static void zonefileCodecDestroy(ZonefileCodec *pCodec){
  sqlite3_free(pCodec);
}
#endif                           /* SQLITE_HAVE_ZONEFILE_CODEC */

/*
** All zonefile and zonefile_files virtual table instances that belong
** to the same database handle (sqlite3*) share a single instance of the
** ZonefileGlobal object. This global object contains a table of 
** configured encryption keys for the various zonefiles in the system.
*/
typedef struct ZonefileGlobal ZonefileGlobal;
typedef struct ZonefileKey ZonefileKey;
struct ZonefileGlobal {
  int nEntry;                     /* Number of entries in the hash table */
  int nHash;                      /* Size of aHash[] array */
  ZonefileKey **aHash;            /* Hash buckets */
};
struct ZonefileKey {
  const char *zName;              /* Zonefile table name */
  const char *zDb;                /* Database name ("main", "temp" etc.) */
  i64 iFileid;                    /* File id */
  const char *zKey;               /* Key buffer */
  int nKey;                       /* Size of zKey in bytes */
  u32 iHash;                      /* zonefileKeyHash() value */
  ZonefileKey *pHashNext;         /* Next colliding key in hash table */
};

/*
** Return a 32-bit hash value for the three arguments.
*/
static u32 zonefileKeyHash(
  const char *zDb,
  const char *zTab,
  i64 iFileid
){
  u32 iHash = 0;
  int i;
  for(i=0; zDb[i]; i++) iHash += (iHash<<3) + (u8)zDb[i];
  for(i=0; zTab[i]; i++) iHash += (iHash<<3) + (u8)zTab[i];
  return (iHash ^ (iFileid & 0xFFFFFFFF));
}

/* 
** Store encryption key zKey in the key-store passed as the first argument.
** Return SQLITE_OK if successful, or an SQLite error code (SQLITE_NOMEM)
** otherwise.
*/
static int zonefileKeyStore(
  ZonefileGlobal *pGlobal,
  const char *zDb,                /* Database containing zonefile table */
  const char *zTab,               /* Name of zonefile table */
  i64 iFileid,                    /* File-id to configure key for */
  const char *zKey                /* Key to store */
){
  ZonefileKey **pp;
  u32 iHash = zonefileKeyHash(zDb, zTab, iFileid);

  /* Remove any old entry */
  if( pGlobal->nHash ){
    for(pp=&pGlobal->aHash[iHash%pGlobal->nHash]; *pp; pp=&((*pp)->pHashNext)){
      ZonefileKey *pThis = *pp;
      if( pThis->iFileid==iFileid 
          && 0==sqlite3_stricmp(zTab, pThis->zName)
          && 0==sqlite3_stricmp(zDb, pThis->zDb)
        ){
        pGlobal->nEntry--;
        *pp = pThis->pHashNext;
        sqlite3_free(pThis);
        break;
      }
    }
  }

  if( zKey ){
    int nKey = strlen(zKey);
    int nDb = strlen(zDb);
    int nTab = strlen(zTab);
    ZonefileKey *pNew;

    /* Resize the hash-table, if necessary */
    if( pGlobal->nEntry>=pGlobal->nHash ){
      int i;
      int n = pGlobal->nHash ? pGlobal->nHash*2 : 16;
      ZonefileKey **a = (ZonefileKey**)sqlite3_malloc(n*sizeof(ZonefileKey*));
      if( a==0 ) return SQLITE_NOMEM;
      memset(a, 0, n*sizeof(ZonefileKey*));
      for(i=0; i<pGlobal->nHash; i++){
        ZonefileKey *p;
        ZonefileKey *pNext;
        for(p=pGlobal->aHash[i]; p; p=pNext){
          pNext = p->pHashNext;
          p->pHashNext = a[p->iHash % n];
          a[p->iHash % n] = p;
        }
      }
      sqlite3_free(pGlobal->aHash);
      pGlobal->aHash = a;
      pGlobal->nHash = n;
    }

    pNew = (ZonefileKey*)sqlite3_malloc(
        sizeof(ZonefileKey) + nKey+1 + nDb+1 + nTab+1
    );
    if( pNew==0 ) return SQLITE_NOMEM;
    memset(pNew, 0, sizeof(ZonefileKey));
    pNew->iFileid = iFileid;
    pNew->iHash = iHash;
    pNew->zKey = (const char*)&pNew[1];
    pNew->nKey = nKey;
    pNew->zDb = &pNew->zKey[nKey+1];
    pNew->zName = &pNew->zDb[nDb+1];
    memcpy((char*)pNew->zKey, zKey, nKey+1);
    memcpy((char*)pNew->zDb, zDb, nDb+1);
    memcpy((char*)pNew->zName, zTab, nTab+1);

    pNew->pHashNext = pGlobal->aHash[iHash % pGlobal->nHash];
    pGlobal->aHash[iHash % pGlobal->nHash] = pNew;
    pGlobal->nEntry++;
  }

  return SQLITE_OK;
}

/*
** Search the key-store passed as the first argument for an encryption
** key to use with the file with file-id iFileid in zonefile table zTab
** in database zDb. If successful, set (*pzKey) to point to the key
** buffer and return the size of the key in bytes.
**
** If no key is found, return 0. The final value of (*pzKey) is undefined
** in this case.
*/
static int zonefileKeyFind(
  ZonefileGlobal *pGlobal,
  const char *zDb,                /* Database containing zonefile table */
  const char *zTab,               /* Name of zonefile table */
  i64 iFileid,                    /* File-id to configure key for */
  const char **pzKey              /* OUT: Pointer to key buffer */
){
  if( pGlobal->nHash ){
    ZonefileKey *pKey;
    u32 iHash = zonefileKeyHash(zDb, zTab, iFileid);
    for(pKey=pGlobal->aHash[iHash%pGlobal->nHash]; pKey; pKey=pKey->pHashNext){
      if( pKey->iFileid==iFileid 
       && 0==sqlite3_stricmp(zTab, pKey->zName)
       && 0==sqlite3_stricmp(zDb, pKey->zDb)
      ){
        *pzKey = pKey->zKey;
        return pKey->nKey;
      }
    }
  }

  return 0;
}

/*
** The pointer passed as the only argument must actually point to a
** ZonefileGlobal structure. This function frees the structure and all
** of its components.
*/
static void zonefileKeyDestroy(void *p){
  ZonefileGlobal *pGlobal = (ZonefileGlobal*)p;
  int i;
  for(i=0; i<pGlobal->nHash; i++){
    ZonefileKey *pKey;
    ZonefileKey *pNext;
    for(pKey=pGlobal->aHash[i]; pKey; pKey=pNext){
      pNext = pKey->pHashNext;
      sqlite3_free(pKey);
    }
  }
  sqlite3_free(pGlobal->aHash);
  sqlite3_free(pGlobal);
}

/*
** Write value v to buffer aBuf as an unsigned 32-bit big-endian integer.
*/
static void zonefilePut32(u8 *aBuf, u32 v){
  aBuf[0] = (v >> 24) & 0xFF;
  aBuf[1] = (v >> 16) & 0xFF;
  aBuf[2] = (v >>  8) & 0xFF;
  aBuf[3] = v & 0xFF;
}

/*
** Read and return an unsigned 32-bit big-endian integer from buffer aBuf.
*/
static u32 zonefileGet32(const u8 *aBuf){
  return (((u32)aBuf[0]) << 24)
       + (((u32)aBuf[1]) << 16)
       + (((u32)aBuf[2]) <<  8)
       + (((u32)aBuf[3]) <<  0);
}

/*
** Generic xOpen, xClose and xUncompressSize methods used by a few
** different compression method bindings.
*/
static int zfGenericOpen(void **pp, u8 *aDict, int nDict){
  *pp = 0;
  return SQLITE_OK;
}
static void zfGenericClose(void *p){
}
static int zfGenericUncompressSize(
  void *p, 
  const u8 *aSrc, int nSrc
){
  return (int)zonefileGet32(aSrc);
}

#ifdef SQLITE_HAVE_ZLIB 
#include <zlib.h>
static int zfZlibCompressBound(void *p, int nSrc){
  return (int)compressBound((uLong)nSrc) + 4;
}
static int zfZlibCompress(
  void *p, 
  u8 *aDest, int *pnDest, 
  const u8 *aSrc, int nSrc
){
  uLongf destLen = (uLongf)(*pnDest)-4;
  int rc = compress(&aDest[4], &destLen, aSrc, (uLong)nSrc);
  *pnDest = (int)(destLen+4);
  zonefilePut32(aDest, nSrc);
  return rc==Z_OK ? SQLITE_OK : SQLITE_ERROR;
}
static int zfZlibUncompress(
  void *p, 
  u8 *aDest, int nDest, 
  const u8 *aSrc, int nSrc
){
  uLongf destLen = (uLongf)nDest;
  int rc = uncompress(aDest, &destLen, &aSrc[4], (uLong)nSrc-4);
  return rc==Z_OK ? SQLITE_OK : SQLITE_ERROR;
}
#endif
#ifdef SQLITE_HAVE_ZSTD 
#include <zstd.h>
static int zfZstdCompressBound(void *p, int nSrc){
  return (int)ZSTD_compressBound((size_t)nSrc);
}
static int zfZstdCompress(
  void *p, 
  u8 *aDest, int *pnDest, 
  const u8 *aSrc, int nSrc
){
  size_t szDest = (size_t)(*pnDest);
  size_t rc = ZSTD_compress(aDest, szDest, aSrc, (size_t)nSrc, 1);
  if( ZSTD_isError(rc) ) return SQLITE_ERROR;
  *pnDest = (int)rc;
  return SQLITE_OK;
}
static int zfZstdUncompressSize(void *p, const u8 *aSrc, int nSrc){
  return (int)ZSTD_getFrameContentSize(aSrc, (size_t)nSrc);
}
static int zfZstdUncompress(
  void *p, 
  u8 *aDest, int nDest, 
  const u8 *aSrc, int nSrc
){
  size_t rc = ZSTD_decompress(aDest, (size_t)nDest, aSrc, (size_t)nSrc);
  if( rc!=(size_t)nDest ) return SQLITE_ERROR;
  return SQLITE_OK;
}

#include <zdict.h>
typedef struct ZfZstddict ZfZstddict;
struct ZfZstddict {
  ZSTD_CDict *pCDict;
  ZSTD_CCtx *pCCtx;
  ZSTD_DDict *pDDict;
  ZSTD_DCtx *pDCtx;
};

static void zfZstddictClose(void *p){
  if( p ){
    ZfZstddict *pCmp = (ZfZstddict*)p;
    if( pCmp->pCDict ) ZSTD_freeCDict(pCmp->pCDict);
    if( pCmp->pCCtx ) ZSTD_freeCCtx(pCmp->pCCtx);
    if( pCmp->pDCtx ) ZSTD_freeDCtx(pCmp->pDCtx);
    sqlite3_free(pCmp);
  }
}
static int zfZstddictOpen(void **pp, u8 *aDict, int nDict){
  int rc = SQLITE_OK;
  ZfZstddict *pDict = (ZfZstddict*)sqlite3_malloc(sizeof(ZfZstddict));
  if( pDict==0 ){
    rc = SQLITE_NOMEM;
  }else{
    memset(pDict, 0, sizeof(ZfZstddict));
    if( aDict ){
      pDict->pDDict = ZSTD_createDDict(aDict, nDict);
      pDict->pDCtx = ZSTD_createDCtx();
      if( pDict->pDDict==0 || pDict->pDCtx==0 ){
        zfZstddictClose((void*)pDict);
        pDict = 0;
        rc = SQLITE_ERROR;
      }
    }
  }
  *pp = (void*)pDict;
  return rc;
}
static int zfZstddictTrain(
  void *p,                        /* Compressor handle */
  u8 *aDict, int *pnDict,         /* OUT: Dictionary buffer */
  u8 *aSamp, size_t *aSz, int nSamp  /* IN: Training samples */
){
  ZfZstddict *pCmp = (ZfZstddict*)p;
  size_t sz = ZDICT_trainFromBuffer(aDict, (size_t)*pnDict, aSamp, aSz, nSamp);
  if( ZDICT_isError(sz) ) return SQLITE_ERROR;
  pCmp->pCDict = ZSTD_createCDict(aDict, sz, 1);
  pCmp->pCCtx = ZSTD_createCCtx();
  if( pCmp->pCDict==0 || pCmp->pCCtx==0 ) return SQLITE_ERROR;
  *pnDict = (int)sz;
  return SQLITE_OK;
}
static int zfZstddictCompress(
  void *p, 
  u8 *aDest, int *pnDest, 
  const u8 *aSrc, int nSrc
){
  ZfZstddict *pCmp = (ZfZstddict*)p;
  size_t szDest = (size_t)(*pnDest);
  size_t rc;
  assert( pCmp && pCmp->pCDict && pCmp->pCCtx );
  rc = ZSTD_compress_usingCDict(
      pCmp->pCCtx, aDest, szDest, aSrc, (size_t)nSrc, pCmp->pCDict
  );
  if( ZSTD_isError(rc) ) return SQLITE_ERROR;
  *pnDest = (int)rc;
  return SQLITE_OK;
}
static int zfZstddictUncompress(
  void *p, 
  u8 *aDest, int nDest, 
  const u8 *aSrc, int nSrc
){
  ZfZstddict *pCmp = (ZfZstddict*)p;
  size_t rc = ZSTD_decompress_usingDDict(
      pCmp->pDCtx, aDest, (size_t)nDest, aSrc, (size_t)nSrc, pCmp->pDDict
  );
  if( rc!=(size_t)nDest ) return SQLITE_ERROR;
  return SQLITE_OK;
}
#endif

#ifdef SQLITE_HAVE_LZ4 
#include <lz4.h>
#include <lz4hc.h>
static int zfLz4CompressBound(void *p, int nSrc){
  return (int)LZ4_compressBound(nSrc) + 4;
}
static int zfLz4Uncompress(
  void *p, 
  u8 *aDest, int nDest, 
  const u8 *aSrc, int nSrc
){
  int rc = LZ4_decompress_safe(
      (const char*)&aSrc[4], (char*)aDest, nSrc-4, nDest
  );
  return rc==nDest ? SQLITE_OK : SQLITE_ERROR;
}
static int zfLz4Compress(
  void *p, 
  u8 *aDest, int *pnDest, 
  const u8 *aSrc, int nSrc
){
  int rc = LZ4_compress_default(
      (const char*)aSrc, (char*)&aDest[4], nSrc, (*pnDest - 4)
  );
  *pnDest = rc+4;
  zonefilePut32(aDest, nSrc);
  return rc==0 ? SQLITE_ERROR : SQLITE_OK;
}
static int zfLz4hcCompress(
  void *p, 
  u8 *aDest, int *pnDest, 
  const u8 *aSrc, int nSrc
){
  int rc = LZ4_compress_HC(
      (const char*)aSrc, (char*)&aDest[4], nSrc, *pnDest, 0
  );
  *pnDest = rc+4;
  zonefilePut32(aDest, nSrc);
  return rc==0 ? SQLITE_ERROR : SQLITE_OK;
}
#endif

#ifdef SQLITE_HAVE_BROTLI 
#include <brotli/encode.h>
#include <brotli/decode.h>

static int zfBrotliCompressBound(void *p, int nSrc){
  return (int)BrotliEncoderMaxCompressedSize((size_t)nSrc) + 4;
}
static int zfBrotliCompress(
  void *p, 
  u8 *aDest, int *pnDest, 
  const u8 *aSrc, int nSrc
){
  size_t nDest = (size_t)*pnDest - 4;
  BROTLI_BOOL rc = BrotliEncoderCompress(
      BROTLI_DEFAULT_QUALITY, BROTLI_DEFAULT_WINDOW, BROTLI_DEFAULT_MODE,
      (size_t)nSrc, aSrc, &nDest, &aDest[4]
  );
  *pnDest = (int)nDest + 4;
  zonefilePut32(aDest, nSrc);
  return rc==0 ? SQLITE_ERROR : SQLITE_OK;
}
static int zfBrotliUncompress(
  void *p, 
  u8 *aDest, int nDest, 
  const u8 *aSrc, int nSrc
){
  size_t n = nDest;
  BrotliDecoderResult rc = BrotliDecoderDecompress(nSrc-4, &aSrc[4], &n, aDest);
  return rc==BROTLI_DECODER_RESULT_SUCCESS ? SQLITE_OK : SQLITE_ERROR;
}
#endif

typedef struct ZonefileCompress ZonefileCompress;
static struct ZonefileCompress {
  int eType;
  const char *zName;
  int (*xOpen)(void**, u8 *aDict, int nDict);
  void (*xClose)(void*);
  int (*xTrain)(void*, u8 *aDict, int *pnDict, u8 *a, size_t *aSz, int n);
  int (*xCompressBound)(void*, int nSrc);
  int (*xCompress)(void*, u8 *aDest, int *pnDest, const u8 *aSrc, int nSrc);
  int (*xUncompressSize)(void*, const u8 *aSrc, int nSrc);
  int (*xUncompress)(void*, u8 *aDest, int nDest, const u8 *aSrc, int nSrc);
} aZonefileCompress[] = {
  { ZONEFILE_COMPRESSION_NONE,             "none",
    0, 0, 0, 0, 0, 0, 0
  },
#ifdef SQLITE_HAVE_ZSTD
  { ZONEFILE_COMPRESSION_ZSTD,             "zstd",
    zfGenericOpen, zfGenericClose, 
    0,
    zfZstdCompressBound, zfZstdCompress, 
    zfZstdUncompressSize, zfZstdUncompress
  },
  { ZONEFILE_COMPRESSION_ZSTD_GLOBAL_DICT, "zstd_global_dict",
    zfZstddictOpen, zfZstddictClose, 
    zfZstddictTrain,
    zfZstdCompressBound, zfZstddictCompress, 
    zfZstdUncompressSize, zfZstddictUncompress
  },
#endif /* SQLITE_HAVE_ZSTD */
#ifdef SQLITE_HAVE_ZLIB
  { ZONEFILE_COMPRESSION_ZLIB,             "zlib",
    zfGenericOpen, zfGenericClose, 
    0,
    zfZlibCompressBound, zfZlibCompress, 
    zfGenericUncompressSize, zfZlibUncompress
  },
#endif /* SQLITE_HAVE_ZLIB */
#ifdef SQLITE_HAVE_BROTLI
  { ZONEFILE_COMPRESSION_BROTLI,           "brotli",
    zfGenericOpen, zfGenericClose, 
    0,
    zfBrotliCompressBound, zfBrotliCompress, 
    zfGenericUncompressSize, zfBrotliUncompress
  },
#endif /* SQLITE_HAVE_BROTLI */
#ifdef SQLITE_HAVE_LZ4
  { ZONEFILE_COMPRESSION_LZ4,              "lz4",
    zfGenericOpen, zfGenericClose, 
    0,
    zfLz4CompressBound, zfLz4Compress, 
    zfGenericUncompressSize, zfLz4Uncompress
  },
  { ZONEFILE_COMPRESSION_LZ4HC,            "lz4hc",
    zfGenericOpen, zfGenericClose, 
    0,
    zfLz4CompressBound, zfLz4hcCompress, 
    zfGenericUncompressSize, zfLz4Uncompress
  },
#endif /* SQLITE_HAVE_LZ4 */
};

static int zonefileCompress(
  const char *zName,              /* Name of requested compression method */
  ZonefileCompress **pp,          /* OUT: Pointer to compression object */
  char **pzErr                    /* OUT: Error message */
){
  int i;
  assert( *pzErr==0 );
  for(i=0; i<sizeof(aZonefileCompress)/sizeof(aZonefileCompress[0]); i++){
    if( sqlite3_stricmp(aZonefileCompress[i].zName, zName)==0 ){
      *pp = &aZonefileCompress[i];
      return SQLITE_OK;
    }
  }
  *pzErr = sqlite3_mprintf("unknown compression scheme: \"%s\"", zName);
  return SQLITE_ERROR;
}

static ZonefileCompress *zonefileCompressByValue(int eType){
  int i;
  for(i=0; i<sizeof(aZonefileCompress)/sizeof(aZonefileCompress[0]); i++){
    if( aZonefileCompress[i].eType==eType ){
      return &aZonefileCompress[i];
    }
  }
  return 0;
}

/* End of code for compression routines
**************************************************************************/


/*
** A structure to store the parameters for a single zonefile_write()
** invocation. An instance of this structure is populated based on
** the json parameters passed to zonefile_write() by function
** zonefileGetParams();
*/
typedef struct ZonefileParam ZonefileParam;
struct ZonefileParam {
  ZonefileCompress *pCmpIdx;      /* For compressing the index */
  ZonefileCompress *pCmpData;     /* For compressing each frame */
  int encryptionType;
  int maxAutoFrameSize;
  int debugExtendedHeaderSize;    /* Size of extended header */
  char *encryptionKey;            /* Encryption key */
};

/*
** A structure to store a deserialized zonefile header in.
*/
typedef struct ZonefileHeader ZonefileHeader;
struct ZonefileHeader {
  u32 magicNumber;
  u8 compressionTypeIndexData;
  u8 compressionTypeContent;
  u32 byteOffsetDictionary;
  u32 byteOffsetFrames;
  u32 numFrames;
  u32 numKeys;
  u8 encryptionType;
  u8 encryptionKeyIdx;
  u8 extendedHeaderVersion;
  u8 extendedHeaderSize;
};

/*
** Resizable buffer structure used by the zonefile_write() implementation.
*/
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

static int zonefilePrepare(
  sqlite3 *db,
  sqlite3_stmt **ppStmt,
  char **pzErr,
  const char *zFmt,
  ...
){
  int rc;
  va_list ap;
  char *zSql;
  va_start(ap, zFmt);
  zSql = sqlite3_vmprintf(zFmt, ap);
  *ppStmt = 0;
  if( zSql ){
    rc = sqlite3_prepare_v2(db, zSql, -1, ppStmt, 0);
    if( rc!=SQLITE_OK ){
      *pzErr = sqlite3_mprintf("%s", sqlite3_errmsg(db));
    }
    sqlite3_free(zSql);
  }else{
    rc = SQLITE_NOMEM;
  }
  return rc;
}

/*
** Return zero if the two SQL values passed as arguments are equal, or
** non-zero otherwise. Values with different types are considered unequal,
** even if they both contain the same numeric value (e.g. 2 and 2.0).
*/
static int zonefileCompareValue(sqlite3_value *p1, sqlite3_value *p2){
  int eType;
  assert( p1 );
  eType = sqlite3_value_type(p1);
  if( sqlite3_value_type(p2)!=eType ) return 1;
  switch( eType ){
    case SQLITE_INTEGER:
      return sqlite3_value_int64(p1)!=sqlite3_value_int64(p2);
    case SQLITE_FLOAT:
      return sqlite3_value_double(p1)!=sqlite3_value_double(p2);
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

static int zonefileEncryption(const char *zName, int *peType, char **pzErr){
  struct Encryption {
    const char *zName;
    int eType;
  } a[] = {
    {"NONE", 0}, 
    {"AES_128_CTR", 1}, 
    {"AES_128_CBC", 2}, 
    {"AES_256_CTR", 3}, 
    {"AES_256_CBC", 4}, 
    {"XOR",         5}, 
  };
  int i;

  for(i=0; i<sizeof(a)/sizeof(a[0]); i++){
    if( 0==sqlite3_stricmp(zName, a[i].zName) ){
      *peType = a[i].eType;
      return SQLITE_OK;
    }
  }

  *pzErr = sqlite3_mprintf("unknown encryption method: %s", zName);
  return SQLITE_ERROR;
}

static int zonefileGetParams(
  sqlite3_context *pCtx,          /* Leave any error message here */
  const char *zJson,              /* JSON configuration parameter (or NULL) */
  ZonefileParam *p                /* Populate this object before returning */
){
  sqlite3 *db = sqlite3_context_db_handle(pCtx);
  sqlite3_stmt *pStmt = 0;
  char *zErr = 0;
  int rc = SQLITE_OK;
  int rc2;

  memset(p, 0, sizeof(ZonefileParam));
  p->maxAutoFrameSize = ZONEFILE_DEFAULT_MAXAUTOFRAMESIZE;
  p->encryptionType = ZONEFILE_DEFAULT_ENCRYPTION;
  p->pCmpData = p->pCmpIdx = zonefileCompressByValue(0);

  rc = zonefilePrepare(db, &pStmt, &zErr,"SELECT key, value FROM json_each(?)");
  if( rc==SQLITE_OK ){
    sqlite3_bind_text(pStmt, 1, zJson, -1, SQLITE_STATIC);
  }
  while( rc==SQLITE_OK && SQLITE_ROW==sqlite3_step(pStmt) ){
    const char *zKey = (const char*)sqlite3_column_text(pStmt, 0);
    int iVal = sqlite3_column_int(pStmt, 1);
    if( sqlite3_stricmp("debugExtendedHeaderSize", zKey)==0 ){
      if( iVal<0 || iVal>255 ){
        zErr = sqlite3_mprintf(
            "debugExtendedHeaderSize value out of range: %d", iVal
        );
        rc = SQLITE_ERROR;
      }
      p->debugExtendedHeaderSize = iVal;
    }else
    if( sqlite3_stricmp("maxAutoFrameSize", zKey)==0 ){
      p->maxAutoFrameSize = iVal;
    }else
    if( sqlite3_stricmp("compressionTypeIndexData", zKey)==0 ){
      const char *zName = (const char*)sqlite3_column_text(pStmt, 1);
      rc = zonefileCompress(zName, &p->pCmpIdx, &zErr);
    }else
    if( sqlite3_stricmp("compressionTypeContent", zKey)==0 ){
      const char *zName = (const char*)sqlite3_column_text(pStmt, 1);
      rc = zonefileCompress(zName, &p->pCmpData, &zErr);
    }else
    if( sqlite3_stricmp("encryptionKey", zKey)==0 ){
      const char *zKey = (const char*)sqlite3_column_text(pStmt, 1);
      p->encryptionKey = sqlite3_mprintf("%s", zKey);
      if( p->encryptionKey==0 ) rc = SQLITE_NOMEM;
    }else
    if( sqlite3_stricmp("encryptionType", zKey)==0 ){
      const char *zName = (const char*)sqlite3_column_text(pStmt, 1);
      rc = zonefileEncryption(zName, &p->encryptionType, &zErr);
    }else{
      rc = SQLITE_ERROR;
      zErr = sqlite3_mprintf("unknown parameter name: \"%s\"", zKey);
    }
  }
  rc2 = sqlite3_finalize(pStmt);
  if( rc==SQLITE_OK ) rc = rc2;

  if( zErr ){
    sqlite3_result_error(pCtx, zErr, -1);
    sqlite3_free(zErr);
  }else if( rc ){
    sqlite3_result_error_code(pCtx, rc);
  }else{
    if( p->encryptionKey==0 ){
      p->encryptionType = 0;
    }
  }
  return rc;
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

static u64 zonefileGet64(u8 *aBuf){
  return (((u64)aBuf[0]) << 56)
       + (((u64)aBuf[1]) << 48)
       + (((u64)aBuf[2]) << 40)
       + (((u64)aBuf[3]) << 32) 
       + (((u64)aBuf[4]) << 24)
       + (((u64)aBuf[5]) << 16)
       + (((u64)aBuf[6]) <<  8)
       + (((u64)aBuf[7]) <<  0);
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

static int zonefileFileWrite(FILE *pFd, const u8 *aBuf, int nBuf){
  size_t res = fwrite(aBuf, 1, nBuf, pFd);
  return res!=(size_t)nBuf ? SQLITE_ERROR : SQLITE_OK;
}

static int zonefileFileRead(FILE *pFd, u8 *aBuf, int nBuf, i64 iOff){
  int rc = fseek(pFd, (long)iOff, SEEK_SET);
  if( rc==0 ){
    rc = fread(aBuf, 1, nBuf, pFd);
    rc = (rc==nBuf) ? SQLITE_OK : SQLITE_ERROR;
  }
  return rc;
}

static FILE *zonefileFileOpen(const char *zFile, int bWrite, char **pzErr){
  FILE *pFd = fopen(zFile, bWrite ? "wb" : "rb");
  if( pFd==0 ){
    *pzErr = sqlite3_mprintf("failed to open file \"%s\" for %s",
        zFile, bWrite ? "writing" : "reading"
    );
  }
  return pFd;
}

static void zonefileFileClose(FILE *pFd){
  if( pFd ) fclose(pFd);
}

static int zonefileAppendData(
  sqlite3_context *pCtx,          /* Leave any error message here */
  ZonefileCompress *pMethod,      /* Compression method object */
  void *pCmp,                     /* Compression handle */
  ZonefileCodec *pCodec,          /* Compression method, if any */
  ZonefileBuffer *pTo,            /* Append new data here */
  ZonefileBuffer *pFrom           /* Input buffer */
){
  int rc = SQLITE_OK;
  int nNonce = pCodec ? zonefileCodecNonceSize(pCodec) : 0;
  int nOrig = pTo->n;
  if( pMethod->eType==ZONEFILE_COMPRESSION_NONE ){
    if( zonefileBufferGrow(pCtx, pTo, pFrom->n + nNonce) ){
      rc = SQLITE_ERROR;
    }else{
      zonefileAppendBlob(pTo, pFrom->a, pFrom->n);
    }
  }else{
    int nReq = pMethod->xCompressBound(pCmp, pFrom->n);
    if( zonefileBufferGrow(pCtx, pTo, nReq + nNonce) ) return SQLITE_ERROR;
    rc = pMethod->xCompress(pCmp, &pTo->a[pTo->n], &nReq, pFrom->a, pFrom->n);
    pTo->n += nReq;
  }

  /* Encrypt the data just appended to buffer pTo. */
  if( rc==SQLITE_OK && pCodec ){
    zonefileCodecEncode(pCodec, &pTo->a[nOrig], pTo->n - nOrig);
    pTo->n += nNonce;
  }
  return rc;
}

/*
** Append nByte bytes of garbage data to the file opened with pFd. Return
** SQLITE_OK if successful, or SQLITE_ERROR if an error occurs.
**
** Parameter nByte is only ever non-zero when running tests. So it doesn't
** matter if this function is inefficient in those cases.
*/
static int zonefilePad(FILE *pFd, int nByte){
  assert( nByte>=0 && nByte<256 );
  if( nByte ){
    int nRem = nByte;
    u8 buf[17] = "0123456789ABCDEF";
    while( nRem>0 ){
      int n = MIN(nRem, sizeof(buf));
      if( zonefileFileWrite(pFd, buf, n) ) return SQLITE_ERROR;
      nRem -= n;
    }
  }
  return SQLITE_OK;
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
  ZonefileParam sParam;           /* Decoded JSON parameters */
  int nKey = 0;                   /* Number of keys in new zonefile */
  int nFrame = 0;                 /* Number of frames in new zonefile */
  sqlite3_stmt *pStmt = 0;        /* SQL used to read data from source table */
  FILE *pFd = 0;
  int rc = SQLITE_OK;
  sqlite3_value *pPrev = 0;
  char *zErr = 0;
  void *pCmp = 0;                 /* Data compressor handle */
  u32 iOff;
  ZonefileCodec *pCodec = 0;

  ZonefileBuffer sFrameIdx = {0, 0, 0};     /* Array of frame offsets */
  ZonefileBuffer sKeyIdx = {0, 0, 0};       /* Array of key locations */
  ZonefileBuffer sData = {0, 0, 0};         /* All completed frames so far */
  ZonefileBuffer sFrame = {0, 0, 0};        /* Current frame (uncompressed) */
  ZonefileBuffer sDict = {0, 0, 0};         /* Compression model (if any) */
  ZonefileBuffer sSample = {0,0,0};         /* Sample data for compressor */
  size_t *aSample = 0;                      /* Array of sample sizes */

  u8 aHdr[ZONEFILE_SZ_HEADER];    /* Space to assemble zonefile header */

  assert( objc==2 || objc==3 );
  zFile = (const char*)sqlite3_value_text(objv[0]);
  zTbl = (const char*)sqlite3_value_text(objv[1]);
  if( objc==3 ){
    zJson = (const char*)sqlite3_value_text(objv[2]);
  }
  if( zonefileGetParams(pCtx, zJson, &sParam) ) return;

  if( sParam.encryptionType!=0 ){
    int n = strlen(sParam.encryptionKey);
    rc = zonefileCodecCreate(
        sParam.encryptionType, 1, (u8*)sParam.encryptionKey, n, &pCodec, &zErr
    );
    if( rc!=SQLITE_OK ){
      if( zErr ){
        sqlite3_result_error(pCtx, zErr, -1);
      }else{
        sqlite3_result_error_code(pCtx, rc);
      }
      sqlite3_free(zErr);
      goto zone_write_out;
    }
  }

  /* Check that the index-data compressor is not one that uses an external
  ** dictionary. This is not permitted as there is no sense in using a
  ** dictionary to compress a single blob of data, and the index-data
  ** compressor only ever compresses a single frame. Also, there is nowhere
  ** in the file-format to store such a dictionary for the index-data.  */
  if( sParam.pCmpIdx->xTrain ){
    zonefileCtxError(pCtx, 
        "compressor \"%s\" may not be used to compress the zonefile index", 
        sParam.pCmpIdx->zName
    );
    goto zone_write_out;
  }

  if( sParam.pCmpData->xOpen ){
    rc = sParam.pCmpData->xOpen(&pCmp, 0, 0);
    if( rc!=SQLITE_OK ){
      zonefileCtxError(pCtx, "error in compressor construction");
      goto zone_write_out;
    }
  }

  /* Prepare the SQL statement used to read data from the source table. This
  ** also serves to verify the suitability of the source table schema. */
  rc = zonefilePrepare(sqlite3_context_db_handle(pCtx), &pStmt, &zErr,
      "SELECT k, frame, v FROM %Q ORDER BY frame, idx, k", zTbl
  );

  /* Open the file-handle used to write out the zonefile */ 
  if( rc==SQLITE_OK ){
    pFd = zonefileFileOpen(zFile, 1, &zErr);
  }
  if( pFd==0 ){
    sqlite3_result_error(pCtx, zErr, -1);
    sqlite3_free(zErr);
    goto zone_write_out;
  }

  /* If the data compressor uses a global dictionary, create the dictionary
  ** and store it in buffer sDict.  */
  if( sParam.pCmpData->xTrain ){
    int nSample = 0;

    while( SQLITE_ROW==sqlite3_step(pStmt) ){
      int nByte = sqlite3_column_bytes(pStmt, 2);
      const u8 *aByte = (const u8*)sqlite3_column_blob(pStmt, 2);
      if( zonefileBufferGrow(pCtx, &sSample, nByte) ){
        goto zone_write_out;
      }
      if( (nSample & (nSample-1))==0 ){
        int nNew = nSample ? nSample*2 : 8;
        size_t *aNew = (size_t*)sqlite3_realloc(aSample, sizeof(size_t) * nNew);
        if( aNew==0 ){
          sqlite3_result_error_nomem(pCtx);
          goto zone_write_out;
        }
        aSample = aNew;
      }
      aSample[nSample] = nByte;
      zonefileAppendBlob(&sSample, aByte, nByte);
      nSample++;
    }
    rc = sqlite3_reset(pStmt);
    if( rc!=SQLITE_OK ){
      zonefileTransferError(pCtx);
      goto zone_write_out;
    }

    if( zonefileBufferGrow(pCtx, &sDict, ZONEFILE_DEFAULT_DICTSIZE) ){
      goto zone_write_out;
    }
    sDict.n = sDict.nAlloc;

    rc = sParam.pCmpData->xTrain(
        pCmp, sDict.a, &sDict.n, sSample.a, aSample, nSample
    );
    if( rc!=SQLITE_OK ){
      zonefileCtxError(pCtx, "error generating dictionary");
      goto zone_write_out;
    }
  }

  while( SQLITE_ROW==sqlite3_step(pStmt) ){
    sqlite3_int64 k = sqlite3_column_int64(pStmt, 0);
    sqlite3_value *pFrame = sqlite3_column_value(pStmt, 1);
    int nBlob = sqlite3_column_bytes(pStmt, 2);
    const u8 *pBlob = (const u8*)sqlite3_column_blob(pStmt, 2);

    int bAuto = zonefileIsAutoFrame(pFrame);
    if( sFrame.n>0 ){
      if( zonefileCompareValue(pFrame, pPrev) 
       || (bAuto && (sFrame.n+nBlob)>sParam.maxAutoFrameSize)
      ){
        /* Add new entry to sFrame */
        if( zonefileBufferGrow(pCtx, &sFrameIdx, 4) 
         || zonefileAppendData(pCtx,sParam.pCmpData,pCmp,pCodec,&sData,&sFrame)
        ){
          goto zone_write_out;
        }
        sFrame.n = 0;
        zonefileAppend32(&sFrameIdx, sData.n);
        sqlite3_value_free(pPrev);
        pPrev = 0;
        nFrame++;
      }
    }

    if( pPrev==0 ){
      pPrev = sqlite3_value_dup(pFrame);
      if( pPrev==0 ){
        sqlite3_result_error_nomem(pCtx);
        goto zone_write_out;
      }
    }

    /* Add new entry to sKeyIdx */
    if( zonefileBufferGrow(pCtx, &sKeyIdx, ZONEFILE_SZ_KEYOFFSETS_ENTRY) ){
      goto zone_write_out;
    }
    zonefileAppend64(&sKeyIdx, k);
    zonefileAppend32(&sKeyIdx, nFrame);
    zonefileAppend32(&sKeyIdx, sFrame.n);
    zonefileAppend32(&sKeyIdx, nBlob);

    /* Add uncompressed data for new entry to sFrame */
    if( zonefileBufferGrow(pCtx, &sFrame, nBlob) ) goto zone_write_out;
    zonefileAppendBlob(&sFrame, pBlob, nBlob);
    nKey++;
  }

  if( sFrame.n>0 ){
    if( zonefileBufferGrow(pCtx, &sFrameIdx, 4) 
     || zonefileAppendData(pCtx, sParam.pCmpData, pCmp, pCodec, &sData, &sFrame)
    ){
      goto zone_write_out;
    }
    zonefileAppend32(&sFrameIdx, sData.n);
    nFrame++;
  }

  /* If a compression method was specified, compress the key-index here */
  if( sParam.pCmpIdx->eType!=ZONEFILE_COMPRESSION_NONE ){
    if( zonefileBufferGrow(pCtx, &sFrameIdx, sKeyIdx.n) ) goto zone_write_out;
    zonefileAppendBlob(&sFrameIdx, sKeyIdx.a, sKeyIdx.n);
    zonefileBufferFree(&sKeyIdx);
    rc = zonefileAppendData(pCtx, sParam.pCmpIdx, 0, 0, &sKeyIdx, &sFrameIdx);
    sFrameIdx.n = 0;
    if( rc ) goto zone_write_out;
  }

  /* Create the zonefile header in the in-memory buffer */
  memset(aHdr, 0, ZONEFILE_SZ_HEADER);
  zonefilePut32(&aHdr[0], ZONEFILE_MAGIC_NUMBER);
  aHdr[4] = (u8)sParam.pCmpIdx->eType;
  aHdr[5] = (u8)sParam.pCmpData->eType;
  iOff = ZONEFILE_SZ_HEADER + sFrameIdx.n + sKeyIdx.n;
  zonefilePut32(&aHdr[6], sDict.n ? iOff+sParam.debugExtendedHeaderSize : 0);
  zonefilePut32(&aHdr[10], iOff + sParam.debugExtendedHeaderSize + sDict.n);
  zonefilePut32(&aHdr[14], nFrame);
  zonefilePut32(&aHdr[18], nKey);
  aHdr[22] = (u8)sParam.encryptionType;
  aHdr[23] = 0;                   /* Encryption key index */
  aHdr[24] = 0;                   /* extended header version */
  aHdr[25] = (u8)sParam.debugExtendedHeaderSize;
  assert( ZONEFILE_SZ_HEADER>=26 );

  rc = zonefileFileWrite(pFd, aHdr, ZONEFILE_SZ_HEADER);
  if( rc==SQLITE_OK ) rc = zonefilePad(pFd, sParam.debugExtendedHeaderSize);
  if( rc==SQLITE_OK ) rc = zonefileFileWrite(pFd, sFrameIdx.a, sFrameIdx.n);
  if( rc==SQLITE_OK ) rc = zonefileFileWrite(pFd, sKeyIdx.a, sKeyIdx.n);
  if( rc==SQLITE_OK ) rc = zonefileFileWrite(pFd, sDict.a, sDict.n);
  if( rc==SQLITE_OK ) rc = zonefileFileWrite(pFd, sData.a, sData.n);
  if( rc ){
    zonefileCtxError(pCtx, "error writing file \"%s\" (fwrite())", zFile);
    goto zone_write_out;
  }

  if( fclose(pFd) ){
    zonefileCtxError(pCtx, "error writing file \"%s\" (fclose())", zFile);
  }
  pFd = 0;

 zone_write_out:
  if( pCmp ) sParam.pCmpData->xClose(pCmp);
  if( pFd ) fclose(pFd);
  sqlite3_value_free(pPrev);
  sqlite3_finalize(pStmt);
  zonefileCodecDestroy(pCodec);
  zonefileBufferFree(&sFrameIdx);
  zonefileBufferFree(&sKeyIdx);
  zonefileBufferFree(&sFrame);
  zonefileBufferFree(&sDict);
  zonefileBufferFree(&sData);
  zonefileBufferFree(&sSample);
  sqlite3_free(aSample);
  sqlite3_free(sParam.encryptionKey);
  if( rc==SQLITE_NOMEM ){
    sqlite3_result_error_nomem(pCtx);
  }
}

typedef struct ZonefileFilesTab ZonefileFilesTab;
struct ZonefileFilesTab {
  sqlite3_vtab base;              /* Base class - must be first */
  sqlite3 *db;
  char *zBase;                    /* Name of this table */
  char *zDb;                      /* Database containing this table */
  ZonefileGlobal *pGlobal;        /* Global object */
  sqlite3_stmt *pInsert;          /* Insert into the %_shadow_file table */
  sqlite3_stmt *pInsertIdx;       /* Insert into the %_shadow_idx table */
  sqlite3_stmt *pDeleteIdx;       /* Delete by fileid from %_shadow_idx table */
  sqlite3_stmt *pDelete;          /* Delete by rowid from %_shadow_file table */
};

typedef struct ZonefileFilesCsr ZonefileFilesCsr;
struct ZonefileFilesCsr {
  sqlite3_vtab_cursor base;       /* Base class - must be first */
  sqlite3_stmt *pSelect;
};

/*
** This function does the work of xCreate (if bCreate!=0) or xConnect
** (if bCreate==0) for the zonefile_files module.
*/
static int zffCreateConnect(
  int bCreate,
  void *pAux,
  sqlite3 *db,
  int argc, const char *const*argv,
  sqlite3_vtab **ppVtab,
  char **pzErr
){
  ZonefileFilesTab *p;
  const char *zName = argv[2];
  const char *zDb = argv[1];
  int nName = strlen(zName);
  int nDb = strlen(zDb);
  int rc = SQLITE_OK;

  if( nName<6 || memcmp(&zName[nName-6], "_files", 6)!=0 ){
    *pzErr = sqlite3_mprintf("do not create zonefile_files tables directly!");
    *ppVtab = 0;
    return SQLITE_ERROR;
  }

  p = (ZonefileFilesTab*)sqlite3_malloc(sizeof(ZonefileFilesTab)+nName+1+nDb+1);
  if( !p ){
    rc = SQLITE_NOMEM;
  }else{
    memset(p, 0, sizeof(ZonefileFilesTab));
    p->zBase = (char*)&p[1];
    memcpy(p->zBase, zName, nName-6);
    p->zBase[nName-6] = '\0';
    p->zDb = &p->zBase[nName+1];
    memcpy(p->zDb, zDb, nDb+1);
    p->db = db;
    p->pGlobal = (ZonefileGlobal*)pAux;
    rc = sqlite3_declare_vtab(db, ZONEFILE_FILES_SCHEMA);
  }

  if( rc!=SQLITE_OK ){
    sqlite3_free(p);
    p = 0;
  }
  *ppVtab = (sqlite3_vtab*)p;
  return rc;
}

/* 
** zonefile_files virtual table module xCreate method.
*/
static int zffCreate(
  sqlite3 *db,
  void *pAux,
  int argc, const char *const*argv,
  sqlite3_vtab **ppVtab,
  char **pzErr
){
  return zffCreateConnect(1, pAux, db, argc, argv, ppVtab, pzErr);
}

/* 
** zonefile_files virtual table module xConnect method.
*/
static int zffConnect(
  sqlite3 *db,
  void *pAux,
  int argc, const char *const*argv,
  sqlite3_vtab **ppVtab,
  char **pzErr
){
  return zffCreateConnect(0, pAux, db, argc, argv, ppVtab, pzErr);
}

/* 
** zonefile_files virtual table module xDisconnect method.
*/
static int zffDisconnect(sqlite3_vtab *pVtab){
  ZonefileFilesTab *pTab = (ZonefileFilesTab*)pVtab;
  sqlite3_finalize(pTab->pInsert);
  sqlite3_finalize(pTab->pDelete);
  sqlite3_finalize(pTab->pInsertIdx);
  sqlite3_finalize(pTab->pDeleteIdx);
  sqlite3_free(pTab);
  return SQLITE_OK;
}

/* 
** zonefile_files virtual table module xBestIndex method.
*/
static int zffBestIndex(sqlite3_vtab *tab, sqlite3_index_info *pInfo){
  return SQLITE_OK;
}

/* 
** zonefile_files virtual table module xOpen method.
*/
static int zffOpen(sqlite3_vtab *pVtab, sqlite3_vtab_cursor **ppCursor){
  ZonefileFilesCsr *pCsr;
  pCsr = (ZonefileFilesCsr*)sqlite3_malloc(sizeof(ZonefileFilesCsr));
  if( pCsr==0 ){
    return SQLITE_NOMEM;
  }
  memset(pCsr, 0, sizeof(ZonefileFilesCsr));
  *ppCursor = (sqlite3_vtab_cursor*)pCsr;
  return SQLITE_OK;
}

/* 
** Reset a ZonefileFilesCsr object to the state it is in immediately after
** it is allocated by zffOpen().
*/
static void zffCursorReset(ZonefileFilesCsr *pCsr){
  sqlite3_finalize(pCsr->pSelect);
  pCsr->pSelect = 0;
}

/* 
** zonefile_files virtual table module xClose method.
*/
static int zffClose(sqlite3_vtab_cursor *cur){
  ZonefileFilesCsr *pCsr = (ZonefileFilesCsr*)cur;
  zffCursorReset(pCsr);
  sqlite3_free(pCsr);
  return SQLITE_OK;
}

/* 
** zonefile_files virtual table module xNext method.
*/
static int zffNext(sqlite3_vtab_cursor *cur){
  ZonefileFilesCsr *pCsr = (ZonefileFilesCsr*)cur;
  int rc;
  if( SQLITE_ROW==sqlite3_step(pCsr->pSelect) ){
    rc = SQLITE_OK;
  }else{
    rc = sqlite3_finalize(pCsr->pSelect);
    pCsr->pSelect = 0;
  }
  return rc;
}

/* 
** zonefile_files virtual table module xFilter method.
*/
static int zffFilter(
  sqlite3_vtab_cursor *cur, 
  int idxNum, const char *idxStr,
  int argc, sqlite3_value **argv
){
  ZonefileFilesCsr *pCsr = (ZonefileFilesCsr*)cur;
  ZonefileFilesTab *pTab = (ZonefileFilesTab*)(pCsr->base.pVtab);
  int rc;
  zffCursorReset(pCsr);

  rc = zonefilePrepare(pTab->db, &pCsr->pSelect, &pTab->base.zErrMsg,
      "SELECT filename, fileid FROM %Q.'%q_shadow_file'",
      pTab->zDb, pTab->zBase
  );
  if( rc==SQLITE_OK ){
    rc = zffNext(cur);
  }
  return rc;
}

/*
** zonefile_files virtual table module xEof method.
*/
static int zffEof(sqlite3_vtab_cursor *cur){
  ZonefileFilesCsr *pCsr = (ZonefileFilesCsr*)cur;
  return pCsr->pSelect==0;
}

static void zonefileHeaderDeserialize(u8 *aBuf, ZonefileHeader *pHdr){
  pHdr->magicNumber = zonefileGet32(&aBuf[0]);
  pHdr->compressionTypeIndexData = aBuf[4];
  pHdr->compressionTypeContent = aBuf[5];
  pHdr->byteOffsetDictionary = zonefileGet32(&aBuf[6]);
  pHdr->byteOffsetFrames = zonefileGet32(&aBuf[10]);
  pHdr->numFrames = zonefileGet32(&aBuf[14]);
  pHdr->numKeys = zonefileGet32(&aBuf[18]);
  pHdr->encryptionType = aBuf[22];
  pHdr->encryptionKeyIdx = aBuf[23];
  pHdr->extendedHeaderVersion = aBuf[24];
  pHdr->extendedHeaderSize = aBuf[25];
}

static void zonefileJsonHeader(sqlite3_context *pCtx, const char *zFile){
  char *zErr = 0;
  FILE *pFd = zonefileFileOpen(zFile, 0, &zErr);
  if( pFd ){
    int rc;
    ZonefileHeader hdr = { 0 };
    u8 aBuf[ZONEFILE_SZ_HEADER];

    rc = zonefileFileRead(pFd, aBuf, ZONEFILE_SZ_HEADER, 0);
    if( rc==SQLITE_OK ){
      zonefileHeaderDeserialize(aBuf, &hdr);
    }

    if( rc!=SQLITE_OK ){
      zonefileCtxError(pCtx, "failed to read header from file: \"%s\"", zFile);
    }else{
      char *zJson = sqlite3_mprintf("{"
          "\"magicNumber\":%u,"
          "\"compressionTypeIndexData\":%u,"
          "\"compressionTypeContent\":%u,"
          "\"byteOffsetDictionary\":%u,"
          "\"byteOffsetFrames\":%u,"
          "\"numFrames\":%u,"
          "\"numKeys\":%u,"
          "\"encryptionType\":%u,"
          "\"encryptionKeyIdx\":%u,"
          "\"extendedHeaderVersion\":%u,"
          "\"extendedHeaderSize\":%u}",
          (u32)hdr.magicNumber,
          (u32)hdr.compressionTypeIndexData,
          (u32)hdr.compressionTypeContent,
          (u32)hdr.byteOffsetDictionary,
          (u32)hdr.byteOffsetFrames,
          (u32)hdr.numFrames,
          (u32)hdr.numKeys,
          (u32)hdr.encryptionType,
          (u32)hdr.encryptionKeyIdx,
          (u32)hdr.extendedHeaderVersion,
          (u32)hdr.extendedHeaderSize
      );
      if( zJson ){
        sqlite3_result_text(pCtx, zJson, -1, SQLITE_TRANSIENT);
        sqlite3_free(zJson);
      }else{
        sqlite3_result_error_nomem(pCtx);
      }
    }
    fclose(pFd);
  }else{
    sqlite3_result_error(pCtx, zErr, -1);
    sqlite3_free(zErr);
  }
}

/* 
** zonefile_files virtual table module xColumn method.
*/
static int zffColumn(sqlite3_vtab_cursor *cur, sqlite3_context *ctx, int i){
  ZonefileFilesCsr *pCsr = (ZonefileFilesCsr*)cur;
  if( sqlite3_vtab_nochange(ctx) ){
    return SQLITE_OK;
  }
  switch( i ){
    case 0: /* filename */
      sqlite3_result_value(ctx, sqlite3_column_value(pCsr->pSelect, 0));
      break;
    case 1: /* ekey */
      break;
    default: {
      const char *zFile = (const char*)sqlite3_column_text(pCsr->pSelect, 0);
      zonefileJsonHeader(ctx, zFile);
      assert( i==2 );
      break;
    }
  }
  return SQLITE_OK;
}

/* 
** zonefile_files virtual table module xRowid method.
*/
static int zffRowid(sqlite3_vtab_cursor *cur, sqlite_int64 *pRowid){
  ZonefileFilesCsr *pCsr = (ZonefileFilesCsr*)cur;
  *pRowid = sqlite3_column_int64(pCsr->pSelect, 1);
  return SQLITE_OK;
}

/*
** Read and decode a Zonefile header from the start of the file opened
** by file-handle pFd. If successful, populate object (*pHdr) before
** returning SQLITE_OK. Otherwise, if an error occurs, set output
** parameter (*pzErr) to point to an English language error message and
** return an SQLite error code.
**
** It is the responsibility of the caller to eventually free any error
** message returned via (*pzErr) using sqlite3_free().
*/
static int zonefileReadHeader(
  FILE *pFd,                      /* File to read from */
  const char *zFile,              /* Name of file opened by pFd */
  ZonefileHeader *pHdr,           /* Populate this object before returning */
  char **pzErr                    /* OUT: Error message */
){
  u8 aBuf[ZONEFILE_SZ_HEADER];
  int rc = zonefileFileRead(pFd, aBuf, ZONEFILE_SZ_HEADER, 0);
  if( rc==SQLITE_OK ){
    zonefileHeaderDeserialize(aBuf, pHdr);
    if( pHdr->magicNumber!=ZONEFILE_MAGIC_NUMBER ){
      rc = SQLITE_ERROR;
    }
  }

  if( rc!=SQLITE_OK ){
    *pzErr = sqlite3_mprintf(
        "failed to read zonefile header from file \"%s\"", zFile
    );
  }

  return rc;
}

static int zonefileUncompress(
  ZonefileCompress *pMethod,
  void *pCmp,
  u8 *aIn, int nIn,
  u8 **paOut, int *pnOut
){
  int rc;
  int nOut = pMethod->xUncompressSize(pCmp, aIn, nIn);
  u8 *aOut = sqlite3_malloc(nOut);

  assert( pMethod->eType!=ZONEFILE_COMPRESSION_NONE );
  if( aOut==0 ){
    rc = SQLITE_NOMEM;
  }else{
    rc = pMethod->xUncompress(pCmp, aOut, nOut, aIn, nIn);
  }
  if( rc ){
    sqlite3_free(aOut);
    aOut = 0;
  }

  *paOut = aOut;
  *pnOut = nOut;
  return rc;
}

static int zfFindCompress(int eType, ZonefileCompress **pp, char **pzErr){
  int rc = SQLITE_OK;
  ZonefileCompress *pCmp;
  pCmp = zonefileCompressByValue(eType);
  if( pCmp==0 ){
    *pzErr = sqlite3_mprintf("unsupported compression method: %d", eType);
    rc = SQLITE_ERROR;
  }else if( pCmp->eType==ZONEFILE_COMPRESSION_NONE ){
    pCmp = 0;
  }
  *pp = pCmp;
  return rc;
}

static int zonefileLoadIndex(
  ZonefileHeader *pHdr,
  FILE *pFd,
  u8 **paIdx, int *pnIdx,
  char **pzErr
){
  ZonefileCompress *pCmp = 0;
  int rc;
  u8 *aIdx = 0;
  int nIdx = 0;
    
  rc = zfFindCompress(pHdr->compressionTypeIndexData, &pCmp, pzErr);
  if( rc==SQLITE_OK ){
    if( pHdr->byteOffsetDictionary ){
      nIdx = pHdr->byteOffsetDictionary - ZONEFILE_SZ_HEADER;
    }else{
      nIdx = pHdr->byteOffsetFrames - ZONEFILE_SZ_HEADER;
    }
    nIdx -= pHdr->extendedHeaderSize;
    aIdx = (u8*)sqlite3_malloc(nIdx);

    if( aIdx==0 ){
      rc = SQLITE_NOMEM;
    }else{
      i64 iOff = ZONEFILE_SZ_HEADER + pHdr->extendedHeaderSize;
      rc = zonefileFileRead(pFd, aIdx, nIdx, iOff);
    }
  }

  if( rc==SQLITE_OK && pCmp ){
    u8 *aUn = 0;
    int nUn = 0;
    rc = zonefileUncompress(pCmp, 0, aIdx, nIdx, &aUn, &nUn);
    if( rc==SQLITE_ERROR ){
      *pzErr = sqlite3_mprintf("failed to uncompress index");
    }
    sqlite3_free(aIdx);
    aIdx = aUn;
    nIdx = nUn;
  }

  if( rc!=SQLITE_OK ){
    sqlite3_free(aIdx);
    aIdx = 0;
    nIdx = 0;
  }

  *paIdx = aIdx;
  *pnIdx = nIdx;
  return rc;
}


static int zonefilePopulateIndex(
  ZonefileFilesTab *pTab,
  const char *zFile,
  i64 iFileid
){
  ZonefileHeader hdr = { 0 };
  int rc;
  FILE *pFd = zonefileFileOpen(zFile, 0, &pTab->base.zErrMsg);

  if( pFd==0 ){
    rc = SQLITE_ERROR;
  }else{
    rc = zonefileReadHeader(pFd, zFile, &hdr, &pTab->base.zErrMsg);
  }

  if( rc==SQLITE_OK && hdr.numKeys>0 ){
    u8 *aKey;                     /* Entire KeyOffsets array */
    int nKey;                     /* Size of buffer aKey[] in bytes */
    int i;

    rc = zonefileLoadIndex(&hdr, pFd, &aKey, &nKey, &pTab->base.zErrMsg);

    if( rc==SQLITE_OK && pTab->pInsertIdx==0 ){
      rc = zonefilePrepare(pTab->db, &pTab->pInsertIdx, &pTab->base.zErrMsg,
          "INSERT INTO %Q.'%q_shadow_idx'(k, fileid, fofst, fsz, ofst, sz)"
          "VALUES(?,?,?,?,?,?)",
          pTab->zDb, pTab->zBase
      );
    }

    for(i=0; (u32)i<hdr.numKeys && rc==SQLITE_OK; i++){
      u8 *aEntry = &aKey[4*hdr.numFrames + ZONEFILE_SZ_KEYOFFSETS_ENTRY * i];
      int iFrame = zonefileGet32(&aEntry[8]);
      i64 iFrameOff = 0;          /* Offset of frame */
      int szFrame;                /* Compressed size of frame */
      i64 iOff;                   /* Offset of blob within uncompressed frame */
      int sz;                     /* Size of blob within uncompressed frame */

      szFrame = zonefileGet32(&aKey[iFrame*4]);
      if( iFrame>0 ){
        iFrameOff = zonefileGet32(&aKey[(iFrame-1)*4]);
        szFrame -= (int)iFrameOff;
      }
      iFrameOff += hdr.byteOffsetFrames;
      iOff = (i64)zonefileGet32(&aEntry[12]);
      sz = (int)zonefileGet32(&aEntry[16]);

      sqlite3_bind_int64(pTab->pInsertIdx, 1, (i64)zonefileGet64(&aEntry[0]));
      sqlite3_bind_int64(pTab->pInsertIdx, 2, iFileid);
      if( hdr.encryptionType || hdr.compressionTypeContent ){
        sqlite3_bind_int64(pTab->pInsertIdx, 5, iOff);
        sqlite3_bind_int(pTab->pInsertIdx, 6, sz);
      }else{
        iFrameOff += iOff;
        szFrame = sz;
      }
      sqlite3_bind_int64(pTab->pInsertIdx, 3, iFrameOff);
      sqlite3_bind_int(pTab->pInsertIdx, 4, szFrame);

      sqlite3_step(pTab->pInsertIdx);
      rc = sqlite3_reset(pTab->pInsertIdx);
      if( rc!=SQLITE_OK ){
        pTab->base.zErrMsg = sqlite3_mprintf("%s", sqlite3_errmsg(pTab->db));
      }
    }
    sqlite3_free(aKey);
  }

  zonefileFileClose(pFd);
  return rc;
}

/*
** zonefile_files virtual table module xUpdate method.
**
** A delete specifies a single argument - the rowid of the row to remove.
** 
** Update and insert operations pass:
**
**   1. The "old" rowid, or NULL.
**   2. The "new" rowid.
**   3. Values for each of the 3 columns: (filename,ekey,header)
*/
static int zffUpdate(
  sqlite3_vtab *pVtab, 
  int nVal, 
  sqlite3_value **apVal, 
  sqlite_int64 *pRowid
){
  int rc = SQLITE_OK;
  ZonefileFilesTab *pTab = (ZonefileFilesTab*)pVtab;

  if( sqlite3_value_type(apVal[0])==SQLITE_INTEGER ){
    if( nVal>1 && sqlite3_value_nochange(apVal[2]) ){
      const char *zKey = (const char*)sqlite3_value_text(apVal[3]);
      i64 iFileid = sqlite3_value_int64(apVal[0]);
      return zonefileKeyStore(
          pTab->pGlobal, pTab->zDb, pTab->zBase, iFileid, zKey
      );
    }else{
      if( pTab->pDelete==0 ){
        rc = zonefilePrepare(pTab->db, &pTab->pDelete, &pVtab->zErrMsg,
            "DELETE FROM %Q.'%q_shadow_file' WHERE fileid=?",
            pTab->zDb, pTab->zBase
            );
      }
      if( rc==SQLITE_OK && pTab->pDeleteIdx==0 ){
        rc = zonefilePrepare(pTab->db, &pTab->pDeleteIdx, &pVtab->zErrMsg,
            "DELETE FROM %Q.'%q_shadow_idx' WHERE fileid=?",
            pTab->zDb, pTab->zBase
            );
      }

      if( rc==SQLITE_OK ){
        sqlite3_bind_value(pTab->pDelete, 1, apVal[0]);
        sqlite3_step(pTab->pDelete);
        rc = sqlite3_reset(pTab->pDelete);
      }
      if( rc==SQLITE_OK ){
        sqlite3_bind_value(pTab->pDeleteIdx, 1, apVal[0]);
        sqlite3_step(pTab->pDeleteIdx);
        rc = sqlite3_reset(pTab->pDeleteIdx);
      }
    }
  }
  if( nVal>1 ){
    i64 iFileid = 0;
    const char *zFile = (const char*)sqlite3_value_text(apVal[2]);

    if( pTab->pInsert==0 ){
      rc = zonefilePrepare(pTab->db, &pTab->pInsert, &pVtab->zErrMsg,
          "INSERT INTO %Q.'%q_shadow_file'(filename) VALUES(?)",
          pTab->zDb, pTab->zBase
      );
    }

    /* Add the new entry to the %_shadow_file table. */
    if( rc==SQLITE_OK ){
      sqlite3_bind_text(pTab->pInsert, 1, zFile, -1, SQLITE_TRANSIENT);
      sqlite3_step(pTab->pInsert);
      rc = sqlite3_reset(pTab->pInsert);
    }

    /* Populate the %_shadow_idx table with entries for all keys in
    ** the zonefile just added to %_shadow_file.  */
    if( rc==SQLITE_OK ){
      iFileid = sqlite3_last_insert_rowid(pTab->db);
      rc = zonefilePopulateIndex(pTab, zFile, iFileid);
    }

    if( rc==SQLITE_OK ){
      const char *zKey = (const char*)sqlite3_value_text(apVal[3]);
      rc = zonefileKeyStore(pTab->pGlobal, pTab->zDb, pTab->zBase,iFileid,zKey);
    }
  }

  return rc;
}

/* Each entry in the frame-cache is represented by an instance of the
** following structure.  */
typedef struct ZonefileFrame ZonefileFrame;
struct ZonefileFrame {
  i64 iFileid;               /* Fileid for this frame */
  i64 iFrameOff;             /* Offset of frame in file */
  int nBuf;                  /* Size of aBuf[] in bytes */
  u8 *aBuf;                  /* Buffer containing uncompressed frame data */
  ZonefileFrame *pLruNext;   /* Next element in LRU list */
  ZonefileFrame *pLruPrev;   /* Previous element in LRU list */
  ZonefileFrame *pHashNext;  /* Next element in same hash bucket */
};

typedef struct ZonefileTab ZonefileTab;
struct ZonefileTab {
  sqlite3_vtab base;         /* Base class - must be first */
  sqlite3 *db;
  sqlite3_stmt *pIdToName;   /* Translate fileid to filename */
  ZonefileGlobal *pGlobal;
  char *zName;               /* Name of this table */
  char *zDb;                 /* Name of db containing this table */

  /* The following variables are used to implement the frame-cache */
  int nCacheSize;            /* Configured cache size */
  int nCacheEntry;           /* Current number of entries in cache */
  int nCacheBucket;          /* Number of buckets in frame cache hash table */
  ZonefileFrame **aCache;    /* Array of hash buckets */
  ZonefileFrame *pCacheFirst;/* Most recently used frame */
  ZonefileFrame *pCacheLast; /* Least recently used frame (first to discard) */
};

typedef struct ZonefileCsr ZonefileCsr;
struct ZonefileCsr {
  sqlite3_vtab_cursor base;  /* Base class - must be first */
  sqlite3_stmt *pSelect;     /* SELECT on %_shadow_idx table */
};

/*
** Attempt to interpret the contents of string z as an integer. If
** successful, set (*piVal) to the integer value and return SQLITE_OK.
** Otherwise, return SQLITE_ERROR.
*/
static int zonefileParseInteger(const char *z, int *piVal){
  *piVal = atoi(z);
  return SQLITE_OK;
}

/*
** Return true character i is considered to be whitespace.
*/
static int zonefile_isspace(char i){
  return (i==' ');
}

/*
** This function is called as part of constructing zonefile virtual table
** pTab. Argument zOption is the full text of a parameter (column name)
** specified as part of the CREATE VIRTUAL TABLE statement. This function
** attempts to interpret the parameter and update structure pTab 
** accordingly. If successful, SQLITE_OK is returned. Otherwise, an
** SQLite error code is returned and (*pzErr) is left pointing to
** a buffer containing an English language error message. It is the
** responsibility of the caller to eventually free this buffer using
** sqlite3_free().
*/
static int zonefileParseOption(
  ZonefileTab *pTab,              /* Zonefile vtab under construction */
  const char *zOption,            /* Text of option (column name) */
  char **pzErr                    /* OUT: Error message */
){
  const char *z = zOption;
  const char *zOpt = z;
  int nOpt;
  const char *zVal;
  int rc = SQLITE_OK;

  /* Skip until EOF, whitespace or "=" */
  assert( 0==zonefile_isspace(*z) );
  while( *z && !zonefile_isspace(*z) && *z!='=' ) z++;
  nOpt = z-zOpt;

  /* Skip whitespace. Then check there is an "=". */
  while( zonefile_isspace(*z) ) z++;
  if( *z!='=' ) goto parse_error;
  z++;
  while( zonefile_isspace(*z) ) z++;
  zVal = z;

  if( nOpt==9 && sqlite3_strnicmp(zOpt, "cachesize", 9)==0 ){
    rc = zonefileParseInteger(zVal, &pTab->nCacheSize);
  }else{
    goto parse_error;
  }

  return rc;

 parse_error:
  *pzErr = sqlite3_mprintf("parse error in option: %s", zOption);
  return SQLITE_ERROR;
}

/*
** This function does the work of xCreate (if bCreate!=0) or xConnect
** (if bCreate==0) for the zonefile module.
**
**   argv[0]   -> module name  ("zonefile")
**   argv[1]   -> database name
**   argv[2]   -> table name
*/
static int zonefileCreateConnect(
  int bCreate,
  void *pAux,
  sqlite3 *db,
  int argc, const char *const*argv,
  sqlite3_vtab **ppVtab,
  char **pzErr
){
  ZonefileTab *p;
  const char *zName = argv[2];
  const char *zDb = argv[1];
  int nName = strlen(zName);
  int nDb = strlen(zDb);
  int rc = SQLITE_OK;

  p = (ZonefileTab*)sqlite3_malloc(sizeof(ZonefileTab) + nName+1 + nDb+1);
  if( !p ){
    rc = SQLITE_NOMEM;
  }else{
    int i;
    memset(p, 0, sizeof(ZonefileTab));
    p->zName = (char*)&p[1];
    memcpy(p->zName, zName, nName+1);
    p->zDb = &p->zName[nName+1];
    memcpy(p->zDb, zDb, nDb+1);
    p->db = db;
    p->pGlobal = (ZonefileGlobal*)pAux;
  
    if( bCreate ){
      char *zSql = sqlite3_mprintf(
          "CREATE TABLE %Q.'%q_shadow_idx'("
          "  k INTEGER PRIMARY KEY,"
          "  fileid INTEGER,"
          "  fofst INTEGER,"
          "  fsz INTEGER,"
          "  ofst INTEGER,"
          "  sz INTEGER"
          ");"
          "CREATE TABLE %Q.'%q_shadow_file'(" 
          "  filename TEXT,"
          "  fileid INTEGER PRIMARY KEY"
          ");" 
          "CREATE VIRTUAL TABLE %Q.'%q_files' USING zonefile_files;",
          zDb, zName, zDb, zName, zDb, zName
      );
  
      if( zSql==0 ){
        rc = SQLITE_NOMEM;
      }else{
        rc = sqlite3_exec(db, zSql, 0, 0, pzErr);
        sqlite3_free(zSql);
      }
    }
    
    if( rc==SQLITE_OK ){
      rc = sqlite3_declare_vtab(db, ZONEFILE_SCHEMA);
    }

    for(i=3; i<argc && rc==SQLITE_OK; i++){
      rc = zonefileParseOption(p, argv[i], pzErr);
    }
    if( rc==SQLITE_OK && p->nCacheSize<1 ) p->nCacheSize = 1;
  }

  if( rc!=SQLITE_OK ){
    sqlite3_free(p);
    p = 0;
  }
  *ppVtab = (sqlite3_vtab*)p;
  return rc;
}

/* 
** zonefile virtual table module xCreate method.
*/
static int zonefileCreate(
  sqlite3 *db,
  void *pAux,
  int argc, const char *const*argv,
  sqlite3_vtab **ppVtab,
  char **pzErr
){
  return zonefileCreateConnect(1, pAux, db, argc, argv, ppVtab, pzErr);
}

/* 
** zonefile virtual table module xConnect method.
*/
static int zonefileConnect(
  sqlite3 *db,
  void *pAux,
  int argc, const char *const*argv,
  sqlite3_vtab **ppVtab,
  char **pzErr
){
  return zonefileCreateConnect(0, pAux, db, argc, argv, ppVtab, pzErr);
}

/* 
** Zonefile virtual table module xBestIndex method.
**
** Equality and range constraints on either the rowid or column "k" (which
** are the same thing) are processed. Bits in the idxNum parameter are
** set to indicate the constraints present:
**
**   0x01:   k == ? 
**   0x02:   k <  ? 
**   0x04:   k <= ? 
**   0x08:   k >  ? 
**   0x10:   k >= ? 
**
** Only some combinations are valid:
**
**   * If an == constraint is found, no other bits are set.
**   * If a < constraint is present, any <= is ignored.
**   * If a > constraint is present, any >= is ignored.
*/
static int zonefileBestIndex(sqlite3_vtab *pVtab, sqlite3_index_info *pInfo){
  int iEq = -1;
  int iLt = -1;
  int iLe = -1;
  int iGt = -1;
  int iGe = -1;
  int i;
  int idxNum = 0;
  double cost = 1000000000.0;

  for(i=0; i<pInfo->nConstraint; i++){
    struct sqlite3_index_constraint *p = &pInfo->aConstraint[i];
    if( p->usable && p->iColumn<=0 ){
      switch( p->op ){
        case SQLITE_INDEX_CONSTRAINT_EQ: iEq = i; break;
        case SQLITE_INDEX_CONSTRAINT_LT: iLt = i; break;
        case SQLITE_INDEX_CONSTRAINT_LE: iLe = i; break;
        case SQLITE_INDEX_CONSTRAINT_GT: iGt = i; break;
        case SQLITE_INDEX_CONSTRAINT_GE: iGe = i; break;
      }
    }
  }

  if( iEq>=0 ){
    cost = 10.0;
    idxNum = 0x01;
    pInfo->aConstraintUsage[iEq].argvIndex = 1;
  }else{
    int iIdx = 1;
    if( iLt>=0 ){
      pInfo->aConstraintUsage[iLt].argvIndex = iIdx++;
      idxNum |= 0x02;
    }else if( iLe>=0 ){
      pInfo->aConstraintUsage[iLe].argvIndex = iIdx++;
      idxNum |= 0x04;
    }
    if( iGt>=0 ){
      pInfo->aConstraintUsage[iGt].argvIndex = iIdx++;
      idxNum |= 0x08;
    }else if( iGe>=0 ){
      pInfo->aConstraintUsage[iGe].argvIndex = iIdx++;
      idxNum |= 0x10;
    }

    if( iIdx==2 ) cost = 10000.0;
    if( iIdx==3 ) cost = 100.0;
  }

  pInfo->idxNum = idxNum;
  pInfo->estimatedCost = cost;

  return SQLITE_OK;
}

/* 
** zonefile virtual table module xOpen method.
*/
static int zonefileOpen(sqlite3_vtab *pVTab, sqlite3_vtab_cursor **ppCursor){
  ZonefileCsr *pCsr;
  pCsr = (ZonefileCsr*)sqlite3_malloc(sizeof(ZonefileCsr));
  if( pCsr==0 ){
    return SQLITE_NOMEM;
  }
  memset(pCsr, 0, sizeof(ZonefileCsr));
  *ppCursor = (sqlite3_vtab_cursor*)pCsr;
  return SQLITE_OK;
}

/* 
** Reset a ZonefileCsr object to the state it is in immediately after
** it is allocated by zffOpen().
*/
static void zonefileCursorReset(ZonefileCsr *pCsr){
  sqlite3_finalize(pCsr->pSelect);
  pCsr->pSelect = 0;
}

/* 
** zonefile virtual table module xClose method.
*/
static int zonefileClose(sqlite3_vtab_cursor *cur){
  ZonefileCsr *pCsr = (ZonefileCsr*)cur;
  zonefileCursorReset(pCsr);
  sqlite3_free(pCsr);
  return SQLITE_OK;
}

/* 
** zonefile virtual table module xNext method.
*/
static int zonefileNext(sqlite3_vtab_cursor *cur){
  ZonefileCsr *pCsr = (ZonefileCsr*)cur;
  int rc = SQLITE_OK;
  if( SQLITE_ROW!=sqlite3_step(pCsr->pSelect) ){
    rc = sqlite3_finalize(pCsr->pSelect);
    if( rc!=SQLITE_OK ){
      ZonefileTab *pTab = (ZonefileTab*)pCsr->base.pVtab;
      pTab->base.zErrMsg = sqlite3_mprintf("%s", sqlite3_errmsg(pTab->db));
    }
    pCsr->pSelect = 0;
  }
  return rc;
}

/* 
** zonefile virtual table module xFilter method.
*/
static int zonefileFilter(
  sqlite3_vtab_cursor *cur, 
  int idxNum, const char *idxStr,
  int argc, sqlite3_value **argv
){
  ZonefileCsr *pCsr = (ZonefileCsr*)cur;
  ZonefileTab *pTab = (ZonefileTab*)pCsr->base.pVtab;
  int rc;

  const char *z1 = 0;
  const char *z2 = 0;

  if( idxNum & 0x01 ){
    z1 = "k = ?";
  }else{
    if( idxNum & 0x02 ) { z1 = "k < ?"; }
    if( idxNum & 0x04 ) { z1 = "k <= ?"; }
    if( idxNum & 0x08 ) { if( z1 ) z2 = "k > ?"; else z1 = "k > ?"; }
    if( idxNum & 0x10 ) { if( z1 ) z2 = "k >= ?"; else z1 = "k >= ?"; }
  }

  rc = zonefilePrepare(pTab->db, &pCsr->pSelect, &pTab->base.zErrMsg, 
      "SELECT k, fileid, fofst, fsz, ofst, sz FROM %Q.'%q_shadow_idx'%s%s%s%s",
      pTab->zDb, pTab->zName,
      z1 ? " WHERE " : "", z1, 
      z2 ? " AND " : "", z2
  );

  if( z1 ) sqlite3_bind_value(pCsr->pSelect, 1, argv[0]);
  if( z2 ) sqlite3_bind_value(pCsr->pSelect, 2, argv[1]);

  if( rc==SQLITE_OK ){
    rc = zonefileNext(cur);
  }

  return rc;
}

/*
** zonefile virtual table module xEof method.
*/
static int zonefileEof(sqlite3_vtab_cursor *cur){
  ZonefileCsr *pCsr = (ZonefileCsr*)cur;
  return (pCsr->pSelect==0);
}

static void zonefileFree(void *p){
  sqlite3_free(p);
}

static int zonefileGetFile(
  sqlite3_context *pCtx,          /* Leave error message here */
  ZonefileCsr *pCsr,              /* Cursor object */
  const char **pzFile             /* OUT: Pointer to current file */
){
  ZonefileTab *pTab = (ZonefileTab*)pCsr->base.pVtab;
  int rc = SQLITE_OK;
  i64 iFileid;

  if( pTab->pIdToName==0 ){
    rc = zonefilePrepare(pTab->db, &pTab->pIdToName, &pTab->base.zErrMsg, 
        "SELECT filename FROM %Q.'%q_shadow_file' WHERE fileid=?",
        pTab->zDb, pTab->zName
    );
    if( rc==SQLITE_ERROR ) zonefileTransferError(pCtx);
  }

  if( rc==SQLITE_OK ){
    iFileid = sqlite3_column_int64(pCsr->pSelect,1);
    sqlite3_bind_int64(pTab->pIdToName, 1, iFileid);
    if( SQLITE_ROW==sqlite3_step(pTab->pIdToName) ){
      *pzFile = (const char*)sqlite3_column_text(pTab->pIdToName, 0);
    }else{
      rc = sqlite3_reset(pTab->pIdToName);
      if( rc==SQLITE_OK ){
        rc = SQLITE_CORRUPT_VTAB;
      }else{
        zonefileTransferError(pCtx);
      }
    }
  }

  return rc;
}

static void zonefileReleaseFile(ZonefileCsr *pCsr){
  ZonefileTab *pTab = (ZonefileTab*)pCsr->base.pVtab;
  sqlite3_reset(pTab->pIdToName);
}

static int zonefileValueReadDirect(sqlite3_context *pCtx, ZonefileCsr *pCsr){
  i64 iOff = sqlite3_column_int64(pCsr->pSelect, 2);
  int sz = sqlite3_column_int(pCsr->pSelect, 3);

  FILE *pFd = 0;                  /* File handle open on zonefile */
  u8 *aBuf;                       /* Buffer to read blob into */
  int rc;                         /* Return code */

  aBuf = sqlite3_malloc(sz);
  if( aBuf==0 ){
    rc = SQLITE_NOMEM;
  }else{
    const char *zFile = 0;
    /* Open the file to read the blob from */
    rc = zonefileGetFile(pCtx, pCsr, &zFile);
    if( rc==SQLITE_OK ){
      char *zErr = 0;
      pFd = zonefileFileOpen(zFile, 0, &zErr);
      if( pFd==0 ){ 
        sqlite3_result_error(pCtx, zErr, -1);
        sqlite3_free(zErr);
        rc = SQLITE_ERROR;
      }
      zonefileReleaseFile(pCsr);
    }
  }

  if( rc==SQLITE_OK ){
    rc = zonefileFileRead(pFd, aBuf, sz, iOff);
    if( rc==SQLITE_OK ){
      sqlite3_result_blob(pCtx, aBuf, sz, zonefileFree);
      aBuf = 0;
    }
  }

  zonefileFileClose(pFd);
  sqlite3_free(aBuf);
  return rc;
}

#ifndef NDEBUG
static void zonefileCacheCheck(ZonefileTab *pTab){
  ZonefileFrame *p;
  int n = 0;
  for(p=pTab->pCacheFirst; p; p=p->pLruNext){
    assert( p!=pTab->pCacheFirst || p->pLruPrev==0 );
    assert( p!=pTab->pCacheLast || p->pLruNext==0 );
    assert( p==pTab->pCacheFirst || p->pLruPrev->pLruNext==p );
    assert( p==pTab->pCacheLast || p->pLruNext->pLruPrev==p );
    n++;
  }
  assert( n==pTab->nCacheEntry );
}
#else
# define zonefileCacheCheck(x)
#endif

/*
** Search the frame-cache belonging to virtual table pTab for an entry
** corresponding to the frame at byte offset iFrameOff of the file with
** file id iFile. If such an entry is found, return a pointer to it.
** Otherwise, if no such entry exists, return NULL.
*/
static ZonefileFrame *zonefileCacheFind(
  ZonefileTab *pTab, 
  i64 iFile, 
  i64 iFrameOff
){
  ZonefileFrame *pFrame;
  for(pFrame=pTab->pCacheFirst; pFrame; pFrame=pFrame->pLruNext){
    if( pFrame->iFileid==iFile && pFrame->iFrameOff==iFrameOff ){
      /* Found a match. Move it to the front of the LRU list and return
      ** a pointer to it. */
      assert( (pFrame->pLruPrev==0)==(pFrame==pTab->pCacheFirst) );
      assert( (pFrame->pLruNext==0)==(pFrame==pTab->pCacheLast) );
      if( pFrame->pLruPrev ){
        assert( pFrame==pFrame->pLruPrev->pLruNext );
        pFrame->pLruPrev->pLruNext = pFrame->pLruNext;

        if( pFrame->pLruNext ){
          assert( pFrame==pFrame->pLruNext->pLruPrev );
          pFrame->pLruNext->pLruPrev = pFrame->pLruPrev;
        }else{
          assert( pFrame==pTab->pCacheLast );
          pTab->pCacheLast = pFrame->pLruPrev;
          pTab->pCacheLast->pLruNext = 0;
        }

        pFrame->pLruPrev = 0;
        pFrame->pLruNext = pTab->pCacheFirst;
        pTab->pCacheFirst->pLruPrev = pFrame;
        pTab->pCacheFirst = pFrame;
      }

      zonefileCacheCheck(pTab);
      return pFrame;
    }
  }
  return 0;
}

/*
** Return the index of the hash bucket that iFileid/iFrameOff belongs to.
*/
int zonefileCacheHash(ZonefileTab *pTab, i64 iFileid, i64 iFrameOff){
  u32 h;
  h = (iFileid & 0xFFFFFFFF) ^ (iFrameOff & 0xFFFFFFFF);
  return (h % pTab->nCacheBucket);
}

/*
** Store the frame object passed as the second argument in the frame
** cache belonging to table pTab.
*/
static int zonefileCacheStore(ZonefileTab *pTab, ZonefileFrame *pFrame){
  int h;

  /* Allocate the hash table if it has not already been allocated. */
  if( pTab->aCache==0 ){
    int nByte = pTab->nCacheSize * 2 * sizeof(ZonefileFrame*);
    pTab->aCache = (ZonefileFrame**)sqlite3_malloc(nByte);
    if( pTab->aCache==0 ){
      sqlite3_free(pFrame);
      return SQLITE_NOMEM;
    }
    memset(pTab->aCache, 0, nByte);
    pTab->nCacheBucket = pTab->nCacheSize * 2;
  }

  /* Add the new entry to the hash table */
  h = zonefileCacheHash(pTab, pFrame->iFileid, pFrame->iFrameOff);
  assert( h>=0 && h<pTab->nCacheBucket );
  pFrame->pHashNext = pTab->aCache[h];
  pTab->aCache[h] = pFrame;

  /* Add the new entry to the LRU list */
  pFrame->pLruPrev = 0;
  pFrame->pLruNext = pTab->pCacheFirst;
  pTab->pCacheFirst = pFrame;
  if( pFrame->pLruNext==0 ){
    pTab->pCacheLast = pFrame;
  }else{
    pFrame->pLruNext->pLruPrev = pFrame;
  }
  pTab->nCacheEntry++;

  if( pTab->nCacheEntry>pTab->nCacheSize ){
    ZonefileFrame **pp;

    /* Remove the oldest entry from the LRU list. */
    ZonefileFrame *pLast = pTab->pCacheLast;
    assert( pTab->pCacheLast );
    assert( pTab->nCacheEntry>1 );
    pTab->pCacheLast = pLast->pLruPrev;
    pTab->pCacheLast->pLruNext = 0;
    pTab->nCacheEntry--;

    /* Remove the same entry from the hash table. */
    h = zonefileCacheHash(pTab, pLast->iFileid, pLast->iFrameOff);
    assert( h>=0 && h<pTab->nCacheBucket );
    for(pp=&pTab->aCache[h]; *pp!=pLast; pp=&((*pp)->pHashNext));
    *pp = pLast->pHashNext;
    sqlite3_free(pLast);
  }
  zonefileCacheCheck(pTab);

  return SQLITE_OK;
}

/*
** Delete all resources associated with the frame-cache for table pTab.
*/
static void zonefileCacheDelete(ZonefileTab *pTab){
  ZonefileFrame *p;
  ZonefileFrame *pNext;
  for(p=pTab->pCacheFirst; p; p=pNext){
    pNext = p->pLruNext;
    sqlite3_free(p);
  }
  sqlite3_free(pTab->aCache);
}

static int zonefileValueReadCache(sqlite3_context *pCtx, ZonefileCsr *pCsr){
  int rc = SQLITE_OK;
  ZonefileTab *pTab = (ZonefileTab*)pCsr->base.pVtab;
  ZonefileFrame *pFrame = 0;
  i64 iFile = sqlite3_column_int64(pCsr->pSelect, 1);
  i64 iFrameOff = sqlite3_column_int64(pCsr->pSelect, 2);
  i64 iKeyOff = sqlite3_column_int64(pCsr->pSelect, 4);
  int nKeySz = sqlite3_column_int(pCsr->pSelect, 5);

  /* Check if this frame is already in the cache. If not, read it from 
  ** the file.  */
  pFrame = zonefileCacheFind(pTab, iFile, iFrameOff);
  if( pFrame==0 ){
    const char *zFile = 0;
    char *zErr = 0;
    FILE *pFd = 0;
    ZonefileHeader hdr = { 0 };
    ZonefileCompress *pCmpMethod = 0;
    ZonefileCodec *pCodec = 0;
    void *pCmp = 0;

    /* Open the file to read the blob from */
    rc = zonefileGetFile(pCtx, pCsr, &zFile);
    if( rc==SQLITE_OK ){
      pFd = zonefileFileOpen(zFile, 0, &zErr);
      if( pFd==0 ) rc = SQLITE_ERROR;
    }

    /* Read the zonefile header */
    if( rc==SQLITE_OK ){
      rc = zonefileReadHeader(pFd, zFile, &hdr, &zErr);
    }

    /* Find the compression method and open the compressor handle. */
    if( rc==SQLITE_OK ){
      rc = zfFindCompress(hdr.compressionTypeContent, &pCmpMethod, &zErr);
    }
    if( pCmpMethod ){
      int nDict = 0;
      u8 *aDict = 0;
      assert( rc==SQLITE_OK );
      if( hdr.byteOffsetDictionary ){
        nDict = hdr.byteOffsetFrames - hdr.byteOffsetDictionary;
        aDict = sqlite3_malloc(nDict);
        if( aDict==0 ){
          rc = SQLITE_NOMEM;
        }else{
          rc = zonefileFileRead(pFd, aDict, nDict, hdr.byteOffsetDictionary);
        }
      }
      if( rc==SQLITE_OK ){
        rc = pCmpMethod->xOpen(&pCmp, aDict, nDict);
      }
      sqlite3_free(aDict);
    }

    /* Find the encryption method and key. */
    if( rc==SQLITE_OK && hdr.encryptionType ){
      const char *z = 0;
      int n = zonefileKeyFind(pTab->pGlobal, pTab->zDb, pTab->zName, iFile, &z);
      if( n==0 ){
        zErr = sqlite3_mprintf("missing encryption key for file \"%s\"", zFile);
        rc = SQLITE_ERROR;
      }else{
        rc = zonefileCodecCreate(hdr.encryptionType, 0, (u8*)z,n,&pCodec,&zErr);
      }
    }

    /* Read some data into memory. */
    if( rc==SQLITE_OK ){
      int szFrame = sqlite3_column_int(pCsr->pSelect, 3);

      pFrame = (ZonefileFrame*)sqlite3_malloc(szFrame + sizeof(ZonefileFrame));
      if( pFrame==0 ){
        rc = SQLITE_NOMEM;
      }else{
        memset(pFrame, 0, sizeof(ZonefileFrame));
        pFrame->aBuf = (u8*)&pFrame[1];
        pFrame->nBuf = szFrame;
        pFrame->iFrameOff = iFrameOff;
        pFrame->iFileid = iFile;
        rc = zonefileFileRead(pFd, pFrame->aBuf, szFrame, iFrameOff);
      }
    }

    /* Decrypt data if necessary */
    if( rc==SQLITE_OK && pCodec ){
      zonefileCodecDecode(pCodec, pFrame->aBuf, pFrame->nBuf);
      pFrame->nBuf -= zonefileCodecNonceSize(pCodec);
    }

    /* Uncompress data if required */
    if( rc==SQLITE_OK && pCmpMethod ){
      ZonefileFrame *p = 0;
      int nOut = pCmpMethod->xUncompressSize(pCmp, pFrame->aBuf, pFrame->nBuf);

      p = (ZonefileFrame*)sqlite3_malloc(nOut + sizeof(ZonefileFrame));
      if( p==0 ){
        rc = SQLITE_NOMEM;
      }else{
        memset(p, 0, sizeof(ZonefileFrame));
        p->aBuf = (u8*)&p[1];
        p->nBuf = nOut;
        p->iFrameOff = iFrameOff;
        p->iFileid = iFile;
        rc = pCmpMethod->xUncompress(
            pCmp, p->aBuf, p->nBuf, pFrame->aBuf, pFrame->nBuf
        );
        sqlite3_free(pFrame);
        pFrame = p;
      }
    }

    if( rc!=SQLITE_OK ){
      sqlite3_free(pFrame);
      pFrame = 0;
    }else{
      rc = zonefileCacheStore(pTab, pFrame);
      if( rc!=SQLITE_OK ) pFrame = 0;
    }
    zonefileReleaseFile(pCsr);
    zonefileFileClose(pFd);
    zonefileCodecDestroy(pCodec);
    if( pCmpMethod ) pCmpMethod->xClose(pCmp);

    if( zErr ){
      assert( rc!=SQLITE_OK );
      sqlite3_result_error(pCtx, zErr, -1);
      sqlite3_free(zErr);
    }
  }

  if( pFrame ){
    assert( rc==SQLITE_OK );
    sqlite3_result_blob(pCtx, &pFrame->aBuf[iKeyOff], nKeySz, SQLITE_TRANSIENT);
  }

  return rc;
}

/* 
** zonefile virtual table module xColumn method.
*/
static int zonefileColumn(
  sqlite3_vtab_cursor *cur, 
  sqlite3_context *pCtx, 
  int i
){
  ZonefileCsr *pCsr = (ZonefileCsr*)cur;
  int rc = SQLITE_OK;
  switch( i ){
    case 0: /* k */
      sqlite3_result_value(pCtx, sqlite3_column_value(pCsr->pSelect, 0));
      break;
    case 1: /* v */
      if( sqlite3_column_type(pCsr->pSelect, 5)==SQLITE_NULL ){
        rc = zonefileValueReadDirect(pCtx, pCsr);
      }else{
        rc = zonefileValueReadCache(pCtx, pCsr);
      }
      break;
    case 2: /* fileid */
      sqlite3_result_value(pCtx, sqlite3_column_value(pCsr->pSelect, 1));
      break;
    default: { /* sz */
      int iCol;
      if( sqlite3_column_type(pCsr->pSelect, 5)==SQLITE_NULL ){
        iCol = 3;
      }else{
        iCol = 5;
      }
      sqlite3_result_value(pCtx, sqlite3_column_value(pCsr->pSelect, iCol));
      break;
    }
  }
  return rc;
}

/* 
** zonefile virtual table module xDisconnect method.
*/
static int zonefileDisconnect(sqlite3_vtab *pVtab){
  ZonefileTab *pTab = (ZonefileTab*)pVtab;
  zonefileCacheDelete(pTab);
  sqlite3_finalize(pTab->pIdToName);
  sqlite3_free(pTab);
  return SQLITE_OK;
}

/* 
** zonefile virtual table module xDestroy method.
*/
static int zonefileDestroy(sqlite3_vtab *pVtab){
  ZonefileTab *pTab = (ZonefileTab*)pVtab;
  int rc = SQLITE_OK;
  char *zSql = sqlite3_mprintf(
      "DROP TABLE IF EXISTS %Q.'%q_shadow_idx';"
      "DROP TABLE IF EXISTS %Q.'%q_shadow_file';"
      "DROP TABLE IF EXISTS %Q.'%q_shadow_frame';"
      "DROP TABLE IF EXISTS %Q.'%q_files';",
      pTab->zDb, pTab->zName, pTab->zDb, pTab->zName, 
      pTab->zDb, pTab->zName, pTab->zDb, pTab->zName
  );
  if( zSql==0 ){
    rc = SQLITE_NOMEM;
  }else{
    rc = sqlite3_exec(pTab->db, zSql, 0, 0, 0);
    sqlite3_free(zSql);
  }

  if( rc==SQLITE_OK ){
    zonefileDisconnect(pVtab);
  }
  return rc;
}

/* 
** zonefile virtual table module xRowid method.
*/
static int zonefileRowid(sqlite3_vtab_cursor *cur, sqlite_int64 *pRowid){
  ZonefileCsr *pCsr = (ZonefileCsr*)cur;
  *pRowid = sqlite3_column_int64(pCsr->pSelect, 0);
  return SQLITE_OK;
}

/*
** Register the "zonefile" extensions.
*/
static int zonefileRegister(sqlite3 *db){
  static sqlite3_module filesModule = {
    0,                            /* iVersion */
    zffCreate,                    /* xCreate - create a table */
    zffConnect,                   /* xConnect - connect to an existing table */
    zffBestIndex,                 /* xBestIndex - Determine search strategy */
    zffDisconnect,                /* xDisconnect - Disconnect from a table */
    zffDisconnect,                /* xDestroy - Drop a table */
    zffOpen,                      /* xOpen - open a cursor */
    zffClose,                     /* xClose - close a cursor */
    zffFilter,                    /* xFilter - configure scan constraints */
    zffNext,                      /* xNext - advance a cursor */
    zffEof,                       /* xEof */
    zffColumn,                    /* xColumn - read data */
    zffRowid,                     /* xRowid - read data */
    zffUpdate,                    /* xUpdate - write data */
    0,                            /* xBegin - begin transaction */
    0,                            /* xSync - sync transaction */
    0,                            /* xCommit - commit transaction */
    0,                            /* xRollback - rollback transaction */
    0,                            /* xFindFunction - function overloading */
    0,                            /* xRename - rename the table */
    0,                            /* xSavepoint */
    0,                            /* xRelease */
    0                             /* xRollbackTo */
  };
  static sqlite3_module zonefileModule = {
    0,                            /* iVersion */
    zonefileCreate,               /* xCreate - create a table */
    zonefileConnect,              /* xConnect - connect to an existing table */
    zonefileBestIndex,            /* xBestIndex - Determine search strategy */
    zonefileDisconnect,           /* xDisconnect - Disconnect from a table */
    zonefileDestroy,              /* xDestroy - Drop a table */
    zonefileOpen,                 /* xOpen - open a cursor */
    zonefileClose,                /* xClose - close a cursor */
    zonefileFilter,               /* xFilter - configure scan constraints */
    zonefileNext,                 /* xNext - advance a cursor */
    zonefileEof,                  /* xEof */
    zonefileColumn,               /* xColumn - read data */
    zonefileRowid,                /* xRowid - read data */
    0,                            /* xUpdate - write data */
    0,                            /* xBegin - begin transaction */
    0,                            /* xSync - sync transaction */
    0,                            /* xCommit - commit transaction */
    0,                            /* xRollback - rollback transaction */
    0,                            /* xFindFunction - function overloading */
    0,                            /* xRename - rename the table */
    0,                            /* xSavepoint */
    0,                            /* xRelease */
    0                             /* xRollbackTo */
  };

  struct Func {
    const char *z;
    int n;
    void (*x)(sqlite3_context*,int,sqlite3_value**);
  } aFunc[] = {
    { "zonefile_write", 2, zonefileWriteFunc },
    { "zonefile_write", 3, zonefileWriteFunc },
  };
  ZonefileGlobal *pGlobal = 0;

  int rc = SQLITE_OK;
  int i;

  for(i=0; rc==SQLITE_OK && i<sizeof(aFunc)/sizeof(aFunc[0]); i++){
    struct Func *p = &aFunc[i];
    rc = sqlite3_create_function(db, p->z, p->n, SQLITE_ANY, 0, p->x, 0, 0);
  }

  if( rc==SQLITE_OK ){
    pGlobal = (ZonefileGlobal*)sqlite3_malloc(sizeof(ZonefileGlobal));
    if( pGlobal==0 ){
      rc = SQLITE_NOMEM;
    }else{
      memset(pGlobal, 0, sizeof(ZonefileGlobal));
    }
  }

  if( rc==SQLITE_OK ){
    rc = sqlite3_create_module(
        db, "zonefile_files", &filesModule, (void*)pGlobal
    );
  }
  if( rc==SQLITE_OK ){
    rc = sqlite3_create_module_v2(db, "zonefile", &zonefileModule, 
        (void*)pGlobal, zonefileKeyDestroy
    );
    pGlobal = 0;
  }

  sqlite3_free(pGlobal);
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

/*
** 2022-09-06
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
** This file contains an experimental VFS layer that operates on a
** Key/Value storage engine where both keys and values must be pure
** text.
*/
#include <sqliteInt.h>
#if SQLITE_OS_KV

/*****************************************************************************
** Debugging logic
*/

/* SQLITE_KV_TRACE() is used for tracing calls to kvstorage routines. */
#if 0
#define SQLITE_KV_TRACE(X)  printf X;
#else
#define SQLITE_KV_TRACE(X)
#endif

/* SQLITE_KV_LOG() is used for tracing calls to the VFS interface */
#if 0
#define SQLITE_KV_LOG(X)  printf X;
#else
#define SQLITE_KV_LOG(X)
#endif


/*
** Forward declaration of objects used by this VFS implementation
*/
typedef struct KVVfsFile KVVfsFile;

/* A single open file.  There are only two files represented by this
** VFS - the database and the rollback journal.
*/
struct KVVfsFile {
  sqlite3_file base;              /* IO methods */
  const char *zClass;             /* Storage class */
  int isJournal;                  /* True if this is a journal file */
  unsigned int nJrnl;             /* Space allocated for aJrnl[] */
  char *aJrnl;                    /* Journal content */
  int szPage;                     /* Last known page size */
  sqlite3_int64 szDb;             /* Database file size.  -1 means unknown */
};

/*
** Methods for KVVfsFile
*/
static int kvvfsClose(sqlite3_file*);
static int kvvfsReadDb(sqlite3_file*, void*, int iAmt, sqlite3_int64 iOfst);
static int kvvfsReadJrnl(sqlite3_file*, void*, int iAmt, sqlite3_int64 iOfst);
static int kvvfsWriteDb(sqlite3_file*,const void*,int iAmt, sqlite3_int64);
static int kvvfsWriteJrnl(sqlite3_file*,const void*,int iAmt, sqlite3_int64);
static int kvvfsTruncateDb(sqlite3_file*, sqlite3_int64 size);
static int kvvfsTruncateJrnl(sqlite3_file*, sqlite3_int64 size);
static int kvvfsSyncDb(sqlite3_file*, int flags);
static int kvvfsSyncJrnl(sqlite3_file*, int flags);
static int kvvfsFileSizeDb(sqlite3_file*, sqlite3_int64 *pSize);
static int kvvfsFileSizeJrnl(sqlite3_file*, sqlite3_int64 *pSize);
static int kvvfsLock(sqlite3_file*, int);
static int kvvfsUnlock(sqlite3_file*, int);
static int kvvfsCheckReservedLock(sqlite3_file*, int *pResOut);
static int kvvfsFileControlDb(sqlite3_file*, int op, void *pArg);
static int kvvfsFileControlJrnl(sqlite3_file*, int op, void *pArg);
static int kvvfsSectorSize(sqlite3_file*);
static int kvvfsDeviceCharacteristics(sqlite3_file*);

/*
** Methods for sqlite3_vfs
*/
static int kvvfsOpen(sqlite3_vfs*, const char *, sqlite3_file*, int , int *);
static int kvvfsDelete(sqlite3_vfs*, const char *zName, int syncDir);
static int kvvfsAccess(sqlite3_vfs*, const char *zName, int flags, int *);
static int kvvfsFullPathname(sqlite3_vfs*, const char *zName, int, char *zOut);
static void *kvvfsDlOpen(sqlite3_vfs*, const char *zFilename);
static int kvvfsRandomness(sqlite3_vfs*, int nByte, char *zOut);
static int kvvfsSleep(sqlite3_vfs*, int microseconds);
static int kvvfsCurrentTime(sqlite3_vfs*, double*);
static int kvvfsCurrentTimeInt64(sqlite3_vfs*, sqlite3_int64*);

static sqlite3_vfs kvvfs_vfs = {
  1,                              /* iVersion */
  sizeof(KVVfsFile),              /* szOsFile */
  1024,                           /* mxPathname */
  0,                              /* pNext */
  "kvvfs",                        /* zName */
  0,                              /* pAppData */
  kvvfsOpen,                      /* xOpen */
  kvvfsDelete,                    /* xDelete */
  kvvfsAccess,                    /* xAccess */
  kvvfsFullPathname,              /* xFullPathname */
  kvvfsDlOpen,                    /* xDlOpen */
  0,                              /* xDlError */
  0,                              /* xDlSym */
  0,                              /* xDlClose */
  kvvfsRandomness,                /* xRandomness */
  kvvfsSleep,                     /* xSleep */
  kvvfsCurrentTime,               /* xCurrentTime */
  0,                              /* xGetLastError */
  kvvfsCurrentTimeInt64           /* xCurrentTimeInt64 */
};

/* Methods for sqlite3_file objects referencing a database file
*/
static sqlite3_io_methods kvvfs_db_io_methods = {
  1,                              /* iVersion */
  kvvfsClose,                     /* xClose */
  kvvfsReadDb,                    /* xRead */
  kvvfsWriteDb,                   /* xWrite */
  kvvfsTruncateDb,                /* xTruncate */
  kvvfsSyncDb,                    /* xSync */
  kvvfsFileSizeDb,                /* xFileSize */
  kvvfsLock,                      /* xLock */
  kvvfsUnlock,                    /* xUnlock */
  kvvfsCheckReservedLock,         /* xCheckReservedLock */
  kvvfsFileControlDb,             /* xFileControl */
  kvvfsSectorSize,                /* xSectorSize */
  kvvfsDeviceCharacteristics,     /* xDeviceCharacteristics */
  0,                              /* xShmMap */
  0,                              /* xShmLock */
  0,                              /* xShmBarrier */
  0,                              /* xShmUnmap */
  0,                              /* xFetch */
  0                               /* xUnfetch */
};

/* Methods for sqlite3_file objects referencing a rollback journal
*/
static sqlite3_io_methods kvvfs_jrnl_io_methods = {
  1,                              /* iVersion */
  kvvfsClose,                     /* xClose */
  kvvfsReadJrnl,                  /* xRead */
  kvvfsWriteJrnl,                 /* xWrite */
  kvvfsTruncateJrnl,              /* xTruncate */
  kvvfsSyncJrnl,                  /* xSync */
  kvvfsFileSizeJrnl,              /* xFileSize */
  kvvfsLock,                      /* xLock */
  kvvfsUnlock,                    /* xUnlock */
  kvvfsCheckReservedLock,         /* xCheckReservedLock */
  kvvfsFileControlJrnl,           /* xFileControl */
  kvvfsSectorSize,                /* xSectorSize */
  kvvfsDeviceCharacteristics,     /* xDeviceCharacteristics */
  0,                              /* xShmMap */
  0,                              /* xShmLock */
  0,                              /* xShmBarrier */
  0,                              /* xShmUnmap */
  0,                              /* xFetch */
  0                               /* xUnfetch */
};

/****** Storage subsystem **************************************************/
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

/* Forward declarations for the low-level storage engine
*/
#define KVSTORAGE_KEY_SZ  32

/* Expand the key name with an appropriate prefix and put the result
** zKeyOut[].  The zKeyOut[] buffer is assumed to hold at least
** KVSTORAGE_KEY_SZ bytes.
*/
static void kvstorageMakeKey(
  const char *zClass,
  const char *zKeyIn,
  char *zKeyOut
){
  sqlite3_snprintf(KVSTORAGE_KEY_SZ, zKeyOut, "kvvfs-%s-%s", zClass, zKeyIn);
}

#ifdef __EMSCRIPTEN__
/* Provide Emscripten-based impls of kvstorageWrite/Read/Delete()... */
#include <emscripten.h>
#include <emscripten/console.h>

/*
** WASM_KEEP is identical to EMSCRIPTEN_KEEPALIVE but is not
** Emscripten-specific. It explicitly includes marked functions for
** export into the target wasm file without requiring explicit listing
** of those functions in Emscripten's -sEXPORTED_FUNCTIONS=... list
** (or equivalent in other build platforms). Any function with neither
** this attribute nor which is listed as an explicit export will not
** be exported from the wasm file (but may still be used internally
** within the wasm file).
**
** The functions in this file (sqlite3-wasm.c) which require exporting
** are marked with this flag. They may also be added to any explicit
** build-time export list but need not be. All of these APIs are
** intended for use only within the project's own JS/WASM code, and
** not by client code, so an argument can be made for reducing their
** visibility by not including them in any build-time export lists.
**
** 2022-09-11: it's not yet _proven_ that this approach works in
** non-Emscripten builds. If not, such builds will need to export
** those using the --export=... wasm-ld flag (or equivalent). As of
** this writing we are tied to Emscripten for various reasons
** and cannot test the library with other build environments.
*/
#define WASM_KEEP __attribute__((used,visibility("default")))
/*
** An internal level of indirection for accessing the static
** kvstorageMakeKey() from EM_JS()-generated functions. This must be
** made available for export via Emscripten but is not intended to be
** used from client code. If called with a NULL zKeyOut it is a no-op.
** It returns KVSTORAGE_KEY_SZ, so JS code (which cannot see that
** constant) may call it with NULL arguments to get the size of the
** allocation they'll need for a kvvfs key.
**
** Maintenance reminder: Emscripten will install this in the Module
** init scope and will prefix its name with "_".
*/
WASM_KEEP
int sqlite3_wasm__kvvfsMakeKey(const char *zClass, const char *zKeyIn,
                               char *zKeyOut){
  if( 0!=zKeyOut ) kvstorageMakeKey(zClass, zKeyIn, zKeyOut);
  return KVSTORAGE_KEY_SZ;
}
/*
** Internal helper for kvstorageWrite/Read/Delete() which creates a
** storage key for the given zClass/zKeyIn combination. Returns a
** pointer to the key: a C string allocated on the WASM stack, or 0 if
** allocation fails. It is up to the caller to save/restore the stack
** before/after this operation.
*/
EM_JS(const char *, kvstorageMakeKeyOnJSStack,
      (const char *zClass, const char *zKeyIn),{
  if( 0==zClass || 0==zKeyIn) return 0;
  const zXKey = stackAlloc(_sqlite3_wasm__kvvfsMakeKey(0,0,0));
  if(zXKey) _sqlite3_wasm__kvvfsMakeKey(zClass, zKeyIn, zXKey);
  return zXKey;
});

/*
** JS impl of kvstorageWrite(). Main docs are in the C impl. This impl
** writes zData to the global sessionStorage (if zClass starts with
** 's') or localStorage, using a storage key derived from zClass and
** zKey.
*/
EM_JS(int, kvstorageWrite,
      (const char *zClass, const char *zKey, const char *zData),{
  const stack = stackSave();
  try {
    const zXKey = kvstorageMakeKeyOnJSStack(zClass,zKey);
    if(!zXKey) return 1/*OOM*/;
    const jKey = UTF8ToString(zXKey);
    /**
       We could simplify this function and eliminate the
       kvstorageMakeKey() symbol acrobatics if we'd simply hard-code
       the key algo into the 3 functions which need it:

       const jKey = "kvvfs-"+UTF8ToString(zClass)+"-"+UTF8ToString(zKey);
    */
    ((115/*=='s'*/===getValue(zClass))
     ? sessionStorage : localStorage).setItem(jKey, UTF8ToString(zData));
  }catch(e){
    console.error("kvstorageWrite()",e);
    return 1; // Can't access SQLITE_xxx from here
  }finally{
    stackRestore(stack);
  }
  return 0;
});

/*
** JS impl of kvstorageDelete(). Main docs are in the C impl. This
** impl generates a key derived from zClass and zKey, and removes the
** matching entry (if any) from global sessionStorage (if zClass
** starts with 's') or localStorage.
*/
EM_JS(int, kvstorageDelete,
      (const char *zClass, const char *zKey),{
  const stack = stackSave();
  try {
    const zXKey = kvstorageMakeKeyOnJSStack(zClass,zKey);
    if(!zXKey) return 1/*OOM*/;
    const jKey = UTF8ToString(zXKey);
    ((115/*=='s'*/===getValue(zClass))
     ? sessionStorage : localStorage).removeItem(jKey);
  }catch(e){
    console.error("kvstorageDelete()",e);
    return 1;
  }finally{
    stackRestore(stack);
  }
  return 0;
});

/*
** JS impl of kvstorageRead(). Main docs are in the C impl. This impl
** reads its data from the global sessionStorage (if zClass starts
** with 's') or localStorage, using a storage key derived from zClass
** and zKey.
*/
EM_JS(int, kvstorageRead,
      (const char *zClass, const char *zKey, char *zBuf, int nBuf),{
  const stack = stackSave();
  try {
    const zXKey = kvstorageMakeKeyOnJSStack(zClass,zKey);
    if(!zXKey) return -3/*OOM*/;
    const jKey = UTF8ToString(zXKey);
    const jV = ((115/*=='s'*/===getValue(zClass))
                ? sessionStorage : localStorage).getItem(jKey);
    if(!jV) return -1;
    const nV = jV.length /* Note that we are relying 100% on v being
                            ASCII so that jV.length is equal to the
                            C-string's byte length. */;
    if(nBuf<=0) return nV;
    else if(1===nBuf){
      setValue(zBuf, 0);
      return nV;
    }
    const zV = allocateUTF8OnStack(jV);
    if(nBuf > nV + 1) nBuf = nV + 1;
    HEAPU8.copyWithin(zBuf, zV, zV + nBuf - 1);
    setValue( zBuf + nBuf - 1, 0 );
    return nBuf - 1;
  }catch(e){
    console.error("kvstorageRead()",e);
    return -2;
  }finally{
    stackRestore(stack);
  }
});

/*
** This function exists for (1) WASM testing purposes and (2) as a
** hook to get Emscripten to export several EM_JS()-generated
** functions (if we don't reference them from exported C functions
** then they get stripped away at build time). It is not part of the
** public API and its signature and semantics may change at any time.
** It's not even part of the private API, for that matter - it's part
** of the Emscripten C/JS/WASM glue.
*/
WASM_KEEP
int sqlite3__wasm_emjs_kvvfs(int whichOp){
  int rc = 0;
  const char * zClass =
    "sezzion" /*don't collide with "session" records!*/;
  const char * zKey = "hello";
  switch( whichOp ){
    case 0: break;
    case 1:
      rc = kvstorageWrite(zClass, zKey, "world");
      break;
    case 2: {
      char buffer[128] = {0};
      char * zBuf = &buffer[0];
      rc = kvstorageRead(zClass, zKey, zBuf, (int)sizeof(buffer));
      emscripten_console_logf("kvstorageRead()=%d %s\n", rc, zBuf);
      break;
    }
    case 3:
      kvstorageDelete(zClass, zKey);
      break;
    case 4:
      kvstorageMakeKeyOnJSStack(0,0);
      break;
    default: break;
  }
  return rc;
}

#undef WASM_KEEP
#else /* end ifdef __EMSCRIPTEN__ */
/* Forward declarations for the low-level storage engine
*/
static int kvstorageWrite(const char*, const char *zKey, const char *zData);
static int kvstorageDelete(const char*, const char *zKey);
static int kvstorageRead(const char*, const char *zKey, char *zBuf, int nBuf);

/* Write content into a key.  zClass is the particular namespace of the
** underlying key/value store to use - either "local" or "session".
**
** Both zKey and zData are zero-terminated pure text strings.
**
** Return the number of errors.
*/
static int kvstorageWrite(
  const char *zClass,
  const char *zKey,
  const char *zData
){
  FILE *fd;
  char zXKey[KVSTORAGE_KEY_SZ];
  kvstorageMakeKey(zClass, zKey, zXKey);
  fd = fopen(zXKey, "wb");
  if( fd ){
    SQLITE_KV_TRACE(("KVVFS-WRITE  %-15s (%d) %.50s%s\n", zXKey,
                 (int)strlen(zData), zData,
                 strlen(zData)>50 ? "..." : ""));
    fputs(zData, fd);
    fclose(fd);
    return 0;
  }else{
    return 1;
  }
}

/* Delete a key (with its corresponding data) from the key/value
** namespace given by zClass.  If the key does not previously exist,
** this routine is a no-op.
*/
static int kvstorageDelete(const char *zClass, const char *zKey){
  char zXKey[KVSTORAGE_KEY_SZ];
  kvstorageMakeKey(zClass, zKey, zXKey);
  unlink(zXKey);
  SQLITE_KV_TRACE(("KVVFS-DELETE %-15s\n", zXKey));
  return 0;
}

/* Read the value associated with a zKey from the key/value namespace given
** by zClass and put the text data associated with that key in the first
** nBuf bytes of zBuf[].  The value might be truncated if zBuf is not large
** enough to hold it all.  The value put into zBuf must always be zero
** terminated, even if it gets truncated because nBuf is not large enough.
**
** Return the total number of bytes in the data, without truncation, and
** not counting the final zero terminator.   Return -1 if the key does
** not exist.
**
** If nBuf<=0 then this routine simply returns the size of the data without
** actually reading it.
*/
static int kvstorageRead(
  const char *zClass,
  const char *zKey,
  char *zBuf,
  int nBuf
){
  FILE *fd;
  struct stat buf;
  char zXKey[KVSTORAGE_KEY_SZ];
  kvstorageMakeKey(zClass, zKey, zXKey);
  if( access(zXKey, R_OK)!=0
   || stat(zXKey, &buf)!=0
   || !S_ISREG(buf.st_mode)
  ){
    SQLITE_KV_TRACE(("KVVFS-READ   %-15s (-1)\n", zXKey));
    return -1;
  }
  if( nBuf<=0 ){
    return (int)buf.st_size;
  }else if( nBuf==1 ){
    zBuf[0] = 0;
    SQLITE_KV_TRACE(("KVVFS-READ   %-15s (%d)\n", zXKey,
                 (int)buf.st_size));
    return (int)buf.st_size;
  }
  if( nBuf > buf.st_size + 1 ){
    nBuf = buf.st_size + 1;
  }
  fd = fopen(zXKey, "rb");
  if( fd==0 ){
    SQLITE_KV_TRACE(("KVVFS-READ   %-15s (-1)\n", zXKey));
    return -1;
  }else{
    sqlite3_int64 n = fread(zBuf, 1, nBuf-1, fd);
    fclose(fd);
    zBuf[n] = 0;
    SQLITE_KV_TRACE(("KVVFS-READ   %-15s (%lld) %.50s%s\n", zXKey,
                 n, zBuf, n>50 ? "..." : ""));
    return (int)n;
  }
}
#endif /* ifdef __EMSCRIPTEN__ */

/****** Utility subroutines ************************************************/

/*
** Encode binary into the text encoded used to persist on disk.
** The output text is stored in aOut[], which must be at least
** nData+1 bytes in length.
**
** Return the actual length of the encoded text, not counting the
** zero terminator at the end.
**
** Encoding format
** ---------------
**
**   *  Non-zero bytes are encoded as upper-case hexadecimal
**
**   *  A sequence of one or more zero-bytes that are not at the
**      beginning of the buffer are encoded as a little-endian
**      base-26 number using a..z.  "a" means 0.  "b" means 1,
**      "z" means 25.  "ab" means 26.  "ac" means 52.  And so forth.
**
**   *  Because there is no overlap between the encoding characters
**      of hexadecimal and base-26 numbers, it is always clear where
**      one stops and the next begins.
*/
static int kvvfsEncode(const char *aData, int nData, char *aOut){
  int i, j;
  const unsigned char *a = (const unsigned char*)aData;
  for(i=j=0; i<nData; i++){
    unsigned char c = a[i];
    if( c!=0 ){
      aOut[j++] = "0123456789ABCDEF"[c>>4];
      aOut[j++] = "0123456789ABCDEF"[c&0xf];
    }else{
      /* A sequence of 1 or more zeros is stored as a little-endian
      ** base-26 number using a..z as the digits. So one zero is "b".
      ** Two zeros is "c". 25 zeros is "z", 26 zeros is "ab", 27 is "bb",
      ** and so forth.
      */
      int k;
      for(k=1; i+k<nData && a[i+k]==0; k++){}
      i += k-1;
      while( k>0 ){
        aOut[j++] = 'a'+(k%26);
        k /= 26;
      }
    }
  }
  aOut[j] = 0;
  return j;
}

static const signed char kvvfsHexValue[256] = {
  -1, -1, -1, -1, -1, -1, -1, -1,   -1, -1, -1, -1, -1, -1, -1, -1,
  -1, -1, -1, -1, -1, -1, -1, -1,   -1, -1, -1, -1, -1, -1, -1, -1,
  -1, -1, -1, -1, -1, -1, -1, -1,   -1, -1, -1, -1, -1, -1, -1, -1,
   0,  1,  2,  3,  4,  5,  6,  7,    8,  9, -1, -1, -1, -1, -1, -1,
  -1, 10, 11, 12, 13, 14, 15, -1,   -1, -1, -1, -1, -1, -1, -1, -1,
  -1, -1, -1, -1, -1, -1, -1, -1,   -1, -1, -1, -1, -1, -1, -1, -1,
  -1, -1, -1, -1, -1, -1, -1, -1,   -1, -1, -1, -1, -1, -1, -1, -1,
  -1, -1, -1, -1, -1, -1, -1, -1,   -1, -1, -1, -1, -1, -1, -1, -1,

  -1, -1, -1, -1, -1, -1, -1, -1,   -1, -1, -1, -1, -1, -1, -1, -1,
  -1, -1, -1, -1, -1, -1, -1, -1,   -1, -1, -1, -1, -1, -1, -1, -1,
  -1, -1, -1, -1, -1, -1, -1, -1,   -1, -1, -1, -1, -1, -1, -1, -1,
  -1, -1, -1, -1, -1, -1, -1, -1,   -1, -1, -1, -1, -1, -1, -1, -1,
  -1, -1, -1, -1, -1, -1, -1, -1,   -1, -1, -1, -1, -1, -1, -1, -1,
  -1, -1, -1, -1, -1, -1, -1, -1,   -1, -1, -1, -1, -1, -1, -1, -1,
  -1, -1, -1, -1, -1, -1, -1, -1,   -1, -1, -1, -1, -1, -1, -1, -1,
  -1, -1, -1, -1, -1, -1, -1, -1,   -1, -1, -1, -1, -1, -1, -1, -1
};

/*
** Decode the text encoding back to binary.  The binary content is
** written into pOut, which must be at least nOut bytes in length.
**
** The return value is the number of bytes actually written into aOut[].
*/
static int kvvfsDecode(const char *a, char *aOut, int nOut){
  int i, j;
  int c;
  const unsigned char *aIn = (const unsigned char*)a;
  i = 0;
  j = 0;
  while( 1 ){
    c = kvvfsHexValue[aIn[i]];
    if( c<0 ){
      int n = 0;
      int mult = 1;
      c = aIn[i];
      if( c==0 ) break;
      while( c>='a' && c<='z' ){
        n += (c - 'a')*mult;
        mult *= 26;
        c = aIn[++i];
      }
      if( j+n>nOut ) return -1;
      memset(&aOut[j], 0, n);
      j += n;
      c = aIn[i];
      if( c==0 ) break;
    }else{
      aOut[j] = c<<4;
      c = kvvfsHexValue[aIn[++i]];
      if( c<0 ) break;
      aOut[j++] += c;
      i++;
    }
  }
  return j;
}

/*
** Decode a complete journal file.  Allocate space in pFile->aJrnl
** and store the decoding there.  Or leave pFile->aJrnl set to NULL
** if an error is encountered.
**
** The first few characters of the text encoding will be a little-endian
** base-26 number (digits a..z) that is the total number of bytes
** in the decoded journal file image.  This base-26 number is followed
** by a single space, then the encoding of the journal.  The space
** separator is required to act as a terminator for the base-26 number.
*/
static void kvvfsDecodeJournal(
  KVVfsFile *pFile,      /* Store decoding in pFile->aJrnl */
  const char *zTxt,      /* Text encoding.  Zero-terminated */
  int nTxt               /* Bytes in zTxt, excluding zero terminator */
){
  unsigned int n;
  int c, i, mult;
  i = 0;
  mult = 1;
  while( (c = zTxt[i++])>='a' && c<='z' ){
    n += (zTxt[i] - 'a')*mult;
    mult *= 26;
  }
  sqlite3_free(pFile->aJrnl);
  pFile->aJrnl = sqlite3_malloc64( n );
  if( pFile->aJrnl==0 ){
    pFile->nJrnl = 0;
    return;
  }
  pFile->nJrnl = n;
  n = kvvfsDecode(zTxt+i, pFile->aJrnl, pFile->nJrnl);
  if( n<pFile->nJrnl ){
    sqlite3_free(pFile->aJrnl);
    pFile->aJrnl = 0;
    pFile->nJrnl = 0;
  }
}

/*
** Read or write the "sz" element, containing the database file size.
*/
static sqlite3_int64 kvvfsReadFileSize(KVVfsFile *pFile){
  char zData[50];
  zData[0] = 0;
  kvstorageRead(pFile->zClass, "sz", zData, sizeof(zData)-1);
  return strtoll(zData, 0, 0);
}
static int kvvfsWriteFileSize(KVVfsFile *pFile, sqlite3_int64 sz){
  char zData[50];
  sqlite3_snprintf(sizeof(zData), zData, "%lld", sz);
  return kvstorageWrite(pFile->zClass, "sz", zData);
}

/****** sqlite3_io_methods methods ******************************************/

/*
** Close an kvvfs-file.
*/
static int kvvfsClose(sqlite3_file *pProtoFile){
  KVVfsFile *pFile = (KVVfsFile *)pProtoFile;

  SQLITE_KV_LOG(("xClose %s %s\n", pFile->zClass, 
             pFile->isJournal ? "journal" : "db"));
  sqlite3_free(pFile->aJrnl);
  return SQLITE_OK;
}

/*
** Read from the -journal file.
*/
static int kvvfsReadJrnl(
  sqlite3_file *pProtoFile,
  void *zBuf, 
  int iAmt, 
  sqlite_int64 iOfst
){
  KVVfsFile *pFile = (KVVfsFile*)pProtoFile;
  assert( pFile->isJournal );
  SQLITE_KV_LOG(("xRead('%s-journal',%d,%lld)\n", pFile->zClass, iAmt, iOfst));
  if( pFile->aJrnl==0 ){
    int szTxt = kvstorageRead(pFile->zClass, "jrnl", 0, 0);
    char *aTxt;
    if( szTxt<=4 ){
      return SQLITE_IOERR;
    }
    aTxt = sqlite3_malloc64( szTxt+1 );
    if( aTxt==0 ) return SQLITE_NOMEM;
    kvstorageRead(pFile->zClass, "jrnl", aTxt, szTxt+1);
    kvvfsDecodeJournal(pFile, aTxt, szTxt);
    sqlite3_free(aTxt);
    if( pFile->aJrnl==0 ) return SQLITE_IOERR;
  }
  if( iOfst+iAmt>pFile->nJrnl ){
    return SQLITE_IOERR_SHORT_READ;
  }
  memcpy(zBuf, pFile->aJrnl+iOfst, iAmt);
  return SQLITE_OK;
}

/*
** Read from the database file.
*/
static int kvvfsReadDb(
  sqlite3_file *pProtoFile,
  void *zBuf, 
  int iAmt, 
  sqlite_int64 iOfst
){
  KVVfsFile *pFile = (KVVfsFile*)pProtoFile;
  unsigned int pgno;
  int got, n;
  char zKey[30];
  char aData[133073];
  assert( iOfst>=0 );
  assert( iAmt>=0 );
  SQLITE_KV_LOG(("xRead('%s-db',%d,%lld)\n", pFile->zClass, iAmt, iOfst));
  if( iOfst+iAmt>=512 ){
    if( (iOfst % iAmt)!=0 ){
      return SQLITE_IOERR_READ;
    }
    if( (iAmt & (iAmt-1))!=0 || iAmt<512 || iAmt>65536 ){
      return SQLITE_IOERR_READ;
    }
    pFile->szPage = iAmt;
    pgno = 1 + iOfst/iAmt;
  }else{
    pgno = 1;
  }
  sqlite3_snprintf(sizeof(zKey), zKey, "%u", pgno);
  got = kvstorageRead(pFile->zClass, zKey, aData, sizeof(aData)-1);
  if( got<0 ){
    n = 0;
  }else{
    aData[got] = 0;
    if( iOfst+iAmt<512 ){
      int k = iOfst+iAmt;
      aData[k*2] = 0;
      n = kvvfsDecode(aData, &aData[2000], sizeof(aData)-2000);
      if( n>=iOfst+iAmt ){
        memcpy(zBuf, &aData[2000+iOfst], iAmt);
        n = iAmt;
      }else{
        n = 0;
      }
    }else{
      n = kvvfsDecode(aData, zBuf, iAmt);
    }
  }
  if( n<iAmt ){
    memset(zBuf+n, 0, iAmt-n);
    return SQLITE_IOERR_SHORT_READ;
  }
  return SQLITE_OK;
}


/*
** Write into the -journal file.
*/
static int kvvfsWriteJrnl(
  sqlite3_file *pProtoFile,
  const void *zBuf, 
  int iAmt, 
  sqlite_int64 iOfst
){
  KVVfsFile *pFile = (KVVfsFile*)pProtoFile;
  sqlite3_int64 iEnd = iOfst+iAmt;
  SQLITE_KV_LOG(("xWrite('%s-journal',%d,%lld)\n", pFile->zClass, iAmt, iOfst));
  if( iEnd>=0x10000000 ) return SQLITE_FULL;
  if( pFile->aJrnl==0 || pFile->nJrnl<iEnd ){
    char *aNew = sqlite3_realloc(pFile->aJrnl, iEnd);
    if( aNew==0 ){
      return SQLITE_IOERR_NOMEM;
    }
    pFile->aJrnl = aNew;
    if( pFile->nJrnl<iOfst ){
      memset(pFile->aJrnl+pFile->nJrnl, 0, iOfst-pFile->nJrnl);
    }
    pFile->nJrnl = iEnd;
  }
  memcpy(pFile->aJrnl+iOfst, zBuf, iAmt);
  return SQLITE_OK;
}

/*
** Write into the database file.
*/
static int kvvfsWriteDb(
  sqlite3_file *pProtoFile,
  const void *zBuf, 
  int iAmt, 
  sqlite_int64 iOfst
){
  KVVfsFile *pFile = (KVVfsFile*)pProtoFile;
  unsigned int pgno;
  char zKey[30];
  char aData[131073];
  SQLITE_KV_LOG(("xWrite('%s-db',%d,%lld)\n", pFile->zClass, iAmt, iOfst));
  assert( iAmt>=512 && iAmt<=65536 );
  assert( (iAmt & (iAmt-1))==0 );
  pgno = 1 + iOfst/iAmt;
  sqlite3_snprintf(sizeof(zKey), zKey, "%u", pgno);
  kvvfsEncode(zBuf, iAmt, aData);
  if( kvstorageWrite(pFile->zClass, zKey, aData) ){
    return SQLITE_IOERR;
  }
  if( iOfst+iAmt > pFile->szDb ){
    pFile->szDb = iOfst + iAmt;
  }
  return SQLITE_OK;
}

/*
** Truncate an kvvfs-file.
*/
static int kvvfsTruncateJrnl(sqlite3_file *pProtoFile, sqlite_int64 size){
  KVVfsFile *pFile = (KVVfsFile *)pProtoFile;
  SQLITE_KV_LOG(("xTruncate('%s-journal',%lld)\n", pFile->zClass, size));
  assert( size==0 );
  kvstorageDelete(pFile->zClass, "jrnl");
  sqlite3_free(pFile->aJrnl);
  pFile->aJrnl = 0;
  pFile->nJrnl = 0;
  return SQLITE_OK;
}
static int kvvfsTruncateDb(sqlite3_file *pProtoFile, sqlite_int64 size){
  KVVfsFile *pFile = (KVVfsFile *)pProtoFile;
  if( pFile->szDb>size
   && pFile->szPage>0 
   && (size % pFile->szPage)==0
  ){
    char zKey[50];
    unsigned int pgno, pgnoMax;
    SQLITE_KV_LOG(("xTruncate('%s-db',%lld)\n", pFile->zClass, size));
    pgno = 1 + size/pFile->szPage;
    pgnoMax = 2 + pFile->szDb/pFile->szPage;
    while( pgno<=pgnoMax ){
      sqlite3_snprintf(sizeof(zKey), zKey, "%u", pgno);
      kvstorageDelete(pFile->zClass, zKey);
      pgno++;
    }
    pFile->szDb = size;
    return kvvfsWriteFileSize(pFile, size) ? SQLITE_IOERR : SQLITE_OK;
  }
  return SQLITE_IOERR;
}

/*
** Sync an kvvfs-file.
*/
static int kvvfsSyncJrnl(sqlite3_file *pProtoFile, int flags){
  int i, n;
  KVVfsFile *pFile = (KVVfsFile *)pProtoFile;
  char *zOut;
  SQLITE_KV_LOG(("xSync('%s-journal')\n", pFile->zClass));
  if( pFile->nJrnl<=0 ){
    return kvvfsTruncateJrnl(pProtoFile, 0);
  }
  zOut = sqlite3_malloc64( pFile->nJrnl*2 + 50 );
  if( zOut==0 ){
    return SQLITE_IOERR_NOMEM;
  }
  n = pFile->nJrnl;
  i = 0;
  do{
    zOut[i++] = 'a' + (n%26);
    n /= 26;
  }while( n>0 );
  zOut[i++] = ' ';
  kvvfsEncode(pFile->aJrnl, pFile->nJrnl, &zOut[i]);
  i = kvstorageWrite(pFile->zClass, "jrnl", zOut);
  sqlite3_free(zOut);
  return i ? SQLITE_IOERR : SQLITE_OK;
}
static int kvvfsSyncDb(sqlite3_file *pProtoFile, int flags){
  return SQLITE_OK;
}

/*
** Return the current file-size of an kvvfs-file.
*/
static int kvvfsFileSizeJrnl(sqlite3_file *pProtoFile, sqlite_int64 *pSize){
  KVVfsFile *pFile = (KVVfsFile *)pProtoFile;
  SQLITE_KV_LOG(("xFileSize('%s-journal')\n", pFile->zClass));
  *pSize = pFile->nJrnl;
  return SQLITE_OK;
}
static int kvvfsFileSizeDb(sqlite3_file *pProtoFile, sqlite_int64 *pSize){
  KVVfsFile *pFile = (KVVfsFile *)pProtoFile;
  SQLITE_KV_LOG(("xFileSize('%s-db')\n", pFile->zClass));
  if( pFile->szDb>=0 ){
    *pSize = pFile->szDb;
  }else{
    *pSize = kvvfsReadFileSize(pFile);
  }
  return SQLITE_OK;
}

/*
** Lock an kvvfs-file.
*/
static int kvvfsLock(sqlite3_file *pProtoFile, int eLock){
  KVVfsFile *pFile = (KVVfsFile *)pProtoFile;
  assert( !pFile->isJournal );
  SQLITE_KV_LOG(("xLock(%s,%d)\n", pFile->zClass, eLock));

  if( eLock!=SQLITE_LOCK_NONE ){
    pFile->szDb = kvvfsReadFileSize(pFile);
  }
  return SQLITE_OK;
}

/*
** Unlock an kvvfs-file.
*/
static int kvvfsUnlock(sqlite3_file *pProtoFile, int eLock){
  KVVfsFile *pFile = (KVVfsFile *)pProtoFile;
  assert( !pFile->isJournal );
  SQLITE_KV_LOG(("xUnlock(%s,%d)\n", pFile->zClass, eLock));
  if( eLock==SQLITE_LOCK_NONE ){
    pFile->szDb = -1;
  }
  return SQLITE_OK;
}

/*
** Check if another file-handle holds a RESERVED lock on an kvvfs-file.
*/
static int kvvfsCheckReservedLock(sqlite3_file *pProtoFile, int *pResOut){
  SQLITE_KV_LOG(("xCheckReservedLock\n"));
  *pResOut = 0;
  return SQLITE_OK;
}

/*
** File control method. For custom operations on an kvvfs-file.
*/
static int kvvfsFileControlJrnl(sqlite3_file *pProtoFile, int op, void *pArg){
  SQLITE_KV_LOG(("xFileControl(%d) on journal\n", op));
  return SQLITE_NOTFOUND;
}
static int kvvfsFileControlDb(sqlite3_file *pProtoFile, int op, void *pArg){
  SQLITE_KV_LOG(("xFileControl(%d) on database\n", op));
  if( op==SQLITE_FCNTL_SYNC ){
    KVVfsFile *pFile = (KVVfsFile *)pProtoFile;
    int rc = SQLITE_OK;
    SQLITE_KV_LOG(("xSync('%s-db')\n", pFile->zClass));
    if( pFile->szDb>0 && 0!=kvvfsWriteFileSize(pFile, pFile->szDb) ){
      rc = SQLITE_IOERR;
    }
    return rc;
  }
  return SQLITE_NOTFOUND;
}

/*
** Return the sector-size in bytes for an kvvfs-file.
*/
static int kvvfsSectorSize(sqlite3_file *pFile){
  return 512;
}

/*
** Return the device characteristic flags supported by an kvvfs-file.
*/
static int kvvfsDeviceCharacteristics(sqlite3_file *pProtoFile){
  return 0;
}

/****** sqlite3_vfs methods *************************************************/

/*
** Open an kvvfs file handle.
*/
static int kvvfsOpen(
  sqlite3_vfs *pProtoVfs,
  const char *zName,
  sqlite3_file *pProtoFile,
  int flags,
  int *pOutFlags
){
  KVVfsFile *pFile = (KVVfsFile*)pProtoFile;
  SQLITE_KV_LOG(("xOpen(\"%s\")\n", zName));
  if( strcmp(zName, "local")==0
   || strcmp(zName, "session")==0
  ){
    pFile->isJournal = 0;
    pFile->base.pMethods = &kvvfs_db_io_methods;
  }else
  if( strcmp(zName, "local-journal")==0 
   || strcmp(zName, "session-journal")==0
  ){
    pFile->isJournal = 1;
    pFile->base.pMethods = &kvvfs_jrnl_io_methods;
  }else{
    return SQLITE_CANTOPEN;
  }
  if( zName[0]=='s' ){
    pFile->zClass = "session";
  }else{
    pFile->zClass = "local";
  }
  pFile->aJrnl = 0;
  pFile->nJrnl = 0;
  pFile->szPage = -1;
  pFile->szDb = -1;
  return SQLITE_OK;
}

/*
** Delete the file located at zPath. If the dirSync argument is true,
** ensure the file-system modifications are synced to disk before
** returning.
*/
static int kvvfsDelete(sqlite3_vfs *pVfs, const char *zPath, int dirSync){
  if( strcmp(zPath, "local-journal")==0 ){
    kvstorageDelete("local", "jrnl");
  }else
  if( strcmp(zPath, "session-journal")==0 ){
    kvstorageDelete("session", "jrnl");
  }
  return SQLITE_OK;
}

/*
** Test for access permissions. Return true if the requested permission
** is available, or false otherwise.
*/
static int kvvfsAccess(
  sqlite3_vfs *pProtoVfs, 
  const char *zPath, 
  int flags, 
  int *pResOut
){
  SQLITE_KV_LOG(("xAccess(\"%s\")\n", zPath));
  if( strcmp(zPath, "local-journal")==0 ){
    *pResOut = kvstorageRead("local", "jrnl", 0, 0)>0;
  }else
  if( strcmp(zPath, "session-journal")==0 ){
    *pResOut = kvstorageRead("session", "jrnl", 0, 0)>0;
  }else
  if( strcmp(zPath, "local")==0 ){
    *pResOut = kvstorageRead("local", "sz", 0, 0)>0;
  }else
  if( strcmp(zPath, "session")==0 ){
    *pResOut = kvstorageRead("session", "sz", 0, 0)>0;
  }else
  {
    *pResOut = 0;
  }
  SQLITE_KV_LOG(("xAccess returns %d\n",*pResOut));
  return SQLITE_OK;
}

/*
** Populate buffer zOut with the full canonical pathname corresponding
** to the pathname in zPath. zOut is guaranteed to point to a buffer
** of at least (INST_MAX_PATHNAME+1) bytes.
*/
static int kvvfsFullPathname(
  sqlite3_vfs *pVfs, 
  const char *zPath, 
  int nOut, 
  char *zOut
){
  size_t nPath;
#ifdef SQLITE_OS_KV_ALWAYS_LOCAL
  zPath = "local";
#endif
  nPath = strlen(zPath);
  SQLITE_KV_LOG(("xFullPathname(\"%s\")\n", zPath));
  if( nOut<nPath+1 ) nPath = nOut - 1;
  memcpy(zOut, zPath, nPath);
  zOut[nPath] = 0;
  return SQLITE_OK;
}

/*
** Open the dynamic library located at zPath and return a handle.
*/
static void *kvvfsDlOpen(sqlite3_vfs *pVfs, const char *zPath){
  return 0;
}

/*
** Populate the buffer pointed to by zBufOut with nByte bytes of 
** random data.
*/
static int kvvfsRandomness(sqlite3_vfs *pVfs, int nByte, char *zBufOut){
  memset(zBufOut, 0, nByte);
  return nByte;
}

/*
** Sleep for nMicro microseconds. Return the number of microseconds 
** actually slept.
*/
static int kvvfsSleep(sqlite3_vfs *pVfs, int nMicro){
  return SQLITE_OK;
}

/*
** Return the current time as a Julian Day number in *pTimeOut.
*/
static int kvvfsCurrentTime(sqlite3_vfs *pVfs, double *pTimeOut){
  sqlite3_int64 i = 0;
  int rc;
  rc = kvvfsCurrentTimeInt64(0, &i);
  *pTimeOut = i/86400000.0;
  return rc;
}
#include <sys/time.h>
static int kvvfsCurrentTimeInt64(sqlite3_vfs *pVfs, sqlite3_int64 *pTimeOut){
  static const sqlite3_int64 unixEpoch = 24405875*(sqlite3_int64)8640000;
  struct timeval sNow;
  (void)gettimeofday(&sNow, 0);  /* Cannot fail given valid arguments */
  *pTimeOut = unixEpoch + 1000*(sqlite3_int64)sNow.tv_sec + sNow.tv_usec/1000;
  return SQLITE_OK;
}

/* 
** This routine is called initialize the KV-vfs as the default VFS.
*/
int sqlite3_os_init(void){
  return sqlite3_vfs_register(&kvvfs_vfs, 1);
}
int sqlite3_os_end(void){
  return SQLITE_OK;
}
#endif /* SQLITE_OS_KV */

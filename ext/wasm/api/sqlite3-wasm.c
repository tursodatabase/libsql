/*
** This file requires access to sqlite3.c static state in order to
** implement certain WASM-specific features, and thus directly
** includes that file. Unlike the rest of sqlite3.c, this file
** requires compiling with -std=c99 (or equivalent, or a later C
** version) because it makes use of features not available in C89.
**
** At it's simplest, to build sqlite3.wasm either place this file
** in the same directory as sqlite3.c/h before compilation or use the
** -I/path flag to tell the compiler where to find both of those
** files, then compile this file. For example:
**
** emcc -o sqlite3.wasm ... -I/path/to/sqlite3-c-and-h sqlite3-wasm.c
*/

#ifndef SQLITE_DEFAULT_UNIX_VFS
# define SQLITE_DEFAULT_UNIX_VFS "unix-none"
#endif
#ifndef SQLITE_OMIT_DEPRECATED
# define SQLITE_OMIT_DEPRECATED
#endif
#ifndef SQLITE_OMIT_LOAD_EXTENSION
# define SQLITE_OMIT_LOAD_EXTENSION
#endif
#ifndef SQLITE_OMIT_SHARED_CACHE
# define SQLITE_OMIT_SHARED_CACHE
#endif
#ifndef SQLITE_OMIT_UTF16
# define SQLITE_OMIT_UTF16
#endif
#ifndef SQLITE_OS_KV_OPTIONAL
# define SQLITE_OS_KV_OPTIONAL 1
#endif
#ifndef SQLITE_TEMP_STORE
# define SQLITE_TEMP_STORE 3
#endif
#ifndef SQLITE_THREADSAFE
# define SQLITE_THREADSAFE 0
#endif

#include "sqlite3.c" /* yes, .c instead of .h. */

/*
** WASM_KEEP is identical to EMSCRIPTEN_KEEPALIVE but is not
** Emscripten-specific. It explicitly marks functions for export into
** the target wasm file without requiring explicit listing of those
** functions in Emscripten's -sEXPORTED_FUNCTIONS=... list (or
** equivalent in other build platforms). Any function with neither
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
// See also:
//__attribute__((export_name("theExportedName"), used, visibility("default")))

/*
** This function is NOT part of the sqlite3 public API. It is strictly
** for use by the sqlite project's own JS/WASM bindings.
**
** For purposes of certain hand-crafted C/Wasm function bindings, we
** need a way of reporting errors which is consistent with the rest of
** the C API, as opposed to throwing JS exceptions. To that end, this
** internal-use-only function is a thin proxy around
** sqlite3ErrorWithMessage(). The intent is that it only be used from
** Wasm bindings such as sqlite3_prepare_v2/v3(), and definitely not
** from client code.
**
** Returns err_code.
*/
WASM_KEEP
int sqlite3_wasm_db_error(sqlite3*db, int err_code, const char *zMsg){
  if(0!=zMsg){
    const int nMsg = sqlite3Strlen30(zMsg);
    sqlite3ErrorWithMsg(db, err_code, "%.*s", nMsg, zMsg);
  }else{
    sqlite3ErrorWithMsg(db, err_code, NULL);
  }
  return err_code;
}

/*
** This function is NOT part of the sqlite3 public API. It is strictly
** for use by the sqlite project's own JS/WASM bindings. Unlike the
** rest of the sqlite3 API, this part requires C99 for snprintf() and
** variadic macros.
**
** Returns a string containing a JSON-format "enum" of C-level
** constants intended to be imported into the JS environment. The JSON
** is initialized the first time this function is called and that
** result is reused for all future calls.
**
** If this function returns NULL then it means that the internal
** buffer is not large enough for the generated JSON. In debug builds
** that will trigger an assert().
*/
WASM_KEEP
const char * sqlite3_wasm_enum_json(void){
  static char strBuf[1024 * 12] = {0} /* where the JSON goes */;
  int n = 0, childCount = 0, structCount = 0
    /* output counters for figuring out where commas go */;
  char * pos = &strBuf[1] /* skip first byte for now to help protect
                          ** against a small race condition */;
  char const * const zEnd = pos + sizeof(strBuf) /* one-past-the-end */;
  if(strBuf[0]) return strBuf;
  /* Leave strBuf[0] at 0 until the end to help guard against a tiny
  ** race condition. If this is called twice concurrently, they might
  ** end up both writing to strBuf, but they'll both write the same
  ** thing, so that's okay. If we set byte 0 up front then the 2nd
  ** instance might return and use the string before the 1st instance
  ** is done filling it. */

/* Core output macros... */
#define lenCheck assert(pos < zEnd - 128 \
  && "sqlite3_wasm_enum_json() buffer is too small."); \
  if(pos >= zEnd - 128) return 0
#define outf(format,...) \
  pos += snprintf(pos, ((size_t)(zEnd - pos)), format, __VA_ARGS__); \
  lenCheck
#define out(TXT) outf("%s",TXT)
#define CloseBrace(LEVEL) \
  assert(LEVEL<5); memset(pos, '}', LEVEL); pos+=LEVEL; lenCheck

/* Macros for emitting maps of integer- and string-type macros to
** their values. */
#define DefGroup(KEY) n = 0; \
  outf("%s\"" #KEY "\": {",(childCount++ ? "," : ""));
#define DefInt(KEY)                                     \
  outf("%s\"%s\": %d", (n++ ? ", " : ""), #KEY, (int)KEY)
#define DefStr(KEY)                                     \
  outf("%s\"%s\": \"%s\"", (n++ ? ", " : ""), #KEY, KEY)
#define _DefGroup CloseBrace(1)

  DefGroup(version) {
    DefInt(SQLITE_VERSION_NUMBER);
    DefStr(SQLITE_VERSION);
    DefStr(SQLITE_SOURCE_ID);
  } _DefGroup;

  DefGroup(resultCodes) {
    DefInt(SQLITE_OK);
    DefInt(SQLITE_ERROR);
    DefInt(SQLITE_INTERNAL);
    DefInt(SQLITE_PERM);
    DefInt(SQLITE_ABORT);
    DefInt(SQLITE_BUSY);
    DefInt(SQLITE_LOCKED);
    DefInt(SQLITE_NOMEM);
    DefInt(SQLITE_READONLY);
    DefInt(SQLITE_INTERRUPT);
    DefInt(SQLITE_IOERR);
    DefInt(SQLITE_CORRUPT);
    DefInt(SQLITE_NOTFOUND);
    DefInt(SQLITE_FULL);
    DefInt(SQLITE_CANTOPEN);
    DefInt(SQLITE_PROTOCOL);
    DefInt(SQLITE_EMPTY);
    DefInt(SQLITE_SCHEMA);
    DefInt(SQLITE_TOOBIG);
    DefInt(SQLITE_CONSTRAINT);
    DefInt(SQLITE_MISMATCH);
    DefInt(SQLITE_MISUSE);
    DefInt(SQLITE_NOLFS);
    DefInt(SQLITE_AUTH);
    DefInt(SQLITE_FORMAT);
    DefInt(SQLITE_RANGE);
    DefInt(SQLITE_NOTADB);
    DefInt(SQLITE_NOTICE);
    DefInt(SQLITE_WARNING);
    DefInt(SQLITE_ROW);
    DefInt(SQLITE_DONE);

    // Extended Result Codes
    DefInt(SQLITE_ERROR_MISSING_COLLSEQ);
    DefInt(SQLITE_ERROR_RETRY);
    DefInt(SQLITE_ERROR_SNAPSHOT);
    DefInt(SQLITE_IOERR_READ);
    DefInt(SQLITE_IOERR_SHORT_READ);
    DefInt(SQLITE_IOERR_WRITE);
    DefInt(SQLITE_IOERR_FSYNC);
    DefInt(SQLITE_IOERR_DIR_FSYNC);
    DefInt(SQLITE_IOERR_TRUNCATE);
    DefInt(SQLITE_IOERR_FSTAT);
    DefInt(SQLITE_IOERR_UNLOCK);
    DefInt(SQLITE_IOERR_RDLOCK);
    DefInt(SQLITE_IOERR_DELETE);
    DefInt(SQLITE_IOERR_BLOCKED);
    DefInt(SQLITE_IOERR_NOMEM);
    DefInt(SQLITE_IOERR_ACCESS);
    DefInt(SQLITE_IOERR_CHECKRESERVEDLOCK);
    DefInt(SQLITE_IOERR_LOCK);
    DefInt(SQLITE_IOERR_CLOSE);
    DefInt(SQLITE_IOERR_DIR_CLOSE);
    DefInt(SQLITE_IOERR_SHMOPEN);
    DefInt(SQLITE_IOERR_SHMSIZE);
    DefInt(SQLITE_IOERR_SHMLOCK);
    DefInt(SQLITE_IOERR_SHMMAP);
    DefInt(SQLITE_IOERR_SEEK);
    DefInt(SQLITE_IOERR_DELETE_NOENT);
    DefInt(SQLITE_IOERR_MMAP);
    DefInt(SQLITE_IOERR_GETTEMPPATH);
    DefInt(SQLITE_IOERR_CONVPATH);
    DefInt(SQLITE_IOERR_VNODE);
    DefInt(SQLITE_IOERR_AUTH);
    DefInt(SQLITE_IOERR_BEGIN_ATOMIC);
    DefInt(SQLITE_IOERR_COMMIT_ATOMIC);
    DefInt(SQLITE_IOERR_ROLLBACK_ATOMIC);
    DefInt(SQLITE_IOERR_DATA);
    DefInt(SQLITE_IOERR_CORRUPTFS);
    DefInt(SQLITE_LOCKED_SHAREDCACHE);
    DefInt(SQLITE_LOCKED_VTAB);
    DefInt(SQLITE_BUSY_RECOVERY);
    DefInt(SQLITE_BUSY_SNAPSHOT);
    DefInt(SQLITE_BUSY_TIMEOUT);
    DefInt(SQLITE_CANTOPEN_NOTEMPDIR);
    DefInt(SQLITE_CANTOPEN_ISDIR);
    DefInt(SQLITE_CANTOPEN_FULLPATH);
    DefInt(SQLITE_CANTOPEN_CONVPATH);
    //DefInt(SQLITE_CANTOPEN_DIRTYWAL)/*docs say not used*/;
    DefInt(SQLITE_CANTOPEN_SYMLINK);
    DefInt(SQLITE_CORRUPT_VTAB);
    DefInt(SQLITE_CORRUPT_SEQUENCE);
    DefInt(SQLITE_CORRUPT_INDEX);
    DefInt(SQLITE_READONLY_RECOVERY);
    DefInt(SQLITE_READONLY_CANTLOCK);
    DefInt(SQLITE_READONLY_ROLLBACK);
    DefInt(SQLITE_READONLY_DBMOVED);
    DefInt(SQLITE_READONLY_CANTINIT);
    DefInt(SQLITE_READONLY_DIRECTORY);
    DefInt(SQLITE_ABORT_ROLLBACK);
    DefInt(SQLITE_CONSTRAINT_CHECK);
    DefInt(SQLITE_CONSTRAINT_COMMITHOOK);
    DefInt(SQLITE_CONSTRAINT_FOREIGNKEY);
    DefInt(SQLITE_CONSTRAINT_FUNCTION);
    DefInt(SQLITE_CONSTRAINT_NOTNULL);
    DefInt(SQLITE_CONSTRAINT_PRIMARYKEY);
    DefInt(SQLITE_CONSTRAINT_TRIGGER);
    DefInt(SQLITE_CONSTRAINT_UNIQUE);
    DefInt(SQLITE_CONSTRAINT_VTAB);
    DefInt(SQLITE_CONSTRAINT_ROWID);
    DefInt(SQLITE_CONSTRAINT_PINNED);
    DefInt(SQLITE_CONSTRAINT_DATATYPE);
    DefInt(SQLITE_NOTICE_RECOVER_WAL);
    DefInt(SQLITE_NOTICE_RECOVER_ROLLBACK);
    DefInt(SQLITE_WARNING_AUTOINDEX);
    DefInt(SQLITE_AUTH_USER);
    DefInt(SQLITE_OK_LOAD_PERMANENTLY);
    //DefInt(SQLITE_OK_SYMLINK) /* internal use only */;
  } _DefGroup;

  DefGroup(dataTypes) {
    DefInt(SQLITE_INTEGER);
    DefInt(SQLITE_FLOAT);
    DefInt(SQLITE_TEXT);
    DefInt(SQLITE_BLOB);
    DefInt(SQLITE_NULL);
  } _DefGroup;

  DefGroup(encodings) {
    /* Noting that the wasm binding only aims to support UTF-8. */
    DefInt(SQLITE_UTF8);
    DefInt(SQLITE_UTF16LE);
    DefInt(SQLITE_UTF16BE);
    DefInt(SQLITE_UTF16);
    /*deprecated DefInt(SQLITE_ANY); */
    DefInt(SQLITE_UTF16_ALIGNED);
  } _DefGroup;

  DefGroup(blobFinalizers) {
    /* SQLITE_STATIC/TRANSIENT need to be handled explicitly as
    ** integers to avoid casting-related warnings. */
    out("\"SQLITE_STATIC\":0, \"SQLITE_TRANSIENT\":-1");
  } _DefGroup;

  DefGroup(udfFlags) {
    DefInt(SQLITE_DETERMINISTIC);
    DefInt(SQLITE_DIRECTONLY);
    DefInt(SQLITE_INNOCUOUS);
  } _DefGroup;

  DefGroup(openFlags) {
    /* Noting that not all of these will have any effect in
    ** WASM-space. */
    DefInt(SQLITE_OPEN_READONLY);
    DefInt(SQLITE_OPEN_READWRITE);
    DefInt(SQLITE_OPEN_CREATE);
    DefInt(SQLITE_OPEN_URI);
    DefInt(SQLITE_OPEN_MEMORY);
    DefInt(SQLITE_OPEN_NOMUTEX);
    DefInt(SQLITE_OPEN_FULLMUTEX);
    DefInt(SQLITE_OPEN_SHAREDCACHE);
    DefInt(SQLITE_OPEN_PRIVATECACHE);
    DefInt(SQLITE_OPEN_EXRESCODE);
    DefInt(SQLITE_OPEN_NOFOLLOW);
    /* OPEN flags for use with VFSes... */
    DefInt(SQLITE_OPEN_MAIN_DB);
    DefInt(SQLITE_OPEN_MAIN_JOURNAL);
    DefInt(SQLITE_OPEN_TEMP_DB);
    DefInt(SQLITE_OPEN_TEMP_JOURNAL);
    DefInt(SQLITE_OPEN_TRANSIENT_DB);
    DefInt(SQLITE_OPEN_SUBJOURNAL);
    DefInt(SQLITE_OPEN_SUPER_JOURNAL);
    DefInt(SQLITE_OPEN_WAL);
    DefInt(SQLITE_OPEN_DELETEONCLOSE);
    DefInt(SQLITE_OPEN_EXCLUSIVE);
  } _DefGroup;

  DefGroup(syncFlags) {
    DefInt(SQLITE_SYNC_NORMAL);
    DefInt(SQLITE_SYNC_FULL);
    DefInt(SQLITE_SYNC_DATAONLY);
  } _DefGroup;

  DefGroup(prepareFlags) {
    DefInt(SQLITE_PREPARE_PERSISTENT);
    DefInt(SQLITE_PREPARE_NORMALIZE);
    DefInt(SQLITE_PREPARE_NO_VTAB);
  } _DefGroup;

  DefGroup(flock) {
    DefInt(SQLITE_LOCK_NONE);
    DefInt(SQLITE_LOCK_SHARED);
    DefInt(SQLITE_LOCK_RESERVED);
    DefInt(SQLITE_LOCK_PENDING);
    DefInt(SQLITE_LOCK_EXCLUSIVE);
  } _DefGroup;

  DefGroup(ioCap) {
    DefInt(SQLITE_IOCAP_ATOMIC);
    DefInt(SQLITE_IOCAP_ATOMIC512);
    DefInt(SQLITE_IOCAP_ATOMIC1K);
    DefInt(SQLITE_IOCAP_ATOMIC2K);
    DefInt(SQLITE_IOCAP_ATOMIC4K);
    DefInt(SQLITE_IOCAP_ATOMIC8K);
    DefInt(SQLITE_IOCAP_ATOMIC16K);
    DefInt(SQLITE_IOCAP_ATOMIC32K);
    DefInt(SQLITE_IOCAP_ATOMIC64K);
    DefInt(SQLITE_IOCAP_SAFE_APPEND);
    DefInt(SQLITE_IOCAP_SEQUENTIAL);
    DefInt(SQLITE_IOCAP_UNDELETABLE_WHEN_OPEN);
    DefInt(SQLITE_IOCAP_POWERSAFE_OVERWRITE);
    DefInt(SQLITE_IOCAP_IMMUTABLE);
    DefInt(SQLITE_IOCAP_BATCH_ATOMIC);
  } _DefGroup;

  DefGroup(fcntl) {
    DefInt(SQLITE_FCNTL_LOCKSTATE);
    DefInt(SQLITE_FCNTL_GET_LOCKPROXYFILE);
    DefInt(SQLITE_FCNTL_SET_LOCKPROXYFILE);
    DefInt(SQLITE_FCNTL_LAST_ERRNO);
    DefInt(SQLITE_FCNTL_SIZE_HINT);
    DefInt(SQLITE_FCNTL_CHUNK_SIZE);
    DefInt(SQLITE_FCNTL_FILE_POINTER);
    DefInt(SQLITE_FCNTL_SYNC_OMITTED);
    DefInt(SQLITE_FCNTL_WIN32_AV_RETRY);
    DefInt(SQLITE_FCNTL_PERSIST_WAL);
    DefInt(SQLITE_FCNTL_OVERWRITE);
    DefInt(SQLITE_FCNTL_VFSNAME);
    DefInt(SQLITE_FCNTL_POWERSAFE_OVERWRITE);
    DefInt(SQLITE_FCNTL_PRAGMA);
    DefInt(SQLITE_FCNTL_BUSYHANDLER);
    DefInt(SQLITE_FCNTL_TEMPFILENAME);
    DefInt(SQLITE_FCNTL_MMAP_SIZE);
    DefInt(SQLITE_FCNTL_TRACE);
    DefInt(SQLITE_FCNTL_HAS_MOVED);
    DefInt(SQLITE_FCNTL_SYNC);
    DefInt(SQLITE_FCNTL_COMMIT_PHASETWO);
    DefInt(SQLITE_FCNTL_WIN32_SET_HANDLE);
    DefInt(SQLITE_FCNTL_WAL_BLOCK);
    DefInt(SQLITE_FCNTL_ZIPVFS);
    DefInt(SQLITE_FCNTL_RBU);
    DefInt(SQLITE_FCNTL_VFS_POINTER);
    DefInt(SQLITE_FCNTL_JOURNAL_POINTER);
    DefInt(SQLITE_FCNTL_WIN32_GET_HANDLE);
    DefInt(SQLITE_FCNTL_PDB);
    DefInt(SQLITE_FCNTL_BEGIN_ATOMIC_WRITE);
    DefInt(SQLITE_FCNTL_COMMIT_ATOMIC_WRITE);
    DefInt(SQLITE_FCNTL_ROLLBACK_ATOMIC_WRITE);
    DefInt(SQLITE_FCNTL_LOCK_TIMEOUT);
    DefInt(SQLITE_FCNTL_DATA_VERSION);
    DefInt(SQLITE_FCNTL_SIZE_LIMIT);
    DefInt(SQLITE_FCNTL_CKPT_DONE);
    DefInt(SQLITE_FCNTL_RESERVE_BYTES);
    DefInt(SQLITE_FCNTL_CKPT_START);
    DefInt(SQLITE_FCNTL_EXTERNAL_READER);
    DefInt(SQLITE_FCNTL_CKSM_FILE);
  } _DefGroup;

  DefGroup(access){
    DefInt(SQLITE_ACCESS_EXISTS);
    DefInt(SQLITE_ACCESS_READWRITE);
    DefInt(SQLITE_ACCESS_READ)/*docs say this is unused*/;
  } _DefGroup;
  
#undef DefGroup
#undef DefStr
#undef DefInt
#undef _DefGroup

  /*
  ** Emit an array of "StructBinder" struct descripions, which look
  ** like:
  **
  ** {
  **   "name": "MyStruct",
  **   "sizeof": 16,
  **   "members": {
  **     "member1": {"offset": 0,"sizeof": 4,"signature": "i"},
  **     "member2": {"offset": 4,"sizeof": 4,"signature": "p"},
  **     "member3": {"offset": 8,"sizeof": 8,"signature": "j"}
  **   }
  ** }
  **
  ** Detailed documentation for those bits are in the docs for the
  ** Jaccwabyt JS-side component.
  */

  /** Macros for emitting StructBinder description. */
#define StructBinder__(TYPE)                 \
  n = 0;                                     \
  outf("%s{", (structCount++ ? ", " : ""));  \
  out("\"name\": \"" # TYPE "\",");         \
  outf("\"sizeof\": %d", (int)sizeof(TYPE)); \
  out(",\"members\": {");
#define StructBinder_(T) StructBinder__(T)
  /** ^^^ indirection needed to expand CurrentStruct */
#define StructBinder StructBinder_(CurrentStruct)
#define _StructBinder CloseBrace(2)
#define M(MEMBER,SIG)                                         \
  outf("%s\"%s\": "                                           \
       "{\"offset\":%d,\"sizeof\": %d,\"signature\":\"%s\"}", \
       (n++ ? ", " : ""), #MEMBER,                            \
       (int)offsetof(CurrentStruct,MEMBER),                   \
       (int)sizeof(((CurrentStruct*)0)->MEMBER),              \
       SIG)

  structCount = 0;
  out(", \"structs\": ["); {

#define CurrentStruct sqlite3_vfs
    StructBinder {
      M(iVersion,"i");
      M(szOsFile,"i");
      M(mxPathname,"i");
      M(pNext,"p");
      M(zName,"s");
      M(pAppData,"p");
      M(xOpen,"i(pppip)");
      M(xDelete,"i(ppi)");
      M(xAccess,"i(ppip)");
      M(xFullPathname,"i(ppip)");
      M(xDlOpen,"p(pp)");
      M(xDlError,"p(pip)");
      M(xDlSym,"p()");
      M(xDlClose,"v(pp)");
      M(xRandomness,"i(pip)");
      M(xSleep,"i(pi)");
      M(xCurrentTime,"i(pp)");
      M(xGetLastError,"i(pip)");
      M(xCurrentTimeInt64,"i(pp)");
      M(xSetSystemCall,"i(ppp)");
      M(xGetSystemCall,"p(pp)");
      M(xNextSystemCall,"p(pp)");
    } _StructBinder;
#undef CurrentStruct

#define CurrentStruct sqlite3_io_methods
    StructBinder {
      M(iVersion,"i");
      M(xClose,"i(p)");
      M(xRead,"i(ppij)");
      M(xWrite,"i(ppij)");
      M(xTruncate,"i(pj)");
      M(xSync,"i(pi)");
      M(xFileSize,"i(pp)");
      M(xLock,"i(pi)");
      M(xUnlock,"i(pi)");
      M(xCheckReservedLock,"i(pp)");
      M(xFileControl,"i(pip)");
      M(xSectorSize,"i(p)");
      M(xDeviceCharacteristics,"i(p)");
      M(xShmMap,"i(piiip)");
      M(xShmLock,"i(piii)");
      M(xShmBarrier,"v(p)");
      M(xShmUnmap,"i(pi)");
      M(xFetch,"i(pjip)");
      M(xUnfetch,"i(pjp)");
    } _StructBinder;
#undef CurrentStruct

#define CurrentStruct sqlite3_file
    StructBinder {
      M(pMethods,"p");
    } _StructBinder;
#undef CurrentStruct

  } out( "]"/*structs*/);

  out("}"/*top-level object*/);
  *pos = 0;
  strBuf[0] = '{'/*end of the race-condition workaround*/;
  return strBuf;
#undef StructBinder
#undef StructBinder_
#undef StructBinder__
#undef M
#undef _StructBinder
#undef CloseBrace
#undef out
#undef outf
#undef lenCheck
}

/*
** This function is NOT part of the sqlite3 public API. It is strictly
** for use by the sqlite project's own JS/WASM bindings.
**
** This function invokes the xDelete method of the default VFS,
** passing on the given filename. If zName is NULL, no default VFS is
** found, or it has no xDelete method, SQLITE_MISUSE is returned, else
** the result of the xDelete() call is returned.
*/
WASM_KEEP
int sqlite3_wasm_vfs_unlink(const char * zName){
  int rc = SQLITE_MISUSE /* ??? */;
  sqlite3_vfs * const pVfs = sqlite3_vfs_find(0);
  if( zName && pVfs && pVfs->xDelete ){
    rc = pVfs->xDelete(pVfs, zName, 1);
  }
  return rc;
}

#if defined(__EMSCRIPTEN__) && defined(SQLITE_WASM_WASMFS)
#include <emscripten/wasmfs.h>
#include <emscripten/console.h>

/*
** This function is NOT part of the sqlite3 public API. It is strictly
** for use by the sqlite project's own JS/WASM bindings, specifically
** only when building with Emscripten's WASMFS support.
**
** This function should only be called if the JS side detects the
** existence of the Origin-Private FileSystem (OPFS) APIs in the
** client. The first time it is called, this function instantiates a
** WASMFS backend impl for OPFS. On success, subsequent calls are
** no-ops.
**
** This function may be passed a "mount point" name, which must have a
** leading "/" and is currently restricted to a single path component,
** e.g. "/foo" is legal but "/foo/" and "/foo/bar" are not. If it is
** NULL or empty, it defaults to "/persistent".
**
** Returns 0 on success, SQLITE_NOMEM if instantiation of the backend
** object fails, SQLITE_IOERR if mkdir() of the zMountPoint dir in
** the virtual FS fails. In builds compiled without SQLITE_WASM_WASMFS
** defined, SQLITE_NOTFOUND is returned without side effects.
*/
WASM_KEEP
int sqlite3_wasm_init_wasmfs(const char *zMountPoint){
  static backend_t pOpfs = 0;
  if( !zMountPoint || !*zMountPoint ) zMountPoint = "/persistent";
  if( !pOpfs ){
    pOpfs = wasmfs_create_opfs_backend();
    if( pOpfs ){
      emscripten_console_log("Created WASMFS OPFS backend.");
    }
  }
  /** It's not enough to instantiate the backend. We have to create a
      mountpoint in the VFS and attach the backend to it. */
  if( pOpfs && 0!=access(zMountPoint, F_OK) ){
    /* mkdir() simply hangs when called from fiddle app. Cause is
       not yet determined but the hypothesis is an init-order
       issue. */
    /* Note that this check and is not robust but it will
       hypothetically suffice for the transient wasm-based virtual
       filesystem we're currently running in. */
    const int rc = wasmfs_create_directory(zMountPoint, 0777, pOpfs);
    emscripten_console_logf("OPFS mkdir(%s) rc=%d", zMountPoint, rc);
    if(rc) return SQLITE_IOERR;
  }
  return pOpfs ? 0 : SQLITE_NOMEM;
}
#else
WASM_KEEP
int sqlite3_wasm_init_wasmfs(void){
  return SQLITE_NOTFOUND;
}
#endif /* __EMSCRIPTEN__ && SQLITE_WASM_WASMFS */


#undef WASM_KEEP

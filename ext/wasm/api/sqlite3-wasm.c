/*
** This file requires access to sqlite3.c static state in order to
** implement certain WASM-specific features, and thus directly
** includes that file. Unlike the rest of sqlite3.c, this file
** requires compiling with -std=c99 (or equivalent, or a later C
** version) because it makes use of features not available in C89.
**
** At its simplest, to build sqlite3.wasm either place this file
** in the same directory as sqlite3.c/h before compilation or use the
** -I/path flag to tell the compiler where to find both of those
** files, then compile this file. For example:
**
** emcc -o sqlite3.wasm ... -I/path/to/sqlite3-c-and-h sqlite3-wasm.c
*/
#define SQLITE_WASM
#ifdef SQLITE_WASM_ENABLE_C_TESTS
/*
** Code blocked off by SQLITE_WASM_TESTS is intended solely for use in
** unit/regression testing. They may be safely omitted from
** client-side builds. The main unit test script, tester1.js, will
** skip related tests if it doesn't find the corresponding functions
** in the WASM exports.
*/
#  define SQLITE_WASM_TESTS 1
#else
#  define SQLITE_WASM_TESTS 0
#endif

/*
** Threading and file locking: JS is single-threaded. Each Worker
** thread is a separate instance of the JS engine so can never access
** the same db handle as another thread, thus multi-threading support
** is unnecessary in the library. Because the filesystems are virtual
** and local to a given wasm runtime instance, two Workers can never
** access the same db file at once, with the exception of OPFS.
**
** Summary: except for the case of OPFS, which supports locking using
** its own API, threading and file locking support are unnecessary in
** the wasm build.
*/

/*
** Undefine any SQLITE_... config flags which we specifically do not
** want defined. Please keep these alphabetized.
*/
#undef SQLITE_OMIT_DESERIALIZE
#undef SQLITE_OMIT_MEMORYDB

/*
** Define any SQLITE_... config defaults we want if they aren't
** overridden by the builder. Please keep these alphabetized.
*/

/**********************************************************************/
/* SQLITE_D... */
#ifndef SQLITE_DEFAULT_CACHE_SIZE
/*
** The OPFS impls benefit tremendously from an increased cache size
** when working on large workloads, e.g. speedtest1 --size 50 or
** higher. On smaller workloads, e.g. speedtest1 --size 25, they
** clearly benefit from having 4mb of cache, but not as much as a
** larger cache benefits the larger workloads. Speed differences
** between 2x and nearly 3x have been measured with ample page cache.
*/
# define SQLITE_DEFAULT_CACHE_SIZE -16384
#endif
#if !defined(SQLITE_DEFAULT_PAGE_SIZE)
/*
** OPFS performance is improved by approx. 12% with a page size of 8kb
** instead of 4kb. Performance with 16kb is equivalent to 8kb.
**
** Performance difference of kvvfs with a page size of 8kb compared to
** 4kb, as measured by speedtest1 --size 4, is indeterminate:
** measurements are all over the place either way and not
** significantly different.
*/
# define SQLITE_DEFAULT_PAGE_SIZE 8192
#endif
#ifndef SQLITE_DEFAULT_UNIX_VFS
# define SQLITE_DEFAULT_UNIX_VFS "unix-none"
#endif
#undef SQLITE_DQS
#define SQLITE_DQS 0

/**********************************************************************/
/* SQLITE_ENABLE_... */
#ifndef SQLITE_ENABLE_BYTECODE_VTAB
#  define SQLITE_ENABLE_BYTECODE_VTAB 1
#endif
#ifndef SQLITE_ENABLE_DBPAGE_VTAB
#  define SQLITE_ENABLE_DBPAGE_VTAB 1
#endif
#ifndef SQLITE_ENABLE_DBSTAT_VTAB
#  define SQLITE_ENABLE_DBSTAT_VTAB 1
#endif
#ifndef SQLITE_ENABLE_EXPLAIN_COMMENTS
#  define SQLITE_ENABLE_EXPLAIN_COMMENTS 1
#endif
#ifndef SQLITE_ENABLE_FTS4
#  define SQLITE_ENABLE_FTS4 1
#endif
#ifndef SQLITE_ENABLE_MATH_FUNCTIONS
#  define SQLITE_ENABLE_MATH_FUNCTIONS 1
#endif
#ifndef SQLITE_ENABLE_OFFSET_SQL_FUNC
#  define SQLITE_ENABLE_OFFSET_SQL_FUNC 1
#endif
#ifndef SQLITE_ENABLE_PREUPDATE_HOOK
#  define SQLITE_ENABLE_PREUPDATE_HOOK 1 /*required by session extension*/
#endif
#ifndef SQLITE_ENABLE_RTREE
#  define SQLITE_ENABLE_RTREE 1
#endif
#ifndef SQLITE_ENABLE_SESSION
#  define SQLITE_ENABLE_SESSION 1
#endif
#ifndef SQLITE_ENABLE_STMTVTAB
#  define SQLITE_ENABLE_STMTVTAB 1
#endif
#ifndef SQLITE_ENABLE_UNKNOWN_SQL_FUNCTION
#  define SQLITE_ENABLE_UNKNOWN_SQL_FUNCTION
#endif

/**********************************************************************/
/* SQLITE_M... */
#ifndef SQLITE_MAX_ALLOCATION_SIZE
# define SQLITE_MAX_ALLOCATION_SIZE 0x1fffffff
#endif

/**********************************************************************/
/* SQLITE_O... */
#ifndef SQLITE_OMIT_DEPRECATED
# define SQLITE_OMIT_DEPRECATED 1
#endif
#ifndef SQLITE_OMIT_LOAD_EXTENSION
# define SQLITE_OMIT_LOAD_EXTENSION 1
#endif
#ifndef SQLITE_OMIT_SHARED_CACHE
# define SQLITE_OMIT_SHARED_CACHE 1
#endif
#ifndef SQLITE_OMIT_UTF16
# define SQLITE_OMIT_UTF16 1
#endif
#ifndef SQLITE_OMIT_WAL
# define SQLITE_OMIT_WAL 1
#endif
#ifndef SQLITE_OS_KV_OPTIONAL
# define SQLITE_OS_KV_OPTIONAL 1
#endif

/**********************************************************************/
/* SQLITE_T... */
#ifndef SQLITE_TEMP_STORE
# define SQLITE_TEMP_STORE 3
#endif
#ifndef SQLITE_THREADSAFE
# define SQLITE_THREADSAFE 0
#endif

/**********************************************************************/
/* SQLITE_USE_... */
#ifndef SQLITE_USE_URI
#  define SQLITE_USE_URI 1
#endif

#include <assert.h>
#include "sqlite3.c" /* yes, .c instead of .h. */

#if defined(__EMSCRIPTEN__)
#  include <emscripten/console.h>
#endif

/*
** SQLITE_WASM_KEEP is functionally identical to EMSCRIPTEN_KEEPALIVE
** but is not Emscripten-specific. It explicitly marks functions for
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
#define SQLITE_WASM_KEEP __attribute__((used,visibility("default")))
// See also:
//__attribute__((export_name("theExportedName"), used, visibility("default")))


#if 0
/*
** An EXPERIMENT in implementing a stack-based allocator analog to
** Emscripten's stackSave(), stackAlloc(), stackRestore().
** Unfortunately, this cannot work together with Emscripten because
** Emscripten defines its own native one and we'd stomp on each
** other's memory. Other than that complication, basic tests show it
** to work just fine.
**
** Another option is to malloc() a chunk of our own and call that our
** "stack".
*/
SQLITE_WASM_KEEP void * sqlite3_wasm_stack_end(void){
  extern void __heap_base
    /* see https://stackoverflow.com/questions/10038964 */;
  return &__heap_base;
}
SQLITE_WASM_KEEP void * sqlite3_wasm_stack_begin(void){
  extern void __data_end;
  return &__data_end;
}
static void * pWasmStackPtr = 0;
SQLITE_WASM_KEEP void * sqlite3_wasm_stack_ptr(void){
  if(!pWasmStackPtr) pWasmStackPtr = sqlite3_wasm_stack_end();
  return pWasmStackPtr;
}
SQLITE_WASM_KEEP void sqlite3_wasm_stack_restore(void * p){
  pWasmStackPtr = p;
}
SQLITE_WASM_KEEP void * sqlite3_wasm_stack_alloc(int n){
  if(n<=0) return 0;
  n = (n + 7) & ~7 /* align to 8-byte boundary */;
  unsigned char * const p = (unsigned char *)sqlite3_wasm_stack_ptr();
  unsigned const char * const b = (unsigned const char *)sqlite3_wasm_stack_begin();
  if(b + n >= p || b + n < b/*overflow*/) return 0;
  return pWasmStackPtr = p - n;
}
#endif /* stack allocator experiment */

/*
** State for the "pseudo-stack" allocator implemented in
** sqlite3_wasm_pstack_xyz(). In order to avoid colliding with
** Emscripten-controled stack space, it carves out a bit of stack
** memory to use for that purpose. This memory ends up in the
** WASM-managed memory, such that routines which manipulate the wasm
** heap can also be used to manipulate this memory.
**
** This particular allocator is intended for small allocations such as
** storage for output pointers. We cannot reasonably size it large
** enough for general-purpose string conversions because some of our
** tests use input files (strings) of 16MB+.
*/
static unsigned char PStack_mem[512 * 8] = {0};
static struct {
  unsigned const char * const pBegin;/* Start (inclusive) of memory */
  unsigned const char * const pEnd;  /* One-after-the-end of memory */
  unsigned char * pPos;              /* Current stack pointer */
} PStack = {
  &PStack_mem[0],
  &PStack_mem[0] + sizeof(PStack_mem),
  &PStack_mem[0] + sizeof(PStack_mem)
};
/*
** Returns the current pstack position.
*/
SQLITE_WASM_KEEP void * sqlite3_wasm_pstack_ptr(void){
  return PStack.pPos;
}
/*
** Sets the pstack position poitner to p. Results are undefined if the
** given value did not come from sqlite3_wasm_pstack_ptr().
*/
SQLITE_WASM_KEEP void sqlite3_wasm_pstack_restore(unsigned char * p){
  assert(p>=PStack.pBegin && p<=PStack.pEnd && p>=PStack.pPos);
  assert(0==(p & 0x7));
  if(p>=PStack.pBegin && p<=PStack.pEnd /*&& p>=PStack.pPos*/){
    PStack.pPos = p;
  }
}
/*
** Allocate and zero out n bytes from the pstack. Returns a pointer to
** the memory on success, 0 on error (including a negative n value). n
** is always adjusted to be a multiple of 8 and returned memory is
** always zeroed out before returning (because this keeps the client
** JS code from having to do so, and most uses of the pstack will
** call for doing so).
*/
SQLITE_WASM_KEEP void * sqlite3_wasm_pstack_alloc(int n){
  if( n<=0 ) return 0;
  //if( n & 0x7 ) n += 8 - (n & 0x7) /* align to 8-byte boundary */;
  n = (n + 7) & ~7 /* align to 8-byte boundary */;
  if( PStack.pBegin + n > PStack.pPos /*not enough space left*/
      || PStack.pBegin + n <= PStack.pBegin /*overflow*/ ) return 0;
  memset((PStack.pPos = PStack.pPos - n), 0, (unsigned int)n);
  return PStack.pPos;
}
/*
** Return the number of bytes left which can be
** sqlite3_wasm_pstack_alloc()'d.
*/
SQLITE_WASM_KEEP int sqlite3_wasm_pstack_remaining(void){
  assert(PStack.pPos >= PStack.pBegin);
  assert(PStack.pPos <= PStack.pEnd);
  return (int)(PStack.pPos - PStack.pBegin);
}

/*
** Return the total number of bytes available in the pstack, including
** any space which is currently allocated. This value is a
** compile-time constant.
*/
SQLITE_WASM_KEEP int sqlite3_wasm_pstack_quota(void){
  return (int)(PStack.pEnd - PStack.pBegin);
}

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
SQLITE_WASM_KEEP
int sqlite3_wasm_db_error(sqlite3*db, int err_code, const char *zMsg){
  if( db!=0 ){
    if( 0!=zMsg ){
      const int nMsg = sqlite3Strlen30(zMsg);
      sqlite3ErrorWithMsg(db, err_code, "%.*s", nMsg, zMsg);
    }else{
      sqlite3ErrorWithMsg(db, err_code, NULL);
    }
  }
  return err_code;
}

#if SQLITE_WASM_TESTS
struct WasmTestStruct {
  int v4;
  void * ppV;
  const char * cstr;
  int64_t v8;
  void (*xFunc)(void*);
};
typedef struct WasmTestStruct WasmTestStruct;
SQLITE_WASM_KEEP
void sqlite3_wasm_test_struct(WasmTestStruct * s){
  if(s){
    s->v4 *= 2;
    s->v8 = s->v4 * 2;
    s->ppV = s;
    s->cstr = __FILE__;
    if(s->xFunc) s->xFunc(s);
  }
  return;
}
#endif /* SQLITE_WASM_TESTS */

/*
** This function is NOT part of the sqlite3 public API. It is strictly
** for use by the sqlite project's own JS/WASM bindings. Unlike the
** rest of the sqlite3 API, this part requires C99 for snprintf() and
** variadic macros.
**
** Returns a string containing a JSON-format "enum" of C-level
** constants and struct-related metadata intended to be imported into
** the JS environment. The JSON is initialized the first time this
** function is called and that result is reused for all future calls.
**
** If this function returns NULL then it means that the internal
** buffer is not large enough for the generated JSON and needs to be
** increased. In debug builds that will trigger an assert().
*/
SQLITE_WASM_KEEP
const char * sqlite3_wasm_enum_json(void){
  static char aBuffer[1024 * 20] = {0} /* where the JSON goes */;
  int n = 0, nChildren = 0, nStruct = 0
    /* output counters for figuring out where commas go */;
  char * zPos = &aBuffer[1] /* skip first byte for now to help protect
                            ** against a small race condition */;
  char const * const zEnd = &aBuffer[0] + sizeof(aBuffer) /* one-past-the-end */;
  if(aBuffer[0]) return aBuffer;
  /* Leave aBuffer[0] at 0 until the end to help guard against a tiny
  ** race condition. If this is called twice concurrently, they might
  ** end up both writing to aBuffer, but they'll both write the same
  ** thing, so that's okay. If we set byte 0 up front then the 2nd
  ** instance might return and use the string before the 1st instance
  ** is done filling it. */

/* Core output macros... */
#define lenCheck assert(zPos < zEnd - 128 \
  && "sqlite3_wasm_enum_json() buffer is too small."); \
  if( zPos >= zEnd - 128 ) return 0
#define outf(format,...) \
  zPos += snprintf(zPos, ((size_t)(zEnd - zPos)), format, __VA_ARGS__); \
  lenCheck
#define out(TXT) outf("%s",TXT)
#define CloseBrace(LEVEL) \
  assert(LEVEL<5); memset(zPos, '}', LEVEL); zPos+=LEVEL; lenCheck

/* Macros for emitting maps of integer- and string-type macros to
** their values. */
#define DefGroup(KEY) n = 0; \
  outf("%s\"" #KEY "\": {",(nChildren++ ? "," : ""));
#define DefInt(KEY)                                     \
  outf("%s\"%s\": %d", (n++ ? ", " : ""), #KEY, (int)KEY)
#define DefStr(KEY)                                     \
  outf("%s\"%s\": \"%s\"", (n++ ? ", " : ""), #KEY, KEY)
#define _DefGroup CloseBrace(1)

  /* The following groups are sorted alphabetic by group name. */
  DefGroup(access){
    DefInt(SQLITE_ACCESS_EXISTS);
    DefInt(SQLITE_ACCESS_READWRITE);
    DefInt(SQLITE_ACCESS_READ)/*docs say this is unused*/;
  } _DefGroup;

  DefGroup(authorizer){
    DefInt(SQLITE_DENY);
    DefInt(SQLITE_IGNORE);
    DefInt(SQLITE_CREATE_INDEX);
    DefInt(SQLITE_CREATE_TABLE);
    DefInt(SQLITE_CREATE_TEMP_INDEX);
    DefInt(SQLITE_CREATE_TEMP_TABLE);
    DefInt(SQLITE_CREATE_TEMP_TRIGGER);
    DefInt(SQLITE_CREATE_TEMP_VIEW);
    DefInt(SQLITE_CREATE_TRIGGER);
    DefInt(SQLITE_CREATE_VIEW);
    DefInt(SQLITE_DELETE);
    DefInt(SQLITE_DROP_INDEX);
    DefInt(SQLITE_DROP_TABLE);
    DefInt(SQLITE_DROP_TEMP_INDEX);
    DefInt(SQLITE_DROP_TEMP_TABLE);
    DefInt(SQLITE_DROP_TEMP_TRIGGER);
    DefInt(SQLITE_DROP_TEMP_VIEW);
    DefInt(SQLITE_DROP_TRIGGER);
    DefInt(SQLITE_DROP_VIEW);
    DefInt(SQLITE_INSERT);
    DefInt(SQLITE_PRAGMA);
    DefInt(SQLITE_READ);
    DefInt(SQLITE_SELECT);
    DefInt(SQLITE_TRANSACTION);
    DefInt(SQLITE_UPDATE);
    DefInt(SQLITE_ATTACH);
    DefInt(SQLITE_DETACH);
    DefInt(SQLITE_ALTER_TABLE);
    DefInt(SQLITE_REINDEX);
    DefInt(SQLITE_ANALYZE);
    DefInt(SQLITE_CREATE_VTABLE);
    DefInt(SQLITE_DROP_VTABLE);
    DefInt(SQLITE_FUNCTION);
    DefInt(SQLITE_SAVEPOINT);
    //DefInt(SQLITE_COPY) /* No longer used */;
    DefInt(SQLITE_RECURSIVE);
  } _DefGroup;

  DefGroup(blobFinalizers) {
    /* SQLITE_STATIC/TRANSIENT need to be handled explicitly as
    ** integers to avoid casting-related warnings. */
    out("\"SQLITE_STATIC\":0, \"SQLITE_TRANSIENT\":-1");
    outf(",\"SQLITE_WASM_DEALLOC\": %lld",
         (sqlite3_int64)(sqlite3_free));
  } _DefGroup;

  DefGroup(changeset){
    DefInt(SQLITE_CHANGESETSTART_INVERT);
    DefInt(SQLITE_CHANGESETAPPLY_NOSAVEPOINT);
    DefInt(SQLITE_CHANGESETAPPLY_INVERT);

    DefInt(SQLITE_CHANGESET_DATA);
    DefInt(SQLITE_CHANGESET_NOTFOUND);
    DefInt(SQLITE_CHANGESET_CONFLICT);
    DefInt(SQLITE_CHANGESET_CONSTRAINT);
    DefInt(SQLITE_CHANGESET_FOREIGN_KEY);

    DefInt(SQLITE_CHANGESET_OMIT);
    DefInt(SQLITE_CHANGESET_REPLACE);
    DefInt(SQLITE_CHANGESET_ABORT);
  } _DefGroup;

  DefGroup(config){
    DefInt(SQLITE_CONFIG_SINGLETHREAD);
    DefInt(SQLITE_CONFIG_MULTITHREAD);
    DefInt(SQLITE_CONFIG_SERIALIZED);
    DefInt(SQLITE_CONFIG_MALLOC);
    DefInt(SQLITE_CONFIG_GETMALLOC);
    DefInt(SQLITE_CONFIG_SCRATCH);
    DefInt(SQLITE_CONFIG_PAGECACHE);
    DefInt(SQLITE_CONFIG_HEAP);
    DefInt(SQLITE_CONFIG_MEMSTATUS);
    DefInt(SQLITE_CONFIG_MUTEX);
    DefInt(SQLITE_CONFIG_GETMUTEX);
/* previously SQLITE_CONFIG_CHUNKALLOC 12 which is now unused. */
    DefInt(SQLITE_CONFIG_LOOKASIDE);
    DefInt(SQLITE_CONFIG_PCACHE);
    DefInt(SQLITE_CONFIG_GETPCACHE);
    DefInt(SQLITE_CONFIG_LOG);
    DefInt(SQLITE_CONFIG_URI);
    DefInt(SQLITE_CONFIG_PCACHE2);
    DefInt(SQLITE_CONFIG_GETPCACHE2);
    DefInt(SQLITE_CONFIG_COVERING_INDEX_SCAN);
    DefInt(SQLITE_CONFIG_SQLLOG);
    DefInt(SQLITE_CONFIG_MMAP_SIZE);
    DefInt(SQLITE_CONFIG_WIN32_HEAPSIZE);
    DefInt(SQLITE_CONFIG_PCACHE_HDRSZ);
    DefInt(SQLITE_CONFIG_PMASZ);
    DefInt(SQLITE_CONFIG_STMTJRNL_SPILL);
    DefInt(SQLITE_CONFIG_SMALL_MALLOC);
    DefInt(SQLITE_CONFIG_SORTERREF_SIZE);
    DefInt(SQLITE_CONFIG_MEMDB_MAXSIZE);
  } _DefGroup;

  DefGroup(dataTypes) {
    DefInt(SQLITE_INTEGER);
    DefInt(SQLITE_FLOAT);
    DefInt(SQLITE_TEXT);
    DefInt(SQLITE_BLOB);
    DefInt(SQLITE_NULL);
  } _DefGroup;

  DefGroup(dbConfig){
    DefInt(SQLITE_DBCONFIG_MAINDBNAME);
    DefInt(SQLITE_DBCONFIG_LOOKASIDE);
    DefInt(SQLITE_DBCONFIG_ENABLE_FKEY);
    DefInt(SQLITE_DBCONFIG_ENABLE_TRIGGER);
    DefInt(SQLITE_DBCONFIG_ENABLE_FTS3_TOKENIZER);
    DefInt(SQLITE_DBCONFIG_ENABLE_LOAD_EXTENSION);
    DefInt(SQLITE_DBCONFIG_NO_CKPT_ON_CLOSE);
    DefInt(SQLITE_DBCONFIG_ENABLE_QPSG);
    DefInt(SQLITE_DBCONFIG_TRIGGER_EQP);
    DefInt(SQLITE_DBCONFIG_RESET_DATABASE);
    DefInt(SQLITE_DBCONFIG_DEFENSIVE);
    DefInt(SQLITE_DBCONFIG_WRITABLE_SCHEMA);
    DefInt(SQLITE_DBCONFIG_LEGACY_ALTER_TABLE);
    DefInt(SQLITE_DBCONFIG_DQS_DML);
    DefInt(SQLITE_DBCONFIG_DQS_DDL);
    DefInt(SQLITE_DBCONFIG_ENABLE_VIEW);
    DefInt(SQLITE_DBCONFIG_LEGACY_FILE_FORMAT);
    DefInt(SQLITE_DBCONFIG_TRUSTED_SCHEMA);
    DefInt(SQLITE_DBCONFIG_MAX);
  } _DefGroup;

  DefGroup(dbStatus){
    DefInt(SQLITE_DBSTATUS_LOOKASIDE_USED);
    DefInt(SQLITE_DBSTATUS_CACHE_USED);
    DefInt(SQLITE_DBSTATUS_SCHEMA_USED);
    DefInt(SQLITE_DBSTATUS_STMT_USED);
    DefInt(SQLITE_DBSTATUS_LOOKASIDE_HIT);
    DefInt(SQLITE_DBSTATUS_LOOKASIDE_MISS_SIZE);
    DefInt(SQLITE_DBSTATUS_LOOKASIDE_MISS_FULL);
    DefInt(SQLITE_DBSTATUS_CACHE_HIT);
    DefInt(SQLITE_DBSTATUS_CACHE_MISS);
    DefInt(SQLITE_DBSTATUS_CACHE_WRITE);
    DefInt(SQLITE_DBSTATUS_DEFERRED_FKS);
    DefInt(SQLITE_DBSTATUS_CACHE_USED_SHARED);
    DefInt(SQLITE_DBSTATUS_CACHE_SPILL);
    DefInt(SQLITE_DBSTATUS_MAX);
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

  DefGroup(limits) {
    DefInt(SQLITE_MAX_ALLOCATION_SIZE);
    DefInt(SQLITE_LIMIT_LENGTH);
    DefInt(SQLITE_MAX_LENGTH);
    DefInt(SQLITE_LIMIT_SQL_LENGTH);
    DefInt(SQLITE_MAX_SQL_LENGTH);
    DefInt(SQLITE_LIMIT_COLUMN);
    DefInt(SQLITE_MAX_COLUMN);
    DefInt(SQLITE_LIMIT_EXPR_DEPTH);
    DefInt(SQLITE_MAX_EXPR_DEPTH);
    DefInt(SQLITE_LIMIT_COMPOUND_SELECT);
    DefInt(SQLITE_MAX_COMPOUND_SELECT);
    DefInt(SQLITE_LIMIT_VDBE_OP);
    DefInt(SQLITE_MAX_VDBE_OP);
    DefInt(SQLITE_LIMIT_FUNCTION_ARG);
    DefInt(SQLITE_MAX_FUNCTION_ARG);
    DefInt(SQLITE_LIMIT_ATTACHED);
    DefInt(SQLITE_MAX_ATTACHED);
    DefInt(SQLITE_LIMIT_LIKE_PATTERN_LENGTH);
    DefInt(SQLITE_MAX_LIKE_PATTERN_LENGTH);
    DefInt(SQLITE_LIMIT_VARIABLE_NUMBER);
    DefInt(SQLITE_MAX_VARIABLE_NUMBER);
    DefInt(SQLITE_LIMIT_TRIGGER_DEPTH);
    DefInt(SQLITE_MAX_TRIGGER_DEPTH);
    DefInt(SQLITE_LIMIT_WORKER_THREADS);
    DefInt(SQLITE_MAX_WORKER_THREADS);
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

  DefGroup(prepareFlags) {
    DefInt(SQLITE_PREPARE_PERSISTENT);
    DefInt(SQLITE_PREPARE_NORMALIZE);
    DefInt(SQLITE_PREPARE_NO_VTAB);
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

  DefGroup(serialize){
    DefInt(SQLITE_SERIALIZE_NOCOPY);
    DefInt(SQLITE_DESERIALIZE_FREEONCLOSE);
    DefInt(SQLITE_DESERIALIZE_READONLY);
    DefInt(SQLITE_DESERIALIZE_RESIZEABLE);
  } _DefGroup;

  DefGroup(session){
    DefInt(SQLITE_SESSION_CONFIG_STRMSIZE);
    DefInt(SQLITE_SESSION_OBJCONFIG_SIZE);
  } _DefGroup;

  DefGroup(sqlite3Status){
    DefInt(SQLITE_STATUS_MEMORY_USED);
    DefInt(SQLITE_STATUS_PAGECACHE_USED);
    DefInt(SQLITE_STATUS_PAGECACHE_OVERFLOW);
    //DefInt(SQLITE_STATUS_SCRATCH_USED) /* NOT USED */;
    //DefInt(SQLITE_STATUS_SCRATCH_OVERFLOW) /* NOT USED */;
    DefInt(SQLITE_STATUS_MALLOC_SIZE);
    DefInt(SQLITE_STATUS_PARSER_STACK);
    DefInt(SQLITE_STATUS_PAGECACHE_SIZE);
    //DefInt(SQLITE_STATUS_SCRATCH_SIZE) /* NOT USED */;
    DefInt(SQLITE_STATUS_MALLOC_COUNT);
  } _DefGroup;

  DefGroup(stmtStatus){
    DefInt(SQLITE_STMTSTATUS_FULLSCAN_STEP);
    DefInt(SQLITE_STMTSTATUS_SORT);
    DefInt(SQLITE_STMTSTATUS_AUTOINDEX);
    DefInt(SQLITE_STMTSTATUS_VM_STEP);
    DefInt(SQLITE_STMTSTATUS_REPREPARE);
    DefInt(SQLITE_STMTSTATUS_RUN);
    DefInt(SQLITE_STMTSTATUS_FILTER_MISS);
    DefInt(SQLITE_STMTSTATUS_FILTER_HIT);
    DefInt(SQLITE_STMTSTATUS_MEMUSED);
  } _DefGroup;
  
  DefGroup(syncFlags) {
    DefInt(SQLITE_SYNC_NORMAL);
    DefInt(SQLITE_SYNC_FULL);
    DefInt(SQLITE_SYNC_DATAONLY);
  } _DefGroup;

  DefGroup(trace) {
    DefInt(SQLITE_TRACE_STMT);
    DefInt(SQLITE_TRACE_PROFILE);
    DefInt(SQLITE_TRACE_ROW);
    DefInt(SQLITE_TRACE_CLOSE);
  } _DefGroup;

  DefGroup(txnState){
    DefInt(SQLITE_TXN_NONE);
    DefInt(SQLITE_TXN_READ);
    DefInt(SQLITE_TXN_WRITE);
  } _DefGroup;

  DefGroup(udfFlags) {
    DefInt(SQLITE_DETERMINISTIC);
    DefInt(SQLITE_DIRECTONLY);
    DefInt(SQLITE_INNOCUOUS);
  } _DefGroup;

  DefGroup(version) {
    DefInt(SQLITE_VERSION_NUMBER);
    DefStr(SQLITE_VERSION);
    DefStr(SQLITE_SOURCE_ID);
  } _DefGroup;

  DefGroup(vtab) {
    DefInt(SQLITE_INDEX_SCAN_UNIQUE);
    DefInt(SQLITE_INDEX_CONSTRAINT_EQ);
    DefInt(SQLITE_INDEX_CONSTRAINT_GT);
    DefInt(SQLITE_INDEX_CONSTRAINT_LE);
    DefInt(SQLITE_INDEX_CONSTRAINT_LT);
    DefInt(SQLITE_INDEX_CONSTRAINT_GE);
    DefInt(SQLITE_INDEX_CONSTRAINT_MATCH);
    DefInt(SQLITE_INDEX_CONSTRAINT_LIKE);
    DefInt(SQLITE_INDEX_CONSTRAINT_GLOB);
    DefInt(SQLITE_INDEX_CONSTRAINT_REGEXP);
    DefInt(SQLITE_INDEX_CONSTRAINT_NE);
    DefInt(SQLITE_INDEX_CONSTRAINT_ISNOT);
    DefInt(SQLITE_INDEX_CONSTRAINT_ISNOTNULL);
    DefInt(SQLITE_INDEX_CONSTRAINT_ISNULL);
    DefInt(SQLITE_INDEX_CONSTRAINT_IS);
    DefInt(SQLITE_INDEX_CONSTRAINT_LIMIT);
    DefInt(SQLITE_INDEX_CONSTRAINT_OFFSET);
    DefInt(SQLITE_INDEX_CONSTRAINT_FUNCTION);
    DefInt(SQLITE_VTAB_CONSTRAINT_SUPPORT);
    DefInt(SQLITE_VTAB_INNOCUOUS);
    DefInt(SQLITE_VTAB_DIRECTONLY);
    DefInt(SQLITE_ROLLBACK);
    //DefInt(SQLITE_IGNORE); // Also used by sqlite3_authorizer() callback
    DefInt(SQLITE_FAIL);
    //DefInt(SQLITE_ABORT); // Also an error code
    DefInt(SQLITE_REPLACE);
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
  outf("%s{", (nStruct++ ? ", " : ""));      \
  out("\"name\": \"" # TYPE "\",");          \
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

  nStruct = 0;
  out(", \"structs\": ["); {

#define CurrentStruct sqlite3_vfs
    StructBinder {
      M(iVersion,          "i");
      M(szOsFile,          "i");
      M(mxPathname,        "i");
      M(pNext,             "p");
      M(zName,             "s");
      M(pAppData,          "p");
      M(xOpen,             "i(pppip)");
      M(xDelete,           "i(ppi)");
      M(xAccess,           "i(ppip)");
      M(xFullPathname,     "i(ppip)");
      M(xDlOpen,           "p(pp)");
      M(xDlError,          "p(pip)");
      M(xDlSym,            "p()");
      M(xDlClose,          "v(pp)");
      M(xRandomness,       "i(pip)");
      M(xSleep,            "i(pi)");
      M(xCurrentTime,      "i(pp)");
      M(xGetLastError,     "i(pip)");
      M(xCurrentTimeInt64, "i(pp)");
      M(xSetSystemCall,    "i(ppp)");
      M(xGetSystemCall,    "p(pp)");
      M(xNextSystemCall,   "p(pp)");
    } _StructBinder;
#undef CurrentStruct

#define CurrentStruct sqlite3_io_methods
    StructBinder {
      M(iVersion,               "i");
      M(xClose,                 "i(p)");
      M(xRead,                  "i(ppij)");
      M(xWrite,                 "i(ppij)");
      M(xTruncate,              "i(pj)");
      M(xSync,                  "i(pi)");
      M(xFileSize,              "i(pp)");
      M(xLock,                  "i(pi)");
      M(xUnlock,                "i(pi)");
      M(xCheckReservedLock,     "i(pp)");
      M(xFileControl,           "i(pip)");
      M(xSectorSize,            "i(p)");
      M(xDeviceCharacteristics, "i(p)");
      M(xShmMap,                "i(piiip)");
      M(xShmLock,               "i(piii)");
      M(xShmBarrier,            "v(p)");
      M(xShmUnmap,              "i(pi)");
      M(xFetch,                 "i(pjip)");
      M(xUnfetch,               "i(pjp)");
    } _StructBinder;
#undef CurrentStruct

#define CurrentStruct sqlite3_file
    StructBinder {
      M(pMethods, "p");
    } _StructBinder;
#undef CurrentStruct

#define CurrentStruct sqlite3_kvvfs_methods
    StructBinder {
      M(xRead,    "i(sspi)");
      M(xWrite,   "i(sss)");
      M(xDelete,  "i(ss)");
      M(nKeySize, "i");
    } _StructBinder;
#undef CurrentStruct


#define CurrentStruct sqlite3_vtab
    StructBinder {
      M(pModule, "p");
      M(nRef,    "i");
      M(zErrMsg, "p");
    } _StructBinder;
#undef CurrentStruct

#define CurrentStruct sqlite3_vtab_cursor
    StructBinder {
      M(pVtab, "p");
    } _StructBinder;
#undef CurrentStruct

#define CurrentStruct sqlite3_module
    StructBinder {
      M(iVersion,       "i");
      M(xCreate,        "i(ppippp)");
      M(xConnect,       "i(ppippp)");
      M(xBestIndex,     "i(pp)");
      M(xDisconnect,    "i(p)");
      M(xDestroy,       "i(p)");
      M(xOpen,          "i(pp)");
      M(xClose,         "i(p)");
      M(xFilter,        "i(pisip)");
      M(xNext,          "i(p)");
      M(xEof,           "i(p)");
      M(xColumn,        "i(ppi)");
      M(xRowid,         "i(pp)");
      M(xUpdate,        "i(pipp)");
      M(xBegin,         "i(p)");
      M(xSync,          "i(p)");
      M(xCommit,        "i(p)");
      M(xRollback,      "i(p)");
      M(xFindFunction,  "i(pispp)");
      M(xRename,        "i(ps)");
      // ^^^ v1. v2+ follows...
      M(xSavepoint,     "i(pi)");
      M(xRelease,       "i(pi)");
      M(xRollbackTo,    "i(pi)");
      // ^^^ v2. v3+ follows...
      M(xShadowName,    "i(s)");
    } _StructBinder;
#undef CurrentStruct
    
    /**
     ** Workaround: in order to map the various inner structs from
     ** sqlite3_index_info, we have to uplift those into constructs we
     ** can access by type name. These structs _must_ match their
     ** in-sqlite3_index_info counterparts byte for byte.
    */
    typedef struct {
      int iColumn;
      unsigned char op;
      unsigned char usable;
      int iTermOffset;
    } sqlite3_index_constraint;
    typedef struct {
      int iColumn;
      unsigned char desc;
    } sqlite3_index_orderby;
    typedef struct {
      int argvIndex;
      unsigned char omit;
    } sqlite3_index_constraint_usage;
    { /* Validate that the above struct sizeof()s match
      ** expectations. We could improve upon this by
      ** checking the offsetof() for each member. */
      const sqlite3_index_info siiCheck;
#define IndexSzCheck(T,M)           \
      (sizeof(T) == sizeof(*siiCheck.M))
      if(!IndexSzCheck(sqlite3_index_constraint,aConstraint)
         || !IndexSzCheck(sqlite3_index_orderby,aOrderBy)
         || !IndexSzCheck(sqlite3_index_constraint_usage,aConstraintUsage)){
        assert(!"sizeof mismatch in sqlite3_index_... struct(s)");
        return 0;
      }
#undef IndexSzCheck
    }

#define CurrentStruct sqlite3_index_constraint
    StructBinder {
      M(iColumn,        "i");
      M(op,             "C");
      M(usable,         "C");
      M(iTermOffset,    "i");
    } _StructBinder;
#undef CurrentStruct

#define CurrentStruct sqlite3_index_orderby
    StructBinder {
      M(iColumn,   "i");
      M(desc,      "C");
    } _StructBinder;
#undef CurrentStruct

#define CurrentStruct sqlite3_index_constraint_usage
    StructBinder {
      M(argvIndex,  "i");
      M(omit,       "C");
    } _StructBinder;
#undef CurrentStruct

#define CurrentStruct sqlite3_index_info
    StructBinder {
      M(nConstraint,        "i");
      M(aConstraint,        "p");
      M(nOrderBy,           "i");
      M(aOrderBy,           "p");
      M(aConstraintUsage,   "p");
      M(idxNum,             "i");
      M(idxStr,             "p");
      M(needToFreeIdxStr,   "i");
      M(orderByConsumed,    "i");
      M(estimatedCost,      "d");
      M(estimatedRows,      "j");
      M(idxFlags,           "i");
      M(colUsed,            "j");
    } _StructBinder;
#undef CurrentStruct

#if SQLITE_WASM_TESTS
#define CurrentStruct WasmTestStruct
    StructBinder {
      M(v4,    "i");
      M(cstr,  "s");
      M(ppV,   "p");
      M(v8,    "j");
      M(xFunc, "v(p)");
    } _StructBinder;
#undef CurrentStruct
#endif

  } out( "]"/*structs*/);

  out("}"/*top-level object*/);
  *zPos = 0;
  aBuffer[0] = '{'/*end of the race-condition workaround*/;
  return aBuffer;
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
** This function invokes the xDelete method of the given VFS (or the
** default VFS if pVfs is NULL), passing on the given filename. If
** zName is NULL, no default VFS is found, or it has no xDelete
** method, SQLITE_MISUSE is returned, else the result of the xDelete()
** call is returned.
*/
SQLITE_WASM_KEEP
int sqlite3_wasm_vfs_unlink(sqlite3_vfs *pVfs, const char *zName){
  int rc = SQLITE_MISUSE /* ??? */;
  if( 0==pVfs && 0!=zName ) pVfs = sqlite3_vfs_find(0);
  if( zName && pVfs && pVfs->xDelete ){
    rc = pVfs->xDelete(pVfs, zName, 1);
  }
  return rc;
}

/*
** This function is NOT part of the sqlite3 public API. It is strictly
** for use by the sqlite project's own JS/WASM bindings.
**
** Returns a pointer to the given DB's VFS for the given DB name,
** defaulting to "main" if zDbName is 0. Returns 0 if no db with the
** given name is open.
*/
SQLITE_WASM_KEEP
sqlite3_vfs * sqlite3_wasm_db_vfs(sqlite3 *pDb, const char *zDbName){
  sqlite3_vfs * pVfs = 0;
  sqlite3_file_control(pDb, zDbName ? zDbName : "main",
                       SQLITE_FCNTL_VFS_POINTER, &pVfs);
  return pVfs;
}

/*
** This function is NOT part of the sqlite3 public API. It is strictly
** for use by the sqlite project's own JS/WASM bindings.
**
** This function resets the given db pointer's database as described at
**
** https://sqlite.org/c3ref/c_dbconfig_defensive.html#sqlitedbconfigresetdatabase
**
** But beware: virtual tables destroyed that way do not have their
** xDestroy() called, so will leak if they require that function for
** proper cleanup.
**
** Returns 0 on success, an SQLITE_xxx code on error. Returns
** SQLITE_MISUSE if pDb is NULL.
*/
SQLITE_WASM_KEEP
int sqlite3_wasm_db_reset(sqlite3 *pDb){
  int rc = SQLITE_MISUSE;
  if( pDb ){
    sqlite3_table_column_metadata(pDb, "main", 0, 0, 0, 0, 0, 0, 0);
    rc = sqlite3_db_config(pDb, SQLITE_DBCONFIG_RESET_DATABASE, 1, 0);
    if( 0==rc ){
      rc = sqlite3_exec(pDb, "VACUUM", 0, 0, 0);
      sqlite3_db_config(pDb, SQLITE_DBCONFIG_RESET_DATABASE, 0, 0);
    }
  }
  return rc;
}

/*
** This function is NOT part of the sqlite3 public API. It is strictly
** for use by the sqlite project's own JS/WASM bindings.
**
** Uses the given database's VFS xRead to stream the db file's
** contents out to the given callback. The callback gets a single
** chunk of size n (its 2nd argument) on each call and must return 0
** on success, non-0 on error. This function returns 0 on success,
** SQLITE_NOTFOUND if no db is open, or propagates any other non-0
** code from the callback. Note that this is not thread-friendly: it
** expects that it will be the only thread reading the db file and
** takes no measures to ensure that is the case.
**
** This implementation appears to work fine, but
** sqlite3_wasm_db_serialize() is arguably the better way to achieve
** this.
*/
SQLITE_WASM_KEEP
int sqlite3_wasm_db_export_chunked( sqlite3* pDb,
                                    int (*xCallback)(unsigned const char *zOut, int n) ){
  sqlite3_int64 nSize = 0;
  sqlite3_int64 nPos = 0;
  sqlite3_file * pFile = 0;
  unsigned char buf[1024 * 8];
  int nBuf = (int)sizeof(buf);
  int rc = pDb
    ? sqlite3_file_control(pDb, "main",
                           SQLITE_FCNTL_FILE_POINTER, &pFile)
    : SQLITE_NOTFOUND;
  if( rc ) return rc;
  rc = pFile->pMethods->xFileSize(pFile, &nSize);
  if( rc ) return rc;
  if(nSize % nBuf){
    /* DB size is not an even multiple of the buffer size. Reduce
    ** buffer size so that we do not unduly inflate the db size
    ** with zero-padding when exporting. */
    if(0 == nSize % 4096) nBuf = 4096;
    else if(0 == nSize % 2048) nBuf = 2048;
    else if(0 == nSize % 1024) nBuf = 1024;
    else nBuf = 512;
  }
  for( ; 0==rc && nPos<nSize; nPos += nBuf ){
    rc = pFile->pMethods->xRead(pFile, buf, nBuf, nPos);
    if( SQLITE_IOERR_SHORT_READ == rc ){
      rc = (nPos + nBuf) < nSize ? rc : 0/*assume EOF*/;
    }
    if( 0==rc ) rc = xCallback(buf, nBuf);
  }
  return rc;
}

/*
** This function is NOT part of the sqlite3 public API. It is strictly
** for use by the sqlite project's own JS/WASM bindings.
**
** A proxy for sqlite3_serialize() which serializes the schema zSchema
** of pDb, placing the serialized output in pOut and nOut. nOut may be
** NULL. If zSchema is NULL then "main" is assumed. If pDb or pOut are
** NULL then SQLITE_MISUSE is returned. If allocation of the
** serialized copy fails, SQLITE_NOMEM is returned.  On success, 0 is
** returned and `*pOut` will contain a pointer to the memory unless
** mFlags includes SQLITE_SERIALIZE_NOCOPY and the database has no
** contiguous memory representation, in which case `*pOut` will be
** NULL but 0 will be returned.
**
** If `*pOut` is not NULL, the caller is responsible for passing it to
** sqlite3_free() to free it.
*/
SQLITE_WASM_KEEP
int sqlite3_wasm_db_serialize( sqlite3 *pDb, const char *zSchema,
                               unsigned char **pOut,
                               sqlite3_int64 *nOut, unsigned int mFlags ){
  unsigned char * z;
  if( !pDb || !pOut ) return SQLITE_MISUSE;
  if( nOut ) *nOut = 0;
  z = sqlite3_serialize(pDb, zSchema ? zSchema : "main", nOut, mFlags);
  if( z || (SQLITE_SERIALIZE_NOCOPY & mFlags) ){
    *pOut = z;
    return 0;
  }else{
    return SQLITE_NOMEM;
  }
}

/*
** This function is NOT part of the sqlite3 public API. It is strictly
** for use by the sqlite project's own JS/WASM bindings.
**
** Creates a new file using the I/O API of the given VFS, containing
** the given number of bytes of the given data. If the file exists, it
** is truncated to the given length and populated with the given
** data.
**
** This function exists so that we can implement the equivalent of
** Emscripten's FS.createDataFile() in a VFS-agnostic way. This
** functionality is intended for use in uploading database files.
**
** Not all VFSes support this functionality, e.g. the "kvvfs" does
** not.
**
** If pVfs is NULL, sqlite3_vfs_find(0) is used.
**
** If zFile is NULL, pVfs is NULL (and sqlite3_vfs_find(0) returns
** NULL), or nData is negative, SQLITE_MISUSE are returned.
**
** On success, it creates a new file with the given name, populated
** with the fist nData bytes of pData. If pData is NULL, the file is
** created and/or truncated to nData bytes.
**
** Whether or not directory components of zFilename are created
** automatically or not is unspecified: that detail is left to the
** VFS. The "opfs" VFS, for example, creates them.
**
** If an error happens while populating or truncating the file, the
** target file will be deleted (if needed) if this function created
** it. If this function did not create it, it is not deleted but may
** be left in an undefined state.
**
** Returns 0 on success. On error, it returns a code described above
** or propagates a code from one of the I/O methods.
**
** Design note: nData is an integer, instead of int64, for WASM
** portability, so that the API can still work in builds where BigInt
** support is disabled or unavailable.
*/
SQLITE_WASM_KEEP
int sqlite3_wasm_vfs_create_file( sqlite3_vfs *pVfs,
                                  const char *zFilename,
                                  const unsigned char * pData,
                                  int nData ){
  int rc;
  sqlite3_file *pFile = 0;
  sqlite3_io_methods const *pIo;
  const int openFlags = SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE;
  int flagsOut = 0;
  int fileExisted = 0;
  int doUnlock = 0;
  const unsigned char *pPos = pData;
  const int blockSize = 512
    /* Because we are using pFile->pMethods->xWrite() for writing, and
    ** it may have a buffer limit related to sqlite3's pager size, we
    ** conservatively write in 512-byte blocks (smallest page
    ** size). */;
  //fprintf(stderr, "pVfs=%p, zFilename=%s, nData=%d\n", pVfs, zFilename, nData);
  if( !pVfs ) pVfs = sqlite3_vfs_find(0);
  if( !pVfs || !zFilename || nData<0 ) return SQLITE_MISUSE;
  pVfs->xAccess(pVfs, zFilename, SQLITE_ACCESS_EXISTS, &fileExisted);
  rc = sqlite3OsOpenMalloc(pVfs, zFilename, &pFile, openFlags, &flagsOut);
#if 0
# define RC fprintf(stderr,"create_file(%s,%s) @%d rc=%d\n", \
                    pVfs->zName, zFilename, __LINE__, rc);
#else
# define RC
#endif
  RC;
  if(rc) return rc;
  pIo = pFile->pMethods;
  if( pIo->xLock ) {
    /* We need xLock() in order to accommodate the OPFS VFS, as it
    ** obtains a writeable handle via the lock operation and releases
    ** it in xUnlock(). If we don't do those here, we have to add code
    ** to the VFS to account check whether it was locked before
    ** xFileSize(), xTruncate(), and the like, and release the lock
    ** only if it was unlocked when the op was started. */
    rc = pIo->xLock(pFile, SQLITE_LOCK_EXCLUSIVE);
    RC;
    doUnlock = 0==rc;
  }
  if( 0==rc ){
    rc = pIo->xTruncate(pFile, nData);
    RC;
  }
  if( 0==rc && 0!=pData && nData>0 ){
    while( 0==rc && nData>0 ){
      const int n = nData>=blockSize ? blockSize : nData;
      rc = pIo->xWrite(pFile, pPos, n, (sqlite3_int64)(pPos - pData));
      RC;
      nData -= n;
      pPos += n;
    }
    if( 0==rc && nData>0 ){
      assert( nData<blockSize );
      rc = pIo->xWrite(pFile, pPos, nData,
                       (sqlite3_int64)(pPos - pData));
      RC;
    }
  }
  if( pIo->xUnlock && doUnlock!=0 ){
    pIo->xUnlock(pFile, SQLITE_LOCK_NONE);
  }
  pIo->xClose(pFile);
  if( rc!=0 && 0==fileExisted ){
    pVfs->xDelete(pVfs, zFilename, 1);
  }
  RC;
#undef RC
  return rc;
}

/*
** This function is NOT part of the sqlite3 public API. It is strictly
** for use by the sqlite project's own JS/WASM bindings.
**
** Allocates sqlite3KvvfsMethods.nKeySize bytes from
** sqlite3_wasm_pstack_alloc() and returns 0 if that allocation fails,
** else it passes that string to kvstorageMakeKey() and returns a
** NUL-terminated pointer to that string. It is up to the caller to
** use sqlite3_wasm_pstack_restore() to free the returned pointer.
*/
SQLITE_WASM_KEEP
char * sqlite3_wasm_kvvfsMakeKeyOnPstack(const char *zClass,
                                         const char *zKeyIn){
  assert(sqlite3KvvfsMethods.nKeySize>24);
  char *zKeyOut =
    (char *)sqlite3_wasm_pstack_alloc(sqlite3KvvfsMethods.nKeySize);
  if(zKeyOut){
    kvstorageMakeKey(zClass, zKeyIn, zKeyOut);
  }
  return zKeyOut;
}

/*
** This function is NOT part of the sqlite3 public API. It is strictly
** for use by the sqlite project's own JS/WASM bindings.
**
** Returns the pointer to the singleton object which holds the kvvfs
** I/O methods and associated state.
*/
SQLITE_WASM_KEEP
sqlite3_kvvfs_methods * sqlite3_wasm_kvvfs_methods(void){
  return &sqlite3KvvfsMethods;
}

/*
** This function is NOT part of the sqlite3 public API. It is strictly
** for use by the sqlite project's own JS/WASM bindings.
**
** This is a proxy for the variadic sqlite3_vtab_config() which passes
** its argument on, or not, to sqlite3_vtab_config(), depending on the
** value of its 2nd argument. Returns the result of
** sqlite3_vtab_config(), or SQLITE_MISUSE if the 2nd arg is not a
** valid value.
*/
SQLITE_WASM_KEEP
int sqlite3_wasm_vtab_config(sqlite3 *pDb, int op, int arg){
  switch(op){
  case SQLITE_VTAB_DIRECTONLY:
  case SQLITE_VTAB_INNOCUOUS:
    return sqlite3_vtab_config(pDb, op);
  case SQLITE_VTAB_CONSTRAINT_SUPPORT:
    return sqlite3_vtab_config(pDb, op, arg);
  default:
    return SQLITE_MISUSE;
  }
}

/*
** This function is NOT part of the sqlite3 public API. It is strictly
** for use by the sqlite project's own JS/WASM bindings.
**
** Wrapper for the variants of sqlite3_db_config() which take
** (int,int*) variadic args.
*/
SQLITE_WASM_KEEP
int sqlite3_wasm_db_config_ip(sqlite3 *pDb, int op, int arg1, int* pArg2){
  switch(op){
    case SQLITE_DBCONFIG_ENABLE_FKEY:
    case SQLITE_DBCONFIG_ENABLE_TRIGGER:
    case SQLITE_DBCONFIG_ENABLE_FTS3_TOKENIZER:
    case SQLITE_DBCONFIG_ENABLE_LOAD_EXTENSION:
    case SQLITE_DBCONFIG_NO_CKPT_ON_CLOSE:
    case SQLITE_DBCONFIG_ENABLE_QPSG:
    case SQLITE_DBCONFIG_TRIGGER_EQP:
    case SQLITE_DBCONFIG_RESET_DATABASE:
    case SQLITE_DBCONFIG_DEFENSIVE:
    case SQLITE_DBCONFIG_WRITABLE_SCHEMA:
    case SQLITE_DBCONFIG_LEGACY_ALTER_TABLE:
    case SQLITE_DBCONFIG_DQS_DML:
    case SQLITE_DBCONFIG_DQS_DDL:
    case SQLITE_DBCONFIG_ENABLE_VIEW:
    case SQLITE_DBCONFIG_LEGACY_FILE_FORMAT:
    case SQLITE_DBCONFIG_TRUSTED_SCHEMA:
      return sqlite3_db_config(pDb, op, arg1, pArg2);
    default: return SQLITE_MISUSE;
  }
}

/*
** This function is NOT part of the sqlite3 public API. It is strictly
** for use by the sqlite project's own JS/WASM bindings.
**
** Wrapper for the variants of sqlite3_db_config() which take
** (void*,int,int) variadic args.
*/
SQLITE_WASM_KEEP
int sqlite3_wasm_db_config_pii(sqlite3 *pDb, int op, void * pArg1, int arg2, int arg3){
  switch(op){
    case SQLITE_DBCONFIG_LOOKASIDE:
      return sqlite3_db_config(pDb, op, pArg1, arg2, arg3);
    default: return SQLITE_MISUSE;
  }
}

/*
** This function is NOT part of the sqlite3 public API. It is strictly
** for use by the sqlite project's own JS/WASM bindings.
**
** Wrapper for the variants of sqlite3_db_config() which take
** (const char *) variadic args.
*/
SQLITE_WASM_KEEP
int sqlite3_wasm_db_config_s(sqlite3 *pDb, int op, const char *zArg){
  switch(op){
    case SQLITE_DBCONFIG_MAINDBNAME:
      return sqlite3_db_config(pDb, op, zArg);
    default: return SQLITE_MISUSE;
  }
}


/*
** This function is NOT part of the sqlite3 public API. It is strictly
** for use by the sqlite project's own JS/WASM bindings.
**
** Binding for combinations of sqlite3_config() arguments which take
** a single integer argument.
*/
SQLITE_WASM_KEEP
int sqlite3_wasm_config_i(int op, int arg){
  return sqlite3_config(op, arg);
}

/*
** This function is NOT part of the sqlite3 public API. It is strictly
** for use by the sqlite project's own JS/WASM bindings.
**
** Binding for combinations of sqlite3_config() arguments which take
** two int arguments.
*/
SQLITE_WASM_KEEP
int sqlite3_wasm_config_ii(int op, int arg1, int arg2){
  return sqlite3_config(op, arg1, arg2);
}

/*
** This function is NOT part of the sqlite3 public API. It is strictly
** for use by the sqlite project's own JS/WASM bindings.
**
** Binding for combinations of sqlite3_config() arguments which take
** a single i64 argument.
*/
SQLITE_WASM_KEEP
int sqlite3_wasm_config_j(int op, sqlite3_int64 arg){
  return sqlite3_config(op, arg);
}

#if 0
// Pending removal after verification of a workaround discussed in the
// forum post linked to below.
/*
** This function is NOT part of the sqlite3 public API. It is strictly
** for use by the sqlite project's own JS/WASM bindings.
**
** Returns a pointer to sqlite3_free(). In compliant browsers the
** return value, when passed to sqlite3.wasm.exports.functionEntry(),
** must resolve to the same function as
** sqlite3.wasm.exports.sqlite3_free. i.e. from a dev console where
** sqlite3 is exported globally, the following must be true:
**
** ```
** sqlite3.wasm.functionEntry(
**   sqlite3.wasm.exports.sqlite3_wasm_ptr_to_sqlite3_free()
** ) === sqlite3.wasm.exports.sqlite3_free
** ```
**
** Using a function to return this pointer, as opposed to exporting it
** via sqlite3_wasm_enum_json(), is an attempt to work around a
** Safari-specific quirk covered at
** https://sqlite.org/forum/info/e5b20e1feb37a19a.
**/
SQLITE_WASM_KEEP
void * sqlite3_wasm_ptr_to_sqlite3_free(void){
  return (void*)sqlite3_free;
}
#endif

#if defined(__EMSCRIPTEN__) && defined(SQLITE_ENABLE_WASMFS)
#include <emscripten/wasmfs.h>

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
** NULL or empty, it defaults to "/opfs".
**
** Returns 0 on success, SQLITE_NOMEM if instantiation of the backend
** object fails, SQLITE_IOERR if mkdir() of the zMountPoint dir in
** the virtual FS fails. In builds compiled without SQLITE_ENABLE_WASMFS
** defined, SQLITE_NOTFOUND is returned without side effects.
*/
SQLITE_WASM_KEEP
int sqlite3_wasm_init_wasmfs(const char *zMountPoint){
  static backend_t pOpfs = 0;
  if( !zMountPoint || !*zMountPoint ) zMountPoint = "/opfs";
  if( !pOpfs ){
    pOpfs = wasmfs_create_opfs_backend();
  }
  /** It's not enough to instantiate the backend. We have to create a
      mountpoint in the VFS and attach the backend to it. */
  if( pOpfs && 0!=access(zMountPoint, F_OK) ){
    /* Note that this check and is not robust but it will
       hypothetically suffice for the transient wasm-based virtual
       filesystem we're currently running in. */
    const int rc = wasmfs_create_directory(zMountPoint, 0777, pOpfs);
    /*emscripten_console_logf("OPFS mkdir(%s) rc=%d", zMountPoint, rc);*/
    if(rc) return SQLITE_IOERR;
  }
  return pOpfs ? 0 : SQLITE_NOMEM;
}
#else
SQLITE_WASM_KEEP
int sqlite3_wasm_init_wasmfs(const char *zUnused){
  //emscripten_console_warn("WASMFS OPFS is not compiled in.");
  if(zUnused){/*unused*/}
  return SQLITE_NOTFOUND;
}
#endif /* __EMSCRIPTEN__ && SQLITE_ENABLE_WASMFS */

#if SQLITE_WASM_TESTS

SQLITE_WASM_KEEP
int sqlite3_wasm_test_intptr(int * p){
  return *p = *p * 2;
}

SQLITE_WASM_KEEP
void * sqlite3_wasm_test_voidptr(void * p){
  return p;
}

SQLITE_WASM_KEEP
int64_t sqlite3_wasm_test_int64_max(void){
  return (int64_t)0x7fffffffffffffff;
}

SQLITE_WASM_KEEP
int64_t sqlite3_wasm_test_int64_min(void){
  return ~sqlite3_wasm_test_int64_max();
}

SQLITE_WASM_KEEP
int64_t sqlite3_wasm_test_int64_times2(int64_t x){
  return x * 2;
}

SQLITE_WASM_KEEP
void sqlite3_wasm_test_int64_minmax(int64_t * min, int64_t *max){
  *max = sqlite3_wasm_test_int64_max();
  *min = sqlite3_wasm_test_int64_min();
  /*printf("minmax: min=%lld, max=%lld\n", *min, *max);*/
}

SQLITE_WASM_KEEP
int64_t sqlite3_wasm_test_int64ptr(int64_t * p){
  /*printf("sqlite3_wasm_test_int64ptr( @%lld = 0x%llx )\n", (int64_t)p, *p);*/
  return *p = *p * 2;
}

SQLITE_WASM_KEEP
void sqlite3_wasm_test_stack_overflow(int recurse){
  if(recurse) sqlite3_wasm_test_stack_overflow(recurse);
}

/* For testing the 'string:dealloc' whwasmutil.xWrap() conversion. */
SQLITE_WASM_KEEP
char * sqlite3_wasm_test_str_hello(int fail){
  char * s = fail ? 0 : (char *)sqlite3_malloc(6);
  if(s){
    memcpy(s, "hello", 5);
    s[5] = 0;
  }
  return s;
}
#endif /* SQLITE_WASM_TESTS */

#undef SQLITE_WASM_KEEP

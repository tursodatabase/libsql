/*
** 2023-07-21
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
*************************************************************************
** This file implements the JNI bindings declared in
** org.sqlite.jni.capi.CApi (from which sqlite3-jni.h is generated).
*/

/*
** If you found this comment by searching the code for
** CallStaticObjectMethod because it appears in console output then
** you're probably the victim of an OpenJDK bug:
**
** https://bugs.openjdk.org/browse/JDK-8130659
**
** It's known to happen with OpenJDK v8 but not with v19. It was
** triggered by this code long before it made any use of
** CallStaticObjectMethod().
*/

/*
** Define any SQLITE_... config defaults we want if they aren't
** overridden by the builder. Please keep these alphabetized.
*/

/**********************************************************************/
/* SQLITE_D... */
#ifndef SQLITE_DEFAULT_CACHE_SIZE
# define SQLITE_DEFAULT_CACHE_SIZE -16384
#endif
#if !defined(SQLITE_DEFAULT_PAGE_SIZE)
# define SQLITE_DEFAULT_PAGE_SIZE 8192
#endif
#ifndef SQLITE_DQS
#  define SQLITE_DQS 0
#endif

/**********************************************************************/
/* SQLITE_ENABLE_... */
/*
** Unconditionally enable API_ARMOR in the JNI build. It ensures that
** public APIs behave predictable in the face of passing illegal NULLs
** or ranges which might otherwise invoke undefined behavior.
*/
#undef SQLITE_ENABLE_API_ARMOR
#define SQLITE_ENABLE_API_ARMOR 1

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
#ifndef SQLITE_ENABLE_MATH_FUNCTIONS
#  define SQLITE_ENABLE_MATH_FUNCTIONS 1
#endif
#ifndef SQLITE_ENABLE_OFFSET_SQL_FUNC
#  define SQLITE_ENABLE_OFFSET_SQL_FUNC 1
#endif
#ifndef SQLITE_ENABLE_RTREE
#  define SQLITE_ENABLE_RTREE 1
#endif
//#ifndef SQLITE_ENABLE_SESSION
//#  define SQLITE_ENABLE_SESSION 1
//#endif
#ifndef SQLITE_ENABLE_STMTVTAB
#  define SQLITE_ENABLE_STMTVTAB 1
#endif
//#ifndef SQLITE_ENABLE_UNKNOWN_SQL_FUNCTION
//#  define SQLITE_ENABLE_UNKNOWN_SQL_FUNCTION
//#endif

/**********************************************************************/
/* SQLITE_J... */
#ifdef SQLITE_JNI_FATAL_OOM
#if !SQLITE_JNI_FATAL_OOM
#undef SQLITE_JNI_FATAL_OOM
#endif
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
#ifdef SQLITE_OMIT_UTF16
/* UTF16 is required for java */
# undef SQLITE_OMIT_UTF16 1
#endif

/**********************************************************************/
/* SQLITE_T... */
#ifndef SQLITE_TEMP_STORE
# define SQLITE_TEMP_STORE 2
#endif
#ifndef SQLITE_THREADSAFE
# define SQLITE_THREADSAFE 1
#endif

/**********************************************************************/
/* SQLITE_USE_... */
#ifndef SQLITE_USE_URI
#  define SQLITE_USE_URI 1
#endif


/*
** Which sqlite3.c we're using needs to be configurable to enable
** building against a custom copy, e.g. the SEE variant. We have to
** include sqlite3.c, as opposed to sqlite3.h, in order to get access
** to some interal details like SQLITE_MAX_... and friends. This
** increases the rebuild time considerably but we need this in order
** to access some internal functionality and keep the to-Java-exported
** values of SQLITE_MAX_... and SQLITE_LIMIT_... in sync with the C
** build.
*/
#ifndef SQLITE_C
# define SQLITE_C sqlite3.c
#endif
#define INC__STRINGIFY_(f) #f
#define INC__STRINGIFY(f) INC__STRINGIFY_(f)
#include INC__STRINGIFY(SQLITE_C)
#undef INC__STRINGIFY_
#undef INC__STRINGIFY
#undef SQLITE_C

/*
** End of the sqlite3 lib setup. What follows is JNI-specific.
*/

#include "sqlite3-jni.h"
#include <assert.h>
#include <stdio.h> /* only for testing/debugging */
#include <stdint.h> /* intptr_t for 32-bit builds */

/* Only for debugging */
#define MARKER(pfexp)                                               \
  do{ printf("MARKER: %s:%d:%s():\t",__FILE__,__LINE__,__func__);   \
    printf pfexp;                                                   \
  } while(0)

/*
** Creates a verbose JNI function name. Suffix must be
** the JNI-mangled form of the function's name, minus the
** prefix seen in this macro.
*/
#define JniFuncName(Suffix) \
  Java_org_sqlite_jni_capi_CApi_sqlite3_ ## Suffix

/* Prologue for JNI function declarations and definitions. */
#define JniDecl(ReturnType,Suffix) \
  JNIEXPORT ReturnType JNICALL JniFuncName(Suffix)

/*
** S3JniApi's intent is that CFunc be the C API func(s) the
** being-declared JNI function is wrapping, making it easier to find
** that function's JNI-side entry point. The other args are for JniDecl.
** See the many examples in this file.
*/
#define S3JniApi(CFunc,ReturnType,Suffix) JniDecl(ReturnType,Suffix)

/*
** S3JniCast_L2P and P2L cast jlong (64-bit) to/from pointers. This is
** required for casting warning-free on 32-bit builds, where we
** otherwise get complaints that we're casting between different-sized
** int types.
**
** This use of intptr_t is the _only_ reason we require <stdint.h>
** which, in turn, requires building with -std=c99 (or later).
**
** See also: the notes for LongPtrGet_T.
*/
#define S3JniCast_L2P(JLongAsPtr) (void*)((intptr_t)(JLongAsPtr))
#define S3JniCast_P2L(PTR) (jlong)((intptr_t)(PTR))

/*
** Shortcuts for the first 2 parameters to all JNI bindings.
**
** The type of the jSelf arg differs, but no docs seem to mention
** this: for static methods it's of type jclass and for non-static
** it's jobject. jobject actually works for all funcs, in the sense
** that it compiles and runs so long as we don't use jSelf (which is
** only rarely needed in this code), but to be pedantically correct we
** need the proper type in the signature.
**
** https://docs.oracle.com/javase/8/docs/technotes/guides/jni/spec/design.html#jni_interface_functions_and_pointers
*/
#define JniArgsEnvObj JNIEnv * env, jobject jSelf
#define JniArgsEnvClass JNIEnv * env, jclass jKlazz
/*
** Helpers to account for -Xcheck:jni warnings about not having
** checked for exceptions.
*/
#define S3JniIfThrew if( (*env)->ExceptionCheck(env) )
#define S3JniExceptionClear (*env)->ExceptionClear(env)
#define S3JniExceptionReport (*env)->ExceptionDescribe(env)
#define S3JniExceptionIgnore S3JniIfThrew S3JniExceptionClear
#define S3JniExceptionWarnIgnore \
  S3JniIfThrew {S3JniExceptionReport; S3JniExceptionClear;}(void)0
#define S3JniExceptionWarnCallbackThrew(STR)             \
  MARKER(("WARNING: " STR " MUST NOT THROW.\n"));  \
  (*env)->ExceptionDescribe(env)

/** To be used for cases where we're _really_ not expecting an
    exception, e.g. looking up well-defined Java class members. */
#define S3JniExceptionIsFatal(MSG) S3JniIfThrew {\
    S3JniExceptionReport; S3JniExceptionClear; \
    (*env)->FatalError(env, MSG); \
  }

/*
** Declares local var env = s3jni_env(). All JNI calls involve a
** JNIEnv somewhere, always named env, and many of our macros assume
** env is in scope. Where it's not, but should be, use this to make it
** so.
*/
#define S3JniDeclLocal_env JNIEnv * const env = s3jni_env()

/* Fail fatally with an OOM message. */
static inline void s3jni_oom(JNIEnv * const env){
  (*env)->FatalError(env, "SQLite3 JNI is out of memory.") /* does not return */;
}

/*
** sqlite3_malloc() proxy which fails fatally on OOM.  This should
** only be used for routines which manage global state and have no
** recovery strategy for OOM. For sqlite3 API which can reasonably
** return SQLITE_NOMEM, s3jni_malloc() should be used instead.
*/
static void * s3jni_malloc_or_die(JNIEnv * const env, size_t n){
  void * const rv = sqlite3_malloc(n);
  if( n && !rv ) s3jni_oom(env);
  return rv;
}

/*
** Works like sqlite3_malloc() unless built with SQLITE_JNI_FATAL_OOM,
** in which case it calls s3jni_oom() on OOM.
*/
#ifdef SQLITE_JNI_FATAL_OOM
#define s3jni_malloc(SIZE) s3jni_malloc_or_die(env, SIZE)
#else
#define s3jni_malloc(SIZE) sqlite3_malloc(((void)env,(SIZE)))
/* the ((void)env) trickery here is to avoid ^^^^^^ an otherwise
   unused arg in at least one place. */
#endif

/*
** Works like sqlite3_realloc() unless built with SQLITE_JNI_FATAL_OOM,
** in which case it calls s3jni_oom() on OOM.
*/
#ifdef SQLITE_JNI_FATAL_OOM
static void * s3jni_realloc_or_die(JNIEnv * const env, void * p, size_t n){
  void * const rv = sqlite3_realloc(p, (int)n);
  if( n && !rv ) s3jni_oom(env);
  return rv;
}
#define s3jni_realloc(MEM,SIZE) s3jni_realloc_or_die(env, (MEM), (SIZE))
#else
#define s3jni_realloc(MEM,SIZE) sqlite3_realloc((MEM), ((void)env, (SIZE)))
#endif

/* Fail fatally if !EXPR. */
#define s3jni_oom_fatal(EXPR) if( !(EXPR) ) s3jni_oom(env)
/* Maybe fail fatally if !EXPR. */
#ifdef SQLITE_JNI_FATAL_OOM
#define s3jni_oom_check s3jni_oom_fatal
#else
#define s3jni_oom_check(EXPR)
#endif
//#define S3JniDb_oom(pDb,EXPR) ((EXPR) ? sqlite3OomFault(pDb) : 0)

#define s3jni_db_oom(pDb) (void)((pDb) ? ((pDb)->mallocFailed=1) : 0)

/* Helpers for Java value reference management. */
static jobject s3jni_ref_global(JNIEnv * const env, jobject const v){
  jobject const rv = v ? (*env)->NewGlobalRef(env, v) : NULL;
  s3jni_oom_fatal( v ? !!rv : 1 );
  return rv;
}
static jobject s3jni_ref_local(JNIEnv * const env, jobject const v){
  jobject const rv = v ? (*env)->NewLocalRef(env, v) : NULL;
  s3jni_oom_fatal( v ? !!rv : 1 );
  return rv;
}
static inline void s3jni_unref_global(JNIEnv * const env, jobject const v){
  if( v ) (*env)->DeleteGlobalRef(env, v);
}
static inline void s3jni_unref_local(JNIEnv * const env, jobject const v){
  if( v ) (*env)->DeleteLocalRef(env, v);
}
#define S3JniRefGlobal(VAR) s3jni_ref_global(env, (VAR))
#define S3JniRefLocal(VAR) s3jni_ref_local(env, (VAR))
#define S3JniUnrefGlobal(VAR) s3jni_unref_global(env, (VAR))
#define S3JniUnrefLocal(VAR) s3jni_unref_local(env, (VAR))

/*
** Lookup key type for use with s3jni_nphop() and a cache of a
** frequently-needed Java-side class reference and one or two Java
** class member IDs.
*/
typedef struct S3JniNphOp S3JniNphOp;
struct S3JniNphOp {
  const int index             /* index into S3JniGlobal.nph[] */;
  const char * const zName    /* Full Java name of the class */;
  const char * const zMember  /* Name of member property */;
  const char * const zTypeSig /* JNI type signature of zMember */;
  /*
  ** klazz is a global ref to the class represented by pRef.
  **
  ** According to:
  **
  **   https://developer.ibm.com/articles/j-jni/
  **
  ** > ... the IDs returned for a given class don't change for the
  **   lifetime of the JVM process. But the call to get the field or
  **   method can require significant work in the JVM, because fields
  **   and methods might have been inherited from superclasses, making
  **   the JVM walk up the class hierarchy to find them. Because the
  **   IDs are the same for a given class, you should look them up
  **   once and then reuse them. Similarly, looking up class objects
  **   can be expensive, so they should be cached as well.
  */
  jclass klazz;
  volatile jfieldID fidValue  /* NativePointerHolder.nativePointer or
                              ** OutputPointer.T.value */;
  volatile jmethodID midCtor  /* klazz's no-arg constructor. Used by
                              ** NativePointerHolder_new(). */;
};

/*
** Cache keys for each concrete NativePointerHolder subclasses and
** OutputPointer.T types. The members are to be used with s3jni_nphop()
** and friends, and each one's member->index corresponds to its index
** in the S3JniGlobal.nph[] array.
*/
static const struct {
  const S3JniNphOp sqlite3;
  const S3JniNphOp sqlite3_backup;
  const S3JniNphOp sqlite3_blob;
  const S3JniNphOp sqlite3_context;
  const S3JniNphOp sqlite3_stmt;
  const S3JniNphOp sqlite3_value;
  const S3JniNphOp OutputPointer_Bool;
  const S3JniNphOp OutputPointer_Int32;
  const S3JniNphOp OutputPointer_Int64;
  const S3JniNphOp OutputPointer_sqlite3;
  const S3JniNphOp OutputPointer_sqlite3_blob;
  const S3JniNphOp OutputPointer_sqlite3_stmt;
  const S3JniNphOp OutputPointer_sqlite3_value;
  const S3JniNphOp OutputPointer_String;
#ifdef SQLITE_ENABLE_FTS5
  const S3JniNphOp OutputPointer_ByteArray;
  const S3JniNphOp Fts5Context;
  const S3JniNphOp Fts5ExtensionApi;
  const S3JniNphOp fts5_api;
  const S3JniNphOp fts5_tokenizer;
  const S3JniNphOp Fts5Tokenizer;
#endif
} S3JniNphOps = {
#define MkRef(INDEX, KLAZZ, MEMBER, SIG) \
  { INDEX, "org/sqlite/jni/" KLAZZ, MEMBER, SIG }
/* NativePointerHolder ref */
#define RefN(INDEX, KLAZZ) MkRef(INDEX, KLAZZ, "nativePointer", "J")
/* OutputPointer.T ref */
#define RefO(INDEX, KLAZZ, SIG) MkRef(INDEX, KLAZZ, "value", SIG)
  RefN(0,  "capi/sqlite3"),
  RefN(1,  "capi/sqlite3_backup"),
  RefN(2,  "capi/sqlite3_blob"),
  RefN(3,  "capi/sqlite3_context"),
  RefN(4,  "capi/sqlite3_stmt"),
  RefN(5,  "capi/sqlite3_value"),
  RefO(6,  "capi/OutputPointer$Bool",  "Z"),
  RefO(7,  "capi/OutputPointer$Int32", "I"),
  RefO(8,  "capi/OutputPointer$Int64", "J"),
  RefO(9,  "capi/OutputPointer$sqlite3",
           "Lorg/sqlite/jni/capi/sqlite3;"),
  RefO(10, "capi/OutputPointer$sqlite3_blob",
           "Lorg/sqlite/jni/capi/sqlite3_blob;"),
  RefO(11, "capi/OutputPointer$sqlite3_stmt",
           "Lorg/sqlite/jni/capi/sqlite3_stmt;"),
  RefO(12, "capi/OutputPointer$sqlite3_value",
           "Lorg/sqlite/jni/capi/sqlite3_value;"),
  RefO(13, "capi/OutputPointer$String", "Ljava/lang/String;"),
#ifdef SQLITE_ENABLE_FTS5
  RefO(14, "capi/OutputPointer$ByteArray", "[B"),
  RefN(15, "fts5/Fts5Context"),
  RefN(16, "fts5/Fts5ExtensionApi"),
  RefN(17, "fts5/fts5_api"),
  RefN(18, "fts5/fts5_tokenizer"),
  RefN(19, "fts5/Fts5Tokenizer")
#endif
#undef MkRef
#undef RefN
#undef RefO
};

#define S3JniNph(T) &S3JniNphOps.T

enum {
  /*
  ** Size of the NativePointerHolder cache.  Need enough space for
  ** (only) the library's NativePointerHolder and OutputPointer types,
  ** a fixed count known at build-time.  This value needs to be
  ** exactly the number of S3JniNphOp entries in the S3JniNphOps
  ** object.
  */
  S3Jni_NphCache_size = sizeof(S3JniNphOps) / sizeof(S3JniNphOp)
};

/*
** State for binding C callbacks to Java methods.
*/
typedef struct S3JniHook S3JniHook;
struct S3JniHook{
  jobject jObj            /* global ref to Java instance */;
  jmethodID midCallback   /* callback method. Signature depends on
                          ** jObj's type */;
  /* We lookup the jObj.xDestroy() method as-needed for contexts which
  ** support custom finalizers. Fundamentally we can support them for
  ** any Java type, but we only want to expose support for them where
  ** the C API does. */
  jobject jExtra          /* Global ref to a per-hook-type value */;
  int doXDestroy          /* If true then S3JniHook_unref() will call
                             jObj->xDestroy() if it's available. */;
  S3JniHook * pNext      /* Next entry in S3Global.hooks.aFree */;
};
/* For clean bitwise-copy init of local instances. */
static const S3JniHook S3JniHook_empty = {0,0,0,0,0};

/*
** Per-(sqlite3*) state for various JNI bindings.  This state is
** allocated as needed, cleaned up in sqlite3_close(_v2)(), and
** recycled when possible.
**
** Trivia: vars and parameters of this type are often named "ps"
** because this class used to have a name for which that abbreviation
** made sense.
*/
typedef struct S3JniDb S3JniDb;
struct S3JniDb {
  sqlite3 *pDb  /* The associated db handle */;
  jobject jDb   /* A global ref of the output object which gets
                   returned from sqlite3_open(_v2)(). We need this in
                   order to have an object to pass to routines like
                   sqlite3_collation_needed()'s callback, or else we
                   have to dynamically create one for that purpose,
                   which would be fine except that it would be a
                   different instance (and maybe even a different
                   class) than the one the user may expect to
                   receive. */;
  char * zMainDbName  /* Holds the string allocated on behalf of
                         SQLITE_DBCONFIG_MAINDBNAME. */;
  struct {
    S3JniHook busyHandler;
    S3JniHook collationNeeded;
    S3JniHook commit;
    S3JniHook progress;
    S3JniHook rollback;
    S3JniHook trace;
    S3JniHook update;
    S3JniHook auth;
#ifdef SQLITE_ENABLE_PREUPDATE_HOOK
    S3JniHook preUpdate;
#endif
  } hooks;
#ifdef SQLITE_ENABLE_FTS5
  /* FTS5-specific state */
  struct {
    jobject jApi  /* global ref to s3jni_fts5_api_from_db() */;
  } fts;
#endif
  S3JniDb * pNext /* Next entry in SJG.perDb.aFree */;
};

static const char * const S3JniDb_clientdata_key = "S3JniDb";
#define S3JniDb_from_clientdata(pDb)                                \
  (pDb ? sqlite3_get_clientdata(pDb, S3JniDb_clientdata_key) : 0)

/*
** Cache for per-JNIEnv (i.e. per-thread) data.
**
** Trivia: vars and parameters of this type are often named "jc"
** because this class used to have a name for which that abbreviation
** made sense.
*/
typedef struct S3JniEnv S3JniEnv;
struct S3JniEnv {
  JNIEnv *env /* JNIEnv in which this cache entry was created */;
  /*
  ** pdbOpening is used to coordinate the Java/DB connection of a
  ** being-open()'d db in the face of auto-extensions.
  ** Auto-extensions run before we can bind the C db to its Java
  ** representation, but auto-extensions require that binding to pass
  ** on to their Java-side callbacks. We handle this as follows:
  **
  ** - In the JNI side of sqlite3_open(), allocate the Java side of
  **   that connection and set pdbOpening to point to that
  **   object.
  **
  ** - Call sqlite3_open(), which triggers the auto-extension
  **   handler.  That handler uses pdbOpening to connect the native
  **   db handle which it receives with pdbOpening.
  **
  ** - When sqlite3_open() returns, check whether pdbOpening->pDb is
  **   NULL. If it isn't, auto-extension handling set it up.  If it
  **   is, complete the Java/C binding unless sqlite3_open() returns
  **   a NULL db, in which case free pdbOpening.
  */
  S3JniDb * pdbOpening;
  S3JniEnv * pNext /* Next entry in SJG.envCache.aHead or
                      SJG.envCache.aFree */;
};

/*
** State for proxying sqlite3_auto_extension() in Java. This was
** initially a separate class from S3JniHook and now the older name is
** retained for readability in the APIs which use this, as well as for
** its better code-searchability.
*/
typedef S3JniHook S3JniAutoExtension;

/*
** Type IDs for SQL function categories.
*/
enum UDFType {
  UDF_UNKNOWN_TYPE = 0/*for error propagation*/,
  UDF_SCALAR,
  UDF_AGGREGATE,
  UDF_WINDOW
};

/*
** State for binding Java-side UDFs.
*/
typedef struct S3JniUdf S3JniUdf;
struct S3JniUdf {
  jobject jObj           /* SQLFunction instance */;
  char * zFuncName       /* Only for error reporting and debug logging */;
  enum UDFType type      /* UDF type */;
  /** Method IDs for the various UDF methods. */
  jmethodID jmidxFunc    /* xFunc method (scalar) */;
  jmethodID jmidxStep    /* xStep method (aggregate/window) */;
  jmethodID jmidxFinal   /* xFinal method (aggregate/window) */;
  jmethodID jmidxValue   /* xValue method (window) */;
  jmethodID jmidxInverse /* xInverse method (window) */;
  S3JniUdf * pNext       /* Next entry in SJG.udf.aFree. */;
};

#if defined(SQLITE_JNI_ENABLE_METRICS) && 0==SQLITE_JNI_ENABLE_METRICS
#  undef SQLITE_JNI_ENABLE_METRICS
#endif

/*
** If true, modifying S3JniGlobal.metrics is protected by a mutex,
** else it isn't.
*/
#ifdef SQLITE_DEBUG
#  define S3JNI_METRICS_MUTEX SQLITE_THREADSAFE
#else
#  define S3JNI_METRICS_MUTEX 0
#endif
#ifndef SQLITE_JNI_ENABLE_METRICS
#  undef S3JNI_METRICS_MUTEX
#  define S3JNI_METRICS_MUTEX 0
#endif

/*
** Global state, e.g. caches and metrics.
*/
typedef struct S3JniGlobalType S3JniGlobalType;
struct S3JniGlobalType {
  /*
  ** According to: https://developer.ibm.com/articles/j-jni/
  **
  ** > A thread can get a JNIEnv by calling GetEnv() using the JNI
  **   invocation interface through a JavaVM object. The JavaVM object
  **   itself can be obtained by calling the JNI GetJavaVM() method
  **   using a JNIEnv object and can be cached and shared across
  **   threads. Caching a copy of the JavaVM object enables any thread
  **   with access to the cached object to get access to its own
  **   JNIEnv when necessary.
  */
  JavaVM * jvm;
  /*
  ** Global mutex. It must not be used for anything which might call
  ** back into the JNI layer.
  */
  sqlite3_mutex * mutex;
  /*
  ** Cache of references to Java classes and method IDs for
  ** NativePointerHolder subclasses and OutputPointer.T types.
  */
  struct {
    S3JniNphOp list[S3Jni_NphCache_size];
    sqlite3_mutex * mutex;    /* mutex for this->list */
    volatile void const * locker;  /* sanity-checking-only context object
                                      for this->mutex */
  } nph;
  /*
  ** Cache of per-thread state.
  */
  struct {
    S3JniEnv * aHead      /* Linked list of in-use instances */;
    S3JniEnv * aFree      /* Linked list of free instances */;
    sqlite3_mutex * mutex /* mutex for aHead and aFree. */;
    volatile void const * locker  /* env mutex is held on this
                                     object's behalf.  Used only for
                                     sanity checking. */;
  } envCache;
  /*
  ** Per-db state. This can move into the core library once we can tie
  ** client-defined state to db handles there.
  */
  struct {
    S3JniDb * aFree  /* Linked list of free instances */;
    sqlite3_mutex * mutex /* mutex for aHead and aFree */;
    volatile void const * locker
    /* perDb mutex is held on this object's behalf. Used only for
       sanity checking. Note that the mutex is at the class level, not
       instance level. */;
  } perDb;
  struct {
    S3JniUdf * aFree    /* Head of the free-item list. Guarded by global
                           mutex. */;
  } udf;
  /*
  ** Refs to global classes and methods. Obtained during static init
  ** and never released.
  */
  struct {
    jclass cLong             /* global ref to java.lang.Long */;
    jclass cString           /* global ref to java.lang.String */;
    jobject oCharsetUtf8     /* global ref to StandardCharset.UTF_8 */;
    jmethodID ctorLong1      /* the Long(long) constructor */;
    jmethodID ctorStringBA   /* the String(byte[],Charset) constructor */;
    jmethodID stringGetBytes /* the String.getBytes(Charset) method */;

    /*
      ByteBuffer may or may not be supported via JNI on any given
      platform:

      https://docs.oracle.com/javase/8/docs/technotes/guides/jni/spec/functions.html#nio_support

      We only store a ref to byteBuffer.klazz if JNI support for
      ByteBuffer is available (which we determine during static init).
    */
    struct {
      jclass klazz       /* global ref to java.nio.ByteBuffer */;
      jmethodID midAlloc /* ByteBuffer.allocateDirect() */;
      jmethodID midLimit /* ByteBuffer.limit() */;
    } byteBuffer;
  } g;
  /*
  ** The list of Java-side auto-extensions
  ** (org.sqlite.jni.capi.AutoExtensionCallback objects).
  */
  struct {
    S3JniAutoExtension *aExt /* The auto-extension list. It is
                                maintained such that all active
                                entries are in the first contiguous
                                nExt array elements. */;
    int nAlloc               /* number of entries allocated for aExt,
                                as distinct from the number of active
                                entries. */;
    int nExt                 /* number of active entries in aExt, all in the
                                first nExt'th array elements. */;
    sqlite3_mutex * mutex    /* mutex for manipulation/traversal of aExt */;
    volatile const void * locker /* object on whose behalf the mutex
                                    is held.  Only for sanity checking
                                    in debug builds. */;
  } autoExt;
#ifdef SQLITE_ENABLE_FTS5
  struct {
    volatile jobject jExt /* Global ref to Java singleton for the
                             Fts5ExtensionApi instance. */;
    struct {
      jfieldID fidA       /* Fts5Phrase::a member */;
      jfieldID fidB       /* Fts5Phrase::b member */;
    } jPhraseIter;
  } fts5;
#endif
  struct {
#ifdef SQLITE_ENABLE_SQLLOG
    S3JniHook sqllog      /* sqlite3_config(SQLITE_CONFIG_SQLLOG) callback */;
#endif
    S3JniHook configlog   /* sqlite3_config(SQLITE_CONFIG_LOG) callback */;
    S3JniHook * aFree     /* free-item list, for recycling. */;
    sqlite3_mutex * mutex /* mutex for aFree */;
    volatile const void * locker /* object on whose behalf the mutex
                                    is held.  Only for sanity checking
                                    in debug builds. */;
  } hook;
#ifdef SQLITE_JNI_ENABLE_METRICS
  /* Internal metrics. */
  struct {
    volatile unsigned nEnvHit;
    volatile unsigned nEnvMiss;
    volatile unsigned nEnvAlloc;
    volatile unsigned nMutexEnv       /* number of times envCache.mutex was entered for
                                         a S3JniEnv operation. */;
    volatile unsigned nMutexNph       /* number of times SJG.mutex was entered */;
    volatile unsigned nMutexHook      /* number of times SJG.mutex hooks.was entered */;
    volatile unsigned nMutexPerDb     /* number of times perDb.mutex was entered */;
    volatile unsigned nMutexAutoExt   /* number of times autoExt.mutex was entered */;
    volatile unsigned nMutexGlobal    /* number of times global mutex was entered. */;
    volatile unsigned nMutexUdf       /* number of times global mutex was entered
                                         for UDFs. */;
    volatile unsigned nDestroy        /* xDestroy() calls across all types */;
    volatile unsigned nPdbAlloc       /* Number of S3JniDb alloced. */;
    volatile unsigned nPdbRecycled    /* Number of S3JniDb reused. */;
    volatile unsigned nUdfAlloc       /* Number of S3JniUdf alloced. */;
    volatile unsigned nUdfRecycled    /* Number of S3JniUdf reused. */;
    volatile unsigned nHookAlloc      /* Number of S3JniHook alloced. */;
    volatile unsigned nHookRecycled   /* Number of S3JniHook reused. */;
    struct {
      /* Number of calls for each type of UDF callback. */
      volatile unsigned nFunc;
      volatile unsigned nStep;
      volatile unsigned nFinal;
      volatile unsigned nValue;
      volatile unsigned nInverse;
    } udf;
    unsigned nMetrics                 /* Total number of mutex-locked
                                         metrics increments. */;
#if S3JNI_METRICS_MUTEX
    sqlite3_mutex * mutex;
#endif
  } metrics;
#endif /* SQLITE_JNI_ENABLE_METRICS */
};
static S3JniGlobalType S3JniGlobal = {};
#define SJG S3JniGlobal

/* Increments *p, possibly protected by a mutex. */
#ifndef SQLITE_JNI_ENABLE_METRICS
#define s3jni_incr(PTR)
#elif S3JNI_METRICS_MUTEX
static void s3jni_incr( volatile unsigned int * const p ){
  sqlite3_mutex_enter(SJG.metrics.mutex);
  ++SJG.metrics.nMetrics;
  ++(*p);
  sqlite3_mutex_leave(SJG.metrics.mutex);
}
#else
#define s3jni_incr(PTR) ++(*(PTR))
#endif

/* Helpers for working with specific mutexes. */
#if SQLITE_THREADSAFE
#define s3jni_mutex_enter2(M, Metric) \
  sqlite3_mutex_enter( M );           \
  s3jni_incr( &SJG.metrics.Metric )
#define s3jni_mutex_leave2(M) \
  sqlite3_mutex_leave( M )

#define s3jni_mutex_enter(M, L, Metric)                    \
  assert( (void*)env != (void*)L && "Invalid use of " #L); \
  s3jni_mutex_enter2( M, Metric );                         \
  L = env
#define s3jni_mutex_leave(M, L)                            \
  assert( (void*)env == (void*)L && "Invalid use of " #L); \
  L = 0;                                                   \
  s3jni_mutex_leave2( M )

#define S3JniEnv_mutex_assertLocked \
  assert( 0 != SJG.envCache.locker && "Misuse of S3JniGlobal.envCache.mutex" )
#define S3JniEnv_mutex_assertLocker \
  assert( (env) == SJG.envCache.locker && "Misuse of S3JniGlobal.envCache.mutex" )
#define S3JniEnv_mutex_assertNotLocker \
  assert( (env) != SJG.envCache.locker && "Misuse of S3JniGlobal.envCache.mutex" )

#define S3JniEnv_mutex_enter \
  s3jni_mutex_enter( SJG.envCache.mutex, SJG.envCache.locker, nMutexEnv )
#define S3JniEnv_mutex_leave \
  s3jni_mutex_leave( SJG.envCache.mutex, SJG.envCache.locker )

#define S3JniAutoExt_mutex_enter \
  s3jni_mutex_enter( SJG.autoExt.mutex, SJG.autoExt.locker, nMutexAutoExt )
#define S3JniAutoExt_mutex_leave \
  s3jni_mutex_leave( SJG.autoExt.mutex, SJG.autoExt.locker )
#define S3JniAutoExt_mutex_assertLocker                     \
  assert( env == SJG.autoExt.locker && "Misuse of S3JniGlobal.autoExt.mutex" )

#define S3JniGlobal_mutex_enter \
  s3jni_mutex_enter2( SJG.mutex, nMutexGlobal )
#define S3JniGlobal_mutex_leave \
  s3jni_mutex_leave2( SJG.mutex )

#define S3JniHook_mutex_enter \
  s3jni_mutex_enter( SJG.hook.mutex, SJG.hook.locker, nMutexHook )
#define S3JniHook_mutex_leave \
  s3jni_mutex_leave( SJG.hook.mutex, SJG.hook.locker )

#define S3JniNph_mutex_enter \
  s3jni_mutex_enter( SJG.nph.mutex, SJG.nph.locker, nMutexNph )
#define S3JniNph_mutex_leave \
  s3jni_mutex_leave( SJG.nph.mutex, SJG.nph.locker )

#define S3JniDb_mutex_assertLocker \
  assert( (env) == SJG.perDb.locker && "Misuse of S3JniGlobal.perDb.mutex" )
#define S3JniDb_mutex_enter \
  s3jni_mutex_enter( SJG.perDb.mutex, SJG.perDb.locker, nMutexPerDb )
#define S3JniDb_mutex_leave \
  s3jni_mutex_leave( SJG.perDb.mutex, SJG.perDb.locker )

#else /* SQLITE_THREADSAFE==0 */
#define S3JniAutoExt_mutex_assertLocker
#define S3JniAutoExt_mutex_enter
#define S3JniAutoExt_mutex_leave
#define S3JniDb_mutex_assertLocker
#define S3JniDb_mutex_enter
#define S3JniDb_mutex_leave
#define S3JniEnv_mutex_assertLocked
#define S3JniEnv_mutex_assertLocker
#define S3JniEnv_mutex_assertNotLocker
#define S3JniEnv_mutex_enter
#define S3JniEnv_mutex_leave
#define S3JniGlobal_mutex_enter
#define S3JniGlobal_mutex_leave
#define S3JniHook_mutex_enter
#define S3JniHook_mutex_leave
#define S3JniNph_mutex_enter
#define S3JniNph_mutex_leave
#endif

/* Helpers for jstring and jbyteArray. */
static const char * s3jni__jstring_to_mutf8(JNIEnv * const env, jstring v ){
  const char *z = v ? (*env)->GetStringUTFChars(env, v, NULL) : 0;
  s3jni_oom_check( v ? !!z : !z );
  return z;
}

#define s3jni_jstring_to_mutf8(ARG) s3jni__jstring_to_mutf8(env, (ARG))
#define s3jni_mutf8_release(ARG,VAR) if( VAR ) (*env)->ReleaseStringUTFChars(env, ARG, VAR)

/*
** If jBA is not NULL then its GetByteArrayElements() value is
** returned. If jBA is not NULL and nBA is not NULL then *nBA is set
** to the GetArrayLength() of jBA. If GetByteArrayElements() requires
** an allocation and that allocation fails then this function either
** fails fatally or returns 0, depending on build-time options.
 */
static jbyte * s3jni__jbyteArray_bytes2(JNIEnv * const env, jbyteArray jBA, jsize * nBA ){
  jbyte * const rv = jBA ? (*env)->GetByteArrayElements(env, jBA, NULL) : 0;
  s3jni_oom_check( jBA ? !!rv : 1 );
  if( jBA && nBA ) *nBA = (*env)->GetArrayLength(env, jBA);
  return rv;
}

#define s3jni_jbyteArray_bytes2(jByteArray,ptrToSz) \
  s3jni__jbyteArray_bytes2(env, (jByteArray), (ptrToSz))
#define s3jni_jbyteArray_bytes(jByteArray) s3jni__jbyteArray_bytes2(env, (jByteArray), 0)
#define s3jni_jbyteArray_release(jByteArray,jBytes) \
  if( jBytes ) (*env)->ReleaseByteArrayElements(env, jByteArray, jBytes, JNI_ABORT)
#define s3jni_jbyteArray_commit(jByteArray,jBytes) \
  if( jBytes ) (*env)->ReleaseByteArrayElements(env, jByteArray, jBytes, JNI_COMMIT)

/*
** If jbb is-a java.nio.Buffer object and the JNI environment supports
** it, *pBuf is set to the buffer's memory and *pN is set to its
** limit() (as opposed to its capacity()). If jbb is NULL, not a
** Buffer, or the JNI environment does not support that operation,
** *pBuf is set to 0 and *pN is set to 0.
**
** Note that the length of the buffer can be larger than SQLITE_LIMIT
** but this function does not know what byte range of the buffer is
** required so cannot check for that violation. The caller is required
** to ensure that any to-be-bind()ed range fits within SQLITE_LIMIT.
**
** Sidebar: it is unfortunate that we cannot get ByteBuffer.limit()
** via a JNI method like we can for ByteBuffer.capacity(). We instead
** have to call back into Java to get the limit(). Depending on how
** the ByteBuffer is used, the limit and capacity might be the same,
** but when reusing a buffer, the limit may well change whereas the
** capacity is fixed. The problem with, e.g., read()ing blob data to a
** ByteBuffer's memory based on its capacity is that Java-level code
** is restricted to accessing the range specified in
** ByteBuffer.limit().  If we were to honor only the capacity, we
** could end up writing to, or reading from, parts of a ByteBuffer
** which client code itself cannot access without explicitly modifying
** the limit. The penalty we pay for this correctness is that we must
** call into Java to get the limit() of every ByteBuffer we work with.
**
** An alternative to having to call into ByteBuffer.limit() from here
** would be to add private native impls of all ByteBuffer-using
** methods, each of which adds a jint parameter which _must_ be set to
** theBuffer.limit() by public Java APIs which use those private impls
** to do the real work.
*/
static void s3jni__get_nio_buffer(JNIEnv * const env, jobject jbb, void **pBuf, jint * pN ){
  *pBuf = 0;
  *pN = 0;
  if( jbb ){
    *pBuf = (*env)->GetDirectBufferAddress(env, jbb);
    if( *pBuf ){
      /*
      ** Maintenance reminder: do not use
      ** (*env)->GetDirectBufferCapacity(env,jbb), even though it
      ** would be much faster, for reasons explained in this
      ** function's comments.
      */
      *pN = (*env)->CallIntMethod(env, jbb, SJG.g.byteBuffer.midLimit);
      S3JniExceptionIsFatal("Error calling ByteBuffer.limit() method.");
    }
  }
}
#define s3jni_get_nio_buffer(JOBJ,vpOut,jpOut) \
  s3jni__get_nio_buffer(env,(JOBJ),(vpOut),(jpOut))

/*
** Returns the current JNIEnv object. Fails fatally if it cannot find
** the object.
*/
static JNIEnv * s3jni_env(void){
  JNIEnv * env = 0;
  if( (*SJG.jvm)->GetEnv(SJG.jvm, (void **)&env,
                                 JNI_VERSION_1_8) ){
    fprintf(stderr, "Fatal error: cannot get current JNIEnv.\n");
    abort();
  }
  return env;
}

/*
** Fetches the S3JniGlobal.envCache row for the given env, allocing a
** row if needed. When a row is allocated, its state is initialized
** insofar as possible. Calls (*env)->FatalError() if allocation of an
** entry fails. That's hypothetically possible but "shouldn't happen."
*/
static S3JniEnv * S3JniEnv__get(JNIEnv * const env){
  struct S3JniEnv * row;
  S3JniEnv_mutex_enter;
  row = SJG.envCache.aHead;
  for( ; row; row = row->pNext ){
    if( row->env == env ){
      s3jni_incr( &SJG.metrics.nEnvHit );
      S3JniEnv_mutex_leave;
      return row;
    }
  }
  s3jni_incr( &SJG.metrics.nEnvMiss );
  row = SJG.envCache.aFree;
  if( row ){
    SJG.envCache.aFree = row->pNext;
  }else{
    row = s3jni_malloc_or_die(env, sizeof(*row));
    s3jni_incr( &SJG.metrics.nEnvAlloc );
  }
  memset(row, 0, sizeof(*row));
  row->pNext = SJG.envCache.aHead;
  SJG.envCache.aHead = row;
  row->env = env;

  S3JniEnv_mutex_leave;
  return row;
}

#define S3JniEnv_get() S3JniEnv__get(env)

/*
** This function is NOT part of the sqlite3 public API. It is strictly
** for use by the sqlite project's own Java/JNI bindings.
**
** For purposes of certain hand-crafted JNI function bindings, we
** need a way of reporting errors which is consistent with the rest of
** the C API, as opposed to throwing Java exceptions. To that end, this
** internal-use-only function is a thin proxy around
** sqlite3ErrorWithMessage(). The intent is that it only be used from
** JNI bindings such as sqlite3_prepare_v2/v3(), and definitely not
** from client code.
**
** Returns err_code.
*/
static int s3jni_db_error(sqlite3* const db, int err_code,
                          const char * const zMsg){
  if( db!=0 ){
    if( 0==zMsg ){
      sqlite3Error(db, err_code);
    }else{
      const int nMsg = sqlite3Strlen30(zMsg);
      sqlite3_mutex_enter(sqlite3_db_mutex(db));
      sqlite3ErrorWithMsg(db, err_code, "%.*s", nMsg, zMsg);
      sqlite3_mutex_leave(sqlite3_db_mutex(db));
    }
  }
  return err_code;
}

/*
** Creates a new jByteArray of length nP, copies p's contents into it,
** and returns that byte array (NULL on OOM unless fail-fast alloc
** errors are enabled). p may be NULL, in which case the array is
** created but no bytes are filled.
*/
static jbyteArray s3jni__new_jbyteArray(JNIEnv * const env,
                                       const void * const p, int nP){
  jbyteArray jba = (*env)->NewByteArray(env, (jint)nP);

  s3jni_oom_check( jba );
  if( jba && p ){
    (*env)->SetByteArrayRegion(env, jba, 0, (jint)nP, (const jbyte*)p);
  }
  return jba;
}

#define s3jni_new_jbyteArray(P,n) s3jni__new_jbyteArray(env, P, n)


/*
** Uses the java.lang.String(byte[],Charset) constructor to create a
** new String from UTF-8 string z. n is the number of bytes to
** copy. If n<0 then sqlite3Strlen30() is used to calculate it.
**
** Returns NULL if z is NULL or on OOM, else returns a new jstring
** owned by the caller.
**
** Sidebar: this is a painfully inefficient way to convert from
** standard UTF-8 to a Java string, but JNI offers only algorithms for
** working with MUTF-8, not UTF-8.
*/
static jstring s3jni__utf8_to_jstring(JNIEnv * const env,
                                      const char * const z, int n){
  jstring rv = NULL;
  if( 0==n || (n<0 && z && !z[0]) ){
    /* Fast-track the empty-string case via the MUTF-8 API. We could
       hypothetically do this for any strings where n<4 and z is
       NUL-terminated and none of z[0..3] are NUL bytes. */
    rv = (*env)->NewStringUTF(env, "");
    s3jni_oom_check( rv );
  }else if( z ){
    jbyteArray jba;
    if( n<0 ) n = sqlite3Strlen30(z);
    jba = s3jni_new_jbyteArray((unsigned const char *)z, n);
    if( jba ){
      rv = (*env)->NewObject(env, SJG.g.cString, SJG.g.ctorStringBA,
                             jba, SJG.g.oCharsetUtf8);
      S3JniIfThrew{
        S3JniExceptionReport;
        S3JniExceptionClear;
      }
      S3JniUnrefLocal(jba);
    }
    s3jni_oom_check( rv );
  }
  return rv;
}
#define s3jni_utf8_to_jstring(CStr,n) s3jni__utf8_to_jstring(env, CStr, n)

/*
** Converts the given java.lang.String object into a NUL-terminated
** UTF-8 C-string by calling jstr.getBytes(StandardCharset.UTF_8).
** Returns NULL if jstr is NULL or on allocation error. If jstr is not
** NULL and nLen is not NULL then nLen is set to the length of the
** returned string, not including the terminating NUL. If jstr is not
** NULL and it returns NULL, this indicates an allocation error. In
** that case, if nLen is not NULL then it is either set to 0 (if
** fetching of jstr's bytes fails to allocate) or set to what would
** have been the length of the string had C-string allocation
** succeeded.
**
** The returned memory is allocated from sqlite3_malloc() and
** ownership is transferred to the caller.
*/
static char * s3jni__jstring_to_utf8(JNIEnv * const env,
                                    jstring jstr, int *nLen){
  jbyteArray jba;
  jsize nBA;
  char *rv;

  if( !jstr ) return 0;
  jba = (*env)->CallObjectMethod(env, jstr, SJG.g.stringGetBytes,
                                 SJG.g.oCharsetUtf8);

  if( (*env)->ExceptionCheck(env) || !jba
      /* order of these checks is significant for -Xlint:jni */ ) {
    S3JniExceptionReport;
    s3jni_oom_check( jba );
    if( nLen ) *nLen = 0;
    return 0;
  }
  nBA = (*env)->GetArrayLength(env, jba);
  if( nLen ) *nLen = (int)nBA;
  rv = s3jni_malloc( nBA + 1 );
  if( rv ){
    (*env)->GetByteArrayRegion(env, jba, 0, nBA, (jbyte*)rv);
    rv[nBA] = 0;
  }
  S3JniUnrefLocal(jba);
  return rv;
}
#define s3jni_jstring_to_utf8(JStr,n) s3jni__jstring_to_utf8(env, JStr, n)

/*
** Expects to be passed a pointer from sqlite3_column_text16() or
** sqlite3_value_text16() and a byte-length value from
** sqlite3_column_bytes16() or sqlite3_value_bytes16(). It creates a
** Java String of exactly half that character length, returning NULL
** if !p or (*env)->NewString() fails.
*/
static jstring s3jni_text16_to_jstring(JNIEnv * const env, const void * const p, int nP){
  jstring const rv = p
    ? (*env)->NewString(env, (const jchar *)p, (jsize)(nP/2))
    : NULL;
  s3jni_oom_check( p ? !!rv : 1 );
  return rv;
}

/*
** Creates a new ByteBuffer instance with a capacity of n. assert()s
** that SJG.g.byteBuffer.klazz is not 0 and n>0.
*/
static jobject s3jni__new_ByteBuffer(JNIEnv * const env, int n){
  jobject rv = 0;
  assert( SJG.g.byteBuffer.klazz );
  assert( SJG.g.byteBuffer.midAlloc );
  assert( n > 0 );
  rv = (*env)->CallStaticObjectMethod(env, SJG.g.byteBuffer.klazz,
                                      SJG.g.byteBuffer.midAlloc, (jint)n);
  S3JniIfThrew {
    S3JniExceptionReport;
    S3JniExceptionClear;
  }
  s3jni_oom_check( rv );
  return rv;
}

/*
** If n>0 and sqlite3_jni_supports_nio() is true then this creates a
** new ByteBuffer object and copies n bytes from p to it. Returns NULL
** if n is 0, sqlite3_jni_supports_nio() is false, or on allocation
** error (unless fatal alloc failures are enabled).
*/
static jobject s3jni__blob_to_ByteBuffer(JNIEnv * const env,
                                         const void * p, int n){
  jobject rv = NULL;
  assert( n >= 0 );
  if( 0==n || !SJG.g.byteBuffer.klazz ){
    return NULL;
  }
  rv = s3jni__new_ByteBuffer(env, n);
  if( rv ){
    void * tgt = (*env)->GetDirectBufferAddress(env, rv);
    memcpy(tgt, p, (size_t)n);
  }
  return rv;
}


/*
** Requires jx to be a Throwable. Calls its toString() method and
** returns its value converted to a UTF-8 string. The caller owns the
** returned string and must eventually sqlite3_free() it.  Returns 0
** if there is a problem fetching the info or on OOM.
**
** Design note: we use toString() instead of getMessage() because the
** former includes the exception type's name:
**
**  Exception e = new RuntimeException("Hi");
**  System.out.println(e.toString()); // java.lang.RuntimeException: Hi
**  System.out.println(e.getMessage()); // Hi
*/
static char * s3jni_exception_error_msg(JNIEnv * const env, jthrowable jx){
  jmethodID mid;
  jstring msg;
  char * zMsg;
  jclass const klazz = (*env)->GetObjectClass(env, jx);
  mid = (*env)->GetMethodID(env, klazz, "toString", "()Ljava/lang/String;");
  S3JniUnrefLocal(klazz);
  S3JniIfThrew{
    S3JniExceptionReport;
    S3JniExceptionClear;
    return 0;
  }
  msg = (*env)->CallObjectMethod(env, jx, mid);
  S3JniIfThrew{
    S3JniExceptionReport;
    S3JniExceptionClear;
    return 0;
  }
  zMsg = s3jni_jstring_to_utf8( msg, 0);
  S3JniUnrefLocal(msg);
  return zMsg;
}

/*
** Extracts env's current exception, sets ps->pDb's error message to
** its message string, and clears the exception. If errCode is non-0,
** it is used as-is, else SQLITE_ERROR is assumed. If there's a
** problem extracting the exception's message, it's treated as
** non-fatal and zDfltMsg is used in its place.
**
** Locks the global S3JniDb mutex.
**
** This must only be called if a JNI exception is pending.
**
** Returns errCode unless it is 0, in which case SQLITE_ERROR is
** returned.
*/
static int s3jni__db_exception(JNIEnv * const env, sqlite3 * const pDb,
                              int errCode, const char *zDfltMsg){
  jthrowable const ex = (*env)->ExceptionOccurred(env);

  if( 0==errCode ) errCode = SQLITE_ERROR;
  if( ex ){
    char * zMsg;
    S3JniExceptionClear;
    zMsg = s3jni_exception_error_msg(env, ex);
    s3jni_db_error(pDb, errCode, zMsg ? zMsg : zDfltMsg);
    sqlite3_free(zMsg);
    S3JniUnrefLocal(ex);
  }else if( zDfltMsg ){
    s3jni_db_error(pDb, errCode, zDfltMsg);
  }
  return errCode;
}
#define s3jni_db_exception(pDb,ERRCODE,DFLTMSG) \
  s3jni__db_exception(env, (pDb), (ERRCODE), (DFLTMSG) )

/*
** Extracts the (void xDestroy()) method from jObj and applies it to
** jObj. If jObj is NULL, this is a no-op. The lack of an xDestroy()
** method is silently ignored. Any exceptions thrown by xDestroy()
** trigger a warning to stdout or stderr and then the exception is
** suppressed.
*/
static void s3jni__call_xDestroy(JNIEnv * const env, jobject jObj){
  if( jObj ){
    jclass const klazz = (*env)->GetObjectClass(env, jObj);
    jmethodID method = (*env)->GetMethodID(env, klazz, "xDestroy", "()V");

    S3JniUnrefLocal(klazz);
    if( method ){
      s3jni_incr( &SJG.metrics.nDestroy );
      (*env)->CallVoidMethod(env, jObj, method);
      S3JniIfThrew{
        S3JniExceptionWarnCallbackThrew("xDestroy() callback");
        S3JniExceptionClear;
      }
    }else{
      /* Non-fatal. */
      S3JniExceptionClear;
    }
  }
}
#define s3jni_call_xDestroy(JOBJ) s3jni__call_xDestroy(env, (JOBJ))

/*
** Internal helper for many hook callback impls. Locks the S3JniDb
** mutex, makes a copy of src into dest, with a some differences: (1)
** if src->jObj or src->jExtra are not NULL then dest will be a new
** LOCAL ref to it instead of a copy of the prior GLOBAL ref. (2)
** dest->doXDestroy is always false.
**
** If dest->jObj is not NULL when this returns then the caller is
** obligated to eventually free the new ref by passing *dest to
** S3JniHook_localundup(). The dest pointer must NOT be passed to
** S3JniHook_unref(), as that routine assumes that dest->jObj/jExtra
** are GLOBAL refs (it's illegal to try to unref the wrong ref type).
**
** Background: when running a hook we need a call-local copy lest
** another thread modify the hook while we're running it. That copy
** has to have its own Java reference, but it need only be call-local.
*/
static void S3JniHook__localdup( JNIEnv * const env, S3JniHook const * const src,
                                 S3JniHook * const dest ){
  S3JniHook_mutex_enter;
  *dest = *src;
  if(src->jObj) dest->jObj = S3JniRefLocal(src->jObj);
  if(src->jExtra) dest->jExtra = S3JniRefLocal(src->jExtra);
  dest->doXDestroy = 0;
  S3JniHook_mutex_leave;
}
#define S3JniHook_localdup(src,dest) S3JniHook__localdup(env,src,dest)

static void S3JniHook__localundup( JNIEnv * const env, S3JniHook * const h  ){
  S3JniUnrefLocal(h->jObj);
  S3JniUnrefLocal(h->jExtra);
  *h = S3JniHook_empty;
}
#define S3JniHook_localundup(HOOK) S3JniHook__localundup(env, &(HOOK))

/*
** Removes any Java references from s and clears its state. If
** doXDestroy is true and s->jObj is not NULL, s->jObj
** is passed to s3jni_call_xDestroy() before any references are
** cleared. It is legal to call this when the object has no Java
** references. s must not be NULL.
*/
static void S3JniHook__unref(JNIEnv * const env, S3JniHook * const s){
  if( s->jObj ){
    if( s->doXDestroy ){
      s3jni_call_xDestroy(s->jObj);
    }
    S3JniUnrefGlobal(s->jObj);
    S3JniUnrefGlobal(s->jExtra);
  }else{
    assert( !s->jExtra );
  }
  *s = S3JniHook_empty;
}
#define S3JniHook_unref(hook) S3JniHook__unref(env, (hook))

/*
** Allocates one blank S3JniHook object from the recycling bin, if
** available, else from the heap. Returns NULL or dies on OOM,
** depending on build options.  Locks on SJG.hooks.mutex.
*/
static S3JniHook *S3JniHook__alloc(JNIEnv  * const env){
  S3JniHook * p = 0;
  S3JniHook_mutex_enter;
  if( SJG.hook.aFree ){
    p = SJG.hook.aFree;
    SJG.hook.aFree = p->pNext;
    p->pNext = 0;
    s3jni_incr(&SJG.metrics.nHookRecycled);
  }
  S3JniHook_mutex_leave;
  if( 0==p ){
    p = s3jni_malloc(sizeof(S3JniHook));
    if( p ){
      s3jni_incr(&SJG.metrics.nHookAlloc);
    }
  }
  if( p ){
    *p = S3JniHook_empty;
  }
  return p;
}
#define S3JniHook_alloc() S3JniHook__alloc(env)

/*
** The rightful fate of all results from S3JniHook_alloc(). Locks on
** SJG.hook.mutex.
*/
static void S3JniHook__free(JNIEnv  * const env, S3JniHook * const p){
  if(p){
    assert( !p->pNext );
    S3JniHook_unref(p);
    S3JniHook_mutex_enter;
    p->pNext = SJG.hook.aFree;
    SJG.hook.aFree = p;
    S3JniHook_mutex_leave;
  }
}
#define S3JniHook_free(hook) S3JniHook__free(env, hook)

#if 0
/* S3JniHook__free() without the lock: caller must hold the global mutex */
static void S3JniHook__free_unlocked(JNIEnv  * const env, S3JniHook * const p){
  if(p){
    assert( !p->pNext );
    assert( p->pNext != SJG.hook.aFree );
    S3JniHook_unref(p);
    p->pNext = SJG.hook.aFree;
    SJG.hook.aFree = p;
  }
}
#define S3JniHook_free_unlocked(hook) S3JniHook__free_unlocked(env, hook)
#endif

/*
** Clears all of s's state. Requires that that the caller has locked
** S3JniGlobal.perDb.mutex. Make sure to do anything needed with
** s->pNext and s->pPrev before calling this, as this clears them.
*/
static void S3JniDb_clear(JNIEnv * const env, S3JniDb * const s){
  S3JniDb_mutex_assertLocker;
  sqlite3_free( s->zMainDbName );
#define UNHOOK(MEMBER) \
  S3JniHook_unref(&s->hooks.MEMBER)
  UNHOOK(auth);
  UNHOOK(busyHandler);
  UNHOOK(collationNeeded);
  UNHOOK(commit);
  UNHOOK(progress);
  UNHOOK(rollback);
  UNHOOK(trace);
  UNHOOK(update);
#ifdef SQLITE_ENABLE_PREUPDATE_HOOK
  UNHOOK(preUpdate);
#endif
#undef UNHOOK
  S3JniUnrefGlobal(s->jDb);
  memset(s, 0, sizeof(S3JniDb));
}

/*
** Clears s's state and moves it to the free-list. Requires that
** S3JniGlobal.perDb.mutex is locked.
*/
static void S3JniDb__set_aside_unlocked(JNIEnv * const env, S3JniDb * const s){
  assert( s );
  S3JniDb_mutex_assertLocker;
  if( s ){
    S3JniDb_clear(env, s);
    s->pNext = SJG.perDb.aFree;
    SJG.perDb.aFree = s;
  }
}
#define S3JniDb_set_aside_unlocked(JniDb) S3JniDb__set_aside_unlocked(env, JniDb)

static void S3JniDb__set_aside(JNIEnv * const env, S3JniDb * const s){
  S3JniDb_mutex_enter;
  S3JniDb_set_aside_unlocked(s);
  S3JniDb_mutex_leave;
}
#define S3JniDb_set_aside(JNIDB) S3JniDb__set_aside(env, JNIDB)

/*
** Uncache any state for the given JNIEnv, clearing all Java
** references the cache owns. Returns true if env was cached and false
** if it was not found in the cache. Ownership of the S3JniEnv object
** associated with the given argument is transferred to this function,
** which makes it free for re-use.
**
** Requires that the env mutex be locked.
*/
static int S3JniEnv_uncache(JNIEnv * const env){
  struct S3JniEnv * row;
  struct S3JniEnv * pPrev = 0;

  S3JniEnv_mutex_assertLocked;
  row = SJG.envCache.aHead;
  for( ; row; pPrev = row, row = row->pNext ){
    if( row->env == env ){
      break;
    }
  }
  if( !row ){
    return 0;
  }
  if( pPrev) pPrev->pNext = row->pNext;
  else{
    assert( SJG.envCache.aHead == row );
    SJG.envCache.aHead = row->pNext;
  }
  memset(row, 0, sizeof(S3JniEnv));
  row->pNext = SJG.envCache.aFree;
  SJG.envCache.aFree = row;
  return 1;
}

/*
** Fetches the given nph-ref from cache the cache and returns the
** object with its klazz member set. This is an O(1) operation except
** on the first call for a given pRef, during which pRef->klazz and
** pRef->pRef are initialized thread-safely. In the latter case it's
** still effectively O(1), but with a much longer 1.
**
** It is up to the caller to populate the other members of the
** returned object if needed, taking care to lock the modification
** with S3JniNph_mutex_enter/leave.
**
** This simple cache catches >99% of searches in the current
** (2023-07-31) tests.
*/
static S3JniNphOp * s3jni__nphop(JNIEnv * const env, S3JniNphOp const* pRef){
  S3JniNphOp * const pNC = &SJG.nph.list[pRef->index];

  assert( (void*)pRef>=(void*)&S3JniNphOps && (void*)pRef<(void*)(&S3JniNphOps + 1)
          && "pRef is out of range" );
  assert( pRef->index>=0
          && (pRef->index < (sizeof(S3JniNphOps) / sizeof(S3JniNphOp)))
          && "pRef->index is out of range" );
  if( !pNC->klazz ){
    S3JniNph_mutex_enter;
    if( !pNC->klazz ){
      jclass const klazz = (*env)->FindClass(env, pRef->zName);
      //printf("FindClass %s\n", pRef->zName);
      S3JniExceptionIsFatal("FindClass() unexpectedly threw");
      pNC->klazz = S3JniRefGlobal(klazz);
    }
    S3JniNph_mutex_leave;
  }
  assert( pNC->klazz );
  return pNC;
}

#define s3jni_nphop(PRef) s3jni__nphop(env, PRef)

/*
** Common code for accessor functions for NativePointerHolder and
** OutputPointer types. pRef must be a pointer from S3JniNphOps. jOut
** must be an instance of that class (Java's type safety takes care of
** that requirement). If necessary, this fetches the jfieldID for
** jOut's pRef->zMember, which must be of the type represented by the
** JNI type signature pRef->zTypeSig, and stores it in
** S3JniGlobal.nph.list[pRef->index].  Fails fatally if the pRef->zMember
** property is not found, as that presents a serious internal misuse.
**
** Property lookups are cached on a per-pRef basis.
*/
static jfieldID s3jni_nphop_field(JNIEnv * const env, S3JniNphOp const* pRef){
  S3JniNphOp * const pNC = s3jni_nphop(pRef);

  if( !pNC->fidValue ){
    S3JniNph_mutex_enter;
    if( !pNC->fidValue ){
      pNC->fidValue = (*env)->GetFieldID(env, pNC->klazz,
                                         pRef->zMember, pRef->zTypeSig);
      S3JniExceptionIsFatal("Code maintenance required: missing "
                            "required S3JniNphOp::fidValue.");
    }
    S3JniNph_mutex_leave;
  }
  assert( pNC->fidValue );
  return pNC->fidValue;
}

/*
** Sets a native ptr value in NativePointerHolder object jNph,
** which must be of the native type described by pRef. jNph
** may not be NULL.
*/
static void NativePointerHolder__set(JNIEnv * const env, S3JniNphOp const* pRef,
                                     jobject jNph, const void * p){
  assert( jNph );
  (*env)->SetLongField(env, jNph, s3jni_nphop_field(env, pRef),
                       S3JniCast_P2L(p));
  S3JniExceptionIsFatal("Could not set NativePointerHolder.nativePointer.");
}

#define NativePointerHolder_set(PREF,JNPH,P) \
  NativePointerHolder__set(env, PREF, JNPH, P)

/*
** Fetches a native ptr value from NativePointerHolder object jNph,
** which must be of the native type described by pRef.  This is a
** no-op if jNph is NULL.
*/
static void * NativePointerHolder__get(JNIEnv * env, jobject jNph,
                                       S3JniNphOp const* pRef){
  void * rv = 0;
  if( jNph ){
    rv = S3JniCast_L2P(
      (*env)->GetLongField(env, jNph, s3jni_nphop_field(env, pRef))
    );
    S3JniExceptionIsFatal("Cannot fetch NativePointerHolder.nativePointer.");
  }
  return rv;
}

#define NativePointerHolder_get(JOBJ,NPHREF) \
  NativePointerHolder__get(env, (JOBJ), (NPHREF))

/*
** Helpers for extracting pointers from jobjects, noting that we rely
** on the corresponding Java interfaces having already done the
** type-checking. OBJ must be a jobject referring to a
** NativePointerHolder<T>, where T matches PtrGet_T. Don't use these
** in contexts where that's not the case. Note that these aren't
** type-safe in the strictest sense:
**
**   sqlite3 * s = PtrGet_sqlite3_stmt(...)
**
** will work, despite the incorrect macro name, so long as the
** argument is a Java sqlite3 object, as this operation only has void
** pointers to work with.
*/
#define PtrGet_T(T,JOBJ) (T*)NativePointerHolder_get((JOBJ), S3JniNph(T))
#define PtrGet_sqlite3(JOBJ) PtrGet_T(sqlite3, (JOBJ))
#define PtrGet_sqlite3_backup(JOBJ) PtrGet_T(sqlite3_backup, (JOBJ))
#define PtrGet_sqlite3_blob(JOBJ) PtrGet_T(sqlite3_blob, (JOBJ))
#define PtrGet_sqlite3_context(JOBJ) PtrGet_T(sqlite3_context, (JOBJ))
#define PtrGet_sqlite3_stmt(JOBJ) PtrGet_T(sqlite3_stmt, (JOBJ))
#define PtrGet_sqlite3_value(JOBJ) PtrGet_T(sqlite3_value, (JOBJ))
/*
** LongPtrGet_T(X,Y) expects X to be an unqualified sqlite3 struct
** type name and Y to be a native pointer to such an object in the
** form of a jlong value. The jlong is simply cast to (X*). This
** approach is, as of 2023-09-27, supplanting the former approach. We
** now do the native pointer extraction in the Java side, rather than
** the C side, because it's reportedly significantly faster. The
** intptr_t part here is necessary for compatibility with (at least)
** ARM32.
**
** 2023-11-09: testing has not revealed any measurable performance
** difference between the approach of passing type T to C compared to
** passing pointer-to-T to C, and adding support for the latter
** everywhere requires sigificantly more code. As of this writing, the
** older/simpler approach is being applied except for (A) where the
** newer approach has already been applied and (B) hot-spot APIs where
** a difference of microseconds (i.e. below our testing measurement
** threshold) might add up.
*/
#define LongPtrGet_T(T,JLongAsPtr) (T*)((intptr_t)((JLongAsPtr)))
#define LongPtrGet_sqlite3(JLongAsPtr) LongPtrGet_T(sqlite3,(JLongAsPtr))
#define LongPtrGet_sqlite3_backup(JLongAsPtr) LongPtrGet_T(sqlite3_backup,(JLongAsPtr))
#define LongPtrGet_sqlite3_blob(JLongAsPtr) LongPtrGet_T(sqlite3_blob,(JLongAsPtr))
#define LongPtrGet_sqlite3_stmt(JLongAsPtr) LongPtrGet_T(sqlite3_stmt,(JLongAsPtr))
#define LongPtrGet_sqlite3_value(JLongAsPtr) LongPtrGet_T(sqlite3_value,(JLongAsPtr))
/*
** Extracts the new S3JniDb instance from the free-list, or allocates
** one if needed, associates it with pDb, and returns.  Returns NULL
** on OOM. The returned object MUST, on success of the calling
** operation, subsequently be associated with jDb via
** NativePointerHolder_set() or freed using S3JniDb_set_aside().
*/
static S3JniDb * S3JniDb_alloc(JNIEnv * const env, jobject jDb){
  S3JniDb * rv = 0;
  S3JniDb_mutex_enter;
  if( SJG.perDb.aFree ){
    rv = SJG.perDb.aFree;
    SJG.perDb.aFree = rv->pNext;
    rv->pNext = 0;
    s3jni_incr( &SJG.metrics.nPdbRecycled );
  }
  S3JniDb_mutex_leave;
  if( 0==rv ){
    rv = s3jni_malloc(sizeof(S3JniDb));
    if( rv ){
      s3jni_incr( &SJG.metrics.nPdbAlloc );
    }
  }
  if( rv ){
    memset(rv, 0, sizeof(S3JniDb));
    rv->jDb = S3JniRefGlobal(jDb);
  }
  return rv;
}

/*
** Returns the S3JniDb object for the given org.sqlite.jni.capi.sqlite3
** object, or NULL if jDb is NULL, no pointer can be extracted
** from it, or no matching entry can be found.
*/
static S3JniDb * S3JniDb__from_java(JNIEnv * const env, jobject jDb){
  sqlite3 * const pDb = jDb ? PtrGet_sqlite3(jDb) : 0;
  return pDb ? S3JniDb_from_clientdata(pDb) : 0;
}
#define S3JniDb_from_java(jObject) S3JniDb__from_java(env,(jObject))

/*
** S3JniDb finalizer for use with sqlite3_set_clientdata().
*/
static void S3JniDb_xDestroy(void *p){
  S3JniDeclLocal_env;
  S3JniDb * const ps = p;
  assert( !ps->pNext && "Else ps is already in the free-list.");
  S3JniDb_set_aside(ps);
}

/*
** Evaluates to the S3JniDb object for the given sqlite3 object, or
** NULL if pDb is NULL or was not initialized via the JNI interfaces.
*/
#define S3JniDb_from_c(sqlite3Ptr) \
  ((sqlite3Ptr) ? S3JniDb_from_clientdata(sqlite3Ptr) : 0)
#define S3JniDb_from_jlong(sqlite3PtrAsLong) \
  S3JniDb_from_c(LongPtrGet_T(sqlite3,sqlite3PtrAsLong))

/*
** Unref any Java-side state in (S3JniAutoExtension*) AX and zero out
** AX.
*/
#define S3JniAutoExtension_clear(AX) S3JniHook_unref(AX);

/*
** Initializes a pre-allocated S3JniAutoExtension object.  Returns
** non-0 if there is an error collecting the required state from
** jAutoExt (which must be an AutoExtensionCallback object). On error,
** it passes ax to S3JniAutoExtension_clear().
*/
static int S3JniAutoExtension_init(JNIEnv *const env,
                                   S3JniAutoExtension * const ax,
                                   jobject const jAutoExt){
  jclass const klazz = (*env)->GetObjectClass(env, jAutoExt);

  S3JniAutoExt_mutex_assertLocker;
  *ax = S3JniHook_empty;
  ax->midCallback = (*env)->GetMethodID(env, klazz, "call",
                                        "(Lorg/sqlite/jni/capi/sqlite3;)I");
  S3JniUnrefLocal(klazz);
  S3JniExceptionWarnIgnore;
  if( !ax->midCallback ){
    S3JniAutoExtension_clear(ax);
    return SQLITE_ERROR;
  }
  ax->jObj = S3JniRefGlobal(jAutoExt);
  return 0;
}

/*
** Sets the value property of the OutputPointer.Bool jOut object to
** v.
*/
static void OutputPointer_set_Bool(JNIEnv * const env, jobject const jOut,
                                    int v){
  (*env)->SetBooleanField(env, jOut, s3jni_nphop_field(
                            env, S3JniNph(OutputPointer_Bool)
                          ), v ? JNI_TRUE : JNI_FALSE );
  S3JniExceptionIsFatal("Cannot set OutputPointer.Bool.value");
}

/*
** Sets the value property of the OutputPointer.Int32 jOut object to
** v.
*/
static void OutputPointer_set_Int32(JNIEnv * const env, jobject const jOut,
                                    int v){
  (*env)->SetIntField(env, jOut, s3jni_nphop_field(
                        env, S3JniNph(OutputPointer_Int32)
                      ), (jint)v);
  S3JniExceptionIsFatal("Cannot set OutputPointer.Int32.value");
}

/*
** Sets the value property of the OutputPointer.Int64 jOut object to
** v.
*/
static void OutputPointer_set_Int64(JNIEnv * const env, jobject const jOut,
                                    jlong v){
  (*env)->SetLongField(env, jOut, s3jni_nphop_field(
                         env, S3JniNph(OutputPointer_Int64)
                       ), v);
  S3JniExceptionIsFatal("Cannot set OutputPointer.Int64.value");
}

/*
** Internal helper for OutputPointer_set_TYPE() where TYPE is an
** Object type.
*/
static void OutputPointer_set_obj(JNIEnv * const env,
                                  S3JniNphOp const * const pRef,
                                  jobject const jOut,
                                  jobject v){
  (*env)->SetObjectField(env, jOut, s3jni_nphop_field(env, pRef), v);
  S3JniExceptionIsFatal("Cannot set OutputPointer.T.value");
}

#ifdef SQLITE_ENABLE_FTS5
#if 0
/*
** Sets the value property of the OutputPointer.ByteArray jOut object
** to v.
*/
static void OutputPointer_set_ByteArray(JNIEnv * const env, jobject const jOut,
                                        jbyteArray const v){
  OutputPointer_set_obj(env, S3JniNph(OutputPointer_ByteArray), jOut, v);
}
#endif
#endif /* SQLITE_ENABLE_FTS5 */

/*
** Sets the value property of the OutputPointer.String jOut object to
** v.
*/
static void OutputPointer_set_String(JNIEnv * const env, jobject const jOut,
                                     jstring const v){
  OutputPointer_set_obj(env, S3JniNph(OutputPointer_String), jOut, v);
}

/*
** Returns true if eTextRep is a valid sqlite3 encoding constant, else
** returns false.
*/
static int encodingTypeIsValid(int eTextRep){
  switch( eTextRep ){
    case SQLITE_UTF8: case SQLITE_UTF16:
    case SQLITE_UTF16LE: case SQLITE_UTF16BE:
      return 1;
    default:
      return 0;
  }
}

/* For use with sqlite3_result_pointer(), sqlite3_value_pointer(),
   sqlite3_bind_java_object(), and sqlite3_column_java_object(). */
static const char * const s3jni__value_jref_key = "org.sqlite.jni.capi.ResultJavaVal";

/*
** If v is not NULL, it must be a jobject global reference. Its
** reference is relinquished.
*/
static void S3Jni_jobject_finalizer(void *v){
  if( v ){
    S3JniDeclLocal_env;
    S3JniUnrefGlobal((jobject)v);
  }
}

/*
** Returns a new Java instance of the class referred to by pRef, which
** MUST be interface-compatible with NativePointerHolder and MUST have
** a no-arg constructor. The NativePointerHolder_set() method is
** passed the new Java object (which must not be NULL) and pNative
** (which may be NULL). Hypothetically returns NULL if Java fails to
** allocate, but the JNI docs are not entirely clear on that detail.
**
** Always use a static pointer from the S3JniNphOps struct for the
** 2nd argument.
*/
static jobject NativePointerHolder_new(JNIEnv * const env,
                                       S3JniNphOp const * pRef,
                                       const void * pNative){
  jobject rv = 0;
  S3JniNphOp * const pNC = s3jni_nphop(pRef);
  if( !pNC->midCtor ){
    S3JniNph_mutex_enter;
    if( !pNC->midCtor ){
      pNC->midCtor = (*env)->GetMethodID(env, pNC->klazz, "<init>", "()V");
      S3JniExceptionIsFatal("Cannot find constructor for class.");
    }
    S3JniNph_mutex_leave;
  }
  rv = (*env)->NewObject(env, pNC->klazz, pNC->midCtor);
  S3JniExceptionIsFatal("No-arg constructor threw.");
  s3jni_oom_check(rv);
  if( rv ) NativePointerHolder_set(pRef, rv, pNative);
  return rv;
}

static inline jobject new_java_sqlite3(JNIEnv * const env, sqlite3 *sv){
  return NativePointerHolder_new(env, S3JniNph(sqlite3), sv);
}
static inline jobject new_java_sqlite3_backup(JNIEnv * const env, sqlite3_backup *sv){
  return NativePointerHolder_new(env, S3JniNph(sqlite3_backup), sv);
}
static inline jobject new_java_sqlite3_blob(JNIEnv * const env, sqlite3_blob *sv){
  return NativePointerHolder_new(env, S3JniNph(sqlite3_blob), sv);
}
static inline jobject new_java_sqlite3_context(JNIEnv * const env, sqlite3_context *sv){
  return NativePointerHolder_new(env, S3JniNph(sqlite3_context), sv);
}
static inline jobject new_java_sqlite3_stmt(JNIEnv * const env, sqlite3_stmt *sv){
  return NativePointerHolder_new(env, S3JniNph(sqlite3_stmt), sv);
}
static inline jobject new_java_sqlite3_value(JNIEnv * const env, sqlite3_value *sv){
  return NativePointerHolder_new(env, S3JniNph(sqlite3_value), sv);
}

/* Helper typedefs for UDF callback types. */
typedef void (*udf_xFunc_f)(sqlite3_context*,int,sqlite3_value**);
typedef void (*udf_xStep_f)(sqlite3_context*,int,sqlite3_value**);
typedef void (*udf_xFinal_f)(sqlite3_context*);
/*typedef void (*udf_xValue_f)(sqlite3_context*);*/
/*typedef void (*udf_xInverse_f)(sqlite3_context*,int,sqlite3_value**);*/

/*
** Allocate a new S3JniUdf (User-defined Function) and associate it
** with the SQLFunction-type jObj. Returns NULL on OOM. If the
** returned object's type==UDF_UNKNOWN_TYPE then the type of UDF was
** not unambiguously detected based on which callback members it has,
** which falls into the category of user error.
**
** The caller must arrange for the returned object to eventually be
** passed to S3JniUdf_free().
*/
static S3JniUdf * S3JniUdf_alloc(JNIEnv * const env, jobject jObj){
  S3JniUdf * s = 0;

  S3JniGlobal_mutex_enter;
  s3jni_incr(&SJG.metrics.nMutexUdf);
  if( SJG.udf.aFree ){
    s = SJG.udf.aFree;
    SJG.udf.aFree = s->pNext;
    s->pNext = 0;
    s3jni_incr(&SJG.metrics.nUdfRecycled);
  }
  S3JniGlobal_mutex_leave;
  if( !s ){
    s = s3jni_malloc( sizeof(*s));
    s3jni_incr(&SJG.metrics.nUdfAlloc);
  }
  if( s ){
    const char * zFSI = /* signature for xFunc, xStep, xInverse */
      "(Lorg/sqlite/jni/capi/sqlite3_context;[Lorg/sqlite/jni/capi/sqlite3_value;)V";
    const char * zFV = /* signature for xFinal, xValue */
      "(Lorg/sqlite/jni/capi/sqlite3_context;)V";
    jclass const klazz = (*env)->GetObjectClass(env, jObj);

    memset(s, 0, sizeof(*s));
    s->jObj = S3JniRefGlobal(jObj);

#define FGET(FuncName,FuncSig,Field)                               \
    s->Field = (*env)->GetMethodID(env, klazz, FuncName, FuncSig); \
    if( !s->Field ) (*env)->ExceptionClear(env)

    FGET("xFunc",    zFSI, jmidxFunc);
    FGET("xStep",    zFSI, jmidxStep);
    FGET("xFinal",   zFV,  jmidxFinal);
    FGET("xValue",   zFV,  jmidxValue);
    FGET("xInverse", zFSI, jmidxInverse);
#undef FGET

    S3JniUnrefLocal(klazz);
    if( s->jmidxFunc ) s->type = UDF_SCALAR;
    else if( s->jmidxStep && s->jmidxFinal ){
      s->type = (s->jmidxValue && s->jmidxInverse)
        ? UDF_WINDOW : UDF_AGGREGATE;
    }else{
      s->type = UDF_UNKNOWN_TYPE;
    }
  }
  return s;
}

/*
** Frees up all resources owned by s, clears its state, then either
** caches it for reuse (if cacheIt is true) or frees it. The former
** requires locking the global mutex, so it must not be held when this
** is called.
*/
static void S3JniUdf_free(JNIEnv * const env, S3JniUdf * const s,
                          int cacheIt){
  assert( !s->pNext );
  if( s->jObj ){
    s3jni_call_xDestroy(s->jObj);
    S3JniUnrefGlobal(s->jObj);
    sqlite3_free(s->zFuncName);
    assert( !s->pNext );
    memset(s, 0, sizeof(*s));
  }
  if( cacheIt ){
    S3JniGlobal_mutex_enter;
    s->pNext = S3JniGlobal.udf.aFree;
    S3JniGlobal.udf.aFree = s;
    S3JniGlobal_mutex_leave;
  }else{
    sqlite3_free( s );
  }
}

/* Finalizer for sqlite3_create_function() and friends. */
static void S3JniUdf_finalizer(void * s){
  S3JniUdf_free(s3jni_env(), (S3JniUdf*)s, 1);
}

/*
** Helper for processing args to UDF handlers with signature
** (sqlite3_context*,int,sqlite3_value**).
*/
typedef struct {
  jobject jcx         /* sqlite3_context */;
  jobjectArray jargv  /* sqlite3_value[] */;
} udf_jargs;

/*
** Converts the given (cx, argc, argv) into arguments for the given
** UDF, writing the result (Java wrappers for cx and argv) in the
** final 2 arguments. Returns 0 on success, SQLITE_NOMEM on allocation
** error. On error *jCx and *jArgv will be set to 0. The output
** objects are of type org.sqlite.jni.capi.sqlite3_context and
** array-of-org.sqlite.jni.capi.sqlite3_value, respectively.
*/
static int udf_args(JNIEnv *env,
                    sqlite3_context * const cx,
                    int argc, sqlite3_value**argv,
                    jobject * jCx, jobjectArray *jArgv){
  jobjectArray ja = 0;
  jobject jcx = new_java_sqlite3_context(env, cx);
  jint i;
  *jCx = 0;
  *jArgv = 0;
  if( !jcx ) goto error_oom;
  ja = (*env)->NewObjectArray(
    env, argc, s3jni_nphop(S3JniNph(sqlite3_value))->klazz,
    NULL);
  s3jni_oom_check( ja );
  if( !ja ) goto error_oom;
  for(i = 0; i < argc; ++i){
    jobject jsv = new_java_sqlite3_value(env, argv[i]);
    if( !jsv ) goto error_oom;
    (*env)->SetObjectArrayElement(env, ja, i, jsv);
    S3JniUnrefLocal(jsv)/*ja has a ref*/;
  }
  *jCx = jcx;
  *jArgv = ja;
  return 0;
error_oom:
  S3JniUnrefLocal(jcx);
  S3JniUnrefLocal(ja);
  return SQLITE_NOMEM;
}

/*
** Requires that jCx and jArgv are sqlite3_context
** resp. array-of-sqlite3_value values initialized by udf_args(). The
** latter will be 0-and-NULL for UDF types with no arguments. This
** function zeroes out the nativePointer member of jCx and each entry
** in jArgv. This is a safety-net precaution to avoid undefined
** behavior if a Java-side UDF holds a reference to its context or one
** of its arguments. This MUST be called from any function which
** successfully calls udf_args(), after calling the corresponding UDF
** and checking its exception status, or which Java-wraps a
** sqlite3_context for use with a UDF(ish) call. It MUST NOT be called
** in any other case.
*/
static void udf_unargs(JNIEnv *env, jobject jCx, int argc, jobjectArray jArgv){
  int i = 0;
  assert(jCx);
  NativePointerHolder_set(S3JniNph(sqlite3_context), jCx, 0);
  for( ; i < argc; ++i ){
    jobject jsv = (*env)->GetObjectArrayElement(env, jArgv, i);
    /*
    ** There is a potential Java-triggerable case of Undefined
    ** Behavior here, but it would require intentional misuse of the
    ** API:
    **
    ** If a Java UDF grabs an sqlite3_value from its argv and then
    ** assigns that element to null, it becomes unreachable to us so
    ** we cannot clear out its pointer. That Java-side object's
    ** getNativePointer() will then refer to a stale value, so passing
    ** it into (e.g.) sqlite3_value_SOMETHING() would invoke UB.
    **
    ** High-level wrappers can avoid that possibility if they do not
    ** expose sqlite3_value directly to clients (as is the case in
    ** org.sqlite.jni.wrapper1.SqlFunction).
    **
    ** One potential (but expensive) workaround for this would be to
    ** privately store a duplicate argv array in each sqlite3_context
    ** wrapper object, and clear the native pointers from that copy.
    */
    assert(jsv && "Someone illegally modified a UDF argument array.");
    if( jsv ){
      NativePointerHolder_set(S3JniNph(sqlite3_value), jsv, 0);
    }
  }
}


/*
** Must be called immediately after a Java-side UDF callback throws.
** If translateToErr is true then it sets the exception's message in
** the result error using sqlite3_result_error(). If translateToErr is
** false then it emits a warning that the function threw but should
** not do so. In either case, it clears the exception state.
**
** Returns SQLITE_NOMEM if an allocation fails, else SQLITE_ERROR. In
** the former case it calls sqlite3_result_error_nomem().
*/
static int udf_report_exception(JNIEnv * const env, int translateToErr,
                                sqlite3_context * cx,
                                const char *zFuncName, const char *zFuncType ){
  jthrowable const ex = (*env)->ExceptionOccurred(env);
  int rc = SQLITE_ERROR;

  assert(ex && "This must only be called when a Java exception is pending.");
  if( translateToErr ){
    char * zMsg;
    char * z;

    S3JniExceptionClear;
    zMsg = s3jni_exception_error_msg(env, ex);
    z = sqlite3_mprintf("Client-defined SQL function %s.%s() threw: %s",
                        zFuncName ? zFuncName : "<unnamed>", zFuncType,
                        zMsg ? zMsg : "Unknown exception" );
    sqlite3_free(zMsg);
    if( z ){
      sqlite3_result_error(cx, z, -1);
      sqlite3_free(z);
    }else{
      sqlite3_result_error_nomem(cx);
      rc = SQLITE_NOMEM;
    }
  }else{
    S3JniExceptionWarnCallbackThrew("client-defined SQL function");
    S3JniExceptionClear;
  }
  S3JniUnrefLocal(ex);
  return rc;
}

/*
** Sets up the state for calling a Java-side xFunc/xStep/xInverse()
** UDF, calls it, and returns 0 on success.
*/
static int udf_xFSI(sqlite3_context* const pCx, int argc,
                    sqlite3_value** const argv, S3JniUdf * const s,
                    jmethodID xMethodID, const char * const zFuncType){
  S3JniDeclLocal_env;
  udf_jargs args = {0,0};
  int rc = udf_args(env, pCx, argc, argv, &args.jcx, &args.jargv);

  if( 0 == rc ){
    (*env)->CallVoidMethod(env, s->jObj, xMethodID, args.jcx, args.jargv);
    S3JniIfThrew{
      rc = udf_report_exception(env, 'F'==zFuncType[1]/*xFunc*/, pCx,
                                s->zFuncName, zFuncType);
    }
    udf_unargs(env, args.jcx, argc, args.jargv);
  }
  S3JniUnrefLocal(args.jcx);
  S3JniUnrefLocal(args.jargv);
  return rc;
}

/*
** Sets up the state for calling a Java-side xFinal/xValue() UDF,
** calls it, and returns 0 on success.
*/
static int udf_xFV(sqlite3_context* cx, S3JniUdf * s,
                   jmethodID xMethodID,
                   const char *zFuncType){
  S3JniDeclLocal_env;
  jobject jcx = new_java_sqlite3_context(env, cx);
  int rc = 0;
  int const isFinal = 'F'==zFuncType[1]/*xFinal*/;

  if( jcx ){
    (*env)->CallVoidMethod(env, s->jObj, xMethodID, jcx);
    S3JniIfThrew{
      rc = udf_report_exception(env, isFinal, cx, s->zFuncName,
                                zFuncType);
    }
    udf_unargs(env, jcx, 0, 0);
    S3JniUnrefLocal(jcx);
  }else{
    if( isFinal ) sqlite3_result_error_nomem(cx);
    rc = SQLITE_NOMEM;
  }
  return rc;
}

/* Proxy for C-to-Java xFunc. */
static void udf_xFunc(sqlite3_context* cx, int argc,
                      sqlite3_value** argv){
  S3JniUdf * const s = (S3JniUdf*)sqlite3_user_data(cx);
  s3jni_incr( &SJG.metrics.udf.nFunc );
  udf_xFSI(cx, argc, argv, s, s->jmidxFunc, "xFunc");
}
/* Proxy for C-to-Java xStep. */
static void udf_xStep(sqlite3_context* cx, int argc,
                      sqlite3_value** argv){
  S3JniUdf * const s = (S3JniUdf*)sqlite3_user_data(cx);
  s3jni_incr( &SJG.metrics.udf.nStep );
  udf_xFSI(cx, argc, argv, s, s->jmidxStep, "xStep");
}
/* Proxy for C-to-Java xFinal. */
static void udf_xFinal(sqlite3_context* cx){
  S3JniUdf * const s = (S3JniUdf*)sqlite3_user_data(cx);
  s3jni_incr( &SJG.metrics.udf.nFinal );
  udf_xFV(cx, s, s->jmidxFinal, "xFinal");
}
/* Proxy for C-to-Java xValue. */
static void udf_xValue(sqlite3_context* cx){
  S3JniUdf * const s = (S3JniUdf*)sqlite3_user_data(cx);
  s3jni_incr( &SJG.metrics.udf.nValue );
  udf_xFV(cx, s, s->jmidxValue, "xValue");
}
/* Proxy for C-to-Java xInverse. */
static void udf_xInverse(sqlite3_context* cx, int argc,
                         sqlite3_value** argv){
  S3JniUdf * const s = (S3JniUdf*)sqlite3_user_data(cx);
  s3jni_incr( &SJG.metrics.udf.nInverse );
  udf_xFSI(cx, argc, argv, s, s->jmidxInverse, "xInverse");
}


////////////////////////////////////////////////////////////////////////
// What follows is the JNI/C bindings. They are in alphabetical order
// except for this macro-generated subset which are kept together
// (alphabetized) here at the front...
////////////////////////////////////////////////////////////////////////

/** Create a trivial JNI wrapper for (int CName(void)). */
#define WRAP_INT_VOID(JniNameSuffix,CName)      \
  JniDecl(jint,JniNameSuffix)(JniArgsEnvClass){ \
    return (jint)CName();                       \
  }
/** Create a trivial JNI wrapper for (int CName(int)). */
#define WRAP_INT_INT(JniNameSuffix,CName)                 \
  JniDecl(jint,JniNameSuffix)(JniArgsEnvClass, jint arg){ \
    return (jint)CName((int)arg);                         \
  }
/*
** Create a trivial JNI wrapper for (const mutf8_string *
** CName(void)). This is only valid for functions which are known to
** return ASCII or text which is equivalent in UTF-8 and MUTF-8.
*/
#define WRAP_MUTF8_VOID(JniNameSuffix,CName)                   \
  JniDecl(jstring,JniNameSuffix)(JniArgsEnvClass){             \
    jstring const rv = (*env)->NewStringUTF( env, CName() );   \
    s3jni_oom_check(rv);                                       \
    return rv;                                                 \
  }
/** Create a trivial JNI wrapper for (int CName(sqlite3_stmt*)). */
#define WRAP_INT_STMT(JniNameSuffix,CName)                    \
  JniDecl(jint,JniNameSuffix)(JniArgsEnvClass, jlong jpStmt){ \
    return (jint)CName(LongPtrGet_sqlite3_stmt(jpStmt));    \
  }
/** Create a trivial JNI wrapper for (int CName(sqlite3_stmt*,int)). */
#define WRAP_INT_STMT_INT(JniNameSuffix,CName)                         \
  JniDecl(jint,JniNameSuffix)(JniArgsEnvClass, jlong jpStmt, jint n){ \
    return (jint)CName(LongPtrGet_sqlite3_stmt(jpStmt), (int)n);            \
  }
/** Create a trivial JNI wrapper for (boolean CName(sqlite3_stmt*)). */
#define WRAP_BOOL_STMT(JniNameSuffix,CName)                           \
  JniDecl(jboolean,JniNameSuffix)(JniArgsEnvClass, jobject jStmt){     \
    return CName(PtrGet_sqlite3_stmt(jStmt)) ? JNI_TRUE : JNI_FALSE; \
  }
/** Create a trivial JNI wrapper for (jstring CName(sqlite3_stmt*,int)). */
#define WRAP_STR_STMT_INT(JniNameSuffix,CName)                             \
  JniDecl(jstring,JniNameSuffix)(JniArgsEnvClass, jlong jpStmt, jint ndx){ \
    return s3jni_utf8_to_jstring(                                       \
      CName(LongPtrGet_sqlite3_stmt(jpStmt), (int)ndx),               \
      -1);                                                              \
  }
/** Create a trivial JNI wrapper for (boolean CName(sqlite3*)). */
#define WRAP_BOOL_DB(JniNameSuffix,CName)                           \
  JniDecl(jboolean,JniNameSuffix)(JniArgsEnvClass, jlong jpDb){     \
    return CName(LongPtrGet_sqlite3(jpDb)) ? JNI_TRUE : JNI_FALSE; \
  }
/** Create a trivial JNI wrapper for (int CName(sqlite3*)). */
#define WRAP_INT_DB(JniNameSuffix,CName)                    \
  JniDecl(jint,JniNameSuffix)(JniArgsEnvClass, jlong jpDb){ \
  return (jint)CName(LongPtrGet_sqlite3(jpDb)); \
  }
/** Create a trivial JNI wrapper for (int64 CName(sqlite3*)). */
#define WRAP_INT64_DB(JniNameSuffix,CName)                   \
  JniDecl(jlong,JniNameSuffix)(JniArgsEnvClass, jlong jpDb){ \
  return (jlong)CName(LongPtrGet_sqlite3(jpDb));  \
  }
/** Create a trivial JNI wrapper for (jstring CName(sqlite3*,int)). */
#define WRAP_STR_DB_INT(JniNameSuffix,CName)                             \
  JniDecl(jstring,JniNameSuffix)(JniArgsEnvClass, jlong jpDb, jint ndx){ \
    return s3jni_utf8_to_jstring(                                       \
      CName(LongPtrGet_sqlite3(jpDb), (int)ndx),               \
      -1);                                                              \
  }
/** Create a trivial JNI wrapper for (int CName(sqlite3_value*)). */
#define WRAP_INT_SVALUE(JniNameSuffix,CName,DfltOnNull)         \
  JniDecl(jint,JniNameSuffix)(JniArgsEnvClass, jlong jpSValue){ \
    sqlite3_value * const sv = LongPtrGet_sqlite3_value(jpSValue); \
    return (jint)(sv ? CName(sv): DfltOnNull);                      \
  }
/** Create a trivial JNI wrapper for (boolean CName(sqlite3_value*)). */
#define WRAP_BOOL_SVALUE(JniNameSuffix,CName,DfltOnNull)            \
  JniDecl(jboolean,JniNameSuffix)(JniArgsEnvClass, jlong jpSValue){ \
    sqlite3_value * const sv = LongPtrGet_sqlite3_value(jpSValue); \
    return (jint)(sv ? CName(sv) : DfltOnNull)                       \
      ? JNI_TRUE : JNI_FALSE;                                       \
  }

WRAP_INT_DB(1changes,                  sqlite3_changes)
WRAP_INT64_DB(1changes64,              sqlite3_changes64)
WRAP_INT_STMT(1clear_1bindings,        sqlite3_clear_bindings)
WRAP_INT_STMT_INT(1column_1bytes,      sqlite3_column_bytes)
WRAP_INT_STMT_INT(1column_1bytes16,    sqlite3_column_bytes16)
WRAP_INT_STMT(1column_1count,          sqlite3_column_count)
WRAP_STR_STMT_INT(1column_1decltype,   sqlite3_column_decltype)
WRAP_STR_STMT_INT(1column_1name,       sqlite3_column_name)
#ifdef SQLITE_ENABLE_COLUMN_METADATA
WRAP_STR_STMT_INT(1column_1database_1name,  sqlite3_column_database_name)
WRAP_STR_STMT_INT(1column_1origin_1name,    sqlite3_column_origin_name)
WRAP_STR_STMT_INT(1column_1table_1name,     sqlite3_column_table_name)
#endif
WRAP_INT_STMT_INT(1column_1type,       sqlite3_column_type)
WRAP_INT_STMT(1data_1count,            sqlite3_data_count)
WRAP_STR_DB_INT(1db_1name,             sqlite3_db_name)
WRAP_INT_DB(1error_1offset,            sqlite3_error_offset)
WRAP_INT_DB(1extended_1errcode,        sqlite3_extended_errcode)
WRAP_BOOL_DB(1get_1autocommit,         sqlite3_get_autocommit)
WRAP_MUTF8_VOID(1libversion,           sqlite3_libversion)
WRAP_INT_VOID(1libversion_1number,     sqlite3_libversion_number)
WRAP_INT_VOID(1keyword_1count,         sqlite3_keyword_count)
#ifdef SQLITE_ENABLE_PREUPDATE_HOOK
WRAP_INT_DB(1preupdate_1blobwrite,     sqlite3_preupdate_blobwrite)
WRAP_INT_DB(1preupdate_1count,         sqlite3_preupdate_count)
WRAP_INT_DB(1preupdate_1depth,         sqlite3_preupdate_depth)
#endif
WRAP_INT_INT(1release_1memory,         sqlite3_release_memory)
WRAP_INT_INT(1sleep,                   sqlite3_sleep)
WRAP_MUTF8_VOID(1sourceid,             sqlite3_sourceid)
WRAP_BOOL_STMT(1stmt_1busy,            sqlite3_stmt_busy)
WRAP_INT_STMT_INT(1stmt_1explain,      sqlite3_stmt_explain)
WRAP_INT_STMT(1stmt_1isexplain,        sqlite3_stmt_isexplain)
WRAP_BOOL_STMT(1stmt_1readonly,        sqlite3_stmt_readonly)
WRAP_INT_DB(1system_1errno,            sqlite3_system_errno)
WRAP_INT_VOID(1threadsafe,             sqlite3_threadsafe)
WRAP_INT_DB(1total_1changes,           sqlite3_total_changes)
WRAP_INT64_DB(1total_1changes64,       sqlite3_total_changes64)
WRAP_INT_SVALUE(1value_1encoding,      sqlite3_value_encoding,SQLITE_UTF8)
WRAP_BOOL_SVALUE(1value_1frombind,     sqlite3_value_frombind,0)
WRAP_INT_SVALUE(1value_1nochange,      sqlite3_value_nochange,0)
WRAP_INT_SVALUE(1value_1numeric_1type, sqlite3_value_numeric_type,SQLITE_NULL)
WRAP_INT_SVALUE(1value_1subtype,       sqlite3_value_subtype,0)
WRAP_INT_SVALUE(1value_1type,          sqlite3_value_type,SQLITE_NULL)

#undef WRAP_BOOL_DB
#undef WRAP_BOOL_STMT
#undef WRAP_BOOL_SVALUE
#undef WRAP_INT64_DB
#undef WRAP_INT_DB
#undef WRAP_INT_INT
#undef WRAP_INT_STMT
#undef WRAP_INT_STMT_INT
#undef WRAP_INT_SVALUE
#undef WRAP_INT_VOID
#undef WRAP_MUTF8_VOID
#undef WRAP_STR_STMT_INT
#undef WRAP_STR_DB_INT

S3JniApi(sqlite3_aggregate_context(),jlong,1aggregate_1context)(
  JniArgsEnvClass, jobject jCx, jboolean initialize
){
  sqlite3_context * const pCx = PtrGet_sqlite3_context(jCx);
  void * const p = pCx
    ? sqlite3_aggregate_context(pCx, (int)(initialize
                                           ? (int)sizeof(void*)
                                           : 0))
    : 0;
  return S3JniCast_P2L(p);
}

/*
** Central auto-extension runner for auto-extensions created in Java.
*/
static int s3jni_run_java_auto_extensions(sqlite3 *pDb, const char **pzErr,
                                          const struct sqlite3_api_routines *ignored){
  int rc = 0;
  unsigned i, go = 1;
  JNIEnv * env = 0;
  S3JniDb * ps;
  S3JniEnv * jc;

  if( 0==SJG.autoExt.nExt ) return 0;
  env = s3jni_env();
  jc = S3JniEnv_get();
  S3JniDb_mutex_enter;
  ps = jc->pdbOpening ? jc->pdbOpening : S3JniDb_from_c(pDb);
  if( !ps ){
    *pzErr = sqlite3_mprintf("Unexpected arrival of null S3JniDb in "
                             "auto-extension runner.");
    S3JniDb_mutex_leave;
    return SQLITE_ERROR;
  }
  assert( ps->jDb );
  if( !ps->pDb ){
    assert( jc->pdbOpening == ps );
    rc = sqlite3_set_clientdata(pDb, S3JniDb_clientdata_key,
                                ps, 0/* we'll re-set this after open()
                                        completes. */);
    if( rc ){
      S3JniDb_mutex_leave;
      return rc;
    }
  }
  else{
    assert( ps == jc->pdbOpening );
    jc->pdbOpening = 0;
  }
  S3JniDb_mutex_leave;
  NativePointerHolder_set(S3JniNph(sqlite3), ps->jDb, pDb)
    /* As of here, the Java/C connection is complete except for the
       (temporary) lack of finalizer for the ps object. */;
  ps->pDb = pDb;
  for( i = 0; go && 0==rc; ++i ){
    S3JniAutoExtension ax = S3JniHook_empty
      /* We need a copy of the auto-extension object, with our own
      ** local reference to it, to avoid a race condition with another
      ** thread manipulating the list during the call and invaliding
      ** what ax references. */;
    S3JniAutoExt_mutex_enter;
    if( i >= SJG.autoExt.nExt ){
      go = 0;
    }else{
      S3JniHook_localdup(&SJG.autoExt.aExt[i], &ax);
    }
    S3JniAutoExt_mutex_leave;
    if( ax.jObj ){
      rc = (*env)->CallIntMethod(env, ax.jObj, ax.midCallback, ps->jDb);
      S3JniHook_localundup(ax);
      S3JniIfThrew {
        jthrowable const ex = (*env)->ExceptionOccurred(env);
        char * zMsg;
        S3JniExceptionClear;
        zMsg = s3jni_exception_error_msg(env, ex);
        S3JniUnrefLocal(ex);
        *pzErr = sqlite3_mprintf("auto-extension threw: %s", zMsg);
        sqlite3_free(zMsg);
        rc = SQLITE_ERROR;
      }
    }
  }
  return rc;
}

S3JniApi(sqlite3_auto_extension(),jint,1auto_1extension)(
  JniArgsEnvClass, jobject jAutoExt
){
  int i;
  S3JniAutoExtension * ax = 0;
  int rc = 0;

  if( !jAutoExt ) return SQLITE_MISUSE;
  S3JniAutoExt_mutex_enter;
  for( i = 0; i < SJG.autoExt.nExt; ++i ){
    /* Look for a match. */
    ax = &SJG.autoExt.aExt[i];
    if( ax->jObj && (*env)->IsSameObject(env, ax->jObj, jAutoExt) ){
      /* same object, so this is a no-op. */
      S3JniAutoExt_mutex_leave;
      return 0;
    }
  }
  if( i == SJG.autoExt.nExt ){
    assert( SJG.autoExt.nExt <= SJG.autoExt.nAlloc );
    if( SJG.autoExt.nExt == SJG.autoExt.nAlloc ){
      /* Allocate another slot. */
      unsigned n = 1 + SJG.autoExt.nAlloc;
      S3JniAutoExtension * const aNew =
        s3jni_realloc( SJG.autoExt.aExt, n * sizeof(*ax) );
      if( !aNew ){
        rc = SQLITE_NOMEM;
      }else{
        SJG.autoExt.aExt = aNew;
        ++SJG.autoExt.nAlloc;
      }
    }
    if( 0==rc ){
      ax = &SJG.autoExt.aExt[SJG.autoExt.nExt];
      rc = S3JniAutoExtension_init(env, ax, jAutoExt);
      assert( rc ? (0==ax->jObj && 0==ax->midCallback)
              : (0!=ax->jObj && 0!=ax->midCallback) );
    }
  }
  if( 0==rc ){
    static int once = 0;
    if( 0==once && ++once ){
      rc = sqlite3_auto_extension(
        (void(*)(void))s3jni_run_java_auto_extensions
        /* Reminder: the JNI binding of sqlite3_reset_auto_extension()
        ** does not call the core-lib impl. It only clears Java-side
        ** auto-extensions. */
      );
      if( rc ){
        assert( ax );
        S3JniAutoExtension_clear(ax);
      }
    }
    if( 0==rc ){
      ++SJG.autoExt.nExt;
    }
  }
  S3JniAutoExt_mutex_leave;
  return rc;
}

S3JniApi(sqlite3_backup_finish(),jint,1backup_1finish)(
  JniArgsEnvClass, jlong jpBack
){
  int rc = 0;
  if( jpBack!=0 ){
    rc = sqlite3_backup_finish( LongPtrGet_sqlite3_backup(jpBack) );
  }
  return rc;
}

S3JniApi(sqlite3_backup_init(),jobject,1backup_1init)(
  JniArgsEnvClass, jlong jpDbDest, jstring jTDest,
  jlong jpDbSrc, jstring jTSrc
){
  sqlite3 * const pDest = LongPtrGet_sqlite3(jpDbDest);
  sqlite3 * const pSrc = LongPtrGet_sqlite3(jpDbSrc);
  char * const zDest = s3jni_jstring_to_utf8(jTDest, 0);
  char * const zSrc = s3jni_jstring_to_utf8(jTSrc, 0);
  jobject rv = 0;

  if( pDest && pSrc && zDest && zSrc ){
    sqlite3_backup * const pB =
      sqlite3_backup_init(pDest, zDest, pSrc, zSrc);
    if( pB ){
      rv = new_java_sqlite3_backup(env, pB);
      if( !rv ){
        sqlite3_backup_finish( pB );
      }
    }
  }
  sqlite3_free(zDest);
  sqlite3_free(zSrc);
  return rv;
}

S3JniApi(sqlite3_backup_pagecount(),jint,1backup_1pagecount)(
  JniArgsEnvClass, jlong jpBack
){
  return sqlite3_backup_pagecount(LongPtrGet_sqlite3_backup(jpBack));
}

S3JniApi(sqlite3_backup_remaining(),jint,1backup_1remaining)(
  JniArgsEnvClass, jlong jpBack
){
  return sqlite3_backup_remaining(LongPtrGet_sqlite3_backup(jpBack));
}

S3JniApi(sqlite3_backup_step(),jint,1backup_1step)(
  JniArgsEnvClass, jlong jpBack, jint nPage
){
  return sqlite3_backup_step(LongPtrGet_sqlite3_backup(jpBack), (int)nPage);
}

S3JniApi(sqlite3_bind_blob(),jint,1bind_1blob)(
  JniArgsEnvClass, jlong jpStmt, jint ndx, jbyteArray baData, jint nMax
){
  jsize nBA = 0;
  jbyte * const pBuf = baData ? s3jni_jbyteArray_bytes2(baData, &nBA) : 0;
  int rc;
  if( pBuf ){
    if( nMax>nBA ){
      nMax = nBA;
    }
    rc = sqlite3_bind_blob(LongPtrGet_sqlite3_stmt(jpStmt), (int)ndx,
                           pBuf, (int)nMax, SQLITE_TRANSIENT);
    s3jni_jbyteArray_release(baData, pBuf);
  }else{
    rc = baData
      ? SQLITE_NOMEM
      : sqlite3_bind_null( LongPtrGet_sqlite3_stmt(jpStmt), ndx );
  }
  return (jint)rc;
}

/**
   Helper for use with s3jni_setup_nio_args().
*/
struct S3JniNioArgs {
  jobject jBuf;        /* input - ByteBuffer */
  jint iOffset;        /* input - byte offset */
  jint iHowMany;       /* input - byte count to bind/read/write */
  jint nBuf;           /* output - jBuf's buffer size */
  void * p;            /* output - jBuf's buffer memory */
  void * pStart;       /* output - offset of p to bind/read/write */
  int nOut;            /* output - number of bytes from pStart to bind/read/write */
};
typedef struct S3JniNioArgs S3JniNioArgs;
static const S3JniNioArgs S3JniNioArgs_empty = {
  0,0,0,0,0,0,0
};

/*
** Internal helper for sqlite3_bind_nio_buffer(),
** sqlite3_result_nio_buffer(), and similar methods which take a
** ByteBuffer object as either input or output. Populates pArgs and
** returns 0 on success, non-0 if the operation should fail. The
** caller is required to check for SJG.g.byteBuffer.klazz!=0 before calling
** this and reporting it in a way appropriate for that routine.  This
** function may assert() that SJG.g.byteBuffer.klazz is not 0.
**
** The (jBuffer, iOffset, iHowMany) arguments are the (ByteBuffer, offset,
** length) arguments to the bind/result method.
**
** If iHowMany is negative then it's treated as "until the end" and
** the calculated slice is trimmed to fit if needed. If iHowMany is
** positive and extends past the end of jBuffer then SQLITE_ERROR is
** returned.
**
** Returns 0 if everything looks to be in order, else some SQLITE_...
** result code
*/
static int s3jni_setup_nio_args(
  JNIEnv *env, S3JniNioArgs * pArgs,
  jobject jBuffer, jint iOffset, jint iHowMany
){
  jlong iEnd = 0;
  const int bAllowTruncate = iHowMany<0;
  *pArgs = S3JniNioArgs_empty;
  pArgs->jBuf = jBuffer;
  pArgs->iOffset = iOffset;
  pArgs->iHowMany = iHowMany;
  assert( SJG.g.byteBuffer.klazz );
  if( pArgs->iOffset<0 ){
    return SQLITE_ERROR
      /* SQLITE_MISUSE or SQLITE_RANGE would fit better but we use
         SQLITE_ERROR for consistency with the code documented for a
         negative target blob offset in sqlite3_blob_read/write(). */;
  }
  s3jni_get_nio_buffer(pArgs->jBuf, &pArgs->p, &pArgs->nBuf);
  if( !pArgs->p ){
    return SQLITE_MISUSE;
  }else if( pArgs->iOffset>=pArgs->nBuf ){
    pArgs->pStart = 0;
    pArgs->nOut = 0;
    return 0;
  }
  assert( pArgs->nBuf > 0 );
  assert( pArgs->iOffset < pArgs->nBuf );
  iEnd = pArgs->iHowMany<0
    ? pArgs->nBuf - pArgs->iOffset
    : pArgs->iOffset + pArgs->iHowMany;
  if( iEnd>(jlong)pArgs->nBuf ){
    if( bAllowTruncate ){
      iEnd = pArgs->nBuf - pArgs->iOffset;
    }else{
      return SQLITE_ERROR
        /* again: for consistency with blob_read/write(), though
           SQLITE_MISUSE or SQLITE_RANGE would be a better fit. */;
    }
  }
  if( iEnd - pArgs->iOffset > (jlong)SQLITE_MAX_LENGTH ){
    return SQLITE_TOOBIG;
  }
  assert( pArgs->iOffset >= 0 );
  assert( iEnd > pArgs->iOffset );
  pArgs->pStart = pArgs->p + pArgs->iOffset;
  pArgs->nOut = (int)(iEnd - pArgs->iOffset);
  assert( pArgs->nOut > 0 );
  assert( (pArgs->pStart + pArgs->nOut) <= (pArgs->p + pArgs->nBuf) );
  return 0;
}

S3JniApi(sqlite3_bind_nio_buffer(),jint,1bind_1nio_1buffer)(
  JniArgsEnvClass, jobject jpStmt, jint ndx, jobject jBuffer,
  jint iOffset, jint iN
){
  sqlite3_stmt * pStmt = PtrGet_sqlite3_stmt(jpStmt);
  S3JniNioArgs args;
  int rc;
  if( !pStmt || !SJG.g.byteBuffer.klazz ) return SQLITE_MISUSE;
  rc = s3jni_setup_nio_args(env, &args, jBuffer, iOffset, iN);
  if(rc){
    return rc;
  }else if( !args.pStart || !args.nOut ){
    return sqlite3_bind_null(pStmt, ndx);
  }
  return sqlite3_bind_blob( pStmt, (int)ndx, args.pStart,
                            args.nOut, SQLITE_TRANSIENT );
}

S3JniApi(sqlite3_bind_double(),jint,1bind_1double)(
  JniArgsEnvClass, jlong jpStmt, jint ndx, jdouble val
){
  return (jint)sqlite3_bind_double(LongPtrGet_sqlite3_stmt(jpStmt),
                                   (int)ndx, (double)val);
}

S3JniApi(sqlite3_bind_int(),jint,1bind_1int)(
  JniArgsEnvClass, jlong jpStmt, jint ndx, jint val
){
  return (jint)sqlite3_bind_int(LongPtrGet_sqlite3_stmt(jpStmt), (int)ndx, (int)val);
}

S3JniApi(sqlite3_bind_int64(),jint,1bind_1int64)(
  JniArgsEnvClass, jlong jpStmt, jint ndx, jlong val
){
  return (jint)sqlite3_bind_int64(LongPtrGet_sqlite3_stmt(jpStmt), (int)ndx, (sqlite3_int64)val);
}

/*
** Bind a new global ref to Object `val` using sqlite3_bind_pointer().
*/
S3JniApi(sqlite3_bind_java_object(),jint,1bind_1java_1object)(
  JniArgsEnvClass, jlong jpStmt, jint ndx, jobject val
){
  sqlite3_stmt * const pStmt = LongPtrGet_sqlite3_stmt(jpStmt);
  int rc = SQLITE_MISUSE;

  if(pStmt){
    jobject const rv = S3JniRefGlobal(val);
    if( rv ){
      rc = sqlite3_bind_pointer(pStmt, ndx, rv, s3jni__value_jref_key,
                                S3Jni_jobject_finalizer);
    }else if(val){
      rc = SQLITE_NOMEM;
    }else{
      rc = sqlite3_bind_null(pStmt, ndx);
    }
  }
  return rc;
}

S3JniApi(sqlite3_bind_null(),jint,1bind_1null)(
  JniArgsEnvClass, jlong jpStmt, jint ndx
){
  return (jint)sqlite3_bind_null(LongPtrGet_sqlite3_stmt(jpStmt), (int)ndx);
}

S3JniApi(sqlite3_bind_parameter_count(),jint,1bind_1parameter_1count)(
  JniArgsEnvClass, jlong jpStmt
){
  return (jint)sqlite3_bind_parameter_count(LongPtrGet_sqlite3_stmt(jpStmt));
}

S3JniApi(sqlite3_bind_parameter_index(),jint,1bind_1parameter_1index)(
  JniArgsEnvClass, jlong jpStmt, jbyteArray jName
){
  int rc = 0;
  jbyte * const pBuf = s3jni_jbyteArray_bytes(jName);
  if( pBuf ){
    rc = sqlite3_bind_parameter_index(LongPtrGet_sqlite3_stmt(jpStmt),
                                      (const char *)pBuf);
    s3jni_jbyteArray_release(jName, pBuf);
  }
  return rc;
}

S3JniApi(sqlite3_bind_parameter_name(),jstring,1bind_1parameter_1name)(
  JniArgsEnvClass, jlong jpStmt, jint ndx
){
  const char *z =
    sqlite3_bind_parameter_name(LongPtrGet_sqlite3_stmt(jpStmt), (int)ndx);
  return z ? s3jni_utf8_to_jstring(z, -1) : 0;
}

/*
** Impl of sqlite3_bind_text/text16().
*/
static int s3jni__bind_text(int is16, JNIEnv *env, jlong jpStmt, jint ndx,
                            jbyteArray baData, jint nMax){
  jsize nBA = 0;
  jbyte * const pBuf =
    baData ? s3jni_jbyteArray_bytes2(baData, &nBA) : 0;
  int rc;
  if( pBuf ){
    if( nMax>nBA ){
      nMax = nBA;
    }
    /* Note that we rely on the Java layer having assured that baData
       is NUL-terminated if nMax is negative. In order to avoid UB for
       such cases, we do not expose the byte-limit arguments in the
       public API. */
    rc = is16
      ? sqlite3_bind_text16(LongPtrGet_sqlite3_stmt(jpStmt), (int)ndx,
                            pBuf, (int)nMax, SQLITE_TRANSIENT)
      : sqlite3_bind_text(LongPtrGet_sqlite3_stmt(jpStmt), (int)ndx,
                          (const char *)pBuf,
                          (int)nMax, SQLITE_TRANSIENT);
  }else{
    rc = baData
      ? sqlite3_bind_null(LongPtrGet_sqlite3_stmt(jpStmt), (int)ndx)
      : SQLITE_NOMEM;
  }
  s3jni_jbyteArray_release(baData, pBuf);
  return (jint)rc;

}

S3JniApi(sqlite3_bind_text(),jint,1bind_1text)(
  JniArgsEnvClass, jlong jpStmt, jint ndx, jbyteArray baData, jint nMax
){
  return s3jni__bind_text(0, env, jpStmt, ndx, baData, nMax);
}

S3JniApi(sqlite3_bind_text16(),jint,1bind_1text16)(
  JniArgsEnvClass, jlong jpStmt, jint ndx, jbyteArray baData, jint nMax
){
  return s3jni__bind_text(1, env, jpStmt, ndx, baData, nMax);
}

S3JniApi(sqlite3_bind_value(),jint,1bind_1value)(
  JniArgsEnvClass, jlong jpStmt, jint ndx, jlong jpValue
){
  int rc = 0;
  sqlite3_stmt * pStmt = LongPtrGet_sqlite3_stmt(jpStmt);
  if( pStmt ){
    sqlite3_value *v = LongPtrGet_sqlite3_value(jpValue);
    if( v ){
      rc = sqlite3_bind_value(pStmt, (int)ndx, v);
    }else{
      rc = sqlite3_bind_null(pStmt, (int)ndx);
    }
  }else{
    rc = SQLITE_MISUSE;
  }
  return (jint)rc;
}

S3JniApi(sqlite3_bind_zeroblob(),jint,1bind_1zeroblob)(
  JniArgsEnvClass, jlong jpStmt, jint ndx, jint n
){
  return (jint)sqlite3_bind_zeroblob(LongPtrGet_sqlite3_stmt(jpStmt),
                                     (int)ndx, (int)n);
}

S3JniApi(sqlite3_bind_zeroblob64(),jint,1bind_1zeroblob64)(
  JniArgsEnvClass, jlong jpStmt, jint ndx, jlong n
){
  return (jint)sqlite3_bind_zeroblob64(LongPtrGet_sqlite3_stmt(jpStmt),
                                       (int)ndx, (sqlite3_uint64)n);
}

S3JniApi(sqlite3_blob_bytes(),jint,1blob_1bytes)(
  JniArgsEnvClass, jlong jpBlob
){
  return sqlite3_blob_bytes(LongPtrGet_sqlite3_blob(jpBlob));
}

S3JniApi(sqlite3_blob_close(),jint,1blob_1close)(
  JniArgsEnvClass, jlong jpBlob
){
  sqlite3_blob * const b = LongPtrGet_sqlite3_blob(jpBlob);
  return b ? (jint)sqlite3_blob_close(b) : SQLITE_MISUSE;
}

S3JniApi(sqlite3_blob_open(),jint,1blob_1open)(
  JniArgsEnvClass, jlong jpDb, jstring jDbName, jstring jTbl, jstring jCol,
  jlong jRowId, jint flags, jobject jOut
){
  sqlite3 * const db = LongPtrGet_sqlite3(jpDb);
  sqlite3_blob * pBlob = 0;
  char * zDbName = 0, * zTableName = 0, * zColumnName = 0;
  int rc;

  if( !db || !jDbName || !jTbl || !jCol ) return SQLITE_MISUSE;
  zDbName = s3jni_jstring_to_utf8(jDbName,0);
  zTableName = zDbName ? s3jni_jstring_to_utf8(jTbl,0) : 0;
  zColumnName = zTableName ? s3jni_jstring_to_utf8(jCol,0) : 0;
  rc = zColumnName
    ? sqlite3_blob_open(db, zDbName, zTableName, zColumnName,
                        (sqlite3_int64)jRowId, (int)flags, &pBlob)
    : SQLITE_NOMEM;
  if( 0==rc ){
    jobject rv = new_java_sqlite3_blob(env, pBlob);
    if( !rv ){
      sqlite3_blob_close(pBlob);
      rc = SQLITE_NOMEM;
    }
    OutputPointer_set_obj(env, S3JniNph(OutputPointer_sqlite3_blob), jOut, rv);
  }
  sqlite3_free(zDbName);
  sqlite3_free(zTableName);
  sqlite3_free(zColumnName);
  return rc;
}

S3JniApi(sqlite3_blob_read(),jint,1blob_1read)(
  JniArgsEnvClass, jlong jpBlob, jbyteArray jTgt, jint iOffset
){
  jbyte * const pBa = s3jni_jbyteArray_bytes(jTgt);
  int rc = jTgt ? (pBa ? SQLITE_MISUSE : SQLITE_NOMEM) : SQLITE_MISUSE;
  if( pBa ){
    jsize const nTgt = (*env)->GetArrayLength(env, jTgt);
    rc = sqlite3_blob_read(LongPtrGet_sqlite3_blob(jpBlob), pBa,
                           (int)nTgt, (int)iOffset);
    if( 0==rc ){
      s3jni_jbyteArray_commit(jTgt, pBa);
    }else{
      s3jni_jbyteArray_release(jTgt, pBa);
    }
  }
  return rc;
}

S3JniApi(sqlite3_blob_read_nio_buffer(),jint,1blob_1read_1nio_1buffer)(
  JniArgsEnvClass, jlong jpBlob, jint iSrcOff, jobject jBB, jint iTgtOff, jint iHowMany
){
  sqlite3_blob * const b = LongPtrGet_sqlite3_blob(jpBlob);
  S3JniNioArgs args;
  int rc;
  if( !b || !SJG.g.byteBuffer.klazz || iHowMany<0 ){
    return SQLITE_MISUSE;
  }else if( iTgtOff<0 || iSrcOff<0 ){
    return SQLITE_ERROR
      /* for consistency with underlying sqlite3_blob_read() */;
  }else if( 0==iHowMany ){
    return 0;
  }
  rc = s3jni_setup_nio_args(env, &args, jBB, iTgtOff, iHowMany);
  if(rc){
    return rc;
  }else if( !args.pStart || !args.nOut ){
    return 0;
  }
  assert( args.iHowMany>0 );
  return sqlite3_blob_read( b, args.pStart, (int)args.nOut, (int)iSrcOff );
}

S3JniApi(sqlite3_blob_reopen(),jint,1blob_1reopen)(
  JniArgsEnvClass, jlong jpBlob, jlong iNewRowId
){
  return (jint)sqlite3_blob_reopen(LongPtrGet_sqlite3_blob(jpBlob),
                                   (sqlite3_int64)iNewRowId);
}

S3JniApi(sqlite3_blob_write(),jint,1blob_1write)(
  JniArgsEnvClass, jlong jpBlob, jbyteArray jBa, jint iOffset
){
  sqlite3_blob * const b = LongPtrGet_sqlite3_blob(jpBlob);
  jbyte * const pBuf = b ? s3jni_jbyteArray_bytes(jBa) : 0;
  const jsize nBA = pBuf ? (*env)->GetArrayLength(env, jBa) : 0;
  int rc = SQLITE_MISUSE;
  if(b && pBuf){
    rc = sqlite3_blob_write( b, pBuf, (int)nBA, (int)iOffset );
  }
  s3jni_jbyteArray_release(jBa, pBuf);
  return (jint)rc;
}

S3JniApi(sqlite3_blob_write_nio_buffer(),jint,1blob_1write_1nio_1buffer)(
  JniArgsEnvClass, jlong jpBlob, jint iTgtOff, jobject jBB, jint iSrcOff, jint iHowMany
){
  sqlite3_blob * const b = LongPtrGet_sqlite3_blob(jpBlob);
  S3JniNioArgs args;
  int rc;
  if( !b || !SJG.g.byteBuffer.klazz ){
    return SQLITE_MISUSE;
  }else if( iTgtOff<0 || iSrcOff<0 ){
    return SQLITE_ERROR
      /* for consistency with underlying sqlite3_blob_write() */;
  }else if( 0==iHowMany ){
    return 0;
  }
  rc = s3jni_setup_nio_args(env, &args, jBB, iSrcOff, iHowMany);
  if(rc){
    return rc;
  }else if( !args.pStart || !args.nOut ){
    return 0;
  }
  return sqlite3_blob_write( b, args.pStart, (int)args.nOut, (int)iTgtOff );
}

/* Central C-to-Java busy handler proxy. */
static int s3jni_busy_handler(void* pState, int n){
  S3JniDb * const ps = (S3JniDb *)pState;
  int rc = 0;
  S3JniDeclLocal_env;
  S3JniHook hook;

  S3JniHook_localdup(&ps->hooks.busyHandler, &hook);
  if( hook.jObj ){
    rc = (*env)->CallIntMethod(env, hook.jObj,
                               hook.midCallback, (jint)n);
    S3JniIfThrew{
      S3JniExceptionWarnCallbackThrew("sqlite3_busy_handler() callback");
      rc = s3jni_db_exception(ps->pDb, SQLITE_ERROR,
                              "sqlite3_busy_handler() callback threw.");
    }
    S3JniHook_localundup(hook);
  }
  return rc;
}

S3JniApi(sqlite3_busy_handler(),jint,1busy_1handler)(
  JniArgsEnvClass, jlong jpDb, jobject jBusy
){
  S3JniDb * const ps = S3JniDb_from_jlong(jpDb);
  S3JniHook * const pHook = ps ? &ps->hooks.busyHandler : 0;
  S3JniHook hook = S3JniHook_empty;
  int rc = 0;

  if( !ps ) return (jint)SQLITE_MISUSE;
  S3JniDb_mutex_enter;
  if( jBusy ){
    if( pHook->jObj && (*env)->IsSameObject(env, pHook->jObj, jBusy) ){
      /* Same object - this is a no-op. */
    }else{
      jclass const klazz = (*env)->GetObjectClass(env, jBusy);
      hook.jObj = S3JniRefGlobal(jBusy);
      hook.midCallback = (*env)->GetMethodID(env, klazz, "call", "(I)I");
      S3JniUnrefLocal(klazz);
      S3JniIfThrew {
        rc = SQLITE_ERROR;
      }
    }
  }
  if( 0==rc ){
    if( jBusy ){
      if( hook.jObj ){ /* Replace handler */
        rc = sqlite3_busy_handler(ps->pDb, s3jni_busy_handler, ps);
        if( 0==rc ){
          S3JniHook_unref(pHook);
          *pHook = hook /* transfer Java ref ownership */;
          hook = S3JniHook_empty;
        }
      }/* else no-op */
    }else{ /* Clear handler */
      rc = sqlite3_busy_handler(ps->pDb, 0, 0);
      if( 0==rc ){
        S3JniHook_unref(pHook);
      }
    }
  }
  S3JniHook_unref(&hook);
  S3JniDb_mutex_leave;
  return rc;
}

S3JniApi(sqlite3_busy_timeout(),jint,1busy_1timeout)(
  JniArgsEnvClass, jlong jpDb, jint ms
){
  S3JniDb * const ps = S3JniDb_from_jlong(jpDb);
  int rc = SQLITE_MISUSE;
  if( ps ){
    S3JniDb_mutex_enter;
    S3JniHook_unref(&ps->hooks.busyHandler);
    rc = sqlite3_busy_timeout(ps->pDb, (int)ms);
    S3JniDb_mutex_leave;
  }
  return rc;
}

S3JniApi(sqlite3_cancel_auto_extension(),jboolean,1cancel_1auto_1extension)(
  JniArgsEnvClass, jobject jAutoExt
){
  S3JniAutoExtension * ax;
  jboolean rc = JNI_FALSE;
  int i;

  if( !jAutoExt ){
    return rc;
  }
  S3JniAutoExt_mutex_enter;
  /* This algo corresponds to the one in the core. */
  for( i = SJG.autoExt.nExt-1; i >= 0; --i ){
    ax = &SJG.autoExt.aExt[i];
    if( ax->jObj && (*env)->IsSameObject(env, ax->jObj, jAutoExt) ){
      S3JniAutoExtension_clear(ax);
      /* Move final entry into this slot. */
      --SJG.autoExt.nExt;
      *ax = SJG.autoExt.aExt[SJG.autoExt.nExt];
      SJG.autoExt.aExt[SJG.autoExt.nExt] = S3JniHook_empty;
      assert( !SJG.autoExt.aExt[SJG.autoExt.nExt].jObj );
      rc = JNI_TRUE;
      break;
    }
  }
  S3JniAutoExt_mutex_leave;
  return rc;
}

/* Wrapper for sqlite3_close(_v2)(). */
static jint s3jni_close_db(JNIEnv * const env, jlong jpDb, int version){
  int rc = 0;
  S3JniDb * const ps = S3JniDb_from_jlong(jpDb);

  assert(version == 1 || version == 2);
  if( ps ){
    rc = 1==version
      ? (jint)sqlite3_close(ps->pDb)
      : (jint)sqlite3_close_v2(ps->pDb);
  }
  return (jint)rc;
}

S3JniApi(sqlite3_close(),jint,1close)(JniArgsEnvClass, jlong pDb){
  return s3jni_close_db(env, pDb, 1);
}

S3JniApi(sqlite3_close_v2(),jint,1close_1v2)(JniArgsEnvClass, jlong pDb){
  return s3jni_close_db(env, pDb, 2);
}

/*
** Assumes z is an array of unsigned short and returns the index in
** that array of the first element with the value 0.
*/
static unsigned int s3jni_utf16_strlen(void const * z){
  unsigned int i = 0;
  const unsigned short * p = z;
  while( p[i] ) ++i;
  return i;
}

/* Descriptive alias for use with sqlite3_collation_needed(). */
typedef S3JniHook S3JniCollationNeeded;

/* Central C-to-Java sqlite3_collation_needed16() hook impl. */
static void s3jni_collation_needed_impl16(void *pState, sqlite3 *pDb,
                                          int eTextRep, const void * z16Name){
  S3JniCollationNeeded * const pHook = pState;
  S3JniDeclLocal_env;
  S3JniHook hook;

  S3JniHook_localdup(pHook, &hook);
  if( hook.jObj ){
    unsigned int const nName = s3jni_utf16_strlen(z16Name);
    jstring jName = (*env)->NewString(env, (jchar const *)z16Name, nName);

    s3jni_oom_check( jName );
    assert( hook.jExtra );
    S3JniIfThrew{
      S3JniExceptionClear;
    }else if( hook.jExtra ){
      (*env)->CallVoidMethod(env, hook.jObj, hook.midCallback,
                             hook.jExtra, (jint)eTextRep, jName);
      S3JniIfThrew{
        S3JniExceptionWarnCallbackThrew("sqlite3_collation_needed() callback");
      }
    }
    S3JniUnrefLocal(jName);
    S3JniHook_localundup(hook);
  }
}

S3JniApi(sqlite3_collation_needed(),jint,1collation_1needed)(
  JniArgsEnvClass, jlong jpDb, jobject jHook
){
  S3JniDb * ps;
  S3JniCollationNeeded * pHook;
  int rc = 0;

  S3JniDb_mutex_enter;
  ps = S3JniDb_from_jlong(jpDb);
  if( !ps ){
    S3JniDb_mutex_leave;
    return SQLITE_MISUSE;
  }
  pHook = &ps->hooks.collationNeeded;
  if( pHook->jObj && jHook &&
     (*env)->IsSameObject(env, pHook->jObj, jHook) ){
    /* no-op */
  }else if( !jHook ){
    rc = sqlite3_collation_needed(ps->pDb, 0, 0);
    if( 0==rc ){
      S3JniHook_unref(pHook);
    }
  }else{
    jclass const klazz = (*env)->GetObjectClass(env, jHook);
    jmethodID const xCallback = (*env)->GetMethodID(
      env, klazz, "call", "(Lorg/sqlite/jni/capi/sqlite3;ILjava/lang/String;)V"
    );
    S3JniUnrefLocal(klazz);
    S3JniIfThrew {
      rc = s3jni_db_exception(ps->pDb, SQLITE_MISUSE,
                              "Cannot not find matching call() in "
                              "CollationNeededCallback object.");
    }else{
      rc = sqlite3_collation_needed16(ps->pDb, pHook,
                                      s3jni_collation_needed_impl16);
      if( 0==rc ){
        S3JniHook_unref(pHook);
        pHook->midCallback = xCallback;
        pHook->jObj = S3JniRefGlobal(jHook);
        pHook->jExtra = S3JniRefGlobal(ps->jDb);
      }
    }
  }
  S3JniDb_mutex_leave;
  return rc;
}

S3JniApi(sqlite3_column_blob(),jbyteArray,1column_1blob)(
  JniArgsEnvClass, jobject jpStmt, jint ndx
){
  sqlite3_stmt * const pStmt = PtrGet_sqlite3_stmt(jpStmt);
  void const * const p = sqlite3_column_blob(pStmt, (int)ndx);
  int const n = p ? sqlite3_column_bytes(pStmt, (int)ndx) : 0;

  return p ? s3jni_new_jbyteArray(p, n) : 0;
}

S3JniApi(sqlite3_column_double(),jdouble,1column_1double)(
  JniArgsEnvClass, jobject jpStmt, jint ndx
){
  return (jdouble)sqlite3_column_double(PtrGet_sqlite3_stmt(jpStmt), (int)ndx);
}

S3JniApi(sqlite3_column_int(),jint,1column_1int)(
  JniArgsEnvClass, jobject jpStmt, jint ndx
){
  return (jint)sqlite3_column_int(PtrGet_sqlite3_stmt(jpStmt), (int)ndx);
}

S3JniApi(sqlite3_column_int64(),jlong,1column_1int64)(
  JniArgsEnvClass, jobject jpStmt, jint ndx
){
  return (jlong)sqlite3_column_int64(PtrGet_sqlite3_stmt(jpStmt), (int)ndx);
}

S3JniApi(sqlite3_column_java_object(),jobject,1column_1java_1object)(
  JniArgsEnvClass, jlong jpStmt, jint ndx
){
  sqlite3_stmt * const stmt = LongPtrGet_sqlite3_stmt(jpStmt);
  jobject rv = 0;
  if( stmt ){
    sqlite3 * const db = sqlite3_db_handle(stmt);
    sqlite3_value * sv;
    sqlite3_mutex_enter(sqlite3_db_mutex(db));
    sv = sqlite3_column_value(stmt, (int)ndx);
    if( sv ){
      rv = S3JniRefLocal(
        sqlite3_value_pointer(sv, s3jni__value_jref_key)
      );
    }
    sqlite3_mutex_leave(sqlite3_db_mutex(db));
  }
  return rv;
}

S3JniApi(sqlite3_column_nio_buffer(),jobject,1column_1nio_1buffer)(
  JniArgsEnvClass, jobject jStmt, jint ndx
){
  sqlite3_stmt * const stmt = PtrGet_sqlite3_stmt(jStmt);
  jobject rv = 0;
  if( stmt ){
    const void * const p = sqlite3_column_blob(stmt, (int)ndx);
    if( p ){
      const int n = sqlite3_column_bytes(stmt, (int)ndx);
      rv = s3jni__blob_to_ByteBuffer(env, p, n);
    }
  }
  return rv;
}

S3JniApi(sqlite3_column_text(),jbyteArray,1column_1text)(
  JniArgsEnvClass, jobject jpStmt, jint ndx
){
  sqlite3_stmt * const stmt = PtrGet_sqlite3_stmt(jpStmt);
  const unsigned char * const p = stmt ? sqlite3_column_text(stmt, (int)ndx) : 0;
  const int n = p ? sqlite3_column_bytes(stmt, (int)ndx) : 0;
  return p ? s3jni_new_jbyteArray(p, n) : NULL;
}

#if 0
// this impl might prove useful.
S3JniApi(sqlite3_column_text(),jstring,1column_1text)(
  JniArgsEnvClass, jobject jpStmt, jint ndx
){
  sqlite3_stmt * const stmt = PtrGet_sqlite3_stmt(jpStmt);
  const unsigned char * const p = stmt ? sqlite3_column_text(stmt, (int)ndx) : 0;
  const int n = p ? sqlite3_column_bytes(stmt, (int)ndx) : 0;
  return p ? s3jni_utf8_to_jstring( (const char *)p, n) : 0;
}
#endif

S3JniApi(sqlite3_column_text16(),jstring,1column_1text16)(
  JniArgsEnvClass, jobject jpStmt, jint ndx
){
  sqlite3_stmt * const stmt = PtrGet_sqlite3_stmt(jpStmt);
  const void * const p = stmt ? sqlite3_column_text16(stmt, (int)ndx) : 0;
  const int n = p ? sqlite3_column_bytes16(stmt, (int)ndx) : 0;
  return s3jni_text16_to_jstring(env, p, n);
}

S3JniApi(sqlite3_column_value(),jobject,1column_1value)(
  JniArgsEnvClass, jobject jpStmt, jint ndx
){
  sqlite3_value * const sv =
    sqlite3_column_value(PtrGet_sqlite3_stmt(jpStmt), (int)ndx)
    /* reminder: returns an SQL NULL if jpStmt==NULL */;
  return new_java_sqlite3_value(env, sv);
}

/*
** Impl for commit hooks (if isCommit is true) or rollback hooks.
*/
static int s3jni_commit_rollback_hook_impl(int isCommit, S3JniDb * const ps){
  S3JniDeclLocal_env;
  int rc = 0;
  S3JniHook hook;

  S3JniHook_localdup(isCommit
                     ? &ps->hooks.commit : &ps->hooks.rollback,
                     &hook);
  if( hook.jObj ){
    rc = isCommit
      ? (int)(*env)->CallIntMethod(env, hook.jObj, hook.midCallback)
      : (int)((*env)->CallVoidMethod(env, hook.jObj, hook.midCallback), 0);
    S3JniIfThrew{
      rc = s3jni_db_exception(ps->pDb, SQLITE_ERROR,
                              isCommit
                              ? "Commit hook callback threw"
                              : "Rollback hook callback threw");
    }
    S3JniHook_localundup(hook);
  }
  return rc;
}

/* C-to-Java commit hook wrapper. */
static int s3jni_commit_hook_impl(void *pP){
  return s3jni_commit_rollback_hook_impl(1, pP);
}

/* C-to-Java rollback hook wrapper. */
static void s3jni_rollback_hook_impl(void *pP){
  (void)s3jni_commit_rollback_hook_impl(0, pP);
}

/*
** Proxy for sqlite3_commit_hook() (if isCommit is true) or
** sqlite3_rollback_hook().
*/
static jobject s3jni_commit_rollback_hook(int isCommit, JNIEnv * const env,
                                          jlong jpDb, jobject jHook){
  S3JniDb * ps;
  jobject pOld = 0;  /* previous hoook */
  S3JniHook * pHook; /* ps->hooks.commit|rollback */

  S3JniDb_mutex_enter;
  ps = S3JniDb_from_jlong(jpDb);
  if( !ps ){
    s3jni_db_error(ps->pDb, SQLITE_MISUSE, 0);
    S3JniDb_mutex_leave;
    return 0;
  }
  pHook = isCommit ? &ps->hooks.commit : &ps->hooks.rollback;
  pOld = pHook->jObj;
  if( pOld && jHook &&
      (*env)->IsSameObject(env, pOld, jHook) ){
    /* No-op. */
  }else if( !jHook ){
    if( pOld ){
      jobject tmp = S3JniRefLocal(pOld);
      S3JniUnrefGlobal(pOld);
      pOld = tmp;
    }
    *pHook = S3JniHook_empty;
    if( isCommit ) sqlite3_commit_hook(ps->pDb, 0, 0);
    else sqlite3_rollback_hook(ps->pDb, 0, 0);
  }else{
    jclass const klazz = (*env)->GetObjectClass(env, jHook);
    jmethodID const xCallback = (*env)->GetMethodID(env, klazz, "call",
                                                    isCommit ? "()I" : "()V");
    S3JniUnrefLocal(klazz);
    S3JniIfThrew {
      S3JniExceptionReport;
      S3JniExceptionClear;
      s3jni_db_error(ps->pDb, SQLITE_ERROR,
                     "Cannot not find matching call() method in"
                     "hook object.");
    }else{
      pHook->midCallback = xCallback;
      pHook->jObj = S3JniRefGlobal(jHook);
      if( isCommit ) sqlite3_commit_hook(ps->pDb, s3jni_commit_hook_impl, ps);
      else sqlite3_rollback_hook(ps->pDb, s3jni_rollback_hook_impl, ps);
      if( pOld ){
        jobject tmp = S3JniRefLocal(pOld);
        S3JniUnrefGlobal(pOld);
        pOld = tmp;
      }
    }
  }
  S3JniDb_mutex_leave;
  return pOld;
}

S3JniApi(sqlite3_commit_hook(),jobject,1commit_1hook)(
  JniArgsEnvClass, jlong jpDb, jobject jHook
){
  return s3jni_commit_rollback_hook(1, env, jpDb, jHook);
}

S3JniApi(sqlite3_compileoption_get(),jstring,1compileoption_1get)(
  JniArgsEnvClass, jint n
){
  const char * z = sqlite3_compileoption_get(n);
  jstring const rv = z ? (*env)->NewStringUTF( env, z ) : 0;
    /* We know these to be ASCII, so MUTF-8 is fine. */;
  s3jni_oom_check(z ? !!rv : 1);
  return rv;
}

S3JniApi(sqlite3_compileoption_used(),jboolean,1compileoption_1used)(
  JniArgsEnvClass, jstring name
){
  const char *zUtf8 = s3jni_jstring_to_mutf8(name)
    /* We know these to be ASCII, so MUTF-8 is fine (and
       hypothetically faster to convert). */;
  const jboolean rc =
    0==sqlite3_compileoption_used(zUtf8) ? JNI_FALSE : JNI_TRUE;
  s3jni_mutf8_release(name, zUtf8);
  return rc;
}

S3JniApi(sqlite3_complete(),jint,1complete)(
  JniArgsEnvClass, jbyteArray jSql
){
  jbyte * const pBuf = s3jni_jbyteArray_bytes(jSql);
  const jsize nBA = pBuf ? (*env)->GetArrayLength(env, jSql) : 0;
  int rc;

  assert( (nBA>0 ? 0==pBuf[nBA-1] : (pBuf ? 0==*pBuf : 1))
          && "Byte array is not NUL-terminated." );
  rc = (pBuf && 0==pBuf[(nBA ? nBA-1 : 0)])
    ? sqlite3_complete( (const char *)pBuf )
    : (jSql ? SQLITE_NOMEM : SQLITE_MISUSE);
  s3jni_jbyteArray_release(jSql, pBuf);
  return rc;
}

S3JniApi(sqlite3_config() /*for a small subset of options.*/
         sqlite3_config__enable()/* internal name to avoid name-mangling issues*/,
         jint,1config_1_1enable)(JniArgsEnvClass, jint n){
  switch( n ){
    case SQLITE_CONFIG_SINGLETHREAD:
    case SQLITE_CONFIG_MULTITHREAD:
    case SQLITE_CONFIG_SERIALIZED:
      return sqlite3_config( n );
    default:
      return SQLITE_MISUSE;
  }
}
/* C-to-Java SQLITE_CONFIG_LOG wrapper. */
static void s3jni_config_log(void *ignored, int errCode, const char *z){
  S3JniDeclLocal_env;
  S3JniHook hook = S3JniHook_empty;

  S3JniHook_localdup(&SJG.hook.configlog, &hook);
  if( hook.jObj ){
    jstring const jArg1 = z ? s3jni_utf8_to_jstring(z, -1) : 0;
    if( z ? !!jArg1 : 1 ){
      (*env)->CallVoidMethod(env, hook.jObj, hook.midCallback, errCode, jArg1);
    }
    S3JniIfThrew{
      S3JniExceptionWarnCallbackThrew("SQLITE_CONFIG_LOG callback");
      S3JniExceptionClear;
    }
    S3JniHook_localundup(hook);
    S3JniUnrefLocal(jArg1);
  }
}

S3JniApi(sqlite3_config() /* for SQLITE_CONFIG_LOG */
         sqlite3_config__config_log() /* internal name */,
         jint, 1config_1_1CONFIG_1LOG
)(JniArgsEnvClass, jobject jLog){
  S3JniHook * const pHook = &SJG.hook.configlog;
  int rc = 0;

  S3JniGlobal_mutex_enter;
  if( !jLog ){
    rc = sqlite3_config( SQLITE_CONFIG_LOG, NULL, NULL );
    if( 0==rc ){
      S3JniHook_unref(pHook);
    }
  }else if( pHook->jObj && (*env)->IsSameObject(env, jLog, pHook->jObj) ){
    /* No-op */
  }else {
    jclass const klazz = (*env)->GetObjectClass(env, jLog);
    jmethodID const midCallback = (*env)->GetMethodID(env, klazz, "call",
                                                      "(ILjava/lang/String;)V");
    S3JniUnrefLocal(klazz);
    if( midCallback ){
      rc = sqlite3_config( SQLITE_CONFIG_LOG, s3jni_config_log, NULL );
      if( 0==rc ){
        S3JniHook_unref(pHook);
        pHook->midCallback = midCallback;
        pHook->jObj = S3JniRefGlobal(jLog);
      }
    }else{
      S3JniExceptionWarnIgnore;
      rc = SQLITE_ERROR;
    }
  }
  S3JniGlobal_mutex_leave;
  return rc;
}

#ifdef SQLITE_ENABLE_SQLLOG
/* C-to-Java SQLITE_CONFIG_SQLLOG wrapper. */
static void s3jni_config_sqllog(void *ignored, sqlite3 *pDb, const char *z, int op){
  jobject jArg0 = 0;
  jstring jArg1 = 0;
  S3JniDeclLocal_env;
  S3JniDb * const ps = S3JniDb_from_c(pDb);
  S3JniHook hook = S3JniHook_empty;

  if( ps ){
    S3JniHook_localdup(&SJG.hook.sqllog, &hook);
  }
  if( !hook.jObj ) return;
  jArg0 = S3JniRefLocal(ps->jDb);
  switch( op ){
    case 0: /* db opened */
    case 1: /* SQL executed */
      jArg1 = s3jni_utf8_to_jstring( z, -1);
      break;
    case 2: /* db closed */
      break;
    default:
      (*env)->FatalError(env, "Unhandled 4th arg to SQLITE_CONFIG_SQLLOG.");
      break;
  }
  (*env)->CallVoidMethod(env, hook.jObj, hook.midCallback, jArg0, jArg1, op);
  S3JniIfThrew{
    S3JniExceptionWarnCallbackThrew("SQLITE_CONFIG_SQLLOG callback");
    S3JniExceptionClear;
  }
  S3JniHook_localundup(hook);
  S3JniUnrefLocal(jArg0);
  S3JniUnrefLocal(jArg1);
}
//! Requirement of SQLITE_CONFIG_SQLLOG.
void sqlite3_init_sqllog(void){
  sqlite3_config( SQLITE_CONFIG_SQLLOG, s3jni_config_sqllog, 0 );
}
#endif

S3JniApi(sqlite3_config() /* for SQLITE_CONFIG_SQLLOG */
         sqlite3_config__SQLLOG() /*internal name*/,
         jint, 1config_1_1SQLLOG
)(JniArgsEnvClass, jobject jLog){
#ifndef SQLITE_ENABLE_SQLLOG
  return SQLITE_MISUSE;
#else
  S3JniHook * const pHook = &SJG.hook.sqllog;
  int rc = 0;

  S3JniGlobal_mutex_enter;
  if( !jLog ){
    rc = sqlite3_config( SQLITE_CONFIG_SQLLOG, NULL );
    if( 0==rc ){
      S3JniHook_unref(pHook);
    }
  }else if( pHook->jObj && (*env)->IsSameObject(env, jLog, pHook->jObj) ){
    /* No-op */
  }else {
    jclass const klazz = (*env)->GetObjectClass(env, jLog);
    jmethodID const midCallback = (*env)->GetMethodID(env, klazz, "call",
                                                      "(Lorg/sqlite/jni/capi/sqlite3;"
                                                      "Ljava/lang/String;"
                                                      "I)V");
    S3JniUnrefLocal(klazz);
    if( midCallback ){
      rc = sqlite3_config( SQLITE_CONFIG_SQLLOG, s3jni_config_sqllog, NULL );
      if( 0==rc ){
        S3JniHook_unref(pHook);
        pHook->midCallback = midCallback;
        pHook->jObj = S3JniRefGlobal(jLog);
      }
    }else{
      S3JniExceptionWarnIgnore;
      rc = SQLITE_ERROR;
    }
  }
  S3JniGlobal_mutex_leave;
  return rc;
#endif
}

S3JniApi(sqlite3_context_db_handle(),jobject,1context_1db_1handle)(
  JniArgsEnvClass, jobject jpCx
){
  sqlite3_context * const pCx = PtrGet_sqlite3_context(jpCx);
  sqlite3 * const pDb = pCx ? sqlite3_context_db_handle(pCx) : 0;
  S3JniDb * const ps = pDb ? S3JniDb_from_c(pDb) : 0;
  return ps ? ps->jDb : 0;
}

/*
** State for CollationCallbacks. This used to be its own separate
** type, but has since been consolidated with S3JniHook. It retains
** its own typedef for code legibility and searchability reasons.
*/
typedef S3JniHook S3JniCollationCallback;

/*
** Proxy for Java-side CollationCallback.xCompare() callbacks.
*/
static int CollationCallback_xCompare(void *pArg, int nLhs, const void *lhs,
                                      int nRhs, const void *rhs){
  S3JniCollationCallback * const pCC = pArg;
  S3JniDeclLocal_env;
  jint rc = 0;
  if( pCC->jObj ){
    jbyteArray jbaLhs = s3jni_new_jbyteArray(lhs, (jint)nLhs);
    jbyteArray jbaRhs = jbaLhs
      ? s3jni_new_jbyteArray(rhs, (jint)nRhs) : 0;
    if( !jbaRhs ){
      S3JniUnrefLocal(jbaLhs);
      /* We have no recovery strategy here. */
      s3jni_oom_check( jbaRhs );
      return 0;
    }
    rc = (*env)->CallIntMethod(env, pCC->jObj, pCC->midCallback,
                               jbaLhs, jbaRhs);
    S3JniExceptionIgnore;
    S3JniUnrefLocal(jbaLhs);
    S3JniUnrefLocal(jbaRhs);
  }
  return (int)rc;
}

/* CollationCallback finalizer for use by the sqlite3 internals. */
static void CollationCallback_xDestroy(void *pArg){
  S3JniCollationCallback * const pCC = pArg;
  S3JniDeclLocal_env;
  S3JniHook_free(pCC);
}

S3JniApi(sqlite3_create_collation() sqlite3_create_collation_v2(),
         jint,1create_1collation
)(JniArgsEnvClass, jobject jDb, jstring name, jint eTextRep,
  jobject oCollation){
  int rc;
  S3JniDb * ps;

  if( !jDb || !name || !encodingTypeIsValid(eTextRep) ){
    return (jint)SQLITE_MISUSE;
  }
  S3JniDb_mutex_enter;
  ps = S3JniDb_from_java(jDb);
  jclass const klazz = (*env)->GetObjectClass(env, oCollation);
  jmethodID const midCallback =
    (*env)->GetMethodID(env, klazz, "call", "([B[B)I");
  S3JniUnrefLocal(klazz);
  S3JniIfThrew{
    rc = s3jni_db_error(ps->pDb, SQLITE_ERROR,
                        "Could not get call() method from "
                        "CollationCallback object.");
  }else{
    char * const zName = s3jni_jstring_to_utf8(name, 0);
    S3JniCollationCallback * const pCC =
      zName ? S3JniHook_alloc() : 0;
    if( pCC ){
      rc = sqlite3_create_collation_v2(ps->pDb, zName, (int)eTextRep,
                                       pCC, CollationCallback_xCompare,
                                       CollationCallback_xDestroy);
      if( 0==rc ){
        pCC->midCallback = midCallback;
        pCC->jObj = S3JniRefGlobal(oCollation);
        pCC->doXDestroy = 1;
      }else{
        CollationCallback_xDestroy(pCC);
      }
    }else{
      rc = SQLITE_NOMEM;
    }
    sqlite3_free(zName);
  }
  S3JniDb_mutex_leave;
  return (jint)rc;
}

S3JniApi(sqlite3_create_function() sqlite3_create_function_v2()
         sqlite3_create_window_function(),
         jint,1create_1function
)(JniArgsEnvClass, jobject jDb, jstring jFuncName, jint nArg,
  jint eTextRep, jobject jFunctor){
  S3JniUdf * s = 0;
  int rc;
  sqlite3 * const pDb = PtrGet_sqlite3(jDb);
  char * zFuncName = 0;

  if( !pDb || !jFuncName ){
    return SQLITE_MISUSE;
  }else if( !encodingTypeIsValid(eTextRep) ){
    return s3jni_db_error(pDb, SQLITE_FORMAT,
                          "Invalid function encoding option.");
  }
  s = S3JniUdf_alloc(env, jFunctor);
  if( !s ) return SQLITE_NOMEM;

  if( UDF_UNKNOWN_TYPE==s->type ){
    rc = s3jni_db_error(pDb, SQLITE_MISUSE,
                        "Cannot unambiguously determine function type.");
    S3JniUdf_free(env, s, 1);
    goto error_cleanup;
  }
  zFuncName = s3jni_jstring_to_utf8(jFuncName,0);
  if( !zFuncName ){
    rc = SQLITE_NOMEM;
    S3JniUdf_free(env, s, 1);
    goto error_cleanup;
  }
  s->zFuncName = zFuncName /* pass on ownership */;
  if( UDF_WINDOW == s->type ){
    rc = sqlite3_create_window_function(pDb, zFuncName, nArg, eTextRep, s,
                                        udf_xStep, udf_xFinal, udf_xValue,
                                        udf_xInverse, S3JniUdf_finalizer);
  }else{
    udf_xFunc_f xFunc = 0;
    udf_xStep_f xStep = 0;
    udf_xFinal_f xFinal = 0;
    if( UDF_SCALAR == s->type ){
      xFunc = udf_xFunc;
    }else{
      assert( UDF_AGGREGATE == s->type );
      xStep = udf_xStep;
      xFinal = udf_xFinal;
    }
    rc = sqlite3_create_function_v2(pDb, zFuncName, nArg, eTextRep, s,
                                    xFunc, xStep, xFinal, S3JniUdf_finalizer);
  }
error_cleanup:
  /* Reminder: on sqlite3_create_function() error, s will be
  ** destroyed via create_function(). */
  return (jint)rc;
}


S3JniApi(sqlite3_db_config() /*for MAINDBNAME*/,
         jint,1db_1config__Lorg_sqlite_jni_capi_sqlite3_2ILjava_lang_String_2
)(JniArgsEnvClass, jobject jDb, jint op, jstring jStr){
  S3JniDb * const ps = S3JniDb_from_java(jDb);
  int rc;
  char *zStr;

  switch( (ps && jStr) ? op : 0 ){
    case SQLITE_DBCONFIG_MAINDBNAME:
      S3JniDb_mutex_enter
        /* Protect against a race in modifying/freeing
           ps->zMainDbName. */;
      zStr = s3jni_jstring_to_utf8( jStr, 0);
      if( zStr ){
        rc = sqlite3_db_config(ps->pDb, (int)op, zStr);
        if( rc ){
          sqlite3_free( zStr );
        }else{
          sqlite3_free( ps->zMainDbName );
          ps->zMainDbName = zStr;
        }
      }else{
        rc = SQLITE_NOMEM;
      }
      S3JniDb_mutex_leave;
      break;
    case 0:
    default:
      rc = SQLITE_MISUSE;
  }
  return rc;
}

S3JniApi(
  sqlite3_db_config(),
  /* WARNING: openjdk v19 creates a different mangled name for this
  ** function than openjdk v8 does. We account for that by exporting
  ** both versions of the name. */
  jint,1db_1config__Lorg_sqlite_jni_capi_sqlite3_2IILorg_sqlite_jni_capi_OutputPointer_Int32_2
)(
  JniArgsEnvClass, jobject jDb, jint op, jint onOff, jobject jOut
){
  S3JniDb * const ps = S3JniDb_from_java(jDb);
  int rc;
  switch( ps ? op : 0 ){
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
    case SQLITE_DBCONFIG_STMT_SCANSTATUS:
    case SQLITE_DBCONFIG_REVERSE_SCANORDER: {
      int pOut = 0;
      rc = sqlite3_db_config( ps->pDb, (int)op, onOff, &pOut );
      if( 0==rc && jOut ){
        OutputPointer_set_Int32(env, jOut, pOut);
      }
      break;
    }
    default:
      rc = SQLITE_MISUSE;
  }
  return (jint)rc;
}

/*
** This is a workaround for openjdk v19 (and possibly others) encoding
** this function's name differently than JDK v8 does. If we do not
** install both names for this function then Java will not be able to
** find the function in both environments.
*/
JniDecl(jint,1db_1config__Lorg_sqlite_jni_capi_sqlite3_2IILorg_sqlite_jni_capi_OutputPointer_00024Int32_2)(
  JniArgsEnvClass, jobject jDb, jint op, jint onOff, jobject jOut
){
  return JniFuncName(1db_1config__Lorg_sqlite_jni_capi_sqlite3_2IILorg_sqlite_jni_capi_OutputPointer_Int32_2)(
    env, jKlazz, jDb, op, onOff, jOut
  );
}

S3JniApi(sqlite3_db_filename(),jstring,1db_1filename)(
  JniArgsEnvClass, jobject jDb, jstring jDbName
){
  S3JniDb * const ps = S3JniDb_from_java(jDb);
  char *zDbName;
  jstring jRv = 0;
  int nStr = 0;

  if( !ps || !jDbName ){
    return 0;
  }
  zDbName = s3jni_jstring_to_utf8( jDbName, &nStr);
  if( zDbName ){
    char const * zRv = sqlite3_db_filename(ps->pDb, zDbName);
    sqlite3_free(zDbName);
    if( zRv ){
      jRv = s3jni_utf8_to_jstring( zRv, -1);
    }
  }
  return jRv;
}

S3JniApi(sqlite3_db_handle(),jobject,1db_1handle)(
  JniArgsEnvClass, jobject jpStmt
){
  sqlite3_stmt * const pStmt = PtrGet_sqlite3_stmt(jpStmt);
  sqlite3 * const pDb = pStmt ? sqlite3_db_handle(pStmt) : 0;
  S3JniDb * const ps = pDb ? S3JniDb_from_c(pDb) : 0;
  return ps ? ps->jDb : 0;
}

S3JniApi(sqlite3_db_readonly(),jint,1db_1readonly)(
  JniArgsEnvClass, jobject jDb, jstring jDbName
){
  int rc = 0;
  S3JniDb * const ps = S3JniDb_from_java(jDb);
  char *zDbName = jDbName ? s3jni_jstring_to_utf8( jDbName, 0 ) : 0;
  rc = sqlite3_db_readonly(ps ? ps->pDb : 0, zDbName);
  sqlite3_free(zDbName);
  return (jint)rc;
}

S3JniApi(sqlite3_db_release_memory(),jint,1db_1release_1memory)(
  JniArgsEnvClass, jobject jDb
){
  sqlite3 * const pDb = PtrGet_sqlite3(jDb);
  return pDb ? sqlite3_db_release_memory(pDb) : SQLITE_MISUSE;
}

S3JniApi(sqlite3_db_status(),jint,1db_1status)(
  JniArgsEnvClass, jobject jDb, jint op, jobject jOutCurrent,
                        jobject jOutHigh, jboolean reset
){
  int iCur = 0, iHigh = 0;
  sqlite3 * const pDb = PtrGet_sqlite3(jDb);
  int rc = sqlite3_db_status( pDb, op, &iCur, &iHigh, reset );
  if( 0==rc ){
    OutputPointer_set_Int32(env, jOutCurrent, iCur);
    OutputPointer_set_Int32(env, jOutHigh, iHigh);
  }
  return (jint)rc;
}

S3JniApi(sqlite3_errcode(),jint,1errcode)(
  JniArgsEnvClass, jobject jpDb
){
  sqlite3 * const pDb = PtrGet_sqlite3(jpDb);
  return pDb ? sqlite3_errcode(pDb) : SQLITE_MISUSE;
}

S3JniApi(sqlite3_errmsg(),jstring,1errmsg)(
  JniArgsEnvClass, jobject jpDb
){
  sqlite3 * const pDb = PtrGet_sqlite3(jpDb);
  return pDb ? s3jni_utf8_to_jstring( sqlite3_errmsg(pDb), -1) : 0
    /* We don't use errmsg16() directly only because it would cause an
       additional level of internal encoding in sqlite3. The end
       effect should be identical to using errmsg16(), however. */;
}

S3JniApi(sqlite3_errstr(),jstring,1errstr)(
  JniArgsEnvClass, jint rcCode
){
  jstring rv;
  const char * z = sqlite3_errstr((int)rcCode);
  if( !z ){
    /* This hypothetically cannot happen, but we'll behave like the
       low-level library would in such a case... */
    z = "unknown error";
  }
  rv = (*env)->NewStringUTF(env, z)
      /* We know these values to be plain ASCII, so pose no MUTF-8
      ** incompatibility */;
  s3jni_oom_check( rv );
  return rv;
}

#ifndef SQLITE_ENABLE_NORMALIZE
/* Dummy stub for sqlite3_normalized_sql(). Never called. */
static const char * sqlite3_normalized_sql(sqlite3_stmt *s){
  S3JniDeclLocal_env;
  (*env)->FatalError(env, "dummy sqlite3_normalized_sql() was "
                     "impossibly called.") /* does not return */;
  return 0;
}
#endif

/*
** Impl for sqlite3_expanded_sql() (if isExpanded is true) and
** sqlite3_normalized_sql().
*/
static jstring s3jni_xn_sql(int isExpanded, JNIEnv *env, jobject jpStmt){
  jstring rv = 0;
  sqlite3_stmt * const pStmt = PtrGet_sqlite3_stmt(jpStmt);

  if( pStmt ){
    char * zSql = isExpanded
      ? sqlite3_expanded_sql(pStmt)
      : (char*)sqlite3_normalized_sql(pStmt);
    s3jni_oom_fatal(zSql);
    if( zSql ){
      rv = s3jni_utf8_to_jstring(zSql, -1);
      if( isExpanded ) sqlite3_free(zSql);
    }
  }
  return rv;
}

S3JniApi(sqlite3_expanded_sql(),jstring,1expanded_1sql)(
  JniArgsEnvClass, jobject jpStmt
){
  return s3jni_xn_sql(1, env, jpStmt);
}

S3JniApi(sqlite3_normalized_sql(),jstring,1normalized_1sql)(
  JniArgsEnvClass, jobject jpStmt
){
#ifdef SQLITE_ENABLE_NORMALIZE
  return s3jni_xn_sql(0, env, jpStmt);
#else
  return 0;
#endif
}

S3JniApi(sqlite3_extended_result_codes(),jint,1extended_1result_1codes)(
  JniArgsEnvClass, jobject jpDb, jboolean onoff
){
  sqlite3 * const pDb = PtrGet_sqlite3(jpDb);
  int const rc = pDb
    ? sqlite3_extended_result_codes(pDb, onoff ? 1 : 0)
    : SQLITE_MISUSE;
  return rc;
}

S3JniApi(sqlite3_finalize(),jint,1finalize)(
  JniArgsEnvClass, jlong jpStmt
){
  return jpStmt
    ? sqlite3_finalize(LongPtrGet_sqlite3_stmt(jpStmt))
    : 0;
}

S3JniApi(sqlite3_get_auxdata(),jobject,1get_1auxdata)(
  JniArgsEnvClass, jobject jCx, jint n
){
  return sqlite3_get_auxdata(PtrGet_sqlite3_context(jCx), (int)n);
}

S3JniApi(sqlite3_initialize(),jint,1initialize)(
  JniArgsEnvClass
){
  return sqlite3_initialize();
}

S3JniApi(sqlite3_interrupt(),void,1interrupt)(
  JniArgsEnvClass, jobject jpDb
){
  sqlite3 * const pDb = PtrGet_sqlite3(jpDb);
  if( pDb ){
    sqlite3_interrupt(pDb);
  }
}

S3JniApi(sqlite3_is_interrupted(),jboolean,1is_1interrupted)(
  JniArgsEnvClass, jobject jpDb
){
  int rc = 0;
  sqlite3 * const pDb = PtrGet_sqlite3(jpDb);
  if( pDb ){
    rc = sqlite3_is_interrupted(pDb);
  }
  return rc ? JNI_TRUE : JNI_FALSE;
}

/*
** Uncaches the current JNIEnv from the S3JniGlobal state, clearing
** any resources owned by that cache entry and making that slot
** available for re-use.
*/
S3JniApi(sqlite3_java_uncache_thread(), jboolean, 1java_1uncache_1thread)(
  JniArgsEnvClass
){
  int rc;
  S3JniEnv_mutex_enter;
  rc = S3JniEnv_uncache(env);
  S3JniEnv_mutex_leave;
  return rc ? JNI_TRUE : JNI_FALSE;
}

S3JniApi(sqlite3_jni_db_error(), jint, 1jni_1db_1error)(
  JniArgsEnvClass, jobject jDb, jint jRc, jstring jStr
){
  S3JniDb * const ps = S3JniDb_from_java(jDb);
  int rc = SQLITE_MISUSE;
  if( ps ){
  char *zStr;
    zStr = jStr
      ? s3jni_jstring_to_utf8( jStr, 0)
      : NULL;
    rc = s3jni_db_error( ps->pDb, (int)jRc, zStr );
    sqlite3_free(zStr);
  }
  return rc;
}

S3JniApi(sqlite3_jni_supports_nio(), jboolean,1jni_1supports_1nio)(
  JniArgsEnvClass
){
  return SJG.g.byteBuffer.klazz ? JNI_TRUE : JNI_FALSE;
}


S3JniApi(sqlite3_keyword_check(),jboolean,1keyword_1check)(
  JniArgsEnvClass, jstring jWord
){
  int nWord = 0;
  char * zWord = s3jni_jstring_to_utf8(jWord, &nWord);
  int rc = 0;

  s3jni_oom_check(jWord ? !!zWord : 1);
  if( zWord && nWord ){
    rc = sqlite3_keyword_check(zWord, nWord);
  }
  sqlite3_free(zWord);
  return rc ? JNI_TRUE : JNI_FALSE;
}

S3JniApi(sqlite3_keyword_name(),jstring,1keyword_1name)(
  JniArgsEnvClass, jint ndx
){
  const char * zWord = 0;
  int n = 0;
  jstring rv = 0;

  if( 0==sqlite3_keyword_name(ndx, &zWord, &n) ){
    rv = s3jni_utf8_to_jstring(zWord, n);
  }
  return rv;
}


S3JniApi(sqlite3_last_insert_rowid(),jlong,1last_1insert_1rowid)(
  JniArgsEnvClass, jobject jpDb
){
  return (jlong)sqlite3_last_insert_rowid(PtrGet_sqlite3(jpDb));
}

S3JniApi(sqlite3_limit(),jint,1limit)(
  JniArgsEnvClass, jobject jpDb, jint id, jint newVal
){
  jint rc = 0;
  sqlite3 * const pDb = PtrGet_sqlite3(jpDb);
  if( pDb ){
    rc = sqlite3_limit( pDb, (int)id, (int)newVal );
  }
  return rc;
}

/* Pre-open() code common to sqlite3_open[_v2](). */
static int s3jni_open_pre(JNIEnv * const env, S3JniEnv **jc,
                          jstring jDbName, char **zDbName,
                          S3JniDb ** ps){
  int rc = 0;
  jobject jDb = 0;

  *jc = S3JniEnv_get();
  if( !*jc ){
    rc = SQLITE_NOMEM;
    goto end;
  }
  *zDbName = jDbName ? s3jni_jstring_to_utf8( jDbName, 0) : 0;
  if( jDbName && !*zDbName ){
    rc = SQLITE_NOMEM;
    goto end;
  }
  jDb = new_java_sqlite3(env, 0);
  if( !jDb ){
    sqlite3_free(*zDbName);
    *zDbName = 0;
    rc = SQLITE_NOMEM;
    goto end;
  }
  *ps = S3JniDb_alloc(env, jDb);
  if( *ps ){
    (*jc)->pdbOpening = *ps;
  }else{
    S3JniUnrefLocal(jDb);
    rc = SQLITE_NOMEM;
  }
end:
  return rc;
}

/*
** Post-open() code common to both the sqlite3_open() and
** sqlite3_open_v2() bindings. ps->jDb must be the
** org.sqlite.jni.capi.sqlite3 object which will hold the db's native
** pointer. theRc must be the result code of the open() op. If
** *ppDb is NULL then ps is set aside and its state cleared,
** else ps is associated with *ppDb. If *ppDb is not NULL then
** ps->jDb is stored in jOut (an OutputPointer.sqlite3 instance).
**
** Must be called if s3jni_open_pre() succeeds and must not be called
** if it doesn't.
**
** Returns theRc.
*/
static int s3jni_open_post(JNIEnv * const env, S3JniEnv * const jc,
                           S3JniDb * ps, sqlite3 **ppDb,
                           jobject jOut, int theRc){
  int rc = 0;
  jc->pdbOpening = 0;
  if( *ppDb ){
    assert(ps->jDb);
    if( 0==ps->pDb ){
      ps->pDb = *ppDb;
      NativePointerHolder_set(S3JniNph(sqlite3), ps->jDb, *ppDb);
    }else{
      assert( ps->pDb==*ppDb
              && "Set up via s3jni_run_java_auto_extensions()" );
    }
    rc = sqlite3_set_clientdata(ps->pDb, S3JniDb_clientdata_key,
                                ps, S3JniDb_xDestroy)
      /* As of here, the Java/C connection is complete */;
  }else{
    S3JniDb_set_aside(ps);
    ps = 0;
  }
  OutputPointer_set_obj(env, S3JniNph(OutputPointer_sqlite3),
                        jOut, ps ? ps->jDb : 0);
  return theRc ? theRc : rc;
}

S3JniApi(sqlite3_open(),jint,1open)(
  JniArgsEnvClass, jstring strName, jobject jOut
){
  sqlite3 * pOut = 0;
  char *zName = 0;
  S3JniDb * ps = 0;
  S3JniEnv * jc = 0;
  int rc;

  if( 0==jOut ) return SQLITE_MISUSE;
  rc = s3jni_open_pre(env, &jc, strName, &zName, &ps);
  if( 0==rc ){
    rc = s3jni_open_post(env, jc, ps, &pOut, jOut,
                         sqlite3_open(zName, &pOut));
    assert(rc==0 ? pOut!=0 : 1);
    sqlite3_free(zName);
  }
  return (jint)rc;
}

S3JniApi(sqlite3_open_v2(),jint,1open_1v2)(
  JniArgsEnvClass, jstring strName,
  jobject jOut, jint flags, jstring strVfs
){
  sqlite3 * pOut = 0;
  char *zName = 0;
  S3JniDb * ps = 0;
  S3JniEnv * jc = 0;
  char *zVfs = 0;
  int rc;

  if( 0==jOut ) return SQLITE_MISUSE;
  rc = s3jni_open_pre(env, &jc, strName, &zName, &ps);
  if( 0==rc ){
    if( strVfs ){
      zVfs = s3jni_jstring_to_utf8( strVfs, 0);
      if( !zVfs ){
        rc = SQLITE_NOMEM;
      }
    }
    if( 0==rc ){
      rc = sqlite3_open_v2(zName, &pOut, (int)flags, zVfs);
    }
    rc = s3jni_open_post(env, jc, ps, &pOut, jOut, rc);
  }
  assert(rc==0 ? pOut!=0 : 1);
  sqlite3_free(zName);
  sqlite3_free(zVfs);
  return (jint)rc;
}

/* Proxy for the sqlite3_prepare[_v2/3]() family. */
static jint sqlite3_jni_prepare_v123( int prepVersion, JNIEnv * const env,
                                      jclass self,
                                      jlong jpDb, jbyteArray baSql,
                                      jint nMax, jint prepFlags,
                                      jobject jOutStmt, jobject outTail){
  sqlite3_stmt * pStmt = 0;
  jobject jStmt = 0;
  const char * zTail = 0;
  sqlite3 * const pDb = LongPtrGet_sqlite3(jpDb);
  jbyte * const pBuf = pDb ? s3jni_jbyteArray_bytes(baSql)  : 0;
  int rc = SQLITE_ERROR;

  assert(prepVersion==1 || prepVersion==2 || prepVersion==3);
  if( !pDb || !jOutStmt ){
    rc = SQLITE_MISUSE;
    goto end;
  }else if( !pBuf ){
    rc = baSql ? SQLITE_NOMEM : SQLITE_MISUSE;
    goto end;
  }
  jStmt = new_java_sqlite3_stmt(env, 0);
  if( !jStmt ){
    rc = SQLITE_NOMEM;
    goto end;
  }
  switch( prepVersion ){
    case 1: rc = sqlite3_prepare(pDb, (const char *)pBuf,
                                 (int)nMax, &pStmt, &zTail);
      break;
    case 2: rc = sqlite3_prepare_v2(pDb, (const char *)pBuf,
                                    (int)nMax, &pStmt, &zTail);
      break;
    case 3: rc = sqlite3_prepare_v3(pDb, (const char *)pBuf,
                                    (int)nMax, (unsigned int)prepFlags,
                                    &pStmt, &zTail);
      break;
    default:
      assert(!"Invalid prepare() version");
  }
end:
  s3jni_jbyteArray_release(baSql,pBuf);
  if( 0==rc ){
    if( 0!=outTail ){
      /* Noting that pBuf is deallocated now but its address is all we need for
      ** what follows... */
      assert(zTail ? ((void*)zTail>=(void*)pBuf) : 1);
      assert(zTail ? (((int)((void*)zTail - (void*)pBuf)) >= 0) : 1);
      OutputPointer_set_Int32(
        env, outTail, (int)(zTail ? (zTail - (const char *)pBuf) : 0)
      );
    }
    if( pStmt ){
      NativePointerHolder_set(S3JniNph(sqlite3_stmt), jStmt, pStmt);
    }else{
      /* Happens for comments and whitespace. */
      S3JniUnrefLocal(jStmt);
      jStmt = 0;
    }
  }else{
    S3JniUnrefLocal(jStmt);
    jStmt = 0;
  }
  if( jOutStmt ){
    OutputPointer_set_obj(env, S3JniNph(OutputPointer_sqlite3_stmt),
                          jOutStmt, jStmt);
  }
  return (jint)rc;
}
S3JniApi(sqlite3_prepare(),jint,1prepare)(
  JNIEnv * const env, jclass self, jlong jpDb, jbyteArray baSql,
                     jint nMax, jobject jOutStmt, jobject outTail
){
  return sqlite3_jni_prepare_v123(1, env, self, jpDb, baSql, nMax, 0,
                                  jOutStmt, outTail);
}
S3JniApi(sqlite3_prepare_v2(),jint,1prepare_1v2)(
  JNIEnv * const env, jclass self, jlong jpDb, jbyteArray baSql,
                         jint nMax, jobject jOutStmt, jobject outTail
){
  return sqlite3_jni_prepare_v123(2, env, self, jpDb, baSql, nMax, 0,
                                  jOutStmt, outTail);
}
S3JniApi(sqlite3_prepare_v3(),jint,1prepare_1v3)(
  JNIEnv * const env, jclass self, jlong jpDb, jbyteArray baSql,
                         jint nMax, jint prepFlags, jobject jOutStmt, jobject outTail
){
  return sqlite3_jni_prepare_v123(3, env, self, jpDb, baSql, nMax,
                                  prepFlags, jOutStmt, outTail);
}

/*
** Impl for C-to-Java of the callbacks for both sqlite3_update_hook()
** and sqlite3_preupdate_hook().  The differences are that for
** update_hook():
**
** - pDb is NULL
** - iKey1 is the row ID
** - iKey2 is unused
*/
static void s3jni_updatepre_hook_impl(void * pState, sqlite3 *pDb, int opId,
                                      const char *zDb, const char *zTable,
                                      sqlite3_int64 iKey1, sqlite3_int64 iKey2){
  S3JniDb * const ps = pState;
  S3JniDeclLocal_env;
  jstring jDbName;
  jstring jTable;
  const int isPre = 0!=pDb;
  S3JniHook hook;

  S3JniHook_localdup(isPre ?
#ifdef SQLITE_ENABLE_PREUPDATE_HOOK
                 &ps->hooks.preUpdate
#else
                 &S3JniHook_empty
#endif
                 : &ps->hooks.update, &hook);
  if( !hook.jObj ){
    return;
  }
  jDbName  = s3jni_utf8_to_jstring( zDb, -1);
  jTable = jDbName ? s3jni_utf8_to_jstring( zTable, -1) : 0;
  S3JniIfThrew {
    S3JniExceptionClear;
    s3jni_db_error(ps->pDb, SQLITE_NOMEM, 0);
  }else{
    assert( hook.jObj );
    assert( hook.midCallback );
    assert( ps->jDb );
#ifdef SQLITE_ENABLE_PREUPDATE_HOOK
    if( isPre ) (*env)->CallVoidMethod(env, hook.jObj, hook.midCallback,
                                       ps->jDb, (jint)opId, jDbName, jTable,
                                       (jlong)iKey1, (jlong)iKey2);
    else
#endif
    (*env)->CallVoidMethod(env, hook.jObj, hook.midCallback,
                           (jint)opId, jDbName, jTable, (jlong)iKey1);
    S3JniIfThrew{
      S3JniExceptionWarnCallbackThrew("sqlite3_(pre)update_hook() callback");
      s3jni_db_exception(ps->pDb, 0,
                         "sqlite3_(pre)update_hook() callback threw");
    }
  }
  S3JniUnrefLocal(jDbName);
  S3JniUnrefLocal(jTable);
  S3JniHook_localundup(hook);
}

#ifdef SQLITE_ENABLE_PREUPDATE_HOOK
static void s3jni_preupdate_hook_impl(void * pState, sqlite3 *pDb, int opId,
                                      const char *zDb, const char *zTable,
                                      sqlite3_int64 iKey1, sqlite3_int64 iKey2){
  return s3jni_updatepre_hook_impl(pState, pDb, opId, zDb, zTable,
                                   iKey1, iKey2);
}
#endif /* SQLITE_ENABLE_PREUPDATE_HOOK */

static void s3jni_update_hook_impl(void * pState, int opId, const char *zDb,
                                   const char *zTable, sqlite3_int64 nRowid){
  return s3jni_updatepre_hook_impl(pState, NULL, opId, zDb, zTable, nRowid, 0);
}

#if !defined(SQLITE_ENABLE_PREUPDATE_HOOK)
/* We need no-op impls for preupdate_{count,depth,blobwrite}() */
S3JniApi(sqlite3_preupdate_blobwrite(),jint,1preupdate_1blobwrite)(
  JniArgsEnvClass, jlong jDb){ return SQLITE_MISUSE; }
S3JniApi(sqlite3_preupdate_count(),jint,1preupdate_1count)(
  JniArgsEnvClass, jlong jDb){ return SQLITE_MISUSE; }
S3JniApi(sqlite3_preupdate_depth(),jint,1preupdate_1depth)(
  JniArgsEnvClass, jlong jDb){ return SQLITE_MISUSE; }
#endif /* !SQLITE_ENABLE_PREUPDATE_HOOK */

/*
** JNI wrapper for both sqlite3_update_hook() and
** sqlite3_preupdate_hook() (if isPre is true).
*/
static jobject s3jni_updatepre_hook(JNIEnv * env, int isPre, jlong jpDb, jobject jHook){
  S3JniDb * const ps = S3JniDb_from_jlong(jpDb);
  jclass klazz;
  jobject pOld = 0;
  jmethodID xCallback;
  S3JniHook * pHook;

  if( !ps ) return 0;
  S3JniDb_mutex_enter;
  pHook = isPre ?
#ifdef SQLITE_ENABLE_PREUPDATE_HOOK
    &ps->hooks.preUpdate
#else
    0
#endif
    : &ps->hooks.update;
  if( !pHook ){
    goto end;
  }
  pOld = pHook->jObj;
  if( pOld && jHook && (*env)->IsSameObject(env, pOld, jHook) ){
    goto end;
  }
  if( !jHook ){
    if( pOld ){
      jobject tmp = S3JniRefLocal(pOld);
      S3JniUnrefGlobal(pOld);
      pOld = tmp;
    }
    *pHook = S3JniHook_empty;
#ifdef SQLITE_ENABLE_PREUPDATE_HOOK
    if( isPre ) sqlite3_preupdate_hook(ps->pDb, 0, 0);
    else
#endif
    sqlite3_update_hook(ps->pDb, 0, 0);
    goto end;
  }
  klazz = (*env)->GetObjectClass(env, jHook);
  xCallback = isPre
    ? (*env)->GetMethodID(env, klazz, "call",
                          "(Lorg/sqlite/jni/capi/sqlite3;"
                          "I"
                          "Ljava/lang/String;"
                          "Ljava/lang/String;"
                          "JJ)V")
    : (*env)->GetMethodID(env, klazz, "call",
                          "(ILjava/lang/String;Ljava/lang/String;J)V");
  S3JniUnrefLocal(klazz);
  S3JniIfThrew {
    S3JniExceptionClear;
    s3jni_db_error(ps->pDb, SQLITE_ERROR,
                   "Cannot not find matching callback on "
                   "(pre)update hook object.");
  }else{
    pHook->midCallback = xCallback;
    pHook->jObj = S3JniRefGlobal(jHook);
#ifdef SQLITE_ENABLE_PREUPDATE_HOOK
    if( isPre ) sqlite3_preupdate_hook(ps->pDb, s3jni_preupdate_hook_impl, ps);
    else
#endif
    sqlite3_update_hook(ps->pDb, s3jni_update_hook_impl, ps);
    if( pOld ){
      jobject tmp = S3JniRefLocal(pOld);
      S3JniUnrefGlobal(pOld);
      pOld = tmp;
    }
  }
end:
  S3JniDb_mutex_leave;
  return pOld;
}


S3JniApi(sqlite3_preupdate_hook(),jobject,1preupdate_1hook)(
  JniArgsEnvClass, jlong jpDb, jobject jHook
){
#ifdef SQLITE_ENABLE_PREUPDATE_HOOK
  return s3jni_updatepre_hook(env, 1, jpDb, jHook);
#else
  return NULL;
#endif /* SQLITE_ENABLE_PREUPDATE_HOOK */
}

/* Impl for sqlite3_preupdate_{new,old}(). */
static int s3jni_preupdate_newold(JNIEnv * const env, int isNew, jlong jpDb,
                                  jint iCol, jobject jOut){
#ifdef SQLITE_ENABLE_PREUPDATE_HOOK
  sqlite3 * const pDb = LongPtrGet_sqlite3(jpDb);
  int rc = SQLITE_MISUSE;
  if( pDb ){
    sqlite3_value * pOut = 0;
    int (*fOrig)(sqlite3*,int,sqlite3_value**) =
      isNew ? sqlite3_preupdate_new : sqlite3_preupdate_old;
    rc = fOrig(pDb, (int)iCol, &pOut);
    if( 0==rc ){
      jobject pWrap = new_java_sqlite3_value(env, pOut);
      if( !pWrap ){
        rc = SQLITE_NOMEM;
      }
      OutputPointer_set_obj(env, S3JniNph(OutputPointer_sqlite3_value),
                            jOut, pWrap);
      S3JniUnrefLocal(pWrap);
    }
  }
  return rc;
#else
  return SQLITE_MISUSE;
#endif
}

S3JniApi(sqlite3_preupdate_new(),jint,1preupdate_1new)(
  JniArgsEnvClass, jlong jpDb, jint iCol, jobject jOut
){
  return s3jni_preupdate_newold(env, 1, jpDb, iCol, jOut);
}

S3JniApi(sqlite3_preupdate_old(),jint,1preupdate_1old)(
  JniArgsEnvClass, jlong jpDb, jint iCol, jobject jOut
){
  return s3jni_preupdate_newold(env, 0, jpDb, iCol, jOut);
}


/* Central C-to-Java sqlite3_progress_handler() proxy. */
static int s3jni_progress_handler_impl(void *pP){
  S3JniDb * const ps = (S3JniDb *)pP;
  int rc = 0;
  S3JniDeclLocal_env;
  S3JniHook hook;

  S3JniHook_localdup(&ps->hooks.progress, &hook);
  if( hook.jObj ){
    rc = (int)(*env)->CallIntMethod(env, hook.jObj, hook.midCallback);
    S3JniIfThrew{
      rc = s3jni_db_exception(ps->pDb, rc,
                              "sqlite3_progress_handler() callback threw");
    }
    S3JniHook_localundup(hook);
  }
  return rc;
}

S3JniApi(sqlite3_progress_handler(),void,1progress_1handler)(
  JniArgsEnvClass,jobject jDb, jint n, jobject jProgress
){
  S3JniDb * const ps = S3JniDb_from_java(jDb);
  S3JniHook * const pHook = ps ? &ps->hooks.progress : 0;

  if( !ps ) return;
  S3JniDb_mutex_enter;
  if( n<1 || !jProgress ){
    S3JniHook_unref(pHook);
    sqlite3_progress_handler(ps->pDb, 0, 0, 0);
  }else{
    jclass const klazz = (*env)->GetObjectClass(env, jProgress);
    jmethodID const xCallback = (*env)->GetMethodID(env, klazz, "call", "()I");
    S3JniUnrefLocal(klazz);
    S3JniIfThrew {
      S3JniExceptionClear;
      s3jni_db_error(ps->pDb, SQLITE_ERROR,
                     "Cannot not find matching xCallback() on "
                     "ProgressHandler object.");
    }else{
      S3JniUnrefGlobal(pHook->jObj);
      pHook->midCallback = xCallback;
      pHook->jObj = S3JniRefGlobal(jProgress);
      sqlite3_progress_handler(ps->pDb, (int)n, s3jni_progress_handler_impl, ps);
    }
  }
  S3JniDb_mutex_leave;
}

S3JniApi(sqlite3_randomness(),void,1randomness)(
  JniArgsEnvClass, jbyteArray jTgt
){
  jbyte * const jba = s3jni_jbyteArray_bytes(jTgt);
  if( jba ){
    jsize const nTgt = (*env)->GetArrayLength(env, jTgt);
    sqlite3_randomness( (int)nTgt, jba );
    s3jni_jbyteArray_commit(jTgt, jba);
  }
}


S3JniApi(sqlite3_reset(),jint,1reset)(
  JniArgsEnvClass, jobject jpStmt
){
  sqlite3_stmt * const pStmt = PtrGet_sqlite3_stmt(jpStmt);
  return pStmt ? sqlite3_reset(pStmt) : SQLITE_MISUSE;
}

/* Clears all entries from S3JniGlobal.autoExt. */
static void s3jni_reset_auto_extension(JNIEnv *env){
  int i;
  S3JniAutoExt_mutex_enter;
  for( i = 0; i < SJG.autoExt.nExt; ++i ){
    S3JniAutoExtension_clear( &SJG.autoExt.aExt[i] );
  }
  SJG.autoExt.nExt = 0;
  S3JniAutoExt_mutex_leave;
}

S3JniApi(sqlite3_reset_auto_extension(),void,1reset_1auto_1extension)(
  JniArgsEnvClass
){
  s3jni_reset_auto_extension(env);
}

/* Impl for sqlite3_result_text/blob() and friends. */
static void result_blob_text(int as64     /* true for text64/blob64() mode */,
                             int eTextRep /* 0 for blobs, else SQLITE_UTF... */,
                             JNIEnv * const env, sqlite3_context *pCx,
                             jbyteArray jBa, jlong nMax){
  int const asBlob = 0==eTextRep;
  if( !pCx ){
    /* We should arguably emit a warning here. But where to log it? */
    return;
  }else if( jBa ){
    jbyte * const pBuf = s3jni_jbyteArray_bytes(jBa);
    jsize nBA = (*env)->GetArrayLength(env, jBa);
    if( nMax>=0 && nBA>(jsize)nMax ){
      nBA = (jsize)nMax;
      /**
         From the sqlite docs:

         > If the 3rd parameter to any of the sqlite3_result_text*
           interfaces other than sqlite3_result_text64() is negative,
           then SQLite computes the string length itself by searching
           the 2nd parameter for the first zero character.

         Note that the text64() interfaces take an unsigned value for
         the length, which Java does not support. This binding takes
         the approach of passing on negative values to the C API,
         which will in turn fail with SQLITE_TOOBIG at some later
         point (recall that the sqlite3_result_xyz() family do not
         have result values).
      */
    }
    if( as64 ){ /* 64-bit... */
      static const jsize nLimit64 =
        SQLITE_MAX_ALLOCATION_SIZE/*only _kinda_ arbitrary*/;
      if( nBA > nLimit64 ){
        sqlite3_result_error_toobig(pCx);
      }else if( asBlob ){
        sqlite3_result_blob64(pCx, pBuf, (sqlite3_uint64)nBA,
                              SQLITE_TRANSIENT);
      }else{ /* text64... */
        if( encodingTypeIsValid(eTextRep) ){
          sqlite3_result_text64(pCx, (const char *)pBuf,
                                (sqlite3_uint64)nBA,
                                SQLITE_TRANSIENT, eTextRep);
        }else{
          sqlite3_result_error_code(pCx, SQLITE_FORMAT);
        }
      }
    }else{ /* 32-bit... */
      static const jsize nLimit = SQLITE_MAX_ALLOCATION_SIZE;
      if( nBA > nLimit ){
        sqlite3_result_error_toobig(pCx);
      }else if( asBlob ){
        sqlite3_result_blob(pCx, pBuf, (int)nBA,
                            SQLITE_TRANSIENT);
      }else{
        switch( eTextRep ){
          case SQLITE_UTF8:
            sqlite3_result_text(pCx, (const char *)pBuf, (int)nBA,
                                SQLITE_TRANSIENT);
            break;
          case SQLITE_UTF16:
            sqlite3_result_text16(pCx, (const char *)pBuf, (int)nBA,
                                  SQLITE_TRANSIENT);
            break;
          case SQLITE_UTF16LE:
            sqlite3_result_text16le(pCx, (const char *)pBuf, (int)nBA,
                                    SQLITE_TRANSIENT);
            break;
          case SQLITE_UTF16BE:
            sqlite3_result_text16be(pCx, (const char *)pBuf, (int)nBA,
                                    SQLITE_TRANSIENT);
            break;
        }
      }
      s3jni_jbyteArray_release(jBa, pBuf);
    }
  }else{
    sqlite3_result_null(pCx);
  }
}

S3JniApi(sqlite3_result_blob(),void,1result_1blob)(
  JniArgsEnvClass, jobject jpCx, jbyteArray jBa, jint nMax
){
  return result_blob_text(0, 0, env, PtrGet_sqlite3_context(jpCx), jBa, nMax);
}

S3JniApi(sqlite3_result_blob64(),void,1result_1blob64)(
  JniArgsEnvClass, jobject jpCx, jbyteArray jBa, jlong nMax
){
  return result_blob_text(1, 0, env, PtrGet_sqlite3_context(jpCx), jBa, nMax);
}

S3JniApi(sqlite3_result_double(),void,1result_1double)(
  JniArgsEnvClass, jobject jpCx, jdouble v
){
  sqlite3_result_double(PtrGet_sqlite3_context(jpCx), v);
}

S3JniApi(sqlite3_result_error(),void,1result_1error)(
  JniArgsEnvClass, jobject jpCx, jbyteArray baMsg, jint eTextRep
){
  const char * zUnspecified = "Unspecified error.";
  jsize const baLen = (*env)->GetArrayLength(env, baMsg);
  jbyte * const pjBuf = baMsg ? s3jni_jbyteArray_bytes(baMsg) : NULL;
  switch( pjBuf ? eTextRep : SQLITE_UTF8 ){
    case SQLITE_UTF8: {
      const char *zMsg = pjBuf ? (const char *)pjBuf : zUnspecified;
      int const n = pjBuf ? (int)baLen : (int)sqlite3Strlen30(zMsg);
      sqlite3_result_error(PtrGet_sqlite3_context(jpCx), zMsg, n);
      break;
    }
    case SQLITE_UTF16: {
      const void *zMsg = pjBuf;
      sqlite3_result_error16(PtrGet_sqlite3_context(jpCx), zMsg, (int)baLen);
      break;
    }
    default:
      sqlite3_result_error(PtrGet_sqlite3_context(jpCx),
                           "Invalid encoding argument passed "
                           "to sqlite3_result_error().", -1);
      break;
  }
  s3jni_jbyteArray_release(baMsg,pjBuf);
}

S3JniApi(sqlite3_result_error_code(),void,1result_1error_1code)(
  JniArgsEnvClass, jobject jpCx, jint v
){
  sqlite3_result_error_code(PtrGet_sqlite3_context(jpCx), (int)v);
}

S3JniApi(sqlite3_result_error_nomem(),void,1result_1error_1nomem)(
  JniArgsEnvClass, jobject jpCx
){
  sqlite3_result_error_nomem(PtrGet_sqlite3_context(jpCx));
}

S3JniApi(sqlite3_result_error_toobig(),void,1result_1error_1toobig)(
  JniArgsEnvClass, jobject jpCx
){
  sqlite3_result_error_toobig(PtrGet_sqlite3_context(jpCx));
}

S3JniApi(sqlite3_result_int(),void,1result_1int)(
  JniArgsEnvClass, jobject jpCx, jint v
){
  sqlite3_result_int(PtrGet_sqlite3_context(jpCx), (int)v);
}

S3JniApi(sqlite3_result_int64(),void,1result_1int64)(
  JniArgsEnvClass, jobject jpCx, jlong v
){
  sqlite3_result_int64(PtrGet_sqlite3_context(jpCx), (sqlite3_int64)v);
}

S3JniApi(sqlite3_result_java_object(),void,1result_1java_1object)(
  JniArgsEnvClass, jobject jpCx, jobject v
){
  sqlite3_context * pCx = PtrGet_sqlite3_context(jpCx);
  if( !pCx ) return;
  else if( v ){
    jobject const rjv = S3JniRefGlobal(v);
    if( rjv ){
      sqlite3_result_pointer(pCx, rjv,
                             s3jni__value_jref_key, S3Jni_jobject_finalizer);
    }else{
      sqlite3_result_error_nomem(PtrGet_sqlite3_context(jpCx));
    }
  }else{
    sqlite3_result_null(PtrGet_sqlite3_context(jpCx));
  }
}

S3JniApi(sqlite3_result_nio_buffer(),void,1result_1nio_1buffer)(
  JniArgsEnvClass, jobject jpCtx, jobject jBuffer,
  jint iOffset, jint iN
){
  sqlite3_context * pCx = PtrGet_sqlite3_context(jpCtx);
  int rc;
  S3JniNioArgs args;
  if( !pCx ){
    return;
  }else if( !SJG.g.byteBuffer.klazz ){
    sqlite3_result_error(
      pCx, "This JVM does not support JNI access to ByteBuffers.", -1
    );
    return;
  }
  rc = s3jni_setup_nio_args(env, &args, jBuffer, iOffset, iN);
  if(rc){
    if( iOffset<0 ){
      sqlite3_result_error(pCx, "Start index may not be negative.", -1);
    }else if( SQLITE_TOOBIG==rc ){
      sqlite3_result_error_toobig(pCx);
    }else{
      sqlite3_result_error(
        pCx, "Invalid arguments to sqlite3_result_nio_buffer().", -1
      );
    }
  }else if( !args.pStart || !args.nOut ){
    sqlite3_result_null(pCx);
  }else{
    sqlite3_result_blob(pCx, args.pStart, args.nOut, SQLITE_TRANSIENT);
  }
}


S3JniApi(sqlite3_result_null(),void,1result_1null)(
  JniArgsEnvClass, jobject jpCx
){
  sqlite3_result_null(PtrGet_sqlite3_context(jpCx));
}

S3JniApi(sqlite3_result_subtype(),void,1result_1subtype)(
  JniArgsEnvClass, jobject jpCx, jint v
){
  sqlite3_result_subtype(PtrGet_sqlite3_context(jpCx), (unsigned int)v);
}


S3JniApi(sqlite3_result_text(),void,1result_1text)(
  JniArgsEnvClass, jobject jpCx, jbyteArray jBa, jint nMax
){
  return result_blob_text(0, SQLITE_UTF8, env,
                          PtrGet_sqlite3_context(jpCx), jBa, nMax);
}

S3JniApi(sqlite3_result_text64(),void,1result_1text64)(
  JniArgsEnvClass, jobject jpCx, jbyteArray jBa, jlong nMax,
                            jint eTextRep
){
  return result_blob_text(1, eTextRep, env,
                          PtrGet_sqlite3_context(jpCx), jBa, nMax);
}

S3JniApi(sqlite3_result_value(),void,1result_1value)(
  JniArgsEnvClass, jobject jpCx, jobject jpSVal
){
  sqlite3_result_value(PtrGet_sqlite3_context(jpCx),
                       PtrGet_sqlite3_value(jpSVal));
}

S3JniApi(sqlite3_result_zeroblob(),void,1result_1zeroblob)(
  JniArgsEnvClass, jobject jpCx, jint v
){
  sqlite3_result_zeroblob(PtrGet_sqlite3_context(jpCx), (int)v);
}

S3JniApi(sqlite3_result_zeroblob64(),jint,1result_1zeroblob64)(
  JniArgsEnvClass, jobject jpCx, jlong v
){
  return (jint)sqlite3_result_zeroblob64(PtrGet_sqlite3_context(jpCx),
                                         (sqlite3_int64)v);
}

S3JniApi(sqlite3_rollback_hook(),jobject,1rollback_1hook)(
  JniArgsEnvClass, jlong jpDb, jobject jHook
){
  return s3jni_commit_rollback_hook(0, env, jpDb, jHook);
}

/* Callback for sqlite3_set_authorizer(). */
int s3jni_xAuth(void* pState, int op,const char*z0, const char*z1,
                const char*z2,const char*z3){
  S3JniDb * const ps = pState;
  S3JniDeclLocal_env;
  S3JniHook hook;
  int rc = 0;

  S3JniHook_localdup(&ps->hooks.auth, &hook );
  if( hook.jObj ){
    jstring const s0 = z0 ? s3jni_utf8_to_jstring( z0, -1) : 0;
    jstring const s1 = z1 ? s3jni_utf8_to_jstring( z1, -1) : 0;
    jstring const s2 = z2 ? s3jni_utf8_to_jstring( z2, -1) : 0;
    jstring const s3 = z3 ? s3jni_utf8_to_jstring( z3, -1) : 0;

    rc = (*env)->CallIntMethod(env, hook.jObj, hook.midCallback, (jint)op,
                               s0, s1, s3, s3);
    S3JniIfThrew{
      rc = s3jni_db_exception(ps->pDb, rc, "sqlite3_set_authorizer() callback");
    }
    S3JniUnrefLocal(s0);
    S3JniUnrefLocal(s1);
    S3JniUnrefLocal(s2);
    S3JniUnrefLocal(s3);
    S3JniHook_localundup(hook);
  }
  return rc;
}

S3JniApi(sqlite3_set_authorizer(),jint,1set_1authorizer)(
  JniArgsEnvClass,jobject jDb, jobject jHook
){
  S3JniDb * const ps = S3JniDb_from_java(jDb);
  S3JniHook * const pHook = ps ? &ps->hooks.auth : 0;
  int rc = 0;

  if( !ps ) return SQLITE_MISUSE;
  S3JniDb_mutex_enter;
  if( !jHook ){
    S3JniHook_unref(pHook);
    rc = sqlite3_set_authorizer( ps->pDb, 0, 0 );
  }else{
    jclass klazz;
    if( pHook->jObj ){
      if( (*env)->IsSameObject(env, pHook->jObj, jHook) ){
      /* Same object - this is a no-op. */
        S3JniDb_mutex_leave;
        return 0;
      }
      S3JniHook_unref(pHook);
    }
    pHook->jObj = S3JniRefGlobal(jHook);
    klazz = (*env)->GetObjectClass(env, jHook);
    pHook->midCallback = (*env)->GetMethodID(env, klazz,
                                             "call",
                                             "(I"
                                             "Ljava/lang/String;"
                                             "Ljava/lang/String;"
                                             "Ljava/lang/String;"
                                             "Ljava/lang/String;"
                                             ")I");
    S3JniUnrefLocal(klazz);
    S3JniIfThrew {
      rc = s3jni_db_error(ps->pDb, SQLITE_ERROR,
                          "Error setting up Java parts of authorizer hook.");
    }else{
      rc = sqlite3_set_authorizer(ps->pDb, s3jni_xAuth, ps);
    }
    if( rc ) S3JniHook_unref(pHook);
  }
  S3JniDb_mutex_leave;
  return rc;
}

S3JniApi(sqlite3_set_auxdata(),void,1set_1auxdata)(
  JniArgsEnvClass, jobject jCx, jint n, jobject jAux
){
  sqlite3_set_auxdata(PtrGet_sqlite3_context(jCx), (int)n,
                      S3JniRefGlobal(jAux), S3Jni_jobject_finalizer);
}

S3JniApi(sqlite3_set_last_insert_rowid(),void,1set_1last_1insert_1rowid)(
  JniArgsEnvClass, jobject jpDb, jlong rowId
){
  sqlite3_set_last_insert_rowid(PtrGet_sqlite3(jpDb),
                                (sqlite3_int64)rowId);
}

S3JniApi(sqlite3_shutdown(),jint,1shutdown)(
  JniArgsEnvClass
){
  s3jni_reset_auto_extension(env);
#ifdef SQLITE_ENABLE_SQLLOG
  S3JniHook_unref(&SJG.hook.sqllog);
#endif
  S3JniHook_unref(&SJG.hook.configlog);
  /* Free up S3JniDb recycling bin. */
  S3JniDb_mutex_enter; {
    while( S3JniGlobal.perDb.aFree ){
      S3JniDb * const d = S3JniGlobal.perDb.aFree;
      S3JniGlobal.perDb.aFree = d->pNext;
      S3JniDb_clear(env, d);
      sqlite3_free(d);
    }
  } S3JniDb_mutex_leave;
  S3JniGlobal_mutex_enter; {
    /* Free up S3JniUdf recycling bin. */
    while( S3JniGlobal.udf.aFree ){
      S3JniUdf * const u = S3JniGlobal.udf.aFree;
      S3JniGlobal.udf.aFree = u->pNext;
      u->pNext = 0;
      S3JniUdf_free(env, u, 0);
    }
  } S3JniGlobal_mutex_leave;
  S3JniHook_mutex_enter; {
    /* Free up S3JniHook recycling bin. */
    while( S3JniGlobal.hook.aFree ){
      S3JniHook * const u = S3JniGlobal.hook.aFree;
      S3JniGlobal.hook.aFree = u->pNext;
      u->pNext = 0;
      assert( !u->doXDestroy );
      assert( !u->jObj );
      assert( !u->jExtra );
      sqlite3_free( u );
    }
  } S3JniHook_mutex_leave;
  /* Free up env cache. */
  S3JniEnv_mutex_enter; {
    while( SJG.envCache.aHead ){
      S3JniEnv_uncache( SJG.envCache.aHead->env );
    }
  } S3JniEnv_mutex_leave;
  /* Do not clear S3JniGlobal.jvm or S3JniGlobal.g: it's legal to
  ** restart the lib. */
  return sqlite3_shutdown();
}

S3JniApi(sqlite3_status(),jint,1status)(
  JniArgsEnvClass, jint op, jobject jOutCurrent, jobject jOutHigh,
                    jboolean reset
){
  int iCur = 0, iHigh = 0;
  int rc = sqlite3_status( op, &iCur, &iHigh, reset );
  if( 0==rc ){
    OutputPointer_set_Int32(env, jOutCurrent, iCur);
    OutputPointer_set_Int32(env, jOutHigh, iHigh);
  }
  return (jint)rc;
}

S3JniApi(sqlite3_status64(),jint,1status64)(
  JniArgsEnvClass, jint op, jobject jOutCurrent, jobject jOutHigh,
                      jboolean reset
){
  sqlite3_int64 iCur = 0, iHigh = 0;
  int rc = sqlite3_status64( op, &iCur, &iHigh, reset );
  if( 0==rc ){
    OutputPointer_set_Int64(env, jOutCurrent, iCur);
    OutputPointer_set_Int64(env, jOutHigh, iHigh);
  }
  return (jint)rc;
}

S3JniApi(sqlite3_stmt_status(),jint,1stmt_1status)(
  JniArgsEnvClass, jobject jStmt, jint op, jboolean reset
){
  return sqlite3_stmt_status(PtrGet_sqlite3_stmt(jStmt),
                             (int)op, reset ? 1 : 0);
}


static int s3jni_strlike_glob(int isLike, JNIEnv *const env,
                              jbyteArray baG, jbyteArray baT, jint escLike){
  int rc = 0;
  jbyte * const pG = s3jni_jbyteArray_bytes(baG);
  jbyte * const pT = s3jni_jbyteArray_bytes(baT);

  /* Note that we're relying on the byte arrays having been
     NUL-terminated on the Java side. */
  rc = isLike
    ? sqlite3_strlike((const char *)pG, (const char *)pT,
                      (unsigned int)escLike)
    : sqlite3_strglob((const char *)pG, (const char *)pT);
  s3jni_jbyteArray_release(baG, pG);
  s3jni_jbyteArray_release(baT, pT);
  return rc;
}

S3JniApi(sqlite3_strglob(),jint,1strglob)(
  JniArgsEnvClass, jbyteArray baG, jbyteArray baT
){
  return s3jni_strlike_glob(0, env, baG, baT, 0);
}

S3JniApi(sqlite3_strlike(),jint,1strlike)(
  JniArgsEnvClass, jbyteArray baG, jbyteArray baT, jint escChar
){
  return s3jni_strlike_glob(1, env, baG, baT, escChar);
}

S3JniApi(sqlite3_sql(),jstring,1sql)(
  JniArgsEnvClass, jobject jpStmt
){
  sqlite3_stmt * const pStmt = PtrGet_sqlite3_stmt(jpStmt);
  jstring rv = 0;
  if( pStmt ){
    const char * zSql = 0;
    zSql = sqlite3_sql(pStmt);
    rv = s3jni_utf8_to_jstring( zSql, -1);
  }
  return rv;
}

S3JniApi(sqlite3_step(),jint,1step)(
  JniArgsEnvClass, jlong jpStmt
){
  sqlite3_stmt * const pStmt = LongPtrGet_sqlite3_stmt(jpStmt);
  return pStmt ? (jint)sqlite3_step(pStmt) : (jint)SQLITE_MISUSE;
}

S3JniApi(sqlite3_table_column_metadata(),jint,1table_1column_1metadata)(
  JniArgsEnvClass, jobject jDb, jstring jDbName, jstring jTableName,
  jstring jColumnName, jobject jDataType, jobject jCollSeq, jobject jNotNull,
  jobject jPrimaryKey, jobject jAutoinc
){
  sqlite3 * const db = PtrGet_sqlite3(jDb);
  char * zDbName = 0, * zTableName = 0, * zColumnName = 0;
  const char * pzCollSeq = 0;
  const char * pzDataType = 0;
  int pNotNull = 0, pPrimaryKey = 0, pAutoinc = 0;
  int rc;

  if( !db || !jDbName || !jTableName ) return SQLITE_MISUSE;
  zDbName = s3jni_jstring_to_utf8(jDbName,0);
  zTableName = zDbName ? s3jni_jstring_to_utf8(jTableName,0) : 0;
  zColumnName = (zTableName && jColumnName)
    ? s3jni_jstring_to_utf8(jColumnName,0) : 0;
  rc = zTableName
    ? sqlite3_table_column_metadata(db, zDbName, zTableName,
                                    zColumnName, &pzDataType, &pzCollSeq,
                                    &pNotNull, &pPrimaryKey, &pAutoinc)
    : SQLITE_NOMEM;
  if( 0==rc ){
    jstring jseq = jCollSeq
      ? (pzCollSeq ? s3jni_utf8_to_jstring(pzCollSeq, -1) : 0)
      : 0;
    jstring jdtype = jDataType
      ? (pzDataType ? s3jni_utf8_to_jstring(pzDataType, -1) : 0)
      : 0;
    if( (jCollSeq && pzCollSeq && !jseq)
        || (jDataType && pzDataType && !jdtype) ){
      rc = SQLITE_NOMEM;
    }else{
      if( jNotNull ) OutputPointer_set_Bool(env, jNotNull, pNotNull);
      if( jPrimaryKey ) OutputPointer_set_Bool(env, jPrimaryKey, pPrimaryKey);
      if( jAutoinc ) OutputPointer_set_Bool(env, jAutoinc, pAutoinc);
      if( jCollSeq ) OutputPointer_set_String(env, jCollSeq, jseq);
      if( jDataType ) OutputPointer_set_String(env, jDataType, jdtype);
    }
    S3JniUnrefLocal(jseq);
    S3JniUnrefLocal(jdtype);
  }
  sqlite3_free(zDbName);
  sqlite3_free(zTableName);
  sqlite3_free(zColumnName);
  return rc;
}

static int s3jni_trace_impl(unsigned traceflag, void *pC, void *pP, void *pX){
  S3JniDb * const ps = (S3JniDb *)pC;
  S3JniDeclLocal_env;
  jobject jX = NULL  /* the tracer's X arg */;
  jobject jP = NULL  /* the tracer's P arg */;
  jobject jPUnref = NULL /* potentially a local ref to jP */;
  int rc = 0;
  S3JniHook hook;

  S3JniHook_localdup(&ps->hooks.trace, &hook );
  if( !hook.jObj ){
    return 0;
  }
  switch( traceflag ){
    case SQLITE_TRACE_STMT:
      jX = s3jni_utf8_to_jstring( (const char *)pX, -1);
      if( !jX ) rc = SQLITE_NOMEM;
      break;
    case SQLITE_TRACE_PROFILE:
      jX = (*env)->NewObject(env, SJG.g.cLong, SJG.g.ctorLong1,
                             (jlong)*((sqlite3_int64*)pX));
      // hmm. ^^^ (*pX) really is zero.
      // MARKER(("profile time = %llu\n", *((sqlite3_int64*)pX)));
      s3jni_oom_check( jX );
      if( !jX ) rc = SQLITE_NOMEM;
      break;
    case SQLITE_TRACE_ROW:
      break;
    case SQLITE_TRACE_CLOSE:
      jP = jPUnref = S3JniRefLocal(ps->jDb);
      break;
    default:
      assert(!"cannot happen - unknown trace flag");
      rc =  SQLITE_ERROR;
  }
  if( 0==rc ){
    if( !jP ){
      /* Create a new temporary sqlite3_stmt wrapper */
      jP = jPUnref = new_java_sqlite3_stmt(env, pP);
      if( !jP ){
        rc = SQLITE_NOMEM;
      }
    }
    if( 0==rc ){
      assert(jP);
      rc = (int)(*env)->CallIntMethod(env, hook.jObj, hook.midCallback,
                                      (jint)traceflag, jP, jX);
      S3JniIfThrew{
        rc = s3jni_db_exception(ps->pDb, SQLITE_ERROR,
                                "sqlite3_trace_v2() callback threw.");
      }
    }
  }
  S3JniUnrefLocal(jPUnref);
  S3JniUnrefLocal(jX);
  S3JniHook_localundup(hook);
  return rc;
}

S3JniApi(sqlite3_trace_v2(),jint,1trace_1v2)(
  JniArgsEnvClass,jobject jDb, jint traceMask, jobject jTracer
){
  S3JniDb * const ps = S3JniDb_from_java(jDb);
  int rc;

  if( !ps ) return SQLITE_MISUSE;
  if( !traceMask || !jTracer ){
    S3JniDb_mutex_enter;
    rc = (jint)sqlite3_trace_v2(ps->pDb, 0, 0, 0);
    S3JniHook_unref(&ps->hooks.trace);
    S3JniDb_mutex_leave;
  }else{
    jclass const klazz = (*env)->GetObjectClass(env, jTracer);
    S3JniHook hook = S3JniHook_empty;
    hook.midCallback = (*env)->GetMethodID(
      env, klazz, "call", "(ILjava/lang/Object;Ljava/lang/Object;)I"
    );
    S3JniUnrefLocal(klazz);
    S3JniIfThrew {
      S3JniExceptionClear;
      rc = s3jni_db_error(ps->pDb, SQLITE_ERROR,
                          "Cannot not find matching call() on "
                          "TracerCallback object.");
    }else{
      hook.jObj = S3JniRefGlobal(jTracer);
      S3JniDb_mutex_enter;
      rc = sqlite3_trace_v2(ps->pDb, (unsigned)traceMask, s3jni_trace_impl, ps);
      if( 0==rc ){
        S3JniHook_unref(&ps->hooks.trace);
        ps->hooks.trace = hook /* transfer ownership of reference */;
      }else{
        S3JniHook_unref(&hook);
      }
      S3JniDb_mutex_leave;
    }
  }
  return rc;
}

S3JniApi(sqlite3_txn_state(),jint,1txn_1state)(
  JniArgsEnvClass,jobject jDb, jstring jSchema
){
  sqlite3 * const pDb = PtrGet_sqlite3(jDb);
  int rc = SQLITE_MISUSE;
  if( pDb ){
    char * zSchema = jSchema
      ? s3jni_jstring_to_utf8(jSchema, 0)
      : 0;
    if( !jSchema || (zSchema && jSchema) ){
      rc = sqlite3_txn_state(pDb, zSchema);
      sqlite3_free(zSchema);
    }else{
      rc = SQLITE_NOMEM;
    }
  }
  return rc;
}

S3JniApi(sqlite3_update_hook(),jobject,1update_1hook)(
  JniArgsEnvClass, jlong jpDb, jobject jHook
){
  return s3jni_updatepre_hook(env, 0, jpDb, jHook);
}


S3JniApi(sqlite3_value_blob(),jbyteArray,1value_1blob)(
  JniArgsEnvClass, jlong jpSVal
){
  sqlite3_value * const sv = LongPtrGet_sqlite3_value(jpSVal);
  const jbyte * pBytes = sv ? sqlite3_value_blob(sv) : 0;
  int const nLen = pBytes ? sqlite3_value_bytes(sv) : 0;

  s3jni_oom_check( nLen ? !!pBytes : 1 );
  return pBytes
    ? s3jni_new_jbyteArray(pBytes, nLen)
    : NULL;
}

S3JniApi(sqlite3_value_bytes(),jint,1value_1bytes)(
  JniArgsEnvClass, jlong jpSVal
){
  sqlite3_value * const sv = LongPtrGet_sqlite3_value(jpSVal);
  return sv ? sqlite3_value_bytes(sv) : 0;
}

S3JniApi(sqlite3_value_bytes16(),jint,1value_1bytes16)(
  JniArgsEnvClass, jlong jpSVal
){
  sqlite3_value * const sv = LongPtrGet_sqlite3_value(jpSVal);
  return sv ? sqlite3_value_bytes16(sv) : 0;
}


S3JniApi(sqlite3_value_double(),jdouble,1value_1double)(
  JniArgsEnvClass, jlong jpSVal
){
  sqlite3_value * const sv = LongPtrGet_sqlite3_value(jpSVal);
  return (jdouble) (sv ? sqlite3_value_double(sv) : 0.0);
}


S3JniApi(sqlite3_value_dup(),jobject,1value_1dup)(
  JniArgsEnvClass, jlong jpSVal
){
  sqlite3_value * const sv = LongPtrGet_sqlite3_value(jpSVal);
  sqlite3_value * const sd = sv ? sqlite3_value_dup(sv) : 0;
  jobject rv = sd ? new_java_sqlite3_value(env, sd) : 0;
  if( sd && !rv ) {
    /* OOM */
    sqlite3_value_free(sd);
  }
  return rv;
}

S3JniApi(sqlite3_value_free(),void,1value_1free)(
  JniArgsEnvClass, jlong jpSVal
){
  sqlite3_value * const sv = LongPtrGet_sqlite3_value(jpSVal);
  if( sv ){
    sqlite3_value_free(sv);
  }
}

S3JniApi(sqlite3_value_int(),jint,1value_1int)(
  JniArgsEnvClass, jlong jpSVal
){
  sqlite3_value * const sv = LongPtrGet_sqlite3_value(jpSVal);
  return (jint) (sv ? sqlite3_value_int(sv) : 0);
}

S3JniApi(sqlite3_value_int64(),jlong,1value_1int64)(
  JniArgsEnvClass, jlong jpSVal
){
  sqlite3_value * const sv = LongPtrGet_sqlite3_value(jpSVal);
  return (jlong) (sv ? sqlite3_value_int64(sv) : 0LL);
}

S3JniApi(sqlite3_value_java_object(),jobject,1value_1java_1object)(
  JniArgsEnvClass, jlong jpSVal
){
  sqlite3_value * const sv = LongPtrGet_sqlite3_value(jpSVal);
  return sv
    ? sqlite3_value_pointer(sv, s3jni__value_jref_key)
    : 0;
}

S3JniApi(sqlite3_value_nio_buffer(),jobject,1value_1nio_1buffer)(
  JniArgsEnvClass, jobject jVal
){
  sqlite3_value * const sv = PtrGet_sqlite3_value(jVal);
  jobject rv = 0;
  if( sv ){
    const void * const p = sqlite3_value_blob(sv);
    if( p ){
      const int n = sqlite3_value_bytes(sv);
      rv = s3jni__blob_to_ByteBuffer(env, p, n);
    }
  }
  return rv;
}

S3JniApi(sqlite3_value_text(),jbyteArray,1value_1text)(
  JniArgsEnvClass, jlong jpSVal
){
  sqlite3_value * const sv = LongPtrGet_sqlite3_value(jpSVal);
  const unsigned char * const p = sv ? sqlite3_value_text(sv) : 0;
  int const n = p ? sqlite3_value_bytes(sv) : 0;
  return p ? s3jni_new_jbyteArray(p, n) : 0;
}

#if 0
// this impl might prove useful.
S3JniApi(sqlite3_value_text(),jstring,1value_1text)(
  JniArgsEnvClass, jlong jpSVal
){
  sqlite3_value * const sv = LongPtrGet_sqlite3_value(jpSVal);
  const unsigned char * const p = sv ? sqlite3_value_text(sv) : 0;
  int const n = p ? sqlite3_value_bytes(sv) : 0;
  return p ? s3jni_utf8_to_jstring( (const char *)p, n) : 0;
}
#endif

S3JniApi(sqlite3_value_text16(),jstring,1value_1text16)(
  JniArgsEnvClass, jlong jpSVal
){
  sqlite3_value * const sv = LongPtrGet_sqlite3_value(jpSVal);
  const int n = sv ? sqlite3_value_bytes16(sv) : 0;
  const void * const p = sv ? sqlite3_value_text16(sv) : 0;
  return p ? s3jni_text16_to_jstring(env, p, n) : 0;
}

JniDecl(void,1jni_1internal_1details)(JniArgsEnvClass){
  MARKER(("\nVarious bits of internal info:\n"));
  puts("FTS5 is "
#ifdef SQLITE_ENABLE_FTS5
       "available"
#else
       "unavailable"
#endif
       "."
       );
  puts("sizeofs:");
#define SO(T) printf("\tsizeof(" #T ") = %u\n", (unsigned)sizeof(T))
  SO(void*);
  SO(jmethodID);
  SO(jfieldID);
  SO(S3JniEnv);
  SO(S3JniHook);
  SO(S3JniDb);
  SO(S3JniNphOps);
  printf("\t(^^^ %u NativePointerHolder/OutputPointer.T types)\n",
         (unsigned)S3Jni_NphCache_size);
  SO(S3JniGlobal);
  SO(S3JniGlobal.nph);
  SO(S3JniGlobal.metrics);
  SO(S3JniAutoExtension);
  SO(S3JniUdf);
#undef SO
#ifdef SQLITE_JNI_ENABLE_METRICS
  printf("Cache info:\n");
  printf("\tJNIEnv cache: %u allocs, %u misses, %u hits\n",
         SJG.metrics.nEnvAlloc, SJG.metrics.nEnvMiss,
         SJG.metrics.nEnvHit);
  printf("Mutex entry:"
         "\n\tglobal       = %u"
         "\n\tenv          = %u"
         "\n\tnph          = %u for S3JniNphOp init"
         "\n\thook         = %u"
         "\n\tperDb        = %u"
         "\n\tautoExt list = %u"
         "\n\tS3JniUdf     = %u (free-list)"
         "\n\tmetrics      = %u\n",
         SJG.metrics.nMutexGlobal, SJG.metrics.nMutexEnv,
         SJG.metrics.nMutexNph, SJG.metrics.nMutexHook,
         SJG.metrics.nMutexPerDb, SJG.metrics.nMutexAutoExt,
         SJG.metrics.nMutexUdf, SJG.metrics.nMetrics);
  puts("Allocs:");
  printf("\tS3JniDb:  %u alloced (*%u = %u bytes), %u recycled\n",
         SJG.metrics.nPdbAlloc, (unsigned) sizeof(S3JniDb),
         (unsigned)(SJG.metrics.nPdbAlloc * sizeof(S3JniDb)),
         SJG.metrics.nPdbRecycled);
  printf("\tS3JniUdf: %u alloced (*%u = %u bytes), %u recycled\n",
         SJG.metrics.nUdfAlloc, (unsigned) sizeof(S3JniUdf),
         (unsigned)(SJG.metrics.nUdfAlloc * sizeof(S3JniUdf)),
         SJG.metrics.nUdfRecycled);
  printf("\tS3JniHook: %u alloced (*%u = %u bytes), %u recycled\n",
         SJG.metrics.nHookAlloc, (unsigned) sizeof(S3JniHook),
         (unsigned)(SJG.metrics.nHookAlloc * sizeof(S3JniHook)),
         SJG.metrics.nHookRecycled);
  printf("\tS3JniEnv: %u alloced (*%u = %u bytes)\n",
         SJG.metrics.nEnvAlloc, (unsigned) sizeof(S3JniEnv),
         (unsigned)(SJG.metrics.nEnvAlloc * sizeof(S3JniEnv)));
  puts("Java-side UDF calls:");
#define UDF(T) printf("\t%-8s = %u\n", "x" #T, SJG.metrics.udf.n##T)
  UDF(Func); UDF(Step); UDF(Final); UDF(Value); UDF(Inverse);
#undef UDF
  printf("xDestroy calls across all callback types: %u\n",
         SJG.metrics.nDestroy);
#else
  puts("Built without SQLITE_JNI_ENABLE_METRICS.");
#endif
}

////////////////////////////////////////////////////////////////////////
// End of the sqlite3_... API bindings. Next up, FTS5...
////////////////////////////////////////////////////////////////////////
#ifdef SQLITE_ENABLE_FTS5

/* Creates a verbose JNI Fts5 function name. */
#define JniFuncNameFtsXA(Suffix)                  \
  Java_org_sqlite_jni_fts5_Fts5ExtensionApi_ ## Suffix
#define JniFuncNameFtsApi(Suffix)                  \
  Java_org_sqlite_jni_fts5_fts5_1api_ ## Suffix
#define JniFuncNameFtsTok(Suffix)                  \
  Java_org_sqlite_jni_fts5_fts5_tokenizer_ ## Suffix

#define JniDeclFtsXA(ReturnType,Suffix)           \
  JNIEXPORT ReturnType JNICALL                  \
  JniFuncNameFtsXA(Suffix)
#define JniDeclFtsApi(ReturnType,Suffix)          \
  JNIEXPORT ReturnType JNICALL                  \
  JniFuncNameFtsApi(Suffix)
#define JniDeclFtsTok(ReturnType,Suffix)          \
  JNIEXPORT ReturnType JNICALL                  \
  JniFuncNameFtsTok(Suffix)

#define PtrGet_fts5_api(OBJ) NativePointerHolder_get(OBJ,S3JniNph(fts5_api))
#define PtrGet_fts5_tokenizer(OBJ) NativePointerHolder_get(OBJ,S3JniNph(fts5_tokenizer))
#define PtrGet_Fts5Context(OBJ) NativePointerHolder_get(OBJ,S3JniNph(Fts5Context))
#define PtrGet_Fts5Tokenizer(OBJ) NativePointerHolder_get(OBJ,S3JniNph(Fts5Tokenizer))
#define s3jni_ftsext() &sFts5Api/*singleton from sqlite3.c*/
#define Fts5ExtDecl Fts5ExtensionApi const * const ext = s3jni_ftsext()

/**
   State for binding Java-side FTS5 auxiliary functions.
*/
typedef struct {
  jobject jObj          /* functor instance */;
  jobject jUserData     /* 2nd arg to JNI binding of
                           xCreateFunction(), ostensibly the 3rd arg
                           to the lib-level xCreateFunction(), except
                           that we necessarily use that slot for a
                           Fts5JniAux instance. */;
  char * zFuncName      /* Only for error reporting and debug logging */;
  jmethodID jmid        /* callback member's method ID */;
} Fts5JniAux;

static void Fts5JniAux_free(Fts5JniAux * const s){
  S3JniDeclLocal_env;
  if( env ){
    /*MARKER(("FTS5 aux function cleanup: %s\n", s->zFuncName));*/
    s3jni_call_xDestroy(s->jObj);
    S3JniUnrefGlobal(s->jObj);
    S3JniUnrefGlobal(s->jUserData);
  }
  sqlite3_free(s->zFuncName);
  sqlite3_free(s);
}

static void Fts5JniAux_xDestroy(void *p){
  if( p ) Fts5JniAux_free(p);
}

static Fts5JniAux * Fts5JniAux_alloc(JNIEnv * const env, jobject jObj){
  Fts5JniAux * s = s3jni_malloc( sizeof(Fts5JniAux));

  if( s ){
    jclass klazz;
    memset(s, 0, sizeof(Fts5JniAux));
    s->jObj = S3JniRefGlobal(jObj);
    klazz = (*env)->GetObjectClass(env, jObj);
    s->jmid = (*env)->GetMethodID(env, klazz, "call",
                                  "(Lorg/sqlite/jni/fts5/Fts5ExtensionApi;"
                                  "Lorg/sqlite/jni/fts5/Fts5Context;"
                                  "Lorg/sqlite/jni/capi/sqlite3_context;"
                                  "[Lorg/sqlite/jni/capi/sqlite3_value;)V");
    S3JniUnrefLocal(klazz);
    S3JniIfThrew{
      S3JniExceptionReport;
      S3JniExceptionClear;
      Fts5JniAux_free(s);
      s = 0;
    }
  }
  return s;
}

static inline jobject new_java_Fts5Context(JNIEnv * const env, Fts5Context *sv){
  return NativePointerHolder_new(env, S3JniNph(Fts5Context), sv);
}
static inline jobject new_java_fts5_api(JNIEnv * const env, fts5_api *sv){
  return NativePointerHolder_new(env, S3JniNph(fts5_api), sv);
}

/*
** Returns a per-JNIEnv global ref to the Fts5ExtensionApi singleton
** instance, or NULL on OOM.
*/
static jobject s3jni_getFts5ExensionApi(JNIEnv * const env){
  if( !SJG.fts5.jExt ){
    S3JniGlobal_mutex_enter;
    if( !SJG.fts5.jExt ){
      jobject const pNPH = NativePointerHolder_new(
        env, S3JniNph(Fts5ExtensionApi), s3jni_ftsext()
      );
      if( pNPH ){
        SJG.fts5.jExt = S3JniRefGlobal(pNPH);
        S3JniUnrefLocal(pNPH);
      }
    }
    S3JniGlobal_mutex_leave;
  }
  return SJG.fts5.jExt;
}

/*
** Returns a pointer to the fts5_api instance for database connection
** db.  If an error occurs, returns NULL and leaves an error in the
** database handle (accessible using sqlite3_errcode()/errmsg()).
*/
static fts5_api *s3jni_fts5_api_from_db(sqlite3 *db){
  fts5_api *pRet = 0;
  sqlite3_stmt *pStmt = 0;
  if( SQLITE_OK==sqlite3_prepare(db, "SELECT fts5(?1)", -1, &pStmt, 0) ){
    sqlite3_bind_pointer(pStmt, 1, (void*)&pRet, "fts5_api_ptr", NULL);
    sqlite3_step(pStmt);
  }
  sqlite3_finalize(pStmt);
  return pRet;
}

JniDeclFtsApi(jobject,getInstanceForDb)(JniArgsEnvClass,jobject jDb){
  S3JniDb * const ps = S3JniDb_from_java(jDb);
#if 0
  jobject rv = 0;
  if( !ps ) return 0;
  else if( ps->fts.jApi ){
    rv = ps->fts.jApi;
  }else{
    fts5_api * const pApi = s3jni_fts5_api_from_db(ps->pDb);
    if( pApi ){
      rv = new_java_fts5_api(env, pApi);
      ps->fts.jApi = rv ? S3JniRefGlobal(rv) : 0;
    }
  }
  return rv;
#else
  if( ps && !ps->fts.jApi ){
    S3JniDb_mutex_enter;
    if( !ps->fts.jApi ){
      fts5_api * const pApi = s3jni_fts5_api_from_db(ps->pDb);
      if( pApi ){
        jobject const rv = new_java_fts5_api(env, pApi);
        ps->fts.jApi = rv ? S3JniRefGlobal(rv) : 0;
      }
    }
    S3JniDb_mutex_leave;
  }
  return ps ? ps->fts.jApi : 0;
#endif
}


JniDeclFtsXA(jobject,getInstance)(JniArgsEnvClass){
  return s3jni_getFts5ExensionApi(env);
}

JniDeclFtsXA(jint,xColumnCount)(JniArgsEnvObj,jobject jCtx){
  Fts5ExtDecl;
  return (jint)ext->xColumnCount(PtrGet_Fts5Context(jCtx));
}

JniDeclFtsXA(jint,xColumnSize)(JniArgsEnvObj,jobject jCtx, jint iIdx, jobject jOut32){
  Fts5ExtDecl;
  int n1 = 0;
  int const rc = ext->xColumnSize(PtrGet_Fts5Context(jCtx), (int)iIdx, &n1);
  if( 0==rc ) OutputPointer_set_Int32(env, jOut32, n1);
  return rc;
}

JniDeclFtsXA(jint,xColumnText)(JniArgsEnvObj,jobject jCtx, jint iCol,
                           jobject jOut){
  Fts5ExtDecl;
  const char *pz = 0;
  int pn = 0;
  int rc = ext->xColumnText(PtrGet_Fts5Context(jCtx), (int)iCol,
                             &pz, &pn);
  if( 0==rc ){
    jstring jstr = pz ? s3jni_utf8_to_jstring( pz, pn) : 0;
    if( pz ){
      if( jstr ){
        OutputPointer_set_String(env, jOut, jstr);
        S3JniUnrefLocal(jstr)/*jOut has a reference*/;
      }else{
        rc = SQLITE_NOMEM;
      }
    }
  }
  return (jint)rc;
}

JniDeclFtsXA(jint,xColumnTotalSize)(JniArgsEnvObj,jobject jCtx, jint iCol, jobject jOut64){
  Fts5ExtDecl;
  sqlite3_int64 nOut = 0;
  int const rc = ext->xColumnTotalSize(PtrGet_Fts5Context(jCtx), (int)iCol, &nOut);
  if( 0==rc && jOut64 ) OutputPointer_set_Int64(env, jOut64, (jlong)nOut);
  return (jint)rc;
}

/*
** Proxy for fts5_extension_function instances plugged in via
** fts5_api::xCreateFunction().
*/
static void s3jni_fts5_extension_function(Fts5ExtensionApi const *pApi,
                                          Fts5Context *pFts,
                                          sqlite3_context *pCx,
                                          int argc,
                                          sqlite3_value **argv){
  Fts5JniAux * const pAux = pApi->xUserData(pFts);
  jobject jpCx = 0;
  jobjectArray jArgv = 0;
  jobject jpFts = 0;
  jobject jFXA;
  int rc;
  S3JniDeclLocal_env;

  assert(pAux);
  jFXA = s3jni_getFts5ExensionApi(env);
  if( !jFXA ) goto error_oom;
  jpFts = new_java_Fts5Context(env, pFts);
  if( !jpFts ) goto error_oom;
  rc = udf_args(env, pCx, argc, argv, &jpCx, &jArgv);
  if( rc ) goto error_oom;
  (*env)->CallVoidMethod(env, pAux->jObj, pAux->jmid,
                         jFXA, jpFts, jpCx, jArgv);
  S3JniIfThrew{
    udf_report_exception(env, 1, pCx, pAux->zFuncName, "call");
  }
  udf_unargs(env, jpCx, argc, jArgv);
  S3JniUnrefLocal(jpFts);
  S3JniUnrefLocal(jpCx);
  S3JniUnrefLocal(jArgv);
  return;
error_oom:
  s3jni_db_oom( sqlite3_context_db_handle(pCx) );
  assert( !jArgv );
  assert( !jpCx );
  S3JniUnrefLocal(jpFts);
  sqlite3_result_error_nomem(pCx);
  return;
}

JniDeclFtsApi(jint,xCreateFunction)(JniArgsEnvObj, jstring jName,
                                  jobject jUserData, jobject jFunc){
  fts5_api * const pApi = PtrGet_fts5_api(jSelf);
  int rc;
  char * zName;
  Fts5JniAux * pAux;

  assert(pApi);
  zName = s3jni_jstring_to_utf8( jName, 0);
  if(!zName) return SQLITE_NOMEM;
  pAux = Fts5JniAux_alloc(env, jFunc);
  if( pAux ){
    rc = pApi->xCreateFunction(pApi, zName, pAux,
                               s3jni_fts5_extension_function,
                               Fts5JniAux_xDestroy);
  }else{
    rc = SQLITE_NOMEM;
  }
  if( 0==rc ){
    pAux->jUserData = jUserData ? S3JniRefGlobal(jUserData) : 0;
    pAux->zFuncName = zName;
  }else{
    sqlite3_free(zName);
  }
  return (jint)rc;
}


typedef struct S3JniFts5AuxData S3JniFts5AuxData;
/*
** TODO: this middle-man struct is no longer necessary. Conider
** removing it and passing around jObj itself instead.
*/
struct S3JniFts5AuxData {
  jobject jObj;
};

static void S3JniFts5AuxData_xDestroy(void *x){
  if( x ){
    S3JniFts5AuxData * const p = x;
    if( p->jObj ){
      S3JniDeclLocal_env;
      s3jni_call_xDestroy(p->jObj);
      S3JniUnrefGlobal(p->jObj);
    }
    sqlite3_free(x);
  }
}

JniDeclFtsXA(jobject,xGetAuxdata)(JniArgsEnvObj,jobject jCtx, jboolean bClear){
  Fts5ExtDecl;
  jobject rv = 0;
  S3JniFts5AuxData * const pAux = ext->xGetAuxdata(PtrGet_Fts5Context(jCtx), bClear);
  if( pAux ){
    if( bClear ){
      if( pAux->jObj ){
        rv = S3JniRefLocal(pAux->jObj);
        S3JniUnrefGlobal(pAux->jObj);
      }
      /* Note that we do not call xDestroy() in this case. */
      sqlite3_free(pAux);
    }else{
      rv = pAux->jObj;
    }
  }
  return rv;
}

JniDeclFtsXA(jint,xInst)(JniArgsEnvObj,jobject jCtx, jint iIdx, jobject jOutPhrase,
                    jobject jOutCol, jobject jOutOff){
  Fts5ExtDecl;
  int n1 = 0, n2 = 2, n3 = 0;
  int const rc = ext->xInst(PtrGet_Fts5Context(jCtx), (int)iIdx, &n1, &n2, &n3);
  if( 0==rc ){
    OutputPointer_set_Int32(env, jOutPhrase, n1);
    OutputPointer_set_Int32(env, jOutCol, n2);
    OutputPointer_set_Int32(env, jOutOff, n3);
  }
  return rc;
}

JniDeclFtsXA(jint,xInstCount)(JniArgsEnvObj,jobject jCtx, jobject jOut32){
  Fts5ExtDecl;
  int nOut = 0;
  int const rc = ext->xInstCount(PtrGet_Fts5Context(jCtx), &nOut);
  if( 0==rc && jOut32 ) OutputPointer_set_Int32(env, jOut32, nOut);
  return (jint)rc;
}

JniDeclFtsXA(jint,xPhraseCount)(JniArgsEnvObj,jobject jCtx){
  Fts5ExtDecl;
  return (jint)ext->xPhraseCount(PtrGet_Fts5Context(jCtx));
}

/* Copy the 'a' and 'b' fields from pSrc to Fts5PhraseIter object jIter. */
static void s3jni_phraseIter_NToJ(JNIEnv *const env,
                                  Fts5PhraseIter const * const pSrc,
                                  jobject jIter){
  S3JniGlobalType * const g = &S3JniGlobal;
  assert(g->fts5.jPhraseIter.fidA);
  (*env)->SetLongField(env, jIter, g->fts5.jPhraseIter.fidA,
                       S3JniCast_P2L(pSrc->a));
  S3JniExceptionIsFatal("Cannot set Fts5PhraseIter.a field.");
  (*env)->SetLongField(env, jIter, g->fts5.jPhraseIter.fidB,
                       S3JniCast_P2L(pSrc->b));
  S3JniExceptionIsFatal("Cannot set Fts5PhraseIter.b field.");
}

/* Copy the 'a' and 'b' fields from Fts5PhraseIter object jIter to pDest. */
static void s3jni_phraseIter_JToN(JNIEnv *const env,  jobject jIter,
                                  Fts5PhraseIter * const pDest){
  S3JniGlobalType * const g = &S3JniGlobal;
  assert(g->fts5.jPhraseIter.fidA);
  pDest->a = S3JniCast_L2P(
    (*env)->GetLongField(env, jIter, g->fts5.jPhraseIter.fidA)
  );
  S3JniExceptionIsFatal("Cannot get Fts5PhraseIter.a field.");
  pDest->b = S3JniCast_L2P(
    (*env)->GetLongField(env, jIter, g->fts5.jPhraseIter.fidB)
  );
  S3JniExceptionIsFatal("Cannot get Fts5PhraseIter.b field.");
}

JniDeclFtsXA(jint,xPhraseFirst)(JniArgsEnvObj,jobject jCtx, jint iPhrase,
                            jobject jIter, jobject jOutCol,
                            jobject jOutOff){
  Fts5ExtDecl;
  Fts5PhraseIter iter;
  int rc, iCol = 0, iOff = 0;
  rc = ext->xPhraseFirst(PtrGet_Fts5Context(jCtx), (int)iPhrase,
                         &iter, &iCol, &iOff);
  if( 0==rc ){
    OutputPointer_set_Int32(env, jOutCol, iCol);
    OutputPointer_set_Int32(env, jOutOff, iOff);
    s3jni_phraseIter_NToJ(env, &iter, jIter);
  }
  return rc;
}

JniDeclFtsXA(jint,xPhraseFirstColumn)(JniArgsEnvObj,jobject jCtx, jint iPhrase,
                                  jobject jIter, jobject jOutCol){
  Fts5ExtDecl;
  Fts5PhraseIter iter;
  int rc, iCol = 0;
  rc = ext->xPhraseFirstColumn(PtrGet_Fts5Context(jCtx), (int)iPhrase,
                                &iter, &iCol);
  if( 0==rc ){
    OutputPointer_set_Int32(env, jOutCol, iCol);
    s3jni_phraseIter_NToJ(env, &iter, jIter);
  }
  return rc;
}

JniDeclFtsXA(void,xPhraseNext)(JniArgsEnvObj,jobject jCtx, jobject jIter,
                           jobject jOutCol, jobject jOutOff){
  Fts5ExtDecl;
  Fts5PhraseIter iter;
  int iCol = 0, iOff = 0;
  s3jni_phraseIter_JToN(env, jIter, &iter);
  ext->xPhraseNext(PtrGet_Fts5Context(jCtx), &iter, &iCol, &iOff);
  OutputPointer_set_Int32(env, jOutCol, iCol);
  OutputPointer_set_Int32(env, jOutOff, iOff);
  s3jni_phraseIter_NToJ(env, &iter, jIter);
}

JniDeclFtsXA(void,xPhraseNextColumn)(JniArgsEnvObj,jobject jCtx, jobject jIter,
                                 jobject jOutCol){
  Fts5ExtDecl;
  Fts5PhraseIter iter;
  int iCol = 0;
  s3jni_phraseIter_JToN(env, jIter, &iter);
  ext->xPhraseNextColumn(PtrGet_Fts5Context(jCtx), &iter, &iCol);
  OutputPointer_set_Int32(env, jOutCol, iCol);
  s3jni_phraseIter_NToJ(env, &iter, jIter);
}


JniDeclFtsXA(jint,xPhraseSize)(JniArgsEnvObj,jobject jCtx, jint iPhrase){
  Fts5ExtDecl;
  return (jint)ext->xPhraseSize(PtrGet_Fts5Context(jCtx), (int)iPhrase);
}

/* State for use with xQueryPhrase() and xTokenize(). */
struct s3jni_xQueryPhraseState {
  Fts5ExtensionApi const * ext;
  jmethodID midCallback; /* jCallback->call() method */
  jobject jCallback;   /* Fts5ExtensionApi.XQueryPhraseCallback instance */
  jobject jFcx;        /* (Fts5Context*) for xQueryPhrase()
                          callback. This is NOT the instance that is
                          passed to xQueryPhrase(), it's the one
                          created by xQueryPhrase() for use by its
                          callback. */
  /* State for xTokenize() */
  struct {
    const char * zPrev;
    int nPrev;
    jbyteArray jba;
  } tok;
};

static int s3jni_xQueryPhrase(const Fts5ExtensionApi *xapi,
                              Fts5Context * pFcx, void *pData){
  struct s3jni_xQueryPhraseState * const s = pData;
  S3JniDeclLocal_env;

  if( !s->jFcx ){
    s->jFcx = new_java_Fts5Context(env, pFcx);
    if( !s->jFcx ) return SQLITE_NOMEM;
  }
  int rc = (int)(*env)->CallIntMethod(env, s->jCallback, s->midCallback,
                                      SJG.fts5.jExt, s->jFcx);
  S3JniIfThrew{
    S3JniExceptionWarnCallbackThrew("xQueryPhrase() callback");
    S3JniExceptionClear;
    rc = SQLITE_ERROR;
  }
  return rc;
}

JniDeclFtsXA(jint,xQueryPhrase)(JniArgsEnvObj,jobject jFcx, jint iPhrase,
                            jobject jCallback){
  Fts5ExtDecl;
  int rc;
  struct s3jni_xQueryPhraseState s;
  jclass klazz = jCallback ? (*env)->GetObjectClass(env, jCallback) : NULL;

  if( !klazz ) return SQLITE_MISUSE;
  s.jCallback = jCallback;
  s.jFcx = 0;
  s.ext = ext;
  s.midCallback = (*env)->GetMethodID(env, klazz, "call",
                                      "(Lorg/sqlite/jni/fts5/Fts5ExtensionApi;"
                                      "Lorg/sqlite/jni/fts5/Fts5Context;)I");
  S3JniUnrefLocal(klazz);
  S3JniExceptionIsFatal("Could not extract xQueryPhraseCallback.call() method.");
  rc = ext->xQueryPhrase(PtrGet_Fts5Context(jFcx), iPhrase, &s,
                         s3jni_xQueryPhrase);
  S3JniUnrefLocal(s.jFcx);
  return (jint)rc;
}


JniDeclFtsXA(jint,xRowCount)(JniArgsEnvObj,jobject jCtx, jobject jOut64){
  Fts5ExtDecl;
  sqlite3_int64 nOut = 0;
  int const rc = ext->xRowCount(PtrGet_Fts5Context(jCtx), &nOut);
  if( 0==rc && jOut64 ) OutputPointer_set_Int64(env, jOut64, (jlong)nOut);
  return (jint)rc;
}

JniDeclFtsXA(jlong,xRowid)(JniArgsEnvObj,jobject jCtx){
  Fts5ExtDecl;
  return (jlong)ext->xRowid(PtrGet_Fts5Context(jCtx));
}

JniDeclFtsXA(jint,xSetAuxdata)(JniArgsEnvObj,jobject jCtx, jobject jAux){
  Fts5ExtDecl;
  int rc;
  S3JniFts5AuxData * pAux;

  pAux = s3jni_malloc( sizeof(*pAux));
  if( !pAux ){
    if( jAux ){
      /* Emulate how xSetAuxdata() behaves when it cannot alloc
      ** its auxdata wrapper. */
      s3jni_call_xDestroy(jAux);
    }
    return SQLITE_NOMEM;
  }
  pAux->jObj = S3JniRefGlobal(jAux);
  rc = ext->xSetAuxdata(PtrGet_Fts5Context(jCtx), pAux,
                         S3JniFts5AuxData_xDestroy);
  return rc;
}

/* xToken() impl for xTokenize(). */
static int s3jni_xTokenize_xToken(void *p, int tFlags, const char* z,
                                  int nZ, int iStart, int iEnd){
  int rc;
  S3JniDeclLocal_env;
  struct s3jni_xQueryPhraseState * const s = p;
  jbyteArray jba;

  S3JniUnrefLocal(s->tok.jba);
  s->tok.zPrev = z;
  s->tok.nPrev = nZ;
  s->tok.jba = s3jni_new_jbyteArray(z, nZ);
  if( !s->tok.jba ) return SQLITE_NOMEM;
  jba = s->tok.jba;
  rc = (int)(*env)->CallIntMethod(env, s->jCallback, s->midCallback,
                                  (jint)tFlags, jba, (jint)iStart,
                                  (jint)iEnd);
  S3JniIfThrew {
    S3JniExceptionWarnCallbackThrew("xTokenize() callback");
    rc = SQLITE_ERROR;
  }
  return rc;
}

/*
** Proxy for Fts5ExtensionApi.xTokenize() and
** fts5_tokenizer.xTokenize()
*/
static jint s3jni_fts5_xTokenize(JniArgsEnvObj, S3JniNphOp const *pRef,
                                 jint tokFlags, jobject jFcx,
                                 jbyteArray jbaText, jobject jCallback){
  Fts5ExtDecl;
  struct s3jni_xQueryPhraseState s;
  int rc = 0;
  jbyte * const pText = jCallback ? s3jni_jbyteArray_bytes(jbaText) : 0;
  jsize nText = pText ? (*env)->GetArrayLength(env, jbaText) : 0;
  jclass const klazz = jCallback ? (*env)->GetObjectClass(env, jCallback) : NULL;

  if( !klazz ) return SQLITE_MISUSE;
  memset(&s, 0, sizeof(s));
  s.jCallback = jCallback;
  s.jFcx = jFcx;
  s.ext = ext;
  s.midCallback = (*env)->GetMethodID(env, klazz, "call", "(I[BII)I");
  S3JniUnrefLocal(klazz);
  S3JniIfThrew {
    S3JniExceptionReport;
    S3JniExceptionClear;
    s3jni_jbyteArray_release(jbaText, pText);
    return SQLITE_ERROR;
  }
  s.tok.jba = S3JniRefLocal(jbaText);
  s.tok.zPrev = (const char *)pText;
  s.tok.nPrev = (int)nText;
  if( pRef == S3JniNph(Fts5ExtensionApi) ){
    rc = ext->xTokenize(PtrGet_Fts5Context(jFcx),
                         (const char *)pText, (int)nText,
                         &s, s3jni_xTokenize_xToken);
  }else if( pRef == S3JniNph(fts5_tokenizer) ){
    fts5_tokenizer * const pTok = PtrGet_fts5_tokenizer(jSelf);
    rc = pTok->xTokenize(PtrGet_Fts5Tokenizer(jFcx), &s, tokFlags,
                         (const char *)pText, (int)nText,
                         s3jni_xTokenize_xToken);
  }else{
    (*env)->FatalError(env, "This cannot happen. Maintenance required.");
  }
  if( s.tok.jba ){
    assert( s.tok.zPrev );
    S3JniUnrefLocal(s.tok.jba);
  }
  s3jni_jbyteArray_release(jbaText, pText);
  return (jint)rc;
}

JniDeclFtsXA(jint,xTokenize)(JniArgsEnvObj,jobject jFcx, jbyteArray jbaText,
                             jobject jCallback){
  return s3jni_fts5_xTokenize(env, jSelf, S3JniNph(Fts5ExtensionApi),
                              0, jFcx, jbaText, jCallback);
}

JniDeclFtsTok(jint,xTokenize)(JniArgsEnvObj,jobject jFcx, jint tokFlags,
                              jbyteArray jbaText, jobject jCallback){
  return s3jni_fts5_xTokenize(env, jSelf, S3JniNph(Fts5Tokenizer),
                              tokFlags, jFcx, jbaText, jCallback);
}


JniDeclFtsXA(jobject,xUserData)(JniArgsEnvObj,jobject jFcx){
  Fts5ExtDecl;
  Fts5JniAux * const pAux = ext->xUserData(PtrGet_Fts5Context(jFcx));
  return pAux ? pAux->jUserData : 0;
}

#endif /* SQLITE_ENABLE_FTS5 */

////////////////////////////////////////////////////////////////////////
// End of the main API bindings. Start of SQLTester bits...
////////////////////////////////////////////////////////////////////////

#ifdef SQLITE_JNI_ENABLE_SQLTester
typedef struct SQLTesterJni SQLTesterJni;
struct SQLTesterJni {
  sqlite3_int64 nDup;
};
static SQLTesterJni SQLTester = {
  0
};

static void SQLTester_dup_destructor(void*pToFree){
  u64 *p = (u64*)pToFree;
  assert( p!=0 );
  p--;
  assert( p[0]==0x2bbf4b7c );
  p[0] = 0;
  p[1] = 0;
  sqlite3_free(p);
}

/*
** Implementation of
**
**         dup(TEXT)
**
** This SQL function simply makes a copy of its text argument.  But it
** returns the result using a custom destructor, in order to provide
** tests for the use of Mem.xDel() in the SQLite VDBE.
*/
static void SQLTester_dup_func(
  sqlite3_context *context,
  int argc,
  sqlite3_value **argv
){
  u64 *pOut;
  char *z;
  int n = sqlite3_value_bytes(argv[0]);
  SQLTesterJni * const p = (SQLTesterJni *)sqlite3_user_data(context);
  S3JniDeclLocal_env;

  ++p->nDup;
  if( n>0 && (pOut = s3jni_malloc( (n+16)&~7 ))!=0 ){
    pOut[0] = 0x2bbf4b7c;
    z = (char*)&pOut[1];
    memcpy(z, sqlite3_value_text(argv[0]), n);
    z[n] = 0;
    sqlite3_result_text(context, z, n, SQLTester_dup_destructor);
  }
  return;
}

/*
** Return the number of calls to the dup() SQL function since the
** SQLTester context was opened or since the last dup_count() call.
*/
static void SQLTester_dup_count_func(
  sqlite3_context *context,
  int argc,
  sqlite3_value **argv
){
  SQLTesterJni * const p = (SQLTesterJni *)sqlite3_user_data(context);
  sqlite3_result_int64(context, p->nDup);
  p->nDup = 0;
}

/*
** Return non-zero if string z matches glob pattern zGlob and zero if the
** pattern does not match.
**
** To repeat:
**
**         zero == no match
**     non-zero == match
**
** Globbing rules:
**
**      '*'       Matches any sequence of zero or more characters.
**
**      '?'       Matches exactly one character.
**
**     [...]      Matches one character from the enclosed list of
**                characters.
**
**     [^...]     Matches one character not in the enclosed list.
**
**      '#'       Matches any sequence of one or more digits with an
**                optional + or - sign in front, or a hexadecimal
**                literal of the form 0x...
*/
static int SQLTester_strnotglob(const char *zGlob, const char *z){
  int c, c2;
  int invert;
  int seen;

  while( (c = (*(zGlob++)))!=0 ){
    if( c=='*' ){
      while( (c=(*(zGlob++))) == '*' || c=='?' ){
        if( c=='?' && (*(z++))==0 ) return 0;
      }
      if( c==0 ){
        return 1;
      }else if( c=='[' ){
        while( *z && SQLTester_strnotglob(zGlob-1,z)==0 ){
          z++;
        }
        return (*z)!=0;
      }
      while( (c2 = (*(z++)))!=0 ){
        while( c2!=c ){
          c2 = *(z++);
          if( c2==0 ) return 0;
        }
        if( SQLTester_strnotglob(zGlob,z) ) return 1;
      }
      return 0;
    }else if( c=='?' ){
      if( (*(z++))==0 ) return 0;
    }else if( c=='[' ){
      int prior_c = 0;
      seen = 0;
      invert = 0;
      c = *(z++);
      if( c==0 ) return 0;
      c2 = *(zGlob++);
      if( c2=='^' ){
        invert = 1;
        c2 = *(zGlob++);
      }
      if( c2==']' ){
        if( c==']' ) seen = 1;
        c2 = *(zGlob++);
      }
      while( c2 && c2!=']' ){
        if( c2=='-' && zGlob[0]!=']' && zGlob[0]!=0 && prior_c>0 ){
          c2 = *(zGlob++);
          if( c>=prior_c && c<=c2 ) seen = 1;
          prior_c = 0;
        }else{
          if( c==c2 ){
            seen = 1;
          }
          prior_c = c2;
        }
        c2 = *(zGlob++);
      }
      if( c2==0 || (seen ^ invert)==0 ) return 0;
    }else if( c=='#' ){
      if( z[0]=='0'
       && (z[1]=='x' || z[1]=='X')
       && sqlite3Isxdigit(z[2])
      ){
        z += 3;
        while( sqlite3Isxdigit(z[0]) ){ z++; }
      }else{
        if( (z[0]=='-' || z[0]=='+') && sqlite3Isdigit(z[1]) ) z++;
        if( !sqlite3Isdigit(z[0]) ) return 0;
        z++;
        while( sqlite3Isdigit(z[0]) ){ z++; }
      }
    }else{
      if( c!=(*(z++)) ) return 0;
    }
  }
  return *z==0;
}

JNIEXPORT jint JNICALL
Java_org_sqlite_jni_capi_SQLTester_strglob(
  JniArgsEnvClass, jbyteArray baG, jbyteArray baT
){
  int rc = 0;
  jbyte * const pG = s3jni_jbyteArray_bytes(baG);
  jbyte * const pT = pG ? s3jni_jbyteArray_bytes(baT) : 0;

  s3jni_oom_fatal(pT);
  /* Note that we're relying on the byte arrays having been
     NUL-terminated on the Java side. */
  rc = !SQLTester_strnotglob((const char *)pG, (const char *)pT);
  s3jni_jbyteArray_release(baG, pG);
  s3jni_jbyteArray_release(baT, pT);
  return rc;
}


static int SQLTester_auto_extension(sqlite3 *pDb, const char **pzErr,
                                    const struct sqlite3_api_routines *ignored){
  sqlite3_create_function(pDb, "dup", 1, SQLITE_UTF8, &SQLTester,
                          SQLTester_dup_func, 0, 0);
  sqlite3_create_function(pDb, "dup_count", 0, SQLITE_UTF8, &SQLTester,
                          SQLTester_dup_count_func, 0, 0);
  return 0;
}

JNIEXPORT void JNICALL
Java_org_sqlite_jni_capi_SQLTester_installCustomExtensions(JniArgsEnvClass){
  sqlite3_auto_extension( (void(*)(void))SQLTester_auto_extension );
}

#endif /* SQLITE_JNI_ENABLE_SQLTester */
////////////////////////////////////////////////////////////////////////
// End of SQLTester bindings. Start of lower-level bits.
////////////////////////////////////////////////////////////////////////

/*
** Called during static init of the CApi class to set up global
** state.
*/
JNIEXPORT void JNICALL
Java_org_sqlite_jni_capi_CApi_init(JniArgsEnvClass){
  jclass klazz;

  memset(&S3JniGlobal, 0, sizeof(S3JniGlobal));
  if( (*env)->GetJavaVM(env, &SJG.jvm) ){
    (*env)->FatalError(env, "GetJavaVM() failure shouldn't be possible.");
    return;
  }

  /* Grab references to various global classes and objects... */
  SJG.g.cLong = S3JniRefGlobal((*env)->FindClass(env,"java/lang/Long"));
  S3JniExceptionIsFatal("Error getting reference to Long class.");
  SJG.g.ctorLong1 = (*env)->GetMethodID(env, SJG.g.cLong,
                                         "<init>", "(J)V");
  S3JniExceptionIsFatal("Error getting reference to Long constructor.");

  SJG.g.cString = S3JniRefGlobal((*env)->FindClass(env,"java/lang/String"));
  S3JniExceptionIsFatal("Error getting reference to String class.");
  SJG.g.ctorStringBA =
    (*env)->GetMethodID(env, SJG.g.cString,
                        "<init>", "([BLjava/nio/charset/Charset;)V");
  S3JniExceptionIsFatal("Error getting reference to String(byte[],Charset) ctor.");
  SJG.g.stringGetBytes =
    (*env)->GetMethodID(env, SJG.g.cString,
                        "getBytes", "(Ljava/nio/charset/Charset;)[B");
  S3JniExceptionIsFatal("Error getting reference to String.getBytes(Charset).");

  { /* java.nio.charset.StandardCharsets.UTF_8 */
    jfieldID fUtf8;
    klazz = (*env)->FindClass(env,"java/nio/charset/StandardCharsets");
    S3JniExceptionIsFatal("Error getting reference to StandardCharsets class.");
    fUtf8 = (*env)->GetStaticFieldID(env, klazz, "UTF_8",
                                     "Ljava/nio/charset/Charset;");
    S3JniExceptionIsFatal("Error getting StandardCharsets.UTF_8 field.");
    SJG.g.oCharsetUtf8 =
      S3JniRefGlobal((*env)->GetStaticObjectField(env, klazz, fUtf8));
    S3JniExceptionIsFatal("Error getting reference to StandardCharsets.UTF_8.");
    S3JniUnrefLocal(klazz);
  }

#ifdef SQLITE_ENABLE_FTS5
  klazz = (*env)->FindClass(env, "org/sqlite/jni/fts5/Fts5PhraseIter");
  S3JniExceptionIsFatal("Error getting reference to org.sqlite.jni.fts5.Fts5PhraseIter.");
  SJG.fts5.jPhraseIter.fidA = (*env)->GetFieldID(env, klazz, "a", "J");
  S3JniExceptionIsFatal("Cannot get Fts5PhraseIter.a field.");
  SJG.fts5.jPhraseIter.fidB = (*env)->GetFieldID(env, klazz, "b", "J");
  S3JniExceptionIsFatal("Cannot get Fts5PhraseIter.b field.");
  S3JniUnrefLocal(klazz);
#endif

  SJG.mutex = sqlite3_mutex_alloc(SQLITE_MUTEX_FAST);
  s3jni_oom_fatal( SJG.mutex );
  SJG.hook.mutex = sqlite3_mutex_alloc(SQLITE_MUTEX_FAST);
  s3jni_oom_fatal( SJG.hook.mutex );
  SJG.nph.mutex = sqlite3_mutex_alloc(SQLITE_MUTEX_FAST);
  s3jni_oom_fatal( SJG.nph.mutex );
  SJG.envCache.mutex = sqlite3_mutex_alloc(SQLITE_MUTEX_FAST);
  s3jni_oom_fatal( SJG.envCache.mutex );
  SJG.perDb.mutex = sqlite3_mutex_alloc(SQLITE_MUTEX_FAST);
  s3jni_oom_fatal( SJG.perDb.mutex );
  SJG.autoExt.mutex = sqlite3_mutex_alloc(SQLITE_MUTEX_FAST);
  s3jni_oom_fatal( SJG.autoExt.mutex );

#if S3JNI_METRICS_MUTEX
  SJG.metrics.mutex = sqlite3_mutex_alloc(SQLITE_MUTEX_FAST);
  s3jni_oom_fatal( SJG.metrics.mutex );
#endif

  {
    /* Test whether this JVM supports direct memory access via
       ByteBuffer. */
    unsigned char buf[16] = {0};
    jobject bb = (*env)->NewDirectByteBuffer(env, buf, 16);
    if( bb ){
      SJG.g.byteBuffer.klazz = S3JniRefGlobal((*env)->GetObjectClass(env, bb));
      SJG.g.byteBuffer.midAlloc = (*env)->GetStaticMethodID(
        env, SJG.g.byteBuffer.klazz, "allocateDirect", "(I)Ljava/nio/ByteBuffer;"
      );
      S3JniExceptionIsFatal("Error getting ByteBuffer.allocateDirect() method.");
      SJG.g.byteBuffer.midLimit = (*env)->GetMethodID(
        env, SJG.g.byteBuffer.klazz, "limit", "()I"
      );
      S3JniExceptionIsFatal("Error getting ByteBuffer.limit() method.");
      S3JniUnrefLocal(bb);
    }else{
      SJG.g.byteBuffer.klazz = 0;
      SJG.g.byteBuffer.midAlloc = 0;
    }
  }

  sqlite3_shutdown()
    /* So that it becomes legal for Java-level code to call
    ** sqlite3_config(). */;
}

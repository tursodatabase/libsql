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
** org.sqlite.jni.SQLiteJni (from which sqlite3-jni.h is generated).
*/

/*
** If you found this comment by searching the code for
** CallStaticObjectMethod then you're the victim of an OpenJDK bug:
**
** https://bugs.openjdk.org/browse/JDK-8130659
**
** It's known to happen with OpenJDK v8 but not with v19.
**
** This code does not use JNI's CallStaticObjectMethod().
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
** to SQLITE_MAX_... and friends. This increases the rebuild time
** considerably but we need this in order to keep the exported values
** of SQLITE_MAX_... and SQLITE_LIMIT_... in sync with the C build.
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
  Java_org_sqlite_jni_SQLite3Jni_sqlite3_ ## Suffix

/* Prologue for JNI function declarations and definitions. */
#define JniDecl(ReturnType,Suffix)                \
  JNIEXPORT ReturnType JNICALL                  \
  JniFuncName(Suffix)

/*
** S3JniApi's intent is that CFunc be the C API func(s) the
** being-declared JNI function is wrapping, making it easier to find
** that function's JNI-side entry point. The other args are for JniDecl.
 */
#define S3JniApi(CFunc,ReturnType,Suffix) JniDecl(ReturnType,Suffix)

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
#define JniArgsEnvObj JNIEnv * const env, jobject jSelf
#define JniArgsEnvClass JNIEnv * const env, jclass jKlazz
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
** Helpers for extracting pointers from jobjects, noting that we rely
** on the corresponding Java interfaces having already done the
** type-checking. Don't use these in contexts where that's not the
** case.
*/
#define PtrGet_T(T,OBJ) NativePointerHolder_get(env, OBJ, &S3NphRefs.T)
#define PtrGet_sqlite3(OBJ) PtrGet_T(sqlite3, OBJ)
#define PtrGet_sqlite3_stmt(OBJ) PtrGet_T(sqlite3_stmt, OBJ)
#define PtrGet_sqlite3_value(OBJ) PtrGet_T(sqlite3_value, OBJ)
#define PtrGet_sqlite3_context(OBJ) PtrGet_T(sqlite3_context, OBJ)
/* Helpers for Java value reference management. */
static inline jobject s3jni_ref_global(JNIEnv * const env, jobject const v){
  return v ? (*env)->NewGlobalRef(env, v) : NULL;
}
static inline void s3jni_unref_global(JNIEnv * const env, jobject const v){
  if( v ) (*env)->DeleteGlobalRef(env, v);
}
static inline void s3jni_unref_local(JNIEnv * const env, jobject const v){
  if( v ) (*env)->DeleteLocalRef(env, v);
}
#define S3JniRefGlobal(VAR) s3jni_ref_global(env, (VAR))
#define S3JniRefLocal(VAR) (*env)->NewLocalRef(env, (VAR))
#define S3JniUnrefGlobal(VAR) s3jni_unref_global(env, (VAR))
#define S3JniUnrefLocal(VAR) s3jni_unref_local(env, (VAR))

/**
   Keys for use with S3JniGlobal_nph_cache().
*/
typedef struct S3NphRef S3NphRef;
struct S3NphRef {
  const int index           /* index into S3JniGlobal.nph[] */;
  const char * const zName  /* Full Java name of the class */;
};

/**
   Keys for each concrete NativePointerHolder subclass. These are to
   be used with S3JniGlobal_nph_cache() and friends. These are
   initialized on-demand by S3JniGlobal_nph_cache().
*/
static const struct {
  const S3NphRef sqlite3;
  const S3NphRef sqlite3_stmt;
  const S3NphRef sqlite3_context;
  const S3NphRef sqlite3_value;
  const S3NphRef OutputPointer_Int32;
  const S3NphRef OutputPointer_Int64;
  const S3NphRef OutputPointer_sqlite3;
  const S3NphRef OutputPointer_sqlite3_stmt;
  const S3NphRef OutputPointer_sqlite3_value;
#ifdef SQLITE_ENABLE_FTS5
  const S3NphRef OutputPointer_String;
  const S3NphRef OutputPointer_ByteArray;
  const S3NphRef Fts5Context;
  const S3NphRef Fts5ExtensionApi;
  const S3NphRef fts5_api;
  const S3NphRef fts5_tokenizer;
  const S3NphRef Fts5Tokenizer;
#endif
} S3NphRefs = {
#define NREF(INDEX, JAVANAME) { INDEX, "org/sqlite/jni/" JAVANAME }
  NREF(0,  "sqlite3"),
  NREF(1,  "sqlite3_stmt"),
  NREF(2,  "sqlite3_context"),
  NREF(3,  "sqlite3_value"),
  NREF(4,  "OutputPointer$Int32"),
  NREF(5,  "OutputPointer$Int64"),
  NREF(6,  "OutputPointer$sqlite3"),
  NREF(7,  "OutputPointer$sqlite3_stmt"),
  NREF(8,  "OutputPointer$sqlite3_value"),
#ifdef SQLITE_ENABLE_FTS5
  NREF(9,  "OutputPointer$String"),
  NREF(10, "OutputPointer$ByteArray"),
  NREF(11, "Fts5Context"),
  NREF(12, "Fts5ExtensionApi"),
  NREF(13, "fts5_api"),
  NREF(14, "fts5_tokenizer"),
  NREF(15, "Fts5Tokenizer")
#endif
#undef NREF
};

/* Helpers for jstring and jbyteArray. */
#define s3jni_jstring_to_mutf8(ARG) (*env)->GetStringUTFChars(env, ARG, NULL)
#define s3jni_mutf8_release(ARG,VAR) if( VAR ) (*env)->ReleaseStringUTFChars(env, ARG, VAR)
#define s3jni_jbytearray_bytes(ARG) (*env)->GetByteArrayElements(env,ARG, NULL)
#define s3jni_jbytearray_release(ARG,VAR) if( VAR ) (*env)->ReleaseByteArrayElements(env, ARG, VAR, JNI_ABORT)

enum {
  /*
  ** Size of the NativePointerHolder cache.  Need enough space for
  ** (only) the library's NativePointerHolder types, a fixed count
  ** known at build-time. If we add more than this a fatal error will
  ** be triggered with a reminder to increase this.  This value needs
  ** to be exactly the number of entries in the S3NphRefs object. The
  ** index field of those entries are the keys for this particular
  ** cache.
  */
  S3Jni_NphCache_size = sizeof(S3NphRefs) / sizeof(S3NphRef)
};

/*
** Cache entry for NativePointerHolder subclasses and OutputPointer
** types.
*/
typedef struct S3JniNphClass S3JniNphClass;
struct S3JniNphClass {
  volatile const S3NphRef * pRef /* Entry from S3NphRefs. */;
  jclass klazz              /* global ref to the concrete
                            ** NativePointerHolder subclass represented by
                            ** zClassName */;
  volatile jmethodID midCtor /* klazz's no-arg constructor. Used by
                             ** new_NativePointerHolder_object(). */;
  volatile jfieldID fidValue /* NativePointerHolder.nativePointer or
                             ** OutputPointer.T.value */;
  volatile jfieldID fidAggCtx /* sqlite3_context::aggregateContext. Used only
                              **  by the sqlite3_context binding. */;
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
  ** have custom finalizers. */
};

/*
** Per-(sqlite3*) state for various JNI bindings.  This state is
** allocated as needed, cleaned up in sqlite3_close(_v2)(), and
** recycled when possible.
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
    S3JniHook collation;
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
  jobject jFtsApi  /* global ref to s3jni_fts5_api_from_db() */;
#endif
  S3JniDb * pNext /* Next entry in the available/free list */;
  S3JniDb * pPrev /* Previous entry in the available/free list */;
};

/*
** Cache for per-JNIEnv (i.e. per-thread) data.
*/
typedef struct S3JniEnv S3JniEnv;
struct S3JniEnv {
  JNIEnv *env            /* env in which this cache entry was created */;
  /*
  ** pdbOpening is used to coordinate the Java/DB connection of a
  ** being-open()'d db in the face of auto-extensions. "The problem"
  ** is that auto-extensions run before we can bind the C db to its
  ** Java representation, but auto-extensions require that binding. We
  ** handle this as follows:
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
  S3JniEnv * pPrev /* Previous entry in the linked list */;
  S3JniEnv * pNext /* Next entry in the linked list */;
};

/*
** State for proxying sqlite3_auto_extension() in Java.
*/
typedef struct S3JniAutoExtension S3JniAutoExtension;
struct S3JniAutoExtension {
  jobject jObj       /* Java object */;
  jmethodID midFunc  /* xEntryPoint() callback */;
};

/*
** If true, modifying S3JniGlobal.metrics is protected by a mutex,
** else it isn't.
*/
#ifdef SQLITE_DEBUG
#define S3JNI_METRICS_MUTEX 1
#else
#define S3JNI_METRICS_MUTEX 0
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
  ** Cache of Java refs/IDs for NativePointerHolder subclasses.
  ** Initialized on demand.
  */
  S3JniNphClass nph[S3Jni_NphCache_size];
  /*
  ** Cache of per-thread state.
  */
  struct {
    S3JniEnv * aHead /* Linked list of in-use instances */;
    S3JniEnv * aFree /* Linked list of free instances */;
    sqlite3_mutex * mutex /* mutex for aHead and aFree as well for
                             first-time inits of nph members. */;
    void const * locker /* env mutex is held on this object's behalf.
                           Used only for sanity checking. */;
  } envCache;
  struct {
    S3JniDb * aUsed  /* Linked list of in-use instances */;
    S3JniDb * aFree  /* Linked list of free instances */;
    sqlite3_mutex * mutex /* mutex for aUsed and aFree */;
    void const * locker /* perDb mutex is held on this object's
                           behalf.  Unlike envCache.locker, we cannot
                           always have this set to the current JNIEnv
                           object. Used only for sanity checking. */;
  } perDb;
#ifdef SQLITE_ENABLE_SQLLOG
  struct {
    S3JniHook sqllog  /* sqlite3_config(SQLITE_CONFIG_SQLLOG) callback */;
  } hooks;
#endif
  /*
  ** Refs to global classes and methods. Obtained during static init
  ** and never released.
  */
  struct {
    jclass cObj              /* global ref to java.lang.Object */;
    jclass cLong             /* global ref to java.lang.Long */;
    jclass cString           /* global ref to java.lang.String */;
    jobject oCharsetUtf8     /* global ref to StandardCharset.UTF_8 */;
    jmethodID ctorLong1      /* the Long(long) constructor */;
    jmethodID ctorStringBA   /* the String(byte[],Charset) constructor */;
    jmethodID stringGetBytes /* the String.getBytes(Charset) method */;
  } g /* refs to global Java state */;
#ifdef SQLITE_ENABLE_FTS5
  struct {
    volatile jobject jFtsExt /* Global ref to Java singleton for the
                                Fts5ExtensionApi instance. */;
    struct {
      jfieldID fidA         /* Fts5Phrase::a member */;
      jfieldID fidB         /* Fts5Phrase::b member */;
    } jPhraseIter;
  } fts5;
#endif
  /* Internal metrics. */
  struct {
    volatile unsigned envCacheHits;
    volatile unsigned envCacheMisses;
    volatile unsigned envCacheAllocs;
    volatile unsigned nMutexEnv       /* number of times envCache.mutex was entered for
                                         a S3JniEnv operation. */;
    volatile unsigned nMutexEnv2      /* number of times envCache.mutex was entered for
                                         a S3JniNphClass operation. */;
    volatile unsigned nMutexPerDb     /* number of times perDb.mutex was entered */;
    volatile unsigned nMutexAutoExt   /* number of times autoExt.mutex was entered */;
    volatile unsigned nDestroy        /* xDestroy() calls across all types */;
    volatile unsigned nPdbAlloc       /* Number of S3JniDb alloced. */;
    volatile unsigned nPdbRecycled    /* Number of S3JniDb reused. */;
    struct {
      /* Number of calls for each type of UDF callback. */
      volatile unsigned nFunc;
      volatile unsigned nStep;
      volatile unsigned nFinal;
      volatile unsigned nValue;
      volatile unsigned nInverse;
    } udf;
    unsigned nMetrics                 /* Total number of mutex-locked metrics increments. */;
#if S3JNI_METRICS_MUTEX
    sqlite3_mutex * mutex;
#endif
  } metrics;
  /**
     The list of bound auto-extensions (Java-side:
     org.sqlite.jni.auto_extension objects).
   */
  struct {
    S3JniAutoExtension *pExt /* Head of the auto-extension list */;
    int nAlloc               /* number of entries allocated for pExt,
                                as distinct from the number of active
                                entries. */;
    int nExt                 /* number of active entries in pExt. */;
    sqlite3_mutex * mutex    /* mutex for manipulation/traversal of pExt */;
  } autoExt;
};
static S3JniGlobalType S3JniGlobal = {};
#define SJG S3JniGlobal

/* Increments *p, possibly protected by a mutex. */
#if S3JNI_METRICS_MUTEX
static void s3jni_incr( volatile unsigned int * const p ){
  sqlite3_mutex * const m = SJG.metrics.mutex;
  sqlite3_mutex_enter(m);
  ++SJG.metrics.nMetrics;
  ++(*p);
  sqlite3_mutex_leave(m);
}
#else
#define s3jni_incr(PTR) ++(*(PTR))
#endif

/* Helpers for working with specific mutexes. */
#define S3JniMutex_Env_assertLocked \
  assert( 0 != SJG.envCache.locker && "Misuse of S3JniGlobal.envCache.mutex" )
#define S3JniMutex_Env_assertLocker \
  assert( (env) == SJG.envCache.locker && "Misuse of S3JniGlobal.envCache.mutex" )
#define S3JniMutex_Env_assertNotLocker \
  assert( (env) != SJG.envCache.locker && "Misuse of S3JniGlobal.envCache.mutex" )
#define S3JniMutex_Env_enter                        \
  S3JniMutex_Env_assertNotLocker;                   \
  /*MARKER(("Entering ENV mutex@%p %s.\n", env));*/ \
  sqlite3_mutex_enter( SJG.envCache.mutex );        \
  ++SJG.metrics.nMutexEnv;                          \
  SJG.envCache.locker = env
#define S3JniMutex_Env_leave                         \
  /*MARKER(("Leaving ENV mutex @%p %s.\n", env));*/  \
  S3JniMutex_Env_assertLocker;                       \
  SJG.envCache.locker = 0;                           \
  sqlite3_mutex_leave( SJG.envCache.mutex )
#define S3JniMutex_Ext_enter                            \
  /*MARKER(("Entering autoExt mutex@%p %s.\n", env));*/ \
  sqlite3_mutex_enter( SJG.autoExt.mutex );             \
  ++SJG.metrics.nMutexAutoExt
#define S3JniMutex_Ext_leave                            \
  /*MARKER(("Leaving autoExt mutex@%p %s.\n", env));*/  \
  sqlite3_mutex_leave( SJG.autoExt.mutex )
#define S3JniMutex_Nph_enter                        \
  S3JniMutex_Env_assertNotLocker;                   \
  /*MARKER(("Entering NPH mutex@%p %s.\n", env));*/ \
  sqlite3_mutex_enter( SJG.envCache.mutex );        \
  ++SJG.metrics.nMutexEnv2;                         \
  SJG.envCache.locker = env
#define S3JniMutex_Nph_leave                         \
  /*MARKER(("Leaving NPH mutex @%p %s.\n", env));*/  \
  S3JniMutex_Env_assertLocker;                       \
  SJG.envCache.locker = 0;                           \
  sqlite3_mutex_leave( SJG.envCache.mutex )
#define S3JniMutex_S3JniDb_enter                      \
  sqlite3_mutex_enter( SJG.perDb.mutex );             \
  assert( 0==SJG.perDb.locker );                      \
  ++SJG.metrics.nMutexPerDb;                          \
  SJG.perDb.locker = env;
#define S3JniMutex_S3JniDb_leave                      \
  /*MARKER(("Leaving PerDb mutex@%p %s.\n", env));*/  \
  assert( env == SJG.perDb.locker );                  \
  SJG.perDb.locker = 0;                               \
  sqlite3_mutex_leave( SJG.perDb.mutex )

#define s3jni_oom_check(VAR) if( !(VAR) ) s3jni_oom(env)
static inline void s3jni_oom(JNIEnv * const env){
  (*env)->FatalError(env, "Out of memory.") /* does not return */;
}

/*
** sqlite3_malloc() proxy which fails fatally on OOM.  This should
** only be used for routines which manage global state and have no
** recovery strategy for OOM. For sqlite3 API which can reasonably
** return SQLITE_NOMEM, sqlite3_malloc() should be used instead.
*/
static void * s3jni_malloc(JNIEnv * const env, size_t n){
  void * const rv = sqlite3_malloc(n);
  if( n && !rv ) s3jni_oom(env);
  return rv;
}

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
/* Declares local var env = s3jni_env(). */
#define LocalJniGetEnv JNIEnv * const env = s3jni_env()

/*
** Fetches the S3JniGlobal.envCache row for the given env, allocing a
** row if needed. When a row is allocated, its state is initialized
** insofar as possible. Calls (*env)->FatalError() if allocation of an
** entry fails. That's hypothetically possible but "shouldn't happen."
*/
static S3JniEnv * S3JniEnv_get(JNIEnv * const env){
  struct S3JniEnv * row;
  S3JniMutex_Env_enter;
  row = SJG.envCache.aHead;
  for( ; row; row = row->pNext ){
    if( row->env == env ){
      s3jni_incr( &SJG.metrics.envCacheHits );
      S3JniMutex_Env_leave;
      return row;
    }
  }
  s3jni_incr( &SJG.metrics.envCacheMisses );
  row = SJG.envCache.aFree;
  if( row ){
    assert(!row->pPrev);
    SJG.envCache.aFree = row->pNext;
    if( row->pNext ) row->pNext->pPrev = 0;
  }else{
    row = s3jni_malloc(env, sizeof(S3JniEnv));
    s3jni_incr( &SJG.metrics.envCacheAllocs );
  }
  memset(row, 0, sizeof(*row));
  row->pNext = SJG.envCache.aHead;
  if( row->pNext ) row->pNext->pPrev = row;
  SJG.envCache.aHead = row;
  row->env = env;

  S3JniMutex_Env_leave;
  return row;
}

/*
** This function is NOT part of the sqlite3 public API. It is strictly
** for use by the sqlite project's own Java/JNI bindings.
**
** For purposes of certain hand-crafted JNI function bindings, we
** need a way of reporting errors which is consistent with the rest of
** the C API, as opposed to throwing JS exceptions. To that end, this
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
** Creates a new jByteArray of length nP, copies p's contents into it, and
** returns that byte array (NULL on OOM).
*/
static jbyteArray s3jni_new_jbyteArray(JNIEnv * const env,
                                       const unsigned char * const p, int nP){
  jbyteArray jba = (*env)->NewByteArray(env, (jint)nP);
  if( jba ){
    (*env)->SetByteArrayRegion(env, jba, 0, (jint)nP, (const jbyte*)p);
  }
  return jba;
}

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
static jstring s3jni_utf8_to_jstring(JNIEnv * const env,
                                     const char * const z, int n){
  jstring rv = NULL;
  if( 0==n || (n<0 && z && !z[0]) ){
    /* Fast-track the empty-string case via the MUTF-8 API. We could
       hypothetically do this for any strings where n<4 and z is
       NUL-terminated and none of z[0..3] are NUL bytes. */
    rv = (*env)->NewStringUTF(env, "");
  }else if( z ){
    jbyteArray jba;
    if( n<0 ) n = sqlite3Strlen30(z);
    jba = s3jni_new_jbyteArray(env, (unsigned const char *)z, (jsize)n);
    if( jba ){
      rv = (*env)->NewObject(env, SJG.g.cString, SJG.g.ctorStringBA,
                             jba, SJG.g.oCharsetUtf8);
      S3JniUnrefLocal(jba);
    }
  }
  return rv;
}

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
static char * s3jni_jstring_to_utf8(JNIEnv * const env,
                                    jstring jstr, int *nLen){
  jbyteArray jba;
  jsize nBa;
  char *rv;

  if( !jstr ) return 0;
  jba = (*env)->CallObjectMethod(env, jstr, SJG.g.stringGetBytes,
                                 SJG.g.oCharsetUtf8);
  if( (*env)->ExceptionCheck(env) || !jba
      /* order of these checks is significant for -Xlint:jni */ ) {
    S3JniExceptionReport;
    if( nLen ) *nLen = 0;
    return 0;
  }
  nBa = (*env)->GetArrayLength(env, jba);
  if( nLen ) *nLen = (int)nBa;
  rv = sqlite3_malloc( nBa + 1 );
  if( rv ){
    (*env)->GetByteArrayRegion(env, jba, 0, nBa, (jbyte*)rv);
    rv[nBa] = 0;
  }
  S3JniUnrefLocal(jba);
  return rv;
}

/*
** Expects to be passed a pointer from sqlite3_column_text16() or
** sqlite3_value_text16() and a byte-length value from
** sqlite3_column_bytes16() or sqlite3_value_bytes16(). It creates a
** Java String of exactly half that character length, returning NULL
** if !p or (*env)->NewString() fails.
*/
static jstring s3jni_text16_to_jstring(JNIEnv * const env, const void * const p, int nP){
  return p
    ? (*env)->NewString(env, (const jchar *)p, (jsize)(nP/2))
    : NULL;
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
static char * s3jni_exception_error_msg(JNIEnv * const env, jthrowable jx ){
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
  zMsg = s3jni_jstring_to_utf8(env, msg, 0);
  S3JniUnrefLocal(msg);
  return zMsg;
}

/*
** Extracts the current JNI exception, sets ps->pDb's error message to
** its message string, and clears the exception. If errCode is non-0,
** it is used as-is, else SQLITE_ERROR is assumed. If there's a
** problem extracting the exception's message, it's treated as
** non-fatal and zDfltMsg is used in its place.
**
** This must only be called if a JNI exception is pending.
**
** Returns errCode unless it is 0, in which case SQLITE_ERROR is
** returned.
*/
static int s3jni_db_exception(JNIEnv * const env, S3JniDb * const ps,
                              int errCode, const char *zDfltMsg){
  jthrowable const ex = (*env)->ExceptionOccurred(env);

  if( 0==errCode ) errCode = SQLITE_ERROR;
  if( ex ){
    char * zMsg;
    S3JniExceptionClear;
    zMsg = s3jni_exception_error_msg(env, ex);
    s3jni_db_error(ps->pDb, errCode, zMsg ? zMsg : zDfltMsg);
    sqlite3_free(zMsg);
    S3JniUnrefLocal(ex);
  }
   return errCode;
}

/*
** Extracts the (void xDestroy()) method from jObj and applies it to
** jObj. If jObj is NULL, this is a no-op. The lack of an xDestroy()
** method is silently ignored and any exceptions thrown by xDestroy()
** trigger a warning to stdout or stderr and then the exception is
** suppressed.
*/
static void s3jni_call_xDestroy(JNIEnv * const env, jobject jObj){
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

/*
** Removes any Java references from s and clears its state. If
** doXDestroy is true and s->jObj is not NULL, s->jObj's
** s is passed to s3jni_call_xDestroy() before any references are
** cleared. It is legal to call this when the object has no Java
** references.
*/
static void S3JniHook_unref(JNIEnv * const env, S3JniHook * const s, int doXDestroy){
  if( doXDestroy && s->jObj ){
    s3jni_call_xDestroy(env, s->jObj);
  }
  S3JniUnrefGlobal(s->jObj);
  memset(s, 0, sizeof(*s));
}

/*
** Clears s's state and moves it to the free-list.
*/
static void S3JniDb_set_aside_unlocked(JNIEnv * env, S3JniDb * const s){
  if( s ){
    assert( S3JniGlobal.perDb.locker == env );
    assert(s->pPrev != s);
    assert(s->pNext != s);
    assert(s->pPrev ? (s->pPrev!=s->pNext) : 1);
    if(s->pNext) s->pNext->pPrev = s->pPrev;
    if(s->pPrev) s->pPrev->pNext = s->pNext;
    else if(SJG.perDb.aUsed == s){
      assert(!s->pPrev);
      SJG.perDb.aUsed = s->pNext;
    }
    sqlite3_free( s->zMainDbName );
#define UNHOOK(MEMBER,XDESTROY) S3JniHook_unref(env, &s->hooks.MEMBER, XDESTROY)
    UNHOOK(trace, 0);
    UNHOOK(progress, 0);
    UNHOOK(commit, 0);
    UNHOOK(rollback, 0);
    UNHOOK(update, 0);
    UNHOOK(auth, 0);
#ifdef SQLITE_ENABLE_PREUPDATE_HOOK
    UNHOOK(preUpdate, 0);
#endif
    UNHOOK(collation, 1);
    UNHOOK(collationNeeded, 0);
    UNHOOK(busyHandler, 1);
#undef UNHOOK
    S3JniUnrefGlobal(s->jDb);
    memset(s, 0, sizeof(S3JniDb));
    s->pNext = SJG.perDb.aFree;
    if(s->pNext) s->pNext->pPrev = s;
    SJG.perDb.aFree = s;
  }
}
static void S3JniDb_set_aside(JNIEnv * env, S3JniDb * const s){
  if( s ){
    S3JniMutex_S3JniDb_enter;
    S3JniDb_set_aside_unlocked(env, s);
    S3JniMutex_S3JniDb_leave;
  }
}

/*
** Uncache any state for the given JNIEnv, clearing all Java
** references the cache owns. Returns true if env was cached and false
** if it was not found in the cache.
*/
static int S3JniEnv_uncache(JNIEnv * const env){
  struct S3JniEnv * row;
  S3JniMutex_Env_assertLocked;
  row = SJG.envCache.aHead;
  for( ; row; row = row->pNext ){
    if( row->env == env ){
      break;
    }
  }
  if( !row ){
      return 0;
  }
  if( row->pNext ) row->pNext->pPrev = row->pPrev;
  if( row->pPrev ) row->pPrev->pNext = row->pNext;
  if( SJG.envCache.aHead == row ){
    assert( !row->pPrev );
    SJG.envCache.aHead = row->pNext;
  }
  memset(row, 0, sizeof(S3JniEnv));
  row->pNext = SJG.envCache.aFree;
  if( row->pNext ) row->pNext->pPrev = row;
  SJG.envCache.aFree = row;
  return 1;
}

/*
** Searches the NativePointerHolder cache for the given combination of
** args.  It returns a cache entry with its klazz member set.
**
** It is up to the caller to populate the other members of the
** returned object if needed, taking care to lock the population with
** S3JniMutex_Nph_enter/LEAVE.
**
** This simple cache catches >99% of searches in the current
** (2023-07-31) tests.
*/
static S3JniNphClass * S3JniGlobal_nph_cache(JNIEnv * const env, S3NphRef const* pRef){
  /**
   According to:

     https://developer.ibm.com/articles/j-jni/

     > ... the IDs returned for a given class don't change for the
     lifetime of the JVM process. But the call to get the field or
     method can require significant work in the JVM, because
     fields and methods might have been inherited from
     superclasses, making the JVM walk up the class hierarchy to
     find them. Because the IDs are the same for a given class,
     you should look them up once and then reuse them. Similarly,
     looking up class objects can be expensive, so they should be
     cached as well.
  */
  S3JniNphClass * const pNC = &SJG.nph[pRef->index];
  assert( (void*)pRef>=(void*)&S3NphRefs && (void*)pRef<(void*)(&S3NphRefs + 1)
          && "pRef is out of range." );
  if( !pNC->pRef ){
    S3JniMutex_Nph_enter;
    if( !pNC->pRef ){
      pNC->pRef = pRef;
      pNC->klazz = (*env)->FindClass(env, pRef->zName);
      S3JniExceptionIsFatal("FindClass() unexpectedly threw");
      pNC->klazz = S3JniRefGlobal(pNC->klazz);
    }
    S3JniMutex_Nph_leave;
  }
  return pNC;
}

/*
** Returns the ID of the "nativePointer" field from the given
** NativePointerHolder<T> class.
*/
static jfieldID NativePointerHolder_getField(JNIEnv * const env, S3NphRef const* pRef){
  S3JniNphClass * const pNC = S3JniGlobal_nph_cache(env, pRef);
  if( !pNC->fidValue ){
    S3JniMutex_Nph_enter;
    if( !pNC->fidValue ){
      pNC->fidValue = (*env)->GetFieldID(env, pNC->klazz, "nativePointer", "J");
      S3JniExceptionIsFatal("Code maintenance required: missing nativePointer field.");
    }
    S3JniMutex_Nph_leave;
  }
  return pNC->fidValue;
}

/*
** Sets a native ptr value in NativePointerHolder object ppOut.
** zClassName must be a static string so we can use its address
** as a cache key.
*/
static void NativePointerHolder_set(JNIEnv * env, jobject ppOut, const void * p,
                                    S3NphRef const* pRef){
  (*env)->SetLongField(env, ppOut, NativePointerHolder_getField(env, pRef),
                       (jlong)p);
  S3JniExceptionIsFatal("Could not set NativePointerHolder.nativePointer.");
}

/*
** Fetches a native ptr value from NativePointerHolder object ppOut.
** zClassName must be a static string so we can use its address as a
** cache key. This is a no-op if pObj is NULL.
*/
static void * NativePointerHolder_get(JNIEnv * env, jobject pObj, S3NphRef const* pRef){
  if( pObj ){
    void * const rv = (void*)(*env)->GetLongField(
      env, pObj, NativePointerHolder_getField(env, pRef)
    );
    S3JniExceptionIsFatal("Cannot fetch NativePointerHolder.nativePointer.");
    return rv;
  }else{
    return 0;
  }
}

/*
** Extracts the new S3JniDb instance from the free-list, or allocates
** one if needed, associats it with pDb, and returns.  Returns NULL on
** OOM. pDb MUST, on success of the calling operation, subsequently be
** associated with jDb via NativePointerHolder_set().
*/
static S3JniDb * S3JniDb_alloc(JNIEnv * const env, sqlite3 *pDb,
                               jobject jDb){
  S3JniDb * rv;
  S3JniMutex_S3JniDb_enter;
  if( SJG.perDb.aFree ){
    rv = SJG.perDb.aFree;
    SJG.perDb.aFree = rv->pNext;
    assert(rv->pNext != rv);
    assert(rv->pPrev != rv);
    assert(rv->pPrev ? (rv->pPrev!=rv->pNext) : 1);
    if( rv->pNext ){
      assert(rv->pNext->pPrev == rv);
      assert(rv->pPrev ? (rv->pNext == rv->pPrev->pNext) : 1);
      rv->pNext->pPrev = 0;
      rv->pNext = 0;
    }
    s3jni_incr( &SJG.metrics.nPdbRecycled );
  }else{
    rv = s3jni_malloc(env, sizeof(S3JniDb));
    if( rv ){
      memset(rv, 0, sizeof(S3JniDb));
      s3jni_incr( &SJG.metrics.nPdbAlloc );
    }
  }
  if( rv ){
    rv->pNext = SJG.perDb.aUsed;
    SJG.perDb.aUsed = rv;
    if( rv->pNext ){
      assert(!rv->pNext->pPrev);
      rv->pNext->pPrev = rv;
    }
    rv->jDb = S3JniRefGlobal(jDb);
    rv->pDb = pDb;
  }
  S3JniMutex_S3JniDb_leave;
  return rv;
}

/*
** Returns the S3JniDb object for the given db.
**
** The 3rd argument should normally only be non-0 for routines which
** are called from the C library and pass a native db handle instead of
** a Java handle. In normal usage, the 2nd argument is a Java-side sqlite3
** object, from which the db is fished out.
**
** Returns NULL if jDb and pDb are both NULL or if there is no
** matching S3JniDb entry for pDb or the pointer fished out of jDb.
*/
static S3JniDb * S3JniDb_for_db(JNIEnv * const env, jobject jDb, sqlite3 *pDb){
  S3JniDb * s = 0;
  if( jDb || pDb ){
    S3JniMutex_S3JniDb_enter;
    s = SJG.perDb.aUsed;
    if( !pDb ){
      assert( jDb );
      pDb = PtrGet_sqlite3(jDb);
    }
    for( ; pDb && s; s = s->pNext){
      if( s->pDb == pDb ){
        break;
      }
    }
    S3JniMutex_S3JniDb_leave;
  }
  return s;
}

/*
** Unref any Java-side state in ax and zero out ax.
*/
static void S3JniAutoExtension_clear(JNIEnv * const env,
                                     S3JniAutoExtension * const ax){
  if( ax->jObj ){
    S3JniUnrefGlobal(ax->jObj);
    memset(ax, 0, sizeof(*ax));
  }
}

/*
** Initializes a pre-allocated S3JniAutoExtension object.  Returns
** non-0 if there is an error collecting the required state from
** jAutoExt (which must be an AutoExtension object). On error, it
** passes ax to S3JniAutoExtension_clear().
*/
static int S3JniAutoExtension_init(JNIEnv *const env,
                                   S3JniAutoExtension * const ax,
                                   jobject const jAutoExt){
  jclass const klazz = (*env)->GetObjectClass(env, jAutoExt);

  ax->midFunc = (*env)->GetMethodID(env, klazz, "call",
                                    "(Lorg/sqlite/jni/sqlite3;)I");
  S3JniUnrefLocal(klazz);
  S3JniExceptionWarnIgnore;
  if( !ax->midFunc ){
    MARKER(("Error getting call(sqlite3) from AutoExtension object.\n"));
    S3JniAutoExtension_clear(env, ax);
    return SQLITE_ERROR;
  }
  ax->jObj = S3JniRefGlobal(jAutoExt);
  return 0;
}

/*
** Common init for OutputPointer_set_Int32() and friends. pRef must be
** a pointer from S3NphRefs. jOut must be an instance of that
** class. If necessary, this fetches the jfieldID for jOut's [value]
** property, which must be of the type represented by the JNI type
** signature zTypeSig, and stores it in pRef's S3JniGlobal.nph entry.
** Fails fatally if the property is not found, as that presents a
** serious internal misuse.
**
** Property lookups are cached on a per-pRef basis. Do not use this
** routine with the same pRef but different zTypeSig: it will
** misbehave.
*/
static jfieldID setupOutputPointer(JNIEnv * const env, S3NphRef const * pRef,
                                   const char * const zTypeSig,
                                   jobject const jOut){
  S3JniNphClass * const pNC = S3JniGlobal_nph_cache(env, pRef);
  if( !pNC->fidValue ){
    S3JniMutex_Nph_enter;
    if( !pNC->fidValue ){
      pNC->fidValue = (*env)->GetFieldID(env, pNC->klazz, "value", zTypeSig);
      S3JniExceptionIsFatal("setupOutputPointer() could not find OutputPointer.*.value");
    }
    S3JniMutex_Nph_leave;
  }
  return pNC->fidValue;
}

/*
** Sets the value property of the OutputPointer.Int32 jOut object to
** v.
*/
static void OutputPointer_set_Int32(JNIEnv * const env, jobject const jOut, int v){
  jfieldID const setter = setupOutputPointer(
    env, &S3NphRefs.OutputPointer_Int32, "I", jOut
  );
  (*env)->SetIntField(env, jOut, setter, (jint)v);
  S3JniExceptionIsFatal("Cannot set OutputPointer.Int32.value");
}

/*
** Sets the value property of the OutputPointer.Int64 jOut object to
** v.
*/
static void OutputPointer_set_Int64(JNIEnv * const env, jobject const jOut, jlong v){
  jfieldID const setter = setupOutputPointer(
    env, &S3NphRefs.OutputPointer_Int64, "J", jOut
  );
  (*env)->SetLongField(env, jOut, setter, v);
  S3JniExceptionIsFatal("Cannot set OutputPointer.Int64.value");
}

/*
** Sets the value property of the OutputPointer.sqlite3 jOut object to
** v.
*/
static void OutputPointer_set_sqlite3(JNIEnv * const env, jobject const jOut,
                                      jobject jDb){
  jfieldID const setter = setupOutputPointer(
    env, &S3NphRefs.OutputPointer_sqlite3, "Lorg/sqlite/jni/sqlite3;", jOut
  );
  (*env)->SetObjectField(env, jOut, setter, jDb);
  S3JniExceptionIsFatal("Cannot set OutputPointer.sqlite3.value");
}

/*
** Sets the value property of the OutputPointer.sqlite3_stmt jOut object to
** v.
*/
static void OutputPointer_set_sqlite3_stmt(JNIEnv * const env, jobject const jOut,
                                           jobject jStmt){
  jfieldID const setter = setupOutputPointer(
    env, &S3NphRefs.OutputPointer_sqlite3_stmt,
    "Lorg/sqlite/jni/sqlite3_stmt;", jOut
  );
  (*env)->SetObjectField(env, jOut, setter, jStmt);
  S3JniExceptionIsFatal("Cannot set OutputPointer.sqlite3_stmt.value");
}

#ifdef SQLITE_ENABLE_PREUPDATE_HOOK
/*
** Sets the value property of the OutputPointer.sqlite3_value jOut object to
** v.
*/
static void OutputPointer_set_sqlite3_value(JNIEnv * const env, jobject const jOut,
                                            jobject jValue){
  jfieldID const setter = setupOutputPointer(
    env, &S3NphRefs.OutputPointer_sqlite3_value,
    "Lorg/sqlite/jni/sqlite3_value;", jOut
  );
  (*env)->SetObjectField(env, jOut, setter, jValue);
  S3JniExceptionIsFatal("Cannot set OutputPointer.sqlite3_value.value");
}
#endif /* SQLITE_ENABLE_PREUPDATE_HOOK */

#ifdef SQLITE_ENABLE_FTS5
#if 0
/*
** Sets the value property of the OutputPointer.ByteArray jOut object
** to v.
*/
static void OutputPointer_set_ByteArray(JNIEnv * const env, jobject const jOut,
                               jbyteArray const v){
  jfieldID const setter = setupOutputPointer(
    env, &S3NphRefs.OutputPointer_ByteArray, "[B", jOut
  );
  (*env)->SetObjectField(env, jOut, setter, v);
  S3JniExceptionIsFatal("Cannot set OutputPointer.ByteArray.value");
}
#endif

/*
** Sets the value property of the OutputPointer.String jOut object to
** v.
*/
static void OutputPointer_set_String(JNIEnv * const env, jobject const jOut,
                            jstring const v){
  jfieldID const setter = setupOutputPointer(
    env, &S3NphRefs.OutputPointer_String, "Ljava/lang/String;", jOut
  );
  (*env)->SetObjectField(env, jOut, setter, v);
  S3JniExceptionIsFatal("Cannot set OutputPointer.String.value");
}
#endif /* SQLITE_ENABLE_FTS5 */

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

/*
** Proxy for Java-side Collation.xCompare() callbacks.
*/
static int CollationState_xCompare(void *pArg, int nLhs, const void *lhs,
                                   int nRhs, const void *rhs){
  S3JniDb * const ps = pArg;
  LocalJniGetEnv;
  jint rc = 0;
  jbyteArray jbaLhs = (*env)->NewByteArray(env, (jint)nLhs);
  jbyteArray jbaRhs = jbaLhs ? (*env)->NewByteArray(env, (jint)nRhs) : NULL;
  if( !jbaRhs ){
    S3JniUnrefLocal(jbaLhs);
    s3jni_db_error(ps->pDb, SQLITE_NOMEM, 0);
    return 0;
  }
  (*env)->SetByteArrayRegion(env, jbaLhs, 0, (jint)nLhs, (const jbyte*)lhs);
  (*env)->SetByteArrayRegion(env, jbaRhs, 0, (jint)nRhs, (const jbyte*)rhs);
  rc = (*env)->CallIntMethod(env, ps->hooks.collation.jObj,
                             ps->hooks.collation.midCallback,
                             jbaLhs, jbaRhs);
  S3JniExceptionIgnore;
  S3JniUnrefLocal(jbaLhs);
  S3JniUnrefLocal(jbaRhs);
  return (int)rc;
}

/* Collation finalizer for use by the sqlite3 internals. */
static void CollationState_xDestroy(void *pArg){
  S3JniDb * const ps = pArg;
  S3JniHook_unref( s3jni_env(), &ps->hooks.collation, 1 );
}

/*
** State for sqlite3_result_java_object() and
** sqlite3_value_java_object().
**
** TODO: this middle-man struct is no longer necessary. Conider
** removing it and passing around jObj itself instead. OTOH, we might
** find more state to pack in here.
*/
typedef struct {
  jobject jObj;
} ResultJavaVal;

/* For use with sqlite3_result/value_pointer() */
#define ResultJavaValuePtrStr "org.sqlite.jni.ResultJavaVal"

/*
** Allocate a new ResultJavaVal and assign it a new global ref of
** jObj.  Caller owns the returned object and must eventually pass it
** to ResultJavaVal_finalizer().
*/
static ResultJavaVal * ResultJavaVal_alloc(JNIEnv * const env, jobject jObj){
  ResultJavaVal * rv = sqlite3_malloc(sizeof(ResultJavaVal));
  if( rv ){
    rv->jObj = jObj ? S3JniRefGlobal(jObj) : 0;
  }
  return rv;
}

/*
** If v is not NULL, it must point to a a ResultJavaVal object. Its
** object reference is relinquished and v is freed.
*/
static void ResultJavaVal_finalizer(void *v){
  if( v ){
    ResultJavaVal * const rv = (ResultJavaVal*)v;
    LocalJniGetEnv;
    S3JniUnrefGlobal(rv->jObj);
    sqlite3_free(rv);
  }
}



/*
** Returns a new Java instance of the class named by zClassName, which
** MUST be interface-compatible with NativePointerHolder and MUST have
** a no-arg constructor. The NativePointerHolder_set() method is
** passed the new Java object and pNative. Hypothetically returns NULL
** if Java fails to allocate, but the JNI docs are not entirely clear
** on that detail.
**
** Always use a static pointer from the S3NphRefs struct for the 2nd
** argument so that we can use pRef->index as an O(1) cache key.
*/
static jobject new_NativePointerHolder_object(JNIEnv * const env, S3NphRef const * pRef,
                                              const void * pNative){
  jobject rv = 0;
  S3JniNphClass * const pNC = S3JniGlobal_nph_cache(env, pRef);
  if( !pNC->midCtor ){
    S3JniMutex_Nph_enter;
    if( !pNC->midCtor ){
      pNC->midCtor = (*env)->GetMethodID(env, pNC->klazz, "<init>", "()V");
      S3JniExceptionIsFatal("Cannot find constructor for class.");
    }
    S3JniMutex_Nph_leave;
  }
  rv = (*env)->NewObject(env, pNC->klazz, pNC->midCtor);
  S3JniExceptionIsFatal("No-arg constructor threw.");
  s3jni_oom_check(rv);
  if( rv ) NativePointerHolder_set(env, rv, pNative, pRef);
  return rv;
}

static inline jobject new_sqlite3_wrapper(JNIEnv * const env, sqlite3 *sv){
  return new_NativePointerHolder_object(env, &S3NphRefs.sqlite3, sv);
}
static inline jobject new_sqlite3_context_wrapper(JNIEnv * const env, sqlite3_context *sv){
  return new_NativePointerHolder_object(env, &S3NphRefs.sqlite3_context, sv);
}
static inline jobject new_sqlite3_stmt_wrapper(JNIEnv * const env, sqlite3_stmt *sv){
  return new_NativePointerHolder_object(env, &S3NphRefs.sqlite3_stmt, sv);
}
static inline jobject new_sqlite3_value_wrapper(JNIEnv * const env, sqlite3_value *sv){
  return new_NativePointerHolder_object(env, &S3NphRefs.sqlite3_value, sv);
}

/*
** Type IDs for SQL function categories.
*/
enum UDFType {
  UDF_SCALAR = 1,
  UDF_AGGREGATE,
  UDF_WINDOW,
  UDF_UNKNOWN_TYPE/*for error propagation*/
};

typedef void (*udf_xFunc_f)(sqlite3_context*,int,sqlite3_value**);
typedef void (*udf_xStep_f)(sqlite3_context*,int,sqlite3_value**);
typedef void (*udf_xFinal_f)(sqlite3_context*);
/*typedef void (*udf_xValue_f)(sqlite3_context*);*/
/*typedef void (*udf_xInverse_f)(sqlite3_context*,int,sqlite3_value**);*/

/**
   State for binding Java-side UDFs.
*/
typedef struct S3JniUdf S3JniUdf;
struct S3JniUdf {
  jobject jObj          /* SQLFunction instance */;
  char * zFuncName      /* Only for error reporting and debug logging */;
  enum UDFType type;
  /** Method IDs for the various UDF methods. */
  jmethodID jmidxFunc;
  jmethodID jmidxStep;
  jmethodID jmidxFinal;
  jmethodID jmidxValue;
  jmethodID jmidxInverse;
};

static S3JniUdf * S3JniUdf_alloc(JNIEnv * const env, jobject jObj){
  S3JniUdf * const s = sqlite3_malloc(sizeof(S3JniUdf));
  if( s ){
    const char * zFSI = /* signature for xFunc, xStep, xInverse */
      "(Lorg/sqlite/jni/sqlite3_context;[Lorg/sqlite/jni/sqlite3_value;)V";
    const char * zFV = /* signature for xFinal, xValue */
      "(Lorg/sqlite/jni/sqlite3_context;)V";
    jclass const klazz = (*env)->GetObjectClass(env, jObj);

    memset(s, 0, sizeof(S3JniUdf));
    s->jObj = S3JniRefGlobal(jObj);
#define FGET(FuncName,FuncType,Field) \
    s->Field = (*env)->GetMethodID(env, klazz, FuncName, FuncType); \
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
      s->type = s->jmidxValue ? UDF_WINDOW : UDF_AGGREGATE;
    }else{
      s->type = UDF_UNKNOWN_TYPE;
    }
  }
  return s;
}

static void S3JniUdf_free(S3JniUdf * s){
  LocalJniGetEnv;
  if( env ){
    //MARKER(("UDF cleanup: %s\n", s->zFuncName));
    s3jni_call_xDestroy(env, s->jObj);
    S3JniUnrefGlobal(s->jObj);
  }
  sqlite3_free(s->zFuncName);
  sqlite3_free(s);
}

static void S3JniUdf_finalizer(void * s){
  //MARKER(("UDF finalizer @ %p\n", s));
  if( s ) S3JniUdf_free((S3JniUdf*)s);
}

/**
   Helper for processing args to UDF handlers
   with signature (sqlite3_context*,int,sqlite3_value**).
*/
typedef struct {
  jobject jcx;
  jobjectArray jargv;
} udf_jargs;

/**
   Converts the given (cx, argc, argv) into arguments for the given
   UDF, placing the result in the final argument. Returns 0 on
   success, SQLITE_NOMEM on allocation error.
*/
static int udf_args(JNIEnv *env,
                    sqlite3_context * const cx,
                    int argc, sqlite3_value**argv,
                    jobject * jCx, jobjectArray *jArgv){
  jobjectArray ja = 0;
  jobject jcx = new_sqlite3_context_wrapper(env, cx);
  jint i;
  *jCx = 0;
  *jArgv = 0;
  if( !jcx ) goto error_oom;
  ja = (*env)->NewObjectArray(env, argc,
                              SJG.g.cObj,
                              NULL);
  if( !ja ) goto error_oom;
  for(i = 0; i < argc; ++i){
    jobject jsv = new_sqlite3_value_wrapper(env, argv[i]);
    if( !jsv ) goto error_oom;
    (*env)->SetObjectArrayElement(env, ja, i, jsv);
    S3JniUnrefLocal(jsv)/*array has a ref*/;
  }
  *jCx = jcx;
  *jArgv = ja;
  return 0;
error_oom:
  sqlite3_result_error_nomem(cx);
  S3JniUnrefLocal(jcx);
  S3JniUnrefLocal(ja);
  return SQLITE_NOMEM;
}

/*
** Must be called immediately after a Java-side UDF callback throws.
** If translateToErr is true then it sets the exception's message in
** the result error using sqlite3_result_error(). If translateToErr is
** false then it emits a warning that the function threw but should
** not do so. In either case, it clears the exception state.
**
** Returns SQLITE_NOMEM if an allocation fails, else SQLITE_ERROR. In
** the latter case it calls sqlite3_result_error_nomem().
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
    MARKER(("Client-defined SQL function %s.%s() threw. "
            "It should not do that.\n",
            zFuncName ? zFuncName : "<unnamed>", zFuncType));
    (*env)->ExceptionDescribe( env );
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
                    sqlite3_value** const argv,
                    S3JniUdf * const s,
                    jmethodID xMethodID,
                    const char * const zFuncType){
  LocalJniGetEnv;
  udf_jargs args = {0,0};
  int rc = udf_args(env, pCx, argc, argv, &args.jcx, &args.jargv);

  //MARKER(("UDF::%s.%s()\n", s->zFuncName, zFuncType));
  if( 0 == rc ){
    (*env)->CallVoidMethod(env, s->jObj, xMethodID, args.jcx, args.jargv);
    S3JniIfThrew{
      rc = udf_report_exception(env, 'F'==zFuncType[1]/*xFunc*/, pCx,
                                s->zFuncName, zFuncType);
    }
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
  LocalJniGetEnv;
  jobject jcx = new_sqlite3_context_wrapper(env, cx);
  int rc = 0;
  int const isFinal = 'F'==zFuncType[1]/*xFinal*/;
  //MARKER(("%s.%s() cx = %p\n", s->zFuncName, zFuncType, cx));
  if( !jcx ){
    if( isFinal ) sqlite3_result_error_nomem(cx);
    return SQLITE_NOMEM;
  }
  //MARKER(("UDF::%s.%s()\n", s->zFuncName, zFuncType));
  (*env)->CallVoidMethod(env, s->jObj, xMethodID, jcx);
  S3JniIfThrew{
    rc = udf_report_exception(env, isFinal, cx, s->zFuncName,
                              zFuncType);
  }
  S3JniUnrefLocal(jcx);
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
#define WRAP_INT_VOID(JniNameSuffix,CName) \
  JniDecl(jint,JniNameSuffix)(JniArgsEnvClass){ \
    return (jint)CName(); \
  }
/** Create a trivial JNI wrapper for (int CName(int)). */
#define WRAP_INT_INT(JniNameSuffix,CName)               \
  JniDecl(jint,JniNameSuffix)(JniArgsEnvClass, jint arg){   \
    return (jint)CName((int)arg);                                    \
  }
/*
** Create a trivial JNI wrapper for (const mutf8_string *
** CName(void)). This is only valid for functions which are known to
** return ASCII or text which is equivalent in UTF-8 and MUTF-8.
*/
#define WRAP_MUTF8_VOID(JniNameSuffix,CName)                             \
  JniDecl(jstring,JniNameSuffix)(JniArgsEnvClass){                  \
    return (*env)->NewStringUTF( env, CName() );                        \
  }
/** Create a trivial JNI wrapper for (int CName(sqlite3_stmt*)). */
#define WRAP_INT_STMT(JniNameSuffix,CName) \
  JniDecl(jint,JniNameSuffix)(JniArgsEnvClass, jobject jpStmt){   \
    jint const rc = (jint)CName(PtrGet_sqlite3_stmt(jpStmt)); \
    S3JniExceptionIgnore /* squelch -Xcheck:jni */;        \
    return rc; \
  }
/** Create a trivial JNI wrapper for (int CName(sqlite3_stmt*,int)). */
#define WRAP_INT_STMT_INT(JniNameSuffix,CName) \
  JniDecl(jint,JniNameSuffix)(JniArgsEnvClass, jobject pStmt, jint n){ \
    return (jint)CName(PtrGet_sqlite3_stmt(pStmt), (int)n);                          \
  }
/** Create a trivial JNI wrapper for (jstring CName(sqlite3_stmt*,int)). */
#define WRAP_STR_STMT_INT(JniNameSuffix,CName)                          \
  JniDecl(jstring,JniNameSuffix)(JniArgsEnvClass, jobject pStmt, jint ndx){    \
    return s3jni_utf8_to_jstring(env,            \
                                 CName(PtrGet_sqlite3_stmt(pStmt), (int)ndx), \
                                 -1);                                   \
  }
/** Create a trivial JNI wrapper for (int CName(sqlite3*)). */
#define WRAP_INT_DB(JniNameSuffix,CName) \
  JniDecl(jint,JniNameSuffix)(JniArgsEnvClass, jobject pDb){   \
    return (jint)CName(PtrGet_sqlite3(pDb)); \
  }
/** Create a trivial JNI wrapper for (int64 CName(sqlite3*)). */
#define WRAP_INT64_DB(JniNameSuffix,CName) \
  JniDecl(jlong,JniNameSuffix)(JniArgsEnvClass, jobject pDb){   \
    return (jlong)CName(PtrGet_sqlite3(pDb)); \
  }
/** Create a trivial JNI wrapper for (int CName(sqlite3_value*)). */
#define WRAP_INT_SVALUE(JniNameSuffix,CName) \
  JniDecl(jint,JniNameSuffix)(JniArgsEnvClass, jobject jpSValue){   \
    return (jint)CName(PtrGet_sqlite3_value(jpSValue)); \
  }

WRAP_INT_STMT(1bind_1parameter_1count, sqlite3_bind_parameter_count)
WRAP_INT_DB(1changes,                  sqlite3_changes)
WRAP_INT64_DB(1changes64,              sqlite3_changes64)
WRAP_INT_STMT(1clear_1bindings,        sqlite3_clear_bindings)
WRAP_INT_STMT_INT(1column_1bytes,      sqlite3_column_bytes)
WRAP_INT_STMT_INT(1column_1bytes16,    sqlite3_column_bytes16)
WRAP_INT_STMT(1column_1count,          sqlite3_column_count)
WRAP_STR_STMT_INT(1column_1decltype,   sqlite3_column_decltype)
WRAP_STR_STMT_INT(1column_1name,       sqlite3_column_name)
WRAP_STR_STMT_INT(1column_1database_1name,  sqlite3_column_database_name)
WRAP_STR_STMT_INT(1column_1origin_1name,    sqlite3_column_origin_name)
WRAP_STR_STMT_INT(1column_1table_1name,     sqlite3_column_table_name)
WRAP_INT_STMT_INT(1column_1type,       sqlite3_column_type)
WRAP_INT_STMT(1data_1count,            sqlite3_data_count)
WRAP_INT_DB(1error_1offset,            sqlite3_error_offset)
WRAP_INT_DB(1extended_1errcode,        sqlite3_extended_errcode)
WRAP_MUTF8_VOID(1libversion,           sqlite3_libversion)
WRAP_INT_VOID(1libversion_1number,     sqlite3_libversion_number)
#ifdef SQLITE_ENABLE_PREUPDATE_HOOK
WRAP_INT_DB(1preupdate_1blobwrite,     sqlite3_preupdate_blobwrite)
WRAP_INT_DB(1preupdate_1count,         sqlite3_preupdate_count)
WRAP_INT_DB(1preupdate_1depth,         sqlite3_preupdate_depth)
#endif
WRAP_INT_INT(1sleep,                   sqlite3_sleep)
WRAP_MUTF8_VOID(1sourceid,             sqlite3_sourceid)
WRAP_INT_VOID(1threadsafe,             sqlite3_threadsafe)
WRAP_INT_DB(1total_1changes,           sqlite3_total_changes)
WRAP_INT64_DB(1total_1changes64,       sqlite3_total_changes64)
WRAP_INT_SVALUE(1value_1bytes,         sqlite3_value_bytes)
WRAP_INT_SVALUE(1value_1bytes16,       sqlite3_value_bytes16)
WRAP_INT_SVALUE(1value_1encoding,      sqlite3_value_encoding)
WRAP_INT_SVALUE(1value_1frombind,      sqlite3_value_frombind)
WRAP_INT_SVALUE(1value_1nochange,      sqlite3_value_nochange)
WRAP_INT_SVALUE(1value_1numeric_1type, sqlite3_value_numeric_type)
WRAP_INT_SVALUE(1value_1subtype,       sqlite3_value_subtype)
WRAP_INT_SVALUE(1value_1type,          sqlite3_value_type)

#undef WRAP_INT64_DB
#undef WRAP_INT_DB
#undef WRAP_INT_INT
#undef WRAP_INT_STMT
#undef WRAP_INT_STMT_INT
#undef WRAP_INT_SVALUE
#undef WRAP_INT_VOID
#undef WRAP_MUTF8_VOID
#undef WRAP_STR_STMT_INT


S3JniApi(sqlite3_aggregate_context(),jlong,1aggregate_1context)(
  JniArgsEnvClass, jobject jCx, jboolean initialize
){
  sqlite3_context * const pCx = PtrGet_sqlite3_context(jCx);
  void * const p = pCx
    ? sqlite3_aggregate_context(pCx, (int)(initialize
                                           ? (int)sizeof(void*)
                                           : 0))
    : 0;
  return (jlong)p / sizeof(void*);
}


/* Central auto-extension handler. */
static int s3jni_run_java_auto_extensions(sqlite3 *pDb, const char **pzErr,
                                          const struct sqlite3_api_routines *ignored){
  int rc = 0;
  unsigned i, go = 1;
  JNIEnv * env = 0;
  S3JniDb * ps;
  S3JniEnv * jc;

  if( 0==SJG.autoExt.nExt ) return 0;
  env = s3jni_env();
  jc = S3JniEnv_get(env);
  ps = jc->pdbOpening;
  if( !ps ){
    MARKER(("Unexpected arrival of null S3JniDb in auto-extension runner.\n"));
    *pzErr = sqlite3_mprintf("Unexpected arrival of null S3JniDb in auto-extension runner.");
    return SQLITE_ERROR;
  }
  jc->pdbOpening = 0;
  assert( !ps->pDb && "it's still being opened" );
  ps->pDb = pDb;
  assert( ps->jDb );
  NativePointerHolder_set(env, ps->jDb, pDb, &S3NphRefs.sqlite3);
  for( i = 0; go && 0==rc; ++i ){
    S3JniAutoExtension ax = {0,0}
      /* We need a copy of the auto-extension object, with our own
      ** local reference to it, to avoid a race condition with another
      ** thread manipulating the list during the call and invaliding
      ** what ax points to. */;
    S3JniMutex_Ext_enter;
    if( i >= SJG.autoExt.nExt ){
      go = 0;
    }else{
      ax.jObj = S3JniRefLocal(SJG.autoExt.pExt[i].jObj);
      ax.midFunc = SJG.autoExt.pExt[i].midFunc;
    }
    S3JniMutex_Ext_leave;
    if( ax.jObj ){
      rc = (*env)->CallIntMethod(env, ax.jObj, ax.midFunc, ps->jDb);
      S3JniUnrefLocal(ax.jObj);
      S3JniIfThrew {
        jthrowable const ex = (*env)->ExceptionOccurred(env);
        char * zMsg;
        S3JniExceptionClear;
        zMsg = s3jni_exception_error_msg(env, ex);
        S3JniUnrefLocal(ex);
        *pzErr = sqlite3_mprintf("auto-extension threw: %s", zMsg);
        sqlite3_free(zMsg);
        if( !rc ) rc = SQLITE_ERROR;
      }
    }
  }
  return rc;
}

S3JniApi(sqlite3_auto_extension(),jint,1auto_1extension)(
  JniArgsEnvClass, jobject jAutoExt
){
  static int once = 0;
  int i;
  S3JniAutoExtension * ax;
  int rc = 0;

  if( !jAutoExt ) return SQLITE_MISUSE;
  S3JniMutex_Ext_enter;
  for( i = 0; i < SJG.autoExt.nExt; ++i ){
    /* Look for match or first empty slot. */
    ax = &SJG.autoExt.pExt[i];
    if( ax->jObj && (*env)->IsSameObject(env, ax->jObj, jAutoExt) ){
      S3JniMutex_Ext_leave;
      return 0 /* this as a no-op. */;
    }
  }
  if( i == SJG.autoExt.nExt ){
    assert( SJG.autoExt.nExt <= SJG.autoExt.nAlloc );
    if( SJG.autoExt.nExt == SJG.autoExt.nAlloc ){
      unsigned n = 1 + SJG.autoExt.nAlloc;
      S3JniAutoExtension * const aNew =
        sqlite3_realloc( SJG.autoExt.pExt,
                         n * sizeof(S3JniAutoExtension) );
      if( !aNew ){
        rc = SQLITE_NOMEM;
      }else{
        SJG.autoExt.pExt = aNew;
        ++SJG.autoExt.nAlloc;
      }
    }
    if( 0==rc ){
      ax = &SJG.autoExt.pExt[SJG.autoExt.nExt];
      rc = S3JniAutoExtension_init(env, ax, jAutoExt);
      assert( rc ? 0==ax->jObj : 0!=ax->jObj );
    }
  }
  if( 0==rc ){
    if( 0==once && ++once ){
      rc = sqlite3_auto_extension( (void(*)(void))s3jni_run_java_auto_extensions );
      if( rc ){
        assert( ax );
        S3JniAutoExtension_clear(env, ax);
      }
    }
    if( 0==rc ){
      ++SJG.autoExt.nExt;
    }
  }
  S3JniMutex_Ext_leave;
  return rc;
}

S3JniApi(sqlite3_bind_blob(),jint,1bind_1blob)(
  JniArgsEnvClass, jobject jpStmt, jint ndx, jbyteArray baData, jint nMax
){
  jbyte * const pBuf = baData ? s3jni_jbytearray_bytes(baData) : 0;
  int const rc = sqlite3_bind_blob(PtrGet_sqlite3_stmt(jpStmt), (int)ndx,
                                   pBuf, (int)nMax, SQLITE_TRANSIENT);
  s3jni_jbytearray_release(baData,pBuf);
  return (jint)rc;
}

S3JniApi(sqlite3_bind_double(),jint,1bind_1double)(
  JniArgsEnvClass, jobject jpStmt, jint ndx, jdouble val
){
  return (jint)sqlite3_bind_double(PtrGet_sqlite3_stmt(jpStmt), (int)ndx, (double)val);
}

S3JniApi(sqlite3_bind_int(),jint,1bind_1int)(
  JniArgsEnvClass, jobject jpStmt, jint ndx, jint val
){
  return (jint)sqlite3_bind_int(PtrGet_sqlite3_stmt(jpStmt), (int)ndx, (int)val);
}

S3JniApi(sqlite3_bind_int64(),jint,1bind_1int64)(
  JniArgsEnvClass, jobject jpStmt, jint ndx, jlong val
){
  return (jint)sqlite3_bind_int64(PtrGet_sqlite3_stmt(jpStmt), (int)ndx, (sqlite3_int64)val);
}

S3JniApi(sqlite3_bind_null(),jint,1bind_1null)(
  JniArgsEnvClass, jobject jpStmt, jint ndx
){
  return (jint)sqlite3_bind_null(PtrGet_sqlite3_stmt(jpStmt), (int)ndx);
}

S3JniApi(sqlite3_bind_parameter_index(),jint,1bind_1parameter_1index)(
  JniArgsEnvClass, jobject jpStmt, jbyteArray jName
){
  int rc = 0;
  jbyte * const pBuf = s3jni_jbytearray_bytes(jName);
  if( pBuf ){
    rc = sqlite3_bind_parameter_index(PtrGet_sqlite3_stmt(jpStmt),
                                      (const char *)pBuf);
    s3jni_jbytearray_release(jName, pBuf);
  }
  return rc;
}

S3JniApi(sqlite3_bind_text(),jint,1bind_1text)(
  JniArgsEnvClass, jobject jpStmt, jint ndx, jbyteArray baData, jint nMax
){
  jbyte * const pBuf = baData ? s3jni_jbytearray_bytes(baData) : 0;
  int const rc = sqlite3_bind_text(PtrGet_sqlite3_stmt(jpStmt), (int)ndx,
                                   (const char *)pBuf,
                                   (int)nMax, SQLITE_TRANSIENT);
  s3jni_jbytearray_release(baData, pBuf);
  return (jint)rc;
}

S3JniApi(sqlite3_text16(),jint,1bind_1text16)(
  JniArgsEnvClass, jobject jpStmt, jint ndx, jbyteArray baData, jint nMax
){
  jbyte * const pBuf = baData ? s3jni_jbytearray_bytes(baData) : 0;
  int const rc = sqlite3_bind_text16(PtrGet_sqlite3_stmt(jpStmt), (int)ndx,
                                     pBuf, (int)nMax, SQLITE_TRANSIENT);
  s3jni_jbytearray_release(baData, pBuf);
  return (jint)rc;
}

S3JniApi(sqlite3_bind_zeroblob(),jint,1bind_1zeroblob)(
  JniArgsEnvClass, jobject jpStmt, jint ndx, jint n
){
  return (jint)sqlite3_bind_zeroblob(PtrGet_sqlite3_stmt(jpStmt), (int)ndx, (int)n);
}

S3JniApi(sqlite3_bind_zeroblob(),jint,1bind_1zeroblob64)(
  JniArgsEnvClass, jobject jpStmt, jint ndx, jlong n
){
  return (jint)sqlite3_bind_zeroblob(PtrGet_sqlite3_stmt(jpStmt), (int)ndx, (sqlite3_uint64)n);
}

/* Central C-to-Java busy handler proxy. */
static int s3jni_busy_handler(void* pState, int n){
  S3JniDb * const ps = (S3JniDb *)pState;
  int rc = 0;
  if( ps->hooks.busyHandler.jObj ){
    LocalJniGetEnv;
    rc = (*env)->CallIntMethod(env, ps->hooks.busyHandler.jObj,
                               ps->hooks.busyHandler.midCallback, (jint)n);
    S3JniIfThrew{
      S3JniExceptionWarnCallbackThrew("sqlite3_busy_handler() callback");
      rc = s3jni_db_exception(env, ps, SQLITE_ERROR,
                              "sqlite3_busy_handler() callback threw.");
    }
  }
  return rc;
}

S3JniApi(sqlite3_busy_handler(),jint,1busy_1handler)(
  JniArgsEnvClass, jobject jDb, jobject jBusy
){
  S3JniDb * const ps = S3JniDb_for_db(env, jDb, 0);
  int rc = 0;
  if( !ps ) return (jint)SQLITE_NOMEM;
  if( jBusy ){
    S3JniHook * const pHook = &ps->hooks.busyHandler;
    if( pHook->jObj && (*env)->IsSameObject(env, pHook->jObj, jBusy) ){
      /* Same object - this is a no-op. */
      return 0;
    }
    jclass klazz;
    S3JniHook_unref(env, pHook, 1);
    pHook->jObj = S3JniRefGlobal(jBusy);
    klazz = (*env)->GetObjectClass(env, jBusy);
    pHook->midCallback = (*env)->GetMethodID(env, klazz, "call", "(I)I");
    S3JniUnrefLocal(klazz);
    S3JniIfThrew {
      S3JniHook_unref(env, pHook, 0);
      rc = SQLITE_ERROR;
    }
    if( rc ){
      return rc;
    }
  }else{
    S3JniHook_unref(env, &ps->hooks.busyHandler, 1);
  }
  return jBusy
    ? sqlite3_busy_handler(ps->pDb, s3jni_busy_handler, ps)
    : sqlite3_busy_handler(ps->pDb, 0, 0);
}

S3JniApi(sqlite3_busy_timeout(),jint,1busy_1timeout)(
  JniArgsEnvClass, jobject jDb, jint ms
){
  S3JniDb * const ps = S3JniDb_for_db(env, jDb, 0);
  if( ps ){
    S3JniHook_unref(env, &ps->hooks.busyHandler, 1);
    return sqlite3_busy_timeout(ps->pDb, (int)ms);
  }
  return SQLITE_MISUSE;
}

S3JniApi(sqlite3_cancel_auto_extension(),jboolean,1cancel_1auto_1extension)(
  JniArgsEnvClass, jobject jAutoExt
){
  S3JniAutoExtension * ax;
  jboolean rc = JNI_FALSE;
  int i;
  S3JniMutex_Ext_enter;
  /* This algo mirrors the one in the core. */
  for( i = SJG.autoExt.nExt-1; i >= 0; --i ){
    ax = &SJG.autoExt.pExt[i];
    if( ax->jObj && (*env)->IsSameObject(env, ax->jObj, jAutoExt) ){
      S3JniAutoExtension_clear(env, ax);
      /* Move final entry into this slot. */
      --SJG.autoExt.nExt;
      *ax = SJG.autoExt.pExt[SJG.autoExt.nExt];
      memset(&SJG.autoExt.pExt[SJG.autoExt.nExt], 0,
             sizeof(S3JniAutoExtension));
      assert(! SJG.autoExt.pExt[SJG.autoExt.nExt].jObj );
      rc = JNI_TRUE;
      break;
    }
  }
  S3JniMutex_Ext_leave;
  return rc;
}


/* Wrapper for sqlite3_close(_v2)(). */
static jint s3jni_close_db(JNIEnv * const env, jobject jDb, int version){
  int rc = 0;
  S3JniDb * ps = 0;
  assert(version == 1 || version == 2);
  ps = S3JniDb_for_db(env, jDb, 0);
  if( ps ){
    rc = 1==version ? (jint)sqlite3_close(ps->pDb) : (jint)sqlite3_close_v2(ps->pDb);
    if( 0==rc ){
      S3JniDb_set_aside(env, ps)
        /* MUST come after close() because of ps->trace. */;
      NativePointerHolder_set(env, jDb, 0, &S3NphRefs.sqlite3);
    }
  }
  return (jint)rc;
}

S3JniApi(sqlite3_close_v2(),jint,1close_1v2)(
  JniArgsEnvClass, jobject pDb
){
  return s3jni_close_db(env, pDb, 2);
}

S3JniApi(sqlite3_close(),jint,1close)(
  JniArgsEnvClass, jobject pDb
){
  return s3jni_close_db(env, pDb, 1);
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

/* Central C-to-Java sqlite3_collation_needed16() hook impl. */
static void s3jni_collation_needed_impl16(void *pState, sqlite3 *pDb,
                                          int eTextRep, const void * z16Name){
  S3JniDb * const ps = pState;
  LocalJniGetEnv;
  unsigned int const nName = s3jni_utf16_strlen(z16Name);
  jstring jName = (*env)->NewString(env, (jchar const *)z16Name, nName);
  S3JniIfThrew{
    s3jni_db_error(ps->pDb, SQLITE_NOMEM, 0);
    S3JniExceptionClear;
  }else{
    (*env)->CallVoidMethod(env, ps->hooks.collationNeeded.jObj,
                           ps->hooks.collationNeeded.midCallback,
                           ps->jDb, (jint)eTextRep, jName);
    S3JniIfThrew{
      s3jni_db_exception(env, ps, 0, "sqlite3_collation_needed() callback threw");
    }
    S3JniUnrefLocal(jName);
  }
}

S3JniApi(sqlite3_collation_needed(),jint,1collation_1needed)(
  JniArgsEnvClass, jobject jDb, jobject jHook
){
  S3JniDb * const ps = S3JniDb_for_db(env, jDb, 0);
  jclass klazz;
  jobject pOld = 0;
  jmethodID xCallback;
  S3JniHook * const pHook = &ps->hooks.collationNeeded;
  int rc;

  if( !ps ) return SQLITE_MISUSE;
  pOld = pHook->jObj;
  if( pOld && jHook &&
     (*env)->IsSameObject(env, pOld, jHook) ){
    return 0;
  }
  if( !jHook ){
    S3JniUnrefGlobal(pOld);
    memset(pHook, 0, sizeof(S3JniHook));
    sqlite3_collation_needed(ps->pDb, 0, 0);
    return 0;
  }
  klazz = (*env)->GetObjectClass(env, jHook);
  xCallback = (*env)->GetMethodID(env, klazz, "call",
                                  "(Lorg/sqlite/jni/sqlite3;ILjava/lang/String;)I");
  S3JniUnrefLocal(klazz);
  S3JniIfThrew {
    rc = s3jni_db_exception(env, ps, SQLITE_MISUSE,
                            "Cannot not find matching callback on "
                            "collation-needed hook object.");
  }else{
    pHook->midCallback = xCallback;
    pHook->jObj = S3JniRefGlobal(jHook);
    S3JniUnrefGlobal(pOld);
    rc = sqlite3_collation_needed16(ps->pDb, ps, s3jni_collation_needed_impl16);
  }
  return rc;
}

S3JniApi(sqlite3_column_blob(),jbyteArray,1column_1blob)(
  JniArgsEnvClass, jobject jpStmt, jint ndx
){
  sqlite3_stmt * const pStmt = PtrGet_sqlite3_stmt(jpStmt);
  void const * const p = sqlite3_column_blob(pStmt, (int)ndx);
  int const n = p ? sqlite3_column_bytes(pStmt, (int)ndx) : 0;
  if( 0==p ) return NULL;
  else{
    jbyteArray const jba = (*env)->NewByteArray(env, n);
    (*env)->SetByteArrayRegion(env, jba, 0, n, (const jbyte *)p);
    return jba;
  }
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

S3JniApi(sqlite3_column_text(),jbyteArray,1column_1text_1utf8)(
  JniArgsEnvClass, jobject jpStmt, jint ndx
){
  sqlite3_stmt * const stmt = PtrGet_sqlite3_stmt(jpStmt);
  const int n = sqlite3_column_bytes(stmt, (int)ndx);
  const unsigned char * const p = sqlite3_column_text(stmt, (int)ndx);
  return p ? s3jni_new_jbyteArray(env, p, n) : NULL;
}

S3JniApi(sqlite3_column_text16(),jstring,1column_1text16)(
  JniArgsEnvClass, jobject jpStmt, jint ndx
){
  sqlite3_stmt * const stmt = PtrGet_sqlite3_stmt(jpStmt);
  const int n = sqlite3_column_bytes16(stmt, (int)ndx);
  const void * const p = sqlite3_column_text16(stmt, (int)ndx);
  return s3jni_text16_to_jstring(env, p, n);
}

S3JniApi(sqlite3_column_value(),jobject,1column_1value)(
  JniArgsEnvClass, jobject jpStmt, jint ndx
){
  sqlite3_value * const sv = sqlite3_column_value(PtrGet_sqlite3_stmt(jpStmt), (int)ndx);
  return new_sqlite3_value_wrapper(env, sv);
}


static int s3jni_commit_rollback_hook_impl(int isCommit, S3JniDb * const ps){
  LocalJniGetEnv;
  int rc = isCommit
    ? (int)(*env)->CallIntMethod(env, ps->hooks.commit.jObj,
                                 ps->hooks.commit.midCallback)
    : (int)((*env)->CallVoidMethod(env, ps->hooks.rollback.jObj,
                                   ps->hooks.rollback.midCallback), 0);
  S3JniIfThrew{
    S3JniExceptionClear;
    rc = s3jni_db_error(ps->pDb, SQLITE_ERROR, "hook callback threw.");
  }
  return rc;
}

static int s3jni_commit_hook_impl(void *pP){
  return s3jni_commit_rollback_hook_impl(1, pP);
}

static void s3jni_rollback_hook_impl(void *pP){
  (void)s3jni_commit_rollback_hook_impl(0, pP);
}

static jobject s3jni_commit_rollback_hook(int isCommit, JNIEnv * const env,
                                          jobject jDb, jobject jHook){
  S3JniDb * const ps = S3JniDb_for_db(env, jDb, 0);
  jclass klazz;
  jobject pOld = 0;
  jmethodID xCallback;
  S3JniHook * const pHook =
    isCommit ? &ps->hooks.commit : &ps->hooks.rollback;
  if( !ps ){
    s3jni_db_error(ps->pDb, SQLITE_NOMEM, 0);
    return 0;
  }
  pOld = pHook->jObj;
  if( pOld && jHook &&
      (*env)->IsSameObject(env, pOld, jHook) ){
    return pOld;
  }
  if( !jHook ){
    if( pOld ){
      jobject tmp = S3JniRefLocal(pOld);
      S3JniUnrefGlobal(pOld);
      pOld = tmp;
    }
    memset(pHook, 0, sizeof(S3JniHook));
    if( isCommit ) sqlite3_commit_hook(ps->pDb, 0, 0);
    else sqlite3_rollback_hook(ps->pDb, 0, 0);
    return pOld;
  }
  klazz = (*env)->GetObjectClass(env, jHook);
  xCallback = (*env)->GetMethodID(env, klazz, "call",
                                  isCommit ? "()I" : "()V");
  S3JniUnrefLocal(klazz);
  S3JniIfThrew {
    S3JniExceptionReport;
    S3JniExceptionClear;
    s3jni_db_error(ps->pDb, SQLITE_ERROR,
                   "Cannot not find matching callback on "
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
  return pOld;
}

S3JniApi(sqlite3_commit_hook(),jobject,1commit_1hook)(
  JniArgsEnvClass,jobject jDb, jobject jHook
){
  return s3jni_commit_rollback_hook(1, env, jDb, jHook);
}

S3JniApi(sqlite3_compileoption_get(),jstring,1compileoption_1get)(
  JniArgsEnvClass, jint n
){
  return (*env)->NewStringUTF( env, sqlite3_compileoption_get(n) )
    /* We know these to be ASCII, so MUTF-8 is fine. */;
}

S3JniApi(sqlite3_compileoption_used(),jboolean,1compileoption_1used)(
  JniArgsEnvClass, jstring name
){
  const char *zUtf8 = s3jni_jstring_to_mutf8(name);
  const jboolean rc =
    0==sqlite3_compileoption_used(zUtf8) ? JNI_FALSE : JNI_TRUE;
  s3jni_mutf8_release(name, zUtf8);
  return rc;
}

S3JniApi(sqlite3_config(/*for a small subset of options.*/),
         jint,1config__I)(JniArgsEnvClass, jint n){
  switch( n ){
    case SQLITE_CONFIG_SINGLETHREAD:
    case SQLITE_CONFIG_MULTITHREAD:
    case SQLITE_CONFIG_SERIALIZED:
      return sqlite3_config( n );
    default:
      return SQLITE_MISUSE;
  }
}

#ifdef SQLITE_ENABLE_SQLLOG
/* C-to-Java SQLITE_CONFIG_SQLLOG wrapper. */
static void s3jni_config_sqllog(void *ignored, sqlite3 *pDb, const char *z, int op){
  jobject jArg0 = 0;
  jstring jArg1 = 0;
  LocalJniGetEnv;
  S3JniDb * const ps = S3JniDb_for_db(env, 0, pDb);
  S3JniHook * const hook = &SJG.hooks.sqllog;

  if( !ps || !hook->jObj ) return;
  jArg0 = S3JniRefLocal(ps->jDb);
  switch( op ){
    case 0: /* db opened */
    case 1: /* SQL executed */
      jArg1 = s3jni_utf8_to_jstring(env, z, -1);
      break;
    case 2: /* db closed */
      break;
    default:
      (*env)->FatalError(env, "Unhandled 4th arg to SQLITE_CONFIG_SQLLOG.");
      break;
  }
  (*env)->CallVoidMethod(env, hook->jObj, hook->midCallback, jArg0, jArg1, op);
  S3JniIfThrew{
    S3JniExceptionWarnCallbackThrew("SQLITE_CONFIG_SQLLOG callback");
    S3JniExceptionClear;
  }
  S3JniUnrefLocal(jArg0);
  S3JniUnrefLocal(jArg1);
}
//! Requirement of SQLITE_CONFIG_SQLLOG.
void sqlite3_init_sqllog(void){
  sqlite3_config( SQLITE_CONFIG_SQLLOG, s3jni_config_sqllog, 0 );
}
#endif

S3JniApi(sqlite3_config(/* for SQLLOG */),
         jint,1config__Lorg_sqlite_jni_SQLLog_2)(JniArgsEnvClass, jobject jLog){
#ifdef SQLITE_ENABLE_SQLLOG
  S3JniHook tmpHook;
  S3JniHook * const hook = &tmpHook;
  S3JniHook * const hookOld = & SJG.hooks.sqllog;
  jclass klazz;
  int rc = 0;
  if( !jLog ){
    S3JniHook_unref(env, hookOld, 0);
    return 0;
  }
  if( hookOld->jObj && (*env)->IsSameObject(env, jLog, hookOld->jObj) ){
    return 0;
  }
  klazz = (*env)->GetObjectClass(env, jLog);
  hook->midCallback = (*env)->GetMethodID(env, klazz, "call",
                                          "(Lorg/sqlite/jni/sqlite3;"
                                          "Ljava/lang/String;"
                                          "I)V");
  S3JniUnrefLocal(klazz);
  if( !hook->midCallback ){
    S3JniExceptionWarnIgnore;
    S3JniHook_unref(env, hook, 0);
    return SQLITE_ERROR;
  }
  hook->jObj = S3JniRefGlobal(jLog);
  rc = sqlite3_config( SQLITE_CONFIG_SQLLOG, s3jni_config_sqllog, 0 );
  if( rc ){
    S3JniHook_unref(env, hook, 0);
  }else{
    S3JniHook_unref(env, hookOld, 0);
    *hookOld = *hook;
  }
  return rc;
#else
  MARKER(("Warning: built without SQLITE_ENABLE_SQLLOG.\n"));
  return SQLITE_MISUSE;
#endif
}

S3JniApi(sqlite3_context_db_handle(),jobject,1context_1db_1handle)(
  JniArgsEnvClass, jobject jpCx
){
  sqlite3 * const pDb = sqlite3_context_db_handle(PtrGet_sqlite3_context(jpCx));
  S3JniDb * const ps = pDb ? S3JniDb_for_db(env, 0, pDb) : 0;
  return ps ? ps->jDb : 0;
}

S3JniApi(sqlite3_create_collation() sqlite3_create_collation_v2(),
         jint,1create_1collation
)(JniArgsEnvClass, jobject jDb, jstring name, jint eTextRep, jobject oCollation){
  int rc;
  const char *zName;
  jclass klazz;
  S3JniDb * const ps = S3JniDb_for_db(env, jDb, 0);
  S3JniHook * const pHook = ps ? &ps->hooks.collation : 0;

  if( !pHook ) return SQLITE_MISUSE;
  klazz = (*env)->GetObjectClass(env, oCollation);
  pHook->midCallback = (*env)->GetMethodID(env, klazz, "call",
                                           "([B[B)I");
  S3JniUnrefLocal(klazz);
  S3JniIfThrew{
    S3JniUnrefLocal(klazz);
    return s3jni_db_error(ps->pDb, SQLITE_ERROR,
                          "Could not get xCompare() method for object.");
  }
  zName = s3jni_jstring_to_mutf8(name);
  rc = sqlite3_create_collation_v2(ps->pDb, zName, (int)eTextRep,
                                   ps, CollationState_xCompare,
                                   CollationState_xDestroy);
  s3jni_mutf8_release(name, zName);
  if( 0==rc ){
    pHook->jObj = S3JniRefGlobal(oCollation);
  }else{
    S3JniHook_unref(env, pHook, 1);
  }
  return (jint)rc;
}

S3JniApi(sqlite3_create_function() sqlite3_create_function_v2() sqlite3_create_window_function(),
         jint,1create_1function
)(JniArgsEnvClass, jobject jDb, jstring jFuncName, jint nArg,
  jint eTextRep, jobject jFunctor){
  S3JniUdf * s = 0;
  int rc;
  sqlite3 * const pDb = PtrGet_sqlite3(jDb);
  char * zFuncName = 0;

  if( !encodingTypeIsValid(eTextRep) ){
    return s3jni_db_error(pDb, SQLITE_FORMAT,
                          "Invalid function encoding option.");
  }
  s = S3JniUdf_alloc(env, jFunctor);
  if( !s ) return SQLITE_NOMEM;
  else if( UDF_UNKNOWN_TYPE==s->type ){
    rc = s3jni_db_error(pDb, SQLITE_MISUSE,
                        "Cannot unambiguously determine function type.");
    S3JniUdf_free(s);
    goto error_cleanup;
  }
  zFuncName = s3jni_jstring_to_utf8(env,jFuncName,0);
  if( !zFuncName ){
    rc = SQLITE_NOMEM;
    S3JniUdf_free(s);
    goto error_cleanup;
  }
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
  sqlite3_free(zFuncName);
  /* on sqlite3_create_function() error, s will be destroyed via
  ** create_function(), so we're not leaking s. */
  return (jint)rc;
}

S3JniApi(sqlite3_db_config(/*for MAINDBNAME*/),
         jint,1db_1config__Lorg_sqlite_jni_sqlite3_2ILjava_lang_String_2
)(JniArgsEnvClass, jobject jDb, jint op, jstring jStr){
  S3JniDb * const ps = S3JniDb_for_db(env, jDb, 0);
  int rc;
  char *zStr;

  switch( (ps && jStr) ? op : 0 ){
    case SQLITE_DBCONFIG_MAINDBNAME:
      zStr = s3jni_jstring_to_utf8(env, jStr, 0);
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
      break;
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
  jint,1db_1config__Lorg_sqlite_jni_sqlite3_2IILorg_sqlite_jni_OutputPointer_Int32_2
)(
  JniArgsEnvClass, jobject jDb, jint op, jint onOff, jobject jOut
){
  S3JniDb * const ps = S3JniDb_for_db(env, jDb, 0);
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
JniDecl(jint,1db_1config__Lorg_sqlite_jni_sqlite3_2IILorg_sqlite_jni_OutputPointer_00024Int32_2)(
  JniArgsEnvClass, jobject jDb, jint op, jint onOff, jobject jOut
){
  return JniFuncName(1db_1config__Lorg_sqlite_jni_sqlite3_2IILorg_sqlite_jni_OutputPointer_Int32_2)(
    env, jKlazz, jDb, op, onOff, jOut
  );
}

S3JniApi(sqlite3_db_filename(),jstring,1db_1filename)(
  JniArgsEnvClass, jobject jDb, jstring jDbName
){
  S3JniDb * const ps = S3JniDb_for_db(env, jDb, 0);
  char *zDbName;
  jstring jRv = 0;
  int nStr = 0;

  if( !ps || !jDbName ){
    return 0;
  }
  zDbName = s3jni_jstring_to_utf8(env, jDbName, &nStr);
  if( zDbName ){
    char const * zRv = sqlite3_db_filename(ps->pDb, zDbName);
    sqlite3_free(zDbName);
    if( zRv ){
      jRv = s3jni_utf8_to_jstring(env, zRv, -1);
    }
  }
  return jRv;
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
  return pDb ? s3jni_utf8_to_jstring(env, sqlite3_errmsg(pDb), -1) : 0;
}

S3JniApi(sqlite3_errstr(),jstring,1errstr)(
  JniArgsEnvClass, jint rcCode
){
  return (*env)->NewStringUTF(env, sqlite3_errstr((int)rcCode))
    /* We know these values to be plain ASCII, so pose no MUTF-8
    ** incompatibility */;
}

S3JniApi(sqlite3_expanded_sql(),jstring,1expanded_1sql)(
  JniArgsEnvClass, jobject jpStmt
){
  sqlite3_stmt * const pStmt = PtrGet_sqlite3_stmt(jpStmt);
  jstring rv = 0;
  if( pStmt ){
    char * zSql = sqlite3_expanded_sql(pStmt);
    s3jni_oom_check(zSql);
    if( zSql ){
      rv = s3jni_utf8_to_jstring(env, zSql, -1);
      sqlite3_free(zSql);
    }
  }
  return rv;
}

S3JniApi(sqlite3_extended_result_codes(),jboolean,1extended_1result_1codes)(
  JniArgsEnvClass, jobject jpDb,
                                         jboolean onoff
){
  int const rc = sqlite3_extended_result_codes(PtrGet_sqlite3(jpDb), onoff ? 1 : 0);
  return rc ? JNI_TRUE : JNI_FALSE;
}

S3JniApi(sqlite3_finalize(),jint,1finalize)(
  JniArgsEnvClass, jobject jpStmt
){
  int rc = 0;
  sqlite3_stmt * const pStmt = PtrGet_sqlite3_stmt(jpStmt);
  if( pStmt ){
    rc = sqlite3_finalize(pStmt);
    NativePointerHolder_set(env, jpStmt, 0, &S3NphRefs.sqlite3_stmt);
  }
  return rc;
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
  if( pDb ) sqlite3_interrupt(pDb);
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
** Uncaches the current JNIEnv from the S3JniGlobal state, clearing any
** resources owned by that cache entry and making that slot available
** for re-use. It is important that the Java-side decl of this
** function be declared as synchronous.
*/
JniDecl(jboolean,1java_1uncache_1thread)(JniArgsEnvClass){
  int rc;
  S3JniMutex_Env_enter;
  rc = S3JniEnv_uncache(env);
  S3JniMutex_Env_leave;
  return rc ? JNI_TRUE : JNI_FALSE;
}


S3JniApi(sqlite3_last_insert_rowid(),jlong,1last_1insert_1rowid)(
  JniArgsEnvClass, jobject jpDb
){
  return (jlong)sqlite3_last_insert_rowid(PtrGet_sqlite3(jpDb));
}

/* Pre-open() code common to sqlite3_open(_v2)(). */
static int s3jni_open_pre(JNIEnv * const env, S3JniEnv **jc,
                          jstring jDbName, char **zDbName,
                          S3JniDb ** ps){
  int rc = 0;
  jobject jDb = 0;
  *jc = S3JniEnv_get(env);
  if( !*jc ){
    rc = SQLITE_NOMEM;
    goto end;
  }
  *zDbName = jDbName ? s3jni_jstring_to_utf8(env, jDbName, 0) : 0;
  if( jDbName && !*zDbName ){
    rc = SQLITE_NOMEM;
    goto end;
  }
  jDb = new_sqlite3_wrapper(env, 0);
  if( !jDb ){
    sqlite3_free(*zDbName);
    *zDbName = 0;
    rc = SQLITE_NOMEM;
    goto end;
  }
  *ps = S3JniDb_alloc(env, 0, jDb);
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
** org.sqlite.jni.sqlite3 object which will hold the db's native
** pointer. theRc must be the result code of the open() op. If
** *ppDb is NULL then ps is set aside and its state cleared,
** else ps is associated with *ppDb. If *ppDb is not NULL then
** ps->jDb is stored in jOut (an OutputPointer.sqlite3 instance).
**
** Returns theRc.
*/
static int s3jni_open_post(JNIEnv * const env, S3JniEnv * const jc,
                           S3JniDb * ps, sqlite3 **ppDb,
                           jobject jOut, int theRc){
  jc->pdbOpening = 0;
  if( *ppDb ){
    assert(ps->jDb);
    if( 0==ps->pDb ){
      ps->pDb = *ppDb;
      NativePointerHolder_set(env, ps->jDb, *ppDb, &S3NphRefs.sqlite3);
    }else{
      assert( ps->pDb == *ppDb /* set up via s3jni_run_java_auto_extensions() */);
    }
  }else{
    S3JniDb_set_aside(env, ps);
    ps = 0;
  }
  OutputPointer_set_sqlite3(env, jOut, ps ? ps->jDb : 0);
  return theRc;
}

S3JniApi(sqlite3_open(),jint,1open)(
  JniArgsEnvClass, jstring strName, jobject jOut
){
  sqlite3 * pOut = 0;
  char *zName = 0;
  S3JniDb * ps = 0;
  S3JniEnv * jc = 0;
  int rc;
  rc = s3jni_open_pre(env, &jc, strName, &zName, &ps);
  if( 0==rc ){
    rc = sqlite3_open(zName, &pOut);
    rc = s3jni_open_post(env, jc, ps, &pOut, jOut, rc);
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
  int rc = s3jni_open_pre(env, &jc, strName, &zName, &ps);
  if( 0==rc && strVfs ){
    zVfs = s3jni_jstring_to_utf8(env, strVfs, 0);
    if( !zVfs ){
      rc = SQLITE_NOMEM;
    }
  }
  if( 0==rc ){
    rc = sqlite3_open_v2(zName, &pOut, (int)flags, zVfs);
  }
  rc = s3jni_open_post(env, jc, ps, &pOut, jOut, rc);
  assert(rc==0 ? pOut!=0 : 1);
  sqlite3_free(zName);
  sqlite3_free(zVfs);
  return (jint)rc;
}

/* Proxy for the sqlite3_prepare[_v2/3]() family. */
jint sqlite3_jni_prepare_v123( int prepVersion, JNIEnv * const env, jclass self,
                               jobject jDb, jbyteArray baSql,
                               jint nMax, jint prepFlags,
                               jobject jOutStmt, jobject outTail){
  sqlite3_stmt * pStmt = 0;
  jobject jStmt = 0;
  const char * zTail = 0;
  jbyte * const pBuf = s3jni_jbytearray_bytes(baSql);
  int rc = SQLITE_ERROR;
  assert(prepVersion==1 || prepVersion==2 || prepVersion==3);
  if( !pBuf ){
     rc = baSql ? SQLITE_MISUSE : SQLITE_NOMEM;
     goto end;
  }
  jStmt = new_sqlite3_stmt_wrapper(env, 0);
  if( !jStmt ){
    rc = SQLITE_NOMEM;
    goto end;
  }
  switch( prepVersion ){
    case 1: rc = sqlite3_prepare(PtrGet_sqlite3(jDb), (const char *)pBuf,
                                 (int)nMax, &pStmt, &zTail);
      break;
    case 2: rc = sqlite3_prepare_v2(PtrGet_sqlite3(jDb), (const char *)pBuf,
                                    (int)nMax, &pStmt, &zTail);
      break;
    case 3: rc = sqlite3_prepare_v3(PtrGet_sqlite3(jDb), (const char *)pBuf,
                                    (int)nMax, (unsigned int)prepFlags,
                                    &pStmt, &zTail);
      break;
    default:
      assert(0 && "Invalid prepare() version");
  }
end:
  s3jni_jbytearray_release(baSql,pBuf);
  if( 0==rc ){
    if( 0!=outTail ){
      /* Noting that pBuf is deallocated now but its address is all we need for
      ** what follows... */
      assert(zTail ? ((void*)zTail>=(void*)pBuf) : 1);
      assert(zTail ? (((int)((void*)zTail - (void*)pBuf)) >= 0) : 1);
      OutputPointer_set_Int32(env, outTail, (int)(zTail ? (zTail - (const char *)pBuf) : 0));
    }
    if( pStmt ){
      NativePointerHolder_set(env, jStmt, pStmt, &S3NphRefs.sqlite3_stmt);
    }else{
      /* Happens for comments and whitespace. */
      S3JniUnrefLocal(jStmt);
      jStmt = 0;
    }
  }else{
    S3JniUnrefLocal(jStmt);
    jStmt = 0;
  }
#if 0
  if( 0!=rc ){
    MARKER(("prepare rc = %d\n", rc));
  }
#endif
  OutputPointer_set_sqlite3_stmt(env, jOutStmt, jStmt);
  return (jint)rc;
}
S3JniApi(sqlite3_prepare(),jint,1prepare)(
  JNIEnv * const env, jclass self, jobject jDb, jbyteArray baSql,
                     jint nMax, jobject jOutStmt, jobject outTail
){
  return sqlite3_jni_prepare_v123(1, env, self, jDb, baSql, nMax, 0,
                                  jOutStmt, outTail);
}
S3JniApi(sqlite3_prepare_v2(),jint,1prepare_1v2)(
  JNIEnv * const env, jclass self, jobject jDb, jbyteArray baSql,
                         jint nMax, jobject jOutStmt, jobject outTail
){
  return sqlite3_jni_prepare_v123(2, env, self, jDb, baSql, nMax, 0,
                                  jOutStmt, outTail);
}
S3JniApi(sqlite3_prepare_v3(),jint,1prepare_1v3)(
  JNIEnv * const env, jclass self, jobject jDb, jbyteArray baSql,
                         jint nMax, jint prepFlags, jobject jOutStmt, jobject outTail
){
  return sqlite3_jni_prepare_v123(3, env, self, jDb, baSql, nMax,
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
  LocalJniGetEnv;
  jstring jDbName;
  jstring jTable;
  S3JniHook * pHook;
  const int isPre = 0!=pDb;

  pHook = isPre ?
#ifdef SQLITE_ENABLE_PREUPDATE_HOOK
    &ps->hooks.preUpdate
#else
    0
#endif
    : &ps->hooks.update;

  assert( pHook );
  jDbName  = s3jni_utf8_to_jstring(env, zDb, -1);
  jTable = jDbName ? s3jni_utf8_to_jstring(env, zTable, -1) : 0;
  S3JniIfThrew {
    S3JniExceptionClear;
    s3jni_db_error(ps->pDb, SQLITE_NOMEM, 0);
  }else{
    assert( pHook->jObj );
    assert( pHook->midCallback );
    assert( ps->jDb );
#ifdef SQLITE_ENABLE_PREUPDATE_HOOK
    if( isPre ) (*env)->CallVoidMethod(env, pHook->jObj, pHook->midCallback,
                                       ps->jDb, (jint)opId, jDbName, jTable,
                                       (jlong)iKey1, (jlong)iKey2);
    else
#endif
    (*env)->CallVoidMethod(env, pHook->jObj, pHook->midCallback,
                           (jint)opId, jDbName, jTable, (jlong)iKey1);
    S3JniIfThrew{
      S3JniExceptionWarnCallbackThrew("sqlite3_(pre)update_hook() callback");
      s3jni_db_exception(env, ps, 0,
                         "sqlite3_(pre)update_hook() callback threw");
    }
  }
  S3JniUnrefLocal(jDbName);
  S3JniUnrefLocal(jTable);
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

#ifndef SQLITE_ENABLE_PREUPDATE_HOOK
/* We need no-op impls for preupdate_{count,depth,blobwrite}() */
S3JniApi(sqlite3_preupdate_blobwrite(),int,1preupdate_1blobwrite)(
  JniArgsEnvClass, jobject jDb){ return SQLITE_MISUSE; }
S3JniApi(sqlite3_preupdate_count(),int,1preupdate_1count)(
  JniArgsEnvClass, jobject jDb){ return SQLITE_MISUSE; }
S3JniApi(sqlite3_preupdate_depth(),int,1preupdate_1depth)(
  JniArgsEnvClass, jobject jDb){ return SQLITE_MISUSE; }
#endif /* !SQLITE_ENABLE_PREUPDATE_HOOK */

/*
** JNI wrapper for both sqlite3_update_hook() and
** sqlite3_preupdate_hook() (if isPre is true).
*/
static jobject s3jni_updatepre_hook(JNIEnv * env, int isPre, jobject jDb, jobject jHook){
  S3JniDb * const ps = S3JniDb_for_db(env, jDb, 0);
  jclass klazz;
  jobject pOld = 0;
  jmethodID xCallback;
  S3JniHook * pHook = ps ? (
    isPre ?
#ifdef SQLITE_ENABLE_PREUPDATE_HOOK
    &ps->hooks.preUpdate
#else
    0
#endif
    : &ps->hooks.update) : 0;

  if( !pHook ){
    return 0;
  }
  pOld = pHook->jObj;
  if( pOld && jHook && (*env)->IsSameObject(env, pOld, jHook) ){
    return pOld;
  }
  if( !jHook ){
    if( pOld ){
      jobject tmp = S3JniRefLocal(pOld);
      S3JniUnrefGlobal(pOld);
      pOld = tmp;
    }
    memset(pHook, 0, sizeof(S3JniHook));
#ifdef SQLITE_ENABLE_PREUPDATE_HOOK
    if( isPre ) sqlite3_preupdate_hook(ps->pDb, 0, 0);
    else
#endif
    sqlite3_update_hook(ps->pDb, 0, 0);
    return pOld;
  }
  klazz = (*env)->GetObjectClass(env, jHook);
  xCallback = isPre
    ? (*env)->GetMethodID(env, klazz, "call",
                          "(Lorg/sqlite/jni/sqlite3;"
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
  return pOld;
}


S3JniApi(sqlite3_preupdate_hook(),jobject,1preupdate_1hook)(
  JniArgsEnvClass, jobject jDb, jobject jHook
){
#ifdef SQLITE_ENABLE_PREUPDATE_HOOK
  return s3jni_updatepre_hook(env, 1, jDb, jHook);
#else
  return NULL;
#endif /* SQLITE_ENABLE_PREUPDATE_HOOK */
}

/* Impl for sqlite3_preupdate_{new,old}(). */
static int s3jni_preupdate_newold(JNIEnv * const env, int isNew, jobject jDb,
                                  jint iCol, jobject jOut){
#ifdef SQLITE_ENABLE_PREUPDATE_HOOK
  sqlite3_value * pOut = 0;
  sqlite3 * const pDb = PtrGet_sqlite3(jDb);
  int rc;
  int (*fOrig)(sqlite3*,int,sqlite3_value**) =
    isNew ? sqlite3_preupdate_new : sqlite3_preupdate_old;
  rc = fOrig(pDb, (int)iCol, &pOut);
  if( 0==rc ){
    jobject pWrap = new_sqlite3_value_wrapper(env, pOut);
    if( pWrap ){
      OutputPointer_set_sqlite3_value(env, jOut, pWrap);
      S3JniUnrefLocal(pWrap);
    }else{
      rc = SQLITE_NOMEM;
    }
  }
  return rc;
#else
  return SQLITE_MISUSE;
#endif
}

S3JniApi(sqlite3_preupdate_new(),jint,1preupdate_1new)(
  JniArgsEnvClass, jobject jDb, jint iCol, jobject jOut
){
  return s3jni_preupdate_newold(env, 1, jDb, iCol, jOut);
}

S3JniApi(sqlite3_preupdate_old(),jint,1preupdate_1old)(
  JniArgsEnvClass, jobject jDb, jint iCol, jobject jOut
){
  return s3jni_preupdate_newold(env, 0, jDb, iCol, jOut);
}


/* Central C-to-Java sqlite3_progress_handler() proxy. */
static int s3jni_progress_handler_impl(void *pP){
  S3JniDb * const ps = (S3JniDb *)pP;
  LocalJniGetEnv;
  int rc = (int)(*env)->CallIntMethod(env, ps->hooks.progress.jObj,
                                      ps->hooks.progress.midCallback);
  S3JniIfThrew{
    rc = s3jni_db_exception(env, ps, rc,
                            "sqlite3_progress_handler() callback threw");
  }
  return rc;
}

S3JniApi(sqlite3_progress_handler(),void,1progress_1handler)(
  JniArgsEnvClass,jobject jDb, jint n, jobject jProgress
){
  S3JniDb * ps = S3JniDb_for_db(env, jDb, 0);
  jclass klazz;
  jmethodID xCallback;
  if( n<1 || !jProgress ){
    if( ps ){
      S3JniUnrefGlobal(ps->hooks.progress.jObj);
      memset(&ps->hooks.progress, 0, sizeof(ps->hooks.progress));
    }
    sqlite3_progress_handler(ps->pDb, 0, 0, 0);
    return;
  }
  if( !ps ){
    s3jni_db_error(ps->pDb, SQLITE_NOMEM, 0);
    return;
  }
  klazz = (*env)->GetObjectClass(env, jProgress);
  xCallback = (*env)->GetMethodID(env, klazz, "call", "()I");
  S3JniUnrefLocal(klazz);
  S3JniIfThrew {
    S3JniExceptionClear;
    s3jni_db_error(ps->pDb, SQLITE_ERROR,
                   "Cannot not find matching xCallback() on "
                   "ProgressHandler object.");
  }else{
    S3JniUnrefGlobal(ps->hooks.progress.jObj);
    ps->hooks.progress.midCallback = xCallback;
    ps->hooks.progress.jObj = S3JniRefGlobal(jProgress);
    sqlite3_progress_handler(ps->pDb, (int)n, s3jni_progress_handler_impl, ps);
  }
}

S3JniApi(sqlite3_reset(),jint,1reset)(
  JniArgsEnvClass, jobject jpStmt
){
  int rc = 0;
  sqlite3_stmt * const pStmt = PtrGet_sqlite3_stmt(jpStmt);
  if( pStmt ){
    rc = sqlite3_reset(pStmt);
  }
  return rc;
}

/* Clears all entries from S3JniGlobal.autoExt. */
static void s3jni_reset_auto_extension(JNIEnv *env){
  int i;
  S3JniMutex_Ext_enter;
  for( i = 0; i < SJG.autoExt.nExt; ++i ){
    S3JniAutoExtension_clear( env, &SJG.autoExt.pExt[i] );
  }
  SJG.autoExt.nExt = 0;
  S3JniMutex_Ext_leave;
}

S3JniApi(sqlite3_reset_auto_extension(),void,1reset_1auto_1extension)(
  JniArgsEnvClass
){
  s3jni_reset_auto_extension(env);
}

/* Impl for sqlite3_result_text/blob() and friends. */
static void result_blob_text(int as64,
                             int eTextRep/*only for (asBlob=0)*/,
                             JNIEnv * const env, sqlite3_context *pCx,
                             jbyteArray jBa, jlong nMax){
  int const asBlob = 0==eTextRep;
  if( jBa ){
    jbyte * const pBuf = s3jni_jbytearray_bytes(jBa);
    jsize nBa = (*env)->GetArrayLength(env, jBa);
    if( nMax>=0 && nBa>(jsize)nMax ){
      nBa = (jsize)nMax;
      /**
         From the sqlite docs:

         > If the 3rd parameter to any of the sqlite3_result_text*
           interfaces other than sqlite3_result_text64() is negative,
           then SQLite computes the string length itself by searching
           the 2nd parameter for the first zero character.

         Note that the text64() interfaces take an unsigned value for
         the length, which Java does not support. This binding takes
         the approach of passing on negative values to the C API,
         which will, in turn fail with SQLITE_TOOBIG at some later
         point (recall that the sqlite3_result_xyz() family do not
         have result values).
      */
    }
    if( as64 ){ /* 64-bit... */
      static const jsize nLimit64 =
        SQLITE_MAX_ALLOCATION_SIZE/*only _kinda_ arbitrary!*/
        /* jsize is int32, not int64! */;
      if( nBa > nLimit64 ){
        sqlite3_result_error_toobig(pCx);
      }else if( asBlob ){
        sqlite3_result_blob64(pCx, pBuf, (sqlite3_uint64)nBa,
                              SQLITE_TRANSIENT);
      }else{ /* text64... */
        if( encodingTypeIsValid(eTextRep) ){
          sqlite3_result_text64(pCx, (const char *)pBuf,
                                (sqlite3_uint64)nBa,
                                SQLITE_TRANSIENT, eTextRep);
        }else{
          sqlite3_result_error_code(pCx, SQLITE_FORMAT);
        }
      }
    }else{ /* 32-bit... */
      static const jsize nLimit = SQLITE_MAX_ALLOCATION_SIZE;
      if( nBa > nLimit ){
        sqlite3_result_error_toobig(pCx);
      }else if( asBlob ){
        sqlite3_result_blob(pCx, pBuf, (int)nBa,
                            SQLITE_TRANSIENT);
      }else{
        switch( eTextRep ){
          case SQLITE_UTF8:
            sqlite3_result_text(pCx, (const char *)pBuf, (int)nBa,
                                SQLITE_TRANSIENT);
            break;
          case SQLITE_UTF16:
            sqlite3_result_text16(pCx, (const char *)pBuf, (int)nBa,
                                  SQLITE_TRANSIENT);
            break;
          case SQLITE_UTF16LE:
            sqlite3_result_text16le(pCx, (const char *)pBuf, (int)nBa,
                                    SQLITE_TRANSIENT);
            break;
          case SQLITE_UTF16BE:
            sqlite3_result_text16be(pCx, (const char *)pBuf, (int)nBa,
                                    SQLITE_TRANSIENT);
            break;
        }
      }
      s3jni_jbytearray_release(jBa, pBuf);
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
  JniArgsEnvClass, jobject jpCx, jbyteArray baMsg,
                           int eTextRep
){
  const char * zUnspecified = "Unspecified error.";
  jsize const baLen = (*env)->GetArrayLength(env, baMsg);
  jbyte * const pjBuf = baMsg ? s3jni_jbytearray_bytes(baMsg) : NULL;
  switch( pjBuf ? eTextRep : SQLITE_UTF8 ){
    case SQLITE_UTF8: {
      const char *zMsg = pjBuf ? (const char *)pjBuf : zUnspecified;
      sqlite3_result_error(PtrGet_sqlite3_context(jpCx), zMsg, (int)baLen);
      break;
    }
    case SQLITE_UTF16: {
      const void *zMsg = pjBuf
        ? (const void *)pjBuf : (const void *)zUnspecified;
      sqlite3_result_error16(PtrGet_sqlite3_context(jpCx), zMsg, (int)baLen);
      break;
    }
    default:
      sqlite3_result_error(PtrGet_sqlite3_context(jpCx),
                           "Invalid encoding argument passed "
                           "to sqlite3_result_error().", -1);
      break;
  }
  s3jni_jbytearray_release(baMsg,pjBuf);
}

S3JniApi(sqlite3_result_error_code(),void,1result_1error_1code)(
  JniArgsEnvClass, jobject jpCx, jint v
){
  sqlite3_result_error_code(PtrGet_sqlite3_context(jpCx), v ? (int)v : SQLITE_ERROR);
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
  if( v ){
    ResultJavaVal * const rjv = ResultJavaVal_alloc(env, v);
    if( rjv ){
      sqlite3_result_pointer(PtrGet_sqlite3_context(jpCx), rjv,
                             ResultJavaValuePtrStr, ResultJavaVal_finalizer);
    }else{
      sqlite3_result_error_nomem(PtrGet_sqlite3_context(jpCx));
    }
  }else{
    sqlite3_result_null(PtrGet_sqlite3_context(jpCx));
  }
}

S3JniApi(sqlite3_result_null(),void,1result_1null)(
  JniArgsEnvClass, jobject jpCx
){
  sqlite3_result_null(PtrGet_sqlite3_context(jpCx));
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
  JniArgsEnvClass, jobject jDb, jobject jHook
){
  return s3jni_commit_rollback_hook(0, env, jDb, jHook);
}

/* Callback for sqlite3_set_authorizer(). */
int s3jni_xAuth(void* pState, int op,const char*z0, const char*z1,
                const char*z2,const char*z3){
  S3JniDb * const ps = pState;
  LocalJniGetEnv;
  S3JniHook const * const pHook = &ps->hooks.auth;
  jstring const s0 = z0 ? s3jni_utf8_to_jstring(env, z0, -1) : 0;
  jstring const s1 = z1 ? s3jni_utf8_to_jstring(env, z1, -1) : 0;
  jstring const s2 = z2 ? s3jni_utf8_to_jstring(env, z2, -1) : 0;
  jstring const s3 = z3 ? s3jni_utf8_to_jstring(env, z3, -1) : 0;
  int rc;

  assert( pHook->jObj );
  rc = (*env)->CallIntMethod(env, pHook->jObj, pHook->midCallback, (jint)op,
                             s0, s1, s3, s3);
  S3JniIfThrew{
    rc = s3jni_db_exception(env, ps, rc, "sqlite3_set_authorizer() callback");
  }
  S3JniUnrefLocal(s0);
  S3JniUnrefLocal(s1);
  S3JniUnrefLocal(s2);
  S3JniUnrefLocal(s3);
  return rc;
}

S3JniApi(sqlite3_set_authorizer(),jint,1set_1authorizer)(
  JniArgsEnvClass,jobject jDb, jobject jHook
){
  S3JniDb * const ps = S3JniDb_for_db(env, jDb, 0);
  S3JniHook * const pHook = ps ? &ps->hooks.auth : 0;

  if( !ps ) return SQLITE_MISUSE;
  else if( !jHook ){
    S3JniHook_unref(env, pHook, 0);
    return (jint)sqlite3_set_authorizer( ps->pDb, 0, 0 );
  }else{
    int rc = 0;
    jclass klazz;
    if( pHook->jObj ){
      if( (*env)->IsSameObject(env, pHook->jObj, jHook) ){
      /* Same object - this is a no-op. */
        return 0;
      }
      S3JniHook_unref(env, pHook, 0);
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
      S3JniHook_unref(env, pHook, 0);
      return s3jni_db_error(ps->pDb, SQLITE_ERROR,
                            "Error setting up Java parts of authorizer hook.");
    }
    rc = sqlite3_set_authorizer(ps->pDb, s3jni_xAuth, ps);
    if( rc ) S3JniHook_unref(env, pHook, 0);
    return rc;
  }
}


S3JniApi(sqlite3_set_last_insert_rowid(),void,1set_1last_1insert_1rowid)(
  JniArgsEnvClass, jobject jpDb, jlong rowId
){
  sqlite3_set_last_insert_rowid(PtrGet_sqlite3_context(jpDb),
                                (sqlite3_int64)rowId);
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

static int s3jni_strlike_glob(int isLike, JNIEnv *const env,
                              jbyteArray baG, jbyteArray baT, jint escLike){
  int rc = 0;
  jbyte * const pG = s3jni_jbytearray_bytes(baG);
  jbyte * const pT = pG ? s3jni_jbytearray_bytes(baT) : 0;

  s3jni_oom_check(pT);
  /* Note that we're relying on the byte arrays having been
     NUL-terminated on the Java side. */
  rc = isLike
    ? sqlite3_strlike((const char *)pG, (const char *)pT,
                      (unsigned int)escLike)
    : sqlite3_strglob((const char *)pG, (const char *)pT);
  s3jni_jbytearray_release(baG, pG);
  s3jni_jbytearray_release(baT, pT);
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

S3JniApi(sqlite3_shutdown(),jint,1shutdown)(
  JniArgsEnvClass
){
  s3jni_reset_auto_extension(env);
  S3JniMutex_Env_enter;
  while( SJG.envCache.aHead ){
    S3JniEnv_uncache( SJG.envCache.aHead->env );
  }
  S3JniMutex_Env_leave;
#if 0
  /*
  ** Is this a good idea? We will get rid of the perDb list once
  ** sqlite3 gets a per-db client state, at which point we won't have
  ** a central list of databases to close.
  */
  S3JniMutex_S3JniDb_enter;
  while( SJG.perDb.pHead ){
    s3jni_close_db(env, SJG.perDb.pHead->jDb, 2);
  }
  S3JniMutex_S3JniDb_leave;
#endif
  /* Do not clear S3JniGlobal.jvm: it's legal to restart the lib. */
  return sqlite3_shutdown();
}

S3JniApi(sqlite3_sql(),jstring,1sql)(
  JniArgsEnvClass, jobject jpStmt
){
  sqlite3_stmt * const pStmt = PtrGet_sqlite3_stmt(jpStmt);
  jstring rv = 0;
  if( pStmt ){
    const char * zSql = 0;
    zSql = sqlite3_sql(pStmt);
    rv = s3jni_utf8_to_jstring(env, zSql, -1);
    s3jni_oom_check(rv);
  }
  return rv;
}

S3JniApi(sqlite3_step(),jint,1step)(
  JniArgsEnvClass,jobject jStmt
){
  int rc = SQLITE_MISUSE;
  sqlite3_stmt * const pStmt = PtrGet_sqlite3_stmt(jStmt);
  if( pStmt ){
    rc = sqlite3_step(pStmt);
  }
  return rc;
}

static int s3jni_trace_impl(unsigned traceflag, void *pC, void *pP, void *pX){
  S3JniDb * const ps = (S3JniDb *)pC;
  LocalJniGetEnv;
  jobject jX = NULL  /* the tracer's X arg */;
  jobject jP = NULL  /* the tracer's P arg */;
  jobject jPUnref = NULL /* potentially a local ref to jP */;
  int rc;
  switch( traceflag ){
    case SQLITE_TRACE_STMT:
      jX = s3jni_utf8_to_jstring(env, (const char *)pX, -1);
      if( !jX ) return SQLITE_NOMEM;
      /*MARKER(("TRACE_STMT@%p SQL=%p / %s\n", pP, jX, (const char *)pX));*/
      break;
    case SQLITE_TRACE_PROFILE:
      jX = (*env)->NewObject(env, SJG.g.cLong, SJG.g.ctorLong1,
                             (jlong)*((sqlite3_int64*)pX));
      // hmm. ^^^ (*pX) really is zero.
      // MARKER(("profile time = %llu\n", *((sqlite3_int64*)pX)));
      if( !jX ) return SQLITE_NOMEM;
      break;
    case SQLITE_TRACE_ROW:
      break;
    case SQLITE_TRACE_CLOSE:
      jP = ps->jDb;
      break;
    default:
      assert(!"cannot happen - unkown trace flag");
      return SQLITE_ERROR;
  }
  if( !jP ){
    /* Create a new temporary sqlite3_stmt wrapper */
    jP = jPUnref = new_sqlite3_stmt_wrapper(env, pP);
    if( !jP ){
      S3JniUnrefLocal(jX);
      return SQLITE_NOMEM;
    }
  }
  assert(jP);
  rc = (int)(*env)->CallIntMethod(env, ps->hooks.trace.jObj,
                                  ps->hooks.trace.midCallback,
                                  (jint)traceflag, jP, jX);
  S3JniIfThrew{
    rc = s3jni_db_exception(env, ps, SQLITE_ERROR,
                            "sqlite3_trace_v2() callback threw.");
  }
  S3JniUnrefLocal(jPUnref);
  S3JniUnrefLocal(jX);
  return rc;
}

S3JniApi(sqlite3_trace_v2(),jint,1trace_1v2)(
  JniArgsEnvClass,jobject jDb, jint traceMask, jobject jTracer
){
  S3JniDb * const ps = S3JniDb_for_db(env, jDb, 0);
  S3JniHook * const pHook = ps ? &ps->hooks.trace : 0;
  jclass klazz;

  if( !ps ) return SQLITE_MISUSE;
  else if( !traceMask || !jTracer ){
    S3JniHook_unref(env, pHook, 0);
    return (jint)sqlite3_trace_v2(ps->pDb, 0, 0, 0);
  }
  klazz = (*env)->GetObjectClass(env, jTracer);
  pHook->midCallback = (*env)->GetMethodID(
    env, klazz, "call", "(ILjava/lang/Object;Ljava/lang/Object;)I"
  );
  S3JniUnrefLocal(klazz);
  S3JniIfThrew {
    S3JniExceptionClear;
    S3JniHook_unref(env, pHook, 0);
    return s3jni_db_error(ps->pDb, SQLITE_ERROR,
                          "Cannot not find matching xCallback() on Tracer object.");
  }
  pHook->jObj = S3JniRefGlobal(jTracer);
  return sqlite3_trace_v2(ps->pDb, (unsigned)traceMask, s3jni_trace_impl, ps);
}

S3JniApi(sqlite3_update_hook(),jobject,1update_1hook)(
  JniArgsEnvClass, jobject jDb, jobject jHook
){
  return s3jni_updatepre_hook(env, 0, jDb, jHook);
}


S3JniApi(sqlite3_value_blob(),jbyteArray,1value_1blob)(
  JniArgsEnvClass, jobject jpSVal
){
  sqlite3_value * const sv = PtrGet_sqlite3_value(jpSVal);
  int const nLen = sqlite3_value_bytes(sv);
  const jbyte * pBytes = sqlite3_value_blob(sv);
  jbyteArray const jba = pBytes
    ? (*env)->NewByteArray(env, (jsize)nLen)
    : NULL;
  if( jba ){
    (*env)->SetByteArrayRegion(env, jba, 0, nLen, pBytes);
  }
  return jba;
}


S3JniApi(sqlite3_value_double(),jdouble,1value_1double)(
  JniArgsEnvClass, jobject jpSVal
){
  return (jdouble) sqlite3_value_double(PtrGet_sqlite3_value(jpSVal));
}


S3JniApi(sqlite3_value_dup(),jobject,1value_1dup)(
  JniArgsEnvClass, jobject jpSVal
){
  sqlite3_value * const sv = sqlite3_value_dup(PtrGet_sqlite3_value(jpSVal));
  return sv ? new_sqlite3_value_wrapper(env, sv) : 0;
}

S3JniApi(sqlite3_value_free(),void,1value_1free)(
  JniArgsEnvClass, jobject jpSVal
){
  sqlite3_value_free(PtrGet_sqlite3_value(jpSVal));
}

S3JniApi(sqlite3_value_int(),jint,1value_1int)(
  JniArgsEnvClass, jobject jpSVal
){
  return (jint) sqlite3_value_int(PtrGet_sqlite3_value(jpSVal));
}

S3JniApi(sqlite3_value_int64(),jlong,1value_1int64)(
  JniArgsEnvClass, jobject jpSVal
){
  return (jlong) sqlite3_value_int64(PtrGet_sqlite3_value(jpSVal));
}

S3JniApi(sqlite3_value_java_object(),jobject,1value_1java_1object)(
  JniArgsEnvClass, jobject jpSVal
){
  ResultJavaVal * const rv = sqlite3_value_pointer(PtrGet_sqlite3_value(jpSVal),
                                                   ResultJavaValuePtrStr);
  return rv ? rv->jObj : NULL;
}

S3JniApi(sqlite3_value_text_utf8(),jbyteArray,1value_1text_1utf8)(
  JniArgsEnvClass, jobject jpSVal
){
  sqlite3_value * const sv = PtrGet_sqlite3_value(jpSVal);
  int const n = sqlite3_value_bytes(sv);
  const unsigned char * const p = sqlite3_value_text(sv);
  return p ? s3jni_new_jbyteArray(env, p, n) : 0;
}

static jbyteArray value_text16(int mode, JNIEnv * const env, jobject jpSVal){
  sqlite3_value * const sv = PtrGet_sqlite3_value(jpSVal);
  int const nLen = sqlite3_value_bytes16(sv);
  jbyteArray jba;
  const jbyte * pBytes;
  switch( mode ){
    case SQLITE_UTF16:
      pBytes = sqlite3_value_text16(sv);
      break;
    case SQLITE_UTF16LE:
      pBytes = sqlite3_value_text16le(sv);
      break;
    case SQLITE_UTF16BE:
      pBytes = sqlite3_value_text16be(sv);
      break;
    default:
      assert(!"not possible");
      return NULL;
  }
  jba = pBytes
    ? (*env)->NewByteArray(env, (jsize)nLen)
    : NULL;
  if( jba ){
    (*env)->SetByteArrayRegion(env, jba, 0, nLen, pBytes);
  }
  return jba;
}

S3JniApi(sqlite3_value_text16(),jbyteArray,1value_1text16)(
  JniArgsEnvClass, jobject jpSVal
){
  return value_text16(SQLITE_UTF16, env, jpSVal);
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
  SO(S3NphRefs);
  printf("\t(^^^ %u NativePointerHolder subclasses)\n",
         (unsigned)S3Jni_NphCache_size);
  SO(S3JniGlobal);
  SO(S3JniAutoExtension);
  SO(S3JniUdf);
  printf("Cache info:\n");
  printf("\tJNIEnv cache: %u allocs, %u misses, %u hits\n",
         SJG.metrics.envCacheAllocs,
         SJG.metrics.envCacheMisses,
         SJG.metrics.envCacheHits);
  printf("Mutex entry:"
         "\n\tenv %u"
         "\n\tnph inits %u"
         "\n\tperDb %u"
         "\n\tautoExt %u list accesses"
         "\n\tmetrics %u\n",
         SJG.metrics.nMutexEnv, SJG.metrics.nMutexEnv2,
         SJG.metrics.nMutexPerDb, SJG.metrics.nMutexAutoExt,
         SJG.metrics.nMetrics);
  printf("S3JniDb: %u alloced (*%u = %u bytes), %u recycled\n",
         SJG.metrics.nPdbAlloc, (unsigned) sizeof(S3JniDb),
         (unsigned)(SJG.metrics.nPdbAlloc * sizeof(S3JniDb)),
         SJG.metrics.nPdbRecycled);
  puts("Java-side UDF calls:");
#define UDF(T) printf("\t%-8s = %u\n", "x" #T, SJG.metrics.udf.n##T)
  UDF(Func); UDF(Step); UDF(Final); UDF(Value); UDF(Inverse);
#undef UDF
  printf("xDestroy calls across all callback types: %u\n",
         SJG.metrics.nDestroy);
#undef SO
}

////////////////////////////////////////////////////////////////////////
// End of the sqlite3_... API bindings. Next up, FTS5...
////////////////////////////////////////////////////////////////////////
#ifdef SQLITE_ENABLE_FTS5

/* Creates a verbose JNI Fts5 function name. */
#define JniFuncNameFtsXA(Suffix)                  \
  Java_org_sqlite_jni_Fts5ExtensionApi_ ## Suffix
#define JniFuncNameFtsApi(Suffix)                  \
  Java_org_sqlite_jni_fts5_1api_ ## Suffix
#define JniFuncNameFtsTok(Suffix)                  \
  Java_org_sqlite_jni_fts5_tokenizer_ ## Suffix

#define JniDeclFtsXA(ReturnType,Suffix)           \
  JNIEXPORT ReturnType JNICALL                  \
  JniFuncNameFtsXA(Suffix)
#define JniDeclFtsApi(ReturnType,Suffix)          \
  JNIEXPORT ReturnType JNICALL                  \
  JniFuncNameFtsApi(Suffix)
#define JniDeclFtsTok(ReturnType,Suffix)          \
  JNIEXPORT ReturnType JNICALL                  \
  JniFuncNameFtsTok(Suffix)

#define PtrGet_fts5_api(OBJ) NativePointerHolder_get(env,OBJ,&S3NphRefs.fts5_api)
#define PtrGet_fts5_tokenizer(OBJ) NativePointerHolder_get(env,OBJ,&S3NphRefs.fts5_tokenizer)
#define PtrGet_Fts5Context(OBJ) NativePointerHolder_get(env,OBJ,&S3NphRefs.Fts5Context)
#define PtrGet_Fts5Tokenizer(OBJ) NativePointerHolder_get(env,OBJ,&S3NphRefs.Fts5Tokenizer)
#define Fts5ExtDecl Fts5ExtensionApi const * const fext = s3jni_ftsext()

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
  LocalJniGetEnv;
  if( env ){
    /*MARKER(("FTS5 aux function cleanup: %s\n", s->zFuncName));*/
    s3jni_call_xDestroy(env, s->jObj);
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
  Fts5JniAux * s = sqlite3_malloc(sizeof(Fts5JniAux));
  if( s ){
    jclass klazz;
    memset(s, 0, sizeof(Fts5JniAux));
    s->jObj = S3JniRefGlobal(jObj);
    klazz = (*env)->GetObjectClass(env, jObj);
    s->jmid = (*env)->GetMethodID(env, klazz, "xFunction",
                                  "(Lorg/sqlite/jni/Fts5ExtensionApi;"
                                  "Lorg/sqlite/jni/Fts5Context;"
                                  "Lorg/sqlite/jni/sqlite3_context;"
                                  "[Lorg/sqlite/jni/sqlite3_value;)V");
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

static inline Fts5ExtensionApi const * s3jni_ftsext(void){
  return &sFts5Api/*singleton from sqlite3.c*/;
}

static inline jobject new_Fts5Context_wrapper(JNIEnv * const env, Fts5Context *sv){
  return new_NativePointerHolder_object(env, &S3NphRefs.Fts5Context, sv);
}
static inline jobject new_fts5_api_wrapper(JNIEnv * const env, fts5_api *sv){
  return new_NativePointerHolder_object(env, &S3NphRefs.fts5_api, sv);
}

/**
   Returns a per-JNIEnv global ref to the Fts5ExtensionApi singleton
   instance, or NULL on OOM.
*/
static jobject s3jni_getFts5ExensionApi(JNIEnv * const env){
  if( !SJG.fts5.jFtsExt ){
    jobject pNPH = new_NativePointerHolder_object(
      env, &S3NphRefs.Fts5ExtensionApi, s3jni_ftsext()
    );
    S3JniMutex_Env_enter;
    if( pNPH ){
      if( !SJG.fts5.jFtsExt ){
        SJG.fts5.jFtsExt = S3JniRefGlobal(pNPH);
      }
      S3JniUnrefLocal(pNPH);
    }
    S3JniMutex_Env_leave;
  }
  return SJG.fts5.jFtsExt;
}

/*
** Return a pointer to the fts5_api instance for database connection
** db.  If an error occurs, return NULL and leave an error in the
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
  S3JniDb * const ps = S3JniDb_for_db(env, jDb, 0);
  jobject rv = 0;
  if( !ps ) return 0;
  else if( ps->jFtsApi ){
    rv = ps->jFtsApi;
  }else{
    fts5_api * const pApi = s3jni_fts5_api_from_db(ps->pDb);
    if( pApi ){
      rv = new_fts5_api_wrapper(env, pApi);
      ps->jFtsApi = rv ? S3JniRefGlobal(rv) : 0;
    }
  }
  return rv;
}


JniDeclFtsXA(jobject,getInstance)(JniArgsEnvClass){
  return s3jni_getFts5ExensionApi(env);
}

JniDeclFtsXA(jint,xColumnCount)(JniArgsEnvObj,jobject jCtx){
  Fts5ExtDecl;
  return (jint)fext->xColumnCount(PtrGet_Fts5Context(jCtx));
}

JniDeclFtsXA(jint,xColumnSize)(JniArgsEnvObj,jobject jCtx, jint iIdx, jobject jOut32){
  Fts5ExtDecl;
  int n1 = 0;
  int const rc = fext->xColumnSize(PtrGet_Fts5Context(jCtx), (int)iIdx, &n1);
  if( 0==rc ) OutputPointer_set_Int32(env, jOut32, n1);
  return rc;
}

JniDeclFtsXA(jint,xColumnText)(JniArgsEnvObj,jobject jCtx, jint iCol,
                           jobject jOut){
  Fts5ExtDecl;
  const char *pz = 0;
  int pn = 0;
  int rc = fext->xColumnText(PtrGet_Fts5Context(jCtx), (int)iCol,
                             &pz, &pn);
  if( 0==rc ){
    jstring jstr = pz ? s3jni_utf8_to_jstring(env, pz, pn) : 0;
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
  int const rc = fext->xColumnTotalSize(PtrGet_Fts5Context(jCtx), (int)iCol, &nOut);
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
  LocalJniGetEnv;

  assert(pAux);
  jFXA = s3jni_getFts5ExensionApi(env);
  if( !jFXA ) goto error_oom;
  jpFts = new_Fts5Context_wrapper(env, pFts);
  if( !jpFts ) goto error_oom;
  rc = udf_args(env, pCx, argc, argv, &jpCx, &jArgv);
  if( rc ) goto error_oom;
  (*env)->CallVoidMethod(env, pAux->jObj, pAux->jmid,
                         jFXA, jpFts, jpCx, jArgv);
  S3JniIfThrew{
    udf_report_exception(env, 1, pCx, pAux->zFuncName, "xFunction");
  }
  S3JniUnrefLocal(jpFts);
  S3JniUnrefLocal(jpCx);
  S3JniUnrefLocal(jArgv);
  return;
error_oom:
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
  char const * zName;
  Fts5JniAux * pAux;

  assert(pApi);
  zName = s3jni_jstring_to_mutf8(jName);
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
    pAux->zFuncName = sqlite3_mprintf("%s", zName)
      /* OOM here is non-fatal. Ignore it. */;
  }
  s3jni_mutf8_release(jName, zName);
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
      LocalJniGetEnv;
      s3jni_call_xDestroy(env, p->jObj);
      S3JniUnrefGlobal(p->jObj);
    }
    sqlite3_free(x);
  }
}

JniDeclFtsXA(jobject,xGetAuxdata)(JniArgsEnvObj,jobject jCtx, jboolean bClear){
  Fts5ExtDecl;
  jobject rv = 0;
  S3JniFts5AuxData * const pAux = fext->xGetAuxdata(PtrGet_Fts5Context(jCtx), bClear);
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
  int const rc = fext->xInst(PtrGet_Fts5Context(jCtx), (int)iIdx, &n1, &n2, &n3);
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
  int const rc = fext->xInstCount(PtrGet_Fts5Context(jCtx), &nOut);
  if( 0==rc && jOut32 ) OutputPointer_set_Int32(env, jOut32, nOut);
  return (jint)rc;
}

JniDeclFtsXA(jint,xPhraseCount)(JniArgsEnvObj,jobject jCtx){
  Fts5ExtDecl;
  return (jint)fext->xPhraseCount(PtrGet_Fts5Context(jCtx));
}

/* Copy the 'a' and 'b' fields from pSrc to Fts5PhraseIter object jIter. */
static void s3jni_phraseIter_NToJ(JNIEnv *const env,
                                  Fts5PhraseIter const * const pSrc,
                                  jobject jIter){
  S3JniGlobalType * const g = &S3JniGlobal;
  assert(g->fts5.jPhraseIter.fidA);
  (*env)->SetLongField(env, jIter, g->fts5.jPhraseIter.fidA, (jlong)pSrc->a);
  S3JniExceptionIsFatal("Cannot set Fts5PhraseIter.a field.");
  (*env)->SetLongField(env, jIter, g->fts5.jPhraseIter.fidB, (jlong)pSrc->b);
  S3JniExceptionIsFatal("Cannot set Fts5PhraseIter.b field.");
}

/* Copy the 'a' and 'b' fields from Fts5PhraseIter object jIter to pDest. */
static void s3jni_phraseIter_JToN(JNIEnv *const env,  jobject jIter,
                                  Fts5PhraseIter * const pDest){
  S3JniGlobalType * const g = &S3JniGlobal;
  assert(g->fts5.jPhraseIter.fidA);
  pDest->a =
    (const unsigned char *)(*env)->GetLongField(env, jIter, g->fts5.jPhraseIter.fidA);
  S3JniExceptionIsFatal("Cannot get Fts5PhraseIter.a field.");
  pDest->b =
    (const unsigned char *)(*env)->GetLongField(env, jIter, g->fts5.jPhraseIter.fidB);
  S3JniExceptionIsFatal("Cannot get Fts5PhraseIter.b field.");
}

JniDeclFtsXA(jint,xPhraseFirst)(JniArgsEnvObj,jobject jCtx, jint iPhrase,
                            jobject jIter, jobject jOutCol,
                            jobject jOutOff){
  Fts5ExtDecl;
  Fts5PhraseIter iter;
  int rc, iCol = 0, iOff = 0;
  rc = fext->xPhraseFirst(PtrGet_Fts5Context(jCtx), (int)iPhrase,
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
  rc = fext->xPhraseFirstColumn(PtrGet_Fts5Context(jCtx), (int)iPhrase,
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
  fext->xPhraseNext(PtrGet_Fts5Context(jCtx), &iter, &iCol, &iOff);
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
  fext->xPhraseNextColumn(PtrGet_Fts5Context(jCtx), &iter, &iCol);
  OutputPointer_set_Int32(env, jOutCol, iCol);
  s3jni_phraseIter_NToJ(env, &iter, jIter);
}


JniDeclFtsXA(jint,xPhraseSize)(JniArgsEnvObj,jobject jCtx, jint iPhrase){
  Fts5ExtDecl;
  return (jint)fext->xPhraseSize(PtrGet_Fts5Context(jCtx), (int)iPhrase);
}

/* State for use with xQueryPhrase() and xTokenize(). */
struct s3jni_xQueryPhraseState {
  Fts5ExtensionApi const * fext;
  S3JniEnv const * jc;
  jmethodID midCallback;
  jobject jCallback;
  jobject jFcx;
  /* State for xTokenize() */
  struct {
    const char * zPrev;
    int nPrev;
    jbyteArray jba;
  } tok;
};

static int s3jni_xQueryPhrase(const Fts5ExtensionApi *xapi,
                              Fts5Context * pFcx, void *pData){
  /* TODO: confirm that the Fts5Context passed to this function is
     guaranteed to be the same one passed to xQueryPhrase(). If it's
     not, we'll have to create a new wrapper object on every call. */
  struct s3jni_xQueryPhraseState const * s = pData;
  LocalJniGetEnv;
  int rc = (int)(*env)->CallIntMethod(env, s->jCallback, s->midCallback,
                                      SJG.fts5.jFtsExt, s->jFcx);
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
  S3JniEnv * const jc = S3JniEnv_get(env);
  struct s3jni_xQueryPhraseState s;
  jclass klazz = jCallback ? (*env)->GetObjectClass(env, jCallback) : NULL;

  if( !klazz ) return SQLITE_MISUSE;
  s.jc = jc;
  s.jCallback = jCallback;
  s.jFcx = jFcx;
  s.fext = fext;
  s.midCallback = (*env)->GetMethodID(env, klazz, "xCallback",
                                      "(Lorg.sqlite.jni.Fts5ExtensionApi;"
                                      "Lorg.sqlite.jni.Fts5Context;)I");
  S3JniUnrefLocal(klazz);
  S3JniExceptionIsFatal("Could not extract xQueryPhraseCallback.xCallback method.");
  return (jint)fext->xQueryPhrase(PtrGet_Fts5Context(jFcx), iPhrase, &s,
                                  s3jni_xQueryPhrase);
}


JniDeclFtsXA(jint,xRowCount)(JniArgsEnvObj,jobject jCtx, jobject jOut64){
  Fts5ExtDecl;
  sqlite3_int64 nOut = 0;
  int const rc = fext->xRowCount(PtrGet_Fts5Context(jCtx), &nOut);
  if( 0==rc && jOut64 ) OutputPointer_set_Int64(env, jOut64, (jlong)nOut);
  return (jint)rc;
}

JniDeclFtsXA(jlong,xRowid)(JniArgsEnvObj,jobject jCtx){
  Fts5ExtDecl;
  return (jlong)fext->xRowid(PtrGet_Fts5Context(jCtx));
}

JniDeclFtsXA(int,xSetAuxdata)(JniArgsEnvObj,jobject jCtx, jobject jAux){
  Fts5ExtDecl;
  int rc;
  S3JniFts5AuxData * pAux;
  pAux = sqlite3_malloc(sizeof(*pAux));
  if( !pAux ){
    if( jAux ){
      /* Emulate how xSetAuxdata() behaves when it cannot alloc
      ** its auxdata wrapper. */
      s3jni_call_xDestroy(env, jAux);
    }
    return SQLITE_NOMEM;
  }
  pAux->jObj = S3JniRefGlobal(jAux);
  rc = fext->xSetAuxdata(PtrGet_Fts5Context(jCtx), pAux,
                         S3JniFts5AuxData_xDestroy);
  return rc;
}

/* xToken() impl for xTokenize(). */
static int s3jni_xTokenize_xToken(void *p, int tFlags, const char* z,
                                  int nZ, int iStart, int iEnd){
  int rc;
  LocalJniGetEnv;
  struct s3jni_xQueryPhraseState * const s = p;
  jbyteArray jba;
  if( s->tok.zPrev == z && s->tok.nPrev == nZ ){
    jba = s->tok.jba;
  }else{
    if(s->tok.jba){
      S3JniUnrefLocal(s->tok.jba);
    }
    s->tok.zPrev = z;
    s->tok.nPrev = nZ;
    s->tok.jba = (*env)->NewByteArray(env, (jint)nZ);
    if( !s->tok.jba ) return SQLITE_NOMEM;
    jba = s->tok.jba;
    (*env)->SetByteArrayRegion(env, jba, 0, (jint)nZ, (const jbyte*)z);
  }
  rc = (int)(*env)->CallIntMethod(env, s->jCallback, s->midCallback,
                                  (jint)tFlags, jba, (jint)iStart,
                                  (jint)iEnd);
  return rc;
}

/*
** Proxy for Fts5ExtensionApi.xTokenize() and
** fts5_tokenizer.xTokenize()
*/
static jint s3jni_fts5_xTokenize(JniArgsEnvObj, S3NphRef const *pRef,
                                 jint tokFlags, jobject jFcx,
                                 jbyteArray jbaText, jobject jCallback){
  Fts5ExtDecl;
  S3JniEnv * const jc = S3JniEnv_get(env);
  struct s3jni_xQueryPhraseState s;
  int rc = 0;
  jbyte * const pText = jCallback ? s3jni_jbytearray_bytes(jbaText) : 0;
  jsize nText = pText ? (*env)->GetArrayLength(env, jbaText) : 0;
  jclass const klazz = jCallback ? (*env)->GetObjectClass(env, jCallback) : NULL;

  if( !klazz ) return SQLITE_MISUSE;
  memset(&s, 0, sizeof(s));
  s.jc = jc;
  s.jCallback = jCallback;
  s.jFcx = jFcx;
  s.fext = fext;
  s.midCallback = (*env)->GetMethodID(env, klazz, "xToken", "(I[BII)I");
  S3JniUnrefLocal(klazz);
  S3JniIfThrew {
    S3JniExceptionReport;
    S3JniExceptionClear;
    s3jni_jbytearray_release(jbaText, pText);
    return SQLITE_ERROR;
  }
  s.tok.jba = S3JniRefLocal(jbaText);
  s.tok.zPrev = (const char *)pText;
  s.tok.nPrev = (int)nText;
  if( pRef == &S3NphRefs.Fts5ExtensionApi ){
    rc = fext->xTokenize(PtrGet_Fts5Context(jFcx),
                         (const char *)pText, (int)nText,
                         &s, s3jni_xTokenize_xToken);
  }else if( pRef == &S3NphRefs.fts5_tokenizer ){
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
  s3jni_jbytearray_release(jbaText, pText);
  return (jint)rc;
}

JniDeclFtsXA(jint,xTokenize)(JniArgsEnvObj,jobject jFcx, jbyteArray jbaText,
                             jobject jCallback){
  return s3jni_fts5_xTokenize(env, jSelf, &S3NphRefs.Fts5ExtensionApi,
                              0, jFcx, jbaText, jCallback);
}

JniDeclFtsTok(jint,xTokenize)(JniArgsEnvObj,jobject jFcx, jint tokFlags,
                              jbyteArray jbaText, jobject jCallback){
  return s3jni_fts5_xTokenize(env, jSelf, &S3NphRefs.Fts5Tokenizer,
                              tokFlags, jFcx, jbaText, jCallback);
}


JniDeclFtsXA(jobject,xUserData)(JniArgsEnvObj,jobject jFcx){
  Fts5ExtDecl;
  Fts5JniAux * const pAux = fext->xUserData(PtrGet_Fts5Context(jFcx));
  return pAux ? pAux->jUserData : 0;
}

#endif /* SQLITE_ENABLE_FTS5 */

////////////////////////////////////////////////////////////////////////
// End of the main API bindings. Start of SQLTester bits...
////////////////////////////////////////////////////////////////////////

#ifdef S3JNI_ENABLE_SQLTester
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

  ++p->nDup;
  if( n>0 && (pOut = sqlite3_malloc( (n+16)&~7 ))!=0 ){
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
Java_org_sqlite_jni_tester_SQLTester_strglob(
  JniArgsEnvClass, jbyteArray baG, jbyteArray baT
){
  int rc = 0;
  jbyte * const pG = s3jni_jbytearray_bytes(baG);
  jbyte * const pT = pG ? s3jni_jbytearray_bytes(baT) : 0;

  s3jni_oom_check(pT);
  /* Note that we're relying on the byte arrays having been
     NUL-terminated on the Java side. */
  rc = !SQLTester_strnotglob((const char *)pG, (const char *)pT);
  s3jni_jbytearray_release(baG, pG);
  s3jni_jbytearray_release(baT, pT);
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
Java_org_sqlite_jni_tester_SQLTester_installCustomExtensions(JniArgsEnvClass){
  sqlite3_auto_extension( (void(*)(void))SQLTester_auto_extension );
}

#endif /* S3JNI_ENABLE_SQLTester */
////////////////////////////////////////////////////////////////////////
// End of SQLTester bindings. Start of lower-level bits.
////////////////////////////////////////////////////////////////////////

/*
** Called during static init of the SQLite3Jni class to sync certain
** compile-time constants to Java-space.
**
** This routine is part of the reason why we have to #include
** sqlite3.c instead of sqlite3.h.
*/
JNIEXPORT void JNICALL
Java_org_sqlite_jni_SQLite3Jni_init(JniArgsEnvClass){
  enum JType {
    JTYPE_INT,
    JTYPE_BOOL
  };
  typedef struct {
    const char *zName;
    enum JType jtype;
    int value;
  } ConfigFlagEntry;
  const ConfigFlagEntry aLimits[] = {
    {"SQLITE_MAX_ALLOCATION_SIZE", JTYPE_INT, SQLITE_MAX_ALLOCATION_SIZE},
    {"SQLITE_LIMIT_LENGTH", JTYPE_INT, SQLITE_LIMIT_LENGTH},
    {"SQLITE_MAX_LENGTH", JTYPE_INT, SQLITE_MAX_LENGTH},
    {"SQLITE_LIMIT_SQL_LENGTH", JTYPE_INT, SQLITE_LIMIT_SQL_LENGTH},
    {"SQLITE_MAX_SQL_LENGTH", JTYPE_INT, SQLITE_MAX_SQL_LENGTH},
    {"SQLITE_LIMIT_COLUMN", JTYPE_INT, SQLITE_LIMIT_COLUMN},
    {"SQLITE_MAX_COLUMN", JTYPE_INT, SQLITE_MAX_COLUMN},
    {"SQLITE_LIMIT_EXPR_DEPTH", JTYPE_INT, SQLITE_LIMIT_EXPR_DEPTH},
    {"SQLITE_MAX_EXPR_DEPTH", JTYPE_INT, SQLITE_MAX_EXPR_DEPTH},
    {"SQLITE_LIMIT_COMPOUND_SELECT", JTYPE_INT, SQLITE_LIMIT_COMPOUND_SELECT},
    {"SQLITE_MAX_COMPOUND_SELECT", JTYPE_INT, SQLITE_MAX_COMPOUND_SELECT},
    {"SQLITE_LIMIT_VDBE_OP", JTYPE_INT, SQLITE_LIMIT_VDBE_OP},
    {"SQLITE_MAX_VDBE_OP", JTYPE_INT, SQLITE_MAX_VDBE_OP},
    {"SQLITE_LIMIT_FUNCTION_ARG", JTYPE_INT, SQLITE_LIMIT_FUNCTION_ARG},
    {"SQLITE_MAX_FUNCTION_ARG", JTYPE_INT, SQLITE_MAX_FUNCTION_ARG},
    {"SQLITE_LIMIT_ATTACHED", JTYPE_INT, SQLITE_LIMIT_ATTACHED},
    {"SQLITE_MAX_ATTACHED", JTYPE_INT, SQLITE_MAX_ATTACHED},
    {"SQLITE_LIMIT_LIKE_PATTERN_LENGTH", JTYPE_INT, SQLITE_LIMIT_LIKE_PATTERN_LENGTH},
    {"SQLITE_MAX_LIKE_PATTERN_LENGTH", JTYPE_INT, SQLITE_MAX_LIKE_PATTERN_LENGTH},
    {"SQLITE_LIMIT_VARIABLE_NUMBER", JTYPE_INT, SQLITE_LIMIT_VARIABLE_NUMBER},
    {"SQLITE_MAX_VARIABLE_NUMBER", JTYPE_INT, SQLITE_MAX_VARIABLE_NUMBER},
    {"SQLITE_LIMIT_TRIGGER_DEPTH", JTYPE_INT, SQLITE_LIMIT_TRIGGER_DEPTH},
    {"SQLITE_MAX_TRIGGER_DEPTH", JTYPE_INT, SQLITE_MAX_TRIGGER_DEPTH},
    {"SQLITE_LIMIT_WORKER_THREADS", JTYPE_INT, SQLITE_LIMIT_WORKER_THREADS},
    {"SQLITE_MAX_WORKER_THREADS", JTYPE_INT, SQLITE_MAX_WORKER_THREADS},
    {0,0}
  };
  jfieldID fieldId;
  jclass klazz;
  const ConfigFlagEntry * pConfFlag;

  if( 0==sqlite3_threadsafe() ){
    (*env)->FatalError(env, "sqlite3 was not built with SQLITE_THREADSAFE.");
    return;
  }
  memset(&S3JniGlobal, 0, sizeof(S3JniGlobal));
  if( (*env)->GetJavaVM(env, &SJG.jvm) ){
    (*env)->FatalError(env, "GetJavaVM() failure shouldn't be possible.");
    return;
  }

  /* Grab references to various global classes and objects... */
  SJG.g.cObj = S3JniRefGlobal((*env)->FindClass(env,"java/lang/Object"));
  S3JniExceptionIsFatal("Error getting reference to Object class.");

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

  { /* StandardCharsets.UTF_8 */
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
  klazz = (*env)->FindClass(env, "org/sqlite/jni/Fts5PhraseIter");
  S3JniExceptionIsFatal("Error getting reference to org.sqlite.jni.Fts5PhraseIter.");
  SJG.fts5.jPhraseIter.fidA = (*env)->GetFieldID(env, klazz, "a", "J");
  S3JniExceptionIsFatal("Cannot get Fts5PhraseIter.a field.");
  SJG.fts5.jPhraseIter.fidB = (*env)->GetFieldID(env, klazz, "b", "J");
  S3JniExceptionIsFatal("Cannot get Fts5PhraseIter.b field.");
  S3JniUnrefLocal(klazz);
#endif

  SJG.envCache.mutex = sqlite3_mutex_alloc(SQLITE_MUTEX_FAST);
  s3jni_oom_check( SJG.envCache.mutex );
  SJG.perDb.mutex = sqlite3_mutex_alloc(SQLITE_MUTEX_FAST);
  s3jni_oom_check( SJG.perDb.mutex );
  SJG.autoExt.mutex = sqlite3_mutex_alloc(SQLITE_MUTEX_FAST);
  s3jni_oom_check( SJG.autoExt.mutex );

#if S3JNI_METRICS_MUTEX
  SJG.metrics.mutex = sqlite3_mutex_alloc(SQLITE_MUTEX_FAST);
  s3jni_oom_check( SJG.metrics.mutex );
#endif

  sqlite3_shutdown()
    /* So that it becomes legal for Java-level code to call
    ** sqlite3_config(), if it's ever implemented. */;

  /* Set up static "consts" of the SQLite3Jni class. */
  for( pConfFlag = &aLimits[0]; pConfFlag->zName; ++pConfFlag ){
    char const * zSig = (JTYPE_BOOL == pConfFlag->jtype) ? "Z" : "I";
    fieldId = (*env)->GetStaticFieldID(env, jKlazz, pConfFlag->zName, zSig);
    S3JniExceptionIsFatal("Missing an expected static member of the SQLite3Jni class.");
    assert(fieldId);
    switch( pConfFlag->jtype ){
      case JTYPE_INT:
        (*env)->SetStaticIntField(env, jKlazz, fieldId, (jint)pConfFlag->value);
        break;
      case JTYPE_BOOL:
        (*env)->SetStaticBooleanField(env, jKlazz, fieldId,
                                      pConfFlag->value ? JNI_TRUE : JNI_FALSE);
        break;
    }
    S3JniExceptionIsFatal("Seting a static member of the SQLite3Jni class failed.");
  }
}

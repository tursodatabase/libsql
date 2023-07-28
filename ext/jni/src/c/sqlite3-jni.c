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
#ifndef SQLITE_DEFAULT_UNIX_VFS
# define SQLITE_DEFAULT_UNIX_VFS "unix"
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
# define SQLITE_THREADSAFE 0
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

#include "sqlite3-jni.h"
#include <stdio.h> /* only for testing/debugging */
#include <assert.h>

/* Only for debugging */
#define MARKER(pfexp)                                               \
  do{ printf("MARKER: %s:%d:%s():\t",__FILE__,__LINE__,__func__);   \
    printf pfexp;                                                   \
  } while(0)

/* Creates a verbose JNI function name. */
#define JFuncName(Suffix) \
  Java_org_sqlite_jni_SQLite3Jni_sqlite3_ ## Suffix

/* Prologue for JNI functions. */
#define JDECL(ReturnType,Suffix)                \
  JNIEXPORT ReturnType JNICALL                  \
  JFuncName(Suffix)
/* First 2 parameters to all JNI bindings. */
#define JENV_JSELF JNIEnv * env, jobject jSelf
/* Helper to squelch -Xcheck:jni warnings about
   not having checked for exceptions. */
#define IFTHREW if((*env)->ExceptionCheck(env))
#define EXCEPTION_IGNORE (void)((*env)->ExceptionCheck(env))
#define EXCEPTION_CLEAR (*env)->ExceptionClear(env)
#define EXCEPTION_REPORT (*env)->ExceptionDescribe(env)
#define IFTHREW_REPORT IFTHREW EXCEPTION_REPORT
#define IFTHREW_CLEAR IFTHREW EXCEPTION_CLEAR


#define PtrGet_sqlite3(OBJ) getNativePointer(env,OBJ,ClassNames.sqlite3)
#define PtrGet_sqlite3_stmt(OBJ) getNativePointer(env,OBJ,ClassNames.sqlite3_stmt)
#define PtrGet_sqlite3_value(OBJ) getNativePointer(env,OBJ,ClassNames.sqlite3_value)
#define PtrGet_sqlite3_context(OBJ) getNativePointer(env,OBJ,ClassNames.sqlite3_context)
#define REF_G(VAR) (*env)->NewGlobalRef(env, VAR)
/*#define REF_L(VAR) (*env)->NewLocalRef(env, VAR)*/
#define UNREF_G(VAR) if(VAR) (*env)->DeleteGlobalRef(env, (VAR))
#define UNREF_L(VAR) if(VAR) (*env)->DeleteLocalRef(env, (VAR))

/**
   Constant string class names used as keys for S3Global_nph_cache() and
   friends.
*/
static const struct {
  const char * const sqlite3;
  const char * const sqlite3_stmt;
  const char * const sqlite3_context;
  const char * const sqlite3_value;
  const char * const OutputPointer_Int32;
} ClassNames = {
  "org/sqlite/jni/sqlite3",
  "org/sqlite/jni/sqlite3_stmt",
  "org/sqlite/jni/sqlite3_context",
  "org/sqlite/jni/sqlite3_value",
  "org/sqlite/jni/OutputPointer$Int32"
};

/** Create a trivial JNI wrapper for (int CName(void)). */
#define WRAP_INT_VOID(JniNameSuffix,CName) \
  JDECL(jint,JniNameSuffix)(JNIEnv *env, jobject jSelf){ \
    return (jint)CName(); \
  }

#define WRAP_INT_INT(JniNameSuffix,CName)               \
  JDECL(jint,JniNameSuffix)(JNIEnv *env, jobject jSelf, jint arg){   \
    return (jint)CName((int)arg);                                    \
  }

/** Create a trivial JNI wrapper for (const mutf8_string *
    CName(void)). This is only value for functions which are known to
    return ASCII or text compatible with Modified UTF8. */
#define WRAP_MUTF8_VOID(JniNameSuffix,CName)                             \
  JDECL(jstring,JniNameSuffix)(JENV_JSELF){                  \
    return (*env)->NewStringUTF( env, CName() );                        \
  }
/** Create a trivial JNI wrapper for (int CName(sqlite3_stmt*)). */
#define WRAP_INT_STMT(JniNameSuffix,CName) \
  JDECL(jint,JniNameSuffix)(JENV_JSELF, jobject jpStmt){   \
    jint const rc = (jint)CName(PtrGet_sqlite3_stmt(jpStmt)); \
    EXCEPTION_IGNORE /* squelch -Xcheck:jni */;        \
    return rc; \
  }
/** Create a trivial JNI wrapper for (int CName(sqlite3*)). */
#define WRAP_INT_DB(JniNameSuffix,CName) \
  JDECL(jint,JniNameSuffix)(JENV_JSELF, jobject pDb){   \
    return (jint)CName(PtrGet_sqlite3(pDb)); \
  }
/** Create a trivial JNI wrapper for (int64 CName(sqlite3*)). */
#define WRAP_INT64_DB(JniNameSuffix,CName) \
  JDECL(jlong,JniNameSuffix)(JENV_JSELF, jobject pDb){   \
    return (jlong)CName(PtrGet_sqlite3(pDb)); \
  }
/** Create a trivial JNI wrapper for (int CName(sqlite3_stmt*,int)). */
#define WRAP_INT_STMT_INT(JniNameSuffix,CName) \
  JDECL(jint,JniNameSuffix)(JENV_JSELF, jobject pStmt, jint n){ \
    return (jint)CName(PtrGet_sqlite3_stmt(pStmt), (int)n);                          \
  }
/** Create a trivial JNI wrapper for (jstring CName(sqlite3_stmt*,int)). */
#define WRAP_STR_STMT_INT(JniNameSuffix,CName) \
  JDECL(jstring,JniNameSuffix)(JENV_JSELF, jobject pStmt, jint ndx){ \
    return (*env)->NewStringUTF(env, CName(PtrGet_sqlite3_stmt(pStmt), (int)ndx));  \
  }
/** Create a trivial JNI wrapper for (int CName(sqlite3_value*)). */
#define WRAP_INT_SVALUE(JniNameSuffix,CName) \
  JDECL(jint,JniNameSuffix)(JENV_JSELF, jobject jpSValue){   \
    return (jint)CName(PtrGet_sqlite3_value(jpSValue)); \
  }

/* Helpers for jstring and jbyteArray. */
#define JSTR_TOC(ARG) (*env)->GetStringUTFChars(env, ARG, NULL)
#define JSTR_RELEASE(ARG,VAR) if(VAR) (*env)->ReleaseStringUTFChars(env, ARG, VAR)
#define JBA_TOC(ARG) (*env)->GetByteArrayElements(env,ARG, NULL)
#define JBA_RELEASE(ARG,VAR) (*env)->ReleaseByteArrayElements(env, ARG, VAR, JNI_ABORT)

/* Marker for code which needs(?) to be made thread-safe. */
#define FIXME_THREADING

enum {
  /**
     Size of the per-JNIEnv cache. We have no way of knowing how many
     distinct JNIEnv's will be used in any given run, but know that it
     will normally be only 1. Perhaps (just speculating) differen
     threads use separate JNIEnvs? If that's the case, we don't(?)
     have enough info to evict from the cache when those JNIEnvs
     expire.

     If this ever fills up, we can refactor this to dynamically
     allocate them.
  */
  JNIEnvCache_SIZE = 10,
  /**
    Need enough space for (only) the library's NativePointerHolder
    types, a fixed count known at build-time. If we add more than this
    a fatal error will be triggered with a reminder to increase this.
  */
  NphCache_SIZE = 10
};

/**
   Cache for NativePointerHolder lookups.
*/
typedef struct NphCacheLine NphCacheLine;
struct NphCacheLine {
  const char * zClassName /* "full/class/Name" */;
  jclass klazz        /* global ref to concrete NPH class */;
  jmethodID midSet    /* setNativePointer() */;
  jmethodID midGet    /* getNativePointer() */;
  jmethodID midCtor   /* constructor */;
  jmethodID midSetAgg /* sqlite3_context::setAggregateContext() */;
};

typedef struct JNIEnvCacheLine JNIEnvCacheLine;
struct JNIEnvCacheLine {
  JNIEnv *env;
  jclass globalClassObj  /* global ref to java.lang.Object */;
  jclass globalClassLong /* global ref to java.lang.Long */;
  jmethodID ctorLong1    /* the Long(long) constructor */;
  struct NphCacheLine nph[NphCache_SIZE];
};
typedef struct JNIEnvCache JNIEnvCache;
struct JNIEnvCache {
  struct JNIEnvCacheLine lines[JNIEnvCache_SIZE];
  unsigned int used;
};

static void NphCacheLine_clear(JNIEnv *env, NphCacheLine * p){
  UNREF_G(p->klazz);
  memset(p, 0, sizeof(NphCacheLine));
}

static void JNIEnvCacheLine_clear(JNIEnvCacheLine * p){
  JNIEnv *env = p->env;
  int i;
  if(env){
    UNREF_G(p->globalClassObj);
    UNREF_G(p->globalClassLong);
    i = 0;
    for( ; i < NphCache_SIZE; ++i){
      NphCacheLine_clear(env, &p->nph[i]);
    }
  }
}

static void JNIEnvCache_clear(JNIEnvCache * p){
  unsigned int i = 0;
  for( ; i < p->used; ++i ){
    JNIEnvCacheLine_clear( &p->lines[i] );
  }
  memset(p, 0, sizeof(JNIEnvCache));
}

/**
   State for binding Java-side busy handlers.
*/
typedef struct {
  JNIEnv * env;         /* env registered from */;
  jobject jObj          /* BusyHandlerJni instance */;
  jclass klazz          /* jObj's class */;
  jmethodID jmidxCallback;
} BusyHandlerJni;


/**
   Per-(sqlite3*) state for bindings which do not have their own
   finalizer functions, e.g. tracing and commit/rollback hooks.  This
   state is allocated as needed, cleaned up in sqlite3_close(_v2)(),
   and recycled when possible. It is freed during sqlite3_shutdown().

   Open questions:

   - Do we need to do a (JNIEnv*) for the db and each set of binding
   data (since they can(?) hypothetically be set via multiple JNIEnv
   objects)?
*/
typedef struct PerDbStateJni PerDbStateJni;
struct PerDbStateJni {
  JNIEnv *env;
  sqlite3 * pDb;
  PerDbStateJni * pNext;
  PerDbStateJni * pPrev;
  struct {
    jobject jObj;
    jmethodID midCallback;
  } trace;
  struct {
    jobject jObj;
    jmethodID midCallback;
  } progress;
  struct {
    jobject jObj;
    jmethodID midCallback;
  } commitHook;
  struct {
    jobject jObj;
    jmethodID midCallback;
  } rollbackHook;
  BusyHandlerJni busyHandler;
};

static struct {
  /**
     According to: https://developer.ibm.com/articles/j-jni/

     > A thread can get a JNIEnv by calling GetEnv() using the JNI
       invocation interface through a JavaVM object. The JavaVM object
       itself can be obtained by calling the JNI GetJavaVM() method
       using a JNIEnv object and can be cached and shared across
       threads. Caching a copy of the JavaVM object enables any thread
       with access to the cached object to get access to its own
       JNIEnv when necessary.
  */
  JavaVM * jvm;
  struct JNIEnvCache envCache;
  struct {
    PerDbStateJni * aUsed;
    PerDbStateJni * aFree;
  } perDb;
} S3Global;

/**
   sqlite3_malloc() proxy which fails fatally on OOM.  This should
   only be used for routines which manage global state and have no
   recovery strategy for OOM. For sqlite3 API which can reasonably
   return SQLITE_NOMEM, sqlite3_malloc() should be used instead.
*/
static void * s3jni_malloc(JNIEnv *env, size_t n){
  void * rv = sqlite3_malloc(n);
  if(n && !rv){
    (*env)->FatalError(env, "Out of memory.") /* does not return */;
  }
  return rv;
}

static void s3jni_free(void * p){
  if(p) sqlite3_free(p);
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
static int s3jni_db_error(sqlite3*db, int err_code, const char *zMsg){
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

/**
   Clears s's state, releasing any Java references. Before doing so,
   it calls s's xDestroy() method, ignoring the lack of that method or
   any exceptions it throws. This is a no-op of s has no current
   state.
*/
static void BusyHandlerJni_clear(BusyHandlerJni * const s){
  if(s->jObj){
    JNIEnv * const env = s->env;
    const jmethodID method =
      (*env)->GetMethodID(env, s->klazz, "xDestroy", "()V");
    if(method){
      (*env)->CallVoidMethod(env, s->jObj, method);
      IFTHREW_CLEAR;
    }else{
      EXCEPTION_CLEAR;
    }
    UNREF_G(s->jObj);
    UNREF_G(s->klazz);
    memset(s, 0, sizeof(BusyHandlerJni));
  }
}

/**
   Initializes s to wrap BusyHandlerJni-type object jObject, clearning
   any current state of s beforehand. Returns 0 on success, non-0 on
   error. On error, s's state is cleared.
*/
static int BusyHandlerJni_init(JNIEnv * const env, BusyHandlerJni * const s,
                               jobject jObj){
  const char * zSig = "(I)I" /* callback signature */;
  if(s->jObj) BusyHandlerJni_clear(s);
  s->env = env;
  s->jObj = REF_G(jObj);
  s->klazz = REF_G((*env)->GetObjectClass(env, jObj));
  s->jmidxCallback = (*env)->GetMethodID(env, s->klazz, "xCallback", zSig);
  IFTHREW {
    BusyHandlerJni_clear(s);
    return SQLITE_ERROR;
  }
  return 0;
}

/**
   Extracts the new PerDbStateJni instance from the free-list, or
   allocates one if needed, associats it with pDb, and returns.
   Returns NULL on OOM.
*/
static PerDbStateJni * PerDbStateJni_alloc(JNIEnv *env, sqlite3 *pDb){
  PerDbStateJni * rv;
  assert( pDb );
  if(S3Global.perDb.aFree){
    rv = S3Global.perDb.aFree;
    S3Global.perDb.aFree = rv->pNext;
    if(rv->pNext){
      assert(rv->pNext->pPrev == rv);
      assert(rv->pNext == rv->pNext->pPrev);
      rv->pNext->pPrev = 0;
      rv->pNext = 0;
    }
  }else{
    rv = s3jni_malloc(env, sizeof(PerDbStateJni));
    if(rv){
      memset(rv, 0, sizeof(PerDbStateJni));
    }
  }
  if(rv){
    rv->pNext = S3Global.perDb.aUsed;
    S3Global.perDb.aUsed = rv;
    if(rv->pNext){
      assert(!rv->pNext->pPrev);
      rv->pNext->pPrev = rv;
    }
    rv->pDb = pDb;
    rv->env = env;
  }
  return rv;
}

/**
   Clears s's state and moves it to the free-list.
*/
FIXME_THREADING
static void PerDbStateJni_set_aside(PerDbStateJni *s){
  if(s){
    JNIEnv * const env = s->env;
    assert(s->pDb && "Else this object is already in the free-list.");
    if(s->pNext) s->pNext->pPrev = s->pPrev;
    if(s->pPrev) s->pPrev->pNext = s->pNext;
    else if(S3Global.perDb.aUsed == s){
      assert(!s->pPrev);
      S3Global.perDb.aUsed = s->pNext;
    }
    UNREF_G(s->trace.jObj);
    UNREF_G(s->progress.jObj);
    UNREF_G(s->commitHook.jObj);
    UNREF_G(s->rollbackHook.jObj);
    BusyHandlerJni_clear(&s->busyHandler);
    memset(s, 0, sizeof(PerDbStateJni));
    s->pNext = S3Global.perDb.aFree;
    S3Global.perDb.aFree = s;
  }
}

static void PerDbStateJni_dump(PerDbStateJni *s){
  MARKER(("PerDbStateJni->env @ %p\n", s->env));
  MARKER(("PerDbStateJni->pDb @ %p\n", s->pDb));
  MARKER(("PerDbStateJni->trace.jObj @ %p\n", s->trace.jObj));
  MARKER(("PerDbStateJni->progress.jObj @ %p\n", s->progress.jObj));
  MARKER(("PerDbStateJni->commitHook.jObj @ %p\n", s->commitHook.jObj));
  MARKER(("PerDbStateJni->rollbackHook.jObj @ %p\n", s->rollbackHook.jObj));
  MARKER(("PerDbStateJni->busyHandler.env @ %p\n", s->busyHandler.env));
  MARKER(("PerDbStateJni->busyHandler.jObj @ %p\n", s->busyHandler.jObj));
  MARKER(("PerDbStateJni->env @ %p\n", s->env));
}

/**
   Returns the PerDbStateJni object for the given db. If allocIfNeeded is
   true then a new instance will be allocated if no mapping currently
   exists, else NULL is returned if no mapping is found.

*/
FIXME_THREADING
static PerDbStateJni * PerDbStateJni_for_db(JNIEnv *env, sqlite3 *pDb, int allocIfNeeded){
  PerDbStateJni * s = S3Global.perDb.aUsed;
  for( ; s; s = s->pNext){
    if(s->pDb == pDb) return s;
  }
  if(allocIfNeeded) s = PerDbStateJni_alloc(env, pDb);
  return s;
}

/**
   Cleans up and frees all state in S3Global.perDb.
*/
FIXME_THREADING
static void PerDbStateJni_free_all(void){
  PerDbStateJni * pS = S3Global.perDb.aUsed;
  PerDbStateJni * pSNext = 0;
  for( ; pS; pS = pSNext ){
    pSNext = pS->pNext;
    PerDbStateJni_set_aside(pS);
    assert(pSNext ? !pSNext->pPrev : 1);
  }
  assert( 0==S3Global.perDb.aUsed );
  pS = S3Global.perDb.aFree;
  S3Global.perDb.aFree = 0;
  pSNext = 0;
  for( ; pS; pS = pSNext ){
    pSNext = pS->pNext;
    s3jni_free(pSNext);
  }
}

/**
   Fetches the S3Global.envCache row for the given env, allocing
   a row if needed. When a row is allocated, its state is initialized
   insofar as possible. Calls (*env)->FatalError() if the cache
   fills up. That's hypothetically possible but "shouldn't happen."
   If it does, we can dynamically allocate these instead.

*/
FIXME_THREADING
static struct JNIEnvCacheLine * S3Global_env_cache(JNIEnv * env){
  struct JNIEnvCacheLine * row = 0;
  int i = 0;
  for( ; i < JNIEnvCache_SIZE; ++i ){
    row = &S3Global.envCache.lines[i];
    if(row->env == env) return row;
    else if(!row->env) break;
  }
  if(i == JNIEnvCache_SIZE){
    (*env)->FatalError(env, "Maintenance required: JNIEnvCache is full.");
    return NULL;
  }
  row->env = env;
  row->globalClassObj = REF_G((*env)->FindClass(env,"java/lang/Object"));
  row->globalClassLong = REF_G((*env)->FindClass(env,"java/lang/Long"));
  row->ctorLong1 = (*env)->GetMethodID(env, row->globalClassLong,
                                       "<init>", "(J)V");
  ++S3Global.envCache.used;
  //MARKER(("Added S3Global.envCache entry #%d.\n", S3Global.envCache.used));
  return row;
}

/**
   Searches the NativePointerHolder cache for the given combination.
   If it finds one, it returns it as-is. If it doesn't AND the cache
   has a free slot, it populates that slot with (env, zClassName,
   klazz) and returns it. If the cache is full with no match it
   returns NULL.

   It is up to the caller to populate the other members of the returned
   object if needed.

   zClassName must be a static string so we can use its address as a
   cache key.

   This simple cache catches the overwhelming majority of searches
   (>95%) in the current (2023-07-24) tests.
*/
FIXME_THREADING
static struct NphCacheLine * S3Global_nph_cache(JNIEnv *env, const char *zClassName){
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
  struct JNIEnvCacheLine * const envRow = S3Global_env_cache(env);
  struct NphCacheLine * freeSlot = 0;
  struct NphCacheLine * cacheLine = 0;
  int i;
  assert(envRow);
  for( i = 0; i < NphCache_SIZE; ++i ){
    cacheLine = &envRow->nph[i];
    if(zClassName == cacheLine->zClassName){
#if 0
      static unsigned int n = 0;
      MARKER(("Cache hit #%u %s klazz@%p getter@%p, setter@%p, ctor@%p\n",
              ++n, zClassName, cacheLine->klazz, cacheLine->midGet,
              cacheLine->midSet, cacheLine->midCtor));
#endif
      assert(cacheLine->klazz);
      return cacheLine;
    }else if(!freeSlot && !cacheLine->zClassName){
      freeSlot = cacheLine;
    }
  }
  if(freeSlot){
    freeSlot->zClassName = zClassName;
    freeSlot->klazz = REF_G((*env)->FindClass(env, zClassName));
#if 0
    static unsigned int cacheMisses = 0;
    MARKER(("Cache miss #%u %s klazz@%p getter@%p, setter@%p, ctor@%p\n",
            ++cacheMisses, zClassName, freeSlot->klazz,
            freeSlot->midGet, freeSlot->midSet, freeSlot->midCtor));
#endif
  }else{
    (*env)->FatalError(env, "MAINTENANCE REQUIRED: NphCache_SIZE is too low.");
  }
  return freeSlot;
}

/**
   Sets a native ptr value in NativePointerHolder object ppOut.
   zClassName must be a static string so we can use its address
   as a cache key.
*/
static void setNativePointer(JNIEnv * env, jobject ppOut, void * p,
                             const char *zClassName){
  jmethodID setter = 0;
  struct NphCacheLine * const cacheLine = S3Global_nph_cache(env, zClassName);
  if(cacheLine && cacheLine->klazz && cacheLine->midSet){
    assert(zClassName == cacheLine->zClassName);
    setter = cacheLine->midSet;
    assert(setter);
  }else{
    jclass const klazz =
      cacheLine ? cacheLine->klazz : (*env)->GetObjectClass(env, ppOut);
    setter = (*env)->GetMethodID(env, klazz, "setNativePointer", "(J)V");
    if(cacheLine){
      assert(cacheLine->klazz);
      assert(!cacheLine->midSet);
      assert(zClassName == cacheLine->zClassName);
      cacheLine->midSet = setter;
    }
  }
  (*env)->CallVoidMethod(env, ppOut, setter, (jlong)p);
  IFTHREW_REPORT;
}

/**
   Fetches a native ptr value from NativePointerHolder object ppOut.
   zClassName must be a static string so we can use its address as a
   cache key.
*/
static void * getNativePointer(JNIEnv * env, jobject pObj, const char *zClassName){
  if( 0==pObj ) return 0;
  else{
    jmethodID getter = 0;
    void * rv = 0;
    struct NphCacheLine * const cacheLine = S3Global_nph_cache(env, zClassName);
    if(cacheLine && cacheLine->midGet){
      getter = cacheLine->midGet;
    }else{
      jclass const klazz =
        cacheLine ? cacheLine->klazz : (*env)->GetObjectClass(env, pObj);
      getter = (*env)->GetMethodID(env, klazz, "getNativePointer", "()J");
      if(cacheLine){
        assert(cacheLine->klazz);
        assert(zClassName == cacheLine->zClassName);
        cacheLine->midGet = getter;
      }
    }
    rv = (void*)(*env)->CallLongMethod(env, pObj, getter);
    IFTHREW_REPORT;
    return rv;
  }
}

/**
   Requires that jCx be a Java-side sqlite3_context wrapper for pCx.
   This function calls sqlite3_aggregate_context() to allocate a tiny
   sliver of memory, the address of which is set in
   jCx->setAggregateContext().  The memory is only used as a key for
   mapping, client-side, results of aggregate result sets across
   xStep() and xFinal() methods.

   isFinal must be 1 for xFinal() calls and 0 for all others.

   Returns 0 on success. Returns SQLITE_NOMEM on allocation error,
   noting that it will not allocate when isFinal is true. It returns
   SQLITE_ERROR if there's a serious internal error in dealing with
   the JNI state.
*/
static int udf_setAggregateContext(JNIEnv * env, jobject jCx,
                                   sqlite3_context * pCx,
                                   int isFinal){
  jmethodID setter;
  void * pAgg;
  int rc = 0;
  struct NphCacheLine * const cacheLine =
    S3Global_nph_cache(env, ClassNames.sqlite3_context);
  if(cacheLine && cacheLine->klazz && cacheLine->midSetAgg){
    setter = cacheLine->midSetAgg;
    assert(setter);
  }else{
    jclass const klazz =
      cacheLine ? cacheLine->klazz : (*env)->GetObjectClass(env, jCx);
    setter = (*env)->GetMethodID(env, klazz, "setAggregateContext", "(J)V");
    if(cacheLine){
      assert(cacheLine->klazz);
      assert(!cacheLine->midSetAgg);
      cacheLine->midSetAgg = setter;
    }
  }
  pAgg = sqlite3_aggregate_context(pCx, isFinal ? 0 : 4);
  if( pAgg || isFinal ){
    (*env)->CallVoidMethod(env, jCx, setter, (jlong)pAgg);
    IFTHREW {
      EXCEPTION_REPORT;
      EXCEPTION_CLEAR/*arguable, but so is propagation*/;
      rc = s3jni_db_error(sqlite3_context_db_handle(pCx),
                          SQLITE_ERROR,
                          "sqlite3_context::setAggregateContext() "
                          "unexpectedly threw.");
    }
  }else{
    assert(!pAgg);
    rc = SQLITE_NOMEM;
  }
  return rc;
}

/* Sets a native int32 value in OutputPointer.Int32 object ppOut. */
static void setOutputInt32(JNIEnv * env, jobject ppOut, int v){
  jmethodID setter = 0;
  struct NphCacheLine * const cacheLine =
    S3Global_nph_cache(env, ClassNames.OutputPointer_Int32);
  if(cacheLine && cacheLine->klazz && cacheLine->midSet){
    setter = cacheLine->midSet;
  }else{
    const jclass klazz = (*env)->GetObjectClass(env, ppOut);
    setter = (*env)->GetMethodID(env, klazz, "setValue", "(I)V");
    if(cacheLine){
      assert(!cacheLine->midSet);
      cacheLine->midSet = setter;
    }
  }
  (*env)->CallVoidMethod(env, ppOut, setter, (jint)v);
  IFTHREW_REPORT;
}

#if 0
/* Fetches a native int32 value from OutputPointer.Int32 object pObj. */
static int getOutputInt(JNIEnv * env, jobject pObj){
  const jclass klazz = (*env)->GetObjectClass(env, pObj);
  const jmethodID getter =
    (*env)->GetMethodID(env, klazz, "getValue", "(V)I;");
  return (int)(*env)->CallIntMethod(env, pObj, getter);
}
#define VAL_GET_INT(OBJ) getOutputInt(env, OBJ)
#endif

static int encodingTypeIsValid(int eTextRep){
  switch(eTextRep){
    case SQLITE_UTF8: case SQLITE_UTF16:
    case SQLITE_UTF16LE: case SQLITE_UTF16BE:
      return 1;
    default:
      return 0;
  }
}

/**
   State for binding Java-side collation sequences.
*/
typedef struct {
  jclass klazz         /* Collation object's class */;
  jobject oCollation   /* Collation instance */;
  jmethodID midCompare /* cached xCompare */;
  JNIEnv * env;        /* env registered from */;
} CollationState;

static CollationState * CollationState_alloc(void){
  CollationState * rc = sqlite3_malloc(sizeof(CollationState));
  if(rc) memset(rc, 0, sizeof(CollationState));
  return rc;
}

static void CollationState_free(CollationState * cs){
  JNIEnv * const env = cs->env;
  if(env){
    //MARKER(("Collation cleanup...\n"));
    if(cs->oCollation) UNREF_G(cs->oCollation);
    if(cs->klazz) UNREF_G(cs->klazz);
  }
  sqlite3_free(cs);
}

static int collation_xCompare_proxy(void *pArg, int nLhs, const void *lhs,
                                    int nRhs, const void *rhs){
  CollationState * const cs = (CollationState*)pArg;
  JNIEnv * env = cs->env;
  jint rc;
  jbyteArray jbaLhs = (*env)->NewByteArray(env, (jint)nLhs);
  jbyteArray jbaRhs = (*env)->NewByteArray(env, (jint)nRhs);
  //MARKER(("native xCompare nLhs=%d nRhs=%d\n", nLhs, nRhs));
  (*env)->SetByteArrayRegion(env, jbaLhs, 0, (jint)nLhs, (const jbyte*)lhs);
  (*env)->SetByteArrayRegion(env, jbaRhs, 0, (jint)nRhs, (const jbyte*)rhs);
  rc = (*env)->CallIntMethod(env, cs->oCollation, cs->midCompare,
                             jbaLhs, jbaRhs);
  EXCEPTION_IGNORE;
  UNREF_L(jbaLhs);
  UNREF_L(jbaRhs);
  return (int)rc;
}

static void collation_xDestroy_proxy(void *pArg){
  CollationState * const cs = (CollationState*)pArg;
  if(cs->oCollation){
    JNIEnv * const env = cs->env;
    const jmethodID method = (*env)->GetMethodID(env, cs->klazz, "xDestroy",
                                                 "()V");
    //MARKER(("Calling Collation.xDestroy()...\n"));
    (*env)->CallVoidMethod(env, cs->oCollation, method);
    IFTHREW {
      MARKER(("Collation.xDestroy() threw. Ignoring!\n"));
      EXCEPTION_REPORT;
      EXCEPTION_CLEAR;
    }
    //MARKER(("Returned from Collation.xDestroy().\n"));
  }
  CollationState_free(cs);
}

/* State for sqlite3_result_java_object() and
   sqlite3_value_java_object(). */
typedef struct {
  /* POTENTIAL bug: the JNI docs say that the JNIEnv pointer
     is guaranteed to resolve the same for the same contexts,
     but the docs are unclear as to whether it's the (JNIEnv *env)
     or (*env) which resolves consistently.

     This posts claims it's unsave to cache JNIEnv at all, even when
     it's always used in the same thread:

     https://stackoverflow.com/questions/12420463

     And this one seems to contradict that:

     https://stackoverflow.com/questions/13964608

     For later reference:

     https://docs.oracle.com/javase/1.5.0/docs/guide/jni/spec/design.html#wp1242

     https://developer.android.com/training/articles/perf-jni

     The later has the following say about caching:

     > If performance is important, it's useful to look the
       [class/method ID] values up once and cache the results in your
       native code. Because there is a limit of one JavaVM per
       process, it's reasonable to store this data in a static local
       structure. ... The class references, field IDs, and method IDs
       are guaranteed valid until the class is unloaded. Classes are
       only unloaded if all classes associated with a ClassLoader can
       be garbage collected, which is rare but will not be impossible
       in Android. Note however that the jclass is a class reference
       and must be protected with a call to NewGlobalRef (see the next
       section).
  */
  JNIEnv * env;
  jobject jObj;
} ResultJavaVal;

/* For use with sqlite3_result/value_pointer() */
#define RESULT_JAVA_VAL_STRING "ResultJavaVal"

static ResultJavaVal * ResultJavaVal_alloc(JNIEnv *env, jobject jObj){
  ResultJavaVal * rv = sqlite3_malloc(sizeof(ResultJavaVal));
  if(rv){
    rv->env = env;
    rv->jObj = jObj ? REF_G(jObj) : 0;
  }
  return rv;
}

static void ResultJavaVal_finalizer(void *v){
  if(v){
    ResultJavaVal * const rv = (ResultJavaVal*)v;
    if(rv->jObj) (*(rv->env))->DeleteGlobalRef(rv->env, rv->jObj);
    sqlite3_free(rv);
  }
}



/**
   Returns a new Java instance of the class named by zClassName, which
   MUST be interface-compatible with NativePointerHolder and MUST have
   a no-arg constructor. Its setNativePointer() method is passed
   pNative. Hypothetically returns NULL if Java fails to allocate, but
   the JNI docs are not entirely clear on that detail.

   Always use a string literal for the 2nd argument so that we can use
   its address as a cache key.
*/
static jobject new_NativePointerHolder_object(JNIEnv *env, const char *zClassName,
                                              void * pNative){
  jobject rv = 0;
  jclass klazz = 0;
  jmethodID ctor = 0;
  struct NphCacheLine * const cacheLine =
    S3Global_nph_cache(env, zClassName);
  if(cacheLine && cacheLine->midCtor){
    assert( cacheLine->klazz );
    klazz = cacheLine->klazz;
    ctor = cacheLine->midCtor;
  }else{
    klazz = cacheLine
      ? cacheLine->klazz
      : (*env)->FindClass(env, zClassName);
    ctor = (*env)->GetMethodID(env, klazz, "<init>", "()V");
    if(cacheLine){
      assert(zClassName == cacheLine->zClassName);
      assert(cacheLine->klazz);
      assert(!cacheLine->midCtor);
      cacheLine->midCtor = ctor;
    }
  }
  assert(klazz);
  assert(ctor);
  rv = (*env)->NewObject(env, klazz, ctor);
  if(rv) setNativePointer(env, rv, pNative, zClassName);
  return rv;
}

static jobject new_sqlite3_value_wrapper(JNIEnv *env, sqlite3_value *sv){
  return new_NativePointerHolder_object(env, "org/sqlite/jni/sqlite3_value", sv);
}

static jobject new_sqlite3_context_wrapper(JNIEnv *env, sqlite3_context *sv){
  return new_NativePointerHolder_object(env, "org/sqlite/jni/sqlite3_context", sv);
}

static jobject new_sqlite3_wrapper(JNIEnv *env, sqlite3 *sv){
  return new_NativePointerHolder_object(env, "org/sqlite/jni/sqlite3", sv);
}

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
typedef struct {
  JNIEnv * env;         /* env registered from */;
  jobject jObj          /* SQLFunction instance */;
  jclass klazz          /* jObj's class */;
  char * zFuncName      /* Only for error reporting and debug logging */;
  enum UDFType type;
  /** Method IDs for the various UDF methods. */
  jmethodID jmidxFunc;
  jmethodID jmidxStep;
  jmethodID jmidxFinal;
  jmethodID jmidxValue;
  jmethodID jmidxInverse;
} UDFState;

static UDFState * UDFState_alloc(JNIEnv *env, jobject jObj){
  UDFState * const s = sqlite3_malloc(sizeof(UDFState));
  if(s){
    const char * zFSI = /* signature for xFunc, xStep, xInverse */
      "(Lorg/sqlite/jni/sqlite3_context;[Lorg/sqlite/jni/sqlite3_value;)V";
    const char * zFV = /* signature for xFinal, xValue */
      "(Lorg/sqlite/jni/sqlite3_context;)V";
    memset(s, 0, sizeof(UDFState));
    s->env = env;
    s->jObj = REF_G(jObj);
    s->klazz = REF_G((*env)->GetObjectClass(env, jObj));
#define FGET(FuncName,FuncType,Field) \
    s->Field = (*env)->GetMethodID(env, s->klazz, FuncName, FuncType); \
    if(!s->Field) (*env)->ExceptionClear(env)
    FGET("xFunc",    zFSI, jmidxFunc);
    FGET("xStep",    zFSI, jmidxStep);
    FGET("xFinal",   zFV,  jmidxFinal);
    FGET("xValue",   zFV,  jmidxValue);
    FGET("xInverse", zFSI, jmidxInverse);
#undef FGET
    if(s->jmidxFunc) s->type = UDF_SCALAR;
    else if(s->jmidxStep && s->jmidxFinal){
      s->type = s->jmidxValue ? UDF_WINDOW : UDF_AGGREGATE;
    }else{
      s->type = UDF_UNKNOWN_TYPE;
    }
  }
  return s;
}

static void UDFState_free(UDFState * s){
  JNIEnv * const env = s->env;
  if(env){
    //MARKER(("Collation cleanup...\n"));
    if(s->jObj){
      const jmethodID method =
        (*env)->GetMethodID(env, s->klazz, "xDestroy", "()V");
      if(method){
        //MARKER(("aCalling SQLFunction.xDestroy()...\n"));
        (*env)->CallVoidMethod(env, s->jObj, method);
        EXCEPTION_IGNORE;
        //MARKER(("Returned from SQLFunction.xDestroy().\n"));
      }else{
        (*env)->ExceptionClear(env);
      }
    }
    UNREF_G(s->jObj);
    UNREF_G(s->klazz);
  }
  sqlite3_free(s->zFuncName);
  sqlite3_free(s);
}

static void UDFState_finalizer(void * s){
  if(s) UDFState_free((UDFState*)s);
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
static int udf_args(sqlite3_context * const cx,
                    int argc, sqlite3_value**argv,
                    UDFState * const s,
                    udf_jargs * const args){
  jobjectArray ja = 0;
  JNIEnv * const env = s->env;
  jobject jcx = new_sqlite3_context_wrapper(s->env, cx);
  jint i;
  args->jcx = 0;
  args->jargv = 0;
  if(!jcx) goto error_oom;
  ja = (*(s->env))->NewObjectArray(s->env, argc,
                                   S3Global_env_cache(env)->globalClassObj,
                                   NULL);
  if(!ja) goto error_oom;
  for(i = 0; i < argc; ++i){
    jobject jsv = new_sqlite3_value_wrapper(s->env, argv[i]);
    if(!jsv) goto error_oom;
    (*env)->SetObjectArrayElement(env, ja, i, jsv);
    UNREF_L(jsv)/*array has a ref*/;
  }
  args->jcx = jcx;
  args->jargv = ja;
  return 0;
error_oom:
  sqlite3_result_error_nomem(cx);
  UNREF_L(jcx);
  UNREF_L(ja);
  return 1;
}

static int udf_report_exception(sqlite3_context * cx, UDFState *s,
                                const char *zFuncType){
  int rc;
  char * z =
    sqlite3_mprintf("UDF %s.%s() threw. FIXME: extract "
                    "Java-side exception message.",
                    s->zFuncName, zFuncType);
  if(z){
    sqlite3_result_error(cx, z, -1);
    sqlite3_free(z);
    rc = SQLITE_ERROR;
  }else{
    rc = SQLITE_NOMEM;
  }
  return rc;
}

static int udf_xFSI(sqlite3_context* pCx, int argc,
                    sqlite3_value** argv,
                    UDFState * s,
                    jmethodID xMethodID,
                    const char * zFuncType){
  udf_jargs args;
  JNIEnv * const env = s->env;
  int rc = udf_args(pCx, argc, argv, s, &args);
  //MARKER(("%s.%s() pCx = %p\n", s->zFuncName, zFuncType, pCx));
  if(rc) return rc;
  //MARKER(("UDF::%s.%s()\n", s->zFuncName, zFuncType));
  if( UDF_SCALAR != s->type ){
    rc = udf_setAggregateContext(env, args.jcx, pCx, 0);
  }
  if( 0 == rc ){
    (*env)->CallVoidMethod(env, s->jObj, xMethodID, args.jcx, args.jargv);
    IFTHREW{
      rc = udf_report_exception(pCx,s, zFuncType);
    }
  }
  UNREF_L(args.jcx);
  UNREF_L(args.jargv);
  return rc;
}

static int udf_xFV(sqlite3_context* cx, UDFState * s,
                   jmethodID xMethodID,
                   const char *zFuncType){
  JNIEnv * const env = s->env;
  jobject jcx = new_sqlite3_context_wrapper(s->env, cx);
  int rc = 0;
  //MARKER(("%s.%s() cx = %p\n", s->zFuncName, zFuncType, cx));
  if(!jcx){
    sqlite3_result_error_nomem(cx);
    return SQLITE_NOMEM;
  }
  //MARKER(("UDF::%s.%s()\n", s->zFuncName, zFuncType));
  if( UDF_SCALAR != s->type ){
    rc = udf_setAggregateContext(env, jcx, cx, 1);
  }
  if( 0 == rc ){
    (*env)->CallVoidMethod(env, s->jObj, xMethodID, jcx);
    IFTHREW{
      rc = udf_report_exception(cx,s, zFuncType);
    }
  }
  UNREF_L(jcx);
  return rc;
}

static void udf_xFunc(sqlite3_context* cx, int argc,
                      sqlite3_value** argv){
  UDFState * const s = (UDFState*)sqlite3_user_data(cx);
  udf_xFSI(cx, argc, argv, s, s->jmidxFunc, "xFunc");
}
static void udf_xStep(sqlite3_context* cx, int argc,
                      sqlite3_value** argv){
  UDFState * const s = (UDFState*)sqlite3_user_data(cx);
  udf_xFSI(cx, argc, argv, s, s->jmidxStep, "xStep");
}
static void udf_xFinal(sqlite3_context* cx){
  UDFState * const s = (UDFState*)sqlite3_user_data(cx);
  udf_xFV(cx, s, s->jmidxFinal, "xFinal");
}
static void udf_xValue(sqlite3_context* cx){
  UDFState * const s = (UDFState*)sqlite3_user_data(cx);
  udf_xFV(cx, s, s->jmidxValue, "xValue");
}
static void udf_xInverse(sqlite3_context* cx, int argc,
                         sqlite3_value** argv){
  UDFState * const s = (UDFState*)sqlite3_user_data(cx);
  udf_xFSI(cx, argc, argv, s, s->jmidxInverse, "xInverse");
}


////////////////////////////////////////////////////////////////////////
// What follows is the JNI/C bindings. They are in alphabetical order
// except for this macro-generated subset which are kept together here
// at the front...
////////////////////////////////////////////////////////////////////////
WRAP_INT_DB(1errcode,                  sqlite3_errcode)
WRAP_INT_DB(1error_1offset,            sqlite3_error_offset)
WRAP_INT_DB(1extended_1errcode,        sqlite3_extended_errcode)
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
WRAP_MUTF8_VOID(1libversion,           sqlite3_libversion)
WRAP_INT_VOID(1libversion_1number,     sqlite3_libversion_number)
WRAP_INT_STMT(1reset,                  sqlite3_reset)
WRAP_INT_INT(1sleep,                   sqlite3_sleep)
WRAP_MUTF8_VOID(1sourceid,             sqlite3_sourceid)
WRAP_INT_STMT(1step,                   sqlite3_step)
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

JDECL(jint,1bind_1blob)(JENV_JSELF, jobject jpStmt,
                        jint ndx, jbyteArray baData, jint nMax){
  int rc;
  if(!baData){
    rc = sqlite3_bind_null(PtrGet_sqlite3_stmt(jpStmt), ndx);
  }else{
    jbyte * const pBuf = JBA_TOC(baData);
    rc = sqlite3_bind_blob(PtrGet_sqlite3_stmt(jpStmt), (int)ndx, pBuf, (int)nMax,
                           SQLITE_TRANSIENT);
    JBA_RELEASE(baData,pBuf);
  }
  return (jint)rc;
}

JDECL(jint,1bind_1double)(JENV_JSELF, jobject jpStmt,
                         jint ndx, jdouble val){
  return (jint)sqlite3_bind_double(PtrGet_sqlite3_stmt(jpStmt), (int)ndx, (double)val);
}

JDECL(jint,1bind_1int)(JENV_JSELF, jobject jpStmt,
                      jint ndx, jint val){
  return (jint)sqlite3_bind_int(PtrGet_sqlite3_stmt(jpStmt), (int)ndx, (int)val);
}

JDECL(jint,1bind_1int64)(JENV_JSELF, jobject jpStmt,
                        jint ndx, jlong val){
  return (jint)sqlite3_bind_int64(PtrGet_sqlite3_stmt(jpStmt), (int)ndx, (sqlite3_int64)val);
}

JDECL(jint,1bind_1null)(JENV_JSELF, jobject jpStmt,
                       jint ndx){
  return (jint)sqlite3_bind_null(PtrGet_sqlite3_stmt(jpStmt), (int)ndx);
}

JDECL(jint,1bind_1parameter_1index)(JENV_JSELF, jobject jpStmt, jbyteArray jName){
  int rc = 0;
  jbyte * const pBuf = JBA_TOC(jName);
  if(pBuf){
    rc = sqlite3_bind_parameter_index(PtrGet_sqlite3_stmt(jpStmt),
                                      (const char *)pBuf);
    JBA_RELEASE(jName, pBuf);
  }
  return rc;
}

JDECL(jint,1bind_1text)(JENV_JSELF, jobject jpStmt,
                       jint ndx, jbyteArray baData, jint nMax){
  if(baData){
    jbyte * const pBuf = JBA_TOC(baData);
    int rc = sqlite3_bind_text(PtrGet_sqlite3_stmt(jpStmt), (int)ndx, (const char *)pBuf,
                               (int)nMax, SQLITE_TRANSIENT);
    JBA_RELEASE(baData, pBuf);
    return (jint)rc;
  }else{
    return sqlite3_bind_null(PtrGet_sqlite3_stmt(jpStmt), (int)ndx);
  }
}

JDECL(jint,1bind_1zeroblob)(JENV_JSELF, jobject jpStmt,
                           jint ndx, jint n){
  return (jint)sqlite3_bind_zeroblob(PtrGet_sqlite3_stmt(jpStmt), (int)ndx, (int)n);
}

JDECL(jint,1bind_1zeroblob64)(JENV_JSELF, jobject jpStmt,
                           jint ndx, jlong n){
  return (jint)sqlite3_bind_zeroblob(PtrGet_sqlite3_stmt(jpStmt), (int)ndx, (sqlite3_uint64)n);
}

static int s3jni_busy_handler(void* pState, int n){
  PerDbStateJni * const pS = (PerDbStateJni *)pState;
  int rc = 0;
  if( pS->busyHandler.jObj ){
    JNIEnv * const env = pS->env;
    rc = (*env)->CallIntMethod(env, pS->busyHandler.jObj,
                               pS->busyHandler.jmidxCallback, (jint)n);
    IFTHREW_CLEAR;
  }
  return rc;
}

JDECL(jint,1busy_1handler)(JENV_JSELF, jobject jDb, jobject jBusy){
  sqlite3 * const pDb = PtrGet_sqlite3(jDb);
  PerDbStateJni * const pS = PerDbStateJni_for_db(env, pDb, 1);
  int rc;
  if(!pS) return (jint)SQLITE_NOMEM;
  if(jBusy){
    if(pS->busyHandler.jObj &&
       (*env)->IsSameObject(env, pS->busyHandler.jObj, jBusy)){
      /* Same object - this is a no-op. */
      return 0;
    }
    rc = BusyHandlerJni_init(env, &pS->busyHandler, jBusy);
    if(rc){
      assert(!pS->busyHandler.jObj);
      return (jint)rc;
    }
    assert(pS->busyHandler.jObj && pS->busyHandler.klazz);
    assert( (*env)->IsSameObject(env, pS->busyHandler.jObj, jBusy) );
  }else{
    BusyHandlerJni_clear(&pS->busyHandler);
  }
  return jBusy
    ? sqlite3_busy_handler(pDb, s3jni_busy_handler, pS)
    : sqlite3_busy_handler(pDb, 0, 0);
}

JDECL(jint,1busy_1timeout)(JENV_JSELF, jobject jDb, jint ms){
  sqlite3* const pDb = PtrGet_sqlite3(jDb);
  PerDbStateJni * const pS = PerDbStateJni_for_db(env, pDb, 0);
  if( pS && pS->busyHandler.jObj ){
    BusyHandlerJni_clear(&pS->busyHandler);
  }
  return sqlite3_busy_timeout(pDb, (int)ms);
}

/**
   Wrapper for sqlite3_close(_v2)().
*/
static jint s3jni_close_db(JNIEnv *env, jobject jDb, int version){
  sqlite3 * pDb;
  int rc = 0;
  PerDbStateJni * pS = 0;
  assert(version == 1 || version == 2);
  if(0){
    PerDbStateJni * s = S3Global.perDb.aUsed;
    for( ; s; s = s->pNext){
      PerDbStateJni_dump(s);
    }
  }
  pDb = PtrGet_sqlite3(jDb);
  if(!pDb) return rc;
  pS = PerDbStateJni_for_db(env, pDb, 0);
  rc = 1==version ? (jint)sqlite3_close(pDb) : (jint)sqlite3_close_v2(pDb);
  if(pS) PerDbStateJni_set_aside(pS)
           /* MUST come after close() because of pS->trace. */;
  setNativePointer(env, jDb, 0, ClassNames.sqlite3);
  return (jint)rc;
}

JDECL(jint,1close_1v2)(JENV_JSELF, jobject pDb){
  return s3jni_close_db(env, pDb, 2);
}

JDECL(jint,1close)(JENV_JSELF, jobject pDb){
  return s3jni_close_db(env, pDb, 1);
}

JDECL(jbyteArray,1column_1blob)(JENV_JSELF, jobject jpStmt,
                                jint ndx){
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

JDECL(jdouble,1column_1double)(JENV_JSELF, jobject jpStmt,
                               jint ndx){
  return (jdouble)sqlite3_column_double(PtrGet_sqlite3_stmt(jpStmt), (int)ndx);
}

JDECL(jint,1column_1int)(JENV_JSELF, jobject jpStmt,
                            jint ndx){
  return (jint)sqlite3_column_int(PtrGet_sqlite3_stmt(jpStmt), (int)ndx);
}

JDECL(jlong,1column_1int64)(JENV_JSELF, jobject jpStmt,
                            jint ndx){
  return (jlong)sqlite3_column_int64(PtrGet_sqlite3_stmt(jpStmt), (int)ndx);
}

/**
   Expects to be passed a pointer from sqlite3_column_text16() or
   sqlite3_value_text16() and a length value from
   sqlite3_column_bytes16() or sqlite3_value_bytes16(). It creates a
   Java String of exactly half that length, returning NULL if !p or
   (*env)->NewString() fails.
*/
static jstring s3jni_text_to_jstring(JNIEnv *env, const void * const p, int nP){
  return p
    ? (*env)->NewString(env, (const jchar *)p, (jsize)(nP/2))
    : NULL;
}

/**
   Creates a new jByteArray of length nP, copies p's contents into it, and
   returns that byte array.
 */
static jbyteArray s3jni_new_jbyteArray(JNIEnv *env, const unsigned char * const p, int nP){
  jbyteArray jba = (*env)->NewByteArray(env, (jint)nP);
  if(jba){
    (*env)->SetByteArrayRegion(env, jba, 0, (jint)nP, (const jbyte*)p);
  }
  return jba;
}

JDECL(jstring,1column_1text)(JENV_JSELF, jobject jpStmt,
                             jint ndx){
  sqlite3_stmt * const stmt = PtrGet_sqlite3_stmt(jpStmt);
  const int n = sqlite3_column_bytes16(stmt, (int)ndx);
  const void * const p = sqlite3_column_text16(stmt, (int)ndx);
  return s3jni_text_to_jstring(env, p, n);
}

JDECL(jbyteArray,1column_1text_1utf8)(JENV_JSELF, jobject jpStmt,
                                      jint ndx){
  sqlite3_stmt * const stmt = PtrGet_sqlite3_stmt(jpStmt);
  const int n = sqlite3_column_bytes(stmt, (int)ndx);
  const unsigned char * const p = sqlite3_column_text(stmt, (int)ndx);
  return s3jni_new_jbyteArray(env, p, n);
}

JDECL(jobject,1column_1value)(JENV_JSELF, jobject jpStmt,
                              jint ndx){
  sqlite3_value * const sv = sqlite3_column_value(PtrGet_sqlite3_stmt(jpStmt), (int)ndx);
  return new_sqlite3_value_wrapper(env, sv);
}

JDECL(jstring,1compileoption_1get)(JENV_JSELF, jint n){
  return (*env)->NewStringUTF( env, sqlite3_compileoption_get(n) );
}

JDECL(jboolean,1compileoption_1used)(JENV_JSELF, jstring name){
  const char *zUtf8 = (*env)->GetStringUTFChars(env, name, NULL);
  const jboolean rc =
    0==sqlite3_compileoption_used(zUtf8) ? JNI_FALSE : JNI_TRUE;
  (*env)->ReleaseStringUTFChars(env, name, zUtf8);
  return rc;
}

JDECL(jobject,1context_1db_1handle)(JENV_JSELF, jobject jpCx){
  sqlite3 * const db = sqlite3_context_db_handle(PtrGet_sqlite3_context(jpCx));
  return db ? new_sqlite3_wrapper(env, db) : NULL;
}

JDECL(jint,1create_1collation)(JENV_JSELF, jobject jpDb,
                               jstring name, jint eTextRep,
                               jobject oCollation){
  const jclass klazz = (*env)->GetObjectClass(env, oCollation);
  int rc;
  const char *zName;
  CollationState * const cs = CollationState_alloc();
  if(!cs) return (jint)SQLITE_NOMEM;
  cs->env = env;
  cs->oCollation = REF_G(oCollation);
  cs->klazz = REF_G(klazz);
  cs->midCompare = (*env)->GetMethodID(env, klazz, "xCompare",
                                    "([B[B)I");
  zName = JSTR_TOC(name);
  rc = sqlite3_create_collation_v2(PtrGet_sqlite3(jpDb), zName, (int)eTextRep,
                                   cs, collation_xCompare_proxy,
                                   collation_xDestroy_proxy);
  JSTR_RELEASE(name, zName);
  if(0 != rc) collation_xDestroy_proxy(cs);
  return (jint)rc;
}

static jint create_function(JNIEnv * env, jobject jDb, jstring jFuncName,
                            jint nArg, jint eTextRep, jobject jFunctor){
  UDFState * s = 0;
  int rc;
  sqlite3 * const pDb = PtrGet_sqlite3(jDb);
  const char * zFuncName = 0;

  if( !encodingTypeIsValid(eTextRep) ){
    return s3jni_db_error(pDb, SQLITE_FORMAT,
                                "Invalid function encoding option.");
  }
  s = UDFState_alloc(env, jFunctor);
  if( !s ) return SQLITE_NOMEM;
  else if( UDF_UNKNOWN_TYPE==s->type ){
    UDFState_free(s);
    rc = s3jni_db_error(pDb, SQLITE_MISUSE,
                              "Cannot unambiguously determine function type.");
    goto error_cleanup;
  }
  zFuncName = JSTR_TOC(jFuncName);
  if(!zFuncName){
    UDFState_free(s);
    rc = SQLITE_NOMEM;
    goto error_cleanup;
  }
  if( UDF_WINDOW == s->type ){
    rc = sqlite3_create_window_function(pDb, zFuncName, nArg, eTextRep, s,
                                        udf_xStep, udf_xFinal, udf_xValue,
                                        udf_xInverse, UDFState_finalizer);
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
                                    xFunc, xStep, xFinal,
                                    UDFState_finalizer);
  }
  s->zFuncName = sqlite3_mprintf("%s", zFuncName);
  if(!s->zFuncName){
    rc = SQLITE_NOMEM;
    UDFState_free(s);
  }
error_cleanup:
  JSTR_RELEASE(jFuncName, zFuncName);
  /* on create_function() error, s will be destroyed via create_function() */
  return (jint)rc;
}

JDECL(jint,1create_1function)(JENV_JSELF, jobject jDb, jstring jFuncName,
                              jint nArg, jint eTextRep, jobject jFunctor){
  return create_function(env, jDb, jFuncName, nArg, eTextRep, jFunctor);
}

/*
JDECL(jint,1create_1window_1function)(JENV_JSELF, jstring jFuncName, jint nArg,
                                      jint eTextRep, jobject jFunctor){
  return create_function_mega(env, jFuncName, nArg, eTextRep, jFunctor);
}
*/

JDECL(jstring,1errmsg)(JENV_JSELF, jobject jpDb){
  return (*env)->NewStringUTF(env, sqlite3_errmsg(PtrGet_sqlite3(jpDb)));
}

JDECL(jstring,1errstr)(JENV_JSELF, jint rcCode){
  return (*env)->NewStringUTF(env, sqlite3_errstr((int)rcCode));
}

JDECL(jboolean,1extended_1result_1codes)(JENV_JSELF, jobject jpDb,
                                         jboolean onoff){
  int const rc = sqlite3_extended_result_codes(PtrGet_sqlite3(jpDb), onoff ? 1 : 0);
  return rc ? JNI_TRUE : JNI_FALSE;
}

JDECL(jint,1initialize)(JENV_JSELF){
  return sqlite3_initialize();
}

JDECL(jint,1finalize)(JENV_JSELF, jobject jpStmt){
  if( jpStmt ){
    sqlite3_stmt * pStmt = PtrGet_sqlite3_stmt(jpStmt);
    setNativePointer(env, jpStmt, 0, ClassNames.sqlite3_stmt);
    if( pStmt ) sqlite3_finalize(pStmt);
  }
  return 0;
}


JDECL(jlong,1last_1insert_1rowid)(JENV_JSELF, jobject jpDb){
  return (jlong)sqlite3_last_insert_rowid(PtrGet_sqlite3(jpDb));
}


JDECL(jint,1open)(JENV_JSELF, jstring strName, jobject ppOut){
  sqlite3 * pOut = 0;
  const char *zName = strName ? JSTR_TOC(strName) : 0;
  int nrc = sqlite3_open(zName, &pOut);
  //MARKER(("env=%p, *env=%p\n", env, *env));
  setNativePointer(env, ppOut, pOut, ClassNames.sqlite3);
  assert(nrc==0 ? pOut!=0 : 1);
  JSTR_RELEASE(strName, zName);
  return (jint)nrc;
}

JDECL(jint,1open_1v2)(JENV_JSELF, jstring strName,
                      jobject ppOut, jint flags, jstring strVfs){
  sqlite3 * pOut = 0;
  const char *zName = strName ? JSTR_TOC(strName) : 0;
  const char *zVfs = strVfs ? JSTR_TOC(strVfs) : 0;
  int nrc = sqlite3_open_v2(zName, &pOut, (int)flags, zVfs);
  /*MARKER(("zName=%s, zVfs=%s, pOut=%p, flags=%d, nrc=%d\n",
    zName, zVfs, pOut, (int)flags, nrc));*/
  setNativePointer(env, ppOut, pOut, ClassNames.sqlite3);
  assert(nrc==0 ? pOut!=0 : 1);
  JSTR_RELEASE(strName, zName);
  JSTR_RELEASE(strVfs, zVfs);
  return (jint)nrc;
}

/* Proxy for the sqlite3_prepare[_v2/3]() family. */
static jint sqlite3_jni_prepare_v123(int prepVersion, JNIEnv *env, jclass self,
                                     jobject jpDb, jbyteArray baSql,
                                     jint nMax, jint prepFlags,
                                     jobject outStmt, jobject outTail){
  sqlite3_stmt * pStmt = 0;
  const char * zTail = 0;
  jbyte * const pBuf = JBA_TOC(baSql);
  int rc = SQLITE_ERROR;
  assert(prepVersion==1 || prepVersion==2 || prepVersion==3);
  switch( prepVersion ){
    case 1: rc = sqlite3_prepare(PtrGet_sqlite3(jpDb), (const char *)pBuf,
                                 (int)nMax, &pStmt, &zTail);
      break;
    case 2: rc = sqlite3_prepare_v2(PtrGet_sqlite3(jpDb), (const char *)pBuf,
                                    (int)nMax, &pStmt, &zTail);
      break;
    case 3: rc = sqlite3_prepare_v3(PtrGet_sqlite3(jpDb), (const char *)pBuf,
                                    (int)nMax, (unsigned int)prepFlags,
                                    &pStmt, &zTail);
      break;
    default:
      assert(0 && "Invalid prepare() version");
  }
  JBA_RELEASE(baSql,pBuf);
  if( 0!=outTail ){
    assert(zTail ? ((void*)zTail>=(void*)pBuf) : 1);
    assert(zTail ? (((int)((void*)zTail - (void*)pBuf)) >= 0) : 1);
    setOutputInt32(env, outTail, (int)(zTail ? (zTail - (const char *)pBuf) : 0));
  }
  setNativePointer(env, outStmt, pStmt, ClassNames.sqlite3_stmt);
  return (jint)rc;
}
JDECL(jint,1prepare)(JNIEnv *env, jclass self, jobject jpDb, jbyteArray baSql,
                     jint nMax, jobject outStmt, jobject outTail){
  return sqlite3_jni_prepare_v123(1, env, self, jpDb, baSql, nMax, 0,
                                  outStmt, outTail);
}
JDECL(jint,1prepare_1v2)(JNIEnv *env, jclass self, jobject jpDb, jbyteArray baSql,
                         jint nMax, jobject outStmt, jobject outTail){
  return sqlite3_jni_prepare_v123(2, env, self, jpDb, baSql, nMax, 0,
                                  outStmt, outTail);
}
JDECL(jint,1prepare_1v3)(JNIEnv *env, jclass self, jobject jpDb, jbyteArray baSql,
                         jint nMax, jint prepFlags, jobject outStmt, jobject outTail){
  return sqlite3_jni_prepare_v123(3, env, self, jpDb, baSql, nMax,
                                  prepFlags, outStmt, outTail);
}

/* sqlite3_result_text/blob() and friends. */
static void result_blob_text(int asBlob, int as64,
                             int eTextRep/*only for (asBlob=0)*/,
                             JNIEnv *env, sqlite3_context *pCx,
                             jbyteArray jBa, jlong nMax){
  if(jBa){
    jbyte * const pBuf = JBA_TOC(jBa);
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
    if(as64){ /* 64-bit... */
      static const jsize nLimit64 =
        SQLITE_MAX_ALLOCATION_SIZE/*only _kinda_ arbitrary!*/
        /* jsize is int32, not int64! */;
      if(nBa > nLimit64){
        sqlite3_result_error_toobig(pCx);
      }else if(asBlob){
        sqlite3_result_blob64(pCx, pBuf, (sqlite3_uint64)nBa,
                              SQLITE_TRANSIENT);
      }else{ /* text64... */
        if(encodingTypeIsValid(eTextRep)){
          sqlite3_result_text64(pCx, (const char *)pBuf,
                                (sqlite3_uint64)nBa,
                                SQLITE_TRANSIENT, eTextRep);
        }else{
          sqlite3_result_error_code(pCx, SQLITE_FORMAT);
        }
      }
    }else{ /* 32-bit... */
      static const jsize nLimit = SQLITE_MAX_ALLOCATION_SIZE;
      if(nBa > nLimit){
        sqlite3_result_error_toobig(pCx);
      }else if(asBlob){
        sqlite3_result_blob(pCx, pBuf, (int)nBa,
                            SQLITE_TRANSIENT);
      }else{
        switch(eTextRep){
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
      JBA_RELEASE(jBa, pBuf);
    }
  }else{
    sqlite3_result_null(pCx);
  }
}

JDECL(void,1result_1blob)(JENV_JSELF, jobject jpCx, jbyteArray jBa, jint nMax){
  return result_blob_text(1, 0, 0, env, PtrGet_sqlite3_context(jpCx), jBa, nMax);
}

JDECL(void,1result_1blob64)(JENV_JSELF, jobject jpCx, jbyteArray jBa, jlong nMax){
  return result_blob_text(1, 1, 0, env, PtrGet_sqlite3_context(jpCx), jBa, nMax);
}

JDECL(void,1result_1double)(JENV_JSELF, jobject jpCx, jdouble v){
  sqlite3_result_double(PtrGet_sqlite3_context(jpCx), v);
}

JDECL(void,1result_1error)(JENV_JSELF, jobject jpCx, jbyteArray baMsg,
                           int eTextRep){
  const char * zUnspecified = "Unspecified error.";
  jsize const baLen = (*env)->GetArrayLength(env, baMsg);
  jbyte * const pjBuf = baMsg ? JBA_TOC(baMsg) : NULL;
  switch(pjBuf ? eTextRep : SQLITE_UTF8){
    case SQLITE_UTF8: {
      const char *zMsg = pjBuf ? (const char *)pjBuf : zUnspecified;
      sqlite3_result_error(PtrGet_sqlite3_context(jpCx), zMsg, baLen);
      break;
    }
    case SQLITE_UTF16: {
      const void *zMsg = pjBuf
        ? (const void *)pjBuf : (const void *)zUnspecified;
      sqlite3_result_error16(PtrGet_sqlite3_context(jpCx), zMsg, baLen);
      break;
    }
    default:
      sqlite3_result_error(PtrGet_sqlite3_context(jpCx),
                           "Invalid encoding argument passed "
                           "to sqlite3_result_error().", -1);
      break;
  }
  JBA_RELEASE(baMsg,pjBuf);
}

JDECL(void,1result_1error_1code)(JENV_JSELF, jobject jpCx, jint v){
  sqlite3_result_error_code(PtrGet_sqlite3_context(jpCx), v ? (int)v : SQLITE_ERROR);
}

JDECL(void,1result_1error_1nomem)(JENV_JSELF, jobject jpCx){
  sqlite3_result_error_nomem(PtrGet_sqlite3_context(jpCx));
}

JDECL(void,1result_1error_1toobig)(JENV_JSELF, jobject jpCx){
  sqlite3_result_error_toobig(PtrGet_sqlite3_context(jpCx));
}

JDECL(void,1result_1int)(JENV_JSELF, jobject jpCx, jint v){
  sqlite3_result_int(PtrGet_sqlite3_context(jpCx), (int)v);
}

JDECL(void,1result_1int64)(JENV_JSELF, jobject jpCx, jlong v){
  sqlite3_result_int64(PtrGet_sqlite3_context(jpCx), (sqlite3_int64)v);
}

JDECL(void,1result_1java_1object)(JENV_JSELF, jobject jpCx, jobject v){
  if(v){
    ResultJavaVal * const rjv = ResultJavaVal_alloc(env, v);
    if(rjv){
      sqlite3_result_pointer(PtrGet_sqlite3_context(jpCx), rjv, RESULT_JAVA_VAL_STRING,
                             ResultJavaVal_finalizer);
    }else{
      sqlite3_result_error_nomem(PtrGet_sqlite3_context(jpCx));
    }
  }else{
    sqlite3_result_null(PtrGet_sqlite3_context(jpCx));
  }
}

JDECL(void,1result_1null)(JENV_JSELF, jobject jpCx){
  sqlite3_result_null(PtrGet_sqlite3_context(jpCx));
}

JDECL(void,1result_1text)(JENV_JSELF, jobject jpCx, jbyteArray jBa, jint nMax){
  return result_blob_text(0, 0, SQLITE_UTF8, env, PtrGet_sqlite3_context(jpCx), jBa, nMax);
}

JDECL(void,1result_1text64)(JENV_JSELF, jobject jpCx, jbyteArray jBa, jlong nMax,
                            jint eTextRep){
  return result_blob_text(0, 1, eTextRep, env, PtrGet_sqlite3_context(jpCx), jBa, nMax);
}

JDECL(void,1result_1value)(JENV_JSELF, jobject jpCx, jobject jpSVal){
  sqlite3_result_value(PtrGet_sqlite3_context(jpCx), PtrGet_sqlite3_value(jpSVal));
}

JDECL(void,1result_1zeroblob)(JENV_JSELF, jobject jpCx, jint v){
  sqlite3_result_zeroblob(PtrGet_sqlite3_context(jpCx), (int)v);
}

JDECL(jint,1result_1zeroblob64)(JENV_JSELF, jobject jpCx, jlong v){
  return (jint)sqlite3_result_zeroblob64(PtrGet_sqlite3_context(jpCx), (sqlite3_int64)v);
}


JDECL(void,1set_1last_1insert_1rowid)(JENV_JSELF, jobject jpDb, jlong rowId){
  sqlite3_set_last_insert_rowid(PtrGet_sqlite3_context(jpDb),
                                (sqlite3_int64)rowId);
}

JDECL(jint,1shutdown)(JENV_JSELF){
  PerDbStateJni_free_all();
  JNIEnvCache_clear(&S3Global.envCache);
  /* Do not clear S3Global.jvm: it's legal to call
     sqlite3_initialize() again to restart the lib. */
  return sqlite3_shutdown();
}

static int s3jni_trace_impl(unsigned traceflag, void *pC, void *pP, void *pX){
  PerDbStateJni * const ps = (PerDbStateJni *)pC;
  JNIEnv * const env = ps->env;
  jobject jX = NULL;
  JNIEnvCacheLine * const pEcl = S3Global_env_cache(env);
  int rc;
  /**
     TODO: convert pX depending on traceflag:

     SQLITE_TRACE_STMT: String
     SQLITE_TRACE_PROFILE: Long
     others: null
  */
  switch(traceflag){
  case SQLITE_TRACE_STMT:
    /* This is not _quite_ right: we're converting to MUTF-8.  It
       should(?) suffice for purposes of tracing, though. */
    jX = (*env)->NewStringUTF(env, (const char *)pX);
    break;
  case SQLITE_TRACE_PROFILE:
    jX = (*env)->NewObject(env, pEcl->globalClassLong, pEcl->ctorLong1,
                           (jlong)*((sqlite3_int64*)pX));
    break;
  }
  rc = (int)(*env)->CallIntMethod(env, ps->trace.jObj,
                                  ps->trace.midCallback,
                                  (jint)traceflag, (jlong)pP, jX);
  UNREF_L(jX);
  IFTHREW{
    EXCEPTION_CLEAR;
    return rc ? rc :
      s3jni_db_error(ps->pDb, SQLITE_ERROR,
                     "sqlite3_trace_v2() callback threw.");
  }
  return rc;
}

JDECL(jint,1trace_1v2)(JENV_JSELF,jobject jDb, jint traceMask, jobject jTracer){
  sqlite3 * const pDb = PtrGet_sqlite3(jDb);
  PerDbStateJni * ps;
  jclass klazz;
  if( !traceMask || !jTracer ){
    return (jint)sqlite3_trace_v2(pDb, 0, 0, 0);
  }
  ps = PerDbStateJni_for_db(env, pDb, 1);
  if(!ps) return SQLITE_NOMEM;
  klazz = (*env)->GetObjectClass(env, jTracer);
  ps->trace.midCallback = (*env)->GetMethodID(env, klazz, "xCallback",
                                              "(IJLjava/lang/Object;)I");
  IFTHREW {
    /* Leave ps in place - it might contain other state. */
    EXCEPTION_CLEAR;
    return s3jni_db_error(pDb, SQLITE_ERROR,
                          "Cannot not find matchin xCallback() on Tracer object.");
  }
  ps->trace.jObj = REF_G(jTracer);
  return sqlite3_trace_v2(pDb, (unsigned)traceMask, s3jni_trace_impl, ps);
}


JDECL(jbyteArray,1value_1blob)(JENV_JSELF, jobject jpSVal){
  sqlite3_value * const sv = PtrGet_sqlite3_value(jpSVal);
  int const nLen = sqlite3_value_bytes(sv);
  const jbyte * pBytes = sqlite3_value_blob(sv);
  jbyteArray const jba = pBytes
    ? (*env)->NewByteArray(env, (jsize)nLen)
    : NULL;
  if(jba){
    (*env)->SetByteArrayRegion(env, jba, 0, nLen, pBytes);
  }
  return jba;
}


JDECL(jdouble,1value_1double)(JENV_JSELF, jobject jpSVal){
  return (jdouble) sqlite3_value_double(PtrGet_sqlite3_value(jpSVal));
}


JDECL(jobject,1value_1dup)(JENV_JSELF, jobject jpSVal){
  sqlite3_value * const sv = sqlite3_value_dup(PtrGet_sqlite3_value(jpSVal));
  return sv ? new_sqlite3_value_wrapper(env, sv) : 0;
}

JDECL(void,1value_1free)(JENV_JSELF, jobject jpSVal){
  sqlite3_value_free(PtrGet_sqlite3_value(jpSVal));
}

JDECL(jint,1value_1int)(JENV_JSELF, jobject jpSVal){
  return (jint) sqlite3_value_int(PtrGet_sqlite3_value(jpSVal));
}

JDECL(jlong,1value_1int64)(JENV_JSELF, jobject jpSVal){
  return (jlong) sqlite3_value_int64(PtrGet_sqlite3_value(jpSVal));
}

JDECL(jobject,1value_1java_1object)(JENV_JSELF, jobject jpSVal){
  ResultJavaVal * const rv = sqlite3_value_pointer(PtrGet_sqlite3_value(jpSVal), RESULT_JAVA_VAL_STRING);
  return rv ? rv->jObj : NULL;
}

JDECL(jstring,1value_1text)(JENV_JSELF, jobject jpSVal){
  sqlite3_value * const sv = PtrGet_sqlite3_value(jpSVal);
  int const n = sqlite3_value_bytes16(sv);
  const void * const p = sqlite3_value_text16(sv);
  return s3jni_text_to_jstring(env, p, n);
}

JDECL(jbyteArray,1value_1text_1utf8)(JENV_JSELF, jobject jpSVal){
  sqlite3_value * const sv = PtrGet_sqlite3_value(jpSVal);
  int const n = sqlite3_value_bytes(sv);
  const unsigned char * const p = sqlite3_value_text(sv);
  return s3jni_new_jbyteArray(env, p, n);
}

static jbyteArray value_text16(int mode, JNIEnv *env, jobject jpSVal){
  int const nLen = sqlite3_value_bytes16(PtrGet_sqlite3_value(jpSVal));
  jbyteArray jba;
  const jbyte * pBytes;
  switch(mode){
    case SQLITE_UTF16:
      pBytes = sqlite3_value_text16(PtrGet_sqlite3_value(jpSVal));
      break;
    case SQLITE_UTF16LE:
      pBytes = sqlite3_value_text16le(PtrGet_sqlite3_value(jpSVal));
      break;
    case SQLITE_UTF16BE:
      pBytes = sqlite3_value_text16be(PtrGet_sqlite3_value(jpSVal));
      break;
    default:
      assert(!"not possible");
      return NULL;
  }
  jba = pBytes
    ? (*env)->NewByteArray(env, (jsize)nLen)
    : NULL;
  if(jba){
    (*env)->SetByteArrayRegion(env, jba, 0, nLen, pBytes);
  }
  return jba;
}

JDECL(jbyteArray,1value_1text16)(JENV_JSELF, jobject jpSVal){
  return value_text16(SQLITE_UTF16, env, jpSVal);
}

JDECL(jbyteArray,1value_1text16le)(JENV_JSELF, jobject jpSVal){
  return value_text16(SQLITE_UTF16LE, env, jpSVal);
}

JDECL(jbyteArray,1value_1text16be)(JENV_JSELF, jobject jpSVal){
  return value_text16(SQLITE_UTF16BE, env, jpSVal);
}



////////////////////////////////////////////////////////////////////////
// End of the main API bindings. What follows are internal utilities.
////////////////////////////////////////////////////////////////////////

/**
   Called during static init of the SQLite3Jni class to sync certain
   compile-time constants to Java-space.

   This routine is why we have to #include sqlite3.c instead of
   sqlite3.h.
*/
JNIEXPORT void JNICALL
Java_org_sqlite_jni_SQLite3Jni_init(JNIEnv *env, jclass self, jobject sJni){
  typedef struct {
    const char *zName;
    int value;
  } LimitEntry;
  const LimitEntry aLimits[] = {
    {"SQLITE_MAX_ALLOCATION_SIZE", SQLITE_MAX_ALLOCATION_SIZE},
    {"SQLITE_LIMIT_LENGTH", SQLITE_LIMIT_LENGTH},
    {"SQLITE_MAX_LENGTH", SQLITE_MAX_LENGTH},
    {"SQLITE_LIMIT_SQL_LENGTH", SQLITE_LIMIT_SQL_LENGTH},
    {"SQLITE_MAX_SQL_LENGTH", SQLITE_MAX_SQL_LENGTH},
    {"SQLITE_LIMIT_COLUMN", SQLITE_LIMIT_COLUMN},
    {"SQLITE_MAX_COLUMN", SQLITE_MAX_COLUMN},
    {"SQLITE_LIMIT_EXPR_DEPTH", SQLITE_LIMIT_EXPR_DEPTH},
    {"SQLITE_MAX_EXPR_DEPTH", SQLITE_MAX_EXPR_DEPTH},
    {"SQLITE_LIMIT_COMPOUND_SELECT", SQLITE_LIMIT_COMPOUND_SELECT},
    {"SQLITE_MAX_COMPOUND_SELECT", SQLITE_MAX_COMPOUND_SELECT},
    {"SQLITE_LIMIT_VDBE_OP", SQLITE_LIMIT_VDBE_OP},
    {"SQLITE_MAX_VDBE_OP", SQLITE_MAX_VDBE_OP},
    {"SQLITE_LIMIT_FUNCTION_ARG", SQLITE_LIMIT_FUNCTION_ARG},
    {"SQLITE_MAX_FUNCTION_ARG", SQLITE_MAX_FUNCTION_ARG},
    {"SQLITE_LIMIT_ATTACHED", SQLITE_LIMIT_ATTACHED},
    {"SQLITE_MAX_ATTACHED", SQLITE_MAX_ATTACHED},
    {"SQLITE_LIMIT_LIKE_PATTERN_LENGTH", SQLITE_LIMIT_LIKE_PATTERN_LENGTH},
    {"SQLITE_MAX_LIKE_PATTERN_LENGTH", SQLITE_MAX_LIKE_PATTERN_LENGTH},
    {"SQLITE_LIMIT_VARIABLE_NUMBER", SQLITE_LIMIT_VARIABLE_NUMBER},
    {"SQLITE_MAX_VARIABLE_NUMBER", SQLITE_MAX_VARIABLE_NUMBER},
    {"SQLITE_LIMIT_TRIGGER_DEPTH", SQLITE_LIMIT_TRIGGER_DEPTH},
    {"SQLITE_MAX_TRIGGER_DEPTH", SQLITE_MAX_TRIGGER_DEPTH},
    {"SQLITE_LIMIT_WORKER_THREADS", SQLITE_LIMIT_WORKER_THREADS},
    {"SQLITE_MAX_WORKER_THREADS", SQLITE_MAX_WORKER_THREADS},
    {0,0}
  };
  jfieldID fieldId;
  jclass const klazz = (*env)->GetObjectClass(env, sJni);
  const LimitEntry * pLimit;
  memset(&S3Global, 0, sizeof(S3Global));
  (void)S3Global_env_cache(env);
  assert( 1 == S3Global.envCache.used );
  assert( env == S3Global.envCache.lines[0].env );
  assert( 0 != S3Global.envCache.lines[0].globalClassObj );
  if( (*env)->GetJavaVM(env, &S3Global.jvm) ){
    (*env)->FatalError(env, "GetJavaVM() failure shouldn't be possible.");
  }

  for( pLimit = &aLimits[0]; pLimit->zName; ++pLimit ){
    fieldId = (*env)->GetStaticFieldID(env, klazz, pLimit->zName, "I");
    //MARKER(("Setting %s (field=%p) = %d\n", pLimit->zName, fieldId, pLimit->value));
    assert(fieldId);
    (*env)->SetStaticIntField(env, klazz, fieldId, (jint)pLimit->value);
  }
}

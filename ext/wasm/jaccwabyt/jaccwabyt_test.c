#include <assert.h>
#include <string.h> /* memset() */
#include <stddef.h> /* offsetof() */
#include <stdio.h>  /* snprintf() */
#include <stdint.h> /* int64_t */
/*#include <stdlib.h>*/ /* malloc/free(), needed for emscripten exports. */
extern void * malloc(size_t);
extern void free(void *);

/*
**  2022-06-25
**
**  The author disclaims copyright to this source code.  In place of a
**  legal notice, here is a blessing:
**
**  *   May you do good and not evil.
**  *   May you find forgiveness for yourself and forgive others.
**  *   May you share freely, never taking more than you give.
**
***********************************************************************
**
** Utility functions for use with the emscripten/WASM bits. These
** functions ARE NOT part of the sqlite3 public API. They are strictly
** for internal use by the JS/WASM bindings.
**
** This file is intended to be WASM-compiled together with sqlite3.c,
** e.g.:
**
**  emcc ... sqlite3.c wasm_util.c
*/

/*
** Experimenting with output parameters.
*/
int jaccwabyt_test_intptr(int * p){
  if(1==((int)p)%3){
    /* kludge to get emscripten to export malloc() and free() */;
    free(malloc(0));
  }
  return *p = *p * 2;
}
int64_t jaccwabyt_test_int64_max(void){
  return (int64_t)0x7fffffffffffffff;
}
int64_t jaccwabyt_test_int64_min(void){
  return ~jaccwabyt_test_int64_max();
}
int64_t jaccwabyt_test_int64_times2(int64_t x){
  return x * 2;
}

void jaccwabyt_test_int64_minmax(int64_t * min, int64_t *max){
  *max = jaccwabyt_test_int64_max();
  *min = jaccwabyt_test_int64_min();
  /*printf("minmax: min=%lld, max=%lld\n", *min, *max);*/
}
int64_t jaccwabyt_test_int64ptr(int64_t * p){
  /*printf("jaccwabyt_test_int64ptr( @%lld = 0x%llx )\n", (int64_t)p, *p);*/
  return *p = *p * 2;
}

void jaccwabyt_test_stack_overflow(int recurse){
  if(recurse) jaccwabyt_test_stack_overflow(recurse);
}

struct WasmTestStruct {
  int v4;
  void * ppV;
  const char * cstr;
  int64_t v8;
  void (*xFunc)(void*);
};
typedef struct WasmTestStruct WasmTestStruct;
void jaccwabyt_test_struct(WasmTestStruct * s){
  if(s){
    s->v4 *= 2;
    s->v8 = s->v4 * 2;
    s->ppV = s;
    s->cstr = __FILE__;
    if(s->xFunc) s->xFunc(s);
  }
  return;
}

/** For testing the 'string-free' whwasmutil.xWrap() conversion. */
char * jaccwabyt_test_str_hello(int fail){
  char * s = fail ? 0 : (char *)malloc(6);
  if(s){
    memcpy(s, "hello", 5);
    s[5] = 0;
  }
  return s;
}

/*
** Returns a NUL-terminated string containing a JSON-format metadata
** regarding C structs, for use with the StructBinder API. The
** returned memory is static and is only written to the first time
** this is called.
*/
const char * jaccwabyt_test_ctype_json(void){
  static char strBuf[1024 * 8] = {0};
  int n = 0, structCount = 0, groupCount = 0;
  char * pos = &strBuf[1] /* skip first byte for now to help protect
                             against a small race condition */;
  char const * const zEnd = pos + sizeof(strBuf);
  if(strBuf[0]) return strBuf;
  /* Leave first strBuf[0] at 0 until the end to help guard against a
     tiny race condition. If this is called twice concurrently, they
     might end up both writing to strBuf, but they'll both write the
     same thing, so that's okay. If we set byte 0 up front then the
     2nd instance might return a partially-populated string. */

  ////////////////////////////////////////////////////////////////////
  // First we need to build up our macro framework...
  ////////////////////////////////////////////////////////////////////
  // Core output macros...
#define lenCheck assert(pos < zEnd - 100)
#define outf(format,...) \
  pos += snprintf(pos, ((size_t)(zEnd - pos)), format, __VA_ARGS__); \
  lenCheck
#define out(TXT) outf("%s",TXT)
#define CloseBrace(LEVEL) \
  assert(LEVEL<5); memset(pos, '}', LEVEL); pos+=LEVEL; lenCheck

  ////////////////////////////////////////////////////////////////////
  // Macros for emitting StructBinder descriptions...
#define StructBinder__(TYPE)                 \
  n = 0;                                     \
  outf("%s{", (structCount++ ? ", " : ""));  \
  out("\"name\": \"" # TYPE "\",");         \
  outf("\"sizeof\": %d", (int)sizeof(TYPE)); \
  out(",\"members\": {");
#define StructBinder_(T) StructBinder__(T)
// ^^^ indirection needed to expand CurrentStruct
#define StructBinder StructBinder_(CurrentStruct)
#define _StructBinder CloseBrace(2)
#define M(MEMBER,SIG)                                         \
  outf("%s\"%s\": "                                           \
       "{\"offset\":%d,\"sizeof\": %d,\"signature\":\"%s\"}", \
       (n++ ? ", " : ""), #MEMBER,                            \
       (int)offsetof(CurrentStruct,MEMBER),                   \
       (int)sizeof(((CurrentStruct*)0)->MEMBER),              \
       SIG)
  // End of macros
  ////////////////////////////////////////////////////////////////////
  
  out("\"structs\": ["); {

#define CurrentStruct WasmTestStruct
    StructBinder {
      M(v4,"i");
      M(cstr,"s");
      M(ppV,"p");
      M(v8,"j");
      M(xFunc,"v(p)");
    } _StructBinder;
#undef CurrentStruct

  } out( "]"/*structs*/);
  out("}"/*top-level object*/);
  *pos = 0;
  strBuf[0] = '{'/*end of the race-condition workaround*/;
  return strBuf;
#undef DefGroup
#undef Def
#undef _DefGroup
#undef StructBinder
#undef StructBinder_
#undef StructBinder__
#undef M
#undef _StructBinder
#undef CurrentStruct
#undef CloseBrace
#undef out
#undef outf
#undef lenCheck
}

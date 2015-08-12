/*
** 2015-08-12
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
** This SQLite extension implements JSON functions.  The interface is
** modeled after MySQL JSON functions:
**
**     https://dev.mysql.com/doc/refman/5.7/en/json.html
**
** JSON is pure text.  JSONB is a binary encoding that is smaller and easier
** to parse but which holds the equivalent information.  Conversions between
** JSON and JSONB are lossless.
**
** Most of the functions here will accept either JSON or JSONB input.  The
** input is understood to be JSONB if it a BLOB and JSON if the input is
** of any other type.  Functions that begin with the "json_" prefix return
** JSON and functions that begin with "jsonb_" return JSONB.
*/
#include "sqlite3ext.h"
SQLITE_EXTENSION_INIT1
#include <assert.h>
#include <string.h>

/* Unsigned integer types */
typedef sqlite3_uint64 u64;
typedef unsigned int u32;
typedef unsigned char u8;

/* An instance of this object represents a JSON string under construction.
*/
typedef struct Json Json;
struct Json {
  sqlite3_context *pCtx;   /* Function context - put error messages here */
  char *zBuf;              /* Append JSON text here */
  u64 nAlloc;              /* Bytes of storage available in zBuf[] */
  u64 nUsed;               /* Bytes of zBuf[] currently used */
  u8 bStatic;              /* True if zBuf is static space */
  u8 mallocFailed;         /* True if an OOM has been encountered */
  char zSpace[100];        /* Initial static space */
};

/* Set the Json object to an empty string
*/
static void jsonZero(Json *p){
  p->zBuf = p->zSpace;
  p->nAlloc = sizeof(p->zSpace);
  p->nUsed = 0;
  p->bStatic = 1;
}

/* Initialize the Json object
*/
static void jsonInit(Json *p, sqlite3_context *pCtx){
  p->pCtx = pCtx;
  p->mallocFailed = 0;
  jsonZero(p);
}


/* Free all allocated memory and reset the Json object back to its
** initial state.
*/
static void jsonReset(Json *p){
  if( !p->bStatic ) sqlite3_free(p->zBuf);
  jsonZero(p);
}


/* Report an out-of-memory (OOM) condition 
*/
static void jsonOom(Json *p){
  p->mallocFailed = 1;
  sqlite3_result_error_nomem(p->pCtx);
  jsonReset(p);
}

/* Enlarge pJson->zBuf so that it can hold at least N more bytes.
** Return zero on success.  Return non-zero on an OOM error
*/
static int jsonGrow(Json *p, u32 N){
  u64 nTotal = N<p->nAlloc ? p->nAlloc*2 : p->nAlloc+N+100;
  char *zNew;
  if( p->bStatic ){
    if( p->mallocFailed ) return SQLITE_NOMEM;
    zNew = sqlite3_malloc64(nTotal);
    if( zNew==0 ){
      jsonOom(p);
      return SQLITE_NOMEM;
    }
    memcpy(zNew, p->zBuf, p->nUsed);
    p->zBuf = zNew;
    p->bStatic = 0;
  }else{
    zNew = sqlite3_realloc64(p->zBuf, nTotal);
    if( zNew==0 ){
      jsonOom(p);
      return SQLITE_NOMEM;
    }
    p->zBuf = zNew;
  }
  p->nAlloc = nTotal;
  return SQLITE_OK;
}

/* Append N bytes from zIn onto the end of the Json string.
*/
static void jsonAppendRaw(Json *p, const char *zIn, u32 N){
  if( (N+p->nUsed >= p->nAlloc) && jsonGrow(p,N)!=0 ) return;
  memcpy(p->zBuf+p->nUsed, zIn, N);
  p->nUsed += N;
}

/* Append the N-byte string in zIn to the end of the Json string
** under construction.  Enclose the string in "..." and escape
** any double-quotes or backslash characters contained within the
** string.
*/
static void jsonAppendString(Json *p, const char *zIn, u32 N){
  u32 i;
  if( (N+p->nUsed+2 >= p->nAlloc) && jsonGrow(p,N+2)!=0 ) return;
  p->zBuf[p->nUsed++] = '"';
  for(i=0; i<N; i++){
    char c = zIn[i];
    if( c=='"' || c=='\\' ){
      if( (p->nUsed+N+1-i > p->nAlloc) && jsonGrow(p,N+1-i)!=0 ) return;
      p->zBuf[p->nUsed++] = '\\';
    }
    p->zBuf[p->nUsed++] = c;
  }
  p->zBuf[p->nUsed++] = '"';
}

/* Make pJson the result of the SQL function.
*/
static void jsonResult(Json *p){
  if( p->mallocFailed==0 ){
    sqlite3_result_text64(p->pCtx, p->zBuf, p->nUsed, 
                          p->bStatic ? SQLITE_TRANSIENT : sqlite3_free,
                          SQLITE_UTF8);
    jsonZero(p);
  }
  assert( p->bStatic );
}

/*
** Implementation of the json_array(VALUE,...) function.  Return a JSON
** array that contains all values given in arguments.  Or if any argument
** is a BLOB, throw an error.
*/
static void jsonArrayFunc(
  sqlite3_context *context,
  int argc,
  sqlite3_value **argv
){
  int i;
  Json jx;
  char cSep = '[';

  jsonInit(&jx, context);
  for(i=0; i<argc; i++){
    jsonAppendRaw(&jx, &cSep, 1);
    cSep = ',';
    switch( sqlite3_value_type(argv[i]) ){
      case SQLITE_NULL: {
        jsonAppendRaw(&jx, "null", 4);
        break;
      }
      case SQLITE_INTEGER:
      case SQLITE_FLOAT: {
        const char *z = (const char*)sqlite3_value_text(argv[i]);
        u32 n = (u32)sqlite3_value_bytes(argv[i]);
        jsonAppendRaw(&jx, z, n);
        break;
      }
      case SQLITE_TEXT: {
        const char *z = (const char*)sqlite3_value_text(argv[i]);
        u32 n = (u32)sqlite3_value_bytes(argv[i]);
        jsonAppendString(&jx, z, n);
        break;
      }
      default: {
        jsonZero(&jx);
        sqlite3_result_error(context, "JSON cannot hold BLOB values", -1);
        return;
      }
    }
  }
  jsonAppendRaw(&jx, "]", 1);
  jsonResult(&jx);      
}

#ifdef _WIN32
__declspec(dllexport)
#endif
int sqlite3_json_init(
  sqlite3 *db, 
  char **pzErrMsg, 
  const sqlite3_api_routines *pApi
){
  int rc = SQLITE_OK;
  int i;
  static const struct {
     const char *zName;
     int nArg;
     void (*xFunc)(sqlite3_context*,int,sqlite3_value**);
  } aFunc[] = {
    { "json_array",  -1,  jsonArrayFunc },
  };
  SQLITE_EXTENSION_INIT2(pApi);
  (void)pzErrMsg;  /* Unused parameter */
  for(i=0; i<sizeof(aFunc)/sizeof(aFunc[0]) && rc==SQLITE_OK; i++){
    rc = sqlite3_create_function(db, aFunc[i].zName, aFunc[i].nArg,
                                 SQLITE_UTF8 | SQLITE_DETERMINISTIC, 0,
                                 aFunc[i].xFunc, 0, 0);
  }
  return rc;
}

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
** This SQLite JSON functions.
**
** This file began as an extension in ext/misc/json1.c in 2015.  That
** extension proved so useful that it has now been moved into the core.
**
** For the time being, all JSON is stored as pure text.  (We might add
** a JSONB type in the future which stores a binary encoding of JSON in
** a BLOB, but there is no support for JSONB in the current implementation.
** This implementation parses JSON text at 250 MB/s, so it is hard to see
** how JSONB might improve on that.)
*/
#ifndef SQLITE_OMIT_JSON
#include "sqliteInt.h"

/*
** Growing our own isspace() routine this way is twice as fast as
** the library isspace() function, resulting in a 7% overall performance
** increase for the parser.  (Ubuntu14.10 gcc 4.8.4 x64 with -Os).
*/
static const char jsonIsSpace[] = {
  0, 0, 0, 0, 0, 0, 0, 0,  0, 1, 1, 0, 0, 1, 0, 0,
  0, 0, 0, 0, 0, 0, 0, 0,  0, 0, 0, 0, 0, 0, 0, 0,
  1, 0, 0, 0, 0, 0, 0, 0,  0, 0, 0, 0, 0, 0, 0, 0,
  0, 0, 0, 0, 0, 0, 0, 0,  0, 0, 0, 0, 0, 0, 0, 0,
  0, 0, 0, 0, 0, 0, 0, 0,  0, 0, 0, 0, 0, 0, 0, 0,
  0, 0, 0, 0, 0, 0, 0, 0,  0, 0, 0, 0, 0, 0, 0, 0,
  0, 0, 0, 0, 0, 0, 0, 0,  0, 0, 0, 0, 0, 0, 0, 0,
  0, 0, 0, 0, 0, 0, 0, 0,  0, 0, 0, 0, 0, 0, 0, 0,

  0, 0, 0, 0, 0, 0, 0, 0,  0, 0, 0, 0, 0, 0, 0, 0,
  0, 0, 0, 0, 0, 0, 0, 0,  0, 0, 0, 0, 0, 0, 0, 0,
  0, 0, 0, 0, 0, 0, 0, 0,  0, 0, 0, 0, 0, 0, 0, 0,
  0, 0, 0, 0, 0, 0, 0, 0,  0, 0, 0, 0, 0, 0, 0, 0,
  0, 0, 0, 0, 0, 0, 0, 0,  0, 0, 0, 0, 0, 0, 0, 0,
  0, 0, 0, 0, 0, 0, 0, 0,  0, 0, 0, 0, 0, 0, 0, 0,
  0, 0, 0, 0, 0, 0, 0, 0,  0, 0, 0, 0, 0, 0, 0, 0,
  0, 0, 0, 0, 0, 0, 0, 0,  0, 0, 0, 0, 0, 0, 0, 0,
};
#define fast_isspace(x) (jsonIsSpace[(unsigned char)x])

/*
** Characters that are special to JSON.  Control charaters,
** '"' and '\\'.
*/
static const char jsonIsOk[256] = {
  0, 0, 0, 0, 0, 0, 0, 0,  0, 0, 0, 0, 0, 0, 0, 0,
  0, 0, 0, 0, 0, 0, 0, 0,  0, 0, 0, 0, 0, 0, 0, 0,
  1, 1, 0, 1, 1, 1, 1, 0,  1, 1, 1, 1, 1, 1, 1, 1,
  1, 1, 1, 1, 1, 1, 1, 1,  1, 1, 1, 1, 1, 1, 1, 1,
  1, 1, 1, 1, 1, 1, 1, 1,  1, 1, 1, 1, 1, 1, 1, 1,
  1, 1, 1, 1, 1, 1, 1, 1,  1, 1, 1, 1, 0, 1, 1, 1,
  1, 1, 1, 1, 1, 1, 1, 1,  1, 1, 1, 1, 1, 1, 1, 1,
  1, 1, 1, 1, 1, 1, 1, 1,  1, 1, 1, 1, 1, 1, 1, 1,

  1, 1, 1, 1, 1, 1, 1, 1,  1, 1, 1, 1, 1, 1, 1, 1,
  1, 1, 1, 1, 1, 1, 1, 1,  1, 1, 1, 1, 1, 1, 1, 1,
  1, 1, 1, 1, 1, 1, 1, 1,  1, 1, 1, 1, 1, 1, 1, 1,
  1, 1, 1, 1, 1, 1, 1, 1,  1, 1, 1, 1, 1, 1, 1, 1,
  1, 1, 1, 1, 1, 1, 1, 1,  1, 1, 1, 1, 1, 1, 1, 1,
  1, 1, 1, 1, 1, 1, 1, 1,  1, 1, 1, 1, 1, 1, 1, 1,
  1, 1, 1, 1, 1, 1, 1, 1,  1, 1, 1, 1, 1, 1, 1, 1,
  1, 1, 1, 1, 1, 1, 1, 1,  1, 1, 1, 1, 1, 1, 1, 1
};


#if !defined(SQLITE_DEBUG) && !defined(SQLITE_COVERAGE_TEST)
#  define VVA(X)
#else
#  define VVA(X) X
#endif

/* Objects */
typedef struct JsonString JsonString;
typedef struct JsonNode JsonNode;
typedef struct JsonParse JsonParse;
typedef struct JsonCleanup JsonCleanup;

/* An instance of this object represents a JSON string
** under construction.  Really, this is a generic string accumulator
** that can be and is used to create strings other than JSON.
*/
struct JsonString {
  sqlite3_context *pCtx;   /* Function context - put error messages here */
  char *zBuf;              /* Append JSON content here */
  u64 nAlloc;              /* Bytes of storage available in zBuf[] */
  u64 nUsed;               /* Bytes of zBuf[] currently used */
  u8 bStatic;              /* True if zBuf is static space */
  u8 bErr;                 /* True if an error has been encountered */
  char zSpace[100];        /* Initial static space */
};

/* A deferred cleanup task.  A list of JsonCleanup objects might be
** run when the JsonParse object is destroyed.
*/
struct JsonCleanup {
  JsonCleanup *pJCNext;    /* Next in a list */
  void (*xOp)(void*);      /* Routine to run */
  void *pArg;              /* Argument to xOp() */
};

/* JSON type values
*/
#define JSON_SUBST    0    /* Special edit node.  Uses u.iPrev */
#define JSON_NULL     1
#define JSON_TRUE     2
#define JSON_FALSE    3
#define JSON_INT      4
#define JSON_REAL     5
#define JSON_STRING   6
#define JSON_ARRAY    7
#define JSON_OBJECT   8

/* The "subtype" set for JSON values */
#define JSON_SUBTYPE  74    /* Ascii for "J" */

/*
** Names of the various JSON types:
*/
static const char * const jsonType[] = {
  "subst",
  "null", "true", "false", "integer", "real", "text", "array", "object"
};

/* Bit values for the JsonNode.jnFlag field
*/
#define JNODE_RAW     0x01  /* Content is raw, not JSON encoded */
#define JNODE_ESCAPE  0x02  /* Content is text with \ escapes */
#define JNODE_REMOVE  0x04  /* Do not output */
#define JNODE_REPLACE 0x08  /* Target of a JSON_SUBST node */
#define JNODE_APPEND  0x10  /* More ARRAY/OBJECT entries at u.iAppend */
#define JNODE_LABEL   0x20  /* Is a label of an object */
#define JNODE_JSON5   0x40  /* Node contains JSON5 enhancements */


/* A single node of parsed JSON.  An array of these nodes describes
** a parse of JSON + edits.
**
** Use the json_parse() SQL function (available when compiled with
** -DSQLITE_DEBUG) to see a dump of complete JsonParse objects, including
** a complete listing and decoding of the array of JsonNodes.
*/
struct JsonNode {
  u8 eType;              /* One of the JSON_ type values */
  u8 jnFlags;            /* JNODE flags */
  u8 eU;                 /* Which union element to use */
  u32 n;                 /* Bytes of content for INT, REAL or STRING
                         ** Number of sub-nodes for ARRAY and OBJECT
                         ** Node that SUBST applies to */
  union {
    const char *zJContent; /* 1: Content for INT, REAL, and STRING */
    u32 iAppend;           /* 2: More terms for ARRAY and OBJECT */
    u32 iKey;              /* 3: Key for ARRAY objects in json_tree() */
    u32 iPrev;             /* 4: Previous SUBST node, or 0 */
  } u;
};


/* A parsed and possibly edited JSON string.  Lifecycle:
**
**   1.  JSON comes in and is parsed into an array aNode[].  The original
**       JSON text is stored in zJson.
**
**   2.  Zero or more changes are made (via json_remove() or json_replace()
**       or similar) to the aNode[] array.
**
**   3.  A new, edited and mimified JSON string is generated from aNode
**       and stored in zAlt.  The JsonParse object always owns zAlt.
**
** Step 1 always happens.  Step 2 and 3 may or may not happen, depending
** on the operation.
**
** aNode[].u.zJContent entries typically point into zJson.  Hence zJson
** must remain valid for the lifespan of the parse.  For edits,
** aNode[].u.zJContent might point to malloced space other than zJson.
** Entries in pClup are responsible for freeing that extra malloced space.
**
** When walking the parse tree in aNode[], edits are ignored if useMod is
** false.
*/
struct JsonParse {
  u32 nNode;         /* Number of slots of aNode[] used */
  u32 nAlloc;        /* Number of slots of aNode[] allocated */
  JsonNode *aNode;   /* Array of nodes containing the parse */
  char *zJson;       /* Original JSON string (before edits) */
  char *zAlt;        /* Revised and/or mimified JSON */
  u32 *aUp;          /* Index of parent of each node */
  JsonCleanup *pClup;/* Cleanup operations prior to freeing this object */
  u16 iDepth;        /* Nesting depth */
  u8 nErr;           /* Number of errors seen */
  u8 oom;            /* Set to true if out of memory */
  u8 bJsonIsRCStr;   /* True if zJson is an RCStr */
  u8 hasNonstd;      /* True if input uses non-standard features like JSON5 */
  u8 useMod;         /* Actually use the edits contain inside aNode */
  u8 hasMod;         /* aNode contains edits from the original zJson */
  u32 nJPRef;        /* Number of references to this object */
  int nJson;         /* Length of the zJson string in bytes */
  int nAlt;          /* Length of alternative JSON string zAlt, in bytes */
  u32 iErr;          /* Error location in zJson[] */
  u32 iSubst;        /* Last JSON_SUBST entry in aNode[] */
  u32 iHold;         /* Age of this entry in the cache for LRU replacement */
};

/*
** Maximum nesting depth of JSON for this implementation.
**
** This limit is needed to avoid a stack overflow in the recursive
** descent parser.  A depth of 1000 is far deeper than any sane JSON
** should go.  Historical note: This limit was 2000 prior to version 3.42.0
*/
#define JSON_MAX_DEPTH  1000

/**************************************************************************
** Utility routines for dealing with JsonString objects
**************************************************************************/

/* Set the JsonString object to an empty string
*/
static void jsonZero(JsonString *p){
  p->zBuf = p->zSpace;
  p->nAlloc = sizeof(p->zSpace);
  p->nUsed = 0;
  p->bStatic = 1;
}

/* Initialize the JsonString object
*/
static void jsonInit(JsonString *p, sqlite3_context *pCtx){
  p->pCtx = pCtx;
  p->bErr = 0;
  jsonZero(p);
}

/* Free all allocated memory and reset the JsonString object back to its
** initial state.
*/
static void jsonReset(JsonString *p){
  if( !p->bStatic ) sqlite3RCStrUnref(p->zBuf);
  jsonZero(p);
}

/* Report an out-of-memory (OOM) condition
*/
static void jsonOom(JsonString *p){
  p->bErr = 1;
  sqlite3_result_error_nomem(p->pCtx);
  jsonReset(p);
}

/* Enlarge pJson->zBuf so that it can hold at least N more bytes.
** Return zero on success.  Return non-zero on an OOM error
*/
static int jsonGrow(JsonString *p, u32 N){
  u64 nTotal = N<p->nAlloc ? p->nAlloc*2 : p->nAlloc+N+10;
  char *zNew;
  if( p->bStatic ){
    if( p->bErr ) return 1;
    zNew = sqlite3RCStrNew(nTotal);
    if( zNew==0 ){
      jsonOom(p);
      return SQLITE_NOMEM;
    }
    memcpy(zNew, p->zBuf, (size_t)p->nUsed);
    p->zBuf = zNew;
    p->bStatic = 0;
  }else{
    p->zBuf = sqlite3RCStrResize(p->zBuf, nTotal);
    if( p->zBuf==0 ){
      p->bErr = 1;
      jsonZero(p);
      return SQLITE_NOMEM;
    }
  }
  p->nAlloc = nTotal;
  return SQLITE_OK;
}

/* Append N bytes from zIn onto the end of the JsonString string.
*/
static SQLITE_NOINLINE void jsonAppendExpand(
  JsonString *p,
  const char *zIn,
  u32 N
){
  assert( N>0 );
  if( jsonGrow(p,N) ) return;
  memcpy(p->zBuf+p->nUsed, zIn, N);
  p->nUsed += N;
}
static void jsonAppendRaw(JsonString *p, const char *zIn, u32 N){
  if( N==0 ) return;
  if( N+p->nUsed >= p->nAlloc ){
    jsonAppendExpand(p,zIn,N);
  }else{
    memcpy(p->zBuf+p->nUsed, zIn, N);
    p->nUsed += N;
  }
}
static void jsonAppendRawNZ(JsonString *p, const char *zIn, u32 N){
  assert( N>0 );
  if( N+p->nUsed >= p->nAlloc ){
    jsonAppendExpand(p,zIn,N);
  }else{
    memcpy(p->zBuf+p->nUsed, zIn, N);
    p->nUsed += N;
  }
}


/* Append formatted text (not to exceed N bytes) to the JsonString.
*/
static void jsonPrintf(int N, JsonString *p, const char *zFormat, ...){
  va_list ap;
  if( (p->nUsed + N >= p->nAlloc) && jsonGrow(p, N) ) return;
  va_start(ap, zFormat);
  sqlite3_vsnprintf(N, p->zBuf+p->nUsed, zFormat, ap);
  va_end(ap);
  p->nUsed += (int)strlen(p->zBuf+p->nUsed);
}

/* Append a single character
*/
static SQLITE_NOINLINE void jsonAppendCharExpand(JsonString *p, char c){
  if( jsonGrow(p,1) ) return;
  p->zBuf[p->nUsed++] = c;
}
static void jsonAppendChar(JsonString *p, char c){
  if( p->nUsed>=p->nAlloc ){
    jsonAppendCharExpand(p,c);
  }else{
    p->zBuf[p->nUsed++] = c;
  }
}

/* Try to force the string to be a zero-terminated RCStr string.
**
** Return true on success.  Return false if an OOM prevents this
** from happening.
*/
static int jsonForceRCStr(JsonString *p){
  jsonAppendChar(p, 0);
  if( p->bErr ) return 0;
  p->nUsed--;
  if( p->bStatic==0 ) return 1;
  p->nAlloc = 0;
  p->nUsed++;
  jsonGrow(p, p->nUsed);
  p->nUsed--;
  return p->bStatic==0;
}


/* Append a comma separator to the output buffer, if the previous
** character is not '[' or '{'.
*/
static void jsonAppendSeparator(JsonString *p){
  char c;
  if( p->nUsed==0 ) return;
  c = p->zBuf[p->nUsed-1];
  if( c=='[' || c=='{' ) return;
  jsonAppendChar(p, ',');
}

/* Append the N-byte string in zIn to the end of the JsonString string
** under construction.  Enclose the string in "..." and escape
** any double-quotes or backslash characters contained within the
** string.
*/
static void jsonAppendString(JsonString *p, const char *zIn, u32 N){
  u32 i;
  if( zIn==0 || ((N+p->nUsed+2 >= p->nAlloc) && jsonGrow(p,N+2)!=0) ) return;
  p->zBuf[p->nUsed++] = '"';
  for(i=0; i<N; i++){
    unsigned char c = ((unsigned const char*)zIn)[i];
    if( jsonIsOk[c] ){
      p->zBuf[p->nUsed++] = c;
    }else if( c=='"' || c=='\\' ){
      json_simple_escape:
      if( (p->nUsed+N+3-i > p->nAlloc) && jsonGrow(p,N+3-i)!=0 ) return;
      p->zBuf[p->nUsed++] = '\\';
      p->zBuf[p->nUsed++] = c;
    }else if( c=='\'' ){
      p->zBuf[p->nUsed++] = c;
    }else{
      static const char aSpecial[] = {
         0, 0, 0, 0, 0, 0, 0, 0, 'b', 't', 'n', 0, 'f', 'r', 0, 0,
         0, 0, 0, 0, 0, 0, 0, 0,   0,   0,   0, 0,   0,   0, 0, 0
      };
      assert( sizeof(aSpecial)==32 );
      assert( aSpecial['\b']=='b' );
      assert( aSpecial['\f']=='f' );
      assert( aSpecial['\n']=='n' );
      assert( aSpecial['\r']=='r' );
      assert( aSpecial['\t']=='t' );
      assert( c>=0 && c<sizeof(aSpecial) );
      if( aSpecial[c] ){
        c = aSpecial[c];
        goto json_simple_escape;
      }
      if( (p->nUsed+N+7+i > p->nAlloc) && jsonGrow(p,N+7-i)!=0 ) return;
      p->zBuf[p->nUsed++] = '\\';
      p->zBuf[p->nUsed++] = 'u';
      p->zBuf[p->nUsed++] = '0';
      p->zBuf[p->nUsed++] = '0';
      p->zBuf[p->nUsed++] = "0123456789abcdef"[c>>4];
      p->zBuf[p->nUsed++] = "0123456789abcdef"[c&0xf];
    }
  }
  p->zBuf[p->nUsed++] = '"';
  assert( p->nUsed<p->nAlloc );
}

/*
** The zIn[0..N] string is a JSON5 string literal.  Append to p a translation
** of the string literal that standard JSON and that omits all JSON5
** features.
*/
static void jsonAppendNormalizedString(JsonString *p, const char *zIn, u32 N){
  u32 i;
  jsonAppendChar(p, '"');
  zIn++;
  N -= 2;
  while( N>0 ){
    for(i=0; i<N && zIn[i]!='\\'; i++){}
    if( i>0 ){
      jsonAppendRawNZ(p, zIn, i);
      zIn += i;
      N -= i;
      if( N==0 ) break;
    }
    assert( zIn[0]=='\\' );
    switch( (u8)zIn[1] ){
      case '\'':
        jsonAppendChar(p, '\'');
        break;
      case 'v':
        jsonAppendRawNZ(p, "\\u0009", 6);
        break;
      case 'x':
        jsonAppendRawNZ(p, "\\u00", 4);
        jsonAppendRawNZ(p, &zIn[2], 2);
        zIn += 2;
        N -= 2;
        break;
      case '0':
        jsonAppendRawNZ(p, "\\u0000", 6);
        break;
      case '\r':
        if( zIn[2]=='\n' ){
          zIn++;
          N--;
        }
        break;
      case '\n':
        break;
      case 0xe2:
        assert( N>=4 );
        assert( 0x80==(u8)zIn[2] );
        assert( 0xa8==(u8)zIn[3] || 0xa9==(u8)zIn[3] );
        zIn += 2;
        N -= 2;
        break;
      default:
        jsonAppendRawNZ(p, zIn, 2);
        break;
    }
    zIn += 2;
    N -= 2;
  }
  jsonAppendChar(p, '"');
}

/*
** The zIn[0..N] string is a JSON5 integer literal.  Append to p a translation
** of the string literal that standard JSON and that omits all JSON5
** features.
*/
static void jsonAppendNormalizedInt(JsonString *p, const char *zIn, u32 N){
  if( zIn[0]=='+' ){
    zIn++;
    N--;
  }else if( zIn[0]=='-' ){
    jsonAppendChar(p, '-');
    zIn++;
    N--;
  }
  if( zIn[0]=='0' && (zIn[1]=='x' || zIn[1]=='X') ){
    sqlite3_int64 i = 0;
    int rc = sqlite3DecOrHexToI64(zIn, &i);
    if( rc<=1 ){
      jsonPrintf(100,p,"%lld",i);
    }else{
      assert( rc==2 );
      jsonAppendRawNZ(p, "9.0e999", 7);
    }
    return;
  }
  assert( N>0 );
  jsonAppendRawNZ(p, zIn, N);
}

/*
** The zIn[0..N] string is a JSON5 real literal.  Append to p a translation
** of the string literal that standard JSON and that omits all JSON5
** features.
*/
static void jsonAppendNormalizedReal(JsonString *p, const char *zIn, u32 N){
  u32 i;
  if( zIn[0]=='+' ){
    zIn++;
    N--;
  }else if( zIn[0]=='-' ){
    jsonAppendChar(p, '-');
    zIn++;
    N--;
  }
  if( zIn[0]=='.' ){
    jsonAppendChar(p, '0');
  }
  for(i=0; i<N; i++){
    if( zIn[i]=='.' && (i+1==N || !sqlite3Isdigit(zIn[i+1])) ){
      i++;
      jsonAppendRaw(p, zIn, i);
      zIn += i;
      N -= i;
      jsonAppendChar(p, '0');
      break;
    }
  }
  if( N>0 ){
    jsonAppendRawNZ(p, zIn, N);
  }
}



/*
** Append a function parameter value to the JSON string under
** construction.
*/
static void jsonAppendValue(
  JsonString *p,                 /* Append to this JSON string */
  sqlite3_value *pValue          /* Value to append */
){
  switch( sqlite3_value_type(pValue) ){
    case SQLITE_NULL: {
      jsonAppendRawNZ(p, "null", 4);
      break;
    }
    case SQLITE_FLOAT: {
      jsonPrintf(100, p, "%!0.15g", sqlite3_value_double(pValue));
      break;
    }
    case SQLITE_INTEGER: {
      const char *z = (const char*)sqlite3_value_text(pValue);
      u32 n = (u32)sqlite3_value_bytes(pValue);
      jsonAppendRaw(p, z, n);
      break;
    }
    case SQLITE_TEXT: {
      const char *z = (const char*)sqlite3_value_text(pValue);
      u32 n = (u32)sqlite3_value_bytes(pValue);
      if( sqlite3_value_subtype(pValue)==JSON_SUBTYPE ){
        jsonAppendRaw(p, z, n);
      }else{
        jsonAppendString(p, z, n);
      }
      break;
    }
    default: {
      if( p->bErr==0 ){
        sqlite3_result_error(p->pCtx, "JSON cannot hold BLOB values", -1);
        p->bErr = 2;
        jsonReset(p);
      }
      break;
    }
  }
}


/* Make the JSON in p the result of the SQL function.
**
** The JSON string is reset.
*/
static void jsonResult(JsonString *p){
  if( p->bErr==0 ){
    if( p->bStatic ){
      sqlite3_result_text64(p->pCtx, p->zBuf, p->nUsed,
                            SQLITE_TRANSIENT, SQLITE_UTF8);
    }else if( jsonForceRCStr(p) ){
      sqlite3RCStrRef(p->zBuf);
      sqlite3_result_text64(p->pCtx, p->zBuf, p->nUsed,
                            (void(*)(void*))sqlite3RCStrUnref,
                            SQLITE_UTF8);
    }
  }
  if( p->bErr==1 ){
    sqlite3_result_error_nomem(p->pCtx);
  }
  jsonReset(p);
}

/**************************************************************************
** Utility routines for dealing with JsonNode and JsonParse objects
**************************************************************************/

/*
** Return the number of consecutive JsonNode slots need to represent
** the parsed JSON at pNode.  The minimum answer is 1.  For ARRAY and
** OBJECT types, the number might be larger.
**
** Appended elements are not counted.  The value returned is the number
** by which the JsonNode counter should increment in order to go to the
** next peer value.
*/
static u32 jsonNodeSize(JsonNode *pNode){
  return pNode->eType>=JSON_ARRAY ? pNode->n+1 : 1;
}

/*
** Reclaim all memory allocated by a JsonParse object.  But do not
** delete the JsonParse object itself.
*/
static void jsonParseReset(JsonParse *pParse){
  while( pParse->pClup ){
    JsonCleanup *pTask = pParse->pClup;
    pParse->pClup = pTask->pJCNext;
    pTask->xOp(pTask->pArg);
    sqlite3_free(pTask);
  }
  assert( pParse->nJPRef<=1 );
  if( pParse->aNode ){
    sqlite3_free(pParse->aNode);
    pParse->aNode = 0;
  }
  pParse->nNode = 0;
  pParse->nAlloc = 0;
  if( pParse->aUp ){
    sqlite3_free(pParse->aUp);
    pParse->aUp = 0;
  }
  if( pParse->bJsonIsRCStr ){
    sqlite3RCStrUnref(pParse->zJson);
    pParse->zJson = 0;
    pParse->bJsonIsRCStr = 0;
  }
  if( pParse->zAlt ){
    sqlite3RCStrUnref(pParse->zAlt);
    pParse->zAlt = 0;
  }
}

/*
** Free a JsonParse object that was obtained from sqlite3_malloc().
**
** Note that destroying JsonParse might call sqlite3RCStrUnref() to
** destroy the zJson value.  The RCStr object might recursively invoke
** JsonParse to destroy this pParse object again.  Take care to ensure
** that this recursive destructor sequence terminates harmlessly.
*/
static void jsonParseFree(JsonParse *pParse){
  if( pParse->nJPRef>1 ){
    pParse->nJPRef--;
  }else{
    jsonParseReset(pParse);
    sqlite3_free(pParse);
  }
}

/*
** Add a cleanup task to the JsonParse object.
**
** If an OOM occurs, the cleanup operation happens immediately
** and this function returns SQLITE_NOMEM.
*/
static int jsonParseAddCleanup(
  JsonParse *pParse,          /* Add the cleanup task to this parser */
  void(*xOp)(void*),          /* The cleanup task */
  void *pArg                  /* Argument to the cleanup */
){
  JsonCleanup *pTask = sqlite3_malloc64( sizeof(*pTask) );
  if( pTask==0 ){
    pParse->oom = 1;
    xOp(pArg);
    return SQLITE_ERROR;
  }
  pTask->pJCNext = pParse->pClup;
  pParse->pClup = pTask;
  pTask->xOp = xOp;
  pTask->pArg = pArg;
  return SQLITE_OK;
}

/*
** Convert the JsonNode pNode into a pure JSON string and
** append to pOut.  Subsubstructure is also included.  Return
** the number of JsonNode objects that are encoded.
*/
static void jsonRenderNode(
  JsonParse *pParse,             /* the complete parse of the JSON */
  JsonNode *pNode,               /* The node to render */
  JsonString *pOut               /* Write JSON here */
){
  assert( pNode!=0 );
  while( (pNode->jnFlags & JNODE_REPLACE)!=0 && pParse->useMod ){
    u32 idx = (u32)(pNode - pParse->aNode);
    u32 i = pParse->iSubst;
    while( 1 /*exit-by-break*/ ){
      assert( i<pParse->nNode );
      assert( pParse->aNode[i].eType==JSON_SUBST );
      assert( pParse->aNode[i].eU==4 );
      assert( pParse->aNode[i].u.iPrev<i );
      if( pParse->aNode[i].n==idx ){
        pNode = &pParse->aNode[i+1];
        break;
      }
      i = pParse->aNode[i].u.iPrev;
    }
  }
  switch( pNode->eType ){
    default: {
      assert( pNode->eType==JSON_NULL );
      jsonAppendRawNZ(pOut, "null", 4);
      break;
    }
    case JSON_TRUE: {
      jsonAppendRawNZ(pOut, "true", 4);
      break;
    }
    case JSON_FALSE: {
      jsonAppendRawNZ(pOut, "false", 5);
      break;
    }
    case JSON_STRING: {
      assert( pNode->eU==1 );
      if( pNode->jnFlags & JNODE_RAW ){
        if( pNode->jnFlags & JNODE_LABEL ){
          jsonAppendChar(pOut, '"');
          jsonAppendRaw(pOut, pNode->u.zJContent, pNode->n);
          jsonAppendChar(pOut, '"');
        }else{
          jsonAppendString(pOut, pNode->u.zJContent, pNode->n);
        }
      }else if( pNode->jnFlags & JNODE_JSON5 ){
        jsonAppendNormalizedString(pOut, pNode->u.zJContent, pNode->n);
      }else{
        assert( pNode->n>0 );
        jsonAppendRawNZ(pOut, pNode->u.zJContent, pNode->n);
      }
      break;
    }
    case JSON_REAL: {
      assert( pNode->eU==1 );
      if( pNode->jnFlags & JNODE_JSON5 ){
        jsonAppendNormalizedReal(pOut, pNode->u.zJContent, pNode->n);
      }else{
        assert( pNode->n>0 );
        jsonAppendRawNZ(pOut, pNode->u.zJContent, pNode->n);
      }
      break;
    }
    case JSON_INT: {
      assert( pNode->eU==1 );
      if( pNode->jnFlags & JNODE_JSON5 ){
        jsonAppendNormalizedInt(pOut, pNode->u.zJContent, pNode->n);
      }else{
        assert( pNode->n>0 );
        jsonAppendRawNZ(pOut, pNode->u.zJContent, pNode->n);
      }
      break;
    }
    case JSON_ARRAY: {
      u32 j = 1;
      jsonAppendChar(pOut, '[');
      for(;;){
        while( j<=pNode->n ){
          if( (pNode[j].jnFlags & JNODE_REMOVE)==0 || pParse->useMod==0 ){
            jsonAppendSeparator(pOut);
            jsonRenderNode(pParse, &pNode[j], pOut);
          }
          j += jsonNodeSize(&pNode[j]);
        }
        if( (pNode->jnFlags & JNODE_APPEND)==0 ) break;
        if( pParse->useMod==0 ) break;
        assert( pNode->eU==2 );
        pNode = &pParse->aNode[pNode->u.iAppend];
        j = 1;
      }
      jsonAppendChar(pOut, ']');
      break;
    }
    case JSON_OBJECT: {
      u32 j = 1;
      jsonAppendChar(pOut, '{');
      for(;;){
        while( j<=pNode->n ){
          if( (pNode[j+1].jnFlags & JNODE_REMOVE)==0 || pParse->useMod==0 ){
            jsonAppendSeparator(pOut);
            jsonRenderNode(pParse, &pNode[j], pOut);
            jsonAppendChar(pOut, ':');
            jsonRenderNode(pParse, &pNode[j+1], pOut);
          }
          j += 1 + jsonNodeSize(&pNode[j+1]);
        }
        if( (pNode->jnFlags & JNODE_APPEND)==0 ) break;
        if( pParse->useMod==0 ) break;
        assert( pNode->eU==2 );
        pNode = &pParse->aNode[pNode->u.iAppend];
        j = 1;
      }
      jsonAppendChar(pOut, '}');
      break;
    }
  }
}

/*
** Return a JsonNode and all its descendants as a JSON string.
*/
static void jsonReturnJson(
  JsonParse *pParse,          /* The complete JSON */
  JsonNode *pNode,            /* Node to return */
  sqlite3_context *pCtx,      /* Return value for this function */
  int bGenerateAlt            /* Also store the rendered text in zAlt */
){
  JsonString s;
  if( pParse->oom ){
    sqlite3_result_error_nomem(pCtx);
    return;
  }
  if( pParse->nErr==0 ){
    jsonInit(&s, pCtx);
    jsonRenderNode(pParse, pNode, &s);
    if( bGenerateAlt && pParse->zAlt==0 && jsonForceRCStr(&s) ){
      pParse->zAlt = sqlite3RCStrRef(s.zBuf);
      pParse->nAlt = s.nUsed;
    }
    jsonResult(&s);
    sqlite3_result_subtype(pCtx, JSON_SUBTYPE);
  }
}

/*
** Translate a single byte of Hex into an integer.
** This routine only works if h really is a valid hexadecimal
** character:  0..9a..fA..F
*/
static u8 jsonHexToInt(int h){
  assert( (h>='0' && h<='9') ||  (h>='a' && h<='f') ||  (h>='A' && h<='F') );
#ifdef SQLITE_EBCDIC
  h += 9*(1&~(h>>4));
#else
  h += 9*(1&(h>>6));
#endif
  return (u8)(h & 0xf);
}

/*
** Convert a 4-byte hex string into an integer
*/
static u32 jsonHexToInt4(const char *z){
  u32 v;
  assert( sqlite3Isxdigit(z[0]) );
  assert( sqlite3Isxdigit(z[1]) );
  assert( sqlite3Isxdigit(z[2]) );
  assert( sqlite3Isxdigit(z[3]) );
  v = (jsonHexToInt(z[0])<<12)
    + (jsonHexToInt(z[1])<<8)
    + (jsonHexToInt(z[2])<<4)
    + jsonHexToInt(z[3]);
  return v;
}

/*
** Make the JsonNode the return value of the function.
*/
static void jsonReturn(
  JsonParse *pParse,          /* Complete JSON parse tree */
  JsonNode *pNode,            /* Node to return */
  sqlite3_context *pCtx       /* Return value for this function */
){
  switch( pNode->eType ){
    default: {
      assert( pNode->eType==JSON_NULL );
      sqlite3_result_null(pCtx);
      break;
    }
    case JSON_TRUE: {
      sqlite3_result_int(pCtx, 1);
      break;
    }
    case JSON_FALSE: {
      sqlite3_result_int(pCtx, 0);
      break;
    }
    case JSON_INT: {
      sqlite3_int64 i = 0;
      int rc;
      int bNeg = 0;
      const char *z;

      assert( pNode->eU==1 );
      z = pNode->u.zJContent;
      if( z[0]=='-' ){ z++; bNeg = 1; }
      else if( z[0]=='+' ){ z++; }
      rc = sqlite3DecOrHexToI64(z, &i);
      if( rc<=1 ){
        sqlite3_result_int64(pCtx, bNeg ? -i : i);
      }else if( rc==3 && bNeg ){
        sqlite3_result_int64(pCtx, SMALLEST_INT64);
      }else{
        goto to_double;
      }
      break;
    }
    case JSON_REAL: {
      double r;
      const char *z;
      assert( pNode->eU==1 );
    to_double:
      z = pNode->u.zJContent;
      sqlite3AtoF(z, &r, sqlite3Strlen30(z), SQLITE_UTF8);
      sqlite3_result_double(pCtx, r);
      break;
    }
    case JSON_STRING: {
      if( pNode->jnFlags & JNODE_RAW ){
        assert( pNode->eU==1 );
        sqlite3_result_text(pCtx, pNode->u.zJContent, pNode->n,
                            SQLITE_TRANSIENT);
      }else if( (pNode->jnFlags & JNODE_ESCAPE)==0 ){
        /* JSON formatted without any backslash-escapes */
        assert( pNode->eU==1 );
        sqlite3_result_text(pCtx, pNode->u.zJContent+1, pNode->n-2,
                            SQLITE_TRANSIENT);
      }else{
        /* Translate JSON formatted string into raw text */
        u32 i;
        u32 n = pNode->n;
        const char *z;
        char *zOut;
        u32 j;
        u32 nOut = n;
        assert( pNode->eU==1 );
        z = pNode->u.zJContent;
        zOut = sqlite3_malloc( nOut+1 );
        if( zOut==0 ){
          sqlite3_result_error_nomem(pCtx);
          break;
        }
        for(i=1, j=0; i<n-1; i++){
          char c = z[i];
          if( c=='\\' ){
            c = z[++i];
            if( c=='u' ){
              u32 v = jsonHexToInt4(z+i+1);
              i += 4;
              if( v==0 ) break;
              if( v<=0x7f ){
                zOut[j++] = (char)v;
              }else if( v<=0x7ff ){
                zOut[j++] = (char)(0xc0 | (v>>6));
                zOut[j++] = 0x80 | (v&0x3f);
              }else{
                u32 vlo;
                if( (v&0xfc00)==0xd800
                  && i<n-6
                  && z[i+1]=='\\'
                  && z[i+2]=='u'
                  && ((vlo = jsonHexToInt4(z+i+3))&0xfc00)==0xdc00
                ){
                  /* We have a surrogate pair */
                  v = ((v&0x3ff)<<10) + (vlo&0x3ff) + 0x10000;
                  i += 6;
                  zOut[j++] = 0xf0 | (v>>18);
                  zOut[j++] = 0x80 | ((v>>12)&0x3f);
                  zOut[j++] = 0x80 | ((v>>6)&0x3f);
                  zOut[j++] = 0x80 | (v&0x3f);
                }else{
                  zOut[j++] = 0xe0 | (v>>12);
                  zOut[j++] = 0x80 | ((v>>6)&0x3f);
                  zOut[j++] = 0x80 | (v&0x3f);
                }
              }
              continue;
            }else if( c=='b' ){
              c = '\b';
            }else if( c=='f' ){
              c = '\f';
            }else if( c=='n' ){
              c = '\n';
            }else if( c=='r' ){
              c = '\r';
            }else if( c=='t' ){
              c = '\t';
            }else if( c=='v' ){
              c = '\v';
            }else if( c=='\'' || c=='"' || c=='/' || c=='\\' ){
              /* pass through unchanged */
            }else if( c=='0' ){
              c = 0;
            }else if( c=='x' ){
              c = (jsonHexToInt(z[i+1])<<4) | jsonHexToInt(z[i+2]);
              i += 2;
            }else if( c=='\r' && z[i+1]=='\n' ){
              i++;
              continue;
            }else if( 0xe2==(u8)c ){
              assert( 0x80==(u8)z[i+1] );
              assert( 0xa8==(u8)z[i+2] || 0xa9==(u8)z[i+2] );
              i += 2;
              continue;
            }else{
              continue;
            }
          } /* end if( c=='\\' ) */
          zOut[j++] = c;
        } /* end for() */
        zOut[j] = 0;
        sqlite3_result_text(pCtx, zOut, j, sqlite3_free);
      }
      break;
    }
    case JSON_ARRAY:
    case JSON_OBJECT: {
      jsonReturnJson(pParse, pNode, pCtx, 0);
      break;
    }
  }
}

/* Forward reference */
static int jsonParseAddNode(JsonParse*,u32,u32,const char*);

/*
** A macro to hint to the compiler that a function should not be
** inlined.
*/
#if defined(__GNUC__)
#  define JSON_NOINLINE  __attribute__((noinline))
#elif defined(_MSC_VER) && _MSC_VER>=1310
#  define JSON_NOINLINE  __declspec(noinline)
#else
#  define JSON_NOINLINE
#endif


/*
** Add a single node to pParse->aNode after first expanding the
** size of the aNode array.  Return the index of the new node.
**
** If an OOM error occurs, set pParse->oom and return -1.
*/
static JSON_NOINLINE int jsonParseAddNodeExpand(
  JsonParse *pParse,        /* Append the node to this object */
  u32 eType,                /* Node type */
  u32 n,                    /* Content size or sub-node count */
  const char *zContent      /* Content */
){
  u32 nNew;
  JsonNode *pNew;
  assert( pParse->nNode>=pParse->nAlloc );
  if( pParse->oom ) return -1;
  nNew = pParse->nAlloc*2 + 10;
  pNew = sqlite3_realloc64(pParse->aNode, sizeof(JsonNode)*nNew);
  if( pNew==0 ){
    pParse->oom = 1;
    return -1;
  }
  pParse->nAlloc = sqlite3_msize(pNew)/sizeof(JsonNode);
  pParse->aNode = pNew;
  assert( pParse->nNode<pParse->nAlloc );
  return jsonParseAddNode(pParse, eType, n, zContent);
}

/*
** Create a new JsonNode instance based on the arguments and append that
** instance to the JsonParse.  Return the index in pParse->aNode[] of the
** new node, or -1 if a memory allocation fails.
*/
static int jsonParseAddNode(
  JsonParse *pParse,        /* Append the node to this object */
  u32 eType,                /* Node type */
  u32 n,                    /* Content size or sub-node count */
  const char *zContent      /* Content */
){
  JsonNode *p;
  assert( pParse->aNode!=0 || pParse->nNode>=pParse->nAlloc );
  if( pParse->nNode>=pParse->nAlloc ){
    return jsonParseAddNodeExpand(pParse, eType, n, zContent);
  }
  assert( pParse->aNode!=0 );
  p = &pParse->aNode[pParse->nNode];
  assert( p!=0 );
  p->eType = (u8)(eType & 0xff);
  p->jnFlags = (u8)(eType >> 8);
  VVA( p->eU = zContent ? 1 : 0 );
  p->n = n;
  p->u.zJContent = zContent;
  return pParse->nNode++;
}

/*
** Add an array of new nodes to the current pParse->aNode array.
** Return the index of the first node added.
**
** If an OOM error occurs, set pParse->oom.
*/
static void jsonParseAddNodeArray(
  JsonParse *pParse,        /* Append the node to this object */
  JsonNode *aNode,          /* Array of nodes to add */
  u32 nNode                 /* Number of elements in aNew */
){
  assert( aNode!=0 );
  assert( nNode>=1 );
  if( pParse->nNode + nNode > pParse->nAlloc ){
    u32 nNew = pParse->nNode + nNode;
    JsonNode *aNew = sqlite3_realloc64(pParse->aNode, nNew*sizeof(JsonNode));
    if( aNew==0 ){
      pParse->oom = 1;
      return;
    }
    pParse->nAlloc = sqlite3_msize(aNew)/sizeof(JsonNode);
    pParse->aNode = aNew;
  }
  memcpy(&pParse->aNode[pParse->nNode], aNode, nNode*sizeof(JsonNode));
  pParse->nNode += nNode;
}

/*
** Add a new JSON_SUBST node.  The node immediately following
** this new node will be the substitute content for iNode.
*/
static int jsonParseAddSubstNode(
  JsonParse *pParse,       /* Add the JSON_SUBST here */
  u32 iNode                /* References this node */
){
  int idx = jsonParseAddNode(pParse, JSON_SUBST, iNode, 0);
  if( pParse->oom ) return -1;
  pParse->aNode[iNode].jnFlags |= JNODE_REPLACE;
  pParse->aNode[idx].eU = 4;
  pParse->aNode[idx].u.iPrev = pParse->iSubst;
  pParse->iSubst = idx;
  pParse->hasMod = 1;
  pParse->useMod = 1;
  return idx;
}

/*
** Return true if z[] begins with 2 (or more) hexadecimal digits
*/
static int jsonIs2Hex(const char *z){
  return sqlite3Isxdigit(z[0]) && sqlite3Isxdigit(z[1]);
}

/*
** Return true if z[] begins with 4 (or more) hexadecimal digits
*/
static int jsonIs4Hex(const char *z){
  return jsonIs2Hex(z) && jsonIs2Hex(&z[2]);
}

/*
** Return the number of bytes of JSON5 whitespace at the beginning of
** the input string z[].
**
** JSON5 whitespace consists of any of the following characters:
**
**    Unicode  UTF-8         Name
**    U+0009   09            horizontal tab
**    U+000a   0a            line feed
**    U+000b   0b            vertical tab
**    U+000c   0c            form feed
**    U+000d   0d            carriage return
**    U+0020   20            space
**    U+00a0   c2 a0         non-breaking space
**    U+1680   e1 9a 80      ogham space mark
**    U+2000   e2 80 80      en quad
**    U+2001   e2 80 81      em quad
**    U+2002   e2 80 82      en space
**    U+2003   e2 80 83      em space
**    U+2004   e2 80 84      three-per-em space
**    U+2005   e2 80 85      four-per-em space
**    U+2006   e2 80 86      six-per-em space
**    U+2007   e2 80 87      figure space
**    U+2008   e2 80 88      punctuation space
**    U+2009   e2 80 89      thin space
**    U+200a   e2 80 8a      hair space
**    U+2028   e2 80 a8      line separator
**    U+2029   e2 80 a9      paragraph separator
**    U+202f   e2 80 af      narrow no-break space (NNBSP)
**    U+205f   e2 81 9f      medium mathematical space (MMSP)
**    U+3000   e3 80 80      ideographical space
**    U+FEFF   ef bb bf      byte order mark
**
** In addition, comments between '/', '*' and '*', '/' and
** from '/', '/' to end-of-line are also considered to be whitespace.
*/
static int json5Whitespace(const char *zIn){
  int n = 0;
  const u8 *z = (u8*)zIn;
  while( 1 /*exit by "goto whitespace_done"*/ ){
    switch( z[n] ){
      case 0x09:
      case 0x0a:
      case 0x0b:
      case 0x0c:
      case 0x0d:
      case 0x20: {
        n++;
        break;
      }
      case '/': {
        if( z[n+1]=='*' && z[n+2]!=0 ){
          int j;
          for(j=n+3; z[j]!='/' || z[j-1]!='*'; j++){
            if( z[j]==0 ) goto whitespace_done;
          }
          n = j+1;
          break;
        }else if( z[n+1]=='/' ){
          int j;
          char c;
          for(j=n+2; (c = z[j])!=0; j++){
            if( c=='\n' || c=='\r' ) break;
            if( 0xe2==(u8)c && 0x80==(u8)z[j+1]
             && (0xa8==(u8)z[j+2] || 0xa9==(u8)z[j+2])
            ){
              j += 2;
              break;
            }
          }
          n = j;
          if( z[n] ) n++;
          break;
        }
        goto whitespace_done;
      }
      case 0xc2: {
        if( z[n+1]==0xa0 ){
          n += 2;
          break;
        }
        goto whitespace_done;
      }
      case 0xe1: {
        if( z[n+1]==0x9a && z[n+2]==0x80 ){
          n += 3;
          break;
        }
        goto whitespace_done;
      }
      case 0xe2: {
        if( z[n+1]==0x80 ){
          u8 c = z[n+2];
          if( c<0x80 ) goto whitespace_done;
          if( c<=0x8a || c==0xa8 || c==0xa9 || c==0xaf ){
            n += 3;
            break;
          }
        }else if( z[n+1]==0x81 && z[n+2]==0x9f ){
          n += 3;
          break;
        }
        goto whitespace_done;
      }
      case 0xe3: {
        if( z[n+1]==0x80 && z[n+2]==0x80 ){
          n += 3;
          break;
        }
        goto whitespace_done;
      }
      case 0xef: {
        if( z[n+1]==0xbb && z[n+2]==0xbf ){
          n += 3;
          break;
        }
        goto whitespace_done;
      }
      default: {
        goto whitespace_done;
      }
    }
  }
  whitespace_done:
  return n;
}

/*
** Extra floating-point literals to allow in JSON.
*/
static const struct NanInfName {
  char c1;
  char c2;
  char n;
  char eType;
  char nRepl;
  char *zMatch;
  char *zRepl;
} aNanInfName[] = {
  { 'i', 'I', 3, JSON_REAL, 7, "inf", "9.0e999" },
  { 'i', 'I', 8, JSON_REAL, 7, "infinity", "9.0e999" },
  { 'n', 'N', 3, JSON_NULL, 4, "NaN", "null" },
  { 'q', 'Q', 4, JSON_NULL, 4, "QNaN", "null" },
  { 's', 'S', 4, JSON_NULL, 4, "SNaN", "null" },
};

/*
** Parse a single JSON value which begins at pParse->zJson[i].  Return the
** index of the first character past the end of the value parsed.
**
** Special return values:
**
**      0    End of input
**     -1    Syntax error
**     -2    '}' seen
**     -3    ']' seen
**     -4    ',' seen
**     -5    ':' seen
*/
static int jsonParseValue(JsonParse *pParse, u32 i){
  char c;
  u32 j;
  int iThis;
  int x;
  JsonNode *pNode;
  const char *z = pParse->zJson;
json_parse_restart:
  switch( (u8)z[i] ){
  case '{': {
    /* Parse object */
    iThis = jsonParseAddNode(pParse, JSON_OBJECT, 0, 0);
    if( iThis<0 ) return -1;
    if( ++pParse->iDepth > JSON_MAX_DEPTH ){
      pParse->iErr = i;
      return -1;
    }
    for(j=i+1;;j++){
      u32 nNode = pParse->nNode;
      x = jsonParseValue(pParse, j);
      if( x<=0 ){
        if( x==(-2) ){
          j = pParse->iErr;
          if( pParse->nNode!=(u32)iThis+1 ) pParse->hasNonstd = 1;
          break;
        }
        j += json5Whitespace(&z[j]);
        if( sqlite3JsonId1(z[j])
         || (z[j]=='\\' && z[j+1]=='u' && jsonIs4Hex(&z[j+2]))
        ){
          int k = j+1;
          while( (sqlite3JsonId2(z[k]) && json5Whitespace(&z[k])==0)
            || (z[k]=='\\' && z[k+1]=='u' && jsonIs4Hex(&z[k+2]))
          ){
            k++;
          }
          jsonParseAddNode(pParse, JSON_STRING | (JNODE_RAW<<8), k-j, &z[j]);
          pParse->hasNonstd = 1;
          x = k;
        }else{
          if( x!=-1 ) pParse->iErr = j;
          return -1;
        }
      }
      if( pParse->oom ) return -1;
      pNode = &pParse->aNode[nNode];
      if( pNode->eType!=JSON_STRING ){
        pParse->iErr = j;
        return -1;
      }
      pNode->jnFlags |= JNODE_LABEL;
      j = x;
      if( z[j]==':' ){
        j++;
      }else{
        if( fast_isspace(z[j]) ){
          do{ j++; }while( fast_isspace(z[j]) );
          if( z[j]==':' ){
            j++;
            goto parse_object_value;
          }
        }
        x = jsonParseValue(pParse, j);
        if( x!=(-5) ){
          if( x!=(-1) ) pParse->iErr = j;
          return -1;
        }
        j = pParse->iErr+1;
      }
    parse_object_value:
      x = jsonParseValue(pParse, j);
      if( x<=0 ){
        if( x!=(-1) ) pParse->iErr = j;
        return -1;
      }
      j = x;
      if( z[j]==',' ){
        continue;
      }else if( z[j]=='}' ){
        break;
      }else{
        if( fast_isspace(z[j]) ){
          do{ j++; }while( fast_isspace(z[j]) );
          if( z[j]==',' ){
            continue;
          }else if( z[j]=='}' ){
            break;
          }
        }
        x = jsonParseValue(pParse, j);
        if( x==(-4) ){
          j = pParse->iErr;
          continue;
        }
        if( x==(-2) ){
          j = pParse->iErr;
          break;
        }
      }
      pParse->iErr = j;
      return -1;
    }
    pParse->aNode[iThis].n = pParse->nNode - (u32)iThis - 1;
    pParse->iDepth--;
    return j+1;
  }
  case '[': {
    /* Parse array */
    iThis = jsonParseAddNode(pParse, JSON_ARRAY, 0, 0);
    if( iThis<0 ) return -1;
    if( ++pParse->iDepth > JSON_MAX_DEPTH ){
      pParse->iErr = i;
      return -1;
    }
    memset(&pParse->aNode[iThis].u, 0, sizeof(pParse->aNode[iThis].u));
    for(j=i+1;;j++){
      x = jsonParseValue(pParse, j);
      if( x<=0 ){
        if( x==(-3) ){
          j = pParse->iErr;
          if( pParse->nNode!=(u32)iThis+1 ) pParse->hasNonstd = 1;
          break;
        }
        if( x!=(-1) ) pParse->iErr = j;
        return -1;
      }
      j = x;
      if( z[j]==',' ){
        continue;
      }else if( z[j]==']' ){
        break;
      }else{
        if( fast_isspace(z[j]) ){
          do{ j++; }while( fast_isspace(z[j]) );
          if( z[j]==',' ){
            continue;
          }else if( z[j]==']' ){
            break;
          }
        }
        x = jsonParseValue(pParse, j);
        if( x==(-4) ){
          j = pParse->iErr;
          continue;
        }
        if( x==(-3) ){
          j = pParse->iErr;
          break;
        }
      }
      pParse->iErr = j;
      return -1;
    }
    pParse->aNode[iThis].n = pParse->nNode - (u32)iThis - 1;
    pParse->iDepth--;
    return j+1;
  }
  case '\'': {
    u8 jnFlags;
    char cDelim;
    pParse->hasNonstd = 1;
    jnFlags = JNODE_JSON5;
    goto parse_string;
  case '"':
    /* Parse string */
    jnFlags = 0;
  parse_string:
    cDelim = z[i];
    for(j=i+1; 1; j++){
      if( jsonIsOk[(unsigned char)z[j]] ) continue;
      c = z[j];
      if( c==cDelim ){
        break;
      }else if( c=='\\' ){
        c = z[++j];
        if( c=='"' || c=='\\' || c=='/' || c=='b' || c=='f'
           || c=='n' || c=='r' || c=='t'
           || (c=='u' && jsonIs4Hex(&z[j+1])) ){
          jnFlags |= JNODE_ESCAPE;
        }else if( c=='\'' || c=='0' || c=='v' || c=='\n'
           || (0xe2==(u8)c && 0x80==(u8)z[j+1]
                && (0xa8==(u8)z[j+2] || 0xa9==(u8)z[j+2]))
           || (c=='x' && jsonIs2Hex(&z[j+1])) ){
          jnFlags |= (JNODE_ESCAPE|JNODE_JSON5);
          pParse->hasNonstd = 1;
        }else if( c=='\r' ){
          if( z[j+1]=='\n' ) j++;
          jnFlags |= (JNODE_ESCAPE|JNODE_JSON5);
          pParse->hasNonstd = 1;
        }else{
          pParse->iErr = j;
          return -1;
        }
      }else if( c<=0x1f ){
        /* Control characters are not allowed in strings */
        pParse->iErr = j;
        return -1;
      }
    }
    jsonParseAddNode(pParse, JSON_STRING | (jnFlags<<8), j+1-i, &z[i]);
    return j+1;
  }
  case 't': {
    if( strncmp(z+i,"true",4)==0 && !sqlite3Isalnum(z[i+4]) ){
      jsonParseAddNode(pParse, JSON_TRUE, 0, 0);
      return i+4;
    }
    pParse->iErr = i;
    return -1;
  }
  case 'f': {
    if( strncmp(z+i,"false",5)==0 && !sqlite3Isalnum(z[i+5]) ){
      jsonParseAddNode(pParse, JSON_FALSE, 0, 0);
      return i+5;
    }
    pParse->iErr = i;
    return -1;
  }
  case '+': {
    u8 seenDP, seenE, jnFlags;
    pParse->hasNonstd = 1;
    jnFlags = JNODE_JSON5;
    goto parse_number;
  case '.':
    if( sqlite3Isdigit(z[i+1]) ){
      pParse->hasNonstd = 1;
      jnFlags = JNODE_JSON5;
      seenE = 0;
      seenDP = JSON_REAL;
      goto parse_number_2;
    }
    pParse->iErr = i;
    return -1;
  case '-':
  case '0':
  case '1':
  case '2':
  case '3':
  case '4':
  case '5':
  case '6':
  case '7':
  case '8':
  case '9':
    /* Parse number */
    jnFlags = 0;
  parse_number:
    seenDP = JSON_INT;
    seenE = 0;
    assert( '-' < '0' );
    assert( '+' < '0' );
    assert( '.' < '0' );
    c = z[i];

    if( c<='0' ){
      if( c=='0' ){
        if( (z[i+1]=='x' || z[i+1]=='X') && sqlite3Isxdigit(z[i+2]) ){
          assert( seenDP==JSON_INT );
          pParse->hasNonstd = 1;
          jnFlags |= JNODE_JSON5;
          for(j=i+3; sqlite3Isxdigit(z[j]); j++){}
          goto parse_number_finish;
        }else if( sqlite3Isdigit(z[i+1]) ){
          pParse->iErr = i+1;
          return -1;
        }
      }else{
        if( !sqlite3Isdigit(z[i+1]) ){
          /* JSON5 allows for "+Infinity" and "-Infinity" using exactly
          ** that case.  SQLite also allows these in any case and it allows
          ** "+inf" and "-inf". */
          if( (z[i+1]=='I' || z[i+1]=='i')
           && sqlite3StrNICmp(&z[i+1], "inf",3)==0
          ){
            pParse->hasNonstd = 1;
            if( z[i]=='-' ){
              jsonParseAddNode(pParse, JSON_REAL, 8, "-9.0e999");
            }else{
              jsonParseAddNode(pParse, JSON_REAL, 7, "9.0e999");
            }
            return i + (sqlite3StrNICmp(&z[i+4],"inity",5)==0 ? 9 : 4);
          }
          if( z[i+1]=='.' ){
            pParse->hasNonstd = 1;
            jnFlags |= JNODE_JSON5;
            goto parse_number_2;
          }
          pParse->iErr = i;
          return -1;
        }
        if( z[i+1]=='0' ){
          if( sqlite3Isdigit(z[i+2]) ){
            pParse->iErr = i+1;
            return -1;
          }else if( (z[i+2]=='x' || z[i+2]=='X') && sqlite3Isxdigit(z[i+3]) ){
            pParse->hasNonstd = 1;
            jnFlags |= JNODE_JSON5;
            for(j=i+4; sqlite3Isxdigit(z[j]); j++){}
            goto parse_number_finish;
          }
        }
      }
    }
  parse_number_2:
    for(j=i+1;; j++){
      c = z[j];
      if( sqlite3Isdigit(c) ) continue;
      if( c=='.' ){
        if( seenDP==JSON_REAL ){
          pParse->iErr = j;
          return -1;
        }
        seenDP = JSON_REAL;
        continue;
      }
      if( c=='e' || c=='E' ){
        if( z[j-1]<'0' ){
          if( ALWAYS(z[j-1]=='.') && ALWAYS(j-2>=i) && sqlite3Isdigit(z[j-2]) ){
            pParse->hasNonstd = 1;
            jnFlags |= JNODE_JSON5;
          }else{
            pParse->iErr = j;
            return -1;
          }
        }
        if( seenE ){
          pParse->iErr = j;
          return -1;
        }
        seenDP = JSON_REAL;
        seenE = 1;
        c = z[j+1];
        if( c=='+' || c=='-' ){
          j++;
          c = z[j+1];
        }
        if( c<'0' || c>'9' ){
          pParse->iErr = j;
          return -1;
        }
        continue;
      }
      break;
    }
    if( z[j-1]<'0' ){
      if( ALWAYS(z[j-1]=='.') && ALWAYS(j-2>=i) && sqlite3Isdigit(z[j-2]) ){
        pParse->hasNonstd = 1;
        jnFlags |= JNODE_JSON5;
      }else{
        pParse->iErr = j;
        return -1;
      }
    }
  parse_number_finish:
    jsonParseAddNode(pParse, seenDP | (jnFlags<<8), j - i, &z[i]);
    return j;
  }
  case '}': {
    pParse->iErr = i;
    return -2;  /* End of {...} */
  }
  case ']': {
    pParse->iErr = i;
    return -3;  /* End of [...] */
  }
  case ',': {
    pParse->iErr = i;
    return -4;  /* List separator */
  }
  case ':': {
    pParse->iErr = i;
    return -5;  /* Object label/value separator */
  }
  case 0: {
    return 0;   /* End of file */
  }
  case 0x09:
  case 0x0a:
  case 0x0d:
  case 0x20: {
    do{
      i++;
    }while( fast_isspace(z[i]) );
    goto json_parse_restart;
  }
  case 0x0b:
  case 0x0c:
  case '/':
  case 0xc2:
  case 0xe1:
  case 0xe2:
  case 0xe3:
  case 0xef: {
    j = json5Whitespace(&z[i]);
    if( j>0 ){
      i += j;
      pParse->hasNonstd = 1;
      goto json_parse_restart;
    }
    pParse->iErr = i;
    return -1;
  }
  case 'n': {
    if( strncmp(z+i,"null",4)==0 && !sqlite3Isalnum(z[i+4]) ){
      jsonParseAddNode(pParse, JSON_NULL, 0, 0);
      return i+4;
    }
    /* fall-through into the default case that checks for NaN */
  }
  default: {
    u32 k;
    int nn;
    c = z[i];
    for(k=0; k<sizeof(aNanInfName)/sizeof(aNanInfName[0]); k++){
      if( c!=aNanInfName[k].c1 && c!=aNanInfName[k].c2 ) continue;
      nn = aNanInfName[k].n;
      if( sqlite3StrNICmp(&z[i], aNanInfName[k].zMatch, nn)!=0 ){
        continue;
      }
      if( sqlite3Isalnum(z[i+nn]) ) continue;
      jsonParseAddNode(pParse, aNanInfName[k].eType,
          aNanInfName[k].nRepl, aNanInfName[k].zRepl);
      pParse->hasNonstd = 1;
      return i + nn;
    }
    pParse->iErr = i;
    return -1;  /* Syntax error */
  }
  } /* End switch(z[i]) */
}

/*
** Parse a complete JSON string.  Return 0 on success or non-zero if there
** are any errors.  If an error occurs, free all memory held by pParse,
** but not pParse itself.
**
** pParse must be initialized to an empty parse object prior to calling
** this routine.
*/
static int jsonParse(
  JsonParse *pParse,           /* Initialize and fill this JsonParse object */
  sqlite3_context *pCtx        /* Report errors here */
){
  int i;
  const char *zJson = pParse->zJson;
  i = jsonParseValue(pParse, 0);
  if( pParse->oom ) i = -1;
  if( i>0 ){
    assert( pParse->iDepth==0 );
    while( fast_isspace(zJson[i]) ) i++;
    if( zJson[i] ){
      i += json5Whitespace(&zJson[i]);
      if( zJson[i] ){
        jsonParseReset(pParse);
        return 1;
      }
      pParse->hasNonstd = 1;
    }
  }
  if( i<=0 ){
    if( pCtx!=0 ){
      if( pParse->oom ){
        sqlite3_result_error_nomem(pCtx);
      }else{
        sqlite3_result_error(pCtx, "malformed JSON", -1);
      }
    }
    jsonParseReset(pParse);
    return 1;
  }
  return 0;
}


/* Mark node i of pParse as being a child of iParent.  Call recursively
** to fill in all the descendants of node i.
*/
static void jsonParseFillInParentage(JsonParse *pParse, u32 i, u32 iParent){
  JsonNode *pNode = &pParse->aNode[i];
  u32 j;
  pParse->aUp[i] = iParent;
  switch( pNode->eType ){
    case JSON_ARRAY: {
      for(j=1; j<=pNode->n; j += jsonNodeSize(pNode+j)){
        jsonParseFillInParentage(pParse, i+j, i);
      }
      break;
    }
    case JSON_OBJECT: {
      for(j=1; j<=pNode->n; j += jsonNodeSize(pNode+j+1)+1){
        pParse->aUp[i+j] = i;
        jsonParseFillInParentage(pParse, i+j+1, i);
      }
      break;
    }
    default: {
      break;
    }
  }
}

/*
** Compute the parentage of all nodes in a completed parse.
*/
static int jsonParseFindParents(JsonParse *pParse){
  u32 *aUp;
  assert( pParse->aUp==0 );
  aUp = pParse->aUp = sqlite3_malloc64( sizeof(u32)*pParse->nNode );
  if( aUp==0 ){
    pParse->oom = 1;
    return SQLITE_NOMEM;
  }
  jsonParseFillInParentage(pParse, 0, 0);
  return SQLITE_OK;
}

/*
** Magic number used for the JSON parse cache in sqlite3_get_auxdata()
*/
#define JSON_CACHE_ID  (-429938)  /* First cache entry */
#define JSON_CACHE_SZ  4          /* Max number of cache entries */

/*
** Obtain a complete parse of the JSON found in the pJson argument
**
** Use the sqlite3_get_auxdata() cache to find a preexisting parse
** if it is available.  If the cache is not available or if it
** is no longer valid, parse the JSON again and return the new parse.
** Also register the new parse so that it will be available for
** future sqlite3_get_auxdata() calls.
**
** If an error occurs and pErrCtx!=0 then report the error on pErrCtx
** and return NULL.
**
** The returned pointer (if it is not NULL) is owned by the cache in
** most cases, not the caller.  The caller does NOT need to invoke
** jsonParseFree(), in most cases.
**
** Except, if an error occurs and pErrCtx==0 then return the JsonParse
** object with JsonParse.nErr non-zero and the caller will own the JsonParse
** object.  In that case, it will be the responsibility of the caller to
** invoke jsonParseFree().  To summarize:
**
**   pErrCtx!=0 || p->nErr==0      ==>   Return value p is owned by the
**                                       cache.  Call does not need to
**                                       free it.
**
**   pErrCtx==0 && p->nErr!=0      ==>   Return value is owned by the caller
**                                       and so the caller must free it.
*/
static JsonParse *jsonParseCached(
  sqlite3_context *pCtx,         /* Context to use for cache search */
  sqlite3_value *pJson,          /* Function param containing JSON text */
  sqlite3_context *pErrCtx,      /* Write parse errors here if not NULL */
  int bUnedited                  /* No prior edits allowed */
){
  char *zJson = (char*)sqlite3_value_text(pJson);
  int nJson = sqlite3_value_bytes(pJson);
  JsonParse *p;
  JsonParse *pMatch = 0;
  int iKey;
  int iMinKey = 0;
  u32 iMinHold = 0xffffffff;
  u32 iMaxHold = 0;
  int bJsonRCStr;

  if( zJson==0 ) return 0;
  for(iKey=0; iKey<JSON_CACHE_SZ; iKey++){
    p = (JsonParse*)sqlite3_get_auxdata(pCtx, JSON_CACHE_ID+iKey);
    if( p==0 ){
      iMinKey = iKey;
      break;
    }
    if( pMatch==0
     && p->nJson==nJson
     && (p->hasMod==0 || bUnedited==0)
     && (p->zJson==zJson || memcmp(p->zJson,zJson,nJson)==0)
    ){
      p->nErr = 0;
      p->useMod = 0;
      pMatch = p;
    }else
    if( pMatch==0
     && p->zAlt!=0
     && bUnedited==0
     && p->nAlt==nJson
     && memcmp(p->zAlt, zJson, nJson)==0
    ){
      p->nErr = 0;
      p->useMod = 1;
      pMatch = p;
    }else if( p->iHold<iMinHold ){
      iMinHold = p->iHold;
      iMinKey = iKey;
    }
    if( p->iHold>iMaxHold ){
      iMaxHold = p->iHold;
    }
  }
  if( pMatch ){
    /* The input JSON text was found in the cache.  Use the preexisting
    ** parse of this JSON */
    pMatch->nErr = 0;
    pMatch->iHold = iMaxHold+1;
    assert( pMatch->nJPRef>0 ); /* pMatch is owned by the cache */
    return pMatch;
  }

  /* The input JSON was not found anywhere in the cache.  We will need
  ** to parse it ourselves and generate a new JsonParse object.
  */
  bJsonRCStr = sqlite3ValueIsOfClass(pJson,(void(*)(void*))sqlite3RCStrUnref);
  p = sqlite3_malloc64( sizeof(*p) + (bJsonRCStr ? 0 : nJson+1) );
  if( p==0 ){
    sqlite3_result_error_nomem(pCtx);
    return 0;
  }
  memset(p, 0, sizeof(*p));
  if( bJsonRCStr ){
    p->zJson = sqlite3RCStrRef(zJson);
    p->bJsonIsRCStr = 1;
  }else{
    p->zJson = (char*)&p[1];
    memcpy(p->zJson, zJson, nJson+1);
  }
  p->nJPRef = 1;
  if( jsonParse(p, pErrCtx) ){
    if( pErrCtx==0 ){
      p->nErr = 1;
      assert( p->nJPRef==1 ); /* Caller will own the new JsonParse object p */
      return p;
    }
    jsonParseFree(p);
    return 0;
  }
  p->nJson = nJson;
  p->iHold = iMaxHold+1;
  /* Transfer ownership of the new JsonParse to the cache */
  sqlite3_set_auxdata(pCtx, JSON_CACHE_ID+iMinKey, p,
                      (void(*)(void*))jsonParseFree);
  return (JsonParse*)sqlite3_get_auxdata(pCtx, JSON_CACHE_ID+iMinKey);
}

/*
** Compare the OBJECT label at pNode against zKey,nKey.  Return true on
** a match.
*/
static int jsonLabelCompare(const JsonNode *pNode, const char *zKey, u32 nKey){
  assert( pNode->eU==1 );
  if( pNode->jnFlags & JNODE_RAW ){
    if( pNode->n!=nKey ) return 0;
    return strncmp(pNode->u.zJContent, zKey, nKey)==0;
  }else{
    if( pNode->n!=nKey+2 ) return 0;
    return strncmp(pNode->u.zJContent+1, zKey, nKey)==0;
  }
}
static int jsonSameLabel(const JsonNode *p1, const JsonNode *p2){
  if( p1->jnFlags & JNODE_RAW ){
    return jsonLabelCompare(p2, p1->u.zJContent, p1->n);
  }else if( p2->jnFlags & JNODE_RAW ){
    return jsonLabelCompare(p1, p2->u.zJContent, p2->n);
  }else{
    return p1->n==p2->n && strncmp(p1->u.zJContent,p2->u.zJContent,p1->n)==0;
  }
}

/* forward declaration */
static JsonNode *jsonLookupAppend(JsonParse*,const char*,int*,const char**);

/*
** Search along zPath to find the node specified.  Return a pointer
** to that node, or NULL if zPath is malformed or if there is no such
** node.
**
** If pApnd!=0, then try to append new nodes to complete zPath if it is
** possible to do so and if no existing node corresponds to zPath.  If
** new nodes are appended *pApnd is set to 1.
*/
static JsonNode *jsonLookupStep(
  JsonParse *pParse,      /* The JSON to search */
  u32 iRoot,              /* Begin the search at this node */
  const char *zPath,      /* The path to search */
  int *pApnd,             /* Append nodes to complete path if not NULL */
  const char **pzErr      /* Make *pzErr point to any syntax error in zPath */
){
  u32 i, j, nKey;
  const char *zKey;
  JsonNode *pRoot;
  if( pParse->oom ) return 0;
  pRoot = &pParse->aNode[iRoot];
  if( pRoot->jnFlags & (JNODE_REPLACE|JNODE_REMOVE) && pParse->useMod ){
    while( (pRoot->jnFlags & JNODE_REPLACE)!=0 ){
      u32 idx = (u32)(pRoot - pParse->aNode);
      i = pParse->iSubst;
      while( 1 /*exit-by-break*/ ){
        assert( i<pParse->nNode );
        assert( pParse->aNode[i].eType==JSON_SUBST );
        assert( pParse->aNode[i].eU==4 );
        assert( pParse->aNode[i].u.iPrev<i );
        if( pParse->aNode[i].n==idx ){
          pRoot = &pParse->aNode[i+1];
          iRoot = i+1;
          break;
        }
        i = pParse->aNode[i].u.iPrev;
      }
    }
    if( pRoot->jnFlags & JNODE_REMOVE ){
      return 0;
    }
  }
  if( zPath[0]==0 ) return pRoot;
  if( zPath[0]=='.' ){
    if( pRoot->eType!=JSON_OBJECT ) return 0;
    zPath++;
    if( zPath[0]=='"' ){
      zKey = zPath + 1;
      for(i=1; zPath[i] && zPath[i]!='"'; i++){}
      nKey = i-1;
      if( zPath[i] ){
        i++;
      }else{
        *pzErr = zPath;
        return 0;
      }
      testcase( nKey==0 );
    }else{
      zKey = zPath;
      for(i=0; zPath[i] && zPath[i]!='.' && zPath[i]!='['; i++){}
      nKey = i;
      if( nKey==0 ){
        *pzErr = zPath;
        return 0;
      }
    }
    j = 1;
    for(;;){
      while( j<=pRoot->n ){
        if( jsonLabelCompare(pRoot+j, zKey, nKey) ){
          return jsonLookupStep(pParse, iRoot+j+1, &zPath[i], pApnd, pzErr);
        }
        j++;
        j += jsonNodeSize(&pRoot[j]);
      }
      if( (pRoot->jnFlags & JNODE_APPEND)==0 ) break;
      if( pParse->useMod==0 ) break;
      assert( pRoot->eU==2 );
      iRoot = pRoot->u.iAppend;
      pRoot = &pParse->aNode[iRoot];
      j = 1;
    }
    if( pApnd ){
      u32 iStart, iLabel;
      JsonNode *pNode;
      assert( pParse->useMod );
      iStart = jsonParseAddNode(pParse, JSON_OBJECT, 2, 0);
      iLabel = jsonParseAddNode(pParse, JSON_STRING, nKey, zKey);
      zPath += i;
      pNode = jsonLookupAppend(pParse, zPath, pApnd, pzErr);
      if( pParse->oom ) return 0;
      if( pNode ){
        pRoot = &pParse->aNode[iRoot];
        assert( pRoot->eU==0 );
        pRoot->u.iAppend = iStart;
        pRoot->jnFlags |= JNODE_APPEND;
        VVA( pRoot->eU = 2 );
        pParse->aNode[iLabel].jnFlags |= JNODE_RAW;
      }
      return pNode;
    }
  }else if( zPath[0]=='[' ){
    i = 0;
    j = 1;
    while( sqlite3Isdigit(zPath[j]) ){
      i = i*10 + zPath[j] - '0';
      j++;
    }
    if( j<2 || zPath[j]!=']' ){
      if( zPath[1]=='#' ){
        JsonNode *pBase = pRoot;
        int iBase = iRoot;
        if( pRoot->eType!=JSON_ARRAY ) return 0;
        for(;;){
          while( j<=pBase->n ){
            if( (pBase[j].jnFlags & JNODE_REMOVE)==0 || pParse->useMod==0 ) i++;
            j += jsonNodeSize(&pBase[j]);
          }
          if( (pBase->jnFlags & JNODE_APPEND)==0 ) break;
          if( pParse->useMod==0 ) break;
          assert( pBase->eU==2 );
          iBase = pBase->u.iAppend;
          pBase = &pParse->aNode[iBase];
          j = 1;
        }
        j = 2;
        if( zPath[2]=='-' && sqlite3Isdigit(zPath[3]) ){
          unsigned int x = 0;
          j = 3;
          do{
            x = x*10 + zPath[j] - '0';
            j++;
          }while( sqlite3Isdigit(zPath[j]) );
          if( x>i ) return 0;
          i -= x;
        }
        if( zPath[j]!=']' ){
          *pzErr = zPath;
          return 0;
        }
      }else{
        *pzErr = zPath;
        return 0;
      }
    }
    if( pRoot->eType!=JSON_ARRAY ) return 0;
    zPath += j + 1;
    j = 1;
    for(;;){
      while( j<=pRoot->n
         && (i>0 || ((pRoot[j].jnFlags & JNODE_REMOVE)!=0 && pParse->useMod))
      ){
        if( (pRoot[j].jnFlags & JNODE_REMOVE)==0 || pParse->useMod==0 ) i--;
        j += jsonNodeSize(&pRoot[j]);
      }
      if( (pRoot->jnFlags & JNODE_APPEND)==0 ) break;
      if( pParse->useMod==0 ) break;
      assert( pRoot->eU==2 );
      iRoot = pRoot->u.iAppend;
      pRoot = &pParse->aNode[iRoot];
      j = 1;
    }
    if( j<=pRoot->n ){
      return jsonLookupStep(pParse, iRoot+j, zPath, pApnd, pzErr);
    }
    if( i==0 && pApnd ){
      u32 iStart;
      JsonNode *pNode;
      assert( pParse->useMod );
      iStart = jsonParseAddNode(pParse, JSON_ARRAY, 1, 0);
      pNode = jsonLookupAppend(pParse, zPath, pApnd, pzErr);
      if( pParse->oom ) return 0;
      if( pNode ){
        pRoot = &pParse->aNode[iRoot];
        assert( pRoot->eU==0 );
        pRoot->u.iAppend = iStart;
        pRoot->jnFlags |= JNODE_APPEND;
        VVA( pRoot->eU = 2 );
      }
      return pNode;
    }
  }else{
    *pzErr = zPath;
  }
  return 0;
}

/*
** Append content to pParse that will complete zPath.  Return a pointer
** to the inserted node, or return NULL if the append fails.
*/
static JsonNode *jsonLookupAppend(
  JsonParse *pParse,     /* Append content to the JSON parse */
  const char *zPath,     /* Description of content to append */
  int *pApnd,            /* Set this flag to 1 */
  const char **pzErr     /* Make this point to any syntax error */
){
  *pApnd = 1;
  if( zPath[0]==0 ){
    jsonParseAddNode(pParse, JSON_NULL, 0, 0);
    return pParse->oom ? 0 : &pParse->aNode[pParse->nNode-1];
  }
  if( zPath[0]=='.' ){
    jsonParseAddNode(pParse, JSON_OBJECT, 0, 0);
  }else if( strncmp(zPath,"[0]",3)==0 ){
    jsonParseAddNode(pParse, JSON_ARRAY, 0, 0);
  }else{
    return 0;
  }
  if( pParse->oom ) return 0;
  return jsonLookupStep(pParse, pParse->nNode-1, zPath, pApnd, pzErr);
}

/*
** Return the text of a syntax error message on a JSON path.  Space is
** obtained from sqlite3_malloc().
*/
static char *jsonPathSyntaxError(const char *zErr){
  return sqlite3_mprintf("JSON path error near '%q'", zErr);
}

/*
** Do a node lookup using zPath.  Return a pointer to the node on success.
** Return NULL if not found or if there is an error.
**
** On an error, write an error message into pCtx and increment the
** pParse->nErr counter.
**
** If pApnd!=NULL then try to append missing nodes and set *pApnd = 1 if
** nodes are appended.
*/
static JsonNode *jsonLookup(
  JsonParse *pParse,      /* The JSON to search */
  const char *zPath,      /* The path to search */
  int *pApnd,             /* Append nodes to complete path if not NULL */
  sqlite3_context *pCtx   /* Report errors here, if not NULL */
){
  const char *zErr = 0;
  JsonNode *pNode = 0;
  char *zMsg;

  if( zPath==0 ) return 0;
  if( zPath[0]!='$' ){
    zErr = zPath;
    goto lookup_err;
  }
  zPath++;
  pNode = jsonLookupStep(pParse, 0, zPath, pApnd, &zErr);
  if( zErr==0 ) return pNode;

lookup_err:
  pParse->nErr++;
  assert( zErr!=0 && pCtx!=0 );
  zMsg = jsonPathSyntaxError(zErr);
  if( zMsg ){
    sqlite3_result_error(pCtx, zMsg, -1);
    sqlite3_free(zMsg);
  }else{
    sqlite3_result_error_nomem(pCtx);
  }
  return 0;
}


/*
** Report the wrong number of arguments for json_insert(), json_replace()
** or json_set().
*/
static void jsonWrongNumArgs(
  sqlite3_context *pCtx,
  const char *zFuncName
){
  char *zMsg = sqlite3_mprintf("json_%s() needs an odd number of arguments",
                               zFuncName);
  sqlite3_result_error(pCtx, zMsg, -1);
  sqlite3_free(zMsg);
}

/*
** Mark all NULL entries in the Object passed in as JNODE_REMOVE.
*/
static void jsonRemoveAllNulls(JsonNode *pNode){
  int i, n;
  assert( pNode->eType==JSON_OBJECT );
  n = pNode->n;
  for(i=2; i<=n; i += jsonNodeSize(&pNode[i])+1){
    switch( pNode[i].eType ){
      case JSON_NULL:
        pNode[i].jnFlags |= JNODE_REMOVE;
        break;
      case JSON_OBJECT:
        jsonRemoveAllNulls(&pNode[i]);
        break;
    }
  }
}


/****************************************************************************
** SQL functions used for testing and debugging
****************************************************************************/

#if SQLITE_DEBUG
/*
** Print N node entries.
*/
static void jsonDebugPrintNodeEntries(
  JsonNode *aNode,  /* First node entry to print */
  int N             /* Number of node entries to print */
){
  int i;
  for(i=0; i<N; i++){
    const char *zType;
    if( aNode[i].jnFlags & JNODE_LABEL ){
      zType = "label";
    }else{
      zType = jsonType[aNode[i].eType];
    }
    printf("node %4u: %-7s n=%-5d", i, zType, aNode[i].n);
    if( (aNode[i].jnFlags & ~JNODE_LABEL)!=0 ){
      u8 f = aNode[i].jnFlags;
      if( f & JNODE_RAW )     printf(" RAW");
      if( f & JNODE_ESCAPE )  printf(" ESCAPE");
      if( f & JNODE_REMOVE )  printf(" REMOVE");
      if( f & JNODE_REPLACE ) printf(" REPLACE");
      if( f & JNODE_APPEND )  printf(" APPEND");
      if( f & JNODE_JSON5 )   printf(" JSON5");
    }
    switch( aNode[i].eU ){
      case 1:  printf(" zJContent=[%.*s]\n",
                      aNode[i].n, aNode[i].u.zJContent);           break;
      case 2:  printf(" iAppend=%u\n", aNode[i].u.iAppend);        break;
      case 3:  printf(" iKey=%u\n", aNode[i].u.iKey);              break;
      case 4:  printf(" iPrev=%u\n", aNode[i].u.iPrev);            break;
      default: printf("\n");
    }
  }
}
#endif /* SQLITE_DEBUG */


#if 0  /* 1 for debugging.  0 normally.  Requires -DSQLITE_DEBUG too */
static void jsonDebugPrintParse(JsonParse *p){
  jsonDebugPrintNodeEntries(p->aNode, p->nNode);
}
static void jsonDebugPrintNode(JsonNode *pNode){
  jsonDebugPrintNodeEntries(pNode, jsonNodeSize(pNode));
}
#else
   /* The usual case */
# define jsonDebugPrintNode(X)
# define jsonDebugPrintParse(X)
#endif

#ifdef SQLITE_DEBUG
/*
** SQL function:   json_parse(JSON)
**
** Parse JSON using jsonParseCached().  Then print a dump of that
** parse on standard output.  Return the mimified JSON result, just
** like the json() function.
*/
static void jsonParseFunc(
  sqlite3_context *ctx,
  int argc,
  sqlite3_value **argv
){
  JsonParse *p;        /* The parse */

  assert( argc==1 );
  p = jsonParseCached(ctx, argv[0], ctx, 0);
  if( p==0 ) return;
  printf("nNode     = %u\n", p->nNode);
  printf("nAlloc    = %u\n", p->nAlloc);
  printf("nJson     = %d\n", p->nJson);
  printf("nAlt      = %d\n", p->nAlt);
  printf("nErr      = %u\n", p->nErr);
  printf("oom       = %u\n", p->oom);
  printf("hasNonstd = %u\n", p->hasNonstd);
  printf("useMod    = %u\n", p->useMod);
  printf("hasMod    = %u\n", p->hasMod);
  printf("nJPRef    = %u\n", p->nJPRef);
  printf("iSubst    = %u\n", p->iSubst);
  printf("iHold     = %u\n", p->iHold);
  jsonDebugPrintNodeEntries(p->aNode, p->nNode);
  jsonReturnJson(p, p->aNode, ctx, 1);
}

/*
** The json_test1(JSON) function return true (1) if the input is JSON
** text generated by another json function.  It returns (0) if the input
** is not known to be JSON.
*/
static void jsonTest1Func(
  sqlite3_context *ctx,
  int argc,
  sqlite3_value **argv
){
  UNUSED_PARAMETER(argc);
  sqlite3_result_int(ctx, sqlite3_value_subtype(argv[0])==JSON_SUBTYPE);
}
#endif /* SQLITE_DEBUG */

/****************************************************************************
** Scalar SQL function implementations
****************************************************************************/

/*
** Implementation of the json_QUOTE(VALUE) function.  Return a JSON value
** corresponding to the SQL value input.  Mostly this means putting
** double-quotes around strings and returning the unquoted string "null"
** when given a NULL input.
*/
static void jsonQuoteFunc(
  sqlite3_context *ctx,
  int argc,
  sqlite3_value **argv
){
  JsonString jx;
  UNUSED_PARAMETER(argc);

  jsonInit(&jx, ctx);
  jsonAppendValue(&jx, argv[0]);
  jsonResult(&jx);
  sqlite3_result_subtype(ctx, JSON_SUBTYPE);
}

/*
** Implementation of the json_array(VALUE,...) function.  Return a JSON
** array that contains all values given in arguments.  Or if any argument
** is a BLOB, throw an error.
*/
static void jsonArrayFunc(
  sqlite3_context *ctx,
  int argc,
  sqlite3_value **argv
){
  int i;
  JsonString jx;

  jsonInit(&jx, ctx);
  jsonAppendChar(&jx, '[');
  for(i=0; i<argc; i++){
    jsonAppendSeparator(&jx);
    jsonAppendValue(&jx, argv[i]);
  }
  jsonAppendChar(&jx, ']');
  jsonResult(&jx);
  sqlite3_result_subtype(ctx, JSON_SUBTYPE);
}


/*
** json_array_length(JSON)
** json_array_length(JSON, PATH)
**
** Return the number of elements in the top-level JSON array.
** Return 0 if the input is not a well-formed JSON array.
*/
static void jsonArrayLengthFunc(
  sqlite3_context *ctx,
  int argc,
  sqlite3_value **argv
){
  JsonParse *p;          /* The parse */
  sqlite3_int64 n = 0;
  u32 i;
  JsonNode *pNode;

  p = jsonParseCached(ctx, argv[0], ctx, 0);
  if( p==0 ) return;
  assert( p->nNode );
  if( argc==2 ){
    const char *zPath = (const char*)sqlite3_value_text(argv[1]);
    pNode = jsonLookup(p, zPath, 0, ctx);
  }else{
    pNode = p->aNode;
  }
  if( pNode==0 ){
    return;
  }
  if( pNode->eType==JSON_ARRAY ){
    while( 1 /*exit-by-break*/ ){
      for(i=1; i<=pNode->n; n++){
        i += jsonNodeSize(&pNode[i]);
      }
      if( (pNode->jnFlags & JNODE_APPEND)==0 ) break;
      if( p->useMod==0 ) break;
      assert( pNode->eU==2 );
      pNode = &p->aNode[pNode->u.iAppend];
    }
  }
  sqlite3_result_int64(ctx, n);
}

/*
** Bit values for the flags passed into jsonExtractFunc() or
** jsonSetFunc() via the user-data value.
*/
#define JSON_JSON      0x01        /* Result is always JSON */
#define JSON_SQL       0x02        /* Result is always SQL */
#define JSON_ABPATH    0x03        /* Allow abbreviated JSON path specs */
#define JSON_ISSET     0x04        /* json_set(), not json_insert() */

/*
** json_extract(JSON, PATH, ...)
** "->"(JSON,PATH)
** "->>"(JSON,PATH)
**
** Return the element described by PATH.  Return NULL if that PATH element
** is not found.
**
** If JSON_JSON is set or if more that one PATH argument is supplied then
** always return a JSON representation of the result.  If JSON_SQL is set,
** then always return an SQL representation of the result.  If neither flag
** is present and argc==2, then return JSON for objects and arrays and SQL
** for all other values.
**
** When multiple PATH arguments are supplied, the result is a JSON array
** containing the result of each PATH.
**
** Abbreviated JSON path expressions are allows if JSON_ABPATH, for
** compatibility with PG.
*/
static void jsonExtractFunc(
  sqlite3_context *ctx,
  int argc,
  sqlite3_value **argv
){
  JsonParse *p;          /* The parse */
  JsonNode *pNode;
  const char *zPath;
  int flags = SQLITE_PTR_TO_INT(sqlite3_user_data(ctx));
  JsonString jx;

  if( argc<2 ) return;
  p = jsonParseCached(ctx, argv[0], ctx, 0);
  if( p==0 ) return;
  if( argc==2 ){
    /* With a single PATH argument */
    zPath = (const char*)sqlite3_value_text(argv[1]);
    if( zPath==0 ) return;
    if( flags & JSON_ABPATH ){
      if( zPath[0]!='$' || (zPath[1]!='.' && zPath[1]!='[' && zPath[1]!=0) ){
        /* The -> and ->> operators accept abbreviated PATH arguments.  This
        ** is mostly for compatibility with PostgreSQL, but also for
        ** convenience.
        **
        **     NUMBER   ==>  $[NUMBER]     // PG compatible
        **     LABEL    ==>  $.LABEL       // PG compatible
        **     [NUMBER] ==>  $[NUMBER]     // Not PG.  Purely for convenience
        */
        jsonInit(&jx, ctx);
        if( sqlite3Isdigit(zPath[0]) ){
          jsonAppendRawNZ(&jx, "$[", 2);
          jsonAppendRaw(&jx, zPath, (int)strlen(zPath));
          jsonAppendRawNZ(&jx, "]", 2);
        }else{
          jsonAppendRawNZ(&jx, "$.", 1 + (zPath[0]!='['));
          jsonAppendRaw(&jx, zPath, (int)strlen(zPath));
          jsonAppendChar(&jx, 0);
        }
        pNode = jx.bErr ? 0 : jsonLookup(p, jx.zBuf, 0, ctx);
        jsonReset(&jx);
      }else{
        pNode = jsonLookup(p, zPath, 0, ctx);
      }
      if( pNode ){
        if( flags & JSON_JSON ){
          jsonReturnJson(p, pNode, ctx, 0);
        }else{
          jsonReturn(p, pNode, ctx);
          sqlite3_result_subtype(ctx, 0);
        }
      }
    }else{
      pNode = jsonLookup(p, zPath, 0, ctx);
      if( p->nErr==0 && pNode ) jsonReturn(p, pNode, ctx);
    }
  }else{
    /* Two or more PATH arguments results in a JSON array with each
    ** element of the array being the value selected by one of the PATHs */
    int i;
    jsonInit(&jx, ctx);
    jsonAppendChar(&jx, '[');
    for(i=1; i<argc; i++){
      zPath = (const char*)sqlite3_value_text(argv[i]);
      pNode = jsonLookup(p, zPath, 0, ctx);
      if( p->nErr ) break;
      jsonAppendSeparator(&jx);
      if( pNode ){
        jsonRenderNode(p, pNode, &jx);
      }else{
        jsonAppendRawNZ(&jx, "null", 4);
      }
    }
    if( i==argc ){
      jsonAppendChar(&jx, ']');
      jsonResult(&jx);
      sqlite3_result_subtype(ctx, JSON_SUBTYPE);
    }
    jsonReset(&jx);
  }
}

/* This is the RFC 7396 MergePatch algorithm.
*/
static JsonNode *jsonMergePatch(
  JsonParse *pParse,   /* The JSON parser that contains the TARGET */
  u32 iTarget,         /* Node of the TARGET in pParse */
  JsonNode *pPatch     /* The PATCH */
){
  u32 i, j;
  u32 iRoot;
  JsonNode *pTarget;
  if( pPatch->eType!=JSON_OBJECT ){
    return pPatch;
  }
  assert( iTarget<pParse->nNode );
  pTarget = &pParse->aNode[iTarget];
  assert( (pPatch->jnFlags & JNODE_APPEND)==0 );
  if( pTarget->eType!=JSON_OBJECT ){
    jsonRemoveAllNulls(pPatch);
    return pPatch;
  }
  iRoot = iTarget;
  for(i=1; i<pPatch->n; i += jsonNodeSize(&pPatch[i+1])+1){
    u32 nKey;
    const char *zKey;
    assert( pPatch[i].eType==JSON_STRING );
    assert( pPatch[i].jnFlags & JNODE_LABEL );
    assert( pPatch[i].eU==1 );
    nKey = pPatch[i].n;
    zKey = pPatch[i].u.zJContent;
    for(j=1; j<pTarget->n; j += jsonNodeSize(&pTarget[j+1])+1 ){
      assert( pTarget[j].eType==JSON_STRING );
      assert( pTarget[j].jnFlags & JNODE_LABEL );
      if( jsonSameLabel(&pPatch[i], &pTarget[j]) ){
        if( pTarget[j+1].jnFlags & (JNODE_REMOVE|JNODE_REPLACE) ) break;
        if( pPatch[i+1].eType==JSON_NULL ){
          pTarget[j+1].jnFlags |= JNODE_REMOVE;
        }else{
          JsonNode *pNew = jsonMergePatch(pParse, iTarget+j+1, &pPatch[i+1]);
          if( pNew==0 ) return 0;
          if( pNew!=&pParse->aNode[iTarget+j+1] ){
            jsonParseAddSubstNode(pParse, iTarget+j+1);
            jsonParseAddNodeArray(pParse, pNew, jsonNodeSize(pNew));
          }
          pTarget = &pParse->aNode[iTarget];
        }
        break;
      }
    }
    if( j>=pTarget->n && pPatch[i+1].eType!=JSON_NULL ){
      int iStart;
      JsonNode *pApnd;
      u32 nApnd;
      iStart = jsonParseAddNode(pParse, JSON_OBJECT, 0, 0);
      jsonParseAddNode(pParse, JSON_STRING, nKey, zKey);
      pApnd = &pPatch[i+1];
      if( pApnd->eType==JSON_OBJECT ) jsonRemoveAllNulls(pApnd);
      nApnd = jsonNodeSize(pApnd);
      jsonParseAddNodeArray(pParse, pApnd, jsonNodeSize(pApnd));
      if( pParse->oom ) return 0;
      pParse->aNode[iStart].n = 1+nApnd;
      pParse->aNode[iRoot].jnFlags |= JNODE_APPEND;
      pParse->aNode[iRoot].u.iAppend = iStart;
      VVA( pParse->aNode[iRoot].eU = 2 );
      iRoot = iStart;
      pTarget = &pParse->aNode[iTarget];
    }
  }
  return pTarget;
}

/*
** Implementation of the json_mergepatch(JSON1,JSON2) function.  Return a JSON
** object that is the result of running the RFC 7396 MergePatch() algorithm
** on the two arguments.
*/
static void jsonPatchFunc(
  sqlite3_context *ctx,
  int argc,
  sqlite3_value **argv
){
  JsonParse *pX;     /* The JSON that is being patched */
  JsonParse *pY;     /* The patch */
  JsonNode *pResult;   /* The result of the merge */

  UNUSED_PARAMETER(argc);
  pX = jsonParseCached(ctx, argv[0], ctx, 1);
  if( pX==0 ) return;
  assert( pX->hasMod==0 );
  pX->hasMod = 1;
  pY = jsonParseCached(ctx, argv[1], ctx, 1);
  if( pY==0 ) return;
  pX->useMod = 1;
  pY->useMod = 1;
  pResult = jsonMergePatch(pX, 0, pY->aNode);
  assert( pResult!=0 || pX->oom );
  if( pResult && pX->oom==0 ){
    jsonDebugPrintParse(pX);
    jsonDebugPrintNode(pResult);
    jsonReturnJson(pX, pResult, ctx, 0);
  }else{
    sqlite3_result_error_nomem(ctx);
  }
}


/*
** Implementation of the json_object(NAME,VALUE,...) function.  Return a JSON
** object that contains all name/value given in arguments.  Or if any name
** is not a string or if any value is a BLOB, throw an error.
*/
static void jsonObjectFunc(
  sqlite3_context *ctx,
  int argc,
  sqlite3_value **argv
){
  int i;
  JsonString jx;
  const char *z;
  u32 n;

  if( argc&1 ){
    sqlite3_result_error(ctx, "json_object() requires an even number "
                                  "of arguments", -1);
    return;
  }
  jsonInit(&jx, ctx);
  jsonAppendChar(&jx, '{');
  for(i=0; i<argc; i+=2){
    if( sqlite3_value_type(argv[i])!=SQLITE_TEXT ){
      sqlite3_result_error(ctx, "json_object() labels must be TEXT", -1);
      jsonReset(&jx);
      return;
    }
    jsonAppendSeparator(&jx);
    z = (const char*)sqlite3_value_text(argv[i]);
    n = (u32)sqlite3_value_bytes(argv[i]);
    jsonAppendString(&jx, z, n);
    jsonAppendChar(&jx, ':');
    jsonAppendValue(&jx, argv[i+1]);
  }
  jsonAppendChar(&jx, '}');
  jsonResult(&jx);
  sqlite3_result_subtype(ctx, JSON_SUBTYPE);
}


/*
** json_remove(JSON, PATH, ...)
**
** Remove the named elements from JSON and return the result.  malformed
** JSON or PATH arguments result in an error.
*/
static void jsonRemoveFunc(
  sqlite3_context *ctx,
  int argc,
  sqlite3_value **argv
){
  JsonParse *pParse;          /* The parse */
  JsonNode *pNode;
  const char *zPath;
  u32 i;

  if( argc<1 ) return;
  pParse = jsonParseCached(ctx, argv[0], ctx, argc>1);
  if( pParse==0 ) return;
  for(i=1; i<(u32)argc; i++){
    zPath = (const char*)sqlite3_value_text(argv[i]);
    if( zPath==0 ) goto remove_done;
    pNode = jsonLookup(pParse, zPath, 0, ctx);
    if( pParse->nErr ) goto remove_done;
    if( pNode ){
      pNode->jnFlags |= JNODE_REMOVE;
      pParse->hasMod = 1;
      pParse->useMod = 1;
    }
  }
  if( (pParse->aNode[0].jnFlags & JNODE_REMOVE)==0 ){
    jsonReturnJson(pParse, pParse->aNode, ctx, 1);
  }
remove_done:
  jsonDebugPrintParse(p);
}

/*
** Substitute the value at iNode with the pValue parameter.
*/
static void jsonReplaceNode(
  sqlite3_context *pCtx,
  JsonParse *p,
  int iNode,
  sqlite3_value *pValue
){
  int idx = jsonParseAddSubstNode(p, iNode);
  if( idx<=0 ){
    assert( p->oom );
    return;
  }
  switch( sqlite3_value_type(pValue) ){
    case SQLITE_NULL: {
      jsonParseAddNode(p, JSON_NULL, 0, 0);
      break;
    }
    case SQLITE_FLOAT: {
      char *z = sqlite3_mprintf("%!0.15g", sqlite3_value_double(pValue));
      int n;
      if( z==0 ){
        p->oom = 1;
        break;
      }
      n = sqlite3Strlen30(z);
      jsonParseAddNode(p, JSON_REAL, n, z);
      jsonParseAddCleanup(p, sqlite3_free, z);
      break;
    }
    case SQLITE_INTEGER: {
      char *z = sqlite3_mprintf("%lld", sqlite3_value_int64(pValue));
      int n;
      if( z==0 ){
        p->oom = 1;
        break;
      }
      n = sqlite3Strlen30(z);
      jsonParseAddNode(p, JSON_INT, n, z);
      jsonParseAddCleanup(p, sqlite3_free, z);

      break;
    }
    case SQLITE_TEXT: {
      const char *z = (const char*)sqlite3_value_text(pValue);
      u32 n = (u32)sqlite3_value_bytes(pValue);
      if( z==0 ){
         p->oom = 1;
         break;
      }
      if( sqlite3_value_subtype(pValue)!=JSON_SUBTYPE ){
        char *zCopy = sqlite3DbStrDup(0, z);
        int k;
        if( zCopy ){
          jsonParseAddCleanup(p, sqlite3_free, zCopy);
       }else{
          p->oom = 1;
          sqlite3_result_error_nomem(pCtx);
        }
        k = jsonParseAddNode(p, JSON_STRING, n, zCopy);
        assert( k>0 || p->oom );
        if( p->oom==0 ) p->aNode[k].jnFlags |= JNODE_RAW;
      }else{
        JsonParse *pPatch = jsonParseCached(pCtx, pValue, pCtx, 1);
        if( pPatch==0 ){
          p->oom = 1;
          break;
        }
        jsonParseAddNodeArray(p, pPatch->aNode, pPatch->nNode);
        /* The nodes copied out of pPatch and into p likely contain
        ** u.zJContent pointers into pPatch->zJson.  So preserve the
        ** content of pPatch until p is destroyed. */
        assert( pPatch->nJPRef>=1 );
        pPatch->nJPRef++;
        jsonParseAddCleanup(p, (void(*)(void*))jsonParseFree, pPatch);
      }
      break;
    }
    default: {
      jsonParseAddNode(p, JSON_NULL, 0, 0);
      sqlite3_result_error(pCtx, "JSON cannot hold BLOB values", -1);
      p->nErr++;
      break;
    }
  }
}

/*
** json_replace(JSON, PATH, VALUE, ...)
**
** Replace the value at PATH with VALUE.  If PATH does not already exist,
** this routine is a no-op.  If JSON or PATH is malformed, throw an error.
*/
static void jsonReplaceFunc(
  sqlite3_context *ctx,
  int argc,
  sqlite3_value **argv
){
  JsonParse *pParse;          /* The parse */
  JsonNode *pNode;
  const char *zPath;
  u32 i;

  if( argc<1 ) return;
  if( (argc&1)==0 ) {
    jsonWrongNumArgs(ctx, "replace");
    return;
  }
  pParse = jsonParseCached(ctx, argv[0], ctx, argc>1);
  if( pParse==0 ) return;
  for(i=1; i<(u32)argc; i+=2){
    zPath = (const char*)sqlite3_value_text(argv[i]);
    pParse->useMod = 1;
    pNode = jsonLookup(pParse, zPath, 0, ctx);
    if( pParse->nErr ) goto replace_err;
    if( pNode ){
      jsonReplaceNode(ctx, pParse, (u32)(pNode - pParse->aNode), argv[i+1]);
    }
  }
  jsonReturnJson(pParse, pParse->aNode, ctx, 1);
replace_err:
  jsonDebugPrintParse(pParse);
}


/*
** json_set(JSON, PATH, VALUE, ...)
**
** Set the value at PATH to VALUE.  Create the PATH if it does not already
** exist.  Overwrite existing values that do exist.
** If JSON or PATH is malformed, throw an error.
**
** json_insert(JSON, PATH, VALUE, ...)
**
** Create PATH and initialize it to VALUE.  If PATH already exists, this
** routine is a no-op.  If JSON or PATH is malformed, throw an error.
*/
static void jsonSetFunc(
  sqlite3_context *ctx,
  int argc,
  sqlite3_value **argv
){
  JsonParse *pParse;       /* The parse */
  JsonNode *pNode;
  const char *zPath;
  u32 i;
  int bApnd;
  int bIsSet = sqlite3_user_data(ctx)!=0;

  if( argc<1 ) return;
  if( (argc&1)==0 ) {
    jsonWrongNumArgs(ctx, bIsSet ? "set" : "insert");
    return;
  }
  pParse = jsonParseCached(ctx, argv[0], ctx, argc>1);
  if( pParse==0 ) return;
  for(i=1; i<(u32)argc; i+=2){
    zPath = (const char*)sqlite3_value_text(argv[i]);
    bApnd = 0;
    pParse->useMod = 1;
    pNode = jsonLookup(pParse, zPath, &bApnd, ctx);
    if( pParse->oom ){
      sqlite3_result_error_nomem(ctx);
      goto jsonSetDone;
    }else if( pParse->nErr ){
      goto jsonSetDone;
    }else if( pNode && (bApnd || bIsSet) ){
      jsonReplaceNode(ctx, pParse, (u32)(pNode - pParse->aNode), argv[i+1]);
    }
  }
  jsonDebugPrintParse(pParse);
  jsonReturnJson(pParse, pParse->aNode, ctx, 1);

jsonSetDone:
  /* no cleanup required */;
}

/*
** json_type(JSON)
** json_type(JSON, PATH)
**
** Return the top-level "type" of a JSON string.  json_type() raises an
** error if either the JSON or PATH inputs are not well-formed.
*/
static void jsonTypeFunc(
  sqlite3_context *ctx,
  int argc,
  sqlite3_value **argv
){
  JsonParse *p;          /* The parse */
  const char *zPath;
  JsonNode *pNode;

  p = jsonParseCached(ctx, argv[0], ctx, 0);
  if( p==0 ) return;
  if( argc==2 ){
    zPath = (const char*)sqlite3_value_text(argv[1]);
    pNode = jsonLookup(p, zPath, 0, ctx);
  }else{
    pNode = p->aNode;
  }
  if( pNode ){
    sqlite3_result_text(ctx, jsonType[pNode->eType], -1, SQLITE_STATIC);
  }
}

/*
** json_valid(JSON)
**
** Return 1 if JSON is a well-formed canonical JSON string according
** to RFC-7159. Return 0 otherwise.
*/
static void jsonValidFunc(
  sqlite3_context *ctx,
  int argc,
  sqlite3_value **argv
){
  JsonParse *p;          /* The parse */
  UNUSED_PARAMETER(argc);
  if( sqlite3_value_type(argv[0])==SQLITE_NULL ){
#ifdef SQLITE_LEGACY_JSON_VALID
    /* Incorrect legacy behavior was to return FALSE for a NULL input */
    sqlite3_result_int(ctx, 0);
#endif
    return;
  }
  p = jsonParseCached(ctx, argv[0], 0, 0);
  if( p==0 || p->oom ){
    sqlite3_result_error_nomem(ctx);
    sqlite3_free(p);
  }else{
    sqlite3_result_int(ctx, p->nErr==0 && (p->hasNonstd==0 || p->useMod));
    if( p->nErr ) jsonParseFree(p);
  }
}

/*
** json_error_position(JSON)
**
** If the argument is not an interpretable JSON string, then return the 1-based
** character position at which the parser first recognized that the input
** was in error.  The left-most character is 1.  If the string is valid
** JSON, then return 0.
**
** Note that json_valid() is only true for strictly conforming canonical JSON.
** But this routine returns zero if the input contains extension.  Thus:
**
** (1) If the input X is strictly conforming canonical JSON:
**
**         json_valid(X) returns true
**         json_error_position(X) returns 0
**
** (2) If the input X is JSON but it includes extension (such as JSON5) that
**     are not part of RFC-8259:
**
**         json_valid(X) returns false
**         json_error_position(X) return 0
**
** (3) If the input X cannot be interpreted as JSON even taking extensions
**     into account:
**
**         json_valid(X) return false
**         json_error_position(X) returns 1 or more
*/
static void jsonErrorFunc(
  sqlite3_context *ctx,
  int argc,
  sqlite3_value **argv
){
  JsonParse *p;          /* The parse */
  UNUSED_PARAMETER(argc);
  if( sqlite3_value_type(argv[0])==SQLITE_NULL ) return;
  p = jsonParseCached(ctx, argv[0], 0, 0);
  if( p==0 || p->oom ){
    sqlite3_result_error_nomem(ctx);
    sqlite3_free(p);
  }else if( p->nErr==0 ){
    sqlite3_result_int(ctx, 0);
  }else{
    int n = 1;
    u32 i;
    const char *z = (const char*)sqlite3_value_text(argv[0]);
    for(i=0; i<p->iErr && ALWAYS(z[i]); i++){
      if( (z[i]&0xc0)!=0x80 ) n++;
    }
    sqlite3_result_int(ctx, n);
    jsonParseFree(p);
  }
}


/****************************************************************************
** Aggregate SQL function implementations
****************************************************************************/
/*
** json_group_array(VALUE)
**
** Return a JSON array composed of all values in the aggregate.
*/
static void jsonArrayStep(
  sqlite3_context *ctx,
  int argc,
  sqlite3_value **argv
){
  JsonString *pStr;
  UNUSED_PARAMETER(argc);
  pStr = (JsonString*)sqlite3_aggregate_context(ctx, sizeof(*pStr));
  if( pStr ){
    if( pStr->zBuf==0 ){
      jsonInit(pStr, ctx);
      jsonAppendChar(pStr, '[');
    }else if( pStr->nUsed>1 ){
      jsonAppendChar(pStr, ',');
    }
    pStr->pCtx = ctx;
    jsonAppendValue(pStr, argv[0]);
  }
}
static void jsonArrayCompute(sqlite3_context *ctx, int isFinal){
  JsonString *pStr;
  pStr = (JsonString*)sqlite3_aggregate_context(ctx, 0);
  if( pStr ){
    pStr->pCtx = ctx;
    jsonAppendChar(pStr, ']');
    if( pStr->bErr ){
      if( pStr->bErr==1 ) sqlite3_result_error_nomem(ctx);
      assert( pStr->bStatic );
    }else if( isFinal ){
      sqlite3_result_text(ctx, pStr->zBuf, (int)pStr->nUsed,
                          pStr->bStatic ? SQLITE_TRANSIENT :
                              (void(*)(void*))sqlite3RCStrUnref);
      pStr->bStatic = 1;
    }else{
      sqlite3_result_text(ctx, pStr->zBuf, (int)pStr->nUsed, SQLITE_TRANSIENT);
      pStr->nUsed--;
    }
  }else{
    sqlite3_result_text(ctx, "[]", 2, SQLITE_STATIC);
  }
  sqlite3_result_subtype(ctx, JSON_SUBTYPE);
}
static void jsonArrayValue(sqlite3_context *ctx){
  jsonArrayCompute(ctx, 0);
}
static void jsonArrayFinal(sqlite3_context *ctx){
  jsonArrayCompute(ctx, 1);
}

#ifndef SQLITE_OMIT_WINDOWFUNC
/*
** This method works for both json_group_array() and json_group_object().
** It works by removing the first element of the group by searching forward
** to the first comma (",") that is not within a string and deleting all
** text through that comma.
*/
static void jsonGroupInverse(
  sqlite3_context *ctx,
  int argc,
  sqlite3_value **argv
){
  unsigned int i;
  int inStr = 0;
  int nNest = 0;
  char *z;
  char c;
  JsonString *pStr;
  UNUSED_PARAMETER(argc);
  UNUSED_PARAMETER(argv);
  pStr = (JsonString*)sqlite3_aggregate_context(ctx, 0);
#ifdef NEVER
  /* pStr is always non-NULL since jsonArrayStep() or jsonObjectStep() will
  ** always have been called to initialize it */
  if( NEVER(!pStr) ) return;
#endif
  z = pStr->zBuf;
  for(i=1; i<pStr->nUsed && ((c = z[i])!=',' || inStr || nNest); i++){
    if( c=='"' ){
      inStr = !inStr;
    }else if( c=='\\' ){
      i++;
    }else if( !inStr ){
      if( c=='{' || c=='[' ) nNest++;
      if( c=='}' || c==']' ) nNest--;
    }
  }
  if( i<pStr->nUsed ){
    pStr->nUsed -= i;
    memmove(&z[1], &z[i+1], (size_t)pStr->nUsed-1);
    z[pStr->nUsed] = 0;
  }else{
    pStr->nUsed = 1;
  }
}
#else
# define jsonGroupInverse 0
#endif


/*
** json_group_obj(NAME,VALUE)
**
** Return a JSON object composed of all names and values in the aggregate.
*/
static void jsonObjectStep(
  sqlite3_context *ctx,
  int argc,
  sqlite3_value **argv
){
  JsonString *pStr;
  const char *z;
  u32 n;
  UNUSED_PARAMETER(argc);
  pStr = (JsonString*)sqlite3_aggregate_context(ctx, sizeof(*pStr));
  if( pStr ){
    if( pStr->zBuf==0 ){
      jsonInit(pStr, ctx);
      jsonAppendChar(pStr, '{');
    }else if( pStr->nUsed>1 ){
      jsonAppendChar(pStr, ',');
    }
    pStr->pCtx = ctx;
    z = (const char*)sqlite3_value_text(argv[0]);
    n = (u32)sqlite3_value_bytes(argv[0]);
    jsonAppendString(pStr, z, n);
    jsonAppendChar(pStr, ':');
    jsonAppendValue(pStr, argv[1]);
  }
}
static void jsonObjectCompute(sqlite3_context *ctx, int isFinal){
  JsonString *pStr;
  pStr = (JsonString*)sqlite3_aggregate_context(ctx, 0);
  if( pStr ){
    jsonAppendChar(pStr, '}');
    if( pStr->bErr ){
      if( pStr->bErr==1 ) sqlite3_result_error_nomem(ctx);
      assert( pStr->bStatic );
    }else if( isFinal ){
      sqlite3_result_text(ctx, pStr->zBuf, (int)pStr->nUsed,
                          pStr->bStatic ? SQLITE_TRANSIENT :
                          (void(*)(void*))sqlite3RCStrUnref);
      pStr->bStatic = 1;
    }else{
      sqlite3_result_text(ctx, pStr->zBuf, (int)pStr->nUsed, SQLITE_TRANSIENT);
      pStr->nUsed--;
    }
  }else{
    sqlite3_result_text(ctx, "{}", 2, SQLITE_STATIC);
  }
  sqlite3_result_subtype(ctx, JSON_SUBTYPE);
}
static void jsonObjectValue(sqlite3_context *ctx){
  jsonObjectCompute(ctx, 0);
}
static void jsonObjectFinal(sqlite3_context *ctx){
  jsonObjectCompute(ctx, 1);
}



#ifndef SQLITE_OMIT_VIRTUALTABLE
/****************************************************************************
** The json_each virtual table
****************************************************************************/
typedef struct JsonEachCursor JsonEachCursor;
struct JsonEachCursor {
  sqlite3_vtab_cursor base;  /* Base class - must be first */
  u32 iRowid;                /* The rowid */
  u32 iBegin;                /* The first node of the scan */
  u32 i;                     /* Index in sParse.aNode[] of current row */
  u32 iEnd;                  /* EOF when i equals or exceeds this value */
  u8 eType;                  /* Type of top-level element */
  u8 bRecursive;             /* True for json_tree().  False for json_each() */
  char *zJson;               /* Input JSON */
  char *zRoot;               /* Path by which to filter zJson */
  JsonParse sParse;          /* Parse of the input JSON */
};

/* Constructor for the json_each virtual table */
static int jsonEachConnect(
  sqlite3 *db,
  void *pAux,
  int argc, const char *const*argv,
  sqlite3_vtab **ppVtab,
  char **pzErr
){
  sqlite3_vtab *pNew;
  int rc;

/* Column numbers */
#define JEACH_KEY     0
#define JEACH_VALUE   1
#define JEACH_TYPE    2
#define JEACH_ATOM    3
#define JEACH_ID      4
#define JEACH_PARENT  5
#define JEACH_FULLKEY 6
#define JEACH_PATH    7
/* The xBestIndex method assumes that the JSON and ROOT columns are
** the last two columns in the table.  Should this ever changes, be
** sure to update the xBestIndex method. */
#define JEACH_JSON    8
#define JEACH_ROOT    9

  UNUSED_PARAMETER(pzErr);
  UNUSED_PARAMETER(argv);
  UNUSED_PARAMETER(argc);
  UNUSED_PARAMETER(pAux);
  rc = sqlite3_declare_vtab(db,
     "CREATE TABLE x(key,value,type,atom,id,parent,fullkey,path,"
                    "json HIDDEN,root HIDDEN)");
  if( rc==SQLITE_OK ){
    pNew = *ppVtab = sqlite3_malloc( sizeof(*pNew) );
    if( pNew==0 ) return SQLITE_NOMEM;
    memset(pNew, 0, sizeof(*pNew));
    sqlite3_vtab_config(db, SQLITE_VTAB_INNOCUOUS);
  }
  return rc;
}

/* destructor for json_each virtual table */
static int jsonEachDisconnect(sqlite3_vtab *pVtab){
  sqlite3_free(pVtab);
  return SQLITE_OK;
}

/* constructor for a JsonEachCursor object for json_each(). */
static int jsonEachOpenEach(sqlite3_vtab *p, sqlite3_vtab_cursor **ppCursor){
  JsonEachCursor *pCur;

  UNUSED_PARAMETER(p);
  pCur = sqlite3_malloc( sizeof(*pCur) );
  if( pCur==0 ) return SQLITE_NOMEM;
  memset(pCur, 0, sizeof(*pCur));
  *ppCursor = &pCur->base;
  return SQLITE_OK;
}

/* constructor for a JsonEachCursor object for json_tree(). */
static int jsonEachOpenTree(sqlite3_vtab *p, sqlite3_vtab_cursor **ppCursor){
  int rc = jsonEachOpenEach(p, ppCursor);
  if( rc==SQLITE_OK ){
    JsonEachCursor *pCur = (JsonEachCursor*)*ppCursor;
    pCur->bRecursive = 1;
  }
  return rc;
}

/* Reset a JsonEachCursor back to its original state.  Free any memory
** held. */
static void jsonEachCursorReset(JsonEachCursor *p){
  sqlite3_free(p->zRoot);
  jsonParseReset(&p->sParse);
  p->iRowid = 0;
  p->i = 0;
  p->iEnd = 0;
  p->eType = 0;
  p->zJson = 0;
  p->zRoot = 0;
}

/* Destructor for a jsonEachCursor object */
static int jsonEachClose(sqlite3_vtab_cursor *cur){
  JsonEachCursor *p = (JsonEachCursor*)cur;
  jsonEachCursorReset(p);
  sqlite3_free(cur);
  return SQLITE_OK;
}

/* Return TRUE if the jsonEachCursor object has been advanced off the end
** of the JSON object */
static int jsonEachEof(sqlite3_vtab_cursor *cur){
  JsonEachCursor *p = (JsonEachCursor*)cur;
  return p->i >= p->iEnd;
}

/* Advance the cursor to the next element for json_tree() */
static int jsonEachNext(sqlite3_vtab_cursor *cur){
  JsonEachCursor *p = (JsonEachCursor*)cur;
  if( p->bRecursive ){
    if( p->sParse.aNode[p->i].jnFlags & JNODE_LABEL ) p->i++;
    p->i++;
    p->iRowid++;
    if( p->i<p->iEnd ){
      u32 iUp = p->sParse.aUp[p->i];
      JsonNode *pUp = &p->sParse.aNode[iUp];
      p->eType = pUp->eType;
      if( pUp->eType==JSON_ARRAY ){
        assert( pUp->eU==0 || pUp->eU==3 );
        testcase( pUp->eU==3 );
        VVA( pUp->eU = 3 );
        if( iUp==p->i-1 ){
          pUp->u.iKey = 0;
        }else{
          pUp->u.iKey++;
        }
      }
    }
  }else{
    switch( p->eType ){
      case JSON_ARRAY: {
        p->i += jsonNodeSize(&p->sParse.aNode[p->i]);
        p->iRowid++;
        break;
      }
      case JSON_OBJECT: {
        p->i += 1 + jsonNodeSize(&p->sParse.aNode[p->i+1]);
        p->iRowid++;
        break;
      }
      default: {
        p->i = p->iEnd;
        break;
      }
    }
  }
  return SQLITE_OK;
}

/* Append an object label to the JSON Path being constructed
** in pStr.
*/
static void jsonAppendObjectPathElement(
  JsonString *pStr,
  JsonNode *pNode
){
  int jj, nn;
  const char *z;
  assert( pNode->eType==JSON_STRING );
  assert( pNode->jnFlags & JNODE_LABEL );
  assert( pNode->eU==1 );
  z = pNode->u.zJContent;
  nn = pNode->n;
  if( (pNode->jnFlags & JNODE_RAW)==0 ){
    assert( nn>=2 );
    assert( z[0]=='"' || z[0]=='\'' );
    assert( z[nn-1]=='"' || z[0]=='\'' );
    if( nn>2 && sqlite3Isalpha(z[1]) ){
      for(jj=2; jj<nn-1 && sqlite3Isalnum(z[jj]); jj++){}
      if( jj==nn-1 ){
        z++;
        nn -= 2;
      }
    }
  }
  jsonPrintf(nn+2, pStr, ".%.*s", nn, z);
}

/* Append the name of the path for element i to pStr
*/
static void jsonEachComputePath(
  JsonEachCursor *p,       /* The cursor */
  JsonString *pStr,        /* Write the path here */
  u32 i                    /* Path to this element */
){
  JsonNode *pNode, *pUp;
  u32 iUp;
  if( i==0 ){
    jsonAppendChar(pStr, '$');
    return;
  }
  iUp = p->sParse.aUp[i];
  jsonEachComputePath(p, pStr, iUp);
  pNode = &p->sParse.aNode[i];
  pUp = &p->sParse.aNode[iUp];
  if( pUp->eType==JSON_ARRAY ){
    assert( pUp->eU==3 || (pUp->eU==0 && pUp->u.iKey==0) );
    testcase( pUp->eU==0 );
    jsonPrintf(30, pStr, "[%d]", pUp->u.iKey);
  }else{
    assert( pUp->eType==JSON_OBJECT );
    if( (pNode->jnFlags & JNODE_LABEL)==0 ) pNode--;
    jsonAppendObjectPathElement(pStr, pNode);
  }
}

/* Return the value of a column */
static int jsonEachColumn(
  sqlite3_vtab_cursor *cur,   /* The cursor */
  sqlite3_context *ctx,       /* First argument to sqlite3_result_...() */
  int i                       /* Which column to return */
){
  JsonEachCursor *p = (JsonEachCursor*)cur;
  JsonNode *pThis = &p->sParse.aNode[p->i];
  switch( i ){
    case JEACH_KEY: {
      if( p->i==0 ) break;
      if( p->eType==JSON_OBJECT ){
        jsonReturn(&p->sParse, pThis, ctx);
      }else if( p->eType==JSON_ARRAY ){
        u32 iKey;
        if( p->bRecursive ){
          if( p->iRowid==0 ) break;
          assert( p->sParse.aNode[p->sParse.aUp[p->i]].eU==3 );
          iKey = p->sParse.aNode[p->sParse.aUp[p->i]].u.iKey;
        }else{
          iKey = p->iRowid;
        }
        sqlite3_result_int64(ctx, (sqlite3_int64)iKey);
      }
      break;
    }
    case JEACH_VALUE: {
      if( pThis->jnFlags & JNODE_LABEL ) pThis++;
      jsonReturn(&p->sParse, pThis, ctx);
      break;
    }
    case JEACH_TYPE: {
      if( pThis->jnFlags & JNODE_LABEL ) pThis++;
      sqlite3_result_text(ctx, jsonType[pThis->eType], -1, SQLITE_STATIC);
      break;
    }
    case JEACH_ATOM: {
      if( pThis->jnFlags & JNODE_LABEL ) pThis++;
      if( pThis->eType>=JSON_ARRAY ) break;
      jsonReturn(&p->sParse, pThis, ctx);
      break;
    }
    case JEACH_ID: {
      sqlite3_result_int64(ctx,
         (sqlite3_int64)p->i + ((pThis->jnFlags & JNODE_LABEL)!=0));
      break;
    }
    case JEACH_PARENT: {
      if( p->i>p->iBegin && p->bRecursive ){
        sqlite3_result_int64(ctx, (sqlite3_int64)p->sParse.aUp[p->i]);
      }
      break;
    }
    case JEACH_FULLKEY: {
      JsonString x;
      jsonInit(&x, ctx);
      if( p->bRecursive ){
        jsonEachComputePath(p, &x, p->i);
      }else{
        if( p->zRoot ){
          jsonAppendRaw(&x, p->zRoot, (int)strlen(p->zRoot));
        }else{
          jsonAppendChar(&x, '$');
        }
        if( p->eType==JSON_ARRAY ){
          jsonPrintf(30, &x, "[%d]", p->iRowid);
        }else if( p->eType==JSON_OBJECT ){
          jsonAppendObjectPathElement(&x, pThis);
        }
      }
      jsonResult(&x);
      break;
    }
    case JEACH_PATH: {
      if( p->bRecursive ){
        JsonString x;
        jsonInit(&x, ctx);
        jsonEachComputePath(p, &x, p->sParse.aUp[p->i]);
        jsonResult(&x);
        break;
      }
      /* For json_each() path and root are the same so fall through
      ** into the root case */
      /* no break */ deliberate_fall_through
    }
    default: {
      const char *zRoot = p->zRoot;
      if( zRoot==0 ) zRoot = "$";
      sqlite3_result_text(ctx, zRoot, -1, SQLITE_STATIC);
      break;
    }
    case JEACH_JSON: {
      assert( i==JEACH_JSON );
      sqlite3_result_text(ctx, p->sParse.zJson, -1, SQLITE_STATIC);
      break;
    }
  }
  return SQLITE_OK;
}

/* Return the current rowid value */
static int jsonEachRowid(sqlite3_vtab_cursor *cur, sqlite_int64 *pRowid){
  JsonEachCursor *p = (JsonEachCursor*)cur;
  *pRowid = p->iRowid;
  return SQLITE_OK;
}

/* The query strategy is to look for an equality constraint on the json
** column.  Without such a constraint, the table cannot operate.  idxNum is
** 1 if the constraint is found, 3 if the constraint and zRoot are found,
** and 0 otherwise.
*/
static int jsonEachBestIndex(
  sqlite3_vtab *tab,
  sqlite3_index_info *pIdxInfo
){
  int i;                     /* Loop counter or computed array index */
  int aIdx[2];               /* Index of constraints for JSON and ROOT */
  int unusableMask = 0;      /* Mask of unusable JSON and ROOT constraints */
  int idxMask = 0;           /* Mask of usable == constraints JSON and ROOT */
  const struct sqlite3_index_constraint *pConstraint;

  /* This implementation assumes that JSON and ROOT are the last two
  ** columns in the table */
  assert( JEACH_ROOT == JEACH_JSON+1 );
  UNUSED_PARAMETER(tab);
  aIdx[0] = aIdx[1] = -1;
  pConstraint = pIdxInfo->aConstraint;
  for(i=0; i<pIdxInfo->nConstraint; i++, pConstraint++){
    int iCol;
    int iMask;
    if( pConstraint->iColumn < JEACH_JSON ) continue;
    iCol = pConstraint->iColumn - JEACH_JSON;
    assert( iCol==0 || iCol==1 );
    testcase( iCol==0 );
    iMask = 1 << iCol;
    if( pConstraint->usable==0 ){
      unusableMask |= iMask;
    }else if( pConstraint->op==SQLITE_INDEX_CONSTRAINT_EQ ){
      aIdx[iCol] = i;
      idxMask |= iMask;
    }
  }
  if( pIdxInfo->nOrderBy>0
   && pIdxInfo->aOrderBy[0].iColumn<0
   && pIdxInfo->aOrderBy[0].desc==0
  ){
    pIdxInfo->orderByConsumed = 1;
  }

  if( (unusableMask & ~idxMask)!=0 ){
    /* If there are any unusable constraints on JSON or ROOT, then reject
    ** this entire plan */
    return SQLITE_CONSTRAINT;
  }
  if( aIdx[0]<0 ){
    /* No JSON input.  Leave estimatedCost at the huge value that it was
    ** initialized to to discourage the query planner from selecting this
    ** plan. */
    pIdxInfo->idxNum = 0;
  }else{
    pIdxInfo->estimatedCost = 1.0;
    i = aIdx[0];
    pIdxInfo->aConstraintUsage[i].argvIndex = 1;
    pIdxInfo->aConstraintUsage[i].omit = 1;
    if( aIdx[1]<0 ){
      pIdxInfo->idxNum = 1;  /* Only JSON supplied.  Plan 1 */
    }else{
      i = aIdx[1];
      pIdxInfo->aConstraintUsage[i].argvIndex = 2;
      pIdxInfo->aConstraintUsage[i].omit = 1;
      pIdxInfo->idxNum = 3;  /* Both JSON and ROOT are supplied.  Plan 3 */
    }
  }
  return SQLITE_OK;
}

/* Start a search on a new JSON string */
static int jsonEachFilter(
  sqlite3_vtab_cursor *cur,
  int idxNum, const char *idxStr,
  int argc, sqlite3_value **argv
){
  JsonEachCursor *p = (JsonEachCursor*)cur;
  const char *z;
  const char *zRoot = 0;
  sqlite3_int64 n;

  UNUSED_PARAMETER(idxStr);
  UNUSED_PARAMETER(argc);
  jsonEachCursorReset(p);
  if( idxNum==0 ) return SQLITE_OK;
  z = (const char*)sqlite3_value_text(argv[0]);
  if( z==0 ) return SQLITE_OK;
  memset(&p->sParse, 0, sizeof(p->sParse));
  p->sParse.nJPRef = 1;
  if( sqlite3ValueIsOfClass(argv[0], (void(*)(void*))sqlite3RCStrUnref) ){
    p->sParse.zJson = sqlite3RCStrRef((char*)z);
  }else{
    n = sqlite3_value_bytes(argv[0]);
    p->sParse.zJson = sqlite3RCStrNew( n+1 );
    if( p->sParse.zJson==0 ) return SQLITE_NOMEM;
    memcpy(p->sParse.zJson, z, (size_t)n+1);
  }
  p->sParse.bJsonIsRCStr = 1;
  p->zJson = p->sParse.zJson;
  if( jsonParse(&p->sParse, 0) ){
    int rc = SQLITE_NOMEM;
    if( p->sParse.oom==0 ){
      sqlite3_free(cur->pVtab->zErrMsg);
      cur->pVtab->zErrMsg = sqlite3_mprintf("malformed JSON");
      if( cur->pVtab->zErrMsg ) rc = SQLITE_ERROR;
    }
    jsonEachCursorReset(p);
    return rc;
  }else if( p->bRecursive && jsonParseFindParents(&p->sParse) ){
    jsonEachCursorReset(p);
    return SQLITE_NOMEM;
  }else{
    JsonNode *pNode = 0;
    if( idxNum==3 ){
      const char *zErr = 0;
      zRoot = (const char*)sqlite3_value_text(argv[1]);
      if( zRoot==0 ) return SQLITE_OK;
      n = sqlite3_value_bytes(argv[1]);
      p->zRoot = sqlite3_malloc64( n+1 );
      if( p->zRoot==0 ) return SQLITE_NOMEM;
      memcpy(p->zRoot, zRoot, (size_t)n+1);
      if( zRoot[0]!='$' ){
        zErr = zRoot;
      }else{
        pNode = jsonLookupStep(&p->sParse, 0, p->zRoot+1, 0, &zErr);
      }
      if( zErr ){
        sqlite3_free(cur->pVtab->zErrMsg);
        cur->pVtab->zErrMsg = jsonPathSyntaxError(zErr);
        jsonEachCursorReset(p);
        return cur->pVtab->zErrMsg ? SQLITE_ERROR : SQLITE_NOMEM;
      }else if( pNode==0 ){
        return SQLITE_OK;
      }
    }else{
      pNode = p->sParse.aNode;
    }
    p->iBegin = p->i = (int)(pNode - p->sParse.aNode);
    p->eType = pNode->eType;
    if( p->eType>=JSON_ARRAY ){
      assert( pNode->eU==0 );
      VVA( pNode->eU = 3 );
      pNode->u.iKey = 0;
      p->iEnd = p->i + pNode->n + 1;
      if( p->bRecursive ){
        p->eType = p->sParse.aNode[p->sParse.aUp[p->i]].eType;
        if( p->i>0 && (p->sParse.aNode[p->i-1].jnFlags & JNODE_LABEL)!=0 ){
          p->i--;
        }
      }else{
        p->i++;
      }
    }else{
      p->iEnd = p->i+1;
    }
  }
  return SQLITE_OK;
}

/* The methods of the json_each virtual table */
static sqlite3_module jsonEachModule = {
  0,                         /* iVersion */
  0,                         /* xCreate */
  jsonEachConnect,           /* xConnect */
  jsonEachBestIndex,         /* xBestIndex */
  jsonEachDisconnect,        /* xDisconnect */
  0,                         /* xDestroy */
  jsonEachOpenEach,          /* xOpen - open a cursor */
  jsonEachClose,             /* xClose - close a cursor */
  jsonEachFilter,            /* xFilter - configure scan constraints */
  jsonEachNext,              /* xNext - advance a cursor */
  jsonEachEof,               /* xEof - check for end of scan */
  jsonEachColumn,            /* xColumn - read data */
  jsonEachRowid,             /* xRowid - read data */
  0,                         /* xUpdate */
  0,                         /* xBegin */
  0,                         /* xSync */
  0,                         /* xCommit */
  0,                         /* xRollback */
  0,                         /* xFindMethod */
  0,                         /* xRename */
  0,                         /* xSavepoint */
  0,                         /* xRelease */
  0,                         /* xRollbackTo */
  0                          /* xShadowName */
};

/* The methods of the json_tree virtual table. */
static sqlite3_module jsonTreeModule = {
  0,                         /* iVersion */
  0,                         /* xCreate */
  jsonEachConnect,           /* xConnect */
  jsonEachBestIndex,         /* xBestIndex */
  jsonEachDisconnect,        /* xDisconnect */
  0,                         /* xDestroy */
  jsonEachOpenTree,          /* xOpen - open a cursor */
  jsonEachClose,             /* xClose - close a cursor */
  jsonEachFilter,            /* xFilter - configure scan constraints */
  jsonEachNext,              /* xNext - advance a cursor */
  jsonEachEof,               /* xEof - check for end of scan */
  jsonEachColumn,            /* xColumn - read data */
  jsonEachRowid,             /* xRowid - read data */
  0,                         /* xUpdate */
  0,                         /* xBegin */
  0,                         /* xSync */
  0,                         /* xCommit */
  0,                         /* xRollback */
  0,                         /* xFindMethod */
  0,                         /* xRename */
  0,                         /* xSavepoint */
  0,                         /* xRelease */
  0,                         /* xRollbackTo */
  0                          /* xShadowName */
};
#endif /* SQLITE_OMIT_VIRTUALTABLE */
#endif /* !defined(SQLITE_OMIT_JSON) */

/*
** Register JSON functions.
*/
void sqlite3RegisterJsonFunctions(void){
#ifndef SQLITE_OMIT_JSON
  static FuncDef aJsonFunc[] = {
    JFUNCTION(json,               1, 0,  jsonRemoveFunc),
    JFUNCTION(json_array,        -1, 0,  jsonArrayFunc),
    JFUNCTION(json_array_length,  1, 0,  jsonArrayLengthFunc),
    JFUNCTION(json_array_length,  2, 0,  jsonArrayLengthFunc),
    JFUNCTION(json_error_position,1, 0,  jsonErrorFunc),
    JFUNCTION(json_extract,      -1, 0,  jsonExtractFunc),
    JFUNCTION(->,                 2, JSON_JSON, jsonExtractFunc),
    JFUNCTION(->>,                2, JSON_SQL, jsonExtractFunc),
    JFUNCTION(json_insert,       -1, 0,  jsonSetFunc),
    JFUNCTION(json_object,       -1, 0,  jsonObjectFunc),
    JFUNCTION(json_patch,         2, 0,  jsonPatchFunc),
    JFUNCTION(json_quote,         1, 0,  jsonQuoteFunc),
    JFUNCTION(json_remove,       -1, 0,  jsonRemoveFunc),
    JFUNCTION(json_replace,      -1, 0,  jsonReplaceFunc),
    JFUNCTION(json_set,          -1, JSON_ISSET,  jsonSetFunc),
    JFUNCTION(json_type,          1, 0,  jsonTypeFunc),
    JFUNCTION(json_type,          2, 0,  jsonTypeFunc),
    JFUNCTION(json_valid,         1, 0,  jsonValidFunc),
#if SQLITE_DEBUG
    JFUNCTION(json_parse,         1, 0,  jsonParseFunc),
    JFUNCTION(json_test1,         1, 0,  jsonTest1Func),
#endif
    WAGGREGATE(json_group_array,  1, 0, 0,
       jsonArrayStep, jsonArrayFinal, jsonArrayValue, jsonGroupInverse,
       SQLITE_SUBTYPE|SQLITE_UTF8|SQLITE_DETERMINISTIC),
    WAGGREGATE(json_group_object, 2, 0, 0,
       jsonObjectStep, jsonObjectFinal, jsonObjectValue, jsonGroupInverse,
       SQLITE_SUBTYPE|SQLITE_UTF8|SQLITE_DETERMINISTIC)
  };
  sqlite3InsertBuiltinFuncs(aJsonFunc, ArraySize(aJsonFunc));
#endif
}

#if  !defined(SQLITE_OMIT_VIRTUALTABLE) && !defined(SQLITE_OMIT_JSON)
/*
** Register the JSON table-valued functions
*/
int sqlite3JsonTableFunctions(sqlite3 *db){
  int rc = SQLITE_OK;
  static const struct {
    const char *zName;
    sqlite3_module *pModule;
  } aMod[] = {
    { "json_each",            &jsonEachModule               },
    { "json_tree",            &jsonTreeModule               },
  };
  unsigned int i;
  for(i=0; i<sizeof(aMod)/sizeof(aMod[0]) && rc==SQLITE_OK; i++){
    rc = sqlite3_create_module(db, aMod[i].zName, aMod[i].pModule, 0);
  }
  return rc;
}
#endif /* !defined(SQLITE_OMIT_VIRTUALTABLE) && !defined(SQLITE_OMIT_JSON) */

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
** For the time being, all JSON is stored as pure text.  (We might add
** a JSONB type in the future which stores a binary encoding of JSON in
** a BLOB, but there is no support for JSONB in the current implementation.
** This implementation parses JSON text at 250 MB/s, so it is hard to see
** how JSONB might improve on that.)
*/
#if !defined(SQLITE_CORE) || defined(SQLITE_ENABLE_JSON1)
#if !defined(_SQLITEINT_H_)
#include "sqlite3ext.h"
#endif
SQLITE_EXTENSION_INIT1
#include <assert.h>
#include <string.h>
#include <stdlib.h>
#include <stdarg.h>

#define UNUSED_PARAM(X)  (void)(X)

#ifndef LARGEST_INT64
# define LARGEST_INT64  (0xffffffff|(((sqlite3_int64)0x7fffffff)<<32))
# define SMALLEST_INT64 (((sqlite3_int64)-1) - LARGEST_INT64)
#endif

/*
** Versions of isspace(), isalnum() and isdigit() to which it is safe
** to pass signed char values.
*/
#ifdef sqlite3Isdigit
   /* Use the SQLite core versions if this routine is part of the
   ** SQLite amalgamation */
#  define safe_isdigit(x) sqlite3Isdigit(x)
#  define safe_isalnum(x) sqlite3Isalnum(x)
#else
   /* Use the standard library for separate compilation */
#include <ctype.h>  /* amalgamator: keep */
#  define safe_isdigit(x) isdigit((unsigned char)(x))
#  define safe_isalnum(x) isalnum((unsigned char)(x))
#endif

/*
** Growing our own isspace() routine this way is twice as fast as
** the library isspace() function, resulting in a 7% overall performance
** increase for the parser.  (Ubuntu14.10 gcc 4.8.4 x64 with -Os).
*/
static const char jsonIsSpace[] = {
  0, 0, 0, 0, 0, 0, 0, 0,     0, 1, 1, 0, 0, 1, 0, 0,
  0, 0, 0, 0, 0, 0, 0, 0,     0, 0, 0, 0, 0, 0, 0, 0,
  1, 0, 0, 0, 0, 0, 0, 0,     0, 0, 0, 0, 0, 0, 0, 0,
  0, 0, 0, 0, 0, 0, 0, 0,     0, 0, 0, 0, 0, 0, 0, 0,
  0, 0, 0, 0, 0, 0, 0, 0,     0, 0, 0, 0, 0, 0, 0, 0,
  0, 0, 0, 0, 0, 0, 0, 0,     0, 0, 0, 0, 0, 0, 0, 0,
  0, 0, 0, 0, 0, 0, 0, 0,     0, 0, 0, 0, 0, 0, 0, 0,
  0, 0, 0, 0, 0, 0, 0, 0,     0, 0, 0, 0, 0, 0, 0, 0,
  0, 0, 0, 0, 0, 0, 0, 0,     0, 0, 0, 0, 0, 0, 0, 0,
  0, 0, 0, 0, 0, 0, 0, 0,     0, 0, 0, 0, 0, 0, 0, 0,
  0, 0, 0, 0, 0, 0, 0, 0,     0, 0, 0, 0, 0, 0, 0, 0,
  0, 0, 0, 0, 0, 0, 0, 0,     0, 0, 0, 0, 0, 0, 0, 0,
  0, 0, 0, 0, 0, 0, 0, 0,     0, 0, 0, 0, 0, 0, 0, 0,
  0, 0, 0, 0, 0, 0, 0, 0,     0, 0, 0, 0, 0, 0, 0, 0,
  0, 0, 0, 0, 0, 0, 0, 0,     0, 0, 0, 0, 0, 0, 0, 0,
  0, 0, 0, 0, 0, 0, 0, 0,     0, 0, 0, 0, 0, 0, 0, 0,
};
#define safe_isspace(x) (jsonIsSpace[(unsigned char)x])

#ifndef SQLITE_AMALGAMATION
  /* Unsigned integer types.  These are already defined in the sqliteInt.h,
  ** but the definitions need to be repeated for separate compilation. */
  typedef sqlite3_uint64 u64;
  typedef unsigned int u32;
  typedef unsigned char u8;
#endif

/* Objects */
typedef struct JsonString JsonString;
typedef struct JsonNode JsonNode;
typedef struct JsonParse JsonParse;

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

/* JSON type values
*/
#define JSON_NULL     0
#define JSON_TRUE     1
#define JSON_FALSE    2
#define JSON_INT      3
#define JSON_REAL     4
#define JSON_STRING   5
#define JSON_ARRAY    6
#define JSON_OBJECT   7

/* The "subtype" set for JSON values */
#define JSON_SUBTYPE  74    /* Ascii for "J" */

/*
** Names of the various JSON types:
*/
static const char * const jsonType[] = {
  "null", "true", "false", "integer", "real", "text", "array", "object"
};

/* Bit values for the JsonNode.jnFlag field
*/
#define JNODE_RAW     0x01         /* Content is raw, not JSON encoded */
#define JNODE_ESCAPE  0x02         /* Content is text with \ escapes */
#define JNODE_REMOVE  0x04         /* Do not output */
#define JNODE_REPLACE 0x08         /* Replace with JsonNode.iVal */
#define JNODE_APPEND  0x10         /* More ARRAY/OBJECT entries at u.iAppend */
#define JNODE_LABEL   0x20         /* Is a label of an object */


/* A single node of parsed JSON
*/
struct JsonNode {
  u8 eType;              /* One of the JSON_ type values */
  u8 jnFlags;            /* JNODE flags */
  u8 iVal;               /* Replacement value when JNODE_REPLACE */
  u32 n;                 /* Bytes of content, or number of sub-nodes */
  union {
    const char *zJContent; /* Content for INT, REAL, and STRING */
    u32 iAppend;           /* More terms for ARRAY and OBJECT */
    u32 iKey;              /* Key for ARRAY objects in json_tree() */
  } u;
};

/* A completely parsed JSON string
*/
struct JsonParse {
  u32 nNode;         /* Number of slots of aNode[] used */
  u32 nAlloc;        /* Number of slots of aNode[] allocated */
  JsonNode *aNode;   /* Array of nodes containing the parse */
  const char *zJson; /* Original JSON string */
  u32 *aUp;          /* Index of parent of each node */
  u8 oom;            /* Set to true if out of memory */
  u8 nErr;           /* Number of errors seen */
};

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
  if( !p->bStatic ) sqlite3_free(p->zBuf);
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
    zNew = sqlite3_malloc64(nTotal);
    if( zNew==0 ){
      jsonOom(p);
      return SQLITE_NOMEM;
    }
    memcpy(zNew, p->zBuf, (size_t)p->nUsed);
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

/* Append N bytes from zIn onto the end of the JsonString string.
*/
static void jsonAppendRaw(JsonString *p, const char *zIn, u32 N){
  if( (N+p->nUsed >= p->nAlloc) && jsonGrow(p,N)!=0 ) return;
  memcpy(p->zBuf+p->nUsed, zIn, N);
  p->nUsed += N;
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
static void jsonAppendChar(JsonString *p, char c){
  if( p->nUsed>=p->nAlloc && jsonGrow(p,1)!=0 ) return;
  p->zBuf[p->nUsed++] = c;
}

/* Append a comma separator to the output buffer, if the previous
** character is not '[' or '{'.
*/
static void jsonAppendSeparator(JsonString *p){
  char c;
  if( p->nUsed==0 ) return;
  c = p->zBuf[p->nUsed-1];
  if( c!='[' && c!='{' ) jsonAppendChar(p, ',');
}

/* Append the N-byte string in zIn to the end of the JsonString string
** under construction.  Enclose the string in "..." and escape
** any double-quotes or backslash characters contained within the
** string.
*/
static void jsonAppendString(JsonString *p, const char *zIn, u32 N){
  u32 i;
  if( (N+p->nUsed+2 >= p->nAlloc) && jsonGrow(p,N+2)!=0 ) return;
  p->zBuf[p->nUsed++] = '"';
  for(i=0; i<N; i++){
    char c = zIn[i];
    if( c=='"' || c=='\\' ){
      if( (p->nUsed+N+3-i > p->nAlloc) && jsonGrow(p,N+3-i)!=0 ) return;
      p->zBuf[p->nUsed++] = '\\';
    }
    p->zBuf[p->nUsed++] = c;
  }
  p->zBuf[p->nUsed++] = '"';
  assert( p->nUsed<p->nAlloc );
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
      jsonAppendRaw(p, "null", 4);
      break;
    }
    case SQLITE_INTEGER:
    case SQLITE_FLOAT: {
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
        p->bErr = 1;
        jsonReset(p);
      }
      break;
    }
  }
}


/* Make the JSON in p the result of the SQL function.
*/
static void jsonResult(JsonString *p){
  if( p->bErr==0 ){
    sqlite3_result_text64(p->pCtx, p->zBuf, p->nUsed, 
                          p->bStatic ? SQLITE_TRANSIENT : sqlite3_free,
                          SQLITE_UTF8);
    jsonZero(p);
  }
  assert( p->bStatic );
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
  sqlite3_free(pParse->aNode);
  pParse->aNode = 0;
  pParse->nNode = 0;
  pParse->nAlloc = 0;
  sqlite3_free(pParse->aUp);
  pParse->aUp = 0;
}

/*
** Convert the JsonNode pNode into a pure JSON string and
** append to pOut.  Subsubstructure is also included.  Return
** the number of JsonNode objects that are encoded.
*/
static void jsonRenderNode(
  JsonNode *pNode,               /* The node to render */
  JsonString *pOut,              /* Write JSON here */
  sqlite3_value **aReplace       /* Replacement values */
){
  switch( pNode->eType ){
    default: {
      assert( pNode->eType==JSON_NULL );
      jsonAppendRaw(pOut, "null", 4);
      break;
    }
    case JSON_TRUE: {
      jsonAppendRaw(pOut, "true", 4);
      break;
    }
    case JSON_FALSE: {
      jsonAppendRaw(pOut, "false", 5);
      break;
    }
    case JSON_STRING: {
      if( pNode->jnFlags & JNODE_RAW ){
        jsonAppendString(pOut, pNode->u.zJContent, pNode->n);
        break;
      }
      /* Fall through into the next case */
    }
    case JSON_REAL:
    case JSON_INT: {
      jsonAppendRaw(pOut, pNode->u.zJContent, pNode->n);
      break;
    }
    case JSON_ARRAY: {
      u32 j = 1;
      jsonAppendChar(pOut, '[');
      for(;;){
        while( j<=pNode->n ){
          if( pNode[j].jnFlags & (JNODE_REMOVE|JNODE_REPLACE) ){
            if( pNode[j].jnFlags & JNODE_REPLACE ){
              jsonAppendSeparator(pOut);
              jsonAppendValue(pOut, aReplace[pNode[j].iVal]);
            }
          }else{
            jsonAppendSeparator(pOut);
            jsonRenderNode(&pNode[j], pOut, aReplace);
          }
          j += jsonNodeSize(&pNode[j]);
        }
        if( (pNode->jnFlags & JNODE_APPEND)==0 ) break;
        pNode = &pNode[pNode->u.iAppend];
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
          if( (pNode[j+1].jnFlags & JNODE_REMOVE)==0 ){
            jsonAppendSeparator(pOut);
            jsonRenderNode(&pNode[j], pOut, aReplace);
            jsonAppendChar(pOut, ':');
            if( pNode[j+1].jnFlags & JNODE_REPLACE ){
              jsonAppendValue(pOut, aReplace[pNode[j+1].iVal]);
            }else{
              jsonRenderNode(&pNode[j+1], pOut, aReplace);
            }
          }
          j += 1 + jsonNodeSize(&pNode[j+1]);
        }
        if( (pNode->jnFlags & JNODE_APPEND)==0 ) break;
        pNode = &pNode[pNode->u.iAppend];
        j = 1;
      }
      jsonAppendChar(pOut, '}');
      break;
    }
  }
}

/*
** Return a JsonNode and all its descendents as a JSON string.
*/
static void jsonReturnJson(
  JsonNode *pNode,            /* Node to return */
  sqlite3_context *pCtx,      /* Return value for this function */
  sqlite3_value **aReplace    /* Array of replacement values */
){
  JsonString s;
  jsonInit(&s, pCtx);
  jsonRenderNode(pNode, &s, aReplace);
  jsonResult(&s);
  sqlite3_result_subtype(pCtx, JSON_SUBTYPE);
}

/*
** Make the JsonNode the return value of the function.
*/
static void jsonReturn(
  JsonNode *pNode,            /* Node to return */
  sqlite3_context *pCtx,      /* Return value for this function */
  sqlite3_value **aReplace    /* Array of replacement values */
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
      const char *z = pNode->u.zJContent;
      if( z[0]=='-' ){ z++; }
      while( z[0]>='0' && z[0]<='9' ){
        unsigned v = *(z++) - '0';
        if( i>=LARGEST_INT64/10 ){
          if( i>LARGEST_INT64/10 ) goto int_as_real;
          if( z[0]>='0' && z[0]<='9' ) goto int_as_real;
          if( v==9 ) goto int_as_real;
          if( v==8 ){
            if( pNode->u.zJContent[0]=='-' ){
              sqlite3_result_int64(pCtx, SMALLEST_INT64);
              goto int_done;
            }else{
              goto int_as_real;
            }
          }
        }
        i = i*10 + v;
      }
      if( pNode->u.zJContent[0]=='-' ){ i = -i; }
      sqlite3_result_int64(pCtx, i);
      int_done:
      break;
      int_as_real: /* fall through to real */;
    }
    case JSON_REAL: {
      double r;
#ifdef SQLITE_AMALGAMATION
      const char *z = pNode->u.zJContent;
      sqlite3AtoF(z, &r, sqlite3Strlen30(z), SQLITE_UTF8);
#else
      r = strtod(pNode->u.zJContent, 0);
#endif
      sqlite3_result_double(pCtx, r);
      break;
    }
    case JSON_STRING: {
#if 0 /* Never happens because JNODE_RAW is only set by json_set(),
      ** json_insert() and json_replace() and those routines do not
      ** call jsonReturn() */
      if( pNode->jnFlags & JNODE_RAW ){
        sqlite3_result_text(pCtx, pNode->u.zJContent, pNode->n,
                            SQLITE_TRANSIENT);
      }else 
#endif
      assert( (pNode->jnFlags & JNODE_RAW)==0 );
      if( (pNode->jnFlags & JNODE_ESCAPE)==0 ){
        /* JSON formatted without any backslash-escapes */
        sqlite3_result_text(pCtx, pNode->u.zJContent+1, pNode->n-2,
                            SQLITE_TRANSIENT);
      }else{
        /* Translate JSON formatted string into raw text */
        u32 i;
        u32 n = pNode->n;
        const char *z = pNode->u.zJContent;
        char *zOut;
        u32 j;
        zOut = sqlite3_malloc( n+1 );
        if( zOut==0 ){
          sqlite3_result_error_nomem(pCtx);
          break;
        }
        for(i=1, j=0; i<n-1; i++){
          char c = z[i];
          if( c!='\\' ){
            zOut[j++] = c;
          }else{
            c = z[++i];
            if( c=='u' ){
              u32 v = 0, k;
              for(k=0; k<4 && i<n-2; i++, k++){
                c = z[i+1];
                if( c>='0' && c<='9' ) v = v*16 + c - '0';
                else if( c>='A' && c<='F' ) v = v*16 + c - 'A' + 10;
                else if( c>='a' && c<='f' ) v = v*16 + c - 'a' + 10;
                else break;
              }
              if( v==0 ) break;
              if( v<=0x7f ){
                zOut[j++] = (char)v;
              }else if( v<=0x7ff ){
                zOut[j++] = (char)(0xc0 | (v>>6));
                zOut[j++] = 0x80 | (v&0x3f);
              }else{
                zOut[j++] = (char)(0xe0 | (v>>12));
                zOut[j++] = 0x80 | ((v>>6)&0x3f);
                zOut[j++] = 0x80 | (v&0x3f);
              }
            }else{
              if( c=='b' ){
                c = '\b';
              }else if( c=='f' ){
                c = '\f';
              }else if( c=='n' ){
                c = '\n';
              }else if( c=='r' ){
                c = '\r';
              }else if( c=='t' ){
                c = '\t';
              }
              zOut[j++] = c;
            }
          }
        }
        zOut[j] = 0;
        sqlite3_result_text(pCtx, zOut, j, sqlite3_free);
      }
      break;
    }
    case JSON_ARRAY:
    case JSON_OBJECT: {
      jsonReturnJson(pNode, pCtx, aReplace);
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
  pNew = sqlite3_realloc(pParse->aNode, sizeof(JsonNode)*nNew);
  if( pNew==0 ){
    pParse->oom = 1;
    return -1;
  }
  pParse->nAlloc = nNew;
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
  if( pParse->nNode>=pParse->nAlloc ){
    return jsonParseAddNodeExpand(pParse, eType, n, zContent);
  }
  p = &pParse->aNode[pParse->nNode];
  p->eType = (u8)eType;
  p->jnFlags = 0;
  p->iVal = 0;
  p->n = n;
  p->u.zJContent = zContent;
  return pParse->nNode++;
}

/*
** Parse a single JSON value which begins at pParse->zJson[i].  Return the
** index of the first character past the end of the value parsed.
**
** Return negative for a syntax error.  Special cases:  return -2 if the
** first non-whitespace character is '}' and return -3 if the first
** non-whitespace character is ']'.
*/
static int jsonParseValue(JsonParse *pParse, u32 i){
  char c;
  u32 j;
  int iThis;
  int x;
  JsonNode *pNode;
  while( safe_isspace(pParse->zJson[i]) ){ i++; }
  if( (c = pParse->zJson[i])=='{' ){
    /* Parse object */
    iThis = jsonParseAddNode(pParse, JSON_OBJECT, 0, 0);
    if( iThis<0 ) return -1;
    for(j=i+1;;j++){
      while( safe_isspace(pParse->zJson[j]) ){ j++; }
      x = jsonParseValue(pParse, j);
      if( x<0 ){
        if( x==(-2) && pParse->nNode==(u32)iThis+1 ) return j+1;
        return -1;
      }
      if( pParse->oom ) return -1;
      pNode = &pParse->aNode[pParse->nNode-1];
      if( pNode->eType!=JSON_STRING ) return -1;
      pNode->jnFlags |= JNODE_LABEL;
      j = x;
      while( safe_isspace(pParse->zJson[j]) ){ j++; }
      if( pParse->zJson[j]!=':' ) return -1;
      j++;
      x = jsonParseValue(pParse, j);
      if( x<0 ) return -1;
      j = x;
      while( safe_isspace(pParse->zJson[j]) ){ j++; }
      c = pParse->zJson[j];
      if( c==',' ) continue;
      if( c!='}' ) return -1;
      break;
    }
    pParse->aNode[iThis].n = pParse->nNode - (u32)iThis - 1;
    return j+1;
  }else if( c=='[' ){
    /* Parse array */
    iThis = jsonParseAddNode(pParse, JSON_ARRAY, 0, 0);
    if( iThis<0 ) return -1;
    for(j=i+1;;j++){
      while( safe_isspace(pParse->zJson[j]) ){ j++; }
      x = jsonParseValue(pParse, j);
      if( x<0 ){
        if( x==(-3) && pParse->nNode==(u32)iThis+1 ) return j+1;
        return -1;
      }
      j = x;
      while( safe_isspace(pParse->zJson[j]) ){ j++; }
      c = pParse->zJson[j];
      if( c==',' ) continue;
      if( c!=']' ) return -1;
      break;
    }
    pParse->aNode[iThis].n = pParse->nNode - (u32)iThis - 1;
    return j+1;
  }else if( c=='"' ){
    /* Parse string */
    u8 jnFlags = 0;
    j = i+1;
    for(;;){
      c = pParse->zJson[j];
      if( c==0 ) return -1;
      if( c=='\\' ){
        c = pParse->zJson[++j];
        if( c==0 ) return -1;
        jnFlags = JNODE_ESCAPE;
      }else if( c=='"' ){
        break;
      }
      j++;
    }
    jsonParseAddNode(pParse, JSON_STRING, j+1-i, &pParse->zJson[i]);
    if( !pParse->oom ) pParse->aNode[pParse->nNode-1].jnFlags = jnFlags;
    return j+1;
  }else if( c=='n'
         && strncmp(pParse->zJson+i,"null",4)==0
         && !safe_isalnum(pParse->zJson[i+4]) ){
    jsonParseAddNode(pParse, JSON_NULL, 0, 0);
    return i+4;
  }else if( c=='t'
         && strncmp(pParse->zJson+i,"true",4)==0
         && !safe_isalnum(pParse->zJson[i+4]) ){
    jsonParseAddNode(pParse, JSON_TRUE, 0, 0);
    return i+4;
  }else if( c=='f'
         && strncmp(pParse->zJson+i,"false",5)==0
         && !safe_isalnum(pParse->zJson[i+5]) ){
    jsonParseAddNode(pParse, JSON_FALSE, 0, 0);
    return i+5;
  }else if( c=='-' || (c>='0' && c<='9') ){
    /* Parse number */
    u8 seenDP = 0;
    u8 seenE = 0;
    j = i+1;
    for(;; j++){
      c = pParse->zJson[j];
      if( c>='0' && c<='9' ) continue;
      if( c=='.' ){
        if( pParse->zJson[j-1]=='-' ) return -1;
        if( seenDP ) return -1;
        seenDP = 1;
        continue;
      }
      if( c=='e' || c=='E' ){
        if( pParse->zJson[j-1]<'0' ) return -1;
        if( seenE ) return -1;
        seenDP = seenE = 1;
        c = pParse->zJson[j+1];
        if( c=='+' || c=='-' ){
          j++;
          c = pParse->zJson[j+1];
        }
        if( c<'0' || c>'9' ) return -1;
        continue;
      }
      break;
    }
    if( pParse->zJson[j-1]<'0' ) return -1;
    jsonParseAddNode(pParse, seenDP ? JSON_REAL : JSON_INT,
                        j - i, &pParse->zJson[i]);
    return j;
  }else if( c=='}' ){
    return -2;  /* End of {...} */
  }else if( c==']' ){
    return -3;  /* End of [...] */
  }else if( c==0 ){
    return 0;   /* End of file */
  }else{
    return -1;  /* Syntax error */
  }
}

/*
** Parse a complete JSON string.  Return 0 on success or non-zero if there
** are any errors.  If an error occurs, free all memory associated with
** pParse.
**
** pParse is uninitialized when this routine is called.
*/
static int jsonParse(
  JsonParse *pParse,           /* Initialize and fill this JsonParse object */
  sqlite3_context *pCtx,       /* Report errors here */
  const char *zJson            /* Input JSON text to be parsed */
){
  int i;
  memset(pParse, 0, sizeof(*pParse));
  if( zJson==0 ) return 1;
  pParse->zJson = zJson;
  i = jsonParseValue(pParse, 0);
  if( pParse->oom ) i = -1;
  if( i>0 ){
    while( safe_isspace(zJson[i]) ) i++;
    if( zJson[i] ) i = -1;
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
  aUp = pParse->aUp = sqlite3_malloc( sizeof(u32)*pParse->nNode );
  if( aUp==0 ){
    pParse->oom = 1;
    return SQLITE_NOMEM;
  }
  jsonParseFillInParentage(pParse, 0, 0);
  return SQLITE_OK;
}

/*
** Compare the OBJECT label at pNode against zKey,nKey.  Return true on
** a match.
*/
static int jsonLabelCompare(JsonNode *pNode, const char *zKey, u32 nKey){
  if( pNode->jnFlags & JNODE_RAW ){
    if( pNode->n!=nKey ) return 0;
    return strncmp(pNode->u.zJContent, zKey, nKey)==0;
  }else{
    if( pNode->n!=nKey+2 ) return 0;
    return strncmp(pNode->u.zJContent+1, zKey, nKey)==0;
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
  JsonNode *pRoot = &pParse->aNode[iRoot];
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
    }else{
      zKey = zPath;
      for(i=0; zPath[i] && zPath[i]!='.' && zPath[i]!='['; i++){}
      nKey = i;
    }
    if( nKey==0 ){
      *pzErr = zPath;
      return 0;
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
      iRoot += pRoot->u.iAppend;
      pRoot = &pParse->aNode[iRoot];
      j = 1;
    }
    if( pApnd ){
      u32 iStart, iLabel;
      JsonNode *pNode;
      iStart = jsonParseAddNode(pParse, JSON_OBJECT, 2, 0);
      iLabel = jsonParseAddNode(pParse, JSON_STRING, i, zPath);
      zPath += i;
      pNode = jsonLookupAppend(pParse, zPath, pApnd, pzErr);
      if( pParse->oom ) return 0;
      if( pNode ){
        pRoot = &pParse->aNode[iRoot];
        pRoot->u.iAppend = iStart - iRoot;
        pRoot->jnFlags |= JNODE_APPEND;
        pParse->aNode[iLabel].jnFlags |= JNODE_RAW;
      }
      return pNode;
    }
  }else if( zPath[0]=='[' && safe_isdigit(zPath[1]) ){
    if( pRoot->eType!=JSON_ARRAY ) return 0;
    i = 0;
    j = 1;
    while( safe_isdigit(zPath[j]) ){
      i = i*10 + zPath[j] - '0';
      j++;
    }
    if( zPath[j]!=']' ){
      *pzErr = zPath;
      return 0;
    }
    zPath += j + 1;
    j = 1;
    for(;;){
      while( j<=pRoot->n && (i>0 || (pRoot[j].jnFlags & JNODE_REMOVE)!=0) ){
        if( (pRoot[j].jnFlags & JNODE_REMOVE)==0 ) i--;
        j += jsonNodeSize(&pRoot[j]);
      }
      if( (pRoot->jnFlags & JNODE_APPEND)==0 ) break;
      iRoot += pRoot->u.iAppend;
      pRoot = &pParse->aNode[iRoot];
      j = 1;
    }
    if( j<=pRoot->n ){
      return jsonLookupStep(pParse, iRoot+j, zPath, pApnd, pzErr);
    }
    if( i==0 && pApnd ){
      u32 iStart;
      JsonNode *pNode;
      iStart = jsonParseAddNode(pParse, JSON_ARRAY, 1, 0);
      pNode = jsonLookupAppend(pParse, zPath, pApnd, pzErr);
      if( pParse->oom ) return 0;
      if( pNode ){
        pRoot = &pParse->aNode[iRoot];
        pRoot->u.iAppend = iStart - iRoot;
        pRoot->jnFlags |= JNODE_APPEND;
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


/****************************************************************************
** SQL functions used for testing and debugging
****************************************************************************/

#ifdef SQLITE_DEBUG
/*
** The json_parse(JSON) function returns a string which describes
** a parse of the JSON provided.  Or it returns NULL if JSON is not
** well-formed.
*/
static void jsonParseFunc(
  sqlite3_context *ctx,
  int argc,
  sqlite3_value **argv
){
  JsonString s;       /* Output string - not real JSON */
  JsonParse x;        /* The parse */
  u32 i;

  assert( argc==1 );
  if( jsonParse(&x, ctx, (const char*)sqlite3_value_text(argv[0])) ) return;
  jsonParseFindParents(&x);
  jsonInit(&s, ctx);
  for(i=0; i<x.nNode; i++){
    const char *zType;
    if( x.aNode[i].jnFlags & JNODE_LABEL ){
      assert( x.aNode[i].eType==JSON_STRING );
      zType = "label";
    }else{
      zType = jsonType[x.aNode[i].eType];
    }
    jsonPrintf(100, &s,"node %3u: %7s n=%-4d up=%-4d",
               i, zType, x.aNode[i].n, x.aUp[i]);
    if( x.aNode[i].u.zJContent!=0 ){
      jsonAppendRaw(&s, " ", 1);
      jsonAppendRaw(&s, x.aNode[i].u.zJContent, x.aNode[i].n);
    }
    jsonAppendRaw(&s, "\n", 1);
  }
  jsonParseReset(&x);
  jsonResult(&s);
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
  UNUSED_PARAM(argc);
  sqlite3_result_int(ctx, sqlite3_value_subtype(argv[0])==JSON_SUBTYPE);
}
#endif /* SQLITE_DEBUG */

/****************************************************************************
** SQL function implementations
****************************************************************************/

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
  JsonParse x;          /* The parse */
  sqlite3_int64 n = 0;
  u32 i;
  JsonNode *pNode;

  if( jsonParse(&x, ctx, (const char*)sqlite3_value_text(argv[0])) ) return;
  assert( x.nNode );
  if( argc==2 ){
    const char *zPath = (const char*)sqlite3_value_text(argv[1]);
    pNode = jsonLookup(&x, zPath, 0, ctx);
  }else{
    pNode = x.aNode;
  }
  if( pNode==0 ){
    x.nErr = 1;
  }else if( pNode->eType==JSON_ARRAY ){
    assert( (pNode->jnFlags & JNODE_APPEND)==0 );
    for(i=1; i<=pNode->n; n++){
      i += jsonNodeSize(&pNode[i]);
    }
  }
  if( x.nErr==0 ) sqlite3_result_int64(ctx, n);
  jsonParseReset(&x);
}

/*
** json_extract(JSON, PATH, ...)
**
** Return the element described by PATH.  Return NULL if there is no
** PATH element.  If there are multiple PATHs, then return a JSON array
** with the result from each path.  Throw an error if the JSON or any PATH
** is malformed.
*/
static void jsonExtractFunc(
  sqlite3_context *ctx,
  int argc,
  sqlite3_value **argv
){
  JsonParse x;          /* The parse */
  JsonNode *pNode;
  const char *zPath;
  JsonString jx;
  int i;

  if( argc<2 ) return;
  if( jsonParse(&x, ctx, (const char*)sqlite3_value_text(argv[0])) ) return;
  jsonInit(&jx, ctx);
  jsonAppendChar(&jx, '[');
  for(i=1; i<argc; i++){
    zPath = (const char*)sqlite3_value_text(argv[i]);
    pNode = jsonLookup(&x, zPath, 0, ctx);
    if( x.nErr ) break;
    if( argc>2 ){
      jsonAppendSeparator(&jx);
      if( pNode ){
        jsonRenderNode(pNode, &jx, 0);
      }else{
        jsonAppendRaw(&jx, "null", 4);
      }
    }else if( pNode ){
      jsonReturn(pNode, ctx, 0);
    }
  }
  if( argc>2 && i==argc ){
    jsonAppendChar(&jx, ']');
    jsonResult(&jx);
    sqlite3_result_subtype(ctx, JSON_SUBTYPE);
  }
  jsonReset(&jx);
  jsonParseReset(&x);
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
  JsonParse x;          /* The parse */
  JsonNode *pNode;
  const char *zPath;
  u32 i;

  if( argc<1 ) return;
  if( jsonParse(&x, ctx, (const char*)sqlite3_value_text(argv[0])) ) return;
  assert( x.nNode );
  for(i=1; i<(u32)argc; i++){
    zPath = (const char*)sqlite3_value_text(argv[i]);
    if( zPath==0 ) goto remove_done;
    pNode = jsonLookup(&x, zPath, 0, ctx);
    if( x.nErr ) goto remove_done;
    if( pNode ) pNode->jnFlags |= JNODE_REMOVE;
  }
  if( (x.aNode[0].jnFlags & JNODE_REMOVE)==0 ){
    jsonReturnJson(x.aNode, ctx, 0);
  }
remove_done:
  jsonParseReset(&x);
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
  JsonParse x;          /* The parse */
  JsonNode *pNode;
  const char *zPath;
  u32 i;

  if( argc<1 ) return;
  if( (argc&1)==0 ) {
    jsonWrongNumArgs(ctx, "replace");
    return;
  }
  if( jsonParse(&x, ctx, (const char*)sqlite3_value_text(argv[0])) ) return;
  assert( x.nNode );
  for(i=1; i<(u32)argc; i+=2){
    zPath = (const char*)sqlite3_value_text(argv[i]);
    pNode = jsonLookup(&x, zPath, 0, ctx);
    if( x.nErr ) goto replace_err;
    if( pNode ){
      pNode->jnFlags |= (u8)JNODE_REPLACE;
      pNode->iVal = (u8)(i+1);
    }
  }
  if( x.aNode[0].jnFlags & JNODE_REPLACE ){
    sqlite3_result_value(ctx, argv[x.aNode[0].iVal]);
  }else{
    jsonReturnJson(x.aNode, ctx, argv);
  }
replace_err:
  jsonParseReset(&x);
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
  JsonParse x;          /* The parse */
  JsonNode *pNode;
  const char *zPath;
  u32 i;
  int bApnd;
  int bIsSet = *(int*)sqlite3_user_data(ctx);

  if( argc<1 ) return;
  if( (argc&1)==0 ) {
    jsonWrongNumArgs(ctx, bIsSet ? "set" : "insert");
    return;
  }
  if( jsonParse(&x, ctx, (const char*)sqlite3_value_text(argv[0])) ) return;
  assert( x.nNode );
  for(i=1; i<(u32)argc; i+=2){
    zPath = (const char*)sqlite3_value_text(argv[i]);
    bApnd = 0;
    pNode = jsonLookup(&x, zPath, &bApnd, ctx);
    if( x.oom ){
      sqlite3_result_error_nomem(ctx);
      goto jsonSetDone;
    }else if( x.nErr ){
      goto jsonSetDone;
    }else if( pNode && (bApnd || bIsSet) ){
      pNode->jnFlags |= (u8)JNODE_REPLACE;
      pNode->iVal = (u8)(i+1);
    }
  }
  if( x.aNode[0].jnFlags & JNODE_REPLACE ){
    sqlite3_result_value(ctx, argv[x.aNode[0].iVal]);
  }else{
    jsonReturnJson(x.aNode, ctx, argv);
  }
jsonSetDone:
  jsonParseReset(&x);
}

/*
** json_type(JSON)
** json_type(JSON, PATH)
**
** Return the top-level "type" of a JSON string.  Throw an error if
** either the JSON or PATH inputs are not well-formed.
*/
static void jsonTypeFunc(
  sqlite3_context *ctx,
  int argc,
  sqlite3_value **argv
){
  JsonParse x;          /* The parse */
  const char *zPath;
  JsonNode *pNode;

  if( jsonParse(&x, ctx, (const char*)sqlite3_value_text(argv[0])) ) return;
  assert( x.nNode );
  if( argc==2 ){
    zPath = (const char*)sqlite3_value_text(argv[1]);
    pNode = jsonLookup(&x, zPath, 0, ctx);
  }else{
    pNode = x.aNode;
  }
  if( pNode ){
    sqlite3_result_text(ctx, jsonType[pNode->eType], -1, SQLITE_STATIC);
  }
  jsonParseReset(&x);
}

/*
** json_valid(JSON)
**
** Return 1 if JSON is a well-formed JSON string according to RFC-7159.
** Return 0 otherwise.
*/
static void jsonValidFunc(
  sqlite3_context *ctx,
  int argc,
  sqlite3_value **argv
){
  JsonParse x;          /* The parse */
  int rc = 0;

  UNUSED_PARAM(argc);
  if( jsonParse(&x, 0, (const char*)sqlite3_value_text(argv[0]))==0 ){
    rc = 1;
  }
  jsonParseReset(&x);
  sqlite3_result_int(ctx, rc);
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
#define JEACH_JSON    8
#define JEACH_ROOT    9

  UNUSED_PARAM(pzErr);
  UNUSED_PARAM(argv);
  UNUSED_PARAM(argc);
  UNUSED_PARAM(pAux);
  rc = sqlite3_declare_vtab(db, 
     "CREATE TABLE x(key,value,type,atom,id,parent,fullkey,path,"
                    "json HIDDEN,root HIDDEN)");
  if( rc==SQLITE_OK ){
    pNew = *ppVtab = sqlite3_malloc( sizeof(*pNew) );
    if( pNew==0 ) return SQLITE_NOMEM;
    memset(pNew, 0, sizeof(*pNew));
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

  UNUSED_PARAM(p);
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
  sqlite3_free(p->zJson);
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
    jsonPrintf(30, pStr, "[%d]", pUp->u.iKey);
  }else{
    assert( pUp->eType==JSON_OBJECT );
    if( (pNode->jnFlags & JNODE_LABEL)==0 ) pNode--;
    assert( pNode->eType==JSON_STRING );
    assert( pNode->jnFlags & JNODE_LABEL );
    jsonPrintf(pNode->n+1, pStr, ".%.*s", pNode->n-2, pNode->u.zJContent+1);
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
        jsonReturn(pThis, ctx, 0);
      }else if( p->eType==JSON_ARRAY ){
        u32 iKey;
        if( p->bRecursive ){
          if( p->iRowid==0 ) break;
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
      jsonReturn(pThis, ctx, 0);
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
      jsonReturn(pThis, ctx, 0);
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
        }else{
          jsonPrintf(pThis->n, &x, ".%.*s", pThis->n-2, pThis->u.zJContent+1);
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
    }
    case JEACH_ROOT: {
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
  int i;
  int jsonIdx = -1;
  int rootIdx = -1;
  const struct sqlite3_index_constraint *pConstraint;

  UNUSED_PARAM(tab);
  pConstraint = pIdxInfo->aConstraint;
  for(i=0; i<pIdxInfo->nConstraint; i++, pConstraint++){
    if( pConstraint->usable==0 ) continue;
    if( pConstraint->op!=SQLITE_INDEX_CONSTRAINT_EQ ) continue;
    switch( pConstraint->iColumn ){
      case JEACH_JSON:   jsonIdx = i;    break;
      case JEACH_ROOT:   rootIdx = i;    break;
      default:           /* no-op */     break;
    }
  }
  if( jsonIdx<0 ){
    pIdxInfo->idxNum = 0;
    pIdxInfo->estimatedCost = 1e99;
  }else{
    pIdxInfo->estimatedCost = 1.0;
    pIdxInfo->aConstraintUsage[jsonIdx].argvIndex = 1;
    pIdxInfo->aConstraintUsage[jsonIdx].omit = 1;
    if( rootIdx<0 ){
      pIdxInfo->idxNum = 1;
    }else{
      pIdxInfo->aConstraintUsage[rootIdx].argvIndex = 2;
      pIdxInfo->aConstraintUsage[rootIdx].omit = 1;
      pIdxInfo->idxNum = 3;
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

  UNUSED_PARAM(idxStr);
  UNUSED_PARAM(argc);
  jsonEachCursorReset(p);
  if( idxNum==0 ) return SQLITE_OK;
  z = (const char*)sqlite3_value_text(argv[0]);
  if( z==0 ) return SQLITE_OK;
  n = sqlite3_value_bytes(argv[0]);
  p->zJson = sqlite3_malloc64( n+1 );
  if( p->zJson==0 ) return SQLITE_NOMEM;
  memcpy(p->zJson, z, (size_t)n+1);
  if( jsonParse(&p->sParse, 0, p->zJson) ){
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
  0                          /* xRollbackTo */
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
  0                          /* xRollbackTo */
};
#endif /* SQLITE_OMIT_VIRTUALTABLE */

/****************************************************************************
** The following routines are the only publically visible identifiers in this
** file.  Call the following routines in order to register the various SQL
** functions and the virtual table implemented by this file.
****************************************************************************/

int sqlite3Json1Init(sqlite3 *db){
  int rc = SQLITE_OK;
  unsigned int i;
  static const struct {
     const char *zName;
     int nArg;
     int flag;
     void (*xFunc)(sqlite3_context*,int,sqlite3_value**);
  } aFunc[] = {
    { "json",                 1, 0,   jsonRemoveFunc        },
    { "json_array",          -1, 0,   jsonArrayFunc         },
    { "json_array_length",    1, 0,   jsonArrayLengthFunc   },
    { "json_array_length",    2, 0,   jsonArrayLengthFunc   },
    { "json_extract",        -1, 0,   jsonExtractFunc       },
    { "json_insert",         -1, 0,   jsonSetFunc           },
    { "json_object",         -1, 0,   jsonObjectFunc        },
    { "json_remove",         -1, 0,   jsonRemoveFunc        },
    { "json_replace",        -1, 0,   jsonReplaceFunc       },
    { "json_set",            -1, 1,   jsonSetFunc           },
    { "json_type",            1, 0,   jsonTypeFunc          },
    { "json_type",            2, 0,   jsonTypeFunc          },
    { "json_valid",           1, 0,   jsonValidFunc         },

#if SQLITE_DEBUG
    /* DEBUG and TESTING functions */
    { "json_parse",           1, 0,   jsonParseFunc         },
    { "json_test1",           1, 0,   jsonTest1Func         },
#endif
  };
#ifndef SQLITE_OMIT_VIRTUALTABLE
  static const struct {
     const char *zName;
     sqlite3_module *pModule;
  } aMod[] = {
    { "json_each",            &jsonEachModule               },
    { "json_tree",            &jsonTreeModule               },
  };
#endif
  for(i=0; i<sizeof(aFunc)/sizeof(aFunc[0]) && rc==SQLITE_OK; i++){
    rc = sqlite3_create_function(db, aFunc[i].zName, aFunc[i].nArg,
                                 SQLITE_UTF8 | SQLITE_DETERMINISTIC, 
                                 (void*)&aFunc[i].flag,
                                 aFunc[i].xFunc, 0, 0);
  }
#ifndef SQLITE_OMIT_VIRTUALTABLE
  for(i=0; i<sizeof(aMod)/sizeof(aMod[0]) && rc==SQLITE_OK; i++){
    rc = sqlite3_create_module(db, aMod[i].zName, aMod[i].pModule, 0);
  }
#endif
  return rc;
}


#ifndef SQLITE_CORE
#ifdef _WIN32
__declspec(dllexport)
#endif
int sqlite3_json_init(
  sqlite3 *db, 
  char **pzErrMsg, 
  const sqlite3_api_routines *pApi
){
  SQLITE_EXTENSION_INIT2(pApi);
  (void)pzErrMsg;  /* Unused parameter */
  return sqlite3Json1Init(db);
}
#endif
#endif /* !defined(SQLITE_CORE) || defined(SQLITE_ENABLE_JSON1) */

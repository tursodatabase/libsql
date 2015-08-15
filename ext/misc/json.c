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
**
** JSONB format:
**
** A JSONB blob is a sequence of terms.  Each term begins with a single
** variable length integer X which determines the type and size of the term.
**
**      type = X%8
**      size = X>>3
**
** Term types are 0 through 7 for null, true, false, integer, real, string,
** array, and object.  The meaning of size depends on the type.
**
** For null, true, and false terms, the size is always 0.
**
** For integer terms, the size is the number of bytes that contains the
** integer value.  The value is stored as big-endian twos-complement.
**
** For real terms, the size is always 8 and the value is a big-ending
** double-precision floating-point number.
**
** For string terms, the size is the number of bytes in the string.  The
** string itself immediately follows the X integer.  There are no escapes
** and the string is not zero-terminated.  The string is always stored as
** UTF8.
**
** For array terms, the size is the number of bytes in content.  The
** content consists of zero or more additional terms that are the elements
** of the array.
**
** For object terms, the size is the number of bytes of content.  The 
** content is zero or more pairs of terms.  The first element of each
** pair is a string term which is the label and the second element is
** the value.
**
** Variable Length Integers:
**
** The variable length integer encoding is the 64-bit unsigned integer encoding
** originally developed for SQLite4.  The encoding for each integer is between
** 1 and 9 bytes.  Call those bytes A0 through A8.  The encoding is as follows:
**
**    If A0 is between 0 and 240 inclusive, then the value is A0.
** 
**    If A0 is between 241 and 248 inclusive, then the value is
**    240+256*(A0-241)+A1.
** 
**    If A0 is 249 then the value is 2288+256*A1+A2.
** 
**    If A0 is 250 or more then the value is a (A0-247)-byte big-endian
**    integer taken from starting at A1.
*/
#include "sqlite3ext.h"
SQLITE_EXTENSION_INIT1
#include <assert.h>
#include <string.h>
#include <ctype.h>

/* Unsigned integer types */
typedef sqlite3_uint64 u64;
typedef unsigned int u32;
typedef unsigned char u8;

/* An instance of this object represents a JSON string or
** JSONB blob under construction.
*/
typedef struct Json Json;
struct Json {
  sqlite3_context *pCtx;   /* Function context - put error messages here */
  char *zBuf;              /* Append JSON or JSONB content here */
  u64 nAlloc;              /* Bytes of storage available in zBuf[] */
  u64 nUsed;               /* Bytes of zBuf[] currently used */
  u8 bStatic;              /* True if zBuf is static space */
  u8 oom;                  /* True if an OOM has been encountered */
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

/* A single node of parsed JSON
*/
typedef struct JsonNode JsonNode;
struct JsonNode {
  u32 eType;             /* One of the JSON_ type values */
  u32 n;                 /* Bytes of content, or number of sub-nodes */
  const char *zContent;  /* Content for JSON_INT, JSON_REAL, or JSON_STRING */
};

/* A completely parsed JSON string
*/
typedef struct JsonParse JsonParse;
struct JsonParse {
  u32 nNode;         /* Number of slots of aNode[] used */
  u32 nAlloc;        /* Number of slots of aNode[] allocated */
  JsonNode *aNode;   /* Array of nodes containing the parse */
  const char *zJson; /* Original JSON string */
  u8 oom;            /* Set to true if out of memory */
};


#if 0
/*
** Decode the varint in the first n bytes z[].  Write the integer value
** into *pResult and return the number of bytes in the varint.
**
** If the decode fails because there are not enough bytes in z[] then
** return 0;
*/
static int jsonGetVarint64(
  const unsigned char *z,
  int n,
  u64 *pResult
){
  unsigned int x;
  if( n<1 ) return 0;
  if( z[0]<=240 ){
    *pResult = z[0];
    return 1;
  }
  if( z[0]<=248 ){
    if( n<2 ) return 0;
    *pResult = (z[0]-241)*256 + z[1] + 240;
    return 2;
  }
  if( n<z[0]-246 ) return 0;
  if( z[0]==249 ){
    *pResult = 2288 + 256*z[1] + z[2];
    return 3;
  }
  if( z[0]==250 ){
    *pResult = (z[1]<<16) + (z[2]<<8) + z[3];
    return 4;
  }
  x = (z[1]<<24) + (z[2]<<16) + (z[3]<<8) + z[4];
  if( z[0]==251 ){
    *pResult = x;
    return 5;
  }
  if( z[0]==252 ){
    *pResult = (((u64)x)<<8) + z[5];
    return 6;
  }
  if( z[0]==253 ){
    *pResult = (((u64)x)<<16) + (z[5]<<8) + z[6];
    return 7;
  }
  if( z[0]==254 ){
    *pResult = (((u64)x)<<24) + (z[5]<<16) + (z[6]<<8) + z[7];
    return 8;
  }
  *pResult = (((u64)x)<<32) +
               (0xffffffff & ((z[5]<<24) + (z[6]<<16) + (z[7]<<8) + z[8]));
  return 9;
}
#endif

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
  p->oom = 0;
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
  p->oom = 1;
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
    if( p->oom ) return SQLITE_NOMEM;
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

/* Append the zero-terminated string zIn
*/
static void jsonAppend(Json *p, const char *zIn){
  jsonAppendRaw(p, zIn, (u32)strlen(zIn));
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

/*
** Write a 32-bit unsigned integer as 4 big-endian bytes.
*/
static void jsonPutInt32(unsigned char *z, unsigned int y){
  z[0] = (unsigned char)(y>>24);
  z[1] = (unsigned char)(y>>16);
  z[2] = (unsigned char)(y>>8);
  z[3] = (unsigned char)(y);
}


/* Write integer X as a variable-length integer into the buffer z[].
** z[] is guaranteed to be at least 9 bytes in length.  Return the
** number of bytes written.
*/
int jsonPutVarint64(char *zIn, u64 x){
  unsigned char *z = (unsigned char*)zIn;
  unsigned int w, y;
  if( x<=240 ){
    z[0] = (unsigned char)x;
    return 1;
  }
  if( x<=2287 ){
    y = (unsigned int)(x - 240);
    z[0] = (unsigned char)(y/256 + 241);
    z[1] = (unsigned char)(y%256);
    return 2;
  }
  if( x<=67823 ){
    y = (unsigned int)(x - 2288);
    z[0] = 249;
    z[1] = (unsigned char)(y/256);
    z[2] = (unsigned char)(y%256);
    return 3;
  }
  y = (unsigned int)x;
  w = (unsigned int)(x>>32);
  if( w==0 ){
    if( y<=16777215 ){
      z[0] = 250;
      z[1] = (unsigned char)(y>>16);
      z[2] = (unsigned char)(y>>8);
      z[3] = (unsigned char)(y);
      return 4;
    }
    z[0] = 251;
    jsonPutInt32(z+1, y);
    return 5;
  }
  if( w<=255 ){
    z[0] = 252;
    z[1] = (unsigned char)w;
    jsonPutInt32(z+2, y);
    return 6;
  }
  if( w<=65535 ){
    z[0] = 253;
    z[1] = (unsigned char)(w>>8);
    z[2] = (unsigned char)w;
    jsonPutInt32(z+3, y);
    return 7;
  }
  if( w<=16777215 ){
    z[0] = 254;
    z[1] = (unsigned char)(w>>16);
    z[2] = (unsigned char)(w>>8);
    z[3] = (unsigned char)w;
    jsonPutInt32(z+4, y);
    return 8;
  }
  z[0] = 255;
  jsonPutInt32(z+1, w);
  jsonPutInt32(z+5, y);
  return 9;
}


/* Append integer X as a variable-length integer on the JSONB currently
** under construction in p.
*/
static void jsonAppendVarint(Json *p, u64 X){
  if( (p->nUsed+9 > p->nAlloc) && jsonGrow(p,9)!=0 ) return;
  p->nUsed += jsonPutVarint64(p->zBuf+p->nUsed, X);
}

/* Make the JSON in p the result of the SQL function.
*/
static void jsonResult(Json *p){
  if( p->oom==0 ){
    sqlite3_result_text64(p->pCtx, p->zBuf, p->nUsed, 
                          p->bStatic ? SQLITE_TRANSIENT : sqlite3_free,
                          SQLITE_UTF8);
    jsonZero(p);
  }
  assert( p->bStatic );
}

/* Make the JSONB in p the result of the SQL function.
*/
static void jsonbResult(Json *p){
  if( p->oom==0 ){
    sqlite3_result_blob(p->pCtx, p->zBuf, p->nUsed, 
                        p->bStatic ? SQLITE_TRANSIENT : sqlite3_free);
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

/*
** Implementation of the jsonb_array(VALUE,...) function.  Return a JSON
** array that contains all values given in arguments.  Or if any argument
** is a BLOB, throw an error.
*/
static void jsonbArrayFunc(
  sqlite3_context *context,
  int argc,
  sqlite3_value **argv
){
  int i;
  Json jx;

  jsonInit(&jx, context);
  jx.nUsed = 5;
  for(i=0; i<argc; i++){
    switch( sqlite3_value_type(argv[i]) ){
      case SQLITE_NULL: {
        jsonAppendVarint(&jx, JSON_NULL);
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
        jsonAppendVarint(&jx, JSON_STRING + 4*(u64)n);
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
  if( jx.oom==0 ){
    jx.zBuf[0] = 251;
    jsonPutInt32((unsigned char*)(jx.zBuf+1), jx.nUsed-5);
    jsonbResult(&jx);
  }
}

/*
** Implementation of the json_object(NAME,VALUE,...) function.  Return a JSON
** object that contains all name/value given in arguments.  Or if any name
** is not a string or if any value is a BLOB, throw an error.
*/
static void jsonObjectFunc(
  sqlite3_context *context,
  int argc,
  sqlite3_value **argv
){
  int i;
  Json jx;
  char cSep = '{';
  const char *z;
  u32 n;

  if( argc&1 ){
    sqlite3_result_error(context, "json_object() requires an even number "
                                  "of arguments", -1);
    return;
  }
  jsonInit(&jx, context);
  for(i=0; i<argc; i+=2){
    jsonAppendRaw(&jx, &cSep, 1);
    cSep = ',';
    if( sqlite3_value_type(argv[i])!=SQLITE_TEXT ){
      sqlite3_result_error(context, "json_object() labels must be TEXT", -1);
      jsonZero(&jx);
      return;
    }
    z = (const char*)sqlite3_value_text(argv[i]);
    n = (u32)sqlite3_value_bytes(argv[i]);
    jsonAppendString(&jx, z, n);
    jsonAppendRaw(&jx, ":", 1);
    switch( sqlite3_value_type(argv[i+1]) ){
      case SQLITE_NULL: {
        jsonAppendRaw(&jx, "null", 4);
        break;
      }
      case SQLITE_INTEGER:
      case SQLITE_FLOAT: {
        z = (const char*)sqlite3_value_text(argv[i+1]);
        n = (u32)sqlite3_value_bytes(argv[i+1]);
        jsonAppendRaw(&jx, z, n);
        break;
      }
      case SQLITE_TEXT: {
        z = (const char*)sqlite3_value_text(argv[i+1]);
        n = (u32)sqlite3_value_bytes(argv[i+1]);
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
  jsonAppendRaw(&jx, "}", 1);
  jsonResult(&jx);
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
    u32 nNew;
    JsonNode *pNew;
    if( pParse->oom ) return -1;
    nNew = pParse->nAlloc*2 + 10;
    if( nNew<=pParse->nNode ){
      pParse->oom = 1;
      return -1;
    }
    pNew = sqlite3_realloc(pParse->aNode, sizeof(JsonNode)*nNew);
    if( pNew==0 ){
      pParse->oom = 1;
      return -1;
    }
    pParse->nAlloc = nNew;
    pParse->aNode = pNew;
  }
  p = &pParse->aNode[pParse->nNode];
  p->eType = eType;
  p->n = n;
  p->zContent = zContent;
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
  u32 iThis;
  int x;
  while( isspace(pParse->zJson[i]) ){ i++; }
  if( (c = pParse->zJson[i])==0 ) return 0;
  if( c=='{' ){
    /* Parse object */
    iThis = jsonParseAddNode(pParse, JSON_OBJECT, 0, 0);
    if( iThis<0 ) return -1;
    for(j=i+1;;j++){
      while( isspace(pParse->zJson[j]) ){ j++; }
      x = jsonParseValue(pParse, j);
      if( x<0 ){
        if( x==(-2) && pParse->nNode==iThis+1 ) return j+1;
        return -1;
      }
      if( pParse->aNode[pParse->nNode-1].eType!=JSON_STRING ) return -1;
      j = x;
      while( isspace(pParse->zJson[j]) ){ j++; }
      if( pParse->zJson[j]!=':' ) return -1;
      j++;
      x = jsonParseValue(pParse, j);
      if( x<0 ) return -1;
      j = x;
      while( isspace(pParse->zJson[j]) ){ j++; }
      c = pParse->zJson[j];
      if( c==',' ) continue;
      if( c!='}' ) return -1;
      break;
    }
    pParse->aNode[iThis].n = pParse->nNode - iThis - 1;
    return j+1;
  }else if( c=='[' ){
    /* Parse array */
    iThis = jsonParseAddNode(pParse, JSON_ARRAY, 0, 0);
    if( iThis<0 ) return -1;
    for(j=i+1;;j++){
      while( isspace(pParse->zJson[j]) ){ j++; }
      x = jsonParseValue(pParse, j);
      if( x<0 ){
        if( x==(-3) && pParse->nNode==iThis+1 ) return j+1;
        return -1;
      }
      j = x;
      while( isspace(pParse->zJson[j]) ){ j++; }
      c = pParse->zJson[j];
      if( c==',' ) continue;
      if( c!=']' ) return -1;
      break;
    }
    pParse->aNode[iThis].n = pParse->nNode - iThis - 1;
    return j+1;
  }else if( c=='"' ){
    /* Parse string */
    j = i+1;
    for(;;){
      c = pParse->zJson[j];
      if( c==0 ) return -1;
      if( c=='\\' ){
        c = pParse->zJson[++j];
        if( c==0 ) return -1;
      }else if( c=='"' ){
        break;
      }
      j++;
    }
    jsonParseAddNode(pParse, JSON_STRING, j+1-i, &pParse->zJson[i]);
    return j+1;
  }else if( c=='n'
         && strncmp(pParse->zJson+i,"null",4)==0
         && !isalnum(pParse->zJson[i+5]) ){
    jsonParseAddNode(pParse, JSON_NULL, 0, 0);
    return i+4;
  }else if( c=='t'
         && strncmp(pParse->zJson+i,"true",4)==0
         && !isalnum(pParse->zJson[i+5]) ){
    jsonParseAddNode(pParse, JSON_TRUE, 0, 0);
    return i+4;
  }else if( c=='f'
         && strncmp(pParse->zJson+i,"false",5)==0
         && !isalnum(pParse->zJson[i+6]) ){
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
        if( c=='+' || c=='-' ) j++;
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
static int jsonParse(JsonParse *pParse, const char *zJson){
  int i;
  if( zJson==0 ) return 1;
  memset(pParse, 0, sizeof(*pParse));
  pParse->zJson = zJson;
  i = jsonParseValue(pParse, 0);
  if( i>0 ){
    while( isspace(zJson[i]) ) i++;
    if( zJson[i] ) i = -1;
  }
  if( i<0 ){
    sqlite3_free(pParse->aNode);
    pParse->aNode = 0;
    pParse->nNode = 0;
    pParse->nAlloc = 0;
    return 1;
  }
  return 0;
}

/*
** The json_debug(JSON) function returns a string which describes
** a parse of the JSON provided.  Or it returns NULL if JSON is not
** well-formed.
*/
static void jsonDebugFunc(
  sqlite3_context *context,
  int argc,
  sqlite3_value **argv
){
  Json s;       /* Output string - not real JSON */
  JsonParse x;  /* The parse */
  u32 i;
  char zBuf[50];
  static const char *azType[] = {
    "NULL", "TRUE", "FALSE", "INT", "REAL", "STRING", "ARRAY", "OBJECT"
  };

  assert( argc==1 );
  if( jsonParse(&x, (const char*)sqlite3_value_text(argv[0])) ) return;
  jsonInit(&s, context);
  for(i=0; i<x.nNode; i++){
    sqlite3_snprintf(sizeof(zBuf), zBuf, "node %u:\n", i);
    jsonAppend(&s, zBuf);
    sqlite3_snprintf(sizeof(zBuf), zBuf, "  type: %s\n",
                     azType[x.aNode[i].eType]);
    jsonAppend(&s, zBuf);
    if( x.aNode[i].eType>=JSON_INT ){
      sqlite3_snprintf(sizeof(zBuf), zBuf, "     n: %u\n", x.aNode[i].n);
      jsonAppend(&s, zBuf);
    }
    if( x.aNode[i].zContent!=0 ){
      sqlite3_snprintf(sizeof(zBuf), zBuf, "  ofst: %u\n",
                       (u32)(x.aNode[i].zContent - x.zJson));
      jsonAppend(&s, zBuf);
      jsonAppendRaw(&s, "  text: ", 8);
      jsonAppendRaw(&s, x.aNode[i].zContent, x.aNode[i].n);
      jsonAppendRaw(&s, "\n", 1);
    }
  }
  sqlite3_free(x.aNode);
  jsonResult(&s);
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
    { "json_array",     -1,    jsonArrayFunc     },
    { "jsonb_array",    -1,    jsonbArrayFunc    },
    { "json_object",    -1,    jsonObjectFunc    },
    { "json_debug",      1,    jsonDebugFunc     },
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

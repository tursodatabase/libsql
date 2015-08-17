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
** a BLOB, but there is no support for JSONB in the current implementation.)
*/
#include "sqlite3ext.h"
SQLITE_EXTENSION_INIT1
#include <assert.h>
#include <string.h>
#include <ctype.h>
#include <stdlib.h>

/* Unsigned integer types */
typedef sqlite3_uint64 u64;
typedef unsigned int u32;
typedef unsigned char u8;

/* An instance of this object represents a JSON string
** under construction.  Really, this is a generic string accumulator
** that can be and is used to create strings other than JSON.
*/
typedef struct Json Json;
struct Json {
  sqlite3_context *pCtx;   /* Function context - put error messages here */
  char *zBuf;              /* Append JSON content here */
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

/*
** Names of the various JSON types:
*/
static const char * const jsonType[] = {
  "null", "true", "false", "integer", "real", "text", "array", "object"
};


/* A single node of parsed JSON
*/
typedef struct JsonNode JsonNode;
struct JsonNode {
  u8 eType;              /* One of the JSON_ type values */
  u8 bRaw;               /* Content is raw, rather than JSON encoded */
  u8 bBackslash;         /* Formatted JSON_STRING contains \ escapes */
  u32 n;                 /* Bytes of content, or number of sub-nodes */
  const char *zJContent; /* JSON content */
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

/* Append a single character
*/
static void jsonAppendChar(Json *p, char c){
  if( p->nUsed>=p->nAlloc && jsonGrow(p,1)!=0 ) return;
  p->zBuf[p->nUsed++] = c;
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

/*
** Convert the JsonNode pNode into a pure JSON string and
** append to pOut.  Subsubstructure is also included.  Return
** the number of JsonNode objects that are encoded.
*/
static int jsonRenderNode(JsonNode *pNode, Json *pOut){
  u32 j = 0;
  switch( pNode->eType ){
    case JSON_NULL: {
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
      if( pNode->bRaw ){
        jsonAppendString(pOut, pNode->zJContent, pNode->n);
        break;
      }
      /* Fall through into the next case */
    }
    case JSON_REAL:
    case JSON_INT: {
      jsonAppendRaw(pOut, pNode->zJContent, pNode->n);
      break;
    }
    case JSON_ARRAY: {
      jsonAppendChar(pOut, '[');
      j = 0;
      while( j<pNode->n ){
        if( j>0 ) jsonAppendChar(pOut, ',');
        j += jsonRenderNode(&pNode[j+1], pOut);
      }
      jsonAppendChar(pOut, ']');
      break;
    }
    case JSON_OBJECT: {
      jsonAppendChar(pOut, '{');
      j = 0;
      while( j<pNode->n ){
        if( j>0 ) jsonAppendChar(pOut, ',');
        j += jsonRenderNode(&pNode[j+1], pOut);
        jsonAppendChar(pOut, ':');
        j += jsonRenderNode(&pNode[j+1], pOut);
      }
      jsonAppendChar(pOut, '}');
      break;
    }
  }
  return j+1;
}

/*
** Make the JsonNode the return value of the function.
*/
static void jsonReturn(JsonNode *pNode, sqlite3_context *pCtx){
  switch( pNode->eType ){
    case JSON_NULL: {
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
    case JSON_REAL: {
      double r = strtod(pNode->zJContent, 0);
      sqlite3_result_double(pCtx, r);
      break;
    }
    case JSON_INT: {
      sqlite3_int64 i = 0;
      const char *z = pNode->zJContent;
      if( z[0]=='-' ){ z++; }
      while( z[0]>='0' && z[0]<='9' ){ i = i*10 + *(z++) - '0'; }
      if( pNode->zJContent[0]=='-' ){ i = -i; }
      sqlite3_result_int64(pCtx, i);
      break;
    }
    case JSON_STRING: {
      if( pNode->bRaw ){
        sqlite3_result_text(pCtx, pNode->zJContent, pNode->n, SQLITE_TRANSIENT);
      }else if( !pNode->bBackslash ){
        /* JSON formatted without any backslash-escapes */
        sqlite3_result_text(pCtx, pNode->zJContent+1, pNode->n-2,
                            SQLITE_TRANSIENT);
      }else{
        /* Translate JSON formatted string into raw text */
        u32 i;
        u32 n = pNode->n;
        const char *z = pNode->zJContent;
        char *zOut;
        u32 j;
        zOut = sqlite3_malloc( n+1 );
        if( zOut==0 ){
          sqlite3_result_error_nomem(pCtx);
          break;
        }
        for(i=1, j=0; i<n-1; i++){
          char c = z[i];
          if( c!='\\' && z[i+1] ){
            zOut[j++] = c;
          }else{
            c = z[++i];
            if( c=='u' && z[1] ){
              u32 v = 0, k;
              z++;
              for(k=0; k<4 && z[k]; k++){
                c = z[0];
                if( c>='0' && c<='9' ) v = v*16 + c - '0';
                else if( c>='A' && c<='F' ) v = v*16 + c - 'A' + 10;
                else if( c>='a' && c<='f' ) v = v*16 + c - 'a' + 10;
                else break;
                z++;
              }
              if( v<=0x7f ){
                zOut[j++] = v;
              }else if( v<=0x7ff ){
                zOut[j++] = 0xc0 | (v>>6);
                zOut[j++] = 0x80 | (v&0x3f);
              }else if( v<=0xffff ){
                zOut[j++] = 0xe0 | (v>>12);
                zOut[j++] = 0x80 | ((v>>6)&0x3f);
                zOut[j++] = 0x80 | (v&0x3f);
              }else if( v<=0x10ffff ){
                zOut[j++] = 0xf0 | (v>>18);
                zOut[j++] = 0x80 | ((v>>12)&0x3f);
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
      Json s;
      jsonInit(&s, pCtx);
      jsonRenderNode(pNode, &s);
      jsonResult(&s);
      break;
    }
  }
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
  p->eType = (u8)eType;
  p->bRaw = 0;
  p->bBackslash = 0;
  p->n = n;
  p->zJContent = zContent;
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
    u8 bBackslash = 0;
    j = i+1;
    for(;;){
      c = pParse->zJson[j];
      if( c==0 ) return -1;
      if( c=='\\' ){
        c = pParse->zJson[++j];
        if( c==0 ) return -1;
        bBackslash = 1;
      }else if( c=='"' ){
        break;
      }
      j++;
    }
    jsonParseAddNode(pParse, JSON_STRING, j+1-i, &pParse->zJson[i]);
    if( bBackslash ) pParse->aNode[pParse->nNode-1].bBackslash = 1;
    return j+1;
  }else if( c=='n'
         && strncmp(pParse->zJson+i,"null",4)==0
         && !isalnum(pParse->zJson[i+4]) ){
    jsonParseAddNode(pParse, JSON_NULL, 0, 0);
    return i+4;
  }else if( c=='t'
         && strncmp(pParse->zJson+i,"true",4)==0
         && !isalnum(pParse->zJson[i+4]) ){
    jsonParseAddNode(pParse, JSON_TRUE, 0, 0);
    return i+4;
  }else if( c=='f'
         && strncmp(pParse->zJson+i,"false",5)==0
         && !isalnum(pParse->zJson[i+5]) ){
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
** Search along zPath to find the node specified.  Return a pointer
** to that node, or NULL if zPath is malformed or if there is no such
** node.
*/
static JsonNode *jsonLookup(JsonNode *pRoot, const char *zPath){
  u32 i, j;
  if( zPath[0]==0 ) return pRoot;
  if( zPath[0]=='.' ){
    if( pRoot->eType!=JSON_OBJECT ) return 0;
    zPath++;
    for(i=0; isalnum(zPath[i]); i++){}
    if( i==0 ) return 0;
    j = 1;
    while( j<=pRoot->n ){
      if( pRoot[j].n==i+2
       && strncmp(&pRoot[j].zJContent[1],zPath,i)==0
      ){
        return jsonLookup(&pRoot[j+1], &zPath[i]);
      }
      j++;
      if( pRoot[j].eType==JSON_ARRAY || pRoot[j].eType==JSON_OBJECT ){
        j += pRoot[j].n;
      }
      j++;
    }
  }else if( zPath[0]=='[' && isdigit(zPath[1]) ){
    if( pRoot->eType!=JSON_ARRAY ) return 0;
    i = 0;
    zPath++;
    while( isdigit(zPath[0]) ){
      i = i + zPath[0] - '0';
      zPath++;
    }
    if( zPath[0]!=']' ) return 0;
    zPath++;
    j = 1;
    while( i>0 && j<=pRoot->n ){
      if( pRoot[j].eType==JSON_ARRAY || pRoot[j].eType==JSON_OBJECT ){
        j += pRoot[j].n;
      }
      j++;
      i--;
    }
    if( j<=pRoot->n ){
      return jsonLookup(&pRoot[j], zPath);
    }
  }
  return 0;
}

/****************************************************************************
** SQL functions used for testing and debugging
****************************************************************************/

/*
** The json_parse(JSON) function returns a string which describes
** a parse of the JSON provided.  Or it returns NULL if JSON is not
** well-formed.
*/
static void jsonParseFunc(
  sqlite3_context *context,
  int argc,
  sqlite3_value **argv
){
  Json s;       /* Output string - not real JSON */
  JsonParse x;  /* The parse */
  u32 i;
  char zBuf[50];

  assert( argc==1 );
  if( jsonParse(&x, (const char*)sqlite3_value_text(argv[0])) ) return;
  jsonInit(&s, context);
  for(i=0; i<x.nNode; i++){
    sqlite3_snprintf(sizeof(zBuf), zBuf, "node %u:\n", i);
    jsonAppend(&s, zBuf);
    sqlite3_snprintf(sizeof(zBuf), zBuf, "  type: %s\n",
                     jsonType[x.aNode[i].eType]);
    jsonAppend(&s, zBuf);
    if( x.aNode[i].eType>=JSON_INT ){
      sqlite3_snprintf(sizeof(zBuf), zBuf, "     n: %u\n", x.aNode[i].n);
      jsonAppend(&s, zBuf);
    }
    if( x.aNode[i].zJContent!=0 ){
      sqlite3_snprintf(sizeof(zBuf), zBuf, "  ofst: %u\n",
                       (u32)(x.aNode[i].zJContent - x.zJson));
      jsonAppend(&s, zBuf);
      jsonAppendRaw(&s, "  text: ", 8);
      jsonAppendRaw(&s, x.aNode[i].zJContent, x.aNode[i].n);
      jsonAppendRaw(&s, "\n", 1);
    }
  }
  sqlite3_free(x.aNode);
  jsonResult(&s);
}

/*
** The json_test1(JSON) function parses and rebuilds the JSON string.
*/
static void jsonTest1Func(
  sqlite3_context *context,
  int argc,
  sqlite3_value **argv
){
  JsonParse x;  /* The parse */
  if( jsonParse(&x, (const char*)sqlite3_value_text(argv[0])) ) return;
  jsonReturn(x.aNode, context);
  sqlite3_free(x.aNode);
}

/*
** The json_nodecount(JSON) function returns the number of nodes in the
** input JSON string.
*/
static void jsonNodeCountFunc(
  sqlite3_context *context,
  int argc,
  sqlite3_value **argv
){
  JsonParse x;  /* The parse */
  if( jsonParse(&x, (const char*)sqlite3_value_text(argv[0])) ) return;
  sqlite3_result_int64(context, x.nNode);
  sqlite3_free(x.aNode);
}

/****************************************************************************
** SQL function implementations
****************************************************************************/

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
** json_array_length(JSON)
** json_array_length(JSON, PATH)
**
** Return the number of elements in the top-level JSON array.  
** Return 0 if the input is not a well-formed JSON array.
*/
static void jsonArrayLengthFunc(
  sqlite3_context *context,
  int argc,
  sqlite3_value **argv
){
  JsonParse x;          /* The parse */
  sqlite3_int64 n = 0;
  u32 i;
  const char *zPath;

  if( argc==2 ){
    zPath = (const char*)sqlite3_value_text(argv[1]);
    if( zPath==0 ) return;
    if( zPath[0]!='$' ) return;
    zPath++;
  }else{
    zPath = 0;
  }
  if( jsonParse(&x, (const char*)sqlite3_value_text(argv[0]))==0 ){
    if( x.nNode ){
      JsonNode *pNode = x.aNode;
      if( zPath ) pNode = jsonLookup(pNode, zPath);
      if( pNode->eType==JSON_ARRAY ){
        for(i=1; i<=pNode->n; i++, n++){
          if( pNode[i].eType==JSON_ARRAY || pNode[i].eType==JSON_OBJECT ){
            i += pNode[i].n;
          }
        }
      }
    }
    sqlite3_free(x.aNode);
  }
  sqlite3_result_int64(context, n);
}

/*
** json_extract(JSON, PATH)
**
** Return the element described by PATH.  Return NULL if JSON is not
** valid JSON or if there is no PATH element or if PATH is malformed.
*/
static void jsonExtractFunc(
  sqlite3_context *context,
  int argc,
  sqlite3_value **argv
){
  JsonParse x;          /* The parse */
  JsonNode *pNode;
  const char *zPath;
  assert( argc==2 );
  zPath = (const char*)sqlite3_value_text(argv[1]);
  if( zPath==0 ) return;
  if( zPath[0]!='$' ) return;
  zPath++;
  if( jsonParse(&x, (const char*)sqlite3_value_text(argv[0])) ) return;
  pNode = jsonLookup(x.aNode, zPath);
  if( pNode ){
    jsonReturn(pNode, context);
  }
  sqlite3_free(x.aNode);
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
** json_type(JSON)
** json_type(JSON, PATH)
**
** Return the top-level "type" of a JSON string.  Return NULL if the
** input is not a well-formed JSON string.
*/
static void jsonTypeFunc(
  sqlite3_context *context,
  int argc,
  sqlite3_value **argv
){
  JsonParse x;          /* The parse */
  const char *zPath;

  if( argc==2 ){
    zPath = (const char*)sqlite3_value_text(argv[1]);
    if( zPath==0 ) return;
    if( zPath[0]!='$' ) return;
    zPath++;
  }else{
    zPath = 0;
  }
  if( jsonParse(&x, (const char*)sqlite3_value_text(argv[0])) ) return;
  if( x.nNode ){
    JsonNode *pNode = x.aNode;
    if( zPath ) pNode = jsonLookup(pNode, zPath);
    sqlite3_result_text(context, jsonType[pNode->eType], -1, SQLITE_STATIC);
  }
  sqlite3_free(x.aNode);
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
    { "json_array",          -1,    jsonArrayFunc         },
    { "json_array_length",    1,    jsonArrayLengthFunc   },
    { "json_array_length",    2,    jsonArrayLengthFunc   },
    { "json_extract",         2,    jsonExtractFunc       },
    { "json_object",         -1,    jsonObjectFunc        },
    { "json_type",            1,    jsonTypeFunc          },
    { "json_type",            2,    jsonTypeFunc          },

    /* DEBUG and TESTING functions */
    { "json_parse",           1,    jsonParseFunc     },
    { "json_test1",           1,    jsonTest1Func     },
    { "json_nodecount",       1,    jsonNodeCountFunc },
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

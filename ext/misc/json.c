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
  if( !p->bErr ){
    p->bErr = 1;
    sqlite3_result_error_nomem(p->pCtx);
    jsonReset(p);
  }
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

/* Append N bytes from zIn onto the end of the JsonString string.
*/
static void jsonAppendRaw(JsonString *p, const char *zIn, u32 N){
  if( (N+p->nUsed >= p->nAlloc) && jsonGrow(p,N)!=0 ) return;
  memcpy(p->zBuf+p->nUsed, zIn, N);
  p->nUsed += N;
}

#ifdef SQLITE_DEBUG
/* Append the zero-terminated string zIn
*/
static void jsonAppend(JsonString *p, const char *zIn){
  jsonAppendRaw(p, zIn, (u32)strlen(zIn));
}
#endif

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
      if( (p->nUsed+N+1-i > p->nAlloc) && jsonGrow(p,N+1-i)!=0 ) return;
      p->zBuf[p->nUsed++] = '\\';
    }
    p->zBuf[p->nUsed++] = c;
  }
  p->zBuf[p->nUsed++] = '"';
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
      jsonAppendString(p, z, n);
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
** Make the JsonNode the return value of the function.
*/
static void jsonReturn(
  JsonNode *pNode,            /* Node to return */
  sqlite3_context *pCtx,      /* Return value for this function */
  sqlite3_value **aReplace    /* Array of replacement values */
){
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
      double r = strtod(pNode->u.zJContent, 0);
      sqlite3_result_double(pCtx, r);
      break;
    }
    case JSON_INT: {
      sqlite3_int64 i = 0;
      const char *z = pNode->u.zJContent;
      if( z[0]=='-' ){ z++; }
      while( z[0]>='0' && z[0]<='9' ){ i = i*10 + *(z++) - '0'; }
      if( pNode->u.zJContent[0]=='-' ){ i = -i; }
      sqlite3_result_int64(pCtx, i);
      break;
    }
    case JSON_STRING: {
      if( pNode->jnFlags & JNODE_RAW ){
        sqlite3_result_text(pCtx, pNode->u.zJContent, pNode->n,
                            SQLITE_TRANSIENT);
      }else if( (pNode->jnFlags & JNODE_ESCAPE)==0 ){
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
      JsonString s;
      jsonInit(&s, pCtx);
      jsonRenderNode(pNode, &s, aReplace);
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
    pParse->aNode[pParse->nNode-1].jnFlags = jnFlags;
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
  if( aUp==0 ) return SQLITE_NOMEM;
  jsonParseFillInParentage(pParse, 0, 0);
  return SQLITE_OK;
}

/* forward declaration */
static JsonNode *jsonLookupAppend(JsonParse*,const char*,int*);

/*
** Search along zPath to find the node specified.  Return a pointer
** to that node, or NULL if zPath is malformed or if there is no such
** node.
**
** If pApnd!=0, then try to append new nodes to complete zPath if it is
** possible to do so and if no existing node corresponds to zPath.  If
** new nodes are appended *pApnd is set to 1.
*/
static JsonNode *jsonLookup(
  JsonParse *pParse,      /* The JSON to search */
  u32 iRoot,              /* Begin the search at this node */
  const char *zPath,      /* The path to search */
  int *pApnd              /* Append nodes to complete path if not NULL */
){
  u32 i, j, k, nKey;
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
      if( zPath[i] ) i++;
    }else{
      zKey = zPath;
      for(i=0; zPath[i] && zPath[i]!='.' && zPath[i]!='['; i++){}
      nKey = i;
    }
    if( nKey==0 ) return 0;
    j = 1;
    for(;;){
      while( j<=pRoot->n ){
        if( pRoot[j].n==nKey+2
         && strncmp(&pRoot[j].u.zJContent[1],zKey,nKey)==0
        ){
          return jsonLookup(pParse, iRoot+j+1, &zPath[i], pApnd);
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
      k = jsonParseAddNode(pParse, JSON_OBJECT, 2, 0);
      pRoot->u.iAppend = k - iRoot;
      pRoot->jnFlags |= JNODE_APPEND;
      k = jsonParseAddNode(pParse, JSON_STRING, i, zPath);
      if( !pParse->oom ) pParse->aNode[k].jnFlags |= JNODE_RAW;
      zPath += i;
      return jsonLookupAppend(pParse, zPath, pApnd);
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
    for(;;){
      while( i>0 && j<=pRoot->n ){
        j += jsonNodeSize(&pRoot[j]);
        i--;
      }
      if( (pRoot->jnFlags & JNODE_APPEND)==0 ) break;
      iRoot += pRoot->u.iAppend;
      pRoot = &pParse->aNode[iRoot];
      j = 1;
    }
    if( j<=pRoot->n ){
      return jsonLookup(pParse, iRoot+j, zPath, pApnd);
    }
    if( i==0 && pApnd ){
      k = jsonParseAddNode(pParse, JSON_ARRAY, 1, 0);
      pRoot->u.iAppend = k - iRoot;
      pRoot->jnFlags |= JNODE_APPEND;
      return jsonLookupAppend(pParse, zPath, pApnd);
    }
  }
  return 0;
}

/*
** Append content to pParse that will complete zPath.
*/
static JsonNode *jsonLookupAppend(
  JsonParse *pParse,     /* Append content to the JSON parse */
  const char *zPath,     /* Description of content to append */
  int *pApnd             /* Set this flag to 1 */
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
  return jsonLookup(pParse, pParse->nNode-1, zPath, pApnd);
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
  sqlite3_context *context,
  int argc,
  sqlite3_value **argv
){
  JsonString s;       /* Output string - not real JSON */
  JsonParse x;        /* The parse */
  u32 i;
  char zBuf[100];

  assert( argc==1 );
  if( jsonParse(&x, (const char*)sqlite3_value_text(argv[0])) ) return;
  jsonInit(&s, context);
  for(i=0; i<x.nNode; i++){
    sqlite3_snprintf(sizeof(zBuf), zBuf, "node %3u: %7s n=%d\n",
                     i, jsonType[x.aNode[i].eType], x.aNode[i].n);
    jsonAppend(&s, zBuf);
    if( x.aNode[i].u.zJContent!=0 ){
      jsonAppendRaw(&s, "    text: ", 10);
      jsonAppendRaw(&s, x.aNode[i].u.zJContent, x.aNode[i].n);
      jsonAppendRaw(&s, "\n", 1);
    }
  }
  jsonParseReset(&x);
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
  jsonReturn(x.aNode, context, 0);
  jsonParseReset(&x);
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
  jsonParseReset(&x);
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
  sqlite3_context *context,
  int argc,
  sqlite3_value **argv
){
  int i;
  JsonString jx;

  jsonInit(&jx, context);
  jsonAppendChar(&jx, '[');
  for(i=0; i<argc; i++){
    jsonAppendSeparator(&jx);
    jsonAppendValue(&jx, argv[i]);
  }
  jsonAppendChar(&jx, ']');
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
      if( zPath ) pNode = jsonLookup(&x, 0, zPath, 0);
      if( pNode->eType==JSON_ARRAY ){
        assert( (pNode->jnFlags & JNODE_APPEND)==0 );
        for(i=1; i<=pNode->n; n++){
          i += jsonNodeSize(&pNode[i]);
        }
      }
    }
    jsonParseReset(&x);
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
  pNode = jsonLookup(&x, 0, zPath, 0);
  if( pNode ){
    jsonReturn(pNode, context, 0);
  }
  jsonParseReset(&x);
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
  JsonString jx;
  const char *z;
  u32 n;

  if( argc&1 ){
    sqlite3_result_error(context, "json_object() requires an even number "
                                  "of arguments", -1);
    return;
  }
  jsonInit(&jx, context);
  jsonAppendChar(&jx, '{');
  for(i=0; i<argc; i+=2){
    if( sqlite3_value_type(argv[i])!=SQLITE_TEXT ){
      sqlite3_result_error(context, "json_object() labels must be TEXT", -1);
      jsonZero(&jx);
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
}


/*
** json_remove(JSON, PATH, ...)
**
** Remove the named elements from JSON and return the result.  Ill-formed
** PATH arguments are silently ignored.  If JSON is ill-formed, then NULL
** is returned.
*/
static void jsonRemoveFunc(
  sqlite3_context *context,
  int argc,
  sqlite3_value **argv
){
  JsonParse x;          /* The parse */
  JsonNode *pNode;
  const char *zPath;
  u32 i;

  if( argc<1 ) return;
  if( jsonParse(&x, (const char*)sqlite3_value_text(argv[0])) ) return;
  if( x.nNode ){
    for(i=1; i<argc; i++){
      zPath = (const char*)sqlite3_value_text(argv[i]);
      if( zPath==0 ) continue;
      if( zPath[0]!='$' ) continue;
      pNode = jsonLookup(&x, 0, &zPath[1], 0);
      if( pNode ) pNode->jnFlags |= JNODE_REMOVE;
    }
    if( (x.aNode[0].jnFlags & JNODE_REMOVE)==0 ){
      jsonReturn(x.aNode, context, 0);
    }
  }
  jsonParseReset(&x);
}

/*
** json_replace(JSON, PATH, VALUE, ...)
**
** Replace the value at PATH with VALUE.  If PATH does not already exist,
** this routine is a no-op.  If JSON is ill-formed, return NULL.
*/
static void jsonReplaceFunc(
  sqlite3_context *context,
  int argc,
  sqlite3_value **argv
){
  JsonParse x;          /* The parse */
  JsonNode *pNode;
  const char *zPath;
  u32 i;

  if( argc<1 ) return;
  if( (argc&1)==0 ) {
    sqlite3_result_error(context,
                         "json_replace() needs an odd number of arguments", -1);
    return;
  }
  if( jsonParse(&x, (const char*)sqlite3_value_text(argv[0])) ) return;
  if( x.nNode ){
    for(i=1; i<argc; i+=2){
      zPath = (const char*)sqlite3_value_text(argv[i]);
      if( zPath==0 ) continue;
      if( zPath[0]!='$' ) continue;
      pNode = jsonLookup(&x, 0, &zPath[1], 0);
      if( pNode ){
        pNode->jnFlags |= JNODE_REPLACE;
        pNode->iVal = i+1;
      }
    }
    if( x.aNode[0].jnFlags & JNODE_REPLACE ){
      sqlite3_result_value(context, argv[x.aNode[0].iVal]);
    }else{
      jsonReturn(x.aNode, context, argv);
    }
  }
  jsonParseReset(&x);
}

/*
** json_set(JSON, PATH, VALUE, ...)
**
** Set the value at PATH to VALUE.  Create the PATH if it does not already
** exist.  Overwrite existing values that do exist.
** If JSON is ill-formed, return NULL.
**
** json_insert(JSON, PATH, VALUE, ...)
**
** Create PATH and initialize it to VALUE.  If PATH already exists, this
** routine is a no-op.  If JSON is ill-formed, return NULL.
*/
static void jsonSetFunc(
  sqlite3_context *context,
  int argc,
  sqlite3_value **argv
){
  JsonParse x;          /* The parse */
  JsonNode *pNode;
  const char *zPath;
  u32 i;
  int bApnd;
  int bIsSet = *(int*)sqlite3_user_data(context);

  if( argc<1 ) return;
  if( (argc&1)==0 ) {
    sqlite3_result_error(context,
                         "json_set() needs an odd number of arguments", -1);
    return;
  }
  if( jsonParse(&x, (const char*)sqlite3_value_text(argv[0])) ) return;
  if( x.nNode ){
    for(i=1; i<argc; i+=2){
      zPath = (const char*)sqlite3_value_text(argv[i]);
      if( zPath==0 ) continue;
      if( zPath[0]!='$' ) continue;
      bApnd = 0;
      pNode = jsonLookup(&x, 0, &zPath[1], &bApnd);
      if( pNode && (bApnd || bIsSet) ){
        pNode->jnFlags |= JNODE_REPLACE;
        pNode->iVal = i+1;
      }
    }
    if( x.aNode[0].jnFlags & JNODE_REPLACE ){
      sqlite3_result_value(context, argv[x.aNode[0].iVal]);
    }else{
      jsonReturn(x.aNode, context, argv);
    }
  }
  jsonParseReset(&x);
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
    if( zPath ) pNode = jsonLookup(&x, 0, zPath, 0);
    sqlite3_result_text(context, jsonType[pNode->eType], -1, SQLITE_STATIC);
  }
  jsonParseReset(&x);
}

/****************************************************************************
** The json_each virtual table
****************************************************************************/
typedef struct JsonEachCursor JsonEachCursor;
struct JsonEachCursor {
  sqlite3_vtab_cursor base;  /* Base class - must be first */
  u32 iRowid;                /* The rowid */
  u32 i;                     /* Index in sParse.aNode[] of current row */
  u32 iEnd;                  /* EOF when i equals or exceeds this value */
  u8 eType;                  /* Type of top-level element */
  u8 bRecursive;             /* True for json_tree().  False for json_each() */
  char *zJson;               /* Input JSON */
  char *zPath;               /* Path by which to filter zJson */
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
#define JEACH_KEY    0
#define JEACH_VALUE  1
#define JEACH_TYPE   2
#define JEACH_ATOM   3
#define JEACH_ID     4
#define JEACH_PARENT 5
#define JEACH_JSON   6
#define JEACH_PATH   7

  rc = sqlite3_declare_vtab(db, 
     "CREATE TABLE x(key,value,type,atom,id,parent,json hidden,path hidden)");
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
  sqlite3_free(p->zPath);
  jsonParseReset(&p->sParse);
  p->iRowid = 0;
  p->i = 0;
  p->iEnd = 0;
  p->eType = 0;
  p->zJson = 0;
  p->zPath = 0;
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
static int jsonEachNextTree(sqlite3_vtab_cursor *cur){
  JsonEachCursor *p = (JsonEachCursor*)cur;
  if( p->i==0 ){
    p->i = 1;
  }else if( p->sParse.aNode[p->sParse.aUp[p->i]].eType==JSON_OBJECT ){
    p->i += 2;
  }else{
    p->i++;
  }
  p->iRowid++;
  if( p->i<p->sParse.nNode ){
    JsonNode *pUp = &p->sParse.aNode[p->sParse.aUp[p->i]];
    p->eType = pUp->eType;
    if( pUp->eType==JSON_ARRAY ) pUp->u.iKey++;
    if( p->sParse.aNode[p->i].eType==JSON_ARRAY ){
      p->sParse.aNode[p->i].u.iKey = 0;
    }
  }
  return SQLITE_OK;
}

/* Advance the cursor to the next element for json_each() */
static int jsonEachNextEach(sqlite3_vtab_cursor *cur){
  JsonEachCursor *p = (JsonEachCursor*)cur;
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
  return SQLITE_OK;
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
      if( p->eType==JSON_OBJECT ){
        jsonReturn(pThis, ctx, 0);
      }else if( p->eType==JSON_ARRAY ){
        u32 iKey;
        if( p->bRecursive ){
          if( p->iRowid==0 ) break;
          iKey = p->sParse.aNode[p->sParse.aUp[p->i]].u.iKey - 1;
        }else{
          iKey = p->iRowid;
        }
        sqlite3_result_int64(ctx, iKey);
      }
      break;
    }
    case JEACH_VALUE: {
      if( p->eType==JSON_OBJECT ) pThis++;
      jsonReturn(pThis, ctx, 0);
      break;
    }
    case JEACH_TYPE: {
      if( p->eType==JSON_OBJECT ) pThis++;
      sqlite3_result_text(ctx, jsonType[pThis->eType], -1, SQLITE_STATIC);
      break;
    }
    case JEACH_ATOM: {
      if( p->eType==JSON_OBJECT ) pThis++;
      if( pThis->eType>=JSON_ARRAY ) break;
      jsonReturn(pThis, ctx, 0);
      break;
    }
    case JEACH_ID: {
      sqlite3_result_int64(ctx, p->i + (p->eType==JSON_OBJECT));
      break;
    }
    case JEACH_PARENT: {
      if( p->i>0 && p->bRecursive ){
        sqlite3_result_int64(ctx, p->sParse.aUp[p->i]);
      }
      break;
    }
    case JEACH_PATH: {
      const char *zPath = p->zPath;
      if( zPath==0 ) zPath = "$";
      sqlite3_result_text(ctx, zPath, -1, SQLITE_STATIC);
      break;
    }
    default: {
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
** 1 if the constraint is found, 3 if the constraint and zPath are found,
** and 0 otherwise.
*/
static int jsonEachBestIndex(
  sqlite3_vtab *tab,
  sqlite3_index_info *pIdxInfo
){
  int i;
  int jsonIdx = -1;
  int pathIdx = -1;
  const struct sqlite3_index_constraint *pConstraint;
  pConstraint = pIdxInfo->aConstraint;
  for(i=0; i<pIdxInfo->nConstraint; i++, pConstraint++){
    if( pConstraint->usable==0 ) continue;
    if( pConstraint->op!=SQLITE_INDEX_CONSTRAINT_EQ ) continue;
    switch( pConstraint->iColumn ){
      case JEACH_JSON:   jsonIdx = i;    break;
      case JEACH_PATH:   pathIdx = i;    break;
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
    if( pathIdx<0 ){
      pIdxInfo->idxNum = 1;
    }else{
      pIdxInfo->aConstraintUsage[pathIdx].argvIndex = 2;
      pIdxInfo->aConstraintUsage[pathIdx].omit = 1;
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
  const char *zPath;
  sqlite3_int64 n;

  jsonEachCursorReset(p);
  if( idxNum==0 ) return SQLITE_OK;
  z = (const char*)sqlite3_value_text(argv[0]);
  if( z==0 ) return SQLITE_OK;
  if( idxNum&2 ){
    zPath = (const char*)sqlite3_value_text(argv[1]);
    if( zPath==0 || zPath[0]!='$' ) return SQLITE_OK;
  }
  n = sqlite3_value_bytes(argv[0]);
  p->zJson = sqlite3_malloc( n+1 );
  if( p->zJson==0 ) return SQLITE_NOMEM;
  memcpy(p->zJson, z, n+1);
  if( jsonParse(&p->sParse, p->zJson) 
   || (p->bRecursive && jsonParseFindParents(&p->sParse))
  ){
    jsonEachCursorReset(p);
  }else{
    JsonNode *pNode;
    if( idxNum==3 ){
      n = sqlite3_value_bytes(argv[1]);
      p->zPath = sqlite3_malloc( n+1 );
      if( p->zPath==0 ) return SQLITE_NOMEM;
      memcpy(p->zPath, zPath, n+1);
      pNode = jsonLookup(&p->sParse, 0, p->zPath+1, 0);
      if( pNode==0 ){
        jsonEachCursorReset(p);
        return SQLITE_OK;
      }
    }else{
      pNode = p->sParse.aNode;
    }
    p->i = (int)(pNode - p->sParse.aNode);
    p->eType = pNode->eType;
    if( p->eType>=JSON_ARRAY ){
      p->i++;
      p->iEnd = p->i + pNode->n;
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
  jsonEachNextEach,          /* xNext - advance a cursor */
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
  jsonEachNextTree,          /* xNext - advance a cursor */
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
};

/****************************************************************************
** The following routine is the only publically visible identifier in this
** file.  Call the following routine in order to register the various SQL
** functions and the virtual table implemented by this file.
****************************************************************************/

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
     int flag;
     void (*xFunc)(sqlite3_context*,int,sqlite3_value**);
  } aFunc[] = {
    { "json_array",          -1, 0,   jsonArrayFunc         },
    { "json_array_length",    1, 0,   jsonArrayLengthFunc   },
    { "json_array_length",    2, 0,   jsonArrayLengthFunc   },
    { "json_extract",         2, 0,   jsonExtractFunc       },
    { "json_insert",         -1, 0,   jsonSetFunc           },
    { "json_object",         -1, 0,   jsonObjectFunc        },
    { "json_remove",         -1, 0,   jsonRemoveFunc        },
    { "json_replace",        -1, 0,   jsonReplaceFunc       },
    { "json_set",            -1, 1,   jsonSetFunc           },
    { "json_type",            1, 0,   jsonTypeFunc          },
    { "json_type",            2, 0,   jsonTypeFunc          },

#if SQLITE_DEBUG
    /* DEBUG and TESTING functions */
    { "json_parse",           1, 0,   jsonParseFunc         },
    { "json_test1",           1, 0,   jsonTest1Func         },
    { "json_nodecount",       1, 0,   jsonNodeCountFunc     },
#endif
  };
  static const struct {
     const char *zName;
     sqlite3_module *pModule;
  } aMod[] = {
    { "json_each",            &jsonEachModule               },
    { "json_tree",            &jsonTreeModule               },
  };
  SQLITE_EXTENSION_INIT2(pApi);
  (void)pzErrMsg;  /* Unused parameter */
  for(i=0; i<sizeof(aFunc)/sizeof(aFunc[0]) && rc==SQLITE_OK; i++){
    rc = sqlite3_create_function(db, aFunc[i].zName, aFunc[i].nArg,
                                 SQLITE_UTF8 | SQLITE_DETERMINISTIC, 
                                 (void*)&aFunc[i].flag,
                                 aFunc[i].xFunc, 0, 0);
  }
  for(i=0; i<sizeof(aMod)/sizeof(aMod[0]) && rc==SQLITE_OK; i++){
    rc = sqlite3_create_module(db, aMod[i].zName, aMod[i].pModule, 0);
  }
  return rc;
}

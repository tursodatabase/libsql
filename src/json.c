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
** The original design stored all JSON as pure text, canonical RFC-8259.
** Support for JSON-5 extensions was added with version 3.42.0 (2023-05-16).
** All generated JSON text still conforms strictly to RFC-8259, but text
** with JSON-5 extensions is accepted as input.
**
** Beginning with version 3.44.0 (pending), these routines also accept
** BLOB values that have JSON encoded using a binary representation we
** call JSONB.  The name JSONB comes from PostgreSQL, however the on-disk
** format SQLite JSONB is completely different and incompatible with
** PostgreSQL JSONB.
**
** Decoding and interpreting JSONB is still O(N) where N is the size of
** the input, the same as text JSON.  However, the constant of proportionality
** for JSONB is much smaller due to faster parsing.  The size of each
** element in JSONB is encoded in its header, so there is no need to search
** for delimiters using persnickety syntax rules.  JSONB seems to be about
** 3x faster than text JSON as a result.  JSONB is also tends to be slightly
** smaller than text JSON, by 5% or 10%, but there are corner cases where
** JSONB can be slightly larger.  So you are not far mistaken to say that
** a JSONB blob is the same size as the equivalent RFC-8259 text.
**
**
** THE JSONB ENCODING:
**
** Every JSON element is encoded in JSONB as a header and a payload.
** The header is between 1 and 9 bytes in size.  The payload is zero
** or more bytes.
**
** The lower 4 bits of the first byte of the header determines the
** element type:
**
**    0:   NULL
**    1:   TRUE
**    2:   FALSE
**    3:   INT        -- RFC-8259 integer literal
**    4:   INT5       -- JSON5 integer literal
**    5:   FLOAT      -- RFC-8259 floating point literal
**    6:   FLOAT5     -- JSON5 floating point literal
**    7:   TEXT       -- Text literal acceptable to both SQL and JSON
**    8:   TEXTJ      -- Text containing RFC-8259 escapes
**    9:   TEXT5      -- Text containing JSON5 and/or RFC-8259 escapes
**   10:   TEXTRAW    -- Text containing unescaped syntax characters
**   11:   ARRAY
**   12:   OBJECT
**
** The other three possible values (13-15) are reserved for future
** enhancements.
**
** The upper 4 bits of the first byte determine the size of the header
** and sometimes also the size of the payload.  If X is the first byte
** of the element and if X>>4 is between 0 and 11, then the payload
** will be that many bytes in size and the header is exactly one byte
** in size.  Other four values for X>>4 (12-15) indicate that the header
** is more than one byte in size and that the payload size is determined
** by the remainder of the header, interpreted as a unsigned big-endian
** integer.
**
**   Value of X>>4         Size integer        Total header size
**   -------------     --------------------    -----------------
**        12           1 byte (0-255)                2
**        13           2 byte (0-65535)              3
**        14           4 byte (0-4294967295)         5
**        15           8 byte (0-1.8e19)             9
**
** The payload size need not be expressed in its minimal form.  For example,
** if the payload size is 10, the size can be expressed in any of 5 different
** ways: (1) (X>>4)==10, (2) (X>>4)==12 following by on 0x0a byte,
** (3) (X>>4)==13 followed by 0x00 and 0x0a, (4) (X>>4)==14 followed by
** 0x00 0x00 0x00 0x0a, or (5) (X>>4)==15 followed by 7 bytes of 0x00 and
** a single byte of 0x0a.  The shorter forms are preferred, of course, but
** sometimes when generating JSONB, the payload size is not known in advance
** and it is convenient to reserve sufficient header space to cover the
** largest possible payload size and then come back later and patch up
** the size when it becomes known, resulting in a non-minimal encoding.
**
** The value (X>>4)==15 is not actually used in the current implementation
** (as SQLite is currently unable handle BLOBs larger than about 2GB)
** but is included in the design to allow for future enhancements.
**
** The payload follows the header.  NULL, TRUE, and FALSE have no payload and
** their payload size must always be zero.  The payload for INT, INT5,
** FLOAT, FLOAT5, TEXT, TEXTJ, TEXT5, and TEXTROW is text.  Note that the
** "..." or '...' delimiters are omitted from the various text encodings.
** The payload for ARRAY and OBJECT is a list of additional elements that
** are the content for the array or object.  The payload for an OBJECT
** must be an even number of elements.  The first element of each pair is
** the label and must be of type TEXT, TEXTJ, TEXT5, or TEXTRAW.
**
** A valid JSONB blob consists of a single element, as described above.
** Usually this will be an ARRAY or OBJECT element which has many more
** elements as its content.  But the overall blob is just a single element.
**
** Input validation for JSONB blobs simply checks that the element type
** code is between 0 and 12 and that the total size of the element
** (header plus payload) is the same as the size of the BLOB.  If those
** checks are true, the BLOB is assumed to be JSONB and processing continues.
** Errors are only raised if some other miscoding is discovered during
** processing.
*/
#ifndef SQLITE_OMIT_JSON
#include "sqliteInt.h"

/* JSONB element types
*/
#define JSONB_NULL     0   /* "null" */
#define JSONB_TRUE     1   /* "true" */
#define JSONB_FALSE    2   /* "false" */
#define JSONB_INT      3   /* integer acceptable to JSON and SQL */
#define JSONB_INT5     4   /* integer in 0x000 notation */
#define JSONB_FLOAT    5   /* float acceptable to JSON and SQL */
#define JSONB_FLOAT5   6   /* float with JSON5 extensions */
#define JSONB_TEXT     7   /* Text compatible with both JSON and SQL */
#define JSONB_TEXTJ    8   /* Text with JSON escapes */
#define JSONB_TEXT5    9   /* Text with JSON-5 escape */
#define JSONB_TEXTRAW 10   /* SQL text that needs escaping for JSON */
#define JSONB_ARRAY   11   /* An array */
#define JSONB_OBJECT  12   /* An object */

/* Human-readalbe names for the JSONB values:
*/
static const char * const jsonbType[] = {
  "null", "true", "false", "integer", "integer", 
  "real", "real", "text",  "text",    "text",
  "text", "array", "object"
};

/*
** Growing our own isspace() routine this way is twice as fast as
** the library isspace() function, resulting in a 7% overall performance
** increase for the text-JSON parser.  (Ubuntu14.10 gcc 4.8.4 x64 with -Os).
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

/* Put code used only for testing inside the JSON_VVA() macro.
*/
#if !defined(SQLITE_DEBUG) && !defined(SQLITE_COVERAGE_TEST)
#  define JSON_VVA(X)
#else
#  define JSON_VVA(X) X
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
  u8 eErr;                 /* True if an error has been encountered */
  char zSpace[100];        /* Initial static space */
};

/* Allowed values for JsonString.eErr */
#define JSTRING_OOM         0x01   /* Out of memory */
#define JSTRING_MALFORMED   0x02   /* Malformed JSONB */
#define JSTRING_ERR         0x04   /* Error already sent to sqlite3_result */

/* A deferred cleanup task.  A list of JsonCleanup objects might be
** run when the JsonParse object is destroyed.
*/
struct JsonCleanup {
  JsonCleanup *pJCNext;    /* Next in a list */
  void (*xOp)(void*);      /* Routine to run */
  void *pArg;              /* Argument to xOp() */
};

/* JSON type values for JsonNode.eType
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

/* Human-readalbe names for the JsonNode types:
*/
static const char * const jsonType[] = {
  "subst",
  "null", "true", "false", "integer", "real", "text", "array", "object"
};

/* The "subtype" set for text JSON values passed through using
** sqlite3_result_subtype() and sqlite3_value_subtype().
*/
#define JSON_SUBTYPE  74    /* Ascii for "J" */

/* Bit values for the JsonNode.jnFlag field
*/
#define JNODE_RAW     0x01  /* Content is raw, not JSON encoded */
#define JNODE_ESCAPE  0x02  /* Content is text with \ escapes */
#define JNODE_REMOVE  0x04  /* Do not output */
#define JNODE_REPLACE 0x08  /* Target of a JSON_SUBST node */
#define JNODE_APPEND  0x10  /* More ARRAY/OBJECT entries at u.iAppend */
#define JNODE_LABEL   0x20  /* Is a label of an object */
#define JNODE_JSON5   0x40  /* Node contains JSON5 enhancements */

/*
** Bit values for the flags passed into jsonExtractFunc() or
** jsonSetFunc() via the user-data value.
*/
#define JSON_JSON      0x01        /* Result is always JSON */
#define JSON_SQL       0x02        /* Result is always SQL */
#define JSON_ABPATH    0x03        /* Allow abbreviated JSON path specs */
#define JSON_ISSET     0x04        /* json_set(), not json_insert() */
#define JSON_BLOB      0x08        /* Use the BLOB output format */


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
  JsonCleanup *pClup;/* Cleanup operations prior to freeing this object */
  u16 iDepth;        /* Nesting depth */
  u8 nErr;           /* Number of errors seen */
  u8 oom;            /* Set to true if out of memory */
  u8 bJsonIsRCStr;   /* True if zJson is an RCStr */
  u8 hasNonstd;      /* True if input uses non-standard features like JSON5 */
  u8 useMod;         /* Actually use the edits contain inside aNode */
  u8 hasMod;         /* aNode contains edits from the original zJson */
  u8 isBinary;       /* True if zJson is the binary encoding */
  u32 nJPRef;        /* Number of references to this object */
  int nJson;         /* Length of the zJson string in bytes */
  int nAlt;          /* Length of alternative JSON string zAlt, in bytes */
  u32 iErr;          /* Error location in zJson[] */
  u32 iSubst;        /* Last JSON_SUBST entry in aNode[] */
  u32 iHold;         /* Age of this entry in the cache for LRU replacement */
  /* Storage for the binary JSONB format */
  u32 nBlob;         /* Bytes of aBlob[] actually used */
  u32 nBlobAlloc;    /* Bytes allocated to aBlob[] */
  u8 *aBlob;         /* BLOB representation of zJson */
  /* Search and edit information.  See jsonLookupBlobStep() */
  u8 eEdit;          /* Edit operation to apply */
  int delta;         /* Size change due to the edit */
  u32 nIns;          /* Number of bytes to insert */
  u32 iLabel;        /* Location of label if search landed on an object value */
  u8 *aIns;          /* Content to be inserted */
};

/* Allowed values for JsonParse.eEdit */
#define JEDIT_DEL   1   /* Delete if exists */
#define JEDIT_REPL  2   /* Overwrite if exists */
#define JEDIT_INS   3   /* Insert if not exists */
#define JEDIT_SET   4   /* Insert or overwrite */

/*
** Maximum nesting depth of JSON for this implementation.
**
** This limit is needed to avoid a stack overflow in the recursive
** descent parser.  A depth of 1000 is far deeper than any sane JSON
** should go.  Historical note: This limit was 2000 prior to version 3.42.0
*/
#define JSON_MAX_DEPTH  1000

/**************************************************************************
** Forward references
**************************************************************************/
static void jsonReturnStringAsBlob(JsonString*);
static void jsonXlateNodeToBlob(JsonParse*,JsonNode*,JsonParse*);
static int jsonParseAddNode(JsonParse*,u32,u32,const char*);
static int jsonXlateBlobToNode(JsonParse *pParse, u32 i);
static int jsonFuncArgMightBeBinary(sqlite3_value *pJson);
static JsonNode *jsonLookupAppend(JsonParse*,const char*,int*,const char**);
static u32 jsonXlateBlobToText(JsonParse*,u32,JsonString*);

/**************************************************************************
** Utility routines for dealing with JsonString objects
**************************************************************************/

/* Turn uninitialized bulk memory into a valid JsonString object
** holding a zero-length string.
*/
static void jsonStringZero(JsonString *p){
  p->zBuf = p->zSpace;
  p->nAlloc = sizeof(p->zSpace);
  p->nUsed = 0;
  p->bStatic = 1;
}

/* Initialize the JsonString object
*/
static void jsonStringInit(JsonString *p, sqlite3_context *pCtx){
  p->pCtx = pCtx;
  p->eErr = 0;
  jsonStringZero(p);
}

/* Free all allocated memory and reset the JsonString object back to its
** initial state.
*/
static void jsonStringReset(JsonString *p){
  if( !p->bStatic ) sqlite3RCStrUnref(p->zBuf);
  jsonStringZero(p);
}

/* Report an out-of-memory (OOM) condition
*/
static void jsonStringOom(JsonString *p){
  p->eErr |= JSTRING_OOM;
  if( p->pCtx ) sqlite3_result_error_nomem(p->pCtx);
  jsonStringReset(p);
}

/* Enlarge pJson->zBuf so that it can hold at least N more bytes.
** Return zero on success.  Return non-zero on an OOM error
*/
static int jsonStringGrow(JsonString *p, u32 N){
  u64 nTotal = N<p->nAlloc ? p->nAlloc*2 : p->nAlloc+N+10;
  char *zNew;
  if( p->bStatic ){
    if( p->eErr ) return 1;
    zNew = sqlite3RCStrNew(nTotal);
    if( zNew==0 ){
      jsonStringOom(p);
      return SQLITE_NOMEM;
    }
    memcpy(zNew, p->zBuf, (size_t)p->nUsed);
    p->zBuf = zNew;
    p->bStatic = 0;
  }else{
    p->zBuf = sqlite3RCStrResize(p->zBuf, nTotal);
    if( p->zBuf==0 ){
      p->eErr |= JSTRING_OOM;
      jsonStringZero(p);
      return SQLITE_NOMEM;
    }
  }
  p->nAlloc = nTotal;
  return SQLITE_OK;
}

/* Append N bytes from zIn onto the end of the JsonString string.
*/
static SQLITE_NOINLINE void jsonStringExpandAndAppend(
  JsonString *p,
  const char *zIn,
  u32 N
){
  assert( N>0 );
  if( jsonStringGrow(p,N) ) return;
  memcpy(p->zBuf+p->nUsed, zIn, N);
  p->nUsed += N;
}
static void jsonAppendRaw(JsonString *p, const char *zIn, u32 N){
  if( N==0 ) return;
  if( N+p->nUsed >= p->nAlloc ){
    jsonStringExpandAndAppend(p,zIn,N);
  }else{
    memcpy(p->zBuf+p->nUsed, zIn, N);
    p->nUsed += N;
  }
}
static void jsonAppendRawNZ(JsonString *p, const char *zIn, u32 N){
  if( N==0 ) return;
  if( N+p->nUsed >= p->nAlloc ){
    jsonStringExpandAndAppend(p,zIn,N);
  }else{
    memcpy(p->zBuf+p->nUsed, zIn, N);
    p->nUsed += N;
  }
}


/* Append formatted text (not to exceed N bytes) to the JsonString.
*/
static void jsonPrintf(int N, JsonString *p, const char *zFormat, ...){
  va_list ap;
  if( (p->nUsed + N >= p->nAlloc) && jsonStringGrow(p, N) ) return;
  va_start(ap, zFormat);
  sqlite3_vsnprintf(N, p->zBuf+p->nUsed, zFormat, ap);
  va_end(ap);
  p->nUsed += (int)strlen(p->zBuf+p->nUsed);
}

/* Append a single character
*/
static SQLITE_NOINLINE void jsonAppendCharExpand(JsonString *p, char c){
  if( jsonStringGrow(p,1) ) return;
  p->zBuf[p->nUsed++] = c;
}
static void jsonAppendChar(JsonString *p, char c){
  if( p->nUsed>=p->nAlloc ){
    jsonAppendCharExpand(p,c);
  }else{
    p->zBuf[p->nUsed++] = c;
  }
}

/* Make sure there is a zero terminator on p->zBuf[]
*/
static void jsonStringTerminate(JsonString *p){
  if( p->nUsed<p->nAlloc || jsonStringGrow(p,1) ){
    p->zBuf[p->nUsed] = 0;
  }   
}

/* Try to force the string to be a zero-terminated RCStr string.
**
** Return true on success.  Return false if an OOM prevents this
** from happening.
*/
static int jsonForceRCStr(JsonString *p){
  jsonAppendChar(p, 0);
  if( p->eErr ) return 0;
  p->nUsed--;
  if( p->bStatic==0 ) return 1;
  p->nAlloc = 0;
  p->nUsed++;
  jsonStringGrow(p, p->nUsed);
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
  if( zIn==0 ) return;
  if( (N+p->nUsed+2 >= p->nAlloc) && jsonStringGrow(p,N+2)!=0 ) return;
  p->zBuf[p->nUsed++] = '"';
  for(i=0; i<N; i++){
    unsigned char c = ((unsigned const char*)zIn)[i];
    if( jsonIsOk[c] ){
      p->zBuf[p->nUsed++] = c;
    }else if( c=='"' || c=='\\' ){
      json_simple_escape:
      if( (p->nUsed+N+3-i > p->nAlloc) && jsonStringGrow(p,N+3-i)!=0 ) return;
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
      if( (p->nUsed+N+7+i > p->nAlloc) && jsonStringGrow(p,N+7-i)!=0 ) return;
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
  while( N>0 ){
    for(i=0; i<N && zIn[i]!='\\' && zIn[i]!='"'; i++){}
    if( i>0 ){
      jsonAppendRawNZ(p, zIn, i);
      if( i>=N ) break;
      zIn += i;
      N -= i;
    }
    if( N<2 ){
      p->eErr |= JSTRING_MALFORMED;
      break;
    }
    if( zIn[0]=='"' ){
      jsonAppendRawNZ(p, "\\\"", 2);
      zIn++;
      N--;
      continue;
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
        if( N<4 ){
          N = 2;
          p->eErr |= JSTRING_MALFORMED;
          break;
        }
        jsonAppendRawNZ(p, "\\u00", 4);
        jsonAppendRawNZ(p, &zIn[2], 2);
        zIn += 2;
        N -= 2;
        break;
      case '0':
        jsonAppendRawNZ(p, "\\u0000", 6);
        break;
      case '\r':
        if( N>2 && zIn[2]=='\n' ){
          zIn++;
          N--;
        }
        break;
      case '\n':
        break;
      case 0xe2:  /* \ followed by U+2028 or U+2029 line terminator ignored */
        if( N<4
         || 0x80!=(u8)zIn[2]
         || (0xa8!=(u8)zIn[3] && 0xa9!=(u8)zIn[3])
        ){
          N = 2;
          p->eErr |= JSTRING_MALFORMED;
          break;
        }
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
    assert( N>=2 );
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
  char *zBuf = sqlite3_malloc64( N+1 );
  if( zBuf==0 ){
    p->eErr |= JSTRING_OOM;
    return;
  }
  memcpy(zBuf, zIn, N);
  zBuf[N] = 0;
  zIn = zBuf;
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
  }else{
    assert( N>0 );
    jsonAppendRawNZ(p, zIn, N);
  }
  sqlite3_free(zBuf);
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
** Append an sqlite3_value (such as a function parameter) to the JSON
** string under construction in p.
*/
static void jsonAppendSqlValue(
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
      if( jsonFuncArgMightBeBinary(pValue) ){
        JsonParse px;
        memset(&px, 0, sizeof(px));
        px.aBlob = (u8*)sqlite3_value_blob(pValue);
        px.nBlob = sqlite3_value_bytes(pValue);
        jsonXlateBlobToText(&px, 0, p);
      }else if( p->eErr==0 ){
        sqlite3_result_error(p->pCtx, "JSON cannot hold BLOB values", -1);
        p->eErr = JSTRING_ERR;
        jsonStringReset(p);
      }
      break;
    }
  }
}

/* Make the text in p (which is probably a generated JSON text string)
** the result of the SQL function.
**
** The JsonString is reset.
*/
static void jsonReturnString(JsonString *p){
  if( p->eErr==0 ){
    int flags = SQLITE_PTR_TO_INT(sqlite3_user_data(p->pCtx));
    if( flags & JSON_BLOB ){
      jsonReturnStringAsBlob(p);
    }else if( p->bStatic ){
      sqlite3_result_text64(p->pCtx, p->zBuf, p->nUsed,
                            SQLITE_TRANSIENT, SQLITE_UTF8);
    }else if( jsonForceRCStr(p) ){
      sqlite3RCStrRef(p->zBuf);
      sqlite3_result_text64(p->pCtx, p->zBuf, p->nUsed,
                            sqlite3RCStrUnref,
                            SQLITE_UTF8);
    }else{
      sqlite3_result_error_nomem(p->pCtx);
    }
  }else if( p->eErr & JSTRING_OOM ){
    sqlite3_result_error_nomem(p->pCtx);
  }else if( p->eErr & JSTRING_MALFORMED ){
    sqlite3_result_error(p->pCtx, "malformed JSON", -1);
  }
  jsonStringReset(p);
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
  if( pParse->bJsonIsRCStr ){
    sqlite3RCStrUnref(pParse->zJson);
    pParse->zJson = 0;
    pParse->bJsonIsRCStr = 0;
  }
  if( pParse->zAlt ){
    sqlite3RCStrUnref(pParse->zAlt);
    pParse->zAlt = 0;
  }
  if( pParse->nBlobAlloc ){
    sqlite3_free(pParse->aBlob);
    pParse->aBlob = 0;
    pParse->nBlob = 0;
    pParse->nBlobAlloc = 0;
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
** Translate the JsonNode pNode into a pure JSON string and
** append that string on pOut.  Subsubstructure is also included.
*/
static void jsonXlateNodeToText(
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
        jsonAppendString(pOut, pNode->u.zJContent, pNode->n);
      }else if( pNode->jnFlags & JNODE_JSON5 ){
        jsonAppendNormalizedString(pOut, pNode->u.zJContent, pNode->n);
      }else{
        jsonAppendChar(pOut, '"');
        jsonAppendRawNZ(pOut, pNode->u.zJContent, pNode->n);
        jsonAppendChar(pOut, '"');
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
            jsonXlateNodeToText(pParse, &pNode[j], pOut);
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
        while( j<pNode->n ){
          if( (pNode[j+1].jnFlags & JNODE_REMOVE)==0 || pParse->useMod==0 ){
            jsonAppendSeparator(pOut);
            jsonXlateNodeToText(pParse, &pNode[j], pOut);
            jsonAppendChar(pOut, ':');
            jsonXlateNodeToText(pParse, &pNode[j+1], pOut);
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
** Make the return value of an SQL function be the JSON encoded by pNode.
**
** By default, the node is rendered as RFC-8259 JSON text (canonical
** JSON text without any JSON-5 enhancements).  However if the
** JSON_BLOB flag is set in the user-data for the function, then the
** node is rendered into the JSONB format and returned as a BLOB.
*/
static void jsonReturnNodeAsJson(
  JsonParse *pParse,          /* The complete JSON */
  JsonNode *pNode,            /* Node to return */
  sqlite3_context *pCtx,      /* Return value for this function */
  int bGenerateAlt,           /* Also store the rendered text in zAlt */
  int omitSubtype             /* Do not call sqlite3_result_subtype() */
){
  int flags;
  JsonString s;
  if( pParse->oom ){
    sqlite3_result_error_nomem(pCtx);
    return;
  }
  if( pParse->nErr ){
    return;
  }
  flags = SQLITE_PTR_TO_INT(sqlite3_user_data(pCtx));
  if( flags & JSON_BLOB ){
    JsonParse x;
    memset(&x, 0, sizeof(x));
    jsonXlateNodeToBlob(pParse, pNode, &x);
    if( x.oom ){
      sqlite3_result_error_nomem(pCtx);
      sqlite3_free(x.aBlob);
    }else{
      sqlite3_result_blob(pCtx, x.aBlob, x.nBlob, sqlite3_free);
    }
  }else{
    jsonStringInit(&s, pCtx);
    jsonXlateNodeToText(pParse, pNode, &s);
    if( bGenerateAlt && pParse->zAlt==0 && jsonForceRCStr(&s) ){
      pParse->zAlt = sqlite3RCStrRef(s.zBuf);
      pParse->nAlt = s.nUsed;
    }
    jsonReturnString(&s);
    if( !omitSubtype ) sqlite3_result_subtype(pCtx, JSON_SUBTYPE);
  }
}

/*
** Translate a single byte of Hex into an integer.
** This routine only works if h really is a valid hexadecimal
** character:  0..9a..fA..F
*/
static u8 jsonHexToInt(int h){
  if( !sqlite3Isxdigit(h) ) return 0;
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
  v = (jsonHexToInt(z[0])<<12)
    + (jsonHexToInt(z[1])<<8)
    + (jsonHexToInt(z[2])<<4)
    + jsonHexToInt(z[3]);
  return v;
}

/*
** Make the return value from an SQL function be the SQL value of
** JsonNode pNode.
**
** If pNode is an atom (not an array or object) then the value returned
** is a pure SQL value - an SQLITE_INTEGER, SQLITE_REAL, SQLITE_TEXT, or
** SQLITE_NULL.  However, if pNode is a JSON array or object, then the
** value returned is either RFC-8259 JSON text or a BLOB in the JSONB
** format, depending on the JSON_BLOB flag of the function user-data.
*/
static void jsonReturnFromNode(
  JsonParse *pParse,          /* Complete JSON parse tree */
  JsonNode *pNode,            /* Node to return */
  sqlite3_context *pCtx,      /* Return value for this function */
  int omitSubtype             /* Do not call sqlite3_result_subtype() */
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
      char *zz;
      sqlite3 *db = sqlite3_context_db_handle(pCtx);

      assert( pNode->eU==1 );
      zz = sqlite3DbStrNDup(db, pNode->u.zJContent, pNode->n);
      if( zz==0 ){
        sqlite3_result_error_nomem(pCtx);
        return;
      }
      z = zz;
      if( z[0]=='-' ){ z++; bNeg = 1; }
      else if( z[0]=='+' ){ z++; }
      rc = sqlite3DecOrHexToI64(z, &i);
      sqlite3DbFree(db, zz);
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
      sqlite3AtoF(z, &r, pNode->n, SQLITE_UTF8);
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
        sqlite3_result_text(pCtx, pNode->u.zJContent, pNode->n,
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
        for(i=0, j=0; i<n; i++){
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
      jsonReturnNodeAsJson(pParse, pNode, pCtx, 0, omitSubtype);
      break;
    }
  }
}

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
  JSON_VVA( p->eU = zContent ? 1 : 0 );
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
** Translate a single element of JSON text beginning at pParse->zJson[i] into
** its JsonNode representation.  Append the translation onto the 
** pParse->aNode[] array, which is increased in size as necessary.
** Return the pJson->zJson[] index of the first character past the end of
** the element that was parsed.
**
** Special return values:
**
**      0    End of input
**     -1    Syntax error
**     -2    '}' seen   \
**     -3    ']' seen    \___  For these returns, pParse->iErr is set to
**     -4    ',' seen    /     the index in zJson[] of the seen character
**     -5    ':' seen   /
*/
static int jsonXlateTextToNode(JsonParse *pParse, u32 i){
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
      x = jsonXlateTextToNode(pParse, j);
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
          jsonParseAddNode(pParse, JSON_STRING, k-j, &z[j]);
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
        x = jsonXlateTextToNode(pParse, j);
        if( x!=(-5) ){
          if( x!=(-1) ) pParse->iErr = j;
          return -1;
        }
        j = pParse->iErr+1;
      }
    parse_object_value:
      x = jsonXlateTextToNode(pParse, j);
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
        x = jsonXlateTextToNode(pParse, j);
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
    if( !pParse->oom ){
      pParse->aNode[iThis].n = pParse->nNode - (u32)iThis - 1;
    }
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
      x = jsonXlateTextToNode(pParse, j);
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
        x = jsonXlateTextToNode(pParse, j);
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
    jsonParseAddNode(pParse, JSON_STRING | (jnFlags<<8), j-1-i, &z[i+1]);
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
          if( ALWAYS(z[j-1]=='.')
           && ALWAYS(j-2>=i)
           && sqlite3Isdigit(z[j-2])
          ){
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
** Parse JSON (either pure RFC-8259 JSON text, or JSON-5 text, or a JSONB
** blob) into the JsonNode representation.
**
** Return 0 on success or non-zero if there are any errors.
** If an error occurs, free all memory held by pParse, but not pParse itself.
**
** pParse must be initialized with pParse->zJson set to the input text or
** blob prior to calling this routine.
*/
static int jsonParse(
  JsonParse *pParse,           /* Initialize and fill this JsonParse object */
  sqlite3_context *pCtx        /* Report errors here */
){
  int i;
  const char *zJson = pParse->zJson;
  if( pParse->isBinary ){
    pParse->aBlob = (u8*)pParse->zJson;
    pParse->nBlob = pParse->nJson;
    i = jsonXlateBlobToNode(pParse, 0);
  }else{
    i = jsonXlateTextToNode(pParse, 0);
  }
  if( pParse->oom ) i = -1;
  if( !pParse->isBinary && i>0 ){
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
  char *zJson;
  int nJson;
  JsonParse *p;
  JsonParse *pMatch = 0;
  int iKey;
  int iMinKey = 0;
  u32 iMinHold = 0xffffffff;
  u32 iMaxHold = 0;
  int bJsonRCStr;
  int isBinary;

  if( jsonFuncArgMightBeBinary(pJson) ){
    zJson = (char*)sqlite3_value_blob(pJson);
    isBinary = 1;
  }else{
    zJson = (char*)sqlite3_value_text(pJson);
    isBinary = 0;
  }
  nJson = sqlite3_value_bytes(pJson);

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
  bJsonRCStr = sqlite3ValueIsOfClass(pJson,sqlite3RCStrUnref);
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
    memcpy(p->zJson, zJson, nJson+(isBinary==0));
  }
  p->nJPRef = 1;
  p->isBinary = isBinary;
  p->nJson = nJson;
  if( jsonParse(p, pErrCtx) ){
    if( pErrCtx==0 ){
      p->nErr = 1;
      assert( p->nJPRef==1 ); /* Caller will own the new JsonParse object p */
      return p;
    }
    jsonParseFree(p);
    return 0;
  }
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
  if( pNode->eType!=JSON_STRING ) return 0;
  if( pNode->n!=nKey ) return 0;
  return strncmp(pNode->u.zJContent, zKey, nKey)==0;
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
        JSON_VVA( pRoot->eU = 2 );
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
            if( (pBase[j].jnFlags & JNODE_REMOVE)==0 || pParse->useMod==0 ){
              i++;
            }
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
      if( i==0 && j<=pRoot->n ) break;
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
        JSON_VVA( pRoot->eU = 2 );
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
** Compute the text of an error in JSON path syntax.
**
** If ctx is not NULL then push the error message into ctx and return NULL.
*  If ctx is NULL, then return the text of the error message.
*/
static char *jsonPathSyntaxError(const char *zErr, sqlite3_context *ctx){
  char *zMsg = sqlite3_mprintf("JSON path error near '%q'", zErr);
  if( ctx==0 ) return zMsg;
  if( zMsg==0 ){
    sqlite3_result_error_nomem(ctx);
  }else{
    sqlite3_result_error(ctx, zMsg, -1);
    sqlite3_free(zMsg);
  }
  return 0;
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
  jsonPathSyntaxError(zErr, pCtx);
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
** Utility routines for dealing with the binary BLOB representation of JSON
****************************************************************************/


/*
** Expand pParse->aBlob so that it holds at least N bytes.
**
** Return the number of errors.
*/
static int jsonBlobExpand(JsonParse *pParse, u32 N){
  u8 *aNew;
  u32 t;
  assert( N>pParse->nBlobAlloc );
  if( pParse->nBlobAlloc==0 ){
    t = 100;
  }else{
    t = pParse->nBlobAlloc*2;
  }
  if( t<N ) t = N+100;
  aNew = sqlite3_realloc64( pParse->aBlob, t );
  if( aNew==0 ){ pParse->oom = 1; return 1; }
  pParse->aBlob = aNew;
  pParse->nBlobAlloc = t;
  return 0;
}

/*
** If pParse->aBlob is not previously editable (because it is taken
** from sqlite3_value_blob(), as indicated by the fact that
** pParse->nBlobAlloc==0 and pParse->nBlob>0) then make it editable
** by making a copy into space obtained from malloc.
**
** Return true on success.  Return false on OOM.
*/
static int jsonBlobMakeEditable(JsonParse *pParse, u32 nExtra){
  u8 *aOld;
  u32 nSize;
  if( pParse->nBlobAlloc>0 ) return 1;
  aOld = pParse->aBlob;
  nSize = pParse->nBlob + pParse->nIns + nExtra;
  if( nSize>100 ) nSize -= 100;
  pParse->aBlob = 0;
  if( jsonBlobExpand(pParse, nSize) ){
    return 0;
  }
  assert( pParse->nBlobAlloc >= pParse->nBlob + pParse->nIns );
  memcpy(pParse->aBlob, aOld, pParse->nBlob);
  return 1;
}


/* Expand pParse->aBlob and append N bytes.
**
** Return the number of errors.
*/
static SQLITE_NOINLINE int jsonBlobExpandAndAppend(
  JsonParse *pParse,
  const u8 *aData,
  u32 N
){
  if( jsonBlobExpand(pParse, pParse->nBlob+N) ) return 1;
  memcpy(&pParse->aBlob[pParse->nBlob], aData, N);
  pParse->nBlob += N;
  return 0;
}

/* Append a single character.  Return 1 if an error occurs.
*/
static int jsonBlobAppendOneByte(JsonParse *pParse, u8 c){
  if( pParse->nBlob >= pParse->nBlobAlloc ){
    return jsonBlobExpandAndAppend(pParse, &c, 1);
  }
  pParse->aBlob[pParse->nBlob++] = c;
  return 0;
}

/* Append bytes.  Return 1 if an error occurs.
*/
static int jsonBlobAppendNBytes(JsonParse *pParse, const u8 *aData, u32 N){
  if( pParse->nBlob+N > pParse->nBlobAlloc ){
    return jsonBlobExpandAndAppend(pParse, aData, N);
  }
  memcpy(&pParse->aBlob[pParse->nBlob], aData, N);
  pParse->nBlob += N;
  return 0;
}

/* Append an node type byte together with the payload size.
*/
static void jsonBlobAppendNodeType(
  JsonParse *pParse,
  u8 eType,
  u32 szPayload
){
  u8 a[5];
  if( szPayload<=11 ){
    jsonBlobAppendOneByte(pParse, eType | (szPayload<<4));
  }else if( szPayload<=0xff ){
    a[0] = eType | 0xc0;
    a[1] = szPayload & 0xff;
    jsonBlobAppendNBytes(pParse, a, 2);
  }else if( szPayload<=0xffff ){
    a[0] = eType | 0xd0;
    a[1] = (szPayload >> 8) & 0xff;
    a[2] = szPayload & 0xff;
    jsonBlobAppendNBytes(pParse, a, 3);
  }else{
    a[0] = eType | 0xe0;
    a[1] = (szPayload >> 24) & 0xff;
    a[2] = (szPayload >> 16) & 0xff;
    a[3] = (szPayload >> 8) & 0xff;
    a[4] = szPayload & 0xff;
    jsonBlobAppendNBytes(pParse, a, 5);
  }
}

/* Change the payload size for the node at index i to be szPayload.
*/
static void jsonBlobChangePayloadSize(
  JsonParse *pParse,
  u32 i,
  u32 szPayload
){
  u8 *a;
  u8 szType;
  u8 nExtra;
  u8 nNeeded;
  i8 delta;
  if( pParse->oom ) return;
  a = &pParse->aBlob[i];
  szType = a[0]>>4;
  if( szType<=11 ){
    nExtra = 0;
  }else if( szType==12 ){
    nExtra = 1;
  }else if( szType==13 ){
    nExtra = 2;
  }else{
    nExtra = 4;
  }
  if( szPayload<=11 ){
    nNeeded = 0;
  }else if( szPayload<=0xff ){
    nNeeded = 1;
  }else if( szPayload<=0xffff ){
    nNeeded = 2;
  }else{
    nNeeded = 4;
  }
  delta = nNeeded - nExtra;
  if( delta ){
    u32 newSize = pParse->nBlob + delta;
    if( delta>0 ){
      if( newSize>pParse->nBlobAlloc && jsonBlobExpand(pParse, newSize) ){
        return;  /* OOM error.  Error state recorded in pParse->oom. */
      }
      a = &pParse->aBlob[i];
      memmove(&a[1+delta], &a[1], pParse->nBlob - (i+1));
    }else{
      memmove(&a[1], &a[1-delta], pParse->nBlob - (i+1-delta));
    }
    pParse->nBlob = newSize;
  }
  if( nNeeded==0 ){
    a[0] = (a[0] & 0x0f) | (szPayload<<4);
  }else if( nNeeded==1 ){
    a[0] = (a[0] & 0x0f) | 0xc0;
    a[1] = szPayload & 0xff;
  }else if( nNeeded==2 ){
    a[0] = (a[0] & 0x0f) | 0xd0;
    a[1] = (szPayload >> 8) & 0xff;
    a[2] = szPayload & 0xff;
  }else{
    a[0] = (a[0] & 0x0f) | 0xe0;
    a[1] = (szPayload >> 24) & 0xff;
    a[2] = (szPayload >> 16) & 0xff;
    a[3] = (szPayload >> 8) & 0xff;
    a[4] = szPayload & 0xff;
  }
}

/*
** If z[0] is 'u' and is followed by exactly 4 hexadecimal character,
** then set *pOp to JSONB_TEXTJ and return true.  If not, do not make
** any changes to *pOp and return false.
*/
static int jsonIs4HexB(const char *z, int *pOp){
  if( z[0]!='u' ) return 0;
  if( !sqlite3Isxdigit(z[1]) ) return 0;
  if( !sqlite3Isxdigit(z[2]) ) return 0;
  if( !sqlite3Isxdigit(z[3]) ) return 0;
  if( !sqlite3Isxdigit(z[4]) ) return 0;
  *pOp = JSONB_TEXTJ;
  return 1;
}

/*
** Translate a single element of JSON text at pParse->zJson[i] into
** its equivalent binary JSONB representation.  Append the translation into
** pParse->aBlob[] beginning at pParse->nBlob.  The size of
** pParse->aBlob[] is increased as necessary.
**
** Return the index of the first character past the end of the element parsed,
** or one of the following special result codes:
**
**      0    End of input
**     -1    Syntax error
**     -2    '}' seen   \
**     -3    ']' seen    \___  For these returns, pParse->iErr is set to
**     -4    ',' seen    /     the index in zJson[] of the seen character
**     -5    ':' seen   /
*/
static int jsonXlateTextToBlob(JsonParse *pParse, u32 i){
  char c;
  u32 j;
  u32 iThis, iStart;
  int x;
  u8 t;
  const char *z = pParse->zJson;
json_parse_restart:
  switch( (u8)z[i] ){
  case '{': {
    /* Parse object */
    iThis = pParse->nBlob;
    jsonBlobAppendNodeType(pParse, JSONB_OBJECT, (pParse->nJson-i)*2);
    if( ++pParse->iDepth > JSON_MAX_DEPTH ){
      pParse->iErr = i;
      return -1;
    }
    iStart = pParse->nBlob;
    for(j=i+1;;j++){
      u32 iBlob = pParse->nBlob;
      x = jsonXlateTextToBlob(pParse, j);
      if( x<=0 ){
        int op;
        if( x==(-2) ){
          j = pParse->iErr;
          if( pParse->nBlob!=(u32)iStart ) pParse->hasNonstd = 1;
          break;
        }
        j += json5Whitespace(&z[j]);
        op = JSONB_TEXT;
        if( sqlite3JsonId1(z[j]) 
         || (z[j]=='\\' && jsonIs4HexB(&z[j+1], &op))
        ){
          int k = j+1;
          while( (sqlite3JsonId2(z[k]) && json5Whitespace(&z[k])==0)
            || (z[k]=='\\' && jsonIs4HexB(&z[k+1], &op))
          ){
            k++;
          }
          assert( iBlob==pParse->nBlob );
          jsonBlobAppendNodeType(pParse, op, k-j);
          jsonBlobAppendNBytes(pParse, (const u8*)&z[j], k-j);
          pParse->hasNonstd = 1;
          x = k;
        }else{
          if( x!=-1 ) pParse->iErr = j;
          return -1;
        }
      }
      if( pParse->oom ) return -1;
      t = pParse->aBlob[iBlob] & 0x0f;
      if( t<JSONB_TEXT || t>JSONB_TEXTRAW ){
        pParse->iErr = j;
        return -1;
      }
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
        x = jsonXlateTextToBlob(pParse, j);
        if( x!=(-5) ){
          if( x!=(-1) ) pParse->iErr = j;
          return -1;
        }
        j = pParse->iErr+1;
      }
    parse_object_value:
      x = jsonXlateTextToBlob(pParse, j);
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
        x = jsonXlateTextToBlob(pParse, j);
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
    jsonBlobChangePayloadSize(pParse, iThis, pParse->nBlob - iStart);
    pParse->iDepth--;
    return j+1;
  }
  case '[': {
    /* Parse array */
    iThis = pParse->nBlob;
    jsonBlobAppendNodeType(pParse, JSONB_ARRAY, pParse->nJson - i);
    iStart = pParse->nBlob;
    if( pParse->oom ) return -1;
    if( ++pParse->iDepth > JSON_MAX_DEPTH ){
      pParse->iErr = i;
      return -1;
    }
    for(j=i+1;;j++){
      x = jsonXlateTextToBlob(pParse, j);
      if( x<=0 ){
        if( x==(-3) ){
          j = pParse->iErr;
          if( pParse->nBlob!=iStart ) pParse->hasNonstd = 1;
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
        x = jsonXlateTextToBlob(pParse, j);
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
    jsonBlobChangePayloadSize(pParse, iThis, pParse->nBlob - iStart);
    pParse->iDepth--;
    return j+1;
  }
  case '\'': {
    u8 opcode;
    char cDelim;
    int nn;
    pParse->hasNonstd = 1;
    opcode = JSONB_TEXT;
    goto parse_string;
  case '"':
    /* Parse string */
    opcode = JSONB_TEXT;
  parse_string:
    cDelim = z[i];
    nn = pParse->nJson;
    for(j=i+1; j<nn; j++){
      if( jsonIsOk[(unsigned char)z[j]] ) continue;
      c = z[j];
      if( c==cDelim ){
        break;
      }else if( c=='\\' ){
        c = z[++j];
        if( c=='"' || c=='\\' || c=='/' || c=='b' || c=='f'
           || c=='n' || c=='r' || c=='t'
           || (c=='u' && jsonIs4Hex(&z[j+1])) ){
          if( opcode==JSONB_TEXT ) opcode = JSONB_TEXTJ;
        }else if( c=='\'' || c=='0' || c=='v' || c=='\n'
           || (0xe2==(u8)c && 0x80==(u8)z[j+1]
                && (0xa8==(u8)z[j+2] || 0xa9==(u8)z[j+2]))
           || (c=='x' && jsonIs2Hex(&z[j+1])) ){
          opcode = JSONB_TEXT5;
          pParse->hasNonstd = 1;
        }else if( c=='\r' ){
          if( z[j+1]=='\n' ) j++;
          opcode = JSONB_TEXT5;
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
    jsonBlobAppendNodeType(pParse, opcode, j-1-i);
    jsonBlobAppendNBytes(pParse, (const u8*)&z[i+1], j-1-i);
    return j+1;
  }
  case 't': {
    if( strncmp(z+i,"true",4)==0 && !sqlite3Isalnum(z[i+4]) ){
      jsonBlobAppendOneByte(pParse, JSONB_TRUE);
      return i+4;
    }
    pParse->iErr = i;
    return -1;
  }
  case 'f': {
    if( strncmp(z+i,"false",5)==0 && !sqlite3Isalnum(z[i+5]) ){
      jsonBlobAppendOneByte(pParse, JSONB_FALSE);
      return i+5;
    }
    pParse->iErr = i;
    return -1;
  }
  case '+': {
    u8 seenE;
    pParse->hasNonstd = 1;
    t = 0x00;            /* Bit 0x01:  JSON5.   Bit 0x02:  FLOAT */
    goto parse_number;
  case '.':
    if( sqlite3Isdigit(z[i+1]) ){
      pParse->hasNonstd = 1;
      t = 0x03;          /* Bit 0x01:  JSON5.   Bit 0x02:  FLOAT */
      seenE = 0;
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
    t = 0x00;            /* Bit 0x01:  JSON5.   Bit 0x02:  FLOAT */
  parse_number:
    seenE = 0;
    assert( '-' < '0' );
    assert( '+' < '0' );
    assert( '.' < '0' );
    c = z[i];

    if( c<='0' ){
      if( c=='0' ){
        if( (z[i+1]=='x' || z[i+1]=='X') && sqlite3Isxdigit(z[i+2]) ){
          assert( t==0x00 );
          pParse->hasNonstd = 1;
          t = 0x01;
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
              jsonBlobAppendNodeType(pParse, JSONB_FLOAT, 6);
              jsonBlobAppendNBytes(pParse, (const u8*)"-9e999", 6);
            }else{
              jsonBlobAppendNodeType(pParse, JSONB_FLOAT, 5);
              jsonBlobAppendNBytes(pParse, (const u8*)"9e999", 5);
            }
            return i + (sqlite3StrNICmp(&z[i+4],"inity",5)==0 ? 9 : 4);
          }
          if( z[i+1]=='.' ){
            pParse->hasNonstd = 1;
            t |= 0x01;
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
            t |= 0x01;
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
        if( (t & 0x02)!=0 ){
          pParse->iErr = j;
          return -1;
        }
        t |= 0x02;
        continue;
      }
      if( c=='e' || c=='E' ){
        if( z[j-1]<'0' ){
          if( ALWAYS(z[j-1]=='.') && ALWAYS(j-2>=i) && sqlite3Isdigit(z[j-2]) ){
            pParse->hasNonstd = 1;
            t |= 0x01;
          }else{
            pParse->iErr = j;
            return -1;
          }
        }
        if( seenE ){
          pParse->iErr = j;
          return -1;
        }
        t |= 0x02;
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
        t |= 0x01;
      }else{
        pParse->iErr = j;
        return -1;
      }
    }
  parse_number_finish:
    assert( JSONB_INT+0x01==JSONB_INT5 );
    assert( JSONB_FLOAT+0x01==JSONB_FLOAT5 );
    assert( JSONB_INT+0x02==JSONB_FLOAT );
    if( z[i]=='+' ) i++;
    jsonBlobAppendNodeType(pParse, JSONB_INT+t, j-i);
    jsonBlobAppendNBytes(pParse, (const u8*)&z[i], j-i);
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
      jsonBlobAppendOneByte(pParse, JSONB_NULL);
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
      if( aNanInfName[k].eType==JSON_REAL ){
        jsonBlobAppendOneByte(pParse, JSONB_FLOAT | 0x50);
        jsonBlobAppendNBytes(pParse, (const u8*)"9e999", 5);
      }else{
        jsonBlobAppendOneByte(pParse, JSONB_NULL);
      }
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
static int jsonConvertTextToBlob(
  JsonParse *pParse,           /* Initialize and fill this JsonParse object */
  sqlite3_context *pCtx        /* Report errors here */
){
  int i;
  const char *zJson = pParse->zJson;
  i = jsonXlateTextToBlob(pParse, 0);
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

/*
** The input string pStr is a well-formed JSON text string.  Convert
** this into the JSONB format and make it the return value of the
** SQL function.
*/
static void jsonReturnStringAsBlob(JsonString *pStr){
  JsonParse px;
  memset(&px, 0, sizeof(px));
  jsonStringTerminate(pStr);
  px.zJson = pStr->zBuf;
  px.nJson = pStr->nUsed;
  (void)jsonXlateTextToBlob(&px, 0);
  if( px.oom ){
    sqlite3_free(px.aBlob);
    sqlite3_result_error_nomem(pStr->pCtx);
  }else{
    sqlite3_result_blob(pStr->pCtx, px.aBlob, px.nBlob, sqlite3_free);
  }
}

/* The byte at index i is a node type-code.  This routine
** determines the payload size for that node and writes that
** payload size in to *pSz.  It returns the offset from i to the
** beginning of the payload.  Return 0 on error.
*/
static u32 jsonbPayloadSize(JsonParse *pParse, u32 i, u32 *pSz){
  u8 x;
  u32 sz;
  u32 n;
  if( NEVER(i>pParse->nBlob) ){
    *pSz = 0;
    return 0;
  }
  x = pParse->aBlob[i]>>4;
  if( x<=11 ){
    sz = x;
    n = 1;
  }else if( x==12 ){
    if( i+1>=pParse->nBlob ){
      *pSz = 0;
      return 0;
    }
    sz = pParse->aBlob[i+1];
    n = 2;
  }else if( x==13 ){
    if( i+2>=pParse->nBlob ){
      *pSz = 0;
      return 0;
    }
    sz = (pParse->aBlob[i+1]<<8) + pParse->aBlob[i+2];
    n = 3;
  }else{
    if( i+4>=pParse->nBlob ){
      *pSz = 0;
      return 0;
    }
    sz = (pParse->aBlob[i+1]<<24) + (pParse->aBlob[i+2]<<16) +
         (pParse->aBlob[i+3]<<8) + pParse->aBlob[i+4];
    n = 5;
  }
  if( i+sz+n>pParse->nBlob ){
    sz = 0;
    n = 0;
  }
  *pSz = sz;
  return n;
}


/*
** Translate the binary JSONB representation of JSON beginning at
** pParse->aBlob[i] into a JSON text string.  Append the JSON
** text onto the end of pOut.  Return the index in pParse->aBlob[]
** of the first byte past the end of the element that is translated.
**
** If an error is detected in the BLOB input, the pOut->eErr flag
** might get set to JSTRING_MALFORMED.  But not all BLOB input errors
** are detected.  So a malformed JSONB input might either result
** in an error, or in incorrect JSON.
**
** The pOut->eErr JSTRING_OOM flag is set on a OOM.
*/
static u32 jsonXlateBlobToText(
  JsonParse *pParse,             /* the complete parse of the JSON */
  u32 i,                         /* Start rendering at this index */
  JsonString *pOut               /* Write JSON here */
){
  u32 sz, n, j, iEnd;

  n = jsonbPayloadSize(pParse, i, &sz);
  if( n==0 ){
    pOut->eErr |= JSTRING_MALFORMED;
    return pParse->nBlob+1;
  }
  switch( pParse->aBlob[i] & 0x0f ){
    case JSONB_NULL: {
      jsonAppendRawNZ(pOut, "null", 4);
      return i+1;
    }
    case JSONB_TRUE: {
      jsonAppendRawNZ(pOut, "true", 4);
      return i+1;
    }
    case JSONB_FALSE: {
      jsonAppendRawNZ(pOut, "false", 5);
      return i+1;
    }
    case JSONB_INT:
    case JSONB_FLOAT: {
      jsonAppendRaw(pOut, (const char*)&pParse->aBlob[i+n], sz);
      break;
    }
    case JSONB_INT5: {  /* Integer literal in hexadecimal notation */
      u32 k = 2;
      sqlite3_uint64 u = 0;
      const char *zIn = (const char*)&pParse->aBlob[i+n];
      int bOverflow = 0;
      if( zIn[0]=='-' ){
        jsonAppendChar(pOut, '-');
        k++;
      }else if( zIn[0]=='+' ){
        k++;
      }
      for(; k<sz; k++){
        if( !sqlite3Isxdigit(zIn[k]) ){
          pOut->eErr |= JSTRING_MALFORMED;
          break;
        }else if( (u>>60)!=0 ){
          bOverflow = 1;
        }else{
          u = u*16 + sqlite3HexToInt(zIn[k]);
        }
      }
      jsonPrintf(100,pOut,bOverflow?"9.0e999":"%llu", u);
      break;
    }
    case JSONB_FLOAT5: { /* Float literal missing digits beside "." */
      u32 k = 0;
      const char *zIn = (const char*)&pParse->aBlob[i+n];
      if( zIn[0]=='-' ){
        jsonAppendChar(pOut, '-');
        k++;
      }
      if( zIn[k]=='.' ){
        jsonAppendChar(pOut, '0');
      }
      for(; k<sz; k++){
        jsonAppendChar(pOut, zIn[k]);
        if( zIn[k]=='.' && (k+1==sz || !sqlite3Isdigit(zIn[k+1])) ){
          jsonAppendChar(pOut, '0');
        }
      }
      break;
    }
    case JSONB_TEXT:
    case JSONB_TEXTJ: {
      jsonAppendChar(pOut, '"');
      jsonAppendRaw(pOut, (const char*)&pParse->aBlob[i+n], sz);
      jsonAppendChar(pOut, '"');
      break;
    }
    case JSONB_TEXT5: {
      const char *zIn;
      u32 k;
      u32 sz2 = sz;
      zIn = (const char*)&pParse->aBlob[i+n];
      jsonAppendChar(pOut, '"');
      while( sz2>0 ){
        for(k=0; k<sz2 && zIn[k]!='\\'; k++){}
        if( k>0 ){
          jsonAppendRawNZ(pOut, zIn, k);
          if( k>=sz2 ){
            break;
          }
          zIn += k;
          sz2 -= k;
        }
        if( sz2<2 ){
          if( sz2>0 ) pOut->eErr |= JSTRING_MALFORMED;
          if( sz2==0 ) break;
        }
        assert( zIn[0]=='\\' );
        switch( (u8)zIn[1] ){
          case '\'':
            jsonAppendChar(pOut, '\'');
            break;
          case 'v':
            jsonAppendRawNZ(pOut, "\\u0009", 6);
            break;
          case 'x':
            if( sz2<2 ){
              pOut->eErr |= JSTRING_MALFORMED;
              sz2 = 0;
              break;
            }
            jsonAppendRawNZ(pOut, "\\u00", 4);
            jsonAppendRawNZ(pOut, &zIn[2], 2);
            zIn += 2;
            sz2 -= 2;
            break;
          case '0':
            jsonAppendRawNZ(pOut, "\\u0000", 6);
            break;
          case '\r':
            if( sz2>2 && zIn[2]=='\n' ){
              zIn++;
              sz2--;
            }
            break;
          case '\n':
            break;
          case 0xe2:
            /* '\' followed by either U+2028 or U+2029 is ignored as
            ** whitespace.  Not that in UTF8, U+2028 is 0xe2 0x80 0x29.
            ** U+2029 is the same except for the last byte */
            if( sz2<4
             || 0x80!=(u8)zIn[2]
             || (0xa8!=(u8)zIn[3] && 0xa9!=(u8)zIn[3])
            ){
              pOut->eErr |= JSTRING_MALFORMED;
              k = sz2;
              break;
            }
            zIn += 2;
            sz2 -= 2;
            break;
          default:
            jsonAppendRawNZ(pOut, zIn, 2);
            break;
        }
        if( sz2<2 ){
          sz2 = 0;
          pOut->eErr |= JSTRING_MALFORMED;
          break;
        }
        zIn += 2;
        sz2 -= 2;
      }
      jsonAppendChar(pOut, '"');
      break;
    }
    case JSONB_TEXTRAW: {
      jsonAppendString(pOut, (const char*)&pParse->aBlob[i+n], sz);
      break;
    }
    case JSONB_ARRAY: {
      jsonAppendChar(pOut, '[');
      j = i+n;
      iEnd = j+sz;
      while( j<iEnd ){
        j = jsonXlateBlobToText(pParse, j, pOut);
        jsonAppendChar(pOut, ',');
      }
      if( sz>0 ) pOut->nUsed--;
      jsonAppendChar(pOut, ']');
      break;
    }
    case JSONB_OBJECT: {
      int x = 0;
      jsonAppendChar(pOut, '{');
      j = i+n;
      iEnd = j+sz;
      while( j<iEnd ){
        j = jsonXlateBlobToText(pParse, j, pOut);
        jsonAppendChar(pOut, (x++ & 1) ? ',' : ':');
      }
      if( x & 1 ) pOut->eErr |= JSTRING_MALFORMED;
      if( sz>0 ) pOut->nUsed--;
      jsonAppendChar(pOut, '}');
      break;
    }

    default: {
      pOut->eErr |= JSTRING_MALFORMED;
      break;
    }
  }
  return i+n+sz;
}

/* Return true if the input pJson
**
** For performance reasons, this routine does not do a detailed check of the
** input BLOB to ensure that it is well-formed.  Hence, false positives are
** possible.  False negatives should never occur, however.
*/
static int jsonFuncArgMightBeBinary(sqlite3_value *pJson){
  u32 sz, n;
  const u8 *aBlob;
  int nBlob;
  JsonParse s;
  if( sqlite3_value_type(pJson)!=SQLITE_BLOB ) return 0;
  nBlob = sqlite3_value_bytes(pJson);
  if( nBlob<1 ) return 0;
  aBlob = sqlite3_value_blob(pJson);
  if( aBlob==0 || (aBlob[0] & 0x0f)>JSONB_OBJECT ) return 0;
  memset(&s, 0, sizeof(s));
  s.aBlob = (u8*)aBlob;
  s.nBlob = nBlob;
  n = jsonbPayloadSize(&s, 0, &sz);
  if( n==0 ) return 0;
  if( sz+n!=(u32)nBlob ) return 0;
  if( (aBlob[0] & 0x0f)<=JSONB_FALSE && sz>0 ) return 0;
  return sz+n==(u32)nBlob;
}

/* Translate a single element of JSONB into the JsonNode format.  The
** first byte of the element to be translated is at pParse->aBlob[i].
** Return the index in pParse->aBlob[] of the first byte past the end
** of the JSONB element.  Append the JsonNode translation in
** pParse->aNode[], which is increased in size as necessary.
*/
static int jsonXlateBlobToNode(JsonParse *pParse, u32 i){
  u8 t;     /* Node type */
  u32 sz;   /* Node size */
  u32 x;    /* Index of payload start */

  const char *zPayload;
  x = jsonbPayloadSize(pParse, i, &sz);
  if( x==0 ) return -1;
  t = pParse->zJson[i] & 0x0f;
  zPayload = &pParse->zJson[i+x];
  switch( t ){
    case JSONB_NULL: {
      if( sz>0 ) return -1;
      jsonParseAddNode(pParse, JSON_NULL, 0, 0);
      break;
    }
    case JSONB_TRUE: {
      if( sz>0 ) return -1;
      jsonParseAddNode(pParse, JSON_TRUE, 0, 0);
      break;
    }
    case JSONB_FALSE: {
      if( sz>0 ) return -1;
      jsonParseAddNode(pParse, JSON_FALSE, 0, 0);
      break;
    }
    case JSONB_INT: {
      if( sz==0 ) return -1;
      jsonParseAddNode(pParse, JSON_INT, sz, zPayload);
      break;
    }
    case JSONB_INT5: {
      if( sz==0 ) return -1;
      pParse->hasNonstd = 1;
      jsonParseAddNode(pParse, JSON_INT | (JNODE_JSON5<<8), sz, zPayload);
      break;
    }
    case JSONB_FLOAT: {
      if( sz==0 ) return -1;
      jsonParseAddNode(pParse, JSON_REAL, sz, zPayload);
      break;
    }
    case JSONB_FLOAT5: {
      if( sz==0 ) return -1;
      pParse->hasNonstd = 1;
      jsonParseAddNode(pParse, JSON_REAL | (JNODE_JSON5<<8), sz, zPayload);
      break;
    }
    case JSONB_TEXTRAW: {
      jsonParseAddNode(pParse, JSON_STRING | (JNODE_RAW<<8), sz, zPayload);
      break;
    }
    case JSONB_TEXT: {
      jsonParseAddNode(pParse, JSON_STRING, sz, zPayload);
      break;
    }
    case JSONB_TEXTJ: {
      jsonParseAddNode(pParse, JSON_STRING | (JNODE_ESCAPE<<8), sz, zPayload);
      break;
    }
    case JSONB_TEXT5: {
      pParse->hasNonstd = 1;
      jsonParseAddNode(pParse, JSON_STRING | ((JNODE_ESCAPE|JNODE_JSON5)<<8),
                       sz, zPayload);
      break;
    }
    case JSONB_ARRAY: {
      int iThis = jsonParseAddNode(pParse, JSON_ARRAY, 0, 0);
      u32 j = i+x;
      while( j<i+x+sz ){
        int r = jsonXlateBlobToNode(pParse, j);
        if( r<=0 ) return -1;
        j = (u32)r;
      }
      if( !pParse->oom ){
        pParse->aNode[iThis].n = pParse->nNode - (u32)iThis - 1;
      }
      break;
    }
    case JSONB_OBJECT: {
      int iThis = jsonParseAddNode(pParse, JSON_OBJECT, 0, 0);
      u32 j = i+x, k = 0;
      while( j<i+x+sz ){
        int r = jsonXlateBlobToNode(pParse, j);
        if( r<=0 ) return -1;
        if( (k++&1)==0 && !pParse->oom ){
          pParse->aNode[pParse->nNode-1].jnFlags |= JNODE_LABEL;
        }
        j = (u32)r;
      }
      if( !pParse->oom ){
        pParse->aNode[iThis].n = pParse->nNode - (u32)iThis - 1;
      }
      if( k&1 ) return -1;
      break;
    }
    default: {
      return -1;
    }
  }
  return i+x+sz;
}

/*
** Translate pNode (which is always a node found in pParse->aNode[]) into 
** the JSONB representation and append the translation onto the end of the
** pOut->aBlob[] array.
*/
static void jsonXlateNodeToBlob(
  JsonParse *pParse,             /* the complete parse of the JSON */
  JsonNode *pNode,               /* The node to render */
  JsonParse *pOut                /* Write the BLOB rendering of JSON here */
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
      jsonBlobAppendNodeType(pOut, JSONB_NULL, 0);
      break;
    }
    case JSON_TRUE: {
      jsonBlobAppendNodeType(pOut, JSONB_TRUE, 0);
      break;
    }
    case JSON_FALSE: {
      jsonBlobAppendNodeType(pOut, JSONB_FALSE, 0);
      break;
    }
    case JSON_STRING: {
      int op;
      assert( pNode->eU==1 );
      if( pNode->jnFlags & JNODE_RAW ){
        if( memchr(pNode->u.zJContent, '"', pNode->n)==0
         && memchr(pNode->u.zJContent, '\\', pNode->n)==0
        ){
          op = JSONB_TEXT;
        }else{
          op = JSONB_TEXTRAW;
        }
      }else if( pNode->jnFlags & JNODE_JSON5 ){
        op = JSONB_TEXT5;
      }else{
        op = JSONB_TEXTJ;
      }
      jsonBlobAppendNodeType(pOut, op, pNode->n);
      jsonBlobAppendNBytes(pOut, (const u8*)pNode->u.zJContent, pNode->n);
      break;
    }
    case JSON_REAL: {
      int op;
      assert( pNode->eU==1 );
      if( pNode->jnFlags & JNODE_JSON5 ){
        op = JSONB_FLOAT5;
      }else{
        assert( pNode->n>0 );
        op = JSONB_FLOAT;
      }
      jsonBlobAppendNodeType(pOut, op, pNode->n);
      jsonBlobAppendNBytes(pOut, (const u8*)pNode->u.zJContent, pNode->n);
      break;
    }
    case JSON_INT: {
      int op;
      assert( pNode->eU==1 );
      if( pNode->jnFlags & JNODE_JSON5 ){
        op = JSONB_INT5;
      }else{
        assert( pNode->n>0 );
        op = JSONB_INT;
      }
      jsonBlobAppendNodeType(pOut, op, pNode->n);
      jsonBlobAppendNBytes(pOut, (const u8*)pNode->u.zJContent, pNode->n);
      break;
    }
    case JSON_ARRAY: {
      u32 j = 1;
      u32 iStart, iThis = pOut->nBlob;
      jsonBlobAppendNodeType(pOut, JSONB_ARRAY, pParse->nJson*2);
      iStart = pOut->nBlob;
      for(;;){
        while( j<=pNode->n ){
          if( (pNode[j].jnFlags & JNODE_REMOVE)==0 || pParse->useMod==0 ){
            jsonXlateNodeToBlob(pParse, &pNode[j], pOut);
          }
          j += jsonNodeSize(&pNode[j]);
        }
        if( (pNode->jnFlags & JNODE_APPEND)==0 ) break;
        if( pParse->useMod==0 ) break;
        assert( pNode->eU==2 );
        pNode = &pParse->aNode[pNode->u.iAppend];
        j = 1;
      }
      jsonBlobChangePayloadSize(pOut, iThis, pOut->nBlob - iStart);
      break;
    }
    case JSON_OBJECT: {
      u32 j = 1;
      u32 iStart, iThis = pOut->nBlob;
      jsonBlobAppendNodeType(pOut, JSONB_OBJECT, pParse->nJson*2);
      iStart = pOut->nBlob;
      for(;;){
        while( j<=pNode->n ){
          if( (pNode[j+1].jnFlags & JNODE_REMOVE)==0 || pParse->useMod==0 ){
            jsonXlateNodeToBlob(pParse, &pNode[j], pOut);
            jsonXlateNodeToBlob(pParse, &pNode[j+1], pOut);
          }
          j += 1 + jsonNodeSize(&pNode[j+1]);
        }
        if( (pNode->jnFlags & JNODE_APPEND)==0 ) break;
        if( pParse->useMod==0 ) break;
        assert( pNode->eU==2 );
        pNode = &pParse->aNode[pNode->u.iAppend];
        j = 1;
      }
      jsonBlobChangePayloadSize(pOut, iThis, pOut->nBlob - iStart);
      break;
    }
  }
}

/*
** Given that a JSONB_ARRAY object starts at offset i, return
** the number of entries in that array.
*/
static u32 jsonbArrayCount(JsonParse *pParse, u32 iRoot){
  u32 n, sz, i, iEnd;
  u32 k = 0;
  n = jsonbPayloadSize(pParse, iRoot, &sz);
  iEnd = iRoot+n+sz;
  for(i=iRoot+n; n>0 && i<iEnd; i+=sz+n, k++){
    n = jsonbPayloadSize(pParse, i, &sz);
  }
  return k;
}

/*
** Edit the size of the element at iRoot by the amount in pParse->delta.
*/
static void jsonAfterEditSizeAdjust(JsonParse *pParse, u32 iRoot){
  u32 sz = 0;
  u32 nBlob;
  assert( pParse->delta!=0 );
  assert( pParse->nBlobAlloc >= pParse->nBlob );
  nBlob = pParse->nBlob;
  pParse->nBlob = pParse->nBlobAlloc;
  (void)jsonbPayloadSize(pParse, iRoot, &sz);
  pParse->nBlob = nBlob;
  sz += pParse->delta;
  jsonBlobChangePayloadSize(pParse, iRoot, sz);
}

/*
** Modify the JSONB blob at pParse->aBlob by removing nDel bytes of
** content beginning at iDel, and replacing them with nIns bytes of
** content given by aIns.
**
** nDel may be zero, in which case no bytes are removed.  But iDel is
** still important as new bytes will be insert beginning at iDel.
**
** nIns may be zero, in which case no new bytes are inserted.  aIns might
** be a NULL pointer in this case.
**
** Set pParse->oom if an OOM occurs.
*/
static void jsonBlobEdit(
  JsonParse *pParse,     /* The JSONB to be modified is in pParse->aBlob */
  u32 iDel,              /* First byte to be removed */
  u32 nDel,              /* Number of bytes to remove */
  const u8 *aIns,        /* Content to insert */
  u32 nIns               /* Bytes of content to insert */
){
  i64 d = (i64)nIns - (i64)nDel;
  if( d!=0 ){
    if( pParse->nBlob + d > pParse->nBlobAlloc ){
      jsonBlobExpand(pParse, pParse->nBlob+d);
      if( pParse->oom ) return;
    }
    memmove(&pParse->aBlob[iDel+nIns],
            &pParse->aBlob[iDel+nDel],
            pParse->nBlob - (iDel+nDel));
    pParse->nBlob += d;
    pParse->delta += d;
  }
  if( nIns ) memcpy(&pParse->aBlob[iDel], aIns, nIns);
}

/*
** Error returns from jsonLookupBlobStep()
*/
#define JSON_BLOB_ERROR      0xffffffff
#define JSON_BLOB_NOTFOUND   0xfffffffe
#define JSON_BLOB_PATHERROR  0xfffffffd
#define JSON_BLOB_ISERROR(x) ((x)>=JSON_BLOB_PATHERROR)

/*
** Search along zPath to find the Json element specified.  Return an
** index into pParse->aBlob[] for the start of that element's value.
**
** Return JSON_BLOB_NOTFOUND if no such element exists.
*/
static u32 jsonLookupBlobStep(
  JsonParse *pParse,      /* The JSON to search */
  u32 iRoot,              /* Begin the search at this element of aBlob[] */
  const char *zPath,      /* The path to search */
  u32 iLabel              /* Label if iRoot is a value of in an object */
){
  u32 i, j, k, nKey, sz, n, iEnd, rc;
  const char *zKey;
  u8 x;

  if( zPath[0]==0 ){
    if( pParse->eEdit && jsonBlobMakeEditable(pParse, 0) ){
      n = jsonbPayloadSize(pParse, iRoot, &sz);
      sz += n;
      if( pParse->eEdit==JEDIT_DEL ){
        if( iLabel>0 ){
          sz += iRoot - iLabel;
          iRoot = iLabel;
        }
        jsonBlobEdit(pParse, iRoot, sz, 0, 0);
      }else if( pParse->eEdit==JEDIT_INS ){
        /* Already exists, so json_insert() is a no-op */
      }else{
        /* json_set() or json_replace() */
        jsonBlobEdit(pParse, iRoot, sz, pParse->aIns, pParse->nIns);
      }
    }
    pParse->iLabel = iLabel;
    return iRoot;
  }
  if( zPath[0]=='.' ){
    x = pParse->aBlob[iRoot];
    zPath++;
    if( zPath[0]=='"' ){
      zKey = zPath + 1;
      for(i=1; zPath[i] && zPath[i]!='"'; i++){}
      nKey = i-1;
      if( zPath[i] ){
        i++;
      }else{
        return JSON_BLOB_PATHERROR;
      }
      testcase( nKey==0 );
    }else{
      zKey = zPath;
      for(i=0; zPath[i] && zPath[i]!='.' && zPath[i]!='['; i++){}
      nKey = i;
      if( nKey==0 ){
        return JSON_BLOB_PATHERROR;
      }
    }
    if( (x & 0x0f)!=JSONB_OBJECT ) return JSON_BLOB_NOTFOUND;
    n = jsonbPayloadSize(pParse, iRoot, &sz);
    j = iRoot + n;  /* j is the index of a label */
    iEnd = j+sz;
    while( j<iEnd ){
      x = pParse->aBlob[j] & 0x0f;
      if( x<JSONB_TEXT || x>JSONB_TEXTRAW ) return JSON_BLOB_ERROR;
      n = jsonbPayloadSize(pParse, j, &sz);
      if( n==0 ) return JSON_BLOB_ERROR;
      k = j+n;  /* k is the index of the label text */
      if( k+sz>=iEnd ) return JSON_BLOB_ERROR;
      if( sz==nKey && memcmp(&pParse->aBlob[k], zKey, nKey)==0 ){
        u32 v = k+sz;  /* v is the index of the value */
        if( ((pParse->aBlob[v])&0x0f)>JSONB_OBJECT ) return JSON_BLOB_ERROR;
        n = jsonbPayloadSize(pParse, v, &sz);
        if( n==0 || v+n+sz>iEnd ) return JSON_BLOB_ERROR;
        assert( j>0 );
        rc = jsonLookupBlobStep(pParse, v, &zPath[i], j);
        if( pParse->delta ) jsonAfterEditSizeAdjust(pParse, iRoot);
        return rc;
      }
      j = k+sz;
      if( ((pParse->aBlob[j])&0x0f)>JSONB_OBJECT ) return JSON_BLOB_ERROR;
      n = jsonbPayloadSize(pParse, j, &sz);
      if( n==0 ) return JSON_BLOB_ERROR;
      j += n+sz;
    }
    if( j>iEnd ) return JSON_BLOB_ERROR;
    if( pParse->eEdit>=JEDIT_INS ){
      u32 nIns;
      u8 aLabel[16];
      JsonParse ix;
      testcase( pParse->eEdit==JEDIT_INS );
      testcase( pParse->eEdit==JEDIT_SET );
      memset(&ix, 0, sizeof(ix));
      ix.aBlob = aLabel;
      ix.nBlobAlloc = sizeof(aLabel);
      jsonBlobAppendNodeType(&ix,JSONB_TEXTRAW, nKey);
      if( jsonBlobMakeEditable(pParse, ix.nBlob+nKey) ){
        /* This is similar to jsonBlobEdit() except that the inserted
        ** bytes come from two different places, ix.aBlob and pParse->aBlob. */
        nIns = ix.nBlob + nKey + pParse->nIns;
        assert( pParse->nBlob + pParse->nIns <= pParse->nBlobAlloc );
        memmove(&pParse->aBlob[j+nIns], &pParse->aBlob[j],
                pParse->nBlob - j);
        memcpy(&pParse->aBlob[j], ix.aBlob, ix.nBlob);
        k = j + ix.nBlob;
        memcpy(&pParse->aBlob[k], zKey, nKey);
        k += nKey;
        memcpy(&pParse->aBlob[k], pParse->aIns, pParse->nIns);
        pParse->delta = nIns;
        pParse->nBlob += nIns;
        jsonAfterEditSizeAdjust(pParse, iRoot);
      }
      return j;
    }
  }else if( zPath[0]=='[' ){
    x = pParse->aBlob[iRoot] & 0x0f;
    if( x!=JSONB_ARRAY )  return JSON_BLOB_NOTFOUND;
    n = jsonbPayloadSize(pParse, iRoot, &sz);
    k = 0;
    i = 1;
    while( sqlite3Isdigit(zPath[i]) ){
      k = k*10 + zPath[i] - '0';
      i++;
    }
    if( i<2 || zPath[i]!=']' ){
      if( zPath[1]=='#' ){
        k = jsonbArrayCount(pParse, iRoot);
        i = 2;
        if( zPath[2]=='-' && sqlite3Isdigit(zPath[3]) ){
          unsigned int nn = 0;
          i = 3;
          do{
            nn = nn*10 + zPath[i] - '0';
            i++;
          }while( sqlite3Isdigit(zPath[i]) );
          if( nn>k ) return JSON_BLOB_NOTFOUND;
          k -= nn;
        }
        if( zPath[i]!=']' ){
          return JSON_BLOB_PATHERROR;
        }
      }else{
        return JSON_BLOB_PATHERROR;
      }
    }
    j = iRoot+n;
    iEnd = j+sz;
    while( j<iEnd ){
      if( k==0 ){
        rc = jsonLookupBlobStep(pParse, j, &zPath[i+1], 0);
        if( pParse->delta ) jsonAfterEditSizeAdjust(pParse, iRoot);
        return rc;
      }
      k--;
      n = jsonbPayloadSize(pParse, j, &sz);
      if( n==0 ) return JSON_BLOB_ERROR;
      j += n+sz;
    }
    if( j>iEnd ) return JSON_BLOB_ERROR;
    if( k>1 ) return JSON_BLOB_NOTFOUND;
    if( pParse->eEdit>=JEDIT_INS && jsonBlobMakeEditable(pParse, 0) ){
      testcase( pParse->eEdit==JEDIT_INS );
      testcase( pParse->eEdit==JEDIT_SET );
      jsonBlobEdit(pParse, j, 0, pParse->aIns, pParse->nIns);
      jsonAfterEditSizeAdjust(pParse, iRoot);
      return j;
    }
  }else{
    return JSON_BLOB_PATHERROR; 
  }
  return JSON_BLOB_NOTFOUND;
}

/*
** Convert a JSON BLOB into text and make that text the return value
** of an SQL function.
*/
static void jsonReturnTextJsonFromBlob(
  sqlite3_context *ctx,
  const u8 *aBlob,
  u32 nBlob
){
  JsonParse x;
  JsonString s;

  if( aBlob==0 ) return;
  memset(&x, 0, sizeof(x));
  x.aBlob = (u8*)aBlob;
  x.nBlob = nBlob;
  jsonStringInit(&s, ctx);
  jsonXlateBlobToText(&x, 0, &s);
  jsonReturnString(&s);
}


/*
** Return the value of the BLOB node at index i.
**
** If the value is a primitive, return it as an SQL value.
** If the value is an array or object, return it as either
** JSON text or the BLOB encoding, depending on the JSON_B flag
** on the userdata.
*/
static void jsonReturnFromBlob(
  JsonParse *pParse,          /* Complete JSON parse tree */
  u32 i,                      /* Index of the node */
  sqlite3_context *pCtx,      /* Return value for this function */
  int textOnly                /* return text JSON.  Disregard user-data */
){
  u32 n, sz;
  int rc;
  sqlite3 *db = sqlite3_context_db_handle(pCtx);

  n = jsonbPayloadSize(pParse, i, &sz);
  if( n==0 ) return;
  switch( pParse->aBlob[i] & 0x0f ){
    case JSONB_NULL: {
      sqlite3_result_null(pCtx);
      break;
    }
    case JSONB_TRUE: {
      sqlite3_result_int(pCtx, 1);
      break;
    }
    case JSONB_FALSE: {
      sqlite3_result_int(pCtx, 0);
      break;
    }
    case JSONB_INT5:
    case JSONB_INT: {
      sqlite3_int64 iRes = 0;
      char *z;
      int bNeg = 0;
      char x = (char)pParse->aBlob[i+n];
      if( x=='-' && ALWAYS(sz>0) ){ n++; sz--; bNeg = 1; }
      z = sqlite3DbStrNDup(db, (const char*)&pParse->aBlob[i+n], (int)sz);
      if( z==0 ) return;
      rc = sqlite3DecOrHexToI64(z, &iRes);
      sqlite3DbFree(db, z);
      if( rc<=1 ){
        sqlite3_result_int64(pCtx, bNeg ? -iRes : iRes);
      }else if( rc==3 && bNeg ){
        sqlite3_result_int64(pCtx, SMALLEST_INT64);
      }else{
        if( bNeg ){ n--; sz++; }
        goto to_double;
      }
      break;
    }
    case JSONB_FLOAT5:
    case JSONB_FLOAT: {
      double r;
      char *z;
    to_double:
      z = sqlite3DbStrNDup(db, (const char*)&pParse->aBlob[i+n], (int)sz);
      if( z==0 ) return;
      sqlite3AtoF(z, &r, sqlite3Strlen30(z), SQLITE_UTF8);
      sqlite3DbFree(db, z);
      sqlite3_result_double(pCtx, r);
      break;
    }
    case JSONB_TEXTRAW:
    case JSONB_TEXT: {
      sqlite3_result_text(pCtx, (char*)&pParse->aBlob[i+n], sz,
                          SQLITE_TRANSIENT);
      break;
    }
    case JSONB_TEXT5:
    case JSONB_TEXTJ: {
      /* Translate JSON formatted string into raw text */
      u32 iIn, iOut;
      const char *z;
      char *zOut;
      u32 nOut = sz;
      z = (const char*)&pParse->aBlob[i+n];
      zOut = sqlite3_malloc( nOut+1 );
      if( zOut==0 ){
        sqlite3_result_error_nomem(pCtx);
        break;
      }
      for(iIn=iOut=0; iIn<sz; iIn++){
        char c = z[iIn];
        if( c=='\\' ){
          c = z[++iIn];
          if( c=='u' ){
            u32 v = jsonHexToInt4(z+iIn+1);
            iIn += 4;
            if( v==0 ) break;
            if( v<=0x7f ){
              zOut[iOut++] = (char)v;
            }else if( v<=0x7ff ){
              zOut[iOut++] = (char)(0xc0 | (v>>6));
              zOut[iOut++] = 0x80 | (v&0x3f);
            }else{
              u32 vlo;
              if( (v&0xfc00)==0xd800
                && i<n-6
                && z[iIn+1]=='\\'
                && z[iIn+2]=='u'
                && ((vlo = jsonHexToInt4(z+iIn+3))&0xfc00)==0xdc00
              ){
                /* We have a surrogate pair */
                v = ((v&0x3ff)<<10) + (vlo&0x3ff) + 0x10000;
                iIn += 6;
                zOut[iOut++] = 0xf0 | (v>>18);
                zOut[iOut++] = 0x80 | ((v>>12)&0x3f);
                zOut[iOut++] = 0x80 | ((v>>6)&0x3f);
                zOut[iOut++] = 0x80 | (v&0x3f);
              }else{
                zOut[iOut++] = 0xe0 | (v>>12);
                zOut[iOut++] = 0x80 | ((v>>6)&0x3f);
                zOut[iOut++] = 0x80 | (v&0x3f);
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
            c = (jsonHexToInt(z[iIn+1])<<4) | jsonHexToInt(z[iIn+2]);
            iIn += 2;
          }else if( c=='\r' && z[i+1]=='\n' ){
            iIn++;
            continue;
          }else if( 0xe2==(u8)c ){
            assert( 0x80==(u8)z[i+1] );
            assert( 0xa8==(u8)z[i+2] || 0xa9==(u8)z[i+2] );
            iIn += 2;
            continue;
          }else{
            continue;
          }
        } /* end if( c=='\\' ) */
        zOut[iOut++] = c;
      } /* end for() */
      zOut[iOut] = 0;
      sqlite3_result_text(pCtx, zOut, iOut, sqlite3_free);
      break;
    }
    case JSONB_ARRAY:
    case JSONB_OBJECT: {
      int flags = textOnly ? 0 : SQLITE_PTR_TO_INT(sqlite3_user_data(pCtx));
      if( flags & JSON_BLOB ){
        sqlite3_result_blob(pCtx, &pParse->aBlob[i], sz+n, SQLITE_TRANSIENT);
      }else{
        jsonReturnTextJsonFromBlob(pCtx, &pParse->aBlob[i], sz+n);
      }
      break;
    }
    default: {
      sqlite3_result_error(pCtx, "malformed JSON", -1);
      break;
    }
  }
}

/* Do a JSON_EXTRACT(JSON, PATH) on a when JSON is a BLOB.
*/
static void jsonExtractFromBlob(
  sqlite3_context *ctx,
  sqlite3_value *pJson,
  sqlite3_value *pPath,
  int flags
){
  const char *zPath = (const char*)sqlite3_value_text(pPath);
  u32 i = 0;
  JsonParse px;
  if( zPath==0 ) return;
  memset(&px, 0, sizeof(px));
  px.nBlob = sqlite3_value_bytes(pJson);
  px.aBlob = (u8*)sqlite3_value_blob(pJson);
  if( px.aBlob==0 ) return;
  if( zPath[0]=='$' ){
    zPath++;
    i = jsonLookupBlobStep(&px, 0, zPath, 0);
  }else if( (flags & JSON_ABPATH) ){
    /* The -> and ->> operators accept abbreviated PATH arguments.  This
    ** is mostly for compatibility with PostgreSQL, but also for
    ** convenience.
    **
    **     NUMBER   ==>  $[NUMBER]     // PG compatible
    **     LABEL    ==>  $.LABEL       // PG compatible
    **     [NUMBER] ==>  $[NUMBER]     // Not PG.  Purely for convenience
    */
    JsonString jx;
    jsonStringInit(&jx, ctx);
    if( sqlite3Isdigit(zPath[0]) ){
      jsonAppendRawNZ(&jx, "[", 1);
      jsonAppendRaw(&jx, zPath, (int)strlen(zPath));
      jsonAppendRawNZ(&jx, "]", 2);
      zPath = jx.zBuf;
    }else if( zPath[0]!='[' ){
      jsonAppendRawNZ(&jx, ".", 1);
      jsonAppendRaw(&jx, zPath, (int)strlen(zPath));
      jsonAppendChar(&jx, 0);
      zPath = jx.zBuf;
    }
    i = jsonLookupBlobStep(&px, 0, zPath, 0);
    jsonStringReset(&jx);
  }else{
    sqlite3_result_error(ctx, "bad path", -1);
    return;
  }
  if( i<px.nBlob ){
    jsonReturnFromBlob(&px, i, ctx, 0);
  }else if( i==JSON_BLOB_NOTFOUND ){
    return;  /* Return NULL if not found */
  }else if( i==JSON_BLOB_ERROR ){
    sqlite3_result_error(ctx, "malformed JSON", -1);
  }else{
    char *zMsg = sqlite3_mprintf("bad path syntax: %s",
                    sqlite3_value_text(pPath));
    sqlite3_result_error(ctx, zMsg, -1);
    sqlite3_free(zMsg);
  }
}
 
/* argv[0] is a BLOB that seems likely to be a JSONB.  Subsequent
** arguments are JSON paths of elements to be removed.  Do that removal
** and return the result.
*/
static void jsonRemoveFromBlob(
  sqlite3_context *ctx,
  int argc,
  sqlite3_value **argv
){
  int i;
  u32 rc;
  const char *zPath = 0;
  int flgs;
  JsonParse px;

  memset(&px, 0, sizeof(px));
  px.nBlob = sqlite3_value_bytes(argv[0]);
  px.aBlob = (u8*)sqlite3_value_blob(argv[0]);
  if( px.aBlob==0 ) return;
  for(i=1; i<argc; i++){
    const char *zPath = (const char*)sqlite3_value_text(argv[i]);
    if( zPath==0 ) goto jsonRemoveFromBlob_patherror;
    if( zPath[0]!='$' ) goto jsonRemoveFromBlob_patherror;
    if( zPath[1]==0 ){
      jsonParseReset(&px);
      return;  /* return NULL if $ is removed */
    }
    px.eEdit = JEDIT_DEL;
    px.delta = 0;
    rc = jsonLookupBlobStep(&px, 0, zPath+1, 0);
    if( rc==JSON_BLOB_NOTFOUND ) continue;
    if( JSON_BLOB_ISERROR(rc) ) goto jsonRemoveFromBlob_patherror;
  }
  flgs = SQLITE_PTR_TO_INT(sqlite3_user_data(ctx));
  if( flgs & JSON_BLOB ){
    sqlite3_result_blob(ctx, px.aBlob, px.nBlob,
                        px.nBlobAlloc>0 ? SQLITE_DYNAMIC : SQLITE_TRANSIENT);
  }else{
    JsonString s;
    jsonStringInit(&s, ctx);
    jsonXlateBlobToText(&px, 0, &s);
    jsonReturnString(&s);
    jsonParseReset(&px);
  }
  return;

jsonRemoveFromBlob_patherror:
  jsonParseReset(&px);
  jsonPathSyntaxError(zPath, ctx);
  return;
}

/*
** pArg is a function argument that might be an SQL value or a JSON
** value.  Figure out what it is and encode it as a JSONB blob.
** Return the results in pParse.
**
** pParse is uninitialized upon entry.  This routine will handle the
** initialization of pParse.  The result will be contained in
** pParse->aBlob and pParse->nBlob.  pParse->aBlob might be dynamically
** allocated (if pParse->nBlobAlloc is greater than zero) in which case
** the caller is responsible for freeing the space allocated to pParse->aBlob
** when it has finished with it.  Or pParse->aBlob might be a static string
** or a value obtained from sqlite3_value_blob(pArg).
**
** If the argument is a BLOB that is clearly not a JSONB, then this
** function might set an error message in ctx and return non-zero.
** It might also set an error message and return non-zero on an OOM error.
*/
static int jsonFunctionArgToBlob(
  sqlite3_context *ctx,
  sqlite3_value *pArg,
  JsonParse *pParse
){
  int eType = sqlite3_value_type(pArg);
  static u8 aNull[] = { 0x00 };
  memset(pParse, 0, sizeof(pParse[0]));
  switch( eType ){
    case SQLITE_NULL: {
      pParse->aBlob = aNull;
      pParse->nBlob = 1;
      return 0;
    }
    case SQLITE_BLOB: {
      if( jsonFuncArgMightBeBinary(pArg) ){
        pParse->aBlob = (u8*)sqlite3_value_blob(pArg);
        pParse->nBlob = sqlite3_value_bytes(pArg);
      }else{
        sqlite3_result_error(ctx, "JSON cannot hold BLOB values", -1);
        return 1;
      }
      break;
    }
    case SQLITE_TEXT: {
      const char *zJson = (const char*)sqlite3_value_text(pArg);
      int nJson = sqlite3_value_bytes(pArg);
      if( zJson==0 ) return 1;
      if( sqlite3_value_subtype(pArg)==JSON_SUBTYPE ){
        pParse->zJson = (char*)zJson;
        pParse->nJson = nJson;
        if( jsonConvertTextToBlob(pParse, ctx) ){
          sqlite3_result_error(ctx, "malformed JSON", -1);
          sqlite3_free(pParse->aBlob);
          memset(pParse, 0, sizeof(pParse[0]));
          return 1;
        }
      }else{
        jsonBlobAppendNodeType(pParse, JSONB_TEXTRAW, nJson);
        jsonBlobAppendNBytes(pParse, (const u8*)zJson, nJson);
      }
      break;
    }
    case SQLITE_FLOAT:
    case SQLITE_INTEGER: {
      int n = sqlite3_value_bytes(pArg);
      const char *z = (const char*)sqlite3_value_text(pArg);
      int e = eType==SQLITE_INTEGER ? JSONB_INT : JSONB_FLOAT;
      if( z==0 ) return 1;
      jsonBlobAppendNodeType(pParse, e, n);
      jsonBlobAppendNBytes(pParse, (const u8*)z, n);
      break;
    }
  }
  return 0;
}
 
/* argv[0] is a BLOB that seems likely to be a JSONB.  Subsequent
** arguments come in parse where each pair contains a JSON path and
** content to insert or set at that patch.  Do the updates
** and return the result.
**
** The specific operation is determined by eEdit, which can be one
** of JEDIT_INS, JEDIT_REPL, or JEDIT_SET.
*/
static void jsonInsertIntoBlob(
  sqlite3_context *ctx,
  int argc,
  sqlite3_value **argv,
  int eEdit                /* JEDIT_INS, JEDIT_REPL, or JEDIT_SET */
){
  int i;
  u32 rc = 0;
  const char *zPath = 0;
  int flgs;
  JsonParse px, ax;

  assert( (argc&1)==1 );
  memset(&px, 0, sizeof(px));
  px.nBlob = sqlite3_value_bytes(argv[0]);
  px.aBlob = (u8*)sqlite3_value_blob(argv[0]);
  if( px.aBlob==0 ) return;
  for(i=1; i<argc-1; i+=2){
    const char *zPath = (const char*)sqlite3_value_text(argv[i]);
    if( zPath==0 ) goto jsonInsertIntoBlob_patherror;
    if( zPath[0]!='$' ) goto jsonInsertIntoBlob_patherror;
    if( jsonFunctionArgToBlob(ctx, argv[i+1], &ax) ){
      break;
    }
    if( zPath[1]==0 ){
      jsonParseReset(&px);
      return;  /* return NULL if $ is removed */
    }
    px.eEdit = eEdit;
    px.nIns = ax.nBlob;
    px.aIns = ax.aBlob;
    px.delta = 0;
    rc = jsonLookupBlobStep(&px, 0, zPath+1, 0);
    jsonParseReset(&ax);
    if( rc==JSON_BLOB_NOTFOUND ) continue;
    if( JSON_BLOB_ISERROR(rc) ) goto jsonInsertIntoBlob_patherror;
  }
  flgs = SQLITE_PTR_TO_INT(sqlite3_user_data(ctx));
  if( flgs & JSON_BLOB ){
    sqlite3_result_blob(ctx, px.aBlob, px.nBlob,
                        px.nBlobAlloc>0 ? SQLITE_DYNAMIC : SQLITE_TRANSIENT);
  }else{
    JsonString s;
    jsonStringInit(&s, ctx);
    jsonXlateBlobToText(&px, 0, &s);
    jsonReturnString(&s);
    jsonParseReset(&px);
  }
  return;

jsonInsertIntoBlob_patherror:
  jsonParseReset(&px);
  if( rc==JSON_BLOB_ERROR ){
    sqlite3_result_error(ctx, "malformed JSON", -1);
  }else{
    jsonPathSyntaxError(zPath, ctx);
  }
  return;
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

#if SQLITE_DEBUG
/*
** Decode JSONB bytes in aBlob[] starting at iStart through but not 
** including iEnd.  Indent the
** content by nIndent spaces.
*/
static void jsonDebugPrintBlob(
  JsonParse *pParse, /* JSON content */
  u32 iStart,        /* Start rendering here */
  u32 iEnd,          /* Do not render this byte or any byte after this one */
  int nIndent        /* Indent by this many spaces */
){
  while( iStart<iEnd ){
    u32 i, n, nn, sz = 0;
    int showContent = 1;
    u8 x = pParse->aBlob[iStart] & 0x0f;
    printf("%5d:%*s", iStart, nIndent, "");
    nn = n = jsonbPayloadSize(pParse, iStart, &sz);
    if( nn==0 ) nn = 1;
    if( sz>0 && x<JSONB_ARRAY ){
      nn += sz;
    }
    for(i=0; i<nn; i++) printf(" %02x", pParse->aBlob[iStart+i]);
    if( n==0 || iStart+n+sz>iEnd ){
      printf("   ERROR invalid node size\n");
      iStart = n==0 ? iStart+1 : iEnd;
      continue;
    }
    printf("  <-- ");
    switch( x ){
      case JSONB_NULL:     printf("null"); break;
      case JSONB_TRUE:     printf("true"); break;
      case JSONB_FALSE:    printf("false"); break;
      case JSONB_INT:      printf("int"); break;
      case JSONB_INT5:     printf("int5"); break;
      case JSONB_FLOAT:    printf("float"); break;
      case JSONB_FLOAT5:   printf("float5"); break;
      case JSONB_TEXT:     printf("text"); break;
      case JSONB_TEXTJ:    printf("textj"); break;
      case JSONB_TEXT5:    printf("text5"); break;
      case JSONB_TEXTRAW:  printf("textraw"); break;
      case JSONB_ARRAY: {
        printf("array, %u bytes\n", sz);
        jsonDebugPrintBlob(pParse, iStart+n, iStart+n+sz, nIndent+2);
        showContent = 0;
        break;
      }
      case JSONB_OBJECT: {
        printf("object, %u bytes\n", sz);
        jsonDebugPrintBlob(pParse, iStart+n, iStart+n+sz, nIndent+2);
        showContent = 0;
        break;
      }
      default: {
        printf("ERROR: unknown node type\n");
        showContent = 0;
        break;
      }
    }
    if( showContent ){
      if( sz==0 && x<=JSON_FALSE ){
        printf("\n");
      }else{
        u32 i;
        printf(": \"");
        for(i=iStart+n; i<iStart+n+sz; i++){
          u8 c = pParse->aBlob[i];
          if( c<0x20 || c>=0x7f ) c = '.';
          putchar(c);
        }
        printf("\"\n");
      }
    }
    iStart += n + sz;
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
  if( jsonFuncArgMightBeBinary(argv[0]) ){
    JsonParse x;
    memset(&x, 0, sizeof(x));
    x.nBlob = sqlite3_value_bytes(argv[0]);
    x.aBlob = (u8*)sqlite3_value_blob(argv[0]);
    jsonDebugPrintBlob(&x, 0, x.nBlob, 0);
    return;
  }
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
  jsonReturnNodeAsJson(p, p->aNode, ctx, 1, 0);
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

/* SQL Function:  jsonb_test2(BLOB_JSON)
**
** Render BLOB_JSON back into text.
** Development testing only.
*/
static void jsonbTest2(
  sqlite3_context *ctx,
  int argc,
  sqlite3_value **argv
){
  const u8 *aBlob;
  int nBlob;
  UNUSED_PARAMETER(argc);

  aBlob = (const u8*)sqlite3_value_blob(argv[0]);
  nBlob = sqlite3_value_bytes(argv[0]);
  jsonReturnTextJsonFromBlob(ctx, aBlob, nBlob);
}
#endif /* SQLITE_DEBUG */

/****************************************************************************
** Scalar SQL function implementations
****************************************************************************/

/* SQL Function:  jsonb(JSON)
**
** Convert the input argument into JSONB (the SQLite binary encoding of
** JSON).
**
** If the input is TEXT, or NUMERIC, try to parse it as JSON.  If the fails,
** raise an error.  Otherwise, return the resulting BLOB value.
**
** If the input is a BLOB, check to see if the input is a plausible
** JSONB.  If it is, return it unchanged.  Raise an error if it is not.
** Note that there could be internal inconsistencies in the BLOB - this
** routine does not do a full byte-for-byte validity check of the
** JSON blob.
**
** If the input is NULL, return NULL.
*/
static void jsonbFunc(
  sqlite3_context *ctx,
  int argc,
  sqlite3_value **argv
){
  JsonParse *pParse;
  int nJson;
  const char *zJson;
  JsonParse x;
  UNUSED_PARAMETER(argc);

  if( sqlite3_value_type(argv[0])==SQLITE_NULL ){
    /* No-op */
  }else if( jsonFuncArgMightBeBinary(argv[0]) ){
    sqlite3_result_value(ctx, argv[0]);
  }else{
    zJson = (const char*)sqlite3_value_text(argv[0]);
    if( zJson==0 ) return;
    nJson = sqlite3_value_bytes(argv[0]);
    pParse = &x;
    memset(&x, 0, sizeof(x));
    x.zJson = (char*)zJson;
    x.nJson = nJson;
    if( jsonConvertTextToBlob(pParse, ctx) ){
      sqlite3_result_error(ctx, "malformed JSON", -1);
    }else{
      sqlite3_result_blob(ctx, pParse->aBlob, pParse->nBlob, sqlite3_free);
      pParse->aBlob = 0;
      pParse->nBlob = 0;
      pParse->nBlobAlloc = 0;
    }
    jsonParseReset(pParse);
  }
}

/*
** Implementation of the json_quote(VALUE) function.  Return a JSON value
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

  jsonStringInit(&jx, ctx);
  jsonAppendSqlValue(&jx, argv[0]);
  jsonReturnString(&jx);
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

  jsonStringInit(&jx, ctx);
  jsonAppendChar(&jx, '[');
  for(i=0; i<argc; i++){
    jsonAppendSeparator(&jx);
    jsonAppendSqlValue(&jx, argv[i]);
  }
  jsonAppendChar(&jx, ']');
  jsonReturnString(&jx);
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
      i = 1;
      while( i<=pNode->n ){
        if( (pNode[i].jnFlags & JNODE_REMOVE)==0 ) n++;
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
  if( jsonFuncArgMightBeBinary(argv[0]) && argc==2 ){
    jsonExtractFromBlob(ctx, argv[0], argv[1], flags);
    return;
  }
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
        jsonStringInit(&jx, ctx);
        if( sqlite3Isdigit(zPath[0]) ){
          jsonAppendRawNZ(&jx, "$[", 2);
          jsonAppendRaw(&jx, zPath, (int)strlen(zPath));
          jsonAppendRawNZ(&jx, "]", 2);
        }else{
          jsonAppendRawNZ(&jx, "$.", 1 + (zPath[0]!='['));
          jsonAppendRaw(&jx, zPath, (int)strlen(zPath));
          jsonAppendChar(&jx, 0);
        }
        pNode = jx.eErr ? 0 : jsonLookup(p, jx.zBuf, 0, ctx);
        jsonStringReset(&jx);
      }else{
        pNode = jsonLookup(p, zPath, 0, ctx);
      }
      if( pNode ){
        if( flags & JSON_JSON ){
          jsonReturnNodeAsJson(p, pNode, ctx, 0, 0);
        }else{
          jsonReturnFromNode(p, pNode, ctx, 1);
        }
      }
    }else{
      pNode = jsonLookup(p, zPath, 0, ctx);
      if( p->nErr==0 && pNode ) jsonReturnFromNode(p, pNode, ctx, 0);
    }
  }else{
    /* Two or more PATH arguments results in a JSON array with each
    ** element of the array being the value selected by one of the PATHs */
    int i;
    jsonStringInit(&jx, ctx);
    jsonAppendChar(&jx, '[');
    for(i=1; i<argc; i++){
      zPath = (const char*)sqlite3_value_text(argv[i]);
      pNode = jsonLookup(p, zPath, 0, ctx);
      if( p->nErr ) break;
      jsonAppendSeparator(&jx);
      if( pNode ){
        jsonXlateNodeToText(p, pNode, &jx);
      }else{
        jsonAppendRawNZ(&jx, "null", 4);
      }
    }
    if( i==argc ){
      jsonAppendChar(&jx, ']');
      jsonReturnString(&jx);
      sqlite3_result_subtype(ctx, JSON_SUBTYPE);
    }
    jsonStringReset(&jx);
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
    if( pPatch[i].eType!=JSON_STRING ){
      pParse->nErr = 1;
      return 0;
    }
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
      JSON_VVA( pParse->aNode[iRoot].eU = 2 );
      iRoot = iStart;
      pTarget = &pParse->aNode[iTarget];
    }
  }
  return pTarget;
}


#if 0
/*
** Return codes for jsonMergePatchBlob()
*/
#define JSON_MERGE_OK          0     /* Success */
#define JSON_MERGE_BADTARGET   1     /* Malformed TARGET blob */
#define JSON_MERGE_BADPATCH    2     /* Malformed PATCH blob */
#define JSON_MERGE_OOM         3     /* Out-of-memory condition */

/*
** RFC-7396 MergePatch for two JSONB blobs.
**
** pTarget is the target. pPatch is the patch.  The target is updated
** in place.  The patch is read-only.
**
** The original RFC-7396 algorithm is this:
**
**   define MergePatch(Target, Patch):
**     if Patch is an Object:
**       if Target is not an Object:
**         Target = {} # Ignore the contents and set it to an empty Object
**     for each Name/Value pair in Patch:
**         if Value is null:
**           if Name exists in Target:
**             remove the Name/Value pair from Target
**         else:
**           Target[Name] = MergePatch(Target[Name], Value)
**       return Target
**     else:
**       return Patch
**
** Here is the same algorithm restrictured to show the actual
** implementation:
**
** 01   define MergePatch(Target, Patch):
** 02      if Patch is not an Object:
** 03         return Patch
** 04      else:  // if Patch is an Object:
** 05         if Target is not an Object:
** 06            Target = {}
** 07      for each Name/Value pair in Patch:
** 08         if Name exists in Target:
** 09            if Value is null:
** 10               remove the Name/Value pair from Target
** 11            else
** 12               Target[name] = MergePatch(Target[Name], Value)
** 13         else if Value is not NULL:
** 14            Target[name] = RemoveNullVAlues(Value)
** 15      return Target
**  |
**  ^---- Line numbers referenced in comments in the implementation
*/
static int jsonMergePatchBlob(
  JsonParse *pTarget,      /* The JSON parser that contains the TARGET */
  u32 iTarget,             /* Index of TARGET in pTarget->aBlob[] */
  const JsonParse *pPatch  /* The PATCH */
  u32 iPatch               /* Index of PATCH in pPatch->aBlob[] */
){
  u8 x;             /* Type of a single node */
  u32 n, sz=0;      /* Return values from jsonbPayloadSize() */
  u32 iTCursor;     /* Cursor position while scanning the target object */
  u32 iTStart;      /* First label in the target object */
  u32 iTEndBE;      /* Original first byte past end of target, before edit */
  u32 iTEnd;        /* Current first byte past end of target */
  u32 iTLabel;      /* Index of the label */
  u32 nTLabel;      /* Header size in bytes for the target label */
  u32 szTLabel;     /* Size of the target label payload */
  u32 iTValue;      /* Index of the target value */
  u32 nTValue;      /* Header size of the target value */
  u32 szTValue;     /* Payload size for the target value */

  u32 iPCursor;     /* Cursor position while scanning the patch */
  u32 iPEnd;        /* First byte past the end of the patch */
  u32 iPLabel;      /* Start of patch label */
  u32 nPLabel;      /* Size of header on the patch label */
  u32 szPLabel;     /* Payload size of the patch label */
  u32 iPValue;      /* Start of patch value */
  u32 nPValue;      /* Header size for the patch value */
  u32 szPValue;     /* Payload size of the patch value */

  assert( iTarget>=0 && iTarget<pTarget->nBlob );
  assert( iPatch>=0 && iPatch<pPatch->nBlob );
  x = pPatch->aBlob[iPatch] & 0x0f;
  if( x!=JSONB_OBJECT ){  /* Algorithm line 02 */
    u32 szPatch;        /* Total size of the patch, header+payload */
    u32 szTarget;       /* Total size of the target, header+payload */
    n = jsonbPayloadSize(pPatch, iPatch, &sz);
    szPatch = n+sz;
    sz = 0;
    n = jsonbPayloadSize(pTarget, iTarget, &sz);
    szTarget = n+sz;
    jsonBlobEdit(pTarget, iTarget, szTarget, pPatch->aBlob+iPatch, szPatch);
    return pTarget->oom ? JSON_MERGE_OOM : JSON_MERGE_OK;  /* Line 03 */
  }
  x = pTarget->aBlob[iTarget] & 0x0f;
  if( x!=JSONB_OBJECT ){  /* Algorithm line 05 */
    static const u8 emptyObject = { JSONB_OBJECT };
    n = jsonbPayloadSize(pTarget, iTarget, &sz);
    jsonBlobEdit(pTarget, iTarget, szTarget, emptyObject, 1); /* Line 06 */
  }
  n = jsonbPayloadSize(pPatch, iPatch, &sz);
  if( n==0 ) return JSON_MERGE_BADPATCH;
  iPCursor = iPatch+n;
  iPEnd = iPCursor+sz;
  n = jsonbPayloadSize(pTarget, iTarget, &sz);
  if( n==0 ) return JSON_MERGE_BADTARGET;
  iTStart = iTarget+n;
  iTEndBE = iTStart+sz;

  while( iPCursor<iPEnd ){  /* Algorithm line 07 */
    iPLabel = iPCursor;
    x = pPatch->aBlob[iPCursor] & 0x0f;
    if( x<JSONB_TEXT || x>JSONB_TEXTRAW ) return JSON_MERGE_BADPATCH;
    nPLabel = jsonbPayloadSize(pPatch, iPCursor, &szPLabel);
    if( nPLabel==0 ) return JSON_MERGE_BADPATCH;
    iPValue = iPCursor + nPLabel + szPLabel;
    if( iPCursor>=iPEnd ) return JSON_MERGE_BADPATCH;
    nPValue = jsonbPayloadSize(pPatch, iPValue, &szPValue);
    if( nPValue==0 ) return JSON_MERGE_BADPATCH;
    iPCursor = iPValue + nPValue + szPValue;
    if( iPCursor>iPEnd ) return JSON_MERGE_BADPATCH;

    iTCursor = iTStart;
    iTEnd = iTEndBE + pTarget->delta;
    while( iTCursor<iTEnd ){
      iTLabel = iTCursor;
      x = pTarget->aBlob[iTCursor] & 0x0f;
      if( x<JSONB_TEXT || x>JSONB_TEXTRAW ) return JSON_MERGE_BADTARGET;
      nTLabel = jsonbPayloadSize(pTarget, iTCursor, &szTLabel);
      if( nTLabel==0 ) return JSON_MERGE_BADTARGET;
      iTValue = iTLabel + nTLabel + szTLabel;
      if( iTValue>=iTEnd ) return JSON_MERGE_BADTARGET;
      nTValue = jsonbPayloadSize(pTarget, iTValue, &szTValue);
      if( nTValue==0 ) return JSON_MERGE_BADTARGET;
      if( iTValue + nTValue + szTValue > iTEnd ) return JSON_MERGE_BADTARGET;
      if( eTLabel==ePLabel ){
        if( szTLabel==szPLabel
         && memcmp(&pTarget->aBlob[iTLabel+nTLabel],
                   &pPatch->aBlob[iPLabel+nPLabel], szTLabel)==0
        ){
          break;  /* Labels match. */
        }
      }else{
        if( jsonLabelEqual(pTarget, iTLabel, pPatch, iPLabel) ) break;
      }
      iTCursor = iTValue + nTValue + szTValue;
    }
    x = pPatch->aBlob[iPValue] & 0x0f;
    if( iTCursor<iTEnd ){
      /* A match was found.  Algorithm line 08 */
      if( x==0 ){
        /* Patch value is NULL.  Algorithm line 09 */
        jsonBlobEdit(pTarget, iTLabel, nTLabel+szTLabel+nTValue+szTValue,
                     0, 0);
        if( pTarget->oom ) return JSON_MERGE_OOM;
      }else{
        /* Algorithm line 12 */
        int rc = jsonMergePatchBlob(pTarget, iTValue, pPatch, pPValue);
        if( rc ) return rc;
      }        
    }else if( x>0 ){  /* Algorithm line 13 */
      /* No match and patch value is not NULL */
      jsonBlobEdit(pTarget, iTEnd, 0,
                   pPatch->aBlob+iPValue, szPValue+nPValue);
      if( pTarget->oom ) return JSON_MERGE_OOM;
      jsonBlobRemoveNullsFromObject(pTarget, iTEnd);
    }
  }
  jsonAfterEditSizeAdjust(pTarget, iTarget);
  return pTarget->oom ? JSON_MERGE_OOM : JSON_MERGE_OK;
}
#endif


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
  assert( pResult!=0 || pX->oom || pX->nErr );
  if( pX->oom ){
    sqlite3_result_error_nomem(ctx);
  }else if( pX->nErr ){
    sqlite3_result_error(ctx, "malformed JSON", -1);
  }else if( pResult ){
    jsonDebugPrintParse(pX);
    jsonDebugPrintNode(pResult);
    jsonReturnNodeAsJson(pX, pResult, ctx, 0, 0);
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
  jsonStringInit(&jx, ctx);
  jsonAppendChar(&jx, '{');
  for(i=0; i<argc; i+=2){
    if( sqlite3_value_type(argv[i])!=SQLITE_TEXT ){
      sqlite3_result_error(ctx, "json_object() labels must be TEXT", -1);
      jsonStringReset(&jx);
      return;
    }
    jsonAppendSeparator(&jx);
    z = (const char*)sqlite3_value_text(argv[i]);
    n = (u32)sqlite3_value_bytes(argv[i]);
    jsonAppendString(&jx, z, n);
    jsonAppendChar(&jx, ':');
    jsonAppendSqlValue(&jx, argv[i+1]);
  }
  jsonAppendChar(&jx, '}');
  jsonReturnString(&jx);
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
  if( jsonFuncArgMightBeBinary(argv[0]) ){
    jsonRemoveFromBlob(ctx, argc, argv);
    return;
  }
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
    jsonReturnNodeAsJson(pParse, pParse->aNode, ctx, 1, 0);
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
        char *zCopy = sqlite3_malloc64( n+1 );
        int k;
        if( zCopy ){
          memcpy(zCopy, z, n);
          zCopy[n] = 0;
          jsonParseAddCleanup(p, sqlite3_free, zCopy);
        }else{
          p->oom = 1;
          sqlite3_result_error_nomem(pCtx);
        }
        k = jsonParseAddNode(p, JSON_STRING, n, zCopy);
        assert( k>0 || p->oom );
        if( p->oom==0 ) p->aNode[k].jnFlags |= JNODE_RAW;
        break;
      }
    replace_with_json:
      {
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
      if( jsonFuncArgMightBeBinary(pValue) ){
        goto replace_with_json;
      }else{
        jsonParseAddNode(p, JSON_NULL, 0, 0);
        sqlite3_result_error(pCtx, "JSON cannot hold BLOB values", -1);
        p->nErr++;
      }
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
  if( jsonFuncArgMightBeBinary(argv[0]) && argc>=3 ){
    jsonInsertIntoBlob(ctx, argc, argv, JEDIT_REPL);
    return;
  }
  pParse = jsonParseCached(ctx, argv[0], ctx, argc>1);
  if( pParse==0 ) return;
  pParse->nJPRef++;
  for(i=1; i<(u32)argc; i+=2){
    zPath = (const char*)sqlite3_value_text(argv[i]);
    pParse->useMod = 1;
    pNode = jsonLookup(pParse, zPath, 0, ctx);
    if( pParse->nErr ) goto replace_err;
    if( pNode ){
      jsonReplaceNode(ctx, pParse, (u32)(pNode - pParse->aNode), argv[i+1]);
    }
  }
  jsonReturnNodeAsJson(pParse, pParse->aNode, ctx, 1, 0);
replace_err:
  jsonDebugPrintParse(pParse);
  jsonParseFree(pParse);
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
  int flags = SQLITE_PTR_TO_INT(sqlite3_user_data(ctx));
  int bIsSet = (flags&JSON_ISSET)!=0;

  if( argc<1 ) return;
  if( (argc&1)==0 ) {
    jsonWrongNumArgs(ctx, bIsSet ? "set" : "insert");
    return;
  }
  if( jsonFuncArgMightBeBinary(argv[0]) && argc>=3 ){
    jsonInsertIntoBlob(ctx, argc, argv, bIsSet ? JEDIT_SET : JEDIT_INS);
    return;
  }
  pParse = jsonParseCached(ctx, argv[0], ctx, argc>1);
  if( pParse==0 ) return;
  pParse->nJPRef++;
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
  jsonReturnNodeAsJson(pParse, pParse->aNode, ctx, 1, 0);
jsonSetDone:
  jsonParseFree(pParse);
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
** json_valid(JSON, FLAGS)
**
** Check the JSON argument to see if it is well-formed.  The FLAGS argument
** encodes the various constraints on what is meant by "well-formed":
**
**     0x01      Canonical RFC-8259 JSON text
**     0x02      JSON text with optional JSON-5 extensions
**     0x04      Superficially appears to be JSONB
**     0x08      Strictly well-formed JSONB
**
** If the FLAGS argument is omitted, it defaults to 1.  Useful values for
** FLAGS include:
**
**    1          Strict canonical JSON text
**    2          JSON text perhaps with JSON-5 extensions
**    4          Superficially appears to be JSONB
**    5          Canonical JSON text or superficial JSONB
**    6          JSON-5 text or superficial JSONB
**    8          Strict JSONB
**    9          Canonical JSON text or strict JSONB
**    10         JSON-5 text or strict JSONB
**
** Other flag combinations are redundant.  For example, every canonical
** JSON text is also well-formed JSON-5 text, so FLAG values 2 and 3
** are the same.  Similarly, any input that passes a strict JSONB validation
** will also pass the superficial validation so 12 through 15 are the same
** as 8 through 11 respectively.
**
** This routine runs in linear time to validate text and when doing strict
** JSONB validation.  Superficial JSONB validation is constant time,
** assuming the BLOB is already in memory.  The performance advantage
** of superficial JSONB validation is why that option is provided.
** Application developers can choose to do fast superficial validation or
** slower strict validation, according to their specific needs.
**
** Only the lower four bits of the FLAGS argument are currently used.
** Higher bits are reserved for future expansion.   To facilitate
** compatibility, the current implementation raises an error if any bit
** in FLAGS is set other than the lower four bits.
**
** The original circa 2015 implementation of the JSON routines in
** SQLite only supported canonical RFC-8259 JSON text and the json_valid()
** function only accepted one argument.  That is why the default value
** for the FLAGS argument is 1, since FLAGS=1 causes this routine to only
** recognize canonical RFC-8259 JSON text as valid.  The extra FLAGS
** argument was added when the JSON routines were extended to support
** JSON5-like extensions and binary JSONB stored in BLOBs.
**
** Return Values:
**
**   *   Raise an error if FLAGS is outside the range of 1 to 15.
**   *   Return NULL if the input is NULL
**   *   Return 1 if the input is well-formed.
**   *   Return 0 if the input is not well-formed.
*/
static void jsonValidFunc(
  sqlite3_context *ctx,
  int argc,
  sqlite3_value **argv
){
  JsonParse *p;          /* The parse */
  u8 flags = 1;
  u8 res = 0;
  if( argc==2 ){
    i64 f = sqlite3_value_int64(argv[1]);
    if( f<1 || f>15 ){
      sqlite3_result_error(ctx, "FLAGS parameter to json_valid() must be"
                                " between 1 and 15", -1);
      return;
    }
    flags = f & 0x0f;
  }
  switch( sqlite3_value_type(argv[0]) ){
    case SQLITE_NULL: {
#ifdef SQLITE_LEGACY_JSON_VALID
      /* Incorrect legacy behavior was to return FALSE for a NULL input */
      sqlite3_result_int(ctx, 0);
#endif
      return;
    }
    case SQLITE_BLOB: {
      if( (flags & 0x0c)!=0 && jsonFuncArgMightBeBinary(argv[0]) ){
        /* TO-DO:  strict checking if flags & 0x08 */
        res = 1;
      }
      break;
    }
    default: {
      if( (flags & 0x3)==0 ) break;
      p = jsonParseCached(ctx, argv[0], 0, 0);
      if( p==0 || p->oom ){
        sqlite3_result_error_nomem(ctx);
        sqlite3_free(p);
      }else if( p->nErr ){
        jsonParseFree(p);
      }else if( (flags & 0x02)!=0 || p->hasNonstd==0 || p->useMod ){
        res = 1;
      }
      break;
    }
  }
  sqlite3_result_int(ctx, res);
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
      jsonStringInit(pStr, ctx);
      jsonAppendChar(pStr, '[');
    }else if( pStr->nUsed>1 ){
      jsonAppendChar(pStr, ',');
    }
    pStr->pCtx = ctx;
    jsonAppendSqlValue(pStr, argv[0]);
  }
}
static void jsonArrayCompute(sqlite3_context *ctx, int isFinal){
  JsonString *pStr;
  pStr = (JsonString*)sqlite3_aggregate_context(ctx, 0);
  if( pStr ){
    int flags;
    pStr->pCtx = ctx;
    jsonAppendChar(pStr, ']');
    flags = SQLITE_PTR_TO_INT(sqlite3_user_data(ctx));
    if( pStr->eErr ){
      jsonReturnString(pStr);
      return;
    }else if( flags & JSON_BLOB ){
      jsonReturnStringAsBlob(pStr);
      if( isFinal ){
        sqlite3RCStrUnref(pStr->zBuf);
      }else{
        pStr->nUsed--;
      }
      return;
    }else if( isFinal ){
      sqlite3_result_text(ctx, pStr->zBuf, (int)pStr->nUsed,
                          pStr->bStatic ? SQLITE_TRANSIENT :
                              sqlite3RCStrUnref);
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
      jsonStringInit(pStr, ctx);
      jsonAppendChar(pStr, '{');
    }else if( pStr->nUsed>1 ){
      jsonAppendChar(pStr, ',');
    }
    pStr->pCtx = ctx;
    z = (const char*)sqlite3_value_text(argv[0]);
    n = (u32)sqlite3_value_bytes(argv[0]);
    jsonAppendString(pStr, z, n);
    jsonAppendChar(pStr, ':');
    jsonAppendSqlValue(pStr, argv[1]);
  }
}
static void jsonObjectCompute(sqlite3_context *ctx, int isFinal){
  JsonString *pStr;
  pStr = (JsonString*)sqlite3_aggregate_context(ctx, 0);
  if( pStr ){
    int flags;
    jsonAppendChar(pStr, '}');
    pStr->pCtx = ctx;
    flags = SQLITE_PTR_TO_INT(sqlite3_user_data(ctx));
    if( pStr->eErr ){
      jsonReturnString(pStr);
      return;
    }else if( flags & JSON_BLOB ){
      jsonReturnStringAsBlob(pStr);
      if( isFinal ){
        sqlite3RCStrUnref(pStr->zBuf);
      }else{
        pStr->nUsed--;
      }
      return;
    }else if( isFinal ){
      sqlite3_result_text(ctx, pStr->zBuf, (int)pStr->nUsed,
                          pStr->bStatic ? SQLITE_TRANSIENT :
                          sqlite3RCStrUnref);
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
typedef struct JsonParent JsonParent;
struct JsonParent {
  u32 iHead;                 /* Start of object or array */
  u32 iValue;                /* Start of the value */
  u32 iEnd;                  /* First byte past the end */
  u32 nPath;                 /* Length of path */
  i64 iKey;                  /* Key for JSONB_ARRAY */
};

typedef struct JsonEachCursor JsonEachCursor;
struct JsonEachCursor {
  sqlite3_vtab_cursor base;  /* Base class - must be first */
  u32 iRowid;                /* The rowid */
  u32 i;                     /* Index in sParse.aBlob[] of current row */
  u32 iEnd;                  /* EOF when i equals or exceeds this value */
  u32 nRoot;                 /* Size of the root path in bytes */
  u8 eType;                  /* Type of the container for element i */
  u8 bRecursive;             /* True for json_tree().  False for json_each() */
  u32 nParent;               /* Current nesting depth */
  u32 nParentAlloc;          /* Space allocated for aParent[] */
  JsonParent *aParent;       /* Parent elements of i */
  sqlite3 *db;               /* Database connection */
  JsonString path;           /* Current path */
  JsonParse sParse;          /* Parse of the input JSON */
};
typedef struct JsonEachConnection JsonEachConnection;
struct JsonEachConnection {
  sqlite3_vtab base;         /* Base class - must be first */
  sqlite3 *db;               /* Database connection */
};


/* Constructor for the json_each virtual table */
static int jsonEachConnect(
  sqlite3 *db,
  void *pAux,
  int argc, const char *const*argv,
  sqlite3_vtab **ppVtab,
  char **pzErr
){
  JsonEachConnection *pNew;
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
    pNew = (JsonEachConnection*)(*ppVtab = sqlite3_malloc( sizeof(*pNew) ));
    if( pNew==0 ) return SQLITE_NOMEM;
    memset(pNew, 0, sizeof(*pNew));
    sqlite3_vtab_config(db, SQLITE_VTAB_INNOCUOUS);
    pNew->db = db;
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
  JsonEachConnection *pVtab = (JsonEachConnection*)p;
  JsonEachCursor *pCur;

  UNUSED_PARAMETER(p);
  pCur = sqlite3_malloc( sizeof(*pCur) );
  if( pCur==0 ) return SQLITE_NOMEM;
  memset(pCur, 0, sizeof(*pCur));
  pCur->db = pVtab->db;
  jsonStringZero(&pCur->path);
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
  jsonParseReset(&p->sParse);
  jsonStringReset(&p->path);
  sqlite3DbFree(p->db, p->aParent);
  p->iRowid = 0;
  p->i = 0;
  p->aParent = 0;
  p->nParent = 0;
  p->nParentAlloc = 0;
  p->iEnd = 0;
  p->eType = 0;
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

/*
** If the cursor is currently pointing at the label of a object entry,
** then return the index of the value.  For all other cases, return the
** current pointer position, which is the value.
*/
static int jsonSkipLabel(JsonEachCursor *p){
  if( p->eType==JSONB_OBJECT ){
    u32 sz = 0;
    u32 n = jsonbPayloadSize(&p->sParse, p->i, &sz);
    return p->i + n + sz;
  }else{
    return p->i;
  }
}

/*
** Append the path name for the current element.
*/
static void jsonAppendPathName(JsonEachCursor *p){
  assert( p->nParent>0 );
  assert( p->eType==JSONB_ARRAY || p->eType==JSONB_OBJECT );
  if( p->eType==JSONB_ARRAY ){
    jsonPrintf(30, &p->path, "[%lld]", p->aParent[p->nParent-1].iKey);
  }else{
    u32 n, sz = 0, k, i;
    const char *z;
    int needQuote = 0;
    n = jsonbPayloadSize(&p->sParse, p->i, &sz);
    k = p->i + n;
    z = (const char*)&p->sParse.aBlob[k];
    if( sz==0 || !sqlite3Isalpha(z[0]) ){
      needQuote = 1;
    }else{
      for(i=0; i<sz; i++){
        if( !sqlite3Isalnum(z[i]) ){
          needQuote = 1;
          break;
        }
      }
    }
    if( needQuote ){
      jsonPrintf(sz+4,&p->path,".\"%.*s\"", sz, z);
    }else{
      jsonPrintf(sz+2,&p->path,".%.*s", sz, z);
    }
  }
}

/* Advance the cursor to the next element for json_tree() */
static int jsonEachNext(sqlite3_vtab_cursor *cur){
  JsonEachCursor *p = (JsonEachCursor*)cur;
  int rc = SQLITE_OK;
  if( p->bRecursive ){
    u8 x;
    u8 levelChange = 0;
    u32 n, sz = 0;
    u32 i = jsonSkipLabel(p);
    x = p->sParse.aBlob[i] & 0x0f;
    n = jsonbPayloadSize(&p->sParse, i, &sz);
    if( x==JSONB_OBJECT || x==JSONB_ARRAY ){
      JsonParent *pParent;
      if( p->nParent>=p->nParentAlloc ){
        JsonParent *pNew;
        u64 nNew;
        nNew = p->nParentAlloc*2 + 3;
        pNew = sqlite3DbRealloc(p->db, p->aParent, sizeof(JsonParent)*nNew);
        if( pNew==0 ) return SQLITE_NOMEM;
        p->nParentAlloc = (u32)nNew;
        p->aParent = pNew;
      }
      levelChange = 1;
      pParent = &p->aParent[p->nParent];
      pParent->iHead = p->i;
      pParent->iValue = i;
      pParent->iEnd = i + n + sz;
      pParent->iKey = -1;
      pParent->nPath = (u32)p->path.nUsed;
      if( p->eType && p->nParent ){
        jsonAppendPathName(p);
        if( p->path.eErr ) rc = SQLITE_NOMEM;
      }
      p->nParent++;
      p->i = i + n;
    }else{
      p->i = i + n + sz;
    }
    while( p->nParent>0 && p->i >= p->aParent[p->nParent-1].iEnd ){
      p->nParent--;
      p->path.nUsed = p->aParent[p->nParent].nPath;
      levelChange = 1;
    }
    if( levelChange ){
      if( p->nParent>0 ){
        JsonParent *pParent = &p->aParent[p->nParent-1];
        u32 i = pParent->iValue;
        p->eType = p->sParse.aBlob[i] & 0x0f;
      }else{
        p->eType = 0;
      }
    }
  }else{
    u32 n, sz = 0;
    u32 i = jsonSkipLabel(p);
    n = jsonbPayloadSize(&p->sParse, i, &sz);
    p->i = i + n + sz;
  }
  if( p->eType==JSONB_ARRAY && p->nParent ){
    p->aParent[p->nParent-1].iKey++;
  }
  p->iRowid++;
  return rc;
}

/* Length of the path for rowid==0 in bRecursive mode.
*/
static int jsonEachPathLength(JsonEachCursor *p){
  u32 n = p->path.nUsed;
  if( p->iRowid==0 && p->bRecursive && n>1 ){
    if( p->path.zBuf[n-1]==']' ){
      do{
        n--;
        assert( n>0 );
      }while( p->path.zBuf[n]!='[' );
    }else{
      u32 sz = 0;
      jsonbPayloadSize(&p->sParse, p->i, &sz);
      if( p->path.zBuf[n-1]=='"' ) sz += 2;
      n -= sz;
      while( p->path.zBuf[n]!='.' ){
        n--;
        assert( n>0 );
      }
    }
  }
  return n;
}

/* Return the value of a column */
static int jsonEachColumn(
  sqlite3_vtab_cursor *cur,   /* The cursor */
  sqlite3_context *ctx,       /* First argument to sqlite3_result_...() */
  int iColumn                 /* Which column to return */
){
  JsonEachCursor *p = (JsonEachCursor*)cur;
  switch( iColumn ){
    case JEACH_KEY: {
      if( p->nParent==0 ){
        u32 n, j;
        if( p->nRoot==1 ) break;
        j = jsonEachPathLength(p);
        n = p->nRoot - j;
        if( n==0 ){
          break;
        }else if( p->path.zBuf[j]=='[' ){
          i64 x;
          sqlite3Atoi64(&p->path.zBuf[j+1], &x, n-1, SQLITE_UTF8);
          sqlite3_result_int64(ctx, x);
        }else if( p->path.zBuf[j+1]=='"' ){
          sqlite3_result_text(ctx, &p->path.zBuf[j+2], n-3, SQLITE_TRANSIENT);
        }else{
          sqlite3_result_text(ctx, &p->path.zBuf[j+1], n-1, SQLITE_TRANSIENT);
        }
        break;
      }
      if( p->eType==JSONB_OBJECT ){
        jsonReturnFromBlob(&p->sParse, p->i, ctx, 1);
      }else{
        assert( p->eType==JSONB_ARRAY );
        sqlite3_result_int64(ctx, p->aParent[p->nParent-1].iKey);
      }
      break;
    }
    case JEACH_VALUE: {
      u32 i = jsonSkipLabel(p);
      jsonReturnFromBlob(&p->sParse, i, ctx, 1);
      break;
    }
    case JEACH_TYPE: {
      u32 i = jsonSkipLabel(p);
      u8 eType = eType = p->sParse.aBlob[i] & 0x0f;
      sqlite3_result_text(ctx, jsonbType[eType], -1, SQLITE_STATIC);
      break;
    }
    case JEACH_ATOM: {
      u32 i = jsonSkipLabel(p);
      if( (p->sParse.aBlob[i] & 0x0f)<JSONB_ARRAY ){
        jsonReturnFromBlob(&p->sParse, i, ctx, 1);
      }
      break;
    }
    case JEACH_ID: {
      sqlite3_result_int64(ctx, (sqlite3_int64)p->i);
      break;
    }
    case JEACH_PARENT: {
      if( p->nParent>0 && p->bRecursive ){
        sqlite3_result_int64(ctx, p->aParent[p->nParent-1].iHead);
      }
      break;
    }
    case JEACH_FULLKEY: {
      u64 nBase = p->path.nUsed;
      if( p->nParent ) jsonAppendPathName(p);
      sqlite3_result_text64(ctx, p->path.zBuf, p->path.nUsed,
                            SQLITE_TRANSIENT, SQLITE_UTF8);
      p->path.nUsed = nBase;
      break;
    }
    case JEACH_PATH: {
      u32 n = jsonEachPathLength(p);
      sqlite3_result_text64(ctx, p->path.zBuf, n,
                            SQLITE_TRANSIENT, SQLITE_UTF8);
      break;
    }
    default: {
      sqlite3_result_text(ctx, p->path.zBuf, p->nRoot, SQLITE_STATIC);
      break;
    }
    case JEACH_JSON: {
      if( p->sParse.isBinary ){
        sqlite3_result_blob(ctx, p->sParse.aBlob, p->sParse.nBlob,
                            SQLITE_STATIC);
      }else{
        sqlite3_result_text(ctx, p->sParse.zJson, -1, SQLITE_STATIC);
      }
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
  const char *zRoot = 0;
  u32 i, n, sz;

  UNUSED_PARAMETER(idxStr);
  UNUSED_PARAMETER(argc);
  jsonEachCursorReset(p);
  if( idxNum==0 ) return SQLITE_OK;
  memset(&p->sParse, 0, sizeof(p->sParse));
  p->sParse.nJPRef = 1;
  if( jsonFuncArgMightBeBinary(argv[0]) ){
    p->sParse.nBlob = sqlite3_value_bytes(argv[0]);
    p->sParse.aBlob = (u8*)sqlite3_value_blob(argv[0]);
    if( p->sParse.aBlob==0 ){
      return SQLITE_NOMEM;
    }
    p->sParse.isBinary = 1;
  }else{
    p->sParse.zJson = (char*)sqlite3_value_text(argv[0]);
    p->sParse.nJson = sqlite3_value_bytes(argv[0]);
    if( p->sParse.zJson==0 ){
      p->i = p->iEnd = 0;
      return SQLITE_OK;
    }      
    if( jsonConvertTextToBlob(&p->sParse, 0) ){
      if( p->sParse.oom ){
        return SQLITE_NOMEM;
      }
      sqlite3_free(cur->pVtab->zErrMsg);
      cur->pVtab->zErrMsg = sqlite3_mprintf("malformed JSON");
      jsonEachCursorReset(p);
      return cur->pVtab->zErrMsg ? SQLITE_ERROR : SQLITE_NOMEM;
    }
  }
  if( idxNum==3 ){
    zRoot = (const char*)sqlite3_value_text(argv[1]);
    if( zRoot==0 ) return SQLITE_OK;
    if( zRoot[0]!='$' ){
      sqlite3_free(cur->pVtab->zErrMsg);
      cur->pVtab->zErrMsg = jsonPathSyntaxError(zRoot, 0);
      jsonEachCursorReset(p);
      return cur->pVtab->zErrMsg ? SQLITE_ERROR : SQLITE_NOMEM;
    }
    p->nRoot = sqlite3_value_bytes(argv[1]);
    if( zRoot[1]==0 ){
      i = p->i = 0;
      p->eType = 0;
    }else{
      i = jsonLookupBlobStep(&p->sParse, 0, zRoot+1, 0);
      if( JSON_BLOB_ISERROR(i) ){
        if( i==JSON_BLOB_NOTFOUND ){
          p->i = 0;
          p->eType = 0;
          p->iEnd = 0;
          return SQLITE_OK;
        }
        sqlite3_free(cur->pVtab->zErrMsg);
        cur->pVtab->zErrMsg = jsonPathSyntaxError(zRoot, 0);
        jsonEachCursorReset(p);
        return cur->pVtab->zErrMsg ? SQLITE_ERROR : SQLITE_NOMEM;
      }
      if( p->sParse.iLabel ){
        p->i = p->sParse.iLabel;
        p->eType = JSONB_OBJECT;
      }else{
        p->i = i;
        p->eType = JSONB_ARRAY;
      }
    }
    jsonAppendRaw(&p->path, zRoot, p->nRoot);
  }else{
    i = p->i = 0;
    p->eType = 0;
    p->nRoot = 1;
    jsonAppendRaw(&p->path, "$", 1);
  }
  p->nParent = 0;
  n = jsonbPayloadSize(&p->sParse, i, &sz);
  p->iEnd = i+n+sz;
  if( (p->sParse.aBlob[i] & 0x0f)>=JSONB_ARRAY && !p->bRecursive ){
    p->i = i + n;
    p->eType = p->sParse.aBlob[i] & 0x0f;
    p->aParent = sqlite3DbMallocZero(p->db, sizeof(JsonParent));
    if( p->aParent==0 ) return SQLITE_NOMEM;
    p->nParent = 1;
    p->nParentAlloc = 1;
    p->aParent[0].iKey = 0;
    p->aParent[0].iEnd = p->iEnd;
    p->aParent[0].iHead = p->i;
    p->aParent[0].iValue = i;
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
  0,                         /* xShadowName */
  0                          /* xIntegrity */
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
  0,                         /* xShadowName */
  0                          /* xIntegrity */
};
#endif /* SQLITE_OMIT_VIRTUALTABLE */
#endif /* !defined(SQLITE_OMIT_JSON) */

/*
** Register JSON functions.
*/
void sqlite3RegisterJsonFunctions(void){
#ifndef SQLITE_OMIT_JSON
  static FuncDef aJsonFunc[] = {
    /*   sqlite3_result_subtype() ----,  ,--- sqlite3_value_subtype()       */
    /*                                |  |                                  */
    /*             Uses cache ------, |  | ,---- Returns JSONB              */
    /*                              | |  | |                                */
    /*     Number of arguments ---, | |  | | ,--- Flags                     */
    /*                            | | |  | | |                              */
    JFUNCTION(json,               1,1,1, 0,0,0,          jsonRemoveFunc),
    JFUNCTION(jsonb,              1,1,0, 0,1,0,          jsonbFunc),
    JFUNCTION(json_array,        -1,0,1, 1,0,0,          jsonArrayFunc),
    JFUNCTION(jsonb_array,       -1,0,1, 1,1,0,          jsonArrayFunc),
    JFUNCTION(json_array_length,  1,1,0, 0,0,0,          jsonArrayLengthFunc),
    JFUNCTION(json_array_length,  2,1,0, 0,0,0,          jsonArrayLengthFunc),
    JFUNCTION(json_error_position,1,1,0, 0,0,0,          jsonErrorFunc),
    JFUNCTION(json_extract,      -1,1,1, 0,0,0,          jsonExtractFunc),
    JFUNCTION(jsonb_extract,     -1,1,0, 0,1,0,          jsonExtractFunc),
    JFUNCTION(->,                 2,1,1, 0,0,JSON_JSON,  jsonExtractFunc),
    JFUNCTION(->>,                2,1,0, 0,0,JSON_SQL,   jsonExtractFunc),
    JFUNCTION(json_insert,       -1,1,1, 1,0,0,          jsonSetFunc),
    JFUNCTION(jsonb_insert,      -1,1,0, 1,1,0,          jsonSetFunc),
    JFUNCTION(json_object,       -1,0,1, 1,0,0,          jsonObjectFunc),
    JFUNCTION(jsonb_object,      -1,0,1, 1,1,0,          jsonObjectFunc),
    JFUNCTION(json_patch,         2,1,1, 0,0,0,          jsonPatchFunc),
    JFUNCTION(jsonb_patch,        2,1,0, 0,1,0,          jsonPatchFunc),
    JFUNCTION(json_quote,         1,0,1, 1,0,0,          jsonQuoteFunc),
    JFUNCTION(json_remove,       -1,1,1, 0,0,0,          jsonRemoveFunc),
    JFUNCTION(jsonb_remove,      -1,1,0, 0,1,0,          jsonRemoveFunc),
    JFUNCTION(json_replace,      -1,1,1, 1,0,0,          jsonReplaceFunc),
    JFUNCTION(jsonb_replace,     -1,1,0, 1,1,0,          jsonReplaceFunc),
    JFUNCTION(json_set,          -1,1,1, 1,0,JSON_ISSET, jsonSetFunc),
    JFUNCTION(jsonb_set,         -1,1,0, 1,1,JSON_ISSET, jsonSetFunc),
    JFUNCTION(json_type,          1,1,0, 0,0,0,          jsonTypeFunc),
    JFUNCTION(json_type,          2,1,0, 0,0,0,          jsonTypeFunc),
    JFUNCTION(json_valid,         1,1,0, 0,0,0,          jsonValidFunc),
    JFUNCTION(json_valid,         2,1,0, 0,0,0,          jsonValidFunc),
#if SQLITE_DEBUG
    JFUNCTION(json_parse,         1,1,0, 0,0,0,          jsonParseFunc),
    JFUNCTION(json_test1,         1,1,0, 1,0,0,          jsonTest1Func),
    JFUNCTION(jsonb_test2,        1,1,0, 0,1,0,          jsonbTest2),
#endif
    WAGGREGATE(json_group_array,  1, 0, 0,
       jsonArrayStep, jsonArrayFinal, jsonArrayValue, jsonGroupInverse,
       SQLITE_SUBTYPE|SQLITE_RESULT_SUBTYPE|SQLITE_UTF8|
       SQLITE_DETERMINISTIC),
    WAGGREGATE(jsonb_group_array, 1, JSON_BLOB, 0,
       jsonArrayStep, jsonArrayFinal, jsonArrayValue, jsonGroupInverse,
       SQLITE_SUBTYPE|SQLITE_RESULT_SUBTYPE|SQLITE_UTF8|SQLITE_DETERMINISTIC),
    WAGGREGATE(json_group_object, 2, 0, 0,
       jsonObjectStep, jsonObjectFinal, jsonObjectValue, jsonGroupInverse,
       SQLITE_SUBTYPE|SQLITE_RESULT_SUBTYPE|SQLITE_UTF8|SQLITE_DETERMINISTIC),
    WAGGREGATE(jsonb_group_object,2, JSON_BLOB, 0,
       jsonObjectStep, jsonObjectFinal, jsonObjectValue, jsonGroupInverse,
       SQLITE_SUBTYPE|SQLITE_RESULT_SUBTYPE|SQLITE_UTF8|
       SQLITE_DETERMINISTIC)
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

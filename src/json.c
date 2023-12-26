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
** SQLite JSON functions.
**
** This file began as an extension in ext/misc/json1.c in 2015.  That
** extension proved so useful that it has now been moved into the core.
**
** The original design stored all JSON as pure text, canonical RFC-8259.
** Support for JSON-5 extensions was added with version 3.42.0 (2023-05-16).
** All generated JSON text still conforms strictly to RFC-8259, but text
** with JSON-5 extensions is accepted as input.
**
** Beginning with version 3.45.0 (circa 2024-01-01), these routines also
** accept BLOB values that have JSON encoded using a binary representation
** called "JSONB".  The name JSONB comes from PostgreSQL, however the on-disk
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
**
** Additional information can be found in the doc/jsonb.md file of the
** canonical SQLite source tree.
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

/* Human-readable names for the JSONB values.  The index for each
** string must correspond to the JSONB_* integer above.
*/
static const char * const jsonbType[] = {
  "null", "true", "false", "integer", "integer", 
  "real", "real", "text",  "text",    "text",
  "text", "array", "object", "", "", "", ""
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
#define jsonIsspace(x) (jsonIsSpace[(unsigned char)x])

/*
** The set of all space characters recognized by jsonIsspace().
** Useful as the second argument to strspn().
*/
static const char jsonSpaces[] = "\011\012\015\040";

/*
** Characters that are special to JSON.  Control characters,
** '"' and '\\' and '\''.  Actually, '\'' is not special to
** canonical JSON, but it is special in JSON-5, so we include
** it in the set of special characters.
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

/* Objects */
typedef struct JsonCache JsonCache;
typedef struct JsonString JsonString;
typedef struct JsonParse JsonParse;

/*
** Magic number used for the JSON parse cache in sqlite3_get_auxdata()
*/
#define JSON_CACHE_ID    (-429938)  /* Cache entry */
#define JSON_CACHE_SIZE  4          /* Max number of cache entries */

/*
** jsonUnescapeOneChar() returns this invalid code point if it encounters
** a syntax error.
*/
#define JSON_INVALID_CHAR 0x99999

/* A cache mapping JSON text into JSONB blobs.
**
** Each cache entry is a JsonParse object with the following restrictions:
**
**    *   The bReadOnly flag must be set
**
**    *   The aBlob[] array must be owned by the JsonParse object.  In other
**        words, nBlobAlloc must be non-zero.
**
**    *   eEdit and delta must be zero.
**
**    *   zJson must be an RCStr.  In other words bJsonIsRCStr must be true.
*/
struct JsonCache {
  sqlite3 *db;                    /* Database connection */
  int nUsed;                      /* Number of active entries in the cache */
  JsonParse *a[JSON_CACHE_SIZE];  /* One line for each cache entry */
};

/* An instance of this object represents a JSON string
** under construction.  Really, this is a generic string accumulator
** that can be and is used to create strings other than JSON.
**
** If the generated string is longer than will fit into the zSpace[] buffer,
** then it will be an RCStr string.  This aids with caching of large
** JSON strings.
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

/* The "subtype" set for text JSON values passed through using
** sqlite3_result_subtype() and sqlite3_value_subtype().
*/
#define JSON_SUBTYPE  74    /* Ascii for "J" */

/*
** Bit values for the flags passed into various SQL function implementations
** via the sqlite3_user_data() value.
*/
#define JSON_JSON      0x01        /* Result is always JSON */
#define JSON_SQL       0x02        /* Result is always SQL */
#define JSON_ABPATH    0x03        /* Allow abbreviated JSON path specs */
#define JSON_ISSET     0x04        /* json_set(), not json_insert() */
#define JSON_BLOB      0x08        /* Use the BLOB output format */


/* A parsed JSON value.  Lifecycle:
**
**   1.  JSON comes in and is parsed into a JSONB value in aBlob.  The
**       original text is stored in zJson.  This step is skipped if the
**       input is JSONB instead of text JSON.
**
**   2.  The aBlob[] array is searched using the JSON path notation, if needed.
**       
**   3.  Zero or more changes are made to aBlob[] (via json_remove() or
**       json_replace() or json_patch() or similar).
**
**   4.  New JSON text is generated from the aBlob[] for output.  This step
**       is skipped if the function is one of the jsonb_* functions that
**       returns JSONB instead of text JSON.
*/
struct JsonParse {
  u8 *aBlob;         /* JSONB representation of JSON value */
  u32 nBlob;         /* Bytes of aBlob[] actually used */
  u32 nBlobAlloc;    /* Bytes allocated to aBlob[].  0 if aBlob is external */
  char *zJson;       /* Json text used for parsing */
  int nJson;         /* Length of the zJson string in bytes */
  u32 nJPRef;        /* Number of references to this object */
  u32 iErr;          /* Error location in zJson[] */
  u16 iDepth;        /* Nesting depth */
  u8 nErr;           /* Number of errors seen */
  u8 oom;            /* Set to true if out of memory */
  u8 bJsonIsRCStr;   /* True if zJson is an RCStr */
  u8 hasNonstd;      /* True if input uses non-standard features like JSON5 */
  u8 bReadOnly;      /* Do not modify. */
  /* Search and edit information.  See jsonLookupStep() */
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
#ifndef SQLITE_JSON_MAX_DEPTH
# define JSON_MAX_DEPTH  1000
#else
# define JSON_MAX_DEPTH SQLITE_JSON_MAX_DEPTH
#endif

/*
** Allowed values for the flgs argument to jsonParseFuncArg();
*/
#define JSON_EDITABLE  0x01   /* Generate a writable JsonParse object */
#define JSON_KEEPERROR 0x02   /* Return non-NULL even if there is an error */

/**************************************************************************
** Forward references
**************************************************************************/
static void jsonReturnStringAsBlob(JsonString*);
static int jsonFuncArgMightBeBinary(sqlite3_value *pJson);
static u32 jsonTranslateBlobToText(const JsonParse*,u32,JsonString*);
static void jsonReturnParse(sqlite3_context*,JsonParse*);
static JsonParse *jsonParseFuncArg(sqlite3_context*,sqlite3_value*,u32);
static void jsonParseFree(JsonParse*);
static u32 jsonbPayloadSize(const JsonParse*, u32, u32*);
static u32 jsonUnescapeOneChar(const char*, u32, u32*);

/**************************************************************************
** Utility routines for dealing with JsonCache objects
**************************************************************************/

/*
** Free a JsonCache object.
*/
static void jsonCacheDelete(JsonCache *p){
  int i;
  for(i=0; i<p->nUsed; i++){
    jsonParseFree(p->a[i]);
  }
  sqlite3DbFree(p->db, p);
}
static void jsonCacheDeleteGeneric(void *p){
  jsonCacheDelete((JsonCache*)p);
}

/*
** Insert a new entry into the cache.  If the cache is full, expel
** the least recently used entry.  Return SQLITE_OK on success or a
** result code otherwise.
**
** Cache entries are stored in age order, oldest first.
*/
static int jsonCacheInsert(
  sqlite3_context *ctx,   /* The SQL statement context holding the cache */
  JsonParse *pParse       /* The parse object to be added to the cache */
){
  JsonCache *p;

  assert( pParse->zJson!=0 );
  assert( pParse->bJsonIsRCStr );
  assert( pParse->delta==0 );
  p = sqlite3_get_auxdata(ctx, JSON_CACHE_ID);
  if( p==0 ){
    sqlite3 *db = sqlite3_context_db_handle(ctx);
    p = sqlite3DbMallocZero(db, sizeof(*p));
    if( p==0 ) return SQLITE_NOMEM;
    p->db = db;
    sqlite3_set_auxdata(ctx, JSON_CACHE_ID, p, jsonCacheDeleteGeneric);
    p = sqlite3_get_auxdata(ctx, JSON_CACHE_ID);
    if( p==0 ) return SQLITE_NOMEM;
  }
  if( p->nUsed >= JSON_CACHE_SIZE ){
    jsonParseFree(p->a[0]);
    memmove(p->a, &p->a[1], (JSON_CACHE_SIZE-1)*sizeof(p->a[0]));
    p->nUsed = JSON_CACHE_SIZE-1;
  }
  assert( pParse->nBlobAlloc>0 );
  pParse->eEdit = 0;
  pParse->nJPRef++;
  pParse->bReadOnly = 1;
  p->a[p->nUsed] = pParse;
  p->nUsed++;
  return SQLITE_OK;
}

/*
** Search for a cached translation the json text supplied by pArg.  Return
** the JsonParse object if found.  Return NULL if not found.
**
** When a match if found, the matching entry is moved to become the
** most-recently used entry if it isn't so already.
**
** The JsonParse object returned still belongs to the Cache and might
** be deleted at any moment.  If the caller whants the JsonParse to
** linger, it needs to increment the nPJRef reference counter.
*/
static JsonParse *jsonCacheSearch(
  sqlite3_context *ctx,    /* The SQL statement context holding the cache */
  sqlite3_value *pArg      /* Function argument containing SQL text */
){
  JsonCache *p;
  int i;
  const char *zJson;
  int nJson;

  if( sqlite3_value_type(pArg)!=SQLITE_TEXT ){
    return 0;
  }
  zJson = (const char*)sqlite3_value_text(pArg);
  if( zJson==0 ) return 0;
  nJson = sqlite3_value_bytes(pArg);

  p = sqlite3_get_auxdata(ctx, JSON_CACHE_ID);
  if( p==0 ){
    return 0;
  }
  for(i=0; i<p->nUsed; i++){
    if( p->a[i]->zJson==zJson ) break;
  }
  if( i>=p->nUsed ){
    for(i=0; i<p->nUsed; i++){
      if( p->a[i]->nJson!=nJson ) continue;
      if( memcmp(p->a[i]->zJson, zJson, nJson)==0 ) break;
    }
  }
  if( i<p->nUsed ){
    if( i<p->nUsed-1 ){
      /* Make the matching entry the most recently used entry */
      JsonParse *tmp = p->a[i];
      memmove(&p->a[i], &p->a[i+1], (p->nUsed-i-1)*sizeof(tmp));
      p->a[p->nUsed-1] = tmp;
      i = p->nUsed - 1;
    }
    assert( p->a[i]->delta==0 );
    return p->a[i];
  }else{
    return 0;
  }
}

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
  assert( N>0 );
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
**
** Return true on success.  Return false if an OOM prevents this
** from happening.
*/
static int jsonStringTerminate(JsonString *p){
  jsonAppendChar(p, 0);
  p->nUsed--;
  return p->eErr==0;
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
** under construction.  Enclose the string in double-quotes ("...") and
** escape any double-quotes or backslash characters contained within the
** string.
**
** This routine is a high-runner.  There is a measurable performance
** increase associated with unwinding the jsonIsOk[] loop.
*/
static void jsonAppendString(JsonString *p, const char *zIn, u32 N){
  u32 k;
  u8 c;
  const u8 *z = (const u8*)zIn;
  if( z==0 ) return;
  if( (N+p->nUsed+2 >= p->nAlloc) && jsonStringGrow(p,N+2)!=0 ) return;
  p->zBuf[p->nUsed++] = '"';
  while( 1 /*exit-by-break*/ ){
    k = 0;
    while( k+1<N && jsonIsOk[z[k]] && jsonIsOk[z[k+1]] ){ k += 2; } /* <--, */
    while( k<N && jsonIsOk[z[k]] ){ k++; }    /* <-- loop unwound for speed  */
    if( k>=N ){
      if( k>0 ){
        memcpy(&p->zBuf[p->nUsed], z, k);
        p->nUsed += k;
      }
      break;
    }
    if( k>0 ){
      memcpy(&p->zBuf[p->nUsed], z, k);
      p->nUsed += k;
      z += k;
      N -= k;
    }
    c = z[0];
    if( c=='"' || c=='\\' ){
      json_simple_escape:
      if( (p->nUsed+N+3 > p->nAlloc) && jsonStringGrow(p,N+3)!=0 ) return;
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
      if( (p->nUsed+N+7 > p->nAlloc) && jsonStringGrow(p,N+7)!=0 ) return;
      p->zBuf[p->nUsed++] = '\\';
      p->zBuf[p->nUsed++] = 'u';
      p->zBuf[p->nUsed++] = '0';
      p->zBuf[p->nUsed++] = '0';
      p->zBuf[p->nUsed++] = "0123456789abcdef"[c>>4];
      p->zBuf[p->nUsed++] = "0123456789abcdef"[c&0xf];
    }
    z++;
    N--;
  }
  p->zBuf[p->nUsed++] = '"';
  assert( p->nUsed<p->nAlloc );
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
        jsonTranslateBlobToText(&px, 0, p);
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
**
** If pParse and ctx are both non-NULL, then the SQL string in p is
** loaded into the zJson field of the pParse object as a RCStr and the
** pParse is added to the cache.
*/
static void jsonReturnString(
  JsonString *p,            /* String to return */
  JsonParse *pParse,        /* JSONB source or NULL */
  sqlite3_context *ctx      /* Where to cache */
){
  assert( (pParse!=0)==(ctx!=0) );
  assert( ctx==0 || ctx==p->pCtx );
  if( p->eErr==0 ){
    int flags = SQLITE_PTR_TO_INT(sqlite3_user_data(p->pCtx));
    if( flags & JSON_BLOB ){
      jsonReturnStringAsBlob(p);
    }else if( p->bStatic ){
      sqlite3_result_text64(p->pCtx, p->zBuf, p->nUsed,
                            SQLITE_TRANSIENT, SQLITE_UTF8);
    }else if( jsonStringTerminate(p) ){
      if( pParse && pParse->bJsonIsRCStr==0 && pParse->nBlobAlloc>0 ){
        int rc;
        pParse->zJson = sqlite3RCStrRef(p->zBuf);
        pParse->nJson = p->nUsed;
        pParse->bJsonIsRCStr = 1;
        rc = jsonCacheInsert(ctx, pParse);
        if( rc==SQLITE_NOMEM ){
          sqlite3_result_error_nomem(ctx);
          jsonStringReset(p);
          return;
        }
      }
      sqlite3_result_text64(p->pCtx, sqlite3RCStrRef(p->zBuf), p->nUsed,
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
** Utility routines for dealing with JsonParse objects
**************************************************************************/

/*
** Reclaim all memory allocated by a JsonParse object.  But do not
** delete the JsonParse object itself.
*/
static void jsonParseReset(JsonParse *pParse){
  assert( pParse->nJPRef<=1 );
  if( pParse->bJsonIsRCStr ){
    sqlite3RCStrUnref(pParse->zJson);
    pParse->zJson = 0;
    pParse->nJson = 0;
    pParse->bJsonIsRCStr = 0;
  }
  if( pParse->nBlobAlloc ){
    sqlite3_free(pParse->aBlob);
    pParse->aBlob = 0;
    pParse->nBlob = 0;
    pParse->nBlobAlloc = 0;
  }
}

/*
** Decrement the reference count on the JsonParse object.  When the
** count reaches zero, free the object.
*/
static void jsonParseFree(JsonParse *pParse){
  if( pParse ){
    if( pParse->nJPRef>1 ){
      pParse->nJPRef--;
    }else{
      jsonParseReset(pParse);
      sqlite3_free(pParse);
    }
  }
}

/**************************************************************************
** Utility routines for the JSON text parser
**************************************************************************/

/*
** Translate a single byte of Hex into an integer.
** This routine only gives a correct answer if h really is a valid hexadecimal
** character:  0..9a..fA..F.  But unlike sqlite3HexToInt(), it does not
** assert() if the digit is not hex.
*/
static u8 jsonHexToInt(int h){
#ifdef SQLITE_ASCII
  h += 9*(1&(h>>6));
#endif
#ifdef SQLITE_EBCDIC
  h += 9*(1&~(h>>4));
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
  { 'i', 'I', 3, JSONB_FLOAT, 7, "inf", "9.0e999" },
  { 'i', 'I', 8, JSONB_FLOAT, 7, "infinity", "9.0e999" },
  { 'n', 'N', 3, JSONB_NULL, 4, "NaN", "null" },
  { 'q', 'Q', 4, JSONB_NULL, 4, "QNaN", "null" },
  { 's', 'S', 4, JSONB_NULL, 4, "SNaN", "null" },
};


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
  assert( !pParse->bReadOnly );
  if( pParse->oom ) return 0;
  if( pParse->nBlobAlloc>0 ) return 1;
  aOld = pParse->aBlob;
  nSize = pParse->nBlob + nExtra;
  pParse->aBlob = 0;
  if( jsonBlobExpand(pParse, nSize) ){
    return 0;
  }
  assert( pParse->nBlobAlloc >= pParse->nBlob + nExtra );
  memcpy(pParse->aBlob, aOld, pParse->nBlob);
  return 1;
}

/* Expand pParse->aBlob and append one bytes.
*/
static SQLITE_NOINLINE void jsonBlobExpandAndAppendOneByte(
  JsonParse *pParse,
  u8 c
){
  jsonBlobExpand(pParse, pParse->nBlob+1);
  if( pParse->oom==0 ){
    assert( pParse->nBlob+1<=pParse->nBlobAlloc );
    pParse->aBlob[pParse->nBlob++] = c;
  }
}

/* Append a single character.
*/
static void jsonBlobAppendOneByte(JsonParse *pParse, u8 c){
  if( pParse->nBlob >= pParse->nBlobAlloc ){
    jsonBlobExpandAndAppendOneByte(pParse, c);
  }else{
    pParse->aBlob[pParse->nBlob++] = c;
  }
}

/* Slow version of jsonBlobAppendNode() that first resizes the
** pParse->aBlob structure.
*/
static void jsonBlobAppendNode(JsonParse*,u8,u32,const void*);
static SQLITE_NOINLINE void jsonBlobExpandAndAppendNode(
  JsonParse *pParse,
  u8 eType,
  u32 szPayload,
  const void *aPayload
){
  if( jsonBlobExpand(pParse, pParse->nBlob+szPayload+9) ) return;
  jsonBlobAppendNode(pParse, eType, szPayload, aPayload);
}


/* Append an node type byte together with the payload size and
** possibly also the payload.
**
** If aPayload is not NULL, then it is a pointer to the payload which
** is also appended.  If aPayload is NULL, the pParse->aBlob[] array
** is resized (if necessary) so that it is big enough to hold the
** payload, but the payload is not appended and pParse->nBlob is left
** pointing to where the first byte of payload will eventually be.
*/
static void jsonBlobAppendNode(
  JsonParse *pParse,          /* The JsonParse object under construction */
  u8 eType,                   /* Node type.  One of JSONB_* */
  u32 szPayload,              /* Number of bytes of payload */
  const void *aPayload        /* The payload.  Might be NULL */
){
  u8 *a;
  if( pParse->nBlob+szPayload+9 > pParse->nBlobAlloc ){
    jsonBlobExpandAndAppendNode(pParse,eType,szPayload,aPayload);
    return;
  }
  assert( pParse->aBlob!=0 );
  a = &pParse->aBlob[pParse->nBlob];
  if( szPayload<=11 ){
    a[0] = eType | (szPayload<<4);
    pParse->nBlob += 1;
  }else if( szPayload<=0xff ){
    a[0] = eType | 0xc0;
    a[1] = szPayload & 0xff;
    pParse->nBlob += 2;
  }else if( szPayload<=0xffff ){
    a[0] = eType | 0xd0;
    a[1] = (szPayload >> 8) & 0xff;
    a[2] = szPayload & 0xff;
    pParse->nBlob += 3;
  }else{
    a[0] = eType | 0xe0;
    a[1] = (szPayload >> 24) & 0xff;
    a[2] = (szPayload >> 16) & 0xff;
    a[3] = (szPayload >> 8) & 0xff;
    a[4] = szPayload & 0xff;
    pParse->nBlob += 5;
  }
  if( aPayload ){
    pParse->nBlob += szPayload;
    memcpy(&pParse->aBlob[pParse->nBlob-szPayload], aPayload, szPayload);
  }
}

/* Change the payload size for the node at index i to be szPayload.
*/
static int jsonBlobChangePayloadSize(
  JsonParse *pParse,
  u32 i,
  u32 szPayload
){
  u8 *a;
  u8 szType;
  u8 nExtra;
  u8 nNeeded;
  int delta;
  if( pParse->oom ) return 0;
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
        return 0;  /* OOM error.  Error state recorded in pParse->oom. */
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
  return delta;
}

/*
** If z[0] is 'u' and is followed by exactly 4 hexadecimal character,
** then set *pOp to JSONB_TEXTJ and return true.  If not, do not make
** any changes to *pOp and return false.
*/
static int jsonIs4HexB(const char *z, int *pOp){
  if( z[0]!='u' ) return 0;
  if( !jsonIs4Hex(&z[1]) ) return 0;
  *pOp = JSONB_TEXTJ;
  return 1;
}

/*
** Check a single element of the JSONB in pParse for validity.
**
** The element to be checked starts at offset i and must end at on the
** last byte before iEnd.
**
** Return 0 if everything is correct.  Return the 1-based byte offset of the
** error if a problem is detected.  (In other words, if the error is at offset
** 0, return 1).
*/
static u32 jsonbValidityCheck(
  const JsonParse *pParse,    /* Input JSONB.  Only aBlob and nBlob are used */
  u32 i,                      /* Start of element as pParse->aBlob[i] */
  u32 iEnd,                   /* One more than the last byte of the element */
  u32 iDepth                  /* Current nesting depth */
){
  u32 n, sz, j, k;
  const u8 *z;
  u8 x;
  if( iDepth>JSON_MAX_DEPTH ) return i+1;
  sz = 0;
  n = jsonbPayloadSize(pParse, i, &sz);
  if( NEVER(n==0) ) return i+1;          /* Checked by caller */
  if( NEVER(i+n+sz!=iEnd) ) return i+1;  /* Checked by caller */
  z = pParse->aBlob;
  x = z[i] & 0x0f;
  switch( x ){
    case JSONB_NULL:
    case JSONB_TRUE:
    case JSONB_FALSE: {
      return n+sz==1 ? 0 : i+1;
    }
    case JSONB_INT: {
      if( sz<1 ) return i+1;
      j = i+n;
      if( z[j]=='-' ){
        j++;
        if( sz<2 ) return i+1;
      }
      k = i+n+sz;
      while( j<k ){
        if( sqlite3Isdigit(z[j]) ){
          j++;
        }else{
          return j+1;
        }
      }
      return 0;
    }
    case JSONB_INT5: {
      if( sz<3 ) return i+1;
      j = i+n;
      if( z[j]=='-' ){
        if( sz<4 ) return i+1;
        j++;
      }
      if( z[j]!='0' ) return i+1;
      if( z[j+1]!='x' && z[j+1]!='X' ) return j+2;
      j += 2;
      k = i+n+sz;
      while( j<k ){
        if( sqlite3Isxdigit(z[j]) ){
          j++;
        }else{
          return j+1;
        }
      }
      return 0;
    }
    case JSONB_FLOAT:
    case JSONB_FLOAT5: {
      u8 seen = 0;   /* 0: initial.  1: '.' seen  2: 'e' seen */
      if( sz<2 ) return i+1;
      j = i+n;
      k = j+sz;
      if( z[j]=='-' ){
        j++;
        if( sz<3 ) return i+1;
      }
      if( z[j]=='.' ){
        if( x==JSONB_FLOAT ) return j+1;
        if( !sqlite3Isdigit(z[j+1]) ) return j+1;
        j += 2;
        seen = 1;
      }else if( z[j]=='0' && x==JSONB_FLOAT ){
        if( j+3>k ) return j+1;
        if( z[j+1]!='.' && z[j+1]!='e' && z[j+1]!='E' ) return j+1;
        j++;
      }
      for(; j<k; j++){
        if( sqlite3Isdigit(z[j]) ) continue;
        if( z[j]=='.' ){
          if( seen>0 ) return j+1;
          if( x==JSONB_FLOAT && (j==k-1 || !sqlite3Isdigit(z[j+1])) ){
            return j+1;
          }
          seen = 1;
          continue;
        }
        if( z[j]=='e' || z[j]=='E' ){
          if( seen==2 ) return j+1;
          if( j==k-1 ) return j+1;
          if( z[j+1]=='+' || z[j+1]=='-' ){
            j++;
            if( j==k-1 ) return j+1;
          }
          seen = 2;
          continue;
        }
        return j+1;
      }
      if( seen==0 ) return i+1;
      return 0;
    }
    case JSONB_TEXT: {
      j = i+n;
      k = j+sz;
      while( j<k ){
        if( !jsonIsOk[z[j]] && z[j]!='\'' ) return j+1;
        j++;
      }
      return 0;
    }
    case JSONB_TEXTJ:
    case JSONB_TEXT5: {
      j = i+n;
      k = j+sz;
      while( j<k ){
        if( !jsonIsOk[z[j]] && z[j]!='\'' ){
          if( z[j]=='"' ){
            if( x==JSONB_TEXTJ ) return j+1;
          }else if( z[j]!='\\' || j+1>=k ){
            return j+1;
          }else if( strchr("\"\\/bfnrt",z[j+1])!=0 ){
            j++;
          }else if( z[j+1]=='u' ){
            if( j+5>=k ) return j+1;
            if( !jsonIs4Hex((const char*)&z[j+2]) ) return j+1;
            j++;
          }else if( x!=JSONB_TEXT5 ){
            return j+1;
          }else{
            u32 c = 0;
            u32 szC = jsonUnescapeOneChar((const char*)&z[j], k-j, &c);
            if( c==JSON_INVALID_CHAR ) return j+1;
            j += szC - 1;
          }
        }
        j++;
      }
      return 0;
    }
    case JSONB_TEXTRAW: {
      return 0;
    }
    case JSONB_ARRAY: {
      u32 sub;
      j = i+n;
      k = j+sz;
      while( j<k ){
        sz = 0;
        n = jsonbPayloadSize(pParse, j, &sz);
        if( n==0 ) return j+1;
        if( j+n+sz>k ) return j+1;
        sub = jsonbValidityCheck(pParse, j, j+n+sz, iDepth+1);
        if( sub ) return sub;
        j += n + sz;
      }
      assert( j==k );
      return 0;
    }
    case JSONB_OBJECT: {
      u32 cnt = 0;
      u32 sub;
      j = i+n;
      k = j+sz;
      while( j<k ){
        sz = 0;
        n = jsonbPayloadSize(pParse, j, &sz);
        if( n==0 ) return j+1;
        if( j+n+sz>k ) return j+1;
        if( (cnt & 1)==0 ){
          x = z[j] & 0x0f;
          if( x<JSONB_TEXT || x>JSONB_TEXTRAW ) return j+1;
        }
        sub = jsonbValidityCheck(pParse, j, j+n+sz, iDepth+1);
        if( sub ) return sub;
        cnt++;
        j += n + sz;
      }
      assert( j==k );
      if( (cnt & 1)!=0 ) return j+1;
      return 0;
    }
    default: {
      return i+1;
    }
  }
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
**     -1    Syntax error or OOM
**     -2    '}' seen   \
**     -3    ']' seen    \___  For these returns, pParse->iErr is set to
**     -4    ',' seen    /     the index in zJson[] of the seen character
**     -5    ':' seen   /
*/
static int jsonTranslateTextToBlob(JsonParse *pParse, u32 i){
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
    jsonBlobAppendNode(pParse, JSONB_OBJECT, pParse->nJson-i, 0);
    if( ++pParse->iDepth > JSON_MAX_DEPTH ){
      pParse->iErr = i;
      return -1;
    }
    iStart = pParse->nBlob;
    for(j=i+1;;j++){
      u32 iBlob = pParse->nBlob;
      x = jsonTranslateTextToBlob(pParse, j);
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
          jsonBlobAppendNode(pParse, op, k-j, &z[j]);
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
        if( jsonIsspace(z[j]) ){
          /* strspn() is not helpful here */
          do{ j++; }while( jsonIsspace(z[j]) );
          if( z[j]==':' ){
            j++;
            goto parse_object_value;
          }
        }
        x = jsonTranslateTextToBlob(pParse, j);
        if( x!=(-5) ){
          if( x!=(-1) ) pParse->iErr = j;
          return -1;
        }
        j = pParse->iErr+1;
      }
    parse_object_value:
      x = jsonTranslateTextToBlob(pParse, j);
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
        if( jsonIsspace(z[j]) ){
          j += 1 + (u32)strspn(&z[j+1], jsonSpaces);
          if( z[j]==',' ){
            continue;
          }else if( z[j]=='}' ){
            break;
          }
        }
        x = jsonTranslateTextToBlob(pParse, j);
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
    jsonBlobAppendNode(pParse, JSONB_ARRAY, pParse->nJson - i, 0);
    iStart = pParse->nBlob;
    if( pParse->oom ) return -1;
    if( ++pParse->iDepth > JSON_MAX_DEPTH ){
      pParse->iErr = i;
      return -1;
    }
    for(j=i+1;;j++){
      x = jsonTranslateTextToBlob(pParse, j);
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
        if( jsonIsspace(z[j]) ){
          j += 1 + (u32)strspn(&z[j+1], jsonSpaces);
          if( z[j]==',' ){
            continue;
          }else if( z[j]==']' ){
            break;
          }
        }
        x = jsonTranslateTextToBlob(pParse, j);
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
    pParse->hasNonstd = 1;
    opcode = JSONB_TEXT;
    goto parse_string;
  case '"':
    /* Parse string */
    opcode = JSONB_TEXT;
  parse_string:
    cDelim = z[i];
    j = i+1;
    while( 1 /*exit-by-break*/ ){
      if( jsonIsOk[(u8)z[j]] ){
        if( !jsonIsOk[(u8)z[j+1]] ){
          j += 1;
        }else if( !jsonIsOk[(u8)z[j+2]] ){
          j += 2;
        }else{
          j += 3;
          continue;
        }
      }
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
      }else if( c=='"' ){
        opcode = JSONB_TEXT5;
      }
      j++;
    }
    jsonBlobAppendNode(pParse, opcode, j-1-i, &z[i+1]);
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
              jsonBlobAppendNode(pParse, JSONB_FLOAT, 6, "-9e999");
            }else{
              jsonBlobAppendNode(pParse, JSONB_FLOAT, 5, "9e999");
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
    jsonBlobAppendNode(pParse, JSONB_INT+t, j-i, &z[i]);
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
    i += 1 + (u32)strspn(&z[i+1], jsonSpaces);
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
      if( aNanInfName[k].eType==JSONB_FLOAT ){
        jsonBlobAppendNode(pParse, JSONB_FLOAT, 5, "9e999");
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
  i = jsonTranslateTextToBlob(pParse, 0);
  if( pParse->oom ) i = -1;
  if( i>0 ){
#ifdef SQLITE_DEBUG
    assert( pParse->iDepth==0 );
    if( sqlite3Config.bJsonSelfcheck ){
      assert( jsonbValidityCheck(pParse, 0, pParse->nBlob, 0)==0 );
    }   
#endif
    while( jsonIsspace(zJson[i]) ) i++;
    if( zJson[i] ){
      i += json5Whitespace(&zJson[i]);
      if( zJson[i] ){
        if( pCtx ) sqlite3_result_error(pCtx, "malformed JSON", -1);
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
  (void)jsonTranslateTextToBlob(&px, 0);
  if( px.oom ){
    sqlite3_free(px.aBlob);
    sqlite3_result_error_nomem(pStr->pCtx);
  }else{
    assert( px.nBlobAlloc>0 );
    assert( !px.bReadOnly );
    sqlite3_result_blob(pStr->pCtx, px.aBlob, px.nBlob, sqlite3_free);
  }
}

/* The byte at index i is a node type-code.  This routine
** determines the payload size for that node and writes that
** payload size in to *pSz.  It returns the offset from i to the
** beginning of the payload.  Return 0 on error.
*/
static u32 jsonbPayloadSize(const JsonParse *pParse, u32 i, u32 *pSz){
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
  }else if( x==14 ){
    if( i+4>=pParse->nBlob ){
      *pSz = 0;
      return 0;
    }
    sz = ((u32)pParse->aBlob[i+1]<<24) + (pParse->aBlob[i+2]<<16) +
         (pParse->aBlob[i+3]<<8) + pParse->aBlob[i+4];
    n = 5;
  }else{
    if( i+8>=pParse->nBlob
     || pParse->aBlob[i+1]!=0
     || pParse->aBlob[i+2]!=0
     || pParse->aBlob[i+3]!=0
     || pParse->aBlob[i+4]!=0
    ){
      *pSz = 0;
      return 0;
    }
    sz = (pParse->aBlob[i+5]<<24) + (pParse->aBlob[i+6]<<16) +
         (pParse->aBlob[i+7]<<8) + pParse->aBlob[i+8];
    n = 9;
  }
  if( i+sz+n > pParse->nBlob
   && i+sz+n > pParse->nBlob-pParse->delta
  ){
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
static u32 jsonTranslateBlobToText(
  const JsonParse *pParse,       /* the complete parse of the JSON */
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
        for(k=0; k<sz2 && zIn[k]!='\\' && zIn[k]!='"'; k++){}
        if( k>0 ){
          jsonAppendRawNZ(pOut, zIn, k);
          if( k>=sz2 ){
            break;
          }
          zIn += k;
          sz2 -= k;
        }
        if( zIn[0]=='"' ){
          jsonAppendRawNZ(pOut, "\\\"", 2);
          zIn++;
          sz2--;
          continue;
        }
        assert( zIn[0]=='\\' );
        assert( sz2>=1 );
        if( sz2<2 ){
          pOut->eErr |= JSTRING_MALFORMED;
          break;
        }
        switch( (u8)zIn[1] ){
          case '\'':
            jsonAppendChar(pOut, '\'');
            break;
          case 'v':
            jsonAppendRawNZ(pOut, "\\u0009", 6);
            break;
          case 'x':
            if( sz2<4 ){
              pOut->eErr |= JSTRING_MALFORMED;
              sz2 = 2;
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
              sz2 = 2;
              break;
            }
            zIn += 2;
            sz2 -= 2;
            break;
          default:
            jsonAppendRawNZ(pOut, zIn, 2);
            break;
        }
        assert( sz2>=2 );
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
        j = jsonTranslateBlobToText(pParse, j, pOut);
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
        j = jsonTranslateBlobToText(pParse, j, pOut);
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
  aBlob = sqlite3_value_blob(pJson);
  nBlob = sqlite3_value_bytes(pJson);
  if( nBlob<1 ) return 0;
  if( NEVER(aBlob==0) || (aBlob[0] & 0x0f)>JSONB_OBJECT ) return 0;
  memset(&s, 0, sizeof(s));
  s.aBlob = (u8*)aBlob;
  s.nBlob = nBlob;
  n = jsonbPayloadSize(&s, 0, &sz);
  if( n==0 ) return 0;
  if( sz+n!=(u32)nBlob ) return 0;
  if( (aBlob[0] & 0x0f)<=JSONB_FALSE && sz>0 ) return 0;
  return sz+n==(u32)nBlob;
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
** Edit the payload size of the element at iRoot by the amount in
** pParse->delta.
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
  pParse->delta += jsonBlobChangePayloadSize(pParse, iRoot, sz);
}

/*
** Modify the JSONB blob at pParse->aBlob by removing nDel bytes of
** content beginning at iDel, and replacing them with nIns bytes of
** content given by aIns.
**
** nDel may be zero, in which case no bytes are removed.  But iDel is
** still important as new bytes will be insert beginning at iDel.
**
** aIns may be zero, in which case space is created to hold nIns bytes
** beginning at iDel, but that space is uninitialized.
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
  if( nIns && aIns ) memcpy(&pParse->aBlob[iDel], aIns, nIns);
}

/*
** Return the number of escaped newlines to be ignored.
** An escaped newline is a one of the following byte sequences:
**
**    0x5c 0x0a
**    0x5c 0x0d
**    0x5c 0x0d 0x0a
**    0x5c 0xe2 0x80 0xa8
**    0x5c 0xe2 0x80 0xa9
*/
static u32 jsonBytesToBypass(const char *z, u32 n){
  u32 i = 0;
  while( i+1<n ){
    if( z[i]!='\\' ) return i;
    if( z[i+1]=='\n' ){
      i += 2;
      continue;
    }
    if( z[i+1]=='\r' ){
      if( i+2<n && z[i+2]=='\n' ){
        i += 3;
      }else{
        i += 2;
      }
      continue;
    }
    if( 0xe2==(u8)z[i+1]
     && i+3<n
     && 0x80==(u8)z[i+2]
     && (0xa8==(u8)z[i+3] || 0xa9==(u8)z[i+3])
    ){
      i += 4;
      continue;
    }
    break;
  }
  return i;
}

/*
** Input z[0..n] defines JSON escape sequence including the leading '\\'.
** Decode that escape sequence into a single character.  Write that
** character into *piOut.  Return the number of bytes in the escape sequence.
**
** If there is a syntax error of some kind (for example too few characters
** after the '\\' to complete the encoding) then *piOut is set to
** JSON_INVALID_CHAR.
*/
static u32 jsonUnescapeOneChar(const char *z, u32 n, u32 *piOut){
  assert( n>0 );
  assert( z[0]=='\\' );
  if( n<2 ){
    *piOut = JSON_INVALID_CHAR;
    return n;
  }
  switch( (u8)z[1] ){
    case 'u': {
      u32 v, vlo;
      if( n<6 ){
        *piOut = JSON_INVALID_CHAR;
        return n;
      }
      v = jsonHexToInt4(&z[2]);
      if( (v & 0xfc00)==0xd800
       && n>=12
       && z[6]=='\\'
       && z[7]=='u'
       && ((vlo = jsonHexToInt4(&z[8]))&0xfc00)==0xdc00
      ){
        *piOut = ((v&0x3ff)<<10) + (vlo&0x3ff) + 0x10000;
        return 12;
      }else{
        *piOut = v;
        return 6;
      }
    }
    case 'b': {   *piOut = '\b';  return 2; }
    case 'f': {   *piOut = '\f';  return 2; }
    case 'n': {   *piOut = '\n';  return 2; }
    case 'r': {   *piOut = '\r';  return 2; }
    case 't': {   *piOut = '\t';  return 2; }
    case 'v': {   *piOut = '\v';  return 2; }
    case '0': {   *piOut = 0;     return 2; }
    case '\'':
    case '"':
    case '/':
    case '\\':{   *piOut = z[1];  return 2; }
    case 'x': {
      if( n<4 ){
        *piOut = JSON_INVALID_CHAR;
        return n;
      }
      *piOut = (jsonHexToInt(z[2])<<4) | jsonHexToInt(z[3]);
      return 4;
    }
    case 0xe2:
    case '\r':
    case '\n': {
      u32 nSkip = jsonBytesToBypass(z, n);
      if( nSkip==0 ){
        *piOut = JSON_INVALID_CHAR;
        return n;
      }else if( nSkip==n ){
        *piOut = 0;
        return n;
      }else if( z[nSkip]=='\\' ){
        return nSkip + jsonUnescapeOneChar(&z[nSkip], n-nSkip, piOut);
      }else{
        int sz = sqlite3Utf8ReadLimited((u8*)&z[nSkip], n-nSkip, piOut);
        return nSkip + sz;
      }
    }
    default: {
      *piOut = JSON_INVALID_CHAR;
      return 2;
    }
  }
}


/*
** Compare two object labels.  Return 1 if they are equal and
** 0 if they differ.
**
** In this version, we know that one or the other or both of the
** two comparands contains an escape sequence.
*/
static SQLITE_NOINLINE int jsonLabelCompareEscaped(
  const char *zLeft,          /* The left label */
  u32 nLeft,                  /* Size of the left label in bytes */
  int rawLeft,                /* True if zLeft contains no escapes */
  const char *zRight,         /* The right label */
  u32 nRight,                 /* Size of the right label in bytes */
  int rawRight                /* True if zRight is escape-free */
){
  u32 cLeft, cRight;
  assert( rawLeft==0 || rawRight==0 );
  while( 1 /*exit-by-return*/ ){
    if( nLeft==0 ){
      cLeft = 0;
    }else if( rawLeft || zLeft[0]!='\\' ){
      cLeft = ((u8*)zLeft)[0];
      if( cLeft>=0xc0 ){
        int sz = sqlite3Utf8ReadLimited((u8*)zLeft, nLeft, &cLeft);
        zLeft += sz;
        nLeft -= sz;
      }else{
        zLeft++;
        nLeft--;
      }
    }else{
      u32 n = jsonUnescapeOneChar(zLeft, nLeft, &cLeft);
      zLeft += n;
      assert( n<=nLeft );
      nLeft -= n;
    }
    if( nRight==0 ){
      cRight = 0;
    }else if( rawRight || zRight[0]!='\\' ){
      cRight = ((u8*)zRight)[0];
      if( cRight>=0xc0 ){
        int sz = sqlite3Utf8ReadLimited((u8*)zRight, nRight, &cRight);
        zRight += sz;
        nRight -= sz;
      }else{
        zRight++;
        nRight--;
      }
    }else{
      u32 n = jsonUnescapeOneChar(zRight, nRight, &cRight);
      zRight += n;
      assert( n<=nRight );
      nRight -= n;
    }
    if( cLeft!=cRight ) return 0;
    if( cLeft==0 ) return 1;
  }
}

/*
** Compare two object labels.  Return 1 if they are equal and
** 0 if they differ.  Return -1 if an OOM occurs.
*/
static int jsonLabelCompare(
  const char *zLeft,          /* The left label */
  u32 nLeft,                  /* Size of the left label in bytes */
  int rawLeft,                /* True if zLeft contains no escapes */
  const char *zRight,         /* The right label */
  u32 nRight,                 /* Size of the right label in bytes */
  int rawRight                /* True if zRight is escape-free */
){
  if( rawLeft && rawRight ){
    /* Simpliest case:  Neither label contains escapes.  A simple
    ** memcmp() is sufficient. */
    if( nLeft!=nRight ) return 0;
    return memcmp(zLeft, zRight, nLeft)==0;
  }else{
    return jsonLabelCompareEscaped(zLeft, nLeft, rawLeft,
                                   zRight, nRight, rawRight);
  }
}

/*
** Error returns from jsonLookupStep()
*/
#define JSON_LOOKUP_ERROR      0xffffffff
#define JSON_LOOKUP_NOTFOUND   0xfffffffe
#define JSON_LOOKUP_PATHERROR  0xfffffffd
#define JSON_LOOKUP_ISERROR(x) ((x)>=JSON_LOOKUP_PATHERROR)

/* Forward declaration */
static u32 jsonLookupStep(JsonParse*,u32,const char*,u32);


/* This helper routine for jsonLookupStep() populates pIns with
** binary data that is to be inserted into pParse.
**
** In the common case, pIns just points to pParse->aIns and pParse->nIns.
** But if the zPath of the original edit operation includes path elements
** that go deeper, additional substructure must be created.
**
** For example:
**
**     json_insert('{}', '$.a.b.c', 123);
**
** The search stops at '$.a'  But additional substructure must be
** created for the ".b.c" part of the patch so that the final result
** is:  {"a":{"b":{"c"::123}}}.  This routine populates pIns with
** the binary equivalent of {"b":{"c":123}} so that it can be inserted.
**
** The caller is responsible for resetting pIns when it has finished
** using the substructure.
*/
static u32 jsonCreateEditSubstructure(
  JsonParse *pParse,  /* The original JSONB that is being edited */
  JsonParse *pIns,    /* Populate this with the blob data to insert */
  const char *zTail   /* Tail of the path that determins substructure */
){
  static const u8 emptyObject[] = { JSONB_ARRAY, JSONB_OBJECT };
  int rc;
  memset(pIns, 0, sizeof(*pIns));
  if( zTail[0]==0 ){
    /* No substructure.  Just insert what is given in pParse. */
    pIns->aBlob = pParse->aIns;
    pIns->nBlob = pParse->nIns;
    rc = 0;
  }else{
    /* Construct the binary substructure */
    pIns->nBlob = 1;
    pIns->aBlob = (u8*)&emptyObject[zTail[0]=='.'];
    pIns->eEdit = pParse->eEdit;
    pIns->nIns = pParse->nIns;
    pIns->aIns = pParse->aIns;
    rc = jsonLookupStep(pIns, 0, zTail, 0);
    pParse->oom |= pIns->oom;
  }
  return rc;  /* Error code only */
}

/*
** Search along zPath to find the Json element specified.  Return an
** index into pParse->aBlob[] for the start of that element's value.
**
** If the value found by this routine is the value half of label/value pair
** within an object, then set pPath->iLabel to the start of the corresponding
** label, before returning.
**
** Return one of the JSON_LOOKUP error codes if problems are seen.
**
** This routine will also modify the blob.  If pParse->eEdit is one of
** JEDIT_DEL, JEDIT_REPL, JEDIT_INS, or JEDIT_SET, then changes might be
** made to the selected value.  If an edit is performed, then the return
** value does not necessarily point to the select element.  If an edit
** is performed, the return value is only useful for detecting error
** conditions.
*/
static u32 jsonLookupStep(
  JsonParse *pParse,      /* The JSON to search */
  u32 iRoot,              /* Begin the search at this element of aBlob[] */
  const char *zPath,      /* The path to search */
  u32 iLabel              /* Label if iRoot is a value of in an object */
){
  u32 i, j, k, nKey, sz, n, iEnd, rc;
  const char *zKey;
  u8 x;

  if( zPath[0]==0 ){
    if( pParse->eEdit && jsonBlobMakeEditable(pParse, pParse->nIns) ){
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
    int rawKey = 1;
    x = pParse->aBlob[iRoot];
    zPath++;
    if( zPath[0]=='"' ){
      zKey = zPath + 1;
      for(i=1; zPath[i] && zPath[i]!='"'; i++){}
      nKey = i-1;
      if( zPath[i] ){
        i++;
      }else{
        return JSON_LOOKUP_PATHERROR;
      }
      testcase( nKey==0 );
      rawKey = memchr(zKey, '\\', nKey)==0;
    }else{
      zKey = zPath;
      for(i=0; zPath[i] && zPath[i]!='.' && zPath[i]!='['; i++){}
      nKey = i;
      if( nKey==0 ){
        return JSON_LOOKUP_PATHERROR;
      }
    }
    if( (x & 0x0f)!=JSONB_OBJECT ) return JSON_LOOKUP_NOTFOUND;
    n = jsonbPayloadSize(pParse, iRoot, &sz);
    j = iRoot + n;  /* j is the index of a label */
    iEnd = j+sz;
    while( j<iEnd ){
      int rawLabel;
      const char *zLabel;
      x = pParse->aBlob[j] & 0x0f;
      if( x<JSONB_TEXT || x>JSONB_TEXTRAW ) return JSON_LOOKUP_ERROR;
      n = jsonbPayloadSize(pParse, j, &sz);
      if( n==0 ) return JSON_LOOKUP_ERROR;
      k = j+n;  /* k is the index of the label text */
      if( k+sz>=iEnd ) return JSON_LOOKUP_ERROR;
      zLabel = (const char*)&pParse->aBlob[k];
      rawLabel = x==JSONB_TEXT || x==JSONB_TEXTRAW;
      if( jsonLabelCompare(zKey, nKey, rawKey, zLabel, sz, rawLabel) ){
        u32 v = k+sz;  /* v is the index of the value */
        if( ((pParse->aBlob[v])&0x0f)>JSONB_OBJECT ) return JSON_LOOKUP_ERROR;
        n = jsonbPayloadSize(pParse, v, &sz);
        if( n==0 || v+n+sz>iEnd ) return JSON_LOOKUP_ERROR;
        assert( j>0 );
        rc = jsonLookupStep(pParse, v, &zPath[i], j);
        if( pParse->delta ) jsonAfterEditSizeAdjust(pParse, iRoot);
        return rc;
      }
      j = k+sz;
      if( ((pParse->aBlob[j])&0x0f)>JSONB_OBJECT ) return JSON_LOOKUP_ERROR;
      n = jsonbPayloadSize(pParse, j, &sz);
      if( n==0 ) return JSON_LOOKUP_ERROR;
      j += n+sz;
    }
    if( j>iEnd ) return JSON_LOOKUP_ERROR;
    if( pParse->eEdit>=JEDIT_INS ){
      u32 nIns;          /* Total bytes to insert (label+value) */
      JsonParse v;       /* BLOB encoding of the value to be inserted */
      JsonParse ix;      /* Header of the label to be inserted */
      testcase( pParse->eEdit==JEDIT_INS );
      testcase( pParse->eEdit==JEDIT_SET );
      memset(&ix, 0, sizeof(ix));
      jsonBlobAppendNode(&ix, rawKey?JSONB_TEXTRAW:JSONB_TEXT5, nKey, 0);
      pParse->oom |= ix.oom;
      rc = jsonCreateEditSubstructure(pParse, &v, &zPath[i]);
      if( !JSON_LOOKUP_ISERROR(rc)
       && jsonBlobMakeEditable(pParse, ix.nBlob+nKey+v.nBlob)
      ){
        assert( !pParse->oom );
        nIns = ix.nBlob + nKey + v.nBlob;
        jsonBlobEdit(pParse, j, 0, 0, nIns);
        if( !pParse->oom ){
          assert( pParse->aBlob!=0 ); /* Because pParse->oom!=0 */
          assert( ix.aBlob!=0 );      /* Because pPasre->oom!=0 */
          memcpy(&pParse->aBlob[j], ix.aBlob, ix.nBlob);
          k = j + ix.nBlob;
          memcpy(&pParse->aBlob[k], zKey, nKey);
          k += nKey;
          memcpy(&pParse->aBlob[k], v.aBlob, v.nBlob);
          if( ALWAYS(pParse->delta) ) jsonAfterEditSizeAdjust(pParse, iRoot);
        }
      }
      jsonParseReset(&v);
      jsonParseReset(&ix);
      return rc;
    }
  }else if( zPath[0]=='[' ){
    x = pParse->aBlob[iRoot] & 0x0f;
    if( x!=JSONB_ARRAY )  return JSON_LOOKUP_NOTFOUND;
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
          if( nn>k ) return JSON_LOOKUP_NOTFOUND;
          k -= nn;
        }
        if( zPath[i]!=']' ){
          return JSON_LOOKUP_PATHERROR;
        }
      }else{
        return JSON_LOOKUP_PATHERROR;
      }
    }
    j = iRoot+n;
    iEnd = j+sz;
    while( j<iEnd ){
      if( k==0 ){
        rc = jsonLookupStep(pParse, j, &zPath[i+1], 0);
        if( pParse->delta ) jsonAfterEditSizeAdjust(pParse, iRoot);
        return rc;
      }
      k--;
      n = jsonbPayloadSize(pParse, j, &sz);
      if( n==0 ) return JSON_LOOKUP_ERROR;
      j += n+sz;
    }
    if( j>iEnd ) return JSON_LOOKUP_ERROR;
    if( k>0 ) return JSON_LOOKUP_NOTFOUND;
    if( pParse->eEdit>=JEDIT_INS ){
      JsonParse v;
      testcase( pParse->eEdit==JEDIT_INS );
      testcase( pParse->eEdit==JEDIT_SET );
      rc = jsonCreateEditSubstructure(pParse, &v, &zPath[i+1]);
      if( !JSON_LOOKUP_ISERROR(rc)
       && jsonBlobMakeEditable(pParse, v.nBlob)
      ){
        assert( !pParse->oom );
        jsonBlobEdit(pParse, j, 0, v.aBlob, v.nBlob);
      }
      jsonParseReset(&v);
      if( pParse->delta ) jsonAfterEditSizeAdjust(pParse, iRoot);
      return rc;
    }
  }else{
    return JSON_LOOKUP_PATHERROR; 
  }
  return JSON_LOOKUP_NOTFOUND;
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

  if( NEVER(aBlob==0) ) return;
  memset(&x, 0, sizeof(x));
  x.aBlob = (u8*)aBlob;
  x.nBlob = nBlob;
  jsonStringInit(&s, ctx);
  jsonTranslateBlobToText(&x, 0, &s);
  jsonReturnString(&s, 0, 0);
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
  if( n==0 ){
    sqlite3_result_error(pCtx, "malformed JSON", -1);
    return;
  }
  switch( pParse->aBlob[i] & 0x0f ){
    case JSONB_NULL: {
      if( sz ) goto returnfromblob_malformed;
      sqlite3_result_null(pCtx);
      break;
    }
    case JSONB_TRUE: {
      if( sz ) goto returnfromblob_malformed;
      sqlite3_result_int(pCtx, 1);
      break;
    }
    case JSONB_FALSE: {
      if( sz ) goto returnfromblob_malformed;
      sqlite3_result_int(pCtx, 0);
      break;
    }
    case JSONB_INT5:
    case JSONB_INT: {
      sqlite3_int64 iRes = 0;
      char *z;
      int bNeg = 0;
      char x;
      if( sz==0 ) goto returnfromblob_malformed;
      x = (char)pParse->aBlob[i+n];
      if( x=='-' ){
        if( sz<2 ) goto returnfromblob_malformed;
        n++;
        sz--;
        bNeg = 1;
      }
      z = sqlite3DbStrNDup(db, (const char*)&pParse->aBlob[i+n], (int)sz);
      if( z==0 ) goto returnfromblob_oom;
      rc = sqlite3DecOrHexToI64(z, &iRes);
      sqlite3DbFree(db, z);
      if( rc==0 ){
        sqlite3_result_int64(pCtx, bNeg ? -iRes : iRes);
      }else if( rc==3 && bNeg ){
        sqlite3_result_int64(pCtx, SMALLEST_INT64);
      }else if( rc==1 ){
        goto returnfromblob_malformed;
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
      if( sz==0 ) goto returnfromblob_malformed;
    to_double:
      z = sqlite3DbStrNDup(db, (const char*)&pParse->aBlob[i+n], (int)sz);
      if( z==0 ) goto returnfromblob_oom;
      rc = sqlite3AtoF(z, &r, sqlite3Strlen30(z), SQLITE_UTF8);
      sqlite3DbFree(db, z);
      if( rc<=0 ) goto returnfromblob_malformed;
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
      if( zOut==0 ) goto returnfromblob_oom;
      for(iIn=iOut=0; iIn<sz; iIn++){
        char c = z[iIn];
        if( c=='\\' ){
          u32 v;
          u32 szEscape = jsonUnescapeOneChar(&z[iIn], sz-iIn, &v);
          if( v<=0x7f ){
            zOut[iOut++] = (char)v;
          }else if( v<=0x7ff ){
            assert( szEscape>=2 );
            zOut[iOut++] = (char)(0xc0 | (v>>6));
            zOut[iOut++] = 0x80 | (v&0x3f);
          }else if( v<0x10000 ){
            assert( szEscape>=3 );
            zOut[iOut++] = 0xe0 | (v>>12);
            zOut[iOut++] = 0x80 | ((v>>6)&0x3f);
            zOut[iOut++] = 0x80 | (v&0x3f);
          }else if( v==JSON_INVALID_CHAR ){
            /* Silently ignore illegal unicode */
          }else{
            assert( szEscape>=4 );
            zOut[iOut++] = 0xf0 | (v>>18);
            zOut[iOut++] = 0x80 | ((v>>12)&0x3f);
            zOut[iOut++] = 0x80 | ((v>>6)&0x3f);
            zOut[iOut++] = 0x80 | (v&0x3f);
          }
          iIn += szEscape - 1;
        }else{
          zOut[iOut++] = c;
        }
      } /* end for() */
      assert( iOut<=nOut );
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
      goto returnfromblob_malformed;
    }
  }
  return;

returnfromblob_oom:
  sqlite3_result_error_nomem(pCtx);
  return;

returnfromblob_malformed:
  sqlite3_result_error(pCtx, "malformed JSON", -1);
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
    default: {
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
        jsonBlobAppendNode(pParse, JSONB_TEXTRAW, nJson, zJson);
      }
      break;
    }
    case SQLITE_FLOAT: {
      double r = sqlite3_value_double(pArg);
      if( NEVER(sqlite3IsNaN(r)) ){
        jsonBlobAppendNode(pParse, JSONB_NULL, 0, 0);
      }else{
        int n = sqlite3_value_bytes(pArg);
        const char *z = (const char*)sqlite3_value_text(pArg);
        if( z==0 ) return 1;
        if( z[0]=='I' ){
          jsonBlobAppendNode(pParse, JSONB_FLOAT, 5, "9e999");
        }else if( z[0]=='-' && z[1]=='I' ){
          jsonBlobAppendNode(pParse, JSONB_FLOAT, 6, "-9e999");
        }else{
          jsonBlobAppendNode(pParse, JSONB_FLOAT, n, z);
        }
      }
      break;
    }
    case SQLITE_INTEGER: {
      int n = sqlite3_value_bytes(pArg);
      const char *z = (const char*)sqlite3_value_text(pArg);
      if( z==0 ) return 1;
      jsonBlobAppendNode(pParse, JSONB_INT, n, z);
      break;
    }
  }
  if( pParse->oom ){
    sqlite3_result_error_nomem(ctx);
    return 1;
  }else{
    return 0;
  }
}

/*
** Generate a bad path error.
**
** If ctx is not NULL then push the error message into ctx and return NULL.
** If ctx is NULL, then return the text of the error message.
*/
static char *jsonBadPathError(
  sqlite3_context *ctx,     /* The function call containing the error */
  const char *zPath         /* The path with the problem */
){
  char *zMsg = sqlite3_mprintf("bad JSON path: %Q", zPath);
  if( ctx==0 ) return zMsg;
  if( zMsg ){
    sqlite3_result_error(ctx, zMsg, -1);
    sqlite3_free(zMsg);
  }else{
    sqlite3_result_error_nomem(ctx);
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
  JsonParse *p;
  JsonParse ax;

  assert( (argc&1)==1 );
  flgs = argc==1 ? 0 : JSON_EDITABLE;
  p = jsonParseFuncArg(ctx, argv[0], flgs);
  if( p==0 ) return;
  for(i=1; i<argc-1; i+=2){
    if( sqlite3_value_type(argv[i])==SQLITE_NULL ) continue;
    zPath = (const char*)sqlite3_value_text(argv[i]);
    if( zPath==0 ){
      sqlite3_result_error_nomem(ctx);
      jsonParseFree(p);
      return;
    }
    if( zPath[0]!='$' ) goto jsonInsertIntoBlob_patherror;
    if( jsonFunctionArgToBlob(ctx, argv[i+1], &ax) ){
      jsonParseReset(&ax);
      jsonParseFree(p);
      return;
    }
    if( zPath[1]==0 ){
      if( eEdit==JEDIT_REPL || eEdit==JEDIT_SET ){
        jsonBlobEdit(p, 0, p->nBlob, ax.aBlob, ax.nBlob);
      }
      rc = 0;
   }else{
      p->eEdit = eEdit;
      p->nIns = ax.nBlob;
      p->aIns = ax.aBlob;
      p->delta = 0;
      rc = jsonLookupStep(p, 0, zPath+1, 0);
    }
    jsonParseReset(&ax);
    if( rc==JSON_LOOKUP_NOTFOUND ) continue;
    if( JSON_LOOKUP_ISERROR(rc) ) goto jsonInsertIntoBlob_patherror;
  }
  jsonReturnParse(ctx, p);
  jsonParseFree(p);
  return;

jsonInsertIntoBlob_patherror:
  jsonParseFree(p);
  if( rc==JSON_LOOKUP_ERROR ){
    sqlite3_result_error(ctx, "malformed JSON", -1);
  }else{
    jsonBadPathError(ctx, zPath);
  }
  return;
}

/*
** Generate a JsonParse object, containing valid JSONB in aBlob and nBlob,
** from the SQL function argument pArg.  Return a pointer to the new
** JsonParse object.
**
** Ownership of the new JsonParse object is passed to the caller.  The
** caller should invoke jsonParseFree() on the return value when it
** has finished using it.
**
** If any errors are detected, an appropriate error messages is set
** using sqlite3_result_error() or the equivalent and this routine
** returns NULL.  This routine also returns NULL if the pArg argument
** is an SQL NULL value, but no error message is set in that case.  This
** is so that SQL functions that are given NULL arguments will return
** a NULL value.
*/
static JsonParse *jsonParseFuncArg(
  sqlite3_context *ctx,
  sqlite3_value *pArg,
  u32 flgs
){
  int eType;                   /* Datatype of pArg */
  JsonParse *p = 0;            /* Value to be returned */
  JsonParse *pFromCache = 0;   /* Value taken from cache */
  
  assert( ctx!=0 );
  eType = sqlite3_value_type(pArg);
  if( eType==SQLITE_NULL ){
    return 0;
  }
  pFromCache = jsonCacheSearch(ctx, pArg);
  if( pFromCache ){
    pFromCache->nJPRef++;
    if( (flgs & JSON_EDITABLE)==0 ){
      return pFromCache;
    }
  }
rebuild_from_cache:
  p = sqlite3_malloc64( sizeof(*p) );
  if( p==0 ) goto json_pfa_oom;
  memset(p, 0, sizeof(*p));
  p->nJPRef = 1;
  if( pFromCache!=0 ){
    u32 nBlob = pFromCache->nBlob;
    p->aBlob = sqlite3_malloc64( nBlob );
    if( p->aBlob==0 ) goto json_pfa_oom;
    memcpy(p->aBlob, pFromCache->aBlob, nBlob);
    p->nBlobAlloc = p->nBlob = nBlob;
    p->hasNonstd = pFromCache->hasNonstd;
    jsonParseFree(pFromCache);
    return p;
  }
  if( eType==SQLITE_BLOB ){
    u32 n, sz = 0;
    p->aBlob = (u8*)sqlite3_value_blob(pArg);
    p->nBlob = (u32)sqlite3_value_bytes(pArg);
    if( p->nBlob==0 ){
      goto json_pfa_malformed;
    }
    if( NEVER(p->aBlob==0) ){
      goto json_pfa_oom;
    }
    if( (p->aBlob[0] & 0x0f)>JSONB_OBJECT ){
      goto json_pfa_malformed;
    }
    n = jsonbPayloadSize(p, 0, &sz);
    if( n==0 
     || sz+n!=p->nBlob
     || ((p->aBlob[0] & 0x0f)<=JSONB_FALSE && sz>0)
    ){
      goto json_pfa_malformed;
    }
    if( (flgs & JSON_EDITABLE)!=0 && jsonBlobMakeEditable(p, 0)==0 ){
      goto json_pfa_oom;
    }
    return p;
  }
  p->zJson = (char*)sqlite3_value_text(pArg);
  p->nJson = sqlite3_value_bytes(pArg);
  if( p->nJson==0 ) goto json_pfa_malformed;
  if( NEVER(p->zJson==0) ) goto json_pfa_oom;
  if( jsonConvertTextToBlob(p, (flgs & JSON_KEEPERROR) ? 0 : ctx) ){
    if( flgs & JSON_KEEPERROR ){
      p->nErr = 1;
      return p;
    }else{
      jsonParseFree(p);
      return 0;
    }
  }else{
    int isRCStr = sqlite3ValueIsOfClass(pArg, sqlite3RCStrUnref);
    int rc;
    if( !isRCStr ){
      char *zNew = sqlite3RCStrNew( p->nJson );
      if( zNew==0 ) goto json_pfa_oom;
      memcpy(zNew, p->zJson, p->nJson);
      p->zJson = zNew;
      p->zJson[p->nJson] = 0;
    }else{
      sqlite3RCStrRef(p->zJson);
    }
    p->bJsonIsRCStr = 1;
    rc = jsonCacheInsert(ctx, p);
    if( rc==SQLITE_NOMEM ) goto json_pfa_oom;
    if( flgs & JSON_EDITABLE ){
      pFromCache = p;
      p = 0;
      goto rebuild_from_cache;
    }
  }
  return p;

json_pfa_malformed:
  if( flgs & JSON_KEEPERROR ){
    p->nErr = 1;
    return p;
  }else{
    jsonParseFree(p);
    sqlite3_result_error(ctx, "malformed JSON", -1);
    return 0;
  }

json_pfa_oom:
  jsonParseFree(pFromCache);
  jsonParseFree(p);
  sqlite3_result_error_nomem(ctx);
  return 0;
}

/*
** Make the return value of a JSON function either the raw JSONB blob
** or make it JSON text, depending on whether the JSON_BLOB flag is
** set on the function.
*/
static void jsonReturnParse(
  sqlite3_context *ctx,
  JsonParse *p
){
  int flgs;
  if( p->oom ){
    sqlite3_result_error_nomem(ctx);
    return;
  }
  flgs = SQLITE_PTR_TO_INT(sqlite3_user_data(ctx));
  if( flgs & JSON_BLOB ){
    if( p->nBlobAlloc>0 && !p->bReadOnly ){
      sqlite3_result_blob(ctx, p->aBlob, p->nBlob, SQLITE_DYNAMIC);
      p->nBlobAlloc = 0;
    }else{
      sqlite3_result_blob(ctx, p->aBlob, p->nBlob, SQLITE_TRANSIENT);
    }
  }else{
    JsonString s;
    jsonStringInit(&s, ctx);
    p->delta = 0;
    jsonTranslateBlobToText(p, 0, &s);
    jsonReturnString(&s, p, ctx);
    sqlite3_result_subtype(ctx, JSON_SUBTYPE);
  }
}

/****************************************************************************
** SQL functions used for testing and debugging
****************************************************************************/

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
    u32 savedNBlob = pParse->nBlob;
    printf("%5d:%*s", iStart, nIndent, "");
    if( pParse->nBlobAlloc>pParse->nBlob ){
      pParse->nBlob = pParse->nBlobAlloc;
    }
    nn = n = jsonbPayloadSize(pParse, iStart, &sz);
    if( nn==0 ) nn = 1;
    if( sz>0 && x<JSONB_ARRAY ){
      nn += sz;
    }
    for(i=0; i<nn; i++) printf(" %02x", pParse->aBlob[iStart+i]);
    if( n==0 ){
      printf("   ERROR invalid node size\n");
      iStart = n==0 ? iStart+1 : iEnd;
      continue;
    }
    pParse->nBlob = savedNBlob;
    if( iStart+n+sz>iEnd ){
      iEnd = iStart+n+sz;
      if( iEnd>pParse->nBlob ){
        if( pParse->nBlobAlloc>0 && iEnd>pParse->nBlobAlloc ){
          iEnd = pParse->nBlobAlloc;
        }else{
          iEnd = pParse->nBlob;
        }
      }
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
      if( sz==0 && x<=JSONB_FALSE ){
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
static void jsonShowParse(JsonParse *pParse){
  if( pParse==0 ){
    printf("NULL pointer\n");
    return;
  }else{
    printf("nBlobAlloc = %u\n", pParse->nBlobAlloc);
    printf("nBlob = %u\n", pParse->nBlob);
    printf("delta = %d\n", pParse->delta);
    if( pParse->nBlob==0 ) return;
    printf("content (bytes 0..%u):\n", pParse->nBlob-1);
  }
  jsonDebugPrintBlob(pParse, 0, pParse->nBlob, 0);
}
#endif /* SQLITE_DEBUG */

#ifdef SQLITE_DEBUG
/*
** SQL function:   json_parse(JSON)
**
** Parse JSON using jsonParseFuncArg().  Then print a dump of that
** parse on standard output.
*/
static void jsonParseFunc(
  sqlite3_context *ctx,
  int argc,
  sqlite3_value **argv
){
  JsonParse *p;        /* The parse */

  assert( argc==1 );
  p = jsonParseFuncArg(ctx, argv[0], 0);
  jsonShowParse(p);
  jsonParseFree(p);
}
#endif /* SQLITE_DEBUG */

/****************************************************************************
** Scalar SQL function implementations
****************************************************************************/

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
  jsonReturnString(&jx, 0, 0);
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
  jsonReturnString(&jx, 0, 0);
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
  sqlite3_int64 cnt = 0;
  u32 i;
  u8 eErr = 0;

  p = jsonParseFuncArg(ctx, argv[0], 0);
  if( p==0 ) return;
  if( argc==2 ){
    const char *zPath = (const char*)sqlite3_value_text(argv[1]);
    if( zPath==0 ){
      jsonParseFree(p);
      return;
    }
    i = jsonLookupStep(p, 0, zPath[0]=='$' ? zPath+1 : "@", 0);
    if( JSON_LOOKUP_ISERROR(i) ){
      if( i==JSON_LOOKUP_NOTFOUND ){
        /* no-op */
      }else if( i==JSON_LOOKUP_PATHERROR ){
        jsonBadPathError(ctx, zPath);
      }else{
        sqlite3_result_error(ctx, "malformed JSON", -1);
      }
      eErr = 1;
      i = 0;
    }
  }else{
    i = 0;
  }
  if( (p->aBlob[i] & 0x0f)==JSONB_ARRAY ){
    cnt = jsonbArrayCount(p, i);
  }
  if( !eErr ) sqlite3_result_int64(ctx, cnt);
  jsonParseFree(p);
}

/* True if the string is all digits */
static int jsonAllDigits(const char *z, int n){
  int i;
  for(i=0; i<n && sqlite3Isdigit(z[i]); i++){}
  return i==n;
}

/* True if the string is all alphanumerics and underscores */
static int jsonAllAlphanum(const char *z, int n){
  int i;
  for(i=0; i<n && (sqlite3Isalnum(z[i]) || z[i]=='_'); i++){}
  return i==n;
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
  JsonParse *p = 0;      /* The parse */
  int flags;             /* Flags associated with the function */
  int i;                 /* Loop counter */
  JsonString jx;         /* String for array result */

  if( argc<2 ) return;
  p = jsonParseFuncArg(ctx, argv[0], 0);
  if( p==0 ) return;
  flags = SQLITE_PTR_TO_INT(sqlite3_user_data(ctx));
  jsonStringInit(&jx, ctx);
  if( argc>2 ){
    jsonAppendChar(&jx, '[');
  }
  for(i=1; i<argc; i++){
    /* With a single PATH argument */
    const char *zPath = (const char*)sqlite3_value_text(argv[i]);
    int nPath;
    u32 j;
    if( zPath==0 ) goto json_extract_error;
    nPath = sqlite3Strlen30(zPath);
    if( zPath[0]=='$' ){
      j = jsonLookupStep(p, 0, zPath+1, 0);
    }else if( (flags & JSON_ABPATH) ){
      /* The -> and ->> operators accept abbreviated PATH arguments.  This
      ** is mostly for compatibility with PostgreSQL, but also for
      ** convenience.
      **
      **     NUMBER   ==>  $[NUMBER]     // PG compatible
      **     LABEL    ==>  $.LABEL       // PG compatible
      **     [NUMBER] ==>  $[NUMBER]     // Not PG.  Purely for convenience
      */
      jsonStringInit(&jx, ctx);
      if( jsonAllDigits(zPath, nPath) ){
        jsonAppendRawNZ(&jx, "[", 1);
        jsonAppendRaw(&jx, zPath, nPath);
        jsonAppendRawNZ(&jx, "]", 2);
      }else if( jsonAllAlphanum(zPath, nPath) ){
        jsonAppendRawNZ(&jx, ".", 1);
        jsonAppendRaw(&jx, zPath, nPath);
      }else if( zPath[0]=='[' && nPath>=3 && zPath[nPath-1]==']' ){
        jsonAppendRaw(&jx, zPath, nPath);
      }else{
        jsonAppendRawNZ(&jx, ".\"", 2);
        jsonAppendRaw(&jx, zPath, nPath);
        jsonAppendRawNZ(&jx, "\"", 1);
      }
      jsonStringTerminate(&jx);
      j = jsonLookupStep(p, 0, jx.zBuf, 0);
      jsonStringReset(&jx);
    }else{
      jsonBadPathError(ctx, zPath);
      goto json_extract_error;
    }
    if( j<p->nBlob ){
      if( argc==2 ){
        if( flags & JSON_JSON ){
          jsonStringInit(&jx, ctx);
          jsonTranslateBlobToText(p, j, &jx);
          jsonReturnString(&jx, 0, 0);
          jsonStringReset(&jx);
          assert( (flags & JSON_BLOB)==0 );
          sqlite3_result_subtype(ctx, JSON_SUBTYPE);
        }else{
          jsonReturnFromBlob(p, j, ctx, 0);
          if( (flags & (JSON_SQL|JSON_BLOB))==0
           && (p->aBlob[j]&0x0f)>=JSONB_ARRAY
          ){
            sqlite3_result_subtype(ctx, JSON_SUBTYPE);
          }
        }
      }else{
        jsonAppendSeparator(&jx);
        jsonTranslateBlobToText(p, j, &jx);
      }
    }else if( j==JSON_LOOKUP_NOTFOUND ){
      if( argc==2 ){
        goto json_extract_error;  /* Return NULL if not found */
      }else{
        jsonAppendSeparator(&jx);
        jsonAppendRawNZ(&jx, "null", 4);
      }
    }else if( j==JSON_LOOKUP_ERROR ){
      sqlite3_result_error(ctx, "malformed JSON", -1);
      goto json_extract_error;
    }else{
      jsonBadPathError(ctx, zPath);
      goto json_extract_error;
    }
  }
  if( argc>2 ){
    jsonAppendChar(&jx, ']');
    jsonReturnString(&jx, 0, 0);
    if( (flags & JSON_BLOB)==0 ){
      sqlite3_result_subtype(ctx, JSON_SUBTYPE);
    }
  }
json_extract_error:
  jsonStringReset(&jx);
  jsonParseFree(p);
  return;
}

/*
** Return codes for jsonMergePatch()
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
** Here is an equivalent algorithm restructured to show the actual
** implementation:
**
** 01   define MergePatch(Target, Patch):
** 02      if Patch is not an Object:
** 03         return Patch
** 04      else: // if Patch is an Object
** 05         if Target is not an Object:
** 06            Target = {}
** 07      for each Name/Value pair in Patch:
** 08         if Name exists in Target:
** 09            if Value is null:
** 10               remove the Name/Value pair from Target
** 11            else
** 12               Target[name] = MergePatch(Target[Name], Value)
** 13         else if Value is not NULL:
** 14            if Value is not an Object:
** 15               Target[name] = Value
** 16            else:
** 17               Target[name] = MergePatch('{}',value)
** 18      return Target
**  |
**  ^---- Line numbers referenced in comments in the implementation
*/
static int jsonMergePatch(
  JsonParse *pTarget,      /* The JSON parser that contains the TARGET */
  u32 iTarget,             /* Index of TARGET in pTarget->aBlob[] */
  const JsonParse *pPatch, /* The PATCH */
  u32 iPatch               /* Index of PATCH in pPatch->aBlob[] */
){
  u8 x;             /* Type of a single node */
  u32 n, sz=0;      /* Return values from jsonbPayloadSize() */
  u32 iTCursor;     /* Cursor position while scanning the target object */
  u32 iTStart;      /* First label in the target object */
  u32 iTEndBE;      /* Original first byte past end of target, before edit */
  u32 iTEnd;        /* Current first byte past end of target */
  u8 eTLabel;       /* Node type of the target label */
  u32 iTLabel = 0;  /* Index of the label */
  u32 nTLabel = 0;  /* Header size in bytes for the target label */
  u32 szTLabel = 0; /* Size of the target label payload */
  u32 iTValue = 0;  /* Index of the target value */
  u32 nTValue = 0;  /* Header size of the target value */
  u32 szTValue = 0; /* Payload size for the target value */

  u32 iPCursor;     /* Cursor position while scanning the patch */
  u32 iPEnd;        /* First byte past the end of the patch */
  u8 ePLabel;       /* Node type of the patch label */
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
    n = jsonbPayloadSize(pTarget, iTarget, &sz);
    jsonBlobEdit(pTarget, iTarget+n, sz, 0, 0);
    x = pTarget->aBlob[iTarget];
    pTarget->aBlob[iTarget] = (x & 0xf0) | JSONB_OBJECT;
  }
  n = jsonbPayloadSize(pPatch, iPatch, &sz);
  if( NEVER(n==0) ) return JSON_MERGE_BADPATCH;
  iPCursor = iPatch+n;
  iPEnd = iPCursor+sz;
  n = jsonbPayloadSize(pTarget, iTarget, &sz);
  if( NEVER(n==0) ) return JSON_MERGE_BADTARGET;
  iTStart = iTarget+n;
  iTEndBE = iTStart+sz;

  while( iPCursor<iPEnd ){  /* Algorithm line 07 */
    iPLabel = iPCursor;
    ePLabel = pPatch->aBlob[iPCursor] & 0x0f;
    if( ePLabel<JSONB_TEXT || ePLabel>JSONB_TEXTRAW ){
      return JSON_MERGE_BADPATCH;
    }
    nPLabel = jsonbPayloadSize(pPatch, iPCursor, &szPLabel);
    if( nPLabel==0 ) return JSON_MERGE_BADPATCH;
    iPValue = iPCursor + nPLabel + szPLabel;
    if( iPValue>=iPEnd ) return JSON_MERGE_BADPATCH;
    nPValue = jsonbPayloadSize(pPatch, iPValue, &szPValue);
    if( nPValue==0 ) return JSON_MERGE_BADPATCH;
    iPCursor = iPValue + nPValue + szPValue;
    if( iPCursor>iPEnd ) return JSON_MERGE_BADPATCH;

    iTCursor = iTStart;
    iTEnd = iTEndBE + pTarget->delta;
    while( iTCursor<iTEnd ){
      int isEqual;   /* true if the patch and target labels match */
      iTLabel = iTCursor;
      eTLabel = pTarget->aBlob[iTCursor] & 0x0f;
      if( eTLabel<JSONB_TEXT || eTLabel>JSONB_TEXTRAW ){
        return JSON_MERGE_BADTARGET;
      }
      nTLabel = jsonbPayloadSize(pTarget, iTCursor, &szTLabel);
      if( nTLabel==0 ) return JSON_MERGE_BADTARGET;
      iTValue = iTLabel + nTLabel + szTLabel;
      if( iTValue>=iTEnd ) return JSON_MERGE_BADTARGET;
      nTValue = jsonbPayloadSize(pTarget, iTValue, &szTValue);
      if( nTValue==0 ) return JSON_MERGE_BADTARGET;
      if( iTValue + nTValue + szTValue > iTEnd ) return JSON_MERGE_BADTARGET;
      isEqual = jsonLabelCompare(
                   (const char*)&pPatch->aBlob[iPLabel+nPLabel],
                   szPLabel,
                   (ePLabel==JSONB_TEXT || ePLabel==JSONB_TEXTRAW),
                   (const char*)&pTarget->aBlob[iTLabel+nTLabel],
                   szTLabel,
                   (eTLabel==JSONB_TEXT || eTLabel==JSONB_TEXTRAW));
      if( isEqual ) break;
      iTCursor = iTValue + nTValue + szTValue;
    }
    x = pPatch->aBlob[iPValue] & 0x0f;
    if( iTCursor<iTEnd ){
      /* A match was found.  Algorithm line 08 */
      if( x==0 ){
        /* Patch value is NULL.  Algorithm line 09 */
        jsonBlobEdit(pTarget, iTLabel, nTLabel+szTLabel+nTValue+szTValue, 0,0);
        /*  vvvvvv----- No OOM on a delete-only edit */
        if( NEVER(pTarget->oom) ) return JSON_MERGE_OOM;
      }else{
        /* Algorithm line 12 */
        int rc, savedDelta = pTarget->delta;
        pTarget->delta = 0;
        rc = jsonMergePatch(pTarget, iTValue, pPatch, iPValue);
        if( rc ) return rc;
        pTarget->delta += savedDelta;
      }        
    }else if( x>0 ){  /* Algorithm line 13 */
      /* No match and patch value is not NULL */
      u32 szNew = szPLabel+nPLabel;
      if( (pPatch->aBlob[iPValue] & 0x0f)!=JSONB_OBJECT ){  /* Line 14 */
        jsonBlobEdit(pTarget, iTEnd, 0, 0, szPValue+nPValue+szNew);
        if( pTarget->oom ) return JSON_MERGE_OOM;
        memcpy(&pTarget->aBlob[iTEnd], &pPatch->aBlob[iPLabel], szNew);
        memcpy(&pTarget->aBlob[iTEnd+szNew], 
               &pPatch->aBlob[iPValue], szPValue+nPValue);
      }else{
        int rc, savedDelta;
        jsonBlobEdit(pTarget, iTEnd, 0, 0, szNew+1);
        if( pTarget->oom ) return JSON_MERGE_OOM;
        memcpy(&pTarget->aBlob[iTEnd], &pPatch->aBlob[iPLabel], szNew);
        pTarget->aBlob[iTEnd+szNew] = 0x00;
        savedDelta = pTarget->delta;
        pTarget->delta = 0;
        rc = jsonMergePatch(pTarget, iTEnd+szNew,pPatch,iPValue);
        if( rc ) return rc;
        pTarget->delta += savedDelta;
      }
    }
  }
  if( pTarget->delta ) jsonAfterEditSizeAdjust(pTarget, iTarget);
  return pTarget->oom ? JSON_MERGE_OOM : JSON_MERGE_OK;
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
  JsonParse *pTarget;    /* The TARGET */
  JsonParse *pPatch;     /* The PATCH */
  int rc;                /* Result code */

  UNUSED_PARAMETER(argc);
  assert( argc==2 );
  pTarget = jsonParseFuncArg(ctx, argv[0], JSON_EDITABLE);
  if( pTarget==0 ) return;
  pPatch = jsonParseFuncArg(ctx, argv[1], 0);
  if( pPatch ){
    rc = jsonMergePatch(pTarget, 0, pPatch, 0);
    if( rc==JSON_MERGE_OK ){
      jsonReturnParse(ctx, pTarget);
    }else if( rc==JSON_MERGE_OOM ){
      sqlite3_result_error_nomem(ctx);
    }else{
      sqlite3_result_error(ctx, "malformed JSON", -1);
    }
    jsonParseFree(pPatch);
  }
  jsonParseFree(pTarget);
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
    n = sqlite3_value_bytes(argv[i]);
    jsonAppendString(&jx, z, n);
    jsonAppendChar(&jx, ':');
    jsonAppendSqlValue(&jx, argv[i+1]);
  }
  jsonAppendChar(&jx, '}');
  jsonReturnString(&jx, 0, 0);
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
  JsonParse *p;          /* The parse */
  const char *zPath = 0; /* Path of element to be removed */
  int i;                 /* Loop counter */
  u32 rc;                /* Subroutine return code */

  if( argc<1 ) return;
  p = jsonParseFuncArg(ctx, argv[0], argc>1 ? JSON_EDITABLE : 0);
  if( p==0 ) return;
  for(i=1; i<argc; i++){
    zPath = (const char*)sqlite3_value_text(argv[i]);
    if( zPath==0 ){
      goto json_remove_done;
    }
    if( zPath[0]!='$' ){
      goto json_remove_patherror;
    }
    if( zPath[1]==0 ){
      /* json_remove(j,'$') returns NULL */
      goto json_remove_done;
    }
    p->eEdit = JEDIT_DEL;
    p->delta = 0;
    rc = jsonLookupStep(p, 0, zPath+1, 0);
    if( JSON_LOOKUP_ISERROR(rc) ){
      if( rc==JSON_LOOKUP_NOTFOUND ){
        continue;  /* No-op */
      }else if( rc==JSON_LOOKUP_PATHERROR ){
        jsonBadPathError(ctx, zPath);
      }else{
        sqlite3_result_error(ctx, "malformed JSON", -1);
      }
      goto json_remove_done;
    }
  }
  jsonReturnParse(ctx, p);
  jsonParseFree(p);
  return;

json_remove_patherror:
  jsonBadPathError(ctx, zPath);

json_remove_done:
  jsonParseFree(p);
  return;
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
  if( argc<1 ) return;
  if( (argc&1)==0 ) {
    jsonWrongNumArgs(ctx, "replace");
    return;
  }
  jsonInsertIntoBlob(ctx, argc, argv, JEDIT_REPL);
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

  int flags = SQLITE_PTR_TO_INT(sqlite3_user_data(ctx));
  int bIsSet = (flags&JSON_ISSET)!=0;

  if( argc<1 ) return;
  if( (argc&1)==0 ) {
    jsonWrongNumArgs(ctx, bIsSet ? "set" : "insert");
    return;
  }
  jsonInsertIntoBlob(ctx, argc, argv, bIsSet ? JEDIT_SET : JEDIT_INS);
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
  const char *zPath = 0;
  u32 i;

  p = jsonParseFuncArg(ctx, argv[0], 0);
  if( p==0 ) return;
  if( argc==2 ){
    zPath = (const char*)sqlite3_value_text(argv[1]);
    if( zPath==0 ) goto json_type_done;
    if( zPath[0]!='$' ){
      jsonBadPathError(ctx, zPath);
      goto json_type_done;
    }
    i = jsonLookupStep(p, 0, zPath+1, 0);
    if( JSON_LOOKUP_ISERROR(i) ){
      if( i==JSON_LOOKUP_NOTFOUND ){
        /* no-op */
      }else if( i==JSON_LOOKUP_PATHERROR ){
        jsonBadPathError(ctx, zPath);
      }else{
        sqlite3_result_error(ctx, "malformed JSON", -1);
      }
      goto json_type_done;
    }
  }else{
    i = 0;
  }
  sqlite3_result_text(ctx, jsonbType[p->aBlob[i]&0x0f], -1, SQLITE_STATIC);
json_type_done:
  jsonParseFree(p);
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
        if( flags & 0x04 ){
          /* Superficial checking only - accomplished by the
          ** jsonFuncArgMightBeBinary() call above. */
          res = 1;
        }else{
          /* Strict checking.  Check by translating BLOB->TEXT->BLOB.  If
          ** no errors occur, call that a "strict check". */
          JsonParse px;
          u32 iErr;
          memset(&px, 0, sizeof(px));
          px.aBlob = (u8*)sqlite3_value_blob(argv[0]);
          px.nBlob = sqlite3_value_bytes(argv[0]);
          iErr = jsonbValidityCheck(&px, 0, px.nBlob, 1);
          res = iErr==0;
        }
      }
      break;
    }
    default: {
      JsonParse px;
      if( (flags & 0x3)==0 ) break;
      memset(&px, 0, sizeof(px));
     
      p = jsonParseFuncArg(ctx, argv[0], JSON_KEEPERROR);
      if( p ){
        if( p->oom ){
          sqlite3_result_error_nomem(ctx);
        }else if( p->nErr ){
          /* no-op */
        }else if( (flags & 0x02)!=0 || p->hasNonstd==0 ){
          res = 1;
        }
        jsonParseFree(p);
      }else{
        sqlite3_result_error_nomem(ctx);
      }
      break;
    }
  }
  sqlite3_result_int(ctx, res);
}

/*
** json_error_position(JSON)
**
** If the argument is NULL, return NULL
**
** If the argument is BLOB, do a full validity check and return non-zero
** if the check fails.  The return value is the approximate 1-based offset
** to the byte of the element that contains the first error.
**
** Otherwise interpret the argument is TEXT (even if it is numeric) and
** return the 1-based character position for where the parser first recognized
** that the input was not valid JSON, or return 0 if the input text looks
** ok.  JSON-5 extensions are accepted.
*/
static void jsonErrorFunc(
  sqlite3_context *ctx,
  int argc,
  sqlite3_value **argv
){
  i64 iErrPos = 0;       /* Error position to be returned */
  JsonParse s;

  assert( argc==1 );
  UNUSED_PARAMETER(argc);
  memset(&s, 0, sizeof(s));
  if( jsonFuncArgMightBeBinary(argv[0]) ){
    s.aBlob = (u8*)sqlite3_value_blob(argv[0]);
    s.nBlob = sqlite3_value_bytes(argv[0]);
    iErrPos = (i64)jsonbValidityCheck(&s, 0, s.nBlob, 1);
  }else{
    s.zJson = (char*)sqlite3_value_text(argv[0]);
    if( s.zJson==0 ) return;  /* NULL input or OOM */
    s.nJson = sqlite3_value_bytes(argv[0]);
    if( jsonConvertTextToBlob(&s,0) ){
      if( s.oom ){
        iErrPos = -1;
      }else{
        /* Convert byte-offset s.iErr into a character offset */
        u32 k;
        assert( s.zJson!=0 );  /* Because s.oom is false */
        for(k=0; k<s.iErr && ALWAYS(s.zJson[k]); k++){
          if( (s.zJson[k] & 0xc0)!=0x80 ) iErrPos++;
        }
        iErrPos++;
      }
    }
  }
  jsonParseReset(&s);
  if( iErrPos<0 ){
    sqlite3_result_error_nomem(ctx);
  }else{
    sqlite3_result_int64(ctx, iErrPos);
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
      jsonReturnString(pStr, 0, 0);
      return;
    }else if( flags & JSON_BLOB ){
      jsonReturnStringAsBlob(pStr);
      if( isFinal ){
        if( !pStr->bStatic ) sqlite3RCStrUnref(pStr->zBuf);
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
    n = sqlite3Strlen30(z);
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
      jsonReturnString(pStr, 0, 0);
      return;
    }else if( flags & JSON_BLOB ){
      jsonReturnStringAsBlob(pStr);
      if( isFinal ){
        if( !pStr->bStatic ) sqlite3RCStrUnref(pStr->zBuf);
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
        u32 iVal = pParent->iValue;
        p->eType = p->sParse.aBlob[iVal] & 0x0f;
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
  char *z = p->path.zBuf;
  if( p->iRowid==0 && p->bRecursive && n>=2 ){
    while( n>1 ){
      n--;
      if( z[n]=='[' || z[n]=='.' ){
        u32 x, sz = 0;
        char cSaved = z[n];
        z[n] = 0;
        assert( p->sParse.eEdit==0 );
        x = jsonLookupStep(&p->sParse, 0, z+1, 0);
        z[n] = cSaved;
        if( JSON_LOOKUP_ISERROR(x) ) continue;
        if( x + jsonbPayloadSize(&p->sParse, x, &sz) == p->i ) break;
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
      u8 eType = p->sParse.aBlob[i] & 0x0f;
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
      if( p->sParse.zJson==0 ){
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
  if( sqlite3_value_type(argv[0])==SQLITE_BLOB ){
    if( jsonFuncArgMightBeBinary(argv[0]) ){
      p->sParse.nBlob = sqlite3_value_bytes(argv[0]);
      p->sParse.aBlob = (u8*)sqlite3_value_blob(argv[0]);
    }else{
      goto json_each_malformed_input;
    }
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
      goto json_each_malformed_input;
    }
  }
  if( idxNum==3 ){
    zRoot = (const char*)sqlite3_value_text(argv[1]);
    if( zRoot==0 ) return SQLITE_OK;
    if( zRoot[0]!='$' ){
      sqlite3_free(cur->pVtab->zErrMsg);
      cur->pVtab->zErrMsg = jsonBadPathError(0, zRoot);
      jsonEachCursorReset(p);
      return cur->pVtab->zErrMsg ? SQLITE_ERROR : SQLITE_NOMEM;
    }
    p->nRoot = sqlite3Strlen30(zRoot);
    if( zRoot[1]==0 ){
      i = p->i = 0;
      p->eType = 0;
    }else{
      i = jsonLookupStep(&p->sParse, 0, zRoot+1, 0);
      if( JSON_LOOKUP_ISERROR(i) ){
        if( i==JSON_LOOKUP_NOTFOUND ){
          p->i = 0;
          p->eType = 0;
          p->iEnd = 0;
          return SQLITE_OK;
        }
        sqlite3_free(cur->pVtab->zErrMsg);
        cur->pVtab->zErrMsg = jsonBadPathError(0, zRoot);
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

json_each_malformed_input:
  sqlite3_free(cur->pVtab->zErrMsg);
  cur->pVtab->zErrMsg = sqlite3_mprintf("malformed JSON");
  jsonEachCursorReset(p);
  return cur->pVtab->zErrMsg ? SQLITE_ERROR : SQLITE_NOMEM;
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
    JFUNCTION(jsonb,              1,1,0, 0,1,0,          jsonRemoveFunc),
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

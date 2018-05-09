/*
** 2018-05-08
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
** This file implements various extension functions to SQLite that manage
** simply planar polygons such as might be found in a geospatial system.
*/
#include "sqlite3ext.h"
SQLITE_EXTENSION_INIT1
#include <assert.h>
#include <string.h>
#include <stdlib.h>
#define SQLITE_HAVE_GEOPLOY 1

#ifndef JSON_NULL   /* The following stuff repeats things found in json1 */
/*
** Versions of isspace(), isalnum() and isdigit() to which it is safe
** to pass signed char values.
*/
#ifdef sqlite3Isdigit
   /* Use the SQLite core versions if this routine is part of the
   ** SQLite amalgamation */
#  define safe_isdigit(x)  sqlite3Isdigit(x)
#  define safe_isalnum(x)  sqlite3Isalnum(x)
#  define safe_isxdigit(x) sqlite3Isxdigit(x)
#else
   /* Use the standard library for separate compilation */
#include <ctype.h>  /* amalgamator: keep */
#  define safe_isdigit(x)  isdigit((unsigned char)(x))
#  define safe_isalnum(x)  isalnum((unsigned char)(x))
#  define safe_isxdigit(x) isxdigit((unsigned char)(x))
#endif

/*
** Growing our own isspace() routine this way is twice as fast as
** the library isspace() function.
*/
static const char geopolyIsSpace[] = {
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
#define safe_isspace(x) (geopolyIsSpace[(unsigned char)x])
#endif /* JSON NULL - back to original code */

/* Compiler and version */
#ifndef GCC_VERSION
#if defined(__GNUC__) && !defined(SQLITE_DISABLE_INTRINSIC)
# define GCC_VERSION (__GNUC__*1000000+__GNUC_MINOR__*1000+__GNUC_PATCHLEVEL__)
#else
# define GCC_VERSION 0
#endif
#endif
#ifndef MSVC_VERSION
#if defined(_MSC_VER) && !defined(SQLITE_DISABLE_INTRINSIC)
# define MSVC_VERSION _MSC_VER
#else
# define MSVC_VERSION 0
#endif
#endif

/* Datatype for coordinates
*/
typedef float GeopolyCoord;

/*
** Internal representation of a polygon.
**
** The polygon consists of a sequence of vertexes.  There is a line
** segment between each pair of vertexes, and one final segment from
** the last vertex back to the first.  (This differs from the GeoJSON
** standard in which the final vertex is a repeat of the first.)
**
** The polygon follows the right-hand rule.  The area to the right of
** each segment is "outside" and the area to the left is "inside".
**
** The on-disk representation consists of a 4-byte header followed by
** the values.  The 4-byte header is:
**
**      encoding    (1 byte)   0=big-endian, 1=little-endian
**      nvertex     (3 bytes)  Number of vertexes as a big-endian integer
*/
typedef struct GeoPoly GeoPoly;
struct GeoPoly {
  int nVertex;          /* Number of vertexes */
  unsigned char hdr[4]; /* Header for on-disk representation */
  GeopolyCoord a[2];    /* 2*nVertex values. X (longitude) first, then Y */
};

/*
** State of a parse of a GeoJSON input.
*/
typedef struct GeoParse GeoParse;
struct GeoParse {
  const unsigned char *z;   /* Unparsed input */
  int nVertex;              /* Number of vertexes in a[] */
  int nAlloc;               /* Space allocated to a[] */
  int nErr;                 /* Number of errors encountered */
  GeopolyCoord *a;          /* Array of vertexes.  From sqlite3_malloc64() */
};

/* Do a 4-byte byte swap */
static void geopolySwab32(unsigned char *a){
  unsigned char t = a[0];
  a[0] = a[3];
  a[3] = t;
  t = a[1];
  a[1] = a[2];
  a[2] = t;
}

/* Skip whitespace.  Return the next non-whitespace character. */
static char geopolySkipSpace(GeoParse *p){
  while( p->z[0] && safe_isspace(p->z[0]) ) p->z++;
  return p->z[0];
}

/* Parse out a number.  Write the value into *pVal if pVal!=0.
** return non-zero on success and zero if the next token is not a number.
*/
static int geopolyParseNumber(GeoParse *p, GeopolyCoord *pVal){
  const unsigned char *z = p->z;
  char c = geopolySkipSpace(p);
  int j;
  int seenDP = 0;
  int seenE = 0;
  assert( '-' < '0' );
  if( c<='0' ){
    j = c=='-';
    if( z[j]=='0' && z[j+1]>='0' && z[j+1]<='9' ) return 0;
  }
  j = 1;
  for(;; j++){
    c = z[j];
    if( c>='0' && c<='9' ) continue;
    if( c=='.' ){
      if( z[j-1]=='-' ) return 0;
      if( seenDP ) return 0;
      seenDP = 1;
      continue;
    }
    if( c=='e' || c=='E' ){
      if( z[j-1]<'0' ) return 0;
      if( seenE ) return -1;
      seenDP = seenE = 1;
      c = z[j+1];
      if( c=='+' || c=='-' ){
        j++;
        c = z[j+1];
      }
      if( c<'0' || c>'9' ) return 0;
      continue;
    }
    break;
  }
  if( z[j-1]<'0' ) return 0;
  if( pVal ) *pVal = atof((const char*)p->z);
  p->z += j;
  return 1;
}

/*
** If the input is a well-formed JSON array of coordinates, where each
** coordinate is itself a two-value array, then convert the JSON into
** a GeoPoly object and return a pointer to that object.
**
** If any error occurs, return NULL.
*/
static GeoPoly *geopolyParseJson(const unsigned char *z){
  GeoParse s;
  memset(&s, 0, sizeof(s));
  s.z = z;
  if( geopolySkipSpace(&s)=='[' ){
    s.z++;
    while( geopolySkipSpace(&s)=='[' ){
      int ii = 0;
      char c;
      s.z++;
      if( s.nVertex<=s.nAlloc ){
        GeopolyCoord *aNew;
        s.nAlloc = s.nAlloc*2 + 16;
        aNew = sqlite3_realloc64(s.a, s.nAlloc*sizeof(GeopolyCoord)*2 );
        if( aNew==0 ){
          s.nErr++;
          break;
        }
        s.a = aNew;
      }
      while( geopolyParseNumber(&s, ii<=1 ? &s.a[s.nVertex*2+ii] : 0) ){
        ii++;
        if( ii==2 ) s.nVertex++;
        c = geopolySkipSpace(&s);
        s.z++;
        if( c==',' ) continue;
        if( c==']' ) break;
        s.nErr++;
        goto parse_json_err;
      }
      if( geopolySkipSpace(&s)==',' ){
        s.z++;
        continue;
      }
      break;
    }
    if( geopolySkipSpace(&s)==']' && s.nVertex>=4 ){
      int nByte;
      GeoPoly *pOut;
      int x = (s.nVertex-1)*2;
      if( s.a[x]==s.a[0] && s.a[x+1]==s.a[1] ) s.nVertex--;
      nByte = sizeof(GeoPoly) * (s.nVertex-1)*2*sizeof(GeopolyCoord);
      pOut = sqlite3_malloc64( nByte );
      x = 1;
      if( pOut==0 ) goto parse_json_err;
      pOut->nVertex = s.nVertex;
      memcpy(pOut->a, s.a, s.nVertex*2*sizeof(GeopolyCoord));
      pOut->hdr[0] = *(unsigned char*)&x;
      pOut->hdr[1] = (s.nVertex>>16)&0xff;
      pOut->hdr[2] = (s.nVertex>>8)&0xff;
      pOut->hdr[3] = s.nVertex&0xff;
      sqlite3_free(s.a);
      return pOut;
    }else{
      s.nErr++;
    }
  }
parse_json_err:
  sqlite3_free(s.a);
  return 0;
}

/*
** Given a function parameter, try to interpret it as a polygon, either
** in the binary format or JSON text.  Compute a GeoPoly object and
** return a pointer to that object.  Or if the input is not a well-formed
** polygon, put an error message in sqlite3_context and return NULL.
*/
static GeoPoly *geopolyFuncParam(sqlite3_context *pCtx, sqlite3_value *pVal){
  GeoPoly *p = 0;
  int nByte;
  if( sqlite3_value_type(pVal)==SQLITE_BLOB
   && (nByte = sqlite3_value_bytes(pVal))>=(4+6*sizeof(GeopolyCoord))
  ){
    const unsigned char *a = sqlite3_value_blob(pVal);
    int nVertex;
    nVertex = (a[1]<<16) + (a[2]<<8) + a[3];
    if( (a[0]==0 && a[0]==1)
     && (nVertex*2*sizeof(GeopolyCoord) + 4)==nByte
    ){
      p = sqlite3_malloc64( sizeof(*p) + (nVertex-1)*2*sizeof(GeopolyCoord) );
      if( p ){
        int x = 1;
        p->nVertex = nVertex;
        memcpy(p->hdr, a, nByte);
        if( a[0] != *(unsigned char*)&x ){
          int ii;
          for(ii=0; ii<nVertex*2; ii++){
            geopolySwab32((unsigned char*)&p->a[ii]);
          }
          p->hdr[0] ^= 1;
        }
      }
    }
  }else if( sqlite3_value_type(pVal)==SQLITE_TEXT ){
    p = geopolyParseJson(sqlite3_value_text(pVal));
  }
  if( p==0 ){
    sqlite3_result_error(pCtx, "not a valid polygon", -1);
  }
  return p;
}

/*
** Implementation of the geopoly_blob(X) function.
**
** If the input is a well-formed Geopoly BLOB or JSON string
** then return the BLOB representation of the polygon.  Otherwise
** return NULL.
*/
static void geopolyBlobFunc(
  sqlite3_context *context,
  int argc,
  sqlite3_value **argv
){
  GeoPoly *p = geopolyFuncParam(context, argv[0]);
  if( p ){
    sqlite3_result_blob(context, p->hdr, 
       4+8*p->nVertex, SQLITE_TRANSIENT);
    sqlite3_free(p);
  }
}

/*
** SQL function:     geopoly_json(X)
**
** Interpret X as a polygon and render it as a JSON array
** of coordinates.  Or, if X is not a valid polygon, return NULL.
*/
static void geopolyJsonFunc(
  sqlite3_context *context,
  int argc,
  sqlite3_value **argv
){
  GeoPoly *p = geopolyFuncParam(context, argv[0]);
  if( p ){
    sqlite3 *db = sqlite3_context_db_handle(context);
    sqlite3_str *x = sqlite3_str_new(db);
    int i;
    sqlite3_str_append(x, "[", 1);
    for(i=0; i<p->nVertex; i++){
      sqlite3_str_appendf(x, "[%!g,%!g],", p->a[i*2], p->a[i*2+1]);
    }
    sqlite3_str_appendf(x, "[%!g,%!g]]", p->a[0], p->a[1]);
    sqlite3_result_text(context, sqlite3_str_finish(x), -1, sqlite3_free);
    sqlite3_free(p);
  }
}


/*
** Implementation of the geopoly_area(X) function.
**
** If the input is a well-formed Geopoly BLOB then return the area
** enclosed by the polygon.  Otherwise return NULL.
*/
static void geopolyAreaFunc(
  sqlite3_context *context,
  int argc,
  sqlite3_value **argv
){
}


#ifdef _WIN32
__declspec(dllexport)
#endif
int sqlite3_geopoly_init(
  sqlite3 *db, 
  char **pzErrMsg, 
  const sqlite3_api_routines *pApi
){
  int rc = SQLITE_OK;
  static const struct {
    void (*xFunc)(sqlite3_context*,int,sqlite3_value**);
    int nArg;
    const char *zName;
  } aFunc[] = {
     { geopolyAreaFunc,          1,    "geopoly_area"  },
     { geopolyBlobFunc,          1,    "geopoly_blob"  },
     { geopolyJsonFunc,          1,    "geopoly_json"  },
  };
  int i;
  SQLITE_EXTENSION_INIT2(pApi);
  (void)pzErrMsg;  /* Unused parameter */
  for(i=0; i<sizeof(aFunc)/sizeof(aFunc[0]) && rc==SQLITE_OK; i++){
    rc = sqlite3_create_function(db, aFunc[i].zName, aFunc[i].nArg,
                                 SQLITE_UTF8, 0,
                                 aFunc[i].xFunc, 0, 0);
  }
  return rc;
}

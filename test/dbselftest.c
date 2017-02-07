/*
** 2017-02-07
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
*************************************************************************
**
** This program implements an SQLite database self-verification utility.
** Usage:
** 
**       dbselftest DATABASE ...
**
** This program reads the "selftest" table in DATABASE, in rowid order,
** and runs each of the tests described there, reporting results at the
** end.
**
** The intent of this program is to have a set of test database files that
** can be run using future versions of SQLite in order to verify that
** legacy database files continue to be readable.  In other words, the
** intent is to confirm that there have been no breaking changes in the 
** file format.  The program can also be used to verify that database files
** are fully compatible between different architectures.
**
** The selftest table looks like this:
**
**     CREATE TABLE selftest (
**       id INTEGER PRIMARY KEY,    -- Run tests in ascending order
**       op TEXT,                   -- "test", "regexp", "print", etc.
**       cmdtxt TEXT,               -- Usually the SQL to be run
**       expected TEXT              -- Expected results
**     );
**
*/
#include <assert.h>
#include <string.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include "sqlite3.h"

static const char zHelp[] =
  "Usage: dbselftest [OPTIONS] DBFILE ...\n"
  "\n"
  "    --init         Create the selftest table\n"
  "    -q             Suppress most output.  Errors only\n"
  "    -v             Show extra output\n"
;


/******************************************************************************
** The following code from ext/misc/sha1.c
**
** Context for the SHA1 hash 
*/
typedef struct SHA1Context SHA1Context;
struct SHA1Context {
  unsigned int state[5];
  unsigned int count[2];
  unsigned char buffer[64];
};


#if __GNUC__ && (defined(__i386__) || defined(__x86_64__))
/*
 * GCC by itself only generates left rotates.  Use right rotates if
 * possible to be kinder to dinky implementations with iterative rotate
 * instructions.
 */
#define SHA_ROT(op, x, k) \
        ({ unsigned int y; asm(op " %1,%0" : "=r" (y) : "I" (k), "0" (x)); y; })
#define rol(x,k) SHA_ROT("roll", x, k)
#define ror(x,k) SHA_ROT("rorl", x, k)

#else
/* Generic C equivalent */
#define SHA_ROT(x,l,r) ((x) << (l) | (x) >> (r))
#define rol(x,k) SHA_ROT(x,k,32-(k))
#define ror(x,k) SHA_ROT(x,32-(k),k)
#endif


#define blk0le(i) (block[i] = (ror(block[i],8)&0xFF00FF00) \
    |(rol(block[i],8)&0x00FF00FF))
#define blk0be(i) block[i]
#define blk(i) (block[i&15] = rol(block[(i+13)&15]^block[(i+8)&15] \
    ^block[(i+2)&15]^block[i&15],1))

/*
 * (R0+R1), R2, R3, R4 are the different operations (rounds) used in SHA1
 *
 * Rl0() for little-endian and Rb0() for big-endian.  Endianness is
 * determined at run-time.
 */
#define Rl0(v,w,x,y,z,i) \
    z+=((w&(x^y))^y)+blk0le(i)+0x5A827999+rol(v,5);w=ror(w,2);
#define Rb0(v,w,x,y,z,i) \
    z+=((w&(x^y))^y)+blk0be(i)+0x5A827999+rol(v,5);w=ror(w,2);
#define R1(v,w,x,y,z,i) \
    z+=((w&(x^y))^y)+blk(i)+0x5A827999+rol(v,5);w=ror(w,2);
#define R2(v,w,x,y,z,i) \
    z+=(w^x^y)+blk(i)+0x6ED9EBA1+rol(v,5);w=ror(w,2);
#define R3(v,w,x,y,z,i) \
    z+=(((w|x)&y)|(w&x))+blk(i)+0x8F1BBCDC+rol(v,5);w=ror(w,2);
#define R4(v,w,x,y,z,i) \
    z+=(w^x^y)+blk(i)+0xCA62C1D6+rol(v,5);w=ror(w,2);

/*
 * Hash a single 512-bit block. This is the core of the algorithm.
 */
void SHA1Transform(unsigned int state[5], const unsigned char buffer[64]){
  unsigned int qq[5]; /* a, b, c, d, e; */
  static int one = 1;
  unsigned int block[16];
  memcpy(block, buffer, 64);
  memcpy(qq,state,5*sizeof(unsigned int));

#define a qq[0]
#define b qq[1]
#define c qq[2]
#define d qq[3]
#define e qq[4]

  /* Copy p->state[] to working vars */
  /*
  a = state[0];
  b = state[1];
  c = state[2];
  d = state[3];
  e = state[4];
  */

  /* 4 rounds of 20 operations each. Loop unrolled. */
  if( 1 == *(unsigned char*)&one ){
    Rl0(a,b,c,d,e, 0); Rl0(e,a,b,c,d, 1); Rl0(d,e,a,b,c, 2); Rl0(c,d,e,a,b, 3);
    Rl0(b,c,d,e,a, 4); Rl0(a,b,c,d,e, 5); Rl0(e,a,b,c,d, 6); Rl0(d,e,a,b,c, 7);
    Rl0(c,d,e,a,b, 8); Rl0(b,c,d,e,a, 9); Rl0(a,b,c,d,e,10); Rl0(e,a,b,c,d,11);
    Rl0(d,e,a,b,c,12); Rl0(c,d,e,a,b,13); Rl0(b,c,d,e,a,14); Rl0(a,b,c,d,e,15);
  }else{
    Rb0(a,b,c,d,e, 0); Rb0(e,a,b,c,d, 1); Rb0(d,e,a,b,c, 2); Rb0(c,d,e,a,b, 3);
    Rb0(b,c,d,e,a, 4); Rb0(a,b,c,d,e, 5); Rb0(e,a,b,c,d, 6); Rb0(d,e,a,b,c, 7);
    Rb0(c,d,e,a,b, 8); Rb0(b,c,d,e,a, 9); Rb0(a,b,c,d,e,10); Rb0(e,a,b,c,d,11);
    Rb0(d,e,a,b,c,12); Rb0(c,d,e,a,b,13); Rb0(b,c,d,e,a,14); Rb0(a,b,c,d,e,15);
  }
  R1(e,a,b,c,d,16); R1(d,e,a,b,c,17); R1(c,d,e,a,b,18); R1(b,c,d,e,a,19);
  R2(a,b,c,d,e,20); R2(e,a,b,c,d,21); R2(d,e,a,b,c,22); R2(c,d,e,a,b,23);
  R2(b,c,d,e,a,24); R2(a,b,c,d,e,25); R2(e,a,b,c,d,26); R2(d,e,a,b,c,27);
  R2(c,d,e,a,b,28); R2(b,c,d,e,a,29); R2(a,b,c,d,e,30); R2(e,a,b,c,d,31);
  R2(d,e,a,b,c,32); R2(c,d,e,a,b,33); R2(b,c,d,e,a,34); R2(a,b,c,d,e,35);
  R2(e,a,b,c,d,36); R2(d,e,a,b,c,37); R2(c,d,e,a,b,38); R2(b,c,d,e,a,39);
  R3(a,b,c,d,e,40); R3(e,a,b,c,d,41); R3(d,e,a,b,c,42); R3(c,d,e,a,b,43);
  R3(b,c,d,e,a,44); R3(a,b,c,d,e,45); R3(e,a,b,c,d,46); R3(d,e,a,b,c,47);
  R3(c,d,e,a,b,48); R3(b,c,d,e,a,49); R3(a,b,c,d,e,50); R3(e,a,b,c,d,51);
  R3(d,e,a,b,c,52); R3(c,d,e,a,b,53); R3(b,c,d,e,a,54); R3(a,b,c,d,e,55);
  R3(e,a,b,c,d,56); R3(d,e,a,b,c,57); R3(c,d,e,a,b,58); R3(b,c,d,e,a,59);
  R4(a,b,c,d,e,60); R4(e,a,b,c,d,61); R4(d,e,a,b,c,62); R4(c,d,e,a,b,63);
  R4(b,c,d,e,a,64); R4(a,b,c,d,e,65); R4(e,a,b,c,d,66); R4(d,e,a,b,c,67);
  R4(c,d,e,a,b,68); R4(b,c,d,e,a,69); R4(a,b,c,d,e,70); R4(e,a,b,c,d,71);
  R4(d,e,a,b,c,72); R4(c,d,e,a,b,73); R4(b,c,d,e,a,74); R4(a,b,c,d,e,75);
  R4(e,a,b,c,d,76); R4(d,e,a,b,c,77); R4(c,d,e,a,b,78); R4(b,c,d,e,a,79);

  /* Add the working vars back into context.state[] */
  state[0] += a;
  state[1] += b;
  state[2] += c;
  state[3] += d;
  state[4] += e;

#undef a
#undef b
#undef c
#undef d
#undef e
}


/* Initialize a SHA1 context */
static void hash_init(SHA1Context *p){
  /* SHA1 initialization constants */
  p->state[0] = 0x67452301;
  p->state[1] = 0xEFCDAB89;
  p->state[2] = 0x98BADCFE;
  p->state[3] = 0x10325476;
  p->state[4] = 0xC3D2E1F0;
  p->count[0] = p->count[1] = 0;
}

/* Add new content to the SHA1 hash */
static void hash_step(
  SHA1Context *p,                 /* Add content to this context */
  const unsigned char *data,      /* Data to be added */
  unsigned int len                /* Number of bytes in data */
){
  unsigned int i, j;

  j = p->count[0];
  if( (p->count[0] += len << 3) < j ){
    p->count[1] += (len>>29)+1;
  }
  j = (j >> 3) & 63;
  if( (j + len) > 63 ){
    (void)memcpy(&p->buffer[j], data, (i = 64-j));
    SHA1Transform(p->state, p->buffer);
    for(; i + 63 < len; i += 64){
      SHA1Transform(p->state, &data[i]);
    }
    j = 0;
  }else{
    i = 0;
  }
  (void)memcpy(&p->buffer[j], &data[i], len - i);
}

/* Compute a string using sqlite3_vsnprintf() and hash it */
static void hash_step_vformat(
  SHA1Context *p,                 /* Add content to this context */
  const char *zFormat,
  ...
){
  va_list ap;
  int n;
  char zBuf[50];
  va_start(ap, zFormat);
  sqlite3_vsnprintf(sizeof(zBuf),zBuf,zFormat,ap);
  va_end(ap);
  n = (int)strlen(zBuf);
  hash_step(p, (unsigned char*)zBuf, n);
}


/* Add padding and compute the message digest.  Render the
** message digest as lower-case hexadecimal and put it into
** zOut[].  zOut[] must be at least 41 bytes long. */
static void hash_finish(
  SHA1Context *p,           /* The SHA1 context to finish and render */
  char *zOut                /* Store hexadecimal hash here */
){
  unsigned int i;
  unsigned char finalcount[8];
  unsigned char digest[20];
  static const char zEncode[] = "0123456789abcdef";

  for (i = 0; i < 8; i++){
    finalcount[i] = (unsigned char)((p->count[(i >= 4 ? 0 : 1)]
       >> ((3-(i & 3)) * 8) ) & 255); /* Endian independent */
  }
  hash_step(p, (const unsigned char *)"\200", 1);
  while ((p->count[0] & 504) != 448){
    hash_step(p, (const unsigned char *)"\0", 1);
  }
  hash_step(p, finalcount, 8);  /* Should cause a SHA1Transform() */
  for (i = 0; i < 20; i++){
    digest[i] = (unsigned char)((p->state[i>>2] >> ((3-(i & 3)) * 8) ) & 255);
  }
  for(i=0; i<20; i++){
    zOut[i*2] = zEncode[(digest[i]>>4)&0xf];
    zOut[i*2+1] = zEncode[digest[i] & 0xf];
  }
  zOut[i*2]= 0;
}

/*
** Implementation of the sha1(X) function.
**
** Return a lower-case hexadecimal rendering of the SHA1 hash of the
** argument X.  If X is a BLOB, it is hashed as is.  For all other
** types of input, X is converted into a UTF-8 string and the string
** is hash without the trailing 0x00 terminator.  The hash of a NULL
** value is NULL.
*/
static void sha1Func(
  sqlite3_context *context,
  int argc,
  sqlite3_value **argv
){
  SHA1Context cx;
  int eType = sqlite3_value_type(argv[0]);
  int nByte = sqlite3_value_bytes(argv[0]);
  char zOut[44];

  assert( argc==1 );
  if( eType==SQLITE_NULL ) return;
  hash_init(&cx);
  if( eType==SQLITE_BLOB ){
    hash_step(&cx, sqlite3_value_blob(argv[0]), nByte);
  }else{
    hash_step(&cx, sqlite3_value_text(argv[0]), nByte);
  }
  hash_finish(&cx, zOut);
  sqlite3_result_text(context, zOut, 40, SQLITE_TRANSIENT);
}

/*
** Run a prepared statement and compute the SHA1 hash on the
** result rows.
*/
static void sha1RunStatement(SHA1Context *pCtx, sqlite3_stmt *pStmt){
  int nCol = sqlite3_column_count(pStmt);
  const char *z = sqlite3_sql(pStmt);
  int n = (int)strlen(z);

  hash_step_vformat(pCtx,"S%d:",n);
  hash_step(pCtx,(unsigned char*)z,n);

  /* Compute a hash over the result of the query */
  while( SQLITE_ROW==sqlite3_step(pStmt) ){
    int i;
    hash_step(pCtx,(const unsigned char*)"R",1);
    for(i=0; i<nCol; i++){
      switch( sqlite3_column_type(pStmt,i) ){
        case SQLITE_NULL: {
          hash_step(pCtx, (const unsigned char*)"N",1);
          break;
        }
        case SQLITE_INTEGER: {
          sqlite3_uint64 u;
          int j;
          unsigned char x[9];
          sqlite3_int64 v = sqlite3_column_int64(pStmt,i);
          memcpy(&u, &v, 8);
          for(j=8; j>=1; j--){
            x[j] = u & 0xff;
            u >>= 8;
          }
          x[0] = 'I';
          hash_step(pCtx, x, 9);
          break;
        }
        case SQLITE_FLOAT: {
          sqlite3_uint64 u;
          int j;
          unsigned char x[9];
          double r = sqlite3_column_double(pStmt,i);
          memcpy(&u, &r, 8);
          for(j=8; j>=1; j--){
            x[j] = u & 0xff;
            u >>= 8;
          }
          x[0] = 'F';
          hash_step(pCtx,x,9);
          break;
        }
        case SQLITE_TEXT: {
          int n2 = sqlite3_column_bytes(pStmt, i);
          const unsigned char *z2 = sqlite3_column_text(pStmt, i);
          hash_step_vformat(pCtx,"T%d:",n2);
          hash_step(pCtx, z2, n2);
          break;
        }
        case SQLITE_BLOB: {
          int n2 = sqlite3_column_bytes(pStmt, i);
          const unsigned char *z2 = sqlite3_column_blob(pStmt, i);
          hash_step_vformat(pCtx,"B%d:",n2);
          hash_step(pCtx, z2, n2);
          break;
        }
      }
    }
  }
}

/*
** Run one or more statements of SQL.  Compute a SHA1 hash of the output.
*/
static int sha1Exec(
  sqlite3 *db,          /* Run against this database connection */
  const char *zSql,     /* The SQL to be run */
  char *zOut            /* Store the SHA1 hash as hexadecimal in this buffer */
){
  sqlite3_stmt *pStmt = 0;    /* A prepared statement */
  int rc;                     /* Result of an API call */
  SHA1Context cx;             /* The SHA1 hash context */

  hash_init(&cx);
  while( zSql[0] ){
    rc = sqlite3_prepare_v2(db, zSql, -1, &pStmt, &zSql);
    if( rc ){
      sqlite3_finalize(pStmt);
      return rc;
    }
    sha1RunStatement(&cx, pStmt);
    sqlite3_finalize(pStmt);
  }
  hash_finish(&cx, zOut);
  return SQLITE_OK;
}

/*
** Implementation of the sha1_query(SQL) function.
**
** This function compiles and runs the SQL statement(s) given in the
** argument. The results are hashed using SHA1 and that hash is returned.
**
** The original SQL text is included as part of the hash.
**
** The hash is not just a concatenation of the outputs.  Each query
** is delimited and each row and value within the query is delimited,
** with all values being marked with their datatypes.
*/
static void sha1QueryFunc(
  sqlite3_context *context,
  int argc,
  sqlite3_value **argv
){
  sqlite3 *db = sqlite3_context_db_handle(context);
  const char *zSql = (const char*)sqlite3_value_text(argv[0]);
  sqlite3_stmt *pStmt = 0;
  int rc;
  SHA1Context cx;
  char zOut[44];

  assert( argc==1 );
  if( zSql==0 ) return;
  hash_init(&cx);
  while( zSql[0] ){
    rc = sqlite3_prepare_v2(db, zSql, -1, &pStmt, &zSql);
    if( rc ){
      char *zMsg = sqlite3_mprintf("error SQL statement [%s]: %s",
                                   zSql, sqlite3_errmsg(db));
      sqlite3_finalize(pStmt);
      sqlite3_result_error(context, zMsg, -1);
      sqlite3_free(zMsg);
      return;
    }
    if( !sqlite3_stmt_readonly(pStmt) ){
      char *zMsg = sqlite3_mprintf("non-query: [%s]", sqlite3_sql(pStmt));
      sqlite3_finalize(pStmt);
      sqlite3_result_error(context, zMsg, -1);
      sqlite3_free(zMsg);
      return;
    }
    sha1RunStatement(&cx, pStmt);
    sqlite3_finalize(pStmt);
  }
  hash_finish(&cx, zOut);
  sqlite3_result_text(context, zOut, 40, SQLITE_TRANSIENT);
}
/* End of ext/misc/sha1.c
******************************************************************************/

/* How much output to display */
#define VOLUME_MIN          0
#define VOLUME_OFF          0
#define VOLUME_ERROR_ONLY   1
#define VOLUME_LOW          2
#define VOLUME_ECHO         3
#define VOLUME_VERBOSE      4
#define VOLUME_MAX          4

/* A string accumulator
*/
typedef struct Str {
  char *z;             /* Accumulated text */
  int n;               /* Bytes of z[] used so far */
  int nAlloc;          /* Bytes allocated for z[] */
} Str;

/* Append text to the Str object
*/
static void strAppend(Str *p, const char *z){
  int n = (int)strlen(z);
  if( p->n+n >= p->nAlloc ){
    p->nAlloc += p->n+n + 100;
    p->z = sqlite3_realloc(p->z, p->nAlloc);
    if( z==0 ){
      printf("Could not allocate %d bytes\n", p->nAlloc);
      exit(1);
    }
  }
  memcpy(p->z+p->n, z, n+1);
  p->n += n;
}

/* This is an sqlite3_exec() callback that will capture all
** output in a Str.
**
** Columns are separated by ",".  Rows are separated by "|".
*/
static int execCallback(void *pStr, int argc, char **argv, char **colv){
  int i;
  Str *p = (Str*)pStr;
  if( p->n ) strAppend(p, "|");
  for(i=0; i<argc; i++){
    const char *z = (const char*)argv[i];
    if( z==0 ) z = "NULL";
    if( i>0 ) strAppend(p, ",");
    strAppend(p, z);
  }
  return 0;
}

/*
** Run an SQL statement constructing using sqlite3_vmprintf().
** Return the number of errors.
*/
static int runSql(sqlite3 *db, const char *zFormat, ...){
  char *zSql;
  char *zErr = 0;
  int rc;
  int nErr = 0;
  va_list ap;

  va_start(ap, zFormat);
  zSql = sqlite3_vmprintf(zFormat, ap);
  va_end(ap);
  if( zSql==0 ){
    printf("Out of memory\n");
    exit(1);
  }
  rc = sqlite3_exec(db, zSql, 0, 0, &zErr);
  if( rc || zErr ){
    printf("SQL error in [%s]: code=%d: %s\n", zSql, rc, zErr);
    nErr++;
  }
  sqlite3_free(zSql);
  return nErr;
}

/*
** Generate a prepared statement using a formatted string.
*/
static sqlite3_stmt *prepareSql(sqlite3 *db, const char *zFormat, ...){
  char *zSql;
  int rc;
  sqlite3_stmt *pStmt = 0;
  va_list ap;

  va_start(ap, zFormat);
  zSql = sqlite3_vmprintf(zFormat, ap);
  va_end(ap);
  if( zSql==0 ){
    printf("Out of memory\n");
    exit(1);
  }
  rc = sqlite3_prepare_v2(db, zSql, -1, &pStmt, 0);
  if( rc ){
    printf("SQL error in [%s]: code=%d: %s\n", zSql, rc, sqlite3_errmsg(db));
    sqlite3_finalize(pStmt);
    pStmt = 0;
  }
  sqlite3_free(zSql);
  return pStmt;
}

/*
** Construct the standard selftest configuration for the database.
*/
static int buildSelftestTable(sqlite3 *db){
  int rc;
  sqlite3_stmt *pStmt;
  int tno = 110;
  char *zSql;
  char zHash[50];

  rc = runSql(db,
     "CREATE TABLE IF NOT EXISTS selftest(\n"
     "  tno INTEGER PRIMARY KEY,  -- test number\n"
     "  op TEXT,                  -- what kind of test\n"
     "  sql TEXT,                 -- SQL text for the test\n"
     "  ans TEXT                  -- expected answer\n"
     ");"
     "INSERT INTO selftest"
     " VALUES(100,'memo','Hashes generated using --init',NULL);"
  );
  if( rc ) return 1;
  tno = 110;
  zSql = "SELECT type,name,tbl_name,sql FROM sqlite_master ORDER BY name";
  sha1Exec(db, zSql, zHash);
  rc = runSql(db, 
      "INSERT INTO selftest(tno,op,sql,ans)"
      " VALUES(%d,'sha1',%Q,%Q)", tno, zSql, zHash);
  tno += 10;
  pStmt = prepareSql(db,
    "SELECT lower(name) FROM sqlite_master"
    " WHERE type='table' AND sql NOT GLOB 'CREATE VIRTUAL*'"
    "   AND name<>'selftest'"
    " ORDER BY 1");
  if( pStmt==0 ) return 1;
  while( SQLITE_ROW==sqlite3_step(pStmt) ){
    zSql = sqlite3_mprintf("SELECT * FROM \"%w\" NOT INDEXED",
                            sqlite3_column_text(pStmt, 0));
    if( zSql==0 ){
      printf("Of of memory\n");
      exit(1);
    }
    sha1Exec(db, zSql, zHash);
    rc = runSql(db,
      "INSERT INTO selftest(tno,op,sql,ans)"
      " VALUES(%d,'sha1',%Q,%Q)", tno, zSql, zHash);
    tno += 10;
    sqlite3_free(zSql);
    if( rc ) break;
  }
  sqlite3_finalize(pStmt);
  if( rc ) return 1;
  rc = runSql(db,
     "INSERT INTO selftest(tno,op,sql,ans)"
     " VALUES(%d,'run','PRAGMA integrity_check','ok');", tno);
  if( rc ) return 1;
  return rc;
}

/*
** Return true if the named table exists
*/
static int tableExists(sqlite3 *db, const char *zTab){
  return sqlite3_table_column_metadata(db, "main", zTab, 0, 0, 0, 0, 0, 0)
            == SQLITE_OK;
}

/*
** Default selftest table content, for use when there is no selftest table
*/
static char *azDefaultTest[] = {
   0, 0, 0, 0,
   "0", "memo", "Missing SELFTEST table - default checks only", "",
   "1", "run", "PRAGMA integrity_check", "ok"
};

int main(int argc, char **argv){
  int eVolume = VOLUME_LOW;    /* How much output to display */
  const char **azDb = 0;       /* Name of the database file */
  int nDb = 0;                 /* Number of database files to check */
  int doInit = 0;              /* True if --init is present */
  sqlite3 *db = 0;             /* Open database connection */
  int rc;                      /* Return code from API calls */
  char *zErrMsg = 0;           /* An error message return */
  char **azTest;               /* Content of the selftest table */
  int nRow = 0, nCol = 0;      /* Rows and columns in azTest[] */
  int i;                       /* Loop counter */
  int nErr = 0;                /* Number of errors */
  int iDb;                     /* Loop counter for databases */
  Str str;                     /* Result accumulator */
  int nTest = 0;               /* Number of tests run */

  for(i=1; i<argc; i++){
    const char *z = argv[i];
    if( z[0]=='-' ){
      if( z[1]=='-' ) z++;
      if( strcmp(z, "-help")==0 ){
        printf("%s", zHelp);
        return 0;
      }else
      if( strcmp(z, "-init")==0 ){
        doInit = 1;
      }else
      if( strcmp(z, "-a")==0 ){
        if( eVolume>VOLUME_MIN) eVolume--;
      }else
      if( strcmp(z, "-v")==0 ){
        if( eVolume<VOLUME_MAX) eVolume++;
      }else
      {
        printf("unknown option: \"%s\"\nUse --help for more information\n",
               argv[i]);
        return 1;
      }
    }else{
      nDb++;
      azDb = sqlite3_realloc(azDb, nDb*sizeof(azDb[0]));
      if( azDb==0 ){
        printf("out of memory\n");
        exit(1);
      }
      azDb[nDb-1] = argv[i];
    }
  }
  if( nDb==0 ){
    printf("No databases specified.  Use --help for more info\n");
    return 1;
  }
  if( eVolume>=VOLUME_LOW ){
    printf("SQLite %s\n", sqlite3_sourceid());
  }
  memset(&str, 0, sizeof(str));
  strAppend(&str, "\n");
  for(iDb=0; iDb<nDb; iDb++, sqlite3_close(db)){
    rc = sqlite3_open_v2(azDb[iDb], &db, 
          doInit ? SQLITE_OPEN_READWRITE : SQLITE_OPEN_READONLY, 0);
    if( rc ){
      printf("Cannot open \"%s\": %s\n", azDb[iDb], sqlite3_errmsg(db));
      return 1;
    }
    rc = sqlite3_create_function(db, "sha1", 1, SQLITE_UTF8, 0,
                                 sha1Func, 0, 0);
    if( rc==SQLITE_OK ){
      rc = sqlite3_create_function(db, "sha1_query", 1, SQLITE_UTF8, 0,
                                   sha1QueryFunc, 0, 0);
    }
    if( rc ){
      printf("Initialization error: %s\n", sqlite3_errmsg(db));
      sqlite3_close(db);
      return 1;
    }
    if( doInit && !tableExists(db, "selftest") ){
       buildSelftestTable(db);
    }
    if( !tableExists(db, "selftest") ){
      azTest = azDefaultTest;
      nCol = 4;
      nRow = 2;
    }else{
      rc = sqlite3_get_table(db, 
          "SELECT tno,op,sql,ans FROM selftest ORDER BY tno",
          &azTest, &nRow, &nCol, &zErrMsg);
      if( rc || zErrMsg ){
        printf("Error querying selftest: %s\n", zErrMsg);
        sqlite3_free_table(azTest);
        continue;
      }
    }
    for(i=1; i<=nRow; i++){
      int tno = atoi(azTest[i*nCol]);
      const char *zOp = azTest[i*nCol+1];
      const char *zSql = azTest[i*nCol+2];
      const char *zAns = azTest[i*nCol+3];
  
      if( eVolume>=VOLUME_ECHO ){
        char *zQuote = sqlite3_mprintf("%q", zSql);
        printf("%d: %s %s\n", tno, zOp, zSql);
        sqlite3_free(zQuote);
      }
      if( strcmp(zOp,"memo")==0 ){
        if( eVolume>=VOLUME_LOW ){
          printf("%s: %s\n", azDb[iDb], zSql);
        }
      }else
      if( strcmp(zOp,"sha1")==0 ){
        char zOut[44];
        rc = sha1Exec(db, zSql, zOut);
        nTest++;
        if( eVolume>=VOLUME_VERBOSE ){
          printf("Result: %s\n", zOut);
        }
        if( rc ){
          nErr++;
          if( eVolume>=VOLUME_ERROR_ONLY ){
            printf("%d: error-code-%d: %s\n", tno, rc, sqlite3_errmsg(db));
          }
        }else if( strcmp(zAns,zOut)!=0 ){
          nErr++;
          if( eVolume>=VOLUME_ERROR_ONLY ){
            printf("%d: Expected: [%s]\n", tno, zAns);
            printf("%d:      Got: [%s]\n", tno, zOut);
          }
        }
      }else
      if( strcmp(zOp,"run")==0 ){
        str.n = 0;
        str.z[0] = 0;
        zErrMsg = 0;
        rc = sqlite3_exec(db, zSql, execCallback, &str, &zErrMsg);
        nTest++;
        if( eVolume>=VOLUME_VERBOSE ){
          printf("Result: %s\n", str.z);
        }
        if( rc || zErrMsg ){
          nErr++;
          if( eVolume>=VOLUME_ERROR_ONLY ){
            printf("%d: error-code-%d: %s\n", tno, rc, zErrMsg);
          }
          sqlite3_free(zErrMsg);
        }else if( strcmp(zAns,str.z)!=0 ){
          nErr++;
          if( eVolume>=VOLUME_ERROR_ONLY ){
            printf("%d: Expected: [%s]\n", tno, zAns);
            printf("%d:      Got: [%s]\n", tno, str.z);
          }
        }
      }else
      {
        printf("Unknown operation \"%s\" on selftest line %d\n", zOp, tno);
        return 1;
      }
    }
    if( azTest!=azDefaultTest ) sqlite3_free_table(azTest);
  }
  if( eVolume>=VOLUME_LOW || (nErr>0 && eVolume>=VOLUME_ERROR_ONLY) ){
    printf("%d errors out of %d tests on %d databases\n", nErr, nTest, nDb);
  }
  return nErr;
}

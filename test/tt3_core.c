/*
** 2016-05-07
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
*************************************************************************
*/


#include <unistd.h>
#include <stdio.h>
#include <pthread.h>
#include <assert.h>
#include <sys/types.h> 
#include <sys/stat.h> 
#include <string.h>
#include <fcntl.h>
#include <errno.h>
#include <stdint.h>

/* 
** The "Set Error Line" macro.
*/
#define SEL(e) ((e)->iLine = ((e)->rc ? (e)->iLine : __LINE__))

/* Database functions */
#define opendb(w,x,y,z)         (SEL(w), opendb_x(w,x,y,z))
#define closedb(y,z)            (SEL(y), closedb_x(y,z))

/* Functions to execute SQL */
#define sql_script(x,y,z)       (SEL(x), sql_script_x(x,y,z))
#define integrity_check(x,y)    (SEL(x), integrity_check_x(x,y))
#define execsql_i64(x,y,...)    (SEL(x), execsql_i64_x(x,y,__VA_ARGS__))
#define execsql_text(x,y,z,...) (SEL(x), execsql_text_x(x,y,z,__VA_ARGS__))
#define execsql(x,y,...)        (SEL(x), (void)execsql_i64_x(x,y,__VA_ARGS__))
#define sql_script_printf(x,y,z,...) (                \
    SEL(x), sql_script_printf_x(x,y,z,__VA_ARGS__)    \
) 

/* Thread functions */
#define launch_thread(w,x,y,z)     (SEL(w), launch_thread_x(w,x,y,z))
#define join_all_threads(y,z)      (SEL(y), join_all_threads_x(y,z))

/* Timer functions */
#define setstoptime(y,z)        (SEL(y), setstoptime_x(y,z))
#define timetostop(z)           (SEL(z), timetostop_x(z))

/* Report/clear errors. */
#define test_error(z, ...)      test_error_x(z, sqlite3_mprintf(__VA_ARGS__))
#define clear_error(y,z)        clear_error_x(y, z)

/* File-system operations */
#define filesize(y,z)           (SEL(y), filesize_x(y,z))
#define filecopy(x,y,z)         (SEL(x), filecopy_x(x,y,z))

#define PTR2INT(x) ((int)((intptr_t)x))
#define INT2PTR(x) ((void*)((intptr_t)x))

/*
** End of test code/infrastructure interface macros.
*************************************************************************/


/************************************************************************
** Start of command line processing utilities.
*/
#define CMDLINE_INT     1
#define CMDLINE_BOOL    2
#define CMDLINE_STRING  3

typedef struct CmdlineArg CmdlineArg;
struct CmdlineArg {
  const char *zSwitch;
  int eType;
  int iOffset;
};

static void cmdline_error(const char *zFmt, ...){
  va_list ap;                   /* ... arguments */
  char *zMsg = 0;
  va_start(ap, zFmt);
  zMsg = sqlite3_vmprintf(zFmt, ap);
  fprintf(stderr, "%s\n", zMsg);
  sqlite3_free(zMsg);
  va_end(ap);
  exit(-1);
}

static void cmdline_usage(const char *zPrg, CmdlineArg *apArg){
  int i;
  fprintf(stderr, "Usage: %s SWITCHES\n", zPrg);
  fprintf(stderr, "\n");
  fprintf(stderr, "where switches are\n");
  for(i=0; apArg[i].zSwitch; i++){
    const char *zExtra = "";
    switch( apArg[i].eType ){
      case CMDLINE_STRING: zExtra = "STRING"; break;
      case CMDLINE_INT: zExtra = "N"; break;
      case CMDLINE_BOOL: zExtra = ""; break;
      default:
        zExtra = "???";
        break;
    }
    fprintf(stderr, "  %s %s\n", apArg[i].zSwitch, zExtra);
  }
  fprintf(stderr, "\n");
  exit(-2);
}

static char *cmdline_construct(CmdlineArg *apArg, void *pObj){
  unsigned char *p = (unsigned char*)pObj;
  char *zRet = 0;
  int iArg;

  for(iArg=0; apArg[iArg].zSwitch; iArg++){
    const char *zSpace = (zRet ? " " : "");
    CmdlineArg *pArg = &apArg[iArg];

    switch( pArg->eType ){
      case CMDLINE_STRING: {
        char *zVal = *(char**)(p + pArg->iOffset);
        if( zVal ){
          zRet = sqlite3_mprintf("%z%s%s %s", zRet, zSpace, pArg->zSwitch,zVal);
        }
        break;
      };

      case CMDLINE_INT: {
        zRet = sqlite3_mprintf("%z%s%s %d", zRet, zSpace, pArg->zSwitch, 
            *(int*)(p + pArg->iOffset)
        );
        break;
      };

      case CMDLINE_BOOL: 
        if( *(int*)(p + pArg->iOffset) ){
          zRet = sqlite3_mprintf("%z%s%s", zRet, zSpace, pArg->zSwitch);
        }
        break;
        
      default:
        zRet = sqlite3_mprintf("%z%s%s ???", zRet, zSpace, pArg->zSwitch);
    }
  }

  return zRet;
}

static void cmdline_process(
 CmdlineArg *apArg, 
 int argc,
 const char **argv,
 void *pObj
){
  int i;
  int iArg;
  unsigned char *p = (unsigned char*)pObj;

  for(i=1; i<argc; i++){
    const char *z = argv[i];
    int n = strlen(z);
    int iOpt = -1;

    if( z[0]=='-' && z[1]=='-' ){
      z++;
      n--;
    }

    for(iArg=0; apArg[iArg].zSwitch; iArg++){
      if( 0==sqlite3_strnicmp(apArg[iArg].zSwitch, z, n) ){
        if( iOpt>=0 ){
          cmdline_error("ambiguous switch: %s", z);
        }
        iOpt = iArg;
        switch( apArg[iArg].eType ){
          case CMDLINE_INT:
            i++;
            if( i==argc ){
              cmdline_error("option requires an argument: %s", z);
            }
            *(int*)(p + apArg[iArg].iOffset) = atoi(argv[i]);
            break;

          case CMDLINE_STRING:
            i++;
            if( i==argc ){
              cmdline_error("option requires an argument: %s", z);
            }
            *(char**)(p + apArg[iArg].iOffset) = sqlite3_mprintf("%s", argv[i]);
            break;

          case CMDLINE_BOOL:
            *(int*)(p + apArg[iArg].iOffset) = 1;
            break;

          default:
            assert( 0 );
            cmdline_error("internal error");
            return;
        }
      }
    }

    if( iOpt<0 ){
      cmdline_usage(argv[0], apArg);
    }
  }
}

/*
** End of command line processing utilities.
*************************************************************************/


/*
 * This code implements the MD5 message-digest algorithm.
 * The algorithm is due to Ron Rivest.  This code was
 * written by Colin Plumb in 1993, no copyright is claimed.
 * This code is in the public domain; do with it what you wish.
 *
 * Equivalent code is available from RSA Data Security, Inc.
 * This code has been tested against that, and is equivalent,
 * except that you don't need to include two pages of legalese
 * with every copy.
 *
 * To compute the message digest of a chunk of bytes, declare an
 * MD5Context structure, pass it to MD5Init, call MD5Update as
 * needed on buffers full of bytes, and then call MD5Final, which
 * will fill a supplied 16-byte array with the digest.
 */

/*
 * If compiled on a machine that doesn't have a 32-bit integer,
 * you just set "uint32" to the appropriate datatype for an
 * unsigned 32-bit integer.  For example:
 *
 *       cc -Duint32='unsigned long' md5.c
 *
 */
#ifndef uint32
#  define uint32 unsigned int
#endif

struct MD5Context {
  int isInit;
  uint32 buf[4];
  uint32 bits[2];
  union {
    unsigned char in[64];
    uint32 in32[16];
  } u;
};
typedef struct MD5Context MD5Context;

/*
 * Note: this code is harmless on little-endian machines.
 */
static void byteReverse (unsigned char *buf, unsigned longs){
  uint32 t;
  do {
    t = (uint32)((unsigned)buf[3]<<8 | buf[2]) << 16 |
          ((unsigned)buf[1]<<8 | buf[0]);
    *(uint32 *)buf = t;
    buf += 4;
  } while (--longs);
}
/* The four core functions - F1 is optimized somewhat */

/* #define F1(x, y, z) (x & y | ~x & z) */
#define F1(x, y, z) (z ^ (x & (y ^ z)))
#define F2(x, y, z) F1(z, x, y)
#define F3(x, y, z) (x ^ y ^ z)
#define F4(x, y, z) (y ^ (x | ~z))

/* This is the central step in the MD5 algorithm. */
#define MD5STEP(f, w, x, y, z, data, s) \
  ( w += f(x, y, z) + data,  w = w<<s | w>>(32-s),  w += x )

/*
 * The core of the MD5 algorithm, this alters an existing MD5 hash to
 * reflect the addition of 16 longwords of new data.  MD5Update blocks
 * the data and converts bytes into longwords for this routine.
 */
static void MD5Transform(uint32 buf[4], const uint32 in[16]){
  register uint32 a, b, c, d;

  a = buf[0];
  b = buf[1];
  c = buf[2];
  d = buf[3];

  MD5STEP(F1, a, b, c, d, in[ 0]+0xd76aa478,  7);
  MD5STEP(F1, d, a, b, c, in[ 1]+0xe8c7b756, 12);
  MD5STEP(F1, c, d, a, b, in[ 2]+0x242070db, 17);
  MD5STEP(F1, b, c, d, a, in[ 3]+0xc1bdceee, 22);
  MD5STEP(F1, a, b, c, d, in[ 4]+0xf57c0faf,  7);
  MD5STEP(F1, d, a, b, c, in[ 5]+0x4787c62a, 12);
  MD5STEP(F1, c, d, a, b, in[ 6]+0xa8304613, 17);
  MD5STEP(F1, b, c, d, a, in[ 7]+0xfd469501, 22);
  MD5STEP(F1, a, b, c, d, in[ 8]+0x698098d8,  7);
  MD5STEP(F1, d, a, b, c, in[ 9]+0x8b44f7af, 12);
  MD5STEP(F1, c, d, a, b, in[10]+0xffff5bb1, 17);
  MD5STEP(F1, b, c, d, a, in[11]+0x895cd7be, 22);
  MD5STEP(F1, a, b, c, d, in[12]+0x6b901122,  7);
  MD5STEP(F1, d, a, b, c, in[13]+0xfd987193, 12);
  MD5STEP(F1, c, d, a, b, in[14]+0xa679438e, 17);
  MD5STEP(F1, b, c, d, a, in[15]+0x49b40821, 22);

  MD5STEP(F2, a, b, c, d, in[ 1]+0xf61e2562,  5);
  MD5STEP(F2, d, a, b, c, in[ 6]+0xc040b340,  9);
  MD5STEP(F2, c, d, a, b, in[11]+0x265e5a51, 14);
  MD5STEP(F2, b, c, d, a, in[ 0]+0xe9b6c7aa, 20);
  MD5STEP(F2, a, b, c, d, in[ 5]+0xd62f105d,  5);
  MD5STEP(F2, d, a, b, c, in[10]+0x02441453,  9);
  MD5STEP(F2, c, d, a, b, in[15]+0xd8a1e681, 14);
  MD5STEP(F2, b, c, d, a, in[ 4]+0xe7d3fbc8, 20);
  MD5STEP(F2, a, b, c, d, in[ 9]+0x21e1cde6,  5);
  MD5STEP(F2, d, a, b, c, in[14]+0xc33707d6,  9);
  MD5STEP(F2, c, d, a, b, in[ 3]+0xf4d50d87, 14);
  MD5STEP(F2, b, c, d, a, in[ 8]+0x455a14ed, 20);
  MD5STEP(F2, a, b, c, d, in[13]+0xa9e3e905,  5);
  MD5STEP(F2, d, a, b, c, in[ 2]+0xfcefa3f8,  9);
  MD5STEP(F2, c, d, a, b, in[ 7]+0x676f02d9, 14);
  MD5STEP(F2, b, c, d, a, in[12]+0x8d2a4c8a, 20);

  MD5STEP(F3, a, b, c, d, in[ 5]+0xfffa3942,  4);
  MD5STEP(F3, d, a, b, c, in[ 8]+0x8771f681, 11);
  MD5STEP(F3, c, d, a, b, in[11]+0x6d9d6122, 16);
  MD5STEP(F3, b, c, d, a, in[14]+0xfde5380c, 23);
  MD5STEP(F3, a, b, c, d, in[ 1]+0xa4beea44,  4);
  MD5STEP(F3, d, a, b, c, in[ 4]+0x4bdecfa9, 11);
  MD5STEP(F3, c, d, a, b, in[ 7]+0xf6bb4b60, 16);
  MD5STEP(F3, b, c, d, a, in[10]+0xbebfbc70, 23);
  MD5STEP(F3, a, b, c, d, in[13]+0x289b7ec6,  4);
  MD5STEP(F3, d, a, b, c, in[ 0]+0xeaa127fa, 11);
  MD5STEP(F3, c, d, a, b, in[ 3]+0xd4ef3085, 16);
  MD5STEP(F3, b, c, d, a, in[ 6]+0x04881d05, 23);
  MD5STEP(F3, a, b, c, d, in[ 9]+0xd9d4d039,  4);
  MD5STEP(F3, d, a, b, c, in[12]+0xe6db99e5, 11);
  MD5STEP(F3, c, d, a, b, in[15]+0x1fa27cf8, 16);
  MD5STEP(F3, b, c, d, a, in[ 2]+0xc4ac5665, 23);

  MD5STEP(F4, a, b, c, d, in[ 0]+0xf4292244,  6);
  MD5STEP(F4, d, a, b, c, in[ 7]+0x432aff97, 10);
  MD5STEP(F4, c, d, a, b, in[14]+0xab9423a7, 15);
  MD5STEP(F4, b, c, d, a, in[ 5]+0xfc93a039, 21);
  MD5STEP(F4, a, b, c, d, in[12]+0x655b59c3,  6);
  MD5STEP(F4, d, a, b, c, in[ 3]+0x8f0ccc92, 10);
  MD5STEP(F4, c, d, a, b, in[10]+0xffeff47d, 15);
  MD5STEP(F4, b, c, d, a, in[ 1]+0x85845dd1, 21);
  MD5STEP(F4, a, b, c, d, in[ 8]+0x6fa87e4f,  6);
  MD5STEP(F4, d, a, b, c, in[15]+0xfe2ce6e0, 10);
  MD5STEP(F4, c, d, a, b, in[ 6]+0xa3014314, 15);
  MD5STEP(F4, b, c, d, a, in[13]+0x4e0811a1, 21);
  MD5STEP(F4, a, b, c, d, in[ 4]+0xf7537e82,  6);
  MD5STEP(F4, d, a, b, c, in[11]+0xbd3af235, 10);
  MD5STEP(F4, c, d, a, b, in[ 2]+0x2ad7d2bb, 15);
  MD5STEP(F4, b, c, d, a, in[ 9]+0xeb86d391, 21);

  buf[0] += a;
  buf[1] += b;
  buf[2] += c;
  buf[3] += d;
}

/*
 * Start MD5 accumulation.  Set bit count to 0 and buffer to mysterious
 * initialization constants.
 */
static void MD5Init(MD5Context *ctx){
  ctx->isInit = 1;
  ctx->buf[0] = 0x67452301;
  ctx->buf[1] = 0xefcdab89;
  ctx->buf[2] = 0x98badcfe;
  ctx->buf[3] = 0x10325476;
  ctx->bits[0] = 0;
  ctx->bits[1] = 0;
}

/*
 * Update context to reflect the concatenation of another buffer full
 * of bytes.
 */
static 
void MD5Update(MD5Context *ctx, const unsigned char *buf, unsigned int len){
  uint32 t;

  /* Update bitcount */

  t = ctx->bits[0];
  if ((ctx->bits[0] = t + ((uint32)len << 3)) < t)
    ctx->bits[1]++; /* Carry from low to high */
  ctx->bits[1] += len >> 29;

  t = (t >> 3) & 0x3f;    /* Bytes already in shsInfo->data */

  /* Handle any leading odd-sized chunks */

  if ( t ) {
    unsigned char *p = (unsigned char *)ctx->u.in + t;

    t = 64-t;
    if (len < t) {
      memcpy(p, buf, len);
      return;
    }
    memcpy(p, buf, t);
    byteReverse(ctx->u.in, 16);
    MD5Transform(ctx->buf, (uint32 *)ctx->u.in);
    buf += t;
    len -= t;
  }

  /* Process data in 64-byte chunks */

  while (len >= 64) {
    memcpy(ctx->u.in, buf, 64);
    byteReverse(ctx->u.in, 16);
    MD5Transform(ctx->buf, (uint32 *)ctx->u.in);
    buf += 64;
    len -= 64;
  }

  /* Handle any remaining bytes of data. */

  memcpy(ctx->u.in, buf, len);
}

/*
 * Final wrapup - pad to 64-byte boundary with the bit pattern 
 * 1 0* (64-bit count of bits processed, MSB-first)
 */
static void MD5Final(unsigned char digest[16], MD5Context *ctx){
  unsigned count;
  unsigned char *p;

  /* Compute number of bytes mod 64 */
  count = (ctx->bits[0] >> 3) & 0x3F;

  /* Set the first char of padding to 0x80.  This is safe since there is
     always at least one byte free */
  p = ctx->u.in + count;
  *p++ = 0x80;

  /* Bytes of padding needed to make 64 bytes */
  count = 64 - 1 - count;

  /* Pad out to 56 mod 64 */
  if (count < 8) {
    /* Two lots of padding:  Pad the first block to 64 bytes */
    memset(p, 0, count);
    byteReverse(ctx->u.in, 16);
    MD5Transform(ctx->buf, (uint32 *)ctx->u.in);

    /* Now fill the next block with 56 bytes */
    memset(ctx->u.in, 0, 56);
  } else {
    /* Pad block to 56 bytes */
    memset(p, 0, count-8);
  }
  byteReverse(ctx->u.in, 14);

  /* Append length in bits and transform */
  ctx->u.in32[14] = ctx->bits[0];
  ctx->u.in32[15] = ctx->bits[1];

  MD5Transform(ctx->buf, (uint32 *)ctx->u.in);
  byteReverse((unsigned char *)ctx->buf, 4);
  memcpy(digest, ctx->buf, 16);
  memset(ctx, 0, sizeof(*ctx));    /* In case it is sensitive */
}

/*
** Convert a 128-bit MD5 digest into a 32-digit base-16 number.
*/
static void MD5DigestToBase16(unsigned char *digest, char *zBuf){
  static char const zEncode[] = "0123456789abcdef";
  int i, j;

  for(j=i=0; i<16; i++){
    int a = digest[i];
    zBuf[j++] = zEncode[(a>>4)&0xf];
    zBuf[j++] = zEncode[a & 0xf];
  }
  zBuf[j] = 0;
}

/*
** During testing, the special md5sum() aggregate function is available.
** inside SQLite.  The following routines implement that function.
*/
static void md5step(sqlite3_context *context, int argc, sqlite3_value **argv){
  MD5Context *p;
  int i;
  if( argc<1 ) return;
  p = sqlite3_aggregate_context(context, sizeof(*p));
  if( p==0 ) return;
  if( !p->isInit ){
    MD5Init(p);
  }
  for(i=0; i<argc; i++){
    const char *zData = (char*)sqlite3_value_text(argv[i]);
    if( zData ){
      MD5Update(p, (unsigned char*)zData, strlen(zData));
    }
  }
}
static void md5finalize(sqlite3_context *context){
  MD5Context *p;
  unsigned char digest[16];
  char zBuf[33];
  p = sqlite3_aggregate_context(context, sizeof(*p));
  MD5Final(digest,p);
  MD5DigestToBase16(digest, zBuf);
  sqlite3_result_text(context, zBuf, -1, SQLITE_TRANSIENT);
}

/*
** End of copied md5sum() code.
**************************************************************************/

typedef sqlite3_int64 i64;

typedef struct Error Error;
typedef struct Sqlite Sqlite;
typedef struct Statement Statement;

typedef struct Threadset Threadset;
typedef struct Thread Thread;

/* Total number of errors in this process so far. */
static int nGlobalErr = 0;

struct Error {
  int rc;
  int iLine;
  char *zErr;
};

struct Sqlite {
  sqlite3 *db;                    /* Database handle */
  Statement *pCache;              /* Linked list of cached statements */
  int nText;                      /* Size of array at aText[] */
  char **aText;                   /* Stored text results */
};

struct Statement {
  sqlite3_stmt *pStmt;            /* Pre-compiled statement handle */
  Statement *pNext;               /* Next statement in linked-list */
};

struct Thread {
  int iTid;                       /* Thread number within test */
  void* pArg;                     /* Pointer argument passed by caller */

  pthread_t tid;                  /* Thread id */
  char *(*xProc)(int, void*);     /* Thread main proc */
  Thread *pNext;                  /* Next in this list of threads */
};

struct Threadset {
  int iMaxTid;                    /* Largest iTid value allocated so far */
  Thread *pThread;                /* Linked list of threads */
};

static void free_err(Error *p){
  sqlite3_free(p->zErr);
  p->zErr = 0;
  p->rc = 0;
}

static void print_err(Error *p){
  if( p->rc!=SQLITE_OK ){
    int isWarn = 0;
    if( p->rc==SQLITE_SCHEMA ) isWarn = 1;
    if( sqlite3_strglob("* - no such table: *",p->zErr)==0 ) isWarn = 1;
    printf("%s: (%d) \"%s\" at line %d\n", isWarn ? "Warning" : "Error",
            p->rc, p->zErr, p->iLine);
    if( !isWarn ) nGlobalErr++;
    fflush(stdout);
  }
}

static void print_and_free_err(Error *p){
  print_err(p);
  free_err(p);
}

static void system_error(Error *pErr, int iSys){
  pErr->rc = iSys;
  pErr->zErr = (char *)sqlite3_malloc(512);
  strerror_r(iSys, pErr->zErr, 512);
  pErr->zErr[511] = '\0';
}

static void sqlite_error(
  Error *pErr, 
  Sqlite *pDb, 
  const char *zFunc
){
  pErr->rc = sqlite3_errcode(pDb->db);
  pErr->zErr = sqlite3_mprintf(
      "sqlite3_%s() - %s (%d)", zFunc, sqlite3_errmsg(pDb->db),
      sqlite3_extended_errcode(pDb->db)
  );
}

static void test_error_x(
  Error *pErr,
  char *zErr
){
  if( pErr->rc==SQLITE_OK ){
    pErr->rc = 1;
    pErr->zErr = zErr;
  }else{
    sqlite3_free(zErr);
  }
}

static void clear_error_x(
  Error *pErr,
  int rc
){
  if( pErr->rc==rc ){
    pErr->rc = SQLITE_OK;
    sqlite3_free(pErr->zErr);
    pErr->zErr = 0;
  }
}

static int busyhandler(void *pArg, int n){
  usleep(10*1000);
  return 1;
}

static void opendb_x(
  Error *pErr,                    /* IN/OUT: Error code */
  Sqlite *pDb,                    /* OUT: Database handle */
  const char *zFile,              /* Database file name */
  int bDelete                     /* True to delete db file before opening */
){
  if( pErr->rc==SQLITE_OK ){
    int rc;
    int flags = SQLITE_OPEN_CREATE | SQLITE_OPEN_READWRITE | SQLITE_OPEN_URI;
    if( bDelete ) unlink(zFile);
    rc = sqlite3_open_v2(zFile, &pDb->db, flags, 0);
    if( rc ){
      sqlite_error(pErr, pDb, "open");
      sqlite3_close(pDb->db);
      pDb->db = 0;
    }else{
      sqlite3_create_function(
          pDb->db, "md5sum", -1, SQLITE_UTF8, 0, 0, md5step, md5finalize
      );
      sqlite3_busy_handler(pDb->db, busyhandler, 0);
      sqlite3_exec(pDb->db, "PRAGMA synchronous=OFF", 0, 0, 0);
    }
  }
}

static void closedb_x(
  Error *pErr,                    /* IN/OUT: Error code */
  Sqlite *pDb                     /* OUT: Database handle */
){
  int rc;
  int i;
  Statement *pIter;
  Statement *pNext;
  for(pIter=pDb->pCache; pIter; pIter=pNext){
    pNext = pIter->pNext;
    sqlite3_finalize(pIter->pStmt);
    sqlite3_free(pIter);
  }
  for(i=0; i<pDb->nText; i++){
    sqlite3_free(pDb->aText[i]);
  }
  sqlite3_free(pDb->aText);
  rc = sqlite3_close(pDb->db);
  if( rc && pErr->rc==SQLITE_OK ){
    pErr->zErr = sqlite3_mprintf("%s", sqlite3_errmsg(pDb->db));
  }
  memset(pDb, 0, sizeof(Sqlite));
}

static void sql_script_x(
  Error *pErr,                    /* IN/OUT: Error code */
  Sqlite *pDb,                    /* Database handle */
  const char *zSql                /* SQL script to execute */
){
  if( pErr->rc==SQLITE_OK ){
    pErr->rc = sqlite3_exec(pDb->db, zSql, 0, 0, &pErr->zErr);
  }
}

static void sql_script_printf_x(
  Error *pErr,                    /* IN/OUT: Error code */
  Sqlite *pDb,                    /* Database handle */
  const char *zFormat,            /* SQL printf format string */
  ...                             /* Printf args */
){
  va_list ap;                     /* ... printf arguments */
  va_start(ap, zFormat);
  if( pErr->rc==SQLITE_OK ){
    char *zSql = sqlite3_vmprintf(zFormat, ap);
    pErr->rc = sqlite3_exec(pDb->db, zSql, 0, 0, &pErr->zErr);
    sqlite3_free(zSql);
  }
  va_end(ap);
}

static Statement *getSqlStatement(
  Error *pErr,                    /* IN/OUT: Error code */
  Sqlite *pDb,                    /* Database handle */
  const char *zSql                /* SQL statement */
){
  Statement *pRet;
  int rc;

  for(pRet=pDb->pCache; pRet; pRet=pRet->pNext){
    if( 0==strcmp(sqlite3_sql(pRet->pStmt), zSql) ){
      return pRet;
    }
  }

  pRet = sqlite3_malloc(sizeof(Statement));
  rc = sqlite3_prepare_v2(pDb->db, zSql, -1, &pRet->pStmt, 0);
  if( rc!=SQLITE_OK ){
    sqlite_error(pErr, pDb, "prepare_v2");
    return 0;
  }
  assert( 0==strcmp(sqlite3_sql(pRet->pStmt), zSql) );

  pRet->pNext = pDb->pCache;
  pDb->pCache = pRet;
  return pRet;
}

static sqlite3_stmt *getAndBindSqlStatement(
  Error *pErr,                    /* IN/OUT: Error code */
  Sqlite *pDb,                    /* Database handle */
  va_list ap                      /* SQL followed by parameters */
){
  Statement *pStatement;          /* The SQLite statement wrapper */
  sqlite3_stmt *pStmt;            /* The SQLite statement to return */
  int i;                          /* Used to iterate through parameters */

  pStatement = getSqlStatement(pErr, pDb, va_arg(ap, const char *));
  if( !pStatement ) return 0;
  pStmt = pStatement->pStmt;
  for(i=1; i<=sqlite3_bind_parameter_count(pStmt); i++){
    const char *zName = sqlite3_bind_parameter_name(pStmt, i);
    void * pArg = va_arg(ap, void*);

    switch( zName[1] ){
      case 'i':
        sqlite3_bind_int64(pStmt, i, *(i64 *)pArg);
        break;

      default:
        pErr->rc = 1;
        pErr->zErr = sqlite3_mprintf("Cannot discern type: \"%s\"", zName);
        pStmt = 0;
        break;
    }
  }

  return pStmt;
}

static i64 execsql_i64_x(
  Error *pErr,                    /* IN/OUT: Error code */
  Sqlite *pDb,                    /* Database handle */
  ...                             /* SQL and pointers to parameter values */
){
  i64 iRet = 0;
  if( pErr->rc==SQLITE_OK ){
    sqlite3_stmt *pStmt;          /* SQL statement to execute */
    va_list ap;                   /* ... arguments */
    va_start(ap, pDb);
    pStmt = getAndBindSqlStatement(pErr, pDb, ap);
    if( pStmt ){
      int first = 1;
      while( SQLITE_ROW==sqlite3_step(pStmt) ){
        if( first && sqlite3_column_count(pStmt)>0 ){
          iRet = sqlite3_column_int64(pStmt, 0);
        }
        first = 0;
      }
      if( SQLITE_OK!=sqlite3_reset(pStmt) ){
        sqlite_error(pErr, pDb, "reset");
      }
    }
    va_end(ap);
  }
  return iRet;
}

static char * execsql_text_x(
  Error *pErr,                    /* IN/OUT: Error code */
  Sqlite *pDb,                    /* Database handle */
  int iSlot,                      /* Db handle slot to store text in */
  ...                             /* SQL and pointers to parameter values */
){
  char *zRet = 0;

  if( iSlot>=pDb->nText ){
    int nByte = sizeof(char *)*(iSlot+1);
    pDb->aText = (char **)sqlite3_realloc(pDb->aText, nByte);
    memset(&pDb->aText[pDb->nText], 0, sizeof(char*)*(iSlot+1-pDb->nText));
    pDb->nText = iSlot+1;
  }

  if( pErr->rc==SQLITE_OK ){
    sqlite3_stmt *pStmt;          /* SQL statement to execute */
    va_list ap;                   /* ... arguments */
    va_start(ap, iSlot);
    pStmt = getAndBindSqlStatement(pErr, pDb, ap);
    if( pStmt ){
      int first = 1;
      while( SQLITE_ROW==sqlite3_step(pStmt) ){
        if( first && sqlite3_column_count(pStmt)>0 ){
          zRet = sqlite3_mprintf("%s", sqlite3_column_text(pStmt, 0));
          sqlite3_free(pDb->aText[iSlot]);
          pDb->aText[iSlot] = zRet;
        }
        first = 0;
      }
      if( SQLITE_OK!=sqlite3_reset(pStmt) ){
        sqlite_error(pErr, pDb, "reset");
      }
    }
    va_end(ap);
  }

  return zRet;
}

static void integrity_check_x(
  Error *pErr,                    /* IN/OUT: Error code */
  Sqlite *pDb                     /* Database handle */
){
  if( pErr->rc==SQLITE_OK ){
    Statement *pStatement;        /* Statement to execute */
    char *zErr = 0;               /* Integrity check error */

    pStatement = getSqlStatement(pErr, pDb, "PRAGMA integrity_check");
    if( pStatement ){
      sqlite3_stmt *pStmt = pStatement->pStmt;
      while( SQLITE_ROW==sqlite3_step(pStmt) ){
        const char *z = (const char*)sqlite3_column_text(pStmt, 0);
        if( strcmp(z, "ok") ){
          if( zErr==0 ){
            zErr = sqlite3_mprintf("%s", z);
          }else{
            zErr = sqlite3_mprintf("%z\n%s", zErr, z);
          }
        }
      }
      sqlite3_reset(pStmt);

      if( zErr ){
        pErr->zErr = zErr;
        pErr->rc = 1;
      }
    }
  }
}

static void *launch_thread_main(void *pArg){
  Thread *p = (Thread *)pArg;
  return (void *)p->xProc(p->iTid, p->pArg);
}

static void launch_thread_x(
  Error *pErr,                    /* IN/OUT: Error code */
  Threadset *pThreads,            /* Thread set */
  char *(*xProc)(int, void*),     /* Proc to run */
  void *pArg                      /* Argument passed to thread proc */
){
  if( pErr->rc==SQLITE_OK ){
    int iTid = ++pThreads->iMaxTid;
    Thread *p;
    int rc;

    p = (Thread *)sqlite3_malloc(sizeof(Thread));
    memset(p, 0, sizeof(Thread));
    p->iTid = iTid;
    p->pArg = pArg;
    p->xProc = xProc;

    rc = pthread_create(&p->tid, NULL, launch_thread_main, (void *)p);
    if( rc!=0 ){
      system_error(pErr, rc);
      sqlite3_free(p);
    }else{
      p->pNext = pThreads->pThread;
      pThreads->pThread = p;
    }
  }
}

static void join_all_threads_x(
  Error *pErr,                    /* IN/OUT: Error code */
  Threadset *pThreads             /* Thread set */
){
  Thread *p;
  Thread *pNext;
  for(p=pThreads->pThread; p; p=pNext){
    void *ret;
    pNext = p->pNext;
    int rc;
    rc = pthread_join(p->tid, &ret);
    if( rc!=0 ){
      if( pErr->rc==SQLITE_OK ) system_error(pErr, rc);
    }else{
      printf("Thread %d says: %s\n", p->iTid, (ret==0 ? "..." : (char *)ret));
      fflush(stdout);
    }
    sqlite3_free(p);
  }
  pThreads->pThread = 0;
}

static i64 filesize_x(
  Error *pErr,
  const char *zFile
){
  i64 iRet = 0;
  if( pErr->rc==SQLITE_OK ){
    struct stat sStat;
    if( stat(zFile, &sStat) ){
      iRet = -1;
    }else{
      iRet = sStat.st_size;
    }
  }
  return iRet;
}

static void filecopy_x(
  Error *pErr,
  const char *zFrom,
  const char *zTo
){
  if( pErr->rc==SQLITE_OK ){
    i64 nByte = filesize_x(pErr, zFrom);
    if( nByte<0 ){
      test_error_x(pErr, sqlite3_mprintf("no such file: %s", zFrom));
    }else{
      i64 iOff;
      char aBuf[1024];
      int fd1;
      int fd2;
      unlink(zTo);

      fd1 = open(zFrom, O_RDONLY);
      if( fd1<0 ){
        system_error(pErr, errno);
        return;
      }
      fd2 = open(zTo, O_RDWR|O_CREAT|O_EXCL, 0644);
      if( fd2<0 ){
        system_error(pErr, errno);
        close(fd1);
        return;
      }

      iOff = 0;
      while( iOff<nByte ){
        int nCopy = sizeof(aBuf);
        if( nCopy+iOff>nByte ){
          nCopy = nByte - iOff;
        }
        if( nCopy!=read(fd1, aBuf, nCopy) ){
          system_error(pErr, errno);
          break;
        }
        if( nCopy!=write(fd2, aBuf, nCopy) ){
          system_error(pErr, errno);
          break;
        }
        iOff += nCopy;
      }

      close(fd1);
      close(fd2);
    }
  }
}

/* 
** Used by setstoptime() and timetostop().
*/
static double timelimit = 0.0;

static double currentTime(void){
  double t;
  static sqlite3_vfs *pTimelimitVfs = 0;
  if( pTimelimitVfs==0 ) pTimelimitVfs = sqlite3_vfs_find(0);
  if( pTimelimitVfs->iVersion>=2 && pTimelimitVfs->xCurrentTimeInt64!=0 ){
    sqlite3_int64 tm;
    pTimelimitVfs->xCurrentTimeInt64(pTimelimitVfs, &tm);
    t = tm/86400000.0;
  }else{
    pTimelimitVfs->xCurrentTime(pTimelimitVfs, &t);
  }
  return t;
}

static void setstoptime_x(
  Error *pErr,                    /* IN/OUT: Error code */
  int nMs                         /* Milliseconds until "stop time" */
){
  if( pErr->rc==SQLITE_OK ){
    double t = currentTime();
    timelimit = t + ((double)nMs)/(1000.0*60.0*60.0*24.0);
  }
}

static int timetostop_x(
  Error *pErr                     /* IN/OUT: Error code */
){
  int ret = 1;
  if( pErr->rc==SQLITE_OK ){
    double t = currentTime();
    ret = (t >= timelimit);
  }
  return ret;
}


/*
** 2016-12-28
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
** This file implements "key-value" performance test for SQLite.  The
** purpose is to compare the speed of SQLite for accessing large BLOBs
** versus reading those same BLOB values out of individual files in the
** filesystem.
**
** Run "kvtest" with no arguments for on-line help, or see comments below.
**
** HOW TO COMPILE:
**
** (1) Gather this source file and a recent SQLite3 amalgamation with its
**     header into the working directory.  You should have:
**
**          kvtest.c       >--- this file
**          sqlite3.c      \___ SQLite
**          sqlite3.h      /    amlagamation & header
**
** (2) Run you compiler against the two C source code files.
**
**    (a) On linux or mac:
**
**        OPTS="-DSQLITE_THREADSAFE=0 -DSQLITE_OMIT_LOAD_EXTENSION"
**        gcc -Os -I. $OPTS kvtest.c sqlite3.c -o kvtest
**
**             The $OPTS options can be omitted.  The $OPTS merely omit
**             the need to link against -ldl and -lpthread, or whatever
**             the equivalent libraries are called on your system.
**
**    (b) Windows with MSVC:
**
**        cl -I. kvtest.c sqlite3.c
**
** USAGE:
**
** (1) Create a test database by running "kvtest init" with appropriate
**     options.  See the help message for available options.
**
** (2) Construct the corresponding pile-of-files database on disk using
**     the "kvtest export" command.
**
** (3) Run tests using "kvtest run" against either the SQLite database or
**     the pile-of-files database and with appropriate options.
**
** For example:
**
**       ./kvtest init x1.db --count 100000 --size 10000
**       mkdir x1
**       ./kvtest export x1.db x1
**       ./kvtest run x1.db --count 10000 --max-id 1000000
**       ./kvtest run x1 --count 10000 --max-id 1000000
*/
static const char zHelp[] = 
"Usage: kvtest COMMAND ARGS...\n"
"\n"
"   kvtest init DBFILE --count N --size M --pagesize X\n"
"\n"
"        Generate a new test database file named DBFILE containing N\n"
"        BLOBs each of size M bytes.  The page size of the new database\n"
"        file will be X.  Additional options:\n"
"\n"
"           --variance V           Randomly vary M by plus or minus V\n"
"\n"
"   kvtest export DBFILE DIRECTORY\n"
"\n"
"        Export all the blobs in the kv table of DBFILE into separate\n"
"        files in DIRECTORY.\n"
"\n"
"   kvtest stat DBFILE\n"
"\n"
"        Display summary information about DBFILE\n"
"\n"
"   kvtest run DBFILE [options]\n"
"\n"
"        Run a performance test.  DBFILE can be either the name of a\n"
"        database or a directory containing sample files.  Options:\n"
"\n"
"           --asc                  Read blobs in ascending order\n"
"           --blob-api             Use the BLOB API\n"
"           --cache-size N         Database cache size\n"
"           --count N              Read N blobs\n"
"           --desc                 Read blobs in descending order\n"
"           --max-id N             Maximum blob key to use\n"
"           --mmap N               Mmap as much as N bytes of DBFILE\n"
"           --jmode MODE           Set MODE journal mode prior to starting\n"
"           --random               Read blobs in a random order\n"
"           --start N              Start reading with this blob key\n"
"           --stats                Output operating stats before exiting\n"
;

/* Reference resources used */
#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <assert.h>
#include <string.h>
#include "sqlite3.h"

#ifndef _WIN32
# include <unistd.h>
#else
  /* Provide Windows equivalent for the needed parts of unistd.h */
# include <io.h>
# define R_OK 2
# define S_ISREG(m) (((m) & S_IFMT) == S_IFREG)
# define S_ISDIR(m) (((m) & S_IFMT) == S_IFDIR)
# define access _access
#endif


/*
** Show thqe help text and quit.
*/
static void showHelp(void){
  fprintf(stdout, "%s", zHelp);
  exit(1);
}

/*
** Show an error message an quit.
*/
static void fatalError(const char *zFormat, ...){
  va_list ap;
  fprintf(stdout, "ERROR: ");
  va_start(ap, zFormat);
  vfprintf(stdout, zFormat, ap);
  va_end(ap);
  fprintf(stdout, "\n");
  exit(1);
}

/*
** Return the value of a hexadecimal digit.  Return -1 if the input
** is not a hex digit.
*/
static int hexDigitValue(char c){
  if( c>='0' && c<='9' ) return c - '0';
  if( c>='a' && c<='f' ) return c - 'a' + 10;
  if( c>='A' && c<='F' ) return c - 'A' + 10;
  return -1;
}

/*
** Interpret zArg as an integer value, possibly with suffixes.
*/
static int integerValue(const char *zArg){
  int v = 0;
  static const struct { char *zSuffix; int iMult; } aMult[] = {
    { "KiB", 1024 },
    { "MiB", 1024*1024 },
    { "GiB", 1024*1024*1024 },
    { "KB",  1000 },
    { "MB",  1000000 },
    { "GB",  1000000000 },
    { "K",   1000 },
    { "M",   1000000 },
    { "G",   1000000000 },
  };
  int i;
  int isNeg = 0;
  if( zArg[0]=='-' ){
    isNeg = 1;
    zArg++;
  }else if( zArg[0]=='+' ){
    zArg++;
  }
  if( zArg[0]=='0' && zArg[1]=='x' ){
    int x;
    zArg += 2;
    while( (x = hexDigitValue(zArg[0]))>=0 ){
      v = (v<<4) + x;
      zArg++;
    }
  }else{
    while( zArg[0]>='0' && zArg[0]<='9' ){
      v = v*10 + zArg[0] - '0';
      zArg++;
    }
  }
  for(i=0; i<sizeof(aMult)/sizeof(aMult[0]); i++){
    if( sqlite3_stricmp(aMult[i].zSuffix, zArg)==0 ){
      v *= aMult[i].iMult;
      break;
    }
  }
  return isNeg? -v : v;
}


/*
** Check the filesystem object zPath.  Determine what it is:
**
**    PATH_DIR     A directory
**    PATH_DB      An SQLite database
**    PATH_NEXIST  Does not exist
**    PATH_OTHER   Something else
*/
#define PATH_DIR     1
#define PATH_DB      2
#define PATH_NEXIST  0
#define PATH_OTHER   99
static int pathType(const char *zPath){
  struct stat x;
  int rc;
  if( access(zPath,R_OK) ) return PATH_NEXIST;
  memset(&x, 0, sizeof(x));
  rc = stat(zPath, &x);
  if( rc<0 ) return PATH_OTHER;
  if( S_ISDIR(x.st_mode) ) return PATH_DIR;
  if( (x.st_size%512)==0 ) return PATH_DB;
  return PATH_OTHER;
}

/*
** Return the size of a file in bytes.  Or return -1 if the
** named object is not a regular file or does not exist.
*/
static sqlite3_int64 fileSize(const char *zPath){
  struct stat x;
  int rc;
  memset(&x, 0, sizeof(x));
  rc = stat(zPath, &x);
  if( rc<0 ) return -1;
  if( !S_ISREG(x.st_mode) ) return -1;
  return x.st_size;
}

/*
** A Pseudo-random number generator with a fixed seed.  Use this so
** that the same sequence of "random" numbers are generated on each
** run, for repeatability.
*/
static unsigned int randInt(void){
  static unsigned int x = 0x333a13cd;
  static unsigned int y = 0xecb2adea;
  x = (x>>1) ^ ((1+~(x&1)) & 0xd0000001);
  y = y*1103515245 + 12345;
  return x^y;
}

/*
** Do database initialization.
*/
static int initMain(int argc, char **argv){
  char *zDb;
  int i, rc;
  int nCount = 1000;
  int sz = 10000;
  int iVariance = 0;
  int pgsz = 4096;
  sqlite3 *db;
  char *zSql;
  char *zErrMsg = 0;

  assert( strcmp(argv[1],"init")==0 );
  assert( argc>=3 );
  zDb = argv[2];
  for(i=3; i<argc; i++){
    char *z = argv[i];
    if( z[0]!='-' ) fatalError("unknown argument: \"%s\"", z);
    if( z[1]=='-' ) z++;
    if( strcmp(z, "-count")==0 ){
      if( i==argc-1 ) fatalError("missing argument on \"%s\"", argv[i]);
      nCount = integerValue(argv[++i]);
      if( nCount<1 ) fatalError("the --count must be positive");
      continue;
    }
    if( strcmp(z, "-size")==0 ){
      if( i==argc-1 ) fatalError("missing argument on \"%s\"", argv[i]);
      sz = integerValue(argv[++i]);
      if( sz<1 ) fatalError("the --size must be positive");
      continue;
    }
    if( strcmp(z, "-variance")==0 ){
      if( i==argc-1 ) fatalError("missing argument on \"%s\"", argv[i]);
      iVariance = integerValue(argv[++i]);
      continue;
    }
    if( strcmp(z, "-pagesize")==0 ){
      if( i==argc-1 ) fatalError("missing argument on \"%s\"", argv[i]);
      pgsz = integerValue(argv[++i]);
      if( pgsz<512 || pgsz>65536 || ((pgsz-1)&pgsz)!=0 ){
        fatalError("the --pagesize must be power of 2 between 512 and 65536");
      }
      continue;
    }
    fatalError("unknown option: \"%s\"", argv[i]);
  }
  rc = sqlite3_open(zDb, &db);
  if( rc ){
    fatalError("cannot open database \"%s\": %s", zDb, sqlite3_errmsg(db));
  }
  zSql = sqlite3_mprintf(
    "DROP TABLE IF EXISTS kv;\n"
    "PRAGMA page_size=%d;\n"
    "VACUUM;\n"
    "BEGIN;\n"
    "CREATE TABLE kv(k INTEGER PRIMARY KEY, v BLOB);\n"
    "WITH RECURSIVE c(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM c WHERE x<%d)"
    " INSERT INTO kv(k,v) SELECT x, randomblob(%d+(random()%%(%d))) FROM c;\n"
    "COMMIT;\n",
    pgsz, nCount, sz, iVariance+1
  );
  rc = sqlite3_exec(db, zSql, 0, 0, &zErrMsg);
  if( rc ) fatalError("database create failed: %s", zErrMsg);
  sqlite3_free(zSql);
  sqlite3_close(db);
  return 0;
}

/*
** Analyze an existing database file.  Report its content.
*/
static int statMain(int argc, char **argv){
  char *zDb;
  int i, rc;
  sqlite3 *db;
  char *zSql;
  sqlite3_stmt *pStmt;

  assert( strcmp(argv[1],"stat")==0 );
  assert( argc>=3 );
  zDb = argv[2];
  for(i=3; i<argc; i++){
    char *z = argv[i];
    if( z[0]!='-' ) fatalError("unknown argument: \"%s\"", z);
    if( z[1]=='-' ) z++;
    fatalError("unknown option: \"%s\"", argv[i]);
  }
  rc = sqlite3_open(zDb, &db);
  if( rc ){
    fatalError("cannot open database \"%s\": %s", zDb, sqlite3_errmsg(db));
  }
  zSql = sqlite3_mprintf(
    "SELECT count(*), min(length(v)), max(length(v)), avg(length(v))"
    "  FROM kv"
  );
  rc = sqlite3_prepare_v2(db, zSql, -1, &pStmt, 0);
  if( rc ) fatalError("cannot prepare SQL [%s]: %s", zSql, sqlite3_errmsg(db));
  sqlite3_free(zSql);
  if( sqlite3_step(pStmt)==SQLITE_ROW ){
    printf("Number of entries:  %8d\n", sqlite3_column_int(pStmt, 0));
    printf("Average value size: %8d\n", sqlite3_column_int(pStmt, 3));
    printf("Minimum value size: %8d\n", sqlite3_column_int(pStmt, 1));
    printf("Maximum value size: %8d\n", sqlite3_column_int(pStmt, 2));
  }else{
    printf("No rows\n");
  }
  sqlite3_finalize(pStmt);
  zSql = sqlite3_mprintf("PRAGMA page_size");
  rc = sqlite3_prepare_v2(db, zSql, -1, &pStmt, 0);
  if( rc ) fatalError("cannot prepare SQL [%s]: %s", zSql, sqlite3_errmsg(db));
  sqlite3_free(zSql);
  if( sqlite3_step(pStmt)==SQLITE_ROW ){
    printf("Page-size:          %8d\n", sqlite3_column_int(pStmt, 0));
  }
  sqlite3_finalize(pStmt);
  zSql = sqlite3_mprintf("PRAGMA page_count");
  rc = sqlite3_prepare_v2(db, zSql, -1, &pStmt, 0);
  if( rc ) fatalError("cannot prepare SQL [%s]: %s", zSql, sqlite3_errmsg(db));
  sqlite3_free(zSql);
  if( sqlite3_step(pStmt)==SQLITE_ROW ){
    printf("Page-count:         %8d\n", sqlite3_column_int(pStmt, 0));
  }
  sqlite3_finalize(pStmt);
  sqlite3_close(db);
  return 0;
}

/*
** Implementation of the "writefile(X,Y)" SQL function.  The argument Y
** is written into file X.  The number of bytes written is returned.  Or
** NULL is returned if something goes wrong, such as being unable to open
** file X for writing.
*/
static void writefileFunc(
  sqlite3_context *context,
  int argc,
  sqlite3_value **argv
){
  FILE *out;
  const char *z;
  sqlite3_int64 rc;
  const char *zFile;

  zFile = (const char*)sqlite3_value_text(argv[0]);
  if( zFile==0 ) return;
  out = fopen(zFile, "wb");
  if( out==0 ) return;
  z = (const char*)sqlite3_value_blob(argv[1]);
  if( z==0 ){
    rc = 0;
  }else{
    rc = fwrite(z, 1, sqlite3_value_bytes(argv[1]), out);
  }
  fclose(out);
  printf("\r%s   ", zFile); fflush(stdout);
  sqlite3_result_int64(context, rc);
}

/*
** Export the kv table to individual files in the filesystem
*/
static int exportMain(int argc, char **argv){
  char *zDb;
  char *zDir;
  sqlite3 *db;
  char *zSql;
  int rc;
  char *zErrMsg = 0;

  assert( strcmp(argv[1],"export")==0 );
  assert( argc>=3 );
  zDb = argv[2];
  if( argc!=4 ) fatalError("Usage: kvtest export DATABASE DIRECTORY");
  zDir = argv[3];
  if( pathType(zDir)!=PATH_DIR ){
    fatalError("object \"%s\" is not a directory", zDir);
  }
  rc = sqlite3_open(zDb, &db);
  if( rc ){
    fatalError("cannot open database \"%s\": %s", zDb, sqlite3_errmsg(db));
  }
  sqlite3_create_function(db, "writefile", 2, SQLITE_UTF8, 0,
                          writefileFunc, 0, 0);
  zSql = sqlite3_mprintf(
    "SELECT writefile(printf('%s/%%06d',k),v) FROM kv;",
    zDir
  );
  rc = sqlite3_exec(db, zSql, 0, 0, &zErrMsg);
  if( rc ) fatalError("database create failed: %s", zErrMsg);
  sqlite3_free(zSql);
  sqlite3_close(db);
  printf("\n");
  return 0;
}

/*
** Read the content of file zName into memory obtained from sqlite3_malloc64()
** and return a pointer to the buffer. The caller is responsible for freeing 
** the memory. 
**
** If parameter pnByte is not NULL, (*pnByte) is set to the number of bytes
** read.
**
** For convenience, a nul-terminator byte is always appended to the data read
** from the file before the buffer is returned. This byte is not included in
** the final value of (*pnByte), if applicable.
**
** NULL is returned if any error is encountered. The final value of *pnByte
** is undefined in this case.
*/
static unsigned char *readFile(const char *zName, int *pnByte){
  FILE *in;               /* FILE from which to read content of zName */
  sqlite3_int64 nIn;      /* Size of zName in bytes */
  size_t nRead;           /* Number of bytes actually read */
  unsigned char *pBuf;    /* Content read from disk */

  nIn = fileSize(zName);
  if( nIn<0 ) return 0;
  in = fopen(zName, "rb");
  if( in==0 ) return 0;
  pBuf = sqlite3_malloc64( nIn );
  if( pBuf==0 ) return 0;
  nRead = fread(pBuf, (size_t)nIn, 1, in);
  fclose(in);
  if( nRead!=1 ){
    sqlite3_free(pBuf);
    return 0;
  }
  if( pnByte ) *pnByte = (int)nIn;
  return pBuf;
}

/*
** Return the current time in milliseconds since the beginning of
** the Julian epoch.
*/
static sqlite3_int64 timeOfDay(void){
  static sqlite3_vfs *clockVfs = 0;
  sqlite3_int64 t;
  if( clockVfs==0 ) clockVfs = sqlite3_vfs_find(0);
  if( clockVfs->iVersion>=2 && clockVfs->xCurrentTimeInt64!=0 ){
    clockVfs->xCurrentTimeInt64(clockVfs, &t);
  }else{
    double r;
    clockVfs->xCurrentTime(clockVfs, &r);
    t = (sqlite3_int64)(r*86400000.0);
  }
  return t;
}

#ifdef __linux__
/*
** Attempt to display I/O stats on Linux using /proc/PID/io
*/
static void displayLinuxIoStats(FILE *out){
  FILE *in;
  char z[200];
  sqlite3_snprintf(sizeof(z), z, "/proc/%d/io", getpid());
  in = fopen(z, "rb");
  if( in==0 ) return;
  while( fgets(z, sizeof(z), in)!=0 ){
    static const struct {
      const char *zPattern;
      const char *zDesc;
    } aTrans[] = {
      { "rchar: ",                  "Bytes received by read():" },
      { "wchar: ",                  "Bytes sent to write():"    },
      { "syscr: ",                  "Read() system calls:"      },
      { "syscw: ",                  "Write() system calls:"     },
      { "read_bytes: ",             "Bytes read from storage:"  },
      { "write_bytes: ",            "Bytes written to storage:" },
      { "cancelled_write_bytes: ",  "Cancelled write bytes:"    },
    };
    int i;
    for(i=0; i<sizeof(aTrans)/sizeof(aTrans[0]); i++){
      int n = (int)strlen(aTrans[i].zPattern);
      if( strncmp(aTrans[i].zPattern, z, n)==0 ){
        fprintf(out, "%-36s %s", aTrans[i].zDesc, &z[n]);
        break;
      }
    }
  }
  fclose(in);
}
#endif

/*
** Display memory stats.
*/
static int display_stats(
  sqlite3 *db,                    /* Database to query */
  int bReset                      /* True to reset SQLite stats */
){
  int iCur;
  int iHiwtr;
  FILE *out = stdout;

  fprintf(out, "\n");

  iHiwtr = iCur = -1;
  sqlite3_status(SQLITE_STATUS_MEMORY_USED, &iCur, &iHiwtr, bReset);
  fprintf(out,
          "Memory Used:                         %d (max %d) bytes\n",
          iCur, iHiwtr);
  iHiwtr = iCur = -1;
  sqlite3_status(SQLITE_STATUS_MALLOC_COUNT, &iCur, &iHiwtr, bReset);
  fprintf(out, "Number of Outstanding Allocations:   %d (max %d)\n",
          iCur, iHiwtr);
  iHiwtr = iCur = -1;
  sqlite3_status(SQLITE_STATUS_PAGECACHE_USED, &iCur, &iHiwtr, bReset);
  fprintf(out,
      "Number of Pcache Pages Used:         %d (max %d) pages\n",
      iCur, iHiwtr);
  iHiwtr = iCur = -1;
  sqlite3_status(SQLITE_STATUS_PAGECACHE_OVERFLOW, &iCur, &iHiwtr, bReset);
  fprintf(out,
          "Number of Pcache Overflow Bytes:     %d (max %d) bytes\n",
          iCur, iHiwtr);
  iHiwtr = iCur = -1;
  sqlite3_status(SQLITE_STATUS_SCRATCH_USED, &iCur, &iHiwtr, bReset);
  fprintf(out,
      "Number of Scratch Allocations Used:  %d (max %d)\n",
      iCur, iHiwtr);
  iHiwtr = iCur = -1;
  sqlite3_status(SQLITE_STATUS_SCRATCH_OVERFLOW, &iCur, &iHiwtr, bReset);
  fprintf(out,
          "Number of Scratch Overflow Bytes:    %d (max %d) bytes\n",
          iCur, iHiwtr);
  iHiwtr = iCur = -1;
  sqlite3_status(SQLITE_STATUS_MALLOC_SIZE, &iCur, &iHiwtr, bReset);
  fprintf(out, "Largest Allocation:                  %d bytes\n",
          iHiwtr);
  iHiwtr = iCur = -1;
  sqlite3_status(SQLITE_STATUS_PAGECACHE_SIZE, &iCur, &iHiwtr, bReset);
  fprintf(out, "Largest Pcache Allocation:           %d bytes\n",
          iHiwtr);
  iHiwtr = iCur = -1;
  sqlite3_status(SQLITE_STATUS_SCRATCH_SIZE, &iCur, &iHiwtr, bReset);
  fprintf(out, "Largest Scratch Allocation:          %d bytes\n",
          iHiwtr);

  iHiwtr = iCur = -1;
  sqlite3_db_status(db, SQLITE_DBSTATUS_CACHE_USED, &iCur, &iHiwtr, bReset);
  fprintf(out, "Pager Heap Usage:                    %d bytes\n",
      iCur);
  iHiwtr = iCur = -1;
  sqlite3_db_status(db, SQLITE_DBSTATUS_CACHE_HIT, &iCur, &iHiwtr, 1);
  fprintf(out, "Page cache hits:                     %d\n", iCur);
  iHiwtr = iCur = -1;
  sqlite3_db_status(db, SQLITE_DBSTATUS_CACHE_MISS, &iCur, &iHiwtr, 1);
  fprintf(out, "Page cache misses:                   %d\n", iCur);
  iHiwtr = iCur = -1;
  sqlite3_db_status(db, SQLITE_DBSTATUS_CACHE_WRITE, &iCur, &iHiwtr, 1);
  fprintf(out, "Page cache writes:                   %d\n", iCur);
  iHiwtr = iCur = -1;

#ifdef __linux__
  displayLinuxIoStats(out);
#endif

  return 0;
}

/* Blob access order */
#define ORDER_ASC     1
#define ORDER_DESC    2
#define ORDER_RANDOM  3


/*
** Run a performance test
*/
static int runMain(int argc, char **argv){
  int eType;                  /* Is zDb a database or a directory? */
  char *zDb;                  /* Database or directory name */
  int i;                      /* Loop counter */
  int rc;                     /* Return code from SQLite calls */
  int nCount = 1000;          /* Number of blob fetch operations */
  int nExtra = 0;             /* Extra cycles */
  int iKey = 1;               /* Next blob key */
  int iMax = 0;               /* Largest allowed key */
  int iPagesize = 0;          /* Database page size */
  int iCache = 1000;          /* Database cache size in kibibytes */
  int bBlobApi = 0;           /* Use the incremental blob I/O API */
  int bStats = 0;             /* Print stats before exiting */
  int eOrder = ORDER_ASC;     /* Access order */
  sqlite3 *db = 0;            /* Database connection */
  sqlite3_stmt *pStmt = 0;    /* Prepared statement for SQL access */
  sqlite3_blob *pBlob = 0;    /* Handle for incremental Blob I/O */
  sqlite3_int64 tmStart;      /* Start time */
  sqlite3_int64 tmElapsed;    /* Elapsed time */
  int mmapSize = 0;           /* --mmap N argument */
  int nData = 0;              /* Bytes of data */
  sqlite3_int64 nTotal = 0;   /* Total data read */
  unsigned char *pData = 0;   /* Content of the blob */
  int nAlloc = 0;             /* Space allocated for pData[] */
  const char *zJMode = 0;     /* Journal mode */
  

  assert( strcmp(argv[1],"run")==0 );
  assert( argc>=3 );
  zDb = argv[2];
  eType = pathType(zDb);
  if( eType==PATH_OTHER ) fatalError("unknown object type: \"%s\"", zDb);
  if( eType==PATH_NEXIST ) fatalError("object does not exist: \"%s\"", zDb);
  for(i=3; i<argc; i++){
    char *z = argv[i];
    if( z[0]!='-' ) fatalError("unknown argument: \"%s\"", z);
    if( z[1]=='-' ) z++;
    if( strcmp(z, "-count")==0 ){
      if( i==argc-1 ) fatalError("missing argument on \"%s\"", argv[i]);
      nCount = integerValue(argv[++i]);
      if( nCount<1 ) fatalError("the --count must be positive");
      continue;
    }
    if( strcmp(z, "-mmap")==0 ){
      if( i==argc-1 ) fatalError("missing argument on \"%s\"", argv[i]);
      mmapSize = integerValue(argv[++i]);
      if( nCount<0 ) fatalError("the --mmap must be non-negative");
      continue;
    }
    if( strcmp(z, "-max-id")==0 ){
      if( i==argc-1 ) fatalError("missing argument on \"%s\"", argv[i]);
      iMax = integerValue(argv[++i]);
      continue;
    }
    if( strcmp(z, "-start")==0 ){
      if( i==argc-1 ) fatalError("missing argument on \"%s\"", argv[i]);
      iKey = integerValue(argv[++i]);
      if( iKey<1 ) fatalError("the --start must be positive");
      continue;
    }
    if( strcmp(z, "-cache-size")==0 ){
      if( i==argc-1 ) fatalError("missing argument on \"%s\"", argv[i]);
      iCache = integerValue(argv[++i]);
      continue;
    }
    if( strcmp(z, "-jmode")==0 ){
      if( i==argc-1 ) fatalError("missing argument on \"%s\"", argv[i]);
      zJMode = argv[++i];
      continue;
    }
    if( strcmp(z, "-random")==0 ){
      eOrder = ORDER_RANDOM;
      continue;
    }
    if( strcmp(z, "-asc")==0 ){
      eOrder = ORDER_ASC;
      continue;
    }
    if( strcmp(z, "-desc")==0 ){
      eOrder = ORDER_DESC;
      continue;
    }
    if( strcmp(z, "-blob-api")==0 ){
      bBlobApi = 1;
      continue;
    }
    if( strcmp(z, "-stats")==0 ){
      bStats = 1;
      continue;
    }
    fatalError("unknown option: \"%s\"", argv[i]);
  }
  tmStart = timeOfDay();
  if( eType==PATH_DB ){
    char *zSql;
    rc = sqlite3_open(zDb, &db);
    if( rc ){
      fatalError("cannot open database \"%s\": %s", zDb, sqlite3_errmsg(db));
    }
    zSql = sqlite3_mprintf("PRAGMA mmap_size=%d", mmapSize);
    sqlite3_exec(db, zSql, 0, 0, 0);
    zSql = sqlite3_mprintf("PRAGMA cache_size=%d", iCache);
    sqlite3_exec(db, zSql, 0, 0, 0);
    sqlite3_free(zSql);
    pStmt = 0;
    sqlite3_prepare_v2(db, "PRAGMA page_size", -1, &pStmt, 0);
    if( sqlite3_step(pStmt)==SQLITE_ROW ){
      iPagesize = sqlite3_column_int(pStmt, 0);
    }
    sqlite3_finalize(pStmt);
    sqlite3_prepare_v2(db, "PRAGMA cache_size", -1, &pStmt, 0);
    if( sqlite3_step(pStmt)==SQLITE_ROW ){
      iCache = sqlite3_column_int(pStmt, 0);
    }else{
      iCache = 0;
    }
    sqlite3_finalize(pStmt);
    pStmt = 0;
    if( zJMode ){
      zSql = sqlite3_mprintf("PRAGMA journal_mode=%Q", zJMode);
      sqlite3_exec(db, zSql, 0, 0, 0);
      sqlite3_free(zSql);
    }
    sqlite3_prepare_v2(db, "PRAGMA journal_mode", -1, &pStmt, 0);
    if( sqlite3_step(pStmt)==SQLITE_ROW ){
      zJMode = sqlite3_mprintf("%s", sqlite3_column_text(pStmt, 0));
    }else{
      zJMode = "???";
    }
    sqlite3_finalize(pStmt);
    if( iMax<=0 ){
      sqlite3_prepare_v2(db, "SELECT max(k) FROM kv", -1, &pStmt, 0);
      if( sqlite3_step(pStmt)==SQLITE_ROW ){
        iMax = sqlite3_column_int(pStmt, 0);
      }
      sqlite3_finalize(pStmt);
    }
    pStmt = 0;
    sqlite3_exec(db, "BEGIN", 0, 0, 0);
  }
  if( iMax<=0 ) iMax = 1000;
  for(i=0; i<nCount; i++){
    if( eType==PATH_DIR ){
      /* CASE 1: Reading blobs out of separate files */
      char *zKey;
      zKey = sqlite3_mprintf("%s/%06d", zDb, iKey);
      nData = 0;
      pData = readFile(zKey, &nData);
      sqlite3_free(zKey);
      sqlite3_free(pData);
    }else if( bBlobApi ){
      /* CASE 2: Reading from database using the incremental BLOB I/O API */
      if( pBlob==0 ){
        rc = sqlite3_blob_open(db, "main", "kv", "v", iKey, 0, &pBlob);
        if( rc ){
          fatalError("could not open sqlite3_blob handle: %s",
                     sqlite3_errmsg(db));
        }
      }else{
        rc = sqlite3_blob_reopen(pBlob, iKey);
      }
      if( rc==SQLITE_OK ){
        nData = sqlite3_blob_bytes(pBlob);
        if( nAlloc<nData+1 ){
          nAlloc = nData+100;
          pData = sqlite3_realloc(pData, nAlloc);
        }
        if( pData==0 ) fatalError("cannot allocate %d bytes", nData+1);
        rc = sqlite3_blob_read(pBlob, pData, nData, 0);
        if( rc!=SQLITE_OK ){
          fatalError("could not read the blob at %d: %s", iKey,
                     sqlite3_errmsg(db));
        }
      }
    }else{
      /* CASE 3: Reading from database using SQL */
      if( pStmt==0 ){
        rc = sqlite3_prepare_v2(db, 
               "SELECT v FROM kv WHERE k=?1", -1, &pStmt, 0);
        if( rc ){
          fatalError("cannot prepare query: %s", sqlite3_errmsg(db));
        }
      }else{
        sqlite3_reset(pStmt);
      }
      sqlite3_bind_int(pStmt, 1, iKey);
      rc = sqlite3_step(pStmt);
      if( rc==SQLITE_ROW ){
        nData = sqlite3_column_bytes(pStmt, 0);
        pData = (unsigned char*)sqlite3_column_blob(pStmt, 0);
      }else{
        nData = 0;
      }
    }
    if( eOrder==ORDER_ASC ){
      iKey++;
      if( iKey>iMax ) iKey = 1;
    }else if( eOrder==ORDER_DESC ){
      iKey--;
      if( iKey<=0 ) iKey = iMax;
    }else{
      iKey = (randInt()%iMax)+1;
    }
    nTotal += nData;
    if( nData==0 ){ nCount++; nExtra++; }
  }
  if( nAlloc ) sqlite3_free(pData);
  if( pStmt ) sqlite3_finalize(pStmt);
  if( pBlob ) sqlite3_blob_close(pBlob);
  if( bStats ){
    display_stats(db, 0);
  }
  if( db ) sqlite3_close(db);
  tmElapsed = timeOfDay() - tmStart;
  if( nExtra ){
    printf("%d cycles due to %d misses\n", nCount, nExtra);
  }
  if( eType==PATH_DB ){
    printf("SQLite version: %s\n", sqlite3_libversion());
  }
  printf("--count %d --max-id %d", nCount-nExtra, iMax);
  switch( eOrder ){
    case ORDER_RANDOM:  printf(" --random\n");  break;
    case ORDER_DESC:    printf(" --desc\n");    break;
    default:            printf(" --asc\n");     break;
  }
  if( eType==PATH_DB ){
    printf("--cache-size %d --jmode %s\n", iCache, zJMode);
    printf("--mmap %d%s\n", mmapSize, bBlobApi ? " --blob-api" : "");
  }
  if( iPagesize ) printf("Database page size: %d\n", iPagesize);
  printf("Total elapsed time: %.3f\n", tmElapsed/1000.0);
  printf("Microseconds per BLOB read: %.3f\n", tmElapsed*1000.0/nCount);
  printf("Content read rate: %.1f MB/s\n", nTotal/(1000.0*tmElapsed));
  return 0;
}


int main(int argc, char **argv){
  if( argc<3 ) showHelp();
  if( strcmp(argv[1],"init")==0 ){
    return initMain(argc, argv);
  }
  if( strcmp(argv[1],"export")==0 ){
    return exportMain(argc, argv);
  }
  if( strcmp(argv[1],"run")==0 ){
    return runMain(argc, argv);
  }
  if( strcmp(argv[1],"stat")==0 ){
    return statMain(argc, argv);
  }
  showHelp();
  return 0;
}

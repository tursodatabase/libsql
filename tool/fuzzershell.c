/*
** 2015-04-17
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
** This is a utility program designed to aid running the SQLite library
** against an external fuzzer, such as American Fuzzy Lop (AFL)
** (http://lcamtuf.coredump.cx/afl/).  Basically, this program reads
** SQL text from standard input and passes it through to SQLite for evaluation,
** just like the "sqlite3" command-line shell.  Differences from the
** command-line shell:
**
**    (1)  The complex "dot-command" extensions are omitted.  This
**         prevents the fuzzer from discovering that it can run things
**         like ".shell rm -rf ~"
**
**    (2)  The database is opened with the SQLITE_OPEN_MEMORY flag so that
**         no disk I/O from the database is permitted.  The ATTACH command
**         with a filename still uses an in-memory database.
**
**    (3)  The main in-memory database can be initialized from a template
**         disk database so that the fuzzer starts with a database containing
**         content.
**
**    (4)  The eval() SQL function is added, allowing the fuzzer to do 
**         interesting recursive operations.
**
**    (5)  An error is raised if there is a memory leak.
**
** The input text can be divided into separate test cases using comments
** of the form:
**
**       |****<...>****|
**
** where the "..." is arbitrary text, except the "|" should really be "/".
** ("|" is used here to avoid compiler errors about nested comments.)
** A separate in-memory SQLite database is created to run each test case.
** This feature allows the "queue" of AFL to be captured into a single big
** file using a command like this:
**
**    (for i in id:*; do echo '|****<'$i'>****|'; cat $i; done) >~/all-queue.txt
**
** (Once again, change the "|" to "/") Then all elements of the AFL queue
** can be run in a single go (for regression testing, for example) by typing:
**
**    fuzzershell -f ~/all-queue.txt
**
** After running each chunk of SQL, the database connection is closed.  The
** program aborts if the close fails or if there is any unfreed memory after
** the close.
**
** New test cases can be appended to all-queue.txt at any time.  If redundant
** test cases are added, they can be eliminated by running:
**
**    fuzzershell -f ~/all-queue.txt --unique-cases ~/unique-cases.txt
**
*/
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdarg.h>
#include <ctype.h>
#include "sqlite3.h"

/*
** All global variables are gathered into the "g" singleton.
*/
struct GlobalVars {
  const char *zArgv0;              /* Name of program */
  sqlite3_mem_methods sOrigMem;    /* Original memory methods */
  sqlite3_mem_methods sOomMem;     /* Memory methods with OOM simulator */
  int iOomCntdown;                 /* Memory fails on 1 to 0 transition */
  int nOomFault;                   /* Increments for each OOM fault */
  int bOomOnce;                    /* Fail just once if true */
  int bOomEnable;                  /* True to enable OOM simulation */
  int nOomBrkpt;                   /* Number of calls to oomFault() */
  char zTestName[100];             /* Name of current test */
} g;

/*
** This routine is called when a simulated OOM occurs.  It exists as a
** convenient place to set a debugger breakpoint.
*/
static void oomFault(void){
  g.nOomBrkpt++; /* Prevent oomFault() from being optimized out */
}


/* Versions of malloc() and realloc() that simulate OOM conditions */
static void *oomMalloc(int nByte){
  if( nByte>0 && g.bOomEnable && g.iOomCntdown>0 ){
    g.iOomCntdown--;
    if( g.iOomCntdown==0 ){
      if( g.nOomFault==0 ) oomFault();
      g.nOomFault++;
      if( !g.bOomOnce ) g.iOomCntdown = 1;
      return 0;
    }
  }
  return g.sOrigMem.xMalloc(nByte);
}
static void *oomRealloc(void *pOld, int nByte){
  if( nByte>0 && g.bOomEnable && g.iOomCntdown>0 ){
    g.iOomCntdown--;
    if( g.iOomCntdown==0 ){
      if( g.nOomFault==0 ) oomFault();
      g.nOomFault++;
      if( !g.bOomOnce ) g.iOomCntdown = 1;
      return 0;
    }
  }
  return g.sOrigMem.xRealloc(pOld, nByte);
}

/*
** Print an error message and abort in such a way to indicate to the
** fuzzer that this counts as a crash.
*/
static void abendError(const char *zFormat, ...){
  va_list ap;
  if( g.zTestName[0] ){
    fprintf(stderr, "%s (%s): ", g.zArgv0, g.zTestName);
  }else{
    fprintf(stderr, "%s: ", g.zArgv0);
  }
  va_start(ap, zFormat);
  vfprintf(stderr, zFormat, ap);
  va_end(ap);
  fprintf(stderr, "\n");
  abort();
}
/*
** Print an error message and quit, but not in a way that would look
** like a crash.
*/
static void fatalError(const char *zFormat, ...){
  va_list ap;
  if( g.zTestName[0] ){
    fprintf(stderr, "%s (%s): ", g.zArgv0, g.zTestName);
  }else{
    fprintf(stderr, "%s: ", g.zArgv0);
  }
  va_start(ap, zFormat);
  vfprintf(stderr, zFormat, ap);
  va_end(ap);
  fprintf(stderr, "\n");
  exit(1);
}

/*
** Evaluate some SQL.  Abort if unable.
*/
static void sqlexec(sqlite3 *db, const char *zFormat, ...){
  va_list ap;
  char *zSql;
  char *zErrMsg = 0;
  int rc;
  va_start(ap, zFormat);
  zSql = sqlite3_vmprintf(zFormat, ap);
  va_end(ap);
  rc = sqlite3_exec(db, zSql, 0, 0, &zErrMsg);
  if( rc ) abendError("failed sql [%s]: %s", zSql, zErrMsg);
  sqlite3_free(zSql);
}

/*
** This callback is invoked by sqlite3_log().
*/
static void shellLog(void *pNotUsed, int iErrCode, const char *zMsg){
  printf("LOG: (%d) %s\n", iErrCode, zMsg);
  fflush(stdout);
}
static void shellLogNoop(void *pNotUsed, int iErrCode, const char *zMsg){
  return;
}

/*
** This callback is invoked by sqlite3_exec() to return query results.
*/
static int execCallback(void *NotUsed, int argc, char **argv, char **colv){
  int i;
  static unsigned cnt = 0;
  printf("ROW #%u:\n", ++cnt);
  for(i=0; i<argc; i++){
    printf(" %s=", colv[i]);
    if( argv[i] ){
      printf("[%s]\n", argv[i]);
    }else{
      printf("NULL\n");
    }
  }
  fflush(stdout);
  return 0;
}
static int execNoop(void *NotUsed, int argc, char **argv, char **colv){
  return 0;
}

#ifndef SQLITE_OMIT_TRACE
/*
** This callback is invoked by sqlite3_trace() as each SQL statement
** starts.
*/
static void traceCallback(void *NotUsed, const char *zMsg){
  printf("TRACE: %s\n", zMsg);
  fflush(stdout);
}
static void traceNoop(void *NotUsed, const char *zMsg){
  return;
}
#endif

/***************************************************************************
** eval() implementation copied from ../ext/misc/eval.c
*/
/*
** Structure used to accumulate the output
*/
struct EvalResult {
  char *z;               /* Accumulated output */
  const char *zSep;      /* Separator */
  int szSep;             /* Size of the separator string */
  sqlite3_int64 nAlloc;  /* Number of bytes allocated for z[] */
  sqlite3_int64 nUsed;   /* Number of bytes of z[] actually used */
};

/*
** Callback from sqlite_exec() for the eval() function.
*/
static int callback(void *pCtx, int argc, char **argv, char **colnames){
  struct EvalResult *p = (struct EvalResult*)pCtx;
  int i; 
  for(i=0; i<argc; i++){
    const char *z = argv[i] ? argv[i] : "";
    size_t sz = strlen(z);
    if( (sqlite3_int64)sz+p->nUsed+p->szSep+1 > p->nAlloc ){
      char *zNew;
      p->nAlloc = p->nAlloc*2 + sz + p->szSep + 1;
      /* Using sqlite3_realloc64() would be better, but it is a recent
      ** addition and will cause a segfault if loaded by an older version
      ** of SQLite.  */
      zNew = p->nAlloc<=0x7fffffff ? sqlite3_realloc(p->z, (int)p->nAlloc) : 0;
      if( zNew==0 ){
        sqlite3_free(p->z);
        memset(p, 0, sizeof(*p));
        return 1;
      }
      p->z = zNew;
    }
    if( p->nUsed>0 ){
      memcpy(&p->z[p->nUsed], p->zSep, p->szSep);
      p->nUsed += p->szSep;
    }
    memcpy(&p->z[p->nUsed], z, sz);
    p->nUsed += sz;
  }
  return 0;
}

/*
** Implementation of the eval(X) and eval(X,Y) SQL functions.
**
** Evaluate the SQL text in X.  Return the results, using string
** Y as the separator.  If Y is omitted, use a single space character.
*/
static void sqlEvalFunc(
  sqlite3_context *context,
  int argc,
  sqlite3_value **argv
){
  const char *zSql;
  sqlite3 *db;
  char *zErr = 0;
  int rc;
  struct EvalResult x;

  memset(&x, 0, sizeof(x));
  x.zSep = " ";
  zSql = (const char*)sqlite3_value_text(argv[0]);
  if( zSql==0 ) return;
  if( argc>1 ){
    x.zSep = (const char*)sqlite3_value_text(argv[1]);
    if( x.zSep==0 ) return;
  }
  x.szSep = (int)strlen(x.zSep);
  db = sqlite3_context_db_handle(context);
  rc = sqlite3_exec(db, zSql, callback, &x, &zErr);
  if( rc!=SQLITE_OK ){
    sqlite3_result_error(context, zErr, -1);
    sqlite3_free(zErr);
  }else if( x.zSep==0 ){
    sqlite3_result_error_nomem(context);
    sqlite3_free(x.z);
  }else{
    sqlite3_result_text(context, x.z, (int)x.nUsed, sqlite3_free);
  }
}
/* End of the eval() implementation
******************************************************************************/

/*
** Print sketchy documentation for this utility program
*/
static void showHelp(void){
  printf("Usage: %s [options]\n", g.zArgv0);
  printf(
"Read SQL text from standard input and evaluate it.\n"
"Options:\n"
"  --autovacuum          Enable AUTOVACUUM mode\n"
"  -f FILE               Read SQL text from FILE instead of standard input\n"
"  --heap SZ MIN         Memory allocator uses SZ bytes & min allocation MIN\n"
"  --help                Show this help text\n"    
"  --initdb DBFILE       Initialize the in-memory database using template DBFILE\n"
"  --lookaside N SZ      Configure lookaside for N slots of SZ bytes each\n"
"  --oom                 Run each test multiple times in a simulated OOM loop\n"
"  --pagesize N          Set the page size to N\n"
"  --pcache N SZ         Configure N pages of pagecache each of size SZ bytes\n"
"  -q                    Reduced output\n"
"  --quiet               Reduced output\n"
"  --scratch N SZ        Configure scratch memory for N slots of SZ bytes each\n"
"  --unique-cases FILE   Write all unique test cases to FILE\n"
"  --utf16be             Set text encoding to UTF-16BE\n"
"  --utf16le             Set text encoding to UTF-16LE\n"
"  -v                    Increased output\n"
"  --verbose             Increased output\n"
  );
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
  sqlite3_int64 v = 0;
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
    while( isdigit(zArg[0]) ){
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
  if( v>0x7fffffff ) abendError("parameter too large - max 2147483648");
  return (int)(isNeg? -v : v);
}

/*
** Various operating modes
*/
#define FZMODE_Generic   1
#define FZMODE_Strftime  2
#define FZMODE_Printf    3
#define FZMODE_Glob      4


int main(int argc, char **argv){
  char *zIn = 0;          /* Input text */
  int nAlloc = 0;         /* Number of bytes allocated for zIn[] */
  int nIn = 0;            /* Number of bytes of zIn[] used */
  size_t got;             /* Bytes read from input */
  FILE *in = stdin;       /* Where to read SQL text from */
  int rc = SQLITE_OK;     /* Result codes from API functions */
  int i;                  /* Loop counter */
  int iNext;              /* Next block of SQL */
  sqlite3 *db;            /* Open database */
  sqlite3 *dbInit = 0;    /* On-disk database used to initialize the in-memory db */
  const char *zInitDb = 0;/* Name of the initialization database file */
  char *zErrMsg = 0;      /* Error message returned from sqlite3_exec() */
  const char *zEncoding = 0;    /* --utf16be or --utf16le */
  int nHeap = 0, mnHeap = 0;    /* Heap size from --heap */
  int nLook = 0, szLook = 0;    /* --lookaside configuration */
  int nPCache = 0, szPCache = 0;/* --pcache configuration */
  int nScratch = 0, szScratch=0;/* --scratch configuration */
  int pageSize = 0;             /* Desired page size.  0 means default */
  void *pHeap = 0;              /* Allocated heap space */
  void *pLook = 0;              /* Allocated lookaside space */
  void *pPCache = 0;            /* Allocated storage for pcache */
  void *pScratch = 0;           /* Allocated storage for scratch */
  int doAutovac = 0;            /* True for --autovacuum */
  char *zSql;                   /* SQL to run */
  char *zToFree = 0;            /* Call sqlite3_free() on this afte running zSql */
  int iMode = FZMODE_Generic;   /* Operating mode */
  const char *zCkGlob = 0;      /* Inputs must match this glob */
  int verboseFlag = 0;          /* --verbose or -v flag */
  int quietFlag = 0;            /* --quiet or -q flag */
  int nTest = 0;                /* Number of test cases run */
  int multiTest = 0;            /* True if there will be multiple test cases */
  int lastPct = -1;             /* Previous percentage done output */
  sqlite3 *dataDb = 0;          /* Database holding compacted input data */
  sqlite3_stmt *pStmt = 0;      /* Statement to insert testcase into dataDb */
  const char *zDataOut = 0;     /* Write compacted data to this output file */
  int nHeader = 0;              /* Bytes of header comment text on input file */
  int oomFlag = 0;              /* --oom */
  int oomCnt = 0;               /* Counter for the OOM loop */
  char zErrBuf[200];            /* Space for the error message */
  const char *zFailCode;        /* Value of the TEST_FAILURE environment var */


  zFailCode = getenv("TEST_FAILURE");
  g.zArgv0 = argv[0];
  for(i=1; i<argc; i++){
    const char *z = argv[i];
    if( z[0]=='-' ){
      z++;
      if( z[0]=='-' ) z++;
      if( strcmp(z,"autovacuum")==0 ){
        doAutovac = 1;
      }else
      if( strcmp(z, "f")==0 && i+1<argc ){
        if( in!=stdin ) abendError("only one -f allowed");
        in = fopen(argv[++i],"rb");
        if( in==0 )  abendError("cannot open input file \"%s\"", argv[i]);
      }else
      if( strcmp(z,"heap")==0 ){
        if( i>=argc-2 ) abendError("missing arguments on %s\n", argv[i]);
        nHeap = integerValue(argv[i+1]);
        mnHeap = integerValue(argv[i+2]);
        i += 2;
      }else
      if( strcmp(z,"help")==0 ){
        showHelp();
        return 0;
      }else
      if( strcmp(z, "initdb")==0 && i+1<argc ){
        if( zInitDb!=0 ) abendError("only one --initdb allowed");
        zInitDb = argv[++i];
      }else
      if( strcmp(z,"lookaside")==0 ){
        if( i>=argc-2 ) abendError("missing arguments on %s", argv[i]);
        nLook = integerValue(argv[i+1]);
        szLook = integerValue(argv[i+2]);
        i += 2;
      }else
      if( strcmp(z,"mode")==0 ){
        if( i>=argc-1 ) abendError("missing argument on %s", argv[i]);
        z = argv[++i];
        if( strcmp(z,"generic")==0 ){
          iMode = FZMODE_Printf;
          zCkGlob = 0;
        }else if( strcmp(z, "glob")==0 ){
          iMode = FZMODE_Glob;
          zCkGlob = "'*','*'";
        }else if( strcmp(z, "printf")==0 ){
          iMode = FZMODE_Printf;
          zCkGlob = "'*',*";
        }else if( strcmp(z, "strftime")==0 ){
          iMode = FZMODE_Strftime;
          zCkGlob = "'*',*";
        }else{
          abendError("unknown --mode: %s", z);
        }
      }else
      if( strcmp(z,"oom")==0 ){
        oomFlag = 1;
      }else
      if( strcmp(z,"pagesize")==0 ){
        if( i>=argc-1 ) abendError("missing argument on %s", argv[i]);
        pageSize = integerValue(argv[++i]);
      }else
      if( strcmp(z,"pcache")==0 ){
        if( i>=argc-2 ) abendError("missing arguments on %s", argv[i]);
        nPCache = integerValue(argv[i+1]);
        szPCache = integerValue(argv[i+2]);
        i += 2;
      }else
      if( strcmp(z,"quiet")==0 || strcmp(z,"q")==0 ){
        quietFlag = 1;
        verboseFlag = 0;
      }else
      if( strcmp(z,"scratch")==0 ){
        if( i>=argc-2 ) abendError("missing arguments on %s", argv[i]);
        nScratch = integerValue(argv[i+1]);
        szScratch = integerValue(argv[i+2]);
        i += 2;
      }else
      if( strcmp(z, "unique-cases")==0 ){
        if( i>=argc-1 ) abendError("missing arguments on %s", argv[i]);
        if( zDataOut ) abendError("only one --minimize allowed");
        zDataOut = argv[++i];
      }else
      if( strcmp(z,"utf16le")==0 ){
        zEncoding = "utf16le";
      }else
      if( strcmp(z,"utf16be")==0 ){
        zEncoding = "utf16be";
      }else
      if( strcmp(z,"verbose")==0 || strcmp(z,"v")==0 ){
        quietFlag = 0;
        verboseFlag = 1;
      }else
      {
        abendError("unknown option: %s", argv[i]);
      }
    }else{
      abendError("unknown argument: %s", argv[i]);
    }
  }
  sqlite3_config(SQLITE_CONFIG_LOG, verboseFlag ? shellLog : shellLogNoop, 0);
  if( nHeap>0 ){
    pHeap = malloc( nHeap );
    if( pHeap==0 ) fatalError("cannot allocate %d-byte heap\n", nHeap);
    rc = sqlite3_config(SQLITE_CONFIG_HEAP, pHeap, nHeap, mnHeap);
    if( rc ) abendError("heap configuration failed: %d\n", rc);
  }
  if( oomFlag ){
    sqlite3_config(SQLITE_CONFIG_GETMALLOC, &g.sOrigMem);
    g.sOomMem = g.sOrigMem;
    g.sOomMem.xMalloc = oomMalloc;
    g.sOomMem.xRealloc = oomRealloc;
    sqlite3_config(SQLITE_CONFIG_MALLOC, &g.sOomMem);
  }
  if( nLook>0 ){
    sqlite3_config(SQLITE_CONFIG_LOOKASIDE, 0, 0);
    if( szLook>0 ){
      pLook = malloc( nLook*szLook );
      if( pLook==0 ) fatalError("out of memory");
    }
  }
  if( nScratch>0 && szScratch>0 ){
    pScratch = malloc( nScratch*(sqlite3_int64)szScratch );
    if( pScratch==0 ) fatalError("cannot allocate %lld-byte scratch",
                                 nScratch*(sqlite3_int64)szScratch);
    rc = sqlite3_config(SQLITE_CONFIG_SCRATCH, pScratch, szScratch, nScratch);
    if( rc ) abendError("scratch configuration failed: %d\n", rc);
  }
  if( nPCache>0 && szPCache>0 ){
    pPCache = malloc( nPCache*(sqlite3_int64)szPCache );
    if( pPCache==0 ) fatalError("cannot allocate %lld-byte pcache",
                                 nPCache*(sqlite3_int64)szPCache);
    rc = sqlite3_config(SQLITE_CONFIG_PAGECACHE, pPCache, szPCache, nPCache);
    if( rc ) abendError("pcache configuration failed: %d", rc);
  }
  while( !feof(in) ){
    nAlloc += nAlloc+1000;
    zIn = realloc(zIn, nAlloc);
    if( zIn==0 ) fatalError("out of memory");
    got = fread(zIn+nIn, 1, nAlloc-nIn-1, in); 
    nIn += (int)got;
    zIn[nIn] = 0;
    if( got==0 ) break;
  }
  if( in!=stdin ) fclose(in);
  if( zDataOut ){
    rc = sqlite3_open(":memory:", &dataDb);
    if( rc ) abendError("cannot open :memory: database");
    rc = sqlite3_exec(dataDb,
          "CREATE TABLE testcase(sql BLOB PRIMARY KEY) WITHOUT ROWID;",0,0,0);
    if( rc ) abendError("%s", sqlite3_errmsg(dataDb));
    rc = sqlite3_prepare_v2(dataDb, "INSERT OR IGNORE INTO testcase(sql)VALUES(?1)",
                            -1, &pStmt, 0);
    if( rc ) abendError("%s", sqlite3_errmsg(dataDb));
  }
  if( zInitDb ){
    rc = sqlite3_open_v2(zInitDb, &dbInit, SQLITE_OPEN_READONLY, 0);
    if( rc!=SQLITE_OK ){
      abendError("unable to open initialization database \"%s\"", zInitDb);
    }
  }
  for(i=0; i<nIn; i=iNext+1){   /* Skip initial lines beginning with '#' */
    if( zIn[i]!='#' ) break;
    for(iNext=i+1; iNext<nIn && zIn[iNext]!='\n'; iNext++){}
  }
  nHeader = i;
  for(nTest=0; i<nIn; i=iNext, nTest++, g.zTestName[0]=0){
    char cSaved;
    if( strncmp(&zIn[i], "/****<",6)==0 ){
      char *z = strstr(&zIn[i], ">****/");
      if( z ){
        z += 6;
        sqlite3_snprintf(sizeof(g.zTestName), g.zTestName, "%.*s", 
                         (int)(z-&zIn[i]), &zIn[i]);
        if( verboseFlag ){
          printf("%.*s\n", (int)(z-&zIn[i]), &zIn[i]);
          fflush(stdout);
        }
        i += (int)(z-&zIn[i]);
        multiTest = 1;
      }
    }
    for(iNext=i; iNext<nIn && strncmp(&zIn[iNext],"/****<",6)!=0; iNext++){}
    if( zDataOut ){
      sqlite3_bind_blob(pStmt, 1, &zIn[i], iNext-i, SQLITE_STATIC);
      rc = sqlite3_step(pStmt);
      if( rc!=SQLITE_DONE ) abendError("%s", sqlite3_errmsg(dataDb));
      sqlite3_reset(pStmt);
      continue;
    }
    cSaved = zIn[iNext];
    zIn[iNext] = 0;
    if( zCkGlob && sqlite3_strglob(zCkGlob,&zIn[i])!=0 ){
      zIn[iNext] = cSaved;
      continue;
    }
    zSql = &zIn[i];
    if( verboseFlag ){
      printf("INPUT (offset: %d, size: %d): [%s]\n",
              i, (int)strlen(&zIn[i]), &zIn[i]);
      fflush(stdout);
    }else if( multiTest && !quietFlag ){
      int pct = oomFlag ? 100*iNext/nIn : ((10*iNext)/nIn)*10;
      if( pct!=lastPct ){
        if( lastPct<0 ) printf("fuzz test:");
        printf(" %d%%", pct);
        fflush(stdout);
        lastPct = pct;
      }
    }
    switch( iMode ){
      case FZMODE_Glob:
        zSql = zToFree = sqlite3_mprintf("SELECT glob(%s);", zSql);
        break;
      case FZMODE_Printf:
        zSql = zToFree = sqlite3_mprintf("SELECT printf(%s);", zSql);
        break;
      case FZMODE_Strftime:
        zSql = zToFree = sqlite3_mprintf("SELECT strftime(%s);", zSql);
        break;
    }
    if( oomFlag ){
      oomCnt = g.iOomCntdown = 1;
      g.nOomFault = 0;
      g.bOomOnce = 1;
      if( verboseFlag ){
        printf("Once.%d\n", oomCnt);
        fflush(stdout);
      }
    }else{
      oomCnt = 0;
    }
    do{
      rc = sqlite3_open_v2(
        "main.db", &db,
        SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_MEMORY,
        0);
      if( rc!=SQLITE_OK ){
        abendError("Unable to open the in-memory database");
      }
      if( pLook ){
        rc = sqlite3_db_config(db, SQLITE_DBCONFIG_LOOKASIDE, pLook, szLook, nLook);
        if( rc!=SQLITE_OK ) abendError("lookaside configuration filed: %d", rc);
      }
      if( zInitDb ){
        sqlite3_backup *pBackup;
        pBackup = sqlite3_backup_init(db, "main", dbInit, "main");
        rc = sqlite3_backup_step(pBackup, -1);
        if( rc!=SQLITE_DONE ){
          abendError("attempt to initialize the in-memory database failed (rc=%d)",
                     rc);
        }
        sqlite3_backup_finish(pBackup);
      }
  #ifndef SQLITE_OMIT_TRACE
      sqlite3_trace(db, verboseFlag ? traceCallback : traceNoop, 0);
  #endif
      sqlite3_create_function(db, "eval", 1, SQLITE_UTF8, 0, sqlEvalFunc, 0, 0);
      sqlite3_create_function(db, "eval", 2, SQLITE_UTF8, 0, sqlEvalFunc, 0, 0);
      sqlite3_limit(db, SQLITE_LIMIT_LENGTH, 1000000);
      if( zEncoding ) sqlexec(db, "PRAGMA encoding=%s", zEncoding);
      if( pageSize ) sqlexec(db, "PRAGMA pagesize=%d", pageSize);
      if( doAutovac ) sqlexec(db, "PRAGMA auto_vacuum=FULL");
      g.bOomEnable = 1;
      if( verboseFlag ){
        zErrMsg = 0;
        rc = sqlite3_exec(db, zSql, execCallback, 0, &zErrMsg);
        if( zErrMsg ){
          sqlite3_snprintf(sizeof(zErrBuf),zErrBuf,"%z", zErrMsg);
          zErrMsg = 0;
        }
      }else {
        rc = sqlite3_exec(db, zSql, execNoop, 0, 0);
      }
      g.bOomEnable = 0;
      rc = sqlite3_close(db);
      if( rc ){
        abendError("sqlite3_close() failed with rc=%d", rc);
      }
      if( sqlite3_memory_used()>0 ){
        abendError("memory in use after close: %lld bytes", sqlite3_memory_used());
      }
      if( oomFlag ){
        if( g.nOomFault==0 || oomCnt>2000 ){
          if( g.bOomOnce ){
            oomCnt = g.iOomCntdown = 1;
            g.bOomOnce = 0;
          }else{
            oomCnt = 0;
          }
        }else{
          g.iOomCntdown = ++oomCnt;
          g.nOomFault = 0;
        }
        if( oomCnt ){
          if( verboseFlag ){
            printf("%s.%d\n", g.bOomOnce ? "Once" : "Multi", oomCnt);
            fflush(stdout);
          }
          nTest++;
        }
      }
    }while( oomCnt>0 );
    if( zToFree ){
      sqlite3_free(zToFree);
      zToFree = 0;
    }
    zIn[iNext] = cSaved;
    if( verboseFlag ){
      printf("RESULT-CODE: %d\n", rc);
      if( zErrMsg ){
        printf("ERROR-MSG: [%s]\n", zErrBuf);
      }
      fflush(stdout);
    }
    /* Simulate an error if the TEST_FAILURE environment variable is "5" */
    if( zFailCode ){
      if( zFailCode[0]=='5' && zFailCode[1]==0 ){
        abendError("simulated failure");
      }else if( zFailCode[0]!=0 ){
        /* If TEST_FAILURE is something other than 5, just exit the test
        ** early */
        printf("\nExit early due to TEST_FAILURE being set");
        break;
      }
    }
  }
  if( !verboseFlag && multiTest && !quietFlag ) printf("\n");
  if( nTest>1 && !quietFlag ){
    printf("%d fuzz tests with no errors\nSQLite %s %s\n",
           nTest, sqlite3_libversion(), sqlite3_sourceid());
  }
  if( zDataOut ){
    int n = 0;
    FILE *out = fopen(zDataOut, "wb");
    if( out==0 ) abendError("cannot open %s for writing", zDataOut);
    if( nHeader>0 ) fwrite(zIn, nHeader, 1, out);
    sqlite3_finalize(pStmt);
    rc = sqlite3_prepare_v2(dataDb, "SELECT sql FROM testcase", -1, &pStmt, 0);
    if( rc ) abendError("%s", sqlite3_errmsg(dataDb));
    while( sqlite3_step(pStmt)==SQLITE_ROW ){
      fprintf(out,"/****<%d>****/", ++n);
      fwrite(sqlite3_column_blob(pStmt,0),sqlite3_column_bytes(pStmt,0),1,out);
    }
    fclose(out);
    sqlite3_finalize(pStmt);
    sqlite3_close(dataDb);
  }
  free(zIn);
  free(pHeap);
  free(pLook);
  free(pScratch);
  free(pPCache);
  return 0;
}

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
*/
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdarg.h>
#include "sqlite3.h"

/*
** All global variables are gathered into the "g" singleton.
*/
struct GlobalVars {
  const char *zArgv0;         /* Name of program */
} g;



/*
** Print an error message and abort in such a way to indicate to the
** fuzzer that this counts as a crash.
*/
static void abendError(const char *zFormat, ...){
  va_list ap;
  fprintf(stderr, "%s: ", g.zArgv0);
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
  fprintf(stderr, "%s: ", g.zArgv0);
  va_start(ap, zFormat);
  vfprintf(stderr, zFormat, ap);
  va_end(ap);
  fprintf(stderr, "\n");
  exit(1);
}

/*
** This callback is invoked by sqlite3_log().
*/
static void shellLog(void *pNotUsed, int iErrCode, const char *zMsg){
  printf("LOG: (%d) %s\n", iErrCode, zMsg);
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
  return 0;
}

/*
** This callback is invoked by sqlite3_trace() as each SQL statement
** starts.
*/
static void traceCallback(void *NotUsed, const char *zMsg){
  printf("TRACE: %s\n", zMsg);
}

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
"  -f FILE             Read SQL text from FILE instead of standard input\n"
"  --help              Show this help text\n"    
"  --initdb DBFILE     Initialize the in-memory database using template DBFILE\n"
  );
}


int main(int argc, char **argv){
  char *zIn = 0;          /* Input text */
  int nAlloc = 0;         /* Number of bytes allocated for zIn[] */
  int nIn = 0;            /* Number of bytes of zIn[] used */
  size_t got;             /* Bytes read from input */
  FILE *in = stdin;       /* Where to read SQL text from */
  int rc = SQLITE_OK;     /* Result codes from API functions */
  int i;                  /* Loop counter */
  sqlite3 *db;            /* Open database */
  sqlite3 *dbInit;        /* On-disk database used to initialize the in-memory db */
  const char *zInitDb = 0;/* Name of the initialization database file */
  char *zErrMsg = 0;      /* Error message returned from sqlite3_exec() */

  g.zArgv0 = argv[0];
  for(i=1; i<argc; i++){
    const char *z = argv[i];
    if( z[0]=='-' ){
      z++;
      if( z[0]=='-' ) z++;
      if( strcmp(z,"help")==0 ){
        showHelp();
        return 0;
      }else
      if( strcmp(z, "f")==0 && i+1<argc ){
        if( in!=stdin ) abendError("only one -f allowed");
        in = fopen(argv[++i],"rb");
        if( in==0 )  abendError("cannot open input file \"%s\"", argv[i]);
      }else
      if( strcmp(z, "initdb")==0 && i+1<argc ){
        if( zInitDb!=0 ) abendError("only one --initdb allowed");
        zInitDb = argv[++i];
      }else
      {
        abendError("unknown option: %s", argv[i]);
      }
    }else{
      abendError("unknown argument: %s", argv[i]);
    }
  }
  sqlite3_config(SQLITE_CONFIG_LOG, shellLog, 0);
  rc = sqlite3_open_v2(
    "main.db", &db,
    SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_MEMORY,
    0);
  if( rc!=SQLITE_OK ){
    abendError("Unable to open the in-memory database");
  }
  if( zInitDb ){
    sqlite3_backup *pBackup;
    rc = sqlite3_open_v2(zInitDb, &dbInit, SQLITE_OPEN_READONLY, 0);
    if( rc!=SQLITE_OK ){
      abendError("unable to open initialization database \"%s\"", zInitDb);
    }
    pBackup = sqlite3_backup_init(db, "main", dbInit, "main");
    rc = sqlite3_backup_step(pBackup, -1);
    if( rc!=SQLITE_DONE ){
      abendError("attempt to initialize the in-memory database failed (rc=%d)",rc);
    }
    sqlite3_backup_finish(pBackup);
    sqlite3_close(dbInit);
  }
  sqlite3_trace(db, traceCallback, 0);
  sqlite3_create_function(db, "eval", 1, SQLITE_UTF8, 0, sqlEvalFunc, 0, 0);
  sqlite3_create_function(db, "eval", 2, SQLITE_UTF8, 0, sqlEvalFunc, 0, 0);
  while( !feof(in) ){
    nAlloc += 1000;
    zIn = sqlite3_realloc(zIn, nAlloc);
    if( zIn==0 ) fatalError("out of memory");
    got = fread(zIn+nIn, 1, nAlloc-nIn-1, in); 
    nIn += (int)got;
    zIn[nIn] = 0;
    if( got==0 ) break;
  }
  printf("INPUT (%d bytes): [%s]\n", nIn, zIn);
  rc = sqlite3_exec(db, zIn, execCallback, 0, &zErrMsg);
  printf("RESULT-CODE: %d\n", rc);
  if( zErrMsg ){
    printf("ERROR-MSG: [%s]\n", zErrMsg);
    sqlite3_free(zErrMsg);
  }
  return rc!=SQLITE_OK;
}

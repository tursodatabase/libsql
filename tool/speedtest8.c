/*
** Performance test for SQLite.
**
** This program reads ASCII text from a file named on the command-line
** and submits that text  to SQLite for evaluation.  A new database
** is created at the beginning of the program.  All statements are
** timed using the high-resolution timer built into Intel-class processors.
**
** To compile this program, first compile the SQLite library separately
** will full optimizations.  For example:
**
**     gcc -c -O6 -DSQLITE_THREADSAFE=0 sqlite3.c
**
** Then link against this program.  But to do optimize this program
** because that defeats the hi-res timer.
**
**     gcc speedtest8.c sqlite3.o -ldl
**
** Then run this program with a single argument which is the name of
** a file containing SQL script that you want to test:
**
**     ./a.out test.db  test.sql
*/
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <ctype.h>
#include <unistd.h>
#include "sqlite3.h"


/*
** The following routine only works on pentium-class processors.
** It uses the RDTSC opcode to read the cycle count value out of the
** processor and returns that value.  This can be used for high-res
** profiling.
*/
__inline__ unsigned long long int hwtime(void){
   unsigned int lo, hi;
   /* We cannot use "=A", since this would use %rax on x86_64 */
   __asm__ __volatile__ ("rdtsc" : "=a" (lo), "=d" (hi));
   return (unsigned long long int)hi << 32 | lo;
}

/*
** Timers
*/
static unsigned long long int prepTime = 0;
static unsigned long long int runTime = 0;
static unsigned long long int finalizeTime = 0;

/*
** Prepare and run a single statement of SQL.
*/
static void prepareAndRun(sqlite3 *db, const char *zSql){
  sqlite3_stmt *pStmt;
  const char *stmtTail;
  unsigned long long int iStart, iElapse;
  int rc;
  
  printf("****************************************************************\n");
  printf("SQL statement: [%s]\n", zSql);
  iStart = hwtime();
  rc = sqlite3_prepare_v2(db, zSql, -1, &pStmt, &stmtTail);
  iElapse = hwtime() - iStart;
  prepTime += iElapse;
  printf("sqlite3_prepare_v2() returns %d in %llu cycles\n", rc, iElapse);
  if( rc==SQLITE_OK ){
    int nRow = 0;
    iStart = hwtime();
    while( (rc=sqlite3_step(pStmt))==SQLITE_ROW ){ nRow++; }
    iElapse = hwtime() - iStart;
    runTime += iElapse;
    printf("sqlite3_step() returns %d after %d rows in %llu cycles\n",
           rc, nRow, iElapse);
    iStart = hwtime();
    rc = sqlite3_finalize(pStmt);
    iElapse = hwtime() - iStart;
    finalizeTime += iElapse;
    printf("sqlite3_finalize() returns %d in %llu cycles\n", rc, iElapse);
  }
}

/***************************************************************************
** The "overwrite" VFS is an overlay over the default VFS.  It modifies
** the xTruncate operation on journal files so that xTruncate merely
** writes zeros into the first 50 bytes of the file rather than truely
** truncating the file.
**
** The following variables are initialized to be the virtual function
** tables for the overwrite VFS.
*/
static sqlite3_vfs overwrite_vfs;
static sqlite3_io_methods overwrite_methods;

/*
** The truncate method for journal files in the overwrite VFS.
*/
static int overwriteTruncate(sqlite3_file *pFile, sqlite_int64 size){
  int rc;
  static const char buf[50];
  if( size ){
    return SQLITE_IOERR;
  }
  rc = pFile->pMethods->xWrite(pFile, buf, sizeof(buf), 0);
  if( rc==SQLITE_OK ){
    rc = pFile->pMethods->xSync(pFile, SQLITE_SYNC_NORMAL);
  }
  return rc;
}

/*
** The delete method for journal files in the overwrite VFS.
*/
static int overwriteDelete(sqlite3_file *pFile){
  return overwriteTruncate(pFile, 0);
}

/*
** The open method for overwrite VFS.  If the file being opened is
** a journal file then substitute the alternative xTruncate method.
*/
static int overwriteOpen(
  sqlite3_vfs *pVfs,
  const char *zName,
  sqlite3_file *pFile,
  int flags,
  int *pOutFlags
){
  int rc;
  sqlite3_vfs *pRealVfs;
  int isJournal;

  isJournal = (flags & (SQLITE_OPEN_MAIN_JOURNAL|SQLITE_OPEN_TEMP_JOURNAL))!=0;
  pRealVfs = (sqlite3_vfs*)pVfs->pAppData;
  rc = pRealVfs->xOpen(pRealVfs, zName, pFile, flags, pOutFlags);
  if( rc==SQLITE_OK && isJournal ){
    if( overwrite_methods.xTruncate==0 ){
      sqlite3_io_methods temp;
      memcpy(&temp, pFile->pMethods, sizeof(temp));
      temp.xTruncate = overwriteTruncate;
      memcpy(&overwrite_methods, &temp, sizeof(temp));
    }
    pFile->pMethods = &overwrite_methods;
  }
  return rc;
}

/*
** Overlay the overwrite VFS over top of the current default VFS
** and make the overlay VFS the new default.
**
** This routine can only be evaluated once.  On second and subsequent
** executions it becomes a no-op.
*/
static void registerOverwriteVfs(void){
  sqlite3_vfs *pBase;
  if( overwrite_vfs.iVersion ) return;
  pBase = sqlite3_vfs_find(0);
  memcpy(&overwrite_vfs, pBase, sizeof(overwrite_vfs));
  overwrite_vfs.pAppData = pBase;
  overwrite_vfs.xOpen = overwriteOpen;
  overwrite_vfs.zName = "overwriteVfs";
  sqlite3_vfs_register(&overwrite_vfs, 1);
}

int main(int argc, char **argv){
  sqlite3 *db;
  int rc;
  int nSql;
  char *zSql;
  int i, j;
  FILE *in;
  unsigned long long int iStart, iElapse;
  unsigned long long int iSetup = 0;
  int nStmt = 0;
  int nByte = 0;
  const char *zArgv0 = argv[0];

#ifdef HAVE_OSINST
  extern sqlite3_vfs *sqlite3_instvfs_binarylog(char *, char *, char *);
  extern void sqlite3_instvfs_destroy(sqlite3_vfs *);
  sqlite3_vfs *pVfs = 0;
#endif

  if( argc>=4 && strcmp(argv[1], "-overwrite")==0 ){
    registerOverwriteVfs();
    argv++;
    argc--;
  }

#ifdef HAVE_OSINST
  if( argc>=5 && strcmp(argv[1], "-log")==0 ){
    pVfs = sqlite3_instvfs_binarylog("oslog", 0, argv[2]);
    sqlite3_vfs_register(pVfs, 1);
    argv += 2;
    argc -= 2;
  }
#endif

  if( argc>=4 && strcmp(argv[1], "-overwrite")==0 ){
    registerOverwriteVfs();
    argv++;
    argc--;
  }

  if( argc!=3 ){
    fprintf(stderr, "Usage: %s [options] FILENAME SQL-SCRIPT\n"
                    "Runs SQL-SCRIPT against a UTF8 database\n",
                    zArgv0);
    exit(1);
  }

  in = fopen(argv[2], "r");
  fseek(in, 0L, SEEK_END);
  nSql = ftell(in);
  zSql = malloc( nSql+1 );
  fseek(in, 0L, SEEK_SET);
  nSql = fread(zSql, 1, nSql, in);
  zSql[nSql] = 0;

  printf("SQLite version: %d\n", sqlite3_libversion_number());
  unlink(argv[1]);
  iStart = hwtime();
  rc = sqlite3_open(argv[1], &db);
  iElapse = hwtime() - iStart;
  iSetup = iElapse;
  printf("sqlite3_open() returns %d in %llu cycles\n", rc, iElapse);
  for(i=j=0; j<nSql; j++){
    if( zSql[j]==';' ){
      int isComplete;
      char c = zSql[j+1];
      zSql[j+1] = 0;
      isComplete = sqlite3_complete(&zSql[i]);
      zSql[j+1] = c;
      if( isComplete ){
        zSql[j] = 0;
        while( i<j && isspace(zSql[i]) ){ i++; }
        if( i<j ){
          int n = j - i;
          if( n>=6 && memcmp(&zSql[i], ".crash",6)==0 ) exit(1);
          nStmt++;
          nByte += n;
          prepareAndRun(db, &zSql[i]);
        }
        zSql[j] = ';';
        i = j+1;
      }
    }
  }
  iStart = hwtime();
  sqlite3_close(db);
  iElapse = hwtime() - iStart;
  iSetup += iElapse;
  printf("sqlite3_close() returns in %llu cycles\n", iElapse);
  printf("\n");
  printf("Statements run:       %15d\n", nStmt);
  printf("Bytes of SQL text:    %15d\n", nByte);
  printf("Total prepare time:   %15llu cycles\n", prepTime);
  printf("Total run time:       %15llu cycles\n", runTime);
  printf("Total finalize time:  %15llu cycles\n", finalizeTime);
  printf("Open/Close time:      %15llu cycles\n", iSetup);
  printf("Total Time:           %15llu cycles\n",
      prepTime + runTime + finalizeTime + iSetup);

#ifdef HAVE_OSINST
  if( pVfs ){
    sqlite3_instvfs_destroy(pVfs);
    printf("vfs log written to %s\n", argv[0]);
  }
#endif

  return 0;
}

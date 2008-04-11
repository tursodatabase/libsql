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
#include <stdarg.h>
#include "sqlite3.h"

/*
** The following routine only works on pentium-class processors.
** It uses the RDTSC opcode to read the cycle count value out of the
** processor and returns that value.  This can be used for high-res
** profiling.
*/
__inline__ sqlite3_uint64 hwtime(void){
   unsigned int lo, hi;
   /* We cannot use "=A", since this would use %rax on x86_64 */
   __asm__ __volatile__ ("rdtsc" : "=a" (lo), "=d" (hi));
   return (sqlite3_uint64)hi << 32 | lo;
}

/*
** Send a message to the log file.
*/
static void logMessage(const char *zFormat, ...){
  va_list ap;
  va_start(ap, zFormat);
  vprintf(zFormat, ap);
  va_end(ap);
}

/*
** Timers
*/
static sqlite3_uint64 prepTime = 0;
static sqlite3_uint64 runTime = 0;
static sqlite3_uint64 finalizeTime = 0;
static sqlite3_uint64 instTime = 0;

typedef struct inst_file inst_file;
struct inst_file {
  sqlite3_file base;
  sqlite3_file *pReal;
};

/*
** Method declarations for inst_file.
*/
static int instClose(sqlite3_file*);
static int instRead(sqlite3_file*, void*, int iAmt, sqlite3_int64 iOfst);
static int instWrite(sqlite3_file*,const void*,int iAmt, sqlite3_int64 iOfst);
static int instTruncate(sqlite3_file*, sqlite3_int64 size);
static int instSync(sqlite3_file*, int flags);
static int instFileSize(sqlite3_file*, sqlite3_int64 *pSize);
static int instLock(sqlite3_file*, int);
static int instUnlock(sqlite3_file*, int);
static int instCheckReservedLock(sqlite3_file*);
static int instFileControl(sqlite3_file*, int op, void *pArg);
static int instSectorSize(sqlite3_file*);
static int instDeviceCharacteristics(sqlite3_file*);

/*
** Method declarations for inst_vfs.
*/
static int instOpen(sqlite3_vfs*, const char *, sqlite3_file*, int , int *);
static int instDelete(sqlite3_vfs*, const char *zName, int syncDir);
static int instAccess(sqlite3_vfs*, const char *zName, int flags);
static int instGetTempName(sqlite3_vfs*, int nOut, char *zOut);
static int instFullPathname(sqlite3_vfs*, const char *zName, int, char *zOut);
static void *instDlOpen(sqlite3_vfs*, const char *zFilename);
static void instDlError(sqlite3_vfs*, int nByte, char *zErrMsg);
static void *instDlSym(sqlite3_vfs*,void*, const char *zSymbol);
static void instDlClose(sqlite3_vfs*, void*);
static int instRandomness(sqlite3_vfs*, int nByte, char *zOut);
static int instSleep(sqlite3_vfs*, int microseconds);
static int instCurrentTime(sqlite3_vfs*, double*);

static sqlite3_vfs inst_vfs = {
  1,                      /* iVersion */
  0,                      /* szOsFile */
  0,                      /* mxPathname */
  0,                      /* pNext */
  "instVfs",              /* zName */
  0,                      /* pAppData */
  instOpen,               /* xOpen */
  instDelete,             /* xDelete */
  instAccess,             /* xAccess */
  instGetTempName,        /* xGetTempName */
  instFullPathname,       /* xFullPathname */
  instDlOpen,             /* xDlOpen */
  instDlError,            /* xDlError */
  instDlSym,              /* xDlSym */
  instDlClose,            /* xDlClose */
  instRandomness,         /* xRandomness */
  instSleep,              /* xSleep */
  instCurrentTime         /* xCurrentTime */
};

static sqlite3_io_methods inst_io_methods = {
  1,                            /* iVersion */
  instClose,                      /* xClose */
  instRead,                       /* xRead */
  instWrite,                      /* xWrite */
  instTruncate,                   /* xTruncate */
  instSync,                       /* xSync */
  instFileSize,                   /* xFileSize */
  instLock,                       /* xLock */
  instUnlock,                     /* xUnlock */
  instCheckReservedLock,          /* xCheckReservedLock */
  instFileControl,                /* xFileControl */
  instSectorSize,                 /* xSectorSize */
  instDeviceCharacteristics       /* xDeviceCharacteristics */
};

#define OS_TIME_IO(MESSAGE, A, B, CALL)      \
  int rc; sqlite3_uint64 t1, t2;             \
  inst_file *p = (inst_file*)pFile;          \
  t1 = hwtime();                             \
  rc = CALL;                                 \
  t2 = hwtime();                             \
  logMessage(MESSAGE, A, B, t2-t1);          \
  instTime += hwtime() - t2;                 \
  return rc;

#define OS_TIME_VFS(MESSAGE, A, B, CALL)                 \
  int rc;                                                \
  sqlite3_uint64 t1, t2;                                 \
  sqlite3_vfs *pRealVfs = (sqlite3_vfs*)pVfs->pAppData;  \
  t1 = hwtime();                                         \
  rc = CALL;                                             \
  t2 = hwtime();                                         \
  logMessage(MESSAGE, A, B, t2-t1);                      \
  instTime += hwtime() - t2;                             \
  return rc;


/*
** Close an inst-file.
*/
static int instClose(sqlite3_file *pFile){
  OS_TIME_IO("xClose: %s%s%lld cycles\n", "", "",
    p->pReal->pMethods->xClose(p->pReal)
  );
}

/*
** Read data from an inst-file.
*/
static int instRead(
  sqlite3_file *pFile, 
  void *zBuf, 
  int iAmt, 
  sqlite_int64 iOfst
){
  OS_TIME_IO("xRead: %d bytes at offset %lld - %lld cycles\n", iAmt, iOfst, 
    p->pReal->pMethods->xRead(p->pReal, zBuf, iAmt,iOfst)
  );
}

/*
** Write data to an inst-file.
*/
static int instWrite(
  sqlite3_file *pFile,
  const void *z,
  int iAmt,
  sqlite_int64 iOfst
){
  OS_TIME_IO("xWrite: %d bytes at offset %lld - %lld cycles\n", iAmt, iOfst,
    p->pReal->pMethods->xWrite(p->pReal, z, iAmt, iOfst)
  );
}

/*
** Truncate an inst-file.
*/
static int instTruncate(sqlite3_file *pFile, sqlite_int64 size){
  OS_TIME_IO("xTruncate: to %lld bytes - %s%lld cycles\n", size, "",
    p->pReal->pMethods->xTruncate(p->pReal, size)
  );
}

/*
** Sync an inst-file.
*/
static int instSync(sqlite3_file *pFile, int flags){
  OS_TIME_IO("xSync: %s%s%lld cycles\n", "", "", 
    p->pReal->pMethods->xSync(p->pReal, flags)
  );
}

/*
** Return the current file-size of an inst-file.
*/
static int instFileSize(sqlite3_file *pFile, sqlite_int64 *pSize){
  OS_TIME_IO("xFileSize: %s%s%lld cycles\n", "", "", 
    p->pReal->pMethods->xFileSize(p->pReal, pSize)
  );
}

/*
** Lock an inst-file.
*/
static int instLock(sqlite3_file *pFile, int eLock){
  OS_TIME_IO("xLock: %d %s%lld cycles\n", eLock, "",
    p->pReal->pMethods->xLock(p->pReal, eLock)
  );
}

/*
** Unlock an inst-file.
*/
static int instUnlock(sqlite3_file *pFile, int eLock){
  OS_TIME_IO("xUnlock: %d %s%lld\n", eLock, "",
    p->pReal->pMethods->xUnlock(p->pReal, eLock)
  );
}

/*
** Check if another file-handle holds a RESERVED lock on an inst-file.
*/
static int instCheckReservedLock(sqlite3_file *pFile){
  OS_TIME_IO("xCheckReservedLock: %s%s%lld cycles\n", "", "",
    p->pReal->pMethods->xCheckReservedLock(p->pReal)
  );
}

/*
** File control method. For custom operations on an inst-file.
*/
static int instFileControl(sqlite3_file *pFile, int op, void *pArg){
  OS_TIME_IO("xFileControl:  op=%d - %s%lld cycles\n", op, "",
    p->pReal->pMethods->xFileControl(p->pReal, op, pArg)
  );
}

/*
** Return the sector-size in bytes for an inst-file.
*/
static int instSectorSize(sqlite3_file *pFile){
  OS_TIME_IO("xSectorSize: %s%s%lld cycles\n", "", "",
    p->pReal->pMethods->xSectorSize(p->pReal)
  );
}

/*
** Return the device characteristic flags supported by an inst-file.
*/
static int instDeviceCharacteristics(sqlite3_file *pFile){
  OS_TIME_IO("xDeviceCharacteristics: %s%s%lld cycles\n", "", "",
    p->pReal->pMethods->xDeviceCharacteristics(p->pReal)
  );
}

/*
** Open an inst file handle.
*/
static int instOpen(
  sqlite3_vfs *pVfs,
  const char *zName,
  sqlite3_file *pFile,
  int flags,
  int *pOutFlags
){
  inst_file *p = (inst_file *)pFile;
  pFile->pMethods = &inst_io_methods;
  p->pReal = (sqlite3_file *)&p[1];

  OS_TIME_VFS("xOpen: \"%s\" flags=0x04%x - %lld cycles\n", zName, flags,
    pRealVfs->xOpen(pRealVfs, zName, p->pReal, flags, pOutFlags)
  );
}

/*
** Delete the file located at zPath. If the dirSync argument is true,
** ensure the file-system modifications are synced to disk before
** returning.
*/
static int instDelete(sqlite3_vfs *pVfs, const char *zPath, int dirSync){
  OS_TIME_VFS("xDelete:  \"%s\", dirSync=%d - %lld cycles\n",
    zPath, dirSync,
    pRealVfs->xDelete(pRealVfs, zPath, dirSync) 
  );
}

/*
** Test for access permissions. Return true if the requested permission
** is available, or false otherwise.
*/
static int instAccess(sqlite3_vfs *pVfs, const char *zPath, int flags){
  OS_TIME_VFS("xAccess of \"%s\", flags=0x%04x - %lld cycles\n",
    zPath, flags,
    pRealVfs->xAccess(pRealVfs, zPath, flags) 
  );
}

/*
** Populate buffer zBufOut with a pathname suitable for use as a 
** temporary file. zBufOut is guaranteed to point to a buffer of 
** at least (INST_MAX_PATHNAME+1) bytes.
*/
static int instGetTempName(sqlite3_vfs *pVfs, int nOut, char *zBufOut){
  OS_TIME_VFS("xGetTempName: %s%s%lld cycles\n", "", "",
    pRealVfs->xGetTempname(pRealVfs, nOut, zBufOut)
  );
}

/*
** Populate buffer zOut with the full canonical pathname corresponding
** to the pathname in zPath. zOut is guaranteed to point to a buffer
** of at least (INST_MAX_PATHNAME+1) bytes.
*/
static int instFullPathname(
  sqlite3_vfs *pVfs, 
  const char *zPath, 
  int nOut, 
  char *zOut
){
  OS_TIME_VFS("xFullPathname: \"%s\" - %s%lld cycles\n",
    zPath, "",
    pRealVfs->xFullPathname(pRealVfs, zPath, nOut, zOut)
  );
}

/*
** Open the dynamic library located at zPath and return a handle.
*/
static void *instDlOpen(sqlite3_vfs *pVfs, const char *zPath){
  sqlite3_vfs *pRealVfs = (sqlite3_vfs*)pVfs->pAppData;
  return pRealVfs->xDlOpen(pRealVfs, zPath);
}

/*
** Populate the buffer zErrMsg (size nByte bytes) with a human readable
** utf-8 string describing the most recent error encountered associated 
** with dynamic libraries.
*/
static void instDlError(sqlite3_vfs *pVfs, int nByte, char *zErrMsg){
  sqlite3_vfs *pRealVfs = (sqlite3_vfs*)pVfs->pAppData;
  pRealVfs->xDlError(pRealVfs, nByte, zErrMsg);
}

/*
** Return a pointer to the symbol zSymbol in the dynamic library pHandle.
*/
static void *instDlSym(sqlite3_vfs *pVfs, void *pHandle, const char *zSymbol){
  sqlite3_vfs *pRealVfs = (sqlite3_vfs*)pVfs->pAppData;
  return pRealVfs->xDlSym(pRealVfs, pHandle, zSymbol);
}

/*
** Close the dynamic library handle pHandle.
*/
static void instDlClose(sqlite3_vfs *pVfs, void *pHandle){
  sqlite3_vfs *pRealVfs = (sqlite3_vfs*)pVfs->pAppData;
  pRealVfs->xDlClose(pRealVfs, pHandle);
}

/*
** Populate the buffer pointed to by zBufOut with nByte bytes of 
** random data.
*/
static int instRandomness(sqlite3_vfs *pVfs, int nByte, char *zBufOut){
  OS_TIME_VFS("xRandomness:  nByte=%d - %s%lld cycles\n", nByte, "",
    pRealVfs->xRandomness(pRealVfs, nByte, zBufOut)
  );
}

/*
** Sleep for nMicro microseconds. Return the number of microseconds 
** actually slept.
*/
static int instSleep(sqlite3_vfs *pVfs, int nMicro){
  OS_TIME_VFS("xSleep:  usec=%d - %s%lld cycles\n", nMicro, "",
    pRealVfs->xSleep(pRealVfs, nMicro) 
  );
}

/*
** Return the current time as a Julian Day number in *pTimeOut.
*/
static int instCurrentTime(sqlite3_vfs *pVfs, double *pTimeOut){
  OS_TIME_VFS("xCurrentTime:  %s%s%lld cycles\n", "", "",
    pRealVfs->xCurrentTime(pRealVfs, pTimeOut) 
  );
}

/*
** Insert the instructed VFS as the default VFS.
*/
static void setupInstrumentedVfs(void){
  sqlite3_vfs *p;
  sqlite3_vfs *pParent;

  pParent = sqlite3_vfs_find(0);
  if( !pParent ){
    return;
  }

  p = sqlite3_malloc(sizeof(inst_vfs));
  if( p ){
    *p = inst_vfs;
    p->szOsFile = pParent->szOsFile + sizeof(inst_file);
    p->mxPathname = pParent->mxPathname;
    p->pAppData = pParent;
    sqlite3_vfs_register(p, 1);
  }
}


/*
** Prepare and run a single statement of SQL.
*/
static void prepareAndRun(sqlite3 *db, const char *zSql){
  sqlite3_stmt *pStmt;
  const char *stmtTail;
  sqlite3_uint64 iStart, iElapse;
  int rc;
  
  printf("****************************************************************\n");
  printf("SQL statement: [%s]\n", zSql);
  instTime = 0;
  iStart = hwtime();
  rc = sqlite3_prepare_v2(db, zSql, -1, &pStmt, &stmtTail);
  iElapse = hwtime();
  iElapse -= iStart + instTime;
  prepTime += iElapse;
  printf("sqlite3_prepare_v2() returns %d in %llu cycles\n", rc, iElapse);
  if( rc==SQLITE_OK ){
    int nRow = 0;
    instTime = 0;
    iStart = hwtime();
    while( (rc=sqlite3_step(pStmt))==SQLITE_ROW ){ nRow++; }
    iElapse = hwtime();
    iElapse -= iStart + instTime;
    runTime += iElapse;
    printf("sqlite3_step() returns %d after %d rows in %llu cycles\n",
           rc, nRow, iElapse);
    instTime = 0;
    iStart = hwtime();
    rc = sqlite3_finalize(pStmt);
    iElapse = hwtime();
    iElapse -= iStart + instTime;
    finalizeTime += iElapse;
    printf("sqlite3_finalize() returns %d in %llu cycles\n", rc, iElapse);
  }
}

int main(int argc, char **argv){
  sqlite3 *db;
  int rc;
  int nSql;
  char *zSql;
  int i, j;
  FILE *in;
  sqlite3_uint64 iStart, iElapse;
  sqlite3_uint64 iSetup = 0;
  int nStmt = 0;
  int nByte = 0;

  if( argc!=3 ){
    fprintf(stderr, "Usage: %s FILENAME SQL-SCRIPT\n"
                    "Runs SQL-SCRIPT against a UTF8 database\n",
                    argv[0]);
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
  setupInstrumentedVfs();
  instTime = 0;
  iStart = hwtime();
  rc = sqlite3_open(argv[1], &db);
  iElapse = hwtime();
  iElapse -= iStart + instTime;
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
          nStmt++;
          nByte += j-i;
          prepareAndRun(db, &zSql[i]);
        }
        zSql[j] = ';';
        i = j+1;
      }
    }
  }
  instTime = 0;
  iStart = hwtime();
  sqlite3_close(db);
  iElapse = hwtime();
  iElapse -= iStart + instTime;
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
  return 0;
}

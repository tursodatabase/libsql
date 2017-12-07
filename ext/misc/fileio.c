/*
** 2014-06-13
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
** This SQLite extension implements SQL functions readfile() and
** writefile().
**
** Also, an eponymous virtual table type "fsdir". Used as follows:
**
**   SELECT * FROM fsdir($dirname);
**
** Returns one row for each entry in the directory $dirname. No row is
** returned for "." or "..". Row columns are as follows:
**
**   name:  Name of directory entry.
**   mode:  Value of stat.st_mode for directory entry.
**   mtime: Value of stat.st_mtime for directory entry.
**   data:  For a regular file, a blob containing the file data. For a
**          symlink, a text value containing the text of the link. For a
**          directory, NULL.
*/
#include "sqlite3ext.h"
SQLITE_EXTENSION_INIT1
#include <stdio.h>
#include <string.h>
#include <assert.h>

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <dirent.h>
#include <time.h>
#include <utime.h>


#define FSDIR_SCHEMA "CREATE TABLE x(name,mode,mtime,data,dir HIDDEN)"

static void readFileContents(sqlite3_context *ctx, const char *zName){
  FILE *in;
  long nIn;
  void *pBuf;

  in = fopen(zName, "rb");
  if( in==0 ) return;
  fseek(in, 0, SEEK_END);
  nIn = ftell(in);
  rewind(in);
  pBuf = sqlite3_malloc( nIn );
  if( pBuf && 1==fread(pBuf, nIn, 1, in) ){
    sqlite3_result_blob(ctx, pBuf, nIn, sqlite3_free);
  }else{
    sqlite3_free(pBuf);
  }
  fclose(in);
}

/*
** Implementation of the "readfile(X)" SQL function.  The entire content
** of the file named X is read and returned as a BLOB.  NULL is returned
** if the file does not exist or is unreadable.
*/
static void readfileFunc(
  sqlite3_context *context,
  int argc,
  sqlite3_value **argv
){
  const char *zName;
  (void)(argc);  /* Unused parameter */
  zName = (const char*)sqlite3_value_text(argv[0]);
  if( zName==0 ) return;
  readFileContents(context, zName);
}

static void ctxErrorMsg(sqlite3_context *ctx, const char *zFmt, ...){
  char *zMsg = 0;
  va_list ap;
  va_start(ap, zFmt);
  zMsg = sqlite3_vmprintf(zFmt, ap);
  sqlite3_result_error(ctx, zMsg, -1);
  sqlite3_free(zMsg);
  va_end(ap);
}

/*
** Implementation of the "writefile(W,X[,Y]])" SQL function.  
**
** The argument X is written into file W.  The number of bytes written is
** returned. Or NULL is returned if something goes wrong, such as being unable
** to open file X for writing.
*/
static void writefileFunc(
  sqlite3_context *context,
  int argc,
  sqlite3_value **argv
){
  const char *zFile;
  mode_t mode = 0;

  if( argc<2 || argc>3 ){
    sqlite3_result_error(context, 
        "wrong number of arguments to function writefile()", -1
    );
    return;
  }

  zFile = (const char*)sqlite3_value_text(argv[0]);
  if( zFile==0 ) return;
  if( argc>=3 ){
    sqlite3_result_int(context, 0);
    mode = sqlite3_value_int(argv[2]);
  }

  if( S_ISLNK(mode) ){
    const char *zTo = (const char*)sqlite3_value_text(argv[1]);
    if( symlink(zTo, zFile)<0 ){
      ctxErrorMsg(context, "failed to create symlink: %s", zFile);
      return;
    }
  }else{
    if( S_ISDIR(mode) ){
      if( mkdir(zFile, mode) ){
        ctxErrorMsg(context, "failed to create directory: %s", zFile);
        return;
      }
    }else{
      sqlite3_int64 nWrite = 0;
      const char *z;
      int rc = 0;
      FILE *out = fopen(zFile, "wb");
      if( out==0 ){
        if( argc>2 ){
          ctxErrorMsg(context, "failed to open file for writing: %s", zFile);
        }
        return;
      }
      z = (const char*)sqlite3_value_blob(argv[1]);
      if( z ){
        sqlite3_int64 n = fwrite(z, 1, sqlite3_value_bytes(argv[1]), out);
        nWrite = sqlite3_value_bytes(argv[1]);
        if( nWrite!=n ){
          ctxErrorMsg(context, "failed to write file: %s", zFile);
          rc = 1;
        }
      }
      fclose(out);
      if( rc ) return;
      sqlite3_result_int64(context, nWrite);
    }

    if( argc>2 && chmod(zFile, mode & 0777) ){
      ctxErrorMsg(context, "failed to chmod file: %s", zFile);
      return;
    }
  }
}

#ifndef SQLITE_OMIT_VIRTUALTABLE

/* 
*/
typedef struct fsdir_cursor fsdir_cursor;
struct fsdir_cursor {
  sqlite3_vtab_cursor base;  /* Base class - must be first */
  int eType;                 /* One of FSDIR_DIR or FSDIR_ENTRY */
  DIR *pDir;                 /* From opendir() */
  struct stat sStat;         /* Current lstat() results */
  char *zDir;                /* Directory to read */
  int nDir;                  /* Value of strlen(zDir) */
  char *zPath;               /* Path to current entry */
  int bEof;
  sqlite3_int64 iRowid;      /* Current rowid */
};

typedef struct fsdir_tab fsdir_tab;
struct fsdir_tab {
  sqlite3_vtab base;         /* Base class - must be first */
  int eType;                 /* One of FSDIR_DIR or FSDIR_ENTRY */
};

#define FSDIR_DIR   0
#define FSDIR_ENTRY 1

/*
** Construct a new fsdir virtual table object.
*/
static int fsdirConnect(
  sqlite3 *db,
  void *pAux,
  int argc, const char *const*argv,
  sqlite3_vtab **ppVtab,
  char **pzErr
){
  fsdir_tab *pNew = 0;
  int rc;

  rc = sqlite3_declare_vtab(db, FSDIR_SCHEMA);
  if( rc==SQLITE_OK ){
    pNew = (fsdir_tab*)sqlite3_malloc( sizeof(*pNew) );
    if( pNew==0 ) return SQLITE_NOMEM;
    memset(pNew, 0, sizeof(*pNew));
    pNew->eType = (pAux==0 ? FSDIR_DIR : FSDIR_ENTRY);
  }
  *ppVtab = (sqlite3_vtab*)pNew;
  return rc;
}

/*
** This method is the destructor for fsdir vtab objects.
*/
static int fsdirDisconnect(sqlite3_vtab *pVtab){
  sqlite3_free(pVtab);
  return SQLITE_OK;
}

/*
** Constructor for a new fsdir_cursor object.
*/
static int fsdirOpen(sqlite3_vtab *p, sqlite3_vtab_cursor **ppCursor){
  fsdir_cursor *pCur;
  pCur = sqlite3_malloc( sizeof(*pCur) );
  if( pCur==0 ) return SQLITE_NOMEM;
  memset(pCur, 0, sizeof(*pCur));
  pCur->eType = ((fsdir_tab*)p)->eType;
  *ppCursor = &pCur->base;
  return SQLITE_OK;
}

/*
** Destructor for an fsdir_cursor.
*/
static int fsdirClose(sqlite3_vtab_cursor *cur){
  fsdir_cursor *pCur = (fsdir_cursor*)cur;
  if( pCur->pDir ) closedir(pCur->pDir);
  sqlite3_free(pCur->zDir);
  sqlite3_free(pCur->zPath);
  sqlite3_free(pCur);
  return SQLITE_OK;
}

/*
** Advance an fsdir_cursor to its next row of output.
*/
static int fsdirNext(sqlite3_vtab_cursor *cur){
  fsdir_cursor *pCur = (fsdir_cursor*)cur;
  struct dirent *pEntry;

  if( pCur->eType==FSDIR_ENTRY ){
    pCur->bEof = 1;
    return SQLITE_OK;
  }

  sqlite3_free(pCur->zPath);
  pCur->zPath = 0;

  while( 1 ){
    pEntry = readdir(pCur->pDir);
    if( pEntry ){
      if( strcmp(pEntry->d_name, ".") 
       && strcmp(pEntry->d_name, "..") 
      ){
        pCur->zPath = sqlite3_mprintf("%s/%s", pCur->zDir, pEntry->d_name);
        if( pCur->zPath==0 ) return SQLITE_NOMEM;
        lstat(pCur->zPath, &pCur->sStat);
        break;
      }
    }else{
      pCur->bEof = 1;
      break;
    }
  }

  pCur->iRowid++;
  return SQLITE_OK;
}

/*
** Return values of columns for the row at which the series_cursor
** is currently pointing.
*/
static int fsdirColumn(
  sqlite3_vtab_cursor *cur,   /* The cursor */
  sqlite3_context *ctx,       /* First argument to sqlite3_result_...() */
  int i                       /* Which column to return */
){
  fsdir_cursor *pCur = (fsdir_cursor*)cur;
  switch( i ){
    case 0: { /* name */
      const char *zName;
      if( pCur->eType==FSDIR_DIR ){
        zName = &pCur->zPath[pCur->nDir+1];
      }else{
        zName = pCur->zPath;
      }
      sqlite3_result_text(ctx, zName, -1, SQLITE_TRANSIENT);
      break;
    }

    case 1: /* mode */
      sqlite3_result_int64(ctx, pCur->sStat.st_mode);
      break;

    case 2: /* mode */
      sqlite3_result_int64(ctx, pCur->sStat.st_mtime);
      break;

    case 3: {
      mode_t m = pCur->sStat.st_mode;
      if( S_ISDIR(m) ){
        sqlite3_result_null(ctx);
      }else if( S_ISLNK(m) ){
        char aStatic[64];
        char *aBuf = aStatic;
        int nBuf = 64;
        int n;

        while( 1 ){
          n = readlink(pCur->zPath, aBuf, nBuf);
          if( n<nBuf ) break;
          if( aBuf!=aStatic ) sqlite3_free(aBuf);
          nBuf = nBuf*2;
          aBuf = sqlite3_malloc(nBuf);
          if( aBuf==0 ){
            sqlite3_result_error_nomem(ctx);
            return SQLITE_NOMEM;
          }
        }

        sqlite3_result_text(ctx, aBuf, n, SQLITE_TRANSIENT);
        if( aBuf!=aStatic ) sqlite3_free(aBuf);
      }else{
        readFileContents(ctx, pCur->zPath);
      }
    }
  }
  return SQLITE_OK;
}

/*
** Return the rowid for the current row. In this implementation, the
** first row returned is assigned rowid value 1, and each subsequent
** row a value 1 more than that of the previous.
*/
static int fsdirRowid(sqlite3_vtab_cursor *cur, sqlite_int64 *pRowid){
  fsdir_cursor *pCur = (fsdir_cursor*)cur;
  *pRowid = pCur->iRowid;
  return SQLITE_OK;
}

/*
** Return TRUE if the cursor has been moved off of the last
** row of output.
*/
static int fsdirEof(sqlite3_vtab_cursor *cur){
  fsdir_cursor *pCur = (fsdir_cursor*)cur;
  return pCur->bEof;
}

static void fsdirSetErrmsg(fsdir_cursor *pCur, const char *zFmt, ...){
  va_list ap;
  va_start(ap, zFmt);
  pCur->base.pVtab->zErrMsg = sqlite3_vmprintf(zFmt, ap);
  va_end(ap);
}

/*
** xFilter callback.
*/
static int fsdirFilter(
  sqlite3_vtab_cursor *cur, 
  int idxNum, const char *idxStr,
  int argc, sqlite3_value **argv
){
  const char *zDir = 0;
  fsdir_cursor *pCur = (fsdir_cursor*)cur;

  sqlite3_free(pCur->zDir);
  pCur->iRowid = 0;
  pCur->zDir = 0;
  pCur->bEof = 0;
  if( pCur->pDir ){
    closedir(pCur->pDir);
    pCur->pDir = 0;
  }

  if( idxNum==0 ){
    fsdirSetErrmsg(pCur, "table function fsdir requires an argument");
    return SQLITE_ERROR;
  }

  assert( argc==1 );
  zDir = (const char*)sqlite3_value_text(argv[0]);
  if( zDir==0 ){
    fsdirSetErrmsg(pCur, "table function fsdir requires a non-NULL argument");
    return SQLITE_ERROR;
  }

  pCur->zDir = sqlite3_mprintf("%s", zDir);
  if( pCur->zDir==0 ){
    return SQLITE_NOMEM;
  }

  if( pCur->eType==FSDIR_ENTRY ){
    int rc = lstat(pCur->zDir, &pCur->sStat);
    if( rc ){
      fsdirSetErrmsg(pCur, "cannot stat file: %s", pCur->zDir);
    }else{
      pCur->zPath = sqlite3_mprintf("%s", pCur->zDir);
      if( pCur->zPath==0 ) return SQLITE_NOMEM;
    }
    return SQLITE_OK;
  }else{
    pCur->nDir = strlen(pCur->zDir);
    pCur->pDir = opendir(zDir);
    if( pCur->pDir==0 ){
      fsdirSetErrmsg(pCur, "error in opendir(\"%s\")", zDir);
      return SQLITE_ERROR;
    }

    return fsdirNext(cur);
  }
}

/*
** SQLite will invoke this method one or more times while planning a query
** that uses the generate_series virtual table.  This routine needs to create
** a query plan for each invocation and compute an estimated cost for that
** plan.
**
** In this implementation idxNum is used to represent the
** query plan.  idxStr is unused.
**
** The query plan is represented by bits in idxNum:
**
**  (1)  start = $value  -- constraint exists
**  (2)  stop = $value   -- constraint exists
**  (4)  step = $value   -- constraint exists
**  (8)  output in descending order
*/
static int fsdirBestIndex(
  sqlite3_vtab *tab,
  sqlite3_index_info *pIdxInfo
){
  int i;                 /* Loop over constraints */

  const struct sqlite3_index_constraint *pConstraint;
  pConstraint = pIdxInfo->aConstraint;
  for(i=0; i<pIdxInfo->nConstraint; i++, pConstraint++){
    if( pConstraint->usable==0 ) continue;
    if( pConstraint->op!=SQLITE_INDEX_CONSTRAINT_EQ ) continue;
    if( pConstraint->iColumn!=4 ) continue;
    break;
  }

  if( i<pIdxInfo->nConstraint ){
    pIdxInfo->aConstraintUsage[i].omit = 1;
    pIdxInfo->aConstraintUsage[i].argvIndex = 1;
    pIdxInfo->idxNum = 1;
    pIdxInfo->estimatedCost = 10.0;
  }else{
    pIdxInfo->idxNum = 0;
    pIdxInfo->estimatedCost = (double)(((sqlite3_int64)1) << 50);
  }

  return SQLITE_OK;
}

static int fsdirRegister(sqlite3 *db){
  static sqlite3_module fsdirModule = {
    0,                         /* iVersion */
    0,                         /* xCreate */
    fsdirConnect,              /* xConnect */
    fsdirBestIndex,            /* xBestIndex */
    fsdirDisconnect,           /* xDisconnect */
    0,                         /* xDestroy */
    fsdirOpen,                 /* xOpen - open a cursor */
    fsdirClose,                /* xClose - close a cursor */
    fsdirFilter,               /* xFilter - configure scan constraints */
    fsdirNext,                 /* xNext - advance a cursor */
    fsdirEof,                  /* xEof - check for end of scan */
    fsdirColumn,               /* xColumn - read data */
    fsdirRowid,                /* xRowid - read data */
    0,                         /* xUpdate */
    0,                         /* xBegin */
    0,                         /* xSync */
    0,                         /* xCommit */
    0,                         /* xRollback */
    0,                         /* xFindMethod */
    0,                         /* xRename */
  };

  int rc = sqlite3_create_module(db, "fsdir", &fsdirModule, 0);
  if( rc==SQLITE_OK ){
    rc = sqlite3_create_module(db, "fsentry", &fsdirModule, (void*)1);
  }
  return rc;
}
#else         /* SQLITE_OMIT_VIRTUALTABLE */
# define fsdirRegister(x) SQLITE_OK
#endif

#ifdef _WIN32
__declspec(dllexport)
#endif
int sqlite3_fileio_init(
  sqlite3 *db, 
  char **pzErrMsg, 
  const sqlite3_api_routines *pApi
){
  int rc = SQLITE_OK;
  SQLITE_EXTENSION_INIT2(pApi);
  (void)pzErrMsg;  /* Unused parameter */
  rc = sqlite3_create_function(db, "readfile", 1, SQLITE_UTF8, 0,
                               readfileFunc, 0, 0);
  if( rc==SQLITE_OK ){
    rc = sqlite3_create_function(db, "writefile", -1, SQLITE_UTF8, 0,
                                 writefileFunc, 0, 0);
  }
  if( rc==SQLITE_OK ){
    rc = fsdirRegister(db);
  }
  return rc;
}

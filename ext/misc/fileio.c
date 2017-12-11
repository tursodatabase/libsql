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


#define FSDIR_SCHEMA "(name,mode,mtime,data,path HIDDEN,dir HIDDEN)"

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
** Cursor type for recursively iterating through a directory structure.
*/
typedef struct fsdir_cursor fsdir_cursor;
typedef struct FsdirLevel FsdirLevel;

struct FsdirLevel {
  DIR *pDir;                 /* From opendir() */
  char *zDir;                /* Name of directory (nul-terminated) */
};

struct fsdir_cursor {
  sqlite3_vtab_cursor base;  /* Base class - must be first */

  int nLvl;                  /* Number of entries in aLvl[] array */
  int iLvl;                  /* Index of current entry */
  FsdirLevel *aLvl;          /* Hierarchy of directories being traversed */

  const char *zBase;
  int nBase;

  struct stat sStat;         /* Current lstat() results */
  char *zPath;               /* Path to current entry */
  sqlite3_int64 iRowid;      /* Current rowid */
};

typedef struct fsdir_tab fsdir_tab;
struct fsdir_tab {
  sqlite3_vtab base;         /* Base class - must be first */
};

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

  rc = sqlite3_declare_vtab(db, "CREATE TABLE x" FSDIR_SCHEMA);
  if( rc==SQLITE_OK ){
    pNew = (fsdir_tab*)sqlite3_malloc( sizeof(*pNew) );
    if( pNew==0 ) return SQLITE_NOMEM;
    memset(pNew, 0, sizeof(*pNew));
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
  pCur->iLvl = -1;
  *ppCursor = &pCur->base;
  return SQLITE_OK;
}

static void fsdirResetCursor(fsdir_cursor *pCur){
  int i;
  for(i=0; i<=pCur->iLvl; i++){
    FsdirLevel *pLvl = &pCur->aLvl[i];
    if( pLvl->pDir ) closedir(pLvl->pDir);
    sqlite3_free(pLvl->zDir);
  }
  sqlite3_free(pCur->zPath);
  pCur->aLvl = 0;
  pCur->zPath = 0;
  pCur->zBase = 0;
  pCur->nBase = 0;
  pCur->iLvl = -1;
  pCur->iRowid = 1;
}

/*
** Destructor for an fsdir_cursor.
*/
static int fsdirClose(sqlite3_vtab_cursor *cur){
  fsdir_cursor *pCur = (fsdir_cursor*)cur;

  fsdirResetCursor(pCur);
  sqlite3_free(pCur->aLvl);
  sqlite3_free(pCur);
  return SQLITE_OK;
}

static void fsdirSetErrmsg(fsdir_cursor *pCur, const char *zFmt, ...){
  va_list ap;
  va_start(ap, zFmt);
  pCur->base.pVtab->zErrMsg = sqlite3_vmprintf(zFmt, ap);
  va_end(ap);
}


/*
** Advance an fsdir_cursor to its next row of output.
*/
static int fsdirNext(sqlite3_vtab_cursor *cur){
  fsdir_cursor *pCur = (fsdir_cursor*)cur;
  mode_t m = pCur->sStat.st_mode;

  pCur->iRowid++;
  if( S_ISDIR(m) ){
    /* Descend into this directory */
    int iNew = pCur->iLvl + 1;
    FsdirLevel *pLvl;
    if( iNew>=pCur->nLvl ){
      int nNew = iNew+1;
      int nByte = nNew*sizeof(FsdirLevel);
      FsdirLevel *aNew = (FsdirLevel*)sqlite3_realloc(pCur->aLvl, nByte);
      if( aNew==0 ) return SQLITE_NOMEM;
      memset(&aNew[pCur->nLvl], 0, sizeof(FsdirLevel)*(nNew-pCur->nLvl));
      pCur->aLvl = aNew;
      pCur->nLvl = nNew;
    }
    pCur->iLvl = iNew;
    pLvl = &pCur->aLvl[iNew];
    
    pLvl->zDir = pCur->zPath;
    pCur->zPath = 0;
    pLvl->pDir = opendir(pLvl->zDir);
    if( pLvl->pDir==0 ){
      fsdirSetErrmsg(pCur, "cannot read directory: %s", pCur->zPath);
      return SQLITE_ERROR;
    }
  }

  while( pCur->iLvl>=0 ){
    FsdirLevel *pLvl = &pCur->aLvl[pCur->iLvl];
    struct dirent *pEntry = readdir(pLvl->pDir);
    if( pEntry ){
      if( pEntry->d_name[0]=='.' ){
       if( pEntry->d_name[1]=='.' && pEntry->d_name[2]=='\0' ) continue;
       if( pEntry->d_name[1]=='\0' ) continue;
      }
      sqlite3_free(pCur->zPath);
      pCur->zPath = sqlite3_mprintf("%s/%s", pLvl->zDir, pEntry->d_name);
      if( pCur->zPath==0 ) return SQLITE_NOMEM;
      if( lstat(pCur->zPath, &pCur->sStat) ){
        fsdirSetErrmsg(pCur, "cannot stat file: %s", pCur->zPath);
        return SQLITE_ERROR;
      }
      return SQLITE_OK;
    }
    closedir(pLvl->pDir);
    sqlite3_free(pLvl->zDir);
    pLvl->pDir = 0;
    pLvl->zDir = 0;
    pCur->iLvl--;
  }

  /* EOF */
  sqlite3_free(pCur->zPath);
  pCur->zPath = 0;
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
      sqlite3_result_text(ctx, &pCur->zPath[pCur->nBase], -1, SQLITE_TRANSIENT);
      break;
    }

    case 1: /* mode */
      sqlite3_result_int64(ctx, pCur->sStat.st_mode);
      break;

    case 2: /* mtime */
      sqlite3_result_int64(ctx, pCur->sStat.st_mtime);
      break;

    case 3: { /* data */
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
  return (pCur->zPath==0);
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

  fsdirResetCursor(pCur);

  if( idxNum==0 ){
    fsdirSetErrmsg(pCur, "table function fsdir requires an argument");
    return SQLITE_ERROR;
  }

  assert( argc==idxNum && (argc==1 || argc==2) );
  zDir = (const char*)sqlite3_value_text(argv[0]);
  if( zDir==0 ){
    fsdirSetErrmsg(pCur, "table function fsdir requires a non-NULL argument");
    return SQLITE_ERROR;
  }
  if( argc==2 ){
    pCur->zBase = (const char*)sqlite3_value_text(argv[1]);
  }
  if( pCur->zBase ){
    pCur->nBase = strlen(pCur->zBase)+1;
    pCur->zPath = sqlite3_mprintf("%s/%s", pCur->zBase, zDir);
  }else{
    pCur->zPath = sqlite3_mprintf("%s", zDir);
  }

  if( pCur->zPath==0 ){
    return SQLITE_NOMEM;
  }
  if( lstat(pCur->zPath, &pCur->sStat) ){
    fsdirSetErrmsg(pCur, "cannot stat file: %s", pCur->zPath);
    return SQLITE_ERROR;
  }

  return SQLITE_OK;
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
  int idx4 = -1;
  int idx5 = -1;

  const struct sqlite3_index_constraint *pConstraint;
  pConstraint = pIdxInfo->aConstraint;
  for(i=0; i<pIdxInfo->nConstraint; i++, pConstraint++){
    if( pConstraint->usable==0 ) continue;
    if( pConstraint->op!=SQLITE_INDEX_CONSTRAINT_EQ ) continue;
    if( pConstraint->iColumn==4 ) idx4 = i;
    if( pConstraint->iColumn==5 ) idx5 = i;
  }

  if( idx4<0 ){
    pIdxInfo->idxNum = 0;
    pIdxInfo->estimatedCost = (double)(((sqlite3_int64)1) << 50);
  }else{
    pIdxInfo->aConstraintUsage[idx4].omit = 1;
    pIdxInfo->aConstraintUsage[idx4].argvIndex = 1;
    if( idx5>=0 ){
      pIdxInfo->aConstraintUsage[idx5].omit = 1;
      pIdxInfo->aConstraintUsage[idx5].argvIndex = 2;
      pIdxInfo->idxNum = 2;
      pIdxInfo->estimatedCost = 10.0;
    }else{
      pIdxInfo->idxNum = 1;
      pIdxInfo->estimatedCost = 100.0;
    }
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

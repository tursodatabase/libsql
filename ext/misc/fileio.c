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
** writefile(), and eponymous virtual type "fsdir".
**
** WRITEFILE(FILE, DATA [, MODE [, MTIME]]):
**
**   If neither of the optional arguments is present, then this UDF
**   function writes blob DATA to file FILE. If successful, the number
**   of bytes written is returned. If an error occurs, NULL is returned.
**
**   If the first option argument - MODE - is present, then it must
**   be passed an integer value that corresponds to a POSIX mode
**   value (file type + permissions, as returned in the stat.st_mode
**   field by the stat() system call). Three types of files may
**   be written/created:
**
**     regular files:  (mode & 0170000)==0100000
**     symbolic links: (mode & 0170000)==0120000
**     directories:    (mode & 0170000)==0040000
**
**   For a directory, the DATA is ignored. For a symbolic link, it is
**   interpreted as text and used as the target of the link. For a
**   regular file, it is interpreted as a blob and written into the
**   named file. Regardless of the type of file, its permissions are
**   set to (mode & 0777) before returning.
**
**   If the optional MTIME argument is present, then it is interpreted
**   as an integer - the number of seconds since the unix epoch. The
**   modification-time of the target file is set to this value before
**   returning.
**
**   If three or more arguments are passed to this function and an
**   error is encountered, an exception is raised.
**
** READFILE(FILE):
**
**   Read and return the contents of file FILE (type blob) from disk.
**
** FSDIR:
**
**   Used as follows:
**
**     SELECT * FROM fsdir($path [, $dir]);
**
**   Parameter $path is an absolute or relative pathname. If the file that it
**   refers to does not exist, it is an error. If the path refers to a regular
**   file or symbolic link, it returns a single row. Or, if the path refers
**   to a directory, it returns one row for the directory, and one row for each
**   file within the hierarchy rooted at $path.
**
**   Each row has the following columns:
**
**     name:  Path to file or directory (text value).
**     mode:  Value of stat.st_mode for directory entry (an integer).
**     mtime: Value of stat.st_mtime for directory entry (an integer).
**     data:  For a regular file, a blob containing the file data. For a
**            symlink, a text value containing the text of the link. For a
**            directory, NULL.
**
**   If a non-NULL value is specified for the optional $dir parameter and
**   $path is a relative path, then $path is interpreted relative to $dir. 
**   And the paths returned in the "name" column of the table are also 
**   relative to directory $dir.
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
#include <errno.h>


#define FSDIR_SCHEMA "(name,mode,mtime,data,path HIDDEN,dir HIDDEN)"

/*
** Set the result stored by context ctx to a blob containing the 
** contents of file zName.
*/
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

/*
** Set the error message contained in context ctx to the results of
** vprintf(zFmt, ...).
*/
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
** Argument zFile is the name of a file that will be created and/or written
** by SQL function writefile(). This function ensures that the directory
** zFile will be written to exists, creating it if required. The permissions
** for any path components created by this function are set to (mode&0777).
**
** If an OOM condition is encountered, SQLITE_NOMEM is returned. Otherwise,
** SQLITE_OK is returned if the directory is successfully created, or
** SQLITE_ERROR otherwise.
*/
static int makeDirectory(
  const char *zFile,
  mode_t mode
){
  char *zCopy = sqlite3_mprintf("%s", zFile);
  int rc = SQLITE_OK;

  if( zCopy==0 ){
    rc = SQLITE_NOMEM;
  }else{
    int nCopy = strlen(zCopy);
    int i = 1;

    while( rc==SQLITE_OK ){
      struct stat sStat;
      int rc;

      for(; zCopy[i]!='/' && i<nCopy; i++);
      if( i==nCopy ) break;
      zCopy[i] = '\0';

      rc = stat(zCopy, &sStat);
      if( rc!=0 ){
        if( mkdir(zCopy, mode & 0777) ) rc = SQLITE_ERROR;
      }else{
        if( !S_ISDIR(sStat.st_mode) ) rc = SQLITE_ERROR;
      }
      zCopy[i] = '/';
      i++;
    }

    sqlite3_free(zCopy);
  }

  return rc;
}

/*
** This function does the work for the writefile() UDF. Refer to 
** header comments at the top of this file for details.
*/
static int writeFile(
  sqlite3_context *pCtx,          /* Context to return bytes written in */
  const char *zFile,              /* File to write */
  sqlite3_value *pData,           /* Data to write */
  mode_t mode,                    /* MODE parameter passed to writefile() */
  sqlite3_int64 mtime             /* MTIME parameter (or -1 to not set time) */
){
  if( S_ISLNK(mode) ){
    const char *zTo = (const char*)sqlite3_value_text(pData);
    if( symlink(zTo, zFile)<0 ) return 1;
  }else{
    if( S_ISDIR(mode) ){
      if( mkdir(zFile, mode) ){
        /* The mkdir() call to create the directory failed. This might not
        ** be an error though - if there is already a directory at the same
        ** path and either the permissions already match or can be changed
        ** to do so using chmod(), it is not an error.  */
        struct stat sStat;
        if( errno!=EEXIST
         || 0!=stat(zFile, &sStat)
         || !S_ISDIR(sStat.st_mode)
         || ((sStat.st_mode&0777)!=(mode&0777) && 0!=chmod(zFile, mode&0777))
        ){
          return 1;
        }
      }
    }else{
      sqlite3_int64 nWrite = 0;
      const char *z;
      int rc = 0;
      FILE *out = fopen(zFile, "wb");
      if( out==0 ) return 1;
      z = (const char*)sqlite3_value_blob(pData);
      if( z ){
        sqlite3_int64 n = fwrite(z, 1, sqlite3_value_bytes(pData), out);
        nWrite = sqlite3_value_bytes(pData);
        if( nWrite!=n ){
          rc = 1;
        }
      }
      fclose(out);
      if( rc==0 && mode && chmod(zFile, mode & 0777) ){
        rc = 1;
      }
      if( rc ) return 2;
      sqlite3_result_int64(pCtx, nWrite);
    }
  }

  if( mtime>=0 ){
    struct timespec times[2];
    times[0].tv_nsec = times[1].tv_nsec = 0;
    times[0].tv_sec = time(0);
    times[1].tv_sec = mtime;
    if( utimensat(AT_FDCWD, zFile, times, AT_SYMLINK_NOFOLLOW) ){
      return 1;
    }
  }

  return 0;
}

/*
** Implementation of the "writefile(W,X[,Y[,Z]]])" SQL function.  
** Refer to header comments at the top of this file for details.
*/
static void writefileFunc(
  sqlite3_context *context,
  int argc,
  sqlite3_value **argv
){
  const char *zFile;
  mode_t mode = 0;
  int res;
  sqlite3_int64 mtime = -1;

  if( argc<2 || argc>4 ){
    sqlite3_result_error(context, 
        "wrong number of arguments to function writefile()", -1
    );
    return;
  }

  zFile = (const char*)sqlite3_value_text(argv[0]);
  if( zFile==0 ) return;
  if( argc>=3 ){
    mode = sqlite3_value_int(argv[2]);
  }
  if( argc==4 ){
    mtime = sqlite3_value_int64(argv[3]);
  }

  res = writeFile(context, zFile, argv[1], mode, mtime);
  if( res==1 && errno==ENOENT ){
    if( makeDirectory(zFile, mode)==SQLITE_OK ){
      res = writeFile(context, zFile, argv[1], mode, mtime);
    }
  }

  if( argc>2 && res!=0 ){
    if( S_ISLNK(mode) ){
      ctxErrorMsg(context, "failed to create symlink: %s", zFile);
    }else if( S_ISDIR(mode) ){
      ctxErrorMsg(context, "failed to create directory: %s", zFile);
    }else{
      ctxErrorMsg(context, "failed to write file: %s", zFile);
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

/*
** Reset a cursor back to the state it was in when first returned
** by fsdirOpen().
*/
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

/*
** Set the error message for the virtual table associated with cursor
** pCur to the results of vprintf(zFmt, ...).
*/
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

/*
** Register the "fsdir" virtual table.
*/
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

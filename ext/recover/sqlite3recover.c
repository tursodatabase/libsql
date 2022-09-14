/*
** 2022-08-27
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
*/


#include "sqlite3recover.h"
#include <assert.h>
#include <string.h>

/*
** Declaration for public API function in file dbdata.c. This may be called
** with NULL as the final two arguments to register the sqlite_dbptr and
** sqlite_dbdata virtual tables with a database handle.
*/
#ifdef _WIN32
__declspec(dllexport)
#endif
int sqlite3_dbdata_init(sqlite3*, char**, const sqlite3_api_routines*);

typedef unsigned int u32;
typedef sqlite3_int64 i64;

typedef struct RecoverTable RecoverTable;
typedef struct RecoverColumn RecoverColumn;

/*
** When recovering rows of data that can be associated with table
** definitions recovered from the sqlite_schema table, each table is
** represented by an instance of the following object.
**
** iRoot:
**   The root page in the original database. Not necessarily (and usually
**   not) the same in the recovered database.
**
** zTab:
**   Name of the table.
**
** nCol/aCol[]:
**   aCol[] is an array of nCol columns. In the order in which they appear 
**   in the table.
**
** bIntkey:
**   Set to true for intkey tables, false for WITHOUT ROWID.
**
** iRowidBind:
**   Each column in the aCol[] array has associated with it the index of
**   the bind parameter its values will be bound to in the INSERT statement
**   used to construct the output database. If the table does has a rowid
**   but not an INTEGER PRIMARY KEY column, then iRowidBind contains the
**   index of the bind paramater to which the rowid value should be bound.
**   Otherwise, it contains -1. If the table does contain an INTEGER PRIMARY 
**   KEY column, then the rowid value should be bound to the index associated
**   with the column.
**
** pNext:
**   All RecoverTable objects used by the recovery operation are allocated
**   and populated as part of creating the recovered database schema in
**   the output database, before any non-schema data are recovered. They
**   are then stored in a singly-linked list linked by this variable beginning
**   at sqlite3_recover.pTblList.
*/
struct RecoverTable {
  u32 iRoot;                      /* Root page in original database */
  char *zTab;                     /* Name of table */
  int nCol;                       /* Number of columns in table */
  RecoverColumn *aCol;            /* Array of columns */
  int bIntkey;                    /* True for intkey, false for without rowid */
  int iRowidBind;                 /* If >0, bind rowid to INSERT here */
  RecoverTable *pNext;
};

/*
** Each database column is represented by an instance of the following object
** stored in the RecoverTable.aCol[] array of the associated table.
**
** iField:
**   The index of the associated field within database records. Or -1 if
**   there is no associated field (e.g. for virtual generated columns).
**
** iBind:
**   The bind index of the INSERT statement to bind this columns values
**   to. Or 0 if there is no such index (iff (iField<0)).
**
** bIPK:
**   True if this is the INTEGER PRIMARY KEY column.
**
** zCol:
**   Name of column.
**
** eHidden:
**   A RECOVER_EHIDDEN_* constant value (see below for interpretation of each).
*/
struct RecoverColumn {
  int iField;                     /* Field in record on disk */
  int iBind;                      /* Binding to use in INSERT */
  int bIPK;                       /* True for IPK column */
  char *zCol;
  int eHidden;
};

#define RECOVER_EHIDDEN_NONE    0      /* Normal database column */
#define RECOVER_EHIDDEN_HIDDEN  1      /* Column is __HIDDEN__ */
#define RECOVER_EHIDDEN_VIRTUAL 2      /* Virtual generated column */
#define RECOVER_EHIDDEN_STORED  3      /* Stored generated column */

/*
** Bitmap object used to track pages in the input database. Allocated
** and manipulated only by the following functions:
**
**     recoverBitmapAlloc()
**     recoverBitmapFree()
**     recoverBitmapSet()
**     recoverBitmapQuery()
**
** nPg:
**   Largest page number that may be stored in the bitmap. The range
**   of valid keys is 1 to nPg, inclusive.
**
** aElem[]:
**   Array large enough to contain a bit for each key. For key value
**   iKey, the associated bit is the bit (iKey%32) of aElem[iKey/32].
**   In other words, the following is true if bit iKey is set, or 
**   false if it is clear:
**
**       (aElem[iKey/32] & (1 << (iKey%32))) ? 1 : 0
*/
typedef struct RecoverBitmap RecoverBitmap;
struct RecoverBitmap {
  i64 nPg;                        /* Size of bitmap */
  u32 aElem[0];                   /* Array of 32-bit bitmasks */
};

/*
** Main recover handle structure.
*/
struct sqlite3_recover {
  /* Copies of sqlite3_recover_init[_sql]() parameters */
  sqlite3 *dbIn;                  /* Input database */
  char *zDb;                      /* Name of input db ("main" etc.) */
  char *zUri;                     /* URI for output database */
  void *pSqlCtx;                  /* SQL callback context */
  int (*xSql)(void*,const char*); /* Pointer to SQL callback function */

  /* Values configured by sqlite3_recover_config() */
  char *zStateDb;                 /* State database to use (or NULL) */
  char *zLostAndFound;            /* Name of lost-and-found table (or NULL) */
  int bFreelistCorrupt;           /* SQLITE_RECOVER_FREELIST_CORRUPT setting */
  int bRecoverRowid;              /* SQLITE_RECOVER_ROWIDS setting */

  /* Error code and error message */
  int errCode;                    /* For sqlite3_recover_errcode() */
  char *zErrMsg;                  /* For sqlite3_recover_errmsg() */

  /* Fields used within sqlite3_recover_run() */
  int bRun;                       /* True once _recover_run() has been called */
  sqlite3 *dbOut;                 /* Output database */
  sqlite3_stmt *pGetPage;         /* SELECT against input db sqlite_dbdata */
  RecoverTable *pTblList;         /* List of tables recovered from schem */
  RecoverBitmap *pUsed;           /* Used by recoverLostAndFound() */
};

/* 
** Default value for SQLITE_RECOVER_ROWIDS (sqlite3_recover.bRecoverRowid).
*/
#define RECOVER_ROWID_DEFAULT 1

/*
** Like strlen(). But handles NULL pointer arguments.
*/
static int recoverStrlen(const char *zStr){
  int nRet = 0;
  if( zStr ){
    while( zStr[nRet] ) nRet++;
  }
  return nRet;
}

/*
** This function is a no-op if the recover handle passed as the first 
** argument already contains an error (if p->errCode!=SQLITE_OK). 
**
** Otherwise, an attempt is made to allocate, zero and return a buffer nByte
** bytes in size. If successful, a pointer to the new buffer is returned. Or,
** if an OOM error occurs, NULL is returned and the handle error code
** (p->errCode) set to SQLITE_NOMEM.
*/
static void *recoverMalloc(sqlite3_recover *p, i64 nByte){
  void *pRet = 0;
  assert( nByte>0 );
  if( p->errCode==SQLITE_OK ){
    pRet = sqlite3_malloc64(nByte);
    if( pRet ){
      memset(pRet, 0, nByte);
    }else{
      p->errCode = SQLITE_NOMEM;
    }
  }
  return pRet;
}

/*
** Set the error code and error message for the recover handle passed as
** the first argument. The error code is set to the value of parameter
** errCode.
**
** Parameter zFmt must be a printf() style formatting string. The handle 
** error message is set to the result of using any trailing arguments for 
** parameter substitutions in the formatting string.
**
** For example:
**
**   recoverError(p, SQLITE_ERROR, "no such table: %s", zTablename);
*/
static int recoverError(
  sqlite3_recover *p, 
  int errCode, 
  const char *zFmt, ...
){
  char *z = 0;
  va_list ap;
  va_start(ap, zFmt);
  if( zFmt ){
    z = sqlite3_vmprintf(zFmt, ap);
    va_end(ap);
  }
  sqlite3_free(p->zErrMsg);
  p->zErrMsg = z;
  p->errCode = errCode;
  return errCode;
}


/*
** This function is a no-op if p->errCode is initially other than SQLITE_OK.
** In this case it returns NULL.
**
** Otherwise, an attempt is made to allocate and return a bitmap object
** large enough to store a bit for all page numbers between 1 and nPg,
** inclusive. The bitmap is initially zeroed.
*/
static RecoverBitmap *recoverBitmapAlloc(sqlite3_recover *p, i64 nPg){
  int nElem = (nPg+1+31) / 32;
  int nByte = sizeof(RecoverBitmap) + nElem*sizeof(u32);
  RecoverBitmap *pRet = (RecoverBitmap*)recoverMalloc(p, nByte);

  if( pRet ){
    pRet->nPg = nPg;
  }
  return pRet;
}

/*
** Free a bitmap object allocated by recoverBitmapAlloc().
*/
static void recoverBitmapFree(RecoverBitmap *pMap){
  sqlite3_free(pMap);
}

/*
** Set the bit associated with page iPg in bitvec pMap.
*/
static void recoverBitmapSet(RecoverBitmap *pMap, i64 iPg){
  if( iPg<=pMap->nPg ){
    int iElem = (iPg / 32);
    int iBit = (iPg % 32);
    pMap->aElem[iElem] |= (((u32)1) << iBit);
  }
}

/*
** Query bitmap object pMap for the state of the bit associated with page
** iPg. Return 1 if it is set, or 0 otherwise.
*/
static int recoverBitmapQuery(RecoverBitmap *pMap, i64 iPg){
  int ret = 1;
  if( iPg<=pMap->nPg ){
    int iElem = (iPg / 32);
    int iBit = (iPg % 32);
    ret = (pMap->aElem[iElem] & (((u32)1) << iBit)) ? 1 : 0;
  }
  return ret;
}

/*
** Set the recover handle error to the error code and message returned by
** calling sqlite3_errcode() and sqlite3_errmsg(), respectively, on database
** handle db.
*/
static int recoverDbError(sqlite3_recover *p, sqlite3 *db){
  return recoverError(p, sqlite3_errcode(db), "%s", sqlite3_errmsg(db));
}

/*
** This function is a no-op if recover handle p already contains an error
** (if p->errCode!=SQLITE_OK). 
**
** Otherwise, it attempts to prepare the SQL statement in zSql against
** database handle db. If successful, the statement handle is returned.
** Or, if an error occurs, NULL is returned and an error left in the
** recover handle.
*/
static sqlite3_stmt *recoverPrepare(
  sqlite3_recover *p,
  sqlite3 *db, 
  const char *zSql
){
  sqlite3_stmt *pStmt = 0;
  if( p->errCode==SQLITE_OK ){
    if( sqlite3_prepare_v2(db, zSql, -1, &pStmt, 0) ){
      recoverDbError(p, db);
    }
  }
  return pStmt;
}

/*
** This function is a no-op if recover handle p already contains an error
** (if p->errCode!=SQLITE_OK). 
**
** Otherwise, argument zFmt is used as a printf() style format string,
** along with any trailing arguments, to create an SQL statement. This
** SQL statement is prepared against database handle db and, if successful,
** the statment handle returned. Or, if an error occurs - either during
** the printf() formatting or when preparing the resulting SQL - an
** error code and message are left in the recover handle.
*/
static sqlite3_stmt *recoverPreparePrintf(
  sqlite3_recover *p,
  sqlite3 *db, 
  const char *zFmt, ...
){
  sqlite3_stmt *pStmt = 0;
  if( p->errCode==SQLITE_OK ){
    va_list ap;
    char *z;
    va_start(ap, zFmt);
    z = sqlite3_vmprintf(zFmt, ap);
    va_end(ap);
    if( z==0 ){
      p->errCode = SQLITE_NOMEM;
    }else{
      pStmt = recoverPrepare(p, db, z);
      sqlite3_free(z);
    }
  }
  return pStmt;
}

/*
** Reset SQLite statement handle pStmt. If the call to sqlite3_reset() 
** indicates that an error occurred, and there is not already an error
** in the recover handle passed as the first argument, set the error
** code and error message appropriately.
**
** This function returns a copy of the statement handle pointer passed
** as the second argument.
*/
static sqlite3_stmt *recoverReset(sqlite3_recover *p, sqlite3_stmt *pStmt){
  int rc = sqlite3_reset(pStmt);
  if( rc!=SQLITE_OK && p->errCode==SQLITE_OK ){
    recoverDbError(p, sqlite3_db_handle(pStmt));
  }
  return pStmt;
}

/*
** Finalize SQLite statement handle pStmt. If the call to sqlite3_reset() 
** indicates that an error occurred, and there is not already an error
** in the recover handle passed as the first argument, set the error
** code and error message appropriately.
*/
static void recoverFinalize(sqlite3_recover *p, sqlite3_stmt *pStmt){
  sqlite3 *db = sqlite3_db_handle(pStmt);
  int rc = sqlite3_finalize(pStmt);
  if( rc!=SQLITE_OK && p->errCode==SQLITE_OK ){
    recoverDbError(p, db);
  }
}

/*
** This function is a no-op if recover handle p already contains an error
** (if p->errCode!=SQLITE_OK). A copy of p->errCode is returned in this 
** case.
**
** Otherwise, execute SQL script zSql. If successful, return SQLITE_OK.
** Or, if an error occurs, leave an error code and message in the recover
** handle and return a copy of the error code.
*/
static int recoverExec(sqlite3_recover *p, sqlite3 *db, const char *zSql){
  if( p->errCode==SQLITE_OK ){
    int rc = sqlite3_exec(db, zSql, 0, 0, 0);
    if( rc ){
      recoverDbError(p, db);
    }
  }
  return p->errCode;
}

/*
** Bind the value pVal to parameter iBind of statement pStmt. Leave an
** error in the recover handle passed as the first argument if an error
** (e.g. an OOM) occurs.
*/
static void recoverBindValue(
  sqlite3_recover *p, 
  sqlite3_stmt *pStmt, 
  int iBind, 
  sqlite3_value *pVal
){
  if( p->errCode==SQLITE_OK ){
    int rc = sqlite3_bind_value(pStmt, iBind, pVal);
    if( rc ) recoverError(p, rc, 0);
  }
}

/*
** This function is a no-op if recover handle p already contains an error
** (if p->errCode!=SQLITE_OK). NULL is returned in this case.
**
** Otherwise, an attempt is made to interpret zFmt as a printf() style
** formatting string and the result of using the trailing arguments for
** parameter substitution with it written into a buffer obtained from
** sqlite3_malloc(). If successful, a pointer to the buffer is returned.
** It is the responsibility of the caller to eventually free the buffer
** using sqlite3_free().
**
** Or, if an error occurs, an error code and message is left in the recover
** handle and NULL returned.
*/
static char *recoverMPrintf(sqlite3_recover *p, const char *zFmt, ...){
  va_list ap;
  char *z;
  va_start(ap, zFmt);
  z = sqlite3_vmprintf(zFmt, ap);
  va_end(ap);
  if( p->errCode==SQLITE_OK ){
    if( z==0 ) p->errCode = SQLITE_NOMEM;
  }else{
    sqlite3_free(z);
    z = 0;
  }
  return z;
}

/*
** This function is a no-op if recover handle p already contains an error
** (if p->errCode!=SQLITE_OK). Zero is returned in this case.
**
** Otherwise, execute "PRAGMA page_count" against the input database. If
** successful, return the integer result. Or, if an error occurs, leave an
** error code and error message in the sqlite3_recover handle and return
** zero.
*/
static i64 recoverPageCount(sqlite3_recover *p){
  i64 nPg = 0;
  if( p->errCode==SQLITE_OK ){
    sqlite3_stmt *pStmt = 0;
    pStmt = recoverPreparePrintf(p, p->dbIn, "PRAGMA %Q.page_count", p->zDb);
    if( pStmt ){
      sqlite3_step(pStmt);
      nPg = sqlite3_column_int64(pStmt, 0);
    }
    recoverFinalize(p, pStmt);
  }
  return nPg;
}

/*
** Implementation of SQL scalar function "read_i32". The first argument to 
** this function must be a blob. The second a non-negative integer. This 
** function reads and returns a 32-bit big-endian integer from byte
** offset (4*<arg2>) of the blob.
**
**     SELECT read_i32(<blob>, <idx>)
*/
static void recoverReadI32(
  sqlite3_context *context, 
  int argc, 
  sqlite3_value **argv
){
  const unsigned char *pBlob;
  int nBlob;
  int iInt;

  assert( argc==2 );
  nBlob = sqlite3_value_bytes(argv[0]);
  pBlob = (const unsigned char*)sqlite3_value_blob(argv[0]);
  iInt = sqlite3_value_int(argv[1]) & 0xFFFF;

  if( (iInt+1)*4<=nBlob ){
    const unsigned char *a = &pBlob[iInt*4];
    i64 iVal = ((i64)a[0]<<24)
             + ((i64)a[1]<<16)
             + ((i64)a[2]<< 8)
             + ((i64)a[3]<< 0);
    sqlite3_result_int64(context, iVal);
  }
}

/*
** Implementation of SQL scalar function "page_is_used". This function
** is used as part of the procedure for locating orphan rows for the
** lost-and-found table, and it depends on those routines having populated
** the sqlite3_recover.pUsed variable.
**
** The only argument to this function is a page-number. It returns true 
** if the page has already been used somehow during data recovery, or false
** otherwise.
**
**     SELECT page_is_used(<pgno>);
*/
static void recoverPageIsUsed(
  sqlite3_context *pCtx,
  int nArg,
  sqlite3_value **apArg
){
  sqlite3_recover *p = (sqlite3_recover*)sqlite3_user_data(pCtx);
  i64 pgno = sqlite3_value_int64(apArg[0]);
  sqlite3_stmt *pStmt = 0;
  int bRet;

  assert( nArg==1 );
  bRet = recoverBitmapQuery(p->pUsed, pgno);
  sqlite3_result_int(pCtx, bRet);
}

/*
** The implementation of a user-defined SQL function invoked by the 
** sqlite_dbdata and sqlite_dbptr virtual table modules to access pages
** of the database being recovered.
**
** This function always takes a single integer argument. If the argument
** is zero, then the value returned is the number of pages in the db being
** recovered. If the argument is greater than zero, it is a page number. 
** The value returned in this case is an SQL blob containing the data for 
** the identified page of the db being recovered. e.g.
**
**     SELECT getpage(0);       -- return number of pages in db
**     SELECT getpage(4);       -- return page 4 of db as a blob of data 
*/
static void recoverGetPage(
  sqlite3_context *pCtx,
  int nArg,
  sqlite3_value **apArg
){
  sqlite3_recover *p = (sqlite3_recover*)sqlite3_user_data(pCtx);
  i64 pgno = sqlite3_value_int64(apArg[0]);
  sqlite3_stmt *pStmt = 0;

  assert( nArg==1 );
  if( pgno==0 ){
    i64 nPg = recoverPageCount(p);
    sqlite3_result_int64(pCtx, nPg);
    return;
  }else{
    if( p->pGetPage==0 ){
      pStmt = p->pGetPage = recoverPreparePrintf(
          p, p->dbIn, "SELECT data FROM sqlite_dbpage(%Q) WHERE pgno=?", p->zDb
      );
    }else{
      pStmt = p->pGetPage;
    }

    if( pStmt ){
      int rc = SQLITE_OK;
      sqlite3_bind_int64(pStmt, 1, pgno);
      if( SQLITE_ROW==sqlite3_step(pStmt) ){
        sqlite3_result_value(pCtx, sqlite3_column_value(pStmt, 0));
      }
      recoverReset(p, pStmt);
    }
  }

  if( p->errCode ){
    if( p->zErrMsg ) sqlite3_result_error(pCtx, p->zErrMsg, -1);
    sqlite3_result_error_code(pCtx, p->errCode);
  }
}

/*
** Find a string that is not found anywhere in z[].  Return a pointer
** to that string.
**
** Try to use zA and zB first.  If both of those are already found in z[]
** then make up some string and store it in the buffer zBuf.
*/
static const char *recoverUnusedString(
  const char *z,                    /* Result must not appear anywhere in z */
  const char *zA, const char *zB,   /* Try these first */
  char *zBuf                        /* Space to store a generated string */
){
  unsigned i = 0;
  if( strstr(z, zA)==0 ) return zA;
  if( strstr(z, zB)==0 ) return zB;
  do{
    sqlite3_snprintf(20,zBuf,"(%s%u)", zA, i++);
  }while( strstr(z,zBuf)!=0 );
  return zBuf;
}

/*
** Implementation of scalar SQL function "escape_crnl".  The argument passed to
** this function is the output of built-in function quote(). If the first
** character of the input is "'", indicating that the value passed to quote()
** was a text value, then this function searches the input for "\n" and "\r"
** characters and adds a wrapper similar to the following:
**
**   replace(replace(<input>, '\n', char(10), '\r', char(13));
**
** Or, if the first character of the input is not "'", then a copy of the input
** is returned.
*/
static void recoverEscapeCrnl(
  sqlite3_context *context, 
  int argc, 
  sqlite3_value **argv
){
  const char *zText = (const char*)sqlite3_value_text(argv[0]);
  if( zText && zText[0]=='\'' ){
    int nText = sqlite3_value_bytes(argv[0]);
    int i;
    char zBuf1[20];
    char zBuf2[20];
    const char *zNL = 0;
    const char *zCR = 0;
    int nCR = 0;
    int nNL = 0;

    for(i=0; zText[i]; i++){
      if( zNL==0 && zText[i]=='\n' ){
        zNL = recoverUnusedString(zText, "\\n", "\\012", zBuf1);
        nNL = (int)strlen(zNL);
      }
      if( zCR==0 && zText[i]=='\r' ){
        zCR = recoverUnusedString(zText, "\\r", "\\015", zBuf2);
        nCR = (int)strlen(zCR);
      }
    }

    if( zNL || zCR ){
      int iOut = 0;
      i64 nMax = (nNL > nCR) ? nNL : nCR;
      i64 nAlloc = nMax * nText + (nMax+64)*2;
      char *zOut = (char*)sqlite3_malloc64(nAlloc);
      if( zOut==0 ){
        sqlite3_result_error_nomem(context);
        return;
      }

      if( zNL && zCR ){
        memcpy(&zOut[iOut], "replace(replace(", 16);
        iOut += 16;
      }else{
        memcpy(&zOut[iOut], "replace(", 8);
        iOut += 8;
      }
      for(i=0; zText[i]; i++){
        if( zText[i]=='\n' ){
          memcpy(&zOut[iOut], zNL, nNL);
          iOut += nNL;
        }else if( zText[i]=='\r' ){
          memcpy(&zOut[iOut], zCR, nCR);
          iOut += nCR;
        }else{
          zOut[iOut] = zText[i];
          iOut++;
        }
      }

      if( zNL ){
        memcpy(&zOut[iOut], ",'", 2); iOut += 2;
        memcpy(&zOut[iOut], zNL, nNL); iOut += nNL;
        memcpy(&zOut[iOut], "', char(10))", 12); iOut += 12;
      }
      if( zCR ){
        memcpy(&zOut[iOut], ",'", 2); iOut += 2;
        memcpy(&zOut[iOut], zCR, nCR); iOut += nCR;
        memcpy(&zOut[iOut], "', char(13))", 12); iOut += 12;
      }

      sqlite3_result_text(context, zOut, iOut, SQLITE_TRANSIENT);
      sqlite3_free(zOut);
      return;
    }
  }

  sqlite3_result_value(context, argv[0]);
}

/*
** This function is a no-op if recover handle p already contains an error
** (if p->errCode!=SQLITE_OK). A copy of the error code is returned in
** this case. 
**
** Otherwise, an attempt is made to open the output database, attach
** and create the schema of the temporary database used to store
** intermediate data, and to register all required user functions and
** virtual table modules with the output handle.
**
** If no error occurs, SQLITE_OK is returned. Otherwise, an error code
** and error message are left in the recover handle and a copy of the
** error code returned.
*/
static int recoverOpenOutput(sqlite3_recover *p){
  struct Func {
    const char *zName;
    int nArg;
    void (*xFunc)(sqlite3_context*,int,sqlite3_value **);
  } aFunc[] = {
    { "getpage", 1, recoverGetPage },
    { "page_is_used", 1, recoverPageIsUsed },
    { "read_i32", 2, recoverReadI32 },
    { "escape_crnl", 1, recoverEscapeCrnl },
  };

  const int flags = SQLITE_OPEN_URI|SQLITE_OPEN_CREATE|SQLITE_OPEN_READWRITE;
  sqlite3 *db = 0;                /* New database handle */
  int ii;                         /* For iterating through aFunc[] */

  assert( p->dbOut==0 );

  if( p->errCode==SQLITE_OK ){
    if( sqlite3_open_v2(p->zUri, &db, flags, 0) ){
      recoverDbError(p, db);
    }else{
      char *zSql = recoverMPrintf(p, "ATTACH %Q AS recovery;", p->zStateDb);
      recoverExec(p, db, zSql);
      recoverExec(p, db,
          "PRAGMA writable_schema = 1;"
          "CREATE TABLE recovery.map(pgno INTEGER PRIMARY KEY, parent INT);" 
          "CREATE TABLE recovery.schema(type, name, tbl_name, rootpage, sql);"
          );
      sqlite3_free(zSql);
    }
  }

  /* Register the sqlite_dbdata and sqlite_dbptr virtual table modules.
  ** These two are registered with the output database handle - this
  ** module depends on the input handle supporting the sqlite_dbpage
  ** virtual table only.  */
  if( p->errCode==SQLITE_OK ){
    p->errCode = sqlite3_dbdata_init(db, 0, 0);
  }

  /* Register the custom user-functions with the output handle. */
  for(ii=0; p->errCode==SQLITE_OK && ii<sizeof(aFunc)/sizeof(aFunc[0]); ii++){
    p->errCode = sqlite3_create_function(db, aFunc[ii].zName, 
        aFunc[ii].nArg, SQLITE_UTF8, (void*)p, aFunc[ii].xFunc, 0, 0
    );
  }

  /* Truncate the output database to 0 pages in size. This is done by 
  ** opening a new, empty, temp db, then using the backup API to clobber 
  ** any existing output db with a copy of it. */
  if( p->errCode==SQLITE_OK ){
    sqlite3 *db2 = 0;
    int rc = sqlite3_open("", &db2);
    if( rc==SQLITE_OK ){
      sqlite3_backup *pBackup = sqlite3_backup_init(db, "main", db2, "main");
      if( pBackup ){
        sqlite3_backup_step(pBackup, -1);
        p->errCode = sqlite3_backup_finish(pBackup);
      }else{
        recoverDbError(p, db);
      }
    }else{
      recoverDbError(p, db2);
    }
    sqlite3_close(db2);
  }

  p->dbOut = db;
  return p->errCode;
}

/*
** This function is a no-op if recover handle p already contains an error
** (if p->errCode!=SQLITE_OK). A copy of the error code is returned in
** this case. 
**
** Otherwise, attempt to populate temporary table "recovery.schema" with the
** parts of the database schema that can be extracted from the input database.
**
** If no error occurs, SQLITE_OK is returned. Otherwise, an error code
** and error message are left in the recover handle and a copy of the
** error code returned. It is not considered an error if part of all of
** the database schema cannot be recovered due to corruption.
*/
static int recoverCacheSchema(sqlite3_recover *p){
  return recoverExec(p, p->dbOut,
    "WITH RECURSIVE pages(p) AS ("
    "  SELECT 1"
    "    UNION"
    "  SELECT child FROM sqlite_dbptr('getpage()'), pages WHERE pgno=p"
    ")"
    "INSERT INTO recovery.schema SELECT"
    "  max(CASE WHEN field=0 THEN value ELSE NULL END),"
    "  max(CASE WHEN field=1 THEN value ELSE NULL END),"
    "  max(CASE WHEN field=2 THEN value ELSE NULL END),"
    "  max(CASE WHEN field=3 THEN value ELSE NULL END),"
    "  max(CASE WHEN field=4 THEN value ELSE NULL END)"
    "FROM sqlite_dbdata('getpage()') WHERE pgno IN ("
    "  SELECT p FROM pages"
    ") GROUP BY pgno, cell"
  );
}

/*
** This function is a no-op if recover handle p already contains an error
** (if p->errCode!=SQLITE_OK).
**
** Otherwise, argument zName must be the name of a table that has just been
** created in the output database. This function queries the output db
** for the schema of said table, and creates a RecoverTable object to
** store the schema in memory. The new RecoverTable object is linked into
** the list at sqlite3_recover.pTblList.
**
** Parameter iRoot must be the root page of table zName in the INPUT 
** database.
*/
static void recoverAddTable(
  sqlite3_recover *p, 
  const char *zName,              /* Name of table created in output db */
  i64 iRoot                       /* Root page of same table in INPUT db */
){
  sqlite3_stmt *pStmt = recoverPreparePrintf(p, p->dbOut, 
      "PRAGMA table_xinfo(%Q)", zName
  );

  if( pStmt ){
    int iPk = -1;
    int iBind = 1;
    RecoverTable *pNew = 0;
    int nCol = 0;
    int nName = recoverStrlen(zName);
    int nByte = 0;
    while( sqlite3_step(pStmt)==SQLITE_ROW ){
      nCol++;
      nByte += (sqlite3_column_bytes(pStmt, 1)+1);
    }
    nByte += sizeof(RecoverTable) + nCol*sizeof(RecoverColumn) + nName+1;
    recoverReset(p, pStmt);

    pNew = recoverMalloc(p, nByte);
    if( pNew ){
      int i = 0;
      int iField = 0;
      char *csr = 0;
      pNew->aCol = (RecoverColumn*)&pNew[1];
      pNew->zTab = csr = (char*)&pNew->aCol[nCol];
      pNew->nCol = nCol;
      pNew->iRoot = iRoot;
      memcpy(csr, zName, nName);
      csr += nName+1;

      for(i=0; sqlite3_step(pStmt)==SQLITE_ROW; i++){
        int iPKF = sqlite3_column_int(pStmt, 5);
        int n = sqlite3_column_bytes(pStmt, 1);
        const char *z = (const char*)sqlite3_column_text(pStmt, 1);
        const char *zType = (const char*)sqlite3_column_text(pStmt, 2);
        int eHidden = sqlite3_column_int(pStmt, 6);

        if( iPk==-1 && iPKF==1 && !sqlite3_stricmp("integer", zType) ) iPk = i;
        if( iPKF>1 ) iPk = -2;
        pNew->aCol[i].zCol = csr;
        pNew->aCol[i].eHidden = eHidden;
        if( eHidden==RECOVER_EHIDDEN_VIRTUAL ){
          pNew->aCol[i].iField = -1;
        }else{
          pNew->aCol[i].iField = iField++;
        }
        if( eHidden!=RECOVER_EHIDDEN_VIRTUAL
         && eHidden!=RECOVER_EHIDDEN_STORED
        ){
          pNew->aCol[i].iBind = iBind++;
        }
        memcpy(csr, z, n);
        csr += (n+1);
      }

      pNew->pNext = p->pTblList;
      p->pTblList = pNew;
      pNew->bIntkey = 1;
    }

    recoverFinalize(p, pStmt);

    pStmt = recoverPreparePrintf(p, p->dbOut, "PRAGMA index_xinfo(%Q)", zName);
    while( pStmt && sqlite3_step(pStmt)==SQLITE_ROW ){
      int iField = sqlite3_column_int(pStmt, 0);
      int iCol = sqlite3_column_int(pStmt, 1);

      assert( iField<pNew->nCol && iCol<pNew->nCol );
      pNew->aCol[iCol].iField = iField;

      pNew->bIntkey = 0;
      iPk = -2;
    }
    recoverFinalize(p, pStmt);

    if( p->errCode==SQLITE_OK ){
      if( iPk>=0 ){
        pNew->aCol[iPk].bIPK = 1;
      }else if( pNew->bIntkey ){
        pNew->iRowidBind = iBind++;
      }
    }
  }
}

/*
** If this recover handle is not in SQL callback mode (i.e. was not created 
** using sqlite3_recover_init_sql()) of if an error has already occurred, 
** this function is a no-op. Otherwise, issue a callback with SQL statement
** zSql as the parameter. 
**
** If the callback returns non-zero, set the recover handle error code to
** the value returned (so that the caller will abandon processing).
*/
static void recoverSqlCallback(sqlite3_recover *p, const char *zSql){
  if( p->errCode==SQLITE_OK && p->xSql ){
    int res = p->xSql(p->pSqlCtx, zSql);
    if( res ){
      recoverError(p, SQLITE_ERROR, "callback returned an error - %d", res);
    }
  }
}

/*
** This function is called after recoverCacheSchema() has cached those parts
** of the input database schema that could be recovered in temporary table
** "recovery.schema". This function creates in the output database copies
** of all parts of that schema that must be created before the tables can
** be populated. Specifically, this means:
**
**     * all tables that are not VIRTUAL, and
**     * UNIQUE indexes.
**
** If the recovery handle uses SQL callbacks, then callbacks containing
** the associated "CREATE TABLE" and "CREATE INDEX" statements are made.
**
** Additionally, records are added to the sqlite_schema table of the
** output database for any VIRTUAL tables. The CREATE VIRTUAL TABLE
** records are written directly to sqlite_schema, not actually executed.
** If the handle is in SQL callback mode, then callbacks are invoked 
** with equivalent SQL statements.
*/
static int recoverWriteSchema1(sqlite3_recover *p){
  sqlite3_stmt *pSelect = 0;
  sqlite3_stmt *pTblname = 0;

  pSelect = recoverPrepare(p, p->dbOut,
      "WITH dbschema(rootpage, name, sql, tbl, isVirtual, isUnique) AS ("
      "  SELECT rootpage, name, sql, "
      "    type='table', "
      "    sql LIKE 'create virtual%',"
      "    (type='index' AND sql LIKE '%unique%')"
      "  FROM recovery.schema"
      ")"
      "SELECT rootpage, tbl, isVirtual, name, sql"
      " FROM dbschema "
      "  WHERE tbl OR isUnique"
      "  ORDER BY tbl DESC, name=='sqlite_sequence' DESC"
  );

  pTblname = recoverPrepare(p, p->dbOut,
      "SELECT name FROM sqlite_schema "
      "WHERE type='table' ORDER BY rowid DESC LIMIT 1"
  );

  if( pSelect ){
    while( sqlite3_step(pSelect)==SQLITE_ROW ){
      i64 iRoot = sqlite3_column_int64(pSelect, 0);
      int bTable = sqlite3_column_int(pSelect, 1);
      int bVirtual = sqlite3_column_int(pSelect, 2);
      const char *zName = (const char*)sqlite3_column_text(pSelect, 3);
      const char *zSql = (const char*)sqlite3_column_text(pSelect, 4);
      char *zFree = 0;
      int rc = SQLITE_OK;

      if( bVirtual ){
        zSql = (const char*)(zFree = recoverMPrintf(p,
            "INSERT INTO sqlite_schema VALUES('table', %Q, %Q, 0, %Q)",
            zName, zName, zSql
        ));
      }
      rc = sqlite3_exec(p->dbOut, zSql, 0, 0, 0);
      if( rc==SQLITE_OK ){
        recoverSqlCallback(p, zSql);
        if( bTable && !bVirtual ){
          if( SQLITE_ROW==sqlite3_step(pTblname) ){
            const char *zName = sqlite3_column_text(pTblname, 0);
            recoverAddTable(p, zName, iRoot);
          }
          recoverReset(p, pTblname);
        }
      }else if( rc!=SQLITE_ERROR ){
        recoverDbError(p, p->dbOut);
      }
      sqlite3_free(zFree);
    }
  }
  recoverFinalize(p, pSelect);
  recoverFinalize(p, pTblname);

  return p->errCode;
}

/*
** This function is called after the output database has been populated. It
** adds all recovered schema elements that were not created in the output
** database by recoverWriteSchema1() - everything except for tables and
** UNIQUE indexes. Specifically:
**
**     * views,
**     * triggers,
**     * non-UNIQUE indexes.
**
** If the recover handle is in SQL callback mode, then equivalent callbacks
** are issued to create the schema elements.
*/
static int recoverWriteSchema2(sqlite3_recover *p){
  sqlite3_stmt *pSelect = 0;

  pSelect = recoverPrepare(p, p->dbOut,
      "SELECT rootpage, sql FROM recovery.schema "
      "  WHERE type!='table' AND (type!='index' OR sql NOT LIKE '%unique%')"
  );

  if( pSelect ){
    while( sqlite3_step(pSelect)==SQLITE_ROW ){
      i64 iRoot = sqlite3_column_int64(pSelect, 0);
      const char *zSql = (const char*)sqlite3_column_text(pSelect, 1);
      int rc = sqlite3_exec(p->dbOut, zSql, 0, 0, 0);
      if( rc==SQLITE_OK ){
        recoverSqlCallback(p, zSql);
      }else if( rc!=SQLITE_ERROR ){
        recoverDbError(p, p->dbOut);
      }
    }
  }
  recoverFinalize(p, pSelect);

  return p->errCode;
}

/*
** This function is a no-op if recover handle p already contains an error
** (if p->errCode!=SQLITE_OK). In this case it returns NULL.
**
** Otherwise, if the recover handle is configured to create an output
** database (was created by sqlite3_recover_init()), then this function
** prepares and returns an SQL statement to INSERT a new record into table
** pTab, assuming the first nField fields of a record extracted from disk
** are valid.
**
** For example, if table pTab is:
**
**     CREATE TABLE name(a, b GENERATED ALWAYS AS (a+1) STORED, c, d, e);
**
** And nField is 4, then the SQL statement prepared and returned is:
**
**     INSERT INTO (a, c, d) VALUES (?1, ?2, ?3);
**
** In this case even though 4 values were extracted from the input db,
** only 3 are written to the output, as the generated STORED column 
** cannot be written.
**
** If the recover handle is in SQL callback mode, then the SQL statement
** prepared is such that evaluating it returns a single row containing
** a single text value - itself an SQL statement similar to the above,
** except with SQL literals in place of the variables. For example:
**
**     SELECT 'INSERT INTO (a, c, d) VALUES (' 
**          || quote(?1) || ', '
**          || quote(?2) || ', '
**          || quote(?3) || ')';
**
** In either case, it is the responsibility of the caller to eventually
** free the statement handle using sqlite3_finalize().
*/
static sqlite3_stmt *recoverInsertStmt(
  sqlite3_recover *p, 
  RecoverTable *pTab,
  int nField
){
  sqlite3_stmt *pRet = 0;
  const char *zSep = "";
  const char *zSqlSep = "";
  char *zSql = 0;
  char *zFinal = 0;
  char *zBind = 0;
  int ii;
  int bSql = p->xSql ? 1 : 0;

  if( nField<=0 ) return 0;

  assert( nField<=pTab->nCol );

  zSql = recoverMPrintf(p, "INSERT OR IGNORE INTO %Q(", pTab->zTab);

  if( pTab->iRowidBind ){
    assert( pTab->bIntkey );
    zSql = recoverMPrintf(p, "%z_rowid_", zSql);
    if( bSql ){
      zBind = recoverMPrintf(p, "%zquote(?%d)", zBind, pTab->iRowidBind);
    }else{
      zBind = recoverMPrintf(p, "%z?%d", zBind, pTab->iRowidBind);
    }
    zSqlSep = "||', '||";
    zSep = ", ";
  }

  for(ii=0; ii<nField; ii++){
    int eHidden = pTab->aCol[ii].eHidden;
    if( eHidden!=RECOVER_EHIDDEN_VIRTUAL
     && eHidden!=RECOVER_EHIDDEN_STORED
    ){
      assert( pTab->aCol[ii].iField>=0 && pTab->aCol[ii].iBind>=1 );
      zSql = recoverMPrintf(p, "%z%s%Q", zSql, zSep, pTab->aCol[ii].zCol);

      if( bSql ){
        zBind = recoverMPrintf(p, 
            "%z%sescape_crnl(quote(?%d))", zBind, zSqlSep, pTab->aCol[ii].iBind
        );
        zSqlSep = "||', '||";
      }else{
        zBind = recoverMPrintf(p, "%z%s?%d", zBind, zSep, pTab->aCol[ii].iBind);
      }
      zSep = ", ";
    }
  }

  if( bSql ){
    zFinal = recoverMPrintf(p, "SELECT %Q || ') VALUES (' || %s || ')'", 
        zSql, zBind
    );
  }else{
    zFinal = recoverMPrintf(p, "%s) VALUES (%s)", zSql, zBind);
  }

  pRet = recoverPrepare(p, p->dbOut, zFinal);
  sqlite3_free(zSql);
  sqlite3_free(zBind);
  sqlite3_free(zFinal);
  
  return pRet;
}


/*
** Search the list of RecoverTable objects at p->pTblList for one that
** has root page iRoot in the input database. If such an object is found,
** return a pointer to it. Otherwise, return NULL.
*/
static RecoverTable *recoverFindTable(sqlite3_recover *p, u32 iRoot){
  RecoverTable *pRet = 0;
  for(pRet=p->pTblList; pRet && pRet->iRoot!=iRoot; pRet=pRet->pNext);
  return pRet;
}

/*
** This function attempts to create a lost and found table within the 
** output db. If successful, it returns a pointer to a buffer containing
** the name of the new table. It is the responsibility of the caller to
** eventually free this buffer using sqlite3_free().
**
** If an error occurs, NULL is returned and an error code and error 
** message left in the recover handle.
*/
static char *recoverLostAndFoundCreate(
  sqlite3_recover *p,             /* Recover object */
  int nField                      /* Number of column fields in new table */
){
  char *zTbl = 0;
  sqlite3_stmt *pProbe = 0;
  int ii = 0;

  pProbe = recoverPrepare(p, p->dbOut,
    "SELECT 1 FROM sqlite_schema WHERE name=?"
  );
  for(ii=-1; zTbl==0 && p->errCode==SQLITE_OK && ii<1000; ii++){
    int bFail = 0;
    if( ii<0 ){
      zTbl = recoverMPrintf(p, "%s", p->zLostAndFound);
    }else{
      zTbl = recoverMPrintf(p, "%s_%d", p->zLostAndFound, ii);
    }

    if( p->errCode==SQLITE_OK ){
      sqlite3_bind_text(pProbe, 1, zTbl, -1, SQLITE_STATIC);
      if( SQLITE_ROW==sqlite3_step(pProbe) ){
        bFail = 1;
      }
      recoverReset(p, pProbe);
    }

    if( bFail ){
      sqlite3_clear_bindings(pProbe);
      sqlite3_free(zTbl);
      zTbl = 0;
    }
  }
  recoverFinalize(p, pProbe);

  if( zTbl ){
    const char *zSep = 0;
    char *zField = 0;
    char *zSql = 0;

    zSep = "rootpgno INTEGER, pgno INTEGER, nfield INTEGER, id INTEGER, ";
    for(ii=0; p->errCode==SQLITE_OK && ii<nField; ii++){
      zField = recoverMPrintf(p, "%z%sc%d", zField, zSep, ii);
      zSep = ", ";
    }

    zSql = recoverMPrintf(p, "CREATE TABLE %s(%s)", zTbl, zField);
    sqlite3_free(zField);

    recoverExec(p, p->dbOut, zSql);
    recoverSqlCallback(p, zSql);
    sqlite3_free(zSql);
  }else if( p->errCode==SQLITE_OK ){
    recoverError(
        p, SQLITE_ERROR, "failed to create %s output table", p->zLostAndFound
    );
  }

  return zTbl;
}

/*
** Synthesize and prepare an INSERT statement to write to the lost_and_found
** table in the output database. The name of the table is zTab, and it has
** nField c* fields.
*/
static sqlite3_stmt *recoverLostAndFoundInsert(
  sqlite3_recover *p,
  const char *zTab,
  int nField
){
  int nTotal = nField + 4;
  int ii;
  char *zBind = 0;
  const char *zSep = "";
  sqlite3_stmt *pRet = 0;

  if( p->xSql==0 ){
    for(ii=0; ii<nTotal; ii++){
      zBind = recoverMPrintf(p, "%z%s?", zBind, zBind?", ":"", ii);
    }
    pRet = recoverPreparePrintf(
        p, p->dbOut, "INSERT INTO %s VALUES(%s)", zTab, zBind
    );
  }else{
    const char *zSep = "";
    for(ii=0; ii<nTotal; ii++){
      zBind = recoverMPrintf(p, "%z%squote(?)", zBind, zSep);
      zSep = "|| ', ' ||";
    }
    pRet = recoverPreparePrintf(
        p, p->dbOut, "SELECT 'INSERT INTO %s VALUES(' || %s || ')'", zTab, zBind
    );
  }

  sqlite3_free(zBind);
  return pRet;
}

/*
** Helper function for recoverLostAndFound().
*/
static void recoverLostAndFoundPopulate(
  sqlite3_recover *p, 
  sqlite3_stmt *pInsert,
  int nField
){
  sqlite3_stmt *pStmt = recoverPrepare(p, p->dbOut,
      "WITH RECURSIVE pages(root, page) AS ("
      "  SELECT pgno, pgno FROM recovery.map WHERE parent IS NULL"
      "    UNION"
      "  SELECT root, child FROM sqlite_dbptr('getpage()'), pages "
      "    WHERE pgno=page"
      ") "
      "SELECT root, page, cell, field, value "
      "FROM sqlite_dbdata('getpage()') d, pages p WHERE p.page=d.pgno "
      "  AND NOT page_is_used(page) "
      "UNION ALL "
      "SELECT 0, 0, 0, 0, 0"
  );

  sqlite3_value **apVal = 0;
  int nVal = -1;
  i64 iRowid = 0;
  int bHaveRowid = 0;
  int ii;

  i64 iPrevRoot = -1;
  i64 iPrevPage = -1;
  int iPrevCell = -1;

  apVal = (sqlite3_value**)recoverMalloc(p, nField*sizeof(sqlite3_value*));
  while( p->errCode==SQLITE_OK && SQLITE_ROW==sqlite3_step(pStmt) ){
    i64 iRoot = sqlite3_column_int64(pStmt, 0);
    i64 iPage = sqlite3_column_int64(pStmt, 1);
    int iCell = sqlite3_column_int64(pStmt, 2);
    int iField = sqlite3_column_int64(pStmt, 3);

    if( iPrevRoot>0 && (iPrevPage!=iPage || iPrevCell!=iCell) ){
      /* Insert the new row */
      sqlite3_bind_int64(pInsert, 1, iPrevRoot);  /* rootpgno */
      sqlite3_bind_int64(pInsert, 2, iPrevPage);  /* pgno */
      sqlite3_bind_int(pInsert, 3, nVal);         /* nfield */
      if( bHaveRowid ){
        sqlite3_bind_int64(pInsert, 4, iRowid);   /* id */
      }
      for(ii=0; ii<nVal; ii++){
        recoverBindValue(p, pInsert, 5+ii, apVal[ii]);
      }
      if( sqlite3_step(pInsert)==SQLITE_ROW ){
        recoverSqlCallback(p, sqlite3_column_text(pInsert, 0));
      }
      recoverReset(p, pInsert);

      /* Discard the accumulated row data */
      for(ii=0; ii<nVal; ii++){
        sqlite3_value_free(apVal[ii]);
        apVal[ii] = 0;
      }
      sqlite3_clear_bindings(pInsert);
      bHaveRowid = 0;
      nVal = -1;
    }

    if( iField<0 ){
      assert( nVal==-1 );
      iRowid = sqlite3_column_int64(pStmt, 4);
      bHaveRowid = 1;
      nVal = 0;
    }else if( iField<nField && iRoot!=0 ){
      sqlite3_value *pVal = sqlite3_column_value(pStmt, 4);
      apVal[iField] = sqlite3_value_dup(pVal);
      assert( iField==nVal || (nVal==-1 && iField==0) );
      nVal = iField+1;
      if( apVal[iField]==0 ){
        recoverError(p, SQLITE_NOMEM, 0);
      }
    }

    iPrevRoot = iRoot;
    iPrevPage = iPage;
    iPrevCell = iCell;
  }
  recoverFinalize(p, pStmt);

  for(ii=0; ii<nVal; ii++){
    sqlite3_value_free(apVal[ii]);
    apVal[ii] = 0;
  }
  sqlite3_free(apVal);
}

/*
** This function searches for orphaned rows in the input database. If
** any are found, it creates the lost-and-found table in the output
** db and writes all orphaned rows to it. Or, if the recover handle is
** in SQL callback mode, issues equivalent callbacks.
*/
static int recoverLostAndFound(sqlite3_recover *p){
  i64 nPg = 0;
  RecoverBitmap *pMap = 0;

  nPg = recoverPageCount(p);
  pMap = p->pUsed = recoverBitmapAlloc(p, nPg);
  if( pMap ){
    char *zTab = 0;               /* Name of lost_and_found table */
    sqlite3_stmt *pInsert = 0;    /* INSERT INTO lost_and_found ... */
    int nField = 0;

    /* Add all pages that are part of any tree in the recoverable part of
    ** the input database schema to the bitmap. */
    sqlite3_stmt *pStmt = recoverPrepare(
        p, p->dbOut,
        "WITH roots(r) AS ("
        "  SELECT 1 UNION ALL"
        "  SELECT rootpage FROM recovery.schema WHERE rootpage>0"
        "),"
        "used(page) AS ("
        "  SELECT r FROM roots"
        "    UNION"
        "  SELECT child FROM sqlite_dbptr('getpage()'), used "
        "    WHERE pgno=page"
        ") "
        "SELECT page FROM used"
    );
    while( pStmt && SQLITE_ROW==sqlite3_step(pStmt) ){
      i64 iPg = sqlite3_column_int64(pStmt, 0);
      recoverBitmapSet(pMap, iPg);
    }
    recoverFinalize(p, pStmt);

    /* Add all pages that appear to be part of the freelist to the bitmap. */
    if( p->bFreelistCorrupt==0 ){
      pStmt = recoverPrepare(p, p->dbOut,
          "WITH trunk(pgno) AS ("
          "  SELECT read_i32(getpage(1), 8) AS x WHERE x>0"
          "    UNION"
          "  SELECT read_i32(getpage(trunk.pgno), 0) AS x FROM trunk WHERE x>0"
          "),"
          "trunkdata(pgno, data) AS ("
          "  SELECT pgno, getpage(pgno) FROM trunk"
          "),"
          "freelist(data, n, freepgno) AS ("
          "  SELECT data, min(16384, read_i32(data, 1)-1), pgno FROM trunkdata"
          "    UNION ALL"
          "  SELECT data, n-1, read_i32(data, 2+n) FROM freelist WHERE n>=0"
          ")"
          "SELECT freepgno FROM freelist"
      );
      while( pStmt && SQLITE_ROW==sqlite3_step(pStmt) ){
        i64 iPg = sqlite3_column_int64(pStmt, 0);
        recoverBitmapSet(pMap, iPg);
      }
      recoverFinalize(p, pStmt);
    }

    /* Add an entry for each page not already added to the bitmap to 
    ** the recovery.map table. This loop leaves the "parent" column
    ** of each recovery.map row set to NULL - to be filled in below.  */
    pStmt = recoverPreparePrintf(
        p, p->dbOut,
        "WITH RECURSIVE seq(ii) AS ("
        "  SELECT 1 UNION ALL SELECT ii+1 FROM seq WHERE ii<%lld"
        ")"
        "INSERT INTO recovery.map(pgno) "
        "    SELECT ii FROM seq WHERE NOT page_is_used(ii)", nPg
    );
    sqlite3_step(pStmt);
    recoverFinalize(p, pStmt);

    /* Set the "parent" column for each row of the recovery.map table */
    pStmt = recoverPrepare(
        p, p->dbOut,
        "UPDATE recovery.map SET parent = ptr.pgno "
        "    FROM sqlite_dbptr('getpage()') AS ptr "
        "    WHERE recovery.map.pgno=ptr.child"
    );
    sqlite3_step(pStmt);
    recoverFinalize(p, pStmt);
      
    /* Figure out the number of fields in the longest record that will be
    ** recovered into the lost_and_found table. Set nField to this value. */
    pStmt = recoverPrepare(
        p, p->dbOut,
        "SELECT max(field)+1 FROM sqlite_dbdata('getpage') WHERE pgno IN ("
        "  SELECT pgno FROM recovery.map"
        ")"
    );
    if( pStmt && SQLITE_ROW==sqlite3_step(pStmt) ){
      nField = sqlite3_column_int64(pStmt, 0);
    }
    recoverFinalize(p, pStmt);

    if( nField>0 ){
      zTab = recoverLostAndFoundCreate(p, nField);
      pInsert = recoverLostAndFoundInsert(p, zTab, nField);
      recoverLostAndFoundPopulate(p, pInsert, nField);
      recoverFinalize(p, pInsert);
      sqlite3_free(zTab);
    }
  }
}

/*
** For each table in the recovered schema, this function extracts as much
** data as possible from the output database and writes it to the input
** database. Or, if the recover handle is in SQL callback mode, issues
** equivalent callbacks.
**
** It does not recover "orphaned" data into the lost-and-found table.
** See recoverLostAndFound() for that.
*/
static int recoverWriteData(sqlite3_recover *p){
  RecoverTable *pTbl;
  int nMax = 0;
  sqlite3_value **apVal = 0;

  sqlite3_stmt *pTbls = 0;
  sqlite3_stmt *pSel = 0;

  /* Figure out the maximum number of columns for any table in the schema */
  for(pTbl=p->pTblList; pTbl; pTbl=pTbl->pNext){
    if( pTbl->nCol>nMax ) nMax = pTbl->nCol;
  }

  apVal = (sqlite3_value**)recoverMalloc(p, sizeof(sqlite3_value*) * (nMax+1));
  if( apVal==0 ) return p->errCode;

  pTbls = recoverPrepare(p, p->dbOut,
      "SELECT rootpage FROM recovery.schema "
      "  WHERE type='table' AND (sql NOT LIKE 'create virtual%')"
      "  ORDER BY (tbl_name='sqlite_sequence') ASC"
  );

  pSel = recoverPrepare(p, p->dbOut, 
      "WITH RECURSIVE pages(page) AS ("
      "  SELECT ?1"
      "    UNION"
      "  SELECT child FROM sqlite_dbptr('getpage()'), pages "
      "    WHERE pgno=page"
      ") "
      "SELECT page, cell, field, value "
      "FROM sqlite_dbdata('getpage()') d, pages p WHERE p.page=d.pgno "
      "UNION ALL "
      "SELECT 0, 0, 0, 0"
  );
  if( pSel ){

    /* The outer loop runs once for each table to recover. */
    while( sqlite3_step(pTbls)==SQLITE_ROW ){
      i64 iRoot = sqlite3_column_int64(pTbls, 0);
      RecoverTable *pTab = recoverFindTable(p, iRoot);
      if( pTab ){
        int ii;
        sqlite3_stmt *pInsert = 0;
        int nInsert = -1;
        i64 iPrevPage = -1;
        int iPrevCell = -1;
        int bHaveRowid = 0;           /* True if iRowid is valid */
        i64 iRowid = 0;
        int nVal = -1;

        if( sqlite3_stricmp("sqlite_sequence", pTab->zTab)==0 ){
          recoverExec(p, p->dbOut, "DELETE FROM sqlite_sequence");
          recoverSqlCallback(p, "DELETE FROM sqlite_sequence");
        }

        sqlite3_bind_int64(pSel, 1, iRoot);
        while( p->errCode==SQLITE_OK && sqlite3_step(pSel)==SQLITE_ROW ){
          i64 iPage = sqlite3_column_int64(pSel, 0);
          int iCell = sqlite3_column_int(pSel, 1);
          int iField = sqlite3_column_int(pSel, 2);
          sqlite3_value *pVal = sqlite3_column_value(pSel, 3);

          int bNewCell = (iPrevPage!=iPage || iPrevCell!=iCell);
          assert( bNewCell==0 || (iField==-1 || iField==0) );
          assert( bNewCell || iField==nVal || nVal==pTab->nCol );

          if( bNewCell ){
            if( nVal>=0 ){
              int ii;
              int iVal = 0;
              int iBind = 1;

              if( pInsert==0 || nVal!=nInsert ){
                recoverFinalize(p, pInsert);
                pInsert = recoverInsertStmt(p, pTab, nVal);
                nInsert = nVal;
              }
              if( nVal>0 ){
                for(ii=0; ii<pTab->nCol; ii++){
                  RecoverColumn *pCol = &pTab->aCol[ii];

                  if( pCol->iBind>0 ){
                    int iBind = pCol->iBind;
                    if( pCol->bIPK ){
                      sqlite3_bind_int64(pInsert, iBind, iRowid);
                    }else if( pCol->iField<nVal ){
                      recoverBindValue(p, pInsert, iBind, apVal[pCol->iField]);
                    }
                  }
                }
                if( p->bRecoverRowid && pTab->iRowidBind>0 && bHaveRowid ){
                  sqlite3_bind_int64(pInsert, pTab->iRowidBind, iRowid);
                }

                if( SQLITE_ROW==sqlite3_step(pInsert) ){
                  const char *z = (const char*)sqlite3_column_text(pInsert, 0);
                  recoverSqlCallback(p, z);
                }
                recoverReset(p, pInsert);
                assert( p->errCode || pInsert );
                if( pInsert ) sqlite3_clear_bindings(pInsert);
              }
            }

            for(ii=0; ii<nVal; ii++){
              sqlite3_value_free(apVal[ii]);
              apVal[ii] = 0;
            }
            nVal = -1;
            bHaveRowid = 0;
          }

          if( iPage!=0 ){
            if( iField<0 ){
              iRowid = sqlite3_column_int64(pSel, 3);
              assert( nVal==-1 );
              nVal = 0;
              bHaveRowid = 1;
            }else if( iField<pTab->nCol ){
              assert( apVal[iField]==0 );
              apVal[iField] = sqlite3_value_dup( pVal );
              if( apVal[iField]==0 ){
                recoverError(p, SQLITE_NOMEM, 0);
              }
              nVal = iField+1;
            }
            iPrevCell = iCell;
            iPrevPage = iPage;
          }
        }

        recoverReset(p, pSel);
        recoverFinalize(p, pInsert);
        pInsert = 0;
        for(ii=0; ii<nVal; ii++){
          sqlite3_value_free(apVal[ii]);
          apVal[ii] = 0;
        }
      }
    }

  }

  recoverFinalize(p, pTbls);
  recoverFinalize(p, pSel);

  sqlite3_free(apVal);
  return p->errCode;
}

/*
** This function does the work of sqlite3_recover_run(). It is assumed that
** no error has occurred when this is called. If an error occurs during
** the recovery operation, an error code and error message are left in
** the recovery handle.
*/
static void recoverRun(sqlite3_recover *p){
  RecoverTable *pTab = 0;
  RecoverTable *pNext = 0;
  int rc = SQLITE_OK;

  assert( p->errCode==SQLITE_OK );
  assert( p->bRun==0 );
  p->bRun = 1;

  recoverSqlCallback(p, "BEGIN");
  recoverSqlCallback(p, "PRAGMA writable_schema = on");

  /* Open the output database. And register required virtual tables and 
  ** user functions with the new handle. */
  recoverOpenOutput(p);

  /* Open transactions on both the input and output databases. */
  recoverExec(p, p->dbIn, "BEGIN");
  recoverExec(p, p->dbOut, "BEGIN");

  recoverCacheSchema(p);
  recoverWriteSchema1(p);
  recoverWriteData(p);
  if( p->zLostAndFound ) recoverLostAndFound(p);
  recoverWriteSchema2(p);

  /* If no error has occurred, commit the write transaction on the output
  ** database. Then end the read transaction on the input database, regardless
  ** of whether or not prior errors have occurred.  */
  recoverExec(p, p->dbOut, "COMMIT");
  rc = sqlite3_exec(p->dbIn, "END", 0, 0, 0);
  if( p->errCode==SQLITE_OK ) p->errCode = rc;

  recoverSqlCallback(p, "PRAGMA writable_schema = off");
  recoverSqlCallback(p, "COMMIT");

  /* Clean up various resources allocated by this function. */
  for(pTab=p->pTblList; pTab; pTab=pNext){
    pNext = pTab->pNext;
    sqlite3_free(pTab);
  }
  p->pTblList = 0;
  sqlite3_finalize(p->pGetPage);
  p->pGetPage = 0;
  recoverBitmapFree(p->pUsed);
  p->pUsed = 0;
  sqlite3_close(p->dbOut);
}


/*
** This is a worker function that does the heavy lifting for both init
** functions:
**
**     sqlite3_recover_init()
**     sqlite3_recover_init_sql()
**
** All this function does is allocate space for the recover handle and
** take copies of the input parameters. All the real work is done within
** sqlite3_recover_run().
*/
sqlite3_recover *recoverInit(
  sqlite3* db, 
  const char *zDb, 
  const char *zUri,               /* Output URI for _recover_init() */
  int (*xSql)(void*, const char*),/* SQL callback for _recover_init_sql() */
  void *pSqlCtx                   /* Context arg for _recover_init_sql() */
){
  sqlite3_recover *pRet = 0;
  int nDb = 0;
  int nUri = 0;
  int nByte = 0;

  if( zDb==0 ){ zDb = "main"; }

  nDb = recoverStrlen(zDb);
  nUri = recoverStrlen(zUri);

  nByte = sizeof(sqlite3_recover) + nDb+1 + nUri+1;
  pRet = (sqlite3_recover*)sqlite3_malloc(nByte);
  if( pRet ){
    memset(pRet, 0, nByte);
    pRet->dbIn = db;
    pRet->zDb = (char*)&pRet[1];
    pRet->zUri = &pRet->zDb[nDb+1];
    memcpy(pRet->zDb, zDb, nDb);
    if( nUri>0 ) memcpy(pRet->zUri, zUri, nUri);
    pRet->xSql = xSql;
    pRet->pSqlCtx = pSqlCtx;
    pRet->bRecoverRowid = RECOVER_ROWID_DEFAULT;
  }

  return pRet;
}

/*
** Initialize a recovery handle that creates a new database containing
** the recovered data.
*/
sqlite3_recover *sqlite3_recover_init(
  sqlite3* db, 
  const char *zDb, 
  const char *zUri
){
  return recoverInit(db, zDb, zUri, 0, 0);
}

/*
** Initialize a recovery handle that returns recovered data in the
** form of SQL statements via a callback.
*/
sqlite3_recover *sqlite3_recover_init_sql(
  sqlite3* db, 
  const char *zDb, 
  int (*xSql)(void*, const char*),
  void *pSqlCtx
){
  return recoverInit(db, zDb, 0, xSql, pSqlCtx);
}

/*
** Return the handle error message, if any.
*/
const char *sqlite3_recover_errmsg(sqlite3_recover *p){
  return (p && p->errCode!=SQLITE_NOMEM) ? p->zErrMsg : "out of memory";
}

/*
** Return the handle error code.
*/
int sqlite3_recover_errcode(sqlite3_recover *p){
  return p ? p->errCode : SQLITE_NOMEM;
}

/*
** Configure the handle.
*/
int sqlite3_recover_config(sqlite3_recover *p, int op, void *pArg){
  int rc = SQLITE_OK;

  if( p==0 ) return SQLITE_NOMEM;
  switch( op ){
    case 789:
      sqlite3_free(p->zStateDb);
      p->zStateDb = recoverMPrintf(p, "%s", (char*)pArg);
      break;

    case SQLITE_RECOVER_LOST_AND_FOUND: {
      const char *zArg = (const char*)pArg;
      sqlite3_free(p->zLostAndFound);
      if( zArg ){
        p->zLostAndFound = recoverMPrintf(p, "%s", zArg);
      }else{
        p->zLostAndFound = 0;
      }
      break;
    }

    case SQLITE_RECOVER_FREELIST_CORRUPT:
      p->bFreelistCorrupt = *(int*)pArg;
      break;

    case SQLITE_RECOVER_ROWIDS:
      p->bRecoverRowid = *(int*)pArg;
      break;

    default:
      rc = SQLITE_NOTFOUND;
      break;
  }

  return rc;
}

/*
** Do the configured recovery operation. Return SQLITE_OK if successful, or
** else an SQLite error code.
*/
int sqlite3_recover_run(sqlite3_recover *p){
  if( p ){
    recoverExec(p, p->dbIn, "PRAGMA writable_schema=1");
    if( p->bRun ) return SQLITE_MISUSE;     /* Has already run */
    if( p->errCode==SQLITE_OK ) recoverRun(p);
    if( sqlite3_exec(p->dbIn, "PRAGMA writable_schema=0", 0, 0, 0) ){
      recoverDbError(p, p->dbIn);
    }
  }
  return p ? p->errCode : SQLITE_NOMEM;
}

/*
** Free all resources associated with the recover handle passed as the only
** argument. The results of using a handle with any sqlite3_recover_**
** API function after it has been passed to this function are undefined.
**
** A copy of the value returned by the first call made to sqlite3_recover_run()
** on this handle is returned, or SQLITE_OK if sqlite3_recover_run() has
** not been called on this handle.
*/
int sqlite3_recover_finish(sqlite3_recover *p){
  int rc;
  if( p==0 ){
    rc = SQLITE_NOMEM;
  }else{
    rc = p->errCode;
    sqlite3_free(p->zErrMsg);
    sqlite3_free(p->zStateDb);
    sqlite3_free(p->zLostAndFound);
    sqlite3_free(p);
  }
  return rc;
}


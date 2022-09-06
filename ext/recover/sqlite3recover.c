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

typedef unsigned int u32;
typedef sqlite3_int64 i64;

typedef struct RecoverColumn RecoverColumn;
struct RecoverColumn {
  int iField;                     /* Field in record on disk */
  int iBind;                      /* Binding to use in INSERT */
  int bIPK;                       /* True for IPK column */
  char *zCol;
  int eHidden;
};

#define RECOVER_EHIDDEN_NONE    0
#define RECOVER_EHIDDEN_HIDDEN  1
#define RECOVER_EHIDDEN_VIRTUAL 2
#define RECOVER_EHIDDEN_STORED  3

/*
** When running the ".recover" command, each output table, and the special
** orphaned row table if it is required, is represented by an instance
** of the following struct.
**
** aCol[]:
**  Array of nCol columns. In the order in which they appear in the table.
*/
typedef struct RecoverTable RecoverTable;
struct RecoverTable {
  u32 iRoot;                      /* Root page in original database */
  char *zTab;                     /* Name of table */
  int nCol;                       /* Number of columns in table */
  RecoverColumn *aCol;            /* Array of columns */
  int bIntkey;                    /* True for intkey, false for without rowid */
  int iRowidBind;                 /* If >0, bind rowid to INSERT here */

  RecoverTable *pNext;
};

typedef struct RecoverBitmap RecoverBitmap;
struct RecoverBitmap {
  i64 nPg;                        /* Size of bitmap */
  u32 aElem[0];                   /* Array of 32-bit bitmasks */
};

/*
** 
*/
#define RECOVERY_SCHEMA \
"  CREATE TABLE recovery.map("                                 \
"      pgno INTEGER PRIMARY KEY, parent INT"                   \
"  );"                                                         \
"  CREATE TABLE recovery.schema("                              \
"      type, name, tbl_name, rootpage, sql"                    \
"  );" 


struct sqlite3_recover {
  sqlite3 *dbIn;
  sqlite3 *dbOut;

  sqlite3_stmt *pGetPage;

  char *zDb;
  char *zUri;
  RecoverTable *pTblList;
  RecoverBitmap *pUsed;           /* Used by recoverLostAndFound() */

  int errCode;                    /* For sqlite3_recover_errcode() */
  char *zErrMsg;                  /* For sqlite3_recover_errmsg() */

  char *zStateDb;
  char *zLostAndFound;            /* Name of lost-and-found table (or NULL) */
  int bFreelistCorrupt;
  int bRecoverRowid;

  void *pSqlCtx;
  int (*xSql)(void*,const char*);
};

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

static int recoverError(
  sqlite3_recover *p, 
  int errCode, 
  const char *zFmt, ...
){
  va_list ap;
  char *z;
  va_start(ap, zFmt);
  z = sqlite3_vmprintf(zFmt, ap);
  va_end(ap);

  sqlite3_free(p->zErrMsg);
  p->zErrMsg = z;
  p->errCode = errCode;
  return errCode;
}


static RecoverBitmap *recoverBitmapAlloc(sqlite3_recover *p, i64 nPg){
  int nElem = (nPg+1+31) / 32;
  int nByte = sizeof(RecoverBitmap) + nElem*sizeof(u32);
  RecoverBitmap *pRet = (RecoverBitmap*)recoverMalloc(p, nByte);

  if( pRet ){
    pRet->nPg = nPg;
  }
  return pRet;
}

static void recoverBitmapFree(RecoverBitmap *pMap){
  sqlite3_free(pMap);
}

static void recoverBitmapSet(RecoverBitmap *pMap, i64 iPg){
  if( iPg<=pMap->nPg ){
    int iElem = (iPg / 32);
    int iBit = (iPg % 32);
    pMap->aElem[iElem] |= (((u32)1) << iBit);
  }
}

static int recoverBitmapQuery(RecoverBitmap *pMap, i64 iPg){
  int ret = 1;
  if( iPg<=pMap->nPg ){
    int iElem = (iPg / 32);
    int iBit = (iPg % 32);
    ret = (pMap->aElem[iElem] & (((u32)1) << iBit)) ? 1 : 0;
  }
  return ret;
}


static int recoverDbError(sqlite3_recover *p, sqlite3 *db){
  return recoverError(p, sqlite3_errcode(db), "%s", sqlite3_errmsg(db));
}

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
** Create a prepared statement using printf-style arguments for the SQL.
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


static sqlite3_stmt *recoverReset(sqlite3_recover *p, sqlite3_stmt *pStmt){
  int rc = sqlite3_reset(pStmt);
  if( rc!=SQLITE_OK && p->errCode==SQLITE_OK ){
    recoverDbError(p, sqlite3_db_handle(pStmt));
  }
  return pStmt;
}

static void recoverFinalize(sqlite3_recover *p, sqlite3_stmt *pStmt){
  sqlite3 *db = sqlite3_db_handle(pStmt);
  int rc = sqlite3_finalize(pStmt);
  if( rc!=SQLITE_OK && p->errCode==SQLITE_OK ){
    recoverDbError(p, db);
  }
}

static int recoverExec(sqlite3_recover *p, sqlite3 *db, const char *zSql){
  if( p->errCode==SQLITE_OK ){
    int rc = sqlite3_exec(p->dbOut, zSql, 0, 0, 0);
    if( rc ){
      recoverDbError(p, p->dbOut);
    }
  }
  return p->errCode;
}

static char *recoverPrintf(sqlite3_recover *p, const char *zFmt, ...){
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
** Execute "PRAGMA page_count" against the input database. If successful,
** return the integer result. Or, if an error occurs, leave an error code 
** and error message in the sqlite3_recover handle.
*/
static i64 recoverPageCount(sqlite3_recover *p){
  i64 nPg = 0;
  if( p->errCode==SQLITE_OK ){
    sqlite3_stmt *pStmt = 0;
    pStmt = recoverPreparePrintf(p, p->dbIn, "PRAGMA %Q.page_count", p->zDb);
    if( pStmt && SQLITE_ROW==sqlite3_step(pStmt) ){
      nPg = sqlite3_column_int64(pStmt, 0);
    }
    recoverFinalize(p, pStmt);
  }
  return nPg;
}

/*
** Scalar function "read_i32". The first argument to this function
** must be a blob. The second a non-negative integer. This function
** reads and returns a 32-bit big-endian integer from byte
** offset (4*<arg2>) of the blob.
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
  iInt = sqlite3_value_int(argv[1]);

  if( iInt>=0 && (iInt+1)*4<=nBlob ){
    const unsigned char *a = &pBlob[iInt*4];
    i64 iVal = ((i64)a[0]<<24)
             + ((i64)a[1]<<16)
             + ((i64)a[2]<< 8)
             + ((i64)a[3]<< 0);
    sqlite3_result_int64(context, iVal);
  }
}

/*
** SELECT page_is_used(pgno);
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
** This function always takes a single integer argument. If the arguement
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
      pStmt = recoverPreparePrintf(
          p, p->dbIn, "SELECT data FROM sqlite_dbpage(%Q) WHERE pgno=?", p->zDb
      );
    }else{
      pStmt = p->pGetPage;
    }

    if( pStmt ){
      sqlite3_bind_int64(pStmt, 1, pgno);
      if( SQLITE_ROW==sqlite3_step(pStmt) ){
        sqlite3_result_value(pCtx, sqlite3_column_value(pStmt, 0));
      }
        p->pGetPage = recoverReset(p, pStmt);
    }
  }

  if( p->errCode ){
    if( p->zErrMsg ) sqlite3_result_error(pCtx, p->zErrMsg, -1);
    sqlite3_result_error_code(pCtx, p->errCode);
  }
}

#ifdef _WIN32
__declspec(dllexport)
#endif
int sqlite3_dbdata_init(sqlite3*, char**, const sqlite3_api_routines*);

static int recoverOpenOutput(sqlite3_recover *p){
  int rc = SQLITE_OK;
  if( p->dbOut==0 ){
    const int flags = SQLITE_OPEN_URI|SQLITE_OPEN_CREATE|SQLITE_OPEN_READWRITE;
    sqlite3 *db = 0;

    assert( p->dbOut==0 );

    rc = sqlite3_open_v2(p->zUri, &db, flags, 0);
    if( rc==SQLITE_OK ){
      rc = sqlite3_exec(db, "PRAGMA writable_schema = 1", 0, 0, 0);
    }
    if( rc==SQLITE_OK ){
      const char *zPath = p->zStateDb ? p->zStateDb : ":memory:";
      char *zSql = sqlite3_mprintf("ATTACH %Q AS recovery", zPath);
      if( zSql==0 ){
        rc = p->errCode = SQLITE_NOMEM;
      }else{
        rc = sqlite3_exec(db, zSql, 0, 0, 0);
      }
      sqlite3_free(zSql);
    }

    /* Truncate the output database. This is done by opening a new, empty,
    ** temp db, then using the backup API to clobber any existing output db
    ** with a copy of it. */
    if( rc==SQLITE_OK ){
      sqlite3 *db2 = 0;
      rc = sqlite3_open("", &db2);
      if( rc==SQLITE_OK ){
        sqlite3_backup *pBackup = sqlite3_backup_init(db, "main", db2, "main");
        if( pBackup ){
          while( sqlite3_backup_step(pBackup, 1000)==SQLITE_OK );
          rc = sqlite3_backup_finish(pBackup);
        }
      }
      sqlite3_close(db2);
    }

    if( rc==SQLITE_OK ){
      rc = sqlite3_exec(db, RECOVERY_SCHEMA, 0, 0, 0);
    }

    if( rc==SQLITE_OK ){
      sqlite3_dbdata_init(db, 0, 0);
      rc = sqlite3_create_function(
          db, "getpage", 1, SQLITE_UTF8, (void*)p, recoverGetPage, 0, 0
      );
    }
    if( rc==SQLITE_OK ){
      rc = sqlite3_create_function(
          db, "page_is_used", 1, SQLITE_UTF8, (void*)p, recoverPageIsUsed, 0, 0
      );
    }
    if( rc==SQLITE_OK ){
      rc = sqlite3_create_function(
          db, "read_i32", 2, SQLITE_UTF8, (void*)p, recoverReadI32, 0, 0
      );
    }

    if( rc!=SQLITE_OK ){
      if( p->errCode==SQLITE_OK ) rc = recoverDbError(p, db);
      sqlite3_close(db);
    }else{
      p->dbOut = db;
    }
  }
  return rc;
}

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

static void recoverAddTable(sqlite3_recover *p, const char *zName, i64 iRoot){
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
    }

    recoverFinalize(p, pStmt);

    pNew->bIntkey = 1;
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

    if( iPk>=0 ){
      pNew->aCol[iPk].bIPK = 1;
    }else if( pNew->bIntkey ){
      pNew->iRowidBind = iBind++;
    }
  }
}

static void recoverSqlCallback(sqlite3_recover *p, const char *zSql){
  if( p->errCode==SQLITE_OK && p->xSql ){
    int res = p->xSql(p->pSqlCtx, zSql);
    if( res ){
      recoverError(p, SQLITE_ERROR, "callback returned an error - %d", res);
    }
  }
}

/*
**
*/
static int recoverWriteSchema1(sqlite3_recover *p){
  sqlite3_stmt *pSelect = 0;
  sqlite3_stmt *pTblname = 0;

  pSelect = recoverPrepare(p, p->dbOut,
      "SELECT rootpage, sql, type='table' FROM recovery.schema "
      "  WHERE type='table' OR (type='index' AND sql LIKE '%unique%') "
      "  ORDER BY type!='table', name!='sqlite_sequence'"
  );

  pTblname = recoverPrepare(p, p->dbOut,
      "SELECT name FROM sqlite_schema "
      "WHERE type='table' ORDER BY rowid DESC LIMIT 1"
  );

  if( pSelect ){
    while( sqlite3_step(pSelect)==SQLITE_ROW ){
      i64 iRoot = sqlite3_column_int64(pSelect, 0);
      const char *zSql = (const char*)sqlite3_column_text(pSelect, 1);
      int bTable = sqlite3_column_int(pSelect, 2);

      int rc = sqlite3_exec(p->dbOut, zSql, 0, 0, 0);
      if( rc==SQLITE_OK ){
        recoverSqlCallback(p, zSql);
        if( bTable ){
          if( SQLITE_ROW==sqlite3_step(pTblname) ){
            const char *zName = sqlite3_column_text(pTblname, 0);
            recoverAddTable(p, zName, iRoot);
          }
          recoverReset(p, pTblname);
        }
      }else if( rc!=SQLITE_ERROR ){
        recoverDbError(p, p->dbOut);
      }
    }
  }
  recoverFinalize(p, pSelect);
  recoverFinalize(p, pTblname);

  return p->errCode;
}

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
      if( rc!=SQLITE_OK && rc!=SQLITE_ERROR ){
        recoverDbError(p, p->dbOut);
      }else if( rc==SQLITE_OK ){
        recoverSqlCallback(p, zSql);
      }
    }
  }
  recoverFinalize(p, pSelect);

  return p->errCode;
}


static char *recoverMPrintf(sqlite3_recover *p, const char *zFmt, ...){
  char *zRet = 0;
  if( p->errCode==SQLITE_OK ){
    va_list ap;
    char *z;
    va_start(ap, zFmt);
    zRet = sqlite3_vmprintf(zFmt, ap);
    va_end(ap);
    if( zRet==0 ){
      p->errCode = SQLITE_NOMEM;
    }
  }
  return zRet;
}

static sqlite3_stmt *recoverInsertStmt(
  sqlite3_recover *p, 
  RecoverTable *pTab,
  int nField
){
  const char *zSep = "";
  const char *zSqlSep = "";
  char *zSql = 0;
  char *zFinal = 0;
  char *zBind = 0;
  int ii;
  int bSql = p->xSql ? 1 : 0;
  sqlite3_stmt *pRet = 0;

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
        zBind = recoverMPrintf(
            p, "%z%squote(?%d)", zBind, zSqlSep, pTab->aCol[ii].iBind
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
      zTbl = recoverPrintf(p, "%s", p->zLostAndFound);
    }else{
      zTbl = recoverPrintf(p, "%s_%d", p->zLostAndFound, ii);
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

    zSql = recoverPrintf(p, "CREATE TABLE %s(%s)", zTbl, zField);
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

    if( iPrevRoot>0 && (
      iPrevRoot!=iRoot || iPrevPage!=iPage || iPrevCell!=iCell
    )){
      /* Insert the new row */
      sqlite3_bind_int64(pInsert, 1, iPrevRoot);  /* rootpgno */
      sqlite3_bind_int64(pInsert, 2, iPrevPage);  /* pgno */
      sqlite3_bind_int(pInsert, 3, nVal);         /* nfield */
      if( bHaveRowid ){
        sqlite3_bind_int64(pInsert, 4, iRowid);   /* id */
      }
      for(ii=0; ii<nVal; ii++){
        sqlite3_bind_value(pInsert, 5+ii, apVal[ii]);
      }
      if( sqlite3_step(pInsert)==SQLITE_ROW && p->xSql ){
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

    recoverBitmapFree(pMap);
    p->pUsed = 0;
  }
}

static int recoverWriteData(sqlite3_recover *p){
  RecoverTable *pTbl;
  int nMax = 0;
  sqlite3_value **apVal = 0;
  sqlite3_stmt *pSel = 0;

  /* Figure out the maximum number of columns for any table in the schema */
  for(pTbl=p->pTblList; pTbl; pTbl=pTbl->pNext){
    if( pTbl->nCol>nMax ) nMax = pTbl->nCol;
  }

  apVal = (sqlite3_value**)recoverMalloc(p, sizeof(sqlite3_value*) * (nMax+1));
  if( apVal==0 ) return p->errCode;

  pSel = recoverPrepare(p, p->dbOut, 
      "WITH RECURSIVE pages(root, page) AS ("
      "  SELECT rootpage, rootpage FROM recovery.schema"
      "    UNION"
      "   SELECT root, child FROM sqlite_dbptr('getpage()'), pages "
      "    WHERE pgno=page"
      ") "
      "SELECT root, page, cell, field, value "
      "FROM sqlite_dbdata('getpage()') d, pages p WHERE p.page=d.pgno "
      "UNION ALL "
      "SELECT 0, 0, 0, 0, 0"
  );
  if( pSel ){
    RecoverTable *pTab = 0;
    sqlite3_stmt *pInsert = 0;
    int nInsert = -1;
    i64 iPrevRoot = -1;
    i64 iPrevPage = -1;
    int iPrevCell = -1;
    int bHaveRowid = 0;           /* True if iRowid is valid */
    i64 iRowid = 0;
    int nVal = -1;

    while( p->errCode==SQLITE_OK && sqlite3_step(pSel)==SQLITE_ROW ){
      i64 iRoot = sqlite3_column_int64(pSel, 0);
      i64 iPage = sqlite3_column_int64(pSel, 1);
      int iCell = sqlite3_column_int(pSel, 2);
      int iField = sqlite3_column_int(pSel, 3);
      sqlite3_value *pVal = sqlite3_column_value(pSel, 4);

      int bNewCell = (iPrevRoot!=iRoot || iPrevPage!=iPage || iPrevCell!=iCell);
      assert( bNewCell==0 || (iField==-1 || iField==0) );
      assert( bNewCell || iField==nVal );

      if( bNewCell ){
        if( nVal>=0 ){
          int ii;

          if( pTab ){
            int iVal = 0;
            int iBind = 1;

            if( pInsert==0 || nVal!=nInsert ){
              recoverFinalize(p, pInsert);
              pInsert = recoverInsertStmt(p, pTab, nVal);
              nInsert = nVal;
            }

            for(ii=0; ii<pTab->nCol; ii++){
              RecoverColumn *pCol = &pTab->aCol[ii];

              if( pCol->iBind>0 ){
                if( pCol->bIPK ){
                  sqlite3_bind_int64(pInsert, pCol->iBind, iRowid);
                }else if( pCol->iField<nVal ){
                  sqlite3_bind_value(pInsert, pCol->iBind, apVal[pCol->iField]);
                }
              }
            }
            if( p->bRecoverRowid && pTab->iRowidBind>0 && bHaveRowid ){
              sqlite3_bind_int64(pInsert, pTab->iRowidBind, iRowid);
            }

            if( SQLITE_ROW==sqlite3_step(pInsert) && p->xSql ){
              const char *zSql = (const char*)sqlite3_column_text(pInsert, 0);
              recoverSqlCallback(p, zSql);
            }
            recoverReset(p, pInsert);
            assert( p->errCode || pInsert );
            if( pInsert ) sqlite3_clear_bindings(pInsert);
          }

          for(ii=0; ii<nVal; ii++){
            sqlite3_value_free(apVal[ii]);
            apVal[ii] = 0;
          }
          nVal = -1;
          bHaveRowid = 0;
        }

        if( iRoot==0 ) continue;

        if( iRoot!=iPrevRoot ){
          pTab = recoverFindTable(p, iRoot);
          recoverFinalize(p, pInsert);
          pInsert = 0;
        }
      }

      if( iField<0 ){
        iRowid = sqlite3_column_int64(pSel, 4);
        assert( nVal==-1 );
        nVal = 0;
        bHaveRowid = 1;
      }else if( iField<nMax ){
        assert( apVal[iField]==0 );
        apVal[iField] = sqlite3_value_dup( pVal );
        nVal = iField+1;
      }
      iPrevRoot = iRoot;
      iPrevCell = iCell;
      iPrevPage = iPage;
    }

    recoverFinalize(p, pInsert);
    recoverFinalize(p, pSel);
  }

  sqlite3_free(apVal);
  return p->errCode;
}

sqlite3_recover *recoverInit(
  sqlite3* db, 
  const char *zDb, 
  const char *zUri,
  int (*xSql)(void*, const char*),
  void *pSqlCtx
){
  sqlite3_recover *pRet = 0;
  int nDb = 0;
  int nUri = 0;
  int nByte = 0;

  if( zDb==0 ){ zDb = "main"; }
  if( zUri==0 ){ zUri = ""; }

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
    memcpy(pRet->zUri, zUri, nUri);
    pRet->xSql = xSql;
    pRet->pSqlCtx = pSqlCtx;
  }

  return pRet;
}

sqlite3_recover *sqlite3_recover_init(
  sqlite3* db, 
  const char *zDb, 
  const char *zUri
){
  return recoverInit(db, zDb, zUri, 0, 0);
}

sqlite3_recover *sqlite3_recover_init_sql(
  sqlite3* db, 
  const char *zDb, 
  int (*xSql)(void*, const char*),
  void *pSqlCtx
){
  return recoverInit(db, zDb, "", xSql, pSqlCtx);
}

const char *sqlite3_recover_errmsg(sqlite3_recover *p){
  return p ? p->zErrMsg : "not an error";
}
int sqlite3_recover_errcode(sqlite3_recover *p){
  return p ? p->errCode : SQLITE_NOMEM;
}

int sqlite3_recover_config(sqlite3_recover *p, int op, void *pArg){
  int rc = SQLITE_OK;

  switch( op ){
    case SQLITE_RECOVER_TESTDB:
      sqlite3_free(p->zStateDb);
      p->zStateDb = sqlite3_mprintf("%s", (char*)pArg);
      break;

    case SQLITE_RECOVER_LOST_AND_FOUND:
      const char *zArg = (const char*)pArg;
      sqlite3_free(p->zLostAndFound);
      if( zArg ){
        p->zLostAndFound = recoverPrintf(p, "%s", zArg);
      }else{
        p->zLostAndFound = 0;
      }
      break;

    case SQLITE_RECOVER_FREELIST_CORRUPT:
      p->bFreelistCorrupt = (pArg ? 1 : 0);
      break;

    case SQLITE_RECOVER_ROWIDS:
      p->bRecoverRowid = (pArg ? 1 : 0);
      break;

    default:
      rc = SQLITE_NOTFOUND;
      break;
  }

  return rc;
}

static void recoverStep(sqlite3_recover *p){

  assert( p->errCode==SQLITE_OK );

  recoverSqlCallback(p, "PRAGMA writable_schema = on");

  if( p->dbOut==0 ){
    if( recoverOpenOutput(p) ) return;
    if( recoverCacheSchema(p) ) return;
    if( recoverWriteSchema1(p) ) return;
    if( recoverWriteData(p) ) return;
    if( p->zLostAndFound && recoverLostAndFound(p) ) return;
    if( recoverWriteSchema2(p) ) return;
  }

  recoverSqlCallback(p, "PRAGMA writable_schema = off");
}

int sqlite3_recover_step(sqlite3_recover *p){
  if( p && p->errCode==SQLITE_OK ){
    recoverStep(p);
  }
  return p ? p->errCode : SQLITE_NOMEM;
}

int sqlite3_recover_finish(sqlite3_recover *p){
  RecoverTable *pTab;
  RecoverTable *pNext;
  int rc;

  for(pTab=p->pTblList; pTab; pTab=pNext){
    pNext = pTab->pNext;
    sqlite3_free(pTab);
  }

  sqlite3_finalize(p->pGetPage);
  rc = sqlite3_close(p->dbOut);
  assert( rc==SQLITE_OK );
  p->pGetPage = 0;
  rc = p->errCode;

  sqlite3_free(p->zStateDb);
  sqlite3_free(p->zLostAndFound);
  sqlite3_free(p);
  return rc;
}


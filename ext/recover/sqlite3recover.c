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
*/
typedef struct RecoverTable RecoverTable;
struct RecoverTable {
  u32 iRoot;                      /* Root page in original database */
  char *zTab;                     /* Name of table */
  int nCol;                       /* Number of columns in table */
  RecoverColumn *aCol;            /* Array of columns */
  int bIntkey;                    /* True for intkey, false for without rowid */
  int iPk;                        /* Index of IPK column, if bIntkey */

  RecoverTable *pNext;
};

/*
** 
*/
#define RECOVERY_SCHEMA \
"  CREATE TABLE recovery.freelist("                            \
"      pgno INTEGER PRIMARY KEY"                               \
"  );"                                                         \
"  CREATE TABLE recovery.dbptr("                               \
"      pgno, child, PRIMARY KEY(child, pgno)"                  \
"  ) WITHOUT ROWID;"                                           \
"  CREATE TABLE recovery.map("                                 \
"      pgno INTEGER PRIMARY KEY, maxlen INT, intkey, root INT" \
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

  int errCode;                    /* For sqlite3_recover_errcode() */
  char *zErrMsg;                  /* For sqlite3_recover_errmsg() */

  char *zStateDb;
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

static void *recoverMalloc(sqlite3_recover *p, sqlite3_int64 nByte){
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
  sqlite3_int64 pgno = sqlite3_value_int64(apArg[0]);
  sqlite3_stmt *pStmt = 0;

  assert( nArg==1 );
  if( pgno==0 ){
    pStmt = recoverPreparePrintf(p, p->dbIn, "PRAGMA %Q.page_count", p->zDb);
  }else if( p->pGetPage==0 ){
    pStmt = recoverPreparePrintf(
        p, p->dbIn, "SELECT data FROM sqlite_dbpage(%Q) WHERE pgno=?", p->zDb
    );
  }else{
    pStmt = p->pGetPage;
  }

  if( pStmt ){
    if( pgno ) sqlite3_bind_int64(pStmt, 1, pgno);
    if( SQLITE_ROW==sqlite3_step(pStmt) ){
      sqlite3_result_value(pCtx, sqlite3_column_value(pStmt, 0));
    }
    if( pgno ){
      p->pGetPage = recoverReset(p, pStmt);
    }else{
      recoverFinalize(p, pStmt);
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
      const char *zPath = p->zStateDb ? p->zStateDb : ":memory:";
      char *zSql = sqlite3_mprintf("ATTACH %Q AS recovery", zPath);
      if( zSql==0 ){
        rc = p->errCode = SQLITE_NOMEM;
      }else{
        rc = sqlite3_exec(db, zSql, 0, 0, 0);
      }
      sqlite3_free(zSql);
    }

    if( rc==SQLITE_OK ){
      sqlite3_backup *pBackup = sqlite3_backup_init(db, "main", db, "recovery");
      if( pBackup ){
        while( sqlite3_backup_step(pBackup, 1000)==SQLITE_OK );
        rc = sqlite3_backup_finish(pBackup);
      }
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

    if( rc!=SQLITE_OK ){
      if( p->errCode==SQLITE_OK ) rc = recoverDbError(p, db);
      sqlite3_close(db);
    }else{
      p->dbOut = db;
    }
  }
  return rc;
}

static int recoverCacheDbptr(sqlite3_recover *p){
  return recoverExec(p, p->dbOut,
    "INSERT INTO recovery.dbptr "
    "SELECT pgno, child FROM sqlite_dbptr('getpage()')"
  );
}

static int recoverCacheSchema(sqlite3_recover *p){
  return recoverExec(p, p->dbOut,
    "WITH RECURSIVE pages(p) AS ("
    "  SELECT 1"
    "    UNION"
    "  SELECT child FROM recovery.dbptr, pages WHERE pgno=p"
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
      char *csr = 0;
      pNew->aCol = (RecoverColumn*)&pNew[1];
      pNew->zTab = csr = (char*)&pNew->aCol[nCol];
      pNew->nCol = nCol;
      pNew->iRoot = iRoot;
      pNew->iPk = -1;
      memcpy(csr, zName, nName);
      csr += nName+1;

      for(i=0; sqlite3_step(pStmt)==SQLITE_ROW; i++){
        int bPk = sqlite3_column_int(pStmt, 5);
        int n = sqlite3_column_bytes(pStmt, 1);
        const char *z = (const char*)sqlite3_column_text(pStmt, 1);
        int eHidden = sqlite3_column_int(pStmt, 6);

        if( bPk ) pNew->iPk = i;
        pNew->aCol[i].zCol = csr;
        pNew->aCol[i].eHidden = eHidden;
        memcpy(csr, z, n);
        csr += (n+1);
      }

      pNew->pNext = p->pTblList;
      p->pTblList = pNew;
    }

    recoverFinalize(p, pStmt);

    pStmt = recoverPreparePrintf(p, p->dbOut, "PRAGMA index_info(%Q)", zName);
    if( pStmt && sqlite3_step(pStmt)!=SQLITE_ROW ){
      pNew->bIntkey = 1;
    }else{
      pNew->iPk = -1;
    }
    recoverFinalize(p, pStmt);
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
      "  WHERE type='table' OR (type='index' AND sql LIKE '%unique%')"
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
  char *zSql = 0;
  char *zBind = 0;
  int ii;
  sqlite3_stmt *pRet = 0;

  assert( nField<=pTab->nCol );

  zSql = recoverMPrintf(p, "INSERT OR IGNORE INTO %Q(", pTab->zTab);
  for(ii=0; ii<nField; ii++){
    int eHidden = pTab->aCol[ii].eHidden;
    if( eHidden!=RECOVER_EHIDDEN_VIRTUAL
     && eHidden!=RECOVER_EHIDDEN_STORED
    ){
      zSql = recoverMPrintf(p, "%z%s%Q", zSql, zSep, pTab->aCol[ii].zCol);
      zBind = recoverMPrintf(p, "%z%s?", zBind, zSep);
      zSep = ", ";
    }
  }
  zSql = recoverMPrintf(p, "%z) VALUES (%z)", zSql, zBind);

  pRet = recoverPrepare(p, p->dbOut, zSql);
  sqlite3_free(zSql);
  
  return pRet;
}


static RecoverTable *recoverFindTable(sqlite3_recover *p, u32 iRoot){
  RecoverTable *pRet = 0;
  for(pRet=p->pTblList; pRet && pRet->iRoot!=iRoot; pRet=pRet->pNext);
  return pRet;
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

  apVal = (sqlite3_value**)recoverMalloc(p, sizeof(sqlite3_value*) * nMax);
  if( apVal==0 ) return p->errCode;

  pSel = recoverPrepare(p, p->dbOut, 
      "WITH RECURSIVE pages(root, page) AS ("
      "  SELECT rootpage, rootpage FROM recovery.schema"
      "    UNION"
      "   SELECT root, child FROM recovery.dbptr, pages WHERE pgno=page"
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

            for(ii=0; ii<pTab->nCol && iVal<nVal; ii++){
              int eHidden = pTab->aCol[ii].eHidden;
              switch( eHidden ){
                case RECOVER_EHIDDEN_NONE:
                case RECOVER_EHIDDEN_HIDDEN:
                  if( ii==pTab->iPk ){
                    sqlite3_bind_int64(pInsert, iBind, iRowid);
                  }else{
                    sqlite3_bind_value(pInsert, iBind, apVal[iVal]);
                  }
                  iBind++;
                  iVal++;
                  break;

                case RECOVER_EHIDDEN_VIRTUAL:
                  break;

                case RECOVER_EHIDDEN_STORED:
                  iVal++;
                  break;
              }
            }

            sqlite3_step(pInsert);
            recoverReset(p, pInsert);
            assert( p->errCode || pInsert );
            if( pInsert ) sqlite3_clear_bindings(pInsert);
          }

          for(ii=0; ii<nVal; ii++){
            sqlite3_value_free(apVal[ii]);
            apVal[ii] = 0;
          }
          nVal = -1;
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

sqlite3_recover *sqlite3_recover_init(
  sqlite3* db, 
  const char *zDb, 
  const char *zUri
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
  }

  return pRet;
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

    default:
      rc = SQLITE_NOTFOUND;
      break;
  }

  return rc;
}

static void recoverStep(sqlite3_recover *p){

  assert( p->errCode==SQLITE_OK );

  if( p->dbOut==0 ){
    if( recoverOpenOutput(p) ) return;
    if( recoverCacheDbptr(p) ) return;
    if( recoverCacheSchema(p) ) return;
    if( recoverWriteSchema1(p) ) return;
    if( recoverWriteData(p) ) return;
    if( recoverWriteSchema2(p) ) return;
  }
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

  sqlite3_free(p);
  return rc;
}


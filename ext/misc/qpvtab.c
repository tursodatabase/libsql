/*
** 2022-01-19
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
** This file implements a virtual-table that returns information about
** how the query planner called the xBestIndex method.  This virtual table
** is intended for testing and debugging only.
**
** The schema of the virtual table is this:
**
**    CREATE TABLE qpvtab(a,b,c,d,e, f,g,h,i,j, k,l,m,n,o, p,q,r,s,t);
**
** There is also a HIDDEN column "flags".
**
** All columns except column "a" have a value that is either TEXT that
** is there name, or INTEGER which is their index (b==1).  TEXT is the
** default, but INTEGER is used of there is a constraint on flags where the
** right-hand side is an integer that includes the 1 bit.
**
** The "a" column returns text that describes one of the parameters that
** xBestIndex was called with.  A completely query of the table should 
** show all details of how xBestIndex was called.
*/
#if !defined(SQLITEINT_H)
#include "sqlite3ext.h"
#endif
SQLITE_EXTENSION_INIT1
#include <string.h>
#include <assert.h>

#if !defined(SQLITE_OMIT_VIRTUALTABLE)

/* qpvtab_vtab is a subclass of sqlite3_vtab which is
** underlying representation of the virtual table
*/
typedef struct qpvtab_vtab qpvtab_vtab;
struct qpvtab_vtab {
  sqlite3_vtab base;  /* Base class - must be first */
};

/* qpvtab_cursor is a subclass of sqlite3_vtab_cursor which will
** serve as the underlying representation of a cursor that scans
** over rows of the result
*/
typedef struct qpvtab_cursor qpvtab_cursor;
struct qpvtab_cursor {
  sqlite3_vtab_cursor base;  /* Base class - must be first */
  sqlite3_int64 iRowid;      /* The rowid */
  const char *zData;         /* Data to return */
  int nData;                 /* Number of bytes of data */
  int flags;                 /* Flags value */
};

/*
** The qpvtabConnect() method is invoked to create a new
** qpvtab virtual table.
*/
static int qpvtabConnect(
  sqlite3 *db,
  void *pAux,
  int argc, const char *const*argv,
  sqlite3_vtab **ppVtab,
  char **pzErr
){
  qpvtab_vtab *pNew;
  int rc;

  rc = sqlite3_declare_vtab(db,
         "CREATE TABLE x(a,b,c,d,e, f,g,h,i,j, k,l,m,n,o, p,q,r,s,t,"
         " flags HIDDEN)"
       );
#define QPVTAB_A       0
#define QPVTAB_B       1
#define QPVTAB_T       19
#define QPVTAB_FLAGS   20
  if( rc==SQLITE_OK ){
    pNew = sqlite3_malloc( sizeof(*pNew) );
    *ppVtab = (sqlite3_vtab*)pNew;
    if( pNew==0 ) return SQLITE_NOMEM;
    memset(pNew, 0, sizeof(*pNew));
  }
  return rc;
}

/*
** This method is the destructor for qpvtab_vtab objects.
*/
static int qpvtabDisconnect(sqlite3_vtab *pVtab){
  qpvtab_vtab *p = (qpvtab_vtab*)pVtab;
  sqlite3_free(p);
  return SQLITE_OK;
}

/*
** Constructor for a new qpvtab_cursor object.
*/
static int qpvtabOpen(sqlite3_vtab *p, sqlite3_vtab_cursor **ppCursor){
  qpvtab_cursor *pCur;
  pCur = sqlite3_malloc( sizeof(*pCur) );
  if( pCur==0 ) return SQLITE_NOMEM;
  memset(pCur, 0, sizeof(*pCur));
  *ppCursor = &pCur->base;
  return SQLITE_OK;
}

/*
** Destructor for a qpvtab_cursor.
*/
static int qpvtabClose(sqlite3_vtab_cursor *cur){
  qpvtab_cursor *pCur = (qpvtab_cursor*)cur;
  sqlite3_free(pCur);
  return SQLITE_OK;
}


/*
** Advance a qpvtab_cursor to its next row of output.
*/
static int qpvtabNext(sqlite3_vtab_cursor *cur){
  qpvtab_cursor *pCur = (qpvtab_cursor*)cur;
  while( pCur->iRowid<pCur->nData && pCur->zData[pCur->iRowid]!='\n' ){
    pCur->iRowid++;
  }
  if( pCur->zData[pCur->iRowid]=='\n' ) pCur->iRowid++;
  return SQLITE_OK;
}

/*
** Return values of columns for the row at which the qpvtab_cursor
** is currently pointing.
*/
static int qpvtabColumn(
  sqlite3_vtab_cursor *cur,   /* The cursor */
  sqlite3_context *ctx,       /* First argument to sqlite3_result_...() */
  int i                       /* Which column to return */
){
  qpvtab_cursor *pCur = (qpvtab_cursor*)cur;
  if( i==0 && pCur->iRowid<pCur->nData ){
    int j;
    for(j=pCur->iRowid; j<pCur->nData && pCur->zData[j]!='\n'; j++){}
    sqlite3_result_text64(ctx, &pCur->zData[pCur->iRowid], j-pCur->iRowid,
                          SQLITE_TRANSIENT, SQLITE_UTF8);
  }else if( i>=QPVTAB_B && i<=QPVTAB_T ){
    if( pCur->flags & 1 ){
      sqlite3_result_int(ctx, i);
    }else{
      char x = 'a'+i;
      sqlite3_result_text64(ctx, &x, 1, SQLITE_TRANSIENT, SQLITE_UTF8);
    }
  }else if( i==QPVTAB_FLAGS ){
    sqlite3_result_int(ctx, pCur->flags);
  }
  return SQLITE_OK;
}

/*
** Return the rowid for the current row.  In this implementation, the
** rowid is the same as the output value.
*/
static int qpvtabRowid(sqlite3_vtab_cursor *cur, sqlite_int64 *pRowid){
  qpvtab_cursor *pCur = (qpvtab_cursor*)cur;
  *pRowid = pCur->iRowid;
  return SQLITE_OK;
}

/*
** Return TRUE if the cursor has been moved off of the last
** row of output.
*/
static int qpvtabEof(sqlite3_vtab_cursor *cur){
  qpvtab_cursor *pCur = (qpvtab_cursor*)cur;
  return pCur->iRowid>=pCur->nData;
}

/*
** This method is called to "rewind" the qpvtab_cursor object back
** to the first row of output.  This method is always called at least
** once prior to any call to qpvtabColumn() or qpvtabRowid() or 
** qpvtabEof().
*/
static int qpvtabFilter(
  sqlite3_vtab_cursor *pVtabCursor, 
  int idxNum, const char *idxStr,
  int argc, sqlite3_value **argv
){
  qpvtab_cursor *pCur = (qpvtab_cursor *)pVtabCursor;
  pCur->iRowid = 0;
  pCur->zData = idxStr;
  pCur->nData = (int)strlen(idxStr);
  pCur->flags = idxNum;
  return SQLITE_OK;
}

/*
** Append the text of a value to pStr
*/
static void qpvtabStrAppendValue(
  sqlite3_str *pStr,
  sqlite3_value *pVal
){
  switch( sqlite3_value_type(pVal) ){
    case SQLITE_NULL:
      sqlite3_str_appendf(pStr, "NULL");
      break;
    case SQLITE_INTEGER:
      sqlite3_str_appendf(pStr, "%lld", sqlite3_value_int64(pVal));
      break;
    case SQLITE_FLOAT:
      sqlite3_str_appendf(pStr, "%f", sqlite3_value_double(pVal));
      break;
    case SQLITE_TEXT:
      sqlite3_str_appendf(pStr, "%Q", sqlite3_value_text(pVal));
      break;
    case SQLITE_BLOB: {
      int i;
      const unsigned char *a = sqlite3_value_blob(pVal);
      int n = sqlite3_value_bytes(pVal);
      sqlite3_str_append(pStr, "x'", 2);
      for(i=0; i<n; i++){
        sqlite3_str_appendf(pStr, "%02x", a[i]);
      }
      sqlite3_str_append(pStr, "'", 1);
      break;
    }
  }
}

/*
** SQLite will invoke this method one or more times while planning a query
** that uses the virtual table.  This routine needs to create
** a query plan for each invocation and compute an estimated cost for that
** plan.
*/
static int qpvtabBestIndex(
  sqlite3_vtab *tab,
  sqlite3_index_info *pIdxInfo
){
  sqlite3_str *pStr = sqlite3_str_new(0);
  int i, k = 0;
  sqlite3_str_appendf(pStr, "nConstraint=%d\n", pIdxInfo->nConstraint);
  for(i=0; i<pIdxInfo->nConstraint; i++){
    sqlite3_value *pVal;
    int iCol = pIdxInfo->aConstraint[i].iColumn;
    char zCol[8];
    if( iCol==QPVTAB_FLAGS ){
      strcpy(zCol, "flags");
      if( pIdxInfo->aConstraint[i].usable ){
        pVal = 0;
        sqlite3_vtab_rhs_value(pIdxInfo, i, &pVal);
        if( pVal ){
          pIdxInfo->idxNum = sqlite3_value_int(pVal);
        }
      }
    }else{
      zCol[0] = iCol+'a';
      zCol[1] = 0;
    }
    sqlite3_str_appendf(pStr,"aConstraint[%d]: iColumn=%s op=%d usable=%d",
       i,
       zCol,
       pIdxInfo->aConstraint[i].op,
       pIdxInfo->aConstraint[i].usable);
    pVal = 0;
    sqlite3_vtab_rhs_value(pIdxInfo, i, &pVal);
    if( pVal ){
      sqlite3_str_appendf(pStr, " value=");
      qpvtabStrAppendValue(pStr, pVal);
    }
    sqlite3_str_append(pStr, "\n", 1);
    if( pIdxInfo->aConstraint[i].usable ){
      pIdxInfo->aConstraintUsage[i].argvIndex = ++k;   
      pIdxInfo->aConstraintUsage[i].omit = 1;
    }
  }
  pIdxInfo->estimatedCost = (double)10;
  pIdxInfo->estimatedRows = 10;
  sqlite3_str_appendf(pStr, "idxNum=%d\n", pIdxInfo->idxNum);
  pIdxInfo->idxStr = sqlite3_str_finish(pStr);
  pIdxInfo->needToFreeIdxStr = 1;
  return SQLITE_OK;
}

/*
** This following structure defines all the methods for the 
** virtual table.
*/
static sqlite3_module qpvtabModule = {
  /* iVersion    */ 0,
  /* xCreate     */ 0,
  /* xConnect    */ qpvtabConnect,
  /* xBestIndex  */ qpvtabBestIndex,
  /* xDisconnect */ qpvtabDisconnect,
  /* xDestroy    */ 0,
  /* xOpen       */ qpvtabOpen,
  /* xClose      */ qpvtabClose,
  /* xFilter     */ qpvtabFilter,
  /* xNext       */ qpvtabNext,
  /* xEof        */ qpvtabEof,
  /* xColumn     */ qpvtabColumn,
  /* xRowid      */ qpvtabRowid,
  /* xUpdate     */ 0,
  /* xBegin      */ 0,
  /* xSync       */ 0,
  /* xCommit     */ 0,
  /* xRollback   */ 0,
  /* xFindMethod */ 0,
  /* xRename     */ 0,
  /* xSavepoint  */ 0,
  /* xRelease    */ 0,
  /* xRollbackTo */ 0,
  /* xShadowName */ 0
};
#endif /* SQLITE_OMIT_VIRTUALTABLE */


#ifdef _WIN32
__declspec(dllexport)
#endif
int sqlite3_qpvtab_init(
  sqlite3 *db, 
  char **pzErrMsg, 
  const sqlite3_api_routines *pApi
){
  int rc = SQLITE_OK;
  SQLITE_EXTENSION_INIT2(pApi);
#ifndef SQLITE_OMIT_VIRTUALTABLE
  rc = sqlite3_create_module(db, "qpvtab", &qpvtabModule, 0);
#endif
  return rc;
}

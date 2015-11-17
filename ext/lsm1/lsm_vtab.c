/*
** 2015-11-16
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
** This file implements a simple virtual table wrapper around the LSM
** storage engine from SQLite4.
*/
#include "sqlite3ext.h"
SQLITE_EXTENSION_INIT1
#include "lsm.h"
#include <assert.h>
#include <string.h>

/* Forward declaration of subclasses of virtual table objects */
typedef struct lsm1_vtab lsm1_vtab;
typedef struct lsm1_cursor lsm1_cursor;

/* Primitive types */
typedef unsigned char u8;

/* An open connection to an LSM table */
struct lsm1_vtab {
  sqlite3_vtab base;          /* Base class - must be first */
  lsm_db *pDb;                /* Open connection to the LSM table */
};


/* lsm1_cursor is a subclass of sqlite3_vtab_cursor which will
** serve as the underlying representation of a cursor that scans
** over rows of the result
*/
struct lsm1_cursor {
  sqlite3_vtab_cursor base;  /* Base class - must be first */
  lsm_cursor *pLsmCur;       /* The LSM cursor */
  u8 isDesc;                 /* 0: scan forward.  1: scan reverse */
  u8 atEof;                  /* True if the scan is complete */
  u8 bUnique;                /* True if no more than one row of output */
};

/*
** The lsm1Connect() method is invoked to create a new
** lsm1_vtab that describes the virtual table.
*/
static int lsm1Connect(
  sqlite3 *db,
  void *pAux,
  int argc, const char *const*argv,
  sqlite3_vtab **ppVtab,
  char **pzErr
){
  lsm1_vtab *pNew;
  int rc;

  if( argc!=4 || argv[3]==0 || argv[3][0]==0 ){
    *pzErr = sqlite3_mprintf("filename argument missing");
    return SQLITE_ERROR;
  }
  *ppVtab = sqlite3_malloc( sizeof(*pNew) );
  pNew = (lsm1_vtab*)*ppVtab;
  if( pNew==0 ){
    return SQLITE_NOMEM;
  }
  memset(pNew, 0, sizeof(*pNew));
  rc = lsm_new(0, &pNew->pDb);
  if( rc ){
    *pzErr = sqlite3_mprintf("lsm_new failed with error code %d",  rc);
    rc = SQLITE_ERROR;
    goto connect_failed;
  }
  rc = lsm_open(pNew->pDb, argv[0]);
  if( rc ){
    *pzErr = sqlite3_mprintf("lsm_open failed with %d", rc);
    rc = SQLITE_ERROR;
    goto connect_failed;
  }

/* Column numbers */
#define LSM1_COLUMN_KEY      0
#define LSM1_COLUMN_VALUE    1
#define LSM1_COLUMN_COMMAND  2

  rc = sqlite3_declare_vtab(db,
     "CREATE TABLE x(key,value,command hidden)");
connect_failed:
  if( rc!=SQLITE_OK ){
    if( pNew ){
      if( pNew->pDb ) lsm_close(pNew->pDb);
      sqlite3_free(pNew);
    }
    *ppVtab = 0;
  }
  return rc;
}

/*
** This method is the destructor for lsm1_cursor objects.
*/
static int lsm1Disconnect(sqlite3_vtab *pVtab){
  lsm1_vtab *p = (lsm1_vtab*)pVtab;
  lsm_close(p->pDb);
  sqlite3_free(p);
  return SQLITE_OK;
}

/*
** Constructor for a new lsm1_cursor object.
*/
static int lsm1Open(sqlite3_vtab *pVtab, sqlite3_vtab_cursor **ppCursor){
  lsm1_vtab *p = (lsm1_vtab*)pVtab;
  lsm1_cursor *pCur;
  int rc;
  pCur = sqlite3_malloc( sizeof(*pCur) );
  if( pCur==0 ) return SQLITE_NOMEM;
  memset(pCur, 0, sizeof(*pCur));
  *ppCursor = &pCur->base;
  rc = lsm_csr_open(p->pDb, &pCur->pLsmCur);
  if( rc==LSM_OK ){
    rc = SQLITE_OK;
  }else{
    sqlite3_free(pCur);
    *ppCursor = 0;
    rc = SQLITE_ERROR;
  }
  return rc;
}

/*
** Destructor for a lsm1_cursor.
*/
static int lsm1Close(sqlite3_vtab_cursor *cur){
  lsm1_cursor *pCur = (lsm1_cursor*)cur;
  lsm_csr_close(pCur->pLsmCur);
  sqlite3_free(pCur);
  return SQLITE_OK;
}


/*
** Advance a lsm1_cursor to its next row of output.
*/
static int lsm1Next(sqlite3_vtab_cursor *cur){
  lsm1_cursor *pCur = (lsm1_cursor*)cur;
  int rc;
  if( pCur->bUnique ){
    pCur->atEof = 1;
  }else{
    if( pCur->isDesc ){
      rc = lsm_csr_prev(pCur->pLsmCur);
    }else{
      rc = lsm_csr_next(pCur->pLsmCur);
    }
    if( rc==LSM_OK && lsm_csr_valid(pCur->pLsmCur)==0 ){
      pCur->atEof = 1;
    }
  }
  return rc==LSM_OK ? SQLITE_OK : SQLITE_ERROR;
}

/*
** Return TRUE if the cursor has been moved off of the last
** row of output.
*/
static int lsm1Eof(sqlite3_vtab_cursor *cur){
  lsm1_cursor *pCur = (lsm1_cursor*)cur;
  return pCur->atEof;
}

/*
** Return values of columns for the row at which the lsm1_cursor
** is currently pointing.
*/
static int lsm1Column(
  sqlite3_vtab_cursor *cur,   /* The cursor */
  sqlite3_context *ctx,       /* First argument to sqlite3_result_...() */
  int i                       /* Which column to return */
){
  lsm1_cursor *pCur = (lsm1_cursor*)cur;
  switch( i ){
    case LSM1_COLUMN_KEY: {
      const void *pVal;
      int nVal;
      if( lsm_csr_value(pCur->pLsmCur, (const void**)&pVal, &nVal)==LSM_OK ){
        sqlite3_result_blob(ctx, pVal, nVal, SQLITE_TRANSIENT);
      }
      break;
    }
    case LSM1_COLUMN_VALUE: {
      const unsigned char *aVal;
      int nVal;
      if( lsm_csr_value(pCur->pLsmCur, (const void**)&aVal, &nVal)==LSM_OK
          && nVal>=1
      ){
        switch( aVal[0] ){
          case SQLITE_FLOAT:
          case SQLITE_INTEGER: {
            sqlite3_uint64 x = 0;
            int j;
            for(j=1; j<=8; j++){
              x = (x<<8) | aVal[j];
            }
            if( aVal[0]==SQLITE_INTEGER ){
              sqlite3_result_int64(ctx, *(sqlite3_int64*)&x);
            }else{
              sqlite3_result_double(ctx, *(double*)&x);
            }
            break;
          }
          case SQLITE_TEXT: {
            sqlite3_result_text(ctx, (char*)&aVal[1], nVal-1, SQLITE_TRANSIENT);
            break;
          }
          case SQLITE_BLOB: {
            sqlite3_result_blob(ctx, &aVal[1], nVal-1, SQLITE_TRANSIENT);
            break;
          }
        }
      }
      break;
    }
    default: {
      break;
    }
  }
  return SQLITE_OK;
}

/*
** Rowids are not supported by the underlying virtual table.  So always
** return 0 for the rowid.
*/
static int lsm1Rowid(sqlite3_vtab_cursor *cur, sqlite_int64 *pRowid){
  *pRowid = 0;
  return SQLITE_OK;
}

/* Move to the first row to return.
*/
static int lsm1Filter(
  sqlite3_vtab_cursor *pVtabCursor, 
  int idxNum, const char *idxStr,
  int argc, sqlite3_value **argv
){
  lsm1_cursor *pCur = (lsm1_cursor *)pVtabCursor;
  int rc;
  if( idxNum==1 ){
    assert( argc==1 );
    pCur->isDesc = 0;
    pCur->bUnique = 1;
    pCur->atEof = 1;
    if( sqlite3_value_type(argv[0])==SQLITE_BLOB ){
      const void *pVal = sqlite3_value_blob(argv[0]);
      int nVal = sqlite3_value_bytes(argv[0]);
      rc = lsm_csr_seek(pCur->pLsmCur, pVal, nVal, LSM_SEEK_EQ);
      if( rc==LSM_OK && lsm_csr_valid(pCur->pLsmCur)!=0 ){
        pCur->atEof = 0;
      }
    }
  }else{
    rc = lsm_csr_first(pCur->pLsmCur);
    pCur->atEof = 0;
    pCur->isDesc = 0;
    pCur->bUnique = 0;
  }
  return rc==LSM_OK ? SQLITE_OK : SQLITE_ERROR;
}

/*
** Only comparisons against the key are allowed.  The idxNum defines
** which comparisons are available:
**
**     0      Full table scan only
**     1      key==?  single argument for ?
**    
*/
static int lsm1BestIndex(
  sqlite3_vtab *tab,
  sqlite3_index_info *pIdxInfo
){
  int i;                 /* Loop over constraints */
  int idxNum = 0;        /* The query plan bitmask */
  int nArg = 0;          /* Number of arguments to xFilter */
  int eqIdx = -1;        /* Index of the key== constraint, or -1 if none */

  const struct sqlite3_index_constraint *pConstraint;
  pConstraint = pIdxInfo->aConstraint;
  for(i=0; i<pIdxInfo->nConstraint && idxNum<16; i++, pConstraint++){
    if( pConstraint->usable==0 ) continue;
    if( pConstraint->iColumn!=LSM1_COLUMN_KEY ) continue;
    if( pConstraint->op!=SQLITE_INDEX_CONSTRAINT_EQ ) continue;
    switch( pConstraint->op ){
      case SQLITE_INDEX_CONSTRAINT_EQ: {
        eqIdx = i;
        idxNum = 1;
        break;
      }
    }
  }
  if( eqIdx>=0 ){
    pIdxInfo->aConstraintUsage[eqIdx].argvIndex = ++nArg;
    pIdxInfo->aConstraintUsage[eqIdx].omit = 1;
  }
  if( idxNum==1 ){
    pIdxInfo->estimatedCost = (double)1;
    pIdxInfo->estimatedRows = 1;
    pIdxInfo->orderByConsumed = 1;
  }else{
    /* Full table scan */
    pIdxInfo->estimatedCost = (double)2147483647;
    pIdxInfo->estimatedRows = 2147483647;
  }
  pIdxInfo->idxNum = idxNum;
  return SQLITE_OK;
}

/*
** The xUpdate method is normally used for INSERT, REPLACE, UPDATE, and
** DELETE.  But this virtual table only supports INSERT and REPLACE.
** DELETE is accomplished by inserting a record with a value of NULL.
** UPDATE is achieved by using REPLACE.
*/
int lsm1Update(
  sqlite3_vtab *pVTab,
  int argc,
  sqlite3_value **argv,
  sqlite_int64 *pRowid
){
  lsm1_vtab *p = (lsm1_vtab*)pVTab;
  const void *pKey;
  int nKey;
  int eType;
  int rc;
  sqlite3_value *pValue;
  if( argc==1 ){
    pVTab->zErrMsg = sqlite3_mprintf("cannot DELETE");
    return SQLITE_ERROR;
  }
  if( sqlite3_value_type(argv[0])!=SQLITE_NULL ){
    pVTab->zErrMsg = sqlite3_mprintf("cannot UPDATE");
    return SQLITE_ERROR;
  }

  /* "INSERT INTO tab(command) VALUES('....')" is used to implement
  ** special commands.
  */
  if( sqlite3_value_type(argv[2+LSM1_COLUMN_COMMAND])!=SQLITE_NULL ){
    return SQLITE_OK;
  }
  if( sqlite3_value_type(argv[2+LSM1_COLUMN_KEY])!=SQLITE_BLOB ){
    pVTab->zErrMsg = sqlite3_mprintf("BLOB keys only");
    return SQLITE_ERROR;
  }
  pKey = sqlite3_value_blob(argv[2+LSM1_COLUMN_KEY]);
  nKey = sqlite3_value_bytes(argv[2+LSM1_COLUMN_KEY]);
  pValue = argv[2+LSM1_COLUMN_VALUE];
  eType = sqlite3_value_type(pValue);
  switch( eType ){
    case SQLITE_NULL: {
      rc = lsm_delete(p->pDb, pKey, nKey);
      break;
    }
    case SQLITE_BLOB:
    case SQLITE_TEXT: {
      const unsigned char *pVal;
      unsigned char *pData;
      int nVal;
      if( eType==SQLITE_TEXT ){
        pVal = sqlite3_value_text(pValue);
      }else{
        pVal = (unsigned char*)sqlite3_value_blob(pValue);
      }
      nVal = sqlite3_value_bytes(pValue);
      pData = sqlite3_malloc( nVal+1 );
      if( pData==0 ){
        rc = SQLITE_NOMEM;
      }else{
        pData[0] = eType;
        memcpy(&pData[1], pVal, nVal);
        rc = lsm_insert(p->pDb, pKey, nKey, pData, nVal+1);
        sqlite3_free(pData);
      }
      break;
    }
    case SQLITE_INTEGER:
    case SQLITE_FLOAT: {
      sqlite3_uint64 x;
      unsigned char aVal[9];
      int i;
      if( eType==SQLITE_INTEGER ){
        *(sqlite3_int64*)&x = sqlite3_value_int64(pValue);
      }else{
        *(double*)&x = sqlite3_value_double(pValue);
      }
      for(i=9; i>=1; i--){
        aVal[i] = x & 0xff;
        x >>= 8;
      }
      aVal[0] = eType;
      rc = lsm_insert(p->pDb, pKey, nKey, aVal, 9);
      break;
    }
  }
  return rc==LSM_OK ? SQLITE_OK : SQLITE_ERROR;
}      

/* Begin a transaction
*/
static int lsm1Begin(sqlite3_vtab *pVtab){
  lsm1_vtab *p = (lsm1_vtab*)pVtab;
  int rc = lsm_begin(p->pDb, 1);
  return rc==LSM_OK ? SQLITE_OK : SQLITE_ERROR;
}

/* Phase 1 of a transaction commit.
*/
static int lsm1Sync(sqlite3_vtab *pVtab){
  return SQLITE_OK;
}

/* Commit a transaction
*/
static int lsm1Commit(sqlite3_vtab *pVtab){
  lsm1_vtab *p = (lsm1_vtab*)pVtab;
  int rc = lsm_commit(p->pDb, 0);
  return rc==LSM_OK ? SQLITE_OK : SQLITE_ERROR;
}

/* Rollback a transaction
*/
static int lsm1Rollback(sqlite3_vtab *pVtab){
  lsm1_vtab *p = (lsm1_vtab*)pVtab;
  int rc = lsm_rollback(p->pDb, 0);
  return rc==LSM_OK ? SQLITE_OK : SQLITE_ERROR;
}

/*
** This following structure defines all the methods for the 
** generate_lsm1 virtual table.
*/
static sqlite3_module lsm1Module = {
  0,                       /* iVersion */
  lsm1Connect,             /* xCreate */
  lsm1Connect,             /* xConnect */
  lsm1BestIndex,           /* xBestIndex */
  lsm1Disconnect,          /* xDisconnect */
  lsm1Disconnect,          /* xDestroy */
  lsm1Open,                /* xOpen - open a cursor */
  lsm1Close,               /* xClose - close a cursor */
  lsm1Filter,              /* xFilter - configure scan constraints */
  lsm1Next,                /* xNext - advance a cursor */
  lsm1Eof,                 /* xEof - check for end of scan */
  lsm1Column,              /* xColumn - read data */
  lsm1Rowid,               /* xRowid - read data */
  lsm1Update,              /* xUpdate */
  lsm1Begin,               /* xBegin */
  lsm1Sync,                /* xSync */
  lsm1Commit,              /* xCommit */
  lsm1Rollback,            /* xRollback */
  0,                       /* xFindMethod */
  0,                       /* xRename */
};


#ifdef _WIN32
__declspec(dllexport)
#endif
int sqlite3_lsm_init(
  sqlite3 *db, 
  char **pzErrMsg, 
  const sqlite3_api_routines *pApi
){
  int rc = SQLITE_OK;
  SQLITE_EXTENSION_INIT2(pApi);
  rc = sqlite3_create_module(db, "lsm1", &lsm1Module, 0);
  return rc;
}

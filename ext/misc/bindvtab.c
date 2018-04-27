/*
** 2018-04-27
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
** This file implements a simple key/value store used to hold bind
** parameters for SQLite.  The key/value store is a singleton - there
** is exactly one per process.  The store can be accessed and controlled
** from SQLite using an eponymous virtual table.
*/
#if !defined(SQLITEINT_H)
#include "sqlite3ext.h"
#endif
SQLITE_EXTENSION_INIT1
#include <string.h>
#include <assert.h>
#include <stdlib.h>

/* Each entry in the key/value store */
typedef struct BindingEntry  BindingEntry;
struct BindingEntry {
  char *zKey;             /* Key */
  BindingEntry *pNext;    /* Next entry in the list */
  BindingEntry *pPrev;    /* Previous entry in the list */
  int eType;              /* SQLITE_INTEGER, _FLOAT, _TEXT, or _BLOB */
  int len;                /* Length for SQLITE_BLOB values */
  union {
    sqlite3_int64 i;         /* Integer value */
    double r;                /* Real value */
    char *z;                 /* Text value */
    unsigned char *b;        /* Blob value */
  } u;
};

/* Global list of all entries */
static BindingEntry *global_pAll = 0;

/* Locate any entry with the given key.  Return NULL if not found.
*/
static BindingEntry *shellBindingFind(const char *zKey){
  BindingEntry *p;
  for(p=global_pAll; p && strcmp(p->zKey,zKey)!=0; p = p->pNext){}
  return p;
}

/* Delete any entry with the given key, if it exists.
*/
static void shellBindingDelete(const char *zKey){
  BindingEntry *p;
  p = shellBindingFind(zKey);
  if( p ){
    if( p->pNext ){
      p->pNext->pPrev = p->pPrev;
    }
    if( p->pPrev ){
      p->pPrev->pNext = p->pNext;
    }else{
      global_pAll = p->pNext;
    }
    free(p);
  }
}

/* Insert a new shell binding */
static void shellBindingInsert(BindingEntry *p){
  p->pNext = global_pAll;
  if( global_pAll ) global_pAll->pPrev = p;
  global_pAll = p;
  p->pPrev = 0;
}

/*
** True if c is a valid ID character.
*/
static int shellBindIdChar(char c){
  if( c>='a' && c<='z' ) return 1;
  if( c>='A' && c<='Z' ) return 1;
  if( c=='_' ) return 1;
  if( c>='0' && c<='9' ) return 2;
  return 0;
}

/* Create a new binding given a string of the form "KEY=VALUE".  Return
** values:
**
**    0:    success
**    1:    out of memory
**    2:    Argument is not a valid KEY=VALUE string
**
** The type of VALUE is TEXT.
*/
int shell_bindings_new_text(const char *z){
  int i;
  int nKey;
  int nData;
  BindingEntry *p;
  for(i=0; shellBindIdChar(z[i]); i++){}
  if( i==0 ) return 2;
  if( shellBindIdChar(z[0])==2 ) return 2;
  nKey = i;
  if( z[i]!='=' ) return 2;
  for(nData=0; z[nKey+1+nData]; nData++){}
  p = malloc( sizeof(*p) + nKey + nData + 2 );
  if( p==0 ) return 1;
  memset(p, 0, sizeof(*p));
  p->zKey = (char*)&p[1];
  memcpy(p->zKey, z, nKey);
  p->zKey[nKey] = 0;
  p->u.z = &p->zKey[nKey+1];
  p->len = nData;
  p->eType = SQLITE_TEXT;
  memcpy(p->u.z, &z[nKey+1], nData+1);
  shellBindingDelete(p->zKey);
  shellBindingInsert(p);
  return 0;
}

/*
** Delete all shell bindings
*/
void shell_bindings_clear(void){
  BindingEntry *pNext;
  while( global_pAll ){
    pNext = global_pAll->pNext;
    free(global_pAll);
    global_pAll = pNext;
  }
}

/* Given a prepared statement, apply all bindings for which there are
** known values in the k-v store
*/
void shell_bindings_apply(sqlite3_stmt *pStmt){
  int n = sqlite3_bind_parameter_count(pStmt);
  int i;
  BindingEntry *p;
  for(i=1; i<=n; i++){
    const char *zKey = sqlite3_bind_parameter_name(pStmt, i);
    if( zKey==0 || zKey[0]==0 ) continue;
    zKey++;
    p = shellBindingFind(zKey);
    if( p==0 ) continue;
    switch( p->eType ){
      case SQLITE_INTEGER:
        sqlite3_bind_int64(pStmt, i, p->u.i);
        break;
      case SQLITE_FLOAT:
        sqlite3_bind_double(pStmt, i, p->u.r);
        break;
      case SQLITE_TEXT:
        sqlite3_bind_text(pStmt, i, p->u.z, p->len, SQLITE_TRANSIENT);
        break;
      case SQLITE_BLOB:
        sqlite3_bind_blob(pStmt, i, p->u.b, p->len, SQLITE_TRANSIENT);
        break;
    }
  }
}

/* bindvtab_vtab is a subclass of sqlite3_vtab which is
** underlying representation of the virtual table
*/
typedef struct bindvtab_vtab bindvtab_vtab;
struct bindvtab_vtab {
  sqlite3_vtab base;  /* Base class - must be first */
};

/* bindvtab_cursor is a subclass of sqlite3_vtab_cursor which will
** serve as the underlying representation of a cursor that scans
** over rows of the result
*/
typedef struct bindvtab_cursor bindvtab_cursor;
struct bindvtab_cursor {
  sqlite3_vtab_cursor base;  /* Base class - must be first */
  BindingEntry *p;           /* Current entry in the scan */
};

/*
** The bindvtabConnect() method is invoked to create a new
** template virtual table.
**
** Think of this routine as the constructor for bindvtab_vtab objects.
**
** All this routine needs to do is:
**
**    (1) Allocate the bindvtab_vtab object and initialize all fields.
**
**    (2) Tell SQLite (via the sqlite3_declare_vtab() interface) what the
**        result set of queries against the virtual table will look like.
*/
static int bindvtabConnect(
  sqlite3 *db,
  void *pAux,
  int argc, const char *const*argv,
  sqlite3_vtab **ppVtab,
  char **pzErr
){
  bindvtab_vtab *pNew;
  int rc;

  rc = sqlite3_declare_vtab(db,
           "CREATE TABLE shell_bindings(k TEXT PRIMARY KEY,v)"
           " WITHOUT ROWID"
       );
  /* For convenience, define symbolic names for the index to each column. */
#define BINDVTAB_KEY    0
#define BINDVTAB_VALUE  1
  if( rc==SQLITE_OK ){
    pNew = sqlite3_malloc( sizeof(*pNew) );
    *ppVtab = (sqlite3_vtab*)pNew;
    if( pNew==0 ) return SQLITE_NOMEM;
    memset(pNew, 0, sizeof(*pNew));
  }
  return rc;
}

/*
** This method is the destructor for bindvtab_vtab objects.
*/
static int bindvtabDisconnect(sqlite3_vtab *pVtab){
  bindvtab_vtab *p = (bindvtab_vtab*)pVtab;
  sqlite3_free(p);
  return SQLITE_OK;
}

/*
** Constructor for a new bindvtab_cursor object.
*/
static int bindvtabOpen(sqlite3_vtab *p, sqlite3_vtab_cursor **ppCursor){
  bindvtab_cursor *pCur;
  pCur = sqlite3_malloc( sizeof(*pCur) );
  if( pCur==0 ) return SQLITE_NOMEM;
  memset(pCur, 0, sizeof(*pCur));
  *ppCursor = &pCur->base;
  return SQLITE_OK;
}

/*
** Destructor for a bindvtab_cursor.
*/
static int bindvtabClose(sqlite3_vtab_cursor *cur){
  bindvtab_cursor *pCur = (bindvtab_cursor*)cur;
  sqlite3_free(pCur);
  return SQLITE_OK;
}


/*
** Advance a bindvtab_cursor to its next row of output.
*/
static int bindvtabNext(sqlite3_vtab_cursor *cur){
  bindvtab_cursor *pCur = (bindvtab_cursor*)cur;
  pCur->p = pCur->p->pNext;
  return SQLITE_OK;
}

/*
** Return values of columns for the row at which the bindvtab_cursor
** is currently pointing.
*/
static int bindvtabColumn(
  sqlite3_vtab_cursor *cur,   /* The cursor */
  sqlite3_context *ctx,       /* First argument to sqlite3_result_...() */
  int i                       /* Which column to return */
){
  bindvtab_cursor *pCur = (bindvtab_cursor*)cur;
  BindingEntry *p = pCur->p;
  if( i==BINDVTAB_KEY ){
    sqlite3_result_text(ctx, p->zKey, -1, SQLITE_TRANSIENT);
  }else{
    assert( i==BINDVTAB_VALUE );
    switch( p->eType ){
      case SQLITE_INTEGER:
        sqlite3_result_int(ctx, p->u.i);
        break;
      case SQLITE_FLOAT:
        sqlite3_result_double(ctx, p->u.r);
        break;
      case SQLITE_TEXT:
        sqlite3_result_text(ctx, p->u.z, p->len, SQLITE_TRANSIENT);
        break;
      case SQLITE_BLOB:
        sqlite3_result_blob(ctx, p->u.b, p->len, SQLITE_TRANSIENT);
        break;
    }
  }
  return SQLITE_OK;
}

/*
** Return the rowid for the current row.  In this implementation, the
** rowid is the same as the output value.
*/
static int bindvtabRowid(sqlite3_vtab_cursor *cur, sqlite_int64 *pRowid){
  return SQLITE_OK;
}

/*
** Return TRUE if the cursor has been moved off of the last
** row of output.
*/
static int bindvtabEof(sqlite3_vtab_cursor *cur){
  bindvtab_cursor *pCur = (bindvtab_cursor*)cur;
  return pCur->p==0;
}

/*
** This method is called to "rewind" the bindvtab_cursor object back
** to the first row of output.  This method is always called at least
** once prior to any call to bindvtabColumn() or bindvtabRowid() or 
** bindvtabEof().
*/
static int bindvtabFilter(
  sqlite3_vtab_cursor *pVtabCursor, 
  int idxNum, const char *idxStr,
  int argc, sqlite3_value **argv
){
  bindvtab_cursor *pCur = (bindvtab_cursor *)pVtabCursor;
  pCur->p = global_pAll;
  return SQLITE_OK;
}

/*
** SQLite will invoke this method one or more times while planning a query
** that uses the virtual table.  This routine needs to create
** a query plan for each invocation and compute an estimated cost for that
** plan.
*/
static int bindvtabBestIndex(
  sqlite3_vtab *tab,
  sqlite3_index_info *pIdxInfo
){
  pIdxInfo->estimatedCost = (double)10;
  pIdxInfo->estimatedRows = 10;
  return SQLITE_OK;
}

/*
** Called to make changes to the shell bindings
*/
static int bindvtabUpdate(
  sqlite3_vtab *pVTab,
  int argc,
  sqlite3_value **argv,
  sqlite_int64 *pRowid
){
  const char *zKey;
  BindingEntry *p;
  int nKey;
  int len;
  int eType;
  if( sqlite3_value_type(argv[0])!=SQLITE_NULL ){
    zKey = (const char*)sqlite3_value_text(argv[0]);
    if( zKey ) shellBindingDelete(zKey);
  }
  if( argc==1 ) return SQLITE_OK;
  eType = sqlite3_value_type(argv[3]);
  if( eType==SQLITE_NULL ) return SQLITE_OK;
  zKey = (const char*)sqlite3_value_text(argv[2]);
  if( zKey==0 ) return SQLITE_OK;
  nKey = sqlite3_value_bytes(argv[2]);
  shellBindingDelete(zKey);
  if( eType==SQLITE_BLOB || eType==SQLITE_TEXT ){
    len = sqlite3_value_bytes(argv[3]);
  }else{
    len = 0;
  }
  p = malloc( sizeof(*p) + nKey + len + 2 );
  if( p==0 ) return SQLITE_NOMEM;
  memset(p, 0, sizeof(*p));
  p->zKey = (char*)&p[1];
  memcpy(p->zKey, zKey, nKey+1);
  p->eType = eType;
  switch( eType ){
    case SQLITE_INTEGER: 
       p->u.i = sqlite3_value_int64(argv[3]);
       break;
    case SQLITE_FLOAT: 
       p->u.r = sqlite3_value_double(argv[3]);
       break;
    case SQLITE_TEXT:
       p->u.z = &p->zKey[nKey+1];
       memcpy(p->u.z, sqlite3_value_text(argv[3]), len);
       break;
    case SQLITE_BLOB:
       p->u.b = (unsigned char*)&p->zKey[nKey+1];
       memcpy(p->u.b, sqlite3_value_blob(argv[3]), len);
       break;
  }
  shellBindingInsert(p);
  return SQLITE_OK;
}

/*
** This following structure defines all the methods for the 
** virtual table.
*/
static sqlite3_module bindvtabModule = {
  /* iVersion    */ 0,
  /* xCreate     */ 0,
  /* xConnect    */ bindvtabConnect,
  /* xBestIndex  */ bindvtabBestIndex,
  /* xDisconnect */ bindvtabDisconnect,
  /* xDestroy    */ 0,
  /* xOpen       */ bindvtabOpen,
  /* xClose      */ bindvtabClose,
  /* xFilter     */ bindvtabFilter,
  /* xNext       */ bindvtabNext,
  /* xEof        */ bindvtabEof,
  /* xColumn     */ bindvtabColumn,
  /* xRowid      */ bindvtabRowid,
  /* xUpdate     */ bindvtabUpdate,
  /* xBegin      */ 0,
  /* xSync       */ 0,
  /* xCommit     */ 0,
  /* xRollback   */ 0,
  /* xFindMethod */ 0,
  /* xRename     */ 0,
  /* xSavepoint  */ 0,
  /* xRelease    */ 0,
  /* xRollbackTo */ 0
};


#ifdef _WIN32
__declspec(dllexport)
#endif
int sqlite3_bindvtab_init(
  sqlite3 *db, 
  char **pzErrMsg, 
  const sqlite3_api_routines *pApi
){
  int rc = SQLITE_OK;
  SQLITE_EXTENSION_INIT2(pApi);
  rc = sqlite3_create_module(db, "shell_bindings", &bindvtabModule, 0);
  return rc;
}

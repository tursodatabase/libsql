/*
** 2024-04-25
**
** Copyright 2024 the libSQL authors
**
** Permission is hereby granted, free of charge, to any person obtaining a copy of
** this software and associated documentation files (the "Software"), to deal in
** the Software without restriction, including without limitation the rights to
** use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
** the Software, and to permit persons to whom the Software is furnished to do so,
** subject to the following conditions:
**
** The above copyright notice and this permission notice shall be included in all
** copies or substantial portions of the Software.
**
** THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
** IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
** FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
** COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
** IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
** CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
**
******************************************************************************
**
** libSQL vector search.
*/
#include "sqliteInt.h"
#if !defined(SQLITE_OMIT_VECTOR) && !defined(SQLITE_OMIT_VIRTUALTABLE)
#include "vdbeInt.h"
#include "vectorInt.h"

typedef struct vectorVtab vectorVtab;
struct vectorVtab {
  sqlite3_vtab base;     /* Base class - must be first */
  sqlite3 *db;           /* Database connection */
};

typedef struct vectorVtab_cursor vectorVtab_cursor;
struct vectorVtab_cursor {
  sqlite3_vtab_cursor base;  /* Base class - must be first */
  unsigned int nRowidOffset;
  unsigned int aRowids;
  i64 *pRowids;
};

static int vectorVtabConnect(
  sqlite3 *db,
  void *pAux,
  int argc, const char *const*argv,
  sqlite3_vtab **ppVtab,
  char **pzErr
){
  vectorVtab *pVtab;
  int rc;

/* Column numbers */
#define VECTOR_COLUMN_ID     0
#define VECTOR_COLUMN_IDX    1
#define VECTOR_COLUMN_VECTOR 2
#define VECTOR_COLUMN_K      3

  rc = sqlite3_declare_vtab(db, "CREATE TABLE x(id, idx hidden, vector hidden, k hidden);");
  if( rc!=SQLITE_OK ){
    return rc;
  }
  pVtab = sqlite3_malloc( sizeof(*pVtab) );
  if( pVtab==0 ){
    return SQLITE_NOMEM;
  }
  memset(pVtab, 0, sizeof(*pVtab));
  pVtab->db = db;
  *ppVtab = (sqlite3_vtab*)pVtab;
  return SQLITE_OK;
}

static int vectorVtabDisconnect(sqlite3_vtab *pVtab){
  sqlite3_free(pVtab);
  return SQLITE_OK;
}

static int vectorVtabOpen(sqlite3_vtab *p, sqlite3_vtab_cursor **ppCursor){
  vectorVtab *pVTab = (vectorVtab*)p;
  vectorVtab_cursor *pCur;
  pCur = sqlite3_malloc( sizeof(*pCur) );
  if( pCur==0 ) return SQLITE_NOMEM;
  memset(pCur, 0, sizeof(*pCur));
  *ppCursor = &pCur->base;
  return SQLITE_OK;
}

static void vectorVtabCursorClear(vectorVtab_cursor *pCur){
  if( pCur->pRowids ){
    sqlite3_free(pCur->pRowids);
    pCur->pRowids = 0;
    pCur->aRowids = 0;
    pCur->nRowidOffset = 0;
  }
}

static int vectorVtabClose(sqlite3_vtab_cursor *cur){
  vectorVtab_cursor *pCur = (vectorVtab_cursor*)cur;
  vectorVtabCursorClear(pCur);
  sqlite3_free(pCur);
  return SQLITE_OK;
}

static int vectorVtabNext(sqlite3_vtab_cursor *cur){
  vectorVtab_cursor *pCur = (vectorVtab_cursor*)cur;
  pCur->nRowidOffset++;
  return SQLITE_OK;
}

static int vectorVtabEof(sqlite3_vtab_cursor *cur){
  vectorVtab_cursor *pCur = (vectorVtab_cursor*)cur;
  return pCur->nRowidOffset>=pCur->aRowids;
}

static int vectorVtabColumn(
  sqlite3_vtab_cursor *cur,   /* The cursor */
  sqlite3_context *ctx,       /* First argument to sqlite3_result_...() */
  int i                       /* Which column to return */
){
  vectorVtab_cursor *pCur = (vectorVtab_cursor*)cur;
  switch( i ){
    case VECTOR_COLUMN_ID: {
      sqlite3_result_int64(ctx, pCur->pRowids[pCur->nRowidOffset]);
      break;
    }
  }
  return SQLITE_OK;
}

static int vectorVtabRowid(sqlite3_vtab_cursor *cur, sqlite_int64 *pRowid){
  vectorVtab_cursor *pCur = (vectorVtab_cursor*)cur;
  *pRowid = pCur->pRowids[pCur->nRowidOffset];
  return SQLITE_OK;
}

static int vectorVtabFilter(
  sqlite3_vtab_cursor *pVtabCursor, 
  int idxNum, const char *idxStr,
  int argc, sqlite3_value **argv
){
  vectorVtab_cursor *pCur = (vectorVtab_cursor *)pVtabCursor;
  vectorVtab *pVTab = (vectorVtab *)pVtabCursor->pVtab;
  const char *zIdxName;
  DiskAnnIndex *index;
  char *zErrMsg = 0;
  Vector *pVec;
  i64 *pRowids;
  sqlite3 *db;
  int nFound;
  int rc;
  int k;

  db = pVTab->db;  
  if( argc!=3 ){
    return SQLITE_ERROR;
  }
  zIdxName = (const char*)sqlite3_value_text(argv[0]);
  pVec = sqlite3_malloc(sizeof(Vector) + MAX_VECTOR_SZ*sizeof(float));
  if( pVec==0 ){
    return SQLITE_NOMEM;
  }
  vectorInit(pVec, VECTOR_TYPE_FLOAT32, MAX_VECTOR_SZ, ((char*)pVec) + sizeof(Vector));
  rc = vectorParse(argv[1], pVec, &zErrMsg);
  if( rc<0 ){
    goto error_free_vec;
  }
  k = sqlite3_value_int(argv[2]);
  if( k<0 ){
    rc = SQLITE_ERROR;
    zErrMsg = sqlite3_mprintf("K must be a positive integer");
    goto error_free_vec;
  }
  pRowids = sqlite3_malloc64(sizeof(u64)*k);
  if( pRowids==0 ){
    rc = SQLITE_NOMEM;
    goto error_free_vec;
  }
  rc = diskAnnOpenIndex(db, zIdxName, &index);
  if( rc!=SQLITE_OK ){
    zErrMsg = sqlite3_mprintf("Failed to open index: %s", sqlite3_errmsg(db));
    goto error_free_rowids;
  }
  nFound = diskAnnSearch(index, pVec, k, pRowids);
  assert( nFound>= 0 );
  diskAnnCloseIndex(index);
  pCur->pRowids = pRowids;
  pCur->aRowids = nFound;
  pCur->nRowidOffset = 0;
  sqlite3_free(pVec);
  return SQLITE_OK;

error_free_rowids:
  sqlite3_free(pRowids);
error_free_vec:
  pVTab->base.zErrMsg = zErrMsg;
  sqlite3_free(pVec);
  return rc;
}

static int vectorVtabBestIndex(
  sqlite3_vtab *tab,
  sqlite3_index_info *pIdxInfo
){
  const struct sqlite3_index_constraint *pConstraint;
  int indexNameIdx = -1;
  int vectorIdx = -1;
  int kIdx = -1;
  int i;

  pConstraint = pIdxInfo->aConstraint;
  for(i=0; i<pIdxInfo->nConstraint; i++, pConstraint++){
    if( pConstraint->usable==0 ) continue;
    if( pConstraint->op!=SQLITE_INDEX_CONSTRAINT_EQ ) continue;
    switch( pConstraint->iColumn ){
      case VECTOR_COLUMN_IDX:
        indexNameIdx = i;
        break;
      case VECTOR_COLUMN_VECTOR:
        vectorIdx = i;
        break;
      case VECTOR_COLUMN_K:
        kIdx = i;
        break;
    }
  }
  if( indexNameIdx >=0 ){
    pIdxInfo->aConstraintUsage[indexNameIdx].argvIndex = 1;
    pIdxInfo->aConstraintUsage[indexNameIdx].omit = 1;
    pIdxInfo->estimatedCost = (double)1;
    pIdxInfo->estimatedRows = 100;
    pIdxInfo->idxNum = 1;
    if( vectorIdx>=0 ){
      pIdxInfo->aConstraintUsage[vectorIdx].argvIndex = 2;
      pIdxInfo->aConstraintUsage[vectorIdx].omit = 1;
      pIdxInfo->idxNum = 2;
      if( kIdx>=0 ){
        pIdxInfo->aConstraintUsage[kIdx].argvIndex = 3;
        pIdxInfo->aConstraintUsage[kIdx].omit = 1;
        pIdxInfo->idxNum = 3;
      }
    }
  }else{
    pIdxInfo->estimatedCost = (double)2147483647;
    pIdxInfo->estimatedRows = 2147483647;
    pIdxInfo->idxNum = 0;
  }
  return SQLITE_OK;
}

static sqlite3_module vectorModule = {
  /* iVersion    */ 0,
  /* xCreate     */ 0,
  /* xConnect    */ vectorVtabConnect,
  /* xBestIndex  */ vectorVtabBestIndex,
  /* xDisconnect */ vectorVtabDisconnect,
  /* xDestroy    */ 0,
  /* xOpen       */ vectorVtabOpen,
  /* xClose      */ vectorVtabClose,
  /* xFilter     */ vectorVtabFilter,
  /* xNext       */ vectorVtabNext,
  /* xEof        */ vectorVtabEof,
  /* xColumn     */ vectorVtabColumn,
  /* xRowid      */ vectorVtabRowid,
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
  /* xShadowName */ 0,
  /* xIntegrity  */ 0
};

int vectorVtabInit(sqlite3 *db){
  return sqlite3_create_module(db, "vector_top_k", &vectorModule, 0);
}
#else
int vectorVtabInit(sqlite3 *db){ return SQLITE_OK; }
#endif /* !defined(SQLITE_OMIT_VECTOR) && !defined(SQLITE_OMIT_VIRTUALTABLE) */

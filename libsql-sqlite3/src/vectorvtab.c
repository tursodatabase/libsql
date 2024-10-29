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
#if !defined(SQLITE_OMIT_VECTOR) && !defined(SQLITE_OMIT_VIRTUALTABLE)
#include "sqlite3.h"
#include "vdbeInt.h"
#include "vectorIndexInt.h"

typedef struct vectorVtab vectorVtab;
struct vectorVtab {
  sqlite3_vtab base;       /* Base class - must be first */
  sqlite3 *db;             /* Database connection */
};

typedef struct vectorVtab_cursor vectorVtab_cursor;
struct vectorVtab_cursor {
  // first fields must copy fields from the sqlite3_vtab_cursor_tracked struct
  sqlite3_vtab_cursor base;  /* Base class - must be first */
  int nReads;                /* Number of row read from the storage backing virtual table */
  int nWrites;               /* Number of row written to the storage backing virtual table */
  VectorOutRows rows;
  int iRow;
};

/* Column numbers */
#define VECTOR_COLUMN_IDX    0
#define VECTOR_COLUMN_VECTOR 1
#define VECTOR_COLUMN_K      2
#define VECTOR_COLUMN_OFFSET 3

static int vectorVtabConnect(
  sqlite3 *db,
  void *pAux,
  int argc, const char *const *argv,
  sqlite3_vtab **ppVtab,
  char **pzErr
){
  vectorVtab *pVtab = NULL;
  int rc;
  /*
   * name of the database ignored by SQLite - so we don't need to provide any schema prefix here
   * hidden column are parameters of table-valued function (see https://www.sqlite.org/vtab.html#table_valued_functions)
  */
  rc = sqlite3_declare_vtab(db, "CREATE TABLE x(idx hidden, vector hidden, k hidden, id);");
  if( rc != SQLITE_OK ){
    return rc;
  }
  pVtab = sqlite3_malloc( sizeof(vectorVtab) );
  if( pVtab == NULL ){
    return SQLITE_NOMEM_BKPT;
  }
  // > Eponymous virtual tables exist in the "main" schema only, so they will not work if prefixed with a different schema name. 
  // so, argv[1] always equal to "main" and we can safely ignore it
  // (see https://www.sqlite.org/vtab.html#epovtab)
  memset(pVtab, 0, sizeof(*pVtab));
  pVtab->db = db;
  *ppVtab = (sqlite3_vtab*)pVtab;
  return SQLITE_OK;
}

static int vectorVtabDisconnect(sqlite3_vtab *pVtab){
  vectorVtab *pVTab = (vectorVtab*)pVtab;
  sqlite3_free(pVtab);
  return SQLITE_OK;
}

static int vectorVtabOpen(sqlite3_vtab *p, sqlite3_vtab_cursor **ppCursor){
  vectorVtab *pVTab = (vectorVtab*)p;
  vectorVtab_cursor *pCur;
  pCur = sqlite3_malloc( sizeof(vectorVtab_cursor) );
  if( pCur == NULL ){
    return SQLITE_NOMEM;
  }
  memset(pCur, 0, sizeof(*pCur));
  *ppCursor = &pCur->base;
  return SQLITE_OK;
}

static int vectorVtabClose(sqlite3_vtab_cursor *cur){
  vectorVtab_cursor *pCur = (vectorVtab_cursor*)cur;
  vectorVtab *pVTab = (vectorVtab *)cur->pVtab;
  vectorOutRowsFree(pVTab->db, &pCur->rows);
  sqlite3_free(pCur);
  return SQLITE_OK;
}

static int vectorVtabNext(sqlite3_vtab_cursor *cur){
  vectorVtab_cursor *pCur = (vectorVtab_cursor*)cur;
  pCur->iRow++;
  return SQLITE_OK;
}

static int vectorVtabEof(sqlite3_vtab_cursor *cur){
  vectorVtab_cursor *pCur = (vectorVtab_cursor*)cur;
  return pCur->iRow >= pCur->rows.nRows;
}

static int vectorVtabColumn(
  sqlite3_vtab_cursor *cur,   /* The cursor */
  sqlite3_context *context,   /* First argument to sqlite3_result_...() */
  int iCol                    /* Which column to return */
){
  vectorVtab_cursor *pCur = (vectorVtab_cursor*)cur;
  vectorOutRowsGet(context, &pCur->rows, pCur->iRow, iCol - VECTOR_COLUMN_OFFSET);
  return SQLITE_OK;
}

static int vectorVtabRowid(sqlite3_vtab_cursor *cur, sqlite_int64 *pRowid){
  vectorVtab_cursor *pCur = (vectorVtab_cursor*)cur;
  // rowid used for internal SQLite needs - so we take it if output has single integer column but in other case just return current row index
  if( pCur->rows.aIntValues != NULL ){
    *pRowid = pCur->rows.aIntValues[pCur->iRow];
  }else{
    *pRowid = pCur->iRow;
  }
  return SQLITE_OK;
}

static int vectorVtabFilter(
  sqlite3_vtab_cursor *pVtabCursor,
  int idxNum, const char *idxStr,
  int argc, sqlite3_value **argv
){
  vectorVtab_cursor *pCur = (vectorVtab_cursor *)pVtabCursor;
  vectorVtab *pVTab = (vectorVtab *)pVtabCursor->pVtab;
  pCur->rows.aIntValues = NULL;
  pCur->rows.ppValues = NULL;

  if( vectorIndexSearch(pVTab->db, argc, argv, &pCur->rows, &pCur->nReads, &pCur->nWrites, &pVTab->base.zErrMsg) != 0 ){
    return SQLITE_ERROR;
  }

  assert( pCur->rows.nRows >= 0 );
  assert( pCur->rows.nCols > 0 );
  pCur->iRow = 0;
  return SQLITE_OK;
}

static int vectorVtabBestIndex(
  sqlite3_vtab *tab,
  sqlite3_index_info *pIdxInfo
){
  const struct sqlite3_index_constraint *pConstraint;
  int i;

  pIdxInfo->estimatedCost = (double)1;
  pIdxInfo->estimatedRows = 100;
  pIdxInfo->idxNum = 1;

  pConstraint = pIdxInfo->aConstraint;
  for(i=0; i<pIdxInfo->nConstraint; i++, pConstraint++){
    if( pConstraint->usable == 0 ) continue;
    if( pConstraint->op != SQLITE_INDEX_CONSTRAINT_EQ ) continue;
    switch( pConstraint->iColumn ){
      case VECTOR_COLUMN_IDX:
        pIdxInfo->aConstraintUsage[i].argvIndex = 1;
        pIdxInfo->aConstraintUsage[i].omit = 1;
        break;
      case VECTOR_COLUMN_VECTOR:
        pIdxInfo->aConstraintUsage[i].argvIndex = 2;
        pIdxInfo->aConstraintUsage[i].omit = 1;
        break;
      case VECTOR_COLUMN_K:
        pIdxInfo->aConstraintUsage[i].argvIndex = 3;
        pIdxInfo->aConstraintUsage[i].omit = 1;
        break;
    }
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
  return sqlite3_create_module(db, VECTOR_INDEX_VTAB_NAME, &vectorModule, 0);
}
#endif /* !defined(SQLITE_OMIT_VECTOR) && !defined(SQLITE_OMIT_VIRTUALTABLE) */

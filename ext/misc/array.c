/*
** 2016-06-29
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
** This file demonstrates how to create a table-valued-function that
** returns the values in a C-language array.
** Examples:
**
**      SELECT * FROM intarray($ptr,5)
**
** The query above returns 5 integers contained in a C-language array
** at the address $ptr.  $ptr is a pointer to the array of integers that
** has been cast to an integer.
**
** HOW IT WORKS
**
** The intarray "function" is really a virtual table with the
** following schema:
**
**     CREATE FUNCTION intarray(
**       value,
**       pointer HIDDEN,
**       count HIDDEN
**     );
*/
#include "sqlite3ext.h"
SQLITE_EXTENSION_INIT1
#include <assert.h>
#include <string.h>

#ifndef SQLITE_OMIT_VIRTUALTABLE


/* intarray_cursor is a subclass of sqlite3_vtab_cursor which will
** serve as the underlying representation of a cursor that scans
** over rows of the result
*/
typedef struct intarray_cursor intarray_cursor;
struct intarray_cursor {
  sqlite3_vtab_cursor base;  /* Base class - must be first */
  int isDesc;                /* True to count down rather than up */
  sqlite3_int64 iRowid;      /* The rowid */
  sqlite3_int64 iPtr;        /* Pointer to array of integers */
  sqlite3_int64 iCnt;        /* Number of integers in the array */
};

/*
** The intarrayConnect() method is invoked to create a new
** intarray_vtab that describes the intarray virtual table.
**
** Think of this routine as the constructor for intarray_vtab objects.
**
** All this routine needs to do is:
**
**    (1) Allocate the intarray_vtab object and initialize all fields.
**
**    (2) Tell SQLite (via the sqlite3_declare_vtab() interface) what the
**        result set of queries against intarray will look like.
*/
static int intarrayConnect(
  sqlite3 *db,
  void *pAux,
  int argc, const char *const*argv,
  sqlite3_vtab **ppVtab,
  char **pzErr
){
  sqlite3_vtab *pNew;
  int rc;

/* Column numbers */
#define INTARRAY_COLUMN_VALUE   0
#define INTARRAY_COLUMN_POINTER 1
#define INTARRAY_COLUMN_COUNT   2

  rc = sqlite3_declare_vtab(db,
     "CREATE TABLE x(value,pointer hidden,count hidden)");
  if( rc==SQLITE_OK ){
    pNew = *ppVtab = sqlite3_malloc( sizeof(*pNew) );
    if( pNew==0 ) return SQLITE_NOMEM;
    memset(pNew, 0, sizeof(*pNew));
  }
  return rc;
}

/*
** This method is the destructor for intarray_cursor objects.
*/
static int intarrayDisconnect(sqlite3_vtab *pVtab){
  sqlite3_free(pVtab);
  return SQLITE_OK;
}

/*
** Constructor for a new intarray_cursor object.
*/
static int intarrayOpen(sqlite3_vtab *p, sqlite3_vtab_cursor **ppCursor){
  intarray_cursor *pCur;
  pCur = sqlite3_malloc( sizeof(*pCur) );
  if( pCur==0 ) return SQLITE_NOMEM;
  memset(pCur, 0, sizeof(*pCur));
  *ppCursor = &pCur->base;
  return SQLITE_OK;
}

/*
** Destructor for a intarray_cursor.
*/
static int intarrayClose(sqlite3_vtab_cursor *cur){
  sqlite3_free(cur);
  return SQLITE_OK;
}


/*
** Advance a intarray_cursor to its next row of output.
*/
static int intarrayNext(sqlite3_vtab_cursor *cur){
  intarray_cursor *pCur = (intarray_cursor*)cur;
  pCur->iRowid++;
  return SQLITE_OK;
}

/*
** Return values of columns for the row at which the intarray_cursor
** is currently pointing.
*/
static int intarrayColumn(
  sqlite3_vtab_cursor *cur,   /* The cursor */
  sqlite3_context *ctx,       /* First argument to sqlite3_result_...() */
  int i                       /* Which column to return */
){
  intarray_cursor *pCur = (intarray_cursor*)cur;
  sqlite3_int64 x = 0;
  switch( i ){
    case INTARRAY_COLUMN_POINTER:   x = pCur->iPtr;   break;
    case INTARRAY_COLUMN_COUNT:     x = pCur->iCnt;   break;
    default: {
      int *p = (int*)pCur->iPtr;
      x = (int)p[pCur->iRowid-1];
      break;
    }
  }
  sqlite3_result_int64(ctx, x);
  return SQLITE_OK;
}

/*
** Return the rowid for the current row.  In this implementation, the
** rowid is the same as the output value.
*/
static int intarrayRowid(sqlite3_vtab_cursor *cur, sqlite_int64 *pRowid){
  intarray_cursor *pCur = (intarray_cursor*)cur;
  *pRowid = pCur->iRowid;
  return SQLITE_OK;
}

/*
** Return TRUE if the cursor has been moved off of the last
** row of output.
*/
static int intarrayEof(sqlite3_vtab_cursor *cur){
  intarray_cursor *pCur = (intarray_cursor*)cur;
  return pCur->iRowid>=pCur->iCnt;
}

/*
** This method is called to "rewind" the intarray_cursor object back
** to the first row of output.
*/
static int intarrayFilter(
  sqlite3_vtab_cursor *pVtabCursor, 
  int idxNum, const char *idxStr,
  int argc, sqlite3_value **argv
){
  intarray_cursor *pCur = (intarray_cursor *)pVtabCursor;
  int i = 0;
  if( idxNum ){
    pCur->iPtr = sqlite3_value_int64(argv[0]);
    pCur->iCnt = sqlite3_value_int64(argv[1]);
  }else{
    pCur->iPtr = 0;
    pCur->iCnt = 0;
  }
  pCur->iRowid = 1;
  return SQLITE_OK;
}

/*
** SQLite will invoke this method one or more times while planning a query
** that uses the intarray virtual table.  This routine needs to create
** a query plan for each invocation and compute an estimated cost for that
** plan.
**
** In this implementation idxNum is used to represent the
** query plan.  idxStr is unused.
**
** idxNum is 1 if the pointer= and count= constraints exist and is 0 otherwise.
** If idxNum is 0, then intarray becomes an empty table.
*/
static int intarrayBestIndex(
  sqlite3_vtab *tab,
  sqlite3_index_info *pIdxInfo
){
  int i;                 /* Loop over constraints */
  int idxNum = 0;        /* The query plan bitmask */
  int ptrIdx = -1;       /* Index of the pointer= constraint, or -1 if none */
  int cntIdx = -1;       /* Index of the count= constraint, or -1 if none */
  int nArg = 0;          /* Number of arguments that intarrayFilter() expects */

  const struct sqlite3_index_constraint *pConstraint;
  pConstraint = pIdxInfo->aConstraint;
  for(i=0; i<pIdxInfo->nConstraint; i++, pConstraint++){
    if( pConstraint->usable==0 ) continue;
    if( pConstraint->op!=SQLITE_INDEX_CONSTRAINT_EQ ) continue;
    switch( pConstraint->iColumn ){
      case INTARRAY_COLUMN_POINTER:
        ptrIdx = i;
        break;
      case INTARRAY_COLUMN_COUNT:
        cntIdx = i;
        break;
    }
  }
  if( ptrIdx>=0 && cntIdx>=0 ){
    pIdxInfo->aConstraintUsage[ptrIdx].argvIndex = 1;
    pIdxInfo->aConstraintUsage[ptrIdx].omit = 1;
    pIdxInfo->aConstraintUsage[cntIdx].argvIndex = 2;
    pIdxInfo->aConstraintUsage[cntIdx].omit = 1;
    pIdxInfo->estimatedCost = (double)1;
    pIdxInfo->estimatedRows = (double)100;
    pIdxInfo->idxNum = 1;
  }else{
    pIdxInfo->estimatedCost = (double)2147483647;
    pIdxInfo->estimatedRows = (double)2147483647;
    pIdxInfo->idxNum = 0;
  }
  return SQLITE_OK;
}

/*
** This following structure defines all the methods for the 
** intarray virtual table.
*/
static sqlite3_module intarrayModule = {
  0,                         /* iVersion */
  0,                         /* xCreate */
  intarrayConnect,           /* xConnect */
  intarrayBestIndex,         /* xBestIndex */
  intarrayDisconnect,        /* xDisconnect */
  0,                         /* xDestroy */
  intarrayOpen,              /* xOpen - open a cursor */
  intarrayClose,             /* xClose - close a cursor */
  intarrayFilter,            /* xFilter - configure scan constraints */
  intarrayNext,              /* xNext - advance a cursor */
  intarrayEof,               /* xEof - check for end of scan */
  intarrayColumn,            /* xColumn - read data */
  intarrayRowid,             /* xRowid - read data */
  0,                         /* xUpdate */
  0,                         /* xBegin */
  0,                         /* xSync */
  0,                         /* xCommit */
  0,                         /* xRollback */
  0,                         /* xFindMethod */
  0,                         /* xRename */
};

#endif /* SQLITE_OMIT_VIRTUALTABLE */

#ifdef _WIN32
__declspec(dllexport)
#endif
int sqlite3_array_init(
  sqlite3 *db, 
  char **pzErrMsg, 
  const sqlite3_api_routines *pApi
){
  int rc = SQLITE_OK;
  SQLITE_EXTENSION_INIT2(pApi);
#ifndef SQLITE_OMIT_VIRTUALTABLE
  if( sqlite3_libversion_number()<3008012 ){
    *pzErrMsg = sqlite3_mprintf(
        "intarray() requires SQLite 3.8.12 or later");
    return SQLITE_ERROR;
  }
  rc = sqlite3_create_module(db, "intarray", &intarrayModule, 0);
#endif
  return rc;
}

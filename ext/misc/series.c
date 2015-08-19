/*
** 2015-08-18
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
** This file implements a virtual table that tries to replicate the 
** behavior of the generate_series() table-valued-function in Postgres.
**
** Example:
**
**     SELECT * FROM generate_series WHERE start=1 AND stop=9 AND step=2
**
** Results in:
**
**     1 3 5 7 9
**
*/
#include "sqlite3ext.h"
SQLITE_EXTENSION_INIT1
#include <assert.h>
#include <string.h>

#ifndef SQLITE_OMIT_VIRTUALTABLE


/* A series cursor object */
typedef struct series_cursor series_cursor;
struct series_cursor {
  sqlite3_vtab_cursor base;  /* Base class - must be first */
  sqlite3_int64 iValue;      /* Current value */
  sqlite3_int64 mnValue;     /* Mimimum value */
  sqlite3_int64 mxValue;     /* Maximum value */
  sqlite3_int64 iStep;       /* How much to increment on each step */
};

/* Methods for the series module */
static int seriesConnect(
  sqlite3 *db,
  void *pAux,
  int argc, const char *const*argv,
  sqlite3_vtab **ppVtab,
  char **pzErr
){
  sqlite3_vtab *pNew;
  pNew = *ppVtab = sqlite3_malloc( sizeof(*pNew) );
  if( pNew==0 ) return SQLITE_NOMEM;

#define SERIES_COLUMN_VALUE 0
#define SERIES_COLUMN_START 1
#define SERIES_COLUMN_STOP  2
#define SERIES_COLUMN_STEP  3

  sqlite3_declare_vtab(db,
     "CREATE TABLE x(value,start hidden,stop hidden,step hidden)");
  memset(pNew, 0, sizeof(*pNew));
  return SQLITE_OK;
}

static int seriesDisconnect(sqlite3_vtab *pVtab){
  sqlite3_free(pVtab);
  return SQLITE_OK;
}

/*
** Open a new series cursor.
*/
static int seriesOpen(sqlite3_vtab *p, sqlite3_vtab_cursor **ppCursor){
  series_cursor *pCur;
  pCur = sqlite3_malloc( sizeof(*pCur) );
  if( pCur==0 ) return SQLITE_NOMEM;
  memset(pCur, 0, sizeof(*pCur));
  *ppCursor = &pCur->base;
  return SQLITE_OK;
}

/*
** Close a series cursor.
*/
static int seriesClose(sqlite3_vtab_cursor *cur){
  sqlite3_free(cur);
  return SQLITE_OK;
}


/*
** Advance a cursor to its next row of output
*/
static int seriesNext(sqlite3_vtab_cursor *cur){
  series_cursor *pCur = (series_cursor*)cur;
  pCur->iValue += pCur->iStep;
  return SQLITE_OK;
}

/*
** Return the value associated with a series.
*/
static int seriesColumn(
  sqlite3_vtab_cursor *cur,
  sqlite3_context *ctx,
  int i
){
  series_cursor *pCur = (series_cursor*)cur;
  sqlite3_int64 x;
  switch( i ){
    case 0:  x = pCur->iValue;  break;
    case 1:  x = pCur->mnValue; break;
    case 2:  x = pCur->mxValue; break;
    case 3:  x = pCur->iStep;   break;
  }
  sqlite3_result_int64(ctx, x);
  return SQLITE_OK;
}

/*
** The rowid.
*/
static int seriesRowid(sqlite3_vtab_cursor *cur, sqlite_int64 *pRowid){
  series_cursor *pCur = (series_cursor*)cur;
  *pRowid = pCur->iValue;
  return SQLITE_OK;
}

/*
** Return TRUE if the last row has been output.
*/
static int seriesEof(sqlite3_vtab_cursor *cur){
  series_cursor *pCur = (series_cursor*)cur;
  return pCur->iValue>pCur->mxValue;
}

/*
** Called to "rewind" a cursor back to the beginning so that
** it starts its output over again.  Always called at least once
** prior to any seriesColumn, seriesRowid, or seriesEof call.
**
** idxNum is a bitmask showing which constraints are available:
**
**    1:    start=VALUE
**    2:    stop=VALUE
**    4:    step=VALUE
**
*/
static int seriesFilter(
  sqlite3_vtab_cursor *pVtabCursor, 
  int idxNum, const char *idxStr,
  int argc, sqlite3_value **argv
){
  series_cursor *pCur = (series_cursor *)pVtabCursor;
  int i = 0;
  if( idxNum & 1 ){
    pCur->mnValue = sqlite3_value_int64(argv[i++]);
  }else{
    pCur->mnValue = 0;
  }
  pCur->iValue = pCur->mnValue;
  if( idxNum & 2 ){
    pCur->mxValue = sqlite3_value_int64(argv[i++]);
  }else{
    pCur->mxValue = 0xffffffff;
  }
  if( idxNum & 4 ){
    pCur->iStep = sqlite3_value_int64(argv[i++]);
  }else{
    pCur->iStep = 1;
  }
  return SQLITE_OK;
}

/*
** Search for terms of these forms:
**
**  (1)  start = $value
**  (2)  stop = $value
**  (4)  step = $value
**
** idxNum is an ORed combination of 1, 2,  4.
*/
static int seriesBestIndex(
  sqlite3_vtab *tab,
  sqlite3_index_info *pIdxInfo
){
  int i;
  int idxNum = 0;
  int startIdx = -1;
  int stopIdx = -1;
  int stepIdx = -1;
  int nArg = 0;

  const struct sqlite3_index_constraint *pConstraint;
  pConstraint = pIdxInfo->aConstraint;
  for(i=0; i<pIdxInfo->nConstraint; i++, pConstraint++){
    if( pConstraint->usable==0 ) continue;
    if( pConstraint->op!=SQLITE_INDEX_CONSTRAINT_EQ ) continue;
    switch( pConstraint->iColumn ){
      case SERIES_COLUMN_START:
        startIdx = i;
        idxNum |= 1;
        break;
      case SERIES_COLUMN_STOP:
        stopIdx = i;
        idxNum |= 2;
        break;
      case SERIES_COLUMN_STEP:
        stepIdx = i;
        idxNum |= 4;
        break;
    }
  }
  pIdxInfo->idxNum = idxNum;
  if( startIdx>=0 ){
    pIdxInfo->aConstraintUsage[startIdx].argvIndex = ++nArg;
    pIdxInfo->aConstraintUsage[startIdx].omit = 1;
  }
  if( stopIdx>=0 ){
    pIdxInfo->aConstraintUsage[stopIdx].argvIndex = ++nArg;
    pIdxInfo->aConstraintUsage[stopIdx].omit = 1;
  }
  if( stepIdx>=0 ){
    pIdxInfo->aConstraintUsage[stepIdx].argvIndex = ++nArg;
    pIdxInfo->aConstraintUsage[stepIdx].omit = 1;
  }
  if( pIdxInfo->nOrderBy==1
   && pIdxInfo->aOrderBy[0].desc==0
  ){
    pIdxInfo->orderByConsumed = 1;
  }
  if( (idxNum & 3)==3 ){
    /* Both start= and stop= boundaries are available.  This is the 
    ** the preferred case */
    pIdxInfo->estimatedCost = (double)1;
  }else{
    /* If either boundary is missing, we have to generate a huge span
    ** of numbers.  Make this case very expensive so that the query
    ** planner will work hard to avoid it. */
    pIdxInfo->estimatedCost = (double)2000000000;
  }
  return SQLITE_OK;
}

/*
** A virtual table module that provides read-only access to a
** Tcl global variable namespace.
*/
static sqlite3_module seriesModule = {
  0,                         /* iVersion */
  0,                         /* xCreate */
  seriesConnect,
  seriesBestIndex,
  seriesDisconnect, 
  0,                         /* xDestroy */
  seriesOpen,                /* xOpen - open a cursor */
  seriesClose,               /* xClose - close a cursor */
  seriesFilter,              /* xFilter - configure scan constraints */
  seriesNext,                /* xNext - advance a cursor */
  seriesEof,                 /* xEof - check for end of scan */
  seriesColumn,              /* xColumn - read data */
  seriesRowid,               /* xRowid - read data */
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
int sqlite3_series_init(
  sqlite3 *db, 
  char **pzErrMsg, 
  const sqlite3_api_routines *pApi
){
  int rc = SQLITE_OK;
  SQLITE_EXTENSION_INIT2(pApi);
#ifndef SQLITE_OMIT_VIRTUALTABLE
  rc = sqlite3_create_module(db, "generate_series", &seriesModule, 0);
#endif
  return rc;
}

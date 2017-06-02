/*
** 2017-05-31
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
** This file demonstrates an eponymous virtual table that returns information
** about all prepared statements for the database connection.
**
** Usage example:
**
**     .load ./stmts
**     .mode line
**     .header on
**     SELECT * FROM stmts;
*/
#include "sqlite3ext.h"
SQLITE_EXTENSION_INIT1
#include <assert.h>
#include <string.h>

#ifndef SQLITE_OMIT_VIRTUALTABLE

/*
** The following macros are used to cast pointers to integers.
** The way you do this varies from one compiler
** to the next, so we have developed the following set of #if statements
** to generate appropriate macros for a wide range of compilers.
*/
#if defined(__PTRDIFF_TYPE__)  /* This case should work for GCC */
# define SQLITE_PTR_TO_INT64(X)  ((sqlite3_int64)(__PTRDIFF_TYPE__)(X))
#elif !defined(__GNUC__)       /* Works for compilers other than LLVM */
# define SQLITE_PTR_TO_INT64(X)  ((sqlite3_int64)(((char*)X)-(char*)0))
#elif defined(HAVE_STDINT_H)   /* Use this case if we have ANSI headers */
# define SQLITE_PTR_TO_INT64(X)  ((sqlite3_int64)(intptr_t)(X))
#else                          /* Generates a warning - but it always works */
# define SQLITE_PTR_TO_INT64(X)  ((sqlite3_int64)(X))
#endif


/* stmts_vtab is a subclass of sqlite3_vtab which will
** serve as the underlying representation of a stmts virtual table
*/
typedef struct stmts_vtab stmts_vtab;
struct stmts_vtab {
  sqlite3_vtab base;  /* Base class - must be first */
  sqlite3 *db;        /* Database connection for this stmts vtab */
};

/* stmts_cursor is a subclass of sqlite3_vtab_cursor which will
** serve as the underlying representation of a cursor that scans
** over rows of the result
*/
typedef struct stmts_cursor stmts_cursor;
struct stmts_cursor {
  sqlite3_vtab_cursor base;  /* Base class - must be first */
  sqlite3 *db;               /* Database connection for this cursor */
  sqlite3_stmt *pStmt;       /* Statement cursor is currently pointing at */
  sqlite3_int64 iRowid;      /* The rowid */
};

/*
** The stmtsConnect() method is invoked to create a new
** stmts_vtab that describes the generate_stmts virtual table.
**
** Think of this routine as the constructor for stmts_vtab objects.
**
** All this routine needs to do is:
**
**    (1) Allocate the stmts_vtab object and initialize all fields.
**
**    (2) Tell SQLite (via the sqlite3_declare_vtab() interface) what the
**        result set of queries against generate_stmts will look like.
*/
static int stmtsConnect(
  sqlite3 *db,
  void *pAux,
  int argc, const char *const*argv,
  sqlite3_vtab **ppVtab,
  char **pzErr
){
  stmts_vtab *pNew;
  int rc;

/* Column numbers */
#define STMTS_COLUMN_PTR   0    /* Numeric value of the statement pointer */
#define STMTS_COLUMN_SQL   1    /* SQL for the statement */
#define STMTS_COLUMN_NCOL  2    /* Number of result columns */
#define STMTS_COLUMN_RO    3    /* True if read-only */
#define STMTS_COLUMN_BUSY  4    /* True if currently busy */
#define STMTS_COLUMN_NSCAN 5    /* SQLITE_STMTSTATUS_FULLSCAN_STEP */
#define STMTS_COLUMN_NSORT 6    /* SQLITE_STMTSTATUS_SORT */
#define STMTS_COLUMN_NAIDX 7    /* SQLITE_STMTSTATUS_AUTOINDEX */
#define STMTS_COLUMN_NSTEP 8    /* SQLITE_STMTSTATUS_VM_STEP */
#define STMTS_COLUMN_MEM   9    /* SQLITE_STMTSTATUS_MEMUSED */


  rc = sqlite3_declare_vtab(db,
     "CREATE TABLE x(ptr,sql,ncol,ro,busy,nscan,nsort,naidx,nstep,mem)");
  if( rc==SQLITE_OK ){
    pNew = sqlite3_malloc( sizeof(*pNew) );
    *ppVtab = (sqlite3_vtab*)pNew;
    if( pNew==0 ) return SQLITE_NOMEM;
    memset(pNew, 0, sizeof(*pNew));
    pNew->db = db;
  }
  return rc;
}

/*
** This method is the destructor for stmts_cursor objects.
*/
static int stmtsDisconnect(sqlite3_vtab *pVtab){
  sqlite3_free(pVtab);
  return SQLITE_OK;
}

/*
** Constructor for a new stmts_cursor object.
*/
static int stmtsOpen(sqlite3_vtab *p, sqlite3_vtab_cursor **ppCursor){
  stmts_cursor *pCur;
  pCur = sqlite3_malloc( sizeof(*pCur) );
  if( pCur==0 ) return SQLITE_NOMEM;
  memset(pCur, 0, sizeof(*pCur));
  pCur->db = ((stmts_vtab*)p)->db;
  *ppCursor = &pCur->base;
  return SQLITE_OK;
}

/*
** Destructor for a stmts_cursor.
*/
static int stmtsClose(sqlite3_vtab_cursor *cur){
  sqlite3_free(cur);
  return SQLITE_OK;
}


/*
** Advance a stmts_cursor to its next row of output.
*/
static int stmtsNext(sqlite3_vtab_cursor *cur){
  stmts_cursor *pCur = (stmts_cursor*)cur;
  pCur->iRowid++;
  pCur->pStmt = sqlite3_next_stmt(pCur->db, pCur->pStmt);
  return SQLITE_OK;
}

/*
** Return values of columns for the row at which the stmts_cursor
** is currently pointing.
*/
static int stmtsColumn(
  sqlite3_vtab_cursor *cur,   /* The cursor */
  sqlite3_context *ctx,       /* First argument to sqlite3_result_...() */
  int i                       /* Which column to return */
){
  stmts_cursor *pCur = (stmts_cursor*)cur;
  switch( i ){
    case STMTS_COLUMN_PTR: {
      sqlite3_result_int64(ctx, SQLITE_PTR_TO_INT64(pCur->pStmt));
      break;
    }
    case STMTS_COLUMN_SQL: {
      sqlite3_result_text(ctx, sqlite3_sql(pCur->pStmt), -1, SQLITE_TRANSIENT);
      break;
    }
    case STMTS_COLUMN_NCOL: {
      sqlite3_result_int(ctx, sqlite3_column_count(pCur->pStmt));
      break;
    }
    case STMTS_COLUMN_RO: {
      sqlite3_result_int(ctx, sqlite3_stmt_readonly(pCur->pStmt));
      break;
    }
    case STMTS_COLUMN_BUSY: {
      sqlite3_result_int(ctx, sqlite3_stmt_busy(pCur->pStmt));
      break;
    }
    case STMTS_COLUMN_NSCAN:
    case STMTS_COLUMN_NSORT:
    case STMTS_COLUMN_NAIDX:
    case STMTS_COLUMN_NSTEP:
    case STMTS_COLUMN_MEM: {
      sqlite3_result_int(ctx, sqlite3_stmt_status(pCur->pStmt,
                      i-STMTS_COLUMN_NSCAN+SQLITE_STMTSTATUS_FULLSCAN_STEP, 0));
      break;
    }
  }
  return SQLITE_OK;
}

/*
** Return the rowid for the current row.  In this implementation, the
** rowid is the same as the output value.
*/
static int stmtsRowid(sqlite3_vtab_cursor *cur, sqlite_int64 *pRowid){
  stmts_cursor *pCur = (stmts_cursor*)cur;
  *pRowid = pCur->iRowid;
  return SQLITE_OK;
}

/*
** Return TRUE if the cursor has been moved off of the last
** row of output.
*/
static int stmtsEof(sqlite3_vtab_cursor *cur){
  stmts_cursor *pCur = (stmts_cursor*)cur;
  return pCur->pStmt==0;
}

/*
** This method is called to "rewind" the stmts_cursor object back
** to the first row of output.  This method is always called at least
** once prior to any call to stmtsColumn() or stmtsRowid() or 
** stmtsEof().
*/
static int stmtsFilter(
  sqlite3_vtab_cursor *pVtabCursor, 
  int idxNum, const char *idxStr,
  int argc, sqlite3_value **argv
){
  stmts_cursor *pCur = (stmts_cursor *)pVtabCursor;
  pCur->pStmt = 0;
  pCur->iRowid = 0;
  return stmtsNext(pVtabCursor);
}

/*
** SQLite will invoke this method one or more times while planning a query
** that uses the generate_stmts virtual table.  This routine needs to create
** a query plan for each invocation and compute an estimated cost for that
** plan.
*/
static int stmtsBestIndex(
  sqlite3_vtab *tab,
  sqlite3_index_info *pIdxInfo
){
  pIdxInfo->estimatedCost = (double)500;
  pIdxInfo->estimatedRows = 500;
  return SQLITE_OK;
}

/*
** This following structure defines all the methods for the 
** generate_stmts virtual table.
*/
static sqlite3_module stmtsModule = {
  0,                         /* iVersion */
  0,                         /* xCreate */
  stmtsConnect,             /* xConnect */
  stmtsBestIndex,           /* xBestIndex */
  stmtsDisconnect,          /* xDisconnect */
  0,                         /* xDestroy */
  stmtsOpen,                /* xOpen - open a cursor */
  stmtsClose,               /* xClose - close a cursor */
  stmtsFilter,              /* xFilter - configure scan constraints */
  stmtsNext,                /* xNext - advance a cursor */
  stmtsEof,                 /* xEof - check for end of scan */
  stmtsColumn,              /* xColumn - read data */
  stmtsRowid,               /* xRowid - read data */
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
int sqlite3_stmts_init(
  sqlite3 *db, 
  char **pzErrMsg, 
  const sqlite3_api_routines *pApi
){
  int rc = SQLITE_OK;
  SQLITE_EXTENSION_INIT2(pApi);
#ifndef SQLITE_OMIT_VIRTUALTABLE
  if( sqlite3_libversion_number()<3008012 ){
    *pzErrMsg = sqlite3_mprintf(
        "generate_stmts() requires SQLite 3.8.12 or later");
    return SQLITE_ERROR;
  }
  rc = sqlite3_create_module(db, "stmts", &stmtsModule, 0);
#endif
  return rc;
}

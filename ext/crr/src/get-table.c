// Copy of `sqlite3_get_table` given some sqlite builds
// omit this function causing our extension to crash.
// We only need `get_table` in two places --
// could replace with something more streamlined in the future.

#include "get-table.h"

#include <assert.h>
#include <string.h>
typedef unsigned int u32;

#if defined(HAVE_STDINT_H) /* Use this case if we have ANSI headers */
#define SQLITE_INT_TO_PTR(X) ((void *)(intptr_t)(X))
#define SQLITE_PTR_TO_INT(X) ((int)(intptr_t)(X))
#elif defined(__PTRDIFF_TYPE__) /* This case should work for GCC */
#define SQLITE_INT_TO_PTR(X) ((void *)(__PTRDIFF_TYPE__)(X))
#define SQLITE_PTR_TO_INT(X) ((int)(__PTRDIFF_TYPE__)(X))
#elif !defined(__GNUC__) /* Works for compilers other than LLVM */
#define SQLITE_INT_TO_PTR(X) ((void *)&((char *)0)[X])
#define SQLITE_PTR_TO_INT(X) ((int)(((char *)X) - (char *)0))
#else /* Generates a warning - but it always works */
#define SQLITE_INT_TO_PTR(X) ((void *)(X))
#define SQLITE_PTR_TO_INT(X) ((int)(X))
#endif

/*
** Compute a string length that is limited to what can be stored in
** lower 30 bits of a 32-bit signed integer.
**
** The value returned will never be negative.  Nor will it ever be greater
** than the actual length of the string.  For very long strings (greater
** than 1GiB) the value returned might be less than the true string length.
*/
static int sqlite3Strlen30(const char *z) {
  if (z == 0) return 0;
  return 0x3fffffff & (int)strlen(z);
}

/*
** This structure is used to pass data from sqlite3_get_table() through
** to the callback function is uses to build the result.
*/
typedef struct TabResult {
  char **azResult; /* Accumulated output */
  char *zErrMsg;   /* Error message text, if an error occurs */
  u32 nAlloc;      /* Slots allocated for azResult[] */
  u32 nRow;        /* Number of rows in the result */
  u32 nColumn;     /* Number of columns in the result */
  u32 nData;       /* Slots used in azResult[].  (nRow+1)*nColumn */
  int rc;          /* Return code from sqlite3_exec() */
} TabResult;

/*
** This routine is called once for each row in the result table.  Its job
** is to fill in the TabResult structure appropriately, allocating new
** memory as necessary.
*/
static int crsql_get_table_cb(void *pArg, int nCol, char **argv, char **colv) {
  TabResult *p = (TabResult *)pArg; /* Result accumulator */
  int need;                         /* Slots needed in p->azResult[] */
  int i;                            /* Loop counter */
  char *z;                          /* A single column of result */

  /* Make sure there is enough space in p->azResult to hold everything
  ** we need to remember from this invocation of the callback.
  */
  if (p->nRow == 0 && argv != 0) {
    need = nCol * 2;
  } else {
    need = nCol;
  }
  if (p->nData + need > p->nAlloc) {
    char **azNew;
    p->nAlloc = p->nAlloc * 2 + need;
    azNew = sqlite3_realloc(p->azResult, sizeof(char *) * p->nAlloc);
    if (azNew == 0) goto malloc_failed;
    p->azResult = azNew;
  }

  /* If this is the first row, then generate an extra row containing
  ** the names of all columns.
  */
  if (p->nRow == 0) {
    p->nColumn = nCol;
    for (i = 0; i < nCol; i++) {
      z = sqlite3_mprintf("%s", colv[i]);
      if (z == 0) goto malloc_failed;
      p->azResult[p->nData++] = z;
    }
  } else if ((int)p->nColumn != nCol) {
    sqlite3_free(p->zErrMsg);
    p->zErrMsg = sqlite3_mprintf(
        "sqlite3_get_table() called with two or more incompatible queries");
    p->rc = SQLITE_ERROR;
    return 1;
  }

  /* Copy over the row data
   */
  if (argv != 0) {
    for (i = 0; i < nCol; i++) {
      if (argv[i] == 0) {
        z = 0;
      } else {
        int n = sqlite3Strlen30(argv[i]) + 1;
        z = sqlite3_malloc64(n);
        if (z == 0) goto malloc_failed;
        memcpy(z, argv[i], n);
      }
      p->azResult[p->nData++] = z;
    }
    p->nRow++;
  }
  return 0;

malloc_failed:
  p->rc = SQLITE_NOMEM;
  return 1;
}

/*
** Query the database.  But instead of invoking a callback for each row,
** malloc() for space to hold the result and return the entire results
** at the conclusion of the call.
**
** The result that is written to ***pazResult is held in memory obtained
** from malloc().  But the caller cannot free this memory directly.
** Instead, the entire table should be passed to crsql_free_table() when
** the calling procedure is finished using it.
*/
int crsql_get_table(
    sqlite3 *db,       /* The database on which the SQL executes */
    const char *zSql,  /* The SQL to be executed */
    char ***pazResult, /* Write the result table here */
    int *pnRow,        /* Write the number of rows in the result here */
    int *pnColumn,     /* Write the number of columns of result here */
    char **pzErrMsg    /* Write error messages here */
) {
  int rc;
  TabResult res;

  *pazResult = 0;
  if (pnColumn) *pnColumn = 0;
  if (pnRow) *pnRow = 0;
  if (pzErrMsg) *pzErrMsg = 0;
  res.zErrMsg = 0;
  res.nRow = 0;
  res.nColumn = 0;
  res.nData = 1;
  res.nAlloc = 20;
  res.rc = SQLITE_OK;
  res.azResult = sqlite3_malloc64(sizeof(char *) * res.nAlloc);
  if (res.azResult == 0) {
    return SQLITE_NOMEM;
  }
  res.azResult[0] = 0;
  rc = sqlite3_exec(db, zSql, crsql_get_table_cb, &res, pzErrMsg);
  assert(sizeof(res.azResult[0]) >= sizeof(res.nData));
  res.azResult[0] = SQLITE_INT_TO_PTR(res.nData);
  if ((rc & 0xff) == SQLITE_ABORT) {
    crsql_free_table(&res.azResult[1]);
    if (res.zErrMsg) {
      if (pzErrMsg) {
        sqlite3_free(*pzErrMsg);
        *pzErrMsg = sqlite3_mprintf("%s", res.zErrMsg);
      }
      sqlite3_free(res.zErrMsg);
    }
    return res.rc;
  }
  sqlite3_free(res.zErrMsg);
  if (rc != SQLITE_OK) {
    crsql_free_table(&res.azResult[1]);
    return rc;
  }
  if (res.nAlloc > res.nData) {
    char **azNew;
    azNew = sqlite3_realloc(res.azResult, sizeof(char *) * res.nData);
    if (azNew == 0) {
      crsql_free_table(&res.azResult[1]);
      return SQLITE_NOMEM;
    }
    res.azResult = azNew;
  }
  *pazResult = &res.azResult[1];
  if (pnColumn) *pnColumn = res.nColumn;
  if (pnRow) *pnRow = res.nRow;
  return rc;
}

/*
** This routine frees the space the sqlite3_get_table() malloced.
*/
void crsql_free_table(
    char **azResult /* Result returned from sqlite3_get_table() */
) {
  if (azResult) {
    int i, n;
    azResult--;
    assert(azResult != 0);
    n = SQLITE_PTR_TO_INT(azResult[0]);
    for (i = 1; i < n; i++) {
      if (azResult[i]) sqlite3_free(azResult[i]);
    }
    sqlite3_free(azResult);
  }
}

/*
** 2014 May 31
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
******************************************************************************
**
** Interfaces to extend FTS5. Using the interfaces defined in this file, 
** FTS5 may be extended with:
**
**     * custom tokenizers, and
**     * custom auxiliary functions.
*/


#ifndef _FTS5_H
#define _FTS5_H

#include "sqlite3.h"

/*************************************************************************
** CUSTOM AUXILIARY FUNCTIONS
**
** Virtual table implemenations may overload SQL functions by implementing
** the sqlite3_module.xFindFunction() method.
*/

typedef struct Fts5ExtensionApi Fts5ExtensionApi;
typedef struct Fts5Context Fts5Context;

typedef void (*fts5_extension_function)(
  const Fts5ExtensionApi *pApi,   /* API offered by current FTS version */
  Fts5Context *pFts,              /* First arg to pass to pApi functions */
  sqlite3_context *pCtx,          /* Context for returning result/error */
  int nVal,                       /* Number of values in apVal[] array */
  sqlite3_value **apVal           /* Array of trailing arguments */
);

/*
**
** xUserData:
**   Return a copy of the context pointer the extension function was 
**   registered with.
**
** xColumnCount:
**   Returns the number of columns in the FTS5 table.
**
** xColumnSize:
**   Reports the size in tokens of a column value from the current row.
**
** xPhraseCount:
**   Returns the number of phrases in the current query expression.
**
** xPhraseSize:
**   Returns the number of tokens in phrase iPhrase of the query. Phrases
**   are numbered starting from zero.
**
** xRowid:
**   Returns the rowid of the current row.
**
** xPoslist:
**   Iterate through instances of phrase iPhrase in the current row. 
*/
struct Fts5ExtensionApi {
  int iVersion;                   /* Currently always set to 1 */

  void *(*xUserData)(Fts5Context*);

  int (*xColumnCount)(Fts5Context*);
  int (*xColumnAvgSize)(Fts5Context*, int iCol, int *pnToken);
  int (*xTokenize)(Fts5Context*, 
    const char *pText, int nText, /* Text to tokenize */
    void *pCtx,                   /* Context passed to xToken() */
    int (*xToken)(void*, const char*, int, int, int, int)    /* Callback */
  );

  int (*xPhraseCount)(Fts5Context*);
  int (*xPhraseSize)(Fts5Context*, int iPhrase);

  sqlite3_int64 (*xRowid)(Fts5Context*);
  int (*xColumnText)(Fts5Context*, int iCol, const char **pz, int *pn);
  int (*xColumnSize)(Fts5Context*, int iCol, int *pnToken);
  int (*xPoslist)(Fts5Context*, int iPhrase, int *pi, int *piCol, int *piOff);
};

/* 
** CUSTOM AUXILIARY FUNCTIONS
*************************************************************************/
#endif /* _FTS5_H */


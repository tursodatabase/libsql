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
*/

#include "fts5Int.h"

static void fts5SnippetFunction(
  const Fts5ExtensionApi *pApi,   /* API offered by current FTS version */
  Fts5Context *pFts,              /* First arg to pass to pApi functions */
  sqlite3_context *pCtx,          /* Context for returning result/error */
  int nVal,                       /* Number of values in apVal[] array */
  sqlite3_value **apVal           /* Array of trailing arguments */
){
  assert( 0 );
}

static void fts5TestFunction(
  const Fts5ExtensionApi *pApi,   /* API offered by current FTS version */
  Fts5Context *pFts,              /* First arg to pass to pApi functions */
  sqlite3_context *pCtx,          /* Context for returning result/error */
  int nVal,                       /* Number of values in apVal[] array */
  sqlite3_value **apVal           /* Array of trailing arguments */
){
  Fts5Buffer s;                   /* Build up text to return here */
  int nCol;                       /* Number of columns in table */
  int nPhrase;                    /* Number of phrases in query */
  i64 iRowid;                     /* Rowid of current row */
  const char *zReq = 0;
  int rc = SQLITE_OK;
  int i;

  if( nVal>=1 ){
    zReq = (const char*)sqlite3_value_text(apVal[0]);
  }

  memset(&s, 0, sizeof(Fts5Buffer));

  if( zReq==0 ){
    sqlite3Fts5BufferAppendPrintf(&rc, &s, "columncount ");
  }
  if( 0==zReq || 0==sqlite3_stricmp(zReq, "columncount") ){
    nCol = pApi->xColumnCount(pFts);
    sqlite3Fts5BufferAppendPrintf(&rc, &s, "%d", nCol);
  }

  if( zReq==0 ){
    sqlite3Fts5BufferAppendPrintf(&rc, &s, " phrasecount ");
  }
  nPhrase = pApi->xPhraseCount(pFts);
  if( 0==zReq || 0==sqlite3_stricmp(zReq, "phrasecount") ){
    sqlite3Fts5BufferAppendPrintf(&rc, &s, "%d", nPhrase);
  }

  if( zReq==0 ){
    sqlite3Fts5BufferAppendPrintf(&rc, &s, " phrasesize ");
  }
  if( 0==zReq || 0==sqlite3_stricmp(zReq, "phrasesize") ){
    if( nPhrase==1 ){
      int nSize = pApi->xPhraseSize(pFts, 0);
      sqlite3Fts5BufferAppendPrintf(&rc, &s, "%d", nSize);
    }else{
      sqlite3Fts5BufferAppendPrintf(&rc, &s, "{");
      for(i=0; i<nPhrase; i++){
        int nSize = pApi->xPhraseSize(pFts, i);
        sqlite3Fts5BufferAppendPrintf(&rc, &s, "%s%d", (i==0?"":" "), nSize);
      }
      sqlite3Fts5BufferAppendPrintf(&rc, &s, "}");
    }
  }

  if( zReq==0 ){
    sqlite3Fts5BufferAppendPrintf(&rc, &s, " poslist ");
  }
  if( 0==zReq || 0==sqlite3_stricmp(zReq, "poslist") ){
    sqlite3Fts5BufferAppendPrintf(&rc, &s, "{");
    for(i=0; i<nPhrase; i++){
      int j = 0;
      int iOff = 0;
      int iCol = 0;
      int bFirst = 1;
      sqlite3Fts5BufferAppendPrintf(&rc, &s, "%s{", (i==0?"":" "));
      while( 0==pApi->xPoslist(pFts, i, &j, &iCol, &iOff) ){
        sqlite3Fts5BufferAppendPrintf(
            &rc, &s, "%s%d.%d", (bFirst?"":" "), iCol, iOff
        );
        bFirst = 0;
      }
      sqlite3Fts5BufferAppendPrintf(&rc, &s, "}");
    }
    sqlite3Fts5BufferAppendPrintf(&rc, &s, "}");
  }

  if( zReq==0 ){
    sqlite3Fts5BufferAppendPrintf(&rc, &s, " rowid ");
  }
  if( 0==zReq || 0==sqlite3_stricmp(zReq, "rowid") ){
    iRowid = pApi->xRowid(pFts);
    sqlite3Fts5BufferAppendPrintf(&rc, &s, "%lld", iRowid);
  }

  if( rc==SQLITE_OK ){
    sqlite3_result_text(pCtx, (const char*)s.p, -1, SQLITE_TRANSIENT);
  }else{
    sqlite3_result_error_code(pCtx, rc);
  }
  sqlite3Fts5BufferFree(&s);
}

int sqlite3Fts5AuxInit(Fts5Global *pGlobal){
  struct Builtin {
    const char *zFunc;            /* Function name (nul-terminated) */
    void *pUserData;              /* User-data pointer */
    fts5_extension_function xFunc;/* Callback function */
    void (*xDestroy)(void*);      /* Destructor function */
  } aBuiltin [] = {
    { "snippet", 0, fts5SnippetFunction, 0 },
    { "fts5_test", 0, fts5TestFunction, 0 },
  };

  int rc = SQLITE_OK;             /* Return code */
  int i;                          /* To iterate through builtin functions */

  for(i=0; rc==SQLITE_OK && i<sizeof(aBuiltin)/sizeof(aBuiltin[0]); i++){
    rc = sqlite3Fts5CreateAux(pGlobal, 
        aBuiltin[i].zFunc,
        aBuiltin[i].pUserData,
        aBuiltin[i].xFunc,
        aBuiltin[i].xDestroy
    );
  }

  return rc;
}



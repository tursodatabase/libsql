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

typedef struct SnippetPhrase SnippetPhrase;
typedef struct SnippetIter SnippetIter;
typedef struct SnippetCtx SnippetCtx;

struct SnippetPhrase {
  u64 mask;                       /* Current mask */
  int nToken;                     /* Tokens in this phrase */
  int i;                          /* Current offset in phrase poslist */
  i64 iPos;                       /* Next position in phrase (-ve -> EOF) */
};

struct SnippetIter {
  i64 iLast;                      /* Last token position of current snippet */
  int nScore;                     /* Score of current snippet */

  const Fts5ExtensionApi *pApi;
  Fts5Context *pFts;
  u64 szmask;                     /* Mask used to on SnippetPhrase.mask */
  int nPhrase;                    /* Number of phrases */
  SnippetPhrase aPhrase[0];       /* Array of size nPhrase */
};

struct SnippetCtx {
  int iFirst;                     /* Offset of first token to record */
  int nToken;                     /* Size of aiStart[] and aiEnd[] arrays */
  int iSeen;                      /* Set to largest offset seen */
  int *aiStart; 
  int *aiEnd;
};

static int fts5SnippetCallback(
  void *pContext,                 /* Pointer to Fts5Buffer object */
  const char *pToken,             /* Buffer containing token */
  int nToken,                     /* Size of token in bytes */
  int iStart,                     /* Start offset of token */
  int iEnd,                       /* End offset of token */
  int iPos                        /* Position offset of token */
){
  int rc = SQLITE_OK;
  SnippetCtx *pCtx = (SnippetCtx*)pContext;
  int iOff = iPos - pCtx->iFirst;

  if( iOff>=0 ){
    if( iOff < pCtx->nToken ){
      pCtx->aiStart[iOff] = iStart;
      pCtx->aiEnd[iOff] = iEnd;
    }
    pCtx->iSeen = iPos;
    if( iOff>=pCtx->nToken ) rc = SQLITE_DONE;
  }

  return rc;
}

/*
** Set pIter->nScore to the score for the current entry.
*/
static void fts5SnippetCalculateScore(SnippetIter *pIter){
  int i;
  int nScore = 0;
  assert( pIter->iLast>=0 );

  for(i=0; i<pIter->nPhrase; i++){
    SnippetPhrase *p = &pIter->aPhrase[i];
    u64 mask = p->mask;
    if( mask ){
      u64 j;
      nScore += 1000;
      for(j=1; j & pIter->szmask; j<<=1){
        if( mask & j ) nScore++;
      }
    }
  }

  pIter->nScore = nScore;
}

/*
** Allocate a new snippet iter.
*/
static int fts5SnippetIterNew(
  const Fts5ExtensionApi *pApi,   /* API offered by current FTS version */
  Fts5Context *pFts,              /* First arg to pass to pApi functions */
  int nToken,                     /* Number of tokens in snippets */
  SnippetIter **ppIter            /* OUT: New object */
){
  int i;                          /* Counter variable */
  SnippetIter *pIter;             /* New iterator object */
  int nByte;                      /* Bytes of space to allocate */
  int nPhrase;                    /* Number of phrases in query */

  *ppIter = 0;
  nPhrase = pApi->xPhraseCount(pFts);
  nByte = sizeof(SnippetIter) + nPhrase * sizeof(SnippetPhrase);
  pIter = (SnippetIter*)sqlite3_malloc(nByte);
  if( pIter==0 ) return SQLITE_NOMEM;
  memset(pIter, 0, nByte);

  pIter->nPhrase = nPhrase;
  pIter->pApi = pApi;
  pIter->pFts = pFts;
  pIter->szmask = ((u64)1 << nToken) - 1;
  assert( nToken<=63 );

  for(i=0; i<nPhrase; i++){
    pIter->aPhrase[i].nToken = pApi->xPhraseSize(pFts, i);
  }

  *ppIter = pIter;
  return SQLITE_OK;
}

/*
** Set the iterator to point to the first candidate snippet.
*/
static void fts5SnippetIterFirst(SnippetIter *pIter){
  const Fts5ExtensionApi *pApi = pIter->pApi;
  Fts5Context *pFts = pIter->pFts;
  int i;                          /* Used to iterate through phrases */
  SnippetPhrase *pMin = 0;        /* Phrase with first match */

  memset(pIter->aPhrase, 0, sizeof(SnippetPhrase) * pIter->nPhrase);

  for(i=0; i<pIter->nPhrase; i++){
    SnippetPhrase *p = &pIter->aPhrase[i];
    p->nToken = pApi->xPhraseSize(pFts, i);
    pApi->xPoslist(pFts, i, &p->i, &p->iPos);
    if( p->iPos>=0 && (pMin==0 || p->iPos<pMin->iPos) ){
      pMin = p;
    }
  }
  assert( pMin );

  pIter->iLast = pMin->iPos + pMin->nToken - 1;
  pMin->mask = 0x01;
  pApi->xPoslist(pFts, pMin - pIter->aPhrase, &pMin->i, &pMin->iPos);
  fts5SnippetCalculateScore(pIter);
}

/*
** Advance the snippet iterator to the next candidate snippet.
*/
static void fts5SnippetIterNext(SnippetIter *pIter){
  const Fts5ExtensionApi *pApi = pIter->pApi;
  Fts5Context *pFts = pIter->pFts;
  int nPhrase = pIter->nPhrase;
  int i;                          /* Used to iterate through phrases */
  SnippetPhrase *pMin = 0;

  for(i=0; i<nPhrase; i++){
    SnippetPhrase *p = &pIter->aPhrase[i];
    if( p->iPos>=0 && (pMin==0 || p->iPos<pMin->iPos) ) pMin = p;
  }

  if( pMin==0 ){
    /* pMin==0 indicates that the SnippetIter is at EOF. */
    pIter->iLast = -1;
  }else{
    i64 nShift = pMin->iPos - pIter->iLast;
    assert( nShift>=0 );
    for(i=0; i<nPhrase; i++){
      SnippetPhrase *p = &pIter->aPhrase[i];
      if( nShift>=63 ){
        p->mask = 0;
      }else{
        p->mask = p->mask << (int)nShift;
        p->mask &= pIter->szmask;
      }
    }

    pIter->iLast = pMin->iPos;
    pMin->mask |= 0x01;
    fts5SnippetCalculateScore(pIter);
    pApi->xPoslist(pFts, pMin - pIter->aPhrase, &pMin->i, &pMin->iPos);
  }
}

static void fts5SnippetIterFree(SnippetIter *pIter){
  if( pIter ){
    sqlite3_free(pIter);
  }
}

static int fts5SnippetText(
  const Fts5ExtensionApi *pApi,   /* API offered by current FTS version */
  Fts5Context *pFts,              /* First arg to pass to pApi functions */
  SnippetIter *pIter,             /* Snippet to write to buffer */
  int nToken,                     /* Size of desired snippet in tokens */
  const char *zStart,
  const char *zFinal,
  const char *zEllip,
  Fts5Buffer *pBuf                /* Write output to this buffer */
){
  SnippetCtx ctx;
  int i;
  u64 all = 0;
  const char *zCol;               /* Column text to extract snippet from */
  int nCol;                       /* Size of column text in bytes */
  int rc;
  int nShift;

  rc = pApi->xColumnText(pFts, FTS5_POS2COLUMN(pIter->iLast), &zCol, &nCol);
  if( rc!=SQLITE_OK ) return rc;

  /* At this point pIter->iLast is the offset of the last token in the
  ** proposed snippet. However, in all cases pIter->iLast contains the
  ** final token of one of the phrases. This makes the snippet look
  ** unbalanced. For example:
  **
  **     "...x x x x x <b>term</b>..."
  **
  ** It is better to increase iLast a little so that the snippet looks
  ** more like:
  **
  **     "...x x x <b>term</b> y y..."
  **
  ** The problem is that there is no easy way to discover whether or not
  ** how many tokens are present in the column following "term". 
  */

  /* Set variable nShift to the number of tokens by which the snippet
  ** should be shifted, assuming there are sufficient tokens to the right
  ** of iLast in the column value.  */
  for(i=0; i<pIter->nPhrase; i++){
    int iToken;
    for(iToken=0; iToken<pIter->aPhrase[i].nToken; iToken++){
      all |= (pIter->aPhrase[i].mask << iToken);
    }
  }
  for(i=nToken-1; i>=0; i--){
    if( all & ((u64)1 << i) ) break;
  }
  assert( i>=0 );
  nShift = (nToken - i) / 2;

  memset(&ctx, 0, sizeof(SnippetCtx));
  ctx.nToken = nToken + nShift;
  ctx.iFirst = FTS5_POS2OFFSET(pIter->iLast) - nToken + 1;
  if( ctx.iFirst<0 ){
    nShift += ctx.iFirst;
    if( nShift<0 ) nShift = 0;
    ctx.iFirst = 0;
  }
  ctx.aiStart = (int*)sqlite3_malloc(sizeof(int) * ctx.nToken * 2);
  if( ctx.aiStart==0 ) return SQLITE_NOMEM;
  ctx.aiEnd = &ctx.aiStart[ctx.nToken];

  rc = pApi->xTokenize(pFts, zCol, nCol, (void*)&ctx, fts5SnippetCallback);
  if( rc==SQLITE_OK ){
    int i1;                       /* First token from input to include */
    int i2;                       /* Last token from input to include */

    int iPrint;
    int iMatchto;
    int iBit0;
    int iLast;

    int *aiStart = ctx.aiStart - ctx.iFirst;
    int *aiEnd = ctx.aiEnd - ctx.iFirst;

    /* Ideally we want to start the snippet with token (ctx.iFirst + nShift).
    ** However, this is only possible if there are sufficient tokens within
    ** the column. This block sets variables i1 and i2 to the first and last
    ** input tokens to include in the snippet.  */
    if( (ctx.iFirst + nShift + nToken)<=ctx.iSeen ){
      i1 = ctx.iFirst + nShift;
      i2 = i1 + nToken - 1;
    }else{
      i2 = ctx.iSeen;
      i1 = ctx.iSeen - nToken + 1;
      assert( i1>=0 || ctx.iFirst==0 );
      if( i1<0 ) i1 = 0;
    }

    /* If required, append the preceding ellipsis. */
    if( i1>0 ) sqlite3Fts5BufferAppendPrintf(&rc, pBuf, "%s", zEllip);

    iLast = FTS5_POS2OFFSET(pIter->iLast);
    iPrint = i1;
    iMatchto = -1;

    for(i=i1; i<=i2; i++){

      /* Check if this is the first token of any phrase match. */
      int ip;
      for(ip=0; ip<pIter->nPhrase; ip++){
        SnippetPhrase *pPhrase = &pIter->aPhrase[ip];
        u64 m = (1 << (iLast - i - pPhrase->nToken + 1));

        if( i<=iLast && (pPhrase->mask & m) ){
          if( iMatchto<0 ){
            sqlite3Fts5BufferAppendPrintf(&rc, pBuf, "%.*s%s",
                aiStart[i] - aiStart[iPrint],
                &zCol[aiStart[iPrint]],
                zStart
            );
            iPrint = i;
          }
          if( i>iMatchto ) iMatchto = i + pPhrase->nToken - 1;
        }
      }

      if( i==iMatchto ){
        sqlite3Fts5BufferAppendPrintf(&rc, pBuf, "%.*s%s",
            aiEnd[i] - aiStart[iPrint],
            &zCol[aiStart[iPrint]],
            zFinal
        );
        iMatchto = -1;
        iPrint = i+1;

        if( i<i2 ){
          sqlite3Fts5BufferAppendPrintf(&rc, pBuf, "%.*s",
              aiStart[i+1] - aiEnd[i],
              &zCol[aiEnd[i]]
          );
        }
      }
    }

    if( iPrint<=i2 ){
      sqlite3Fts5BufferAppendPrintf(&rc, pBuf, "%.*s", 
          aiEnd[i2] - aiStart[iPrint], 
          &zCol[aiStart[iPrint]]
      );
      if( iMatchto>=0 ){
        sqlite3Fts5BufferAppendString(&rc, pBuf, zFinal);
      }
    }

    /* If required, append the trailing ellipsis. */
    if( i2<ctx.iSeen ) sqlite3Fts5BufferAppendString(&rc, pBuf, zEllip);
  }

  sqlite3_free(ctx.aiStart);
  return rc;
}

/*
** A default snippet() implementation. This is compatible with the FTS3
** snippet() function.
*/
static void fts5SnippetFunction(
  const Fts5ExtensionApi *pApi,   /* API offered by current FTS version */
  Fts5Context *pFts,              /* First arg to pass to pApi functions */
  sqlite3_context *pCtx,          /* Context for returning result/error */
  int nVal,                       /* Number of values in apVal[] array */
  sqlite3_value **apVal           /* Array of trailing arguments */
){
  const char *zStart = "<b>";
  const char *zFinal = "</b>";
  const char *zEllip = "<b>...</b>";
  int nToken = -15;
  int nAbs;
  int nFrag;                      /* Number of fragments to return */
  int rc;
  SnippetIter *pIter = 0;

  if( nVal>=1 ) zStart = (const char*)sqlite3_value_text(apVal[0]);
  if( nVal>=2 ) zFinal = (const char*)sqlite3_value_text(apVal[1]);
  if( nVal>=3 ) zEllip = (const char*)sqlite3_value_text(apVal[2]);
  if( nVal>=4 ){
    nToken = sqlite3_value_int(apVal[3]);
    if( nToken==0 ) nToken = -15;
  }
  nAbs = nToken * (nToken<0 ? -1 : 1);

  rc = fts5SnippetIterNew(pApi, pFts, nAbs, &pIter);
  if( rc==SQLITE_OK ){
    Fts5Buffer buf;               /* Result buffer */
    int nBestScore = 0;           /* Score of best snippet found */
    int n;                        /* Size of column snippet is from in bytes */
    int i;                        /* Used to iterate through phrases */

    for(fts5SnippetIterFirst(pIter); 
        pIter->iLast>=0; 
        fts5SnippetIterNext(pIter)
    ){
      if( pIter->nScore>nBestScore ) nBestScore = pIter->nScore;
    }
    for(fts5SnippetIterFirst(pIter); 
        pIter->iLast>=0; 
        fts5SnippetIterNext(pIter)
    ){
      if( pIter->nScore==nBestScore ) break;
    }

    memset(&buf, 0, sizeof(Fts5Buffer));
    rc = fts5SnippetText(pApi, pFts, pIter, nAbs, zStart, zFinal, zEllip, &buf);
    if( rc==SQLITE_OK ){
      sqlite3_result_text(pCtx, (const char*)buf.p, buf.n, SQLITE_TRANSIENT);
    }
    sqlite3_free(buf.p);
  }

  fts5SnippetIterFree(pIter);
  if( rc!=SQLITE_OK ){
    sqlite3_result_error_code(pCtx, rc);
  }
}

static void fts5Bm25Function(
  const Fts5ExtensionApi *pApi,   /* API offered by current FTS version */
  Fts5Context *pFts,              /* First arg to pass to pApi functions */
  sqlite3_context *pCtx,          /* Context for returning result/error */
  int nVal,                       /* Number of values in apVal[] array */
  sqlite3_value **apVal           /* Array of trailing arguments */
){
  assert( 0 );
}

static int fts5TestCallback(
  void *pContext,                 /* Pointer to Fts5Buffer object */
  const char *pToken,             /* Buffer containing token */
  int nToken,                     /* Size of token in bytes */
  int iStart,                     /* Start offset of token */
  int iEnd,                       /* End offset of token */
  int iPos                        /* Position offset of token */
){
  int rc = SQLITE_OK;
  Fts5Buffer *pBuf = (Fts5Buffer*)pContext;
  if( pBuf->n!=0 ){
    sqlite3Fts5BufferAppendString(&rc, pBuf, " ");
  }
  sqlite3Fts5BufferAppendListElem(&rc, pBuf, pToken, nToken);
  return rc;
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
  nCol = pApi->xColumnCount(pFts);

  if( zReq==0 ){
    sqlite3Fts5BufferAppendPrintf(&rc, &s, "columnavgsize ");
  }
  if( 0==zReq || 0==sqlite3_stricmp(zReq, "columnavgsize") ){
    if( zReq==0 && nCol>1 ) sqlite3Fts5BufferAppendPrintf(&rc, &s, "{");
    for(i=0; rc==SQLITE_OK && i<nCol; i++){
      int colsz = 0;
      rc = pApi->xColumnAvgSize(pFts, i, &colsz);
      sqlite3Fts5BufferAppendPrintf(&rc, &s, "%s%d", i==0?"":" ", colsz);
    }
    if( zReq==0 && nCol>1 ) sqlite3Fts5BufferAppendPrintf(&rc, &s, "}");
  }

  if( zReq==0 ){
    sqlite3Fts5BufferAppendPrintf(&rc, &s, "columncount ");
  }
  if( 0==zReq || 0==sqlite3_stricmp(zReq, "columncount") ){
    sqlite3Fts5BufferAppendPrintf(&rc, &s, "%d", nCol);
  }

  if( zReq==0 ){
    sqlite3Fts5BufferAppendPrintf(&rc, &s, "columnsize ");
  }
  if( 0==zReq || 0==sqlite3_stricmp(zReq, "columnsize") ){
    if( zReq==0 && nCol>1 ) sqlite3Fts5BufferAppendPrintf(&rc, &s, "{");
    for(i=0; rc==SQLITE_OK && i<nCol; i++){
      int colsz = 0;
      rc = pApi->xColumnSize(pFts, i, &colsz);
      sqlite3Fts5BufferAppendPrintf(&rc, &s, "%s%d", i==0?"":" ", colsz);
    }
    if( zReq==0 && nCol>1 ) sqlite3Fts5BufferAppendPrintf(&rc, &s, "}");
  }

  if( zReq==0 ){
    sqlite3Fts5BufferAppendPrintf(&rc, &s, "columntext ");
  }
  if( 0==zReq || 0==sqlite3_stricmp(zReq, "columntext") ){
    for(i=0; rc==SQLITE_OK && i<nCol; i++){
      const char *z;
      int n;
      rc = pApi->xColumnText(pFts, i, &z, &n);
      if( i!=0 ) sqlite3Fts5BufferAppendPrintf(&rc, &s, " ");
      sqlite3Fts5BufferAppendListElem(&rc, &s, z, n);
    }
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
    int bParen = 0;
    Fts5Buffer s3;
    memset(&s3, 0, sizeof(s3));


    for(i=0; i<nPhrase; i++){
      Fts5Buffer s2;                  /* List of positions for phrase/column */
      int j = 0;
      i64 iPos = 0;
      int nElem = 0;

      memset(&s2, 0, sizeof(s2));
      while( 0==pApi->xPoslist(pFts, i, &j, &iPos) ){
        int iOff = FTS5_POS2OFFSET(iPos);
        int iCol = FTS5_POS2COLUMN(iPos);
        if( nElem!=0 ) sqlite3Fts5BufferAppendPrintf(&rc, &s2, " ");
        sqlite3Fts5BufferAppendPrintf(&rc, &s2, "%d.%d", iCol, iOff);
        nElem++;
      }

      if( i!=0 ){
        sqlite3Fts5BufferAppendPrintf(&rc, &s3, " ");
      }
      if( nElem==1 ){
        sqlite3Fts5BufferAppendPrintf(&rc, &s3, "%s", (const char*)s2.p);
      }else{
        sqlite3Fts5BufferAppendPrintf(&rc, &s3, "{%s}", (const char*)s2.p);
        bParen = 1;
      }
      sqlite3_free(s2.p);
    }

    if(zReq==0 && (nPhrase>1 || bParen) ){
      sqlite3Fts5BufferAppendPrintf(&rc, &s, "{%s}", (const char*)s3.p);
    }else{
      sqlite3Fts5BufferAppendPrintf(&rc, &s, "%s", (const char*)s3.p);
    }
    sqlite3_free(s3.p);
  }

  if( zReq==0 ){
    sqlite3Fts5BufferAppendPrintf(&rc, &s, " rowid ");
  }
  if( 0==zReq || 0==sqlite3_stricmp(zReq, "rowid") ){
    iRowid = pApi->xRowid(pFts);
    sqlite3Fts5BufferAppendPrintf(&rc, &s, "%lld", iRowid);
  }

  if( zReq==0 ){
    sqlite3Fts5BufferAppendPrintf(&rc, &s, " tokenize ");
  }
  if( 0==zReq || 0==sqlite3_stricmp(zReq, "tokenize") ){
    Fts5Buffer buf;
    memset(&buf, 0, sizeof(buf));
    for(i=0; rc==SQLITE_OK && i<nCol; i++){
      const char *z;
      int n;
      rc = pApi->xColumnText(pFts, i, &z, &n);
      if( rc==SQLITE_OK ){
        Fts5Buffer buf1;
        memset(&buf1, 0, sizeof(Fts5Buffer));
        rc = pApi->xTokenize(pFts, z, n, (void*)&buf1, fts5TestCallback);
        if( i!=0 ) sqlite3Fts5BufferAppendPrintf(&rc, &buf, " ");
        sqlite3Fts5BufferAppendListElem(&rc, &buf, (const char*)buf1.p, buf1.n);
        sqlite3_free(buf1.p);
      }
    }
    if( zReq==0 ){
      sqlite3Fts5BufferAppendListElem(&rc, &s, (const char*)buf.p, buf.n);
    }else{
      sqlite3Fts5BufferAppendString(&rc, &s, (const char*)buf.p);
    }
    sqlite3_free(buf.p);
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



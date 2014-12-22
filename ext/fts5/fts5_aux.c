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
#include <math.h>

/*
** Object used to iterate through all "coalesced phrase instances" in 
** a single column of the current row. If the phrase instances in the
** column being considered do not overlap, this object simply iterates
** through them. Or, if they do overlap (share one or more tokens in
** common), each set of overlapping instances is treated as a single
** match. See documentation for the highlight() auxiliary function for
** details.
**
** Usage is:
**
**   for(rc = fts5CInstIterNext(pApi, pFts, iCol, &iter);
**      (rc==SQLITE_OK && 0==fts5CInstIterEof(&iter);
**      rc = fts5CInstIterNext(&iter)
**   ){
**     printf("instance starts at %d, ends at %d\n", iter.iStart, iter.iEnd);
**   }
**
*/
typedef struct CInstIter CInstIter;
struct CInstIter {
  const Fts5ExtensionApi *pApi;   /* API offered by current FTS version */
  Fts5Context *pFts;              /* First arg to pass to pApi functions */
  int iCol;                       /* Column to search */
  int iInst;                      /* Next phrase instance index */
  int nInst;                      /* Total number of phrase instances */

  /* Output variables */
  int iStart;                     /* First token in coalesced phrase instance */
  int iEnd;                       /* Last token in coalesced phrase instance */
};

/*
** Return non-zero if the iterator is at EOF, or zero otherwise.
*/
static int fts5CInstIterEof(CInstIter *pIter){
  return (pIter->iStart < 0);
}

/*
** Advance the iterator to the next coalesced phrase instance. Return
** an SQLite error code if an error occurs, or SQLITE_OK otherwise.
*/
static int fts5CInstIterNext(CInstIter *pIter){
  int rc = SQLITE_OK;
  pIter->iStart = -1;
  pIter->iEnd = -1;

  while( rc==SQLITE_OK && pIter->iInst<pIter->nInst ){
    int ip; int ic; int io;
    rc = pIter->pApi->xInst(pIter->pFts, pIter->iInst, &ip, &ic, &io);
    if( rc==SQLITE_OK ){
      if( ic==pIter->iCol ){
        int iEnd = io - 1 + pIter->pApi->xPhraseSize(pIter->pFts, ip);
        if( pIter->iStart<0 ){
          pIter->iStart = io;
          pIter->iEnd = iEnd;
        }else if( io<=pIter->iEnd ){
          if( iEnd>pIter->iEnd ) pIter->iEnd = iEnd;
        }else{
          break;
        }
      }
      pIter->iInst++;
    }
  }

  return rc;
}

/*
** Initialize the iterator object indicated by the final parameter to 
** iterate through coalesced phrase instances in column iCol.
*/
static int fts5CInstIterInit(
  const Fts5ExtensionApi *pApi,
  Fts5Context *pFts,
  int iCol,
  CInstIter *pIter
){
  int rc;

  memset(pIter, 0, sizeof(CInstIter));
  pIter->pApi = pApi;
  pIter->pFts = pFts;
  pIter->iCol = iCol;
  rc = pApi->xInstCount(pFts, &pIter->nInst);

  if( rc==SQLITE_OK ){
    rc = fts5CInstIterNext(pIter);
  }

  return rc;
}



/*************************************************************************
** Start of highlight() implementation.
*/
typedef struct HighlightContext HighlightContext;
struct HighlightContext {
  CInstIter iter;                 /* Coalesced Instance Iterator */
  int iRangeStart;
  int iRangeEnd;
  const char *zOpen;              /* Opening highlight */
  const char *zClose;             /* Closing highlight */
  const char *zIn;                /* Input text */
  int nIn;                        /* Size of input text in bytes */
  int iOff;                       /* Current offset within zIn[] */
  char *zOut;                     /* Output value */
};

/*
** Append text to the HighlightContext output string - p->zOut. Argument
** z points to a buffer containing n bytes of text to append. If n is 
** negative, everything up until the first '\0' is appended to the output.
**
** If *pRc is set to any value other than SQLITE_OK when this function is 
** called, it is a no-op. If an error (i.e. an OOM condition) is encountered, 
** *pRc is set to an error code before returning. 
*/
static void fts5HighlightAppend(
  int *pRc, 
  HighlightContext *p, 
  const char *z, int n
){
  if( *pRc==SQLITE_OK ){
    if( n<0 ) n = strlen(z);
    p->zOut = sqlite3_mprintf("%z%.*s", p->zOut, n, z);
    if( p->zOut==0 ) *pRc = SQLITE_NOMEM;
  }
}

/*
** Tokenizer callback used by implementation of highlight() function.
*/
static int fts5HighlightCb(
  void *pContext,                 /* Pointer to HighlightContext object */
  const char *pToken,             /* Buffer containing token */
  int nToken,                     /* Size of token in bytes */
  int iStartOff,                  /* Start offset of token */
  int iEndOff,                    /* End offset of token */
  int iPos                        /* Position offset of token */
){
  HighlightContext *p = (HighlightContext*)pContext;
  int rc = SQLITE_OK;

  if( p->iRangeEnd>0 ){
    if( iPos<p->iRangeStart || iPos>p->iRangeEnd ) return SQLITE_OK;
    if( iPos==p->iRangeStart ) p->iOff = iStartOff;
  }

  if( iPos==p->iter.iStart ){
    fts5HighlightAppend(&rc, p, &p->zIn[p->iOff], iStartOff - p->iOff);
    fts5HighlightAppend(&rc, p, p->zOpen, -1);
    p->iOff = iStartOff;
  }

  if( iPos==p->iter.iEnd ){
    if( p->iRangeEnd && p->iter.iStart<p->iRangeStart ){
      fts5HighlightAppend(&rc, p, p->zOpen, -1);
    }
    fts5HighlightAppend(&rc, p, &p->zIn[p->iOff], iEndOff - p->iOff);
    fts5HighlightAppend(&rc, p, p->zClose, -1);
    p->iOff = iEndOff;
    if( rc==SQLITE_OK ){
      rc = fts5CInstIterNext(&p->iter);
    }
  }

  if( p->iRangeEnd>0 && iPos==p->iRangeEnd ){
    fts5HighlightAppend(&rc, p, &p->zIn[p->iOff], iEndOff - p->iOff);
    p->iOff = iEndOff;
    if( iPos<p->iter.iEnd ){
      fts5HighlightAppend(&rc, p, p->zClose, -1);
    }
  }

  return rc;
}

/*
** Implementation of highlight() function.
*/
static void fts5HighlightFunction(
  const Fts5ExtensionApi *pApi,   /* API offered by current FTS version */
  Fts5Context *pFts,              /* First arg to pass to pApi functions */
  sqlite3_context *pCtx,          /* Context for returning result/error */
  int nVal,                       /* Number of values in apVal[] array */
  sqlite3_value **apVal           /* Array of trailing arguments */
){
  HighlightContext ctx;
  int rc;
  int iCol;

  if( nVal!=3 ){
    const char *zErr = "wrong number of arguments to function highlight()";
    sqlite3_result_error(pCtx, zErr, -1);
    return;
  }

  iCol = sqlite3_value_int(apVal[0]);
  memset(&ctx, 0, sizeof(HighlightContext));
  ctx.zOpen = (const char*)sqlite3_value_text(apVal[1]);
  ctx.zClose = (const char*)sqlite3_value_text(apVal[2]);
  rc = pApi->xColumnText(pFts, iCol, &ctx.zIn, &ctx.nIn);

  if( rc==SQLITE_OK ){
    rc = fts5CInstIterInit(pApi, pFts, iCol, &ctx.iter);
  }

  if( rc==SQLITE_OK ){
    rc = pApi->xTokenize(pFts, ctx.zIn, ctx.nIn, (void*)&ctx, fts5HighlightCb);
  }
  fts5HighlightAppend(&rc, &ctx, &ctx.zIn[ctx.iOff], ctx.nIn - ctx.iOff);

  if( rc==SQLITE_OK ){
    sqlite3_result_text(pCtx, (const char*)ctx.zOut, -1, SQLITE_TRANSIENT);
  }else{
    sqlite3_result_error_code(pCtx, rc);
  }
  sqlite3_free(ctx.zOut);
}
/*
**************************************************************************/


static void fts5SnippetFunction(
  const Fts5ExtensionApi *pApi,   /* API offered by current FTS version */
  Fts5Context *pFts,              /* First arg to pass to pApi functions */
  sqlite3_context *pCtx,          /* Context for returning result/error */
  int nVal,                       /* Number of values in apVal[] array */
  sqlite3_value **apVal           /* Array of trailing arguments */
){
  HighlightContext ctx;
  int rc = SQLITE_OK;             /* Return code */
  int iCol;                       /* 1st argument to snippet() */
  const char *zEllips;            /* 4th argument to snippet() */
  int nToken;                     /* 5th argument to snippet() */
  int nInst;                      /* Number of instance matches this row */
  int i;                          /* Used to iterate through instances */
  int nPhrase;                    /* Number of phrases in query */
  unsigned char *aSeen;           /* Array of "seen instance" flags */
  int iBestCol;                   /* Column containing best snippet */
  int iBestStart = 0;             /* First token of best snippet */
  int iBestLast = nToken;         /* Last token of best snippet */
  int nBestScore = 0;             /* Score of best snippet */
  int nColSize;                   /* Total size of iBestCol in tokens */

  if( nVal!=5 ){
    const char *zErr = "wrong number of arguments to function snippet()";
    sqlite3_result_error(pCtx, zErr, -1);
    return;
  }

  memset(&ctx, 0, sizeof(HighlightContext));
  rc = pApi->xColumnText(pFts, iCol, &ctx.zIn, &ctx.nIn);

  iCol = sqlite3_value_int(apVal[0]);
  ctx.zOpen = (const char*)sqlite3_value_text(apVal[1]);
  ctx.zClose = (const char*)sqlite3_value_text(apVal[2]);
  zEllips = (const char*)sqlite3_value_text(apVal[3]);
  nToken = sqlite3_value_int(apVal[4]);

  iBestCol = (iCol>=0 ? iCol : 0);
  nPhrase = pApi->xPhraseCount(pFts);
  aSeen = sqlite3_malloc(nPhrase);
  if( aSeen==0 ){
    rc = SQLITE_NOMEM;
  }

  if( rc==SQLITE_OK ){
    rc = pApi->xInstCount(pFts, &nInst);
  }
  for(i=0; rc==SQLITE_OK && i<nInst; i++){
    int ip, iSnippetCol, iStart;
    memset(aSeen, 0, nPhrase);
    rc = pApi->xInst(pFts, i, &ip, &iSnippetCol, &iStart);
    if( rc==SQLITE_OK && (iCol<0 || iSnippetCol==iCol) ){
      int nScore = 1000;
      int iLast = iStart - 1 + pApi->xPhraseSize(pFts, ip);
      int j;
      aSeen[ip] = 1;

      for(j=i+1; rc==SQLITE_OK && j<nInst; j++){
        int ic; int io; int iFinal;
        rc = pApi->xInst(pFts, j, &ip, &ic, &io);
        iFinal = io + pApi->xPhraseSize(pFts, ip) - 1;
        if( rc==SQLITE_OK && ic==iSnippetCol && iLast<iStart+nToken ){
          nScore += aSeen[ip] ? 1000 : 1;
          aSeen[ip] = 1;
          if( iFinal>iLast ) iLast = iFinal;
        }
      }

      if( rc==SQLITE_OK && nScore>nBestScore ){
        iBestCol = iSnippetCol;
        iBestStart = iStart;
        iBestLast = iLast;
        nBestScore = nScore;
      }
    }
  }

  if( rc==SQLITE_OK ){
    rc = pApi->xColumnSize(pFts, iBestCol, &nColSize);
  }
  if( rc==SQLITE_OK ){
    rc = pApi->xColumnText(pFts, iBestCol, &ctx.zIn, &ctx.nIn);
  }
  if( rc==SQLITE_OK ){
    rc = fts5CInstIterInit(pApi, pFts, iBestCol, &ctx.iter);
  }

  if( (iBestStart+nToken-1)>iBestLast ){
    iBestStart -= (iBestStart+nToken-1-iBestLast) / 2;
  }
  if( iBestStart+nToken>nColSize ){
    iBestStart = nColSize - nToken;
  }
  if( iBestStart<0 ) iBestStart = 0;

  ctx.iRangeStart = iBestStart;
  ctx.iRangeEnd = iBestStart + nToken - 1;

  if( iBestStart>0 ){
    fts5HighlightAppend(&rc, &ctx, zEllips, -1);
  }
  if( rc==SQLITE_OK ){
    rc = pApi->xTokenize(pFts, ctx.zIn, ctx.nIn, (void*)&ctx, fts5HighlightCb);
  }
  if( ctx.iRangeEnd>=(nColSize-1) ){
    fts5HighlightAppend(&rc, &ctx, &ctx.zIn[ctx.iOff], ctx.nIn - ctx.iOff);
  }else{
    fts5HighlightAppend(&rc, &ctx, zEllips, -1);
  }

  if( rc==SQLITE_OK ){
    sqlite3_result_text(pCtx, (const char*)ctx.zOut, -1, SQLITE_TRANSIENT);
  }else{
    sqlite3_result_error_code(pCtx, rc);
  }
  sqlite3_free(ctx.zOut);
  sqlite3_free(aSeen);
}

/************************************************************************/


/*
** Context object passed by fts5GatherTotals() to xQueryPhrase callback
** fts5GatherCallback().
*/
struct Fts5GatherCtx {
  int nCol;                       /* Number of columns in FTS table */
  int iPhrase;                    /* Phrase currently under investigation */
  int *anVal;                     /* Array to populate */
};

/*
** Callback used by fts5GatherTotals() with the xQueryPhrase() API.
*/
static int fts5GatherCallback(
  const Fts5ExtensionApi *pApi, 
  Fts5Context *pFts,
  void *pUserData                 /* Pointer to Fts5GatherCtx object */
){
  struct Fts5GatherCtx *p = (struct Fts5GatherCtx*)pUserData;
  int i = 0;
  int iPrev = -1;
  i64 iPos = 0;

  while( 0==pApi->xPoslist(pFts, 0, &i, &iPos) ){
    int iCol = FTS5_POS2COLUMN(iPos);
    if( iCol!=iPrev ){
      p->anVal[p->iPhrase * p->nCol + iCol]++;
      iPrev = iCol;
    }
  }

  return SQLITE_OK;
}

/*
** This function returns a pointer to an array of integers containing entries
** indicating the number of rows in the table for which each phrase features 
** at least once in each column.
**
** If nCol is the number of matchable columns in the table, and nPhrase is
** the number of phrases in the query, the array contains a total of
** (nPhrase*nCol) entries.
**
** For phrase iPhrase and column iCol:
**
**   anVal[iPhrase * nCol + iCol]
**
** is set to the number of rows in the table for which column iCol contains 
** at least one instance of phrase iPhrase.
*/
static int fts5GatherTotals(
  const Fts5ExtensionApi *pApi,   /* API offered by current FTS version */
  Fts5Context *pFts,              /* First arg to pass to pApi functions */
  int **panVal
){
  int rc = SQLITE_OK;
  int *anVal = 0;
  int i;                          /* For iterating through expression phrases */
  int nPhrase = pApi->xPhraseCount(pFts);
  int nCol = pApi->xColumnCount(pFts);
  int nByte = nCol * nPhrase * sizeof(int);
  struct Fts5GatherCtx sCtx;

  sCtx.nCol = nCol;
  anVal = sCtx.anVal = (int*)sqlite3_malloc(nByte);
  if( anVal==0 ){
    rc = SQLITE_NOMEM;
  }else{
    memset(anVal, 0, nByte);
  }

  for(i=0; i<nPhrase && rc==SQLITE_OK; i++){
    sCtx.iPhrase = i;
    rc = pApi->xQueryPhrase(pFts, i, (void*)&sCtx, fts5GatherCallback);
  }

  if( rc!=SQLITE_OK ){
    sqlite3_free(anVal);
    anVal = 0;
  }

  *panVal = anVal;
  return rc;
}

typedef struct Fts5Bm25Context Fts5Bm25Context;
struct Fts5Bm25Context {
  int nPhrase;                    /* Number of phrases in query */
  int nCol;                       /* Number of columns in FTS table */
  double *aIDF;                   /* Array of IDF values */
  double *aAvg;                   /* Average size of each column in tokens */
};

static int fts5Bm25GetContext(
  const Fts5ExtensionApi *pApi,   /* API offered by current FTS version */
  Fts5Context *pFts,              /* First arg to pass to pApi functions */
  Fts5Bm25Context **pp            /* OUT: Context object */
){
  Fts5Bm25Context *p;
  int rc = SQLITE_OK;

  p = pApi->xGetAuxdata(pFts, 0);
  if( p==0 ){
    int *anVal = 0;
    int ic;                       /* For iterating through columns */
    int ip;                       /* For iterating through phrases */
    i64 nRow;                     /* Total number of rows in table */
    int nPhrase = pApi->xPhraseCount(pFts);
    int nCol = pApi->xColumnCount(pFts);
    int nByte = sizeof(Fts5Bm25Context) 
              + sizeof(double) * nPhrase * nCol       /* aIDF[] */
              + sizeof(double) * nCol;                /* aAvg[] */

    p = (Fts5Bm25Context*)sqlite3_malloc(nByte);
    if( p==0 ){
      rc = SQLITE_NOMEM;
    }else{
      memset(p, 0, nByte);
      p->aAvg = (double*)&p[1];
      p->aIDF = (double*)&p->aAvg[nCol];
      p->nCol = nCol;
      p->nPhrase = nPhrase;
    }

    if( rc==SQLITE_OK ){
      rc = pApi->xRowCount(pFts, &nRow); 
      assert( nRow>0 || rc!=SQLITE_OK );
      if( nRow<2 ) nRow = 2;
    }

    for(ic=0; rc==SQLITE_OK && ic<nCol; ic++){
      i64 nToken = 0;
      rc = pApi->xColumnTotalSize(pFts, ic, &nToken);
      p->aAvg[ic] = (double)nToken / (double)nRow;
    }

    if( rc==SQLITE_OK ){
      rc = fts5GatherTotals(pApi, pFts, &anVal);
    }
    for(ic=0; ic<nCol; ic++){
      for(ip=0; rc==SQLITE_OK && ip<nPhrase; ip++){
        /* Calculate the IDF (Inverse Document Frequency) for phrase ip
        ** in column ic. This is done using the standard BM25 formula as
        ** found on wikipedia:
        **
        **   IDF = log( (N - nHit + 0.5) / (nHit + 0.5) )
        **
        ** where "N" is the total number of documents in the set and nHit
        ** is the number that contain at least one instance of the phrase
        ** under consideration.
        **
        ** The problem with this is that if (N < 2*nHit), the IDF is 
        ** negative. Which is undesirable. So the mimimum allowable IDF is
        ** (1e-6) - roughly the same as a term that appears in just over
        ** half of set of 5,000,000 documents.  */
        int idx = ip * nCol + ic; /* Index in aIDF[] and anVal[] arrays */
        int nHit = anVal[idx];    /* Number of docs matching "ic: ip" */

        p->aIDF[idx] = log( (0.5 + nRow - nHit) / (0.5 + nHit) );
        if( p->aIDF[idx]<=0.0 ) p->aIDF[idx] = 1e-6;
        assert( p->aIDF[idx]>=0.0 );
      }
    }

    sqlite3_free(anVal);
    if( rc==SQLITE_OK ){
      rc = pApi->xSetAuxdata(pFts, p, sqlite3_free);
    }
    if( rc!=SQLITE_OK ){
      sqlite3_free(p);
      p = 0;
    }
  }

  *pp = p;
  return rc;
}

static void fts5Bm25DebugContext(
  int *pRc,                       /* IN/OUT: Return code */
  Fts5Buffer *pBuf,               /* Buffer to populate */
  Fts5Bm25Context *p              /* Context object to decode */
){
  int ip;
  int ic;

  sqlite3Fts5BufferAppendString(pRc, pBuf, "idf ");
  if( p->nPhrase>1 || p->nCol>1 ){
    sqlite3Fts5BufferAppendString(pRc, pBuf, "{");
  }
  for(ip=0; ip<p->nPhrase; ip++){
    if( ip>0 ) sqlite3Fts5BufferAppendString(pRc, pBuf, " ");
    if( p->nCol>1 ) sqlite3Fts5BufferAppendString(pRc, pBuf, "{");
    for(ic=0; ic<p->nCol; ic++){
      if( ic>0 ) sqlite3Fts5BufferAppendString(pRc, pBuf, " ");
      sqlite3Fts5BufferAppendPrintf(pRc, pBuf, "%f", p->aIDF[ip*p->nCol+ic]);
    }
    if( p->nCol>1 ) sqlite3Fts5BufferAppendString(pRc, pBuf, "}");
  }
  if( p->nPhrase>1 || p->nCol>1 ){
    sqlite3Fts5BufferAppendString(pRc, pBuf, "}");
  }

  sqlite3Fts5BufferAppendString(pRc, pBuf, " avgdl ");
  if( p->nCol>1 ) sqlite3Fts5BufferAppendString(pRc, pBuf, "{");
  for(ic=0; ic<p->nCol; ic++){
    if( ic>0 ) sqlite3Fts5BufferAppendString(pRc, pBuf, " ");
    sqlite3Fts5BufferAppendPrintf(pRc, pBuf, "%f", p->aAvg[ic]);
  }
  if( p->nCol>1 ) sqlite3Fts5BufferAppendString(pRc, pBuf, "}");
}

static void fts5Bm25DebugRow(
  int *pRc, 
  Fts5Buffer *pBuf, 
  Fts5Bm25Context *p, 
  const Fts5ExtensionApi *pApi, 
  Fts5Context *pFts
){
}

static void fts5Bm25Function(
  const Fts5ExtensionApi *pApi,   /* API offered by current FTS version */
  Fts5Context *pFts,              /* First arg to pass to pApi functions */
  sqlite3_context *pCtx,          /* Context for returning result/error */
  int nVal,                       /* Number of values in apVal[] array */
  sqlite3_value **apVal           /* Array of trailing arguments */
){
  const double k1 = 1.2;
  const double B = 0.75;
  int rc = SQLITE_OK;
  Fts5Bm25Context *p;

  rc = fts5Bm25GetContext(pApi, pFts, &p);

  if( rc==SQLITE_OK ){
    /* If the bDebug flag is set, instead of returning a numeric rank, this
    ** function returns a text value showing how the rank is calculated. */
    Fts5Buffer debug;
    int bDebug = (pApi->xUserData(pFts)!=0);
    memset(&debug, 0, sizeof(Fts5Buffer));

    int ip;
    double score = 0.0;

    if( bDebug ){
      fts5Bm25DebugContext(&rc, &debug, p);
      fts5Bm25DebugRow(&rc, &debug, p, pApi, pFts);
    }

    for(ip=0; rc==SQLITE_OK && ip<p->nPhrase; ip++){
      int iPrev = 0;
      int nHit = 0;
      int i = 0;
      i64 iPos = 0;

      while( rc==SQLITE_OK ){
        int bDone = pApi->xPoslist(pFts, ip, &i, &iPos);
        int iCol = FTS5_POS2COLUMN(iPos);
        if( (iCol!=iPrev || bDone) && nHit>0 ){
          int sz = 0;
          int idx = ip * p->nCol + iPrev;
          double bm25;
          rc = pApi->xColumnSize(pFts, iPrev, &sz);

          bm25 = (p->aIDF[idx] * nHit * (k1+1.0)) /
            (nHit + k1 * (1.0 - B + B * sz / p->aAvg[iPrev]));


          score = score + bm25;
          nHit = 0;
        }
        if( bDone ) break;
        nHit++;
        iPrev = iCol;
      }
    }

    if( rc==SQLITE_OK ){
      if( bDebug ){
        sqlite3_result_text(pCtx, (const char*)debug.p, -1, SQLITE_TRANSIENT);
      }else{
        sqlite3_result_double(pCtx, score);
      }
    }
    sqlite3_free(debug.p);
  }

  if( rc!=SQLITE_OK ){
    sqlite3_result_error_code(pCtx, rc);
  }
}

int sqlite3Fts5AuxInit(fts5_api *pApi){
  struct Builtin {
    const char *zFunc;            /* Function name (nul-terminated) */
    void *pUserData;              /* User-data pointer */
    fts5_extension_function xFunc;/* Callback function */
    void (*xDestroy)(void*);      /* Destructor function */
  } aBuiltin [] = {
    { "bm25debug", (void*)1, fts5Bm25Function,    0 },
    { "snippet",   0, fts5SnippetFunction, 0 },
    { "highlight", 0, fts5HighlightFunction, 0 },
    { "bm25",      0, fts5Bm25Function,    0 },
  };

  int rc = SQLITE_OK;             /* Return code */
  int i;                          /* To iterate through builtin functions */

  for(i=0; rc==SQLITE_OK && i<sizeof(aBuiltin)/sizeof(aBuiltin[0]); i++){
    rc = pApi->xCreateFunction(pApi,
        aBuiltin[i].zFunc,
        aBuiltin[i].pUserData,
        aBuiltin[i].xFunc,
        aBuiltin[i].xDestroy
    );
  }

  return rc;
}



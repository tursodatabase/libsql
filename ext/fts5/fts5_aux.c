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
#include <math.h>                 /* amalgamator: keep */

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
  int iPos;                       /* Current token offset in zIn[] */
  int iRangeStart;                /* First token to include */
  int iRangeEnd;                  /* If non-zero, last token to include */
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
    if( n<0 ) n = (int)strlen(z);
    p->zOut = sqlite3_mprintf("%z%.*s", p->zOut, n, z);
    if( p->zOut==0 ) *pRc = SQLITE_NOMEM;
  }
}

/*
** Tokenizer callback used by implementation of highlight() function.
*/
static int fts5HighlightCb(
  void *pContext,                 /* Pointer to HighlightContext object */
  int tflags,                     /* Mask of FTS5_TOKEN_* flags */
  const char *pToken,             /* Buffer containing token */
  int nToken,                     /* Size of token in bytes */
  int iStartOff,                  /* Start offset of token */
  int iEndOff                     /* End offset of token */
){
  HighlightContext *p = (HighlightContext*)pContext;
  int rc = SQLITE_OK;
  int iPos;

  UNUSED_PARAM2(pToken, nToken);

  if( tflags & FTS5_TOKEN_COLOCATED ) return SQLITE_OK;
  iPos = p->iPos++;

  if( p->iRangeEnd>0 ){
    if( iPos<p->iRangeStart || iPos>p->iRangeEnd ) return SQLITE_OK;
    if( p->iRangeStart && iPos==p->iRangeStart ) p->iOff = iStartOff;
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

  if( ctx.zIn ){
    if( rc==SQLITE_OK ){
      rc = fts5CInstIterInit(pApi, pFts, iCol, &ctx.iter);
    }

    if( rc==SQLITE_OK ){
      rc = pApi->xTokenize(pFts, ctx.zIn, ctx.nIn, (void*)&ctx,fts5HighlightCb);
    }
    fts5HighlightAppend(&rc, &ctx, &ctx.zIn[ctx.iOff], ctx.nIn - ctx.iOff);

    if( rc==SQLITE_OK ){
      sqlite3_result_text(pCtx, (const char*)ctx.zOut, -1, SQLITE_TRANSIENT);
    }
    sqlite3_free(ctx.zOut);
  }
  if( rc!=SQLITE_OK ){
    sqlite3_result_error_code(pCtx, rc);
  }
}
/*
** End of highlight() implementation.
**************************************************************************/

/*
** Implementation of snippet() function.
*/
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
  int nInst = 0;                  /* Number of instance matches this row */
  int i;                          /* Used to iterate through instances */
  int nPhrase;                    /* Number of phrases in query */
  unsigned char *aSeen;           /* Array of "seen instance" flags */
  int iBestCol;                   /* Column containing best snippet */
  int iBestStart = 0;             /* First token of best snippet */
  int iBestLast;                  /* Last token of best snippet */
  int nBestScore = 0;             /* Score of best snippet */
  int nColSize = 0;               /* Total size of iBestCol in tokens */

  if( nVal!=5 ){
    const char *zErr = "wrong number of arguments to function snippet()";
    sqlite3_result_error(pCtx, zErr, -1);
    return;
  }

  memset(&ctx, 0, sizeof(HighlightContext));
  iCol = sqlite3_value_int(apVal[0]);
  ctx.zOpen = (const char*)sqlite3_value_text(apVal[1]);
  ctx.zClose = (const char*)sqlite3_value_text(apVal[2]);
  zEllips = (const char*)sqlite3_value_text(apVal[3]);
  nToken = sqlite3_value_int(apVal[4]);
  iBestLast = nToken-1;

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
  if( ctx.zIn ){
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
      rc = pApi->xTokenize(pFts, ctx.zIn, ctx.nIn, (void*)&ctx,fts5HighlightCb);
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
  }
  sqlite3_free(aSeen);
}

/************************************************************************/

/*
** The first time the bm25() function is called for a query, an instance
** of the following structure is allocated and populated.
*/
typedef struct Fts5Bm25Data Fts5Bm25Data;
struct Fts5Bm25Data {
  int nPhrase;                    /* Number of phrases in query */
  double avgdl;                   /* Average number of tokens in each row */
  double *aIDF;                   /* IDF for each phrase */
  double *aFreq;                  /* Array used to calculate phrase freq. */
};

/*
** Callback used by fts5Bm25GetData() to count the number of rows in the
** table matched by each individual phrase within the query.
*/
static int fts5CountCb(
  const Fts5ExtensionApi *pApi, 
  Fts5Context *pFts,
  void *pUserData                 /* Pointer to sqlite3_int64 variable */
){
  sqlite3_int64 *pn = (sqlite3_int64*)pUserData;
  UNUSED_PARAM2(pApi, pFts);
  (*pn)++;
  return SQLITE_OK;
}

/*
** Set *ppData to point to the Fts5Bm25Data object for the current query. 
** If the object has not already been allocated, allocate and populate it
** now.
*/
static int fts5Bm25GetData(
  const Fts5ExtensionApi *pApi, 
  Fts5Context *pFts,
  Fts5Bm25Data **ppData           /* OUT: bm25-data object for this query */
){
  int rc = SQLITE_OK;             /* Return code */
  Fts5Bm25Data *p;                /* Object to return */

  p = pApi->xGetAuxdata(pFts, 0);
  if( p==0 ){
    int nPhrase;                  /* Number of phrases in query */
    sqlite3_int64 nRow = 0;       /* Number of rows in table */
    sqlite3_int64 nToken = 0;     /* Number of tokens in table */
    int nByte;                    /* Bytes of space to allocate */
    int i;

    /* Allocate the Fts5Bm25Data object */
    nPhrase = pApi->xPhraseCount(pFts);
    nByte = sizeof(Fts5Bm25Data) + nPhrase*2*sizeof(double);
    p = (Fts5Bm25Data*)sqlite3_malloc(nByte);
    if( p==0 ){
      rc = SQLITE_NOMEM;
    }else{
      memset(p, 0, nByte);
      p->nPhrase = nPhrase;
      p->aIDF = (double*)&p[1];
      p->aFreq = &p->aIDF[nPhrase];
    }

    /* Calculate the average document length for this FTS5 table */
    if( rc==SQLITE_OK ) rc = pApi->xRowCount(pFts, &nRow);
    if( rc==SQLITE_OK ) rc = pApi->xColumnTotalSize(pFts, -1, &nToken);
    if( rc==SQLITE_OK ) p->avgdl = (double)nToken  / (double)nRow;

    /* Calculate an IDF for each phrase in the query */
    for(i=0; rc==SQLITE_OK && i<nPhrase; i++){
      sqlite3_int64 nHit = 0;
      rc = pApi->xQueryPhrase(pFts, i, (void*)&nHit, fts5CountCb);
      if( rc==SQLITE_OK ){
        /* Calculate the IDF (Inverse Document Frequency) for phrase i.
        ** This is done using the standard BM25 formula as found on wikipedia:
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
        double idf = log( (nRow - nHit + 0.5) / (nHit + 0.5) );
        if( idf<=0.0 ) idf = 1e-6;
        p->aIDF[i] = idf;
      }
    }

    if( rc!=SQLITE_OK ){
      sqlite3_free(p);
    }else{
      rc = pApi->xSetAuxdata(pFts, p, sqlite3_free);
    }
    if( rc!=SQLITE_OK ) p = 0;
  }
  *ppData = p;
  return rc;
}

/*
** Implementation of bm25() function.
*/
static void fts5Bm25Function(
  const Fts5ExtensionApi *pApi,   /* API offered by current FTS version */
  Fts5Context *pFts,              /* First arg to pass to pApi functions */
  sqlite3_context *pCtx,          /* Context for returning result/error */
  int nVal,                       /* Number of values in apVal[] array */
  sqlite3_value **apVal           /* Array of trailing arguments */
){
  const double k1 = 1.2;          /* Constant "k1" from BM25 formula */
  const double b = 0.75;          /* Constant "b" from BM25 formula */
  int rc = SQLITE_OK;             /* Error code */
  double score = 0.0;             /* SQL function return value */
  Fts5Bm25Data *pData;            /* Values allocated/calculated once only */
  int i;                          /* Iterator variable */
  int nInst = 0;                  /* Value returned by xInstCount() */
  double D = 0.0;                 /* Total number of tokens in row */
  double *aFreq = 0;              /* Array of phrase freq. for current row */

  /* Calculate the phrase frequency (symbol "f(qi,D)" in the documentation)
  ** for each phrase in the query for the current row. */
  rc = fts5Bm25GetData(pApi, pFts, &pData);
  if( rc==SQLITE_OK ){
    aFreq = pData->aFreq;
    memset(aFreq, 0, sizeof(double) * pData->nPhrase);
    rc = pApi->xInstCount(pFts, &nInst);
  }
  for(i=0; rc==SQLITE_OK && i<nInst; i++){
    int ip; int ic; int io;
    rc = pApi->xInst(pFts, i, &ip, &ic, &io);
    if( rc==SQLITE_OK ){
      double w = (nVal > ic) ? sqlite3_value_double(apVal[ic]) : 1.0;
      aFreq[ip] += w;
    }
  }

  /* Figure out the total size of the current row in tokens. */
  if( rc==SQLITE_OK ){
    int nTok;
    rc = pApi->xColumnSize(pFts, -1, &nTok);
    D = (double)nTok;
  }

  /* Determine the BM25 score for the current row. */
  for(i=0; rc==SQLITE_OK && i<pData->nPhrase; i++){
    score += pData->aIDF[i] * (
      ( aFreq[i] * (k1 + 1.0) ) / 
      ( aFreq[i] + k1 * (1 - b + b * D / pData->avgdl) )
    );
  }
  
  /* If no error has occurred, return the calculated score. Otherwise,
  ** throw an SQL exception.  */
  if( rc==SQLITE_OK ){
    sqlite3_result_double(pCtx, -1.0 * score);
  }else{
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
    { "snippet",   0, fts5SnippetFunction, 0 },
    { "highlight", 0, fts5HighlightFunction, 0 },
    { "bm25",      0, fts5Bm25Function,    0 },
  };
  int rc = SQLITE_OK;             /* Return code */
  int i;                          /* To iterate through builtin functions */

  for(i=0; rc==SQLITE_OK && i<ArraySize(aBuiltin); i++){
    rc = pApi->xCreateFunction(pApi,
        aBuiltin[i].zFunc,
        aBuiltin[i].pUserData,
        aBuiltin[i].xFunc,
        aBuiltin[i].xDestroy
    );
  }

  return rc;
}



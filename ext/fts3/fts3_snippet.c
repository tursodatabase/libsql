/*
** 2009 Oct 23
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

#if !defined(SQLITE_CORE) || defined(SQLITE_ENABLE_FTS3)

#include "fts3Int.h"
#include <string.h>
#include <assert.h>
#include <ctype.h>

#define SNIPPET_BUFFER_CHUNK  64
#define SNIPPET_BUFFER_SIZE   SNIPPET_BUFFER_CHUNK*4
#define SNIPPET_BUFFER_MASK   (SNIPPET_BUFFER_SIZE-1)

static void fts3GetDeltaPosition(char **pp, int *piPos){
  int iVal;
  *pp += sqlite3Fts3GetVarint32(*pp, &iVal);
  *piPos += (iVal-2);
}

/*
** Iterate through all phrase nodes in an FTS3 query, except those that
** are part of a sub-tree that is the right-hand-side of a NOT operator.
** For each phrase node found, the supplied callback function is invoked.
**
** If the callback function returns anything other than SQLITE_OK, 
** the iteration is abandoned and the error code returned immediately.
** Otherwise, SQLITE_OK is returned after a callback has been made for
** all eligible phrase nodes.
*/
static int fts3ExprIterate(
  Fts3Expr *pExpr,                /* Expression to iterate phrases of */
  int (*x)(Fts3Expr *, void *),   /* Callback function to invoke for phrases */
  void *pCtx                      /* Second argument to pass to callback */
){
  int rc;
  int eType = pExpr->eType;
  if( eType==FTSQUERY_NOT ){
    rc = SQLITE_OK;
  }else if( eType!=FTSQUERY_PHRASE ){
    assert( pExpr->pLeft && pExpr->pRight );
    rc = fts3ExprIterate(pExpr->pLeft, x, pCtx);
    if( rc==SQLITE_OK ){
      rc = fts3ExprIterate(pExpr->pRight, x, pCtx);
    }
  }else{
    rc = x(pExpr, pCtx);
  }
  return rc;
}

typedef struct LoadDoclistCtx LoadDoclistCtx;
struct LoadDoclistCtx {
  Fts3Table *pTab;                /* FTS3 Table */
  int nPhrase;                    /* Number of phrases so far */
  int nToken;                     /* Number of tokens so far */
};

static int fts3ExprNearTrim(Fts3Expr *pExpr){
  int rc = SQLITE_OK;
  Fts3Expr *pParent = pExpr->pParent;

  assert( pExpr->eType==FTSQUERY_PHRASE );
  while( rc==SQLITE_OK
   && pExpr->aDoclist && pParent 
   && pParent->eType==FTSQUERY_NEAR 
   && pParent->pRight==pExpr 
  ){
    /* This expression (pExpr) is the right-hand-side of a NEAR operator. 
    ** Find the expression to the left of the same operator.
    */
    int nNear = pParent->nNear;
    Fts3Expr *pLeft = pParent->pLeft;

    if( pLeft->eType!=FTSQUERY_PHRASE ){
      assert( pLeft->eType==FTSQUERY_NEAR );
      assert( pLeft->pRight->eType==FTSQUERY_PHRASE );
      pLeft = pLeft->pRight;
    }

    rc = sqlite3Fts3ExprNearTrim(pLeft, pExpr, nNear);

    pExpr = pLeft;
    pParent = pExpr->pParent;
  }

  return rc;
}

static int fts3ExprLoadDoclistsCb1(Fts3Expr *pExpr, void *ctx){
  int rc = SQLITE_OK;
  LoadDoclistCtx *p = (LoadDoclistCtx *)ctx;

  p->nPhrase++;
  p->nToken += pExpr->pPhrase->nToken;

  if( pExpr->isLoaded==0 ){
    rc = sqlite3Fts3ExprLoadDoclist(p->pTab, pExpr);
    pExpr->isLoaded = 1;
    if( rc==SQLITE_OK ){
      fts3ExprNearTrim(pExpr);
    }
  }

  return rc;
}

static int fts3ExprLoadDoclistsCb2(Fts3Expr *pExpr, void *ctx){
  if( pExpr->aDoclist ){
    pExpr->pCurrent = pExpr->aDoclist;
    pExpr->iCurrent = 0;
    pExpr->pCurrent += sqlite3Fts3GetVarint(pExpr->pCurrent, &pExpr->iCurrent);
  }
  return SQLITE_OK;
}

static int fts3ExprLoadDoclists(
  Fts3Cursor *pCsr, 
  int *pnPhrase,                  /* OUT: Number of phrases in query */
  int *pnToken                    /* OUT: Number of tokens in query */
){
  int rc;
  LoadDoclistCtx sCtx = {0, 0, 0};
  sCtx.pTab = (Fts3Table *)pCsr->base.pVtab;
  rc = fts3ExprIterate(pCsr->pExpr, fts3ExprLoadDoclistsCb1, (void *)&sCtx);
  if( rc==SQLITE_OK ){
    (void)fts3ExprIterate(pCsr->pExpr, fts3ExprLoadDoclistsCb2, 0);
  }
  if( pnPhrase ) *pnPhrase = sCtx.nPhrase;
  if( pnToken ) *pnToken = sCtx.nToken;
  return rc;
}

/*
** Each call to this function populates a chunk of a snippet-buffer 
** SNIPPET_BUFFER_CHUNK bytes in size.
**
** Return true if the end of the data has been reached (and all subsequent
** calls to fts3LoadSnippetBuffer() with the same arguments will be no-ops), 
** or false otherwise.
*/
static int fts3LoadSnippetBuffer(
  int iPos,                       /* Document token offset to load data for */
  u8 *aBuffer,                    /* Circular snippet buffer to populate */
  int nList,                      /* Number of position lists in appList */
  char **apList,                  /* IN/OUT: nList position list pointers */
  int *aiPrev                     /* IN/OUT: Previous positions read */
){
  int i;
  int nFin = 0;

  assert( (iPos&(SNIPPET_BUFFER_CHUNK-1))==0 );

  memset(&aBuffer[iPos&SNIPPET_BUFFER_MASK], 0, SNIPPET_BUFFER_CHUNK);

  for(i=0; i<nList; i++){
    int iPrev = aiPrev[i];
    char *pList = apList[i];

    if( iPrev<0 || !pList ){
      nFin++;
      continue;
    }

    while( iPrev<(iPos+SNIPPET_BUFFER_CHUNK) ){
      assert( iPrev>=iPos );
      aBuffer[iPrev&SNIPPET_BUFFER_MASK] = i+1;
      if( 0==((*pList)&0xFE) ){
        iPrev = -1;
        break;
      }else{
        fts3GetDeltaPosition(&pList, &iPrev); 
      }
    }

    aiPrev[i] = iPrev;
    apList[i] = pList;
  }

  return (nFin==nList);
}

typedef struct SnippetCtx SnippetCtx;
struct SnippetCtx {
  Fts3Cursor *pCsr;
  int iCol;
  int iPhrase;
  int *aiPrev;
  int *anToken;
  char **apList;
};

static int fts3SnippetFindPositions(Fts3Expr *pExpr, void *ctx){
  SnippetCtx *p = (SnippetCtx *)ctx;
  int iPhrase = p->iPhrase++;
  char *pCsr;

  p->anToken[iPhrase] = pExpr->pPhrase->nToken;
  pCsr = sqlite3Fts3FindPositions(pExpr, p->pCsr->iPrevId, p->iCol);

  if( pCsr ){
    int iVal;
    pCsr += sqlite3Fts3GetVarint32(pCsr, &iVal);
    p->apList[iPhrase] = pCsr;
    p->aiPrev[iPhrase] = iVal-2;
  }
  return SQLITE_OK;
}

static void fts3SnippetCnt(
  int iIdx, 
  int nSnippet, 
  int *anCnt, 
  u8 *aBuffer,
  int *anToken,
  u64 *pHlmask
){
  int iSub =  (iIdx-1)&SNIPPET_BUFFER_MASK;
  int iAdd =  (iIdx+nSnippet-1)&SNIPPET_BUFFER_MASK;

  u64 h = *pHlmask;

  anCnt[ aBuffer[iSub]  ]--;
  anCnt[ aBuffer[iAdd]  ]++;

  h = h >> 1;
  if( aBuffer[iAdd] ){
    int j;
    for(j=anToken[aBuffer[iAdd]-1]; j>=1; j--){
      h |= (u64)1 << (nSnippet-j);
    }
  }
  *pHlmask = h;
}

static int fts3SnippetScore(int n, int *anCnt, u64 covmask){
  int j;
  int iScore = 0;
  for(j=1; j<=n; j++){
    int nCnt = anCnt[j];
    iScore += nCnt;
    if( nCnt && 0==(covmask & ((u64)1 << (j-1))) ){
      iScore += 1000;
    }
  }
  return iScore;
}

static u64 fts3SnippetMask(int n, int *anCnt){
  int j;
  u64 mask = 0;

  if( n>64 ) n = 64;
  for(j=1; j<=n; j++){
    if( anCnt[j] ) mask |= ((u64)1)<<(j-1);
  }
  return mask;
}

typedef struct SnippetFragment SnippetFragment;
struct SnippetFragment {
  int iCol;                       /* Column snippet is extracted from */
  int iPos;                       /* Index of first token in snippet */
  u64 covered;                    /* Mask of query phrases covered */
  u64 hlmask;                     /* Mask of snippet terms to highlight */
};

static int fts3BestSnippet(
  int nSnippet,                   /* Desired snippet length */
  Fts3Cursor *pCsr,               /* Cursor to create snippet for */
  int iCol,                       /* Index of column to create snippet from */
  u64 mCovered,                   /* Mask of phrases already covered */
  u64 *pmSeen,                    /* IN/OUT: Mask of phrases seen */
  SnippetFragment *pFragment,     /* OUT: Best snippet found */
  int *piScore                    /* OUT: Score of snippet pFragment */
){
  int rc;                         /* Return Code */
  u8 aBuffer[SNIPPET_BUFFER_SIZE];/* Circular snippet buffer */
  int *aiPrev;                    /* Used by fts3LoadSnippetBuffer() */
  int *anToken;                   /* Number of tokens in each phrase */
  char **apList;                  /* Array of position lists */
  int *anCnt;                     /* Running totals of phrase occurences */
  int nList;                      /* Number of phrases in expression */
  int nByte;                      /* Bytes of dynamic space required */
  int i;                          /* Loop counter */
  u64 hlmask = 0;                 /* Current mask of highlighted terms */
  u64 besthlmask = 0;             /* Mask of highlighted terms for iBestPos */
  u64 bestcovmask = 0;            /* Mask of terms with at least one hit */
  int iBestPos = 0;               /* Starting position of 'best' snippet */
  int iBestScore = 0;             /* Score of best snippet higher->better */
  int iEnd = 0x7FFFFFFF;
  SnippetCtx sCtx;

  /* Iterate through the phrases in the expression to count them. The same
  ** callback makes sure the doclists are loaded for each phrase.
  */
  rc = fts3ExprLoadDoclists(pCsr, &nList, 0);
  if( rc!=SQLITE_OK ){
    return rc;
  }

  /* Now that it is known how many phrases there are, allocate and zero
  ** the required arrays using malloc().
  */
  nByte = sizeof(u8*)*nList +     /* apList */
      sizeof(int)*(nList) +       /* anToken */
      sizeof(int)*nList +         /* aiPrev */
      sizeof(int)*(nList+1);      /* anCnt */
  apList = (char **)sqlite3_malloc(nByte);
  if( !apList ){
    return SQLITE_NOMEM;
  }
  memset(apList, 0, nByte);
  anToken = (int *)&apList[nList];
  aiPrev = &anToken[nList];
  anCnt = &aiPrev[nList];

  /* Initialize the contents of the aiPrev and aiList arrays. */
  sCtx.pCsr = pCsr;
  sCtx.iCol = iCol;
  sCtx.apList = apList;
  sCtx.aiPrev = aiPrev;
  sCtx.anToken = anToken;
  sCtx.iPhrase = 0;
  (void)fts3ExprIterate(pCsr->pExpr, fts3SnippetFindPositions, (void *)&sCtx);

  for(i=0; i<nList; i++){
    if( apList[i] && aiPrev[i]>=0 ){
      *pmSeen |= (u64)1 << i;
    }
  }

  /* Load the first two chunks of data into the buffer. */
  memset(aBuffer, 0, SNIPPET_BUFFER_SIZE);
  fts3LoadSnippetBuffer(0, aBuffer, nList, apList, aiPrev);
  fts3LoadSnippetBuffer(SNIPPET_BUFFER_CHUNK, aBuffer, nList, apList, aiPrev);

  /* Set the initial contents of the highlight-mask and anCnt[] array. */
  for(i=1-nSnippet; i<=0; i++){
    fts3SnippetCnt(i, nSnippet, anCnt, aBuffer, anToken, &hlmask);
  }
  iBestScore = fts3SnippetScore(nList, anCnt, mCovered);
  besthlmask = hlmask;
  iBestPos = 0;
  bestcovmask = fts3SnippetMask(nList, anCnt);

  for(i=1; i<iEnd; i++){
    int iScore;

    if( 0==(i&(SNIPPET_BUFFER_CHUNK-1)) ){
      int iLoad = i + SNIPPET_BUFFER_CHUNK;
      if( fts3LoadSnippetBuffer(iLoad, aBuffer, nList, apList, aiPrev) ){
        iEnd = iLoad;
      }
    }

    /* Figure out how highly a snippet starting at token offset i scores
    ** according to fts3SnippetScore(). If it is higher than any previously
    ** considered position, save the current position, score and hlmask as 
    ** the best snippet candidate found so far.
    */
    fts3SnippetCnt(i, nSnippet, anCnt, aBuffer, anToken, &hlmask);
    iScore = fts3SnippetScore(nList, anCnt, mCovered);
    if( iScore>iBestScore ){
      iBestPos = i;
      iBestScore = iScore;
      besthlmask = hlmask;
      bestcovmask = fts3SnippetMask(nList, anCnt);
    }
  }

  sqlite3_free(apList);

  pFragment->iPos = iBestPos;
  pFragment->hlmask = besthlmask;
  pFragment->iCol = iCol;
  pFragment->covered = bestcovmask;
  *piScore = iBestScore;
  return SQLITE_OK;
}

typedef struct StrBuffer StrBuffer;
struct StrBuffer {
  char *z;
  int n;
  int nAlloc;
};

static int fts3StringAppend(
  StrBuffer *pStr, 
  const char *zAppend, 
  int nAppend
){
  if( nAppend<0 ){
    nAppend = strlen(zAppend);
  }

  if( pStr->n+nAppend+1>=pStr->nAlloc ){
    int nAlloc = pStr->nAlloc+nAppend+100;
    char *zNew = sqlite3_realloc(pStr->z, nAlloc);
    if( !zNew ){
      return SQLITE_NOMEM;
    }
    pStr->z = zNew;
    pStr->nAlloc = nAlloc;
  }

  memcpy(&pStr->z[pStr->n], zAppend, nAppend);
  pStr->n += nAppend;
  pStr->z[pStr->n] = '\0';

  return SQLITE_OK;
}

int fts3SnippetShift(
  Fts3Table *pTab, 
  int nSnippet,
  const char *zDoc,
  int nDoc,
  int *piPos,
  u64 *pHlmask
){
  u64 hlmask = *pHlmask;

  if( hlmask ){
    int nLeft;
    int nRight;
    int nDesired;

    for(nLeft=0; !(hlmask & ((u64)1 << nLeft)); nLeft++);
    for(nRight=0; !(hlmask & ((u64)1 << (nSnippet-1-nRight))); nRight++);

    nDesired = (nLeft-nRight)/2;
    if( nDesired>0 ){
      int nShift;
      int iCurrent = 0;
      int rc;
      sqlite3_tokenizer_module *pMod;
      sqlite3_tokenizer_cursor *pC;

      pMod = (sqlite3_tokenizer_module *)pTab->pTokenizer->pModule;
      rc = pMod->xOpen(pTab->pTokenizer, zDoc, nDoc, &pC);
      pC->pTokenizer = pTab->pTokenizer;
      while( rc==SQLITE_OK && iCurrent<(nSnippet+nDesired) ){
        const char *ZDUMMY; int DUMMY1, DUMMY2, DUMMY3;
        rc = pMod->xNext(pC, &ZDUMMY, &DUMMY1, &DUMMY2, &DUMMY3, &iCurrent);
      }
      pMod->xClose(pC);
      if( rc!=SQLITE_OK && rc!=SQLITE_DONE ){
        return rc;
      }
      nShift = iCurrent-nSnippet;
      if( nShift>0 ){
        *piPos += nShift;
        *pHlmask = hlmask >> nShift;
      }
    }
  }
  return SQLITE_OK;
}

static int fts3SnippetText(
  Fts3Cursor *pCsr,               /* FTS3 Cursor */
  SnippetFragment *pFragment,     /* Snippet to extract */
  int nSnippet,                   /* Number of tokens in extracted snippet */
  const char *zOpen,              /* String inserted before highlighted term */
  const char *zClose,             /* String inserted after highlighted term */
  const char *zEllipsis,
  StrBuffer *pOut
){
  Fts3Table *pTab = (Fts3Table *)pCsr->base.pVtab;
  int rc;                         /* Return code */
  const char *zDoc;               /* Document text to extract snippet from */
  int nDoc;                       /* Size of zDoc in bytes */
  int iCurrent = 0;               /* Current token number of document */
  int iStart = 0;                 /* Byte offset of current token */
  int iEnd = 0;                   /* Byte offset of end of current token */
  int isShiftDone = 0;
  int iPos = pFragment->iPos;
  u64 hlmask = pFragment->hlmask;

  sqlite3_tokenizer_module *pMod; /* Tokenizer module methods object */
  sqlite3_tokenizer_cursor *pC;   /* Tokenizer cursor open on zDoc/nDoc */
  const char *ZDUMMY;             /* Dummy arguments used with tokenizer */
  int DUMMY1, DUMMY2, DUMMY3;     /* Dummy arguments used with tokenizer */
  
  zDoc = (const char *)sqlite3_column_text(pCsr->pStmt, pFragment->iCol+1);
  if( zDoc==0 ){
    if( sqlite3_column_type(pCsr->pStmt, pFragment->iCol+1)!=SQLITE_NULL ){
      return SQLITE_NOMEM;
    }
    return SQLITE_OK;
  }
  nDoc = sqlite3_column_bytes(pCsr->pStmt, pFragment->iCol+1);

  /* Open a token cursor on the document. Read all tokens up to and 
  ** including token iPos (the first token of the snippet). Set variable
  ** iStart to the byte offset in zDoc of the start of token iPos.
  */
  pMod = (sqlite3_tokenizer_module *)pTab->pTokenizer->pModule;
  rc = pMod->xOpen(pTab->pTokenizer, zDoc, nDoc, &pC);
  if( rc!=SQLITE_OK ){
    return rc;
  }
  pC->pTokenizer = pTab->pTokenizer;

  while( rc==SQLITE_OK ){
    int iBegin;
    int iFin;
    rc = pMod->xNext(pC, &ZDUMMY, &DUMMY1, &iBegin, &iFin, &iCurrent);

    if( rc==SQLITE_OK ){
      if( iCurrent<iPos ) continue;

      if( !isShiftDone ){
        int n = nDoc - iBegin;
        rc = fts3SnippetShift(pTab, nSnippet, &zDoc[iBegin], n, &iPos, &hlmask);
        if( rc!=SQLITE_OK || iCurrent<iPos ) continue;
      }
      if( iCurrent==iPos ){
        iStart = iEnd = iBegin;
      }

      if( iCurrent>=(iPos+nSnippet) ){
        rc = SQLITE_DONE;
      }else{
        iEnd = iFin;
        if( hlmask & ((u64)1 << (iCurrent-iPos)) ){
          if( fts3StringAppend(pOut, &zDoc[iStart], iBegin-iStart)
           || fts3StringAppend(pOut, zOpen, -1)
           || fts3StringAppend(pOut, &zDoc[iBegin], iEnd-iBegin)
           || fts3StringAppend(pOut, zClose, -1)
          ){
            rc = SQLITE_NOMEM;
          }
          iStart = iEnd;
        }
      }
    }
  }
  assert( rc!=SQLITE_OK );
  if( rc==SQLITE_DONE ){
    rc = fts3StringAppend(pOut, &zDoc[iStart], iEnd-iStart);
    if( rc==SQLITE_OK ){
      rc = pMod->xNext(pC, &ZDUMMY, &DUMMY1, &DUMMY2, &DUMMY3, &iCurrent);
      if( rc==SQLITE_DONE ){
        rc = fts3StringAppend(pOut, &zDoc[iEnd], -1);
      }else if( rc==SQLITE_OK && zEllipsis ){
        rc = fts3StringAppend(pOut, zEllipsis, -1);
      }
    }
  }

  pMod->xClose(pC);
  return rc;
}


/*
** An instance of this structure is used to collect the 'global' part of
** the matchinfo statistics. The 'global' part consists of the following:
**
**   1. The number of phrases in the query (nPhrase).
**
**   2. The number of columns in the FTS3 table (nCol).
**
**   3. A matrix of (nPhrase*nCol) integers containing the sum of the
**      number of hits for each phrase in each column across all rows
**      of the table.
**
** The total size of the global matchinfo array, assuming the number of
** columns is N and the number of phrases is P is:
**
**   2 + P*(N+1)
**
** The number of hits for the 3rd phrase in the second column is found
** using the expression:
**
**   aGlobal[2 + P*(1+2) + 1]
*/
typedef struct MatchInfo MatchInfo;
struct MatchInfo {
  Fts3Table *pTab;                /* FTS3 Table */
  Fts3Cursor *pCursor;            /* FTS3 Cursor */
  int iPhrase;                    /* Number of phrases so far */
  int nCol;                       /* Number of columns in table */
  u32 *aGlobal;                   /* Pre-allocated buffer */
};

/*
** This function is used to count the entries in a column-list (delta-encoded
** list of term offsets within a single column of a single row).
*/
static int fts3ColumnlistCount(char **ppCollist){
  char *pEnd = *ppCollist;
  char c = 0;
  int nEntry = 0;

  /* A column-list is terminated by either a 0x01 or 0x00. */
  while( 0xFE & (*pEnd | c) ){
    c = *pEnd++ & 0x80;
    if( !c ) nEntry++;
  }

  *ppCollist = pEnd;
  return nEntry;
}

static void fts3LoadColumnlistCounts(char **pp, u32 *aOut){
  char *pCsr = *pp;
  while( *pCsr ){
    sqlite3_int64 iCol = 0;
    if( *pCsr==0x01 ){
      pCsr++;
      pCsr += sqlite3Fts3GetVarint(pCsr, &iCol);
    }
    aOut[iCol] += fts3ColumnlistCount(&pCsr);
  }
  pCsr++;
  *pp = pCsr;
}

/*
** fts3ExprIterate() callback used to collect the "global" matchinfo stats
** for a single query.
*/
static int fts3ExprGlobalMatchinfoCb(
  Fts3Expr *pExpr,                /* Phrase expression node */
  void *pCtx                      /* Pointer to MatchInfo structure */
){
  MatchInfo *p = (MatchInfo *)pCtx;
  char *pCsr;
  char *pEnd;
  const int iStart = 2 + p->nCol*p->iPhrase;

  assert( pExpr->isLoaded );

  /* Fill in the global hit count matrix row for this phrase. */
  pCsr = pExpr->aDoclist;
  pEnd = &pExpr->aDoclist[pExpr->nDoclist];
  while( pCsr<pEnd ){
    while( *pCsr++ & 0x80 );
    fts3LoadColumnlistCounts(&pCsr, &p->aGlobal[iStart]);
  }

  p->iPhrase++;
  return SQLITE_OK;
}

static int fts3ExprLocalMatchinfoCb(
  Fts3Expr *pExpr,                /* Phrase expression node */
  void *pCtx                      /* Pointer to MatchInfo structure */
){
  MatchInfo *p = (MatchInfo *)pCtx;
  int iPhrase = p->iPhrase++;

  if( pExpr->aDoclist ){
    char *pCsr;
    int iOffset = 2 + p->nCol*(p->aGlobal[0]+iPhrase);

    memset(&p->aGlobal[iOffset], 0, p->nCol*sizeof(u32));
    pCsr = sqlite3Fts3FindPositions(pExpr, p->pCursor->iPrevId, -1);
    if( pCsr ) fts3LoadColumnlistCounts(&pCsr, &p->aGlobal[iOffset]);
  }

  return SQLITE_OK;
}

/*
** Populate pCsr->aMatchinfo[] with data for the current row. The 'matchinfo'
** data is an array of 32-bit unsigned integers (C type u32).
*/
static int fts3GetMatchinfo(Fts3Cursor *pCsr){
  MatchInfo g;
  Fts3Table *pTab = (Fts3Table *)pCsr->base.pVtab;
  if( pCsr->aMatchinfo==0 ){
    int rc;
    int nPhrase;
    int nMatchinfo;

    g.pTab = pTab;
    g.nCol = pTab->nColumn;
    g.iPhrase = 0;
    rc = fts3ExprLoadDoclists(pCsr, &nPhrase, 0);
    if( rc!=SQLITE_OK ){
      return rc;
    }

    nMatchinfo = 2 + 2*g.nCol*nPhrase;

    g.iPhrase = 0;
    g.aGlobal = (u32 *)sqlite3_malloc(sizeof(u32)*nMatchinfo);
    if( !g.aGlobal ){ 
      return SQLITE_NOMEM;
    }
    memset(g.aGlobal, 0, sizeof(u32)*nMatchinfo);

    g.aGlobal[0] = nPhrase;
    g.aGlobal[1] = g.nCol;
    (void)fts3ExprIterate(pCsr->pExpr, fts3ExprGlobalMatchinfoCb, (void *)&g);

    pCsr->aMatchinfo = g.aGlobal;
  }

  g.pTab = pTab;
  g.pCursor = pCsr;
  g.nCol = pTab->nColumn;
  g.iPhrase = 0;
  g.aGlobal = pCsr->aMatchinfo;

  if( pCsr->isMatchinfoOk ){
    (void)fts3ExprIterate(pCsr->pExpr, fts3ExprLocalMatchinfoCb, (void *)&g);
    pCsr->isMatchinfoOk = 0;
  }

  return SQLITE_OK;
}

void sqlite3Fts3Snippet(
  sqlite3_context *pCtx,          /* SQLite function call context */
  Fts3Cursor *pCsr,               /* Cursor object */
  const char *zStart,             /* Snippet start text - "<b>" */
  const char *zEnd,               /* Snippet end text - "</b>" */
  const char *zEllipsis,          /* Snippet ellipsis text - "<b>...</b>" */
  int iCol,                       /* Extract snippet from this column */
  int nToken                      /* Approximate number of tokens in snippet */
){
  Fts3Table *pTab = (Fts3Table *)pCsr->base.pVtab;
  int rc = SQLITE_OK;
  int i;
  StrBuffer res = {0, 0, 0};

  /* The returned text includes up to four fragments of text extracted from
  ** the data in the current row. The first iteration of the for(...) loop
  ** below attempts to locate a single fragment of text nToken tokens in 
  ** size that contains at least one instance of all phrases in the query
  ** expression that appear in the current row. If such a fragment of text
  ** cannot be found, the second iteration of the loop attempts to locate
  ** a pair of fragments, and so on.
  */
  int nSnippet = 0;               /* Number of fragments in this snippet */
  SnippetFragment aSnippet[4];    /* Maximum of 4 fragments per snippet */
  int nFToken = -1;               /* Number of tokens in each fragment */

  do {
    int iSnip;                    /* Loop counter 0..nSnippet-1 */
    u64 mCovered = 0;             /* Bitmask of phrases covered by snippet */
    u64 mSeen = 0;                /* Bitmask of phrases seen by BestSnippet() */

    nSnippet++;
    nFToken = (nToken+nSnippet-1) / nSnippet;

    for(iSnip=0; iSnip<nSnippet; iSnip++){
      int iBestScore = -1;        /* Best score of columns checked so far */
      int iRead;                  /* Used to iterate through columns */
      SnippetFragment *pFragment = &aSnippet[iSnip];

      memset(pFragment, 0, sizeof(*pFragment));

      /* Loop through all columns of the table being considered for snippets.
      ** If the iCol argument to this function was negative, this means all
      ** columns of the FTS3 table. Otherwise, only column iCol is considered.
      */
      for(iRead=0; iRead<pTab->nColumn; iRead++){
        SnippetFragment sF;
        int iS;
        if( iCol>=0 && iRead!=iCol ) continue;

        /* Find the best snippet of nFToken tokens in column iRead. */
        rc = fts3BestSnippet(nFToken, pCsr, iRead, mCovered, &mSeen, &sF, &iS);
        if( rc!=SQLITE_OK ){
          goto snippet_out;
        }
        if( iS>iBestScore ){
          *pFragment = sF;
          iBestScore = iS;
        }
      }

      mCovered |= pFragment->covered;
    }

    /* If all query phrases seen by fts3BestSnippet() are present in at least
    ** one of the nSnippet snippet fragments, break out of the loop.
    */
    assert( (mCovered&mSeen)==mCovered );
    if( mSeen==mCovered ) break;
  }while( nSnippet<SizeofArray(aSnippet) );

  assert( nFToken>0 );

  for(i=0; i<nSnippet && rc==SQLITE_OK; i++){
    SnippetFragment *p = &aSnippet[i];
    const char *zTail = ((i==nSnippet-1) ? zEllipsis : 0);

    if( i>0 || p->iPos>0 ){
      fts3StringAppend(&res, zEllipsis, -1);
    }
    rc = fts3SnippetText(pCsr, p, nFToken, zStart, zEnd, zTail, &res);
  }

 snippet_out:
  if( rc!=SQLITE_OK ){
    sqlite3_result_error_code(pCtx, rc);
    sqlite3_free(res.z);
  }else{
    sqlite3_result_text(pCtx, res.z, -1, sqlite3_free);
  }
}


typedef struct TermOffset TermOffset;
struct TermOffset {
  char *pList;                    /* Position-list */
  int iPos;                       /* Position just read from pList */
  int iOff;
};
typedef struct TermOffsetCtx TermOffsetCtx;

struct TermOffsetCtx {
  int iCol;                       /* Column of table to populate aTerm for */
  int iTerm;
  sqlite3_int64 iDocid;
  TermOffset *aTerm;
};

/*
** This function is an fts3ExprIterate() callback used by sqlite3Fts3Offsets().
*/
static int fts3ExprTermOffsetInit(Fts3Expr *pExpr, void *ctx){
  TermOffsetCtx *p = (TermOffsetCtx *)ctx;
  int nTerm;                      /* Number of tokens in phrase */
  int iTerm;                      /* For looping through nTerm phrase terms */
  char *pList;                    /* Pointer to position list for phrase */
  int iPos = 0;                   /* First position in position-list */

  pList = sqlite3Fts3FindPositions(pExpr, p->iDocid, p->iCol);
  nTerm = pExpr->pPhrase->nToken;
  if( pList ){
    fts3GetDeltaPosition(&pList, &iPos);
    assert( iPos>=0 );
  }

  for(iTerm=0; iTerm<nTerm; iTerm++){
    TermOffset *pT = &p->aTerm[p->iTerm++];
    pT->iOff = nTerm-iTerm-1;
    pT->pList = pList;
    pT->iPos = iPos;
  }

  return SQLITE_OK;
}

/*
** Implementation of offsets() function.
*/
void sqlite3Fts3Offsets(
  sqlite3_context *pCtx,          /* SQLite function call context */
  Fts3Cursor *pCsr                /* Cursor object */
){
  Fts3Table *pTab = (Fts3Table *)pCsr->base.pVtab;
  sqlite3_tokenizer_module const *pMod = pTab->pTokenizer->pModule;
  const char *ZDUMMY;
  int NDUMMY;

  int rc;                         /* Return Code */
  int nToken;                     /* Number of tokens in query */
  int iCol;                       /* Column currently being processed */
  StrBuffer res = {0, 0, 0};      /* Result string */

  TermOffsetCtx sCtx;
  memset(&sCtx, 0, sizeof(sCtx));

  assert( pCsr->isRequireSeek==0 );

  /* Count the number of terms in the query */
  rc = fts3ExprLoadDoclists(pCsr, 0, &nToken);
  if( rc!=SQLITE_OK ) goto offsets_out;

  /* Allocate the array of TermOffset iterators. */
  sCtx.aTerm = (TermOffset *)sqlite3_malloc(sizeof(TermOffset)*nToken);
  if( 0==sCtx.aTerm ){
    rc = SQLITE_NOMEM;
    goto offsets_out;
  }
  sCtx.iDocid = pCsr->iPrevId;

  for(iCol=0; iCol<pTab->nColumn; iCol++){
    sqlite3_tokenizer_cursor *pC; /* Tokenizer cursor */
    int iStart;
    int iEnd;
    int iCurrent;
    const char *zDoc;
    int nDoc;

    /* Initialize the contents of sCtx.aTerm[] for column iCol. */
    sCtx.iCol = iCol;
    sCtx.iTerm = 0;
    rc = fts3ExprIterate(pCsr->pExpr, fts3ExprTermOffsetInit, (void *)&sCtx);
    if( rc!=SQLITE_OK ) goto offsets_out;

    /* Initialize a tokenizer iterator to iterate through column iCol. */
    zDoc = (const char *)sqlite3_column_text(pCsr->pStmt, iCol+1);
    nDoc = sqlite3_column_bytes(pCsr->pStmt, iCol+1);
    rc = pMod->xOpen(pTab->pTokenizer, zDoc, nDoc, &pC);
    if( rc!=SQLITE_OK ) goto offsets_out;
    pC->pTokenizer = pTab->pTokenizer;

    rc = pMod->xNext(pC, &ZDUMMY, &NDUMMY, &iStart, &iEnd, &iCurrent);
    while( rc==SQLITE_OK ){
      int i;                      /* Used to loop through terms */
      int iMinPos = 0x7FFFFFFF;   /* Position of next token */
      TermOffset *pTerm = 0;      /* TermOffset associated with next token */

      for(i=0; i<nToken; i++){
        TermOffset *pT = &sCtx.aTerm[i];
        if( pT->pList && (pT->iPos-pT->iOff)<iMinPos ){
          iMinPos = pT->iPos-pT->iOff;
          pTerm = pT;
        }
      }

      if( !pTerm ){
        /* All offsets for this column have been gathered. */
        break;
      }else{
        assert( iCurrent<=iMinPos );
        if( 0==(0xFE&*pTerm->pList) ){
          pTerm->pList = 0;
        }else{
          fts3GetDeltaPosition(&pTerm->pList, &pTerm->iPos);
        }
        while( rc==SQLITE_OK && iCurrent<iMinPos ){
          rc = pMod->xNext(pC, &ZDUMMY, &NDUMMY, &iStart, &iEnd, &iCurrent);
        }
        if( rc==SQLITE_OK ){
          char aBuffer[64];
          sqlite3_snprintf(sizeof(aBuffer), aBuffer, 
              "%d %d %d %d ", iCol, pTerm-sCtx.aTerm, iStart, iEnd-iStart
          );
          fts3StringAppend(&res, aBuffer, -1);
        }
      }
    }
    if( rc==SQLITE_DONE ){
      rc = SQLITE_ERROR;
    }

    pMod->xClose(pC);
    if( rc!=SQLITE_OK ) goto offsets_out;
  }

 offsets_out:
  sqlite3_free(sCtx.aTerm);
  assert( rc!=SQLITE_DONE );
  if( rc!=SQLITE_OK ){
    sqlite3_result_error_code(pCtx,  rc);
    sqlite3_free(res.z);
  }else{
    sqlite3_result_text(pCtx, res.z, res.n-1, sqlite3_free);
  }
  return;
}

void sqlite3Fts3Matchinfo(sqlite3_context *pContext, Fts3Cursor *pCsr){
  int rc = fts3GetMatchinfo(pCsr);
  if( rc!=SQLITE_OK ){
    sqlite3_result_error_code(pContext, rc);
  }else{
    int n = sizeof(u32)*(2+pCsr->aMatchinfo[0]*pCsr->aMatchinfo[1]*2);
    sqlite3_result_blob(pContext, pCsr->aMatchinfo, n, SQLITE_TRANSIENT);
  }
}

#endif

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

static int fts3ExprIterate2(
  Fts3Expr *pExpr,                /* Expression to iterate phrases of */
  int *piPhrase,                  /* Pointer to phrase counter */
  int (*x)(Fts3Expr*,int,void*),  /* Callback function to invoke for phrases */
  void *pCtx                      /* Second argument to pass to callback */
){
  int rc;
  int eType = pExpr->eType;
  if( eType!=FTSQUERY_PHRASE ){
    assert( pExpr->pLeft && pExpr->pRight );
    rc = fts3ExprIterate2(pExpr->pLeft, piPhrase, x, pCtx);
    if( rc==SQLITE_OK && eType!=FTSQUERY_NOT ){
      rc = fts3ExprIterate2(pExpr->pRight, piPhrase, x, pCtx);
    }
  }else{
    rc = x(pExpr, *piPhrase, pCtx);
    (*piPhrase)++;
  }
  return rc;
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
  int (*x)(Fts3Expr*,int,void*),  /* Callback function to invoke for phrases */
  void *pCtx                      /* Second argument to pass to callback */
){
  int iPhrase = 0;
  return fts3ExprIterate2(pExpr, &iPhrase, x, pCtx);
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

static int fts3ExprLoadDoclistsCb1(Fts3Expr *pExpr, int iPhrase, void *ctx){
  int rc = SQLITE_OK;
  LoadDoclistCtx *p = (LoadDoclistCtx *)ctx;

  p->nPhrase++;
  p->nToken += pExpr->pPhrase->nToken;

  if( pExpr->isLoaded==0 ){
    rc = sqlite3Fts3ExprLoadDoclist(p->pTab, pExpr);
    pExpr->isLoaded = 1;
    if( rc==SQLITE_OK ){
      rc = fts3ExprNearTrim(pExpr);
    }
  }

  return rc;
}

static int fts3ExprLoadDoclistsCb2(Fts3Expr *pExpr, int iPhrase, void *ctx){
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
** The following types are used as part of the implementation of the 
** fts3BestSnippet() routine.
*/
typedef struct SnippetCtx SnippetCtx;
typedef struct SnippetPhrase SnippetPhrase;

struct SnippetCtx {
  Fts3Cursor *pCsr;               /* Cursor snippet is being generated from */
  int iCol;                       /* Extract snippet from this column */
  int nSnippet;                   /* Requested snippet length (in tokens) */
  int nPhrase;                    /* Number of phrases in query */
  SnippetPhrase *aPhrase;         /* Array of size nPhrase */
  int iCurrent;                   /* First token of current snippet */
};
struct SnippetPhrase {
  int nToken;                     /* Number of tokens in phrase */
  char *pList;                    /* Pointer to start of phrase position list */
  int iHead;                      /* Next value in position list */
  char *pHead;                    /* Position list data following iHead */
  int iTail;                      /* Next value in trailing position list */
  char *pTail;                    /* Position list data following iTail */
};

/*
** Advance the position list iterator specified by the first two 
** arguments so that it points to the first element with a value greater
** than or equal to parameter iNext.
*/
static void fts3SnippetAdvance(char **ppIter, int *piIter, int iNext){
  char *pIter = *ppIter;
  if( pIter ){
    int iIter = *piIter;

    while( iIter<iNext ){
      if( 0==(*pIter & 0xFE) ){
        iIter = -1;
        pIter = 0;
        break;
      }
      fts3GetDeltaPosition(&pIter, &iIter);
    }

    *piIter = iIter;
    *ppIter = pIter;
  }
}

static int fts3SnippetNextCandidate(SnippetCtx *pIter){
  int i;                          /* Loop counter */

  if( pIter->iCurrent<0 ){
    /* The SnippetCtx object has just been initialized. The first snippet
    ** candidate always starts at offset 0 (even if this candidate has a
    ** score of 0.0).
    */
    pIter->iCurrent = 0;

    /* Advance the 'head' iterator of each phrase to the first offset that
    ** is greater than or equal to (iNext+nSnippet).
    */
    for(i=0; i<pIter->nPhrase; i++){
      SnippetPhrase *pPhrase = &pIter->aPhrase[i];
      fts3SnippetAdvance(&pPhrase->pHead, &pPhrase->iHead, pIter->nSnippet);
    }
  }else{
    int iStart;
    int iEnd = 0x7FFFFFFF;

    for(i=0; i<pIter->nPhrase; i++){
      SnippetPhrase *pPhrase = &pIter->aPhrase[i];
      if( pPhrase->pHead && pPhrase->iHead<iEnd ){
        iEnd = pPhrase->iHead;
      }
    }
    if( iEnd==0x7FFFFFFF ){
      return 1;
    }

    pIter->iCurrent = iStart = iEnd - pIter->nSnippet + 1;
    for(i=0; i<pIter->nPhrase; i++){
      SnippetPhrase *pPhrase = &pIter->aPhrase[i];
      fts3SnippetAdvance(&pPhrase->pHead, &pPhrase->iHead, iEnd+1);
      fts3SnippetAdvance(&pPhrase->pTail, &pPhrase->iTail, iStart);
    }
  }

  return 0;
}

static void fts3SnippetDetails(
  SnippetCtx *pIter,              /* Snippet iterator */
  u64 mCovered,                   /* Bitmask of phrases already covered */
  int *piToken,                   /* OUT: First token of proposed snippet */
  int *piScore,                   /* OUT: "Score" for this snippet */
  u64 *pmCover,                   /* OUT: Bitmask of phrases covered */
  u64 *pmHighlight                /* OUT: Bitmask of terms to highlight */
){
  int iStart = pIter->iCurrent;   /* First token of snippet */

  int iScore = 0;
  int i;
  u64 mCover = 0;
  u64 mHighlight = 0;

  for(i=0; i<pIter->nPhrase; i++){
    SnippetPhrase *pPhrase = &pIter->aPhrase[i];
    if( pPhrase->pTail ){
      char *pCsr = pPhrase->pTail;
      int iCsr = pPhrase->iTail;

      while( iCsr<(iStart+pIter->nSnippet) ){
        int j;
        u64 mPhrase = (u64)1 << i;
        u64 mPos = (u64)1 << (iCsr - iStart);
        assert( iCsr>=iStart );
        if( (mCover|mCovered)&mPhrase ){
          iScore++;
        }else{
          iScore += 1000;
        }
        mCover |= mPhrase;

        for(j=0; j<pPhrase->nToken; j++){
          mHighlight |= (mPos>>j);
        }

        if( 0==(*pCsr & 0x0FE) ) break;
        fts3GetDeltaPosition(&pCsr, &iCsr);
      }
    }
  }

  *piToken = iStart;
  *piScore = iScore;
  *pmCover = mCover;
  *pmHighlight = mHighlight;
}

/*
** This function is an fts3ExprIterate() callback used by fts3BestSnippet().
** Each invocation populates an element of the SnippetCtx.aPhrase[] array.
*/
static int fts3SnippetFindPositions(Fts3Expr *pExpr, int iPhrase, void *ctx){
  SnippetCtx *p = (SnippetCtx *)ctx;
  SnippetPhrase *pPhrase = &p->aPhrase[iPhrase];
  char *pCsr;

  pPhrase->nToken = pExpr->pPhrase->nToken;

  pCsr = sqlite3Fts3FindPositions(pExpr, p->pCsr->iPrevId, p->iCol);
  if( pCsr ){
    int iFirst = 0;
    pPhrase->pList = pCsr;
    fts3GetDeltaPosition(&pCsr, &iFirst);
    pPhrase->pHead = pCsr;
    pPhrase->pTail = pCsr;
    pPhrase->iHead = iFirst;
    pPhrase->iTail = iFirst;
  }else{
    assert( pPhrase->pList==0 && pPhrase->pHead==0 && pPhrase->pTail==0 );
  }

  return SQLITE_OK;
}

#define BITMASK_SIZE 64

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
  int nList;                      /* Number of phrases in expression */
  SnippetCtx sCtx;                /* Snippet context object */
  int nByte;                      /* Number of bytes of space to allocate */
  int iBestScore = -1;
  int i;

  memset(&sCtx, 0, sizeof(sCtx));

  /* Iterate through the phrases in the expression to count them. The same
  ** callback makes sure the doclists are loaded for each phrase.
  */
  rc = fts3ExprLoadDoclists(pCsr, &nList, 0);
  if( rc!=SQLITE_OK ){
    return rc;
  }

  /* Now that it is known how many phrases there are, allocate and zero
  ** the required space using malloc().
  */
  nByte = sizeof(SnippetPhrase) * nList;
  sCtx.aPhrase = (SnippetPhrase *)sqlite3_malloc(nByte);
  if( !sCtx.aPhrase ){
    return SQLITE_NOMEM;
  }
  memset(sCtx.aPhrase, 0, nByte);

  /* Initialize the contents of the SnippetCtx object. Then iterate through
  ** the set of phrases in the expression to populate the aPhrase[] array.
  */
  sCtx.pCsr = pCsr;
  sCtx.iCol = iCol;
  sCtx.nSnippet = nSnippet;
  sCtx.nPhrase = nList;
  sCtx.iCurrent = -1;
  (void)fts3ExprIterate(pCsr->pExpr, fts3SnippetFindPositions, (void *)&sCtx);

  for(i=0; i<nList; i++){
    if( sCtx.aPhrase[i].pHead ){
      *pmSeen |= (u64)1 << i;
    }
  }

  pFragment->iCol = iCol;
  while( !fts3SnippetNextCandidate(&sCtx) ){
    int iPos;
    int iScore;
    u64 mCover;
    u64 mHighlight;
    fts3SnippetDetails(&sCtx, mCovered, &iPos, &iScore, &mCover, &mHighlight);

    assert( iScore>=0 );
    if( iScore>iBestScore ){
      pFragment->iPos = iPos;
      pFragment->hlmask = mHighlight;
      pFragment->covered = mCover;
      iBestScore = iScore;
    }
  }

  sqlite3_free(sCtx.aPhrase);
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
      if( rc!=SQLITE_OK ){
        return rc;
      }
      pC->pTokenizer = pTab->pTokenizer;
      while( rc==SQLITE_OK && iCurrent<(nSnippet+nDesired) ){
        const char *ZDUMMY; int DUMMY1, DUMMY2, DUMMY3;
        rc = pMod->xNext(pC, &ZDUMMY, &DUMMY1, &DUMMY2, &DUMMY3, &iCurrent);
      }
      pMod->xClose(pC);
      if( rc!=SQLITE_OK && rc!=SQLITE_DONE ){ return rc; }

      nShift = (rc==SQLITE_DONE)+iCurrent-nSnippet;
      assert( nShift<=nDesired );
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
  int iFragment,                  /* Fragment number */
  int isLast,                     /* True for final fragment in snippet */
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
  int iEnd = 0;                   /* Byte offset of end of current token */
  int isShiftDone = 0;
  int iPos = pFragment->iPos;
  u64 hlmask = pFragment->hlmask;

  sqlite3_tokenizer_module *pMod; /* Tokenizer module methods object */
  sqlite3_tokenizer_cursor *pC;   /* Tokenizer cursor open on zDoc/nDoc */
  const char *ZDUMMY;             /* Dummy arguments used with tokenizer */
  int DUMMY1;                     /* Dummy arguments used with tokenizer */
  
  zDoc = (const char *)sqlite3_column_text(pCsr->pStmt, pFragment->iCol+1);
  if( zDoc==0 ){
    if( sqlite3_column_type(pCsr->pStmt, pFragment->iCol+1)!=SQLITE_NULL ){
      return SQLITE_NOMEM;
    }
    return SQLITE_OK;
  }
  nDoc = sqlite3_column_bytes(pCsr->pStmt, pFragment->iCol+1);

  /* Open a token cursor on the document. */
  pMod = (sqlite3_tokenizer_module *)pTab->pTokenizer->pModule;
  rc = pMod->xOpen(pTab->pTokenizer, zDoc, nDoc, &pC);
  if( rc!=SQLITE_OK ){
    return rc;
  }
  pC->pTokenizer = pTab->pTokenizer;

  while( rc==SQLITE_OK ){
    int iBegin;                   /* Offset in zDoc of start of token */
    int iFin;                     /* Offset in zDoc of end of token */
    int isHighlight;

    rc = pMod->xNext(pC, &ZDUMMY, &DUMMY1, &iBegin, &iFin, &iCurrent);
    if( rc!=SQLITE_OK ){
      if( rc==SQLITE_DONE ){
        /* Special case - the last token of the snippet is also the last token
        ** of the column. Append any punctuation that occurred between the end
        ** of the previous token and the end of the document to the output. 
        ** Then break out of the loop. */
        rc = fts3StringAppend(pOut, &zDoc[iEnd], -1);
      }
      break;
    }
    if( iCurrent<iPos ){ continue; }

    if( !isShiftDone ){
      int n = nDoc - iBegin;
      rc = fts3SnippetShift(pTab, nSnippet, &zDoc[iBegin], n, &iPos, &hlmask);
      isShiftDone = 1;

      /* Now that the shift has been done, check if the initial "..." are
      ** required. They are required if (a) this is not the first fragment,
      ** or (b) this fragment does not begin at position 0 of its column. 
      */
      if( rc==SQLITE_OK && (iPos>0 || iFragment>0) ){
        rc = fts3StringAppend(pOut, zEllipsis, -1);
      }
      if( rc!=SQLITE_OK || iCurrent<iPos ) continue;
    }

    if( iCurrent>=(iPos+nSnippet) ){
      if( isLast ){
        rc = fts3StringAppend(pOut, zEllipsis, -1);
      }
      break;
    }

    /* Set isHighlight to true if this term should be highlighted. */
    isHighlight = (hlmask & ((u64)1 << (iCurrent-iPos)))!=0;

    if( iCurrent>iPos ) rc = fts3StringAppend(pOut, &zDoc[iEnd], iBegin-iEnd);
    if( rc==SQLITE_OK && isHighlight ) rc = fts3StringAppend(pOut, zOpen, -1);
    if( rc==SQLITE_OK ) rc = fts3StringAppend(pOut, &zDoc[iBegin], iFin-iBegin);
    if( rc==SQLITE_OK && isHighlight ) rc = fts3StringAppend(pOut, zClose, -1);

    iEnd = iFin;
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
  int iPhrase,
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
  int iPhrase,
  void *pCtx                      /* Pointer to MatchInfo structure */
){
  MatchInfo *p = (MatchInfo *)pCtx;
  p->iPhrase++;

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
    rc = fts3SnippetText(pCsr, &aSnippet[i], 
        i, (i==nSnippet-1), nFToken, zStart, zEnd, zEllipsis, &res
    );
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
static int fts3ExprTermOffsetInit(Fts3Expr *pExpr, int iPhrase, void *ctx){
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

    /* Retreive the text stored in column iCol. If an SQL NULL is stored 
    ** in column iCol, jump immediately to the next iteration of the loop.
    ** If an OOM occurs while retrieving the data (this can happen if SQLite
    ** needs to transform the data from utf-16 to utf-8), return SQLITE_NOMEM 
    ** to the caller. 
    */
    zDoc = (const char *)sqlite3_column_text(pCsr->pStmt, iCol+1);
    nDoc = sqlite3_column_bytes(pCsr->pStmt, iCol+1);
    if( zDoc==0 ){
      if( sqlite3_column_type(pCsr->pStmt, iCol+1)==SQLITE_NULL ){
        continue;
      }
      rc = SQLITE_NOMEM;
      goto offsets_out;
    }

    /* Initialize a tokenizer iterator to iterate through column iCol. */
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
          rc = fts3StringAppend(&res, aBuffer, -1);
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

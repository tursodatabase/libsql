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

typedef struct Snippet Snippet;

/*
** An instance of the following structure keeps track of generated
** matching-word offset information and snippets.
*/
struct Snippet {
  int nMatch;                     /* Total number of matches */
  int nAlloc;                     /* Space allocated for aMatch[] */
  struct snippetMatch {  /* One entry for each matching term */
    char snStatus;       /* Status flag for use while constructing snippets */
    short int nByte;     /* Number of bytes in the term */
    short int iCol;      /* The column that contains the match */
    short int iTerm;     /* The index in Query.pTerms[] of the matching term */
    int iToken;          /* The index of the matching document token */
    int iStart;          /* The offset to the first character of the term */
  } *aMatch;                      /* Points to space obtained from malloc */
  char *zOffset;                  /* Text rendering of aMatch[] */
  int nOffset;                    /* strlen(zOffset) */
  char *zSnippet;                 /* Snippet text */
  int nSnippet;                   /* strlen(zSnippet) */
};


/* It is not safe to call isspace(), tolower(), or isalnum() on
** hi-bit-set characters.  This is the same solution used in the
** tokenizer.
*/
static int fts3snippetIsspace(char c){
  return (c&0x80)==0 ? isspace(c) : 0;
}


/*
** A StringBuffer object holds a zero-terminated string that grows
** arbitrarily by appending.  Space to hold the string is obtained
** from sqlite3_malloc().  After any memory allocation failure, 
** StringBuffer.z is set to NULL and no further allocation is attempted.
*/
typedef struct StringBuffer {
  char *z;         /* Text of the string.  Space from malloc. */
  int nUsed;       /* Number bytes of z[] used, not counting \000 terminator */
  int nAlloc;      /* Bytes allocated for z[] */
} StringBuffer;


/*
** Initialize a new StringBuffer.
*/
static void fts3SnippetSbInit(StringBuffer *p){
  p->nAlloc = 100;
  p->nUsed = 0;
  p->z = sqlite3_malloc( p->nAlloc );
}

/*
** Append text to the string buffer.
*/
static void fts3SnippetAppend(StringBuffer *p, const char *zNew, int nNew){
  if( p->z==0 ) return;
  if( nNew<0 ) nNew = (int)strlen(zNew);
  if( p->nUsed + nNew >= p->nAlloc ){
    int nAlloc;
    char *zNew;

    nAlloc = p->nUsed + nNew + p->nAlloc;
    zNew = sqlite3_realloc(p->z, nAlloc);
    if( zNew==0 ){
      sqlite3_free(p->z);
      p->z = 0;
      return;
    }
    p->z = zNew;
    p->nAlloc = nAlloc;
  }
  memcpy(&p->z[p->nUsed], zNew, nNew);
  p->nUsed += nNew;
  p->z[p->nUsed] = 0;
}

/* If the StringBuffer ends in something other than white space, add a
** single space character to the end.
*/
static void fts3SnippetAppendWhiteSpace(StringBuffer *p){
  if( p->z && p->nUsed && !fts3snippetIsspace(p->z[p->nUsed-1]) ){
    fts3SnippetAppend(p, " ", 1);
  }
}

/* Remove white space from the end of the StringBuffer */
static void fts3SnippetTrimWhiteSpace(StringBuffer *p){
  if( p->z ){
    while( p->nUsed && fts3snippetIsspace(p->z[p->nUsed-1]) ){
      p->nUsed--;
    }
    p->z[p->nUsed] = 0;
  }
}

/* 
** Release all memory associated with the Snippet structure passed as
** an argument.
*/
static void fts3SnippetFree(Snippet *p){
  if( p ){
    sqlite3_free(p->aMatch);
    sqlite3_free(p->zOffset);
    sqlite3_free(p->zSnippet);
    sqlite3_free(p);
  }
}

/*
** Append a single entry to the p->aMatch[] log.
*/
static int snippetAppendMatch(
  Snippet *p,               /* Append the entry to this snippet */
  int iCol, int iTerm,      /* The column and query term */
  int iToken,               /* Matching token in document */
  int iStart, int nByte     /* Offset and size of the match */
){
  int i;
  struct snippetMatch *pMatch;
  if( p->nMatch+1>=p->nAlloc ){
    struct snippetMatch *pNew;
    p->nAlloc = p->nAlloc*2 + 10;
    pNew = sqlite3_realloc(p->aMatch, p->nAlloc*sizeof(p->aMatch[0]) );
    if( pNew==0 ){
      p->aMatch = 0;
      p->nMatch = 0;
      p->nAlloc = 0;
      return SQLITE_NOMEM;
    }
    p->aMatch = pNew;
  }
  i = p->nMatch++;
  pMatch = &p->aMatch[i];
  pMatch->iCol = (short)iCol;
  pMatch->iTerm = (short)iTerm;
  pMatch->iToken = iToken;
  pMatch->iStart = iStart;
  pMatch->nByte = (short)nByte;
  return SQLITE_OK;
}

/*
** Sizing information for the circular buffer used in snippetOffsetsOfColumn()
*/
#define FTS3_ROTOR_SZ   (32)
#define FTS3_ROTOR_MASK (FTS3_ROTOR_SZ-1)

/*
** Function to iterate through the tokens of a compiled expression.
**
** Except, skip all tokens on the right-hand side of a NOT operator.
** This function is used to find tokens as part of snippet and offset
** generation and we do nt want snippets and offsets to report matches
** for tokens on the RHS of a NOT.
*/
static int fts3NextExprToken(Fts3Expr **ppExpr, int *piToken){
  Fts3Expr *p = *ppExpr;
  int iToken = *piToken;
  if( iToken<0 ){
    /* In this case the expression p is the root of an expression tree.
    ** Move to the first token in the expression tree.
    */
    while( p->pLeft ){
      p = p->pLeft;
    }
    iToken = 0;
  }else{
    assert(p && p->eType==FTSQUERY_PHRASE );
    if( iToken<(p->pPhrase->nToken-1) ){
      iToken++;
    }else{
      iToken = 0;
      while( p->pParent && p->pParent->pLeft!=p ){
        assert( p->pParent->pRight==p );
        p = p->pParent;
      }
      p = p->pParent;
      if( p ){
        assert( p->pRight!=0 );
        p = p->pRight;
        while( p->pLeft ){
          p = p->pLeft;
        }
      }
    }
  }

  *ppExpr = p;
  *piToken = iToken;
  return p?1:0;
}

/*
** Return TRUE if the expression node pExpr is located beneath the
** RHS of a NOT operator.
*/
static int fts3ExprBeneathNot(Fts3Expr *p){
  Fts3Expr *pParent;
  while( p ){
    pParent = p->pParent;
    if( pParent && pParent->eType==FTSQUERY_NOT && pParent->pRight==p ){
      return 1;
    }
    p = pParent;
  }
  return 0;
}

/*
** Add entries to pSnippet->aMatch[] for every match that occurs against
** document zDoc[0..nDoc-1] which is stored in column iColumn.
*/
static int snippetOffsetsOfColumn(
  Fts3Cursor *pCur,         /* The fulltest search cursor */
  Snippet *pSnippet,             /* The Snippet object to be filled in */
  int iColumn,                   /* Index of fulltext table column */
  const char *zDoc,              /* Text of the fulltext table column */
  int nDoc                       /* Length of zDoc in bytes */
){
  const sqlite3_tokenizer_module *pTModule;  /* The tokenizer module */
  sqlite3_tokenizer *pTokenizer;             /* The specific tokenizer */
  sqlite3_tokenizer_cursor *pTCursor;        /* Tokenizer cursor */
  Fts3Table *pVtab;                /* The full text index */
  int nColumn;                         /* Number of columns in the index */
  int i, j;                            /* Loop counters */
  int rc;                              /* Return code */
  unsigned int match, prevMatch;       /* Phrase search bitmasks */
  const char *zToken;                  /* Next token from the tokenizer */
  int nToken;                          /* Size of zToken */
  int iBegin, iEnd, iPos;              /* Offsets of beginning and end */

  /* The following variables keep a circular buffer of the last
  ** few tokens */
  unsigned int iRotor = 0;             /* Index of current token */
  int iRotorBegin[FTS3_ROTOR_SZ];      /* Beginning offset of token */
  int iRotorLen[FTS3_ROTOR_SZ];        /* Length of token */

  pVtab =  (Fts3Table *)pCur->base.pVtab;
  nColumn = pVtab->nColumn;
  pTokenizer = pVtab->pTokenizer;
  pTModule = pTokenizer->pModule;
  rc = pTModule->xOpen(pTokenizer, zDoc, nDoc, &pTCursor);
  if( rc ) return rc;
  pTCursor->pTokenizer = pTokenizer;

  prevMatch = 0;
  while( (rc = pTModule->xNext(pTCursor, &zToken, &nToken,
                               &iBegin, &iEnd, &iPos))==SQLITE_OK ){
    Fts3Expr *pIter = pCur->pExpr;
    int iIter = -1;
    iRotorBegin[iRotor&FTS3_ROTOR_MASK] = iBegin;
    iRotorLen[iRotor&FTS3_ROTOR_MASK] = iEnd-iBegin;
    match = 0;
    for(i=0; i<(FTS3_ROTOR_SZ-1) && fts3NextExprToken(&pIter, &iIter); i++){
      int nPhrase;                    /* Number of tokens in current phrase */
      struct PhraseToken *pToken;     /* Current token */
      int iCol;                       /* Column index */

      if( fts3ExprBeneathNot(pIter) ) continue;
      nPhrase = pIter->pPhrase->nToken;
      pToken = &pIter->pPhrase->aToken[iIter];
      iCol = pIter->pPhrase->iColumn;
      if( iCol>=0 && iCol<nColumn && iCol!=iColumn ) continue;
      if( pToken->n>nToken ) continue;
      if( !pToken->isPrefix && pToken->n<nToken ) continue;
      assert( pToken->n<=nToken );
      if( memcmp(pToken->z, zToken, pToken->n) ) continue;
      if( iIter>0 && (prevMatch & (1<<i))==0 ) continue;
      match |= 1<<i;
      if( i==(FTS3_ROTOR_SZ-2) || nPhrase==iIter+1 ){
        for(j=nPhrase-1; j>=0; j--){
          int k = (iRotor-j) & FTS3_ROTOR_MASK;
          rc = snippetAppendMatch(pSnippet, iColumn, i-j, iPos-j,
                                  iRotorBegin[k], iRotorLen[k]);
          if( rc ) goto end_offsets_of_column;
        }
      }
    }
    prevMatch = match<<1;
    iRotor++;
  }
end_offsets_of_column:
  pTModule->xClose(pTCursor);  
  return rc==SQLITE_DONE ? SQLITE_OK : rc;
}

/*
** Remove entries from the pSnippet structure to account for the NEAR
** operator. When this is called, pSnippet contains the list of token 
** offsets produced by treating all NEAR operators as AND operators.
** This function removes any entries that should not be present after
** accounting for the NEAR restriction. For example, if the queried
** document is:
**
**     "A B C D E A"
**
** and the query is:
** 
**     A NEAR/0 E
**
** then when this function is called the Snippet contains token offsets
** 0, 4 and 5. This function removes the "0" entry (because the first A
** is not near enough to an E).
**
** When this function is called, the value pointed to by parameter piLeft is
** the integer id of the left-most token in the expression tree headed by
** pExpr. This function increments *piLeft by the total number of tokens
** in the expression tree headed by pExpr.
**
** Return 1 if any trimming occurs.  Return 0 if no trimming is required.
*/
static int trimSnippetOffsets(
  Fts3Expr *pExpr,      /* The search expression */
  Snippet *pSnippet,    /* The set of snippet offsets to be trimmed */
  int *piLeft           /* Index of left-most token in pExpr */
){
  if( pExpr ){
    if( trimSnippetOffsets(pExpr->pLeft, pSnippet, piLeft) ){
      return 1;
    }

    switch( pExpr->eType ){
      case FTSQUERY_PHRASE:
        *piLeft += pExpr->pPhrase->nToken;
        break;
      case FTSQUERY_NEAR: {
        /* The right-hand-side of a NEAR operator is always a phrase. The
        ** left-hand-side is either a phrase or an expression tree that is 
        ** itself headed by a NEAR operator. The following initializations
        ** set local variable iLeft to the token number of the left-most
        ** token in the right-hand phrase, and iRight to the right most
        ** token in the same phrase. For example, if we had:
        **
        **     <col> MATCH '"abc def" NEAR/2 "ghi jkl"'
        **
        ** then iLeft will be set to 2 (token number of ghi) and nToken will
        ** be set to 4.
        */
        Fts3Expr *pLeft = pExpr->pLeft;
        Fts3Expr *pRight = pExpr->pRight;
        int iLeft = *piLeft;
        int nNear = pExpr->nNear;
        int nToken = pRight->pPhrase->nToken;
        int jj, ii;
        if( pLeft->eType==FTSQUERY_NEAR ){
          pLeft = pLeft->pRight;
        }
        assert( pRight->eType==FTSQUERY_PHRASE );
        assert( pLeft->eType==FTSQUERY_PHRASE );
        nToken += pLeft->pPhrase->nToken;

        for(ii=0; ii<pSnippet->nMatch; ii++){
          struct snippetMatch *p = &pSnippet->aMatch[ii];
          if( p->iTerm==iLeft ){
            int isOk = 0;
            /* Snippet ii is an occurence of query term iLeft in the document.
            ** It occurs at position (p->iToken) of the document. We now
            ** search for an instance of token (iLeft-1) somewhere in the 
            ** range (p->iToken - nNear)...(p->iToken + nNear + nToken) within 
            ** the set of snippetMatch structures. If one is found, proceed. 
            ** If one cannot be found, then remove snippets ii..(ii+N-1) 
            ** from the matching snippets, where N is the number of tokens 
            ** in phrase pRight->pPhrase.
            */
            for(jj=0; isOk==0 && jj<pSnippet->nMatch; jj++){
              struct snippetMatch *p2 = &pSnippet->aMatch[jj];
              if( p2->iTerm==(iLeft-1) ){
                if( p2->iToken>=(p->iToken-nNear-1) 
                 && p2->iToken<(p->iToken+nNear+nToken) 
                ){
                  isOk = 1;
                }
              }
            }
            if( !isOk ){
              int kk;
              for(kk=0; kk<pRight->pPhrase->nToken; kk++){
                pSnippet->aMatch[kk+ii].iTerm = -2;
              }
              return 1;
            }
          }
          if( p->iTerm==(iLeft-1) ){
            int isOk = 0;
            for(jj=0; isOk==0 && jj<pSnippet->nMatch; jj++){
              struct snippetMatch *p2 = &pSnippet->aMatch[jj];
              if( p2->iTerm==iLeft ){
                if( p2->iToken<=(p->iToken+nNear+1) 
                 && p2->iToken>(p->iToken-nNear-nToken) 
                ){
                  isOk = 1;
                }
              }
            }
            if( !isOk ){
              int kk;
              for(kk=0; kk<pLeft->pPhrase->nToken; kk++){
                pSnippet->aMatch[ii-kk].iTerm = -2;
              }
              return 1;
            }
          }
        }
        break;
      }
    }

    if( trimSnippetOffsets(pExpr->pRight, pSnippet, piLeft) ){
      return 1;
    }
  }
  return 0;
}

/*
** Compute all offsets for the current row of the query.  
** If the offsets have already been computed, this routine is a no-op.
*/
static int snippetAllOffsets(Fts3Cursor *pCsr, Snippet **ppSnippet){
  Fts3Table *p = (Fts3Table *)pCsr->base.pVtab;  /* The FTS3 virtual table */
  int nColumn;           /* Number of columns.  Docid does count */
  int iColumn;           /* Index of of a column */
  int i;                 /* Loop index */
  int iFirst;            /* First column to search */
  int iLast;             /* Last coumn to search */
  int iTerm = 0;
  Snippet *pSnippet;
  int rc = SQLITE_OK;

  if( pCsr->pExpr==0 ){
    return SQLITE_OK;
  }

  pSnippet = (Snippet *)sqlite3_malloc(sizeof(Snippet));
  *ppSnippet = pSnippet;
  if( !pSnippet ){
    return SQLITE_NOMEM;
  }
  memset(pSnippet, 0, sizeof(Snippet));

  nColumn = p->nColumn;
  iColumn = (pCsr->eSearch - 2);
  if( iColumn<0 || iColumn>=nColumn ){
    /* Look for matches over all columns of the full-text index */
    iFirst = 0;
    iLast = nColumn-1;
  }else{
    /* Look for matches in the iColumn-th column of the index only */
    iFirst = iColumn;
    iLast = iColumn;
  }
  for(i=iFirst; rc==SQLITE_OK && i<=iLast; i++){
    const char *zDoc;
    int nDoc;
    zDoc = (const char*)sqlite3_column_text(pCsr->pStmt, i+1);
    nDoc = sqlite3_column_bytes(pCsr->pStmt, i+1);
    if( zDoc==0 && sqlite3_column_type(pCsr->pStmt, i+1)!=SQLITE_NULL ){
      rc = SQLITE_NOMEM;
    }else{
      rc = snippetOffsetsOfColumn(pCsr, pSnippet, i, zDoc, nDoc);
    }
  }

  while( trimSnippetOffsets(pCsr->pExpr, pSnippet, &iTerm) ){
    iTerm = 0;
  }

  return rc;
}

/*
** Convert the information in the aMatch[] array of the snippet
** into the string zOffset[0..nOffset-1]. This string is used as
** the return of the SQL offsets() function.
*/
static void snippetOffsetText(Snippet *p){
  int i;
  int cnt = 0;
  StringBuffer sb;
  char zBuf[200];
  if( p->zOffset ) return;
  fts3SnippetSbInit(&sb);
  for(i=0; i<p->nMatch; i++){
    struct snippetMatch *pMatch = &p->aMatch[i];
    if( pMatch->iTerm>=0 ){
      /* If snippetMatch.iTerm is less than 0, then the match was 
      ** discarded as part of processing the NEAR operator (see the 
      ** trimSnippetOffsetsForNear() function for details). Ignore 
      ** it in this case
      */
      zBuf[0] = ' ';
      sqlite3_snprintf(sizeof(zBuf)-1, &zBuf[cnt>0], "%d %d %d %d",
          pMatch->iCol, pMatch->iTerm, pMatch->iStart, pMatch->nByte);
      fts3SnippetAppend(&sb, zBuf, -1);
      cnt++;
    }
  }
  p->zOffset = sb.z;
  p->nOffset = sb.z ? sb.nUsed : 0;
}

/*
** zDoc[0..nDoc-1] is phrase of text.  aMatch[0..nMatch-1] are a set
** of matching words some of which might be in zDoc.  zDoc is column
** number iCol.
**
** iBreak is suggested spot in zDoc where we could begin or end an
** excerpt.  Return a value similar to iBreak but possibly adjusted
** to be a little left or right so that the break point is better.
*/
static int wordBoundary(
  int iBreak,                   /* The suggested break point */
  const char *zDoc,             /* Document text */
  int nDoc,                     /* Number of bytes in zDoc[] */
  struct snippetMatch *aMatch,  /* Matching words */
  int nMatch,                   /* Number of entries in aMatch[] */
  int iCol                      /* The column number for zDoc[] */
){
  int i;
  if( iBreak<=10 ){
    return 0;
  }
  if( iBreak>=nDoc-10 ){
    return nDoc;
  }
  for(i=0; ALWAYS(i<nMatch) && aMatch[i].iCol<iCol; i++){}
  while( i<nMatch && aMatch[i].iStart+aMatch[i].nByte<iBreak ){ i++; }
  if( i<nMatch ){
    if( aMatch[i].iStart<iBreak+10 ){
      return aMatch[i].iStart;
    }
    if( i>0 && aMatch[i-1].iStart+aMatch[i-1].nByte>=iBreak ){
      return aMatch[i-1].iStart;
    }
  }
  for(i=1; i<=10; i++){
    if( fts3snippetIsspace(zDoc[iBreak-i]) ){
      return iBreak - i + 1;
    }
    if( fts3snippetIsspace(zDoc[iBreak+i]) ){
      return iBreak + i + 1;
    }
  }
  return iBreak;
}



/*
** Allowed values for Snippet.aMatch[].snStatus
*/
#define SNIPPET_IGNORE  0   /* It is ok to omit this match from the snippet */
#define SNIPPET_DESIRED 1   /* We want to include this match in the snippet */

/*
** Generate the text of a snippet.
*/
static void snippetText(
  Fts3Cursor *pCursor,   /* The cursor we need the snippet for */
  Snippet *pSnippet,
  const char *zStartMark,     /* Markup to appear before each match */
  const char *zEndMark,       /* Markup to appear after each match */
  const char *zEllipsis       /* Ellipsis mark */
){
  int i, j;
  struct snippetMatch *aMatch;
  int nMatch;
  int nDesired;
  StringBuffer sb;
  int tailCol;
  int tailOffset;
  int iCol;
  int nDoc;
  const char *zDoc;
  int iStart, iEnd;
  int tailEllipsis = 0;
  int iMatch;
  

  sqlite3_free(pSnippet->zSnippet);
  pSnippet->zSnippet = 0;
  aMatch = pSnippet->aMatch;
  nMatch = pSnippet->nMatch;
  fts3SnippetSbInit(&sb);

  for(i=0; i<nMatch; i++){
    aMatch[i].snStatus = SNIPPET_IGNORE;
  }
  nDesired = 0;
  for(i=0; i<FTS3_ROTOR_SZ; i++){
    for(j=0; j<nMatch; j++){
      if( aMatch[j].iTerm==i ){
        aMatch[j].snStatus = SNIPPET_DESIRED;
        nDesired++;
        break;
      }
    }
  }

  iMatch = 0;
  tailCol = -1;
  tailOffset = 0;
  for(i=0; i<nMatch && nDesired>0; i++){
    if( aMatch[i].snStatus!=SNIPPET_DESIRED ) continue;
    nDesired--;
    iCol = aMatch[i].iCol;
    zDoc = (const char*)sqlite3_column_text(pCursor->pStmt, iCol+1);
    nDoc = sqlite3_column_bytes(pCursor->pStmt, iCol+1);
    iStart = aMatch[i].iStart - 40;
    iStart = wordBoundary(iStart, zDoc, nDoc, aMatch, nMatch, iCol);
    if( iStart<=10 ){
      iStart = 0;
    }
    if( iCol==tailCol && iStart<=tailOffset+20 ){
      iStart = tailOffset;
    }
    if( (iCol!=tailCol && tailCol>=0) || iStart!=tailOffset ){
      fts3SnippetTrimWhiteSpace(&sb);
      fts3SnippetAppendWhiteSpace(&sb);
      fts3SnippetAppend(&sb, zEllipsis, -1);
      fts3SnippetAppendWhiteSpace(&sb);
    }
    iEnd = aMatch[i].iStart + aMatch[i].nByte + 40;
    iEnd = wordBoundary(iEnd, zDoc, nDoc, aMatch, nMatch, iCol);
    if( iEnd>=nDoc-10 ){
      iEnd = nDoc;
      tailEllipsis = 0;
    }else{
      tailEllipsis = 1;
    }
    while( iMatch<nMatch && aMatch[iMatch].iCol<iCol ){ iMatch++; }
    while( iStart<iEnd ){
      while( iMatch<nMatch && aMatch[iMatch].iStart<iStart
             && aMatch[iMatch].iCol<=iCol ){
        iMatch++;
      }
      if( iMatch<nMatch && aMatch[iMatch].iStart<iEnd
             && aMatch[iMatch].iCol==iCol ){
        fts3SnippetAppend(&sb, &zDoc[iStart], aMatch[iMatch].iStart - iStart);
        iStart = aMatch[iMatch].iStart;
        fts3SnippetAppend(&sb, zStartMark, -1);
        fts3SnippetAppend(&sb, &zDoc[iStart], aMatch[iMatch].nByte);
        fts3SnippetAppend(&sb, zEndMark, -1);
        iStart += aMatch[iMatch].nByte;
        for(j=iMatch+1; j<nMatch; j++){
          if( aMatch[j].iTerm==aMatch[iMatch].iTerm
              && aMatch[j].snStatus==SNIPPET_DESIRED ){
            nDesired--;
            aMatch[j].snStatus = SNIPPET_IGNORE;
          }
        }
      }else{
        fts3SnippetAppend(&sb, &zDoc[iStart], iEnd - iStart);
        iStart = iEnd;
      }
    }
    tailCol = iCol;
    tailOffset = iEnd;
  }
  fts3SnippetTrimWhiteSpace(&sb);
  if( tailEllipsis ){
    fts3SnippetAppendWhiteSpace(&sb);
    fts3SnippetAppend(&sb, zEllipsis, -1);
  }
  pSnippet->zSnippet = sb.z;
  pSnippet->nSnippet = sb.z ? sb.nUsed : 0;
}

void sqlite3Fts3Offsets(
  sqlite3_context *pCtx,          /* SQLite function call context */
  Fts3Cursor *pCsr                /* Cursor object */
){
  Snippet *p;                     /* Snippet structure */
  int rc = snippetAllOffsets(pCsr, &p);
  if( rc==SQLITE_OK ){
    snippetOffsetText(p);
    if( p->zOffset ){
      sqlite3_result_text(pCtx, p->zOffset, p->nOffset, SQLITE_TRANSIENT);
    }else{
      sqlite3_result_error_nomem(pCtx);
    }
  }else{
    sqlite3_result_error_nomem(pCtx);
  }
  fts3SnippetFree(p);
}

void sqlite3Fts3Snippet(
  sqlite3_context *pCtx,          /* SQLite function call context */
  Fts3Cursor *pCsr,               /* Cursor object */
  const char *zStart,             /* Snippet start text - "<b>" */
  const char *zEnd,               /* Snippet end text - "</b>" */
  const char *zEllipsis           /* Snippet ellipsis text - "<b>...</b>" */
){
  Snippet *p;                     /* Snippet structure */
  int rc = snippetAllOffsets(pCsr, &p);
  if( rc==SQLITE_OK ){
    snippetText(pCsr, p, zStart, zEnd, zEllipsis);
    if( p->zSnippet ){
      sqlite3_result_text(pCtx, p->zSnippet, p->nSnippet, SQLITE_TRANSIENT);
    }else{
      sqlite3_result_error_nomem(pCtx);
    }
  }else{
    sqlite3_result_error_nomem(pCtx);
  }
  fts3SnippetFree(p);
}

/*************************************************************************
** Below this point is the alternative, experimental snippet() implementation.
*/

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
};

static int fts3ExprLoadDoclistsCb(Fts3Expr *pExpr, void *ctx){
  int rc = SQLITE_OK;
  LoadDoclistCtx *p = (LoadDoclistCtx *)ctx;
  p->nPhrase++;
  if( pExpr->isLoaded==0 ){
    rc = sqlite3Fts3ExprLoadDoclist(p->pTab, pExpr);
    pExpr->isLoaded = 1;
    if( rc==SQLITE_OK && pExpr->aDoclist ){
      pExpr->pCurrent = pExpr->aDoclist;
      pExpr->pCurrent += sqlite3Fts3GetVarint(pExpr->pCurrent,&pExpr->iCurrent);
    }
  }
  return rc;
}

static int fts3ExprLoadDoclists(Fts3Cursor *pCsr, int *pnPhrase){
  int rc;
  LoadDoclistCtx sCtx = {0, 0};
  sCtx.pTab = (Fts3Table *)pCsr->base.pVtab;
  rc = fts3ExprIterate(pCsr->pExpr, fts3ExprLoadDoclistsCb, (void *)&sCtx);
  *pnPhrase = sCtx.nPhrase;
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

    if( !pList ){
      nFin++;
      continue;
    }

    while( iPrev<(iPos+SNIPPET_BUFFER_CHUNK) ){
      if( iPrev>=iPos ){
        aBuffer[iPrev&SNIPPET_BUFFER_MASK] = (u8)(i+1);
      }
      if( 0==((*pList)&0xFE) ){
        nFin++;
        break;
      }
      fts3GetDeltaPosition(&pList, &iPrev); 
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
  int iSub2 = (iIdx+(nSnippet/3)-1)&SNIPPET_BUFFER_MASK;
  int iAdd2 = (iIdx+(nSnippet*2/3)-1)&SNIPPET_BUFFER_MASK;

  u64 h = *pHlmask;

  anCnt[ aBuffer[iSub]  ]--;
  anCnt[ aBuffer[iSub2] ]--;
  anCnt[ aBuffer[iAdd]  ]++;
  anCnt[ aBuffer[iAdd2] ]++;

  h = h >> 1;
  if( aBuffer[iAdd] ){
    int j;
    for(j=anToken[aBuffer[iAdd]-1]; j>=1; j--){
      h |= (u64)1 << (nSnippet-j);
    }
  }
  *pHlmask = h;
}

static int fts3SnippetScore(int n, int *anCnt){
  int j;
  int iScore = 0;
  for(j=1; j<=n; j++){
    int nCnt = anCnt[j];
    iScore += nCnt + (nCnt ? 1000 : 0);
  }
  return iScore;
}

static int fts3BestSnippet(
  int nSnippet,                   /* Desired snippet length */
  Fts3Cursor *pCsr,               /* Cursor to create snippet for */
  int iCol,                       /* Index of column to create snippet from */
  int *piPos,                     /* OUT: Starting token for best snippet */
  u64 *pHlmask                    /* OUT: Highlight mask for best snippet */
){
  int rc;                         /* Return Code */
  u8 aBuffer[SNIPPET_BUFFER_SIZE];/* Circular snippet buffer */
  int *aiPrev;                    /* Used by fts3LoadSnippetBuffer() */
  int *anToken;                   /* Number of tokens in each phrase */
  char **apList;                  /* Array of position lists */
  int *anCnt;                     /* Running totals of phrase occurences */
  int nList;

  int i;

  u64 hlmask = 0;                 /* Current mask of highlighted terms */
  u64 besthlmask = 0;             /* Mask of highlighted terms for iBestPos */
  int iBestPos = 0;               /* Starting position of 'best' snippet */
  int iBestScore = 0;             /* Score of best snippet higher->better */
  SnippetCtx sCtx;

  /* Iterate through the phrases in the expression to count them. The same
  ** callback makes sure the doclists are loaded for each phrase.
  */
  rc = fts3ExprLoadDoclists(pCsr, &nList);
  if( rc!=SQLITE_OK ){
    return rc;
  }

  /* Now that it is known how many phrases there are, allocate and zero
  ** the required arrays using malloc().
  */
  apList = sqlite3_malloc(
      sizeof(u8*)*nList +         /* apList */
      sizeof(int)*(nList) +       /* anToken */
      sizeof(int)*nList +         /* aiPrev */
      sizeof(int)*(nList+1)       /* anCnt */
  );
  if( !apList ){
    return SQLITE_NOMEM;
  }
  memset(apList, 0, sizeof(u8*)*nList+sizeof(int)*nList+sizeof(int)*nList);
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

  /* Load the first two chunks of data into the buffer. */
  memset(aBuffer, 0, SNIPPET_BUFFER_SIZE);
  fts3LoadSnippetBuffer(0, aBuffer, nList, apList, aiPrev);
  fts3LoadSnippetBuffer(SNIPPET_BUFFER_CHUNK, aBuffer, nList, apList, aiPrev);

  /* Set the initial contents of the highlight-mask and anCnt[] array. */
  for(i=1-nSnippet; i<=0; i++){
    fts3SnippetCnt(i, nSnippet, anCnt, aBuffer, anToken, &hlmask);
  }
  iBestScore = fts3SnippetScore(nList, anCnt);
  besthlmask = hlmask;
  iBestPos = 0;

  for(i=1; 1; i++){
    int iScore;

    if( 0==(i&(SNIPPET_BUFFER_CHUNK-1)) ){
      int iLoad = i + SNIPPET_BUFFER_CHUNK;
      if( fts3LoadSnippetBuffer(iLoad, aBuffer, nList, apList, aiPrev) ) break;
    }

    /* Figure out how highly a snippet starting at token offset i scores
    ** according to fts3SnippetScore(). If it is higher than any previously
    ** considered position, save the current position, score and hlmask as 
    ** the best snippet candidate found so far.
    */
    fts3SnippetCnt(i, nSnippet, anCnt, aBuffer, anToken, &hlmask);
    iScore = fts3SnippetScore(nList, anCnt);
    if( iScore>iBestScore ){
      iBestPos = i;
      iBestScore = iScore;
      besthlmask = hlmask;
    }
  }

  sqlite3_free(apList);
  *piPos = iBestPos;
  *pHlmask = besthlmask;
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
    nAppend = (int)strlen(zAppend);
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

static int fts3SnippetText(
  Fts3Cursor *pCsr,               /* FTS3 Cursor */
  const char *zDoc,               /* Document to extract snippet from */
  int nDoc,                       /* Size of zDoc in bytes */
  int nSnippet,                   /* Number of tokens in extracted snippet */
  int iPos,                       /* Index of first document token in snippet */
  u64 hlmask,                     /* Bitmask of terms to highlight in snippet */
  const char *zOpen,              /* String inserted before highlighted term */
  const char *zClose,             /* String inserted after highlighted term */
  const char *zEllipsis,
  char **pzSnippet                /* OUT: Snippet text */
){
  Fts3Table *pTab = (Fts3Table *)pCsr->base.pVtab;
  int rc;                         /* Return code */
  int iCurrent = 0;
  int iStart = 0;
  int iEnd;

  sqlite3_tokenizer_module *pMod; /* Tokenizer module methods object */
  sqlite3_tokenizer_cursor *pC;   /* Tokenizer cursor open on zDoc/nDoc */
  const char *ZDUMMY;             /* Dummy arguments used with tokenizer */
  int DUMMY1, DUMMY2, DUMMY3;     /* Dummy arguments used with tokenizer */

  StrBuffer res = {0, 0, 0};   /* Result string */

  /* Open a token cursor on the document. Read all tokens up to and 
  ** including token iPos (the first token of the snippet). Set variable
  ** iStart to the byte offset in zDoc of the start of token iPos.
  */
  pMod = (sqlite3_tokenizer_module *)pTab->pTokenizer->pModule;
  rc = pMod->xOpen(pTab->pTokenizer, zDoc, nDoc, &pC);
  while( rc==SQLITE_OK && iCurrent<iPos ){
    rc = pMod->xNext(pC, &ZDUMMY, &DUMMY1, &iStart, &DUMMY2, &iCurrent);
  }
  iEnd = iStart;

  if( rc==SQLITE_OK && iStart>0 ){
    rc = fts3StringAppend(&res, zEllipsis, -1);
  }

  while( rc==SQLITE_OK ){
    int iBegin;
    int iFin;
    rc = pMod->xNext(pC, &ZDUMMY, &DUMMY1, &iBegin, &iFin, &iCurrent);

    if( rc==SQLITE_OK ){
      if( iCurrent>=(iPos+nSnippet) ){
        rc = SQLITE_DONE;
      }else{
        iEnd = iFin;
        if( hlmask & ((u64)1 << (iCurrent-iPos)) ){
          if( fts3StringAppend(&res, &zDoc[iStart], iBegin-iStart)
           || fts3StringAppend(&res, zOpen, -1)
           || fts3StringAppend(&res, &zDoc[iBegin], iEnd-iBegin)
           || fts3StringAppend(&res, zClose, -1)
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
    rc = fts3StringAppend(&res, &zDoc[iStart], iEnd-iStart);
    if( rc==SQLITE_OK ){
      rc = pMod->xNext(pC, &ZDUMMY, &DUMMY1, &DUMMY2, &DUMMY3, &iCurrent);
      if( rc==SQLITE_OK ){
        rc = fts3StringAppend(&res, zEllipsis, -1);
      }else if( rc==SQLITE_DONE ){
        rc = fts3StringAppend(&res, &zDoc[iEnd], -1);
      }
    }
  }

  pMod->xClose(pC);
  if( rc!=SQLITE_OK ){
    sqlite3_free(res.z);
  }else{
    *pzSnippet = res.z;
  }
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
    rc = fts3ExprLoadDoclists(pCsr, &nPhrase);
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

void sqlite3Fts3Snippet2(
  sqlite3_context *pCtx,          /* SQLite function call context */
  Fts3Cursor *pCsr,               /* Cursor object */
  const char *zStart,             /* Snippet start text - "<b>" */
  const char *zEnd,               /* Snippet end text - "</b>" */
  const char *zEllipsis,          /* Snippet ellipsis text - "<b>...</b>" */
  int iCol,                       /* Extract snippet from this column */
  int nToken                      /* Approximate number of tokens in snippet */
){
  int rc;
  int iPos = 0;
  u64 hlmask = 0;
  char *z = 0;
  int nDoc;
  const char *zDoc;

  rc = fts3BestSnippet(nToken, pCsr, iCol, &iPos, &hlmask);

  nDoc = sqlite3_column_bytes(pCsr->pStmt, iCol+1);
  zDoc = (const char *)sqlite3_column_text(pCsr->pStmt, iCol+1);

  if( rc==SQLITE_OK ){
    rc = fts3SnippetText(
        pCsr, zDoc, nDoc, nToken, iPos, hlmask, zStart, zEnd, zEllipsis, &z);
  }
  if( rc!=SQLITE_OK ){
    sqlite3_result_error_code(pCtx, rc);
  }else{
    sqlite3_result_text(pCtx, z, -1, sqlite3_free);
  }
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

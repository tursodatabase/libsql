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

#endif

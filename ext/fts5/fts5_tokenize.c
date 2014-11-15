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

#include "fts5.h"


/*
** Create a "simple" tokenizer.
*/
static int fts5SimpleCreate(
  void *pCtx, 
  const char **azArg, int nArg,
  Fts5Tokenizer **ppOut
){
  *ppOut = 0;
  return SQLITE_OK;
}

/*
** Delete a "simple" tokenizer.
*/
static void fts5SimpleDelete(Fts5Tokenizer *p){
  return;
}

/*
** For tokenizers with no "unicode" modifier, the set of token characters
** is the same as the set of ASCII range alphanumeric characters. 
*/
static unsigned char aSimpleTokenChar[128] = {
  0, 0, 0, 0, 0, 0, 0, 0,   0, 0, 0, 0, 0, 0, 0, 0,   /* 0x00..0x0F */
  0, 0, 0, 0, 0, 0, 0, 0,   0, 0, 0, 0, 0, 0, 0, 0,   /* 0x10..0x1F */
  0, 0, 0, 0, 0, 0, 0, 0,   0, 0, 0, 0, 0, 0, 0, 0,   /* 0x20..0x2F */
  1, 1, 1, 1, 1, 1, 1, 1,   1, 1, 0, 0, 0, 0, 0, 0,   /* 0x30..0x3F */
  0, 1, 1, 1, 1, 1, 1, 1,   1, 1, 1, 1, 1, 1, 1, 1,   /* 0x40..0x4F */
  1, 1, 1, 1, 1, 1, 1, 1,   1, 1, 1, 0, 0, 0, 0, 0,   /* 0x50..0x5F */
  0, 1, 1, 1, 1, 1, 1, 1,   1, 1, 1, 1, 1, 1, 1, 1,   /* 0x60..0x6F */
  1, 1, 1, 1, 1, 1, 1, 1,   1, 1, 1, 0, 0, 0, 0, 0,   /* 0x70..0x7F */
};


static void simpleFold(char *aOut, const char *aIn, int nByte){
  int i;
  for(i=0; i<nByte; i++){
    char c = aIn[i];
    if( c>='A' && c<='Z' ) c += 32;
    aOut[i] = c;
  }
}

/*
** Tokenize some text using the simple tokenizer.
*/
static int fts5SimpleTokenize(
  Fts5Tokenizer *pTokenizer,
  void *pCtx,
  const char *pText, int nText,
  int (*xToken)(void*, const char*, int nToken, int iStart, int iEnd, int iPos)
){
  int rc;
  int ie;
  int is = 0;
  int iPos = 0;

  char aFold[64];
  int nFold = sizeof(aFold);
  char *pFold = aFold;

  do {
    int nByte;

    /* Skip any leading divider characters. */
    while( is<nText && ((pText[is]&0x80) || aSimpleTokenChar[pText[is]]==0 ) ){
      is++;
    }
    if( is==nText ) break;

    /* Count the token characters */
    ie = is+1;
    while( ie<nText && ((pText[ie]&0x80)==0 && aSimpleTokenChar[pText[ie]] ) ){
      ie++;
    }

    /* Fold to lower case */
    nByte = ie-is;
    if( nByte>nFold ){
      if( pFold!=aFold ) sqlite3_free(pFold);
      pFold = sqlite3_malloc(nByte*2);
      if( pFold==0 ){
        rc = SQLITE_NOMEM;
        break;
      }
      nFold = nByte*2;
    }
    simpleFold(pFold, &pText[is], nByte);

    /* Invoke the token callback */
    rc = xToken(pCtx, pFold, nByte, is, ie, iPos);
    iPos++;
    is = ie+1;
  }while( is<nText && rc==SQLITE_OK );
  
  if( pFold!=aFold ) sqlite3_free(pFold);
  if( rc==SQLITE_DONE ) rc = SQLITE_OK;
  return rc;
}

/*
** Register all built-in tokenizers with FTS5.
*/
int sqlite3Fts5TokenizerInit(fts5_api *pApi){
  struct BuiltinTokenizer {
    const char *zName;
    void *pUserData;
    fts5_tokenizer x;
  } aBuiltin[] = {
    { "simple", 0, { fts5SimpleCreate, fts5SimpleDelete, fts5SimpleTokenize } }
  };
  
  int rc = SQLITE_OK;             /* Return code */
  int i;                          /* To iterate through builtin functions */

  for(i=0; rc==SQLITE_OK && i<sizeof(aBuiltin)/sizeof(aBuiltin[0]); i++){
    rc = pApi->xCreateTokenizer(pApi,
        aBuiltin[i].zName,
        &aBuiltin[i].pUserData,
        &aBuiltin[i].x,
        0
    );
  }

  return SQLITE_OK;
}



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
#include <string.h>
#include <assert.h>


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

/**************************************************************************
** Start of porter2 stemmer implementation.
*/

/* Any tokens larger than this (in bytes) are passed through without
** stemming. */
#define FTS5_PORTER_MAX_TOKEN 64

typedef struct PorterTokenizer PorterTokenizer;
struct PorterTokenizer {
  fts5_tokenizer tokenizer;       /* Parent tokenizer module */
  Fts5Tokenizer *pTokenizer;      /* Parent tokenizer instance */
  char aBuf[FTS5_PORTER_MAX_TOKEN + 64];
};

/*
** Delete a "porter" tokenizer.
*/
static void fts5PorterDelete(Fts5Tokenizer *pTok){
  if( pTok ){
    PorterTokenizer *p = (PorterTokenizer*)pTok;
    if( p->pTokenizer ){
      p->tokenizer.xDelete(p->pTokenizer);
    }
    sqlite3_free(p);
  }
}

/*
** Create a "porter" tokenizer.
*/
static int fts5PorterCreate(
  void *pCtx, 
  const char **azArg, int nArg,
  Fts5Tokenizer **ppOut
){
  fts5_api *pApi = (fts5_api*)pCtx;
  int rc = SQLITE_OK;
  PorterTokenizer *pRet;
  void *pUserdata = 0;

  pRet = (PorterTokenizer*)sqlite3_malloc(sizeof(PorterTokenizer));
  if( pRet ){
    memset(pRet, 0, sizeof(PorterTokenizer));
    rc = pApi->xFindTokenizer(pApi, "simple", &pUserdata, &pRet->tokenizer);
  }else{
    rc = SQLITE_NOMEM;
  }
  if( rc==SQLITE_OK ){
    rc = pRet->tokenizer.xCreate(pUserdata, 0, 0, &pRet->pTokenizer);
  }

  if( rc!=SQLITE_OK ){
    fts5PorterDelete((Fts5Tokenizer*)pRet);
    pRet = 0;
  }
  *ppOut = (Fts5Tokenizer*)pRet;
  return rc;
}

typedef struct PorterContext PorterContext;
struct PorterContext {
  void *pCtx;
  int (*xToken)(void*, const char*, int, int, int, int);
  char *aBuf;
};

typedef struct PorterRule PorterRule;
struct PorterRule {
  const char *zSuffix;
  int nSuffix;
  int (*xCond)(char *zStem, int nStem);
  const char *zOutput;
  int nOutput;
};

static int fts5PorterApply(char *aBuf, int *pnBuf, PorterRule *aRule){
  int ret = -1;
  int nBuf = *pnBuf;
  PorterRule *p;


  for(p=aRule; p->zSuffix; p++){
    assert( strlen(p->zSuffix)==p->nSuffix );
    assert( strlen(p->zOutput)==p->nOutput );
    if( nBuf<p->nSuffix ) continue;
    if( 0==memcmp(&aBuf[nBuf - p->nSuffix], p->zSuffix, p->nSuffix) ) break;
  }

  if( p->zSuffix ){
    int nStem = nBuf - p->nSuffix;
    if( p->xCond==0 || p->xCond(aBuf, nStem) ){
      memcpy(&aBuf[nStem], p->zOutput, p->nOutput);
      *pnBuf = nStem + p->nOutput;
      ret = p - aRule;
    }
  }

  return ret;
}

static int fts5PorterIsVowel(char c, int bYIsVowel){
  return (
      c=='a' || c=='e' || c=='i' || c=='o' || c=='u' || (bYIsVowel && c=='y')
  );
}

static int fts5PorterGobbleVC(char *zStem, int nStem, int bPrevCons){
  int i;
  int bCons = bPrevCons;

  /* Scan for a vowel */
  for(i=0; i<nStem; i++){
    if( 0==(bCons = !fts5PorterIsVowel(zStem[i], bCons)) ) break;
  }

  /* Scan for a consonent */
  for(i++; i<nStem; i++){
    if( (bCons = !fts5PorterIsVowel(zStem[i], bCons)) ) return i+1;
  }
  return 0;
}

/* porter rule condition: (m > 0) */
static int fts5Porter_MGt0(char *zStem, int nStem){
  return !!fts5PorterGobbleVC(zStem, nStem, 0);
}

/* porter rule condition: (m > 1) */
static int fts5Porter_MGt1(char *zStem, int nStem){
  int n;
  n = fts5PorterGobbleVC(zStem, nStem, 0);
  if( n && fts5PorterGobbleVC(&zStem[n], nStem-n, 1) ){
    return 1;
  }
  return 0;
}

/* porter rule condition: (m = 1) */
static int fts5Porter_MEq1(char *zStem, int nStem){
  int n;
  n = fts5PorterGobbleVC(zStem, nStem, 0);
  if( n && 0==fts5PorterGobbleVC(&zStem[n], nStem-n, 1) ){
    return 1;
  }
  return 0;
}

/* porter rule condition: (*o) */
static int fts5Porter_Ostar(char *zStem, int nStem){
  if( zStem[nStem-1]=='w' || zStem[nStem-1]=='x' || zStem[nStem-1]=='y' ){
    return 0;
  }else{
    int i;
    int mask = 0;
    int bCons = 0;
    for(i=0; i<nStem; i++){
      bCons = !fts5PorterIsVowel(zStem[i], bCons);
      assert( bCons==0 || bCons==1 );
      mask = (mask << 1) + bCons;
    }
    return ((mask & 0x0007)==0x0005);
  }
}

/* porter rule condition: (m > 1 and (*S or *T)) */
static int fts5Porter_MGt1_and_S_or_T(char *zStem, int nStem){
  return nStem>0
      && (zStem[nStem-1]=='s' || zStem[nStem-1]=='t')
      && fts5Porter_MGt1(zStem, nStem);
}

/* porter rule condition: (*v*) */
static int fts5Porter_Vowel(char *zStem, int nStem){
  int i;
  for(i=0; i<nStem; i++){
    if( fts5PorterIsVowel(zStem[i], i>0) ){
      return 1;
    }
  }
  return 0;
}

static int fts5PorterCb(
  void *pCtx, 
  const char *pToken, 
  int nToken, 
  int iStart, 
  int iEnd, 
  int iPos
){
  PorterContext *p = (PorterContext*)pCtx;

  PorterRule aStep1A[] = {
    { "sses", 4,  0, "ss", 2 },
    { "ies",  3,  0, "i",  1  },
    { "ss",   2,  0, "ss", 2 },
    { "s",    1,  0, "",   0 },
    { 0, 0, 0, 0 }
  };

  PorterRule aStep1B[] = {
    { "eed", 3,  fts5Porter_MGt0,  "ee", 2 },
    { "ed",  2,  fts5Porter_Vowel, "",   0 },
    { "ing", 3,  fts5Porter_Vowel, "",   0 },
    { 0, 0, 0, 0 }
  };

  PorterRule aStep1B2[] = {
    { "at", 2,  0, "ate", 3 },
    { "bl", 2,  0, "ble", 3 },
    { "iz", 2,  0, "ize", 3 },
    { 0, 0, 0, 0 }
  };

  PorterRule aStep1C[] = {
    { "y",  1,  fts5Porter_Vowel, "i", 1 },
    { 0, 0, 0, 0 }
  };

  PorterRule aStep2[] = {
    { "ational", 7, fts5Porter_MGt0, "ate", 3}, 
    { "tional", 6, fts5Porter_MGt0, "tion", 4}, 
    { "enci", 4, fts5Porter_MGt0, "ence", 4}, 
    { "anci", 4, fts5Porter_MGt0, "ance", 4}, 
    { "izer", 4, fts5Porter_MGt0, "ize", 3}, 
    { "logi", 4, fts5Porter_MGt0, "log", 3},     /* added post 1979 */
    { "bli", 3, fts5Porter_MGt0, "ble", 3},      /* modified post 1979 */
    { "alli", 4, fts5Porter_MGt0, "al", 2}, 
    { "entli", 5, fts5Porter_MGt0, "ent", 3}, 
    { "eli", 3, fts5Porter_MGt0, "e", 1}, 
    { "ousli", 5, fts5Porter_MGt0, "ous", 3}, 
    { "ization", 7, fts5Porter_MGt0, "ize", 3}, 
    { "ation", 5, fts5Porter_MGt0, "ate", 3}, 
    { "ator", 4, fts5Porter_MGt0, "ate", 3}, 
    { "alism", 5, fts5Porter_MGt0, "al", 2}, 
    { "iveness", 7, fts5Porter_MGt0, "ive", 3}, 
    { "fulness", 7, fts5Porter_MGt0, "ful", 3}, 
    { "ousness", 7, fts5Porter_MGt0, "ous", 3}, 
    { "aliti", 5, fts5Porter_MGt0, "al", 2}, 
    { "iviti", 5, fts5Porter_MGt0, "ive", 3}, 
    { "biliti", 6, fts5Porter_MGt0, "ble", 3}, 
    { 0, 0, 0, 0 }
  };

  PorterRule aStep3[] = {
    { "icate", 5, fts5Porter_MGt0, "ic", 2}, 
    { "ative", 5, fts5Porter_MGt0, "", 0}, 
    { "alize", 5, fts5Porter_MGt0, "al", 2}, 
    { "iciti", 5, fts5Porter_MGt0, "ic", 2}, 
    { "ical", 4, fts5Porter_MGt0, "ic", 2}, 
    { "ful", 3, fts5Porter_MGt0, "", 0}, 
    { "ness", 4, fts5Porter_MGt0, "", 0}, 
    { 0, 0, 0, 0 }
  };

  PorterRule aStep4[] = {
    { "al", 2, fts5Porter_MGt1, "", 0}, 
    { "ance", 4, fts5Porter_MGt1, "", 0}, 
    { "ence", 4, fts5Porter_MGt1, "", 0}, 
    { "er", 2, fts5Porter_MGt1, "", 0}, 
    { "ic", 2, fts5Porter_MGt1, "", 0}, 
    { "able", 4, fts5Porter_MGt1, "", 0}, 
    { "ible", 4, fts5Porter_MGt1, "", 0}, 
    { "ant", 3, fts5Porter_MGt1, "", 0}, 
    { "ement", 5, fts5Porter_MGt1, "", 0}, 
    { "ment", 4, fts5Porter_MGt1, "", 0}, 
    { "ent", 3, fts5Porter_MGt1, "", 0}, 
    { "ion", 3, fts5Porter_MGt1_and_S_or_T, "", 0}, 
    { "ou", 2, fts5Porter_MGt1, "", 0}, 
    { "ism", 3, fts5Porter_MGt1, "", 0}, 
    { "ate", 3, fts5Porter_MGt1, "", 0}, 
    { "iti", 3, fts5Porter_MGt1, "", 0}, 
    { "ous", 3, fts5Porter_MGt1, "", 0}, 
    { "ive", 3, fts5Porter_MGt1, "", 0}, 
    { "ize", 3, fts5Porter_MGt1, "", 0}, 
    { 0, 0, 0, 0 }
  };


  char *aBuf;
  int nBuf;
  int n;

  if( nToken>FTS5_PORTER_MAX_TOKEN || nToken<3 ) goto pass_through;
  aBuf = p->aBuf;
  nBuf = nToken;
  memcpy(aBuf, pToken, nBuf);

  /* Step 1. */
  fts5PorterApply(aBuf, &nBuf, aStep1A);
  n = fts5PorterApply(aBuf, &nBuf, aStep1B);
  if( n==1 || n==2 ){
    if( fts5PorterApply(aBuf, &nBuf, aStep1B2)<0 ){
      char c = aBuf[nBuf-1];
      if( fts5PorterIsVowel(c, 0)==0 
       && c!='l' && c!='s' && c!='z' && c==aBuf[nBuf-2] 
      ){
        nBuf--;
      }else if( fts5Porter_MEq1(aBuf, nBuf) && fts5Porter_Ostar(aBuf, nBuf) ){
        aBuf[nBuf++] = 'e';
      }
    }
  }
  fts5PorterApply(aBuf, &nBuf, aStep1C);

  /* Steps 2 through 4. */
  fts5PorterApply(aBuf, &nBuf, aStep2);
  fts5PorterApply(aBuf, &nBuf, aStep3);
  fts5PorterApply(aBuf, &nBuf, aStep4);

  /* Step 5a. */
  if( nBuf>0 && aBuf[nBuf-1]=='e' ){
    if( fts5Porter_MGt1(aBuf, nBuf-1) 
     || (fts5Porter_MEq1(aBuf, nBuf-1) && !fts5Porter_Ostar(aBuf, nBuf-1))
    ){
      nBuf--;
    }
  }

  /* Step 5b. */
  if( nBuf>1 && aBuf[nBuf-1]=='l' 
   && aBuf[nBuf-2]=='l' && fts5Porter_MGt1(aBuf, nBuf-1) 
  ){
    nBuf--;
  }

  return p->xToken(p->pCtx, aBuf, nBuf, iStart, iEnd, iPos);

 pass_through:
  return p->xToken(p->pCtx, pToken, nToken, iStart, iEnd, iPos);
}

/*
** Tokenize using the porter tokenizer.
*/
static int fts5PorterTokenize(
  Fts5Tokenizer *pTokenizer,
  void *pCtx,
  const char *pText, int nText,
  int (*xToken)(void*, const char*, int nToken, int iStart, int iEnd, int iPos)
){
  PorterTokenizer *p = (PorterTokenizer*)pTokenizer;
  PorterContext sCtx;
  sCtx.xToken = xToken;
  sCtx.pCtx = pCtx;
  sCtx.aBuf = p->aBuf;
  return p->tokenizer.xTokenize(
      p->pTokenizer, (void*)&sCtx, pText, nText, fts5PorterCb
  );
}

/*
** Register all built-in tokenizers with FTS5.
*/
int sqlite3Fts5TokenizerInit(fts5_api *pApi){
  struct BuiltinTokenizer {
    const char *zName;
    fts5_tokenizer x;
  } aBuiltin[] = {
    { "porter",  { fts5PorterCreate, fts5PorterDelete, fts5PorterTokenize } },
    { "simple",  { fts5SimpleCreate, fts5SimpleDelete, fts5SimpleTokenize } }
  };
  
  int rc = SQLITE_OK;             /* Return code */
  int i;                          /* To iterate through builtin functions */

  for(i=0; rc==SQLITE_OK && i<sizeof(aBuiltin)/sizeof(aBuiltin[0]); i++){
    rc = pApi->xCreateTokenizer(pApi,
        aBuiltin[i].zName,
        (void*)pApi,
        &aBuiltin[i].x,
        0
    );
  }

  return SQLITE_OK;
}



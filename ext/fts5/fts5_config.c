/*
** 2014 Jun 09
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
******************************************************************************
**
** This is an SQLite module implementing full-text search.
*/

#include "fts5Int.h"

/*
** Convert an SQL-style quoted string into a normal string by removing
** the quote characters.  The conversion is done in-place.  If the
** input does not begin with a quote character, then this routine
** is a no-op.
**
** Examples:
**
**     "abc"   becomes   abc
**     'xyz'   becomes   xyz
**     [pqr]   becomes   pqr
**     `mno`   becomes   mno
*/
void sqlite3Fts5Dequote(char *z){
  char quote;                     /* Quote character (if any ) */

  quote = z[0];
  if( quote=='[' || quote=='\'' || quote=='"' || quote=='`' ){
    int iIn = 1;                  /* Index of next byte to read from input */
    int iOut = 0;                 /* Index of next byte to write to output */

    /* If the first byte was a '[', then the close-quote character is a ']' */
    if( quote=='[' ) quote = ']';  

    while( ALWAYS(z[iIn]) ){
      if( z[iIn]==quote ){
        if( z[iIn+1]!=quote ) break;
        z[iOut++] = quote;
        iIn += 2;
      }else{
        z[iOut++] = z[iIn++];
      }
    }
    z[iOut] = '\0';
  }
}

/*
** Parse the "special" CREATE VIRTUAL TABLE directive and update
** configuration object pConfig as appropriate.
**
** If successful, object pConfig is updated and SQLITE_OK returned. If
** an error occurs, an SQLite error code is returned and an error message
** may be left in *pzErr. It is the responsibility of the caller to
** eventually free any such error message using sqlite3_free().
*/
static int fts5ConfigParseSpecial(
  Fts5Config *pConfig,            /* Configuration object to update */
  char *zCmd,                     /* Special command to parse */
  char *zArg,                     /* Argument to parse */
  char **pzErr                    /* OUT: Error message */
){
  if( sqlite3_stricmp(zCmd, "prefix")==0 ){
    char *p;
    if( pConfig->aPrefix ){
      *pzErr = sqlite3_mprintf("multiple prefix=... directives");
      return SQLITE_ERROR;
    }
    pConfig->aPrefix = sqlite3_malloc(sizeof(int) * FTS5_MAX_PREFIX_INDEXES);
    p = zArg;
    while( p[0] ){
      int nPre = 0;
      while( p[0]==' ' ) p++;
      while( p[0]>='0' && p[0]<='9' && nPre<1000 ){
        nPre = nPre*10 + (p[0] - '0');
        p++;
      }
      while( p[0]==' ' ) p++;
      if( p[0]==',' ){
        p++;
      }else if( p[0] ){
        *pzErr = sqlite3_mprintf("malformed prefix=... directive");
        return SQLITE_ERROR;
      }
      if( nPre==0 || nPre>=1000 ){
        *pzErr = sqlite3_mprintf("prefix length out of range: %d", nPre);
        return SQLITE_ERROR;
      }
      pConfig->aPrefix[pConfig->nPrefix] = nPre;
      pConfig->nPrefix++;
    }
    return SQLITE_OK;
  }

  *pzErr = sqlite3_mprintf("unrecognized directive: \"%s\"", zCmd);
  return SQLITE_ERROR;
}

/*
** Duplicate the string passed as the only argument into a buffer allocated
** by sqlite3_malloc().
**
** Return 0 if an OOM error is encountered.
*/
static char *fts5Strdup(const char *z){
  return sqlite3_mprintf("%s", z);
}

void sqlite3Fts3SimpleTokenizerModule(sqlite3_tokenizer_module const**);

/*
** Allocate an instance of the default tokenizer ("simple") at 
** Fts5Config.pTokenizer. Return SQLITE_OK if successful, or an SQLite error
** code if an error occurs.
*/
static int fts5ConfigDefaultTokenizer(Fts5Config *pConfig){
  const sqlite3_tokenizer_module *pMod; /* Tokenizer module "simple" */
  sqlite3_tokenizer *pTokenizer;  /* Tokenizer instance */
  int rc;                         /* Return code */

  sqlite3Fts3SimpleTokenizerModule(&pMod);
  rc = pMod->xCreate(0, 0, &pTokenizer);
  if( rc==SQLITE_OK ){
    pTokenizer->pModule = pMod;
    pConfig->pTokenizer = pTokenizer;
  }

  return rc;
}

/*
** Arguments nArg/azArg contain the string arguments passed to the xCreate
** or xConnect method of the virtual table. This function attempts to 
** allocate an instance of Fts5Config containing the results of parsing
** those arguments.
**
** If successful, SQLITE_OK is returned and *ppOut is set to point to the
** new Fts5Config object. If an error occurs, an SQLite error code is 
** returned, *ppOut is set to NULL and an error message may be left in
** *pzErr. It is the responsibility of the caller to eventually free any 
** such error message using sqlite3_free().
*/
int sqlite3Fts5ConfigParse(
  sqlite3 *db,
  int nArg,                       /* Number of arguments */
  const char **azArg,             /* Array of nArg CREATE VIRTUAL TABLE args */
  Fts5Config **ppOut,             /* OUT: Results of parse */
  char **pzErr                    /* OUT: Error message */
){
  int rc = SQLITE_OK;             /* Return code */
  Fts5Config *pRet;               /* New object to return */

  *ppOut = pRet = (Fts5Config*)sqlite3_malloc(sizeof(Fts5Config));
  if( pRet==0 ) return SQLITE_NOMEM;
  memset(pRet, 0, sizeof(Fts5Config));
  pRet->db = db;

  pRet->azCol = (char**)sqlite3_malloc(sizeof(char*) * nArg);
  pRet->zDb = fts5Strdup(azArg[1]);
  pRet->zName = fts5Strdup(azArg[2]);
  if( sqlite3_stricmp(pRet->zName, FTS5_RANK_NAME)==0 ){
    *pzErr = sqlite3_mprintf("reserved fts5 table name: %s", pRet->zName);
    rc = SQLITE_ERROR;
  }else if( pRet->azCol==0 || pRet->zDb==0 || pRet->zName==0 ){
    rc = SQLITE_NOMEM;
  }else{
    int i;
    for(i=3; rc==SQLITE_OK && i<nArg; i++){
      char *zDup = fts5Strdup(azArg[i]);
      if( zDup==0 ){
        rc = SQLITE_NOMEM;
      }else{

        /* Check if this is a special directive - "cmd=arg" */
        if( zDup[0]!='"' && zDup[0]!='\'' && zDup[0]!='[' && zDup[0]!='`' ){
          char *p = zDup;
          while( *p && *p!='=' ) p++;
          if( *p ){
            char *zArg = &p[1];
            *p = '\0';
            sqlite3Fts5Dequote(zArg);
            rc = fts5ConfigParseSpecial(pRet, zDup, zArg, pzErr);
            sqlite3_free(zDup);
            zDup = 0;
          }
        }

        /* If it is not a special directive, it must be a column name. In
        ** this case, check that it is not the reserved column name "rank". */
        if( zDup ){
          sqlite3Fts5Dequote(zDup);
          pRet->azCol[pRet->nCol++] = zDup;
          if( sqlite3_stricmp(zDup, FTS5_RANK_NAME)==0 ){
            *pzErr = sqlite3_mprintf("reserved fts5 column name: %s", zDup);
            rc = SQLITE_ERROR;
          }
        }
      }
    }
  }

  if( rc==SQLITE_OK && pRet->pTokenizer==0 ){
    rc = fts5ConfigDefaultTokenizer(pRet);
  }

  if( rc!=SQLITE_OK ){
    sqlite3Fts5ConfigFree(pRet);
    *ppOut = 0;
  }
  return rc;
}

/*
** Free the configuration object passed as the only argument.
*/
void sqlite3Fts5ConfigFree(Fts5Config *pConfig){
  if( pConfig ){
    int i;
    if( pConfig->pTokenizer ){
      pConfig->pTokenizer->pModule->xDestroy(pConfig->pTokenizer);
    }
    sqlite3_free(pConfig->zDb);
    sqlite3_free(pConfig->zName);
    for(i=0; i<pConfig->nCol; i++){
      sqlite3_free(pConfig->azCol[i]);
    }
    sqlite3_free(pConfig->azCol);
    sqlite3_free(pConfig->aPrefix);
    sqlite3_free(pConfig);
  }
}

/*
** Call sqlite3_declare_vtab() based on the contents of the configuration
** object passed as the only argument. Return SQLITE_OK if successful, or
** an SQLite error code if an error occurs.
*/
int sqlite3Fts5ConfigDeclareVtab(Fts5Config *pConfig){
  int i;
  int rc;
  char *zSql;
  char *zOld;

  zSql = (char*)sqlite3_mprintf("CREATE TABLE x(");
  for(i=0; zSql && i<pConfig->nCol; i++){
    zOld = zSql;
    zSql = sqlite3_mprintf("%s%s%Q", zOld, (i==0?"":", "), pConfig->azCol[i]);
    sqlite3_free(zOld);
  }

  if( zSql ){
    zOld = zSql;
    zSql = sqlite3_mprintf("%s, %Q HIDDEN, %s HIDDEN)", 
        zOld, pConfig->zName, FTS5_RANK_NAME
    );
    sqlite3_free(zOld);
  }

  if( zSql==0 ){
    rc = SQLITE_NOMEM;
  }else{
    rc = sqlite3_declare_vtab(pConfig->db, zSql);
    sqlite3_free(zSql);
  }
  
  return rc;
}

/*
** Tokenize the text passed via the second and third arguments.
**
** The callback is invoked once for each token in the input text. The
** arguments passed to it are, in order:
**
**     void *pCtx          // Copy of 4th argument to sqlite3Fts5Tokenize()
**     const char *pToken  // Pointer to buffer containing token
**     int nToken          // Size of token in bytes
**     int iStart          // Byte offset of start of token within input text
**     int iEnd            // Byte offset of end of token within input text
**     int iPos            // Position of token in input (first token is 0)
**
** If the callback returns a non-zero value the tokenization is abandoned
** and no further callbacks are issued. 
**
** This function returns SQLITE_OK if successful or an SQLite error code
** if an error occurs. If the tokenization was abandoned early because
** the callback returned SQLITE_DONE, this is not an error and this function
** still returns SQLITE_OK. Or, if the tokenization was abandoned early
** because the callback returned another non-zero value, it is assumed
** to be an SQLite error code and returned to the caller.
*/
int sqlite3Fts5Tokenize(
  Fts5Config *pConfig,            /* FTS5 Configuration object */
  const char *pText, int nText,   /* Text to tokenize */
  void *pCtx,                     /* Context passed to xToken() */
  int (*xToken)(void*, const char*, int, int, int, int)    /* Callback */
){
  const sqlite3_tokenizer_module *pMod = pConfig->pTokenizer->pModule;
  sqlite3_tokenizer_cursor *pCsr = 0;
  int rc;

  rc = pMod->xOpen(pConfig->pTokenizer, pText, nText, &pCsr);
  assert( rc==SQLITE_OK || pCsr==0 );
  if( rc==SQLITE_OK ){
    const char *pToken;           /* Pointer to token buffer */
    int nToken;                   /* Size of token in bytes */
    int iStart, iEnd, iPos;       /* Start, end and position of token */
    pCsr->pTokenizer = pConfig->pTokenizer;
    for(rc = pMod->xNext(pCsr, &pToken, &nToken, &iStart, &iEnd, &iPos);
        rc==SQLITE_OK;
        rc = pMod->xNext(pCsr, &pToken, &nToken, &iStart, &iEnd, &iPos)
    ){
      if( (rc = xToken(pCtx, pToken, nToken, iStart, iEnd, iPos)) ) break;
    }
    if( rc==SQLITE_DONE ) rc = SQLITE_OK;
    pMod->xClose(pCsr);
  }
  return rc;
}



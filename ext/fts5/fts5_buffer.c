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

int sqlite3Fts5BufferSize(int *pRc, Fts5Buffer *pBuf, int nByte){
  int nNew = pBuf->nSpace ? pBuf->nSpace*2 : 64;
  u8 *pNew;
  while( nNew<nByte ){
    nNew = nNew * 2;
  }
  pNew = sqlite3_realloc(pBuf->p, nNew);
  if( pNew==0 ){
    *pRc = SQLITE_NOMEM;
    return 1;
  }else{
    pBuf->nSpace = nNew;
    pBuf->p = pNew;
  }
  return 0;
}


/*
** Encode value iVal as an SQLite varint and append it to the buffer object
** pBuf. If an OOM error occurs, set the error code in p.
*/
void sqlite3Fts5BufferAppendVarint(int *pRc, Fts5Buffer *pBuf, i64 iVal){
  if( fts5BufferGrow(pRc, pBuf, 9) ) return;
  pBuf->n += sqlite3Fts5PutVarint(&pBuf->p[pBuf->n], iVal);
}

void sqlite3Fts5Put32(u8 *aBuf, int iVal){
  aBuf[0] = (iVal>>24) & 0x00FF;
  aBuf[1] = (iVal>>16) & 0x00FF;
  aBuf[2] = (iVal>> 8) & 0x00FF;
  aBuf[3] = (iVal>> 0) & 0x00FF;
}

int sqlite3Fts5Get32(const u8 *aBuf){
  return (aBuf[0] << 24) + (aBuf[1] << 16) + (aBuf[2] << 8) + aBuf[3];
}

/*
** Append buffer nData/pData to buffer pBuf. If an OOM error occurs, set 
** the error code in p. If an error has already occurred when this function
** is called, it is a no-op.
*/
void sqlite3Fts5BufferAppendBlob(
  int *pRc,
  Fts5Buffer *pBuf, 
  int nData, 
  const u8 *pData
){
  assert( *pRc || nData>=0 );
  if( fts5BufferGrow(pRc, pBuf, nData) ) return;
  memcpy(&pBuf->p[pBuf->n], pData, nData);
  pBuf->n += nData;
}

/*
** Append the nul-terminated string zStr to the buffer pBuf. This function
** ensures that the byte following the buffer data is set to 0x00, even 
** though this byte is not included in the pBuf->n count.
*/
void sqlite3Fts5BufferAppendString(
  int *pRc,
  Fts5Buffer *pBuf, 
  const char *zStr
){
  int nStr = (int)strlen(zStr);
  sqlite3Fts5BufferAppendBlob(pRc, pBuf, nStr+1, (const u8*)zStr);
  pBuf->n--;
}

/*
** Argument zFmt is a printf() style format string. This function performs
** the printf() style processing, then appends the results to buffer pBuf.
**
** Like sqlite3Fts5BufferAppendString(), this function ensures that the byte 
** following the buffer data is set to 0x00, even though this byte is not
** included in the pBuf->n count.
*/ 
void sqlite3Fts5BufferAppendPrintf(
  int *pRc,
  Fts5Buffer *pBuf, 
  char *zFmt, ...
){
  if( *pRc==SQLITE_OK ){
    char *zTmp;
    va_list ap;
    va_start(ap, zFmt);
    zTmp = sqlite3_vmprintf(zFmt, ap);
    va_end(ap);

    if( zTmp==0 ){
      *pRc = SQLITE_NOMEM;
    }else{
      sqlite3Fts5BufferAppendString(pRc, pBuf, zTmp);
      sqlite3_free(zTmp);
    }
  }
}

char *sqlite3Fts5Mprintf(int *pRc, const char *zFmt, ...){
  char *zRet = 0;
  if( *pRc==SQLITE_OK ){
    va_list ap;
    va_start(ap, zFmt);
    zRet = sqlite3_vmprintf(zFmt, ap);
    va_end(ap);
    if( zRet==0 ){
      *pRc = SQLITE_NOMEM; 
    }
  }
  return zRet;
}
 

/*
** Free any buffer allocated by pBuf. Zero the structure before returning.
*/
void sqlite3Fts5BufferFree(Fts5Buffer *pBuf){
  sqlite3_free(pBuf->p);
  memset(pBuf, 0, sizeof(Fts5Buffer));
}

/*
** Zero the contents of the buffer object. But do not free the associated 
** memory allocation.
*/
void sqlite3Fts5BufferZero(Fts5Buffer *pBuf){
  pBuf->n = 0;
}

/*
** Set the buffer to contain nData/pData. If an OOM error occurs, leave an
** the error code in p. If an error has already occurred when this function
** is called, it is a no-op.
*/
void sqlite3Fts5BufferSet(
  int *pRc,
  Fts5Buffer *pBuf, 
  int nData, 
  const u8 *pData
){
  pBuf->n = 0;
  sqlite3Fts5BufferAppendBlob(pRc, pBuf, nData, pData);
}

int sqlite3Fts5PoslistNext64(
  const u8 *a, int n,             /* Buffer containing poslist */
  int *pi,                        /* IN/OUT: Offset within a[] */
  i64 *piOff                      /* IN/OUT: Current offset */
){
  int i = *pi;
  if( i>=n ){
    /* EOF */
    *piOff = -1;
    return 1;  
  }else{
    i64 iOff = *piOff;
    int iVal;
    fts5FastGetVarint32(a, i, iVal);
    if( iVal==1 ){
      fts5FastGetVarint32(a, i, iVal);
      iOff = ((i64)iVal) << 32;
      fts5FastGetVarint32(a, i, iVal);
    }
    *piOff = iOff + (iVal-2);
    *pi = i;
    return 0;
  }
}


/*
** Advance the iterator object passed as the only argument. Return true
** if the iterator reaches EOF, or false otherwise.
*/
int sqlite3Fts5PoslistReaderNext(Fts5PoslistReader *pIter){
  if( sqlite3Fts5PoslistNext64(pIter->a, pIter->n, &pIter->i, &pIter->iPos) ){
    pIter->bEof = 1;
  }
  return pIter->bEof;
}

int sqlite3Fts5PoslistReaderInit(
  const u8 *a, int n,             /* Poslist buffer to iterate through */
  Fts5PoslistReader *pIter        /* Iterator object to initialize */
){
  memset(pIter, 0, sizeof(*pIter));
  pIter->a = a;
  pIter->n = n;
  sqlite3Fts5PoslistReaderNext(pIter);
  return pIter->bEof;
}

int sqlite3Fts5PoslistWriterAppend(
  Fts5Buffer *pBuf, 
  Fts5PoslistWriter *pWriter,
  i64 iPos
){
  static const i64 colmask = ((i64)(0x7FFFFFFF)) << 32;
  int rc = SQLITE_OK;
  if( 0==fts5BufferGrow(&rc, pBuf, 5+5+5) ){
    if( (iPos & colmask) != (pWriter->iPrev & colmask) ){
      pBuf->p[pBuf->n++] = 1;
      pBuf->n += sqlite3Fts5PutVarint(&pBuf->p[pBuf->n], (iPos>>32));
      pWriter->iPrev = (iPos & colmask);
    }
    pBuf->n += sqlite3Fts5PutVarint(&pBuf->p[pBuf->n], (iPos-pWriter->iPrev)+2);
    pWriter->iPrev = iPos;
  }
  return rc;
}

void *sqlite3Fts5MallocZero(int *pRc, int nByte){
  void *pRet = 0;
  if( *pRc==SQLITE_OK ){
    pRet = sqlite3_malloc(nByte);
    if( pRet==0 && nByte>0 ){
      *pRc = SQLITE_NOMEM;
    }else{
      memset(pRet, 0, nByte);
    }
  }
  return pRet;
}

/*
** Return a nul-terminated copy of the string indicated by pIn. If nIn
** is non-negative, then it is the length of the string in bytes. Otherwise,
** the length of the string is determined using strlen().
**
** It is the responsibility of the caller to eventually free the returned
** buffer using sqlite3_free(). If an OOM error occurs, NULL is returned. 
*/
char *sqlite3Fts5Strndup(int *pRc, const char *pIn, int nIn){
  char *zRet = 0;
  if( *pRc==SQLITE_OK ){
    if( nIn<0 ){
      nIn = (int)strlen(pIn);
    }
    zRet = (char*)sqlite3_malloc(nIn+1);
    if( zRet ){
      memcpy(zRet, pIn, nIn);
      zRet[nIn] = '\0';
    }else{
      *pRc = SQLITE_NOMEM;
    }
  }
  return zRet;
}


/*
** Return true if character 't' may be part of an FTS5 bareword, or false
** otherwise. Characters that may be part of barewords:
**
**   * All non-ASCII characters,
**   * The 52 upper and lower case ASCII characters, and
**   * The 10 integer ASCII characters.
**   * The underscore character "_" (0x5F).
**   * The unicode "subsitute" character (0x1A).
*/
int sqlite3Fts5IsBareword(char t){
  u8 aBareword[128] = {
    0, 0, 0, 0, 0, 0, 0, 0,    0, 0, 0, 0, 0, 0, 0, 0,   /* 0x00 .. 0x0F */
    0, 0, 0, 0, 0, 0, 0, 0,    0, 0, 1, 0, 0, 0, 0, 0,   /* 0x10 .. 0x1F */
    0, 0, 0, 0, 0, 0, 0, 0,    0, 0, 0, 0, 0, 0, 0, 0,   /* 0x20 .. 0x2F */
    1, 1, 1, 1, 1, 1, 1, 1,    1, 1, 0, 0, 0, 0, 0, 0,   /* 0x30 .. 0x3F */
    0, 1, 1, 1, 1, 1, 1, 1,    1, 1, 1, 1, 1, 1, 1, 1,   /* 0x40 .. 0x4F */
    1, 1, 1, 1, 1, 1, 1, 1,    1, 1, 1, 0, 0, 0, 0, 1,   /* 0x50 .. 0x5F */
    0, 1, 1, 1, 1, 1, 1, 1,    1, 1, 1, 1, 1, 1, 1, 1,   /* 0x60 .. 0x6F */
    1, 1, 1, 1, 1, 1, 1, 1,    1, 1, 1, 0, 0, 0, 0, 0    /* 0x70 .. 0x7F */
  };

  return (t & 0x80) || aBareword[(int)t];
}



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

int sqlite3Fts5BufferGrow(int *pRc, Fts5Buffer *pBuf, int nByte){
  /* A no-op if an error has already occurred */
  if( *pRc ) return 1;

  if( (pBuf->n + nByte) > pBuf->nSpace ){
    u8 *pNew;
    int nNew = pBuf->nSpace ? pBuf->nSpace*2 : 64;
    while( nNew<(pBuf->n + nByte) ){
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
  }
  return 0;
}

/*
** Encode value iVal as an SQLite varint and append it to the buffer object
** pBuf. If an OOM error occurs, set the error code in p.
*/
void sqlite3Fts5BufferAppendVarint(int *pRc, Fts5Buffer *pBuf, i64 iVal){
  if( sqlite3Fts5BufferGrow(pRc, pBuf, 9) ) return;
  pBuf->n += sqlite3PutVarint(&pBuf->p[pBuf->n], iVal);
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
  if( sqlite3Fts5BufferGrow(pRc, pBuf, nData) ) return;
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
  int nStr = strlen(zStr);
  if( sqlite3Fts5BufferGrow(pRc, pBuf, nStr+1) ) return;
  sqlite3Fts5BufferAppendBlob(pRc, pBuf, nStr, (const u8*)zStr);
  if( *pRc==SQLITE_OK ) pBuf->p[pBuf->n] = 0x00;
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


/*
** Advance the iterator object passed as the only argument. Return true
** if the iterator reaches EOF, or false otherwise.
*/
int sqlite3Fts5PoslistReaderNext(Fts5PoslistReader *pIter){
  if( pIter->i>=pIter->n ){
    pIter->bEof = 1;
  }else{
    int iVal;
    pIter->i += getVarint32(&pIter->a[pIter->i], iVal);
    if( iVal==1 ){
      pIter->i += getVarint32(&pIter->a[pIter->i], iVal);
      if( pIter->iCol>=0 && iVal>pIter->iCol ){
        pIter->bEof = 1;
      }else{
        pIter->iPos = ((u64)iVal << 32);
        pIter->i += getVarint32(&pIter->a[pIter->i], iVal);
      }
    }
    pIter->iPos += (iVal-2);
  }
  return pIter->bEof;
}

int sqlite3Fts5PoslistReaderInit(
  int iCol,                       /* If (iCol>=0), this column only */
  const u8 *a, int n,             /* Poslist buffer to iterate through */
  Fts5PoslistReader *pIter        /* Iterator object to initialize */
){
  memset(pIter, 0, sizeof(*pIter));
  pIter->a = a;
  pIter->n = n;
  pIter->iCol = iCol;
  do {
    sqlite3Fts5PoslistReaderNext(pIter);
  }while( pIter->bEof==0 && (pIter->iPos >> 32)<iCol );
  return pIter->bEof;
}

int sqlite3Fts5PoslistWriterAppend(
  Fts5Buffer *pBuf, 
  Fts5PoslistWriter *pWriter,
  i64 iPos
){
  int rc = SQLITE_OK;
  int iCol = (int)(iPos >> 32);
  int iOff = (iPos & 0x7FFFFFFF);

  if( iCol!=pWriter->iCol ){
    fts5BufferAppendVarint(&rc, pBuf, 1);
    fts5BufferAppendVarint(&rc, pBuf, iCol);
    pWriter->iCol = iCol;
    pWriter->iOff = 0;
  }
  fts5BufferAppendVarint(&rc, pBuf, (iOff - pWriter->iOff) + 2);

  return rc;
}

int sqlite3Fts5PoslistNext(
  const u8 *a, int n,             /* Buffer containing poslist */
  int *pi,                        /* IN/OUT: Offset within a[] */
  int *piCol,                     /* IN/OUT: Current column */
  int *piOff                      /* IN/OUT: Current token offset */
){
  int i = *pi;
  int iVal;
  if( i>=n ){
    /* EOF */
    return 1;  
  }
  i += getVarint32(&a[i], iVal);
  if( iVal==1 ){
    i += getVarint32(&a[i], iVal);
    *piCol = iVal;
    *piOff = 0;
    i += getVarint32(&a[i], iVal);
  }
  *piOff += (iVal-2);
  *pi = i;
  return 0;
}



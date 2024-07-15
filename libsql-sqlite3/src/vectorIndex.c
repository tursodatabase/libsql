/*
** 2024-03-18
**
** Copyright 2024 the libSQL authors
**
** Permission is hereby granted, free of charge, to any person obtaining a copy of
** this software and associated documentation files (the "Software"), to deal in
** the Software without restriction, including without limitation the rights to
** use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
** the Software, and to permit persons to whom the Software is furnished to do so,
** subject to the following conditions:
**
** The above copyright notice and this permission notice shall be included in all
** copies or substantial portions of the Software.
**
** THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
** IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
** FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
** COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
** IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
** CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
**
******************************************************************************
**
** libSQL vector search.
*/
#ifndef SQLITE_OMIT_VECTOR
#include "sqlite3.h"
#include "vdbeInt.h"
#include "sqliteInt.h"
#include "vectorIndexInt.h"

/**************************************************************************
** VectorIdxParams utilities
****************************************************************************/

void vectorIdxParamsInit(VectorIdxParams *pParams, u8 *pBinBuf, int nBinSize) {
  assert( nBinSize <= VECTOR_INDEX_PARAMS_BUF_SIZE );

  pParams->nBinSize = nBinSize;
  if( pBinBuf != NULL ){
    memcpy(pParams->pBinBuf, pBinBuf, nBinSize);
  }
}

u64 vectorIdxParamsGetU64(const VectorIdxParams *pParams, char tag) {
  int i, offset;
  u64 value = 0;
  for (i = 0; i + 9 <= pParams->nBinSize; i += 9){
    if( pParams->pBinBuf[i] != tag ){
      continue;
    }
    // choose latest value from the VectorIdxParams bin
    value = 0;
    for(offset = 0; offset < 8; offset++){
      value |= ((u64)(pParams->pBinBuf[i + 1 + offset]) << (u64)(8 * offset));
    }
  }
  return value;
}

int vectorIdxParamsPutU64(VectorIdxParams *pParams, char tag, u64 value) {
  int i;
  if( pParams->nBinSize + 9 > VECTOR_INDEX_PARAMS_BUF_SIZE ){
    return -1;
  }
  pParams->pBinBuf[pParams->nBinSize++] = tag;
  for(i = 0; i < 8; i++){
    pParams->pBinBuf[pParams->nBinSize++] = value & 0xff;
    value >>= 8;
  }
  return 0;
}

double vectorIdxParamsGetF64(const VectorIdxParams *pParams, char tag) {
  u64 value = vectorIdxParamsGetU64(pParams, tag);
  return *((double*)&value);
}

int vectorIdxParamsPutF64(VectorIdxParams *pParams, char tag, double value) {
  return vectorIdxParamsPutU64(pParams, tag, *((u64*)&value));
}

/**************************************************************************
** VectorIdxKey utilities
****************************************************************************/

int vectorIdxKeyGet(Table *pTable, VectorIdxKey *pKey, const char **pzErrMsg) {
  int i;
  Index *pPk;
  // we actually need to change strategy here and use PK if it's available and fallback to ROWID only if there is no other choice
  // will change this later as it must be done carefully in order to not brake behaviour of existing indices
  if( !HasRowid(pTable) ){
    pPk = sqlite3PrimaryKeyIndex(pTable);
    if( pPk->nKeyCol > VECTOR_INDEX_MAX_KEY_COLUMNS ){
      *pzErrMsg = "exceeded limit for composite columns in primary key index";
      return -1;
    }
    pKey->nKeyColumns = pPk->nKeyCol;
    for(i = 0; i < pPk->nKeyCol; i++){
      pKey->aKeyAffinity[i] = pTable->aCol[pPk->aiColumn[i]].affinity;
      pKey->azKeyCollation[i] = pPk->azColl[i];
    }
  } else{
    pKey->nKeyColumns = 1;
    pKey->aKeyAffinity[0] = SQLITE_AFF_INTEGER;
    pKey->azKeyCollation[0] = "BINARY";
  }
  return 0;
}

int vectorIdxKeyDefsRender(const VectorIdxKey *pKey, const char *prefix, char *pBuf, int nBufSize) {
  static const char * const azType[] = {
    /* SQLITE_AFF_BLOB    */ " BLOB",
    /* SQLITE_AFF_TEXT    */ " TEXT",
    /* SQLITE_AFF_NUMERIC */ " NUMERIC",
    /* SQLITE_AFF_INTEGER */ " INTEGER",
    /* SQLITE_AFF_REAL    */ " REAL",
    /* SQLITE_AFF_FLEXNUM */ " NUMERIC",
  };
  int i, size;
  for(i = 0; i < pKey->nKeyColumns && nBufSize > 0; i++){
    const char *collation = pKey->azKeyCollation[i];
    if( sqlite3_strnicmp(collation, "BINARY", 6) == 0 ){
      collation = "";
    }
    if( i == 0 ){
      size = snprintf(pBuf, nBufSize, "%s %s %s", prefix, azType[pKey->aKeyAffinity[i] - SQLITE_AFF_BLOB], collation);
    }else {
      size = snprintf(pBuf, nBufSize, ",%s%d %s %s", prefix, i, azType[pKey->aKeyAffinity[i] - SQLITE_AFF_BLOB], collation);
    }
    if( size < 0 ){
      return -1;
    }
    pBuf += size;
    nBufSize -= size;
  }
  if( nBufSize <= 0 ){
    return -1;
  }
  return 0;
}

int vectorIdxKeyNamesRender(int nKeyColumns, const char *prefix, char *pBuf, int nBufSize) {
  int i, size;
  for(i = 0; i < nKeyColumns && nBufSize > 0; i++){
    if( i == 0 ){
      size = snprintf(pBuf, nBufSize, "%s", prefix);
    }else {
      size = snprintf(pBuf, nBufSize, ",%s%d", prefix, i);
    }
    if( size < 0 ){
      return -1;
    }
    pBuf += size;
    nBufSize -= size;
  }
  if( nBufSize <= 0 ){
    return -1;
  }
  return 0;
}

/**************************************************************************
** VectorInRow utilities
****************************************************************************/

sqlite3_value* vectorInRowKey(const VectorInRow *pVectorInRow, int iKey) {
  assert( 0 <= iKey && iKey < pVectorInRow->nKeys );
  return pVectorInRow->pKeyValues + iKey;
}

i64 vectorInRowLegacyId(const VectorInRow *pVectorInRow) {
  if( pVectorInRow->nKeys == 1 && sqlite3_value_type(pVectorInRow->pKeyValues + 0) == SQLITE_INTEGER ){
    return sqlite3_value_int64(pVectorInRow->pKeyValues);
  }
  return 0;
}

int vectorInRowTryGetRowid(const VectorInRow *pVectorInRow, u64 *nRowid) {
  if( pVectorInRow->nKeys != 1 ){
    return -1;
  }
  if( sqlite3_value_type(vectorInRowKey(pVectorInRow, 0)) != SQLITE_INTEGER ){
    return -1;
  }
  *nRowid = sqlite3_value_int64(vectorInRowKey(pVectorInRow, 0));
  return 0;
}

int vectorInRowPlaceholderRender(const VectorInRow *pVectorInRow, char *pBuf, int nBufSize) {
  int i;
  assert( pVectorInRow->nKeys > 0 );
  if( nBufSize < 2 * pVectorInRow->nKeys ){
    return -1;
  }
  for(i = 0; i < pVectorInRow->nKeys; i++){
    *(pBuf++) = '?';
    *(pBuf++) = ',';
  }
  *(pBuf - 1) = '\0';
  return 0;
}

int vectorInRowAlloc(sqlite3 *db, const UnpackedRecord *pRecord, VectorInRow *pVectorInRow, char **pzErrMsg) {
  int rc = SQLITE_OK;
  int type, dims;
  struct sqlite3_value *pVectorValue = pRecord->aMem + 0;
  pVectorInRow->pKeyValues = pRecord->aMem + 1;
  pVectorInRow->nKeys = pRecord->nField - 1;
  pVectorInRow->pVector = NULL;

  if( pVectorInRow->nKeys <= 0 ){
    rc = SQLITE_ERROR;
    goto out;  
  }

  if( sqlite3_value_type(pVectorValue)==SQLITE_NULL ){
    rc = SQLITE_OK;
    goto out;
  }

  if( detectVectorParameters(pVectorValue, VECTOR_TYPE_FLOAT32, &type, &dims, pzErrMsg) != 0 ){
    rc = SQLITE_ERROR;
    goto out;
  }

  pVectorInRow->pVector = vectorAlloc(type, dims);
  if( pVectorInRow->pVector == NULL ){
    rc = SQLITE_NOMEM_BKPT;
    goto out;
  }

  if( sqlite3_value_type(pVectorValue) == SQLITE_BLOB ){
    vectorInitFromBlob(pVectorInRow->pVector, sqlite3_value_blob(pVectorValue), sqlite3_value_bytes(pVectorValue));
  } else if( sqlite3_value_type(pVectorValue) == SQLITE_TEXT ){ 
    // users can put strings (e.g. '[1,2,3]') in the table and we should process them correctly
    if( vectorParse(pVectorValue, pVectorInRow->pVector, pzErrMsg) != 0 ){
      rc = SQLITE_ERROR;
      goto out;
    }
  }
  rc = SQLITE_OK;
out:
  if( rc != SQLITE_OK ){
    vectorFree(pVectorInRow->pVector);
  }
  return rc;
}

void vectorInRowFree(sqlite3 *db, VectorInRow *pVectorInRow) {
  vectorFree(pVectorInRow->pVector);
}

/**************************************************************************
** VectorOutRows utilities
****************************************************************************/

int vectorOutRowsAlloc(sqlite3 *db, VectorOutRows *pRows, int nRows, int nCols, char firstColumnAff){
  assert( nCols > 0 && nRows >= 0 );
  pRows->nRows = nRows;
  pRows->nCols = nCols;
  pRows->aRowids = NULL;
  pRows->ppValues = NULL;

  if( (u64)nRows * (u64)nCols > VECTOR_OUT_ROWS_MAX_CELLS ){
    return SQLITE_NOMEM_BKPT;
  }

  if( nCols == 1 && firstColumnAff == SQLITE_AFF_INTEGER ){
    pRows->aRowids = sqlite3DbMallocRaw(db, nRows * sizeof(i64));
    if( pRows->aRowids == NULL ){
      return SQLITE_NOMEM_BKPT;
    }
  }else{
    pRows->ppValues = sqlite3DbMallocZero(db, nRows * nCols * sizeof(sqlite3_value*));
    if( pRows->ppValues == NULL ){
      return SQLITE_NOMEM_BKPT;
    }
  }
  return SQLITE_OK;
}

int vectorOutRowsPut(VectorOutRows *pRows, int iRow, int iCol, const u64 *pInt, sqlite3_value *pValue) {
  sqlite3_value *pCopy;
  assert( 0 <= iRow && iRow < pRows->nRows );
  assert( 0 <= iCol && iCol < pRows->nCols );
  assert( pRows->aRowids != NULL || pRows->ppValues != NULL );
  assert( pInt == NULL || pRows->aRowids != NULL );
  assert( pInt != NULL || pValue != NULL );

  if( pRows->aRowids != NULL && pInt != NULL ){
    assert( pRows->nCols == 1 );
    pRows->aRowids[iRow] = *pInt;
  }else if( pRows->aRowids != NULL ){
    assert( pRows->nCols == 1 );
    assert( sqlite3_value_type(pValue) == SQLITE_INTEGER );
    pRows->aRowids[iRow] = sqlite3_value_int64(pValue);
  }else{
    // pValue can be unprotected and we must own sqlite3_value* - so we are making copy of it
    pCopy = sqlite3_value_dup(pValue);
    if( pCopy == NULL ){
      return SQLITE_NOMEM_BKPT;
    }
    pRows->ppValues[iRow * pRows->nCols + iCol] = pCopy;
  }
  return SQLITE_OK;
}

void vectorOutRowsGet(sqlite3_context *context, const VectorOutRows *pRows, int iRow, int iCol) {
  assert( 0 <= iRow && iRow < pRows->nRows );
  assert( 0 <= iCol && iCol < pRows->nCols );
  assert( pRows->aRowids != NULL || pRows->ppValues != NULL );
  if( pRows->aRowids != NULL ){
    assert( pRows->nCols == 1 );
    sqlite3_result_int64(context, pRows->aRowids[iRow]);
  }else{
    sqlite3_result_value(context, pRows->ppValues[iRow * pRows->nCols + iCol]);
  }
}

void vectorOutRowsFree(sqlite3 *db, VectorOutRows *pRows) {
  int i;
  
  // both aRowids and ppValues can be null if processing failing in the middle and we didn't created VectorOutRows
  assert( pRows->aRowids == NULL || pRows->ppValues == NULL );
 
  if( pRows->aRowids != NULL ){
    sqlite3DbFree(db, pRows->aRowids);
  }else if( pRows->ppValues != NULL ){
    for(i = 0; i < pRows->nRows * pRows->nCols; i++){
      if( pRows->ppValues[i] != NULL ){
        sqlite3_value_free(pRows->ppValues[i]);
      }
    }
    sqlite3DbFree(db, pRows->ppValues);
  }
}

/* 
 * Internal type to represent VECTOR_COLUMN_TYPES array 
 * We support both FLOATNN and FNN_BLOB type names for the following reasons:
 * 1. FLOATNN is easy to type for humans and generally OK to use for column type names
 * 2. FNN_BLOB is aligned with SQLite affinity rules and can be used in cases where compatibility with type affinity rules is important
 *    For example, before loading some third-party extensions or analysis of DB file with tools from SQLite ecosystem)
*/
struct VectorColumnType {
  const char *zName;
  int nBits;
};

static struct VectorColumnType VECTOR_COLUMN_TYPES[] = { 
  { "FLOAT32",  32 }, 
  { "FLOAT64",  64 }, 
  { "F32_BLOB", 32 }, 
  { "F64_BLOB", 64 } 
};

/*
 * Internal type to represent VECTOR_PARAM_NAMES array with recognized parameters for index creation 
 * For example, libsql_vector_idx(embedding, 'type=diskann', 'metric=cosine')
*/
struct VectorParamName {
  const char *zName;
  int tag;
  int type; // 0 - enum, 1 - integer, 2 - float
  const char *zValueStr;
  u64 value;
};

static struct VectorParamName VECTOR_PARAM_NAMES[] = { 
  { "type",     VECTOR_INDEX_TYPE_PARAM_ID, 0, "diskann", VECTOR_INDEX_TYPE_DISKANN },
  { "metric",   VECTOR_METRIC_TYPE_PARAM_ID, 0, "cosine", VECTOR_METRIC_TYPE_COS },
  { "alpha",    VECTOR_PRUNING_ALPHA_PARAM_ID, 2, 0, 0 },
  { "search_l", VECTOR_SEARCH_L_PARAM_ID, 1, 0, 0 },
  { "insert_l", VECTOR_INSERT_L_PARAM_ID, 2, 0, 0 },
};

static int parseVectorIdxParam(const char *zParam, VectorIdxParams *pParams, const char **pErrMsg) {
  int i, iDelimiter = 0, nValueLen = 0;
  const char *zValue;
  while( zParam[iDelimiter] && zParam[iDelimiter] != '=' ){
    iDelimiter++;
  }
  if( zParam[iDelimiter] != '=' ){
    *pErrMsg = "unexpected parameter format";
    return -1;
  }
  zValue = zParam + iDelimiter + 1;
  nValueLen = sqlite3Strlen30(zValue);
  for(i = 0; i < ArraySize(VECTOR_PARAM_NAMES); i++){
    if( sqlite3_strnicmp(VECTOR_PARAM_NAMES[i].zName, zParam, iDelimiter) != 0 ){
      continue;
    }
    if( VECTOR_PARAM_NAMES[i].type == 1 ){
      u64 value = sqlite3Atoi(zValue);
      if( value == 0 ){
        *pErrMsg = "invalid representation of integer vector index parameter";
        return -1;
      }
      if( vectorIdxParamsPutU64(pParams, VECTOR_PARAM_NAMES[i].tag, value) != 0 ){
        *pErrMsg = "unable to serialize integer vector index parameter";
        return -1;
      }
      return 0;
    }else if( VECTOR_PARAM_NAMES[i].type == 2 ){
      double value;
      // sqlite3AtoF returns value >= 1 if string is valid float
      if( sqlite3AtoF(zValue, &value, nValueLen, SQLITE_UTF8) <= 0 ){
        *pErrMsg = "invalid representation of floating point vector index parameter";
        return -1;
      }
      if( vectorIdxParamsPutF64(pParams, VECTOR_PARAM_NAMES[i].tag, value) != 0 ){
        *pErrMsg = "unable to serialize floating point vector index parameter";
        return -1;
      }
      return 0;
    }else if( VECTOR_PARAM_NAMES[i].type == 0 && sqlite3_strnicmp(VECTOR_PARAM_NAMES[i].zValueStr, zValue, nValueLen) == 0 ){
      if( vectorIdxParamsPutU64(pParams, VECTOR_PARAM_NAMES[i].tag, VECTOR_PARAM_NAMES[i].value) != 0 ){
        *pErrMsg = "unable to serialize vector index parameter";
        return -1;
      }
      return 0;
    }else{
      *pErrMsg = "unexpected parameter type";
      return -1;
    }
  }
  *pErrMsg = "unexpected parameter key";
  return -1;
}

int parseVectorIdxParams(Parse *pParse, VectorIdxParams *pParams, int type, int dims, struct ExprList_item *pArgList, int nArgs) {
  int i;
  const char *pErrMsg;
  if( vectorIdxParamsPutU64(pParams, VECTOR_FORMAT_PARAM_ID, VECTOR_FORMAT_DEFAULT) != 0 ){
    sqlite3ErrorMsg(pParse, "unable to serialize vector index parameter: format");
    return SQLITE_ERROR;
  }
  if( vectorIdxParamsPutU64(pParams, VECTOR_TYPE_PARAM_ID, type) != 0 ){
    sqlite3ErrorMsg(pParse, "unable to serialize vector index parameter: type");
    return SQLITE_ERROR;
  }
  if( vectorIdxParamsPutU64(pParams, VECTOR_DIM_PARAM_ID, dims) != 0 ){
    sqlite3ErrorMsg(pParse, "unable to serialize vector index parameter: dim");
    return SQLITE_ERROR;
  }
  for(i = 1; i < nArgs; i++){
    Expr *pArgExpr = pArgList[i].pExpr;
    if( pArgExpr->op != TK_STRING ){
      sqlite3ErrorMsg(pParse, "all arguments after first must be strings");
      return SQLITE_ERROR;
    }
    if( parseVectorIdxParam(pArgExpr->u.zToken, pParams, &pErrMsg) != 0 ){
      sqlite3ErrorMsg(pParse, "invalid vector index parameter '%s': %s", pArgExpr->u.zToken, pErrMsg);
      return SQLITE_ERROR;
    }
  }
  return SQLITE_OK;
}

#endif /* !defined(SQLITE_OMIT_VECTOR) */

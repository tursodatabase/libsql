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

/*
 * The code which glue SQLite internals with pure DiskANN implementation resides here
 * Main internal API methods are:
 * vectorIndexCreate()
 * vectorIndexClear()
 * vectorIndexDrop()
 * vectorIndexSearch()
 * vectorIndexCursorInit()
 * vectorIndexCursorClose()
 *
 * + cursor operations:
 * vectorIndexInsert(cursor)
 * vectorIndexDelete(cursor)
*/

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

int vectorIdxKeyGet(const Index *pIndex, VectorIdxKey *pKey, const char **pzErrMsg) {
  Table *pTable;
  Index *pPkIndex;
  int i, nKeyColumns;

  assert( pIndex->nKeyCol == 1 );
  assert( pIndex->nColumn > pIndex->nKeyCol );

  pTable = pIndex->pTable;
  nKeyColumns = pIndex->nColumn - pIndex->nKeyCol;
  if( nKeyColumns == 1 && pIndex->aiColumn[pIndex->nKeyCol] == XN_ROWID ){
    pKey->nKeyColumns = 1;
    pKey->aKeyAffinity[0] = SQLITE_AFF_INTEGER;
    pKey->azKeyCollation[0] = "BINARY";
    return 0;
  }
  if( nKeyColumns > VECTOR_INDEX_MAX_KEY_COLUMNS ){
    *pzErrMsg = "exceeded limit for composite columns in primary key index";
    return -1;
  }
  pPkIndex = sqlite3PrimaryKeyIndex(pIndex->pTable);
  assert( pPkIndex->nKeyCol == nKeyColumns );

  pKey->nKeyColumns = nKeyColumns;
  for(i = 0; i < pPkIndex->nKeyCol; i++){
    pKey->aKeyAffinity[i] = pTable->aCol[pPkIndex->aiColumn[i]].affinity;
    pKey->azKeyCollation[i] = pPkIndex->azColl[i];
  }
  return 0;
}

int vectorIdxKeyRowidLike(const VectorIdxKey *pKey){
  return pKey->nKeyColumns == 1 && pKey->aKeyAffinity[0] == SQLITE_AFF_INTEGER && sqlite3StrICmp(pKey->azKeyCollation[0], "BINARY") == 0;
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
  if( pVectorInRow->nKeys == 1 && sqlite3_value_type(&pVectorInRow->pKeyValues[0]) == SQLITE_INTEGER ){
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
  struct sqlite3_value *pVectorValue = &pRecord->aMem[0];
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
    if( vectorParseWithType(pVectorValue, pVectorInRow->pVector, pzErrMsg) != 0 ){
      rc = SQLITE_ERROR;
      goto out;
    }
  }
  rc = SQLITE_OK;
out:
  if( rc != SQLITE_OK && pVectorInRow->pVector != NULL ){
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

int vectorOutRowsAlloc(sqlite3 *db, VectorOutRows *pRows, int nRows, int nCols, int rowidLike){
  assert( nCols > 0 && nRows >= 0 );
  pRows->nRows = nRows;
  pRows->nCols = nCols;
  pRows->aIntValues = NULL;
  pRows->ppValues = NULL;

  if( (u64)nRows * (u64)nCols > VECTOR_OUT_ROWS_MAX_CELLS ){
    return SQLITE_NOMEM_BKPT;
  }

  if( rowidLike ){
    assert( nCols == 1 );
    pRows->aIntValues = sqlite3DbMallocRaw(db, nRows * sizeof(i64));
    if( pRows->aIntValues == NULL ){
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
  assert( pRows->aIntValues != NULL || pRows->ppValues != NULL );
  assert( pInt == NULL || pRows->aIntValues != NULL );
  assert( pInt != NULL || pValue != NULL );

  if( pRows->aIntValues != NULL && pInt != NULL ){
    assert( pRows->nCols == 1 );
    pRows->aIntValues[iRow] = *pInt;
  }else if( pRows->aIntValues != NULL ){
    assert( pRows->nCols == 1 );
    assert( sqlite3_value_type(pValue) == SQLITE_INTEGER );
    pRows->aIntValues[iRow] = sqlite3_value_int64(pValue);
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
  assert( pRows->aIntValues != NULL || pRows->ppValues != NULL );
  if( pRows->aIntValues != NULL ){
    assert( pRows->nCols == 1 );
    sqlite3_result_int64(context, pRows->aIntValues[iRow]);
  }else{
    sqlite3_result_value(context, pRows->ppValues[iRow * pRows->nCols + iCol]);
  }
}

void vectorOutRowsFree(sqlite3 *db, VectorOutRows *pRows) {
  int i;

  // both aIntValues and ppValues can be null if processing failing in the middle and we didn't created VectorOutRows
  assert( pRows->aIntValues == NULL || pRows->ppValues == NULL );

  if( pRows->aIntValues != NULL ){
    sqlite3DbFree(db, pRows->aIntValues);
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
  int type;
};

static struct VectorColumnType VECTOR_COLUMN_TYPES[] = {
  { "FLOAT32",    VECTOR_TYPE_FLOAT32 },
  { "F32_BLOB",   VECTOR_TYPE_FLOAT32 },
  { "FLOAT64",    VECTOR_TYPE_FLOAT64 },
  { "F64_BLOB",   VECTOR_TYPE_FLOAT64 },
  { "FLOAT1BIT",  VECTOR_TYPE_FLOAT1BIT },
  { "F1BIT_BLOB", VECTOR_TYPE_FLOAT1BIT },
  { "FLOAT8",     VECTOR_TYPE_FLOAT8 },
  { "F8_BLOB",    VECTOR_TYPE_FLOAT8 },
  { "FLOAT16",    VECTOR_TYPE_FLOAT16 },
  { "F16_BLOB",   VECTOR_TYPE_FLOAT16 },
  { "FLOATB16",   VECTOR_TYPE_FLOATB16 },
  { "FB16_BLOB",  VECTOR_TYPE_FLOATB16 },
};

/*
 * Internal type to represent VECTOR_PARAM_NAMES array with recognized parameters for index creation
 * For example, libsql_vector_idx(embedding, 'type=diskann', 'metric=cosine')
*/
struct VectorParamName {
  const char *zName;
  int tag;
  int type; // 0 - string enum, 1 - integer, 2 - float
  const char *zValueStr;
  u64 value;
};

static struct VectorParamName VECTOR_PARAM_NAMES[] = {
  { "type",               VECTOR_INDEX_TYPE_PARAM_ID,         0, "diskann",   VECTOR_INDEX_TYPE_DISKANN },
  { "metric",             VECTOR_METRIC_TYPE_PARAM_ID,        0, "cosine",    VECTOR_METRIC_TYPE_COS },
  { "metric",             VECTOR_METRIC_TYPE_PARAM_ID,        0, "l2",        VECTOR_METRIC_TYPE_L2 },
  { "compress_neighbors", VECTOR_COMPRESS_NEIGHBORS_PARAM_ID, 0, "float1bit", VECTOR_TYPE_FLOAT1BIT },
  { "compress_neighbors", VECTOR_COMPRESS_NEIGHBORS_PARAM_ID, 0, "float8",    VECTOR_TYPE_FLOAT8 },
  { "compress_neighbors", VECTOR_COMPRESS_NEIGHBORS_PARAM_ID, 0, "float16",   VECTOR_TYPE_FLOAT16 },
  { "compress_neighbors", VECTOR_COMPRESS_NEIGHBORS_PARAM_ID, 0, "floatb16",  VECTOR_TYPE_FLOATB16 },
  { "compress_neighbors", VECTOR_COMPRESS_NEIGHBORS_PARAM_ID, 0, "float32",   VECTOR_TYPE_FLOAT32 },
  { "alpha",              VECTOR_PRUNING_ALPHA_PARAM_ID, 2, 0, 0 },
  { "search_l",           VECTOR_SEARCH_L_PARAM_ID,      1, 0, 0 },
  { "insert_l",           VECTOR_INSERT_L_PARAM_ID,      1, 0, 0 },
  { "max_neighbors",      VECTOR_MAX_NEIGHBORS_PARAM_ID, 1, 0, 0 },
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
    if( iDelimiter != strlen(VECTOR_PARAM_NAMES[i].zName) || sqlite3_strnicmp(VECTOR_PARAM_NAMES[i].zName, zParam, iDelimiter) != 0 ){
      continue;
    }
    if( VECTOR_PARAM_NAMES[i].type == 1 ){
      int value = sqlite3Atoi(zValue);
      if( value == 0 ){
        *pErrMsg = "invalid representation of integer vector index parameter";
        return -1;
      }
      if( value < 0 ){
        *pErrMsg = "integer vector index parameter must be positive";
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
    }else if( VECTOR_PARAM_NAMES[i].type == 0 ){
      if( sqlite3_strnicmp(VECTOR_PARAM_NAMES[i].zValueStr, zValue, nValueLen) == 0 ){
        if( vectorIdxParamsPutU64(pParams, VECTOR_PARAM_NAMES[i].tag, VECTOR_PARAM_NAMES[i].value) != 0 ){
          *pErrMsg = "unable to serialize vector index parameter";
          return -1;
        }
        return 0;
      }
    }else{
      *pErrMsg = "unexpected parameter type";
      return -1;
    }
  }
  *pErrMsg = "invalid parameter";
  return -1;
}

int parseVectorIdxParams(Parse *pParse, VectorIdxParams *pParams, int type, int dims, struct ExprList_item *pArgList, int nArgs) {
  int i;
  const char *pErrMsg;
  if( vectorIdxParamsPutU64(pParams, VECTOR_FORMAT_PARAM_ID, VECTOR_FORMAT_DEFAULT) != 0 ){
    sqlite3ErrorMsg(pParse, "vector index: unable to serialize vector index parameter: format");
    return SQLITE_ERROR;
  }
  if( vectorIdxParamsPutU64(pParams, VECTOR_TYPE_PARAM_ID, type) != 0 ){
    sqlite3ErrorMsg(pParse, "vector index: unable to serialize vector index parameter: type");
    return SQLITE_ERROR;
  }
  if( vectorIdxParamsPutU64(pParams, VECTOR_DIM_PARAM_ID, dims) != 0 ){
    sqlite3ErrorMsg(pParse, "vector index: unable to serialize vector index parameter: dim");
    return SQLITE_ERROR;
  }
  for(i = 0; i < nArgs; i++){
    Expr *pArgExpr = pArgList[i].pExpr;
    if( pArgExpr->op != TK_STRING ){
      sqlite3ErrorMsg(pParse, "vector index: all arguments after first must be strings");
      return SQLITE_ERROR;
    }
    if( parseVectorIdxParam(pArgExpr->u.zToken, pParams, &pErrMsg) != 0 ){
      sqlite3ErrorMsg(pParse, "vector index: invalid vector index parameter '%s': %s", pArgExpr->u.zToken, pErrMsg);
      return SQLITE_ERROR;
    }
  }
  return SQLITE_OK;
}

/**************************************************************************
** Vector index cursor implementation
****************************************************************************/

/*
** A VectorIdxCursor is a special cursor to perform vector index lookups.
 */
struct VectorIdxCursor {
  sqlite3 *db;            /* Database connection */
  DiskAnnIndex *pIndex;   /* DiskANN index */
};


void skipSpaces(const char **pzStr) {
  while( **pzStr != '\0' && sqlite3Isspace(**pzStr) ){
    (*pzStr)++;
  }
}
/**
** Parses a type string such as `FLOAT32(3)` and set number of dimensions and bits
**
** Returns  0 if succeed and set correct values in both pDims and pType pointers
** Returns -1 if the type string is not a valid vector type for index and set pErrMsg to static string with error description in this case
**/
int vectorIdxParseColumnType(const char *zType, int *pType, int *pDims, const char **pErrMsg){
  assert( zType != NULL );

  int dimensions = 0;
  int i;
  skipSpaces(&zType);
  for(i = 0; i < ArraySize(VECTOR_COLUMN_TYPES); i++){
    const char* name = VECTOR_COLUMN_TYPES[i].zName;
    const char* zTypePtr = zType + strlen(name);
    if( sqlite3_strnicmp(zType, name, strlen(name)) != 0 ){
      continue;
    }
    skipSpaces(&zTypePtr);
    if( *zTypePtr != '(' ) {
      break;
    }
    zTypePtr++;
    skipSpaces(&zTypePtr);

    while( *zTypePtr != '\0' && *zTypePtr != ')' && !sqlite3Isspace(*zTypePtr) ){
      if( !sqlite3Isdigit(*zTypePtr) ){
        *pErrMsg = "non digit symbol in vector column parameter";
        return -1;
      }
      dimensions = dimensions*10 + (*zTypePtr - '0');
      if( dimensions > MAX_VECTOR_SZ ) {
        *pErrMsg = "max vector dimension exceeded";
        return -1;
      }
      zTypePtr++;
    }
    skipSpaces(&zTypePtr);
    if( *zTypePtr != ')' ){
      *pErrMsg = "missed closing brace for vector column type";
      return -1;
    }
    zTypePtr++;
    skipSpaces(&zTypePtr);

    if( *zTypePtr != '\0' ) {
      *pErrMsg = "extra data after dimension parameter for vector column type";
      return -1;
    }

    if( dimensions <= 0 ){
      *pErrMsg = "vector column must have non-zero dimension for index";
      return -1;
    }

    *pDims = dimensions;
    *pType = VECTOR_COLUMN_TYPES[i].type;
    return 0;
  }
  *pErrMsg = "unexpected vector column type";
  return -1;
}

int initVectorIndexMetaTable(sqlite3* db, const char *zDbSName) {
  int rc;
  static const char *zSqlTemplate = "CREATE TABLE IF NOT EXISTS \"%w\"." VECTOR_INDEX_GLOBAL_META_TABLE " ( name TEXT PRIMARY KEY, metadata BLOB ) WITHOUT ROWID;";
  char* zSql;

  assert( zDbSName != NULL );

  zSql = sqlite3_mprintf(zSqlTemplate, zDbSName);
  if( zSql == NULL ){
    return SQLITE_NOMEM_BKPT;
  }
  rc = sqlite3_exec(db, zSql, 0, 0, 0);
  sqlite3_free(zSql);
  return rc;
}

int insertIndexParameters(sqlite3* db, const char *zDbSName, const char *zName, const VectorIdxParams *pParameters) {
  int rc = SQLITE_ERROR;
  static const char *zSqlTemplate = "INSERT INTO \"%w\"." VECTOR_INDEX_GLOBAL_META_TABLE " VALUES (?, ?)";
  sqlite3_stmt* pStatement = NULL;
  char *zSql;

  assert( zDbSName != NULL );

  zSql = sqlite3_mprintf(zSqlTemplate, zDbSName);
  if( zSql == NULL ){
    return SQLITE_NOMEM_BKPT;
  }

  rc = sqlite3_prepare_v2(db, zSql, -1, &pStatement, 0);
  if( rc != SQLITE_OK ){
    goto clear_and_exit;
  }
  rc = sqlite3_bind_text(pStatement, 1, zName, -1, 0);
  if( rc != SQLITE_OK ){
    goto clear_and_exit;
  }
  rc = sqlite3_bind_blob(pStatement, 2, pParameters->pBinBuf, pParameters->nBinSize, SQLITE_STATIC);
  if( rc != SQLITE_OK ){
    goto clear_and_exit;
  }
  rc = sqlite3_step(pStatement);
  if( (rc&0xff) == SQLITE_CONSTRAINT ){
    rc = SQLITE_CONSTRAINT;
  }else if( rc != SQLITE_DONE ){
    rc = SQLITE_ERROR;
  }else{
    rc = SQLITE_OK;
  }
clear_and_exit:
  if( zSql != NULL ){
    sqlite3_free(zSql);
  }
  if( pStatement != NULL ){
    sqlite3_finalize(pStatement);
  }
  return rc;
}

int removeIndexParameters(sqlite3* db, const char *zName) {
  static const char *zSql = "DELETE FROM " VECTOR_INDEX_GLOBAL_META_TABLE " WHERE name = ?";
  sqlite3_stmt* pStatement = NULL;
  int rc = SQLITE_ERROR;

  rc = sqlite3_prepare_v2(db, zSql, -1, &pStatement, 0);
  if( rc != SQLITE_OK ){
    goto clear_and_exit;
  }
  rc = sqlite3_bind_text(pStatement, 1, zName, -1, 0);
  if( rc != SQLITE_OK ){
    goto clear_and_exit;
  }
  rc = sqlite3_step(pStatement);
  if( rc != SQLITE_DONE ){
    rc = SQLITE_ERROR;
  } else {
    rc = SQLITE_OK;
  }
clear_and_exit:
  if( pStatement != NULL ){
    sqlite3_finalize(pStatement);
  }
  return rc;
}

int vectorIndexTryGetParametersFromTableFormat(sqlite3 *db, const char *zSql, const char *zIdxName, VectorIdxParams *pParams) {
  int rc = SQLITE_OK;
  sqlite3_stmt *pStmt = NULL;
  int nBinSize;

  vectorIdxParamsInit(pParams, NULL, 0);

  rc = sqlite3_prepare_v2(db, zSql, -1, &pStmt, 0);
  if( rc != SQLITE_OK ){
    goto out;
  }
  rc = sqlite3_bind_text(pStmt, 1, zIdxName, -1, SQLITE_STATIC);
  if( rc != SQLITE_OK ){
    goto out;
  }
  if( sqlite3_step(pStmt) != SQLITE_ROW ){
    rc = SQLITE_ERROR;
    goto out;
  }
  vectorIdxParamsPutU64(pParams, VECTOR_FORMAT_PARAM_ID, 1);
  vectorIdxParamsPutU64(pParams, VECTOR_INDEX_TYPE_PARAM_ID, VECTOR_INDEX_TYPE_DISKANN);
  vectorIdxParamsPutU64(pParams, VECTOR_TYPE_PARAM_ID, VECTOR_TYPE_FLOAT32);
  vectorIdxParamsPutU64(pParams, VECTOR_DIM_PARAM_ID, sqlite3_column_int(pStmt, 2));
  vectorIdxParamsPutU64(pParams, VECTOR_METRIC_TYPE_PARAM_ID, VECTOR_METRIC_TYPE_COS);
  if( vectorIdxParamsPutU64(pParams, VECTOR_BLOCK_SIZE_PARAM_ID, sqlite3_column_int(pStmt, 1)) != 0 ){
    rc = SQLITE_ERROR;
    goto out;
  }
  assert( sqlite3_step(pStmt) == SQLITE_DONE );
  rc = SQLITE_OK;
out:
  if( pStmt != NULL ){
    sqlite3_finalize(pStmt);
  }
  return rc;
}

int vectorIndexTryGetParametersFromBinFormat(sqlite3 *db, const char *zSql, const char *zIdxName, VectorIdxParams *pParams) {
  int rc = SQLITE_OK;
  sqlite3_stmt *pStmt = NULL;
  int nBinSize;

  vectorIdxParamsInit(pParams, NULL, 0);

  rc = sqlite3_prepare_v2(db, zSql, -1, &pStmt, 0);
  if( rc != SQLITE_OK ){
    goto out;
  }
  rc = sqlite3_bind_text(pStmt, 1, zIdxName, -1, SQLITE_STATIC);
  if( rc != SQLITE_OK ){
    goto out;
  }
  if( sqlite3_step(pStmt) != SQLITE_ROW ){
    rc = SQLITE_ERROR;
    goto out;
  }
  assert( sqlite3_column_type(pStmt, 0) == SQLITE_BLOB );
  nBinSize = sqlite3_column_bytes(pStmt, 0);
  if( nBinSize > VECTOR_INDEX_PARAMS_BUF_SIZE ){
    rc = SQLITE_ERROR;
    goto out;
  }
  vectorIdxParamsInit(pParams, (u8*)sqlite3_column_blob(pStmt, 0), nBinSize);
  assert( sqlite3_step(pStmt) == SQLITE_DONE );
  rc = SQLITE_OK;
out:
  if( pStmt != NULL ){
    sqlite3_finalize(pStmt);
  }
  return rc;
}

int vectorIndexGetParameters(
  sqlite3 *db,
  const char *zIdxName,
  VectorIdxParams *pParams
) {
  int rc = SQLITE_OK;

  static const char* zSelectSql = "SELECT metadata FROM " VECTOR_INDEX_GLOBAL_META_TABLE " WHERE name = ?";
  // zSelectSqlPekkaLegacy handles the case when user created DB before 04 July 2024 (https://discord.com/channels/933071162680958986/1225560924526477322/1258367912402489397)
  // when instead of table with binary parameters rigid schema was used for index settings
  // we should drop this eventually - but for now we postponed this decision
  static const char* zSelectSqlPekkaLegacy = "SELECT vector_type, block_size, dims, distance_ops FROM libsql_vector_index WHERE name = ?";
  rc = vectorIndexTryGetParametersFromBinFormat(db, zSelectSql, zIdxName, pParams);
  if( rc == SQLITE_OK ){
    return SQLITE_OK;
  }
  rc = vectorIndexTryGetParametersFromTableFormat(db, zSelectSqlPekkaLegacy, zIdxName, pParams);
  if( rc == SQLITE_OK ){
    return SQLITE_OK;
  }
  return SQLITE_ERROR;
}

int vectorIndexDrop(sqlite3 *db, const char *zDbSName, const char *zIdxName) {
  // we want to try delete all traces of index on every attempt
  // this is done to prevent unrecoverable situations where index were dropped but index parameters deletion failed and second attempt will fail on first step
  int rcIdx, rcParams;

  assert( zDbSName != NULL );

  rcIdx = diskAnnDropIndex(db, zDbSName, zIdxName);
  rcParams = removeIndexParameters(db, zIdxName);
  return rcIdx != SQLITE_OK ? rcIdx : rcParams;
}

int vectorIndexClear(sqlite3 *db, const char *zDbSName, const char *zIdxName) {
  assert( zDbSName != NULL );

  return diskAnnClearIndex(db, zDbSName, zIdxName);
}

/*
 * vectorIndexCreate analyzes any index creation expression and create vector index if needed
 * it tolerates the situation when insert into VECTOR_INDEX_GLOBAL_META_TABLE failed with conflict
 * this made intentionally in order to natively support upload of SQLite dumps
 *
 * dump populates tables first and create indices after
 * so we must omit index refill setp because shadow tables already filled
 *
 * 1. in case of any error                                        :-1 returned (and pParse errMsg is populated with some error message)
 * 2. if vector index must not be created                         : 0 returned
 * 3. if vector index must be created but refill must be skipped  : 1 returned
 * 4. if vector index must be created and refilled from base table: 2 returned
*/
int vectorIndexCreate(Parse *pParse, const Index *pIdx, const char *zDbSName, const IdList *pUsing) {
  static const int CREATE_FAIL = -1;
  static const int CREATE_IGNORE = 0;
  static const int CREATE_OK_SKIP_REFILL = 1;
  static const int CREATE_OK = 2;

  int i, rc = SQLITE_OK;
  int dims, type;
  int hasLibsqlVectorIdxFn = 0, hasCollation = 0;
  const char *pzErrMsg = NULL;

  assert( zDbSName != NULL );

  sqlite3 *db = pParse->db;
  Table *pTable = pIdx->pTable;
  struct ExprList_item *pListItem;
  ExprList *pArgsList;
  int iEmbeddingColumn;
  char* zEmbeddingColumnTypeName;
  VectorIdxKey idxKey;
  VectorIdxParams idxParams;
  vectorIdxParamsInit(&idxParams, NULL, 0);

  if( pParse->eParseMode ){
    // scheme can be re-parsed by SQLite for different reasons (for example, to check schema after
    // ALTER COLUMN statements) - so we must skip creation in such cases
    return CREATE_IGNORE;
  }

  // backward compatibility: preserve old indices with deprecated syntax but forbid creation of new indices with this syntax
  if( pParse->db->init.busy == 0 && pUsing != NULL ){
    if( pIdx->zName != NULL && pTable->zName != NULL && pIdx->nKeyCol == 1 && pIdx->aiColumn != NULL && pIdx->aiColumn[0] < pTable->nCol ){
      sqlite3ErrorMsg(pParse, "vector index: USING syntax is deprecated, please use plain CREATE INDEX: CREATE INDEX %s ON %s ( " VECTOR_INDEX_MARKER_FUNCTION "(%s) )", pIdx->zName, pTable->zName, pTable->aCol[pIdx->aiColumn[0]].zCnName);
    } else {
      sqlite3ErrorMsg(pParse, "vector index: USING syntax is deprecated, please use plain CREATE INDEX: CREATE INDEX xxx ON yyy ( " VECTOR_INDEX_MARKER_FUNCTION "(zzz) )");
    }
    return CREATE_FAIL;
  }
  if( db->init.busy == 1 && pUsing != NULL ){
    return CREATE_OK;
  }

  // vector index must have expressions over column
  if( pIdx->aColExpr == NULL ) {
    return CREATE_IGNORE;
  }

  pListItem = pIdx->aColExpr->a;
  for(i=0; i<pIdx->aColExpr->nExpr; i++, pListItem++){
    Expr* pExpr = pListItem->pExpr;
    while( pExpr->op == TK_COLLATE ){
      pExpr = pExpr->pLeft;
      hasCollation = 1;
    }
    if( pExpr->op == TK_FUNCTION && sqlite3StrICmp(pExpr->u.zToken, VECTOR_INDEX_MARKER_FUNCTION) == 0 ) {
      hasLibsqlVectorIdxFn = 1;
    }
  }
  if( !hasLibsqlVectorIdxFn ) {
    return CREATE_IGNORE;
  }
  if( hasCollation ){
    sqlite3ErrorMsg(pParse, "vector index: collation in expression is forbidden");
    return CREATE_FAIL;
  }
  if( pIdx->aColExpr->nExpr != 1 ) {
    sqlite3ErrorMsg(pParse, "vector index: must contain exactly one column wrapped into the " VECTOR_INDEX_MARKER_FUNCTION " function");
    return CREATE_FAIL;
  }

  pArgsList = pIdx->aColExpr->a[0].pExpr->x.pList;
  pListItem = pArgsList->a;

  if( pArgsList->nExpr < 1 ){
    sqlite3ErrorMsg(pParse, "vector idnex: " VECTOR_INDEX_MARKER_FUNCTION " must contain at least one argument");
    return CREATE_FAIL;
  }
  if( pListItem[0].pExpr->op != TK_COLUMN ) {
    sqlite3ErrorMsg(pParse, "vector index: " VECTOR_INDEX_MARKER_FUNCTION " first argument must be a column token");
    return CREATE_FAIL;
  }
  iEmbeddingColumn = pListItem[0].pExpr->iColumn;
  if( iEmbeddingColumn < 0 ) {
    sqlite3ErrorMsg(pParse, "vector index: " VECTOR_INDEX_MARKER_FUNCTION " first argument must be column with vector type");
    return CREATE_FAIL;
  }
  assert( iEmbeddingColumn >= 0 && iEmbeddingColumn < pTable->nCol );

  zEmbeddingColumnTypeName = sqlite3ColumnType(&pTable->aCol[iEmbeddingColumn], "");
  if( vectorIdxParseColumnType(zEmbeddingColumnTypeName, &type, &dims, &pzErrMsg) != 0 ){
    sqlite3ErrorMsg(pParse, "vector index: %s: %s", pzErrMsg, zEmbeddingColumnTypeName);
    return CREATE_FAIL;
  }
  // schema is locked while db is initializing and we need to just proceed here
  if( db->init.busy == 1 ){
    return CREATE_OK;
  }

  rc = initVectorIndexMetaTable(db, zDbSName);
  if( rc != SQLITE_OK ){
    sqlite3ErrorMsg(pParse, "vector index: failed to init meta table: %s", sqlite3_errmsg(db));
    return CREATE_FAIL;
  }
  rc = parseVectorIdxParams(pParse, &idxParams, type, dims, pListItem + 1, pArgsList->nExpr - 1);
  if( rc != SQLITE_OK ){
    return CREATE_FAIL;
  }
  if( vectorIdxKeyGet(pIdx, &idxKey, &pzErrMsg) != 0 ){
    sqlite3ErrorMsg(pParse, "vector index: failed to detect underlying table key: %s", pzErrMsg);
    return CREATE_FAIL;
  }
  if( idxKey.nKeyColumns != 1 ){
    sqlite3ErrorMsg(pParse, "vector index: unsupported for tables without ROWID and composite primary key");
    return CREATE_FAIL;
  }
  rc = diskAnnCreateIndex(db, zDbSName, pIdx->zName, &idxKey, &idxParams, &pzErrMsg);
  if( rc != SQLITE_OK ){
    if( pzErrMsg != NULL ){
      sqlite3ErrorMsg(pParse, "vector index: unable to initialize diskann: %s", pzErrMsg);
    }else{
      sqlite3ErrorMsg(pParse, "vector index: unable to initialize diskann");
    }
    return CREATE_FAIL;
  }
  rc = insertIndexParameters(db, zDbSName, pIdx->zName, &idxParams);

  // we must consider only lower bits because with sqlite3_extended_result_codes on
  // we can recieve different subtypes of CONSTRAINT error
  if( (rc&0xff) == SQLITE_CONSTRAINT ){
    // we are violating unique constraint here which means that someone inserted parameters in the table before us
    // taking aside corruption scenarios, this can be in case of loading dump (because tables and data are loaded before indices)
    // this case is valid and we must proceed with index creating but avoid index-refill step as it is already filled
    return CREATE_OK_SKIP_REFILL;
  }
  if( rc != SQLITE_OK ){
    sqlite3ErrorMsg(pParse, "vector index: unable to update global metadata table");
    return CREATE_FAIL;
  }
  return CREATE_OK;
}

int vectorIndexSearch(
  sqlite3 *db,
  const char* zDbSName,
  int argc,
  sqlite3_value **argv,
  VectorOutRows *pRows,
  int *nReads,
  int *nWrites,
  char **pzErrMsg
) {
  int type, dims, k, rc;
  const char *zIdxName;
  const char *zErrMsg;
  Vector *pVector = NULL;
  DiskAnnIndex *pDiskAnn = NULL;
  Index *pIndex;
  VectorIdxKey pKey;
  VectorIdxParams idxParams;
  vectorIdxParamsInit(&idxParams, NULL, 0);

  assert( zDbSName != NULL );

  if( argc != 3 ){
    *pzErrMsg = sqlite3_mprintf("vector index(search): got %d parameters, expected 3", argc);
    rc = SQLITE_ERROR;
    goto out;
  }
  if( detectVectorParameters(argv[1], VECTOR_TYPE_FLOAT32, &type, &dims, pzErrMsg) != 0 ){
    rc = SQLITE_ERROR;
    goto out;
  }

  pVector = vectorAlloc(type, dims);
  if( pVector == NULL ){
    rc = SQLITE_NOMEM_BKPT;
    goto out;
  }
  if( vectorParseWithType(argv[1], pVector, pzErrMsg) != 0 ){
    rc = SQLITE_ERROR;
    goto out;
  }
  if( sqlite3_value_type(argv[2]) != SQLITE_INTEGER ){
    *pzErrMsg = sqlite3_mprintf("vector index(search): third parameter (k) must be a non-negative integer");
    rc = SQLITE_ERROR;
    goto out;
  }
  k = sqlite3_value_int(argv[2]);
  if( k < 0 ){
    *pzErrMsg = sqlite3_mprintf("vector index(search): third parameter (k) must be a non-negative integer");
    rc = SQLITE_ERROR;
    goto out;
  }
  if( sqlite3_value_type(argv[0]) != SQLITE_TEXT ){
    *pzErrMsg = sqlite3_mprintf("vector index(search): first parameter (index) must be a string");
    rc = SQLITE_ERROR;
    goto out;
  }
  zIdxName = (const char*)sqlite3_value_text(argv[0]);
  if( vectorIndexGetParameters(db, zIdxName, &idxParams) != 0 ){
    *pzErrMsg = sqlite3_mprintf("vector index(search): failed to parse vector index parameters");
    rc = SQLITE_ERROR;
    goto out;
  }
  pIndex = sqlite3FindIndex(db, zIdxName, zDbSName);
  if( pIndex == NULL ){
    *pzErrMsg = sqlite3_mprintf("vector index(search): index not found");
    rc = SQLITE_ERROR;
    goto out;
  }
  rc = diskAnnOpenIndex(db, zDbSName, zIdxName, &idxParams, &pDiskAnn);
  if( rc != SQLITE_OK ){
    *pzErrMsg = sqlite3_mprintf("vector index(search): failed to open diskann index");
    goto out;
  }
  if( vectorIdxKeyGet(pIndex, &pKey, &zErrMsg) != 0 ){
    *pzErrMsg = sqlite3_mprintf("vector index(search): failed to extract table key: %s", zErrMsg);
    rc = SQLITE_ERROR;
    goto out;
  }
  rc = diskAnnSearch(pDiskAnn, pVector, k, &pKey, pRows, pzErrMsg);
out:
  if( pDiskAnn != NULL ){
    *nReads += pDiskAnn->nReads;
    *nWrites += pDiskAnn->nWrites;
    diskAnnCloseIndex(pDiskAnn);
  }
  if( pVector != NULL ){
    vectorFree(pVector);
  }
  return rc;
}

int vectorIndexInsert(
  VectorIdxCursor *pCur,
  const UnpackedRecord *pRecord,
  char **pzErrMsg
){
  int rc;
  VectorInRow vectorInRow;

  rc = vectorInRowAlloc(pCur->db, pRecord, &vectorInRow, pzErrMsg);
  if( rc != SQLITE_OK ){
    return rc;
  }
  if( vectorInRow.pVector == NULL ){
    return SQLITE_OK;
  }
  rc = diskAnnInsert(pCur->pIndex, &vectorInRow, pzErrMsg);
  vectorInRowFree(pCur->db, &vectorInRow);
  return rc;
}

int vectorIndexDelete(
  VectorIdxCursor *pCur,
  const UnpackedRecord *r,
  char **pzErrMsg
){
  VectorInRow payload;

  payload.pVector = NULL;
  payload.nKeys = r->nField - 1;
  payload.pKeyValues = r->aMem + 1;
  return diskAnnDelete(pCur->pIndex, &payload, pzErrMsg);
}

int vectorIndexCursorInit(
  sqlite3 *db,
  const char *zDbSName,
  const char *zIndexName,
  VectorIdxCursor **ppCursor
){
  int rc;
  VectorIdxCursor* pCursor;
  VectorIdxParams params;
  vectorIdxParamsInit(&params, NULL, 0);

  assert( zDbSName != NULL );

  if( vectorIndexGetParameters(db, zIndexName, &params) != 0 ){
    return SQLITE_ERROR;
  }
  pCursor = sqlite3DbMallocZero(db, sizeof(VectorIdxCursor));
  if( pCursor == 0 ){
    return SQLITE_NOMEM_BKPT;
  }
  rc = diskAnnOpenIndex(db, zDbSName, zIndexName, &params, &pCursor->pIndex);
  if( rc != SQLITE_OK ){
    sqlite3DbFree(db, pCursor);
    return rc;
  }
  pCursor->db = db;
  *ppCursor = pCursor;
  return SQLITE_OK;
}

void vectorIndexCursorClose(sqlite3 *db, VectorIdxCursor *pCursor, int *nReads, int *nWrites){
  *nReads = pCursor->pIndex->nReads;
  *nWrites = pCursor->pIndex->nWrites;

  diskAnnCloseIndex(pCursor->pIndex);
  sqlite3DbFree(db, pCursor);
}

#endif /* !defined(SQLITE_OMIT_VECTOR) */

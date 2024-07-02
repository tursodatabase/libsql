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
#include "vdbeInt.h"
#ifndef SQLITE_OMIT_VECTOR
#include "sqliteInt.h"

#include "vectorInt.h"

#define MAX_FLOAT_CHAR_SZ  1024

VectorIndexParameters createDefaultVectorIndexParameters() {
  VectorIndexParameters parameters;
  parameters.formatVersion = VECTOR_INDEX_FORMAT_VERSION;
  parameters.indexType = VECTOR_DISKANN_INDEX;
  parameters.metricType = VECTOR_DISTANCE_COS;
  parameters.vectorType = 0;
  parameters.vectorDimension = 0;
  parameters.blockSize = 128;
  return parameters;
}

void serializeParameter(u8 tag, u64 value, u8 *buffer) {
  int i;
  *(buffer++) = tag;
  for(i = 0; i < 8; i++){
    *(buffer++) = value & 0xff;
    value >>= 8;
  }
}
void deserializeParameter(VectorIndexParameters *pParameters, u8 *buffer) {
  u64 value = 0;
  int i = 0;
  u8 tag = *(buffer++);
  for(i = 0; i < 8; i++){
    value |= ((u64)(*buffer) << (u64)(8 * i));
    buffer++;
  }
  if(tag == 1){ pParameters->formatVersion = value; }
  if(tag == 2){ pParameters->indexType = value; }
  if(tag == 3){ pParameters->vectorType = value; }
  if(tag == 4){ pParameters->vectorDimension = value; }
  if(tag == 5){ pParameters->metricType = value; }
  if(tag == 6){ pParameters->blockSize = value; }
}
void serializeVectorIndexParameters(VectorIndexParameters *pParameters, u8 *buffer, int bufferSize) {
  assert( bufferSize == VECTOR_INDEX_PARAMETERS_COUNT * VECTOR_INDEX_PARAMETER_BIN_LENGTH );
  serializeParameter(1, pParameters->formatVersion,   buffer + 0 * VECTOR_INDEX_PARAMETER_BIN_LENGTH);
  serializeParameter(2, pParameters->indexType,       buffer + 1 * VECTOR_INDEX_PARAMETER_BIN_LENGTH);
  serializeParameter(3, pParameters->vectorType,      buffer + 2 * VECTOR_INDEX_PARAMETER_BIN_LENGTH);
  serializeParameter(4, pParameters->vectorDimension, buffer + 3 * VECTOR_INDEX_PARAMETER_BIN_LENGTH);
  serializeParameter(5, pParameters->metricType,      buffer + 4 * VECTOR_INDEX_PARAMETER_BIN_LENGTH);
  serializeParameter(6, pParameters->blockSize,       buffer + 5 * VECTOR_INDEX_PARAMETER_BIN_LENGTH);
}

int deserializeVectorIndexParameters(VectorIndexParameters *pParameters, u8 *buffer, int bufferSize) {
  int i;
  if( bufferSize % VECTOR_INDEX_PARAMETER_BIN_LENGTH != 0 ){
    return -1;
  }
  for(i = 0; i * VECTOR_INDEX_PARAMETER_BIN_LENGTH < bufferSize; i++){
    deserializeParameter(pParameters, buffer + i * VECTOR_INDEX_PARAMETER_BIN_LENGTH);
  }
  if( pParameters->formatVersion == 0 || 
      pParameters->vectorDimension == 0 || 
      pParameters->vectorType == 0 || 
      pParameters->indexType == 0 || 
      pParameters->metricType == 0 || 
      pParameters->blockSize == 0 ){
    return -1;
  }
  return 0;
}
int typeParameterConversion(const char *zValue, VectorIndexParameters *parameters){
  if( sqlite3_stricmp(zValue, "diskann") == 0 ){
    parameters->indexType = VECTOR_DISKANN_INDEX;
    return 0;
  }
  return -1;
}
int metricParameterConversion(const char *zValue, VectorIndexParameters *parameters){
  if( sqlite3_stricmp(zValue, "cosine") == 0 ){
    parameters->metricType = VECTOR_DISTANCE_COS;
    return 0;
  }
  return -1;
}


/**************************************************************************
** Utility routines for dealing with Vector objects
**************************************************************************/

size_t vectorDataSize(VectorType type, VectorDims dims){
  switch( type ){
    case VECTOR_TYPE_FLOAT32:
      return dims * sizeof(float);
    case VECTOR_TYPE_FLOAT64:
      return dims * sizeof(double);
    default:
      assert(0);
  }
  return 0;
}

/*
** Initialize the Vector object
*/
static void vectorInit(Vector *p, VectorType type, VectorDims dims, void *data){
  p->type = type;
  p->dims = dims;
  p->data = data;
  p->flags = 0;
}

/**
** Allocate a Vector object and its data buffer.
**/
Vector *vectorAlloc(VectorType type, VectorDims dims){
  void *p;

  p = sqlite3_malloc(sizeof(Vector) + vectorDataSize(type, dims));
  if( p==NULL ){
    return NULL;
  }
  vectorInit(p, type, dims, ((char*) p) + sizeof(Vector));
  return p;
}

/**
** Allocate a Vector object and its data buffer from the SQLite context. 
**/
static Vector *vectorContextAlloc(sqlite3_context *pCtx, VectorType type){
  void *p;

  // contextMalloc will set NOMEM or TOOBIG errors by itself
  p = contextMalloc(pCtx, sizeof(Vector) + vectorDataSize(type, MAX_VECTOR_SZ));
  if( p==NULL ){
    return NULL;
  }
  vectorInit(p, type, MAX_VECTOR_SZ, ((char*) p) + sizeof(Vector));
  return p;
}

/**
** Free a Vector object and its data buffer allocated, unless the vector is static.
**/
void vectorFree(Vector *p){
  if( p==0 ){
    return;
  }
  if( p->flags & VECTOR_FLAGS_STATIC ){
    return;
  }
  sqlite3_free(p);
}

/*
** Initialize a static Vector object.
**
** Note that that the vector object points to the blob so if
** you free the blob, the vector becomes invalid.
**/
static void vectorInitStatic(Vector *p, u32 type, const unsigned char *blob, size_t blobSz){
  switch (type) {
    case VECTOR_TYPE_FLOAT32:
      vectorF32InitFromBlob(p, blob, blobSz);
      break;
    case VECTOR_TYPE_FLOAT64:
      vectorF64InitFromBlob(p, blob, blobSz);
      break;
    default:
      assert(0);
  }
  p->type = type;
  p->flags = VECTOR_FLAGS_STATIC;
}

// should this function return double (wider possible type)
float vectorDistanceCos(Vector *v1, Vector *v2){
  assert(v1->type == v2->type);
  switch (v1->type) {
    case VECTOR_TYPE_FLOAT32:
      return vectorF32DistanceCos(v1, v2);
      break;
    case VECTOR_TYPE_FLOAT64:
      return vectorF64DistanceCos(v1, v2);
      break;
    default:
      assert(0);
  }
  return -1;
}

static int vectorParseText(
  sqlite3_value *arg,
  Vector *v,
  char **pzErrMsg
){
  // one more extra character in order to safely print data from elBuf with printf-like method; will be set to zero later
  char elBuf[MAX_FLOAT_CHAR_SZ + 1];
  const unsigned char *zStr;
  float *elems = v->data;
  int bufidx = 0;
  int vecidx = 0;
  double el;

  if( sqlite3_value_type(arg) != SQLITE_TEXT ){
    *pzErrMsg = sqlite3_mprintf("invalid vector: not a text type");
    goto error;
  }

  zStr = sqlite3_value_text(arg);
  if (zStr == NULL) return 0;

  while( zStr && sqlite3Isspace(*zStr) )
    zStr++;

  if( *zStr != '[' ){
    *pzErrMsg = sqlite3_mprintf("invalid vector: doesn't start with '['");
    goto error;
  }
  zStr++;

  // clear elBuf when we are ready to parse floats
  memset(elBuf, 0, sizeof(elBuf));

  for(; zStr != NULL && *zStr != '\0'; zStr++){
    char this = *zStr;
    if( sqlite3Isspace(this) ){
      continue;
    }
    if( this != ',' && this != ']' ){
      elBuf[bufidx++] = this;
      if( bufidx > MAX_FLOAT_CHAR_SZ ){
        *pzErrMsg = sqlite3_mprintf("float too big while parsing vector: '%s'", elBuf);
        goto error;
      }
      continue;
    }
    // empty vector case: '[]'
    if( this == ']' && vecidx == 0 && bufidx == 0 ){
      break;
    }
    if( sqlite3AtoF(elBuf, &el, bufidx, SQLITE_UTF8) <= 0 ){
      *pzErrMsg = sqlite3_mprintf("invalid number: '%s'", elBuf);
      goto error;
    }
    if( vecidx >= MAX_VECTOR_SZ ){
      *pzErrMsg = sqlite3_mprintf("vector is larger than the maximum: (%d)", MAX_VECTOR_SZ);
      goto error;
    }
    // clear only first bufidx positions - all other are zero
    memset(elBuf, 0, bufidx);
    bufidx = 0;
    elems[vecidx++] = el;
    if( this == ']' ){
      break;
    }
  }
  if( zStr != NULL && *zStr != ']' ){
    *pzErrMsg = sqlite3_mprintf("malformed vector, doesn't end with ']'");
    goto error;
  }
  zStr++;

  while( zStr && sqlite3Isspace(*zStr) )
    zStr++;
  if( zStr != NULL && *zStr != '\0' ){
    *pzErrMsg = sqlite3_mprintf("malformed vector, extra data after closing ']'");
    goto error;
  }
  v->dims = vecidx;
  return vecidx;
error:
  return -1;
}

static int vectorParseBlob(
  sqlite3_value *arg,
  Vector *v,
  char **pzErrMsg
){
  switch (v->type) {
    case VECTOR_TYPE_FLOAT32: return vectorF32ParseBlob(arg, v, pzErrMsg);
    case VECTOR_TYPE_FLOAT64: return vectorF64ParseBlob(arg, v, pzErrMsg);
    default: assert(0);
  }
  return -1;
}

int vectorParse(
  sqlite3_value *arg,
  Vector *v,
  char **pzErrMsg
){
  switch( sqlite3_value_type(arg) ){
    case SQLITE_NULL:
      *pzErrMsg = sqlite3_mprintf("invalid vector: NULL");
      return -1;
    case SQLITE_BLOB:
      return vectorParseBlob(arg, v, pzErrMsg);
    case SQLITE_TEXT:
      return vectorParseText(arg, v, pzErrMsg);
    default:
      *pzErrMsg = sqlite3_mprintf("invalid vector: not a text or blob type");
      return -1;
  }
}

// why do we need this actually?
static inline int isInteger(float num){
  return num == (u64)num;
}

void vectorDump(Vector *pVec){
  switch (pVec->type) {
    case VECTOR_TYPE_FLOAT32:
      vectorF32Dump(pVec);
      break;
    case VECTOR_TYPE_FLOAT64:
      vectorF64Dump(pVec);
      break;
    default:
      assert(0);
  }
}

static void vectorDeserialize(
  sqlite3_context *context,
  Vector *v
){
  switch (v->type) {
    case VECTOR_TYPE_FLOAT32:
      vectorF32Deserialize(context, v);
      break;
    case VECTOR_TYPE_FLOAT64:
      vectorF64Deserialize(context, v);
      break;
    default:
      assert(0);
  }
}

static void vectorSerialize(
  sqlite3_context *context,
  Vector *v
){
  if( v->dims==0 ) {
    sqlite3_result_null(context);
    return;
  }
  switch (v->type) {
    case VECTOR_TYPE_FLOAT32:
      vectorF32Serialize(context, v);
      break;
    case VECTOR_TYPE_FLOAT64:
      vectorF64Serialize(context, v);
      break;
    default:
      assert(0);
  }
}

size_t vectorSerializeToBlob(Vector *p, unsigned char *blob, size_t blobSize){
  switch (p->type) {
    case VECTOR_TYPE_FLOAT32:
      return vectorF32SerializeToBlob(p, blob, blobSize);
    case VECTOR_TYPE_FLOAT64:
      return vectorF64SerializeToBlob(p, blob, blobSize);
    default:
      assert(0);
  }
  return 0;
}

size_t vectorDeserializeFromBlob(Vector *p, const unsigned char *blob, size_t blobSize){
  switch (p->type) {
    case VECTOR_TYPE_FLOAT32:
      return vectorF32DeserializeFromBlob(p, blob, blobSize);
    case VECTOR_TYPE_FLOAT64:
      
    default:
      assert(0);
  }
  return 0;
}

/**************************************************************************
** Vector index cursor implementations
****************************************************************************/

/*
** A VectorIdxCursor is a special cursor to perform vector index lookups.
 */
struct VectorIdxCursor {
  sqlite3 *db;          /* Database connection */
  DiskAnnIndex *index;   /* DiskANN index on disk */
};

static int parseVectorIndexParameter(const char *zParam, VectorIndexParameters *pParameters, char **pErrMsg) {
  int i;
  int iDelimiter = 0;
  const char* zValue;
  while( zParam[iDelimiter] && zParam[iDelimiter] != '=' ){
    iDelimiter++;
  }
  if( zParam[iDelimiter] != '=' ){
    *pErrMsg = "unexpected parameter format";
    return -1;
  }
  zValue = zParam + iDelimiter + 1;
  for(i = 0; i < ArraySize(VECTOR_INDEX_PARAMETERS); i++){
    if( sqlite3_strnicmp(VECTOR_INDEX_PARAMETERS[i].zName, zParam, iDelimiter) != 0 ){
      continue;
    }
    if( VECTOR_INDEX_PARAMETERS[i].conversion(zValue, pParameters) != 0 ){
      *pErrMsg = "invalid parameter value";
      return -1;
    }
    return 0;
  }
  *pErrMsg = "unexpected parameter key";
  return -1;
}

/**
** Parses a type string such as `FLOAT32(3)` and set number of dimensions and bits
**
** Returns  0 if suceed and set correct values in both nDimensions and nType pointers
** Returns -1 if the type string is not a valid vector type for index and set pErrMsg to static string with error description in this case
**/
static int parseVectorDimensionsForIndex(const char *zType, int *nDimensions, int *nType, char **pErrMsg){
  int dimensions = 0;
  int i;
  for(i = 0; i < ArraySize(VECTOR_COLUMN_TYPES); i++){
    const char* name = VECTOR_COLUMN_TYPES[i].zName;
    const char* zTypePtr = zType + strlen(name);
    if( sqlite3_strnicmp(zType, name, strlen(name)) != 0 ){
      continue;
    }
    if( *zTypePtr != '(' ) {
      break;
    }
    zTypePtr++;

    while( *zTypePtr && *zTypePtr != ')' ){
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
    if( *zTypePtr != ')' ){
      *pErrMsg = "missed closing brace for vector column type";
      return -1;
    }
    zTypePtr++;

    if( *zTypePtr ) {
      *pErrMsg = "extra data after dimension parameter for vector column type";
      return -1;
    }

    if( dimensions <= 0 ){
      *pErrMsg = "vector column must have non-zero dimension for index";
      return -1;
    }

    *nDimensions = dimensions;
    if( VECTOR_COLUMN_TYPES[i].nBits == 32 ) {
      *nType = VECTOR_TYPE_FLOAT32;
    } else if( VECTOR_COLUMN_TYPES[i].nBits == 64 ) {
      *nType = VECTOR_TYPE_FLOAT64;
    } else {
      *pErrMsg = "unsupported vector type";
      return -1;
    }
    return 0;
  }
  *pErrMsg = "unexpected vector column type";
  return -1;
}

int initVectorIndexMetaTable(sqlite3* db) {
  static const char *zSql = "CREATE TABLE IF NOT EXISTS " VECTOR_INDEX_GLOBAL_META_TABLE " ( name TEXT, metadata BLOB );";
  return sqlite3_exec(db, zSql, 0, 0, 0);
}

int insertIndexParameters(sqlite3* db, const char *zName, VectorIndexParameters *pParameters) {
  static const char *zSql = "INSERT INTO " VECTOR_INDEX_GLOBAL_META_TABLE " VALUES (?, ?)";
  u8 parametersBin[VECTOR_INDEX_PARAMETERS_COUNT * VECTOR_INDEX_PARAMETER_BIN_LENGTH];
  sqlite3_stmt* pStatement = 0;
  int rc = SQLITE_ERROR;

  rc = sqlite3_prepare_v2(db, zSql, -1, &pStatement, 0);
  if( rc != SQLITE_OK ){
    goto clear_and_exit;
  }
  rc = sqlite3_bind_text(pStatement, 1, zName, -1, 0);
  if( rc != SQLITE_OK ){
    goto clear_and_exit;
  }
  serializeVectorIndexParameters(pParameters, parametersBin, sizeof(parametersBin));

  rc = sqlite3_bind_blob(pStatement, 2, parametersBin, sizeof(parametersBin), 0);
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
  if( pStatement ){
    sqlite3_finalize(pStatement);
  }
  return rc;
}

int vectorIndexCreate(Parse *pParse, Index *pIdx, IdList *pUsing) {
  sqlite3* db = pParse->db;
  int rc;
  int i;
  struct ExprList_item *pListItem;
  ExprList *pArgsList;
  int iEmbeddingColumn;
  char* zEmbeddingColumnTypeName;
  int nEmbeddingDim;
  int nEmbeddingType;
  char *pErrMsg;
  int hasLibsqlVectorIdxFn = 0;
  struct VectorIndexParameters parameters = createDefaultVectorIndexParameters();

  // backward compatibility: preserve old indices with deprecated syntax but forbid creation of new indices with this syntax
  if( pParse->db->init.busy == 0 && pUsing != 0 ){
    sqlite3ErrorMsg(pParse, "USING syntax is deprecated, please use plain CREATE INDEX: CREATE INDEX xxx ON yyy ( " VECTOR_INDEX_MARKER_FUNCTION "(zzz) )");
    goto failed;
  }
  if( pParse->db->init.busy == 1 && pUsing != 0 ){
    goto succeed;
  }

  // vector index must have expressions over column
  if( pIdx->aColExpr == 0 ) {
    goto ignored;
  }

  pListItem = pIdx->aColExpr->a;
  for(i=0; i<pIdx->aColExpr->nExpr; i++, pListItem++){
    Expr* pExpr = pListItem->pExpr;
    if( pExpr->op == TK_FUNCTION && sqlite3StrICmp(pExpr->u.zToken, VECTOR_INDEX_MARKER_FUNCTION) == 0 ) {
      hasLibsqlVectorIdxFn = 1;
    }
  }
  if( !hasLibsqlVectorIdxFn ) {
    goto ignored;
  }
  if( pIdx->aColExpr->nExpr != 1 ) {
    sqlite3ErrorMsg(pParse, "vector index must contain exactly one column wrapped into the " VECTOR_INDEX_MARKER_FUNCTION " function");
    goto failed;
  }
  if( pIdx->pPartIdxWhere != 0 ) {
    sqlite3ErrorMsg(pParse, "partial vector index is not supported");
    goto failed;
  }

  pArgsList = pIdx->aColExpr->a[0].pExpr->x.pList;
  pListItem = pArgsList->a;

  if( pArgsList->nExpr < 1 ){
    sqlite3ErrorMsg(pParse, VECTOR_INDEX_MARKER_FUNCTION " must contain at least one argument");
    goto failed;
  }
  if( pListItem[0].pExpr->op != TK_COLUMN ) {
    sqlite3ErrorMsg(pParse, VECTOR_INDEX_MARKER_FUNCTION " first argument must be a column token");
    goto failed;
  }
  iEmbeddingColumn = pListItem[0].pExpr->iColumn;
  if( iEmbeddingColumn < 0 ) {
    sqlite3ErrorMsg(pParse, VECTOR_INDEX_MARKER_FUNCTION " first argument must be column with vector type");
    goto failed;
  }
  assert( iEmbeddingColumn >= 0 && iEmbeddingColumn < pIdx->pTable->nCol );

  zEmbeddingColumnTypeName = sqlite3ColumnType(&pIdx->pTable->aCol[iEmbeddingColumn], "");
  if( parseVectorDimensionsForIndex(zEmbeddingColumnTypeName, &nEmbeddingDim, &nEmbeddingType, &pErrMsg) != 0 ){
    sqlite3ErrorMsg(pParse, "%s: %s", pErrMsg, zEmbeddingColumnTypeName);
    goto failed;
  }

  parameters.vectorDimension = nEmbeddingDim;
  parameters.vectorType = nEmbeddingType;
  for(i = 1; i < pArgsList->nExpr; i++){
    Expr *pArgExpr = pListItem[i].pExpr;
    if( pArgExpr->op != TK_STRING ){
      sqlite3ErrorMsg(pParse, "all arguments after first must be strings");
      goto failed;
    }
    if( parseVectorIndexParameter(pArgExpr->u.zToken, &parameters, &pErrMsg) != 0 ){
      sqlite3ErrorMsg(pParse, "invalid vector index parameter '%s': %s", pArgExpr->u.zToken, pErrMsg);
      goto failed;
    }
  }

  // we deliberately ignore return code as SQLite will execute CREATE INDEX command twice and exec will fail on second attempt
  // todo: actually, it looks pretty fragile - maybe we should avoid sqlite3_exec calls from the inside of SQLite internals...
  if( initVectorIndexMetaTable(db) != SQLITE_OK ){
    goto succeed;
  }
  if( insertIndexParameters(db, pIdx->zName, &parameters) != 0 ){
    sqlite3ErrorMsg(pParse, "unable to update global metadata table");
    goto failed;
  }
  if( diskAnnInitIndex(db, pIdx->zName, &parameters) != 0 ){
    sqlite3ErrorMsg(pParse, "unable to initialize diskann vector index");
    goto failed;
  }
succeed:
  pIdx->idxType = SQLITE_IDXTYPE_VECTOR;
  return SQLITE_OK;
ignored:
  return SQLITE_OK;
failed:
  return SQLITE_ERROR;
}

int vectorIndexInsert(
  VectorIdxCursor *pCur,
  const BtreePayload *pX
){
  struct sqlite3_value *rowid;
  struct sqlite3_value *vec;
  UnpackedRecord r;
  r.aMem = pX->aMem;
  r.nField = pX->nMem;
  assert( r.nField == 2 );
  vec = r.aMem + 0;
  if( sqlite3_value_type(vec)==SQLITE_NULL ){
    return SQLITE_OK;
  }
  assert( sqlite3_value_type(vec) == SQLITE_BLOB );
  rowid = r.aMem + 1;
  assert( sqlite3_value_type(rowid) == SQLITE_INTEGER );
  Vector v;
  vectorInitStatic(&v, VECTOR_TYPE_FLOAT32, sqlite3_value_blob(vec), sqlite3_value_bytes(vec));
  return diskAnnInsert(pCur->index, &v, sqlite3_value_int64(rowid));
}

int vectorIndexDelete(
  VectorIdxCursor *pCur,
  const UnpackedRecord *r
){
  struct sqlite3_value *rowid;
  rowid = r->aMem + 1;
  return diskAnnDelete(pCur->index, sqlite3_value_int64(rowid));
}

int vectorIndexCursorInit(
  sqlite3 *db,
  VectorIdxCursor **pCursor,
  const char *zIndexName
){
  const char *zDbPath;
  int rc;

  *pCursor = sqlite3DbMallocZero(db, sizeof(VectorIdxCursor));
  if( pCursor == 0 ){
    return SQLITE_NOMEM_BKPT;
  }
  rc = diskAnnOpenIndex(db, zIndexName, &(*pCursor)->index);
  if( rc!=SQLITE_OK ){
    return rc;
  }
  (*pCursor)->db = db;
  return SQLITE_OK;
}

void vectorIndexCursorClose(sqlite3 *db, VectorIdxCursor *pCursor){
  diskAnnCloseIndex(pCursor->index);
  sqlite3DbFree(db, pCursor);
}

/**************************************************************************
** SQL function implementations
****************************************************************************/

/*
** Implementation of vector(X) function.
*/
static void vectorFunc(
  sqlite3_context *context,
  int argc,
  sqlite3_value **argv
){
  char *zErrMsg = 0;
  Vector *pVec;
  int rc;
  if( argc < 1 ){
    return;
  }
  pVec = vectorContextAlloc(context, VECTOR_TYPE_FLOAT32);
  if( pVec==NULL ){
    return;
  }
  rc = vectorParse(argv[0], pVec, &zErrMsg);
  if( rc<0 ){
    sqlite3_result_error(context, zErrMsg, -1);
    sqlite3_free(zErrMsg);
    goto out_free_vec;
  }
  vectorSerialize(context, pVec);
out_free_vec:
  vectorFree(pVec);
}

/*
** Implementation of vector_extract(X) function.
*/
static void vectorExtractFunc(
  sqlite3_context *context,
  int argc,
  sqlite3_value **argv
){
  const unsigned char *zStr;
  char *zErrMsg = 0;
  Vector *pVec;
  unsigned i;

  if( argc < 1 ){
    return;
  }
  // do we need to add type + dimension to the vector blob?
  pVec = vectorContextAlloc(context, VECTOR_TYPE_FLOAT32);
  if( pVec==NULL ){
    return;
  }
  if( vectorParse(argv[0], pVec, &zErrMsg)<0 ){
    sqlite3_result_error(context, zErrMsg, -1);
    sqlite3_free(zErrMsg);
    goto out_free;
  }
  vectorDeserialize(context, pVec);
out_free:
  vectorFree(pVec);
}

/*
** Implementation of vector_distance_cos(X, Y) function.
*/
static void vectorDistanceCosFunc(
  sqlite3_context *context,
  int argc,
  sqlite3_value **argv
){
  Vector *pVec1, *pVec2;
  char *zErrMsg = 0;
  if( argc < 2 ) {
    return;
  }
  pVec1 = vectorContextAlloc(context, VECTOR_TYPE_FLOAT32);
  if( pVec1==NULL ){
    return;
  }
  pVec2 = vectorContextAlloc(context, VECTOR_TYPE_FLOAT32);
  if( pVec2==NULL ){
    goto out_free_vec1;
  }
  if( vectorParse(argv[0], pVec1, &zErrMsg)<0 ){
    sqlite3_result_error(context, zErrMsg, -1);
    sqlite3_free(zErrMsg);
    goto out_free_vec2;
  }
  if( vectorParse(argv[1], pVec2, &zErrMsg)<0 ){
    sqlite3_result_error(context, zErrMsg, -1);
    sqlite3_free(zErrMsg);
    goto out_free_vec2;
  }
  if( pVec1->dims != pVec2->dims ){
    sqlite3_result_error(context, "vectors must have the same length", -1);
    goto out_free_vec2;
  }
  if( pVec1->type != pVec2->type ){
    sqlite3_result_error(context, "vectors must have the same type", -1);
    goto out_free_vec2;
  }
  sqlite3_result_double(context, vectorDistanceCos(pVec1, pVec2));
out_free_vec2:
  vectorFree(pVec2);
out_free_vec1:
  vectorFree(pVec1);
}

/*
** Register vector functions.
*/
static void libsqlVectorIdx(sqlite3_context *context, int argc, sqlite3_value **argv){ 
  sqlite3_result_blob(context, sqlite3_value_blob(argv[0]), sqlite3_value_bytes(argv[0]), SQLITE_STATIC);
}

void sqlite3RegisterVectorFunctions(void){
 static FuncDef aVectorFuncs[] = {
    VECTOR_FUNCTION(vector_distance_cos,  2, 0, 0, vectorDistanceCosFunc),

    FUNCTION(vector,         1, 0, 0, vectorFunc),
    FUNCTION(vector_extract, 1, 0, 0, vectorExtractFunc),

    FUNCTION(libsql_vector_idx,  -1, 0, 0, libsqlVectorIdx),
  };
  sqlite3InsertBuiltinFuncs(aVectorFuncs, ArraySize(aVectorFuncs));
}

#endif /* !defined(SQLITE_OMIT_VECTOR) */

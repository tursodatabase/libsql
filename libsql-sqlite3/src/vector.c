/*
** 2024-07-04
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
** libSQL basic vector functions
*/
#ifndef SQLITE_OMIT_VECTOR

#include "sqliteInt.h"
#include "vectorInt.h"

#define MAX_FLOAT_CHAR_SZ  1024

/**************************************************************************
** Utility routines for dealing with Vector objects
**************************************************************************/

size_t vectorDataSize(VectorType type, VectorDims dims){
  switch( type ){
    case VECTOR_TYPE_FLOAT32:
      return dims * sizeof(float);
    case VECTOR_TYPE_FLOAT64:
      return dims * sizeof(double);
    case VECTOR_TYPE_1BIT:
      assert( dims > 0 );
      return (dims + 7) / 8;
    default:
      assert(0);
  }
  return 0;
}

void vectorInit(Vector *pVector, VectorType type, VectorDims dims, void *data){
  pVector->type = type;
  pVector->dims = dims;
  pVector->data = data;
  pVector->flags = 0;
}

/*
 * Allocate a Vector object and its data buffer
*/
Vector *vectorAlloc(VectorType type, VectorDims dims){
  void *pVector = sqlite3_malloc(sizeof(Vector) + vectorDataSize(type, dims));
  if( pVector==NULL ){
    return NULL;
  }
  vectorInit(pVector, type, dims, ((char*) pVector) + sizeof(Vector));
  return pVector;
}

/*
** Initialize a static Vector object.
**
** Note that the vector object points to the blob so if
** you free the blob, the vector becomes invalid.
**/
void vectorInitStatic(Vector *pVector, VectorType type, VectorDims dims, void *pBlob){
  pVector->flags = VECTOR_FLAGS_STATIC;
  pVector->type = type;
  pVector->dims = dims;
  pVector->data = pBlob;
}

/*
 * Allocate a Vector object and its data buffer from the SQLite context. 
*/
static Vector* vectorContextAlloc(sqlite3_context *context, int type, int dims){
  void *pVector = sqlite3_malloc64(sizeof(Vector) + vectorDataSize(type, dims));
  if( pVector==NULL ){
    sqlite3_result_error_nomem(context);
    return NULL;
  }
  vectorInit(pVector, type, dims, ((char*) pVector) + sizeof(Vector));
  return pVector;
}

/*
 * Free a Vector object and its data buffer allocated, unless the vector is static.
*/
void vectorFree(Vector *pVector){
  if( pVector == NULL ){
    return;
  }
  if( pVector->flags & VECTOR_FLAGS_STATIC ){
    return;
  }
  sqlite3_free(pVector);
}

float vectorDistanceCos(const Vector *pVector1, const Vector *pVector2){
  assert( pVector1->type == pVector2->type );
  switch (pVector1->type) {
    case VECTOR_TYPE_FLOAT32:
      return vectorF32DistanceCos(pVector1, pVector2);
    case VECTOR_TYPE_FLOAT64:
      return vectorF64DistanceCos(pVector1, pVector2);
    case VECTOR_TYPE_1BIT:
      return vector1BitDistanceHamming(pVector1, pVector2);
    default:
      assert(0);
  }
  return 0;
}

float vectorDistanceL2(const Vector *pVector1, const Vector *pVector2){
  assert( pVector1->type == pVector2->type );
  switch (pVector1->type) {
    case VECTOR_TYPE_FLOAT32:
      return vectorF32DistanceL2(pVector1, pVector2);
    case VECTOR_TYPE_FLOAT64:
      return vectorF64DistanceL2(pVector1, pVector2);
    default:
      assert(0);
  }
  return 0;
}

const char *sqlite3_type_repr(int type){
  switch( type ){
    case SQLITE_NULL:
      return "NULL";
    case SQLITE_INTEGER:
      return "INTEGER";
    case SQLITE_FLOAT:
      return "FLOAT";
    case SQLITE_BLOB:
      return "BLOB";
    case SQLITE_TEXT:
      return "TEXT";
    default:
      return "UNKNOWN";
  }
}
/*
 * Parses vector from text representation (e.g. '[1,2,3]'); vector type must be set
*/
static int vectorParseSqliteText(
  sqlite3_value *arg,
  Vector *pVector,
  char **pzErrMsg
){
  const unsigned char *pzText;
  double elem;
  float *elemsFloat;
  double *elemsDouble;
  int iElem = 0;
  // one more extra character in order to safely print data from elBuf with
  // printf-like method; will be set to zero later
  char valueBuf[MAX_FLOAT_CHAR_SZ + 1];
  int iBuf = 0;

  assert( pVector->type == VECTOR_TYPE_FLOAT32 || pVector->type == VECTOR_TYPE_FLOAT64 );
  assert( sqlite3_value_type(arg) == SQLITE_TEXT );

  if( pVector->type == VECTOR_TYPE_FLOAT32 ){
    elemsFloat = pVector->data;
  } else if( pVector->type == VECTOR_TYPE_FLOAT64 ){
    elemsDouble = pVector->data;
  }

  pzText = sqlite3_value_text(arg);
  if ( pzText == NULL ) return 0;

  while( sqlite3Isspace(*pzText) )
    pzText++;

  if( *pzText != '[' ){
    *pzErrMsg = sqlite3_mprintf("vector: must start with '['");
    goto error;
  }
  pzText++;

  // clear elBuf when we are ready to parse floats
  memset(valueBuf, 0, sizeof(valueBuf));

  for(; *pzText != '\0'; pzText++){
    char this = *pzText;
    if( sqlite3Isspace(this) ){
      continue;
    }
    if( this != ',' && this != ']' ){
      if( iBuf > MAX_FLOAT_CHAR_SZ ){
        *pzErrMsg = sqlite3_mprintf("vector: float string length exceeded %d characters: '%s'", MAX_FLOAT_CHAR_SZ, valueBuf);
        goto error;
      }
      valueBuf[iBuf++] = this;
      continue;
    }
    // empty vector case: '[]'
    if( this == ']' && iElem == 0 && iBuf == 0 ){
      break;
    }
    if( sqlite3AtoF(valueBuf, &elem, iBuf, SQLITE_UTF8) <= 0 ){
      *pzErrMsg = sqlite3_mprintf("vector: invalid float at position %d: '%s'", iElem, valueBuf);
      goto error;
    }
    if( iElem >= MAX_VECTOR_SZ ){
      *pzErrMsg = sqlite3_mprintf("vector: max size exceeded %d", MAX_VECTOR_SZ);
      goto error;
    }
    // clear only first bufidx positions - all other are zero
    memset(valueBuf, 0, iBuf);
    iBuf = 0;
    if( pVector->type == VECTOR_TYPE_FLOAT32 ){
      elemsFloat[iElem++] = elem;
    } else if( pVector->type == VECTOR_TYPE_FLOAT64 ){
      elemsDouble[iElem++] = elem;
    }
    if( this == ']' ){
      break;
    }
  }
  while( sqlite3Isspace(*pzText) )
    pzText++;

  if( *pzText != ']' ){
    *pzErrMsg = sqlite3_mprintf("vector: must end with ']'");
    goto error;
  }
  pzText++;

  while( sqlite3Isspace(*pzText) )
    pzText++;
  
  if( *pzText != '\0' ){
    *pzErrMsg = sqlite3_mprintf("vector: non-space symbols after closing ']' are forbidden");
    goto error;
  }
  pVector->dims = iElem;
  return 0;
error:
  return -1;
}

int vectorParseSqliteBlobWithType(
  sqlite3_value *arg,
  Vector *pVector,
  char **pzErrMsg
){
  const unsigned char *pBlob;
  size_t nBlobSize;

  assert( sqlite3_value_type(arg) == SQLITE_BLOB );

  pBlob = sqlite3_value_blob(arg);
  nBlobSize = sqlite3_value_bytes(arg);
  if( nBlobSize % 2 == 1 ){
    nBlobSize--;
  }

  if( nBlobSize < vectorDataSize(pVector->type, pVector->dims) ){
    *pzErrMsg = sqlite3_mprintf("invalid vector: not enough bytes: type=%d, dims=%d, size=%ull", pVector->type, pVector->dims, nBlobSize);
    return SQLITE_ERROR;
  }

  switch (pVector->type) {
    case VECTOR_TYPE_FLOAT32: 
      vectorF32DeserializeFromBlob(pVector, pBlob, nBlobSize);
      return 0;
    case VECTOR_TYPE_FLOAT64: 
      vectorF64DeserializeFromBlob(pVector, pBlob, nBlobSize);
      return 0;
    default: 
      assert(0);
  }
  return -1;
}

int detectBlobVectorParameters(sqlite3_value *arg, int *pType, int *pDims, char **pzErrMsg) {
  const u8 *pBlob;
  int nBlobSize;
  
  assert( sqlite3_value_type(arg) == SQLITE_BLOB );

  pBlob = sqlite3_value_blob(arg);
  nBlobSize = sqlite3_value_bytes(arg);
  if( nBlobSize % 2 != 0 ){ 
    // we have trailing byte with explicit type definition
    *pType = pBlob[nBlobSize - 1];
  } else { 
    // else, fallback to FLOAT32
    *pType = VECTOR_TYPE_FLOAT32;
  }
  if( *pType == VECTOR_TYPE_FLOAT32 ){
    *pDims = nBlobSize / sizeof(float);
  } else if( *pType == VECTOR_TYPE_FLOAT64 ){
    *pDims = nBlobSize / sizeof(double);
  } else{
    *pzErrMsg = sqlite3_mprintf("vector: unexpected binary type: got %d, expected %d or %d", *pType, VECTOR_TYPE_FLOAT32, VECTOR_TYPE_FLOAT64);
    return -1;
  }
  if( *pDims > MAX_VECTOR_SZ ){
    *pzErrMsg = sqlite3_mprintf("vector: max size exceeded: %d > %d", *pDims, MAX_VECTOR_SZ);
    return -1;
  }
  return 0;
}

int detectTextVectorParameters(sqlite3_value *arg, int typeHint, int *pType, int *pDims, char **pzErrMsg) {
  const u8 *text;
  int textBytes;
  int iText;
  int textHasDigit = 0;
  
  assert( sqlite3_value_type(arg) == SQLITE_TEXT );
  text = sqlite3_value_text(arg);
  textBytes = sqlite3_value_bytes(arg);
  if( typeHint == 0 ){ 
    *pType = VECTOR_TYPE_FLOAT32;
  }else if( typeHint == VECTOR_TYPE_FLOAT32 ){
    *pType = VECTOR_TYPE_FLOAT32;
  }else if( typeHint == VECTOR_TYPE_FLOAT64 ){
    *pType = VECTOR_TYPE_FLOAT64;
  }else{
    *pzErrMsg = sqlite3_mprintf("unexpected vector type");
    return -1;
  }
  *pDims = 0;
  for(iText = 0; iText < textBytes; iText++){
    if( text[iText] == ',' ){
      *pDims += 1;
    }
    if( sqlite3Isdigit(text[iText]) ){
      textHasDigit = 1;
    }
  }
  if( textHasDigit ){
    *pDims += 1;
  }
  return 0;
}

int detectVectorParameters(sqlite3_value *arg, int typeHint, int *pType, int *pDims, char **pzErrMsg) {
  switch( sqlite3_value_type(arg) ){
    case SQLITE_BLOB:
      return detectBlobVectorParameters(arg, pType, pDims, pzErrMsg);
    case SQLITE_TEXT:
      return detectTextVectorParameters(arg, typeHint, pType, pDims, pzErrMsg);
    default:
      *pzErrMsg = sqlite3_mprintf("vector: unexpected value type: got %s, expected TEXT or BLOB", sqlite3_type_repr(sqlite3_value_type(arg)));
      return -1;
  }
}

int vectorParseWithType(
  sqlite3_value *arg,
  Vector *pVector,
  char **pzErrMsg
){
  switch( sqlite3_value_type(arg) ){
    case SQLITE_BLOB:
      return vectorParseSqliteBlobWithType(arg, pVector, pzErrMsg);
    case SQLITE_TEXT:
      return vectorParseSqliteText(arg, pVector, pzErrMsg);
    default:
      *pzErrMsg = sqlite3_mprintf("vector: unexpected value type: got %s, expected TEXT or BLOB", sqlite3_type_repr(sqlite3_value_type(arg)));
      return -1;
  }
}

void vectorDump(const Vector *pVector){
  switch (pVector->type) {
    case VECTOR_TYPE_FLOAT32:
      vectorF32Dump(pVector);
      break;
    case VECTOR_TYPE_FLOAT64:
      vectorF64Dump(pVector);
      break;
    case VECTOR_TYPE_1BIT:
      vector1BitDump(pVector);
      break;
    default:
      assert(0);
  }
}

void vectorMarshalToText(
  sqlite3_context *context,
  const Vector *pVector
){
  switch (pVector->type) {
    case VECTOR_TYPE_FLOAT32:
      vectorF32MarshalToText(context, pVector);
      break;
    case VECTOR_TYPE_FLOAT64:
      vectorF64MarshalToText(context, pVector);
      break;
    default:
      assert(0);
  }
}

void vectorSerializeWithType(
  sqlite3_context *context,
  const Vector *pVector
){
  unsigned char *pBlob;
  size_t nBlobSize, nDataSize;

  assert( pVector->dims <= MAX_VECTOR_SZ );

  nDataSize = vectorDataSize(pVector->type, pVector->dims);
  nBlobSize = nDataSize;
  if( pVector->type != VECTOR_TYPE_FLOAT32 ){
    nBlobSize += (nBlobSize % 2 == 0 ? 1 : 2);
  }

  if( nBlobSize == 0 ){
    sqlite3_result_zeroblob(context, 0);
    return;
  }

  pBlob = sqlite3_malloc64(nBlobSize);
  if( pBlob == NULL ){
    sqlite3_result_error_nomem(context);
    return;
  }

  if( pVector->type != VECTOR_TYPE_FLOAT32 ){
    pBlob[nBlobSize - 1] = pVector->type;
  }

  switch (pVector->type) {
    case VECTOR_TYPE_FLOAT32:
      vectorF32SerializeToBlob(pVector, pBlob, nDataSize);
      break;
    case VECTOR_TYPE_FLOAT64:
      vectorF64SerializeToBlob(pVector, pBlob, nDataSize);
      break;
    default:
      assert(0);
  }
  sqlite3_result_blob(context, (char*)pBlob, nBlobSize, sqlite3_free);
}

size_t vectorSerializeToBlob(const Vector *pVector, unsigned char *pBlob, size_t nBlobSize){
  switch (pVector->type) {
    case VECTOR_TYPE_FLOAT32:
      return vectorF32SerializeToBlob(pVector, pBlob, nBlobSize);
    case VECTOR_TYPE_FLOAT64:
      return vectorF64SerializeToBlob(pVector, pBlob, nBlobSize);
    case VECTOR_TYPE_1BIT:
      return vector1BitSerializeToBlob(pVector, pBlob, nBlobSize);
    default:
      assert(0);
  }
  return 0;
}

void vectorInitFromBlob(Vector *pVector, const unsigned char *pBlob, size_t nBlobSize){
  switch (pVector->type) {
    case VECTOR_TYPE_FLOAT32:
      vectorF32InitFromBlob(pVector, pBlob, nBlobSize);
      break;
    case VECTOR_TYPE_FLOAT64:
      vectorF64InitFromBlob(pVector, pBlob, nBlobSize);
      break;
    default:
      assert(0);
  }
}

static void vectorConvertFromF32(const Vector *pFrom, Vector *pTo){
  int i;
  float *src;

  u8 *dst1Bit;
  double *dstF64;

  assert( pFrom->dims == pTo->dims );
  assert( pFrom->type != pTo->type );
  assert( pFrom->type == VECTOR_TYPE_FLOAT32 );

  src = pFrom->data;
  if( pTo->type == VECTOR_TYPE_FLOAT64 ){
    dstF64 = pTo->data;
    for(i = 0; i < pFrom->dims; i++){
      dstF64[i] = src[i];
    }
  }else if( pTo->type == VECTOR_TYPE_1BIT ){
    dst1Bit = pTo->data;
    for(i = 0; i < pFrom->dims; i += 8){
      dst1Bit[i / 8] = 0;
    }
    for(i = 0; i < pFrom->dims; i++){
      if( src[i] > 0 ){
        dst1Bit[i / 8] |= (1 << (i & 7));
      }
    }
  }else{
    assert( 0 );
  }
}

static void vectorConvertFromF64(const Vector *pFrom, Vector *pTo){
  int i;
  double *src;

  u8 *dst1Bit;
  float *dstF32;

  assert( pFrom->dims == pTo->dims );
  assert( pFrom->type != pTo->type );
  assert( pFrom->type == VECTOR_TYPE_FLOAT64 );

  src = pFrom->data;
  if( pTo->type == VECTOR_TYPE_FLOAT32 ){
    dstF32 = pTo->data;
    for(i = 0; i < pFrom->dims; i++){
      dstF32[i] = src[i];
    }
  }else if( pTo->type == VECTOR_TYPE_1BIT ){
    dst1Bit = pTo->data;
    for(i = 0; i < pFrom->dims; i += 8){
      dst1Bit[i / 8] = 0;
    }
    for(i = 0; i < pFrom->dims; i++){
      if( src[i] > 0 ){
        dst1Bit[i / 8] |= (1 << (i & 7));
      }
    }
  }else{
    assert( 0 );
  }
}

static void vectorConvertFrom1Bit(const Vector *pFrom, Vector *pTo){
  int i;
  u8 *src;

  float *dstF32;
  double *dstF64;

  assert( pFrom->dims == pTo->dims );
  assert( pFrom->type != pTo->type );
  assert( pFrom->type == VECTOR_TYPE_1BIT );

  src = pFrom->data;
  if( pTo->type == VECTOR_TYPE_FLOAT32 ){
    dstF32 = pTo->data;
    for(i = 0; i < pFrom->dims; i++){
      if( ((src[i / 8] >> (i & 7)) & 1) == 1 ){
        dstF32[i] = +1;
      }else{
        dstF32[i] = -1;
      }
    }
  }else if( pTo->type == VECTOR_TYPE_FLOAT64 ){
    dstF64 = pTo->data;
    for(i = 0; i < pFrom->dims; i++){
      if( ((src[i / 8] >> (i & 7)) & 1) == 1 ){
        dstF64[i] = +1;
      }else{
        dstF64[i] = -1;
      }
    }
  }else{
    assert( 0 );
  }
}

void vectorConvert(const Vector *pFrom, Vector *pTo){
  assert( pFrom->dims == pTo->dims );

  if( pFrom->type == pTo->type ){
    memcpy(pTo->data, pFrom->data, vectorDataSize(pFrom->type, pFrom->dims));
    return;
  }

  if( pFrom->type == VECTOR_TYPE_FLOAT32 ){
    vectorConvertFromF32(pFrom, pTo);
  }else if( pFrom->type == VECTOR_TYPE_FLOAT64 ){
    vectorConvertFromF64(pFrom, pTo);
  }else if( pFrom->type == VECTOR_TYPE_1BIT ){
    vectorConvertFrom1Bit(pFrom, pTo);
  }else{
    assert( 0 );
  }
}

/**************************************************************************
** SQL function implementations
****************************************************************************/

/*
** Generic vector(...) function with type hint
*/
static void vectorFuncHintedType(
  sqlite3_context *context,
  int argc,
  sqlite3_value **argv,
  int typeHint
){
  char *pzErrMsg = NULL;
  Vector *pVector;
  int type, dims;
  if( argc < 1 ){
    return;
  }
  if( detectVectorParameters(argv[0], typeHint, &type, &dims, &pzErrMsg) != 0 ){
    sqlite3_result_error(context, pzErrMsg, -1);
    sqlite3_free(pzErrMsg);
    return;
  }
  pVector = vectorContextAlloc(context, type, dims);
  if( pVector==NULL ){
    return;
  }
  if( vectorParseWithType(argv[0], pVector, &pzErrMsg) != 0 ){
    sqlite3_result_error(context, pzErrMsg, -1);
    sqlite3_free(pzErrMsg);
    goto out_free_vec;
  }
  vectorSerializeWithType(context, pVector);
out_free_vec:
  vectorFree(pVector);
}

static void vector32Func(
  sqlite3_context *context,
  int argc,
  sqlite3_value **argv
){
  vectorFuncHintedType(context, argc, argv, VECTOR_TYPE_FLOAT32);
}
static void vector64Func(
  sqlite3_context *context,
  int argc,
  sqlite3_value **argv
){
  vectorFuncHintedType(context, argc, argv, VECTOR_TYPE_FLOAT64);
}

/*
** Implementation of vector_extract(X) function.
*/
static void vectorExtractFunc(
  sqlite3_context *context,
  int argc,
  sqlite3_value **argv
){
  char *pzErrMsg = NULL;
  Vector *pVector;
  unsigned i;
  int type, dims;

  if( argc < 1 ){
    return;
  }
  if( detectVectorParameters(argv[0], 0, &type, &dims, &pzErrMsg) != 0 ){
    sqlite3_result_error(context, pzErrMsg, -1);
    sqlite3_free(pzErrMsg);
    return;
  }
  pVector = vectorContextAlloc(context, type, dims);
  if( pVector==NULL ){
    return;
  }
  if( vectorParseWithType(argv[0], pVector, &pzErrMsg)<0 ){
    sqlite3_result_error(context, pzErrMsg, -1);
    sqlite3_free(pzErrMsg);
    goto out_free;
  }
  vectorMarshalToText(context, pVector);
out_free:
  vectorFree(pVector);
}

/*
** Implementation of vector_distance_cos(X, Y) function.
*/
static void vectorDistanceCosFunc(
  sqlite3_context *context,
  int argc,
  sqlite3_value **argv
){
  char *pzErrMsg = NULL;
  Vector *pVector1 = NULL, *pVector2 = NULL;
  int type1, type2;
  int dims1, dims2;
  if( argc < 2 ) {
    return;
  }
  if( detectVectorParameters(argv[0], 0, &type1, &dims1, &pzErrMsg) != 0 ){
    sqlite3_result_error(context, pzErrMsg, -1);
    sqlite3_free(pzErrMsg);
    goto out_free;
  }
  if( detectVectorParameters(argv[1], 0, &type2, &dims2, &pzErrMsg) != 0 ){
    sqlite3_result_error(context, pzErrMsg, -1);
    sqlite3_free(pzErrMsg);
    goto out_free;
  }
  if( type1 != type2 ){
    pzErrMsg = sqlite3_mprintf("vector_distance_cos: vectors must have the same type: %d != %d", type1, type2);
    sqlite3_result_error(context, pzErrMsg, -1);
    sqlite3_free(pzErrMsg);
    goto out_free;
  }
  if( dims1 != dims2 ){
    pzErrMsg = sqlite3_mprintf("vector_distance_cos: vectors must have the same length: %d != %d", dims1, dims2);
    sqlite3_result_error(context, pzErrMsg, -1);
    sqlite3_free(pzErrMsg);
    goto out_free;
  }
  pVector1 = vectorContextAlloc(context, type1, dims1);
  if( pVector1==NULL ){
    goto out_free;
  }
  pVector2 = vectorContextAlloc(context, type2, dims2);
  if( pVector2==NULL ){
    goto out_free;
  }
  if( vectorParseWithType(argv[0], pVector1, &pzErrMsg)<0 ){
    sqlite3_result_error(context, pzErrMsg, -1);
    sqlite3_free(pzErrMsg);
    goto out_free;
  }
  if( vectorParseWithType(argv[1], pVector2, &pzErrMsg)<0 ){
    sqlite3_result_error(context, pzErrMsg, -1);
    sqlite3_free(pzErrMsg);
    goto out_free;
  }
  sqlite3_result_double(context, vectorDistanceCos(pVector1, pVector2));
out_free:
  if( pVector2 ){
    vectorFree(pVector2);
  }
  if( pVector1 ){
    vectorFree(pVector1);
  }
}

/*
 * Marker function which is used in index creation syntax: CREATE INDEX idx ON t(libsql_vector_idx(emb));
*/
static void libsqlVectorIdx(sqlite3_context *context, int argc, sqlite3_value **argv){ 
  // it's important for this function to be no-op as sqlite will apply this function to the column before feeding it to the index
  sqlite3_result_value(context, argv[0]);
}

/*
** Register vector functions.
*/
void sqlite3RegisterVectorFunctions(void){
 static FuncDef aVectorFuncs[] = {
    FUNCTION(vector,              1, 0, 0, vector32Func),
    FUNCTION(vector32,            1, 0, 0, vector32Func),
    FUNCTION(vector64,            1, 0, 0, vector64Func),
    FUNCTION(vector_extract,      1, 0, 0, vectorExtractFunc),
    FUNCTION(vector_distance_cos, 2, 0, 0, vectorDistanceCosFunc),

    FUNCTION(libsql_vector_idx,  -1, 0, 0, libsqlVectorIdx),
  };
  sqlite3InsertBuiltinFuncs(aVectorFuncs, ArraySize(aVectorFuncs));
}

#endif /* !defined(SQLITE_OMIT_VECTOR) */

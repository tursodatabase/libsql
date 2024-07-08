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
** 32-bit floating point vector format utilities.
*/
#ifndef SQLITE_OMIT_VECTOR
#include "sqliteInt.h"

#include "vectorInt.h"

#include <math.h>

/**************************************************************************
** Utility routines for debugging
**************************************************************************/

void vectorF32Dump(const Vector *pVec){
  float *elems = pVec->data;
  unsigned i;

  assert( pVec->type == VECTOR_TYPE_FLOAT32 );

  for(i = 0; i < pVec->dims; i++){
    printf("%f ", elems[i]);
  }
  printf("\n");
}

/**************************************************************************
** Utility routines for vector serialization and deserialization
**************************************************************************/

static inline unsigned formatF32(float value, char *pBuf, int nBufSize){
  sqlite3_snprintf(nBufSize, pBuf, "%g", (double)value);
  return strlen(pBuf);
}

static inline unsigned serializeF32(unsigned char *pBuf, float value){
  u32 *p = (u32 *)&value;
  pBuf[0] = *p & 0xFF;
  pBuf[1] = (*p >> 8) & 0xFF;
  pBuf[2] = (*p >> 16) & 0xFF;
  pBuf[3] = (*p >> 24) & 0xFF;
  return sizeof(float);
}

static inline float deserializeF32(const unsigned char *pBuf){
  u32 value = 0;
  value |= (u32)pBuf[0];
  value |= (u32)pBuf[1] << 8;
  value |= (u32)pBuf[2] << 16;
  value |= (u32)pBuf[3] << 24;
  return *(float *)&value;
}

size_t vectorF32SerializeToBlob(
  const Vector *pVector,
  unsigned char *pBlob,
  size_t nBlobSize
){
  float *elems = pVector->data;
  unsigned char *pPtr = pBlob;
  size_t len = 0;
  unsigned i;

  assert( pVector->type == VECTOR_TYPE_FLOAT32 );
  assert( pVector->dims <= MAX_VECTOR_SZ );
  assert( nBlobSize >= pVector->dims * sizeof(float) );

  for(i = 0; i < pVector->dims; i++){
    pPtr += serializeF32(pPtr, elems[i]);
  }
  return sizeof(float) * pVector->dims;
}

size_t vectorF32DeserializeFromBlob(
  Vector *pVector,
  const unsigned char *pBlob,
  size_t nBlobSize
){
  float *elems = pVector->data;
  unsigned i;
  pVector->type = VECTOR_TYPE_FLOAT32;
  pVector->dims = nBlobSize / sizeof(float);

  assert( pVector->dims <= MAX_VECTOR_SZ );
  assert( nBlobSize % 2 == 0 || pBlob[nBlobSize - 1] == VECTOR_TYPE_FLOAT32 );

  for(i = 0; i < pVector->dims; i++){
    elems[i] = deserializeF32(pBlob);
    pBlob += sizeof(float);
  }
  return vectorDataSize(pVector->type, pVector->dims);
}

void vectorF32Serialize(
  sqlite3_context *context,
  const Vector *pVector
){
  float *elems = pVector->data;
  unsigned char *pBlob;
  size_t nBlobSize;

  assert( pVector->type == VECTOR_TYPE_FLOAT32 );
  assert( pVector->dims <= MAX_VECTOR_SZ );

  nBlobSize = vectorDataSize(pVector->type, pVector->dims);

  if( nBlobSize == 0 ){
    sqlite3_result_zeroblob(context, 0);
    return;
  }

  pBlob = sqlite3_malloc64(nBlobSize);
  if( pBlob == NULL ){
    sqlite3_result_error_nomem(context);
    return;
  }

  vectorF32SerializeToBlob(pVector, pBlob, nBlobSize);
  sqlite3_result_blob(context, (char*)pBlob, nBlobSize, sqlite3_free);
}

#define SINGLE_FLOAT_CHAR_LIMIT 32
void vectorF32MarshalToText(
  sqlite3_context *context,
  const Vector *pVector
){
  float *elems = pVector->data;
  size_t nBufSize;
  size_t iBuf = 0;
  char *pText;
  char valueBuf[SINGLE_FLOAT_CHAR_LIMIT];

  assert( pVector->type == VECTOR_TYPE_FLOAT32 );
  assert( pVector->dims <= MAX_VECTOR_SZ );

  // there is no trailing comma - so we allocating 1 more extra byte; but this is fine
  nBufSize = 2 + pVector->dims * (SINGLE_FLOAT_CHAR_LIMIT + 1 /* plus comma */);
  pText = sqlite3_malloc64(nBufSize);
  if( pText != NULL ){
    unsigned i;

    pText[iBuf++]= '[';
    for(i = 0; i < pVector->dims; i++){ 
      unsigned valueLength = formatF32(elems[i], valueBuf, sizeof(valueBuf));
      memcpy(&pText[iBuf], valueBuf, valueLength);
      iBuf += valueLength;
      pText[iBuf++] = ',';
    }
    if( pVector->dims > 0 ){
      iBuf--;
    }
    pText[iBuf++] = ']';

    sqlite3_result_text(context, pText, iBuf, sqlite3_free);
  } else {
    sqlite3_result_error_nomem(context);
  }
}

float vectorF32DistanceCos(const Vector *v1, const Vector *v2){
  float dot = 0, norm1 = 0, norm2 = 0;
  float *e1 = v1->data;
  float *e2 = v2->data;
  int i;

  assert( v1->dims == v2->dims );
  assert( v1->type == VECTOR_TYPE_FLOAT32 );
  assert( v2->type == VECTOR_TYPE_FLOAT32 );

  for(i = 0; i < v1->dims; i++){
    dot += e1[i]*e2[i];
    norm1 += e1[i]*e1[i];
    norm2 += e2[i]*e2[i];
  }
  return 1.0 - (dot / sqrt(norm1 * norm2));
}

void vectorF32InitFromBlob(Vector *pVector, const unsigned char *pBlob, size_t nBlobSize){
  pVector->dims = nBlobSize / sizeof(float);
  pVector->data = (void*)pBlob;
}

int vectorF32ParseSqliteBlob(
  sqlite3_value *arg,
  Vector *pVector,
  char **pzErr
){
  const unsigned char *pBlob;
  float *elems = pVector->data;
  unsigned i;

  assert( pVector->type == VECTOR_TYPE_FLOAT32 );
  assert( 0 <= pVector->dims && pVector->dims <= MAX_VECTOR_SZ );

  if( sqlite3_value_type(arg) != SQLITE_BLOB ){
    *pzErr = sqlite3_mprintf("invalid f32 vector: not a blob type");
    goto error;
  }

  pBlob = sqlite3_value_blob(arg);
  if( sqlite3_value_bytes(arg) < sizeof(float) * pVector->dims ){
    *pzErr = sqlite3_mprintf("invalid f32 vector: not enough bytes for all dimensions");
    goto error;
  }

  for(i = 0; i < pVector->dims; i++){
    elems[i] = deserializeF32(pBlob);
    pBlob += sizeof(float);
  }
  return 0;
error:
  return -1;
}

#endif /* !defined(SQLITE_OMIT_VECTOR) */

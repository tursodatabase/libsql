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
** 64-bit floating point vector format utilities.
*/
#ifndef SQLITE_OMIT_VECTOR
#include "sqliteInt.h"

#include "vectorInt.h"

#include <math.h>

/**************************************************************************
** Utility routines for debugging
**************************************************************************/

void vectorF64Dump(const Vector *pVec){
  double *elems = pVec->data;
  unsigned i;
  for(i = 0; i < pVec->dims; i++){
    printf("%lf ", elems[i]);
  }
  printf("\n");
}

/**************************************************************************
** Utility routines for vector serialization and deserialization
**************************************************************************/

static inline unsigned formatF64(double value, char *pBuf, int nBufSize){
  sqlite3_snprintf(nBufSize, pBuf, "%g", value);
  return strlen(pBuf);
}

static inline unsigned serializeF64(unsigned char *pBuf, double value){
  u64 *p = (u64 *)&value;
  pBuf[0] = *p & 0xFF;
  pBuf[1] = (*p >> 8) & 0xFF;
  pBuf[2] = (*p >> 16) & 0xFF;
  pBuf[3] = (*p >> 24) & 0xFF;
  pBuf[4] = (*p >> 32) & 0xFF;
  pBuf[5] = (*p >> 40) & 0xFF;
  pBuf[6] = (*p >> 48) & 0xFF;
  pBuf[7] = (*p >> 56) & 0xFF;
  return sizeof(double);
}

static inline double deserializeF64(const unsigned char *pBuf){
  u64 value = 0;
  value |= (u64)pBuf[0];
  value |= (u64)pBuf[1] << 8;
  value |= (u64)pBuf[2] << 16;
  value |= (u64)pBuf[3] << 24;
  value |= (u64)pBuf[4] << 32;
  value |= (u64)pBuf[5] << 40;
  value |= (u64)pBuf[6] << 48;
  value |= (u64)pBuf[7] << 56;
  return *(double *)&value;
}

size_t vectorF64SerializeToBlob(
  const Vector *pVector,
  unsigned char *pBlob,
  size_t nBlobSize
){
  double *elems = pVector->data;
  unsigned char *pPtr = pBlob;
  unsigned i;

  assert( pVector->type == VECTOR_TYPE_FLOAT64 );
  assert( pVector->dims <= MAX_VECTOR_SZ );
  assert( nBlobSize >= pVector->dims * sizeof(double) );

  for (i = 0; i < pVector->dims; i++) {
    pPtr += serializeF64(pPtr, elems[i]);
  }
  return sizeof(double) * pVector->dims;
}

size_t vectorF64DeserializeFromBlob(
  Vector *pVector,
  const unsigned char *pBlob,
  size_t nBlobSize
){
  double *elems = pVector->data;
  unsigned i;
  pVector->type = VECTOR_TYPE_FLOAT64;
  pVector->dims = nBlobSize / sizeof(double);

  assert( pVector->dims <= MAX_VECTOR_SZ );
  assert( nBlobSize % 2 == 1 && pBlob[nBlobSize - 1] == VECTOR_TYPE_FLOAT64 );

  for(i = 0; i < pVector->dims; i++){
    elems[i] = deserializeF64(pBlob);
    pBlob += sizeof(double);
  }
  return vectorDataSize(pVector->type, pVector->dims);
}

void vectorF64Serialize(
  sqlite3_context *context,
  const Vector *pVector
){
  double *elems = pVector->data;
  unsigned char *pBlob;
  size_t nBlobSize;

  assert( pVector->type == VECTOR_TYPE_FLOAT64 );
  assert( pVector->dims <= MAX_VECTOR_SZ );

  // allocate one extra trailing byte with vector blob type metadata
  nBlobSize = vectorDataSize(pVector->type, pVector->dims) + 1;

  if( nBlobSize == 0 ){
    sqlite3_result_zeroblob(context, 0);
    return;
  }

  pBlob = sqlite3_malloc64(nBlobSize);
  if( pBlob == NULL ){
    sqlite3_result_error_nomem(context);
    return;
  }

  vectorF64SerializeToBlob(pVector, pBlob, nBlobSize - 1);
  pBlob[nBlobSize - 1] = VECTOR_TYPE_FLOAT64;

  sqlite3_result_blob(context, (char*)pBlob, nBlobSize, sqlite3_free);
}

#define SINGLE_DOUBLE_CHAR_LIMIT 32
void vectorF64MarshalToText(
  sqlite3_context *context,
  const Vector *pVector
){
  double *elems = pVector->data;
  size_t nBufSize;
  size_t iBuf = 0;
  char *pText;
  char valueBuf[SINGLE_DOUBLE_CHAR_LIMIT];

  assert( pVector->type == VECTOR_TYPE_FLOAT64 );
  assert( pVector->dims <= MAX_VECTOR_SZ );

  // there is no trailing comma - so we allocating 1 more extra byte; but this is fine
  nBufSize = 2 + pVector->dims * (SINGLE_DOUBLE_CHAR_LIMIT + 1 /* plus comma */);
  pText = sqlite3_malloc64(nBufSize);
  if( pText != NULL ){
    unsigned i;

    pText[iBuf++]= '[';
    for(i = 0; i < pVector->dims; i++){ 
      unsigned valueLength = formatF64(elems[i], valueBuf, sizeof(valueBuf));
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

double vectorF64DistanceCos(const Vector *v1, const Vector *v2){
  double dot = 0, norm1 = 0, norm2 = 0;
  double *e1 = v1->data;
  double *e2 = v2->data;
  int i;

  assert( v1->dims == v2->dims );
  assert( v1->type == VECTOR_TYPE_FLOAT64 );
  assert( v2->type == VECTOR_TYPE_FLOAT64 );

  for(i = 0; i < v1->dims; i++){
    dot += e1[i]*e2[i];
    norm1 += e1[i]*e1[i];
    norm2 += e2[i]*e2[i];
  }
  return 1.0 - (dot / sqrt(norm1 * norm2));
}

double vectorF64DistanceL2(const Vector *v1, const Vector *v2){
  double sum = 0;
  double *e1 = v1->data;
  double *e2 = v2->data;
  int i;

  assert( v1->dims == v2->dims );
  assert( v1->type == VECTOR_TYPE_FLOAT64 );
  assert( v2->type == VECTOR_TYPE_FLOAT64 );

  for(i = 0; i < v1->dims; i++){
    double d = e1[i]-e2[i];
    sum += d*d;
  }
  return sqrt(sum);
}

void vectorF64Mult(Vector *v, double k){
  double *e = v->data;
  int i;

  assert( v->type == VECTOR_TYPE_FLOAT64 );

  for(i = 0; i < v->dims; i++){
    e[i] *= k;
  }
}

void vectorF64Add(Vector *v1, const Vector *v2){
  double *e1 = v1->data;
  double *e2 = v2->data;
  int i;

  assert( v1->type == VECTOR_TYPE_FLOAT64 );
  assert( v1->type == v2->type );
  assert( v1->dims == v2->dims );

  for(i = 0; i < v1->dims; i++){
    e1[i] += e2[i];
  }
}

void vectorF64InitFromBlob(Vector *pVector, const unsigned char *pBlob, size_t nBlobSize){
  pVector->dims = nBlobSize / sizeof(double);
  pVector->data = (void*)pBlob;
}

int vectorF64ParseSqliteBlob(
  sqlite3_value *arg,
  Vector *pVector,
  char **pzErr
){
  const unsigned char *pBlob;
  double *elems = pVector->data;
  unsigned i;

  assert( pVector->type == VECTOR_TYPE_FLOAT64 );
  assert( 0 <= pVector->dims && pVector->dims <= MAX_VECTOR_SZ );
  assert( sqlite3_value_type(arg) == SQLITE_BLOB );

  pBlob = sqlite3_value_blob(arg);
  if( sqlite3_value_bytes(arg) < sizeof(double) * pVector->dims ){
    *pzErr = sqlite3_mprintf("invalid f64 vector: not enough bytes for all dimensions");
    goto error;
  }

  for(i = 0; i < pVector->dims; i++){
    elems[i] = deserializeF64(pBlob);
    pBlob += sizeof(double);
  }
  return 0;
error:
  return -1;
}

#endif /* !defined(SQLITE_OMIT_VECTOR) */

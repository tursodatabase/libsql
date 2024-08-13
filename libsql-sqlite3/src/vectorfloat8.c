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
** 8-bit (INT8) floating point vector format utilities.
**
** The idea is to replace vector [f_0, f_1, ... f_k] with quantized uint8 values [q_0, q_1, ..., q_k] in such a way that
** f_i = alpha * q_i + shift, when alpha and shift determined from all f_i values like that:
** alpha = (max(f) - min(f)) / 255, shift = min(f)
**
** This differs from uint8 quantization in neural-network as it usually take form of f_i = alpha * (q_i - z) conversion instead
** But, neural-network uint8 quantization is less generic and works better for distributions centered around zero (symmetric or not)
** In our implementation we want to handle more generic cases - so profits from neural-network-style quantization are not clear
*/
#ifndef SQLITE_OMIT_VECTOR
#include "sqliteInt.h"

#include "vectorInt.h"

#include <math.h>

/**************************************************************************
** Utility routines for vector serialization and deserialization
**************************************************************************/

void vectorF8GetParameters(const u8 *pData, int dims, float *pAlpha, float *pShift){
  pData = pData + ALIGN(dims, sizeof(float));
  *pAlpha = deserializeF32(pData);
  *pShift = deserializeF32(pData + sizeof(*pAlpha));
}

void vectorF8SetParameters(u8 *pData, int dims, float alpha, float shift){
  pData = pData + ALIGN(dims, sizeof(float));
  serializeF32(pData, alpha);
  serializeF32(pData + sizeof(alpha), shift);
}

void vectorF8Dump(const Vector *pVec){
  u8 *elems = pVec->data;
  float alpha, shift;
  unsigned i;

  assert( pVec->type == VECTOR_TYPE_FLOAT8 );

  vectorF8GetParameters(pVec->data, pVec->dims, &alpha, &shift);

  printf("f8: [");
  for(i = 0; i < pVec->dims; i++){
    printf("%s%f", i == 0 ? "" : ", ", (float)elems[i] * alpha + shift);
  }
  printf("]\n");
}

void vectorF8SerializeToBlob(
  const Vector *pVector,
  unsigned char *pBlob,
  size_t nBlobSize
){
  float alpha, shift;

  assert( pVector->type == VECTOR_TYPE_FLOAT8 );
  assert( pVector->dims <= MAX_VECTOR_SZ );
  assert( nBlobSize >= vectorDataSize(pVector->type, pVector->dims) );

  memcpy(pBlob, pVector->data, pVector->dims);

  vectorF8GetParameters(pVector->data, pVector->dims, &alpha, &shift);
  vectorF8SetParameters(pBlob, pVector->dims, alpha, shift);
}

float vectorF8DistanceCos(const Vector *v1, const Vector *v2){
  int i;
  float alpha1, shift1, alpha2, shift2;
  u32 sum1 = 0, sum2 = 0, sumsq1 = 0, sumsq2 = 0, doti = 0;
  float dot = 0, norm1 = 0, norm2 = 0;
  u8 *data1 = v1->data, *data2 = v2->data;

  assert( v1->dims == v2->dims );
  assert( v1->type == VECTOR_TYPE_FLOAT8 );
  assert( v2->type == VECTOR_TYPE_FLOAT8 );

  vectorF8GetParameters(v1->data, v1->dims, &alpha1, &shift1);
  vectorF8GetParameters(v2->data, v2->dims, &alpha2, &shift2);

  /*
   * (Ax + S)^2 = A^2 x^2 + 2AS x + S^2 -> we need to maintain 'sumsq' and 'sum'
   * (A1x + S1) * (A2y + S2) = A1A2 xy + A1 S2 x + A2 S1 y + S1 S2 -> we need to maintain 'dot' and 'sum' again
  */

  for(i = 0; i < v1->dims; i++){
    sum1 += data1[i];
    sum2 += data2[i];
    sumsq1 += data1[i]*data1[i];
    sumsq2 += data2[i]*data2[i];
    doti += data1[i]*data2[i];
  }

  dot = alpha1 * alpha2 * (float)doti + alpha1 * shift2 * (float)sum1 + alpha2 * shift1 * (float)sum2 + shift1 * shift2 * v1->dims;
  norm1 = alpha1 * alpha1 * (float)sumsq1 + 2 * alpha1 * shift1 * (float)sum1 + shift1 * shift1 * v1->dims;
  norm2 = alpha2 * alpha2 * (float)sumsq2 + 2 * alpha2 * shift2 * (float)sum2 + shift2 * shift2 * v1->dims;

  return 1.0 - (dot / sqrt(norm1 * norm2));
}

float vectorF8DistanceL2(const Vector *v1, const Vector *v2){
  int i;
  float alpha1, shift1, alpha2, shift2;
  float sum = 0;
  u8 *data1 = v1->data, *data2 = v2->data;

  assert( v1->dims == v2->dims );
  assert( v1->type == VECTOR_TYPE_FLOAT8 );
  assert( v2->type == VECTOR_TYPE_FLOAT8 );

  vectorF8GetParameters(v1->data, v1->dims, &alpha1, &shift1);
  vectorF8GetParameters(v2->data, v2->dims, &alpha2, &shift2);

  for(i = 0; i < v1->dims; i++){
    float d = (alpha1 * data1[i] + shift1) - (alpha2 * data2[i] + shift2);
    sum += d*d;
  }
  return sqrt(sum);
}

void vectorF8DeserializeFromBlob(
  Vector *pVector,
  const unsigned char *pBlob,
  size_t nBlobSize
){
  float alpha, shift;

  assert( pVector->type == VECTOR_TYPE_FLOAT8 );
  assert( 0 <= pVector->dims && pVector->dims <= MAX_VECTOR_SZ );
  assert( nBlobSize >= vectorDataSize(pVector->type, pVector->dims) );

  memcpy((u8*)pVector->data, (u8*)pBlob, ALIGN(pVector->dims, sizeof(float)));

  vectorF8GetParameters(pBlob, pVector->dims, &alpha, &shift);
  vectorF8SetParameters(pVector->data, pVector->dims, alpha, shift);
}

#endif /* !defined(SQLITE_OMIT_VECTOR) */

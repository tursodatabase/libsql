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
** 16-bit brain floating point vector format utilities.
**
** See https://en.wikipedia.org/wiki/Bfloat16_floating-point_format
*/
#ifndef SQLITE_OMIT_VECTOR
#include "sqliteInt.h"

#include "vectorInt.h"

#include <math.h>

/**************************************************************************
** Utility routines for vector serialization and deserialization
**************************************************************************/

float vectorFB16ToFloat(u16 f16){
  u32 f32 = (u32)f16 << 16;
  return *((float*)&f32);
}

u16 vectorFB16FromFloat(float f){
  u32 f32 = *((u32*)&f);
  return (u16)(f32 >> 16);
}

void vectorFB16Dump(const Vector *pVec){
  u16 *elems = pVec->data;
  unsigned i;

  assert( pVec->type == VECTOR_TYPE_FLOATB16 );

  printf("fb16: [");
  for(i = 0; i < pVec->dims; i++){
    printf("%s%f", i == 0 ? "" : ", ", vectorFB16ToFloat(elems[i]));
  }
  printf("]\n");
}

void vectorFB16SerializeToBlob(
  const Vector *pVector,
  unsigned char *pBlob,
  size_t nBlobSize
){
  assert( pVector->type == VECTOR_TYPE_FLOATB16 );
  assert( pVector->dims <= MAX_VECTOR_SZ );
  assert( nBlobSize >= vectorDataSize(pVector->type, pVector->dims) );

  memcpy(pBlob, pVector->data, pVector->dims * sizeof(u16));
}

float vectorFB16DistanceCos(const Vector *v1, const Vector *v2){
  int i;
  float dot = 0, norm1 = 0, norm2 = 0;
  float value1, value2;
  u16 *data1 = v1->data, *data2 = v2->data;

  assert( v1->dims == v2->dims );
  assert( v1->type == VECTOR_TYPE_FLOATB16 );
  assert( v2->type == VECTOR_TYPE_FLOATB16 );

  for(i = 0; i < v1->dims; i++){
    value1 = vectorFB16ToFloat(data1[i]);
    value2 = vectorFB16ToFloat(data2[i]);
    dot += value1*value2;
    norm1 += value1*value1;
    norm2 += value2*value2;
  }

  return 1.0 - (dot / sqrt(norm1 * norm2));
}

float vectorFB16DistanceL2(const Vector *v1, const Vector *v2){
  int i;
  float sum = 0;
  float value1, value2;
  u16 *data1 = v1->data, *data2 = v2->data;

  assert( v1->dims == v2->dims );
  assert( v1->type == VECTOR_TYPE_FLOATB16 );
  assert( v2->type == VECTOR_TYPE_FLOATB16 );

  for(i = 0; i < v1->dims; i++){
    value1 = vectorFB16ToFloat(data1[i]);
    value2 = vectorFB16ToFloat(data2[i]);
    float d = (value1 - value2);
    sum += d*d;
  }
  return sqrt(sum);
}

void vectorFB16DeserializeFromBlob(
  Vector *pVector,
  const unsigned char *pBlob,
  size_t nBlobSize
){
  assert( pVector->type == VECTOR_TYPE_FLOATB16 );
  assert( 0 <= pVector->dims && pVector->dims <= MAX_VECTOR_SZ );
  assert( nBlobSize >= vectorDataSize(pVector->type, pVector->dims) );

  memcpy((u8*)pVector->data, (u8*)pBlob, pVector->dims * sizeof(u16));
}

#endif /* !defined(SQLITE_OMIT_VECTOR) */


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
** 16-bit (FLOAT16) floating point vector format utilities.
**
** See https://en.wikipedia.org/wiki/Half-precision_floating-point_format
*/
#ifndef SQLITE_OMIT_VECTOR
#include "sqliteInt.h"

#include "vectorInt.h"

#include <math.h>

/**************************************************************************
** Utility routines for vector serialization and deserialization
**************************************************************************/

// f32: [fffffffffffffffffffffffeeeeeeees]
//       01234567890123456789012345678901
// f16: [ffffffffffeeeees]
//       0123456789012345

float vectorF16ToFloat(u16 f16){
  u32 f32;
  // sng: [0000000000000000000000000000000s]
  u32 sgn = ((u32)f16 & 0x8000) << 16;   
  
  int expBits = (f16 >> 10) & 0x1f;
  int exp = expBits - 15; // 15 is exp bias for f16

  u32 mnt = ((u32)f16 & 0x3ff);
  u32 mntNonZero = !!mnt;

  if( exp == 16 ){ // NaN or +/- Infinity
    exp = 128, mnt = mntNonZero << 22; // set mnt high bit to represent NaN if it was NaN in f16
  }else if( exp == -15 && mnt == 0 ){ // zero
    exp = -127, mnt = 0; 
  }else if( exp == -15 ){ // denormalized value
    // shift mantissa until we get 1 as a high bit
    exp++;
    while( (mnt & 0x400) == 0 ){
      mnt <<= 1;
      exp--;
    }
    // then reset high bit as this will be normal value (not denormalized) in f32
    mnt &= 0x3ff;
    mnt <<= 13;
  }else{
    mnt <<= 13;
  }
  f32 = sgn | ((u32)(exp + 127) << 23) | mnt;
  return *((float*)&f32);
}

u16 vectorF16FromFloat(float f){
  u32 i = *((u32*)&f);

  // sng: [000000000000000s]
  u32 sgn = (i >> 16) & (0x8000);

  // expBits: [eeeeeeee]
  int expBits = (i >> 23) & (0xff);
  int exp = expBits - 127; // 127 is exp bias for f32

  // mntBits: [fffffffffffffffffffffff]
  u32 mntBits = (i & 0x7fffff);
  u32 mntNonZero = !!mntBits;
  u32 mnt;

  if( exp == 128 ){ // NaN or +/- Infinity
    exp = 16, mntBits = mntNonZero << 22; // set mnt high bit to represent NaN if it was NaN in f32
  }else if( exp > 15 ){ // just too big numbers for f16
    exp = 16, mntBits = 0;
  }else if( exp < -14 && exp >= -25 ){ // small value, but we can be represented as denormalized f16
    // set high bit to 1 as normally mantissa has form 1.[mnt] but denormalized mantissa has form 0.[mnt]
    mntBits = (mntBits | 0x800000) >> (-exp - 14);
    exp = -15; 
  }else if( exp < -24 ){ // very small or denormalized value
    exp = -15, mntBits = 0;
  }
  // round to nearest, ties to even
  if( (mntBits & 0x1fff) > (0x1000 - ((mntBits >> 13) & 1)) ){
    mntBits += 0x2000;
  }
  mnt = mntBits >> 13;

  // handle overflow here (note, that overflow can happen only if exp < 16)
  return sgn | ((u32)(exp + 15 + (mnt >> 10)) << 10) | (mnt & 0x3ff);
}

void vectorF16Dump(const Vector *pVec){
  u16 *elems = pVec->data;
  unsigned i;

  assert( pVec->type == VECTOR_TYPE_FLOAT16 );

  printf("f16: [");
  for(i = 0; i < pVec->dims; i++){
    printf("%s%f", i == 0 ? "" : ", ", vectorF16ToFloat(elems[i]));
  }
  printf("]\n");
}

void vectorF16SerializeToBlob(
  const Vector *pVector,
  unsigned char *pBlob,
  size_t nBlobSize
){
  assert( pVector->type == VECTOR_TYPE_FLOAT16 );
  assert( pVector->dims <= MAX_VECTOR_SZ );
  assert( nBlobSize >= vectorDataSize(pVector->type, pVector->dims) );

  memcpy(pBlob, pVector->data, pVector->dims * sizeof(u16));
}

float vectorF16DistanceCos(const Vector *v1, const Vector *v2){
  int i;
  float dot = 0, norm1 = 0, norm2 = 0;
  float value1, value2;
  u16 *data1 = v1->data, *data2 = v2->data;

  assert( v1->dims == v2->dims );
  assert( v1->type == VECTOR_TYPE_FLOAT16 );
  assert( v2->type == VECTOR_TYPE_FLOAT16 );

  for(i = 0; i < v1->dims; i++){
    value1 = vectorF16ToFloat(data1[i]);
    value2 = vectorF16ToFloat(data2[i]);
    dot += value1*value2;
    norm1 += value1*value1;
    norm2 += value2*value2;
  }

  return 1.0 - (dot / sqrt(norm1 * norm2));
}

float vectorF16DistanceL2(const Vector *v1, const Vector *v2){
  int i;
  float sum = 0;
  float value1, value2;
  u16 *data1 = v1->data, *data2 = v2->data;

  assert( v1->dims == v2->dims );
  assert( v1->type == VECTOR_TYPE_FLOAT16 );
  assert( v2->type == VECTOR_TYPE_FLOAT16 );

  for(i = 0; i < v1->dims; i++){
    value1 = vectorF16ToFloat(data1[i]);
    value2 = vectorF16ToFloat(data2[i]);
    float d = (value1 - value2);
    sum += d*d;
  }
  return sqrt(sum);
}

void vectorF16DeserializeFromBlob(
  Vector *pVector,
  const unsigned char *pBlob,
  size_t nBlobSize
){
  assert( pVector->type == VECTOR_TYPE_FLOAT16 );
  assert( 0 <= pVector->dims && pVector->dims <= MAX_VECTOR_SZ );
  assert( nBlobSize >= vectorDataSize(pVector->type, pVector->dims) );

  memcpy((u8*)pVector->data, (u8*)pBlob, pVector->dims * sizeof(u16));
}

#endif /* !defined(SQLITE_OMIT_VECTOR) */

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
** 1-bit vector format utilities.
*/
#ifndef SQLITE_OMIT_VECTOR
#include "sqliteInt.h"

#include "vectorInt.h"

#include <math.h>

/**************************************************************************
** Utility routines for debugging
**************************************************************************/

void vector1BitDump(const Vector *pVec){
  u8 *elems = pVec->data;
  unsigned i;

  assert( pVec->type == VECTOR_TYPE_FLOAT1BIT );

  printf("f1bit: [");
  for(i = 0; i < pVec->dims; i++){
    printf("%s%d", i == 0 ? "" : ", ", ((elems[i / 8] >> (i & 7)) & 1) ? +1 : -1);
  }
  printf("]\n");
}

/**************************************************************************
** Utility routines for vector serialization and deserialization
**************************************************************************/

void vector1BitSerializeToBlob(
  const Vector *pVector,
  unsigned char *pBlob,
  size_t nBlobSize
){
  u8 *elems = pVector->data;
  u8 *pPtr = pBlob;
  unsigned i;

  assert( pVector->type == VECTOR_TYPE_FLOAT1BIT );
  assert( pVector->dims <= MAX_VECTOR_SZ );
  assert( nBlobSize >= vectorDataSize(pVector->type, pVector->dims) );

  for(i = 0; i < (pVector->dims + 7) / 8; i++){
    pPtr[i] = elems[i];
  }
}

// [sum(map(int, bin(i)[2:])) for i in range(256)]
static int BitsCount[256] = {
  0, 1, 1, 2, 1, 2, 2, 3, 1, 2, 2, 3, 2, 3, 3, 4,
  1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5,
  1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5,
  2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
  1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5,
  2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
  2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
  3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7,
  1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5,
  2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
  2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
  3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7,
  2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
  3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7,
  3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7,
  4, 5, 5, 6, 5, 6, 6, 7, 5, 6, 6, 7, 6, 7, 7, 8,
};

static inline int sqlite3PopCount32(u32 a){
#if GCC_VERSION>=5004000 && !defined(__INTEL_COMPILER)
  return __builtin_popcount(a);
#else
  return BitsCount[a >> 24] + BitsCount[(a >> 16) & 0xff] + BitsCount[(a >> 8) & 0xff] + BitsCount[a & 0xff];
#endif
}

int vector1BitDistanceHamming(const Vector *v1, const Vector *v2){
  int diff = 0;
  u8 *e1U8 = v1->data;
  u32 *e1U32 = v1->data;
  u8 *e2U8 = v2->data;
  u32 *e2U32 = v2->data;
  int i, len8, len32, offset8;

  assert( v1->dims == v2->dims );
  assert( v1->type == VECTOR_TYPE_FLOAT1BIT );
  assert( v2->type == VECTOR_TYPE_FLOAT1BIT );

  len8 = (v1->dims + 7) / 8;
  len32 = v1->dims / 32;
  offset8 = len32 * 4;

  for(i = 0; i < len32; i++){
    diff += sqlite3PopCount32(e1U32[i] ^ e2U32[i]);
  }
  for(i = offset8; i < len8; i++){
    diff += sqlite3PopCount32(e1U8[i] ^ e2U8[i]);
  }
  return diff;
}

void vector1BitDeserializeFromBlob(
  Vector *pVector,
  const unsigned char *pBlob,
  size_t nBlobSize
){
  u8 *elems = pVector->data;

  assert( pVector->type == VECTOR_TYPE_FLOAT1BIT );
  assert( 0 <= pVector->dims && pVector->dims <= MAX_VECTOR_SZ );
  assert( nBlobSize >= vectorDataSize(pVector->type, pVector->dims) );

  memcpy(elems, pBlob, (pVector->dims + 7) / 8);
}

#endif /* !defined(SQLITE_OMIT_VECTOR) */

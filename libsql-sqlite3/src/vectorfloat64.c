/*
** 2024-06-14
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

static inline unsigned formatF64(double num, char *str){
  char tmp[32];
  if (isInteger(num)) {
    return snprintf(tmp, 32, "%lld", (u64)num);
  } else {
    return snprintf(tmp, 32, "%.6e", num);
  }
}

void vectorF64Dump(Vector *pVec){
  double *elems = pVec->data;
  unsigned i;
  for(i = 0; i < pVec->dims; i++){
    printf("%f ", elems[i]);
  }
  printf("\n");
}

/**************************************************************************
** Utility routines for vector serialization and deserialization
**************************************************************************/

static inline unsigned serializeF64(unsigned char *mem, double num){
  u64 *p = (u64 *)&num;
  mem[0] = *p & 0xFF;
  mem[1] = (*p >> 8) & 0xFF;
  mem[2] = (*p >> 16) & 0xFF;
  mem[3] = (*p >> 24) & 0xFF;
  mem[4] = (*p >> 32) & 0xFF;
  mem[5] = (*p >> 40) & 0xFF;
  mem[6] = (*p >> 48) & 0xFF;
  mem[7] = (*p >> 56) & 0xFF;
  return sizeof(double);
}

static inline double deserializeF64(const unsigned char *mem){
  u64 p = 0;
  p |= (u64)mem[0];
  p |= (u64)mem[1] << 8;
  p |= (u64)mem[2] << 16;
  p |= (u64)mem[3] << 24;
  p |= (u64)mem[4] << 32;
  p |= (u64)mem[5] << 40;
  p |= (u64)mem[6] << 48;
  p |= (u64)mem[7] << 56;
  return *(double *)&p;
}

size_t vectorF64SerializeToBlob(
  Vector *v,
  unsigned char *blob,
  size_t blobSz
){
  double *elems = v->data;
  unsigned char *blobPtr = blob;
  size_t len = 0;
  assert( blobSz >= v->dims * sizeof(double) );
  for (unsigned i = 0; i < v->dims; i++) {
    blobPtr += serializeF64(blobPtr, elems[i]);
    len += sizeof(double);
  }
  return len;
}

size_t vectorF64DeserializeFromBlob(
  Vector *v,
  const unsigned char *blob,
  size_t blobSz
){
  double *elems = v->data;
  v->dims = blobSz / sizeof(double);
  assert( blobSz >= vectorDataSize(v->type, v->dims) );
  for (unsigned i = 0; i < v->dims; i++) {
    elems[i] = deserializeF64(blob);
    blob += sizeof(double);
  }
  return vectorDataSize(v->type, v->dims);
}

void vectorF64Serialize(
  sqlite3_context *context,
  Vector *v
){
  double *elems = v->data;
  unsigned char *blob;
  unsigned int blobSz;

  blobSz = vectorDataSize(v->type, v->dims);
  blob = contextMalloc(context, blobSz);

  if( blob ){
    vectorF64SerializeToBlob(v, blob, blobSz);

    sqlite3_result_blob(context, (char*)blob, blobSz, sqlite3_free);
  } else {
    sqlite3_result_error_nomem(context);
  }
}

void vectorF64Deserialize(
  sqlite3_context *context,
  Vector *v
){
  double *elems = v->data;
  unsigned bufSz;
  unsigned bufIdx = 0;
  char *z;

  bufSz = 2 + v->dims * 33;
  z = contextMalloc(context, bufSz);

  if( z ){
    unsigned i;

    z[bufIdx++]= '[';
    for (i = 0; i < v->dims; i++) { 
      char tmp[12];
      unsigned bytes = formatF64(elems[i], tmp);
      memcpy(&z[bufIdx], tmp, bytes);
      bufIdx += strlen(tmp);
      z[bufIdx++] = ',';
    }
    bufIdx--;
    z[bufIdx++] = ']';

    sqlite3_result_text(context, z, bufIdx, sqlite3_free);
  } else {
    sqlite3_result_error_nomem(context);
  }
}

double vectorF64DistanceCos(Vector *v1, Vector *v2){
  double dot = 0, norm1 = 0, norm2 = 0;
  double *e1 = v1->data;
  double *e2 = v2->data;
  int i;
  assert( v1->dims == v2->dims );
  for(i = 0; i < v1->dims; i++){
    dot += e1[i]*e2[i];
    norm1 += e1[i]*e1[i];
    norm2 += e2[i]*e2[i];
  }
  return 1.0 - (dot / sqrt(norm1 * norm2));
}

void vectorF64InitFromBlob(Vector *p, const unsigned char *blob, size_t blobSz){
  p->dims = blobSz / sizeof(double);
  p->data = (void*)blob;
}

int vectorF64ParseBlob(
  sqlite3_value *arg,
  Vector *v,
  char **pzErr
){
  const unsigned char *blob;
  double *elems = v->data;
  unsigned i;
  size_t len;
  size_t vectorBytes;

  if( sqlite3_value_type(arg)!=SQLITE_BLOB ){
    *pzErr = sqlite3_mprintf("invalid vector: not a blob type");
    goto error;
  }

  blob = sqlite3_value_blob(arg);
  if( !blob ) {
    *pzErr = sqlite3_mprintf("invalid vector: zero length");
    goto error;
  }
  vectorBytes = sqlite3_value_bytes(arg);
  if (vectorBytes % 8 != 0) {
    *pzErr = sqlite3_mprintf("invalid f64 vector: %d %% 8 != 0", vectorBytes);
    goto error;
  }
  len = vectorBytes / sizeof(double);
  if (len > MAX_VECTOR_SZ) {
    *pzErr = sqlite3_mprintf("invalid vector: too large: %d", len);
    goto error;
  }
  for(i = 0; i < len; i++){
    elems[i] = deserializeF64(blob);
    blob += sizeof(double);
  }
  v->dims = len;
  return len;
error:
  return -1;
}

#endif /* !defined(SQLITE_OMIT_VECTOR) */

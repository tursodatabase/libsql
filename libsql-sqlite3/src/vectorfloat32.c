/*
** 2024-03-23
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

void vectorF32Dump(Vector *pVec){
  float *elems = pVec->data;
  unsigned i;
  for(i = 0; i < pVec->dims; i++){
    printf("%f ", elems[i]);
  }
  printf("\n");
}

/**************************************************************************
** Utility routines for vector serialization and deserialization
**************************************************************************/

static inline unsigned formatF32(float num, char *str){
  char tmp[32];
  if (isInteger(num)) {
    return snprintf(tmp, 32, "%lld", (u64)num);
  } else {
    return snprintf(tmp, 32, "%.6e", num);
  }
}

static inline unsigned serializeF32(unsigned char *mem, float num){
  u32 *p = (u32 *)&num;
  mem[0] = *p & 0xFF;
  mem[1] = (*p >> 8) & 0xFF;
  mem[2] = (*p >> 16) & 0xFF;
  mem[3] = (*p >> 24) & 0xFF;
  return sizeof(float);
}

static inline float deserializeF32(const unsigned char *mem){
  u32 p = 0;
  p |= (u32)mem[0];
  p |= (u32)mem[1] << 8;
  p |= (u32)mem[2] << 16;
  p |= (u32)mem[3] << 24;
  return *(float *)&p;
}

size_t vectorF32SerializeToBlob(
  Vector *v,
  unsigned char *blob,
  size_t blobSz
){
  float *elems = v->data;
  unsigned char *blobPtr = blob;
  size_t len = 0;
  assert( blobSz >= v->dims * sizeof(float) );
  for (unsigned i = 0; i < v->dims; i++) {
    blobPtr += serializeF32(blobPtr, elems[i]);
    len += sizeof(float);
  }
  return len;
}

size_t vectorF32DeserializeFromBlob(
  Vector *v,
  const unsigned char *blob,
  size_t blobSz
){
  float *elems = v->data;
  v->dims = blobSz / sizeof(float);
  assert( blobSz >= vectorDataSize(v->type, v->dims) );
  for (unsigned i = 0; i < v->dims; i++) {
    elems[i] = deserializeF32(blob);
    blob += sizeof(float);
  }
  return vectorDataSize(v->type, v->dims);
}

void vectorF32Serialize(
  sqlite3_context *context,
  Vector *v
){
  float *elems = v->data;
  unsigned char *blob;
  unsigned int blobSz;

  blobSz = vectorDataSize(v->type, v->dims);
  blob = contextMalloc(context, blobSz);

  if( blob ){
    vectorF32SerializeToBlob(v, blob, blobSz);

    sqlite3_result_blob(context, (char*)blob, blobSz, sqlite3_free);
  } else {
    sqlite3_result_error_nomem(context);
  }
}

void vectorF32Deserialize(
  sqlite3_context *context,
  Vector *v
){
  float *elems = v->data;
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
      unsigned bytes = formatF32(elems[i], tmp);
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

float vectorF32DistanceCos(Vector *v1, Vector *v2){
  float dot = 0, norm1 = 0, norm2 = 0;
  float *e1 = v1->data;
  float *e2 = v2->data;
  int i;
  assert( v1->dims == v2->dims );
  for(i = 0; i < v1->dims; i++){
    dot += e1[i]*e2[i];
    norm1 += e1[i]*e1[i];
    norm2 += e2[i]*e2[i];
  }
  return 1.0 - (dot / sqrt(norm1 * norm2));
}

void vectorF32InitFromBlob(Vector *p, const unsigned char *blob, size_t blobSz){
  p->dims = blobSz / sizeof(float);
  p->data = (void*)blob;
}

int vectorF32ParseBlob(
  sqlite3_value *arg,
  Vector *v,
  char **pzErr
){
  const unsigned char *blob;
  float *elems = v->data;
  unsigned i;
  size_t len;
  size_t vectorBytes;

  if( sqlite3_value_type(arg)!=SQLITE_BLOB ){
    *pzErr = sqlite3_mprintf("invalid f32 vector: not a blob type");
    goto error;
  }

  blob = sqlite3_value_blob(arg);
  if( !blob ) {
    *pzErr = sqlite3_mprintf("invalid f32 vector: zero length");
    goto error;
  }
  vectorBytes = sqlite3_value_bytes(arg);
  if (vectorBytes % sizeof(float) != 0) {
    *pzErr = sqlite3_mprintf("invalid f32 vector: %d %% 4 != 0", vectorBytes);
    goto error;
  }
  len = vectorBytes / sizeof(float);
  if (len > MAX_VECTOR_SZ) {
    *pzErr = sqlite3_mprintf("invalid f32 vector: too large: %d", len);
    goto error;
  }
  for(i = 0; i < len; i++){
    elems[i] = deserializeF32(blob);
    blob += sizeof(float);
  }
  v->dims = len;
  return len;
error:
  return -1;
}

#endif /* !defined(SQLITE_OMIT_VECTOR) */

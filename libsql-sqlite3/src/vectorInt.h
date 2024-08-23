#ifndef _VECTOR_H
#define _VECTOR_H

#include "sqlite3.h"
#include "sqliteInt.h" // for u16/u32 types

#ifdef __cplusplus
extern "C" {
#endif

/* Objects */
typedef struct Vector Vector;
typedef u16 VectorType;
typedef u32 VectorDims;

/* 
 * Maximum dimensions for single vector in the DB. Any attempt to work with vector of bigger size will results to an error
 * (this is possible as user can write blob manually and later try to deserialize it)
*/
#define MAX_VECTOR_SZ 65536

/*
 * on-disk binary format for vector of different types:
 * 1. float32
 *  [data[0] as f32] [data[1] as f32] ... [data[dims - 1] as f32] [1 as u8]?
 *  - last 'type'-byte is optional for float32 vectors
 *
 * 2. float64
 *  [data[0] as f64] [data[1] as f64] ... [data[dims - 1] as f64] [2 as u8]
 *  - last 'type'-byte is mandatory for float64 vectors
 *
 * 3. float1bit
 *  [data[0] as u8] [data[1] as u8] ... [data[(dims + 7) / 8] as u8] [_ as u8; padding]? [trailing_bits as u8] [3 as u8]
 *  - every data byte (except for the last) represents exactly 8 components of the vector
 *  - last data byte represents [1..8] components of the vector
 *  - optional padding byte ensures that "trailing_bits" byte will be written at the odd blob position (0-based)
 *  - "trailing_bits" byte specify amount of trailing *bits* in the blob without last 'type'-byte which must be omitted
 *    (so, vector dimensions are equal to 8 * (blob_size - 1) - trailing_bits)
 *  - last 'type'-byte is mandatory for float1bit vectors
 *
 * 4. float8
 *  [data[0] as u8] [data[1] as u8] ... [data[dims - 1] as u8] [_ as u8; alignment_padding]* [alpha as f32] [shift as f32] [padding as u8] [trailing_bytes as u8] [4 as u8]
 *  - every data byte represents single quantized vector component
 *  - "alignment_padding" has size from 0 to 3 bytes in order to pad content to multiple of 4 = sizeof(float)
 *  - "trailing_bytes" byte specify amount of bytes in the "alignment_padding"
 *  - last 'type'-byte is mandatory for float8 vectors
*/

/*
 * Enumerate of supported vector types (0 omitted intentionally as we can use zero as "undefined" value)
*/
#define VECTOR_TYPE_FLOAT32   1
#define VECTOR_TYPE_FLOAT64   2
#define VECTOR_TYPE_FLOAT1BIT 3
#define VECTOR_TYPE_FLOAT8    4
#define VECTOR_TYPE_FLOAT16   5
#define VECTOR_TYPE_FLOATB16  6

#define VECTOR_FLAGS_STATIC 1

#define ALIGN(n, size) (((n + size - 1) / size) * size)

/*
 * Object which represents a vector
 * data points to the memory which must be interpreted according to the vector type
*/
struct Vector {
  VectorType type;  /* Type of vector */
  u16 flags;        /* Vector flags */
  VectorDims dims;  /* Number of dimensions */
  void *data;       /* Vector data */
};

size_t vectorDataSize(VectorType, VectorDims);
Vector *vectorAlloc(VectorType, VectorDims);
void vectorFree(Vector *v);
int vectorParseWithType(sqlite3_value *, Vector *, char **);
void vectorInit(Vector *, VectorType, VectorDims, void *);

/*
 * Dumps vector on the console (used only for debugging)
*/
void vectorDump    (const Vector *v);
void vectorF8Dump  (const Vector *v);
void vectorF16Dump (const Vector *v);
void vectorFB16Dump(const Vector *v);
void vectorF32Dump (const Vector *v);
void vectorF64Dump (const Vector *v);
void vector1BitDump(const Vector *v);

void vectorF8GetParameters(const u8 *, int, float *, float *);
void vectorF8SetParameters(u8 *, int, float, float);

/* 
 * Converts vector to the text representation and write the result to the sqlite3_context
*/
void vectorMarshalToText   (sqlite3_context *, const Vector *);
void vectorF32MarshalToText(sqlite3_context *, const Vector *);
void vectorF64MarshalToText(sqlite3_context *, const Vector *);

/* 
 * Serializes vector to the blob in little-endian format according to the IEEE-754 standard
*/
void vectorSerializeToBlob    (const Vector *, unsigned char *, size_t);
void vectorF8SerializeToBlob  (const Vector *, unsigned char *, size_t);
void vectorF16SerializeToBlob (const Vector *, unsigned char *, size_t);
void vectorFB16SerializeToBlob(const Vector *, unsigned char *, size_t);
void vectorF32SerializeToBlob (const Vector *, unsigned char *, size_t);
void vectorF64SerializeToBlob (const Vector *, unsigned char *, size_t);
void vector1BitSerializeToBlob(const Vector *, unsigned char *, size_t);

/* 
 * Calculates cosine distance between two vectors (vector must have same type and same dimensions)
*/
float vectorDistanceCos    (const Vector *, const Vector *);
float vectorF8DistanceCos  (const Vector *, const Vector *);
float vectorF16DistanceCos (const Vector *, const Vector *);
float vectorFB16DistanceCos(const Vector *, const Vector *);
float vectorF32DistanceCos (const Vector *, const Vector *);
double vectorF64DistanceCos(const Vector *, const Vector *);

/*
 * Calculates hamming distance between two 1-bit vectors (vector must have same dimensions)
*/
int vector1BitDistanceHamming(const Vector *, const Vector *);

/*
 * Calculates L2 distance between two vectors (vector must have same type and same dimensions)
*/
float vectorDistanceL2    (const Vector *, const Vector *);
float vectorF8DistanceL2  (const Vector *, const Vector *);
float vectorF16DistanceL2 (const Vector *, const Vector *);
float vectorFB16DistanceL2(const Vector *, const Vector *);
float vectorF32DistanceL2 (const Vector *, const Vector *);
double vectorF64DistanceL2(const Vector *, const Vector *);

/* 
 * Serializes vector to the sqlite_blob in little-endian format according to the IEEE-754 standard
 * LibSQL can append one trailing byte in the end of final blob. This byte will be later used to determine type of the blob
 * By default, blob with even length will be treated as a f32 blob
*/
void vectorSerializeWithMeta(sqlite3_context *, const Vector *);

/*
 * Parses Vector content from the blob; vector type and dimensions must be filled already
*/
int vectorParseSqliteBlobWithType(sqlite3_value *, Vector *, char **);

void vectorF8DeserializeFromBlob  (Vector *, const unsigned char *, size_t);
void vectorF16DeserializeFromBlob (Vector *, const unsigned char *, size_t);
void vectorFB16DeserializeFromBlob(Vector *, const unsigned char *, size_t);
void vectorF32DeserializeFromBlob (Vector *, const unsigned char *, size_t);
void vectorF64DeserializeFromBlob (Vector *, const unsigned char *, size_t);
void vector1BitDeserializeFromBlob(Vector *, const unsigned char *, size_t);

void vectorInitStatic(Vector *, VectorType, VectorDims, void *);
void vectorInitFromBlob(Vector *, const unsigned char *, size_t);

u16 vectorF16FromFloat(float);
float vectorF16ToFloat(u16);

u16 vectorFB16FromFloat(float);
float vectorFB16ToFloat(u16);

void vectorConvert(const Vector *, Vector *);

/* Detect type and dimension of vector provided with first parameter of sqlite3_value * type */
int detectVectorParameters(sqlite3_value *, int, int *, int *, char **);

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

#ifdef __cplusplus
}  /* end of the 'extern "C"' block */
#endif

#endif /* _VECTOR_H */

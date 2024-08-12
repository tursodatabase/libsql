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
 * Enumerate of supported vector types (0 omitted intentionally as we can use zero as "undefined" value)
*/
#define VECTOR_TYPE_FLOAT32 1
#define VECTOR_TYPE_FLOAT64 2
#define VECTOR_TYPE_1BIT    3

#define VECTOR_FLAGS_STATIC 1

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
void vectorDump   (const Vector *v);
void vectorF32Dump (const Vector *v);
void vectorF64Dump (const Vector *v);
void vector1BitDump(const Vector *v);

/* 
 * Converts vector to the text representation and write the result to the sqlite3_context
*/
void vectorMarshalToText   (sqlite3_context *, const Vector *);
void vectorF32MarshalToText(sqlite3_context *, const Vector *);
void vectorF64MarshalToText(sqlite3_context *, const Vector *);

/* 
 * Serializes vector to the blob in little-endian format according to the IEEE-754 standard
*/
size_t vectorSerializeToBlob    (const Vector *, unsigned char *, size_t);
size_t vectorF32SerializeToBlob (const Vector *, unsigned char *, size_t);
size_t vectorF64SerializeToBlob (const Vector *, unsigned char *, size_t);
size_t vector1BitSerializeToBlob(const Vector *, unsigned char *, size_t);

/* 
 * Calculates cosine distance between two vectors (vector must have same type and same dimensions)
*/
float vectorDistanceCos    (const Vector *, const Vector *);
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

void vectorF32DeserializeFromBlob (Vector *, const unsigned char *, size_t);
void vectorF64DeserializeFromBlob (Vector *, const unsigned char *, size_t);
void vector1BitDeserializeFromBlob(Vector *, const unsigned char *, size_t);

void vectorInitStatic(Vector *, VectorType, VectorDims, void *);
void vectorInitFromBlob(Vector *, const unsigned char *, size_t);
void vectorF32InitFromBlob(Vector *, const unsigned char *, size_t);
void vectorF64InitFromBlob(Vector *, const unsigned char *, size_t);

void vectorConvert(const Vector *, Vector *);

/* Detect type and dimension of vector provided with first parameter of sqlite3_value * type */
int detectVectorParameters(sqlite3_value *, int, int *, int *, char **);

#ifdef __cplusplus
}  /* end of the 'extern "C"' block */
#endif

#endif /* _VECTOR_H */

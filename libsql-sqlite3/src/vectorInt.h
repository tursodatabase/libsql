#ifndef _VECTOR_H
#define _VECTOR_H

#include "sqlite3.h"

#ifdef __cplusplus
extern "C" {
#endif

/* Objects */
typedef struct Vector Vector;
typedef struct DiskAnnIndex DiskAnnIndex;

#define MAX_VECTOR_SZ 65536

typedef u16 VectorType;
typedef u32 VectorDims;

#define VECTOR_TYPE_FLOAT32 0
#define VECTOR_TYPE_FLOAT64 1

#define VECTOR_FLAGS_STATIC 1

#define VECTOR_DISTANCE_COS 0

/* An instance of this object represents a vector.
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
int vectorParse(sqlite3_value *, Vector *, char **);
size_t vectorSerializeToBlob(Vector *, unsigned char *, size_t);
size_t vectorDeserializeFromBlob(Vector *, const unsigned char *, size_t);
void vectorDump(Vector *v);
float vectorDistanceCos(Vector *, Vector *);

void vectorF32Dump(Vector *v);
void vectorF32Deserialize(sqlite3_context *,Vector *v);
void vectorF32Serialize(sqlite3_context *,Vector *v);
void vectorF32InitFromBlob(Vector *, const unsigned char *, size_t);
int vectorF32ParseBlob(sqlite3_value *, Vector *, char **);
size_t vectorF32SerializeToBlob(Vector *, unsigned char *, size_t);
size_t vectorF32DeserializeFromBlob(Vector *, const unsigned char *, size_t);
float vectorF32DistanceCos(Vector *, Vector *);

void vectorF64Dump(Vector *v);
void vectorF64Deserialize(sqlite3_context *,Vector *v);
void vectorF64Serialize(sqlite3_context *,Vector *v);
void vectorF64InitFromBlob(Vector *, const unsigned char *, size_t);
int vectorF64ParseBlob(sqlite3_value *, Vector *, char **);
size_t vectorF64SerializeToBlob(Vector *, unsigned char *, size_t);
size_t vectorF64DeserializeFromBlob(Vector *, const unsigned char *, size_t);
double vectorF64DistanceCos(Vector *, Vector *);

int diskAnnCreateIndex(sqlite3 *, const char *, unsigned int, unsigned int);
int diskAnnOpenIndex(sqlite3 *, const char *, DiskAnnIndex **);
void diskAnnCloseIndex(DiskAnnIndex *pIndex);
int diskAnnInsert(DiskAnnIndex *, Vector *v, i64);
int diskAnnDelete(DiskAnnIndex *, i64);
int diskAnnSearch(DiskAnnIndex *, Vector*, unsigned int, i64*);

#ifdef __cplusplus
}  /* end of the 'extern "C"' block */
#endif

#endif /* _VECTOR_H */

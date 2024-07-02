#ifndef _VECTOR_H
#define _VECTOR_H

#include "sqlite3.h"

#ifdef __cplusplus
extern "C" {
#endif

/* Objects */
typedef struct Vector Vector;
typedef struct VectorIndexParameters VectorIndexParameters;
typedef struct DiskAnnIndex DiskAnnIndex;

typedef u8 IndexType;
typedef u8 MetricType;
typedef u16 VectorType;
typedef u32 VectorDims;

#define MAX_VECTOR_SZ 65536
#define VECTOR_INDEX_GLOBAL_META_TABLE "libsql_vector_meta_shadow"
#define VECTOR_INDEX_MARKER_FUNCTION   "libsql_vector_idx"

#define VECTOR_INDEX_FORMAT_VERSION       1
#define VECTOR_INDEX_PARAMETER_BIN_LENGTH 9
#define VECTOR_INDEX_PARAMETERS_COUNT     6

// all enum-like type constants starts with 1 to make 0 an "unset" value placeholder
#define VECTOR_DISKANN_INDEX 1

#define VECTOR_TYPE_FLOAT32 1
#define VECTOR_TYPE_FLOAT64 2

#define VECTOR_DISTANCE_COS 1

#define VECTOR_FLAGS_STATIC 1

struct VectorIndexParameters {
  u8 formatVersion;
  IndexType indexType;
  VectorType vectorType;
  VectorDims vectorDimension; /* dimension of the vector column; note that ALTERing of column type is forbidden for vector columns */
  MetricType metricType;
  u32 blockSize;
};


struct VectorColumnType {
  const char *zName;
  int nBits;
};
static struct VectorColumnType VECTOR_COLUMN_TYPES[] = { { "FLOAT32", 32 }, { "FLOAT64", 64 }, { "F32_BLOB", 32 }, { "F64_BLOB", 64 } };

int typeParameterConversion(const char *zValue, VectorIndexParameters *parameters);
int metricParameterConversion(const char *zValue, VectorIndexParameters *parameters);
struct VectorIndexParameterDef {
  const char *zName;
  int (*conversion)(const char *zValue, VectorIndexParameters *parameters);
};
static struct VectorIndexParameterDef VECTOR_INDEX_PARAMETERS[] = { { "type", typeParameterConversion }, { "metric", metricParameterConversion } };

/* An instance of this object represents a vector.
*/
struct Vector {
  VectorType type;  /* Type of vector */
  u16 flags;        /* Vector flags */
  VectorDims dims;  /* Number of dimensions */
  void *data;       /* Vector data */
};


VectorIndexParameters createDefaultVectorIndexParameters();
void serializeVectorIndexParameters(VectorIndexParameters *pParameters, u8 *buffer, int bufferSize);
int deserializeVectorIndexParameters(VectorIndexParameters *pParameters, u8 *buffer, int bufferSize);
int vectorIndexCreate(Parse*, Index*, IdList*);
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

int diskAnnInitIndex(sqlite3 *, const char *, VectorIndexParameters*);
int diskAnnOpenIndex(sqlite3 *, const char *, DiskAnnIndex **);
void diskAnnCloseIndex(DiskAnnIndex *pIndex);
int diskAnnInsert(DiskAnnIndex *, Vector *v, i64);
int diskAnnDelete(DiskAnnIndex *, i64);
int diskAnnSearch(DiskAnnIndex *, Vector*, unsigned int, i64*);

#ifdef __cplusplus
}  /* end of the 'extern "C"' block */
#endif

#endif /* _VECTOR_H */

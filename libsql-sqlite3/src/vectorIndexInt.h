#ifndef _VECTOR_INDEX_H
#define _VECTOR_INDEX_H

#include "sqlite3.h"
#include "vectorInt.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct DiskAnnIndex DiskAnnIndex;
typedef struct BlobSpot BlobSpot;

/*
 * Main type which holds all necessary index information and will be passed as a first argument in all index-related operations
*/
struct DiskAnnIndex {
  sqlite3 *db;         /* Database connection */
  char *zDbSName;      /* Database name */
  char *zName;         /* Index name */
  char *zShadow;       /* Shadow table name */
  int nFormatVersion;  /* DiskAnn format version */
  int nDistanceFunc;   /* Distance function */
  int nBlockSize;      /* Size of the block which stores all data for single node */
  int nVectorDims;     /* Vector dimensions */
  int nNodeVectorType; /* Vector type of each node */
  int nEdgeVectorType; /* Vector type of each edge */
  int nNodeVectorSize; /* Vector size of each node in bytes */
  int nEdgeVectorSize; /* Vector size of each edge in bytes */
  float pruningAlpha;  /* Alpha parameter for edge pruning during INSERT operation */
  int insertL;         /* Max size of candidate set (L) visited during INSERT operation */
  int searchL;         /* Max size of candidate set (L) visited during SEARCH operation (can be overriden from query in future) */

  int nReads;
  int nWrites;
};

/*
 * Simple utility class which holds sqlite3_blob handle poiting to the nRowid (undefined if pBlob == NULL)
 * Caller can re-load BlobSpot with blobSpotReload(...) method which will reopen blob at new row position
 * sqlite3_blob_reopen API can be visibly faster than close/open pair since a lot of check can be omitted
*/
struct BlobSpot {
  u64 nRowid;           /* last rowid for which open/reopen was called; undefined if BlobSpot was never opened */
  sqlite3_blob *pBlob;  /* BLOB handle */
  u8 *pBuffer;          /* buffer for BLOB data */
  int nBufferSize;      /* buffer size */
  u8 isWritable;        /* blob open mode (readonly or read/write) */
  u8 isInitialized;     /* was blob read after creation or not */
  u8 isAborted;         /* set to true if last operation with blob failed with non-zero code */
};

/* Special error code for blobSpotCreate/blobSpotReload functions which will fire where rowid doesn't exists in the table */
#define DISKANN_ROW_NOT_FOUND 1001

#define DISKANN_BLOB_WRITABLE 1
#define DISKANN_BLOB_READONLY 0

/* BlobSpot operations */
int blobSpotCreate(const DiskAnnIndex *pIndex, BlobSpot **ppBlobSpot, u64 nRowid, int nBufferSize, int isWritable);
int blobSpotReload(DiskAnnIndex *pIndex, BlobSpot *pBlobSpot, u64 nRowid, int nBufferSize);
int blobSpotFlush(DiskAnnIndex *pIndex, BlobSpot *pBlobSpot);
void blobSpotFree(BlobSpot *pBlobSpot);

/*
 * Accessor for node binary format
 * - default format is the following:
 *   [u64 nRowid] [u16 nEdges] [6 byte padding] [node vector] [edge vector] * nEdges [trash vector] * (nMaxEdges - nEdges) ([u32 unused] [f32 distance] [u64 edgeId]) * nEdges
 *   Note, that 6 byte padding after nEdges required to align [node vector] by word boundary and avoid unaligned reads
 *   Note, that node vector and edge vector can have different representations (and edge vector can be smaller in size than node vector)
*/
int nodeEdgesMaxCount(const DiskAnnIndex *pIndex);
int nodeEdgesMetadataOffset(const DiskAnnIndex *pIndex);
void nodeBinInit(const DiskAnnIndex *pIndex, BlobSpot *pBlobSpot, u64 nRowid, Vector *pVector);
void nodeBinVector(const DiskAnnIndex *pIndex, const BlobSpot *pBlobSpot, Vector *pVector);
u16 nodeBinEdges(const DiskAnnIndex *pIndex, const BlobSpot *pBlobSpot);
void nodeBinEdge(const DiskAnnIndex *pIndex, const BlobSpot *pBlobSpot, int iEdge, u64 *pRowid, float *distance, Vector *pVector);
int nodeBinEdgeFindIdx(const DiskAnnIndex *pIndex, const BlobSpot *pBlobSpot, u64 nRowid);
void nodeBinPruneEdges(const DiskAnnIndex *pIndex, BlobSpot *pBlobSpot, int nPruned);
void nodeBinReplaceEdge(const DiskAnnIndex *pIndex, BlobSpot *pBlobSpot, int iReplace, u64 nRowid, float distance, Vector *pVector);
void nodeBinDeleteEdge(const DiskAnnIndex *pIndex, BlobSpot *pBlobSpot, int iDelete);
void nodeBinDebug(const DiskAnnIndex *pIndex, const BlobSpot *pBlobSpot);

/**************************************************************************
** Vector index utilities
****************************************************************************/

/* Vector index utility objects */
typedef struct VectorIdxKey VectorIdxKey;
typedef struct VectorIdxParams VectorIdxParams;
typedef struct VectorInRow VectorInRow;
typedef struct VectorOutRows VectorOutRows;

typedef u8 IndexType;
typedef u8 MetricType;

/*
 * All vector index parameters must be known to the vectorIndex module although it's interpretation are up to the specific implementation of the index
 * (so, there is no validation of parameter values in the vectorIndex module - all this work must be delegated to the specific implementation)
 * All enum-like type constants starts with 1 to make 0 an "unset" value placeholder
*/

/* format version which can help to upgrade vector on-disk format without breaking older version of the db */
#define VECTOR_FORMAT_PARAM_ID              1
/*
 * 1 - v1 version; node block format: [node meta] [node vector] [edge vectors] ... [ [u64 unused               ] [u64 edge rowid] ] ...
 * 2 - v2 version; node block format: [node meta] [node vector] [edge vectors] ... [ [u32 unused] [f32 distance] [u64 edge rowid] ] ...
 * 3 - v3 version; node meta aligned to 8-byte boundary (instead of having u64 + u16 size - we round it up to u64 + u64)
*/
#define VECTOR_FORMAT_V1                    1
#define VECTOR_FORMAT_V2                    2
#define VECTOR_FORMAT_DEFAULT               3

/* type of the vector index */
#define VECTOR_INDEX_TYPE_PARAM_ID          2
#define VECTOR_INDEX_TYPE_DISKANN           1

/* type of the underlying vector for the vector index */
#define VECTOR_TYPE_PARAM_ID                3
/* dimension of the underlying vector for the vector index */
#define VECTOR_DIM_PARAM_ID                 4

/* metric type used for comparing two vectors */
#define VECTOR_METRIC_TYPE_PARAM_ID         5
#define VECTOR_METRIC_TYPE_COS              1
#define VECTOR_METRIC_TYPE_L2               2

/* block size */
#define VECTOR_BLOCK_SIZE_PARAM_ID          6
#define VECTOR_BLOCK_SIZE_DEFAULT           128

#define VECTOR_PRUNING_ALPHA_PARAM_ID       7
#define VECTOR_PRUNING_ALPHA_DEFAULT        1.2

#define VECTOR_INSERT_L_PARAM_ID            8
#define VECTOR_INSERT_L_DEFAULT             70

#define VECTOR_SEARCH_L_PARAM_ID            9
#define VECTOR_SEARCH_L_DEFAULT             200

#define VECTOR_MAX_NEIGHBORS_PARAM_ID       10

#define VECTOR_COMPRESS_NEIGHBORS_PARAM_ID  11

/* total amount of vector index parameters */
#define VECTOR_PARAM_IDS_COUNT              11

/*
 * Vector index parameters are stored in simple binary format (1 byte tag + 8 byte u64 integer / f64 float)
 * This will allow us to add parameters in future version more easily as we have full control over the format (compared to the "rigid" SQL schema)
 * For now, VectorIdxParams allocated on stack and have 128 bytes hard limit (so far we have 9 parameters and 72 are enough for us)
*/
#define VECTOR_INDEX_PARAMS_BUF_SIZE 128
struct VectorIdxParams {
  u8 pBinBuf[VECTOR_INDEX_PARAMS_BUF_SIZE];
  int nBinSize;
};


/*
 * Structure which holds information about primary key of the base table for vector index
 * For tables with ROWID only this structure will have information about single column with INTEGER affinity and BINARY collation
 * For now, VectorIdxKey allocated on stack have 16 columns hard limit (for now we are not supporting composite primary keys due to the limitation of virtual tables)
*/
#define VECTOR_INDEX_MAX_KEY_COLUMNS 16
struct VectorIdxKey {
  int nKeyColumns;
  char aKeyAffinity[VECTOR_INDEX_MAX_KEY_COLUMNS];
  /* collation is owned by the caller and structure is not responsible for reclamation of collation string resources */
  const char *azKeyCollation[VECTOR_INDEX_MAX_KEY_COLUMNS];
};

/*
 * Structure which holds information about input payload for vector index (for INSERT/DELETE operations)
 * pVector must be NULL for DELETE operation
 *
 * Resources must be reclaimed with vectorInRowFree(...) method
*/
struct VectorInRow {
  Vector *pVector;
  int nKeys;
  sqlite3_value *pKeyValues;
};

/*
 * Structure which holds information about result set of SEARCH operation
 * It have special optimization for cases when single INTEGER primary key is used - in this case aIntValues array stores all values instead of ppValues
 * In other case generic ppValues stores all column information
 *
 * Resources must be reclaimed with vectorOutRowsFree(...) method
*/
#define VECTOR_OUT_ROWS_MAX_CELLS (1<<30)
struct VectorOutRows {
  int nRows;
  int nCols;
  i64 *aIntValues;
  sqlite3_value **ppValues;
};

// limit to the sql part which we render in order to perform operations with shadow tables
// we render this parts of SQL on stack - thats why we have hard limit on this
// stack simplify memory managment code and also doesn't impose very strict limits here since 128 bytes for column names should be enough for almost all use cases
#define VECTOR_INDEX_SQL_RENDER_LIMIT 128

void vectorIdxParamsInit(VectorIdxParams *, u8 *, int);
u64 vectorIdxParamsGetU64(const VectorIdxParams *, char);
double vectorIdxParamsGetF64(const VectorIdxParams *, char);
int vectorIdxParamsPutU64(VectorIdxParams *, char, u64);
int vectorIdxParamsPutF64(VectorIdxParams *, char, double);

int vectorIdxKeyGet(const Index *, VectorIdxKey *, const char **);
int vectorIdxKeyRowidLike(const VectorIdxKey *);
int vectorIdxKeyDefsRender(const VectorIdxKey *, const char *, char *, int);
int vectorIdxKeyNamesRender(int, const char *, char *, int);

int vectorInRowAlloc(sqlite3 *, const UnpackedRecord *, VectorInRow *, char **);
sqlite3_value* vectorInRowKey(const VectorInRow *, int);
int vectorInRowTryGetRowid(const VectorInRow *, u64 *);
i64 vectorInRowLegacyId(const VectorInRow *);
int vectorInRowPlaceholderRender(const VectorInRow *, char *, int);
void vectorInRowFree(sqlite3 *, VectorInRow *);

int vectorOutRowsAlloc(sqlite3 *, VectorOutRows *, int, int, int);
int vectorOutRowsPut(VectorOutRows *, int, int, const u64 *, sqlite3_value *);
void vectorOutRowsGet(sqlite3_context *, const VectorOutRows *, int, int);
void vectorOutRowsFree(sqlite3 *, VectorOutRows *);

int diskAnnCreateIndex(sqlite3 *, const char *, const char *, const VectorIdxKey *, VectorIdxParams *, const char **);
int diskAnnClearIndex(sqlite3 *, const char *, const char *);
int diskAnnDropIndex(sqlite3 *, const char *, const char *);
int diskAnnOpenIndex(sqlite3 *, const char *, const char *, const VectorIdxParams *, DiskAnnIndex **);
void diskAnnCloseIndex(DiskAnnIndex *);
int diskAnnInsert(DiskAnnIndex *, const VectorInRow *, char **);
int diskAnnDelete(DiskAnnIndex *, const VectorInRow *, char **);
int diskAnnSearch(DiskAnnIndex *, const Vector *, int, const VectorIdxKey *, VectorOutRows *, char **);

typedef struct VectorIdxCursor VectorIdxCursor;

#define VECTOR_INDEX_VTAB_NAME         "vector_top_k"
#define VECTOR_INDEX_GLOBAL_META_TABLE "libsql_vector_meta_shadow"
#define VECTOR_INDEX_MARKER_FUNCTION   "libsql_vector_idx"

int vectorIdxParseColumnType(const char *, int *, int *, const char **);

int vectorIndexCreate(Parse*, const Index*, const char *, const IdList*);
int vectorIndexClear(sqlite3 *, const char *, const char *);
int vectorIndexDrop(sqlite3 *, const char *, const char *);
int vectorIndexSearch(sqlite3 *, int, sqlite3_value **, VectorOutRows *, int *, int *, char **);
int vectorIndexCursorInit(sqlite3 *, const char *, const char *, VectorIdxCursor **);
void vectorIndexCursorClose(sqlite3 *, VectorIdxCursor *, int *, int *);
int vectorIndexInsert(VectorIdxCursor *, const UnpackedRecord *, char **);
int vectorIndexDelete(VectorIdxCursor *, const UnpackedRecord *, char **);

#ifdef __cplusplus
}  /* end of the 'extern "C"' block */
#endif

#endif /* _VECTOR_INDEX_H */


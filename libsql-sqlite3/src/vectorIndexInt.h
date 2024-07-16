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
  char *zDb;           /* Database name */
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
};

/* Special error code for blobSpotCreate/blobSpotReload functions which will fire where rowid doesn't exists in the table */
#define DISKANN_ROW_NOT_FOUND 1001

#define DISKANN_BLOB_WRITABLE 1
#define DISKANN_BLOB_READONLY 0

/* BlobSpot operations */
int blobSpotCreate(const DiskAnnIndex *pIndex, BlobSpot **ppBlobSpot, u64 nRowid, int nBufferSize, int isWritable);
int blobSpotReload(const DiskAnnIndex *pIndex, BlobSpot *pBlobSpot, u64 nRowid, int nBufferSize);
int blobSpotFlush(BlobSpot *pBlobSpot);
void blobSpotFree(BlobSpot *pBlobSpot);

/*
 * Accessor for node binary format
 * - v1 format is the following:
 *   [u64 nRowid] [u16 nEdges] [node vector] [edge vector] * nEdges [trash vector] * (nMaxEdges - nEdges) ([u64 legacyField] [u64 edgeId]) * nEdges
 *   Note, that node vector and edge vector can have different representations (and edge vector can be smaller in size than node vector)
*/
int nodeEdgesMaxCount(const DiskAnnIndex *pIndex);
int nodeEdgesMetadataOffset(const DiskAnnIndex *pIndex);
void nodeBinInit(const DiskAnnIndex *pIndex, BlobSpot *pBlobSpot, u64 nRowid, Vector *pVector);
void nodeBinVector(const DiskAnnIndex *pIndex, const BlobSpot *pBlobSpot, Vector *pVector);
u16 nodeBinEdges(const DiskAnnIndex *pIndex, const BlobSpot *pBlobSpot);
void nodeBinEdge(const DiskAnnIndex *pIndex, const BlobSpot *pBlobSpot, int iEdge, u64 *pRowid, Vector *pVector);
int nodeBinEdgeFindIdx(const DiskAnnIndex *pIndex, const BlobSpot *pBlobSpot, u64 nRowid);
void nodeBinPruneEdges(const DiskAnnIndex *pIndex, BlobSpot *pBlobSpot, int nPruned);
void nodeBinInsertEdge(const DiskAnnIndex *pIndex, BlobSpot *pBlobSpot, int iInsert, u64 nRowid, Vector *pVector);
void nodeBinDeleteEdge(const DiskAnnIndex *pIndex, BlobSpot *pBlobSpot, int iDelete);
void nodeBinDebug(const DiskAnnIndex *pIndex, const BlobSpot *pBlobSpot);

#ifdef __cplusplus
}  /* end of the 'extern "C"' block */
#endif

#endif /* _VECTOR_INDEX_H */


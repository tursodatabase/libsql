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
** DiskANN for SQLite/libSQL.
**
** The algorithm is described in the following publications:
**
**   Suhas Jayaram Subramanya et al (2019). DiskANN: Fast Accurate Billion-point
**   Nearest Neighbor Search on a Single Node. In NeurIPS 2019.
**
**   Aditi Singh et al (2021). FreshDiskANN: A Fast and Accurate Graph-Based ANN
**   Index for Streaming Similarity Search. ArXiv.
**
**   Yu Pan et al (2023). LM-DiskANN: Low Memory Footprint in Disk-Native
**   Dynamic Graph-Based ANN Indexing. In IEEE BIGDATA 2023.
**
** Here is the (internal, non-API) interface between this module and the
** rest of the SQLite system:
**
**    diskAnnCreateIndex()     Create new index and fill default values for diskann parameters (if some of them are omitted)
**    diskAnnDropIndex()       Delete existing index
**    diskAnnClearIndex()      Truncate existing index
**    diskAnnOpenIndex()       Open index for operations (allocate all necessary internal structures)
**    diskAnnCloseIndex()      Close index and free associated resources
**    diskAnnSearch()          Search K nearest neighbours to the query vector in an opened index
**    diskAnnInsert()          Insert single new(!) vector in an opened index
**    diskAnnDelete()          Delete row by key from an opened index
*/
#ifndef SQLITE_OMIT_VECTOR

#include "math.h"
#include "sqliteInt.h"
#include "vectorIndexInt.h"

// #define SQLITE_VECTOR_TRACE
#if defined(SQLITE_DEBUG) && defined(SQLITE_VECTOR_TRACE)
#define DiskAnnTrace(X) sqlite3DebugPrintf X;
#else
#define DiskAnnTrace(X)
#endif

// limit to the sql part which we render in order to perform operations with shadow table
// we render this parts of SQL on stack - thats why we have hard limit on this
// stack simplify memory managment code and also doesn't impose very strict limits here since 128 bytes for column names should be enough for almost all use cases
#define DISKANN_SQL_RENDER_LIMIT 128

// limit to the maximum size of DiskANN block (128 MB)
// even with 1MB we can store tens of thousands of nodes in several GBs - which is already too much
// but we are "generous" here and allow user to store up to 128MB blobs
#define DISKANN_MAX_BLOCK_SZ 134217728

/*
 * Due to historical reasons parameter for index block size were stored as u16 value and divided by 512 (2^9)
 * So, we will make inverse transform before initializing index from stored parameters
*/
#define DISKANN_BLOCK_SIZE_SHIFT 9

#define VECTOR_NODE_METADATA_SIZE (sizeof(u64) + sizeof(u16))
#define VECTOR_EDGE_METADATA_SIZE (sizeof(u64) + sizeof(u64))

typedef struct VectorPair VectorPair;
typedef struct DiskAnnSearchCtx DiskAnnSearchCtx;
typedef struct DiskAnnNode DiskAnnNode;

// VectorPair represents single vector where pNode is an exact representation and pEdge - compressed representation 
// (pEdge pointer always equals to pNode if pNodeType == pEdgeType)
struct VectorPair {
  int nodeType;
  int edgeType;
  Vector *pNode;
  Vector *pEdge;
};

// DiskAnnNode represents single node in the DiskAnn graph
struct DiskAnnNode {
  u64 nRowid;           /* node id */
  int visited;          /* is this node visited? */
  DiskAnnNode *pNext;   /* next node in the visited list */
  BlobSpot *pBlobSpot;  /* reference to the blob with node data (can be NULL when data actually is not needed; for example - node waiting in the queue) */
};

/*
 * DiskAnnSearchCtx stores information required for search operation to succeed
 *
 * search context usually "borrows" candidates (storing them in aCandidates or visitedList)
 * so caller which puts nodes in the context can forget about resource managmenet (context will take care of this)
*/
struct DiskAnnSearchCtx {
  VectorPair query;             /* initial query vector; user query for SELECT and row vector for INSERT */
  DiskAnnNode **aCandidates;    /* array of unvisited candidates ordered by distance (possibly approximate) to the query (ascending) */
  float *aDistances;            /* array of distances (possible approximate) to the query vector */
  unsigned int nCandidates;     /* current size of aCandidates/aDistances arrays */
  unsigned int maxCandidates;   /* max size of aCandidates/aDistances arrays */
  DiskAnnNode **aTopCandidates; /* top candidates with exact distance calculated */
  float *aTopDistances;         /* top candidates exact distances */
  int nTopCandidates;           /* current size of aTopCandidates/aTopDistances arrays */
  int maxTopCandidates;         /* max size of aTopCandidates/aTopDistances arrays */
  DiskAnnNode *visitedList;     /* list of all visited candidates (so, candidates from aCandidates array either got replaced or moved to the visited list) */
  unsigned int nUnvisited;      /* amount of unvisited candidates in the aCadidates array */
  int blobMode;                 /* DISKANN_BLOB_READONLY if we wont modify node blobs; DISKANN_BLOB_WRITABLE - otherwise */
};

/**************************************************************************
** Serialization utilities
**************************************************************************/

static inline u16 readLE16(const unsigned char *p){
  return (u16)p[0] | (u16)p[1] << 8;
}

static inline u32 readLE32(const unsigned char *p){
  return (u32)p[0] | (u32)p[1] << 8 | (u32)p[2] << 16 | (u32)p[3] << 24;
}

static inline u64 readLE64(const unsigned char *p){
  return (u64)p[0]
       | (u64)p[1] << 8
       | (u64)p[2] << 16
       | (u64)p[3] << 24
       | (u64)p[4] << 32
       | (u64)p[5] << 40
       | (u64)p[6] << 48
       | (u64)p[7] << 56;
}

static inline void writeLE16(unsigned char *p, u16 v){
  p[0] = v;
  p[1] = v >> 8;
}

static inline void writeLE32(unsigned char *p, u32 v){
  p[0] = v;
  p[1] = v >> 8;
  p[2] = v >> 16;
  p[3] = v >> 24;
}

static inline void writeLE64(unsigned char *p, u64 v){
  p[0] = v;
  p[1] = v >> 8;
  p[2] = v >> 16;
  p[3] = v >> 24;
  p[4] = v >> 32;
  p[5] = v >> 40;
  p[6] = v >> 48;
  p[7] = v >> 56;
}

/**************************************************************************
** BlobSpot utilities
**************************************************************************/

// sqlite3_blob_* API return SQLITE_ERROR in any case but we need to distinguish between "row not found" and other errors in some cases
static int blobSpotConvertRc(const DiskAnnIndex *pIndex, int rc){
  if( rc == SQLITE_ERROR && strncmp(sqlite3_errmsg(pIndex->db), "no such rowid", 13) == 0 ){
    return DISKANN_ROW_NOT_FOUND;
  }
  return rc;
}

int blobSpotCreate(const DiskAnnIndex *pIndex, BlobSpot **ppBlobSpot, u64 nRowid, int nBufferSize, int isWritable) {
  int rc = SQLITE_OK;
  BlobSpot *pBlobSpot;
  u8 *pBuffer;

  DiskAnnTrace(("blob spot created: rowid=%lld, isWritable=%d\n", nRowid, isWritable));
  assert( nBufferSize > 0 );

  pBlobSpot = sqlite3_malloc(sizeof(BlobSpot));
  if( pBlobSpot == NULL ){
    rc = SQLITE_NOMEM_BKPT;
    goto out;
  }

  pBuffer = sqlite3_malloc(nBufferSize);
  if( pBuffer == NULL ){
    rc = SQLITE_NOMEM_BKPT;
    goto out;
  }

  // open blob in the end so we don't need to close it in error case
  rc = sqlite3_blob_open(pIndex->db, pIndex->zDbSName, pIndex->zShadow, "data", nRowid, isWritable, &pBlobSpot->pBlob);
  rc = blobSpotConvertRc(pIndex, rc);
  if( rc != SQLITE_OK ){
    goto out;
  }
  pBlobSpot->nRowid = nRowid;
  pBlobSpot->pBuffer = pBuffer;
  pBlobSpot->nBufferSize = nBufferSize;
  pBlobSpot->isWritable = isWritable;
  pBlobSpot->isInitialized = 0;
  pBlobSpot->isAborted = 0;

  *ppBlobSpot = pBlobSpot;
  return SQLITE_OK;

out:
  if( pBlobSpot != NULL ){
    sqlite3_free(pBlobSpot);
  }
  if( pBuffer != NULL ){
    sqlite3_free(pBuffer);
  }
  return rc;
}

int blobSpotReload(DiskAnnIndex *pIndex, BlobSpot *pBlobSpot, u64 nRowid, int nBufferSize) {
  int rc;

  DiskAnnTrace(("blob spot reload: rowid=%lld\n", nRowid));
  assert( pBlobSpot != NULL && (pBlobSpot->pBlob != NULL || pBlobSpot->isAborted ) );
  assert( pBlobSpot->nBufferSize == nBufferSize );

  if( pBlobSpot->nRowid == nRowid && pBlobSpot->isInitialized ){
    return SQLITE_OK;
  }

  // if last blob open/reopen operation aborted - we need to close current blob and open new one
  // (as all operations over aborted blob will return SQLITE_ABORT error)
  if( pBlobSpot->isAborted ){
    if( pBlobSpot->pBlob != NULL ){
      sqlite3_blob_close(pBlobSpot->pBlob);
    }
    pBlobSpot->pBlob = NULL;
    pBlobSpot->isInitialized = 0;
    pBlobSpot->isAborted = 0;
    pBlobSpot->nRowid = nRowid;

    rc = sqlite3_blob_open(pIndex->db, pIndex->zDbSName, pIndex->zShadow, "data", nRowid, pBlobSpot->isWritable, &pBlobSpot->pBlob);
    rc = blobSpotConvertRc(pIndex, rc);
    if( rc != SQLITE_OK ){
      goto abort;
    }
  }

  if( pBlobSpot->nRowid != nRowid ){
    rc = sqlite3_blob_reopen(pBlobSpot->pBlob, nRowid);
    rc = blobSpotConvertRc(pIndex, rc);
    if( rc != SQLITE_OK ){
      goto abort;
    }
    pBlobSpot->nRowid = nRowid;
    pBlobSpot->isInitialized = 0;
  }
  rc = sqlite3_blob_read(pBlobSpot->pBlob, pBlobSpot->pBuffer, nBufferSize, 0);
  if( rc != SQLITE_OK ){
    goto abort;
  }
  pIndex->nReads++;
  pBlobSpot->isInitialized = 1;
  return SQLITE_OK;

abort:
  pBlobSpot->isAborted = 1;
  pBlobSpot->isInitialized = 0;
  return rc;
}

int blobSpotFlush(DiskAnnIndex* pIndex, BlobSpot *pBlobSpot) {
  int rc = sqlite3_blob_write(pBlobSpot->pBlob, pBlobSpot->pBuffer, pBlobSpot->nBufferSize, 0);
  if( rc != SQLITE_OK ){
    return rc;
  }
  pIndex->nWrites++;
  return rc;
}

void blobSpotFree(BlobSpot *pBlobSpot) {
  if( pBlobSpot->pBlob != NULL ){
    sqlite3_blob_close(pBlobSpot->pBlob);
  }
  if( pBlobSpot->pBuffer != NULL ){
    sqlite3_free(pBlobSpot->pBuffer);
  }
  sqlite3_free(pBlobSpot);
}

/**************************************************************************
** Layout specific utilities
**************************************************************************/

int nodeEdgeOverhead(int nEdgeVectorSize){
  return nEdgeVectorSize + VECTOR_EDGE_METADATA_SIZE;
}

int nodeOverhead(int nNodeVectorSize){
  return nNodeVectorSize + VECTOR_NODE_METADATA_SIZE;
}

int nodeEdgesMaxCount(const DiskAnnIndex *pIndex){
  unsigned int nMaxEdges = (pIndex->nBlockSize - nodeOverhead(pIndex->nNodeVectorSize)) / nodeEdgeOverhead(pIndex->nEdgeVectorSize);
  assert( nMaxEdges > 0);
  return nMaxEdges;
}

int nodeEdgesMetadataOffset(const DiskAnnIndex *pIndex){
  unsigned int offset;
  unsigned int nMaxEdges = nodeEdgesMaxCount(pIndex);
  offset = VECTOR_NODE_METADATA_SIZE + pIndex->nNodeVectorSize + nMaxEdges * pIndex->nEdgeVectorSize;
  assert( offset <= pIndex->nBlockSize );
  return offset;
}

void nodeBinInit(const DiskAnnIndex *pIndex, BlobSpot *pBlobSpot, u64 nRowid, Vector *pVector){
  assert( VECTOR_NODE_METADATA_SIZE + pIndex->nNodeVectorSize <= pBlobSpot->nBufferSize );

  memset(pBlobSpot->pBuffer, 0, pBlobSpot->nBufferSize);
  writeLE64(pBlobSpot->pBuffer, nRowid);
  // neighbours count already zero after memset - no need to set it explicitly

  vectorSerializeToBlob(pVector, pBlobSpot->pBuffer + VECTOR_NODE_METADATA_SIZE, pIndex->nNodeVectorSize);
}

void nodeBinVector(const DiskAnnIndex *pIndex, const BlobSpot *pBlobSpot, Vector *pVector) {
  assert( VECTOR_NODE_METADATA_SIZE + pIndex->nNodeVectorSize <= pBlobSpot->nBufferSize );

  vectorInitStatic(pVector, pIndex->nNodeVectorType, pIndex->nVectorDims, pBlobSpot->pBuffer + VECTOR_NODE_METADATA_SIZE);
}

u16 nodeBinEdges(const DiskAnnIndex *pIndex, const BlobSpot *pBlobSpot) {
  assert( VECTOR_NODE_METADATA_SIZE <= pBlobSpot->nBufferSize );

  return readLE16(pBlobSpot->pBuffer + sizeof(u64));
}

void nodeBinEdge(const DiskAnnIndex *pIndex, const BlobSpot *pBlobSpot, int iEdge, u64 *pRowid, float *pDistance, Vector *pVector) {
  u32 distance;
  int offset = nodeEdgesMetadataOffset(pIndex);

  if( pRowid != NULL ){
    assert( offset + (iEdge + 1) * VECTOR_EDGE_METADATA_SIZE <= pBlobSpot->nBufferSize );
    *pRowid = readLE64(pBlobSpot->pBuffer + offset + iEdge * VECTOR_EDGE_METADATA_SIZE + sizeof(u64));
  }
  if( pIndex->nFormatVersion != VECTOR_FORMAT_V1 && pDistance != NULL ){
    distance = readLE32(pBlobSpot->pBuffer + offset + iEdge * VECTOR_EDGE_METADATA_SIZE + sizeof(u32));
    *pDistance = *((float*)&distance);
  }
  if( pVector != NULL ){
    assert( VECTOR_NODE_METADATA_SIZE + pIndex->nNodeVectorSize + iEdge * pIndex->nEdgeVectorSize < offset );
    vectorInitStatic(
      pVector,
      pIndex->nEdgeVectorType,
      pIndex->nVectorDims,
      pBlobSpot->pBuffer + VECTOR_NODE_METADATA_SIZE + pIndex->nNodeVectorSize + iEdge * pIndex->nEdgeVectorSize
    );
  }
}

int nodeBinEdgeFindIdx(const DiskAnnIndex *pIndex, const BlobSpot *pBlobSpot, u64 nRowid) {
  int i, nEdges = nodeBinEdges(pIndex, pBlobSpot);
  // todo: if edges will be sorted by identifiers we can use binary search here (although speed up will be visible only on pretty loaded nodes: >128 edges)
  for(i = 0; i < nEdges; i++){
    u64 edgeId;
    nodeBinEdge(pIndex, pBlobSpot, i, &edgeId, NULL, NULL);
    if( edgeId == nRowid ){
      return i;
    }
  }
  return -1;
}

void nodeBinPruneEdges(const DiskAnnIndex *pIndex, BlobSpot *pBlobSpot, int nPruned) {
  assert( 0 <= nPruned && nPruned <= nodeBinEdges(pIndex, pBlobSpot) );

  writeLE16(pBlobSpot->pBuffer + sizeof(u64), nPruned);
}

// replace edge at position iReplace or add new one if iReplace == nEdges
void nodeBinReplaceEdge(const DiskAnnIndex *pIndex, BlobSpot *pBlobSpot, int iReplace, u64 nRowid, float distance, Vector *pVector) {
  int nMaxEdges = nodeEdgesMaxCount(pIndex);
  int nEdges = nodeBinEdges(pIndex, pBlobSpot);
  int edgeVectorOffset, edgeMetaOffset, itemsToMove;

  assert( 0 <= iReplace && iReplace < nMaxEdges );
  assert( 0 <= iReplace && iReplace <= nEdges );

  if( iReplace == nEdges ){
    nEdges++;
  }

  edgeVectorOffset = VECTOR_NODE_METADATA_SIZE + pIndex->nNodeVectorSize + iReplace * pIndex->nEdgeVectorSize;
  edgeMetaOffset = nodeEdgesMetadataOffset(pIndex) + iReplace * VECTOR_EDGE_METADATA_SIZE;

  assert( edgeVectorOffset + pIndex->nEdgeVectorSize <= pBlobSpot->nBufferSize );
  assert( edgeMetaOffset + VECTOR_EDGE_METADATA_SIZE <= pBlobSpot->nBufferSize );

  vectorSerializeToBlob(pVector, pBlobSpot->pBuffer + edgeVectorOffset, pIndex->nEdgeVectorSize);
  writeLE32(pBlobSpot->pBuffer + edgeMetaOffset + sizeof(u32), *((u32*)&distance));
  writeLE64(pBlobSpot->pBuffer + edgeMetaOffset + sizeof(u64), nRowid);

  writeLE16(pBlobSpot->pBuffer + sizeof(u64), nEdges);
}

// delete edge at position iDelete by swapping it with the last edge
void nodeBinDeleteEdge(const DiskAnnIndex *pIndex, BlobSpot *pBlobSpot, int iDelete) {
  int nEdges = nodeBinEdges(pIndex, pBlobSpot);
  int edgeVectorOffset, edgeMetaOffset, lastVectorOffset, lastMetaOffset;

  assert( 0 <= iDelete && iDelete < nEdges );

  edgeVectorOffset = VECTOR_NODE_METADATA_SIZE + pIndex->nNodeVectorSize + iDelete * pIndex->nEdgeVectorSize;
  lastVectorOffset = VECTOR_NODE_METADATA_SIZE + pIndex->nNodeVectorSize + (nEdges - 1) * pIndex->nEdgeVectorSize;
  edgeMetaOffset = nodeEdgesMetadataOffset(pIndex) + iDelete * VECTOR_EDGE_METADATA_SIZE;
  lastMetaOffset = nodeEdgesMetadataOffset(pIndex) + (nEdges - 1) * VECTOR_EDGE_METADATA_SIZE;

  assert( edgeVectorOffset + pIndex->nEdgeVectorSize <= pBlobSpot->nBufferSize );
  assert( lastVectorOffset + pIndex->nEdgeVectorSize <= pBlobSpot->nBufferSize );
  assert( edgeMetaOffset + VECTOR_EDGE_METADATA_SIZE <= pBlobSpot->nBufferSize );
  assert( lastMetaOffset + VECTOR_EDGE_METADATA_SIZE <= pBlobSpot->nBufferSize );

  if( edgeVectorOffset < lastVectorOffset ){
    memmove(pBlobSpot->pBuffer + edgeVectorOffset, pBlobSpot->pBuffer + lastVectorOffset, pIndex->nEdgeVectorSize);
    memmove(pBlobSpot->pBuffer + edgeMetaOffset, pBlobSpot->pBuffer + lastMetaOffset, VECTOR_EDGE_METADATA_SIZE);
  }

  writeLE16(pBlobSpot->pBuffer + sizeof(u64), nEdges - 1);
}

void nodeBinDebug(const DiskAnnIndex *pIndex, const BlobSpot *pBlobSpot) {
#if defined(SQLITE_DEBUG) && defined(SQLITE_VECTOR_TRACE)
  int nEdges, nMaxEdges, i;
  u64 nRowid;
  float distance = 0;
  Vector vector;

  nEdges = nodeBinEdges(pIndex, pBlobSpot);
  nMaxEdges = nodeEdgesMaxCount(pIndex);
  nodeBinVector(pIndex, pBlobSpot, &vector);

  DiskAnnTrace(("debug blob content for root=%lld (buffer size=%d)\n", pBlobSpot->nRowid, pBlobSpot->nBufferSize));
  DiskAnnTrace(("  nEdges=%d, nMaxEdges=%d, vector=", nEdges, nMaxEdges));
  vectorDump(&vector);
  for(i = 0; i < nEdges; i++){
    nodeBinEdge(pIndex, pBlobSpot, i, &nRowid, &distance, &vector);
    DiskAnnTrace(("  to=%lld, distance=%f, vector=", nRowid, distance));
    vectorDump(&vector);
  }
#endif
}

/*******************************************************************************
** DiskANN shadow index operations (some of them exposed as DiskANN internal API)
********************************************************************************/

int diskAnnCreateIndex(
  sqlite3 *db,
  const char *zDbSName,
  const char *zIdxName,
  const VectorIdxKey *pKey,
  VectorIdxParams *pParams,
  const char **pzErrMsg
){
  int rc;
  int type, dims, metric, neighbours;
  u64 maxNeighborsParam, blockSizeBytes;
  char *zSql;
  const char *zRowidColumnName;
  char columnSqlDefs[VECTOR_INDEX_SQL_RENDER_LIMIT]; // definition of columns (e.g. index_key INTEGER BINARY, index_key1 TEXT, ...)
  char columnSqlNames[VECTOR_INDEX_SQL_RENDER_LIMIT]; // just column names (e.g. index_key, index_key1, index_key2, ...)
  if( vectorIdxKeyDefsRender(pKey, "index_key", columnSqlDefs, sizeof(columnSqlDefs)) != 0 ){
    return SQLITE_ERROR;
  }
  if( vectorIdxKeyNamesRender(pKey->nKeyColumns, "index_key", columnSqlNames, sizeof(columnSqlNames)) != 0 ){
    return SQLITE_ERROR;
  }
  if( vectorIdxParamsPutU64(pParams, VECTOR_INDEX_TYPE_PARAM_ID, VECTOR_INDEX_TYPE_DISKANN) != 0 ){
    return SQLITE_ERROR;
  }
  type = vectorIdxParamsGetU64(pParams, VECTOR_TYPE_PARAM_ID);
  if( type == 0 ){
    return SQLITE_ERROR;
  }
  dims = vectorIdxParamsGetU64(pParams, VECTOR_DIM_PARAM_ID);
  if( dims == 0 ){
    return SQLITE_ERROR;
  }
  assert( 0 < dims && dims <= MAX_VECTOR_SZ );

  metric = vectorIdxParamsGetU64(pParams, VECTOR_METRIC_TYPE_PARAM_ID);
  if( metric == 0 ){
    metric = VECTOR_METRIC_TYPE_COS;
    if( vectorIdxParamsPutU64(pParams, VECTOR_METRIC_TYPE_PARAM_ID, metric) != 0 ){
      return SQLITE_ERROR;
    }
  }
  neighbours = vectorIdxParamsGetU64(pParams, VECTOR_COMPRESS_NEIGHBORS_PARAM_ID);
  if( neighbours == VECTOR_TYPE_FLOAT1BIT && metric != VECTOR_METRIC_TYPE_COS ){
    *pzErrMsg = "1-bit compression available only for cosine metric";
    return SQLITE_ERROR;
  }
  if( neighbours == 0 ){
    neighbours = type;
  }

  maxNeighborsParam = vectorIdxParamsGetU64(pParams, VECTOR_MAX_NEIGHBORS_PARAM_ID);
  if( maxNeighborsParam == 0 ){
    // 3 D**(1/2) gives good recall values (90%+)
    // we also want to keep disk overhead at moderate level - 50x of the disk size increase is the current upper bound
    maxNeighborsParam = MIN(3 * ((int)(sqrt(dims)) + 1), (50 * nodeOverhead(vectorDataSize(type, dims))) / nodeEdgeOverhead(vectorDataSize(neighbours, dims)) + 1);
  }
  blockSizeBytes = nodeOverhead(vectorDataSize(type, dims)) + maxNeighborsParam * (u64)nodeEdgeOverhead(vectorDataSize(neighbours, dims));
  if( blockSizeBytes > DISKANN_MAX_BLOCK_SZ ){
    return SQLITE_ERROR;
  }
  if( vectorIdxParamsPutU64(pParams, VECTOR_BLOCK_SIZE_PARAM_ID, MAX(256, blockSizeBytes))  != 0 ){
    return SQLITE_ERROR;
  }

  if( vectorIdxParamsGetF64(pParams, VECTOR_PRUNING_ALPHA_PARAM_ID) == 0 ){
    if( vectorIdxParamsPutF64(pParams, VECTOR_PRUNING_ALPHA_PARAM_ID, VECTOR_PRUNING_ALPHA_DEFAULT) != 0 ){
      return SQLITE_ERROR;
    }
  }
  if( vectorIdxParamsGetU64(pParams, VECTOR_INSERT_L_PARAM_ID) == 0 ){
    if( vectorIdxParamsPutU64(pParams, VECTOR_INSERT_L_PARAM_ID, VECTOR_INSERT_L_DEFAULT) != 0 ){
      return SQLITE_ERROR;
    }
  }
  if( vectorIdxParamsGetU64(pParams, VECTOR_SEARCH_L_PARAM_ID) == 0 ){
    if( vectorIdxParamsPutU64(pParams, VECTOR_SEARCH_L_PARAM_ID, VECTOR_SEARCH_L_DEFAULT) != 0 ){
      return SQLITE_ERROR;
    }
  }
  // we want to preserve rowid - so it must be explicit in the schema
  // also, we don't want to store redundant set of fields - so the strategy is like that:
  // 1. If we have single PK with INTEGER affinity and BINARY collation we only need single PK of same type
  // 2. In other case we need rowid PK and unique index over other fields
  if( vectorIdxKeyRowidLike(pKey) ){
    zSql = sqlite3MPrintf(
        db,
        "CREATE TABLE IF NOT EXISTS \"%w\".%s_shadow (%s, data BLOB, PRIMARY KEY (%s))",
        zDbSName,
        zIdxName,
        columnSqlDefs,
        columnSqlNames
        );
    zRowidColumnName = "index_key";
  }else{
    zSql = sqlite3MPrintf(
        db,
        "CREATE TABLE IF NOT EXISTS \"%w\".%s_shadow (rowid INTEGER PRIMARY KEY, %s, data BLOB, UNIQUE (%s))",
        zDbSName,
        zIdxName,
        columnSqlDefs,
        columnSqlNames
        );
    zRowidColumnName = "rowid";
  }
  rc = sqlite3_exec(db, zSql, 0, 0, 0);
  sqlite3DbFree(db, zSql);
  if( rc != SQLITE_OK ){
    return rc;
  }
  /*
   * vector blobs are usually pretty huge (more than a page size, for example, node block for 1024d f32 embeddings with 1bit compression will occupy ~20KB)
   * in this case, main table B-Tree takes on redundant shape where all leaf nodes has only 1 cell
   *
   * as we have a query which selects random row using OFFSET/LIMIT trick - we will need to read all these leaf nodes pages just to skip them
   * so, in order to remove this overhead for random row selection - we creating an index with just single column used
   * in this case B-Tree leafs will be full of rowids and the overhead for page reads will be very small
  */
  zSql = sqlite3MPrintf(
      db,
      "CREATE INDEX IF NOT EXISTS \"%w\".%s_shadow_idx ON %s_shadow (%s)",
      zDbSName,
      zIdxName,
      zIdxName,
      zRowidColumnName
  );
  rc = sqlite3_exec(db, zSql, 0, 0, 0);
  sqlite3DbFree(db, zSql);
  return rc;
}

int diskAnnClearIndex(sqlite3 *db, const char *zDbSName, const char *zIdxName) {
  char *zSql = sqlite3MPrintf(db, "DELETE FROM \"%w\".%s_shadow", zDbSName, zIdxName);
  int rc = sqlite3_exec(db, zSql, 0, 0, 0);
  sqlite3DbFree(db, zSql);
  return rc;
}

int diskAnnDropIndex(sqlite3 *db, const char *zDbSName, const char *zIdxName){
  char *zSql = sqlite3MPrintf(db, "DROP TABLE \"%w\".%s_shadow", zDbSName, zIdxName);
  int rc = sqlite3_exec(db, zSql, 0, 0, 0);
  sqlite3DbFree(db, zSql);
  return rc;
}

/*
 * Select random row from the shadow table and set its rowid to pRowid
 * returns SQLITE_DONE if no row found (this will be used to determine case when table is empty)
 * TODO: we need to make this selection procedure faster - now it works in linear time
*/
static int diskAnnSelectRandomShadowRow(const DiskAnnIndex *pIndex, u64 *pRowid){
  int rc;
  sqlite3_stmt *pStmt = NULL;
  char *zSql = NULL;

  zSql = sqlite3MPrintf(
    pIndex->db,
    "SELECT rowid FROM \"%w\".%s LIMIT 1 OFFSET ABS(RANDOM()) %% MAX((SELECT COUNT(*) FROM \"%w\".%s), 1)",
    pIndex->zDbSName, pIndex->zShadow, pIndex->zDbSName, pIndex->zShadow
  );
  if( zSql == NULL ){
    rc = SQLITE_NOMEM_BKPT;
    goto out;
  }
  rc = sqlite3_prepare_v2(pIndex->db, zSql, -1, &pStmt, 0);
  if( rc != SQLITE_OK ){
    goto out;
  }
  rc = sqlite3_step(pStmt);
  if( rc != SQLITE_ROW ){
    goto out;
  }

  assert( sqlite3_column_type(pStmt, 0) == SQLITE_INTEGER );
  *pRowid = sqlite3_column_int64(pStmt, 0);

  // check that we has only single row matching the criteria (otherwise - this is a bug)
  assert( sqlite3_step(pStmt) == SQLITE_DONE );
  rc = SQLITE_OK;
out:
  if( pStmt != NULL ){
    sqlite3_finalize(pStmt);
  }
  if( zSql != NULL ){
    sqlite3DbFree(pIndex->db, zSql);
  }
  return rc;
}

/*
 * Find row by keys from pInRow and set its rowid to pRowid
 * returns SQLITE_DONE if no row found (this will be used to determine case when table is empty)
*/
static int diskAnnGetShadowRowid(const DiskAnnIndex *pIndex, const VectorInRow *pInRow, u64 *pRowid) {
  int rc, i;
  sqlite3_stmt *pStmt = NULL;
  char *zSql = NULL;

  char columnSqlNames[VECTOR_INDEX_SQL_RENDER_LIMIT]; // just column names (e.g. index_key, index_key1, index_key2, ...)
  char columnSqlPlaceholders[VECTOR_INDEX_SQL_RENDER_LIMIT]; // just placeholders (e.g. ?,?,?, ...)
  if( vectorIdxKeyNamesRender(pInRow->nKeys, "index_key", columnSqlNames, sizeof(columnSqlNames)) != 0 ){
    rc = SQLITE_ERROR;
    goto out;
  }
  if( vectorInRowPlaceholderRender(pInRow, columnSqlPlaceholders, sizeof(columnSqlPlaceholders)) != 0 ){
    rc = SQLITE_ERROR;
    goto out;
  }
  zSql = sqlite3MPrintf(
      pIndex->db,
      "SELECT rowid FROM \"%w\".%s WHERE (%s) = (%s)",
      pIndex->zDbSName, pIndex->zShadow, columnSqlNames, columnSqlPlaceholders
  );
  if( zSql == NULL ){
    rc = SQLITE_NOMEM;
    goto out;
  }
  rc = sqlite3_prepare_v2(pIndex->db, zSql, -1, &pStmt, 0);
  if( rc != SQLITE_OK ){
    goto out;
  }
  for(i = 0; i < pInRow->nKeys; i++){
    rc = sqlite3_bind_value(pStmt, i + 1, vectorInRowKey(pInRow, i));
    if( rc != SQLITE_OK ){
      goto out;
    }
  }
  rc = sqlite3_step(pStmt);
  if( rc != SQLITE_ROW ){
    goto out;
  }

  assert( sqlite3_column_type(pStmt, 0) == SQLITE_INTEGER );
  *pRowid = sqlite3_column_int64(pStmt, 0);

  // check that we has only single row matching the criteria (otherwise - this is a bug)
  assert( sqlite3_step(pStmt) == SQLITE_DONE );
  rc = SQLITE_OK;
out:
  if( pStmt != NULL ){
    sqlite3_finalize(pStmt);
  }
  if( zSql != NULL ){
    sqlite3DbFree(pIndex->db, zSql);
  }
  return rc;
}

/*
 * Find row keys by rowid and put them in right into pRows structure
*/
static int diskAnnGetShadowRowKeys(const DiskAnnIndex *pIndex, u64 nRowid, const VectorIdxKey *pKey, VectorOutRows *pRows, int iRow) {
  int rc, i;
  sqlite3_stmt *pStmt = NULL;
  char *zSql = NULL;

  char columnSqlNames[VECTOR_INDEX_SQL_RENDER_LIMIT]; // just column names (e.g. index_key, index_key1, index_key2, ...)
  if( vectorIdxKeyNamesRender(pKey->nKeyColumns, "index_key", columnSqlNames, sizeof(columnSqlNames)) != 0 ){
    rc = SQLITE_ERROR;
    goto out;
  }
  zSql = sqlite3MPrintf(
      pIndex->db,
      "SELECT %s FROM \"%w\".%s WHERE rowid = ?",
      columnSqlNames, pIndex->zDbSName, pIndex->zShadow
  );
  if( zSql == NULL ){
    rc = SQLITE_NOMEM;
    goto out;
  }
  rc = sqlite3_prepare_v2(pIndex->db, zSql, -1, &pStmt, 0);
  if( rc != SQLITE_OK ){
    goto out;
  }
  rc = sqlite3_bind_int64(pStmt, 1, nRowid);
  if( rc != SQLITE_OK ){
    goto out;
  }
  rc = sqlite3_step(pStmt);
  if( rc != SQLITE_ROW ){
    goto out;
  }
  for(i = 0; i < pRows->nCols; i++){
    rc = vectorOutRowsPut(pRows, iRow, i, NULL, sqlite3_column_value(pStmt, i));
    if( rc != SQLITE_OK ){
      goto out;
    }
  }

  // check that we has only single row matching the criteria (otherwise - this is a bug)
  assert( sqlite3_step(pStmt) == SQLITE_DONE );
  rc = SQLITE_OK;
out:
  if( pStmt != NULL ){
    sqlite3_finalize(pStmt);
  }
  if( zSql != NULL ){
    sqlite3DbFree(pIndex->db, zSql);
  }
  return rc;
}

/*
 * Insert new empty row to the shadow table and set new rowid to the pRowid (data will be zeroe-filled blob of size pIndex->nBlockSize)
*/
static int diskAnnInsertShadowRow(const DiskAnnIndex *pIndex, const VectorInRow *pVectorInRow, u64 *pRowid){
  int rc, i;
  sqlite3_stmt *pStmt = NULL;
  char *zSql = NULL;

  char columnSqlPlaceholders[VECTOR_INDEX_SQL_RENDER_LIMIT]; // just placeholders (e.g. ?,?,?, ...)
  char columnSqlNames[VECTOR_INDEX_SQL_RENDER_LIMIT]; // just column names (e.g. index_key, index_key1, index_key2, ...)
  if( vectorInRowPlaceholderRender(pVectorInRow, columnSqlPlaceholders, sizeof(columnSqlPlaceholders)) != 0 ){
    rc = SQLITE_ERROR;
    goto out;
  }
  if( vectorIdxKeyNamesRender(pVectorInRow->nKeys, "index_key", columnSqlNames, sizeof(columnSqlNames)) != 0 ){
    return SQLITE_ERROR;
  }
  zSql = sqlite3MPrintf(
      pIndex->db,
      "INSERT INTO \"%w\".%s(%s, data) VALUES (%s, ?) RETURNING rowid",
      pIndex->zDbSName, pIndex->zShadow, columnSqlNames, columnSqlPlaceholders
  );
  if( zSql == NULL ){
    rc = SQLITE_NOMEM_BKPT;
    goto out;
  }
  rc = sqlite3_prepare_v2(pIndex->db, zSql, -1, &pStmt, 0);
  if( rc != SQLITE_OK ){
    goto out;
  }
  for(i = 0; i < pVectorInRow->nKeys; i++){
    rc = sqlite3_bind_value(pStmt, i + 1, vectorInRowKey(pVectorInRow, i));
    if( rc != SQLITE_OK ){
      goto out;
    }
  }
  rc = sqlite3_bind_zeroblob(pStmt, pVectorInRow->nKeys + 1, pIndex->nBlockSize);
  if( rc != SQLITE_OK ){
    goto out;
  }
  rc = sqlite3_step(pStmt);
  if( rc != SQLITE_ROW ){
    rc = SQLITE_ERROR;
    goto out;
  }

  assert( sqlite3_column_type(pStmt, 0) == SQLITE_INTEGER );
  *pRowid = sqlite3_column_int64(pStmt, 0);

  // check that we has only single row matching the criteria (otherwise - this is a bug)
  assert( sqlite3_step(pStmt) == SQLITE_DONE );
  rc = SQLITE_OK;
out:
  if( pStmt != NULL ){
    sqlite3_finalize(pStmt);
  }
  if( zSql != NULL ){
    sqlite3DbFree(pIndex->db, zSql);
  }
  return rc;
}

/*
 * Delete row from the shadow table
*/
static int diskAnnDeleteShadowRow(const DiskAnnIndex *pIndex, i64 nRowid){
  int rc;
  sqlite3_stmt *pStmt = NULL;
  char *zSql = sqlite3MPrintf(
      pIndex->db,
      "DELETE FROM \"%w\".%s WHERE rowid = ?",
      pIndex->zDbSName, pIndex->zShadow
  );
  if( zSql == NULL ){
    rc = SQLITE_NOMEM_BKPT;
    goto out;
  }
  rc = sqlite3_prepare_v2(pIndex->db, zSql, -1, &pStmt, 0);
  if( rc != SQLITE_OK ){
    goto out;
  }
  rc = sqlite3_bind_int64(pStmt, 1, nRowid);
  if( rc != SQLITE_OK ){
    goto out;
  }
  rc = sqlite3_step(pStmt);
  if( rc != SQLITE_DONE ){
    goto out;
  }
  rc = SQLITE_OK;
out:
  if( pStmt != NULL ){
    sqlite3_finalize(pStmt);
  }
  if( zSql != NULL ){
    sqlite3DbFree(pIndex->db, zSql);
  }
  return rc;
}

/**************************************************************************
** Generic utilities
**************************************************************************/

int initVectorPair(int nodeType, int edgeType, int dims, VectorPair *pPair){
  pPair->nodeType = nodeType;
  pPair->edgeType = edgeType;
  pPair->pNode = NULL;
  pPair->pEdge = NULL;
  if( pPair->nodeType == pPair->edgeType ){
    return 0;
  }
  pPair->pEdge = vectorAlloc(edgeType, dims);
  if( pPair->pEdge == NULL ){
    return SQLITE_NOMEM_BKPT;
  }
  return 0;
}

void loadVectorPair(VectorPair *pPair, const Vector *pVector){
  pPair->pNode = (Vector*)pVector;
  if( pPair->edgeType != pPair->nodeType ){
    vectorConvert(pPair->pNode, pPair->pEdge);
  }else{
    pPair->pEdge = pPair->pNode;
  }
}

void deinitVectorPair(VectorPair *pPair) {
  if( pPair->pEdge != NULL && pPair->pNode != pPair->pEdge ){
    vectorFree(pPair->pEdge);
  }
}

int distanceBufferInsertIdx(const float *aDistances, int nSize, int nMaxSize, float distance){
  int i;
#ifdef SQLITE_DEBUG
  for(i = 0; i < nSize - 1; i++){
    assert(aDistances[i] <= aDistances[i + 1]);
  }
#endif
  for(i = 0; i < nSize; i++){
    if( distance < aDistances[i] ){
      return i;
    }
  }
  return nSize < nMaxSize ? nSize : -1;
}

void bufferInsert(u8 *aBuffer, int nSize, int nMaxSize, int iInsert, int nItemSize, const u8 *pItem, u8 *pLast) {
  int itemsToMove;

  assert( nMaxSize > 0 && nItemSize > 0 );
  assert( nSize <= nMaxSize );
  assert( 0 <= iInsert && iInsert <= nSize && iInsert < nMaxSize );

  if( nSize == nMaxSize ){
    if( pLast != NULL ){
      memcpy(pLast, aBuffer + (nSize - 1) * nItemSize, nItemSize);
    }
    nSize--;
  }
  itemsToMove = nSize - iInsert;
  memmove(aBuffer + (iInsert + 1) * nItemSize, aBuffer + iInsert * nItemSize, itemsToMove * nItemSize);
  memcpy(aBuffer + iInsert * nItemSize, pItem, nItemSize);
}

void bufferDelete(u8 *aBuffer, int nSize, int iDelete, int nItemSize) {
  int itemsToMove;

  assert( nItemSize > 0 );
  assert( 0 <= iDelete && iDelete < nSize );

  itemsToMove = nSize - iDelete - 1;
  memmove(aBuffer + iDelete * nItemSize, aBuffer + (iDelete + 1) * nItemSize, itemsToMove * nItemSize);
}

/**************************************************************************
** DiskANN internals
**************************************************************************/

static float diskAnnVectorDistance(const DiskAnnIndex *pIndex, const Vector *pVec1, const Vector *pVec2){
  switch( pIndex->nDistanceFunc ){
    case VECTOR_METRIC_TYPE_COS:
      return vectorDistanceCos(pVec1, pVec2);
    case VECTOR_METRIC_TYPE_L2:
      return vectorDistanceL2(pVec1, pVec2);
    default:
      assert(0);
    break;
  }
  return 0.0;
}

static DiskAnnNode *diskAnnNodeAlloc(const DiskAnnIndex *pIndex, u64 nRowid){
  DiskAnnNode *pNode = sqlite3_malloc(sizeof(DiskAnnNode));
  if( pNode == NULL ){
    return NULL;
  }
  pNode->nRowid = nRowid;
  pNode->visited = 0;
  pNode->pNext = NULL;
  pNode->pBlobSpot = NULL;
  return pNode;
}

static void diskAnnNodeFree(DiskAnnNode *pNode){
  if( pNode->pBlobSpot != NULL ){
    blobSpotFree(pNode->pBlobSpot);
  }
  sqlite3_free(pNode);
}

static int diskAnnSearchCtxInit(const DiskAnnIndex *pIndex, DiskAnnSearchCtx *pCtx, const Vector* pQuery, int maxCandidates, int topCandidates, int blobMode){
  if( initVectorPair(pIndex->nNodeVectorType, pIndex->nEdgeVectorType, pIndex->nVectorDims, &pCtx->query) != 0 ){
    return SQLITE_NOMEM_BKPT;
  }
  loadVectorPair(&pCtx->query, pQuery);

  pCtx->aDistances = sqlite3_malloc(maxCandidates * sizeof(double));
  pCtx->aCandidates = sqlite3_malloc(maxCandidates * sizeof(DiskAnnNode*));
  pCtx->nCandidates = 0;
  pCtx->maxCandidates = maxCandidates;
  pCtx->aTopDistances = sqlite3_malloc(topCandidates * sizeof(double));
  pCtx->aTopCandidates = sqlite3_malloc(topCandidates * sizeof(DiskAnnNode*));
  pCtx->nTopCandidates = 0;
  pCtx->maxTopCandidates = topCandidates;
  pCtx->visitedList = NULL;
  pCtx->nUnvisited = 0;
  pCtx->blobMode = blobMode;

  if( pCtx->aDistances != NULL && pCtx->aCandidates != NULL && pCtx->aTopDistances != NULL && pCtx->aTopCandidates != NULL ){
    return SQLITE_OK;
  }
  if( pCtx->aDistances != NULL ){
    sqlite3_free(pCtx->aDistances);
  }
  if( pCtx->aCandidates != NULL ){
    sqlite3_free(pCtx->aCandidates);
  }
  if( pCtx->aTopDistances != NULL ){
    sqlite3_free(pCtx->aTopDistances);
  }
  if( pCtx->aTopCandidates != NULL ){
    sqlite3_free(pCtx->aTopCandidates);
  }
  deinitVectorPair(&pCtx->query);
  return SQLITE_NOMEM_BKPT;
}

static void diskAnnSearchCtxDeinit(DiskAnnSearchCtx *pCtx){
  int i;
  DiskAnnNode *pNode, *pNext;

  // usually, aCandidates array should contain only visited candidates (and they are duplicated in the visited list - so will be managed by code below)
  // but in case of early return from error there can be unvisited candidates in the aCandidates array
  for(i = 0; i < pCtx->nCandidates; i++){
    if( !pCtx->aCandidates[i]->visited ){
      diskAnnNodeFree(pCtx->aCandidates[i]);
    }
  }

  pNode = pCtx->visitedList;
  while( pNode != NULL ){
    pNext = pNode->pNext;
    diskAnnNodeFree(pNode);
    pNode = pNext;
  }
  sqlite3_free(pCtx->aCandidates);
  sqlite3_free(pCtx->aDistances);
  sqlite3_free(pCtx->aTopCandidates);
  sqlite3_free(pCtx->aTopDistances);
  deinitVectorPair(&pCtx->query);
}

// check if we visited this node earlier
// todo: we better to replace this linear check with something more efficient
static int diskAnnSearchCtxIsVisited(const DiskAnnSearchCtx *pCtx, u64 nRowid){
  DiskAnnNode *pNode;
  for(pNode = pCtx->visitedList; pNode != NULL; pNode = pNode->pNext){
    if( pNode->nRowid == nRowid ){
      return 1;
    }
  }
  return 0;
}

// check if we already have candidate in the queue
// todo: we better to replace this linear check with something more efficient
static int diskAnnSearchCtxHasCandidate(const DiskAnnSearchCtx *pCtx, u64 nRowid){
  int i;
  for(i = 0; i < pCtx->nCandidates; i++){
    if( pCtx->aCandidates[i]->nRowid == nRowid ){
      return 1;
    }
  }
  return 0;
}

// return position of new candition or -1 if we should not add it to the quee
static int diskAnnSearchCtxShouldAddCandidate(const DiskAnnIndex *pIndex, const DiskAnnSearchCtx *pCtx, float candidateDist){
  int i;
  // Find the index of the candidate that is further away from the query
  // vector than the one we're inserting.
  for(i = 0; i < pCtx->nCandidates; i++){
    float distCandidate = pCtx->aDistances[i];
    if( candidateDist < distCandidate ){
      return i;
    }
  }
  return pCtx->nCandidates < pCtx->maxCandidates ? pCtx->nCandidates : -1;
}

// mark node as visited and put it in the head of visitedList
static void diskAnnSearchCtxMarkVisited(DiskAnnSearchCtx *pCtx, DiskAnnNode *pNode, float distance){
  int iInsert;

  assert( pCtx->nUnvisited > 0 );
  assert( pNode->visited == 0 );

  pNode->visited = 1;
  pCtx->nUnvisited--;

  pNode->pNext = pCtx->visitedList;
  pCtx->visitedList = pNode;

  iInsert = distanceBufferInsertIdx(pCtx->aTopDistances, pCtx->nTopCandidates, pCtx->maxTopCandidates, distance);
  if( iInsert < 0 ){
    return;
  }
  bufferInsert((u8*)pCtx->aTopCandidates, pCtx->nTopCandidates, pCtx->maxTopCandidates, iInsert, sizeof(DiskAnnNode*), (u8*)&pNode, NULL);
  bufferInsert((u8*)pCtx->aTopDistances, pCtx->nTopCandidates, pCtx->maxTopCandidates, iInsert, sizeof(float), (u8*)&distance, NULL);
  pCtx->nTopCandidates = MIN(pCtx->nTopCandidates + 1, pCtx->maxTopCandidates);
}

static int diskAnnSearchCtxHasUnvisited(const DiskAnnSearchCtx *pCtx){
  return pCtx->nUnvisited > 0;
}

static void diskAnnSearchCtxGetCandidate(DiskAnnSearchCtx *pCtx, int i, DiskAnnNode **ppNode, float *pDistance){
  assert( 0 <= i && i < pCtx->nCandidates );
  *ppNode = pCtx->aCandidates[i];
  *pDistance = pCtx->aDistances[i];
}

static void diskAnnSearchCtxDeleteCandidate(DiskAnnSearchCtx *pCtx, int iDelete){
  int i;
  assert( pCtx->nUnvisited > 0 );
  assert( !pCtx->aCandidates[iDelete]->visited );
  assert( pCtx->aCandidates[iDelete]->pBlobSpot == NULL );

  diskAnnNodeFree(pCtx->aCandidates[iDelete]);
  bufferDelete((u8*)pCtx->aCandidates, pCtx->nCandidates, iDelete, sizeof(DiskAnnNode*));
  bufferDelete((u8*)pCtx->aDistances, pCtx->nCandidates, iDelete, sizeof(float));

  pCtx->nCandidates--;
  pCtx->nUnvisited--;
}

static void diskAnnSearchCtxInsertCandidate(DiskAnnSearchCtx *pCtx, int iInsert, DiskAnnNode* pCandidate, float distance){
  DiskAnnNode *pLast = NULL;
  bufferInsert((u8*)pCtx->aCandidates, pCtx->nCandidates, pCtx->maxCandidates, iInsert, sizeof(DiskAnnNode*), (u8*)&pCandidate, (u8*)&pLast);
  bufferInsert((u8*)pCtx->aDistances, pCtx->nCandidates, pCtx->maxCandidates, iInsert, sizeof(float), (u8*)&distance, NULL);
  pCtx->nCandidates = MIN(pCtx->nCandidates + 1, pCtx->maxCandidates);
  if( pLast != NULL && !pLast->visited ){
    // since pLast is not visited it should have uninitialized pBlobSpot - so it's safe to completely free the node
    assert( pLast->pBlobSpot == NULL );
    pCtx->nUnvisited--;
    diskAnnNodeFree(pLast);
  }
  pCtx->nUnvisited++;
}

// find closest candidate
// we can return early as aCandidate array is sorted by the distance from the query
static int diskAnnSearchCtxFindClosestCandidateIdx(const DiskAnnSearchCtx *pCtx){
  int i;
#ifdef SQLITE_DEBUG
  for(i = 0; i < pCtx->nCandidates - 1; i++){
    assert(pCtx->aDistances[i] <= pCtx->aDistances[i + 1]);
  }
#endif
  for(i = 0; i < pCtx->nCandidates; i++){
    DiskAnnNode *pCandidate = pCtx->aCandidates[i];
    if( pCandidate->visited ){
      continue;
    }
    return i;
  }
  return -1;
}

/**************************************************************************
** DiskANN core
**************************************************************************/

// return position for new edge(C) which will replace previous edge on that position or -1 if we should ignore it
// we also check that no current edge(B) will "prune" new vertex: i.e. dist(B, C) >= (means worse than) alpha * dist(node, C) for all current edges
// if any edge(B) will "prune" new edge(C) we will ignore it (return -1)
static int diskAnnReplaceEdgeIdx(
  const DiskAnnIndex *pIndex,
  BlobSpot *pNodeBlob,
  u64 newRowid,
  VectorPair *pNewVector,
  VectorPair *pPlaceholder,
  float *pNodeToNew
) {
  int i, nEdges, nMaxEdges, iReplace = -1;
  Vector nodeVector, edgeVector;
  float nodeToNew, nodeToReplace;

  nEdges = nodeBinEdges(pIndex, pNodeBlob);
  nMaxEdges = nodeEdgesMaxCount(pIndex);
  nodeBinVector(pIndex, pNodeBlob, &nodeVector);
  loadVectorPair(pPlaceholder, &nodeVector);

  // we need to evaluate potentially approximate distance here in order to correctly compare it with edge distances
  nodeToNew = diskAnnVectorDistance(pIndex, pPlaceholder->pEdge, pNewVector->pEdge);
  *pNodeToNew = nodeToNew;

  for(i = nEdges - 1; i >= 0; i--){
    u64 edgeRowid;
    float edgeToNew, nodeToEdge;

    nodeBinEdge(pIndex, pNodeBlob, i, &edgeRowid, &nodeToEdge, &edgeVector);
    if( edgeRowid == newRowid ){
      // deletes can leave "zombie" edges in the graph and we must override them and not store duplicate edges in the node
      return i;
    }

    if( pIndex->nFormatVersion == VECTOR_FORMAT_V1 ){
      nodeToEdge = diskAnnVectorDistance(pIndex, pPlaceholder->pEdge, &edgeVector);
    }

    edgeToNew = diskAnnVectorDistance(pIndex, &edgeVector, pNewVector->pEdge);
    if( nodeToNew > pIndex->pruningAlpha * edgeToNew ){
      return -1;
    }
    if( nodeToNew < nodeToEdge && (iReplace == -1 || nodeToReplace < nodeToEdge) ){
      nodeToReplace = nodeToEdge;
      iReplace = i;
    }
  }
  if( nEdges < nMaxEdges ){
    return nEdges;
  }
  return iReplace;
}

// prune edges after we inserted new edge at position iInserted
// we only need to check for edges which will be pruned by new vertex
// no need to check for other pairs as we checked them on previous insertions
static void diskAnnPruneEdges(const DiskAnnIndex *pIndex, BlobSpot *pNodeBlob, int iInserted, VectorPair *pPlaceholder) {
  int i, s, nEdges;
  Vector nodeVector, hintEdgeVector;
  u64 hintRowid;

  nodeBinVector(pIndex, pNodeBlob, &nodeVector);
  loadVectorPair(pPlaceholder, &nodeVector);

  nEdges = nodeBinEdges(pIndex, pNodeBlob);

  assert( 0 <= iInserted && iInserted < nEdges );

#if defined(SQLITE_DEBUG) && defined(SQLITE_VECTOR_TRACE)
  DiskAnnTrace(("before pruning:\n"));
  nodeBinDebug(pIndex, pNodeBlob);
#endif

  nodeBinEdge(pIndex, pNodeBlob, iInserted, &hintRowid, NULL, &hintEdgeVector);

  // remove edges which is no longer interesting due to the addition of iInserted
  i = 0;
  while( i < nEdges ){
    Vector edgeVector;
    float nodeToEdge, hintToEdge;
    u64 edgeRowid;
    nodeBinEdge(pIndex, pNodeBlob, i, &edgeRowid, &nodeToEdge, &edgeVector);

    if( hintRowid == edgeRowid ){
      i++;
      continue;
    }
    if( pIndex->nFormatVersion == VECTOR_FORMAT_V1 ){
      nodeToEdge = diskAnnVectorDistance(pIndex, pPlaceholder->pEdge, &edgeVector);
    }

    hintToEdge = diskAnnVectorDistance(pIndex, &hintEdgeVector, &edgeVector);
    if( nodeToEdge > pIndex->pruningAlpha * hintToEdge ){
      nodeBinDeleteEdge(pIndex, pNodeBlob, i);
      nEdges--;
    }else{
      i++;
    }
  }

#if defined(SQLITE_DEBUG) && defined(SQLITE_VECTOR_TRACE)
  DiskAnnTrace(("after pruning:\n"));
  nodeBinDebug(pIndex, pNodeBlob);
#endif

  // Every node needs at least one edge node so that the graph is connected.
  assert( nEdges > 0 );
}

// main search routine - called from both SEARCH and INSERT operation
static int diskAnnSearchInternal(DiskAnnIndex *pIndex, DiskAnnSearchCtx *pCtx, u64 nStartRowid, char **pzErrMsg){
  DiskAnnTrace(("diskAnnSearchInternal: ready to search: rootId=%lld\n", nStartRowid));
  DiskAnnNode *start = NULL;
  // in case of SEARCH operation (blobMode == DISKANN_BLOB_READONLY) we don't need to preserve all node blobs in the memory
  // so we will reload them to the single blob instead of creating new blob for every new visited node
  BlobSpot *pReusableBlobSpot = NULL;
  Vector startVector;
  float startDistance;
  int rc, i, nVisited = 0;

  start = diskAnnNodeAlloc(pIndex, nStartRowid);
  if( start == NULL ){
    *pzErrMsg = sqlite3_mprintf("vector index(search): failed to allocate new node");
    rc = SQLITE_NOMEM_BKPT;
    goto out;
  }

  rc = blobSpotCreate(pIndex, &start->pBlobSpot, nStartRowid, pIndex->nBlockSize, pCtx->blobMode);
  if( rc != SQLITE_OK ){
    *pzErrMsg = sqlite3_mprintf("vector index(search): failed to create new blob");
    goto out;
  }

  rc = blobSpotReload(pIndex, start->pBlobSpot, nStartRowid, pIndex->nBlockSize);
  if( rc != SQLITE_OK ){
    *pzErrMsg = sqlite3_mprintf("vector index(search): failed to load new blob");
    goto out;
  }

  nodeBinVector(pIndex, start->pBlobSpot, &startVector);
  startDistance = diskAnnVectorDistance(pIndex, pCtx->query.pNode, &startVector);

  if( pCtx->blobMode == DISKANN_BLOB_READONLY ){
    assert( start->pBlobSpot != NULL );
    pReusableBlobSpot = start->pBlobSpot;
    start->pBlobSpot = NULL;
  }
  // we are transferring ownership of start node to the DiskAnnSearchCtx - so we no longer need to clean up anything in this function
  // (caller must take care of DiskAnnSearchCtx resource reclamation)
  diskAnnSearchCtxInsertCandidate(pCtx, 0, start, startDistance);
  start = NULL;

  while( diskAnnSearchCtxHasUnvisited(pCtx) ){
    int nEdges;
    Vector vCandidate;
    DiskAnnNode *pCandidate;
    BlobSpot *pCandidateBlob;
    float distance;
    int iCandidate = diskAnnSearchCtxFindClosestCandidateIdx(pCtx);
    diskAnnSearchCtxGetCandidate(pCtx, iCandidate, &pCandidate, &distance);

    rc = SQLITE_OK;
    if( pReusableBlobSpot != NULL ){
      rc = blobSpotReload(pIndex, pReusableBlobSpot, pCandidate->nRowid, pIndex->nBlockSize);
      pCandidateBlob = pReusableBlobSpot;
    }else{
      // we are lazy-loading blobs, so pBlobSpot usually NULL except for the first start node
      if( pCandidate->pBlobSpot == NULL ){
        rc = blobSpotCreate(pIndex, &pCandidate->pBlobSpot, pCandidate->nRowid, pIndex->nBlockSize, pCtx->blobMode);
      }
      if( rc == SQLITE_OK ){
        rc = blobSpotReload(pIndex, pCandidate->pBlobSpot, pCandidate->nRowid, pIndex->nBlockSize);
      }
      pCandidateBlob = pCandidate->pBlobSpot;
    }

    if( rc == DISKANN_ROW_NOT_FOUND ){
      // it's possible that some edges corresponds to already deleted nodes - so processing this case gracefully
      // (since we store only "forward" edges of the graph - we can accidentally preserve some "zombie" edges in the graph)
      // todo: we better to fix graph and remove these edges from node (but it must be done carefully since we have READONLY and WRITABLE separate modes)
      diskAnnSearchCtxDeleteCandidate(pCtx, iCandidate);
      continue;
    }else if( rc != SQLITE_OK ){
      *pzErrMsg = sqlite3_mprintf("vector index(search): failed to create new blob for candidate");
      goto out;
    }

    nVisited += 1;
    DiskAnnTrace(("visiting candidate(%d): id=%lld\n", nVisited, pCandidate->nRowid));
    nodeBinVector(pIndex, pCandidateBlob, &vCandidate);
    nEdges = nodeBinEdges(pIndex, pCandidateBlob);

    // if pNodeQuery != pEdgeQuery then distance from aDistances is approximate and we must recalculate it
    if( pCtx->query.pNode != pCtx->query.pEdge ){
      distance = diskAnnVectorDistance(pIndex, &vCandidate, pCtx->query.pNode);
    }

    diskAnnSearchCtxMarkVisited(pCtx, pCandidate, distance);

    for(i = 0; i < nEdges; i++){
      u64 edgeRowid;
      Vector edgeVector;
      float edgeDistance;
      int iInsert;
      DiskAnnNode *pNewCandidate;
      nodeBinEdge(pIndex, pCandidateBlob, i, &edgeRowid, NULL, &edgeVector);
      if( diskAnnSearchCtxIsVisited(pCtx, edgeRowid) || diskAnnSearchCtxHasCandidate(pCtx, edgeRowid) ){
        continue;
      }

      edgeDistance = diskAnnVectorDistance(pIndex, pCtx->query.pEdge, &edgeVector);
      iInsert = diskAnnSearchCtxShouldAddCandidate(pIndex, pCtx, edgeDistance);
      if( iInsert < 0 ){
        continue;
      }
      pNewCandidate = diskAnnNodeAlloc(pIndex, edgeRowid);
      if( pNewCandidate == NULL ){
        continue;
      }
      DiskAnnTrace(("want to insert new candidate %lld at position %d with distance %f\n", edgeRowid, iInsert, edgeDistance));
      // note that here we are inserting "bare" candidate with NULL blob
      // this way we fully postpone blob loading until we will really visit the candidate
      // (and this is not always the case since other better candidate can excommunicate this candidate)
      diskAnnSearchCtxInsertCandidate(pCtx, iInsert, pNewCandidate, edgeDistance);
    }
  }
  DiskAnnTrace(("diskAnnSearchInternal: search context in the end\n", nStartRowid));
#if defined(SQLITE_DEBUG) && defined(SQLITE_VECTOR_TRACE)
  for(i = 0; i < pCtx->nCandidates; i++){
    DiskAnnTrace(("%lld(%f) ", pCtx->aCandidates[i]->nRowid, pCtx->aDistances[i]));
  }
  DiskAnnTrace(("\n"));
#endif
out:
  if( start != NULL ){
    diskAnnNodeFree(start);
  }
  if( pReusableBlobSpot != NULL ){
    blobSpotFree(pReusableBlobSpot);
  }
  return SQLITE_OK;
}

/**************************************************************************
** DiskANN main internal API
**************************************************************************/

// search k nearest neighbours for pVector in the pIndex (with pKey primary key structure) and put result in the pRows output
int diskAnnSearch(
  DiskAnnIndex *pIndex,
  const Vector *pVector,
  int k,
  const VectorIdxKey *pKey,
  VectorOutRows *pRows,
  char **pzErrMsg
){
  int rc = SQLITE_OK;
  DiskAnnSearchCtx ctx;
  u64 nStartRowid;
  int nOutRows;
  int i;

  DiskAnnTrace(("diskAnnSearch started\n"));

  if( k < 0 ){
    *pzErrMsg = sqlite3_mprintf("vector index(search): k must be a non-negative integer");
    return SQLITE_ERROR;
  }
  if( pVector->dims != pIndex->nVectorDims ){
    *pzErrMsg = sqlite3_mprintf("vector index(search): dimensions are different: %d != %d", pVector->dims, pIndex->nVectorDims);
    return SQLITE_ERROR;
  }
  if( pVector->type != pIndex->nNodeVectorType ){
    *pzErrMsg = sqlite3_mprintf("vector index(search): vector type differs from column type: %d != %d", pVector->type, pIndex->nNodeVectorType);
    return SQLITE_ERROR;
  }

  rc = diskAnnSelectRandomShadowRow(pIndex, &nStartRowid);
  if( rc == SQLITE_DONE ){
    // SQLITE_DONE returned from select function is a signal that table is empty table - return zero rows in this case
    pRows->nRows = 0;
    pRows->nCols = pKey->nKeyColumns;
    return SQLITE_OK;
  }else if( rc != SQLITE_OK ){
    *pzErrMsg = sqlite3_mprintf("vector index(search): failed to select start node for search");
    return rc;
  }
  rc = diskAnnSearchCtxInit(pIndex, &ctx, pVector, pIndex->searchL, k, DISKANN_BLOB_READONLY);
  if( rc != SQLITE_OK ){
    *pzErrMsg = sqlite3_mprintf("vector index(search): failed to initialize search context");
    goto out;
  }
  rc = diskAnnSearchInternal(pIndex, &ctx, nStartRowid, pzErrMsg);
  if( rc != SQLITE_OK ){
    goto out;
  }
  nOutRows = MIN(k, ctx.nTopCandidates);
  rc = vectorOutRowsAlloc(pIndex->db, pRows, nOutRows, pKey->nKeyColumns, vectorIdxKeyRowidLike(pKey));
  if( rc != SQLITE_OK ){
    *pzErrMsg = sqlite3_mprintf("vector index(search): failed to allocate output rows");
    goto out;
  }
  for(i = 0; i < nOutRows; i++){
    if( pRows->aIntValues != NULL ){
      rc = vectorOutRowsPut(pRows, i, 0, &ctx.aTopCandidates[i]->nRowid, NULL);
    }else{
      rc = diskAnnGetShadowRowKeys(pIndex, ctx.aTopCandidates[i]->nRowid, pKey, pRows, i);
    }
    if( rc != SQLITE_OK ){
      *pzErrMsg = sqlite3_mprintf("vector index(search): failed to put result in the output row");
      goto out;
    }
  }
  rc = SQLITE_OK;
out:
  diskAnnSearchCtxDeinit(&ctx);
  return rc;
}

// insert pVectorInRow in the pIndex
int diskAnnInsert(
  DiskAnnIndex *pIndex,
  const VectorInRow *pVectorInRow,
  char **pzErrMsg
){
  int rc, first = 0;
  u64 nStartRowid, nNewRowid;
  BlobSpot *pBlobSpot = NULL;
  DiskAnnNode *pVisited;
  DiskAnnSearchCtx ctx;
  VectorPair vInsert, vCandidate;
  vInsert.pNode = NULL; vInsert.pEdge = NULL;
  vCandidate.pNode = NULL; vCandidate.pEdge = NULL;

  if( pVectorInRow->pVector->dims != pIndex->nVectorDims ){
    *pzErrMsg = sqlite3_mprintf("vector index(insert): dimensions are different: %d != %d", pVectorInRow->pVector->dims, pIndex->nVectorDims);
    return SQLITE_ERROR;
  }
  if( pVectorInRow->pVector->type != pIndex->nNodeVectorType ){
    *pzErrMsg = sqlite3_mprintf("vector index(insert): vector type differs from column type: %d != %d", pVectorInRow->pVector->type, pIndex->nNodeVectorType);
    return SQLITE_ERROR;
  }

  DiskAnnTrace(("diskAnnInset started\n"));

  rc = diskAnnSearchCtxInit(pIndex, &ctx, pVectorInRow->pVector, pIndex->insertL, 1, DISKANN_BLOB_WRITABLE);
  if( rc != SQLITE_OK ){
    *pzErrMsg = sqlite3_mprintf("vector index(insert): failed to initialize search context");
    return rc;
  }

  if( initVectorPair(pIndex->nNodeVectorType, pIndex->nEdgeVectorType, pIndex->nVectorDims, &vInsert) != 0 ){
    *pzErrMsg = sqlite3_mprintf("vector index(insert): unable to allocate mem for node VectorPair");
    rc = SQLITE_NOMEM_BKPT;
    goto out;
  }

  if( initVectorPair(pIndex->nNodeVectorType, pIndex->nEdgeVectorType, pIndex->nVectorDims, &vCandidate) != 0 ){
    *pzErrMsg = sqlite3_mprintf("vector index(insert): unable to allocate mem for candidate VectorPair");
    rc = SQLITE_NOMEM_BKPT;
    goto out;
  }

  // note: we must select random row before we will insert new row in the shadow table
  rc = diskAnnSelectRandomShadowRow(pIndex, &nStartRowid);
  if( rc == SQLITE_DONE ){
    first = 1;
  }else if( rc != SQLITE_OK ){
    *pzErrMsg = sqlite3_mprintf("vector index(insert): failed to select start node for search");
    rc = SQLITE_ERROR;
    goto out;
  }
  if( !first ){
    // search is made before insetion in order to simplify life with "zombie" edges which can have same IDs as new inserted row
    rc = diskAnnSearchInternal(pIndex, &ctx, nStartRowid, pzErrMsg);
    if( rc != SQLITE_OK ){
      goto out;
    }
  }

  rc = diskAnnInsertShadowRow(pIndex, pVectorInRow, &nNewRowid);
  if( rc != SQLITE_OK ){
    *pzErrMsg = sqlite3_mprintf("vector index(insert): failed to insert shadow row");
    goto out;
  }

  rc = blobSpotCreate(pIndex, &pBlobSpot, nNewRowid, pIndex->nBlockSize, 1);
  if( rc != SQLITE_OK ){
    *pzErrMsg = sqlite3_mprintf("vector index(insert): failed to read blob for shadow row");
    goto out;
  }
  nodeBinInit(pIndex, pBlobSpot, nNewRowid, pVectorInRow->pVector);

  if( first ){
    DiskAnnTrace(("inserted first row\n"));
    rc = SQLITE_OK;
    goto out;
  }
  // first pass - add all visited nodes as a potential neighbours of new node
  for(pVisited = ctx.visitedList; pVisited != NULL; pVisited = pVisited->pNext){
    Vector nodeVector;
    int iReplace;
    float nodeToNew;

    nodeBinVector(pIndex, pVisited->pBlobSpot, &nodeVector);
    loadVectorPair(&vCandidate, &nodeVector);

    iReplace = diskAnnReplaceEdgeIdx(pIndex, pBlobSpot, pVisited->nRowid, &vCandidate, &vInsert, &nodeToNew);
    if( iReplace == -1 ){
      continue;
    }
    nodeBinReplaceEdge(pIndex, pBlobSpot, iReplace, pVisited->nRowid, nodeToNew, vCandidate.pEdge);
    diskAnnPruneEdges(pIndex, pBlobSpot, iReplace, &vInsert);
  }

  // second pass - add new node as a potential neighbour of all visited nodes
  loadVectorPair(&vInsert, pVectorInRow->pVector);
  for(pVisited = ctx.visitedList; pVisited != NULL; pVisited = pVisited->pNext){
    int iReplace;
    float nodeToNew;

    iReplace = diskAnnReplaceEdgeIdx(pIndex, pVisited->pBlobSpot, nNewRowid, &vInsert, &vCandidate, &nodeToNew);
    if( iReplace == -1 ){
      continue;
    }
    nodeBinReplaceEdge(pIndex, pVisited->pBlobSpot, iReplace, nNewRowid, nodeToNew, vInsert.pEdge);
    diskAnnPruneEdges(pIndex, pVisited->pBlobSpot, iReplace, &vCandidate);

    rc = blobSpotFlush(pIndex, pVisited->pBlobSpot);
    if( rc != SQLITE_OK ){
      *pzErrMsg = sqlite3_mprintf("vector index(insert): failed to flush blob");
      goto out;
    }
  }

  rc = SQLITE_OK;
out:
  deinitVectorPair(&vInsert);
  deinitVectorPair(&vCandidate);
  if( rc == SQLITE_OK ){
    rc = blobSpotFlush(pIndex, pBlobSpot);
    if( rc != SQLITE_OK ){
      *pzErrMsg = sqlite3_mprintf("vector index(insert): failed to flush blob");
    }
  }
  if( pBlobSpot != NULL ){
    blobSpotFree(pBlobSpot);
  }
  diskAnnSearchCtxDeinit(&ctx);
  return rc;
}

// delete pInRow from pIndex
int diskAnnDelete(
  DiskAnnIndex *pIndex,
  const VectorInRow *pInRow,
  char **pzErrMsg
){
  int rc;
  BlobSpot *pNodeBlob = NULL, *pEdgeBlob = NULL;
  u64 nodeRowid;
  int iDelete, nNeighbours, i;
  if( vectorInRowTryGetRowid(pInRow, &nodeRowid) != 0 ){
    rc = diskAnnGetShadowRowid(pIndex, pInRow, &nodeRowid);
    if( rc != SQLITE_OK ){
      *pzErrMsg = sqlite3_mprintf("vector index(delete): failed to determined node id for deletion");
      goto out;
    }
  }

  DiskAnnTrace(("diskAnnDelete started: rowid=%lld\n", nodeRowid));

  rc = blobSpotCreate(pIndex, &pNodeBlob, nodeRowid, pIndex->nBlockSize, DISKANN_BLOB_WRITABLE);
  if( rc == DISKANN_ROW_NOT_FOUND ){
    // as we omit rows with NULL values during insert, it can be the case that there is nothing to delete in the index, while row exists in the base table
    // so, we must simply silently stop delete process as there is nothing to delete from index
    rc = SQLITE_OK;
    goto out;
  }else if( rc != SQLITE_OK ){
    *pzErrMsg = sqlite3_mprintf("vector index(delete): failed to create blob for node row");
    goto out;
  }
  rc = blobSpotReload(pIndex, pNodeBlob, nodeRowid, pIndex->nBlockSize);
  if( rc != 0 ){
    *pzErrMsg = sqlite3_mprintf("vector index(delete): failed to reload blob for node row");
    goto out;
  }
  rc = blobSpotCreate(pIndex, &pEdgeBlob, nodeRowid, pIndex->nBlockSize, DISKANN_BLOB_WRITABLE);
  if( rc != SQLITE_OK ){
    *pzErrMsg = sqlite3_mprintf("vector index(delete): failed to create blob for edge rows");
    goto out;
  }
  nNeighbours = nodeBinEdges(pIndex, pNodeBlob);
  for(i = 0; i < nNeighbours; i++){
    u64 edgeRowid;
    nodeBinEdge(pIndex, pNodeBlob, i, &edgeRowid, NULL, NULL);
    rc = blobSpotReload(pIndex, pEdgeBlob, edgeRowid, pIndex->nBlockSize);
    if( rc == DISKANN_ROW_NOT_FOUND ){
      continue;
    }else if( rc != SQLITE_OK ){
      *pzErrMsg = sqlite3_mprintf("vector index(delete): failed to reload blob for edge row: %d", rc);
      goto out;
    }
    iDelete = nodeBinEdgeFindIdx(pIndex, pEdgeBlob, edgeRowid);
    if( iDelete == -1 ){
      continue;
    }
    nodeBinDeleteEdge(pIndex, pEdgeBlob, iDelete);
    rc = blobSpotFlush(pIndex, pEdgeBlob);
    if( rc != SQLITE_OK ){
      *pzErrMsg = sqlite3_mprintf("vector index(delete): failed to flush blob for edge row");
      goto out;
    }
  }

  rc = diskAnnDeleteShadowRow(pIndex, nodeRowid);
  if( rc != SQLITE_OK ){
    *pzErrMsg = sqlite3_mprintf("vector index(delete): failed to remove shadow row");
    goto out;
  }

  rc = SQLITE_OK;
out:
  if( pNodeBlob != NULL ){
    blobSpotFree(pNodeBlob);
  }
  if( pEdgeBlob != NULL ){
    blobSpotFree(pEdgeBlob);
  }
  return rc;
}

// open index with zIdxName and pParams serialized binary parameters and set result to the ppIndex
int diskAnnOpenIndex(
  sqlite3 *db,                       /* Database connection */
  const char *zDbSName,              /* Database schema name */
  const char *zIdxName,              /* Index name */
  const VectorIdxParams *pParams,    /* Index parameters */
  DiskAnnIndex **ppIndex             /* OUT: Index */
){
  DiskAnnIndex *pIndex;
  u64 nBlockSize;
  int compressNeighbours;
  pIndex = sqlite3DbMallocRaw(db, sizeof(DiskAnnIndex));
  if( pIndex == NULL ){
    return SQLITE_NOMEM;
  }
  pIndex->db = db;
  pIndex->zDbSName = sqlite3DbStrDup(db, zDbSName);
  pIndex->zName = sqlite3DbStrDup(db, zIdxName);
  pIndex->zShadow = sqlite3MPrintf(db, "%s_shadow", zIdxName);
  if( pIndex->zShadow == NULL ){
    diskAnnCloseIndex(pIndex);
    return SQLITE_NOMEM_BKPT;
  }
  nBlockSize = vectorIdxParamsGetU64(pParams, VECTOR_BLOCK_SIZE_PARAM_ID);
  // preserve backward compatibility: treat block size > 128 literally, but <= 128 with shift
  if( nBlockSize <= 128 ){
    nBlockSize <<= DISKANN_BLOCK_SIZE_SHIFT;
  }

  pIndex->nFormatVersion = vectorIdxParamsGetU64(pParams, VECTOR_FORMAT_PARAM_ID);
  pIndex->nDistanceFunc = vectorIdxParamsGetU64(pParams, VECTOR_METRIC_TYPE_PARAM_ID);
  pIndex->nBlockSize = nBlockSize;
  pIndex->nNodeVectorType = vectorIdxParamsGetU64(pParams, VECTOR_TYPE_PARAM_ID);
  pIndex->nVectorDims = vectorIdxParamsGetU64(pParams, VECTOR_DIM_PARAM_ID);
  pIndex->pruningAlpha = vectorIdxParamsGetF64(pParams, VECTOR_PRUNING_ALPHA_PARAM_ID);
  pIndex->insertL = vectorIdxParamsGetU64(pParams, VECTOR_INSERT_L_PARAM_ID);
  pIndex->searchL = vectorIdxParamsGetU64(pParams, VECTOR_SEARCH_L_PARAM_ID);
  pIndex->nReads = 0;
  pIndex->nWrites = 0;
  if( pIndex->nDistanceFunc == 0 ||
      pIndex->nBlockSize == 0 ||
      pIndex->nNodeVectorType == 0 ||
      pIndex->nVectorDims == 0
    ){
    diskAnnCloseIndex(pIndex);
    return SQLITE_ERROR;
  }
  if( pIndex->pruningAlpha == 0 ){
    pIndex->pruningAlpha = VECTOR_PRUNING_ALPHA_DEFAULT;
  }
  if( pIndex->insertL == 0 ){
    pIndex->insertL = VECTOR_INSERT_L_DEFAULT;
  }
  if( pIndex->searchL == 0 ){
    pIndex->searchL = VECTOR_SEARCH_L_DEFAULT;
  }
  pIndex->nNodeVectorSize = vectorDataSize(pIndex->nNodeVectorType, pIndex->nVectorDims);

  compressNeighbours = vectorIdxParamsGetU64(pParams, VECTOR_COMPRESS_NEIGHBORS_PARAM_ID);
  if( compressNeighbours == 0 ){
    pIndex->nEdgeVectorType = pIndex->nNodeVectorType;
    pIndex->nEdgeVectorSize = pIndex->nNodeVectorSize;
  }else{
    pIndex->nEdgeVectorType = compressNeighbours;
    pIndex->nEdgeVectorSize = vectorDataSize(compressNeighbours, pIndex->nVectorDims);
  }

  *ppIndex = pIndex;
  DiskAnnTrace(("opened index %s: max edges %d\n", zIdxName, nodeEdgesMaxCount(pIndex)));
  return SQLITE_OK;
}

void diskAnnCloseIndex(DiskAnnIndex *pIndex){
  if( pIndex->zDbSName ){
    sqlite3DbFree(pIndex->db, pIndex->zDbSName);
  }
  if( pIndex->zName ){
    sqlite3DbFree(pIndex->db, pIndex->zName);
  }
  if( pIndex->zShadow ){
    sqlite3DbFree(pIndex->db, pIndex->zShadow);
  }
  sqlite3DbFree(pIndex->db, pIndex);
}
#endif /* !defined(SQLITE_OMIT_VECTOR) */

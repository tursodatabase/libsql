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
**    diskAnnCreateIndex()     Create new index
**    diskAnnDeleteIndex()     Delete existing index
**    diskAnnOpenIndex()       Open index for operations (allocate all necessary internal structures)
**    diskAnnCloseIndex()      Close index and free associated resources
**    diskAnnSearch()          
**    diskAnnInsert()          
*/
#ifndef SQLITE_OMIT_VECTOR

#include "sqliteInt.h"
#include "vectorIndexInt.h"

#define SQLITE_VECTOR_TRACE
#if defined(SQLITE_DEBUG) && defined(SQLITE_VECTOR_TRACE)
#define DiskAnnTrace(X) sqlite3DebugPrintf X;
#else
#define DiskAnnTrace(X)
#endif


/*
 * Due to historical reasons parameter for index block size were stored as u16 value and divided by 512 (2^9)
 * So, we will make inverse transform before initializing index from stored parameters
*/
#define DISKANN_BLOCK_SIZE_SHIFT 9

#define VECTOR_NODE_METADATA_SIZE (sizeof(u64) + sizeof(u16))
#define VECTOR_EDGE_METADATA_SIZE (sizeof(u64) + sizeof(u64))

/**************************************************************************
** Serialization utilities
**************************************************************************/

static inline u16 readLE16(const unsigned char *p){
  return (u16)p[0] | (u16)p[1] << 8;
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
  rc = sqlite3_blob_open(pIndex->db, pIndex->zDb, pIndex->zShadow, "data", nRowid, isWritable, &pBlobSpot->pBlob);
  rc = blobSpotConvertRc(pIndex, rc);
  if( rc != SQLITE_OK ){
    goto out;
  }
  pBlobSpot->nRowid = nRowid;
  pBlobSpot->pBuffer = pBuffer;
  pBlobSpot->nBufferSize = nBufferSize;
  pBlobSpot->isWritable = isWritable;
  pBlobSpot->isInitialized = 0;

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

int blobSpotReload(const DiskAnnIndex *pIndex, BlobSpot *pBlobSpot, u64 nRowid, int nBufferSize) {
  int rc;

  DiskAnnTrace(("blob spot reload: rowid=%lld\n", nRowid));
  assert( pBlobSpot != NULL && pBlobSpot->pBlob != NULL );
  assert( pBlobSpot->nBufferSize == nBufferSize );

  if( pBlobSpot->nRowid == nRowid && pBlobSpot->isInitialized ){
    return SQLITE_OK;
  }

  if( pBlobSpot->nRowid != nRowid ){
    rc = sqlite3_blob_reopen(pBlobSpot->pBlob, nRowid);
    rc = blobSpotConvertRc(pIndex, rc);
    if( rc != SQLITE_OK ){
      return rc;
    }
    pBlobSpot->nRowid = nRowid;
    pBlobSpot->isInitialized = 0;
  }
  rc = sqlite3_blob_read(pBlobSpot->pBlob, pBlobSpot->pBuffer, nBufferSize, 0);
  if( rc != SQLITE_OK ){
    return rc;
  }
  pBlobSpot->isInitialized = 1;
  return SQLITE_OK;
}

int blobSpotFlush(BlobSpot *pBlobSpot) {
  return sqlite3_blob_write(pBlobSpot->pBlob, pBlobSpot->pBuffer, pBlobSpot->nBufferSize, 0);
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

int nodeEdgesMaxCount(const DiskAnnIndex *pIndex){
  unsigned int nMaxEdges = (pIndex->nBlockSize - pIndex->nNodeVectorSize - VECTOR_NODE_METADATA_SIZE) / (pIndex->nEdgeVectorSize + VECTOR_EDGE_METADATA_SIZE);
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

  vectorInitStatic(pVector, pIndex->nNodeVectorType, pBlobSpot->pBuffer + VECTOR_NODE_METADATA_SIZE, pIndex->nNodeVectorSize);
}

u16 nodeBinEdges(const DiskAnnIndex *pIndex, const BlobSpot *pBlobSpot) {
  assert( VECTOR_NODE_METADATA_SIZE <= pBlobSpot->nBufferSize );

  return readLE16(pBlobSpot->pBuffer + sizeof(u64));
}

void nodeBinEdge(const DiskAnnIndex *pIndex, const BlobSpot *pBlobSpot, int iEdge, u64 *pRowid, Vector *pVector) {
  int offset = nodeEdgesMetadataOffset(pIndex);

  if( pRowid != NULL ){
    assert( offset + (iEdge + 1) * VECTOR_EDGE_METADATA_SIZE <= pBlobSpot->nBufferSize );
    *pRowid = readLE64(pBlobSpot->pBuffer + offset + iEdge * VECTOR_EDGE_METADATA_SIZE + sizeof(u64));
  }
  if( pVector != NULL ){
    assert( VECTOR_NODE_METADATA_SIZE + pIndex->nNodeVectorSize + iEdge * pIndex->nEdgeVectorSize < offset );
    vectorInitStatic(
      pVector,
      pIndex->nEdgeVectorType,
      pBlobSpot->pBuffer + VECTOR_NODE_METADATA_SIZE + pIndex->nNodeVectorSize + iEdge * pIndex->nNodeVectorSize,
      pIndex->nEdgeVectorSize
    );
  }
}

int nodeBinEdgeFindIdx(const DiskAnnIndex *pIndex, const BlobSpot *pBlobSpot, u64 nRowid) {
  int i, nEdges = nodeBinEdges(pIndex, pBlobSpot);
  // todo: if edges will be sorted by identifiers we can use binary search here (although speed up will be visible only on pretty loaded nodes: >128 edges)
  for(i = 0; i < nEdges; i++){
    u64 edgeId;
    nodeBinEdge(pIndex, pBlobSpot, i, &edgeId, NULL);
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

void nodeBinInsertEdge(const DiskAnnIndex *pIndex, BlobSpot *pBlobSpot, int iInsert, u64 nRowid, Vector *pVector) {
  int nMaxEdges = nodeEdgesMaxCount(pIndex);
  int nEdges = nodeBinEdges(pIndex, pBlobSpot);
  int edgeVectorOffset, edgeMetaOffset, itemsToMove;

  assert( 0 <= iInsert && iInsert < nMaxEdges );
  assert( 0 <= iInsert && iInsert <= nEdges );

  if( nEdges < nMaxEdges ){
    nEdges++;
  }

  itemsToMove = nEdges - iInsert - 1;
  edgeVectorOffset = VECTOR_NODE_METADATA_SIZE + pIndex->nNodeVectorSize + iInsert * pIndex->nEdgeVectorSize;
  edgeMetaOffset = nodeEdgesMetadataOffset(pIndex) + iInsert * VECTOR_EDGE_METADATA_SIZE;

  assert( edgeVectorOffset + pIndex->nEdgeVectorSize * (itemsToMove + 1) <= pBlobSpot->nBufferSize );
  assert( edgeMetaOffset + VECTOR_EDGE_METADATA_SIZE * (itemsToMove + 1) <= pBlobSpot->nBufferSize );

  memmove(pBlobSpot->pBuffer + edgeVectorOffset + pIndex->nEdgeVectorSize, pBlobSpot->pBuffer + edgeVectorOffset, itemsToMove * pIndex->nEdgeVectorSize);
  memmove(pBlobSpot->pBuffer + edgeMetaOffset + VECTOR_EDGE_METADATA_SIZE, pBlobSpot->pBuffer + edgeMetaOffset, itemsToMove * VECTOR_EDGE_METADATA_SIZE);

  vectorSerializeToBlob(pVector, pBlobSpot->pBuffer + edgeVectorOffset, pIndex->nEdgeVectorSize);
  writeLE64(pBlobSpot->pBuffer + edgeMetaOffset + sizeof(u64), nRowid);

  writeLE16(pBlobSpot->pBuffer + sizeof(u64), nEdges);
}

void nodeBinDeleteEdge(const DiskAnnIndex *pIndex, BlobSpot *pBlobSpot, int iDelete) {
  int nEdges = nodeBinEdges(pIndex, pBlobSpot);
  int edgeVectorOffset, edgeMetaOffset, itemsToMove;

  assert( 0 <= iDelete && iDelete < nEdges );

  itemsToMove = nEdges - iDelete - 1;
  edgeVectorOffset = VECTOR_NODE_METADATA_SIZE + pIndex->nNodeVectorSize + iDelete * pIndex->nEdgeVectorSize;
  edgeMetaOffset = nodeEdgesMetadataOffset(pIndex) + iDelete * VECTOR_EDGE_METADATA_SIZE;

  assert( edgeVectorOffset + pIndex->nEdgeVectorSize * (itemsToMove + 1) <= pBlobSpot->nBufferSize );
  assert( edgeMetaOffset + VECTOR_EDGE_METADATA_SIZE * (itemsToMove + 1) <= pBlobSpot->nBufferSize );

  memmove(pBlobSpot->pBuffer + edgeVectorOffset, pBlobSpot->pBuffer + edgeVectorOffset + pIndex->nEdgeVectorSize, itemsToMove * pIndex->nEdgeVectorSize);
  memmove(pBlobSpot->pBuffer + edgeMetaOffset, pBlobSpot->pBuffer + edgeMetaOffset + VECTOR_EDGE_METADATA_SIZE, itemsToMove * VECTOR_EDGE_METADATA_SIZE);

  writeLE16(pBlobSpot->pBuffer + sizeof(u64), nEdges - 1);
}

void nodeBinDebug(const DiskAnnIndex *pIndex, const BlobSpot *pBlobSpot) {
#if defined(SQLITE_DEBUG) && defined(SQLITE_VECTOR_TRACE)
  int nEdges, nMaxEdges, i;
  u64 nRowid;
  Vector vector;

  nEdges = nodeBinEdges(pIndex, pBlobSpot);
  nMaxEdges = nodeEdgesMaxCount(pIndex);
  nodeBinVector(pIndex, pBlobSpot, &vector);

  DiskAnnTrace(("debug blob content for root=%lld (buffer size=%d)\n", pBlobSpot->nRowid, pBlobSpot->nBufferSize));
  DiskAnnTrace(("  nEdges=%d, nMaxEdges=%d, vector=", nEdges, nMaxEdges));
  vectorDump(&vector);
  for(i = 0; i < nEdges; i++){
    nodeBinEdge(pIndex, pBlobSpot, i, &nRowid, &vector);
    DiskAnnTrace(("  to=%d, vector=", i, nRowid));
    vectorDump(&vector);
  }
#endif
}
#endif /* !defined(SQLITE_OMIT_VECTOR) */

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
**    diskAnnOpenIndex()       Open a vector index file and return a DiskAnnIndex object.
**    diskAnnCloseIndex()      Close a DiskAnnIndex object.
**    diskAnnSearch()          Find the k-nearest neighbours of a vector.
**    diskAnnInsert()          Insert a vector to the index.
*/
#ifndef SQLITE_OMIT_VECTOR
#include "sqliteInt.h"

#include "vectorInt.h"

/* Objects */
typedef struct DiskAnnHeader DiskAnnHeader;
typedef struct SearchContext SearchContext;
typedef struct VectorMetadata VectorMetadata;
typedef struct VectorNode VectorNode;

/* TODO: Make this configurable. */
#define DISKANN_DEFAULT_ALPHA 1.2
#define DISKANN_DEFAULT_SEARCH_L 200
#define DISKANN_DEFAULT_INSERT_L 70

/**
** The block size in bytes.
**/
#define DISKANN_BLOCK_SIZE 65536

/**
** The bit shift to get the block size in bytes.
**/
#define DISKANN_BLOCK_SIZE_SHIFT 9

struct DiskAnnIndex {
  sqlite3 *db;                    /* Database connection */
  char *zDb;                      /* Database name */
  char *zName;                    /* Index name */
  char *zShadow;                  /* Shadow table name */
  int nDistanceFunc;              /* Distance function */
  unsigned short nBlockSize;      /* Block size */
  unsigned short nVectorType;     /* Vector type */
  unsigned short nVectorDims;     /* Vector dimensions */
};

struct VectorMetadata {
  u64 id;
  u64 offset;
};

struct VectorNode {
  sqlite3_blob *pBlob;
  u8 *pBuffer;
  Vector *vec;
  u64 id;
  u64 offset;
  int refCnt;
  int visited;                    /* Is this node visited? */
  VectorNode *pNext;              /* Next node in the visited list */
};

/**************************************************************************
** Utility routines for managing vector nodes
**************************************************************************/

static inline u16 diskAnnReadLE16(const unsigned char *p){
  return (u16)p[0] | (u16)p[1] << 8;
}

static inline u64 diskAnnReadLE64(const unsigned char *p){
  return (u64)p[0]
       | (u64)p[1] << 8
       | (u64)p[2] << 16
       | (u64)p[3] << 24
       | (u64)p[4] << 32
       | (u64)p[5] << 40
       | (u64)p[6] << 48
       | (u64)p[7] << 56;
}

static inline unsigned int diskAnnWriteLE16(unsigned char *p, u16 v){
  p[0] = v;
  p[1] = v >> 8;
  return 2;
}

static inline unsigned int diskAnnWriteLE64(unsigned char *p, u64 v){
  p[0] = v;
  p[1] = v >> 8;
  p[2] = v >> 16;
  p[3] = v >> 24;
  p[4] = v >> 32;
  p[5] = v >> 40;
  p[6] = v >> 48;
  p[7] = v >> 56;
  return 8;
}

static VectorNode *vectorNodeNew(DiskAnnIndex *pIndex, u64 nBlockRowid, u64 id, u8 *pBuffer){
  VectorNode *pNode;
  int rc;

  pNode = sqlite3_malloc(sizeof(VectorNode));
  if( !pNode ) {
    return NULL;
  }
  rc = sqlite3_blob_open(pIndex->db, pIndex->zDb, pIndex->zShadow, "data", nBlockRowid, 1, &pNode->pBlob);
  if( rc!=SQLITE_OK ){
    sqlite3_free(pNode);
    return NULL;
  }
  pNode->pBuffer = pBuffer;
  pNode->vec = NULL;
  pNode->id = id;
  pNode->offset = nBlockRowid;
  pNode->refCnt = 1;
  pNode->visited = 0;
  pNode->pNext = NULL;
  return pNode;
}

static void vectorNodeFree(VectorNode *pNode){
  sqlite3_blob_close(pNode->pBlob);
  vectorFree(pNode->vec);
  sqlite3_free(pNode->pBuffer);
  sqlite3_free(pNode);
}

static VectorNode* vectorNodeGet(VectorNode *pNode){
  pNode->refCnt++;
  return pNode;
}

static void vectorNodePut(VectorNode *pNode){
  assert( pNode->refCnt>0 );
  pNode->refCnt--;
  if( pNode->refCnt==0 ){
    vectorNodeFree(pNode);
  }
}

/**************************************************************************
** Utility routines for parsing the index file
**************************************************************************/

static float diskannVectorDistance(DiskAnnIndex *pIndex, Vector *pVec1, Vector *pVec2){
  switch( pIndex->nDistanceFunc ){
    case VECTOR_DISTANCE_COS:
      return vectorDistanceCos(pVec1, pVec2);
    default:
      assert(0);
      break;
  }
  return 0.0;
}

#define VECTOR_METADATA_SIZE    (sizeof(u64) + sizeof(u16))
#define NEIGHBOUR_METADATA_SIZE (sizeof(u64) + sizeof(u64))

static unsigned int blockSize(DiskAnnIndex *pIndex){
  return pIndex->nBlockSize << DISKANN_BLOCK_SIZE_SHIFT;
}

static unsigned int vectorSize(DiskAnnIndex *pIndex){
  return vectorDataSize(pIndex->nVectorType, pIndex->nVectorDims);
}

static int diskAnnMaxNeighbours(DiskAnnIndex *pIndex){
  unsigned int nNeighbourVectorSize;
  unsigned int maxNeighbours;
  unsigned int nVectorSize;
  unsigned int nBlockSize;
  nBlockSize = blockSize(pIndex);
  nVectorSize = vectorSize(pIndex);
  nNeighbourVectorSize = vectorSize(pIndex);
  maxNeighbours = (nBlockSize - nVectorSize - VECTOR_METADATA_SIZE) / (nNeighbourVectorSize + NEIGHBOUR_METADATA_SIZE);
  assert( maxNeighbours > 0);
  return maxNeighbours;

}

static int neighbourMetadataOffset(DiskAnnIndex *pIndex){
  unsigned int nNeighbourVectorSize;
  unsigned int maxNeighbours;
  unsigned int nVectorSize;
  unsigned int nBlockSize;
  nBlockSize = blockSize(pIndex);
  nVectorSize = vectorSize(pIndex);
  nNeighbourVectorSize = vectorSize(pIndex);
  maxNeighbours = (nBlockSize - nVectorSize - VECTOR_METADATA_SIZE) / (nNeighbourVectorSize + NEIGHBOUR_METADATA_SIZE);
  assert( maxNeighbours > 0);
  return nVectorSize + VECTOR_METADATA_SIZE + maxNeighbours * (nNeighbourVectorSize); 
}

static VectorNode *diskAnnReadVector(
  DiskAnnIndex *pIndex,
  u64 nRowid
){
  VectorNode *pNode;
  u8 *pBuffer;
  int off = 0;
  u64 id;
  int rc;

  pBuffer = sqlite3_malloc(DISKANN_BLOCK_SIZE);
  if( pBuffer==NULL ){
    return NULL;
  }
  pNode = vectorNodeNew(pIndex, nRowid, id, pBuffer);
  if( pNode==NULL ){
    sqlite3_free(pBuffer);
    return NULL;
  }
  rc = sqlite3_blob_read(pNode->pBlob, pNode->pBuffer, DISKANN_BLOCK_SIZE, 0);
  if( rc!=SQLITE_OK ){
    vectorNodePut(pNode);
    return NULL;
  }
  id = diskAnnReadLE64(pBuffer + off);
  off += sizeof(u64);
  off += sizeof(u16);
  pNode->id = id;
  pNode->offset = nRowid;
  pNode->vec = vectorAlloc(pIndex->nVectorType, pIndex->nVectorDims);
  if( pNode->vec==NULL ){
    vectorNodePut(pNode);
    return NULL;
  }
  off += vectorDeserializeFromBlob(pNode->vec, pBuffer+off, vectorSize(pIndex));
  return pNode;
}

static void diskAnnInitVectorNode(
  DiskAnnIndex *pIndex,
  VectorNode *pNode,
  u64 id,
  u64 offset
){
  int rc = SQLITE_OK;
  int off = 0;
  memset(pNode->pBuffer, 0, DISKANN_BLOCK_SIZE);
  /* ID */
  diskAnnWriteLE64(pNode->pBuffer+off, id);
  off += sizeof(u64);
  /* nNeighbours */
  diskAnnWriteLE16(pNode->pBuffer+off, 0);
  off += sizeof(u16);
  off += vectorSerializeToBlob(pNode->vec, pNode->pBuffer+off, vectorSize(pIndex));
}

/**
** Flushes in-memory vector to disk.
*/
static int diskAnnFlushVector(DiskAnnIndex *pIndex, VectorNode *pNode){
  return sqlite3_blob_write(pNode->pBlob, pNode->pBuffer, DISKANN_BLOCK_SIZE, 0);
}

static int diskAnnNeighbourCount(VectorNode *pNode){
  return diskAnnReadLE16(pNode->pBuffer+sizeof(u64));
}

static int diskAnnNeighbourMetadata(DiskAnnIndex *pIndex, VectorNode *pNode, size_t idx, VectorMetadata *pMetadata){
  if( idx >= diskAnnNeighbourCount(pNode) ){
    return -1;
  }
  int off = neighbourMetadataOffset(pIndex) + idx * NEIGHBOUR_METADATA_SIZE;
  pMetadata->id = diskAnnReadLE64(pNode->pBuffer+off);
  pMetadata->offset = diskAnnReadLE64(pNode->pBuffer+off+sizeof(u64));
  return 0;
}

/**
** Updates on-disk vector with a new neighbour, pruning the neighbour list if needed.
**/
static int diskAnnInsertNeighbour(
  DiskAnnIndex *pIndex,
  VectorNode *pVec,
  VectorNode *pNodeToAdd,
  Vector *pVecToAdd
) {
  unsigned int maxNeighbours = diskAnnMaxNeighbours(pIndex);
  u16 nNeighbours;
  int off;
  int rc;
  if( pVec->offset==0 ){
    return -1;
  }
  nNeighbours = (u16) pVec->pBuffer[8] | (u16) pVec->pBuffer[9] << 8;
  off = sizeof(u64) + sizeof(u16) + vectorSize(pIndex);
  int insertIdx = -1;
  double toAddDist = diskannVectorDistance(pIndex, pVecToAdd, pVec->vec);
  for( int i = 0; i < nNeighbours; i++ ){
    Vector neighbour;
    vectorInitStatic(&neighbour, pIndex->nVectorType, pVec->pBuffer+off, vectorSize(pIndex));
    float dist = diskannVectorDistance(pIndex, &neighbour, pVec->vec);
    if( toAddDist < dist ){
      insertIdx = i;
      break;
    }
    off += vectorSize(pIndex);
  }
  if( nNeighbours<maxNeighbours ){
    if( insertIdx==-1 ){
      insertIdx = nNeighbours;
    }
    nNeighbours++;
  } else {
    /* If the node to insert is to be pruned, just bail out. */
    if( insertIdx==-1 ){
      return SQLITE_OK;
    }
  }
  /* Calculate how many neighbours need to move. */
  int nToMove = nNeighbours-insertIdx-1;

  /* Move the neighbours to the right to make room. */
  off = sizeof(u64) + sizeof(u16) + vectorSize(pIndex) + insertIdx * vectorSize(pIndex);
  memmove(pVec->pBuffer+off+vectorSize(pIndex), pVec->pBuffer+off, nToMove * vectorSize(pIndex));

  /* Insert new neighbour to the list. */
  off = sizeof(u64) + sizeof(u16) + vectorSize(pIndex) + insertIdx * vectorSize(pIndex);
  vectorSerializeToBlob(pVecToAdd, pVec->pBuffer+off, vectorSize(pIndex));

  off = neighbourMetadataOffset(pIndex) + insertIdx * NEIGHBOUR_METADATA_SIZE;

  /* Move the metadata to right to make room. */
  memmove(pVec->pBuffer+off+NEIGHBOUR_METADATA_SIZE, pVec->pBuffer+off, nToMove * NEIGHBOUR_METADATA_SIZE);

  /* Insert new metadata to the list */
  pVec->pBuffer[off++] = pNodeToAdd->id;
  pVec->pBuffer[off++] = pNodeToAdd->id >> 8;
  pVec->pBuffer[off++] = pNodeToAdd->id >> 16;
  pVec->pBuffer[off++] = pNodeToAdd->id >> 24;
  pVec->pBuffer[off++] = pNodeToAdd->id >> 32;
  pVec->pBuffer[off++] = pNodeToAdd->id >> 40;
  pVec->pBuffer[off++] = pNodeToAdd->id >> 48;
  pVec->pBuffer[off++] = pNodeToAdd->id >> 56;
  pVec->pBuffer[off++] = pNodeToAdd->offset;
  pVec->pBuffer[off++] = pNodeToAdd->offset >> 8;
  pVec->pBuffer[off++] = pNodeToAdd->offset >> 16;
  pVec->pBuffer[off++] = pNodeToAdd->offset >> 24;
  pVec->pBuffer[off++] = pNodeToAdd->offset >> 32;
  pVec->pBuffer[off++] = pNodeToAdd->offset >> 40;
  pVec->pBuffer[off++] = pNodeToAdd->offset >> 48;

  off = sizeof(u64) + sizeof(u16) + vectorSize(pIndex) + insertIdx * vectorSize(pIndex);
  for( int i = insertIdx; i < nNeighbours-1; i++ ){
    Vector prev, curr;
    vectorInitStatic(&prev, pIndex->nVectorType, pVec->pBuffer+off, vectorSize(pIndex));
    vectorInitStatic(&curr, pIndex->nVectorType, pVec->pBuffer+off+vectorSize(pIndex), vectorSize(pIndex));
    float prevDist = diskannVectorDistance(pIndex, &prev, pVec->vec);
    float currDist = diskannVectorDistance(pIndex, &curr, pVec->vec);
    if( prevDist * DISKANN_DEFAULT_ALPHA < currDist ){
      // Prune remaining neighbours because they're too far away.
      nNeighbours = i + 1;
      break;
    }
    off += vectorSize(pIndex);
  }
  // Every node needs at least one neighbour node so that the graph is connected.
  assert( nNeighbours > 0 );
  assert( nNeighbours <= maxNeighbours );
  pVec->pBuffer[8] = nNeighbours;
  pVec->pBuffer[9] = nNeighbours >> 8;

  return SQLITE_OK;
}

/**************************************************************************
** DiskANN search
**************************************************************************/

struct SearchContext {
  Vector *pQuery;
  VectorNode **aCandidates;
  double *aDistances;         /* Candidate distances to the query vector */
  unsigned int nCandidates;
  unsigned int maxCandidates;
  VectorNode *visitedList;
  unsigned int nUnvisited;
  int k;
};

static void initSearchContext(SearchContext *pCtx, Vector* pQuery, unsigned int maxCandidates){
  pCtx->pQuery = pQuery;
  pCtx->aDistances = sqlite3_malloc(maxCandidates * sizeof(double));
  pCtx->aCandidates = sqlite3_malloc(maxCandidates * sizeof(VectorNode));
  pCtx->nCandidates = 0;
  pCtx->maxCandidates = maxCandidates;
  pCtx->visitedList = NULL;
  pCtx->nUnvisited = 0;
}

static void deinitSearchContext(SearchContext *pCtx){
  VectorNode *pNode, *pNext;

  pNode = pCtx->visitedList;
  while( pNode!=NULL ){
    pNext = pNode->pNext;
    vectorNodePut(pNode);
    pNode = pNext;
  }
  sqlite3_free(pCtx->aCandidates);
  sqlite3_free(pCtx->aDistances);
}

static int isVisited(SearchContext *pCtx, u64 id){
  for( VectorNode *pNode = pCtx->visitedList; pNode!=NULL; pNode = pNode->pNext ){
    if( pNode->id==id ){
      return 1;
    }
  }
  return 0;
}

/**
** Add a candidate to the candidate set, replacing an existing candidate if needed.
*/
static void addCandidate(DiskAnnIndex *pIndex, SearchContext *pCtx, VectorNode *pNode){
  // TODO: replace the check with a better data structure
  for( int i = 0; i < pCtx->nCandidates; i++ ){
    if( pCtx->aCandidates[i]->id==pNode->id ){
      return;
    }
  }
  float toInsertDist = diskannVectorDistance(pIndex, pCtx->pQuery, pNode->vec);
  // Special-case insertion to empty candidate set to avoid the distance calculation.
  if( pCtx->nCandidates==0 ){
    pCtx->aCandidates[pCtx->nCandidates] = vectorNodeGet(pNode);
    pCtx->aDistances[pCtx->nCandidates] = toInsertDist;
    pCtx->nCandidates++;
    pCtx->nUnvisited++;
    return;
  }
  int insertIdx = -1;
  // Find the index of the candidate that is further away from the query
  // vector than the one we're inserting.
  for( int i = 0; i < pCtx->nCandidates; i++ ){
    float distCandidate = pCtx->aDistances[i];
    if( toInsertDist < distCandidate ){
      insertIdx = i;
      break;
    }
  }
  // If there is space for the new candidate, insert it; otherwise replace an
  // existing one.
  if( pCtx->nCandidates < pCtx->maxCandidates ){
    if( insertIdx==-1 ){
      insertIdx = pCtx->nCandidates;
    }
    pCtx->nCandidates++;
  } else {
    if( insertIdx==-1 ){
      return;
    }
    VectorNode *toDrop = pCtx->aCandidates[pCtx->nCandidates-1];
    if( !toDrop->visited ){
      pCtx->nUnvisited--;
      vectorNodePut(toDrop);
    }
  }
  // Shift the candidates to the right to make space for the new one.
  for( int i = pCtx->nCandidates-1; i > insertIdx; i-- ){
    pCtx->aCandidates[i] = pCtx->aCandidates[i-1];
    pCtx->aDistances[i] = pCtx->aDistances[i-1];
  }
  // Insert the new candidate.
  pCtx->aCandidates[insertIdx] = vectorNodeGet(pNode);
  pCtx->aDistances[insertIdx] = toInsertDist;
  pCtx->nUnvisited++;
}

/**
** Find the closest unvisited candidate to the query vector. 
*/
static VectorNode* findClosestCandidate(SearchContext *pCtx){
  VectorNode *pClosestCandidate = NULL;
  int closestIdx = -1;
  for (int i = 0; i < pCtx->nCandidates; i++) {
    VectorNode *pNewCandidate = pCtx->aCandidates[i];
    if( !pNewCandidate->visited ){
      if( pClosestCandidate==NULL ){
        pClosestCandidate = pNewCandidate;
        closestIdx = i;
        continue;
      }
      float closestDist = pCtx->aDistances[closestIdx];
      float newDist = pCtx->aDistances[i];
      if( newDist < closestDist ){
        pClosestCandidate = pNewCandidate;
        break;
      }
    }
  }
  return pClosestCandidate;
}

static void markAsVisited(SearchContext *pCtx, VectorNode *pNode){
  pNode->visited = 1;
  assert(pCtx->nUnvisited > 0);
  pCtx->nUnvisited--;
  pNode->pNext = pCtx->visitedList;
  pCtx->visitedList = pNode;
}

static int hasUnvisitedCandidates(SearchContext *pCtx){
  return pCtx->nUnvisited > 0;
}

static int diskAnnSelectRandom(DiskAnnIndex *pIndex, u64 *pRowid){
  sqlite3_stmt *pStmt;
  char *zSql;
  int rc;

  zSql = sqlite3MPrintf(pIndex->db, "SELECT rowid FROM %s_shadow ORDER BY RANDOM() LIMIT 1", pIndex->zName);
  if( zSql==NULL ){
    return SQLITE_ERROR;
  }
  rc = sqlite3_prepare_v2(pIndex->db, zSql, -1, &pStmt, 0);
  if( rc!=SQLITE_OK ){
    goto out_free;
  }
  if( sqlite3_step(pStmt)!=SQLITE_ROW ){
    rc = SQLITE_ERROR;
    goto out;
  }
  *pRowid = sqlite3_column_int64(pStmt, 0);
  rc = SQLITE_OK;
out:
  sqlite3_finalize(pStmt);
out_free:
  sqlite3DbFree(pIndex->db, zSql);
  return rc;
}

static int diskAnnSearchInternal(
  DiskAnnIndex *pIndex,
  SearchContext *pCtx,
  u64 nEntryRowid
){
  VectorNode *start;

  start = diskAnnReadVector(pIndex, nEntryRowid);
  if( start==NULL ){
    return 0;
  }
  addCandidate(pIndex, pCtx, start);
  while( hasUnvisitedCandidates(pCtx) ){
    VectorNode *pCandidate = findClosestCandidate(pCtx);
    assert( pCandidate!=NULL );
    markAsVisited(pCtx, pCandidate);
    for( int i = 0; i < diskAnnNeighbourCount(pCandidate); i++ ){
      VectorMetadata neighbourMetadata;
      if( diskAnnNeighbourMetadata(pIndex, pCandidate, i, &neighbourMetadata) < 0 ){
        continue;
      }
      if( isVisited(pCtx, neighbourMetadata.id) ){
        continue;
      }
      VectorNode *pNeighbour = diskAnnReadVector(pIndex, neighbourMetadata.offset);
      if( pNeighbour==NULL ){
        continue;
      }
      addCandidate(pIndex, pCtx, pNeighbour);
      vectorNodePut(pNeighbour);
    }
  }
  vectorNodePut(start);
  return 0;
}

int diskAnnSearch(
  DiskAnnIndex *pIndex,
  Vector *pVec,
  unsigned int k,
  i64 *aIds
){
  SearchContext ctx;
  u64 nEntryRowid;
  int nIds = 0;
  int rc;

  if (diskAnnSelectRandom(pIndex, &nEntryRowid) != SQLITE_OK) {
    return 0;
  }
  initSearchContext(&ctx, pVec, DISKANN_DEFAULT_SEARCH_L);
  rc = diskAnnSearchInternal(pIndex, &ctx, nEntryRowid);
  if( rc==0 ){
    for( int i = 0; i < ctx.nCandidates; i++ ){
      if( i < k ){
        aIds[nIds++] = ctx.aCandidates[i]->id;
      }
    }
  }
  deinitSearchContext(&ctx);
  return nIds;
}

/**************************************************************************
** DiskANN insertion
**************************************************************************/

static int diskAnnInsertShadowRow(DiskAnnIndex *pIndex, i64 id, u64 *pRowid){
  sqlite3_stmt *pStmt;
  char *zSql;
  u64 rowid;
  int rc;
  zSql = sqlite3MPrintf(pIndex->db, "INSERT INTO %s_shadow VALUES (?, ?) RETURNING rowid", pIndex->zName);
  if( zSql==NULL ){
    return SQLITE_NOMEM;
  }
  rc = sqlite3_prepare_v2(pIndex->db, zSql, -1, &pStmt, 0);
  if( rc!=SQLITE_OK ){
    goto out_free;
  }
  rc = sqlite3_bind_int64(pStmt, 1, id);
  if( rc!=SQLITE_OK ){
    goto out;
  }
  rc = sqlite3_bind_zeroblob(pStmt, 2, DISKANN_BLOCK_SIZE);
  if( rc!=SQLITE_OK ){
    goto out;
  }
  rc = sqlite3_step(pStmt);
  if( rc!=SQLITE_ROW ){
    rc = SQLITE_ERROR;
    goto out;
  }
  *pRowid = sqlite3_column_int64(pStmt, 0);
  rc = SQLITE_OK;
out:
  sqlite3_finalize(pStmt);
out_free:
  sqlite3DbFree(pIndex->db, zSql);
  return rc;
}

int diskAnnInsert(
  DiskAnnIndex *pIndex,
  Vector *pVec,
  i64 id
){
  unsigned int nWritten;
  VectorNode *pNode;
  SearchContext ctx;
  u64 nBlockRowid;
  u64 nEntryRowid;
  int first = 0;
  u8 *pBuffer;
  int rc;

  if (pVec->dims != pIndex->nVectorDims) {
    return SQLITE_ERROR;
  }
  if (diskAnnSelectRandom(pIndex, &nEntryRowid) != SQLITE_OK) {
    first = 1;
  }
  rc = diskAnnInsertShadowRow(pIndex, id, &nBlockRowid);
  if( rc!=SQLITE_OK ){
    return rc;
  }
  pBuffer = sqlite3_malloc(DISKANN_BLOCK_SIZE);
  if( !pBuffer ){
    return SQLITE_NOMEM;
  }
  pNode = vectorNodeNew(pIndex, nBlockRowid, id, pBuffer);
  if( pNode==NULL ){
    sqlite3_free(pBuffer);
    return SQLITE_NOMEM;
  }
  pNode->vec = pVec;
  diskAnnInitVectorNode(pIndex, pNode, id, pNode->offset);
  diskAnnFlushVector(pIndex, pNode);
  if( first ){
    goto out_free_node;
  }
  initSearchContext(&ctx, pVec, DISKANN_DEFAULT_INSERT_L);
  diskAnnSearchInternal(pIndex, &ctx, nEntryRowid);
  for( VectorNode *pVisited = ctx.visitedList; pVisited!=NULL; pVisited = pVisited->pNext ){
    diskAnnInsertNeighbour(pIndex, pNode, pVisited, pVisited->vec);
  }
  for( VectorNode* pVisited = ctx.visitedList; pVisited!=NULL; pVisited = pVisited->pNext ){
    diskAnnInsertNeighbour(pIndex, pVisited, pNode, pVec);
    diskAnnFlushVector(pIndex, pVisited);
  }
  nWritten = diskAnnFlushVector(pIndex, pNode);

  pNode->vec = NULL; /* HACK ALERT */
  deinitSearchContext(&ctx);

  if( nWritten<0 ){
    rc = SQLITE_ERROR;
    goto out_free_node;
  }

out_free_node:
  vectorNodePut(pNode);
  return rc;
}

/**************************************************************************
** DiskANN deletion
**************************************************************************/

static int diskAnnDeleteShadowRow(DiskAnnIndex *pIndex, i64 id){
  sqlite3_stmt *pStmt;
  char *zSql;
  int rc;
  zSql = sqlite3MPrintf(pIndex->db, "DELETE FROM %s_shadow WHERE index_key = ?", pIndex->zName);
  if( zSql==NULL ){
    return SQLITE_NOMEM;
  }
  rc = sqlite3_prepare_v2(pIndex->db, zSql, -1, &pStmt, 0);
  if( rc!=SQLITE_OK ){
    goto out_free;
  }
  rc = sqlite3_bind_int64(pStmt, 1, id);
  if( rc!=SQLITE_OK ){
    goto out;
  }
  rc = sqlite3_step(pStmt);
  if( rc!=SQLITE_DONE ){
    rc = SQLITE_ERROR;
    goto out;
  }
  rc = SQLITE_OK;
out:
  sqlite3_finalize(pStmt);
out_free:
  sqlite3DbFree(pIndex->db, zSql);
  return rc;
}

/**
** Updates on-disk vector deleting a neighbour, pruning the neighbour list if needed.
**/
static int diskAnnDeleteNeighbour(
  DiskAnnIndex *pIndex,
  VectorNode *pVec,
  i64 id
){
  unsigned int maxNeighbours = diskAnnMaxNeighbours(pIndex);
  u16 nNeighbours;
  int off;
  nNeighbours = (u16) pVec->pBuffer[8] | (u16) pVec->pBuffer[9] << 8;
  off = sizeof(u64) + sizeof(u16) + vectorSize(pIndex);
  int deleteIdx = -1;
  for( int i = 0; i < nNeighbours; i++ ){
    VectorMetadata neighbourMetadata;
    if( diskAnnNeighbourMetadata(pIndex, pVec, i, &neighbourMetadata) < 0 ){
      continue;
    }
    if( neighbourMetadata.id==id ){
      deleteIdx = i;
      break;
    }
  }
  if( deleteIdx==-1 ){
    return SQLITE_OK;
  }
  /* Calculate how many neighbours need to move. */
  int nToMove = nNeighbours-deleteIdx-1;
  /* Move the neighbours to the left to delete the neighbour. */
  off = sizeof(u64) + sizeof(u16) + vectorSize(pIndex) + deleteIdx * vectorSize(pIndex);
  memmove(pVec->pBuffer+off, pVec->pBuffer+off+vectorSize(pIndex), nToMove * vectorSize(pIndex));
  off = neighbourMetadataOffset(pIndex) + deleteIdx * NEIGHBOUR_METADATA_SIZE;
  /* Move the metadata to left to delete the neighbour. */
  memmove(pVec->pBuffer+off+NEIGHBOUR_METADATA_SIZE, pVec->pBuffer+off, nToMove * NEIGHBOUR_METADATA_SIZE);
  nNeighbours--;
  assert( nNeighbours <= maxNeighbours );
  pVec->pBuffer[8] = nNeighbours;
  pVec->pBuffer[9] = nNeighbours >> 8;
  return SQLITE_OK;
}

int diskAnnDelete(
  DiskAnnIndex *pIndex,
  i64 id
){
  VectorNode *pNode;
  pNode = diskAnnReadVector(pIndex, id);
  if( pNode==NULL ){
    return SQLITE_OK;
  }
  for( int i = 0; i < diskAnnNeighbourCount(pNode); i++ ){
    VectorMetadata neighbourMetadata;
    if( diskAnnNeighbourMetadata(pIndex, pNode, i, &neighbourMetadata) < 0 ){
      continue;
    }
    VectorNode *pNeighbour = diskAnnReadVector(pIndex, neighbourMetadata.offset);
    if( pNeighbour==NULL ){
      continue;
    }
    diskAnnDeleteNeighbour(pIndex, pNeighbour, id);
    diskAnnFlushVector(pIndex, pNeighbour);
    vectorNodePut(pNeighbour);
  }
  vectorNodePut(pNode);
  return diskAnnDeleteShadowRow(pIndex, id);
}

/**************************************************************************
** DiskANN index management
**************************************************************************/

/*
** Create internal tables.
*/
static int vectorInternalTableInit(sqlite3 *db){
  static const char *zSql =
    "CREATE TABLE IF NOT EXISTS libsql_vector_index ("
    "type TEXT, "
    "name TEXT, "
    "vector_type TEXT, "
    "block_size INTEGER, "
    "dims INTEGER, "
    "distance_ops TEXT"
    ");";
  return sqlite3_exec(db, zSql, 0, 0, 0);
}

static const char *diskAnnToVectorType(int nVectorType){
  switch( nVectorType ){
    case VECTOR_TYPE_FLOAT32:
      return "float32";
    case VECTOR_TYPE_FLOAT64:
      return "float64";
    default:
      return NULL;
  }
}

static int diskAnnFromVectorType(const char *zVectorType){
  if( strcmp(zVectorType, "float32")==0 ){
    return VECTOR_TYPE_FLOAT32;
  } else if( strcmp(zVectorType, "float64")==0 ){
    return VECTOR_TYPE_FLOAT64;
  }
  return -1;
}

static const char *diskAnnToDistanceOps(int nDistanceFunc){
  switch( nDistanceFunc ){
    case VECTOR_DISTANCE_COS:
      return "cosine";
    default:
      return NULL;
  }
}

static int diskannFromDistanceOps(const char *zDistanceOps){
  if( strcmp(zDistanceOps, "cosine")==0 ){
    return VECTOR_DISTANCE_COS;
  }
  return -1;
}

int diskAnnCreateIndex(
  sqlite3 *db,
  const char *zIdxName,
  unsigned int nDims,
  unsigned int nDistanceFunc
){
  const char *zDistanceOps;
  DiskAnnIndex *pIndex;
  sqlite3_stmt *pStmt;
  int rc;

  rc = vectorInternalTableInit(db);
  if( rc!=SQLITE_OK ){
    return rc;
  }
  zDistanceOps = diskAnnToDistanceOps(nDistanceFunc);
  if( zDistanceOps==NULL ){
    return SQLITE_ERROR;
  }
  pIndex = sqlite3_malloc(sizeof(DiskAnnIndex));
  if( pIndex == NULL ){
    return SQLITE_NOMEM;
  }
  pIndex->db = db;
  pIndex->zDb = strdup(db->aDb[0].zDbSName);
  pIndex->zName = strdup(zIdxName);
  pIndex->zShadow = sqlite3MPrintf(db, "%s_shadow", zIdxName);
  pIndex->nDistanceFunc = nDistanceFunc;
  pIndex->nBlockSize = DISKANN_BLOCK_SIZE >> DISKANN_BLOCK_SIZE_SHIFT;
  pIndex->nVectorType = VECTOR_TYPE_FLOAT32;
  pIndex->nVectorDims = nDims;

  static const char zInsertSql[] =
    "INSERT INTO libsql_vector_index "
    "(type, name, vector_type, block_size, dims, distance_ops)"
    "VALUES "
    "(?, ?, ?, ?, ?, ?)";
    
  rc = sqlite3_prepare_v2(db, zInsertSql, -1, &pStmt, 0);
  if( rc!=SQLITE_OK ){
    return rc;
  }
  sqlite3_bind_text(pStmt, 1, "diskann", -1, SQLITE_STATIC);
  sqlite3_bind_text(pStmt, 2, zIdxName, -1, SQLITE_STATIC);
  sqlite3_bind_text(pStmt, 3, diskAnnToVectorType(pIndex->nVectorType), -1, SQLITE_STATIC);
  sqlite3_bind_int (pStmt, 4, DISKANN_BLOCK_SIZE >> DISKANN_BLOCK_SIZE_SHIFT);
  sqlite3_bind_int (pStmt, 5, nDims);
  sqlite3_bind_text(pStmt, 6, diskAnnToDistanceOps(pIndex->nDistanceFunc), -1, SQLITE_STATIC);
  rc = sqlite3_step(pStmt);
  sqlite3_finalize(pStmt);
  diskAnnCloseIndex(pIndex);
  return rc;
}

int diskAnnOpenIndex(
  sqlite3 *db,                    /* Database connection */
  const char *zIdxName,           /* Index name */
  DiskAnnIndex **ppIndex          /* OUT: Index */
){
  DiskAnnIndex *pIndex;
  sqlite3_stmt *pStmt;
  int rc = SQLITE_OK;

  static const char zInsertSql[] =
    "SELECT vector_type, block_size, dims, distance_ops "
    "FROM libsql_vector_index "
    "WHERE type = ? AND name = ?";
  rc = sqlite3_prepare_v2(db, zInsertSql, -1, &pStmt, 0);
  if( rc!=SQLITE_OK ){
    return rc;
  }
  sqlite3_bind_text(pStmt, 1, "diskann", -1, SQLITE_STATIC);
  sqlite3_bind_text(pStmt, 2, zIdxName, -1, SQLITE_STATIC);
  if( sqlite3_step(pStmt)!=SQLITE_ROW ){
    rc = SQLITE_ERROR;
    goto out_finalize_stmt;
  }
  pIndex = sqlite3_malloc(sizeof(DiskAnnIndex));
  if( pIndex == NULL ){
    rc = SQLITE_NOMEM;
    goto out_finalize_stmt;
  }
  pIndex->db = db;
  pIndex->zDb = strdup(db->aDb[0].zDbSName);
  pIndex->zName = strdup(zIdxName);
  pIndex->zShadow = sqlite3MPrintf(db, "%s_shadow", zIdxName);
  pIndex->nVectorType = diskAnnFromVectorType(sqlite3_column_text(pStmt, 0));
  pIndex->nBlockSize = sqlite3_column_int(pStmt, 1);
  pIndex->nVectorDims = sqlite3_column_int(pStmt, 2);
  pIndex->nDistanceFunc = diskannFromDistanceOps(sqlite3_column_text(pStmt, 3));

  *ppIndex = pIndex;

out_finalize_stmt:
  sqlite3_finalize(pStmt);
  return rc;
}

void diskAnnCloseIndex(DiskAnnIndex *pIndex){
  free(pIndex->zDb);
  free(pIndex->zName);
  sqlite3DbFree(pIndex->db, pIndex->zShadow);
  sqlite3_free(pIndex);
}
#endif /* !defined(SQLITE_OMIT_VECTOR) */

/*
 * BUILD: cc test_libsql_diskann.c -I ../ -L ../.libs -llibsql -o test_libsql_diskann
 * RUN:   LD_LIBRARY_PATH=../.libs ./test_libsql_diskann
*/

#include "assert.h"
#include "stdbool.h"
#include "stdarg.h"
#include "stdio.h"
#include "stdlib.h"
#include "stddef.h"
#include "vectorIndexInt.h"
#include "vectorInt.h"
#include "vdbeInt.h"

#define eprintf(...) fprintf(stderr, __VA_ARGS__)
#define ensure(condition, ...) { if (!(condition)) { eprintf(__VA_ARGS__); exit(1); } }

#define TEST_BLOCK_SIZE 74
#define PAYLOAD1_74 "0102030400000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000"
#define PAYLOAD2_74 "0506070800000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000"

int main() {
  sqlite3 *db;
  ensure(sqlite3_open(":memory:", &db) == 0, "unable to open in memory db: %s\n", sqlite3_errmsg(db));
  ensure(sqlite3_exec(db, "CREATE TABLE t_idx_shadow (index_key INTEGER, data BLOB, PRIMARY KEY (index_key))", 0, 0, 0) == 0, "unable to create table: %s\n", sqlite3_errmsg(db));
  DiskAnnIndex index = {
    .db = db,
    .zDb = db->aDb[0].zDbSName,
    .zName = "t_idx",
    .zShadow = "t_idx_shadow",
    .nFormatVersion = 1,
    .nDistanceFunc = 0,
    .nBlockSize = TEST_BLOCK_SIZE,
    .nVectorDims = 1,
    .nNodeVectorType = VECTOR_TYPE_FLOAT32,
    .nEdgeVectorType = VECTOR_TYPE_FLOAT32,
    .nNodeVectorSize = vectorDataSize(VECTOR_TYPE_FLOAT32, 1),
    .nEdgeVectorSize = vectorDataSize(VECTOR_TYPE_FLOAT32, 1),
  };
  BlobSpot *pBlobSpot;

  // test1: try read non existing row
  ensure(blobSpotCreate(&index, &pBlobSpot, 0, TEST_BLOCK_SIZE, DISKANN_BLOB_WRITABLE) == DISKANN_ROW_NOT_FOUND, "unexpected error: %s\n", sqlite3_errmsg(db));
  ensure(sqlite3_exec(db, "INSERT INTO t_idx_shadow VALUES (1, x'00')", 0, 0, 0) == 0, "unable to insert entry: %s\n", sqlite3_errmsg(db));
  ensure(sqlite3_exec(db, "INSERT INTO t_idx_shadow VALUES (2, x'" PAYLOAD1_74 "')", 0, 0, 0) == 0, "unable to insert entry: %s\n", sqlite3_errmsg(db));
  ensure(sqlite3_exec(db, "INSERT INTO t_idx_shadow VALUES (3, x'" PAYLOAD2_74 "')", 0, 0, 0) == 0, "unable to insert entry: %s\n", sqlite3_errmsg(db));

  // test2: create blob poiting to the existing row
  ensure(blobSpotCreate(&index, &pBlobSpot, 1, TEST_BLOCK_SIZE, DISKANN_BLOB_WRITABLE) == SQLITE_OK, "unexpected error: %s\n", sqlite3_errmsg(db));
  blobSpotFree(pBlobSpot);

  // test3: create blob poiting to the existing row and try to read more data than it has
  ensure(blobSpotCreate(&index, &pBlobSpot, 1, TEST_BLOCK_SIZE, DISKANN_BLOB_WRITABLE) == SQLITE_OK, "unexpected error: %s\n", sqlite3_errmsg(db));
  ensure(blobSpotReload(&index, pBlobSpot, 1, TEST_BLOCK_SIZE) == SQLITE_ERROR, "unexpected error: %s\n", sqlite3_errmsg(db));
  blobSpotFree(pBlobSpot);

  // test4: now read the amount we want and also reposition opened BlobSpot to another row
  ensure(blobSpotCreate(&index, &pBlobSpot, 2, TEST_BLOCK_SIZE, DISKANN_BLOB_WRITABLE) == SQLITE_OK, "unexpected error: %s\n", sqlite3_errmsg(db));
  ensure(blobSpotReload(&index, pBlobSpot, 2, TEST_BLOCK_SIZE) == SQLITE_OK, "unexpected error: %s\n", sqlite3_errmsg(db));
  ensure(strncmp((const char*)pBlobSpot->pBuffer, "\x01\x02\x03\x04", 4) == 0, "unexpected blob content\n");
  ensure(blobSpotReload(&index, pBlobSpot, 3, TEST_BLOCK_SIZE) == SQLITE_OK, "unexpected error: %s\n", sqlite3_errmsg(db));
  ensure(strncmp((const char*)pBlobSpot->pBuffer, "\x05\x06\x07\x08", 4) == 0, "unexpected blob content\n");
  ensure(blobSpotReload(&index, pBlobSpot, 2, TEST_BLOCK_SIZE) == SQLITE_OK, "unexpected error: %s\n", sqlite3_errmsg(db));
  ensure(strncmp((const char*)pBlobSpot->pBuffer, "\x01\x02\x03\x04", 4) == 0, "unexpected blob content\n");
  blobSpotFree(pBlobSpot);

  // test5: update row
  ensure(blobSpotCreate(&index, &pBlobSpot, 2, TEST_BLOCK_SIZE, DISKANN_BLOB_WRITABLE) == SQLITE_OK, "unexpected error: %s\n", sqlite3_errmsg(db));
  float vectorData[4] = { 12.34, 1.1, 2.2, 3.3 };
  Vector vector = { .type = VECTOR_TYPE_FLOAT32, .dims = 1, .flags = 0, .data = vectorData };
  nodeBinInit(&index, pBlobSpot, 2, &vector);
  ensure(blobSpotFlush(pBlobSpot) == SQLITE_OK, "unexpected error: %s\n", sqlite3_errmsg(db));

  BlobSpot *pBlobSpotOther;
  ensure(blobSpotCreate(&index, &pBlobSpotOther, 2, TEST_BLOCK_SIZE, DISKANN_BLOB_WRITABLE) == SQLITE_OK, "unexpected error: %s\n", sqlite3_errmsg(db));
  ensure(blobSpotReload(&index, pBlobSpotOther, 2, TEST_BLOCK_SIZE) == SQLITE_OK, "unexpected error: %s\n", sqlite3_errmsg(db));
  nodeBinVector(&index, pBlobSpotOther, &vector);
  ensure(nodeBinEdges(&index, pBlobSpotOther) == 0, "unexpected edges count\n");
  ensure(((float*)vector.data)[0] == vectorData[0], "unexpected vector content\n");
  blobSpotFree(pBlobSpotOther);

  Vector vector1 = { .type = VECTOR_TYPE_FLOAT32, .dims = 1, .flags = 0, .data = vectorData + 1 };
  Vector vector2 = { .type = VECTOR_TYPE_FLOAT32, .dims = 1, .flags = 0, .data = vectorData + 2 };
  Vector vector3 = { .type = VECTOR_TYPE_FLOAT32, .dims = 1, .flags = 0, .data = vectorData + 3 };
  nodeBinReplaceEdge(&index, pBlobSpot, 0, 111, &vector1);
  nodeBinReplaceEdge(&index, pBlobSpot, 1, 112, &vector2);
  nodeBinReplaceEdge(&index, pBlobSpot, 2, 113, &vector3);
  ensure(nodeBinEdges(&index, pBlobSpot) == 3, "unexpected edges count\n");
  nodeBinDebug(&index, pBlobSpot);
  nodeBinPruneEdges(&index, pBlobSpot, 2);
  nodeBinDebug(&index, pBlobSpot);
  nodeBinReplaceEdge(&index, pBlobSpot, 1, 113, &vector3);
  nodeBinDebug(&index, pBlobSpot);
  nodeBinDeleteEdge(&index, pBlobSpot, 0);
  nodeBinDebug(&index, pBlobSpot);

  ensure(blobSpotFlush(pBlobSpot) == SQLITE_OK, "unexpected error: %s\n", sqlite3_errmsg(db));
  blobSpotFree(pBlobSpot);


  VectorIdxParams params;
  vectorIdxParamsInit(&params, NULL, 0);
  vectorIdxParamsPutU64(&params, 1, 101);
  vectorIdxParamsPutU64(&params, 2, 102);
  vectorIdxParamsPutU64(&params, 1, 103);
  vectorIdxParamsPutF64(&params, 3, 1.4);

  ensure(vectorIdxParamsGetU64(&params, 1) == 103, "invalid parameter\n");
  ensure(vectorIdxParamsGetU64(&params, 2) == 102, "invalid parameter\n");
  ensure(vectorIdxParamsGetF64(&params, 3) == 1.4, "invalid parameter\n");


  ensure(sqlite3_exec(db, "CREATE TABLE vectors ( emb FLOAT32(2) )", 0, 0, 0) == 0, "unable to create table: %s\n", sqlite3_errmsg(db));
  VectorIdxKey idxKey;
  idxKey.nKeyColumns = 1;
  idxKey.aKeyAffinity[0] = SQLITE_AFF_INTEGER;
  idxKey.azKeyCollation[0] = "BINARY";

  VectorIdxParams idxParams;
  vectorIdxParamsInit(&idxParams, NULL, 0);
  vectorIdxParamsPutU64(&idxParams, VECTOR_TYPE_PARAM_ID, VECTOR_TYPE_FLOAT32);
  vectorIdxParamsPutU64(&idxParams, VECTOR_DIM_PARAM_ID, 2);

  // this is hack for test - we are not in the context of query execution - so some invariants are violated and without this lock assertions failing
  sqlite3_mutex_enter(db->mutex);
  ensure(diskAnnCreateIndex(db, "vectors_idx", &idxKey, &idxParams) == 0, "unable to create diskann index: %s\n", sqlite3_errmsg(db));

  DiskAnnIndex *pIndex;
  int rc = diskAnnOpenIndex(db, "vectors_idx", &idxParams, &pIndex);
  ensure(rc == 0, "unable to open diskann index: %d\n", rc);

  sqlite3_value key;
  Vector vVector;
  VectorInRow inRow;
  key.db = db;
  key.flags = 0x04;
  inRow.nKeys = 1;
  inRow.pKeyValues = &key;
  inRow.pVector = &vVector;

  char* pzErrMsg;
  int deleted = 11, inserted = 11;
  // inserts:delete proportion is 3:1
  for(int i = 0; i < 100; i++){
    float vIndex[2] = { 1 + i, 1 - i };
    if( i % 4 != 3 ){
      key.u.i = inserted++;
      vectorInitStatic(inRow.pVector, VECTOR_TYPE_FLOAT32, (void*)vIndex, 4 * 2);
      ensure(diskAnnInsert(pIndex, &inRow, &pzErrMsg) == 0, "unable to insert vector: %s %s\n", pzErrMsg, sqlite3_errmsg(db));
    }else{
      key.u.i = deleted++;
      ensure(diskAnnDelete(pIndex, &inRow, &pzErrMsg) == 0, "unable to delete vector: %s %s\n", pzErrMsg, sqlite3_errmsg(db));
    }
  }

  float vIndex[2] = { 1, 1 };
  VectorOutRows rows;
  vectorInitStatic(inRow.pVector, VECTOR_TYPE_FLOAT32, (void*)vIndex, 4 * 2);
  ensure(diskAnnSearch(pIndex, inRow.pVector, 10, &idxKey, &rows, &pzErrMsg) == 0, "unable to search vector: %s\n", pzErrMsg);
  ensure(rows.nRows == 10, "unexpected rows count: %d != 10\n", rows.nRows);
  ensure(rows.nCols == 1, "unexpected cols count\n");
  vectorOutRowsFree(db, &rows);

  ensure(diskAnnSearch(pIndex, inRow.pVector, 60, &idxKey, &rows, &pzErrMsg) == 0, "unable to search vector: %s\n", pzErrMsg);
  ensure(rows.nRows == 50, "unexpected rows count: %d != 50\n", rows.nRows);
  ensure(rows.nCols == 1, "unexpected cols count\n");
  vectorOutRowsFree(db, &rows);

  ensure(diskAnnClearIndex(db, "vectors_idx") == 0, "unable to clear index\n");
  ensure(diskAnnSearch(pIndex, inRow.pVector, 60, &idxKey, &rows, &pzErrMsg) == 0, "unable to search vector: %s\n", pzErrMsg);
  ensure(rows.nRows == 0, "unexpected rows count: %d != 0\n", rows.nRows);
  ensure(rows.nCols == 1, "unexpected cols count\n");

  sqlite3_mutex_leave(db->mutex);
  // since we are manually holding locks - explicit close of db connection also triggers some assertion; so we don't close it here

  printf("all tests are passed!\n");
}

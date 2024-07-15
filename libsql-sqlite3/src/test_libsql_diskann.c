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

#define eprintf(...) fprintf(stderr, __VA_ARGS__)
#define ensure(condition, ...) { if (!(condition)) { eprintf(__VA_ARGS__); exit(1); } }

#define TEST_BLOCK_SIZE 74
#define ZERO74 "0102030400000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000"

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
  ensure(sqlite3_exec(db, "INSERT INTO t_idx_shadow VALUES (2, x'" ZERO74 "')", 0, 0, 0) == 0, "unable to insert entry: %s\n", sqlite3_errmsg(db));

  // test2: create blob poiting to the existing row
  ensure(blobSpotCreate(&index, &pBlobSpot, 1, TEST_BLOCK_SIZE, DISKANN_BLOB_WRITABLE) == SQLITE_OK, "unexpected error: %s\n", sqlite3_errmsg(db));
  blobSpotFree(pBlobSpot);

  // test3: create blob poiting to the existing row and try to read more data than it has
  ensure(blobSpotCreate(&index, &pBlobSpot, 1, TEST_BLOCK_SIZE, DISKANN_BLOB_WRITABLE) == SQLITE_OK, "unexpected error: %s\n", sqlite3_errmsg(db));
  ensure(blobSpotReload(&index, pBlobSpot, 1, TEST_BLOCK_SIZE) == SQLITE_ERROR, "unexpected error: %s\n", sqlite3_errmsg(db));
  // test4: now read the amount we want and also reposition opened BlobSpot to another row
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
  nodeBinInsertEdge(&index, pBlobSpot, 0, 111, &vector1);
  nodeBinInsertEdge(&index, pBlobSpot, 0, 112, &vector2);
  nodeBinInsertEdge(&index, pBlobSpot, 2, 113, &vector3);
  ensure(nodeBinEdges(&index, pBlobSpot) == 3, "unexpected edges count\n");
  nodeBinDebug(&index, pBlobSpot);
  nodeBinPruneEdges(&index, pBlobSpot, 2);
  nodeBinDebug(&index, pBlobSpot);
  nodeBinInsertEdge(&index, pBlobSpot, 1, 113, &vector3);
  nodeBinDebug(&index, pBlobSpot);
  nodeBinDeleteEdge(&index, pBlobSpot, 0);
  nodeBinDebug(&index, pBlobSpot);

  ensure(blobSpotFlush(pBlobSpot) == SQLITE_OK, "unexpected error: %s\n", sqlite3_errmsg(db));
  blobSpotFree(pBlobSpot);

  ensure(sqlite3_close(db) == 0, "unable to close memory db: %s\n", sqlite3_errmsg(db));
}

#include "../parse.h"
#include "../opcodes.h"
#include "../src/sqliteInt.h"
#include "../src/vectorIndexInt.h"
#include "assert.h"
#include "stdbool.h"
#include "string.h"
#include "stdarg.h"
#include "time.h"
#include <sqlite3.h>

#define eprintf(...) fprintf(stderr, __VA_ARGS__)
#define ensure(condition, ...) { if (!(condition)) { eprintf(__VA_ARGS__); exit(1); } }

int searchVectors(sqlite3 *db, sqlite3_stmt *pStmt, void **ppItems, int *pItemSize) {
  ensure(sqlite3_reset(pStmt) == SQLITE_OK, "failed to reset statement: %s\n", sqlite3_errmsg(db));
  int rows = 0;
  while(1){
    int rc = sqlite3_step(pStmt);
    if( rc == SQLITE_DONE ){
      break;
    } else if( rc == SQLITE_ROW ){
      const void *pBlob = sqlite3_column_blob(pStmt, 0);
      int nBlobSize = sqlite3_column_bytes(pStmt, 0);
      void *pBlobCopy = malloc(nBlobSize);
      memcpy(pBlobCopy, pBlob, nBlobSize);
      ppItems[rows] = pBlobCopy;
      pItemSize[rows] = nBlobSize;
      rows++;
    }else{
      ensure(false, "unexpected step result: %s\n", sqlite3_errmsg(db));
    }
  }
  return rows;
}

int searchRows(sqlite3 *db, sqlite3_stmt *pStmt, unsigned char *pBlob, int nBlobSize, int *result) {
  ensure(sqlite3_reset(pStmt) == SQLITE_OK, "failed to reset statement: %s\n", sqlite3_errmsg(db));
  ensure(sqlite3_bind_blob(pStmt, 1, pBlob, nBlobSize, SQLITE_TRANSIENT) == SQLITE_OK, "failed to bind blob: %s\n", sqlite3_errmsg(db));
  int rows = 0;
  while(1){
    int rc = sqlite3_step(pStmt);
    if( rc == SQLITE_DONE ){
      break;
    } else if( rc == SQLITE_ROW ){
      int rowid = sqlite3_column_int(pStmt, 0);
      result[rows++] = rowid;
    }else{
      ensure(false, "unexpected step result: %s\n", sqlite3_errmsg(db));
    }
  }
  return rows;
}

double recall(int *pExact, int nExactSize, int *pAnn, int nAnnSize) {
  int overlap = 0;
  for( int i = 0; i < nExactSize; i++ ){
    int ok = 0;
    for(int s = 0; !ok && s < nAnnSize; s++ ){
      ok |= pExact[i] == pAnn[s];
    }
    if(ok){
      overlap++;
    }
  }
  return overlap * 1.0 / nExactSize;
}

int main(int argc, char* argv[]) {
  ensure(argc == 5, "path to the db file, recall type, ann query, exact query");
  sqlite3* db;
  int rc = sqlite3_open(argv[1], &db);
  ensure(rc == 0, "failed to open db: rc=%d\n", rc);
  printf("open sqlite db at '%s'\n", argv[1]);

  char *zType = argv[2];
  void* vectors[65536];
  int vectorSize[65536];
  char *zAnnQuery = argv[3];
  char *zExactQuery = argv[4];

  sqlite3_stmt *pVectors;
  ensure(sqlite3_prepare_v2(db, "SELECT emb FROM queries", -1, &pVectors, 0) == SQLITE_OK, "failed to prepare vectors statement: %s\n", sqlite3_errmsg(db));
  sqlite3_stmt *pAnn;
  ensure(sqlite3_prepare_v2(db, zAnnQuery, -1, &pAnn, 0) == SQLITE_OK, "failed to prepare ann statement: %s\n", sqlite3_errmsg(db));
  sqlite3_stmt *pExact;
  ensure(sqlite3_prepare_v2(db, zExactQuery, -1, &pExact, 0) == SQLITE_OK, "failed to prepare exact statement: %s\n", sqlite3_errmsg(db));

  int nVectors = searchVectors(db, pVectors, vectors, vectorSize);

  unsigned char blob[8 * 65536];
  int annResult[65536];
  int exactResult[65536];

  printf("ready to perform %d queries with %s ann query and %s exact query\n", nVectors, zAnnQuery, zExactQuery);
  double totalRecall = 0;
  int total = 0;
  for(int i = 0; i < nVectors; i++){
    if( i % 10 == 9 ){
      eprintf("progress: %d / %d, %.2f%% %s (avg.)\n", i, nVectors, totalRecall / total * 100, zType);
    }
    int nAnnSize = searchRows(db, pAnn, vectors[i], vectorSize[i], annResult);
    int nExactSize = searchRows(db, pExact, vectors[i], vectorSize[i], exactResult);
    double r = recall(exactResult, nExactSize, annResult, nAnnSize);
    totalRecall += r;
    total++;
  }
  sqlite3_finalize(pAnn);
  sqlite3_finalize(pExact);
  printf("%.2f%% %s (avg.)\n", totalRecall / total * 100, zType);
  sqlite3_close(db);
  return 0;
}

#include "../src/sqliteInt.h"
#include "time.h"

#define eprintf(...) fprintf(stderr, __VA_ARGS__)
#define ensure(condition, ...) { if (!(condition)) { eprintf(__VA_ARGS__); exit(1); } }

int main(int argc, char* argv[]) {
  ensure(argc == 6, "provide path to the db file, blob open flags(read|write), blob open strategy(simple|reopen), amount of rows, size of the blob\n");
  sqlite3* db;
  int rc = sqlite3_open(argv[1], &db);
  ensure(rc == 0, "failed to open db: rc=%d\n", rc);
  printf("open sqlite db at '%s'\n", argv[1]);
  
  int openFlags = argv[2][0] == 'w';
  int openStrategy = argv[3][0] == 'r';
  int nRows = atoi(argv[4]);
  int nBlobSize = atoi(argv[5]);

  printf("blob table: ready to prepare\n");
  ensure(sqlite3_exec(db, "CREATE TABLE x ( id INTEGER PRIMARY KEY, blob BLOB )", 0, 0, NULL) == SQLITE_OK, "unable to create table: %s\n", sqlite3_errmsg(db));
  sqlite3_stmt *pStmt;
  ensure(sqlite3_prepare(db, "INSERT INTO x VALUES (?, ?)", -1, &pStmt, NULL) == SQLITE_OK, "unable to prepare statement: %s\n", sqlite3_errmsg(db));
  char *pTrash = malloc(nBlobSize);
  for(int i = 0; i < nRows; i++){
    ensure(sqlite3_reset(pStmt) == SQLITE_OK, "unable to reset statement: %s\n", sqlite3_errmsg(db));
    ensure(sqlite3_bind_int(pStmt, 1, i) == SQLITE_OK, "unable to bind int: %s\n", sqlite3_errmsg(db));
    ensure(sqlite3_bind_blob(pStmt, 2, pTrash, nBlobSize, 0) == SQLITE_OK, "unable to bind blob: %s\n", sqlite3_errmsg(db));
    ensure(sqlite3_step(pStmt) == SQLITE_DONE, "unexpected result of step: %s\n", sqlite3_errmsg(db));
  }
  sqlite3_finalize(pStmt);
  printf("blob table: prepared\n");

  time_t start_time = clock();
  int total = 0;
  sqlite3_blob *pBlob;
  if( openStrategy == 1 ){
    ensure(sqlite3_blob_open(db, db->aDb[0].zDbSName, "x", "blob", 0, openFlags, &pBlob) == SQLITE_OK, "unable to open blob: %s\n", sqlite3_errmsg(db));
  }
  for(int i = 0; i < nRows; i++){
    int rowid = rand() % nRows;
    total++;
    if( openStrategy == 1 ){
      ensure(sqlite3_blob_reopen(pBlob, rowid) == SQLITE_OK, "unable to reopen blob: %s\n", sqlite3_errmsg(db));
    }else{
      ensure(sqlite3_blob_open(db, db->aDb[0].zDbSName, "x", "blob", rowid, openFlags, &pBlob) == SQLITE_OK, "unable to reopen blob: %s\n", sqlite3_errmsg(db));
    }
    ensure(sqlite3_blob_read(pBlob, pTrash, nBlobSize, 0) == SQLITE_OK, "unable to read blob: %s\n", sqlite3_errmsg(db));
    if( openStrategy == 0 ){
      ensure(sqlite3_blob_close(pBlob) == SQLITE_OK, "unable to close blob: %s\n", sqlite3_errmsg(db));
    }
  }
  if( openStrategy == 1 ){
    ensure(sqlite3_blob_close(pBlob) == SQLITE_OK, "unable to close blob: %s\n", sqlite3_errmsg(db));
  }

  double total_time = (clock() - start_time) * 1.0 / CLOCKS_PER_SEC;
  printf("time: %.2f micros (avg.), %d (count)\n", total_time / total * 1000000, total);

  free(pTrash);
  sqlite3_close(db);
  return 0;
}

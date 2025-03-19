#include <sqlite3.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <assert.h>

#define eprintf(...) fprintf(stderr, __VA_ARGS__)
#define ensure(condition, ...) { if (!(condition)) { eprintf(__VA_ARGS__); exit(1); } }

#if 0
static void dump_frame(unsigned char *frame, size_t size){
  for(int addr=0; addr<size; addr+=16){
    int sum = 0;
    for(int i=0; i<16 && addr+1<size; i++){
      sum += frame[addr+i] != 0;
    }
    if( sum ){
      printf("%08x: ", addr);
      for(int i=0; i<16 && addr+i<size; i++){
        printf("%02x ", frame[addr+i]);
      }
      printf("  |");
      for(int i=0; i<16 && addr+i<size; i++){
        printf("%c", frame[addr+i] ? frame[addr+i] : '.');
      }
      printf("|");
      printf("\n");
    }
  }
}
#endif

static void cmp_data(sqlite3 *db1, sqlite3 *db2){
  sqlite3_stmt *stmt1, *stmt2;
  ensure(sqlite3_prepare_v2(db1, "SELECT HEX(x) FROM t", -1, &stmt1, 0) == SQLITE_OK, "can't prepare statement: %s\n", sqlite3_errmsg(db1));
  ensure(sqlite3_prepare_v2(db2, "SELECT HEX(x) FROM t", -1, &stmt2, 0) == SQLITE_OK, "can't prepare statement: %s\n", sqlite3_errmsg(db2));

  for(;;){
    int step1 = sqlite3_step(stmt1);
    int step2 = sqlite3_step(stmt2);
    ensure(step1 == step2, "step mismatch: %d != %d\n", step1, step2);
    if( step1!=SQLITE_ROW ){
      break;
    }
    const unsigned char *text1 = sqlite3_column_text(stmt1, 0);
    const unsigned char *text2 = sqlite3_column_text(stmt2, 0);
    ensure(strncmp((const char *)text1, (const char *)text2, 4096) == 0, "data mismatch");
  }
}

static void sync_db(sqlite3 *db_primary, sqlite3 *db_backup){
  unsigned int max_frame;
  ensure(libsql_wal_frame_count(db_primary, &max_frame) == SQLITE_OK, "can't get frame count: %s\n", sqlite3_errmsg(db_primary));
  ensure(libsql_wal_insert_begin(db_backup) == SQLITE_OK, "can't begin commit: %s\n", sqlite3_errmsg(db_backup));
  for(int i=1; i<=max_frame; i++){
    char frame[4096+24];
    ensure(libsql_wal_get_frame(db_primary, i, frame, sizeof(frame)) == SQLITE_OK, "can't get frame: %s\n", sqlite3_errmsg(db_primary));
    int conflict;
    ensure(libsql_wal_insert_frame(db_backup, i, frame, sizeof(frame), &conflict) == SQLITE_OK, "can't inject frame: %s\n", sqlite3_errmsg(db_backup));
    ensure(conflict == 0, "conflict at frame %d\n", i);
  }
  ensure(libsql_wal_insert_end(db_backup) == SQLITE_OK, "can't end commit: %s\n", sqlite3_errmsg(db_backup));
}

void open_db(const char *path, sqlite3 **db) {
  ensure(sqlite3_open(path, db) == SQLITE_OK, "can't open database %s: %s\n", path, sqlite3_errmsg(*db));
  ensure(sqlite3_exec(*db, "PRAGMA journal_mode=WAL", NULL, NULL, NULL) == SQLITE_OK, "can't set journal mode for %s: %s\n", path, sqlite3_errmsg(*db));
  ensure(sqlite3_wal_autocheckpoint(*db, 0) == SQLITE_OK, "can't disable checkpointing for %s: %s\n", path, sqlite3_errmsg(*db));
}

void test_huge_payload() {
    sqlite3 *db_primary, *db_backup;
    unsigned int max_frame;
    open_db("primary_test_huge_payload.db", &db_primary);
    ensure(sqlite3_exec(db_primary, "CREATE TABLE t (x)", 0, 0, 0) == SQLITE_OK, "failed to insert data\n");
    ensure(sqlite3_exec(db_primary, "INSERT INTO t VALUES (randomblob(4 * 1024))", 0, 0, 0) == SQLITE_OK, "failed to insert data\n");
    ensure(sqlite3_exec(db_primary, "INSERT INTO t VALUES (randomblob(1 * 1024))", 0, 0, 0) == SQLITE_OK, "failed to insert data\n");
    ensure(sqlite3_exec(db_primary, "INSERT INTO t VALUES (randomblob(1 * 1024))", 0, 0, 0) == SQLITE_OK, "failed to insert data\n");

    open_db("backup_test_huge_payload.db", &db_backup);
    ensure(libsql_wal_frame_count(db_backup, &max_frame) == SQLITE_OK, "failed to get frames count: %s\n", sqlite3_errmsg(db_backup));
    assert(max_frame == 0);
    
    eprintf("start full sync\n");
    sync_db(db_primary, db_backup);
    cmp_data(db_primary, db_backup);
    sync_db(db_primary, db_backup);
    cmp_data(db_primary, db_backup);
}

void test_sync_by_parts() {
    sqlite3 *db_primary, *db_backup;
    unsigned int max_frame;
    uint32_t in_commit = 0;
    open_db("primary_test_sync_by_parts.db", &db_primary);
    ensure(sqlite3_exec(db_primary, "CREATE TABLE t (x)", 0, 0, 0) == SQLITE_OK, "failed to insert data\n");
    ensure(sqlite3_exec(db_primary, "INSERT INTO t VALUES (randomblob(4 * 1024))", 0, 0, 0) == SQLITE_OK, "failed to insert data\n");
    ensure(sqlite3_exec(db_primary, "INSERT INTO t VALUES (randomblob(1 * 1024))", 0, 0, 0) == SQLITE_OK, "failed to insert data\n");
    ensure(sqlite3_exec(db_primary, "INSERT INTO t VALUES (randomblob(1 * 1024))", 0, 0, 0) == SQLITE_OK, "failed to insert data\n");

    open_db("backup_test_sync_by_parts.db", &db_backup);

    ensure(libsql_wal_frame_count(db_primary, &max_frame) == SQLITE_OK, "can't get frame count: %s\n", sqlite3_errmsg(db_primary));
    eprintf("start sync by parts\n");
    for(int i=1; i<=max_frame; i++){
      char frame[4096+24];
      uint32_t is_commit;
      ensure(libsql_wal_get_frame(db_primary, i, frame, sizeof(frame)) == SQLITE_OK, "can't get frame: %s\n", sqlite3_errmsg(db_primary));
      is_commit = ((uint32_t)frame[4] << 24) + ((uint32_t)frame[5] << 16) + ((uint32_t)frame[6] << 8) + ((uint32_t)frame[7] << 0);
      if (!in_commit) {
        in_commit = 1;
        ensure(libsql_wal_insert_begin(db_backup) == SQLITE_OK, "can't begin commit: %s\n", sqlite3_errmsg(db_backup));
      }
      int conflict; 
      ensure(libsql_wal_insert_frame(db_backup, i, frame, sizeof(frame), &conflict) == SQLITE_OK, "can't inject frame: %s\n", sqlite3_errmsg(db_backup));
      ensure(conflict == 0, "conflict at frame %d\n", i);
      if (is_commit) {
        ensure(libsql_wal_insert_end(db_backup) == SQLITE_OK, "can't end commit: %s\n", sqlite3_errmsg(db_backup));
        in_commit = 0;
      }
    }
    cmp_data(db_primary, db_backup);
}

// This test case writes to a local database, syncs it to remote, and then verifies the remote.
// The test then writes some more to local database, syncs it again, and verifies the remote again.
void test_sync_while_reading() {
     sqlite3 *db_primary, *db_backup;
    unsigned int max_frame;
    open_db("primary_test_sync_while_reading.db", &db_primary);
    ensure(sqlite3_exec(db_primary, "CREATE TABLE t (x)", 0, 0, 0) == SQLITE_OK, "failed to insert data\n");
    ensure(sqlite3_exec(db_primary, "INSERT INTO t VALUES (randomblob(4 * 1024))", 0, 0, 0) == SQLITE_OK, "failed to insert data\n");
    ensure(sqlite3_exec(db_primary, "INSERT INTO t VALUES (randomblob(1 * 1024))", 0, 0, 0) == SQLITE_OK, "failed to insert data\n");
    ensure(sqlite3_exec(db_primary, "INSERT INTO t VALUES (randomblob(1 * 1024))", 0, 0, 0) == SQLITE_OK, "failed to insert data\n");

    open_db("backup_test_sync_while_reading.db", &db_backup);
    ensure(libsql_wal_frame_count(db_backup, &max_frame) == SQLITE_OK, "failed to get frames count: %s\n", sqlite3_errmsg(db_backup));
    assert(max_frame == 0);
    
    eprintf("start full sync\n");
    sync_db(db_primary, db_backup);
    cmp_data(db_primary, db_backup);

    ensure(sqlite3_exec(db_primary, "INSERT INTO t VALUES (randomblob(4 * 1024))", 0, 0, 0) == SQLITE_OK, "failed to insert data\n");
    ensure(sqlite3_exec(db_primary, "INSERT INTO t VALUES (randomblob(1 * 1024))", 0, 0, 0) == SQLITE_OK, "failed to insert data\n");
    ensure(sqlite3_exec(db_primary, "INSERT INTO t VALUES (randomblob(1 * 1024))", 0, 0, 0) == SQLITE_OK, "failed to insert data\n");
    sync_db(db_primary, db_backup);
    cmp_data(db_primary, db_backup); 
}

// This test case writes to two different databases and then attempts to sync them to a third database.
// Only the first database should be synced, the second database sync should return a conflict error
void test_conflict() {    
  sqlite3 *db1, *db2, *db_synced;
  open_db("test_conflict_1.db", &db1);
  open_db("test_conflict_2.db", &db2);
  open_db("test_conflict_synced.db", &db_synced);

  ensure(sqlite3_exec(db1, "CREATE TABLE t (x)", 0, 0, 0) == SQLITE_OK, "failed to insert data\n");
  ensure(sqlite3_exec(db1, "INSERT INTO t VALUES (randomblob(4 * 1024))", 0, 0, 0) == SQLITE_OK, "failed to insert data\n");

  sync_db(db1, db_synced);

  ensure(sqlite3_exec(db2, "CREATE TABLE t (x)", 0, 0, 0) == SQLITE_OK, "failed to insert data\n");
  ensure(sqlite3_exec(db2, "INSERT INTO t VALUES (randomblob(4 * 1024))", 0, 0, 0) == SQLITE_OK, "failed to insert data\n");

  unsigned int max_frame;
  ensure(libsql_wal_frame_count(db2, &max_frame) == SQLITE_OK, "can't get frame count: %s\n", sqlite3_errmsg(db2));
  ensure(libsql_wal_insert_begin(db_synced) == SQLITE_OK, "can't begin commit: %s\n", sqlite3_errmsg(db_synced));
  // First 3 frames should not conflict.
  for(int i=1; i<=3; i++){
    char frame[4096+24];
    ensure(libsql_wal_get_frame(db2, i, frame, sizeof(frame)) == SQLITE_OK, "can't get frame: %s\n", sqlite3_errmsg(db2));
    int conflict;
    ensure(libsql_wal_insert_frame(db_synced, i, frame, sizeof(frame), &conflict) == SQLITE_OK, "conflict detected: %s\n", sqlite3_errmsg(db_synced));
    ensure(conflict == 0, "conflict at frame %d\n", i);
  }
  // The rest should conflict.
  for(int i=4; i<=max_frame; i++){
    char frame[4096+24];
    ensure(libsql_wal_get_frame(db2, i, frame, sizeof(frame)) == SQLITE_OK, "can't get frame: %s\n", sqlite3_errmsg(db2));
    int conflict;
    ensure(libsql_wal_insert_frame(db_synced, i, frame, sizeof(frame), &conflict) == SQLITE_ERROR, "conflict not detected: %s\n", sqlite3_errmsg(db_synced));
    ensure(conflict == 1, "no conflict at frame %d\n", i);
  }
  ensure(libsql_wal_insert_end(db_synced) == SQLITE_OK, "can't end commit: %s\n", sqlite3_errmsg(db_synced));
}

int main(int argc, char *argv[])
{
    test_huge_payload();
    printf("============= OK test_huge_payload\n");

    test_sync_by_parts();
    printf("============= OK test_sync_by_parts\n");

    test_sync_while_reading();
    printf("============= OK test_sync_while_reading\n");

    test_conflict();
    printf("============= OK test_conflict\n");

    return 0;
}

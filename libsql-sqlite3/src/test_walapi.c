#include <sqlite3.h>
#include <stdio.h>
#include <string.h>

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

static int cmp_data(sqlite3 *db1, sqlite3 *db2){
  sqlite3_stmt *stmt1, *stmt2;
  int rc;

  rc = sqlite3_prepare_v2(db1, "SELECT * FROM users", -1, &stmt1, 0);
  if( rc!=SQLITE_OK ){
    fprintf(stderr, "Can't prepare statement: %s\n", sqlite3_errmsg(db1));
    return 1;
  }

  rc = sqlite3_prepare_v2(db2, "SELECT * FROM users", -1, &stmt2, 0);
  if( rc!=SQLITE_OK ){
    fprintf(stderr, "Can't prepare statement: %s\n", sqlite3_errmsg(db2));
    return 1;
  }

  for(;;){
    int step1 = sqlite3_step(stmt1);
    int step2 = sqlite3_step(stmt2);
    if( step1!=step2 ){
      fprintf(stderr, "Step mismatch: %d != %d\n", step1, step2);
      return 1;
    }
    if( step1!=SQLITE_ROW ){
      break;
    }
    const unsigned char *name1 = sqlite3_column_text(stmt1, 1);
    const unsigned char *name2 = sqlite3_column_text(stmt2, 1);
    if( strcmp((const char *)name1, (const char *)name2)!=0 ){
      fprintf(stderr, "Data mismatch: %s != %s\n", name1, name2);
      return 1;
    }
  }
  return 0;
}

static int sync_db(sqlite3 *db_primary, sqlite3 *db_backup){
  unsigned int max_frame;
  int rc;

  rc = libsql_wal_frame_count(db_primary, &max_frame);
  if( rc!=SQLITE_OK ){
    fprintf(stderr, "Can't get frame count: %s\n", sqlite3_errmsg(db_primary));
    return 1;
  }
  rc = libsql_wal_insert_begin(db_backup);
  if( rc!=SQLITE_OK ){
    fprintf(stderr, "Can't begin commit: %s\n", sqlite3_errmsg(db_backup));
    return 1;
  }
  for(int i=1; i<=max_frame; i++){
    char frame[4096+24];
    rc = libsql_wal_get_frame(db_primary, i, frame, sizeof(frame));
    if( rc!=SQLITE_OK ){
      fprintf(stderr, "Can't get frame: %s\n", sqlite3_errmsg(db_primary));
      return 1;
    }
    rc = libsql_wal_insert_frame(db_backup, i, frame, sizeof(frame));
    if( rc!=SQLITE_OK ){
      fprintf(stderr, "Can't inject frame %d: %s\n", rc, sqlite3_errmsg(db_backup));
      return 1;
    }
  }
  rc = libsql_wal_insert_end(db_backup);
  if( rc!=SQLITE_OK ){
    fprintf(stderr, "Can't end commit: %s\n", sqlite3_errmsg(db_backup));
    return 1;
  }
  return 0;
}

static void gen_data(sqlite3 *db){
  sqlite3_exec(db, "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)", 0, 0, 0);
  sqlite3_exec(db, "INSERT INTO users (id, name) VALUES (1, 'John Doe')", 0, 0, 0);
  sqlite3_exec(db, "INSERT INTO users (id, name) VALUES (2, 'Jane Doe')", 0, 0, 0);
  sqlite3_exec(db, "INSERT INTO users (id, name) VALUES (3, 'Jim Beam')", 0, 0, 0);
}

int main(int argc, char *argv[])
{
    sqlite3 *db_primary, *db_backup;
    int rc;

    rc = sqlite3_open("primary.db", &db_primary);
    if (rc != SQLITE_OK) {
        fprintf(stderr, "Can't open database: %s\n", sqlite3_errmsg(db_primary));
        return 1;
    }
    rc = sqlite3_wal_autocheckpoint(db_primary, 0);
    if (rc != SQLITE_OK) {
        fprintf(stderr, "Can't disable checkpointing: %s\n", sqlite3_errmsg(db_primary));
        return 1;
    }
    sqlite3_exec(db_primary, "PRAGMA journal_mode=WAL", NULL, NULL, NULL);

    gen_data(db_primary);

    rc = sqlite3_open("backup.db", &db_backup); 
    if (rc != SQLITE_OK) {
        fprintf(stderr, "Can't open database: %s\n", sqlite3_errmsg(db_backup));
        return 1;
    }
    rc = sqlite3_wal_autocheckpoint(db_backup, 0);
    if (rc != SQLITE_OK) {
        fprintf(stderr, "Can't disable checkpointing: %s\n", sqlite3_errmsg(db_backup));
        return 1;
    }
    rc = sqlite3_exec(db_backup, "PRAGMA journal_mode=WAL", NULL, NULL, NULL);
    if (rc != SQLITE_OK) {
        fprintf(stderr, "Can't set journal mode: %s\n", sqlite3_errmsg(db_backup));
        return 1;
    }

    sync_db(db_primary, db_backup);
    if (cmp_data(db_primary, db_backup)) {
        return 1;
    }

    sync_db(db_primary, db_backup);
    if (cmp_data(db_primary, db_backup)) {
        return 1;
    }

    printf("OK\n");

    return 0;
}

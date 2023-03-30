#include "seen-peers.h"

#include <assert.h>
#include <stdio.h>
#include <string.h>

#include "ext-data.h"

int crsql_close(sqlite3 *db);

static void testAllocation() {
  printf("Allocation\n");
  crsql_SeenPeers *seen = crsql_newSeenPeers();
  assert(seen->len == 0);
  assert(seen->capacity == CRSQL_SEEN_PEERS_INITIAL_SIZE);
  assert(seen->peers != 0);

  for (int i = 0; i < CRSQL_SEEN_PEERS_INITIAL_SIZE; ++i) {
    assert(seen->peers[i].clock == 0);
    assert(seen->peers[i].siteId == 0);
    assert(seen->peers[i].siteIdLen == 0);
  }

  printf("\t\e[0;32mSuccess\e[0m\n");

  crsql_freeSeenPeers(seen);
}

static void testTrackNewPeer() {
  printf("TrackNewPeer\n");
  crsql_SeenPeers *seen = crsql_newSeenPeers();

  crsql_trackSeenPeer(seen, (const unsigned char *)"blob", 5, 100);
  assert(seen->len == 1);
  assert(seen->peers[0].clock == 100);
  assert(seen->peers[0].siteIdLen == 5);
  assert(strcmp((const char *)seen->peers[0].siteId, "blob") == 0);
  assert(seen->capacity == CRSQL_SEEN_PEERS_INITIAL_SIZE);

  printf("\t\e[0;32mSuccess\e[0m\n");
  crsql_freeSeenPeers(seen);
}

static void testTrackExistingPeer() {
  printf("TrackExistingPeer\n");
  crsql_SeenPeers *seen = crsql_newSeenPeers();

  crsql_trackSeenPeer(seen, (const unsigned char *)"blob", 5, 100);
  crsql_trackSeenPeer(seen, (const unsigned char *)"blob", 5, 200);

  assert(seen->len == 1);
  assert(seen->peers[0].clock == 200);
  assert(seen->peers[0].siteIdLen == 5);
  assert(strcmp((const char *)seen->peers[0].siteId, "blob") == 0);
  assert(seen->capacity == CRSQL_SEEN_PEERS_INITIAL_SIZE);

  crsql_trackSeenPeer(seen, (const unsigned char *)"blob", 5, 2);

  assert(seen->len == 1);
  assert(seen->peers[0].clock == 200);
  assert(seen->peers[0].siteIdLen == 5);
  assert(strcmp((const char *)seen->peers[0].siteId, "blob") == 0);
  assert(seen->capacity == CRSQL_SEEN_PEERS_INITIAL_SIZE);

  printf("\t\e[0;32mSuccess\e[0m\n");
  crsql_freeSeenPeers(seen);
}

static void testArrayGrowth() {
  printf("ArrayGrowth\n");
  crsql_SeenPeers *seen = crsql_newSeenPeers();

  for (int i = 0; i < 11; ++i) {
    char *blob = sqlite3_mprintf("b%d", i);
    int blobLen = strlen(blob) + 1;
    crsql_trackSeenPeer(seen, (unsigned char *)blob, blobLen, i);
    sqlite3_free(blob);
  }

  assert(seen->capacity == 20);
  assert(seen->len == 11);

  for (int i = 0; i < 11; ++i) {
    char *blob = sqlite3_mprintf("b%d", i);
    int blobLen = strlen(blob) + 1;
    assert(seen->peers[i].clock == i);
    assert(seen->peers[i].siteIdLen == blobLen);
    assert(strcmp((char *)seen->peers[i].siteId, blob) == 0);
    sqlite3_free(blob);
  }

  printf("\t\e[0;32mSuccess\e[0m\n");
  crsql_freeSeenPeers(seen);
}

static void testReset() {
  printf("Reset\n");
  crsql_SeenPeers *seen = crsql_newSeenPeers();
  crsql_trackSeenPeer(seen, (const unsigned char *)"blob1", 6, 100);
  crsql_trackSeenPeer(seen, (const unsigned char *)"blob2", 6, 200);

  crsql_resetSeenPeers(seen);
  assert(seen->len == 0);

  crsql_trackSeenPeer(seen, (const unsigned char *)"blob1", 6, 1);
  crsql_trackSeenPeer(seen, (const unsigned char *)"blob2", 6, 2);

  assert(seen->len == 2);
  assert(seen->peers[0].clock == 1);
  assert(seen->peers[1].clock == 2);

  crsql_trackSeenPeer(seen, (const unsigned char *)"blob1", 6, 11);
  crsql_trackSeenPeer(seen, (const unsigned char *)"blob2", 6, 22);

  assert(seen->len == 2);
  assert(seen->peers[0].clock == 11);
  assert(seen->peers[1].clock == 22);

  printf("\t\e[0;32mSuccess\e[0m\n");
  crsql_freeSeenPeers(seen);
}

// Really only exists for simple valgrind/asan leak tracking
static void testFree() {
  printf("Free\n");
  crsql_SeenPeers *seen = crsql_newSeenPeers();
  crsql_freeSeenPeers(seen);
  printf("\t\e[0;32mSuccess\e[0m\n");
}

static int countTrackedPeers(sqlite3 *db) {
  sqlite3_stmt *pStmt;
  int rc = sqlite3_prepare_v2(db, "SELECT count(*) FROM crsql_tracked_peers",
                              -1, &pStmt, 0);
  assert(rc == SQLITE_OK);

  rc = sqlite3_step(pStmt);
  assert(rc == SQLITE_ROW);
  int ret = sqlite3_column_int(pStmt, 0);
  sqlite3_finalize(pStmt);
  return ret;
}

static void assertWrittenPeers(sqlite3 *db, crsql_SeenPeer *zExpected,
                               int zExpectedLen) {
  sqlite3_stmt *pStmt;
  int rc = sqlite3_prepare_v2(
      db, "SELECT site_id, version, tag FROM crsql_tracked_peers", -1, &pStmt,
      0);
  assert(rc == SQLITE_OK);

  int compared = 0;
  while ((rc = sqlite3_step(pStmt)) == SQLITE_ROW) {
    assert(compared < zExpectedLen);

    int siteIdLen = sqlite3_column_bytes(pStmt, 0);
    const char *siteId = (const char *)sqlite3_column_blob(pStmt, 0);
    int clock = sqlite3_column_int64(pStmt, 1);
    assert(strcmp((const char *)zExpected[compared].siteId, siteId) == 0);
    assert(zExpected[compared].clock == clock);
    assert(zExpected[compared].siteIdLen == siteIdLen);

    compared++;
  }

  assert(compared == zExpectedLen);
  assert(rc == SQLITE_DONE);
  sqlite3_finalize(pStmt);
}

static void testWriteTrackedPeersToDb() {
  printf("WriteTrackedPeersToDb");
  sqlite3 *db;
  int rc = sqlite3_open(":memory:", &db);
  assert(rc == SQLITE_OK);

  crsql_SeenPeers *seen = crsql_newSeenPeers();
  crsql_ExtData *extData = crsql_newExtData(db);

  // writing empty set is a no-op
  assert(crsql_writeTrackedPeers(seen, extData) == SQLITE_OK);
  assert(countTrackedPeers(db) == 0);

  crsql_SeenPeer *expected = sqlite3_malloc(2 * sizeof(crsql_SeenPeer));
  expected[0].siteId = (unsigned char *)"blob1";
  expected[0].siteIdLen = 6;
  expected[0].clock = 11;
  expected[1].siteId = (unsigned char *)"blob2";
  expected[1].siteIdLen = 6;
  expected[1].clock = 22;

  // writing a some peers
  crsql_trackSeenPeer(seen, expected[0].siteId, expected[0].siteIdLen,
                      expected[0].clock);
  crsql_trackSeenPeer(seen, expected[1].siteId, expected[1].siteIdLen,
                      expected[1].clock);
  assert(crsql_writeTrackedPeers(seen, extData) == SQLITE_OK);
  assert(countTrackedPeers(db) == 2);
  assertWrittenPeers(db, expected, 2);

  // we can't run the clocks backwards
  crsql_resetSeenPeers(seen);
  crsql_trackSeenPeer(seen, expected[0].siteId, expected[0].siteIdLen, 1);
  crsql_trackSeenPeer(seen, expected[1].siteId, expected[1].siteIdLen, 2);
  assert(crsql_writeTrackedPeers(seen, extData) == SQLITE_OK);
  assert(countTrackedPeers(db) == 2);
  assertWrittenPeers(db, expected, 2);

  // but can run them forward
  expected[0].clock = 100;
  expected[1].clock = 200;
  crsql_trackSeenPeer(seen, expected[0].siteId, expected[0].siteIdLen,
                      expected[0].clock);
  crsql_trackSeenPeer(seen, expected[1].siteId, expected[1].siteIdLen,
                      expected[1].clock);
  assert(crsql_writeTrackedPeers(seen, extData) == SQLITE_OK);
  assert(countTrackedPeers(db) == 2);
  assertWrittenPeers(db, expected, 2);

  printf("\t\e[0;32mSuccess\e[0m\n");
  sqlite3_free(expected);
  crsql_freeSeenPeers(seen);
  crsql_freeExtData(extData);
  crsql_close(db);
}

void crsqlSeenPeersTestSuite() {
  printf("\e[47m\e[1;30mSuite: seenpeers\e[0m\n");

  testAllocation();
  testTrackNewPeer();
  testTrackExistingPeer();
  testArrayGrowth();
  testReset();
  testFree();
  testWriteTrackedPeersToDb();
}
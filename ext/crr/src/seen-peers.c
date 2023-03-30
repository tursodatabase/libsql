/**
 * Tracks what peers we have seen in a transaction against `crsql_changes`
 *
 * This is so, at the end of the transaction, we can update clock tables
 * for the user making network layers simpler to build.
 */
#include "seen-peers.h"

#include <string.h>

#include "ext-data.h"
#include "util.h"

// The assumption for using an array over a hash table is that we generally
// don't merge changes from many peers all at the same time.
// TODO: maybe don't even allow this to be growable so we can exit
// when we hit a use case with too many peers? Hard cap to 25?
crsql_SeenPeers *crsql_newSeenPeers() {
  crsql_SeenPeers *ret = sqlite3_malloc(sizeof *ret);
  ret->peers =
      sqlite3_malloc(CRSQL_SEEN_PEERS_INITIAL_SIZE * sizeof(crsql_SeenPeer));
  memset(ret->peers, 0, CRSQL_SEEN_PEERS_INITIAL_SIZE * sizeof(crsql_SeenPeer));
  ret->len = 0;
  ret->capacity = CRSQL_SEEN_PEERS_INITIAL_SIZE;

  return ret;
}

void crsql_freeSeenPeers(crsql_SeenPeers *a) {
  for (size_t i = 0; i < a->len; ++i) {
    sqlite3_free(a->peers[i].siteId);
  }
  sqlite3_free(a->peers);
  sqlite3_free(a);
}

int crsql_trackSeenPeer(crsql_SeenPeers *a, const unsigned char *siteId,
                        int siteIdLen, sqlite3_int64 clock) {
  // Have we already tacked this peer?
  // If so, take the max of clock values and return.
  for (size_t i = 0; i < a->len; ++i) {
    if (crsql_siteIdCmp(siteId, siteIdLen, a->peers[i].siteId,
                        a->peers[i].siteIdLen) == 0) {
      if (a->peers[i].clock < clock) {
        a->peers[i].clock = clock;
      }

      return SQLITE_OK;
    }
  }

  // are we at capacity and it is a new peer?
  // increase our size.
  if (a->len == a->capacity) {
    a->capacity *= 2;
    crsql_SeenPeer *reallocedPeers =
        sqlite3_realloc(a->peers, a->capacity * sizeof(crsql_SeenPeer));
    if (reallocedPeers == 0) {
      return SQLITE_ERROR;
    }
    a->peers = reallocedPeers;
  }

  // assign the peer
  // the provided `siteId` param is controlled by `sqlite` as an argument to the
  // insert statement and may not exist on transaction commit if many insert
  // calls are made against the vtab
  a->peers[a->len].siteId = sqlite3_malloc(siteIdLen * sizeof(char));
  memcpy(a->peers[a->len].siteId, siteId, siteIdLen);
  a->peers[a->len].clock = clock;
  a->peers[a->len].siteIdLen = siteIdLen;

  a->len += 1;
  return SQLITE_OK;
}

void crsql_resetSeenPeers(crsql_SeenPeers *a) {
  // free the inner allocations since we'll overwrite those
  for (size_t i = 0; i < a->len; ++i) {
    sqlite3_free(a->peers[i].siteId);
  }

  // re-wind our length back to 0 for the next transaction
  // this structure is allocated once per connection and each connection must
  // only be used from one thread.
  a->len = 0;
}

int crsql_writeTrackedPeers(crsql_SeenPeers *a, crsql_ExtData *pExtData) {
  int rc = SQLITE_OK;
  if (a->len == 0) {
    return rc;
  }

  for (size_t i = 0; i < a->len; ++i) {
    rc = sqlite3_bind_blob(pExtData->pTrackPeersStmt, 1, a->peers[i].siteId,
                           a->peers[i].siteIdLen, SQLITE_STATIC);
    rc += sqlite3_bind_int64(pExtData->pTrackPeersStmt, 2, a->peers[i].clock);
    // TODO: allow applying a tag. Currently always 0 for whole db
    rc += sqlite3_bind_int64(pExtData->pTrackPeersStmt, 3, 0);
    // Binding event. 0 for recv, 1 for send
    rc += sqlite3_bind_int(pExtData->pTrackPeersStmt, 4, 0);
    if (rc != SQLITE_OK) {
      sqlite3_clear_bindings(pExtData->pTrackPeersStmt);
      return rc;
    }

    rc = sqlite3_step(pExtData->pTrackPeersStmt);
    if (rc != SQLITE_DONE) {
      sqlite3_clear_bindings(pExtData->pTrackPeersStmt);
      sqlite3_reset(pExtData->pTrackPeersStmt);
      return rc;
    }

    rc = sqlite3_clear_bindings(pExtData->pTrackPeersStmt);
    rc += sqlite3_reset(pExtData->pTrackPeersStmt);
    if (rc != SQLITE_OK) {
      return rc;
    }
  }

  return rc;
}
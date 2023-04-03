#ifndef CRSQLITE_SEEN_PEERS_H
#define CRSQLITE_SEEN_PEERS_H

#include "sqlite3ext.h"
SQLITE_EXTENSION_INIT3

#include <ctype.h>
#include <stdlib.h>

#include "ext-data.h"

#define CRSQL_SEEN_PEERS_INITIAL_SIZE 5
#define CRSQL_SEEN_PEERS_RECV 0
#define CRSQL_SEEN_PEERS_SEND 1

typedef struct crsql_SeenPeer crsql_SeenPeer;
struct crsql_SeenPeer {
  unsigned char *siteId;
  int siteIdLen;
  sqlite3_int64 clock;
};

typedef struct crsql_SeenPeers crsql_SeenPeers;
struct crsql_SeenPeers {
  crsql_SeenPeer *peers;
  size_t len;
  size_t capacity;
};

crsql_SeenPeers *crsql_newSeenPeers();
int crsql_trackSeenPeer(crsql_SeenPeers *a, const unsigned char *siteId,
                        int siteIdLen, sqlite3_int64 clock);
void crsql_resetSeenPeers(crsql_SeenPeers *a);
void crsql_freeSeenPeers(crsql_SeenPeers *a);
int crsql_writeTrackedPeers(crsql_SeenPeers *a, crsql_ExtData *pExtData);

#endif

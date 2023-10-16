/*
** 2021-01-01
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
*************************************************************************
** 
** This file implements a program used to measure the start-up performance
** of SQLite.
**
** To use:
**
**     ./startup init
**     valgrind --tool=cachegrind ./startup run
**
**
** The "./startup init" command creates the test database file named
** "startup.db".  The performance test is run by the "./startup run"
** command.  That command does nothing but open the database file and
** parse the entire schema.
*/
#include <stdio.h>
#include <unistd.h>
#include <string.h>
#include <stdlib.h>
#include <ctype.h>
#include "sqlite3.h"

static const char zHelp[] =
  "Usage: %s COMMAND\n"
  "Commands:\n"
  "  init                Initialized the startup.db database file\n"
  "  run                 Run the startup performance test\n"
  "Options:\n"
  "  --dbname NAME       Set the name of the test database file\n"
  "  --heap SZ MIN       Memory allocator uses SZ bytes & min allocation MIN\n"
  "  --stats             Show statistics at the end\n"
/* TBD
  "  --journal M         Set the journal_mode to M\n"
  "  --lookaside N SZ    Configure lookaside for N slots of SZ bytes each\n"
  "  --mmap SZ           MMAP the first SZ bytes of the database file\n"
  "  --multithread       Set multithreaded mode\n"
  "  --nomemstat         Disable memory statistics\n"
  "  --pagesize N        Set the page size to N\n"
  "  --pcache N SZ       Configure N pages of pagecache each of size SZ bytes\n"
  "  --serialized        Set serialized threading mode\n"
  "  --singlethread      Set single-threaded mode - disables all mutexing\n"
  "  --utf16be           Set text encoding to UTF-16BE\n"
  "  --utf16le           Set text encoding to UTF-16LE\n"
  "  --utf8              Set text encoding to UTF-8\n"
*/
;

static void usage(const char *argv0){
  printf(zHelp, argv0);
  exit(1);
}

/*
** The test schema is derived from the Fossil repository for SQLite itself.
** The schema covers the repository, the local checkout database, and
** the global configuration database.
*/
static const char zTestSchema[] = 
  "CREATE TABLE repo_blob(\n"
  "  rid INTEGER PRIMARY KEY,\n"
  "  rcvid INTEGER,\n"
  "  size INTEGER,\n"
  "  uuid TEXT UNIQUE NOT NULL,\n"
  "  content BLOB,\n"
  "  CHECK( length(uuid)>=40 AND rid>0 )\n"
  ");\n"
  "CREATE TABLE repo_delta(\n"
  "  rid INTEGER PRIMARY KEY,\n"
  "  srcid INTEGER NOT NULL REFERENCES blob\n"
  ");\n"
  "CREATE TABLE repo_rcvfrom(\n"
  "  rcvid INTEGER PRIMARY KEY,\n"
  "  uid INTEGER REFERENCES user,\n"
  "  mtime DATETIME,\n"
  "  nonce TEXT UNIQUE,\n"
  "  ipaddr TEXT\n"
  ");\n"
  "CREATE TABLE repo_private(rid INTEGER PRIMARY KEY);\n"
  "CREATE TABLE repo_accesslog(\n"
  "  uname TEXT,\n"
  "  ipaddr TEXT,\n"
  "  success BOOLEAN,\n"
  "  mtime TIMESTAMP);\n"
  "CREATE TABLE repo_user(\n"
  "  uid INTEGER PRIMARY KEY,\n"
  "  login TEXT UNIQUE,\n"
  "  pw TEXT,\n"
  "  cap TEXT,\n"
  "  cookie TEXT,\n"
  "  ipaddr TEXT,\n"
  "  cexpire DATETIME,\n"
  "  info TEXT,\n"
  "  mtime DATE,\n"
  "  photo BLOB\n"
  ");\n"
  "CREATE TABLE repo_reportfmt(\n"
  "   rn INTEGER PRIMARY KEY,\n"
  "   owner TEXT,\n"
  "   title TEXT UNIQUE,\n"
  "   mtime INTEGER,\n"
  "   cols TEXT,\n"
  "   sqlcode TEXT\n"
  ");\n"
  "CREATE TABLE repo_sqlite_stat2(tbl,idx,sampleno,sample);\n"
  "CREATE TABLE repo_sqlite_stat1(tbl,idx,stat);\n"
  "CREATE TABLE repo_sqlite_stat3(tbl,idx,neq,nlt,ndlt,sample);\n"
  "CREATE TABLE repo_config(\n"
  "  name TEXT PRIMARY KEY NOT NULL,\n"
  "  value CLOB, mtime INTEGER,\n"
  "  CHECK( typeof(name)='text' AND length(name)>=1 )\n"
  ") WITHOUT ROWID;\n"
  "CREATE TABLE repo_shun(uuid PRIMARY KEY,\n"
  "  mtime INTEGER,\n"
  "  scom TEXT) WITHOUT ROWID;\n"
  "CREATE TABLE repo_concealed(\n"
  "  hash TEXT PRIMARY KEY,\n"
  "  content TEXT\n"
  ", mtime INTEGER) WITHOUT ROWID;\n"
  "CREATE TABLE repo_admin_log(\n"
  " id INTEGER PRIMARY KEY,\n"
  " time INTEGER, -- Seconds since 1970\n"
  " page TEXT,    -- path of page\n"
  " who TEXT,     -- User who made the change\n"
  "  what TEXT     -- What changed\n"
  ");\n"
  "CREATE TABLE repo_unversioned(\n"
  "  name TEXT PRIMARY KEY,\n"
  "  rcvid INTEGER,\n"
  "  mtime DATETIME,\n"
  "  hash TEXT,\n"
  "  sz INTEGER,\n"
  "  encoding INT,\n"
  "  content BLOB\n"
  ") WITHOUT ROWID;\n"
  "CREATE TABLE repo_subscriber(\n"
  "  subscriberId INTEGER PRIMARY KEY,\n"
  "  subscriberCode BLOB DEFAULT (randomblob(32)) UNIQUE,\n"
  "  semail TEXT UNIQUE COLLATE nocase,\n"
  "  suname TEXT,\n"
  "  sverified BOOLEAN DEFAULT true,\n"
  "  sdonotcall BOOLEAN,\n"
  "  sdigest BOOLEAN,\n"
  "  ssub TEXT,\n"
  "  sctime INTDATE,\n"
  "  mtime INTDATE,\n"
  "  smip TEXT\n"
  ");\n"
  "CREATE TABLE repo_pending_alert(\n"
  "  eventid TEXT PRIMARY KEY,\n"
  "  sentSep BOOLEAN DEFAULT false,\n"
  "  sentDigest BOOLEAN DEFAULT false\n"
  ", sentMod BOOLEAN DEFAULT false) WITHOUT ROWID;\n"
  "CREATE INDEX repo_delta_i1 ON repo_delta(srcid);\n"
  "CREATE INDEX repo_blob_rcvid ON repo_blob(rcvid);\n"
  "CREATE INDEX repo_subscriberUname\n"
  "  ON repo_subscriber(suname) WHERE suname IS NOT NULL;\n"
  "CREATE VIEW repo_artifact(rid,rcvid,size,atype,srcid,hash,content) AS\n"
  "     SELECT blob.rid,rcvid,size,1,srcid,uuid,content\n"
  "       FROM repo_blob LEFT JOIN repo_delta ON (blob.rid=delta.rid);\n"
  "CREATE TABLE repo_filename(\n"
  "  fnid INTEGER PRIMARY KEY,\n"
  "  name TEXT UNIQUE\n"
  ");\n"
  "CREATE TABLE repo_mlink(\n"
  "  mid INTEGER,\n"
  "  fid INTEGER,\n"
  "  pmid INTEGER,\n"
  "  pid INTEGER,\n"
  "  fnid INTEGER REFERENCES filename,\n"
  "  pfnid INTEGER,\n"
  "  mperm INTEGER,\n"
  "  isaux BOOLEAN DEFAULT 0\n"
  ");\n"
  "CREATE INDEX repo_mlink_i1 ON repo_mlink(mid);\n"
  "CREATE INDEX repo_mlink_i2 ON repo_mlink(fnid);\n"
  "CREATE INDEX repo_mlink_i3 ON repo_mlink(fid);\n"
  "CREATE INDEX repo_mlink_i4 ON repo_mlink(pid);\n"
  "CREATE TABLE repo_plink(\n"
  "  pid INTEGER REFERENCES blob,\n"
  "  cid INTEGER REFERENCES blob,\n"
  "  isprim BOOLEAN,\n"
  "  mtime DATETIME,\n"
  "  baseid INTEGER REFERENCES blob,\n"
  "  UNIQUE(pid, cid)\n"
  ");\n"
  "CREATE INDEX repo_plink_i2 ON repo_plink(cid,pid);\n"
  "CREATE TABLE repo_leaf(rid INTEGER PRIMARY KEY);\n"
  "CREATE TABLE repo_event(\n"
  "  type TEXT,\n"
  "  mtime DATETIME,\n"
  "  objid INTEGER PRIMARY KEY,\n"
  "  tagid INTEGER,\n"
  "  uid INTEGER REFERENCES user,\n"
  "  bgcolor TEXT,\n"
  "  euser TEXT,\n"
  "  user TEXT,\n"
  "  ecomment TEXT,\n"
  "  comment TEXT,\n"
  "  brief TEXT,\n"
  "  omtime DATETIME\n"
  ");\n"
  "CREATE INDEX repo_event_i1 ON repo_event(mtime);\n"
  "CREATE TABLE repo_phantom(\n"
  "  rid INTEGER PRIMARY KEY\n"
  ");\n"
  "CREATE TABLE repo_orphan(\n"
  "  rid INTEGER PRIMARY KEY,\n"
  "  baseline INTEGER\n"
  ");\n"
  "CREATE INDEX repo_orphan_baseline ON repo_orphan(baseline);\n"
  "CREATE TABLE repo_unclustered(\n"
  "  rid INTEGER PRIMARY KEY\n"
  ");\n"
  "CREATE TABLE repo_unsent(\n"
  "  rid INTEGER PRIMARY KEY\n"
  ");\n"
  "CREATE TABLE repo_tag(\n"
  "  tagid INTEGER PRIMARY KEY,\n"
  "  tagname TEXT UNIQUE\n"
  ");\n"
  "CREATE TABLE repo_tagxref(\n"
  "  tagid INTEGER REFERENCES tag,\n"
  "  tagtype INTEGER,\n"
  "  srcid INTEGER REFERENCES blob,\n"
  "  origid INTEGER REFERENCES blob,\n"
  "  value TEXT,\n"
  "  mtime TIMESTAMP,\n"
  "  rid INTEGER REFERENCE blob,\n"
  "  UNIQUE(rid, tagid)\n"
  ");\n"
  "CREATE INDEX repo_tagxref_i1 ON repo_tagxref(tagid, mtime);\n"
  "CREATE TABLE repo_backlink(\n"
  "  target TEXT,\n"
  "  srctype INT,\n"
  "  srcid INT,\n"
  "  mtime TIMESTAMP,\n"
  "  UNIQUE(target, srctype, srcid)\n"
  ");\n"
  "CREATE INDEX repo_backlink_src ON repo_backlink(srcid, srctype);\n"
  "CREATE TABLE repo_attachment(\n"
  "  attachid INTEGER PRIMARY KEY,\n"
  "  isLatest BOOLEAN DEFAULT 0,\n"
  "  mtime TIMESTAMP,\n"
  "  src TEXT,\n"
  "  target TEXT,\n"
  "  filename TEXT,\n"
  "  comment TEXT,\n"
  "  user TEXT\n"
  ");\n"
  "CREATE INDEX repo_attachment_idx1\n"
  " ON repo_attachment(target, filename, mtime);\n"
  "CREATE INDEX repo_attachment_idx2 ON repo_attachment(src);\n"
  "CREATE TABLE repo_cherrypick(\n"
  "  parentid INT,\n"
  "  childid INT,\n"
  "  isExclude BOOLEAN DEFAULT false,\n"
  "  PRIMARY KEY(parentid, childid)\n"
  ") WITHOUT ROWID;\n"
  "CREATE INDEX repo_cherrypick_cid ON repo_cherrypick(childid);\n"
  "CREATE TABLE repo_ticket(\n"
  "  -- Do not change any column that begins with tkt_\n"
  "  tkt_id INTEGER PRIMARY KEY,\n"
  "  tkt_uuid TEXT UNIQUE,\n"
  "  tkt_mtime DATE,\n"
  "  tkt_ctime DATE,\n"
  "  -- Add as many fields as required below this line\n"
  "  type TEXT,\n"
  "  status TEXT,\n"
  "  subsystem TEXT,\n"
  "  priority TEXT,\n"
  "  severity TEXT,\n"
  "  foundin TEXT,\n"
  "  private_contact TEXT,\n"
  "  resolution TEXT,\n"
  "  title TEXT,\n"
  "  comment TEXT\n"
  ");\n"
  "CREATE TABLE repo_ticketchng(\n"
  "  -- Do not change any column that begins with tkt_\n"
  "  tkt_id INTEGER REFERENCES ticket,\n"
  "  tkt_rid INTEGER REFERENCES blob,\n"
  "  tkt_mtime DATE,\n"
  "  -- Add as many fields as required below this line\n"
  "  login TEXT,\n"
  "  username TEXT,\n"
  "  mimetype TEXT,\n"
  "  icomment TEXT\n"
  ");\n"
  "CREATE INDEX repo_ticketchng_idx1 ON repo_ticketchng(tkt_id, tkt_mtime);\n"
  "CREATE TRIGGER repo_alert_trigger1\n"
  "AFTER INSERT ON repo_event BEGIN\n"
  "  INSERT INTO repo_pending_alert(eventid)\n"
  "    SELECT printf('%.1c%d',new.type,new.objid) WHERE true\n"
  "    ON CONFLICT(eventId) DO NOTHING;\n"
  "END;\n"
  "CREATE TABLE repo_vcache(\n"
  "  vid INTEGER,         -- check-in ID\n"
  "  fname TEXT,          -- filename\n"
  "  rid INTEGER,         -- artifact ID\n"
  "  PRIMARY KEY(vid,fname)\n"
  ") WITHOUT ROWID;\n"
  "CREATE TABLE localdb_vvar(\n"
  "  name TEXT PRIMARY KEY NOT NULL,\n"
  "  value CLOB,\n"
  "  CHECK( typeof(name)='text' AND length(name)>=1 )\n"
  ");\n"
  "CREATE TABLE localdb_vfile(\n"
  "  id INTEGER PRIMARY KEY,\n"
  "  vid INTEGER REFERENCES blob,\n"
  "  chnged INT DEFAULT 0,\n"
  "  deleted BOOLEAN DEFAULT 0,\n"
  "  isexe BOOLEAN,\n"
  "  islink BOOLEAN,\n"
  "  rid INTEGER,\n"
  "  mrid INTEGER,\n"
  "  mtime INTEGER,\n"
  "  pathname TEXT,\n"
  "  origname TEXT, mhash,\n"
  "  UNIQUE(pathname,vid)\n"
  ");\n"
  "CREATE TABLE localdb_sqlite_stat1(tbl,idx,stat);\n"
  "CREATE TABLE localdb_vcache(\n"
  "  vid INTEGER,         -- check-in ID\n"
  "  fname TEXT,          -- filename\n"
  "  rid INTEGER,         -- artifact ID\n"
  "  PRIMARY KEY(vid,fname)\n"
  ") WITHOUT ROWID;\n"
  "CREATE TABLE localdb_stash(\n"
  "  stashid INTEGER PRIMARY KEY,\n"
  "  vid INTEGER,\n"
  "  hash TEXT,\n"
  "  comment TEXT,\n"
  "  ctime TIMESTAMP\n"
  ");\n"
  "CREATE TABLE localdb_stashfile(\n"
  "  stashid INTEGER REFERENCES stash,\n"
  "  isAdded BOOLEAN,\n"
  "  isRemoved BOOLEAN,\n"
  "  isExec BOOLEAN,\n"
  "  isLink BOOLEAN,\n"
  "  rid INTEGER,\n"
  "  hash TEXT,\n"
  "  origname TEXT,\n"
  "  newname TEXT,\n"
  "  delta BLOB,\n"
  "  PRIMARY KEY(newname, stashid)\n"
  ");\n"
  "CREATE TABLE localdb_vmerge(\n"
  "  id INTEGER REFERENCES vfile,\n"
  "  merge INTEGER,\n"
  "  mhash TEXT\n"
  ");\n"
  "CREATE UNIQUE INDEX localdb_vmergex1 ON localdb_vmerge(id,mhash);\n"
  "CREATE TRIGGER localdb_vmerge_ck1 AFTER INSERT ON localdb_vmerge\n"
  "WHEN new.mhash IS NULL BEGIN\n"
  "  SELECT raise(FAIL,\n"
  "  'trying to update a newer checkout with an older version of Fossil');\n"
  "END;\n"
  "CREATE TABLE configdb_global_config(\n"
  "  name TEXT PRIMARY KEY,\n"
  "  value TEXT\n"
  ");\n"
  "CREATE TABLE configdb_sqlite_stat1(tbl,idx,stat);\n"
;

#ifdef __linux__
#include <sys/types.h>
#include <unistd.h>

/*
** Attempt to display I/O stats on Linux using /proc/PID/io
*/
static void displayLinuxIoStats(FILE *out){
  FILE *in;
  char z[200];
  sqlite3_snprintf(sizeof(z), z, "/proc/%d/io", getpid());
  in = fopen(z, "rb");
  if( in==0 ) return;
  while( fgets(z, sizeof(z), in)!=0 ){
    static const struct {
      const char *zPattern;
      const char *zDesc;
    } aTrans[] = {
      { "rchar: ",                  "Bytes received by read():" },
      { "wchar: ",                  "Bytes sent to write():"    },
      { "syscr: ",                  "Read() system calls:"      },
      { "syscw: ",                  "Write() system calls:"     },
      { "read_bytes: ",             "Bytes rcvd from storage:"  },
      { "write_bytes: ",            "Bytes sent to storage:"    },
      { "cancelled_write_bytes: ",  "Cancelled write bytes:"    },
    };
    int i;
    for(i=0; i<sizeof(aTrans)/sizeof(aTrans[0]); i++){
      int n = (int)strlen(aTrans[i].zPattern);
      if( strncmp(aTrans[i].zPattern, z, n)==0 ){
        fprintf(out, "-- %-28s %s", aTrans[i].zDesc, &z[n]);
        break;
      }
    }
  }
  fclose(in);
}   
#endif

/*
** Return the value of a hexadecimal digit.  Return -1 if the input
** is not a hex digit.
*/
static int hexDigitValue(char c){
  if( c>='0' && c<='9' ) return c - '0';
  if( c>='a' && c<='f' ) return c - 'a' + 10;
  if( c>='A' && c<='F' ) return c - 'A' + 10;
  return -1;
}

/*
** Interpret zArg as an integer value, possibly with suffixes.
*/
static int integerValue(const char *zArg){
  sqlite3_int64 v = 0;
  static const struct { char *zSuffix; int iMult; } aMult[] = {
    { "KiB", 1024 },
    { "MiB", 1024*1024 },
    { "GiB", 1024*1024*1024 },
    { "KB",  1000 },
    { "MB",  1000000 },
    { "GB",  1000000000 },
    { "K",   1000 },
    { "M",   1000000 },
    { "G",   1000000000 },
  };
  int i;
  int isNeg = 0;
  if( zArg[0]=='-' ){
    isNeg = 1;
    zArg++;
  }else if( zArg[0]=='+' ){
    zArg++;
  }
  if( zArg[0]=='0' && zArg[1]=='x' ){
    int x;
    zArg += 2;
    while( (x = hexDigitValue(zArg[0]))>=0 ){
      v = (v<<4) + x;
      zArg++;
    }
  }else{
    while( isdigit(zArg[0]) ){
      v = v*10 + zArg[0] - '0';
      zArg++;
    }
  }
  for(i=0; i<sizeof(aMult)/sizeof(aMult[0]); i++){
    if( sqlite3_stricmp(aMult[i].zSuffix, zArg)==0 ){
      v *= aMult[i].iMult;
      break;
    }
  }
  if( v>0x7fffffff ){
    printf("ERROR: parameter too large - max 2147483648\n");
    exit(1);
  }
  return (int)(isNeg? -v : v);
}


int main(int argc, char **argv){
  const char *zCmd = 0;
  int i;
  int bAutovac = 0;
  int showStats = 0;
  const char *zDbName = "./startup.db";
  int nHeap = 0;
  int mnHeap = 0;

  for(i=1; i<argc; i++){
    const char *z = argv[i];
    if( z[0]!='-' ){
      if( zCmd ){
        usage(argv[0]);
      }
      zCmd = z;
      continue;
    }
    if( z[1]=='-' ) z++;
    if( strcmp(z, "-autovacuum")==0 ){
      bAutovac = 1;
    }else
    if( strcmp(z, "-dbname")==0 ){
      if( i==argc-1 ){
        printf("ERROR: missing argument on \"%s\"\n", argv[0]);
        exit(1);
      }
      zDbName = argv[++i];
    }else
    if( strcmp(z,"-heap")==0 ){
      if( i>=argc-2 ){
        printf("ERROR: missing arguments on %s\n", argv[i]);
        exit(1);
      }
      nHeap = integerValue(argv[i+1]);
      mnHeap = integerValue(argv[i+2]);
      i += 2;
    }else
    if( strcmp(z,"-stats")==0 ){
       showStats = 1;
    }else
    {
      printf("ERROR: unknown option \"%s\"\n", argv[i]);
      usage(argv[0]);
    }
  }
  if( zCmd==0 ){
    printf("ERROR: no COMMAND specified\n");
    usage(argv[0]);
  }
  if( strcmp(zCmd, "run")==0 ){
    sqlite3 *db;
    int rc;
    char *zErr = 0;
    void *pHeap = 0;
    if( nHeap>0 ){
      pHeap = malloc( nHeap );
      if( pHeap==0 ){
        printf("ERROR: cannot allocate %d-byte heap\n", nHeap);
        exit(1);
      }
      rc = sqlite3_config(SQLITE_CONFIG_HEAP, pHeap, nHeap, mnHeap);
      if( rc ){
        printf("ERROR: heap configuration failed: %d\n", rc);
        exit(1);
      }
    }
    rc = sqlite3_open(zDbName, &db);
    if( rc ){
      printf("SQLite error: %s\n", sqlite3_errmsg(db));
    }else{
      sqlite3_exec(db, "PRAGMA synchronous", 0, 0, &zErr);
    }
    if( zErr ){
      printf("ERROR: %s\n", zErr);
      sqlite3_free(zErr);
    }
    if( showStats ){
      int iCur, iHi;
      sqlite3_db_status(db, SQLITE_DBSTATUS_LOOKASIDE_USED, &iCur, &iHi, 0);
      printf("-- Lookaside Slots Used:        %d (max %d)\n", iCur,iHi);
      sqlite3_db_status(db, SQLITE_DBSTATUS_LOOKASIDE_HIT, &iCur, &iHi, 0);
      printf("-- Successful lookasides:       %d\n", iHi);
      sqlite3_db_status(db, SQLITE_DBSTATUS_LOOKASIDE_MISS_SIZE, &iCur,&iHi,0);
      printf("-- Lookaside size faults:       %d\n", iHi);
      sqlite3_db_status(db, SQLITE_DBSTATUS_LOOKASIDE_MISS_FULL, &iCur,&iHi,0);
      printf("-- Lookaside OOM faults:        %d\n", iHi);
      sqlite3_db_status(db, SQLITE_DBSTATUS_CACHE_USED, &iCur, &iHi, 0);
      printf("-- Pager Heap Usage:            %d bytes\n", iCur);
      sqlite3_db_status(db, SQLITE_DBSTATUS_CACHE_HIT, &iCur, &iHi, 1);
      printf("-- Page cache hits:             %d\n", iCur);
      sqlite3_db_status(db, SQLITE_DBSTATUS_CACHE_MISS, &iCur, &iHi, 1);
      printf("-- Page cache misses:           %d\n", iCur);
      sqlite3_db_status(db, SQLITE_DBSTATUS_CACHE_WRITE, &iCur, &iHi, 1);
      printf("-- Page cache writes:           %d\n", iCur); 
      sqlite3_db_status(db, SQLITE_DBSTATUS_SCHEMA_USED, &iCur, &iHi, 0);
      printf("-- Schema Heap Usage:           %d bytes\n", iCur); 
      sqlite3_db_status(db, SQLITE_DBSTATUS_STMT_USED, &iCur, &iHi, 0);
      printf("-- Statement Heap Usage:        %d bytes\n", iCur); 
    }
    sqlite3_close(db);
    free(pHeap);
    /* Global memory usage statistics printed after the database connection
    ** has closed.  Memory usage should be zero at this point. */
    if( showStats ){
      int iCur, iHi;
      sqlite3_status(SQLITE_STATUS_MEMORY_USED, &iCur, &iHi, 0);
      printf("-- Memory Used (bytes):         %d (max %d)\n", iCur,iHi);
      sqlite3_status(SQLITE_STATUS_MALLOC_COUNT, &iCur, &iHi, 0);
      printf("-- Outstanding Allocations:     %d (max %d)\n", iCur,iHi);
      sqlite3_status(SQLITE_STATUS_PAGECACHE_OVERFLOW, &iCur, &iHi, 0);
      printf("-- Pcache Overflow Bytes:       %d (max %d)\n", iCur,iHi);
      sqlite3_status(SQLITE_STATUS_MALLOC_SIZE, &iCur, &iHi, 0);
      printf("-- Largest Allocation:          %d bytes\n",iHi);
      sqlite3_status(SQLITE_STATUS_PAGECACHE_SIZE, &iCur, &iHi, 0);
      printf("-- Largest Pcache Allocation:   %d bytes\n",iHi);
#ifdef __linux__
      displayLinuxIoStats(stdout);
#endif
    }
    return 0;
  }
  if( strcmp(zCmd, "init")==0 ){
    sqlite3 *db;
    char *zAux;
    char *zErr = 0;
    int rc;
    unlink(zDbName);
    zAux = sqlite3_mprintf("%s-journal", zDbName);
    unlink(zAux);
    sqlite3_free(zAux);
    zAux = sqlite3_mprintf("%s-wal", zDbName);
    unlink(zAux);
    sqlite3_free(zAux);
    rc = sqlite3_open(zDbName, &db);
    if( rc ){
      printf("SQLite error: %s\n", sqlite3_errmsg(db));
    }else{
      sqlite3_exec(db, "BEGIN", 0, 0, 0);
      sqlite3_exec(db, zTestSchema, 0, 0, &zErr);
      sqlite3_exec(db, "COMMIT", 0, 0, 0);
    }
    if( zErr ){
      printf("ERROR: %s\n", zErr);
      sqlite3_free(zErr);
    }
    sqlite3_close(db);
    return 0;

  }
}

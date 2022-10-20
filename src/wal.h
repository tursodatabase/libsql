/*
** 2010 February 1
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
*************************************************************************
** This header file defines the interface to the write-ahead logging 
** system. Refer to the comments below and the header comment attached to 
** the implementation of each function in log.c for further details.
*/

#ifndef SQLITE_WAL_H
#define SQLITE_WAL_H

#include "sqliteInt.h"

/* Macros for extracting appropriate sync flags for either transaction
** commits (WAL_SYNC_FLAGS(X)) or for checkpoint ops (CKPT_SYNC_FLAGS(X)):
*/
#define WAL_SYNC_FLAGS(X)   ((X)&0x03)
#define CKPT_SYNC_FLAGS(X)  (((X)>>2)&0x03)

#define WAL_SAVEPOINT_NDATA 4

/* Connection to a write-ahead log (WAL) file. 
** There is one object of this type for each pager. 
*/
typedef struct Wal Wal;

typedef struct libsql_wal_methods {
  /* Open and close a connection to a write-ahead log. */
  int (*xOpen)(sqlite3_vfs*, sqlite3_file* , const char*, int no_shm_mode, i64 max_size, struct libsql_wal_methods*, Wal**);
  int (*xClose)(Wal*, sqlite3* db, int sync_flags, int nBuf, u8 *zBuf);

  /* Set the limiting size of a WAL file. */
  void (*xLimit)(Wal*, i64 limit);

  /* Used by readers to open (lock) and close (unlock) a snapshot.  A 
  ** snapshot is like a read-transaction.  It is the state of the database
  ** at an instant in time.  sqlite3WalOpenSnapshot gets a read lock and
  ** preserves the current state even if the other threads or processes
  ** write to or checkpoint the WAL.  sqlite3WalCloseSnapshot() closes the
  ** transaction and releases the lock.
  */
  int (*xBeginReadTransaction)(Wal *, int *);
  void (*xEndReadTransaction)(Wal *);

  /* Read a page from the write-ahead log, if it is present. */
  int (*xFindFrame)(Wal *, Pgno, u32 *);
  int (*xReadFrame)(Wal *, u32, int, u8 *);

  /* If the WAL is not empty, return the size of the database. */
  Pgno (*xDbsize)(Wal *pWal);

  /* Obtain or release the WRITER lock. */
  int (*xBeginWriteTransaction)(Wal *pWal);
  int (*xEndWriteTransaction)(Wal *pWal);

  /* Undo any frames written (but not committed) to the log */
  int (*xUndo)(Wal *pWal, int (*xUndo)(void *, Pgno), void *pUndoCtx);

  /* Return an integer that records the current (uncommitted) write
  ** position in the WAL */
  void (*xSavepoint)(Wal *pWal, u32 *aWalData);

  /* Move the write position of the WAL back to iFrame.  Called in
  ** response to a ROLLBACK TO command. */
  int (*xSavepointUndo)(Wal *pWal, u32 *aWalData);

  /* Write a frame or frames to the log. */
  int (*xFrames)(Wal *pWal, int, PgHdr *, Pgno, int, int);

  /* Copy pages from the log to the database file */ 
  int (*xCheckpoint)(
    Wal *pWal,                      /* Write-ahead log connection */
    sqlite3 *db,                    /* Check this handle's interrupt flag */
    int eMode,                      /* One of PASSIVE, FULL and RESTART */
    int (*xBusy)(void*),            /* Function to call when busy */
    void *pBusyArg,                 /* Context argument for xBusyHandler */
    int sync_flags,                 /* Flags to sync db file with (or 0) */
    int nBuf,                       /* Size of buffer nBuf */
    u8 *zBuf,                       /* Temporary buffer to use */
    int *pnLog,                     /* OUT: Number of frames in WAL */
    int *pnCkpt                     /* OUT: Number of backfilled frames in WAL */
  );

  /* Return the value to pass to a sqlite3_wal_hook callback, the
  ** number of frames in the WAL at the point of the last commit since
  ** sqlite3WalCallback() was called.  If no commits have occurred since
  ** the last call, then return 0.
  */
  int (*xCallback)(Wal *pWal);

  /* Tell the wal layer that an EXCLUSIVE lock has been obtained (or released)
  ** by the pager layer on the database file.
  */
  int (*xExclusiveMode)(Wal *pWal, int op);

  /* Return true if the argument is non-NULL and the WAL module is using
  ** heap-memory for the wal-index. Otherwise, if the argument is NULL or the
  ** WAL module is using shared-memory, return false. 
  */
  int (*xHeapMemory)(Wal *pWal);

#ifdef SQLITE_ENABLE_SNAPSHOT
  int (*xSnapshotGet)(Wal *pWal, sqlite3_snapshot **ppSnapshot);
  void (*xSnapshotOpen)(Wal *pWal, sqlite3_snapshot *pSnapshot);
  int (*xSnapshotRecover)(Wal *pWal);
  int (*xSnapshotCheck)(Wal *pWal, sqlite3_snapshot *pSnapshot);
  void (*xSnapshotUnlock)(Wal *pWal);
#endif

#ifdef SQLITE_ENABLE_ZIPVFS
  /* If the WAL file is not empty, return the number of bytes of content
  ** stored in each frame (i.e. the db page-size when the WAL was created).
  */
  int (*xFramesize)(Wal *pWal);
#endif

  /* Return the sqlite3_file object for the WAL file */
  sqlite3_file *(*xFile)(Wal *pWal);

#ifdef SQLITE_ENABLE_SETLK_TIMEOUT
  int (*xWriteLock)(Wal *pWal, int bLock);
#endif

  void (*xDb)(Wal *pWal, sqlite3 *db);

  const char *zName;
  struct libsql_wal_methods *pNext;
} libsql_wal_methods;

libsql_wal_methods* libsql_wal_methods_find(const char *zName);

/* Object declarations */
typedef struct WalIndexHdr WalIndexHdr;
typedef struct WalIterator WalIterator;
typedef struct WalCkptInfo WalCkptInfo;


/*
** The following object holds a copy of the wal-index header content.
**
** The actual header in the wal-index consists of two copies of this
** object followed by one instance of the WalCkptInfo object.
** For all versions of SQLite through 3.10.0 and probably beyond,
** the locking bytes (WalCkptInfo.aLock) start at offset 120 and
** the total header size is 136 bytes.
**
** The szPage value can be any power of 2 between 512 and 32768, inclusive.
** Or it can be 1 to represent a 65536-byte page.  The latter case was
** added in 3.7.1 when support for 64K pages was added.  
*/
struct WalIndexHdr {
  u32 iVersion;                   /* Wal-index version */
  u32 unused;                     /* Unused (padding) field */
  u32 iChange;                    /* Counter incremented each transaction */
  u8 isInit;                      /* 1 when initialized */
  u8 bigEndCksum;                 /* True if checksums in WAL are big-endian */
  u16 szPage;                     /* Database page size in bytes. 1==64K */
  u32 mxFrame;                    /* Index of last valid frame in the WAL */
  u32 nPage;                      /* Size of database in pages */
  u32 aFrameCksum[2];             /* Checksum of last frame in log */
  u32 aSalt[2];                   /* Two salt values copied from WAL header */
  u32 aCksum[2];                  /* Checksum over all prior fields */
};

/*
** An open write-ahead log file is represented by an instance of the
** following object.
*/
struct Wal {
  sqlite3_vfs *pVfs;         /* The VFS used to create pDbFd */
  sqlite3_file *pDbFd;       /* File handle for the database file */
  sqlite3_file *pWalFd;      /* File handle for WAL file */
  u32 iCallback;             /* Value to pass to log callback (or 0) */
  i64 mxWalSize;             /* Truncate WAL to this size upon reset */
  int nWiData;               /* Size of array apWiData */
  int szFirstBlock;          /* Size of first block written to WAL file */
  volatile u32 **apWiData;   /* Pointer to wal-index content in memory */
  u32 szPage;                /* Database page size */
  i16 readLock;              /* Which read lock is being held.  -1 for none */
  u8 syncFlags;              /* Flags to use to sync header writes */
  u8 exclusiveMode;          /* Non-zero if connection is in exclusive mode */
  u8 writeLock;              /* True if in a write transaction */
  u8 ckptLock;               /* True if holding a checkpoint lock */
  u8 readOnly;               /* WAL_RDWR, WAL_RDONLY, or WAL_SHM_RDONLY */
  u8 truncateOnCommit;       /* True to truncate WAL file on commit */
  u8 syncHeader;             /* Fsync the WAL header if true */
  u8 padToSectorBoundary;    /* Pad transactions out to the next sector */
  u8 bShmUnreliable;         /* SHM content is read-only and unreliable */
  WalIndexHdr hdr;           /* Wal-index header for current transaction */
  u32 minFrame;              /* Ignore wal frames before this one */
  u32 iReCksum;              /* On commit, recalculate checksums from here */
  const char *zWalName;      /* Name of WAL file */
  u32 nCkpt;                 /* Checkpoint sequence counter in the wal-header */
#ifdef SQLITE_DEBUG
  u8 lockError;              /* True if a locking error has occurred */
#endif
#ifdef SQLITE_ENABLE_SNAPSHOT
  WalIndexHdr *pSnapshot;    /* Start transaction here if not NULL */
#endif
#ifdef SQLITE_ENABLE_SETLK_TIMEOUT
  sqlite3 *db;
#endif
  libsql_wal_methods *pMethods; /* Virtual methods for interacting with WAL */;
};

#endif /* SQLITE_WAL_H */

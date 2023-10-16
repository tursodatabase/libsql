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

#include "sqlite3.h"
#include "page_header.h"

/* Macros for extracting appropriate sync flags for either transaction
** commits (WAL_SYNC_FLAGS(X)) or for checkpoint ops (CKPT_SYNC_FLAGS(X)):
*/
#define WAL_SYNC_FLAGS(X)   ((X)&0x03)
#define CKPT_SYNC_FLAGS(X)  (((X)>>2)&0x03)

#define WAL_SAVEPOINT_NDATA 4

/* Connection to a write-ahead log (WAL) file. 
** There is one object of this type for each pager. 
*/
typedef struct libsql_wal libsql_wal;

typedef struct libsql_wal_methods {
  int iVersion; /* Current version is 1, versioning is here for backward compatibility */
  /* Open and close a connection to a write-ahead log. */
  int (*xOpen)(sqlite3_vfs*, sqlite3_file* , const char*, int no_shm_mode, long long max_size, struct libsql_wal_methods*, libsql_wal**);
  int (*xClose)(libsql_wal*, sqlite3* db, int sync_flags, int nBuf, unsigned char *zBuf);

  /* Set the limiting size of a WAL file. */
  void (*xLimit)(libsql_wal*, long long limit);

  /* Used by readers to open (lock) and close (unlock) a snapshot.  A 
  ** snapshot is like a read-transaction.  It is the state of the database
  ** at an instant in time.  sqlite3WalOpenSnapshot gets a read lock and
  ** preserves the current state even if the other threads or processes
  ** write to or checkpoint the WAL.  sqlite3WalCloseSnapshot() closes the
  ** transaction and releases the lock.
  */
  int (*xBeginReadTransaction)(libsql_wal *, int *);
  void (*xEndReadTransaction)(libsql_wal *);

  /* Read a page from the write-ahead log, if it is present. */
  int (*xFindFrame)(libsql_wal *, unsigned int, unsigned int *);
  int (*xReadFrame)(libsql_wal *, unsigned int, int, unsigned char *);

  /* If the WAL is not empty, return the size of the database. */
  unsigned int (*xDbsize)(libsql_wal *pWal);

  /* Obtain or release the WRITER lock. */
  int (*xBeginWriteTransaction)(libsql_wal *pWal);
  int (*xEndWriteTransaction)(libsql_wal *pWal);

  /* Undo any frames written (but not committed) to the log */
  int (*xUndo)(libsql_wal *pWal, int (*xUndo)(void *, unsigned int), void *pUndoCtx);

  /* Return an integer that records the current (uncommitted) write
  ** position in the WAL */
  void (*xSavepoint)(libsql_wal *pWal, unsigned int *aWalData);

  /* Move the write position of the WAL back to iFrame.  Called in
  ** response to a ROLLBACK TO command. */
  int (*xSavepointUndo)(libsql_wal *pWal, unsigned int *aWalData);

  /* Write a frame or frames to the log. */
  int (*xFrames)(libsql_wal *pWal, int, libsql_pghdr *, unsigned int, int, int);

  /* Copy pages from the log to the database file */ 
  int (*xCheckpoint)(
    libsql_wal *pWal,                      /* Write-ahead log connection */
    sqlite3 *db,                    /* Check this handle's interrupt flag */
    int eMode,                      /* One of PASSIVE, FULL and RESTART */
    int (*xBusy)(void*),            /* Function to call when busy */
    void *pBusyArg,                 /* Context argument for xBusyHandler */
    int sync_flags,                 /* Flags to sync db file with (or 0) */
    int nBuf,                       /* Size of buffer nBuf */
    unsigned char *zBuf,                       /* Temporary buffer to use */
    int *pnLog,                     /* OUT: Number of frames in WAL */
    int *pnCkpt                     /* OUT: Number of backfilled frames in WAL */
  );

  /* Return the value to pass to a sqlite3_wal_hook callback, the
  ** number of frames in the WAL at the point of the last commit since
  ** sqlite3WalCallback() was called.  If no commits have occurred since
  ** the last call, then return 0.
  */
  int (*xCallback)(libsql_wal *pWal);

  /* Tell the wal layer that an EXCLUSIVE lock has been obtained (or released)
  ** by the pager layer on the database file.
  */
  int (*xExclusiveMode)(libsql_wal *pWal, int op);

  /* Return true if the argument is non-NULL and the WAL module is using
  ** heap-memory for the wal-index. Otherwise, if the argument is NULL or the
  ** WAL module is using shared-memory, return false. 
  */
  int (*xHeapMemory)(libsql_wal *pWal);

  // Only needed with SQLITE_ENABLE_SNAPSHOT, but part of the ABI
  int (*xSnapshotGet)(libsql_wal *pWal, sqlite3_snapshot **ppSnapshot);
  void (*xSnapshotOpen)(libsql_wal *pWal, sqlite3_snapshot *pSnapshot);
  int (*xSnapshotRecover)(libsql_wal *pWal);
  int (*xSnapshotCheck)(libsql_wal *pWal, sqlite3_snapshot *pSnapshot);
  void (*xSnapshotUnlock)(libsql_wal *pWal);

  // Only needed with SQLITE_ENABLE_ZIPVFS, but part of the ABI
  /* If the WAL file is not empty, return the number of bytes of content
  ** stored in each frame (i.e. the db page-size when the WAL was created).
  */
  int (*xFramesize)(libsql_wal *pWal);


  /* Return the sqlite3_file object for the WAL file */
  sqlite3_file *(*xFile)(libsql_wal *pWal);

  // Only needed with  SQLITE_ENABLE_SETLK_TIMEOUT
  int (*xWriteLock)(libsql_wal *pWal, int bLock);

  void (*xDb)(libsql_wal *pWal, sqlite3 *db);

  /* Return the WAL pathname length based on the owning pager's pathname len.
  ** For WAL implementations not based on a single file, 0 should be returned. */
  int (*xPathnameLen)(int origPathname);

  /* Get the WAL pathname to given buffer. Assumes that the buffer can hold
  ** at least xPathnameLen bytes. For WAL implementations not based on a single file,
  ** this operation can safely be a no-op.
  ** */
  void (*xGetWalPathname)(char *buf, const char *orig, int orig_len);

  /*
  ** This optional callback gets called before the main database file which owns
  ** the WAL file is open. It is a good place for initialization routines, as WAL
  ** is otherwise open lazily.
  */
  int (*xPreMainDbOpen)(libsql_wal_methods *methods, const char *main_db_path);

  /* True if the implementation relies on shared memory routines (e.g. locks) */
  int bUsesShm;

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
  unsigned int iVersion;                   /* Wal-index version */
  unsigned int unused;                     /* Unused (padding) field */
  unsigned int iChange;                    /* Counter incremented each transaction */
  unsigned char isInit;                    /* 1 when initialized */
  unsigned char bigEndCksum;               /* True if checksums in WAL are big-endian */
  unsigned short szPage;                   /* Database page size in bytes. 1==64K */
  unsigned int mxFrame;                    /* Index of last valid frame in the WAL */
  unsigned int nPage;                      /* Size of database in pages */
  unsigned int aFrameCksum[2];             /* Checksum of last frame in log */
  unsigned int aSalt[2];                   /* Two salt values copied from WAL header */
  unsigned int aCksum[2];                  /* Checksum over all prior fields */
};

/*
** An open write-ahead log file is represented by an instance of the
** following object.
*/
struct libsql_wal {
  sqlite3_vfs *pVfs;                  /* The VFS used to create pDbFd */
  sqlite3_file *pDbFd;                /* File handle for the database file */
  sqlite3_file *pWalFd;               /* File handle for WAL file */
  unsigned int iCallback;             /* Value to pass to log callback (or 0) */
  long long mxWalSize;                     /* Truncate WAL to this size upon reset */
  int nWiData;                        /* Size of array apWiData */
  int szFirstBlock;                   /* Size of first block written to WAL file */
  volatile unsigned int **apWiData;   /* Pointer to wal-index content in memory */
  unsigned int szPage;                /* Database page size */
  short readLock;                     /* Which read lock is being held.  -1 for none */
  unsigned char syncFlags;            /* Flags to use to sync header writes */
  unsigned char exclusiveMode;        /* Non-zero if connection is in exclusive mode */
  unsigned char writeLock;            /* True if in a write transaction */
  unsigned char ckptLock;             /* True if holding a checkpoint lock */
  unsigned char readOnly;             /* WAL_RDWR, WAL_RDONLY, or WAL_SHM_RDONLY */
  unsigned char truncateOnCommit;     /* True to truncate WAL file on commit */
  unsigned char syncHeader;           /* Fsync the WAL header if true */
  unsigned char padToSectorBoundary;  /* Pad transactions out to the next sector */
  unsigned char bShmUnreliable;       /* SHM content is read-only and unreliable */
  WalIndexHdr hdr;                    /* Wal-index header for current transaction */
  unsigned int minFrame;              /* Ignore wal frames before this one */
  unsigned int iReCksum;              /* On commit, recalculate checksums from here */
  const char *zWalName;               /* Name of WAL file */
  unsigned int nCkpt;                 /* Checkpoint sequence counter in the wal-header */
  unsigned char lockError;            /* True if a locking error has occurred */
  WalIndexHdr *pSnapshot;             /* Start transaction here if not NULL */
  sqlite3 *db;
  libsql_wal_methods *pMethods;       /* Virtual methods for interacting with WAL */
  void *pMethodsData;                 /* Optional context for private use of libsql_wal_methods */
};

typedef struct libsql_wal libsql_wal;

#endif /* SQLITE_WAL_H */

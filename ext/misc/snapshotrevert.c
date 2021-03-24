/*
** 2021 March 25
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
** Code to revert a database to a snapshot. The procedure for reverting a 
** live database to the supplied snapshot is:
**
**   1. Open snapshot for reading.
**   2. Take exclusive CHECKPOINTER lock. 
**   3.  Take exclusive WRITER lock.
**   4.   Clobber the current wal-index header with the snapshot.
**   5.   Set nBackfill to 0. nBackfillAttempted is not modified.
**   6.   Truncate wal file.
**   7.  Release write lock.
**   8. Release checkpoint lock.
**   9. Close snapshot transaction.
**
** This extension exports a single API function:
**
**     int sqlite3_snapshot_revert(
**       sqlite3 *db, 
**       const char *zDb, 
**       sqlite3_snapshot *pSnap
**     );
**
** See comments above the implementation of this function below for details.
*/
#include <sqlite3.h>

#include <string.h>
#include <assert.h>

#if !defined(SQLITE_TEST) || defined(SQLITE_ENABLE_SNAPSHOT)

/*
** Values for the eLock parameter accepted by snapshotRevertLock() and 
** snapshotRevertUnlock().
*/
#define SNAPSHOT_REVERT_CHECKPOINTER  2
#define SNAPSHOT_REVERT_WRITER        0

static int snapshotRevertLock(sqlite3_file *pFd, int eLock){
  int f = SQLITE_SHM_LOCK | SQLITE_SHM_EXCLUSIVE;
  return pFd->pMethods->xShmLock(pFd, eLock, 1, f);
}

static int snapshotRevertUnlock(sqlite3_file *pFd, int eLock){
  int f = SQLITE_SHM_UNLOCK | SQLITE_SHM_EXCLUSIVE;
  return pFd->pMethods->xShmLock(pFd, eLock, 1, f);
}

/*
** Revert database zDb of connection db to the state it was in when snapshot
** pSnap was taken. The database handle must be in auto-commit mode and
** not have an open read or write transction on zDb when this function is
** called.
**
** This function uses normal SQLite locks to ensure that the database is
** not corrupted by a simultaneous writer or checkpointer. However, the
** effects of a successful call to this function on readers that are
** reading from a snapshot newer than the snapshot supplied as the 
** third argument are undefined.
**
** Return SQLITE_OK if successful, or an SQLite error code otherwise.
*/
int sqlite3_snapshot_revert(
  sqlite3 *db, 
  const char *zDb, 
  sqlite3_snapshot *pSnap
){
  sqlite3_file *pDbFd = 0;
  sqlite3_file *pWalFd = 0;
  int rc;
  volatile void *pShm = 0;
  sqlite3_stmt *pCommit = 0;
  int nLock = 0;                  /* Successful snapshotRevertLock() calls */

  /* Put the db handle in non-auto-commit mode, as required by the
  ** sqlite3_snapshot_open() API.
  **
  ** Also prepare a "COMMIT" command to end the transaction. Such a VM does not
  ** need to allocate memory or do anything else that is likely to fail,
  ** so we ignore the error code when it is eventually executed and assume
  ** that the transaction was successfully closed.  */
  rc = sqlite3_prepare_v2(db, "COMMIT", -1, &pCommit, 0);
  if( rc==SQLITE_OK ){
    rc = sqlite3_exec(db, "BEGIN", 0, 0, 0);
  }
  if( rc!=SQLITE_OK ){
    sqlite3_finalize(pCommit);
    return rc;
  }

  /* 1. Open snapshot for reading */
  rc = sqlite3_snapshot_open(db, zDb, pSnap);

  /* Obtain pointers to the database file handle, the shared-memory mapping,
  ** and the wal file handle.  */
  if( rc==SQLITE_OK ){
    const int op = SQLITE_FCNTL_FILE_POINTER;
    rc = sqlite3_file_control(db, zDb, op, (void*)&pDbFd);
  }
  if( rc==SQLITE_OK ){
    rc = pDbFd->pMethods->xShmMap(pDbFd, 0, 32*1024, 1, &pShm);
  }
  if( rc==SQLITE_OK ){
    const int op = SQLITE_FCNTL_JOURNAL_POINTER;
    rc = sqlite3_file_control(db, zDb, op, (void*)&pWalFd);
  }

  /* 2. Take exclusive CHECKPOINTER lock */
  if( rc==SQLITE_OK ){
    rc = snapshotRevertLock(pDbFd, SNAPSHOT_REVERT_CHECKPOINTER);
    if( rc==SQLITE_OK ) nLock = 1;
  }

  /* 3. Take exclusive WRITER lock */
  if( rc==SQLITE_OK ){
    rc = snapshotRevertLock(pDbFd, SNAPSHOT_REVERT_WRITER);
    if( rc==SQLITE_OK ) nLock = 2;
  }

  if( rc==SQLITE_OK ){
    /* Constants from https://www.sqlite.org/walformat.html#walidxfmt */
    const int nWalHdrSz = 32;     /* Size of wal file header */
    const int nIdxHdrSz = 48;     /* Size of each WalIndexHdr */
    const int nFrameHdrSz = 24;   /* Size of each frame header */
    const int iHdrOff1 = 0;       /* Offset of first WalIndexHdr */
    const int iHdrOff2 = 48;      /* Offset of second WalIndexHdr */
    const int iBackfillOff = 96;  /* offset of 32-bit nBackfill value  */
    const int iPgszOff = 14;      /* Offset of 16-bit page-size value */
    const int iMxFrameOff = 16;   /* Offset of 32-bit mxFrame value */

    unsigned char *a = (unsigned char*)pShm;
    int pgsz;                     /* Database page size */
    int mxFrame;                  /* Valid frames in wal file after revert */
    sqlite3_int64 szWal;          /* Size in bytes to truncate wal file to */

    /* 4. Clobber the current wal-index header with the snapshot. */
    memcpy(&a[iHdrOff1], pSnap, nIdxHdrSz);
    memcpy(&a[iHdrOff2], pSnap, nIdxHdrSz);

    /* 5. Set nBackfill to 0. nBackfillAttempted is not modified. */
    *(int*)&a[iBackfillOff] = 0;

    /* 6. Truncate the wal file */
    assert( sizeof(unsigned short int)==2 );
    pgsz = *(unsigned short int*)&a[iPgszOff];
    if( pgsz==1 ) pgsz = 65536;
    mxFrame = *(int*)&a[iMxFrameOff];
    szWal = (sqlite3_int64)mxFrame * (pgsz + nFrameHdrSz) + nWalHdrSz;
    rc = pWalFd->pMethods->xTruncate(pWalFd, szWal);
  }

  /* Steps 8 and 9 - drop locks if they were acquired */
  if( nLock==2 ) snapshotRevertUnlock(pDbFd, SNAPSHOT_REVERT_WRITER);
  if( nLock>0 ) snapshotRevertUnlock(pDbFd, SNAPSHOT_REVERT_CHECKPOINTER);

  /* End the snapshot transaction, if one was opened. */
  sqlite3_step(pCommit);
  sqlite3_finalize(pCommit);

  return rc;
}

#endif /* !defined(SQLITE_TEST) || defined(SQLITE_ENABLE_SNAPSHOT) */

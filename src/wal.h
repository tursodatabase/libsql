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

#ifndef _WAL_H_
#define _WAL_H_

#include "sqliteInt.h"

#ifdef SQLITE_OMIT_WAL
# define sqlite3WalOpen(x,y,z)                 0
# define sqlite3WalClose(w,x,y,z)              0
# define sqlite3WalBeginReadTransaction(y,z)   0
# define sqlite3WalEndReadTransaction(z)
# define sqlite3WalRead(v,w,x,y,z)             0
# define sqlite3WalDbsize(y)                   0
# define sqlite3WalBeginWriteTransaction(y)    0
# define sqlite3WalEndWriteTransaction(x)      0
# define sqlite3WalUndo(x,y,z)                 0
# define sqlite3WalSavepoint(y,z)
# define sqlite3WalSavepointUndo(y,z)          0
# define sqlite3WalFrames(u,v,w,x,y,z)         0
# define sqlite3WalCheckpoint(u,v,w,x)         0
# define sqlite3WalCallback(z)                 0
# define sqlite3WalExclusiveMode(y,z)          0
#else

#define WAL_SAVEPOINT_NDATA 4

/* Connection to a write-ahead log (WAL) file. 
** There is one object of this type for each pager. 
*/
typedef struct Wal Wal;

/* Open and close a connection to a write-ahead log. */
int sqlite3WalOpen(sqlite3_vfs*, sqlite3_file*, const char *zName, Wal**);
int sqlite3WalClose(Wal *pWal, int sync_flags, int, u8 *);

/* Used by readers to open (lock) and close (unlock) a snapshot.  A 
** snapshot is like a read-transaction.  It is the state of the database
** at an instant in time.  sqlite3WalOpenSnapshot gets a read lock and
** preserves the current state even if the other threads or processes
** write to or checkpoint the WAL.  sqlite3WalCloseSnapshot() closes the
** transaction and releases the lock.
*/
int sqlite3WalBeginReadTransaction(Wal *pWal, int *);
void sqlite3WalEndReadTransaction(Wal *pWal);

/* Read a page from the write-ahead log, if it is present. */
int sqlite3WalRead(Wal *pWal, Pgno pgno, int *pInWal, int nOut, u8 *pOut);

/* If the WAL is not empty, return the size of the database. */
Pgno sqlite3WalDbsize(Wal *pWal);

/* Obtain or release the WRITER lock. */
int sqlite3WalBeginWriteTransaction(Wal *pWal);
int sqlite3WalEndWriteTransaction(Wal *pWal);

/* Undo any frames written (but not committed) to the log */
int sqlite3WalUndo(Wal *pWal, int (*xUndo)(void *, Pgno), void *pUndoCtx);

/* Return an integer that records the current (uncommitted) write
** position in the WAL */
void sqlite3WalSavepoint(Wal *pWal, u32 *aWalData);

/* Move the write position of the WAL back to iFrame.  Called in
** response to a ROLLBACK TO command. */
int sqlite3WalSavepointUndo(Wal *pWal, u32 *aWalData);

/* Write a frame or frames to the log. */
int sqlite3WalFrames(Wal *pWal, int, PgHdr *, Pgno, int, int);

/* Copy pages from the log to the database file */ 
int sqlite3WalCheckpoint(
  Wal *pWal,                      /* Write-ahead log connection */
  int sync_flags,                 /* Flags to sync db file with (or 0) */
  int nBuf,                       /* Size of buffer nBuf */
  u8 *zBuf                        /* Temporary buffer to use */
);

/* Return the value to pass to a sqlite3_wal_hook callback, the
** number of frames in the WAL at the point of the last commit since
** sqlite3WalCallback() was called.  If no commits have occurred since
** the last call, then return 0.
*/
int sqlite3WalCallback(Wal *pWal);

/* Tell the wal layer that an EXCLUSIVE lock has been obtained (or released)
** by the pager layer on the database file.
*/
int sqlite3WalExclusiveMode(Wal *pWal, int op);

#endif /* ifndef SQLITE_OMIT_WAL */
#endif /* _WAL_H_ */

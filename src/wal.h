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

/* Connection to a log file. There is one object of this type for each pager. */
typedef struct Log Log;

/* Open and close a connection to a log file. */
int sqlite3WalOpen(sqlite3_vfs*, const char *zDb, Log **ppLog);
int sqlite3WalClose(Log *pLog, sqlite3_file *pFd, int sync_flags, u8 *zBuf);

/* Used by readers to open (lock) and close (unlock) a snapshot. */
int sqlite3WalOpenSnapshot(Log *pLog, int *);
void sqlite3WalCloseSnapshot(Log *pLog);

/* Read a page from the log, if it is present. */
int sqlite3WalRead(Log *pLog, Pgno pgno, int *pInLog, u8 *pOut);
void sqlite3WalDbsize(Log *pLog, Pgno *pPgno);

/* Obtain or release the WRITER lock. */
int sqlite3WalWriteLock(Log *pLog, int op);

/* Undo any frames written (but not committed) to the log */
int sqlite3WalUndo(Log *pLog, int (*xUndo)(void *, Pgno), void *pUndoCtx);

u32 sqlite3WalSavepoint(Log *pLog);
int sqlite3WalSavepointUndo(Log *pLog, u32 iFrame);

/* Return true if data has been written but not committed to the log file. */
int sqlite3WalDirty(Log *pLog);

/* Write a frame or frames to the log. */
int sqlite3WalFrames(Log *pLog, int, PgHdr *, Pgno, int, int);

/* Copy pages from the log to the database file */ 
int sqlite3WalCheckpoint(
  Log *pLog,                      /* Log connection */
  sqlite3_file *pFd,              /* File descriptor open on db file */
  int sync_flags,                 /* Flags to sync db file with (or 0) */
  u8 *zBuf,                       /* Temporary buffer to use */
  int (*xBusyHandler)(void *),    /* Pointer to busy-handler function */
  void *pBusyHandlerArg           /* Argument to pass to xBusyHandler */
);

/* Return the value to pass to a log callback. Or 0 for no callback. */
int sqlite3WalCallback(Log *pLog);

#endif /* _WAL_H_ */

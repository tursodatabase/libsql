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

#ifndef _LOG_H_
#define _LOG_H_

#include "sqliteInt.h"

/* Flags that may be set in the 'flags' argument to sqlite3LogWrite(): */
#define LOG_MASK_COMMIT        0x08
#define LOG_MASK_MASTERJOURNAL 0x10
#define LOG_MASK_TRUNCATE      0x20


#define LOG_TRUNCATE_BIT       0x80000000 

/* Connection to a log file. There is one object of this type for each pager. */
typedef struct Log Log;

/* Open and close a connection to a log file. */
int sqlite3LogOpen(sqlite3_vfs*, const char *zDb, Log **ppLog);
int sqlite3LogClose(Log *pLog, sqlite3_file *pFd, u8 *zBuf);

/* Configure the log connection. */
void sqlite3LogSetSyncflags(Log *, int sync_flags);

/* Used by readers to open (lock) and close (unlock) a database snapshot. */
int sqlite3LogOpenSnapshot(Log *pLog, int *);
void sqlite3LogCloseSnapshot(Log *pLog);

/* Read a page from the log, if it is present. */
int sqlite3LogRead(Log *pLog, Pgno pgno, int *pInLog, u8 *pOut);
void sqlite3LogMaxpgno(Log *pLog, Pgno *pPgno);

/* Obtain or release the WRITER lock. */
int sqlite3LogWriteLock(Log *pLog, int op);

/* Write a segment to the log. */
int sqlite3LogFrames(Log *pLog, int, PgHdr *, Pgno, int, int);

/* Copy pages from the log to the database file */ 
int sqlite3LogCheckpoint(
  Log *pLog,                      /* Log connection */
  sqlite3_file *pFd,              /* File descriptor open on db file */
  u8 *zBuf                        /* Temporary buffer to use */
);

#endif /* _LOG_H_ */

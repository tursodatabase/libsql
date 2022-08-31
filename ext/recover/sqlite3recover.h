/*
** 2022-08-27
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
*/


#ifndef _SQLITE_RECOVER_H
#define _SQLITE_RECOVER_H

#include "sqlite3.h"              /* Required for error code definitions */

#ifdef __cplusplus
extern "C" {
#endif

typedef struct sqlite3_recover sqlite3_recover;

/* Create an object to recover data from database zDb (e.g. "main")
** opened by handle db. Data will be recovered into the database
** identified by parameter zUri. Database zUri is clobbered if it
** already exists.
*/
sqlite3_recover *sqlite3_recover_init(
  sqlite3* db, 
  const char *zDb, 
  const char *zUri
);

/* Details TBD. */
int sqlite3_recover_config(sqlite3_recover*, int op, void *pArg);

#define SQLITE_RECOVER_TESTDB 789

/* Step the recovery object. Return SQLITE_DONE if recovery is complete,
** SQLITE_OK if recovery is not complete but no error has occurred, or
** an SQLite error code if an error has occurred.
*/
int sqlite3_recover_step(sqlite3_recover*);

const char *sqlite3_recover_errmsg(sqlite3_recover*);

int sqlite3_recover_errcode(sqlite3_recover*);

/* Clean up a recovery object created by a call to sqlite3_recover_init().
** This function returns SQLITE_DONE if the new database was created,
** SQLITE_OK if it processing was abandoned before it as finished or
** an SQLite error code (e.g. SQLITE_IOERR, SQLITE_NOMEM etc.) if an
** error occurred.  */
int sqlite3_recover_finish(sqlite3_recover*);


#ifdef __cplusplus
}  /* end of the 'extern "C"' block */
#endif

#endif /* ifndef _SQLITE_RECOVER_H */


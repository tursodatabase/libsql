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
** This file contains the public interface to the "recover" extension -
** an SQLite extension designed to recover data from corrupted database
** files.
*/

/*
** OVERVIEW:
**
** To use the API to recover data from a corrupted database, an
** application:
**
** 1) Creates an sqlite3_recover handle by calling either
**    sqlite3_recover_init() or sqlite3_recover_init_sql().
**
** 2) Configures the new handle using one or more calls to
**    sqlite3_recover_config().
**
** 3) Executes the recovery by calling sqlite3_recover_run() on the handle.
**
** 4) Retrieves any error code and English language error message using the
**    sqlite3_recover_errcode() and sqlite3_recover_errmsg() APIs,
**    respectively.
**
** 5) Destroys the sqlite3_recover handle and frees all resources
**    using sqlite3_recover_finish().
*/


#ifndef _SQLITE_RECOVER_H
#define _SQLITE_RECOVER_H

#include "sqlite3.h"

#ifdef __cplusplus
extern "C" {
#endif

/*
** Opaque handle type.
*/
typedef struct sqlite3_recover sqlite3_recover;

/* 
** These two APIs attempt to create and return a new sqlite3_recover object.
** In both cases the first two arguments identify the (possibly
** corrupt) database to recover data from. The first argument is an open
** database handle and the second the name of a database attached to that
** handle (i.e. "main", "temp" or the name of an attached database).
**
** If sqlite3_recover_init() is used to create the new sqlite3_recover
** handle, then data is recovered into a new database, identified by
** string parameter zUri. zUri may be an absolute or relative file path,
** or may be an SQLite URI. If the identified database file already exists,
** it is overwritten.
**
** If sqlite3_recover_init_sql() is invoked, then any recovered data will
** be returned to the user as a series of SQL statements. Executing these
** SQL statements results in the same database as would have been created
** had sqlite3_recover_init() been used. For each SQL statement in the
** output, the callback function passed as the third argument (xSql) is 
** invoked once. The first parameter is a passed a copy of the fourth argument
** to this function (pCtx) as its first parameter, and a pointer to a
** nul-terminated buffer containing the SQL statement formated as UTF-8 as 
** the second. If the xSql callback returns any value other than SQLITE_OK,
** then processing is immediately abandoned and the value returned used as
** the recover handle error code (see below).
**
** If an out-of-memory error occurs, NULL may be returned instead of
** a valid handle. In all other cases, it is the responsibility of the
** application to avoid resource leaks by ensuring that
** sqlite3_recover_finish() is called on all allocated handles.
*/
sqlite3_recover *sqlite3_recover_init(
  sqlite3* db, 
  const char *zDb, 
  const char *zUri
);
sqlite3_recover *sqlite3_recover_init_sql(
  sqlite3* db, 
  const char *zDb, 
  int (*xSql)(void*, const char*),
  void *pCtx
);

/*
**
*/
int sqlite3_recover_config(sqlite3_recover*, int op, void *pArg);

/*
** SQLITE_RECOVER_TESTDB:
**
** SQLITE_RECOVER_LOST_AND_FOUND:
**   The pArg argument points to a string buffer containing the name
**   of a "lost-and-found" table in the output database, or NULL. If
**   the argument is non-NULL and the database contains seemingly
**   valid pages that cannot be associated with any table in the
**   recovered part of the schema, data is extracted from these
**   pages to add to the lost-and-found table.
**
** SQLITE_RECOVER_FREELIST_CORRUPT:
**   The pArg value must actually be a pointer to a value of type
**   int containing value 0 or 1 cast as a (void*). If this option is set
**   (argument is 1) and a lost-and-found table has been configured using
**   SQLITE_RECOVER_LOST_AND_FOUND, then is assumed that the freelist is 
**   corrupt and an attempt is made to recover records from pages that
**   appear to be linked into the freelist. Otherwise, pages on the freelist
**   are ignored. Setting this option can recover more data from the
**   database, but often ends up "recovering" deleted records.
**
** SQLITE_RECOVER_ROWIDS:
**
** SQLITE_RECOVER_SQLHOOK:
*/
#define SQLITE_RECOVER_TESTDB           789
#define SQLITE_RECOVER_LOST_AND_FOUND   790
#define SQLITE_RECOVER_FREELIST_CORRUPT 791
#define SQLITE_RECOVER_ROWIDS           792

/* 
** Run the recovery. Return an SQLite error code if an error occurs, or
** SQLITE_OK otherwise. 
*/
int sqlite3_recover_run(sqlite3_recover*);

/*
** Return a pointer to a buffer containing the English language error
** message stored in the sqlite3_recover handle. If no error message
** is available (including in the case where no error has occurred),
** NULL is returned.
*/
const char *sqlite3_recover_errmsg(sqlite3_recover*);

/*
** Return the recover handle error code. SQLITE_OK is returned if no error
** has occurred.
*/
int sqlite3_recover_errcode(sqlite3_recover*);

/* 
** Clean up a recovery object created by a call to sqlite3_recover_init().
** This function returns SQLITE_OK if no error occurred, or else a copy
** of the recover handle error code.
*/
int sqlite3_recover_finish(sqlite3_recover*);


#ifdef __cplusplus
}  /* end of the 'extern "C"' block */
#endif

#endif /* ifndef _SQLITE_RECOVER_H */


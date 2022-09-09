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
** Configure an sqlite3_recover object that has just been created using
** sqlite3_recover_init() or sqlite3_recover_init_sql(). The second
** argument passed to this function must be one of the SQLITE_RECOVER_*
** symbols defined below. Valid values for the third argument depend
** on the specific SQLITE_RECOVER_* symbol in use.
**
** SQLITE_OK is returned if the configuration operation was successful,
** or an SQLite error code otherwise.
*/
int sqlite3_recover_config(sqlite3_recover*, int op, void *pArg);

/*
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
**   database, but often ends up "recovering" deleted records. The default 
**   value is 0 (clear).
**
** SQLITE_RECOVER_ROWIDS:
**   The pArg value must actually be a pointer to a value of type
**   int containing value 0 or 1 cast as a (void*). If this option is set
**   (argument is 1), then an attempt is made to recover rowid values
**   that are not also INTEGER PRIMARY KEY values. If this option is
**   clear, then new rowids are assigned to all recovered rows. The
**   default value is 1 (set).
*/
#define SQLITE_RECOVER_LOST_AND_FOUND   1
#define SQLITE_RECOVER_FREELIST_CORRUPT 2
#define SQLITE_RECOVER_ROWIDS           3

/* 
** Run the recovery operation. This function does not return until the 
** recovery operation is completed - either the new database has been
** created and populated (sqlite3_recover_init()) or all SQL statements have
** been passed to the callback (sqlite3_recover_init_sql()) - or an error
** occurs. If the recovery is completed without error, SQLITE_OK
** is returned. It is not considered an error if data cannot be recovered
** due to database corruption.
**
** If an error (for example an out-of-memory or IO error) occurs, then
** an SQLite error code is returned. The final state of the output database
** or the results of running any SQL statements already passed to the
** callback in this case are undefined. An English language error 
** message corresponding to the error may be available via the 
** sqlite3_recover_errmsg() API.
**
** This function may only be called once on an sqlite3_recover handle.
** If it is called more than once, the second and subsequent calls
** return SQLITE_MISUSE. The error code and error message returned
** by sqlite3_recover_errcode() and sqlite3_recover_errmsg() are not
** updated in this case.
*/
int sqlite3_recover_run(sqlite3_recover*);

/*
** If this is called on an sqlite3_recover handle before 
** sqlite3_recover_run() has been called, or if the call to
** sqlite3_recover_run() returned SQLITE_OK, then this API always returns
** a NULL pointer.
**
** Otherwise, an attempt is made to return a pointer to a buffer containing 
** an English language error message related to the error that occurred
** within the sqlite3_recover_run() call. If no error message is available,
** or if an out-of-memory error occurs while attempting to allocate a buffer
** for one, NULL may still be returned.
**
** The buffer remains valid until the sqlite3_recover handle is destroyed
** using sqlite3_recover_finish().
*/
const char *sqlite3_recover_errmsg(sqlite3_recover*);

/*
** If this function is called on an sqlite3_recover handle before
** sqlite3_recover_run() has been called, it always returns SQLITE_OK.
** Otherwise, it returns a copy of the value returned by the first
** sqlite3_recover_run() call made on the handle.
*/
int sqlite3_recover_errcode(sqlite3_recover*);

/* 
** Clean up a recovery object created by a call to sqlite3_recover_init().
** The results of using a recovery object with any API after it has been
** passed to this function are undefined.
**
** If this function is called on an sqlite3_recover handle before
** sqlite3_recover_run() has been called, it always returns SQLITE_OK.
** Otherwise, it returns a copy of the value returned by the first
** sqlite3_recover_run() call made on the handle.
*/
int sqlite3_recover_finish(sqlite3_recover*);


#ifdef __cplusplus
}  /* end of the 'extern "C"' block */
#endif

#endif /* ifndef _SQLITE_RECOVER_H */


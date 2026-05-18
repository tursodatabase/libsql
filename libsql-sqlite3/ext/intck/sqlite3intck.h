/*
** 2024-02-08
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
*************************************************************************
*/

/*
** Incremental Integrity-Check Extension
** -------------------------------------
**
** This module contains code to check whether or not an SQLite database
** is well-formed or corrupt. This is the same task as performed by SQLite's
** built-in "PRAGMA integrity_check" command. This module differs from
** "PRAGMA integrity_check" in that:
**
**   +  It is less thorough - this module does not detect certain types
**      of corruption that are detected by the PRAGMA command. However,
**      it does detect all kinds of corruption that are likely to cause
**      errors in SQLite applications.
**
**   +  It is slower. Sometimes up to three times slower.
**
**   +  It allows integrity-check operations to be split into multiple
**      transactions, so that the database does not need to be read-locked
**      for the duration of the integrity-check.
**
** One way to use the API to run integrity-check on the "main" database
** of handle db is:
**
**   int rc = SQLITE_OK;
**   sqlite3_intck *p = 0;
**
**   sqlite3_intck_open(db, "main", &p);
**   while( SQLITE_OK==sqlite3_intck_step(p) ){
**     const char *zMsg = sqlite3_intck_message(p);
**     if( zMsg ) printf("corruption: %s\n", zMsg);
**   }
**   rc = sqlite3_intck_error(p, &zErr);
**   if( rc!=SQLITE_OK ){
**     printf("error occured (rc=%d), (errmsg=%s)\n", rc, zErr);
**   }
**   sqlite3_intck_close(p);
**
** Usually, the sqlite3_intck object opens a read transaction within the
** first call to sqlite3_intck_step() and holds it open until the 
** integrity-check is complete. However, if sqlite3_intck_unlock() is
** called, the read transaction is ended and a new read transaction opened
** by the subsequent call to sqlite3_intck_step().
*/

#ifndef _SQLITE_INTCK_H
#define _SQLITE_INTCK_H

#include "sqlite3.h"

#ifdef __cplusplus
extern "C" {
#endif

/*
** An ongoing incremental integrity-check operation is represented by an
** opaque pointer of the following type.
*/
typedef struct sqlite3_intck sqlite3_intck;

/*
** Open a new incremental integrity-check object. If successful, populate
** output variable (*ppOut) with the new object handle and return SQLITE_OK.
** Or, if an error occurs, set (*ppOut) to NULL and return an SQLite error
** code (e.g. SQLITE_NOMEM).
**
** The integrity-check will be conducted on database zDb (which must be "main",
** "temp", or the name of an attached database) of database handle db. Once
** this function has been called successfully, the caller should not use 
** database handle db until the integrity-check object has been destroyed
** using sqlite3_intck_close().
*/
int sqlite3_intck_open(
  sqlite3 *db,                    /* Database handle */
  const char *zDb,                /* Database name ("main", "temp" etc.) */
  sqlite3_intck **ppOut           /* OUT: New sqlite3_intck handle */
);

/*
** Close and release all resources associated with a handle opened by an
** earlier call to sqlite3_intck_open(). The results of using an
** integrity-check handle after it has been passed to this function are
** undefined.
*/
void sqlite3_intck_close(sqlite3_intck *pCk);

/*
** Do the next step of the integrity-check operation specified by the handle
** passed as the only argument. This function returns SQLITE_DONE if the 
** integrity-check operation is finished, or an SQLite error code if
** an error occurs, or SQLITE_OK if no error occurs but the integrity-check
** is not finished. It is not considered an error if database corruption
** is encountered.
**
** Following a successful call to sqlite3_intck_step() (one that returns
** SQLITE_OK), sqlite3_intck_message() returns a non-NULL value if 
** corruption was detected in the db.
**
** If an error occurs and a value other than SQLITE_OK or SQLITE_DONE is
** returned, then the integrity-check handle is placed in an error state.
** In this state all subsequent calls to sqlite3_intck_step() or 
** sqlite3_intck_unlock() will immediately return the same error. The 
** sqlite3_intck_error() method may be used to obtain an English language 
** error message in this case.
*/
int sqlite3_intck_step(sqlite3_intck *pCk);

/*
** If the previous call to sqlite3_intck_step() encountered corruption 
** within the database, then this function returns a pointer to a buffer
** containing a nul-terminated string describing the corruption in 
** English. If the previous call to sqlite3_intck_step() did not encounter
** corruption, or if there was no previous call, this function returns 
** NULL.
*/
const char *sqlite3_intck_message(sqlite3_intck *pCk);

/*
** Close any read-transaction opened by an earlier call to 
** sqlite3_intck_step(). Any subsequent call to sqlite3_intck_step() will
** open a new transaction. Return SQLITE_OK if successful, or an SQLite error
** code otherwise.
**
** If an error occurs, then the integrity-check handle is placed in an error
** state. In this state all subsequent calls to sqlite3_intck_step() or 
** sqlite3_intck_unlock() will immediately return the same error. The 
** sqlite3_intck_error() method may be used to obtain an English language 
** error message in this case.
*/
int sqlite3_intck_unlock(sqlite3_intck *pCk);

/*
** If an error has occurred in an earlier call to sqlite3_intck_step()
** or sqlite3_intck_unlock(), then this method returns the associated 
** SQLite error code. Additionally, if pzErr is not NULL, then (*pzErr)
** may be set to point to a nul-terminated string containing an English
** language error message. Or, if no error message is available, to
** NULL.
**
** If no error has occurred within sqlite3_intck_step() or
** sqlite_intck_unlock() calls on the handle passed as the first argument, 
** then SQLITE_OK is returned and (*pzErr) set to NULL.
*/
int sqlite3_intck_error(sqlite3_intck *pCk, const char **pzErr);

/*
** This API is used for testing only. It returns the full-text of an SQL
** statement used to test object zObj, which may be a table or index.
** The returned buffer is valid until the next call to either this function
** or sqlite3_intck_close() on the same sqlite3_intck handle.
*/
const char *sqlite3_intck_test_sql(sqlite3_intck *pCk, const char *zObj);


#ifdef __cplusplus
}  /* end of the 'extern "C"' block */
#endif

#endif /* ifndef _SQLITE_INTCK_H */

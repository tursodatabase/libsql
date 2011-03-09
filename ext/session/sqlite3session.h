
#ifndef __SQLITESESSION_H_
#define __SQLITESESSION_H_ 1

/*
** Make sure we can call this stuff from C++.
*/
#ifdef __cplusplus
extern "C" {
#endif

#include "sqlite3.h"

typedef struct sqlite3_session sqlite3_session;
typedef struct sqlite3_changeset_iter sqlite3_changeset_iter;

/*
** Create a session object. This session object will record changes to
** database zDb attached to connection db.
*/
int sqlite3session_create(
  sqlite3 *db,                    /* Database handle */
  const char *zDb,                /* Name of db (e.g. "main") */
  sqlite3_session **ppSession     /* OUT: New session object */
);

/*
** Enable or disable the recording of changes by a session object. When
** enabled, a session object records changes made to the database. When
** disabled - it does not. A newly created session object is enabled.
**
** Passing zero to this function disables the session. Passing a value
** greater than zero enables it. Passing a value less than zero is a 
** no-op, and may be used to query the current state of the session.
**
** The return value indicates the final state of the session object: 0 if 
** the session is disabled, or 1 if it is enabled.
*/
int sqlite3session_enable(sqlite3_session *pSession, int bEnable);

/*
** Attach a table to a session. All subsequent changes made to the table
** while the session object is enabled will be recorded.
**
** Only tables that have a PRIMARY KEY defined may be attached. It does
** not matter if the PRIMARY KEY is an "INTEGER PRIMARY KEY" (rowid alias)
** or not.
*/
int sqlite3session_attach(
  sqlite3_session *pSession,      /* Session object */
  const char *zTab                /* Table name */
);

/*
** Obtain a changeset object containing all changes recorded by the 
** session object passed as the first argument.
**
** It is the responsibility of the caller to eventually free the buffer 
** using sqlite3_free().
*/
int sqlite3session_changeset(
  sqlite3_session *pSession,      /* Session object */
  int *pnChangeset,               /* OUT: Size of buffer at *ppChangeset */
  void **ppChangeset              /* OUT: Buffer containing changeset */
);

/*
** Delete a session object previously allocated using sqlite3session_create().
*/
void sqlite3session_delete(sqlite3_session *pSession);


/*
** Create an iterator used to iterate through the contents of a changeset.
*/
int sqlite3changeset_start(
  sqlite3_changeset_iter **ppIter,
  int nChangeset, 
  void *pChangeset
);

/*
** Advance an iterator created by sqlite3changeset_start() to the next
** change in the changeset. This function may return SQLITE_ROW, SQLITE_DONE
** or SQLITE_CORRUPT.
**
** This function may not be called on iterators passed to a conflict handler
** callback by changeset_apply().
*/
int sqlite3changeset_next(sqlite3_changeset_iter *pIter);

/*
** The following three functions extract information on the current change
** from a changeset iterator. They may only be called after changeset_next()
** has returned SQLITE_ROW.
*/
int sqlite3changeset_op(
  sqlite3_changeset_iter *pIter,      /* Iterator object */
  const char **pzTab,                 /* OUT: Pointer to table name */
  int *pnCol,                         /* OUT: Number of columns in table */
  int *pOp                            /* OUT: SQLITE_INSERT, DELETE or UPDATE */
);

int sqlite3changeset_old(
  sqlite3_changeset_iter *pIter,
  int iVal,
  sqlite3_value **ppValue             /* OUT: Old value (or NULL pointer) */
);

int sqlite3changeset_new(
  sqlite3_changeset_iter *pIter,
  int iVal,
  sqlite3_value **ppValue             /* OUT: New value (or NULL pointer) */
);

/*
** Finalize an iterator allocated with sqlite3changeset_start().
**
** This function may not be called on iterators passed to a conflict handler
** callback by changeset_apply().
*/
int sqlite3changeset_finalize(sqlite3_changeset_iter *pIter);

/*
** Invert a changeset object.
*/
int sqlite3changeset_invert(
  int nIn, void *pIn,             /* Input changeset */
  int *pnOut, void **ppOut        /* OUT: Inverse of input */
);


#endif


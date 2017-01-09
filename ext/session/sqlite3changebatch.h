
#if !defined(SQLITECHANGEBATCH_H_) 
#define SQLITECHANGEBATCH_H_ 1

typedef struct sqlite3_changebatch sqlite3_changebatch;

/*
** Create a new changebatch object for detecting conflicts between
** changesets associated with a schema equivalent to that of the "main"
** database of the open database handle db passed as the first
** parameter. It is the responsibility of the caller to ensure that
** the database handle is not closed until after the changebatch
** object has been deleted.
**
** A changebatch object is used to detect batches of non-conflicting
** changesets. Changesets that do not conflict may be applied to the 
** target database in any order without affecting the final state of 
** the database.
**
** The changebatch object only works reliably if PRIMARY KEY and UNIQUE
** constraints on tables affected by the changesets use collation
** sequences that are equivalent to built-in collation sequence 
** BINARY for the == operation.
**
** If successful, SQLITE_OK is returned and (*pp) set to point to
** the new changebatch object. If an error occurs, an SQLite error
** code is returned and the final value of (*pp) is undefined.
*/
int sqlite3changebatch_new(sqlite3 *db, sqlite3_changebatch **pp);

/*
** Argument p points to a buffer containing a changeset n bytes in
** size. Assuming no error occurs, this function returns SQLITE_OK
** if the changeset does not conflict with any changeset passed 
** to an sqlite3changebatch_add() call made on the same 
** sqlite3_changebatch* handle since the most recent call to
** sqlite3changebatch_zero(). If the changeset does conflict with 
** an earlier such changeset, SQLITE_CONSTRAINT is returned. Or, 
** if an error occurs, some other SQLite error code may be returned.
**
** One changeset is said to conflict with another if
** either:
**
**   * the two changesets contain operations (INSERT, UPDATE or 
**     DELETE) on the same row, identified by primary key, or
**
**   * the two changesets contain operations (INSERT, UPDATE or 
**     DELETE) on rows with identical values in any combination 
**     of fields constrained by a UNIQUE constraint.
**
** Even if this function returns SQLITE_CONFLICT, the current
** changeset is added to the internal data structures - so future
** calls to this function may conflict with it. If this function
** returns any result code other than SQLITE_OK or SQLITE_CONFLICT,
** the result of any future call to sqlite3changebatch_add() is
** undefined.
**
** Only changesets may be passed to this function. Passing a 
** patchset to this function results in an SQLITE_MISUSE error.
*/
int sqlite3changebatch_add(sqlite3_changebatch*, void *p, int n);

/*
** Zero a changebatch object. This causes the records of all earlier 
** calls to sqlite3changebatch_add() to be discarded.
*/
void sqlite3changebatch_zero(sqlite3_changebatch*);

/*
** Return a copy of the first argument passed to the sqlite3changebatch_new()
** call used to create the changebatch object passed as the only argument
** to this function.
*/
sqlite3 *sqlite3changebatch_db(sqlite3_changebatch*);

/*
** Delete a changebatch object.
*/
void sqlite3changebatch_delete(sqlite3_changebatch*);

#endif /* !defined(SQLITECHANGEBATCH_H_) */


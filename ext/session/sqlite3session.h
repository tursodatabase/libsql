
#if !defined(__SQLITESESSION_H_) && defined(SQLITE_ENABLE_SESSION)
#define __SQLITESESSION_H_ 1

/*
** Make sure we can call this stuff from C++.
*/
#ifdef __cplusplus
extern "C" {
#endif

#include "sqlite3.h"

/*
** CAPI3REF: Session Object Handle
*/
typedef struct sqlite3_session sqlite3_session;

/*
** CAPI3REF: Changeset Iterator Handle
*/
typedef struct sqlite3_changeset_iter sqlite3_changeset_iter;

/*
** CAPI3REF: Create A New Session Object
**
** Create a new session object attached to database handle db. If successful,
** a pointer to the new object is written to *ppSession and SQLITE_OK is
** returned. If an error occurs, *ppSession is set to NULL and an SQLite
** error code (e.g. SQLITE_NOMEM) is returned.
**
** It is possible to create multiple session objects attached to a single
** database handle.
**
** Session objects created using this function should be deleted using the
** [sqlite3session_delete()] function before the database handle that they
** are attached to is itself closed. If the database handle is closed before
** the session object is deleted, then the results of calling any session
** module function, including [sqlite3session_delete()] on the session object
** are undefined.
**
** Because the session module uses the [sqlite3_preupdate_hook()] API, it
** is not possible for an application to register a pre-update hook on a
** database handle that has one or more session objects attached. Nor is
** it possible to create a session object attached to a database handle for
** which a pre-update hook is already defined. The results of attempting 
** either of these things are undefined.
**
** The session object will be used to create changesets for tables in
** database zDb, where zDb is either "main", or "temp", or the name of an
** attached database. It is not an error if database zDb is not attached
** to the database when the session object is created.
*/
int sqlite3session_create(
  sqlite3 *db,                    /* Database handle */
  const char *zDb,                /* Name of db (e.g. "main") */
  sqlite3_session **ppSession     /* OUT: New session object */
);

/*
** CAPI3REF: Delete A Session Object
**
** Delete a session object previously allocated using 
** [sqlite3session_create()]. Once a session object has been deleted, the
** results of attempting to use pSession with any other session module
** function are undefined.
**
** Session objects must be deleted before the database handle to which they
** are attached is closed. Refer to the documentation for 
** [sqlite3session_create()] for details.
*/
void sqlite3session_delete(sqlite3_session *pSession);


/*
** CAPI3REF: Enable Or Disable A Session Object
**
** Enable or disable the recording of changes by a session object. When
** enabled, a session object records changes made to the database. When
** disabled - it does not. A newly created session object is enabled.
** Refer to the documentation for [sqlite3session_changeset()] for further
** details regarding how enabling and disabling a session object affects
** the eventual changesets.
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
** CAPI3REF: Set Or Clear the Indirect Change Flag
**
** Each change recorded by a session object is marked as either direct or
** indirect. A change is marked as indirect if either:
**
** <ul>
**   <li> The session object "indirect" flag is set when the change is
**        made, or
**   <li> The change is made by an SQL trigger or foreign key action 
**        instead of directly as a result of a users SQL statement.
** </ul>
**
** If a single row is affected by more than one operation within a session,
** then the change is considered indirect if all operations meet the criteria
** for an indirect change above, or direct otherwise.
**
** This function is used to set, clear or query the session object indirect
** flag.  If the second argument passed to this function is zero, then the
** indirect flag is cleared. If it is greater than zero, the indirect flag
** is set. Passing a value less than zero does not modify the current value
** of the indirect flag, and may be used to query the current state of the 
** indirect flag for the specified session object.
**
** The return value indicates the final state of the indirect flag: 0 if 
** it is clear, or 1 if it is set.
*/
int sqlite3session_indirect(sqlite3_session *pSession, int bIndirect);

/*
** CAPI3REF: Attach A Table To A Session Object
**
** If argument zTab is not NULL, then it is the name of a table to attach
** to the session object passed as the first argument. All subsequent changes 
** made to the table while the session object is enabled will be recorded. See 
** documentation for [sqlite3session_changeset()] for further details.
**
** Or, if argument zTab is NULL, then changes are recorded for all tables
** in the database. If additional tables are added to the database (by 
** executing "CREATE TABLE" statements) after this call is made, changes for 
** the new tables are also recorded.
**
** Changes can only be recorded for tables that have a PRIMARY KEY explicitly
** defined as part of their CREATE TABLE statement. It does not matter if the 
** PRIMARY KEY is an "INTEGER PRIMARY KEY" (rowid alias) or not. The PRIMARY
** KEY may consist of a single column, or may be a composite key.
** 
** It is not an error if the named table does not exist in the database. Nor
** is it an error if the named table does not have a PRIMARY KEY. However,
** no changes will be recorded in either of these scenarios.
**
** Changes are not recorded for individual rows that have NULL values stored
** in one or more of their PRIMARY KEY columns.
**
** SQLITE_OK is returned if the call completes without error. Or, if an error 
** occurs, an SQLite error code (e.g. SQLITE_NOMEM) is returned.
*/
int sqlite3session_attach(
  sqlite3_session *pSession,      /* Session object */
  const char *zTab                /* Table name */
);

/*
** CAPI3REF: Set a table filter on a Session Object.
**
** The second argument (xFilter) is the "filter callback". For changes to rows 
** in tables that are not attached to the Session object, the filter is called
** to determine whether changes to the table's rows should be tracked or not. 
** If xFilter returns 0, changes is not tracked. Note that once a table is 
** attached, xFilter will not be called again.
*/
void sqlite3session_table_filter(
  sqlite3_session *pSession,      /* Session object */
  int(*xFilter)(
    void *pCtx,                   /* Copy of third arg to _filter_table() */
    const char *zTab              /* Table name */
  ),
  void *pCtx                      /* First argument passed to xFilter */
);

/*
** CAPI3REF: Generate A Changeset From A Session Object
**
** Obtain a changeset containing changes to the tables attached to the 
** session object passed as the first argument. If successful, 
** set *ppChangeset to point to a buffer containing the changeset 
** and *pnChangeset to the size of the changeset in bytes before returning
** SQLITE_OK. If an error occurs, set both *ppChangeset and *pnChangeset to
** zero and return an SQLite error code.
**
** A changeset consists of zero or more INSERT, UPDATE and/or DELETE changes,
** each representing a change to a single row of an attached table. An INSERT
** change contains the values of each field of a new database row. A DELETE
** contains the original values of each field of a deleted database row. An
** UPDATE change contains the original values of each field of an updated
** database row along with the updated values for each updated non-primary-key
** column. It is not possible for an UPDATE change to represent a change that
** modifies the values of primary key columns. If such a change is made, it
** is represented in a changeset as a DELETE followed by an INSERT.
**
** Changes are not recorded for rows that have NULL values stored in one or 
** more of their PRIMARY KEY columns. If such a row is inserted or deleted,
** no corresponding change is present in the changesets returned by this
** function. If an existing row with one or more NULL values stored in
** PRIMARY KEY columns is updated so that all PRIMARY KEY columns are non-NULL,
** only an INSERT is appears in the changeset. Similarly, if an existing row
** with non-NULL PRIMARY KEY values is updated so that one or more of its
** PRIMARY KEY columns are set to NULL, the resulting changeset contains a
** DELETE change only.
**
** The contents of a changeset may be traversed using an iterator created
** using the [sqlite3changeset_start()] API. A changeset may be applied to
** a database with a compatible schema using the [sqlite3changeset_apply()]
** API.
**
** Within a changeset generated by this function, all changes related to a
** single table are grouped together. In other words, when iterating through
** a changeset or when applying a changeset to a database, all changes related
** to a single table are processed before moving on to the next table. Tables
** are sorted in the same order in which they were attached (or auto-attached)
** to the sqlite3_session object. The order in which the changes related to
** a single table are stored is undefined.
**
** Following a successful call to this function, it is the responsibility of
** the caller to eventually free the buffer that *ppChangeset points to using
** [sqlite3_free()].
**
** <h3>Changeset Generation</h3>
**
** Once a table has been attached to a session object, the session object
** records the primary key values of all new rows inserted into the table.
** It also records the original primary key and other column values of any
** deleted or updated rows. For each unique primary key value, data is only
** recorded once - the first time a row with said primary key is inserted,
** updated or deleted in the lifetime of the session.
**
** There is one exception to the previous paragraph: when a row is inserted,
** updated or deleted, if one or more of its primary key columns contain a
** NULL value, no record of the change is made.
**
** The session object therefore accumulates two types of records - those
** that consist of primary key values only (created when the user inserts
** a new record) and those that consist of the primary key values and the
** original values of other table columns (created when the users deletes
** or updates a record).
**
** When this function is called, the requested changeset is created using
** both the accumulated records and the current contents of the database
** file. Specifically:
**
** <ul>
**   <li> For each record generated by an insert, the database is queried
**        for a row with a matching primary key. If one is found, an INSERT
**        change is added to the changeset. If no such row is found, no change 
**        is added to the changeset.
**
**   <li> For each record generated by an update or delete, the database is 
**        queried for a row with a matching primary key. If such a row is
**        found and one or more of the non-primary key fields have been
**        modified from their original values, an UPDATE change is added to 
**        the changeset. Or, if no such row is found in the table, a DELETE 
**        change is added to the changeset. If there is a row with a matching
**        primary key in the database, but all fields contain their original
**        values, no change is added to the changeset.
** </ul>
**
** This means, amongst other things, that if a row is inserted and then later
** deleted while a session object is active, neither the insert nor the delete
** will be present in the changeset. Or if a row is deleted and then later a 
** row with the same primary key values inserted while a session object is
** active, the resulting changeset will contain an UPDATE change instead of
** a DELETE and an INSERT.
**
** When a session object is disabled (see the [sqlite3session_enable()] API),
** it does not accumulate records when rows are inserted, updated or deleted.
** This may appear to have some counter-intuitive effects if a single row
** is written to more than once during a session. For example, if a row
** is inserted while a session object is enabled, then later deleted while 
** the same session object is disabled, no INSERT record will appear in the
** changeset, even though the delete took place while the session was disabled.
** Or, if one field of a row is updated while a session is disabled, and 
** another field of the same row is updated while the session is enabled, the
** resulting changeset will contain an UPDATE change that updates both fields.
*/
int sqlite3session_changeset(
  sqlite3_session *pSession,      /* Session object */
  int *pnChangeset,               /* OUT: Size of buffer at *ppChangeset */
  void **ppChangeset              /* OUT: Buffer containing changeset */
);

/*
** CAPI3REF: Load The Difference Between Tables Into A Session 
**
** If it is not already attached to the session object passed as the first
** argument, this function attaches table zTbl in the same manner as the
** [sqlite3session_attach()] function. If zTbl does not exist, or if it
** does not have a primary key, this function is a no-op (but does not return
** an error).
**
** Argument zFromDb must be the name of a database ("main", "temp" etc.)
** attached to the same database handle as the session object that contains 
** a table compatible with the table attached to the session by this function.
** A table is considered compatible if it:
**
** <ul>
**   <li> Has the same name,
**   <li> Has the same set of columns declared in the same order, and
**   <li> Has the same PRIMARY KEY definition.
** </ul>
**
** If the tables are not compatible, SQLITE_SCHEMA is returned. If the tables
** are compatible but do not have any PRIMARY KEY columns, it is not an error
** but no changes are added to the session object. As with other session
** APIs, tables without PRIMARY KEYs are simply ignored.
**
** This function adds a set of changes to the session object that could be
** used to update the table in database zFrom (call this the "from-table") 
** so that its content is the same as the table attached to the session 
** object (call this the "to-table"). Specifically:
**
** <ul>
**   <li> For each row (primary key) that exists in the to-table but not in 
**     the from-table, an INSERT record is added to the session object.
**
**   <li> For each row (primary key) that exists in the to-table but not in 
**     the from-table, a DELETE record is added to the session object.
**
**   <li> For each row (primary key) that exists in both tables, but features 
**     different in each, an UPDATE record is added to the session.
** </ul>
**
** To clarify, if this function is called and then a changeset constructed
** using [sqlite3session_changeset()], then after applying that changeset to 
** database zFrom the contents of the two compatible tables would be 
** identical.
**
** It an error if database zFrom does not exist or does not contain the
** required compatible table.
**
** If the operation successful, SQLITE_OK is returned. Otherwise, an SQLite
** error code. In this case, if argument pzErrMsg is not NULL, *pzErrMsg
** may be set to point to a buffer containing an English language error 
** message. It is the responsibility of the caller to free this buffer using
** sqlite3_free().
*/
int sqlite3session_diff(
  sqlite3_session *pSession,
  const char *zFromDb,
  const char *zTbl,
  char **pzErrMsg
);


/*
** CAPI3REF: Generate A Patchset From A Session Object
**
** The differences between a patchset and a changeset are that:
**
** <ul>
**   <li> DELETE records consist of the primary key fields only. The 
**        original values of other fields are omitted.
**   <li> The original values of any modified fields are omitted from 
**        UPDATE records.
** </ul>
**
** A patchset blob may be used with up to date versions of all 
** sqlite3changeset_xxx API functions except for sqlite3changeset_invert(), 
** which returns SQLITE_CORRUPT if it is passed a patchset. Similarly,
** attempting to use a patchset blob with old versions of the
** sqlite3changeset_xxx APIs also provokes an SQLITE_CORRUPT error. 
**
** Because the non-primary key "old.*" fields are omitted, no 
** SQLITE_CHANGESET_DATA conflicts can be detected or reported if a patchset
** is passed to the sqlite3changeset_apply() API. Other conflict types work
** in the same way as for changesets.
**
** Changes within a patchset are ordered in the same way as for changesets
** generated by the sqlite3session_changeset() function (i.e. all changes for
** a single table are grouped together, tables appear in the order in which
** they were attached to the session object).
*/
int sqlite3session_patchset(
  sqlite3_session *pSession,      /* Session object */
  int *pnPatchset,                /* OUT: Size of buffer at *ppChangeset */
  void **ppPatchset               /* OUT: Buffer containing changeset */
);

/*
** CAPI3REF: Test if a changeset has recorded any changes.
**
** Return non-zero if no changes to attached tables have been recorded by 
** the session object passed as the first argument. Otherwise, if one or 
** more changes have been recorded, return zero.
**
** Even if this function returns zero, it is possible that calling
** [sqlite3session_changeset()] on the session handle may still return a
** changeset that contains no changes. This can happen when a row in 
** an attached table is modified and then later on the original values 
** are restored. However, if this function returns non-zero, then it is
** guaranteed that a call to sqlite3session_changeset() will return a 
** changeset containing zero changes.
*/
int sqlite3session_isempty(sqlite3_session *pSession);

/*
** CAPI3REF: Create An Iterator To Traverse A Changeset 
**
** Create an iterator used to iterate through the contents of a changeset.
** If successful, *pp is set to point to the iterator handle and SQLITE_OK
** is returned. Otherwise, if an error occurs, *pp is set to zero and an
** SQLite error code is returned.
**
** The following functions can be used to advance and query a changeset 
** iterator created by this function:
**
** <ul>
**   <li> [sqlite3changeset_next()]
**   <li> [sqlite3changeset_op()]
**   <li> [sqlite3changeset_new()]
**   <li> [sqlite3changeset_old()]
** </ul>
**
** It is the responsibility of the caller to eventually destroy the iterator
** by passing it to [sqlite3changeset_finalize()]. The buffer containing the
** changeset (pChangeset) must remain valid until after the iterator is
** destroyed.
**
** Assuming the changeset blob was created by one of the
** [sqlite3session_changeset()], [sqlite3changeset_concat()] or
** [sqlite3changeset_invert()] functions, all changes within the changeset 
** that apply to a single table are grouped together. This means that when 
** an application iterates through a changeset using an iterator created by 
** this function, all changes that relate to a single table are visited 
** consecutively. There is no chance that the iterator will visit a change 
** the applies to table X, then one for table Y, and then later on visit 
** another change for table X.
*/
int sqlite3changeset_start(
  sqlite3_changeset_iter **pp,    /* OUT: New changeset iterator handle */
  int nChangeset,                 /* Size of changeset blob in bytes */
  void *pChangeset                /* Pointer to blob containing changeset */
);


/*
** CAPI3REF: Advance A Changeset Iterator
**
** This function may only be used with iterators created by function
** [sqlite3changeset_start()]. If it is called on an iterator passed to
** a conflict-handler callback by [sqlite3changeset_apply()], SQLITE_MISUSE
** is returned and the call has no effect.
**
** Immediately after an iterator is created by sqlite3changeset_start(), it
** does not point to any change in the changeset. Assuming the changeset
** is not empty, the first call to this function advances the iterator to
** point to the first change in the changeset. Each subsequent call advances
** the iterator to point to the next change in the changeset (if any). If
** no error occurs and the iterator points to a valid change after a call
** to sqlite3changeset_next() has advanced it, SQLITE_ROW is returned. 
** Otherwise, if all changes in the changeset have already been visited,
** SQLITE_DONE is returned.
**
** If an error occurs, an SQLite error code is returned. Possible error 
** codes include SQLITE_CORRUPT (if the changeset buffer is corrupt) or 
** SQLITE_NOMEM.
*/
int sqlite3changeset_next(sqlite3_changeset_iter *pIter);

/*
** CAPI3REF: Obtain The Current Operation From A Changeset Iterator
**
** The pIter argument passed to this function may either be an iterator
** passed to a conflict-handler by [sqlite3changeset_apply()], or an iterator
** created by [sqlite3changeset_start()]. In the latter case, the most recent
** call to [sqlite3changeset_next()] must have returned [SQLITE_ROW]. If this
** is not the case, this function returns [SQLITE_MISUSE].
**
** If argument pzTab is not NULL, then *pzTab is set to point to a
** nul-terminated utf-8 encoded string containing the name of the table
** affected by the current change. The buffer remains valid until either
** sqlite3changeset_next() is called on the iterator or until the 
** conflict-handler function returns. If pnCol is not NULL, then *pnCol is 
** set to the number of columns in the table affected by the change. If
** pbIncorrect is not NULL, then *pbIndirect is set to true (1) if the change
** is an indirect change, or false (0) otherwise. See the documentation for
** [sqlite3session_indirect()] for a description of direct and indirect
** changes. Finally, if pOp is not NULL, then *pOp is set to one of 
** [SQLITE_INSERT], [SQLITE_DELETE] or [SQLITE_UPDATE], depending on the 
** type of change that the iterator currently points to.
**
** If no error occurs, SQLITE_OK is returned. If an error does occur, an
** SQLite error code is returned. The values of the output variables may not
** be trusted in this case.
*/
int sqlite3changeset_op(
  sqlite3_changeset_iter *pIter,  /* Iterator object */
  const char **pzTab,             /* OUT: Pointer to table name */
  int *pnCol,                     /* OUT: Number of columns in table */
  int *pOp,                       /* OUT: SQLITE_INSERT, DELETE or UPDATE */
  int *pbIndirect                 /* OUT: True for an 'indirect' change */
);

/*
** CAPI3REF: Obtain The Primary Key Definition Of A Table
**
** For each modified table, a changeset includes the following:
**
** <ul>
**   <li> The number of columns in the table, and
**   <li> Which of those columns make up the tables PRIMARY KEY.
** </ul>
**
** This function is used to find which columns comprise the PRIMARY KEY of
** the table modified by the change that iterator pIter currently points to.
** If successful, *pabPK is set to point to an array of nCol entries, where
** nCol is the number of columns in the table. Elements of *pabPK are set to
** 0x01 if the corresponding column is part of the tables primary key, or
** 0x00 if it is not.
**
** If argument pnCol is not NULL, then *pnCol is set to the number of columns
** in the table.
**
** If this function is called when the iterator does not point to a valid
** entry, SQLITE_MISUSE is returned and the output variables zeroed. Otherwise,
** SQLITE_OK is returned and the output variables populated as described
** above.
*/
int sqlite3changeset_pk(
  sqlite3_changeset_iter *pIter,  /* Iterator object */
  unsigned char **pabPK,          /* OUT: Array of boolean - true for PK cols */
  int *pnCol                      /* OUT: Number of entries in output array */
);

/*
** CAPI3REF: Obtain old.* Values From A Changeset Iterator
**
** The pIter argument passed to this function may either be an iterator
** passed to a conflict-handler by [sqlite3changeset_apply()], or an iterator
** created by [sqlite3changeset_start()]. In the latter case, the most recent
** call to [sqlite3changeset_next()] must have returned SQLITE_ROW. 
** Furthermore, it may only be called if the type of change that the iterator
** currently points to is either [SQLITE_DELETE] or [SQLITE_UPDATE]. Otherwise,
** this function returns [SQLITE_MISUSE] and sets *ppValue to NULL.
**
** Argument iVal must be greater than or equal to 0, and less than the number
** of columns in the table affected by the current change. Otherwise,
** [SQLITE_RANGE] is returned and *ppValue is set to NULL.
**
** If successful, this function sets *ppValue to point to a protected
** sqlite3_value object containing the iVal'th value from the vector of 
** original row values stored as part of the UPDATE or DELETE change and
** returns SQLITE_OK. The name of the function comes from the fact that this 
** is similar to the "old.*" columns available to update or delete triggers.
**
** If some other error occurs (e.g. an OOM condition), an SQLite error code
** is returned and *ppValue is set to NULL.
*/
int sqlite3changeset_old(
  sqlite3_changeset_iter *pIter,  /* Changeset iterator */
  int iVal,                       /* Column number */
  sqlite3_value **ppValue         /* OUT: Old value (or NULL pointer) */
);

/*
** CAPI3REF: Obtain new.* Values From A Changeset Iterator
**
** The pIter argument passed to this function may either be an iterator
** passed to a conflict-handler by [sqlite3changeset_apply()], or an iterator
** created by [sqlite3changeset_start()]. In the latter case, the most recent
** call to [sqlite3changeset_next()] must have returned SQLITE_ROW. 
** Furthermore, it may only be called if the type of change that the iterator
** currently points to is either [SQLITE_UPDATE] or [SQLITE_INSERT]. Otherwise,
** this function returns [SQLITE_MISUSE] and sets *ppValue to NULL.
**
** Argument iVal must be greater than or equal to 0, and less than the number
** of columns in the table affected by the current change. Otherwise,
** [SQLITE_RANGE] is returned and *ppValue is set to NULL.
**
** If successful, this function sets *ppValue to point to a protected
** sqlite3_value object containing the iVal'th value from the vector of 
** new row values stored as part of the UPDATE or INSERT change and
** returns SQLITE_OK. If the change is an UPDATE and does not include
** a new value for the requested column, *ppValue is set to NULL and 
** SQLITE_OK returned. The name of the function comes from the fact that 
** this is similar to the "new.*" columns available to update or delete 
** triggers.
**
** If some other error occurs (e.g. an OOM condition), an SQLite error code
** is returned and *ppValue is set to NULL.
*/
int sqlite3changeset_new(
  sqlite3_changeset_iter *pIter,  /* Changeset iterator */
  int iVal,                       /* Column number */
  sqlite3_value **ppValue         /* OUT: New value (or NULL pointer) */
);

/*
** CAPI3REF: Obtain Conflicting Row Values From A Changeset Iterator
**
** This function should only be used with iterator objects passed to a
** conflict-handler callback by [sqlite3changeset_apply()] with either
** [SQLITE_CHANGESET_DATA] or [SQLITE_CHANGESET_CONFLICT]. If this function
** is called on any other iterator, [SQLITE_MISUSE] is returned and *ppValue
** is set to NULL.
**
** Argument iVal must be greater than or equal to 0, and less than the number
** of columns in the table affected by the current change. Otherwise,
** [SQLITE_RANGE] is returned and *ppValue is set to NULL.
**
** If successful, this function sets *ppValue to point to a protected
** sqlite3_value object containing the iVal'th value from the 
** "conflicting row" associated with the current conflict-handler callback
** and returns SQLITE_OK.
**
** If some other error occurs (e.g. an OOM condition), an SQLite error code
** is returned and *ppValue is set to NULL.
*/
int sqlite3changeset_conflict(
  sqlite3_changeset_iter *pIter,  /* Changeset iterator */
  int iVal,                       /* Column number */
  sqlite3_value **ppValue         /* OUT: Value from conflicting row */
);

/*
** CAPI3REF: Determine The Number Of Foreign Key Constraint Violations
**
** This function may only be called with an iterator passed to an
** SQLITE_CHANGESET_FOREIGN_KEY conflict handler callback. In this case
** it sets the output variable to the total number of known foreign key
** violations in the destination database and returns SQLITE_OK.
**
** In all other cases this function returns SQLITE_MISUSE.
*/
int sqlite3changeset_fk_conflicts(
  sqlite3_changeset_iter *pIter,  /* Changeset iterator */
  int *pnOut                      /* OUT: Number of FK violations */
);


/*
** CAPI3REF: Finalize A Changeset Iterator
**
** This function is used to finalize an iterator allocated with
** [sqlite3changeset_start()].
**
** This function should only be called on iterators created using the
** [sqlite3changeset_start()] function. If an application calls this
** function with an iterator passed to a conflict-handler by
** [sqlite3changeset_apply()], [SQLITE_MISUSE] is immediately returned and the
** call has no effect.
**
** If an error was encountered within a call to an sqlite3changeset_xxx()
** function (for example an [SQLITE_CORRUPT] in [sqlite3changeset_next()] or an 
** [SQLITE_NOMEM] in [sqlite3changeset_new()]) then an error code corresponding
** to that error is returned by this function. Otherwise, SQLITE_OK is
** returned. This is to allow the following pattern (pseudo-code):
**
**   sqlite3changeset_start();
**   while( SQLITE_ROW==sqlite3changeset_next() ){
**     // Do something with change.
**   }
**   rc = sqlite3changeset_finalize();
**   if( rc!=SQLITE_OK ){
**     // An error has occurred 
**   }
*/
int sqlite3changeset_finalize(sqlite3_changeset_iter *pIter);

/*
** CAPI3REF: Invert A Changeset
**
** This function is used to "invert" a changeset object. Applying an inverted
** changeset to a database reverses the effects of applying the uninverted
** changeset. Specifically:
**
** <ul>
**   <li> Each DELETE change is changed to an INSERT, and
**   <li> Each INSERT change is changed to a DELETE, and
**   <li> For each UPDATE change, the old.* and new.* values are exchanged.
** </ul>
**
** This function does not change the order in which changes appear within
** the changeset. It merely reverses the sense of each individual change.
**
** If successful, a pointer to a buffer containing the inverted changeset
** is stored in *ppOut, the size of the same buffer is stored in *pnOut, and
** SQLITE_OK is returned. If an error occurs, both *pnOut and *ppOut are
** zeroed and an SQLite error code returned.
**
** It is the responsibility of the caller to eventually call sqlite3_free()
** on the *ppOut pointer to free the buffer allocation following a successful 
** call to this function.
**
** WARNING/TODO: This function currently assumes that the input is a valid
** changeset. If it is not, the results are undefined.
*/
int sqlite3changeset_invert(
  int nIn, const void *pIn,       /* Input changeset */
  int *pnOut, void **ppOut        /* OUT: Inverse of input */
);

/*
** CAPI3REF: Concatenate Two Changeset Objects
**
** This function is used to concatenate two changesets, A and B, into a 
** single changeset. The result is a changeset equivalent to applying
** changeset A followed by changeset B. 
**
** This function combines the two input changesets using an 
** sqlite3_changegroup object. Calling it produces similar results as the
** following code fragment:
**
**   sqlite3_changegroup *pGrp;
**   rc = sqlite3_changegroup_new(&pGrp);
**   if( rc==SQLITE_OK ) rc = sqlite3changegroup_add(pGrp, nA, pA);
**   if( rc==SQLITE_OK ) rc = sqlite3changegroup_add(pGrp, nB, pB);
**   if( rc==SQLITE_OK ){
**     rc = sqlite3changegroup_output(pGrp, pnOut, ppOut);
**   }else{
**     *ppOut = 0;
**     *pnOut = 0;
**   }
**
** Refer to the sqlite3_changegroup documentation below for details.
*/
int sqlite3changeset_concat(
  int nA,                         /* Number of bytes in buffer pA */
  void *pA,                       /* Pointer to buffer containing changeset A */
  int nB,                         /* Number of bytes in buffer pB */
  void *pB,                       /* Pointer to buffer containing changeset B */
  int *pnOut,                     /* OUT: Number of bytes in output changeset */
  void **ppOut                    /* OUT: Buffer containing output changeset */
);


/*
** CAPI3REF: Changegroup Handle
*/
typedef struct sqlite3_changegroup sqlite3_changegroup;

/*
** CAPI3REF: Create A New Changegroup Object
**
** An sqlite3_changegroup object is used to combine two or more changesets
** (or patchsets) into a single changeset (or patchset). A single changegroup
** object may combine changesets or patchsets, but not both. The output is
** always in the same format as the input.
**
** If successful, this function returns SQLITE_OK and populates (*pp) with
** a pointer to a new sqlite3_changegroup object before returning. The caller
** should eventually free the returned object using a call to 
** sqlite3changegroup_delete(). If an error occurs, an SQLite error code
** (i.e. SQLITE_NOMEM) is returned and *pp is set to NULL.
**
** The usual usage pattern for an sqlite3_changegroup object is as follows:
**
** <ul>
**   <li> It is created using a call to sqlite3changegroup_new().
**
**   <li> Zero or more changesets (or patchsets) are added to the object
**        by calling sqlite3changegroup_add().
**
**   <li> The result of combining all input changesets together is obtained 
**        by the application via a call to sqlite3changegroup_output().
**
**   <li> The object is deleted using a call to sqlite3changegroup_delete().
** </ul>
**
** Any number of calls to add() and output() may be made between the calls to
** new() and delete(), and in any order.
**
** As well as the regular sqlite3changegroup_add() and 
** sqlite3changegroup_output() functions, also available are the streaming
** versions sqlite3changegroup_add_strm() and sqlite3changegroup_output_strm().
*/
int sqlite3changegroup_new(sqlite3_changegroup **pp);

/*
** CAPI3REF: Add A Changeset To A Changegroup
**
** Add all changes within the changeset (or patchset) in buffer pData (size
** nData bytes) to the changegroup. 
**
** If the buffer contains a patchset, then all prior calls to this function
** on the same changegroup object must also have specified patchsets. Or, if
** the buffer contains a changeset, so must have the earlier calls to this
** function. Otherwise, SQLITE_ERROR is returned and no changes are added
** to the changegroup.
**
** Rows within the changeset and changegroup are identified by the values in
** their PRIMARY KEY columns. A change in the changeset is considered to
** apply to the same row as a change already present in the changegroup if
** the two rows have the same primary key.
**
** Changes to rows that do not already appear in the changegroup are
** simply copied into it. Or, if both the new changeset and the changegroup
** contain changes that apply to a single row, the final contents of the
** changegroup depends on the type of each change, as follows:
**
** <table border=1 style="margin-left:8ex;margin-right:8ex">
**   <tr><th style="white-space:pre">Existing Change  </th>
**       <th style="white-space:pre">New Change       </th>
**       <th>Output Change
**   <tr><td>INSERT <td>INSERT <td>
**       The new change is ignored. This case does not occur if the new
**       changeset was recorded immediately after the changesets already
**       added to the changegroup.
**   <tr><td>INSERT <td>UPDATE <td>
**       The INSERT change remains in the changegroup. The values in the 
**       INSERT change are modified as if the row was inserted by the
**       existing change and then updated according to the new change.
**   <tr><td>INSERT <td>DELETE <td>
**       The existing INSERT is removed from the changegroup. The DELETE is
**       not added.
**   <tr><td>UPDATE <td>INSERT <td>
**       The new change is ignored. This case does not occur if the new
**       changeset was recorded immediately after the changesets already
**       added to the changegroup.
**   <tr><td>UPDATE <td>UPDATE <td>
**       The existing UPDATE remains within the changegroup. It is amended 
**       so that the accompanying values are as if the row was updated once 
**       by the existing change and then again by the new change.
**   <tr><td>UPDATE <td>DELETE <td>
**       The existing UPDATE is replaced by the new DELETE within the
**       changegroup.
**   <tr><td>DELETE <td>INSERT <td>
**       If one or more of the column values in the row inserted by the
**       new change differ from those in the row deleted by the existing 
**       change, the existing DELETE is replaced by an UPDATE within the
**       changegroup. Otherwise, if the inserted row is exactly the same 
**       as the deleted row, the existing DELETE is simply discarded.
**   <tr><td>DELETE <td>UPDATE <td>
**       The new change is ignored. This case does not occur if the new
**       changeset was recorded immediately after the changesets already
**       added to the changegroup.
**   <tr><td>DELETE <td>DELETE <td>
**       The new change is ignored. This case does not occur if the new
**       changeset was recorded immediately after the changesets already
**       added to the changegroup.
** </table>
**
** If the new changeset contains changes to a table that is already present
** in the changegroup, then the number of columns and the position of the
** primary key columns for the table must be consistent. If this is not the
** case, this function fails with SQLITE_SCHEMA. If the input changeset
** appears to be corrupt and the corruption is detected, SQLITE_CORRUPT is
** returned. Or, if an out-of-memory condition occurs during processing, this
** function returns SQLITE_NOMEM. In all cases, if an error occurs the
** final contents of the changegroup is undefined.
**
** If no error occurs, SQLITE_OK is returned.
*/
int sqlite3changegroup_add(sqlite3_changegroup*, int nData, void *pData);

/*
** CAPI3REF: Obtain A Composite Changeset From A Changegroup
**
** Obtain a buffer containing a changeset (or patchset) representing the
** current contents of the changegroup. If the inputs to the changegroup
** were themselves changesets, the output is a changeset. Or, if the
** inputs were patchsets, the output is also a patchset.
**
** As with the output of the sqlite3session_changeset() and
** sqlite3session_patchset() functions, all changes related to a single
** table are grouped together in the output of this function. Tables appear
** in the same order as for the very first changeset added to the changegroup.
** If the second or subsequent changesets added to the changegroup contain
** changes for tables that do not appear in the first changeset, they are
** appended onto the end of the output changeset, again in the order in
** which they are first encountered.
**
** If an error occurs, an SQLite error code is returned and the output
** variables (*pnData) and (*ppData) are set to 0. Otherwise, SQLITE_OK
** is returned and the output variables are set to the size of and a 
** pointer to the output buffer, respectively. In this case it is the
** responsibility of the caller to eventually free the buffer using a
** call to sqlite3_free().
*/
int sqlite3changegroup_output(
  sqlite3_changegroup*,
  int *pnData,                    /* OUT: Size of output buffer in bytes */
  void **ppData                   /* OUT: Pointer to output buffer */
);

/*
** CAPI3REF: Delete A Changegroup Object
*/
void sqlite3changegroup_delete(sqlite3_changegroup*);

/*
** CAPI3REF: Apply A Changeset To A Database
**
** Apply a changeset to a database. This function attempts to update the
** "main" database attached to handle db with the changes found in the
** changeset passed via the second and third arguments.
**
** The fourth argument (xFilter) passed to this function is the "filter
** callback". If it is not NULL, then for each table affected by at least one
** change in the changeset, the filter callback is invoked with
** the table name as the second argument, and a copy of the context pointer
** passed as the sixth argument to this function as the first. If the "filter
** callback" returns zero, then no attempt is made to apply any changes to 
** the table. Otherwise, if the return value is non-zero or the xFilter
** argument to this function is NULL, all changes related to the table are
** attempted.
**
** For each table that is not excluded by the filter callback, this function 
** tests that the target database contains a compatible table. A table is 
** considered compatible if all of the following are true:
**
** <ul>
**   <li> The table has the same name as the name recorded in the 
**        changeset, and
**   <li> The table has the same number of columns as recorded in the 
**        changeset, and
**   <li> The table has primary key columns in the same position as 
**        recorded in the changeset.
** </ul>
**
** If there is no compatible table, it is not an error, but none of the
** changes associated with the table are applied. A warning message is issued
** via the sqlite3_log() mechanism with the error code SQLITE_SCHEMA. At most
** one such warning is issued for each table in the changeset.
**
** For each change for which there is a compatible table, an attempt is made 
** to modify the table contents according to the UPDATE, INSERT or DELETE 
** change. If a change cannot be applied cleanly, the conflict handler 
** function passed as the fifth argument to sqlite3changeset_apply() may be 
** invoked. A description of exactly when the conflict handler is invoked for 
** each type of change is below.
**
** Unlike the xFilter argument, xConflict may not be passed NULL. The results
** of passing anything other than a valid function pointer as the xConflict
** argument are undefined.
**
** Each time the conflict handler function is invoked, it must return one
** of [SQLITE_CHANGESET_OMIT], [SQLITE_CHANGESET_ABORT] or 
** [SQLITE_CHANGESET_REPLACE]. SQLITE_CHANGESET_REPLACE may only be returned
** if the second argument passed to the conflict handler is either
** SQLITE_CHANGESET_DATA or SQLITE_CHANGESET_CONFLICT. If the conflict-handler
** returns an illegal value, any changes already made are rolled back and
** the call to sqlite3changeset_apply() returns SQLITE_MISUSE. Different 
** actions are taken by sqlite3changeset_apply() depending on the value
** returned by each invocation of the conflict-handler function. Refer to
** the documentation for the three 
** [SQLITE_CHANGESET_OMIT|available return values] for details.
**
** <dl>
** <dt>DELETE Changes<dd>
**   For each DELETE change, this function checks if the target database 
**   contains a row with the same primary key value (or values) as the 
**   original row values stored in the changeset. If it does, and the values 
**   stored in all non-primary key columns also match the values stored in 
**   the changeset the row is deleted from the target database.
**
**   If a row with matching primary key values is found, but one or more of
**   the non-primary key fields contains a value different from the original
**   row value stored in the changeset, the conflict-handler function is
**   invoked with [SQLITE_CHANGESET_DATA] as the second argument.
**
**   If no row with matching primary key values is found in the database,
**   the conflict-handler function is invoked with [SQLITE_CHANGESET_NOTFOUND]
**   passed as the second argument.
**
**   If the DELETE operation is attempted, but SQLite returns SQLITE_CONSTRAINT
**   (which can only happen if a foreign key constraint is violated), the
**   conflict-handler function is invoked with [SQLITE_CHANGESET_CONSTRAINT]
**   passed as the second argument. This includes the case where the DELETE
**   operation is attempted because an earlier call to the conflict handler
**   function returned [SQLITE_CHANGESET_REPLACE].
**
** <dt>INSERT Changes<dd>
**   For each INSERT change, an attempt is made to insert the new row into
**   the database.
**
**   If the attempt to insert the row fails because the database already 
**   contains a row with the same primary key values, the conflict handler
**   function is invoked with the second argument set to 
**   [SQLITE_CHANGESET_CONFLICT].
**
**   If the attempt to insert the row fails because of some other constraint
**   violation (e.g. NOT NULL or UNIQUE), the conflict handler function is 
**   invoked with the second argument set to [SQLITE_CHANGESET_CONSTRAINT].
**   This includes the case where the INSERT operation is re-attempted because 
**   an earlier call to the conflict handler function returned 
**   [SQLITE_CHANGESET_REPLACE].
**
** <dt>UPDATE Changes<dd>
**   For each UPDATE change, this function checks if the target database 
**   contains a row with the same primary key value (or values) as the 
**   original row values stored in the changeset. If it does, and the values 
**   stored in all non-primary key columns also match the values stored in 
**   the changeset the row is updated within the target database.
**
**   If a row with matching primary key values is found, but one or more of
**   the non-primary key fields contains a value different from an original
**   row value stored in the changeset, the conflict-handler function is
**   invoked with [SQLITE_CHANGESET_DATA] as the second argument. Since
**   UPDATE changes only contain values for non-primary key fields that are
**   to be modified, only those fields need to match the original values to
**   avoid the SQLITE_CHANGESET_DATA conflict-handler callback.
**
**   If no row with matching primary key values is found in the database,
**   the conflict-handler function is invoked with [SQLITE_CHANGESET_NOTFOUND]
**   passed as the second argument.
**
**   If the UPDATE operation is attempted, but SQLite returns 
**   SQLITE_CONSTRAINT, the conflict-handler function is invoked with 
**   [SQLITE_CHANGESET_CONSTRAINT] passed as the second argument.
**   This includes the case where the UPDATE operation is attempted after 
**   an earlier call to the conflict handler function returned
**   [SQLITE_CHANGESET_REPLACE].  
** </dl>
**
** It is safe to execute SQL statements, including those that write to the
** table that the callback related to, from within the xConflict callback.
** This can be used to further customize the applications conflict
** resolution strategy.
**
** All changes made by this function are enclosed in a savepoint transaction.
** If any other error (aside from a constraint failure when attempting to
** write to the target database) occurs, then the savepoint transaction is
** rolled back, restoring the target database to its original state, and an 
** SQLite error code returned.
*/
int sqlite3changeset_apply(
  sqlite3 *db,                    /* Apply change to "main" db of this handle */
  int nChangeset,                 /* Size of changeset in bytes */
  void *pChangeset,               /* Changeset blob */
  int(*xFilter)(
    void *pCtx,                   /* Copy of sixth arg to _apply() */
    const char *zTab              /* Table name */
  ),
  int(*xConflict)(
    void *pCtx,                   /* Copy of sixth arg to _apply() */
    int eConflict,                /* DATA, MISSING, CONFLICT, CONSTRAINT */
    sqlite3_changeset_iter *p     /* Handle describing change and conflict */
  ),
  void *pCtx                      /* First argument passed to xConflict */
);

/* 
** CAPI3REF: Constants Passed To The Conflict Handler
**
** Values that may be passed as the second argument to a conflict-handler.
**
** <dl>
** <dt>SQLITE_CHANGESET_DATA<dd>
**   The conflict handler is invoked with CHANGESET_DATA as the second argument
**   when processing a DELETE or UPDATE change if a row with the required
**   PRIMARY KEY fields is present in the database, but one or more other 
**   (non primary-key) fields modified by the update do not contain the 
**   expected "before" values.
** 
**   The conflicting row, in this case, is the database row with the matching
**   primary key.
** 
** <dt>SQLITE_CHANGESET_NOTFOUND<dd>
**   The conflict handler is invoked with CHANGESET_NOTFOUND as the second
**   argument when processing a DELETE or UPDATE change if a row with the
**   required PRIMARY KEY fields is not present in the database.
** 
**   There is no conflicting row in this case. The results of invoking the
**   sqlite3changeset_conflict() API are undefined.
** 
** <dt>SQLITE_CHANGESET_CONFLICT<dd>
**   CHANGESET_CONFLICT is passed as the second argument to the conflict
**   handler while processing an INSERT change if the operation would result 
**   in duplicate primary key values.
** 
**   The conflicting row in this case is the database row with the matching
**   primary key.
**
** <dt>SQLITE_CHANGESET_FOREIGN_KEY<dd>
**   If foreign key handling is enabled, and applying a changeset leaves the
**   database in a state containing foreign key violations, the conflict 
**   handler is invoked with CHANGESET_FOREIGN_KEY as the second argument
**   exactly once before the changeset is committed. If the conflict handler
**   returns CHANGESET_OMIT, the changes, including those that caused the
**   foreign key constraint violation, are committed. Or, if it returns
**   CHANGESET_ABORT, the changeset is rolled back.
**
**   No current or conflicting row information is provided. The only function
**   it is possible to call on the supplied sqlite3_changeset_iter handle
**   is sqlite3changeset_fk_conflicts().
** 
** <dt>SQLITE_CHANGESET_CONSTRAINT<dd>
**   If any other constraint violation occurs while applying a change (i.e. 
**   a UNIQUE, CHECK or NOT NULL constraint), the conflict handler is 
**   invoked with CHANGESET_CONSTRAINT as the second argument.
** 
**   There is no conflicting row in this case. The results of invoking the
**   sqlite3changeset_conflict() API are undefined.
**
** </dl>
*/
#define SQLITE_CHANGESET_DATA        1
#define SQLITE_CHANGESET_NOTFOUND    2
#define SQLITE_CHANGESET_CONFLICT    3
#define SQLITE_CHANGESET_CONSTRAINT  4
#define SQLITE_CHANGESET_FOREIGN_KEY 5

/* 
** CAPI3REF: Constants Returned By The Conflict Handler
**
** A conflict handler callback must return one of the following three values.
**
** <dl>
** <dt>SQLITE_CHANGESET_OMIT<dd>
**   If a conflict handler returns this value no special action is taken. The
**   change that caused the conflict is not applied. The session module 
**   continues to the next change in the changeset.
**
** <dt>SQLITE_CHANGESET_REPLACE<dd>
**   This value may only be returned if the second argument to the conflict
**   handler was SQLITE_CHANGESET_DATA or SQLITE_CHANGESET_CONFLICT. If this
**   is not the case, any changes applied so far are rolled back and the 
**   call to sqlite3changeset_apply() returns SQLITE_MISUSE.
**
**   If CHANGESET_REPLACE is returned by an SQLITE_CHANGESET_DATA conflict
**   handler, then the conflicting row is either updated or deleted, depending
**   on the type of change.
**
**   If CHANGESET_REPLACE is returned by an SQLITE_CHANGESET_CONFLICT conflict
**   handler, then the conflicting row is removed from the database and a
**   second attempt to apply the change is made. If this second attempt fails,
**   the original row is restored to the database before continuing.
**
** <dt>SQLITE_CHANGESET_ABORT<dd>
**   If this value is returned, any changes applied so far are rolled back 
**   and the call to sqlite3changeset_apply() returns SQLITE_ABORT.
** </dl>
*/
#define SQLITE_CHANGESET_OMIT       0
#define SQLITE_CHANGESET_REPLACE    1
#define SQLITE_CHANGESET_ABORT      2

/*
** CAPI3REF: Streaming Versions of API functions.
**
** The six streaming API xxx_strm() functions serve similar purposes to the 
** corresponding non-streaming API functions:
**
** <table border=1 style="margin-left:8ex;margin-right:8ex">
**   <tr><th>Streaming function<th>Non-streaming equivalent</th>
**   <tr><td>sqlite3changeset_apply_str<td>[sqlite3changeset_apply] 
**   <tr><td>sqlite3changeset_concat_str<td>[sqlite3changeset_concat] 
**   <tr><td>sqlite3changeset_invert_str<td>[sqlite3changeset_invert] 
**   <tr><td>sqlite3changeset_start_str<td>[sqlite3changeset_start] 
**   <tr><td>sqlite3session_changeset_str<td>[sqlite3session_changeset] 
**   <tr><td>sqlite3session_patchset_str<td>[sqlite3session_patchset] 
** </table>
**
** Non-streaming functions that accept changesets (or patchsets) as input
** require that the entire changeset be stored in a single buffer in memory. 
** Similarly, those that return a changeset or patchset do so by returning 
** a pointer to a single large buffer allocated using sqlite3_malloc(). 
** Normally this is convenient. However, if an application running in a 
** low-memory environment is required to handle very large changesets, the
** large contiguous memory allocations required can become onerous.
**
** In order to avoid this problem, instead of a single large buffer, input
** is passed to a streaming API functions by way of a callback function that
** the sessions module invokes to incrementally request input data as it is
** required. In all cases, a pair of API function parameters such as
**
**  <pre>
**  &nbsp;     int nChangeset,
**  &nbsp;     void *pChangeset,
**  </pre>
**
** Is replaced by:
**
**  <pre>
**  &nbsp;     int (*xInput)(void *pIn, void *pData, int *pnData),
**  &nbsp;     void *pIn,
**  </pre>
**
** Each time the xInput callback is invoked by the sessions module, the first
** argument passed is a copy of the supplied pIn context pointer. The second 
** argument, pData, points to a buffer (*pnData) bytes in size. Assuming no 
** error occurs the xInput method should copy up to (*pnData) bytes of data 
** into the buffer and set (*pnData) to the actual number of bytes copied 
** before returning SQLITE_OK. If the input is completely exhausted, (*pnData) 
** should be set to zero to indicate this. Or, if an error occurs, an SQLite 
** error code should be returned. In all cases, if an xInput callback returns
** an error, all processing is abandoned and the streaming API function
** returns a copy of the error code to the caller.
**
** In the case of sqlite3changeset_start_strm(), the xInput callback may be
** invoked by the sessions module at any point during the lifetime of the
** iterator. If such an xInput callback returns an error, the iterator enters
** an error state, whereby all subsequent calls to iterator functions 
** immediately fail with the same error code as returned by xInput.
**
** Similarly, streaming API functions that return changesets (or patchsets)
** return them in chunks by way of a callback function instead of via a
** pointer to a single large buffer. In this case, a pair of parameters such
** as:
**
**  <pre>
**  &nbsp;     int *pnChangeset,
**  &nbsp;     void **ppChangeset,
**  </pre>
**
** Is replaced by:
**
**  <pre>
**  &nbsp;     int (*xOutput)(void *pOut, const void *pData, int nData),
**  &nbsp;     void *pOut
**  </pre>
**
** The xOutput callback is invoked zero or more times to return data to
** the application. The first parameter passed to each call is a copy of the
** pOut pointer supplied by the application. The second parameter, pData,
** points to a buffer nData bytes in size containing the chunk of output
** data being returned. If the xOutput callback successfully processes the
** supplied data, it should return SQLITE_OK to indicate success. Otherwise,
** it should return some other SQLite error code. In this case processing
** is immediately abandoned and the streaming API function returns a copy
** of the xOutput error code to the application.
**
** The sessions module never invokes an xOutput callback with the third 
** parameter set to a value less than or equal to zero. Other than this,
** no guarantees are made as to the size of the chunks of data returned.
*/
int sqlite3changeset_apply_strm(
  sqlite3 *db,                    /* Apply change to "main" db of this handle */
  int (*xInput)(void *pIn, void *pData, int *pnData), /* Input function */
  void *pIn,                                          /* First arg for xInput */
  int(*xFilter)(
    void *pCtx,                   /* Copy of sixth arg to _apply() */
    const char *zTab              /* Table name */
  ),
  int(*xConflict)(
    void *pCtx,                   /* Copy of sixth arg to _apply() */
    int eConflict,                /* DATA, MISSING, CONFLICT, CONSTRAINT */
    sqlite3_changeset_iter *p     /* Handle describing change and conflict */
  ),
  void *pCtx                      /* First argument passed to xConflict */
);
int sqlite3changeset_concat_strm(
  int (*xInputA)(void *pIn, void *pData, int *pnData),
  void *pInA,
  int (*xInputB)(void *pIn, void *pData, int *pnData),
  void *pInB,
  int (*xOutput)(void *pOut, const void *pData, int nData),
  void *pOut
);
int sqlite3changeset_invert_strm(
  int (*xInput)(void *pIn, void *pData, int *pnData),
  void *pIn,
  int (*xOutput)(void *pOut, const void *pData, int nData),
  void *pOut
);
int sqlite3changeset_start_strm(
  sqlite3_changeset_iter **pp,
  int (*xInput)(void *pIn, void *pData, int *pnData),
  void *pIn
);
int sqlite3session_changeset_strm(
  sqlite3_session *pSession,
  int (*xOutput)(void *pOut, const void *pData, int nData),
  void *pOut
);
int sqlite3session_patchset_strm(
  sqlite3_session *pSession,
  int (*xOutput)(void *pOut, const void *pData, int nData),
  void *pOut
);
int sqlite3changegroup_add_strm(sqlite3_changegroup*, 
    int (*xInput)(void *pIn, void *pData, int *pnData),
    void *pIn
);
int sqlite3changegroup_output_strm(sqlite3_changegroup*,
    int (*xOutput)(void *pOut, const void *pData, int nData), 
    void *pOut
);


/*
** Make sure we can call this stuff from C++.
*/
#ifdef __cplusplus
}
#endif

#endif  /* !defined(__SQLITESESSION_H_) && defined(SQLITE_ENABLE_SESSION) */

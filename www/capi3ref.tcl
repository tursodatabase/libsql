set rcsid {$Id: capi3ref.tcl,v 1.14 2004/10/10 17:24:54 drh Exp $}
source common.tcl
header {C/C++ Interface For SQLite Version 3}
puts {
<h2>C/C++ Interface For SQLite Version 3</h2>
}

proc api {name prototype desc {notused x}} {
  global apilist
  if {$name==""} {
    regsub -all {sqlite3_[a-z0-9_]+\(} $prototype \
      {[lappend name [string trimright & (]]} x1
    subst $x1
  }
  lappend apilist [list $name $prototype $desc]
}

api {result-codes} {
#define SQLITE_OK           0   /* Successful result */
#define SQLITE_ERROR        1   /* SQL error or missing database */
#define SQLITE_INTERNAL     2   /* An internal logic error in SQLite */
#define SQLITE_PERM         3   /* Access permission denied */
#define SQLITE_ABORT        4   /* Callback routine requested an abort */
#define SQLITE_BUSY         5   /* The database file is locked */
#define SQLITE_LOCKED       6   /* A table in the database is locked */
#define SQLITE_NOMEM        7   /* A malloc() failed */
#define SQLITE_READONLY     8   /* Attempt to write a readonly database */
#define SQLITE_INTERRUPT    9   /* Operation terminated by sqlite_interrupt() */
#define SQLITE_IOERR       10   /* Some kind of disk I/O error occurred */
#define SQLITE_CORRUPT     11   /* The database disk image is malformed */
#define SQLITE_NOTFOUND    12   /* (Internal Only) Table or record not found */
#define SQLITE_FULL        13   /* Insertion failed because database is full */
#define SQLITE_CANTOPEN    14   /* Unable to open the database file */
#define SQLITE_PROTOCOL    15   /* Database lock protocol error */
#define SQLITE_EMPTY       16   /* (Internal Only) Database table is empty */
#define SQLITE_SCHEMA      17   /* The database schema changed */
#define SQLITE_TOOBIG      18   /* Too much data for one row of a table */
#define SQLITE_CONSTRAINT  19   /* Abort due to constraint violation */
#define SQLITE_MISMATCH    20   /* Data type mismatch */
#define SQLITE_MISUSE      21   /* Library used incorrectly */
#define SQLITE_NOLFS       22   /* Uses OS features not supported on host */
#define SQLITE_AUTH        23   /* Authorization denied */
#define SQLITE_ROW         100  /* sqlite_step() has another row ready */
#define SQLITE_DONE        101  /* sqlite_step() has finished executing */
} {
Many SQLite functions return an integer result code from the set shown
above in order to indicates success or failure.
}

api {} {
  const char *sqlite3_libversion(void);
} {
  Return a pointer to a string which contains the version number of
  the library.  The same string is available in the global
  variable named "sqlite3_version".  This interface is provided since
  windows is unable to access global variables in DLLs.
}

api {} {
  void *sqlite3_aggregate_context(sqlite3_context*, int nBytes);
} {
  Aggregate functions use the following routine to allocate
  a structure for storing their state.  The first time this routine
  is called for a particular aggregate, a new structure of size nBytes
  is allocated, zeroed, and returned.  On subsequent calls (for the
  same aggregate instance) the same buffer is returned.  The implementation
  of the aggregate can use the returned buffer to accumulate data.

  The buffer allocated is freed automatically by SQLite.
}

api {} {
  int sqlite3_aggregate_count(sqlite3_context*);
} {
  The next routine returns the number of calls to xStep for a particular
  aggregate function instance.  The current call to xStep counts so this
  routine always returns at least 1.
}

api {} {
  int sqlite3_bind_blob(sqlite3_stmt*, int, const void*, int n, void(*)(void*));
  int sqlite3_bind_double(sqlite3_stmt*, int, double);
  int sqlite3_bind_int(sqlite3_stmt*, int, int);
  int sqlite3_bind_int64(sqlite3_stmt*, int, long long int);
  int sqlite3_bind_null(sqlite3_stmt*, int);
  int sqlite3_bind_text(sqlite3_stmt*, int, const char*, int n, void(*)(void*));
  int sqlite3_bind_text16(sqlite3_stmt*, int, const void*, int n, void(*)(void*));
  #define SQLITE_STATIC      ((void(*)(void *))0)
  #define SQLITE_TRANSIENT   ((void(*)(void *))-1)
} {
 In the SQL strings input to sqlite3_prepare() and sqlite3_prepare16(),
 one or more literals can be replace by a wildcard "?" or ":AAA" where
 AAA is an alphanumeric identifier.
 The value of these wildcard literals (also called "host parameter names")
 can be set using these routines.

 The first parameter is a pointer to the sqlite3_stmt
 structure returned from sqlite3_prepare().  The second parameter is the
 index of the wildcard.  The first wildcard has an index of 1. 

 The fifth parameter to sqlite3_bind_blob(), sqlite3_bind_text(), and
 sqlite3_bind_text16() is a destructor used to dispose of the BLOB or
 text after SQLite has finished with it.  If the fifth argument is the
 special value SQLITE_STATIC, then the library assumes that the information
 is in static, unmanaged space and does not need to be freed.  If the
 fifth argument has the value SQLITE_TRANSIENT, then SQLite makes its
 own private copy of the data.

 The sqlite3_bind_*() routine must be called after
 sqlite3_prepare() or sqlite3_reset() and before sqlite3_step().
 Bindings are not reset by the sqlite3_reset() routine.
 Unbound wildcards are interpreted as NULL.
}

api {} {
  int sqlite3_bind_parameter_count(sqlite3_stmt*);
} {
  Return the number of wildcards in the precompiled statement given as
  the argument.
}

api {} {
  const char *sqlite3_bind_parameter_name(sqlite3_stmt*, int n);
} {
  Return the name of the n-th wildcard in the precompiled statement.
  Wildcards of the form ":AAA" have a name which is the string ":AAA".
  Wildcards of the form "?" or "?NNN" have no name.

  If the value n is out of range or if the n-th wildcard is nameless,
  then NULL is returned.  The returned string is always in the
  UTF-8 encoding.
}

api {} {
  int sqlite3_bind_parameter_index(sqlite3_stmt*, const char *zName);
} {
  Return the index of the wildcard with the given name.
  The name must match exactly.
  If there is no wildcard with the given name, return 0.
  The string zName is always in the UTF-8 encoding.
}

api {} {
  int sqlite3_busy_handler(sqlite3*, int(*)(void*,int), void*);
} {
 This routine identifies a callback function that is invoked
 whenever an attempt is made to open a database table that is
 currently locked by another process or thread.  If the busy callback
 is NULL, then sqlite3_exec() returns SQLITE_BUSY immediately if
 it finds a locked table.  If the busy callback is not NULL, then
 sqlite3_exec() invokes the callback with two arguments.  The
 second argument is the number of prior calls to the busy callback
 for the same lock.  If the
 busy callback returns 0, then sqlite3_exec() immediately returns
 SQLITE_BUSY.  If the callback returns non-zero, then sqlite3_exec()
 tries to open the table again and the cycle repeats.

 The default busy callback is NULL.

 Sqlite is re-entrant, so the busy handler may start a new query. 
 (It is not clear why anyone would every want to do this, but it
 is allowed, in theory.)  But the busy handler may not close the
 database.  Closing the database from a busy handler will delete 
 data structures out from under the executing query and will 
 probably result in a coredump.
}

api {} {
  int sqlite3_busy_timeout(sqlite3*, int ms);
} {
 This routine sets a busy handler that sleeps for a while when a
 table is locked.  The handler will sleep multiple times until 
 at least "ms" milliseconds of sleeping have been done.  After
 "ms" milliseconds of sleeping, the handler returns 0 which
 causes sqlite3_exec() to return SQLITE_BUSY.

 Calling this routine with an argument less than or equal to zero
 turns off all busy handlers.
}

api {} {
  int sqlite3_changes(sqlite3*);
} {
 This function returns the number of database rows that were changed
 (or inserted or deleted) by the most recently completed
 INSERT, UPDATE, or DELETE
 statement.  Only changes that are directly specified by the INSERT,
 UPDATE, or DELETE statement are counted.  Auxiliary changes caused by
 triggers are not counted.  Use the sqlite3_total_changes() function
 to find the total number of changes including changes caused by triggers.

 Within the body of a trigger, the sqlite3_changes() function does work
 to report the number of rows that were changed for the most recently
 completed INSERT, UPDATE, or DELETE statement within the trigger body.

 SQLite implements the command "DELETE FROM table" without a WHERE clause
 by dropping and recreating the table.  (This is much faster than going
 through and deleting individual elements form the table.)  Because of
 this optimization, the change count for "DELETE FROM table" will be
 zero regardless of the number of elements that were originally in the
 table. To get an accurate count of the number of rows deleted, use
 "DELETE FROM table WHERE 1" instead.
}

api {} {
  int sqlite3_total_changes(sqlite3*);
} {
  This function returns the total number of database rows that have
  be modified, inserted, or deleted since the database connection was
  created using sqlite3_open().  All changes are counted, including
  changes by triggers and changes to TEMP and auxiliary databases.
  Except, changes to the SQLITE_MASTER table (caused by statements 
  such as CREATE TABLE) are not counted.  Nor are changes counted when
  an entire table is deleted using DROP TABLE.

  See also the sqlite3_changes() API.

  SQLite implements the command "DELETE FROM table" without a WHERE clause
  by dropping and recreating the table.  (This is much faster than going
  through and deleting individual elements form the table.)  Because of
  this optimization, the change count for "DELETE FROM table" will be
  zero regardless of the number of elements that were originally in the
  table. To get an accurate count of the number of rows deleted, use
  "DELETE FROM table WHERE 1" instead.
}

api {} {
  int sqlite3_close(sqlite3*);
} {
  Call this function with a pointer to a structure that was previously
  returned from sqlite3_open() or sqlite3_open16()
  and the corresponding database will by closed.

  SQLITE_OK is returned if the close is successful.  If there are
  prepared statements that have not been finalized, then SQLITE_BUSY
  is returned.  SQLITE_ERROR might be returned if the argument is not
  a valid connection pointer returned by sqlite3_open() or if the connection
  pointer has been closed previously.
}

api {} {
const void *sqlite3_column_blob(sqlite3_stmt*, int iCol);
int sqlite3_column_bytes(sqlite3_stmt*, int iCol);
int sqlite3_column_bytes16(sqlite3_stmt*, int iCol);
double sqlite3_column_double(sqlite3_stmt*, int iCol);
int sqlite3_column_int(sqlite3_stmt*, int iCol);
long long int sqlite3_column_int64(sqlite3_stmt*, int iCol);
const unsigned char *sqlite3_column_text(sqlite3_stmt*, int iCol);
const void *sqlite3_column_text16(sqlite3_stmt*, int iCol);
int sqlite3_column_type(sqlite3_stmt*, int iCol);
#define SQLITE_INTEGER  1
#define SQLITE_FLOAT    2
#define SQLITE_TEXT     3
#define SQLITE_BLOB     4
#define SQLITE_NULL     5
} {
 These routines returns information about the information
 in a single column of the current result row of a query.  In every
 case the first parameter is a pointer to the SQL statement that is being
 executed (the sqlite_stmt* that was returned from sqlite3_prepare()) and
 the second argument is the index of the column for which information 
 should be returned.  iCol is zero-indexed.  The left-most column as an
 index of 0.

 If the SQL statement is not currently point to a valid row, or if the
 the column index is out of range, the result is undefined.

 If the result is a BLOB then the sqlite3_column_bytes() routine returns
 the number of bytes in that BLOB.  No type conversions occur.
 If the result is a string (or a number since a number can be converted
 into a string) then sqlite3_column_bytes() converts
 the value into a UTF-8 string and returns
 the number of bytes in the resulting string.  The value returned does
 not include the \\000 terminator at the end of the string.  The
 sqlite3_column_bytes16() routine converts the value into a UTF-16
 encoding and returns the number of bytes (not characters) in the
 resulting string.  The \\u0000 terminator is not included in this count.

 These routines attempt to convert the value where appropriate.  For
 example, if the internal representation is FLOAT and a text result
 is requested, sprintf() is used internally to do the conversion
 automatically.  The following table details the conversions that
 are applied:

<blockquote>
<table border="1">
<tr><th>Internal Type</th><th>Requested Type</th><th>Conversion</th></tr>
<tr><td> NULL    </td><td> INTEGER</td><td>Result is 0</td></tr>
<tr><td> NULL </td><td>    FLOAT </td><td> Result is 0.0</td></tr>
<tr><td> NULL </td><td>    TEXT </td><td>  Result is an empty string</td></tr>
<tr><td> NULL </td><td>    BLOB </td><td>  Result is a zero-length BLOB</td></tr>
<tr><td> INTEGER </td><td> FLOAT </td><td> Convert from integer to float</td></tr>
<tr><td> INTEGER </td><td> TEXT </td><td>  ASCII rendering of the integer</td></tr>
<tr><td> INTEGER </td><td> BLOB </td><td>  Same as for INTEGER->TEXT</td></tr>
<tr><td> FLOAT </td><td>   INTEGER</td><td>Convert from float to integer</td></tr>
<tr><td> FLOAT </td><td>   TEXT </td><td>  ASCII rendering of the float</td></tr>
<tr><td> FLOAT </td><td>   BLOB </td><td>  Same as FLOAT->TEXT</td></tr>
<tr><td> TEXT </td><td>    INTEGER</td><td>Use atoi()</td></tr>
<tr><td> TEXT </td><td>    FLOAT </td><td> Use atof()</td></tr>
<tr><td> TEXT </td><td>    BLOB </td><td>  No change</td></tr>
<tr><td> BLOB </td><td>    INTEGER</td><td>Convert to TEXT then use atoi()</td></tr>
<tr><td> BLOB </td><td>    FLOAT </td><td> Convert to TEXT then use atof()</td></tr>
<tr><td> BLOB </td><td>    TEXT </td><td>  Add a \\000 terminator if needed</td></tr>
</table>
</blockquote>
}

api {} {
int sqlite3_column_count(sqlite3_stmt *pStmt);
} {
 Return the number of columns in the result set returned by the prepared
 SQL statement. This routine returns 0 if pStmt is an SQL statement
 that does not return data (for example an UPDATE).

 See also sqlite3_data_count().
}

api {} {
const char *sqlite3_column_decltype(sqlite3_stmt *, int i);
const void *sqlite3_column_decltype16(sqlite3_stmt*,int);
} {
 The first parameter is a prepared SQL statement. If this statement
 is a SELECT statement, the Nth column of the returned result set 
 of the SELECT is a table column then the declared type of the table
 column is returned. If the Nth column of the result set is not at table
 column, then a NULL pointer is returned. The returned string is 
 UTF-8 encoded for sqlite3_column_decltype() and UTF-16 encoded
 for sqlite3_column_decltype16().
 For example, in the database schema:

 <blockquote><pre>
 CREATE TABLE t1(c1 INTEGER);
 </pre></blockquote>

 And the following statement compiled:

 <blockquote><pre>
 SELECT c1 + 1, 0 FROM t1;
 </pre></blockquote>

 Then this routine would return the string "INTEGER" for the second
 result column (i==1), and a NULL pointer for the first result column
 (i==0).
}

api {} {
const char *sqlite3_column_name(sqlite3_stmt*,int);
const void *sqlite3_column_name16(sqlite3_stmt*,int);
} {
 The first parameter is a prepared SQL statement. This function returns
 the column heading for the Nth column of that statement, where N is the
 second function parameter.  The string returned is UTF-8 for
 sqlite3_column_name() and UTF-16 for sqlite3_column_name16().
}

api {} {
void *sqlite3_commit_hook(sqlite3*, int(*xCallback)(void*), void *pArg);
} {
 <i>Experimental</i>

 Register a callback function to be invoked whenever a new transaction
 is committed.  The pArg argument is passed through to the callback.
 callback.  If the callback function returns non-zero, then the commit
 is converted into a rollback.

 If another function was previously registered, its pArg value is returned.
 Otherwise NULL is returned.

 Registering a NULL function disables the callback.  Only a single commit
 hook callback can be registered at a time.
}

api {} {
int sqlite3_complete(const char *sql);
int sqlite3_complete16(const void *sql);
} {
 These functions return true if the given input string comprises
 one or more complete SQL statements.
 The parameter must be a nul-terminated UTF-8 string for sqlite3_complete()
 and a nul-terminated UTF-16 string for sqlite3_complete16().

 The algorithm is simple.  If the last token other than spaces
 and comments is a semicolon, then return true.  otherwise return
 false.
} {}

api {} {
int sqlite3_create_collation(
  sqlite3*, 
  const char *zName, 
  int pref16, 
  void*,
  int(*xCompare)(void*,int,const void*,int,const void*)
);
int sqlite3_create_collation16(
  sqlite3*, 
  const char *zName, 
  int pref16, 
  void*,
  int(*xCompare)(void*,int,const void*,int,const void*)
);
#define SQLITE_UTF8     1
#define SQLITE_UTF16BE  2
#define SQLITE_UTF16LE  3
#define SQLITE_UTF16    4
} {
 These two functions are used to add new collation sequences to the
 sqlite3 handle specified as the first argument. 

 The name of the new collation sequence is specified as a UTF-8 string
 for sqlite3_create_collation() and a UTF-16 string for
 sqlite3_create_collation16(). In both cases the name is passed as the
 second function argument.

 The third argument must be one of the constants SQLITE_UTF8,
 SQLITE_UTF16LE or SQLITE_UTF16BE, indicating that the user-supplied
 routine expects to be passed pointers to strings encoded using UTF-8,
 UTF-16 little-endian or UTF-16 big-endian respectively.  The
 SQLITE_UTF16 constant indicates that text strings are expected in
 UTF-16 in the native byte order of the host machine.

 A pointer to the user supplied routine must be passed as the fifth
 argument. If it is NULL, this is the same as deleting the collation
 sequence (so that SQLite cannot call it anymore). Each time the user
 supplied function is invoked, it is passed a copy of the void* passed as
 the fourth argument to sqlite3_create_collation() or
 sqlite3_create_collation16() as its first parameter.

 The remaining arguments to the user-supplied routine are two strings,
 each represented by a [length, data] pair and encoded in the encoding
 that was passed as the third argument when the collation sequence was
 registered. The user routine should return negative, zero or positive if
 the first string is less than, equal to, or greater than the second
 string. i.e. (STRING1 - STRING2).
}

api {} {
int sqlite3_collation_needed(
  sqlite3*, 
  void*, 
  void(*)(void*,sqlite3*,int eTextRep,const char*)
);
int sqlite3_collation_needed16(
  sqlite3*, 
  void*,
  void(*)(void*,sqlite3*,int eTextRep,const void*)
);
} {
 To avoid having to register all collation sequences before a database
 can be used, a single callback function may be registered with the
 database handle to be called whenever an undefined collation sequence is
 required.

 If the function is registered using the sqlite3_collation_needed() API,
 then it is passed the names of undefined collation sequences as strings
 encoded in UTF-8. If sqlite3_collation_needed16() is used, the names
 are passed as UTF-16 in machine native byte order. A call to either
 function replaces any existing callback.

 When the user-function is invoked, the first argument passed is a copy
 of the second argument to sqlite3_collation_needed() or
 sqlite3_collation_needed16(). The second argument is the database
 handle. The third argument is one of SQLITE_UTF8, SQLITE_UTF16BE or
 SQLITE_UTF16LE, indicating the most desirable form of the collation
 sequence function required. The fourth parameter is the name of the
 required collation sequence.

 The collation sequence is returned to SQLite by a collation-needed
 callback using the sqlite3_create_collation() or
 sqlite3_create_collation16() APIs, described above.
}

api {} {
int sqlite3_create_function(
  sqlite3 *,
  const char *zFunctionName,
  int nArg,
  int eTextRep,
  void*,
  void (*xFunc)(sqlite3_context*,int,sqlite3_value**),
  void (*xStep)(sqlite3_context*,int,sqlite3_value**),
  void (*xFinal)(sqlite3_context*)
);
int sqlite3_create_function16(
  sqlite3*,
  const void *zFunctionName,
  int nArg,
  int eTextRep,
  void*,
  void (*xFunc)(sqlite3_context*,int,sqlite3_value**),
  void (*xStep)(sqlite3_context*,int,sqlite3_value**),
  void (*xFinal)(sqlite3_context*)
);
#define SQLITE_UTF8     1
#define SQLITE_UTF16    2
#define SQLITE_UTF16BE  3
#define SQLITE_UTF16LE  4
#define SQLITE_ANY      5
} {
 These two functions are used to add user functions or aggregates
 implemented in C to the SQL language interpreted by SQLite. The
 difference only between the two is that the second parameter, the
 name of the (scalar) function or aggregate, is encoded in UTF-8 for
 sqlite3_create_function() and UTF-16 for sqlite3_create_function16().

 The first argument is the database handle that the new function or
 aggregate is to be added to. If a single program uses more than one
 database handle internally, then user functions or aggregates must 
 be added individually to each database handle with which they will be
 used.

 The third parameter is the number of arguments that the function or
 aggregate takes. If this parameter is negative, then the function or
 aggregate may take any number of arguments.

 The sixth, seventh and  eighth, xFunc, xStep and xFinal, are
 pointers to user implemented C functions that implement the user
 function or aggregate. A scalar function requires an implementation of
 the xFunc callback only, NULL pointers should be passed as the xStep
 and xFinal parameters. An aggregate function requires an implementation
 of xStep and xFinal, but NULL should be passed for xFunc. To delete an
 existing user function or aggregate, pass NULL for all three function
 callback. Specifying an inconstant set of callback values, such as an
 xFunc and an xFinal, or an xStep but no xFinal, SQLITE_ERROR is
 returned.
}

api {} {
int sqlite3_data_count(sqlite3_stmt *pStmt);
} {
 Return the number of values in the current row of the result set.

 After a call to sqlite3_step() that returns SQLITE_ROW, this routine
 will return the same value as the sqlite3_column_count() function.
 After sqlite3_step() has returned an SQLITE_DONE, SQLITE_BUSY or
 error code, or before sqlite3_step() has been called on a 
 prepared SQL statement, this routine returns zero.
}

api {} {
int sqlite3_errcode(sqlite3 *db);
} {
 Return the error code for the most recent failed sqlite3_* API call associated
 with sqlite3 handle 'db'.  If a prior API call failed but the most recent
 API call succeeded, the return value from this routine is undefined. 

 Calls to many sqlite3_* functions set the error code and string returned
 by sqlite3_errcode(), sqlite3_errmsg() and sqlite3_errmsg16()
 (overwriting the previous values). Note that calls to sqlite3_errcode(),
 sqlite3_errmsg() and sqlite3_errmsg16() themselves do not affect the
 results of future invocations.  Calls to API routines that do not return
 an error code (examples: sqlite3_data_count() or sqlite3_mprintf()) do
 not change the error code returned by this routine.

 Assuming no other intervening sqlite3_* API calls are made, the error
 code returned by this function is associated with the same error as
 the strings returned by sqlite3_errmsg() and sqlite3_errmsg16().
} {}

api {} {
const char *sqlite3_errmsg(sqlite3*);
const void *sqlite3_errmsg16(sqlite3*);
} {
 Return a pointer to a UTF-8 encoded string (sqlite3_errmsg)
 or a UTF-16 encoded string (sqlite3_errmsg16) describing in English the
 error condition for the most recent sqlite3_* API call. The returned
 string is always terminated by an 0x00 byte.

 The string "not an error" is returned when the most recent API call was
 successful.
}

api {} {
int sqlite3_exec(
  sqlite3*,                     /* An open database */
  const char *sql,              /* SQL to be executed */
  sqlite_callback,              /* Callback function */
  void *,                       /* 1st argument to callback function */
  char **errmsg                 /* Error msg written here */
);
} {
 A function to executes one or more statements of SQL.

 If one or more of the SQL statements are queries, then
 the callback function specified by the 3rd parameter is
 invoked once for each row of the query result.  This callback
 should normally return 0.  If the callback returns a non-zero
 value then the query is aborted, all subsequent SQL statements
 are skipped and the sqlite3_exec() function returns the SQLITE_ABORT.

 The 4th parameter is an arbitrary pointer that is passed
 to the callback function as its first parameter.

 The 2nd parameter to the callback function is the number of
 columns in the query result.  The 3rd parameter to the callback
 is an array of strings holding the values for each column.
 The 4th parameter to the callback is an array of strings holding
 the names of each column.

 The callback function may be NULL, even for queries.  A NULL
 callback is not an error.  It just means that no callback
 will be invoked.

 If an error occurs while parsing or evaluating the SQL (but
 not while executing the callback) then an appropriate error
 message is written into memory obtained from malloc() and
 *errmsg is made to point to that message.  The calling function
 is responsible for freeing the memory that holds the error
 message.   Use sqlite3_free() for this.  If errmsg==NULL,
 then no error message is ever written.

 The return value is is SQLITE_OK if there are no errors and
 some other return code if there is an error.  The particular
 return value depends on the type of error. 

 If the query could not be executed because a database file is
 locked or busy, then this function returns SQLITE_BUSY.  (This
 behavior can be modified somewhat using the sqlite3_busy_handler()
 and sqlite3_busy_timeout() functions.)
} {}

api {} {
int sqlite3_finalize(sqlite3_stmt *pStmt);
} {
 The sqlite3_finalize() function is called to delete a prepared
 SQL statement obtained by a previous call to sqlite3_prepare()
 or sqlite3_prepare16(). If the statement was executed successfully, or
 not executed at all, then SQLITE_OK is returned. If execution of the
 statement failed then an error code is returned. 

 All prepared statements must finalized before sqlite3_close() is
 called or else the close will fail with a return code of SQLITE_BUSY.

 This routine can be called at any point during the execution of the
 virtual machine.  If the virtual machine has not completed execution
 when this routine is called, that is like encountering an error or
 an interrupt.  (See sqlite3_interrupt().)  Incomplete updates may be
 rolled back and transactions canceled,  depending on the circumstances,
 and the result code returned will be SQLITE_ABORT.
}

api {} {
void sqlite3_free(char *z);
} {
 Use this routine to free memory obtained from 
 sqlite3_mprintf() or sqlite3_vmprintf().
}

api {} {
int sqlite3_get_table(
  sqlite3*,              /* An open database */
  const char *sql,       /* SQL to be executed */
  char ***resultp,       /* Result written to a char *[]  that this points to */
  int *nrow,             /* Number of result rows written here */
  int *ncolumn,          /* Number of result columns written here */
  char **errmsg          /* Error msg written here */
);
void sqlite3_free_table(char **result);
} {
 This next routine is really just a wrapper around sqlite3_exec().
 Instead of invoking a user-supplied callback for each row of the
 result, this routine remembers each row of the result in memory
 obtained from malloc(), then returns all of the result after the
 query has finished. 

 As an example, suppose the query result where this table:

 <pre>
        Name        | Age
        -----------------------
        Alice       | 43
        Bob         | 28
        Cindy       | 21
 </pre>

 If the 3rd argument were &azResult then after the function returns
 azResult will contain the following data:

 <pre>
        azResult[0] = "Name";
        azResult[1] = "Age";
        azResult[2] = "Alice";
        azResult[3] = "43";
        azResult[4] = "Bob";
        azResult[5] = "28";
        azResult[6] = "Cindy";
        azResult[7] = "21";
 </pre>

 Notice that there is an extra row of data containing the column
 headers.  But the *nrow return value is still 3.  *ncolumn is
 set to 2.  In general, the number of values inserted into azResult
 will be ((*nrow) + 1)*(*ncolumn).

 After the calling function has finished using the result, it should 
 pass the result data pointer to sqlite3_free_table() in order to 
 release the memory that was malloc-ed.  Because of the way the 
 malloc() happens, the calling function must not try to call 
 malloc() directly.  Only sqlite3_free_table() is able to release 
 the memory properly and safely.

 The return value of this routine is the same as from sqlite3_exec().
}

api {sqlite3_interrupt} {
 void sqlite3_interrupt(sqlite3*);
} {
 This function causes any pending database operation to abort and
 return at its earliest opportunity.  This routine is typically
 called in response to a user action such as pressing "Cancel"
 or Ctrl-C where the user wants a long query operation to halt
 immediately.
} {}

api {} {
long long int sqlite3_last_insert_rowid(sqlite3*);
} {
 Each entry in an SQLite table has a unique integer key.  (The key is
 the value of the INTEGER PRIMARY KEY column if there is such a column,
 otherwise the key is generated at random.  The unique key is always
 available as the ROWID, OID, or _ROWID_ column.)  The following routine
 returns the integer key of the most recent insert in the database.

 This function is similar to the mysql_insert_id() function from MySQL.
} {}

api {} {
char *sqlite3_mprintf(const char*,...);
char *sqlite3_vmprintf(const char*, va_list);
} {
 These routines are variants of the "sprintf()" from the
 standard C library.  The resulting string is written into memory
 obtained from malloc() so that there is never a possibility of buffer
 overflow.  These routines also implement some additional formatting
 options that are useful for constructing SQL statements.

 The strings returned by these routines should be freed by calling
 sqlite3_free().

 All of the usual printf formatting options apply.  In addition, there
 is a "%q" option.  %q works like %s in that it substitutes a null-terminated
 string from the argument list.  But %q also doubles every '\\'' character.
 %q is designed for use inside a string literal.  By doubling each '\\''
 character it escapes that character and allows it to be inserted into
 the string.

 For example, so some string variable contains text as follows:

 <blockquote><pre>
  char *zText = "It's a happy day!";
 </pre></blockquote>

 One can use this text in an SQL statement as follows:

 <blockquote><pre>
  sqlite3_exec_printf(db, "INSERT INTO table VALUES('%q')",
       callback1, 0, 0, zText);
  </pre></blockquote>

 Because the %q format string is used, the '\\'' character in zText
 is escaped and the SQL generated is as follows:

 <blockquote><pre>
  INSERT INTO table1 VALUES('It''s a happy day!')
 </pre></blockquote>

 This is correct.  Had we used %s instead of %q, the generated SQL
 would have looked like this:

  <blockquote><pre>
  INSERT INTO table1 VALUES('It's a happy day!');
  </pre></blockquote>

 This second example is an SQL syntax error.  As a general rule you
 should always use %q instead of %s when inserting text into a string 
 literal.
} {}

api {} {
int sqlite3_open(
  const char *filename,   /* Database filename (UTF-8) */
  sqlite3 **ppDb          /* OUT: SQLite db handle */
);
int sqlite3_open16(
  const void *filename,   /* Database filename (UTF-16) */
  sqlite3 **ppDb          /* OUT: SQLite db handle */
);
} {
 Open the sqlite database file "filename".  The "filename" is UTF-8
 encoded for sqlite3_open() and UTF-16 encoded in the native byte order
 for sqlite3_open16().  An sqlite3* handle is returned in *ppDb, even
 if an error occurs. If the database is opened (or created) successfully,
 then SQLITE_OK is returned. Otherwise an error code is returned. The
 sqlite3_errmsg() or sqlite3_errmsg16()  routines can be used to obtain
 an English language description of the error.

 If the database file does not exist, then a new database will be created
 as needed.
 The encoding for the database will be UTF-8 if sqlite3_open() is called and
 UTF-16 if sqlite3_open16 is used.

 Whether or not an error occurs when it is opened, resources associated
 with the sqlite3* handle should be released by passing it to
 sqlite3_close() when it is no longer required.
}

api {} {
int sqlite3_prepare(
  sqlite3 *db,            /* Database handle */
  const char *zSql,       /* SQL statement, UTF-8 encoded */
  int nBytes,             /* Length of zSql in bytes. */
  sqlite3_stmt **ppStmt,  /* OUT: Statement handle */
  const char **pzTail     /* OUT: Pointer to unused portion of zSql */
);
int sqlite3_prepare16(
  sqlite3 *db,            /* Database handle */
  const void *zSql,       /* SQL statement, UTF-16 encoded */
  int nBytes,             /* Length of zSql in bytes. */
  sqlite3_stmt **ppStmt,  /* OUT: Statement handle */
  const void **pzTail     /* OUT: Pointer to unused portion of zSql */
);
} {
 To execute an SQL query, it must first be compiled into a byte-code
 program using one of the following routines. The only difference between
 them is that the second argument, specifying the SQL statement to
 compile, is assumed to be encoded in UTF-8 for the sqlite3_prepare()
 function and UTF-16 for sqlite3_prepare16().

 The first parameter "db" is an SQLite database handle. The second
 parameter "zSql" is the statement to be compiled, encoded as either
 UTF-8 or UTF-16 (see above). If the next parameter, "nBytes", is less
 than zero, then zSql is read up to the first nul terminator.  If
 "nBytes" is not less than zero, then it is the length of the string zSql
 in bytes (not characters).

 *pzTail is made to point to the first byte past the end of the first
 SQL statement in zSql.  This routine only compiles the first statement
 in zSql, so *pzTail is left pointing to what remains uncompiled.

 *ppStmt is left pointing to a compiled SQL statement that can be
 executed using sqlite3_step().  Or if there is an error, *ppStmt may be
 set to NULL.  If the input text contained no SQL (if the input is and
 empty string or a comment) then *ppStmt is set to NULL.

 On success, SQLITE_OK is returned.  Otherwise an error code is returned.
}

api {} {
void sqlite3_progress_handler(sqlite3*, int, int(*)(void*), void*);
} {
 <i>Experimental</i>

 This routine configures a callback function - the progress callback - that
 is invoked periodically during long running calls to sqlite3_exec(),
 sqlite3_step() and sqlite3_get_table().
 An example use for this API is to keep
 a GUI updated during a large query.

 The progress callback is invoked once for every N virtual machine opcodes,
 where N is the second argument to this function. The progress callback
 itself is identified by the third argument to this function. The fourth
 argument to this function is a void pointer passed to the progress callback
 function each time it is invoked.

 If a call to sqlite3_exec(), sqlite3_step() or sqlite3_get_table() results 
 in less than N opcodes being executed, then the progress callback is not
 invoked.
 
 To remove the progress callback altogether, pass NULL as the third
 argument to this function.

 If the progress callback returns a result other than 0, then the current 
 query is immediately terminated and any database changes rolled back. If the
 query was part of a larger transaction, then the transaction is not rolled
 back and remains active. The sqlite3_exec() call returns SQLITE_ABORT. 

}

api {} {
int sqlite3_reset(sqlite3_stmt *pStmt);
} {
 The sqlite3_reset() function is called to reset a prepared SQL
 statement obtained by a previous call to sqlite3_prepare() or
 sqlite3_prepare16() back to it's initial state, ready to be re-executed.
 Any SQL statement variables that had values bound to them using
 the sqlite3_bind_*() API retain their values.
}

api {} {
void sqlite3_result_blob(sqlite3_context*, const void*, int n, void(*)(void*));
void sqlite3_result_double(sqlite3_context*, double);
void sqlite3_result_error(sqlite3_context*, const char*, int);
void sqlite3_result_error16(sqlite3_context*, const void*, int);
void sqlite3_result_int(sqlite3_context*, int);
void sqlite3_result_int64(sqlite3_context*, long long int);
void sqlite3_result_null(sqlite3_context*);
void sqlite3_result_text(sqlite3_context*, const char*, int n, void(*)(void*));
void sqlite3_result_text16(sqlite3_context*, const void*, int n, void(*)(void*));
void sqlite3_result_text16be(sqlite3_context*, const void*, int n, void(*)(void*));
void sqlite3_result_text16le(sqlite3_context*, const void*, int n, void(*)(void*));
void sqlite3_result_value(sqlite3_context*, sqlite3_value*);
} {
 User-defined functions invoke the following routines in order to
 set their return value.  The sqlite3_result_value() routine is used
 to return an exact copy of one of the parameters to the function.

 The operation of these routines is very similar to the operation of
 sqlite3_bind_blob() and its cousins.  Refer to the documentation there
 for additional information.
}

api {} {
int sqlite3_set_authorizer(
  sqlite3*,
  int (*xAuth)(void*,int,const char*,const char*,const char*,const char*),
  void *pUserData
);
#define SQLITE_CREATE_INDEX          1   /* Index Name      Table Name      */
#define SQLITE_CREATE_TABLE          2   /* Table Name      NULL            */
#define SQLITE_CREATE_TEMP_INDEX     3   /* Index Name      Table Name      */
#define SQLITE_CREATE_TEMP_TABLE     4   /* Table Name      NULL            */
#define SQLITE_CREATE_TEMP_TRIGGER   5   /* Trigger Name    Table Name      */
#define SQLITE_CREATE_TEMP_VIEW      6   /* View Name       NULL            */
#define SQLITE_CREATE_TRIGGER        7   /* Trigger Name    Table Name      */
#define SQLITE_CREATE_VIEW           8   /* View Name       NULL            */
#define SQLITE_DELETE                9   /* Table Name      NULL            */
#define SQLITE_DROP_INDEX           10   /* Index Name      Table Name      */
#define SQLITE_DROP_TABLE           11   /* Table Name      NULL            */
#define SQLITE_DROP_TEMP_INDEX      12   /* Index Name      Table Name      */
#define SQLITE_DROP_TEMP_TABLE      13   /* Table Name      NULL            */
#define SQLITE_DROP_TEMP_TRIGGER    14   /* Trigger Name    Table Name      */
#define SQLITE_DROP_TEMP_VIEW       15   /* View Name       NULL            */
#define SQLITE_DROP_TRIGGER         16   /* Trigger Name    Table Name      */
#define SQLITE_DROP_VIEW            17   /* View Name       NULL            */
#define SQLITE_INSERT               18   /* Table Name      NULL            */
#define SQLITE_PRAGMA               19   /* Pragma Name     1st arg or NULL */
#define SQLITE_READ                 20   /* Table Name      Column Name     */
#define SQLITE_SELECT               21   /* NULL            NULL            */
#define SQLITE_TRANSACTION          22   /* NULL            NULL            */
#define SQLITE_UPDATE               23   /* Table Name      Column Name     */
#define SQLITE_ATTACH               24   /* Filename        NULL            */
#define SQLITE_DETACH               25   /* Database Name   NULL            */

#define SQLITE_DENY   1   /* Abort the SQL statement with an error */
#define SQLITE_IGNORE 2   /* Don't allow access, but don't generate an error */
} {
 This routine registers a callback with the SQLite library.  The
 callback is invoked (at compile-time, not at run-time) for each
 attempt to access a column of a table in the database.  The callback should
 return SQLITE_OK if access is allowed, SQLITE_DENY if the entire
 SQL statement should be aborted with an error and SQLITE_IGNORE
 if the column should be treated as a NULL value.

 The second parameter to the access authorization function above will
 be one of the values below.  These values signify what kind of operation
 is to be authorized.  The 3rd and 4th parameters to the authorization
 function will be parameters or NULL depending on which of the following
 codes is used as the second parameter.  The 5th parameter is the name
 of the database ("main", "temp", etc.) if applicable.  The 6th parameter
 is the name of the inner-most trigger or view that is responsible for
 the access attempt or NULL if this access attempt is directly from 
 input SQL code.

 The return value of the authorization function should be one of the
 constants SQLITE_OK, SQLITE_DENY, or SQLITE_IGNORE.

 The intent of this routine is to allow applications to safely execute
 user-entered SQL.  An appropriate callback can deny the user-entered
 SQL access certain operations (ex: anything that changes the database)
 or to deny access to certain tables or columns within the database.
}

api {} {
int sqlite3_step(sqlite3_stmt*);
} {
 After an SQL query has been prepared with a call to either
 sqlite3_prepare() or sqlite3_prepare16(), then this function must be
 called one or more times to execute the statement.

 The return value will be either SQLITE_BUSY, SQLITE_DONE, 
 SQLITE_ROW, SQLITE_ERROR, or SQLITE_MISUSE.

 SQLITE_BUSY means that the database engine attempted to open
 a locked database and there is no busy callback registered.
 Call sqlite3_step() again to retry the open.

 SQLITE_DONE means that the statement has finished executing
 successfully.  sqlite3_step() should not be called again on this virtual
 machine.

 If the SQL statement being executed returns any data, then 
 SQLITE_ROW is returned each time a new row of data is ready
 for processing by the caller. The values may be accessed using
 the sqlite3_column_*() functions described below. sqlite3_step()
 is called again to retrieve the next row of data.
 
 SQLITE_ERROR means that a run-time error (such as a constraint
 violation) has occurred.  sqlite3_step() should not be called again on
 the VM. More information may be found by calling sqlite3_errmsg().

 SQLITE_MISUSE means that the this routine was called inappropriately.
 Perhaps it was called on a virtual machine that had already been
 finalized or on one that had previously returned SQLITE_ERROR or
 SQLITE_DONE.  Or it could be the case the the same database connection
 is being used simultaneously by two or more threads.
}

api {} {
void *sqlite3_trace(sqlite3*, void(*xTrace)(void*,const char*), void*);
} {
 Register a function that is called each time an SQL statement is evaluated.
 The callback function is invoked on the first call to sqlite3_step() after
 calls to sqlite3_prepare() or sqlite3_reset().
 This function can be used (for example) to generate
 a log file of all SQL executed against a database.  This can be
 useful when debugging an application that uses SQLite.
}

api {} {
void *sqlite3_user_data(sqlite3_context*);
} {
 The pUserData parameter to the sqlite3_create_function() and
 sqlite3_create_function16() routines used to register user functions
 is available to the implementation of the function using this
 call.
}

api {} {
const void *sqlite3_value_blob(sqlite3_value*);
int sqlite3_value_bytes(sqlite3_value*);
int sqlite3_value_bytes16(sqlite3_value*);
double sqlite3_value_double(sqlite3_value*);
int sqlite3_value_int(sqlite3_value*);
long long int sqlite3_value_int64(sqlite3_value*);
const unsigned char *sqlite3_value_text(sqlite3_value*);
const void *sqlite3_value_text16(sqlite3_value*);
const void *sqlite3_value_text16be(sqlite3_value*);
const void *sqlite3_value_text16le(sqlite3_value*);
int sqlite3_value_type(sqlite3_value*);
} {
 This group of routines returns information about parameters to
 a user-defined function.  Function implementations use these routines
 to access their parameters.  These routines are the same as the
 sqlite3_column_... routines except that these routines take a single
 sqlite3_value* pointer instead of an sqlite3_stmt* and an integer
 column number.

 See the documentation under sqlite3_column_blob for additional
 information.
}

set n 0
set i 0
foreach item $apilist {
  set namelist [lindex $item 0]
  foreach name $namelist {
    set n_to_name($n) $name
    set n_to_idx($n) $i
    set name_to_idx($name) $i
    incr n
  }
  incr i
}
set i 0
foreach name [lsort [array names name_to_idx]] {
  set sname($i) $name
  incr i
}
puts {<table width="100%" cellpadding="5"><tr>}
set nrow [expr {($n+2)/3}]
set i 0
for {set j 0} {$j<3} {incr j} {
  if {$j>0} {puts {<td width="10"></td>}}
  puts {<td valign="top">}
  set limit [expr {$i+$nrow}]
  puts {<ul>}
  while {$i<$limit && $i<$n} {
    set name $sname($i)
    if {[regexp {^sqlite} $name]} {set display $name} {set display <i>$name</i>}
    puts "<li><a href=\"#$name\">$display</a></li>"
    incr i
  }
  puts {</ul></td>}
}
puts "</table>"
puts "<!-- $n entries.  $nrow rows in 3 columns -->"

proc resolve_name {ignore_list name} {
  global name_to_idx
  if {![info exists name_to_idx($name)] || [lsearch $ignore_list $name]>=0} {
    return $name
  } else {
    return "<a href=\"#$name\">$name</a>"
  }
}

foreach name [lsort [array names name_to_idx]] {
  set i $name_to_idx($name)
  if {[info exists done($i)]} continue
  set done($i) 1
  foreach {namelist prototype desc} [lindex $apilist $i] break
  foreach name $namelist {
    puts "<a name=\"$name\">"
  }
  puts "<p><hr></p>"
  puts "<blockquote><pre>"
  regsub "^( *\n)+" $prototype {} p2
  regsub "(\n *)+\$" $p2 {} p3
  puts $p3
  puts "</pre></blockquote>"
  regsub -all {\[} $desc {\[} desc
  regsub -all {sqlite3_[a-z0-9_]+} $desc "\[resolve_name $name &\]" d2
  regsub -all "\n( *\n)+" [subst $d2] "</p>\n\n<p>" d3
  puts "<p>$d3</p>"
}

footer $rcsid

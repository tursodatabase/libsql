set rcsid {$Id: capi3ref.tcl,v 1.1 2004/06/01 01:22:38 drh Exp $}
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
#define SQLITE_CONSTRAINT  19   /* Abort due to contraint violation */
#define SQLITE_MISMATCH    20   /* Data type mismatch */
#define SQLITE_MISUSE      21   /* Library used incorrectly */
#define SQLITE_NOLFS       22   /* Uses OS features not supported on host */
#define SQLITE_AUTH        23   /* Authorization denied */
#define SQLITE_ROW         100  /* sqlite_step() has another row ready */
#define SQLITE_DONE        101  /* sqlite_step() has finished executing */
} {
The sqlite3.h header file defines macros for the integer result codes
returned by many API functions.
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
  int sqlite3_bind_blob(sqlite3_stmt*, int, const void*, int n, int eCopy);
  int sqlite3_bind_double(sqlite3_stmt*, int, double);
  int sqlite3_bind_int(sqlite3_stmt*, int, int);
  int sqlite3_bind_int64(sqlite3_stmt*, int, long long int);
  int sqlite3_bind_null(sqlite3_stmt*, int);
  int sqlite3_bind_text(sqlite3_stmt*, int, const char*, int n, int eCopy);
  int sqlite3_bind_text16(sqlite3_stmt*, int, const void*, int n, int eCopy);
} {
 In the SQL strings input to sqlite3_prepare() and sqlite3_prepare16(),
 one or more literals can be replace by a wildcard "?" or ":N:" where
 N is an integer.  The value of these wildcard literals can be set
 using these routines.

 The first parameter is a pointer to the sqlite3_stmt
 structure returned from sqlite3_prepare().  The second parameter is the
 index of the wildcard.  The first "?" has an index of 1.  ":N:" wildcards
 use the index N.

 When the eCopy parameter is true, a copy of the value is made into
 memory obtained and managed by SQLite.  When eCopy is false, SQLite
 assumes that the value is a constant and just stores a pointer to the
 value without making a copy.

 The sqlite3_bind_*() routine must be called after
 sqlite3_prepare() or sqlite3_reset() and before sqlite3_step().
 Bindings are not reset by the sqlite3_reset() routine.
 Unbound wildcards are interpreted as NULL.
}

api {} {
  void sqlite3_busy_handler(sqlite*, int(*)(void*,const char*,int), void*);
} {
 This routine identifies a callback function that is invoked
 whenever an attempt is made to open a database table that is
 currently locked by another process or thread.  If the busy callback
 is NULL, then sqlite3_exec() returns SQLITE_BUSY immediately if
 it finds a locked table.  If the busy callback is not NULL, then
 sqlite3_exec() invokes the callback with three arguments.  The
 second argument is the name of the locked table and the third
 argument is the number of times the table has been busy.  If the
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
  void sqlite3_busy_timeout(sqlite*, int ms);
} {
 This routine sets a busy handler that sleeps for a while when a
 table is locked.  The handler will sleep multiple times until 
 at least "ms" milleseconds of sleeping have been done.  After
 "ms" milleseconds of sleeping, the handler returns 0 which
 causes sqlite3_exec() to return SQLITE_BUSY.

 Calling this routine with an argument less than or equal to zero
 turns off all busy handlers.
}

api {} {
int sqlite3_changes(sqlite*);
} {
 This function returns the number of database rows that were changed
 (or inserted or deleted) by the most recent called sqlite3_exec().

 All changes are counted, even if they were later undone by a
 ROLLBACK or ABORT.  Except, changes associated with creating and
 dropping tables are not counted.

 If a callback invokes sqlite3_exec() recursively, then the changes
 in the inner, recursive call are counted together with the changes
 in the outer call.

 SQLite implements the command "DELETE FROM table" without a WHERE clause
 by dropping and recreating the table.  (This is much faster than going
 through and deleting individual elements form the table.)  Because of
 this optimization, the change count for "DELETE FROM table" will be
 zero regardless of the number of elements that were originally in the
 table. To get an accurate count of the number of rows deleted, use
 "DELETE FROM table WHERE 1" instead.
}

api {} {
  void sqlite3_close(sqlite *);
} {
 Call this function with a pointer to a structure that was previously
 returned from sqlite3_open() or sqlite3_open16()
 and the corresponding database will by closed.
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
 the colulmn index is out of range, the result is undefined.

 These routines attempt to convert the value where appropriate.  For
 example, if the internal representation is FLOAT and a text result
 is requested, sprintf() is used internally to do the conversion
 automatically.  The following table details the conversions that
 are applied:

 <table broder=1>
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
<tr><td> BLOB </td><td>    TEXT </td><td>  Add a \000 terminator if needed</td></tr>
</table>
}

api {} {
int sqlite3_column_count(sqlite3_stmt *pStmt);
} {
 Return the number of columns in the result set returned by the compiled
 SQL statement. This routine returns 0 if pStmt is an SQL statement
 that does not return data (for example an UPDATE).
}

api {} {
const char *sqlite3_column_decltype(sqlite3_stmt *, int i);
const void *sqlite3_column_decltype16(sqlite3_stmt*,int);
} {
 The first parameter is a compiled SQL statement. If this statement
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
 The first parameter is a compiled SQL statement. This function returns
 the column heading for the Nth column of that statement, where N is the
 second function parameter.  The string returned is UTF-8 for
 sqlite3_column_name() and UTF-16 for sqlite3_column_name16().
}

api {} {
void *sqlite3_commit_hook(sqlite*, int(*)(void*), void*);
} {
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
int sqlite3_create_function(
  sqlite3 *,
  const char *zFunctionName,
  int nArg,
  int eTextRep,
  int iCollateArg,
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
  int iCollateArg,
  void*,
  void (*xFunc)(sqlite3_context*,int,sqlite3_value**),
  void (*xStep)(sqlite3_context*,int,sqlite3_value**),
  void (*xFinal)(sqlite3_context*)
);
#define SQLITE3_UTF8     1
#define SQLITE3_UTF16LE  2
#define SQLITE3_UTF16BE  3
#define SQLITE3_ANY      4
} {
 These two functions are used to add user functions or aggregates
 implemented in C to the SQL langauge interpreted by SQLite. The
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

 The seventh, eighth and ninth parameters, xFunc, xStep and xFinal, are
 pointers to user implemented C functions that implement the user
 function or aggregate. A scalar function requires an implementation of
 the xFunc callback only, NULL pointers should be passed as the xStep
 and xFinal parameters. An aggregate function requires an implementation
 of xStep and xFinal, but NULL should be passed for xFunc. To delete an
 existing user function or aggregate, pass NULL for all three function
 callback. Specifying an inconstent set of callback values, such as an
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
 compiled SQL statement, this routine returns zero.
}

api {} {
int sqlite3_errcode(sqlite3 *db);
} {
 Return the error code for the most recent sqlite3_* API call associated
 with sqlite3 handle 'db'. SQLITE_OK is returned if the most recent 
 API call was successful.

 Calls to many sqlite3_* functions set the error code and string returned
 by sqlite3_errcode(), sqlite3_errmsg() and sqlite3_errmsg16()
 (overwriting the previous values). Note that calls to sqlite3_errcode(),
 sqlite3_errmsg() and sqlite3_errmsg16() themselves do not affect the
 results of future invocations.

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
  sqlite*,                      /* An open database */
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
 The sqlite3_finalize() function is called to delete a compiled
 SQL statement obtained by a previous call to sqlite3_prepare()
 or sqlite3_prepare16(). If the statement was executed successfully, or
 not executed at all, then SQLITE_OK is returned. If execution of the
 statement failed then an error code is returned. 

 This routine can be called at any point during the execution of the
 virtual machine.  If the virtual machine has not completed execution
 when this routine is called, that is like encountering an error or
 an interrupt.  (See sqlite3_interrupt().)  Incomplete updates may be
 rolled back and transactions cancelled,  depending on the circumstances,
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
  sqlite*,               /* An open database */
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
 void sqlite3_interrupt(sqlite*);
} {
 This function causes any pending database operation to abort and
 return at its earliest opportunity.  This routine is typically
 called in response to a user action such as pressing "Cancel"
 or Ctrl-C where the user wants a long query operation to halt
 immediately.
} {}

api {} {
int sqlite3_last_statement_changes(sqlite*);
} {
Experimental
}

api {} {
long long int sqlite3_last_insert_rowid(sqlite*);
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
 obtained from malloc() so that there is never a possiblity of buffer
 overflow.  These routines also implement some additional formatting
 options that are useful for constructing SQL statements.

 The strings returned by these routines should be freed by calling
 sqlite3_free().

 All of the usual printf formatting options apply.  In addition, there
 is a "%q" option.  %q works like %s in that it substitutes a null-terminated
 string from the argument list.  But %q also doubles every '\'' character.
 %q is designed for use inside a string literal.  By doubling each '\''
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

 Because the %q format string is used, the '\'' character in zText
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
  sqlite3 **ppDb,         /* OUT: SQLite db handle */
  const char **args       /* Null terminated array of option strings */
);
int sqlite3_open16(
  const void *filename,   /* Database filename (UTF-16) */
  sqlite3 **ppDb,         /* OUT: SQLite db handle */
  const char **args       /* Null terminated array of option strings */
);
} {
 Open the sqlite database file "filename".  The "filename" is UTF-8
 encoded for sqlite3_open() and UTF-16 encoded in the native byte order
 for sqlite3_open16().  An sqlite3* handle is returned in *ppDb, even
 if an error occurs. If the database is opened (or created) successfully,
 then SQLITE_OK is returned. Otherwise an error code is returned. The
 sqlite3_errmsg() or sqlite3_errmsg16()  routines can be used to obtain
 an English language description of the error.

 If the database file does not exist, then a new database is created.
 The encoding for the database is UTF-8 if sqlite3_open() is called and
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
void sqlite3_progress_handler(sqlite*, int, int(*)(void*), void*);
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
 The sqlite3_reset() function is called to reset a compiled SQL
 statement obtained by a previous call to sqlite3_prepare() or
 sqlite3_prepare16() back to it's initial state, ready to be re-executed.
 Any SQL statement variables that had values bound to them using
 the sqlite3_bind_*() API retain their values.
}

api {} {
void sqlite3_result_blob(sqlite3_context*, const void*, int n, int eCopy);
void sqlite3_result_double(sqlite3_context*, double);
void sqlite3_result_error(sqlite3_context*, const char*, int);
void sqlite3_result_error16(sqlite3_context*, const void*, int);
void sqlite3_result_int(sqlite3_context*, int);
void sqlite3_result_int64(sqlite3_context*, long long int);
void sqlite3_result_null(sqlite3_context*);
void sqlite3_result_text(sqlite3_context*, const char*, int n, int eCopy);
void sqlite3_result_text16(sqlite3_context*, const void*, int n, int eCopy);
void sqlite3_result_value(sqlite3_context*, sqlite3_value*);
} {
 User-defined functions invoke the following routines in order to
 set their return value.  The sqlite3_result_value() routine is used
 to return an exact copy of one of the parameters to the function.
}

api {} {
int sqlite3_set_authorizer(
  sqlite*,
  int (*xAuth)(void*,int,const char*,const char*,const char*,const char*),
  void *pUserData
);
#define SQLITE_COPY                  0   /* Table Name      File Name       */
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
 attempt to access a column of a table in the database.  The callback
 returns SQLITE_OK if access is allowed, SQLITE_DENY if the entire
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
 constants SQLITE_DENY or SQLITE_IGNORE.
}

api {} {
int sqlite3_step(sqlite3_stmt*);
} {
 After an SQL query has been compiled with a call to either
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
 is being used simulataneously by two or more threads.
}

api {} {
void *sqlite3_trace(sqlite*, void(*xTrace)(void*,const char*), void*);
} {
 Register a function that is called at every invocation of sqlite3_exec()
 or sqlite3_prepare().  This function can be used (for example) to generate
 a log file of all SQL executed against a database.  This is frequently
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
int sqlite3_value_type(sqlite3_value*);
} {
 This group of routines returns information about parameters to
 a user-defined function.  Function implementations use these routines
 to access their parameters.  These routines are the same as the
 sqlite3_column_* routines except that these routines take a single
 sqlite3_value* pointer instead of an sqlite3_stmt* and an integer
 column number.
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
    puts "<li><a href=\"#$name\">$name</a></li>"
    incr i
  }
  puts {</ul></td>}
}
puts "</table>"

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

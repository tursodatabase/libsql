set rcsid {$Id: capi3.tcl,v 1.6 2004/06/30 13:28:33 danielk1977 Exp $}
source common.tcl
header {C/C++ Interface For SQLite Version 3}
puts {
<h2>C/C++ Interface For SQLite Version 3</h2>

<h3>1.0 Overview</h3>

<p>
SQLite version 3.0 is a new version of SQLite, derived from
the SQLite 2.8.13 code base, but with an incompatible file format
and API.
SQLite version 3.0 was created to answer demand for the following features:
</p>

<ul>
<li>Support for UTF-16.</li>
<li>User-definable text collating sequences.</li>
<li>The ability to store BLOBs in indexed columns.</li>
</ul>

<p>
It was necessary to move to version 3.0 to implement these features because
each requires incompatible changes to the database file format.  Other
incompatible changes, such as a cleanup of the API, were introduced at the
same time under the theory that it is best to get your incompatible changes
out of the way all at once.  
</p>

<p>
The API for version 3.0 is similar to the version 2.X API,
but with some important changes.  Most noticeably, the "<tt>sqlite_</tt>"
prefix that occurs on the beginning of all API functions and data
structures are changed to "<tt>sqlite3_</tt>".  
This avoids confusion between the two APIs and allows linking against both
SQLite 2.X and SQLite 3.0 at the same time.
</p>

<p>
There is no agreement on what the C datatype for a UTF-16
string should be.  Therefore, SQLite uses a generic type of void*
to refer to UTF-16 strings.  Client software can cast the void* 
to whatever datatype is appropriate for their system.
</p>

<h3>2.0 C/C++ Interface</h3>

<p>
The API for SQLite 3.0 includes 83 separate functions in addition
to several data structures and #defines.  (A complete
<a href="capi3ref.html">API reference</a> is provided as a separate document.)
Fortunately, the interface is not nearly as complex as its size implies.
Simple programs can still make do with only 3 functions:
<a href="capi3ref.html#sqlite3_open">sqlite3_open()</a>,
<a href="capi3ref.html#sqlite3_exec">sqlite3_exec()</a>, and
<a href="capi3ref.html#sqlite3_close">sqlite3_close()</a>.
More control over the execution of the database engine is provided
using
<a href="capi3ref.html#sqlite3_prepare">sqlite3_prepare()</a>
to compile an SQLite statement into byte code and
<a href="capi3ref.html#sqlite3_prepare">sqlite3_step()</a>
to execute that bytecode.
A family of routines with names beginning with 
<a href="capi3ref.html#sqlite3_column_blob">sqlite3_column_</a>
is used to extract information about the result set of a query.
Many interface functions come in pairs, with both a UTF-8 and
UTF-16 version.  And there is a collection of routines
used to implement user-defined SQL functions and user-defined
text collating sequences.
</p>


<h4>2.1 Opening and closing a database</h4>

<blockquote><pre>
   typedef struct sqlite3 sqlite3;
   int sqlite3_open(const char*, sqlite3**);
   int sqlite3_open16(const void*, sqlite3**);
   int sqlite3_close(sqlite3*);
   const char *sqlite3_errmsg(sqlite3*);
   const void *sqlite3_errmsg16(sqlite3*);
   int sqlite3_errcode(sqlite3*);
</pre></blockquote>

<p>
The sqlite3_open() routine returns an integer error code rather than
a pointer to the sqlite3 structure as the version 2 interface did.
The difference between sqlite3_open()
and sqlite3_open16() is that sqlite3_open16() takes UTF-16 (in host native
byte order) for the name of the database file.  If a new database file
needs to be created, then sqlite3_open16() sets the internal text
representation to UTF-16 whereas sqlite3_open() sets the text
representation to UTF-8.
</p>

<p>
The opening and/or creating of the database file is deferred until the
file is actually needed.  This allows options and parameters, such
as the native text representation and default page size, to be
set using PRAGMA statements.
</p>

<p>
The sqlite3_errcode() routine returns a result code for the most
recent major API call.  sqlite3_errmsg() returns an English-language
text error message for the most recent error.  The error message is
represented in UTF-8 and will be ephemeral - it could disappear on
the next call to any SQLite API function.  sqlite3_errmsg16() works like
sqlite3_errmsg() except that it returns the error message represented
as UTF-16 in host native byte order.
</p>

<p>
The error codes for SQLite version 3 are unchanged from version 2.
They are as follows:
</p>

<blockquote><pre>
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
</pre></blockquote>

<h4>2.2 Executing SQL statements</h4>

<blockquote><pre>
   typedef int (*sqlite_callback)(void*,int,char**, char**);
   int sqlite3_exec(sqlite3*, const char *sql, sqlite_callback, void*, char**);
</pre></blockquote>

<p>
The sqlite3_exec function works much as it did in SQLite version 2.
Zero or more SQL statements specified in the second parameter are compiled
and executed.  Query results are returned to a callback routine.
See the <a href="capi3ref.html#sqlite3_exec">API reference</a> for additional
information.
</p>

<p>
In SQLite version 3, the sqlite3_exec routine is just a wrapper around
calls to the prepared statement interface.
</p>

<blockquote><pre>
   typedef struct sqlite3_stmt sqlite3_stmt;
   int sqlite3_prepare(sqlite3*, const char*, int, sqlite3_stmt**, const char**);
   int sqlite3_prepare16(sqlite3*, const void*, int, sqlite3_stmt**, const void**);
   int sqlite3_finalize(sqlite3_stmt*);
   int sqlite3_reset(sqlite3_stmt*);
</pre></blockquote>

<p>
The sqlite3_prepare interface compiles a single SQL statement into byte code
for later execution.  This interface is now the preferred way of accessing
the database.
</p>

<p>
The SQL statement is a UTF-8 string for sqlite3_prepare().
The sqlite3_prepare16() works the same way except
that it expects a UTF-16 string as SQL input.
Only the first SQL statement in the input string is compiled.
The fourth parameter is filled in with a pointer to the next (uncompiled)
SQLite statement in the input string, if any.
The sqlite3_finalize() routine deallocates a prepared SQL statement.
All prepared statements must be finalized before the database can be
closed.
The sqlite3_reset() routine resets a prepared SQL statement so that it
can be executed again.
</p>

<p>
The SQL statement may contain tokens of the form "?" or "?nnn" or ":nnn:"
where "nnn" is an integer.  Such tokens represent unspecified literal values
(or wildcards) to be filled in later by the 
<a href="capi3ref.html#sqlite3_bind_blob">sqlite3_bind</a> interface.
Each wildcard as an associated number given
by the "nnn" that follows the "?".  If the "?" is not followed by an
integer, then its number one more than the number of prior wildcards
in the same SQL statement.  It is allowed for the same wildcard
to occur more than once in the same SQL statement, in which case
all instance of that wildcard will be filled in with the same value.
Unbound wildcards have a value of NULL.
</p>

<blockquote><pre>
   int sqlite3_bind_blob(sqlite3_stmt*, int, const void*, int n, void(*)(void*));
   int sqlite3_bind_double(sqlite3_stmt*, int, double);
   int sqlite3_bind_int(sqlite3_stmt*, int, int);
   int sqlite3_bind_int64(sqlite3_stmt*, int, long long int);
   int sqlite3_bind_null(sqlite3_stmt*, int);
   int sqlite3_bind_text(sqlite3_stmt*, int, const char*, int n, void(*)(void*));
   int sqlite3_bind_text16(sqlite3_stmt*, int, const void*, int n, void(*)(void*));
   int sqlite3_bind_value(sqlite3_stmt*, int, const sqlite3_value*);
</pre></blockquote>

<p>
There is an assortment of sqlite3_bind routines used to assign values
to wildcards in a prepared SQL statement.  Unbound wildcards
are interpreted as NULLs.  Bindings are not reset by sqlite3_reset().
But wildcards can be rebound to new values after an sqlite3_reset().
</p>

<p>
After an SQL statement has been prepared (and optionally bound), it
is executed using:
</p>

<blockquote><pre>
   int sqlite3_step(sqlite3_stmt*);
</pre></blockquote>

<p>
The sqlite3_step() routine return SQLITE3_ROW if it is returning a single
row of the result set, or SQLITE3_DONE if execution has completed, either
normally or due to an error.  It might also return SQLITE3_BUSY if it is
unable to open the database file.  If the return value is SQLITE3_ROW, then
the following routines can be used to extract information about that row
of the result set:
</p>

<blockquote><pre>
   const void *sqlite3_column_blob(sqlite3_stmt*, int iCol);
   int sqlite3_column_bytes(sqlite3_stmt*, int iCol);
   int sqlite3_column_bytes16(sqlite3_stmt*, int iCol);
   int sqlite3_column_count(sqlite3_stmt*);
   const char *sqlite3_column_decltype(sqlite3_stmt *, int iCol);
   const void *sqlite3_column_decltype16(sqlite3_stmt *, int iCol);
   double sqlite3_column_double(sqlite3_stmt*, int iCol);
   int sqlite3_column_int(sqlite3_stmt*, int iCol);
   long long int sqlite3_column_int64(sqlite3_stmt*, int iCol);
   const char *sqlite3_column_name(sqlite3_stmt*, int iCol);
   const void *sqlite3_column_name16(sqlite3_stmt*, int iCol);
   const unsigned char *sqlite3_column_text(sqlite3_stmt*, int iCol);
   const void *sqlite3_column_text16(sqlite3_stmt*, int iCol);
   int sqlite3_column_type(sqlite3_stmt*, int iCol);
</pre></blockquote>

<p>
The 
<a href="capi3ref.html#sqlite3_column_count">sqlite3_column_count()</a>
function returns the number of columns in
the results set.  sqlite3_column_count() can be called at any time after
sqlite3_prepare().  
<a href="capi3ref.html#sqlite3_data_count">sqlite3_data_count()</a>
works similarly to
sqlite3_column_count() except that it only works following sqlite3_step().
If the previous call to sqlite3_step() returned SQLITE_DONE or an error code,
then sqlite3_data_count() will return 0 whereas sqlite3_column_count() will
continue to return the number of columns in the result set.
</p>

<p>
The sqlite3_column_type() function returns the
datatype for the value in the Nth column.  The return value is one
of these:
</p>

<blockquote><pre>
   #define SQLITE_INTEGER  1
   #define SQLITE_FLOAT    2
   #define SQLITE_TEXT     3
   #define SQLITE_BLOB     4
   #define SQLITE_NULL     5
</pre></blockquote>

<p>
The sqlite3_column_decltype() routine returns text which is the
declared type of the column in the CREATE TABLE statement.  For an
expression, the return type is an empty string.  sqlite3_column_name()
returns the name of the Nth column.  sqlite3_column_bytes() returns
the number of bytes in a column that has type BLOB or the number of bytes
in a TEXT string with UTF-8 encoding.  sqlite3_column_bytes16() returns
the same value for BLOBs but for TEXT strings returns the number of bytes
in a UTF-16 encoding.
sqlite3_column_blob() return BLOB data.  
sqlite3_column_text() return TEXT data as UTF-8.
sqlite3_column_text16() return TEXT data as UTF-16.
sqlite3_column_int() return INTEGER data in the host machines native
integer format.
sqlite3_column_int64() returns 64-bit INTEGER data.
Finally, sqlite3_column_double() return floating point data.
</p>

<p>
It is not necessary to retrieve data in the format specify by
sqlite3_column_type().  If a different format is requested, the data
is converted automatically.
</p>

<h4>2.3 User-defined functions</h4>

<p>
User defined functions can be created using the following routine:
</p>

<blockquote><pre>
   typedef struct sqlite3_value sqlite3_value;
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
   #define SQLITE3_UTF8     1
   #define SQLITE3_UTF16    2
   #define SQLITE3_UTF16BE  3
   #define SQLITE3_UTF16LE  4
   #define SQLITE3_ANY      5
</pre></blockquote>

<p>
The nArg parameter specifies the number of arguments to the function.
A value of 0 indicates that any number of arguments is allowed.  The
eTextRep parameter specifies what representation text values are expected
to be in for arguments to this function.  The value of this parameter should
be one of the parameters defined above.  SQLite version 3 allows multiple
implementations of the same function using different text representations.
The database engine chooses the function that minimization the number
of text conversions required.
</p>

<p>
Normal functions specify only xFunc and leave xStep and xFinal set to NULL.
Aggregate functions specify xStep and xFinal and leave xFunc set to NULL.
There is no separate sqlite3_create_aggregate() API.
</p>

<p>
The function name is specified in UTF-8.  A separate sqlite3_create_function16()
API works the same as sqlite_create_function()
except that the function name is specified in UTF-16 host byte order.
</p>

<p>
Notice that the parameters to functions are now pointers to sqlite3_value
structures instead of pointers to strings as in SQLite version 2.X.
The following routines are used to extract useful information from these
"values":
</p>

<blockquote><pre>
   const void *sqlite3_value_blob(sqlite3_value*);
   int sqlite3_value_bytes(sqlite3_value*);
   int sqlite3_value_bytes16(sqlite3_value*);
   double sqlite3_value_double(sqlite3_value*);
   int sqlite3_value_int(sqlite3_value*);
   long long int sqlite3_value_int64(sqlite3_value*);
   const unsigned char *sqlite3_value_text(sqlite3_value*);
   const void *sqlite3_value_text16(sqlite3_value*);
   int sqlite3_value_type(sqlite3_value*);
</pre></blockquote>

<p>
Function implementations use the following APIs to acquire context and
to report results:
</p>

<blockquote><pre>
   void *sqlite3_aggregate_context(sqlite3_context*, int nbyte);
   void *sqlite3_user_data(sqlite3_context*);
   void sqlite3_result_blob(sqlite3_context*, const void*, int n, void(*)(void*));
   void sqlite3_result_double(sqlite3_context*, double);
   void sqlite3_result_error(sqlite3_context*, const char*, int);
   void sqlite3_result_error16(sqlite3_context*, const void*, int);
   void sqlite3_result_int(sqlite3_context*, int);
   void sqlite3_result_int64(sqlite3_context*, long long int);
   void sqlite3_result_null(sqlite3_context*);
   void sqlite3_result_text(sqlite3_context*, const char*, int n, void(*)(void*));
   void sqlite3_result_text16(sqlite3_context*, const void*, int n, void(*)(void*));
   void sqlite3_result_value(sqlite3_context*, sqlite3_value*);
   void *sqlite3_get_auxdata(sqlite3_context*, int);
   void sqlite3_set_auxdata(sqlite3_context*, int, void*, void (*)(void*));
</pre></blockquote>

<h4>2.4 User-defined collating sequences</h4>

<p>
The following routines are used to implement user-defined
collating sequences:
</p>

<blockquote><pre>
   sqlite3_create_collation(sqlite3*, const char *zName, int eTextRep, void*,
      int(*xCompare)(void*,int,const void*,int,const void*));
   sqlite3_create_collation16(sqlite3*, const void *zName, int eTextRep, void*,
      int(*xCompare)(void*,int,const void*,int,const void*));
   sqlite3_collation_needed(sqlite3*, void*, 
      void(*)(void*,sqlite3*,int eTextRep,const char*));
   sqlite3_collation_needed16(sqlite3*, void*,
      void(*)(void*,sqlite3*,int eTextRep,const void*));
</pre></blockquote>

<p>
The sqlite3_create_collation() function specifies a collating sequence name
and a comparison function to implement that collating sequence.  The
comparison function is only used for comparing text values.  The eTextRep
parameter is one of SQLITE3_UTF8, SQLITE3_UTF16LE, SQLITE3_UTF16BE, or
SQLITE3_ANY to specify which text representation the comparison function works
with.  Separate comparison functions can exist for the same collating
sequence for each of the UTF-8, UTF-16LE and UTF-16BE text representations.
The sqlite3_create_collation16() works like sqlite3_create_collation() except
that the collation name is specified in UTF-16 host byte order instead of
in UTF-8.
</p>

<p>
The sqlite3_collation_needed() routine registers a callback which the
database engine will invoke if it encounters an unknown collating sequence.
The callback can lookup an appropriate comparison function and invoke
sqlite_3_create_collation() as needed.  The fourth parameter to the callback
is the name of the collating sequence in UTF-8.  For sqlite3_collation_need16()
the callback sends the collating sequence name in UTF-16 host byte order.
</p>
}
footer $rcsid

#
# Run this Tcl script to generate the sqlite.html file.
#
set rcsid {$Id: c_interface.tcl,v 1.9 2000/10/08 22:20:58 drh Exp $}

puts {<html>
<head>
  <title>The C language interface to the SQLite library</title>
</head>
<body bgcolor=white>
<h1 align=center>
The C language interface to the SQLite library
</h1>}
puts "<p align=center>
(This page was last modified on [lrange $rcsid 3 4] GMT)
</p>"

puts {
<p>The SQLite library is designed to be very easy to use from
a C or C++ program.  This document gives an overview of the C/C++
programming interface.</p>

<h2>The API</h2>

<p>The interface to the SQLite library consists of three core functions,
one opaque data structure, and some constants used as return values.
The core interface is as follows:</p>

<blockquote><pre>
typedef struct sqlite sqlite;

sqlite *sqlite_open(const char *filename, int mode, char **errmsg);

void sqlite_close(sqlite*);

int sqlite_exec(
  sqlite*,
  char *sql,
  int (*)(void*,int,char**,char**),
  void*,
  char **errmsg
);

#define SQLITE_OK        0    /* Successful result */
#define SQLITE_INTERNAL  1    /* An internal logic error in SQLite */
#define SQLITE_ERROR     2    /* SQL error or missing database */
#define SQLITE_PERM      3    /* Access permission denied */
#define SQLITE_ABORT     4    /* Callback routine requested an abort */
#define SQLITE_BUSY      5    /* One or more database files are locked */
#define SQLITE_NOMEM     6    /* A malloc() failed */
#define SQLITE_READONLY  7    /* Attempt to write a readonly database */
</pre></blockquote>

<p>Only the three core routines shown above are required to use
SQLite.  But there are many other functions that provide 
useful interfaces.  These extended routines are as follows:
</p>

<blockquote><pre>
int sqlite_get_table(
  sqlite*,
  char *sql,
  char ***result,
  int *nrow,
  int *ncolumn,
  char **errmsg
);

void sqlite_free_table(char**);

int sqlite_complete(const char *sql);

void sqlite_busy_handler(sqlite*, int (*)(void*,const char*,int), void*);

void sqlite_busy_timeout(sqlite*, int ms);

const char sqlite_version[];

int sqlite_exec_printf(
  sqlite*,
  char *sql,
  int (*)(void*,int,char**,char**),
  void*,
  char **errmsg,
  ...
);

int sqlite_exec_vprintf(
  sqlite*,
  char *sql,
  int (*)(void*,int,char**,char**),
  void*,
  char **errmsg,
  va_list
);

int sqlite_get_table_printf(
  sqlite*,
  char *sql,
  char ***result,
  int *nrow,
  int *ncolumn,
  char **errmsg,
  ...
);

int sqlite_get_table_vprintf(
  sqlite*,
  char *sql,
  char ***result,
  int *nrow,
  int *ncolumn,
  char **errmsg,
  va_list
);

</pre></blockquote>

<p>All of the above definitions are included in the "sqlite.h"
header file that comes in the source tree.</p>

<h2>Opening a database</h2>

<p>Use the <b>sqlite_open()</b> function to open an existing SQLite
database or to create a new SQLite database.  The first argument
is the database name.  The second argument is a constant 0666 to
open the database for reading and writing and 0444 to open the
database read only.  The third argument is a pointer to a string
pointer.  If the third argument is not NULL and an error occurs
while trying to open the database, then an error message will be
written to memory obtained from malloc() and *errmsg will be made
to point to this error message.  The calling function is responsible
for freeing the memory when it has finished with it.</p>

<p>An SQLite database is just a directory containing a collection of
GDBM files.  There is one GDBM file for each table and index in the
database.  All GDBM files end with the ".tbl" suffix.  Every SQLite
database also contains a special database table named <b>sqlite_master</b>
stored in its own GDBM file.  This special table records the database
schema.</p>

<p>To create a new SQLite database, all you have to do is call
<b>sqlite_open()</b> with the first parameter set to the name of
an empty directory and the second parameter set to 0666.</p>

<p>The return value of the <b>sqlite_open()</b> function is a
pointer to an opaque <b>sqlite</b> structure.  This pointer will
be the first argument to all subsequent SQLite function calls that
deal with the same database.  NULL is returned if the open fails
for any reason.</p>

<h2>Closing the database</h2>

<p>To close an SQLite database, call the <b>sqlite_close()</b>
function passing it the sqlite structure pointer that was obtained
from a prior call to <b>sqlite_open</b>.

<h2>Executing SQL statements</h2>

<p>The <b>sqlite_exec()</b> function is used to process SQL statements
and queries.  This function requires 5 parameters as follows:</p>

<ol>
<li><p>A pointer to the sqlite structure obtained from a prior call
       to <b>sqlite_open()</b>.</p></li>
<li><p>A null-terminated string containing the text of one or more
       SQL statements and/or queries to be processed.</p></li>
<li><p>A pointer to a callback function which is invoked once for each
       row in the result of a query.  This argument may be NULL, in which
       case no callbacks will ever be invoked.</p></li>
<li><p>A pointer that is forwarded to become the first argument
       to the callback function.</p></li>
<li><p>A pointer to an error string.  Error messages are written to space
       obtained from malloc() and the error string is made to point to
       the malloced space.  The calling function is responsible for freeing
       this space when it has finished with it.
       This argument may be NULL, in which case error messages are not
       reported back to the calling function.</p></li>
</ol>

<p>
The callback function is used to receive the results of a query.  A
prototype for the callback function is as follows:</p>

<blockquote><pre>
int Callback(void *pArg, int argc, char **argv, char **columnNames){
  return 0;
}
</pre></blockquote>

<p>The first argument to the callback is just a copy of the fourth argument
to <b>sqlite_exec()</b>  This parameter can be used to pass arbitrary
information through to the callback function from client code.
The second argument is the number columns in the query result.
The third argument is an array of pointers to strings where each string
is a single column of the result for that record.  Note that the
callback function reports a NULL value in the database as a NULL pointer,
which is very different from an empty string.  If the i-th parameter
is an empty string, we will get:</p>
<blockquote><pre>
argv[i][0] == 0
</pre></blockquote>
<p>But if the i-th parameter is NULL we will get:</p>
<blockquote><pre>
argv[i] == 0
</pre></blockquote>
<p>The names of the columns are contained in the fourth argument.</p>

<p>The callback function should normally return 0.  If the callback
function returns non-zero, the query is immediately aborted and 
<b>sqlite_exec()</b> will return SQLITE_ABORT.</p>

<p>The <b>sqlite_exec()</b> function returns an integer to indicate
success or failure of the operation.  The following are possible
return values:</p>

<blockquote>
<dl>
<dt>SQLITE_OK</dt>
<dd><p>This value is returned if everything worked and there were no errors.
</p></dd>
<dt>SQLITE_INTERNAL</dt>
<dd><p>This value indicates that an internal consistency check within
the SQLite library failed.  This can only happen if there is a bug in
the SQLite library.  If you ever get an SQLITE_INTERNAL reply from
an <b>sqlite_exec()</b> call, please report the problem on the SQLite
mailing list.
</p></dd>
<dt>SQLITE_ERROR</dt>
<dd><p>This return value indicates that there was an error in the SQL
that was passed into the <b>sqlite_exec()</b>.
</p></dd>
<dt>SQLITE_PERM</dt>
<dd><p>This return value says that the access permissions on one of the
GDBM files is such that the file cannot be opened.
</p></dd>
<dt>SQLITE_ABORT</dt>
<dd><p>This value is returned if the callback function returns non-zero.
</p></dd>
<dt>SQLITE_BUSY</dt>
<dd><p>This return code indicates that one of the underlying GDBM files
is locked because it is currently being accessed by another thread or
process.  GDBM allows mutiple readers of the same file, but only one
writer.  So multiple processes can query an SQLite database at once.
But only a single process can write to an SQLite database at one time.
If an attempt is made to write to an SQLite database that another
process is currently reading, the write is not performed and 
<b>sqlite_exec()</b> returns SQLITE_BUSY.  Similarly, an attempt to read
an SQLite database that is currently being written by another process
will return SQLITE_BUSY.  In both cases, the write or query attempt
can be retried after the other process finishes.</p>
<p>Note that locking is done at the file level.  One process can
write to table ABC (for example) while another process simultaneously
reads from a different table XYZ.  But you cannot have two processes reading
and writing table ABC at the same time.
</p></dd>
<dt>SQLITE_NOMEM</dt>
<dd><p>This value is returned if a call to <b>malloc()</b> fails.
</p></dd>
<dt>SQLITE_READONLY</dt>
<dd><p>This return code indicates that an attempt was made to write to
a database file that was originally opened for reading only.  This can
happen if the callback from a query attempts to update the table
being queried.
</p></dd>
</dl>
</blockquote>

<h2>Querying without using a callback function</h2>

<p>The <b>sqlite_get_table()</b> function is a wrapper around
<b>sqlite_exec()</b> that collects all the information from successive
callbacks and write it into memory obtained from malloc().  This
is a convenience function that allows the application to get the
entire result of a database query with a single function call.</p>

<p>The main result from <b>sqlite_get_table()</b> is an array of pointers
to strings.  There is one element in this array for each column of
each row in the result.  NULL results are represented by a NULL
pointer. In addition to the regular data, there is an added row at the 
beginning of the array that contains the names of each column of the
result.</p>

<p>As an example, consider the following query:</p>

<blockquote>
SELECT employee_name, login, host FROM users WHERE logic LIKE 'd%';
</blockquote>

<p>This query will return the name, login and host computer name
for every employee whose login begins with the letter "d".  If this
query is submitted to <b>sqlite_get_table()</b> the result might
look like this:</p>

<blockquote>
nrow = 2<br>
ncolumn = 3<br>
result[0] = "employee_name"<br>
result[1] = "login"<br>
result[2] = "host"<br>
result[3] = "dummy"<br>
result[4] = "No such user"<br>
result[5] = 0<br>
result[6] = "D. Richard Hipp"<br>
result[7] = "drh"<br>
result[8] = "zadok"
</blockquote>

<p>Notice that the "host" value for the "dummy" record is NULL so
the result[] array contains a NULL pointer at that slot.</p>

<p>Memory to hold the information returned by <b>sqlite_get_table()</b>
is obtained from malloc().  But the calling function should not try
to free this information directly.  Instead, pass the complete table
to <b>sqlite_free_table()</b> when the table is no longer needed.</p>

<p>The <b>sqlite_get_table()</b> routine returns the same integer
result code as <b>sqlite_exec()</b>.</p>

<h2>Testing for a complete SQL statement</h2>

<p>The last interface routine to SQLite is a convenience function used
to test whether or not a string forms a complete SQL statement.
If the <b>sqlite_complete()</b> function returns true when its input
is a string, then the argument forms a complete SQL statement.
There are no guarantees that the syntax of that statement is correct,
but we at least know the statement is complete.  If <b>sqlite_complete()</b>
returns false, then more text is required to complete the SQL statement.</p>

<p>For the purpose of the <b>sqlite_complete()</b> function, an SQL
statement is complete if it ends in a semicolon.</p>

<p>The <b>sqlite</b> command-line utility uses the <b>sqlite_complete()</b>
function to know when it needs to call <b>sqlite_exec()</b>.  After each
line of input is received, <b>sqlite</b> calls <b>sqlite_complete()</b>
on all input in its buffer.  If <b>sqlite_complete()</b> returns true, 
then <b>sqlite_exec()</b> is called and the input buffer is reset.  If
<b>sqlite_complete()</b> returns false, then the prompt is changed to
the continuation prompt and another line of text is read and added to
the input buffer.</p>

<h2>Library version string</h2>

<p>The SQLite library exports the string constant named
<b>sqlite_version</b> which contains the version number of the
library.  The header file contains a macro SQLITE_VERSION
with the same information.  If desired, a program can compare
the SQLITE_VERSION macro against the <b>sqlite_version</b>
string constant to verify that the version number of the
header file and the library match.</p> 

<h2>Changing the libraries response to locked files</h2>

<p>The GDBM library supports database locks at the file level.
If a GDBM database file is opened for reading, then that same
file cannot be reopened for writing until all readers have closed
the file.  If a GDBM file is open for writing, then the file cannot
be reopened for reading or writing until it is closed.</p>

<p>If the SQLite library attempts to open a GDBM file and finds that
the file is locked, the default action is to abort the current
operation and return SQLITE_BUSY.  But this is not always the most
convenient behavior, so a mechanism exists to change it.</p>

<p>The <b>sqlite_busy_handler()</b> procedure can be used to register
a busy callback with an open SQLite database.  The busy callback will
be invoked whenever SQLite tries to open a GDBM file that is locked.
The callback will typically do some other useful work, or perhaps sleep,
in order to give the lock a chance to clear.  If the callback returns
non-zero, then SQLite tries again to open the GDBM file and the cycle
repeats.  If the callback returns zero, then SQLite aborts the current
operation and returns SQLITE_BUSY.</p>

<p>The arguments to <b>sqlite_busy_handler()</b> are the opaque
structure returned from <b>sqlite_open()</b>, a pointer to the busy
callback function, and a generic pointer that will be passed as
the first argument to the busy callback.  When SQLite invokes the
busy callback, it sends it three arguments:  the generic pointer
that was passed in as the third argument to <b>sqlite_busy_handler</b>,
the name of the database table or index that the library is trying
to open, and the number of times that the library has attempted to
open the database table or index.</p>

<p>For the common case where we want the busy callback to sleep,
the SQLite library provides a convenience routine <b>sqlite_busy_timeout()</b>.
The first argument to <b>sqlite_busy_timeout()</b> is a pointer to
an open SQLite database and the second argument is a number of milliseconds.
After <b>sqlite_busy_timeout()</b> has been executed, the SQLite library
will wait for the lock to clear for at least the number of milliseconds 
specified before it returns SQLITE_BUSY.  Specifying zero milliseconds for
the timeout restores the default behavior.</p>

<h2>Using the <tt>_printf()</tt> wrapper functions</h2>

<p>The four utility functions</p>

<p>
<ul>
<li><b>sqlite_exec_printf()</b></li>
<li><b>sqlite_exec_vprintf()</b></li>
<li><b>sqlite_get_table_printf()</b></li>
<li><b>sqlite_get_table_vprintf()</b></li>
</ul>
</p>

<p>implement the same query functionality as <b>sqlite_exec()</b>
and <b>sqlite_get_table()</b>.  But instead of taking a complete
SQL statement as their second argument, the four <b>_printf</b>
routines take a printf-style format string.  The SQL statement to
be executed is generated from this format string and from whatever
additional arguments are attached to the end of the function call.</p>

<p>There are two advantages to using the SQLite printf
functions instead of <b>sprintf()</b>.  First of all, with the
SQLite printf routines, there is never a danger of overflowing a
static buffer as there is with <b>sprintf()</b>.  The SQLite
printf routines automatically allocate (and later free)
as much memory as is 
necessary to hold the SQL statements generated.</p>

<p>The second advantage the SQLite printf routines have over
<b>sprintf()</b> is a new formatting option specifically designed
to support string literals in SQL.  Within the format string,
the %q formatting option works very much like %s in that it
reads a null-terminated string from the argument list and inserts
it into the result.  But %q translates the inserted string by
making two copies of every single-quote (') character in the
substituted string.  This has the effect of escaping the end-of-string
meaning of single-quote within a string literal.
</p>

<p>Consider an example.  Suppose you are trying to insert a string
values into a database table where the string value was obtained from
user input.  Suppose the string to be inserted is stored in a variable
named zString.  The code to insert this string might look like this:</p>

<blockquote><pre>
sqlite_exec_printf(db,
  "INSERT INTO table1 VALUES('%s')",
  0, 0, 0, zString);
</pre></blockquote>

<p>If the zString variable holds text like "Hello", then this statement
will work just fine.  But suppose the user enters a string like 
"Hi y'all!".  The SQL statement generated reads as follows:

<blockquote><pre>
INSERT INTO table1 VALUES('Hi y'all')
</pre></blockquote>

<p>This is not valid SQL because of the apostrophy in the word "y'all".
But if the %q formatting option is used instead of %s, like this:</p>

<blockquote><pre>
sqlite_exec_printf(db,
  "INSERT INTO table1 VALUES('%q')",
  0, 0, 0, zString);
</pre></blockquote>

<p>Then the generated SQL will look like the following:</p>

<blockquote><pre>
INSERT INTO table1 VALUES('Hi y''all')
</pre></blockquote>

<p>Here the apostrophy has been escaped and the SQL statement is well-formed.
When generating SQL on-the-fly from data that might contain a
single-quote character ('), it is always a good idea to use the
SQLite printf routines and the %q formatting option instead of <b>sprintf</b>.
</p>

<h2>Usage Examples</h2>

<p>For examples of how the SQLite C/C++ interface can be used,
refer to the source code for the <b>sqlite</b> program in the
file <b>src/shell.c</b> of the source tree.
Additional information about sqlite is available at
<a href="sqlite.html">sqlite.html</a>.
See also the sources to the Tcl interface for SQLite in
the source file <b>src/tclsqlite.c</b>.</p>
}

puts {
<p><hr /></p>
<p><a href="index.html"><img src="/goback.jpg" border=0 />
Back to the SQLite Home Page</a>
</p>

</body></html>}

#
# Run this Tcl script to generate the sqlite.html file.
#
set rcsid {$Id: c_interface.tcl,v 1.2 2000/05/29 18:32:17 drh Exp $}

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

<p>The interface to the SQLite library consists of 4 functions
and one opaque data structure.</p>

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

int sqlite_complete(const char *sql);
</pre></blockquote>

<p>All of the above definitions are included in the "sqlite.h"
header file that comes in the source tree.</p>

<h2>Opening a database</h2>

<p>Use the <b>sqlite_open</b> function to open an existing SQLite
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
deal with the same database.</p>

<h2>Closing the database</h2>

<p>To close an SQLite database, just call the <b>sqlite_close()</b>
function passing it the sqlite structure pointer that was obtained
from a prior call to <b>sqlite_open</b>.

<h2>Executing SQL statements</h2>

<p>The <b>sqlite_exec()</b> function is used to process SQL statements
and queries.  This function requires 5 parameters as follows:</p>

<ol>
<li><p>A pointer to the sqlite structure obtained from a prior call
       to <b>sqlite_open()</b>.</p></li>
<li><p>A null-terminated string containing the text of the SQL statements
       and/or queries to be processed.</p></li>
<li><p>A pointer to a callback function which is invoked once for each
       row in the result of a query.  This argument may be NULL, in which
       case no callbacks will ever be invoked.</p></li>
<li><p>A pointer to anything that is forward to become the first argument
       to the callback function.</p></li>
<li><p>A pointer to a string pointer into which error messages are written.
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
The third argument is an array of pointers to string where each string
is a single column of the result for that record.  The names of the
columns are contained in the fourth argument.</p>

<p>The callback function should normally return 0.  If the callback
function returns non-zero, the query is immediately aborted and the
return value of the callback is returned from <b>sqlite_exec()</b>.

<h2>Testing for a complete SQL statement</h2>

<p>The last interface routine to SQLite is a convenience function used
to test whether or not a string forms a complete SQL statement.
If the <b>sqlite_complete</b> function returns true when its input
is a string, then the argument forms a complete SQL statement.
There are no guarantees that the syntax of that statement is correct,
but we at least know the statement is complete.  If <b>sqlite_complete</b>
returns false, then more text is required to complete the SQL statement.</p>

<p>For the purpose of the <b>sqlite_complete()</b> function, an SQL
statement is complete if it ends in a semicolon.</p>

<h2>Usage Examples</h2>

<p>For examples of how the SQLite C/C++ interface can be used,
refer to the source code for the "sqlite" program in the
file <b>src/shell.c</b> of the source tree.
(Additional information about sqlite is available at
<a href="sqlite.html">sqlite.html</a>.)
See also the sources to the Tcl interface for SQLite in
the source file <b>src/tclsqlite.c</b>.</p>
}

puts {
<p><hr /></p>
<p><a href="index.html"><img src="/goback.jpg" border=0 />
Back to the SQLite Home Page</a>
</p>

</body></html>}

#
# Run this Tcl script to generate the audit.html file.
#
set rcsid {$Id: audit.tcl,v 1.1 2002/07/13 16:52:35 drh Exp $}

puts {<html>
<head>
  <title>SQLite Security Audit Procedure</title>
</head>
<body bgcolor=white>
<h1 align=center>
SQLite Security Audit Procedure
</h1>}
puts "<p align=center>
(This page was last modified on [lrange $rcsid 3 4] UTC)
</p>"

puts {
<p>
A security audit for SQLite consists of two components.  First, there is
a check for common errors that often lead to security problems.  Second,
an attempt is made to construct a proof that SQLite has certain desirable
security properties.
</p>

<h2>Part I: Things to check</h2>

<p>
Scan all source code and check for the following common errors:
</p>

<ol>
<li><p>
Verify that the destination buffer is large enough to hold its result
in every call to the following routines:
<ul>
<li> <b>strcpy()</b> </li>
<li> <b>strncpy()</b> </li>
<li> <b>strcat()</b> </li>
<li> <b>memcpy()</b> </li>
<li> <b>memset()</b> </li>
<li> <b>memmove()</b> </li>
<li> <b>bcopy()</b> </li>
<li> <b>sprintf()</b> </li>
<li> <b>scanf()</b> </li>
</ul>
</p></li>
<li><p>
Verify that pointers returned by subroutines are not NULL before using
the pointers.  In particular, make sure the return values for the following
routines are checked before they are used:
<ul>
<li> <b>malloc()</b> </li>
<li> <b>realloc()</b> </li>
<li> <b>sqliteMalloc()</b> </li>
<li> <b>sqliteRealloc()</b> </li>
<li> <b>sqliteStrDup()</b> </li>
<li> <b>sqliteStrNDup()</b> </li>
<li> <b>sqliteExpr()</b> </li>
<li> <b>sqliteExprFunction()</b> </li>
<li> <b>sqliteExprListAppend()</b> </li>
<li> <b>sqliteResultSetOfSelect()</b> </li>
<li> <b>sqliteIdListAppend()</b> </li>
<li> <b>sqliteSrcListAppend()</b> </li>
<li> <b>sqliteSelectNew()</b> </li>
<li> <b>sqliteTableNameToTable()</b> </li>
<li> <b>sqliteTableTokenToSrcList()</b> </li>
<li> <b>sqliteWhereBegin()</b> </li>
<li> <b>sqliteFindTable()</b> </li>
<li> <b>sqliteFindIndex()</b> </li>
<li> <b>sqliteTableNameFromToken()</b> </li>
<li> <b>sqliteGetVdbe()</b> </li>
<li> <b>sqlite_mprintf()</b> </li>
<li> <b>sqliteExprDup()</b> </li>
<li> <b>sqliteExprListDup()</b> </li>
<li> <b>sqliteSrcListDup()</b> </li>
<li> <b>sqliteIdListDup()</b> </li>
<li> <b>sqliteSelectDup()</b> </li>
<li> <b>sqliteFindFunction()</b> </li>
<li> <b>sqliteTriggerSelectStep()</b> </li>
<li> <b>sqliteTriggerInsertStep()</b> </li>
<li> <b>sqliteTriggerUpdateStep()</b> </li>
<li> <b>sqliteTriggerDeleteStep()</b> </li>
</ul>
</p></li>
<li><p>
On all functions and procedures, verify that pointer parameters are not NULL
before dereferencing those parameters.
</p></li>
<li><p>
Check to make sure that temporary files are opened safely: that the process
will not overwrite an existing file when opening the temp file and that
another process is unable to substitute a file for the temp file being
opened.
</p></li>
</ol>



<h2>Part II: Things to prove</h2>

<p>
Prove that SQLite exhibits the characteristics outlined below:
</p>

<ol>
<li><p>
The following are preconditions:</p>
<p><ul>
<li><b>Z</b> is an arbitrary-length NUL-terminated string.</li>
<li>An existing SQLite database has been opened.  The return value
    from the call to <b>sqlite_open()</b> is stored in the variable
    <b>db</b>.</li>
<li>The database contains at least one table of the form:
<blockquote><pre>
CREATE TABLE t1(a CLOB);
</pre></blockquote></li>
<li>There are no user-defined functions other than the standard
    build-in functions.</li>
</ul></p>
<p>The following statement of C code is executed:</p>
<blockquote><pre>
sqlite_exec_printf(
   db,
   "INSERT INTO t1(a) VALUES('%q');", 
   0, 0, 0, Z
);
</pre></blockquote>
<p>Prove the following are true for all possible values of string <b>Z</b>:</p>
<ol type="a">
<li><p>
The call to <b>sqlite_exec_printf()</b> will
return in a length of time that is a polynomial in <b>strlen(Z)</b>.
It might return an error code but it will not crash.
</p></li>
<li><p>
At most one new row will be inserted into table t1.
</p></li>
<li><p>
No preexisting rows of t1 will be deleted or modified.
</p></li>
<li><p>
No tables other than t1 will be altered in any way.
</p></li>
<li><p>
No preexisting files on the host computers filesystem, other than
the database file itself, will be deleted or modified.
</p></li>
<li><p>
For some constants <b>K1</b> and <b>K2</b>,
if at least <b>K1*strlen(Z) + K2</b> bytes of contiguous memory are
available to <b>malloc()</b>, then the call to <b>sqlite_exec_printf()</b>
will not return SQLITE_NOMEM.
</p></li>
</ol>
</p></li>


<li><p>
The following are preconditions:
<p><ul>
<li><b>Z</b> is an arbitrary-length NUL-terminated string.</li>
<li>An existing SQLite database has been opened.  The return value
    from the call to <b>sqlite_open()</b> is stored in the variable
    <b>db</b>.</li>
<li>There exists a callback function <b>cb()</b> that appends all
    information passed in through its parameters into a single
    data buffer called <b>Y</b>.</li>
<li>There are no user-defined functions other than the standard
    build-in functions.</li>
</ul></p>
<p>The following statement of C code is executed:</p>
<blockquote><pre>
sqlite_exec(db, Z, cb, 0, 0);
</pre></blockquote>
<p>Prove the following are true for all possible values of string <b>Z</b>:</p>
<ol type="a">
<li><p>
The call to <b>sqlite_exec()</b> will
return in a length of time which is a polynomial in <b>strlen(Z)</b>.
It might return an error code but it will not crash.
</p></li>
<li><p>
After <b>sqlite_exec()</b> returns, the buffer <b>Y</b> will not contain
any content from any preexisting file on the host computers file system,
except for the database file.
</p></li>
<li><p>
After the call to <b>sqlite_exec()</b> returns, the database file will
still be well-formed.  It might not contain the same data, but it will
still be a properly constructed SQLite database file.
</p></li>
<li><p>
No preexisting files on the host computers filesystem, other than
the database file itself, will be deleted or modified.
</p></li>
<li><p>
For some constants <b>K1</b> and <b>K2</b>,
if at least <b>K1*strlen(Z) + K2</b> bytes of contiguous memory are
available to <b>malloc()</b>, then the call to <b>sqlite_exec()</b>
will not return SQLITE_NOMEM.
</p></li>
</ol>
</p></li>

</ol>
}
puts {
<p><hr /></p>
<p><a href="index.html"><img src="/goback.jpg" border=0 />
Back to the SQLite Home Page</a>
</p>

</body></html>}

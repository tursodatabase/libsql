#
# Run this Tcl script to generate the tclsqlite.html file.
#
set rcsid {$Id: tclsqlite.tcl,v 1.13 2005/04/03 23:54:45 danielk1977 Exp $}
source common.tcl
header {The Tcl interface to the SQLite library}
proc METHOD {name text} {
  puts "<a name=\"$name\">\n<h3>The \"$name\" method</h3>\n"
  puts $text
}
puts {
<h2>The Tcl interface to the SQLite library</h2>

<p>The SQLite library is designed to be very easy to use from
a Tcl or Tcl/Tk script.  This document gives an overview of the Tcl
programming interface.</p>

<h3>The API</h3>

<p>The interface to the SQLite library consists of single
tcl command named <b>sqlite</b> (version 2.8) or <b>sqlite3</b>
(version 3.0).  Because there is only this
one command, the interface is not placed in a separate
namespace.</p>

<p>The <b>sqlite3</b> command is used as follows:</p>

<blockquote>
<b>sqlite3</b>&nbsp;&nbsp;<i>dbcmd&nbsp;&nbsp;database-name</i>
</blockquote>

<p>
The <b>sqlite3</b> command opens the database named in the second
argument.  If the database does not already exist, it is
automatically created.
The <b>sqlite3</b> command also creates a new Tcl
command to control the database.  The name of the new Tcl command
is given by the first argument.  This approach is similar to the
way widgets are created in Tk.
</p>

<p>
The name of the database is just the name of a disk file in which
the database is stored.
</p>

<p>
Once an SQLite database is open, it can be controlled using 
methods of the <i>dbcmd</i>.  There are currently 19 methods
defined:</p>

<p>
<ul>
}
foreach m [lsort {
 authorizer
 busy
 changes
 close
 collate
 collation_needed
 commit_hook
 complete
 copy
 errorcode
 eval
 function
 last_insert_rowid
 nullvalue
 onecolumn
 progress
 timeout
 total_changes
 trace
}] {
 puts "<li><a href=\"#$m\">$m</a></li>"
}
puts {
</ul>
</p>

<p>The use of each of these methods will be explained in the sequel, though
not in the order shown above.</p>

}

##############################################################################
METHOD close {

<p>
As its name suggests, the "close" method to an SQLite database just
closes the database.  This has the side-effect of deleting the
<i>dbcmd</i> Tcl command.  Here is an example of opening and then
immediately closing a database:
</p>

<blockquote>
<b>sqlite3 db1 ./testdb<br>
db1 close</b>
</blockquote>

<p>
If you delete the <i>dbcmd</i> directly, that has the same effect
as invoking the "close" method.  So the following code is equivalent
to the previous:</p>

<blockquote>
<b>sqlite3 db1 ./testdb<br>
rename db1 {}</b>
</blockquote>
}

##############################################################################
METHOD eval {
<p>
The most useful <i>dbcmd</i> method is "eval".  The eval method is used
to execute SQL on the database.  The syntax of the eval method looks
like this:</p>

<blockquote>
<i>dbcmd</i>&nbsp;&nbsp;<b>eval</b>&nbsp;&nbsp;<i>sql</i>
&nbsp;&nbsp;&nbsp;&nbsp;?<i>array-name&nbsp;</i>?&nbsp;?<i>script</i>?
</blockquote>

<p>
The job of the eval method is to execute the SQL statement or statements
given in the second argument.  For example, to create a new table in
a database, you can do this:</p>

<blockquote>
<b>sqlite3 db1 ./testdb<br>
db1 eval {CREATE TABLE t1(a int, b text)}</b>
</blockquote>

<p>The above code creates a new table named <b>t1</b> with columns
<b>a</b> and <b>b</b>.  What could be simpler?</p>

<p>Query results are returned as a list of column values.  If a
query requests 2 columns and there are 3 rows matching the query,
then the returned list will contain 6 elements.  For example:</p>

<blockquote>
<b>db1 eval {INSERT INTO t1 VALUES(1,'hello')}<br>
db1 eval {INSERT INTO t1 VALUES(2,'goodbye')}<br>
db1 eval {INSERT INTO t1 VALUES(3,'howdy!')}<br>
set x [db1 eval {SELECT * FROM t1 ORDER BY a}]</b>
</blockquote>

<p>The variable <b>$x</b> is set by the above code to</p>

<blockquote>
<b>1 hello 2 goodbye 3 howdy!</b>
</blockquote>

<p>You can also process the results of a query one row at a time
by specifying the name of an array variable and a script following
the SQL code.  For each row of the query result, the values of all
columns will be inserted into the array variable and the script will
be executed.  For instance:</p>

<blockquote>
<b>db1 eval {SELECT * FROM t1 ORDER BY a} values {<br>
&nbsp;&nbsp;&nbsp;&nbsp;parray values<br>
&nbsp;&nbsp;&nbsp;&nbsp;puts ""<br>
}</b>
</blockquote>

<p>This last code will give the following output:</p>

<blockquote><b>
values(*) = a b<br>
values(a) = 1<br>
values(b) = hello<p>

values(*) = a b<br>
values(a) = 2<br>
values(b) = goodbye<p>

values(*) = a b<br>
values(a) = 3<br>
values(b) = howdy!</b>
</blockquote>

<p>
For each column in a row of the result, the name of that column
is used as an index in to array.  The value of the column is stored
in the corresponding array entry.  The special array index * is
used to store a list of column names in the order that they appear.
</p>

<p>
If the array variable name is omitted or is the empty string, then the value of
each column is stored in a variable with the same name as the column
itself.  For example:
</p>

<blockquote>
<b>db1 eval {SELECT * FROM t1 ORDER BY a} {<br>
&nbsp;&nbsp;&nbsp;&nbsp;puts "a=$a b=$b"<br>
}</b>
</blockquote>

<p>
From this we get the following output
</p>

<blockquote><b>
a=1 b=hello<br>
a=2 b=goodbye<br>
a=3 b=howdy!</b>
</blockquote>

<p>
Tcl variable names can appear in the SQL statement of the second argument
in any position where it is legal to put a string or number literal.  The
value of the variable is substituted for the variable name.  If the
variable does not exist a NULL values is used.  For example:
</p>

<blockquote><b>
db1 eval {INSERT INTO t1 VALUES(5,$bigblob)}
</b></blockquote>

<p>
Note that it is not necessary to quote the $bigblob value.  That happens
automatically.  If $bigblob is a large string or binary object, this
technique is not only easier to write, it is also much more efficient
since it avoids making a copy of the content of $bigblob.
</p>

}

##############################################################################
METHOD complete {

<p>
The "complete" method takes a string of supposed SQL as its only argument.
It returns TRUE if the string is a complete statement of SQL and FALSE if
there is more to be entered.</p>

<p>The "complete" method is useful when building interactive applications
in order to know when the user has finished entering a line of SQL code.
This is really just an interface to the <b>sqlite3_complete()</b> C
function.  Refer to the <a href="c_interface.html">C/C++ interface</a>
specification for additional information.</p>
}

##############################################################################
METHOD copy {

<p>
The "copy" method copies data from a file into a table.
It returns the number of rows processed successfully from the file.
The syntax of the copy method looks like this:</p>

<blockquote>
<i>dbcmd</i>&nbsp;&nbsp;<b>copy</b>&nbsp;&nbsp;<i>conflict-algorithm</i>
&nbsp;&nbsp;<i>table-name&nbsp;</i>&nbsp;&nbsp;<i>file-name&nbsp;</i>
&nbsp;&nbsp;&nbsp;&nbsp;?<i>column-separator&nbsp;</i>?
&nbsp;&nbsp;?<i>null-indicator</i>?
</blockquote>

<p>Conflict-alogrithm must be one of the SQLite conflict algorithms for
the INSERT statement: <i>rollback</i>, <i>abort</i>,
<i>fail</i>,<i>ignore</i>, or <i>replace</i>. See the SQLite Language
section for <a href="lang.html#conflict">ON CONFLICT</a> for more information.
The conflict-algorithm must be specified in lower case.
</p>

<p>Table-name must already exists as a table.  File-name must exist, and
each row must contain the same number of columns as defined in the table.
If a line in the file contains more or less than the number of columns defined,
the copy method rollbacks any inserts, and returns an error.</p>

<p>Column-separator is an optional column separator string.  The default is
the ASCII tab character \t. </p>

<p>Null-indicator is an optional string that indicates a column value is null.
The default is an empty string.  Note that column-separator and
null-indicator are optional positional arguments; if null-indicator
is specified, a column-separator argument must be specifed and
precede the null-indicator argument.</p>

<p>The copy method implements similar functionality to the <b>.import</b>
SQLite shell command. 
The SQLite 2.x <a href="lang.html#copy"><b>COPY</b></a> statement 
(using the PostgreSQL COPY file format)
can be implemented with this method as:</p>

<blockquote>
dbcmd&nbsp;&nbsp;copy&nbsp;&nbsp;$conflictalgo
&nbsp;&nbsp;$tablename&nbsp;&nbsp;&nbsp;$filename&nbsp;
&nbsp;&nbsp;&nbsp;&nbsp;\t&nbsp;
&nbsp;&nbsp;\\N
</blockquote>

}

##############################################################################
METHOD timeout {

<p>The "timeout" method is used to control how long the SQLite library
will wait for locks to clear before giving up on a database transaction.
The default timeout is 0 millisecond.  (In other words, the default behavior
is not to wait at all.)</p>

<p>The SQLite database allows multiple simultaneous
readers or a single writer but not both.  If any process is writing to
the database no other process is allows to read or write.  If any process
is reading the database other processes are allowed to read but not write.
The entire database shared a single lock.</p>

<p>When SQLite tries to open a database and finds that it is locked, it
can optionally delay for a short while and try to open the file again.
This process repeats until the query times out and SQLite returns a
failure.  The timeout is adjustable.  It is set to 0 by default so that
if the database is locked, the SQL statement fails immediately.  But you
can use the "timeout" method to change the timeout value to a positive
number.  For example:</p>

<blockquote><b>db1 timeout 2000</b></blockquote>

<p>The argument to the timeout method is the maximum number of milliseconds
to wait for the lock to clear.  So in the example above, the maximum delay
would be 2 seconds.</p>
}

##############################################################################
METHOD busy {

<p>The "busy" method, like "timeout", only comes into play when the
database is locked.  But the "busy" method gives the programmer much more
control over what action to take.  The "busy" method specifies a callback
Tcl procedure that is invoked whenever SQLite tries to open a locked
database.  This callback can do whatever is desired.  Presumably, the
callback will do some other useful work for a short while (such as service
GUI events) then return
so that the lock can be tried again.  The callback procedure should
return "0" if it wants SQLite to try again to open the database and
should return "1" if it wants SQLite to abandon the current operation.
}

##############################################################################
METHOD last_insert_rowid {

<p>The "last_insert_rowid" method returns an integer which is the ROWID
of the most recently inserted database row.</p>
}

##############################################################################
METHOD function {

<p>The "function" method registers new SQL functions with the SQLite engine.
The arguments are the name of the new SQL function and a TCL command that
implements that function.  Arguments to the function are appended to the
TCL command before it is invoked.</p>

<p>
The following example creates a new SQL function named "hex" that converts
its numeric argument in to a hexadecimal encoded string:
</p>

<blockquote><b>
db function hex {format 0x%X}
</b></blockquote>

}

##############################################################################
METHOD nullvalue {

<p>
The "nullvalue" method changes the representation for NULL returned
as result of the "eval" method.</p>

<blockquote><b>
db1 nullvalue NULL
</b></blockquote>

<p>The "nullvalue" method is useful to differ between NULL and empty
column values as Tcl lacks a NULL representation.  The default
representation for NULL values is an empty string.</p>
}



##############################################################################
METHOD onecolumn {

<p>The "onecolumn" method works like "eval" in that it evaluates the
SQL query statement given as its argument.  The difference is that
"onecolumn" returns a single element which is the first column of the
first row of the query result.</p>

<p>This is a convenience method.  It saves the user from having to
do a "<tt>[lindex&nbsp;...&nbsp;0]</tt>" on the results of an "eval"
in order to extract a single column result.</p>
}

##############################################################################
METHOD changes {

<p>The "changes" method returns an integer which is the number of rows
in the database that were inserted, deleted, and/or modified by the most
recent "eval" method.</p>
}

##############################################################################
METHOD total_changes {

<p>The "total_changes" method returns an integer which is the number of rows
in the database that were inserted, deleted, and/or modified since the
current database connection was first opened.</p>
}

##############################################################################
METHOD authorizer {

<p>The "authorizer" method provides access to the sqlite3_set_authorizer
C/C++ interface.  The argument to authorizer is the name of a procedure that
is called when SQL statements are being compiled in order to authorize
certain operations.  The callback procedure takes 5 arguments which describe
the operation being coded.  If the callback returns the text string
"SQLITE_OK", then the operation is allowed.  If it returns "SQLITE_IGNORE",
then the operation is silently disabled.  If the return is "SQLITE_DENY"
then the compilation fails with an error.
</p>

<p>If the argument is an empty string then the authorizer is disabled.
If the argument is omitted, then the current authorizer is returned.</p>
}

##############################################################################
METHOD progress {

<p>This method registers a callback that is invoked periodically during
query processing.  There are two arguments: the number of SQLite virtual
machine opcodes between invocations, and the TCL command to invoke.
Setting the progress callback to an empty string disables it.</p>

<p>The progress callback can be used to display the status of a lengthy
query or to process GUI events during a lengthy query.</p>
}


##############################################################################
METHOD collate {

<p>This method registers new text collating sequences.  There are
two arguments: the name of the collating sequence and the name of a
TCL procedure that implements a comparison function for the collating
sequence.
</p>

<p>For example, the following code implements a collating sequence called
"NOCASE" that sorts in text order without regard to case:
</p>

<blockquote><b>
proc nocase_compare {a b} {<br>
&nbsp;&nbsp;&nbsp;&nbsp;return [string compare [string tolower $a] [string tolower $b]]<br>
}<br>
db collate NOCASE nocase_compare<br>
</b></blockquote>
}

##############################################################################
METHOD collation_needed {

<p>This method registers a callback routine that is invoked when the SQLite
engine needs a particular collating sequence but does not have that
collating sequence registered.  The callback can register the collating
sequence.  The callback is invoked with a single parameter which is the
name of the needed collating sequence.</p>
}

##############################################################################
METHOD commit_hook {

<p>This method registers a callback routine that is invoked just before
SQLite tries to commit changes to a database.  If the callback throws
an exception or returns a non-zero result, then the transaction rolls back
rather than commit.</p>
}

##############################################################################
METHOD errorcode {

<p>This method returns the numeric error code that resulted from the most
recent SQLite operation.</p>
}

##############################################################################
METHOD trace {

<p>The "trace" method registers a callback that is invoked as each SQL
statement is compiled.  The text of the SQL is appended as a single string
to the command before it is invoked.  This can be used (for example) to
keep a log of all SQL operations that an application performs.
</p>
}


footer $rcsid

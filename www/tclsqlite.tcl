#
# Run this Tcl script to generate the tclsqlite.html file.
#
set rcsid {$Id: tclsqlite.tcl,v 1.1 2000/09/30 22:46:07 drh Exp $}

puts {<html>
<head>
  <title>The Tcl interface to the SQLite library</title>
</head>
<body bgcolor=white>
<h1 align=center>
The Tcl interface to the SQLite library
</h1>}
puts "<p align=center>
(This page was last modified on [lrange $rcsid 3 4] GMT)
</p>"

puts {
<p>The SQLite library is designed to be very easy to use from
a Tcl or Tcl/Tk script.  This document gives an overview of the Tcl
programming interface.</p>

<h2>The API</h2>

<p>The interface to the SQLite library consists of single
tcl command named <b>sqlite</b>.  Because there is only this
one interface command, the interface is not placed in a separate
namespace.</p>

<p>The <b>sqlite</b> command is used as follows:</p>

<blockquote>
<b>sqlite</b>&nbsp;&nbsp;<i>dbcmd&nbsp;&nbsp;database-directory-name</i>
</blockquote>

<p>
The <b>sqlite</b> command opens the SQLite database located in the
directory named by the second argument.  If the database or directory
does not exist, it is created.  The <b>sqlite</b> command 
also creates a new Tcl
command to control the database.  The name of the new Tcl command
is given by the first argument.  This approach is similar to the
way widgets are created in Tk.
</p>

<p>
Once an SQLite database is open, it can be controlled using 
methods of the <i>dbcmd</i>.  There are currently 5 methods
defined:</p>

<p>
<ul>
<li> busy
<li> close
<li> complete
<li> eval
<li> timeout
</ul>
</p>

<p>We will explain all of these methods, though not in that order.
We will be begin with the "close" method.</p>

<h2>The "close" method</h2>

<p>
As its name suggests, the "close" method to an SQLite database just
closes the database.  This has the side-effect of deleting the
<i>dbcmd</i> Tcl command.  Here is an example of opening and then
immediately closing a database:
</p>

<blockquote>
<b>sqlite db1 ./testdb<br>
db1 close</b>
</blockquote>

<p>
If you delete the <i>dbcmd</i> directly, that has the same effect
as invoking the "close" method.  So the following code is equivalent
to the previous:</p>

<blockquote>
<b>sqlite db1 ./testdb<br>
rename db1 {}</b>
</blockquote>

<h2>The "eval" method</h2>

<p>
The most useful <i>dbcmd</i> method is "eval".  The eval method is used
to execute SQL on the database.  The syntax of the eval method looks
like this:</p>

<blockquote>
<i>dbcmd</i>&nbsp;&nbsp;<b>eval</b>&nbsp;&nbsp;<i>sql</i>
&nbsp;&nbsp;?<i>array-name&nbsp;&nbsp;script</i>?
</blockquote>

<p>
The job of the eval method is to execute the SQL statement or statements
given in the second argument.  For example, to create a new table in
a database, you can do this:</p>

<blockquote>
<b>sqlite db1 ./testdb<br>
db1 eval {CREATE TABLE t1(a int, b text)}</b>
</blockquote>

<p>The above code creates a new table named <b>t1</b> with columns
<b>a</b> and <b>b</b>.  What could be simplier?</p>

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
the SQL code.  For each row of the query result, the value of each
column will be inserted into the array variable and the script will
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
If the array variable name is the empty string, then the value of
each column is stored in a variable with the same name as the column
itself.  For example:
</p>

<blockquote>
<b>db1 eval {SELECT * FROM t1 ORDER BY a} {} {<br>
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

<h2>The "complete" method</h2>
<i>TBD</i>

<h2>The "timeout" method</h2>
<i>TBD</i>

<h2>The "busy" method</h2>
<i>TBD</i>


}

puts {
<p><hr /></p>
<p><a href="index.html"><img src="/goback.jpg" border=0 />
Back to the SQLite Home Page</a>
</p>

</body></html>}

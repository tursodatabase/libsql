#
# Run this Tcl script to generate the sqlite.html file.
#
set rcsid {$Id: sqlite.tcl,v 1.13 2000/08/08 20:19:09 drh Exp $}

puts {<html>
<head>
  <title>sqlite: A program of interacting with SQLite databases</title>
</head>
<body bgcolor=white>
<h1 align=center>
sqlite: A program to administer SQLite databases
</h1>}
puts "<p align=center>
(This page was last modified on [lrange $rcsid 3 4] GMT)
</p>"

puts {
<p>The SQLite library includes a simple command-line utility named
<b>sqlite</b> that allows the user to manually enter and execute SQL
commands against an SQLite database.  This document provides a brief
introduction on how to use <b>sqlite</b>.

<h2>Getting Started</h2>

<p>To start the <b>sqlite</b> program, just type "sqlite" followed by
the name of an SQLite database.  An SQLite database is really just
a directory full of GDBM files, so the argument to the sqlite command
should really be the name of a directory on your disk.  If that
directory did not previously contain an SQLite database, a new one
is created for you automatically.  If the directory did not previously
exist, it is automatically created.  The <b>sqlite</b> program will
then prompt you to enter SQL.  Type in SQL statements (terminated by a
semicolon), press "Enter" and the SQL will be executed.  It's as
simple as that!</p>

<p>For example, to create a new SQLite database named "ex1" 
with a single table named "tbl1", you might do this:</p>
}

proc Code {body} {
  puts {<blockquote><pre>}
  regsub -all {&} [string trim $body] {\&amp;} body
  regsub -all {>} $body {\&gt;} body
  regsub -all {<} $body {\&lt;} body
  regsub -all {\(\(\(} $body {<font color="#00671f"><u>} body
  regsub -all {\)\)\)} $body {</u></font>} body
  puts $body
  puts {</pre></blockquote>}
}

Code {
$ (((sqlite ex1)))
Enter ".help" for instructions
sqlite> (((create table tbl1(one varchar(10), two smallint);)))
sqlite> (((insert into tbl1 values('hello!',10);)))
sqlite> (((insert into tbl1 values('goodbye', 20);)))
sqlite> (((select * from tbl1;)))
hello!|10
goodbye|20
sqlite>
}

puts {
<p>(In the example above, and in all subsequent examples, the commands
you type are underlined shown with a green tint and the responses
from the computer are shown in black without underlining.)</p>

<p>You can terminate the sqlite program by typing your systems
End-Of-File character (usually a Control-D) or the interrupt
character (usually a Control-C).</p>

<p>Make sure you type a semicolon at the end of each SQL command!
The sqlite looks for a semicolon to know when your SQL command is
complete.  If you omit the semicolon, sqlite will give you a
continuation prompt and wait for you to enter more text to be
added to the current SQL command.  This feature allows you to
enter SQL commands that span multiple lines.  For example:</p>
}

Code {
sqlite> (((CREATE TABLE tbl2 ()))
   ...> (((  f1 varchar(30) primary key,)))
   ...> (((  f2 text,)))
   ...> (((  f3 real)))
   ...> ((();)))
sqlite> 
}

puts {
<p>If you exit sqlite and look at the contents of the directory "ex1"
you'll see that it now contains two files: <b>sqlite_master.tcl</b>
and <b>tbl1.tbl</b>.  The <b>tbl1.tbl</b> file contains all the
data for table "tbl1" in your database.  The file
<b>sqlite_master.tbl</b> is a special table found on all SQLite
databases that records information about all other tables and
indices.  In general, an SQLite database will contain one "*.tbl"
file for each table and index in your database, plus the extra
"sqlite_master.tbl" file used to store the database schema.</p>

<h2>Aside: Querying the SQLITE_MASTER table</h2>

<p>You can execute "SELECT" statements against the
special sqlite_master table just like any other table
in an SQLite database.  For example:</p>
}

Code {
$ (((sqlite ex1)))
Enter ".help" for instructions
sqlite> (((select * from sqlite_master;)))
type = table
name = tbl1
tbl_name = tbl1
sql = create table tbl1(one varchar(10), two smallint)
sqlite>
}

puts {
<p>
But you cannot execute DROP TABLE, UPDATE, INSERT or DELETE against
the sqlite_master table.  The sqlite_master
table is updated automatically as you create or drop tables and
indices from the database.  You can not make manual changes
to the sqlite_master table.
</p>

<h2>Special commands to sqlite</h2>

<p>
Most of the time, sqlite just reads lines of input and passes them
on to the SQLite library for execution.
But if an input line begins with a dot ("."), then
that line is intercepted and interpreted by the sqlite program itself.
These "dot commands" are typically used to change the output format
of queries, or to execute certain prepackaged query statements.
</p>

<p>
For a listing of the available dot commands, you can enter ".help"
at any time.  For example:
</p>}

Code {
sqlite> (((.help)))
.dump                  Dump database in a text format
.exit                  Exit this program
.explain               Set output mode suitable for EXPLAIN
.header ON|OFF         Turn display of headers on or off
.help                  Show this message
.indices TABLE         Show names of all indices on TABLE
.mode MODE             Set mode to one of "line", "column", "list", or "html"
.mode insert TABLE     Generate SQL insert statements for TABLE
.output FILENAME       Send output to FILENAME
.output stdout         Send output to the screen
.schema ?TABLE?        Show the CREATE statements
.separator STRING      Change separator string for "list" mode
.tables                List names all tables in the database
.timeout MS            Try opening locked tables for MS milliseconds
.width NUM NUM ...     Set column widths for "column" mode
sqlite> 
}

puts {
<h2>Changing Output Formats</h2>

<p>The sqlite program is able to show the results of a query
in five different formats: "line", "column", "list", "html", and "insert".
You can use the ".mode" dot command to switch between these three output
formats.</p>

<p>The default output mode is "list".  In
list mode, each record of a query result is written on one line of
output and each column within that record is separated by a specific
separator string.  The default separator is a pipe symbol ("|").
List mode is especially useful when you are going to send the output
of a query to another program (such as AWK) for additional processing.</p>}

Code {
sqlite> (((.mode list)))
sqlite> (((select * from tbl1;)))
hello|10
goodbye|20
sqlite>
}

puts {
<p>You can use the ".separator" dot command to change the separator
for list mode.  For example, to change the separator to a comma and
a space, you could do this:</p>}

Code {
sqlite> (((.separator ", ")))
sqlite> (((select * from tbl1;)))
hello, 10
goodbye, 20
sqlite>
}

puts {
<p>In "line" mode, each column in a row of the database
is shown on a line by itself.  Each line consists of the column
name, an equal sign and the column data.  Successive records are
separated by a blank line.  Here is an example of line mode
output:</p>}

Code {
sqlite> (((.mode line)))
sqlite> (((select * from tbl1;)))
one = hello
two = 10

one = goodbye
two = 20
sqlite>
}

puts {
<p>In column mode, each record is shown on a separate line with the
data aligned in columns.  For example:</p>}

Code {
sqlite> (((.mode column)))
sqlite> (((select * from tbl1;)))
one         two       
----------  ----------
hello       10        
goodbye     20        
sqlite>
}

puts {
<p>By default, each column is at least 10 characters wide. 
Data that is too wide to fit in a column is truncated.  You can
adjust the column widths using the ".width" command.  Like this:</p>}

Code {
sqlite> (((.width 12 6)))
sqlite> (((select * from tbl1;)))
one           two   
------------  ------
hello         10    
goodbye       20    
sqlite>
}

puts {
<p>The ".width" command in the example above sets the width of the first
column to 12 and the width of the second column to 6.  All other column
widths were unaltered.  You can gives as many arguments to ".width" as
necessary to specify the widths of as many columns as are in your
query results.</p>

<p>If you specify a column a width of 0, then the column
width is automatically adjusted to be the maximum of three
numbers: 10, the width of the header, and the width of the
first row of data.  This makes the column width self-adjusting.
The default width setting for every column is this 
auto-adjusting 0 value.</p>

<p>The column labels that appear on the first two lines of output
can be turned on and off using the ".header" dot command.  In the
examples above, the column labels are on.  To turn them off you
could do this:</p>}

Code {
sqlite> (((.header off)))
sqlite> (((select * from tbl1;)))
hello         10    
goodbye       20    
sqlite>
}

puts {
<p>Another useful output mode is "insert".  In insert mode, the output
is formatted to look like SQL INSERT statements.  You can use insert
mode to generate text that can later be used to input data into a 
different database.</p>

<p>When specifying insert mode, you have to give an extra argument
which is the name of the table to be inserted into.  For example:</p>
}

Code {
sqlite> (((.mode insert new_table)))
sqlite> (((select * from tbl1;)))
INSERT INTO 'new_table' VALUES('hello',10);
INSERT INTO 'new_table' VALUES('goodbye',20);
sqlite>
}

puts {
<p>The last output mode is "html".  In this mode, sqlite writes
the results of the query as an XHTML table.  The beginning
&lt;TABLE&gt; and the ending &lt;/TABLE&gt; are not written, but
all of the intervening &lt;TR&gt;s, &lt;TH&gt;s, and &lt;TD&gt;s
are.  The html output mode is envisioned as being useful for
CGI.</p>
}

puts {
<h2>Writing results to a file</h2>

<p>By default, sqlite sends query results to standard output.  You
can change this using the ".output" command.  Just put the name of
an output file as an argument to the .output command and all subsequent
query results will be written to that file.  Use ".output stdout" to
begin writing to standard output again.  For example:</p>}

Code {
sqlite> (((.mode list)))
sqlite> (((.separator |)))
sqlite> (((.output test_file_1.txt)))
sqlite> (((select * from tbl1;)))
sqlite> (((.exit)))
$ (((cat test_file_1.txt)))
hello|10
goodbye|20
$
}

puts {
<h2>Querying the database schema</h2>

<p>The sqlite program provides several convenience commands that
are useful for looking at the schema of the database.  There is
nothing that these commands do that cannot be done by some other
means.  These commands are provided purely as a shortcut.</p>

<p>For example, to see a list of the tables in the database, you
can enter ".tables".</p>
}

Code {
sqlite> (((.tables)))
tbl1
tbl2
sqlite>
}

puts {
<p>The ".tables" command is the same as setting list mode then
executing the following query:</p>

<blockquote><pre>
SELECT name FROM sqlite_master 
WHERE type='table' 
ORDER BY name;
</pre></blockquote>

<p>In fact, if you look at the source code to the sqlite program
(found in the source tree in the file src/shell.c) you'll find
exactly the above query.</p>

<p>The ".indices" command works in a similar way to list all of
the indices for a particular table.  The ".indices" command takes
a single argument which is the name of the table for which the
indices are desired.  Last, but not least, is the ".schema" command.
With no arguments, the ".schema" command shows the original CREATE TABLE
and CREATE INDEX statements that were used to build the current database.
If you give the name of a table to ".schema", it shows the original
CREATE statement used to make that table and all if its indices.
We have:</p>}

Code {
sqlite> (((.schema)))
create table tbl1(one varchar(10), two smallint)
CREATE TABLE tbl2 (
  f1 varchar(30) primary key,
  f2 text,
  f3 real
)
sqlite> (((.schema tbl2)))
CREATE TABLE tbl2 (
  f1 varchar(30) primary key,
  f2 text,
  f3 real
)
sqlite>
}

puts {
<p>The ".schema" command accomplishes the same thing as setting
list mode, then entering the following query:</p>

<blockquote><pre>
SELECT sql FROM sqlite_master
WHERE type!='meta'
ORDER BY tbl_name, type DESC, name
</pre></blockquote>

<p>Or, if you give an argument to ".schema" because you only
want the schema for a single table, the query looks like this:</p>

<blockquote><pre>
SELECT sql FROM sqlite_master
WHERE tbl_name LIKE '%s' AND type!='meta'
ORDER BY type DESC, name
</pre></blockquote>

<p>The <b>%s</b> in the query above is replaced by the argument
to ".schema", of course.  Notice that the argument to the ".schema"
command appears to the right of an SQL LIKE operator.  So you can
use wildcards in the name of the table.  For example, to get the
schema for all tables whose names contain the character string
"abc" you could enter:</p>}

Code {
sqlite> (((.schema %abc%)))
}

puts {
<p>
Along these same lines,
the ".table" command also accepts a pattern as its first argument.
If you give an argument to the .table command, a "%" is both
appended and prepended and a LIKE clause is added to the query.
This allows you to list only those tables that match a particular
pattern.</p>

<h2>Converting An Entire Database To An ASCII Text File</h2>

<p>Use the ".dump" command to convert the entire contents of a
database into a single ASCII text file.  This file can be converted
back into a database by piping it back into <b>sqlite</b>.</p>

<p>A good way to make an archival copy of a database is this:</p>
}

Code {
$ (((echo '.dump' | sqlite ex1 | gzip -c >ex1.dump.gz)))
}

puts {
<p>This generates a file named <b>ex1.dump.gz</b> that contains everything
you need to reconstruct the database at a later time, or on another
machine.  To reconstruct the database, just type:</p>
}

Code {
$ (((zcat ex1.dump.gz | sqlite ex2)))
}

puts {
<p>The text format used is the same as used by
<a href="http://www.postgresql.org/">PostgreSQL</a>, so you
can also use the .dump command to export an SQLite database
into a PostgreSQL database.  Like this:</p>
}

Code {
$ (((createdb ex2)))
$ (((echo '.dump' | sqlite ex1 | psql ex2)))
}

puts {
<p>You can almost (but not quite) go the other way and export
a PostgreSQL database into SQLite using the <b>pg_dump</b> utility.
Unfortunately, when <b>pg_dump</b> writes the database schema information,
it uses some SQL syntax that SQLite does not understand.
So you cannot pipe the output of <b>pg_dump</b> directly 
into <b>sqlite</b>.
But if you can recreate the
schema separately, you can use <b>pg_dump</b> with the <b>-a</b>
option to list just the data
of a PostgreSQL database and import that directly into SQLite.</p>
}

Code {
$ (((sqlite ex3 <schema.sql)))
$ (((pg_dump -a ex2 | sqlite ex3)))
}

puts {
<h2>Other Dot Commands</h2>

<p>The ".explain" dot command can be used to set the output mode
to "column" and to set the column widths to values that are reasonable
for looking at the output of an EXPLAIN command.  The EXPLAIN command
is an SQLite-specific SQL extension that is useful for debugging.  If any
regular SQL is prefaced by EXPLAIN, then the SQL command is parsed and
analyzed but is not executed.  Instead, the sequence of virtual machine
instructions that would have been used to execute the SQL command are
returned like a query result.  For example:</p>}

Code {
sqlite> (((.explain)))
sqlite> (((explain delete from tbl1 where two<20;)))
addr  opcode        p1     p2     p3          
----  ------------  -----  -----  -------------------------------------   
0     ListOpen      0      0                  
1     Open          0      1      tbl1        
2     Next          0      9                  
3     Field         0      1                  
4     Integer       20     0                  
5     Ge            0      2                  
6     Key           0      0                  
7     ListWrite     0      0                  
8     Goto          0      2                  
9     Noop          0      0                  
10    ListRewind    0      0                  
11    ListRead      0      14                 
12    Delete        0      0                  
13    Goto          0      11                 
14    ListClose     0      0                  
}

puts {

<p>The ".timeout" command sets the amount of time that the <b>sqlite</b>
program will wait for locks to clear on files it is trying to access
before returning an error.  The default value of the timeout is zero so
that an error is returned immediately if any needed database table or
index is locked.</p>

<p>And finally, we mention the ".exit" command which causes the
sqlite program to exit.</p>

<h2>Using sqlite in a shell script</h2>

<p>
One way to use sqlite in a shell script is to use "echo" or
"cat" to generate a sequence of commands in a file, then invoke sqlite 
while redirecting input from the generated command file.  This
works fine and is appropriate in many circumstances.  But as
an added convenience, sqlite allows a single SQL command to be
entered on the command line as a second argument after the
database name.  When the sqlite program is launched with two
arguments, the second argument is passed to the SQLite library
for processing, the query results are printed on standard output
in list mode, and the program exits.  This mechanism is designed
to make sqlite easy to use in conjunction with programs like
"awk".  For example:</p>}

Code {
$ (((sqlite ex1 'select * from tbl1' |)))
> ((( awk '{printf "<tr><td>%s<td>%s\n",$1,$2 }')))
<tr><td>hello<td>10
<tr><td>goodbye<td>20
$
}

puts {
<h2>Compiling the sqlite program from sources</h2>

<p>
The sqlite program is built automatically when you compile the
sqlite library.  Just get a copy of the source tree, run
"configure" and then "make".</p>
}

puts {
<p><hr /></p>
<p><a href="index.html"><img src="/goback.jpg" border=0 />
Back to the SQLite Home Page</a>
</p>

</body></html>}

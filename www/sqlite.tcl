#
# Run this Tcl script to generate the sqlite.html file.
#
set rcsid {$Id: sqlite.tcl,v 1.4 2000/05/31 02:27:50 drh Exp $}

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
is created for you automatically.  The <b>sqlite</b> program will
prompt you to enter SQL.  Type in SQL statements (terminated by a
semicolon, press "Enter" and the SQL will be executed.  It's as
simple as that!</p>

<p>For example, to create a new SQLite database named "ex1" 
with a single table named "tbl1", you might do this:</p>
}

proc Code {body} {
  puts {<blockquote><pre>}
  regsub -all {&} [string trim $body] {\&amp;} body
  regsub -all {>} $body {\&gt;} body
  regsub -all {<} $body {\&lt;} body
  regsub -all {\(\(\(} $body {<font color="#00671f"><i>} body
  regsub -all {\)\)\)} $body {</i></font>} body
  puts $body
  puts {</pre></blockquote>}
}

Code {
$ (((mkdir ex1)))
$ (((sqlite ex1)))
Enter ".help" for instructions
sql> (((create table tbl1(one varchar(10), two smallint);)))
sql> (((insert into tbl1 values('hello!',10);)))
sql> (((insert into tbl1 values('goodbye', 20);)))
sql> (((select * from tbl1;)))
one = hello!
two = 10

one = goodbye
two = 20
sql>
}

puts {
<p>(In the example above, and in all subsequent examples, the commands
you type are shown with a green tint in an italic font and the responses
from the computer are shown in black with a constant-width font.)</p>

<p>You can terminate the sqlite program by typing your systems
End-Of-File character (usually a Control-D) or the interrupt
character (usually a Control-C).</p>

<p>Make sure you type a semicolon at the end of each SQL command.
The sqlite looks for a semicolon to know when your SQL command is
complete.  If you omit the semicolon, sqlite will give you a
continuation prompt and wait for you to enter more text to be
added to the current SQL command.  This feature allows you to
enter SQL commands that span multiple lines.  For example:</p>
}

Code {
sql> (((CREATE TABLE tbl2 ()))
.... (((  f1 varchar(30) primary key,)))
.... (((  f2 text,)))
.... (((  f3 real)))
.... ((();)))
sql> 
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
sql> (((select * from sqlite_master;)))
type = table
name = tbl1
tbl_name = tbl1
sql = create table tbl1(one varchar(10), two smallint)
sql>
}

puts {
<p>
But you cannot execute DROP TABLE, UPDATE, INSERT or DELETE against
the sqlite_master table.  At least not directly.  The sqlite_master
table is updated automatically as you create or drop tables and
indices from the database, but you can not modify sqlite_master
directly.
</p>

<h2>Special commands to sqlite</h2>

<p>
Most of the time, sqlite just reads lines of input and passes them
on to the SQLite library for execution.
But if an input line begins with a dot ("."), then
that line is intercepted and interpreted by the sqlite program itself.
These "dot commands" are typically used to change the output format
of queries, or to execute certain command prepackaged query statements.
</p>

<p>
For a listing of the available dot commands, you can enter ".help"
at any time.  For example:
</p>}

Code {
sql> (((.help)))
.exit                  Exit this program
.explain               Set output mode suitable for EXPLAIN
.header ON|OFF         Turn display of headers on or off
.help                  Show this message
.indices TABLE         Show names of all indices on TABLE
.mode MODE             Set mode to one of "line", "column", or "list"
.output FILENAME       Send output to FILENAME
.output stdout         Send output to the screen
.schema ?TABLE?        Show the CREATE statements
.separator STRING      Change separator string for "list" mode
.tables                List names all tables in the database
.width NUM NUM ...     Set column widths for "column" mode
sql> 
}

puts {
<h2>Changing Output Formats</h2>

<p>The sqlite program is able to show the results of a query
in three different formats: "line", "column", and "list".  You can
use the ".mode" dot command to switch between these three output
formats.</p>

<p>In "line" mode (the default), each field in a record of the database
is shown on a line by itself.  Each line consists of the field
name, an equal sign and the field data.  Successive records are
separated by a blank line.  Here is an example of line mode
output:</p>}

Code {
sql> (((.mode line)))
sql> (((select * from tbl1;)))
one = hello
two = 10

one = goodbye
two = 20
sql>
}

puts {
<p>In column mode, each record is shown on a separate line with the
data aligned in columns.  For example:</p>}

Code {
sql> (((.mode column)))
sql> (((select * from tbl1;)))
one         two       
----------  ----------
hello       10        
goodbye     20        
sql>
}

puts {
<p>By default, each column is 10 characters wide. 
Data that is too wide to fit in a column is truncated.  You can
adjust the column widths using the ".width" command.  Like this:</p>}

Code {
sql> (((.width 12 6)))
sql> (((select * from tbl1;)))
one           two   
------------  ------
hello         10    
goodbye       20    
sql>
}

puts {
<p>The ".width" command in the example above set the width of the first
column to 12 and the width of the second column to 6.  All other column
widths were unaltered.  You can gives as many arguments to ".width" as
necessary to specify the widths of as many columns as are in your
query results.</p>

<p>The column labels that appear on the first two lines of output
can be turned on and off using the ".header" dot command.  In the
examples above, the column labels are on.  To turn them off you
could do this:</p>}

Code {
sql> (((.header off)))
sql> (((select * from tbl1;)))
hello         10    
goodbye       20    
sql>
}

puts {
<p>The third output mode supported by sqlite is called "list".  In
list mode, each record of a query result is written on one line of
output and each field within that record is separated by a specific
separator string.  The default separator is a pipe symbol ("|").
List mode is especially useful when you are going to send the output
of a query to another program (such as AWK) for additional process.</p>}

Code {
sql> (((.mode list)))
sql> (((select * from tbl1;)))
hello|10
goodbye|20
sql>
}

puts {
<p>You can use the ".separator" dot command to change the separator
for list mode.  For example, to change the separator to a comma and
a space, you could do this:</p>}

Code {
sql> (((.separator ", ")))
sql> (((select * from tbl1;)))
hello, 10
goodbye, 20
sql>
}

puts {
<h2>Writing results to a file</h2>

<p>By default, sqlite sends query results to standard output.  You
can change this using the ".output" command.  Just put the name of
an output file as an argument to the .output command and all subsequent
query results will be written to that file.  Use ".output stdout" to
begin writing to standard output again.  For example:</p>}

Code {
sql> (((.mode list)))
sql> (((.separator |)))
sql> (((.output test_file_1.txt)))
sql> (((select * from tbl1;)))
sql> (((.exit)))
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
sql> (((.tables)))
tbl1
tbl2
sql>
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
sql> (((.schema)))
create table tbl1(one varchar(10), two smallint)
CREATE TABLE tbl2 (
  f1 varchar(30) primary key,
  f2 text,
  f3 real
)
sql> (((.schema tbl2)))
CREATE TABLE tbl2 (
  f1 varchar(30) primary key,
  f2 text,
  f3 real
)
sql>
}

puts {
<p>The ".schema" command accomplishes the same thing as setting
list mode, then entering the following query:</p>

<blockquote><pre>
SELECT sql FROM sqlite_master
ORDER BY tbl_name, type DESC, name
</pre></blockquote>

<p>Of, if you give an argument to ".schema" because you only
one the schema for a single table, the query looks like this:</p>

<blockquote><pre>
SELECT sql FROM sqlite_master
WHERE tbl_name LIKE '%s'
ORDER BY type DESC, name
</pre></blockquote>

<p>The <b>%s</b> in the query above is replaced by the argument
to ".schema", of course.</p>

<h2>Other Dot Commands</h2>

<p>The ".explain" dot command can be used to set the output mode
to "column" and to set the column widths to values that are reasonable
for looking at the output of an EXPLAIN command.  The EXPLAIN command
is an SQLite-specific command that is useful for debugging.  If any
regular SQL is prefaced by EXPLAIN, then the SQL command is parsed and
analyzed but is not executed.  Instead, the sequence of virtual machine
instructions that would have been used to execute the SQL command are
returned like a query result.  For example:</p>}

Code {
sql> (((.explain)))
sql> (((explain delete from tbl1 where two<20;)))
addr  opcode        p1     p2     p3          
----  ------------  -----  -----  -------------------------------------   
0     ListOpen      0      0                  
1     Open          0      0      tbl1        
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

#
# Run this script to generated a faq.html output file
#
set rcsid {$Id: faq.tcl,v 1.2 2001/11/24 13:23:05 drh Exp $}

puts {<html>
<head>
  <title>SQLite Frequently Asked Questions</title>
</head>
<body bgcolor="white">
<h1 align="center">Frequently Asked Questions</h1>
}
puts "<p align=center>
(This page was last modified on [lrange $rcsid 3 4] UTC)
</p>"


set cnt 1
proc faq {question answer} {
  set ::faq($::cnt) [list [string trim $question] [string trim $answer]]
  incr ::cnt
}

#############
# Enter questions and answers here.

faq {
  How do I create an AUTOINCREMENT field.
} {
  SQLite does not support AUTOINCREMENT.  If you need a unique key for
  a new entry in a table, you can create an auxiliary table
  with a single entry that holds the next available value for that key.
  Like this:
<blockquote><pre>
CREATE TABLE counter(cnt);
INSERT INTO counter VALUES(1);
</pre></blockquote>
  Once you have a counter set up, you can generate a unique key as follows:
<blockquote><pre>
BEGIN TRANSACTION;
SELECT cnt FROM counter;
UPDATE counter SET cnt=cnt+1;
COMMIT;
</pre></blockquote>
  There are other ways of simulating the effect of AUTOINCREMENT but
  this approach seems to be the easiest and most efficient.
}

faq {
  SQLite lets me insert a string into a database column of type integer!
} {
  <p>This is a feature, not a bug.  SQLite is typeless.  Any data can be
  inserted into any column.  You can put arbitrary length strings into
  integer columns, floating point numbers in boolean columns, or dates
  in character columns.  The datatype you assign to a column in the
  CREATE TABLE command is (mostly) ignored.  Every column is able to hold
  an arbitrary length string.</p>

  <p>Because SQLite ignores data types, you can omit the data type definition
  from columns in CREATE TABLE statements.  For example, instead of saying
<blockquote><pre>
CREATE TABLE t1(
  f1 int,
  f2 varchar(10),
  f3 boolean
);
</pre></blockquote>
  You can save yourself a lot of typing and formatting by omitting the
  data type declarations, like this:
<blockquote><pre>
CREATE TABLE t1(f1,f2,f3);
</pre></blockquote>
  </p>
}

faq {
  Why does SQLite think that the expression '0'=='00' is TRUE?
} {
  <p>This is a consequence of SQLite being typeless.  All data is stored
  internally as a null-terminated string.  There is no concept of
  separate data types for strings and numbers.</p>

  <p>When doing a comparison, SQLite looks at the string on both sides of
  the comparison operator.  If both strings look like pure numeric
  values (with no extra punctuation or spacing) then the strings are
  converted to floating point numbers using <b>atof()</b> and the results
  are compared.  The results of <b>atof("0")</b> and <b>atof("00")</b>
  are both 0.0, so those two strings are considered to be equal.</p>

  <p>If only one string in a comparison is a pure numeric, then that string
  is assumed to be less than the other.  Of neither string is a pure numeric,
  then <b>strcmp()</b> is used for the comparison.</p>
}

faq {
  The second INSERT in the following sequence of commands returns with 
  constraint error.
  <blockquote>
     CREATE TABLE t(s varchar(10) primary key);<br>
     INSERT INTO t VALUES('0');<br>
     INSERT INTO t VALUES('0.0');<br>
  </blockquote>
  Why is this?
} {
  <p>Because column <b>s</b> is a primary key, all values of <b>s</b> must
  be unique.  But SQLite thinks that <b>'0'</b> and <b>'0.0'</b> are the
  same value because they compare equal to one another numerically.
  (See the previous question.)  Hence the values are not unique and the
  constraint fails.</p>

  <p>You can work around this issue in several ways:</p>
  <ol>
  <li><p>Remove the <b>primary key</b> clause from the CREATE TABLE so that
         <b>s</b> can contain more than one entry with the same value. 
         If you need an index on the <b>s</b> column then create it separately.
         </p></li>
  <li><p>Prepend a space to the beginning of every <b>s</b> value.  The initial
         space will mean that the entries are not pure numerics and hence
         will be compared as strings using <b>strcmp()</b>.</p></li>
  </ol>
}
        
faq {
  My linux box is not able to read an SQLite database that was created
  on my SparcStation.
} {
  <p>The x86 processor on your windows box is little-endian (meaning that
  the least signification byte of integers comes first) but the Sparc is
  big-endian (the most significant bytes comes first).  SQLite databases
  created on a little-endian architecture cannot be used on a big-endian
  machine and vice versa.</p>

  <p>If you need to move the database from one machine to another, you'll
  have to do an ASCII dump of the database on the source machine and then
  reconstruct the database at the destination machine.  The following is
  a typical command for transferring an SQLite databases between two
  machines:
<blockquote><pre>
echo .dump | sqlite from.db | ssh sparc sqlite to.db
</pre></blockquote>
  The command above assumes the name of the destination machine is
  <b>sparc</b> and that you have SSH running on both the source and
  destination.  An alternative approach is to save the output of the first
  <b>sqlite</b> command in a temporary file, move the temporary file
  to the destination machine, then run the second <b>sqlite</b> command
  while redirecting input from the temporary file.</p>
}

faq {
  Can multiple applications or multiple instances of the same
  application access a single database file at the same time?
} {
  <p>Multiple processes can have the same database open at the same
  time.  On unix systems, multiple processes can be doing a SELECT
  at the same time.  But only one process can be making changes to
  the database at once.  On windows, only a single process can be
  reading from the database at one time since Win95/98/ME does not
  support reader/writer locks.</p>

  <p>The locking mechanism used to control simultaneous access might
  not work correctly if the database file is kept on an NFS filesystem.
  You should avoid putting SQLite database files on NFS if multiple
  processes might try to access the file at the same time.</p>

  <p>Locking in SQLite is very course-grained.  SQLite locks the
  entire database.  Big database servers (PostgreSQL, MySQL, Oracle, etc.)
  generally have finer grained locking, such as locking on a single
  table or a single row within a table.  If you have a massively
  parallel database application, you should consider using a big database
  server instead of SQLite.</p>

  <p>When SQLite tries to access a file that is locked by another
  process, the default behavior is to return SQLITE_BUSY.  You can
  adjust this behavior from C code using the <b>sqlite_busy_handler()</b> or
  <b>sqlite_busy_timeout()</b> API functions.  See the API documentation
  for details.</p>
}

faq {
  Is SQLite threadsafe?
} {
  <p>Almost.  In the source file named "<b>os.c</b>" there are two functions
  named <b>sqliteOsEnterMutex()</b> and <b>sqliteOsLeaveMutex()</b>.  In
  the default distribution these functions are stubs.  They do not do anything.
  If you change them so that they actually implement a mutex, then SQLite
  will be threadsafe.  But because these routines are stubs, the default
  SQLite distribution is not threadsafe.</p>
}

faq {
  How do I list all tables/indices contained in an SQLite database
} {
  <p>If you are running the <b>sqlite</b> command-line access program
  you can type "<b>.tables</b>" to get a list of all tables.  Or you
  can type "<b>.schema</b>" to see the complete database schema including
  all tables and indices.  Either of these commands can be followed by
  a LIKE pattern that will restrict the tables that are displayed.</p>

  <p>From within a C/C++ program (or a script using Tcl/Ruby/Perl/Python
  bindings) you can get access to table and index names by doing a SELECT
  on a special table named "<b>SQLITE_MASTER</b>".  Every SQLite database
  has an SQLITE_MASTER table that defines the schema for the database.
  The SQLITE_MASTER table looks like this:</p>
<blockquote><pre>
CREATE TABLE sqlite_master (
  type TEXT,
  name TEXT,
  tbl_name TEXT,
  rootpage INTEGER,
  sql TEXT
);
</pre></blockquote>
  <p>For tables, the <b>type</b> field will always be <b>'table'</b> and the
  <b>name</b> field will be the name of the table.  So to get a list of
  all tables in the database, use the following SELECT command:</p>
<blockquote><pre>
SELECT name FROM sqlite_master
WHERE type='table'
ORDER BY name;
</pre></blockquote>
  <p>For indices, <b>type</b> is equal to <b>'index'</b>, <b>name</b> is the
  name of the index and <b>tbl_name</b> is the name of the table to which
  the index belongs.  For both tables and indices, the <b>sql</b> field is
  the text of the original CREATE TABLE or CREATE INDEX statement that
  created the table or index.  For automatically created indices (used
  to implement the PRIMARY KEY or UNIQUE constraints) the <b>sql</b> field
  is NULL.</p>

  <p>The SQLITE_MASTER table is read-only.  You cannot change this table
  using UPDATE, INSERT, or DELETE.  The table is automatically updated by
  CREATE TABLE, CREATE INDEX, DROP TABLE, and DROP INDEX commands.</p>

  <p>Temporary tables do not appear in the SQLITE_MASTER table.  At this time
  there is no way to get a listing of temporary tables and indices.</p>
}

# End of questions and answers.
#############

puts {<DL COMPACT>}
for {set i 1} {$i<$cnt} {incr i} {
  puts "  <DT><A HREF=\"#q$i\">($i)</A></DT>"
  puts "  <DD>[lindex $faq($i) 0]</DD>"
}
puts {</DL><HR />}

for {set i 1} {$i<$cnt} {incr i} {
  puts "<A NAME=\"q$i\">"
  puts "<P><B>($i) [lindex $faq($i) 0]</B></P>\n"
  puts "<BLOCKQUOTE>[lindex $faq($i) 1]</BLOCKQUOTE>\n"
}

puts {
<p><hr /></p>
<p><a href="index.html"><img src="/goback.jpg" border=0 />
Back to the SQLite Home Page</a>
</p>

</body></html>}

#
# Run this script to generated a faq.html output file
#
set rcsid {$Id: faq.tcl,v 1.12 2002/07/30 17:42:10 drh Exp $}

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
  <p>Short answer: A column declared INTEGER PRIMARY KEY will
  autoincrement.</p>

  <p>Here is the long answer:
  Beginning with version SQLite 2.3.4, If you declare a column of
  a table to be INTEGER PRIMARY KEY, then whenever you insert a NULL
  into that column of the table, the NULL is automatically converted
  into an integer which is one greater than the largest value of that
  column over all other rows in the table, or 1 if the table is empty.
  For example, suppose you have a table like this:
<blockquote><pre>
CREATE TABLE t1(
  a INTEGER PRIMARY KEY,
  b INTEGER
);
</pre></blockquote>
  <p>With this table, the statement</p>
<blockquote><pre>
INSERT INTO t1 VALUES(NULL,123);
</pre></blockquote>
  <p>is logically equivalent to saying:</p>
<blockquote><pre>
INSERT INTO t1 VALUES((SELECT max(a) FROM t1)+1,123);
</pre></blockquote>
  <p>For SQLite version 2.2.0 through 2.3.3, if you insert a NULL into
  an INTEGER PRIMARY KEY column, the NULL will be changed to a unique
  integer, but it will a semi-random integer.  Unique keys generated this
  way will not be sequential.  For SQLite version 2.3.4 and beyond, the
  unique keys will be sequential until the largest key reaches a value
  of 2147483647.  That is the largest 32-bit signed integer and cannot
  be incremented, so subsequent insert attempts will revert to the
  semi-random key generation algorithm of SQLite version 2.3.3 and
  earlier.</p>

  <p>Beginning with version 2.2.3, there is a new API function named
  <b>sqlite_last_insert_rowid()</b> which will return the integer key
  for the most recent insert operation.  See the API documentation for
  details.</p>
}

faq {
  What datatypes does SQLite support?
} {
  <p>SQLite is typeless. All data is stored as null-terminated strings.
  The datatype information that follows the column name in CREATE TABLE
  statements is ignored (mostly).  You can put any type of data you want
  into any column, without regard to the declared datatype of that column.
  </p>

  <p>An exception to this rule is a column of type INTEGER PRIMARY KEY.
  Such columns must hold an integer.  An attempt to put a non-integer
  value into an INTEGER PRIMARY KEY column will generate an error.</p>
}

faq {
  SQLite lets me insert a string into a database column of type integer!
} {
  <p>This is a feature, not a bug.  SQLite is typeless.  Any data can be
  inserted into any column.  You can put arbitrary length strings into
  integer columns, floating point numbers in boolean columns, or dates
  in character columns.  The datatype you assign to a column in the
  CREATE TABLE command is (mostly) ignored.  Every column is able to hold
  an arbitrary length string.  (There is one exception: Columns of
  type INTEGER PRIMARY KEY may only hold an integer.  An error will result
  if you try to put anything other than an integer into an
  INTEGER PRIMARY KEY column.)</p>

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
  Why doesn't SQLite allow me to use '0' and '0.0' as the primary
  key on two different rows of the same table?
} {
  <p>Every row much have a unique primary key.
  But SQLite thinks that <b>'0'</b> and <b>'0.0'</b> are the
  same value because they compare equal to one another numerically.
  (See the previous question.)  Hence the values are not unique.</p>

  <p>You can work around this issue in two ways:</p>
  <ol>
  <li><p>Remove the <b>primary key</b> clause from the CREATE TABLE.</p></li>
  <li><p>Prepend a space to the beginning of every value you use for
      the primary key.  The initial
     space will mean that the entries are not pure numerics and hence
     will be compared as strings using <b>strcmp()</b>.</p></li>
  </ol>
}
        
faq {
  My linux box is not able to read an SQLite database that was created
  on my SparcStation.
} {
  <p>The x86 processor on your linux box is little-endian (meaning that
  the least significant byte of integers comes first) but the Sparc is
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

  <p>If two or more processes have the same database open and one
  process creates a new table or index, the other processes might
  not be able to see the new table right away.  You might have to
  get the other processes to close and reopen their connection to
  the database before they will be able to see the new table.</p>
}

faq {
  Is SQLite threadsafe?
} {
  <p>Yes.  Sometimes.  In order to be thread-safe, SQLite must be compiled
  with the THREADSAFE preprocessor macro set to 1.  In the default
  distribution, the windows binaries are compiled to be threadsafe but
  the linux binaries are not.  If you want to change this, you'll have to
  recompile.</p>

  <p>"Threadsafe" in the previous paragraph means that two or more threads
  can run SQLite at the same time on different "<b>sqlite</b>" structures
  returned from separate calls to <b>sqlite_open()</b>.  It is never safe
  to use the same <b>sqlite</b> structure pointer simultaneously in two
  or more threads.</p>

  <p>Note that if two or more threads have the same database open and one
  thread creates a new table or index, the other threads might
  not be able to see the new table right away.  You might have to
  get the other threads to close and reopen their connection to
  the database before they will be able to see the new table.</p>

  <p>Under UNIX, you should not carry an open SQLite database across
  a fork() system call into the child process.  Problems will result
  if you do.  Under LinuxThreads, because each thread has its own
  process ID, you may not start a transaction in one thread and attempt
  to complete it in another.</p>
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

  <p>Temporary tables do not appear in the SQLITE_MASTER table.  Temporary
  tables and their indices and triggers occur in another special table
  named SQLITE_TEMP_MASTER.  SQLITE_TEMP_MASTER works just like SQLITE_MASTER
  except that it is only visible to the application that created the 
  temporary tables.  To get a list of all tables, both permanent and
  temporary, one can use a command similar to the following:
</blockquote><pre>
SELECT name FROM 
   (SELECT * FROM sqlite_master UNION ALL
    SELECT * FROM sqlite_temp_master)
WHERE type='table'
ORDER BY name
</pre></blockquote>
}

faq {
  Are there any known size limits to SQLite databases?
} {
  <p>Internally, SQLite can handle databases up to 2^40 bytes (1 terabyte)
  in size.  But the backend interface to POSIX and Win32 limits files to
  2^31 (2 gigabytes).</p>

  <p>SQLite arbitrarily limits the amount of data in one row to 1 megabyte.
  There is a single #define in the source code that can be changed to raise
  this limit as high as 16 megabytes if desired.</p>

  <p>There is a theoretical limit of about 2^32 (4 billion) rows
  in a single table, but there
  is no way to test this limit without exceeding the maximum file size, so
  it is not really an issue.  There is also a theoretical limit of about 2^32
  tables and indices, but again it is not really possible to reach this
  limit due to the file size constraint.</p>

  <p>The name and "CREATE TABLE" statement for a table must fit entirely
  within a 1-megabyte row of the SQLITE_MASTER table.  Other than this,
  there are no constraints on the length of the name of a table, or on the
  number of columns, etc.  Indices are similarly unconstrained.</p>
}

faq {
  What is the maximum size of a VARCHAR in SQLite?
} {
  <p>Remember, SQLite is typeless.  A VARCHAR column can hold as much
  data as any other column.  The total amount of data in a single row
  of the database is limited to 1 megabyte.  You can increase this limit
  to 16 megabytes, if you need to, by adjusting a single #define in the
  source tree and recompiling.</p>

  <p>For maximum speed and space efficiency, you should try to keep the
  amount of data in a single row below about 230 bytes.</p>
}

faq {
  Does SQLite support a BLOB type?
} {
  <p>You can declare a table column to be of type "BLOB" but it will still
  only store null-terminated strings.  This is because the only way to 
  insert information into an SQLite database is using an INSERT SQL statement,
  and you can not include binary data in the middle of the ASCII text string
  of an INSERT statement.</p>

  <p>SQLite is 8-bit clean with regard to the data is stores as long as
  the data does not contain any NUL characters.  If you want to store binary
  data, consider encoding your data in such a way that it contains no NUL
  characters and inserting it that way.  You might use URL-style encoding:
  encode NUL as "%00" and "%" as "%25".  Or you might consider encoding your
  binary data using base-64.</p>
}

faq {
  How do I add or delete columns from an existing table in SQLite.
} {
  <p>SQLite does not support the "ALTER TABLE" SQL command.  If you
  what to change the structure of a table, you have to recreate the
  table.  You can save existing data to a temporary table, drop the
  old table, create the new table, then copy the data back in from
  the temporary table.</p>

  <p>For example, suppose you have a table named "t1" with columns
  names "a", "b", and "c" and that you want to delete column "c" from
  this table.  The following steps illustrate how this could be done:
  </p>

  <blockquote><pre>
BEGIN TRANSACTION;
CREATE TEMPORARY TABLE t1_backup(a,b);
INSERT INTO t1_backup SELECT a,b FROM t1;
DROP TABLE t1;
CREATE TABLE t1(a,b);
INSERT INTO t1 SELECT a,b FROM t1_backup;
DROP TABLE t1_backup;
COMMIT;
</pre></blockquote>
}

# End of questions and answers.
#############

puts {<DL COMPACT>}
for {set i 1} {$i<$cnt} {incr i} {
  puts "  <DT><A HREF=\"#q$i\">($i)</A></DT>"
  puts "  <DD>[lindex $faq($i) 0]</DD>"
}
puts {</DL>}

for {set i 1} {$i<$cnt} {incr i} {
  puts "<A NAME=\"q$i\"><HR />"
  puts "<P><B>($i) [lindex $faq($i) 0]</B></P>\n"
  puts "<BLOCKQUOTE>[lindex $faq($i) 1]</BLOCKQUOTE></LI>\n"
}

puts {
</OL>
<p><hr /></p>
<p><a href="index.html"><img src="/goback.jpg" border=0 />
Back to the SQLite Home Page</a>
</p>

</body></html>}

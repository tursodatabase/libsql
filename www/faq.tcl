#
# Run this script to generated a faq.html output file
#
set rcsid {$Id: faq.tcl,v 1.28 2005/01/26 10:39:58 danielk1977 Exp $}
source common.tcl
header {SQLite Frequently Asked Questions</title>}

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

  <p>SQLite version 3.0 expands the size of the rowid to 64 bits.</p>
}

faq {
  What datatypes does SQLite support?
} {
  <p>SQLite ignores
  the datatype information that follows the column name in CREATE TABLE.
  You can put any type of data you want
  into any column, without regard to the declared datatype of that column.
  </p>

  <p>An exception to this rule is a column of type INTEGER PRIMARY KEY.
  Such columns must hold an integer.  An attempt to put a non-integer
  value into an INTEGER PRIMARY KEY column will generate an error.</p>

  <p>There is a page on <a href="datatypes.html">datatypes in SQLite
  version 2.8</a>
  and another for <a href="datatype3.html">version 3.0</a>
  that explains this concept further.</p>
}

faq {
  SQLite lets me insert a string into a database column of type integer!
} {
  <p>This is a feature, not a bug.  SQLite does not enforce data type
  constraints.  Any data can be
  inserted into any column.  You can put arbitrary length strings into
  integer columns, floating point numbers in boolean columns, or dates
  in character columns.  The datatype you assign to a column in the
  CREATE TABLE command does not restrict what data can be put into
  that column.  Every column is able to hold
  an arbitrary length string.  (There is one exception: Columns of
  type INTEGER PRIMARY KEY may only hold an integer.  An error will result
  if you try to put anything other than an integer into an
  INTEGER PRIMARY KEY column.)</p>

  <p>The datatype does effect how values are compared, however.  For
  columns with a numeric type (such as "integer") any string that looks
  like a number is treated as a number for comparison and sorting purposes.
  Consider these two command sequences:</p>

  <blockquote><pre>
CREATE TABLE t1(a INTEGER UNIQUE);        CREATE TABLE t2(b TEXT UNIQUE);
INSERT INTO t1 VALUES('0');               INSERT INTO t2 VALUES(0);
INSERT INTO t1 VALUES('0.0');             INSERT INTO t2 VALUES(0.0);
</pre></blockquote>

  <p>In the sequence on the left, the second insert will fail.  In this case,
  the strings '0' and '0.0' are treated as numbers since they are being 
  inserted into a numeric column and 0==0.0 which violates the uniqueness
  constraint.  But the second insert in the right-hand sequence works.  In
  this case, the constants 0 and 0.0 are treated a strings which means that
  they are distinct.</p>

  <p>There is a page on <a href="datatypes.html">datatypes in SQLite
  version 2.8</a>
  and another for <a href="datatype3.html">version 3.0</a>
  that explains this concept further.</p>
}

faq {
  Why does SQLite think that the expression '0'=='00' is TRUE?
} {
  <p>As of version 2.7.0, it doesn't.</p>

  <p>But if one of the two values being compared is stored in a column that
  has a numeric type, the the other value is treated as a number, not a
  string and the result succeeds.  For example:</p>

<blockquote><pre>
CREATE TABLE t3(a INTEGER, b TEXT);
INSERT INTO t3 VALUES(0,0);
SELECT count(*) FROM t3 WHERE a=='00';
</pre></blockquote>

  <p>The SELECT in the above series of commands returns 1.  The "a" column
  is numeric so in the WHERE clause the string '00' is converted into a
  number for comparison against "a".  0==00 so the test is true.  Now
  consider a different SELECT:</p>

<blockquote><pre>
SELECT count(*) FROM t3 WHERE b=='00';
</pre></blockquote>

  <p>In this case the answer is 0.  B is a text column so a text comparison
  is done against '00'.  '0'!='00' so the WHERE clause returns FALSE and
  the count is zero.</p>

  <p>There is a page on <a href="datatypes.html">datatypes in SQLite
  version 2.8</a>
  and another for <a href="datatype3.html">version 3.0</a>
  that explains this concept further.</p>
}

faq {
  Why doesn't SQLite allow me to use '0' and '0.0' as the primary
  key on two different rows of the same table?
} {
  <p>Your primary key must have a numeric type.  Change the datatype of
  your primary key to TEXT and it should work.</p>

  <p>Every row must have a unique primary key.  For a column with a
  numeric type, SQLite thinks that <b>'0'</b> and <b>'0.0'</b> are the
  same value because they compare equal to one another numerically.
  (See the previous question.)  Hence the values are not unique.</p>
}
        
faq {
  My linux box is not able to read an SQLite database that was created
  on my SparcStation.
} {
  <p>You need to upgrade your SQLite library to version 2.6.3 or later.</p>

  <p>The x86 processor on your linux box is little-endian (meaning that
  the least significant byte of integers comes first) but the Sparc is
  big-endian (the most significant bytes comes first).  SQLite databases
  created on a little-endian architecture cannot be on a big-endian
  machine by version 2.6.2 or earlier of SQLite.  Beginning with
  version 2.6.3, SQLite should be able to read and write database files
  regardless of byte order of the machine on which the file was created.</p>
}

faq {
  Can multiple applications or multiple instances of the same
  application access a single database file at the same time?
} {
  <p>Multiple processes can have the same database open at the same
  time.  Multiple processes can be doing a SELECT
  at the same time.  But only one process can be making changes to
  the database at once.</p>

  <p>Win95/98/ME lacks support for reader/writer locks in the operating
  system.  Prior to version 2.7.0, this meant that under windows you
  could only have a single process reading the database at one time.
  This problem was resolved in version 2.7.0 by implementing a user-space
  probabilistic reader/writer locking strategy in the windows interface
  code file.  Windows
  now works like Unix in allowing multiple simultaneous readers.</p>

  <p>The locking mechanism used to control simultaneous access might
  not work correctly if the database file is kept on an NFS filesystem.
  This is because file locking is broken on some NFS implementations.
  You should avoid putting SQLite database files on NFS if multiple
  processes might try to access the file at the same time.  On Windows,
  Microsoft's documentation says that locking may not work under FAT
  filesystems if you are not running the Share.exe daemon.  People who
  have a lot of experience with Windows tell me that file locking of
  network files is very buggy and is not dependable.  If what they
  say is true, sharing an SQLite database between two or more Windows
  machines might cause unexpected problems.</p>

  <p>Locking in SQLite is very course-grained.  SQLite locks the
  entire database.  Big database servers (PostgreSQL, Oracle, etc.)
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
  if you do.</p>
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
<blockquote><pre>
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
  <p>As of version 2.7.4, 
  SQLite can handle databases up to 2<sup>41</sup> bytes (2 terabytes)
  in size on both Windows and Unix.  Older version of SQLite
  were limited to databases of 2<sup>31</sup> bytes (2 gigabytes).</p>

  <p>SQLite version 2.8 limits the amount of data in one row to 
  1 megabyte.  SQLite version 3.0 has no limit on the amount of
  data that can be stored in a single row.
  </p>

  <p>The names of tables, indices, view, triggers, and columns can be
  as long as desired.  However, the names of SQL functions (as created
  by the <a href="c_interface.html#cfunc">sqlite_create_function()</a> API)
  may not exceed 255 characters in length.</p>
}

faq {
  What is the maximum size of a VARCHAR in SQLite?
} {
  <p>SQLite does not enforce datatype constraints.
  A VARCHAR column can hold as much data as you care to put in it.</p>
}

faq {
  Does SQLite support a BLOB type?
} {
  <p>SQLite version 3.0 lets you puts BLOB data into any column, even
  columns that are declared to hold some other type.</p>

  <p>SQLite version 2.8 will store any text data without embedded
  '\000' characters.  If you need to store BLOB data in SQLite version
  2.8 you'll want to encode that data first.
  There is a source file named 
  "<b>src/encode.c</b>" in the SQLite version 2.8 distribution that contains
  implementations of functions named "<b>sqlite_encode_binary()</b>
  and <b>sqlite_decode_binary()</b> that can be used for converting
  binary data to ASCII and back again, if you like.</p>

 
}

faq {
  How do I add or delete columns from an existing table in SQLite.
} {
  <p>SQLite does yes not support the "ALTER TABLE" SQL command.  If you
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

faq {
  I deleted a lot of data but the database file did not get any
  smaller.  Is this a bug?
} {
  <p>No.  When you delete information from an SQLite database, the
  unused disk space is added to an internal "free-list" and is reused
  the next time you insert data.  The disk space is not lost.  But
  neither is it returned to the operating system.</p>

  <p>If you delete a lot of data and want to shrink the database file,
  run the VACUUM command (version 2.8.1 and later).  VACUUM will reconstruct
  the database from scratch.  This will leave the database with an empty
  free-list and a file that is minimal in size.  Note, however, that the
  VACUUM can take some time to run (around a half second per megabyte
  on the Linux box where SQLite is developed) and it can use up to twice
  as much temporary disk space as the original file while it is running.
  </p>

  <p>As of SQLite version 3.1, an alternative to using the VACUUM command
  is auto-vacuum mode, enabled using the 
  <a href="pragma.html#pragma_auto_vacuum">auto_vacuum pragma</a>.</p>
}

faq {
  Can I use SQLite in my commercial product without paying royalties?
} {
  <p>Yes.  SQLite is in the public domain.  No claim of ownership is made
  to any part of the code.  You can do anything you want with it.</p>
}

faq {
  How do I use a string literal that contains an embedded single-quote (')
  character?
} {
  <p>The SQL standard specifies that single-quotes in strings are escaped
  by putting two single quotes in a row.  SQL works like the Pascal programming
  language in the regard.  SQLite follows this standard.  Example:
  </p>

  <blockquote><pre>
    INSERT INTO xyz VALUES('5 O''clock');
  </pre></blockquote>
}

faq {What is an SQLITE_SCHEMA error, and why am I getting one?} {
  <p>In version 3 of SQLite, an SQLITE_SCHEMA error is returned when a 
  prepared SQL statement is no longer valid and cannot be executed.
  When this occurs, the statement must be recompiled from SQL using 
  the sqlite3_prepare() API. In SQLite 3, an SQLITE_SCHEMA error can
  only occur when using the sqlite3_prepare()/sqlite3_step()/sqlite3_finalize()
  API to execute SQL, not when using the sqlite3_exec(). This was not
  the case in version 2.</p>

  <p>The most common reason for a prepared statement to become invalid
  is that the schema of the database was modified after the SQL was 
  prepared (possibly by another process).  The other reasons this can 
  happen are:</p> 
  <ul>
  <li>A database was DETACHed.
  <li>A user-function definition was deleted or changed.
  <li>A collation sequence definition was deleted or changed.
  <li>The authorization function was changed.
  </ul>

  <p>In all cases, the solution is to recompile the statement from SQL
  and attempt to execute it again. Because a prepared statement can be
  invalidated by another process changing the database schema, all code
  that uses the sqlite3_prepare()/sqlite3_step()/sqlite3_finalize()
  API should be prepared to handle SQLITE_SCHEMA errors. An example
  of one approach to this follows:</p>

  <blockquote><pre>

    int rc;
    sqlite3_stmt *pStmt;
    char zSql[] = "SELECT .....";

    do {
      /* Compile the statement from SQL. Assume success. */
      sqlite3_prepare(pDb, zSql, -1, &pStmt, 0);

      while( SQLITE_ROW==sqlite3_step(pStmt) ){
        /* Do something with the row of available data */
      }

      /* Finalize the statement. If an SQLITE_SCHEMA error has
      ** occured, then the above call to sqlite3_step() will have
      ** returned SQLITE_ERROR. sqlite3_finalize() will return
      ** SQLITE_SCHEMA. In this case the loop will execute again.
      */
      rc = sqlite3_finalize(pStmt);
    } while( rc==SQLITE_SCHEMA );
    
  </pre></blockquote>
}

# End of questions and answers.
#############

puts {<h2>Frequently Asked Questions</h2>}

# puts {<DL COMPACT>}
# for {set i 1} {$i<$cnt} {incr i} {
#   puts "  <DT><A HREF=\"#q$i\">($i)</A></DT>"
#   puts "  <DD>[lindex $faq($i) 0]</DD>"
# }
# puts {</DL>}
puts {<OL>}
for {set i 1} {$i<$cnt} {incr i} {
  puts "<li><a href=\"#q$i\">[lindex $faq($i) 0]</a></li>"
}
puts {</OL>}

for {set i 1} {$i<$cnt} {incr i} {
  puts "<A NAME=\"q$i\"><HR />"
  puts "<P><B>($i) [lindex $faq($i) 0]</B></P>\n"
  puts "<BLOCKQUOTE>[lindex $faq($i) 1]</BLOCKQUOTE></LI>\n"
}

puts {</OL>}
footer $rcsid

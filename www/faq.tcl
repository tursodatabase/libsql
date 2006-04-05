#
# Run this script to generated a faq.html output file
#
set rcsid {$Id: faq.tcl,v 1.36 2006/04/05 01:02:08 drh Exp $}
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
  If you declare a column of a table to be INTEGER PRIMARY KEY, then
  whenever you insert a NULL
  into that column of the table, the NULL is automatically converted
  into an integer which is one greater than the largest value of that
  column over all other rows in the table, or 1 if the table is empty.
  (If the largest possible integer key, 9223372036854775807, then an
  unused key value is chosen at random.)
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

  <p>There is a new API function named
  <a href="capi3ref.html#sqlite3_last_insert_rowid">
  sqlite3_last_insert_rowid()</a> which will return the integer key
  for the most recent insert operation.</p>

  <p>Note that the integer key is one greater than the largest
  key that was in the table just prior to the insert.  The new key
  will be unique over all keys currently in the table, but it might
  overlap with keys that have been previously deleted from the
  table.  To create keys that are unique over the lifetime of the
  table, add the AUTOINCREMENT keyword to the INTEGER PRIMARY KEY
  declaration.  Then the key chosen will be one more than than the
  largest key that has ever existed in that table.  If the largest
  possible key has previously existed in that table, then the INSERT
  will fail with an SQLITE_FULL error code.</p>
}

faq {
  What datatypes does SQLite support?
} {
  <p>See <a href="datatype3.html">http://www.sqlite.org/datatype3.html</a>.</p>
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
  type INTEGER PRIMARY KEY may only hold a 64-bit signed integer.
  An error will result
  if you try to put anything other than an integer into an
  INTEGER PRIMARY KEY column.)</p>

  <p>But SQLite does use the declared type of a column as a hint
  that you prefer values in that format.  So, for example, if a
  column is of type INTEGER and you try to insert a string into
  that column, SQLite will attempt to convert the string into an
  integer.  If it can, it inserts the integer instead.  If not,
  it inserts the string.  This feature is sometimes
  call <a href="datatype3.html#affinity">type or column affinity</a>.
  </p>
}

faq {
  Why does SQLite think that the expression '0'=='00' is TRUE?
} {
  <p>As of version 2.7.0, it doesn't.  See the document on
  <a href="datatype3.html">datatypes in SQLite version 3</a>
  for details.</p>
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
  the database at any moment in time, however.</p>

  <p>SQLite uses reader/writer locks to control access to the database.
  (Under Win95/98/ME which lacks support for reader/writer locks, a
  probabilistic simulation is used instead.)
  But use caution: this locking mechanism might
  not work correctly if the database file is kept on an NFS filesystem.
  This is because fcntl() file locking is broken on many NFS implementations.
  You should avoid putting SQLite database files on NFS if multiple
  processes might try to access the file at the same time.  On Windows,
  Microsoft's documentation says that locking may not work under FAT
  filesystems if you are not running the Share.exe daemon.  People who
  have a lot of experience with Windows tell me that file locking of
  network files is very buggy and is not dependable.  If what they
  say is true, sharing an SQLite database between two or more Windows
  machines might cause unexpected problems.</p>

  <p>We are aware of no other <i>embedded</i> SQL database engine that
  supports as much concurrancy as SQLite.  SQLite allows multiple processes
  to have the database file open at once, and for multiple processes to
  read the database at once.  When any process wants to write, it must
  lock the entire database file for the duration of its update.  But that
  normally only takes a few milliseconds.  Other processes just wait on
  the writer to finish then continue about their business.  Other embedded
  SQL database engines typically only allow a single process to connect to
  the database at once.</p>

  <p>However, client/server database engines (such as PostgreSQL, MySQL,
  or Oracle) usually support a higher level of concurrency and allow
  multiple processes to be writing to the same database at the same time.
  This is possible in a client/server database because there is always a
  single well-controlled server process available to coordinate access.
  If your application has a need for a lot of concurrency, then you should
  consider using a client/server database.  But experience suggests that
  most applications need much less concurrency than their designers imagine.
  </p>

  <p>When SQLite tries to access a file that is locked by another
  process, the default behavior is to return SQLITE_BUSY.  You can
  adjust this behavior from C code using the 
  <a href="capi3ref#sqlite3_busy_handler">sqlite3_busy_handler()</a> or
  <a href="capi3ref#sqlite3_busy_timeout">sqlite3_busy_timeout()</a>
  API functions.</p>
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
  can run SQLite at the same time on different "<b>sqlite3</b>" structures
  returned from separate calls to 
  <a href="capi3ref#sqlite3_open">sqlite3_open()</a>.  It is never safe
  to use the same <b>sqlite3</b> structure pointer in two
  or more threads.</p>

  <p>Prior to version 3.3.1,
  an <b>sqlite3</b> structure could only be used in the same thread
  that called <a href="capi3ref#sqlite3_open">sqlite3_open</a> to create it.
  You could not open a
  database in one thread then pass the handle off to another thread for
  it to use.  This was due to limitations (bugs?) in many common threading
  implementations such as on RedHat9.  Specifically, an fcntl() lock
  created by one thread cannot be removed or modified by a different
  thread on the troublesome systems.  And since SQLite uses fcntl()
  locks heavily for concurrency control, serious problems arose if you 
  start moving database connections across threads.</p>

  <p>The restriction on moving database connections across threads
  was relaxed somewhat in version 3.3.1.  With that and subsequent
  versions, it is safe to move a connection handle across threads
  as long as the connection is not holding any fcntl() locks.  You
  can safely assume that no locks are being held if no
  transaction is pending and all statements have been finalized.</p>

  <p>Under UNIX, you should not carry an open SQLite database across
  a fork() system call into the child process.  Problems will result
  if you do.</p>
}

faq {
  How do I list all tables/indices contained in an SQLite database
} {
  <p>If you are running the <b>sqlite3</b> command-line access program
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
  <p>A database is limited in size to 2 tibibytes (2<sup>41</sup> bytes).
  That is a theoretical limitation.  In practice, you should try to keep
  your SQLite databases below 100 gigabytes to avoid performance problems.
  If you need to store 100 gigabytes or more in a database, consider using
  an enterprise database engine which is designed for that purpose.</p>

  <p>The theoretical limit on the number of rows in a table is
  2<sup>64</sup>-1, though obviously you will run into the file size
  limitation prior to reaching the row limit.  A single row can hold
  up to 2<sup>30</sup> bytes of data in the current implementation.  The
  underlying file format supports row sizes up to about 2<sup>62</sup> bytes.
  </p>

  <p>There are probably limits on the number of tables or indices or
  the number of columns in a table or index, but nobody is sure what
  those limits are.  In practice, SQLite must read and parse the original
  SQL of all table and index declarations everytime a new database file
  is opened, so for the best performance of
  <a href="capi3ref.html#sqlite3_open">sqlite3_open()</a> it is best
  to keep down the number of declared tables.   Likewise, though there
  is no limit on the number of columns in a table, more than a few hundred
  seems extreme.  Only the first 31 columns of a table are candidates for
  certain optimizations.  You can put as many columns in an index as you like
  but indexes with more than 30 columns will not be used to optimize queries.
  </p>

  <p>The names of tables, indices, view, triggers, and columns can be
  as long as desired.  However, the names of SQL functions (as created
  by the 
  <a href="capi3ref.html#sqlite3_create_function">sqlite3_create_function()</a>
  API) may not exceed 255 characters in length.</p>
}

faq {
  What is the maximum size of a VARCHAR in SQLite?
} {
  <p>SQLite does not enforce the length of a VARCHAR.  You can declare
  a VARCHAR(10) and SQLite will be happy to let you put 500 characters
  in it.  And it will keep all 500 characters intact - it never truncates.
  </p>
}

faq {
  Does SQLite support a BLOB type?
} {
  <p>SQLite versions 3.0 and later allow you to store BLOB data in any 
  column, even columns that are declared to hold some other type.</p>
}

faq {
  How do I add or delete columns from an existing table in SQLite.
} {
  <p>SQLite has limited 
  <a href="lang_altertable.html">ALTER TABLE</a> support that you can
  use to add a column to the end of a table or to change the name of
  a table.  
  If you what make more complex changes the structure of a table,
  you will have to recreate the
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
  run the <a href="lang_vacuum.html">VACUUM</a> command.
  VACUUM will reconstruct
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
  <p>Yes.  SQLite is in the 
  <a href="copyright.html">public domain</a>.  No claim of ownership is made
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
  <p>An SQLITE_SCHEMA error is returned when a 
  prepared SQL statement is no longer valid and cannot be executed.
  When this occurs, the statement must be recompiled from SQL using 
  the 
  <a href="capi3ref.html#sqlite3_prepare">sqlite3_prepare()</a> API.
  In SQLite version 3, an SQLITE_SCHEMA error can
  only occur when using the 
  <a href="capi3ref.html#sqlite3_prepare">sqlite3_prepare()</a>/<a
  href="capi3ref.html#sqlite3_step">sqlite3_step()</a>/<a
  href="capi3ref.html#sqlite3_finalize">sqlite3_finalize()</a>
  API to execute SQL, not when using the
  <a href="capi3ref.html#sqlite3_exec">sqlite3_exec()</a>. This was not
  the case in version 2.</p>

  <p>The most common reason for a prepared statement to become invalid
  is that the schema of the database was modified after the SQL was 
  prepared (possibly by another process).  The other reasons this can 
  happen are:</p> 
  <ul>
  <li>A database was <a href="lang_detach.html">DETACH</a>ed.
  <li>The database was <a href="lang_vacuum.html">VACUUM</a>ed
  <li>A user-function definition was deleted or changed.
  <li>A collation sequence definition was deleted or changed.
  <li>The authorization function was changed.
  </ul>

  <p>In all cases, the solution is to recompile the statement from SQL
  and attempt to execute it again. Because a prepared statement can be
  invalidated by another process changing the database schema, all code
  that uses the
  <a href="capi3ref.html#sqlite3_prepare">sqlite3_prepare()</a>/<a
  href="capi3ref.html#sqlite3_step">sqlite3_step()</a>/<a
  href="capi3ref.html#sqlite3_finalize">sqlite3_finalize()</a>
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

faq {Why does ROUND(9.95,1)  return 9.9 instead of 10.0?
     Shouldn't 9.95 round up?} {
  <p>SQLite uses binary arithmetic and in binary, there is no
  way to write 9.95 in a finite number of bits.  The closest to
  you can get to 9.95 in a 64-bit IEEE float (which is what
  SQLite uses) is 9.949999999999999289457264239899814128875732421875.
  So when you type "9.95", SQLite really understands the number to be
  the much longer value shown above.  And that value rounds down.</p>

  <p>This kind of problem comes up all the time when dealing with
  floating point binary numbers.  The general rule to remember is
  that most fractional numbers that have a finite representation in decimal
  (a.k.a "base-10")
  do not have a finite representation in binary (a.k.a "base-2").
  And so they are
  approximated using the closest binary number available.  That
  approximation is usually very close, but it will be slightly off
  and in some cases can cause your results to be a little different
  from what you might expect.</p>
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

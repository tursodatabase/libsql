set rcsid {$Id: different.tcl,v 1.5 2005/04/01 16:29:12 drh Exp $}
source common.tcl
header {Distinctive Features Of SQLite}
puts {
<p>
This page highlights some of the characteristics of SQLite that are
unusual and which make SQLite different from many other SQL
database engines.
</p>
}
proc feature {tag name text} {
  puts "<a name=\"$tag\" />"
  puts "<p><b>$name</b></p>\n"
  puts "<blockquote>$text</blockquote>\n"
}

feature zeroconfig {Zero-Configuration} {
  SQLite does not need to be "installed" before it is used. 
  There is no "setup" procedure.  There is no
  server process that needs to be started, stopped, or configured.
  There is
  no need for an administrator to create a new database instance or assign
  access permissions to users.
  SQLite uses no configuration files.
  Nothing needs to be done to tell the system that SQLite is running.
  No actions are required to recover after a system crash or power failure.
  There is nothing to troubleshoot.
  <p>
  SQLite just works.
  <p>
  Other more familiar database engines run great once you get them going.
  But doing the initial installation and configuration can be
  intimidatingly complex.
}

feature serverless {Serverless} {
  Most SQL database engines are implemented as a separate server
  process.  Programs that want to access the database communicate
  with the server using some kind of interprocess communcation
  (typically TCP/IP) to send requests to the server and to receive
  back results.  SQLite does not work this way.  With SQLite, the
  process that wants to access the database reads and writes
  directly from the database files on disk.  There is no intermediary
  server process.
  <p>
  There are advantages and disadvantages to being serverless.  The
  main advantage is that there is no separate server process
  to install, setup, configure, initialize, manage, and troubleshoot.
  This is one reason why SQLite is a "zero-configuration" database
  engine.  Programs that use SQLite require no administrative support
  for setting up the database engine before they are run.  Any program
  that is able to access the disk is able to use an SQLite database.
  <p>
  On the other hand, a database engine that uses a server can provide
  better protection from bugs in the client application - stray pointers
  in a client cannot corrupt memory on the server.  And because a server
  is a single persistent process, it is able control database access with
  more precision, allowing for finer grain locking and better concurrancy.
  <p>
  Most SQL database engines are client/server based.  Of those that are
  serverless, SQLite is the only one that this author knows of that
  allows multiple applications to access the same database at the same time.
}

feature onefile {Single Database File} {
  An SQLite database is a single ordinary disk file that can be located
  anywhere in the directory hierarchy.  If SQLite can read
  the disk file then it can read anything in the database.  If the disk
  file and its directory are writable, then SQLite can change anything
  in the database.   Database files can easily be copied onto a USB
  memory stick or emailed for sharing.
  <p>
  Other SQL database engines tend to store data as a large collection of
  files.  Often these files are in a standard location that only the
  database engine itself can access.  This makes the data more secure,
  but also makes it harder to access.  Some SQL database engines provide
  the option of writing directly to disk and bypassing the filesystem
  all together.  This provides added performance, but at the cost of
  considerable setup and maintenance complexity.
}

feature small {Compact} {
  When optimized for size, the whole SQLite library with everything enabled
  is less than 225KiB in size (as measured on an ix86 using the "size"
  utility from the GNU compiler suite.)  Unneeded features can be disabled
  at compile-time to further reduce the size of the library to under
  170KiB if desired.
  <p>
  Most other SQL database engines are much larger than this.  IBM boasts
  that it's recently released CloudScape database engine is "only" a 2MiB
  jar file - 10 times larger than SQLite even after it is compressed!
  Firefox boasts that it's client-side library is only 350KiB.  That's
  50% larger than SQLite and does not even contain the database engine.
  The Berkeley DB library from Sleepycat is 450KiB and it omits SQL
  support, providing the programmer with only simple key/value pairs.
}

feature typing {Manifest typing} {
  Most SQL database engines use static typing.  A datatype is associated
  with each column in a table and only values of that particular datatype
  are allowed to be stored in that column.  SQLite relaxes this restriction
  by using manifest typing.
  In manifest typing, the datatype is a property of the value itself, not 
  of the column in which the value is stored.
  SQLite thus allows the user to store
  any value of any datatype into any column regardless of the declared type
  of that column.  (There are some exceptions to this rule: An INTEGER
  PRIMARY KEY column may only store integers.  And SQLite attempts to coerce
  values into the declared datatype of the column when it can.)
  <p>
  The SQL language specification calls for static typing.  So some people
  feel that the use of manifest typing is a bug in SQLite.  But the authors
  of SQLite feel very strongly that this is a feature.  The authors argue
  that static typing is a bug in the SQL specification that SQLite has fixed
  in a backwards compatible way.
}

feature flex {Variable-length records} {
  Most other SQL database engines allocated a fixed amount of disk space
  for each row in most tables.  They play special tricks for handling
  BLOBs and CLOBs which can be of wildly varying length.  But for most
  tables, if you declare a column to be a VARCHAR(100) then the database
  engine will allocate
  100 bytes of disk space regardless of how much information you actually
  store in that column.
  <p>
  SQLite, in contrast, use only the amount of disk space actually
  needed to store the information in a row.  If you store a single
  character in a VARCHAR(100) column, then only a single byte of disk
  space is consumed.  (Actually two bytes - there is some overhead at
  the beginning of each column to record its datatype and length.)
  <p>
  The use of variable-length records by SQLite has a number of advantages.
  It results in smaller database files, obviously.  It also makes the
  database run faster, since there is less information to move to and from
  disk.  And, the use of variable-length records makes it possible for
  SQLite to employ manifest typing instead of static typing.
}

feature readable {Readable source code} {
  The source code to SQLite is designed to be readable and accessible to
  the average programmer.  All procedures and data structures and many
  automatic variables are carefully commented with useful information about
  what they do.  Boilerplate commenting is omitted.
}

feature vdbe {SQL statements compile into virtual machine code} {
  Every SQL database engine compiles each SQL statement into some kind of
  internal data structure which is then used to carry out the work of the
  statement.  But in most SQL engines that internal data structure is a
  complex web of interlinked structures and objects.  In SQLite, the compiled
  form of statements is a short program in a machine-language like
  representation.  Users of the database can view this 
  <a href="opcode.html">virtual machine language</a>
  by prepending the <a href="lang_explain.html">EXPLAIN</a> keyword
  to a query.
  <p>
  The use of a virtual machine in SQLite has been a great benefit to
  library's development.  The virtual machine provides a crisp, well-defined
  junction between the front-end of SQLite (the part that parses SQL
  statements and generates virtual machine code) and the back-end (the
  part that executes the virtual machine code and computes a result.)
  The virtual machine allows the developers to see clearly and in an
  easily readable form what SQLite is trying to do with each statement
  it compiles, which is a tremendous help in debugging.
  Depending on how it is compiled, SQLite also has the capability of
  tracing the execution of the virtual machine - printing each
  virtual machine instruction and its result as it executes.
}

#feature binding {Tight bindings to dynamic languages} {
#  Because it is embedded, SQLite can have a much tighter and more natural
#  binding to high-level dynamic languages such as Tcl, Perl, Python,
#  PHP, and Ruby.
#  For example, 
#}

feature license {Public domain} {
  The source code for SQLite is in the public domain.  No claim of copyright
  is made on any part of the core source code.  (The documentation and test
  code is a different matter - some sections of documentation and test logic
  are governed by open-sources licenses.)  All contributors to the
  SQLite core software have signed affidavits specifically disavowing any
  copyright interest in the code.  This means that anybody is able to legally
  do anything they want with the SQLite source code.
  <p>
  There are other SQL database engines with liberal licenses that allow
  the code to be broadly and freely used.  But those other engines are
  still governed by copyright law.  SQLite is different in that copyright
  law simply does not apply.  
  <p>
  The source code files for other SQL database engines typically begin
  with a comment describing your license rights to view and copy that file.
  The SQLite source code contains no license since it is not governed by
  copyright.  Instead of a license, the SQLite source code offers a blessing:
  <blockquote>
  <i>May you do good and not evil<br>
  May you find forgiveness for yourself and forgive others<br>
  May you share freely, never taking more than you give.</i>
  </blockquote>
}

feature extensions {SQL language extensions} {
  SQLite provides a number of enhancements to the SQL language 
  not normally found in other database engines.
  The EXPLAIN keyword and manifest typing have already been mentioned
  above.  SQLite also provides statements such as 
  <a href="lang_replace.html">REPLACE</a> and the
  <a href="lang_conflict.html">ON CONFLICT</a> clause that allow for
  added control over the resolution of constraint conflicts.
  SQLite supports <a href="lang_attach.html">ATTACH</a> and
  <a href="lang_detach.html">DETACH</a> commands that allow multiple
  independent databases to be used together in the same query.
  And SQLite defines APIs that allows the user to add new
  <a href="capi3ref.html#sqlite3_create_function>SQL functions</a>
  and <a href="capi3ref.html#sqlite3_create_collation>collating sequences</a>.
}


footer $rcsid

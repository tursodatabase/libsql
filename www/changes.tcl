#
# Run this script to generated a changes.html output file
#
puts {<html>
<head>
  <title>SQLite Change Log</title>
</head>
<body bgcolor="white">
<h1 align="center">Recent Changes To SQLite</h1>

<DL>
}


proc chng {date desc} {
  puts "<DT><B>$date</B></DT>"
  puts "<DD><P><UL>$desc</UL></P></DD>"
}

chng {2001 Sep 27 (2.0-alpha-3)} {
<li>SQLite now honors the UNIQUE keyword in CREATE UNIQUE INDEX.  Primary
    keys are required to be unique.</li>
<li>File format changed back to what it was for alpha-1</li>
<li>Fixes to the rollback and locking behavior</li>
}

chng {2001 Sep 20 (2.0-alpha-2)} {
<li>Initial release of version 2.0.  The idea of renaming the library
    to "SQLus" was abandoned in favor of keeping the "SQLite" name and
    bumping the major version number.</li>
<li>The pager and btree subsystems added back. They are now the only
    available backend.</li>
<li>The Dbbe abstraction and the GDBM and memory drivers were removed.</li>
<li>Copyright on all code was disclaimed.  The library is now in the
    public domain.</li>
}

chng {2001 Jul 23 (1.0.32)} {
<li>Pager and btree subsystems removed.  These will be used in a follow-on
    SQL server library named "SQLus".</li>
<li>Add the ability to use quoted strings as table and column names in
    expressions.</li>
}

chng {2001 Apr 14 (1.0.31)} {
<li>Pager subsystem added but not yet used.</li>
<li>More robust handling of out-of-memory errors.</li>
<li>New tests added to the test suite.</li>
}

chng {2001 Apr 6 (1.0.30)} {
<li>Remove the <b>sqlite_encoding</b> TCL variable that was introduced
    in the previous version.</li>
<li>Add options <b>-encoding</b> and <b>-tcl-uses-utf</b> to the
    <b>sqlite</b> TCL command.</li>
<li>Add tests to make sure that tclsqlite was compiled using Tcl header
    files and libraries that match.</li>
}

chng {2001 Apr 5 (1.0.29)} {
<li>The library now assumes data is stored as UTF-8 if the --enable-utf8
    option is given to configure.  The default behavior is to assume
    iso8859-x, as it has always done.  This only makes a difference for
    LIKE and GLOB operators and the LENGTH and SUBSTR functions.</li>
<li>If the library is not configured for UTF-8 and the Tcl library
    is one of the newer ones that uses UTF-8 internally,
    then a conversion from UTF-8 to iso8859 and
    back again is done inside the TCL interface.</li>
}

chng {2001 Apr 4 (1.0.28)} {
<li>Added limited support for transactions.  At this point, transactions
    will do table locking on the GDBM backend.  There is no support (yet)
    for rollback or atomic commit.</li>
<li>Added special column names ROWID, OID, and _ROWID_ that refer to the
    unique random integer key associated with every row of every table.</li>
<li>Additional tests added to the regression suite to cover the new ROWID
    feature and the TCL interface bugs mentioned below.</li>
<li>Changes to the "lemon" parser generator to help it work better when
    compiled using MSVC.</li>
<li>Bug fixes in the TCL interface identified by Oleg Oleinick.</li>
}

chng {2001 Mar 20 (1.0.27)} {
<li>When doing DELETE and UPDATE, the library used to write the record
    numbers of records to be deleted or updated into a temporary file.
    This is changed so that the record numbers are held in memory.</li>
<li>The DELETE command without a WHILE clause just removes the database
    files from the disk, rather than going through and deleting record
    by record.</li>
}

chng {2001 Mar 20 (1.0.26)} {
<li>A serious bug fixed on Windows.  Windows users should upgrade.
    No impact to Unix.</li>
}

chng {2001 Mar 15 (1.0.25)} {
<li>Modify the test scripts to identify tests that depend on system
    load and processor speed and
    to warn the user that a failure of one of those (rare) tests does
    not necessarily mean the library is malfunctioning.  No changes to
    code.
    </li>
}

chng {2001 Mar 14 (1.0.24)} {
<li>Fix a bug which was causing
    the UPDATE command to fail on systems where "malloc(0)" returns
    NULL.  The problem does not appear Windows, Linux, or HPUX but does 
    cause the library to fail on QNX.
    </li>
}

chng {2001 Feb 19 (1.0.23)} {
<li>An unrelated (and minor) bug from Mark Muranwski fixed.  The algorithm
    for figuring out where to put temporary files for a "memory:" database
    was not working quite right.
    </li>
}

chng {2001 Feb 19 (1.0.22)} {
<li>The previous fix was not quite right.  This one seems to work better.
    </li>
}

chng {2001 Feb 19 (1.0.21)} {
<li>The UPDATE statement was not working when the WHERE clause contained
    some terms that could be satisfied using indices and other terms that
    could not.  Fixed.</li>
}

chng {2001 Feb 11 (1.0.20)} {
<li>Merge development changes into the main trunk.  Future work toward
    using a BTree file structure will use a separate CVS source tree.  This
    CVS tree will continue to support the GDBM version of SQLite only.</li>
}

chng {2001 Feb 6 (1.0.19)} {
<li>Fix a strange (but valid) C declaration that was causing problems
    for QNX.  No logical changes.</li>
}

chng {2001 Jan 4 (1.0.18)} {
<li>Print the offending SQL statement when an error occurs.</li>
<li>Do not require commas between constraints in CREATE TABLE statements.</li>
<li>Added the "-echo" option to the shell.</li>
<li>Changes to comments.</li>
}

chng {2000 Dec 10 (1.0.17)} {
<li>Rewrote <b>sqlite_complete()</b> to make it faster.</li>
<li>Minor tweaks to other code to make it run a little faster.</li>
<li>Added new tests for <b>sqlite_complete()</b> and for memory leaks.</li>
}

chng {2000 Dec 4 (1.0.16)} {
<li>Documentation updates.  Mostly fixing of typos and spelling errors.</li>
}

chng {2000 Oct 23 (1.0.15)} {
<li>Documentation updates</li>
<li>Some sanity checking code was removed from the inner loop of vdbe.c
    to help the library to run a little faster.  The code is only
    removed if you compile with -DNDEBUG.</li>
}

chng {2000 Oct 19 (1.0.14)} {
<li>Added a "memory:" backend driver that stores its database in an
    in-memory hash table.</li>
}

chng {2000 Oct 18 (1.0.13)} {
<li>Break out the GDBM driver into a separate file in anticipation
    to added new drivers.</li>
<li>Allow the name of a database to be prefixed by the driver type.
    For now, the only driver type is "gdbm:".</li>
}

chng {2000 Oct 16 (1.0.12)} {
<li>Fixed an off-by-one error that was causing a coredump in 
    the '%q' format directive of the new
    <b>sqlite_..._printf()</b> routines.</li>
<li>Added the <b>sqlite_interrupt()</b> interface.</li>
<li>In the shell, <b>sqlite_interrupt()</b> is invoked when the
    user presses Control-C</li>
<li>Fixed some instances where <b>sqlite_exec()</b> was
    returning the wrong error code.</li>
}

chng {2000 Oct 11 (1.0.10)} {
<li>Added notes on how to compile for Windows95/98.</li>
<li>Removed a few variables that were not being used.  Etc.</li>
}

chng {2000 Oct 8 (1.0.9)} {
<li>Added the <b>sqlite_..._printf()</b> interface routines.</li>
<li>Modified the <b>sqlite</b> shell program to use the new interface 
    routines.</li>
<li>Modified the <b>sqlite</b> shell program to print the schema for
    the built-in SQLITE_MASTER table, if explicitly requested.</li>
}

chng {2000 Sep 30 (1.0.8)} {
<li>Begin writing documentation on the TCL interface.</li>
}

chng {2000 Sep 29 (Not Released)} {
<li>Added the <b>sqlite_get_table()</b> API</li>
<li>Updated the documentation for due to the above change.</li>
<li>Modified the <b>sqlite</b> shell to make use of the new
    sqlite_get_table() API in order to print a list of tables
    in multiple columns, similar to the way "ls" prints filenames.</li>
<li>Modified the <b>sqlite</b> shell to print a semicolon at the
    end of each CREATE statement in the output of the ".schema" command.</li>
}

chng {2000 Sep 21 (Not Released)} {
<li>Change the tclsqlite "eval" method to return a list of results if
    no callback script is specified.</li>
<li>Change tclsqlite.c to use the Tcl_Obj interface</li>
<li>Add tclsqlite.c to the libsqlite.a library</li>
}

chng {2000 Sep 13 (Version 1.0.5)} {
<li>Changed the print format for floating point values from "%g" to "%.15g".
    </li>
<li>Changed the comparison function so that numbers in exponential notation
    (ex: 1.234e+05) sort in numerical order.</li>
}

chng {2000 Aug 28 (Version 1.0.4)} {
<li>Added functions <b>length()</b> and <b>substr()</b>.</li>
<li>Fix a bug in the <b>sqlite</b> shell program that was causing
    a coredump when the output mode was "column" and the first row
    of data contained a NULL.</li>
}

chng {2000 Aug 22 (Version 1.0.3)} {
<li>In the sqlite shell, print the "Database opened READ ONLY" message
    to stderr instead of stdout.</li>
<li>In the sqlite shell, now print the version number on initial startup.</li>
<li>Add the <b>sqlite_version[]</b> string constant to the library</li>
<li>Makefile updates</li>
<li>Bug fix: incorrect VDBE code was being generated for the following
    circumstance: a query on an indexed table containing a WHERE clause with
    an IN operator that had a subquery on its right-hand side.</li>
}

chng {2000 Aug 18 (Version 1.0.1)} {
<li>Fix a bug in the configure script.</li>
<li>Minor revisions to the website.</li>
}

chng {2000 Aug 17 (Version 1.0)} {
<li>Change the <b>sqlite</b> program so that it can read
    databases for which it lacks write permission.  (It used to
    refuse all access if it could not write.)</li>
}

chng {2000 Aug 9} {
<li>Treat carriage returns as white space.</li>
}

chng {2000 Aug 8} {
<li>Added pattern matching to the ".table" command in the "sqlite"
command shell.</li>
}

chng {2000 Aug 4} {
<li>Documentation updates</li>
<li>Added "busy" and "timeout" methods to the Tcl interface</li>
}

chng {2000 Aug 3} {
<li>File format version number was being stored in sqlite_master.tcl
    multiple times. This was harmless, but unnecessary. It is now fixed.</li>
}

chng {2000 Aug 2} {
<li>The file format for indices was changed slightly in order to work
    around an inefficiency that can sometimes come up with GDBM when
    there are large indices having many entries with the same key.
    <font color="red">** Incompatible Change **</font></li>
}

chng {2000 Aug 1} {
<li>The parser's stack was overflowing on a very long UPDATE statement.
    This is now fixed.</li>
}

chng {2000 July 31} {
<li>Finish the <a href="vdbe.html">VDBE tutorial</a>.</li>
<li>Added documentation on compiling to WindowsNT.</li>
<li>Fix a configuration program for WindowsNT.</li>
<li>Fix a configuration problem for HPUX.</li>
}

chng {2000 July 29} {
<li>Better labels on column names of the result.</li>
}

chng {2000 July 28} {
<li>Added the <b>sqlite_busy_handler()</b> 
    and <b>sqlite_busy_timeout()</b> interface.</li>
}

chng {2000 June 23} {
<li>Begin writing the <a href="vdbe.html">VDBE tutorial</a>.</li>
}

chng {2000 June 21} {
<li>Clean up comments and variable names.  Changes to documentation.
    No functional changes to the code.</li>
}

chng {2000 June 19} {
<li>Column names in UPDATE statements were case sensitive.
    This mistake has now been fixed.</li>
}

chng {2000 June 16} {
<li>Added the concatenate string operator (||)</li>
}

chng {2000 June 12} {
<li>Added the fcnt() function to the SQL interpreter.  The fcnt() function
    returns the number of database "Fetch" operations that have occurred.
    This function is designed for use in test scripts to verify that
    queries are efficient and appropriately optimized.  Fcnt() has no other
    useful purpose, as far as I know.</li>
<li>Added a bunch more tests that take advantage of the new fcnt() function.
    The new tests did not uncover any new problems.</li>
}

chng {2000 June 8} {
<li>Added lots of new test cases</li>
<li>Fix a few bugs discovered while adding test cases</li>
<li>Begin adding lots of new documentation</li>
}

chng {2000 June 6} {
<li>Added compound select operators: <B>UNION</b>, <b>UNION ALL</B>,
<b>INTERSECT</b>, and <b>EXCEPT</b></li>
<li>Added support for using <b>(SELECT ...)</b> within expressions</li>
<li>Added support for <b>IN</b> and <b>BETWEEN</b> operators</li>
<li>Added support for <b>GROUP BY</b> and <b>HAVING</b></li>
<li>NULL values are now reported to the callback as a NULL pointer
    rather than an empty string.</li>
}

chng {2000 June 3} {
<li>Added support for default values on columns of a table.</li>
<li>Improved test coverage.  Fixed a few obscure bugs found by the
improved tests.</li>
}

chng {2000 June 2} {
<li>All database files to be modified by an UPDATE, INSERT or DELETE are 
now locked before any changes are made to any files.  
This makes it safe (I think) to access
the same database simultaneously from multiple processes.</li>
<li>The code appears stable so we are now calling it "beta".</li>
}

chng {2000 June 1} {
<li>Better support for file locking so that two or more processes 
(or threads)
can access the same database simultaneously.  More work needed in
this area, though.</li>
}

chng {2000 May 31} {
<li>Added support for aggregate functions (Ex: <b>COUNT(*)</b>, <b>MIN(...)</b>)
to the SELECT statement.</li>
<li>Added support for <B>SELECT DISTINCT ...</B></li>
}

chng {2000 May 30} {
<li>Added the <b>LIKE</b> operator.</li>
<li>Added a <b>GLOB</b> operator: similar to <B>LIKE</B> 
but it uses Unix shell globbing wildcards instead of the '%' 
and '_' wildcards of SQL.</li>
<li>Added the <B>COPY</b> command patterned after 
<a href="http://www.postgresql.org/">PostgreSQL</a> so that SQLite
can now read the output of the <b>pg_dump</b> database dump utility
of PostgreSQL.</li>
<li>Added a <B>VACUUM</B> command that that calls the 
<b>gdbm_reorganize()</b> function on the underlying database
files.</li>
<li>And many, many bug fixes...</li>
}

chng {2000 May 29} {
<li>Initial Public Release of Alpha code</li>
}

puts {
</DL>
<p><hr /></p>
<p><a href="index.html"><img src="/goback.jpg" border=0 />
Back to the SQLite Home Page</a>
</p>

</body></html>}

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
<li>NULL values are now reported ot the callback as a NULL pointer
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

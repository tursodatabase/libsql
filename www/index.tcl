#
# Run this TCL script to generate HTML for the index.html file.
#
set rcsid {$Id: index.tcl,v 1.9 2000/06/01 00:04:41 drh Exp $}

puts {<html>
<head><title>SQLite: An SQL Frontend For GDBM</title></head>
<body bgcolor=white>
<h1 align=center>SQLite: An SQL Frontend For GDBM</h1>
<p align=center>}
puts "Last modified [lrange $rcsid 3 4] GMT"
puts {</p>}

puts {
<blockquote><em><p>
The SQLite code base is rapidly becoming usable.  Most of the commonly
used features of SQL (at least the features of SQL that this author
commonly uses) are now supported.  There are currently no known
errors in the code.  (There are known omissions but that is another
matter.)
One very large database (1M+ records in 50+ separate tables) has
been converted from PostgreSQL and gives every appearance of working
correctly.  We are rapidly approaching a "beta" release, I think...</p>

<p>Your constructive comments are still very important to us.
Please visit the 
<a href="#mailinglist">mailing list</a> to offer your feedback.</p>
</em></blockquote>
}

puts {<h2>Introduction</h2>

<p>SQLite is a C library that implements an SQL frontend to GDBM.
SQLite is intended for use in standalone programs that need 
to use an SQL database but which do not have access to a full-blown 
SQL RDBMS.</p>

<p>The C interface to SQLite is very simple, consisting of only
four functions and a single opaque data structure.  
See <a href="c_interface.html">c_interface.html</a> for details.
A Tcl interface
to SQLite is also available and is included in the source tree.
Documentation on the Tcl interface is pending.
Interfaces for perl and python may be supplied in future releases.</p>

<p>There is a standalone C program named "sqlite" that can be used
to interactively create, update and/or query an SQLite database.
The sources to the sqlite program are part of the source tree and
can be used as an example of how to interact with the SQLite C
library.  For more information on the sqlite program,
see <a href="sqlite.html">sqlite.html</a>.</p>

<p>A history of changes to SQLite is found
<a href="changes.html">here</a>.</p>

<p>SQLite does not try to implement every feature of SQL. 
A few of the many SQL features that SQLite does not (currently) 
implement are as follows:</p>

<p>
<ul>
<li>The GROUP BY or HAVING clauses of a SELECT</li>
<li>Constraints</li>
<li>Nested queries</li>
<li>Transactions or rollback</li>
</ul>
</p>

<H2>Status</h2>

<p>The SQLite code is rapidly stablizing.  There are currently
no known errors in the code.  At least one large database has
be loaded into SQLite and appears to work.  Most of the major
functionality is in place.</p>

<p>SQLite has so far been tested only on RedHat 6.0 Linux.  But we
know of no reason why it will not work on any other Unix platform,
or on Windows95/98/NT.</p>
}

puts {
<a name="mailinglist" />
<h2>Mailing List</h2>
<p>A mailing list has been set up on eGroups for discussion of
SQLite design issues or for asking questions about SQLite.</p>
<center>
<a href="http://www.egroups.com/subscribe/sqlite">
<img src="http://www.egroups.com/img/ui/join.gif" border=0 /><br />
Click to subscribe to sqlite</a>
</center>}

puts {<h2>Download</h2>

<p>You can download a tarball containing all C source
code for SQLite at <a href="sqlite.tar.gz">sqlite.tar.gz</a>.}
puts "This is a [file size sqlite.tar.gz] byte download.  The
tarball was last modified at [clock format [file mtime sqlite.tar.gz]]"
puts {</p>

<p>You can also download a larger tarball that contains everything
in the source tarball plus all of the sources for the text that
appears on this website, and other miscellaneous files.  The
complete tarball is found at <a href="all.tar.gz">all.tar.gz</a>.}
puts "This is a [file size all.tar.gz] byte download and was
was last modified at [clock format [file mtime sqlite.tar.gz]]</p>"

puts {<h2>Related Sites</h2>

<ul>
<li><p>The canonical site for GDBM is
       <a href="http://www.gnu.org/software/gdbm/gdbm.html">
       http://www.gnu.org/software/gdbm/gdbm.html</a></p></li>

<li><p>Someday, we would like to port SQLite to work with
       the Berkeley DB library in addition to GDBM.  For information
       about the Berkeley DB library, see
       <a href="http://www.sleepycat.com/">http://www.sleepycat.com/</a>
       </p></li>

<li><p>Here is a <a href="http://w3.one.net/~jhoffman/sqltut.htm">
       tutorial on SQL</a>.</p></li>
</ul>}

puts {
<p><hr /></p>
<p>
<a href="../index.html"><img src="/goback.jpg" border=0 />
More Open Source Software</a> from Hwaci.
</p>

</body></html>}

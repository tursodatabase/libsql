#
# Run this TCL script to generate HTML for the index.html file.
#
set rcsid {$Id: index.tcl,v 1.4 2000/05/30 00:05:13 drh Exp $}

puts {<html>
<head><title>SQLite: An SQL Frontend For GDBM</title></head>
<body bgcolor=white>
<h1 align=center>SQLite: An SQL Frontend For GDBM</h1>
<p align=center>}
puts "Last modified [lrange $rcsid 3 4] GMT"
puts {</p>}

puts {
<blockquote><em><p>
SQLite is currently "alpha"-quality software under active development.
It is being release early so that you can have an opportunity
to comment on its design and implementation and possibly influence
the direction of its development.  Your constructive comments
are <b>very</b> important to us and are encouraged.  If you have 
any suggestions or any words of encouragement, please submit
them to the mailing list described <a href="#mailinglist">below</a>.</p>

<p>If you are looking for a stable SQL library, check back here in a few
months...</p></em></blockquote>
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

<p>SQLite does not try to implement every feature of SQL.  But it
does strive to implement to most commonly used features.  SQLite
currently understands the following SQL commands:</p>

<p>
<ul>
<li>CREATE TABLE</li>
<li>CREATE INDEX</li>
<li>DROP TABLE</li>
<li>DROP INDEX</li>
<li>INSERT INTO</li>
<li>UPDATE</li>
<li>SELECT</li>
<li>DELETE FROM</li>
</ul>
</p>

<p>A few of the many SQL features that SQLite does not (currently) 
implement are as follows:</p>

<p>
<ul>
<li>ALTER TABLE</li>
<li>The GROUP BY or HAVING clauses of a SELECT</li>
<li>The LIKE or IN operators</li>
<li>The COUNT(), MAX(), MIN(), and AVG() functions</li>
<li>Constraints</li>
<li>Nested queries</li>
<li>Transactions or rollback</li>
</ul>
</p>

<H2>Status</h2>

<p>The current version of SQLite should be considered "alpha" software.
It is incomplete and is known to contain bugs.  The software is
subject to incompatible changes with each release.  You should not use
SQLite in its present form in production software.</p>

<p>The purpose of releasing SQLite before it is ready is to evoke
public comment and criticism of the software.  If you find bugs
or have any thoughts on how to make SQLite better, or would
like to contribute code or patches to SQLite, please join
the mailing (see below) and let us know.</p>

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
       <a href="http://www.sleepcat.com/">http://www.sleepycat.com</a>
       </p></li>
</ul>}

puts {
<p><hr /></p>
<p>
<a href="../index.html"><img src="/goback.jpg" border=0 />
More Open Source Software</a> from Hwaci.
</p>

</body></html>}

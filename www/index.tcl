#
# Run this TCL script to generate HTML for the index.html file.
#
set rcsid {$Id: index.tcl,v 1.14 2000/06/06 22:19:02 drh Exp $}

puts {<html>
<head><title>SQLite: An SQL Database Built Atop GDBM</title></head>
<body bgcolor=white>
<h1 align=center>SQLite: An SQL Database Built Upon 
<a href="http://www.gnu.org/software/gdbm/gdbm.html">GDBM</a></h1>
<p align=center>}
puts "This page was last modified on [lrange $rcsid 3 4] GMT<br>"
puts "The SQLite source code was last modifed on [exec cat last_change] GMT"
puts {</p>}

puts {
<h2>News</h2>
<p>
The SQLite code base is being called "beta" only because it is
relatively new.  It appears to be stable and usable.
Most of the SQL language is now implemented and working.  
The regression test suite
provides good coverage, according to
<a href="http://gcc.gnu.org/onlinedocs/gcov_1.html">gcov</a>.
There are currently no known errors in the code.</p>

<p>If you find bugs or missing features, please submit a comment
to the <a href="#mailinglist">SQLite mailing list</a>.</p>
}

puts {<h2>Introduction</h2>

<p>SQLite is an SQL database built atop the 
<a href="http://www.gnu.org/software/gdbm/gdbm.html">GDBM library</a>.
The SQLite distribution includes both a interactive command-line
access program (<b>sqlite</b>) and a C library (<b>libsqlite.a</b>)
that can be linked
with a C/C++ program to provide SQL database access without having
to rely on an external RDBMS.</p>

<p>The C interface to SQLite is very simple, consisting of only
four functions, a single opaque data structure, and a handful of
constants that define error return codes.
See <a href="c_interface.html">c_interface.html</a> for details.
A Tcl interface
to SQLite is also available and is included in the source tree.
Documentation on the Tcl interface is pending.
Interfaces for perl and python may be supplied in future releases.</p>

<p>The standalone program <b>sqlite</b> can be used
to interactively create, update and/or query an SQLite database.
The sources to the sqlite program are part of the source tree and
can be used as an example of how to interact with the SQLite C
library.  For more information on the sqlite program,
see <a href="sqlite.html">sqlite.html</a>.</p>

<p>A history of changes to SQLite is found
<a href="changes.html">here</a>.</p>

<p>SQLite now implements most of the SQL language.
The following are the known limitations:</p>

<p>
<ul>
<li>Constraints are parsed but are not enforced</li>
<li>There is no support for transactions or rollback</li>
</ul>
</p>

<H2>Status</h2>

<p>New features are still being added to the SQLite code base.
Nevertheless, the code appears to be stable and relatively
bug-free. At least one large database has
be loaded into SQLite and appears to work.</p>

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

<p>You can download a tarball containing all source
code for SQLite (including the TCL scripts that generate the
HTML files for this website) at <a href="sqlite.tar.gz">sqlite.tar.gz</a>.}
puts "This is a [file size sqlite.tar.gz] byte download.  The
tarball was last modified at [clock format [file mtime sqlite.tar.gz]]"
puts {</p>

<p>To build sqlite, just unwrap the tarball, create a separate
build directory, run configure from the build directory and then
type "make".  For example:</p>

<blockquote><pre>
$ tar xzf sqlite.tar.gz   ;# Unpacks into directory named "sqlite"
$ mkdir bld               ;# Create a separate build directory
$ cd bld
$ ../sqlite/configure
$ make                    ;# Builds "sqlite" and "libsqlite.a"
$ make test               ;# Optional: run regression tests
</pre></blockquote>
}

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

<li><p>Here is a good <a href="http://w3.one.net/~jhoffman/sqltut.htm">
       tutorial on SQL</a>.</p></li>

<li><p><a href="http://www.postgresql.org/">PostgreSQL</a> is a
       full-blown SQL RDBMS that is also open source.</p></li>

<li><p><a href="http://www.chordate.com/gadfly.html">Gadfly</a> is another
       SQL library, similar to SQLite, except that Gadfly is written
       in Python.</p></li>

<li><p><a href="http://www.vogel-nest.de/tcl/qgdbm.html">Qgdbm</a> is
       a wrapper around 
       <a href="http://www.vogel-nest.de/tcl/tclgdbm.html">tclgdbm</a>
       that provides SQL-like access to GDBM files.</p></li>
</ul>}

puts {
<p><hr /></p>
<p>
<a href="../index.html"><img src="/goback.jpg" border=0 />
More Open Source Software</a> from Hwaci.
</p>

</body></html>}

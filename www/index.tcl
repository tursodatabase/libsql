#
# Run this TCL script to generate HTML for the index.html file.
#
set rcsid {$Id: index.tcl,v 1.29 2000/09/30 22:46:07 drh Exp $}

puts {<html>
<head><title>SQLite: An SQL Database Library Built Atop GDBM</title></head>
<body bgcolor=white>
<h1 align=center>SQLite: An SQL Database Library Built Atop
<a href="http://www.gnu.org/software/gdbm/gdbm.html">GDBM</a></h1>
<p align=center>}
puts "This page was last modified on [lrange $rcsid 3 4] GMT<br>"
set vers [lindex $argv 0]
puts "The latest SQLite version is <b>$vers</b>"
puts " created on [exec cat last_change] GMT"
puts {</p>}

puts {<h2>Introduction</h2>

<p>SQLite is an SQL database library
(<a href="c_interface.html">libsqlite.a</a>) that uses
<a href="http://www.gnu.org/software/gdbm/gdbm.html">GDBM</a>
as its underlying file storage mechanism.
Programs that link the SQLite library can have SQL database
access without running a separate RDBMS process.
The distribution comes with a standalone command-line
access program (<a href="sqlite.html">sqlite</a>) that can
be used to administer an SQLite database and which serves as
an example of how to use the SQLite library.</p>


<h2>Features</h2>

<p><ul>
<li>Implements most of SQL92.</li>
<li>A database is just a directory of GDBM files.</li>
<li>Unlimited length records.</li>
<li>Import and export data from 
<a href="http://www.postgresql.org/">PostgreSQL</a>.</li>
<li>Very simple 
<a href="c_interface.html">C/C++ interface</a> requires the use of only
three functions and one opaque structure.</li>
<li>A <a href="tclsqlite.html">Tcl</a> interface is
included.</li>
<li>Command-line access program <a href="sqlite.html">sqlite</a> uses
the <a href="http://www.google.com/search?q=gnu+readline+library">GNU
Readline library</a></li>
<li>A Tcl-based test suite provides near 100% code coverage</li>
<li>7500+ lines of C code.  No external dependencies other than GDBM.</li>
<li>Built and tested under Linux, HPUX, and WinNT.</li>
</ul>
</p>

<h2>Current Status</h2>

<p>A <a href="changes.html">change history</a> is available online.
There are currently no <em>known</em> bugs or memory leaks
in the library.  <a href="http://gcc.gnu.org/onlinedocs/gcov_1.html">Gcov</a>
is used to verify test coverage.  The test suite currently exercises
all code except for a few areas which are unreachable or which are
only reached when <tt>malloc()</tt> fails.  The code has been tested
for memory leaks and is found to be clean.</p>

<p>
Among the SQL features that SQLite does not currently implement are:</p>

<p>
<ul>
<li>outer joins</li>
<li>constraints are parsed but are not enforced</li>
<li>no support for transactions or rollback</li>
</ul>
</p>

<h2>Important News Flash!</h2>
<p>
The SQLite file format was changed in an incompatible way on
Aug 2, 2000.  If you are updated the library and have databases
built using the old version of the library, you should save your
old databases into an ASCII file then reimport the
database using the new library.  For example, if you change the
name of the old <b>sqlite</b> utility to "old-sqlite" and
change the name of the old database directory to "old-db", then
you can reconstruct the database as follows:</p>

<blockquote><pre>
echo .dump | old-sqlite old-db | sqlite db
</pre></blockquote>

<p>This file format change was made to work around a potential 
inefficiency in GDBM that comes up when large indices are created 
on tables where many entries in the table have the same index key.</p>

<h2>Documentation</h2>

<p>The following documentation is currently available:</p>

<p><ul>
<li>Information on the <a href="sqlite.html">sqlite</a>
    command-line utility.</li>
<li>The <a href="lang.html">SQL Language</a> subset understood by SQLite.</li>
<li>The <a href="c_interface.html">C/C++ Interface</a>.</li>
<li>The <a href="tclsqlite.html">Tcl Interface</a>.</li>
<li>The <a href="fileformat.html">file format</a> used by SQLite databases.</li>
<li>The <a href="arch.html">Architecture of the SQLite Library</a> describes
    how the library is put together.</li>
<li>A description of the <a href="opcode.html">virtual machine</a> that
    SQLite uses to access the database.</li>
<li>Instructions for building 
    <a href="crosscompile.html">SQLite for Win98/NT</a> using the
    MinGW cross-compiler.  There are also instructions on
    <a href="mingw.html">building MinGW</a> in case you don't already have
    a copy.</li>
</ul>
</p>

<p>The SQLite source code is 35% comment.  These comments are
another important source of information. </p>
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
code for SQLite
}
puts "version $vers"
puts {
(including the TCL scripts that generate the
HTML files for this website) at <a href="sqlite.tar.gz">sqlite.tar.gz</a>.}
puts "This is a [file size sqlite.tar.gz] byte download."
puts {</p>

<p>To build sqlite under Unix, just unwrap the tarball, create a separate
build directory, run configure from the build directory and then
type "make".  For example:</p>

<blockquote><pre>
$ tar xzf sqlite.tar.gz      <i> Unpacks into directory named "sqlite" </i>
$ mkdir bld                  <i> Create a separate build directory </i>
$ cd bld
$ ../sqlite/configure
$ make                       <i> Builds "sqlite" and "libsqlite.a" </i>
$ make test                  <i> Optional: run regression tests </i>
</pre></blockquote>

<p>Instructions for building SQLite for WindowsNT are
found <a href="crosscompile.html">here</a>.
}

puts {<h2>Command-line Usage Example</h2>

<p>Download the source archive and compile the <b>sqlite</b>
program as described above.  The type:</p>

<blockquote><pre>
bash$ sqlite ~/newdb              <i>Directory ~/newdb created automatically</i>
sqlite> create table t1(
   ...>    a int,
   ...>    b varchar(20)
   ...>    c text
   ...> );                        <i>End each SQL statement with a ';'</i>
sqlite> insert into t1
   ...> values(1,'hi','y''all');
sqlite> select * from t1;
1|hello|world
sqlite> .mode columns             <i>Special commands begin with '.'</i>
sqlite> .header on                <i>Type ".help" for a list of commands</i>
sqlite> select * from t1;
a      b       c
------ ------- -------
1      hi      y'all
sqlite> .exit
base$
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

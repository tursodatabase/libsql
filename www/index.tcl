#
# Run this TCL script to generate HTML for the index.html file.
#
set rcsid {$Id: index.tcl,v 1.74 2003/05/17 01:39:40 drh Exp $}

puts {<html>
<head><title>SQLite: An Embeddable SQL Database Engine</title></head>
<body bgcolor=white>
<h1 align=center>SQLite<br>An Embeddable SQL Database Engine</h1>
<p align=center>}
puts "This page was last modified on [lrange $rcsid 3 4] UTC<br>"
set vers [lindex $argv 0]
puts "The latest SQLite version is <b>$vers</b>"
puts " created on [exec cat last_change] UTC"
puts {</p>}

puts {<h2>Introduction</h2>

<p>SQLite is a C library that implements an embeddable SQL database engine.
Programs that link with the SQLite library can have SQL database
access without running a separate RDBMS process.
The distribution comes with a standalone command-line
access program (<a href="sqlite.html">sqlite</a>) that can
be used to administer an SQLite database and which serves as
an example of how to use the SQLite library.</p>

<p>SQLite is <b>not</b> a client library used to connect to a
big database server.  SQLite <b>is</b> the server.  The SQLite
library reads and writes directly to and from the database files
on disk.</p>}

puts {
<table align="right" hspace="10" cellpadding=0 cellspacing=0 broder=0>
<tr><td align="right" bgcolor="#cacae4">
<table border="2" width="100%" cellspacing=0 cellpadding=5><tr><td align="left">
Quick Links:
<ul>
<li><a href="download.html">Download</a></li>
<li><a href="http://cvs.hwaci.com/sqlite/timeline">Change Log</a></li>
<li><a href="http://cvs.hwaci.com/sqlite/tktnew">Report a bug</a></li>
<li><a href="quickstart.html">Quick start</a></li>
</ul>
</td></tr></table>
</td></tr>
</table>
}

puts {<h2>Features</h2>

<p><ul>
<li>Implements most of SQL92.
    (<a href="omitted.html">Features not supported</a>)</li>
<li>A complete database (with multiple tables and indices) is
    stored in a single disk file.</li>
<li>Atomic commit and rollback protect data integrity.</li>
<li>Database files can be freely shared between machines with
    different byte orders.</li>
<li>Supports databases up to 2 terabytes (2^41 bytes) in size.</li>
<li>Small memory footprint: less than 25K lines of C code.</li>
<li><a href="speed.html">Two times faster</a> than PostgreSQL and
    MySQL for many common operations.</li>
<li>Very simple 
<a href="c_interface.html">C/C++ interface</a> requires the use of only
three functions and one opaque structure.</li>
<li><a href="tclsqlite.html">TCL bindings</a> included.
    Bindings for many other languages available separately.</li>
<li>Simple, well-commented source code.</li>
<li>Automated test suite provides near 100% code coverage.</li>
<li>Self-contained: no external dependencies.</li>
<li>Built and tested under Linux and Windows.</li>
<li>Sources are in the public domain.  Use for any purpose.</li>
</ul>
</p>
}

puts {<h2>Current Status</h2>

<p>A <a href="changes.html">Change Summary</a> is available on this
website.  You can also access a detailed
<a href="http://cvs.hwaci.com/sqlite/timeline">change history</a>,
<a href="http://cvs.hwaci.com/sqlite/rptview?rn=2">view open bugs</a>,
or
<a href="http://cvs.hwaci.com/sqlite/tktnew">report new bugs</a>
at the
<a href="http://cvs.hwaci.com/sqlite/">CVS server</a>.</p>

<p>Complete source code and precompiled binaries for the latest release are
<a href="download.html">available for download</a> on this site.  You
can also obtain the latest changes by anonymous CVS access:
<blockquote><pre>
cvs -d :pserver:anonymous@cvs.hwaci.com:/home/cvs/sqlite login
cvs -d :pserver:anonymous@cvs.hwaci.com:/home/cvs/sqlite checkout sqlite
</pre></blockquote>
When prompted for a password, enter "anonymous".
</p>

<p>Note that the CVS server is located on a cable modem with a dynamic
IP address.  The IP address changes every 3 or 4 months.  After an
IP address change
occurs it usually takes a day or two for the new DNS information to propagate.
So if you have trouble accessing the CVS server, it could be because the
IP address has recently changed.  Try again in a few days.
</p>

<p>
Whenever either of the first two digits in the version number
for SQLite change, it means that the underlying file format
has changed.  Usually these changes are backwards compatible.
See <a href="formatchng.html">formatchng.html</a>
for additional information.
</p>
}

puts {<h2>Documentation</h2>

<p>The following documentation is currently available:</p>

<p><ul>
<li>A <a href="quickstart.html">Quick Start</a> guide to using SQLite in
    5 minutes or less.</li>
<li><a href="faq.html">Frequently Asked Questions</a> are available online.</li>
<li>Information on the <a href="sqlite.html">sqlite</a>
    command-line utility.</li>
<li>SQLite is <a href="datatypes.html">typeless</a>.
<li>The <a href="lang.html">SQL Language</a> subset understood by SQLite.</li>
<li>The <a href="c_interface.html">C/C++ Interface</a>.</li>
<li>The <a href="nulls.html">NULL handling</a> in SQLite versus
    other SQL database engines.</li>
<li>The <a href="tclsqlite.html">Tcl Binding</a> to SQLite.</li>
<li>The <a href="arch.html">Architecture of the SQLite Library</a> describes
    how the library is put together.</li>
<li>A description of the <a href="opcode.html">virtual machine</a> that
    SQLite uses to access the database.</li>
<li>A description of the 
    <a href="fileformat.html">database file format</a> used by SQLite.
<li>A <a href="speed.html">speed comparison</a> between SQLite, PostgreSQL,
    and MySQL.</li>
<li>User-written documentation is available on the
    <a href="http://cvs.hwaci.com/sqlite/wiki">SQLite Wiki</a>.  Please
    contribute if you can.</li>
</ul>
</p>

<p>The SQLite source code is 30% comment.  These comments are
another important source of information.  </p>

}

puts {
<table align="right">
<tr><td align="center">
<a href="http://www.yahoogroups.com/subscribe/sqlite">
<img src="http://www.egroups.com/img/ui/join.gif" border=0 /><br />
Click to subscribe to sqlite</a>
</td></tr>
</table>
<a name="mailinglist" />
<h2>Mailing List</h2>
<p>A mailing list has been set up on yahooGroups for discussion of
SQLite design issues or for asking questions about SQLite.</p>
}

puts {<h2>Professional Support and Custom Modifications</h2>}

puts {
<p>
If you would like professional support for SQLite
or if you want custom modifications to SQLite preformed by the
original author, these services are available for a modest fee.
For additional information visit
<a href="http://www.hwaci.com/sw/sqlite/support.html">
http://www.hwaci.com/sw/sqlite/support.html</a> or contact:</p>

<blockquote>
D. Richard Hipp <br />
Hwaci - Applied Software Research <br />
704.948.4565 <br />
<a href="mailto:drh@hwaci.com">drh@hwaci.com</a>
</blockquote>
}

puts {<h2>Building From Source</h2>}

puts {
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

<p>If you prefer, you can also build by making whatever modifications
you desire to the file "Makefile.linux-gcc" and then executing that
makefile.  Tha latter method is used for all official development
and testing of SQLite and for building the precompiled
binaries found on this website.  Windows binaries are generated by
cross-compiling from Linux using <a href="http://www.mingw.org/">MinGW</a></p>
}

puts {<h2>Related Sites</h2>

<p>
For information bindings of SQLite to other programming languages
(Perl, Python, Ruby, PHP, etc.) and for a list of programs currently
using SQLite, visit the Wiki documentation at:
</p>

<blockquote>
<a href="http://cvs.hwaci.com/sqlite/wiki">
http://cvs.hwaci.com/sqlite/wiki</a>
</blockquote>
}

puts {
</body></html>}

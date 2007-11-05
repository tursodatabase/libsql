#!/usr/bin/tclsh
source common.tcl
header {SQLite home page}
puts {
<table width="100%" border="0" cellspacing="5">
<tr>
<td width="50%" valign="top">
<h2>About SQLite</h2>
<p>
  <table align="right" border="0"><tr><td>
  <a href="http://osdir.com/Article6677.phtml">
  <img src="2005osaward.gif"></a>
  </td></tr></table>
SQLite is a small
C library that implements a self-contained, embeddable,
zero-configuration
SQL database engine.
Features include:
</p>

<p><ul>
<li>Transactions are atomic, consistent, isolated, and durable (ACID)
    even after system crashes and power failures.
<li>Zero-configuration - no setup or administration needed.</li>
<li>Implements most of SQL92.
    (<a href="omitted.html">Features not supported</a>)</li>
<li>A complete database is stored in a single disk file.</li>
<li>Database files can be freely shared between machines with
    different byte orders.</li>
<li>Supports terabyte-sized databases and gigabyte-sized strings
    and blobs.  (See <a href="limits.html">limits.html</a>.)
<li>Small code footprint: 
    <a href="http://www.sqlite.org/cvstrac/wiki?p=SizeOfSqlite">
    less than 250KiB</a> fully configured or less
    than 150KiB with optional features omitted.</li>
<li><a href="speed.html">Faster</a> than popular client/server database
    engines for most common operations.</li>
<li>Simple, easy to use <a href="capi3.html">API</a>.</li>
<li><a href="tclsqlite.html">TCL bindings</a> included.
    Bindings for many other languages 
    <a href="http://www.sqlite.org/cvstrac/wiki?p=SqliteWrappers">
    available separately.</a></li>
<li>Well-commented source code with over 98% test coverage.</li>
<li>Available as a 
    <a href="http://www.sqlite.org/cvstrac/wiki?p=TheAmalgamation">
    single ANSI-C source-code file</a> that you can easily drop into
    another project.
<li>Self-contained: no external dependencies.</li>
<li>Sources are in the <a href="copyright.html">public domain</a>.
    Use for any purpose.</li>
</ul>
</p>

<p>
The SQLite distribution comes with a standalone command-line
access program (<a href="sqlite.html">sqlite</a>) that can
be used to administer an SQLite database and which serves as
an example of how to use the SQLite library.
</p>

</td>
<td width="1" bgcolor="#80a796"></td>
<td valign="top" width="50%">
<h2>News</h2>
}

proc newsitem {date title text} {
  puts "<h3>$date - $title</h3>"
  regsub -all "\n( *\n)+" $text "</p>\n\n<p>" txt
  puts "<p>$txt</p>"
  puts "<hr width=\"50%\">"
}

newsitem {2007-Nov-05} {Version 3.5.2} {
  This is an incremental release that fixes several minor problems,
  adds some obscure features, and provides some performance tweaks.  
  Upgrading is optional.

  The experimental compile-time option
  SQLITE_OMIT_MEMORY_ALLOCATION is no longer supported.  On the other
  hand, it is now possible to compile SQLite so that it uses a static
  array for all its dynamic memory allocation needs and never calls
  malloc.  Expect to see additional radical changes to the memory 
  allocation subsystem in future releases.
}

newsitem {2007-Oct-04} {Version 3.5.1} {
  Fix a long-standing bug that might cause database corruption if a
  disk-full error occurs in the middle of a transaction and that
  transaction is not rolled back.
  <a href="http://www.sqlite.org/cvstrac/tktview?tn=2686">Ticket #2686.</a>

  The new VFS layer is stable.  However, we still reserve the right to
  make tweaks to the interface definition of the VFS if necessary.
}

newsitem {2007-Sep-04} {Version 3.5.0 alpha} {
  The OS interface layer and the memory allocation subsystems in
  SQLite have been reimplemented.  The published API is largely unchanged
  but the (unpublished) OS interface has been modified extensively.  
  Applications that implement their own OS interface will require
  modification.  See
  <a href="34to35.html">34to35.html</a> for details.<p>

  This is a large change.  Approximately 10% of the source code was
  modified.  We are calling this first release "alpha" in order to give
  the user community time to test and evaluate the changes before we
  freeze the new design.
}

puts {
<p align="right"><a href="oldnews.html">Old news...</a></p>
</td></tr></table>
}
footer {$Id: index.tcl,v 1.165 2007/11/05 18:11:18 drh Exp $}

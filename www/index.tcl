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
<li>Supports databases up to 2 tebibytes
    (2<sup><small>41</small></sup> bytes) in size.</li>
<li>Strings and BLOBs up to 2 gibibytes (2<sup><small>31</small></sup> bytes)
    in size.</li>
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

newsitem {2007-Apr-18} {Version 3.3.16} {
  Performance improvements added in 3.3.14 but mistakenly turned off
  in 3.3.15 have been reinstated.  A bug has been fixed that prevented 
  VACUUM from running if a NULL value was in a UNIQUE column.
}

newsitem {2007-Apr-09} {Version 3.3.15} {
  An annoying bug introduced in 3.3.14 has been fixed.  There are
  also many enhancements to the test suite.  
}

newsitem {2007-Apr-02} {Version 3.3.14} {
  This version focuses on performance improvements.  If you recompile
  <a href="http://www.sqlite.org/cvstrac/wiki?p=TheAmalgamation">
  the amalgamation</a> using GCC option -O3 (the precompiled binaries
  use -O2) you may see performance
  improvements of 35% or more over version 3.3.13 depending on your
  workload.  This version also
  adds support for <a href="pragma.html#pragma_locking_mode">
  exclusive access mode</a>.
}

newsitem {2007-Feb-13} {Version 3.3.13} {
  This version fixes a subtle bug in the ORDER BY optimizer that can 
  occur when using joins.  There are also a few minor enhancements.
  Upgrading is recommended.
}

newsitem {2007-Jan-27} {Version 3.3.12} {
  The first published build of the previous version used the wrong
  set of source files.  Consequently, many people downloaded a build
  that was labeled as "3.3.11" but was really 3.3.10.  Version 3.3.12
  is released to clear up the ambiguity.  A couple more bugs have
  also been fixed and <a href="pragma.html#pragma_integrity_check">
  PRAGMA integrity_check</a> has been enhanced.
}

puts {
<p align="right"><a href="oldnews.html">Old news...</a></p>
</td></tr></table>
}
footer {$Id: index.tcl,v 1.153 2007/04/18 13:49:37 drh Exp $}

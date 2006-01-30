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
<li>Supports databases up to 2 terabytes
    (2<sup><small>41</small></sup> bytes) in size.</li>
<li>Sizes of strings and BLOBs limited only by available memory.</li>
<li>Small code footprint: less than 250KiB fully configured or less
    than 150KiB with optional features omitted.</li>
<li><a href="speed.html">Faster</a> than popular client/server database
    engines for most common operations.</li>
<li>Simple, easy to use <a href="capi3.html">API</a>.</li>
<li><a href="tclsqlite.html">TCL bindings</a> included.
    Bindings for many other languages 
    <a href="http://www.sqlite.org/cvstrac/wiki?p=SqliteWrappers">
    available separately.</a></li>
<li>Well-commented source code with over 95% test coverage.</li>
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

newsitem {2006-Jan-30} {Version 3.3.3 stable} {
  There have been no major problems discovered in version 3.3.2, so
  we hereby declare the new APIs and language features to be stable
  and supported.
}

newsitem {2006-Jan-24} {Version 3.3.2 beta} {
  More bug fixes and performance improvements as we move closer to
  a production-ready version 3.3.x.
}

newsitem {2006-Jan-16} {Version 3.3.1 alpha} {
  Many bugs found in last week's alpha release have now been fixed and
  the library is running much faster again.

  Database connections can now be moved between threads as long as the
  connection holds no locks at the time it is moved.  Thus the common
  paradigm of maintaining a pool of database connections and handing
  them off to transient worker threads is now supported.
  Please help test this new feature.
  See <a href="http://www.sqlite.org/cvstrac/wiki?p=MultiThreading">
  the MultiThreading wiki page</a> for additional
  information.
}

newsitem {2006-Jan-10} {Version 3.3.0 alpha} {
  Version 3.3.0 adds support for CHECK constraints, DESC indices,
  separate REAL and INTEGER column affinities, a new OS interface layer
  design, and many other changes.  The code passed a regression
  test but should still be considered alpha.  Please report any
  problems.

  The file format for version 3.3.0 has changed slightly in order provide
  a more efficient encoding of binary values.  SQLite 3.3.0 will read and
  write legacy databases created with any prior version of SQLite 3.  But
  databases created by version 3.3.0 will not be readable or writable
  by earlier versions of the SQLite.  The older file format can be
  specified at compile-time for those rare cases where it is needed.
}

newsitem {2005-Dec-19} {Versions 3.2.8 and 2.8.17} {
  These versions contain one-line changes to 3.2.7 and 2.8.16 to fix a bug
  that has been present since March of 2002 and version 2.4.0.
  That bug might possibly cause database corruption if a large INSERT or
  UPDATE statement within a multi-statement transaction fails due to a
  uniqueness constraint but the containing transaction commits.
}


puts {
<p align="right"><a href="oldnews.html">Old news...</a></p>
</td></tr></table>
}
footer {$Id: index.tcl,v 1.133 2006/01/30 16:20:30 drh Exp $}

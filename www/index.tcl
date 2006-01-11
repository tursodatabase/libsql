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

newsitem {2005-Sep-24} {Version 3.2.7} {
  This version fixes several minor and obscure bugs.
  Upgrade only if you are having problems.
}

newsitem {2005-Sep-16} {Version 3.2.6 - Critical Bug Fix} {
  This version fixes a bug that can result in database
  corruption if a VACUUM of a 1 gibibyte or larger database fails
  (perhaps do to running out of disk space or an unexpected power loss)
  and is later rolled back.
  <p>
  Also in this release:
  The ORDER BY and GROUP BY processing was rewritten to use less memory.
  Support for COUNT(DISTINCT) was added.  The LIKE operator can now be
  used by the optimizer on columns with COLLATE NOCASE.
}

newsitem {2005-Aug-27} {Version 3.2.5} {
  This release fixes a few more lingering bugs in the new code.
  We expect that this release will be stable and ready for production use.
}

newsitem {2005-Aug-24} {Version 3.2.4} {
  This release fixes a bug in the new optimizer that can lead to segfaults
  when parsing very complex WHERE clauses.
}

newsitem {2005-Aug-21} {Version 3.2.3} {
  This release adds the <a href="lang_analyze.html">ANALYZE</a> command,
  the <a href="lang_expr.html">CAST</a> operator, and many
  very substantial improvements to the query optimizer.  See the
  <a href="changes.html#version_3_2_3">change log</a> for additional
  information.
}

newsitem {2005-Aug-2} {2005 Open Source Award for SQLite} {
  SQLite and its primary author D. Richard Hipp have been honored with
  a <a href="http://osdir.com/Article6677.phtml">2005 Open Source
  Award</a> from Google and O'Reilly.<br clear="right">
}


puts {
<p align="right"><a href="oldnews.html">Old news...</a></p>
</td></tr></table>
}
footer {$Id: index.tcl,v 1.129 2006/01/11 01:08:34 drh Exp $}

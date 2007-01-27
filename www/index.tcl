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

newsitem {2007-Jan-27} {Version 3.3.12} {
  The first published build of the previous version used the wrong
  set of source files.  Consequently, many people downloaded a build
  that was labeled as "3.3.11" but was really 3.3.10.  Version 3.3.12
  is released to clear up the ambiguity.  A couple more bugs have
  also been fixed and <a href="pragma.html#pragma_integrity_check">
  PRAGMA integrity_check</a> has been enhanced.
}

newsitem {2007-Jan-22} {Version 3.3.11} {
  Version 3.3.11 fixes for a few more problems in version 3.3.9 that
  version 3.3.10 failed to catch.  Upgrading is recommended.
}

newsitem {2007-Jan-9} {Version 3.3.10} {
  Version 3.3.10 fixes several bugs that were introduced by the previous
  release.  Upgrading is recommended.
}

newsitem {2007-Jan-4} {Version 3.3.9} {
  Version 3.3.9 fixes bugs that can lead to database corruption under
  obscure and difficult to reproduce circumstances.  See
  <a href="http://www.sqlite.org/cvstrac/wiki?p=DatabaseCorruption">
  DatabaseCorruption</a> in the
  <a href="http://www.sqlite.org/cvstrac/wiki">wiki</a> for details.
  This release also adds the new
  <a href="capi3ref.html#sqlite3_prepare_v2">sqlite3_prepare_v2()</a>
  API and includes important bug fixes in the command-line
  shell and enhancements to the query optimizer.  Upgrading is
  recommended.
}

newsitem {2006-Oct-9} {Version 3.3.8} {
  Version 3.3.8 adds support for full-text search using the 
  <a href="http://www.sqlite.org/cvstrac/wiki?p=FtsOne">FTS1
  module.</a>  There are also minor bug fixes.  Upgrade only if
  you want to try out the new full-text search capabilities or if
  you are having problems with 3.3.7.
}

puts {
<p align="right"><a href="oldnews.html">Old news...</a></p>
</td></tr></table>
}
footer {$Id: index.tcl,v 1.149 2007/01/27 13:38:26 drh Exp $}

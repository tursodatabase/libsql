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

newsitem {2007-Jan-9} {Version 3.3.10} {
  Version 3.3.10 fixes several bugs that were introduced by the previous
  release.  Upgrading is recommended.
}

newsitem {2007-Jan-4} {Version 3.3.9} {
  Version 3.3.9 fixes bugs that can lead to database corruption under
  obsure and difficult to reproduce circumstances.  See
  <a href="http://www.sqlite.org/cvstrac/wiki?p=DatabaseCorruption">
  DatabaseCorruption</a> in the
  <a href="http://www.sqlite.org/cvstrac/wiki">wiki</a> for details.
  This release also add the new
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

newsitem {2006-Aug-12} {Version 3.3.7} {
  Version 3.3.7 includes support for loadable extensions and virtual
  tables.  But both features are still considered "beta" and their
  APIs are subject to change in a future release.  This release is
  mostly to make available the minor bug fixes that have accumulated
  since 3.3.6.  Upgrading is not necessary.  Do so only if you encounter
  one of the obscure bugs that have been fixed or if you want to try
  out the new features.
}

newsitem {2006-Jun-19} {New Book About SQLite} {
  <a href="http://www.apress.com/book/bookDisplay.html?bID=10130">
  <i>The Definitive Guide to SQLite</i></a>, a new book by
  <a href="http://www.mikesclutter.com">Mike Owens</a>.
  is now available from <a href="http://www.apress.com">Apress</a>.
  The books covers the latest SQLite internals as well as
  the native C interface and bindings for PHP, Python,
  Perl, Ruby, Tcl, and Java.  Recommended.
}


puts {
<p align="right"><a href="oldnews.html">Old news...</a></p>
</td></tr></table>
}
footer {$Id: index.tcl,v 1.146 2007/01/10 13:32:43 drh Exp $}

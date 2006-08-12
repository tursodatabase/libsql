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

newsitem {2006-Jun-6} {Version 3.3.6} {
  Changes include improved tolerance for windows virus scanners
  and faster :memory: databases.  There are also fixes for several
  obscure bugs.  Upgrade if you are having problems.
}

newsitem {2006-Apr-5} {Version 3.3.5} {
  This release fixes many minor bugs and documentation typos and
  provides some minor new features and performance enhancements.
  Upgrade only if you are having problems or need one of the new features.
}


puts {
<p align="right"><a href="oldnews.html">Old news...</a></p>
</td></tr></table>
}
footer {$Id: index.tcl,v 1.142 2006/08/12 14:38:47 drh Exp $}

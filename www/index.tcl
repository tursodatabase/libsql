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
<li>Small code footprint: less than 30K lines of C code,
    less than 250KB code space (gcc on i486)</li>
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

newsitem {2005-Aug-28} {Version 3.2.5} {
  This release fixes a bug that causes DELETEs and UPDATEs to fail if
  they attempt to changes more than 40960 rows.
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
footer {$Id: index.tcl,v 1.120 2005/08/27 17:06:16 drh Exp $}

#!/usr/bin/tclsh
source common.tcl
header {SQLite home page}
puts {
<table width="100%" border="0" cellspacing="5">
<tr>
<td width="50%" valign="top">
<h2>About SQLite</h2>
<p>
SQLite is a small C library that implements a 
self-contained, embeddable,
zero-configuration SQL database engine.
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
<li>Simple, easy to use <a href="c_interface.html">API</a>.</li>
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

newsitem {2005-Feb-15} {Versions 2.8.16 and 3.1.2 Released} {
  A critical bug in the VACUUM command that can lead to database
  corruption has been fixed in both the 2.x branch and the main
  3.x line.  This bug has existed in all prior versions of SQLite.
  Even though it is unlikely you will ever encounter this bug,
  it is suggested that all users upgrade.  See
  <a href="http://www.sqlite.org/cvstrac/tktview?tn=1116">
  ticket #1116</a>. for additional information.

  Version 3.1.2 is also the first stable release of the 3.1
  series.  SQLite 3.1 features added support for correlated
  subqueries, autovacuum, autoincrement, ALTER TABLE, and
  other enhancements.  See the 
  <a href="www.sqlite.org/releasenotes310.html">release notes
  for version 3.1.0</a> for a detailed description of the
  changes available in the 3.1 series.
}

newsitem {2004-Nov-09} {SQLite at the 2004 International PHP Conference} {
  There was a talk on the architecture of SQLite and how to optimize
  SQLite queries at the 2004 International PHP Conference in Frankfurt,
  Germany.
  <a href="http://www.sqlite.org/php2004/page-001.html">
  Slides</a> from that talk are available.
}


newsitem {2004-Oct-10} {SQLite at the 11<sup><small>th</small></sup>
Annual Tcl/Tk Conference} {
  There will be a talk on the use of SQLite in Tcl/Tk at the
  11<sup><small>th</small></sup> Tcl/Tk Conference this week in
  New Orleans.  Visit <a href="http://www.tcl.tk/community/tcl2004/">
  http://www.tcl.tk/</a> for details.
  <a href="http://www.sqlite.org/tclconf2004/page-001.html">
  Slides</a> from the talk are available.
}
  

puts {
<p align="right"><a href="oldnews.html">Old news...</a></p>
</td></tr></table>
}
footer {$Id: index.tcl,v 1.106 2005/02/15 12:51:16 drh Exp $}

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
<li>ACID (Atomic, Consistent, Isolated, Durable) transactions.</li>
<li>Zero-configuration - no setup or administration needed.</li>
<li>Implements most of SQL92.
    (<a href="omitted.html">Features not supported</a>)</li>
<li>A complete database is stored in a single disk file.</li>
<li>Database files can be freely shared between machines with
    different byte orders.</li>
<li>Supports databases up to 2 terabytes (2^41 bytes) in size.</li>
<li>Small memory footprint: less than 30K lines of C code,
    less than 250KB code space (gcc on i486)</li>
<li><a href="speed.html">Faster</a> than other popular database
    engines for most common operations.</li>
<li>Simple, easy to use <a href="c_interface.html">API</a>.</li>
<li><a href="tclsqlite.html">TCL bindings</a> included.
    Bindings for many other languages 
    <a href="http://www.sqlite.org/cvstrac/wiki?p=SqliteWrappers">
    available separately.</a></li>
<li>Well-commented source code with over 90% test coverage.</li>
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

newsitem {2004-Jly-22} {Version 3.0.3 (beta)} {
  The second beta release of SQLite version 3.0 is now available.
  This new beta fixes many bugs and adds support for databases with
  varying page sizes.  The next 3.0 release will probably be called
  a final or stable release.

  Version 3.0 adds support for internationalization and a new
  more compact file format. 
  <a href="version3.html">Details.</a>
  The API and file format have been fixed since 3.0.2.  All
  regression tests pass (over 100000 tests) and the test suite
  exercises over 95% of the code.

  SQLite version 3.0 is made possible in part by AOL
  developers supporting and embracing great Open-Source Software.
}

newsitem {2004-Jly-22} {Version 2.8.15} {
  SQLite version 2.8.15 is a maintenance release for the version 2.8
  series.  Version 2.8 continues to be maintained with bug fixes, but
  no new features will be added to version 2.8.  All the changes in
  this release are minor.  If you are not having problems, there is
  there is no reason to upgrade.
}
  

puts {
<p align="right"><a href="oldnews.html">Old news...</a></p>
</td></tr></table>
}
footer {$Id: index.tcl,v 1.91 2004/07/22 19:06:32 drh Exp $}

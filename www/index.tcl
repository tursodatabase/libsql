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

newsitem {2004-Jun-30} {Version 3.0.2 (beta) Released} {
  The first beta release of SQLite version 3.0 is now available.
  Version 3.0 adds support for internationalization and a new
  more compact file format. 
  <a href="version3.html">Details.</a>
  As of this release, the API and file format are frozen.  All
  regression tests pass (over 100000 tests) and the test suite
  exercises over 95% of the code.

  SQLite version 3.0 is made possible in part by AOL
  developers supporting and embracing great Open-Source Software.
}
  

newsitem {2004-Jun-25} {Website hacked} {
  The www.sqlite.org website was hacked sometime around 2004-Jun-22
  because the lead SQLite developer failed to properly patch CVS.
  Evidence suggests that the attacker was unable to elevate privileges
  above user "cvs".  Nevertheless, as a precaution the entire website
  has been reconstructed from scratch on a fresh machine.  All services
  should be back to normal as of 2004-Jun-28.
}

puts {
<p align="right"><a href="oldnews.html">Old news...</a></p>
</td></tr></table>
}
footer {$Id: index.tcl,v 1.90 2004/06/30 22:35:07 drh Exp $}

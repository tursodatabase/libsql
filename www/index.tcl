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

newsitem {2004-Jun-18} {Version 3.0.0 (alpha) Released} {
  The first alpha release of SQLite version 3.0 is available for
  public review and comment.  Version 3.0 enhances internationalization support
  through the use of UTF-16 and user-defined text collating sequences.
  BLOBs can now be stored directly, without encoding.
  A new file format results in databases that are 25% smaller (depending
  on content).  The code is also a little faster.  In spite of the many
  new features, the library footprint is still less than 240KB
  (x86, gcc -O1).
  <a href="version3.html">Additional information</a>.

  Our intent is to freeze the file format and API on 2004-Jul-01.
  Users are encouraged to review and evaluate this alpha release carefully 
  and submit any feedback prior to that date.

  The 2.8 series of SQLite will continue to be supported with bug
  fixes for the foreseeable future.
}

newsitem {2004-Jun-09} {Version 2.8.14 Released} {
  SQLite version 2.8.14 is a patch release to the stable 2.8 series.
  There is no reason to upgrade if 2.8.13 is working ok for you.
  This is only a bug-fix release.  Most developement effort is
  going into version 3.0.0 which is due out soon.
}

puts {
<p align="right"><a href="oldnews.html">Old news...</a></p>
</td></tr></table>
}
footer {$Id: index.tcl,v 1.88 2004/06/17 19:04:17 drh Exp $}

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
<li>A zero-configuration - no DBA needed.</li>
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

newsitem {2004-May-31} {CVS Access Temporarily Suspended} {
  Anonymous access to the CVS repository will be suspended
  temporarily beginning on 2004-June-04
  so that the developers working on version 3.0 can have a free hand to
  implement features that a covered by a non-compete agreement.
  Access will be reenabled after the non-compete expires on 2004-June-17.
}

newsitem {2004-Apr-23} {Work Begins On SQLite Version 3} {
  Work has begun on version 3 of SQLite.  Version 3 is a major
  changes to both the C-language API and the underlying file format
  that will enable SQLite to better support internationalization.
  The first beta is schedule for release on 2004-July-01.

  Plans are to continue to support SQLite version 2.8 with
  bug fixes.  But all new development will occur in version 3.0.
}

puts {
</td></tr></table>
}
footer {$Id: index.tcl,v 1.83 2004/05/31 15:06:30 drh Exp $}

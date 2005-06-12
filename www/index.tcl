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

newsitem {2005-Jun-13} {Version 3.2.2} {
  This release includes numerous minor bug fixes, speed improvements,
  and code size reductions.  There is no reason to upgrade unless you
  are having problems or unless you just want to.
}

newsitem {2005-Mar-29} {Version 3.2.1} {
  This release fixes a memory allocation problem in the new
  <a href="lang_altertable.html">ALTER TABLE ADD COLUMN</a>
  command.
}

newsitem {2005-Mar-21} {Version 3.2.0} {
  The primary purpose for version 3.2.0 is to add support for
  <a href="lang_altertable.html">ALTER TABLE ADD COLUMN</a>.
  The new ADD COLUMN capability is made
  possible by AOL developers supporting and embracing great
  open-source software.  Thanks, AOL!

  Version 3.2.0 also fixes an obscure but serious bug that was discovered
  just prior to release.  If you have a multi-statement transaction and
  within that transaction an UPDATE or INSERT statement fails due to a
  constraint, then you try to rollback the whole transaction, the rollback
  might not work correctly.  See
  <a href="http://www.sqlite.org/cvstrac/tktview?tn=1171">Ticket #1171</a>
  for details.  Upgrading is recommended for all users.
}

newsitem {2005-Mar-16} {Version 3.1.6} {
  Version 3.1.6 fixes a critical bug that can cause database corruption
  when inserting rows into tables with around 125 columns. This bug was
  introduced in version 3.0.0.  See
  <a href="http://www.sqlite.org/cvstrac/tktview?tn=1163">Ticket #1163</a>
  for additional information.
}

newsitem {2005-Mar-11} {Versions 3.1.4 and 3.1.5 Released} {
  Version 3.1.4 fixes a critical bug that could cause database corruption
  if the autovacuum mode of version 3.1.0 is turned on (it is off by
  default) and a CREATE UNIQUE INDEX is executed within a transaction but
  fails because the indexed columns are not unique.  Anyone using the
  autovacuum feature and unique indices should upgrade.

  Version 3.1.5 adds the ability to disable
  the F_FULLFSYNC ioctl() in OS-X by setting "PRAGMA synchronous=on" instead
  of the default "PRAGMA synchronous=full".  There was an attempt to add
  this capability in 3.1.4 but it did not work due to a spelling error.
}

newsitem {2005-Feb-19} {Version 3.1.3 Released} {
  Version 3.1.3 cleans up some minor issues discovered in version 3.1.2.
}
  

puts {
<p align="right"><a href="oldnews.html">Old news...</a></p>
</td></tr></table>
}
footer {$Id: index.tcl,v 1.116 2005/06/12 22:23:40 drh Exp $}

#
# Run this Tcl script to generate the formatchng.html file.
#
set rcsid {$Id: formatchng.tcl,v 1.1 2001/12/22 19:27:41 drh Exp $ }

puts {<html>
<head>
  <title>File Format Changes in SQLite</title>
</head>
<body bgcolor=white>
<h1 align=center>
File Format Changes in SQLite
</h1>}
puts "<p align=center>
(This page was last modified on [lrange $rcsid 3 4] UTC)
</p>"

puts {
<p>
From time to time, enhancements or bug fixes require a change to
the underlying file format for SQLite.  When this happens and you
want to upgrade your library, you must convert the contents of your
databases into a portable ASCII representation using the old version
of the library then reload the data using the new version of the
library.
</p>

<p>
You can tell if you should reload your databases by comparing the
version numbers of the old and new libraries.  If either of the
first two digits in the version number change, then a reload is
either required or recommended.  For example, upgrading from
version 1.0.32 to 2.0.0 requires a reload.  So does going from
version 2.0.8 to 2.1.0.
</p>

<p>
The following table summarizes the SQLite file format changes that have
occurred since version 1.0.0:
</p>

<blockquote>
<table border=2 cellpadding=5>
<tr>
  <th>Version Change</th>
  <th>Approx. Date</th>
  <th>Description Of File Format Change</th>
</tr>
<tr>
  <td valign="top">1.0.32 to 2.0.0</td>
  <td valign="top">2001-Sep-20</td>
  <td>Version 1.0.X of SQLite used the GDBM library as its backend
  interface to the disk.  Beginning in version 2.0.0, GDBM was replaced
  by a custom B-Tree library written especially for SQLite.  The new
  B-Tree backend is twice as fast as GDBM, supports atomic commits and
  rollback, and stores an entire database in a single disk file instead
  using a separate file for each table as GDBM does.  The two
  file formats are not even remotely similar.</td>
</tr>
<tr>
  <td valign="top">2.0.8 to 2.1.0</td>
  <td valign="top">2001-Nov-12</td>
  <td>The same basic B-Tree format is used but the details of the 
  index keys were changed in order to provide better query 
  optimization opportunities.  Some of the headers were also changed in order
  to increase the maximum size of a row from 64KB to 24MB.</td>
</tr>
<tr>
  <td valign="top">2.1.7 to 2.2.0</td>
  <td valign="top">2001-Dec-21</td>
  <td>Beginning with version 2.2.0, SQLite no longer builds an index for
  an INTEGER PRIMARY KEY column.  Instead, it uses that column as the actual
  B-Tree key for the main table.<p>Version 2.2.0 and later of the library
  will automatically detect when it is reading a 2.1.x database and will
  disable the new INTEGER PRIMARY KEY feature.   In other words, version
  2.2.x is backwards compatible to version 2.1.x.  But version 2.1.x is not
  forward compatible with version 2.2.x. If you try to open
  a 2.2.x database with an older 2.1.x library and that database contains
  an INTEGER PRIMARY KEY, you will likely get a coredump.  If the database
  schema does not contain any INTEGER PRIMARY KEYs, then the version 2.1.x
  and version 2.2.x database files will be identical and completely
  interchangeable.</p>
</tr>
</table>
</blockquote>

<p>
To perform a database reload, have ready versions of the
<b>sqlite</b> command-line utility for both the old and new
version of SQLite.  Call these two executables "<b>sqlite-old</b>"
and "<b>sqlite-new</b>".  Suppose the name of your old database
is "<b>old.db</b>" and you want to create a new database with
the same information named "<b>new.db</b>".  The command to do
this is as follows:
</p>

<blockquote>
  echo .dump | sqlite-old old.db | sqlite-new new.db
</blockquote>
}

puts {
<p><hr /></p>
<p><a href="index.html"><img src="/goback.jpg" border=0 />
Back to the SQLite Home Page</a>
</p>

</body></html>}

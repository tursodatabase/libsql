#
# Run this Tcl script to generate the formatchng.html file.
#
set rcsid {$Id: formatchng.tcl,v 1.19 2006/08/12 14:38:47 drh Exp $ }
source common.tcl
header {File Format Changes in SQLite}
puts {
<h2>File Format Changes in SQLite</h2>

<p>
Every effort is made to keep SQLite fully backwards compatible from
one release to the next.  Rarely, however, some
enhancements or bug fixes may require a change to
the underlying file format.  When this happens and you
must convert the contents of your
databases into a portable ASCII representation using the old version
of the library then reload the data using the new version of the
library.
</p>

<p>
You can tell if you should reload your databases by comparing the
version numbers of the old and new libraries.  If the first digit
of the version number is different, then a reload of the database will
be required.  If the second digit changes, newer versions of SQLite
will be able to read and write older database files, but older versions
of the library may have difficulty reading or writing newer database
files.
For example, upgrading from
version 2.8.14 to 3.0.0 requires a reload.  Going from
version 3.0.8 to 3.1.0 is backwards compatible but not necessarily
forwards compatible.
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
  to increase the maximum size of a row from 64KB to 24MB.<p>

  This change is an exception to the version number rule described above
  in that it is neither forwards or backwards compatible.  A complete
  reload of the database is required.  This is the only exception.</td>
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
<tr>
  <td valign="top">2.2.5 to 2.3.0</td>
  <td valign="top">2002-Jan-30</td>
  <td>Beginning with version 2.3.0, SQLite supports some additional syntax
  (the "ON CONFLICT" clause) in the CREATE TABLE and CREATE INDEX statements
  that are stored in the SQLITE_MASTER table.  If you create a database that
  contains this new syntax, then try to read that database using version 2.2.5
  or earlier, the parser will not understand the new syntax and you will get
  an error.  Otherwise, databases for 2.2.x and 2.3.x are interchangeable.</td>
</tr>
<tr>
  <td valign="top">2.3.3 to 2.4.0</td>
  <td valign="top">2002-Mar-10</td>
  <td>Beginning with version 2.4.0, SQLite added support for views. 
  Information about views is stored in the SQLITE_MASTER table.  If an older
  version of SQLite attempts to read a database that contains VIEW information
  in the SQLITE_MASTER table, the parser will not understand the new syntax
  and initialization will fail.  Also, the
  way SQLite keeps track of unused disk blocks in the database file
  changed slightly.
  If an older version of SQLite attempts to write a database that
  was previously written by version 2.4.0 or later, then it may leak disk
  blocks.</td>
</tr>
<tr>
  <td valign="top">2.4.12 to 2.5.0</td>
  <td valign="top">2002-Jun-17</td>
  <td>Beginning with version 2.5.0, SQLite added support for triggers. 
  Information about triggers is stored in the SQLITE_MASTER table.  If an older
  version of SQLite attempts to read a database that contains a CREATE TRIGGER
  in the SQLITE_MASTER table, the parser will not understand the new syntax
  and initialization will fail.
  </td>
</tr>
<tr>
  <td valign="top">2.5.6 to 2.6.0</td>
  <td valign="top">2002-July-17</td>
  <td>A design flaw in the layout of indices required a file format change
  to correct.  This change appeared in version 2.6.0.<p>

  If you use version 2.6.0 or later of the library to open a database file
  that was originally created by version 2.5.6 or earlier, an attempt to
  rebuild the database into the new format will occur automatically.
  This can take some time for a large database.  (Allow 1 or 2 seconds
  per megabyte of database under Unix - longer under Windows.)  This format
  conversion is irreversible.  It is <strong>strongly</strong> suggested
  that you make a backup copy of older database files prior to opening them
  with version 2.6.0 or later of the library, in case there are errors in
  the format conversion logic.<p>

  Version 2.6.0 or later of the library cannot open read-only database
  files from version 2.5.6 or earlier, since read-only files cannot be
  upgraded to the new format.</p>
  </td>
</tr>
<tr>
  <td valign="top">2.6.3 to 2.7.0</td>
  <td valign="top">2002-Aug-13</td>
  <td><p>Beginning with version 2.7.0, SQLite understands two different
  datatypes: text and numeric.  Text data sorts in memcmp() order.
  Numeric data sorts in numerical order if it looks like a number,
  or in memcmp() order if it does not.</p>

  <p>When SQLite version 2.7.0 or later opens a 2.6.3 or earlier database,
  it assumes all columns of all tables have type "numeric".  For 2.7.0
  and later databases, columns have type "text" if their datatype
  string contains the substrings "char" or "clob" or "blob" or "text".
  Otherwise they are of type "numeric".</p>

  <p>Because "text" columns have a different sort order from numeric,
  indices on "text" columns occur in a different order for version
  2.7.0 and later database.  Hence version 2.6.3 and earlier of SQLite 
  will be unable to read a 2.7.0 or later database.  But version 2.7.0
  and later of SQLite will read earlier databases.</p>
  </td>
</tr>
<tr>
  <td valign="top">2.7.6 to 2.8.0</td>
  <td valign="top">2003-Feb-14</td>
  <td><p>Version 2.8.0 introduces a change to the format of the rollback
  journal file.  The main database file format is unchanged.  Versions
  2.7.6 and earlier can read and write 2.8.0 databases and vice versa.
  Version 2.8.0 can rollback a transaction that was started by version
  2.7.6 and earlier.  But version 2.7.6 and earlier cannot rollback a
  transaction started by version 2.8.0 or later.</p>

  <p>The only time this would ever be an issue is when you have a program
  using version 2.8.0 or later that crashes with an incomplete
  transaction, then you try to examine the database using version 2.7.6 or
  earlier.  The 2.7.6 code will not be able to read the journal file
  and thus will not be able to rollback the incomplete transaction
  to restore the database.</p>
  </td>
</tr>
<tr>
  <td valign="top">2.8.14 to 3.0.0</td>
  <td valign="top">2004-Jun-18</td>
  <td><p>Version 3.0.0 is a major upgrade for SQLite that incorporates
  support for UTF-16, BLOBs, and a more compact encoding that results
  in database files that are typically 25% to 50% smaller.  The new file
  format is very different and is completely incompatible with the
  version 2 file format.</p>
  </td>
</tr>
<tr>
  <td valign="top">3.0.8 to 3.1.0</td>
  <td valign="top">2005-Jan-21</td>
  <td><p>Version 3.1.0 adds support for
  <a href="pragma.html#pragma_auto_vacuum">autovacuum mode</a>.
  Prior versions of SQLite will be able to read an autovacuumed
  database but will not be able to write it.  If autovaccum is disabled
  (which is the default condition)
  then databases are fully forwards and backwards compatible.</p>
  </td>
</tr>
<tr>
  <td valign="top">3.1.6 to 3.2.0</td>
  <td valign="top">2005-Mar-19</td>
  <td><p>Version 3.2.0 adds support for the 
  <a href="lang_altertable.html">ALTER TABLE ADD COLUMN</a>
  command.  A database that has been modified by this command can
  not be read by a version of SQLite prior to 3.1.4.  Running 
  <a href="lang_vacuum.html">VACUUM</a>
  after the ALTER TABLE
  restores the database to a format such that it can be read by earlier
  SQLite versions.</p>
  </td>
</tr>
<tr>
  <td valign="top">3.2.8 to 3.3.0</td>
  <td valign="top">2006-Jan-10</td>
  <td><p>Version 3.3.0 adds support for descending indices and
  uses a new encoding for boolean values that requires
  less disk space.  Version 3.3.0 can read and write database
  files created by prior versions of SQLite.  But prior versions
  of SQLite will not be able to read or write databases created
  by Version 3.3.0</p>
  <p>If you need backwards and forwards capatibility, you can
  compile with -DSQLITE_DEFAULT_FILE_FORMAT=1.  Or at runtime
  you can say "PRAGMA legacy_file_format=ON" prior to creating
  a new database file</p>
  <p>Once a database file is created, its format is fixed.  So
  a database file created by SQLite 3.2.8 and merely modified
  by version 3.3.0 or later will retain the old format.  Except,
  the VACUUM command recreates the database so running VACUUM
  on 3.3.0 or later will change the file format to the latest
  edition.</p>
  </td>
</tr>
<tr>
  <td valign="top">3.3.6 to 3.3.7</td>
  <td valign="top">2006-Aug-12</td>
  <td><p>The previous file format change has caused so much
  grief that the default behavior has been changed back to 
  the original file format.  This means that DESC option on
  indices is ignored by default that the more efficient encoding
  of boolean values is not used.  In that way, older versions
  of SQLite can read and write databases created by newer
  versions.  If the new features are desired, they can be
  enabled using pragma: "PRAGMA legacy_file_format=OFF".</p>
  <p>To be clear: both old and new file formats continue to
  be understood and continue to work.  But the old file format
  is used by default instead of the new.  This might change
  again in some future release - we may go back to generating
  the new file format by default - but probably not until
  all users have upgraded to a version of SQLite that will
  understand the new file format.  That might take several
  years.</p></td>
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
  sqlite-old old.db .dump | sqlite-new new.db
</blockquote>
}
footer $rcsid

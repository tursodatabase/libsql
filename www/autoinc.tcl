#
# Run this Tcl script to generate the autoinc.html file.
#
set rcsid {$Id: }
source common.tcl

if {[llength $argv]>0} {
  set outputdir [lindex $argv 0]
} else {
  set outputdir ""
}

header {SQLite Autoincrement}
puts {
<h1>SQLite Autoincrement</h1>

<p>
In SQLite, every row of every table has an integer ROWID.
The ROWID for each row is unique among all rows in the same table.
In SQLite version 2.8 the ROWID is a 32-bit signed integer.
Version 3.0 of SQLite expanded the ROWID to be a 64-bit signed integer.
</p>

<p>
You can access the ROWID of an SQLite table using one the special column
names ROWID, _ROWID_, or OID.
Except if you declare an ordinary table column to use one of those special
names, then the use of that name will refer to the declared column not
to the internal ROWID.
</p>

<p>
If a table contains a column of type INTEGER PRIMARY KEY, then that
column becomes an alias for the ROWID.  You can then access the ROWID
using any of four different names, the original three names described above
or the name given to the INTEGER PRIMARY KEY column.  All these names are
aliases for one another and work equally well in any context.
</p>

<p>
When a new row is inserted into an SQLite table, the ROWID can either
be specified as part of the INSERT statement or it can be assigned
automatically by the database engine.  To specify a ROWID manually,
just include it in the list of values to be inserted.  For example:
</p>

<blockquote><pre>
CREATE TABLE test1(a INT, b TEXT);
INSERT INTO test1(rowid, a, b) VALUES(123, 5, 'hello');
</pre></blockquote>

<p>
If no ROWID is specified on the insert, an appropriate ROWID is created
automatically.  The usual algorithm is to give the newly created row
a ROWID that is one larger than the largest ROWID in the table prior
to the insert.  If the table is initially empty, then a ROWID of 1 is
used.  If the largest ROWID is equal to the largest possible integer
(9223372036854775807 in SQLite version 3.0 and later) then the database
engine starts picking candidate ROWIDs at random until it finds one
that is not previously used.
</p>

<p>
The normal ROWID selection algorithm described above
will generate monotonically increasing
unique ROWIDs as long as you never use the maximum ROWID value and you never
delete the entry in the table with the largest ROWID. 
If you ever delete rows or if you ever create a row with the maximum possible
ROWID, then ROWIDs from previously deleted rows might be reused when creating
new rows and newly created ROWIDs might not be in strictly accending order.
</p>


<h2>The AUTOINCREMENT Keyword</h2>

<p>
If a column has the type INTEGER PRIMARY KEY AUTOINCREMENT then a slightly
different ROWID selection algorithm is used.  
The ROWID chosen for the new row is one larger than the largest ROWID
that has ever before existed in that same table.  If the table has never
before contained any data, then a ROWID of 1 is used.  If the table
has previously held a row with the largest possible ROWID, then new INSERTs
are not allowed and any attempt to insert a new row will fail with an
SQLITE_FULL error.
</p>

<p>
SQLite keeps track of the largest ROWID that a table has ever held using
the special SQLITE_SEQUENCE table.  The SQLITE_SEQUENCE table is created
and initialized automatically whenever a normal table that contains an
AUTOINCREMENT column is created.  The content of the SQLITE_SEQUENCE table
can be modified using ordinary UPDATE, INSERT, and DELETE statements.
But making modifications to this table will likely perturb the AUTOINCREMENT
key generation algorithm.  Make sure you know what you are doing before
you undertake such changes.
</p>

<p>
The behavior implemented by the AUTOINCREMENT keyword is subtly different
from the default behavior.  With AUTOINCREMENT, rows with automatically
selected ROWIDs are guaranteed to have ROWIDs that have never been used
before by the same table in the same database.  And the automatically generated
ROWIDs are guaranteed to be monotonically increasing.  These are important
properties in certain applications.  But if your application does not
need these properties, you should probably stay with the default behavior
since the use of AUTOINCREMENT requires additional work to be done
as each row is inserted and thus causes INSERTs to run a little slower.
}
footer $rcsid

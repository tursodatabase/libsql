#
# Run this Tcl script to generate the constraint.html file.
#
set rcsid {$Id: conflict.tcl,v 1.2 2002/02/03 00:56:11 drh Exp $ }

puts {<html>
<head>
  <title>Constraint Conflict Resolution in SQLite</title>
</head>
<body bgcolor=white>
<h1 align=center>
Constraint Conflict Resolution in SQLite
</h1>}
puts "<p align=center>
(This page was last modified on [lrange $rcsid 3 4] UTC)
</p>"

puts {
<h2>Introduction</h2>

<p>
In most SQL databases, if you have a UNIQUE constraint on
a table and you try to do an UPDATE or INSERT that violates
that constraint, the database will aborts the operation in
progress, back out any prior changes associated with that 
one UPDATE or INSERT command, and return an error.
This is the default behavior of SQLite.
Beginning with version 2.3.0, though, SQLite allows you to
define alternative ways for dealing with constraint violations.
This article describes those alternatives and how to use them.
</p>

<h2>Conflict Resolution Algorithms</h2>

<p>
SQLite defines five constraint conflict resolution algorithms
as follows:
</p>

<dl>
<dt><b>ROLLBACK</b></dt>
<dd><p>When a constraint violation occurs, an immediate ROLLBACK
occurs, thus ending the current transaction, and the command aborts
with a return code of SQLITE_CONSTRAINT.  If no transaction is
active (other than the implied transaction that is created on every
command) then this algorithm works the same as ABORT.</p></dd>

<dt><b>ABORT</b></dt>
<dd><p>When a constraint violation occurs, the command backs out
any prior changes it might have made and aborts with a return code
of SQLITE_CONSTRAINT.  But no ROLLBACK is executed so changes
from prior commands within the same transaction
are preserved.  This is the default behavior for SQLite.</p></dd>

<dt><b>FAIL</b></dt>
<dd><p>When a constraint violation occurs, the command aborts with a
return code SQLITE_CONSTRAINT.  But any changes to the database that
the command made prior to encountering the constraint violation
are preserved and are not backed out.  For example, if an UPDATE
statement encountered a constraint violation on the 100th row that
it attempts to update, then the first 99 row changes are preserved
by change to rows 100 and beyond never occur.</p></dd>

<dt><b>IGNORE</b></dt>
<dd><p>When a constraint violation occurs, the one row that contains
the constraint violation is not inserted or changed.  But the command
continues executing normally.  Other rows before and after the row that
contained the constraint violation continue to be inserted or updated
normally.  No error is returned.</p></dd>

<dt><b>REPLACE</b></dt>
<dd><p>When a UNIQUE constraint violation occurs, the pre-existing row
that caused the constraint violation is removed prior to inserting
or updating the current row.  Thus the insert or update always occurs.
The command continues executing normally.  No error is returned.</p></dd>
</dl>

<h2>Why So Many Choices?</h2>

<p>SQLite provides multiple conflict resolution algorithms for a
couple of reasons.  First, SQLite tries to be roughly compatible with as
many other SQL databases as possible, but different SQL database
engines exhibit different conflict resolution strategies.  For
example, PostgreSQL always uses ROLLBACK, Oracle always uses ABORT, and
MySQL usually uses FAIL but can be instructed to use IGNORE or REPLACE.
By supporting all five alternatives, SQLite provides maximum
portability.</p>

<p>Another reason for supporing multiple algorithms is that sometimes
it is useful to use an algorithm other than the default.
Suppose, for example, you are
inserting 1000 records into a database, all within a single
transaction, but one of those records is malformed and causes
a constraint error.  Under PostgreSQL or Oracle, none of the
1000 records would get inserted.  In MySQL, some subset of the
records that appeared before the malformed record would be inserted
but the rest would not.  Neither behavior is espeically helpful.
What you really want is to use the IGNORE algorithm to insert
all but the malformed record.</p>

}

puts {
<p><hr /></p>
<p><a href="index.html"><img src="/goback.jpg" border=0 />
Back to the SQLite Home Page</a>
</p>

</body></html>}

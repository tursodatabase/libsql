#
# Run this Tcl script to generate the constraint.html file.
#
set rcsid {$Id: conflict.tcl,v 1.1 2002/01/30 16:17:25 drh Exp $ }

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
progress and rolls back the current transaction.
This is the default behavior of SQLite.
Beginning with version 2.3.0, though, SQLite allows you to
define alternative ways for dealing with constraint violations.
This article describes those alternatives and how to use them.
</p>

<h2>Conflict Resolution Algorithms</h2>

<p>
The default conflict resolution algorithm is to abort the
operation in progress, rollback all changes, and cancel the
current transaction.  Call this algorithm "ABORT".  Abort
is the standard way of dealing with a constraint error
in most SQL databases.
</p>

<p>
Sometimes ABORT is not the most helpful way of dealing
with constraint violations.  Suppose, for example, you are
inserting 1000 records into a database, all within a single
transaction, but one of those records is malformed and causes
a constraint error.  With the default ABORT behavior, none
of the 1000 records gets inserted.  But sometimes it is 
desirable to just omit the single malformed insert and
finish the other 999.
</p>

<p>
SQLite defines two addition conflict resolution algorithms
called "IGNORE" and "REPLACE".  
If you are trying to do multiple INSERTs or UPDATEs when a constraint
fails for a single row and the conflict behavior is IGNORE, then
that row remains uninserted or unmodified.  But the overall operation
is not aborted and no rollback occurs.  If a constraint
fails and the behavior is REPLACE, then SQLite tries to
delete other rows in the table in order to eliminate the
constraint problem.  Again, the overall operation continues
and no rollback occurs.
</p>

<p>
The default conflict resolution algorithm is always ABORT
but you can specify an alternative algorithm using special
(non-standard) syntax on the INSERT and UPDATE commands.
You can add the clause "ON CONFLICT <algorithm>" immediately
after the "INSERT" or "UPDATE" keywords to specify the 
conflict resolution algorithm to use for that one operation.
(Substitute "ABORT", "IGNORE", or "REPLACE" for <algorithm>,
of course.)
</p>

Consider this example:

<blockquote><pre>
   BEGIN;
   CREATE TABLE t1(
      a INTEGER,
      b INTEGER,
      c INTEGER,
      UNIQUE(a,b)
   );
   INSERT INTO a VALUES(1,2,3);
   COMMIT;

   BEGIN;
   INSERT INTO a VALUES(2,3,4);
   INSERT INTO a VALUES(1,2,5);
</pre></blockquote>

<p>
In the last instruction, the UNIQUE constraint fails
and the entire transaction is rolled back.  The database
now contains a single entry: {1,2,3}.  
</p>

<blockquote><pre>
   BEGIN;
   INSERT ON CONFLICT IGNORE INTO a VALUES(2,3,4);
   INSERT ON CONFLICT IGNORE INTO a VALUES(1,2,5);
   COMMIT;
</pre></blockquote>

<p>This time the "ON CONFLICT IGNORE" clause tells SQLite to use
IGNORE semantics when a constraint fails.  The second
INSERT statement fails, but the database is
not rolled back and there is no failure.  The database
now contains two rows:  {1,2,3} and {2,3,4}.</p>

<blockquote><pre>
   BEGIN;
   INSERT ON CONFLICT REPLACE INTO a VALUES(1,2,5);
   COMMIT;
</pre></blockquote>

<p>Here the "ON CONFLICT REPLACE" clause tells SQLite to use REPLACE
semantics.  The {1,2,3} is deleted when the {1,2,5} row
is inserted in order to satisfy the constraint.  After
the above, the database contains {1,2,5} and {2,3,4}.</p>

<h2>A Syntactic Shortcut</h2>

<p>On an INSERT, the "ON CONFLICT" keywords may be omitted for brevity.
So you can say</p>

<blockquote><pre>
   INSERT IGNORE INTO a VALUES(1,2,5);
</pre></blockquote>

<p>Instead of the more wordy:</p>

<blockquote><pre>
   INSERT ON CONFLICT IGNORE INTO a VALUES(1,2,5);
</pre></blockquote>

<p>Unfortunately, you cannot do this with an UPDATE.</p>

<h2>Changing The Default Conflict Resolution Algorithm</h2>

<p>You can change the default conflict resolution algorithm
on a constraint-by-constraint basis using special (non-standard)
syntax in CREATE TABLE and CREATE INDEX statements.  The
same "ON CONFLICT" clause that appears in INSERT and UPDATE
statements is used but the clause is attached to the constraint
in the CREATE TABLE statement.  Like this:

<blockquote><pre>
   CREATE TABLE t1 (
     a INTEGER,
     b INTEGER,
     c INTEGER,
     UNIQUE(a,b) ON CONFLICT REPLACE
   );
</pre></blockquote>

<p>The ON CONFLICT clause in the above table definition says that
the default conflict resolution algorithm is REPLACE instead
of ABORT.  REPLACE will always be used unless you override
this by saying "INSERT IGNORE" or "INSERT ABORT".</p>

<p>The ON CONFLICT clause can also appear on a NOT NULL constraint,
a PRIMARY KEY constraint, and a CHECK constraint.
(Note, however, that CHECK constraints are not currently enforced
so the ON CONFLICT clause has no effect there.)</p>

<p>A NOT NULL constraint will normally ABORT if you try to insert
a NULL.  But if you substitute the REPLACE algorithm, it tries to insert
the default value in place of the NULL.  If there is no default value,
then REPLACE is the same as ABORT for NOT NULL constraints.
With the IGNORE algorithm on a NOT NULL, the INSERT or UPDATE 
is suppressed if the value is NULL.</p>

<h2>Portability</h2>

<p>The ON CONFLICT syntax is not standard SQL and will not
(as far as is known) work on any other database product.  Furthermore,
the syntax might change in future versions of SQLite.  So use it
with appropriate discretion.</p>
}

puts {
<p><hr /></p>
<p><a href="index.html"><img src="/goback.jpg" border=0 />
Back to the SQLite Home Page</a>
</p>

</body></html>}

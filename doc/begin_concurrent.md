
Begin Concurrent
================

## Overview

Usually, SQLite allows at most one writer to proceed concurrently. The
BEGIN CONCURRENT enhancement allows multiple writers to process write
transactions simultanously if the database is in "wal" or "wal2" mode,
although the system still serializes COMMIT commands.

When a write-transaction is opened with "BEGIN CONCURRENT", actually 
locking the database is deferred until a COMMIT is executed. This means
that any number of transactions started with BEGIN CONCURRENT may proceed
concurrently. The system uses optimistic page-level-locking to prevent
conflicting concurrent transactions from being committed.

When a BEGIN CONCURRENT transaction is committed, the system checks whether 
or not any of the database pages that the transaction has read have been
modified since the BEGIN CONCURRENT was opened. In other words - it asks 
if the transaction being committed operates on a different set of data than
all other concurrently executing transactions. If the answer is "yes, this
transaction did not read or modify any data modified by any concurrent
transaction", then the transaction is committed as normal. Otherwise, if the
transaction does conflict, it cannot be committed and an SQLITE&#95;BUSY&#95;SNAPSHOT
error is returned. At this point, all the client can do is ROLLBACK the
transaction.

If SQLITE&#95;BUSY&#95;SNAPSHOT is returned, messages are output via the sqlite3&#95;log
mechanism indicating the page and table or index on which the conflict
occurred. This can be useful when optimizing concurrency.

## Application Programming Notes

In order to serialize COMMIT processing, SQLite takes a lock on the database
as part of each COMMIT command and releases it before returning. At most one
writer may hold this lock at any one time. If a writer cannot obtain the lock,
it uses SQLite's busy-handler to pause and retry for a while:

  <a href=https://www.sqlite.org/c3ref/busy_handler.html>
      https://www.sqlite.org/c3ref/busy_handler.html
  </a>

If there is significant contention for the writer lock, this mechanism can be
inefficient. In this case it is better for the application to use a mutex or
some other mechanism that supports blocking to ensure that at most one writer
is attempting to COMMIT a BEGIN CONCURRENT transaction at a time. This is
usually easier if all writers are part of the same operating system process.

If all database clients (readers and writers) are located in the same OS
process, and if that OS is a Unix variant, then it can be more efficient to
the built-in VFS "unix-excl" instead of the default "unix". This is because it
uses more efficient locking primitives.

The key to maximizing concurrency using BEGIN CONCURRENT is to ensure that
there are a large number of non-conflicting transactions. In SQLite, each
table and each index is stored as a separate b-tree, each of which is
distributed over a discrete set of database pages. This means that:

  * Two transactions that write to different sets of tables never 
    conflict, and that

  * Two transactions that write to the same tables or indexes only 
    conflict if the values of the keys (either primary keys or indexed 
    rows) are fairly close together. For example, given a large 
    table with the schema:

      <pre>     CREATE TABLE t1(a INTEGER PRIMARY KEY, b BLOB);</pre>

    writing two rows with adjacent values for "a" probably will cause a
    conflict (as the two keys are stored on the same page), but writing two
    rows with vastly different values for "a" will not (as the keys will likly
    be stored on different pages).

Note that, in SQLite, if values are not explicitly supplied for an INTEGER
PRIMARY KEY, as for example in:

>
     INSERT INTO t1(b) VALUES(&lt;blob-value>);

then monotonically increasing values are assigned automatically. This is
terrible for concurrency, as it all but ensures that all new rows are 
added to the same database page. In such situations, it is better to
explicitly assign random values to INTEGER PRIMARY KEY fields.

This problem also comes up for non-WITHOUT ROWID tables that do not have an
explicit INTEGER PRIMARY KEY column. In these cases each table has an implicit
INTEGER PRIMARY KEY column that is assigned increasing values, leading to the
same problem as omitting to assign a value to an explicit INTEGER PRIMARY KEY
column.

For both explicit and implicit INTEGER PRIMARY KEYs, it is possible to have
SQLite assign values at random (instead of the monotonically increasing
values) by writing a row with a rowid equal to the largest possible signed
64-bit integer to the table. For example:

     INSERT INTO t1(a) VALUES(9223372036854775807);

Applications should take care not to malfunction due to the presence of such
rows.

The nature of some types of indexes, for example indexes on timestamp fields,
can also cause problems (as concurrent transactions may assign similar
timestamps that will be stored on the same db page to new records). In these
cases the database schema may need to be rethought to increase the concurrency
provided by page-level-locking.


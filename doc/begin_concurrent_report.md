
Begin Concurrent Report
=======================

## Overview

<a href=../../../timeline?r=begin-concurrent-report>
This patch</a> enhances the library so that it may provide a report on each
transaction that uses "BEGIN CONCURRENT". The report includes the following
details:

  * Whether or not the transaction was successfully committed.

  * A list of database pages modified (if the transaction was successfully
    committed).

  * A list of database pages read from the db by the transaction..

  * A copy of each tuple written, overwritten or deleted from the database
    by the transaction.

  * A list of ranges of index keys scanned by the transaction.

Only operations on the main database are reported on. Reads and writes of 
attached or temp databases are not included in reports.

The second and third points above describe the data that the
optimistic-concurrency page-locking system currently uses to determine at
COMMIT time if a BEGIN CONCURRENT transaction conflicts with any other
transaction that executed concurrently. The fourth and fifth points represent
the data that we could use to identify conflicts in a row-locking system.

Using this patch we can gather a series of reports from a real installation,
then by analyzing those reports offline determine whether or not concurrency
would have been increased if the system supported row-locking instead of page
locking.

## Programming

Capturing the data required for the reports is disabled by default. It is
enabled or disabled on a per-connection basis. To enable capturing of
report data, call the sqlite3\_begin\_concurrent\_report() function with the
bEnable argument set to non-zero. To disable it, call the same function with
bEnable set to zero. If capturing report data is enabled or disabled in the
middle of a transaction, the report for that transaction may be incomplete.

<pre>
  void sqlite3_begin_concurrent_report_enable(sqlite3 *db, int bEnable);
</pre>

The following API returns a pointer to the full text of the report for the 
most recently completed BEGIN CONCURRENT transaction. The API may be called
at any point after the BEGIN CONCURRENT transaction is committed or rolled
back until the next invocation of the command "BEGIN CONCURRENT". The returned
pointer is valid until either the database handle is closed or the
sqlite3\_begin\_concurrent\_report() is called again on the same database
handle.

<pre>
  const char *sqlite3_begin_concurrent_report(sqlite3 *db);
</pre>


## Report Format

The report is broken into lines separated by "\\n" characters.

If a transaction is successfully committed, the report contains entries
for the set of pages read by the transaction and the set of pages written.
For example:

<pre>
  R:{3 4 8 10}
  W:{4 8}
</pre>

indicates that the transaction read pages 3, 4, 8 and 10, and wrote to pages
4 and 8. 

If a transaction cannot be committed due to conflicts, then its report
contains only a list of database pages written. As follows:

<pre>
  F:{3 4 8 10}
</pre>

For each row written, overwritten or deleted from the database, the report
contains an entry similar to the following:

<pre>
  3<-(44){NULL,2,'three'}
</pre>

which means that a record with rowid 44 was written (or overwritten or deleted)
to the table with root page 3. The values in the tuple are NULL, 2 and 'three'.
The rowid value is always 0 for WITHOUT ROWID tables.

Ranges scanned by the transaction are represented by lines similar to the
following:

<pre>
  4:[{'one'}..{'three'})
  4:({'three'}..EOF)
  3:[44..44]
</pre>

Assuming that page 3 is the root page of table t1, and page 4 is the root page
of an index on column "c", the lines above might be produced by SQL similar to:

<pre>
  SELECT * FROM t1 WHERE c>='one' AND c<'three';
  SELECT * FROM t1 WHERE c>'three';
  SELECT * FROM t1 WHERE rowid=44;
</pre>




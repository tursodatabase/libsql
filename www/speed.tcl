#
# Run this Tcl script to generate the speed.html file.
#
set rcsid {$Id: speed.tcl,v 1.3 2001/09/28 23:11:24 drh Exp $ }

puts {<html>
<head>
  <title>Database Speed Comparison: SQLite versus PostgreSQL</title>
</head>
<body bgcolor=white>
<h1 align=center>
Database Speed Comparison
</h1>}
puts "<p align=center>
(This page was last modified on [lrange $rcsid 3 4] GMT)
</p>"

puts {
<h2>Executive Summary</h2>

<p>A series of tests are run to measure the relative performance of
SQLite version 1.0 and 2.0 and PostgreSQL version 6.4.
The following are general
conclusions drawn from these experiments:
</p>

<ul>
<li><p>
  SQLite 2.0 is significantly faster than both SQLite 1.0 and PostgreSQL
  for most common operations.
  SQLite 2.0 is over 4 times faster than PostgreSQL for simple
  query operations and about 7 times faster for <b>INSERT</b> statements 
  within a transaction.
</p></li>
<li><p>
  PostgreSQL performs better on complex queries, possibly due to having
  a more sophisticated query optimizer.
</p></li>
<li><p>
  SQLite 2.0 is significantly slower than both SQLite 1.0 and PostgreSQL
  on <b>DROP TABLE</b> statements and on doing lots of small <b>INSERT</b>
  statements that are not grouped into a single transaction.
</p></li>
</ul>

<h2>Test Environment</h2>

<p>
The platform used for these tests is a 550MHz Athlon with 256MB or memory
and 33MHz IDE disk drives.  The operating system is RedHat Linux 6.0 with
various upgrades, including an upgrade to kernel version 2.2.18.
</p>

<p>
PostgreSQL version 6.4.2 was used for these tests because that is what
came pre-installed with RedHat 6.0.  Newer version of PostgreSQL may give
better performance.
</p>

<p>
SQLite version 1.0.32 was compiled with -O2 optimization and without
the -DNDEBUG=1 switch.  Setting the NDEBUG macro disables all "assert()"
statements within the code, but SQLite version 1.0 does not have any
expensive assert() statements so the difference in performance is
negligible.
</p>

<p>
SQLite version 2.0-alpha-2 was compiled with -O2 optimization and
with the -DNDEBUG=1 compiler switch.  Setting the NDEBUG macro is very
important in SQLite version 2.0.  SQLite 2.0 contains some expensive
"assert()" statements in the inner loop of its processing.  Setting
the NDEBUG macro makes SQLite 2.0 run nearly twice as fast.
</p>

<p>
All tests are conducted on an otherwise quiescent machine.
A simple shell script was used to generate and run all the tests.
Each test reports three different times:
</p>

<p>
<ol>
<li> "<b>Real</b>" or wall-clock time. </li>
<li> "<b>User</b>" time, the time spent executing user-level code. </li>
<li> "<b>Sys</b>" or system time, the time spent in the operating system. </li>
</ol>
</p>

<p>
PostgreSQL uses a client-server model.  The experiment is unable to measure
CPU used by the server, only the client, so the "user" and "sys" numbers
from PostgreSQL are meaningless.
</p>

<h2>Test 1: CREATE TABLE</h2>

<blockquote><pre>
CREATE TABLE t1(f1 int, f2 int, f3 int);
COPY t1 FROM '/home/drh/sqlite/bld/speeddata3.txt';

PostgreSQL:   real   1.84
SQLite 1.0:   real   3.29   user   0.64   sys   1.60
SQLite 2.0:   real   0.77   user   0.51   sys   0.05
</pre></blockquote>

<p>
The speeddata3.txt data file contains 30000 rows of data.
</p>

<h2>Test 2: SELECT</h2>

<blockquote><pre>
SELECT max(f2), min(f3), count(*) FROM t1
WHERE f3<10000 OR f1>=20000;

PostgreSQL:   real   1.22
SQLite 1.0:   real   0.80   user   0.67   sys   0.12
SQLite 2.0:   real   0.65   user   0.60   sys   0.05
</pre></blockquote>

<p>
With no indices, a complete scan of the table must be performed
(all 30000 rows) in order to complete this query.
</p>

<h2>Test 3: CREATE INDEX</h2>

<blockquote><pre>
CREATE INDEX idx1 ON t1(f1);
CREATE INDEX idx2 ON t1(f2,f3);

PostgreSQL:   real   2.24
SQLite 1.0:   real   5.37   user   1.22   sys   3.10
SQLite 2.0:   real   3.71   user   2.31   sys   1.06
</pre></blockquote>

<p>
PostgreSQL is fastest at creating new indices.
Note that SQLite 2.0 is faster than SQLite 1.0 but still
spends longer in user-space code.
</p>

<h2>Test 4: SELECT using an index</h2>

<blockquote><pre>
SELECT max(f2), min(f3), count(*) FROM t1
WHERE f3<10000 OR f1>=20000;

PostgreSQL:   real   0.19
SQLite 1.0:   real   0.77   user   0.66   sys   0.12
SQLite 2.0:   real   0.62   user   0.62   sys   0.01
</pre></blockquote>

<p>
This is the same query as in Test 2, but now there are indices.
Unfortunately, SQLite is reasonably simple-minded about its querying
and not able to take advantage of the indices.  It still does a
linear scan of the entire table.  PostgreSQL, on the other hand,
is able to use the indices to make its query over six times faster.
</p>

<h2>Test 5: SELECT a single record</h2>

<blockquote><pre>
SELECT f2, f3 FROM t1 WHERE f1==1;
SELECT f2, f3 FROM t1 WHERE f1==2;
SELECT f2, f3 FROM t1 WHERE f1==3;
...
SELECT f2, f3 FROM t1 WHERE f1==998;
SELECT f2, f3 FROM t1 WHERE f1==999;
SELECT f2, f3 FROM t1 WHERE f1==1000;

PostgreSQL:   real   0.95
SQLite 1.0:   real  15.70   user   0.70   sys  14.41
SQLite 2.0:   real   0.20   user   0.15   sys   0.05
</pre></blockquote>

<p>
This test involves 1000 separate SELECT statements, only the first
and last three of which are show above.  SQLite 2.0 is the clear
winner.  The miserable showing by SQLite 1.0 is due (it is thought)
to the high overhead of executing <b>gdbm_open</b> 2000 times in
quick succession.
</p>

<h2>Test 6: UPDATE</h2>

<blockquote><pre>
UPDATE t1 SET f2=f3, f3=f2
WHERE f1 BETWEEN 15000 AND 20000;

PostgreSQL:   real   6.56
SQLite 1.0:   real   3.54   user   0.74   sys   1.16
SQLite 2.0:   real   2.70   user   0.70   sys   1.25
</pre></blockquote>

<p>
We have no explanation for why PostgreSQL does poorly here.
</p>

<h2>Test 7: INSERT from a SELECT</h2>

<blockquote><pre>
CREATE TABLE t2(f1 int, f2 int);
INSERT INTO t2 SELECT f1, f2 FROM t1 WHERE f3<10000;

PostgreSQL:   real   2.05
SQLite 1.0:   real   1.80   user   0.81   sys   0.73
SQLite 2.0:   real   0.69   user   0.58   sys   0.07
</pre></blockquote>


<h2>Test 8: Many small INSERTs</h2>

<blockquote><pre>
CREATE TABLE t3(f1 int, f2 int, f3 int);
INSERT INTO t3 VALUES(1,1641,1019);
INSERT INTO t3 VALUES(2,984,477);
...
INSERT INTO t3 VALUES(998,1411,1392);
INSERT INTO t3 VALUES(999,1715,526);
INSERT INTO t3 VALUES(1000,1906,1037);

PostgreSQL:   real   5.28
SQLite 1.0:   real   2.20   user   0.21   sys   0.67
SQLite 2.0:   real  10.99   user   0.21   sys   7.02
</pre></blockquote>

<p>
This test involves 1000 separate INSERT statements, only 5 of which
are shown above.  SQLite 2.0 does poorly because of its atomic commit
logic.  A minimum of two calls to <b>fsync()</b> are required for each
INSERT statement, and that really slows things down.  On the other hand,
PostgreSQL also has to support atomic commits and it seems to do so
efficiently.
</p>

<h2>Test 9: Many small INSERTs within a TRANSACTION</h2>

<blockquote><pre>
CREATE TABLE t4(f1 int, f2 int, f3 int);
BEGIN TRANSACTION;
INSERT INTO t4 VALUES(1,440,1084);
...
INSERT INTO t4 VALUES(999,1527,423);
INSERT INTO t4 VALUES(1000,74,1865);
COMMIT;

PostgreSQL:   real   0.68
SQLite 1.0:   real   1.72   user   0.09   sys   0.55
SQLite 2.0:   real   0.10   user   0.08   sys   0.02
</pre></blockquote>

<p>
By putting all the inserts inside a single transaction, there
only needs to be a single atomic commit at the very end.  This
allows SQLite 2.0 to go (literally) 100 times faster!  PostgreSQL
only gets a eight-fold speedup.  Perhaps PostgreSQL is limited here by
the IPC overhead.
</p>

<h2>Test 10: DELETE</h2>

<blockquote><pre>
DELETE FROM t1 WHERE f2 NOT BETWEEN 10000 AND 20000;

PostgreSQL:   real   7.25
SQLite 1.0:   real   6.98   user   1.66   sys   4.11
SQLite 2.0:   real   5.89   user   1.35   sys   3.11
</pre></blockquote>

<p>
All three database run at about the same speed here.
</p>

<h2>Test 11: DROP TABLE</h2>

<blockquote><pre>
BEGIN TRANSACTION;
DROP TABLE t1; DROP TABLE t2;
DROP TABLE t3; DROP TABLE t4;
COMMIT;

PostgreSQL:   real   0.06
SQLite 1.0:   real   0.03   user   0.00   sys   0.02
SQLite 2.0:   real   3.12   user   0.02   sys   0.31
</pre></blockquote>

<p>
SQLite 2.0 is much slower at dropping tables.  This may be because
both SQLite 1.0 and PostgreSQL can drop a table simply by unlinking
or renaming a file, since that both use one or more files per table.
SQLite 2.0, on the other hand, uses a single file for the entire
database, so dropping a table involves moving lots of page of that
file to the free-list, which takes time.
</p>

}
puts {
<p><hr /></p>
<p><a href="index.html"><img src="/goback.jpg" border=0 />
Back to the SQLite Home Page</a>
</p>

</body></html>}

#
# Run this Tcl script to generate the speed.html file.
#
set rcsid {$Id: speed.tcl,v 1.13 2003/06/29 16:11:13 drh Exp $ }

puts {<html>
<head>
  <title>Database Speed Comparison: SQLite versus PostgreSQL</title>
</head>
<body bgcolor=white>
<h1 align=center>
Database Speed Comparison
</h1>}
puts "<p align=center>
(This page was last modified on [lrange $rcsid 3 4] UTC)
</p>"

puts {
<h2>Executive Summary</h2>

<p>A series of tests were run to measure the relative performance of
SQLite 2.7.6, PostgreSQL 7.1.3, and MySQL 3.23.41.
The following are general
conclusions drawn from these experiments:
</p>

<ul>
<li><p>
  SQLite 2.7.6 is significantly faster (sometimes as much as 10 or
  20 times faster) than the default PostgreSQL 7.1.3 installation
  on RedHat 7.2 for most common operations.  
</p></li>
<li><p>
  SQLite 2.7.6 is often faster (sometimes
  more than twice as fast) than MySQL 3.23.41
  for most common operations.
</p></li>
<li><p>
  SQLite does not execute CREATE INDEX or DROP TABLE as fast as
  the other databases.  But this is not seen as a problem because
  those are infrequent operations.
</p></li>
<li><p>
  SQLite works best if you group multiple operations together into
  a single transaction.
</p></li>
</ul>

<p>
The results presented here come with the following caveats:
</p>

<ul>
<li><p>
  These tests did not attempt to measure multi-user performance or
  optimization of complex queries involving multiple joins and subqueries.
</p></li>
<li><p>
  These tests are on a relatively small (approximately 14 megabyte) database.
  They do not measure how well the database engines scale to larger problems.
</p></li>
</ul>

<h2>Test Environment</h2>

<p>
The platform used for these tests is a 1.6GHz Athlon with 1GB or memory
and an IDE disk drive.  The operating system is RedHat Linux 7.2 with
a stock kernel.
</p>

<p>
The PostgreSQL and MySQL servers used were as delivered by default on
RedHat 7.2.  (PostgreSQL version 7.1.3 and MySQL version 3.23.41.)
No effort was made to tune these engines.  Note in particular
the the default MySQL configuration on RedHat 7.2 does not support
transactions.  Not having to support transactions gives MySQL a
big speed advantage, but SQLite is still able to hold its own on most
tests.
</p>

<p>
I am told that the default PostgreSQL configuration in RedHat 7.3
is unnecessarily conservative (it is designed to
work on a machine with 8MB of RAM) and that PostgreSQL could
be made to run a lot faster with some knowledgable configuration
tuning.
Matt Sergeant reports that he has tuned his PostgreSQL installation
and rerun the tests shown below.  His results show that
PostgreSQL and MySQL run at about the same speed.  For Matt's
results, visit
</p>

<blockquote>
<a href="http://www.sergeant.org/sqlite_vs_pgsync.html">
http://www.sergeant.org/sqlite_vs_pgsync.html</a>
</blockquote>

<p>
SQLite was tested in the same configuration that it appears
on the website.  It was compiled with -O6 optimization and with
the -DNDEBUG=1 switch which disables the many "assert()" statements
in the SQLite code.  The -DNDEBUG=1 compiler option roughly doubles
the speed of SQLite.
</p>

<p>
All tests are conducted on an otherwise quiescent machine.
A simple Tcl script was used to generate and run all the tests.
A copy of this Tcl script can be found in the SQLite source tree
in the file <b>tools/speedtest.tcl</b>.
</p>

<p>
The times reported on all tests represent wall-clock time 
in seconds.  Two separate time values are reported for SQLite.
The first value is for SQLite in its default configuration with
full disk synchronization turned on.  With synchronization turned
on, SQLite executes
an <b>fsync()</b> system call (or the equivalent) at key points
to make certain that critical data has 
actually been written to the disk drive surface.  Synchronization
is necessary to guarantee the integrity of the database if the
operating system crashes or the computer powers down unexpectedly
in the middle of a database update.  The second time reported for SQLite is
when synchronization is turned off.  With synchronization off,
SQLite is sometimes much faster, but there is a risk that an
operating system crash or an unexpected power failure could
damage the database.  Generally speaking, the synchronous SQLite
times are for comparison against PostgreSQL (which is also
synchronous) and the asynchronous SQLite times are for 
comparison against the asynchronous MySQL engine.
</p>

<h2>Test 1: 1000 INSERTs</h2>
<blockquote>
CREATE TABLE t1(a INTEGER, b INTEGER, c VARCHAR(100));<br>
INSERT INTO t1 VALUES(1,13153,'thirteen thousand one hundred fifty three');<br>
INSERT INTO t1 VALUES(2,75560,'seventy five thousand five hundred sixty');<br>
<i>... 995 lines omitted</i><br>
INSERT INTO t1 VALUES(998,66289,'sixty six thousand two hundred eighty nine');<br>
INSERT INTO t1 VALUES(999,24322,'twenty four thousand three hundred twenty two');<br>
INSERT INTO t1 VALUES(1000,94142,'ninety four thousand one hundred forty two');<br>

</blockquote><table border=0 cellpadding=0 cellspacing=0>
<tr><td>PostgreSQL:</td><td align="right">&nbsp;&nbsp;&nbsp;4.373</td></tr>
<tr><td>MySQL:</td><td align="right">&nbsp;&nbsp;&nbsp;0.114</td></tr>
<tr><td>SQLite 2.7.6:</td><td align="right">&nbsp;&nbsp;&nbsp;13.061</td></tr>
<tr><td>SQLite 2.7.6 (nosync):</td><td align="right">&nbsp;&nbsp;&nbsp;0.223</td></tr>
</table>

<p>
Because it does not have a central server to coordinate access,
SQLite must close and reopen the database file, and thus invalidate
its cache, for each transaction.  In this test, each SQL statement
is a separate transaction so the database file must be opened and closed
and the cache must be flushed 1000 times.  In spite of this, the asynchronous
version of SQLite is still nearly as fast as MySQL.  Notice how much slower
the synchronous version is, however.  SQLite calls <b>fsync()</b> after 
each synchronous transaction to make sure that all data is safely on
the disk surface before continuing.  For most of the 13 seconds in the
synchronous test, SQLite was sitting idle waiting on disk I/O to complete.</p>


<h2>Test 2: 25000 INSERTs in a transaction</h2>
<blockquote>
BEGIN;<br>
CREATE TABLE t2(a INTEGER, b INTEGER, c VARCHAR(100));<br>
INSERT INTO t2 VALUES(1,59672,'fifty nine thousand six hundred seventy two');<br>
<i>... 24997 lines omitted</i><br>
INSERT INTO t2 VALUES(24999,89569,'eighty nine thousand five hundred sixty nine');<br>
INSERT INTO t2 VALUES(25000,94666,'ninety four thousand six hundred sixty six');<br>
COMMIT;<br>

</blockquote><table border=0 cellpadding=0 cellspacing=0>
<tr><td>PostgreSQL:</td><td align="right">&nbsp;&nbsp;&nbsp;4.900</td></tr>
<tr><td>MySQL:</td><td align="right">&nbsp;&nbsp;&nbsp;2.184</td></tr>
<tr><td>SQLite 2.7.6:</td><td align="right">&nbsp;&nbsp;&nbsp;0.914</td></tr>
<tr><td>SQLite 2.7.6 (nosync):</td><td align="right">&nbsp;&nbsp;&nbsp;0.757</td></tr>
</table>

<p>
When all the INSERTs are put in a transaction, SQLite no longer has to
close and reopen the database or invalidate its cache between each statement.
It also does not
have to do any fsync()s until the very end.  When unshackled in
this way, SQLite is much faster than either PostgreSQL and MySQL.
</p>

<h2>Test 3: 25000 INSERTs into an indexed table</h2>
<blockquote>
BEGIN;<br>
CREATE TABLE t3(a INTEGER, b INTEGER, c VARCHAR(100));<br>
CREATE INDEX i3 ON t3(c);<br>
<i>... 24998 lines omitted</i><br>
INSERT INTO t3 VALUES(24999,88509,'eighty eight thousand five hundred nine');<br>
INSERT INTO t3 VALUES(25000,84791,'eighty four thousand seven hundred ninety one');<br>
COMMIT;<br>

</blockquote><table border=0 cellpadding=0 cellspacing=0>
<tr><td>PostgreSQL:</td><td align="right">&nbsp;&nbsp;&nbsp;8.175</td></tr>
<tr><td>MySQL:</td><td align="right">&nbsp;&nbsp;&nbsp;3.197</td></tr>
<tr><td>SQLite 2.7.6:</td><td align="right">&nbsp;&nbsp;&nbsp;1.555</td></tr>
<tr><td>SQLite 2.7.6 (nosync):</td><td align="right">&nbsp;&nbsp;&nbsp;1.402</td></tr>
</table>

<p>
There were reports that SQLite did not perform as well on an indexed table.
This test was recently added to disprove those rumors.  It is true that
SQLite is not as fast at creating new index entries as the other engines
(see Test 6 below) but its overall speed is still better.
</p>

<h2>Test 4: 100 SELECTs without an index</h2>
<blockquote>
BEGIN;<br>
SELECT count(*), avg(b) FROM t2 WHERE b>=0 AND b<1000;<br>
SELECT count(*), avg(b) FROM t2 WHERE b>=100 AND b<1100;<br>
<i>... 96 lines omitted</i><br>
SELECT count(*), avg(b) FROM t2 WHERE b>=9800 AND b<10800;<br>
SELECT count(*), avg(b) FROM t2 WHERE b>=9900 AND b<10900;<br>
COMMIT;<br>

</blockquote><table border=0 cellpadding=0 cellspacing=0>
<tr><td>PostgreSQL:</td><td align="right">&nbsp;&nbsp;&nbsp;3.629</td></tr>
<tr><td>MySQL:</td><td align="right">&nbsp;&nbsp;&nbsp;2.760</td></tr>
<tr><td>SQLite 2.7.6:</td><td align="right">&nbsp;&nbsp;&nbsp;2.494</td></tr>
<tr><td>SQLite 2.7.6 (nosync):</td><td align="right">&nbsp;&nbsp;&nbsp;2.526</td></tr>
</table>


<p>
This test does 100 queries on a 25000 entry table without an index,
thus requiring a full table scan.   Prior versions of SQLite used to
be slower than PostgreSQL and MySQL on this test, but recent performance
enhancements have increased its speed so that it is now the fastest
of the group.
</p>

<h2>Test 5: 100 SELECTs on a string comparison</h2>
<blockquote>
BEGIN;<br>
SELECT count(*), avg(b) FROM t2 WHERE c LIKE '%one%';<br>
SELECT count(*), avg(b) FROM t2 WHERE c LIKE '%two%';<br>
<i>... 96 lines omitted</i><br>
SELECT count(*), avg(b) FROM t2 WHERE c LIKE '%ninety nine%';<br>
SELECT count(*), avg(b) FROM t2 WHERE c LIKE '%one hundred%';<br>
COMMIT;<br>

</blockquote><table border=0 cellpadding=0 cellspacing=0>
<tr><td>PostgreSQL:</td><td align="right">&nbsp;&nbsp;&nbsp;13.409</td></tr>
<tr><td>MySQL:</td><td align="right">&nbsp;&nbsp;&nbsp;4.640</td></tr>
<tr><td>SQLite 2.7.6:</td><td align="right">&nbsp;&nbsp;&nbsp;3.362</td></tr>
<tr><td>SQLite 2.7.6 (nosync):</td><td align="right">&nbsp;&nbsp;&nbsp;3.372</td></tr>
</table>

<p>
This test still does 100 full table scans but it uses
uses string comparisons instead of numerical comparisions.
SQLite is over three times faster than PostgreSQL here and about 30%
faster than MySQL.
</p>

<h2>Test 6: Creating an index</h2>
<blockquote>
CREATE INDEX i2a ON t2(a);<br>CREATE INDEX i2b ON t2(b);
</blockquote><table border=0 cellpadding=0 cellspacing=0>
<tr><td>PostgreSQL:</td><td align="right">&nbsp;&nbsp;&nbsp;0.381</td></tr>
<tr><td>MySQL:</td><td align="right">&nbsp;&nbsp;&nbsp;0.318</td></tr>
<tr><td>SQLite 2.7.6:</td><td align="right">&nbsp;&nbsp;&nbsp;0.777</td></tr>
<tr><td>SQLite 2.7.6 (nosync):</td><td align="right">&nbsp;&nbsp;&nbsp;0.659</td></tr>
</table>

<p>
SQLite is slower at creating new indices.  This is not a huge problem
(since new indices are not created very often) but it is something that
is being worked on.  Hopefully, future versions of SQLite will do better
here.
</p>

<h2>Test 7: 5000 SELECTs with an index</h2>
<blockquote>
SELECT count(*), avg(b) FROM t2 WHERE b>=0 AND b<100;<br>
SELECT count(*), avg(b) FROM t2 WHERE b>=100 AND b<200;<br>
SELECT count(*), avg(b) FROM t2 WHERE b>=200 AND b<300;<br>
<i>... 4994 lines omitted</i><br>
SELECT count(*), avg(b) FROM t2 WHERE b>=499700 AND b<499800;<br>
SELECT count(*), avg(b) FROM t2 WHERE b>=499800 AND b<499900;<br>
SELECT count(*), avg(b) FROM t2 WHERE b>=499900 AND b<500000;<br>

</blockquote><table border=0 cellpadding=0 cellspacing=0>
<tr><td>PostgreSQL:</td><td align="right">&nbsp;&nbsp;&nbsp;4.614</td></tr>
<tr><td>MySQL:</td><td align="right">&nbsp;&nbsp;&nbsp;1.270</td></tr>
<tr><td>SQLite 2.7.6:</td><td align="right">&nbsp;&nbsp;&nbsp;1.121</td></tr>
<tr><td>SQLite 2.7.6 (nosync):</td><td align="right">&nbsp;&nbsp;&nbsp;1.162</td></tr>
</table>

<p>
All three database engines run faster when they have indices to work with.
But SQLite is still the fastest.
</p>

<h2>Test 8: 1000 UPDATEs without an index</h2>
<blockquote>
BEGIN;<br>
UPDATE t1 SET b=b*2 WHERE a>=0 AND a<10;<br>
UPDATE t1 SET b=b*2 WHERE a>=10 AND a<20;<br>
<i>... 996 lines omitted</i><br>
UPDATE t1 SET b=b*2 WHERE a>=9980 AND a<9990;<br>
UPDATE t1 SET b=b*2 WHERE a>=9990 AND a<10000;<br>
COMMIT;<br>

</blockquote><table border=0 cellpadding=0 cellspacing=0>
<tr><td>PostgreSQL:</td><td align="right">&nbsp;&nbsp;&nbsp;1.739</td></tr>
<tr><td>MySQL:</td><td align="right">&nbsp;&nbsp;&nbsp;8.410</td></tr>
<tr><td>SQLite 2.7.6:</td><td align="right">&nbsp;&nbsp;&nbsp;0.637</td></tr>
<tr><td>SQLite 2.7.6 (nosync):</td><td align="right">&nbsp;&nbsp;&nbsp;0.638</td></tr>
</table>

<p>
For this particular UPDATE test, MySQL is consistently
five or ten times
slower than PostgreSQL and SQLite.  I do not know why.  MySQL is
normally a very fast engine.  Perhaps this problem has been addressed
in later versions of MySQL.
</p>

<h2>Test 9: 25000 UPDATEs with an index</h2>
<blockquote>
BEGIN;<br>
UPDATE t2 SET b=468026 WHERE a=1;<br>
UPDATE t2 SET b=121928 WHERE a=2;<br>
<i>... 24996 lines omitted</i><br>
UPDATE t2 SET b=35065 WHERE a=24999;<br>
UPDATE t2 SET b=347393 WHERE a=25000;<br>
COMMIT;<br>

</blockquote><table border=0 cellpadding=0 cellspacing=0>
<tr><td>PostgreSQL:</td><td align="right">&nbsp;&nbsp;&nbsp;18.797</td></tr>
<tr><td>MySQL:</td><td align="right">&nbsp;&nbsp;&nbsp;8.134</td></tr>
<tr><td>SQLite 2.7.6:</td><td align="right">&nbsp;&nbsp;&nbsp;3.520</td></tr>
<tr><td>SQLite 2.7.6 (nosync):</td><td align="right">&nbsp;&nbsp;&nbsp;3.104</td></tr>
</table>

<p>
As recently as version 2.7.0, SQLite ran at about the same speed as
MySQL on this test.  But recent optimizations to SQLite have more
than doubled speed of UPDATEs.
</p>

<h2>Test 10: 25000 text UPDATEs with an index</h2>
<blockquote>
BEGIN;<br>
UPDATE t2 SET c='one hundred forty eight thousand three hundred eighty two' WHERE a=1;<br>
UPDATE t2 SET c='three hundred sixty six thousand five hundred two' WHERE a=2;<br>
<i>... 24996 lines omitted</i><br>
UPDATE t2 SET c='three hundred eighty three thousand ninety nine' WHERE a=24999;<br>
UPDATE t2 SET c='two hundred fifty six thousand eight hundred thirty' WHERE a=25000;<br>
COMMIT;<br>

</blockquote><table border=0 cellpadding=0 cellspacing=0>
<tr><td>PostgreSQL:</td><td align="right">&nbsp;&nbsp;&nbsp;48.133</td></tr>
<tr><td>MySQL:</td><td align="right">&nbsp;&nbsp;&nbsp;6.982</td></tr>
<tr><td>SQLite 2.7.6:</td><td align="right">&nbsp;&nbsp;&nbsp;2.408</td></tr>
<tr><td>SQLite 2.7.6 (nosync):</td><td align="right">&nbsp;&nbsp;&nbsp;1.725</td></tr>
</table>

<p>
Here again, version 2.7.0 of SQLite used to run at about the same speed
as MySQL.  But now version 2.7.6 is over two times faster than MySQL and
over twenty times faster than PostgreSQL.
</p>

<p>
In fairness to PostgreSQL, it started thrashing on this test.  A
knowledgeable administrator might be able to get PostgreSQL to run a lot
faster here by tweaking and tuning the server a little.
</p>

<h2>Test 11: INSERTs from a SELECT</h2>
<blockquote>
BEGIN;<br>INSERT INTO t1 SELECT b,a,c FROM t2;<br>INSERT INTO t2 SELECT b,a,c FROM t1;<br>COMMIT;
</blockquote><table border=0 cellpadding=0 cellspacing=0>
<tr><td>PostgreSQL:</td><td align="right">&nbsp;&nbsp;&nbsp;61.364</td></tr>
<tr><td>MySQL:</td><td align="right">&nbsp;&nbsp;&nbsp;1.537</td></tr>
<tr><td>SQLite 2.7.6:</td><td align="right">&nbsp;&nbsp;&nbsp;2.787</td></tr>
<tr><td>SQLite 2.7.6 (nosync):</td><td align="right">&nbsp;&nbsp;&nbsp;1.599</td></tr>
</table>

<p>
The asynchronous SQLite is just a shade slower than MySQL on this test.
(MySQL seems to be especially adept at INSERT...SELECT statements.)
The PostgreSQL engine is still thrashing - most of the 61 seconds it used
were spent waiting on disk I/O.
</p>

<h2>Test 12: DELETE without an index</h2>
<blockquote>
DELETE FROM t2 WHERE c LIKE '%fifty%';
</blockquote><table border=0 cellpadding=0 cellspacing=0>
<tr><td>PostgreSQL:</td><td align="right">&nbsp;&nbsp;&nbsp;1.509</td></tr>
<tr><td>MySQL:</td><td align="right">&nbsp;&nbsp;&nbsp;0.975</td></tr>
<tr><td>SQLite 2.7.6:</td><td align="right">&nbsp;&nbsp;&nbsp;4.004</td></tr>
<tr><td>SQLite 2.7.6 (nosync):</td><td align="right">&nbsp;&nbsp;&nbsp;0.560</td></tr>
</table>

<p>
The synchronous version of SQLite is the slowest of the group in this test,
but the asynchronous version is the fastest.  
The difference is the extra time needed to execute fsync().
</p>

<h2>Test 13: DELETE with an index</h2>
<blockquote>
DELETE FROM t2 WHERE a>10 AND a<20000;
</blockquote><table border=0 cellpadding=0 cellspacing=0>
<tr><td>PostgreSQL:</td><td align="right">&nbsp;&nbsp;&nbsp;1.316</td></tr>
<tr><td>MySQL:</td><td align="right">&nbsp;&nbsp;&nbsp;2.262</td></tr>
<tr><td>SQLite 2.7.6:</td><td align="right">&nbsp;&nbsp;&nbsp;2.068</td></tr>
<tr><td>SQLite 2.7.6 (nosync):</td><td align="right">&nbsp;&nbsp;&nbsp;0.752</td></tr>
</table>

<p>
This test is significant because it is one of the few where
PostgreSQL is faster than MySQL.  The asynchronous SQLite is,
however, faster then both the other two.
</p>

<h2>Test 14: A big INSERT after a big DELETE</h2>
<blockquote>
INSERT INTO t2 SELECT * FROM t1;
</blockquote><table border=0 cellpadding=0 cellspacing=0>
<tr><td>PostgreSQL:</td><td align="right">&nbsp;&nbsp;&nbsp;13.168</td></tr>
<tr><td>MySQL:</td><td align="right">&nbsp;&nbsp;&nbsp;1.815</td></tr>
<tr><td>SQLite 2.7.6:</td><td align="right">&nbsp;&nbsp;&nbsp;3.210</td></tr>
<tr><td>SQLite 2.7.6 (nosync):</td><td align="right">&nbsp;&nbsp;&nbsp;1.485</td></tr>
</table>

<p>
Some older versions of SQLite (prior to version 2.4.0)
would show decreasing performance after a
sequence of DELETEs followed by new INSERTs.  As this test shows, the
problem has now been resolved.
</p>

<h2>Test 15: A big DELETE followed by many small INSERTs</h2>
<blockquote>
BEGIN;<br>
DELETE FROM t1;<br>
INSERT INTO t1 VALUES(1,10719,'ten thousand seven hundred nineteen');<br>
<i>... 11997 lines omitted</i><br>
INSERT INTO t1 VALUES(11999,72836,'seventy two thousand eight hundred thirty six');<br>
INSERT INTO t1 VALUES(12000,64231,'sixty four thousand two hundred thirty one');<br>
COMMIT;<br>

</blockquote><table border=0 cellpadding=0 cellspacing=0>
<tr><td>PostgreSQL:</td><td align="right">&nbsp;&nbsp;&nbsp;4.556</td></tr>
<tr><td>MySQL:</td><td align="right">&nbsp;&nbsp;&nbsp;1.704</td></tr>
<tr><td>SQLite 2.7.6:</td><td align="right">&nbsp;&nbsp;&nbsp;0.618</td></tr>
<tr><td>SQLite 2.7.6 (nosync):</td><td align="right">&nbsp;&nbsp;&nbsp;0.406</td></tr>
</table>

<p>
SQLite is very good at doing INSERTs within a transaction, which probably
explains why it is so much faster than the other databases at this test.
</p>

<h2>Test 16: DROP TABLE</h2>
<blockquote>
DROP TABLE t1;<br>DROP TABLE t2;<br>DROP TABLE t3;
</blockquote><table border=0 cellpadding=0 cellspacing=0>
<tr><td>PostgreSQL:</td><td align="right">&nbsp;&nbsp;&nbsp;0.135</td></tr>
<tr><td>MySQL:</td><td align="right">&nbsp;&nbsp;&nbsp;0.015</td></tr>
<tr><td>SQLite 2.7.6:</td><td align="right">&nbsp;&nbsp;&nbsp;0.939</td></tr>
<tr><td>SQLite 2.7.6 (nosync):</td><td align="right">&nbsp;&nbsp;&nbsp;0.254</td></tr>
</table>

<p>
SQLite is slower than the other databases when it comes to dropping tables.
This probably is because when SQLite drops a table, it has to go through and
erase the records in the database file that deal with that table.  MySQL and
PostgreSQL, on the other hand, use separate files to represent each table
so they can drop a table simply by deleting a file, which is much faster.
</p>

<p>
On the other hand, dropping tables is not a very common operation 
so if SQLite takes a little longer, that is not seen as a big problem.
</p>

}
puts {
<p><hr /></p>
<p><a href="index.html"><img src="/goback.jpg" border=0 />
Back to the SQLite Home Page</a>
</p>

</body></html>}

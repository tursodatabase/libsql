#
# Run this Tcl script to generate the speed.html file.
#
set rcsid {$Id: speed.tcl,v 1.9 2003/01/18 22:01:07 drh Exp $ }

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
  20 times faster) than PostgreSQL 7.1.3
  for most common operations.  
</p></li>
<li><p>
  SQLite 2.7.6 is usually faster than MySQL 3.23.41 (sometimes
  more than twice as fast) though for some operations such as
  full table scans, it can be as much as 30% slower.
</p></li>
<li><p>
  SQLite does not execute CREATE INDEX or DROP TABLE as fast as
  the other databases.  But this as not seen is a problem because
  those are infrequent operations.
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
  These tests are on a relatively small (approximately 10 megabyte) database.
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
tests.  On the other hand, I am told that the default PostgreSQL
configuration is unnecessarily conservative (it is designed to
work on a machine with 8MB of RAM) and that PostgreSQL could
be made to run a lot faster with some knowledgable configuration
tuning.  I have not, however, been able to personally confirm
these reports.
</p>

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
<tr><td>PostgreSQL:</td><td align="right">&nbsp;&nbsp;&nbsp;3.658</td></tr>
<tr><td>MySQL:</td><td align="right">&nbsp;&nbsp;&nbsp;0.109</td></tr>
<tr><td>SQLite 2.7.6:</td><td align="right">&nbsp;&nbsp;&nbsp;7.177</td></tr>
<tr><td>SQLite 2.7.6 (nosync):</td><td align="right">&nbsp;&nbsp;&nbsp;0.266</td></tr>
</table>

<p>SQLite must close and reopen the database file, and thus invalidate
its cache, for each SQL statement.  In spite of this, the asynchronous
version of SQLite is still nearly as fast as MySQL.  Notice how much slower
the synchronous version is, however.  This is due to the necessity of
calling <b>fsync()</b> after each SQL statement.</p>


<h2>Test 2: 25000 INSERTs in a transaction</h2>
<blockquote>
BEGIN;<br>
CREATE TABLE t2(a INTEGER, b INTEGER, c VARCHAR(100));<br>
INSERT INTO t2 VALUES(1,298361,'two hundred ninety eight thousand three hundred sixty one');<br>
<i>... 24997 lines omitted</i><br>
INSERT INTO t2 VALUES(24999,447847,'four hundred forty seven thousand eight hundred forty seven');<br>
INSERT INTO t2 VALUES(25000,473330,'four hundred seventy three thousand three hundred thirty');<br>
COMMIT;<br>

</blockquote><table border=0 cellpadding=0 cellspacing=0>
<tr><td>PostgreSQL:</td><td align="right">&nbsp;&nbsp;&nbsp;5.058</td></tr>
<tr><td>MySQL:</td><td align="right">&nbsp;&nbsp;&nbsp;2.271</td></tr>
<tr><td>SQLite 2.7.6:</td><td align="right">&nbsp;&nbsp;&nbsp;0.912</td></tr>
<tr><td>SQLite 2.7.6 (nosync):</td><td align="right">&nbsp;&nbsp;&nbsp;0.798</td></tr>
</table>

<p>
When all the INSERTs are put in a transaction, SQLite no longer has to
close and reopen the database between each statement.  It also does not
have to do any fsync()s until the very end.  When unshackled in
this way, SQLite is much faster than either PostgreSQL and MySQL.
</p>

<h2>Test 3: 100 SELECTs without an index</h2>
<blockquote>
BEGIN;<br>
SELECT count(*), avg(b) FROM t2 WHERE b>=0 AND b<1000;<br>
SELECT count(*), avg(b) FROM t2 WHERE b>=100 AND b<1100;<br>
<i>... 96 lines omitted</i><br>
SELECT count(*), avg(b) FROM t2 WHERE b>=9800 AND b<10800;<br>
SELECT count(*), avg(b) FROM t2 WHERE b>=9900 AND b<10900;<br>
COMMIT;<br>

</blockquote><table border=0 cellpadding=0 cellspacing=0>
<tr><td>PostgreSQL:</td><td align="right">&nbsp;&nbsp;&nbsp;3.657</td></tr>
<tr><td>MySQL:</td><td align="right">&nbsp;&nbsp;&nbsp;3.368</td></tr>
<tr><td>SQLite 2.7.6:</td><td align="right">&nbsp;&nbsp;&nbsp;4.386</td></tr>
<tr><td>SQLite 2.7.6 (nosync):</td><td align="right">&nbsp;&nbsp;&nbsp;4.314</td></tr>
</table>

<p>
This test does 100 queries on a 25000 entry table without an index,
thus requiring a full table scan.  SQLite is about 20% or 30% slower
than PostgreSQL and MySQL.  The reason for this is believed to be
because SQLite stores all data as strings
and must therefore do 5 million string-to-number conversions in the
course of evaluating the WHERE clauses.  Both PostgreSQL and MySQL
store data as binary values where appropriate and can forego
this conversion effort.
</p>


<h2>Test 4: 100 SELECTs on a string comparison</h2>
<blockquote>
BEGIN;<br>
SELECT count(*), avg(b) FROM t2 WHERE c LIKE '%one%';<br>
SELECT count(*), avg(b) FROM t2 WHERE c LIKE '%two%';<br>
<i>... 96 lines omitted</i><br>
SELECT count(*), avg(b) FROM t2 WHERE c LIKE '%ninety nine%';<br>
SELECT count(*), avg(b) FROM t2 WHERE c LIKE '%one hundred%';<br>
COMMIT;<br>

</blockquote><table border=0 cellpadding=0 cellspacing=0>
<tr><td>PostgreSQL:</td><td align="right">&nbsp;&nbsp;&nbsp;15.967</td></tr>
<tr><td>MySQL:</td><td align="right">&nbsp;&nbsp;&nbsp;5.088</td></tr>
<tr><td>SQLite 2.7.6:</td><td align="right">&nbsp;&nbsp;&nbsp;5.419</td></tr>
<tr><td>SQLite 2.7.6 (nosync):</td><td align="right">&nbsp;&nbsp;&nbsp;5.367</td></tr>
</table>

<p>
This test still does 100 full table scans but it uses
uses string comparisons instead of numerical comparisions.
SQLite is almost three times faster than PostgreSQL here.  But it is
still 15% slower than MySQL.  MySQL appears to be very good
at doing full table scans.
</p>

<h2>Test 5: Creating an index</h2>
<blockquote>
CREATE INDEX i2a ON t2(a);<br>CREATE INDEX i2b ON t2(b);
</blockquote><table border=0 cellpadding=0 cellspacing=0>
<tr><td>PostgreSQL:</td><td align="right">&nbsp;&nbsp;&nbsp;0.431</td></tr>
<tr><td>MySQL:</td><td align="right">&nbsp;&nbsp;&nbsp;0.340</td></tr>
<tr><td>SQLite 2.7.6:</td><td align="right">&nbsp;&nbsp;&nbsp;0.814</td></tr>
<tr><td>SQLite 2.7.6 (nosync):</td><td align="right">&nbsp;&nbsp;&nbsp;0.675</td></tr>
</table>

<p>
SQLite is slower at creating new indices.  But since creating
new indices is an uncommon operation, this is not seen as a
problem.
</p>

<h2>Test 6: 5000 SELECTs with an index</h2>
<blockquote>
SELECT count(*), avg(b) FROM t2 WHERE b>=0 AND b<100;<br>
SELECT count(*), avg(b) FROM t2 WHERE b>=100 AND b<200;<br>
SELECT count(*), avg(b) FROM t2 WHERE b>=200 AND b<300;<br>
<i>... 4994 lines omitted</i><br>
SELECT count(*), avg(b) FROM t2 WHERE b>=499700 AND b<499800;<br>
SELECT count(*), avg(b) FROM t2 WHERE b>=499800 AND b<499900;<br>
SELECT count(*), avg(b) FROM t2 WHERE b>=499900 AND b<500000;<br>

</blockquote><table border=0 cellpadding=0 cellspacing=0>
<tr><td>PostgreSQL:</td><td align="right">&nbsp;&nbsp;&nbsp;5.369</td></tr>
<tr><td>MySQL:</td><td align="right">&nbsp;&nbsp;&nbsp;1.489</td></tr>
<tr><td>SQLite 2.7.6:</td><td align="right">&nbsp;&nbsp;&nbsp;1.423</td></tr>
<tr><td>SQLite 2.7.6 (nosync):</td><td align="right">&nbsp;&nbsp;&nbsp;1.358</td></tr>
</table>

<p>
This test runs a set of 5000 queries that are similar in form to
those in test 3.  But now instead of being slower, SQLite
is faster than both PostgreSQL and MySQL.
</p>

<h2>Test 7: 1000 UPDATEs without an index</h2>
<blockquote>
BEGIN;<br>
UPDATE t1 SET b=b*2 WHERE a>=0 AND a<10;<br>
UPDATE t1 SET b=b*2 WHERE a>=10 AND a<20;<br>
<i>... 996 lines omitted</i><br>
UPDATE t1 SET b=b*2 WHERE a>=9980 AND a<9990;<br>
UPDATE t1 SET b=b*2 WHERE a>=9990 AND a<10000;<br>
COMMIT;<br>

</blockquote><table border=0 cellpadding=0 cellspacing=0>
<tr><td>PostgreSQL:</td><td align="right">&nbsp;&nbsp;&nbsp;1.740</td></tr>
<tr><td>MySQL:</td><td align="right">&nbsp;&nbsp;&nbsp;8.162</td></tr>
<tr><td>SQLite 2.7.6:</td><td align="right">&nbsp;&nbsp;&nbsp;0.635</td></tr>
<tr><td>SQLite 2.7.6 (nosync):</td><td align="right">&nbsp;&nbsp;&nbsp;0.608</td></tr>
</table>

<p>
For this particular UPDATE test, MySQL is consistently
five or ten times
slower than PostgreSQL and SQLite.  I do not know why.  MySQL is
normally a very fast engine.  Perhaps this problem has been addressed
in later versions of MySQL.
</p>

<h2>Test 8: 25000 UPDATEs with an index</h2>
<blockquote>
BEGIN;<br>
UPDATE t2 SET b=271822 WHERE a=1;<br>
UPDATE t2 SET b=28304 WHERE a=2;<br>
<i>... 24996 lines omitted</i><br>
UPDATE t2 SET b=442549 WHERE a=24999;<br>
UPDATE t2 SET b=423958 WHERE a=25000;<br>
COMMIT;<br>

</blockquote><table border=0 cellpadding=0 cellspacing=0>
<tr><td>PostgreSQL:</td><td align="right">&nbsp;&nbsp;&nbsp;32.118</td></tr>
<tr><td>MySQL:</td><td align="right">&nbsp;&nbsp;&nbsp;8.132</td></tr>
<tr><td>SQLite 2.7.6:</td><td align="right">&nbsp;&nbsp;&nbsp;4.109</td></tr>
<tr><td>SQLite 2.7.6 (nosync):</td><td align="right">&nbsp;&nbsp;&nbsp;3.712</td></tr>
</table>

<p>
As recently as version 2.7.0, SQLite ran at about the same speed as
MySQL on this test.  But recent optimizations to SQLite have doubled
speed of UPDATEs.
</p>

<h2>Test 9: 25000 text UPDATEs with an index</h2>
<blockquote>
BEGIN;<br>
UPDATE t2 SET c='four hundred sixty eight thousand twenty six' WHERE a=1;<br>
UPDATE t2 SET c='one hundred twenty one thousand nine hundred twenty eight' WHERE a=2;<br>
<i>... 24996 lines omitted</i><br>
UPDATE t2 SET c='thirty five thousand sixty five' WHERE a=24999;<br>
UPDATE t2 SET c='three hundred forty seven thousand three hundred ninety three' WHERE a=25000;<br>
COMMIT;<br>

</blockquote><table border=0 cellpadding=0 cellspacing=0>
<tr><td>PostgreSQL:</td><td align="right">&nbsp;&nbsp;&nbsp;55.309</td></tr>
<tr><td>MySQL:</td><td align="right">&nbsp;&nbsp;&nbsp;6.585</td></tr>
<tr><td>SQLite 2.7.6:</td><td align="right">&nbsp;&nbsp;&nbsp;2.474</td></tr>
<tr><td>SQLite 2.7.6 (nosync):</td><td align="right">&nbsp;&nbsp;&nbsp;1.800</td></tr>
</table>

<p>
Here again, version 2.7.0 of SQLite used to run at about the same speed
as MySQL.  But now version 2.7.6 is over two times faster than MySQL and
over twenty times faster than PostgreSQL.
</p>

<h2>Test 10: INSERTs from a SELECT</h2>
<blockquote>
BEGIN;<br>INSERT INTO t1 SELECT b,a,c FROM t2;<br>INSERT INTO t2 SELECT b,a,c FROM t1;<br>COMMIT;
</blockquote><table border=0 cellpadding=0 cellspacing=0>
<tr><td>PostgreSQL:</td><td align="right">&nbsp;&nbsp;&nbsp;58.956</td></tr>
<tr><td>MySQL:</td><td align="right">&nbsp;&nbsp;&nbsp;1.465</td></tr>
<tr><td>SQLite 2.7.6:</td><td align="right">&nbsp;&nbsp;&nbsp;2.926</td></tr>
<tr><td>SQLite 2.7.6 (nosync):</td><td align="right">&nbsp;&nbsp;&nbsp;1.664</td></tr>
</table>

<p>
The poor performance of PostgreSQL in this case appears to be due to its
synchronous behavior.  The CPU was mostly idle the test run.  Presumably,
PostgreSQL was spending most of its time waiting on disk I/O to complete.
I'm not sure why SQLite performs poorly here.  It use to be quicker at this
test, but the same enhancements that sped up the UPDATE logic seem to have
slowed down this test.
</p>

<h2>Test 11: DELETE without an index</h2>
<blockquote>
DELETE FROM t2 WHERE c LIKE '%fifty%';
</blockquote><table border=0 cellpadding=0 cellspacing=0>
<tr><td>PostgreSQL:</td><td align="right">&nbsp;&nbsp;&nbsp;1.365</td></tr>
<tr><td>MySQL:</td><td align="right">&nbsp;&nbsp;&nbsp;0.849</td></tr>
<tr><td>SQLite 2.7.6:</td><td align="right">&nbsp;&nbsp;&nbsp;4.005</td></tr>
<tr><td>SQLite 2.7.6 (nosync):</td><td align="right">&nbsp;&nbsp;&nbsp;0.631</td></tr>
</table>

<p>
The synchronous version of SQLite is the slowest of the group in this test,
but the asynchronous version is the fastest.  SQLite used about the same
amount of CPU time in both versions; the difference is the extra time needed
to write information to the disk surface.
</p>

<h2>Test 12: DELETE with an index</h2>
<blockquote>
DELETE FROM t2 WHERE a>10 AND a<20000;
</blockquote><table border=0 cellpadding=0 cellspacing=0>
<tr><td>PostgreSQL:</td><td align="right">&nbsp;&nbsp;&nbsp;1.340</td></tr>
<tr><td>MySQL:</td><td align="right">&nbsp;&nbsp;&nbsp;2.167</td></tr>
<tr><td>SQLite 2.7.6:</td><td align="right">&nbsp;&nbsp;&nbsp;2.344</td></tr>
<tr><td>SQLite 2.7.6 (nosync):</td><td align="right">&nbsp;&nbsp;&nbsp;0.858</td></tr>
</table>

<p>
This test is significant because it is one of the few where
PostgreSQL is faster than MySQL.  The asynchronous SQLite is,
however, faster then both the other two.
</p>

</table>
<h2>Test 13: A big INSERT after a big DELETE</h2>
<blockquote>
INSERT INTO t2 SELECT * FROM t1;
</blockquote><table border=0 cellpadding=0 cellspacing=0>
<tr><td>PostgreSQL:</td><td align="right">&nbsp;&nbsp;&nbsp;12.672</td></tr>
<tr><td>MySQL:</td><td align="right">&nbsp;&nbsp;&nbsp;1.837</td></tr>
<tr><td>SQLite 2.7.6:</td><td align="right">&nbsp;&nbsp;&nbsp;3.076</td></tr>
<tr><td>SQLite 2.7.6 (nosync):</td><td align="right">&nbsp;&nbsp;&nbsp;1.570</td></tr>
</table>

<p>
Some older versions of SQLite would show decreasing performance after a
sequence DELETEs followed by new INSERTs.  As this test shows, the
problem has now been resolved.
</p>

<h2>Test 14: A big DELETE followed by many small INSERTs</h2>
<blockquote>
BEGIN;<br>
DELETE FROM t1;<br>
INSERT INTO t1 VALUES(1,29676,'twenty nine thousand six hundred seventy six');<br>
<i>... 11997 lines omitted</i><br>
INSERT INTO t1 VALUES(11999,71818,'seventy one thousand eight hundred eighteen');<br>
INSERT INTO t1 VALUES(12000,58579,'fifty eight thousand five hundred seventy nine');<br>
COMMIT;<br>

</blockquote><table border=0 cellpadding=0 cellspacing=0>
<tr><td>PostgreSQL:</td><td align="right">&nbsp;&nbsp;&nbsp;4.165</td></tr>
<tr><td>MySQL:</td><td align="right">&nbsp;&nbsp;&nbsp;1.733</td></tr>
<tr><td>SQLite 2.7.6:</td><td align="right">&nbsp;&nbsp;&nbsp;0.652</td></tr>
<tr><td>SQLite 2.7.6 (nosync):</td><td align="right">&nbsp;&nbsp;&nbsp;0.465</td></tr>
</table>

<p>
SQLite is very good at doing INSERTs within a transaction, which probably
explains why it is so much faster than the other databases at this test.
</p>

<h2>Test 15: DROP TABLE</h2>
<blockquote>
DROP TABLE t1;<br>DROP TABLE t2;
</blockquote><table border=0 cellpadding=0 cellspacing=0>
<tr><td>PostgreSQL:</td><td align="right">&nbsp;&nbsp;&nbsp;0.133</td></tr>
<tr><td>MySQL:</td><td align="right">&nbsp;&nbsp;&nbsp;0.014</td></tr>
<tr><td>SQLite 2.7.6:</td><td align="right">&nbsp;&nbsp;&nbsp;0.873</td></tr>
<tr><td>SQLite 2.7.6 (nosync):</td><td align="right">&nbsp;&nbsp;&nbsp;0.224</td></tr>
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

#
# Run this Tcl script to generate the speed.html file.
#
set rcsid {$Id: speed.tcl,v 1.7 2002/08/06 12:05:01 drh Exp $ }

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
SQLite 2.4.0, PostgreSQL, and MySQL
The following are general
conclusions drawn from these experiments:
</p>

<ul>
<li><p>
  SQLite 2.4.0 is significantly faster than PostgreSQL
  for most common operations.
</p></li>
<li><p>
  The speed of SQLite 2.4.0 is similar to MySQL.
  This is true in spite of the
  fact that SQLite contains full transaction support whereas the
  version of MySQL tested did not.
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
RedHat 7.2.  No effort was made to tune these engines.  Note in particular
the the default MySQL configuration on RedHat 7.2 does not support
transactions.  Not having to support transactions gives MySQL a
big advantage, but SQLite is still able to hold its own on most
tests.
</p>

<p>
SQLite was compiled with -O6 optimization and with
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
<tr><td>PostgreSQL:</td><td align="right">&nbsp;&nbsp;&nbsp;4.027</td></tr>
<tr><td>MySQL:</td><td align="right">&nbsp;&nbsp;&nbsp;0.113</td></tr>
<tr><td>SQLite 2.4:</td><td align="right">&nbsp;&nbsp;&nbsp;8.409</td></tr>
<tr><td>SQLite 2.4 (nosync):</td><td align="right">&nbsp;&nbsp;&nbsp;0.188</td></tr>
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
<tr><td>PostgreSQL:</td><td align="right">&nbsp;&nbsp;&nbsp;5.175</td></tr>
<tr><td>MySQL:</td><td align="right">&nbsp;&nbsp;&nbsp;2.444</td></tr>
<tr><td>SQLite 2.4:</td><td align="right">&nbsp;&nbsp;&nbsp;0.858</td></tr>
<tr><td>SQLite 2.4 (nosync):</td><td align="right">&nbsp;&nbsp;&nbsp;0.739</td></tr>
</table>

<p>
When all the INSERTs are put in a transaction, SQLite no longer has to
close and reopen the database between each statement.  It also does not
have to do any fsync()s until the very end.  When unshackled in
this way, SQLite is much faster than either PostgreSQL and MySQL.
</p>

<h2>Test 3: 100 SELECTs without an index</h2>
<blockquote>
SELECT count(*), avg(b) FROM t2 WHERE b>=0 AND b<1000;<br>
SELECT count(*), avg(b) FROM t2 WHERE b>=100 AND b<1100;<br>
SELECT count(*), avg(b) FROM t2 WHERE b>=200 AND b<1200;<br>
<i>... 94 lines omitted</i><br>
SELECT count(*), avg(b) FROM t2 WHERE b>=9700 AND b<10700;<br>
SELECT count(*), avg(b) FROM t2 WHERE b>=9800 AND b<10800;<br>
SELECT count(*), avg(b) FROM t2 WHERE b>=9900 AND b<10900;<br>

</blockquote><table border=0 cellpadding=0 cellspacing=0>
<tr><td>PostgreSQL:</td><td align="right">&nbsp;&nbsp;&nbsp;3.773</td></tr>
<tr><td>MySQL:</td><td align="right">&nbsp;&nbsp;&nbsp;3.023</td></tr>
<tr><td>SQLite 2.4:</td><td align="right">&nbsp;&nbsp;&nbsp;6.281</td></tr>
<tr><td>SQLite 2.4 (nosync):</td><td align="right">&nbsp;&nbsp;&nbsp;6.247</td></tr>
</table>

<p>
This test does 100 queries on a 25000 entry table without an index,
thus requiring a full table scan.  SQLite is about half the speed of
PostgreSQL and MySQL.  This is because SQLite stores all data as strings
and must therefore call <b>strtod()</b> 5 million times in the
course of evaluating the WHERE clauses.  Both PostgreSQL and MySQL
store data as binary values where appropriate and can forego
this conversion effort.
</p>


<h2>Test 4: 100 SELECTs on a string comparison</h2>
<blockquote>
SELECT count(*), avg(b) FROM t2 WHERE c LIKE '%one%';<br>
SELECT count(*), avg(b) FROM t2 WHERE c LIKE '%two%';<br>
SELECT count(*), avg(b) FROM t2 WHERE c LIKE '%three%';<br>
<i>... 94 lines omitted</i><br>
SELECT count(*), avg(b) FROM t2 WHERE c LIKE '%ninety eight%';<br>
SELECT count(*), avg(b) FROM t2 WHERE c LIKE '%ninety nine%';<br>
SELECT count(*), avg(b) FROM t2 WHERE c LIKE '%one hundred%';<br>

</blockquote><table border=0 cellpadding=0 cellspacing=0>
<tr><td>PostgreSQL:</td><td align="right">&nbsp;&nbsp;&nbsp;16.726</td></tr>
<tr><td>MySQL:</td><td align="right">&nbsp;&nbsp;&nbsp;5.237</td></tr>
<tr><td>SQLite 2.4:</td><td align="right">&nbsp;&nbsp;&nbsp;6.137</td></tr>
<tr><td>SQLite 2.4 (nosync):</td><td align="right">&nbsp;&nbsp;&nbsp;6.112</td></tr>
</table>

<p>
This set of 100 queries uses string comparisons instead of
numerical comparisions.  As a result, the speed of SQLite is
compariable to or better then PostgreSQL and MySQL.
</p>

<h2>Test 5: Creating an index</h2>
<blockquote>
CREATE INDEX i2a ON t2(a);<br>CREATE INDEX i2b ON t2(b);
</blockquote><table border=0 cellpadding=0 cellspacing=0>
<tr><td>PostgreSQL:</td><td align="right">&nbsp;&nbsp;&nbsp;0.510</td></tr>
<tr><td>MySQL:</td><td align="right">&nbsp;&nbsp;&nbsp;0.352</td></tr>
<tr><td>SQLite 2.4:</td><td align="right">&nbsp;&nbsp;&nbsp;0.809</td></tr>
<tr><td>SQLite 2.4 (nosync):</td><td align="right">&nbsp;&nbsp;&nbsp;0.720</td></tr>
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
<tr><td>PostgreSQL:</td><td align="right">&nbsp;&nbsp;&nbsp;5.318</td></tr>
<tr><td>MySQL:</td><td align="right">&nbsp;&nbsp;&nbsp;1.555</td></tr>
<tr><td>SQLite 2.4:</td><td align="right">&nbsp;&nbsp;&nbsp;1.289</td></tr>
<tr><td>SQLite 2.4 (nosync):</td><td align="right">&nbsp;&nbsp;&nbsp;1.273</td></tr>
</table>

<p>
This test runs a set of 5000 queries that are similar in form to
those in test 3.  But now instead of being half as fast, SQLite
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
<tr><td>PostgreSQL:</td><td align="right">&nbsp;&nbsp;&nbsp;1.828</td></tr>
<tr><td>MySQL:</td><td align="right">&nbsp;&nbsp;&nbsp;9.272</td></tr>
<tr><td>SQLite 2.4:</td><td align="right">&nbsp;&nbsp;&nbsp;0.915</td></tr>
<tr><td>SQLite 2.4 (nosync):</td><td align="right">&nbsp;&nbsp;&nbsp;0.889</td></tr>
</table>

<p>
Here is a case where MySQL is over 10 times slower than SQLite.
The reason for this is unclear.
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
<tr><td>PostgreSQL:</td><td align="right">&nbsp;&nbsp;&nbsp;28.021</td></tr>
<tr><td>MySQL:</td><td align="right">&nbsp;&nbsp;&nbsp;8.565</td></tr>
<tr><td>SQLite 2.4:</td><td align="right">&nbsp;&nbsp;&nbsp;10.939</td></tr>
<tr><td>SQLite 2.4 (nosync):</td><td align="right">&nbsp;&nbsp;&nbsp;11.199</td></tr>
</table>

<p>
In this case MySQL is slightly faster than SQLite, though not by much.
The difference is believed to have to do with the fact SQLite 
handles the integers as strings instead of binary numbers.
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
<tr><td>PostgreSQL:</td><td align="right">&nbsp;&nbsp;&nbsp;48.739</td></tr>
<tr><td>MySQL:</td><td align="right">&nbsp;&nbsp;&nbsp;7.059</td></tr>
<tr><td>SQLite 2.4:</td><td align="right">&nbsp;&nbsp;&nbsp;7.868</td></tr>
<tr><td>SQLite 2.4 (nosync):</td><td align="right">&nbsp;&nbsp;&nbsp;6.720</td></tr>
</table>

<p>
When updating a text field instead of an integer field,
SQLite is slightly faster than MySQL.
</p>

<h2>Test 10: INSERTs from a SELECT</h2>
<blockquote>
BEGIN;<br>INSERT INTO t1 SELECT * FROM t2;<br>INSERT INTO t2 SELECT * FROM t1;<br>COMMIT;
</blockquote><table border=0 cellpadding=0 cellspacing=0>
<tr><td>PostgreSQL:</td><td align="right">&nbsp;&nbsp;&nbsp;54.822</td></tr>
<tr><td>MySQL:</td><td align="right">&nbsp;&nbsp;&nbsp;1.512</td></tr>
<tr><td>SQLite 2.4:</td><td align="right">&nbsp;&nbsp;&nbsp;4.423</td></tr>
<tr><td>SQLite 2.4 (nosync):</td><td align="right">&nbsp;&nbsp;&nbsp;2.386</td></tr>
</table>

<p>
The poor performance of PostgreSQL in this case appears to be due to its
synchronous behavior.  The CPU was mostly idle during the 55 second run.
</p>

<h2>Test 11: DELETE without an index</h2>
<blockquote>
DELETE FROM t2 WHERE c LIKE '%fifty%';
</blockquote><table border=0 cellpadding=0 cellspacing=0>
<tr><td>PostgreSQL:</td><td align="right">&nbsp;&nbsp;&nbsp;0.734</td></tr>
<tr><td>MySQL:</td><td align="right">&nbsp;&nbsp;&nbsp;0.888</td></tr>
<tr><td>SQLite 2.4:</td><td align="right">&nbsp;&nbsp;&nbsp;5.405</td></tr>
<tr><td>SQLite 2.4 (nosync):</td><td align="right">&nbsp;&nbsp;&nbsp;0.731</td></tr>
</table>


<h2>Test 12: DELETE with an index</h2>
<blockquote>
DELETE FROM t2 WHERE a>10 AND a<20000;
</blockquote><table border=0 cellpadding=0 cellspacing=0>
<tr><td>PostgreSQL:</td><td align="right">&nbsp;&nbsp;&nbsp;2.318</td></tr>
<tr><td>MySQL:</td><td align="right">&nbsp;&nbsp;&nbsp;2.600</td></tr>
<tr><td>SQLite 2.4:</td><td align="right">&nbsp;&nbsp;&nbsp;1.436</td></tr>
<tr><td>SQLite 2.4 (nosync):</td><td align="right">&nbsp;&nbsp;&nbsp;0.775</td></tr>
</table>


<h2>Test 13: A big INSERT after a big DELETE</h2>
<blockquote>
INSERT INTO t2 SELECT * FROM t1;
</blockquote><table border=0 cellpadding=0 cellspacing=0>
<tr><td>PostgreSQL:</td><td align="right">&nbsp;&nbsp;&nbsp;63.867</td></tr>
<tr><td>MySQL:</td><td align="right">&nbsp;&nbsp;&nbsp;1.839</td></tr>
<tr><td>SQLite 2.4:</td><td align="right">&nbsp;&nbsp;&nbsp;3.971</td></tr>
<tr><td>SQLite 2.4 (nosync):</td><td align="right">&nbsp;&nbsp;&nbsp;1.993</td></tr>
</table>

<p>
Earlier versions of SQLite would show decreasing performance after a
sequence DELETEs followed by new INSERTs.  As this test shows, the
problem has now been resolved.
</p>

<h2>Test 14: A big DELETE followed by many small INSERTs</h2>
<blockquote>
BEGIN;<br>
DELETE FROM t1;<br>
INSERT INTO t1 VALUES(1,29676,'twenty nine thousand six hundred seventy six');<br>
<i>... 2997 lines omitted</i><br>
INSERT INTO t1 VALUES(2999,37835,'thirty seven thousand eight hundred thirty five');<br>
INSERT INTO t1 VALUES(3000,97817,'ninety seven thousand eight hundred seventeen');<br>
COMMIT;<br>

</blockquote><table border=0 cellpadding=0 cellspacing=0>
<tr><td>PostgreSQL:</td><td align="right">&nbsp;&nbsp;&nbsp;1.209</td></tr>
<tr><td>MySQL:</td><td align="right">&nbsp;&nbsp;&nbsp;1.031</td></tr>
<tr><td>SQLite 2.4:</td><td align="right">&nbsp;&nbsp;&nbsp;0.298</td></tr>
<tr><td>SQLite 2.4 (nosync):</td><td align="right">&nbsp;&nbsp;&nbsp;0.282</td></tr>
</table>

<h2>Test 15: DROP TABLE</h2>
<blockquote>
DROP TABLE t1;<br>DROP TABLE t2;
</blockquote><table border=0 cellpadding=0 cellspacing=0>
<tr><td>PostgreSQL:</td><td align="right">&nbsp;&nbsp;&nbsp;0.105</td></tr>
<tr><td>MySQL:</td><td align="right">&nbsp;&nbsp;&nbsp;0.015</td></tr>
<tr><td>SQLite 2.4:</td><td align="right">&nbsp;&nbsp;&nbsp;0.472</td></tr>
<tr><td>SQLite 2.4 (nosync):</td><td align="right">&nbsp;&nbsp;&nbsp;0.232</td></tr>
</table>

<p>
SQLite is slower than the other databases when it comes to dropping tables.
This is not seen as a big problem, however, since DROP TABLE is seldom
used in speed-critical situations.
</p>

}
puts {
<p><hr /></p>
<p><a href="index.html"><img src="/goback.jpg" border=0 />
Back to the SQLite Home Page</a>
</p>

</body></html>}

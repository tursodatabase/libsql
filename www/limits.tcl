#
# Run this script to generate the limits.html output file
#
set rcsid {$Id: limits.tcl,v 1.1 2007/06/09 09:53:51 drh Exp $}
source common.tcl
header {Implementation Limits For SQLite}
puts {
<h2>Limits In SQLite</h2>

<p>
"Limits" in the context of this article means sizes or
quantities that can not be exceeded.  We are concerned
with things like the maximum number of bytes in a
BLOB or the maximum number of columns in a table.
</p>

<p>
SQLite was originally designed with a policy of avoiding
arbitrary limits.
Of course, every program that runs on a machine with finite
memory and disk space has limits of some kind.  But in SQLite, 
those limits
were not well defined.  The policy was that if it would fit
in memory and you could count it with a 32-bit integer, then
it should work.
</p>

<p>
Unfortunately, the no-limits policy has been shown to create
problems.  Because the upper bounds where not well
defined, they were not tested, and bugs (including possible
security exploits) where often found when pushing SQLite to
extremes.  For this reason, newer versions of SQLite have
well-defined limits and those limits are tested as part of
the test suite.
</p>

<p>
This article defines what the limits of SQLite are and how they
can be customized for specific applications.  The default settings
for limits are normally quite large and adequate for almost every
application.  Some applications may what to increase a limit here
or there, but we expect such needs to be rare.  More commonly,
an application might want to recompile SQLite with much lower
limits to avoid excess resource utilization in the event of
bug in higher-level SQL statement generators or to help thwart 
attackers who inject malicious SQL statements.
</p>
}
proc limititem {title text} {
  puts "<li><p><b>$title</b></p>\n$text</li>"
}
puts {
<ol>
}

limititem {Maximum length of a string or BLOB} {
<p>
The maximum number of bytes in a string or BLOB in SQLite is defined
by the preprocessor macro SQLITE_MAX_LENGTH.  The default value
of this macro is 1 billion (1 thousand million or 1,000,000,000).
You can raise or lower this value at compile-time using a command-line 
option like this:
</p>

<blockquote>-DSQLITE_MAX_LENGTH=123456789</blockquote>

<p>
The current implementation will only support a string or BLOB
length up to 2<small><sup>31</sup></small>-1 or 2147483647.  And
some built-in functions such as hex() might fail well before that
point.  In security-sensitive applications it is best not to
try to increase the maximum string and blob length.  In fact,
you might do well to lower the maximum string and blob length
to something more in the range of a few million if that is
possible.
</p>

<p>
During part of SQLite's INSERT and SELECT processing, the complete
content of each row in the database is encoded as a single BLOB.
So the SQLTIE_MAX_LENGTH parameter also determines the maximum
number of bytes in a row.
</p>
}

limititem {Maximum Number Of Columns} {
<p>
The SQLITE_MAX_COLUMN compile-time parameter is used to set an upper
bound on:
</p>

<ul>
<li>The number of columns in a table</li>
<li>The number of columns in an index</li>
<li>The number of columns in a view</li>
<li>The number of terms in the SET clause of an UPDATE statement</li>
<li>The number of columns in the result set of a SELECT statement</li>
<li>The number of terms in a GROUP BY or ORDER BY clause</li>
<li>The number of values in an INSERT statement</li>
</ul>

<p>
The default setting for SQLITE_MAX_COLUMN is 2000.  You can change it
at compile time to values as large as 32676.  You might be able to
redefine this value to be as large as billions, though nobody has ever
tried doing that so we do not know if it will work.  On the other hand, there
are people who will argument that a well-normalized database design
will never need a value larger than about 100.
</p>

<p>
In most applications, the number of columns is small - a few dozen.
There are places in the SQLite code generator that use algorithms
that are O(N&sup2;) where N is the number of columns.  
So if you redefine SQLITE_MAX_COLUMN to be a
really huge number and you generate SQL that uses a large number of
columns, you may find that 
<a href="capi3ref.html#sqlite3_prepare_v2">sqlite3_prepare_v2()</a>
runs slowly.
}

limititem {Maximum Length Of An SQL Statement} {
<p>
The maximum number of bytes in the text of an SQL statement is 
limited to SQLITE_MAX_SQL_LENGTH which defaults to 1000000.  You
can redefine this limit to be as large as the smaller of SQLITE_MAX_LENGTH
and 1073741824.  
</p>

<p>
If an SQL statement is limited to be a million bytes in length, then
obviously you will not be able to insert multi-million byte strings
by embedding them as literals inside of INSERT statements.  But
you should not do that anyway.  Use host parameters 
for your data.  Prepare short SQL statements like this:
</p>

<blockquote>
INSERT INTO tab1 VALUES(?,?,?);
</blockquote>

<p>
Then use the
<a href="capi3ref.html#sqlite3_bind_text">sqlite3_bind_XXXX()</a> functions
to bind your large string values to the SQL statement.  The use of binding
obviates the need to escape quote characters in the string, reducing the
risk of SQL injection attacks.  It is also runs faster since the large
string does not need to be parsed or copied as much.
</p>
}

limititem {Maximum Number Of Tables In A Join} {
<p>
SQLite does not support joins containing more than 64 tables.
This limit arises from the fact that the SQLite code generator
uses bitmaps with one bit per join-table in the query optimizer.
</p>
}

limititem {Maximum Depth Of An Expression Tree} {
<p>
SQLite parses expressions into a tree for processing.  During
code generation, SQLite walks this tree recursively.  The depth
of expression trees is therefore limited in order to avoid
using too much stack space.
</p>

<p>
The SQLITE_MAX_EXPR_DEPTH parameter determines the maximum expression
tree depth.  If the value is 0, then no limit is enforced.  The
current implementation has a default value of 1000.
</p>
}

limititem {Maximum Number Of Arguments On A Function} {
<p>
The SQLITE_MAX_FUNCTION_ARG parameter determines the maximum number
of parameters that can be passed to an SQL function.  The default value
of this limit is 100.  We know of no 
technical reason why SQLite would not work with functions that have 
millions of parameters.  However, we suspect that anybody who tries
to invoke a function with millions of parameters is really
trying to find security exploits in systems that use SQLite, 
not do useful work, 
and so for that reason we have set this parameter relatively low.
}

limititem {Maximum Number Of Terms In A Compound SELECT Statement} {
<p>
A compound SELECT statement is two or more SELECT statements connected
by operators UNION, UNION ALL, EXCEPT, or INTERSECT.  We call each
individual SELECT statement within a compound SELECT a "term".
</p>

<p>
The code generator in SQLite processes compound SELECT statements using
a recursive algorithm.  In order to limit the size of the stack, we
therefore limit the number of terms in a compound SELECT.  The maximum
number of terms is SQLITE_MAX_COMPOUND_SELECT which defaults to 500.
We think this is a generous allotment since in practice we almost
never see the number of terms in a compound select exceed single digits.
</p>
}

limititem {Maximum Length Of A LIKE Or GLOB Pattern} {
<p>
The pattern matching algorithm used in the default LIKE and GLOB
implementation of SQLite can exhibit O(N&sup2) performance (where
N is the number of characters in the pattern) for certain pathological
cases.  To avoid denial-of-service attacks from miscreants who are able
to specify their own LIKE or GLOB patterns, the length of the LIKE
or GLOB pattern is limited to SQLITE_MAX_LIKE_PATTERN_LENGTH bytes.
The default value of this limit is 50000.  A modern workstation can
evaluate even a pathological LIKE or GLOB pattern of 50000 bytes
relatively quickly.  The denial of service problem only comes into
play when the pattern length gets into millions of bytes.  Nevertheless,
since most useful LIKE or GLOB patterns are at most a few dozen bytes
in length, paranoid application developers may want to reduce this
parameter to something in the range of a few hundred if they know that
external users are able to generate arbitrary patterns.
</p>
}

limititem {Maximum Number Of Host Parameters In A Single SQL Statement} {
<p>
A host parameter is a place-holder in an SQL statement that is filled
in using one of the
<a href="capi3ref.html#sqlite3_bind_blob">sqlite3_bind_XXXX()</a> interfaces.
Many SQL programmers are familiar with using a question mark ("?") as a
host parameter.  SQLite also supports named host parameters prefaced
by ":", "$", or "@" and numbered host parameters of the form "?123".
</p>

<p>
Each host parameter in an SQLite statement is assigned a number.  The
numbers normally begin with 1 and increase by one with each new
parameter.  However, when the "?123" form is used, the host parameter
number is the number that follows the question mark.
</p>

<p>
The maximum value of a host parameter number is SQLITE_MAX_VARIABLE_NUMBER.
This setting defaults to 999.
</p>
}

limititem {Maximum Number Of Attached Databases} {
<p>
The <a href="lang_attach.html">ATTACH</a> statement is an SQLite extension
that allows two or more databases to be associated to the same database
connection and to operate as if they were a single database.  The number
of simulataneously attached databases is limited to SQLITE_MAX_ATTACHED
which is set to 10 by default.
The code generator in SQLite uses bitmaps
to keep track of attached databases.  That means that the number of
attached databases cannot be increased above 30 on a 32-bit machine
or 62 on a 64-bit machine.
}

limititem {Maximum Database Page Size} {
<p>
An SQLite database file is organized as pages.  The size of each
page is a power of 2 between 512 and SQLITE_MAX_PAGE_SIZE.
The default value for SQLITE_MAX_PAGE_SIZE is 32768.  The current
implementation will not support a larger value.
</p>

<p>
It used to be the case that SQLite would allocate some stack
structures whose size was proportional to the maximum page size.
For this reason, SQLite would sometimes be compiled with a smaller
maximum page size on embedded devices with limited stack memory.  But
more recent versions of SQLite put these large structures on the
heap, not on the stack, so reducing the maximum page size is no
longer necessary on embedded devices.
</p>
}

limititem {Maximum Number Of Pages In A Database File} {
<p>
SQLite is able to limit the size of a database file to prevent
the database file from growing too large and consuming too much
disk or flash space.
The SQLITE_MAX_PAGE_COUNT parameter, which is normally set to
1073741823, is the maximum number of pages allowed in a single
database file.  An attempt to insert new data that would cause
the database file to grow larger than this will return
SQLITE_FULL.
</p>

<p>
The <a href="pragma.html#pragma_max_page_count">
max_page_count PRAGMA</a> can be used to raise or lower this
limit at run-time.
</p>

<p>
Note that the transaction processing in SQLite requires two bits
of heap memory for every page in the database file.  For databases
of a few megabytes in size, this amounts to only a few hundred
bytes of heap memory.  But for gigabyte-sized database the amount
of heap memory required is getting into the kilobyte range and
for terabyte-sized databases, megabytes of heap memory must be
allocated and zeroed at each transaction.  SQLite will
support very large databases in theory, but the current implementation
is optimized for the common SQLite use cases of embedded devices
and persistent stores for desktop applications.  In other words,
SQLite is designed for use with databases sized in kilobytes or 
megabytes not gigabytes.  If you are building an application to
work with databases that are hundreds of gigabytes or more 
in size, then you should perhaps consider using a different database 
engine that is explicitly designed for such large data sets.
</p>
}

puts {</ol>}
footer $rcsid

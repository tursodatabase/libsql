#
# Run this Tcl script to generate the fileformat.html file.
#
set rcsid {$Id: fileformat.tcl,v 1.4 2000/08/04 13:49:03 drh Exp $}

puts {<html>
<head>
  <title>The SQLite file format</title>
</head>
<body bgcolor=white>
<h1 align=center>
The SQLite File Format
</h1>}
puts "<p align=center>
(This page was last modified on [lrange $rcsid 3 4] GMT)
</p>"

puts {
<p>SQLite stores each SQL table and index in a separate GDBM file.
The name of the GDBM file used to store a particular table is usually
just the table name with "<b>.tbl</b>" appended.
Consider an example:</p>
}

proc Code {body} {
  puts {<blockquote><pre>}
  regsub -all {&} [string trim $body] {\&amp;} body
  regsub -all {>} $body {\&gt;} body
  regsub -all {<} $body {\&lt;} body
  regsub -all {\(\(\(} $body {<font color="#00671f"><u>} body
  regsub -all {\)\)\)} $body {</u></font>} body
  puts $body
  puts {</pre></blockquote>}
}

Code {
$ (((rm -rf ex1)))
$ (((sqlite ex1)))
Enter ".help" for instructions
sqlite> (((create table tbl1(one varchar(10), two smallint);)))
sqlite> (((create index idx1 on tbl1(one);)))
sqlite> (((insert into tbl1 values('hello!',10);)))
sqlite> (((.exit)))
$ ls ex1
idx1.tbl  sqlite_master.tbl  tbl1.tbl
$
}

puts {
<p>The example above creates a new SQL database with a single
table named <b>tbl1</b> and a single index named <b>idx1</b>.
Three files were created for this database.  <b>tbl1.tbl</b> stores
all the data for the <b>tbl1</b> table and <b>idx1.tbl</b> stores
all the information needed by the index <b>idx1</b>.  The remaining file
<b>sqlite_master.tbl</b> holds the data for the special
built-in table called <b>sqlite_master</b>.  Every SQLite database
has an <b>sqlite_master</b> table.  This table contains the schema
for the database.  You can query the <b>sqlite_master</b> table
using ordinary SQL commands, but you cannot write to the
<b>sqlite_master</b> table.</p>

<p>The GDBM file used to store an SQL table is <em>usually</em>
just the name of the table with <b>.tbl</b> appended.  But there
are exceptions.  First, the name of the table is converted to
all lower case letters before being used to construct the filename.
This is because SQL table names are not case sensitive but Unix filenames are.
Second, if the table name contains any characters other than 
alphanumerics and underscores, the exceptional characters are encoded
as a single '+' sign.  For example:</p>
}

Code {
$ (((sqlite ex1)))
sqlite> (((create table 'Strange Table Name!'(a int, b char(30));)))
sqlite> .exit
$ (((ls ex1)))
idx1.tbl sqlite_master.tbl strange+table+name+.tbl tbl1.tbl
$
}

puts {
<h2>SQL Table File Format</h2>

<p>Each record of a GDBM file contains a key and a data.
Both key and data are arbitary bytes of any length.  The information
from an SQL table is mapped into a GDBM file as follows:</p>

<p>The GDBM key for each record of an SQL table file is a
randomly chosen integer.  The key size thus depends on the size
of an integer on the host computer.  (Typically this means "4 bytes".)
</p>

<p>If the SQL table contains N columns, then the data entry
for each record begins with N integers.  Each integer is the
offset in bytes from the beginning of the GDBM data to the 
start of the data for the corresponding column.  If the column
contains a NULL value, then its corresponding integer will
be zero.  All column data is stored as null-terminated ASCII
text strings.</p>

<p>Consider a simple example:</p>
}

Code {
$ (((rm -rf ex1)))
$ (((sqlite ex1)))
sqlite> (((create table t1(a int, b text, c text);)))
sqlite> (((insert into t1 values(10,NULL,'hello!');)))
sqlite> (((insert into t1 values(-11,'this is','a test');)))
sqlite> (((.exit)))
$ (((gdbmdump ex1/t1.tbl)))
key  : 6d1a6e03                                      m.n.
data : 0c000000 10000000 18000000 2d313100 74686973  ............-11.this
       20697300 61207465 737400                       is.a test.

key  : 6d3f90e2                                      m?..
data : 0c000000 00000000 0f000000 31300068 656c6c6f  ............10.hello
       2100                                          !.

$
}

puts {
<p>In the example above, we have created a new table named <b>t1</b>
that contains two records. The <b>gdbmdump</b> program is used to
dump the contents of the <b>t1</b> GDBM file
in a human readable format.  The source code to <b>gdbmdump</b>
is included with the SQLite distribution.  Just type "make gdbmdump"
to build it.</p>

<p>We can see in the dump of <b>t1</b> that each record
is a separate GDBM entry with a 4-byte random key.  The keys
shown are for a single sample run. If you try
this experiment yourself, you will probably get completely different
keys.<p>

<p>Because the <b>t1</b> table contains 3 columns, the data part of
each record begins with 3 integers.  In both records of the example,
the first integer
has the value 12 since the beginning of the data for the first column
begins on the 13th byte of the record.  You can see how each column's
data is stored as a null-terminated string.  For the second record,
observe that the offset integer is zero for the second column.  This
indicates that the second column contains NULL data.</p>

<h2>SQL Index File Format</h2>

<p>Each SQL index is also represented using a single GDBM file.
There is one entry in the GDBM file for each unique SQL key in the
table that is being indexed.  The GDBM key is an
arbitrary length null-terminated string which is SQL key that
is used by the index.  The data is a list of integers that correspond
to GDBM keys of entries in data table that have the corresponding
SQL key.  If the data record of the index is exactly 4 bytes in size,
then the data represents a single integer key.  If the data is greater
than 4 bytes in size, then the first 4 bytes form an integer that
tells us how many keys are in the data.  The index data record is
always sized to be a power of 2.  Unused slots at the end of the
index data record are filled with zero.</p>

<p>To illustrate, we will create an index on the example table
shown above, and add a new entry to this table that has a duplicate
SQL key.</p>
}

Code {
$ (((sqlite ex1)))
sqlite> (((create index i1 on t1(a);)))
sqlite> (((insert into t1 values(10,'another','record');)))
sqlite> (((.exit)))
$ (((gdbmdump ex1/t1.tbl)))
key  : 223100ae                                      "1..
data : 0c000000 10000000 18000000 2d313100 74686973  ............-11.this
       20697300 61207465 737400                       is.a test.

key  : a840e996                                      .@..
data : 0c000000 00000000 0f000000 31300068 656c6c6f  ............10.hello
       2100                                          !.

key  : c19e3119                                      ..1.
data : 0c000000 0f000000 17000000 31300061 6e6f7468  ............10.anoth
       65720072 65636f72 6400                        er.record.
$
}

puts {
<p>We added the new record to the <b>t1</b> table because we wanted to
have two records with the same value on column <b>a</b> since that
column is used by the <b>i1</b> index.  You can see from the dump
above that the new <b>t1</b> record is assigned another random
GDBM key.</p>

<p>Now let's look at a dump of the index file.</p>
}

Code {
$ (((gdbmdump ex1/i1.tbl)))
key  : 313000                                        10.
data : 02000000 45b4f724 6d3f90e2 00000000           ....E..$m?......

key  : 2d313100                                      -11.
data : 6d1a6e03                                      m.n.

$
}

puts {
<p>The GDBM file for the index contains only two records because
the <b>t1</b> table contains only two distinct values for
column <b>a</b>.  You can see that the GDBM keys for each record
are just the text values for <b>a</b> columns of table <b>t1</b>.
The data for each record of the index is a list of integers
where each integer is the GDBM key for an entry in the <b>t1</b>
table that has the corresponding value for the <b>a</b> column.</p>
The index entry for -11 contains only a single entry and is 4
bytes in size.  The index entry for 10 is 16 bytes in size but
contains only 2 entries.  The first integer is the number of
entires.  The two integer keys follow.  The last 4 bytes are unused.
}

puts {
<p><hr /></p>
<p><a href="index.html"><img src="/goback.jpg" border=0 />
Back to the SQLite Home Page</a>
</p>

</body></html>}

#
# Run this script to generated a datatypes.html output file
#
set rcsid {$Id: datatypes.tcl,v 1.6 2003/12/18 14:19:41 drh Exp $}

puts {<html>
<head>
  <title>Datatypes In SQLite</title>
</head>
<body bgcolor="white">
<h1 align="center">
Datatypes In SQLite
</h1>
}
puts "<p align=center>
(This page was last modified on [lrange $rcsid 3 4] UTC)
</p>"

puts {<h2>1.0 &nbsp; Typelessness</h2>
<p>
SQLite is "typeless".  This means that you can store any
kind of data you want in any column of any table, regardless of the
declared datatype of that column.  
(See the one exception to this rule in section 2.0 below.)
This behavior is a feature, not
a bug.  A database is suppose to store and retrieve data and it 
should not matter to the database what format that data is in.
The strong typing system found in most other SQL engines and
codified in the SQL language spec is a misfeature -
it is an example of the implementation showing through into the
interface.  SQLite seeks to overcome this misfeature by allowing
you to store any kind of data into any kind of column and by
allowing flexibility in the specification of datatypes.
</p>

<p>
A datatype to SQLite is any sequence of zero or more names
optionally followed by a parenthesized lists of one or two
signed integers.  Notice in particular that a datatype may
be <em>zero</em> or more names.  That means that an empty
string is a valid datatype as far as SQLite is concerned.
So you can declare tables where the datatype of each column
is left unspecified, like this:
</p>

<blockquote><pre>
CREATE TABLE ex1(a,b,c);
</pre></blockquote>

<p>
Even though SQLite allows the datatype to be omitted, it is
still a good idea to include it in your CREATE TABLE statements,
since the data type often serves as a good hint to other
programmers about what you intend to put in the column. And
if you ever port your code to another database engine, that
other engine will probably require a datatype of some kind.
SQLite accepts all the usual datatypes.  For example:
</p>

<blockquote><pre>
CREATE TABLE ex2(
  a VARCHAR(10),
  b NVARCHAR(15),
  c TEXT,
  d INTEGER,
  e FLOAT,
  f BOOLEAN,
  g CLOB,
  h BLOB,
  i TIMESTAMP,
  j NUMERIC(10,5)
  k VARYING CHARACTER (24),
  l NATIONAL VARYING CHARACTER(16)
);
</pre></blockquote>

<p>
And so forth.  Basically any sequence of names optionally followed by 
one or two signed integers in parentheses will do.
</p>

<h2>2.0 &nbsp; The INTEGER PRIMARY KEY</h2>

<p>
One exception to the typelessness of SQLite is a column whose type
is INTEGER PRIMARY KEY.  (And you must use "INTEGER" not "INT".
A column of type INT PRIMARY KEY is typeless just like any other.)
INTEGER PRIMARY KEY columns must contain a 32-bit signed integer.  Any
attempt to insert non-integer data will result in an error.
</p>

<p>
INTEGER PRIMARY KEY columns can be used to implement the equivalent
of AUTOINCREMENT.  If you try to insert a NULL into an INTEGER PRIMARY
KEY column, the column will actually be filled with a integer that is
one greater than the largest key already in the table.  Or if the
largest key is 2147483647, then the column will be filled with a
random integer.  Either way, the INTEGER PRIMARY KEY column will be
assigned a unique integer.  You can retrieve this integer using
the <b>sqlite_last_insert_rowid()</b> API function or using the
<b>last_insert_rowid()</b> SQL function in a subsequent SELECT statement.
</p>

<h2>3.0 &nbsp; Comparison and Sort Order</h2>

<p>
SQLite is typeless for the purpose of deciding what data is allowed
to be stored in a column.  But some notion of type comes into play
when sorting and comparing data.  For these purposes, a column or
an expression can be one of two types: <b>numeric</b> and <b>text</b>.
The sort or comparison may give different results depending on which
type of data is being sorted or compared.
</p>

<p>
If data is of type <b>text</b> then the comparison is determined by
the standard C data comparison functions <b>memcmp()</b> or
<b>strcmp()</b>.  The comparison looks at bytes from two inputs one
by one and returns the first non-zero difference.
Strings are '\000' terminated so shorter
strings sort before longer strings, as you would expect.
</p>

<p>
For numeric data, this situation is more complex.  If both inputs
look like well-formed numbers, then they are converted
into floating point values using <b>atof()</b> and compared numerically.
If one input is not a well-formed number but the other is, then the
number is considered to be less than the non-number.  If neither inputs
is a well-formed number, then <b>strcmp()</b> is used to do the
comparison.
</p>

<p>
Do not be confused by the fact that a column might have a "numeric"
datatype.  This does not mean that the column can contain only numbers.
It merely means that if the column does contain a number, that number
will sort in numerical order.
</p>

<p>
For both text and numeric values, NULL sorts before any other value.
A comparison of any value against NULL using operators like "&lt;" or
"&gt;=" is always false.
</p>

<h2>4.0 &nbsp; How SQLite Determines Datatypes</h2>

<p>
For SQLite version 2.6.3 and earlier, all values used the numeric datatype.
The text datatype appears in version 2.7.0 and later.  In the sequel it
is assumed that you are using version 2.7.0 or later of SQLite.
</p>

<p>
For an expression, the datatype of the result is often determined by
the outermost operator.  For example, arithmatic operators ("+", "*", "%")
always return a numeric results.  The string concatenation operator
("||") returns a text result.  And so forth.  If you are ever in doubt
about the datatype of an expression you can use the special <b>typeof()</b>
SQL function to determine what the datatype is.  For example:
</p>

<blockquote><pre>
sqlite&gt; SELECT typeof('abc'+123);
numeric
sqlite&gt; SELECT typeof('abc'||123);
text
</pre></blockquote>

<p>
For table columns, the datatype is determined by the type declaration
of the CREATE TABLE statement.  The datatype is text if and only if
the type declaration contains one or more of the following strings:
</p>

<blockquote>
BLOB<br>
CHAR<br>
CLOB</br>
TEXT
</blockquote>

<p>
The search for these strings in the type declaration is case insensitive,
of course.  If any of the above strings occur anywhere in the type
declaration, then the datatype of the column is text.  Notice that
the type "VARCHAR" contains "CHAR" as a substring so it is considered
text.</p>

<p>If none of the strings above occur anywhere in the type declaration,
then the datatype is numeric.  Note in particular that the datatype for columns
with an empty type declaration is numeric.
</p>

<h2>5.0 &nbsp; Examples</h2>

<p>
Consider the following two command sequences:
</p>

<blockquote><pre>
CREATE TABLE t1(a INTEGER UNIQUE);        CREATE TABLE t2(b TEXT UNIQUE);
INSERT INTO t1 VALUES('0');               INSERT INTO t2 VALUES(0);
INSERT INTO t1 VALUES('0.0');             INSERT INTO t2 VALUES(0.0);
</pre></blockquote>

<p>In the sequence on the left, the second insert will fail.  In this case,
the strings '0' and '0.0' are treated as numbers since they are being 
inserted into a numeric column but 0==0.0 which violates the uniqueness
constraint.  However, the second insert in the right-hand sequence works.  In
this case, the constants 0 and 0.0 are treated a strings which means that
they are distinct.</p>

<p>SQLite always converts numbers into double-precision (64-bit) floats
for comparison purposes.  This means that a long sequence of digits that
differ only in insignificant digits will compare equal if they
are in a numeric column but will compare unequal if they are in a text
column.  We have:</p>

<blockquote><pre>
INSERT INTO t1                            INSERT INTO t2
   VALUES('12345678901234567890');           VALUES(12345678901234567890);
INSERT INTO t1                            INSERT INTO t2
   VALUES('12345678901234567891');           VALUES(12345678901234567891);
</pre></blockquote>

<p>As before, the second insert on the left will fail because the comparison
will convert both strings into floating-point number first and the only
difference in the strings is in the 20-th digit which exceeds the resolution
of a 64-bit float.  In contrast, the second insert on the right will work
because in that case, the numbers being inserted are strings and are
compared using memcmp().</p>

<p>
Numeric and text types make a difference for the DISTINCT keyword too:
</p>

<blockquote><pre>
CREATE TABLE t3(a INTEGER);               CREATE TABLE t4(b TEXT);
INSERT INTO t3 VALUES('0');               INSERT INTO t4 VALUES(0);
INSERT INTO t3 VALUES('0.0');             INSERT INTO t4 VALUES(0.0);
SELECT DISTINCT * FROM t3;                SELECT DISTINCT * FROM t4;
</pre></blockquote>

<p>
The SELECT statement on the left returns a single row since '0' and '0.0'
are treated as numbers and are therefore indistinct.  But the SELECT 
statement on the right returns two rows since 0 and 0.0 are treated
a strings which are different.</p>
}

puts {
<p><hr /></p>
<p><a href="index.html"><img src="/goback.jpg" border=0 />
Back to the SQLite Home Page</a>
</p>

</body></html>}

set rcsid {$Id: datatype3.tcl,v 1.10 2004/11/19 11:59:24 danielk1977 Exp $}
source common.tcl
header {Datatypes In SQLite Version 3}
puts {
<h2>Datatypes In SQLite Version 3</h2>

<h3>1. Storage Classes</h3>

<P>Version 2 of SQLite stores all column values as ASCII text.
Version 3 enhances this by providing the ability to store integer and
real numbers in a more compact format and the capability to store
BLOB data.</P>

<P>Each value stored in an SQLite database (or manipulated by the
database engine) has one of the following storage classes:</P>
<UL>
	<LI><P><B>NULL</B>. The value is a NULL value.</P>
	<LI><P><B>INTEGER</B>. The value is a signed integer, stored in 1,
	2, 3, 4, 6, or 8 bytes depending on the magnitude of the value.</P>
	<LI><P><B>REAL</B>. The value is a floating point value, stored as
	an 8-byte IEEE floating point number.</P>
	<LI><P><B>TEXT</B>. The value is a text string, stored using the
	database encoding (UTF-8, UTF-16BE or UTF-16-LE).</P>
	<LI><P><B>BLOB</B>. The value is a blob of data, stored exactly as
	it was input.</P>
</UL>

<P>As in SQLite version 2, any column in a version 3 database except an INTEGER
PRIMARY KEY may be used to store any type of value. The exception to
this rule is described below under 'Strict Affinity Mode'.</P>

<P>All values supplied to SQLite, whether as literals embedded in SQL
statements or values bound to pre-compiled SQL statements
are assigned a storage class before the SQL statement is executed.
Under circumstances described below, the
database engine may convert values between numeric storage classes
(INTEGER and REAL) and TEXT during query execution. 
</P>

<P>Storage classes are initially assigned as follows:</P>
<UL>
	<LI><P>Values specified as literals as part of SQL statements are
	assigned storage class TEXT if they are enclosed by single or double
	quotes, INTEGER if the literal is specified as an unquoted number
	with no decimal point or exponent, REAL if the literal is an
	unquoted number with a decimal point or exponent and NULL if the
	value is a NULL. Literals with storage class BLOB are specified
        using the X'ABCD' notation.</P>
	<LI><P>Values supplied using the sqlite3_bind_* APIs are assigned
	the storage class that most closely matches the native type bound
	(i.e. sqlite3_bind_blob() binds a value with storage class BLOB).</P>
</UL>
<P>The storage class of a value that is the result of an SQL scalar
operator depends on the outermost operator of the expression.
User-defined functions may return values with any storage class. It
is not generally possible to determine the storage class of the
result of an expression at compile time.</P>

<h3>2. Column Affinity</h3>

<p>
In SQLite version 3, the type of a value is associated with the value
itself, not with the column or variable in which the value is stored.
(This is sometimes called
<a href="http://www.cliki.net/manifest%20type%20system">
manifest typing</a>.)
All other SQL databases engines that we are aware of use the more
restrictive system of static typing where the type is associated with
the container, not the value.
</p>

<p>
In order to maximize compatibility between SQLite and other database
engines, SQLite support the concept of "type affinity" on columns.
The type affinity of a column is the recommended type for data stored
in that column.  The key here is that the type is recommended, not
required.  Any column can still store any type of data, in theory.
It is just that some columns, given the choice, will prefer to use
one storage class over another.  The preferred storage class for
a column is called its "affinity".
</p>

<P>Each column in an SQLite 3 database is assigned one of the
following type affinities:</P>
<UL>
	<LI>TEXT</LI>
	<LI>NUMERIC</LI>
	<LI>INTEGER</LI>
	<LI>NONE</LI>
</UL>

<P>A column with TEXT affinity stores all data using storage classes
NULL, TEXT or BLOB. If numerical data is inserted into a column with
TEXT affinity it is converted to text form before being stored.</P>

<P>A column with NUMERIC affinity may contain values using all five
storage classes. When text data is inserted into a NUMERIC column, an
attempt is made to convert it to an integer or real number before it
is stored. If the conversion is successful, then the value is stored
using the INTEGER or REAL storage class. If the conversion cannot be
performed the value is stored using the TEXT storage class. No
attempt is made to convert NULL or blob values.</P>

<P>A column that uses INTEGER affinity behaves in the same way as a
column with NUMERIC affinity, except that if a real value with no
floating point component (or text value that converts to such) is
inserted it is converted to an integer and stored using the INTEGER
storage class.</P>

<P>A column with affinity NONE does not prefer one storage class over
another.  It makes no attempt to coerce data before
it is inserted.</P>

<h4>2.1 Determination Of Column Affinity</h4>

<P>The type affinity of a column is determined by the declared type
of the column, according to the following rules:</P>
<OL>
	<LI><P>If the datatype contains the string &quot;INT&quot; then it
	is assigned INTEGER affinity.</P>

	<LI><P>If the datatype of the column contains any of the strings
	&quot;CHAR&quot;, &quot;CLOB&quot;, or &quot;TEXT&quot; then that
	column has TEXT affinity. Notice that the type VARCHAR contains the
	string &quot;CHAR&quot; and is thus assigned TEXT affinity.</P>

	<LI><P>If the datatype for a column
         contains the string &quot;BLOB&quot; or if
        no datatype is specified then the column has affinity NONE.</P>

	<LI><P>Otherwise, the affinity is NUMERIC.</P>
</OL>

<P>If a table is created using a "CREATE TABLE &lt;table&gt; AS
SELECT..." statement, then all columns have no datatype specified
and they are given no affinity.</P>

<h4>2.2 Column Affinity Example</h4>

<blockquote>
<PRE>CREATE TABLE t1(
    t  TEXT,
    nu NUMERIC, 
    i  INTEGER,
    no BLOB
);

-- Storage classes for the following row:
-- TEXT, REAL, INTEGER, TEXT
INSERT INTO t1 VALUES('500.0', '500.0', '500.0', '500.0');

-- Storage classes for the following row:
-- TEXT, REAL, INTEGER, REAL
INSERT INTO t1 VALUES(500.0, 500.0, 500.0, 500.0);
</PRE>
</blockquote>

<h3>3. Comparison Expressions</h3>

<P>Like SQLite version 2, version 3
features the binary comparison operators '=',
'&lt;', '&lt;=', '&gt;=' and '!=', an operation to test for set
membership, 'IN', and the ternary comparison operator 'BETWEEN'.</P>
<P>The results of a comparison depend on the storage classes of the
two values being compared, according to the following rules:</P>
<UL>
	<LI><P>A value with storage class NULL is considered less than any
	other value (including another value with storage class NULL).</P>

	<LI><P>An INTEGER or REAL value is less than any TEXT or BLOB value.
	When an INTEGER or REAL is compared to another INTEGER or REAL, a
	numerical comparison is performed.</P>

	<LI><P>A TEXT value is less than a BLOB value. When two TEXT values
	are compared, the C library function memcmp() is usually used to
	determine the result. However this can be overridden, as described
	under 'User-defined collation Sequences' below.</P>

	<LI><P>When two BLOB values are compared, the result is always
	determined using memcmp().</P>
</UL>

<P>SQLite may attempt to convert values between the numeric storage
classes (INTEGER and REAL) and TEXT before performing a comparison.
For binary comparisons, this is done in the cases enumerated below.
The term "expression" used in the bullet points below means any
SQL scalar expression or literal other than a column value.</P>
<UL>
	<LI><P>When a column value is compared to the result of an
	expression, the affinity of the column is applied to the result of
	the expression before the comparison takes place.</P>

	<LI><P>When two column values are compared, if one column has
	INTEGER or NUMERIC affinity and the other does not, the NUMERIC
	affinity is applied to any values with storage class TEXT extracted
	from the non-NUMERIC column.</P>

	<LI><P>When the results of two expressions are compared, the no
        conversions occur.  The results are compared as is.  If a string
        is compared to a number, the number will always be less than the
        string.</P>
</UL>

<P>
In SQLite, the expression "a BETWEEN b AND c" is equivalent to "a &gt;= b
AND a &lt;= c", even if this means that different affinities are applied to
'a' in each of the comparisons required to evaluate the expression.
</P>

<P>Expressions of the type "a IN (SELECT b ....)" are handled by the three
rules enumerated above for binary comparisons (e.g. in a
similar manner to "a = b"). For example if 'b' is a column value
and 'a' is an expression, then the affinity of 'b' is applied to 'a'
before any comparisons take place.</P>

<P>SQLite treats the expression "a IN (x, y, z)" as equivalent to "a = z OR
a = y OR a = z".
</P>

<h4>3.1 Comparison Example</h4>

<blockquote>
<PRE>
CREATE TABLE t1(
    a TEXT,
    b NUMERIC,
    c BLOB
);

-- Storage classes for the following row:
-- TEXT, REAL, TEXT
INSERT INTO t1 VALUES('500', '500', '500');

-- 60 and 40 are converted to '60' and '40' and values are compared as TEXT.
SELECT a &lt; 60, a &lt; 40 FROM t1;
1|0

-- Comparisons are numeric. No conversions are required.
SELECT b &lt; 60, b &lt; 600 FROM t1;
0|1

-- Both 60 and 600 (storage class NUMERIC) are less than '500'
-- (storage class TEXT).
SELECT c &lt; 60, c &lt; 600 FROM t1;
0|0
</PRE>
</blockquote>
<h3>4. Operators</h3>

<P>All mathematical operators (which is to say, all operators other
than the concatenation operator &quot;||&quot;) apply NUMERIC
affinity to all operands prior to being carried out. If one or both
operands cannot be converted to NUMERIC then the result of the
operation is NULL.</P>

<P>For the concatenation operator, TEXT affinity is applied to both
operands. If either operand cannot be converted to TEXT (because it
is NULL or a BLOB) then the result of the concatenation is NULL.</P>

<h3>5. Sorting, Grouping and Compound SELECTs</h3>

<P>When values are sorted by an ORDER by clause, values with storage
class NULL come first, followed by INTEGER and REAL values
interspersed in numeric order, followed by TEXT values usually in
memcmp() order, and finally BLOB values in memcmp() order. No storage
class conversions occur before the sort.</P>

<P>When grouping values with the GROUP BY clause values with
different storage classes are considered distinct, except for INTEGER
and REAL values which are considered equal if they are numerically
equal. No affinities are applied to any values as the result of a
GROUP by clause.</P>

<P>The compound SELECT operators UNION,
INTERSECT and EXCEPT perform implicit comparisons between values.
Before these comparisons are performed an affinity may be applied to
each value. The same affinity, if any, is applied to all values that
may be returned in a single column of the compound SELECT result set.
The affinity applied is the affinity of the column returned by the
left most component SELECTs that has a column value (and not some
other kind of expression) in that position. If for a given compound
SELECT column none of the component SELECTs return a column value, no
affinity is applied to the values from that column before they are
compared.</P>

<h3>6. Other Affinity Modes</h3>

<P>The above sections describe the operation of the database engine
in 'normal' affinity mode. SQLite version 3 will feature two other affinity
modes, as follows:</P>
<UL>
	<LI><P><B>Strict affinity</B> mode. In this mode if a conversion
	between storage classes is ever required, the database engine
	returns an error and the current statement is rolled back.</P>

	<LI><P><B>No affinity</B> mode. In this mode no conversions between
	storage classes are ever performed. Comparisons between values of
	different storage classes (except for INTEGER and REAL) are always
	false.</P>
</UL>

<a name="collation"></a>
<h3>7. User-defined Collation Sequences</h3>

<p>
By default, when SQLite compares two text values, the result of the
comparison is determined using memcmp(), regardless of the encoding of the
string. SQLite v3 provides the ability for users to supply arbitrary
comparison functions, known as user-defined collation sequences, to be used
instead of memcmp().
</p>  
<p>
Aside from the default collation sequence BINARY, implemented using
memcmp(), SQLite features two extra built-in collation sequences 
intended for testing purposes, NOCASE and REVERSE:
</p>  
<UL>
	<LI><b>BINARY</b> - Compares string data using memcmp(), regardless
                            of text encoding.</LI>
	<LI><b>REVERSE</b> - Collate in the reverse order to BINARY. </LI>
	<LI><b>NOCASE</b> - The same as binary, except the 26 upper case
			    characters used by the English language are
			    folded to their lower case equivalents before
                            the comparison is performed.  </UL>


<h4>7.1 Assigning Collation Sequences from SQL</h4>

<p>
Each column of each table has a default collation type. If a collation type
other than BINARY is required, a COLLATE clause is specified as part of the
<a href="lang_createtable.html">column definition</a> to define it. 
</p>  

<p>
Whenever two text values are compared by SQLite, a collation sequence is
used to determine the results of the comparison according to the following
rules. Sections 3 and 5 of this document describe the circumstances under
which such a comparison takes place.
</p>  

<p>
For binary comparison operators (=, <, >, <= and >=) if either operand is a
column, then the default collation type of the column determines the
collation sequence to use for the comparison. If both operands are columns,
then the collation type for the left operand determines the collation
sequence used. If neither operand is a column, then the BINARY collation
sequence is used.
</p>  

<p>
The expression "x BETWEEN y and z" is equivalent to "x &gt;= y AND x &lt;=
z". The expression "x IN (SELECT y ...)" is handled in the same way as the
expression "x = y" for the purposes of determining the collation sequence
to use. The collation sequence used for expressions of the form "x IN (y, z
...)" is the default collation type of x if x is a column, or BINARY
otherwise.
</p>  

<p>
An <a href="lang_select.html">ORDER BY</a> clause that is part of a SELECT
statement may be assigned a collation sequence to be used for the sort
operation explicitly. In this case the explicit collation sequence is
always used.  Otherwise, if the expression sorted by an ORDER BY clause is
a column, then the default collation type of the column is used to
determine sort order. If the expression is not a column, then the BINARY
collation sequence is used.
</p>  

<h4>7.2 Collation Sequences Example</h4>
<p>
The examples below identify the collation sequences that would be used to
determine the results of text comparisons that may be performed by various
SQL statements. Note that a text comparison may not be required, and no
collation sequence used, in the case of numeric, blob or NULL values.
</p>
<blockquote>
<PRE>
CREATE TABLE t1(
    a,                 -- default collation type BINARY
    b COLLATE BINARY,  -- default collation type BINARY
    c COLLATE REVERSE, -- default collation type REVERSE
    d COLLATE NOCASE   -- default collation type NOCASE
);

-- Text comparison is performed using the BINARY collation sequence.
SELECT (a = b) FROM t1;

-- Text comparison is performed using the NOCASE collation sequence.
SELECT (a = d) FROM t1;

-- Text comparison is performed using the BINARY collation sequence.
SELECT (d = a) FROM t1;

-- Text comparison is performed using the REVERSE collation sequence.
SELECT ('abc' = c) FROM t1;

-- Text comparison is performed using the REVERSE collation sequence.
SELECT (c = 'abc') FROM t1;

-- Grouping is performed using the NOCASE collation sequence (i.e. values
-- 'abc' and 'ABC' are placed in the same group).
SELECT count(*) GROUP BY d FROM t1;

-- Grouping is performed using the BINARY collation sequence.
SELECT count(*) GROUP BY (d || '') FROM t1;

-- Sorting is performed using the REVERSE collation sequence.
SELECT * FROM t1 ORDER BY c;

-- Sorting is performed using the BINARY collation sequence.
SELECT * FROM t1 ORDER BY (c || '');

-- Sorting is performed using the NOCASE collation sequence.
SELECT * FROM t1 ORDER BY c COLLATE NOCASE;

</PRE>
</blockquote>

}
footer $rcsid

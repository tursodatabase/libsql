#
# Run this Tcl script to generate the sqlite.html file.
#
set rcsid {$Id: lang.tcl,v 1.29 2002/03/28 14:20:08 drh Exp $}

puts {<html>
<head>
  <title>Query Language Understood By SQLite</title>
</head>
<body bgcolor=white>
<h1 align=center>
SQL As Understood By SQLite
</h1>}
puts "<p align=center>
(This page was last modified on [lrange $rcsid 3 4] UTC)
</p>"

puts {
<p>The SQLite library understands most of the standard SQL
language.  But it does omit some features while at the same time
adding a few features of its own.  This document attempts to
describe percisely what parts of the SQL language SQLite does
and does not support.</p>

<p>In all of the syntax diagrams that follow, literal text is shown in
bold blue.  Non-terminal symbols are shown in italic red.  Operators
that are part of the syntactic markup itself are shown in black roman.</p>

<p>This document is just an overview of the SQL syntax implemented
by SQLite.  Many low-level productions are omitted.  For detailed information
on the language that SQLite understands, refer to the source code.</p>


<p>SQLite implements the follow syntax:</p>
<p><ul>
}

foreach {section} [lsort -index 0 -dictionary {
  {{CREATE TABLE} createtable}
  {{CREATE INDEX} createindex}
  {VACUUM vacuum}
  {{DROP TABLE} droptable}
  {{DROP INDEX} dropindex}
  {INSERT insert}
  {REPLACE replace}
  {DELETE delete}
  {UPDATE update}
  {SELECT select}
  {COPY copy}
  {EXPLAIN explain}
  {expression expr}
  {{BEGIN TRANSACTION} transaction}
  {PRAGMA pragma}
  {{ON CONFLICT clause} conflict}
  {{CREATE VIEW} createview}
  {{DROP VIEW} dropview}
}] {
  puts "<li><a href=\"#[lindex $section 1]\">[lindex $section 0]</a></li>"
}
puts {</ul></p>

<p>Details on the implementation of each command are provided in
the sequel.</p>
}

proc Syntax {args} {
  puts {<table cellpadding="10">}
  foreach {rule body} $args {
    puts "<tr><td align=\"right\" valign=\"top\">"
    puts "<i><font color=\"#ff3434\">$rule</font></i>&nbsp;::=</td>"
    regsub -all < $body {%LT} body
    regsub -all > $body {%GT} body
    regsub -all %LT $body {</font></b><i><font color="#ff3434">} body
    regsub -all %GT $body {</font></i><b><font color="#2c2cf0">} body
    regsub -all {[]|[*?]} $body {</font></b>&<b><font color="#2c2cf0">} body
    regsub -all "\n" [string trim $body] "<br>\n" body
    regsub -all "\n  *" $body "\n\\&nbsp;\\&nbsp;\\&nbsp;\\&nbsp;" body
    regsub -all {[|,.*()]} $body {<big>&</big>} body
    regsub -all { = } $body { <big>=</big> } body
    regsub -all {STAR} $body {<big>*</big>} body
    puts "<td><b><font color=\"#2c2cf0\">$body</font></b></td></tr>"
  }
  puts {</table>}
}
proc Operator {name} {
  return "<font color=\"#2c2cf0\"><big>$name</big></font>"
}
proc Nonterminal {name} {
  return "<i><font color=\"#ff3434\">$name</font></i>"
}
proc Keyword {name} {
  return "<font color=\"#2c2cf0\">$name</font>"
}
 

proc Section {name {label {}}} {
  puts "\n<hr />"
  if {$label!=""} {
    puts "<a name=\"$label\">"
  }
  puts "<h1>$name</h1>\n"
}

proc Example {text} {
  puts "<blockquote><pre>$text</pre></blockquote>"
}

Section {BEGIN TRANSACTION} createindex

Syntax {sql-statement} {
BEGIN [TRANSACTION [<name>]] [ON CONFLICT <conflict-algorithm>]
}
Syntax {sql-statement} {
END [TRANSACTION [<name>]]
}
Syntax {sql-statement} {
COMMIT [TRANSACTION [<name>]]
}
Syntax {sql-statement} {
ROLLBACK [TRANSACTION [<name>]]
}

puts {
<p>Beginning in version 2.0, SQLite supports transactions with
rollback and atomic commit.</p>

<p>
No changes can be made to the database except within a transaction.
Any command that changes the database (basically, any SQL command
other than SELECT) will automatically starts a transaction if
one is not already in effect.  Automatically stared transactions
are committed at the conclusion of the command.
</p>

<p>
Transactions can be started manually using the BEGIN
command. Such transactions usually persist until the next
COMMIT or ROLLBACK command. But a transaction will also 
ROLLBACK if the database is closed or if an error occurs
and the ROLLBACK conflict resolution algorithm is specified.
See the documention on the <a href="#conflict">ON CONFLICT</a>
clause for additional information about the ROLLBACK
conflict resolution algorithm.
</p>

<p>
The optional ON CONFLICT clause at the end of a BEGIN statement
can be used to changed the default conflict resolution algorithm.
The normal default is ABORT.  If an alternative is specified by
the ON CONFLICT clause of a BEGIN, then that alternative is used
as the default for all commands within the transaction.  The default
algorithm is overridden by ON CONFLICT clauses on individual
constraints within the CREATE TABLE or CREATE INDEX statements
and by the OR clauses on COPY, INSERT, and UPDATE commands.
</p>
}

Section COPY copy

Syntax {sql-statement} {
COPY [ OR <conflict-algorithm> ] <table-name> FROM <filename>
[ USING DELIMITERS <delim> ]
}


puts {
<p>The COPY command is an extension used to load large amounts of
data into a table.  It is modeled after a similar command found
in PostgreSQL.  In fact, the SQLite COPY command is specifically
designed to be able to read the output of the PostgreSQL dump
utility <b>pg_dump</b> so that data can be easily transferred from
PostgreSQL into SQLite.<p>

<p>The table-name is the name of an existing table which is to
be filled with data.  The filename is a string or identifier that
names a file from which data will be read.  The filename can be
the <b>STDIN</b> to read data from standard input.<p>

<p>Each line of the input file is converted into a single record
in the table.  Columns are separated by tabs.  If a tab occurs as
data within a column, then that tab is preceded by a baskslash "\"
character.  A baskslash in the data appears as two backslashes in
a row.  The optional USING DELIMITERS clause can specify a delimiter
other than tab.</p>

<p>If a column consists of the character "\N", that column is filled
with the value NULL.</p>

<p>The optional conflict-clause allows the specification of an alternative
constraint conflict resolution algorithm to use for this one command.
See the section titled
<a href="#conflict">ON CONFLICT</a> for additional information.</p>

<p>When the input data source is STDIN, the input can be terminated
by a line that contains only a baskslash and a dot:}
puts "\"[Operator \\.]\".</p>"

Section {CREATE INDEX} createindex

Syntax {sql-statement} {
CREATE [UNIQUE] INDEX <index-name> 
ON <table-name> ( <column-name> [, <column-name>]* )
[ ON CONFLICT <conflict-algorithm> ]
} {column-name} {
<name> [ ASC | DESC ]
}


puts {
<p>The CREATE INDEX command consists of the keywords "CREATE INDEX" followed
by the name of the new index, the keyword "ON", the name of a previously
created table that is to be indexed, and a parenthesized list of names of
columns in the table that are used for the index key.
Each column name can be followed by one of the "ASC" or "DESC" keywords
to indicate sort order, but the sort order is ignored in the current
implementation.</p>

<p>There are no arbitrary limits on the number of indices that can be
attached to a single table, nor on the number of columns in an index.</p>

<p>If the UNIQUE keyword appears between CREATE and INDEX then duplicate
index entries are not allowed.  Any attempt to insert a duplicate entry
will result in a rollback and an error message.</p>

<p>The optional conflict-clause allows the specification of al alternative
default constraint conflict resolution algorithm for this index.
This only makes sense if the UNIQUE keyword is used since otherwise
there are not constraints on the index.  The default algorithm is
ABORT.  If a COPY, INSERT, or UPDATE statement specifies a particular
conflict resolution algorithm, that algorithm is used in place of
the default algorithm specified here.
See the section titled
<a href="#conflict">ON CONFLICT</a> for additional information.</p>

<p>The exact text
of each CREATE INDEX statement is stored in the <b>sqlite_master</b>
table.  Everytime the database is opened, all CREATE INDEX statements
are read from the <b>sqlite_master</b> table and used to regenerate
SQLite's internal representation of the index layout.</p>
}


Section {CREATE TABLE} {createtable}

Syntax {sql-command} {
CREATE [TEMP | TEMPORARY] TABLE <table-name> (
  <column-def> [, <column-def>]*
  [, <constraint>]*
)
} {sql-command} {
CREATE [TEMP | TEMPORARY] TABLE <table-name> AS <select-statement>
} {column-def} {
<name> [<type>] [<column-constraint>]*
} {type} {
<typename> |
<typename> ( <number> ) |
<typename> ( <number> , <number> )
} {column-constraint} {
NOT NULL [ <conflict-clause> ] |
PRIMARY KEY [<sort-order>] [ <conflict-clause> ] |
UNIQUE [ <conflict-clause> ] |
CHECK ( <expr> ) [ <conflict-clause> ] |
DEFAULT <value>
} {constraint} {
PRIMARY KEY ( <name> [, <name>]* ) [ <conflict-clause> ]|
UNIQUE ( <name> [, <name>]* ) [ <conflict-clause> ] |
CHECK ( <expr> ) [ <conflict-clause> ]
} {conflict-clause} {
ON CONFLICT <conflict-algorithm>
}

puts {
<p>A CREATE TABLE statement is basically the keywords "CREATE TABLE"
followed by the name of a new table and a parenthesized list of column
definitions and constraints.  The table name can be either an identifier
or a string.  The only reserved table name is "<b>sqlite_master</b>" which
is the name of the table that records the database schema.</p>

<p>Each column definition is the name of the column followed by the
datatype for that column, then one or more optional column constraints.
The datatype for the column is (usually) ignored and may be omitted.
All information is stored as null-terminated strings.
The UNIQUE constraint causes an index to be created on the specified
columns.  This index must contain unique keys.
The DEFAULT constraint
specifies a default value to use when doing an INSERT.
</p>

<p>Specifying a PRIMARY KEY normally just creates a UNIQUE index
on the primary key.  However, if primary key is on a single column
that has datatype INTEGER, then that column is used internally
as the actual key of the B-Tree for the table.  This means that the column
may only hold unique integer values.  (Except for this one case,
SQLite ignores the datatype specification of columns and allows
any kind of data to be put in a column regardless of its declared
datatype.)  If a table does not have an INTEGER PRIMARY KEY column,
then the B-Tree key will be a automatically generated integer.  The
B-Tree key for a row can always be accessed using one of the
special names "<b>ROWID</b>", "<b>OID</b>", or "<b>_ROWID_</b>".
This is true regardless of whether or not there is an INTEGER
PRIMARY KEY.</p>

<p>If the "TEMP" or "TEMPORARY" keyword occurs in between "CREATE"
and "TABLE" then the table that is created is only visible to the
process that opened the database and is automatically deleted when
the database is closed.  Any indices created on a temporary table
are also temporary.  Temporary tables and indices are stored in a
separate file distinct from the main database file.</p>

<p>The optional conflict-clause following each constraint
allows the specification of an alternative default
constraint conflict resolution algorithm for that constraint.
The default is abort ABORT.  Different constraints within the same
table may have different default conflict resolution algorithms.
If an COPY, INSERT, or UPDATE command specifies a different conflict
resolution algorithm, then that algorithm is used in place of the
default algorithm specified in the CREATE TABLE statement.
See the section titled
<a href="#conflict">ON CONFLICT</a> for additional information.</p>

<p>CHECK constraints are ignored in the current implementation.
Support for CHECK constraints may be added in the future.  As of
version 2.3.0, NOT NULL, PRIMARY KEY, and UNIQUE constraints all
work.</p>

<p>There are no arbitrary limits on the number
of columns or on the number of constraints in a table.
The total amount of data in a single row is limited to about
1 megabytes.  (This limit can be increased to 16MB by changing
a single #define in the source code and recompiling.)</p>

<p>The CREATE TABLE AS form defines the table to be
the result set of a query.  The names of the table columns are
the names of the columns in the result.</p>

<p>The exact text
of each CREATE TABLE statement is stored in the <b>sqlite_master</b>
table.  Everytime the database is opened, all CREATE TABLE statements
are read from the <b>sqlite_master</b> table and used to regenerate
SQLite's internal representation of the table layout.
If the original command was a CREATE TABLE AS then then an equivalent
CREATE TABLE statement is synthesized and store in <b>sqlite_master</b>
in place of the original command.
</p>
}

Section {CREATE VIEW} {createview}

Syntax {sql-command} {
CREATE VIEW <view-name> AS <select-statement>
}

puts {
<p>The CREATE VIEW command assigns a name to a pre-packaged SELECT
statement.  Once the view is created, it can be used in the FROM clause
of another SELECT in place of a table name.
</p>

<p>You cannot COPY, INSERT or UPDATE a view.  Views are read-only.</p>
}

Section DELETE delete

Syntax {sql-statement} {
DELETE FROM <table-name> [WHERE <expression>]
}

puts {
<p>The DELETE command is used to remove records from a table.
The command consists of the "DELETE FROM" keywords followed by
the name of the table from which records are to be removed.
</p>

<p>Without a WHERE clause, all rows of the table are removed.
If a WHERE clause is supplied, then only those rows that match
the expression are removed.</p>
}


Section {DROP INDEX} dropindex

Syntax {sql-command} {
DROP INDEX <index-name>
}

puts {
<p>The DROP INDEX statement consists of the keywords "DROP INDEX" followed
by the name of the index.  The index named is completely removed from
the disk.  The only way to recover the index is to reenter the
appropriate CREATE INDEX command.</p>
}

Section {DROP TABLE} droptable

Syntax {sql-command} {
DROP TABLE <table-name>
}

puts {
<p>The DROP TABLE statement consists of the keywords "DROP TABLE" followed
by the name of the table.  The table named is completely removed from
the disk.  The table can not be recovered.  All indices associated with
the table are also deleted.</p>}

Section {DROP VIEW} dropview

Syntax {sql-command} {
DROP VIEW <view-name>
}

puts {
<p>The DROP VIEW statement consists of the keywords "DROP TABLE" followed
by the name of the view.  The view named is removed from the database.
But no actual data is modified.</p>}

Section EXPLAIN explain

Syntax {sql-statement} {
EXPLAIN <sql-statement>
}

puts {
<p>The EXPLAIN command modifier is a non-standard extension.  The
idea comes from a similar command found in PostgreSQL, but the operation
is completely different.</p>

<p>If the EXPLAIN keyword appears before any other SQLite SQL command
then instead of actually executing the command, the SQLite library will
report back the sequence of virtual machine instructions it would have
used to execute the command had the EXPLAIN keyword not been present.
For additional information about virtual machine instructions see
the <a href="arch.html">architecture description</a> or the documentation
on <a href="opcode.html">available opcodes</a> for the virtual machine.</p>
}

Section expression expr

Syntax {expression} {
<expression> <binary-op> <expression> |
<expression> <like-op> <expression> |
<unary-op> <expression> |
( <expression> ) |
<column-name> |
<table-name> . <column-name> |
<literal-value> |
<function-name> ( <expr-list> | STAR ) |
<expression> ISNULL |
<expression> NOTNULL |
<expression> [NOT] BETWEEN <expression> AND <expression> |
<expression> [NOT] IN ( <value-list> ) |
<expression> [NOT] IN ( <select-statement> ) |
( <select-statement> )
} {like-op} {
LIKE | GLOB | NOT LIKE | NOT GLOB
}

puts {
<p>This section is different from the others.  Most other sections of
this document talks about a particular SQL command.  This section does
not talk about a standalone command but about "expressions" which are 
subcomponent of most other commands.</p>

<p>SQLite understands the following binary operators, in order from
highest to lowest precedence:</p>

<blockquote><pre>
<font color="#2c2cf0"><big>*    /    %
+    -
&lt;&lt;   &gt;&gt;   &amp;    |
&lt;    &lt;=   &gt;    &gt;=
=    ==   !=   &lt;&gt;   </big>IN
AND
OR</font>
</pre></blockquote>

<p>Supported unary operaters are these:</p>

<blockquote><pre>
<font color="#2c2cf0"><big>-    +    !    ~</big></font>
</pre></blockquote>

<p>Any SQLite value can be used as part of an expression.  
For arithmetic operations, integers are treated as integers.
Strings are first converted to real numbers using <b>atof()</b>.
For comparison operators, numbers compare as numbers and strings
compare as strings.  For string comparisons, case is significant
but is only used to break a tie.
Note that there are two variations of the equals and not equals
operators.  Equals can be either}
puts "[Operator =] or [Operator ==].
The non-equals operator can be either
[Operator !=] or [Operator {&lt;&gt;}].</p>"
puts {

<p>The LIKE operator does a wildcard comparision.  The operand
to the right contains the wildcards.}
puts "A percent symbol [Operator %] in the right operand
matches any sequence of zero or more characters on the left.
An underscore [Operator _] on the right
matches any single character on the left."
puts {The LIKE operator is
not case sensitive and will match upper case characters on one
side against lower case characters on the other.
(A bug: SQLite only understands upper/lower case for 7-bit Latin
characters.  Hence the LIKE operator is case sensitive for
8-bit iso8859 characters or UTF-8 characters.  For example,
the expression <b>'a'&nbsp;LIKE&nbsp;'A'</b> is TRUE but
<b>'&aelig;'&nbsp;LIKE&nbsp;'&AElig;'</b> is FALSE.)
</p>

<p>The GLOB operator is similar to LIKE but uses the Unix
file globbing syntax for its wildcards.  Also, GLOB is case
sensitive, unlike LIKE.  Both GLOB and LIKE may be preceded by
the NOT keyword to invert the sense of the test.</p>

<p>A column name can be any of the names defined in the CREATE TABLE
statement or one of the following special identifiers: "<b>ROWID</b>",
"<b>OID</b>", or "<b>_ROWID_</b>".
These special identifiers all describe the
unique random integer key (the "row key") associated with every 
row of every table.
The special identifiers only refer to the row key if the CREATE TABLE
statement does not define a real column with the same name.  Row keys
act like read-only columns.  A row key can be used anywhere a regular
column can be used, except that you cannot change the value
of a row key in an UPDATE or INSERT statement.
"SELECT * ..." does not return the row key.</p>

<p>SELECT statements can appear in expressions as either the
right-hand operand of the IN operator or as a scalar quantity.
In both cases, the SELECT should have only a single column in its
result.  Compound SELECTs (connected with keywords like UNION or
EXCEPT) are allowed.  Any ORDER BY clause on the select is ignored.
A SELECT in an expression is evaluated once before any other processing
is performed, so none of the expressions within the select itself can
refer to quantities in the containing expression.</p>

<p>When a SELECT is the right operand of the IN operator, the IN
operator returns TRUE if the result of the left operand is any of
the values generated by the select.  The IN operator may be preceded
by the NOT keyword to invert the sense of the test.</p>

<p>When a SELECT appears within an expression but is not the right
operand of an IN operator, then the first row of the result of the
SELECT becomes the value used in the expression.  If the SELECT yields
more than one result row, all rows after the first are ignored.  If
the SELECT yeilds no rows, then the value of the SELECT is NULL.</p>

<p>Both simple and aggregate functions are supported.  A simple
function can be used in any expression.  Simple functions return
a result immediately based on their inputs.  Aggregate functions
may only be used in a SELECT statement.  Aggregate functions compute
their result across all rows of the result set.</p>

<p>The following simple functions are currently supported:</p>

<table border=0 cellpadding=10>
<tr>
<td valign="top" align="right" width=120>abs(<i>X</i>)</td>
<td valign="top">Return the absolute value of argument <i>X</i>.</td>
</tr>

<tr>
<td valign="top" align="right">coalesce(<i>X</i>,<i>Y</i>,...)</td>
<td valign="top">Return a copy of the first non-NULL argument.  If
all arguments are NULL then NULL is returned.</td>
</tr>

<tr>
<td valign="top" align="right">length(<i>X</i>)</td>
<td valign="top">Return the string length of <i>X</i> in characters.
If SQLite is configured to support UTF-8, then the number of UTF-8
characters is returned, not the number of bytes.</td>
</tr>

<tr>
<td valign="top" align="right">lower(<i>X</i>)</td>
<td valign="top">Return a copy of string <i>X</i> will all characters
converted to lower case.  The C library <b>tolower()</b> routine is used
for the conversion, which means that this function might not
work correctly on UTF-8 characters.</td>
</tr>

<tr>
<td valign="top" align="right">max(<i>X</i>,<i>Y</i>,...)</td>
<td valign="top">Return the argument with the maximum value.  Arguments
may be strings in addition to numbers.  The maximum value is determined
by the usual sort order.  Note that <b>max()</b> is a simple function when
it has 2 or more arguments but converts to an aggregate function if given
only a single argument.</td>
</tr>

<tr>
<td valign="top" align="right">min(<i>X</i>,<i>Y</i>,...)</td>
<td valign="top">Return the argument with the minimum value.  Arguments
may be strings in addition to numbers.  The mminimum value is determined
by the usual sort order.  Note that <b>min()</b> is a simple function when
it has 2 or more arguments but converts to an aggregate function if given
only a single argument.</td>
</tr>

<tr>
<td valign="top" align="right">random(*)</td>
<td valign="top">Return a random integer between -2147483648 and
+2147483647.</td>
</tr>

<tr>
<td valign="top" align="right">round(<i>X</i>)<br>round(<i>X</i>,<i>Y</i>)</td>
<td valign="top">Round off the number <i>X</i> to <i>Y</i> digits to the
right of the decimal point.  If the <i>Y</i> argument is omitted, 0 is 
assumed.</td>
</tr>

<tr>
<td valign="top" align="right">substr(<i>X</i>,<i>Y</i>,<i>Z</i>)</td>
<td valign="top">Return a substring of input string <i>X</i> that begins
with the <i>Y</i>-th character and which is <i>Z</i> characters long.
The left-most character of <i>X</i> is number 1.  If <i>Y</i> is negative
the the first character of the substring is found by counting from the
right rather than the left.  If SQLite is configured to support UTF-8,
then characters indices refer to actual UTF-8 characters, not bytes.</td>
</tr>

<tr>
<td valign="top" align="right">upper(<i>X</i>)</td>
<td valign="top">Return a copy of input string <i>X</i> converted to all
upper-case letters.  The implementation of this function uses the C library
routine <b>toupper()</b> which means it may not work correctly on 
UTF-8 strings.</td>
</tr>
</table>

<p>
The following aggregate functions are supported:
</p>

<table border=0 cellpadding=10>
<tr>
<td valign="top" align="right" width=120>avg(<i>X</i>)</td>
<td valign="top">Return the average value of all <i>X</i> within a group.</td>
</tr>

<tr>
<td valign="top" align="right">count(<i>X</i>)<br>count(*)</td>
<td valign="top">The first form return a count of the number of times
that <i>X</i> is not NULL in a group.  The second form (with no argument)
returns the total number of rows in the group.</td>
</tr>

<tr>
<td valign="top" align="right">max(<i>X</i>)</td>
<td valign="top">Return the maximum value of all values in the group.
The usual sort order is used to determine the maximum.</td>
</tr>

<tr>
<td valign="top" align="right">min(<i>X</i>)</td>
<td valign="top">Return the minimum value of all values in the group.
The usual sort order is used to determine the minimum.</td>
</tr>

<tr>
<td valign="top" align="right">sum(<i>X</i>)</td>
<td valign="top">Return the numeric sum of all values in the group.</td>
</tr>
</table>
}

Section INSERT insert

Syntax {sql-statement} {
INSERT [OR <conflict-algorithm>] INTO <table-name> [(<column-list>)] VALUES(<value-list>) |
INSERT [OR <conflict-algorithm>] INTO <table-name> [(<column-list>)] <select-statement>
}

puts {
<p>The INSERT statement comes in two basic forms.  The first form
(with the "VALUES" keyword) creates a single new row in an existing table.
If no column-list is specified then the number of values must
be the same as the number of columns in the table.  If a column-list
is specified, then the number of values must match the number of
specified columns.  Columns of the table that do not appear in the
column list are fill with the default value, or with NULL if not
default value is specified.
</p>

<p>The second form of the INSERT statement takes it data from a
SELECT statement.  The number of columns in the result of the
SELECT must exactly match the number of columns in the table if
no column list is specified, or it must match the number of columns
name in the column list.  A new entry is made in the table
for every row of the SELECT result.  The SELECT may be simple
or compound.  If the SELECT statement has an ORDER BY clause,
the ORDER BY is ignored.</p>

<p>The optional conflict-clause allows the specification of an alternative
constraint conflict resolution algorithm to use during this one command.
See the section titled
<a href="#conflict">ON CONFLICT</a> for additional information.
For compatibility with MySQL, the parser allows the use of the
single keyword "REPLACE" as an alias for "INSERT OR REPLACE".
</p>
}

Section {ON CONFLICT clause} conflict

Syntax {conflict-clause} {
ON CONFLICT <conflict-algorithm>
} {conflict-algorithm} {
ROLLBACK | ABORT | FAIL | IGNORE | REPLACE
}

puts {
<p>The ON CONFLICT clause is not a separate SQL command.  It is a
non-standard clause that can appear in many other SQL commands.
It is given its own section in this document because it is not
part of standard SQL and therefore might not be familiar.</p>

<p>The syntax for the ON CONFLICT clause is as shown above for
the CREATE TABLE, CREATE INDEX, and BEGIN TRANSACTION commands.
For the COPY, INSERT, and UPDATE commands, the keywords
"ON CONFLICT" are replaced by "OR", to make the syntax seem more
natural.  But the meaning of the clause is the same either way.</p>

<p>The ON CONFLICT clause specifies an algorithm used to resolve
constraint conflicts.  There are five choices: ROLLBACK, ABORT,
FAIL, IGNORE, and REPLACE. The default algorithm is ABORT.  This
is what they mean:</p>

<dl>
<dt><b>ROLLBACK</b></dt>
<dd><p>When a constraint violation occurs, an immediate ROLLBACK
occurs, thus ending the current transaction, and the command aborts
with a return code of SQLITE_CONSTRAINT.  If no transaction is
active (other than the implied transaction that is created on every
command) then this algorithm works the same as ABORT.</p></dd>

<dt><b>ABORT</b></dt>
<dd><p>When a constraint violation occurs, the command backs out
any prior changes it might have made and aborts with a return code
of SQLITE_CONSTRAINT.  But no ROLLBACK is executed so changes
from prior commands within the same transaction
are preserved.  This is the default behavior.</p></dd>

<dt><b>FAIL</b></dt>
<dd><p>When a constraint violation occurs, the command aborts with a
return code SQLITE_CONSTRAINT.  But any changes to the database that
the command made prior to encountering the constraint violation
are preserved and are not backed out.  For example, if an UPDATE
statement encountered a constraint violation on the 100th row that
it attempts to update, then the first 99 row changes are preserved
but changes to rows 100 and beyond never occur.</p></dd>

<dt><b>IGNORE</b></dt>
<dd><p>When a constraint violation occurs, the one row that contains
the constraint violation is not inserted or changed.  But the command
continues executing normally.  Other rows before and after the row that
contained the constraint violation continue to be inserted or updated
normally.  No error is returned.</p></dd>

<dt><b>REPLACE</b></dt>
<dd><p>When a UNIQUE constraint violation occurs, the pre-existing row
that is causing the constraint violation is removed prior to inserting
or updating the current row.  Thus the insert or update always occurs.
The command continues executing normally.  No error is returned.</p>
<p>If a NOT NULL constraint violation occurs, the NULL value is replaced
by the default value for that column.  If the column has no default
value, then the ABORT algorithm is used.</p>
</dd>
</dl>

<p>
The conflict resolution algorithm can be specified in three places,
in order from lowest to highest precedence:
</p>

<ol>
<li><p>
On a BEGIN TRANSACTION command.
</p></li>

<li><p>
On individual constraints within a CREATE TABLE or CREATE INDEX
statement.
</p></li>

<li><p>
In the OR clause of a COPY, INSERT, or UPDATE command.
</p></li>
</ol>

<p>The algorithm specified in the OR clause of a COPY, INSERT, or UPDATE
overrides any algorithm specified by a CREATE TABLE or CREATE INDEX.
The algorithm specified within a CREATE TABLE or CREATE INDEX will, in turn,
override the algorithm specified by a BEGIN TRANSACTION command.
If no algorithm is specified anywhere, the ABORT algorithm is used.</p>

}
# <p>For additional information, see 
# <a href="conflict.html">conflict.html</a>.</p>


Section PRAGMA pragma

Syntax {sql-statement} {
PRAGMA <name> = <value> |
PRAGMA <function>(<arg>)
}

puts {
<p>The PRAGMA command is used to modify the operation of the SQLite library.
The pragma command is experimental and specific pragma statements may
removed or added in future releases of SQLite.  Use this command
with caution.</p>

<p>The current implementation supports the following pragmas:</p>

<ul>
<li><p><b>PRAGMA cache_size;
       <br>PRAGMA cache_size = </b><i>Number-of-pages</i><b>;</b></p>
    <p>Query or change the maximum number of database disk pages that SQLite
    will hold in memory at once.  Each page uses about 1.5K of memory.
    The default cache size is 2000.  If you are doing UPDATEs or DELETEs
    that change many rows of a database and you do not mind if SQLite
    uses more memory, you can increase the cache size for a possible speed
    improvement.</p>
    <p>When you change the cache size using the cache_size pragma, the
    change only endures for the current session.  The cache size reverts
    to the default value when the database is closed and reopened.  Use
    the <b>default_cache_size</b> pragma to check the cache size permanently
    </p></li>

<li><p><b>PRAGMA count_changes = ON;
       <br>PRAGMA count_changes = OFF;</b></p>
    <p>When on, the COUNT_CHANGES pragma causes the callback function to
    be invoked once for each DELETE, INSERT, or UPDATE operation.  The
    argument is the number of rows that were changed.</p>

<li><p><b>PRAGMA default_cache_size;
       <br>PRAGMA default_cache_size = </b><i>Number-of-pages</i><b>;</b></p>
    <p>Query or change the maximum number of database disk pages that SQLite
    will hold in memory at once.  Each page uses about 1.5K of memory.
    This pragma works like the <b>cache_size</b> pragma with the addition
    feature that it changes the cache size persistently.  With this pragma,
    you can set the cache size once and that setting is retained and reused
    everytime you reopen the database.</p></li>

<li><p><b>PRAGMA default_synchronous;
       <br>PRAGMA default_synchronous = ON;
       <br>PRAGMA default_synchronous = OFF;</b></p>
    <p>Query or change the setting of the "synchronous" flag in
    the database.  When synchronous is on (the default), the SQLite database
    engine will pause at critical moments to make sure that data has actually
    be written to the disk surface.  (In other words, it invokes the
    equivalent of the <b>fsync()</b> system call.)  In synchronous mode,
    an SQLite database should be fully recoverable even if the operating
    system crashes or power is interrupted unexpectedly.  The penalty for
    this assurance is that some database operations take longer because the
    engine has to wait on the (relatively slow) disk drive.  The alternative
    is to turn synchronous off.  With synchronous off, SQLite continues
    processing as soon as it has handed data off to the operating system.
    If the application running SQLite crashes, the data will be safe, but
    the database could (in theory) become corrupted if the operating system
    crashes or the computer suddenly loses power.  On the other hand, some
    operations are as much as 50 or more times faster with synchronous off.
    </p>
    <p>This pragma changes the synchronous mode persistently.  Once changed,
    the mode stays as set even if the database is closed and reopened.  The
    <b>synchronous</b> pragma does the same thing but only applies the setting
    to the current session.</p>

<li><p><b>PRAGMA empty_result_callbacks = ON;
       <br>PRAGMA empty_result_callbacks = OFF;</b></p>
    <p>When on, the EMPTY_RESULT_CALLBACKS pragma causes the callback
    function to be invoked once for each query that has an empty result
    set.  The third "<b>argv</b>" parameter to the callback is set to NULL
    because there is no data to report.  But the second "<b>argc</b>" and
    fourth "<b>columnNames</b>" parameters are valid and can be used to
    determine the number and names of the columns that would have been in
    the result set had the set not been empty.</p>

<li><p><b>PRAGMA full_column_names = ON;
       <br>PRAGMA full_column_names = OFF;</b></p>
    <p>The column names reported in an SQLite callback are normally just
    the name of the column itself, except for joins when "TABLE.COLUMN"
    is used.  But when full_column_names is turned on, column names are
    always reported as "TABLE.COLUMN" even for simple queries.</p></li>

<li><p><b>PRAGMA index_info(</b><i>index-name</i><b>);</b></p>
    <p>For each column that the named index references, invoke the 
    callback function
    once with information about that column, including the column name,
    and the column number.</p>

<li><p><b>PRAGMA index_list(</b><i>table-name</i><b>);</b></p>
    <p>For each index on the named table, invoke the callback function
    once with information about that index.  Arguments include the
    index name and a flag to indicate whether or not the index must be
    unique.</p>

<li><p><b>PRAGMA parser_trace = ON;<br>PRAGMA parser_trace = OFF;</b></p>
    <p>Turn tracing of the SQL parser inside of the
    SQLite library on and off.  This is used for debugging.
    This only works if the library is compiled without the NDEBUG macro.
    </p></li>

<li><p><b>PRAGMA integrity_check;</b></p>
    <p>The command does an integrity check of the entire database.  It
    looks for out-of-order records, missing pages, and malformed records.
    If any problems are found, then a single string is returned which is
    a description of all problems.  If everything is in order, "ok" is
    returned.</p>

<li><p><b>PRAGMA synchronous;
       <br>PRAGMA synchronous = ON;
       <br>PRAGMA synchronous = OFF;</b></p>
    <p>Query or change the setting of the "synchronous" flag in
    the database for the duration of the current database connect.
    The synchronous flag reverts to its default value when the database
    is closed and reopened.  For additional information on the synchronous
    flag, see the description of the <b>default_synchronous</b> pragma.</p>
    </li>

<li><p><b>PRAGMA table_info(</b><i>table-name</i><b>);</b></p>
    <p>For each column in the named table, invoke the callback function
    once with information about that column, including the column name,
    data type, whether or not the column can be NULL, and the default
    value for the column.</p>

<li><p><b>PRAGMA vdbe_trace = ON;<br>PRAGMA vdbe_trace = OFF;</b></p>
    <p>Turn tracing of the virtual database engine inside of the
    SQLite library on and off.  This is used for debugging.</p></li>
</ul>

<p>No error message is generated if an unknown pragma is issued.
Unknown pragmas are ignored.</p>
}

Section REPLACE replace

Syntax {sql-statement} {
REPLACE INTO <table-name> [( <column-list> )] VALUES ( <value-list> ) |
REPLACE INTO <table-name> [( <column-list> )] <select-statement>
}

puts {
<p>The REPLACE command is an alias for the "INSERT OR REPLACE" variant
of the <a href="#insert">INSERT command</a>.  This alias is provided for
compatibility with MySQL.  See the 
<a href="#insert">INSERT command</a> documentation for additional
information.</p>  
}

Section SELECT select

Syntax {sql-statement} {
SELECT <result> FROM <table-list>
[WHERE <expression>]
[GROUP BY <expr-list>]
[HAVING <expression>]
[<compound-op> <select>]*
[ORDER BY <sort-expr-list>]
[LIMIT <integer> [OFFSET <integer>]]
} {result} {
<result-column> [, <result-column>]*
} {result-column} {
STAR | <expression> [ [AS] <string> ]
} {table-list} {
<table> [, <table>]*
} {table} {
<table-name> [AS <alias>] |
( <select> ) [AS <alias>]
} {sort-expr-list} {
<expr> [<sort-order>] [, <expr> [<sort-order>]]*
} {sort-order} {
ASC | DESC
} {compound_op} {
UNION | UNION ALL | INTERSECT | EXCEPT
}

puts {
<p>The SELECT statement is used to query the database.  The
result of a SELECT is zero or more rows of data where each row
has a fixed number of columns.  The number of columns in the
result is specified by the expression list in between the
SELECT and FROM keywords.  Any arbitrary expression can be used
as a result.  If a result expression is }
puts "[Operator *] then all columns of all tables are substituted"
puts {for that one expression.</p>

<p>The query is executed again one or more tables specified after
the FROM keyword.  If more than one table is specified, then the
query is against the (inner) join of the various tables.  A sub-query
in parentheses may be substituted for any table name in the FROM clause.
</p>

<p>The WHERE clause can be used to limit the number of rows over
which the query operates.  In the current implementation,
indices will only be used to
optimize the query if WHERE expression contains equality comparisons
connected by the AND operator.</p>

<p>The GROUP BY clauses causes one or more rows of the result to
be combined into a single row of output.  This is especially useful
when the result contains aggregate functions.  The expressions in
the GROUP BY clause do <em>not</em> have to be expressions that
appear in the result.  The HAVING clause is similar to WHERE except
that HAVING applies after grouping has occurred.  The HAVING expression
may refer to values, even aggregate functions, that are not in the result.</p>

<p>The ORDER BY clause causes the output rows to be sorted.  
The argument to ORDER BY is a list of expressions that are used as the
key for the sort.  The expressions do not have to be part of the
result for a simple SELECT, but in a compound SELECT each sort
expression must exactly match one of the result columns.  Each
sort expression may be optionally followed by ASC or DESC to specify
the sort order.</p>

<p>The LIMIT clause places an upper bound on the number of rows
returned in the result.  A LIMIT of 0 indicates no upper bound.
The optional OFFSET following LIMIT specifies how many
rows to skip at the beginning of the result set.</p>

<p>A compound SELECT is formed from two or more simple SELECTs connected
by one of the operators UNION, UNION ALL, INTERSECT, or EXCEPT.  In
a compound SELECT, all the constituent SELECTs must specify the
same number of result columns.  There may be only a single ORDER BY
clause at the end of the compound SELECT.  The UNION and UNION ALL
operators combine the results of the SELECTs to the right and left into
a single big table.  The difference is that in UNION all result rows
are distinct where in UNION ALL there may be duplicates.
The INTERSECT operator takes the intersection of the results of the
left and right SELECTs.  EXCEPT takes the result of left SELECT after
removing the results of the right SELECT.  When three are more SELECTs
are connected into a compound, they group from left to right.</p>
}

Section UPDATE update

Syntax {sql-statement} {
UPDATE [ OR <conflict-algorithm> ] <table-name>
SET <assignment> [, <assignment>] 
[WHERE <expression>]
} {assignment} {
<column-name> = <expression>
}

puts {
<p>The UPDATE statement is used to change the value of columns in 
selected rows of a table.  Each assignment in an UPDATE specifies
a column name to the left of the equals sign and an arbitrary expression
to the right.  The expressions may use the values of other columns.
All expressions are evaluated before any assignments are made.
A WHERE clause can be used to restrict which rows are updated.</p>

<p>The optional conflict-clause allows the specification of an alternative
constraint conflict resolution algorithm to use during this one command.
See the section titled
<a href="#conflict">ON CONFLICT</a> for additional information.</p>
}

Section VACUUM vacuum

Syntax {sql-statement} {
VACUUM [<index-or-table-name>]
}

puts {
<p>The VACUUM command is an SQLite extension modelled after a similar
command found in PostgreSQL.  If VACUUM is invoked with the name of a
table or index then it is suppose to clean up the named table or index.
In version 1.0 of SQLite, the VACUUM command would invoke 
<b>gdbm_reorganize()</b> to clean up the backend database file.
Beginning with version 2.0 of SQLite, GDBM is no longer used for
the database backend and VACUUM has become a no-op.
</p>
}


puts {
<p><hr /></p>
<p><a href="index.html"><img src="/goback.jpg" border=0 />
Back to the SQLite Home Page</a>
</p>

</body></html>}

#
# Run this Tcl script to generate the sqlite.html file.
#
set rcsid {$Id: lang.tcl,v 1.5 2000/08/04 13:49:03 drh Exp $}

puts {<html>
<head>
  <title>Query Language Understood By SQLite</title>
</head>
<body bgcolor=white>
<h1 align=center>
SQL As Understood By SQLite
</h1>}
puts "<p align=center>
(This page was last modified on [lrange $rcsid 3 4] GMT)
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


<p>SQLite implements the follow SQL commands:</p>
<p><ul>
}

foreach {section} [lsort -index 0 -dictionary {
  {{CREATE TABLE} createtable}
  {{CREATE INDEX} createindex}
  {VACUUM vacuum}
  {{DROP TABLE} droptable}
  {{DROP INDEX} dropindex}
  {INSERT insert}
  {DELETE delete}
  {UPDATE update}
  {SELECT select}
  {COPY copy}
  {EXPLAIN explain}
  {expression expr}
}] {
  puts "<li><a href=\"#[lindex $section 1]\">[lindex $section 0]</a></li>"
}
puts {</ul></p>

<p>Details on the implementation of each command are provided in
the sequel.</p>
}

proc Syntax {args} {
  puts {<table cellpadding="15">}
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

Section COPY copy

Syntax {sql-statement} {
COPY <table-name> FROM <filename>
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
a row.</p>

<p>When the input data source is STDIN, the input can be terminated
by a line that contains only a baskslash and a dot:}
puts "\"[Operator \\.]\".</p>"

Section {CREATE INDEX} createindex

Syntax {sql-statement} {
CREATE INDEX <index-name> 
ON <table-name> ( <column-name> [, <column-name>]* )
} {column-name} {
<name> [ ASC | DESC ]
}

puts {
<p>The CREATE INDEX command consists of the keywords "CREATE INDEX" followed
by the name of the new index, the keyword "ON", the name of a previously
created table that is to be indexed, and a parenthesized list of names of
columns in the table that are used for the index key.
Each column name can be followed by one of the "ASC" or "DESC" keywords
to indicate sort order, but since GDBM does not implement ordered keys,
these keywords are ignored.</p>

<p>There are no arbitrary limits on the number of indices that can be
attached to a single table, nor on the number of columns in an index.</p>

<p>The exact text
of each CREATE INDEX statement is stored in the <b>sqlite_master</b>
table.  Everytime the database is opened, all CREATE INDEX statements
are read from the <b>sqlite_master</b> table and used to regenerate
SQLite's internal representation of the index layout.</p>
}


Section {CREATE TABLE} {createtable}

Syntax {sql-command} {
CREATE TABLE <table-name> (
  <column-def> [, <column-def>]*
  [, <constraint>]*
)
} {column-def} {
<name> <type> [<column-constraint>]*
} {type} {
<typename> |
<typename> ( <number> ) |
<typename> ( <number> , <number> )
} {column-constraint} {
NOT NULL |
PRIMARY KEY [<sort-order>] |
UNIQUE |
CHECK ( <expr> ) |
DEFAULT <value>
} {constraint} {
PRIMARY KEY ( <name> [, <name>]* ) |
UNIQUE ( <name> [, <name>]* ) |
CHECK ( <expr> )
}

puts {
<p>A CREATE TABLE statement is basically the keywords "CREATE TABLE"
followed by the name of a new table and a parenthesized list of column
definitions and constraints.  The table name can be either an identifier
or a string.  The only reserved table name is "<b>sqlite_master</b>" which
is the name of the table that records the database schema.</p>

<p>Each column definition is the name of the column followed by the
datatype for that column, then one or more optional column constraints.
The datatype for the column is ignored.  All information
is stored as null-terminated strings.  The constraints are also ignored,
except that the PRIMARY KEY constraint will cause an index to be automatically
created that implements the primary key and the DEFAULT constraint
which specifies a default value to use when doing an INSERT.
The name of the primary
key index will be the table name
with "<b>__primary_key</b>" appended.  The index used for a primary key
does not show up in the <b>sqlite_master</b> table, but a GDBM file is
created for that index.</p>

<p>There are no arbitrary limits on the size of columns, on the number
of columns, or on the number of constraints in a table.</p>

<p>The exact text
of each CREATE TABLE statement is stored in the <b>sqlite_master</b>
table.  Everytime the database is opened, all CREATE TABLE statements
are read from the <b>sqlite_master</b> table and used to regenerate
SQLite's internal representation of the table layout.</p>
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
<expression> [NOT] IN ( <select> ) |
( <select> )
} {like-op} {
LIKE | GLOB | NOT LIKE | NOT GLOB
}

puts {
<p>This section is different from the others.  Every other section of
this document talks about a particular SQL command.  This section does
not talk about a standalone command but about "expressions" which are 
subcomponent of most other commands.</p>

<p>SQLite understands the following binary operators, in order from
highest to lowest precedence:</p>

<blockquote><pre>
<font color="#2c2cf0"><big>*    /
+    -
&lt;    &lt;=   &gt;    &gt;=
=    ==   !=   &lt;&gt;   </big>IN
AND
OR</font>
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
matches any single character on the left.  The LIKE operator is
not case sensitive and will match upper case characters on one
side against lower case characters on the other.</p>"
puts {

<p>The GLOB operator is similar to LIKE but uses the Unix
file globbing syntax for its wildcards.  Also, GLOB is case
sensitive, unlike LIKE.  Both GLOB and LIKE may be preceded by
the NOT keyword to invert the sense of the test.</p>

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
}

Section INSERT insert

Syntax {sql-statement} {
INSERT INTO <table-name> [( <column-list> )] VALUES ( <value-list> ) |
INSERT INTO <table-name> [( <column-list> )] <select-statement>
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
}

Section SELECT select

Syntax {sql-statement} {
SELECT <result> FROM <table-list> 
[WHERE <expression>]
[GROUP BY <expr-list>]
[HAVING <expression>]
[<compound-op> <select>]*
[ORDER BY <sort-expr-list>]
} {result} {
STAR | <result-column> [, <result-column>]*
} {result-column} {
<expression> [ [AS] <string> ]
} {table-list} {
<table-name> [, <table-name>]*
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
as a result.  If the result specification is just}
puts "[Operator *] then all columns of all tables are used as the result."
puts {</p>

<p>The query is executed again one or more tables specified after
the FROM keyword.  If more than one table is specified, then the
query is against the join of the various tables.</p>

<p>The WHERE clause can be used to limit the number of rows over
which the query operates.  Note that because of limitations of
GDBM (it uses hashing not b-trees) indices will only be used to
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
UPDATE <table-name> SET <assignment> [, <assignment>] [WHERE <expression>]
} {assignment} {
<column-name> = <expression>
}

puts {
<p>The UPDATE statement is used to change the value of columns in 
selected rows of a table.  Each assignment in an UPDATE specifies
a column name to the left of the equals sign and an arbitrary expression
to the right.  The expressions may use the values of other columns.
All expressions are evaluated before any assignments are made.
A WHERE clause can be used to restrict which rows are updated.
}

Section VACUUM vacuum

Syntax {sql-statement} {
VACUUM [<index-or-table-name>]
}

puts {
<p>The VACUUM command is an SQLite extension modelled after a similar
command found in PostgreSQL.  If VACUUM is invoked with the name of a
table or index, then the <b>gdbm_reorganize()</b> function is called
on the corresponding GDBM file.  If VACUUM is invoked with no arguments,
then <b>gdbm_reorganize()</b> is call on every GDBM file in the database.</p>

<p>It is a good idea to run VACUUM after creating large indices,
especially indices where a single index value refers to many
entries in the data table.  Reorganizing these indices will make
the underlying GDBM file much smaller and will help queries to
run much faster.</p>
}

puts {
<p></p>
}

puts {
<p><hr /></p>
<p><a href="index.html"><img src="/goback.jpg" border=0 />
Back to the SQLite Home Page</a>
</p>

</body></html>}

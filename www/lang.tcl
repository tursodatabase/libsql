#
# Run this Tcl script to generate the sqlite.html file.
#
set rcsid {$Id: lang.tcl,v 1.3 2000/06/09 03:47:19 drh Exp $}

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
  {expressions expr}
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
COPY <table-name> FROM <string>
}

Section {CREATE INDEX} createindex

Syntax {sql-statement} {
CREATE INDEX <index-name> 
ON <table-name> ( <column-name> [, <column-name>]* )
} {column-name} {
<name> [ ASC | DESC ]
}

puts {
<p>The CREATE INDEX command consists of the keywords "CREATE INDEX" followed
by the name of the new index, the keyword "ON" the name of a previously
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
<p></p>
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
the table are also reversibly deleted.</p>}

Section EXPLAIN explain

Syntax {sql-statement} {
EXPLAIN <sql-statement>
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
<expression> BETWEEN <expression> AND <expression> |
<expression> IN ( <value-list> ) |
<expression> IN ( <select> ) |
( <select> )
} {like-op} {
LIKE | GLOB | NOT LIKE | NOT GLOB
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
STAR | <expresssion> [, <expression>]*
} {table-list} {
<table-name> [, <table-name>]*
} {sort-expr-list} {
<expr> [<sort-order>] [, <expr> [<sort-order>]]*
} {sort-order} {
ASC | DESC
} {compound_op} {
UNION | UNION ALL | INTERSECT | EXCEPT
}

Section UPDATE update

Syntax {sql-statement} {
UPDATE <table-name> SET <assignment> [, <assignment>] [WHERE <expression>]
} {assignment} {
<column-name> = <expression>
}

puts {
<p>
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

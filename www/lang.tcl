#
# Run this Tcl script to generate the sqlite.html file.
#
set rcsid {$Id: lang.tcl,v 1.74 2004/10/10 17:24:55 drh Exp $}
source common.tcl
header {Query Language Understood by SQLite}
puts {
<h2>SQL As Understood By SQLite</h2>

<p>The SQLite library understands most of the standard SQL
language.  But it does <a href="omitted.html">omit some features</a>
while at the same time
adding a few features of its own.  This document attempts to
describe precisely what parts of the SQL language SQLite does
and does not support.  A list of <a href="#keywords">keywords</a> is 
given at the end.</p>

<p>In all of the syntax diagrams that follow, literal text is shown in
bold blue.  Non-terminal symbols are shown in italic red.  Operators
that are part of the syntactic markup itself are shown in black roman.</p>

<p>This document is just an overview of the SQL syntax implemented
by SQLite.  Many low-level productions are omitted.  For detailed information
on the language that SQLite understands, refer to the source code and
the grammar file "parse.y".</p>


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
  {comment comment}
  {COPY copy}
  {EXPLAIN explain}
  {expression expr}
  {{BEGIN TRANSACTION} transaction}
  {{COMMIT TRANSACTION} transaction}
  {{END TRANSACTION} transaction}
  {{ROLLBACK TRANSACTION} transaction}
  {PRAGMA pragma}
  {{ON CONFLICT clause} conflict}
  {{CREATE VIEW} createview}
  {{DROP VIEW} dropview}
  {{CREATE TRIGGER} createtrigger}
  {{DROP TRIGGER} droptrigger}
  {{ATTACH DATABASE} attach}
  {{DETACH DATABASE} detach}
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
    ## These metacharacters must be handled to undo being
    ## treated as SQL punctuation characters above.
    regsub -all {RPPLUS} $body {</font></b>)+<b><font color="#2c2cf0">} body
    regsub -all {LP} $body {</font></b>(<b><font color="#2c2cf0">} body
    regsub -all {RP} $body {</font></b>)<b><font color="#2c2cf0">} body
    ## Place the left-hand side of the rule in the 2nd table column.
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
    puts "<a name=\"$label\"></a>"
  }
  puts "<h1>$name</h1>\n"
}

proc Example {text} {
  puts "<blockquote><pre>$text</pre></blockquote>"
}


Section {ATTACH DATABASE} attach

Syntax {sql-statement} {
ATTACH [DATABASE] <database-filename> AS <database-name>
}

puts {
<p>The ATTACH DATABASE statement adds a preexisting database 
file to the current database connection.  If the filename contains 
punctuation characters it must be quoted.  The names 'main' and 
'temp' refer to the main database and the database used for 
temporary tables.  These cannot be detached.  Attached databases 
are removed using the <a href="#detach">DETACH DATABASE</a> 
statement.</p>

<p>You can read from and write to an attached database and you
can modify the schema of the attached database.  This is a new
feature of SQLite version 3.0.  In SQLite 2.8, schema changes
to attached databases were not allowed.</p>

<p>You cannot create a new table with the same name as a table in 
an attached database, but you can attach a database which contains
tables whose names are duplicates of tables in the main database.  It is 
also permissible to attach the same database file multiple times.</p>

<p>Tables in an attached database can be referred to using the syntax 
<i>database-name.table-name</i>.  If an attached table doesn't have 
a duplicate table name in the main database, it doesn't require a 
database name prefix.  When a database is attached, all of its 
tables which don't have duplicate names become the 'default' table
of that name.  Any tables of that name attached afterwards require the table 
prefix. If the 'default' table of a given name is detached, then 
the last table of that name attached becomes the new default.</p>

<p>
Transactions involving multiple attached databases are atomic,
assuming that the main database is not ":memory:".  If the main
database is ":memory:" then 
transactions continue to be atomic within each individual
database file. But if the host computer crashes in the middle
of a COMMIT where two or more database files are updated,
some of those files might get the changes where others
might not.
Atomic commit of attached databases is a new feature of SQLite version 3.0.
In SQLite version 2.8, all commits to attached databases behaved as if
the main database were ":memory:".
</p>

<p>There is a compile-time limit of 10 attached database files.</p>
}


Section {BEGIN TRANSACTION} transaction

Syntax {sql-statement} {
BEGIN [ DEFERRED | IMMEDIATE | EXCLUSIVE ] [TRANSACTION [<name>]]
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

<p>The optional transaction name is ignored. SQLite currently 
does not allow nested transactions.</p>

<p>
No changes can be made to the database except within a transaction.
Any command that changes the database (basically, any SQL command
other than SELECT) will automatically start a transaction if
one is not already in effect.  Automatically started transactions
are committed at the conclusion of the command.
</p>

<p>
Transactions can be started manually using the BEGIN
command.  Such transactions usually persist until the next
COMMIT or ROLLBACK command.  But a transaction will also 
ROLLBACK if the database is closed or if an error occurs
and the ROLLBACK conflict resolution algorithm is specified.
See the documentation on the <a href="#conflict">ON CONFLICT</a>
clause for additional information about the ROLLBACK
conflict resolution algorithm.
</p>

<p>
In SQLite version 3.0.8 and later, transactions can be deferred,
immediate, or exclusive.  Deferred means that no locks are acquired
on the database until the database is first accessed.  Thus with a
deferred transaction, the BEGIN statement itself does nothing.  Locks
are not acquired until the first read or write operation.  The first read
operation against a database creates a SHARED lock and the first
write operation creates a RESERVED lock.   Because the acquisition of
locks is deferred until they are needed, it is possible that another
thread or process could create a separate transaction and write to
the database after the BEGIN on the current thread has executed.
If the transaction is immediate, then RESERVED locks
are acquired on all databases as soon as the BEGIN command is
executed, without waiting for the
database to be used.  After a BEGIN IMMEDIATE, you are guaranteed that
no other thread or process will be able to write to the database or
do a BEGIN IMMEDIATE or BEGIN EXCLUSIVE.  Other processes can continue
to read from the database, however.  An exclusive transaction causes
EXCLUSIVE locks to be acquired on all databases.  After a BEGIN
EXCLUSIVE, you are guaranteed that no other thread or process will
be able to read or write the database until the transaction is
complete.
</p>

<p>
A description of the meaning of SHARED, RESERVED, and EXCLUSIVE locks
is available <a href="lockingv3.html">separately</a>.
</p>

<p>
The default behavior for SQLite version 3.0.8 is a
deferred transaction.  For SQLite version 3.0.0 through 3.0.7,
deferred is the only kind of transaction available.  For SQLite
version 2.8 and earlier, all transactions are exclusive.
</p>

<p>
The COMMIT command does not actually perform a commit until all
pending SQL commands finish.  Thus if two or more SELECT statements
are in the middle of processing and a COMMIT is executed, the commit
will not actually occur until all SELECT statements finish.
</p>

<p>
An attempt to execute COMMIT might result in an SQLITE_BUSY return code.
This indicates that another thread or process had a read lock on the database
that prevented the database from being updated.  When COMMIT fails in this
way, the transaction remains active and the COMMIT can be retried later
after the reader has had a chance to clear.
</p>
}


Section comment comment

Syntax {comment} {<SQL-comment> | <C-comment>
} {SQL-comment} {-- <single-line>
} {C-comment} {/STAR <multiple-lines> [STAR/]
}

puts {
<p> Comments aren't SQL commands, but can occur in SQL queries. They are 
treated as whitespace by the parser.  They can begin anywhere whitespace 
can be found, including inside expressions that span multiple lines.
</p>

<p> SQL comments only extend to the end of the current line.</p>

<p> C comments can span any number of lines.  If there is no terminating
delimiter, they extend to the end of the input.  This is not treated as
an error.  A new SQL statement can begin on a line after a multiline
comment ends.  C comments can be embedded anywhere whitespace can occur,
including inside expressions, and in the middle of other SQL statements.
C comments do not nest.  SQL comments inside a C comment will be ignored.
</p>
}


Section COPY copy

Syntax {sql-statement} {
COPY [ OR <conflict-algorithm> ] [<database-name> .] <table-name> FROM <filename>
[ USING DELIMITERS <delim> ]
}

puts {
<p>The COPY command is available in SQLite version 2.8 and earlier.
The COPY command has been removed from SQLite version 3.0 due to
complications in trying to support it in a mixed UTF-8/16 environment.
In version 3.0, the <a href="sqlite.html">command-line shell</a>
contains a new command <b>.import</b> that can be used as a substitute
for COPY.
</p>

<p>The COPY command is an extension used to load large amounts of
data into a table.  It is modeled after a similar command found
in PostgreSQL.  In fact, the SQLite COPY command is specifically
designed to be able to read the output of the PostgreSQL dump
utility <b>pg_dump</b> so that data can be easily transferred from
PostgreSQL into SQLite.</p>

<p>The table-name is the name of an existing table which is to
be filled with data.  The filename is a string or identifier that
names a file from which data will be read.  The filename can be
the <b>STDIN</b> to read data from standard input.</p>

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
ON [<database-name> .] <table-name> ( <column-name> [, <column-name>]* )
[ ON CONFLICT <conflict-algorithm> ]
} {column-name} {
<name> [ COLLATE <collation-name>] [ ASC | DESC ]
}

puts {
<p>The CREATE INDEX command consists of the keywords "CREATE INDEX" followed
by the name of the new index, the keyword "ON", the name of a previously
created table that is to be indexed, and a parenthesized list of names of
columns in the table that are used for the index key.
Each column name can be followed by one of the "ASC" or "DESC" keywords
to indicate sort order, but the sort order is ignored in the current
implementation.  Sorting is always done in ascending order.</p>

<p>The COLLATE clause following each column name defines a collating
sequence used for text entires in that column.  The default collating
sequence is the collating sequence defined for that column in the
CREATE TABLE statement.  Or if no collating sequence is otherwise defined,
the built-in BINARY collating sequence is used.</p>

<p>There are no arbitrary limits on the number of indices that can be
attached to a single table, nor on the number of columns in an index.</p>

<p>If the UNIQUE keyword appears between CREATE and INDEX then duplicate
index entries are not allowed.  Any attempt to insert a duplicate entry
will result in an error.</p>

<p>The optional conflict-clause allows the specification of an alternative
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
or <b>sqlite_temp_master</b> table, depending on whether the table
being indexed is temporary.  Every time the database is opened,
all CREATE INDEX statements
are read from the <b>sqlite_master</b> table and used to regenerate
SQLite's internal representation of the index layout.</p>

<p>Indexes are removed with the <a href="#dropindex">DROP INDEX</a> 
command.</p>
}


Section {CREATE TABLE} {createtable}

Syntax {sql-command} {
CREATE [TEMP | TEMPORARY] TABLE <table-name> (
  <column-def> [, <column-def>]*
  [, <constraint>]*
)
} {sql-command} {
CREATE [TEMP | TEMPORARY] TABLE [<database-name>.] <table-name> AS <select-statement>
} {column-def} {
<name> [<type>] [[CONSTRAINT <name>] <column-constraint>]*
} {type} {
<typename> |
<typename> ( <number> ) |
<typename> ( <number> , <number> )
} {column-constraint} {
NOT NULL [ <conflict-clause> ] |
PRIMARY KEY [<sort-order>] [ <conflict-clause> ] |
UNIQUE [ <conflict-clause> ] |
CHECK ( <expr> ) [ <conflict-clause> ] |
DEFAULT <value> |
COLLATE <collation-name>
} {constraint} {
PRIMARY KEY ( <column-list> ) [ <conflict-clause> ] |
UNIQUE ( <column-list> ) [ <conflict-clause> ] |
CHECK ( <expr> ) [ <conflict-clause> ]
} {conflict-clause} {
ON CONFLICT <conflict-algorithm>
}

puts {
<p>A CREATE TABLE statement is basically the keywords "CREATE TABLE"
followed by the name of a new table and a parenthesized list of column
definitions and constraints.  The table name can be either an identifier
or a string.  Tables names that begin with "<b>sqlite_</b>" are reserved
for use by the engine.</p>

<p>Each column definition is the name of the column followed by the
datatype for that column, then one or more optional column constraints.
The datatype for the column does not restrict what data may be put
in that column.
See <a href="datatype3.html">Datatypes In SQLite Version 3</a> for
additional information.
The UNIQUE constraint causes an index to be created on the specified
columns.  This index must contain unique keys.
The DEFAULT constraint
specifies a default value to use when doing an INSERT.
The COLLATE clause specifies what text collating function to use
when comparing text entries for the column.  The built-in BINARY
collating function is used by default.
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

<p> If a <database-name> is specified, then the table is created in 
the named database. It is an error to specify both a <database-name>
and the TEMP keyword, unless the <database-name> is "temp". If no
database name is specified, and the TEMP keyword is not present,
the table is created in the main database.</p>

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
1 megabytes in version 2.8.  In version 3.0 there is no arbitrary
limit on the amount of data in a row.</p>


<p>The CREATE TABLE AS form defines the table to be
the result set of a query.  The names of the table columns are
the names of the columns in the result.</p>

<p>The exact text
of each CREATE TABLE statement is stored in the <b>sqlite_master</b>
table.  Every time the database is opened, all CREATE TABLE statements
are read from the <b>sqlite_master</b> table and used to regenerate
SQLite's internal representation of the table layout.
If the original command was a CREATE TABLE AS then then an equivalent
CREATE TABLE statement is synthesized and store in <b>sqlite_master</b>
in place of the original command.
The text of CREATE TEMPORARY TABLE statements are stored in the
<b>sqlite_temp_master</b> table.
</p>

<p>Tables are removed using the <a href="#droptable">DROP TABLE</a> 
statement.  </p>
}


Section {CREATE TRIGGER} createtrigger

Syntax {sql-statement} {
CREATE [TEMP | TEMPORARY] TRIGGER <trigger-name> [ BEFORE | AFTER ]
<database-event> ON [<database-name> .] <table-name>
<trigger-action>
}

Syntax {sql-statement} {
CREATE [TEMP | TEMPORARY] TRIGGER <trigger-name> INSTEAD OF
<database-event> ON [<database-name> .] <view-name>
<trigger-action>
}

Syntax {database-event} {
DELETE | 
INSERT | 
UPDATE | 
UPDATE OF <column-list>
}

Syntax {trigger-action} {
[ FOR EACH ROW | FOR EACH STATEMENT ] [ WHEN <expression> ] 
BEGIN 
  <trigger-step> ; [ <trigger-step> ; ]*
END
}

Syntax {trigger-step} {
<update-statement> | <insert-statement> | 
<delete-statement> | <select-statement> 
}

puts {
<p>The CREATE TRIGGER statement is used to add triggers to the 
database schema. Triggers are database operations (the <i>trigger-action</i>) 
that are automatically performed when a specified database event (the
<i>database-event</i>) occurs.  </p>

<p>A trigger may be specified to fire whenever a DELETE, INSERT or UPDATE of a
particular database table occurs, or whenever an UPDATE of one or more
specified columns of a table are updated.</p>

<p>At this time SQLite supports only FOR EACH ROW triggers, not FOR EACH
STATEMENT triggers. Hence explicitly specifying FOR EACH ROW is optional.  FOR
EACH ROW implies that the SQL statements specified as <i>trigger-steps</i> 
may be executed (depending on the WHEN clause) for each database row being
inserted, updated or deleted by the statement causing the trigger to fire.</p>

<p>Both the WHEN clause and the <i>trigger-steps</i> may access elements of 
the row being inserted, deleted or updated using references of the form 
"NEW.<i>column-name</i>" and "OLD.<i>column-name</i>", where
<i>column-name</i> is the name of a column from the table that the trigger
is associated with. OLD and NEW references may only be used in triggers on
<i>trigger-event</i>s for which they are relevant, as follows:</p>

<table border=0 cellpadding=10>
<tr>
<td valign="top" align="right" width=120><i>INSERT</i></td>
<td valign="top">NEW references are valid</td>
</tr>
<tr>
<td valign="top" align="right" width=120><i>UPDATE</i></td>
<td valign="top">NEW and OLD references are valid</td>
</tr>
<tr>
<td valign="top" align="right" width=120><i>DELETE</i></td>
<td valign="top">OLD references are valid</td>
</tr>
</table>
</p>

<p>If a WHEN clause is supplied, the SQL statements specified as <i>trigger-steps</i> are only executed for rows for which the WHEN clause is true. If no WHEN clause is supplied, the SQL statements are executed for all rows.</p>

<p>The specified <i>trigger-time</i> determines when the <i>trigger-steps</i>
will be executed relative to the insertion, modification or removal of the
associated row.</p>

<p>An ON CONFLICT clause may be specified as part of an UPDATE or INSERT
<i>trigger-step</i>. However if an ON CONFLICT clause is specified as part of 
the statement causing the trigger to fire, then this conflict handling
policy is used instead.</p>

<p>Triggers are automatically dropped when the table that they are 
associated with is dropped.</p>

<p>Triggers may be created on views, as well as ordinary tables, by specifying
INSTEAD OF in the CREATE TRIGGER statement. If one or more ON INSERT, ON DELETE
or ON UPDATE triggers are defined on a view, then it is not an error to execute
an INSERT, DELETE or UPDATE statement on the view, respectively. Thereafter,
executing an INSERT, DELETE or UPDATE on the view causes the associated
  triggers to fire. The real tables underlying the view are not modified
  (except possibly explicitly, by a trigger program).</p>

<p><b>Example:</b></p>

<p>Assuming that customer records are stored in the "customers" table, and
that order records are stored in the "orders" table, the following trigger
ensures that all associated orders are redirected when a customer changes
his or her address:</p>
}
Example {
CREATE TRIGGER update_customer_address UPDATE OF address ON customers 
  BEGIN
    UPDATE orders SET address = new.address WHERE customer_name = old.name;
  END;
}
puts {
<p>With this trigger installed, executing the statement:</p>
}

Example {
UPDATE customers SET address = '1 Main St.' WHERE name = 'Jack Jones';
}
puts {
<p>causes the following to be automatically executed:</p>
}
Example {
UPDATE orders SET address = '1 Main St.' WHERE customer_name = 'Jack Jones';
}

puts {
<p>Note that currently, triggers may behave oddly when created on tables
  with INTEGER PRIMARY KEY fields. If a BEFORE trigger program modifies the 
  INTEGER PRIMARY KEY field of a row that will be subsequently updated by the
  statement that causes the trigger to fire, then the update may not occur. 
  The workaround is to declare the table with a PRIMARY KEY column instead
  of an INTEGER PRIMARY KEY column.</p>
}

puts {
<p>A special SQL function RAISE() may be used within a trigger-program, with the following syntax</p> 
}
Syntax {raise-function} {
RAISE ( ABORT, <error-message> ) | 
RAISE ( FAIL, <error-message> ) | 
RAISE ( ROLLBACK, <error-message> ) | 
RAISE ( IGNORE )
}
puts {
<p>When one of the first three forms is called during trigger-program execution, the specified ON CONFLICT processing is performed (either ABORT, FAIL or 
 ROLLBACK) and the current query terminates. An error code of SQLITE_CONSTRAINT is returned to the user, along with the specified error message.</p>

<p>When RAISE(IGNORE) is called, the remainder of the current trigger program,
the statement that caused the trigger program to execute and any subsequent
    trigger programs that would of been executed are abandoned. No database
    changes are rolled back.  If the statement that caused the trigger program
    to execute is itself part of a trigger program, then that trigger program
    resumes execution at the beginning of the next step.
</p>

<p>Triggers are removed using the <a href="#droptrigger">DROP TRIGGER</a>
statement.  Non-temporary triggers cannot be added on a table in an 
attached database.</p>
}


Section {CREATE VIEW} {createview}

Syntax {sql-command} {
CREATE [TEMP | TEMPORARY] VIEW [<database-name>.] <view-name> AS <select-statement>
}

puts {
<p>The CREATE VIEW command assigns a name to a pre-packaged 
<a href="#select">SELECT</a>
statement.  Once the view is created, it can be used in the FROM clause
of another SELECT in place of a table name.
</p>

<p>If the "TEMP" or "TEMPORARY" keyword occurs in between "CREATE"
and "TABLE" then the table that is created is only visible to the
process that opened the database and is automatically deleted when
the database is closed.</p>

<p> If a <database-name> is specified, then the view is created in 
the named database. It is an error to specify both a <database-name>
and the TEMP keyword, unless the <database-name> is "temp". If no
database name is specified, and the TEMP keyword is not present,
the table is created in the main database.</p>

<p>You cannot COPY, DELETE, INSERT or UPDATE a view.  Views are read-only 
in SQLite.  However, in many cases you can use a <a href="#trigger">
TRIGGER</a> on the view to accomplish the same thing.  Views are removed 
with the <a href="#dropview">DROP VIEW</a> 
command.  Non-temporary views cannot be created on tables in an attached 
database.</p>
}


Section DELETE delete

Syntax {sql-statement} {
DELETE FROM [<database-name> .] <table-name> [WHERE <expr>]
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


Section {DETACH DATABASE} detach

Syntax {sql-command} {
DETACH [DATABASE] <database-name>
}

puts {
<p>This statement detaches an additional database connection previously 
attached using the <a href="#attach">ATTACH DATABASE</a> statement.  It
is possible to have the same database file attached multiple times using 
different names, and detaching one connection to a file will leave the 
others intact.</p>

<p>This statement will fail if SQLite is in the middle of a transaction.</p>
}


Section {DROP INDEX} dropindex

Syntax {sql-command} {
DROP INDEX [<database-name> .] <index-name>
}

puts {
<p>The DROP INDEX statement removes an index added
with the <a href="#createindex">
CREATE INDEX</a> statement.  The index named is completely removed from
the disk.  The only way to recover the index is to reenter the
appropriate CREATE INDEX command.  Non-temporary indexes on tables in 
an attached database cannot be dropped.</p>

<p>The DROP INDEX statement does not reduce the size of the database 
file.  Empty space in the database is retained for later INSERTs.  To 
remove free space in the database, use the <a href="#vacuum">VACUUM</a> 
command.</p>
}


Section {DROP TABLE} droptable

Syntax {sql-command} {
DROP TABLE [<database-name>.] <table-name>
}

puts {
<p>The DROP TABLE statement removes a table added with the <a href=
"#createtable">CREATE TABLE</a> statement.  The name specified is the
table name.  It is completely removed from the database schema and the 
disk file.  The table can not be recovered.  All indices associated 
with the table are also deleted.  Non-temporary tables in an attached 
database cannot be dropped.</p>

<p>The DROP TABLE statement does not reduce the size of the database 
file.  Empty space in the database is retained for later INSERTs.  To 
remove free space in the database, use the <a href="#vacuum">VACUUM</a> 
command.</p>
}


Section {DROP TRIGGER} droptrigger
Syntax {sql-statement} {
DROP TRIGGER [<database-name> .] <trigger-name>
}
puts { 
<p>The DROP TRIGGER statement removes a trigger created by the 
<a href="#createtrigger">CREATE TRIGGER</a> statement.  The trigger is 
deleted from the database schema. Note that triggers are automatically 
dropped when the associated table is dropped.  Non-temporary triggers 
cannot be dropped on attached tables.</p>
}


Section {DROP VIEW} dropview

Syntax {sql-command} {
DROP VIEW <view-name>
}

puts {
<p>The DROP VIEW statement removes a view created by the <a href=
"#createview">CREATE VIEW</a> statement.  The name specified is the 
view name.  It is removed from the database schema, but no actual data 
in the underlying base tables is modified.  Non-temporary views in 
attached databases cannot be dropped.</p>
}


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

Syntax {expr} {
<expr> <binary-op> <expr> |
<expr> <like-op> <expr> |
<unary-op> <expr> |
( <expr> ) |
<column-name> |
<table-name> . <column-name> |
<database-name> . <table-name> . <column-name> |
<literal-value> |
<function-name> ( <expr-list> | STAR ) |
<expr> ISNULL |
<expr> NOTNULL |
<expr> [NOT] BETWEEN <expr> AND <expr> |
<expr> [NOT] IN ( <value-list> ) |
<expr> [NOT] IN ( <select-statement> ) |
<expr> [NOT] IN [<database-name> .] <table-name> |
( <select-statement> ) |
CASE [<expr>] LP WHEN <expr> THEN <expr> RPPLUS [ELSE <expr>] END
} {like-op} {
LIKE | GLOB | NOT LIKE | NOT GLOB
}

puts {
<p>This section is different from the others.  Most other sections of
this document talks about a particular SQL command.  This section does
not talk about a standalone command but about "expressions" which are 
subcomponents of most other commands.</p>

<p>SQLite understands the following binary operators, in order from
highest to lowest precedence:</p>

<blockquote><pre>
<font color="#2c2cf0"><big>||
*    /    %
+    -
&lt;&lt;   &gt;&gt;   &amp;    |
&lt;    &lt;=   &gt;    &gt;=
=    ==   !=   &lt;&gt;   </big>IN
AND   
OR</font>
</pre></blockquote>

<p>Supported unary operators are these:</p>

<blockquote><pre>
<font color="#2c2cf0"><big>-    +    !    ~</big></font>
</pre></blockquote>

<p>Any SQLite value can be used as part of an expression.  
For arithmetic operations, integers are treated as integers.
Strings are first converted to real numbers using <b>atof()</b>.
For comparison operators, numbers compare as numbers and strings
compare using the <b>strcmp()</b> function.
Note that there are two variations of the equals and not equals
operators.  Equals can be either}
puts "[Operator =] or [Operator ==].
The non-equals operator can be either
[Operator !=] or [Operator {&lt;&gt;}].
The [Operator ||] operator is \"concatenate\" - it joins together
the two strings of its operands.
The operator [Operator %] outputs the remainder of its left 
operand modulo its right operand.</p>"
puts {

<a name="like"></a>
<p>The LIKE operator does a wildcard comparison.  The operand
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
<b>'&aelig;'&nbsp;LIKE&nbsp;'&AElig;'</b> is FALSE.).  The infix 
LIKE operator is identical the user function <a href="#likeFunc">
like(<i>X</i>,<i>Y</i>)</a>.
</p>

<a name="glob"></a>
<p>The GLOB operator is similar to LIKE but uses the Unix
file globbing syntax for its wildcards.  Also, GLOB is case
sensitive, unlike LIKE.  Both GLOB and LIKE may be preceded by
the NOT keyword to invert the sense of the test.  The infix GLOB 
operator is identical the user function <a href="#globFunc">
glob(<i>X</i>,<i>Y</i>)</a>.</p>

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
EXCEPT) are allowed.
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
the SELECT yields no rows, then the value of the SELECT is NULL.</p>

<p>Both simple and aggregate functions are supported.  A simple
function can be used in any expression.  Simple functions return
a result immediately based on their inputs.  Aggregate functions
may only be used in a SELECT statement.  Aggregate functions compute
their result across all rows of the result set.</p>

<p>The functions shown below are available by default.  Additional
functions may be written in C and added to the database engine using
the <a href="capi3ref.html#cfunc">sqlite3_create_function()</a>
API.</p>

<table border=0 cellpadding=10>
<tr>
<td valign="top" align="right" width=120>abs(<i>X</i>)</td>
<td valign="top">Return the absolute value of argument <i>X</i>.</td>
</tr>

<tr>
<td valign="top" align="right">coalesce(<i>X</i>,<i>Y</i>,...)</td>
<td valign="top">Return a copy of the first non-NULL argument.  If
all arguments are NULL then NULL is returned.  There must be at least 
2 arguments.</td>
</tr>

<tr>
<a name="globFunc"></a>
<td valign="top" align="right">glob(<i>X</i>,<i>Y</i>)</td>
<td valign="top">This function is used to implement the
"<b>X GLOB Y</b>" syntax of SQLite.  The
<a href="capi3ref.html#sqlite3_create_function">sqlite3_create_function()</a> 
interface can
be used to override this function and thereby change the operation
of the <a href="#glob">GLOB</a> operator.</td>
</tr>

<tr>
<td valign="top" align="right">ifnull(<i>X</i>,<i>Y</i>)</td>
<td valign="top">Return a copy of the first non-NULL argument.  If
both arguments are NULL then NULL is returned. This behaves the same as 
<b>coalesce()</b> above.</td>
</tr>

<tr>
<td valign="top" align="right">last_insert_rowid()</td>
<td valign="top">Return the ROWID of the last row insert from this
connection to the database.  This is the same value that would be returned
from the <b>sqlite_last_insert_rowid()</b> API function.</td>
</tr>

<tr>
<td valign="top" align="right">length(<i>X</i>)</td>
<td valign="top">Return the string length of <i>X</i> in characters.
If SQLite is configured to support UTF-8, then the number of UTF-8
characters is returned, not the number of bytes.</td>
</tr>

<tr>
<a name="likeFunc"></a>
<td valign="top" align="right">like(<i>X</i>,<i>Y</i>)</td>
<td valign="top">This function is used to implement the
"<b>X LIKE Y</b>" syntax of SQL.  The
<a href="capi3ref.html#sqlite3_create_function">sqlite_create_function()</a> 
interface can
be used to override this function and thereby change the operation
of the <a href="#like">LIKE</a> operator.</td>
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
may be strings in addition to numbers.  The minimum value is determined
by the usual sort order.  Note that <b>min()</b> is a simple function when
it has 2 or more arguments but converts to an aggregate function if given
only a single argument.</td>
</tr>

<tr>
<td valign="top" align="right">nullif(<i>X</i>,<i>Y</i>)</td>
<td valign="top">Return the first argument if the arguments are different, 
otherwise return NULL.</td>
</tr>

<tr>
<td valign="top" align="right">quote(<i>X</i>)</td>
<td valign="top">This routine returns a string which is the value of
its argument suitable for inclusion into another SQL statement.
Strings are surrounded by single-quotes with escapes on interior quotes
as needed.  BLOBs are encoded as hexadecimal literals.
The current implementation of VACUUM uses this function.  The function
is also useful when writing triggers to implement undo/redo functionality.
</td>
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
<td valign="top" align="right">soundex(<i>X</i>)</td>
<td valign="top">Compute the soundex encoding of the string <i>X</i>.
The string "?000" is returned if the argument is NULL.
This function is omitted from SQLite by default.
It is only available the -DSQLITE_SOUNDEX=1 compiler option
is used when SQLite is built.</td>
</tr>

<tr>
<td valign="top" align="right">sqlite_version(*)</td>
<td valign="top">Return the version string for the SQLite library
that is running.  Example:  "2.8.0"</td>
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
<td valign="top" align="right">typeof(<i>X</i>)</td>
<td valign="top">Return the type of the expression <i>X</i>.  The only 
return values are "null", "integer", "real", "text", and "blob".
SQLite's type handling is 
explained in <a href="datatype3.html">Datatypes in SQLite Version 3</a>.</td>
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
The following aggregate functions are available by default.  Additional
aggregate functions written in C may be added using the 
<a href="capi3ref.html#sqlite3_create_function">sqlite3_create_function()</a>
API.</p>

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
<td valign="top">Return the minimum non-NULL value of all values in the group.
The usual sort order is used to determine the minimum.  NULL is only returned
if all values in the group are NULL.</td>
</tr>

<tr>
<td valign="top" align="right">sum(<i>X</i>)</td>
<td valign="top">Return the numeric sum of all values in the group.</td>
</tr>
</table>
}


Section INSERT insert

Syntax {sql-statement} {
INSERT [OR <conflict-algorithm>] INTO [<database-name> .] <table-name> [(<column-list>)] VALUES(<value-list>) |
INSERT [OR <conflict-algorithm>] INTO [<database-name> .] <table-name> [(<column-list>)] <select-statement>
}

puts {
<p>The INSERT statement comes in two basic forms.  The first form
(with the "VALUES" keyword) creates a single new row in an existing table.
If no column-list is specified then the number of values must
be the same as the number of columns in the table.  If a column-list
is specified, then the number of values must match the number of
specified columns.  Columns of the table that do not appear in the
column list are filled with the default value, or with NULL if not
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
single keyword <a href="#replace">REPLACE</a> as an alias for "INSERT OR REPLACE".
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
the CREATE TABLE and CREATE INDEX commands.  For the COPY, INSERT, and
UPDATE commands, the keywords "ON CONFLICT" are replaced by "OR", to make
the syntax seem more natural.  But the meaning of the clause is the same
either way.</p>

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
<dd><p>When a UNIQUE constraint violation occurs, the pre-existing rows
that are causing the constraint violation are removed prior to inserting
or updating the current row.  Thus the insert or update always occurs.
The command continues executing normally.  No error is returned.
If a NOT NULL constraint violation occurs, the NULL value is replaced
by the default value for that column.  If the column has no default
value, then the ABORT algorithm is used.</p>

<p>When this conflict resolution strategy deletes rows in order to
satisfy a constraint, it does not invoke delete triggers on those
rows.  But that may change in a future release.</p>

<p>The algorithm specified in the OR clause of a COPY, INSERT, or UPDATE
overrides any algorithm specified in a CREATE TABLE or CREATE INDEX.
If no algorithm is specified anywhere, the ABORT algorithm is used.</p>

}
# <p>For additional information, see 
# <a href="conflict.html">conflict.html</a>.</p>


Section PRAGMA pragma

Syntax {sql-statement} {
PRAGMA <name> [= <value>] |
PRAGMA <function>(<arg>)
}

puts {
<p>The PRAGMA command is used to modify the operation of the SQLite library.
The pragma command is experimental and specific pragma statements may be
removed or added in future releases of SQLite.  Use this command
with caution.</p>

<p>The pragmas that take an integer <b><i>value</i></b> also accept 
symbolic names.  The strings "<b>on</b>", "<b>true</b>", and "<b>yes</b>" 
are equivalent to <b>1</b>.  The strings "<b>off</b>", "<b>false</b>", 
and "<b>no</b>" are equivalent to <b>0</b>.  These strings are case-
insensitive, and do not require quotes.  An unrecognized string will be 
treated as <b>1</b>, and will not generate an error.  When the <i>value</i> 
is returned it is as an integer.</p>

<p>The current implementation supports the following pragmas:</p>

<ul>
<a name="pragma_cache_size"></a>
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
    the <a href="#pragma_default_cache_size"><b>default_cache_size</b></a> 
    pragma to check the cache size permanently.</p></li>

<li><p><b>PRAGMA database_list;</b></p>
    <p>For each open database, invoke the callback function once with
    information about that database.  Arguments include the index and 
    the name the database was attached with.  The first row will be for 
    the main database.  The second row will be for the database used to 
    store temporary tables.</p></li>

<a name="pragma_default_cache_size"></a>
<li><p><b>PRAGMA default_cache_size;
       <br>PRAGMA default_cache_size = </b><i>Number-of-pages</i><b>;</b></p>
    <p>Query or change the maximum number of database disk pages that SQLite
    will hold in memory at once.  Each page uses 1K on disk and about
    1.5K in memory.
    This pragma works like the
    <a href="#pragma_cache_size"><b>cache_size</b></a> 
    pragma with the additional
    feature that it changes the cache size persistently.  With this pragma,
    you can set the cache size once and that setting is retained and reused
    every time you reopen the database.</p></li>

<a name="pragma_default_synchronous"></a>
<li><p><b>PRAGMA default_synchronous;
       <br>PRAGMA default_synchronous = FULL; </b>(2)<b>
       <br>PRAGMA default_synchronous = NORMAL; </b>(1)<b>
       <br>PRAGMA default_synchronous = OFF; </b>(0)</p>
    <p>Query or change the setting of the "synchronous" flag in
    the database.  The first (query) form will return the setting as an 
    integer.  When synchronous is FULL (2), the SQLite database engine will
    pause at critical moments to make sure that data has actually been 
    written to the disk surface before continuing.  This ensures that if
    the operating system crashes or if there is a power failure, the database
    will be uncorrupted after rebooting.  FULL synchronous is very 
    safe, but it is also slow.  
    When synchronous is NORMAL (1, the default), the SQLite database
    engine will still pause at the most critical moments, but less often
    than in FULL mode.  There is a very small (though non-zero) chance that
    a power failure at just the wrong time could corrupt the database in
    NORMAL mode.  But in practice, you are more likely to suffer
    a catastrophic disk failure or some other unrecoverable hardware
    fault.  So NORMAL is the default mode.
    With synchronous OFF (0), SQLite continues without pausing
    as soon as it has handed data off to the operating system.
    If the application running SQLite crashes, the data will be safe, but
    the database might become corrupted if the operating system
    crashes or the computer loses power before that data has been written
    to the disk surface.  On the other hand, some
    operations are as much as 50 or more times faster with synchronous OFF.
    </p>
    <p>This pragma changes the synchronous mode persistently.  Once changed,
    the mode stays as set even if the database is closed and reopened.  The
    <a href="#pragma_synchronous"><b>synchronous</b></a> pragma does the same 
    thing but only applies the setting to the current session.
    
    </p></li>

<a name="pragma_default_temp_store"></a>
<li><p><b>PRAGMA default_temp_store;
       <br>PRAGMA default_temp_store = DEFAULT; </b>(0)<b>
       <br>PRAGMA default_temp_store = MEMORY; </b>(2)<b>
       <br>PRAGMA default_temp_store = FILE;</b> (1)</p>
    <p>Query or change the setting of the "<b>temp_store</b>" flag stored in
    the database.  When temp_store is DEFAULT (0), the compile-time value
    of the symbol TEMP_STORE is used for the temporary database.  
    When temp_store is MEMORY (2), an in-memory database is used.  
    When temp_store is FILE (1), a temporary database file on disk will be used.
    It is possible for the library compile-time symbol TEMP_STORE to override 
    this setting.  The following table summarizes this:</p>

<table cellpadding="2">
<tr><th>TEMP_STORE</th><th>temp_store</th><th>temp database location</th></tr>
<tr><td align="center">0</td><td align="center"><em>any</em></td><td align="center">file</td></tr>
<tr><td align="center">1</td><td align="center">0</td><td align="center">file</td></tr>
<tr><td align="center">1</td><td align="center">1</td><td align="center">file</td></tr>
<tr><td align="center">1</td><td align="center">2</td><td align="center">memory</td></tr>
<tr><td align="center">2</td><td align="center">0</td><td align="center">memory</td></tr>
<tr><td align="center">2</td><td align="center">1</td><td align="center">file</td></tr>
<tr><td align="center">2</td><td align="center">2</td><td align="center">memory</td></tr>
<tr><td align="center">3</td><td align="center"><em>any</em></td><td align="center">memory</td></tr>
</table>

    <p>This pragma changes the temp_store mode for whenever the database
    is opened in the future.  The temp_store mode for the current session
    is unchanged.  Use the 
    <a href="#pragma_temp_store"><b>temp_store</b></a> pragma to change the
    temp_store mode for the current session.</p></li>

<li><p><b>PRAGMA foreign_key_list(</b><i>table-name</i><b>);</b></p>
    <p>For each foreign key that references a column in the argument
    table, invoke the callback function with information about that
    foreign key. The callback function will be invoked once for each
    column in each foreign key.</p></li>

<li><p><b>PRAGMA index_info(</b><i>index-name</i><b>);</b></p>
    <p>For each column that the named index references, invoke the 
    callback function
    once with information about that column, including the column name,
    and the column number.</p></li>

<li><p><b>PRAGMA index_list(</b><i>table-name</i><b>);</b></p>
    <p>For each index on the named table, invoke the callback function
    once with information about that index.  Arguments include the
    index name and a flag to indicate whether or not the index must be
    unique.</p></li>

<li><p><b>PRAGMA integrity_check;</b></p>
    <p>The command does an integrity check of the entire database.  It
    looks for out-of-order records, missing pages, malformed records, and
    corrupt indices.
    If any problems are found, then a single string is returned which is
    a description of all problems.  If everything is in order, "ok" is
    returned.</p></li>

<li><p><b>PRAGMA parser_trace = ON; </b>(1)<b>
    <br>PRAGMA parser_trace = OFF;</b> (0)</p>
    <p>Turn tracing of the SQL parser inside of the
    SQLite library on and off.  This is used for debugging.
    This only works if the library is compiled without the NDEBUG macro.
    </p></li>

<a name="pragma_synchronous"></a>
<li><p><b>PRAGMA synchronous;
       <br>PRAGMA synchronous = FULL; </b>(2)<b>
       <br>PRAGMA synchronous = NORMAL; </b>(1)<b>
       <br>PRAGMA synchronous = OFF;</b> (0)</p>
    <p>Query or change the setting of the "synchronous" flag affecting
    the database for the duration of the current database connection.
    The synchronous flag reverts to its default value when the database
    is closed and reopened.  For additional information on the synchronous
    flag, see the description of the <a href="#pragma_default_synchronous">
    <b>default_synchronous</b></a> pragma.</p>
    </li>

<li><p><b>PRAGMA table_info(</b><i>table-name</i><b>);</b></p>
    <p>For each column in the named table, invoke the callback function
    once with information about that column, including the column name,
    data type, whether or not the column can be NULL, and the default
    value for the column.</p></li>

<a name="pragma_temp_store"></a>
<li><p><b>PRAGMA temp_store;
       <br>PRAGMA temp_store = DEFAULT; </b>(0)<b>
       <br>PRAGMA temp_store = MEMORY; </b>(2)<b>
       <br>PRAGMA temp_store = FILE;</b> (1)</p>
    <p>Query or change the setting of the "temp_store" flag affecting
    the database for the duration of the current database connection.
    The temp_store flag reverts to its default value when the database
    is closed and reopened.  For additional information on the temp_store
    flag, see the description of the <a href="#pragma_default_temp_store">
    <b>default_temp_store</b></a> pragma.  Note that it is possible for 
    the library compile-time options to override this setting. </p>

    <p>When the temp_store setting is changed, all existing temporary
    tables, indices, triggers, and viewers are immediately deleted.
    </p>
    </li>

<a name="pragma_vdbe_trace"></a>
<li><p><b>PRAGMA vdbe_trace = ON; </b>(1)<b>
    <br>PRAGMA vdbe_trace = OFF;</b> (0)</p>
    <p>Turn tracing of the virtual database engine inside of the
    SQLite library on and off.  This is used for debugging.  See the 
    <a href="vdbe.html#trace">VDBE documentation</a> for more 
    information.</p></li>
</ul>

<p>No error message is generated if an unknown pragma is issued.
Unknown pragmas are ignored.</p>
}


Section REPLACE replace

Syntax {sql-statement} {
REPLACE INTO [<database-name> .] <table-name> [( <column-list> )] VALUES ( <value-list> ) |
REPLACE INTO [<database-name> .] <table-name> [( <column-list> )] <select-statement>
}

puts {
<p>The REPLACE command is an alias for the "INSERT OR REPLACE" variant
of the <a href="#insert">INSERT</a> command.  This alias is provided for
compatibility with MySQL.  See the 
<a href="#insert">INSERT</a> command documentation for additional
information.</p>  
}


Section SELECT select

Syntax {sql-statement} {
SELECT [ALL | DISTINCT] <result> [FROM <table-list>]
[WHERE <expr>]
[GROUP BY <expr-list>]
[HAVING <expr>]
[<compound-op> <select>]*
[ORDER BY <sort-expr-list>]
[LIMIT <integer> [LP OFFSET | , RP <integer>]]
} {result} {
<result-column> [, <result-column>]*
} {result-column} {
STAR | <table-name> . STAR | <expr> [ [AS] <string> ]
} {table-list} {
<table> [<join-op> <table> <join-args>]*
} {table} {
<table-name> [AS <alias>] |
( <select> ) [AS <alias>]
} {join-op} {
, | [NATURAL] [LEFT | RIGHT | FULL] [OUTER | INNER | CROSS] JOIN
} {join-args} {
[ON <expr>] [USING ( <id-list> )]
} {sort-expr-list} {
<expr> [<sort-order>] [, <expr> [<sort-order>]]*
} {sort-order} {
[ COLLATE <collation-name> ] [ ASC | DESC ]
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
puts {for that one expression.  If the expression is the name of}
puts "a table followed by [Operator .*] then the result is all columns"
puts {in that one table.</p>

<p>The DISTINCT keyword causes a subset of result rows to be returned, 
in which each result row is different.  NULL values are not treated as 
distinct from each other.  The default behavior is that all result rows 
be returned, which can be made explicit with the keyword ALL.</p>

<p>The query is executed against one or more tables specified after
the FROM keyword.  If multiple tables names are separated by commas,
then the query is against the cross join of the various tables.
The full SQL-92 join syntax can also be used to specify joins.
A sub-query
in parentheses may be substituted for any table name in the FROM clause.
The entire FROM clause may be omitted, in which case the result is a
single row consisting of the values of the expression list.
</p>

<p>The WHERE clause can be used to limit the number of rows over
which the query operates.</p>

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
sort expression may be optionally followed by a COLLATE keyword and
the name of a collating function used for ordering text and/or
keywords ASC or DESC to specify the sort order.</p>

<p>The LIMIT clause places an upper bound on the number of rows
returned in the result.  A negative LIMIT indicates no upper bound.
The optional OFFSET following LIMIT specifies how many
rows to skip at the beginning of the result set.
In a compound query, the LIMIT clause may only appear on the
final SELECT statement.
The limit is applied to the entire query not
to the individual SELECT statement to which it is attached.
</p>

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
UPDATE [ OR <conflict-algorithm> ] [<database-name> .] <table-name>
SET <assignment> [, <assignment>]*
[WHERE <expr>]
} {assignment} {
<column-name> = <expr>
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
<p>The VACUUM command is an SQLite extension modeled after a similar
command found in PostgreSQL.  If VACUUM is invoked with the name of a
table or index then it is suppose to clean up the named table or index.
In version 1.0 of SQLite, the VACUUM command would invoke 
<b>gdbm_reorganize()</b> to clean up the backend database file.</p>

<p>
VACUUM became a no-op when the GDBM backend was removed from
SQLITE in version 2.0.0.
VACUUM was reimplemented in version 2.8.1.
The index or table name argument is now ignored.
</p>

<p>When an object (table, index, or trigger) is dropped from the 
database, it leaves behind empty space.  This makes the database 
file larger than it needs to be, but can speed up inserts.  In time 
inserts and deletes can leave the database file structure fragmented, 
which slows down disk access to the database contents.

The VACUUM command cleans
the main database by copying its contents to a temporary database file and 
reloading the original database file from the copy.  This eliminates 
free pages,  aligns table data to be contiguous, and otherwise cleans 
up the database file structure. It is not possible to perform the same
process on an attached database file.</p>

<p>This command will fail if there is an active transaction.  This 
command has no effect on an in-memory database.</p>
}


Section {SQLite keywords} keywords

puts {
<p>The following keywords are used by SQLite.  Most are either reserved 
words in SQL-92 or were listed as potential reserved words.  Those which 
aren't are shown in italics.  Not all of these words are actually used
by SQLite.  Keywords are not reserved in SQLite.  Any keyword can be used 
as an identifier for SQLite objects (columns, databases, indexes, tables, 
triggers, views, ...) but must generally be enclosed by brackets or 
quotes to avoid confusing the parser.  Keyword matching in SQLite is 
case-insensitive.</p>

<p>Keywords can be used as identifiers in three ways:</p>

<table>
<tr>	<td width=12%> 'keyword'
	<td>Interpreted as a literal string if it occurs in a legal string 
	context, otherwise as an identifier.
<tr>	<td> "keyword"
	<td>Interpreted as an identifier if it matches a known identifier 
	and occurs in a legal identifier context, otherwise as a string. 
<tr>	<td> [keyword]
	<td> Always interpreted as an identifier. (This notation is used 
	by MS Access and SQL Server.)
</table>

<h2>Fallback Keywords</h2>

<p>These keywords can be used as identifiers for SQLite objects without 
delimiters.</p>
}

proc keyword_list {x} {
  puts "<p>"
  foreach k $x {
    if {[string index $k 0]=="*"} {
      set nonstandard 1
      set k [string range $k 1 end]
    } else {
      set nonstandard 0
    }
    if {$nonstandard} {
      puts "<i>$k</i> &nbsp;&nbsp;"
    } else {
      puts "$k &nbsp;&nbsp;"
    }
  }
  puts "</p>\n"
}

keyword_list {
  *ABORT
  AFTER
  ASC
  *ATTACH
  BEFORE
  BEGIN
  DEFERRED
  CASCADE 
  *CLUSTER 
  *CONFLICT
  *COPY
  CROSS
  *DATABASE
  *DELIMITERS
  DESC
  *DETACH
  EACH
  END
  *EXPLAIN
  *FAIL
  FOR
  FULL
  IGNORE
  IMMEDIATE
  INITIALLY
  INNER
  *INSTEAD
  KEY
  LEFT
  MATCH 
  NATURAL
  OF
  *OFFSET
  OUTER
  *PRAGMA
  *RAISE
  *REPLACE
  RESTRICT
  RIGHT
  *ROW
  *STATEMENT
  *TEMP
  TEMPORARY
  TRIGGER 
  *VACUUM
  VIEW
}
puts {

<h2>Normal keywords</h2>

<p>These keywords can be used as identifiers for SQLite objects, but 
must be enclosed in brackets or quotes for SQLite to recognize them as 
an identifier.</p>
}

keyword_list {
  ALL
  AND
  AS
  BETWEEN
  BY
  CASE
  CHECK
  COLLATE
  COMMIT
  CONSTRAINT
  CREATE 
  DEFAULT
  DEFERRABLE
  DELETE
  DISTINCT
  DROP
  ELSE
  EXCEPT
  FOREIGN
  FROM 
  *GLOB
  GROUP
  HAVING
  IN
  *INDEX
  INSERT
  INTERSECT
  INTO
  IS 
  *ISNULL
  JOIN
  LIKE
  LIMIT
  NOT
  *NOTNULL
  NULL
  ON
  OR
  ORDER 
  PRIMARY
  REFERENCES
  ROLLBACK
  SELECT
  SET
  TABLE
  THEN
  TRANSACTION
  UNION 
  UNIQUE
  UPDATE
  USING
  VALUES
  WHEN
  WHERE
}

puts {
<h2>Special words</h2>

<p>The following are not keywords in SQLite, but are used as names of 
system objects.  They can be used as an identifier for a different 
type of object.</p>
}

keyword_list {
  *_ROWID_
  *MAIN
  OID
  *ROWID
  *SQLITE_MASTER
  *SQLITE_TEMP_MASTER
}

footer $rcsid

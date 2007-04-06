#
# Run this Tcl script to generate the lang-*.html files.
#
set rcsid {$Id: lang.tcl,v 1.128 2007/04/06 11:26:00 drh Exp $}
source common.tcl

if {[llength $argv]>0} {
  set outputdir [lindex $argv 0]
} else {
  set outputdir ""
}

header {Query Language Understood by SQLite}
puts {
<h1 class="pdf_section">SQL As Understood By SQLite</h1>

<p>The SQLite library understands most of the standard SQL
language.  But it does <a href="omitted.html">omit some features</a>
while at the same time
adding a few features of its own.  This document attempts to
describe precisely what parts of the SQL language SQLite does
and does not support.  A list of <a href="lang_keywords.html">keywords</a> is 
also provided.</p>

<p>In all of the syntax diagrams that follow, literal text is shown in
bold blue.  Non-terminal symbols are shown in italic red.  Operators
that are part of the syntactic markup itself are shown in black roman.</p>

<p>This document is just an overview of the SQL syntax implemented
by SQLite.  Many low-level productions are omitted.  For detailed information
on the language that SQLite understands, refer to the source code and
the grammar file "parse.y".</p>

<div class="pdf_ignore">
<p>SQLite implements the follow syntax:</p>
<p><ul>
}

proc slink {label} {
  if {[string match *.html $label]} {
    return $label
  }
  if {[string length $::outputdir]==0} {
    return #$label
  } else { 
    return lang_$label.html
  }
}

foreach {section} [lsort -index 0 -dictionary {
  {{CREATE TABLE} createtable}
  {{CREATE VIRTUAL TABLE} createvtab}
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
  {PRAGMA pragma.html}
  {{ON CONFLICT clause} conflict}
  {{CREATE VIEW} createview}
  {{DROP VIEW} dropview}
  {{CREATE TRIGGER} createtrigger}
  {{DROP TRIGGER} droptrigger}
  {{ATTACH DATABASE} attach}
  {{DETACH DATABASE} detach}
  {REINDEX reindex}
  {{ALTER TABLE} altertable}
  {{ANALYZE} analyze}
}] {
  foreach {s_title s_tag} $section {}
  puts "<li><a href=\"[slink $s_tag]\">$s_title</a></li>"
}
puts {</ul></p>
</div>

<p>Details on the implementation of each command are provided in
the sequel.</p>
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
proc Example {text} {
  puts "<blockquote><pre>$text</pre></blockquote>"
}

proc Section {name label} {
  global outputdir

  if {[string length $outputdir]!=0} {
    if {[llength [info commands puts_standard]]>0} {
      footer $::rcsid
    }

    if {[string length $label]>0} {
      rename puts puts_standard
      proc puts {str} {
        regsub -all {href="#([a-z]+)"} $str {href="lang_\1.html"} str
        puts_standard $::section_file $str
      }
      rename footer footer_standard
      proc footer {id} {
        footer_standard $id
        rename footer ""
        rename puts ""
        rename puts_standard puts
        rename footer_standard footer
      } 
      set ::section_file [open [file join $outputdir lang_$label.html] w]
      header "Query Language Understood by SQLite: $name"
      puts "<h1>SQL As Understood By SQLite</h1>"
      puts "<a href=\"lang.html\">\[Contents\]</a>"
      puts "<h2>$name</h2>"
      return 
    }
  }
  puts "\n<hr />"
  if {$label!=""} {
    puts "<a name=\"$label\"></a>"
  }
  puts "<h1>$name</h1>\n"
}

Section {ALTER TABLE} altertable

Syntax {sql-statement} {
ALTER TABLE [<database-name> .] <table-name> <alteration>
} {alteration} {
RENAME TO <new-table-name>
} {alteration} {
ADD [COLUMN] <column-def>
}

puts {
<p>SQLite's version of the ALTER TABLE command allows the user to 
rename or add a new column to an existing table. It is not possible
to remove a column from a table.
</p>

<p>The RENAME TO syntax is used to rename the table identified by 
<i>[database-name.]table-name</i> to <i>new-table-name</i>. This command 
cannot be used to move a table between attached databases, only to rename 
a table within the same database.</p>

<p>If the table being renamed has triggers or indices, then these remain
attached to the table after it has been renamed. However, if there are
any view definitions, or statements executed by triggers that refer to
the table being renamed, these are not automatically modified to use the new
table name. If this is required, the triggers or view definitions must be
dropped and recreated to use the new table name by hand.
</p>

<p>The ADD [COLUMN] syntax is used to add a new column to an existing table.
The new column is always appended to the end of the list of existing columns.
<i>Column-def</i> may take any of the forms permissable in a CREATE TABLE 
statement, with the following restrictions:
<ul>
<li>The column may not have a PRIMARY KEY or UNIQUE constraint.</li>
<li>The column may not have a default value of CURRENT_TIME, CURRENT_DATE 
    or CURRENT_TIMESTAMP.</li>
<li>If a NOT NULL constraint is specified, then the column must have a
    default value other than NULL.
</ul>

<p>The execution time of the ALTER TABLE command is independent of
the amount of data in the table.  The ALTER TABLE command runs as quickly
on a table with 10 million rows as it does on a table with 1 row.
</p>

<p>After ADD COLUMN has been run on a database, that database will not
be readable by SQLite version 3.1.3 and earlier until the database
is <a href="lang_vacuum.html">VACUUM</a>ed.</p>
}

Section {ANALYZE} analyze

Syntax {sql-statement} {
  ANALYZE
}
Syntax {sql-statement} {
  ANALYZE <database-name>
}
Syntax {sql-statement} {
  ANALYZE [<database-name> .] <table-name>
}

puts {
<p>The ANALYZE command gathers statistics about indices and stores them
in a special tables in the database where the query optimizer can use
them to help make better index choices.
If no arguments are given, all indices in all attached databases are
analyzed.  If a database name is given as the argument, all indices
in that one database are analyzed.  If the argument is a table name,
then only indices associated with that one table are analyzed.</p>

<p>The initial implementation stores all statistics in a single
table named <b>sqlite_stat1</b>.  Future enhancements may create
additional tables with the same name pattern except with the "1"
changed to a different digit.  The <b>sqlite_stat1</b> table cannot
be <a href="#droptable">DROP</a>ped,
but all the content can be <a href="#delete">DELETE</a>d which has the
same effect.</p>
}

Section {ATTACH DATABASE} attach

Syntax {sql-statement} {
ATTACH [DATABASE] <database-filename> AS <database-name>
}

puts {
<p>The ATTACH DATABASE statement adds another database 
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
tables which don't have duplicate names become the default table
of that name.  Any tables of that name attached afterwards require the table 
prefix. If the default table of a given name is detached, then 
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
CREATE [UNIQUE] INDEX [IF NOT EXISTS] [<database-name> .] <index-name> 
ON <table-name> ( <column-name> [, <column-name>]* )
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

<p>The exact text
of each CREATE INDEX statement is stored in the <b>sqlite_master</b>
or <b>sqlite_temp_master</b> table, depending on whether the table
being indexed is temporary.  Every time the database is opened,
all CREATE INDEX statements
are read from the <b>sqlite_master</b> table and used to regenerate
SQLite's internal representation of the index layout.</p>

<p>If the optional IF NOT EXISTS clause is present and another index
with the same name aleady exists, then this command becomes a no-op.</p>

<p>Indexes are removed with the <a href="#dropindex">DROP INDEX</a> 
command.</p>
}


Section {CREATE TABLE} {createtable}

Syntax {sql-command} {
CREATE [TEMP | TEMPORARY] TABLE [IF NOT EXISTS] [<database-name> .] <table-name> (
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
PRIMARY KEY [<sort-order>] [ <conflict-clause> ] [AUTOINCREMENT] |
UNIQUE [ <conflict-clause> ] |
CHECK ( <expr> ) |
DEFAULT <value> |
COLLATE <collation-name>
} {constraint} {
PRIMARY KEY ( <column-list> ) [ <conflict-clause> ] |
UNIQUE ( <column-list> ) [ <conflict-clause> ] |
CHECK ( <expr> )
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
The COLLATE clause specifies what text <a href="datatype3.html#collation">
collating function</a> to use when comparing text entries for the column.  
The built-in BINARY collating function is used by default.
<p>
The DEFAULT constraint specifies a default value to use when doing an INSERT.
The value may be NULL, a string constant or a number. Starting with version
3.1.0, the default value may also be one of the special case-independant
keywords CURRENT_TIME, CURRENT_DATE or CURRENT_TIMESTAMP. If the value is
NULL, a string constant or number, it is literally inserted into the column
whenever an INSERT statement that does not specify a value for the column is
executed. If the value is CURRENT_TIME, CURRENT_DATE or CURRENT_TIMESTAMP, then
the current UTC date and/or time is inserted into the columns. For
CURRENT_TIME, the format is HH:MM:SS. For CURRENT_DATE, YYYY-MM-DD. The format
for CURRENT_TIMESTAMP is "YYYY-MM-DD HH:MM:SS".
</p>

<p>Specifying a PRIMARY KEY normally just creates a UNIQUE index
on the corresponding columns.  However, if primary key is on a single column
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
PRIMARY KEY.  An INTEGER PRIMARY KEY column can also include the
keyword AUTOINCREMENT.  The AUTOINCREMENT keyword modified the way
that B-Tree keys are automatically generated.  Additional detail
on automatic B-Tree key generation is available
<a href="autoinc.html">separately</a>.</p>

<p>According to the SQL standard, PRIMARY KEY should imply NOT NULL.
Unfortunately, due to a long-standing coding oversight, this is not 
the case in SQLite.  SQLite allows NULL values
in a PRIMARY KEY column.  We could change SQLite to conform to the
standard (and we might do so in the future), but by the time the
oversight was discovered, SQLite was in such wide use that we feared
breaking legacy code if we fixed the problem.  So for now we have
chosen to contain allowing NULLs in PRIMARY KEY columns.
Developers should be aware, however, that we may change SQLite to
conform to the SQL standard in future and should design new programs
accordingly.</p>

<p>If the "TEMP" or "TEMPORARY" keyword occurs in between "CREATE"
and "TABLE" then the table that is created is only visible
within that same database connection
and is automatically deleted when
the database connection is closed.  Any indices created on a temporary table
are also temporary.  Temporary tables and indices are stored in a
separate file distinct from the main database file.</p>

<p> If a &lt;database-name&gt; is specified, then the table is created in 
the named database. It is an error to specify both a &lt;database-name&gt;
and the TEMP keyword, unless the &lt;database-name&gt; is "temp". If no
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

<p>CHECK constraints are supported as of version 3.3.0.  Prior
to version 3.3.0, CHECK constraints were parsed but not enforced.</p>

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

<p>If the optional IF NOT EXISTS clause is present and another table
with the same name aleady exists, then this command becomes a no-op.</p>

<p>Tables are removed using the <a href="#droptable">DROP TABLE</a> 
statement.  </p>
}


Section {CREATE TRIGGER} createtrigger

Syntax {sql-statement} {
CREATE [TEMP | TEMPORARY] TRIGGER [IF NOT EXISTS] <trigger-name> [ BEFORE | AFTER ]
<database-event> ON [<database-name> .] <table-name>
<trigger-action>
}

Syntax {sql-statement} {
CREATE [TEMP | TEMPORARY] TRIGGER [IF NOT EXISTS] <trigger-name> INSTEAD OF
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
[ FOR EACH ROW ] [ WHEN <expression> ] 
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
statement.</p>
}


Section {CREATE VIEW} {createview}

Syntax {sql-command} {
CREATE [TEMP | TEMPORARY] VIEW [IF NOT EXISTS] [<database-name>.] <view-name> AS <select-statement>
}

puts {
<p>The CREATE VIEW command assigns a name to a pre-packaged 
<a href="#select">SELECT</a>
statement.  Once the view is created, it can be used in the FROM clause
of another SELECT in place of a table name.
</p>

<p>If the "TEMP" or "TEMPORARY" keyword occurs in between "CREATE"
and "VIEW" then the view that is created is only visible to the
process that opened the database and is automatically deleted when
the database is closed.</p>

<p> If a &lt;database-name&gt; is specified, then the view is created in 
the named database. It is an error to specify both a &lt;database-name&gt;
and the TEMP keyword, unless the &lt;database-name&gt; is "temp". If no
database name is specified, and the TEMP keyword is not present,
the table is created in the main database.</p>

<p>You cannot COPY, DELETE, INSERT or UPDATE a view.  Views are read-only 
in SQLite.  However, in many cases you can use a <a href="#createtrigger">
TRIGGER</a> on the view to accomplish the same thing.  Views are removed 
with the <a href="#dropview">DROP VIEW</a> 
command.</p>
}

Section {CREATE VIRTUAL TABLE} {createvtab}

Syntax {sql-command} {
CREATE VIRTUAL TABLE [<database-name> .] <table-name> USING <module-name> [( <arguments> )]
}

puts {
<p>A virtual table is an interface to an external storage or computation
engine that appears to be a table but does not actually store information
in the database file.</p>

<p>In general, you can do anything with a virtual table that can be done
with an ordinary table, except that you cannot create triggers on a
virtual table.  Some virtual table implementations might impose additional
restrictions.  For example, many virtual tables are read-only.</p>

<p>The &lt;module-name&gt; is the name of an object that implements
the virtual table.  The &lt;module-name&gt; must be registered with
the SQLite database connection using
<a href="capi3ref.html#sqlite3_create_module">sqlite3_create_module</a>
prior to issuing the CREATE VIRTUAL TABLE statement.
The module takes zero or more comma-separated arguments.
The arguments can be just about any text as long as it has balanced
parentheses.  The argument syntax is sufficiently general that the
arguments can be made to appear as column definitions in a traditional
<a href="#createtable">CREATE TABLE</a> statement.  
SQLite passes the module arguments directly
to the module without any interpretation.  It is the responsibility
of the module implementation to parse and interpret its own arguments.</p>

<p>A virtual table is destroyed using the ordinary
<a href="#droptable">DROP TABLE</a> statement.  There is no
DROP VIRTUAL TABLE statement.</p>
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
DROP INDEX [IF EXISTS] [<database-name> .] <index-name>
}

puts {
<p>The DROP INDEX statement removes an index added
with the <a href="#createindex">
CREATE INDEX</a> statement.  The index named is completely removed from
the disk.  The only way to recover the index is to reenter the
appropriate CREATE INDEX command.</p>

<p>The DROP INDEX statement does not reduce the size of the database 
file in the default mode.
Empty space in the database is retained for later INSERTs.  To 
remove free space in the database, use the <a href="#vacuum">VACUUM</a> 
command.  If AUTOVACUUM mode is enabled for a database then space
will be freed automatically by DROP INDEX.</p>
}


Section {DROP TABLE} droptable

Syntax {sql-command} {
DROP TABLE [IF EXISTS] [<database-name>.] <table-name>
}

puts {
<p>The DROP TABLE statement removes a table added with the <a href=
"#createtable">CREATE TABLE</a> statement.  The name specified is the
table name.  It is completely removed from the database schema and the 
disk file.  The table can not be recovered.  All indices associated 
with the table are also deleted.</p>

<p>The DROP TABLE statement does not reduce the size of the database 
file in the default mode.  Empty space in the database is retained for
later INSERTs.  To 
remove free space in the database, use the <a href="#vacuum">VACUUM</a> 
command.  If AUTOVACUUM mode is enabled for a database then space
will be freed automatically by DROP TABLE.</p>

<p>The optional IF EXISTS clause suppresses the error that would normally
result if the table does not exist.</p>
}


Section {DROP TRIGGER} droptrigger
Syntax {sql-statement} {
DROP TRIGGER [IF EXISTS] [<database-name> .] <trigger-name>
}
puts { 
<p>The DROP TRIGGER statement removes a trigger created by the 
<a href="#createtrigger">CREATE TRIGGER</a> statement.  The trigger is 
deleted from the database schema. Note that triggers are automatically 
dropped when the associated table is dropped.</p>
}


Section {DROP VIEW} dropview

Syntax {sql-command} {
DROP VIEW [IF EXISTS] <view-name>
}

puts {
<p>The DROP VIEW statement removes a view created by the <a href=
"#createview">CREATE VIEW</a> statement.  The name specified is the 
view name.  It is removed from the database schema, but no actual data 
in the underlying base tables is modified.</p>
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
<expr> [NOT] <like-op> <expr> [ESCAPE <expr>] |
<unary-op> <expr> |
( <expr> ) |
<column-name> |
<table-name> . <column-name> |
<database-name> . <table-name> . <column-name> |
<literal-value> |
<parameter> |
<function-name> ( <expr-list> | STAR ) |
<expr> ISNULL |
<expr> NOTNULL |
<expr> [NOT] BETWEEN <expr> AND <expr> |
<expr> [NOT] IN ( <value-list> ) |
<expr> [NOT] IN ( <select-statement> ) |
<expr> [NOT] IN [<database-name> .] <table-name> |
[EXISTS] ( <select-statement> ) |
CASE [<expr>] LP WHEN <expr> THEN <expr> RPPLUS [ELSE <expr>] END |
CAST ( <expr> AS <type> ) |
<expr> COLLATE <collation-name>
} {like-op} {
LIKE | GLOB | REGEXP | MATCH
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

<p>Supported unary prefix operators are these:</p>

<blockquote><pre>
<font color="#2c2cf0"><big>-    +    !    ~    NOT</big></font>
</pre></blockquote>

<p>The COLLATE operator can be thought of as a unary postfix
operator.  The COLLATE operator has the highest precedence.
It always binds more tightly than any prefix unary operator or
any binary operator.</p>

<p>The unary operator [Operator +] is a no-op.  It can be applied
to strings, numbers, or blobs and it always gives as its result the
value of the operand.</p>

<p>Note that there are two variations of the equals and not equals
operators.  Equals can be either}
puts "[Operator =] or [Operator ==].
The non-equals operator can be either
[Operator !=] or [Operator {&lt;&gt;}].
The [Operator ||] operator is \"concatenate\" - it joins together
the two strings of its operands.
The operator [Operator %] outputs the remainder of its left 
operand modulo its right operand.</p>

<p>The result of any binary operator is a numeric value, except
for the [Operator ||] concatenation operator which gives a string
result.</p>"

puts {

<a name="literal_value"></a>
<p>
A literal value is an integer number or a floating point number.
Scientific notation is supported.  The "." character is always used
as the decimal point even if the locale setting specifies "," for
this role - the use of "," for the decimal point would result in
syntactic ambiguity.  A string constant is formed by enclosing the
string in single quotes (').  A single quote within the string can
be encoded by putting two single quotes in a row - as in Pascal.
C-style escapes using the backslash character are not supported because
they are not standard SQL.
BLOB literals are string literals containing hexadecimal data and
preceded by a single "x" or "X" character.  For example:</p>

<blockquote><pre>
X'53514697465'
</pre></blockquote>

<p>
A literal value can also be the token "NULL".
</p>

<p>
A parameter specifies a placeholder in the expression for a literal
value that is filled in at runtime using the
<a href="capi3ref.html#sqlite3_bind_int">sqlite3_bind</a> API.
Parameters can take several forms:
</p

<blockquote>
<table class="pdf_functions">
<tr>
<td align="right" valign="top"><b>?</b><i>NNN</i></td><td width="20"></td>
<td>A question mark followed by a number <i>NNN</i> holds a spot for the
NNN-th parameter.  NNN must be between 1 and 999.</td>
</tr>
<tr>
<td align="right" valign="top"><b>?</b></td><td width="20"></td>
<td>A question mark that is not followed by a number holds a spot for
the next unused parameter.</td>
</tr>
<tr>
<td align="right" valign="top"><b>:</b><i>AAAA</i></td><td width="20"></td>
<td>A colon followed by an identifier name holds a spot for a named
parameter with the name AAAA.  Named parameters are also numbered.
The number assigned is the next unused number.  To avoid confusion,
it is best to avoid mixing named and numbered parameters.</td>
</tr>
<tr>
<td align="right" valign="top"><b>@</b><i>AAAA</i></td><td width="20"></td>
<td>An "at" sign works exactly like a colon.</td>
</tr>
<tr>
<td align="right" valign="top"><b>$</b><i>AAAA</i></td><td width="20"></td>
<td>A dollar-sign followed by an identifier name also holds a spot for a named
parameter with the name AAAA.  The identifier name in this case can include
one or more occurances of "::" and a suffix enclosed in "(...)" containing
any text at all.  This syntax is the form of a variable name in the Tcl
programming language.</td>
</tr>
</table>
</blockquote>

<p>Parameters that are not assigned values using
<a href="capi3ref.html#sqlite3_bind_int">sqlite3_bind</a> are treated
as NULL.</p>

<a name="like"></a>
<p>The LIKE operator does a pattern matching comparison. The operand
to the right contains the pattern, the left hand operand contains the
string to match against the pattern. 
}
puts "A percent symbol [Operator %] in the pattern matches any
sequence of zero or more characters in the string.  An underscore
[Operator _] in the pattern matches any single character in the
string.  Any other character matches itself or it's lower/upper case
equivalent (i.e. case-insensitive matching).  (A bug: SQLite only
understands upper/lower case for 7-bit Latin characters.  Hence the
LIKE operator is case sensitive for 8-bit iso8859 characters or UTF-8
characters.  For example, the expression <b>'a'&nbsp;LIKE&nbsp;'A'</b>
is TRUE but <b>'&aelig;'&nbsp;LIKE&nbsp;'&AElig;'</b> is FALSE.).</p>"

puts {
<p>If the optional ESCAPE clause is present, then the expression
following the ESCAPE keyword must evaluate to a string consisting of
a single character. This character may be used in the LIKE pattern
to include literal percent or underscore characters. The escape
character followed by a percent symbol, underscore or itself matches a
literal percent symbol, underscore or escape character in the string,
respectively. The infix LIKE operator is implemented by calling the
user function <a href="#likeFunc"> like(<i>X</i>,<i>Y</i>)</a>.</p>
}

puts {
The LIKE operator is not case sensitive and will match upper case
characters on one side against lower case characters on the other.  
(A bug: SQLite only understands upper/lower case for 7-bit Latin
characters.  Hence the LIKE operator is case sensitive for 8-bit
iso8859 characters or UTF-8 characters.  For example, the expression
<b>'a'&nbsp;LIKE&nbsp;'A'</b> is TRUE but
<b>'&aelig;'&nbsp;LIKE&nbsp;'&AElig;'</b> is FALSE.).</p>

<p>The infix LIKE
operator is implemented by calling the user function <a href="#likeFunc">
like(<i>X</i>,<i>Y</i>)</a>.  If an ESCAPE clause is present, it adds
a third parameter to the function call. If the functionality of LIKE can be
overridden by defining an alternative implementation of the
like() SQL function.</p>
</p>

<a name="glob"></a>
<p>The GLOB operator is similar to LIKE but uses the Unix
file globbing syntax for its wildcards.  Also, GLOB is case
sensitive, unlike LIKE.  Both GLOB and LIKE may be preceded by
the NOT keyword to invert the sense of the test.  The infix GLOB 
operator is implemented by calling the user function <a href="#globFunc">
glob(<i>X</i>,<i>Y</i>)</a> and can be modified by overriding
that function.</p>

<a name="regexp"></a>
<p>The REGEXP operator is a special syntax for the regexp()
user function.  No regexp() user function is defined by default
and so use of the REGEXP operator will normally result in an
error message.  If a user-defined function named "regexp"
is added at run-time, that function will be called in order
to implement the REGEXP operator.</p>

<a name="match"></a>
<p>The MATCH operator is a special syntax for the match()
user function.  The default match() function implementation
raises and exception and is not really useful for anything.
But extensions can override the match() function with more
helpful logic.</p>

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
right-hand operand of the IN operator, as a scalar quantity, or
as the operand of an EXISTS operator.
As a scalar quantity or the operand of an IN operator,
the SELECT should have only a single column in its
result.  Compound SELECTs (connected with keywords like UNION or
EXCEPT) are allowed.
With the EXISTS operator, the columns in the result set of the SELECT are
ignored and the expression returns TRUE if one or more rows exist
and FALSE if the result set is empty.
If no terms in the SELECT expression refer to value in the containing
query, then the expression is evaluated once prior to any other
processing and the result is reused as necessary.  If the SELECT expression
does contain variables from the outer query, then the SELECT is reevaluated
every time it is needed.</p>

<p>When a SELECT is the right operand of the IN operator, the IN
operator returns TRUE if the result of the left operand is any of
the values generated by the select.  The IN operator may be preceded
by the NOT keyword to invert the sense of the test.</p>

<p>When a SELECT appears within an expression but is not the right
operand of an IN operator, then the first row of the result of the
SELECT becomes the value used in the expression.  If the SELECT yields
more than one result row, all rows after the first are ignored.  If
the SELECT yields no rows, then the value of the SELECT is NULL.</p>

<p>A CAST expression changes the datatype of the <expr> into the
type specified by &lt;type&gt;. 
&lt;type&gt; can be any non-empty type name that is valid
for the type in a column definition of a CREATE TABLE statement.</p>

<p>Both simple and aggregate functions are supported.  A simple
function can be used in any expression.  Simple functions return
a result immediately based on their inputs.  Aggregate functions
may only be used in a SELECT statement.  Aggregate functions compute
their result across all rows of the result set.</p>

<a name="corefunctions"></a>
<b>Core Functions</b>

<p>The core functions shown below are available by default.  Additional
functions may be written in C and added to the database engine using
the <a href="capi3ref.html#cfunc">sqlite3_create_function()</a>
API.</p>

<table border=0 cellpadding=10 class="pdf_functions">
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
<td valign="top" align="right">
<a name="globFunc"></a>
glob(<i>X</i>,<i>Y</i>)</td>
<td valign="top">This function is used to implement the
"<b>X GLOB Y</b>" syntax of SQLite.  The
<a href="capi3ref.html#sqlite3_create_function">sqlite3_create_function()</a> 
interface can
be used to override this function and thereby change the operation
of the <a href="#globFunc">GLOB</a> operator.</td>
</tr>

<tr>
<td valign="top" align="right">ifnull(<i>X</i>,<i>Y</i>)</td>
<td valign="top">Return a copy of the first non-NULL argument.  If
both arguments are NULL then NULL is returned. This behaves the same as 
<b>coalesce()</b> above.</td>
</tr>

<tr>
<td valign="top" align="right">
<a name="hexFunc">
hex(<i>X</i>)</td>
<td valign="top">The argument is interpreted as a BLOB.  The result
is a hexadecimal rendering of the content of that blob.</td>
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
<td valign="top" align="right">
<a name="likeFunc"></a>
like(<i>X</i>,<i>Y</i>)<br>
like(<i>X</i>,<i>Y</i>,<i>Z</i>)</td>
<td valign="top">
This function is used to implement the "<b>X LIKE Y [ESCAPE Z]</b>"
syntax of SQL. If the optional ESCAPE clause is present, then the
user-function is invoked with three arguments. Otherwise, it is
invoked with two arguments only. The 
<a href="capi3ref.html#sqlite3_create_function">
sqlite_create_function()</a> interface can be used to override this
function and thereby change the operation of the <a
href= "#like">LIKE</a> operator. When doing this, it may be important
to override both the two and three argument versions of the like() 
function. Otherwise, different code may be called to implement the
LIKE operator depending on whether or not an ESCAPE clause was 
specified.</td>
</tr>

<tr>
<td valign="top" align="right">load_extension(<i>X</i>)<br>
load_extension(<i>X</i>,<i>Y</i>)</td>
<td valign="top">Load SQLite extensions out of the shared library
file named <i>X</i> using the entry point <i>Y</i>.  The result
is a NULL.  If <i>Y</i> is omitted then the default entry point
of <b>sqlite3_extension_init</b> is used.  This function raises
an exception if the extension fails to load or initialize correctly.
</tr>

<tr>
<td valign="top" align="right">lower(<i>X</i>)</td>
<td valign="top">Return a copy of string <i>X</i> will all characters
converted to lower case.  The C library <b>tolower()</b> routine is used
for the conversion, which means that this function might not
work correctly on UTF-8 characters.</td>
</tr>

<tr>
<td valign="top" align="right">
<a name="ltrimFunc">
ltrim(<i>X</i>)<br>ltrim(<i>X</i>,<i>Y</i>)</td>
<td valign="top">Return a string formed by removing any and all
characters that appear in <i>Y</i> from the left side of <i>X</i>.
If the <i>Y</i> argument is omitted, spaces are removed.</td>
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
<td valign="top">Return a pseudo-random integer
between -9223372036854775808 and +9223372036854775807.</td>
</tr>

<tr>
<td valign="top" align="right">
<a name="replaceFunc">
replace(<i>X</i>,<i>Y</i>,<i>Z</i>)</td>
<td valign="top">Return a string formed by substituting string <i>Z</i> for
every occurrance of string <i>Y</i> in string <i>X</i>.  The BINARY
collating sequence is used for comparisons.</td>
</tr>

<tr>
<td valign="top" align="right">
<a name="randomblobFunc">
randomblob(<i>N</i>)</td>
<td valign="top">Return a <i>N</i>-byte blob containing pseudo-random bytes.
<i>N</i> should be a postive integer.</td>
</tr>

<tr>
<td valign="top" align="right">round(<i>X</i>)<br>round(<i>X</i>,<i>Y</i>)</td>
<td valign="top">Round off the number <i>X</i> to <i>Y</i> digits to the
right of the decimal point.  If the <i>Y</i> argument is omitted, 0 is 
assumed.</td>
</tr>

<tr>
<td valign="top" align="right">
<a name="rtrimFunc">
rtrim(<i>X</i>)<br>rtrim(<i>X</i>,<i>Y</i>)</td>
<td valign="top">Return a string formed by removing any and all
characters that appear in <i>Y</i> from the right side of <i>X</i>.
If the <i>Y</i> argument is omitted, spaces are removed.</td>
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
<td valign="top" align="right">
<a name="trimFunc">
trim(<i>X</i>)<br>trim(<i>X</i>,<i>Y</i>)</td>
<td valign="top">Return a string formed by removing any and all
characters that appear in <i>Y</i> from both ends of <i>X</i>.
If the <i>Y</i> argument is omitted, spaces are removed.</td>
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

<b>Date And Time Functions</b>

<p>Date and time functions are documented in the 
<a href="http://www.sqlite.org/cvstrac/wiki?p=DateAndTimeFunctions">
SQLite Wiki</a>.</p>

<a name="aggregatefunctions"></a>
<b>Aggregate Functions</b>

<p>
The aggregate functions shown below are available by default.  Additional
aggregate functions written in C may be added using the 
<a href="capi3ref.html#sqlite3_create_function">sqlite3_create_function()</a>
API.</p>

<p>
In any aggregate function that takes a single argument, that argument
can be preceeded by the keyword DISTINCT.  In such cases, duplicate
elements are filtered before being passed into the aggregate function.
For example, the function "count(distinct X)" will return the number
of distinct values of column X instead of the total number of non-null
values in column X.
</p>

<table border=0 cellpadding=10 class="pdf_functions">
<tr>
<td valign="top" align="right" width=120>avg(<i>X</i>)</td>
<td valign="top">Return the average value of all non-NULL <i>X</i> within a
group.  String and BLOB values that do not look like numbers are
interpreted as 0.
The result of avg() is always a floating point value even if all
inputs are integers. </p></td>
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
<td valign="top" align="right">sum(<i>X</i>)<br>total(<i>X</i>)</td>
<td valign="top">Return the numeric sum of all non-NULL values in the group.
   If there are no non-NULL input rows then sum() returns
   NULL but total() returns 0.0.
   NULL is not normally a helpful result for the sum of no rows
   but the SQL standard requires it and most other
   SQL database engines implement sum() that way so SQLite does it in the
   same way in order to be compatible.   The non-standard total() function
   is provided as a convenient way to work around this design problem
   in the SQL language.</p>

   <p>The result of total() is always a floating point value.
   The result of sum() is an integer value if all non-NULL inputs are integers.
   If any input to sum() is neither an integer or a NULL
   then sum() returns a floating point value
   which might be an approximation to the true sum.</p>

   <p>Sum() will throw an "integer overflow" exception if all inputs
   are integers or NULL
   and an integer overflow occurs at any point during the computation.
   Total() never throws an exception.</p>
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
or compound.</p>

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
the CREATE TABLE command.  For the INSERT and
UPDATE commands, the keywords "ON CONFLICT" are replaced by "OR", to make
the syntax seem more natural.  For example, instead of
"INSERT ON CONFLICT IGNORE" we have "INSERT OR IGNORE".
The keywords change but the meaning of the clause is the same
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
value, then the ABORT algorithm is used.  If a CHECK constraint violation
occurs then the IGNORE algorithm is used.</p>

<p>When this conflict resolution strategy deletes rows in order to
satisfy a constraint, it does not invoke delete triggers on those
rows.  This behavior might change in a future release.</p>
</dl>

<p>The algorithm specified in the OR clause of a INSERT or UPDATE
overrides any algorithm specified in a CREATE TABLE.
If no algorithm is specified anywhere, the ABORT algorithm is used.</p>
}

Section REINDEX reindex

Syntax {sql-statement} {
  REINDEX <collation name>
}
Syntax {sql-statement} {
  REINDEX [<database-name> .] <table/index-name>
}

puts {
<p>The REINDEX command is used to delete and recreate indices from scratch.
This is useful when the definition of a collation sequence has changed.
</p>

<p>In the first form, all indices in all attached databases that use the
named collation sequence are recreated. In the second form, if 
<i>[database-name.]table/index-name</i> identifies a table, then all indices
associated with the table are rebuilt. If an index is identified, then only
this specific index is deleted and recreated.
</p>

<p>If no <i>database-name</i> is specified and there exists both a table or
index and a collation sequence of the specified name, then indices associated
with the collation sequence only are reconstructed. This ambiguity may be
dispelled by always specifying a <i>database-name</i> when reindexing a
specific table or index.
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
Note that if the OFFSET keyword is used in the LIMIT clause, then the
limit is the first number and the offset is the second number.  If a
comma is used instead of the OFFSET keyword, then the offset is the
first number and the limit is the second number.  This seeming
contradition is intentional - it maximizes compatibility with legacy
SQL database systems.
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
removing the results of the right SELECT.  When three or more SELECTs
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
up the database file structure.</p>

<p>VACUUM only works on the main database.
It is not possible to VACUUM an attached database file.</p>

<p>The VACUUM command will fail if there is an active transaction.
The VACUUM command is a no-op for in-memory databases.</p>

<p>As of SQLite version 3.1, an alternative to using the VACUUM command
is auto-vacuum mode, enabled using the 
<a href="pragma.html#pragma_auto_vacuum">auto_vacuum pragma</a>.
When auto-vacuum is enabled for a database, large deletes cause
the size of the database file to shrink.  However, auto-vacuum
also causes excess fragmentation of the database file.  And auto-vacuum
does not compact partially filled pages of the database as VACUUM
does.
</p>
}

# A list of keywords.  A asterisk occurs after the keyword if it is on
# the fallback list.
#
set keyword_list [lsort {
   ABORT*
   ADD
   AFTER*
   ALL
   ALTER
   ANALYZE*
   AND
   AS
   ASC*
   ATTACH*
   AUTOINCREMENT
   BEFORE*
   BEGIN*
   BETWEEN
   BY
   CASCADE*
   CASE
   CAST*
   CHECK
   COLLATE
   COMMIT
   CONFLICT*
   CONSTRAINT
   CREATE
   CROSS
   CURRENT_DATE*
   CURRENT_TIME*
   CURRENT_TIMESTAMP*
   DATABASE*
   DEFAULT
   DEFERRED*
   DEFERRABLE
   DELETE
   DESC*
   DETACH*
   DISTINCT
   DROP
   END*
   EACH*
   ELSE
   ESCAPE
   EXCEPT
   EXCLUSIVE*
   EXPLAIN*
   FAIL*
   FOR*
   FOREIGN
   FROM
   FULL
   GLOB*
   GROUP
   HAVING
   IF*
   IGNORE*
   IMMEDIATE*
   IN
   INDEX
   INITIALLY*
   INNER
   INSERT
   INSTEAD*
   INTERSECT
   INTO
   IS
   ISNULL
   JOIN
   KEY*
   LEFT
   LIKE*
   LIMIT
   MATCH*
   NATURAL
   NOT
   NOTNULL
   NULL
   OF*
   OFFSET*
   ON
   OR
   ORDER
   OUTER
   PLAN*
   PRAGMA*
   PRIMARY
   QUERY*
   RAISE*
   REFERENCES
   REINDEX*
   RENAME*
   REPLACE*
   RESTRICT*
   RIGHT
   ROLLBACK
   ROW*
   SELECT
   SET
   TABLE
   TEMP*
   TEMPORARY*
   THEN
   TO
   TRANSACTION
   TRIGGER*
   UNION
   UNIQUE
   UPDATE
   USING
   VACUUM*
   VALUES
   VIEW*
   VIRTUAL*
   WHEN
   WHERE
}]



puts {<DIV class="pdf_section">}
Section {SQLite Keywords} keywords 
puts {</DIV>}

puts {
<p>The SQL standard specifies a huge number of keywords which may not
be used as the names of tables, indices, columns, databases, user-defined
functions, collations, virtual table modules, or any other named object.
The list of keywords is so long that few people can remember them all.
For most SQL code, your safest bet is to never use any English language
word as the name of a user-defined object.</p>

<p>If you want to use a keyword as a name, you need to quote it.  There
are three ways of quoting keywords in SQLite:</p>

<p>
<blockquote>
<table class="pdf_functions">
<tr>	<td valign="top"><b>'keyword'</b></td><td width="20"></td>
	<td>A keyword in single quotes is interpreted as a literal string
        if it occurs in a context where a string literal is allowed, otherwise
	it is understood as an identifier.</td></tr>
<tr>	<td valign="top"><b>"keyword"</b></td><td></td>
	<td>A keyword in double-quotes is interpreted as an identifier if
        it matches a known identifier.  Otherwise it is interpreted as a
        string literal.</td></tr>
<tr>	<td valign="top"><b>[keyword]</b></td><td></td>
	<td>A keyword enclosed in square brackets is always understood as
        an identifier.  This is not standard SQL.  This quoting mechanism
        is used by MS Access and SQL Server and is included in SQLite for
        compatibility.</td></tr>
</table>
</blockquote>
</p>

<p>Quoted keywords are unaesthetic.
To help you avoid them, SQLite allows many keywords to be used unquoted
as the names of databases, tables, indices, triggers, views, columns,
user-defined functions, collations, attached databases, and virtual
function modules.
In the list of keywords that follows, those that can be used as identifiers
are shown in an italic font.  Keywords that must be quoted in order to be
used as identifiers are shown in bold.</p>

<p>
SQLite adds new keywords from time to time when it take on new features.
So to prevent your code from being broken by future enhancements, you should
normally quote any indentifier that is an English language word, even if
you do not have to.
</p>

<p>
The following are the keywords currently recognized by SQLite:
</p>

<blockquote>
<table width="100%" class="pdf_keywords">
<tr>
<td align="left" valign="top" width="20%">
}

set n [llength $keyword_list]
set nCol 5
set nRow [expr {($n+$nCol-1)/$nCol}]
set i 0
foreach word $keyword_list {
  if {[string index $word end]=="*"} {
    set word [string range $word 0 end-1]
    set font i
  } else {
    set font b
  }
  if {$i==$nRow} {
    puts "</td><td valign=\"top\" align=\"left\" width=\"20%\">"
    set i 1
  } else {
    incr i
  }
  puts "<$font>$word</$font><br>"
}

puts {
</td></tr></table></blockquote>

<h2>Special names</h2>

<p>The following are not keywords in SQLite, but are used as names of 
system objects.  They can be used as an identifier for a different 
type of object.</p>

<blockquote class="pdf_keywords"><b>
  _ROWID_<br>
  MAIN<br>
  OID<br>
  ROWID<br>
  SQLITE_MASTER<br>
  SQLITE_SEQUENCE<br>
  SQLITE_TEMP_MASTER<br>
  TEMP<br>
</b></blockquote>
}

puts {<DIV class="pdf_ignore">}
footer $rcsid
if {[string length $outputdir]} {
  footer $rcsid
}
puts {</DIV>}

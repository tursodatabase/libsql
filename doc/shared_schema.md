
Shared-Schema Mode Notes
========================

The [reuse-schema](/timeline?r=reuse-schema) branch contains changes 
to allow SQLite connections to share schemas
between database connections within the same process in order to save memory.
Schemas may be shared between multiple databases attached to the same or
distinct connection handles.

Compile with -DSQLITE\_ENABLE\_SHARED\_SCHEMA in order to enable the
shared-schema enhancement.  Enabling the shared-schema enhancement causes
approximately a 0.1% increase in CPU cycles consumed and about a 3000-byte
increase in the size of the library, even if shared-schema is never used.

Assuming the compile-time requirements are satisfied, the shared-schema
feature is engaged by opening the database connection using the
sqlite3&#95;open&#95;v2() API with the SQLITE&#95;OPEN&#95;SHARED&#95;SCHEMA
flag specified.  The main database and any attached databases will then share
an in-memory Schema object with any other database opened within the process
for which: 

  * the contents of the sqlite&#95;master table, including all object names,
    SQL statements and root pages are identical, and
  * have the same values for the schema-cookie.

Temp databases (those populated with "CREATE TEMP TABLE" and similar
statements) never share schemas.

Connections opened with the SQLITE&#95;OPEN&#95;SHARED&#95;SCHEMA flag
specified may not modify any database schema except that belonging to the
temp database in anyway. This includes creating or dropping database 
objects, vacuuming the database, or running ANALYZE when the
sqlite&#95;stat\[14\] tables do not exist.

For SQLITE&#95;OPEN&#95;SHARED&#95;SCHEMA connections, the
SQLITE&#95;DBSTATUS&#95;SCHEMA&#95;USED sqlite3&#95;db&#95;status() verb
distributes the memory used for a shared schema object evenly between all
database connections that share it.

## The ".shared-schema" Command

The shell tool on this branch contains a special dot-command to help with
managing databases. The ".shared-schema" dot-command can be used to test
whether or not two databases are similar enough to share in-memory schemas,
and to fix minor problems that prevent them from doing so. To test if
two or more database are compatible, one database is opened directly using 
the shell tool and the following command issued:

        .shared-schema check <database-1> [<database-2>]...

where &lt;database-1&gt; etc. are replaced with the names of database files
on disk. For each database specified on the command line, a single line of
output is produced. If the database can share an in-memory schema with the
main database opened by the shell tool, the output is of the form:

        <database> is compatible

Otherwise, if the database cannot share a schema with the main db, the output
is of the form:

        <database> is NOT compatible (<reason>)

where &lt;reason&gt; indicates the cause of the incompatibility. &lt;reason&gt;
is always one of the following.

<ul>
  <li> <b>objects</b> - the databases contain a different set schema objects
  (tables, indexes, views and triggers).

  <li> <b>SQL</b> - the databases contain the same set of objects, but the SQL
  statements used to create them were not the same.

  <li> <b>root pages</b> - the databases contain the same set of objects created
  by the same SQL statements, but the root pages are not the same.

  <li> <b>order of sqlite&#95;master rows</b> - the databases contain the same
  set of objects created by the same SQL statements with the same root pages,
  but the order of the rows in the sqlite&#95;master tables are different.

  <li> <b>schema cookie</b> - the database schemas are compatible, but the 
  schema cookie values ("PRAGMA schema&#95;version") are different.
</ul>

The final three problems in the list above can be fixed using the
.shared-schema command. To modify such a database so that it can share a 
schema with the main database, the following shell command is used:

        .shared-schema fix <database-1> [<database-2>]...

If a database can be modified so that it may share a schema with the main
database opened by the shell tool, output is as follows:

        Fixing <database>... <database> is compatible

If a database does not require modification, or cannot be modified such that
it can share a schema with the main database, the output of "fix" is identical
to that of the "check" command.

## Implementation Notes

A single Schema object is never used by more than one database simultaneously,
regardless of whether or not those databases are attached to the same or
different database handles. Instead, a pool of schema objects is maintained 
for each unique sqlite&#95;master-contents/schema-cookie combination
opened within the process. Each time database schemas are required by a
connection, for example as part of an sqlite3&#95;prepare\*(),
sqlite3&#95;blob&#95;open() or sqlite3&#95;blob&#95;open() call, it obtains
the minimum number of schemas required from the various schema-pools, returning
them at the end of the call. This means that a single schema-pool only ever
contains more than one copy of the schema if:

  * Two threads require schemas from the same pool at the same time, or
  * A single sqlite3&#95;prepare\*() call requires schemas for two or more
    attached databases that use the same schema-pool.

The size of a schema-pool never shrinks. Each schema pool always maintains 
a number of schema objects equal to the highwater mark of schema objects
simultaneously required by clients.

This approach is preferred to allowing multiple databases to use the same
Schema object simultaneously for three reasons:

  * The Schema object is not completely read-only. For example, the 
    Index.zIdxAff string is allocated lazily.
  * Throughout the statement compiler, SQLite uses variables like 
    Table.pSchema and Index.pSchema with the sqlite3SchemaToIndex() routine
    in order to determine which attached database a Table or Index object
    resides in. This mechanism does not work if the same Schema may be
    used by two or more attached databases.
  * It may be easier to modify this approach in order to allow
    SQLITE&#95;OPEN&#95;SHARED&#95;SCHEMA connections to modify database
    schemas, should that be required.

SQLITE&#95;OPEN&#95;SHARED&#95;SCHEMA connections do not store their
virtual-table handles in the Table.pVTable list of each table. This would not
work, as (a) there is no guarantee that a connection will be assigned the same
Schema object each time it requests one from a schema-pool and (b) a single
Schema (and therefore Table) object may correspond to tables in two or more
databases attached to a single connection. Instead, all virtual-table handles
associated with a single database are stored in a linked-list headed at
Db.pVTable.

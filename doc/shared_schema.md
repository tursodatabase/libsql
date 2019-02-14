
Shared-Schema Mode Notes
========================

This branch contains a patch to allow SQLite connections to share schemas
between database connections within the same process in order to save memory.
Schemas may be shared between multiple databases attached to the same or
distinct connection handles.

To activate shared-schemas, a database connection must be opened using the
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

If the schema of a database attached to an
SQLITE&#95;OPEN&#95;SHARED&#95;SCHEMA database handle is corrupt, or if
corruption is encountered while parsing the database schema, then the
database is treated as empty. This usually means that corruption results in
a "no such table: xxx" error instead of a more specific error message.

For SQLITE&#95;OPEN&#95;SHARED&#95;SCHEMA connections, the
SQLITE&#95;DBSTATUS&#95;SCHEMA&#95;USED sqlite3&#95;db&#95;used() distributes
the memory used for a shared schema object evenly between all database
connections that share it.

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





This file contains notes regarding the implementation of the OTA extension.
User documentation is in sqlite3ota.h.

SQLite Hacks
------------


1) PRAGMA pager_ota_mode=1:

  This pragma sets a flag on the pager associated with the main database only.
  In a zipvfs system, this pragma is intercepted by zipvfs and the flag is set
  on the lower level pager only.

  The flag can only be set when there is no open transaction and the pager does
  not already have an open WAL file. Attempting to do so is an error.

  Once the flag has been set, it is not possible to open a regular WAL file.
  If, when the next read-transaction is opened, a *-wal file is found or the
  database header flags indicate that it is a wal-mode database,
  SQLITE_CANTOPEN is returned.

  Otherwise, if no WAL file or flags are found, the pager opens the *-oal file
  and uses it as a write-ahead-log with the *-shm data stored in heap-memory.

  The 8-bytes of "salt" at the start of an *-oal file is a copy of the 8 bytes
  starting at offset 24 of the database file header (the change counter and the
  number of pages in the file). If the *-oal file already exists when it is
  opened, SQLite checks that the salt still matches the database header fields.
  If not, it concludes that the database file has been written by a
  rollback-mode client since the *-oal wa created and an SQLITE_BUSY_SNAPSHOT
  error is returned. No read-transaction can be opened in this case.

  A pager with the pager_ota_mode flag set never runs a checkpoint.

  Other clients see a rollback-mode database on which the pager_ota_mode client
  is holding a SHARED lock. There are no locks to arbitrate between multiple
  pager_ota_mode connections. If two or more such connections attempt to write
  simultaneously, the results are undefined.

2) PRAGMA pager_ota_mode=2:

  The pager_ota_mode pragma may also be set to 2 if the main database is open 
  in WAL mode. This prevents SQLite from checkpointing the wal file as part
  of sqlite3_close().

  The effects of setting pager_ota_mode=2 if the db is not in WAL mode are
  undefined.

3) sqlite3_ckpt_open/step/close()

  API for performing (and resuming) incremental checkpoints.


The OTA extension
-----------------

The OTA extension requires that the OTA update be packaged as an SQLite
database. The tables it expects to find are described in sqlite3ota.h.
Essentially, for each table xyz in the target database that the user wishes
to write to, a corresponding data_xyz table is created in the OTA database
and populated with one row for each row to update, insert or delete from 
the target table.

The OTA extension opens the target and OTA update databases using a single
database handle (the target database is "main", and the OTA update database is
attached as "ota"). It executes both the "pager_ota_mode" and "ota_mode"
pragmas described above. For each data_xyz table in then:

  * CREATEs an ota_xyz table in the OTA update database.

  * Loops through the data_xyz table, running the INSERT, UPDATE or DELETE
    command on the corresponding target database table. Only the main b-tree 
    is updated by these statements. Modified pages are appended to the *-oal
    file.

    Temporary triggers installed on the target database catch the old.* 
    values associated with any UPDATEd or DELETEd rows and store them in
    the ota_xyz table (in the OTA update database).

  * For each index on the data_xyz table in the target database:

    Loop through a union of the data_xyz and ota_xyz tables in the order
    specified by the data_xyz index. In other words, if the index is on
    columns (a, b), read rows from the OTA update database using:

      SELECT * FROM data_xyz UNION ALL ota_xyz ORDER BY a, b;

    For each row visited, use an sqlite3_index_writer() VM to update the index 
    in the target database.

  * DROPs the ota_xyz table.

At any point in the above, the process may be suspended by the user. In this
case the "ota_state" table is created in the OTA database, containing a single
row indicating the current table/index being processed and the number of updates
already performed on it, and the transaction on the target database is committed
to the *-oal file. The next OTA client will use the contents of the ota_state
table to continue the update from where this one left off.

Alternatively, if the OTA update is completely applied, the transaction is
committed to the *-oal file and the database connection closed. sqlite3ota.c
then uses a rename() call to move the *-oal file to the corresponding *-wal
path. At that point it is finished - it does not take responsibility for
checkpointing the *-wal file.


Problems
--------

The rename() call might not be portable. And in theory it is unsafe if some
other client starts writing the db file.

When state is saved, the commit to the *-oal file and the commit to the OTA
update database are not atomic. So if the power fails at the wrong moment they
might get out of sync. As the main database will be committed before the OTA
update database this will likely either just pass unnoticed, or result in
SQLITE_CONSTRAINT errors (due to UNIQUE constraint violations).

If some client does modify the target database mid OTA update, or some other
error occurs, the OTA extension will keep throwing errors. It's not really
clear how to get out of this state. The system could just by delete the OTA
update database and *-oal file and have the device download the update again
and start over.

At present, for an UPDATE, both the new.* and old.* records are collected in
the ota_xyz table. And for both UPDATEs and DELETEs all fields are collected.
This means we're probably writing a lot more data to disk when saving the
state of an ongoing update to the OTA update database than is strictly
necessary.






Wal2 Mode Notes
===============

## Activating/Deactivating Wal2 Mode

"Wal2" mode is very similar to "wal" mode. To change a database to wal2 mode,
use the command:

>
     PRAGMA journal_mode = wal2;

It is not possible to change a database directly from "wal" mode to "wal2"
mode. Instead, it must first be changed to rollback mode. So, to change a wal
mode database to wal2 mode, the following two commands may be used:

>
     PRAGMA journal_mode = delete;
     PRAGMA journal_mode = wal2;

A database in wal2 mode may only be accessed by versions of SQLite compiled
from this branch. Attempting to use any other version of SQLite results in an
SQLITE&#95;NOTADB error. A wal2 mode database may be changed back to rollback mode
(making it accessible by all versions of SQLite) using:

>
     PRAGMA journal_mode = delete;

## The Advantage of Wal2 Mode

In legacy wal mode, when a writer writes data to the database, it doesn't
modify the database file directly. Instead, it appends new data to the
"&lt;database>-wal" file. Readers read data from both the original database
file and the "&lt;database>-wal" file. At some point, data is copied from the
"&lt;database>-wal" file into the database file, after which the wal file can
be deleted or overwritten. Copying data from the wal file into the database
file is called a "checkpoint", and may be done explictly (either by "PRAGMA
wal&#95;checkpoint" or sqlite3&#95;wal&#95;checkpoint&#95;v2()), or
automatically (by configuring "PRAGMA wal&#95;autocheckpoint" - this is the
default).

Checkpointers do not block writers, and writers do not block checkpointers.
However, if a writer writes to the database while a checkpoint is ongoing,
then the new data is appended to the end of the wal file. This means that,
even following the checkpoint, the wal file cannot be overwritten or deleted,
and so all subsequent transactions must also be appended to the wal file. The
work of the checkpointer is not wasted - SQLite remembers which parts of the
wal file have already been copied into the db file so that the next checkpoint
does not have to do so again - but it does mean that the wal file may grow
indefinitely if the checkpointer never gets a chance to finish without a
writer appending to the wal file. There are also circumstances in which
long-running readers may prevent a checkpointer from checkpointing the entire
wal file - also causing the wal file to grow indefinitely in a busy system.

Wal2 mode does not have this problem. In wal2 mode, wal files do not grow
indefinitely even if the checkpointer never has a chance to finish
uninterrupted.

In wal2 mode, the system uses two wal files instead of one. The files are named
"&lt;database>-wal" and "&lt;database>-wal2", where "&lt;database>" is of
course the name of the database file. When data is written to the database, the
writer begins by appending the new data to the first wal file. Once the first
wal file has grown large enough, writers switch to appending data to the second
wal file. At this point the first wal file can be checkpointed (after which it
can be overwritten). Then, once the second wal file has grown large enough and
the first wal file has been checkpointed, writers switch back to the first wal
file. And so on.

## Application Programming

From the point of view of the user, the main differences between wal and 
wal2 mode are to do with checkpointing:

  * In wal mode, a checkpoint may be attempted at any time. In wal2 
    mode, the checkpointer has to wait until writers have switched 
    to the "other" wal file before a checkpoint can take place.

  * In wal mode, the wal-hook (callback registered using
    sqlite3&#95;wal&#95;hook()) is invoked after a transaction is committed
    with the total number of pages in the wal file as an argument. In wal2
    mode, the argument is either the total number of uncheckpointed pages in
    both wal files, or - if the "other" wal file is empty or already
    checkpointed - 0.

Clients are recommended to use the same strategies for checkpointing wal2 mode
databases as for wal databases - by registering a wal-hook using
sqlite3&#95;wal&#95;hook() and attempting a checkpoint when the parameter
exceeds a certain threshold.

However, it should be noted that although the wal-hook is invoked after each
transaction is committed to disk and database locks released, it is still
invoked from within the sqlite3&#95;step() call used to execute the "COMMIT"
command. In BEGIN CONCURRENT systems, where the "COMMIT" is often protected by
an application mutex, this may reduce concurrency. In such systems, instead of
executing a checkpoint from within the wal-hook, a thread might defer this
action until after the application mutex has been released.



#
# Run this script to generated a lockingv3.html output file
#
set rcsid {$Id: }
source common.tcl
header {File Locking And Concurrency In SQLite Version 3}

proc HEADING {level title} {
  global pnum
  incr pnum($level)
  foreach i [array names pnum] {
    if {$i>$level} {set pnum($i) 0}
  }
  set h [expr {$level+1}]
  if {$h>6} {set h 6}
  set n $pnum(1).$pnum(2)
  for {set i 3} {$i<=$level} {incr i} {
    append n .$pnum($i)
  }
  puts "<h$h>$n $title</h$h>"
}
set pnum(1) 0
set pnum(2) 0
set pnum(3) 0
set pnum(4) 0
set pnum(5) 0
set pnum(6) 0
set pnum(7) 0
set pnum(8) 0

HEADING 1 {File Locking And Concurrency In SQLite Version 3}

puts {
<p>Version 3 of SQLite introduces a more complex locking and journaling 
mechanism designed to improve concurrency and reduce the writer starvation 
problem.  The new mechanism also allows atomic commits of transactions
involving multiple database files.
This document describes the new locking mechanism.
The intended audience is programmers who want to understand and/or modify
the pager code and reviewers working to verify the design
of SQLite version 3.
</p>
}

HEADING 1 {Overview}

puts {
<p>
Locking and concurrency control are handled by the the 
<a href="http://www.sqlite.org/cvstrac/getfile/sqlite/src/pager.c">
pager module</a>.
The pager module is responsible for making SQLite "ACID" (Atomic,
Consistent, Isolated, and Durable).  The pager module makes sure changes
happen all at once, that either all changes occur or none of them do,
that two or more processes do not try to access the database
in incompatible ways at the same time, and that once changes have been
written they persist until explicitly deleted.  The pager also provides
an memory cache of some of the contents of the disk file.</p>

<p>The pager is unconcerned
with the details of B-Trees, text encodings, indices, and so forth.
From the point of view of the pager the database consists of
a single file of uniform-sized blocks.  Each block is called a
"page" and is usually 1024 bytes in size.   The pages are numbered
beginning with 1.  So the first 1024 bytes of the database are called
"page 1" and the second 1024 bytes are call "page 2" and so forth. All 
other encoding details are handled by higher layers of the library.  
The pager communicates with the operating system using one of several
modules 
(Examples:
<a href="http://www.sqlite.org/cvstrac/getfile/sqlite/src/os_unix.c">
os_unix.c</a>,
<a href="http://www.sqlite.org/cvstrac/getfile/sqlite/src/os_win.c">
os_win.c</a>)
that provides a uniform abstraction for operating system services.
</p>

<p>The pager module effectively controls access for separate threads, or
separate processes, or both.  Throughout this document whenever the
word "process" is written you may substitute the word "thread" without
changing the truth of the statement.</p>
}

HEADING 1 {Locking}

puts {
<p>
From the point of view of a single process, a database file
can be in one of five locking states:
</p>

<p>
<table cellpadding="20">
<tr><td valign="top">UNLOCKED</td>
<td valign="top">
No locks are held on the database.  The database may be neither read nor
written.  Any internally cached data is considered suspect and subject to
verification against the database file before being used.  Other 
processes can read or write the database as their own locking states
permit.  This is the default state.
</td></tr>

<tr><td valign="top">SHARED</td>
<td valign="top">
The database may be read but not written.  Any number of 
processes can hold SHARED locks at the same time, hence there can be
many simultaneous readers.  But no other thread or process is allowed
to write to the database file while one or more SHARED locks are active.
</td></tr>

<tr><td valign="top">RESERVED</td>
<td valign="top">
A RESERVED lock means that the process is planning on writing to the
database file at some point in the future but that it is currently just
reading from the file.  Only a single RESERVED lock may be active at one
time, though multiple SHARED locks can coexist with a single RESERVED lock.
RESERVED differs from PENDING in that new SHARED locks can be acquired
while there is a RESERVED lock.
</td></tr>

<tr><td valign="top">PENDING</td>
<td valign="top">
A PENDING lock means that the process holding the lock wants to write
to the database as soon as possible and is just waiting on all current
SHARED locks to clear so that it can get an EXCLUSIVE lock.  No new 
SHARED locks are permitted against the database if
a PENDING lock is active, though existing SHARED locks are allowed to
continue.
</td></tr>

<tr><td valign="top">EXCLUSIVE</td>
<td valign="top">
An EXCLUSIVE lock is needed in order to write to the database file.
Only one EXCLUSIVE lock is allowed on the file and no other locks of
any kind are allowed to coexist with an EXCLUSIVE lock.  In order to
maximize concurrency, SQLite works to minimize the amount of time that
EXCLUSIVE locks are held.
</td></tr>
</table>
</p>

<p>
The operating system interface layer understands and tracks all five
locking states described above.  
The pager module only tracks four of the five locking states.
A PENDING lock is always just a temporary
stepping stone on the path to an EXCLUSIVE lock and so the pager module
does not track PENDING locks.
</p>
}

HEADING 1 {The Rollback Journal}

puts {
<p>Any time a process wants to make a changes to a database file, it
first records enough information in the <em>rollback journal</em> to
restore the database file back to its initial condition.  Thus, before
altering any page of the database, the original contents of that page
must be written into the journal.  The journal also records the initial
size of the database so that if the database file grows it can be truncated
back to its original size on a rollback.</p>

<p>The rollback journal is a ordinary disk file that has the same name as
the database file with the suffix "<tt>-journal</tt>" added.</p>

<p>If SQLite is working with multiple databases at the same time
(using the ATTACH command) then each database has its own journal.
But there is also a separate aggregate journal
called the <em>master journal</em>.
The master journal does not contain page data used for rolling back
changes.  Instead the master journal contains the names of the
individual file journals for each of the ATTACHed databases.   Each of
the individual file journals also contain the name of the master journal.
If there are no ATTACHed databases (or if none of the ATTACHed database
is participating in the current transaction) no master journal is
created and the normal rollback journal contains an empty string
in the place normally reserved for recording the name of the master
journal.</p>

<p>A individual file journal is said to be <em>hot</em>
if it needs to be rolled back
in order to restore the integrity of its database.  
A hot journal is created when a process is in the middle of a database
update and a program or operating system crash or power failure prevents 
the update from completing.
Hot journals are an exception condition. 
Hot journals exist to recover from crashes and power failures.
If everything is working correctly 
(that is, if there are no crashes or power failures)
you will never get a hot journal.
</p>

<p>
If no master journal is involved, then
a journal is hot if it exists and its corresponding database file
does not have a RESERVED lock.
If a master journal is named in the file journal, then the file journal
is hot if its master journal exists and there is no RESERVED
lock on the corresponding database file.
It is important to understand when a journal is hot so the
preceding rules will be repeated in bullets:
</p>

<ul>
<li>A journal is hot if...
    <ul>
    <li>It exists, and</li>
    <li>It's master journal exists or the master journal name is an
        empty string, and</li>
    <li>There is no RESERVED lock on the corresponding database file.</li>
    </ul>
</li>
</ul>
}

HEADING 2 {Dealing with hot journals}

puts {
<p>
Before reading from a a database file, SQLite always checks to see if that
database file has a hot journal.  If the file does have a hot journal, then
the journal is rolled back before the file is read.  In this way, we ensure
that the database file is in a consistent state before it is read.
</p>

<p>When a process wants to read from a database file, it followed
the following sequence of steps:
</p>

<ol>
<li>Open the database file and obtain a SHARED lock.  If the SHARED lock
    cannot be obtained, fail immediately and return SQLITE_BUSY.</li>
<li>Check to see if the database file has a hot journal.   If the file
    does not have a hot journal, we are done.  Return immediately.
    If there is a hot journal, that journal must be rolled back by
    the subsequent steps of this algorithm.</li>
<li>Acquire a PENDING lock then an EXCLUSIVE lock on the database file.
    (Note: Do not acquire a RESERVED lock because that would make
    other processes think the journal was no longer hot.)  If we
    fail to acquire these locks it means another process
    is already trying to do the rollback.  In that case,
    drop all locks, close the database, and return SQLITE_BUSY. </li>
<li>Read the journal file and roll back the changes.</li>
<li>Wait for the rolled back changes to be written onto 
    the surface of the disk.  This protects the integrity of the database
    in case another power failure or crash occurs.</li>
<li>Delete the journal file.</li>
<li>Delete the master journal file if it is safe to do so.
    This step is optional.  It is here only to prevent stale
    master journals from cluttering up the disk drive.
    See the discussion below for details.</li>
<li>Drop the EXCLUSIVE and PENDING locks but retain the SHARED lock.</li>
</ol>

<p>After the algorithm above completes successfully, it is safe to 
read from the database file.  Once all reading has completed, the
SHARED lock is dropped.</p>
}

HEADING 2 {Deleting stale master journals}

puts {
<p>A stale master journal is a master journal that is no longer being
used for anything.  There is no requirement that stale master journals
be deleted.  The only reason for doing so is to free up disk space.</p>

<p>A master journal is stale if no individual file journals are pointing
to it.  To figure out if a master journal is stale, we first read the
master journal to obtain the names of all of its file journals.  Then
we check each of those file journals.  If any of the file journals named
in the master journal exists and points back to the master journal, then
the master journal is not stale.  If all file journals are either missing
or refer to other master journals or no master journal at all, then the
master journal we are testing is stale and can be safely deleted.</p>
}

HEADING 1 {Writing to a database file}

puts {
<p>To write to a database, a process must first acquire a SHARED lock
as described above (possibly rolling back incomplete changes if there
is a hot journal). 
After a SHARED lock is obtained, a RESERVED lock must be acquired.
The RESERVED lock signals that the process intends to write to the
database at some point in the future.  Only one process at a time
can hold a RESERVED lock.  But other processes can continue to read
the database while the RESERVED lock is held.
</p>

<p>If the process that wants to write is unable to obtain a RESERVED
lock, it must mean that another process already has a RESERVED lock.
In that case, the write attempt fails and returns SQLITE_BUSY.</p>

<p>After obtaining a RESERVED lock, the process that wants to write
creates a rollback journal.  The header of the journal is initialized
with the original size of the database file.  Space in the journal header
is also reserved for a master journal name, though the master journal
name is initially empty.</p>

<p>Before making changes to any page of the database, the process writes
the original content of that page into the rollback journal.  Changes
to pages are held in memory at first and are not written to the disk.
The original database file remains unaltered, which means that other
processes can continue to read the database.</p>

<p>Eventually, the writing process will want to update the database
file, either because its memory cache has filled up or because it is
ready to commit its changes.  Before this happens, the writer must
make sure no other process is reading the database and that the rollback
journal data is safely on the disk surface so that it can be used to
rollback incomplete changes in the event of a power failure.
The steps are as follows:</p>

<ol>
<li>Make sure all rollback journal data has actually been written to
    the surface of the disk (and is not just being held in the operating
    system's  or disk controllers cache) so that if a power failure occurs
    the data will still be there after power is restored.</li>
<li>Obtain a PENDING lock and then an EXCLUSIVE lock on the database file.
    If other processes are still have SHARED locks, the writer might have
    to wait until those SHARED locks clear before it is able to obtain
    an EXCLUSIVE lock.</li>
<li>Write all page modifications currently held in memory out to the
    original database disk file.</li>
</ol>

<p>
If the reason for writing to the database file is because the memory
cache was full, then the writer will not commit right away.  Instead,
the writer might continue to make changes to other pages.  Before 
subsequent changes are written to the database file, the rollback
journal must be flushed to disk again.  Note also that the EXCLUSIVE
lock that the writer obtained in order to write to the database initially
must be held until all changes are committed.  That means that no other
processes are able to access the database from the
time the memory cache first spills to disk until the transaction
commits.
</p>

<p>
When a writer is ready to commit its changes, it executes the following
steps:
</p>

<ol>
<li value="4">
   Obtain an EXCLUSIVE lock on the database file and
   make sure all memory changes have been written to the database file
   using the algorithm of steps 1-3 above.</li>
<li>Flush all database file changes to the disk.  Wait for those changes
    to actually be written onto the disk surface.</li>
<li>Delete the journal file.  This is the instant when the changes are
    committed.  Prior to deleting the journal file, if a power failure
    or crash occurs, the next process to open the database will see that
    it has a hot journal and will roll the changes back.
    After the journal is deleted, there will no longer be a hot journal
    and the changes will persist.
    </li>
<li>Drop the EXCLUSIVE and PENDING locks from the database file.
    </li>
</ol>

<p>As soon as PENDING lock is released from the database file, other
processes can begin reading the database again.  In the current implementation,
the RESERVED lock is also released, but that is not essential.  Future
versions of SQLite might provide a "CHECKPOINT" SQL command that will
commit all changes made so far within a transaction but retain the
RESERVED lock so that additional changes can be made without given
any other process an opportunity to write.</p>

<p>If a transaction involves multiple databases, then a more complex
commit sequence is used, as follows:</p>

<ol>
<li value="4">
   Make sure all individual database files have an EXCLUSIVE lock and a
   valid journal.
<li>Create a master-journal.  The name of the master-journal is arbitrary.
    (The current implementation appends random suffixes to the name of the
    main database file until it finds a name that does not previously exist.)
    Fill the master journal with the names of all the individual journals
    and flush its contents to disk.
<li>Write the name of the master journal into
    all individual journals (in space set aside for that purpose in the
    headers of the individual journals) and flush the contents of the
    individual journals to disk and wait for those changes to reach the
    disk surface.
<li>Flush all database file changes to the disk.  Wait for those changes
    to actually be written onto the disk surface.</li>
<li>Delete the master journal file.  This is the instant when the changes are
    committed.  Prior to deleting the master journal file, if a power failure
    or crash occurs, the individual file journals will be considered hot
    and will be rolled back by the next process that
    attempts to read them.  After the master journal has been deleted,
    the file journals will no longer be considered hot and the changes
    will persist.
    </li>
<li>Delete all individual journal files.
<li>Drop the EXCLUSIVE and PENDING locks from all database files.
    </li>
</ol>
}

HEADING 2 {Writer starvation}

puts {
<p>In SQLite version 2, if many processes are reading from the database,
it might be the case that there is never a time when there are
no active readers.  And if there is always at least one read lock on the
database, no process would ever be able to make changes to the database
because it would be impossible to acquire a write lock.  This situation
is called <em>writer starvation</em>.</p>

<p>SQLite version 3 seeks to avoid writer starvation through the use of
the PENDING lock.  The PENDING lock allows existing readers to continue
but prevents new readers from connecting to the database.  So when a
process wants to write a busy database, it can set a PENDING lock which
will prevent new readers from coming in.  Assuming existing readers do
eventually complete, all SHARED locks will eventually clear and the
writer will be given a chance to make its changes.</p>
}

HEADING 1 {How To Corrupt Your Database Files}

puts {
<p>The pager module is robust but it is not completely failsafe.
It can be subverted.  This section attempts to identify and explain
the risks.</p>

<p>
Clearly, a hardware or operating system fault that introduces incorrect data
into the middle of the database file or journal will cause problems.
Likewise, 
if a rogue process opens a database file or journal and writes malformed
data into the middle of it, then the database will become corrupt.
There is not much that can be done about these kinds of problems
so they are given no further attention.
</p>

<p>
SQLite uses POSIX advisory locks to implement locking on Unix.  On
windows it uses the LockFile(), LockFileEx(), and UnlockFile() system
calls.  SQLite assumes that these system calls all work as advertised.  If
that is not the case, then database corruption can result.  One should
note that POSIX advisory locking is known to be buggy or even unimplemented
on many NFS implementations (including recent versions of Mac OS X)
and that there are reports of locking problems
for network filesystems under windows.  Your best defense is to not
use SQLite for files on a network filesystem.
</p>

<p>
SQLite uses the fsync() system call to flush data to the disk under Unix and
it uses the FlushFileBuffers() to do the same under windows.  Once again,
SQLite assumes that these operating system services function as advertised.
But it has been reported that fsync() and FlushFileBuffers() do not always
work correctly, especially with inexpensive IDE disks.  Apparently some
manufactures of IDE disks have defective controller chips that report
that data has reached the disk surface when in fact the data is still
in volatile cache memory in the disk drive electronics.  There are also
reports that windows sometimes chooses to ignore FlushFileBuffers() for
unspecified reasons.  The author cannot verify any of these reports.
But if they are true, it means that database corruption is a possibility
following an unexpected power loss.  These are hardware and/or operating
system bugs that SQLite is unable to defend against.
</p>

<p>
If a crash or power failure occurs and results in a hot journal but that
journal is deleted, the next process to open the database will not
know that it contains changes that need to be rolled back.  The rollback
will not occur and the database will be left in an inconsistent state.
Rollback journals might be deleted for any number of reasons:
</p>

<ul>
<li>An administrator might be cleaning up after an OS crash or power failure,
    see the journal file, think it is junk, and delete it.</li>
<li>Someone (or some process) might rename the database file but fail to
    also rename its associated journal.</li>
<li>If the database file has aliases (hard or soft links) and the file
    is opened by a different alias than the one used to create the journal,
    then the journal will not be found.  To avoid this problem, you should
    not create links to SQLite database files.</li>
<li>Filesystem corruption following a power failure might cause the
    journal to be renamed or deleted.</li>
</ul>

<p>
The last (fourth) bullet above merits additional comment.  When SQLite creates
a journal file on Unix, it opens the directory that contains that file and
calls fsync() on the directory, in an effort to push the directory information
to disk.  But suppose some other process is adding or removing unrelated
files to the directory that contains the database and journal at the the
moment of a power failure.  The supposedly unrelated actions of this other
process might result in the journal file being dropped from the directory and
moved into "lost+found".  This is an unlikely scenario, but it could happen.
The best defenses are to use a journaling filesystem or to keep the
database and journal in a directory by themselves.
</p>

<p>
For a commit involving multiple databases and a master journal, if the
various databases were on different disk volumes and a power failure occurs
during the commit, then when the machine comes back up the disks might
be remounted with different names.  Or some disks might not be mounted
at all.   When this happens the individual file journals and the master
journal might not be able to find each other. The worst outcome from
this scenario is that the commit ceases to be atomic.  
Some databases might be rolled back and others might not. 
All databases will continue to be self-consistent.
To defend against this problem, keep all databases
on the same disk volume and/or remount disks using exactly the same names
after a power failure.
</p>
}

HEADING 1 {Transaction Control At The SQL Level}

puts {
<p>
The changes to locking and concurrency control in SQLite version 3 also
introduce some subtle changes in the way transactions work at the SQL
language level.
By default, SQLite version 3 operates in <em>autocommit</em> mode.
In autocommit mode,
all changes to the database are committed as soon as all operations associated
with the current database connection complete.</p>

<p>The SQL command "BEGIN TRANSACTION" (the TRANSACTION keyword
is optional) is used to take SQLite out of autocommit mode.
Note that the BEGIN command does not acquire any locks on the database.
After a BEGIN command, a SHARED lock will be acquired when the first
SELECT statement is executed.  A RESERVED lock will be acquired when
the first INSERT, UPDATE, or DELETE statement is executed.  No EXCLUSIVE
lock is acquired until either the memory cache fills up and must
be spilled to disk or until the transaction commits.  In this way,
the system delays blocking read access to the file file until the
last possible moment.
</p>

<p>The SQL command "COMMIT"  does not actually commit the changes to
disk.  It just turns autocommit back on.  Then, at the conclusion of
the command, the regular autocommit logic takes over and causes the
actual commit to disk to occur.
The SQL command "ROLLBACK" also operates by turning autocommit back on,
but it also sets a flag that tells the autocommit logic to rollback rather
than commit.</p>

<p>If the SQL COMMIT command turns autocommit on and the autocommit logic
then tries to commit change but fails because some other process is holding
a SHARED lock, then autocommit is turned back off automatically.  This
allows the user to retry the COMMIT at a later time after the SHARED lock
has had an opportunity to clear.</p>

<p>If multiple commands are being executed against the same SQLite database
connection at the same time, the autocommit is deferred until the very
last command completes.  For example, if a SELECT statement is being
executed, the execution of the command will pause as each row of the
result is returned.  During this pause other INSERT, UPDATE, or DELETE
commands can be executed against other tables in the database.  But none
of these changes will commit until the original SELECT statement finishes.
</p>
}


footer $rcsid

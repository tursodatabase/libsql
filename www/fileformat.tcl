#
# Run this script to generated a fileformat.html output file
#
set rcsid {$Id: fileformat.tcl,v 1.13 2004/10/10 17:24:55 drh Exp $}
source common.tcl
header {SQLite Database File Format (Version 2)}
puts {
<h2>SQLite 2.X Database File Format</h2>

<p>
This document describes the disk file format for SQLite versions 2.1
through 2.8.  SQLite version 3.0 and following uses a very different
format which is described separately.
</p>

<h3>1.0 &nbsp; Layers</h3>

<p>
SQLite is implemented in layers.
(See the <a href="arch.html">architecture description</a>.)
The format of database files is determined by three different
layers in the architecture.
</p>

<ul>
<li>The <b>schema</b> layer implemented by the VDBE.</li>
<li>The <b>b-tree</b> layer implemented by btree.c</li>
<li>The <b>pager</b> layer implemented by pager.c</li>
</ul>

<p>
We will describe each layer beginning with the bottom (pager)
layer and working upwards.
</p>

<h3>2.0 &nbsp; The Pager Layer</h3>

<p>
An SQLite database consists of
"pages" of data.  Each page is 1024 bytes in size.
Pages are numbered beginning with 1.
A page number of 0 is used to indicate "no such page" in the
B-Tree and Schema layers.
</p>

<p>
The pager layer is responsible for implementing transactions
with atomic commit and rollback.  It does this using a separate
journal file.  Whenever a new transaction is started, a journal
file is created that records the original state of the database.
If the program terminates before completing the transaction, the next
process to open the database can use the journal file to restore
the database to its original state.
</p>

<p>
The journal file is located in the same directory as the database
file and has the same name as the database file but with the
characters "<tt>-journal</tt>" appended.
</p>

<p>
The pager layer does not impose any content restrictions on the
main database file.  As far as the pager is concerned, each page
contains 1024 bytes of arbitrary data.  But there is structure to
the journal file.
</p>

<p>
A journal file begins with 8 bytes as follows:
0xd9, 0xd5, 0x05, 0xf9, 0x20, 0xa1, 0x63, and 0xd6.
Processes that are attempting to rollback a journal use these 8 bytes
as a sanity check to make sure the file they think is a journal really
is a valid journal.  Prior version of SQLite used different journal
file formats.  The magic numbers for these prior formats are different
so that if a new version of the library attempts to rollback a journal
created by an earlier version, it can detect that the journal uses
an obsolete format and make the necessary adjustments.  This article
describes only the newest journal format - supported as of version
2.8.0.
</p>

<p>
Following the 8 byte prefix is a three 4-byte integers that tell us
the number of pages that have been committed to the journal,
a magic number used for
sanity checking each page, and the
original size of the main database file before the transaction was
started.  The number of committed pages is used to limit how far
into the journal to read.  The use of the checksum magic number is
described below.
The original size of the database is used to restore the database
file back to its original size.
The size is expressed in pages (1024 bytes per page).
</p>

<p>
All three integers in the journal header and all other multi-byte
numbers used in the journal file are big-endian.
That means that the most significant byte
occurs first.  That way, a journal file that is
originally created on one machine can be rolled back by another
machine that uses a different byte order.  So, for example, a
transaction that failed to complete on your big-endian SparcStation
can still be rolled back on your little-endian Linux box.
</p>

<p>
After the 8-byte prefix and the three 4-byte integers, the
journal file consists of zero or more page records.  Each page
record is a 4-byte (big-endian) page number followed by 1024 bytes
of data and a 4-byte checksum.  
The data is the original content of the database page
before the transaction was started.  So to roll back the transaction,
the data is simply written into the corresponding page of the
main database file.  Pages can appear in the journal in any order,
but they are guaranteed to appear only once. All page numbers will be
between 1 and the maximum specified by the page size integer that
appeared at the beginning of the journal.
</p>

<p>
The so-called checksum at the end of each record is not really a
checksum - it is the sum of the page number and the magic number which
was the second integer in the journal header.  The purpose of this
value is to try to detect journal corruption that might have occurred
because of a power loss or OS crash that occurred which the journal
file was being written to disk.  It could have been the case that the
meta-data for the journal file, specifically the size of the file, had
been written to the disk so that when the machine reboots it appears that
file is large enough to hold the current record.  But even though the
file size has changed, the data for the file might not have made it to
the disk surface at the time of the OS crash or power loss.  This means
that after reboot, the end of the journal file will contain quasi-random
garbage data.  The checksum is an attempt to detect such corruption.  If
the checksum does not match, that page of the journal is not rolled back.
</p>

<p>
Here is a summary of the journal file format:
</p>

<ul>
<li>8 byte prefix: 0xd9, 0xd5, 0x05, 0xf9, 0x20, 0xa1, 0x63, 0xd6</li>
<li>4 byte number of records in journal</li>
<li>4 byte magic number used for page checksums</li>
<li>4 byte initial database page count</li>
<li>Zero or more instances of the following:
   <ul>
   <li>4 byte page number</li>
   <li>1024 bytes of original data for the page</li>
   <li>4 byte checksum</li>
   </ul>
</li>
</ul>

<h3>3.0 &nbsp; The B-Tree Layer</h3>

<p>
The B-Tree layer builds on top of the pager layer to implement
one or more separate b-trees all in the same disk file.  The
algorithms used are taken from Knuth's <i>The Art Of Computer
Programming.</i></p>

<p>
Page 1 of a database contains a header string used for sanity
checking, a few 32-bit words of configuration data, and a pointer
to the beginning of a list of unused pages in the database.
All other pages in the
database are either pages of a b-tree, overflow pages, or unused
pages on the freelist.
</p>

<p>
Each b-tree page contains zero or more database entries.
Each entry has an unique key of one or more bytes and data of
zero or more bytes.
Both the key and data are arbitrary byte sequences.  The combination
of key and data are collectively known as "payload".  The current
implementation limits the amount of payload in a single entry to
1048576 bytes.  This limit can be raised to 16777216 by adjusting
a single #define in the source code and recompiling.  But most entries
contain less than a hundred bytes of payload so a megabyte limit seems
more than enough.
</p>

<p>
Up to 238 bytes of payload for an entry can be held directly on
a b-tree page.  Any additional payload is contained on a linked list
of overflow pages.  This limit on the amount of payload held directly
on b-tree pages guarantees that each b-tree page can hold at least
4 entries.  In practice, most entries are smaller than 238 bytes and
thus most pages can hold more than 4 entries.
</p>

<p>
A single database file can hold any number of separate, independent b-trees.
Each b-tree is identified by its root page, which never changes.
Child pages of the b-tree may change as entries are added and removed
and pages split and combine.  But the root page always stays the same.
The b-tree itself does not record which pages are root pages and which
are not.  That information is handled entirely at the schema layer.
</p>

<h4>3.1 &nbsp; B-Tree Page 1 Details</h4>

<p>
Page 1 begins with the following 48-byte string:
</p>

<blockquote><pre>
** This file contains an SQLite 2.1 database **
</pre></blockquote>

<p>
If you count the number of characters in the string above, you will
see that there are only 47.  A '\000' terminator byte is added to
bring the total to 48.
</p>

<p>
A frequent question is why the string says version 2.1 when (as
of this writing) we are up to version 2.7.0 of SQLite and any
change to the second digit of the version is suppose to represent
a database format change.  The answer to this is that the B-tree
layer has not changed any since version 2.1.  There have been
database format changes since version 2.1 but those changes have
all been in the schema layer.  Because the format of the b-tree
layer is unchanged since version 2.1.0, the header string still
says version 2.1.
</p>

<p>
After the format string is a 4-byte integer used to determine the
byte-order of the database.  The integer has a value of
0xdae37528.  If this number is expressed as 0xda, 0xe3, 0x75, 0x28, then
the database is in a big-endian format and all 16 and 32-bit integers
elsewhere in the b-tree layer are also big-endian.  If the number is
expressed as 0x28, 0x75, 0xe3, and 0xda, then the database is in a
little-endian format and all other multi-byte numbers in the b-tree 
layer are also little-endian.  
Prior to version 2.6.3, the SQLite engine was only able to read databases
that used the same byte order as the processor they were running on.
But beginning with 2.6.3, SQLite can read or write databases in any
byte order.
</p>

<p>
After the byte-order code are six 4-byte integers.  Each integer is in the
byte order determined by the byte-order code.  The first integer is the
page number for the first page of the freelist.  If there are no unused
pages in the database, then this integer is 0.  The second integer is
the number of unused pages in the database.  The last 4 integers are
not used by the b-tree layer.  These are the so-called "meta" values that
are passed up to the schema layer
and used there for configuration and format version information.
All bytes of page 1 past beyond the meta-value integers are unused 
and are initialized to zero.
</p>

<p>
Here is a summary of the information contained on page 1 in the b-tree layer:
</p>

<ul>
<li>48 byte header string</li>
<li>4 byte integer used to determine the byte-order</li>
<li>4 byte integer which is the first page of the freelist</li>
<li>4 byte integer which is the number of pages on the freelist</li>
<li>36 bytes of meta-data arranged as nine 4-byte integers</li>
<li>928 bytes of unused space</li>
</ul>

<h4>3.2 &nbsp; Structure Of A Single B-Tree Page</h4>

<p>
Conceptually, a b-tree page contains N database entries and N+1 pointers
to other b-tree pages.
</p>

<blockquote>
<table border=1 cellspacing=0 cellpadding=5>
<tr>
<td align="center">Ptr<br>0</td>
<td align="center">Entry<br>0</td>
<td align="center">Ptr<br>1</td>
<td align="center">Entry<br>1</td>
<td align="center"><b>...</b></td>
<td align="center">Ptr<br>N-1</td>
<td align="center">Entry<br>N-1</td>
<td align="center">Ptr<br>N</td>
</tr>
</table>
</blockquote>

<p>
The entries are arranged in increasing order.  That is, the key to
Entry 0 is less than the key to Entry 1, and the key to Entry 1 is
less than the key of Entry 2, and so forth.  The pointers point to
pages containing additional entries that have keys in between the
entries on either side.  So Ptr 0 points to another b-tree page that
contains entries that all have keys less than Key 0, and Ptr 1
points to a b-tree pages where all entries have keys greater than Key 0
but less than Key 1, and so forth.
</p>

<p>
Each b-tree page in SQLite consists of a header, zero or more "cells"
each holding a single entry and pointer, and zero or more "free blocks"
that represent unused space on the page.
</p>

<p>
The header on a b-tree page is the first 8 bytes of the page.
The header contains the value
of the right-most pointer (Ptr N) and the byte offset into the page
of the first cell and the first free block.  The pointer is a 32-bit
value and the offsets are each 16-bit values.  We have:
</p>

<blockquote>
<table border=1 cellspacing=0 cellpadding=5>
<tr>
<td align="center" width=30>0</td>
<td align="center" width=30>1</td>
<td align="center" width=30>2</td>
<td align="center" width=30>3</td>
<td align="center" width=30>4</td>
<td align="center" width=30>5</td>
<td align="center" width=30>6</td>
<td align="center" width=30>7</td>
</tr>
<tr>
<td align="center" colspan=4>Ptr N</td>
<td align="center" colspan=2>Cell 0</td>
<td align="center" colspan=2>Freeblock 0</td>
</tr>
</table>
</blockquote>

<p>
The 1016 bytes of a b-tree page that come after the header contain
cells and freeblocks.  All 1016 bytes are covered by either a cell
or a freeblock.
</p>

<p>
The cells are connected in a linked list.  Cell 0 contains Ptr 0 and
Entry 0.  Bytes 4 and 5 of the header point to Cell 0.  Cell 0 then
points to Cell 1 which contains Ptr 1 and Entry 1.  And so forth.
Cells vary in size.  Every cell has a 12-byte header and at least 4
bytes of payload space.  Space is allocated to payload in increments
of 4 bytes.  Thus the minimum size of a cell is 16 bytes and up to
63 cells can fit on a single page.  The size of a cell is always a multiple
of 4 bytes.
A cell can have up to 238 bytes of payload space.  If
the payload is more than 238 bytes, then an additional 4 byte page
number is appended to the cell which is the page number of the first
overflow page containing the additional payload.  The maximum size
of a cell is thus 254 bytes, meaning that a least 4 cells can fit into
the 1016 bytes of space available on a b-tree page.
An average cell is usually around 52 to 100 bytes in size with about
10 or 20 cells to a page.
</p>

<p>
The data layout of a cell looks like this:
</p>

<blockquote>
<table border=1 cellspacing=0 cellpadding=5>
<tr>
<td align="center" width=20>0</td>
<td align="center" width=20>1</td>
<td align="center" width=20>2</td>
<td align="center" width=20>3</td>
<td align="center" width=20>4</td>
<td align="center" width=20>5</td>
<td align="center" width=20>6</td>
<td align="center" width=20>7</td>
<td align="center" width=20>8</td>
<td align="center" width=20>9</td>
<td align="center" width=20>10</td>
<td align="center" width=20>11</td>
<td align="center" width=100>12 ... 249</td>
<td align="center" width=20>250</td>
<td align="center" width=20>251</td>
<td align="center" width=20>252</td>
<td align="center" width=20>253</td>
</tr>
<tr>
<td align="center" colspan=4>Ptr</td>
<td align="center" colspan=2>Keysize<br>(low)</td>
<td align="center" colspan=2>Next</td>
<td align="center" colspan=1>Ksz<br>(hi)</td>
<td align="center" colspan=1>Dsz<br>(hi)</td>
<td align="center" colspan=2>Datasize<br>(low)</td>
<td align="center" colspan=1>Payload</td>
<td align="center" colspan=4>Overflow<br>Pointer</td>
</tr>
</table>
</blockquote>

<p>
The first four bytes are the pointer.  The size of the key is a 24-bit
where the upper 8 bits are taken from byte 8 and the lower 16 bits are
taken from bytes 4 and 5 (or bytes 5 and 4 on little-endian machines.)
The size of the data is another 24-bit value where the upper 8 bits
are taken from byte 9 and the lower 16 bits are taken from bytes 10 and
11 or 11 and 10, depending on the byte order.  Bytes 6 and 7 are the
offset to the next cell in the linked list of all cells on the current
page.  This offset is 0 for the last cell on the page.
</p>

<p>
The payload itself can be any number of bytes between 1 and 1048576.
But space to hold the payload is allocated in 4-byte chunks up to
238 bytes.  If the entry contains more than 238 bytes of payload, then
additional payload data is stored on a linked list of overflow pages.
A 4 byte page number is appended to the cell that contains the first
page of this linked list.
</p>

<p>
Each overflow page begins with a 4-byte value which is the
page number of the next overflow page in the list.   This value is
0 for the last page in the list.  The remaining
1020 bytes of the overflow page are available for storing payload.
Note that a full page is allocated regardless of the number of overflow
bytes stored.  Thus, if the total payload for an entry is 239 bytes,
the first 238 are stored in the cell and the overflow page stores just
one byte.
</p>

<p>
The structure of an overflow page looks like this:
</p>

<blockquote>
<table border=1 cellspacing=0 cellpadding=5>
<tr>
<td align="center" width=20>0</td>
<td align="center" width=20>1</td>
<td align="center" width=20>2</td>
<td align="center" width=20>3</td>
<td align="center" width=200>4 ... 1023</td>
</tr>
<tr>
<td align="center" colspan=4>Next Page</td>
<td align="center" colspan=1>Overflow Data</td>
</tr>
</table>
</blockquote>

<p>
All space on a b-tree page which is not used by the header or by cells
is filled by freeblocks.  Freeblocks, like cells, are variable in size.
The size of a freeblock is at least 4 bytes and is always a multiple of
4 bytes.
The first 4 bytes contain a header and the remaining bytes
are unused.  The structure of the freeblock is as follows:
</p>

<blockquote>
<table border=1 cellspacing=0 cellpadding=5>
<tr>
<td align="center" width=20>0</td>
<td align="center" width=20>1</td>
<td align="center" width=20>2</td>
<td align="center" width=20>3</td>
<td align="center" width=200>4 ... 1015</td>
</tr>
<tr>
<td align="center" colspan=2>Size</td>
<td align="center" colspan=2>Next</td>
<td align="center" colspan=1>Unused</td>
</tr>
</table>
</blockquote>

<p>
Freeblocks are stored in a linked list in increasing order.  That is
to say, the first freeblock occurs at a lower index into the page than
the second free block, and so forth.  The first 2 bytes of the header
are an integer which is the total number of bytes in the freeblock.
The second 2 bytes are the index into the page of the next freeblock
in the list.  The last freeblock has a Next value of 0.
</p>

<p>
When a new b-tree is created in a database, the root page of the b-tree
consist of a header and a single 1016 byte freeblock.  As entries are
added, space is carved off of that freeblock and used to make cells.
When b-tree entries are deleted, the space used by their cells is converted
into freeblocks.  Adjacent freeblocks are merged, but the page can still
become fragmented.  The b-tree code will occasionally try to defragment
the page by moving all cells to the beginning and constructing a single
freeblock at the end to take up all remaining space.
</p>

<h4>3.3 &nbsp; The B-Tree Free Page List</h4>

<p>
When information is removed from an SQLite database such that one or
more pages are no longer needed, those pages are added to a list of
free pages so that they can be reused later when new information is
added.  This subsection describes the structure of this freelist.
</p>

<p>
The 32-bit integer beginning at byte-offset 52 in page 1 of the database
contains the address of the first page in a linked list of free pages.
If there are no free pages available, this integer has a value of 0.
The 32-bit integer at byte-offset 56 in page 1 contains the number of
free pages on the freelist.
</p>

<p>
The freelist contains a trunk and many branches.  The trunk of
the freelist is composed of overflow pages.  That is to say, each page
contains a single 32-bit integer at byte offset 0 which
is the page number of the next page on the freelist trunk.
The payload area
of each trunk page is used to record pointers to branch pages. 
The first 32-bit integer in the payload area of a trunk page
is the number of branch pages to follow (between 0 and 254)
and each subsequent 32-bit integer is a page number for a branch page.
The following diagram shows the structure of a trunk freelist page:
</p>

<blockquote>
<table border=1 cellspacing=0 cellpadding=5>
<tr>
<td align="center" width=20>0</td>
<td align="center" width=20>1</td>
<td align="center" width=20>2</td>
<td align="center" width=20>3</td>
<td align="center" width=20>4</td>
<td align="center" width=20>5</td>
<td align="center" width=20>6</td>
<td align="center" width=20>7</td>
<td align="center" width=200>8 ... 1023</td>
</tr>
<tr>
<td align="center" colspan=4>Next trunk page</td>
<td align="center" colspan=4># of branch pages</td>
<td align="center" colspan=1>Page numbers for branch pages</td>
</tr>
</table>
</blockquote>

<p>
It is important to note that only the pages on the trunk of the freelist
contain pointers to other pages.  The branch pages contain no
data whatsoever.  The fact that the branch pages are completely
blank allows for an important optimization in the paging layer.  When
a branch page is removed from the freelist to be reused, it is not
necessary to write the original content of that page into the rollback
journal.  The branch page contained no data to begin with, so there is
no need to restore the page in the event of a rollback.  Similarly,
when a page is not longer needed and is added to the freelist as a branch
page, it is not necessary to write the content of that page
into the database file.
Again, the page contains no real data so it is not necessary to record the
content of that page.  By reducing the amount of disk I/O required,
these two optimizations allow some database operations
to go four to six times faster than they would otherwise.
</p>

<h3>4.0 &nbsp; The Schema Layer</h3>

<p>
The schema layer implements an SQL database on top of one or more
b-trees and keeps track of the root page numbers for all b-trees.
Where the b-tree layer provides only unformatted data storage with
a unique key, the schema layer allows each entry to contain multiple
columns.  The schema layer also allows indices and non-unique key values.
</p>

<p>
The schema layer implements two separate data storage abstractions:
tables and indices.  Each table and each index uses its own b-tree
but they use the b-tree capabilities in different ways.  For a table,
the b-tree key is a unique 4-byte integer and the b-tree data is the
content of the table row, encoded so that columns can be separately
extracted.  For indices, the b-tree key varies in size depending on the
size of the fields being indexed and the b-tree data is empty.
</p>

<h4>4.1 &nbsp; SQL Table Implementation Details</h4>

<p>Each row of an SQL table is stored in a single b-tree entry.
The b-tree key is a 4-byte big-endian integer that is the ROWID
or INTEGER PRIMARY KEY for that table row.
The key is stored in a big-endian format so
that keys will sort in numerical order using memcmp() function.</p>

<p>The content of a table row is stored in the data portion of
the corresponding b-tree table.  The content is encoded to allow
individual columns of the row to be extracted as necessary.  Assuming
that the table has N columns, the content is encoded as N+1 offsets
followed by N column values, as follows:
</p>

<blockquote>
<table border=1 cellspacing=0 cellpadding=5>
<tr>
<td>offset 0</td>
<td>offset 1</td>
<td><b>...</b></td>
<td>offset N-1</td>
<td>offset N</td>
<td>value 0</td>
<td>value 1</td>
<td><b>...</b></td>
<td>value N-1</td>
</tr>
</table>
</blockquote>

<p>
The offsets can be either 8-bit, 16-bit, or 24-bit integers depending
on how much data is to be stored.  If the total size of the content
is less than 256 bytes then 8-bit offsets are used.  If the total size
of the b-tree data is less than 65536 then 16-bit offsets are used.
24-bit offsets are used otherwise.  Offsets are always little-endian,
which means that the least significant byte occurs first.
</p>

<p>
Data is stored as a nul-terminated string.  Any empty string consists
of just the nul terminator.  A NULL value is an empty string with no
nul-terminator.  Thus a NULL value occupies zero bytes and an empty string
occupies 1 byte.
</p>

<p>
Column values are stored in the order that they appear in the CREATE TABLE
statement.  The offsets at the beginning of the record contain the
byte index of the corresponding column value.  Thus, Offset 0 contains
the byte index for Value 0, Offset 1 contains the byte offset
of Value 1, and so forth.  The number of bytes in a column value can
always be found by subtracting offsets.  This allows NULLs to be
recovered from the record unambiguously.
</p>

<p>
Most columns are stored in the b-tree data as described above.
The one exception is column that has type INTEGER PRIMARY KEY.
INTEGER PRIMARY KEY columns correspond to the 4-byte b-tree key.
When an SQL statement attempts to read the INTEGER PRIMARY KEY,
the 4-byte b-tree key is read rather than information out of the
b-tree data.  But there is still an Offset associated with the
INTEGER PRIMARY KEY, just like any other column.  But the Value
associated with that offset is always NULL.
</p>

<h4>4.2 &nbsp; SQL Index Implementation Details</h4>

<p>
SQL indices are implement using a b-tree in which the key is used
but the data is always empty.  The purpose of an index is to map
one or more column values into the ROWID for the table entry that
contains those column values.
</p>

<p>
Each b-tree in an index consists of one or more column values followed
by a 4-byte ROWID.  Each column value is nul-terminated (even NULL values)
and begins with a single character that indicates the datatype for that
column value.  Only three datatypes are supported: NULL, Number, and
Text.  NULL values are encoded as the character 'a' followed by the
nul terminator.  Numbers are encoded as the character 'b' followed by
a string that has been crafted so that sorting the string using memcmp()
will sort the corresponding numbers in numerical order.  (See the
sqliteRealToSortable() function in util.c of the SQLite sources for
additional information on this encoding.)  Numbers are also nul-terminated.
Text values consists of the character 'c' followed by a copy of the
text string and a nul-terminator.  These encoding rules result in
NULLs being sorted first, followed by numerical values in numerical
order, followed by text values in lexicographical order.
</p>

<h4>4.4 &nbsp; SQL Schema Storage And Root B-Tree Page Numbers</h4>

<p>
The database schema is stored in the database in a special tabled named
"sqlite_master" and which always has a root b-tree page number of 2.
This table contains the original CREATE TABLE,
CREATE INDEX, CREATE VIEW, and CREATE TRIGGER statements used to define
the database to begin with.  Whenever an SQLite database is opened,
the sqlite_master table is scanned from beginning to end and 
all the original CREATE statements are played back through the parser
in order to reconstruct an in-memory representation of the database
schema for use in subsequent command parsing.  For each CREATE TABLE
and CREATE INDEX statement, the root page number for the corresponding
b-tree is also recorded in the sqlite_master table so that SQLite will
know where to look for the appropriate b-tree.
</p>

<p>
SQLite users can query the sqlite_master table just like any other table
in the database.  But the sqlite_master table cannot be directly written.
The sqlite_master table is automatically updated in response to CREATE
and DROP statements but it cannot be changed using INSERT, UPDATE, or
DELETE statements as that would risk corrupting the database.
</p>

<p>
SQLite stores temporary tables and indices in a separate
file from the main database file.  The temporary table database file
is the same structure as the main database file.  The schema table
for the temporary tables is stored on page 2 just as in the main
database.  But the schema table for the temporary database named
"sqlite_temp_master" instead of "sqlite_master".  Other than the
name change, it works exactly the same.
</p>

<h4>4.4 &nbsp; Schema Version Numbering And Other Meta-Information</h4>

<p>
The nine 32-bit integers that are stored beginning at byte offset
60 of Page 1 in the b-tree layer are passed up into the schema layer
and used for versioning and configuration information.  The meaning
of the first four integers is shown below.  The other five are currently
unused.
</p>

<ol>
<li>The schema version number</li>
<li>The format version number</li>
<li>The recommended pager cache size</li>
<li>The safety level</li>
</ol>

<p>
The first meta-value, the schema version number, is used to detect when
the schema of the database is changed by a CREATE or DROP statement.
Recall that when a database is first opened the sqlite_master table is
scanned and an internal representation of the tables, indices, views,
and triggers for the database is built in memory.  This internal
representation is used for all subsequent SQL command parsing and
execution.  But what if another process were to change the schema
by adding or removing a table, index, view, or trigger?  If the original
process were to continue using the old schema, it could potentially
corrupt the database by writing to a table that no longer exists.
To avoid this problem, the schema version number is changed whenever
a CREATE or DROP statement is executed.  Before each command is
executed, the current schema version number for the database file
is compared against the schema version number from when the sqlite_master
table was last read.  If those numbers are different, the internal
schema representation is erased and the sqlite_master table is reread
to reconstruct the internal schema representation.
(Calls to sqlite_exec() generally return SQLITE_SCHEMA when this happens.)
</p>

<p>
The second meta-value is the schema format version number.  This
number tells what version of the schema layer should be used to
interpret the file.  There have been changes to the schema layer
over time and this number is used to detect when an older database
file is being processed by a newer version of the library.
As of this writing (SQLite version 2.7.0) the current format version
is "4".
</p>

<p>
The third meta-value is the recommended pager cache size as set 
by the DEFAULT_CACHE_SIZE pragma.  If the value is positive it
means that synchronous behavior is enable (via the DEFAULT_SYNCHRONOUS
pragma) and if negative it means that synchronous behavior is
disabled.
</p>

<p>
The fourth meta-value is safety level added in version 2.8.0.
A value of 1 corresponds to a SYNCHRONOUS setting of OFF.  In other
words, SQLite does not pause to wait for journal data to reach the disk
surface before overwriting pages of the database.  A value of 2 corresponds
to a SYNCHRONOUS setting of NORMAL.  A value of 3 corresponds to a
SYNCHRONOUS setting of FULL. If the value is 0, that means it has not
been initialized so the default synchronous setting of NORMAL is used.
</p>
}
footer $rcsid

#
# Run this Tcl script to generate the sqlite.html file.
#
set rcsid {$Id: c_interface.tcl,v 1.41 2004/08/30 14:58:12 drh Exp $}
source common.tcl
header {The C language interface to the SQLite library}
puts {
<h2>The C language interface to the SQLite library</h2>

<p>The SQLite library is designed to be very easy to use from
a C or C++ program.  This document gives an overview of the C/C++
programming interface.</p>

<h3>1.0 The Core API</h3>

<p>The interface to the SQLite library consists of three core functions,
one opaque data structure, and some constants used as return values.
The core interface is as follows:</p>

<blockquote><pre>
typedef struct sqlite sqlite;
#define SQLITE_OK           0   /* Successful result */

sqlite *sqlite_open(const char *dbname, int mode, char **errmsg);

void sqlite_close(sqlite *db);

int sqlite_exec(
  sqlite *db,
  char *sql,
  int (*xCallback)(void*,int,char**,char**),
  void *pArg,
  char **errmsg
);
</pre></blockquote>

<p>
The above is all you really need to know in order to use SQLite
in your C or C++ programs.  There are other interface functions
available (and described below) but we will begin by describing
the core functions shown above.
</p>

<a name="sqlite_open">
<h4>1.1 Opening a database</h4>

<p>Use the <b>sqlite_open</b> function to open an existing SQLite
database or to create a new SQLite database.  The first argument
is the database name.  The second argument is intended to signal
whether the database is going to be used for reading and writing
or just for reading.  But in the current implementation, the
second argument to <b>sqlite_open</b> is ignored.
The third argument is a pointer to a string pointer.
If the third argument is not NULL and an error occurs
while trying to open the database, then an error message will be
written to memory obtained from malloc() and *errmsg will be made
to point to this error message.  The calling function is responsible
for freeing the memory when it has finished with it.</p>

<p>The name of an SQLite database is the name of a file that will
contain the database.  If the file does not exist, SQLite attempts
to create and initialize it.  If the file is read-only (due to
permission bits or because it is located on read-only media like
a CD-ROM) then SQLite opens the database for reading only.  The
entire SQL database is stored in a single file on the disk.  But
additional temporary files may be created during the execution of
an SQL command in order to store the database rollback journal or
temporary and intermediate results of a query.</p>

<p>The return value of the <b>sqlite_open</b> function is a
pointer to an opaque <b>sqlite</b> structure.  This pointer will
be the first argument to all subsequent SQLite function calls that
deal with the same database.  NULL is returned if the open fails
for any reason.</p>

<a name="sqlite_close">
<h4>1.2 Closing the database</h4>

<p>To close an SQLite database, call the <b>sqlite_close</b>
function passing it the sqlite structure pointer that was obtained
from a prior call to <b>sqlite_open</b>.
If a transaction is active when the database is closed, the transaction
is rolled back.</p>

<a name="sqlite_exec">
<h4>1.3 Executing SQL statements</h4>

<p>The <b>sqlite_exec</b> function is used to process SQL statements
and queries.  This function requires 5 parameters as follows:</p>

<ol>
<li><p>A pointer to the sqlite structure obtained from a prior call
       to <b>sqlite_open</b>.</p></li>
<li><p>A null-terminated string containing the text of one or more
       SQL statements and/or queries to be processed.</p></li>
<li><p>A pointer to a callback function which is invoked once for each
       row in the result of a query.  This argument may be NULL, in which
       case no callbacks will ever be invoked.</p></li>
<li><p>A pointer that is forwarded to become the first argument
       to the callback function.</p></li>
<li><p>A pointer to an error string.  Error messages are written to space
       obtained from malloc() and the error string is made to point to
       the malloced space.  The calling function is responsible for freeing
       this space when it has finished with it.
       This argument may be NULL, in which case error messages are not
       reported back to the calling function.</p></li>
</ol>

<p>
The callback function is used to receive the results of a query.  A
prototype for the callback function is as follows:</p>

<blockquote><pre>
int Callback(void *pArg, int argc, char **argv, char **columnNames){
  return 0;
}
</pre></blockquote>

<a name="callback_row_data">
<p>The first argument to the callback is just a copy of the fourth argument
to <b>sqlite_exec</b>  This parameter can be used to pass arbitrary
information through to the callback function from client code.
The second argument is the number of columns in the query result.
The third argument is an array of pointers to strings where each string
is a single column of the result for that record.  Note that the
callback function reports a NULL value in the database as a NULL pointer,
which is very different from an empty string.  If the i-th parameter
is an empty string, we will get:</p>
<blockquote><pre>
argv[i][0] == 0
</pre></blockquote>
<p>But if the i-th parameter is NULL we will get:</p>
<blockquote><pre>
argv[i] == 0
</pre></blockquote>

<p>The names of the columns are contained in first <i>argc</i>
entries of the fourth argument.
If the <a href="lang.html#pragma_show_datatypes">SHOW_DATATYPES</a> pragma
is on (it is off by default) then
the second <i>argc</i> entries in the 4th argument are the datatypes
for the corresponding columns.
</p>

<p>If the <a href="lang.html#pragma_empty_result_callbacks">
EMPTY_RESULT_CALLBACKS</a> pragma is set to ON and the result of
a query is an empty set, then the callback is invoked once with the
third parameter (argv) set to 0.  In other words
<blockquote><pre>
argv == 0
</pre></blockquote>
The second parameter (argc)
and the fourth parameter (columnNames) are still valid
and can be used to determine the number and names of the result
columns if there had been a result.
The default behavior is not to invoke the callback at all if the
result set is empty.</p>

<a name="callback_returns_nonzero">
<p>The callback function should normally return 0.  If the callback
function returns non-zero, the query is immediately aborted and 
<b>sqlite_exec</b> will return SQLITE_ABORT.</p>

<h4>1.4 Error Codes</h4>

<p>
The <b>sqlite_exec</b> function normally returns SQLITE_OK.  But
if something goes wrong it can return a different value to indicate
the type of error.  Here is a complete list of the return codes:
</p>

<blockquote><pre>
#define SQLITE_OK           0   /* Successful result */
#define SQLITE_ERROR        1   /* SQL error or missing database */
#define SQLITE_INTERNAL     2   /* An internal logic error in SQLite */
#define SQLITE_PERM         3   /* Access permission denied */
#define SQLITE_ABORT        4   /* Callback routine requested an abort */
#define SQLITE_BUSY         5   /* The database file is locked */
#define SQLITE_LOCKED       6   /* A table in the database is locked */
#define SQLITE_NOMEM        7   /* A malloc() failed */
#define SQLITE_READONLY     8   /* Attempt to write a readonly database */
#define SQLITE_INTERRUPT    9   /* Operation terminated by sqlite_interrupt() */
#define SQLITE_IOERR       10   /* Some kind of disk I/O error occurred */
#define SQLITE_CORRUPT     11   /* The database disk image is malformed */
#define SQLITE_NOTFOUND    12   /* (Internal Only) Table or record not found */
#define SQLITE_FULL        13   /* Insertion failed because database is full */
#define SQLITE_CANTOPEN    14   /* Unable to open the database file */
#define SQLITE_PROTOCOL    15   /* Database lock protocol error */
#define SQLITE_EMPTY       16   /* (Internal Only) Database table is empty */
#define SQLITE_SCHEMA      17   /* The database schema changed */
#define SQLITE_TOOBIG      18   /* Too much data for one row of a table */
#define SQLITE_CONSTRAINT  19   /* Abort due to contraint violation */
#define SQLITE_MISMATCH    20   /* Data type mismatch */
#define SQLITE_MISUSE      21   /* Library used incorrectly */
#define SQLITE_NOLFS       22   /* Uses OS features not supported on host */
#define SQLITE_AUTH        23   /* Authorization denied */
#define SQLITE_ROW         100  /* sqlite_step() has another row ready */
#define SQLITE_DONE        101  /* sqlite_step() has finished executing */
</pre></blockquote>

<p>
The meanings of these various return values are as follows:
</p>

<blockquote>
<dl>
<dt>SQLITE_OK</dt>
<dd><p>This value is returned if everything worked and there were no errors.
</p></dd>
<dt>SQLITE_INTERNAL</dt>
<dd><p>This value indicates that an internal consistency check within
the SQLite library failed.  This can only happen if there is a bug in
the SQLite library.  If you ever get an SQLITE_INTERNAL reply from
an <b>sqlite_exec</b> call, please report the problem on the SQLite
mailing list.
</p></dd>
<dt>SQLITE_ERROR</dt>
<dd><p>This return value indicates that there was an error in the SQL
that was passed into the <b>sqlite_exec</b>.
</p></dd>
<dt>SQLITE_PERM</dt>
<dd><p>This return value says that the access permissions on the database
file are such that the file cannot be opened.
</p></dd>
<dt>SQLITE_ABORT</dt>
<dd><p>This value is returned if the callback function returns non-zero.
</p></dd>
<dt>SQLITE_BUSY</dt>
<dd><p>This return code indicates that another program or thread has
the database locked.  SQLite allows two or more threads to read the
database at the same time, but only one thread can have the database
open for writing at the same time.  Locking in SQLite is on the
entire database.</p>
</p></dd>
<dt>SQLITE_LOCKED</dt>
<dd><p>This return code is similar to SQLITE_BUSY in that it indicates
that the database is locked.  But the source of the lock is a recursive
call to <b>sqlite_exec</b>.  This return can only occur if you attempt
to invoke sqlite_exec from within a callback routine of a query
from a prior invocation of sqlite_exec.  Recursive calls to
sqlite_exec are allowed as long as they do
not attempt to write the same table.
</p></dd>
<dt>SQLITE_NOMEM</dt>
<dd><p>This value is returned if a call to <b>malloc</b> fails.
</p></dd>
<dt>SQLITE_READONLY</dt>
<dd><p>This return code indicates that an attempt was made to write to
a database file that is opened for reading only.
</p></dd>
<dt>SQLITE_INTERRUPT</dt>
<dd><p>This value is returned if a call to <b>sqlite_interrupt</b>
interrupts a database operation in progress.
</p></dd>
<dt>SQLITE_IOERR</dt>
<dd><p>This value is returned if the operating system informs SQLite
that it is unable to perform some disk I/O operation.  This could mean
that there is no more space left on the disk.
</p></dd>
<dt>SQLITE_CORRUPT</dt>
<dd><p>This value is returned if SQLite detects that the database it is
working on has become corrupted.  Corruption might occur due to a rogue
process writing to the database file or it might happen due to an 
perviously undetected logic error in of SQLite. This value is also
returned if a disk I/O error occurs in such a way that SQLite is forced
to leave the database file in a corrupted state.  The latter should only
happen due to a hardware or operating system malfunction.
</p></dd>
<dt>SQLITE_FULL</dt>
<dd><p>This value is returned if an insertion failed because there is
no space left on the disk, or the database is too big to hold any
more information.  The latter case should only occur for databases
that are larger than 2GB in size.
</p></dd>
<dt>SQLITE_CANTOPEN</dt>
<dd><p>This value is returned if the database file could not be opened
for some reason.
</p></dd>
<dt>SQLITE_PROTOCOL</dt>
<dd><p>This value is returned if some other process is messing with
file locks and has violated the file locking protocol that SQLite uses
on its rollback journal files.
</p></dd>
<dt>SQLITE_SCHEMA</dt>
<dd><p>When the database first opened, SQLite reads the database schema
into memory and uses that schema to parse new SQL statements.  If another
process changes the schema, the command currently being processed will
abort because the virtual machine code generated assumed the old
schema.  This is the return code for such cases.  Retrying the
command usually will clear the problem.
</p></dd>
<dt>SQLITE_TOOBIG</dt>
<dd><p>SQLite will not store more than about 1 megabyte of data in a single
row of a single table.  If you attempt to store more than 1 megabyte
in a single row, this is the return code you get.
</p></dd>
<dt>SQLITE_CONSTRAINT</dt>
<dd><p>This constant is returned if the SQL statement would have violated
a database constraint.
</p></dd>
<dt>SQLITE_MISMATCH</dt>
<dd><p>This error occurs when there is an attempt to insert non-integer
data into a column labeled INTEGER PRIMARY KEY.  For most columns, SQLite
ignores the data type and allows any kind of data to be stored.  But
an INTEGER PRIMARY KEY column is only allowed to store integer data.
</p></dd>
<dt>SQLITE_MISUSE</dt>
<dd><p>This error might occur if one or more of the SQLite API routines
is used incorrectly.  Examples of incorrect usage include calling
<b>sqlite_exec</b> after the database has been closed using
<b>sqlite_close</b> or 
calling <b>sqlite_exec</b> with the same
database pointer simultaneously from two separate threads.
</p></dd>
<dt>SQLITE_NOLFS</dt>
<dd><p>This error means that you have attempts to create or access a file
database file that is larger that 2GB on a legacy Unix machine that
lacks large file support.
</p></dd>
<dt>SQLITE_AUTH</dt>
<dd><p>This error indicates that the authorizer callback
has disallowed the SQL you are attempting to execute.
</p></dd>
<dt>SQLITE_ROW</dt>
<dd><p>This is one of the return codes from the
<b>sqlite_step</b> routine which is part of the non-callback API.
It indicates that another row of result data is available.
</p></dd>
<dt>SQLITE_DONE</dt>
<dd><p>This is one of the return codes from the
<b>sqlite_step</b> routine which is part of the non-callback API.
It indicates that the SQL statement has been completely executed and
the <b>sqlite_finalize</b> routine is ready to be called.
</p></dd>
</dl>
</blockquote>

<h3>2.0 Accessing Data Without Using A Callback Function</h3>

<p>
The <b>sqlite_exec</b> routine described above used to be the only
way to retrieve data from an SQLite database.  But many programmers found
it inconvenient to use a callback function to obtain results.  So beginning
with SQLite version 2.7.7, a second access interface is available that
does not use callbacks.
</p>

<p>
The new interface uses three separate functions to replace the single
<b>sqlite_exec</b> function.
</p>

<blockquote><pre>
typedef struct sqlite_vm sqlite_vm;

int sqlite_compile(
  sqlite *db,              /* The open database */
  const char *zSql,        /* SQL statement to be compiled */
  const char **pzTail,     /* OUT: uncompiled tail of zSql */
  sqlite_vm **ppVm,        /* OUT: the virtual machine to execute zSql */
  char **pzErrmsg          /* OUT: Error message. */
);

int sqlite_step(
  sqlite_vm *pVm,          /* The virtual machine to execute */
  int *pN,                 /* OUT: Number of columns in result */
  const char ***pazValue,  /* OUT: Column data */
  const char ***pazColName /* OUT: Column names and datatypes */
);

int sqlite_finalize(
  sqlite_vm *pVm,          /* The virtual machine to be finalized */
  char **pzErrMsg          /* OUT: Error message */
);
</pre></blockquote>

<p>
The strategy is to compile a single SQL statement using
<b>sqlite_compile</b> then invoke <b>sqlite_step</b> multiple times,
once for each row of output, and finally call <b>sqlite_finalize</b>
to clean up after the SQL has finished execution.
</p>

<h4>2.1 Compiling An SQL Statement Into A Virtual Machine</h4>

<p>
The <b>sqlite_compile</b> "compiles" a single SQL statement (specified
by the second parameter) and generates a virtual machine that is able
to execute that statement.  
As with must interface routines, the first parameter must be a pointer
to an sqlite structure that was obtained from a prior call to
<b>sqlite_open</b>.

<p>
A pointer to the virtual machine is stored in a pointer which is passed
in as the 4th parameter.
Space to hold the virtual machine is dynamically allocated.  To avoid
a memory leak, the calling function must invoke
<b>sqlite_finalize</b> on the virtual machine after it has finished
with it.
The 4th parameter may be set to NULL if an error is encountered during
compilation.
</p>

<p>
If any errors are encountered during compilation, an error message is
written into memory obtained from <b>malloc</b> and the 5th parameter
is made to point to that memory.  If the 5th parameter is NULL, then
no error message is generated.  If the 5th parameter is not NULL, then
the calling function should dispose of the memory containing the error
message by calling <b>sqlite_freemem</b>.
</p>

<p>
If the 2nd parameter actually contains two or more statements of SQL,
only the first statement is compiled.  (This is different from the
behavior of <b>sqlite_exec</b> which executes all SQL statements
in its input string.)  The 3rd parameter to <b>sqlite_compile</b>
is made to point to the first character beyond the end of the first
statement of SQL in the input.  If the 2nd parameter contains only
a single SQL statement, then the 3rd parameter will be made to point
to the '\000' terminator at the end of the 2nd parameter.
</p>

<p>
On success, <b>sqlite_compile</b> returns SQLITE_OK.
Otherwise and error code is returned.
</p>

<h4>2.2 Step-By-Step Execution Of An SQL Statement</h4>

<p>
After a virtual machine has been generated using <b>sqlite_compile</b>
it is executed by one or more calls to <b>sqlite_step</b>.  Each
invocation of <b>sqlite_step</b>, except the last one,
returns a single row of the result.
The number of columns in  the result is stored in the integer that
the 2nd parameter points to.
The pointer specified by the 3rd parameter is made to point
to an array of pointers to column values.
The pointer in the 4th parameter is made to point to an array
of pointers to column names and datatypes.
The 2nd through 4th parameters to <b>sqlite_step</b> convey the
same information as the 2nd through 4th parameters of the
<b>callback</b> routine when using
the <b>sqlite_exec</b> interface.  Except, with <b>sqlite_step</b>
the column datatype information is always included in the in the
4th parameter regardless of whether or not the
<a href="lang.html#pragma_show_datatypes">SHOW_DATATYPES</a> pragma
is on or off.
</p>

<p>
Each invocation of <b>sqlite_step</b> returns an integer code that
indicates what happened during that step.  This code may be
SQLITE_BUSY, SQLITE_ROW, SQLITE_DONE, SQLITE_ERROR, or
SQLITE_MISUSE.
</p>

<p>
If the virtual machine is unable to open the database file because
it is locked by another thread or process, <b>sqlite_step</b>
will return SQLITE_BUSY.  The calling function should do some other
activity, or sleep, for a short amount of time to give the lock a
chance to clear, then invoke <b>sqlite_step</b> again.  This can
be repeated as many times as desired.
</p>

<p>
Whenever another row of result data is available,
<b>sqlite_step</b> will return SQLITE_ROW.  The row data is
stored in an array of pointers to strings and the 2nd parameter
is made to point to this array.
</p>

<p>
When all processing is complete, <b>sqlite_step</b> will return
either SQLITE_DONE or SQLITE_ERROR.  SQLITE_DONE indicates that the
statement completed successfully and SQLITE_ERROR indicates that there
was a run-time error.  (The details of the error are obtained from
<b>sqlite_finalize</b>.)  It is a misuse of the library to attempt
to call <b>sqlite_step</b> again after it has returned SQLITE_DONE
or SQLITE_ERROR.
</p>

<p>
When <b>sqlite_step</b> returns SQLITE_DONE or SQLITE_ERROR,
the *pN and *pazColName values are set to the number of columns
in the result set and to the names of the columns, just as they
are for an SQLITE_ROW return.  This allows the calling code to
find the number of result columns and the column names and datatypes
even if the result set is empty.  The *pazValue parameter is always
set to NULL when the return codes is SQLITE_DONE or SQLITE_ERROR.
If the SQL being executed is a statement that does not
return a result (such as an INSERT or an UPDATE) then *pN will
be set to zero and *pazColName will be set to NULL.
</p>

<p>
If you abuse the library by trying to call <b>sqlite_step</b>
inappropriately it will attempt return SQLITE_MISUSE.
This can happen if you call sqlite_step() on the same virtual machine
at the same
time from two or more threads or if you call sqlite_step()
again after it returned SQLITE_DONE or SQLITE_ERROR or if you
pass in an invalid virtual machine pointer to sqlite_step().
You should not depend on the SQLITE_MISUSE return code to indicate
an error.  It is possible that a misuse of the interface will go
undetected and result in a program crash.  The SQLITE_MISUSE is
intended as a debugging aid only - to help you detect incorrect
usage prior to a mishap.  The misuse detection logic is not guaranteed
to work in every case.
</p>

<h4>2.3 Deleting A Virtual Machine</h4>

<p>
Every virtual machine that <b>sqlite_compile</b> creates should
eventually be handed to <b>sqlite_finalize</b>.  The sqlite_finalize()
procedure deallocates the memory and other resources that the virtual
machine uses.  Failure to call sqlite_finalize() will result in 
resource leaks in your program.
</p>

<p>
The <b>sqlite_finalize</b> routine also returns the result code
that indicates success or failure of the SQL operation that the
virtual machine carried out.
The value returned by sqlite_finalize() will be the same as would
have been returned had the same SQL been executed by <b>sqlite_exec</b>.
The error message returned will also be the same.
</p>

<p>
It is acceptable to call <b>sqlite_finalize</b> on a virtual machine
before <b>sqlite_step</b> has returned SQLITE_DONE.  Doing so has
the effect of interrupting the operation in progress.  Partially completed
changes will be rolled back and the database will be restored to its
original state (unless an alternative recovery algorithm is selected using
an ON CONFLICT clause in the SQL being executed.)  The effect is the
same as if a callback function of <b>sqlite_exec</b> had returned
non-zero.
</p>

<p>
It is also acceptable to call <b>sqlite_finalize</b> on a virtual machine
that has never been passed to <b>sqlite_step</b> even once.
</p>

<h3>3.0 The Extended API</h3>

<p>Only the three core routines described in section 1.0 are required to use
SQLite.  But there are many other functions that provide 
useful interfaces.  These extended routines are as follows:
</p>

<blockquote><pre>
int sqlite_last_insert_rowid(sqlite*);

int sqlite_changes(sqlite*);

int sqlite_get_table(
  sqlite*,
  char *sql,
  char ***result,
  int *nrow,
  int *ncolumn,
  char **errmsg
);

void sqlite_free_table(char**);

void sqlite_interrupt(sqlite*);

int sqlite_complete(const char *sql);

void sqlite_busy_handler(sqlite*, int (*)(void*,const char*,int), void*);

void sqlite_busy_timeout(sqlite*, int ms);

const char sqlite_version[];

const char sqlite_encoding[];

int sqlite_exec_printf(
  sqlite*,
  char *sql,
  int (*)(void*,int,char**,char**),
  void*,
  char **errmsg,
  ...
);

int sqlite_exec_vprintf(
  sqlite*,
  char *sql,
  int (*)(void*,int,char**,char**),
  void*,
  char **errmsg,
  va_list
);

int sqlite_get_table_printf(
  sqlite*,
  char *sql,
  char ***result,
  int *nrow,
  int *ncolumn,
  char **errmsg,
  ...
);

int sqlite_get_table_vprintf(
  sqlite*,
  char *sql,
  char ***result,
  int *nrow,
  int *ncolumn,
  char **errmsg,
  va_list
);

char *sqlite_mprintf(const char *zFormat, ...);

char *sqlite_vmprintf(const char *zFormat, va_list);

void sqlite_freemem(char*);

void sqlite_progress_handler(sqlite*, int, int (*)(void*), void*);

</pre></blockquote>

<p>All of the above definitions are included in the "sqlite.h"
header file that comes in the source tree.</p>

<h4>3.1 The ROWID of the most recent insert</h4>

<p>Every row of an SQLite table has a unique integer key.  If the
table has a column labeled INTEGER PRIMARY KEY, then that column
serves as the key.  If there is no INTEGER PRIMARY KEY column then
the key is a unique integer.  The key for a row can be accessed in
a SELECT statement or used in a WHERE or ORDER BY clause using any
of the names "ROWID", "OID", or "_ROWID_".</p>

<p>When you do an insert into a table that does not have an INTEGER PRIMARY
KEY column, or if the table does have an INTEGER PRIMARY KEY but the value
for that column is not specified in the VALUES clause of the insert, then
the key is automatically generated.  You can find the value of the key
for the most recent INSERT statement using the
<b>sqlite_last_insert_rowid</b> API function.</p>

<h4>3.2 The number of rows that changed</h4>

<p>The <b>sqlite_changes</b> API function returns the number of rows
that have been inserted, deleted, or modified since the database was
last quiescent.  A "quiescent" database is one in which there are
no outstanding calls to <b>sqlite_exec</b> and no VMs created by
<b>sqlite_compile</b> that have not been finalized by <b>sqlite_finalize</b>.
In common usage, <b>sqlite_changes</b> returns the number
of rows inserted, deleted, or modified by the most recent <b>sqlite_exec</b>
call or since the most recent <b>sqlite_compile</b>.  But if you have
nested calls to <b>sqlite_exec</b> (that is, if the callback routine
of one <b>sqlite_exec</b> invokes another <b>sqlite_exec</b>) or if
you invoke <b>sqlite_compile</b> to create a new VM while there is
still another VM in existance, then
the meaning of the number returned by <b>sqlite_changes</b> is more
complex.
The number reported includes any changes
that were later undone by a ROLLBACK or ABORT.  But rows that are
deleted because of a DROP TABLE are <em>not</em> counted.</p>

<p>SQLite implements the command "<b>DELETE FROM table</b>" (without
a WHERE clause) by dropping the table then recreating it.  
This is much faster than deleting the elements of the table individually.
But it also means that the value returned from <b>sqlite_changes</b>
will be zero regardless of the number of elements that were originally
in the table.  If an accurate count of the number of elements deleted
is necessary, use "<b>DELETE FROM table WHERE 1</b>" instead.</p>

<h4>3.3 Querying into memory obtained from malloc()</h4>

<p>The <b>sqlite_get_table</b> function is a wrapper around
<b>sqlite_exec</b> that collects all the information from successive
callbacks and writes it into memory obtained from malloc().  This
is a convenience function that allows the application to get the
entire result of a database query with a single function call.</p>

<p>The main result from <b>sqlite_get_table</b> is an array of pointers
to strings.  There is one element in this array for each column of
each row in the result.  NULL results are represented by a NULL
pointer. In addition to the regular data, there is an added row at the 
beginning of the array that contains the name of each column of the
result.</p>

<p>As an example, consider the following query:</p>

<blockquote>
SELECT employee_name, login, host FROM users WHERE login LIKE 'd%';
</blockquote>

<p>This query will return the name, login and host computer name
for every employee whose login begins with the letter "d".  If this
query is submitted to <b>sqlite_get_table</b> the result might
look like this:</p>

<blockquote>
nrow = 2<br>
ncolumn = 3<br>
result[0] = "employee_name"<br>
result[1] = "login"<br>
result[2] = "host"<br>
result[3] = "dummy"<br>
result[4] = "No such user"<br>
result[5] = 0<br>
result[6] = "D. Richard Hipp"<br>
result[7] = "drh"<br>
result[8] = "zadok"
</blockquote>

<p>Notice that the "host" value for the "dummy" record is NULL so
the result[] array contains a NULL pointer at that slot.</p>

<p>If the result set of a query is empty, then by default
<b>sqlite_get_table</b> will set nrow to 0 and leave its
result parameter is set to NULL.  But if the EMPTY_RESULT_CALLBACKS
pragma is ON then the result parameter is initialized to the names
of the columns only.  For example, consider this query which has
an empty result set:</p>

<blockquote>
SELECT employee_name, login, host FROM users WHERE employee_name IS NULL;
</blockquote>

<p>
The default behavior gives this results:
</p>

<blockquote>
nrow = 0<br>
ncolumn = 0<br>
result = 0<br>
</blockquote>

<p>
But if the EMPTY_RESULT_CALLBACKS pragma is ON, then the following
is returned:
</p>

<blockquote>
nrow = 0<br>
ncolumn = 3<br>
result[0] = "employee_name"<br>
result[1] = "login"<br>
result[2] = "host"<br>
</blockquote>

<p>Memory to hold the information returned by <b>sqlite_get_table</b>
is obtained from malloc().  But the calling function should not try
to free this information directly.  Instead, pass the complete table
to <b>sqlite_free_table</b> when the table is no longer needed.
It is safe to call <b>sqlite_free_table</b> with a NULL pointer such
as would be returned if the result set is empty.</p>

<p>The <b>sqlite_get_table</b> routine returns the same integer
result code as <b>sqlite_exec</b>.</p>

<h4>3.4 Interrupting an SQLite operation</h4>

<p>The <b>sqlite_interrupt</b> function can be called from a
different thread or from a signal handler to cause the current database
operation to exit at its first opportunity.  When this happens,
the <b>sqlite_exec</b> routine (or the equivalent) that started
the database operation will return SQLITE_INTERRUPT.</p>

<h4>3.5 Testing for a complete SQL statement</h4>

<p>The next interface routine to SQLite is a convenience function used
to test whether or not a string forms a complete SQL statement.
If the <b>sqlite_complete</b> function returns true when its input
is a string, then the argument forms a complete SQL statement.
There are no guarantees that the syntax of that statement is correct,
but we at least know the statement is complete.  If <b>sqlite_complete</b>
returns false, then more text is required to complete the SQL statement.</p>

<p>For the purpose of the <b>sqlite_complete</b> function, an SQL
statement is complete if it ends in a semicolon.</p>

<p>The <b>sqlite</b> command-line utility uses the <b>sqlite_complete</b>
function to know when it needs to call <b>sqlite_exec</b>.  After each
line of input is received, <b>sqlite</b> calls <b>sqlite_complete</b>
on all input in its buffer.  If <b>sqlite_complete</b> returns true, 
then <b>sqlite_exec</b> is called and the input buffer is reset.  If
<b>sqlite_complete</b> returns false, then the prompt is changed to
the continuation prompt and another line of text is read and added to
the input buffer.</p>

<h4>3.6 Library version string</h4>

<p>The SQLite library exports the string constant named
<b>sqlite_version</b> which contains the version number of the
library.  The header file contains a macro SQLITE_VERSION
with the same information.  If desired, a program can compare
the SQLITE_VERSION macro against the <b>sqlite_version</b>
string constant to verify that the version number of the
header file and the library match.</p> 

<h4>3.7 Library character encoding</h4>

<p>By default, SQLite assumes that all data uses a fixed-size
8-bit character (iso8859).  But if you give the --enable-utf8 option
to the configure script, then the library assumes UTF-8 variable
sized characters.  This makes a difference for the LIKE and GLOB
operators and the LENGTH() and SUBSTR() functions.  The static
string <b>sqlite_encoding</b> will be set to either "UTF-8" or
"iso8859" to indicate how the library was compiled.  In addition,
the <b>sqlite.h</b> header file will define one of the
macros <b>SQLITE_UTF8</b> or <b>SQLITE_ISO8859</b>, as appropriate.</p>

<p>Note that the character encoding mechanism used by SQLite cannot
be changed at run-time.  This is a compile-time option only.  The
<b>sqlite_encoding</b> character string just tells you how the library
was compiled.</p>

<h4>3.8 Changing the library's response to locked files</h4>

<p>The <b>sqlite_busy_handler</b> procedure can be used to register
a busy callback with an open SQLite database.  The busy callback will
be invoked whenever SQLite tries to access a database that is locked.
The callback will typically do some other useful work, or perhaps sleep,
in order to give the lock a chance to clear.  If the callback returns
non-zero, then SQLite tries again to access the database and the cycle
repeats.  If the callback returns zero, then SQLite aborts the current
operation and returns SQLITE_BUSY.</p>

<p>The arguments to <b>sqlite_busy_handler</b> are the opaque
structure returned from <b>sqlite_open</b>, a pointer to the busy
callback function, and a generic pointer that will be passed as
the first argument to the busy callback.  When SQLite invokes the
busy callback, it sends it three arguments:  the generic pointer
that was passed in as the third argument to <b>sqlite_busy_handler</b>,
the name of the database table or index that the library is trying
to access, and the number of times that the library has attempted to
access the database table or index.</p>

<p>For the common case where we want the busy callback to sleep,
the SQLite library provides a convenience routine <b>sqlite_busy_timeout</b>.
The first argument to <b>sqlite_busy_timeout</b> is a pointer to
an open SQLite database and the second argument is a number of milliseconds.
After <b>sqlite_busy_timeout</b> has been executed, the SQLite library
will wait for the lock to clear for at least the number of milliseconds 
specified before it returns SQLITE_BUSY.  Specifying zero milliseconds for
the timeout restores the default behavior.</p>

<h4>3.9 Using the <tt>_printf()</tt> wrapper functions</h4>

<p>The four utility functions</p>

<p>
<ul>
<li><b>sqlite_exec_printf()</b></li>
<li><b>sqlite_exec_vprintf()</b></li>
<li><b>sqlite_get_table_printf()</b></li>
<li><b>sqlite_get_table_vprintf()</b></li>
</ul>
</p>

<p>implement the same query functionality as <b>sqlite_exec</b>
and <b>sqlite_get_table</b>.  But instead of taking a complete
SQL statement as their second argument, the four <b>_printf</b>
routines take a printf-style format string.  The SQL statement to
be executed is generated from this format string and from whatever
additional arguments are attached to the end of the function call.</p>

<p>There are two advantages to using the SQLite printf
functions instead of <b>sprintf</b>.  First of all, with the
SQLite printf routines, there is never a danger of overflowing a
static buffer as there is with <b>sprintf</b>.  The SQLite
printf routines automatically allocate (and later frees)
as much memory as is 
necessary to hold the SQL statements generated.</p>

<p>The second advantage the SQLite printf routines have over
<b>sprintf</b> are two new formatting options specifically designed
to support string literals in SQL.  Within the format string,
the %q formatting option works very much like %s in that it
reads a null-terminated string from the argument list and inserts
it into the result.  But %q translates the inserted string by
making two copies of every single-quote (') character in the
substituted string.  This has the effect of escaping the end-of-string
meaning of single-quote within a string literal. The %Q formatting
option works similar; it translates the single-quotes like %q and
additionally encloses the resulting string in single-quotes.
If the argument for the %Q formatting options is a NULL pointer,
the resulting string is NULL without single quotes.
</p>

<p>Consider an example.  Suppose you are trying to insert a string
value into a database table where the string value was obtained from
user input.  Suppose the string to be inserted is stored in a variable
named zString.  The code to do the insertion might look like this:</p>

<blockquote><pre>
sqlite_exec_printf(db,
  "INSERT INTO table1 VALUES('%s')",
  0, 0, 0, zString);
</pre></blockquote>

<p>If the zString variable holds text like "Hello", then this statement
will work just fine.  But suppose the user enters a string like 
"Hi y'all!".  The SQL statement generated reads as follows:

<blockquote><pre>
INSERT INTO table1 VALUES('Hi y'all')
</pre></blockquote>

<p>This is not valid SQL because of the apostrophy in the word "y'all".
But if the %q formatting option is used instead of %s, like this:</p>

<blockquote><pre>
sqlite_exec_printf(db,
  "INSERT INTO table1 VALUES('%q')",
  0, 0, 0, zString);
</pre></blockquote>

<p>Then the generated SQL will look like the following:</p>

<blockquote><pre>
INSERT INTO table1 VALUES('Hi y''all')
</pre></blockquote>

<p>Here the apostrophy has been escaped and the SQL statement is well-formed.
When generating SQL on-the-fly from data that might contain a
single-quote character ('), it is always a good idea to use the
SQLite printf routines and the %q formatting option instead of <b>sprintf</b>.
</p>

<p>If the %Q formatting option is used instead of %q, like this:</p>

<blockquote><pre>
sqlite_exec_printf(db,
  "INSERT INTO table1 VALUES(%Q)",
  0, 0, 0, zString);
</pre></blockquote>

<p>Then the generated SQL will look like the following:</p>

<blockquote><pre>
INSERT INTO table1 VALUES('Hi y''all')
</pre></blockquote>

<p>If the value of the zString variable is NULL, the generated SQL
will look like the following:</p>

<blockquote><pre>
INSERT INTO table1 VALUES(NULL)
</pre></blockquote>

<p>All of the _printf() routines above are built around the following
two functions:</p>

<blockquote><pre>
char *sqlite_mprintf(const char *zFormat, ...);
char *sqlite_vmprintf(const char *zFormat, va_list);
</pre></blockquote>

<p>The <b>sqlite_mprintf()</b> routine works like the the standard library
<b>sprintf()</b> except that it writes its results into memory obtained
from malloc() and returns a pointer to the malloced buffer.  
<b>sqlite_mprintf()</b> also understands the %q and %Q extensions described
above.  The <b>sqlite_vmprintf()</b> is a varargs version of the same
routine.  The string pointer that these routines return should be freed
by passing it to <b>sqlite_freemem()</b>.
</p>

<h4>3.10 Performing background jobs during large queries</h3>

<p>The <b>sqlite_progress_handler()</b> routine can be used to register a
callback routine with an SQLite database to be invoked periodically during long
running calls to <b>sqlite_exec()</b>, <b>sqlite_step()</b> and the various
wrapper functions.
</p>

<p>The callback is invoked every N virtual machine operations, where N is
supplied as the second argument to <b>sqlite_progress_handler()</b>. The third
and fourth arguments to <b>sqlite_progress_handler()</b> are a pointer to the
routine to be invoked and a void pointer to be passed as the first argument to
it.
</p>

<p>The time taken to execute each virtual machine operation can vary based on
many factors.  A typical value for a 1 GHz PC is between half and three million
per second but may be much higher or lower, depending on the query.  As such it
is difficult to schedule background operations based on virtual machine
operations. Instead, it is recommended that a callback be scheduled relatively
frequently (say every 1000 instructions) and external timer routines used to
determine whether or not background jobs need to be run.  
</p>

<a name="cfunc">
<h3>4.0 Adding New SQL Functions</h3>

<p>Beginning with version 2.4.0, SQLite allows the SQL language to be
extended with new functions implemented as C code.  The following interface
is used:
</p>

<blockquote><pre>
typedef struct sqlite_func sqlite_func;

int sqlite_create_function(
  sqlite *db,
  const char *zName,
  int nArg,
  void (*xFunc)(sqlite_func*,int,const char**),
  void *pUserData
);
int sqlite_create_aggregate(
  sqlite *db,
  const char *zName,
  int nArg,
  void (*xStep)(sqlite_func*,int,const char**),
  void (*xFinalize)(sqlite_func*),
  void *pUserData
);

char *sqlite_set_result_string(sqlite_func*,const char*,int);
void sqlite_set_result_int(sqlite_func*,int);
void sqlite_set_result_double(sqlite_func*,double);
void sqlite_set_result_error(sqlite_func*,const char*,int);

void *sqlite_user_data(sqlite_func*);
void *sqlite_aggregate_context(sqlite_func*, int nBytes);
int sqlite_aggregate_count(sqlite_func*);
</pre></blockquote>

<p>
The <b>sqlite_create_function()</b> interface is used to create 
regular functions and <b>sqlite_create_aggregate()</b> is used to
create new aggregate functions.  In both cases, the <b>db</b>
parameter is an open SQLite database on which the functions should
be registered, <b>zName</b> is the name of the new function,
<b>nArg</b> is the number of arguments, and <b>pUserData</b> is
a pointer which is passed through unchanged to the C implementation
of the function.  Both routines return 0 on success and non-zero
if there are any errors.
</p>

<p>
The length of a function name may not exceed 255 characters.
Any attempt to create a function whose name exceeds 255 characters
in length will result in an error.
</p>

<p>
For regular functions, the <b>xFunc</b> callback is invoked once
for each function call.  The implementation of xFunc should call
one of the <b>sqlite_set_result_...</b> interfaces to return its
result.  The <b>sqlite_user_data()</b> routine can be used to
retrieve the <b>pUserData</b> pointer that was passed in when the
function was registered.
</p>

<p>
For aggregate functions, the <b>xStep</b> callback is invoked once
for each row in the result and then <b>xFinalize</b> is invoked at the
end to compute a final answer.  The xStep routine can use the
<b>sqlite_aggregate_context()</b> interface to allocate memory that
will be unique to that particular instance of the SQL function.
This memory will be automatically deleted after xFinalize is called.
The <b>sqlite_aggregate_count()</b> routine can be used to find out
how many rows of data were passed to the aggregate.  The xFinalize
callback should invoke one of the <b>sqlite_set_result_...</b>
interfaces to set the final result of the aggregate.
</p>

<p>
SQLite now implements all of its built-in functions using this
interface.  For additional information and examples on how to create
new SQL functions, review the SQLite source code in the file
<b>func.c</b>.
</p>

<h3>5.0 Multi-Threading And SQLite</h3>

<p>
If SQLite is compiled with the THREADSAFE preprocessor macro set to 1,
then it is safe to use SQLite from two or more threads of the same process
at the same time.  But each thread should have its own <b>sqlite*</b>
pointer returned from <b>sqlite_open</b>.  It is never safe for two
or more threads to access the same <b>sqlite*</b> pointer at the same time.
</p>

<p>
In precompiled SQLite libraries available on the website, the Unix
versions are compiled with THREADSAFE turned off but the windows
versions are compiled with THREADSAFE turned on.  If you need something
different that this you will have to recompile.
</p>

<p>
Under Unix, an <b>sqlite*</b> pointer should not be carried across a
<b>fork()</b> system call into the child process.  The child process
should open its own copy of the database after the <b>fork()</b>.
</p>

<h3>6.0 Usage Examples</h3>

<p>For examples of how the SQLite C/C++ interface can be used,
refer to the source code for the <b>sqlite</b> program in the
file <b>src/shell.c</b> of the source tree.
Additional information about sqlite is available at
<a href="sqlite.html">sqlite.html</a>.
See also the sources to the Tcl interface for SQLite in
the source file <b>src/tclsqlite.c</b>.</p>
}
footer $rcsid

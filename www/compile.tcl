#
# Run this Tcl script to generate the compile.html file.
#
set rcsid {$Id: compile.tcl,v 1.5 2005/03/19 15:10:45 drh Exp $ }
source common.tcl
header {Compilation Options For SQLite}

puts {
<h1>Compilation Options For SQLite</h1>

<p>
For most purposes, SQLite can be built just fine using the default
compilation options. However, if required, the compile-time options
documented below can be used to 
<a href="#omitfeatures">omit SQLite features</a> (resulting in
a smaller compiled library size) or to change the
<a href="#defaults">default values</a> of some parameters.
</p>
<p>
Every effort has been made to ensure that the various combinations
of compilation options work harmoniously and produce a working library.
Nevertheless, it is strongly recommended that the SQLite test-suite
be executed to check for errors before using an SQLite library built
with non-standard compilation options.
</p>
<a name="defaults"></a>
<h2>Options To Set Default Parameter Values</h2>

<p><b>SQLITE_DEFAULT_AUTOVACUUM=<i>&lt;1 or 0&gt;</i></b><br>
This macro determines if SQLite creates databases with the 
<a href="pragma.html#pragma_auto_vacuum">auto-vacuum</a> 
flag set by default. The default value is 0 (do not create auto-vacuum
databases). In any case the compile-time default may be overridden by the 
"PRAGMA auto_vacuum" command.
</p>

<p><b>SQLITE_DEFAULT_CACHE_SIZE=<i>&lt;pages&gt;</i></b><br>
This macro sets the default size of the page-cache for each attached
database, in pages. This can be overridden by the "PRAGMA cache_size"
comamnd. The default value is 2000.
</p>

<p><b>SQLITE_DEFAULT_PAGE_SIZE=<i>&lt;bytes&gt;</i></b><br>
This macro is used to set the default page-size used when a
database is created. The value assigned must be a power of 2. The
default value is 1024. The compile-time default may be overridden at 
runtime by the "PRAGMA page_size" command.
</p>

<p><b>SQLITE_DEFAULT_TEMP_CACHE_SIZE=<i>&lt;pages&gt;</i></b><br>
This macro sets the default size of the page-cache for temporary files
created by SQLite to store intermediate results, in pages. It does
not affect the page-cache for the temp database, where tables created
using "CREATE TEMP TABLE" are stored. The default value is 500.
</p>

<p><b>SQLITE_MAX_PAGE_SIZE=<i>&lt;bytes&gt;</i></b><br>
This is used to set the maximum allowable page-size that can
be specified by the "PRAGMA page_size" command. The default value
is 8192.
</p>

<a name="omitfeatures"></a>
<h2>Options To Omit Features</h2>

<p>The following options are used to reduce the size of the compiled
library by omiting optional features. This is probably only useful
in embedded systems where space is especially tight, as even with all
features included the SQLite library is relatively small. Don't forget
to tell your compiler to optimize for binary size! (the -Os option if
using GCC).</p>

<p>The macros in this section do not require values. The following 
compilation switches all have the same effect:<br>
-DSQLITE_OMIT_ALTERTABLE<br>
-DSQLITE_OMIT_ALTERTABLE=1<br>
-DSQLITE_OMIT_ALTERTABLE=0
</p>

<p>If any of these options are defined, then the same set of SQLITE_OMIT_XXX
options must also be defined when using the 'lemon' tool to generate a parse.c
file. Because of this, these options may only used when the library is built
from source, not from the collection of pre-packaged C files provided for
non-UNIX like platforms on the website.
</p>

<p><b>SQLITE_OMIT_ALTERTABLE</b><br>
When this option is defined, the 
<a href="lang_altertable.html">ALTER TABLE</a> command is not included in the 
library. Executing an ALTER TABLE statement causes a parse error.
</p>

<p><b>SQLITE_OMIT_AUTHORIZATION</b><br>
Defining this option omits the authorization callback feature from the
library. The <a href="capi3ref.html#sqlite3_set_authorizer">
sqlite3_set_authorizer()</a> API function is not present in the library.
</p>

<p><b>SQLITE_OMIT_AUTOVACUUM</b><br>
If this option is defined, the library cannot create or write to 
databases that support 
<a href="pragma.html#pragma_auto_vacuum">auto-vacuum</a>. Executing a
"PRAGMA auto_vacuum" statement is not an error, but does not return a value
or modify the auto-vacuum flag in the database file. If a database that
supports auto-vacuum is opened by a library compiled with this option, it
is automatically opened in read-only mode.
</p>

<p><b>SQLITE_OMIT_AUTOINCREMENT</b><br>
This option is used to omit the AUTOINCREMENT functionality. When this 
is macro is defined, columns declared as "INTEGER PRIMARY KEY AUTOINCREMENT"
behave in the same way as columns declared as "INTEGER PRIMARY KEY" when a 
NULL is inserted. The sqlite_sequence system table is neither created, nor
respected if it already exists.
</p>
<p><i>TODO: Need a link here - AUTOINCREMENT is not yet documented</i><p>

<p><b>SQLITE_OMIT_BLOB_LITERAL</b><br>
When this option is defined, it is not possible to specify a blob in
an SQL statement using the X'ABCD' syntax.</p> 
}
#<p>WARNING: The VACUUM command depends on this syntax for vacuuming databases
#that contain blobs, so disabling this functionality may render a database
#unvacuumable.
#</p>
#<p><i>TODO: Need a link here - is that syntax documented anywhere?</i><p>
puts {

<p><b>SQLITE_OMIT_COMPLETE</b><br>
This option causes the <a href="capi3ref.html#sqlite3_complete">
sqlite3_complete</a> API to be omitted.
</p>

<p><b>SQLITE_OMIT_COMPOUND_SELECT</b><br>
This option is used to omit the compound SELECT functionality. 
<a href="lang_select.html">SELECT statements</a> that use the 
UNION, UNION ALL, INTERSECT or EXCEPT compound SELECT operators will 
cause a parse error.
</p>

<p><b>SQLITE_OMIT_CONFLICT_CLAUSE</b><br>
In the future, this option will be used to omit the 
<a href="lang_conflict.html">ON CONFLICT</a> clause from the library.
</p>

<p><b>SQLITE_OMIT_DATETIME_FUNCS</b><br>
If this option is defined, SQLite's built-in date and time manipulation
functions are omitted. Specifically, the SQL functions julianday(), date(),
time(), datetime() and strftime() are not available. The default column
values CURRENT_TIME, CURRENT_DATE and CURRENT_DATETIME are still available.
</p>

<p><b>SQLITE_OMIT_EXPLAIN</b><br>
Defining this option causes the EXPLAIN command to be omitted from the
library. Attempting to execute an EXPLAIN statement will cause a parse
error.
</p>

<p><b>SQLITE_OMIT_FLOATING_POINT</b><br>
This option is used to omit floating-point number support from the SQLite
library. When specified, specifying a floating point number as a literal 
(i.e. "1.01") results in a parse error.
</p>
<p>In the future, this option may also disable other floating point 
functionality, for example the sqlite3_result_double(), 
sqlite3_bind_double(), sqlite3_value_double() and sqlite3_column_double() 
API functions.
</p>

<p><b>SQLITE_OMIT_FOREIGN_KEY</b><br>
If this option is defined, FOREIGN KEY clauses in column declarations are
ignored.
</p>

<p><b>SQLITE_OMIT_INTEGRITY_CHECK</b><br>
This option may be used to omit the 
<a href="pragma.html#pragma_integrity_check">"PRAGMA integrity_check"</a> 
command from the compiled library.
</p>

<p><b>SQLITE_OMIT_MEMORYDB</b><br>
When this is defined, the library does not respect the special database
name ":memory:" (normally used to create an in-memory database). If 
":memory:" is passed to sqlite3_open(), a file with this name will be 
opened or created.
</p>

<p><b>SQLITE_OMIT_PAGER_PRAGMAS</b><br>
Defining this option omits pragmas related to the pager subsystem from 
the build. Currently, the 
<a href="pragma.html#pragma_default_cache_size">default_cache_size</a> and 
<a href="pragma.html#pragma_cache_size">cache_size</a> pragmas are omitted.
</p>

<p><b>SQLITE_OMIT_PRAGMA</b><br>
This option is used to omit the <a href="pragma.html">PRAGMA command</a> 
from the library. Note that it is useful to define the macros that omit
specific pragmas in addition to this, as they may also remove supporting code
in other sub-systems. This macro removes the PRAGMA command only.
</p>

<p><b>SQLITE_OMIT_PROGRESS_CALLBACK</b><br>
This option may be defined to omit the capability to issue "progress" 
callbacks during long-running SQL statements. The 
<a href="capi3ref.html#sqlite3_progress_handler">sqlite3_progress_handler()</a>
API function is not present in the library.

<p><b>SQLITE_OMIT_REINDEX</b><br>
When this option is defined, the <a href="lang_reindex.html">REINDEX</a> 
command is not included in the library. Executing a REINDEX statement causes 
a parse error.
</p>

<p><b>SQLITE_OMIT_SCHEMA_PRAGMAS</b><br>
Defining this option omits pragmas for querying the database schema from 
the build. Currently, the 
<a href="pragma.html#pragma_table_info">table_info</a>,
<a href="pragma.html#pragma_index_info">index_info</a>,
<a href="pragma.html#pragma_index_list">index_list</a> and
<a href="pragma.html#pragma_database_list">database_list</a>
pragmas are omitted.
</p>

<p><b>SQLITE_OMIT_SCHEMA_VERSION_PRAGMAS</b><br>
Defining this option omits pragmas for querying and modifying the 
database schema version and user version from the build. Specifically, the 
<a href="pragma.html#pragma_schema_version">schema_version</a> and
<a href="pragma.html#pragma_user_version">user_version</a>
pragmas are omitted.

<p><b>SQLITE_OMIT_SUBQUERY</b><br>
<p>If defined, support for sub-selects and the IN() operator are omitted.
</p>

<p><b>SQLITE_OMIT_TCL_VARIABLE</b><br>
<p>If this macro is defined, then the special "$<variable-name>" syntax
used to automatically bind SQL variables to TCL variables is omitted.
</p>

<p><b>SQLITE_OMIT_TRIGGER</b><br>
Defining this option omits support for VIEW objects. Neither the 
<a href="lang_createtrigger.html">CREATE TRIGGER</a> or 
<a href="lang_droptrigger.html">DROP TRIGGER</a> 
commands are available in this case, attempting to execute either will result
in a parse error.
</p>
<p>
WARNING: If this macro is defined, it will not be possible to open a database
for which the schema contains TRIGGER objects. 
</p>

<p><b>SQLITE_OMIT_UTF16</b><br>
This macro is used to omit support for UTF16 text encoding. When this is
defined all API functions that return or accept UTF16 encoded text are
unavailable. These functions can be identified by the fact that they end
with '16', for example sqlite3_prepare16(), sqlite3_column_text16() and
sqlite3_bind_text16().
</p>

<p><b>SQLITE_OMIT_VACUUM</b><br>
When this option is defined, the <a href="lang_vacuum.html">VACUUM</a> 
command is not included in the library. Executing a VACUUM statement causes 
a parse error.
</p>

<p><b>SQLITE_OMIT_VIEW</b><br>
Defining this option omits support for VIEW objects. Neither the 
<a href="lang_createview.html">CREATE VIEW</a> or 
<a href="lang_dropview.html">DROP VIEW</a> 
commands are available in this case, attempting to execute either will result
in a parse error.
</p>
<p>
WARNING: If this macro is defined, it will not be possible to open a database
for which the schema contains VIEW objects. 
</p>
}
footer $rcsid

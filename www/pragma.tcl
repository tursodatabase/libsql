#
# Run this Tcl script to generate the pragma.html file.
#
set rcsid {$Id: pragma.tcl,v 1.2 2004/11/11 05:10:44 danielk1977 Exp $}
source common.tcl
header {Pragma statements supported by SQLite}

puts {
<p>The <a href="#syntax">PRAGMA command</a> is a special command used to 
modify the operation of the SQLite library or to query the library for 
internal (non-table) data. The PRAGMA command is issued using the same
interface as other SQLite commands (e.g. SELECT, INSERT) but is different
different in the following important respects:
</p>
<ul>
<li>Specific pragma statements may be removed and others added in future
    releases of SQLite. Use with caution!
<li>No error messages are generated if an unknown pragma is issued.
    Unknown pragmas are simply ignored. This means if there is a typo in 
    a pragma statement the library does not inform the user of the fact.
<li>Some pragmas take effect during the SQL compilation stage, not the
    execution stage. This means if using the C-language sqlite3_compile(), 
    sqlite3_step(), sqlite3_finalize() API (or similar in a wrapper 
    interface), the pragma may be applied to the library during the 
    sqlite3_compile() call.
<li>The pragma command is unlikely to be compatible with any other SQL
    engine.
</ul>

<p>The available pragmas fall into four basic categories:</p>
<ul>
<li>Pragmas used to <a href="#schema">query the schema</a> of the current 
    database.
<li>Pragmas used to <a href="#modify">modify the operation</a> of the 
    SQLite library in some manner, or to query for the current mode of 
    operation.
<li>Pragmas used to <a href="#cookie">query or modify the databases two 
    cookie values</a>, the schema-cookie and the user-cookie.
<li>Pragmas used to <a href="#debug">debug the library</a> and verify that
    database files are not corrupted.
</ul>
}

Section {PRAGMA command syntax} syntax

Syntax {sql-statement} {
PRAGMA <name> [= <value>] |
PRAGMA <function>(<arg>)
}

puts {
<p>The pragmas that take an integer <b><i>value</i></b> also accept 
symbolic names.  The strings "<b>on</b>", "<b>true</b>", and "<b>yes</b>" 
are equivalent to <b>1</b>.  The strings "<b>off</b>", "<b>false</b>", 
and "<b>no</b>" are equivalent to <b>0</b>.  These strings are case-
insensitive, and do not require quotes.  An unrecognized string will be 
treated as <b>1</b>, and will not generate an error.  When the <i>value</i> 
is returned it is as an integer.</p>
}

Section {Pragmas to modify library operation} modify

puts {
<ul>
<a name="pragma_auto_vacuum"></a>
<li><p><b>PRAGMA auto_vacuum;
       <br>PRAGMA auto_vacuum = </b><i>0 | 1</i><b>;</b></p>
    <p> Query or set the auto-vacuum flag in the database.</p>

    <p>Normally, when a transaction that deletes data from a database is
    committed, the database file remains the same size. Unused database file 
    pages are marked as such and reused later on, when data is inserted into 
    the database. In this mode the <a href="lang.html#vacuum">VACUUM</a>
    command is used to reclaim unused space.</p>

    <p>When the auto-vacuum flag is set, the database file shrinks when a
    transaction that deletes data is committed (The VACUUM command is not
    useful in a database with the auto-vacuum flag set). To support this
    functionality the database stores extra information internally, resulting
    in slightly larger database files than would otherwise be possible.</p>

    <p>It is only possible to modify the value of the auto-vacuum flag before
    any tables have been created in the database. No error message is 
    returned if an attempt to modify the auto-vacuum flag is made after
    one or more tables have been created.
    </p></li>

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
</ul>
}

Section {Pragmas to query the database schema} schema

puts {
<ul>
<li><p><b>PRAGMA database_list;</b></p>
    <p>For each open database, invoke the callback function once with
    information about that database.  Arguments include the index and 
    the name the database was attached with.  The first row will be for 
    the main database.  The second row will be for the database used to 
    store temporary tables.</p></li>

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

<li><p><b>PRAGMA table_info(</b><i>table-name</i><b>);</b></p>
    <p>For each column in the named table, invoke the callback function
    once with information about that column, including the column name,
    data type, whether or not the column can be NULL, and the default
    value for the column.</p></li>
</ul>
}

Section {Pragmas to query/modify cookie values} cookie

puts {

<ul>
<li><p><b>PRAGMA [database.]schema_cookie; 
       <br>PRAGMA [database.]schema_cookie = </b><i>integer </i><b>;
       <br>PRAGMA [database.]user_cookie;
       <br>PRAGMA [database.]user_cookie = </b><i>integer </i><b>;</b>

  
<p>    The pragmas schema_cookie and user_cookie are used to set or get
       the value of the schema-cookie and user-cookie, respectively. Both
       the schema-cookie and the user-cookie are 32-bit signed integers
       stored in the database header.</p>
  
<p>    The schema-cookie is usually only manipulated internally by SQLite.  
       It is incremented by SQLite whenever the database schema is modified 
       (by creating or dropping a table or index). The schema cookie is 
       used by SQLite each time a query is executed to ensure that the 
       internal cache of the schema used when compiling the SQL query matches 
       the schema of the database against which the compiled query is actually 
       executed.  Subverting this mechanism by using "PRAGMA schema_cookie" 
       to modify the schema-cookie is potentially dangerous and may lead 
       to program crashes or database corruption. Use with caution!</p>
  
<p>    The user-cookie is not used internally by SQLite. It may be used by
       applications for any purpose.</p>
</li>
</ul>
}

Section {Pragmas to debug the library} debug

puts {
<ul>
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

<a name="pragma_vdbe_trace"></a>
<li><p><b>PRAGMA vdbe_trace = ON; </b>(1)<b>
    <br>PRAGMA vdbe_trace = OFF;</b> (0)</p>
    <p>Turn tracing of the virtual database engine inside of the
    SQLite library on and off.  This is used for debugging.  See the 
    <a href="vdbe.html#trace">VDBE documentation</a> for more 
    information.</p></li>
</ul>

}


#
# Run this Tcl script to generate the pragma.html file.
#
set rcsid {$Id: pragma.tcl,v 1.20 2007/02/02 12:33:17 drh Exp $}
source common.tcl
header {Pragma statements supported by SQLite}

proc Section {name {label {}}} {
  puts "\n<hr />"
  if {$label!=""} {
    puts "<a name=\"$label\"></a>"
  }
  puts "<h1>$name</h1>\n"
}

puts {
<p>The <a href="#syntax">PRAGMA command</a> is a special command used to 
modify the operation of the SQLite library or to query the library for 
internal (non-table) data. The PRAGMA command is issued using the same
interface as other SQLite commands (e.g. SELECT, INSERT) but is
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
<li>Pragmas used to <a href="#version">query or modify the databases two 
    version values</a>, the schema-version and the user-version.
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
    the database. In this mode the <a href="lang_vacuum.html">VACUUM</a>
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

<a name="pragma_case_sensitive_like"></a>
<li><p><b>PRAGMA case_sensitive_like;
       <br>PRAGMA case_sensitive_like = </b><i>0 | 1</i><b>;</b></p>
    <p>The default behavior of the LIKE operator is to ignore case
    for latin1 characters. Hence, by default <b>'a' LIKE 'A'</b> is
    true.  The case_sensitive_like pragma can be turned on to change
    this behavior.  When case_sensitive_like is enabled,
    <b>'a' LIKE 'A'</b> is false but <b>'a' LIKE 'a'</b> is still true.</p>
    </li>

<a name="pragma_count_changes"></a>
<li><p><b>PRAGMA count_changes;
       <br>PRAGMA count_changes = </b><i>0 | 1</i><b>;</b></p>
    <p>Query or change the count-changes flag. Normally, when the
    count-changes flag is not set, INSERT, UPDATE and DELETE statements
    return no data. When count-changes is set, each of these commands 
    returns a single row of data consisting of one integer value - the
    number of rows inserted, modified or deleted by the command. The 
    returned change count does not include any insertions, modifications
    or deletions performed by triggers.</p>

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
<li><p><b>PRAGMA default_synchronous;</b></p>
    <p>This pragma was available in version 2.8 but was removed in version
    3.0.  It is a dangerous pragma whose use is discouraged.  To help
    dissuide users of version 2.8 from employing this pragma, the documentation
    will not tell you what it does.</p></li>


<a name="pragma_empty_result_callbacks"></a>
<li><p><b>PRAGMA empty_result_callbacks;
       <br>PRAGMA empty_result_callbacks = </b><i>0 | 1</i><b>;</b></p>
    <p>Query or change the empty-result-callbacks flag.</p> 
    <p>The empty-result-callbacks flag affects the sqlite3_exec API only.
    Normally, when the empty-result-callbacks flag is cleared, the
    callback function supplied to the sqlite3_exec() call is not invoked
    for commands that return zero rows of data. When empty-result-callbacks
    is set in this situation, the callback function is invoked exactly once,
    with the third parameter set to 0 (NULL). This is to enable programs  
    that use the sqlite3_exec() API to retrieve column-names even when
    a query returns no data.
    </p>

<a name="pragma_encoding"></a>
<li><p><b>PRAGMA encoding;
       <br>PRAGMA encoding = "UTF-8";
       <br>PRAGMA encoding = "UTF-16";
       <br>PRAGMA encoding = "UTF-16le";
       <br>PRAGMA encoding = "UTF-16be";</b></p>
    <p>In first form, if the main database has already been
    created, then this pragma returns the text encoding used by the
    main database, one of "UTF-8", "UTF-16le" (little-endian UTF-16
    encoding) or "UTF-16be" (big-endian UTF-16 encoding).  If the main
    database has not already been created, then the value returned is the
    text encoding that will be used to create the main database, if 
    it is created by this session.</p>
    <p>The second and subsequent forms of this pragma are only useful if
    the main database has not already been created. In this case the 
    pragma sets the encoding that the main database will be created with if
    it is created by this session. The string "UTF-16" is interpreted
    as "UTF-16 encoding using native machine byte-ordering".  If the second
    and subsequent forms are used after the database file has already
    been created, they have no effect and are silently ignored.</p>

    <p>Once an encoding has been set for a database, it cannot be changed.</p>

    <p>Databases created by the ATTACH command always use the same encoding
    as the main database.</p>
</li>

<a name="pragma_full_column_names"></a>
<li><p><b>PRAGMA full_column_names;
       <br>PRAGMA full_column_names = </b><i>0 | 1</i><b>;</b></p>
    <p>Query or change the full-column-names flag. This flag affects
    the way SQLite names columns of data returned by SELECT statements
    when the expression for the column is a table-column name or the
    wildcard "*".  Normally, such result columns are named
    &lt;table-name/alias&gt;&lt;column-name&gt; if the SELECT statement joins 
    two or
    more tables together, or simply &lt;column-name&gt; if the SELECT
    statement queries a single table. When the full-column-names flag
    is set, such columns are always named &lt;table-name/alias&gt;
    &lt;column-name&gt; regardless of whether or not a join is performed.
    </p>
    <p>If both the short-column-names and full-column-names are set,
    then the behaviour associated with the full-column-names flag is
    exhibited.
    </p>
</li>

<a name="pragma_fullfsync"></a>
<li><p><b>PRAGMA fullfsync
       <br>PRAGMA fullfsync = </b><i>0 | 1</i><b>;</b></p>
    <p>Query or change the fullfsync flag. This flag affects
    determines whether or not the F_FULLFSYNC syncing method is used
    on systems that support it.  The default value is off.  As of this
    writing (2006-02-10) only Mac OS X supports F_FULLFSYNC.
    </p>
</li>

<a name="pragma_legacy_file_format"></a>
<li><p><b>PRAGMA legacy_file_format;
       <br>PRAGMA legacy_file_format = <i>ON | OFF</i></b></p>
    <p>This pragma sets or queries the value of the legacy_file_format
    flag.  When this flag is on, new SQLite databases are created in
    a file format that is readable and writable by all versions of
    SQLite going back to 3.0.0.  When the flag is off, new databases
    are created using the latest file format which might not be
    readable or writable by older versions of SQLite.</p>

    <p>This flag only affects newly created databases.  It has no
    effect on databases that already exist.</p>
</li>


<a name="pragma_page_size"></a>
<li><p><b>PRAGMA page_size;
       <br>PRAGMA page_size = </b><i>bytes</i><b>;</b></p>
    <p>Query or set the page-size of the database. The page-size
    may only be set if the database has not yet been created. The page
    size must be a power of two greater than or equal to 512 and less
    than or equal to 8192. The upper limit may be modified by setting
    the value of macro SQLITE_MAX_PAGE_SIZE during compilation.  The
    maximum upper bound is 32768.
    </p>
</li>

<a name="pragma_read_uncommitted"></a>
<li><p><b>PRAGMA read_uncommitted;
       <br>PRAGMA read_uncommitted = </b><i>0 | 1</i><b>;</b></p>
    <p>Query, set, or clear READ UNCOMMITTED isolation.  The default isolation
    level for SQLite is SERIALIZABLE.  Any process or thread can select
    READ UNCOMMITTED isolation, but SERIALIZABLE will still be used except
    between connections that share a common page and schema cache.
    Cache sharing is enabled using the
    <a href="capi3ref.html#sqlite3_enable_shared_cache">
    sqlite3_enable_shared_cache()</a> API and is only available between
    connections running the same thread.  Cache sharing is off by default.
    </p>
</li>

<a name="pragma_short_column_names"></a>
<li><p><b>PRAGMA short_column_names;
       <br>PRAGMA short_column_names = </b><i>0 | 1</i><b>;</b></p>
    <p>Query or change the short-column-names flag. This flag affects
    the way SQLite names columns of data returned by SELECT statements
    when the expression for the column is a table-column name or the
    wildcard "*".  Normally, such result columns are named
    &lt;table-name/alias&gt;lt;column-name&gt; if the SELECT statement 
    joins two or more tables together, or simply &lt;column-name&gt; if 
    the SELECT statement queries a single table. When the short-column-names 
    flag is set, such columns are always named &lt;column-name&gt; 
    regardless of whether or not a join is performed.
    </p>
    <p>If both the short-column-names and full-column-names are set,
    then the behaviour associated with the full-column-names flag is
    exhibited.
    </p>
</li>

<a name="pragma_synchronous"></a>
<li><p><b>PRAGMA synchronous;
       <br>PRAGMA synchronous = FULL; </b>(2)<b>
       <br>PRAGMA synchronous = NORMAL; </b>(1)<b>
       <br>PRAGMA synchronous = OFF; </b>(0)</p>
    <p>Query or change the setting of the "synchronous" flag.  
    The first (query) form will return the setting as an 
    integer.  When synchronous is FULL (2), the SQLite database engine will
    pause at critical moments to make sure that data has actually been 
    written to the disk surface before continuing.  This ensures that if
    the operating system crashes or if there is a power failure, the database
    will be uncorrupted after rebooting.  FULL synchronous is very 
    safe, but it is also slow.  
    When synchronous is NORMAL, the SQLite database
    engine will still pause at the most critical moments, but less often
    than in FULL mode.  There is a very small (though non-zero) chance that
    a power failure at just the wrong time could corrupt the database in
    NORMAL mode.  But in practice, you are more likely to suffer
    a catastrophic disk failure or some other unrecoverable hardware
    fault.
    With synchronous OFF (0), SQLite continues without pausing
    as soon as it has handed data off to the operating system.
    If the application running SQLite crashes, the data will be safe, but
    the database might become corrupted if the operating system
    crashes or the computer loses power before that data has been written
    to the disk surface.  On the other hand, some
    operations are as much as 50 or more times faster with synchronous OFF.
    </p>
    <p>In SQLite version 2, the default value is NORMAL. For version 3, the
    default was changed to FULL.
    </p>
</li>


<a name="pragma_temp_store"></a>
<li><p><b>PRAGMA temp_store;
       <br>PRAGMA temp_store = DEFAULT;</b> (0)<b>
       <br>PRAGMA temp_store = FILE;</b> (1)<b>
       <br>PRAGMA temp_store = MEMORY;</b> (2)</p>
    <p>Query or change the setting of the "<b>temp_store</b>" parameter.
    When temp_store is DEFAULT (0), the compile-time C preprocessor macro
    TEMP_STORE is used to determine where temporary tables and indices
    are stored.  When
    temp_store is MEMORY (2) temporary tables and indices are kept in memory.
    When temp_store is FILE (1) temporary tables and indices are stored
    in a file.  The <a href="#pragma_temp_store_directory">
    temp_store_directory</a> pragma can be used to specify the directory
    containing this file.
    <b>FILE</b> is specified. When the temp_store setting is changed,
    all existing temporary tables, indices, triggers, and views are
    immediately deleted.</p>

    <p>It is possible for the library compile-time C preprocessor symbol
    TEMP_STORE to override this pragma setting.  The following table summarizes
    the interaction of the TEMP_STORE preprocessor macro and the
    temp_store pragma:</p>

    <blockquote>
    <table cellpadding="2" border="1">
    <tr><th valign="bottom">TEMP_STORE</th>
        <th valign="bottom">PRAGMA<br>temp_store</th>
        <th>Storage used for<br>TEMP tables and indices</th></tr>
    <tr><td align="center">0</td>
        <td align="center"><em>any</em></td>
        <td align="center">file</td></tr>
    <tr><td align="center">1</td>
        <td align="center">0</td>
        <td align="center">file</td></tr>
    <tr><td align="center">1</td>
        <td align="center">1</td>
        <td align="center">file</td></tr>
    <tr><td align="center">1</td>
        <td align="center">2</td>
        <td align="center">memory</td></tr>
    <tr><td align="center">2</td>
        <td align="center">0</td>
        <td align="center">memory</td></tr>
    <tr><td align="center">2</td>
        <td align="center">1</td>
        <td align="center">file</td></tr>
    <tr><td align="center">2</td>
        <td align="center">2</td>
        <td align="center">memory</td></tr>
    <tr><td align="center">3</td>
        <td align="center"><em>any</em></td>
        <td align="center">memory</td></tr>
    </table>
    </blockquote>
    </li>
    <br>

<a name="pragma_temp_store_directory"></a>
<li><p><b>PRAGMA temp_store_directory;
       <br>PRAGMA temp_store_directory = 'directory-name';</b></p>
    <p>Query or change the setting of the "temp_store_directory" - the
    directory where files used for storing temporary tables and indices
    are kept.  This setting lasts for the duration of the current connection
    only and resets to its default value for each new connection opened.

    <p>When the temp_store_directory setting is changed, all existing temporary
    tables, indices, triggers, and viewers are immediately deleted.  In
    practice, temp_store_directory should be set immediately after the 
    database is opened.  </p>

    <p>The value <i>directory-name</i> should be enclosed in single quotes.
    To revert the directory to the default, set the <i>directory-name</i> to
    an empty string, e.g., <i>PRAGMA temp_store_directory = ''</i>.  An
    error is raised if <i>directory-name</i> is not found or is not
    writable. </p>

    <p>The default directory for temporary files depends on the OS.  For
    Unix/Linux/OSX, the default is the is the first writable directory found
    in the list of: <b>/var/tmp, /usr/tmp, /tmp,</b> and <b>
    <i>current-directory</i></b>.  For Windows NT, the default 
    directory is determined by Windows, generally
    <b>C:\Documents and Settings\<i>user-name</i>\Local Settings\Temp\</b>. 
    Temporary files created by SQLite are unlinked immediately after
    opening, so that the operating system can automatically delete the
    files when the SQLite process exits.  Thus, temporary files are not
    normally visible through <i>ls</i> or <i>dir</i> commands.</p>
 
    </li>
</ul>
}

Section {Pragmas to query the database schema} schema

puts {
<ul>
<a name="pragma_database_list"></a>
<li><p><b>PRAGMA database_list;</b></p>
    <p>For each open database, invoke the callback function once with
    information about that database.  Arguments include the index and 
    the name the database was attached with.  The first row will be for 
    the main database.  The second row will be for the database used to 
    store temporary tables.</p></li>

<a name="pragma_foreign_key_list"></a>
<li><p><b>PRAGMA foreign_key_list(</b><i>table-name</i><b>);</b></p>
    <p>For each foreign key that references a column in the argument
    table, invoke the callback function with information about that
    foreign key. The callback function will be invoked once for each
    column in each foreign key.</p></li>

<a name="pragma_index_info"></a>
<li><p><b>PRAGMA index_info(</b><i>index-name</i><b>);</b></p>
    <p>For each column that the named index references, invoke the 
    callback function
    once with information about that column, including the column name,
    and the column number.</p></li>

<a name="pragma_index_list"></a>
<li><p><b>PRAGMA index_list(</b><i>table-name</i><b>);</b></p>
    <p>For each index on the named table, invoke the callback function
    once with information about that index.  Arguments include the
    index name and a flag to indicate whether or not the index must be
    unique.</p></li>

<a name="pragma_table_info"></a>
<li><p><b>PRAGMA table_info(</b><i>table-name</i><b>);</b></p>
    <p>For each column in the named table, invoke the callback function
    once with information about that column, including the column name,
    data type, whether or not the column can be NULL, and the default
    value for the column.</p></li>
</ul>
}

Section {Pragmas to query/modify version values} version

puts {

<ul>
<a name="pragma_schema_version"></a>
<a name="pragma_user_version"></a>
<li><p><b>PRAGMA [database.]schema_version; 
       <br>PRAGMA [database.]schema_version = </b><i>integer </i><b>;
       <br>PRAGMA [database.]user_version;
       <br>PRAGMA [database.]user_version = </b><i>integer </i><b>;</b>

  
<p>    The pragmas schema_version and user_version are used to set or get
       the value of the schema-version and user-version, respectively. Both
       the schema-version and the user-version are 32-bit signed integers
       stored in the database header.</p>
  
<p>    The schema-version is usually only manipulated internally by SQLite.  
       It is incremented by SQLite whenever the database schema is modified 
       (by creating or dropping a table or index). The schema version is 
       used by SQLite each time a query is executed to ensure that the 
       internal cache of the schema used when compiling the SQL query matches 
       the schema of the database against which the compiled query is actually 
       executed.  Subverting this mechanism by using "PRAGMA schema_version" 
       to modify the schema-version is potentially dangerous and may lead 
       to program crashes or database corruption. Use with caution!</p>
  
<p>    The user-version is not used internally by SQLite. It may be used by
       applications for any purpose.</p>
</li>
</ul>
}

Section {Pragmas to debug the library} debug

puts {
<ul>
<a name="pragma_integrity_check"></a>
<li><p><b>PRAGMA integrity_check;
    <br>PRAGMA integrity_check(</b><i>integer</i><b>)</b></p>
    <p>The command does an integrity check of the entire database.  It
    looks for out-of-order records, missing pages, malformed records, and
    corrupt indices.
    If any problems are found, then strings are returned (as multiple
    rows with a single column per row) which describe
    the problems.  At most <i>integer</i> errors will be reported
    before the analysis quits.  The default value for <i>integer</i>
    is 100.  If no errors are found, a single row with the value "ok" is
    returned.</p></li>

<a name="pragma_parser_trace"></a>
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

<a name="pragma_vdbe_listing"></a>
<li><p><b>PRAGMA vdbe_listing = ON; </b>(1)<b>
    <br>PRAGMA vdbe_listing = OFF;</b> (0)</p>
    <p>Turn listings of virtual machine programs on and off.
    With listing is on, the entire content of a program is printed
    just prior to beginning execution.  This is like automatically
    executing an EXPLAIN prior to each statement.  The statement
    executes normally after the listing is printed.
    This is used for debugging.  See the 
    <a href="vdbe.html#trace">VDBE documentation</a> for more 
    information.</p></li>
</ul>

}

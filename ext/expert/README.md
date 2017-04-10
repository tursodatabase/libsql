## SQLite Expert Extension

This folder contains code for a simple system to propose useful indexes
given a database and a set of SQL queries. It works as follows:

  1. The user database schema is copied to a temporary database.

  1. All SQL queries are prepared against the temporary database. The
     **sqlite3\_whereinfo\_hook()** API is used to record information regarding
     the WHERE and ORDER BY clauses attached to each query.

  1. The information gathered in step 2 is used to create (possibly a large
     number of) candidate indexes.

  1. The SQL queries are prepared a second time. If the planner uses any
     of the indexes created in step 3, they are recommended to the user.

No ANALYZE data is available to the planner in step 4 above. This can lead to sub-optimal results.

This extension requires that SQLite be built with the 
SQLITE\_ENABLE\_WHEREINFO\_HOOK pre-processor symbol defined.

# C API

The SQLite expert C API is defined in sqlite3expert.h. Most uses will proceed
as follows:

  1. An sqlite3expert object is created by calling **sqlite3\_expert\_new()**.
     A database handle opened by the user is passed as an argument.

  1. The sqlite3expert object is configured with one or more SQL statements
     by making one or more calls to **sqlite3\_expert\_sql()**. Each call may
     specify a single SQL statement, or multiple statements separated by
     semi-colons.

  1. **sqlite3\_expert\_analyze()** is called to run the analysis.

  1. One or more calls are made to **sqlite3\_expert\_report()** to extract
     components of the results of the analysis.

  1. **sqlite3\_expert\_destroy()** is called to free all resources.

Refer to comments in sqlite3expert.h for further details.

# sqlite3_expert application

The file "expert.c" contains the code for a command line application that
uses the API described above. It can be compiled with (for example):

<pre>
  gcc -O2 -DSQLITE_ENABLE_WHEREINFO_HOOK sqlite3.c expert.c sqlite3expert.c -o sqlite3_expert
</pre>

Assuming the database is "test.db", it can then be run to analyze a single query:

<pre>
  ./sqlite3_expert -sql &lt;sql-query&gt; test.db
</pre>

Or an entire text file worth of queries with:

<pre>
  ./sqlite3_expert -file &lt;text-file&gt; test.db
</pre>





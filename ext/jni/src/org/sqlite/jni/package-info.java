/**
   This package houses a JNI binding to the SQLite3 C API.

   <p>The docs are in progress.

   <p>The primary interfaces are in {@link org.sqlite.jni.SQLite3Jni}.

   <h1>State of this API</h1>

   <p>As of version 3.43, this software is in "tech preview" form. We
   tentatively plan to stamp it as stable with the 3.44 release.

   <h1>Threading Considerations</h1>

   <p>This API is, if built with SQLITE_THREADSAFE set to 1 or 2,
   thread-safe, insofar as the C API guarantees, with some addenda:

   <ul>

     <li>It is not legal to use Java-facing SQLite3 resource handles
     (sqlite3, sqlite3_stmt, etc) from multiple threads concurrently,
     nor to use any database-specific resources concurrently in a
     thread separate from the one the database is currently in use
     in. i.e. do not use a sqlite3_stmt in thread #2 when thread #1 is
     using the database which prepared that handle.

     <br>Violating this will eventually corrupt the JNI-level bindings
     between Java's and C's view of the database. This is a limitation
     of the JNI bindings, not the lower-level library.
     </li>

     <li>It is legal to use a given handle, and database-specific
     resources, across threads, so long as no two threads pass
     resources owned by the same database into the library
     concurrently.
     </li>

   </ul>

   <p>Any number of threads may, of course, create and use any number
   of database handles they wish. Care only needs to be taken when
   those handles or their associated resources cross threads, or...

   <p>When built with SQLITE_THREADSAFE=0 then no threading guarantees
   are provided and multi-threaded use of the library will provoke
   undefined behavior.

*/
package org.sqlite.jni;

/**
   This package houses a JNI binding to the SQLite3 C API.

   <p>The primary interfaces are in {@link
   org.sqlite.jni.capi.CApi}.</p>

   <h1>API Goals and Requirements</h1>

   <ul>

     <li>A 1-to-1(-ish) mapping of the C API to Java via JNI, insofar
     as cross-language semantics allow for. A closely-related goal is
     that <a href='https://sqlite.org/c3ref/intro.html'>the C
     documentation</a> should be usable as-is, insofar as possible,
     for most of the JNI binding. As a rule, undocumented symbols in
     the Java interface behave as documented for their C API
     counterpart. Only semantic differences and Java-specific features
     are documented here.</li>

     <li>Support Java as far back as version 8 (2014).</li>

     <li>Environment-independent. Should work everywhere both Java and
     SQLite3 do.</li>

     <li>No 3rd-party dependencies beyond the JDK. That includes no
     build-level dependencies for specific IDEs and toolchains.  We
     welcome the addition of build files for arbitrary environments
     insofar as they neither interfere with each other nor become a
     maintenance burden for the sqlite developers.</li>

  </ul>

  <h2>Non-Goals</h2>

  <ul>

    <li>Creation of high-level OO wrapper APIs. Clients are free to
    create them off of the C-style API.</li>

    <li>Support for mixed-mode operation, where client code accesses
    SQLite both via the Java-side API and the C API via their own
    native code. In such cases, proxy functionalities (primarily
    callback handler wrappers of all sorts) may fail because the
    C-side use of the SQLite APIs will bypass those proxies.</li>

  </ul>

   <h1>State of this API</h1>

   <p>As of version 3.43, this software is in "tech preview" form. We
   tentatively plan to stamp it as stable with the 3.44 release.</p>

   <h1>Threading Considerations</h1>

   <p>This API is, if built with SQLITE_THREADSAFE set to 1 or 2,
   thread-safe, insofar as the C API guarantees, with some addenda:</p>

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
   those handles or their associated resources cross threads, or...</p>

   <p>When built with SQLITE_THREADSAFE=0 then no threading guarantees
   are provided and multi-threaded use of the library will provoke
   undefined behavior.</p>

*/
package org.sqlite.jni.capi;

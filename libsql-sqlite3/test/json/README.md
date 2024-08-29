The files in this subdirectory are used to help measure the performance
of the SQLite JSON functions, especially in relation to handling large
JSON inputs.

# 1.0 Prerequisites

  *   Standard SQLite build environment (SQLite source tree, compiler, make, etc.)

  *   Valgrind

  *   Fossil (only the "fossil xdiff" command is used by this procedure)

  *   tclsh

# 2.0 Setup

  *   Run: "`tclsh json-generator.tcl | sqlite3 json100mb.db`" to create
      the 100 megabyte test database.  Do this so that the "json100mb.db"
      file lands in the directory from which you will run tests, not in
      the test/json subdirectory of the source tree.

  *   Make a copy of "json100mb.db" into "jsonb100mb.db" - change the prefix
      from "json" to "jsonb".

  *   Bring up jsonb100mb.db in the sqlite3 command-line shell.
      Convert all of the content into JSONB using a commands like this:

>        UPDATE data1 SET x=jsonb(x);
>        VACUUM;

  *   Build the baseline sqlite3.c file with sqlite3.h and shell.c.

>        make clean sqlite3.c

  *   Run "`sh json-speed-check.sh trunk`".   This creates the baseline
      profile in "jout-trunk.txt" for the preformance test using text JSON.

  *   Run "`sh json-speed-check.sh trunk --jsonb`".  This creates the
      baseline profile in "joutb-trunk.txt" for the performance test
      for processing JSONB

  *   (Optional) Verify that the json100mb.db database really does contain
      approximately 100MB of JSON content by running:

>        SELECT sum(length(x)) FROM data1;
>        SELECT * FROM data1 WHERE NOT json_valid(x);

# 3.0 Testing

  *   Build the sqlite3.c (with sqlite3.h and shell.c) to be tested.

  *   Run "`sh json-speed-check.sh x1`".  The profile output will appear
      in jout-x1.txt.  Substitute any label you want in place of "x1".

  *   Run "`sh json-speed-check.sh x1 --jsonb`".  The profile output will appear
      in joutb-x1.txt.  Substitute any label you want in place of "x1".

  *   Run the script shown below in the CLI.
      Divide 2500 by the real elapse time from this test
      to get an estimate for number of MB/s that the JSON parser is
      able to process.

>        .open json100mb.db
>        .timer on
>        WITH RECURSIVE c(n) AS (VALUES(1) UNION ALL SELECT n+1 FROM c WHERE n<25)
>        SELECT sum(json_valid(x)) FROM c, data1;

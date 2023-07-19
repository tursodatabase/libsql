The files in this subdirectory are used to help measure the performance
of the SQLite JSON parser.

# 1.0 Prerequisites

  1.  Valgrind

  2.  Fossil

# 2.0 Setup

  1.  Run: "`tclsh json-generator.tcl | sqlite3 json100mb.db`" to create
      the 100 megabyte test database.  Do this so that the "json100mb.db"
      file lands in the directory from which you will run tests, not in
      the test/json subdirectory of the source tree.

  2.  Build the baseline sqlite3.c file.  ("`make sqlite3.c`")

  3.  Run "`sh json-speed-check-1.sh trunk`".   This creates the baseline
      profile in "jout-trunk.txt".

# 3.0 Testing

  1.  Build the sqlite3.c to be tested.

  2.  Run "`sh json-speed-check-1.sh x1`".  The profile output will appear
      in jout-x1.txt.  Substitute any label you want in place of "x1".

  3.  Run the query "`SELECT sum(json_valid(x)) FROM data1;`" on the
      json100mb.db database file.  Measure the
      runtime.  Divide 100 by the real elapse time of this test
      to get an estimate for number of MB/s that the JSON parser is
      able to process.

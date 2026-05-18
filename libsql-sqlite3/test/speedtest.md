# Performance And Size Measurements

This document shows a procedure for making performance and size
comparisons between two versions of the SQLite Amalgamation "sqlite3.c".
You will need:

  *  fossil
  *  valgrind
  *  tclsh
  *  A script or program named "open" that brings up *.txt files in an
     editor for viewing.  (Macs provide this by default.  You'll need to
     come up with your own on Linux and Windows.)
  *  An SQLite source tree

The procedure described in this document is not the only way to make
performance and size measurements.  Use this as a guide and make
adjustments as needed.

## Establish the baseline measurement

  *  Begin at the root the SQLite source tree
  *  <b>mkdir -p ../speed</b> <br>
      &uarr;  Speed measurement output files will go into this directory.
     You can actually put those files wherever you want.  This is just a
     suggestion.  It might be good to keep these files outside of the
     source tree so that "fossil clean" does not delete them.
  *  Obtain the baseline SQLite amalgamation.  For the purpose of this
     technical note, assume the baseline SQLite sources are in files
     "../baseline/sqlite3.c" and "../baseline/sqlite3.h".
  *  <b>test/speedtest.tcl ../baseline/sqlite3.c ../speed/baseline.txt</b> <br>
     &uarr; The performance measure will be written into ../speed/baseline.txt
     and that file will be brought up in an editor for easy viewing. <br>
     &uarr; The "sqlite3.h" will be taken from the directory that contains
     the "sqlite3.c" amalgamation file.

## Comparing the current checkout against the baseline

  *  <b>make sqlite3.c</b>
  *  <b>test/speedtest.tcl sqlite3.c ../speed/test.txt ../speed/baseline.txt</b> <br>
     &uarr; Test results written into ../speed/test.txt and then
     "fossil xdiff" is run to compare ../speed/baseline.txt against
     the new test results.

## When to do this

Performance and size checks should be done prior to trunk check-ins.
Sometimes a seemingly innocuous change can have large performance
impacts.  A large impact does not mean that the change cannot continue,
but it is important to be aware of the impact.

## Additional hints

Use the --help option to test/speedtest.tcl to see other available options.

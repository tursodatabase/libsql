# 2021 February 15
#
# The author disclaims copyright to this source code.  In place of
# a legal notice, here is a blessing:
#
#    May you do good and not evil.
#    May you find forgiveness for yourself and forgive others.
#    May you share freely, never taking more than you give.
#
#***********************************************************************
# This file implements regression tests for SQLite library.  The
# focus of this file is testing optimizations associated with "IS NULL"
# and "IS NOT NULL" operators on columns with NOT NULL constraints.
#

set testdir [file dirname $argv0]
source $testdir/tester.tcl
set testprefix notnullfault

do_execsql_test 1.0 {
  CREATE TABLE t1(a, b);
  CREATE TABLE t2(c, d NOT NULL);
}
faultsim_save_and_close

do_faultsim_test 1 -prep {
  faultsim_restore_and_reopen
} -body {
  execsql {
    SELECT * FROM t2 WHERE d NOT NULL
  }
} -test {
  faultsim_test_result {0 {}}
}

#-------------------------------------------------------------------------
reset_db
do_execsql_test 2.0 {
  CREATE TABLE t1(a, b, c); 
  CREATE TABLE t2(a, b, c, PRIMARY KEY(a, b, c)) WITHOUT ROWID;
}
faultsim_save_and_close

do_faultsim_test 2.1 -faults oom-t* -prep {
  faultsim_restore_and_reopen
} -body {
  execsql {
    SELECT dense_rank() OVER win FROM t2
    WINDOW win AS (ORDER BY c IS NULL)
  }
} -test {
  faultsim_test_result {0 {}}
}

finish_test

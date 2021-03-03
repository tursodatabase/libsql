# 2020-12-16
#
# The author disclaims copyright to this source code.  In place of
# a legal notice, here is a blessing:
#
#    May you do good and not evil.
#    May you find forgiveness for yourself and forgive others.
#    May you share freely, never taking more than you give.
#
#***********************************************************************
#

set testdir [file dirname $argv0]
source $testdir/tester.tcl
set testprefix unionallfault

do_execsql_test 1.0 {
  CREATE TABLE t1(x,y,z);
  CREATE TABLE t3(x,y,z);
}
faultsim_save_and_close


do_faultsim_test 1 -faults oom-t* -prep {
  faultsim_restore_and_reopen
} -body {
  execsql {
    SELECT * FROM t1, (
      SELECT x FROM t1 UNION ALL SELECT y FROM t1
    ), t3
  }
} -test {
  faultsim_test_result {0 {}}
}

finish_test

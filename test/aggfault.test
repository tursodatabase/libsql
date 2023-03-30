# 2023 March 30
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
set testprefix aggfault


do_execsql_test 1 {
  CREATE TABLE t1(x);
  CREATE INDEX t1x ON t1(x, x=0);
}
faultsim_save_and_close

do_faultsim_test 2 -faults oom* -prep {
  faultsim_restore_and_reopen
  execsql { SELECT * FROM sqlite_schema }
} -body {
  execsql {
    SELECT * FROM t1 AS a1 WHERE (
      SELECT count(x AND 0=a1.x) FROM t1 GROUP BY abs(1)
    ) AND x=(
      SELECT * FROM t1 AS a1 
      WHERE (SELECT count(x IS 1 AND a1.x=0) 
      FROM t1 
      GROUP BY abs(1)) AND x=0
    );
  }
} -test {
  faultsim_test_result {0 {}}
}


finish_test

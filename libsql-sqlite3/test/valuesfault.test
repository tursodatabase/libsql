# 2024 March 3
#
# The author disclaims copyright to this source code.  In place of
# a legal notice, here is a blessing:
#
#    May you do good and not evil.
#    May you find forgiveness for yourself and forgive others.
#    May you share freely, never taking more than you give.
#
#***********************************************************************
# This file implements regression tests for SQLite library.
#

set testdir [file dirname $argv0]
source $testdir/tester.tcl
set testprefix valuesfault
source $testdir/malloc_common.tcl


do_execsql_test 1.0 {
  CREATE TABLE x1(a, b, c);
}
faultsim_save_and_close

do_faultsim_test 1 -prep {
  faultsim_restore_and_reopen
  sqlite3_limit db SQLITE_LIMIT_COMPOUND_SELECT 2
} -body {
  execsql {
    INSERT INTO x1 VALUES(1, 1, 1), (2, 2, 2), (3, 3, 3), (4, 4, 4);
  }
} -test {
  faultsim_test_result {0 {}} 
}


finish_test

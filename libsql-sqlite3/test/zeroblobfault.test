# 2021 November 8
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
#

set testdir [file dirname $argv0]
source $testdir/tester.tcl

set testprefix zeroblobfault
set quoted_res [db one { SELECT quote(zeroblob(2000)) }]

do_faultsim_test 1 -prep {
  sqlite3 db test.db
} -body {
  execsql { SELECT quote(zeroblob(2000)) }
} -test {
  faultsim_test_result [list 0 $::quoted_res]
}

finish_test

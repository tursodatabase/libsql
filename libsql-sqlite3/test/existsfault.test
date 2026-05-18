# 2024 May 25
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
source $testdir/lock_common.tcl
source $testdir/malloc_common.tcl
set testprefix existsfault

db close
sqlite3_shutdown
sqlite3_config_lookaside 0 0
sqlite3_initialize
autoinstall_test_functions
sqlite3 db test.db

do_execsql_test 1.0 {
  CREATE TABLE x1(a, b);
  INSERT INTO x1 VALUES(1, 2), (3, 4), (5, 6);
  CREATE UNIQUE INDEX x1a ON x1(a);
  CREATE INDEX x1b ON x1(b);

  CREATE TABLE x2(x, y);
  INSERT INTO x2 VALUES(1, 2), (3, 4), (5, 6);
}

do_faultsim_test 1 -faults oom* -prep {
  sqlite3 db test.db
  execsql { SELECT * FROM sqlite_schema }
} -body {
  execsql {
    SELECT count(*) FROM x2 WHERE EXISTS (SELECT 1 FROM x1 WHERE a=x) AND y!=11
  }
} -test {
  faultsim_test_result {0 3}
}

finish_test



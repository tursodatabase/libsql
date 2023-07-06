# 2019 April 23
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
# Tests for the SQLITE_RECOVER_ROWIDS option.
#

source [file join [file dirname [info script]] recover_common.tcl]
set testprefix recoverclobber

proc recover {db output} {
  set R [sqlite3_recover_init db main test.db2]
  $R run
  $R finish
}

forcedelete test.db2
do_execsql_test 1.0 {
  ATTACH 'test.db2' AS aux;
  CREATE TABLE aux.x1(x, one);
  INSERT INTO x1 VALUES(1, 'one'), (2, 'two'), (3, 'three');

  CREATE TABLE t1(a, b);
  INSERT INTO t1 VALUES(1, 1), (2, 2), (3, 3), (4, 4);

  DETACH aux;
}

breakpoint
do_test 1.1 {
  recover db test.db2
} {}

do_execsql_test 1.2 {
  ATTACH 'test.db2' AS aux;
  SELECT * FROM aux.t1;
} {1 1   2 2   3 3   4 4}

do_catchsql_test 1.3 {
  SELECT * FROM aux.x1;
} {1 {no such table: aux.x1}}

finish_test

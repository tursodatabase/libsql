# 2024 February 24
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

source [file join [file dirname [info script]] intck_common.tcl]
set testprefix intckbusy
return_if_no_intck



do_execsql_test 1.0 {
  CREATE TABLE t1(a INTEGER PRIMARY KEY, b, c);
  INSERT INTO t1 VALUES(1, 2, 3);
  INSERT INTO t1 VALUES(2, 'two', 'three');
  INSERT INTO t1 VALUES(3, NULL, NULL);
  CREATE INDEX i1 ON t1(b, c);
}

sqlite3 db2 test.db

do_execsql_test -db db2 1.1 {
  BEGIN EXCLUSIVE;
    INSERT INTO t1 VALUES(4, 5, 6);
}

do_test 1.2 {
  set ic [sqlite3_intck db main]
  $ic step
} {SQLITE_BUSY}
do_test 1.3 {
  $ic unlock
} {SQLITE_BUSY}
do_test 1.4 {
  $ic error
} {SQLITE_BUSY {database is locked}}
do_test 1.4 {
  $ic close
} {}

finish_test


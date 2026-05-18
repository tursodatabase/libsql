# 2025 August 27
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
set testprefix reservebytes



reset_db
file_control_reservebytes db 0


do_execsql_test 1.0 {
  CREATE TABLE t1(a INTEGER PRIMARY KEY, b, c);
  CREATE INDEX i1 ON t1(b, c);
  WITH s(i) AS (
    VALUES(1) UNION ALL SELECT i+1 FROM s WHERE i<1000
  )
  INSERT INTO t1 SELECT NULL, i, hex(randomblob(500)) FROM s;
}

sqlite3 db2 test.db
if {[permutation]=="prepare"} { db2 cache size 0 }

do_execsql_test -db db2 1.1 { PRAGMA integrity_check } {ok}

file_control_reservebytes db 8
do_test 1.2.1 { hexio_read test.db 20 1 } {00}
do_execsql_test -db db2 1.2.2 { PRAGMA integrity_check } {ok}

do_execsql_test 1.3.2 { VACUUM }
do_execsql_test -db db2 1.3.4 { PRAGMA integrity_check } {ok}
do_test 1.3.5 { hexio_read test.db 20 1 } {08}

file_control_reservebytes db 16
do_test 1.4.1 { hexio_read test.db 20 1 } {08}
do_execsql_test 1.4.2 { VACUUM }
do_execsql_test -db db2 1.4.3 { PRAGMA integrity_check } {ok}
do_test 1.4.4 { hexio_read test.db 20 1 } {10}

finish_test

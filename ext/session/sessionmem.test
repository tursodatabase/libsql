# 2020 December 23
#
# The author disclaims copyright to this source code.  In place of
# a legal notice, here is a blessing:
#
#    May you do good and not evil.
#    May you find forgiveness for yourself and forgive others.
#    May you share freely, never taking more than you give.
#
#***********************************************************************
# This file implements regression tests for the SQLite sessions module
# Specifically, for the sqlite3session_memory_used() API.
#

if {![info exists testdir]} {
  set testdir [file join [file dirname [info script]] .. .. test]
} 
source [file join [file dirname [info script]] session_common.tcl]
source $testdir/tester.tcl
ifcapable !session {finish_test; return}

set testprefix sessionmem

do_execsql_test 1.0 {
  CREATE TABLE t1(i INTEGER PRIMARY KEY, x, y);
  CREATE TABLE t2(i INTEGER, x, y, PRIMARY KEY(x, y));
}

do_test 1.1 {
  sqlite3session S db main
  S attach *
} {}

foreach {tn sql eRes} {
  1 { INSERT INTO t1 VALUES(1, 2, 3) } 1
  2 { UPDATE t1 SET x=5 }                    0
  3 { UPDATE t1 SET i=5 }                    1
  4 { DELETE FROM t1 }                       0
  5 { INSERT INTO t1 VALUES(1, 2, 3) }       0
  6 { INSERT INTO t1 VALUES(5, 2, 3) }       0
  7 { INSERT INTO t2 VALUES('a', 'b', 'c') } 1
  8 { INSERT INTO t2 VALUES('d', 'e', 'f') } 1
  9 { UPDATE t2 SET i='e' }                  0
} {
  set mem1 [S memory_used]
  do_test 1.2.$tn.(mu=$mem1) {
    execsql $sql
    set mem2 [S memory_used]
    expr {$mem2 > $mem1}
  } $eRes
}

do_test 1.3 {
  S delete
} {}

finish_test

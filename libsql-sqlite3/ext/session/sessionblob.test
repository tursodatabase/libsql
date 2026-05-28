# 2024 November 04
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

if {![info exists testdir]} {
  set testdir [file join [file dirname [info script]] .. .. test]
} 
source [file join [file dirname [info script]] session_common.tcl]
source $testdir/tester.tcl
ifcapable !session {finish_test; return}

if {$::tcl_platform(pointerSize)<8} {
  finish_test
  return
}

set testprefix sessionblob

forcedelete test.db2
sqlite3 db2 test.db2

set NBLOB 2000000

do_execsql_test 1.0 {
  CREATE TABLE t1(a INTEGER PRIMARY KEY, b);
  INSERT INTO t1 VALUES(123, zeroblob($NBLOB));
}

do_test 1.1 {
  sqlite3session S db main
  S attach t1
} {}

set b2 [string repeat x 1000]
do_test 1.2 {
  set ::blob [db incrblob t1 b 123]
  for {set ii 0} {$ii < $NBLOB} {incr ii [string length $b2]} {
    seek $::blob $ii
    puts -nonewline $::blob $b2
  }
  close $::blob
} {}

S delete

finish_test


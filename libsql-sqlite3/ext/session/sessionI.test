# 2015 July 14
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

if {![info exists testdir]} {
  set testdir [file join [file dirname [info script]] .. .. test]
} 
source [file join [file dirname [info script]] session_common.tcl]
source $testdir/tester.tcl
ifcapable !session {finish_test; return}

set testprefix sessionI

forcedelete test.db2
sqlite3 db2 test.db2

do_test 1.0 {
  do_common_sql {
    CREATE TABLE t1(x INTEGER PRIMARY KEY, y);
  }
} {}

set C [changeset_from_sql {
  INSERT INTO t1 VALUES(1, 'one');
  INSERT INTO t1 VALUES(2, 'two');
  INSERT INTO t1 VALUES(3, 'three');
  INSERT INTO t1 VALUES(4, 'four');
  INSERT INTO t1 VALUES(5, 'five');
  INSERT INTO t1 VALUES(6, 'six');
}]

do_execsql_test 1.1 {
  SELECT * FROM t1
} {
  1 one 2 two 3 three 4 four 5 five 6 six
}

proc xFilter {data} {
  foreach {op tname flag pk old new} $data {}
  if {$op=="INSERT"} {
    set ipk [lindex $new 1]
    return [expr $ipk % 2]
  }
  return 1
}
proc xConflict {args} {
}

sqlite3changeset_apply_v3 db2 $C xConflict xFilter

do_execsql_test -db db2 1.2 {
  SELECT * FROM t1
} {
  1 one 3 three 5 five
}

do_execsql_test -db db2 1.3 {
  DELETE FROM t1
}
sqlite3changeset_apply_v3 db2 $C xConflict

do_execsql_test -db db2 1.4 {
  SELECT * FROM t1
} {
  1 one 2 two 3 three 4 four 5 five 6 six
}

proc xFilter2 {data} {
  return 0
}
do_execsql_test -db db2 1.5 {
  DELETE FROM t1
}
sqlite3changeset_apply_v3 db2 $C xConflict xFilter2
do_execsql_test -db db2 1.6 {
  SELECT * FROM t1
} { }

finish_test

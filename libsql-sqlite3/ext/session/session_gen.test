# 2025 Jan 28
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

if {![info exists testdir]} {
  set testdir [file join [file dirname [info script]] .. .. test]
} 
source [file join [file dirname [info script]] session_common.tcl]
source $testdir/tester.tcl
ifcapable !session {finish_test; return}

set testprefix session_gen


foreach {otn sct} {
  1  VIRTUAL
  2  STORED
} {
eval [string map [list %TYPE% $sct] {
  reset_db
  set testprefix $testprefix-$otn

do_execsql_test 1.0 {
  CREATE TABLE t1(a INTEGER PRIMARY KEY, b);

  CREATE TABLE t2(a INTEGER PRIMARY KEY, b AS (c+1) %TYPE%, c);

  CREATE TABLE t3(
    a, 
    b AS (a+10) %TYPE%, 
    c, 
    d AS (c+1) %TYPE%, 
    e, 
    PRIMARY KEY(c, e)
  ) WITHOUT ROWID;

  CREATE TABLE t4(a AS (c*100) %TYPE%, b INTEGER PRIMARY KEY, c);

  CREATE TABLE t5(x, y);
}

foreach {tn sql changeset} {
  
  0.1 {
    INSERT INTO t5 VALUES('abc', 'def');
  } {
    {INSERT t5 0 X.. {} {i 1 t abc t def}}
  }
  0.2 {
    UPDATE t5 SET y='xyz' WHERE rowid=1;
  } {
    {UPDATE t5 0 X.. {i 1 {} {} t def} {{} {} {} {} t xyz}}
  }
  0.3 {
    DELETE FROM t5;
  } {
    {DELETE t5 0 X.. {i 1 t abc t xyz} {}}
  }

  1.1 {
    INSERT INTO t2 VALUES(1, 2);
    INSERT INTO t2 VALUES(2, 123);
  } {
    {INSERT t2 0 X. {} {i 1 i 2}}
    {INSERT t2 0 X. {} {i 2 i 123}}
  }
  1.2 {
    UPDATE t2 SET c=456 WHERE a=1
  } {
    {UPDATE t2 0 X. {i 1 i 2} {{} {} i 456}}
  }

  1.3 {
    DELETE FROM t2 WHERE a=2
  } {
    {DELETE t2 0 X. {i 2 i 123} {}}
  }

  1.4 {
    UPDATE t2 SET a=15
  } {
    {INSERT t2 0 X. {} {i 15 i 456}} 
    {DELETE t2 0 X. {i 1 i 456} {}}
  }

  2.1 {
    INSERT INTO t3 VALUES(5, 6, 7);
    INSERT INTO t3 VALUES(8, 9, 10);
  } {
    {INSERT t3 0 .XX {} {i 8 i 9 i 10}}
    {INSERT t3 0 .XX {} {i 5 i 6 i 7}}
  }

  2.2 {
    UPDATE t3 SET a = 505 WHERE (c, e) = (6, 7);
  } {
    {UPDATE t3 0 .XX {i 5 i 6 i 7} {i 505 {} {} {} {}}}
  }

  2.3 {
    DELETE FROM t3 WHERE (c, e) = (9, 10);
  } {
    {DELETE t3 0 .XX {i 8 i 9 i 10} {}}
  }

  2.4 {
    UPDATE t3 SET c=1000
  } {
    {DELETE t3 0 .XX {i 505 i 6 i 7} {}}
    {INSERT t3 0 .XX {} {i 505 i 1000 i 7}}
  }

  3.1 {
    INSERT INTO t4 VALUES(100, 100);
  } {
    {INSERT t4 0 X. {} {i 100 i 100}}
  }

} {
  do_test 1.$tn.1 {
    sqlite3session S db main
  S object_config rowid 1
    S attach *
    execsql $sql
  } {}

  do_changeset_test 1.$tn.2 S $changeset

  S delete
}
#-------------------------------------------------------------------------
reset_db

forcedelete test.db2
sqlite3 db2 test.db2

do_common_sql {
  CREATE TABLE t0(x INTEGER PRIMARY KEY, y);
  INSERT INTO t0 VALUES(1, 'one');
  INSERT INTO t0 VALUES(2, 'two');

  CREATE TABLE t1(a AS (c*10) %TYPE%, b INTEGER PRIMARY KEY, c);
  INSERT INTO t1 VALUES(1, 5);
  INSERT INTO t1 VALUES(2, 10);
  INSERT INTO t1 VALUES(3, 5);

  CREATE TABLE t2(
    a, b, c AS (a*b) %TYPE%,
    'k 1', 'k 2', PRIMARY KEY('k 1', 'k 2')
  ) WITHOUT ROWID;
  INSERT INTO t2 VALUES('a', 'b', 1, 11);
  INSERT INTO t2 VALUES('A', 'B', 2, 22);
  INSERT INTO t2 VALUES('Aa', 'Bb', 3, 33);
}

foreach {tn sql} {
  1.1 { INSERT INTO t0 VALUES(4, 15) }
  1.2 { INSERT INTO t1 VALUES(4, 15) }
  1.3 { INSERT INTO t2 VALUES(1, 2, 3, 4) }

  2.1 { UPDATE t1 SET c=100 WHERE b=2 }
  2.2 { UPDATE t2 SET a=11 }

  3.1 { DELETE FROM t2 WHERE (t2.'k 1') = 2 }
  3.2 { DELETE FROM t1 }
} {
  do_test 2.$tn.1 {
  # execsql { PRAGMA vdbe_listing = 1 } db2
    do_then_apply_sql $sql
  } {}
  do_test 2.$tn.2 {
    compare_db db db2
  } {}
}
db2 close

}]}




finish_test

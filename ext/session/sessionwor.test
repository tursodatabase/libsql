# 2017 Jan 31
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
# The focus of this file is testing the session module. Specifically,
# testing support for WITHOUT ROWID tables.
#

if {![info exists testdir]} {
  set testdir [file join [file dirname [info script]] .. .. test]
} 
source [file join [file dirname [info script]] session_common.tcl]
source $testdir/tester.tcl
ifcapable !session {finish_test; return}

set testprefix sessionwor

proc test_reset {} {
  catch { db close }
  catch { db2 close }
  forcedelete test.db test.db2
  sqlite3 db test.db
  sqlite3 db2 test.db2
}

foreach {tn wo} {
  1 ""
  2 "WITHOUT ROWID"
} {
  reset_db

  do_execsql_test 1.$tn.0 "CREATE TABLE t1(a PRIMARY KEY, b) $wo ;"
  
  do_iterator_test 1.$tn.1 t1 {
    INSERT INTO t1 VALUES('one', 'two');
  } {
    {INSERT t1 0 X. {} {t one t two}}
  }
  
  do_iterator_test 1.$tn.2 t1 {
    UPDATE t1 SET b='three'
  } {
    {UPDATE t1 0 X. {t one t two} {{} {} t three}}
  }
  
  do_iterator_test 1.$tn.3 t1 {
    REPLACE INTO t1 VALUES('one', 'four');
  } {
    {UPDATE t1 0 X. {t one t three} {{} {} t four}}
  }
  
  do_iterator_test 1.$tn.4 t1 {
    DELETE FROM t1;
  } {
    {DELETE t1 0 X. {t one t four} {}}
  }
}

foreach {tn wo} {
  1 ""
  2 "WITHOUT ROWID"
} {
  reset_db

  do_execsql_test 2.$tn.0.1 "CREATE TABLE t1(a INTEGER PRIMARY KEY, b) $wo ;"
  do_execsql_test 2.$tn.0.2 "CREATE TABLE t2(a INTEGER PRIMARY KEY, b) $wo ;"
  do_execsql_test 2.$tn.0.3 "CREATE TABLE t3(a INTEGER PRIMARY KEY, b) $wo ;"
  
  do_iterator_test 1.1 t1 {
    INSERT INTO t1 VALUES(1, 'two');
  } {
    {INSERT t1 0 X. {} {i 1 t two}}
  }
  
  do_iterator_test 2.$tn.2 t1 {
    UPDATE t1 SET b='three'
  } {
    {UPDATE t1 0 X. {i 1 t two} {{} {} t three}}
  }
  
  do_iterator_test 2.$tn.3 t1 {
    REPLACE INTO t1 VALUES(1, 'four');
  } {
    {UPDATE t1 0 X. {i 1 t three} {{} {} t four}}
  }
  
  do_iterator_test 2.$tn.4 t1 {
    DELETE FROM t1;
  } {
    {DELETE t1 0 X. {i 1 t four} {}}
  }

  do_execsql_test 2.$tn.5 {
    INSERT INTO t1 VALUES(1, 'one');
    INSERT INTO t1 VALUES(2, 'two');
    INSERT INTO t1 VALUES(3, 'three');
  }

  do_iterator_test 2.$tn.6 t2 {
    INSERT INTO t2 SELECT a, b FROM t1
  } {
    {INSERT t2 0 X. {} {i 1 t one}} 
    {INSERT t2 0 X. {} {i 2 t two}}
    {INSERT t2 0 X. {} {i 3 t three}}
  }
  do_iterator_test 2.$tn.7 t3 {
    INSERT INTO t3 SELECT * FROM t1
  } {
    {INSERT t3 0 X. {} {i 1 t one}} 
    {INSERT t3 0 X. {} {i 2 t two}}
    {INSERT t3 0 X. {} {i 3 t three}}
  }
}

finish_test


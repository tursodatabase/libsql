# 2014 August 16
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
# This file implements regression tests for sessions SQLite extension.
# Specifically, this file contains tests for "patchset" changes.
#

if {![info exists testdir]} {
  set testdir [file join [file dirname [info script]] .. .. test]
} 
source [file join [file dirname [info script]] session_common.tcl]
source $testdir/tester.tcl
ifcapable !session {finish_test; return}

if {[permutation]=="session_strm" || [permutation]=="session_eec"} {
  finish_test
  return
}

if {$::tcl_platform(pointerSize)<8} {
  finish_test
  return
}

set testprefix sessionbig

forcedelete test.db2
sqlite3 db2 test.db2

do_execsql_test 1.0 {
  CREATE TABLE t1(a INTEGER PRIMARY KEY, b);
}
do_execsql_test -db db2 1.1 {
  CREATE TABLE t1(a INTEGER PRIMARY KEY, b);
}

do_test 1.2 {
  do_then_apply_sql -ignorenoop {
    INSERT INTO t1(b) VALUES( zeroblob(100*1000*1000) );
    INSERT INTO t1(b) VALUES( zeroblob(100*1000*1000) );
    INSERT INTO t1(b) VALUES( zeroblob(100*1000*1000) );
    INSERT INTO t1(b) VALUES( zeroblob(100*1000*1000) );
    INSERT INTO t1(b) VALUES( zeroblob(100*1000*1000) );

    INSERT INTO t1(b) VALUES( zeroblob(100*1000*1000) );
    INSERT INTO t1(b) VALUES( zeroblob(100*1000*1000) );
    INSERT INTO t1(b) VALUES( zeroblob(100*1000*1000) );
    INSERT INTO t1(b) VALUES( zeroblob(100*1000*1000) );
    INSERT INTO t1(b) VALUES( zeroblob(100*1000*1000) );

    INSERT INTO t1(b) VALUES( zeroblob(100*1000*1000) );
    INSERT INTO t1(b) VALUES( zeroblob(100*1000*1000) );
    INSERT INTO t1(b) VALUES( zeroblob(100*1000*1000) );
    INSERT INTO t1(b) VALUES( zeroblob(100*1000*1000) );
    INSERT INTO t1(b) VALUES( zeroblob(100*1000*1000) );
  }
} {}

do_test 1.3 {
  execsql { DELETE FROM t1 }
  execsql2 { DELETE FROM t1 }
} {}

do_test 1.4 {
  set rc [catch {
  do_then_apply_sql -ignorenoop {
    INSERT INTO t1(b) VALUES( zeroblob(100*1000*1000) );
    INSERT INTO t1(b) VALUES( zeroblob(100*1000*1000) );
    INSERT INTO t1(b) VALUES( zeroblob(100*1000*1000) );
    INSERT INTO t1(b) VALUES( zeroblob(100*1000*1000) );
    INSERT INTO t1(b) VALUES( zeroblob(100*1000*1000) );

    INSERT INTO t1(b) VALUES( zeroblob(100*1000*1000) );
    INSERT INTO t1(b) VALUES( zeroblob(100*1000*1000) );
    INSERT INTO t1(b) VALUES( zeroblob(100*1000*1000) );
    INSERT INTO t1(b) VALUES( zeroblob(100*1000*1000) );
    INSERT INTO t1(b) VALUES( zeroblob(100*1000*1000) );

    INSERT INTO t1(b) VALUES( zeroblob(100*1000*1000) );
    INSERT INTO t1(b) VALUES( zeroblob(100*1000*1000) );
    INSERT INTO t1(b) VALUES( zeroblob(100*1000*1000) );
    INSERT INTO t1(b) VALUES( zeroblob(100*1000*1000) );
    INSERT INTO t1(b) VALUES( zeroblob(100*1000*1000) );

    INSERT INTO t1(b) VALUES( zeroblob(100*1000*1000) );
    INSERT INTO t1(b) VALUES( zeroblob(100*1000*1000) );
    INSERT INTO t1(b) VALUES( zeroblob(100*1000*1000) );
    INSERT INTO t1(b) VALUES( zeroblob(100*1000*1000) );
    INSERT INTO t1(b) VALUES( zeroblob(100*1000*1000) );

    INSERT INTO t1(b) VALUES( zeroblob(100*1000*1000) );
    INSERT INTO t1(b) VALUES( zeroblob(100*1000*1000) );
  }
  } msg]
  list $rc $msg
} {1 SQLITE_NOMEM}


finish_test


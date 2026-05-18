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
set testprefix intckfault
return_if_no_intck

do_execsql_test 1.0 {
  CREATE TABLE t1(a INTEGER PRIMARY KEY, b, c);
  INSERT INTO t1 VALUES(1, 2, 3);
  INSERT INTO t1 VALUES(2, 'two', 'three');
  INSERT INTO t1 VALUES(3, NULL, NULL);
  CREATE INDEX i1 ON t1(b, c);
}

do_faultsim_test 1 -faults oom-t* -prep {
} -body {
  set ::ic [sqlite3_intck db main]
  set nStep 0
  while {"SQLITE_OK"==[$::ic step]} {
    incr nStep
    if {$nStep==3} { $::ic unlock }
  }
  set res [$::ic error]
  $::ic close
  set res
} -test {
  catch { $::ic close }
  faultsim_test_result {0 {SQLITE_OK {}}} {0 {SQLITE_NOMEM {}}} {0 {SQLITE_NOMEM {out of memory}}}
}

finish_test


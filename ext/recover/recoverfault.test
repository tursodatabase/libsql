# 2022 August 28
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

source [file join [file dirname [info script]] recover_common.tcl]
set testprefix recoverfault


#--------------------------------------------------------------------------
proc compare_result {db1 db2 sql} {
  set r1 [$db1 eval $sql]
  set r2 [$db2 eval $sql]
  if {$r1 != $r2} {
    puts "r1: $r1"
    puts "r2: $r2"
    error "mismatch for $sql"
  }
  return ""
}

proc compare_dbs {db1 db2} {
  compare_result $db1 $db2 "SELECT sql FROM sqlite_master ORDER BY 1"
  foreach tbl [$db1 eval {SELECT name FROM sqlite_master WHERE type='table'}] {
    compare_result $db1 $db2 "SELECT * FROM $tbl"
  }
}
#--------------------------------------------------------------------------

do_execsql_test 1.0 {
  CREATE TABLE t1(a INTEGER PRIMARY KEY, b, c);
  INSERT INTO t1 VALUES(1, 2, 3);
  INSERT INTO t1 VALUES(2, hex(randomblob(1000)), randomblob(2000));
  CREATE INDEX i1 ON t1(b, c);
  ANALYZE;
}
faultsim_save_and_close

do_faultsim_test 1 -faults oom* -prep {
  catch { db2 close }
  faultsim_restore_and_reopen
} -body {
  set R [sqlite3_recover_init db main test.db2]
  $R run
  $R finish
} -test {
  faultsim_test_result {0 {}} {1 {}}
  if {$testrc==0} {
    sqlite3 db2 test.db2
    compare_dbs db db2
    db2 close
  }
}

faultsim_restore_and_reopen 
do_execsql_test 2.0 {
  CREATE TABLE t2(a INTEGER PRIMARY KEY, b, c);
  INSERT INTO t2 VALUES(1, 2, 3);
  INSERT INTO t2 VALUES(2, hex(randomblob(1000)), hex(randomblob(2000)));
  PRAGMA writable_schema = 1;
  DELETE FROM sqlite_schema WHERE name='t2';
}
faultsim_save_and_close

do_faultsim_test 2 -faults oom* -prep {
  faultsim_restore_and_reopen
} -body {
  set R [sqlite3_recover_init db main test.db2]
  $R config lostandfound lost_and_found
  $R run
  $R finish
} -test {
  faultsim_test_result {0 {}} {1 {}}
}

finish_test


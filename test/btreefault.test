# 2013 April 02
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
# This file contains fault injection tests designed to test the btree.c 
# module.
#

set testdir [file dirname $argv0]
source $testdir/tester.tcl
source $testdir/malloc_common.tcl
set testprefix btreefault

# This test will not work with an in-memory journal, as the database will
# become corrupt if an error is injected into a transaction after it starts
# writing data out to the db file.
if {[permutation]=="inmemory_journal"} {
  finish_test
  return
}

do_test 1-pre1 {
  execsql {
    PRAGMA auto_vacuum = incremental;
    PRAGMA journal_mode = DELETE;
    CREATE TABLE t1(a PRIMARY KEY, b);
    INSERT INTO t1 VALUES(randomblob(1000), randomblob(100));
    INSERT INTO t1 SELECT randomblob(1000), randomblob(1000) FROM t1;
    INSERT INTO t1 SELECT randomblob(1000), randomblob(1000) FROM t1;
    INSERT INTO t1 SELECT randomblob(1000), randomblob(1000) FROM t1;
    INSERT INTO t1 SELECT randomblob(1000), randomblob(1000) FROM t1;
    DELETE FROM t1 WHERE rowid%2;
  }
  faultsim_save_and_close
} {}

do_faultsim_test 1 -prep {
  faultsim_restore_and_reopen
  set ::STMT [sqlite3_prepare db "SELECT * FROM t1 ORDER BY a" -1 DUMMY]
  sqlite3_step $::STMT
  sqlite3_step $::STMT
} -body {
  execsql { PRAGMA incremental_vacuum = 10 }
} -test {
  sqlite3_finalize $::STMT
  faultsim_test_result {0 {}} 
  faultsim_integrity_check
}

#-------------------------------------------------------------------------
# dbsqlfuzz crash-6ef3cd3b18ccc5de86120950a0498641acd90a33.txt
#
reset_db

do_execsql_test 2.0 {
  CREATE TABLE t1(i INTEGER PRIMARY KEY, a, b);
  CREATE INDEX i1 ON t1(b);
  CREATE TABLE t2(x, y);
}

do_execsql_test 2.1 {
  INSERT INTO t1 VALUES(25, 25, 25);
  INSERT INTO t2 VALUES(25, 'a'), (25, 'b'), (25, 'c');
}

faultsim_save
do_test 2.2 {
  set res [list]
  db eval {
    SELECT x, y FROM t1 CROSS JOIN t2 WHERE t2.x=t1.i AND +t1.i=25 ORDER BY b
  } {
    lappend res $x $y
    if {$y=="b"} {
      db eval { DELETE FROM t1 WHERE i=25 }
    }
  }
  set res
} {25 a 25 b}

do_faultsim_test 2 -faults oom-t* -prep {
  faultsim_restore_and_reopen
  db eval {SELECT * FROM sqlite_master}
} -body {
  set ::myres [list]
  db eval {
    SELECT x, y FROM t1 CROSS JOIN t2 WHERE t2.x=t1.i AND +t1.i=25 ORDER BY b
  } {
    lappend ::myres $x $y
    if {$y=="b"} {
      db eval { DELETE FROM t1 WHERE i=25 }
    }
  }
  set ::myres
} -test {
  faultsim_test_result {0 {25 a 25 b}} 
}


finish_test

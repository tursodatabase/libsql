# 2022 July 06
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
source $testdir/lock_common.tcl
source $testdir/malloc_common.tcl

if {[permutation] == "inmemory_journal"} {
  finish_test
  return
}

ifcapable !vtab {
  finish_test
  return
}

set testprefix dbpagefault

faultsim_save_and_close
do_faultsim_test 1 -prep {
  faultsim_restore_and_reopen
  execsql { ATTACH 'test.db2' AS aux; }
} -body {
  execsql { 
    CREATE VIRTUAL TABLE t1 USING sqlite_dbpage();
  }
} -test {
  execsql { PRAGMA journal_mode = off }
  faultsim_test_result {0 {}} 
}

do_faultsim_test 2 -prep {
  sqlite3 db "xyz.db" -vfs memdb
  execsql { ATTACH 'test.db2' AS aux; }
} -body {
  execsql { 
    CREATE VIRTUAL TABLE t1 USING sqlite_dbpage();
    UPDATE t1 SET data=zeroblob(1024) WHERE pgno=1 AND schema='aux';
  }
} -test {
  execsql { PRAGMA journal_mode = off }
  faultsim_test_result {0 {}} {1 {no such schema}}  {1 {SQL logic error}} {1 {unable to open a temporary database file for storing temporary tables}}
}

reset_db
do_execsql_test 3.0 {
  CREATE TABLE x1(z, b);
  CREATE TRIGGER BEFORE INSERT ON x1 BEGIN
    DELETE FROM sqlite_dbpage WHERE pgno=100;
    UPDATE sqlite_dbpage SET data=null WHERE pgno=100;
  END;
}

# This test case no longer works, as it is no longer possible to use
# virtual table sqlite_dbpage from within a trigger.
#
do_execsql_test 3.1 {
  PRAGMA trusted_schema = 1;
}
do_catchsql_test 3.2 {
  PRAGMA trusted_schema = 1;
  INSERT INTO x1 DEFAULT VALUES;
} {1 {unsafe use of virtual table "sqlite_dbpage"}}
#do_faultsim_test 3 -prep {
#  catch { db close }
#  sqlite3 db test.db
#  execsql { PRAGMA trusted_schema = 1 }
#} -body {
#  execsql { INSERT INTO x1 DEFAULT VALUES; }
#} -test {
#  faultsim_test_result {0 {}}
#}

reset_db
forcedelete test.db2
do_execsql_test 4.0 {
  CREATE TABLE t1(x);
  INSERT INTO t1 VALUES('one');
  CREATE TABLE t2(x);
  INSERT INTO t2 VALUES('two');
  ATTACH 'test.db2' AS aux;
  CREATE TABLE aux.x1(x);
}

set pgno [db one {SELECT max(rootpage) FROM sqlite_schema}]

faultsim_save_and_close
do_faultsim_test 4 -prep {
  faultsim_restore_and_reopen
  execsql { ATTACH 'test.db2' AS aux; }
} -body {
  execsql { 
    UPDATE sqlite_dbpage SET data = (
      SELECT data FROM sqlite_dbpage WHERE pgno=($pgno-1)
    ) WHERE pgno = $pgno;
  }
} -test {
  faultsim_test_result {0 {}} {1 {unable to open a temporary database file for storing temporary tables}}
}

finish_test

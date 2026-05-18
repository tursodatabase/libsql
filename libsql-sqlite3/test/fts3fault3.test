# 2023 October 23 
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

set ::testprefix fts3fault

# If SQLITE_ENABLE_FTS3 is not defined, omit this file.
ifcapable !fts3 { finish_test ; return }

set ::TMPDBERROR [list 1 \
  {unable to open a temporary database file for storing temporary tables}
]


# Test error handling in an "ALTER TABLE ... RENAME TO" statement on an
# FTS3 table. Specifically, test renaming the table within a transaction
# after it has been written to.
#
do_execsql_test 1.0 {
  CREATE VIRTUAL TABLE t1 USING fts3(a);
  INSERT INTO t1 VALUES('test renaming the table');
  INSERT INTO t1 VALUES(' after it has been written');
  INSERT INTO t1 VALUES(' actually other stuff instead');
}
faultsim_save_and_close
do_faultsim_test 1 -faults oom* -prep { 
  faultsim_restore_and_reopen
  execsql {
    BEGIN;
      DELETE FROM t1 WHERE rowid=2;
  }
} -body {
  execsql {
    DELETE FROM t1;
  }
} -test {
  catchsql { COMMIT }
  faultsim_integrity_check
  faultsim_test_result {0 {}}
}

#-------------------------------------------------------------------
reset_db

do_execsql_test 2.0 {
  BEGIN;
  CREATE VIRTUAL TABLE t1 USING fts3(a);
  WITH s(i) AS (
      SELECT 1 UNION ALL SELECT i+1 FROM s WHERE i<50
  )
  INSERT INTO t1 SELECT 'abc def ghi jkl mno pqr' FROM s;
  COMMIT;
}

faultsim_save_and_close
do_faultsim_test 2 -faults oom-t* -prep { 
  faultsim_restore_and_reopen
  execsql {
    BEGIN;
      CREATE TABLE x1(a PRIMARY KEY);
  }
} -body {
  execsql {
    PRAGMA integrity_check;
  }
} -test {
  faultsim_test_result {0 ok} $::TMPDBERROR
}


finish_test

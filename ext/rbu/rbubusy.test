# 2014 August 30
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

source [file join [file dirname [info script]] rbu_common.tcl]
ifcapable !rbu { finish_test ; return }
if_no_rbu_support { finish_test ; return }
set ::testprefix rbubusy

db close
sqlite3_shutdown
test_sqlite3_log xLog
reset_db

set db_sql {
  CREATE TABLE t1(a PRIMARY KEY, b, c);
  INSERT INTO t1 VALUES(1000, 2000, 3000);
}

set rbu_sql {
  CREATE TABLE data_t1(a, b, c, rbu_control);
  INSERT INTO data_t1 VALUES(1, 2, 3, 0);
  INSERT INTO data_t1 VALUES(4, 5, 6, 0);
  INSERT INTO data_t1 VALUES(7, 8, 9, 0);
}

do_test 1.1 {
  forcedelete rbu.db
  sqlite3 rbu rbu.db
  rbu eval $rbu_sql
  rbu close

  db eval $db_sql
} {}

do_execsql_test 1.2 {
  BEGIN;
    SELECT * FROM t1
} {1000 2000 3000}

do_test 1.3 {
  sqlite3rbu rbu test.db rbu.db
  rbu step
} {SQLITE_OK}

do_test 1.4 {
  while 1 {
    set rc [rbu step]
    if {$rc!="SQLITE_OK"} break
  }
  set rc
} {SQLITE_BUSY}

do_test 1.5 {
  rbu step
} {SQLITE_BUSY}

do_test 1.6 {
  db eval COMMIT
  rbu step
} {SQLITE_BUSY}
catch { rbu close }

do_test 1.7 {
  sqlite3rbu rbu test.db rbu.db
  while 1 {
    set rc [rbu step]
    if {$rc!="SQLITE_OK"} break
  }
  set rc
} {SQLITE_DONE}

rbu close

db close
sqlite3_shutdown
test_sqlite3_log 
sqlite3_initialize
finish_test


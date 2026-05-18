# 2024 March 19
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
set testprefix cksumvfs


db close
sqlite3_shutdown
#test_sqlite3_log logfunc
sqlite3_initialize

proc logfunc {args} {
  puts "LOG: $args"
}

sqlite3_register_cksumvfs
sqlite3 db test.db

file_control_reservebytes db 8
execsql {
  PRAGMA page_size = 4096;
}

set text [db one "SELECT hex(randomblob(5000))"]

do_execsql_test 1.0 {
  CREATE TABLE t1(a INTEGER PRIMARY KEY, b, c);
  INSERT INTO t1 VALUES(1, $text, NULL);
}

do_execsql_test 1.1 {
  SELECT * FROM t1;
} [list 1 $text {}]

do_execsql_test 1.2 {
  DELETE FROM t1;
}

do_test 1.3 {
  execsql BEGIN
  for {set ii 1500} {$ii < 10000} {incr ii} {
    execsql { INSERT INTO t1 VALUES(NULL, randomblob(5000), randomblob($ii)) }
  }
  execsql COMMIT
} {}

do_execsql_test 1.4 {
  SELECT count(b) FROM t1
} {8500}

do_execsql_test 1.5 {
  PRAGMA journal_mode = wal;
  DELETE FROM t1;
} {wal}

do_execsql_test 1.6 {
  PRAGMA wal_checkpoint;
} {/0 # #/}

do_execsql_test 1.7 {
  WITH s(i) AS (
    VALUES(1) UNION ALL SELECT i+1 FROM s WHERE i<100
  )
  INSERT INTO t1 SELECT NULL, randomblob(5000), randomblob(i) FROM s;
  SELECT count(*) FROM t1;
} {100}

db_save_and_close
db_restore_and_reopen

do_execsql_test 1.8 {
  SELECT count(*) FROM t1;
} {100}

db close
sqlite3 db test.db

do_execsql_test 1.9 {
  SELECT count(*) FROM t1;
} {100}

finish_test

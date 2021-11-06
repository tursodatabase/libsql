# 2020-09-24
#
# The author disclaims copyright to this source code.  In place of
# a legal notice, here is a blessing:
#
#    May you do good and not evil.
#    May you find forgiveness for yourself and forgive others.
#    May you share freely, never taking more than you give.
#
#***********************************************************************
# This file implements tests for a strange scenario discovered by
# dbsqlfuzz (0ad6d441f9bf3dfc32626a9900bc1700495b16f9) in which a
# virtual table is named "sqlite_stat1".
#

set testdir [file dirname $argv0]
source $testdir/tester.tcl
set testprefix vtabK

ifcapable !vtab||!rtree||!fts5 {
  finish_test
  return
}

do_execsql_test 100 {
  CREATE TABLE t1(x);
  INSERT INTO t1 VALUES(123);
  PRAGMA writable_schema=ON;
  CREATE VIRTUAL TABLE sqlite_stat1 USING fts5(a);
  PRAGMA writable_schema=OFF;
  CREATE VIRTUAL TABLE t3 USING fts5(b);
  INSERT INTO t3 VALUES('this is a test');
}
do_catchsql_test 110 {
  CREATE VIRTUAL TABLE t2 USING rtree(id,x,y);
} {1 {no such column: stat}}
do_execsql_test 120 {
  SELECT * FROM t1;
} {123}
do_execsql_test 130 {
  INSERT INTO t3(b) VALUES('Four score and seven years ago');
  SELECT * FROM t3 WHERE t3 MATCH 'this';
} {{this is a test}}
do_execsql_test 140 {
  SELECT * FROM t3 WHERE t3 MATCH 'four seven';
} {{Four score and seven years ago}}
do_execsql_test 150 {
  INSERT INTO sqlite_stat1(a)
  VALUES('We hold these truths to be self-evident...');
  SELECT * FROM sqlite_stat1;
} {{We hold these truths to be self-evident...}}
do_catchsql_test 160 {
  ANALYZE;
} {1 {database disk image is malformed}}
do_execsql_test 170 {
  PRAGMA integrity_check;
} {ok}

# Follow-on dbsqlfuzz bc02a0cde82dee801a8d6f653d2831680f87dca1
reset_db
do_execsql_test 200 {
  CREATE TABLE t1(a);
  INSERT INTO t1 VALUES('Ebed-malech');
  CREATE TABLE x(a);
  PRAGMA writable_schema=ON;
  CREATE VIRTUAL TABLE sqlite_stat1 USING fts5(a);
} {}
do_catchsql_test 210 {
  CREATE VIRTUAL TABLE t2 USING rtree(id,x,y);
} {1 {no such column: stat}}
do_execsql_test 220 {
  SELECT * FROM t1;
} {Ebed-malech}

# Follow-on dbsqlfuzz a097eaad43c3c845b236126df92fb49b25449b0c
reset_db
do_catchsql_test 300 {
  CREATE VIRTUAL TABLE t1 USING rtree(a,b,c);
  CREATE TABLE t2(x);
  ALTER TABLE t2 ADD d GENERATED ALWAYS AS (c IN (SELECT 1 FROM t1)) VIRTUAL;
} {1 {error in table t2 after add column: subqueries prohibited in generated columns}}
  
finish_test

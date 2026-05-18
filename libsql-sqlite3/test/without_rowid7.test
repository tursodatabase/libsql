# 2019 July 17
#
# The author disclaims copyright to this source code.  In place of
# a legal notice, here is a blessing:
#
#    May you do good and not evil.
#    May you find forgiveness for yourself and forgive others.
#    May you share freely, never taking more than you give.
#
#*************************************************************************
# This file implements regression tests for SQLite library.  
#

set testdir [file dirname $argv0]
source $testdir/tester.tcl
set testprefix without_rowid7

proc do_execsql_test_if_vtab {tn sql {res {}}} {
  ifcapable vtab { uplevel [list do_execsql_test $tn $sql $res] }
}

do_execsql_test 1.0 {
  CREATE TABLE t1(a, b COLLATE nocase, PRIMARY KEY(a, a, b)) WITHOUT ROWID;
}

do_catchsql_test 1.1 {
  INSERT INTO t1 VALUES(1, 'one'), (1, 'ONE');
} {1 {UNIQUE constraint failed: t1.a, t1.b}}


do_execsql_test 2.0 {
  CREATE TABLE t2(a, b, PRIMARY KEY(a COLLATE nocase, a)) WITHOUT ROWID;
}

do_execsql_test 2.1 {
  INSERT INTO t2 VALUES(1, 'one');
  SELECT b FROM t2;
} {one}

do_execsql_test 2.2a {
  PRAGMA index_info(t2);
} {0 0 a 1 0 a}
do_execsql_test_if_vtab 2.2b {
  SELECT *, '|' FROM pragma_index_info('t2');
} {0 0 a | 1 0 a |}
do_execsql_test 2.3a {
  PRAGMA index_xinfo(t2);
} {0 0 a 0 nocase 1 1 0 a 0 BINARY 1 2 1 b 0 BINARY 0}
do_execsql_test_if_vtab 2.3b {
  SELECT *, '|' FROM pragma_index_xinfo('t2');
} {0 0 a 0 nocase 1 | 1 0 a 0 BINARY 1 | 2 1 b 0 BINARY 0 |}

do_execsql_test 2.4 {
  CREATE TABLE t3(a, b, PRIMARY KEY(a COLLATE nocase, a));
  PRAGMA index_info(t3);
} {}

#-------------------------------------------------------------------------
reset_db
db collate mysort mysort
db collate mysort2 mysort
proc mysort {a b} { string compare $a $b }
do_execsql_test 3.0 {
  CREATE TABLE t1(
      a PRIMARY KEY COLLATE mysort, b COLLATE mysort2
  ) WITHOUT ROWID;
  INSERT INTO t1 VALUES(1, 2);
}

db close
sqlite3 db test.db

do_catchsql_test 3.1.1 {
  SELECT * FROM t1 WHERE a=1;
} {1 {no such collation sequence: mysort}}
do_test 3.1.2 {
  sqlite3_extended_errcode db
} {SQLITE_ERROR_MISSING_COLLSEQ}

db collate mysort mysort

do_catchsql_test 3.2.1 {
  CREATE UNIQUE INDEX i1 ON t1(b);
} {1 {no such collation sequence: mysort2}}
do_test 3.2.2 {
  sqlite3_extended_errcode db
} {SQLITE_ERROR_MISSING_COLLSEQ}

db close
sqlite3 db test.db

do_catchsql_test 3.3.1 {
  CREATE UNIQUE INDEX i1 ON t1(1);
} {1 {no such collation sequence: mysort}}
do_test 3.3.2 {
  sqlite3_extended_errcode db
} {SQLITE_ERROR_MISSING_COLLSEQ}

do_test 3.4.1 {
  list [catch {
    sqlite3_prepare_v3 db "CREATE UNIQUE INDEX i1 ON t1(1)" -1 0
  } msg] $msg
} {1 {(1) no such collation sequence: mysort}}
do_test 3.4.2 {
  sqlite3_extended_errcode db
} {SQLITE_ERROR_MISSING_COLLSEQ}

sqlite3_extended_result_codes db 1

do_test 3.5.1 {
  list [catch {
    sqlite3_prepare_v3 db "CREATE UNIQUE INDEX i1 ON t1(1)" -1 0
  } msg] $msg
} {1 {(257) no such collation sequence: mysort}}
do_test 3.5.2 {
  sqlite3_extended_errcode db
} {SQLITE_ERROR_MISSING_COLLSEQ}

do_catchsql_test 3.6 {
  SELECT * FROM t1 WHERE a=1;
} {1 {no such collation sequence: mysort}}

finish_test

# 2023 December 16
#
#    May you do good and not evil.
#    May you find forgiveness for yourself and forgive others.
#    May you share freely, never taking more than you give.
#
#***********************************************************************
# This file runs all tests.
#
# $Id: fts3.test,v 1.2 2008/07/23 18:17:32 drh Exp $

set testdir [file dirname $argv0]
source $testdir/tester.tcl
set ::testprefix fts3integrity

# If SQLITE_ENABLE_FTS3 is defined, omit this file.
ifcapable !fts3 {
  finish_test
  return
}
  
do_execsql_test 1.0 {
  CREATE VIRTUAL TABLE t1 USING fts3(x);
  INSERT INTO t1 VALUES('first row');
  INSERT INTO t1 VALUES('second row');

  CREATE TABLE t2(x PRIMARY KEY);
  INSERT INTO t2 VALUES('first row');
  INSERT INTO t2 VALUES('second row');
}

sqlite3 db2 test.db

do_execsql_test -db db2 1.1 {
  CREATE TABLE t3(x, y);
}

do_execsql_test 1.2 {
  PRAGMA integrity_check;
} {ok}

finish_test

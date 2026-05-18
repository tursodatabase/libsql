# 2022 August 9
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
# This test is focused on edge cases that cause ubsan errors.
#

source [file join [file dirname [info script]] fts5_common.tcl]
set testprefix fts5ubsan

# If SQLITE_ENABLE_FTS5 is not defined, omit this file.
ifcapable !fts5 {
  finish_test
  return
}

do_execsql_test 1.0 {
  CREATE VIRTUAL TABLE x1 USING fts5(x);
}

set BIG    9000000000000000000
set SMALL -9000000000000000000

do_execsql_test 1.1 {
  BEGIN;
    INSERT INTO x1 (rowid, x) VALUES($BIG,   'aaa aba acc');
    INSERT INTO x1 (rowid, x) VALUES($SMALL, 'aaa abc acb');
  COMMIT;
}

do_execsql_test 1.2 {
  SELECT rowid, x FROM x1('ab*');
} [list $SMALL {aaa abc acb} $BIG {aaa aba acc}]

do_execsql_test 1.3 {
  SELECT rowid, x FROM x1('ac*');
} [list $SMALL {aaa abc acb} $BIG {aaa aba acc}]

reset_db
do_execsql_test 2.0 {
  CREATE VIRTUAL TABLE x1 USING fts5(x);
}

do_execsql_test 2.1 {
  INSERT INTO x1 (rowid, x) VALUES($BIG,   'aaa aba acc');
  INSERT INTO x1 (rowid, x) VALUES($SMALL, 'aaa abc acb');
}

do_execsql_test 2.2 {
  INSERT INTO x1 (x1) VALUES('optimize');
}

finish_test

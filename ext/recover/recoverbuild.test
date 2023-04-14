# 2023 February 28
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
set testprefix recoverbuild


# The following tests verify that if the recovery extension is used with
# a build that does not support the sqlite_dbpage table, the error message
# is "no such table: sqlite_dbpage", and not something more generic.
#
reset_db
create_null_module db sqlite_dbpage
do_execsql_test 1.0 {
  CREATE TABLE t1(a INTEGER PRIMARY KEY, b TEXT);
  INSERT INTO t1 VALUES(123, 'one hundred and twenty three');
}

forcedelete test.db2
do_test 1.1 {
  set R [sqlite3_recover_init db main test.db2]
} {/sqlite_recover.*/}

do_test 1.2 {
  $R run
} {1}

do_test 1.3 {
  list [catch { $R finish } msg] $msg
} {1 {no such table: sqlite_dbpage}}

finish_test


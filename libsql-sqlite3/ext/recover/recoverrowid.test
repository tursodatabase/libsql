# 2022 September 07
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
# Tests for the SQLITE_RECOVER_ROWIDS option.
#

source [file join [file dirname [info script]] recover_common.tcl]
set testprefix recoverrowid

proc recover {db bRowids output} {
  forcedelete $output

  set R [sqlite3_recover_init db main test.db2]
  $R config rowids $bRowids
  $R run
  $R finish
}

do_execsql_test 1.0 {
  CREATE TABLE t1(a, b);
  INSERT INTO t1 VALUES(1, 1), (2, 2), (3, 3), (4, 4);
  DELETE FROM t1 WHERE a IN (1, 3);
}

do_test 1.1 {
  recover db 0 test.db2
  sqlite3 db2 test.db2
  execsql { SELECT rowid, a, b FROM t1 ORDER BY rowid} db2
} {1 2 2 2 4 4}

do_test 1.2 {
  db2 close
  recover db 1 test.db2
  sqlite3 db2 test.db2
  execsql { SELECT rowid, a, b FROM t1 ORDER BY rowid} db2
} {2 2 2 4 4 4}
db2 close




finish_test

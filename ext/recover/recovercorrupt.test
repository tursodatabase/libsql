# 2022 August 28
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
set testprefix recovercorrupt

database_may_be_corrupt

do_execsql_test 1.0 {
  PRAGMA page_size = 512;
  CREATE TABLE t1(a INTEGER PRIMARY KEY, b, c);
  INSERT INTO t1 VALUES(1, 2, 3);
  INSERT INTO t1 VALUES(2, hex(randomblob(100)), randomblob(200));
  CREATE INDEX i1 ON t1(b, c);
  CREATE TABLE t2(a PRIMARY KEY, b, c) WITHOUT ROWID;
  INSERT INTO t2 VALUES(1, 2, 3);
  INSERT INTO t2 VALUES(2, hex(randomblob(100)), randomblob(200));
  ANALYZE;
  PRAGMA writable_schema = 1;
  DELETE FROM sqlite_schema WHERE name='t2';
}

do_test 1.1 {
  expr [file size test.db]>3072
} {1}

proc toggle_bit {blob bit} {
  set byte [expr {$bit / 8}]
  set bit [expr {$bit & 0x0F}]
  binary scan $blob a${byte}ca* A x B
  set x [expr {$x ^ (1 << $bit)}]
  binary format a*ca* $A $x $B
}


db_save_and_close
for {set ii 0} {$ii < 10000} {incr ii} {
  db_restore_and_reopen
  db func toggle_bit toggle_bit
  set bitsperpage [expr 512*8]

  set pg [expr {($ii / $bitsperpage)+1}]
  set byte [expr {$ii % $bitsperpage}]
  db eval {
    UPDATE sqlite_dbpage SET data = toggle_bit(data, $byte) WHERE pgno=$pg
  }

    set R [sqlite3_recover_init db main test.db2]
    $R config lostandfound lost_and_found
    $R run
  do_test 1.2.$ii {
    $R finish
  } {}
}


finish_test


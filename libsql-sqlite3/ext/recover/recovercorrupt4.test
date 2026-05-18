# 2024 May 15
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
set testprefix recovercorrupt4

database_may_be_corrupt

if {[permutation]!="inmemory_journal"} {
  # This test cannot be run with the inmemory_journal permutation, as it
  # must open a truncated, corrupt, database file. With the inmemory_journal
  # permutation, this fails (SQLITE_CORRUPT error) when the [sqlite3] wrapper
  # executes "PRAGMA journal_mode = memory".
  do_execsql_test 1.0 {
    CREATE TABLE rows(indexed INTEGER NOT NULL, unindexed INTEGER NOT NULL, filler BLOB NOT NULL DEFAULT 13);
    -- CREATE UNIQUE INDEX rows_index ON rows(indexed);
    INSERT INTO rows(indexed, unindexed, filler) VALUES(1, 1, x'31');
    INSERT INTO rows(indexed, unindexed, filler) VALUES(2, 2, x'32');
    INSERT INTO rows(indexed, unindexed, filler) VALUES(4, 4, x'34');
    INSERT INTO rows(indexed, unindexed, filler) VALUES(8, 8, randomblob(2048));
  }
  
  db close
  
  do_test 1.1 {
    set sz [expr [file size test.db] - 1024]
    set fd [open test.db]
    fconfigure $fd -translation binary
  
    set data [read $fd $sz]
    set fd2 [open test.db2 w]
    fconfigure $fd2 -translation binary
    puts -nonewline $fd2 $data
    close $fd2
    set {} {}
  } {}
  
  do_test 1.2 {
    forcedelete test.db3
    sqlite3 db test.db2
    set R [sqlite3_recover_init db main test.db3]
    $R run
    $R finish
  } {}
  
  do_test 1.3 {
    sqlite3 db test.db3
    execsql {
      SELECT indexed, unindexed FROM rows
    }
  } {1 1 2 2 4 4 8 8}
}

finish_test

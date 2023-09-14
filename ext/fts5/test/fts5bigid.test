# 2023 May 28
#
# The author disclaims copyright to this source code.  In place of
# a legal notice, here is a blessing:
#
#    May you do good and not evil.
#    May you find forgiveness for yourself and forgive others.
#    May you share freely, never taking more than you give.
#
#*************************************************************************
#

source [file join [file dirname [info script]] fts5_common.tcl]
set testprefix fts5bigid

# If SQLITE_ENABLE_FTS5 is not defined, omit this file.
ifcapable !fts5 {
  finish_test
  return
}

set nRow 20000

proc do_ascdesc_test {tn query} {
  set ::lAsc  [db eval { SELECT rowid FROM x1($query) }] 
  set ::lDesc [db eval { SELECT rowid FROM x1($query) ORDER BY rowid DESC }] 
  do_test $tn.1 { lsort -integer $::lAsc } $::lAsc
  do_test $tn.2 { lsort -integer -decr $::lDesc } $::lDesc
  do_test $tn.3 { lsort -integer $::lDesc } $::lAsc
}

do_execsql_test 1.0 {
  CREATE VIRTUAL TABLE x1 USING fts5(a);
}

do_test 1.1 {
  for {set ii 0} {$ii < $nRow} {incr ii} {
    db eval {
      REPLACE INTO x1(rowid, a) VALUES(random(), 'movement at the station');
    }
  }
} {}

do_ascdesc_test 1.2 "the"

do_execsql_test 1.3 {
  DELETE FROM x1
}

do_test 1.4 {
  for {set ii 0} {$ii < $nRow} {incr ii} {
    db eval {
      INSERT INTO x1(rowid, a) VALUES(
          $ii + 0x6FFFFFFFFFFFFFFF, 'movement at the station'
      );
    }
  }
} {}

do_ascdesc_test 1.5 "movement"

finish_test

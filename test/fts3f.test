# 2006 September 9
#
# The author disclaims copyright to this source code.  In place of
# a legal notice, here is a blessing:
#
#    May you do good and not evil.
#    May you find forgiveness for yourself and forgive others.
#    May you share freely, never taking more than you give.
#
#*************************************************************************
# This file implements regression tests for SQLite library.  The
# focus of this script is testing the FTS3 module.
#
# $Id: fts3aa.test,v 1.1 2007/08/20 17:38:42 shess Exp $
#

set testdir [file dirname $argv0]
source $testdir/tester.tcl
set testprefix fts3f

# If SQLITE_ENABLE_FTS3 is defined, omit this file.
ifcapable !fts3 {
  finish_test
  return
}

do_execsql_test 1.0 {
  CREATE VIRTUAL TABLE ft USING fts3(x);
  BEGIN;
    INSERT INTO ft VALUES('a one'), ('b one'), ('c one');
}

do_test 1.1 {
  set ret [list]
  db eval { SELECT docid FROM ft WHERE ft MATCH 'one' } {
    if { $docid==2 } {
      db eval COMMIT
    }
    lappend ret $docid
  }
  set ret
} {1 2 3}

do_execsql_test 1.2 {
  BEGIN;
    INSERT INTO ft VALUES('a one'), ('b one'), ('c one');
}

do_execsql_test 1.3 {
  SELECT docid, optimize(ft) FROM ft WHERE ft MATCH 'one'
} {
  1 {Index optimized} 2 {Index already optimal} 3 {Index already optimal}
  4 {Index already optimal}
  5 {Index already optimal} 6 {Index already optimal}
}

finish_test

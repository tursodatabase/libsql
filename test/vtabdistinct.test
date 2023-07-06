# 2022-01-21
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
# This file implements tests for sqlite3_vtab_distinct() interface.
#

set testdir [file dirname $argv0]
source $testdir/tester.tcl
set testprefix vtabdistinct

ifcapable !vtab {
  finish_test
  return
}
load_static_extension db qpvtab

do_execsql_test 1.1 {
  SELECT ix FROM qpvtab WHERE vn='sqlite3_vtab_distinct';
} {0}
do_execsql_test 1.2 {
  SELECT DISTINCT ix FROM qpvtab WHERE vn='sqlite3_vtab_distinct';
} {2}
do_execsql_test 1.3 {
  SELECT distinct vn, ix FROM qpvtab(3)
   WHERE +vn IN ('sqlite3_vtab_distinct','nOrderBy');
} {nOrderBy 2 sqlite3_vtab_distinct 2}
do_execsql_test 1.4 {
  SELECT vn, ix FROM qpvtab
   GROUP BY vn
  HAVING vn='sqlite3_vtab_distinct';
} {sqlite3_vtab_distinct 1}

finish_test

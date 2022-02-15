# 2022-01-20
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
# This file implements tests for sqlite3_vtab_rhs_value() interface.
#

set testdir [file dirname $argv0]
source $testdir/tester.tcl
set testprefix vtabrhs1

ifcapable !vtab {
  finish_test
  return
}
load_static_extension db qpvtab

# EVIDENCE-OF: R-60223-49197 When the sqlite3_vtab_rhs_value(P,J,V)
# interface is invoked from within the xBestIndex method of a virtual
# table implementation, with P being a copy of the sqlite3_index_info
# object pointer passed into xBestIndex and J being a 0-based index into
# P->aConstraint[], then this routine attempts to set *V to the value
# of the right-hand operand of that constraint if the right-hand operand
# is known.
#
do_execsql_test 1.1 {
  SELECT rhs FROM qpvtab
   WHERE cn='a'
     AND a=12345
} {12345}
do_execsql_test 1.2 {
  SELECT rhs FROM qpvtab
   WHERE cn='a'
     AND a<>4.5
} {4.5}
do_execsql_test 1.3 {
  SELECT rhs FROM qpvtab
   WHERE cn='a'
     AND 'quokka' < a
} {'quokka'}
do_execsql_test 1.4 {
  SELECT rhs FROM qpvtab
   WHERE cn='a'
     AND a IS NULL
} {{}}
do_execsql_test 1.5 {
  SELECT rhs FROM qpvtab
   WHERE cn='a'
     AND a GLOB x'0123'
} {x'0123'}

# EVIDENCE-OF: R-37799-62852 If the right-hand operand is not known,
# then *V is set to a NULL pointer.
#
do_execsql_test 2.1 {
  SELECT typeof(rhs) FROM qpvtab WHERE cn='a' AND a=format('abc');
} {null}
do_execsql_test 2.2 {
  SELECT typeof(rhs) FROM qpvtab WHERE cn='a' AND a=?2
} {null}

# EVIDENCE-OF: R-14553-25174 When xBestIndex returns, the sqlite3_value
# object returned by sqlite3_vtab_rhs_value() is automatically
# deallocated.
#
# Where this not the case, the following "finish_test" statement would
# report a memory leak.
#
finish_test

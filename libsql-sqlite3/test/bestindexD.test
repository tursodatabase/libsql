# 2024-08-03
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
#

set testdir [file dirname $argv0]
source $testdir/tester.tcl
set testprefix bestindexD

ifcapable !vtab {
  finish_test
  return
}

register_tcl_module db

proc vtab_command {method args} {
  switch -- $method {
    xConnect {
      return "CREATE TABLE t1(a PRIMARY KEY, b, c) WITHOUT ROWID"
    }

    xBestIndex {
      set hdl [lindex $args 0]
      set ::colUsed [$hdl mask]

      set cost 1000000
      set used ""

      set cons 0
      foreach c [$hdl constraints] {
        set cost [expr $cost/10]
        append used " use $cons"
        incr cons
      }

      return "cost $cost rows $cost $used"
    }
  }

  return {}
}

do_execsql_test 1.0 {
  CREATE VIRTUAL TABLE x1 USING tcl(vtab_command);

  CREATE TABLE t2(a, b);
} {}

# This proc assumes that there is only one use of a virtual table - x1 -
# in SQL statement $sql. It tests that the colUsed value passed to the
# xBestIndex method matches the actual columns used, which is ascertained 
# by searching the compiled VM code for VColumn instructions.
#
proc do_colsused_test {tn sql} {
  set ::colUsed ""
  execsql $sql
  set got $::colUsed

  set expect 0
  db eval "EXPLAIN $sql" x {
    if {$x(opcode)=="VColumn"} {
      set expect [expr $expect | (1<<$x(p2))]
    }
  }

  uplevel [list do_test $tn.($expect/$got) [list expr ($expect & $got)] $expect]
}

do_colsused_test 1.1 { SELECT a FROM x1 }   
do_colsused_test 1.2 { SELECT a,c FROM x1 } 
do_colsused_test 1.3 { SELECT b FROM x1 }   
do_colsused_test 1.4 { SELECT b FROM x1 WHERE c=? } 

do_colsused_test 1.5 {
  select 1 from t2 full join x1;
}

do_colsused_test 1.6 {
  select 1 from x1 WHERE (b=? AND c=?) OR (b=? AND c=?)
}

finish_test



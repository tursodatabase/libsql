# 2021 Februar 20
#
# The author disclaims copyright to this source code.  In place of
# a legal notice, here is a blessing:
#
#    May you do good and not evil.
#    May you find forgiveness for yourself and forgive others.
#    May you share freely, never taking more than you give.
#
#***********************************************************************
# This file implements regression tests for SQLite library.
#

if {![info exists testdir]} {
  set testdir [file join [file dirname [info script]] .. .. test]
} 
source [file join [file dirname [info script]] session_common.tcl]
source $testdir/tester.tcl
ifcapable !session {finish_test; return}

set testprefix sessionnoop

#-------------------------------------------------------------------------
# Test plan:
#
#   1.*: Test that concatenating changesets cannot produce a noop UPDATE.
#   2.*: Test that rebasing changesets cannot produce a noop UPDATE.
#   3.*: Test that sqlite3changeset_apply() ignores noop UPDATE changes.
#

do_execsql_test 1.0 {
  CREATE TABLE t1(a PRIMARY KEY, b, c, d);
  INSERT INTO t1 VALUES(1, 1, 1, 1);
  INSERT INTO t1 VALUES(2, 2, 2, 2);
  INSERT INTO t1 VALUES(3, 3, 3, 3);
}

proc do_concat_test {tn sql1 sql2 res} {
  uplevel [list do_test $tn [subst -nocommands {
    set C1 [changeset_from_sql {$sql1}]
    set C2 [changeset_from_sql {$sql2}]
    set C3 [sqlite3changeset_concat [set C1] [set C2]]
    set got [list]
    sqlite3session_foreach elem [set C3] { lappend got [set elem] }
    set got
  }] [list {*}$res]]
}

do_concat_test 1.1 {
  UPDATE t1 SET c=c+1;
} {
  UPDATE t1 SET c=c-1;
} {
}

#-------------------------------------------------------------------------
reset_db
do_execsql_test 2.0 {
  CREATE TABLE t1(a PRIMARY KEY, b, c);
  INSERT INTO t1 VALUES(1, 1, 1);
  INSERT INTO t1 VALUES(2, 2, 2);
  INSERT INTO t1 VALUES(3, 3, 3);
}

proc do_rebase_test {tn sql_local sql_remote conflict_res expected} {
  proc xConflict {args} [list return $conflict_res]

  uplevel [list \
    do_test $tn [subst -nocommands {
      execsql BEGIN
        set c_remote [changeset_from_sql {$sql_remote}]
      execsql ROLLBACK

      execsql BEGIN
        set c_local [changeset_from_sql {$sql_local}]
        set base [sqlite3changeset_apply_v2 db [set c_remote] xConflict]
      execsql ROLLBACK

      sqlite3rebaser_create R
      R config [set base]
      set res [list]
      sqlite3session_foreach elem [R rebase [set c_local]] { 
        lappend res [set elem] 
      }
      R delete
      set res
    }] [list {*}$expected]
  ]
}

do_rebase_test 2.1 {
  UPDATE t1 SET c=2 WHERE a=1;              -- local
} {
  UPDATE t1 SET c=3 WHERE a=1;              -- remote
} OMIT {
  {UPDATE t1 0 X.. {i 1 {} {} i 3} {{} {} {} {} i 2}}
}

do_rebase_test 2.2 {
  UPDATE t1 SET c=2 WHERE a=1;              -- local
} {
  UPDATE t1 SET c=3 WHERE a=1;              -- remote
} REPLACE {
}

do_rebase_test 2.3.1 {
  UPDATE t1 SET c=4 WHERE a=1;              -- local
} {
  UPDATE t1 SET c=4 WHERE a=1               -- remote
} OMIT {
  {UPDATE t1 0 X.. {i 1 {} {} i 4} {{} {} {} {} i 4}}
}

do_rebase_test 2.3.2 {
  UPDATE t1 SET c=5 WHERE a=1;              -- local
} {
  UPDATE t1 SET c=5 WHERE a=1               -- remote
} REPLACE {
}

#-------------------------------------------------------------------------
#
reset_db
do_execsql_test 3.0 {
  CREATE TABLE t1(a INTEGER PRIMARY KEY, b, c);
  INSERT INTO t1 VALUES(1, 1, 1);
  INSERT INTO t1 VALUES(2, 2, 2);
  INSERT INTO t1 VALUES(3, 3, 3);
  INSERT INTO t1 VALUES(4, 4, 4);
}

# Arg $pkstr contains one character for each column in the table. An
# "X" for PK column, or a "." for a non-PK.
#
proc mk_tbl_header {name pkstr} {
  set ret [binary format H2c 54 [string length $pkstr]]
  foreach i [split $pkstr {}] {
    if {$i=="X"} {
      append ret [binary format H2 01]
    } else {
      if {$i!="."} {error "bad pkstr: $pkstr ($i)"}
      append ret [binary format H2 00]
    }
  }
  append ret $name
  append ret [binary format H2 00]
  set ret
}

proc mk_update_change {args} {
  set ret [binary format H2H2 17 00]
  foreach a $args {
    if {$a==""} {
      append ret [binary format H2 00]
    } else {
      append ret [binary format H2W 01 $a]
    }
  }
  set ret
}

proc xConflict {args} { return "ABORT" }
do_test 3.1 {
  set    C [mk_tbl_header t1 X..]
  append C [mk_update_change    1 {} 1   {} {}  500]
  append C [mk_update_change    2 {} {}  {} {}  {}]
  append C [mk_update_change    3 3  {}  {} 600 {}]
  append C [mk_update_change    4 {} {}  {} {}  {}]

  sqlite3changeset_apply_v2 db $C xConflict
} {}
do_execsql_test 3.2 {
  SELECT * FROM t1
} {
  1 1 500
  2 2 2
  3 600 3
  4 4 4
}






finish_test


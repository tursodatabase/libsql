# 2025-09-02
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
set testprefix bestindexE

ifcapable !vtab {
  finish_test
  return
}

register_tcl_module db

proc pretty_constraint {lCol cons} {
  array set C $cons

  set ret ""
  if {$C(usable)} {
    set OP(eq) =
    set ret "[lindex $lCol $C(column)]$OP($C(op))?"
  }

  return $ret
}

proc vtab_command {zName lCol method args} {
  switch -- $method {
    xConnect {
      return "CREATE TABLE $zName ([join $lCol ,]) "
    }

    xBestIndex {
      set hdl [lindex $args 0]
      set conslist [$hdl constraints]

      set ret [list]
      foreach cons $conslist {
        set pretty [pretty_constraint $lCol $cons]
        if {$pretty != ""} { lappend ret $pretty }
      }

      lappend ::xBestIndex "$zName: [join $ret { AND }]"

      return "cost 1000 rows 1000 idxnum 555"
    }
  }

  return {}
}

proc do_bestindex_test {tn sql lCons} {
  set ::xBestIndex [list]
  do_execsql_test $tn.1 $sql
  uplevel [list do_test $tn.2 [list set ::xBestIndex] [list {*}$lCons]]
}

proc create_vtab {tname clist} {
  set cmd [list vtab_command $tname $clist]
  execsql "
    CREATE VIRTUAL TABLE $tname USING tcl('$cmd')
  "
}

do_test 1.0 {
  create_vtab x1 {a b c}
} {}

do_bestindex_test 1.1 {
  SELECT * FROM x1 WHERE a=?
} {{x1: a=?}}

do_bestindex_test 1.2 {
  SELECT * FROM x1 WHERE a=? AND b=?
} {{x1: a=? AND b=?}}

#--------------------------------------------------------------------------
reset_db
register_tcl_module db

do_test 2.0 {
  create_vtab Delivery       {id customer}
  create_vtab ReturnDelivery {id customer}
  create_vtab Customer       {oid name}
} {}

do_bestindex_test 2.1 {
  SELECT Delivery.ID, Customer.Name
  FROM Delivery LEFT JOIN
  Customer ON Delivery.Customer = Customer.OID
} {
  {Delivery: }
  {Customer: oid=?}
}

do_bestindex_test 2.2 {
  SELECT * FROM
  (
     SELECT Delivery.ID, Customer.Name
     FROM Delivery LEFT JOIN
     Customer ON Delivery.Customer = Customer.OID

     UNION

     SELECT ReturnDelivery.ID, Customer.Name
     FROM ReturnDelivery LEFT JOIN
     Customer ON ReturnDelivery.Customer = Customer.OID
  )
  WHERE ID = 1
} {
  {Delivery: id=?} 
  {Customer: oid=?} 
  {ReturnDelivery: id=?} 
  {Customer: oid=?}
}

#--------------------------------------------------------------------------
reset_db
register_tcl_module db eponymous_cmd

proc eponymous_cmd {method args} {
  switch -- $method {
    xConnect {
      db eval { SELECT * FROM sqlite_schema }
      return "CREATE TABLE t1 (a, b)"
    }

    xBestIndex {
      return "idxnum 555"
    }

    xFilter {
      return [list sql {SELECT 123, 'A', 'B'}]
    }

    xUpdate {
      return 123
    }

  }

  return {}
}

do_execsql_test 3.1.0 {
  PRAGMA table_info = tcl
} {
  0 a {} 0 {} 0 1 b {} 0 {} 0
}
do_execsql_test 3.1.1 {
  SELECT rowid, * FROM tcl
} {123 A B}
do_execsql_test 3.1.2 {
  INSERT INTO tcl VALUES('i', 'ii') RETURNING *;
} {i ii}
do_test 3.1.3 {
  db last_insert_rowid
} {123}

do_execsql_test 3.1.4 {
  CREATE TABLE x1(x);
}

db close
sqlite3 db  test.db
register_tcl_module db eponymous_cmd
sqlite3 db2 test.db

# Load the schema into connection [db]
do_execsql_test 3.2.1 { SELECT * FROM x1 }

# Modify the schema on disk
do_execsql_test -db db2 3.2.2 { CREATE TABLE x2(x) }

# Insert with RETURNING trigger on eponymous table.
do_execsql_test 3.2.3 {
  INSERT INTO tcl VALUES('i', 'ii') RETURNING *;
} {i ii}

finish_test


# 2024 Feb 18
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

if {![info exists testdir]} {
  set testdir [file join [file dirname [info script]] .. .. test]
}
source $testdir/tester.tcl

ifcapable !vtab||!pragma {
  proc return_if_no_intck {} {
    finish_test
    return -code return
  }
  return
} else {
  proc return_if_no_intck {} {}
}

proc do_intck {db {bSuspend 0}} {
  set ic [sqlite3_intck $db main]

  set ret [list]
  while {"SQLITE_OK"==[$ic step]} {
    set msg [$ic message]
    if {$msg!=""} {
      lappend ret $msg
    }
    if {$bSuspend} { 
      $ic unlock 
      #puts "SQL: [$ic test_sql {}]"
      #execsql_pp "EXPLAIN query plan [$ic test_sql {}]"
      #explain_i [$ic test_sql {}]
    }
  }

  set err [$ic error]
  if {[lindex $err 0]!="SQLITE_OK"} {
    error $err
  }
  $ic close

  return $ret
}

proc intck_sql {db tbl} {
  set ic [sqlite3_intck $db main]
  set sql [$ic test_sql $tbl]
  $ic close
  return $sql
}

proc do_intck_test {tn expect} {
  uplevel [list do_test $tn.a [list do_intck db] [list {*}$expect]]
  uplevel [list do_test $tn.b [list do_intck db 1] [list {*}$expect]]
}



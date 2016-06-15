# 2015 Aug 8
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

# Run the RBU in file $rbu on target database $target until completion.
#
proc run_rbu {target rbu} {
  sqlite3rbu rbu $target $rbu
  while 1 {
    set rc [rbu step]
    if {$rc!="SQLITE_OK"} break
  }
  rbu close
}

proc step_rbu {target rbu} {
  while 1 {
    sqlite3rbu rbu $target $rbu
    set rc [rbu step]
    rbu close
    if {$rc != "SQLITE_OK"} break
  }
  set rc
}

proc do_rbu_vacuum_test {tn step} {
  uplevel [list do_test $tn.1 {
    if {$step==0} { sqlite3rbu_vacuum rbu test.db state.db }
    while 1 {
      if {$step==1} { sqlite3rbu_vacuum rbu test.db state.db }
      set rc [rbu step]
      if {$rc!="SQLITE_OK"} break
      if {$step==1} { rbu close }
    }
    rbu close
  } {SQLITE_DONE}]

  uplevel [list do_execsql_test $tn.2 {
    PRAGMA integrity_check
  } ok]
}


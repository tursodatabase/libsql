

if {![info exists testdir]} {
  set testdir [file join [file dirname [info script]] .. .. test]
} 
source $testdir/tester.tcl

if {[info commands sqlite3_recover_init]==""} {
  finish_test
  return -code return
}




# 2001 September 15
#
# The author disclaims copyright to this source code.  In place of
# a legal notice, here is a blessing:
#
#    May you do good and not evil.
#    May you find forgiveness for yourself and forgive others.
#    May you share freely, never taking more than you give.
#
#***********************************************************************
# This file implements some common TCL routines used for regression
# testing the SQLite library
#
# $Id: tester.tcl,v 1.38 2004/08/20 18:34:20 drh Exp $

# Make sure tclsqlite3 was compiled correctly.  Abort now with an
# error message if not.
#
if {[sqlite3 -tcl-uses-utf]} {
  if {"\u1234"=="u1234"} {
    puts stderr "***** BUILD PROBLEM *****"
    puts stderr "$argv0 was linked against an older version"
    puts stderr "of TCL that does not support Unicode, but uses a header"
    puts stderr "file (\"tcl.h\") from a new TCL version that does support"
    puts stderr "Unicode.  This combination causes internal errors."
    puts stderr "Recompile using a TCL library and header file that match"
    puts stderr "and try again.\n**************************"
    exit 1
  }
} else {
  if {"\u1234"!="u1234"} {
    puts stderr "***** BUILD PROBLEM *****"
    puts stderr "$argv0 was linked against an newer version"
    puts stderr "of TCL that supports Unicode, but uses a header file"
    puts stderr "(\"tcl.h\") from a old TCL version that does not support"
    puts stderr "Unicode.  This combination causes internal errors."
    puts stderr "Recompile using a TCL library and header file that match"
    puts stderr "and try again.\n**************************"
    exit 1
  }
}

set tcl_precision 15

# Use the pager codec if it is available
#
if {[sqlite3 -has-codec] && [info command sqlite_orig]==""} {
  rename sqlite3 sqlite_orig
  proc sqlite3 {args} {
    if {[llength $args]==2 && [string index [lindex $args 0] 0]!="-"} {
      lappend args -key {xyzzy}
    }
    uplevel 1 sqlite_orig $args
  }
}


# Create a test database
#
catch {db close}
file delete -force test.db
file delete -force test.db-journal
sqlite3 db ./test.db
if {[info exists ::SETUP_SQL]} {
  db eval $::SETUP_SQL
}

# Abort early if this script has been run before.
#
if {[info exists nTest]} return

# Set the test counters to zero
#
set nErr 0
set nTest 0
set nProb 0
set skip_test 0
set failList {}
set maxErr 1000

# Invoke the do_test procedure to run a single test 
#
proc do_test {name cmd expected} {
  global argv nErr nTest skip_test maxErr
  if {$skip_test} {
    set skip_test 0
    return
  }
  if {[llength $argv]==0} { 
    set go 1
  } else {
    set go 0
    foreach pattern $argv {
      if {[string match $pattern $name]} {
        set go 1
        break
      }
    }
  }
  if {!$go} return
  incr nTest
  puts -nonewline $name...
  flush stdout
  if {[catch {uplevel #0 "$cmd;\n"} result]} {
    puts "\nError: $result"
    incr nErr
    lappend ::failList $name
    if {$nErr>$maxErr} {puts "*** Giving up..."; finalize_testing}
  } elseif {[string compare $result $expected]} {
    puts "\nExpected: \[$expected\]\n     Got: \[$result\]"
    incr nErr
    lappend ::failList $name
    if {$nErr>=$maxErr} {puts "*** Giving up..."; finalize_testing}
  } else {
    puts " Ok"
  }
}

# The procedure uses the special "sqlite_malloc_stat" command
# (which is only available if SQLite is compiled with -DMEMORY_DEBUG=1)
# to see how many malloc()s have not been free()ed.  The number
# of surplus malloc()s is stored in the global variable $::Leak.
# If the value in $::Leak grows, it may mean there is a memory leak
# in the library.
#
proc memleak_check {} {
  if {[info command sqlite_malloc_stat]!=""} {
    set r [sqlite_malloc_stat]
    set ::Leak [expr {[lindex $r 0]-[lindex $r 1]}]
  }
}

# Run this routine last
#
proc finish_test {} {
  finalize_testing
}
proc finalize_testing {} {
  global nTest nErr nProb sqlite_open_file_count
  if {$nErr==0} memleak_check
  catch {db close}
  puts "$nErr errors out of $nTest tests"
  puts "Failures on these tests: $::failList"
  if {$nProb>0} {
    puts "$nProb probabilistic tests also failed, but this does"
    puts "not necessarily indicate a malfunction."
  }
  if 0 {
  if {$sqlite_open_file_count} {
    puts "$sqlite_open_file_count files were left open"
    incr nErr
  }
  }
  exit [expr {$nErr>0}]
}

# A procedure to execute SQL
#
proc execsql {sql {db db}} {
  # puts "SQL = $sql"
  return [$db eval $sql]
}

# Execute SQL and catch exceptions.
#
proc catchsql {sql {db db}} {
  # puts "SQL = $sql"
  set r [catch {$db eval $sql} msg]
  lappend r $msg
  return $r
}

# Do an VDBE code dump on the SQL given
#
proc explain {sql {db db}} {
  puts ""
  puts "addr  opcode        p1       p2     p3             "
  puts "----  ------------  ------  ------  ---------------"
  $db eval "explain $sql" {} {
    puts [format {%-4d  %-12.12s  %-6d  %-6d  %s} $addr $opcode $p1 $p2 $p3]
  }
}

# Another procedure to execute SQL.  This one includes the field
# names in the returned list.
#
proc execsql2 {sql} {
  set result {}
  db eval $sql data {
    foreach f $data(*) {
      lappend result $f $data($f)
    }
  }
  return $result
}

# Use the non-callback API to execute multiple SQL statements
#
proc stepsql {dbptr sql} {
  set sql [string trim $sql]
  set r 0
  while {[string length $sql]>0} {
    if {[catch {sqlite3_prepare $dbptr $sql -1 sqltail} vm]} {
      return [list 1 $vm]
    }
    set sql [string trim $sqltail]
#    while {[sqlite_step $vm N VAL COL]=="SQLITE_ROW"} {
#      foreach v $VAL {lappend r $v}
#    }
    while {[sqlite3_step $vm]=="SQLITE_ROW"} {
      for {set i 0} {$i<[sqlite3_data_count $vm]} {incr i} {
        lappend r [sqlite3_column_text $vm $i]
      }
    }
    if {[catch {sqlite3_finalize $vm} errmsg]} {
      return [list 1 $errmsg]
    }
  }
  return $r
}

# Delete a file or directory
#
proc forcedelete {filename} {
  if {[catch {file delete -force $filename}]} {
    exec rm -rf $filename
  }
}

# Do an integrity check of the entire database
#
proc integrity_check {name} {
  do_test $name {
    execsql {PRAGMA integrity_check}
  } {ok}
}

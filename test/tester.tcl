# Copyright (c) 1999, 2000 D. Richard Hipp
#
# This program is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public
# License as published by the Free Software Foundation; either
# version 2 of the License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
# General Public License for more details.
# 
# You should have received a copy of the GNU General Public
# License along with this library; if not, write to the
# Free Software Foundation, Inc., 59 Temple Place - Suite 330,
# Boston, MA  02111-1307, USA.
#
# Author contact information:
#   drh@hwaci.com
#   http://www.hwaci.com/drh/
#
#***********************************************************************
# This file implements some common TCL routines used for regression
# testing the SQLite library
#
# $Id: tester.tcl,v 1.16 2001/09/13 21:53:10 drh Exp $

# Make sure tclsqlite was compiled correctly.  Abort now with an
# error message if not.
#
if {[sqlite -tcl-uses-utf]} {
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

# Create a test database
#
file delete -force ./test.db
file delete -force ./test.db-journal
sqlite db ./test.db

# Abort early if this script has been run before.
#
if {[info exists nTest]} return

# Set the test counters to zero
#
set nErr 0
set nTest 0
set nProb 0
set skip_test 0

# Invoke the do_test procedure to run a single test 
#
proc do_test {name cmd expected} {
  global argv nErr nTest skip_test
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
    if {$nErr>10} {puts "*** Giving up..."; exit 1}
  } elseif {[string compare $result $expected]} {
    puts "\nExpected: \[$expected\]\n     Got: \[$result\]"
    incr nErr
    if {$nErr>10} {puts "*** Giving up..."; exit 1}
  } else {
    puts " Ok"
  }
}

# Invoke this procedure on a test that is probabilistic
# and might fail sometimes.
#
proc do_probtest {name cmd expected} {
  global argv nProb nTest skip_test
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
  } elseif {[string compare $result $expected]} {
    puts "\nExpected: \[$expected\]\n     Got: \[$result\]"
    puts "NOTE: The results of the previous test depend on system load"
    puts "and processor speed.  The test may sometimes fail even if the"
    puts "library is working correctly."
    incr nProb	
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
  global nTest nErr nProb
  memleak_check
  catch {db close}
  puts "$nErr errors out of $nTest tests"
  if {$nProb>0} {
    puts "$nProb probabilistic tests also failed, but this does"
    puts "not necessarily indicate a malfunction."
  }
  exit [expr {$nErr>0}]
}

# A procedure to execute SQL
#
proc execsql {sql {db db}} {
  # puts "SQL = $sql"
  return [$db eval $sql]
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

# Delete a file or directory
#
proc forcedelete {filename} {
  if {[catch {file delete -force $filename}]} {
    exec rm -rf $filename
  }
}

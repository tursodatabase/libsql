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
# $Id: tester.tcl,v 1.1 2000/05/29 20:41:51 drh Exp $

set nErr 0
set nTest 0

# Invoke the do_test procedure to run a single test 
#
proc do_test {name cmd expected} {
  global argv nErr nTest
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
    incr nErr
  } else {
    puts " Ok"
  }
}

# Run this routine last
#
proc finish_test {} {
  global nTest nErr
  puts "$nErr errors out of $nTest tests"
  exit $nErr
}

# Create a test database
#
file delete -force testdb
file mkdir testdb
sqlite db testdb

# A procedure to execute SQL
#
proc execsql {sql} {
  set result {}
  db eval $sql data {
    foreach f [lsort [array names data]] {
      if {$f=="*"} continue
      lappend result $data($f)
    }
  }
  return $result
}

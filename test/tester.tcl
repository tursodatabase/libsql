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
# $Id: tester.tcl,v 1.7 2000/10/19 14:10:09 drh Exp $

# Create a test database
#
if {![info exists dbprefix]} {
  if {[info exists env(SQLITE_PREFIX)]} {
    set dbprefix $env(SQLITE_PREFIX):
  } else {
    set dbprefix "gdbm:"
  }
}
switch $dbprefix {
  gdbm: {
   file delete -force testdb
   file mkdir testdb
  }
  memory: {
   # do nothing
  }
}
sqlite db ${dbprefix}testdb

# Abort early if this script has been run before.
#
if {[info exists nTest]} return

# Set the test counters to zero
#
set nErr 0
set nTest 0
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
  puts -nonewline $::dbprefix$name...
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

# Skip a test based on the dbprefix
#
proc skipif {args} {
  foreach a $args {
    if {$::dbprefix==$a} {
      set ::skip_test 1
      return
    }
  }
}

# Run the next test only if the dbprefix is among the listed arguments
#
proc testif {args} {
  foreach a $args {
    if {$::dbprefix==$a} {
      set ::skip_test 0
      return
    }
  }
  set ::skip_test 1
}

# Run this routine last
#
proc finish_test {} {
  global nTest nErr
  catch {db close}
  puts "$nErr errors out of $nTest tests"
  exit $nErr
}

# A procedure to execute SQL
#
proc execsql {sql} {
  return [db eval $sql]
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

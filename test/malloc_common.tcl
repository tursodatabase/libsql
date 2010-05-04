# 2007 May 05
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
# This file contains common code used by many different malloc tests
# within the test suite.
#
# $Id: malloc_common.tcl,v 1.22 2008/09/23 16:41:30 danielk1977 Exp $

# If we did not compile with malloc testing enabled, then do nothing.
#
ifcapable builtin_test {
  set MEMDEBUG 1
} else {
  set MEMDEBUG 0
  return 0
}

# Usage: do_malloc_test <test number> <options...>
#
# The first argument, <test number>, is an integer used to name the
# tests executed by this proc. Options are as follows:
#
#     -tclprep          TCL script to run to prepare test.
#     -sqlprep          SQL script to run to prepare test.
#     -tclbody          TCL script to run with malloc failure simulation.
#     -sqlbody          TCL script to run with malloc failure simulation.
#     -cleanup          TCL script to run after the test.
#
# This command runs a series of tests to verify SQLite's ability
# to handle an out-of-memory condition gracefully. It is assumed
# that if this condition occurs a malloc() call will return a
# NULL pointer. Linux, for example, doesn't do that by default. See
# the "BUGS" section of malloc(3).
#
# Each iteration of a loop, the TCL commands in any argument passed
# to the -tclbody switch, followed by the SQL commands in any argument
# passed to the -sqlbody switch are executed. Each iteration the
# Nth call to sqliteMalloc() is made to fail, where N is increased
# each time the loop runs starting from 1. When all commands execute
# successfully, the loop ends.
#
proc do_malloc_test {tn args} {
  array unset ::mallocopts 
  array set ::mallocopts $args

  if {[string is integer $tn]} {
    set tn malloc-$tn
  }
  if {[info exists ::mallocopts(-start)]} {
    set start $::mallocopts(-start)
  } else {
    set start 0
  }
  if {[info exists ::mallocopts(-end)]} {
    set end $::mallocopts(-end)
  } else {
    set end 50000
  }
  save_prng_state

  foreach ::iRepeat {0 10000000} {
    set ::go 1
    for {set ::n $start} {$::go && $::n <= $end} {incr ::n} {

      # If $::iRepeat is 0, then the malloc() failure is transient - it
      # fails and then subsequent calls succeed. If $::iRepeat is 1, 
      # then the failure is persistent - once malloc() fails it keeps
      # failing.
      #
      set zRepeat "transient"
      if {$::iRepeat} {set zRepeat "persistent"}
      restore_prng_state
      foreach file [glob -nocomplain test.db-mj*] {file delete -force $file}

      do_test ${tn}.${zRepeat}.${::n} {
  
        # Remove all traces of database files test.db and test2.db 
        # from the file-system. Then open (empty database) "test.db" 
        # with the handle [db].
        # 
        catch {db close} 
        catch {file delete -force test.db}
        catch {file delete -force test.db-journal}
        catch {file delete -force test.db-wal}
        catch {file delete -force test2.db}
        catch {file delete -force test2.db-journal}
        catch {file delete -force test2.db-wal}
        if {[info exists ::mallocopts(-testdb)]} {
          file copy $::mallocopts(-testdb) test.db
        }
        catch { sqlite3 db test.db }
        if {[info commands db] ne ""} {
          sqlite3_extended_result_codes db 1
        }
        sqlite3_db_config_lookaside db 0 0 0
  
        # Execute any -tclprep and -sqlprep scripts.
        #
        if {[info exists ::mallocopts(-tclprep)]} {
          eval $::mallocopts(-tclprep)
        }
        if {[info exists ::mallocopts(-sqlprep)]} {
          execsql $::mallocopts(-sqlprep)
        }
  
        # Now set the ${::n}th malloc() to fail and execute the -tclbody 
        # and -sqlbody scripts.
        #
        sqlite3_memdebug_fail $::n -repeat $::iRepeat
        set ::mallocbody {}
        if {[info exists ::mallocopts(-tclbody)]} {
          append ::mallocbody "$::mallocopts(-tclbody)\n"
        }
        if {[info exists ::mallocopts(-sqlbody)]} {
          append ::mallocbody "db eval {$::mallocopts(-sqlbody)}"
        }

        # The following block sets local variables as follows:
        #
        #     isFail  - True if an error (any error) was reported by sqlite.
        #     nFail   - The total number of simulated malloc() failures.
        #     nBenign - The number of benign simulated malloc() failures.
        #
        set isFail [catch $::mallocbody msg]
        set nFail [sqlite3_memdebug_fail -1 -benigncnt nBenign]
        # puts -nonewline " (isFail=$isFail nFail=$nFail nBenign=$nBenign) "

        # If one or more mallocs failed, run this loop body again.
        #
        set go [expr {$nFail>0}]

        if {($nFail-$nBenign)==0} {
          if {$isFail} {
            set v2 $msg
          } else {
            set isFail 1
            set v2 1
          }
        } elseif {!$isFail} {
          set v2 $msg
        } elseif {
          [info command db]=="" || 
          [db errorcode]==7 ||
          $msg=="out of memory"
        } {
          set v2 1
        } else {
          set v2 $msg
          puts [db errorcode]
        }
        lappend isFail $v2
      } {1 1}
  
      if {[info exists ::mallocopts(-cleanup)]} {
        catch [list uplevel #0 $::mallocopts(-cleanup)] msg
      }
    }
  }
  unset ::mallocopts
  sqlite3_memdebug_fail -1
}


#-------------------------------------------------------------------------
# This proc is used to test a single SELECT statement. Parameter $name is
# passed a name for the test case (i.e. "fts3_malloc-1.4.1") and parameter
# $sql is passed the text of the SELECT statement. Parameter $result is
# set to the expected output if the SELECT statement is successfully
# executed using [db eval].
#
# Example:
#
#   do_select_test testcase-1.1 "SELECT 1+1, 1+2" {1 2}
#
# If global variable DO_MALLOC_TEST is set to a non-zero value, or if
# it is not defined at all, then OOM testing is performed on the SELECT
# statement. Each OOM test case is said to pass if either (a) executing
# the SELECT statement succeeds and the results match those specified
# by parameter $result, or (b) TCL throws an "out of memory" error.
#
# If DO_MALLOC_TEST is defined and set to zero, then the SELECT statement
# is executed just once. In this case the test case passes if the results
# match the expected results passed via parameter $result.
#
proc do_select_test {name sql result} {
  uplevel [list doPassiveTest 0 $name $sql [list 0 $result]]
}

proc do_restart_select_test {name sql result} {
  uplevel [list doPassiveTest 1 $name $sql [list 0 $result]]
}

proc do_error_test {name sql error} {
  uplevel [list doPassiveTest 0 $name $sql [list 1 $error]]
}

proc doPassiveTest {isRestart name sql catchres} {
  if {![info exists ::DO_MALLOC_TEST]} { set ::DO_MALLOC_TEST 1 }

  switch $::DO_MALLOC_TEST {
    0 { # No malloc failures.
      do_test $name [list set {} [uplevel [list catchsql $sql]]] $catchres
      return
    }
    1 { # Simulate transient failures.
      set nRepeat 1
      set zName "transient"
      set nStartLimit 100000
      set nBackup 1
    }
    2 { # Simulate persistent failures.
      set nRepeat 1
      set zName "persistent"
      set nStartLimit 100000
      set nBackup 1
    }
    3 { # Simulate transient failures with extra brute force.
      set nRepeat 100000
      set zName "ridiculous"
      set nStartLimit 1
      set nBackup 10
    }
  }

  # The set of acceptable results from running [catchsql $sql].
  #
  set answers [list {1 {out of memory}} $catchres]
  set str [join $answers " OR "]

  set nFail 1
  for {set iLimit $nStartLimit} {$nFail} {incr iLimit} {
    for {set iFail 1} {$nFail && $iFail<=$iLimit} {incr iFail} {
      for {set iTest 0} {$iTest<$nBackup && ($iFail-$iTest)>0} {incr iTest} {

        if {$isRestart} { sqlite3 db test.db }

        sqlite3_memdebug_fail [expr $iFail-$iTest] -repeat $nRepeat
        set res [uplevel [list catchsql $sql]]
        if {[lsearch -exact $answers $res]>=0} { set res $str }
        set testname "$name.$zName.$iFail"
        do_test "$name.$zName.$iLimit.$iFail" [list set {} $res] $str

        set nFail [sqlite3_memdebug_fail -1 -benigncnt nBenign]
      }
    }
  }
}


#-------------------------------------------------------------------------
# Test a single write to the database. In this case a  "write" is a 
# DELETE, UPDATE or INSERT statement.
#
# If OOM testing is performed, there are several acceptable outcomes:
#
#   1) The write succeeds. No error is returned.
#
#   2) An "out of memory" exception is thrown and:
#
#     a) The statement has no effect, OR
#     b) The current transaction is rolled back, OR
#     c) The statement succeeds. This can only happen if the connection
#        is in auto-commit mode (after the statement is executed, so this
#        includes COMMIT statements).
#
# If the write operation eventually succeeds, zero is returned. If a
# transaction is rolled back, non-zero is returned.
#
# Parameter $name is the name to use for the test case (or test cases).
# The second parameter, $tbl, should be the name of the database table
# being modified. Parameter $sql contains the SQL statement to test.
#
proc do_write_test {name tbl sql} {
  if {![info exists ::DO_MALLOC_TEST]} { set ::DO_MALLOC_TEST 1 }

  # Figure out an statement to get a checksum for table $tbl.
  db eval "SELECT * FROM $tbl" V break
  set cksumsql "SELECT md5sum([join [concat rowid $V(*)] ,]) FROM $tbl"

  # Calculate the initial table checksum.
  set cksum1 [db one $cksumsql]

  if {$::DO_MALLOC_TEST } {
    set answers [list {1 {out of memory}} {0 {}}]
    if {$::DO_MALLOC_TEST==1} {
      set modes {100000 transient}
    } else {
      set modes {1 persistent}
    }
  } else {
    set answers [list {0 {}}]
    set modes [list 0 nofail]
  }
  set str [join $answers " OR "]

  foreach {nRepeat zName} $modes {
    for {set iFail 1} 1 {incr iFail} {
      if {$::DO_MALLOC_TEST} {sqlite3_memdebug_fail $iFail -repeat $nRepeat}

      set res [uplevel [list catchsql $sql]]
      set nFail [sqlite3_memdebug_fail -1 -benigncnt nBenign]
      if {$nFail==0} {
        do_test $name.$zName.$iFail [list set {} $res] {0 {}}
        return
      } else {
        if {[lsearch $answers $res]>=0} {
          set res $str
        }
        do_test $name.$zName.$iFail [list set {} $res] $str
        set cksum2 [db one $cksumsql]
        if {$cksum1 != $cksum2} return
      }
    }
  }
}

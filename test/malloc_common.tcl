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
# $Id: malloc_common.tcl,v 1.18 2008/06/23 18:49:45 danielk1977 Exp $

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
        catch {file delete -force test2.db}
        catch {file delete -force test2.db-journal}
        if {[info exists ::mallocopts(-testdb)]} {
          file copy $::mallocopts(-testdb) test.db
        }
        catch { sqlite3 db test.db }
        if {[info commands db] ne ""} {
          sqlite3_extended_result_codes db 1
        }
  
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
          [db errorcode]==[expr 10+(12<<8)] ||
          $msg=="out of memory"
        } {
          set v2 1
        } else {
          set v2 $msg
          breakpoint
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

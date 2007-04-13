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
# $Id: tester.tcl,v 1.78 2007/04/13 03:23:21 drh Exp $

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
set sqlite_pending_byte 0x0010000

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
set ::DB [sqlite3_connection_pointer db]
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
set skip_test 0
set failList {}
set maxErr 1000
if {![info exists speedTest]} {
  set speedTest 0
}

# Invoke the do_test procedure to run a single test 
#
proc do_test {name cmd expected} {
  global argv nErr nTest skip_test maxErr
  set ::sqlite_malloc_id $name
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
  flush stdout
}

# Run an SQL script.  
# Return the number of microseconds per statement.
#
proc speed_trial {name numstmt units sql} {
  puts -nonewline [format {%-21.21s } $name...]
  flush stdout
  set speed [time {sqlite3_exec_nr db $sql}]
  set tm [lindex $speed 0]
  set rate [expr {1000000.0*$numstmt/$tm}]
  set u2 $units/s
  puts [format {%12d uS %20.5f %s} $tm $rate $u2]
  global total_time
  set total_time [expr {$total_time+$tm}]
}
proc speed_trial_init {name} {
  global total_time
  set total_time 0
}
proc speed_trial_summary {name} {
  global total_time
  puts [format {%-21.21s %12d uS TOTAL} $name $total_time]
}

# The procedure uses the special "sqlite_malloc_stat" command
# (which is only available if SQLite is compiled with -DSQLITE_DEBUG=1)
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
  global nTest nErr sqlite_open_file_count
  if {$nErr==0} memleak_check

  catch {db close}
  catch {db2 close}
  catch {db3 close}

  catch {
    pp_check_for_leaks
  }
  sqlite3 db {}
  # sqlite3_clear_tsd_memdebug
  db close
  if {$::sqlite3_tsd_count} {
     puts "Thread-specific data leak: $::sqlite3_tsd_count instances"
     incr nErr
  } else {
     puts "Thread-specific data deallocated properly"
  }
  incr nTest
  puts "$nErr errors out of $nTest tests"
  puts "Failures on these tests: $::failList"
  if {$nErr>0 && ![working_64bit_int]} {
    puts "******************************************************************"
    puts "N.B.:  The version of TCL that you used to build this test harness"
    puts "is defective in that it does not support 64-bit integers.  Some or"
    puts "all of the test failures above might be a result from this defect"
    puts "in your TCL build."
    puts "******************************************************************"
  }
  if {$sqlite_open_file_count} {
    puts "$sqlite_open_file_count files were left open"
    incr nErr
  }
  foreach f [glob -nocomplain test.db-*-journal] {
    file delete -force $f
  }
  foreach f [glob -nocomplain test.db-mj*] {
    file delete -force $f
  }
  exit [expr {$nErr>0}]
}

# A procedure to execute SQL
#
proc execsql {sql {db db}} {
  # puts "SQL = $sql"
  uplevel [list $db eval $sql]
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
  ifcapable integrityck {
    do_test $name {
      execsql {PRAGMA integrity_check}
    } {ok}
  }
}

# Evaluate a boolean expression of capabilities.  If true, execute the
# code.  Omit the code if false.
#
proc ifcapable {expr code {else ""} {elsecode ""}} {
  regsub -all {[a-z_0-9]+} $expr {$::sqlite_options(&)} e2
  if ($e2) {
    set c [catch {uplevel 1 $code} r]
  } else {
    set c [catch {uplevel 1 $elsecode} r]
  }
  return -code $c $r
}

# This proc execs a seperate process that crashes midway through executing
# the SQL script $sql on database test.db.
#
# The crash occurs during a sync() of file $crashfile. When the crash
# occurs a random subset of all unsynced writes made by the process are
# written into the files on disk. Argument $crashdelay indicates the
# number of file syncs to wait before crashing.
#
# The return value is a list of two elements. The first element is a
# boolean, indicating whether or not the process actually crashed or
# reported some other error. The second element in the returned list is the
# error message. This is "child process exited abnormally" if the crash
# occured.
#
#   crashsql -delay CRASHDELAY -file CRASHFILE ?-blocksize BLOCKSIZE $sql
#
proc crashsql {args} {
  if {$::tcl_platform(platform)!="unix"} {
    error "crashsql should only be used on unix"
  }

  set blocksize ""
  set crashdelay 1
  set crashfile ""
  set sql [lindex $args end]
  
  for {set ii 0} {$ii < [llength $args]-1} {incr ii 2} {
    set z [lindex $args $ii]
    set n [string length $z]
    set z2 [lindex $args [expr $ii+1]]

    if     {$n>1 && [string first $z -delay]==0}     {set crashdelay $z2} \
    elseif {$n>1 && [string first $z -file]==0}      {set crashfile $z2}  \
    elseif {$n>1 && [string first $z -blocksize]==0} {set blocksize $z2}  \
    else   { error "Unrecognized option: $z" }
  }

  if {$crashfile eq ""} {
    error "Compulsory option -file missing"
  }

  set cfile [file join [pwd] $crashfile]

  set f [open crash.tcl w]
  puts $f "sqlite3_crashparams $crashdelay $cfile $blocksize"
  puts $f "set sqlite_pending_byte $::sqlite_pending_byte"
  puts $f "sqlite3 db test.db"

  # This block sets the cache size of the main database to 10
  # pages. This is done in case the build is configured to omit
  # "PRAGMA cache_size".
  puts $f {db eval {SELECT * FROM sqlite_master;}}
  puts $f {set bt [btree_from_db db]}
  puts $f {btree_set_cache_size $bt 10}

  puts $f "db eval {"
  puts $f   "$sql"
  puts $f "}"
  close $f

  set r [catch {
    exec [info nameofexec] crash.tcl >@stdout
  } msg]
  lappend r $msg
}

# Usage: do_ioerr_test <test number> <options...>
#
# This proc is used to implement test cases that check that IO errors
# are correctly handled. The first argument, <test number>, is an integer 
# used to name the tests executed by this proc. Options are as follows:
#
#     -tclprep          TCL script to run to prepare test.
#     -sqlprep          SQL script to run to prepare test.
#     -tclbody          TCL script to run with IO error simulation.
#     -sqlbody          TCL script to run with IO error simulation.
#     -exclude          List of 'N' values not to test.
#     -erc              Use extended result codes
#     -persist          Make simulated I/O errors persistent
#     -start            Value of 'N' to begin with (default 1)
#
#     -cksum            Boolean. If true, test that the database does
#                       not change during the execution of the test case.
#
proc do_ioerr_test {testname args} {

  set ::ioerropts(-start) 1
  set ::ioerropts(-cksum) 0
  set ::ioerropts(-erc) 0
  set ::ioerropts(-count) 100000000
  set ::ioerropts(-persist) 1
  array set ::ioerropts $args

  set ::go 1
  for {set n $::ioerropts(-start)} {$::go} {incr n} {
    incr ::ioerropts(-count) -1
    if {$::ioerropts(-count)<0} break
 
    # Skip this IO error if it was specified with the "-exclude" option.
    if {[info exists ::ioerropts(-exclude)]} {
      if {[lsearch $::ioerropts(-exclude) $n]!=-1} continue
    }

    # Delete the files test.db and test2.db, then execute the TCL and 
    # SQL (in that order) to prepare for the test case.
    do_test $testname.$n.1 {
      set ::sqlite_io_error_pending 0
      catch {db close}
      catch {file delete -force test.db}
      catch {file delete -force test.db-journal}
      catch {file delete -force test2.db}
      catch {file delete -force test2.db-journal}
      set ::DB [sqlite3 db test.db; sqlite3_connection_pointer db]
      sqlite3_extended_result_codes $::DB $::ioerropts(-erc)
      if {[info exists ::ioerropts(-tclprep)]} {
        eval $::ioerropts(-tclprep)
      }
      if {[info exists ::ioerropts(-sqlprep)]} {
        execsql $::ioerropts(-sqlprep)
      }
      expr 0
    } {0}

    # Read the 'checksum' of the database.
    if {$::ioerropts(-cksum)} {
      set checksum [cksum]
    }
  
    # Set the Nth IO error to fail.
    do_test $testname.$n.2 [subst {
      set ::sqlite_io_error_persist $::ioerropts(-persist)
      set ::sqlite_io_error_pending $n
    }] $n
  
    # Create a single TCL script from the TCL and SQL specified
    # as the body of the test.
    set ::ioerrorbody {}
    if {[info exists ::ioerropts(-tclbody)]} {
      append ::ioerrorbody "$::ioerropts(-tclbody)\n"
    }
    if {[info exists ::ioerropts(-sqlbody)]} {
      append ::ioerrorbody "db eval {$::ioerropts(-sqlbody)}"
    }

    # Execute the TCL Script created in the above block. If
    # there are at least N IO operations performed by SQLite as
    # a result of the script, the Nth will fail.
    do_test $testname.$n.3 {
      set r [catch $::ioerrorbody msg]
      # puts rc=[sqlite3_errcode $::DB]
      set rc [sqlite3_errcode $::DB]
      if {$::ioerropts(-erc)} {
        # In extended result mode, all IOERRs are qualified 
        if {[regexp {^SQLITE_IOERR} $rc] && ![regexp {IOERR\+\d} $rc]} {
          return $rc
        }
      } else {
        # Not in extended result mode, no errors are qualified
        if {[regexp {\+\d} $rc]} {
          return $rc
        }
      }
      set ::go [expr {$::sqlite_io_error_pending<=0}]
      set s [expr $::sqlite_io_error_hit==0]
      set ::sqlite_io_error_hit 0
      # puts "$::sqlite_io_error_pending $r $msg"
      # puts "r=$r s=$s go=$::go msg=\"$msg\""
      expr { ($s && !$r && !$::go) || (!$s && $r && $::go) }
      # expr {$::sqlite_io_error_pending>0 || $r!=0}
    } {1}

    # If an IO error occured, then the checksum of the database should
    # be the same as before the script that caused the IO error was run.
    if {$::go && $::ioerropts(-cksum)} {
      do_test $testname.$n.4 {
        catch {db close}
        set ::DB [sqlite3 db test.db; sqlite3_connection_pointer db]
        cksum
      } $checksum
    }

    set ::sqlite_io_error_pending 0
    if {[info exists ::ioerropts(-cleanup)]} {
      catch $::ioerropts(-cleanup)
    }
  }
  set ::sqlite_io_error_pending 0
  set ::sqlite_io_error_persist 0
  unset ::ioerropts
}

# Return a checksum based on the contents of database 'db'.
#
proc cksum {{db db}} {
  set txt [$db eval {
      SELECT name, type, sql FROM sqlite_master order by name
  }]\n
  foreach tbl [$db eval {
      SELECT name FROM sqlite_master WHERE type='table' order by name
  }] {
    append txt [$db eval "SELECT * FROM $tbl"]\n
  }
  foreach prag {default_synchronous default_cache_size} {
    append txt $prag-[$db eval "PRAGMA $prag"]\n
  }
  set cksum [string length $txt]-[md5 $txt]
  # puts $cksum-[file size test.db]
  return $cksum
}

# Copy file $from into $to. This is used because some versions of
# TCL for windows (notably the 8.4.1 binary package shipped with the
# current mingw release) have a broken "file copy" command.
#
proc copy_file {from to} {
  if {$::tcl_platform(platform)=="unix"} {
    file copy -force $from $to
  } else {
    set f [open $from]
    fconfigure $f -translation binary
    set t [open $to w]
    fconfigure $t -translation binary
    puts -nonewline $t [read $f [file size $from]]
    close $t
    close $f
  }
}

# This command checks for outstanding calls to sqliteMalloc() from within
# the current thread. A list is returned with one entry for each outstanding
# malloc. Each list entry is itself a list of 5 items, as follows:
#
#     { <number-bytes> <file-name> <line-number> <test-case> <stack-dump> }
#
proc check_for_leaks {} {
  set ret [list]
  set cnt 0
  foreach alloc [sqlite_malloc_outstanding] {
    foreach {nBytes file iLine userstring backtrace} $alloc {}
    set stack [list]
    set skip 0

    # The first command in this block will probably fail on windows. This
    # means there will be no stack dump available.
    if {$cnt < 25 && $backtrace!=""} {
      catch {
        set stuff [eval "exec addr2line -e ./testfixture -f $backtrace"]
        foreach {func line} $stuff {
          if {$func != "??" || $line != "??:0"} {
            regexp {.*/(.*)} $line dummy line
            lappend stack "${func}() $line"
          } else {
            if {[lindex $stack end] != "..."} {
              lappend stack "..."
            }
          }
        }
      }
      incr cnt
    }

    if {!$skip} {
      lappend ret [list $nBytes $file $iLine $userstring $stack]
    }
  }
  return $ret
}

# Pretty print a report based on the return value of [check_for_leaks] to
# stdout.
proc pp_check_for_leaks {} {
  set l [check_for_leaks]
  set n 0
  foreach leak $l {
    foreach {nBytes file iLine userstring stack} $leak {}
    puts "$nBytes bytes leaked at $file:$iLine ($userstring)"
    foreach frame $stack {
      puts "        $frame"
    }
    incr n $nBytes
  }
  puts "Memory leaked: $n bytes in [llength $l] allocations"
  puts ""
}

# If the library is compiled with the SQLITE_DEFAULT_AUTOVACUUM macro set
# to non-zero, then set the global variable $AUTOVACUUM to 1.
set AUTOVACUUM $sqlite_options(default_autovacuum)

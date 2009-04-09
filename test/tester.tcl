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
# $Id: tester.tcl,v 1.143 2009/04/09 01:23:49 drh Exp $

#
# What for user input before continuing.  This gives an opportunity
# to connect profiling tools to the process.
#
for {set i 0} {$i<[llength $argv]} {incr i} {
  if {[regexp {^-+pause$} [lindex $argv $i] all value]} {
    puts -nonewline "Press RETURN to begin..."
    flush stdout
    gets stdin
    set argv [lreplace $argv $i $i]
  }
}

set tcl_precision 15
sqlite3_test_control_pending_byte 0x0010000

# 
# Check the command-line arguments for a default soft-heap-limit.
# Store this default value in the global variable ::soft_limit and
# update the soft-heap-limit each time this script is run.  In that
# way if an individual test file changes the soft-heap-limit, it
# will be reset at the start of the next test file.
#
if {![info exists soft_limit]} {
  set soft_limit 0
  for {set i 0} {$i<[llength $argv]} {incr i} {
    if {[regexp {^--soft-heap-limit=(.+)$} [lindex $argv $i] all value]} {
      if {$value!="off"} {
        set soft_limit $value
      }
      set argv [lreplace $argv $i $i]
    }
  }
}
sqlite3_soft_heap_limit $soft_limit

# 
# Check the command-line arguments to set the memory debugger
# backtrace depth.
#
# See the sqlite3_memdebug_backtrace() function in mem2.c or
# test_malloc.c for additional information.
#
for {set i 0} {$i<[llength $argv]} {incr i} {
  if {[lindex $argv $i] eq "--malloctrace"} {
    set argv [lreplace $argv $i $i]
    sqlite3_memdebug_backtrace 10
    sqlite3_memdebug_log start
    set tester_do_malloctrace 1
  }
}
for {set i 0} {$i<[llength $argv]} {incr i} {
  if {[regexp {^--backtrace=(\d+)$} [lindex $argv $i] all value]} {
    sqlite3_memdebug_backtrace $value
    set argv [lreplace $argv $i $i]
  }
}


proc ostrace_call {zCall nClick zFile i32 i64} {
  set s "INSERT INTO ostrace VALUES('$zCall', $nClick, '$zFile', $i32, $i64);"
  puts $::ostrace_fd $s
}

for {set i 0} {$i<[llength $argv]} {incr i} {
  if {[lindex $argv $i] eq "--ossummary" || [lindex $argv $i] eq "--ostrace"} {
    sqlite3_instvfs create -default ostrace
    set tester_do_ostrace 1
    set ostrace_fd [open ostrace.sql w]
    puts $ostrace_fd "BEGIN;"
    if {[lindex $argv $i] eq "--ostrace"} {
      set    s "CREATE TABLE ostrace"
      append s "(method TEXT, clicks INT, file TEXT, i32 INT, i64 INT);"
      puts $ostrace_fd $s
      sqlite3_instvfs configure ostrace ostrace_call
      sqlite3_instvfs configure ostrace ostrace_call
    }
    set argv [lreplace $argv $i $i]
  }
  if {[lindex $argv $i] eq "--binarylog"} {
    set tester_do_binarylog 1
    set argv [lreplace $argv $i $i]
  }
}

# 
# Check the command-line arguments to set the maximum number of
# errors tolerated before halting.
#
if {![info exists maxErr]} {
  set maxErr 1000
}
for {set i 0} {$i<[llength $argv]} {incr i} {
  if {[regexp {^--maxerror=(\d+)$} [lindex $argv $i] all maxErr]} {
    set argv [lreplace $argv $i $i]
  }
}
#puts "Max error = $maxErr"


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
if {![info exists nTest]} {
  sqlite3_shutdown 
  install_malloc_faultsim 1 
  sqlite3_initialize
  autoinstall_test_functions
  if {[info exists tester_do_binarylog]} {
    sqlite3_instvfs binarylog -default binarylog ostrace.bin
    sqlite3_instvfs marker binarylog "$argv0 $argv"
  }
}

proc reset_db {} {
  catch {db close}
  file delete -force test.db
  file delete -force test.db-journal
  sqlite3 db ./test.db
  set ::DB [sqlite3_connection_pointer db]
  if {[info exists ::SETUP_SQL]} {
    db eval $::SETUP_SQL
  }
}
reset_db

# Abort early if this script has been run before.
#
if {[info exists nTest]} return

# Set the test counters to zero
#
set nErr 0
set nTest 0
set skip_test 0
set failList {}
set omitList {}
if {![info exists speedTest]} {
  set speedTest 0
}

# Record the fact that a sequence of tests were omitted.
#
proc omit_test {name reason} {
  global omitList
  lappend omitList [list $name $reason]
}

# Invoke the do_test procedure to run a single test 
#
proc do_test {name cmd expected} {
  global argv nErr nTest skip_test maxErr
  sqlite3_memdebug_settitle $name
  if {[info exists ::tester_do_binarylog]} {
    sqlite3_instvfs marker binarylog "Start of $name"
  }
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
  if {[info exists ::tester_do_binarylog]} {
    sqlite3_instvfs marker binarylog "End of $name"
  }
}

# Run an SQL script.  
# Return the number of microseconds per statement.
#
proc speed_trial {name numstmt units sql} {
  puts -nonewline [format {%-21.21s } $name...]
  flush stdout
  set speed [time {sqlite3_exec_nr db $sql}]
  set tm [lindex $speed 0]
  if {$tm == 0} {
    set rate [format %20s "many"]
  } else {
    set rate [format %20.5f [expr {1000000.0*$numstmt/$tm}]]
  }
  set u2 $units/s
  puts [format {%12d uS %s %s} $tm $rate $u2]
  global total_time
  set total_time [expr {$total_time+$tm}]
}
proc speed_trial_tcl {name numstmt units script} {
  puts -nonewline [format {%-21.21s } $name...]
  flush stdout
  set speed [time {eval $script}]
  set tm [lindex $speed 0]
  if {$tm == 0} {
    set rate [format %20s "many"]
  } else {
    set rate [format %20.5f [expr {1000000.0*$numstmt/$tm}]]
  }
  set u2 $units/s
  puts [format {%12d uS %s %s} $tm $rate $u2]
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

# Run this routine last
#
proc finish_test {} {
  finalize_testing
}
proc finalize_testing {} {
  global nTest nErr sqlite_open_file_count omitList

  catch {db close}
  catch {db2 close}
  catch {db3 close}

  vfs_unlink_test
  sqlite3 db {}
  # sqlite3_clear_tsd_memdebug
  db close
  sqlite3_reset_auto_extension
  set heaplimit [sqlite3_soft_heap_limit]
  if {$heaplimit!=$::soft_limit} {
    puts "soft-heap-limit changed by this script\
          from $::soft_limit to $heaplimit"
  } elseif {$heaplimit!="" && $heaplimit>0} {
    puts "soft-heap-limit set to $heaplimit"
  }
  sqlite3_soft_heap_limit 0
  incr nTest
  puts "$nErr errors out of $nTest tests"
  if {$nErr>0} {
    puts "Failures on these tests: $::failList"
  }
  run_thread_tests 1
  if {[llength $omitList]>0} {
    puts "Omitted test cases:"
    set prec {}
    foreach {rec} [lsort $omitList] {
      if {$rec==$prec} continue
      set prec $rec
      puts [format {  %-12s %s} [lindex $rec 0] [lindex $rec 1]]
    }
  }
  if {$nErr>0 && ![working_64bit_int]} {
    puts "******************************************************************"
    puts "N.B.:  The version of TCL that you used to build this test harness"
    puts "is defective in that it does not support 64-bit integers.  Some or"
    puts "all of the test failures above might be a result from this defect"
    puts "in your TCL build."
    puts "******************************************************************"
  }
  if {[info exists ::tester_do_binarylog]} {
    sqlite3_instvfs destroy binarylog
  }
  if {$sqlite_open_file_count} {
    puts "$sqlite_open_file_count files were left open"
    incr nErr
  }
  if {[info exists ::tester_do_ostrace]} {
    puts "Writing ostrace.sql..."
    set fd $::ostrace_fd

    puts -nonewline $fd "CREATE TABLE ossummary"
    puts $fd "(method TEXT, clicks INTEGER, count INTEGER);"
    foreach row [sqlite3_instvfs report ostrace] {
      foreach {method count clicks} $row break
      puts $fd "INSERT INTO ossummary VALUES('$method', $clicks, $count);"
    }
    puts $fd "COMMIT;"
    close $fd
    sqlite3_instvfs destroy ostrace
  }
  if {[sqlite3_memory_used]>0} {
    puts "Unfreed memory: [sqlite3_memory_used] bytes"
    incr nErr
    ifcapable memdebug||mem5||(mem3&&debug) {
      puts "Writing unfreed memory log to \"./memleak.txt\""
      sqlite3_memdebug_dump ./memleak.txt
    }
  } else {
    puts "All memory allocations freed - no leaks"
    ifcapable memdebug||mem5 {
      sqlite3_memdebug_dump ./memusage.txt
    }
  }
  show_memstats
  puts "Maximum memory usage: [sqlite3_memory_highwater 1] bytes"
  puts "Current memory usage: [sqlite3_memory_highwater] bytes"
  if {[info commands sqlite3_memdebug_malloc_count] ne ""} {
    puts "Number of malloc()  : [sqlite3_memdebug_malloc_count] calls"
  }
  if {[info exists ::tester_do_malloctrace]} {
    puts "Writing mallocs.sql..."
    memdebug_log_sql
    sqlite3_memdebug_log stop
    sqlite3_memdebug_log clear

    if {[sqlite3_memory_used]>0} {
      puts "Writing leaks.sql..."
      sqlite3_memdebug_log sync
      memdebug_log_sql leaks.sql
    }
  }
  foreach f [glob -nocomplain test.db-*-journal] {
    file delete -force $f
  }
  foreach f [glob -nocomplain test.db-mj*] {
    file delete -force $f
  }
  exit [expr {$nErr>0}]
}

# Display memory statistics for analysis and debugging purposes.
#
proc show_memstats {} {
  set x [sqlite3_status SQLITE_STATUS_MEMORY_USED 0]
  set y [sqlite3_status SQLITE_STATUS_MALLOC_SIZE 0]
  set val [format {now %10d  max %10d  max-size %10d} \
              [lindex $x 1] [lindex $x 2] [lindex $y 2]]
  puts "Memory used:          $val"
  set x [sqlite3_status SQLITE_STATUS_PAGECACHE_USED 0]
  set y [sqlite3_status SQLITE_STATUS_PAGECACHE_SIZE 0]
  set val [format {now %10d  max %10d  max-size %10d} \
              [lindex $x 1] [lindex $x 2] [lindex $y 2]]
  puts "Page-cache used:      $val"
  set x [sqlite3_status SQLITE_STATUS_PAGECACHE_OVERFLOW 0]
  set val [format {now %10d  max %10d} [lindex $x 1] [lindex $x 2]]
  puts "Page-cache overflow:  $val"
  set x [sqlite3_status SQLITE_STATUS_SCRATCH_USED 0]
  set val [format {now %10d  max %10d} [lindex $x 1] [lindex $x 2]]
  puts "Scratch memory used:  $val"
  set x [sqlite3_status SQLITE_STATUS_SCRATCH_OVERFLOW 0]
  set y [sqlite3_status SQLITE_STATUS_SCRATCH_SIZE 0]
  set val [format {now %10d  max %10d  max-size %10d} \
               [lindex $x 1] [lindex $x 2] [lindex $y 2]]
  puts "Scratch overflow:     $val"
  ifcapable yytrackmaxstackdepth {
    set x [sqlite3_status SQLITE_STATUS_PARSER_STACK 0]
    set val [format {               max %10d} [lindex $x 2]]
    puts "Parser stack depth:    $val"
  }
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
  puts "addr  opcode        p1      p2      p3      p4               p5  #"
  puts "----  ------------  ------  ------  ------  ---------------  --  -"
  $db eval "explain $sql" {} {
    puts [format {%-4d  %-12.12s  %-6d  %-6d  %-6d  % -17s %s  %s} \
      $addr $opcode $p1 $p2 $p3 $p4 $p5 $comment
    ]
  }
}

# Show the VDBE program for an SQL statement but omit the Trace
# opcode at the beginning.  This procedure can be used to prove
# that different SQL statements generate exactly the same VDBE code.
#
proc explain_no_trace {sql} {
  set tr [db eval "EXPLAIN $sql"]
  return [lrange $tr 7 end]
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
proc integrity_check {name {db db}} {
  ifcapable integrityck {
    do_test $name [list execsql {PRAGMA integrity_check} $db] {ok}
  }
}

proc fix_ifcapable_expr {expr} {
  set ret ""
  set state 0
  for {set i 0} {$i < [string length $expr]} {incr i} {
    set char [string range $expr $i $i]
    set newstate [expr {[string is alnum $char] || $char eq "_"}]
    if {$newstate && !$state} {
      append ret {$::sqlite_options(}
    }
    if {!$newstate && $state} {
      append ret )
    }
    append ret $char
    set state $newstate
  }
  if {$state} {append ret )}
  return $ret
}

# Evaluate a boolean expression of capabilities.  If true, execute the
# code.  Omit the code if false.
#
proc ifcapable {expr code {else ""} {elsecode ""}} {
  #regsub -all {[a-z_0-9]+} $expr {$::sqlite_options(&)} e2
  set e2 [fix_ifcapable_expr $expr]
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
#   crashsql -delay CRASHDELAY -file CRASHFILE ?-blocksize BLOCKSIZE? $sql
#
proc crashsql {args} {
  if {$::tcl_platform(platform)!="unix"} {
    error "crashsql should only be used on unix"
  }

  set blocksize ""
  set crashdelay 1
  set prngseed 0
  set tclbody {}
  set crashfile ""
  set dc ""
  set sql [lindex $args end]
  
  for {set ii 0} {$ii < [llength $args]-1} {incr ii 2} {
    set z [lindex $args $ii]
    set n [string length $z]
    set z2 [lindex $args [expr $ii+1]]

    if     {$n>1 && [string first $z -delay]==0}     {set crashdelay $z2} \
    elseif {$n>1 && [string first $z -seed]==0}      {set prngseed $z2} \
    elseif {$n>1 && [string first $z -file]==0}      {set crashfile $z2}  \
    elseif {$n>1 && [string first $z -tclbody]==0}   {set tclbody $z2}  \
    elseif {$n>1 && [string first $z -blocksize]==0} {set blocksize "-s $z2" } \
    elseif {$n>1 && [string first $z -characteristics]==0} {set dc "-c {$z2}" } \
    else   { error "Unrecognized option: $z" }
  }

  if {$crashfile eq ""} {
    error "Compulsory option -file missing"
  }

  set cfile [file join [pwd] $crashfile]

  set f [open crash.tcl w]
  puts $f "sqlite3_crash_enable 1"
  puts $f "sqlite3_crashparams $blocksize $dc $crashdelay $cfile"
  puts $f "sqlite3_test_control_pending_byte $::sqlite_pending_byte"
  puts $f "sqlite3 db test.db -vfs crash"

  # This block sets the cache size of the main database to 10
  # pages. This is done in case the build is configured to omit
  # "PRAGMA cache_size".
  puts $f {db eval {SELECT * FROM sqlite_master;}}
  puts $f {set bt [btree_from_db db]}
  puts $f {btree_set_cache_size $bt 10}
  if {$prngseed} {
    set seed [expr {$prngseed%10007+1}]
    # puts seed=$seed
    puts $f "db eval {SELECT randomblob($seed)}"
  }

  if {[string length $tclbody]>0} {
    puts $f $tclbody
  }
  if {[string length $sql]>0} {
    puts $f "db eval {"
    puts $f   "$sql"
    puts $f "}"
  }
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
  set ::ioerropts(-ckrefcount) 0
  set ::ioerropts(-restoreprng) 1
  array set ::ioerropts $args

  # TEMPORARY: For 3.5.9, disable testing of extended result codes. There are
  # a couple of obscure IO errors that do not return them.
  set ::ioerropts(-erc) 0

  set ::go 1
  #reset_prng_state
  save_prng_state
  for {set n $::ioerropts(-start)} {$::go} {incr n} {
    set ::TN $n
    incr ::ioerropts(-count) -1
    if {$::ioerropts(-count)<0} break
 
    # Skip this IO error if it was specified with the "-exclude" option.
    if {[info exists ::ioerropts(-exclude)]} {
      if {[lsearch $::ioerropts(-exclude) $n]!=-1} continue
    }
    if {$::ioerropts(-restoreprng)} {
      restore_prng_state
    }

    # Delete the files test.db and test2.db, then execute the TCL and 
    # SQL (in that order) to prepare for the test case.
    do_test $testname.$n.1 {
      set ::sqlite_io_error_pending 0
      catch {db close}
      catch {db2 close}
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
      set ::sqlite_io_error_hit 0
      set ::sqlite_io_error_hardhit 0
      set r [catch $::ioerrorbody msg]
      set ::errseen $r
      set rc [sqlite3_errcode $::DB]
      if {$::ioerropts(-erc)} {
        # If we are in extended result code mode, make sure all of the
        # IOERRs we get back really do have their extended code values.
        # If an extended result code is returned, the sqlite3_errcode
        # TCLcommand will return a string of the form:  SQLITE_IOERR+nnnn
        # where nnnn is a number
        if {[regexp {^SQLITE_IOERR} $rc] && ![regexp {IOERR\+\d} $rc]} {
          return $rc
        }
      } else {
        # If we are not in extended result code mode, make sure no
        # extended error codes are returned.
        if {[regexp {\+\d} $rc]} {
          return $rc
        }
      }
      # The test repeats as long as $::go is non-zero.  $::go starts out
      # as 1.  When a test runs to completion without hitting an I/O
      # error, that means there is no point in continuing with this test
      # case so set $::go to zero.
      #
      if {$::sqlite_io_error_pending>0} {
        set ::go 0
        set q 0
        set ::sqlite_io_error_pending 0
      } else {
        set q 1
      }

      set s [expr $::sqlite_io_error_hit==0]
      if {$::sqlite_io_error_hit>$::sqlite_io_error_hardhit && $r==0} {
        set r 1
      }
      set ::sqlite_io_error_hit 0

      # One of two things must have happened. either
      #   1.  We never hit the IO error and the SQL returned OK
      #   2.  An IO error was hit and the SQL failed
      #
      expr { ($s && !$r && !$q) || (!$s && $r && $q) }
    } {1}

    set ::sqlite_io_error_hit 0
    set ::sqlite_io_error_pending 0

    # Check that no page references were leaked. There should be 
    # a single reference if there is still an active transaction, 
    # or zero otherwise.
    #
    # UPDATE: If the IO error occurs after a 'BEGIN' but before any
    # locks are established on database files (i.e. if the error 
    # occurs while attempting to detect a hot-journal file), then
    # there may 0 page references and an active transaction according
    # to [sqlite3_get_autocommit].
    #
    if {$::go && $::sqlite_io_error_hardhit && $::ioerropts(-ckrefcount)} {
      do_test $testname.$n.4 {
        set bt [btree_from_db db]
        db_enter db
        array set stats [btree_pager_stats $bt]
        db_leave db
        set nRef $stats(ref)
        expr {$nRef == 0 || ([sqlite3_get_autocommit db]==0 && $nRef == 1)}
      } {1}
    }

    # If there is an open database handle and no open transaction, 
    # and the pager is not running in exclusive-locking mode,
    # check that the pager is in "unlocked" state. Theoretically,
    # if a call to xUnlock() failed due to an IO error the underlying
    # file may still be locked.
    #
    ifcapable pragma {
      if { [info commands db] ne ""
        && $::ioerropts(-ckrefcount)
        && [db one {pragma locking_mode}] eq "normal"
        && [sqlite3_get_autocommit db]
      } {
        do_test $testname.$n.5 {
          set bt [btree_from_db db]
          db_enter db
          array set stats [btree_pager_stats $bt]
          db_leave db
          set stats(state)
        } 0
      }
    }

    # If an IO error occured, then the checksum of the database should
    # be the same as before the script that caused the IO error was run.
    #
    if {$::go && $::sqlite_io_error_hardhit && $::ioerropts(-cksum)} {
      do_test $testname.$n.6 {
        catch {db close}
        catch {db2 close}
        set ::DB [sqlite3 db test.db; sqlite3_connection_pointer db]
        cksum
      } $checksum
    }

    set ::sqlite_io_error_hardhit 0
    set ::sqlite_io_error_pending 0
    if {[info exists ::ioerropts(-cleanup)]} {
      catch $::ioerropts(-cleanup)
    }
  }
  set ::sqlite_io_error_pending 0
  set ::sqlite_io_error_persist 0
  unset ::ioerropts
}

# Return a checksum based on the contents of the main database associated
# with connection $db
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

# Generate a checksum based on the contents of the main and temp tables
# database $db. If the checksum of two databases is the same, and the
# integrity-check passes for both, the two databases are identical.
#
proc allcksum {{db db}} {
  set ret [list]
  ifcapable tempdb {
    set sql {
      SELECT name FROM sqlite_master WHERE type = 'table' UNION
      SELECT name FROM sqlite_temp_master WHERE type = 'table' UNION
      SELECT 'sqlite_master' UNION
      SELECT 'sqlite_temp_master' ORDER BY 1
    }
  } else {
    set sql {
      SELECT name FROM sqlite_master WHERE type = 'table' UNION
      SELECT 'sqlite_master' ORDER BY 1
    }
  }
  set tbllist [$db eval $sql]
  set txt {}
  foreach tbl $tbllist {
    append txt [$db eval "SELECT * FROM $tbl"]
  }
  foreach prag {default_cache_size} {
    append txt $prag-[$db eval "PRAGMA $prag"]\n
  }
  # puts txt=$txt
  return [md5 $txt]
}

# Generate a checksum based on the contents of a single database with
# a database connection.  The name of the database is $dbname.  
# Examples of $dbname are "temp" or "main".
#
proc dbcksum {db dbname} {
  if {$dbname=="temp"} {
    set master sqlite_temp_master
  } else {
    set master $dbname.sqlite_master
  }
  set alltab [$db eval "SELECT name FROM $master WHERE type='table'"]
  set txt [$db eval "SELECT * FROM $master"]\n
  foreach tab $alltab {
    append txt [$db eval "SELECT * FROM $dbname.$tab"]\n
  }
  return [md5 $txt]
}

proc memdebug_log_sql {{filename mallocs.sql}} {

  set data [sqlite3_memdebug_log dump]
  set nFrame [expr [llength [lindex $data 0]]-2]
  if {$nFrame < 0} { return "" }

  set database temp

  set tbl "CREATE TABLE ${database}.malloc(zTest, nCall, nByte, lStack);"

  set sql ""
  foreach e $data {
    set nCall [lindex $e 0]
    set nByte [lindex $e 1]
    set lStack [lrange $e 2 end]
    append sql "INSERT INTO ${database}.malloc VALUES"
    append sql "('test', $nCall, $nByte, '$lStack');\n"
    foreach f $lStack {
      set frames($f) 1
    }
  }

  set tbl2 "CREATE TABLE ${database}.frame(frame INTEGER PRIMARY KEY, line);\n"
  set tbl3 "CREATE TABLE ${database}.file(name PRIMARY KEY, content);\n"

  foreach f [array names frames] {
    set addr [format %x $f]
    set cmd "addr2line -e [info nameofexec] $addr"
    set line [eval exec $cmd]
    append sql "INSERT INTO ${database}.frame VALUES($f, '$line');\n"

    set file [lindex [split $line :] 0]
    set files($file) 1
  }

  foreach f [array names files] {
    set contents ""
    catch {
      set fd [open $f]
      set contents [read $fd]
      close $fd
    }
    set contents [string map {' ''} $contents]
    append sql "INSERT INTO ${database}.file VALUES('$f', '$contents');\n"
  }

  set fd [open $filename w]
  puts $fd "BEGIN; ${tbl}${tbl2}${tbl3}${sql} ; COMMIT;"
  close $fd
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

# If the library is compiled with the SQLITE_DEFAULT_AUTOVACUUM macro set
# to non-zero, then set the global variable $AUTOVACUUM to 1.
set AUTOVACUUM $sqlite_options(default_autovacuum)

source $testdir/thread_common.tcl

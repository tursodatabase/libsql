
#-------------------------------------------------------------------------
# Usage:
#
proc usage {} {
  set a0 testrunner.tcl

  set ::argv [list]
  uplevel [list source $::testdir/permutations.test]

  puts stderr "Usage: $a0 ?SWITCHES? ?PERMUTATION? ?PATTERNS?" 
  puts stderr ""
  puts stderr "where SWITCHES are:"
  puts stderr "    --jobs NUMBER-OF-JOBS"
  puts stderr ""
  puts stderr "available PERMUTATION values are:"
  set ii 0
  foreach name [lsort [array names ::testspec]] {
    if {($ii % 3)==0} { puts -nonewline stderr "  " }
    puts -nonewline stderr [format "% -22s" $name]
    if {($ii % 3)==2} { puts stderr "" }
    incr ii
  }
  puts stderr ""
  puts stderr ""
  puts stderr "Examples:"
  puts stderr " 1) Run the veryquick tests:"
  puts stderr "      $a0"
  puts stderr " 2) Run all test scripts in the source tree:"
  puts stderr "      $a0 full"
  puts stderr " 2) Run the 'memsubsys1' permutation:"
  puts stderr "      $a0 memsubsys1"
  puts stderr " 3) Run all permutations usually run by \[make fulltest\]"
  puts stderr "      $a0 release"
  puts stderr " 4) Run all scripts that match the pattern 'select%':"
  puts stderr "      $a0 select%"
  puts stderr "      $a0 all select%"
  puts stderr "      $a0 full select%"
  puts stderr " 5) Run all scripts that are part of the veryquick permutation and match the pattern 'select%':"
  puts stderr "      $a0 veryquick select%"
  puts stderr " 6) Run the 'memsubsys1' permutation, but just those scripts that match 'window%':"
  puts stderr "      $a0 memsubsys1 window%"
  puts stderr " 7) Run all the permutations, but only the scripts that match either 'fts5%' or 'rtree%':"
  puts stderr "      $a0 release fts5% rtree%"

  exit 1
}
#-------------------------------------------------------------------------

#-------------------------------------------------------------------------
# The database schema used by the testrunner.db database.
#
set R(schema) {
  DROP TABLE IF EXISTS script;
  DROP TABLE IF EXISTS msg;
  DROP TABLE IF EXISTS malloc;

  CREATE TABLE script(
    config TEXT,
    filename TEXT,                -- full path to test script
    slow BOOLEAN,                 -- true if script is "slow"
    state TEXT CHECK( state IN ('ready', 'running', 'done') ),
    testfixtureid,                -- Id of process that ran script
    time INTEGER,                 -- Time in ms
    nerr INTEGER,                 -- if 'done', the number of errors
    ntest INTEGER,                -- if 'done', the number of tests
    output TEXT,                  -- full output of test script
    PRIMARY KEY(config, filename)
  );

  CREATE TABLE malloc(
    id INTEGER PRIMARY KEY,
    nmalloc INTEGER,
    nbyte INTEGER,
    leaker TEXT
  );

  CREATE TABLE msg(
    id INTEGER PRIMARY KEY,
    msg TEXT
  );
}
#-------------------------------------------------------------------------

#-------------------------------------------------------------------------
# Try to estimate a the number of processes to use.
#
# Command [guess_number_of_cores] attempts to glean the number of logical
# cores. Command [default_njob] returns the default value for the --jobs
# switch.
#
proc guess_number_of_cores {} {
  set ret 4
  
  if {$::tcl_platform(os)=="Darwin"} {
    set cmd "sysctl -n hw.logicalcpu"
  } else {
    set cmd "nproc"
  }
  catch {
    set fd [open "|$cmd" r]
    set ret [gets $fd]
    close $fd
    set ret [expr $ret]
  }
  return $ret
}

proc default_njob {} {
  set nCore [guess_number_of_cores]
  set nHelper [expr int($nCore*0.75)]
  expr $nHelper>0 ? $nHelper : 1
}
#-------------------------------------------------------------------------


set R(dbname) [file normalize testrunner.db]
set R(logname) [file normalize testrunner.log]
set R(info_script) [file normalize [info script]]
set R(timeout) 10000              ;# Default busy-timeout for testrunner.
set R(nJob)    [default_njob]     ;# Default number of helper processes
set R(leaker)  ""                 ;# Name of first script to leak memory

set R(patternlist) [list]

set testdir [file dirname $argv0]

# Parse the command line options. There are two ways to invoke this
# script - to create a helper or coordinator process. If there are
# no helper processes, the coordinator runs test scripts.
#
# To create a helper process:
#
#    testrunner.tcl helper ID
#
# where ID is an integer greater than 0. The process will create and
# run tests in the "testdir$ID" directory. Helper processes are only
# created by coordinators - there is no need for a user to create
# helper processes manually.
#
# If the first argument is anything other than "helper", then a coordinator
# process is started. See the implementation of the [usage] proc above for
# details.
#
switch -- [lindex $argv 0] {
  helper {
    set R(helper) 1
    set R(helper_id) [lindex $argv 1]
    set argv [list --testdir=testdir$R(helper_id)]
  }

  default {
    set R(helper) 0
    set R(helper_id) 0

  }
}
if {$R(helper)==0} {
  for {set ii 0} {$ii < [llength $argv]} {incr ii} {
    set a [lindex $argv $ii]
    set n [string length $a]

    if {[string range $a 0 0]=="-"} {
      if {($n>2 && [string match "$a*" --jobs]) || $a=="-j"} {
        incr ii
          set R(nJob) [lindex $argv $ii]
      } else {
        usage
      }
    } else {
      lappend R(patternlist) [string map {% *} $a]
    }
  }

  set argv [list]
}
source $testdir/permutations.test

#-------------------------------------------------------------------------
# Return a list of tests to run. Each element of the list is itself a
# list of two elements - the name of a permuations.test configuration
# followed by the full path to a test script. i.e.:
#
#    {CONFIG FILENAME} {CONFIG FILENAME} ...
#
proc testset_patternlist {patternlist} {

  set first [lindex $patternlist 0]
  if {$first=="all"} { set first "full" }

  if {$first=="release"} {

    # The following mirrors the set of test suites invoked by "all.test".
    #
    set clist {
      full
      no_optimization memsubsys1 memsubsys2 singlethread 
      multithread onefile utf16 exclusive persistent_journal 
      persistent_journal_error no_journal no_journal_error
      autovacuum_ioerr no_mutex_try fullmutex journaltest 
      inmemory_journal pcache0 pcache10 pcache50 pcache90 
      pcache100 prepare mmap
    }
    ifcapable rbu { lappend clist rbu }
    if {$::tcl_platform(platform)=="unix"} {
      ifcapable !default_autovacuum {
        lappend clist autovacuum_crash 
      }
    }
    set patternlist [lrange $patternlist 1 end]

  } elseif {[info exists ::testspec($first)]} {
    set clist $first
    set patternlist [lrange $patternlist 1 end]
  } elseif { [llength $patternlist]==0 } {
    set clist veryquick
  } else {
    set clist full
  }

  set testset [list]

  foreach config $clist {
    catch { array unset O }
    array set O $::testspec($config)
    foreach f $O(-files) {
      if {[file pathtype $f]!="absolute"} {
        set f [file join $::testdir $f]
      }
      lappend testset [list $config [file normalize $f]]
    }
  }

  if {[llength $patternlist]>0} {
    foreach t $testset {
      set tail [file tail [lindex $t 1]]
      foreach p $patternlist {
        if {[string match $p $tail]} {
          lappend ret $t
          break;
        }
      }
    }
  } else {
    set ret $testset
  }

  set ret
}
#--------------------------------------------------------------------------


proc r_write_db {tcl} {
  global R

  sqlite3_test_control_pending_byte 0x010000
  sqlite3 db $R(dbname)
  db timeout $R(timeout)
  db eval { BEGIN EXCLUSIVE }

  uplevel $tcl

  db eval { COMMIT }
  db close
}

proc make_new_testset {} {
  global R

  set tests [testset_patternlist $R(patternlist)]
  r_write_db {
    db eval $R(schema)
    foreach t $tests {
      foreach {c s} $t {}
      set slow 0

      set fd [open $s]
      for {set ii 0} {$ii<100 && ![eof $fd]} {incr ii} {
        set line [gets $fd]
        if {[string match -nocase *testrunner:* $line]} {
          regexp -nocase {.*testrunner:(.*)} $line -> properties
          foreach p $properties {
            if {$p=="slow"} { set slow 1 }
          }
        }
      }
      close $fd

      db eval { 
        INSERT INTO script(config, filename, slow, state) 
            VALUES ($c, $s, $slow, 'ready') 
      }
    }
  }
}

# Find the next job in the database and mark it as 'running'. Then return
# a list consisting of the 
#
#   CONFIG FILENAME
#
# pair for the test.
#
proc get_next_test {} {
  global R
  set myid $R(helper_id)

  r_write_db {
    set f ""
    set c ""
    db eval {
      SELECT config, filename FROM script WHERE state='ready' 
      ORDER BY 
        (slow * (($myid+1) % 2)) DESC, 
        config!='full', 
        config,
        filename
      LIMIT 1
    } {
      set c $config
      set f $filename
    }
    if {$f!=""} {
      db eval { 
        UPDATE script SET state='running', testfixtureid=$myid 
        WHERE (config, filename) = ($c, $f)
      }
    }
  }

  if {$f==""} { return "" }
  list $c $f
}

proc r_testname {config filename} {
  set name [file tail $filename]
  if {$config!="" && $config!="full" && $config!="veryquick"} {
    set name "$config-$name"
  }
  return $name
}

proc r_set_test_result {config filename ms nerr ntest output} {
  global R

  set f [r_testname $config $filename]
  if {$nerr==0} {
    set msg "$f... Ok"
  } else {
    set msg "$f... FAILED - $nerr errors of $ntest tests"
  }
  append msg " (${ms}ms)"
  if {$R(helper)} {
    append msg " (helper $R(helper_id))"
  }

  sqlite3_shutdown
  set nMalloc [lindex [sqlite3_status SQLITE_STATUS_MALLOC_COUNT 0] 1]
  set nByte   [sqlite3_memory_used]
  if {($nByte>0 || $nMalloc>0) && $R(leaker)==""} {
    set R(leaker) $f
  }

  r_write_db {
    db eval {
      UPDATE script 
        SET state='done', output=$output, nerr=$nerr, ntest=$ntest, time=$ms
      WHERE (config, filename)=($config, $filename);

      INSERT INTO msg(msg) VALUES ($msg);
    }
  }
}

set R(iNextMsg) 1
proc r_get_messages {{db ""}} {
  global R

  sqlite3_test_control_pending_byte 0x010000

  if {$db==""} {
    sqlite3 rgmhandle $R(dbname)
    set dbhandle rgmhandle
    $dbhandle timeout $R(timeout)
  } else {
    set dbhandle $db
  }

  $dbhandle transaction {
    set next $R(iNextMsg)
    set ret [$dbhandle eval {SELECT msg FROM msg WHERE id>=$next}]
    set R(iNextMsg) [$dbhandle one {SELECT COALESCE(max(id), 0)+1 FROM msg}]
  }

  if {$db==""} {
    rgmhandle close
  }

  set ret
}

# This is called after all tests have been run to write the leaked memory
# report into the malloc table of testrunner.db.
#
proc r_memory_report {} {
  global R

  sqlite3_shutdown

  set nMalloc [lindex [sqlite3_status SQLITE_STATUS_MALLOC_COUNT 0] 1]
  set nByte   [sqlite3_memory_used]
  set id $R(helper_id)
  set leaker $R(leaker)

  r_write_db {
    db eval {
      INSERT INTO malloc(id, nMalloc, nByte, leaker) 
        VALUES($id, $nMalloc, $nByte, $leaker)
    }
  }
}


#--------------------------------------------------------------------------
#
set ::R_INSTALL_PUTS_WRAPPER {
  proc puts_sts_wrapper {args} {
    set n [llength $args]
    if {$n==1 || ($n==2 && [string first [lindex $args 0] -nonewline]==0)} {
      uplevel puts_into_caller $args
    } else {
      # A channel was explicitly specified.
      uplevel puts_sts_original $args
    }
  }
  rename puts puts_sts_original
  proc puts {args} { uplevel puts_sts_wrapper $args }
}

proc r_install_puts_wrapper {} $::R_INSTALL_PUTS_WRAPPER
proc r_uninstall_puts_wrapper {} {
  rename puts ""
  rename puts_sts_original puts
}

proc slave_test_script {script} {

  # Create the interpreter used to run the test script.
  interp create tinterp

  # Populate some global variables that tester.tcl expects to see.
  foreach {var value} [list              \
    ::argv0 $::argv0                     \
    ::argv  {}                           \
    ::SLAVE 1                            \
  ] {
    interp eval tinterp [list set $var $value]
  }

  # The alias used to access the global test counters.
  tinterp alias set_test_counter set_test_counter

  # Set up an empty ::cmdlinearg array in the slave.
  interp eval tinterp [list array set ::cmdlinearg [array get ::cmdlinearg]]

  # Set up the ::G array in the slave.
  interp eval tinterp [list array set ::G [array get ::G]]
  interp eval tinterp [list set ::G(runner.tcl) 1]

  interp eval tinterp $::R_INSTALL_PUTS_WRAPPER
  tinterp alias puts_into_caller puts_into_caller

  # Load the various test interfaces implemented in C.
  load_testfixture_extensions tinterp

  # Run the test script.
  set rc [catch { interp eval tinterp $script } msg opt]
  if {$rc} {
    puts_into_caller $msg
    puts_into_caller [dict get $opt -errorinfo]
    incr ::TC(errors)
  }

  # Check if the interpreter call [run_thread_tests]
  if { [interp eval tinterp {info exists ::run_thread_tests_called}] } {
    set ::run_thread_tests_called 1
  }

  # Delete the interpreter used to run the test script.
  interp delete tinterp
}

proc slave_test_file {zFile} {
  set tail [file tail $zFile]

  # Remember the value of the shared-cache setting. So that it is possible
  # to check afterwards that it was not modified by the test script.
  #
  ifcapable shared_cache { set scs [sqlite3_enable_shared_cache] }

  # Run the test script in a slave interpreter.
  #
  unset -nocomplain ::run_thread_tests_called
  reset_prng_state
  set ::sqlite_open_file_count 0
  set time [time { slave_test_script [list source $zFile] }]
  set ms [expr [lindex $time 0] / 1000]

  r_install_puts_wrapper

  # Test that all files opened by the test script were closed. Omit this
  # if the test script has "thread" in its name. The open file counter
  # is not thread-safe.
  #
  if {[info exists ::run_thread_tests_called]==0} {
    do_test ${tail}-closeallfiles { expr {$::sqlite_open_file_count>0} } {0}
  }
  set ::sqlite_open_file_count 0

  # Test that the global "shared-cache" setting was not altered by
  # the test script.
  #
  ifcapable shared_cache {
    set res [expr {[sqlite3_enable_shared_cache] == $scs}]
    do_test ${tail}-sharedcachesetting [list set {} $res] 1
  }

  # Add some info to the output.
  #
  output2 "Time: $tail $ms ms"
  show_memstats

  r_uninstall_puts_wrapper
  return $ms
}

proc puts_into_caller {args} {
  global R
  if {[llength $args]==1} {
    append R(output) [lindex $args 0]
    append R(output) "\n"
  } else {
    append R(output) [lindex $args 1]
  }
}

#-------------------------------------------------------------------------
#
proc r_final_report {} {
  global R

  sqlite3_test_control_pending_byte 0x010000
  sqlite3 db $R(dbname)

  db timeout $R(timeout)

  set errcode 0

  # Create the text log file. This is just the concatenation of the 
  # 'output' column of the database for every script that was run.
  set fd [open $R(logname) w]
  db eval {SELECT output FROM script ORDER BY config!='full',config,filename} {
    puts $fd $output
  }
  close $fd

  # Check if any scripts reported errors. If so, print one line noting
  # how many errors, and another identifying the scripts in which they
  # occured. Or, if no errors occurred, print out "no errors at all!".
  sqlite3 db $R(dbname)
  db timeout $R(timeout)
  db eval { SELECT sum(nerr) AS nerr, sum(ntest) AS ntest FROM script } { }
  puts "$nerr errors from $ntest tests."
  if {$nerr>0} {
    db eval { SELECT config, filename FROM script WHERE nerr>0 } {
      lappend errlist [r_testname $config $filename]
    }
    puts "Errors in: $errlist"
    set errcode 1
  }

  # Check if any scripts were not run or did not finish. Print out a
  # line identifying them if there are any. 
  set errlist [list]
  db eval { SELECT config, filename FROM script WHERE state!='done' } {
    lappend errlist [r_testname $config $filename]
  }
  if {$errlist!=[list]} {
    puts "Tests DID NOT FINISH (crashed?): $errlist"
    set errcode 1
  }

  set bLeak 0
  db eval {
    SELECT id, nmalloc, nbyte, leaker FROM malloc 
      WHERE nmalloc>0 OR nbyte>0
  } {
    if {$id==0} { 
      set line "This process " 
    } else {
      set line "Helper $id "
    }
    append line "leaked $nbyte byte in $nmalloc allocations"
    if {$leaker!=""} { append line " (perhaps in [file tail $leaker])" }
    puts $line
    set bLeak 1
  }
  if {$bLeak==0} {
    puts "No leaks - all allocations freed."
  }

  db close

  puts "Test database is $R(dbname)"
  puts "Test log file is $R(logname)"
  if {$errcode} {
    puts "This test has FAILED."
  }
  return $errcode
}


if {$R(helper)==0} {
  make_new_testset
}

set R(nHelperRunning) 0
if {$R(helper)==0 && $R(nJob)>1} {
  cd $cmdlinearg(TESTFIXTURE_HOME)
  for {set ii 1} {$ii <= $R(nJob)} {incr ii} {
    set cmd "[info nameofexec] $R(info_script) helper $ii 2>@1"
    puts "Launching helper $ii ($cmd)"
    set chan [open "|$cmd" r]
    fconfigure $chan -blocking false
    fileevent $chan readable [list r_helper_readable $ii $chan]
    incr R(nHelperRunning) 
  }
  cd $cmdlinearg(testdir)
}

proc r_helper_readable {id chan} {
  set data [gets $chan]
  if {$data!=""} { puts "helper $id:$data" }
  if {[eof $chan]} {
    puts "helper $id is finished"
    incr ::R(nHelperRunning) -1
    close $chan
  }
}

if {$R(nHelperRunning)==0} {
  while { ""!=[set t [get_next_test]] } {
    set R(output) ""
    set TC(count) 0
    set TC(errors) 0

    foreach {config filename} $t {}

    array set O $::testspec($config)
    set ::G(perm:name)         $config
    set ::G(perm:prefix)       $O(-prefix)
    set ::G(isquick)           1
    set ::G(perm:dbconfig)     $O(-dbconfig)
    set ::G(perm:presql)       $O(-presql)

    eval $O(-initialize)
    set ms [slave_test_file $filename]
    eval $O(-shutdown)

    unset -nocomplain ::G(perm:sqlite3_args)
    unset ::G(perm:name)
    unset ::G(perm:prefix)
    unset ::G(perm:dbconfig)
    unset ::G(perm:presql)

    r_set_test_result $config $filename $ms $TC(errors) $TC(count) $R(output)
  
    if {$R(helper)==0} {
      foreach msg [r_get_messages] { puts $msg }
    }
  }

  # Tests are finished - write a record into testrunner.db describing 
  # any memory leaks. 
  r_memory_report

} else {
  set TTT 0
  sqlite3 db $R(dbname)
  db timeout $R(timeout)
  while {$R(nHelperRunning)>0} {
    after 250 { incr TTT }
    vwait TTT
    foreach msg [r_get_messages db] { puts $msg }
  }
  db close
}

set errcode 0
if {$R(helper)==0} {
  set errcode [r_final_report]
}

exit $errcode


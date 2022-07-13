

#-------------------------------------------------------------------------
# Usage:
#
proc usage {} {
  puts stderr "Usage: $::argv0 ?SWITCHES?"
  puts stderr ""
  puts stderr "where SWITCHES are:"
  puts stderr "    --jobs NUMBER-OF-JOBS"
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
    filename TEXT PRIMARY KEY,    -- full path to test script
    state TEXT CHECK( state IN ('ready', 'running', 'done') ),
    testfixtureid,                -- Id of process that ran script
    time INTEGER,                 -- Time in ms
    nerr INTEGER,                 -- if 'done', the number of errors
    ntest INTEGER,                -- if 'done', the number of tests
    output TEXT                   -- full output of test script
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
  catch {
    set fd [open "|nproc" r]
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

    if {($n>2 && [string match "$a*" --jobs]) || $a=="-j"} {
      incr ii
      set R(nJob) [lindex $argv $ii]
    } else {
      usage
    }
  }

  set argv [list]
}

set testdir [file dirname $argv0]
source $testdir/tester.tcl
db close

#--------------------------------------------------------------------
# This is temporary!
# 
# Return a list of all scripts in the "veryquick" test.
#
proc all_veryquick_scripts {} {
  set OMIT {
  async2.test async3.test backup_ioerr.test corrupt.test
  corruptC.test crash.test crash2.test crash3.test crash4.test crash5.test
  crash6.test crash7.test delete3.test e_fts3.test fts3rnd.test
  fkey_malloc.test fuzz.test fuzz3.test fuzz_malloc.test in2.test loadext.test
  misc7.test mutex2.test notify2.test onefile.test pagerfault2.test 
  savepoint4.test savepoint6.test select9.test 
  speed1.test speed1p.test speed2.test speed3.test speed4.test 
  speed4p.test sqllimits1.test tkt2686.test thread001.test thread002.test
  thread003.test thread004.test thread005.test trans2.test vacuum3.test 
  incrvacuum_ioerr.test autovacuum_crash.test btree8.test shared_err.test
  vtab_err.test walslow.test walcrash.test walcrash3.test
  walthread.test rtree3.test indexfault.test securedel2.test
  sort3.test sort4.test fts4growth.test fts4growth2.test
  bigsort.test walprotocol.test mmap4.test fuzzer2.test
  walcrash2.test e_fkey.test backup.test
  writecrash.test

  fts4merge.test fts4merge2.test fts4merge4.test fts4check.test
  fts4merge5.test
  fts3cov.test fts3snippet.test fts3corrupt2.test fts3an.test
  fts3defer.test fts4langid.test fts3sort.test fts5unicode.test
  rtree4.test sessionbig.test

  all.test        async.test         quick.test  veryquick.test
  memleak.test    permutations.test  soak.test   fts3.test
  mallocAll.test  rtree.test         full.test   extraquick.test
  session.test    rbu.test

  }

  set testdir [file normalize $::testdir]
  set ret [list]

  foreach f [glob -nocomplain $testdir/*.test] {
    if {[lsearch $OMIT [file tail $f]]<0
     && [string match *malloc* $f]==0
     && [string match *ioerr* $f]==0
     && [string match *fault* $f]==0
     && [string match *bigfile* $f]==0
     && [string match *_err* $f]==0
     && [string match *fts5corrupt* $f]==0
     && [string match *fts5big* $f]==0
     && [string match *fts5aj* $f]==0
    } { 
      lappend ret $f
    }
  }

  set ret
}
#--------------------------------------------------------------------


proc r_write_db {tcl} {
  global R
  sqlite3 db $R(dbname)
  db timeout $R(timeout)
  db eval { BEGIN EXCLUSIVE }

  uplevel $tcl

  db eval { COMMIT }
  db close
}

proc make_new_testset {} {
  global R

  r_write_db {
    db eval $R(schema)
    foreach s [all_veryquick_scripts] {
      db eval { INSERT INTO script(filename, state) VALUES ($s, 'ready') }
    }
  }
}

proc get_next_test {} {
  global R
  set myid $R(helper_id)

  r_write_db {
    set f [db one { 
      SELECT filename FROM script WHERE state='ready' ORDER BY 1 LIMIT 1 
    }]
    if {$f!=""} {
      db eval { 
        UPDATE script SET state='running', testfixtureid=$myid WHERE filename=$f
      }
    }
  }

  return $f
}

proc r_set_test_result {filename ms nerr ntest output} {
  global R

  set f [file tail $filename]
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
    set R(leaker) $filename
  }

  r_write_db {
    db eval {
      UPDATE script 
        SET state='done', output=$output, nerr=$nerr, ntest=$ntest, time=$ms
      WHERE filename=$filename;

      INSERT INTO msg(msg) VALUES ($msg);
    }
  }
}

set R(iNextMsg) 1
proc r_get_messages {{db ""}} {
  global R

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

  sqlite3 db $R(dbname)
  db timeout $R(timeout)

  set errcode 0

  # Create the text log file. This is just the concatenation of the 
  # 'output' column of the database for every script that was run.
  set fd [open $R(logname) w]
  db eval {SELECT output FROM script ORDER BY filename} {
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
    db eval { SELECT filename FROM script WHERE nerr>0 } {
      lappend errlist [file tail $filename]
    }
    puts "Errors in: $errlist"
    set errcode 1
  }

  # Check if any scripts were not run or did not finish. Print out a
  # line identifying them if there are any. 
  set errlist [list]
  db eval { SELECT filename FROM script WHERE state!='done' } {
    lappend errlist [file tail $filename]
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
  if {$data!=""} { puts "helper $id:[gets $chan]" }
  if {[eof $chan]} {
    puts "helper $id is finished"
    incr ::R(nHelperRunning) -1
    close $chan
  }
}

if {$R(nHelperRunning)==0} {
  while { ""!=[set f [get_next_test]] } {
    set R(output) ""
    set TC(count) 0
    set TC(errors) 0
    set ms [slave_test_file $f]

    r_set_test_result $f $ms $TC(errors) $TC(count) $R(output)
  
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


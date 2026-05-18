#!/bin/sh
# Script to runs tests for SQLite.  Run with option "help" for more info. \
exec tclsh "$0" "$@"

set dir [pwd]
set testdir [file normalize [file dirname $argv0]]
set saved $argv
set argv [list]
source [file join $testdir testrunner_data.tcl]
source [file join $testdir permutations.test]
set argv $saved
cd $dir

# This script requires an interpreter that supports [package require sqlite3]
# to run. If this is not such an intepreter, see if there is a [testfixture]
# in the current directory. If so, run the command using it. If not, 
# recommend that the user build one.
#
proc find_interpreter {} {
  global dir
  set interpreter [file tail [info nameofexec]]
  set rc [catch { package require sqlite3 }]
  if {$rc} {
    if {[file readable pkgIndex.tcl] && [catch {source pkgIndex.tcl}]==0} {
      set rc [catch { package require sqlite3 }]
    }
  }
  if {$rc} {
    if { [string match -nocase testfixture* $interpreter]==0
      && [file executable ./testfixture]
    } {
      puts "Failed to find tcl package sqlite3. Restarting with ./testfixture.."
      set status [catch { 
          exec ./testfixture [info script] {*}$::argv >@ stdout 
      } msg]
      exit $status
    }
  }
  if {$rc} {
    puts "Cannot find tcl package sqlite3: Trying to build it now..."
    if {$::tcl_platform(platform)=="windows"} {
      set bat [open make-tcl-extension.bat w]
      puts $bat "nmake /f Makefile.msc tclextension"
      close $bat
      catch {exec -ignorestderr -- make-tcl-extension.bat}
    } else {
      catch {exec make tclextension}
    }
    if {[file readable pkgIndex.tcl] && [catch {source pkgIndex.tcl}]==0} {
      set rc [catch { package require sqlite3 }]
    }
    if {$rc==0} {
      puts "The SQLite tcl extension was successfully built and loaded."
      puts "Run \"make tclextension-install\" to avoid having to rebuild\
            it in the future."
    } else {
      puts "Unable to build the SQLite tcl extension"
    }
  }
  if {$rc} {
    puts stderr "Cannot find a working instance of the SQLite tcl extension."
    puts stderr "Run \"make tclextension\" or \"make testfixture\" and\
                 try again..."
    exit 1
  }
}
find_interpreter

# Usually this script is run by [testfixture]. But it can also be run
# by a regular [tclsh]. For these cases, emulate the [clock_milliseconds] 
# command.
if {[info commands clock_milliseconds]==""} {
  proc clock_milliseconds {} {
    clock milliseconds
  }
}

#-------------------------------------------------------------------------
# Usage:
#
proc usage {} {
  set a0 [file tail $::argv0]

  puts stderr [string trim [subst -nocommands {
Usage: 
    $a0 ?SWITCHES? ?PERMUTATION? ?PATTERNS?
    $a0 PERMUTATION FILE
    $a0 errors ?-v|--verbose? ?-s|--summary? ?PATTERN?
    $a0 help
    $a0 joblist ?PATTERN?
    $a0 njob ?NJOB?
    $a0 script ?-msvc? CONFIG
    $a0 status ?-d SECS? ?--cls?

  where SWITCHES are:
    --buildonly              Build test exes but do not run tests
    --config CONFIGS         Only use configs on comma-separate list CONFIGS
    --dryrun                 Write what would have happened to testrunner.log
    --explain                Write summary to stdout
    --jobs NUM               Run tests using NUM separate processes
    --omit CONFIGS           Omit configs on comma-separated list CONFIGS
    --status                 Show the full "status" report while running
    --stop-on-coredump       Stop running if any test segfaults
    --stop-on-error          Stop running after any reported error
    --zipvfs ZIPVFSDIR       ZIPVFS source directory

Special values for PERMUTATION that work with plain tclsh:

    list      - show all allowed PERMUTATION arguments.
    mdevtest  - tests recommended prior to normal development check-ins.
    release   - full release test with various builds.
    sdevtest  - like mdevtest but using ASAN and UBSAN.

Other PERMUTATION arguments must be run using testfixture, not tclsh:

    all       - all tcl test scripts, plus a subset of test scripts rerun
                with various permutations.
    full      - all tcl test scripts.
    veryquick - a fast subset of the tcl test scripts. This is the default.

If no PATTERN arguments are present, all tests specified by the PERMUTATION
are run. Otherwise, each pattern is interpreted as a glob pattern. Only
those tcl tests for which the final component of the filename matches at
least one specified pattern are run.  The glob wildcard '*' is prepended
to the pattern if it does not start with '^' and appended to every
pattern that does not end with '$'.

If no PATTERN arguments are present, then various fuzztest, threadtest
and other tests are run as part of the "release" permutation. These are
omitted if any PATTERN arguments are specified on the command line.

If a PERMUTATION is specified and is followed by the path to a Tcl script
instead of a list of patterns, then that single Tcl test script is run
with the specified permutation.

The "status" and "njob" commands are designed to be run from the same
directory as a running testrunner.tcl script that is running tests. The
"status" command prints a report describing the current state and progress 
of the tests.  Use the "-d N" option to have the status display clear the
screen and repeat every N seconds.  The "njob" command may be used to query
or modify the number of sub-processes the test script uses to run tests.

The "script" command outputs the script used to build a configuration.
Add the "-msvc" option for a Windows-compatible script. For a list of
available configurations enter "$a0 script help".

The "errors" commands shows the output of tests that failed in the
most recent run.  Complete output is shown if the -v or --verbose options
are used.  Otherwise, an attempt is made to minimize the output to show
only the parts that contain the error messages.  The --summary option just
shows the jobs that failed.  If PATTERN are provided, the error information
is only provided for jobs that match PATTERN.

Full documentation here: https://sqlite.org/src/doc/trunk/doc/testrunner.md
  }]]

  exit 1
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
  if {[catch {number_of_cores} ret]} {
    set ret 4
  
    if {$::tcl_platform(platform)=="windows"} {
      catch { set ret $::env(NUMBER_OF_PROCESSORS) }
    } else {
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
    }
  }
  return $ret
}

proc default_njob {} {
  global env
  if {[info exists env(NJOB)] && $env(NJOB)>=1} {
    return $env(NJOB)
  }
  set nCore [guess_number_of_cores]
  if {$nCore<=2} {
    set nHelper 1
  } else {
    set nHelper [expr int($nCore*0.5)]
  }
  return $nHelper
}
#-------------------------------------------------------------------------

#-------------------------------------------------------------------------
# Setup various default values in the global TRG() array.
# 
set TRG(dbname) [file normalize testrunner.db]
set TRG(logname) [file normalize testrunner.log]
set TRG(build.logname) [file normalize testrunner_build.log]
set TRG(info_script) [file normalize [info script]]
set TRG(timeout) 10000              ;# Default busy-timeout for testrunner.db 
set TRG(nJob)    [default_njob]     ;# Default number of helper processes
set TRG(patternlist) [list]
set TRG(cmdline) $argv
set TRG(reporttime) 2000
set TRG(fuzztest) 0                 ;# is the fuzztest option present.
set TRG(zipvfs) ""                  ;# -zipvfs option, if any
set TRG(buildonly) 0                ;# True if --buildonly option 
set TRG(config) {}                  ;# Only build the named configurations
set TRG(omitconfig) {}              ;# Do not build these configurations
set TRG(dryrun) 0                   ;# True if --dryrun option 
set TRG(explain) 0                  ;# True for the --explain option
set TRG(stopOnError) 0              ;# Stop running at first failure
set TRG(stopOnCore) 0               ;# Stop on a core-dump
set TRG(fullstatus) 0               ;# Full "status" report while running

switch -nocase -glob -- $tcl_platform(os) {
  *darwin* {
    set TRG(platform)    osx
    set TRG(make)        make.sh
    set TRG(makecmd)     "bash make.sh"
    set TRG(testfixture) testfixture
    set TRG(shell)       sqlite3
    set TRG(run)         run.sh
    set TRG(runcmd)      "bash run.sh"
  }
  *linux* {
    set TRG(platform)    linux
    set TRG(make)        make.sh
    set TRG(makecmd)     "bash make.sh"
    set TRG(testfixture) testfixture
    set TRG(shell)       sqlite3
    set TRG(run)         run.sh
    set TRG(runcmd)      "bash run.sh"
  }
  *openbsd* {
    set TRG(platform)    linux
    set TRG(make)        make.sh
    set TRG(makecmd)     "sh make.sh"
    set TRG(testfixture) testfixture
    set TRG(shell)       sqlite3
    set TRG(run)         run.sh
    set TRG(runcmd)      "sh run.sh"
  }
  *win* {
    set TRG(platform)    win
    set TRG(make)        make.bat
    set TRG(makecmd)     "call make.bat"
    set TRG(testfixture) testfixture.exe
    set TRG(shell)       sqlite3.exe
    set TRG(run)         run.bat
    set TRG(runcmd)      "run.bat"
  }
  default {
    error "cannot determine platform!"
  }
} 
#-------------------------------------------------------------------------

#-------------------------------------------------------------------------
# The database schema used by the testrunner.db database.
#
set TRG(schema) {
  DROP TABLE IF EXISTS jobs;
  DROP TABLE IF EXISTS config;

  /*
  ** This table contains one row for each job that testrunner.tcl must run
  ** before the entire test run is finished.
  **
  ** jobid:
  **   Unique identifier for each job. Must be a +ve non-zero number.
  **
  ** displaytype:
  **   3 or 4 letter mnemonic for the class of tests this belongs to e.g.
  **   "fuzz", "tcl", "make" etc.
  **
  ** displayname:
  **   Name/description of job. For display purposes.
  **
  ** build:
  **   If the job requires a make.bat/make.sh make wrapper (i.e. to build
  **   something), the name of the build configuration it uses. See 
  **   testrunner_data.tcl for a list of build configs. e.g. "Win32-MemDebug".
  **
  ** dirname:
  **   If the job should use a well-known directory name for its 
  **   sub-directory instead of an anonymous "testdir[1234...]" sub-dir
  **   that is deleted after the job is finished.
  **
  ** cmd:
  **   Bash or batch script to run the job.
  **
  ** depid:
  **   The jobid value of a job that this job depends on. This job may not
  **   be run before its depid job has finished successfully.
  **
  ** priority:
  **   Higher values run first. Sometimes.
  */
  CREATE TABLE jobs(
    /* Fields populated when db is initialized */
    jobid INTEGER PRIMARY KEY,          -- id to identify job
    displaytype TEXT NOT NULL,          -- Type of test (for one line report)
    displayname TEXT NOT NULL,          -- Human readable job name
    build TEXT NOT NULL DEFAULT '',     -- make.sh/make.bat file request, if any
    dirname TEXT NOT NULL DEFAULT '',   -- directory name, if required
    cmd TEXT NOT NULL,                  -- shell command to run
    depid INTEGER,                      -- identifier of dependency (or '')
    priority INTEGER NOT NULL,          -- higher priority jobs may run earlier
  
    /* Fields updated as jobs run */
    starttime INTEGER,                  -- Start time (milliseconds since 1970)
    endtime INTEGER,                    -- End time
    state TEXT CHECK( state IN ('','ready','running','done','failed','omit') ),
    ntest INT,                          -- Number of test cases run
    nerr INT,                           -- Number of errors reported
    svers TEXT,                         -- Reported SQLite version
    pltfm TEXT,                         -- Host platform reported
    output TEXT                         -- test output
  );

  CREATE TABLE config(
    name TEXT COLLATE nocase PRIMARY KEY,
    value 
  ) WITHOUT ROWID;

  CREATE INDEX i1 ON jobs(state, priority);
  CREATE INDEX i2 ON jobs(depid);
}
#-------------------------------------------------------------------------

#--------------------------------------------------------------------------
# Check if this script is being invoked to run a single file. If so,
# run it.
#
if {[llength $argv]==2
 && ([lindex $argv 0]=="" || [info exists ::testspec([lindex $argv 0])])
 && [file exists [lindex $argv 1]]
} {
  set permutation [lindex $argv 0]
  set script [file normalize [lindex $argv 1]]
  set ::argv [list]

  set testdir [file dirname $argv0]
  source $::testdir/tester.tcl

  if {$permutation=="full"} {

    unset -nocomplain ::G(isquick)
    reset_db

  } elseif {$permutation!="default" && $permutation!=""} {

    if {[info exists ::testspec($permutation)]==0} {
      error "no such permutation: $permutation"
    }

    array set O $::testspec($permutation)
    set ::G(perm:name)         $permutation
    set ::G(perm:prefix)       $O(-prefix)
    set ::G(isquick)           1
    set ::G(perm:dbconfig)     $O(-dbconfig)
    set ::G(perm:presql)       $O(-presql)

    rename finish_test helper_finish_test
    proc finish_test {} "
      uplevel {
        $O(-shutdown)
      }
      helper_finish_test
    "

    eval $O(-initialize)
  }

  reset_db
  source $script
  exit
}
#--------------------------------------------------------------------------

#--------------------------------------------------------------------------
# Check if this is the "njob" command:
#
if {([llength $argv]==2 || [llength $argv]==1) 
 && [string compare -nocase njob [lindex $argv 0]]==0
} {
  sqlite3 mydb $TRG(dbname)
  if {[llength $argv]==2} {
    set param [lindex $argv 1]
    if {[string is integer $param]==0 || $param<0 || $param>128} {
      puts stderr "parameter must be an integer between 0 and 128"
      exit 1
    }

    mydb eval { REPLACE INTO config VALUES('njob', $param); }
  }
  set res [mydb one { SELECT value FROM config WHERE name='njob' }]
  mydb close
  puts "$res"
  exit
}
#--------------------------------------------------------------------------

#--------------------------------------------------------------------------
# Check if this is the "help" command:
#
if {[string compare -nocase help [lindex $argv 0]]==0} {
  usage
}
#--------------------------------------------------------------------------

#--------------------------------------------------------------------------
# Check if this is the "script" command:
#
if {[string compare -nocase script [lindex $argv 0]]==0} {
  if {[llength $argv]!=2 && !([llength $argv]==3&&[lindex $argv 1]=="-msvc")} {
    usage
  }

  set bMsvc [expr ([llength $argv]==3)]
  set config [lindex $argv [expr [llength $argv]-1]]

  puts [trd_buildscript $config [file dirname $testdir] $bMsvc]
  exit
}

# Compute an elapse time string MM:SS or HH:MM:SS based on the
# number of milliseconds in the argument.
#
proc elapsetime {ms} {
  set s [expr {int(($ms+500.0)*0.001)}]
  set hr [expr {$s/3600}]
  set mn [expr {($s/60)%60}]
  set sc [expr {$s%60}]
  if {$hr>0} {
    return [format %02d:%02d:%02d $hr $mn $sc]
  } else {
    return [format %02d:%02d $mn $sc]
  }
}

# Helper routine for show_status
#
proc display_job {jobdict {tm ""}} {
  array set job $jobdict
  if {[string length $job(displayname)]>65} {
    set dfname [format %.65s... $job(displayname)]
  } else {
    set dfname [format %-68s $job(displayname)]
  }
  set dtm ""
  if {$tm!=""} {
    set dtm [expr {$tm-$job(starttime)}]
    set dtm [format %8s [elapsetime $dtm]]
  } else {
    set dtm [format %8s ""]
  }
  puts "  $dfname $dtm"
}

# This procedure shows the "status" page.  It uses the database
# connect passed in as the "db" parameter.  If the "cls" parameter
# is true, then VT100 escape codes are used to format the display.
#
proc show_status {db cls} {
  global TRG
  $db eval BEGIN
  if {[catch {
    set cmdline [$db one { SELECT value FROM config WHERE name='cmdline' }]
    set nJob [$db one { SELECT value FROM config WHERE name='njob' }]
  } msg]} {
    if {$cls} {puts "\033\[H\033\[2J"}
    puts "Cannot read database: $TRG(dbname)"
    return
  }
  set now [clock_milliseconds]
  set tm [$db one {
    SELECT 
      COALESCE((SELECT value FROM config WHERE name='end'), $now) -
      (SELECT value FROM config WHERE name='start')
  }]

  set total 0
  foreach s {"" ready running done failed} { set S($s) 0 }
  $db eval {
    SELECT state, count(*) AS cnt FROM jobs GROUP BY 1
  } {
    incr S($state) $cnt
    incr total $cnt
  }
  set nt 0
  set ne 0
  $db eval {
    SELECT sum(ntest) AS nt, sum(nerr) AS ne FROM jobs HAVING nt>0
  } break
  set fin [expr $S(done)+$S(failed)]
  if {$cmdline!=""} {set cmdline " $cmdline"}

  if {$cls} {
    # Move the cursor to the top-left corner.  Each iteration will simply
    # overwrite.
    puts -nonewline "\033\[H"
    flush stdout
  }
  puts [format %-79.79s "Command: \[testrunner.tcl$cmdline\]"]
  puts [format %-79.79s "Summary: [elapsetime $tm], $fin/$total jobs,\
                         $ne errors, $nt tests"]

  set srcdir [file dirname [file dirname $TRG(info_script)]]
  set line "Running: $S(running) (max: $nJob)"
  if {$S(running)>0 && $fin>100 && $fin>0.05*$total} {
    # Only estimate the time remaining after completing at least 100
    # jobs amounting to 10% of the total.  Never estimate less than
    # 2% of the total time used so far.
    set tmleft [expr {($tm/$fin)*($total-$fin)}]
    if {$tmleft<0.02*$tm} {
      set tmleft [expr {$tm*0.02}]
    }
    append line " est time left [elapsetime $tmleft]"
  }
  puts [format %-79.79s $line]
  if {$S(running)>0} {
    $db eval {
      SELECT * FROM jobs WHERE state='running' ORDER BY starttime 
    } job {
      display_job [array get job] $now
    }
  }
  if {$S(failed)>0} {
    # $toshow is the number of failures to report.  In $cls mode,
    # status tries to limit the number of failure reported so that
    # the status display does not overflow a 24-line terminal.  It will
    # always show at least the most recent 4 failures, even if an overflow
    # is needed.  No limit is imposed for a status within $cls.
    #
    if {$cls && $S(failed)>18-$S(running)} {
      set toshow [expr {18-$S(running)}]
      if {$toshow<4} {set toshow 4}
      set shown " (must recent $toshow shown)"
    } else {
      set toshow $S(failed)
      set shown ""
    }
    puts [format %-79s  "Failed:  $S(failed) $shown"]
    $db eval {
      SELECT * FROM jobs WHERE state='failed'
       ORDER BY endtime DESC LIMIT $toshow
    } job {
      display_job [array get job]
    }
    set nOmit [$db one {SELECT count(*) FROM jobs WHERE state='omit'}]
    if {$nOmit} {
      puts [format %-79s "  ... $nOmit jobs omitted due to failures"]
    }
  }
  if {$cls} {
    # Clear everything else to the bottom of the screen
    puts -nonewline "\033\[0J"
    flush stdout
  }
  $db eval COMMIT
}

  

#--------------------------------------------------------------------------
# Check if this is the "status" command:
#
if {[llength $argv]>=1 
 && [string compare -nocase status [lindex $argv 0]]==0 
} {
  set delay 0
  set cls 0
  for {set ii 1} {$ii<[llength $argv]} {incr ii} {
    set a0 [lindex $argv $ii]
    if {$a0=="-d" && $ii+1<[llength $argv]} {
      incr ii
      set delay [lindex $argv $ii]
      if {![string is integer -strict $delay]} {
        puts "Argument to -d should be an integer"
        exit 1
      }
    } elseif {$a0=="-cls" || $a0=="--cls"} {
      set cls 1
    } else {
      puts "unknown option: \"$a0\""
      exit 1
    }
  }

  if {![file readable $TRG(dbname)]} {
    puts "Database missing: $TRG(dbname)"
    exit
  }
  sqlite3 mydb $TRG(dbname)
  mydb timeout 2000

  # Clear the whole screen initially.
  #
  if {$delay>0 || $cls} {puts -nonewline "\033\[2J"}

  while {1} {
    show_status mydb [expr {$delay>0 || $cls}]
    if {$delay<=0} break
    after [expr {$delay*1000}]
  }
  mydb close
  exit
}

#--------------------------------------------------------------------------
# Check if this is the "joblist" command:
#
if {[llength $argv]>=1 
 && [string compare -nocase "joblist" [lindex $argv 0]]==0 
} {
  set pattern {}
  for {set ii 1} {$ii<[llength $argv]} {incr ii} {
    set a0 [lindex $argv $ii]
    if {$pattern==""} {
      set pattern [string trim $a0 *]
    } else {
      puts "unknown option: \"$a0\""
      exit 1
    }
  }
  set SQL {SELECT displaytype, displayname, state FROM jobs}
  if {$pattern!=""} {
    regsub -all {[^a-zA-Z0-9*.-/]} $pattern ? pattern
    append SQL " WHERE displayname GLOB '*$pattern*'"
  }
  append SQL " ORDER BY starttime"

  if {![file readable $TRG(dbname)]} {
    puts "Database missing: $TRG(dbname)"
    exit
  }
  sqlite3 mydb $TRG(dbname)
  mydb timeout 2000

  mydb eval $SQL {
    set label UNKNOWN
    switch -- $state {
      ready {set label READY}
      done {set label DONE}
      failed {set label FAILED}
      omit {set label OMIT}
      running {set label RUNNING}
    }
    puts [format {%-7s %-5s %s} $label $displaytype $displayname]
  }
  mydb close
  exit
}

# Scan the output of all jobs looking for the summary lines that
# report the number of test cases and the number of errors.
# Aggregate these numbers and return them.
#
proc aggregate_test_counts {db} {
  set ne 0
  set nt 0
  $db eval {SELECT sum(nerr) AS ne, sum(ntest) as nt FROM jobs} break
  return [list $ne $nt]
}

#--------------------------------------------------------------------------
# Check if this is the "errors" command:
#
if {[llength $argv]>=1
 && ([string compare -nocase errors [lindex $argv 0]]==0 ||
     [string match err* [lindex $argv 0]]==1)
} {
  set verbose 0
  set pattern {}
  set summary 0
  for {set ii 1} {$ii<[llength $argv]} {incr ii} {
    set a0 [lindex $argv $ii]
    if {$a0=="-v" || $a0=="--verbose" || $a0=="-verbose"} {
      set verbose 1
    } elseif {$a0=="-s" || $a0=="--summary" || $a0=="-summary"} {
      set summary 1
    } elseif {$pattern==""} {
      set pattern *[string trim $a0 *]*
    } else {
      puts "unknown option: \"$a0\"".  Use --help for more info."
      exit 1
    }
  }
  set cnt 0
  sqlite3 mydb $TRG(dbname)
  mydb timeout 5000
  if {$summary} {
    set sql "SELECT displayname FROM jobs WHERE state='failed'"
  } else {
    set sql "SELECT displaytype, displayname, output FROM jobs \
              WHERE state='failed'"
  }
  if {$pattern!=""} {
    regsub -all {[^a-zA-Z0-9*/ ?]} $pattern . pattern
    append sql " AND displayname GLOB '$pattern'"
  }
  mydb eval $sql {
    if {$summary} {
      puts "FAILED: $displayname"
      continue
    }
    puts "**** $displayname ****"
    if {$verbose || $displaytype!="tcl"} {
      puts $output
    } else {
      foreach line [split $output \n] {
        if {[string match {!*} $line] || [string match *failed* $line]} {
          puts $line
        }
      }
    }
    incr cnt
  }
  if {$pattern==""} {
    set summary [aggregate_test_counts mydb]
    mydb close
    puts "Total [lindex $summary 0] errors out of [lindex $summary 1] tests"
  } else {
    mydb close
  }
  exit
}

#-------------------------------------------------------------------------
# Parse the command line.
#
for {set ii 0} {$ii < [llength $argv]} {incr ii} {
  set isLast [expr $ii==([llength $argv]-1)]
  set a [lindex $argv $ii]
  set n [string length $a]

  if {[string range $a 0 0]=="-"} {
    if {($n>2 && [string match "$a*" --jobs]) || $a=="-j"} {
      incr ii
      set TRG(nJob) [lindex $argv $ii]
      if {$isLast} { usage }
    } elseif {($n>2 && [string match "$a*" --zipvfs]) || $a=="-z"} {
      incr ii
      set TRG(zipvfs) [file normalize [lindex $argv $ii]]
      if {$isLast} { usage }
    } elseif {($n>2 && [string match "$a*" --buildonly]) || $a=="-b"} {
      set TRG(buildonly) 1
    } elseif {($n>2 && [string match "$a*" --config]) || $a=="-c"} {
      incr ii
      set TRG(config) [lindex $argv $ii]
    } elseif {($n>2 && [string match "$a*" --dryrun]) || $a=="-d"} {
      set TRG(dryrun) 1
    } elseif {($n>2 && [string match "$a*" --explain]) || $a=="-e"} {
      set TRG(explain) 1
    } elseif {($n>2 && [string match "$a*" --omit]) || $a=="-c"} {
      incr ii
      set TRG(omitconfig) [lindex $argv $ii]
    } elseif {[string match "$a*" --stop-on-error]} {
      set TRG(stopOnError) 1
    } elseif {[string match "$a*" --stop-on-coredump]} {
      set TRG(stopOnCore) 1
    } elseif {[string match "$a*" --status]} {
      if {$tcl_platform(platform)=="windows"} {
        puts stdout \
"The --status option is not available on Windows. A suggested work-around"
        puts stdout \
"is to run the following command in a separate window:\n"
        puts stdout "   [info nameofexe] $argv0 status -d 2\n"
      } else {
        set TRG(fullstatus) 1
      }
    } else {
      usage
    }
  } else {
    lappend TRG(patternlist) [string map {% *} $a]
  }
}
set argv [list]

# This script runs individual tests - tcl scripts or [make xyz] commands -
# in directories named "testdir$N", where $N is an integer. This variable
# contains a list of integers indicating the directories in use.
#
# This variable is accessed only via the following commands:
#
#   dirs_nHelper
#     Return the number of entries currently in the list.
#
#   dirs_freeDir IDIR
#     Remove value IDIR from the list. It is an error if it is not present.
#
#   dirs_allocDir
#     Select a value that is not already in the list. Add it to the list
#     and return it.
#
set TRG(dirs_in_use) [list]

proc dirs_nHelper {} {
  global TRG
  llength $TRG(dirs_in_use)
}
proc dirs_freeDir {iDir} {
  global TRG
  set out [list]
  foreach d $TRG(dirs_in_use) {
    if {$iDir!=$d} { lappend out $d }
  }
  if {[llength $out]!=[llength $TRG(dirs_in_use)]-1} {
    error "dirs_freeDir could not find $iDir"
  }
  set TRG(dirs_in_use) $out
}
proc dirs_allocDir {} {
  global TRG
  array set inuse [list]
  foreach d $TRG(dirs_in_use) {
    set inuse($d) 1
  }
  for {set iRet 0} {[info exists inuse($iRet)]} {incr iRet} { }
  lappend TRG(dirs_in_use) $iRet
  return $iRet
}

# Check that directory $dir exists. If it does not, create it. If 
# it does, delete its contents.
#
proc create_or_clear_dir {dir} {
  set dir [file normalize $dir]
  catch { file mkdir $dir }
  foreach f [glob -nocomplain [file join $dir *]] {
    catch { file delete -force $f }
  }
}

proc build_to_dirname {bname} {
  set fold [string tolower [string map {- _} $bname]]
  return "testrunner_build_$fold"
}

#-------------------------------------------------------------------------

proc r_write_db {tcl} {
  trdb eval { BEGIN EXCLUSIVE }
  uplevel $tcl
  trdb eval { COMMIT }
}

# Obtain a new job to be run by worker $iJob (an integer). A job is
# returned as a three element list:
#
#    {$build $config $file}
#
proc r_get_next_job {iJob} {
  global T

  if {($iJob%2)} {
    set orderby "ORDER BY priority ASC"
  } else {
    set orderby "ORDER BY priority DESC"
  }

  set ret [list]

  r_write_db {
    set query "
      SELECT * FROM jobs AS j WHERE state='ready' $orderby LIMIT 1
    " 
    trdb eval $query job {
      set tm [clock_milliseconds]
      set T($iJob) $tm
      set jobid $job(jobid)

      trdb eval {
        UPDATE jobs SET starttime=$tm, state='running' WHERE jobid=$jobid
      }

      set ret [array get job]
    }
  }

  return $ret
}

# Usage:
#
#   add_job OPTION ARG OPTION ARG...
#
# where available OPTIONS are:
#
#   -displaytype
#   -displayname
#   -build
#   -dirname     
#   -cmd 
#   -depid 
#   -priority 
#
# Returns the jobid value for the new job.
# 
proc add_job {args} {

  set options {
      -displaytype -displayname -build -dirname 
      -cmd -depid -priority
  }

  # Set default values of options.
  set A(-dirname) ""
  set A(-depid)   ""
  set A(-priority) 0
  set A(-build)   ""

  array set A $args

  # Check all required options are present. And that no extras are present.
  foreach o $options {
    if {[info exists A($o)]==0} { error "missing required option $o" }
  }
  foreach o [array names A] {
    if {[lsearch -exact $options $o]<0} { error "unrecognized option: $o" }
  }

  set state ""
  if {$A(-depid)==""} { set state ready }

  trdb eval {
    INSERT INTO jobs(
      displaytype, displayname, build, dirname, cmd, depid, priority,
      state
    ) VALUES (
      $A(-displaytype),
      $A(-displayname),
      $A(-build),
      $A(-dirname),
      $A(-cmd),
      $A(-depid),
      $A(-priority),
      $state
    )
  }

  trdb last_insert_rowid
}

# Argument $build is either an empty string, or else a list of length 3 
# describing the job to build testfixture. In the usual form:
#
#    {ID DIRNAME DISPLAYNAME}
# 
# e.g    
#
#    {1 /home/user/sqlite/test/testrunner_bld_xyz All-Debug}
# 
proc add_tcl_jobs {build config patternlist {shelldepid ""}} {
  global TRG

  set topdir [file dirname $::testdir]
  set testrunner_tcl [file normalize [info script]]

  if {$build==""} {
    set testfixture [info nameofexec]
  } else {
    set testfixture [file join [lindex $build 1] $TRG(testfixture)]
  }
  if {[lindex $build 2]=="Valgrind"} {
    set setvar "export OMIT_MISUSE=1\n"
    set testfixture "${setvar}valgrind -v --error-exitcode=1 $testfixture"
  }

  # The ::testspec array is populated by permutations.test
  foreach f [dict get $::testspec($config) -files] {

    if {[llength $patternlist]>0} {
      set bMatch 0
      foreach p $patternlist {
        set p [string trim $p *]
        if {[string index $p 0]=="^"} {
          set p [string range $p 1 end]
        } else {
          set p "*$p"
        }
        if {[string index $p end]=="\$"} {
          set p [string range $p 0 end-1]
        } else {
          set p "$p*"
        }
        if {[string match $p "$config [file tail $f]"]} {
          set bMatch 1
          break
        }
      }
      if {$bMatch==0} continue
    }

    if {[file pathtype $f]!="absolute"} { set f [file join $::testdir $f] }
    set f [file normalize $f]

    set displayname [string map [list $topdir/ {}] $f]
    if {$config=="full" || $config=="veryquick"} {
      set cmd "$testfixture $f"
    } else {
      set cmd "$testfixture $testrunner_tcl $config $f"
      set displayname "config=$config $displayname"
    }
    if {$build!=""} {
      set displayname "[lindex $build 2] $displayname"
    }

    set lProp [trd_test_script_properties $f]
    set priority 0
    if {[lsearch $lProp slow]>=0} { set priority 2 }
    if {[lsearch $lProp superslow]>=0} { set priority 4 }

    set depid [lindex $build 0]
    if {$shelldepid!="" && [lsearch $lProp shell]>=0} { set depid $shelldepid }

    add_job                            \
        -displaytype tcl               \
        -displayname $displayname      \
        -cmd $cmd                      \
        -depid $depid                  \
        -priority $priority
  }
}

proc add_build_job {buildname target {postcmd ""} {depid ""}} {
  global TRG

  set dirname "[string tolower [string map {- _} $buildname]]_$target"
  set dirname "testrunner_bld_$dirname"

  set cmd "$TRG(makecmd) $target"
  if {$postcmd!=""} {
    append cmd "\n"
    append cmd $postcmd
  }

  set id [add_job                                \
    -displaytype bld                             \
    -displayname "Build $buildname ($target)"    \
    -dirname $dirname                            \
    -build $buildname                            \
    -cmd  $cmd                                   \
    -depid $depid                                \
    -priority 3
  ]

  list $id [file normalize $dirname] $buildname
}

proc add_shell_build_job {buildname dirname depid} {
  global TRG

  if {$TRG(platform)=="win"} {
    set path [string map {/ \\} "$dirname/"]
    set copycmd "xcopy $TRG(shell) $path"
  } else {
    set copycmd "cp $TRG(shell) $dirname/"
  }

  return [
    add_build_job $buildname $TRG(shell) $copycmd $depid
  ]
}


proc add_make_job {bld target} {
  global TRG

  if {$TRG(platform)=="win"} {
    set path [string map {/ \\} [lindex $bld 1]]
    set cmd "xcopy /S $path\\* ."
  } else {
    set cmd "cp -r [lindex $bld 1]/* ."
  }
  append cmd "\n$TRG(makecmd) $target"

  add_job                                       \
    -displaytype make                           \
    -displayname "[lindex $bld 2] make $target" \
    -cmd $cmd                                   \
    -depid [lindex $bld 0]                      \
    -priority 1
}

proc add_fuzztest_jobs {buildname} {

  foreach {interpreter scripts} [trd_fuzztest_data] {
    set subcmd [lrange $interpreter 1 end]
    set interpreter [lindex $interpreter 0]

    set bld [add_build_job $buildname $interpreter]
    foreach {depid dirname displayname} $bld {}

    foreach s $scripts {

      # Fuzz data files fuzzdata1.db and fuzzdata2.db are larger than
      # the others. So ensure that these are run as a higher priority.
      set tail [file tail $s]
      if {$tail=="fuzzdata1.db" || $tail=="fuzzdata2.db"} {
        set priority 5
      } else {
        set priority 1
      }

      add_job                                                   \
        -displaytype fuzz                                       \
        -displayname "$buildname $interpreter $tail"            \
        -depid $depid                                           \
        -cmd "[file join $dirname $interpreter] $subcmd $s"     \
        -priority $priority
    }
  }
}

proc add_zipvfs_jobs {} {
  global TRG
  source [file join $TRG(zipvfs) test zipvfs_testrunner.tcl]

  set bld [add_build_job Zipvfs $TRG(testfixture)]
  foreach s [zipvfs_testrunner_files] {
    set cmd "[file join [lindex $bld 1] $TRG(testfixture)] $s"
    add_job                                  \
        -displaytype tcl                     \
        -displayname "Zipvfs [file tail $s]" \
        -cmd $cmd                            \
        -depid [lindex $bld 0]
  }

  set ::env(SQLITE_TEST_DIR) $::testdir
}

# Used to add jobs for "mdevtest" and "sdevtest".
#
proc add_devtest_jobs {lBld patternlist} {
  global TRG

  foreach b $lBld {
    set bld [add_build_job $b $TRG(testfixture)]
    add_tcl_jobs $bld veryquick $patternlist SHELL
    if {$patternlist==""} {
      add_fuzztest_jobs $b
    }

    if {[trdb one "SELECT EXISTS (SELECT 1 FROM jobs WHERE depid='SHELL')"]} {
      set sbld [add_shell_build_job $b [lindex $bld 1] [lindex $bld 0]]
      set sbldid [lindex $sbld 0]
      trdb eval {
        UPDATE jobs SET depid=$sbldid WHERE depid='SHELL'
      }
    }

  }
}

# Check to ensure that the interpreter is a full-blown "testfixture"
# build and not just a "tclsh".  If this is not the case, issue an
# error message and exit.
#
proc must_be_testfixture {} {
  if {[lsearch [info commands] sqlite3_soft_heap_limit]<0} {
    puts "Use testfixture, not tclsh, for these arguments."
    exit 1
  }
}

proc add_jobs_from_cmdline {patternlist} {
  global TRG

  if {$TRG(zipvfs)!=""} {
    add_zipvfs_jobs
    if {[llength $patternlist]==0} return
  }

  if {[llength $patternlist]==0} {
    set patternlist [list veryquick]
  }

  set first [lindex $patternlist 0]
  switch -- $first {
    all {
      must_be_testfixture
      set patternlist [lrange $patternlist 1 end]
      set clist [trd_all_configs]
      foreach c $clist {
        add_tcl_jobs "" $c $patternlist
      }
    }

    devtest -
    mdevtest {
      set config_set {
        All-O0
        All-Debug
      }
      add_devtest_jobs $config_set [lrange $patternlist 1 end]
    }

    sdevtest {
      set config_set {
        All-Sanitize
        All-Debug
      }
      add_devtest_jobs $config_set [lrange $patternlist 1 end]
    }

    release {
      set patternlist [lrange $patternlist 1 end]
      foreach b [trd_builds $TRG(platform)] {
        if {$TRG(config)!="" && ![regexp "\\y$b\\y" $TRG(config)]} continue
        if {[regexp "\\y$b\\y" $TRG(omitconfig)]} continue
        set bld [add_build_job $b $TRG(testfixture)]
        foreach c [trd_configs $TRG(platform) $b] {
          add_tcl_jobs $bld $c $patternlist SHELL
        }

        if {$patternlist==""} {
          foreach e [trd_extras $TRG(platform) $b] {
            if {$e=="fuzztest"} {
              add_fuzztest_jobs $b
            } else {
              add_make_job $bld $e
            }
          }
        }

        if {[trdb one "SELECT EXISTS(SELECT 1
                                       FROM jobs WHERE depid='SHELL')"]} {
          set sbld [add_shell_build_job $b [lindex $bld 1] [lindex $bld 0]]
          set sbldid [lindex $sbld 0]
          trdb eval {
            UPDATE jobs SET depid=$sbldid WHERE depid='SHELL'
          }
        }
      }
    }

    list {
      set allperm [array names ::testspec]
      lappend allperm all mdevtest sdevtest release list
      puts "Allowed values for the PERMUTATION argument: [lsort $allperm]"
      exit 0
    }

    default {
      must_be_testfixture
      if {[info exists ::testspec($first)]} {
        add_tcl_jobs "" $first [lrange $patternlist 1 end]
      } else {
        add_tcl_jobs "" full $patternlist
      }
    }
  }
}

proc make_new_testset {} {
  global TRG

  trdb eval {PRAGMA journal_mode=WAL;}
  r_write_db {
    trdb eval $TRG(schema)
    set nJob $TRG(nJob)
    set cmdline $TRG(cmdline)
    set tm [clock_milliseconds]
    trdb eval { REPLACE INTO config VALUES('njob', $nJob ); }
    trdb eval { REPLACE INTO config VALUES('cmdline', $cmdline ); }
    trdb eval { REPLACE INTO config VALUES('start', $tm ); }

    add_jobs_from_cmdline $TRG(patternlist)
  }

}

proc mark_job_as_finished {jobid output state endtm} {
  set ntest 1
  set nerr 0
  if {$endtm>0} {
    set re {\y(\d+) errors out of (\d+) tests( on [^\n]+\n)?}
    if {[regexp $re $output all a b pltfm]} {
      set nerr $a
      set ntest $b
    }
    regexp {\ySQLite \d\d\d\d-\d\d-\d\d \d\d:\d\d:\d\d [0-9a-fA-F]+} \
         $output svers
  }
  r_write_db {
    if {$state=="failed"} {
      set childstate omit
      if {$nerr<=0} {set nerr 1}
    } else {
      set childstate ready
    }
    if {[info exists pltfm]} {set pltfm [string trim $pltfm]}
    trdb eval {
      UPDATE jobs 
        SET output=$output, state=$state, endtime=$endtm,
            ntest=$ntest, nerr=$nerr, svers=$svers, pltfm=$pltfm
        WHERE jobid=$jobid;
      UPDATE jobs SET state=$childstate WHERE depid=$jobid;
    }
  }
}

proc script_input_ready {fd iJob jobid} {
  global TRG
  global O
  global T

  if {[eof $fd]} {
    trdb eval { SELECT * FROM jobs WHERE jobid=$jobid } job {}

    # If this job specified a directory name, then delete the run.sh/run.bat
    # file from it before continuing. This is because the contents of this
    # directory might be copied by some other job, and we don't want to copy
    # the run.sh file in this case.
    if {$job(dirname)!=""} {
      file delete -force [file join $job(dirname) $TRG(run)]
    }

    set ::done 1
    fconfigure $fd -blocking 1
    set state "done"
    set rc [catch { close $fd } msg]
    if {$rc} { 
      if {[info exists TRG(reportlength)]} {
        puts -nonewline "[string repeat " " $TRG(reportlength)]\r"
      }
      puts "FAILED: $job(displayname) ($iJob)"
      set state "failed" 
      if {$TRG(stopOnError)} {
        puts "OUTPUT: $O($iJob)"
        exit 1
      }
      if {$TRG(stopOnCore) && [string first {core dumped} $O($iJob)]>0} {
        puts "OUTPUT: $O($iJob)"
        exit 1
      }
    }

    set tm [clock_milliseconds]
    set jobtm [expr {$tm - $job(starttime)}]

    puts $TRG(log) "### $job(displayname) ${jobtm}ms ($state)"
    puts $TRG(log) [string trim $O($iJob)]

    mark_job_as_finished $jobid $O($iJob) $state $tm

    dirs_freeDir $iJob
    launch_some_jobs
    incr ::wakeup
  } else {
    set rc [catch { gets $fd line } res]
    if {$rc} {
      puts "ERROR $res"
    } 
    if {$res>=0} {
      append O($iJob) "$line\n"
    }
  }

}

proc dirname {ii} {
  return "testdir$ii"
}

proc launch_another_job {iJob} {
  global TRG
  global O
  global T

  set testfixture [info nameofexec]
  set script $TRG(info_script)

  set O($iJob) ""
  
  set jobdict [r_get_next_job $iJob]
  if {$jobdict==""} { return 0 }
  array set job $jobdict

  set dir $job(dirname)
  if {$dir==""} { set dir [dirname $iJob] }
  create_or_clear_dir $dir

  if {$job(build)!=""} {
    set srcdir [file dirname $::testdir]
    if {$job(build)=="Zipvfs"} {
      set script [zipvfs_testrunner_script]
    } else {
      set bWin [expr {$TRG(platform)=="win"}]
      set script [trd_buildscript $job(build) $srcdir $bWin]
    }
    set fd [open [file join $dir $TRG(make)] w]
    puts $fd $script
    close $fd
  }

  # Add a batch/shell file command to set the directory used for temp
  # files to the test's working directory. Otherwise, tests that use
  # large numbers of temp files (e.g. zipvfs), might generate temp 
  # filename collisions.
  if {$TRG(platform)=="win"} {
    set set_tmp_dir "SET SQLITE_TMPDIR=[file normalize $dir]"
  } else {
    set set_tmp_dir "export SQLITE_TMPDIR=\"[file normalize $dir]\""
  }

  if { $TRG(dryrun) } {

    mark_job_as_finished $job(jobid) "" done 0
    dirs_freeDir $iJob
    if {$job(build)!=""} {
      puts $TRG(log) "(cd $dir ; $job(cmd) )"
    } else {
      puts $TRG(log) "$job(cmd)"
    }

  } else {
    set pwd [pwd]
    cd $dir
    set fd [open $TRG(run) w]
    puts $fd $set_tmp_dir
    puts $fd $job(cmd)
    close $fd
    set fd [open "|$TRG(runcmd) 2>@1" r]
    cd $pwd

    fconfigure $fd -blocking false -translation binary
    fileevent $fd readable [list script_input_ready $fd $iJob $job(jobid)]
  }

  return 1
}

# Show the testing progress report
#
proc progress_report {} {
  global TRG

  if {$TRG(fullstatus)} {
    if {$::tcl_platform(platform)=="windows"} {
      exec [info nameofexe] $::argv0 status --cls
    } else {
      show_status trdb 1
    }
  } else {
    set tm [expr [clock_milliseconds] - $TRG(starttime)]
    set tm [format "%d" [expr int($tm/1000.0 + 0.5)]]
  
    r_write_db {
      trdb eval { 
        SELECT displaytype, state, count(*) AS cnt 
        FROM jobs 
        GROUP BY 1, 2 
      } {
        set v($state,$displaytype) $cnt
        incr t($displaytype) $cnt
      }
    }
  
    set text ""
    foreach j [lsort [array names t]] {
      foreach k {done failed running} { incr v($k,$j) 0 }
      set fin [expr $v(done,$j) + $v(failed,$j)]
      lappend text "${j}($fin/$t($j))"
      if {$v(failed,$j)>0} {
        lappend text "f$v(failed,$j)"
      }
      if {$v(running,$j)>0} {
        lappend text "r$v(running,$j)"
      }
    }
  
    if {[info exists TRG(reportlength)]} {
      puts -nonewline "[string repeat " " $TRG(reportlength)]\r"
    }
    set report "${tm} [join $text { }]"
    set TRG(reportlength) [string length $report]
    if {[string length $report]<100} {
      puts -nonewline "$report\r"
      flush stdout
    } else {
      puts $report
    }
  }
  after $TRG(reporttime) progress_report
}

proc launch_some_jobs {} {
  global TRG
  set nJob [trdb one { SELECT value FROM config WHERE name='njob' }]

  while {[dirs_nHelper]<$nJob} {
    set iDir [dirs_allocDir]
    if {0==[launch_another_job $iDir]} {
      dirs_freeDir $iDir
      break;
    }
  }
}

proc run_testset {} {
  global TRG
  set ii 0

  set TRG(starttime) [clock_milliseconds]
  set TRG(log) [open $TRG(logname) w]

  launch_some_jobs

  if {$TRG(fullstatus)} {puts "\033\[2J"}
  progress_report
  while {[dirs_nHelper]>0} {
    after 500 {incr ::wakeup}
    vwait ::wakeup
  }
  close $TRG(log)
  progress_report

  r_write_db {
    set tm [clock_milliseconds]
    trdb eval { REPLACE INTO config VALUES('end', $tm ); }
    set nErr [trdb one {SELECT count(*) FROM jobs WHERE state='failed'}]
    if {$nErr>0} {
      puts "$nErr failures:"
      trdb eval {
        SELECT displayname FROM jobs WHERE state='failed'
      } {
        puts "FAILED: $displayname"
      }
    }
    set nOmit [trdb one {SELECT count(*) FROM jobs WHERE state='omit'}]
    if {$nOmit>0} {
      puts "$nOmit jobs skipped due to prior failures"
    }
  }

  puts "\nTest database is $TRG(dbname)"
  puts "Test log is $TRG(logname)"
  trdb eval {
     SELECT sum(ntest) AS totaltest,
            sum(nerr) AS totalerr
       FROM jobs
  } break
  trdb eval {
     SELECT max(endtime)-min(starttime) AS totaltime
       FROM jobs WHERE endtime>0
  } break;
  set et [elapsetime $totaltime]
  set pltfm {}
  trdb eval {
     SELECT pltfm, count(*) FROM jobs WHERE pltfm IS NOT NULL
      ORDER BY 2 DESC LIMIT 1
  } break
  puts "$totalerr errors out of $totaltest tests in $et $pltfm"
  trdb eval {
     SELECT DISTINCT substr(svers,1,79) as v1 FROM jobs WHERE svers IS NOT NULL
  } {puts $v1}

}

# Handle the --buildonly option, if it was specified.
#
proc handle_buildonly {} {
  global TRG
  if {$TRG(buildonly)} {
    r_write_db {
      trdb eval { DELETE FROM jobs WHERE displaytype!='bld' }
    }
  }
}

# Handle the --explain option.  Provide a human-readable
# explanation of all the tests that are in the trdb database jobs
# table.
#
proc explain_layer {indent depid} {
  global TRG
  if {$TRG(buildonly)} {
    set showtests 0
  } else {
    set showtests 1
  }
  trdb eval {SELECT jobid, displayname, displaytype, dirname
               FROM jobs WHERE depid=$depid ORDER BY displayname} {
    if {$displaytype=="bld"} {
      puts "${indent}$displayname in $dirname"
      explain_layer "${indent}   " $jobid
    } elseif {$showtests} {
      set tail [lindex $displayname end]
      set e1 [lindex $displayname 1]
      if {[string match config=* $e1]} {
        set cfg [string range $e1 7 end]
        puts "${indent}($cfg) $tail"
      } else {
        puts "${indent}$tail"
      }
    }
  }
}
proc explain_tests {} {
  explain_layer "" ""
}

sqlite3 trdb $TRG(dbname)
trdb timeout $TRG(timeout)
set tm [lindex [time { make_new_testset }] 0]
if {$TRG(explain)} {
  explain_tests
} else {
  if {$TRG(nJob)>1} {
    puts "splitting work across $TRG(nJob) cores"
  }
  puts "built testset in [expr $tm/1000]ms.."
  handle_buildonly
  run_testset
}
trdb close

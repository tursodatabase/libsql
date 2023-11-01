
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
  set interpreter [file tail [info nameofexec]]
  set rc [catch { package require sqlite3 }]
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
    puts stderr "Failed to find tcl package sqlite3"
    puts stderr "Run \"make testfixture\" and then try again..."
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
    $a0 njob ?NJOB?
    $a0 status

  where SWITCHES are:
    --jobs NUMBER-OF-JOBS
    --zipvfs ZIPVFS-SOURCE-DIR

Interesting values for PERMUTATION are:

    veryquick - a fast subset of the tcl test scripts. This is the default.
    full      - all tcl test scripts.
    all       - all tcl test scripts, plus a subset of test scripts rerun
                with various permutations.
    release   - full release test with various builds.

If no PATTERN arguments are present, all tests specified by the PERMUTATION
are run. Otherwise, each pattern is interpreted as a glob pattern. Only
those tcl tests for which the final component of the filename matches at
least one specified pattern are run.

If no PATTERN arguments are present, then various fuzztest, threadtest
and other tests are run as part of the "release" permutation. These are
omitted if any PATTERN arguments are specified on the command line.

If a PERMUTATION is specified and is followed by the path to a Tcl script
instead of a list of patterns, then that single Tcl test script is run
with the specified permutation.

The "status" and "njob" commands are designed to be run from the same
directory as a running testrunner.tcl script that is running tests. The
"status" command prints a report describing the current state and progress 
of the tests. The "njob" command may be used to query or modify the number
of sub-processes the test script uses to run tests.
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

switch -nocase -glob -- $tcl_platform(os) {
  *darwin* {
    set TRG(platform)    osx
    set TRG(make)        make.sh
    set TRG(makecmd)     "bash make.sh"
    set TRG(testfixture) testfixture
    set TRG(run)         run.sh
    set TRG(runcmd)      "bash run.sh"
  }
  *linux* {
    set TRG(platform)    linux
    set TRG(make)        make.sh
    set TRG(makecmd)     "bash make.sh"
    set TRG(testfixture) testfixture
    set TRG(run)         run.sh
    set TRG(runcmd)      "bash run.sh"
  }
  *win* {
    set TRG(platform)    win
    set TRG(make)        make.bat
    set TRG(makecmd)     make.bat
    set TRG(testfixture) testfixture.exe
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
    starttime INTEGER, 
    endtime INTEGER,
    state TEXT CHECK( state IN ('', 'ready', 'running', 'done', 'failed') ),
    output TEXT
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
    if {[string is integer $param]==0 || $param<1 || $param>128} {
      puts stderr "parameter must be an integer between 1 and 128"
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
  

#--------------------------------------------------------------------------
# Check if this is the "status" command:
#
if {[llength $argv]==1 
 && [string compare -nocase status [lindex $argv 0]]==0 
} {

  proc display_job {jobdict {tm ""}} {
    array set job $jobdict

    set dfname [format %-60s $job(displayname)]

    set dtm ""
    if {$tm!=""} { set dtm "\[[expr {$tm-$job(starttime)}]ms\]" }
    puts "  $dfname $dtm"
  }

  sqlite3 mydb $TRG(dbname)
  mydb timeout 1000
  mydb eval BEGIN

  set cmdline [mydb one { SELECT value FROM config WHERE name='cmdline' }]
  set nJob [mydb one { SELECT value FROM config WHERE name='njob' }]

  set now [clock_milliseconds]
  set tm [mydb one {
    SELECT 
      COALESCE((SELECT value FROM config WHERE name='end'), $now) -
      (SELECT value FROM config WHERE name='start')
  }]

  set total 0
  foreach s {"" ready running done failed} { set S($s) 0 }
  mydb eval {
    SELECT state, count(*) AS cnt FROM jobs GROUP BY 1
  } {
    incr S($state) $cnt
    incr total $cnt
  }
  set fin [expr $S(done)+$S(failed)]
  if {$cmdline!=""} {set cmdline " $cmdline"}

  set f ""
  if {$S(failed)>0} {
    set f "$S(failed) FAILED, "
  }
  puts "Command line: \[testrunner.tcl$cmdline\]"
  puts "Jobs:         $nJob"
  puts "Summary:      ${tm}ms, ($fin/$total) finished, ${f}$S(running) running"

  set srcdir [file dirname [file dirname $TRG(info_script)]]
  if {$S(running)>0} {
    puts "Running: "
    mydb eval {
      SELECT * FROM jobs WHERE state='running' ORDER BY starttime 
    } job {
      display_job [array get job] $now
    }
  }
  if {$S(failed)>0} {
    puts "Failures: "
    mydb eval {
      SELECT * FROM jobs WHERE state='failed' ORDER BY starttime
    } job {
      display_job [array get job]
    }
  }
 
  mydb close
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

#rename r_get_next_job r_get_next_job_r
#proc r_get_next_job {iJob} {
  #puts [time { set res [r_get_next_job_r $iJob] }]
  #set res
#}

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

proc add_tcl_jobs {build config patternlist} {
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
        if {[string match $p [file tail $f]]} {
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

    add_job                            \
        -displaytype tcl               \
        -displayname $displayname      \
        -cmd $cmd                      \
        -depid [lindex $build 0]       \
        -priority $priority

  }
}

proc add_build_job {buildname target} {
  global TRG

  set dirname "[string tolower [string map {- _} $buildname]]_$target"
  set dirname "testrunner_bld_$dirname"

  set id [add_job                                \
    -displaytype bld                             \
    -displayname "Build $buildname ($target)"    \
    -dirname $dirname                            \
    -build $buildname                            \
    -cmd  "$TRG(makecmd) $target"                \
    -priority 3
  ]

  list $id [file normalize $dirname] $buildname
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
      set patternlist [lrange $patternlist 1 end]
      set clist [trd_all_configs]
      foreach c $clist {
        add_tcl_jobs "" $c $patternlist
      }
    }

    mdevtest {
      foreach b [list All-O0 All-Debug] {
        set bld [add_build_job $b $TRG(testfixture)]
        add_tcl_jobs $bld veryquick ""
        add_fuzztest_jobs $b
      }
    }

    sdevtest {
      foreach b [list All-Sanitize All-Debug] {
        set bld [add_build_job $b $TRG(testfixture)]
        add_tcl_jobs $bld veryquick ""
        add_fuzztest_jobs $b
      }
    }

    release {
      foreach b [trd_builds $TRG(platform)] {
        set bld [add_build_job $b $TRG(testfixture)]
        foreach c [trd_configs $TRG(platform) $b] {
          add_tcl_jobs $bld $c ""
        }

        foreach e [trd_extras $TRG(platform) $b] {
          if {$e=="fuzztest"} {
            add_fuzztest_jobs $b
          } else {
            add_make_job $bld $e
          }
        }
      }
    }

    default {
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
    }

    set tm [clock_milliseconds]
    set jobtm [expr {$tm - $job(starttime)}]

    puts $TRG(log) "### $job(displayname) ${jobtm}ms ($state)"
    puts $TRG(log) [string trim $O($iJob)]

    r_write_db {
      set output $O($iJob)
      trdb eval {
        UPDATE jobs 
          SET output=$output, state=$state, endtime=$tm
          WHERE jobid=$jobid;
        UPDATE jobs SET state='ready' WHERE depid=$jobid;
      }
    }

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

  set pwd [pwd]
  cd $dir
  set fd [open $TRG(run) w]
  puts $fd $job(cmd) 
  close $fd
  set fd [open "|$TRG(runcmd) 2>@1" r]
  cd $pwd

  fconfigure $fd -blocking false
  fileevent $fd readable [list script_input_ready $fd $iJob $job(jobid)]

  return 1
}

proc one_line_report {} {
  global TRG

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

  after $TRG(reporttime) one_line_report
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

  one_line_report
  while {[dirs_nHelper]>0} {
    after 500 {incr ::wakeup}
    vwait ::wakeup
  }
  close $TRG(log)
  one_line_report

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
  }

  puts "\nTest database is $TRG(dbname)"
  puts "Test log is $TRG(logname)"
}


sqlite3 trdb $TRG(dbname)
trdb timeout $TRG(timeout)
set tm [lindex [time { make_new_testset }] 0]
if {$TRG(nJob)>1} {
  puts "splitting work across $TRG(nJob) jobs"
}
puts "built testset in [expr $tm/1000]ms.."
run_testset
trdb close
#puts [pwd]

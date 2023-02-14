
set dir [pwd]
set testdir [file dirname $argv0]
set saved $argv
set argv [list]
source [file join $testdir testrunner_data.tcl]
source [file join $testdir permutations.test]
set argv $saved
cd $dir

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
    --fuzztest
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

The --fuzztest option is ignored if the PERMUTATION is "release". Otherwise,
if it is present, then "make -C <dir> fuzztest" is run as part of the tests,
where <dir> is the directory containing the testfixture binary used to
run the script.

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
    set TRG(platform) osx
    set TRG(make)     make.sh
    set TRG(makecmd)  "bash make.sh"
  }
  *linux* {
    set TRG(platform) linux
    set TRG(make)     make.sh
    set TRG(makecmd)  "bash make.sh"
  }
  *win* {
    set TRG(platform) win
    set TRG(make)     make.bat
    set TRG(makecmd)  make.bat
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
  DROP TABLE IF EXISTS script;
  DROP TABLE IF EXISTS config;

  CREATE TABLE script(
    build TEXT DEFAULT '',
    config TEXT,
    filename TEXT,                -- full path to test script
    slow BOOLEAN,                 -- true if script is "slow"
    state TEXT CHECK( state IN ('', 'ready', 'running', 'done', 'failed') ),
    time INTEGER,                 -- Time in ms
    output TEXT,                  -- full output of test script
    priority AS ((config='make') + ((config='build')*2) + (slow*4)),
    jobtype AS (
      CASE WHEN config IN ('build', 'make') THEN config ELSE 'script' END
    ),
    PRIMARY KEY(build, config, filename)
  );

  CREATE TABLE config(
    name TEXT COLLATE nocase PRIMARY KEY,
    value 
  ) WITHOUT ROWID;

  CREATE INDEX i1 ON script(state, jobtype);
  CREATE INDEX i2 ON script(state, priority);
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

  if {$permutation=="full"} {

    set testdir [file dirname $argv0]
    source $::testdir/tester.tcl
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
# Check if this is the "status" command:
#
if {[llength $argv]==1 
 && [string compare -nocase status [lindex $argv 0]]==0 
} {

  proc display_job {build config filename {tm ""}} {
    if {$config=="build"} {
      set fname "build: $filename"
      set config ""
    } elseif {$config=="make"} {
      set fname "make: $filename"
      set config ""
    } else {
      set fname [file normalize $filename]
      if {[string first $::srcdir $fname]==0} {
        set fname [string range $fname [string length $::srcdir]+1 end]
      }
    }
    set dfname [format %-33s $fname]

    set dbuild ""
    set dconfig ""
    set dparams ""
    set dtm ""
    if {$build!=""} { set dbuild $build }
    if {$config!="" && $config!="full"} { set dconfig $config }
    if {$dbuild!="" || $dconfig!=""} {
      append dparams "("
      if {$dbuild!=""}                 {append dparams "build=$dbuild"}
      if {$dbuild!="" && $dconfig!=""} {append dparams " "}
      if {$dconfig!=""}                {append dparams "config=$dconfig"}
      append dparams ")"
      set dparams [format %-33s $dparams]
    }
    if {$tm!=""} {
      set dtm "\[${tm}ms\]"
    }
    puts "  $dfname $dparams $dtm"
  }

  sqlite3 mydb $TRG(dbname)
  mydb timeout 1000
  mydb eval BEGIN

  set cmdline [mydb one { SELECT value FROM config WHERE name='cmdline' }]
  set nJob [mydb one { SELECT value FROM config WHERE name='njob' }]
  set tm [expr [clock_milliseconds] - [mydb one {
    SELECT value FROM config WHERE name='start'
  }]]

  set total 0
  foreach s {"" ready running done failed} { set S($s) 0 }
  mydb eval {
    SELECT state, count(*) AS cnt FROM script GROUP BY 1
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
    set now [clock_milliseconds]
    mydb eval {
      SELECT build, config, filename, time FROM script WHERE state='running'
      ORDER BY time 
    } {
      display_job $build $config $filename [expr $now-$time]
    }
  }
  if {$S(failed)>0} {
    puts "Failures: "
    mydb eval {
      SELECT build, config, filename FROM script WHERE state='failed'
      ORDER BY 3
    } {
      display_job $build $config $filename
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
    } elseif {($n>2 && [string match "$a*" --fuzztest]) || $a=="-f"} {
      set TRG(fuzztest) 1
    } elseif {($n>2 && [string match "$a*" --zipvfs]) || $a=="-z"} {
      incr ii
      set TRG(zipvfs) [lindex $argv $ii]
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

set testdir [file dirname $argv0]

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

proc copy_dir {from to} {
  foreach f [glob -nocomplain [file join $from *]] {
    catch { file copy -force $f $to }
  }
}

proc build_to_dirname {bname} {
  set fold [string tolower [string map {- _} $bname]]
  return "testrunner_build_$fold"
}

#-------------------------------------------------------------------------
# Return a list of tests to run. Each element of the list is itself a
# list of two elements - the name of a permuations.test configuration
# followed by the full path to a test script. i.e.:
#
#    {BUILD CONFIG FILENAME} {BUILD CONFIG FILENAME} ...
#
proc testset_patternlist {patternlist} {
  global TRG

  set testset [list]              ;# return value

  set first [lindex $patternlist 0]

  if {$first=="release"} {
    set platform $::TRG(platform)

    set patternlist [lrange $patternlist 1 end]
    foreach b [trd_builds $platform] {
      foreach c [trd_configs $platform $b] {
        testset_append testset $b $c $patternlist
      }

      if {[llength $patternlist]==0 || $b=="User-Auth"} {
        set target testfixture
      } else {
        set target coretestprogs
      }
      lappend testset [list $b build $target]
    }

    if {[llength $patternlist]==0} {
      foreach b [trd_builds $platform] {
        foreach e [trd_extras $platform $b] {
          lappend testset [list $b make $e]
        }
      }
    }

    set TRG(fuzztest) 0           ;# ignore --fuzztest option in this case

  } elseif {$first=="all"} {

    set clist [trd_all_configs]
    set patternlist [lrange $patternlist 1 end]
    foreach c $clist {
      testset_append testset "" $c $patternlist
    }

  } elseif {[info exists ::testspec($first)]} {
    set clist $first
    testset_append testset "" $first [lrange $patternlist 1 end]
  } elseif { [llength $patternlist]==0 } {
    testset_append testset "" veryquick $patternlist
  } else {
    testset_append testset "" full $patternlist
  }
  if {$TRG(fuzztest)} {
    if {$TRG(platform)=="win"} { error "todo" }
    lappend testset [list "" make fuzztest]
  }

  set testset
}

proc testset_append {listvar build config patternlist} {
  upvar $listvar lvar

  catch { array unset O }
  array set O $::testspec($config)

  foreach f $O(-files) {
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

    if {[file pathtype $f]!="absolute"} {
      set f [file join $::testdir $f]
    }
    lappend lvar [list $build $config $f]
  }
}

#--------------------------------------------------------------------------


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

  r_write_db {
    set f ""
    set c ""
    trdb eval "
      SELECT build, config, filename 
        FROM script 
        WHERE state='ready' 
        $orderby LIMIT 1
    " {
      set b $build
      set c $config
      set f $filename
    }
    if {$f!=""} {
      set tm [clock_milliseconds]
      set T($iJob) $tm
      trdb eval { 
        UPDATE script SET state='running', time=$tm
        WHERE (build, config, filename) = ($b, $c, $f)
      }
    }
  }

  if {$f==""} { return "" }
  list $b $c $f
}

#rename r_get_next_job r_get_next_job_r
#proc r_get_next_job {iJob} {
#  puts [time { set res [r_get_next_job_r $iJob] }]
#  set res
#}

proc make_new_testset {} {
  global TRG

  set tests [testset_patternlist $TRG(patternlist)]

  if {$TRG(zipvfs)!=""} {
    source [file join $TRG(zipvfs) test zipvfs_testrunner.tcl]
    set tests [concat $tests [zipvfs_testrunner_testset]]
  }

  r_write_db {

    trdb eval $TRG(schema)
    set nJob $TRG(nJob)
    set cmdline $TRG(cmdline)
    set tm [clock_milliseconds]
    trdb eval { REPLACE INTO config VALUES('njob', $nJob ); }
    trdb eval { REPLACE INTO config VALUES('cmdline', $cmdline ); }
    trdb eval { REPLACE INTO config VALUES('start', $tm ); }

    foreach t $tests {
      foreach {b c s} $t {}
      set slow 0

      if {$c!="make" && $c!="build"} {
        set fd [open $s]
        for {set ii 0} {$ii<100 && ![eof $fd]} {incr ii} {
          set line [gets $fd]
          if {[string match -nocase *testrunner:* $line]} {
            regexp -nocase {.*testrunner:(.*)} $line -> properties
            foreach p $properties {
              if {$p=="slow"} { set slow 1 }
              if {$p=="superslow"} { set slow 2 }
            }
          }
        }
        close $fd
      }

      if {$c=="make" && $b==""} {
        # --fuzztest option
        set slow 1
      }

      if {$c=="veryquick"} {
        set c ""
      }

      set state ready
      if {$b!="" && $c!="build"} {
        set state ""
      }

      trdb eval { 
        INSERT INTO script(build, config, filename, slow, state) 
            VALUES ($b, $c, $s, $slow, $state) 
      }
    }
  }
}

proc script_input_ready {fd iJob b c f} {
  global TRG
  global O
  global T

  if {[eof $fd]} {
    set ::done 1
    fconfigure $fd -blocking 1
    set state "done"
    set rc [catch { close $fd } msg]
    if {$rc} { 
      puts "FAILED: $b $c $f"
      set state "failed" 
    }

    set tm [expr [clock_milliseconds] - $T($iJob)]

    puts $TRG(log) "### $b ### $c ### $f ${tm}ms ($state)"
    puts $TRG(log) [string trim $O($iJob)]

    r_write_db {
      set output $O($iJob)
      trdb eval {
        UPDATE script SET output = $output, state=$state, time=$tm
        WHERE (build, config, filename) = ($b, $c, $f)
      }
      if {$state=="done" && $c=="build"} {
        trdb eval {
          UPDATE script SET state = 'ready' WHERE (build, state)==($b, '')
        }
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

  set dir [dirname $iJob]
  create_or_clear_dir $dir

  set O($iJob) ""
  
  set job [r_get_next_job $iJob]
  if {$job==""} { return 0 }

  foreach {b c f} $job {}

  if {$c=="build"} {
    set testdir [file dirname $TRG(info_script)]
    set srcdir [file dirname $testdir]
    set builddir [build_to_dirname $b]
    create_or_clear_dir $builddir

    if {$b=="Zipvfs"} {
      set script [zipvfs_testrunner_script]
    } else {
      set     cmd [info nameofexec]
      lappend cmd [file join $testdir releasetest_data.tcl]
      lappend cmd trscript
      if {$TRG(platform)=="win"} { lappend cmd -msvc }
      lappend cmd $b $srcdir
      set script [exec {*}$cmd]
    }

    set fd [open [file join $builddir $TRG(make)] w]
    puts $fd $script
    close $fd

    puts "Launching build \"$b\" in directory $builddir..."
    set target coretestprogs
    if {$b=="User-Auth"}  { set target testfixture }

    set cmd "$TRG(makecmd) $target"
    set dir $builddir

  } elseif {$c=="make"} {
    if {$b==""} {
      if {$f!="fuzztest"} { error "corruption in testrunner.db!" }
      # Special case - run [make fuzztest] 
      set makedir [file dirname $testfixture]
      if {$TRG(platform)=="win"} {
        error "how?"
      } else {
        set cmd [list make -C $makedir fuzztest]
      }
    } else {
      set builddir [build_to_dirname $b]
      copy_dir $builddir $dir
      set cmd "$TRG(makecmd) $f"
    }
  } else {
    if {$b==""} {
      set testfixture [info nameofexec]
    } else {
      set tail testfixture
      if {$TRG(platform)=="win"} { set tail testfixture.exe }
      set testfixture [file normalize [file join [build_to_dirname $b] $tail]]
    }

    if {$c=="valgrind"} {
      set testfixture "valgrind -v --error-exitcode=1 $testfixture"
      set ::env(OMIT_MISUSE) 1
    }
    set cmd [concat $testfixture [list $script $c $f]]
  }

  set pwd [pwd]
  cd $dir
  set fd [open "|$cmd 2>@1" r]
  cd $pwd
  set pid [pid $fd]

  fconfigure $fd -blocking false
  fileevent $fd readable [list script_input_ready $fd $iJob $b $c $f]
  unset -nocomplain ::env(OMIT_MISUSE)

  return 1
}

proc one_line_report {} {
  global TRG

  set tm [expr [clock_milliseconds] - $TRG(starttime)]
  set tm [format "%d" [expr int($tm/1000.0 + 0.5)]]

  foreach s {ready running done failed} {
    set v($s,build) 0
    set v($s,make) 0
    set v($s,script) 0
  }

  r_write_db {
    trdb eval {
      SELECT state, jobtype, count(*) AS cnt 
      FROM script 
      GROUP BY state, jobtype
    } {
      set v($state,$jobtype) $cnt
      if {[info exists t($jobtype)]} {
        incr t($jobtype) $cnt
      } else {
        set t($jobtype) $cnt
      }
    }
  }

  set text ""
  foreach j [array names t] {
    set fin [expr $v(done,$j) + $v(failed,$j)]
    lappend text "$j ($fin/$t($j)) f=$v(failed,$j) r=$v(running,$j)"
  }

  if {[info exists TRG(reportlength)]} {
    puts -nonewline "[string repeat " " $TRG(reportlength)]\r"
  }
  set report "${tm}s: [join $text { }]"
  set TRG(reportlength) [string length $report]
  if {[string length $report]<80} {
    puts -nonewline "$report\r"
    flush stdout
  } else {
    puts $report
  }

  after $TRG(reporttime) one_line_report
}

proc launch_some_jobs {} {
  global TRG
  r_write_db {
    set nJob [trdb one { SELECT value FROM config WHERE name='njob' }]
  }
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
    # launch_another_job $ii

  one_line_report
  while {[dirs_nHelper]>0} {
    after 500 {incr ::wakeup}
    vwait ::wakeup
  }
  close $TRG(log)
  one_line_report

  r_write_db {
    set nErr [trdb one {SELECT count(*) FROM script WHERE state='failed'}]
    if {$nErr>0} {
      puts "$nErr failures:"
      trdb eval {
        SELECT build, config, filename FROM script WHERE state='failed'
      } {
        puts "FAILED: $build $config $filename"
      }
    }
  }

  puts "\nTest database is $TRG(dbname)"
  puts "Test log is $TRG(logname)"
}


sqlite3 trdb $TRG(dbname)
trdb timeout $TRG(timeout)
set tm [lindex [time { make_new_testset }] 0]
puts "built testset in [expr $tm/1000]ms.."
run_testset
trdb close
#puts [pwd]

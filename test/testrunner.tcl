
source [file join [file dirname [info script]] testrunner_data.tcl]

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


# If this script is invoked using:
#
#   testrunner.tcl helper <directory> <permutation> <script>
#
if {[lindex $argv 0]=="helper"} {
  if {[llength $argv]!=3} { error "BAD ARGS" }

  set permutation [lindex $argv 1]
  set script [file normalize [lindex $argv 2]]

  set ::argv [list]

  if {$permutation=="full"} {

    set testdir [file dirname $argv0]
    source $::testdir/tester.tcl
    unset -nocomplain ::G(isquick)
    reset_db

  } elseif {$permutation!="default" && $permutation!=""} {
    set testdir [file dirname $argv0]
    source $::testdir/permutations.test

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
    reset_db
  }

  source $script
  exit
}

#-------------------------------------------------------------------------
# The database schema used by the testrunner.db database.
#
set R(schema) {
  DROP TABLE IF EXISTS script;

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

  CREATE INDEX i1 ON script(state, jobtype);
  CREATE INDEX i2 ON script(state, priority);
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
set R(build.logname) [file normalize testrunner_build.log]
set R(info_script) [file normalize [info script]]
set R(timeout) 10000              ;# Default busy-timeout for testrunner.db 
set R(nJob)    [default_njob]     ;# Default number of helper processes
set R(patternlist) [list]

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
set R(dirs_in_use) [list]

proc dirs_nHelper {} {
  global R
  llength $R(dirs_in_use)
}
proc dirs_freeDir {iDir} {
  global R
  set out [list]
  foreach d $R(dirs_in_use) {
    if {$iDir!=$d} { lappend out $d }
  }
  if {[llength $out]!=[llength $R(dirs_in_use)]-1} {
    error "dirs_freeDir could not find $iDir"
  }
  set R(dirs_in_use) $out
}
proc dirs_allocDir {} {
  global R
  array set inuse [list]
  foreach d $R(dirs_in_use) {
    set inuse($d) 1
  }
  for {set iRet 0} {[info exists inuse($iRet)]} {incr iRet} { }
  lappend R(dirs_in_use) $iRet
  return $iRet
}

switch -nocase -glob -- $tcl_platform(os) {
  *darwin* {
    set R(platform) osx
    set R(make)     make.sh
  }
  *linux* {
    set R(platform) linux
    set R(make)     make.sh
  }
  *win* {
    set R(platform) win
    set R(make)     make.bat
  }
  default {
    error "cannot determine platform!"
  }
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

##########################################################################
##########################################################################
proc build_to_dirname {bname} {
  set fold [string tolower [string map {- _} $bname]]
  return "testrunner_build_$fold"
}

proc build_input_ready {fd build} {
  global R
  global O

  if {[eof $fd]} {
    foreach {dirname b} $build {}

    fconfigure $fd -blocking 1
    set rc [catch { close $fd } msg]
    if {$rc} { 
      puts "Build \"$b\" finished - FAILED"
      lappend R(lBuildFail) $build
    } else {
      puts "Build \"$b\" finished - ok"
    }

    puts $R(log) "### Build \"$b\" in directory $dirname"
    puts $R(log) $O($fd)

    launch_another_build
    incr R(nHelperRunning) -1
    incr ::wakeup
  } else {
    if {[gets $fd line]>=0} {
      append O($fd) "$line\n"
    }
  }
  global R
}

proc launch_another_build {} {
  global R
  if {[llength $R(lBuild)]>0} {
    set build [lindex $R(lBuild) 0]
    set R(lBuild) [lrange $R(lBuild) 1 end]
    foreach {dirname b} $build {}

    puts "Launching build \"$b\" in directory $dirname..."
    set target coretestprogs
    if {$b=="User-Auth"}  { set target testfixture }

    incr R(nHelperRunning)

    set pwd [pwd]
    cd $dirname
    set fd [open "|bash $R(make) $target 2>@1"]
    cd $pwd

    set O($fd) ""
    fconfigure $fd -blocking 0
    fileevent $fd readable [list build_input_ready $fd $build]
  }
}

if {[lindex $argv 0]=="build"} {

  # Load configuration data.
  source [file join [file dirname [info script]] testrunner_data.tcl]
  set srcdir [file dirname [file dirname $R(info_script)]]

  foreach b [trd_builds $R(platform)] {
    set dirname [build_to_dirname $b]
    create_or_clear_dir $dirname

    set     cmd [info nameofexec]
    lappend cmd [file join [file dirname $R(info_script)] releasetest_data.tcl]
    if {$R(platform)=="win"} { lappend $cmd -msvc }
    lappend cmd script $b $srcdir

    set script [exec {*}$cmd]

    set fd [open [file join $dirname $R(make)] w]
    puts $fd $script
    close $fd

    lappend R(lBuild) [list $dirname $b]
  }

  set R(log) [open $R(build.logname) w]

  set R(nHelperRunning) 0
  set R(lBuildFail) [list]
  for {set ii 0} {$ii < $R(nJob)} {incr ii} {
    launch_another_build
  }

  while {$R(nHelperRunning)>0} {
    vwait ::wakeup
  }
  close $R(log)

  if {[llength $R(lBuildFail)]==0} {
    puts "All builds succeeded!"
  } else {
    puts "Builds failed:"
    foreach build $R(lBuildFail) {
      foreach {dirname b} $build {}
      puts "  $b ($dirname)"
    }
    exit 1
  }

  puts "Log file is $R(build.logname)"
  exit
}
##########################################################################
##########################################################################

set R(helper) 0
set R(helper_id) 0
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

set dir [pwd]
source $testdir/permutations.test
cd $dir

#-------------------------------------------------------------------------
# Return a list of tests to run. Each element of the list is itself a
# list of two elements - the name of a permuations.test configuration
# followed by the full path to a test script. i.e.:
#
#    {BUILD CONFIG FILENAME} {BUILD CONFIG FILENAME} ...
#
proc testset_patternlist {patternlist} {

  set testset [list]              ;# return value

  set first [lindex $patternlist 0]

  if {$first=="release"} {
    set platform $::R(platform)

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

  } elseif {$first=="all"} {

    set clist [trd_all_configs]
    set patternlist [lrange $patternlist 1 end]
    foreach c $clist {
      testset_append testset "" $c $patternlist
    }

  } elseif {[info exists ::testspec($first)]} {
    set clist $first
    set patternlist [lrange $patternlist 1 end]

    testset_append testset "" $first [lrange $patternlist 1 end]
  } elseif { [llength $patternlist]==0 } {
    testset_append testset "" veryquick $patternlist
  } else {
    testset_append testset "" full $patternlist
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
  global R

  sqlite3_test_control_pending_byte 0x010000
  sqlite3 db $R(dbname)
  db timeout $R(timeout)
  db eval { BEGIN EXCLUSIVE }

  uplevel $tcl

  db eval { COMMIT }
  db close
}

# Obtain a new job to be run by worker $iJob (an integer). A job is
# returned as a three element list:
#
#    {$build $config $file}
#
proc r_get_next_job {iJob} {

  if {($iJob%2)} {
    set orderby "ORDER BY priority ASC"
  } else {
    set orderby "ORDER BY priority DESC"
  }

  r_write_db {
    set f ""
    set c ""
    db eval "
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
      db eval { 
        UPDATE script SET state='running'
        WHERE (build, config, filename) = ($b, $c, $f)
      }
    }
  }

  if {$f==""} { return "" }
  list $b $c $f
}

#rename r_get_next_job r_get_next_job_r
#proc r_get_next_job {iJob} {
  #puts [time { set res [r_get_next_job_r $iJob] }]
  #set res
#}


proc make_new_testset {} {
  global R

  set tests [testset_patternlist $R(patternlist)]
  r_write_db {
    db eval $R(schema)
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

      if {$c=="veryquick"} {
        set c "default"
      }

      set state ready
      if {$b!="" && $c!="build"} {
        set state ""
      }

      db eval { 
        INSERT INTO script(build, config, filename, slow, state) 
            VALUES ($b, $c, $s, $slow, $state) 
      }
    }
  }
}

proc script_input_ready {fd iJob b c f} {
  global R
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

    puts $R(log) "### $b ### $c ### $f ${tm}ms ($state)"
    puts $R(log) [string trim $O($iJob)]

    incr R(nHelperRunning) -1
    r_write_db {
      set output $O($iJob)
      db eval {
        UPDATE script SET output = $output, state=$state, time=$tm
        WHERE (build, config, filename) = ($b, $c, $f)
      }
      if {$state=="done" && $c=="build"} {
        db eval {
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
  global R
  global O
  global T

  set testfixture [info nameofexec]
  set script $R(info_script)

  set dir [dirname $iJob]
  create_or_clear_dir $dir

  set O($iJob) ""
  set T($iJob) [clock_milliseconds]
  
  set job [r_get_next_job $iJob]
  if {$job==""} { return 0 }

  foreach {b c f} $job {}

  if {$c=="build"} {
    set srcdir [file dirname [file dirname $R(info_script)]]
    set builddir [build_to_dirname $b]
    create_or_clear_dir $builddir

    set     cmd [info nameofexec]
    lappend cmd [file join [file dirname $R(info_script)] releasetest_data.tcl]
    if {$R(platform)=="win"} { lappend $cmd -msvc }
    lappend cmd script $b $srcdir

    set script [exec {*}$cmd]
    set fd [open [file join $builddir $R(make)] w]
    puts $fd $script
    close $fd

    puts "Launching build \"$b\" in directory $builddir..."
    set target coretestprogs
    if {$b=="User-Auth"}  { set target testfixture }

    set cmd "bash $R(make) $target"
    set dir $builddir

  } elseif {$c=="make"} {
    set builddir [build_to_dirname $b]
    copy_dir $builddir $dir
    set cmd "bash $R(make) $f"
  } else {
    if {$b==""} {
      set testfixture [info nameofexec]
    } else {
      set testfixture [
        file normalize [file join [build_to_dirname $b] testfixture]
      ]
    }

    if {$c=="valgrind"} {
      set testfixture "valgrind -v --error-exitcode=1 $testfixture"
      set ::env(OMIT_MISUSE) 1
    }
    set cmd [concat $testfixture [list $script helper $c $f]]
  }

  set pwd [pwd]
  cd $dir
  set fd [open "|$cmd 2>@1" r]
  cd $pwd
  set pid [pid $fd]

  fconfigure $fd -blocking false
  fileevent $fd readable [list script_input_ready $fd $iJob $b $c $f]
  incr R(nHelperRunning) +1
  unset -nocomplain ::env(OMIT_MISUSE)

  return 1
}

proc one_line_report {} {
  global R

  set tm [expr [clock_milliseconds] - $R(starttime)]
  set tm [format "%.2f" [expr $tm/1000.0]]

  foreach s {ready running done failed} {
    set v($s,build) 0
    set v($s,make) 0
    set v($s,script) 0
  }

  r_write_db {
    db eval {
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
    lappend text "$j: ($fin/$t($j)) f=$v(failed,$j) r=$v(running,$j)"
  }

  puts "${tm}s: [join $text { || }]"
  after 1000 one_line_report
}

proc launch_some_jobs {} {
  global R
  while {[dirs_nHelper]<$R(nJob)} {
    set iDir [dirs_allocDir]
    if {0==[launch_another_job $iDir]} {
      dirs_freeDir $iDir
      break;
    }
  }
}

proc run_testset {} {
  global R
  set ii 0

  set R(starttime) [clock_milliseconds]
  set R(log) [open $R(logname) w]

  launch_some_jobs
    # launch_another_job $ii

  one_line_report
  while {$R(nHelperRunning)>0} {
    after 500 {incr ::wakeup}
    vwait ::wakeup
  }
  close $R(log)
  one_line_report

  r_write_db {
    set nErr [db one {SELECT count(*) FROM script WHERE state='failed'}]
    if {$nErr>0} {
      puts "$nErr failures:"
      db eval {
        SELECT build, config, filename FROM script WHERE state='failed'
      } {
        puts "FAILED: $build $config $filename"
      }
    }
  }

  puts "Test database is $R(dbname)"
  puts "Test log is $R(logname)"
}

set R(nHelperRunning) 0
set tm [lindex [time { make_new_testset }] 0]
puts "built testset in [expr $tm/1000]ms.."

run_testset
#puts [pwd]


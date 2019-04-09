#!/bin/sh 
# \
exec wapptclsh "$0" ${1+"$@"}

#
#
#

# Variables set by the "control" form:
#
#   G(platform) - User selected platform.
#   G(test)     - Set to "Normal", "Veryquick", "Smoketest" or "Build-Only".
#   G(keep)     - Boolean. True to delete no files after each test.
#   G(msvc)     - Boolean. True to use MSVC as the compiler.
#   G(tcl)      - Use Tcl from this directory for builds.
#   G(jobs)     - How many sub-processes to run simultaneously.
#
set G(platform) $::tcl_platform(os)-$::tcl_platform(machine)
set G(test)     Normal
set G(keep)     0
set G(msvc)     0
set G(tcl)      ""
set G(jobs)     3

set G(sqlite_version) unknown

# The root of the SQLite source tree.
#
set G(srcdir)   [file dirname [file dirname [info script]]]

# Either "config", "running", "stopped":
#
set G(state) "config"

# releasetest.tcl script
#
set G(releaseTest) [file join [file dirname [info script]] releasetest.tcl]

set G(cnt) 0

# package required wapp
source [file join [file dirname [info script]] wapp.tcl]

# Read the data from the releasetest_data.tcl script.
#
source [file join [file dirname [info script]] releasetest_data.tcl]

# Check to see if there are uncommitted changes in the SQLite source
# directory. Return true if there are, or false otherwise.
#
proc check_uncommitted {} {
  global G
  set ret 0
  set pwd [pwd]
  cd $G(srcdir)
  if {[catch {exec fossil changes} res]==0 && [string trim $res]!=""} {
    set ret 1
  }
  cd $pwd
  return $ret
}

# If the application is in "config" state, set the contents of the 
# ::G(test_array) global to reflect the tests that will be run. If the
# app is in some other state ("running" or "stopped"), this command
# is a no-op.
#
proc set_test_array {} {
  global G
  if { $G(state)=="config" } {
    set G(test_array) [list]
    foreach {config target} $::Platforms($G(platform)) {

      # If using MSVC, do not run sanitize or valgrind tests. Or the
      # checksymbols test.
      if {$G(msvc) && (
          "Sanitize" == $config 
       || "checksymbols" in $target
       || "valgrindtest" in $target
      )} {
        continue
      }

      # If the test mode is not "Normal", override the target.
      #
      if {$target!="checksymbols" && $G(platform)!="Failure-Detection"} {
        switch -- $G(test) {
          Veryquick { set target quicktest }
          Smoketest { set target smoketest }
          Build-Only {
            set target testfixture
            if {$::tcl_platform(platform)=="windows"} {
              set target testfixture.exe
            }
          }
        }
      }

      lappend G(test_array) [dict create config $config target $target]
    }
  }
}

proc count_tests_and_errors {name logfile} {
  global G

  set fd [open $logfile rb]
  set seen 0
  while {![eof $fd]} {
    set line [gets $fd]
    if {[regexp {(\d+) errors out of (\d+) tests} $line all nerr ntest]} {
      incr G(test.$name.nError) $nerr
      incr G(test.$name.nTest) $ntest
      set seen 1
      if {$nerr>0} {
        set G(test.$name.errmsg) $line
      }
    }
    if {[regexp {runtime error: +(.*)} $line all msg]} {
      # skip over "value is outside range" errors
      if {[regexp {value .* is outside the range of representable} $line]} {
         # noop
      } else {
        incr G(test.$name.nError)
        if {$G(test.$name.errmsg)==""} {
          set G(test.$name.errmsg) $msg
        }
      }
    }
    if {[regexp {fatal error +(.*)} $line all msg]} {
      incr G(test.$name.nError)
      if {$G(test.$name.errmsg)==""} {
        set G(test.$name.errmsg) $msg
      }
    }
    if {[regexp {ERROR SUMMARY: (\d+) errors.*} $line all cnt] && $cnt>0} {
      incr G(test.$name.nError)
      if {$G(test.$name.errmsg)==""} {
        set G(test.$name.errmsg) $all
      }
    }
    if {[regexp {^VERSION: 3\.\d+.\d+} $line]} {
      set v [string range $line 9 end]
      if {$G(sqlite_version) eq "unknown"} {
        set G(sqlite_version) $v
      } elseif {$G(sqlite_version) ne $v} {
        set G(test.$name.errmsg) "version conflict: {$G(sqlite_version)} vs. {$v}"
      }
    }
  }
  close $fd
  if {$G(test) == "Build-Only"} {
    incr G(test.$name.nTest)
    if {$G(test.$name.nError)>0} {
      set errmsg "Build failed"
    }
  } elseif {!$seen} {
    set G(test.$name.errmsg) "Test did not complete"
    if {[file readable core]} {
      append G(test.$name.errmsg) " - core file exists"
    }
  }
}

proc slave_fileevent {name} {
  global G
  set fd $G(test.$name.channel)

  if {[eof $fd]} {
    fconfigure $fd -blocking 1
    set rc [catch { close $fd }]
    unset G(test.$name.channel)
    set G(test.$name.done) [clock seconds]
    set G(test.$name.nError) 0
    set G(test.$name.nTest) 0
    set G(test.$name.errmsg) ""
    if {$rc} {
      incr G(test.$name.nError)
    }
    if {[file exists $G(test.$name.log)]} {
      count_tests_and_errors $name $G(test.$name.log)
    }
  } else {
    set line [gets $fd]
    if {[string trim $line] != ""} { puts "Trace   : $name - \"$line\"" }
  }

  do_some_stuff
}

proc do_some_stuff {} {
  global G

  # Count the number of running jobs. A running job has an entry named
  # "channel" in its dictionary.
  set nRunning 0
  set bFinished 1
  foreach j $G(test_array) {
    set name [dict get $j config]
    if { [info exists G(test.$name.channel)]} { incr nRunning   }
    if {![info exists G(test.$name.done)]}    { set bFinished 0 }
  }

  if {$bFinished} {
    set nError 0
    set nTest 0
    set nConfig 0
    foreach j $G(test_array) {
      set name [dict get $j config]
      incr nError $G(test.$name.nError)
      incr nTest $G(test.$name.nTest)
      incr nConfig 
    }
    set G(result) "$nError errors from $nTest tests in $nConfig configurations."
    catch {
      append G(result) " SQLite version $G(sqlite_version)"
    }
  } else {
    set nLaunch [expr $G(jobs) - $nRunning]
    foreach j $G(test_array) {
      if {$nLaunch<=0} break
      set name [dict get $j config]
      if { ![info exists G(test.$name.channel)]
        && ![info exists G(test.$name.done)]
      } {
        set target [dict get $j target]
        set G(test.$name.start) [clock seconds]
        set fd [open "|[info nameofexecutable] $G(releaseTest) --slave" r+]
        set G(test.$name.channel) $fd
        fconfigure $fd -blocking 0
        fileevent $fd readable [list slave_fileevent $name]

        puts $fd [list 0 $G(msvc) 0 $G(keep)]
        set L [make_test_suite $G(msvc) "" $name $target $::Configs($name)]
        puts $fd $L
        flush $fd
        set G(test.$name.log) [file join [lindex $L 1] test.log]
        incr nLaunch -1
      }
    }
  }
}

proc generate_main_page {{extra {}}} {
  global G
  set_test_array

  wapp-trim {
    <html>
    <head>
      <link rel="stylesheet" type="text/css" href="style.css"/>
    </head>
    <body>
  }

  # If the checkout contains uncommitted changs, put a warning at the top
  # of the page.
  if {[check_uncommitted]} {
    wapp-trim {
      <div class=warning>
        WARNING: Uncommitted changes in checkout.
      </div>
    }
  }

  wapp-trim {
      <div class=div id=controls> 
        <form action="control" method="post" name="control">
        <label> Platform: </label>
        <select id="control_platform" name="control_platform">
  }
  foreach platform [array names ::Platforms] {
    set selected ""
    if {$platform==$G(platform)} { set selected " selected=1" }
    wapp-subst "<option $selected>$platform</option>"
  }
  wapp-trim {
        </select>
        <label> Test: </label>
        <select id="control_test" name="control_test">
  }
  foreach test [list Normal Veryquick Smoketest Build-Only] {
    set selected ""
    if {$test==$G(test)} { set selected " selected=1" }
    wapp-subst "<option $selected>$test</option>"
  }
  wapp-trim [subst -nocommands {
        </select>
        <label> Tcl: </label>
        <input id="control_tcl" name="control_tcl"></input>

        <label> Keep files: </label>
        <input id="control_keep" name="control_keep" type=checkbox value=1>
        </input>
        <label> Use MSVC: </label>
        <input id="control_msvc" name="control_msvc" type=checkbox value=1>
        </input>
        <hr>
        <div class=right>
          <label> Jobs: </label>
          <select id="control_jobs" name="control_jobs">
  }]
  for {set i 1} {$i <= 8} {incr i} {
    if {$G(jobs)==$i} {
      wapp-trim {
        <option selected=1>%string($i)</option>
      }
    } else {
      wapp-trim {
        <option>%string($i)</option>
      }
    }
  }
  wapp-trim {
          </select>
          <input id=control_go name=control_go type=submit value="Run Tests!">
          </input>
        </div>
     </form>
      </div>
      <div class=div id=tests>    
      <table>
  }
  foreach t $G(test_array) {
    set config [dict get $t config]
    set target [dict get $t target]

    set class "testwait"
    set seconds ""

    if {[info exists G(test.$config.log)]} {
      if {[info exists G(test.$config.channel)]} {
        set class "testrunning"
        set seconds [expr [clock seconds] - $G(test.$config.start)]
      } elseif {[info exists G(test.$config.done)]} {
        if {$G(test.$config.nError)>0} {
          set class "testfail" 
        } else {
          set class "testdone"
        }
        set seconds [expr $G(test.$config.done) - $G(test.$config.start)]
      }

      set min [format %.2d [expr ($seconds / 60) % 60]]
      set  hr [format %.2d [expr $seconds / 3600]]
      set sec [format %.2d [expr $seconds % 60]]
      set seconds "$hr:$min:$sec"
    }

    wapp-trim {
      <tr class=%string($class)>
      <td class=testfield> %html($config) 
      <td class=testfield> %html($target)
      <td class=testfield> %html($seconds)
      <td class=testfield>
    }
    if {[info exists G(test.$config.log)]} {
      set log $G(test.$config.log)
      set uri "log/$log"
      wapp-trim {
        <a href=%url($uri)> %html($log) </a>
      }
    }
    if {[info exists G(test.$config.errmsg)] && $G(test.$config.errmsg)!=""} {
      set errmsg $G(test.$config.errmsg)
      wapp-trim {
        <tr class=testfail>
        <td class=testfield>
        <td class=testfield colspan=3> %html($errmsg)
      }
    }
  }

  wapp-trim {
      </table>
      </div>
  }
  if {[info exists G(result)]} {
    set res $G(result)
    wapp-trim {
      <div class=div id=log> %string($res) </div>
    }
  }
  wapp-trim {
    <script src="script.js"></script>
    </body>
    </html>
  }
  incr G(cnt)
}

proc wapp-default {} {
  generate_main_page
}

proc wapp-page-control {} {
  global G
  foreach v {platform test tcl jobs keep msvc} {
    if {[wapp-param-exists control_$v]} {
      set G($v) [wapp-param control_$v]
    } else {
      set G($v) 0
    }
  }

  if {[wapp-param-exists control_go]} {
    # This is an actual "run test" command, not just a change of 
    # configuration!
    set_test_array
    set ::G(state) "running"
  }

  if {$::G(state) == "running"} {
    do_some_stuff
  }

  wapp-redirect /
}

proc wapp-page-style.css {} {
  wapp-subst {
    .div {
      border: 3px groove #444444;
      margin: 1em;
      padding: 1em;
    }

    .warning {
      text-align:center;
      color: red;
      font-size: 2em;
      font-weight: bold;
    }

    .right {
    }

    .testfield {
      padding-right: 10ex;
    }

    .testwait {}
    .testrunning { color: blue }
    .testdone { color: green }
    .testfail { color: red }
  }
}

proc wapp-page-script.js {} {

  set tcl $::G(tcl)
  set keep $::G(keep)
  set msvc $::G(msvc)
  
  wapp-subst {
    var lElem = \["control_platform", "control_test", "control_msvc", "control_jobs"\];
    lElem.forEach(function(e) {
      var elem = document.getElementById(e);
      elem.addEventListener("change", function() { control.submit() } );
    })

    elem = document.getElementById("control_tcl");
    elem.value = "%string($tcl)"

    elem = document.getElementById("control_keep");
    elem.checked = %string($keep);

    elem = document.getElementById("control_msvc");
    elem.checked = %string($msvc);
  }

  if {$::G(state)!="config"} {
    wapp-subst {
      var lElem = \["control_platform", "control_test", 
          "control_tcl", "control_keep", "control_msvc", "control_go"
      \];
      lElem.forEach(function(e) {
        var elem = document.getElementById(e);
        elem.disabled = true;
      })
    }
  }
}

proc wapp-page-env {} {
  wapp-allow-xorigin-params
  wapp-trim {
    <h1>Wapp Environment</h1>\n<pre>
    <pre>%html([wapp-debug-env])</pre>
  }
}

proc wapp-page-log {} {
  set log [string range [wapp-param REQUEST_URI] 5 end]
  set fd [open $log]
  set data [read $fd]
  close $fd
  wapp-trim {
    <pre>
    %html($data)
    </pre>
  }
}

wapp-start $argv


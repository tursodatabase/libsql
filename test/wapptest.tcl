#!/bin/sh 
# \
exec wapptclsh "$0" ${1+"$@"}

# package required wapp
source [file join [file dirname [info script]] wapp.tcl]

# Read the data from the releasetest_data.tcl script.
#
source [file join [file dirname [info script]] releasetest_data.tcl]

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

proc wapptest_init {} {
  global G

  set lSave [list platform test keep msvc tcl jobs] 
  foreach k $lSave { set A($k) $G($k) }
  array unset G
  foreach k $lSave { set G($k) $A($k) }

  # The root of the SQLite source tree.
  set G(srcdir)   [file dirname [file dirname [info script]]]

  # releasetest.tcl script
  set G(releaseTest) [file join [file dirname [info script]] releasetest.tcl]

  set G(sqlite_version) "unknown"

  # Either "config", "running" or "stopped":
  set G(state) "config"
}

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

proc slave_test_done {name rc} {
  global G
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
}

proc slave_fileevent {name} {
  global G
  set fd $G(test.$name.channel)

  if {[eof $fd]} {
    fconfigure $fd -blocking 1
    set rc [catch { close $fd }]
    unset G(test.$name.channel)
    slave_test_done $name $rc
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
    set G(state) "stopped"
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

proc generate_select_widget {label id lOpt opt} {
  wapp-trim {
    <label> %string($label) </label>
    <select id=%string($id) name=%string($id)>
  }
  foreach o $lOpt {
    set selected ""
    if {$o==$opt} { set selected " selected=1" }
    wapp-subst "<option $selected>$o</option>"
  }
  wapp-trim { </select> }
}

proc generate_main_page {{extra {}}} {
  global G
  set_test_array

  # <meta http-equiv="refresh" content="5; URL=/">
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
  }

  # Build the "platform" select widget. 
  set lOpt [array names ::Platforms]
  generate_select_widget Platform control_platform $lOpt $G(platform)

  # Build the "test" select widget. 
  set lOpt [list Normal Veryquick Smoketest Build-Only] 
  generate_select_widget Test control_test $lOpt $G(test)

  # Build the "jobs" select widget. Options are 1 to 8.
  generate_select_widget Jobs control_jobs {1 2 3 4 5 6 7 8} $G(jobs)

  switch $G(state) {
    config {
      set txt "Run Tests!"
      set id control_run
    }
    running {
      set txt "STOP Tests!"
      set id control_stop
    }
    stopped {
      set txt "Reset!"
      set id control_reset
    }
  }
  wapp-trim {
    <div class=right>
    <input id=%string($id) name=%string($id) type=submit value="%string($txt)">
    </input>
    </div>
  }

  wapp-trim {
  <br><br>
        <label> Tcl: </label>
        <input id="control_tcl" name="control_tcl"></input>
        <label> Keep files: </label>
        <input id="control_keep" name="control_keep" type=checkbox value=1>
        </input>
        <label> Use MSVC: </label>
        <input id="control_msvc" name="control_msvc" type=checkbox value=1>
        </input>
  }
  wapp-trim {
     </form>
  }
  wapp-trim {
     </div>
     <div class=div2 id=tests>
  }
  wapp-page-tests

  set script "script/$G(state).js"
  wapp-trim {
    </div>
      <script src=%string($script)></script>
    </body>
    </html>
  }
}

proc wapp-default {} {
  generate_main_page
}

proc wapp-page-tests {} {
  global G
  wapp-trim { <table> }
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

  wapp-trim { </table> }

  if {[info exists G(result)]} {
    set res $G(result)
    wapp-trim {
      <div class=border id=result> %string($res) </div>
    }
  }
}

# URI: /control
#
# Whenever the form at the top of the application page is submitted, it
# is submitted here.
#
proc wapp-page-control {} {
  global G
  catch { puts [wapp-param control_msvc] }
  if {$::G(state)=="config"} {
    set lControls [list platform test tcl jobs keep msvc]
    set G(msvc) 0
    set G(keep) 0
  } else {
    set lControls [list jobs]
  }
  foreach v $lControls {
    if {[wapp-param-exists control_$v]} {
      set G($v) [wapp-param control_$v]
    }
  }

  if {[wapp-param-exists control_run]} {
    # This is a "run test" command.
    set_test_array
    set ::G(state) "running"
  }

  if {[wapp-param-exists control_stop]} {
    # A "STOP tests" command.
    set G(state) "stopped"
    set G(result) "Test halted by user"
    foreach j $G(test_array) {
      set name [dict get $j config]
      if { [info exists G(test.$name.channel)] } {
        close $G(test.$name.channel)
        unset G(test.$name.channel)
        slave_test_done $name 1
      }
    }
  }

  if {[wapp-param-exists control_reset]} {
    # A "reset app" command.
    set G(state) "config"
    wapptest_init
  }

  if {$::G(state) == "running"} {
    do_some_stuff
  }
  wapp-redirect /
}

# URI: /style.css
#
# Return the stylesheet for the application main page.
#
proc wapp-page-style.css {} {
  wapp-subst {
    .div {
      border: 3px groove #444444;
      margin: 1em;
      padding: 1em;
    }

    .border {
      border: 3px groove #444444;
      padding: 1em;
      margin-top: 1em;
      margin-bottom: 1em;
    }

    .div2 {
      margin: 1em;
    }

    table {
      padding: 1em;
      width:100%;
      border: 3px groove #444444;
    }

    .warning {
      text-align:center;
      color: red;
      font-size: 2em;
      font-weight: bold;
    }

    .testfield {
      padding-right: 10ex;
      white-space: nowrap;
    }

    .testwait {}
    .testrunning { color: blue }
    .testdone { color: green }
    .testfail { color: red }

    .right { float: right; }

  }
}

# URI: /script/${state}.js
#
# The last part of this URI is always "config.js", "running.js" or 
# "stopped.js", depending on the state of the application. It returns
# the javascript part of the front-end for the requested state to the
# browser.
#
proc wapp-page-script {} {
  regexp {[^/]*$} [wapp-param REQUEST_URI] script

  set tcl $::G(tcl)
  set keep $::G(keep)
  set msvc $::G(msvc)
  
  wapp-subst {
    var lElem = \["control_platform", "control_test", "control_msvc", 
        "control_jobs"
    \];
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

  if {$script != "config.js"} {
    wapp-subst {
      var lElem = \["control_platform", "control_test", 
          "control_tcl", "control_keep", "control_msvc"
      \];
      lElem.forEach(function(e) {
        var elem = document.getElementById(e);
        elem.disabled = true;
      })
    }
  }

  if {$script == "running.js"} {
    wapp-subst {
      function reload_tests() {
        fetch('tests')
          .then( data => data.text() )
          .then( data => {
            document.getElementById("tests").innerHTML = data;
          })
          .then( data => {
            if( document.getElementById("result") ){
              document.location = document.location;
            } else {
              setTimeout(reload_tests, 1000)
            }
          });
      }

      setTimeout(reload_tests, 1000)
    }
  }
}

# URI: /env
#
# This is for debugging only. Serves no other purpose.
#
proc wapp-page-env {} {
  wapp-allow-xorigin-params
  wapp-trim {
    <h1>Wapp Environment</h1>\n<pre>
    <pre>%html([wapp-debug-env])</pre>
  }
}

# URI: /log/dirname/test.log
#
# This URI reads file "dirname/test.log" from disk, wraps it in a <pre>
# block, and returns it to the browser. Use for viewing log files.
#
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

wapptest_init
wapp-start $argv


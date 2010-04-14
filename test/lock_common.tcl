# 2010 April 14
#
# The author disclaims copyright to this source code.  In place of
# a legal notice, here is a blessing:
#
#    May you do good and not evil.
#    May you find forgiveness for yourself and forgive others.
#    May you share freely, never taking more than you give.
#
#***********************************************************************
# This file contains code used by several different test scripts. The
# code in this file allows testfixture to control another process (or
# processes) to test locking.
#

# Launch another testfixture process to be controlled by this one. A
# channel name is returned that may be passed as the first argument to proc
# 'testfixture' to execute a command. The child testfixture process is shut
# down by closing the channel.
proc launch_testfixture {} {
  set prg [info nameofexec]
  if {$prg eq ""} {
    set prg [file join . testfixture]
  }
  set chan [open "|$prg tf_main.tcl" r+]
  fconfigure $chan -buffering line
  return $chan
}

# Execute a command in a child testfixture process, connected by two-way
# channel $chan. Return the result of the command, or an error message.
proc testfixture {chan cmd} {
  puts $chan $cmd
  puts $chan OVER
  set r ""
  while { 1 } {
    set line [gets $chan]
    if { $line == "OVER" } { 
      return $r
    }
    if {[eof $chan]} {
      return "ERROR: Child process hung up"
    }
    append r $line
  }
}

# Write the main loop for the child testfixture processes into file
# tf_main.tcl. The parent (this script) interacts with the child processes
# via a two way pipe. The parent writes a script to the stdin of the child
# process, followed by the word "OVER" on a line of its own. The child
# process evaluates the script and writes the results to stdout, followed
# by an "OVER" of its own.
set f [open tf_main.tcl w]
puts $f {
  set l [open log w]
  set script ""
  while {![eof stdin]} {
    flush stdout
    set line [gets stdin]
    puts $l "READ $line"
    if { $line == "OVER" } {
      catch {eval $script} result
      puts $result
      puts $l "WRITE $result"
      puts OVER
      puts $l "WRITE OVER"
      flush stdout
      set script ""
    } else {
      append script $line
      append script " ; "
    }
  }
  close $l
}
close $f

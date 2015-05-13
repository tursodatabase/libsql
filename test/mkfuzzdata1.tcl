#!/usr/bin/tclsh
#
# Run this script in order to rebuild the fuzzdata1.txt file containing
# fuzzer data for the fuzzershell utility that is create by afl-fuzz.
#
# This script gathers all of the test cases identified by afl-fuzz and
# runs afl-cmin and afl-tmin over them all to try to generate a mimimum
# set of tests that cover all observed behavior.
# 
# Options:
#
#    --afl-bin DIR1             DIR1 contains the AFL binaries
#    --fuzzershell PATH         Full pathname of instrumented fuzzershell
#    --afl-data DIR3            DIR3 is the "-o" directory from afl-fuzz
#    -o FILE                    Write results into FILE
#
set AFLBIN {}
set FUZZERSHELL {}
set AFLDATA {}
set OUTFILE {}

proc usage {} {
  puts stderr "Usage: $::argv0 --afl-bin DIR --fuzzershell PATH\
                  --afl-data DIR -o FILE"
  exit 1
}
proc cmdlineerr {msg} {
  puts stderr $msg
  usage
}

for {set i 0} {$i<[llength $argv]} {incr i} {
  set x [lindex $argv $i]
  if {[string index $x 0]!="-"} {cmdlineerr "illegal argument: $x"}
  set x [string trimleft $x -]
  incr i
  if {$i>=[llength $argv]} {cmdlineerr "no argument on --$x"}
  set a [lindex $argv $i]
  switch -- $x {
     afl-bin {set AFLBIN $a}
     afl-data {set AFLDATA $a}
     fuzzershell {set FUZZERSHELL $a}
     o {set OUTFILE $a}
     default {cmdlineerr "unknown option: --$x"}
  }
}
proc checkarg {varname option} {
  set val [set ::$varname]
  if {$val==""} {cmdlineerr "required option missing: --$option"}
}
checkarg AFLBIN afl-bin
checkarg AFLDATA afl-data
checkarg FUZZERSHELL fuzzershell
checkarg OUTFILE o
proc checkexec {x} {
  if {![file exec $x]} {cmdlineerr "cannot find $x"}
}
checkexec $AFLBIN/afl-cmin
checkexec $AFLBIN/afl-tmin
checkexec $FUZZERSHELL
proc checkdir {x} {
  if {![file isdir $x]} {cmdlineerr "no such directory: $x"}
}
checkdir $AFLDATA/queue

proc progress {msg} {
  puts "******** $msg"
  flush stdout
}
progress "mkdir tmp1 tmp2"
file mkdir tmp1 tmp2
progress "copying test cases from $AFLDATA into tmp1..."
set n 0
foreach file [glob -nocomplain $AFLDATA/queue/id:*] {
  incr n
  file copy $file tmp1/$n
}
foreach file [glob -nocomplain $AFLDATA/crash*/id:*] {
  incr n
  file copy $file tmp1/$n
}
progress "total $n files copied."
progress "running: $AFLBIN/afl-cmin -i tmp1 -o tmp2 $FUZZERSHELL"
exec $AFLBIN/afl-cmin -i tmp1 -o tmp2 $FUZZERSHELL >&@ stdout
progress "afl-cmin complete."
#
# Experiments show that running afl-tmin is too slow for this application.
# And it doesn't really make the test cases that much smaller.  So let's
# just skip it.
#
# foreach file [glob tmp2/*] {
#   progress "$AFLBIN/afl-tmin -i $file -o tmp3/[file tail $file] $FUZZERSHELL"
#   exec $AFLBIN/afl-tmin -i $file -o tmp3/[file tail $file] \
#       $FUZZERSHELL >&@ stdout
# }
progress "generating final output into $OUTFILE"
set out [open $OUTFILE wb]
puts $out "# Test data for use with fuzzershell.  Automatically
# generated using $argv0.  This file contains binary data
#"
set n 0
foreach file [glob tmp2/*] {
  incr n
  puts -nonewline $out "/****<$n>****/"
  set in [open $file rb]
  puts -nonewline $out [read $in]
  close $in
}
close $out
progress "done.  $n test cases written to $OUTFILE"
progress "clean-up..."
file delete -force tmp1
progress "culled test cases left in the tmp2 directory"

#!/usr/bin/tclsh
#
# Run this script from the top of the source tree in order to confirm that
# various aspects of the source tree are up-to-date.  Items checked include:
#
#     *    Makefile.msc and autoconf/Makefile.msc agree
#     *    VERSION agrees with autoconf/tea/configure.ac
#
# Other tests might be added later.  
#
# Error messages are printed and the process exists non-zero if problems
# are found.  If everything is ok, no output is generated and the process
# exits with 0.
#

# Read an entire file.
#
proc readfile {filename} {
  set fd [open $filename rb]
  set txt [read $fd]
  close $fd
  return $txt
}

# Find the root of the tree.
#
set ROOT [file dir [file dir [file normalize $argv0]]]

# Name of the TCL interpreter
#
set TCLSH [info nameofexe]

# Number of errors seen.
#
set NERR 0

######################### autoconf/Makefile.msc ###############################

set f1 [readfile $ROOT/autoconf/Makefile.msc]
exec $TCLSH $ROOT/tool/mkmsvcmin.tcl $ROOT/Makefile.msc tmp1.txt
set f2 [readfile tmp1.txt]
file delete tmp1.txt
if {$f1 != $f2} {
  puts "ERROR: ./autoconf/Makefile.msc does not agree with ./Makefile.msc"
  puts "...... Fix: tclsh tool/mkmsvcmin.tcl"
  incr NERR
}

#!/usr/bin/tclsh
#
# Run this script from the top of the source tree in order to confirm that
# various aspects of the source tree are up-to-date.  Items checked include:
#
#     *    Makefile.msc and autoconf/Makefile.msc agree
#     *    src/ctime.tcl is consistent with tool/mkctimec.tcl
#     *    VERSION agrees with autoconf/tea/configure.ac
#     *    src/pragma.h agrees with tool/mkpragmatab.tcl
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

######################### configure ###########################################

set conf [readfile $ROOT/configure]
set vers [readfile $ROOT/VERSION]
if {[string first $vers $conf]<=0} {
  puts "ERROR: ./configure does not agree with ./VERSION"
  puts "...... Fix: run autoconf"
  incr NERR
}
unset conf

######################### autoconf/tea/configure.ac ###########################

set confac [readfile $ROOT/autoconf/tea/configure.ac]
set vers [readfile $ROOT/VERSION]
set pattern {AC_INIT([sqlite],[}
append pattern [string trim $vers]
append pattern {])}
if {[string first $pattern $confac]<=0} {
  puts "ERROR: ./autoconf/tea/configure.ac does not agree with ./VERSION"
  puts "...... Fix: manually edit ./autoconf/tea/configure.ac and put the"
  puts "......      correct version number in AC_INIT()"
  incr NERR
}
unset confac

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

######################### src/pragma.h ########################################

set f1 [readfile $ROOT/src/pragma.h]
exec $TCLSH $ROOT/tool/mkpragmatab.tcl tmp2.txt
set f2 [readfile tmp2.txt]
file delete tmp2.txt
if {$f1 != $f2} {
  puts "ERROR: ./src/pragma.h does not agree with ./tool/mkpragmatab.tcl"
  puts "...... Fix: tclsh tool/mkpragmatab.tcl"
  incr NERR
}

######################### src/ctime.c ########################################

set f1 [readfile $ROOT/src/ctime.c]
exec $TCLSH $ROOT/tool/mkctimec.tcl tmp3.txt
set f2 [readfile tmp3.txt]
file delete tmp3.txt
if {$f1 != $f2} {
  puts "ERROR: ./src/ctime.c does not agree with ./tool/mkctimec.tcl"
  puts ".....  Fix: tclsh tool/mkctimec.tcl"
  incr NERR
}

# If any errors are seen, exit 1 so that the build will fail.
#
if {$NERR>0} {exit 1}

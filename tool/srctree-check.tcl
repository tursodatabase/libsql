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
cd $ROOT

# Name of the TCL interpreter
#
set TCLSH [info nameofexe]

######################### autoconf/tea/configure.ac ###########################

set confac [readfile $ROOT/autoconf/tea/configure.ac]
set vers [readfile $ROOT/VERSION]
set pattern {AC_INIT([sqlite],[}
append pattern [string trim $vers]
append pattern {])}
if {[string first $pattern $confac]<=0} {
  puts "ERROR: ./autoconf/tea/configure.ac does not agree with ./VERSION"
  exit 1
}

######################### autoconf/Makefile.msc ###############################

set f1 [readfile $ROOT/autoconf/Makefile.msc]
exec mv $ROOT/autoconf/Makefile.msc $ROOT/autoconf/Makefile.msc.tmp
exec $TCLSH $ROOT/tool/mkmsvcmin.tcl
set f2 [readfile $ROOT/autoconf/Makefile.msc]
exec mv $ROOT/autoconf/Makefile.msc.tmp $ROOT/autoconf/Makefile.msc
if {$f1 != $f2} {
  puts "ERROR: ./autoconf/Makefile.msc does not agree with ./Makefile.msc"
}

######################### src/pragma.h ########################################

set f1 [readfile $ROOT/src/pragma.h]
exec mv $ROOT/src/pragma.h $ROOT/src/pragma.h.tmp
exec $TCLSH $ROOT/tool/mkpragmatab.tcl
set f2 [readfile $ROOT/src/pragma.h]
exec mv $ROOT/src/pragma.h.tmp $ROOT/src/pragma.h
if {$f1 != $f2} {
  puts "ERROR: ./src/pragma.h does not agree with ./tool/mkpragmatab.tcl"
}

######################### src/ctime.c ########################################

set f1 [readfile $ROOT/src/ctime.c]
exec mv $ROOT/src/ctime.c $ROOT/src/ctime.c.tmp
exec $TCLSH $ROOT/tool/mkctimec.tcl
set f2 [readfile $ROOT/src/ctime.c]
exec mv $ROOT/src/ctime.c.tmp $ROOT/src/ctime.c
if {$f1 != $f2} {
  puts "ERROR: ./src/ctime.c does not agree with ./tool/mkctimec.tcl"
}

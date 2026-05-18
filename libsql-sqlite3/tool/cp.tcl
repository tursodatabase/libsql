#/usr/bin/tclsh
#
# This is a TCL script that copies multiple files into a common directory.
# The "cp" command will do this on unix, but no such command is available
# by default on Windows, so we have to use this script.
#
#   tclsh cp.tcl FILE1 FILE2 ... FILEN DIR
#

# This should be as simple as
#
#     file copy -force -- {*}$argv
#
# But jimtcl doesn't support that.  So we have to do it the hard way.

if {[llength $argv]<2} {
  error "Usage: $argv0 SRC... DESTDIR"
}
set n [llength $argv]
set destdir [lindex $argv [expr {$n-1}]]
if {![file isdir $destdir]} {
  error "$argv0: not a directory: \"$destdir\""
}
for {set i 0} {$i<$n-1} {incr i} {
  set fn [file normalize [lindex $argv $i]]
  set tail [file tail $fn]
  file copy -force $fn [file normalize $destdir/$tail]
}

#!/usr/bin/tcl
#
# Convert input text into a C string
#
set in [open [lindex $argv 0] rb]
while {![eof $in]} {
  set line [gets $in]
  if {[eof $in]} break;
  set x [string map "\\\\ \\\\\\\\ \\\" \\\\\"" $line]
  puts "\"$x\\n\""
}
close $in

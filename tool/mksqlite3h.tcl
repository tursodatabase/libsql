#!/usr/bin/tclsh
#
# This script performs processing on src/sqlite.h.in. It:
#
#   1) Adds SQLITE_EXTERN in front of the declaration of global variables,
#   2) Adds SQLITE_API in front of the declaration of API functions,
#   3) Replaces the string --VERS-- with the current library version, 
#      formatted as a string (e.g. "3.6.17"), and
#   4) Replaces the string --VERSION-NUMBER-- with current library version,
#      formatted as an integer (e.g. "3006017").
#
# This script reads from stdin, and outputs to stdout. The current library
# version number should be passed as the only argument. Example invocation:
#
#   cat sqlite.h.in | mksqlite3h.tcl 3.6.17 > sqlite3.h
#

set zVersion [lindex $argv 0]
set nVersion [eval format "%d%03d%03d" [split $zVersion .]]

while {![eof stdin]} {
  set varpattern {^[a-zA-Z][a-zA-Z_0-9 *]+sqlite3_[_a-zA-Z0-9]+(\[|;| =)}
  set declpattern {^ *[a-zA-Z][a-zA-Z_0-9 ]+ \**sqlite3_[_a-zA-Z0-9]+\(}

  set line [gets stdin]

  regsub -- --VERS--           $line $zVersion line
  regsub -- --VERSION-NUMBER-- $line $nVersion line

  if {[regexp {define SQLITE_EXTERN extern} $line]} {
    puts $line
    puts [gets stdin]
    puts ""
    puts "#ifndef SQLITE_API"
    puts "# define SQLITE_API"
    puts "#endif"
    set line ""
  }

  if {([regexp $varpattern $line] && ![regexp {^ *typedef} $line])
   || ([regexp $declpattern $line])
  } {
    set line "SQLITE_API $line"
  }
  puts $line
}


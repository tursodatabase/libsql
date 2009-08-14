#!/usr/bin/tclsh
#
# This script constructs the "sqlite3.h" header file from the following
# sources:
#
#   1) The src/sqlite.h.in source file.  This is the template for sqlite3.h.
#   2) The VERSION file containing the current SQLite version number.
#   3) The manifest file from the fossil SCM.  This gives use the date.
#   4) The manifest.uuid file from the fossil SCM.  This gives the SHA1 hash.
#
# Run this script by specifying the root directory of the source tree
# on the command-line.
# 
# This script performs processing on src/sqlite.h.in. It:
#
#   1) Adds SQLITE_EXTERN in front of the declaration of global variables,
#   2) Adds SQLITE_API in front of the declaration of API functions,
#   3) Replaces the string --VERS-- with the current library version, 
#      formatted as a string (e.g. "3.6.17"), and
#   4) Replaces the string --VERSION-NUMBER-- with current library version,
#      formatted as an integer (e.g. "3006017").
#   5) Replaces the string --SOURCE-ID-- with the date and time and sha1 
#      hash of the fossil-scm manifest for the source tree.
#
# This script outputs to stdout.
#
# Example usage:
#
#   tclsh mksqlite3h.tcl ../sqlite >sqlite3.h
#


# Get the source tree root directory from the command-line
#
set TOP [lindex $argv 0]

# Get the SQLite version number (ex: 3.6.18) from the $TOP/VERSION file.
#
set in [open $TOP/VERSION]
set zVersion [string trim [read $in]]
close $in
set nVersion [eval format "%d%03d%03d" [split $zVersion .]]

# Get the fossil-scm version number from $TOP/manifest.uuid.
#
set in [open $TOP/manifest.uuid]
set zUuid [string trim [read $in]]
close $in

# Get the fossil-scm check-in date from the "D" card of $TOP/manifest.
#
set in [open $TOP/manifest]
set zDate {}
while {![eof $in]} {
  set line [gets $in]
  if {[regexp {^D (2.*[0-9])} $line all date]} {
    set zDate [string map {T { }} $date]
    break
  }
}
close $in

# Set up patterns for recognizing API declarations.
#
set varpattern {^[a-zA-Z][a-zA-Z_0-9 *]+sqlite3_[_a-zA-Z0-9]+(\[|;| =)}
set declpattern {^ *[a-zA-Z][a-zA-Z_0-9 ]+ \**sqlite3_[_a-zA-Z0-9]+\(}

# Process the  src/sqlite.h.in  file.
#
set in [open $TOP/src/sqlite.h.in]
while {![eof $in]} {

  set line [gets $in]

  regsub -- --VERS--           $line $zVersion line
  regsub -- --VERSION-NUMBER-- $line $nVersion line
  regsub -- --SOURCE-ID--      $line "$zDate $zUuid" line

  if {[regexp {define SQLITE_EXTERN extern} $line]} {
    puts $line
    puts [gets $in]
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
close $in

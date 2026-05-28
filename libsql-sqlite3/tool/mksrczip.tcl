#!/usr/bin/tclsh
#
# Build a ZIP archive for the complete, unedited source code that
# corresponds to the current check-out.
#
set VERSION-file [file dirname [file dirname [file normalize $argv0]]]/VERSION
set fd [open ${VERSION-file} rb]
set vers [read $fd]
close $fd
scan $vers %d.%d.%d major minor patch
set numvers [format {3%02d%02d00} $minor $patch]
set cmd "fossil zip current sqlite-src-$numvers.zip --name sqlite-src-$numvers"
puts $cmd
exec {*}$cmd

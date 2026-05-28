#!/usr/bin/tclsh
#
# Build a ZIP archive for the amalgamation source code found in the current
# directory.
#
set VERSION-file [file dirname [file dirname [file normalize $argv0]]]/VERSION
set fd [open ${VERSION-file} rb]
set vers [read $fd]
close $fd
scan $vers %d.%d.%d major minor patch
set numvers [format {3%02d%02d00} $minor $patch]
set dir sqlite-amalgamation-$numvers
file delete -force $dir
file mkdir $dir
set filelist {sqlite3.c sqlite3.h shell.c sqlite3ext.h}
foreach f $filelist {
  file copy $f $dir/$f
}
set cmd "zip -r $dir.zip $dir"
puts $cmd
file delete -force $dir.zip
exec {*}$cmd
file delete -force $dir

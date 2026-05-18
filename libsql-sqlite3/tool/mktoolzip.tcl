#!/usr/bin/tclsh
#
# Run this script in order to generate a ZIP archive containing various
# command-line tools.
#
# The makefile that invokes this script must first build the following
# binaries:
#
#     testfixture             -- used to run this script
#     sqlite3                 -- the SQLite CLI
#     sqldiff                 -- Program to diff two databases
#     sqlite3_analyzer        -- Space analyzer
#     sqlite3_rsync           -- Remote db sync
#
# On Windows, add:
#
#     sqlite3.def
#     sqlite3.dll
#
# Add the --snapshot option to generate a snapshot ZIP archive instead of
# a release ZIP archive.
#
set bSnapshot 0
for {set i 0} {$i<[llength $argv]} {incr i} {
  set a [lindex $argv $i]
  if {$a eq "-snapshot" || $a eq "--snapshot"} {
    set bSnapshot 1
    continue
  }
  puts stderr "unknown argument: $a"
  exit 1
} 
switch $tcl_platform(os) {
  {Windows NT} {
    set OS win
    set EXE .exe
  }
  Linux {
    set OS linux
    set EXE {}
  }
  Darwin {
    set OS osx
    set EXE {}
  }
  default {
    set OS unknown
    set EXE {}
  }
}
switch $tcl_platform(machine) {
  arm64 {
    set ARCH arm64
  }
  x86_64 {
    set ARCH x64
  }
  amd64 -
  intel {
    if {$tcl_platform(pointerSize)==4} {
      set ARCH x86
    } else {
      set ARCH x64
    }
  }
  default {
    set ARCH unk
  }
}
if {$bSnapshot} {
  set in [open [file join [file dirname [file dirname [info script]]] manifest]]
  set manifest [read $in]
  close $in
  regexp {\nD (.{16})} $manifest all date
  regsub -all {[-:T]} $date {} v2
} else {
  set in [open [file join [file dirname [file dirname [info script]]] VERSION]]
  set vers [read $in]
  close $in
  scan $vers %d.%d.%d v1 v2 v3
  set v2 [format 3%02d%02d00 $v2 $v3]
}

set name sqlite-tools-$OS-$ARCH-$v2.zip
set filelist "sqlite3$EXE sqldiff$EXE sqlite3_analyzer$EXE sqlite3_rsync$EXE"
proc make_zip_archive {name filelist} {
  file delete -force $name
  puts "fossil test-filezip $name $filelist"
  if {[catch {exec fossil test-filezip $name {*}$filelist}]} {
    puts "^--- Unable.  Trying again as:"
    puts "zip $name $filelist"
    file delete -force $name
    exec zip $name {*}$filelist
  }
  puts "$name: [file size $name] bytes"
}
make_zip_archive $name $filelist

# On Windows, also try to construct a DLL
#
if {$OS eq "win" && [file exists sqlite3.dll] && [file exists sqlite3.def]} {
  set name sqlite-dll-win-$ARCH-$v2.zip
  set filelist [list sqlite3.def sqlite3.dll]
  make_zip_archive $name $filelist
}

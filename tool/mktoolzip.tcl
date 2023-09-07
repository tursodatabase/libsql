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
#
switch $tcl_platform(os) {
  {Windows NT} {
    set OS win32
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
set in [open VERSION]
set vers [read $in]
close $in
scan $vers %d.%d.%d v1 v2 v3
set v2 [format 3%02d%02d00 $v2 $v3]
set name sqlite-tools-$OS-$ARCH-$v2.zip
puts "zip $name sqlite3$EXE sqldiff$EXE sqlite3_analyzer$EXE"
puts [exec zip $name sqlite3$EXE sqldiff$EXE sqlite3_analyzer$EXE]
puts [exec ls -l $name]

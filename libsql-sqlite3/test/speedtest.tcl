#!/bin/sh
# the next line restarts using tclsh \
exec tclsh "$0" ${1+"$@"}
#
# This program runs performance testing on sqlite3.c.  Usage:
set usage {USAGE:

  speedtest.tcl  sqlite3.c  x1.txt  trunk.txt  -Os -DSQLITE_ENABLE_STAT4
                     |         |        |       `-----------------------'
    File to test ----'         |        |                |
                               |        |                `- options
       Output filename --------'        |
                                        `--- optional prior output to diff

Do a cache-grind performance analysis of the sqlite3.c file named and
write the results into the output file.  The ".txt" is appended to the
output file (and diff-file) name if it is not already present.  If the
diff-file is specified then show a diff from the diff-file to the new
output.

Other options include:
   CC=...                Specify an alternative C compiler.  Default is "gcc".
   -D...                 -D and -O options are passed through to the C compiler.
   --dryrun              Show what would happen but don't do anything.
   --help                Show this help screen.
   --lean                "Lean" mode.
   --lookaside N SZ      Lookahead uses N slots of SZ bytes each.
   --osmalloc            Use the OS native malloc() instead of MEMSYS5
   --pagesize N          Use N as the page size.
   --quiet | -q          "Quite".  Put results in file but don't pop up editor
   --size N              Change the test size.  100 means 100%.  Default: 5.
   --testset TEST        Specify the specific testset to use.  The default
                         is "mix1".  Other options include: "main", "json",
                         "cte", "orm", "fp", "rtree".
}
set srcfile {}
set outfile {}
set difffile {}
set cflags {-DSQLITE_OMIT_LOAD_EXTENSION -DSQLITE_THREADSAFE=0}
set cc gcc
set testset mix1
set dryrun 0
set quiet 0
set osmalloc 0
set speedtestflags {--shrink-memory --reprepare --stats}
lappend speedtestflags --journal wal --size 5

for {set i 0} {$i<[llength $argv]} {incr i} {
  set arg [lindex $argv $i]
  if {[string index $arg 0]=="-"} {
    switch -- $arg {
      -pagesize -
      --pagesize {
        lappend speedtestflags --pagesize
        incr i
        lappend speedtestflags [lindex $argv $i]
      }
      -lookaside -
      --lookaside {
        lappend speedtestflags --lookaside
        incr i
        lappend speedtestflags [lindex $argv $i]
        incr i
        lappend speedtestflags [lindex $argv $i]
      }
      -lean -
      --lean {
        lappend cflags \
           -DSQLITE_DEFAULT_MEMSTATUS=0 \
           -DSQLITE_DEFAULT_WAL_SYNCHRONOUS=1 \
           -DSQLITE_LIKE_DOESNT_MATCH_BLOBS=1 \
           -DSQLITE_MAX_EXPR_DEPTH=1 \
           -DSQLITE_OMIT_DECLTYPE \
           -DSQLITE_OMIT_DEPRECATED \
           -DSQLITE_OMIT_PROGRESS_CALLBACK \
           -DSQLITE_OMIT_SHARED_CACHE \
           -DSQLITE_USE_ALLOCA
      }
      -testset -
      --testset {
        incr i
        set testset [lindex $argv $i]
      }
      -size -
      --size {
        incr i
        set newsize [lindex $argv $i]
        if {$newsize<1} {set newsize 1}
        set speedtestflags \
          [regsub {.-size \d+} $speedtestflags "-size $newsize"]
      }
      -n -
      -dryrun -
      --dryrun {
        set dryrun 1
      }
      -osmalloc -
      --osmalloc {
        set osmalloc 1
      }
      -? -
      -help -
      --help {
        puts $usage
        exit 0
      }
      -q -
      -quiet -
      --quiet {
        set quiet 1
      }
      default {
        lappend cflags $arg
      }
    }
    continue
  }
  if {[string match CC=* $arg]} {
    set cc [string range $arg 3 end]
    continue
  }
  if {[string match *.c $arg]} {
    if {$srcfile!=""} {
      puts stderr "multiple source files: $srcfile $arg"
      exit 1
    }
    set srcfile $arg
    continue
  }
  if {[lsearch {main cte rtree orm fp json parsenumber mix1} $arg]>=0} {
    set testset $arg
    continue
  }
  if {$outfile==""} {
    set outfile $arg
    continue
  }
  if {$difffile==""} {
    set difffile $arg
    continue
  }
  puts stderr "unknown option: \"$arg\".  Use --help for more info."
  exit 1
}
if {[lsearch -glob $cflags -O*]<0} {
  lappend cflags -Os
}
if {!$osmalloc} {
  append speedtestflags { --heap 40000000 64}
}
if {!$osmalloc && [lsearch -glob $cflags {-DSQLITE_ENABLE_MEMSYS*}]<0} {
  lappend cflags -DSQLITE_ENABLE_MEMSYS5
}
if {[lsearch -glob $cflags {-DSQLITE_ENABLE_RTREE*}]<0} {
  lappend cflags -DSQLITE_ENABLE_RTREE
}
if {$srcfile==""} {
  puts stderr "no sqlite3.c source file specified"
  exit 1
}
if {![file readable $srcfile]} {
  puts stderr "source file \"$srcfile\" does not exist"
  exit 1
}
if {$outfile==""} {
  puts stderr "no output file specified"
  exit 1
}
if {![string match *.* [file tail $outfile]]} {
  append outfile .txt
}
if {$difffile!=""} {
  if {![file exists $difffile]} {
    if {[file exists $difffile.txt]} {
      append difffile .txt
    } else {
      puts stderr "No such file: \"$difffile\""
      exit 1
    }
  }
}

set cccmd [list $cc -g]
lappend cccmd -I[file dir $srcfile]
lappend cccmd {*}[lsort $cflags]
lappend cccmd [file dir $argv0]/speedtest1.c
lappend cccmd $srcfile
lappend cccmd -o speedtest1
puts $cccmd
if {!$dryrun} {
  exec {*}$cccmd
}
lappend speedtestflags --testset $testset
set stcmd [list valgrind --tool=cachegrind ./speedtest1 {*}$speedtestflags]
lappend stcmd speedtest1.db
lappend stcmd >valgrind-out.txt 2>valgrind-err.txt
puts $stcmd
if {!$dryrun} {
   foreach file {speedtest1.db speedtest1.db-journal speedtest1.db-wal
                 speedtest1.db-shm} {
     if {[file exists $file]} {file delete $file}
   }
   exec {*}$stcmd
}

set maxmtime 0
set cgfile {}
foreach cgout [glob -nocomplain cachegrind.out.*] {
  if {[file mtime $cgout]>$maxmtime} {
    set cgfile $cgout
    set maxmtime [file mtime $cgfile]
  }
}
if {$cgfile==""} {
  puts "no cachegrind output"
  exit 1
}

############# Process the cachegrind.out.# file ##########################
set fd [open $outfile wb]
set in [open "|cg_annotate --show=Ir --auto=yes --context=40 $cgfile" r]
set dest !
set out(!) {}
set linenum 0
set cntlines 0      ;# true to remember cycle counts on each line
set seenSqlite3 0   ;# true if we have seen the sqlite3.c file
while {![eof $in]} {
  set line [string map {\t {        }} [gets $in]]
  if {[regexp {^-- Auto-annotated source: (.*)} $line all name]} {
    set dest $name
    if {[string match */sqlite3.c $dest]} {
      set cntlines 1
      set seenSqlite3 1
    } else {
      set cntlines 0
    }
  } elseif {[regexp {^-- line (\d+) ------} $line all ln]} {
    set line [lreplace $line 2 2 {#}]
    set linenum [expr {$ln-1}]
  } elseif {[regexp {^The following files chosen for } $line]} {
    set dest !
  }
  append out($dest) $line\n
  if {$cntlines} {
    incr linenum
    if {[regexp {^ *([0-9,]+) } $line all x]} {
      set x [string map {, {}} $x]
      set cycles($linenum) $x
    }
  }
}
foreach x [lsort [array names out]] {
  puts $fd $out($x)
}
# If the sqlite3.c file has been seen, then output a summary of the
# cycle counts for each file that went into making up sqlite3.c
#
if {$seenSqlite3} {
  close $in
  set in [open sqlite3.c]
  set linenum 0
  set fn sqlite3.c
  set pattern1 {^/\*+ Begin file ([^ ]+) \*}
  set pattern2 {^/\*+ Continuing where we left off in ([^ ]+) \*}
  while {![eof $in]} {
    set line [gets $in]
    incr linenum
    if {[regexp $pattern1 $line all newfn]} {
      set fn $newfn
    } elseif {[regexp $pattern2 $line all newfn]} {
      set fn $newfn
    } elseif {[info exists cycles($linenum)]} {
      incr fcycles($fn) $cycles($linenum)
    }
  }
  close $in
  puts $fd \
     {**********************************************************************}
  set lx {}
  set sum 0
  foreach {fn cnt} [array get fcycles] {
    lappend lx [list $cnt $fn]
    incr sum $cnt
  }
  puts $fd [format {%20s %14d  %8.3f%%} TOTAL $sum 100]
  foreach entry [lsort -index 0 -integer -decreasing $lx] {
    foreach {cnt fn} $entry break
    puts $fd [format {%20s %14d  %8.3f%%} $fn $cnt [expr {$cnt*100.0/$sum}]]
  }
}
puts $fd "Executable size:"
close $fd
exec size speedtest1 >>$outfile
#
#  Processed cachegrind output should now be in the $outfile
#############################################################################

if {$quiet} {
  # Skip this last part of popping up a GUI viewer
} elseif {$difffile!=""} {
  set fossilcmd {fossil xdiff --tk -c 20}
  lappend fossilcmd $difffile
  lappend fossilcmd $outfile
  lappend fossilcmd &
  puts $fossilcmd
  if {!$dryrun} {
    exec {*}$fossilcmd
  }
} else {
  if {!$dryrun} {
    exec open $outfile
  }
}

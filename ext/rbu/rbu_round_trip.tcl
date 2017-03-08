##########################################################################
# 2016 Mar 8
#
# The author disclaims copyright to this source code.  In place of
# a legal notice, here is a blessing:
#
#    May you do good and not evil.
#    May you find forgiveness for yourself and forgive others.
#    May you share freely, never taking more than you give.
#
proc process_cmdline {} { 
  cmdline::process ::A $::argv {
    {make                 "try to build missing tools"}
    {verbose              "make more noise"}
    database
    database2
  } {
 This script uses/tests the following tools:

   dbselftest
   rbu
   sqldiff
   sqlite3
 
 The user passes the names of two database files - a.db and b.db below - as
 arguments. This program:

 1. Runs [dbselftest --init] against both databases.
 2. Runs [sqldiff --rbu --vtab a.db b.db | sqlite3 <tmpname>.db] to create an 
    RBU database.
 3. Runs [rbu b.db <tmpname>.db] to patch b.db to a.db.
 4. Runs [sqldiff --table selftest a.db b.db] to check that the selftest
    tables are now identical.
 5. Runs [dbselftest] against both databases.
  }
}

###########################################################################
###########################################################################
# Command line options processor. This is generic code that can be copied
# between scripts.
#
namespace eval cmdline {
  proc cmdline_error {O E {msg ""}} {
    if {$msg != ""} {
      puts stderr "Error: $msg"
      puts stderr ""
    }
  
    set L [list]
    foreach o $O {
      if {[llength $o]==1} {
        lappend L [string toupper $o]
      }
    }
  
    puts stderr "Usage: $::argv0 ?SWITCHES? $L"
    puts stderr ""
    puts stderr "Switches are:"
    foreach o $O {
      if {[llength $o]==3} {
        foreach {a b c} $o {}
        puts stderr [format "    -%-15s %s (default \"%s\")" "$a VAL" $c $b]
      } elseif {[llength $o]==2} {
        foreach {a b} $o {}
        puts stderr [format "    -%-15s %s" $a $b]
      }
    }
    puts stderr ""
    puts stderr $E
    exit -1
  }
  
  proc process {avar lArgs O E} {
    upvar $avar A
    set zTrailing ""       ;# True if ... is present in $O
    set lPosargs [list]
  
    # Populate A() with default values. Also, for each switch in the command
    # line spec, set an entry in the idx() array as follows:
    #
    #  {tblname t1 "table name to use"}  
    #      -> [set idx(-tblname) {tblname t1 "table name to use"}  
    #
    # For each position parameter, append its name to $lPosargs. If the ...
    # specifier is present, set $zTrailing to the name of the prefix.
    #
    foreach o $O {
      set nm [lindex $o 0]
      set nArg [llength $o]
      switch -- $nArg {
        1 {
          if {[string range $nm end-2 end]=="..."} {
            set zTrailing [string range $nm 0 end-3]
          } else {
            lappend lPosargs $nm
          }
        }
        2 {
          set A($nm) 0
          set idx(-$nm) $o
        }
        3 {
          set A($nm) [lindex $o 1]
          set idx(-$nm) $o
        }
        default {
          error "Error in command line specification"
        }
      }
    }
  
    # Set explicitly specified option values
    #
    set nArg [llength $lArgs]
    for {set i 0} {$i < $nArg} {incr i} {
      set opt [lindex $lArgs $i]
      if {[string range $opt 0 0]!="-" || $opt=="--"} break
      set c [array names idx "${opt}*"]
      if {[llength $c]==0} { cmdline_error $O $E "Unrecognized option: $opt"}
      if {[llength $c]>1}  { cmdline_error $O $E "Ambiguous option: $opt"}
  
      if {[llength $idx($c)]==3} {
        if {$i==[llength $lArgs]-1} {
          cmdline_error $O $E "Option requires argument: $c" 
        }
        incr i
        set A([lindex $idx($c) 0]) [lindex $lArgs $i]
      } else {
        set A([lindex $idx($c) 0]) 1
      }
    }
  
    # Deal with position arguments.
    #
    set nPosarg [llength $lPosargs]
    set nRem [expr $nArg - $i]
    if {$nRem < $nPosarg || ($zTrailing=="" && $nRem > $nPosarg)} {
      cmdline_error $O $E
    }
    for {set j 0} {$j < $nPosarg} {incr j} {
      set A([lindex $lPosargs $j]) [lindex $lArgs [expr $j+$i]]
    }
    if {$zTrailing!=""} {
      set A($zTrailing) [lrange $lArgs [expr $j+$i] end]
    }
  }
} ;# namespace eval cmdline
# End of command line options processor.
###########################################################################
###########################################################################

process_cmdline

# Check that the specified tool is present.
#
proc check_for_tool {tool} {
  if {[file exists $tool]==0 || [file executable $tool]==0} {
    puts stderr "Missing $tool... exiting. (run \[make $tool\])"
    exit -1
  }
}

if {$A(make)} {
  if {$A(verbose)} { puts "building tools..." }
  exec make dbselftest rbu sqlite3 sqldiff
}

check_for_tool dbselftest
check_for_tool rbu
check_for_tool sqlite3
check_for_tool sqldiff

exec ./sqlite3 $A(database) "DROP TABLE selftest;"
exec ./sqlite3 $A(database2) "DROP TABLE selftest;"

# Run [dbselftest --init] on both databases
if {$A(verbose)} { puts "Running \[dbselftest --init\]" }
exec ./dbselftest --init $A(database)
exec ./dbselftest --init $A(database2)

# Create an RBU patch.
set tmpname "./rrt-[format %x [expr int(rand()*0x7FFFFFFF)]].db"
if {$A(verbose)} { puts "rbu database is $tmpname" }
exec ./sqldiff --rbu --vtab $A(database2) $A(database) | ./sqlite3 $tmpname

# Run the [rbu] patch.
if {$A(verbose)} { puts "Running \[rbu]" }
exec ./rbu $A(database2) $tmpname

set selftest_diff [exec ./sqldiff --table selftest $A(database) $A(database2)]
if {$selftest_diff != ""} {
  puts stderr "patching table \"selftest\" failed: $selftest_diff"
  exit -1
}

# Run [dbselftest] on both databases
if {$A(verbose)} { puts "Running \[dbselftest]" }
exec ./dbselftest $A(database)
exec ./dbselftest $A(database2)

# Remove the RBU database
file delete $tmpname
puts "round trip test successful."


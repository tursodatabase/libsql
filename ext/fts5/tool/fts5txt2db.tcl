

#-------------------------------------------------------------------------
# Command line options processor.
#
proc command_line_error {O E {msg ""}} {
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

proc process_command_line {avar lArgs O E} {

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
    if {[llength $c]==0} { command_line_error $O $E "Unrecognized option: $opt"}
    if {[llength $c]>1}  { command_line_error $O $E "Ambiguous option: $opt"}

    if {[llength $idx($c)]==3} {
      if {$i==[llength $lArgs]-1} {
        command_line_error $O $E "Option requires argument: $c" 
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
    command_line_error $O $E
  }
  for {set j 0} {$j < $nPosarg} {incr j} {
    set A([lindex $lPosargs $j]) [lindex $lArgs [expr $j+$i]]
  }
  if {$zTrailing!=""} {
    set A($zTrailing) [lrange $lArgs [expr $j+$i] end]
  }
}
# End of command line options processor.
#-------------------------------------------------------------------------


process_command_line A $argv {
  {fts5                 "use fts5"}
  {fts4                 "use fts4"}
  {colsize   "10 10 10" "list of column sizes"}
  {tblname   "t1"       "table name to create"}
  {detail    "full"     "Fts5 detail mode to use"}
  {repeat    1          "Load each file this many times"}
  database
  file...
} {
This script is designed to create fts4/5 tables with more than one column.
The -colsize option should be set to a Tcl list of integer values, one for
each column in the table. Each value is the number of tokens that will be
inserted into the column value for each row. For example, setting the -colsize
option to "5 10" creates an FTS table with 2 columns, with roughly 5 and 10
tokens per row in each, respectively.

Each "FILE" argument should be a text file. The contents of these text files is
split on whitespace characters to form a list of tokens. The first N1 tokens
are used for the first column of the first row, where N1 is the first element
of the -colsize list. The next N2 are used for the second column of the first
row, and so on. Rows are added to the table until the entire list of tokens
is exhausted.
}

if {$A(fts4)} {
  set A(fts) fts4
} else {
  set A(fts) fts5
}

sqlite3 db $A(database)

# Create the FTS table in the db. Return a list of the table columns.
#
proc create_table {} {
  global A
  set cols [list a b c d e f g h i j k l m n o p q r s t u v w x y z]

  set nCol [llength $A(colsize)]
  set cols [lrange $cols 0 [expr $nCol-1]]

  set sql    "CREATE VIRTUAL TABLE IF NOT EXISTS $A(tblname) USING $A(fts) ("
  append sql [join $cols ,]
  if {$A(fts)=="fts5"} { append sql ",detail=$A(detail));" }

  db eval $sql
  return $cols
}

# Return a list of tokens from the named file.
#
proc readfile {file} {
  set fd [open $file]
  set data [read $fd]
  close $fd
  split $data
}

proc repeat {L n} {
  set res [list]
  for {set i 0} {$i < $n} {incr i} {
    set res [concat $res $L]
  }
  set res
}


# Load all the data into a big list of tokens.
#
set tokens [list]
foreach f $A(file) {
  set tokens [concat $tokens [repeat [readfile $f] $A(repeat)]]
}

set N [llength $tokens]
set i 0
set cols [create_table]
set sql "INSERT INTO $A(tblname) VALUES(\$R([lindex $cols 0])"
foreach c [lrange $cols 1 end] {
  append sql ", \$R($c)"
}
append sql ")"

db eval BEGIN
  while {$i < $N} {
    foreach c $cols s $A(colsize) {
      set R($c) [lrange $tokens $i [expr $i+$s-1]]
      incr i $s
    }
    db eval $sql
  }
db eval COMMIT




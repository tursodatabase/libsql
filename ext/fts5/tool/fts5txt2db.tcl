

proc usage {} {
  puts stderr "$::argv0 ?OPTIONS? DATABASE FILE1..."
  puts stderr ""
  puts stderr "Options are"
  puts stderr "  -fts5"
  puts stderr "  -fts4"
  puts stderr "  -colsize <list of column sizes>"
  puts stderr {
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
  exit -1
}

set O(aColSize)       [list 10 10 10]
set O(tblname)        t1
set O(fts)            fts5


set options_with_values {-colsize}

for {set i 0} {$i < [llength $argv]} {incr i} {
  set opt [lindex $argv $i]
  if {[string range $opt 0 0]!="-"} break

  if {[lsearch $options_with_values $opt]>=0} {
    incr i
    if {$i==[llength $argv]} usage
    set val [lindex $argv $i]
  }

  switch -- $opt {
    -colsize {
      set O(aColSize) $val
    }

    -fts4 {
      set O(fts) fts4
    }

    -fts5 {
      set O(fts) fts5
    }
  }
}

if {$i > [llength $argv]-2} usage
set O(db) [lindex $argv $i]
set O(files) [lrange $argv [expr $i+1] end]

sqlite3 db $O(db)

# Create the FTS table in the db. Return a list of the table columns.
#
proc create_table {} {
  global O
  set cols [list a b c d e f g h i j k l m n o p q r s t u v w x y z]

  set nCol [llength $O(aColSize)]
  set cols [lrange $cols 0 [expr $nCol-1]]

  set sql    "CREATE VIRTUAL TABLE IF NOT EXISTS $O(tblname) USING $O(fts) ("
  append sql [join $cols ,]
  append sql ");"

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


# Load all the data into a big list of tokens.
#
set tokens [list]
foreach f $O(files) {
  set tokens [concat $tokens [readfile $f]]
}

set N [llength $tokens]
set i 0
set cols [create_table]
set sql "INSERT INTO $O(tblname) VALUES(\$[lindex $cols 0]"
foreach c [lrange $cols 1 end] {
  append sql ", \$A($c)"
}
append sql ")"

db eval BEGIN
  while {$i < $N} {
    foreach c $cols s $O(aColSize) {
      set A($c) [lrange $tokens $i [expr $i+$s-1]]
      incr i $s
    }
    db eval $sql
  }
db eval COMMIT




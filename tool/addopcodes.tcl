#!/usr/bin/tclsh
#
# This script appends additional token codes to the end of the
# parse.h file that lemon generates.  These extra token codes are
# not used by the parser.  But they are used by the tokenizer and/or
# the code generator.
#
#
set in [open [lindex $argv 0] rb]
set max 0
while {![eof $in]} {
  set line [gets $in]
  if {[regexp {^#define TK_} $line]} {
    puts $line
    set x [lindex $line 2]
    if {$x>$max} {set max $x}
  }
}
close $in

# The following are the extra token codes to be added
#
set extras {
  TO_TEXT
  TO_BLOB
  TO_NUMERIC
  TO_INT
  TO_REAL
  ISNOT
  END_OF_FILE
  ILLEGAL
  SPACE
  UNCLOSED_STRING
  FUNCTION
  COLUMN
  AGG_FUNCTION
  AGG_COLUMN
  UMINUS
  UPLUS
  REGISTER
}
foreach x $extras {
  incr max
  puts [format "#define TK_%-29s %4d" $x $max]
}

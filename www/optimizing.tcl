set rcsid {$Id: optimizing.tcl,v 1.1 2005/01/17 03:42:52 drh Exp $}
source common.tcl
header {Hints For Optimizing Queries In SQLite}
proc section {level tag name} {
  incr level
  if {$level>6} {set level 6}
  puts "\n"<a name=\"tag\" />"
  puts "<h$level>$name</h$level>\n"
}
section 1 recompile {Recompile the library for optimal performance}
section 2 avoidtrans {Minimize the number of transactions}
section 3 usebind {Use sqlite3_bind to insert large chunks of data}
section 4 useindices {Use appropriate indices}
section 5 recordjoin {Reorder the tables in a join}
footer $rcsid




#-------------------------------------------------------------------------
# Process command line arguments.
#
proc usage {} {
  puts stderr "usage: $::argv0 database table"
  puts stderr ""
  exit 1
}
if {[llength $argv]!=2} usage
set database [lindex $argv 0]
set tbl [lindex $argv 1]



#-------------------------------------------------------------------------
# Start of main program.
#
sqlite3 db $database
catch { load_static_extension db fts5 }

db eval "SELECT fts5_decode(rowid, block) AS d FROM ${tbl}_data WHERE id=10" {
  foreach lvl [lrange $d 1 end] {
    puts [lrange $lvl 0 2]
    foreach seg [lrange $lvl 3 end] {
      puts "        $seg"
    }
  }
}






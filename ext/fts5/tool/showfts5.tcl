

proc usage {} {
  puts stderr "usage: $::argv0 database table"
  puts stderr ""
  exit 1
}

set o(vtab)       fts5
set o(tok)        ""
set o(limit)      0
set o(automerge)  -1
set o(crisismerge)  -1

if {[llength $argv]!=2} usage

set database [lindex $argv 0]
set tbl [lindex $argv 1]

sqlite3 db $database

db eval "SELECT fts5_decode(rowid, block) AS d FROM ${tbl}_data WHERE id=10" {
  foreach lvl [lrange $d 1 end] {
    puts $lvl
  }
}






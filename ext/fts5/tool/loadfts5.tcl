

proc loadfile {f} {
  set fd [open $f]
  set data [read $fd]
  close $fd
  return $data
}

set ::nRow 0
proc load_hierachy {dir} {
  foreach f [glob -nocomplain -dir $dir *] {
    if {$::O(limit) && $::nRow>=$::O(limit)} break
    if {[file isdir $f]} {
      load_hierachy $f
    } else {
      db eval { INSERT INTO t1 VALUES($f, loadfile($f)) }
      incr ::nRow
    }
  }
}

proc usage {} {
  puts stderr "Usage: $::argv0 ?SWITCHES? DATABASE PATH"
  puts stderr ""
  puts stderr "Switches are:"
  puts stderr "  -fts4     (use fts4 instead of fts5)"
  exit 1
}

set O(vtab)   fts5
set O(tok)    ""
set O(limit)  0

if {[llength $argv]<2} usage
for {set i 0} {$i < [llength $argv]-2} {incr i} {
  set arg [lindex $argv $i]
  switch -- [lindex $argv $i] {
    -fts4 {
      set O(vtab) fts4
    }

    -fts5 {
      set O(vtab) fts5
    }

    -porter {
      set O(tok) ", tokenize=porter"
    }

    -limit {
      incr i
      set O(limit) [lindex $argv $i]
    }

    default {
      usage
    }
  }
}

sqlite3 db [lindex $argv end-1]
db func loadfile loadfile

db transaction {
  db eval "CREATE VIRTUAL TABLE t1 USING $O(vtab) (path, content$O(tok))"
  load_hierachy [lindex $argv end]
}




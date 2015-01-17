

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
  puts stderr "  -fts4        (use fts4 instead of fts5)"
  puts stderr "  -fts5        (use fts5)"
  puts stderr "  -porter      (use porter tokenizer)"
  puts stderr "  -limit N     (load no more than N documents)"
  puts stderr "  -automerge N (set the automerge parameter to N)"
  exit 1
}

set O(vtab)       fts5
set O(tok)        ""
set O(limit)      0
set O(automerge)  -1

if {[llength $argv]<2} usage
set nOpt [expr {[llength $argv]-2}]
for {set i 0} {$i < $nOpt} {incr i} {
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
      if { [incr i]>=$nOpt } usage
      set O(limit) [lindex $argv $i]
    }
    
    -automerge {
      if { [incr i]>=$nOpt } usage
      set O(automerge) [lindex $argv $i]
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
  if {$O(automerge)>=0} {
    if {$O(vtab) == "fts5"} {
      db eval { INSERT INTO t1(t1, rank) VALUES('automerge', $O(automerge)) }
    } else {
      db eval { INSERT INTO t1(t1) VALUES('automerge=' || $O(automerge)) }
    }
  }
  load_hierachy [lindex $argv end]
}




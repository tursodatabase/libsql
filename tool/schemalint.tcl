


set ::G(lSelect)  [list]           ;# List of SELECT statements to analyze
set ::G(database) ""               ;# Name of database or SQL schema file
set ::G(trace)    [list]           ;# List of data from xTrace()
set ::G(verbose)  0                ;# True if -verbose option was passed 

proc usage {} {
  puts stderr "Usage: $::argv0 ?SWITCHES? DATABASE/SCHEMA"
  puts stderr "  Switches are:"
  puts stderr "  -select SQL     (recommend indexes for SQL statement)"
  puts stderr "  -verbose        (increase verbosity of output)"
  puts stderr ""
  exit
}

proc process_cmdline_args {argv} {
  global G
  set nArg [llength $argv]
  set G(database) [lindex $argv end]

  for {set i 0} {$i < [llength $argv]-1} {incr i} {
    set k [lindex $argv $i]
    switch -- $k {
      -select {
        incr i
        if {$i>=[llength $argv]-1} usage
        lappend G(lSelect) [lindex $argv $i]
      }
      -verbose {
        set G(verbose) 1
      }
      default {
        usage
      }
    }
  }
}

proc open_database {} {
  global G
  sqlite3 db ""

  # Check if the "database" file is really an SQLite database. If so, copy
  # it into the temp db just opened. Otherwise, assume that it is an SQL
  # schema and execute it directly.
  set fd [open $G(database)]
  set hdr [read $fd 16]
  if {$hdr == "SQLite format 3\000"} {
    close $fd
    sqlite3 db2 $G(database)
    sqlite3_backup B db main db2 main
    B step 2000000000
    set rc [B finish]
    db2 close
    if {$rc != "SQLITE_OK"} { error "Failed to load database $G(database)" }
  } else {
    append hdr [read $fd]
    db eval $hdr
    close $fd
  }
}

proc analyze_selects {} {
  global G
  set G(trace) ""

  # Collect a line of xTrace output for each loop in the set of SELECT
  # statements.
  proc xTrace {zMsg} { lappend ::G(trace) $zMsg }
  db trace "lappend ::G(trace)"
  foreach s $G(lSelect) {
    set stmt [sqlite3_prepare_v2 db $s -1 dummy]
    set rc [sqlite3_finalize $stmt]
    if {$rc!="SQLITE_OK"} {
      error "Failed to compile SQL: [sqlite3_errmsg db]"
    }
  }

  db trace ""
  if {$G(verbose)} {
    foreach t $G(trace) { puts "trace: $t" }
  }

  # puts $G(trace)
}

# The argument is a list of the form:
#
#    key1 {value1.1 value1.2} key2 {value2.1 value 2.2...}
#
# Values lists may be of any length greater than zero. This function returns
# a list of lists created by pivoting on each values list. i.e. a list
# consisting of the elements:
#
#   {{key1 value1.1} {key2 value2.1}}
#   {{key1 value1.2} {key2 value2.1}}
#   {{key1 value1.1} {key2 value2.2}}
#   {{key1 value1.2} {key2 value2.2}}
#
proc expand_eq_list {L} {
  set ll [list {}]
  for {set i 0} {$i < [llength $L]} {incr i 2} {
    set key [lindex $L $i]
    set new [list]
    foreach piv [lindex $L $i+1] {
      foreach l $ll {
        lappend new [concat $l [list [list $key $piv]]]
      }
    }
    set ll $new
  }

  return $ll
}

proc eqset_to_index {tname eqset {range {}}} {
  global G
  set lCols [list]
  set idxname $tname
  foreach e [concat [lsort $eqset] [list $range]] {
    if {[llength $e]==0} continue
    foreach {c collate} $e {}
    lappend lCols "$c collate $collate"
    append idxname "_$c"
    if {[string compare -nocase binary $collate]!=0} {
      append idxname [string tolower $collate]
    }
  }

  set create_index "CREATE INDEX $idxname ON ${tname}("
  append create_index [join $lCols ", "]
  append create_index ");"

  set G(trial.$idxname) $create_index
}

proc expand_or_cons {L} {
  set lRet [list [list]]
  foreach elem $L {
    set type [lindex $elem 0]
    if {$type=="eq" || $type=="range"} {
      set lNew [list]
      for {set i 0} {$i < [llength $lRet]} {incr i} {
        lappend lNew [concat [lindex $lRet $i] [list $elem]]
      }
      set lRet $lNew
    } elseif {$type=="or"} {
      set lNew [list]
      foreach branch [lrange $elem 1 end] {
        foreach b [expand_or_cons $branch] {
          for {set i 0} {$i < [llength $lRet]} {incr i} {
            lappend lNew [concat [lindex $lRet $i] $b]
          }
        }
      }
      set lRet $lNew
    } 
  }
  return $lRet
}

proc find_trial_indexes {} {
  global G
  foreach t $G(trace) {
    set tname [lindex $t 0]
    catch { array unset mask }

    foreach lCons [expand_or_cons [lrange $t 2 end]] {
      set constraints [list]

      foreach a $lCons {
        set type [lindex $a 0]
        if {$type=="eq" || $type=="range"} {
          set m [lindex $a 3]
          foreach k [array names mask] { set mask([expr ($k & $m)]) 1 }
          set mask($m) 1
          lappend constraints $a
        }
      }

      foreach k [array names mask] {
        catch {array unset eq}
        foreach a $constraints {
          foreach {type col collate m} $a {
            if {($m & $k)==$m} {
              if {$type=="eq"} {
                lappend eq($col) $collate
              } else {
                set range($col.$collate) 1
              }
            }
          }
        }

        #puts "mask=$k eq=[array get eq] range=[array get range]"
        
        set ranges [array names range]
        foreach eqset [expand_eq_list [array get eq]] {
          if {[llength $ranges]==0} {
            eqset_to_index $tname $eqset
          } else {
            foreach r $ranges {
              set bSeen 0
              foreach {c collate} [split $r .] {}
              foreach e $eqset {
                if {[lindex $e 0] == $c} {
                  set bSeen 1
                  break
                }
              }
              if {$bSeen} {
                eqset_to_index $tname $eqset
              } else {
                eqset_to_index $tname $eqset [list $c $collate]
              }
            }
          }
        }
      }
    }
  }

  if {$G(verbose)} {
    foreach k [array names G trial.*] { puts "index: $G($k)" }
  }
}

proc run_trials {} {
  global G

  foreach k [array names G trial.*] {
    set idxname [lindex [split $k .] 1]
    db eval $G($k)
    set pgno [db one {SELECT rootpage FROM sqlite_master WHERE name = $idxname}]
    set IDX($pgno) $idxname
  }
  db eval ANALYZE

  catch { array unset used }
  foreach s $G(lSelect) {
    db eval "EXPLAIN $s" x {
      if {($x(opcode)=="OpenRead" || $x(opcode)=="ReopenIdx")} {
        if {[info exists IDX($x(p2))]} { set used($IDX($x(p2))) 1 }
      }
    }
    foreach idx [array names used] {
      puts $G(trial.$idx)
    }
  }
}

process_cmdline_args $argv
open_database
analyze_selects
find_trial_indexes
run_trials


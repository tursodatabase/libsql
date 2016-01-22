if {[catch {

set ::VERBOSE 0

proc usage {} {
  puts stderr "Usage: $::argv0 ?SWITCHES? DATABASE/SCHEMA"
  puts stderr "  Switches are:"
  puts stderr "  -select SQL     (recommend indexes for SQL statement)"
  puts stderr "  -verbose        (increase verbosity of output)"
  puts stderr "  -test           (run internal tests and then exit)"
  puts stderr ""
  exit
}

# Return the quoted version of identfier $id. Quotes are only added if 
# they are required by SQLite.
#
# This command currently assumes that quotes are required if the 
# identifier contains any ASCII-range characters that are not 
# alpha-numeric or underscores.
#
proc quote {id} {
  if {[requires_quote $id]} {
    set x [string map {\" \"\"} $id]
    return "\"$x\""
  }
  return $id
}
proc requires_quote {id} {
  foreach c [split $id {}] {
    if {[string is alnum $c]==0 && $c!="_"} {
      return 1
    }
  }
  return 0
}

# The argument passed to this command is a Tcl list of identifiers. The
# value returned is the same list, except with each item quoted and the
# elements comma-separated.
#
proc list_to_sql {L} {
  set ret [list]
  foreach l $L {
    lappend ret [quote $l]
  }
  join $ret ", "
}

proc readfile {zFile} {
  set fd [open $zFile]
  set data [read $fd]
  close $fd
  return $data
}

proc process_cmdline_args {ctxvar argv} {
  upvar $ctxvar G
  set nArg [llength $argv]
  set G(database) [lindex $argv end]

  for {set i 0} {$i < [llength $argv]-1} {incr i} {
    set k [lindex $argv $i]
    switch -- $k {
      -select {
        incr i
        if {$i>=[llength $argv]-1} usage
        set zSelect [lindex $argv $i]
        if {[file readable $zSelect]} {
          lappend G(lSelect) [readfile $zSelect]
        } else {
          lappend G(lSelect) $zSelect
        }
      }
      -verbose {
        set ::VERBOSE 1
      }
      -test {
        sqlidx_internal_tests
      }
      default {
        usage
      }
    }
  }

  if {$G(database)=="-test"} {
    sqlidx_internal_tests
  }
}

proc open_database {ctxvar} {
  upvar $ctxvar G
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

proc analyze_selects {ctxvar} {
  upvar $ctxvar G
  set G(trace) ""

  # Collect a line of xTrace output for each loop in the set of SELECT
  # statements.
  proc xTrace {zMsg} { 
    upvar G G
    lappend G(trace) $zMsg 
  }
  db trace xTrace
  foreach s $G(lSelect) {
    set stmt [sqlite3_prepare_v2 db $s -1 dummy]
    set rc [sqlite3_finalize $stmt]
    if {$rc!="SQLITE_OK"} {
      error "Failed to compile SQL: [sqlite3_errmsg db]"
    }
  }

  db trace ""
  if {$::VERBOSE} {
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

#--------------------------------------------------------------------------
# Formulate a CREATE INDEX statement that creates an index on table $tname.
#
proc eqset_to_index {ctxvar aCollVar tname eqset {range {}}} {
  upvar $ctxvar G
  upvar $aCollVar aColl

  set rangeset [list]
  foreach e [lsort $eqset] {
    lappend rangeset [lindex $e 0] [lindex $e 1] ASC
  }
  set rangeset [concat $rangeset $range]

  set lCols [list]
  set idxname $tname

  foreach {c collate dir} $rangeset {
    append idxname "_$c"
    set coldef [quote $c]

    if {[string compare -nocase $collate $aColl($c)]!=0} {
      append idxname [string tolower $collate]
      append coldef " COLLATE [quote $collate]"
    }

    if {$dir=="DESC"} {
      append coldef " DESC"
      append idxname "desc"
    }
    lappend lCols $coldef
  }

  set create_index "CREATE INDEX [quote $idxname] ON [quote $tname]("
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

#--------------------------------------------------------------------------
# Argument $tname is the name of a table in the main database opened by
# database handle [db]. $arrayvar is the name of an array variable in the
# caller's context. This command populates the array with an entry mapping 
# from column name to default collation sequence for each column of table
# $tname. For example, if a table is declared:
#
#   CREATE TABLE t1(a COLLATE nocase, b, c COLLATE binary)
#
# the mapping is populated with:
#
#   map(a) -> "nocase"
#   map(b) -> "binary"
#   map(c) -> "binary"
#
proc sqlidx_get_coll_map {tname arrayvar} {
  upvar $arrayvar aColl
  set colnames [list]
  set qname [quote $tname]
  db eval "PRAGMA table_info = $qname" x { lappend colnames $x(name) }
  db eval "CREATE INDEX schemalint_test ON ${qname}([list_to_sql $colnames])"
  db eval "PRAGMA index_xinfo = schemalint_test" x { 
    set aColl($x(name)) $x(coll)
  }
  db eval "DROP INDEX schemalint_test"
}

proc find_trial_indexes {ctxvar} {
  upvar $ctxvar G
  foreach t $G(trace) {
    set tname [lindex $t 0]
    catch { array unset mask }

    # Invoke "PRAGMA table_info" on the table. Use the results to create
    # an array mapping from column name to collation sequence. Store the
    # array in local variable aColl.
    #
    sqlidx_get_coll_map $tname aColl

    set orderby [list]
    if {[lindex $t end 0]=="orderby"} {
      set orderby [lrange [lindex $t end] 1 end]
    }

    foreach lCons [expand_or_cons [lrange $t 2 end]] {

      # Populate the array mask() so that it contains an entry for each
      # combination of prerequisite scans that may lead to distinct sets
      # of constraints being usable.
      #
      catch { array unset mask }
      set mask(0) 1
      foreach a $lCons {
        set type [lindex $a 0]
        if {$type=="eq" || $type=="range"} {
          set m [lindex $a 3]
          foreach k [array names mask] { set mask([expr ($k & $m)]) 1 }
          set mask($m) 1
        }
      }

      # Loop once for each distinct prerequisite scan mask identified in
      # the previous block.
      #
      foreach k [array names mask] {

        # Identify the constraints available for prerequisite mask $k. For
        # each == constraint, set an entry in the eq() array as follows:
        # 
        #   set eq(<col>) <collation>
        #
        # If there is more than one == constraint for a column, and they use
        # different collation sequences, <collation> is replaced with a list
        # of the possible collation sequences. For example, for:
        #
        #   SELECT * FROM t1 WHERE a=? COLLATE BINARY AND a=? COLLATE NOCASE
        #
        # Set the following entry in the eq() array:
        #
        #   set eq(a) {binary nocase}
        #
        # For each range constraint found an entry is appended to the $ranges
        # list. The entry is itself a list of the form {<col> <collation>}.
        #
        catch {array unset eq}
        set ranges [list]
        foreach a $lCons {
          set type [lindex $a 0]
          if {$type=="eq" || $type=="range"} {
            foreach {type col collate m} $a {
              if {($m & $k)==$m} {
                if {$type=="eq"} {
                  lappend eq($col) $collate
                } else {
                  lappend ranges [list $col $collate ASC]
                }
              }
            }
          }
        }
        set ranges [lsort -unique $ranges]
        if {$orderby != ""} {
          lappend ranges $orderby
        }

        foreach eqset [expand_eq_list [array get eq]] {
          if {$eqset != ""} {
            eqset_to_index G aColl $tname $eqset
          }

          foreach r $ranges {
            set tail [list]
            foreach {c collate dir} $r {
              set bSeen 0
              foreach e $eqset {
                if {[lindex $e 0] == $c} {
                  set bSeen 1
                  break
                }
              }
              if {$bSeen==0} { lappend tail {*}$r }
            }
            if {[llength $tail]} {
              eqset_to_index G aColl $tname $eqset $r
            }
          }
        }
      }
    }
  }

  if {$::VERBOSE} {
    foreach k [array names G trial.*] { puts "index: $G($k)" }
  }
}

proc run_trials {ctxvar} {
  upvar $ctxvar G
  set ret [list]

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
      lappend ret $G(trial.$idx)
    }
  }

  set ret
}

proc sqlidx_init_context {varname} {
  upvar $varname G
  set G(lSelect)  [list]           ;# List of SELECT statements to analyze
  set G(database) ""               ;# Name of database or SQL schema file
  set G(trace)    [list]           ;# List of data from xTrace()
}

#-------------------------------------------------------------------------
# The following is test code only.
#
proc sqlidx_one_test {tn schema select expected} {
#  if {$tn!=2} return
  sqlidx_init_context C

  sqlite3 db ""
  db collate "a b c" [list string compare]
  db eval $schema
  lappend C(lSelect) $select
  analyze_selects C
  find_trial_indexes C

  set idxlist [run_trials C]
  if {$idxlist != [list {*}$expected]} {
    puts stderr "Test $tn failed"
    puts stderr "Expected: $expected"
    puts stderr "Got: $idxlist"
    exit -1
  }

  db close

  upvar nTest nTest
  incr nTest
}

proc sqlidx_internal_tests {} {
  set nTest 0


  # No indexes for a query with no constraints.
  sqlidx_one_test 0 {
    CREATE TABLE t1(a, b, c);
  } {
    SELECT * FROM t1;
  } {
  }

  sqlidx_one_test 1 {
    CREATE TABLE t1(a, b, c);
    CREATE TABLE t2(x, y, z);
  } {
    SELECT a FROM t1, t2 WHERE a=? AND x=c
  } {
    {CREATE INDEX t2_x ON t2(x);}
    {CREATE INDEX t1_a_c ON t1(a, c);}
  }

  sqlidx_one_test 2 {
    CREATE TABLE t1(a, b, c);
  } {
    SELECT * FROM t1 WHERE b>?;
  } {
    {CREATE INDEX t1_b ON t1(b);}
  }

  sqlidx_one_test 3 {
    CREATE TABLE t1(a, b, c);
  } {
    SELECT * FROM t1 WHERE b COLLATE nocase BETWEEN ? AND ?
  } {
    {CREATE INDEX t1_bnocase ON t1(b COLLATE NOCASE);}
  }

  sqlidx_one_test 4 {
    CREATE TABLE t1(a, b, c);
  } {
    SELECT a FROM t1 ORDER BY b;
  } {
    {CREATE INDEX t1_b ON t1(b);}
  }

  sqlidx_one_test 5 {
    CREATE TABLE t1(a, b, c);
  } {
    SELECT a FROM t1 WHERE a=? ORDER BY b;
  } {
    {CREATE INDEX t1_a_b ON t1(a, b);}
  }

  sqlidx_one_test 5 {
    CREATE TABLE t1(a, b, c);
  } {
    SELECT min(a) FROM t1
  } {
    {CREATE INDEX t1_a ON t1(a);}
  }

  sqlidx_one_test 6 {
    CREATE TABLE t1(a, b, c);
  } {
    SELECT * FROM t1 ORDER BY a ASC, b COLLATE nocase DESC, c ASC;
  } {
    {CREATE INDEX t1_a_bnocasedesc_c ON t1(a, b COLLATE NOCASE DESC, c);}
  }

  sqlidx_one_test 7 {
    CREATE TABLE t1(a COLLATE NOCase, b, c);
  } {
    SELECT * FROM t1 WHERE a=?
  } {
    {CREATE INDEX t1_a ON t1(a);}
  }

  # Tables with names that require quotes.
  #
  sqlidx_one_test 8.1 {
    CREATE TABLE "t t"(a, b, c);
  } {
    SELECT * FROM "t t" WHERE a=?
  } {
    {CREATE INDEX "t t_a" ON "t t"(a);}
  }
  sqlidx_one_test 8.2 {
    CREATE TABLE "t t"(a, b, c);
  } {
    SELECT * FROM "t t" WHERE b BETWEEN ? AND ?
  } {
    {CREATE INDEX "t t_b" ON "t t"(b);}
  }
  
  # Columns with names that require quotes.
  #
  sqlidx_one_test 9.1 {
    CREATE TABLE t3(a, "b b", c);
  } {
    SELECT * FROM t3 WHERE "b b" = ?
  } {
    {CREATE INDEX "t3_b b" ON t3("b b");}
  }
  sqlidx_one_test 9.2 {
    CREATE TABLE t3(a, "b b", c);
  } {
    SELECT * FROM t3 ORDER BY "b b"
  } {
    {CREATE INDEX "t3_b b" ON t3("b b");}
  }

  # Collations with names that require quotes.
  #
  sqlidx_one_test 10.1 {
    CREATE TABLE t4(a, b, c);
  } {
    SELECT * FROM t4 ORDER BY c COLLATE "a b c"
  } {
    {CREATE INDEX "t4_ca b c" ON t4(c COLLATE "a b c");}
  }
  sqlidx_one_test 10.2 {
    CREATE TABLE t4(a, b, c);
  } {
    SELECT * FROM t4 WHERE c = ? COLLATE "a b c"
  } {
    {CREATE INDEX "t4_ca b c" ON t4(c COLLATE "a b c");}
  }

  puts "All $nTest tests passed"
  exit
}
# End of internal test code.
#-------------------------------------------------------------------------

if {[info exists ::argv0]==0} { set ::argv0 [info nameofexec] }
if {[info exists ::argv]==0} usage
sqlidx_init_context D
process_cmdline_args D $::argv
open_database D
analyze_selects D
find_trial_indexes D
foreach idx [run_trials D] { puts $idx }

} err]} {
  puts "ERROR: $err"
  puts $errorInfo
  exit 1
}


proc do_changeset_test {tn session res} {
  set r [list]
  foreach x $res {lappend r $x}
  uplevel do_test $tn [list [subst -nocommands {
    set x [list]
    sqlite3session_foreach c [$session changeset] { lappend x [set c] }
    set x
  }]] [list $r]
}

proc do_patchset_test {tn session res} {
  set r [list]
  foreach x $res {lappend r $x}
  uplevel do_test $tn [list [subst -nocommands {
    set x [list]
    sqlite3session_foreach c [$session patchset] { lappend x [set c] }
    set x
  }]] [list $r]
}


proc do_changeset_invert_test {tn session res} {
  set r [list]
  foreach x $res {lappend r $x}
  uplevel do_test $tn [list [subst -nocommands {
    set x [list]
    set changeset [sqlite3changeset_invert [$session changeset]]
    sqlite3session_foreach c [set changeset] { lappend x [set c] }
    set x
  }]] [list $r]
}


proc do_conflict_test {tn args} {

  set O(-tables)    [list]
  set O(-sql)       [list]
  set O(-conflicts) [list]
  set O(-policy)    "OMIT"

  array set V $args
  foreach key [array names V] {
    if {![info exists O($key)]} {error "no such option: $key"}
  }
  array set O $args

  proc xConflict {args} [subst -nocommands { 
    lappend ::xConflict [set args]
    return $O(-policy) 
  }]
  proc bgerror {args} { set ::background_error $args }

  sqlite3session S db main
  S object_config rowid 1
  foreach t $O(-tables) { S attach $t }
  execsql $O(-sql)

  set ::xConflict [list]
  sqlite3changeset_apply db2 [S changeset] xConflict

  set conflicts [list]
  foreach c $O(-conflicts) {
    lappend conflicts $c
  }

  after 1 {set go 1}
  vwait go

  uplevel do_test $tn [list { set ::xConflict }] [list $conflicts]
  S delete
}

proc do_common_sql {sql} {
  execsql $sql db
  execsql $sql db2
}

proc changeset_from_sql {sql {dbname main}} {
  if {$dbname == "main"} {
    return [sql_exec_changeset db $sql]
  }
  set rc [catch {
    sqlite3session S db $dbname
    S object_config rowid 1
    db eval "SELECT name FROM $dbname.sqlite_master WHERE type = 'table'" {
      S attach $name
    }
    db eval $sql
    S changeset
  } changeset]
  catch { S delete }

  if {$rc} {
    error $changeset
  }
  return $changeset
}

proc patchset_from_sql {sql {dbname main}} {
  set rc [catch {
    sqlite3session S db $dbname
    db eval "SELECT name FROM $dbname.sqlite_master WHERE type = 'table'" {
      S attach $name
    }
    db eval $sql
    S patchset
  } patchset]
  catch { S delete }

  if {$rc} {
    error $patchset
  }
  return $patchset
}

# Usage: do_then_apply_sql ?-ignorenoop? SQL ?DBNAME? 
#
proc do_then_apply_sql {args} {
  
  set bIgnoreNoop 0
  set a1 [lindex $args 0]
  if {[string length $a1]>1 && [string first $a1 -ignorenoop]==0} {
    set bIgnoreNoop 1
    set args [lrange $args 1 end]
  }

  if {[llength $args]!=1 && [llength $args]!=2} {
    error "usage: do_then_apply_sql ?-ignorenoop? SQL ?DBNAME?"
  }

  set sql [lindex $args 0]
  if {[llength $args]==1} {
    set dbname main
  } else {
    set dbname [lindex $args 1]
  }

  set ::n_conflict 0
  proc xConflict args { incr ::n_conflict ; return "OMIT" }
  set rc [catch {
    sqlite3session S db $dbname
    S object_config rowid 1
    db eval "SELECT name FROM $dbname.sqlite_master WHERE type = 'table'" {
      S attach $name
    }
    db eval $sql
    set ::changeset [S changeset]
    sqlite3changeset_apply db2 $::changeset xConflict
  } msg]

  catch { S delete }
  if {$rc} {error $msg}

  if {$bIgnoreNoop} {
    set nSave $::n_conflict
    set ::n_conflict 0
    proc xConflict args { incr ::n_conflict ; return "OMIT" }
    sqlite3changeset_apply_v2 -ignorenoop db2 $::changeset xConflict
    if {$::n_conflict!=$nSave} {
      error "-ignorenoop problem ($::n_conflict $nSave)..."
    }
  }
}

proc do_iterator_test {tn tbl_list sql res} {
  sqlite3session S db main
  S object_config rowid 1

  if {[llength $tbl_list]==0} { S attach * }
  foreach t $tbl_list {S attach $t}

  execsql $sql

  set r [list]
  foreach v $res { lappend r $v }

  set x [list]
# set ::c [S changeset] ; execsql_pp { SELECT quote($::c) }
  sqlite3session_foreach c [S changeset] { lappend x $c }
  uplevel do_test $tn [list [list set {} $x]] [list $r]

  S delete
}

# Compare the contents of all tables in [db1] and [db2]. Throw an error if 
# they are not identical, or return an empty string if they are.
#
proc compare_db {db1 db2} {

  set sql {SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name}
  set lot1 [$db1 eval $sql]
  set lot2 [$db2 eval $sql]

  if {$lot1 != $lot2} { 
    puts $lot1
    puts $lot2
    error "databases contain different tables" 
  }

  foreach tbl $lot1 {
    set col1 [list]
    set col2 [list]
    set quoted [list]

    $db1 eval "PRAGMA table_info = $tbl" { lappend col1 $name }
    $db2 eval "PRAGMA table_info = $tbl" { 
      lappend col2 $name 
      lappend quoted "\"[string map {\" \"\"} $name]\""
    }
    if {$col1 != $col2} { error "table $tbl schema mismatch" }

    set sql "SELECT * FROM $tbl ORDER BY [join $quoted ,]"
    set data1 [$db1 eval $sql]
    set data2 [$db2 eval $sql]
    if {$data1 != $data2} { 
      puts "$db1: $data1"
      puts "$db2: $data2"
      error "table $tbl data mismatch" 
    }
  }

  return ""
}

proc changeset_to_list {c} {
  set list [list]
  sqlite3session_foreach elem $c { lappend list $elem }
  lsort $list
}

set ones {zero one two three four five six seven eight nine
          ten eleven twelve thirteen fourteen fifteen sixteen seventeen
          eighteen nineteen}
set tens {{} ten twenty thirty forty fifty sixty seventy eighty ninety}
proc number_name {n} {
  if {$n>=1000} {
    set txt "[number_name [expr {$n/1000}]] thousand"
    set n [expr {$n%1000}]
  } else {
    set txt {}
  }
  if {$n>=100} {
    append txt " [lindex $::ones [expr {$n/100}]] hundred"
    set n [expr {$n%100}]
  }
  if {$n>=20} {
    append txt " [lindex $::tens [expr {$n/10}]]"
    set n [expr {$n%10}]
  }
  if {$n>0} {
    append txt " [lindex $::ones $n]"
  }
  set txt [string trim $txt]
  if {$txt==""} {set txt zero}
  return $txt
}

proc scksum {db dbname} {

  if {$dbname=="temp"} {
    set master sqlite_temp_master
  } else {
    set master $dbname.sqlite_master
  }

  set alltab [$db eval "SELECT name FROM $master WHERE type='table'"]
  set txt [$db eval "SELECT * FROM $master ORDER BY type,name,sql"]
  foreach tab $alltab {
    set cols [list]
    db eval "PRAGMA $dbname.table_info = $tab" x { 
      lappend cols "quote($x(name))" 
    }
    set cols [join $cols ,]
    append txt [db eval "SELECT $cols FROM $dbname.$tab ORDER BY $cols"]
  }
  return [md5 $txt]
}

proc do_diff_test {tn setup} {
  reset_db
  forcedelete test.db2
  execsql { ATTACH 'test.db2' AS aux }
  execsql $setup

  sqlite3session S db main
  S object_config rowid 1
  foreach tbl [db eval {SELECT name FROM sqlite_master WHERE type='table'}] {
    S attach $tbl
    S diff aux $tbl
  }

  set C [S changeset]
  S delete

  sqlite3 db2 test.db2
  sqlite3changeset_apply db2 $C ""
  uplevel do_test $tn.1 [list {execsql { PRAGMA integrity_check } db2}] ok
  db2 close

  set cksum [scksum db main]
  uplevel do_test $tn.2 [list {scksum db aux}] [list $cksum]
}

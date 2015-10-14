# 2014 Dec 19
#
# The author disclaims copyright to this source code.  In place of
# a legal notice, here is a blessing:
#
#    May you do good and not evil.
#    May you find forgiveness for yourself and forgive others.
#    May you share freely, never taking more than you give.
#
#***********************************************************************
#

if {![info exists testdir]} {
  set testdir [file join [file dirname [info script]] .. .. .. test]
}
source $testdir/tester.tcl

catch { 
  sqlite3_fts5_may_be_corrupt 0 
  reset_db
}

proc fts5_test_poslist {cmd} {
  set res [list]
  for {set i 0} {$i < [$cmd xInstCount]} {incr i} {
    lappend res [string map {{ } .} [$cmd xInst $i]]
  }
  set res
}

proc fts5_test_columnsize {cmd} {
  set res [list]
  for {set i 0} {$i < [$cmd xColumnCount]} {incr i} {
    lappend res [$cmd xColumnSize $i]
  }
  set res
}

proc fts5_test_columntext {cmd} {
  set res [list]
  for {set i 0} {$i < [$cmd xColumnCount]} {incr i} {
    lappend res [$cmd xColumnText $i]
  }
  set res
}

proc fts5_test_columntotalsize {cmd} {
  set res [list]
  for {set i 0} {$i < [$cmd xColumnCount]} {incr i} {
    lappend res [$cmd xColumnTotalSize $i]
  }
  set res
}

proc test_append_token {varname token iStart iEnd} {
  upvar $varname var
  lappend var $token
  return "SQLITE_OK"
}
proc fts5_test_tokenize {cmd} {
  set res [list]
  for {set i 0} {$i < [$cmd xColumnCount]} {incr i} {
    set tokens [list]
    $cmd xTokenize [$cmd xColumnText $i] [list test_append_token tokens]
    lappend res $tokens
  }
  set res
}

proc fts5_test_rowcount {cmd} {
  $cmd xRowCount
}

proc test_queryphrase_cb {cnt cmd} {
  upvar $cnt L 
  for {set i 0} {$i < [$cmd xInstCount]} {incr i} {
    foreach {ip ic io} [$cmd xInst $i] break
    set A($ic) 1
  }
  foreach ic [array names A] {
    lset L $ic [expr {[lindex $L $ic] + 1}]
  }
}
proc fts5_test_queryphrase {cmd} {
  set res [list]
  for {set i 0} {$i < [$cmd xPhraseCount]} {incr i} {
    set cnt [list]
    for {set j 0} {$j < [$cmd xColumnCount]} {incr j} { lappend cnt 0 }
    $cmd xQueryPhrase $i [list test_queryphrase_cb cnt]
    lappend res $cnt
  }
  set res
}

proc fts5_test_phrasecount {cmd} {
  $cmd xPhraseCount
}

proc fts5_test_all {cmd} {
  set res [list]
  lappend res columnsize      [fts5_test_columnsize $cmd]
  lappend res columntext      [fts5_test_columntext $cmd]
  lappend res columntotalsize [fts5_test_columntotalsize $cmd]
  lappend res poslist         [fts5_test_poslist $cmd]
  lappend res tokenize        [fts5_test_tokenize $cmd]
  lappend res rowcount        [fts5_test_rowcount $cmd]
  set res
}

proc fts5_aux_test_functions {db} {
  foreach f {
    fts5_test_columnsize
    fts5_test_columntext
    fts5_test_columntotalsize
    fts5_test_poslist
    fts5_test_tokenize
    fts5_test_rowcount
    fts5_test_all

    fts5_test_queryphrase
    fts5_test_phrasecount
  } {
    sqlite3_fts5_create_function $db $f $f
  }
}

proc fts5_level_segs {tbl} {
  set sql "SELECT fts5_decode(rowid,block) aS r FROM ${tbl}_data WHERE rowid=10"
  set ret [list]
  foreach L [lrange [db one $sql] 1 end] {
    lappend ret [expr [llength $L] - 3]
  }
  set ret
} 

proc fts5_level_segids {tbl} {
  set sql "SELECT fts5_decode(rowid,block) aS r FROM ${tbl}_data WHERE rowid=10"
  set ret [list]
  foreach L [lrange [db one $sql] 1 end] {
    set lvl [list]
    foreach S [lrange $L 3 end] {
      regexp {id=([1234567890]*)} $S -> segid
      lappend lvl $segid
    }
    lappend ret $lvl
  }
  set ret
}

proc fts5_rnddoc {n} {
  set map [list 0 a  1 b  2 c  3 d  4 e  5 f  6 g  7 h  8 i  9 j]
  set doc [list]
  for {set i 0} {$i < $n} {incr i} {
    lappend doc "x[string map $map [format %.3d [expr int(rand()*1000)]]]"
  }
  set doc
}

#-------------------------------------------------------------------------
# Usage:
#
#   nearset aCol ?-pc VARNAME? ?-near N? ?-col C? -- phrase1 phrase2...
#
# This command is used to test if a document (set of column values) matches
# the logical equivalent of a single FTS5 NEAR() clump and, if so, return
# the equivalent of an FTS5 position list.
#
# Parameter $aCol is passed a list of the column values for the document
# to test. Parameters $phrase1 and so on are the phrases.
#
# The result is a list of phrase hits. Each phrase hit is formatted as
# three integers separated by "." characters, in the following format:
#
#   <phrase number> . <column number> . <token offset>
#
# Options:
#
#   -near N        (NEAR distance. Default 10)
#   -col  C        (List of column indexes to match against)
#   -pc   VARNAME  (variable in caller frame to use for phrase numbering)
#
proc nearset {aCol args} {
  set O(-near) 10
  set O(-col)  {}
  set O(-pc)   ""

  set nOpt [lsearch -exact $args --]
  if {$nOpt<0} { error "no -- option" }

  foreach {k v} [lrange $args 0 [expr $nOpt-1]] {
    if {[info exists O($k)]==0} { error "unrecognized option $k" }
    set O($k) $v
  }

  if {$O(-pc) == ""} {
    set counter 0
  } else {
    upvar $O(-pc) counter
  }

  # Set $phraselist to be a list of phrases. $nPhrase its length.
  set phraselist [lrange $args [expr $nOpt+1] end]
  set nPhrase [llength $phraselist]

  for {set j 0} {$j < [llength $aCol]} {incr j} {
    for {set i 0} {$i < $nPhrase} {incr i} { 
      set A($j,$i) [list]
    }
  }

  set iCol -1
  foreach col $aCol {
    incr iCol
    if {$O(-col)!="" && [lsearch $O(-col) $iCol]<0} continue
    set nToken [llength $col]

    set iFL [expr $O(-near) >= $nToken ? $nToken - 1 : $O(-near)]
    for { } {$iFL < $nToken} {incr iFL} {
      for {set iPhrase 0} {$iPhrase<$nPhrase} {incr iPhrase} {
        set B($iPhrase) [list]
      }
      
      for {set iPhrase 0} {$iPhrase<$nPhrase} {incr iPhrase} {
        set p [lindex $phraselist $iPhrase]
        set nPm1 [expr {[llength $p] - 1}]
        set iFirst [expr $iFL - $O(-near) - [llength $p]]

        for {set i $iFirst} {$i <= $iFL} {incr i} {
          if {[lrange $col $i [expr $i+$nPm1]] == $p} { lappend B($iPhrase) $i }
        }
        if {[llength $B($iPhrase)] == 0} break
      }

      if {$iPhrase==$nPhrase} {
        for {set iPhrase 0} {$iPhrase<$nPhrase} {incr iPhrase} {
          set A($iCol,$iPhrase) [concat $A($iCol,$iPhrase) $B($iPhrase)]
          set A($iCol,$iPhrase) [lsort -integer -uniq $A($iCol,$iPhrase)]
        }
      }
    }
  }

  set res [list]
  #puts [array names A]

  for {set iPhrase 0} {$iPhrase<$nPhrase} {incr iPhrase} {
    for {set iCol 0} {$iCol < [llength $aCol]} {incr iCol} {
      foreach a $A($iCol,$iPhrase) {
        lappend res "$counter.$iCol.$a"
      }
    }
    incr counter
  }

  #puts $res
  sort_poslist $res
}

#-------------------------------------------------------------------------
# Usage:
#
#   sort_poslist LIST
#
# Sort a position list of the type returned by command [nearset]
#
proc sort_poslist {L} {
  lsort -command instcompare $L
}
proc instcompare {lhs rhs} {
  foreach {p1 c1 o1} [split $lhs .] {}
  foreach {p2 c2 o2} [split $rhs .] {}

  set res [expr $c1 - $c2]
  if {$res==0} { set res [expr $o1 - $o2] }
  if {$res==0} { set res [expr $p1 - $p2] }

  return $res
}

#-------------------------------------------------------------------------
# Logical operators used by the commands returned by fts5_tcl_expr().
#
proc AND {args} {
  foreach a $args {
    if {[llength $a]==0} { return [list] }
  }
  sort_poslist [concat {*}$args]
}
proc OR {args} {
  sort_poslist [concat {*}$args]
}
proc NOT {a b} {
  if {[llength $b]>0} { return [list] }
  return $a
}

#-------------------------------------------------------------------------
# This command is similar to [split], except that it also provides the
# start and end offsets of each token. For example:
#
#   [fts5_tokenize_split "abc d ef"] -> {abc 0 3 d 4 5 ef 6 8}
#

proc gobble_whitespace {textvar} {
  upvar $textvar t
  regexp {([ ]*)(.*)} $t -> space t
  return [string length $space]
}

proc gobble_text {textvar wordvar} {
  upvar $textvar t
  upvar $wordvar w
  regexp {([^ ]*)(.*)} $t -> w t
  return [string length $w]
}

proc fts5_tokenize_split {text} {
  set token ""
  set ret [list]
  set iOff [gobble_whitespace text]
  while {[set nToken [gobble_text text word]]} {
    lappend ret $word $iOff [expr $iOff+$nToken]
    incr iOff $nToken
    incr iOff [gobble_whitespace text]
  }

  set ret
}


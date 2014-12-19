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

proc test_append_token {varname token iStart iEnd iPos} {
  upvar $varname var
  lappend var $token
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
  } {
    sqlite3_fts5_create_function $db $f $f
  }
}



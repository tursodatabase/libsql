# 2021 September 13
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
# The focus of this file is testing the r-tree extension.
#

if {![info exists testdir]} {
  set testdir [file join [file dirname [info script]] .. .. test]
}
source [file join [file dirname [info script]] rtree_util.tcl]
source $testdir/tester.tcl
set testprefix rtreedoc3

ifcapable !rtree {
  finish_test
  return
}


# This command assumes that the argument is a node blob for a 2 dimensional
# i32 r-tree table. It decodes and returns a list of cells from the node
# as a list. Each cell is itself a list of the following form:
#
#    {$rowid $minX $maxX $minY $maxY}
#
# For internal (non-leaf) nodes, the rowid is replaced by the child node
# number.
#
proc rnode_cells {aData} {
  set nDim 2

  set nData [string length $aData]
  set nBytePerCell [expr (8 + 2*$nDim*4)]
  binary scan [string range $aData 2 3] S nCell

  set res [list]
  for {set i 0} {$i < $nCell} {incr i} {
    set iOff [expr $i*$nBytePerCell+4]
    set cell [string range $aData $iOff [expr $iOff+$nBytePerCell-1]]
    binary scan $cell WIIII rowid x1 x2 y1 y2
    lappend res [list $rowid $x1 $x2 $y1 $y2]
  }

  return $res
}

# Interpret the first two bytes of the blob passed as the only parameter
# as a 16-bit big-endian integer and return the value. If this blob is
# the root node of an r-tree, this value is the height of the tree.
#
proc rnode_height {aData} {
  binary scan [string range $aData 0 1] S nHeight
  return $nHeight
}

# Return a blob containing node iNode of r-tree "rt".
#
proc rt_node_get {iNode} {
  db one { SELECT data FROM rt_node WHERE nodeno=$iNode }
}


#--------------------------------------------------------------
# API:
#
#    pq_init 
#      Initialize a new test.
#
#    pq_test_callback
#      Invoked each time the xQueryCallback function is called. This Tcl
#      command checks that the arguments that SQLite passed to xQueryCallback
#      are as expected.
#
#    pq_test_row
#      Invoked each time a row is returned. Checks that the row returned
#      was predicted by the documentation.
#
# DATA STRUCTURE:
#    The priority queue is stored as a Tcl list. The order of elements in 
#    the list is unimportant - it is just used as a set here. Each element
#    in the priority queue is itself a list. The first element is the
#    priority value for the entry (a real). Following this is a list of
#    key-value pairs that make up the entries fields.
#
proc pq_init {} {
  global Q 
  set Q(pri_queue)  [list]

  set nHeight [rnode_height [rt_node_get 1]]
  set nCell [llength [rnode_cells [rt_node_get 1]]]

  # EVIDENCE-OF: R-54708-13595 An R*Tree query is initialized by making
  # the root node the only entry in a priority queue sorted by rScore.
  lappend Q(pri_queue) [list 0.0 [list \
    iLevel [expr $nHeight+1] \
    iChild 1                 \
    iCurrent   0             \
  ]]
}

proc pq_extract {} {
  global Q
  if {[llength $Q(pri_queue)]==0} {
    error "priority queue is empty!"
  }

  # Find the priority queue entry with the lowest score.
  #
  # EVIDENCE-OF: R-47257-47871 Smaller scores are processed first.
  set iBest 0
  set rBestScore [lindex $Q(pri_queue) 0 0]
  for {set ii 1} {$ii < [llength $Q(pri_queue)]} {incr ii} {
    set rScore [expr [lindex $Q(pri_queue) $ii 0]]
    if {$rScore<$rBestScore} {
      set rBestScore $rScore
      set iBest $ii
    }
  }

  # Extract the entry with the lowest score from the queue and return it. 
  #
  # EVIDENCE-OF: R-60002-49798 The query proceeds by extracting the entry
  # from the priority queue that has the lowest score.
  set ret [lindex $Q(pri_queue) $iBest]
  set Q(pri_queue) [lreplace $Q(pri_queue) $iBest $iBest]

  return $ret
}

proc pq_new_entry {rScore iLevel cell} {
  global Q

  set rowid_name "iChild"
  if {$iLevel==0} { set rowid_name "iRowid" }

  set kv [list]
  lappend kv aCoord [lrange $cell 1 end]
  lappend kv iLevel $iLevel

  if {$iLevel==0} {
    lappend kv iRowid [lindex $cell 0]
  } else {
    lappend kv iChild [lindex $cell 0]
    lappend kv iCurrent 0
  }

  lappend Q(pri_queue) [list $rScore $kv]
}

proc pq_test_callback {L res} {
  #pq_debug "pq_test_callback $L -> $res"
  global Q

  array set G $L    ;# "Got" - as in stuff passed to xQuery

  # EVIDENCE-OF: R-65127-42665 If the extracted priority queue entry is a
  # node (a subtree), then the next child of that node is passed to the
  # xQueryFunc callback.
  #
  # If it had been a leaf, the row should have been returned, instead of
  # xQueryCallback being called on a child - as is happening here.
  foreach {rParentScore parent} [pq_extract] {}
  array set P $parent ;# "Parent" - as in parent of expected cell
  if {$P(iLevel)==0} { error "query callback mismatch (1)" }
  set child_node [rnode_cells [rt_node_get $P(iChild)]]
  set expected_cell [lindex $child_node $P(iCurrent)]
  set expected_coords [lrange $expected_cell 1 end]
  if {[llength $expected_coords] != [llength $G(aCoord)]} {
  puts [array get P]
  puts "E: $expected_coords  G: $G(aCoord)"
    error "coordinate mismatch in query callback (1)"
  }
  foreach a [lrange $expected_cell 1 end] b $G(aCoord) {
    if {$a!=$b} { error "coordinate mismatch in query callback (2)" }
  }

  # Check level is as expected
  #
  if {$G(iLevel) != $P(iLevel)-1} {
    error "iLevel mismatch in query callback (1)"
  }

  # Unless the callback returned NOT_WITHIN, add the entry to the priority
  # queue.
  #
  # EVIDENCE-OF: R-28754-35153 Those subelements for which the xQueryFunc
  # callback sets eWithin to PARTLY_WITHIN or FULLY_WITHIN are added to
  # the priority queue using the score supplied by the callback.
  #
  # EVIDENCE-OF: R-08681-45277 Subelements that return NOT_WITHIN are
  # discarded.
  set r [lindex $res 0]
  set rScore [lindex $res 1]
  if {$r!="fully" && $r!="partly" && $r!="not"} {
    error "unknown result: $r - expected \"fully\", \"partly\" or \"not\""
  }
  if {$r!="not"} {
    pq_new_entry $rScore [expr $P(iLevel)-1] $expected_cell
  }

  # EVIDENCE-OF: R-07194-63805 If the node has more children then it is
  # returned to the priority queue. Otherwise it is discarded.
  incr P(iCurrent)
  if {$P(iCurrent)<[llength $child_node]} {
    lappend Q(pri_queue) [list $rParentScore [array get P]]
  }
}

proc pq_test_result {id x1 x2 y1 y2} {
  #pq_debug "pq_test_result $id $x1 $x2 $y1 $y2"
  foreach {rScore next} [pq_extract] {}

  # The extracted entry must be a leaf (otherwise, xQueryCallback would
  # have been called on the extracted entries children instead of just
  # returning the data).
  #
  # EVIDENCE-OF: R-13214-54017 If that entry is a leaf (meaning that it is
  # an actual R*Tree entry and not a subtree) then that entry is returned
  # as one row of the query result.
  array set N $next
  if {$N(iLevel)!=0} { error "result row mismatch (1)" }

  if {$x1!=[lindex $N(aCoord) 0] || $x2!=[lindex $N(aCoord) 1]
   || $y1!=[lindex $N(aCoord) 2] || $y2!=[lindex $N(aCoord) 3]
  } {
    if {$N(iLevel)!=0} { error "result row mismatch (2)" }
  }

  if {$id!=$N(iRowid)} { error "result row mismatch (3)" }
}

proc pq_done {} {
  global Q
  # EVIDENCE-OF: R-57438-45968 The query runs until the priority queue is
  # empty.
  if {[llength $Q(pri_queue)]>0} {
    error "priority queue is not empty!"
  }
}

proc pq_debug {caption} {
  global Q

  puts "**** $caption ****"
  set i 0
  foreach q [lsort -real -index 0 $Q(pri_queue)] { 
    puts "PQ $i: $q" 
    incr i
  }
}

#--------------------------------------------------------------

proc box_query {a} {
  set res [list fully [expr rand()]]
  pq_test_callback $a $res
  return $res
}

register_box_query db box_query

do_execsql_test 1.0 {
  CREATE VIRTUAL TABLE rt USING rtree_i32(id,  x1,x2,  y1,y2);
  WITH s(i) AS (
    SELECT 0 UNION ALL SELECT i+1 FROM s WHERE i<64
  )
  INSERT INTO rt SELECT NULL, a.i, a.i+1, b.i, b.i+1 FROM s a, s b;
}

proc box_query {a} {
  set res [list fully [expr rand()]]
  pq_test_callback $a $res
  return $res
}

pq_init
db eval { SELECT id, x1,x2, y1,y2 FROM rt WHERE id MATCH qbox() } {
  pq_test_result $id $x1 $x2 $y1 $y2
}
pq_done

finish_test



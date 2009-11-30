# 2009 November 04
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
# This file contains common code used the fts3 tests. At one point
# equivalent functionality was implemented in C code. But it is easier
# to use Tcl.
#

#-------------------------------------------------------------------------
# USAGE: fts3_integrity_check TBL
#
# This proc is used to verify that the full-text index is consistent with
# the contents of the fts3 table. In other words, it checks that the
# data in the %_contents table matches that in the %_segdir and %_segments 
# tables.
#
# This is not an efficient procedure. It uses a lot of memory and a lot
# of CPU. But it is better than not checking at all.
#
# The procedure is:
#
#   1) Read the entire full-text index from the %_segdir and %_segments
#      tables into memory. For each entry in the index, the following is
#      done:
#
#          set C($iDocid,$iCol,$iPosition) $zTerm
#
#   2) Iterate through each column of each row of the %_content table. 
#      Tokenize all documents, and check that for each token there is
#      a corresponding entry in the $C array. After checking a token,
#      [unset] the $C array entry.
#
#   3) Check that array $C is now empty.
#      
#
proc fts3_integrity_check {tbl} {

  fts3_read2 $tbl 1 A

  foreach zTerm [array names A] {
    foreach doclist $A($zTerm) {
      set docid 0
      while {[string length $doclist]>0} {
        set iCol 0
        set iPos 0
        set lPos [list]
        set lCol [list]

        # First varint of a doclist-entry is the docid. Delta-compressed
        # with respect to the docid of the previous entry.
        #
        incr docid [gobble_varint doclist]
        if {[info exists D($zTerm,$docid)]} {
          while {[set iDelta [gobble_varint doclist]] != 0} {}
          continue
        }
        set D($zTerm,$docid) 1

        # Gobble varints until the 0x00 that terminates the doclist-entry
        # is found.
        while {[set iDelta [gobble_varint doclist]] > 0} {
          if {$iDelta == 1} {
            set iCol [gobble_varint doclist]
            set iPos 0
          } else {
            incr iPos $iDelta
            incr iPos -2
            set C($docid,$iCol,$iPos) $zTerm
          }
        }
      }
    }
  }

  foreach key [array names C] {
    #puts "$key -> $C($key)"
  }


  db eval "SELECT * FROM ${tbl}_content" E {
    set iCol 0
    set iDoc $E(docid)
    foreach col [lrange $E(*) 1 end] {
      set c $E($col)
      set sql {SELECT fts3_tokenizer_test('simple', $c)}

      foreach {pos term dummy} [db one $sql] {
        if {![info exists C($iDoc,$iCol,$pos)]} {
          set es "Error at docid=$iDoc col=$iCol pos=$pos. Index is missing"
          lappend errors $es
        } else {
          if {$C($iDoc,$iCol,$pos) != "$term"} {
            set    es "Error at docid=$iDoc col=$iCol pos=$pos. Index "
            append es "has \"$C($iDoc,$iCol,$pos)\", document has \"$term\""
            lappend errors $es
          }
          unset C($iDoc,$iCol,$pos)
        }
      }
      incr iCol
    }
  }

  foreach c [array names C] {
    lappend errors "Bad index entry: $c -> $C($c)"
  }

  if {[info exists errors]} { return [join $errors "\n"] }
  return "ok"
}

# USAGE: fts3_terms TBL WHERE
#
# Argument TBL must be the name of an FTS3 table. Argument WHERE is an
# SQL expression that will be used as the WHERE clause when scanning
# the %_segdir table. As in the following query:
#
#   "SELECT * FROM ${TBL}_segdir WHERE ${WHERE}"
#
# This function returns a list of all terms present in the segments
# selected by the statement above.
#
proc fts3_terms {tbl where} {
  fts3_read $tbl $where a
  return [lsort [array names a]]
}


# USAGE: fts3_doclist TBL TERM WHERE
#
# Argument TBL must be the name of an FTS3 table. TERM is a term that may
# or may not be present in the table. Argument WHERE is used to select a 
# subset of the b-tree segments in the associated full-text index as 
# described above for [fts3_terms].
#
# This function returns the results of merging the doclists associated
# with TERM in the selected segments. Each doclist is an element of the
# returned list. Each doclist is formatted as follows:
#
#   [$docid ?$col[$off1 $off2...]?...]
#
# The formatting is odd for a Tcl command in order to be compatible with
# the original C-language implementation. If argument WHERE is "1", then 
# any empty doclists are omitted from the returned list.
#
proc fts3_doclist {tbl term where} {
  fts3_read $tbl $where a


  foreach doclist $a($term) {
    set docid 0

    while {[string length $doclist]>0} {
      set iCol 0
      set iPos 0
      set lPos [list]
      set lCol [list]
      incr docid [gobble_varint doclist]
  
      while {[set iDelta [gobble_varint doclist]] > 0} {
        if {$iDelta == 1} {
          lappend lCol [list $iCol $lPos]
          set iPos 0
          set lPos [list]
          set iCol [gobble_varint doclist]
        } else {
          incr iPos $iDelta
          incr iPos -2
          lappend lPos $iPos
        }
      }
  
      if {[llength $lPos]>0} {
        lappend lCol [list $iCol $lPos]
      }
  
      if {$where != "1" || [llength $lCol]>0} {
        set ret($docid) $lCol
      } else {
        unset -nocomplain ret($docid)
      }
    }
  }

  set lDoc [list]
  foreach docid [lsort -integer [array names ret]] {
    set lCol [list]
    set cols ""
    foreach col $ret($docid) {
      foreach {iCol lPos} $col {}
      append cols " $iCol\[[join $lPos { }]\]"
    }
    lappend lDoc "\[${docid}${cols}\]"
  }

  join $lDoc " "
}

###########################################################################

proc gobble_varint {varname} {
  upvar $varname blob
  set n [read_varint $blob ret]
  set blob [string range $blob $n end]
  return $ret
}
proc gobble_string {varname nLength} {
  upvar $varname blob
  set ret [string range $blob 0 [expr $nLength-1]]
  set blob [string range $blob $nLength end]
  return $ret
}

# The argument is a blob of data representing an FTS3 segment leaf. 
# Return a list consisting of alternating terms (strings) and doclists
# (blobs of data).
#
proc fts3_readleaf {blob} {
  set zPrev ""
  set terms [list]

  while {[string length $blob] > 0} {
    set nPrefix [gobble_varint blob]
    set nSuffix [gobble_varint blob]

    set zTerm [string range $zPrev 0 [expr $nPrefix-1]]
    append zTerm [gobble_string blob $nSuffix]
    set doclist [gobble_string blob [gobble_varint blob]]

    lappend terms $zTerm $doclist
    set zPrev $zTerm
  }

  return $terms
}

proc fts3_read2 {tbl where varname} {
  upvar $varname a
  array unset a
  db eval " SELECT start_block, leaves_end_block, root 
            FROM ${tbl}_segdir WHERE $where
            ORDER BY level ASC, idx DESC
  " {
    if {$start_block == 0} {
      foreach {t d} [fts3_readleaf $root] { lappend a($t) $d }
    } else {
      db eval " SELECT block 
                FROM ${tbl}_segments 
                WHERE blockid>=$start_block AND blockid<=$leaves_end_block
                ORDER BY blockid
      " {
        foreach {t d} [fts3_readleaf $block] { lappend a($t) $d }

      }
    }
  }
}

proc fts3_read {tbl where varname} {
  upvar $varname a
  array unset a
  db eval " SELECT start_block, leaves_end_block, root 
            FROM ${tbl}_segdir WHERE $where
            ORDER BY level DESC, idx ASC
  " {
    if {$start_block == 0} {
      foreach {t d} [fts3_readleaf $root] { lappend a($t) $d }
    } else {
      db eval " SELECT block 
                FROM ${tbl}_segments 
                WHERE blockid>=$start_block AND blockid<$leaves_end_block
                ORDER BY blockid
      " {
        foreach {t d} [fts3_readleaf $block] { lappend a($t) $d }

      }
    }
  }
}

##########################################################################

#-------------------------------------------------------------------------
# This proc is used to test a single SELECT statement. Parameter $name is
# passed a name for the test case (i.e. "fts3_malloc-1.4.1") and parameter
# $sql is passed the text of the SELECT statement. Parameter $result is
# set to the expected output if the SELECT statement is successfully
# executed using [db eval].
#
# Example:
#
#   do_select_test testcase-1.1 "SELECT 1+1, 1+2" {1 2}
#
# If global variable DO_MALLOC_TEST is set to a non-zero value, or if
# it is not defined at all, then OOM testing is performed on the SELECT
# statement. Each OOM test case is said to pass if either (a) executing
# the SELECT statement succeeds and the results match those specified
# by parameter $result, or (b) TCL throws an "out of memory" error.
#
# If DO_MALLOC_TEST is defined and set to zero, then the SELECT statement
# is executed just once. In this case the test case passes if the results
# match the expected results passed via parameter $result.
#
proc do_select_test {name sql result} {
  doPassiveTest $name $sql [list 0 $result]
}

proc do_error_test {name sql error} {
  doPassiveTest $name $sql [list 1 $error]
}

proc doPassiveTest {name sql catchres} {
  if {![info exists ::DO_MALLOC_TEST]} { set ::DO_MALLOC_TEST 1 }

  if {$::DO_MALLOC_TEST} {
    set answers [list {1 {out of memory}} $catchres]
    set modes [list 100000 transient 1 persistent]
  } else {
    set answers [list $catchres]
    set modes [list 0 nofail]
  }
  set str [join $answers " OR "]

  foreach {nRepeat zName} $modes {
    for {set iFail 1} 1 {incr iFail} {
      if {$::DO_MALLOC_TEST} {sqlite3_memdebug_fail $iFail -repeat $nRepeat}

      set res [catchsql $sql]
      if {[lsearch -exact $answers $res]>=0} {
        set res $str
      }
      do_test $name.$zName.$iFail [list set {} $res] $str
      set nFail [sqlite3_memdebug_fail -1 -benigncnt nBenign]
      if {$nFail==0} break
    }
  }
}


#-------------------------------------------------------------------------
# Test a single write to the database. In this case a  "write" is a 
# DELETE, UPDATE or INSERT statement.
#
# If OOM testing is performed, there are several acceptable outcomes:
#
#   1) The write succeeds. No error is returned.
#
#   2) An "out of memory" exception is thrown and:
#
#     a) The statement has no effect, OR
#     b) The current transaction is rolled back, OR
#     c) The statement succeeds. This can only happen if the connection
#        is in auto-commit mode (after the statement is executed, so this
#        includes COMMIT statements).
#
# If the write operation eventually succeeds, zero is returned. If a
# transaction is rolled back, non-zero is returned.
#
# Parameter $name is the name to use for the test case (or test cases).
# The second parameter, $tbl, should be the name of the database table
# being modified. Parameter $sql contains the SQL statement to test.
#
proc do_write_test {name tbl sql} {
  if {![info exists ::DO_MALLOC_TEST]} { set ::DO_MALLOC_TEST 1 }

  # Figure out an statement to get a checksum for table $tbl.
  db eval "SELECT * FROM $tbl" V break
  set cksumsql "SELECT md5sum([join [concat rowid $V(*)] ,]) FROM $tbl"

  # Calculate the initial table checksum.
  set cksum1 [db one $cksumsql]

  if {$::DO_MALLOC_TEST } {
    set answers [list {1 {out of memory}} {0 {}}]
    if {$::DO_MALLOC_TEST==1} {
      set modes {100000 transient}
    } else {
      set modes {1 persistent}
    }
  } else {
    set answers [list {0 {}}]
    set modes [list 0 nofail]
  }
  set str [join $answers " OR "]

  foreach {nRepeat zName} $modes {
    for {set iFail 1} 1 {incr iFail} {
      if {$::DO_MALLOC_TEST} {sqlite3_memdebug_fail $iFail -repeat $nRepeat}

      set res [uplevel [list catchsql $sql]]
      set nFail [sqlite3_memdebug_fail -1 -benigncnt nBenign]
      if {$nFail==0} {
        do_test $name.$zName.$iFail [list set {} $res] {0 {}}
        return
      } else {
        if {[lsearch $answers $res]>=0} {
          set res $str
        }
        do_test $name.$zName.$iFail [list set {} $res] $str
        set cksum2 [db one $cksumsql]
        if {$cksum1 != $cksum2} return
      }
    }
  }
}
